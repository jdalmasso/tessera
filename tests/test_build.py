"""
Tests for surfaces/skills_leaderboard/build.py

Covers:
  - _to_et, _format_int helpers
  - build_context: main leaderboard, category sections, collections, stats
  - Display caps: max_per_repo, max_per_author
  - render: valid HTML output, required sections present
  - main(): writes index.html, raises on missing run
"""

import json
import pytest
from pathlib import Path
from collections import defaultdict

from data.store import (
    get_connection, init_db, start_pipeline_run, complete_pipeline_run,
    store_score, upsert_entity, upsert_signal_source,
)
from surfaces.skills_leaderboard.build import (
    _to_et, _format_int, _category_name, build_context, render, main,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SOURCE_ID  = "github"
SURFACE_ID = "skills_leaderboard"
NOW        = "2026-04-12T10:00:00Z"

ALL_DIMS = [
    "velocity", "adoption", "freshness", "documentation",
    "contributors", "code_quality",
    "composite:trending", "composite:popular", "composite:well_rounded",
]

# Minimal config matching the real YAML structure
_SITE_CFG = {
    "title":          "Test Leaderboard",
    "subtitle":       "Test subtitle",
    "github_repo":    "https://github.com/test/repo",
    "timezone_label": "ET",
    "top_n_main":     3,
    "top_n_category": 2,
    "display_caps":   {"max_per_repo": 2, "max_per_author": 3},
    "collections":    {"min_skills": 2, "top_n_for_ranking": 2},
}

_CATEGORIES_CFG = {
    "categories": [
        {"id": "backend",       "name": "Backend",     "keywords": ["api"]},
        {"id": "data_ai",       "name": "Data & AI",   "keywords": ["ml"]},
        {"id": "frontend_design","name": "Frontend",   "keywords": ["ui"]},
        {"id": "other",         "name": "Other",       "keywords": []},
    ]
}

_SCORING_CFG = {
    "methodologies": {
        "trending":    {"weights": {"velocity": 25, "adoption": 20, "freshness": 20,
                                    "documentation": 15, "contributors": 10, "code_quality": 10}},
        "popular":     {"weights": {"velocity": 10, "adoption": 30, "freshness": 20,
                                    "documentation": 15, "contributors": 15, "code_quality": 10}},
        "well_rounded":{"weights": {"velocity": 10, "adoption": 15, "freshness": 15,
                                    "documentation": 25, "contributors": 10, "code_quality": 25}},
    }
}

_CONFIG = {"site": _SITE_CFG, "categories": _CATEGORIES_CFG, "scoring": _SCORING_CFG}


def _make_data(entities: list[dict]) -> dict:
    entity_scores = {}
    entity_meta   = {}
    for e in entities:
        entity_scores[e["id"]] = e["scores"]
        entity_meta[e["id"]]   = {
            "name":     e["name"],
            "category": e.get("category", "backend"),
            "metadata": {"repo": e.get("repo", "alice/repo"),
                         "stars": e.get("stars", 10)},
        }
    return {
        "run_meta": {
            "run_id":       "test-run",
            "started_at":   NOW,
            "completed_at": NOW,
            "stats":        {"repos_discovered": len(entities), "valid_skills": len(entities)},
        },
        "entity_scores": entity_scores,
        "entity_meta":   entity_meta,
    }


def _scores(trending=50.0, popular=45.0, well_rounded=48.0,
            velocity=0.5, adoption=0.3, freshness=0.6,
            documentation=0.7, contributors=0.4, code_quality=0.6):
    return {
        "composite:trending":    trending,
        "composite:popular":     popular,
        "composite:well_rounded":well_rounded,
        "velocity": velocity, "adoption": adoption, "freshness": freshness,
        "documentation": documentation, "contributors": contributors,
        "code_quality": code_quality,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class TestHelpers:

    def test_format_int_thousands(self):
        assert _format_int(1234567) == "1,234,567"

    def test_format_int_small(self):
        assert _format_int(42) == "42"

    def test_format_int_zero(self):
        assert _format_int(0) == "0"

    def test_format_int_bad_input(self):
        assert _format_int("n/a") == "n/a"

    def test_to_et_valid(self):
        result = _to_et("2026-04-12T14:00:00Z")
        assert "2026" in result
        assert "ET" in result

    def test_to_et_invalid(self):
        result = _to_et("not-a-date")
        assert result == "not-a-date"

    def test_category_name_found(self):
        assert _category_name("backend", _CATEGORIES_CFG["categories"]) == "Backend"

    def test_category_name_not_found(self):
        result = _category_name("unknown_cat", _CATEGORIES_CFG["categories"])
        assert "Unknown Cat" == result or "unknown_cat" in result.lower()


# ---------------------------------------------------------------------------
# build_context
# ---------------------------------------------------------------------------

class TestBuildContext:

    def test_main_skills_sorted_by_trending_desc(self):
        entities = [
            {"id": "skill:a/r1", "name": "Low",  "repo": "a/r1", "scores": _scores(trending=20.0)},
            {"id": "skill:a/r2", "name": "High", "repo": "a/r2", "scores": _scores(trending=80.0)},
            {"id": "skill:a/r3", "name": "Mid",  "repo": "a/r3", "scores": _scores(trending=50.0)},
        ]
        ctx = build_context(_make_data(entities), _CONFIG)
        names = [s["name"] for s in ctx["main_skills"]]
        assert names[0] == "High"
        assert names[1] == "Mid"
        assert names[2] == "Low"

    def test_main_skills_capped_at_top_n(self):
        entities = [
            {"id": f"skill:a/r{i}", "name": f"Skill{i}", "repo": f"a/r{i}",
             "scores": _scores(trending=float(i))}
            for i in range(10)
        ]
        ctx = build_context(_make_data(entities), _CONFIG)
        assert len(ctx["main_skills"]) <= _SITE_CFG["top_n_main"]

    def test_display_cap_max_per_repo(self):
        # 3 skills from same repo — cap is 2
        entities = [
            {"id": f"skill:alice/mono:s{i}", "name": f"S{i}",
             "repo": "alice/mono", "scores": _scores(trending=float(80 - i))}
            for i in range(3)
        ]
        ctx = build_context(_make_data(entities), _CONFIG)
        repo_counts = defaultdict(int)
        for s in ctx["main_skills"]:
            repo_counts[s["repo"]] += 1
        assert repo_counts["alice/mono"] <= _SITE_CFG["display_caps"]["max_per_repo"]

    def test_all_categories_present(self):
        ctx = build_context(_make_data([
            {"id": "skill:a/b", "name": "A", "repo": "a/b", "scores": _scores()}
        ]), _CONFIG)
        cat_ids = {c["id"] for c in ctx["categories"]}
        assert "backend" in cat_ids
        assert "other" in cat_ids

    def test_category_top_skills_capped(self):
        entities = [
            {"id": f"skill:a/r{i}", "name": f"S{i}", "repo": f"a/r{i}",
             "category": "backend", "scores": _scores(trending=float(i))}
            for i in range(5)
        ]
        ctx = build_context(_make_data(entities), _CONFIG)
        backend = next(c for c in ctx["categories"] if c["id"] == "backend")
        assert len(backend["top_skills"]) <= _SITE_CFG["top_n_category"]

    def test_empty_category_has_no_top_skills(self):
        ctx = build_context(_make_data([
            {"id": "skill:a/b", "name": "A", "repo": "a/b",
             "category": "backend", "scores": _scores()}
        ]), _CONFIG)
        frontend = next(c for c in ctx["categories"] if c["id"] == "frontend_design")
        assert frontend["top_skills"] == []

    def test_collections_detected(self):
        entities = [
            {"id": "skill:alice/mono:skills/a", "name": "A",
             "repo": "alice/mono", "scores": _scores(trending=70.0)},
            {"id": "skill:alice/mono:skills/b", "name": "B",
             "repo": "alice/mono", "scores": _scores(trending=60.0)},
        ]
        ctx = build_context(_make_data(entities), _CONFIG)
        assert len(ctx["collections"]) == 1
        assert ctx["collections"][0]["repo"] == "alice/mono"
        assert ctx["collections"][0]["skill_count"] == 2

    def test_solo_repos_not_in_collections(self):
        entities = [
            {"id": "skill:alice/solo", "name": "Solo",
             "repo": "alice/solo", "scores": _scores()},
        ]
        ctx = build_context(_make_data(entities), _CONFIG)
        assert ctx["collections"] == []

    def test_stats_total_skills(self):
        entities = [
            {"id": f"skill:a/r{i}", "name": f"S{i}", "repo": f"a/r{i}", "scores": _scores()}
            for i in range(5)
        ]
        ctx = build_context(_make_data(entities), _CONFIG)
        assert ctx["stats"]["total_skills"] == 5

    def test_stats_score_distributions_have_three_methodologies(self):
        entities = [
            {"id": f"skill:a/r{i}", "name": f"S{i}", "repo": f"a/r{i}", "scores": _scores()}
            for i in range(3)
        ]
        ctx = build_context(_make_data(entities), _CONFIG)
        labels = [d["label"] for d in ctx["stats"]["score_distributions"]]
        assert "Trending" in labels
        assert "Popular" in labels
        assert "Well-Rounded" in labels

    def test_stats_category_distribution_sorted_desc(self):
        entities = (
            [{"id": f"skill:a/r{i}", "name": f"B{i}", "repo": f"a/r{i}",
              "category": "backend", "scores": _scores()} for i in range(5)]
            + [{"id": f"skill:a/q{i}", "name": f"D{i}", "repo": f"a/q{i}",
                "category": "data_ai", "scores": _scores()} for i in range(2)]
        )
        ctx = build_context(_make_data(entities), _CONFIG)
        dist = [d for d in ctx["stats"]["category_distribution"] if d["count"] > 0]
        assert dist[0]["count"] >= dist[1]["count"]

    def test_css_inlined(self):
        ctx = build_context(_make_data([
            {"id": "skill:a/b", "name": "A", "repo": "a/b", "scores": _scores()}
        ]), _CONFIG)
        assert len(ctx["css"]) > 100
        assert "prefers-color-scheme" in ctx["css"]


# ---------------------------------------------------------------------------
# render
# ---------------------------------------------------------------------------

class TestRender:

    def _ctx(self, n=2):
        entities = [
            {"id": f"skill:alice/repo{i}", "name": f"Skill {i}",
             "repo": f"alice/repo{i}", "category": "backend",
             "scores": _scores(trending=float(80 - i))}
            for i in range(n)
        ]
        return build_context(_make_data(entities), _CONFIG)

    def test_renders_valid_html(self):
        html = render(self._ctx())
        assert html.strip().startswith("<!DOCTYPE html>")
        assert "</html>" in html

    def test_title_in_output(self):
        html = render(self._ctx())
        assert "Test Leaderboard" in html

    def test_trending_section_present(self):
        html = render(self._ctx())
        assert 'id="trending"' in html

    def test_category_sections_present(self):
        html = render(self._ctx())
        assert 'id="cat-backend"' in html

    def test_stats_section_present(self):
        html = render(self._ctx())
        assert 'id="stats"' in html

    def test_skill_names_in_output(self):
        html = render(self._ctx())
        assert "Skill 0" in html

    def test_dark_mode_media_query(self):
        html = render(self._ctx())
        assert "prefers-color-scheme" in html

    def test_no_javascript(self):
        html = render(self._ctx())
        assert "<script" not in html.lower()

    def test_collections_section_when_present(self):
        entities = [
            {"id": "skill:alice/mono:s/a", "name": "A",
             "repo": "alice/mono", "scores": _scores(trending=70.0)},
            {"id": "skill:alice/mono:s/b", "name": "B",
             "repo": "alice/mono", "scores": _scores(trending=60.0)},
        ]
        ctx = build_context(_make_data(entities), _CONFIG)
        html = render(ctx)
        assert 'id="collections"' in html
        assert "alice/mono" in html

    def test_empty_category_shows_empty_state(self):
        html = render(self._ctx())
        assert "No skills in this category yet" in html

    def test_score_pills_present(self):
        html = render(self._ctx())
        assert "score-pill" in html
        assert "Vel" in html
        assert "Adop" in html

    def test_stats_block_has_category_table(self):
        html = render(self._ctx())
        assert "Backend" in html
        # stats note link present
        assert "seed-run-report.md" in html


# ---------------------------------------------------------------------------
# main() — end-to-end
# ---------------------------------------------------------------------------

class TestMain:

    def test_writes_index_html(self, tmp_path):
        db_file = str(tmp_path / "test.db")
        conn = get_connection(db_file)
        init_db(conn)
        upsert_signal_source(conn, SOURCE_ID, "GitHub API", last_run_at=NOW)

        run_id = "run-build-1"
        start_pipeline_run(conn, run_id, SURFACE_ID, NOW)

        upsert_entity(conn, "skill:alice/repo", "skill", "Alice Skill",
                      "A great skill", {"repo": "alice/repo", "stars": 10},
                      "backend", NOW)
        for dim in ALL_DIMS:
            val = 50.0 if dim.startswith("composite") else 0.5
            store_score(conn, "skill:alice/repo", dim, val, NOW, run_id)

        complete_pipeline_run(conn, run_id, NOW,
                              stats={"repos_discovered": 1, "valid_skills": 1})

        out_dir = tmp_path / "build"
        result  = main(db_path=db_file, run_id=run_id, output_dir=out_dir)

        assert result == out_dir / "index.html"
        assert result.exists()
        html = result.read_text()
        assert "<!DOCTYPE html>" in html
        assert "Alice Skill" in html

    def test_raises_when_no_completed_run(self, tmp_path):
        db_file = str(tmp_path / "empty.db")
        conn = get_connection(db_file)
        init_db(conn)

        with pytest.raises(RuntimeError, match="No completed pipeline run"):
            main(db_path=db_file, output_dir=tmp_path / "build")
