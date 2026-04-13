"""
Tests for surfaces/skills_leaderboard/seed_report.py

Covers:
  - Statistical helpers: dist_stats, is_degenerate, _percentile
  - collect_run_data: reads scores + entity metadata from DB
  - generate_report: correct sections, tables, anomaly flags
  - main(): writes file to disk, raises on missing run
"""

import pytest

from data.store import (
    get_connection,
    init_db,
    start_pipeline_run,
    store_score,
    upsert_entity,
    upsert_signal_source,
    complete_pipeline_run,
)
from surfaces.skills_leaderboard.seed_report import (
    _percentile,
    collect_run_data,
    dist_stats,
    generate_report,
    is_degenerate,
    main,
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


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db(tmp_path):
    conn = get_connection(str(tmp_path / "test.db"))
    init_db(conn)
    upsert_signal_source(conn, SOURCE_ID, "GitHub API", last_run_at=NOW)
    return conn


def _seed_entity_with_scores(conn, run_id, entity_id, name, category,
                              dim_scores: dict[str, float]):
    """Insert an entity and a score row for each dimension."""
    upsert_entity(
        conn,
        entity_id=entity_id,
        entity_type="skill",
        name=name,
        description=f"Description for {name}",
        metadata={"repo": entity_id.replace("skill:", "").split(":")[0]},
        category=category,
        now=NOW,
    )
    for dim, value in dim_scores.items():
        store_score(conn, entity_id, dim, value, NOW, run_id)


def _full_scores(velocity=0.5, adoption=0.5, freshness=0.5,
                 documentation=0.5, contributors=0.5, code_quality=0.5,
                 trending=50.0, popular=50.0, well_rounded=50.0):
    return {
        "velocity": velocity, "adoption": adoption, "freshness": freshness,
        "documentation": documentation, "contributors": contributors,
        "code_quality": code_quality,
        "composite:trending": trending,
        "composite:popular": popular,
        "composite:well_rounded": well_rounded,
    }


# ---------------------------------------------------------------------------
# Statistical helpers
# ---------------------------------------------------------------------------

class TestPercentile:

    def test_empty_list(self):
        assert _percentile([], 50) == 0.0

    def test_single_element(self):
        assert _percentile([0.7], 25) == 0.7

    def test_median_even(self):
        assert _percentile([0.0, 1.0], 50) == pytest.approx(0.5)

    def test_p25_four_values(self):
        # [0, 1, 2, 3]: p25 index = 0.75 → 0 + 0.75*(1-0) = 0.75
        assert _percentile([0.0, 1.0, 2.0, 3.0], 25) == pytest.approx(0.75)

    def test_p100_returns_max(self):
        vals = [1.0, 2.0, 5.0, 7.0]
        assert _percentile(vals, 100) == pytest.approx(7.0)

    def test_p0_returns_min(self):
        vals = [1.0, 2.0, 5.0, 7.0]
        assert _percentile(vals, 0) == pytest.approx(1.0)


class TestDistStats:

    def test_empty_returns_zeros(self):
        s = dist_stats([])
        assert s["count"] == 0
        assert s["mean"] == 0.0

    def test_single_value(self):
        s = dist_stats([0.6])
        assert s["count"] == 1
        assert s["min"] == pytest.approx(0.6)
        assert s["max"] == pytest.approx(0.6)
        assert s["stddev"] == pytest.approx(0.0)

    def test_known_values(self):
        s = dist_stats([0.0, 0.5, 1.0])
        assert s["min"]    == pytest.approx(0.0)
        assert s["max"]    == pytest.approx(1.0)
        assert s["mean"]   == pytest.approx(0.5)
        assert s["median"] == pytest.approx(0.5)

    def test_stddev_nonzero(self):
        s = dist_stats([0.0, 1.0])
        assert s["stddev"] > 0.0


class TestIsDegenerate:

    def test_all_identical_is_degenerate(self):
        assert is_degenerate([0.5] * 100) is True

    def test_all_distinct_not_degenerate(self):
        assert is_degenerate([i / 100 for i in range(100)]) is False

    def test_exactly_80_percent_threshold(self):
        # 80 identical + 20 distinct → 80% → not degenerate (strictly >)
        vals = [0.5] * 80 + list(range(20))
        assert is_degenerate(vals, threshold=0.80) is False

    def test_over_threshold_is_degenerate(self):
        vals = [0.5] * 81 + list(range(19))
        assert is_degenerate(vals, threshold=0.80) is True

    def test_empty_list(self):
        assert is_degenerate([]) is False


# ---------------------------------------------------------------------------
# collect_run_data
# ---------------------------------------------------------------------------

class TestCollectRunData:

    def test_returns_correct_entity_count(self, db):
        run_id = "run-crd-1"
        start_pipeline_run(db, run_id, SURFACE_ID, NOW)
        for i in range(3):
            _seed_entity_with_scores(
                db, run_id, f"skill:alice/repo{i}", f"Skill {i}", "backend",
                _full_scores(),
            )
        complete_pipeline_run(db, run_id, NOW, stats={"repos_discovered": 3})

        data = collect_run_data(db, run_id)
        assert len(data["entity_scores"]) == 3

    def test_run_meta_populated(self, db):
        run_id = "run-crd-2"
        start_pipeline_run(db, run_id, SURFACE_ID, NOW)
        complete_pipeline_run(db, run_id, NOW,
                              stats={"repos_discovered": 5, "valid_skills": 3})

        data = collect_run_data(db, run_id)
        assert data["run_meta"]["run_id"] == run_id
        assert data["run_meta"]["stats"]["repos_discovered"] == 5

    def test_entity_meta_category(self, db):
        run_id = "run-crd-3"
        start_pipeline_run(db, run_id, SURFACE_ID, NOW)
        _seed_entity_with_scores(
            db, run_id, "skill:alice/ml", "ML Skill", "data_ai", _full_scores()
        )

        data = collect_run_data(db, run_id)
        assert data["entity_meta"]["skill:alice/ml"]["category"] == "data_ai"

    def test_empty_run_returns_empty_scores(self, db):
        run_id = "run-crd-empty"
        start_pipeline_run(db, run_id, SURFACE_ID, NOW)

        data = collect_run_data(db, run_id)
        assert data["entity_scores"] == {}


# ---------------------------------------------------------------------------
# generate_report — section presence
# ---------------------------------------------------------------------------

def _make_data(entities: list[dict]) -> dict:
    """Build a data dict from a list of {id, name, category, scores} dicts."""
    entity_scores = {}
    entity_meta   = {}
    for e in entities:
        entity_scores[e["id"]] = e["scores"]
        entity_meta[e["id"]]   = {
            "name":     e["name"],
            "category": e["category"],
            "metadata": {},
        }
    return {
        "run_meta": {
            "run_id":       "test-run",
            "started_at":   NOW,
            "completed_at": NOW,
            "stats":        {"repos_discovered": len(entities)},
        },
        "entity_scores": entity_scores,
        "entity_meta":   entity_meta,
    }


class TestGenerateReport:

    def test_header_present(self):
        data   = _make_data([{"id": "skill:a/b", "name": "A", "category": "backend",
                               "scores": _full_scores()}])
        report = generate_report(data)
        assert "# Tessera — Seed Calibration Report" in report
        assert "test-run" in report

    def test_score_distributions_section(self):
        data   = _make_data([{"id": "skill:a/b", "name": "A", "category": "backend",
                               "scores": _full_scores()}])
        report = generate_report(data)
        assert "## Score Distributions" in report
        assert "velocity" in report
        assert "trending" in report

    def test_category_distribution_section(self):
        entities = [
            {"id": f"skill:a/repo{i}", "name": f"S{i}", "category": "backend",
             "scores": _full_scores()}
            for i in range(3)
        ]
        report = generate_report(_make_data(entities))
        assert "## Category Distribution" in report
        assert "backend" in report

    def test_top20_section_present(self):
        entities = [
            {"id": f"skill:a/r{i}", "name": f"Skill{i}", "category": "backend",
             "scores": _full_scores(trending=float(i))}
            for i in range(5)
        ]
        report = generate_report(_make_data(entities))
        assert "## Top 20 by Trending Score" in report

    def test_bottom20_section_present(self):
        entities = [
            {"id": f"skill:a/r{i}", "name": f"Skill{i}", "category": "backend",
             "scores": _full_scores(trending=float(i))}
            for i in range(5)
        ]
        report = generate_report(_make_data(entities))
        assert "## Bottom 20 by Trending Score" in report

    def test_anomaly_flags_section(self):
        report = generate_report(_make_data([
            {"id": "skill:a/b", "name": "A", "category": "other",
             "scores": _full_scores()}
        ]))
        assert "## Anomaly Flags" in report

    def test_monorepo_section(self):
        report = generate_report(_make_data([
            {"id": "skill:a/b", "name": "A", "category": "other",
             "scores": _full_scores()}
        ]))
        assert "Monorepo" in report

    def test_empty_run_graceful(self):
        data = {
            "run_meta": {"run_id": "empty", "started_at": NOW,
                         "completed_at": NOW, "stats": {}},
            "entity_scores": {},
            "entity_meta":   {},
        }
        report = generate_report(data)
        assert "No scored skills" in report

    def test_other_category_flag_when_over_20_percent(self):
        """When >20% of skills are in 'other', the report should flag it."""
        entities = (
            [{"id": f"skill:a/other{i}", "name": f"Other{i}", "category": "other",
              "scores": _full_scores()} for i in range(5)]
            +
            [{"id": f"skill:a/back{i}", "name": f"Back{i}", "category": "backend",
              "scores": _full_scores()} for i in range(1)]
        )
        report = generate_report(_make_data(entities))
        assert "target >20% exceeded" in report

    def test_other_category_no_flag_when_under_20_percent(self):
        entities = (
            [{"id": f"skill:a/other{i}", "name": f"Other{i}", "category": "other",
              "scores": _full_scores()} for i in range(1)]
            +
            [{"id": f"skill:a/back{i}", "name": f"Back{i}", "category": "backend",
              "scores": _full_scores()} for i in range(10)]
        )
        report = generate_report(_make_data(entities))
        assert "target >20% exceeded" not in report

    def test_high_anomaly_flagged(self):
        entities = [
            {"id": "skill:a/hot", "name": "Hot Skill", "category": "backend",
             "scores": _full_scores(trending=97.5)},
            {"id": "skill:a/avg", "name": "Avg Skill", "category": "backend",
             "scores": _full_scores(trending=50.0)},
        ]
        report = generate_report(_make_data(entities))
        assert "skill:a/hot" in report
        assert "97.5" in report

    def test_low_anomaly_flagged(self):
        entities = [
            {"id": "skill:a/low", "name": "Low Skill", "category": "other",
             "scores": _full_scores(trending=2.0)},
            {"id": "skill:a/avg", "name": "Avg Skill", "category": "backend",
             "scores": _full_scores(trending=50.0)},
        ]
        report = generate_report(_make_data(entities))
        assert "skill:a/low" in report

    def test_no_anomaly_shows_none(self):
        entities = [
            {"id": f"skill:a/r{i}", "name": f"S{i}", "category": "backend",
             "scores": _full_scores(trending=50.0)}
            for i in range(3)
        ]
        report = generate_report(_make_data(entities))
        assert "*None.*" in report

    def test_degenerate_flag_shown(self):
        """A dimension where all values are identical should be flagged."""
        entities = [
            {"id": f"skill:a/r{i}", "name": f"S{i}", "category": "backend",
             "scores": _full_scores(velocity=0.5)}  # all velocity = 0.5
            for i in range(10)
        ]
        report = generate_report(_make_data(entities))
        assert "⚠️ YES" in report

    def test_top_skill_ranked_first(self):
        entities = [
            {"id": "skill:a/best", "name": "Best Skill", "category": "backend",
             "scores": _full_scores(trending=90.0)},
            {"id": "skill:a/mid",  "name": "Mid Skill",  "category": "backend",
             "scores": _full_scores(trending=50.0)},
            {"id": "skill:a/low",  "name": "Low Skill",  "category": "other",
             "scores": _full_scores(trending=10.0)},
        ]
        report = generate_report(_make_data(entities))
        idx_best = report.index("Best Skill")
        idx_mid  = report.index("Mid Skill")
        assert idx_best < idx_mid  # best appears before mid in Top 20 table

    def test_monorepo_collection_counted(self):
        entities = [
            {"id": "skill:alice/mono:skills/a", "name": "Mono A",
             "category": "backend", "scores": _full_scores()},
            {"id": "skill:alice/mono:skills/b", "name": "Mono B",
             "category": "backend", "scores": _full_scores()},
            {"id": "skill:bob/solo", "name": "Solo",
             "category": "devops_infra", "scores": _full_scores()},
        ]
        report = generate_report(_make_data(entities))
        assert "alice/mono" in report
        assert "Collection repos" in report


# ---------------------------------------------------------------------------
# main() — end-to-end file write
# ---------------------------------------------------------------------------

class TestMain:

    def test_writes_report_file(self, tmp_path):
        db_file = str(tmp_path / "test.db")
        conn = get_connection(db_file)
        init_db(conn)
        upsert_signal_source(conn, SOURCE_ID, "GitHub API", last_run_at=NOW)

        run_id = "run-main-1"
        start_pipeline_run(conn, run_id, SURFACE_ID, NOW)
        _seed_entity_with_scores(
            conn, run_id, "skill:alice/repo", "Alice Skill", "backend",
            _full_scores(),
        )
        complete_pipeline_run(conn, run_id, NOW, stats={"repos_discovered": 1})

        out = tmp_path / "report.md"
        result = main(db_path=db_file, run_id=run_id, output_path=out)

        assert result == out
        assert out.exists()
        content = out.read_text()
        assert "Tessera" in content
        assert "Alice Skill" in content

    def test_raises_when_no_completed_run(self, tmp_path):
        db_file = str(tmp_path / "empty.db")
        conn = get_connection(db_file)
        init_db(conn)

        with pytest.raises(RuntimeError, match="No completed pipeline run"):
            main(db_path=db_file, output_path=tmp_path / "report.md")
