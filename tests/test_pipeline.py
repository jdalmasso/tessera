"""
End-to-end pipeline tests for score_and_store_skills().

Strategy: seed an in-memory DB with raw signals (as if ingest_repo() had
already run), then call score_and_store_skills() and assert that every
entity has all 9 expected score records (6 dimensions + 3 composites).

No GitHub API calls are made — the scoring pass reads only from the DB.
"""

import pytest

from data.store import (
    get_connection,
    init_db,
    start_pipeline_run,
    store_raw_signal,
    upsert_signal_source,
)
from surfaces.skills_leaderboard.pipeline import (
    load_config,
    score_and_store_skills,
    _repo_from_entity_ref,
    _days_between,
    _compute_corpus_max,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SOURCE_ID = "github"
SURFACE_ID = "skills_leaderboard"
NOW = "2026-04-12T10:00:00Z"
CREATED_OLD = "2025-01-01T00:00:00Z"   # > 30 days ago → mature repo
PUSHED_RECENT = "2026-04-01T00:00:00Z"  # 11 days ago → fresh

def _expected_dimensions() -> set:
    """Derive expected score dimensions from the real config (6 dims + N composites)."""
    cfg = load_config()
    base = {"velocity", "adoption", "freshness", "documentation", "contributors", "code_quality"}
    composites = {f"composite:{m}" for m in cfg["scoring"]["methodologies"].keys()}
    return base | composites


EXPECTED_DIMENSIONS = _expected_dimensions()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db(tmp_path):
    """Provide a fresh, initialised SQLite DB with the signal source seeded."""
    conn = get_connection(str(tmp_path / "test.db"))
    init_db(conn)
    upsert_signal_source(conn, SOURCE_ID, "GitHub API", last_run_at=NOW)
    return conn


@pytest.fixture
def config():
    return load_config()


def _seed_repo(conn, run_id, full_name, *, stars=50, forks=10, watchers=8,
               topics=None, has_license=True, created_at=CREATED_OLD,
               pushed_at=PUSHED_RECENT, commit_30d=4, commit_prev=2,
               commit_90d=10, weeks=6, contributor_count=2,
               has_gitignore=True, has_github_dir=True, has_tests=True):
    """Seed all repo-level raw signals for one repo."""
    store_raw_signal(conn, SOURCE_ID, "repo_metadata", full_name, {
        "stars": stars, "forks": forks, "watchers": watchers,
        "is_fork": False, "is_archived": False,
        "created_at": created_at, "pushed_at": pushed_at,
        "topics": topics or [],
        "has_license": has_license,
        "default_branch": "main",
    }, NOW, run_id)
    store_raw_signal(conn, SOURCE_ID, "code_quality", full_name, {
        "has_gitignore": has_gitignore,
        "has_github_dir": has_github_dir,
        "has_tests": has_tests,
    }, NOW, run_id)
    store_raw_signal(conn, SOURCE_ID, "commits", full_name, {
        "commit_count_30d": commit_30d,
        "commit_count_prev_30d": commit_prev,
        "commit_count_90d": commit_90d,
        "unique_commit_weeks_90d": weeks,
    }, NOW, run_id)
    store_raw_signal(conn, SOURCE_ID, "contributors", full_name, {
        "contributor_count": contributor_count,
    }, NOW, run_id)


def _seed_skill(conn, run_id, entity_ref, skill_path="SKILL.md", *,
                name="Test Skill", description="A backend API skill",
                category=None, tags=None, line_count=150,
                has_frontmatter=True, has_usage=True, has_examples=True,
                has_readme=True, has_scripts=False, has_references=False):
    """Seed one skill_file raw signal."""
    store_raw_signal(conn, SOURCE_ID, "skill_file", entity_ref, {
        "skill_path": skill_path,
        "char_count": 800,
        "line_count": line_count,
        "has_frontmatter": has_frontmatter,
        "frontmatter_name": name,
        "frontmatter_description": description,
        "frontmatter_category": category,
        "frontmatter_tags": tags or [],
        "has_usage_section": has_usage,
        "has_examples_section": has_examples,
        "has_readme": has_readme,
        "has_scripts_dir": has_scripts,
        "has_references_dir": has_references,
    }, NOW, run_id)


# ---------------------------------------------------------------------------
# Helper unit tests
# ---------------------------------------------------------------------------

class TestHelpers:

    def test_repo_from_entity_ref_root(self):
        assert _repo_from_entity_ref("skill:owner/repo") == "owner/repo"

    def test_repo_from_entity_ref_monorepo(self):
        assert _repo_from_entity_ref("skill:owner/repo:skills/backend") == "owner/repo"

    def test_days_between_known_values(self):
        assert _days_between("2026-01-01T00:00:00Z", "2026-04-12T00:00:00Z") == 101

    def test_days_between_bad_input_returns_default(self):
        assert _days_between("not-a-date", "2026-04-12T00:00:00Z", default=99) == 99

    def test_days_between_same_day(self):
        assert _days_between(NOW, NOW) == 0

    def test_compute_corpus_max(self, db):
        run_id = "run-corpus"
        start_pipeline_run(db, run_id, SURFACE_ID, NOW)
        store_raw_signal(db, SOURCE_ID, "repo_metadata", "a/b", {
            "stars": 500, "forks": 100, "watchers": 50,
        }, NOW, run_id)
        store_raw_signal(db, SOURCE_ID, "repo_metadata", "c/d", {
            "stars": 200, "forks": 300, "watchers": 80,
        }, NOW, run_id)
        ms, mf, mw = _compute_corpus_max(db, run_id)
        assert ms == 500
        assert mf == 300
        assert mw == 80

    def test_compute_corpus_max_empty_run(self, db):
        run_id = "run-empty"
        start_pipeline_run(db, run_id, SURFACE_ID, NOW)
        ms, mf, mw = _compute_corpus_max(db, run_id)
        # Falls back to 1 so log-normalisation never divides by zero
        assert ms == mf == mw == 1


# ---------------------------------------------------------------------------
# Core: single skill, single repo
# ---------------------------------------------------------------------------

class TestSingleSkill:

    def test_returns_scored_count(self, db, config):
        run_id = "run-single"
        start_pipeline_run(db, run_id, SURFACE_ID, NOW)
        full_name = "alice/myskill"
        entity_ref = f"skill:{full_name}"
        _seed_repo(db, run_id, full_name)
        _seed_skill(db, run_id, entity_ref)

        count = score_and_store_skills(db, run_id, config)
        assert count == 1

    def test_all_nine_dimensions_stored(self, db, config):
        run_id = "run-nine"
        start_pipeline_run(db, run_id, SURFACE_ID, NOW)
        full_name = "alice/myskill"
        entity_ref = f"skill:{full_name}"
        _seed_repo(db, run_id, full_name)
        _seed_skill(db, run_id, entity_ref)

        score_and_store_skills(db, run_id, config)

        rows = db.execute(
            "SELECT dimension FROM scores WHERE entity_id = ? AND run_id = ?",
            (entity_ref, run_id),
        ).fetchall()
        stored = {r["dimension"] for r in rows}
        assert stored == EXPECTED_DIMENSIONS

    def test_entity_upserted(self, db, config):
        run_id = "run-entity"
        start_pipeline_run(db, run_id, SURFACE_ID, NOW)
        full_name = "alice/myskill"
        entity_ref = f"skill:{full_name}"
        _seed_repo(db, run_id, full_name)
        _seed_skill(db, run_id, entity_ref, name="My Skill",
                    description="A backend API skill")

        score_and_store_skills(db, run_id, config)

        entity = db.execute(
            "SELECT * FROM entities WHERE id = ?", (entity_ref,)
        ).fetchone()
        assert entity is not None
        assert entity["name"] == "My Skill"
        assert entity["entity_type"] == "skill"

    def test_dimension_scores_bounded_0_to_1(self, db, config):
        run_id = "run-bounds-dim"
        start_pipeline_run(db, run_id, SURFACE_ID, NOW)
        full_name = "alice/boundsrepo"
        entity_ref = f"skill:{full_name}"
        _seed_repo(db, run_id, full_name)
        _seed_skill(db, run_id, entity_ref)

        score_and_store_skills(db, run_id, config)

        rows = db.execute(
            "SELECT dimension, value FROM scores WHERE entity_id = ? AND run_id = ?",
            (entity_ref, run_id),
        ).fetchall()
        for row in rows:
            if not row["dimension"].startswith("composite:"):
                assert 0.0 <= row["value"] <= 1.0, (
                    f"{row['dimension']} = {row['value']} out of [0,1]"
                )

    def test_composite_scores_bounded_0_to_100(self, db, config):
        run_id = "run-bounds-comp"
        start_pipeline_run(db, run_id, SURFACE_ID, NOW)
        full_name = "alice/boundsrepo2"
        entity_ref = f"skill:{full_name}"
        _seed_repo(db, run_id, full_name)
        _seed_skill(db, run_id, entity_ref)

        score_and_store_skills(db, run_id, config)

        rows = db.execute(
            "SELECT dimension, value FROM scores WHERE entity_id = ? AND run_id = ?",
            (entity_ref, run_id),
        ).fetchall()
        for row in rows:
            if row["dimension"].startswith("composite:"):
                assert 0.0 <= row["value"] <= 100.0, (
                    f"{row['dimension']} = {row['value']} out of [0,100]"
                )


# ---------------------------------------------------------------------------
# Categorisation wired through pipeline
# ---------------------------------------------------------------------------

class TestCategorization:

    def test_frontmatter_category_used(self, db, config):
        run_id = "run-cat-fm"
        start_pipeline_run(db, run_id, SURFACE_ID, NOW)
        full_name = "alice/catrepo"
        entity_ref = f"skill:{full_name}"
        _seed_repo(db, run_id, full_name)
        _seed_skill(db, run_id, entity_ref, category="devops_infra",
                    description="something unrelated")

        score_and_store_skills(db, run_id, config)

        entity = db.execute(
            "SELECT category FROM entities WHERE id = ?", (entity_ref,)
        ).fetchone()
        assert entity["category"] == "devops_infra"

    def test_keyword_fallback_category(self, db, config):
        run_id = "run-cat-kw"
        start_pipeline_run(db, run_id, SURFACE_ID, NOW)
        full_name = "alice/apirepo"
        entity_ref = f"skill:{full_name}"
        _seed_repo(db, run_id, full_name, topics=["rest", "api", "backend"])
        _seed_skill(db, run_id, entity_ref, category=None,
                    description="REST API backend server with FastAPI")

        score_and_store_skills(db, run_id, config)

        entity = db.execute(
            "SELECT category FROM entities WHERE id = ?", (entity_ref,)
        ).fetchone()
        assert entity["category"] == "backend"

    def test_unknown_falls_back_to_other(self, db, config):
        run_id = "run-cat-other"
        start_pipeline_run(db, run_id, SURFACE_ID, NOW)
        full_name = "alice/mystery"
        entity_ref = f"skill:{full_name}"
        _seed_repo(db, run_id, full_name, topics=[])
        _seed_skill(db, run_id, entity_ref, category=None,
                    name="zzz xyz 123",
                    description="zzz xyz nonexistent gibberish 123 zzz")

        score_and_store_skills(db, run_id, config)

        entity = db.execute(
            "SELECT category FROM entities WHERE id = ?", (entity_ref,)
        ).fetchone()
        assert entity["category"] == "other"


# ---------------------------------------------------------------------------
# Multiple entities
# ---------------------------------------------------------------------------

class TestMultipleEntities:

    def test_two_skills_two_repos_both_scored(self, db, config):
        run_id = "run-two"
        start_pipeline_run(db, run_id, SURFACE_ID, NOW)
        for full_name in ("alice/repo1", "bob/repo2"):
            entity_ref = f"skill:{full_name}"
            _seed_repo(db, run_id, full_name)
            _seed_skill(db, run_id, entity_ref)

        count = score_and_store_skills(db, run_id, config)
        assert count == 2

        for full_name in ("alice/repo1", "bob/repo2"):
            entity_ref = f"skill:{full_name}"
            rows = db.execute(
                "SELECT dimension FROM scores WHERE entity_id = ? AND run_id = ?",
                (entity_ref, run_id),
            ).fetchall()
            assert {r["dimension"] for r in rows} == EXPECTED_DIMENSIONS

    def test_monorepo_two_skills_both_scored(self, db, config):
        """Two SKILL.md files in one repo — both should be scored."""
        run_id = "run-mono"
        start_pipeline_run(db, run_id, SURFACE_ID, NOW)
        full_name = "alice/monorepo"
        _seed_repo(db, run_id, full_name, stars=200)

        ref_a = f"skill:{full_name}:skills/backend"
        ref_b = f"skill:{full_name}:skills/frontend"
        _seed_skill(db, run_id, ref_a, skill_path="skills/backend/SKILL.md",
                    name="Backend Skill")
        _seed_skill(db, run_id, ref_b, skill_path="skills/frontend/SKILL.md",
                    name="Frontend Skill")

        count = score_and_store_skills(db, run_id, config)
        assert count == 2

        for ref in (ref_a, ref_b):
            rows = db.execute(
                "SELECT dimension FROM scores WHERE entity_id = ? AND run_id = ?",
                (ref, run_id),
            ).fetchall()
            assert {r["dimension"] for r in rows} == EXPECTED_DIMENSIONS

    def test_monorepo_adoption_dampened_vs_solo(self, db, config):
        """
        The adoption score for a skill in a 2-skill monorepo should be
        lower than for a solo repo with the same star count.
        """
        run_id = "run-damp"
        start_pipeline_run(db, run_id, SURFACE_ID, NOW)

        # Solo repo — 1 skill
        _seed_repo(db, run_id, "alice/solo", stars=100)
        _seed_skill(db, run_id, "skill:alice/solo")

        # Monorepo — 2 skills, same star count
        _seed_repo(db, run_id, "alice/mono", stars=100)
        _seed_skill(db, run_id, "skill:alice/mono:skills/a",
                    skill_path="skills/a/SKILL.md")
        _seed_skill(db, run_id, "skill:alice/mono:skills/b",
                    skill_path="skills/b/SKILL.md")

        score_and_store_skills(db, run_id, config)

        solo_adop = db.execute(
            "SELECT value FROM scores WHERE entity_id = ? AND dimension = 'adoption' AND run_id = ?",
            ("skill:alice/solo", run_id),
        ).fetchone()["value"]

        mono_adop = db.execute(
            "SELECT value FROM scores WHERE entity_id = ? AND dimension = 'adoption' AND run_id = ?",
            ("skill:alice/mono:skills/a", run_id),
        ).fetchone()["value"]

        assert mono_adop < solo_adop, (
            f"Monorepo adoption ({mono_adop:.4f}) should be less than solo ({solo_adop:.4f})"
        )


# ---------------------------------------------------------------------------
# Graceful degradation
# ---------------------------------------------------------------------------

class TestGracefulDegradation:

    def test_missing_repo_signals_skips_skill(self, db, config):
        """A skill_file signal with no matching repo signals is silently skipped."""
        run_id = "run-skip"
        start_pipeline_run(db, run_id, SURFACE_ID, NOW)
        # Seed only the skill_file signal — no repo_metadata / commits / etc.
        _seed_skill(db, run_id, "skill:ghost/repo")

        count = score_and_store_skills(db, run_id, config)
        assert count == 0

    def test_empty_run_returns_zero(self, db, config):
        run_id = "run-empty2"
        start_pipeline_run(db, run_id, SURFACE_ID, NOW)

        count = score_and_store_skills(db, run_id, config)
        assert count == 0

    def test_good_and_bad_skill_counts_only_good(self, db, config):
        """One complete skill + one with missing repo signals → only 1 scored."""
        run_id = "run-mixed"
        start_pipeline_run(db, run_id, SURFACE_ID, NOW)

        # Good skill
        _seed_repo(db, run_id, "alice/good")
        _seed_skill(db, run_id, "skill:alice/good")

        # Bad skill — no repo signals
        _seed_skill(db, run_id, "skill:bob/ghost")

        count = score_and_store_skills(db, run_id, config)
        assert count == 1


# ---------------------------------------------------------------------------
# _compute_commit_windows
# ---------------------------------------------------------------------------

class TestComputeCommitWindows:
    """Tests for _compute_commit_windows — now timezone-aware throughout."""

    def setup_method(self):
        from surfaces.skills_leaderboard.pipeline import _compute_commit_windows
        self._fn = _compute_commit_windows

    def _make_commit(self, iso_date: str) -> dict:
        return {"commit": {"author": {"date": iso_date}}}

    def test_empty_commits_returns_zeros(self):
        result = self._fn([])
        assert result == {
            "commit_count_30d": 0,
            "commit_count_prev_30d": 0,
            "commit_count_90d": 0,
            "unique_commit_weeks_90d": 0,
        }

    def test_recent_commit_counted_in_30d(self):
        from surfaces.skills_leaderboard.pipeline import _utcnow
        import datetime
        recent = (_utcnow() - datetime.timedelta(days=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
        result = self._fn([self._make_commit(recent)])
        assert result["commit_count_30d"] == 1
        assert result["commit_count_90d"] == 1

    def test_old_commit_not_in_30d_but_in_90d(self):
        from surfaces.skills_leaderboard.pipeline import _utcnow
        import datetime
        old = (_utcnow() - datetime.timedelta(days=45)).strftime("%Y-%m-%dT%H:%M:%SZ")
        result = self._fn([self._make_commit(old)])
        assert result["commit_count_30d"] == 0
        assert result["commit_count_prev_30d"] == 1
        assert result["commit_count_90d"] == 1

    def test_aware_iso_string_with_offset_handled(self):
        """GitHub may return '+00:00' suffix — must not crash or be wrong."""
        from surfaces.skills_leaderboard.pipeline import _utcnow
        import datetime
        recent = (_utcnow() - datetime.timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%S+00:00")
        result = self._fn([self._make_commit(recent)])
        assert result["commit_count_30d"] == 1

    def test_malformed_date_skipped(self):
        result = self._fn([{"commit": {"author": {"date": "not-a-date"}}}])
        assert result["commit_count_90d"] == 0

    def test_unique_weeks_counted(self):
        from surfaces.skills_leaderboard.pipeline import _utcnow
        import datetime
        now = _utcnow()
        commits = [
            self._make_commit((now - datetime.timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%SZ")),
            self._make_commit((now - datetime.timedelta(days=3)).strftime("%Y-%m-%dT%H:%M:%SZ")),
            self._make_commit((now - datetime.timedelta(days=10)).strftime("%Y-%m-%dT%H:%M:%SZ")),
        ]
        result = self._fn(commits)
        # days 2 and 3 are in the same ISO week; day 10 is in a different week
        assert result["unique_commit_weeks_90d"] == 2


# ---------------------------------------------------------------------------
# _find_skill_paths
# ---------------------------------------------------------------------------

class TestFindSkillPaths:
    def setup_method(self):
        from surfaces.skills_leaderboard.pipeline import _find_skill_paths
        self._fn = _find_skill_paths

    def _make_client(self, contents):
        from unittest.mock import MagicMock
        client = MagicMock()
        client.get_contents.return_value = contents
        return client

    def test_finds_skill_md_in_root(self):
        client = self._make_client([
            {"name": "SKILL.md", "type": "file", "path": "SKILL.md"},
            {"name": "README.md", "type": "file", "path": "README.md"},
        ])
        result = self._fn(client, "alice", "repo")
        assert result == ["SKILL.md"]

    def test_case_insensitive_match(self):
        client = self._make_client([
            {"name": "skill.md", "type": "file", "path": "skill.md"},
        ])
        result = self._fn(client, "alice", "repo")
        assert result == ["skill.md"]

    def test_skips_directories(self):
        client = self._make_client([
            {"name": "SKILL.md", "type": "dir", "path": "SKILL.md"},
        ])
        result = self._fn(client, "alice", "repo")
        assert result == []

    def test_no_skill_md_returns_empty(self):
        client = self._make_client([
            {"name": "README.md", "type": "file", "path": "README.md"},
        ])
        result = self._fn(client, "alice", "repo")
        assert result == []

    def test_non_list_contents_returns_empty(self):
        client = self._make_client(None)
        result = self._fn(client, "alice", "repo")
        assert result == []

    def test_uses_provided_root_contents(self):
        from unittest.mock import MagicMock
        client = MagicMock()
        root = [{"name": "SKILL.md", "type": "file", "path": "SKILL.md"}]
        result = self._fn(client, "alice", "repo", root_contents=root)
        client.get_contents.assert_not_called()
        assert result == ["SKILL.md"]


# ---------------------------------------------------------------------------
# _parse_iso
# ---------------------------------------------------------------------------

class TestParseIso:
    def setup_method(self):
        from surfaces.skills_leaderboard.pipeline import _parse_iso
        self._fn = _parse_iso

    def test_parses_z_suffix(self):
        result = self._fn("2026-04-12T10:00:00Z")
        assert result.year == 2026
        assert result.month == 4
        assert result.day == 12

    def test_result_is_naive(self):
        result = self._fn("2026-04-12T10:00:00Z")
        assert result.tzinfo is None

    def test_parses_offset_string(self):
        result = self._fn("2026-04-12T10:00:00+00:00")
        assert result.year == 2026


# ---------------------------------------------------------------------------
# ingest_repo
# ---------------------------------------------------------------------------

class TestIngestRepo:
    """
    Tests for ingest_repo() using a mocked GitHubClient and in-memory DB.
    Verifies that raw signals are stored correctly without hitting the network.
    """

    # Must be >= 100 chars to pass is_valid_skill()
    _DEFAULT_SKILL = (
        "---\nname: Test Skill\ndescription: A test skill for unit tests\n---\n"
        "## Usage\nRun this skill to do something useful in your project.\n"
        "It demonstrates the ingestion pipeline working end-to-end.\n"
    )

    def _make_client(self, skill_content=None):
        from unittest.mock import MagicMock
        client = MagicMock()
        client.get_contents.return_value = [
            {"name": "SKILL.md", "type": "file", "path": "SKILL.md"},
            {"name": "README.md", "type": "file", "path": "README.md"},
        ]
        client.get_commits.return_value = []
        client.get_contributors.return_value = [{"login": "alice"}, {"login": "bob"}]
        client.get_file_content.return_value = skill_content if skill_content is not None else self._DEFAULT_SKILL
        return client

    def _make_conn(self, run_id="run-1"):
        from data.store import get_connection, init_db, upsert_signal_source, start_pipeline_run
        conn = get_connection()
        init_db(conn)
        upsert_signal_source(conn, "github", "GitHub API", last_run_at=NOW)
        start_pipeline_run(conn, run_id, SURFACE_ID, NOW)
        return conn

    def _make_repo_data(self, full_name="alice/repo"):
        return {
            "full_name": full_name,
            "stargazers_count": 42,
            "forks_count": 5,
            "watchers_count": 10,
            "fork": False,
            "archived": False,
            "created_at": "2025-01-01T00:00:00Z",
            "pushed_at": "2026-04-01T00:00:00Z",
            "topics": ["claude-skill"],
            "license": {"spdx_id": "MIT"},
            "default_branch": "main",
        }

    def test_returns_skill_count(self, config):
        conn = self._make_conn()
        client = self._make_client()
        from surfaces.skills_leaderboard.pipeline import ingest_repo
        count = ingest_repo(client, self._make_repo_data(), [], config, "run-1", conn)
        assert count == 1

    def test_stores_repo_metadata_signal(self, config):
        import json
        from data.store import get_raw_signals
        conn = self._make_conn()
        client = self._make_client()
        from surfaces.skills_leaderboard.pipeline import ingest_repo
        ingest_repo(client, self._make_repo_data(), [], config, "run-1", conn)
        signals = get_raw_signals(conn, "alice/repo", "repo_metadata")
        assert len(signals) == 1
        assert json.loads(signals[0]["payload"])["stars"] == 42

    def test_stores_contributor_signal(self, config):
        import json
        from data.store import get_raw_signals
        conn = self._make_conn()
        client = self._make_client()
        from surfaces.skills_leaderboard.pipeline import ingest_repo
        ingest_repo(client, self._make_repo_data(), [], config, "run-1", conn)
        signals = get_raw_signals(conn, "alice/repo", "contributors")
        assert json.loads(signals[0]["payload"])["contributor_count"] == 2

    def test_invalid_skill_content_not_stored(self, config):
        conn = self._make_conn()
        client = self._make_client(skill_content="no frontmatter here")
        from surfaces.skills_leaderboard.pipeline import ingest_repo
        count = ingest_repo(client, self._make_repo_data(), [], config, "run-1", conn)
        assert count == 0
        # Commits and contributors must NOT be fetched when no valid skill exists
        client.get_commits.assert_not_called()
        client.get_contributors.assert_not_called()

    def test_valid_skill_fetches_commits_and_contributors(self, config):
        """Commits and contributors are fetched only when ≥1 valid skill is found."""
        conn = self._make_conn()
        client = self._make_client()  # default content is valid
        from surfaces.skills_leaderboard.pipeline import ingest_repo
        count = ingest_repo(client, self._make_repo_data(), [], config, "run-1", conn)
        assert count == 1
        client.get_commits.assert_called_once()
        client.get_contributors.assert_called_once()

    def test_provided_skill_paths_used(self, config):
        conn = self._make_conn()
        client = self._make_client()
        from surfaces.skills_leaderboard.pipeline import ingest_repo
        count = ingest_repo(client, self._make_repo_data(), ["SKILL.md"], config, "run-1", conn)
        assert count == 1
        # Commits and contributors fetched because skill is valid
        client.get_commits.assert_called_once()
        client.get_contributors.assert_called_once()
