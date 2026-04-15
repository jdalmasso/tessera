"""
Tests for data/store.py — all CRUD operations use an in-memory SQLite database.
"""

import json
import pytest

from data.store import (
    complete_pipeline_run,
    get_connection,
    get_entity,
    get_known_repo_names,
    get_latest_completed_run,
    get_latest_scores,
    get_previous_completed_run,
    get_raw_signals,
    get_score_history,
    init_db,
    start_pipeline_run,
    store_raw_signal,
    store_score,
    upsert_entity,
    upsert_signal_source,
)
from data.models import RUN_STATUS_COMPLETED, RUN_STATUS_FAILED, RUN_STATUS_RUNNING


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def conn():
    """In-memory SQLite connection with schema initialised."""
    c = get_connection(":memory:")
    init_db(c)
    return c


@pytest.fixture
def seeded_conn(conn):
    """Connection pre-seeded with a signal source and one entity."""
    upsert_signal_source(conn, "github", "GitHub API")
    upsert_entity(
        conn,
        entity_id="skill:owner/repo",
        entity_type="skill",
        name="my-skill",
        description="A test skill",
        metadata={"repo_url": "https://github.com/owner/repo", "stars": 42},
        category="Backend",
        now="2026-04-01T00:00:00Z",
    )
    return conn


# ---------------------------------------------------------------------------
# init_db / get_connection
# ---------------------------------------------------------------------------

class TestInitDb:
    def test_creates_all_tables(self, conn):
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert {"signal_sources", "raw_signals", "entities", "scores", "pipeline_runs"} <= tables

    def test_wal_mode_enabled(self, tmp_path):
        # WAL mode only applies to file-based databases; :memory: stays in 'memory' mode.
        db_file = str(tmp_path / "test.db")
        c = get_connection(db_file)
        init_db(c)
        mode = c.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode == "wal"

    def test_idempotent(self, conn):
        """Calling init_db twice should not raise."""
        init_db(conn)  # second call
        tables = conn.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table'"
        ).fetchone()[0]
        assert tables >= 5

    def test_row_factory_set(self, conn):
        """Rows should be accessible by column name."""
        import sqlite3
        assert conn.row_factory is sqlite3.Row


# ---------------------------------------------------------------------------
# signal_sources
# ---------------------------------------------------------------------------

class TestUpsertSignalSource:
    def test_insert(self, conn):
        upsert_signal_source(conn, "github", "GitHub API")
        row = conn.execute("SELECT * FROM signal_sources WHERE id='github'").fetchone()
        assert row is not None
        assert row["name"] == "GitHub API"
        assert row["last_run_at"] is None

    def test_insert_with_last_run_at(self, conn):
        upsert_signal_source(conn, "github", "GitHub API", last_run_at="2026-04-01T00:00:00Z")
        row = conn.execute("SELECT * FROM signal_sources WHERE id='github'").fetchone()
        assert row["last_run_at"] == "2026-04-01T00:00:00Z"

    def test_update_on_conflict(self, conn):
        upsert_signal_source(conn, "github", "GitHub API")
        upsert_signal_source(conn, "github", "GitHub API v2", last_run_at="2026-04-02T00:00:00Z")
        row = conn.execute("SELECT * FROM signal_sources WHERE id='github'").fetchone()
        assert row["name"] == "GitHub API v2"
        assert row["last_run_at"] == "2026-04-02T00:00:00Z"


# ---------------------------------------------------------------------------
# entities
# ---------------------------------------------------------------------------

class TestUpsertEntity:
    def test_insert_new(self, conn):
        upsert_entity(
            conn, "skill:a/b", "skill", "my-skill", "desc",
            {"stars": 10}, "Backend", "2026-01-01T00:00:00Z",
        )
        row = get_entity(conn, "skill:a/b")
        assert row is not None
        assert row["name"] == "my-skill"
        assert row["category"] == "Backend"
        assert row["first_seen_at"] == "2026-01-01T00:00:00Z"
        assert row["updated_at"] == "2026-01-01T00:00:00Z"

    def test_metadata_roundtrip(self, conn):
        meta = {"repo_url": "https://github.com/a/b", "stars": 99, "is_collection": False}
        upsert_entity(conn, "skill:a/b", "skill", "x", None, meta, "Other", "2026-01-01T00:00:00Z")
        row = get_entity(conn, "skill:a/b")
        assert json.loads(row["metadata"]) == meta

    def test_description_nullable(self, conn):
        upsert_entity(conn, "skill:a/b", "skill", "x", None, {}, "Other", "2026-01-01T00:00:00Z")
        row = get_entity(conn, "skill:a/b")
        assert row["description"] is None

    def test_update_preserves_first_seen_at(self, conn):
        upsert_entity(conn, "skill:a/b", "skill", "v1", None, {}, "Other", "2026-01-01T00:00:00Z")
        upsert_entity(conn, "skill:a/b", "skill", "v2", "new desc", {"x": 1}, "Backend", "2026-04-01T00:00:00Z")
        row = get_entity(conn, "skill:a/b")
        assert row["first_seen_at"] == "2026-01-01T00:00:00Z"  # preserved
        assert row["updated_at"] == "2026-04-01T00:00:00Z"     # updated
        assert row["name"] == "v2"
        assert row["category"] == "Backend"

    def test_get_entity_missing(self, conn):
        assert get_entity(conn, "skill:does/not-exist") is None


# ---------------------------------------------------------------------------
# raw_signals
# ---------------------------------------------------------------------------

class TestRawSignals:
    def test_store_returns_id(self, seeded_conn):
        row_id = store_raw_signal(
            seeded_conn, "github", "repo_metadata", "skill:owner/repo",
            {"stars": 42}, "2026-04-01T00:00:00Z", "run-001",
        )
        assert isinstance(row_id, int)
        assert row_id >= 1

    def test_payload_roundtrip(self, seeded_conn):
        payload = {"stars": 42, "topics": ["claude-skill"], "nested": {"a": 1}}
        store_raw_signal(
            seeded_conn, "github", "repo_metadata", "skill:owner/repo",
            payload, "2026-04-01T00:00:00Z", "run-001",
        )
        rows = get_raw_signals(seeded_conn, "skill:owner/repo")
        assert len(rows) == 1
        assert json.loads(rows[0]["payload"]) == payload

    def test_multiple_signals_for_entity(self, seeded_conn):
        for i in range(3):
            store_raw_signal(
                seeded_conn, "github", f"type_{i}", "skill:owner/repo",
                {"i": i}, "2026-04-01T00:00:00Z", "run-001",
            )
        rows = get_raw_signals(seeded_conn, "skill:owner/repo")
        assert len(rows) == 3

    def test_filter_by_signal_type(self, seeded_conn):
        store_raw_signal(seeded_conn, "github", "repo_metadata", "skill:owner/repo",
                         {"a": 1}, "2026-04-01T00:00:00Z", "run-001")
        store_raw_signal(seeded_conn, "github", "skill_file", "skill:owner/repo",
                         {"b": 2}, "2026-04-01T00:00:00Z", "run-001")
        rows = get_raw_signals(seeded_conn, "skill:owner/repo", signal_type="skill_file")
        assert len(rows) == 1
        assert json.loads(rows[0]["payload"]) == {"b": 2}

    def test_no_signals_for_unknown_entity(self, seeded_conn):
        rows = get_raw_signals(seeded_conn, "skill:nobody/nothing")
        assert rows == []


# ---------------------------------------------------------------------------
# scores
# ---------------------------------------------------------------------------

class TestScores:
    def test_store_returns_id(self, seeded_conn):
        row_id = store_score(
            seeded_conn, "skill:owner/repo", "velocity", 0.85,
            "2026-04-01T00:00:00Z", "run-001",
        )
        assert isinstance(row_id, int)

    def test_details_roundtrip(self, seeded_conn):
        details = {"acceleration": 1.5, "consistency": 0.7}
        store_score(
            seeded_conn, "skill:owner/repo", "velocity", 0.85,
            "2026-04-01T00:00:00Z", "run-001", details=details,
        )
        rows = get_score_history(seeded_conn, "skill:owner/repo", "velocity")
        assert json.loads(rows[0]["details"]) == details

    def test_details_nullable(self, seeded_conn):
        store_score(seeded_conn, "skill:owner/repo", "adoption", 0.5,
                    "2026-04-01T00:00:00Z", "run-001")
        rows = get_score_history(seeded_conn, "skill:owner/repo", "adoption")
        assert rows[0]["details"] is None

    def test_get_latest_scores_ordered_by_value(self, seeded_conn):
        # Add a second entity
        upsert_entity(seeded_conn, "skill:other/repo", "skill", "other", None,
                      {}, "Backend", "2026-04-01T00:00:00Z")
        store_score(seeded_conn, "skill:owner/repo", "composite:trending", 75.0,
                    "2026-04-01T00:00:00Z", "run-001")
        store_score(seeded_conn, "skill:other/repo", "composite:trending", 90.0,
                    "2026-04-01T00:00:00Z", "run-001")

        rows = get_latest_scores(seeded_conn, "composite:trending", "run-001")
        assert len(rows) == 2
        assert rows[0]["entity_id"] == "skill:other/repo"  # higher score first
        assert rows[1]["entity_id"] == "skill:owner/repo"

    def test_get_latest_scores_includes_entity_fields(self, seeded_conn):
        store_score(seeded_conn, "skill:owner/repo", "composite:trending", 80.0,
                    "2026-04-01T00:00:00Z", "run-001")
        rows = get_latest_scores(seeded_conn, "composite:trending", "run-001")
        assert rows[0]["name"] == "my-skill"
        assert rows[0]["category"] == "Backend"

    def test_get_latest_scores_scoped_to_run(self, seeded_conn):
        store_score(seeded_conn, "skill:owner/repo", "composite:trending", 80.0,
                    "2026-04-01T00:00:00Z", "run-001")
        store_score(seeded_conn, "skill:owner/repo", "composite:trending", 85.0,
                    "2026-04-02T00:00:00Z", "run-002")
        rows = get_latest_scores(seeded_conn, "composite:trending", "run-001")
        assert len(rows) == 1
        assert rows[0]["value"] == 80.0

    def test_get_score_history_chronological(self, seeded_conn):
        store_score(seeded_conn, "skill:owner/repo", "velocity", 0.5,
                    "2026-04-01T00:00:00Z", "run-001")
        store_score(seeded_conn, "skill:owner/repo", "velocity", 0.7,
                    "2026-04-02T00:00:00Z", "run-002")
        store_score(seeded_conn, "skill:owner/repo", "velocity", 0.9,
                    "2026-04-03T00:00:00Z", "run-003")
        rows = get_score_history(seeded_conn, "skill:owner/repo", "velocity")
        assert [r["value"] for r in rows] == [0.5, 0.7, 0.9]

    def test_get_score_history_empty(self, seeded_conn):
        rows = get_score_history(seeded_conn, "skill:owner/repo", "velocity")
        assert rows == []

    def test_get_score_history_scoped_to_dimension(self, seeded_conn):
        store_score(seeded_conn, "skill:owner/repo", "velocity", 0.8,
                    "2026-04-01T00:00:00Z", "run-001")
        store_score(seeded_conn, "skill:owner/repo", "adoption", 0.6,
                    "2026-04-01T00:00:00Z", "run-001")
        rows = get_score_history(seeded_conn, "skill:owner/repo", "velocity")
        assert len(rows) == 1
        assert rows[0]["value"] == 0.8


# ---------------------------------------------------------------------------
# pipeline runs
# ---------------------------------------------------------------------------

class TestPipelineRuns:
    def test_start_creates_running_record(self, conn):
        start_pipeline_run(conn, "run-001", "skills_leaderboard", "2026-04-01T10:00:00Z")
        row = conn.execute("SELECT * FROM pipeline_runs WHERE id='run-001'").fetchone()
        assert row["status"] == RUN_STATUS_RUNNING
        assert row["completed_at"] is None
        assert row["surface_id"] == "skills_leaderboard"

    def test_complete_updates_record(self, conn):
        start_pipeline_run(conn, "run-001", "skills_leaderboard", "2026-04-01T10:00:00Z")
        complete_pipeline_run(conn, "run-001", "2026-04-01T11:30:00Z",
                              stats={"entities": 500, "errors": 3})
        row = conn.execute("SELECT * FROM pipeline_runs WHERE id='run-001'").fetchone()
        assert row["status"] == RUN_STATUS_COMPLETED
        assert row["completed_at"] == "2026-04-01T11:30:00Z"
        assert json.loads(row["stats"]) == {"entities": 500, "errors": 3}

    def test_complete_with_failed_status(self, conn):
        start_pipeline_run(conn, "run-001", "skills_leaderboard", "2026-04-01T10:00:00Z")
        complete_pipeline_run(conn, "run-001", "2026-04-01T10:05:00Z",
                              status=RUN_STATUS_FAILED)
        row = conn.execute("SELECT * FROM pipeline_runs WHERE id='run-001'").fetchone()
        assert row["status"] == RUN_STATUS_FAILED

    def test_get_latest_completed_run(self, conn):
        start_pipeline_run(conn, "run-001", "skills_leaderboard", "2026-04-01T10:00:00Z")
        complete_pipeline_run(conn, "run-001", "2026-04-01T11:00:00Z")
        start_pipeline_run(conn, "run-002", "skills_leaderboard", "2026-04-02T10:00:00Z")
        complete_pipeline_run(conn, "run-002", "2026-04-02T11:00:00Z")

        row = get_latest_completed_run(conn, "skills_leaderboard")
        assert row["id"] == "run-002"

    def test_get_latest_completed_run_ignores_running(self, conn):
        start_pipeline_run(conn, "run-001", "skills_leaderboard", "2026-04-01T10:00:00Z")
        complete_pipeline_run(conn, "run-001", "2026-04-01T11:00:00Z")
        start_pipeline_run(conn, "run-002", "skills_leaderboard", "2026-04-02T10:00:00Z")
        # run-002 never completed

        row = get_latest_completed_run(conn, "skills_leaderboard")
        assert row["id"] == "run-001"

    def test_get_latest_completed_run_none_when_empty(self, conn):
        assert get_latest_completed_run(conn, "skills_leaderboard") is None

    def test_get_previous_completed_run(self, conn):
        start_pipeline_run(conn, "run-001", "skills_leaderboard", "2026-04-01T10:00:00Z")
        complete_pipeline_run(conn, "run-001", "2026-04-01T11:00:00Z")
        start_pipeline_run(conn, "run-002", "skills_leaderboard", "2026-04-02T10:00:00Z")
        complete_pipeline_run(conn, "run-002", "2026-04-02T11:00:00Z")
        start_pipeline_run(conn, "run-003", "skills_leaderboard", "2026-04-03T10:00:00Z")
        complete_pipeline_run(conn, "run-003", "2026-04-03T11:00:00Z")

        prev = get_previous_completed_run(conn, "skills_leaderboard", "run-003")
        assert prev["id"] == "run-002"

    def test_get_previous_completed_run_none_for_first(self, conn):
        start_pipeline_run(conn, "run-001", "skills_leaderboard", "2026-04-01T10:00:00Z")
        complete_pipeline_run(conn, "run-001", "2026-04-01T11:00:00Z")
        prev = get_previous_completed_run(conn, "skills_leaderboard", "run-001")
        assert prev is None

    def test_surface_isolation(self, conn):
        """Runs from different surfaces should not interfere."""
        start_pipeline_run(conn, "run-A", "surface_a", "2026-04-01T10:00:00Z")
        complete_pipeline_run(conn, "run-A", "2026-04-01T11:00:00Z")
        start_pipeline_run(conn, "run-B", "surface_b", "2026-04-02T10:00:00Z")
        complete_pipeline_run(conn, "run-B", "2026-04-02T11:00:00Z")

        assert get_latest_completed_run(conn, "surface_a")["id"] == "run-A"
        assert get_latest_completed_run(conn, "surface_b")["id"] == "run-B"
        assert get_latest_completed_run(conn, "surface_c") is None


# ---------------------------------------------------------------------------
# get_known_repo_names
# ---------------------------------------------------------------------------

def _add_skill(conn, entity_id, name="test-skill"):
    """Helper: insert a skill entity with minimal required fields."""
    upsert_entity(
        conn,
        entity_id=entity_id,
        entity_type="skill",
        name=name,
        description=None,
        metadata={},
        category="other",
        now="2026-04-14T00:00:00Z",
    )


class TestGetKnownRepoNames:

    def test_empty_db_returns_empty_set(self, conn):
        assert get_known_repo_names(conn) == set()

    def test_single_root_skill(self, conn):
        _add_skill(conn, "skill:owner/repo")
        assert get_known_repo_names(conn) == {"owner/repo"}

    def test_monorepo_multiple_entity_ids_deduplicated(self, conn):
        """Three sub-skills for the same repo should collapse to one entry."""
        _add_skill(conn, "skill:owner/monorepo:path/a", name="skill-a")
        _add_skill(conn, "skill:owner/monorepo:path/b", name="skill-b")
        _add_skill(conn, "skill:owner/monorepo:path/c", name="skill-c")
        result = get_known_repo_names(conn)
        assert result == {"owner/monorepo"}

    def test_multiple_distinct_repos(self, conn):
        _add_skill(conn, "skill:alice/foo")
        _add_skill(conn, "skill:bob/bar")
        _add_skill(conn, "skill:carol/baz")
        assert get_known_repo_names(conn) == {"alice/foo", "bob/bar", "carol/baz"}

    def test_non_skill_entity_type_excluded(self, conn):
        """Entities that are not 'skill' type should not appear in results."""
        upsert_entity(
            conn,
            entity_id="tool:owner/some-tool",
            entity_type="tool",
            name="some-tool",
            description=None,
            metadata={},
            category="other",
            now="2026-04-14T00:00:00Z",
        )
        assert get_known_repo_names(conn) == set()

    def test_mixed_root_and_subpath_same_repo(self, conn):
        """Root skill + sub-skill in same repo → single entry."""
        _add_skill(conn, "skill:owner/repo", name="root")
        _add_skill(conn, "skill:owner/repo:.claude/skills", name="sub")
        assert get_known_repo_names(conn) == {"owner/repo"}
