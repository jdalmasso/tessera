"""
Tests for signals/github/discovery.py

Unit tests use a mock GitHubClient. The integration test at the bottom
requires a live GITHUB_TOKEN and is skipped otherwise.
"""

import os
from unittest.mock import MagicMock

import pytest

from signals.github.client import GitHubClient
from signals.github.discovery import (
    DiscoveredRepo,
    discover,
    is_monorepo,
    make_entity_id,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

MINIMAL_CONFIG = {
    "phase_1": {
        "queries": [
            {"query": "topic:claude-skill", "sort": "stars"},
        ]
    },
    "phase_2": {
        "queries": [
            {"query": "filename:SKILL.md"},
        ]
    },
    "filters": {},
}


def make_mock_client(
    search_repos_results: list = None,
    search_code_results: list = None,
) -> MagicMock:
    """Return a mock GitHubClient with pre-configured return values."""
    client = MagicMock(spec=GitHubClient)
    client.search_repos.return_value = search_repos_results or []
    client.search_code.return_value = search_code_results or []
    return client


def repo_item(full_name: str) -> dict:
    """Minimal repo search result item."""
    return {"full_name": full_name, "stargazers_count": 0}


def code_item(full_name: str, path: str) -> dict:
    """Minimal code search result item."""
    return {
        "path": path,
        "repository": {"full_name": full_name},
    }


# ---------------------------------------------------------------------------
# Phase 1 — topic search
# ---------------------------------------------------------------------------

class TestPhase1:
    def test_repos_added_from_topic_search(self):
        client = make_mock_client(
            search_repos_results=[repo_item("owner/repo-a"), repo_item("owner/repo-b")]
        )
        results = discover(client, MINIMAL_CONFIG)
        full_names = {r.full_name for r in results}
        assert "owner/repo-a" in full_names
        assert "owner/repo-b" in full_names

    def test_phase1_repos_have_empty_skill_paths(self):
        client = make_mock_client(search_repos_results=[repo_item("owner/repo-a")])
        results = discover(client, MINIMAL_CONFIG)
        match = next(r for r in results if r.full_name == "owner/repo-a")
        assert match.skill_paths == []
        assert match.discovery_source == "topic_search"

    def test_multiple_phase1_queries_deduplicated(self):
        config = {
            "phase_1": {
                "queries": [
                    {"query": "topic:claude-skill", "sort": "stars"},
                    {"query": "topic:agent-skill", "sort": "stars"},
                ]
            },
            "phase_2": {"queries": []},
        }
        client = MagicMock(spec=GitHubClient)
        # Same repo appears in both queries
        client.search_repos.side_effect = [
            [repo_item("owner/repo-a")],
            [repo_item("owner/repo-a"), repo_item("owner/repo-b")],
        ]
        results = discover(client, config)
        assert len(results) == 2  # deduped, not 3

    def test_repos_with_missing_full_name_skipped(self):
        client = make_mock_client(
            search_repos_results=[{"stargazers_count": 0}]  # no full_name key
        )
        results = discover(client, MINIMAL_CONFIG)
        assert results == []


# ---------------------------------------------------------------------------
# Phase 2 — code search
# ---------------------------------------------------------------------------

class TestPhase2:
    def test_repos_added_from_code_search(self):
        client = make_mock_client(
            search_code_results=[code_item("owner/new-repo", "SKILL.md")]
        )
        results = discover(client, MINIMAL_CONFIG)
        match = next(r for r in results if r.full_name == "owner/new-repo")
        assert match.skill_paths == ["SKILL.md"]
        assert match.discovery_source == "code_search"

    def test_code_search_results_with_missing_fields_skipped(self):
        client = make_mock_client(
            search_code_results=[
                {"path": "SKILL.md"},           # no repository key
                {"repository": {"full_name": "a/b"}},  # no path key
            ]
        )
        results = discover(client, MINIMAL_CONFIG)
        assert results == []

    def test_multiple_skill_paths_accumulated_for_same_repo(self):
        client = make_mock_client(
            search_code_results=[
                code_item("owner/monorepo", "skills/backend/SKILL.md"),
                code_item("owner/monorepo", "skills/frontend/SKILL.md"),
            ]
        )
        results = discover(client, MINIMAL_CONFIG)
        match = next(r for r in results if r.full_name == "owner/monorepo")
        assert sorted(match.skill_paths) == [
            "skills/backend/SKILL.md",
            "skills/frontend/SKILL.md",
        ]


# ---------------------------------------------------------------------------
# Deduplication across phases
# ---------------------------------------------------------------------------

class TestDeduplication:
    def test_phase1_and_phase2_merged(self):
        """A repo found in both phases should appear once with code-search paths."""
        client = make_mock_client(
            search_repos_results=[repo_item("owner/repo")],
            search_code_results=[code_item("owner/repo", "SKILL.md")],
        )
        results = discover(client, MINIMAL_CONFIG)
        assert len(results) == 1
        dr = results[0]
        assert dr.full_name == "owner/repo"
        assert dr.skill_paths == ["SKILL.md"]
        assert dr.discovery_source == "both"

    def test_duplicate_skill_paths_not_added_twice(self):
        client = make_mock_client(
            search_repos_results=[repo_item("owner/repo")],
            search_code_results=[
                code_item("owner/repo", "SKILL.md"),
                code_item("owner/repo", "SKILL.md"),  # duplicate path
            ],
        )
        results = discover(client, MINIMAL_CONFIG)
        match = results[0]
        assert match.skill_paths.count("SKILL.md") == 1


# ---------------------------------------------------------------------------
# max_repos cap
# ---------------------------------------------------------------------------

class TestMaxReposCap:
    def test_cap_respected_during_phase1(self):
        client = make_mock_client(
            search_repos_results=[repo_item(f"owner/repo-{i}") for i in range(10)]
        )
        results = discover(client, MINIMAL_CONFIG, max_repos=3)
        assert len(results) == 3

    def test_cap_respected_across_phases(self):
        client = make_mock_client(
            search_repos_results=[repo_item(f"owner/p1-{i}") for i in range(5)],
            search_code_results=[
                code_item(f"owner/p2-{i}", "SKILL.md") for i in range(5)
            ],
        )
        results = discover(client, MINIMAL_CONFIG, max_repos=7)
        assert len(results) == 7

    def test_cap_zero_returns_empty(self):
        client = make_mock_client(
            search_repos_results=[repo_item("owner/repo")]
        )
        results = discover(client, MINIMAL_CONFIG, max_repos=0)
        assert results == []


# ---------------------------------------------------------------------------
# is_monorepo / make_entity_id
# ---------------------------------------------------------------------------

class TestHelpers:
    def test_is_monorepo_true(self):
        dr = DiscoveredRepo("a/b", ["path/one/SKILL.md", "path/two/SKILL.md"])
        assert is_monorepo(dr) is True

    def test_is_monorepo_false_single_path(self):
        dr = DiscoveredRepo("a/b", ["SKILL.md"])
        assert is_monorepo(dr) is False

    def test_is_monorepo_false_empty_paths(self):
        dr = DiscoveredRepo("a/b", [])
        assert is_monorepo(dr) is False

    def test_make_entity_id_root_skill(self):
        assert make_entity_id("owner/repo", "SKILL.md") == "skill:owner/repo"

    def test_make_entity_id_monorepo_skill(self):
        assert make_entity_id("owner/repo", "skills/backend/SKILL.md") == \
               "skill:owner/repo:skills/backend"

    def test_make_entity_id_one_level_deep(self):
        assert make_entity_id("owner/repo", "backend/SKILL.md") == \
               "skill:owner/repo:backend"


# ---------------------------------------------------------------------------
# Integration test — requires GITHUB_TOKEN
# ---------------------------------------------------------------------------

@pytest.mark.skipif(
    not os.environ.get("GITHUB_TOKEN"),
    reason="requires GITHUB_TOKEN environment variable",
)
def test_live_discovery_returns_results():
    """
    Smoke test: run a small live discovery query and verify we get at least
    one repo back with valid structure.
    """

    from data.store import get_connection, init_db, store_raw_signal

    token = os.environ["GITHUB_TOKEN"]
    client = GitHubClient(token=token)

    # Use a very specific, small query to minimise API usage
    small_config = {
        "phase_1": {"queries": [{"query": "topic:claude-skill", "sort": "stars"}]},
        "phase_2": {"queries": []},
    }

    results = discover(client, small_config, max_repos=3)
    assert len(results) >= 1
    assert all(isinstance(r, DiscoveredRepo) for r in results)
    assert all("/" in r.full_name for r in results)

    # Verify a raw signal can be stored for the first result
    conn = get_connection(":memory:")
    init_db(conn)
    row_id = store_raw_signal(
        conn,
        source_id="github",
        signal_type="test_signal",
        entity_ref=f"skill:{results[0].full_name}",
        payload={"full_name": results[0].full_name},
        collected_at="2026-04-12T00:00:00Z",
        run_id="integration-test",
    )
    assert isinstance(row_id, int)
    assert row_id >= 1
