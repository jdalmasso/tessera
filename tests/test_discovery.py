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
    "discovery": {
        "query": "filename:SKILL.md path:.claude/skills",
        "shards": [
            {"size": "<1000"},
            {"size": "1000..5000"},
        ],
    },
    "filters": {},
}


def make_mock_client(search_code_results: list = None) -> MagicMock:
    """Return a mock GitHubClient with pre-configured return values."""
    client = MagicMock(spec=GitHubClient)
    client.search_code.return_value = search_code_results or []
    return client


def code_item(full_name: str, path: str) -> dict:
    """Minimal code search result item."""
    return {
        "path": path,
        "repository": {"full_name": full_name},
    }


# ---------------------------------------------------------------------------
# Sharded discovery
# ---------------------------------------------------------------------------

class TestDiscoverSharded:

    def test_repos_found_via_shard(self):
        client = make_mock_client([code_item("owner/repo-a", "SKILL.md")])
        results = discover(client, MINIMAL_CONFIG)
        full_names = {r.full_name for r in results}
        assert "owner/repo-a" in full_names

    def test_skill_paths_populated(self):
        client = make_mock_client([code_item("owner/repo-a", ".claude/skills/SKILL.md")])
        results = discover(client, MINIMAL_CONFIG)
        match = next(r for r in results if r.full_name == "owner/repo-a")
        assert match.skill_paths == [".claude/skills/SKILL.md"]
        assert match.discovery_source == "code_search"

    def test_all_shards_queried(self):
        """search_code is called once per shard."""
        client = make_mock_client([])
        discover(client, MINIMAL_CONFIG)
        assert client.search_code.call_count == 2  # 2 shards in MINIMAL_CONFIG

    def test_shard_query_constructed_correctly(self):
        client = make_mock_client([])
        discover(client, MINIMAL_CONFIG)
        calls = [c.args[0] for c in client.search_code.call_args_list]
        assert calls[0] == "filename:SKILL.md path:.claude/skills size:<1000"
        assert calls[1] == "filename:SKILL.md path:.claude/skills size:1000..5000"

    def test_zero_result_shard_does_not_break(self):
        """A shard returning no results is silently skipped."""
        client = MagicMock(spec=GitHubClient)
        client.search_code.side_effect = [
            [],  # first shard empty
            [code_item("owner/repo-b", "SKILL.md")],  # second shard has results
        ]
        results = discover(client, MINIMAL_CONFIG)
        assert len(results) == 1
        assert results[0].full_name == "owner/repo-b"

    def test_missing_fields_skipped(self):
        client = make_mock_client([
            {"path": "SKILL.md"},                      # no repository key
            {"repository": {"full_name": "a/b"}},      # no path key
            {"repository": {}, "path": "SKILL.md"},    # empty full_name
        ])
        results = discover(client, MINIMAL_CONFIG)
        assert results == []

    def test_no_query_configured_returns_empty(self):
        client = make_mock_client([code_item("owner/repo", "SKILL.md")])
        results = discover(client, {"discovery": {"query": "", "shards": [{"size": "<1000"}]}})
        assert results == []
        client.search_code.assert_not_called()

    def test_no_shards_runs_base_query_once(self):
        """Falls back to a single unsharded query if shards list is empty."""
        client = make_mock_client([code_item("owner/repo", "SKILL.md")])
        config = {"discovery": {"query": "filename:SKILL.md path:.claude/skills", "shards": []}}
        results = discover(client, config)
        assert len(results) == 1
        client.search_code.assert_called_once_with("filename:SKILL.md path:.claude/skills")


# ---------------------------------------------------------------------------
# Deduplication across shards
# ---------------------------------------------------------------------------

class TestDeduplication:

    def test_same_repo_in_two_shards_appears_once(self):
        client = MagicMock(spec=GitHubClient)
        client.search_code.side_effect = [
            [code_item("owner/repo", "SKILL.md")],
            [code_item("owner/repo", "SKILL.md")],  # same repo, same path
        ]
        results = discover(client, MINIMAL_CONFIG)
        assert len(results) == 1

    def test_different_paths_from_different_shards_accumulated(self):
        """Same repo found in two shards with different SKILL.md paths → monorepo."""
        client = MagicMock(spec=GitHubClient)
        client.search_code.side_effect = [
            [code_item("owner/monorepo", "skills/backend/SKILL.md")],
            [code_item("owner/monorepo", "skills/frontend/SKILL.md")],
        ]
        results = discover(client, MINIMAL_CONFIG)
        assert len(results) == 1
        assert sorted(results[0].skill_paths) == [
            "skills/backend/SKILL.md",
            "skills/frontend/SKILL.md",
        ]

    def test_duplicate_skill_path_not_added_twice(self):
        client = make_mock_client([
            code_item("owner/repo", "SKILL.md"),
            code_item("owner/repo", "SKILL.md"),
        ])
        results = discover(client, MINIMAL_CONFIG)
        assert results[0].skill_paths.count("SKILL.md") == 1


# ---------------------------------------------------------------------------
# max_repos cap
# ---------------------------------------------------------------------------

class TestMaxReposCap:

    def test_cap_stops_within_shard(self):
        client = make_mock_client(
            [code_item(f"owner/repo-{i}", "SKILL.md") for i in range(10)]
        )
        results = discover(client, MINIMAL_CONFIG, max_repos=3)
        assert len(results) == 3

    def test_cap_stops_later_shards(self):
        """Once cap reached after first shard, second shard is not queried."""
        client = MagicMock(spec=GitHubClient)
        client.search_code.side_effect = [
            [code_item(f"owner/repo-{i}", "SKILL.md") for i in range(5)],
            [code_item(f"owner/repo-{i}", "SKILL.md") for i in range(5, 10)],
        ]
        results = discover(client, MINIMAL_CONFIG, max_repos=5)
        assert len(results) == 5
        assert client.search_code.call_count == 1  # second shard never called

    def test_cap_zero_returns_empty(self):
        client = make_mock_client([code_item("owner/repo", "SKILL.md")])
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

    def test_make_entity_id_claude_skills_path(self):
        assert make_entity_id("owner/repo", ".claude/skills/SKILL.md") == \
               "skill:owner/repo:.claude/skills"

    def test_make_entity_id_monorepo_skill(self):
        assert make_entity_id("owner/repo", "skills/backend/SKILL.md") == \
               "skill:owner/repo:skills/backend"

    def test_make_entity_id_one_level_deep(self):
        assert make_entity_id("owner/repo", "backend/SKILL.md") == \
               "skill:owner/repo:backend"


# ---------------------------------------------------------------------------
# Repo-search sources (discovery.repo_sources)
# ---------------------------------------------------------------------------

class TestRepoSources:

    REPO_SOURCE_CONFIG = {
        "discovery": {
            "query": "filename:SKILL.md path:.claude/skills",
            "shards": [{"size": "<1000"}],
            "repo_sources": [
                {
                    "query": "topic:claude-skill",
                    "shards": [
                        {"pushed": ">2025-01-01"},
                        {"pushed": "2024-01-01..2024-12-31"},
                    ],
                }
            ],
        },
        "filters": {},
    }

    def repo_item(self, full_name: str) -> dict:
        """Minimal repository search result item."""
        return {"full_name": full_name, "name": full_name.split("/")[-1]}

    def test_repo_sources_queried(self):
        """search_repos is called once per shard when repo_sources is configured."""
        client = MagicMock(spec=GitHubClient)
        client.search_code.return_value = []
        client.search_repos.return_value = []
        discover(client, self.REPO_SOURCE_CONFIG)
        assert client.search_repos.call_count == 2  # 2 shards in REPO_SOURCE_CONFIG

    def test_repo_sources_query_constructed_correctly(self):
        """Repo-search shard filters are appended correctly to the source query."""
        client = MagicMock(spec=GitHubClient)
        client.search_code.return_value = []
        client.search_repos.return_value = []
        discover(client, self.REPO_SOURCE_CONFIG)
        calls = [c.args[0] for c in client.search_repos.call_args_list]
        assert calls[0] == "topic:claude-skill pushed:>2025-01-01"
        assert calls[1] == "topic:claude-skill pushed:2024-01-01..2024-12-31"

    def test_repo_sources_repos_added(self):
        """Repos found via repo search appear in the results."""
        client = MagicMock(spec=GitHubClient)
        client.search_code.return_value = []
        client.search_repos.return_value = [self.repo_item("owner/from-repo-search")]
        results = discover(client, self.REPO_SOURCE_CONFIG)
        assert any(r.full_name == "owner/from-repo-search" for r in results)

    def test_repo_sources_empty_skill_paths(self):
        """Repos from repo search have skill_paths == [] and discovery_source == 'repo_search'."""
        client = MagicMock(spec=GitHubClient)
        client.search_code.return_value = []
        client.search_repos.return_value = [self.repo_item("owner/repo-search-repo")]
        results = discover(client, self.REPO_SOURCE_CONFIG)
        match = next(r for r in results if r.full_name == "owner/repo-search-repo")
        assert match.skill_paths == []
        assert match.discovery_source == "repo_search"

    def test_repo_sources_deduplicated_with_code_search(self):
        """A repo found in both code search and repo search appears only once."""
        client = MagicMock(spec=GitHubClient)
        client.search_code.return_value = [code_item("owner/shared-repo", "SKILL.md")]
        client.search_repos.return_value = [self.repo_item("owner/shared-repo")]
        results = discover(client, self.REPO_SOURCE_CONFIG)
        matches = [r for r in results if r.full_name == "owner/shared-repo"]
        assert len(matches) == 1
        # Code search result takes precedence (has skill_paths populated)
        assert matches[0].skill_paths == ["SKILL.md"]
        assert matches[0].discovery_source == "code_search"

    def test_no_repo_sources_skips_search_repos(self):
        """search_repos is never called when repo_sources is absent."""
        client = MagicMock(spec=GitHubClient)
        client.search_code.return_value = []
        client.search_repos.return_value = []
        discover(client, MINIMAL_CONFIG)  # MINIMAL_CONFIG has no repo_sources
        client.search_repos.assert_not_called()

    def test_repo_sources_respect_max_repos_cap(self):
        """max_repos cap stops repo search from running once the cap is reached."""
        client = MagicMock(spec=GitHubClient)
        # Code search fills the cap exactly
        client.search_code.return_value = [
            code_item(f"owner/code-repo-{i}", "SKILL.md") for i in range(3)
        ]
        client.search_repos.return_value = [self.repo_item("owner/extra")]
        results = discover(client, self.REPO_SOURCE_CONFIG, max_repos=3)
        assert len(results) == 3
        client.search_repos.assert_not_called()

    def test_repo_source_missing_query_is_skipped(self):
        """A repo_sources entry without a 'query' key is skipped with a warning."""
        client = MagicMock(spec=GitHubClient)
        client.search_code.return_value = []
        client.search_repos.return_value = []
        config = {
            "discovery": {
                "query": "filename:SKILL.md path:.claude/skills",
                "shards": [],
                "repo_sources": [{"shards": [{"pushed": ">2025-01-01"}]}],  # no query
            }
        }
        results = discover(client, config)
        assert results == []
        client.search_repos.assert_not_called()


# ---------------------------------------------------------------------------
# Integration test — requires GITHUB_TOKEN
# ---------------------------------------------------------------------------

@pytest.mark.skipif(
    not os.environ.get("GITHUB_TOKEN"),
    reason="requires GITHUB_TOKEN environment variable",
)
def test_live_discovery_returns_results():
    """
    Smoke test: run a single sharded discovery query and verify we get at
    least one repo back with valid structure.
    """
    from data.store import get_connection, init_db, store_raw_signal

    token = os.environ["GITHUB_TOKEN"]
    client = GitHubClient(token=token)

    small_config = {
        "discovery": {
            "query": "filename:SKILL.md path:.claude/skills",
            "shards": [{"size": "<1000"}],
        },
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
        collected_at="2026-04-13T00:00:00Z",
        run_id="integration-test",
    )
    assert isinstance(row_id, int)
    assert row_id >= 1
