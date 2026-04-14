"""
Tests for config loading failure paths and per-repo pipeline isolation.

Covers:
- load_config() raises FileNotFoundError with a clear message when a config
  file is absent.
- load_config() raises yaml.YAMLError with a clear message when a config
  file contains invalid YAML.
- The per-repo except-Exception catch-all in pipeline.run() keeps the
  pipeline alive when individual repos fail, and the surviving repos are
  still processed.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from surfaces.skills_leaderboard.pipeline import load_config


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def _minimal_configs(tmp_path: Path) -> None:
    """Write the four minimal valid YAML config files to *tmp_path*."""
    _write(tmp_path / "discovery.yaml", "max_repos: 10\napi:\n  progress_log_interval: 100\nfilters: {}\n")
    _write(tmp_path / "scoring.yaml", "methodologies:\n  trending:\n    weights:\n      velocity: 40\n      adoption: 20\n      freshness: 20\n      documentation: 10\n      contributors: 5\n      code_quality: 5\n")
    _write(tmp_path / "categories.yaml", "categories: []\n")
    _write(tmp_path / "site.yaml", "title: Test\n")


# ---------------------------------------------------------------------------
# load_config — FileNotFoundError
# ---------------------------------------------------------------------------

class TestLoadConfigMissingFile:
    """load_config() must re-raise FileNotFoundError with a helpful message."""

    def test_missing_discovery_raises_file_not_found(self, tmp_path):
        # Only write scoring, categories, and site; leave discovery absent.
        _write(tmp_path / "scoring.yaml", "methodologies: {}\n")
        _write(tmp_path / "categories.yaml", "categories: []\n")
        _write(tmp_path / "site.yaml", "title: Test\n")

        with patch(
            "surfaces.skills_leaderboard.pipeline.CONFIG_DIR", tmp_path
        ):
            with pytest.raises(FileNotFoundError, match="discovery.yaml"):
                load_config()

    def test_missing_scoring_raises_file_not_found(self, tmp_path):
        _write(tmp_path / "discovery.yaml", "max_repos: 10\napi:\n  progress_log_interval: 100\nfilters: {}\n")
        _write(tmp_path / "categories.yaml", "categories: []\n")
        _write(tmp_path / "site.yaml", "title: Test\n")

        with patch(
            "surfaces.skills_leaderboard.pipeline.CONFIG_DIR", tmp_path
        ):
            with pytest.raises(FileNotFoundError, match="scoring.yaml"):
                load_config()

    def test_error_message_mentions_path(self, tmp_path):
        """The raised FileNotFoundError message must contain the file path."""
        # Provide all files except 'site.yaml'.
        _write(tmp_path / "discovery.yaml", "max_repos: 10\napi:\n  progress_log_interval: 100\nfilters: {}\n")
        _write(tmp_path / "scoring.yaml", "methodologies: {}\n")
        _write(tmp_path / "categories.yaml", "categories: []\n")

        with patch(
            "surfaces.skills_leaderboard.pipeline.CONFIG_DIR", tmp_path
        ):
            with pytest.raises(FileNotFoundError) as exc_info:
                load_config()

        assert "site.yaml" in str(exc_info.value)


# ---------------------------------------------------------------------------
# load_config — yaml.YAMLError
# ---------------------------------------------------------------------------

class TestLoadConfigMalformedYaml:
    """load_config() must re-raise yaml.YAMLError with a helpful message."""

    def test_malformed_discovery_raises_yaml_error(self, tmp_path):
        _write(tmp_path / "discovery.yaml", "key: [unclosed bracket\n")
        _write(tmp_path / "scoring.yaml", "methodologies: {}\n")
        _write(tmp_path / "categories.yaml", "categories: []\n")
        _write(tmp_path / "site.yaml", "title: Test\n")

        with patch(
            "surfaces.skills_leaderboard.pipeline.CONFIG_DIR", tmp_path
        ):
            with pytest.raises(yaml.YAMLError, match="discovery.yaml"):
                load_config()

    def test_malformed_scoring_raises_yaml_error(self, tmp_path):
        _write(tmp_path / "discovery.yaml", "max_repos: 10\napi:\n  progress_log_interval: 100\nfilters: {}\n")
        _write(tmp_path / "scoring.yaml", "key: [unclosed bracket\n")
        _write(tmp_path / "categories.yaml", "categories: []\n")
        _write(tmp_path / "site.yaml", "title: Test\n")

        with patch(
            "surfaces.skills_leaderboard.pipeline.CONFIG_DIR", tmp_path
        ):
            with pytest.raises(yaml.YAMLError, match="scoring.yaml"):
                load_config()

    def test_error_message_mentions_config_name(self, tmp_path):
        """The raised YAMLError must mention which file caused the problem."""
        _minimal_configs(tmp_path)
        # Overwrite categories.yaml with bad YAML.
        _write(tmp_path / "categories.yaml", "key: {bad: yaml: here\n")

        with patch(
            "surfaces.skills_leaderboard.pipeline.CONFIG_DIR", tmp_path
        ):
            with pytest.raises(yaml.YAMLError) as exc_info:
                load_config()

        assert "categories.yaml" in str(exc_info.value)

    def test_valid_configs_load_without_error(self, tmp_path):
        """Sanity check: valid YAML in all four files must not raise."""
        _minimal_configs(tmp_path)

        with patch(
            "surfaces.skills_leaderboard.pipeline.CONFIG_DIR", tmp_path
        ):
            result = load_config()

        assert set(result.keys()) == {"discovery", "scoring", "categories", "site"}


# ---------------------------------------------------------------------------
# Per-repo failure isolation
# ---------------------------------------------------------------------------

class TestPerRepoIsolation:
    """
    The except-Exception guard at pipeline.run() keeps the run alive when
    individual repos fail, and the surviving repos are still processed.
    """

    # A minimal valid SKILL.md that passes is_valid_skill()
    _VALID_SKILL = (
        "---\nname: Test Skill\ndescription: A test skill for unit testing\n---\n"
        "## Usage\nRun this skill to do something useful.\n"
        "It demonstrates that the pipeline handles per-repo errors gracefully.\n"
    )

    def _make_discovered(self, full_name: str):
        from signals.github.discovery import DiscoveredRepo
        return DiscoveredRepo(full_name=full_name, skill_paths=["SKILL.md"])

    def _make_repo_data(self, full_name: str) -> dict:
        return {
            "full_name": full_name,
            "stargazers_count": 10,
            "forks_count": 2,
            "watchers_count": 3,
            "fork": False,
            "archived": False,
            "created_at": "2025-01-01T00:00:00Z",
            "pushed_at": "2026-04-01T00:00:00Z",
            "topics": ["claude-skill"],
            "license": {"spdx_id": "MIT"},
            "default_branch": "main",
        }

    def _make_client(self, good_repos: list[str], bad_repo: str) -> MagicMock:
        """Return a mock GitHubClient where *bad_repo* raises on get_repo."""
        client = MagicMock()

        def get_repo_side_effect(owner, repo):
            full = f"{owner}/{repo}"
            if full == bad_repo:
                raise RuntimeError(f"Simulated network failure for {full}")
            return self._make_repo_data(full)

        client.get_repo.side_effect = get_repo_side_effect
        client.__enter__ = MagicMock(return_value=client)
        client.__exit__ = MagicMock(return_value=False)
        client.get_contents.return_value = [
            {"name": "SKILL.md", "type": "file", "path": "SKILL.md"},
        ]
        client.get_file_content.return_value = self._VALID_SKILL
        client.get_commits.return_value = []
        client.get_contributors.return_value = [{"login": "alice"}]
        return client

    def test_pipeline_continues_after_per_repo_error(self, tmp_path, monkeypatch):
        """
        When one repo raises during ingestion, the pipeline must complete
        (return a run_id) and still process the other repos.
        """
        from surfaces.skills_leaderboard.pipeline import run

        db_file = str(tmp_path / "test.db")
        monkeypatch.setenv("GITHUB_TOKEN", "fake-token")
        monkeypatch.setenv("TESSERA_DB_PATH", db_file)

        good_repo = "alice/good"
        bad_repo = "bob/bad"

        discovered = [self._make_discovered(good_repo), self._make_discovered(bad_repo)]
        client = self._make_client(good_repos=[good_repo], bad_repo=bad_repo)

        with (
            patch("surfaces.skills_leaderboard.pipeline.GitHubClient", return_value=client),
            patch("surfaces.skills_leaderboard.pipeline.discover", return_value=discovered),
            patch("surfaces.skills_leaderboard.pipeline.score_and_store_skills", return_value=1),
        ):
            run_id = run(db_path=db_file)

        assert run_id is not None

    def test_error_repo_increments_error_count(self, tmp_path, monkeypatch):
        """
        The errors counter must be incremented for the failing repo,
        and the completed pipeline_run stats must reflect it.
        """
        import json
        from data.store import get_connection
        from surfaces.skills_leaderboard.pipeline import run

        db_file = str(tmp_path / "test.db")
        monkeypatch.setenv("GITHUB_TOKEN", "fake-token")
        monkeypatch.setenv("TESSERA_DB_PATH", db_file)

        good_repo = "alice/good"
        bad_repo = "bob/bad"

        discovered = [self._make_discovered(good_repo), self._make_discovered(bad_repo)]
        client = self._make_client(good_repos=[good_repo], bad_repo=bad_repo)

        with (
            patch("surfaces.skills_leaderboard.pipeline.GitHubClient", return_value=client),
            patch("surfaces.skills_leaderboard.pipeline.discover", return_value=discovered),
            patch("surfaces.skills_leaderboard.pipeline.score_and_store_skills", return_value=0),
        ):
            run_id = run(db_path=db_file)

        conn = get_connection(db_file)
        row = conn.execute(
            "SELECT stats FROM pipeline_runs WHERE id = ?", (run_id,)
        ).fetchone()
        assert row is not None
        stats = json.loads(row["stats"])
        assert stats["errors"] >= 1

    def test_good_repo_ingested_despite_bad_repo(self, tmp_path, monkeypatch):
        """
        After one repo fails, the good repos must still have their signals
        stored in the database.
        """
        from data.store import get_connection, get_raw_signals
        from surfaces.skills_leaderboard.pipeline import run

        db_file = str(tmp_path / "test.db")
        monkeypatch.setenv("GITHUB_TOKEN", "fake-token")
        monkeypatch.setenv("TESSERA_DB_PATH", db_file)

        good_repo = "alice/good"
        bad_repo = "bob/bad"

        discovered = [self._make_discovered(good_repo), self._make_discovered(bad_repo)]
        client = self._make_client(good_repos=[good_repo], bad_repo=bad_repo)

        with (
            patch("surfaces.skills_leaderboard.pipeline.GitHubClient", return_value=client),
            patch("surfaces.skills_leaderboard.pipeline.discover", return_value=discovered),
            patch("surfaces.skills_leaderboard.pipeline.score_and_store_skills", return_value=0),
        ):
            run(db_path=db_file)

        conn = get_connection(db_file)
        signals = get_raw_signals(conn, good_repo, "repo_metadata")
        assert len(signals) >= 1, (
            f"Expected repo_metadata signal for {good_repo} but found none"
        )
