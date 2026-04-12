"""
Pipeline orchestration for the Skills Leaderboard.

Flow: discovery → ingestion → entity resolution → scoring → storage

This module is the opinionated consumer of signals/github/ and data/.
It knows about Skills: what to fetch, how to validate, and how to store.
Scoring and entity resolution are added in Phase 3.
"""

import datetime
import logging
import os
import uuid
from pathlib import Path
from typing import Any, Optional

import yaml

from data.models import RUN_STATUS_COMPLETED, RUN_STATUS_FAILED
from data.store import (
    complete_pipeline_run,
    get_connection,
    init_db,
    start_pipeline_run,
    store_raw_signal,
    upsert_signal_source,
)
from signals.github.client import GitHubClient
from signals.github.discovery import DiscoveredRepo, discover, make_entity_id
from utils.parsers import count_lines, extract_frontmatter, has_section, is_valid_skill

logger = logging.getLogger(__name__)

CONFIG_DIR = Path(__file__).parent / "config"
_PROJECT_ROOT = Path(__file__).parent.parent.parent
DB_PATH = Path(os.environ.get("TESSERA_DB_PATH", _PROJECT_ROOT / "db" / "tessera.db"))
SURFACE_ID = "skills_leaderboard"
SOURCE_ID = "github"


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def load_config() -> dict[str, Any]:
    configs: dict[str, Any] = {}
    for name in ("discovery", "scoring", "categories", "site"):
        with open(CONFIG_DIR / f"{name}.yaml") as fh:
            configs[name] = yaml.safe_load(fh)
    return configs


# ---------------------------------------------------------------------------
# Signal collection helpers
# ---------------------------------------------------------------------------

def _find_skill_paths(
    client: GitHubClient,
    owner: str,
    repo: str,
    root_contents: Optional[list] = None,
) -> list[str]:
    """
    Locate SKILL.md files in a repo via the contents API.
    Only checks the repo root in v0.1 (monorepo paths come from code search).
    """
    if root_contents is None:
        root_contents = client.get_contents(owner, repo, "")
    if not isinstance(root_contents, list):
        return []
    return [
        entry["path"]
        for entry in root_contents
        if entry.get("name", "").upper() == "SKILL.MD"
        and entry.get("type") == "file"
    ]


def _compute_commit_windows(commits: list[dict]) -> dict[str, int]:
    """
    Given a list of commit dicts from the GitHub API, compute the four
    commit-window metrics used by velocity and freshness scoring.
    """
    now = datetime.datetime.utcnow()
    since_30d = now - datetime.timedelta(days=30)
    since_60d = now - datetime.timedelta(days=60)

    commit_datetimes: list[datetime.datetime] = []
    for c in commits:
        date_str = c.get("commit", {}).get("author", {}).get("date", "")
        if not date_str:
            continue
        try:
            dt = datetime.datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            commit_datetimes.append(dt.replace(tzinfo=None))
        except ValueError:
            continue

    return {
        "commit_count_30d": sum(1 for d in commit_datetimes if d >= since_30d),
        "commit_count_prev_30d": sum(
            1 for d in commit_datetimes if since_60d <= d < since_30d
        ),
        "commit_count_90d": len(commit_datetimes),
        "unique_commit_weeks_90d": len(
            {d.isocalendar()[:2] for d in commit_datetimes}
        ),
    }


def ingest_repo(
    client: GitHubClient,
    repo_data: dict,
    skill_paths: list[str],
    config: dict,
    run_id: str,
    conn: Any,
) -> int:
    """
    Fetch and store all raw signals for one repository and its skill files.

    Returns the number of valid skill files stored.
    `skill_paths` may be empty (topic-search repos); paths are resolved here.
    """
    full_name: str = repo_data["full_name"]
    owner, repo_name = full_name.split("/", 1)
    now = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    min_chars: int = config["discovery"]["filters"]["min_skill_md_chars"]

    # --- Repo metadata signal ---
    store_raw_signal(
        conn, SOURCE_ID, "repo_metadata", full_name,
        {
            "stars": repo_data.get("stargazers_count", 0),
            "forks": repo_data.get("forks_count", 0),
            "watchers": repo_data.get("watchers_count", 0),
            "is_fork": repo_data.get("fork", False),
            "is_archived": repo_data.get("archived", False),
            "created_at": repo_data.get("created_at", ""),
            "pushed_at": repo_data.get("pushed_at", ""),
            "topics": repo_data.get("topics", []),
            "has_license": repo_data.get("license") is not None,
            "license_name": (repo_data.get("license") or {}).get("spdx_id"),
            "default_branch": repo_data.get("default_branch", "main"),
        },
        now, run_id,
    )

    # --- Root contents (structural checks + path resolution) ---
    root = client.get_contents(owner, repo_name, "")
    root_names: set[str] = set()
    if isinstance(root, list):
        root_names = {e.get("name", "").lower() for e in root}

    store_raw_signal(
        conn, SOURCE_ID, "code_quality", full_name,
        {
            "has_gitignore": ".gitignore" in root_names,
            "has_github_dir": ".github" in root_names,
            "has_tests": "tests" in root_names or "test" in root_names,
        },
        now, run_id,
    )

    # Resolve skill paths for topic-search repos
    if not skill_paths:
        skill_paths = _find_skill_paths(client, owner, repo_name, root)

    # --- Commit signals (velocity + freshness) ---
    since_90d = (datetime.datetime.utcnow() - datetime.timedelta(days=90)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    commits = client.get_commits(owner, repo_name, since=since_90d)
    store_raw_signal(
        conn, SOURCE_ID, "commits", full_name,
        _compute_commit_windows(commits),
        now, run_id,
    )

    # --- Contributor signal ---
    contributors = client.get_contributors(owner, repo_name)
    store_raw_signal(
        conn, SOURCE_ID, "contributors", full_name,
        {"contributor_count": len(contributors)},
        now, run_id,
    )

    # --- Skill file signals ---
    skill_count = 0
    for skill_path in skill_paths:
        content = client.get_file_content(owner, repo_name, skill_path)
        if not content:
            continue

        fm, body = extract_frontmatter(content)
        if not is_valid_skill(fm, content, min_chars=min_chars):
            continue

        # Check sibling files in the skill's directory
        skill_dir = "/".join(skill_path.replace("\\", "/").split("/")[:-1])
        dir_contents = (
            client.get_contents(owner, repo_name, skill_dir)
            if skill_dir else root
        )
        dir_names: set[str] = set()
        if isinstance(dir_contents, list):
            dir_names = {e.get("name", "").lower() for e in dir_contents}

        entity_ref = make_entity_id(full_name, skill_path)
        description = fm.get("description")
        store_raw_signal(
            conn, SOURCE_ID, "skill_file", entity_ref,
            {
                "skill_path": skill_path,
                "char_count": len(content),
                "line_count": count_lines(content),
                "has_frontmatter": bool(fm),
                "frontmatter_name": fm.get("name"),
                "frontmatter_description": str(description)[:500] if description else None,
                "frontmatter_category": fm.get("category"),
                "frontmatter_tags": fm.get("tags", []),
                "has_usage_section": has_section(body, "Usage", "How to Use", "How to use"),
                "has_examples_section": has_section(body, "Examples", "Example"),
                "has_readme": "readme.md" in dir_names,
                "has_scripts_dir": "scripts" in dir_names,
                "has_references_dir": "references" in dir_names,
            },
            now, run_id,
        )
        skill_count += 1

    return skill_count


# ---------------------------------------------------------------------------
# Main pipeline entry point
# ---------------------------------------------------------------------------

def run(db_path: Optional[str] = None, max_repos: Optional[int] = None) -> str:
    """
    Run the full pipeline: discover → ingest → (score in Phase 3).
    Returns the run_id of the completed pipeline run.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        raise RuntimeError("GITHUB_TOKEN environment variable is not set")

    config = load_config()
    cap = max_repos or config["discovery"].get("max_repos", 2000)
    log_interval = config["discovery"]["api"].get("progress_log_interval", 100)
    filters = config["discovery"].get("filters", {})

    conn = get_connection(db_path or str(DB_PATH))
    init_db(conn)

    run_id = str(uuid.uuid4())
    started_at = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    upsert_signal_source(conn, SOURCE_ID, "GitHub API", last_run_at=started_at)
    start_pipeline_run(conn, run_id, SURFACE_ID, started_at)

    client = GitHubClient(token=token)

    try:
        # --- Discovery ---
        logger.info("Starting discovery (cap=%d)...", cap)
        discovered = discover(client, config["discovery"], max_repos=cap)
        logger.info("Discovered %d repos.", len(discovered))

        # --- Batch-then-filter ingestion ---
        valid_skills = 0
        errors = 0

        for i, dr in enumerate(discovered):
            if i > 0 and i % log_interval == 0:
                logger.info(
                    "Processed %d/%d repos, %d valid skills found, %d errors skipped",
                    i, len(discovered), valid_skills, errors,
                )

            owner, repo_name = dr.full_name.split("/", 1)
            try:
                # Fetch repo metadata first (batch step)
                repo_data = client.get_repo(owner, repo_name)
                if not repo_data:
                    errors += 1
                    continue

                # Filter invalid repos
                if filters.get("exclude_forks") and repo_data.get("fork"):
                    continue
                if filters.get("exclude_archived") and repo_data.get("archived"):
                    continue

                # Fetch remaining signals for valid repos
                count = ingest_repo(
                    client=client,
                    repo_data=repo_data,
                    skill_paths=dr.skill_paths,
                    config=config,
                    run_id=run_id,
                    conn=conn,
                )
                valid_skills += count

            except Exception as exc:
                logger.warning("Error processing %s: %s", dr.full_name, exc)
                errors += 1

        logger.info(
            "Ingestion complete: %d valid skills, %d errors.", valid_skills, errors
        )

        completed_at = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        complete_pipeline_run(
            conn, run_id, completed_at,
            stats={"repos_discovered": len(discovered),
                   "valid_skills": valid_skills,
                   "errors": errors},
        )

    except Exception as exc:
        logger.exception("Pipeline failed: %s", exc)
        failed_at = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        complete_pipeline_run(conn, run_id, failed_at, status=RUN_STATUS_FAILED,
                              stats={"error": str(exc)})
        raise

    return run_id


if __name__ == "__main__":
    run()
