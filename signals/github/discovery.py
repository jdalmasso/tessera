"""
Two-phase GitHub discovery of repositories containing SKILL.md files.

Generic — knows nothing about Skills. Returns DiscoveredRepo objects
that the pipeline then ingests and validates.

Phase 1: repository search by topic (sorted by stars) — captures popular
         repos early and front-loads high-quality results.
Phase 2: code search by filename — backfills repos that lack topics.

Discovery deduplicates continuously by full_name and stops at max_repos.
"""

import logging
from dataclasses import dataclass, field

from signals.github.client import GitHubClient

logger = logging.getLogger(__name__)


@dataclass
class DiscoveredRepo:
    """
    A repository found during discovery, with any known SKILL.md paths.

    `skill_paths` is populated by code-search results (Phase 2). For repos
    found only via topic search (Phase 1), it is empty — the pipeline
    resolves paths later via the contents API.

    `discovery_source` tracks which phase(s) found this repo:
      "topic_search"  — Phase 1 only
      "code_search"   — Phase 2 only
      "both"          — found in both phases
    """

    full_name: str
    skill_paths: list[str] = field(default_factory=list)
    discovery_source: str = "topic_search"


def discover(
    client: GitHubClient,
    config: dict,
    max_repos: int = 2000,
) -> list[DiscoveredRepo]:
    """
    Run two-phase discovery and return a deduplicated list of DiscoveredRepo.

    `config` is the parsed contents of config/discovery.yaml.
    Stops when `max_repos` unique repos have been found.
    """
    seen: dict[str, DiscoveredRepo] = {}  # full_name → DiscoveredRepo

    # ------------------------------------------------------------------
    # Phase 1: repository search by topic, sorted by stars
    # ------------------------------------------------------------------
    for q in config.get("phase_1", {}).get("queries", []):
        if len(seen) >= max_repos:
            break
        query = q.get("query", "")
        sort = q.get("sort", "stars")
        logger.info("Phase 1 — repo search: %r (sort=%s)", query, sort)

        for repo in client.search_repos(query, sort=sort):
            full_name = repo.get("full_name", "")
            if not full_name or full_name in seen:
                continue
            seen[full_name] = DiscoveredRepo(
                full_name=full_name,
                skill_paths=[],
                discovery_source="topic_search",
            )
            if len(seen) >= max_repos:
                break

    logger.info("Phase 1 complete: %d unique repos", len(seen))

    # ------------------------------------------------------------------
    # Phase 2: code search by SKILL.md filename
    # ------------------------------------------------------------------
    for q in config.get("phase_2", {}).get("queries", []):
        if len(seen) >= max_repos:
            break
        query = q.get("query", "")
        logger.info("Phase 2 — code search: %r", query)

        for item in client.search_code(query):
            repo_info = item.get("repository", {})
            full_name = repo_info.get("full_name", "")
            skill_path = item.get("path", "")
            if not full_name or not skill_path:
                continue

            if full_name in seen:
                dr = seen[full_name]
                if skill_path not in dr.skill_paths:
                    dr.skill_paths.append(skill_path)
                if dr.discovery_source == "topic_search":
                    dr.discovery_source = "both"
            else:
                seen[full_name] = DiscoveredRepo(
                    full_name=full_name,
                    skill_paths=[skill_path],
                    discovery_source="code_search",
                )

            if len(seen) >= max_repos:
                break

    logger.info("Phase 2 complete: %d unique repos total", len(seen))
    return list(seen.values())


def is_monorepo(repo: DiscoveredRepo) -> bool:
    """Return True if the repo has more than one known SKILL.md path."""
    return len(repo.skill_paths) > 1


def make_entity_id(full_name: str, skill_path: str) -> str:
    """
    Build the canonical entity ID for a skill.

    Root-level SKILL.md   ("SKILL.md")                  → "skill:owner/repo"
    Monorepo SKILL.md     ("skills/backend/SKILL.md")   → "skill:owner/repo:skills/backend"
    """
    parts = skill_path.replace("\\", "/").split("/")
    if len(parts) == 1:
        return f"skill:{full_name}"
    parent_dir = "/".join(parts[:-1])
    return f"skill:{full_name}:{parent_dir}"
