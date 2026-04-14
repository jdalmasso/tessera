"""
Two-source GitHub discovery of repositories containing SKILL.md files at the
canonical .claude/skills/ path.

Generic — knows nothing about Skills. Returns DiscoveredRepo objects
that the pipeline then ingests and validates.

Strategy
--------
Source 1 — GitHub code search (file-level precision, size-sharded):
    Hard-caps at ~1000 per query. Sharded by file ``size`` (in bytes):

        filename:SKILL.md path:.claude/skills size:<1000
        filename:SKILL.md path:.claude/skills size:1000..5000
        ...

    Each shard yields up to ~1000 unique repos.

Source 2 — GitHub repository search (broader, date-filtered supplemental):
    Uses ``pushed:`` date-range sharding, which IS valid in repository search
    (unlike code search where it silently returns 0 results). Queries repos
    that self-identify via ``topic:claude-skill``.

    Repos from this source have empty ``skill_paths``; the pipeline resolves
    them later via the contents API.

Note: ``pushed:`` is **not** a valid GitHub code search qualifier — using it
in ``/search/code`` silently returns 0 results. It IS valid in
``/search/repositories`` and is used exclusively there.
"""

import logging
from dataclasses import dataclass, field

from signals.github.client import GitHubClient

logger = logging.getLogger(__name__)


@dataclass
class DiscoveredRepo:
    """
    A repository found during discovery, with any known SKILL.md paths.

    `skill_paths` is populated by code-search results. It may be empty
    for repos where only the repo was returned without a path — the
    pipeline resolves paths later via the contents API.

    `discovery_source` is always "code_search" in the sharded approach.
    """

    full_name: str
    skill_paths: list[str] = field(default_factory=list)
    discovery_source: str = "code_search"


def discover(
    client: GitHubClient,
    config: dict,
    max_repos: int = 6000,
) -> list[DiscoveredRepo]:
    """
    Run sharded discovery and return a deduplicated list of DiscoveredRepo.

    `config` is the parsed contents of config/discovery.yaml.

    Source 1 (``discovery.query`` + ``discovery.shards``): code search with
    generic key-value shard filters, e.g. ``{"size": "<1000"}`` → ``size:<1000``.

    Source 2 (``discovery.repo_sources``): list of repo-search sources, each
    with a ``query`` and optional ``shards``. Each source entry runs
    ``client.search_repos()``; found repos get ``skill_paths=[]`` and
    ``discovery_source="repo_search"``.

    Deduplication is in-memory across all shards. Stops when `max_repos`
    unique repos have been found.
    """
    seen: dict[str, DiscoveredRepo] = {}  # full_name → DiscoveredRepo

    discovery_cfg = config.get("discovery", {})
    base_query    = discovery_cfg.get("query", "")
    shards        = discovery_cfg.get("shards", [])

    if not base_query:
        logger.warning("No discovery.query configured in discovery.yaml; returning empty.")
        return []

    if not shards:
        logger.warning("No discovery.shards configured; running base query without sharding.")
        shards = [{}]  # single pass with no pushed-date filter

    for shard in shards:
        if len(seen) >= max_repos:
            break

        shard_filters = " ".join(f"{k}:{v}" for k, v in shard.items())
        full_query    = f"{base_query} {shard_filters}".strip() if shard_filters else base_query
        logger.info("Discovery shard: %r (%d unique repos so far)", full_query, len(seen))

        for item in client.search_code(full_query):
            repo_info  = item.get("repository", {})
            full_name  = repo_info.get("full_name", "")
            skill_path = item.get("path", "")
            if not full_name or not skill_path:
                continue

            if full_name in seen:
                dr = seen[full_name]
                if skill_path not in dr.skill_paths:
                    dr.skill_paths.append(skill_path)
            else:
                seen[full_name] = DiscoveredRepo(
                    full_name=full_name,
                    skill_paths=[skill_path],
                    discovery_source="code_search",
                )

            if len(seen) >= max_repos:
                break

        logger.info("Shard complete: %d unique repos total", len(seen))

    # --- Source 2: repository search (repo_sources) ---
    for source in discovery_cfg.get("repo_sources", []):
        if len(seen) >= max_repos:
            break
        source_query = source.get("query", "")
        if not source_query:
            logger.warning("repo_sources entry missing 'query'; skipping.")
            continue
        source_shards = source.get("shards", [{}])
        for shard in source_shards:
            if len(seen) >= max_repos:
                break
            shard_filters = " ".join(f"{k}:{v}" for k, v in shard.items())
            full_query    = f"{source_query} {shard_filters}".strip() if shard_filters else source_query
            logger.info("Repo-search shard: %r (%d unique repos so far)", full_query, len(seen))
            for item in client.search_repos(full_query):
                full_name = item.get("full_name", "")
                if not full_name:
                    continue
                if full_name not in seen:
                    seen[full_name] = DiscoveredRepo(
                        full_name=full_name,
                        skill_paths=[],
                        discovery_source="repo_search",
                    )
                if len(seen) >= max_repos:
                    break
            logger.info("Repo-search shard complete: %d unique repos total", len(seen))

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
