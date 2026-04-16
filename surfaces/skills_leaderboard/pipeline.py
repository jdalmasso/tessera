"""
Pipeline orchestration for the Skills Leaderboard.

Flow: discovery → ingestion → entity resolution → scoring → storage

This module is the opinionated consumer of signals/github/ and data/.
It knows about Skills: what to fetch, how to validate, and how to store.
Scoring and entity resolution are added in Phase 3.
"""

import datetime
import hashlib
import json
import logging
import os
import uuid
from pathlib import Path
from typing import Any, Optional

import yaml

from data.models import RUN_STATUS_FAILED
from data.store import (
    complete_pipeline_run,
    get_connection,
    get_entity,
    get_known_repo_names,
    init_db,
    start_pipeline_run,
    store_raw_signal,
    store_score,
    upsert_entity,
    upsert_signal_source,
)
from signals.github.client import GitHubClient
from signals.github.discovery import discover, make_entity_id
from signals.github.scoring import (
    compute_composite,
    score_adoption,
    score_code_quality,
    score_contributors,
    score_documentation,
    score_freshness,
    score_velocity,
)
from surfaces.skills_leaderboard.categorization import categorize
from surfaces.skills_leaderboard.llm_categorize import LLMCategorizer
from utils.parsers import count_lines, extract_frontmatter, has_section, is_latin_script, is_valid_skill

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
        path = CONFIG_DIR / f"{name}.yaml"
        try:
            with open(path) as fh:
                configs[name] = yaml.safe_load(fh)
        except FileNotFoundError:
            raise FileNotFoundError(f"Config file not found: {path}") from None
        except yaml.YAMLError as exc:
            raise yaml.YAMLError(f"Malformed YAML in {path}: {exc}") from exc

    # Validate required top-level keys so downstream bracket access fails
    # with a descriptive error rather than a bare KeyError.
    discovery = configs.get("discovery")
    if not isinstance(discovery, dict):
        raise ValueError("Config 'discovery.yaml' must be a mapping at the top level")
    if "api" not in discovery:
        raise ValueError("Config 'discovery.yaml' is missing required key: 'api'")

    scoring = configs.get("scoring")
    if not isinstance(scoring, dict):
        raise ValueError("Config 'scoring.yaml' must be a mapping at the top level")
    if "methodologies" not in scoring:
        raise ValueError("Config 'scoring.yaml' is missing required key: 'methodologies'")
    for method_name, method_cfg in scoring["methodologies"].items():
        if not isinstance(method_cfg, dict) or "weights" not in method_cfg:
            raise ValueError(
                f"Config 'scoring.yaml': methodology {method_name!r} is missing required key: 'weights'"
            )

    for method_name, method_cfg in scoring["methodologies"].items():
        total = sum(method_cfg["weights"].values())
        if abs(total - 100) > 0.01:
            raise ValueError(
                f"Methodology '{method_name}' weights must sum to 100, got {total}"
            )

    categories = configs.get("categories")
    if not isinstance(categories, dict):
        raise ValueError("Config 'categories.yaml' must be a mapping at the top level")

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


def _utcnow() -> datetime.datetime:
    """Return the current UTC time as a timezone-aware datetime."""
    return datetime.datetime.now(datetime.timezone.utc)


def _utcnow_str() -> str:
    """Return the current UTC time as an ISO-8601 string (e.g. '2026-04-12T10:00:00Z')."""
    return _utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def _compute_commit_windows(commits: list[dict]) -> dict[str, int]:
    """
    Given a list of commit dicts from the GitHub API, compute the four
    commit-window metrics used by velocity and freshness scoring.
    """
    now = _utcnow()
    since_30d = now - datetime.timedelta(days=30)
    since_60d = now - datetime.timedelta(days=60)

    commit_datetimes: list[datetime.datetime] = []
    for c in commits:
        date_str = c.get("commit", {}).get("author", {}).get("date", "")
        if not date_str:
            continue
        try:
            dt = datetime.datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            commit_datetimes.append(dt)
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
    now = _utcnow_str()
    min_chars: int = (
        config.get("discovery", {}).get("filters", {}).get("min_skill_md_chars", 100)
    )

    # --- Root contents (structural checks + path resolution) ---
    root = client.get_contents(owner, repo_name, "")
    root_names: set[str] = set()
    if isinstance(root, list):
        root_names = {e.get("name", "").lower() for e in root}

    # Resolve skill paths for repos found via topic search (no paths yet)
    if not skill_paths:
        skill_paths = _find_skill_paths(client, owner, repo_name, root)

    # --- Validate skill files first (before any expensive API calls) ---
    # Accumulate valid skills; only proceed to commits/contributors if ≥1 is valid.
    valid_skills: list[dict] = []
    for skill_path in skill_paths:
        content = client.get_file_content(owner, repo_name, skill_path)
        if not content:
            continue

        fm, body = extract_frontmatter(content)
        if not is_valid_skill(fm, content, min_chars=min_chars):
            continue

        # Filter non-Latin-script skills (Chinese, Japanese, Korean, Arabic, etc.)
        # Latin-script languages (Spanish, French, German, etc.) pass through.
        skill_text = f"{fm.get('name', '')} {fm.get('description', '')}"
        if not is_latin_script(skill_text):
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

        valid_skills.append({
            "skill_path":   skill_path,
            "content":      content,
            "content_hash": hashlib.sha256(content.encode()).hexdigest()[:16],
            "fm":           fm,
            "body":         body,
            "dir_names":    dir_names,
            "entity_ref":   make_entity_id(full_name, skill_path),
            "description":  fm.get("description"),
        })

    # No valid skills — skip commits and contributors entirely
    if not valid_skills:
        return 0

    # --- Commit signals (velocity + freshness) ---
    since_90d = (_utcnow() - datetime.timedelta(days=90)).strftime("%Y-%m-%dT%H:%M:%SZ")
    commits = client.get_commits(owner, repo_name, since=since_90d)

    # --- Contributor signal ---
    contributors = client.get_contributors(owner, repo_name)

    # Batch ALL writes for this repo into a single transaction so all inserts
    # land in one fsync rather than one per store_raw_signal call.  This
    # includes repo_metadata and code_quality which were previously written
    # before the transaction block.
    with conn:
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

        store_raw_signal(
            conn, SOURCE_ID, "code_quality", full_name,
            {
                "has_gitignore": ".gitignore" in root_names,
                "has_github_dir": ".github" in root_names,
                "has_tests": "tests" in root_names or "test" in root_names,
            },
            now, run_id,
        )

        store_raw_signal(
            conn, SOURCE_ID, "commits", full_name,
            _compute_commit_windows(commits),
            now, run_id,
        )

        store_raw_signal(
            conn, SOURCE_ID, "contributors", full_name,
            {"contributor_count": len(contributors)},
            now, run_id,
        )

        # --- Skill file signals ---
        for skill in valid_skills:
            description = skill["description"]
            # Normalise dir_names at lookup time to guarantee case-insensitive
            # matching even if the set was populated without lowercasing.
            dir_names_lc = {n.lower() for n in skill["dir_names"]}
            store_raw_signal(
                conn, SOURCE_ID, "skill_file", skill["entity_ref"],
                {
                    "skill_path": skill["skill_path"],
                    "content_hash": skill["content_hash"],
                    "char_count": len(skill["content"]),
                    "line_count": count_lines(skill["content"]),
                    "has_frontmatter": bool(skill["fm"]),
                    "frontmatter_name": skill["fm"].get("name"),
                    "frontmatter_description": str(description)[:500] if description else None,
                    "frontmatter_category": skill["fm"].get("category"),
                    "frontmatter_tags": skill["fm"].get("tags", []),
                    "has_usage_section": has_section(
                        skill["body"], "Usage", "How to Use", "How to use"
                    ),
                    "has_examples_section": has_section(skill["body"], "Examples", "Example"),
                    "has_readme": "readme.md" in dir_names_lc,
                    "has_scripts_dir": "scripts" in dir_names_lc,
                    "has_references_dir": "references" in dir_names_lc,
                },
                now, run_id,
            )

    return len(valid_skills)


# ---------------------------------------------------------------------------
# Scoring helpers
# ---------------------------------------------------------------------------

def _repo_from_entity_ref(entity_ref: str) -> str:
    """
    Extract the ``owner/repo`` portion from an entity ref.

    "skill:owner/repo"              → "owner/repo"
    "skill:owner/repo:skills/path"  → "owner/repo"
    """
    without_prefix = entity_ref[len("skill:"):]
    return without_prefix.split(":")[0]


def _get_latest_payload(
    conn: Any,
    entity_ref: str,
    signal_type: str,
    run_id: str,
) -> Optional[dict]:
    """
    Return the most-recent payload dict for a given entity_ref + signal_type.

    Looks in the current *run_id* first.  If no row is found (e.g. a repo
    that was discovered in a previous run but not re-ingested today), falls
    back to the most-recent signal from **any** run.  This "carry-forward"
    behaviour lets previously-ingested repos stay on the leaderboard without
    a full re-ingest every day.
    """
    for extra_filter in (
        "AND run_id = ?",   # first: current run only
        "",                 # fallback: any run
    ):
        params: tuple = (
            (entity_ref, signal_type, run_id)
            if extra_filter
            else (entity_ref, signal_type)
        )
        row = conn.execute(
            f"""
            SELECT payload FROM raw_signals
            WHERE entity_ref = ? AND signal_type = ? {extra_filter}
            ORDER BY id DESC LIMIT 1
            """,
            params,
        ).fetchone()
        if row is not None:
            try:
                return json.loads(row["payload"])
            except (json.JSONDecodeError, TypeError) as exc:
                logger.warning(
                    "Malformed payload for %s/%s: %s", entity_ref, signal_type, exc
                )
                return None
    return None


def _parse_iso(ts: str) -> datetime.datetime:
    """Parse an ISO-8601 UTC string to a naive UTC datetime."""
    return datetime.datetime.fromisoformat(ts.replace("Z", "+00:00")).replace(tzinfo=None)


def _days_between(earlier: str, later: str, default: int = 0) -> int:
    """Return days between two ISO-8601 strings; return *default* on error."""
    try:
        return max(0, (_parse_iso(later) - _parse_iso(earlier)).days)
    except (ValueError, AttributeError):
        return default


def _compute_corpus_max(conn: Any, run_id: str) -> tuple[int, int, int]:
    """
    Scan all ``repo_metadata`` signals for the current run and return
    (max_stars, max_forks, max_watchers).  Falls back to 1 so log-normalisation
    never divides by zero.

    Performance note: this performs a full table scan of ``raw_signals``
    filtered by ``run_id``.  Call this **once** per pipeline run and cache
    the result — do not call it inside per-repo or per-skill loops.
    The result is also stored in ``pipeline_runs.stats`` so future tooling
    can read it without re-scanning the signals table.
    """
    rows = conn.execute(
        "SELECT payload FROM raw_signals WHERE signal_type = 'repo_metadata' AND run_id = ?",
        (run_id,),
    ).fetchall()
    max_stars = max_forks = max_watchers = 1
    for row in rows:
        try:
            p = json.loads(row["payload"])
        except (json.JSONDecodeError, TypeError):
            continue
        max_stars = max(max_stars, p.get("stars", 0))
        max_forks = max(max_forks, p.get("forks", 0))
        max_watchers = max(max_watchers, p.get("watchers", 0))
    return max_stars, max_forks, max_watchers


# ---------------------------------------------------------------------------
# Scoring + entity resolution
# ---------------------------------------------------------------------------

def score_and_store_skills(
    conn: Any,
    run_id: str,
    config: dict,
    corpus_max: Optional[tuple[int, int, int]] = None,
    llm_categorizer: Optional[Any] = None,
) -> int:
    """
    Second pass over every ``skill_file`` signal in *run_id*:

    1. Join associated repo-level signals (metadata, commits, contributors,
       code_quality).
    2. Compute the six dimension scores.
    3. Run the categorisation cascade.
    4. Upsert the entity record.
    5. Store 9 scores: 6 dimensions + ``composite:trending``,
       ``composite:popular``, ``composite:well_rounded``.

    *corpus_max* — optional pre-computed ``(max_stars, max_forks,
    max_watchers)`` tuple from the ingestion phase.  When provided the
    full-table scan inside :func:`_compute_corpus_max` is skipped.

    Returns the number of skills successfully scored.
    """
    now = _utcnow_str()
    # Use the pre-computed values when available to avoid a redundant full
    # table scan of raw_signals.  _compute_corpus_max is expensive (O(n)
    # over all repo_metadata rows) and the result does not change between
    # ingestion and scoring within the same pipeline run.
    if corpus_max is not None:
        corpus_max_stars, corpus_max_forks, corpus_max_watchers = corpus_max
    else:
        corpus_max_stars, corpus_max_forks, corpus_max_watchers = _compute_corpus_max(conn, run_id)

    # Current run's skill_file signals (freshly ingested today).
    current_skill_rows = conn.execute(
        "SELECT entity_ref, payload FROM raw_signals "
        "WHERE signal_type = 'skill_file' AND run_id = ?",
        (run_id,),
    ).fetchall()

    # Carry-forward: entities with recent skill_file signals from *previous*
    # runs that were NOT re-ingested today.  This keeps repos on the
    # leaderboard without doubling ingest work.
    carry_forward_days = config.get("discovery", {}).get("carry_forward_days", 7)
    carry_forward_rows = conn.execute(
        f"""
        SELECT r.entity_ref, r.payload
        FROM raw_signals r
        WHERE r.signal_type = 'skill_file'
          AND datetime(r.collected_at) > datetime('now', '-{int(carry_forward_days)} days')
          AND r.entity_ref NOT IN (
              SELECT entity_ref FROM raw_signals
              WHERE signal_type = 'skill_file' AND run_id = ?
          )
          AND r.id = (
              SELECT MAX(id) FROM raw_signals
              WHERE signal_type = 'skill_file' AND entity_ref = r.entity_ref
          )
        """,
        (run_id,),
    ).fetchall()

    if carry_forward_rows:
        logger.info(
            "Carry-forward: scoring %d entities from recent runs not re-ingested today.",
            len(carry_forward_rows),
        )

    skill_rows = list(current_skill_rows) + list(carry_forward_rows)

    scored = 0
    seen_hashes: dict[str, str] = {}  # content_hash → first entity_ref that claimed it

    for skill_row in skill_rows:
        entity_ref: str = skill_row["entity_ref"]
        try:
            skill_payload: dict = json.loads(skill_row["payload"])
        except (json.JSONDecodeError, TypeError) as exc:
            logger.warning("Malformed skill_file payload for %s: %s", entity_ref, exc)
            continue

        # Content-hash deduplication: skip copy-paste repos (same SKILL.md content
        # as a different, already-scored entity in this run).
        content_hash = skill_payload.get("content_hash")
        if content_hash:
            if content_hash in seen_hashes:
                logger.debug(
                    "Skipping %s — duplicate content (hash %s already seen for %s)",
                    entity_ref, content_hash, seen_hashes[content_hash],
                )
                continue
            seen_hashes[content_hash] = entity_ref

        repo_full_name = _repo_from_entity_ref(entity_ref)

        # Gather repo-level signals (all keyed by full_name in raw_signals)
        repo_meta = _get_latest_payload(conn, repo_full_name, "repo_metadata", run_id)
        commits    = _get_latest_payload(conn, repo_full_name, "commits", run_id)
        contribs   = _get_latest_payload(conn, repo_full_name, "contributors", run_id)
        cq_raw     = _get_latest_payload(conn, repo_full_name, "code_quality", run_id)

        if not all([repo_meta, commits, contribs, cq_raw]):
            logger.warning("Skipping %s — missing repo-level signals", entity_ref)
            continue

        # Temporal helpers
        repo_age_days         = _days_between(repo_meta.get("created_at", "2000-01-01T00:00:00Z"), now, default=9999)
        pushed_at_raw = repo_meta.get("pushed_at", "")
        if not pushed_at_raw:
            # pushed_at absent entirely → treat as fresh if repo is ≤ 30 days old,
            # otherwise fall back to the pessimistic 365-day default.
            days_since_last_commit = 0 if repo_age_days <= 30 else 365
        else:
            days_since_last_commit = _days_between(pushed_at_raw, now, default=365)

        # Monorepo dampening: count all skills in this repo for this run
        skill_count = conn.execute(
            "SELECT COUNT(*) FROM raw_signals "
            "WHERE signal_type = 'skill_file' AND run_id = ? "
            "AND entity_ref LIKE ?",
            (run_id, f"skill:{repo_full_name}%"),
        ).fetchone()[0]

        # --- Dimension scores ---
        vel = score_velocity(
            commit_count_30d=commits.get("commit_count_30d", 0),
            commit_count_prev_30d=commits.get("commit_count_prev_30d", 0),
            unique_commit_weeks_90d=commits.get("unique_commit_weeks_90d", 0),
            repo_age_days=repo_age_days,
            config=config["scoring"],
        )
        adop = score_adoption(
            stars=repo_meta.get("stars", 0),
            forks=repo_meta.get("forks", 0),
            watchers=repo_meta.get("watchers", 0),
            corpus_max_stars=corpus_max_stars,
            corpus_max_forks=corpus_max_forks,
            corpus_max_watchers=corpus_max_watchers,
            skill_count=skill_count,
            config=config["scoring"],
        )
        fresh = score_freshness(
            days_since_last_commit=days_since_last_commit,
            commit_count_90d=commits.get("commit_count_90d", 0),
            repo_age_days=repo_age_days,
            config=config["scoring"],
        )
        description_text = skill_payload.get("frontmatter_description") or ""
        doc = score_documentation(
            has_frontmatter=skill_payload.get("has_frontmatter", False),
            has_name=bool(skill_payload.get("frontmatter_name")),
            has_description=bool(description_text),
            description_len=len(description_text),
            line_count=skill_payload.get("line_count", 0),
            has_examples=skill_payload.get("has_examples_section", False),
            has_usage=skill_payload.get("has_usage_section", False),
            has_readme=skill_payload.get("has_readme", False),
            has_scripts=skill_payload.get("has_scripts_dir", False),
            has_references=skill_payload.get("has_references_dir", False),
            config=config["scoring"],
        )
        contrib = score_contributors(
            contributor_count=contribs.get("contributor_count", 0),
            config=config["scoring"],
        )
        cq = score_code_quality(
            has_license=repo_meta.get("has_license", False),
            has_workflows=cq_raw.get("has_github_dir", False),
            has_tests=cq_raw.get("has_tests", False),
            has_gitignore=cq_raw.get("has_gitignore", False),
            has_topics=bool(repo_meta.get("topics")),
        )

        # --- Categorise ---
        # Incremental skip: if the entity already has a non-"other" category
        # from a previous run, reuse it so we don't burn LLM tokens on
        # already-classified skills. Only newly ingested or still-"other"
        # skills pass through the LLM.
        existing_entity = get_entity(conn, entity_ref)
        existing_category = (
            existing_entity["category"]
            if existing_entity and existing_entity["category"] != "other"
            else None
        )
        if existing_category:
            category = existing_category
        else:
            category = categorize(
                frontmatter_category=skill_payload.get("frontmatter_category"),
                frontmatter_tags=skill_payload.get("frontmatter_tags") or [],
                name=skill_payload.get("frontmatter_name") or "",
                description=description_text,
                repo_topics=repo_meta.get("topics") or [],
                skill_path=skill_payload.get("skill_path", ""),
                readme_excerpt="",  # not fetched in v0.1
                config=config["categories"],
                llm_categorizer=llm_categorizer,
            )

        # --- Upsert entity ---
        upsert_entity(
            conn,
            entity_id=entity_ref,
            entity_type="skill",
            name=skill_payload.get("frontmatter_name") or repo_full_name,
            description=description_text or None,
            metadata={
                "repo": repo_full_name,
                "skill_path": skill_payload.get("skill_path"),
                "stars": repo_meta.get("stars", 0),
                "forks": repo_meta.get("forks", 0),
                "topics": repo_meta.get("topics", []),
            },
            category=category,
            now=now,
        )

        # --- Store dimension scores ---
        dim_scores = {
            "velocity": vel,
            "adoption": adop,
            "freshness": fresh,
            "documentation": doc,
            "contributors": contrib,
            "code_quality": cq,
        }
        for dim, value in dim_scores.items():
            store_score(conn, entity_ref, dim, round(value, 6), now, run_id)

        # --- Store composite scores ---
        for methodology in config["scoring"].get("methodologies", {}).keys():
            composite = compute_composite(
                velocity=vel,
                adoption=adop,
                freshness=fresh,
                documentation=doc,
                contributors=contrib,
                code_quality=cq,
                methodology=methodology,
                config=config["scoring"],
            )
            store_score(
                conn, entity_ref, f"composite:{methodology}",
                round(composite, 4), now, run_id,
            )

        scored += 1

    return scored


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
    discovery_cfg = config["discovery"]
    cap = max_repos or discovery_cfg.get("max_repos", 2000)
    log_interval = discovery_cfg.get("api", {}).get("progress_log_interval", 100)
    filters = discovery_cfg.get("filters", {})

    conn = get_connection(db_path or str(DB_PATH))
    try:
        init_db(conn)

        run_id = str(uuid.uuid4())
        started_at = _utcnow_str()

        upsert_signal_source(conn, SOURCE_ID, "GitHub API", last_run_at=started_at)
        start_pipeline_run(conn, run_id, SURFACE_ID, started_at)

        try:
            with GitHubClient(token=token) as client:
                # --- Discovery ---
                logger.info("Starting discovery (cap=%d)...", cap)
                discovered = discover(client, discovery_cfg, max_repos=cap)
                logger.info("Discovered %d repos.", len(discovered))

                # --- DB retention: log repos not seen today; scored via carry-forward ---
                # Previously-known repos that are absent from today's discovery sample
                # are NOT re-queued for a full re-ingest (that doubled runtime and caused
                # 6h timeouts).  Instead, score_and_store_skills() carries forward their
                # most recent signals automatically within the carry_forward_days window.
                known_repos      = get_known_repo_names(conn)
                discovered_names = {dr.full_name for dr in discovered}
                missing_count    = len(known_repos - discovered_names)
                if missing_count:
                    logger.info(
                        "DB retention: %d known repos not in today's sample; "
                        "will be scored via carry-forward (no re-ingest).",
                        missing_count,
                    )

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

                # Compute corpus-wide maxima once after ingestion so scoring can
                # use the cached values rather than re-scanning the raw_signals
                # table.  The result is also persisted in pipeline_runs.stats so
                # future tooling can read it without a full table scan.
                corpus_max = _compute_corpus_max(conn, run_id)

                # --- Instantiate LLM categoriser (once per run) ---
                _llm: Optional[LLMCategorizer] = None
                if os.environ.get("ANTHROPIC_API_KEY"):
                    try:
                        _llm = LLMCategorizer(config["categories"])
                        logger.info("LLM categoriser initialised (model=%s).", _llm._model)
                    except Exception as exc:
                        logger.warning(
                            "LLM categoriser init failed: %s. Keyword cascade only.", exc
                        )

                # --- Scoring + entity resolution ---
                logger.info("Scoring skills...")
                scored = score_and_store_skills(
                    conn, run_id, config, corpus_max=corpus_max, llm_categorizer=_llm
                )
                logger.info("Scored %d skills.", scored)

                completed_at = _utcnow_str()
                complete_pipeline_run(
                    conn, run_id, completed_at,
                    stats={
                        "repos_discovered": len(discovered),
                        "missing_from_sample": missing_count,
                        "valid_skills": valid_skills,
                        "scored_skills": scored,
                        "errors": errors,
                        "corpus_max_stars": corpus_max[0],
                        "corpus_max_forks": corpus_max[1],
                        "corpus_max_watchers": corpus_max[2],
                    },
                )

        except Exception as exc:
            logger.exception("Pipeline failed: %s", exc)
            failed_at = _utcnow_str()
            complete_pipeline_run(conn, run_id, failed_at, status=RUN_STATUS_FAILED,
                                  stats={"error": str(exc)})
            raise
    finally:
        conn.close()

    return run_id


if __name__ == "__main__":
    run()
