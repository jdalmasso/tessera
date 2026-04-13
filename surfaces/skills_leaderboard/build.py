"""
Static site generator for the Tessera Skills Leaderboard.

Reads scores from the DB (latest completed run), assembles template
context, renders template.html → build/index.html.

Usage::

    python -m surfaces.skills_leaderboard.build
    python -m surfaces.skills_leaderboard.build --db /path/to/tessera.db
    python -m surfaces.skills_leaderboard.build --output /path/to/build/
"""

from __future__ import annotations

import argparse
import os
import statistics
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from jinja2 import Environment, FileSystemLoader

from data.store import get_connection, get_latest_completed_run, get_previous_completed_run
from surfaces.skills_leaderboard.pipeline import load_config
from surfaces.skills_leaderboard.seed_report import collect_run_data, dist_stats

_PROJECT_ROOT = Path(__file__).parent.parent.parent
_TEMPLATE_DIR = Path(__file__).parent / "templates"
DB_PATH = Path(os.environ.get("TESSERA_DB_PATH", _PROJECT_ROOT / "db" / "tessera.db"))
BUILD_DIR = _PROJECT_ROOT / "build"
SURFACE_ID = "skills_leaderboard"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_utc(utc_str: str) -> str:
    """Convert an ISO-8601 UTC string to a human-readable UTC string."""
    try:
        dt = datetime.fromisoformat(utc_str.replace("Z", "+00:00"))
        utc = dt.astimezone(timezone.utc)
        return utc.strftime("%Y-%m-%d %H:%M UTC")
    except (ValueError, AttributeError):
        return utc_str


def _time_ago(iso_str: str, _now: Optional[datetime] = None) -> str:
    """Convert a UTC ISO-8601 timestamp to a human-readable relative string.

    Examples: "today", "3 days ago", "2 weeks ago", "4 months ago", "1 year ago".
    Returns an empty string if *iso_str* is empty; falls back to *iso_str* on
    any parse error. Pass *_now* in tests to pin the reference time.
    """
    if not iso_str:
        return ""
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        now = _now if _now is not None else datetime.now(timezone.utc)
        days = (now - dt).days
    except (ValueError, AttributeError):
        return iso_str

    if days < 1:
        return "today"
    if days == 1:
        return "1 day ago"
    if days < 7:
        return f"{days} days ago"
    if days < 14:
        return "1 week ago"
    if days < 30:
        return f"{days // 7} weeks ago"
    if days < 60:
        return "1 month ago"
    if days < 365:
        return f"{days // 30} months ago"
    years = days // 365
    return "1 year ago" if years == 1 else f"{years} years ago"


def _format_int(value: Any) -> str:
    """Format an integer with comma thousands separator."""
    try:
        return f"{int(value):,}"
    except (TypeError, ValueError):
        return str(value)


def _fetch_previous_ranks(conn: Any, run_id: str) -> dict[str, int]:
    """
    Return a dict mapping entity_id → rank from the previous completed run.
    Rank is computed by ordering composite:trending scores descending.
    Returns an empty dict if there is no previous run.
    """
    prev_run = get_previous_completed_run(conn, SURFACE_ID, run_id)
    if prev_run is None:
        return {}

    rows = conn.execute(
        """
        SELECT entity_id, value
        FROM scores
        WHERE dimension = 'composite:trending' AND run_id = ?
        ORDER BY value DESC
        """,
        (prev_run["id"],),
    ).fetchall()

    return {row["entity_id"]: rank for rank, row in enumerate(rows, 1)}


def _category_name(cat_id: str, categories_config: list[dict]) -> str:
    """Look up a category display name from the categories.yaml list."""
    for cat in categories_config:
        if cat["id"] == cat_id:
            return cat["name"]
    return cat_id.replace("_", " ").title()


# ---------------------------------------------------------------------------
# Context assembly
# ---------------------------------------------------------------------------

def build_context(data: dict, config: dict, conn: Optional[Any] = None) -> dict:
    """
    Transform raw DB data (from collect_run_data) into the Jinja2
    template context.

    Returns a dict with keys: config, last_updated, main_skills,
    categories, collections, stats, css.

    If *conn* is provided, rank deltas vs the previous completed run are
    computed. Otherwise all skills are marked as NEW.
    """
    site_cfg        = config["site"]
    categories_cfg  = config["categories"]["categories"]
    entity_scores   = data["entity_scores"]
    entity_meta     = data["entity_meta"]
    run_meta        = data["run_meta"]

    top_n_main      = site_cfg.get("top_n_main", 10)
    top_n_cat       = site_cfg.get("top_n_category", 10)
    max_per_repo    = site_cfg.get("display_caps", {}).get("max_per_repo", 3)
    max_per_author  = site_cfg.get("display_caps", {}).get("max_per_author", 5)
    min_coll_skills = site_cfg.get("collections", {}).get("min_skills", 2)
    top_n_coll_rank = max(1, site_cfg.get("collections", {}).get("top_n_for_ranking", 3))

    last_updated = _to_utc(run_meta.get("completed_at", ""))

    # ── Rank deltas from previous run ──
    prev_ranks: dict[str, int] = {}
    if conn is not None:
        prev_ranks = _fetch_previous_ranks(conn, run_meta["run_id"])

    # ── Build flat skill list sorted by trending desc ──
    def _skill_dict(eid: str, rank: int, delta: int, is_new: bool) -> dict:
        s    = entity_scores[eid]
        meta = entity_meta[eid]
        repo = meta["metadata"].get("repo", eid.replace("skill:", "").split(":")[0])
        author = repo.split("/")[0] if "/" in repo else repo
        return {
            "entity_id":          eid,
            "rank":               rank,
            "delta":              delta,
            "is_new":             is_new,
            "name":               meta["name"],
            "author":             author,
            "repo":               repo,
            "category":           meta["category"],
            "category_name":      _category_name(meta["category"], categories_cfg),
            "description":        meta.get("description") or "",
            "stars":              meta["metadata"].get("stars", 0),
            "last_updated":       _time_ago(meta.get("pushed_at") or ""),
            "composite_trending": s.get("composite:trending", 0.0),
            "composite_popular":  s.get("composite:popular", 0.0),
            "composite_well_rounded": s.get("composite:well_rounded", 0.0),
            "velocity":           s.get("velocity", 0.0),
            "adoption":           s.get("adoption", 0.0),
            "freshness":          s.get("freshness", 0.0),
            "documentation":      s.get("documentation", 0.0),
            "contributors":       s.get("contributors", 0.0),
            "code_quality":       s.get("code_quality", 0.0),
        }

    ranked_eids = sorted(
        [e for e in entity_scores if "composite:trending" in entity_scores[e]],
        key=lambda e: entity_scores[e]["composite:trending"],
        reverse=True,
    )

    all_skills = []
    for rank, eid in enumerate(ranked_eids, 1):
        if eid in prev_ranks:
            delta  = prev_ranks[eid] - rank   # positive = moved up
            is_new = False
        else:
            delta  = 0
            is_new = True
        all_skills.append(_skill_dict(eid, rank, delta, is_new))

    # ── Main leaderboard with display caps ──
    main_skills: list[dict] = []
    repo_counts:   dict[str, int] = defaultdict(int)
    author_counts: dict[str, int] = defaultdict(int)

    for skill in all_skills:
        if len(main_skills) >= top_n_main:
            break
        if repo_counts[skill["repo"]] >= max_per_repo:
            continue
        if author_counts[skill["author"]] >= max_per_author:
            continue
        main_skills.append(skill)
        repo_counts[skill["repo"]]     += 1
        author_counts[skill["author"]] += 1

    # ── Per-category sections ──
    cat_skill_map: dict[str, list[dict]] = defaultdict(list)
    for skill in all_skills:
        cat_skill_map[skill["category"]].append(skill)

    categories_ctx = []
    for cat in categories_cfg:
        cid    = cat["id"]
        skills = cat_skill_map.get(cid, [])
        categories_ctx.append({
            "id":         cid,
            "name":       cat["name"],
            "skills":     skills,
            "top_skills": skills[:top_n_cat],
        })

    # ── Collections ──
    repo_skills: dict[str, list[dict]] = defaultdict(list)
    for skill in all_skills:
        if skill["entity_id"].count(":") >= 2:
            repo_skills[skill["repo"]].append(skill)

    collections_ctx = []
    for repo, skills in repo_skills.items():
        if len(skills) < min_coll_skills:
            continue
        top = sorted(skills, key=lambda s: s["composite_trending"], reverse=True)
        avg = statistics.mean(s["composite_trending"] for s in top[:top_n_coll_rank])
        collections_ctx.append({
            "repo":        repo,
            "skill_count": len(skills),
            "avg_trending": avg,
            "top_skills":  top[:3],
        })

    collections_ctx.sort(key=lambda c: c["avg_trending"], reverse=True)

    # ── Stats block ──
    run_stats  = run_meta.get("stats", {})
    total      = len(entity_scores)
    cat_counts = defaultdict(int)
    for meta in entity_meta.values():
        cat_counts[meta["category"]] += 1

    cat_dist = []
    for cat in categories_cfg:
        cnt = cat_counts.get(cat["id"], 0)
        cat_dist.append({
            "id":    cat["id"],
            "name":  cat["name"],
            "count": cnt,
            "pct":   100.0 * cnt / total if total > 0 else 0.0,
        })
    cat_dist.sort(key=lambda x: -x["count"])

    score_dists = []
    for dim, label in [
        ("composite:trending",    "Trending"),
        ("composite:popular",     "Popular"),
        ("composite:well_rounded","Well-Rounded"),
    ]:
        vals = [entity_scores[e][dim] for e in entity_scores if dim in entity_scores[e]]
        s    = dist_stats(vals)
        score_dists.append({"label": label, **s})

    stats_ctx = {
        "total_skills":        total,
        "repos_discovered":    run_stats.get("repos_discovered", 0),
        "category_count":      len([c for c in cat_counts if cat_counts[c] > 0]),
        "score_distributions": score_dists,
        "category_distribution": cat_dist,
    }

    # ── CSS ──
    css = (_TEMPLATE_DIR / "style.css").read_text(encoding="utf-8")

    return {
        "config":      site_cfg,
        "last_updated": last_updated,
        "main_skills": main_skills,
        "categories":  categories_ctx,
        "collections": collections_ctx,
        "stats":       stats_ctx,
        "css":         css,
    }


# ---------------------------------------------------------------------------
# Render
# ---------------------------------------------------------------------------

def render(context: dict) -> str:
    """Render the Jinja2 template with the given context."""
    env = Environment(
        loader=FileSystemLoader(str(_TEMPLATE_DIR)),
        autoescape=True,
    )
    env.filters["format_int"] = _format_int
    env.filters["abs"]        = abs

    template = env.get_template("template.html")
    return template.render(**context)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main(
    db_path:    Optional[str]  = None,
    run_id:     Optional[str]  = None,
    output_dir: Optional[Path] = None,
) -> Path:
    """
    Build the static site. Returns the path to the written index.html.
    """
    conn = get_connection(db_path or str(DB_PATH))

    if run_id is None:
        run_row = get_latest_completed_run(conn, SURFACE_ID)
        if run_row is None:
            raise RuntimeError(
                "No completed pipeline run found. Run 'make pipeline' first."
            )
        run_id = run_row["id"]

    print(f"Building site for run {run_id} ...")

    config = load_config()
    data   = collect_run_data(conn, run_id)
    ctx    = build_context(data, config, conn=conn)
    html   = render(ctx)

    out_dir = output_dir or BUILD_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "index.html"
    out_file.write_text(html, encoding="utf-8")

    print(f"Site written → {out_file}  ({len(html):,} bytes, {len(ctx['main_skills'])} main skills)")
    return out_file


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build Tessera leaderboard site")
    parser.add_argument("--db",     help="Path to tessera.db")
    parser.add_argument("--run-id", help="Pipeline run ID (default: latest)")
    parser.add_argument("--output", help="Output directory (default: build/)")
    args = parser.parse_args()

    main(
        db_path=args.db,
        run_id=args.run_id,
        output_dir=Path(args.output) if args.output else None,
    )
