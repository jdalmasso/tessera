"""
Seed calibration report generator.

Connects to the Tessera DB, reads scores from the latest completed
pipeline run, and writes ``seed-run-report.md`` to the project root.

Usage::

    python -m surfaces.skills_leaderboard.seed_report
    python -m surfaces.skills_leaderboard.seed_report --run-id <uuid>
    python -m surfaces.skills_leaderboard.seed_report --db /path/to/db
    python -m surfaces.skills_leaderboard.seed_report --output /path/to/report.md

Output: ``seed-run-report.md`` (project root by default).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import statistics
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Optional

from data.models import DIMENSIONS
from data.store import get_connection, get_latest_completed_run

_PROJECT_ROOT = Path(__file__).parent.parent.parent
DB_PATH = Path(os.environ.get("TESSERA_DB_PATH", _PROJECT_ROOT / "db" / "tessera.db"))
SURFACE_ID = "skills_leaderboard"
REPORT_PATH = _PROJECT_ROOT / "seed-run-report.md"

logger = logging.getLogger(__name__)

# Dimension groups
_DIM_SCORES = [d for d in DIMENSIONS if not d.startswith("composite:")]
_DIM_COMPOSITES = [d for d in DIMENSIONS if d.startswith("composite:")]


# ---------------------------------------------------------------------------
# Statistics helpers
# ---------------------------------------------------------------------------

def _percentile(sorted_vals: list[float], p: float) -> float:
    """Linear-interpolation percentile on a pre-sorted list."""
    n = len(sorted_vals)
    if n == 0:
        return 0.0
    if n == 1:
        return sorted_vals[0]
    idx = p / 100.0 * (n - 1)
    lo = int(idx)
    hi = min(lo + 1, n - 1)
    return sorted_vals[lo] + (idx - lo) * (sorted_vals[hi] - sorted_vals[lo])


def dist_stats(values: list[float]) -> dict[str, float]:
    """Return min/max/mean/median/p25/p75/stddev for a list of values."""
    if not values:
        return {"count": 0, "min": 0.0, "max": 0.0, "mean": 0.0,
                "median": 0.0, "p25": 0.0, "p75": 0.0, "stddev": 0.0}
    sv = sorted(values)
    return {
        "count": len(sv),
        "min":    sv[0],
        "max":    sv[-1],
        "mean":   statistics.mean(sv),
        "median": statistics.median(sv),
        "p25":    _percentile(sv, 25),
        "p75":    _percentile(sv, 75),
        "stddev": statistics.stdev(sv) if len(sv) > 1 else 0.0,
    }


def is_degenerate(values: list[float], threshold: float = 0.80) -> bool:
    """
    Return True if more than *threshold* fraction of values share the same
    rounded value — a sign that the dimension is not discriminating.
    """
    if not values:
        return False
    most_common_count = Counter(round(v, 4) for v in values).most_common(1)[0][1]
    return most_common_count / len(values) > threshold


# ---------------------------------------------------------------------------
# Data collection
# ---------------------------------------------------------------------------

def collect_run_data(conn: Any, run_id: str) -> dict:
    """
    Pull all scores and entity metadata for *run_id* from the DB.

    Returns::

        {
            "run_meta":     {run_id, started_at, completed_at, stats},
            "entity_scores": {entity_id: {dimension: value, ...}, ...},
            "entity_meta":   {entity_id: {name, category, metadata}, ...},
        }
    """
    # Run metadata
    run_row = conn.execute(
        "SELECT * FROM pipeline_runs WHERE id = ?", (run_id,)
    ).fetchone()

    run_meta: dict[str, Any] = {
        "run_id": run_id,
        "started_at":   run_row["started_at"]   if run_row else "unknown",
        "completed_at": run_row["completed_at"] if run_row else "unknown",
        "stats": {},
    }
    if run_row and run_row["stats"]:
        try:
            run_meta["stats"] = json.loads(run_row["stats"])
        except (json.JSONDecodeError, TypeError):
            pass

    # Scores + entity metadata
    rows = conn.execute(
        """
        SELECT s.entity_id, s.dimension, s.value,
               e.name, e.category, e.description, e.metadata
        FROM   scores s
        JOIN   entities e ON e.id = s.entity_id
        WHERE  s.run_id = ?
        """,
        (run_id,),
    ).fetchall()

    entity_scores: dict[str, dict[str, float]] = {}
    entity_meta:   dict[str, dict[str, Any]]   = {}

    for row in rows:
        eid = row["entity_id"]
        if eid not in entity_scores:
            entity_scores[eid] = {}
            try:
                meta = json.loads(row["metadata"]) if row["metadata"] else {}
            except (json.JSONDecodeError, TypeError):
                meta = {}
            entity_meta[eid] = {
                "name":        row["name"],
                "category":    row["category"],
                "description": row["description"] or "",
                "metadata":    meta,
                "pushed_at":   "",
            }
        entity_scores[eid][row["dimension"]] = row["value"]

    # Enrich entity_meta with pushed_at from repo_metadata raw signals
    pushed_at_map: dict[str, str] = {}
    try:
        meta_rows = conn.execute(
            """
            SELECT entity_ref, payload
            FROM raw_signals
            WHERE signal_type = 'repo_metadata' AND run_id = ?
            """,
            (run_id,),
        ).fetchall()
        seen: set[str] = set()
        for mrow in meta_rows:
            ref = mrow["entity_ref"]
            if ref in seen:
                continue
            seen.add(ref)
            try:
                payload = json.loads(mrow["payload"])
                pushed_at_map[ref] = payload.get("pushed_at", "")
            except (json.JSONDecodeError, TypeError):
                pass
    except Exception:
        pass  # raw_signals table may not exist in test fixtures

    for eid, meta in entity_meta.items():
        repo = eid[len("skill:"):].split(":")[0]
        meta["pushed_at"] = pushed_at_map.get(repo, "")

    return {
        "run_meta":      run_meta,
        "entity_scores": entity_scores,
        "entity_meta":   entity_meta,
    }


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def generate_report(data: dict) -> str:
    """
    Accept the dict returned by :func:`collect_run_data` and return the
    full calibration report as a Markdown string.
    """
    run_meta      = data["run_meta"]
    entity_scores = data["entity_scores"]
    entity_meta   = data["entity_meta"]
    total_skills  = len(entity_scores)

    L: list[str] = []

    # ── Header ──────────────────────────────────────────────────────────────
    L += [
        "# Tessera — Seed Calibration Report",
        "",
        f"**Run ID:** `{run_meta['run_id']}`  ",
        f"**Started:** {run_meta['started_at']}  ",
        f"**Completed:** {run_meta['completed_at']}  ",
        f"**Total skills scored:** {total_skills}  ",
    ]
    stats = run_meta["stats"]
    if stats:
        L += [
            f"**Repos discovered:** {stats.get('repos_discovered', 'n/a')}  ",
            f"**Valid skill files:** {stats.get('valid_skills', 'n/a')}  ",
            f"**Scored skills:** {stats.get('scored_skills', 'n/a')}  ",
            f"**Errors:** {stats.get('errors', 'n/a')}  ",
        ]

    if total_skills == 0:
        L += ["", "> ⚠️ No scored skills found for this run.", ""]
        return "\n".join(L)

    # ── Score distributions ──────────────────────────────────────────────────
    L += ["", "---", "", "## Score Distributions", "", "### Dimension Scores (range: 0.0 – 1.0)", ""]
    L += ["| Dimension | Count | Min | P25 | Median | P75 | Max | Mean | StdDev | Degenerate? |"]
    L += ["|-----------|-------|-----|-----|--------|-----|-----|------|--------|-------------|"]

    for dim in _DIM_SCORES:
        vals = [entity_scores[e][dim] for e in entity_scores if dim in entity_scores[e]]
        s    = dist_stats(vals)
        flag = "⚠️ YES" if is_degenerate(vals) else "no"
        L.append(
            f"| {dim} | {s['count']} "
            f"| {s['min']:.3f} | {s['p25']:.3f} | {s['median']:.3f} "
            f"| {s['p75']:.3f} | {s['max']:.3f} | {s['mean']:.3f} "
            f"| {s['stddev']:.3f} | {flag} |"
        )

    L += ["", "### Composite Scores (range: 0.0 – 100.0)", ""]
    L += ["| Methodology | Count | Min | P25 | Median | P75 | Max | Mean | StdDev |"]
    L += ["|-------------|-------|-----|-----|--------|-----|-----|------|--------|"]

    for dim in _DIM_COMPOSITES:
        label = dim.replace("composite:", "")
        vals  = [entity_scores[e][dim] for e in entity_scores if dim in entity_scores[e]]
        s     = dist_stats(vals)
        L.append(
            f"| {label} | {s['count']} "
            f"| {s['min']:.1f} | {s['p25']:.1f} | {s['median']:.1f} "
            f"| {s['p75']:.1f} | {s['max']:.1f} | {s['mean']:.1f} "
            f"| {s['stddev']:.1f} |"
        )

    # ── Category distribution ────────────────────────────────────────────────
    L += ["", "---", "", "## Category Distribution", ""]

    cat_counts = Counter(entity_meta[e]["category"] for e in entity_meta)
    total      = sum(cat_counts.values())
    other_pct  = 100.0 * cat_counts.get("other", 0) / total if total > 0 else 0.0
    other_flag = "  ⚠️ target >20% exceeded" if other_pct > 20 else ""

    L += ["| Category | Count | % of Total |"]
    L += ["|----------|-------|------------|"]
    for cat, count in sorted(cat_counts.items(), key=lambda x: -x[1]):
        pct  = 100.0 * count / total
        note = other_flag if cat == "other" else ""
        L.append(f"| {cat} | {count} | {pct:.1f}%{note} |")
    L.append(f"| **Total** | **{total}** | **100%** |")
    L += ["", f"**Other %:** {other_pct:.1f}%{other_flag}  "]

    # ── Monorepo / collection detection ─────────────────────────────────────
    L += ["", "---", "", "## Monorepo / Collection Detection", ""]

    repo_skill_counts: dict[str, int] = defaultdict(int)
    for eid in entity_scores:
        repo = eid[len("skill:"):].split(":")[0]
        repo_skill_counts[repo] += 1

    collection_repos   = {r: c for r, c in repo_skill_counts.items() if c > 1}
    skills_in_colls    = sum(collection_repos.values())
    monorepo_skill_ids = [e for e in entity_scores if e.count(":") >= 2]

    L += [
        f"- **Total skills:** {total_skills}",
        f"- **Skills with sub-path (monorepo entity refs):** {len(monorepo_skill_ids)}",
        f"- **Collection repos (>1 skill):** {len(collection_repos)}",
        f"- **Total skills in collections:** {skills_in_colls}",
        "",
    ]

    if collection_repos:
        L += ["| Repo | Skill Count |", "|------|------------|"]
        for repo, cnt in sorted(collection_repos.items(), key=lambda x: -x[1])[:20]:
            L.append(f"| {repo} | {cnt} |")

    # ── Top 20 by Trending ───────────────────────────────────────────────────
    L += ["", "---", "", "## Top 20 by Trending Score", ""]

    ranked = sorted(
        [e for e in entity_scores if "composite:trending" in entity_scores[e]],
        key=lambda e: entity_scores[e]["composite:trending"],
        reverse=True,
    )

    L += ["| # | Name | Category | Trending | Popular | Well-Rounded | Vel | Adop | Fresh | Doc | Contrib | CQ |"]
    L += ["|---|------|----------|----------|---------|--------------|-----|------|-------|-----|---------|-----|"]
    for rank, eid in enumerate(ranked[:20], 1):
        s    = entity_scores[eid]
        meta = entity_meta[eid]
        L.append(
            f"| {rank} | {meta['name'][:40]} | {meta['category']} "
            f"| {s.get('composite:trending', 0):.1f} "
            f"| {s.get('composite:popular', 0):.1f} "
            f"| {s.get('composite:well_rounded', 0):.1f} "
            f"| {s.get('velocity', 0):.2f} "
            f"| {s.get('adoption', 0):.2f} "
            f"| {s.get('freshness', 0):.2f} "
            f"| {s.get('documentation', 0):.2f} "
            f"| {s.get('contributors', 0):.2f} "
            f"| {s.get('code_quality', 0):.2f} |"
        )

    # ── Bottom 20 by Trending ────────────────────────────────────────────────
    L += ["", "---", "", "## Bottom 20 by Trending Score", ""]
    L += ["| # | Name | Category | Trending | Weakest Dimension | Score |"]
    L += ["|---|------|----------|----------|-------------------|-------|"]

    bottom = list(reversed(ranked[-20:])) if len(ranked) >= 20 else list(reversed(ranked))
    n_total = len(ranked)

    for i, eid in enumerate(bottom, 1):
        s    = entity_scores[eid]
        meta = entity_meta[eid]
        dim_only    = {d: s[d] for d in _DIM_SCORES if d in s}
        weakest_dim = min(dim_only, key=dim_only.get) if dim_only else "n/a"
        weakest_val = dim_only.get(weakest_dim, 0.0)
        rank        = n_total - i + 1
        L.append(
            f"| {rank} | {meta['name'][:40]} | {meta['category']} "
            f"| {s.get('composite:trending', 0):.1f} "
            f"| {weakest_dim} | {weakest_val:.3f} |"
        )

    # ── Anomaly flags ────────────────────────────────────────────────────────
    L += ["", "---", "", "## Anomaly Flags", ""]

    high_anom = sorted(
        [(e, entity_scores[e]["composite:trending"])
         for e in entity_scores
         if entity_scores[e].get("composite:trending", 0) > 95],
        key=lambda x: -x[1],
    )
    low_anom = sorted(
        [(e, entity_scores[e]["composite:trending"])
         for e in entity_scores
         if entity_scores[e].get("composite:trending", 0) < 5],
        key=lambda x: x[1],
    )

    label_hi = f"⬆️  High anomalies — Trending > 95 ({len(high_anom)} skills)"
    label_lo = f"⬇️  Low anomalies — Trending < 5 ({len(low_anom)} skills)"

    L += [f"### {label_hi}", ""]
    if high_anom:
        L += ["| Entity | Trending |", "|--------|----------|"]
        for eid, score in high_anom:
            L.append(f"| `{eid}` | {score:.1f} |")
    else:
        L.append("*None.*")

    L += ["", f"### {label_lo}", ""]
    if low_anom:
        L += ["| Entity | Trending |", "|--------|----------|"]
        for eid, score in low_anom:
            L.append(f"| `{eid}` | {score:.1f} |")
    else:
        L.append("*None.*")

    # ── Footer ───────────────────────────────────────────────────────────────
    L += [
        "",
        "---",
        "",
        "*Generated by `seed_report.py` · Run `make seed-run` to refresh.*",
    ]

    return "\n".join(L)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main(
    db_path: Optional[str] = None,
    run_id:  Optional[str] = None,
    output_path: Optional[Path] = None,
) -> Path:
    """
    Connect to the DB, find the latest completed run (or use *run_id*),
    generate the report, and write it to *output_path*.

    Returns the path of the written report.
    """
    conn = get_connection(db_path or str(DB_PATH))

    if run_id is None:
        run_row = get_latest_completed_run(conn, SURFACE_ID)
        if run_row is None:
            raise RuntimeError(
                "No completed pipeline run found in the DB. "
                "Run 'make pipeline' first."
            )
        run_id = run_row["id"]

    logger.info("Generating report for run %s ...", run_id)
    data   = collect_run_data(conn, run_id)
    report = generate_report(data)

    out = output_path or REPORT_PATH
    out.write_text(report, encoding="utf-8")
    logger.info("Report written → %s", out)
    return out


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate Tessera seed calibration report"
    )
    parser.add_argument("--db",     help="Path to tessera.db")
    parser.add_argument("--run-id", help="Pipeline run ID (default: latest completed)")
    parser.add_argument("--output", help="Output path (default: seed-run-report.md)")
    args = parser.parse_args()

    main(
        db_path=args.db,
        run_id=args.run_id,
        output_path=Path(args.output) if args.output else None,
    )
