"""
Dimension scoring functions for GitHub-sourced signals.

Generic — knows nothing about Skills. Each function accepts raw signal
values and returns a normalised float in [0.0, 1.0].

The six dimensions are:
  velocity      — commit momentum and acceleration
  adoption      — stars, forks, watchers (log-normalised against corpus)
  freshness     — recency and sustained activity
  documentation — structural completeness of a skill file
  contributors  — unique contributor count (log-scaled)
  code_quality  — presence of quality markers (license, CI, tests, etc.)

All sigmoid/decay parameters are read from config/scoring.yaml.
"""

import math
from typing import Any


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _piecewise(x: float, low: float, mid: float, high: float) -> float:
    """
    Piecewise-linear curve anchored at three points:
      x <= low  → 0.0
      x == mid  → 0.5
      x >= high → 1.0
    Linear interpolation between the segments.
    """
    if x <= low:
        return 0.0
    if x >= high:
        return 1.0
    if x <= mid:
        return 0.5 * (x - low) / (mid - low)
    return 0.5 + 0.5 * (x - mid) / (high - mid)


def _log_normalise(value: float, corpus_max: float) -> float:
    """
    Log-normalise a raw count against the corpus maximum:
      log(value + 1) / log(corpus_max + 1)
    Returns 0.0 when corpus_max is 0.
    """
    if corpus_max <= 0:
        return 0.0
    return math.log(value + 1) / math.log(corpus_max + 1)


# ---------------------------------------------------------------------------
# Velocity
# ---------------------------------------------------------------------------

def score_velocity(
    commit_count_30d: int,
    commit_count_prev_30d: int,
    unique_commit_weeks_90d: int,
    repo_age_days: int,
    config: dict[str, Any],
) -> float:
    """
    Momentum signal combining commit acceleration and consistency.

    Acceleration ratio: commits_30d / max(commits_prev_30d, 1), capped at
    `acceleration_cap` (default 2.0), then normalised to [0, 1].

    Consistency: unique commit weeks in last 90 days / total weeks (13).

    Minimum age rule: repos younger than `min_age_days` (default 14) return
    0.5 to avoid artificial perfect scores from brand-new repos.

    Final score: equal blend of normalised acceleration and consistency.
    """
    vcfg = config.get("velocity", {})
    min_age = config.get("thresholds", {}).get("min_age_days", 14)
    default_score = config.get("thresholds", {}).get("min_age_velocity_default", 0.5)
    cap = vcfg.get("acceleration_cap", 2.0)
    total_weeks = vcfg.get("consistency_weeks", 13)

    if repo_age_days < min_age:
        return float(default_score)

    # Acceleration
    prev = max(commit_count_prev_30d, 1)
    ratio = min(commit_count_30d / prev, cap)
    acceleration_score = ratio / cap

    # Consistency
    consistency_score = min(unique_commit_weeks_90d / total_weeks, 1.0)

    return 0.5 * acceleration_score + 0.5 * consistency_score


# ---------------------------------------------------------------------------
# Adoption
# ---------------------------------------------------------------------------

def score_adoption(
    stars: int,
    forks: int,
    watchers: int,
    corpus_max_stars: int,
    corpus_max_forks: int,
    corpus_max_watchers: int,
    skill_count: int = 1,
    config: dict[str, Any] = None,
) -> float:
    """
    Ecosystem adoption signal: log-normalised stars, forks, and watchers
    weighted and averaged.

    Monorepo dampening: for skills in collections (skill_count > 1), the
    effective stars used for normalisation are dampened by
    log(stars + 1) / log(skill_count + 1), reducing the adoption score
    proportionally to collection size.

    Weights: stars 0.5, forks 0.3, watchers 0.2 (from config).
    """
    cfg = (config or {}).get("adoption", {})
    weights = cfg.get("weights", {"stars": 0.5, "forks": 0.3, "watchers": 0.2})

    # Apply monorepo dampening to stars
    if skill_count > 1 and stars > 0:
        dampened_stars = math.log(stars + 1) / math.log(skill_count + 1)
        # Re-normalise against the same corpus max using the dampened value
        stars_score = dampened_stars / math.log(corpus_max_stars + 1) if corpus_max_stars > 0 else 0.0
        stars_score = min(stars_score, 1.0)
    else:
        stars_score = _log_normalise(stars, corpus_max_stars)

    forks_score = _log_normalise(forks, corpus_max_forks)
    watchers_score = _log_normalise(watchers, corpus_max_watchers)

    return (
        weights["stars"] * stars_score
        + weights["forks"] * forks_score
        + weights["watchers"] * watchers_score
    )


# ---------------------------------------------------------------------------
# Freshness
# ---------------------------------------------------------------------------

def score_freshness(
    days_since_last_commit: int,
    commit_count_90d: int,
    repo_age_days: int,
    config: dict[str, Any],
) -> float:
    """
    Recency and sustained activity signal.

    Decay component: exponential decay on days since last commit.
      score = 0.5 ^ (days / half_life)
      → 1.0 at 0 days, 0.5 at half_life (default 30d), ~0.1 at 180d.

    Activity component: piecewise-linear on 90-day commit count.
      → 0.0 at 0 commits, 0.5 at mid (default 5), 1.0 at sat (default 20+).

    Maturity component: binary — 1.0 if repo older than maturity_days, else 0.0.

    Final score: weighted sum (decay 0.5, activity 0.4, maturity 0.1).
    """
    fcfg = config.get("freshness", {})
    half_life = fcfg.get("half_life_days", 30)
    act_mid = fcfg.get("activity_sigmoid_mid", 5)
    act_sat = fcfg.get("activity_sigmoid_sat", 20)
    maturity_days = fcfg.get("maturity_days", 30)
    w = fcfg.get("weights", {"decay": 0.5, "activity": 0.4, "maturity": 0.1})

    decay_score = math.pow(0.5, days_since_last_commit / half_life)
    activity_score = _piecewise(commit_count_90d, 0, act_mid, act_sat)
    maturity_score = 1.0 if repo_age_days >= maturity_days else 0.0

    return (
        w["decay"] * decay_score
        + w["activity"] * activity_score
        + w["maturity"] * maturity_score
    )


# ---------------------------------------------------------------------------
# Documentation
# ---------------------------------------------------------------------------

def score_documentation(
    has_frontmatter: bool,
    has_name: bool,
    has_description: bool,
    description_len: int,
    line_count: int,
    has_examples: bool,
    has_usage: bool,
    has_readme: bool,
    has_scripts: bool,
    has_references: bool,
    config: dict[str, Any],
) -> float:
    """
    Structural completeness of a SKILL.md and its surrounding directory.

    Signals and their weights (from config):
      has_frontmatter    0.10  — YAML block present
      has_name           0.05  — name field present
      has_description    0.15  — description field with >min_chars characters
      line_count         0.20  — piecewise-linear on line count (low/mid/high)
      has_examples       0.15  — ## Examples heading present
      has_usage          0.10  — ## Usage heading present
      has_readme         0.10  — README.md exists alongside SKILL.md
      has_scripts        0.075 — scripts/ directory exists
      has_references     0.075 — references/ directory exists

    Returns weighted sum normalised to [0, 1].
    """
    dcfg = config.get("documentation", {})
    desc_min = dcfg.get("description_min_chars", 20)
    line_low = dcfg.get("line_count_low", 50)
    line_mid = dcfg.get("line_count_mid", 100)
    line_high = dcfg.get("line_count_high", 300)
    w = dcfg.get("weights", {
        "has_frontmatter": 0.10,
        "has_name": 0.05,
        "has_description": 0.15,
        "line_count": 0.20,
        "has_examples": 0.15,
        "has_usage": 0.10,
        "has_readme": 0.10,
        "has_scripts": 0.075,
        "has_references": 0.075,
    })

    description_ok = has_description and description_len >= desc_min

    return (
        w.get("has_frontmatter", 0.10) * float(has_frontmatter)
        + w.get("has_name", 0.05) * float(has_name)
        + w.get("has_description", 0.15) * float(description_ok)
        + w.get("line_count", 0.20) * _piecewise(line_count, line_low, line_mid, line_high)
        + w.get("has_examples", 0.15) * float(has_examples)
        + w.get("has_usage", 0.10) * float(has_usage)
        + w.get("has_readme", 0.10) * float(has_readme)
        + w.get("has_scripts", 0.075) * float(has_scripts)
        + w.get("has_references", 0.075) * float(has_references)
    )


# ---------------------------------------------------------------------------
# Contributors
# ---------------------------------------------------------------------------

def score_contributors(
    contributor_count: int,
    config: dict[str, Any],
) -> float:
    """
    Unique contributor signal — rewards the 1→2 jump (solo → externally
    validated) more than linear increments at higher counts.

    Formula: log(min(contributors, cap) + 1) / log(cap + 1)
      0 contributors → 0.0
      1 contributor  → log(2)  / log(11) ≈ 0.29
      2 contributors → log(3)  / log(11) ≈ 0.46
      5 contributors → log(6)  / log(11) ≈ 0.75
      10 contributors→ log(11) / log(11) = 1.0
    """
    cap = config.get("contributors", {}).get("log_cap", 10)
    capped = min(contributor_count, cap)
    return math.log(capped + 1) / math.log(cap + 1)


# ---------------------------------------------------------------------------
# Composite
# ---------------------------------------------------------------------------

DIMENSIONS = ["velocity", "adoption", "freshness", "documentation", "contributors", "code_quality"]


def compute_composite(
    velocity: float,
    adoption: float,
    freshness: float,
    documentation: float,
    contributors: float,
    code_quality: float,
    methodology: str,
    config: dict[str, Any],
) -> float:
    """
    Combine six dimension scores into a single 0–100 composite.

    Each dimension score is expected to be in [0.0, 1.0].
    Weights are loaded from config["methodologies"][methodology]["weights"]
    and must sum to 100 (so the result is already on a 0–100 scale).

    Supported methodologies (defined in scoring.yaml):
      trending      — momentum-weighted (velocity 25, adoption 20, …)
      popular       — adoption-weighted (adoption 30, …)
      well_rounded  — quality/docs-weighted (documentation 25, code_quality 25, …)

    Raises ValueError for unknown methodology names.
    """
    methodologies = config.get("methodologies", {})
    if methodology not in methodologies:
        raise ValueError(
            f"Unknown methodology: {methodology!r}. "
            f"Expected one of {sorted(methodologies)}"
        )

    weights = methodologies[methodology]["weights"]
    scores = {
        "velocity": velocity,
        "adoption": adoption,
        "freshness": freshness,
        "documentation": documentation,
        "contributors": contributors,
        "code_quality": code_quality,
    }

    return sum(weights[dim] * scores[dim] for dim in DIMENSIONS)


# ---------------------------------------------------------------------------
# Code quality
# ---------------------------------------------------------------------------

def score_code_quality(
    has_license: bool,
    has_workflows: bool,
    has_tests: bool,
    has_gitignore: bool,
    has_topics: bool,
) -> float:
    """
    Repository hygiene signal based on five binary checks:
      has_license    — LICENSE file present
      has_workflows  — .github/workflows/ exists
      has_tests      — tests/ directory or test files present
      has_gitignore  — .gitignore present
      has_topics     — GitHub repo topics are set

    Score = (number of signals present) / 5.
    """
    signals = [has_license, has_workflows, has_tests, has_gitignore, has_topics]
    return sum(1 for s in signals if s) / len(signals)
