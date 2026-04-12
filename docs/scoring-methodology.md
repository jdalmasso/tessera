# Scoring Methodology

This document describes how Tessera scores Claude Code Skills across six dimensions and three composite methodologies.

## Dimensions

- **Velocity** — commit momentum and acceleration over the last 30–90 days
- **Adoption** — stars, forks, and watchers, log-normalized against the corpus
- **Freshness** — recency of last commit plus sustained activity over the last 90 days
- **Documentation** — structural completeness of SKILL.md and supporting files
- **Contributors** — unique contributor count, log-scaled to reward early external validation
- **Code Quality** — presence of license, CI workflows, tests, .gitignore, and repo topics

## Composite Methodologies

| Methodology  | Default? | Focus                  |
|--------------|----------|------------------------|
| Trending     | Yes      | Momentum signals       |
| Popular      | No       | Ecosystem adoption     |
| Well-Rounded | No       | Balanced strength      |

## Weight Tables

See `config/scoring.yaml` for the authoritative weight values per methodology.

| Dimension     | Trending | Popular | Well-Rounded |
|---------------|----------|---------|--------------|
| Velocity      | 25       | 10      | 10           |
| Adoption      | 20       | 30      | 15           |
| Freshness     | 20       | 20      | 15           |
| Documentation | 15       | 15      | 25           |
| Contributors  | 10       | 15      | 10           |
| Code Quality  | 10       | 10      | 25           |

## Composite Calculation

```python
composite = sum(dimension_scores[dim] * weights[dim] for dim in weights)
final_score = round(composite * 100)  # 0–100
```

Three composites are stored per entity per pipeline run: `composite:trending`, `composite:popular`, `composite:well_rounded`.

## Formulas and Thresholds

All sigmoid parameters, decay half-lives, log caps, and minimum thresholds are defined in `config/scoring.yaml`.
