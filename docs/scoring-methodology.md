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

| Dimension     | Trending | Popular | Well-Rounded |
|---------------|----------|---------|--------------|
| Velocity      | 25       | 10      | 10           |
| Adoption      | 20       | 30      | 15           |
| Freshness     | 20       | 20      | 15           |
| Documentation | 15       | 15      | 25           |
| Contributors  | 10       | 15      | 10           |
| Code Quality  | 10       | 10      | 25           |

## Composite Calculation

Each dimension produces a score in [0, 1]. The composite is a weighted sum divided by 100 (because weights sum to 100), then rounded to an integer in [0, 100]:

```python
composite = sum(dimension_score[dim] * weight[dim] for dim in weights) / 100
final_score = round(composite * 100)  # 0–100 integer
```

Three composites are stored per entity per pipeline run: `composite:trending`, `composite:popular`, `composite:well_rounded`.

## Dimension Formulas

### Velocity

Measures commit acceleration over the last 30 days relative to the prior 30-day window, blended with consistency over 13 weeks.

- **Recent window:** commits in the last 30 days (`recent_commits`)
- **Previous window:** commits in days 31–60 (`prev_commits`)
- **Consistency window:** 90 days, divided into 13 weekly buckets (`consistent_weeks` = number of weeks with at least one commit)
- **Acceleration cap:** 2.0 (a repo cannot score above 1.0 on the acceleration component regardless of growth rate)

```
score = clamp(recent_commits / max(prev_commits, 1), 0, acceleration_cap) / acceleration_cap * 0.6
      + consistent_weeks / 13 * 0.4
```

Repos younger than 14 days receive a default velocity score of **0.5** (insufficient history to compute a meaningful delta).

### Adoption

Measures community interest via stars, forks, and watchers. Each signal is log-normalized against the corpus maximum so that one outlier repo does not compress all others.

```
normalized(x) = log(x + 1) / log(corpus_max + 1)
```

Component weights:

| Signal   | Weight |
|----------|--------|
| Stars    | 0.5    |
| Forks    | 0.3    |
| Watchers | 0.2    |

```
score = 0.5 * normalized(stars) + 0.3 * normalized(forks) + 0.2 * normalized(watchers)
```

For monorepo skills, stars are dampened before normalization to prevent a single large collection from dominating:

```
dampened_stars = log(stars) / log(skill_count + 1)
```

### Freshness

Combines three sub-signals: exponential decay from the last commit date, a sigmoid over recent activity, and a maturity bonus.

**Decay** (half-life 30 days):

```
decay = exp(-0.693 * days_since_commit / 30)
```

**Activity sigmoid** over commits in the last 90 days, with midpoint at 5 commits and saturation at 20:

```
activity = sigmoid(commits_90d, mid=5, sat=20)
```

**Maturity bonus:** +0.05 added for repos older than 30 days (capped so total does not exceed 1.0).

Component weights:

| Sub-signal | Weight |
|------------|--------|
| Decay      | 0.5    |
| Activity   | 0.4    |
| Maturity   | 0.1    |

```
score = 0.5 * decay + 0.4 * activity + 0.1 * maturity_bonus
```

### Documentation

Checks the structural completeness of SKILL.md and associated files. Each check contributes a fixed weight:

| Check            | Weight | Notes                                          |
|------------------|--------|------------------------------------------------|
| has_frontmatter  | 0.100  | YAML frontmatter block present                 |
| has_name         | 0.050  | `name` field present in frontmatter            |
| has_description  | 0.150  | `description` field present and ≥ 20 chars     |
| line_count       | 0.200  | Sigmoid: 0 below 50 lines, 0.5 at 100, 1.0 at 300 |
| has_examples     | 0.150  | Examples section detected                      |
| has_usage        | 0.100  | Usage section detected                         |
| has_readme       | 0.100  | README.md present in repo                      |
| has_scripts      | 0.075  | Script or command definitions present          |
| has_references   | 0.075  | References or links section present            |

```
score = sum(weight[check] * passes(check) for check in checks)
```

The `line_count` check uses a sigmoid with 0 at fewer than 50 lines, 0.5 at 100 lines, and 1.0 at 300 or more lines.

### Contributors

Measures the breadth of contribution using a log scale capped at 10 contributors:

```
score = log(contributors + 1) / log(11)
```

Representative values:

| Contributors | Score |
|--------------|-------|
| 1            | 0.29  |
| 2            | 0.46  |
| 5            | 0.75  |
| 10           | 1.00  |

### Code Quality

Five binary signals, each worth 0.2:

| Signal        | Condition                         |
|---------------|-----------------------------------|
| has_license   | LICENSE file present              |
| has_workflows | `.github/workflows/` present      |
| has_tests     | Test directory or file detected   |
| has_gitignore | `.gitignore` present              |
| has_topics    | At least one GitHub repo topic    |

```
score = matched_signals / 5
```
