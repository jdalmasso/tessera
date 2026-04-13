# Leaderboard Display

This document describes how the static leaderboard is structured, how entries are formatted, and how display logic works.

## Page Sections

1. **Header** — title, subtitle, last updated timestamp in ET
2. **How Scoring Works** — brief summary of the three methodologies with a link to `scoring-methodology.md`
3. **Table of Contents** — anchor links to all sections below
4. **Main Leaderboard — Top 10** — ranked by Trending composite, display caps applied
5. **Per-Category Sections — Top 10 each** — all 15 categories shown, uncapped
6. **Top Collections** — monorepos with ≥ 10 skills, ranked by top-3 average Trending composite
7. **Stats** — score distributions and category breakdown table
8. **Footer** — link to the Tessera GitHub repo

## Entry Format

```
#1 ▲3  skill-creator                           Score 94
anthropics/skills · Consulting & Strategy        github.com/anthropics/skills
"Create, evaluate, improve Claude skills"
★ 2.1k · Updated 2d ago
Vel: 23/25 | Adopt: 18/20 | Fresh: 19/20 | Doc: 14/15 | Contrib: 9/10 | Code: 9/10
```

Fields: rank, delta indicator, skill name, author, category, repo link, description, stars, last updated, and a numeric per-dimension breakdown. Denominators reflect the weight of each dimension under the Trending methodology.

## Collections Format

```
#1  alirezarezvani/claude-skills             Top-3 Avg: 88
233 skills · Best: security-auditor (91), senior-architect (88), content-creator (85)
github.com/alirezarezvani/claude-skills
```

Collections are ranked by the average Trending composite of their 3 highest-scoring skills. This rewards collections with standout skills regardless of their long tail.

## Display Caps (Main Leaderboard Only)

- Max **3 skills per repo**
- Max **5 skills per author**
- When a cap is reached, the highest composite score within the repo/author determines which skills appear.
- Per-category sections and the Top Collections section are **uncapped**.

## Rank Delta Indicators

| Indicator | Meaning |
|-----------|---------|
| `▲N` | Moved up N positions since the previous run |
| `▼N` | Moved down N positions since the previous run |
| `NEW` | Not present in the previous run |
| `—` | No change in rank |

If a skill drops off the leaderboard and returns later, it shows `NEW`.

## Build Process

`build.py` generates `build/index.html` by:

1. Querying SQLite for the latest Trending scores
2. Finding the previous completed run to compute rank deltas
3. Applying display caps and selecting the top 10 for the main leaderboard
4. Querying the top 10 per category (uncapped, all 15 categories always shown)
5. Querying collections (≥ 10 skills), computing top-3 average per collection
6. Rendering the Jinja2 template and writing `build/index.html`

See `config/site.yaml` for display cap values, top-N settings, and timezone configuration.
