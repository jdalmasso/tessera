# Leaderboard Display

> **Stub** — full content added in Phase 5.

This document describes how the static leaderboard is structured and how entries are formatted.

## Page Sections

1. Header — title, subtitle, last updated timestamp (ET)
2. How Scoring Works — methodology summary + link to `scoring-methodology.md`
3. Table of Contents
4. **Main Leaderboard — Top 10** (Trending, display caps applied)
5. **Per-Category Sections — Top 10 each** (all 15 categories, uncapped)
6. **Top Collections** (monorepos with ≥ 10 skills)
7. Footer

## Display Caps (Main Leaderboard Only)

- Max **3 skills per repo**
- Max **5 skills per author**
- When a cap is hit, the highest composite score within the repo/author determines which skills appear.
- Per-category and collections sections are **uncapped**.

## Entry Format

```
#1 ▲3  skill-creator                           Score 94
anthropics/skills · Consulting & Strategy        github.com/anthropics/skills
"Create, evaluate, improve Claude skills"
★ 2.1k · Updated 2d ago
Vel: 23/25 | Adopt: 18/20 | Fresh: 19/20 | Doc: 14/15 | Contrib: 9/10 | Code: 9/10
```

## Collections Format

```
#1  alirezarezvani/claude-skills             Top-3 Avg: 88
233 skills · Best: security-auditor (91), senior-architect (88), content-creator (85)
github.com/alirezarezvani/claude-skills
```

Collections are ranked by the average Trending composite of their 3 highest-scoring skills.

## Rank Delta Indicators

- `▲N` — moved up N positions since last run
- `▼N` — moved down N positions since last run
- `NEW` — not present in the previous run
- `—` — no change

_Full narrative documentation to be completed in Phase 5._
