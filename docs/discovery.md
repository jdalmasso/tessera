# Discovery

> **Stub** — full content added in Phase 5.

This document describes how Tessera discovers Claude Code Skills on GitHub.

## Two-Phase Strategy

### Phase 1 — Repository Search (sorted by stars)

Targets repos with explicit skill-related topics. Runs first to prioritize popular repos.

### Phase 2 — Code Search (relevance-sorted)

Backfills repos that lack topics by searching for `SKILL.md` files directly.

Discovery stops at 2,000 repos regardless of which phase is active.

## Filtering Rules

A repo is included only if **all** of the following are true:
- SKILL.md contains valid YAML frontmatter
- Frontmatter has at least one of `name` or `description`
- SKILL.md is ≥ 100 characters
- Repo is not archived
- Repo is not a fork

## Monorepo Detection and Dampening

Repos with multiple SKILL.md files are monorepos. Each skill path becomes a separate entity. Repo-level adoption signals (stars, forks, watchers) are dampened: `log(stars) / log(skill_count + 1)`.

Monorepos with ≥ 10 skills are flagged as **collections**.

See `config/discovery.yaml` for query definitions and caps.

_Full narrative documentation to be completed in Phase 5._
