# Discovery

This document describes how Tessera discovers Claude Code Skills on GitHub.

## Two-Phase Strategy

Discovery runs two phases sequentially, deduplicating continuously by repo `full_name`. It stops at 2,000 repos regardless of which phase is active.

### Phase 1 — Repository Search (sorted by stars)

Searches GitHub for repos with skill-related topics, sorted by stars. This captures popular repos early and front-loads high-quality results.

Queries:
- `topic:claude-skill`
- `topic:claude-code-skill`
- `topic:agent-skill`

### Phase 2 — Code Search (relevance-sorted)

Backfills repos that lack topics by searching for `SKILL.md` files directly.

Queries:
- `filename:SKILL.md path:.claude/skills`
- `filename:SKILL.md "claude" "skill"`
- `filename:SKILL.md`

## Filtering Rules

A discovered repo is included only if **all** of the following are true:

- SKILL.md contains valid YAML frontmatter
- Frontmatter has at least one of `name` or `description`
- SKILL.md is ≥ 100 bytes (measured in raw bytes, not characters)
- Repo is not archived
- Repo is not a fork (all forks excluded in v0.1)

## Monorepo Detection and Dampening

Repos with multiple SKILL.md files at different paths are treated as monorepos. Each skill path becomes a separate scored entity with its own ID (`skill:owner/repo:subdir/path`).

Repo-level adoption signals (stars, forks, watchers) are dampened to avoid a single large collection dominating:

```
dampened_stars = log(stars) / log(skill_count + 1)
```

Skill-level signals (SKILL.md content, commits to that path) remain per-skill and are not dampened.

Monorepos with ≥ 10 skills are flagged as **collections** and appear in the Top Collections section of the leaderboard.

## Rate Limiting

The pipeline uses exponential backoff with up to 3 retries per request and a 30-second timeout per request. GitHub's REST API allows 5,000 requests per hour for authenticated requests. The pipeline logs progress every 100 repos. A full run over 2,000 repos typically uses approximately 400–600 API calls.

## Configuration

See `config/discovery.yaml` for query definitions, the 2,000-repo cap, filtering flags, and API settings.
