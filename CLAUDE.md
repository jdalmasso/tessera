# Tessera — Skills Leaderboard

## What this is
First module of Tessera, an open-source VC intelligence platform.
Discovers, scores, and ranks Claude Code Skills (SKILL.md repos) on GitHub.

## Tech stack
- Python 3.12
- SQLite (WAL mode)
- Jinja2 for static site generation
- GitHub API (REST v3)
- GitHub Pages for hosting

## Commands
- `make test` — run all tests
- `make pipeline` — run discovery + scoring pipeline
- `make build` — generate static site
- `make seed-run` — full pipeline + calibration report
- `make lint` — ruff check

## Architecture
- `signals/github/` — generic GitHub API wrapper, discovery, scoring (knows nothing about Skills)
- `surfaces/skills_leaderboard/` — opinionated consumer that uses signals/github/
- `data/` — generic SQLite storage layer (signals-before-entities pattern)
- `utils/` — generic markdown/YAML parsers
- Config lives in `surfaces/skills_leaderboard/config/`

## Conventions
- All tests in `tests/`
- Never commit `.db` files or `.env`
- Entity IDs: `skill:owner/repo` or `skill:owner/repo:subdir/path`
- `signals/` and `data/` modules must not import from `surfaces/`
- Store raw signals separately from derived scores (temporal, append-only)

## Scoring
Three methodologies (Trending, Popular, Well-Rounded) using 6 dimensions:
velocity, adoption, freshness, documentation, contributors, code_quality.
Weights in `surfaces/skills_leaderboard/config/scoring.yaml`.

## Pipeline schedule
Daily at 6am ET via GitHub Actions. Cron: `0 10 * * *` (UTC).
