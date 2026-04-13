# Tessera — Skills Leaderboard

Tessera is an open-source VC intelligence platform. The Skills Leaderboard is its first module: it discovers, scores, and ranks [Claude Code Skills](https://docs.anthropic.com/claude/docs/skills) — GitHub repositories containing a `SKILL.md` file — then publishes a static leaderboard to GitHub Pages.

The pipeline runs daily, pulls signals from the GitHub API, stores them in SQLite, and generates a static site from Jinja2 templates.

Live site: [https://github.com/tessera-vc/tessera](https://github.com/tessera-vc/tessera)

---

## Architecture

The codebase is split into generic, reusable layers and an opinionated consumer surface. The rule is strict: `signals/` and `data/` must not import from `surfaces/`.

```
signals/github/          Generic GitHub API wrapper, discovery logic, scoring functions.
                         No knowledge of Skills specifically.

surfaces/skills_leaderboard/
                         Opinionated consumer. Runs the pipeline, builds the static site,
                         and produces calibration reports. Uses signals/github/.

data/                    Generic SQLite storage layer (WAL mode).
                         Follows a signals-before-entities pattern: raw signals are stored
                         before derived scores are computed.

utils/                   Generic markdown and YAML parsers.

tests/                   All tests (279 passing, 1 skipped).

surfaces/skills_leaderboard/config/
                         scoring.yaml, categories.yaml, discovery.yaml, site.yaml
```

**Entity IDs** follow the format `skill:owner/repo` for single-skill repos, or `skill:owner/repo:subdir/path` for monorepos that host multiple skills under subdirectories.

---

## Scoring

Each skill is scored across six dimensions and ranked under three methodologies. Scores are derived from raw signals and stored separately in an append-only, temporal fashion.

### Dimensions

| Dimension | What it measures |
|---|---|
| `velocity` | Rate of recent activity (stars, forks, commits) |
| `adoption` | Absolute usage and community uptake |
| `freshness` | How recently the skill has been updated |
| `documentation` | Quality and completeness of the SKILL.md and supporting docs |
| `contributors` | Breadth of contributor base |
| `code_quality` | Repository health signals |

### Methodologies

**Trending** — surfaces skills that are gaining momentum right now.

| Dimension | Weight |
|---|---|
| velocity | 25 |
| adoption | 20 |
| freshness | 20 |
| documentation | 15 |
| contributors | 10 |
| code_quality | 10 |

**Popular** — shows what the community has already validated over time.

| Dimension | Weight |
|---|---|
| adoption | 30 |
| freshness | 20 |
| contributors | 15 |
| documentation | 15 |
| velocity | 10 |
| code_quality | 10 |

**Well-Rounded** — rewards skills that have no significant weak spots.

| Dimension | Weight |
|---|---|
| documentation | 25 |
| code_quality | 25 |
| adoption | 15 |
| freshness | 15 |
| velocity | 10 |
| contributors | 10 |

Weights and category definitions are in `surfaces/skills_leaderboard/config/`. See `docs/` for a full explanation of how each dimension is computed.

---

## Setup

**Requirements:** Python 3.12, a GitHub personal access token with `read:public_repo` scope.

```bash
git clone https://github.com/tessera-vc/tessera.git
cd tessera

python -m venv .venv
source .venv/bin/activate

pip install -e ".[dev]"

cp .env.example .env
# Edit .env and set GH_PAT to your GitHub token
```

The `.env` file is git-ignored. Never commit it or any `.db` files.

---

## Commands

| Command | Description |
|---|---|
| `make pipeline` | Run the discovery and scoring pipeline |
| `make build` | Generate the static site from current database state |
| `make seed-run` | Full pipeline run followed by a calibration report |
| `make test` | Run all tests |
| `make lint` | Run ruff linter |

A typical first run:

```bash
make pipeline
make build
```

The generated site is written to `site/` and is ready to serve or deploy.

---

## CI and GitHub Actions

The pipeline runs automatically every day at 6am ET (cron `0 10 * * *` UTC). The workflow requires one repository secret:

- `GH_PAT` — a GitHub personal access token with `read:public_repo` scope.

---

## Contributing

1. Fork the repository and create a branch from `main`.
2. Make your changes. Keep `signals/` and `data/` free of any imports from `surfaces/`.
3. Run `make test` and ensure all tests pass before opening a pull request.
4. Open a PR with a clear description of what changed and why.

For significant changes — new scoring dimensions, methodology adjustments, or architectural decisions — open an issue first to discuss the approach.

---

## Documentation

Detailed methodology, dimension definitions, and design decisions are in the `docs/` directory.
