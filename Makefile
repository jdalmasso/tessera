.PHONY: test pipeline build seed-run lint

PYTHON := .venv/bin/python
PYTEST := .venv/bin/pytest
RUFF   := .venv/bin/ruff

test:
	$(PYTEST) tests/

pipeline:
	$(PYTHON) -m surfaces.skills_leaderboard.pipeline

build:
	$(PYTHON) -m surfaces.skills_leaderboard.build

seed-run:
	$(PYTHON) -m surfaces.skills_leaderboard.pipeline
	$(PYTHON) -m surfaces.skills_leaderboard.seed_report

lint:
	$(RUFF) check .
