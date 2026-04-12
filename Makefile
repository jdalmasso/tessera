.PHONY: test pipeline build seed-run lint

test:
	pytest tests/

pipeline:
	python -m surfaces.skills_leaderboard.pipeline

build:
	python -m surfaces.skills_leaderboard.build

seed-run:
	python -m surfaces.skills_leaderboard.pipeline
	python -m surfaces.skills_leaderboard.seed_report

lint:
	ruff check .
