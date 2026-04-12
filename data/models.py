"""
SQLite schema definitions for Tessera's generic data layer.

Design principles:
- Signals before entities: raw signals stored separately from derived scores
- Temporal: every signal and score is timestamped
- Generic: table patterns reusable for future signal sources and entity types
- No pruning in v0.1: all raw signals and scores retained
"""

SCHEMA = """
CREATE TABLE IF NOT EXISTS signal_sources (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    last_run_at TEXT
);

CREATE TABLE IF NOT EXISTS raw_signals (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id       TEXT NOT NULL REFERENCES signal_sources(id),
    signal_type     TEXT NOT NULL,
    entity_ref      TEXT NOT NULL,
    payload         TEXT NOT NULL,
    collected_at    TEXT NOT NULL,
    run_id          TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_signals_entity
    ON raw_signals(entity_ref);

CREATE INDEX IF NOT EXISTS idx_raw_signals_type
    ON raw_signals(signal_type, collected_at);

CREATE TABLE IF NOT EXISTS entities (
    id              TEXT PRIMARY KEY,
    entity_type     TEXT NOT NULL,
    name            TEXT NOT NULL,
    description     TEXT,
    metadata        TEXT NOT NULL,
    category        TEXT NOT NULL,
    first_seen_at   TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_entities_type
    ON entities(entity_type);

CREATE INDEX IF NOT EXISTS idx_entities_category
    ON entities(category);

CREATE TABLE IF NOT EXISTS scores (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_id       TEXT NOT NULL REFERENCES entities(id),
    dimension       TEXT NOT NULL,
    value           REAL NOT NULL,
    details         TEXT,
    scored_at       TEXT NOT NULL,
    run_id          TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_scores_entity
    ON scores(entity_id, dimension);

CREATE INDEX IF NOT EXISTS idx_scores_latest
    ON scores(entity_id, dimension, scored_at DESC);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id              TEXT PRIMARY KEY,
    surface_id      TEXT NOT NULL,
    started_at      TEXT NOT NULL,
    completed_at    TEXT,
    status          TEXT NOT NULL,
    stats           TEXT
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_surface
    ON pipeline_runs(surface_id, status, completed_at);
"""

# Valid values for scores.dimension
DIMENSIONS = [
    "velocity",
    "adoption",
    "freshness",
    "documentation",
    "contributors",
    "code_quality",
    "composite:trending",
    "composite:popular",
    "composite:well_rounded",
]

# Valid values for pipeline_runs.status
RUN_STATUS_RUNNING = "running"
RUN_STATUS_COMPLETED = "completed"
RUN_STATUS_FAILED = "failed"
