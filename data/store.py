"""
SQLite CRUD operations for Tessera's generic data layer.

All timestamps are ISO-8601 strings in UTC (e.g. "2026-04-12T10:00:00Z").
All metadata/payload/stats fields are JSON-encoded dicts.
"""

import json
import sqlite3
from typing import Any, Optional

from data.models import SCHEMA, RUN_STATUS_COMPLETED, RUN_STATUS_RUNNING


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def get_connection(db_path: str = ":memory:") -> sqlite3.Connection:
    """
    Open a SQLite connection with WAL mode and row_factory set to
    sqlite3.Row so callers can access columns by name.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    """Create all tables and indexes if they don't already exist."""
    conn.executescript(SCHEMA)
    conn.commit()


# ---------------------------------------------------------------------------
# Signal sources
# ---------------------------------------------------------------------------

def upsert_signal_source(
    conn: sqlite3.Connection,
    source_id: str,
    name: str,
    last_run_at: Optional[str] = None,
) -> None:
    """Insert or update a signal source record."""
    conn.execute(
        """
        INSERT INTO signal_sources (id, name, last_run_at)
        VALUES (?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            name        = excluded.name,
            last_run_at = excluded.last_run_at
        """,
        (source_id, name, last_run_at),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Entities
# ---------------------------------------------------------------------------

def upsert_entity(
    conn: sqlite3.Connection,
    entity_id: str,
    entity_type: str,
    name: str,
    description: Optional[str],
    metadata: dict[str, Any],
    category: str,
    now: str,
) -> None:
    """
    Insert a new entity or update an existing one.
    `first_seen_at` is preserved on update; `updated_at` is always refreshed.
    `metadata` is a dict — stored as JSON.
    """
    conn.execute(
        """
        INSERT INTO entities
            (id, entity_type, name, description, metadata, category, first_seen_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            name        = excluded.name,
            description = excluded.description,
            metadata    = excluded.metadata,
            category    = excluded.category,
            updated_at  = excluded.updated_at
        """,
        (entity_id, entity_type, name, description, json.dumps(metadata), category, now, now),
    )
    conn.commit()


def get_entity(conn: sqlite3.Connection, entity_id: str) -> Optional[sqlite3.Row]:
    """Return a single entity row by ID, or None if not found."""
    return conn.execute(
        "SELECT * FROM entities WHERE id = ?", (entity_id,)
    ).fetchone()


# ---------------------------------------------------------------------------
# Raw signals
# ---------------------------------------------------------------------------

def store_raw_signal(
    conn: sqlite3.Connection,
    source_id: str,
    signal_type: str,
    entity_ref: str,
    payload: dict[str, Any],
    collected_at: str,
    run_id: str,
) -> int:
    """
    Insert a raw signal record. `payload` is a dict — stored as JSON.
    Returns the new row's id.
    """
    cur = conn.execute(
        """
        INSERT INTO raw_signals
            (source_id, signal_type, entity_ref, payload, collected_at, run_id)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (source_id, signal_type, entity_ref, json.dumps(payload), collected_at, run_id),
    )
    conn.commit()
    return cur.lastrowid


def get_raw_signals(
    conn: sqlite3.Connection,
    entity_ref: str,
    signal_type: Optional[str] = None,
) -> list[sqlite3.Row]:
    """Return all raw signals for an entity, optionally filtered by type."""
    if signal_type:
        return conn.execute(
            "SELECT * FROM raw_signals WHERE entity_ref = ? AND signal_type = ? ORDER BY collected_at",
            (entity_ref, signal_type),
        ).fetchall()
    return conn.execute(
        "SELECT * FROM raw_signals WHERE entity_ref = ? ORDER BY collected_at",
        (entity_ref,),
    ).fetchall()


# ---------------------------------------------------------------------------
# Scores
# ---------------------------------------------------------------------------

def store_score(
    conn: sqlite3.Connection,
    entity_id: str,
    dimension: str,
    value: float,
    scored_at: str,
    run_id: str,
    details: Optional[dict[str, Any]] = None,
) -> int:
    """
    Insert a score record. `details` is an optional dict — stored as JSON.
    Returns the new row's id.
    """
    cur = conn.execute(
        """
        INSERT INTO scores (entity_id, dimension, value, details, scored_at, run_id)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            entity_id,
            dimension,
            value,
            json.dumps(details) if details is not None else None,
            scored_at,
            run_id,
        ),
    )
    conn.commit()
    return cur.lastrowid


def get_latest_scores(
    conn: sqlite3.Connection,
    dimension: str,
    run_id: str,
) -> list[sqlite3.Row]:
    """
    Return all scores for a given dimension and run, ordered by value descending.
    Each row has: entity_id, value, details, scored_at, run_id.
    """
    return conn.execute(
        """
        SELECT s.entity_id, s.value, s.details, s.scored_at, s.run_id,
               e.name, e.description, e.metadata, e.category
        FROM scores s
        JOIN entities e ON e.id = s.entity_id
        WHERE s.dimension = ? AND s.run_id = ?
        ORDER BY s.value DESC
        """,
        (dimension, run_id),
    ).fetchall()


def get_score_history(
    conn: sqlite3.Connection,
    entity_id: str,
    dimension: str,
) -> list[sqlite3.Row]:
    """
    Return all historical scores for an entity+dimension, oldest first.
    Each row has: value, details, scored_at, run_id.
    """
    return conn.execute(
        """
        SELECT value, details, scored_at, run_id
        FROM scores
        WHERE entity_id = ? AND dimension = ?
        ORDER BY scored_at ASC
        """,
        (entity_id, dimension),
    ).fetchall()


# ---------------------------------------------------------------------------
# Pipeline runs
# ---------------------------------------------------------------------------

def start_pipeline_run(
    conn: sqlite3.Connection,
    run_id: str,
    surface_id: str,
    started_at: str,
) -> None:
    """Record the start of a pipeline run."""
    conn.execute(
        """
        INSERT INTO pipeline_runs (id, surface_id, started_at, status)
        VALUES (?, ?, ?, ?)
        """,
        (run_id, surface_id, started_at, RUN_STATUS_RUNNING),
    )
    conn.commit()


def complete_pipeline_run(
    conn: sqlite3.Connection,
    run_id: str,
    completed_at: str,
    status: str = RUN_STATUS_COMPLETED,
    stats: Optional[dict[str, Any]] = None,
) -> None:
    """Mark a pipeline run as completed (or failed), recording stats."""
    conn.execute(
        """
        UPDATE pipeline_runs
        SET completed_at = ?, status = ?, stats = ?
        WHERE id = ?
        """,
        (completed_at, status, json.dumps(stats) if stats is not None else None, run_id),
    )
    conn.commit()


def get_latest_completed_run(
    conn: sqlite3.Connection,
    surface_id: str,
) -> Optional[sqlite3.Row]:
    """Return the most recently completed pipeline run for a surface, or None."""
    return conn.execute(
        """
        SELECT * FROM pipeline_runs
        WHERE surface_id = ? AND status = ?
        ORDER BY completed_at DESC
        LIMIT 1
        """,
        (surface_id, RUN_STATUS_COMPLETED),
    ).fetchone()


def get_previous_completed_run(
    conn: sqlite3.Connection,
    surface_id: str,
    before_run_id: str,
) -> Optional[sqlite3.Row]:
    """
    Return the completed run immediately before `before_run_id`, or None.
    Used to compute rank deltas in the site builder.
    """
    current = conn.execute(
        "SELECT completed_at FROM pipeline_runs WHERE id = ?", (before_run_id,)
    ).fetchone()
    if current is None:
        return None

    return conn.execute(
        """
        SELECT * FROM pipeline_runs
        WHERE surface_id = ? AND status = ? AND completed_at < ?
        ORDER BY completed_at DESC
        LIMIT 1
        """,
        (surface_id, RUN_STATUS_COMPLETED, current["completed_at"]),
    ).fetchone()
