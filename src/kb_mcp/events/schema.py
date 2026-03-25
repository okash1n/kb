"""SQLite schema management for event pipeline."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path

from kb_mcp.config import runtime_events_db_path, runtime_events_dir

SCHEMA_VERSION = 2

DDL = [
    """
    CREATE TABLE IF NOT EXISTS schema_meta (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS events (
      event_id TEXT PRIMARY KEY,
      occurred_at TEXT NOT NULL,
      received_at TEXT NOT NULL,
      source_tool TEXT NOT NULL,
      source_client TEXT NOT NULL,
      source_layer TEXT NOT NULL,
      event_name TEXT NOT NULL,
      aggregate_type TEXT NOT NULL,
      management_mode TEXT NOT NULL,
      logical_key TEXT NOT NULL,
      correlation_id TEXT,
      session_id TEXT,
      tool_call_id TEXT,
      error_fingerprint TEXT,
      summary TEXT,
      content_excerpt TEXT,
      cwd TEXT,
      repo TEXT,
      project TEXT,
      transcript_path TEXT,
      raw_payload_json TEXT NOT NULL,
      redacted_payload_json TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS logical_events (
      logical_key TEXT PRIMARY KEY,
      aggregate_type TEXT NOT NULL,
      correlation_id TEXT,
      session_id TEXT,
      management_mode TEXT NOT NULL,
      source_tool TEXT NOT NULL,
      source_client TEXT NOT NULL,
      status TEXT NOT NULL,
      aggregate_version INTEGER NOT NULL,
      summary TEXT,
      content_excerpt TEXT,
      cwd TEXT,
      repo TEXT,
      project TEXT,
      transcript_path TEXT,
      final_outcome TEXT,
      debug_only_reason TEXT,
      aggregate_state_json TEXT NOT NULL,
      ready_at TEXT,
      updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS outbox (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      logical_key TEXT NOT NULL,
      aggregate_version INTEGER NOT NULL,
      sink_name TEXT NOT NULL,
      status TEXT NOT NULL,
      due_at TEXT NOT NULL,
      claimed_at TEXT,
      last_error TEXT,
      created_at TEXT NOT NULL,
      UNIQUE(logical_key, aggregate_version, sink_name)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS sink_runs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      logical_key TEXT NOT NULL,
      aggregate_version INTEGER NOT NULL,
      sink_name TEXT NOT NULL,
      receipt TEXT NOT NULL,
      status TEXT NOT NULL,
      created_at TEXT NOT NULL,
      UNIQUE(receipt)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS runtime_observations (
      observation_key TEXT PRIMARY KEY,
      severity TEXT NOT NULL,
      message TEXT NOT NULL,
      details_json TEXT NOT NULL,
      expires_at TEXT,
      updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS checkpoint_sequences (
      partition_key TEXT PRIMARY KEY,
      next_ordinal INTEGER NOT NULL
    )
    """,
]


def connect() -> sqlite3.Connection:
    """Return a SQLite connection with runtime defaults."""
    runtime_events_dir().mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(runtime_events_db_path())
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def ensure_schema(conn: sqlite3.Connection) -> None:
    """Create or upgrade schema."""
    for ddl in DDL:
        conn.execute(ddl)
    conn.execute(
        "INSERT INTO schema_meta(key, value) VALUES('schema_version', ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (str(SCHEMA_VERSION),),
    )
    conn.commit()


@contextmanager
def schema_locked_connection() -> sqlite3.Connection:
    """Open a schema-ready connection."""
    conn = connect()
    try:
        ensure_schema(conn)
        yield conn
    finally:
        conn.close()


def db_path() -> Path:
    """Return database path for diagnostics."""
    return runtime_events_db_path()
