"""SQLite schema management for event pipeline."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path

from kb_mcp.config import runtime_events_db_path, runtime_events_dir

SCHEMA_VERSION = 4

_PROMOTION_CANDIDATES_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS promotion_candidates (
      candidate_key TEXT PRIMARY KEY,
      window_id TEXT NOT NULL,
      judge_run_key TEXT NOT NULL REFERENCES judge_runs(judge_run_key),
      label TEXT NOT NULL CHECK (label IN ('adr', 'gap', 'knowledge', 'session_thin')),
      status TEXT NOT NULL CHECK (status IN ('pending_review', 'accepted', 'rejected', 'relabeled', 'materialized')),
      score REAL,
      slice_fingerprint TEXT,
      reasons_json TEXT NOT NULL,
      payload_json TEXT NOT NULL,
      last_suggested_at TEXT,
      suggestion_seq INTEGER NOT NULL DEFAULT 0,
      resolved_at TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )
"""

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
    """
    CREATE TABLE IF NOT EXISTS judge_runs (
      judge_run_key TEXT PRIMARY KEY,
      partition_key TEXT NOT NULL,
      window_id TEXT NOT NULL,
      start_ordinal INTEGER NOT NULL,
      end_ordinal INTEGER NOT NULL,
      window_index INTEGER NOT NULL,
      status TEXT NOT NULL CHECK (status IN ('ready', 'judged', 'superseded', 'failed')),
      labels_json TEXT NOT NULL,
      decision_json TEXT NOT NULL,
      prompt_version TEXT NOT NULL,
      model_hint TEXT,
      supersedes_judge_run_key TEXT,
      lease_owner TEXT,
      lease_expires_at TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      UNIQUE(window_id, prompt_version)
    )
    """,
    _PROMOTION_CANDIDATES_TABLE_SQL,
    """
    CREATE TABLE IF NOT EXISTS candidate_reviews (
      review_id TEXT PRIMARY KEY,
      candidate_key TEXT NOT NULL REFERENCES promotion_candidates(candidate_key),
      review_seq INTEGER NOT NULL,
      window_id TEXT NOT NULL,
      judge_run_key TEXT NOT NULL REFERENCES judge_runs(judge_run_key),
      ai_labels_json TEXT NOT NULL,
      ai_score_json TEXT NOT NULL,
      human_verdict TEXT NOT NULL CHECK (human_verdict IN ('accepted', 'rejected', 'relabeled')),
      human_label TEXT CHECK (human_label IS NULL OR human_label IN ('adr', 'gap', 'knowledge', 'session_thin')),
      review_comment TEXT,
      reviewed_by TEXT,
      reviewed_at TEXT NOT NULL,
      UNIQUE(candidate_key, review_seq)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS materialization_records (
      materialization_key TEXT PRIMARY KEY,
      candidate_key TEXT NOT NULL REFERENCES promotion_candidates(candidate_key),
      review_seq INTEGER NOT NULL,
      judge_run_key TEXT NOT NULL REFERENCES judge_runs(judge_run_key),
      window_id TEXT NOT NULL,
      materialized_label TEXT NOT NULL CHECK (materialized_label IN ('adr', 'gap', 'knowledge', 'session_thin')),
      effective_label TEXT NOT NULL CHECK (effective_label IN ('adr', 'gap', 'knowledge', 'session_thin')),
      status TEXT NOT NULL CHECK (status IN ('planned', 'applying', 'applied', 'repair_pending', 'failed', 'superseded')),
      note_id TEXT,
      note_path TEXT,
      promotion_key TEXT,
      supersedes_materialization_key TEXT,
      payload_json TEXT NOT NULL,
      last_error TEXT,
      lease_owner TEXT,
      lease_expires_at TEXT,
      lease_epoch INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      UNIQUE(candidate_key, review_seq, effective_label)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS note_mutations (
      mutation_id TEXT PRIMARY KEY,
      note_id TEXT NOT NULL,
      note_path TEXT NOT NULL,
      mutation_kind TEXT NOT NULL CHECK (mutation_kind IN ('frontmatter_merge', 'body_replace', 'body_append')),
      request_key TEXT NOT NULL,
      before_sha256 TEXT NOT NULL,
      after_sha256 TEXT NOT NULL,
      payload_json TEXT NOT NULL,
      created_at TEXT NOT NULL,
      UNIQUE(note_id, request_key)
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
    _ensure_relabeled_candidate_status(conn)
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


def _ensure_relabeled_candidate_status(conn: sqlite3.Connection) -> None:
    row = conn.execute(
        """
        SELECT sql
        FROM sqlite_master
        WHERE type='table' AND name='promotion_candidates'
        """
    ).fetchone()
    table_sql = str(row["sql"] or "") if row is not None else ""
    if "'relabeled'" in table_sql:
        return
    conn.execute("PRAGMA foreign_keys=OFF")
    try:
        conn.execute(_PROMOTION_CANDIDATES_TABLE_SQL.replace("IF NOT EXISTS ", ""))
        conn.execute(
            """
            INSERT OR IGNORE INTO promotion_candidates(
              candidate_key, window_id, judge_run_key, label, status, score, slice_fingerprint,
              reasons_json, payload_json, last_suggested_at, suggestion_seq, resolved_at, created_at, updated_at
            )
            SELECT
              candidate_key, window_id, judge_run_key, label, status, score, slice_fingerprint,
              reasons_json, payload_json, last_suggested_at, suggestion_seq, resolved_at, created_at, updated_at
            FROM promotion_candidates_old
            """
        )
    except sqlite3.OperationalError:
        conn.execute("ALTER TABLE promotion_candidates RENAME TO promotion_candidates_old")
        conn.execute(_PROMOTION_CANDIDATES_TABLE_SQL)
        conn.execute(
            """
            INSERT INTO promotion_candidates(
              candidate_key, window_id, judge_run_key, label, status, score, slice_fingerprint,
              reasons_json, payload_json, last_suggested_at, suggestion_seq, resolved_at, created_at, updated_at
            )
            SELECT
              candidate_key, window_id, judge_run_key, label, status, score, slice_fingerprint,
              reasons_json, payload_json, last_suggested_at, suggestion_seq, resolved_at, created_at, updated_at
            FROM promotion_candidates_old
            """
        )
        conn.execute("DROP TABLE promotion_candidates_old")
    finally:
        conn.execute("PRAGMA foreign_keys=ON")
