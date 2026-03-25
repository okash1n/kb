"""SQLite schema management for event pipeline."""

from __future__ import annotations

import sqlite3
import json
from contextlib import contextmanager
from pathlib import Path

from kb_mcp.config import runtime_events_db_path, runtime_events_dir
from kb_mcp.events.learning_contract import default_backfilled_asset_fields

SCHEMA_VERSION = 7

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

_LEARNING_ASSETS_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS learning_assets (
      asset_key TEXT PRIMARY KEY,
      candidate_key TEXT REFERENCES promotion_candidates(candidate_key),
      review_id TEXT REFERENCES candidate_reviews(review_id),
      materialization_key TEXT REFERENCES materialization_records(materialization_key),
      note_id TEXT,
      note_path TEXT,
      memory_class TEXT NOT NULL CHECK (memory_class IN ('adr', 'gap', 'knowledge', 'session_thin')),
      update_target TEXT NOT NULL,
      scope TEXT NOT NULL CHECK (scope IN ('session_local', 'client_local', 'project_local', 'user_global', 'general')),
      force TEXT NOT NULL CHECK (force IN ('hint', 'preferred', 'default', 'guardrail')),
      confidence TEXT NOT NULL CHECK (confidence IN ('observed', 'candidate', 'reviewed', 'stable', 'stale')),
      lifecycle TEXT NOT NULL CHECK (lifecycle IN ('observed', 'candidate', 'active', 'superseded', 'retracted', 'expired')),
      provenance_json TEXT NOT NULL,
      traceability_json TEXT NOT NULL,
      revocation_path_json TEXT NOT NULL,
      learning_state_visibility TEXT NOT NULL CHECK (
        learning_state_visibility IN ('candidate', 'active', 'held', 'retractable', 'superseded', 'retracted', 'expired')
      ),
      source_status TEXT NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )
"""

_LEARNING_PACKETS_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS learning_packets (
      packet_id TEXT PRIMARY KEY,
      source_tool TEXT NOT NULL,
      source_client TEXT NOT NULL,
      tool_name TEXT NOT NULL,
      session_id TEXT,
      project TEXT,
      repo TEXT,
      cwd TEXT,
      asset_count INTEGER NOT NULL,
      status TEXT NOT NULL CHECK (status IN ('active', 'invalidated')),
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )
"""

_LEARNING_PACKET_ASSETS_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS learning_packet_assets (
      packet_id TEXT NOT NULL REFERENCES learning_packets(packet_id),
      asset_key TEXT NOT NULL REFERENCES learning_assets(asset_key),
      packet_order INTEGER NOT NULL,
      PRIMARY KEY (packet_id, asset_key)
    )
"""

_LEARNING_APPLICATIONS_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS learning_applications (
      application_id TEXT PRIMARY KEY,
      packet_id TEXT NOT NULL REFERENCES learning_packets(packet_id),
      tool_name TEXT NOT NULL,
      tool_call_id TEXT NOT NULL,
      source_tool TEXT NOT NULL,
      source_client TEXT NOT NULL,
      session_id TEXT,
      save_request_id TEXT,
      saved_note_id TEXT,
      saved_note_path TEXT,
      created_at TEXT NOT NULL
    )
"""

_LEARNING_REVOCATIONS_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS learning_revocations (
      revocation_id TEXT PRIMARY KEY,
      asset_key TEXT NOT NULL REFERENCES learning_assets(asset_key),
      action TEXT NOT NULL CHECK (action IN ('retract', 'supersede', 'expire')),
      actor TEXT NOT NULL,
      reason TEXT NOT NULL,
      replacement_asset_key TEXT,
      invalidated_packet_count INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL
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
    conn.execute(_LEARNING_ASSETS_TABLE_SQL)
    conn.execute(_LEARNING_PACKETS_TABLE_SQL)
    conn.execute(_LEARNING_PACKET_ASSETS_TABLE_SQL)
    conn.execute(_LEARNING_APPLICATIONS_TABLE_SQL)
    conn.execute(_LEARNING_REVOCATIONS_TABLE_SQL)
    _ensure_learning_packet_columns(conn)
    _backfill_learning_assets(conn)
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
    conn.execute("PRAGMA foreign_keys=ON")


def _backfill_learning_assets(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        """
        WITH latest_reviews AS (
          SELECT candidate_key, MAX(review_seq) AS max_review_seq
          FROM candidate_reviews
          GROUP BY candidate_key
        ),
        latest_materializations AS (
          SELECT candidate_key, MAX(review_seq) AS max_review_seq
          FROM materialization_records
          GROUP BY candidate_key
            )
        SELECT
          pc.candidate_key,
          pc.label AS candidate_label,
          pc.status AS candidate_status,
          pc.created_at AS candidate_created_at,
          pc.updated_at AS candidate_updated_at,
          cr.review_id,
          cr.review_seq,
          cr.human_verdict,
          cr.human_label,
          cr.reviewed_at,
          mr.materialization_key,
          mr.status AS materialization_status,
          mr.note_id,
          mr.note_path,
          mr.created_at AS materialization_created_at,
          mr.updated_at AS materialization_updated_at
        FROM promotion_candidates pc
        JOIN latest_reviews lr
          ON lr.candidate_key = pc.candidate_key
        JOIN candidate_reviews cr
          ON cr.candidate_key = lr.candidate_key
         AND cr.review_seq = lr.max_review_seq
        LEFT JOIN latest_materializations lm
          ON lm.candidate_key = pc.candidate_key
        LEFT JOIN materialization_records mr
          ON mr.candidate_key = lm.candidate_key
         AND mr.review_seq = lm.max_review_seq
        WHERE pc.status IN ('accepted', 'relabeled', 'materialized')
        """
    ).fetchall()
    for row in rows:
        memory_class = str(row["human_label"] or row["candidate_label"])
        fields = default_backfilled_asset_fields(
            memory_class=memory_class,
            source_status=str(row["candidate_status"]),
        )
        scope = str(fields["scope"])
        asset_key = _learning_asset_key(
            candidate_key=str(row["candidate_key"]),
            review_seq=int(row["review_seq"]),
            memory_class=memory_class,
            scope=scope,
        )
        source_status = str(row["candidate_status"])
        created_at = (
            row["materialization_created_at"]
            or row["reviewed_at"]
            or row["candidate_created_at"]
        )
        updated_at = (
            row["materialization_updated_at"]
            or row["reviewed_at"]
            or row["candidate_updated_at"]
        )
        conn.execute(
            """
            INSERT INTO learning_assets(
              asset_key, candidate_key, review_id, materialization_key, note_id, note_path,
              memory_class, update_target, scope, force, confidence, lifecycle,
              provenance_json, traceability_json, revocation_path_json,
              learning_state_visibility, source_status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(asset_key) DO UPDATE SET
              review_id=excluded.review_id,
              materialization_key=COALESCE(excluded.materialization_key, learning_assets.materialization_key),
              note_id=COALESCE(excluded.note_id, learning_assets.note_id),
              note_path=COALESCE(excluded.note_path, learning_assets.note_path),
              confidence=excluded.confidence,
              lifecycle=excluded.lifecycle,
              provenance_json=excluded.provenance_json,
              traceability_json=excluded.traceability_json,
              learning_state_visibility=excluded.learning_state_visibility,
              source_status=excluded.source_status,
              updated_at=excluded.updated_at
            """,
            (
                asset_key,
                row["candidate_key"],
                row["review_id"],
                row["materialization_key"],
                row["note_id"],
                row["note_path"],
                str(fields["memory_class"]),
                str(fields["update_target"]),
                scope,
                str(fields["force"]),
                str(fields["confidence"]),
                str(fields["lifecycle"]),
                _json_dumps(
                    {
                        "candidate_key": row["candidate_key"],
                        "review_id": row["review_id"],
                        "review_seq": row["review_seq"],
                        "materialization_key": row["materialization_key"],
                    }
                ),
                _json_dumps(
                    {
                        "note_id": row["note_id"],
                        "note_path": row["note_path"],
                        "materialization_status": row["materialization_status"],
                    }
                ),
                _json_dumps(
                    {
                        "supersede_key": asset_key,
                        "rollback_scope": scope,
                    }
                ),
                str(fields["learning_state_visibility"]),
                source_status,
                created_at,
                updated_at,
            ),
        )


def _ensure_learning_packet_columns(conn: sqlite3.Connection) -> None:
    rows = conn.execute("PRAGMA table_info(learning_packets)").fetchall()
    columns = {str(row["name"]) for row in rows}
    if "expires_at" not in columns:
        conn.execute("ALTER TABLE learning_packets ADD COLUMN expires_at TEXT")
    if "invalidated_at" not in columns:
        conn.execute("ALTER TABLE learning_packets ADD COLUMN invalidated_at TEXT")
    if "invalidation_reason" not in columns:
        conn.execute("ALTER TABLE learning_packets ADD COLUMN invalidation_reason TEXT")


def _learning_asset_key(*, candidate_key: str, review_seq: int, memory_class: str, scope: str) -> str:
    return f"learning:{candidate_key}:{review_seq}:{memory_class}:{scope}"


def _json_dumps(value: dict[str, object]) -> str:
    return json.dumps(value, ensure_ascii=False)
