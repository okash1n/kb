"""SQLite-backed event store and aggregate merge logic."""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from typing import Any

from kb_mcp.events.schema import schema_locked_connection
from kb_mcp.events.types import DispatchResult, EventEnvelope, utc_now_iso


class EventStore:
    """Durable event store for hook and middleware events."""

    def append(self, envelope: EventEnvelope) -> DispatchResult:
        """Persist an event and merge it into an aggregate."""
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO events(
                  event_id, occurred_at, received_at, source_tool, source_client,
                  source_layer, event_name, aggregate_type, management_mode, logical_key,
                  correlation_id, session_id, tool_call_id, error_fingerprint, summary,
                  content_excerpt, cwd, repo, project, transcript_path, raw_payload_json,
                  redacted_payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    envelope.event_id,
                    envelope.occurred_at,
                    envelope.received_at,
                    envelope.source_tool,
                    envelope.source_client,
                    envelope.source_layer,
                    envelope.event_name,
                    envelope.aggregate_type,
                    envelope.management_mode,
                    envelope.logical_key,
                    envelope.correlation_id,
                    envelope.session_id,
                    envelope.tool_call_id,
                    envelope.error_fingerprint,
                    envelope.summary,
                    envelope.content_excerpt,
                    envelope.cwd,
                    envelope.repo,
                    envelope.project,
                    envelope.transcript_path,
                    json.dumps(envelope.raw_payload, ensure_ascii=False),
                    json.dumps(envelope.redacted_payload, ensure_ascii=False),
                ),
            )
            version, status, queued = _merge_envelope(conn, envelope)
            return DispatchResult(
                event_id=envelope.event_id,
                logical_key=envelope.logical_key,
                aggregate_type=envelope.aggregate_type,
                status=status,
                aggregate_version=version,
                queued_sinks=queued,
            )

    def ready_sinks(self, *, maintenance: bool = False, limit: int = 50) -> list[sqlite3.Row]:
        """Claim due outbox rows."""
        with self.transaction() as conn:
            if maintenance:
                _promote_pending_sessions(conn)
            now = utc_now_iso()
            rows = conn.execute(
                """
                SELECT * FROM outbox
                WHERE status = 'ready' AND due_at <= ?
                ORDER BY id
                LIMIT ?
                """,
                (now, limit),
            ).fetchall()
            ids = [row["id"] for row in rows]
            if ids:
                conn.executemany(
                    "UPDATE outbox SET status='claimed', claimed_at=? WHERE id=?",
                    [(now, row_id) for row_id in ids],
                )
            return rows

    def mark_sink_succeeded(self, row_id: int, logical_key: str, aggregate_version: int, sink_name: str, receipt: str) -> None:
        with self.transaction() as conn:
            conn.execute(
                "UPDATE outbox SET status='applied', last_error=NULL WHERE id=?",
                (row_id,),
            )
            conn.execute(
                """
                INSERT OR IGNORE INTO sink_runs(logical_key, aggregate_version, sink_name, receipt, status, created_at)
                VALUES (?, ?, ?, ?, 'applied', ?)
                """,
                (logical_key, aggregate_version, sink_name, receipt, utc_now_iso()),
            )
            conn.execute(
                "UPDATE logical_events SET status='applied', updated_at=? WHERE logical_key=?",
                (utc_now_iso(), logical_key),
            )

    def mark_sink_failed(self, row_id: int, message: str) -> None:
        with self.transaction() as conn:
            conn.execute(
                "UPDATE outbox SET status='dead_letter', last_error=? WHERE id=?",
                (message[:500], row_id),
            )

    def fetch_logical_event(self, logical_key: str) -> sqlite3.Row | None:
        with self.transaction() as conn:
            return conn.execute(
                "SELECT * FROM logical_events WHERE logical_key=?",
                (logical_key,),
            ).fetchone()

    def put_runtime_observation(self, key: str, severity: str, message: str, details: dict[str, Any] | None = None) -> None:
        payload = json.dumps(details or {}, ensure_ascii=False)
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO runtime_observations(observation_key, severity, message, details_json, expires_at, updated_at)
                VALUES (?, ?, ?, ?, NULL, ?)
                ON CONFLICT(observation_key) DO UPDATE SET
                  severity=excluded.severity,
                  message=excluded.message,
                  details_json=excluded.details_json,
                  updated_at=excluded.updated_at
                """,
                (key, severity, message, payload, utc_now_iso()),
            )

    @contextmanager
    def transaction(self) -> sqlite3.Connection:
        with schema_locked_connection() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise


def _merge_envelope(conn: sqlite3.Connection, envelope: EventEnvelope) -> tuple[int, str, list[str]]:
    existing = conn.execute(
        "SELECT * FROM logical_events WHERE logical_key=?",
        (envelope.logical_key,),
    ).fetchone()
    state = dict(envelope.aggregate_state)
    if existing:
        current_state = json.loads(existing["aggregate_state_json"])
        current_state.update({k: v for k, v in state.items() if v is not None})
        state = current_state
        version = int(existing["aggregate_version"]) + 1
        status = existing["status"]
        debug_only_reason = existing["debug_only_reason"]
        summary = envelope.summary or existing["summary"]
        content_excerpt = envelope.content_excerpt or existing["content_excerpt"]
        cwd = envelope.cwd or existing["cwd"]
        repo = envelope.repo or existing["repo"]
        project = envelope.project or existing["project"]
        transcript_path = envelope.transcript_path or existing["transcript_path"]
        final_outcome = existing["final_outcome"]
    else:
        version = 1
        status = "collecting"
        debug_only_reason = None
        summary = envelope.summary
        content_excerpt = envelope.content_excerpt
        cwd = envelope.cwd
        repo = envelope.repo
        project = envelope.project
        transcript_path = envelope.transcript_path
        final_outcome = None

    queued: list[str] = []

    if envelope.aggregate_type == "session":
        if envelope.event_name == "session_ended":
            if envelope.correlation_id:
                status = "pending_finalization"
            else:
                status = "collecting"
                debug_only_reason = "missing_correlation_id"
        elif envelope.event_name == "process_exit":
            final_outcome = state.get("exit_code")
        else:
            status = "collecting"
    elif envelope.aggregate_type == "tool":
        if envelope.event_name in {"tool_succeeded", "tool_failed"}:
            status = "ready"
            if envelope.event_name == "tool_failed":
                queued = ["incident_writer"]
        else:
            status = "collecting"
    elif envelope.aggregate_type == "error":
        status = "ready"
        queued = ["incident_writer"]
    elif envelope.aggregate_type == "compact":
        if envelope.event_name == "compact_finished":
            status = "ready"
            queued = ["checkpoint_writer"]
        else:
            status = "collecting"

    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO logical_events(
          logical_key, aggregate_type, correlation_id, session_id, management_mode,
          source_tool, source_client, status, aggregate_version, summary, content_excerpt,
          cwd, repo, project, transcript_path, final_outcome, debug_only_reason,
          aggregate_state_json, ready_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(logical_key) DO UPDATE SET
          status=excluded.status,
          aggregate_version=excluded.aggregate_version,
          summary=excluded.summary,
          content_excerpt=excluded.content_excerpt,
          cwd=excluded.cwd,
          repo=excluded.repo,
          project=excluded.project,
          transcript_path=excluded.transcript_path,
          final_outcome=excluded.final_outcome,
          debug_only_reason=excluded.debug_only_reason,
          aggregate_state_json=excluded.aggregate_state_json,
          ready_at=excluded.ready_at,
          updated_at=excluded.updated_at
        """,
        (
            envelope.logical_key,
            envelope.aggregate_type,
            envelope.correlation_id,
            envelope.session_id,
            envelope.management_mode,
            envelope.source_tool,
            envelope.source_client,
            status,
            version,
            summary,
            content_excerpt,
            cwd,
            repo,
            project,
            transcript_path,
            str(final_outcome) if final_outcome is not None else None,
            debug_only_reason,
            json.dumps(state, ensure_ascii=False),
            now if status == "ready" else None,
            now,
        ),
    )

    for sink_name in queued:
        conn.execute(
            """
            INSERT OR IGNORE INTO outbox(logical_key, aggregate_version, sink_name, status, due_at, claimed_at, last_error, created_at)
            VALUES (?, ?, ?, 'ready', ?, NULL, NULL, ?)
            """,
            (envelope.logical_key, version, sink_name, now, now),
        )
    return version, status, queued


def _promote_pending_sessions(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        """
        SELECT logical_key, aggregate_version
        FROM logical_events
        WHERE aggregate_type='session' AND status='pending_finalization' AND debug_only_reason IS NULL
        """
    ).fetchall()
    now = utc_now_iso()
    for row in rows:
        conn.execute(
            "UPDATE logical_events SET status='ready', ready_at=?, updated_at=? WHERE logical_key=?",
            (now, now, row["logical_key"]),
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO outbox(logical_key, aggregate_version, sink_name, status, due_at, claimed_at, last_error, created_at)
            VALUES (?, ?, 'session_finalizer', 'ready', ?, NULL, NULL, ?)
            """,
            (row["logical_key"], row["aggregate_version"], now, now),
        )
