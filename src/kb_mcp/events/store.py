"""SQLite-backed event store and aggregate merge logic."""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from typing import Any

from kb_mcp.events.candidates import detect_candidates
from kb_mcp.events.schema import schema_locked_connection
from kb_mcp.events.types import DispatchResult, EventEnvelope, utc_now_iso

_CANDIDATE_LABELS = {"adr", "gap", "knowledge", "session_thin"}
_CANDIDATE_STATUSES = {"pending_review", "accepted", "rejected", "materialized"}
_JUDGE_STATUSES = {"ready", "judged", "superseded", "failed"}
_HUMAN_VERDICTS = {"accepted", "rejected", "relabeled"}


class EventStore:
    """Durable event store for hook and middleware events."""

    def append(self, envelope: EventEnvelope) -> DispatchResult:
        """Persist an event and merge it into an aggregate."""
        with self.transaction() as conn:
            _assign_checkpoint_identity(conn, envelope)
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
            remaining = conn.execute(
                """
                SELECT COUNT(*) AS count
                FROM outbox
                WHERE logical_key=? AND aggregate_version=? AND status!='applied'
                """,
                (logical_key, aggregate_version),
            ).fetchone()
            if int(remaining["count"]) == 0:
                conn.execute(
                    """
                    UPDATE logical_events
                    SET status='applied', updated_at=?
                    WHERE logical_key=? AND aggregate_version=?
                    """,
                    (utc_now_iso(), logical_key, aggregate_version),
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

    def dead_letter_count(self) -> int:
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS count FROM outbox WHERE status='dead_letter'"
            ).fetchone()
            return int(row["count"])

    def replay_dead_letters(self, *, limit: int = 50) -> int:
        with self.transaction() as conn:
            rows = conn.execute(
                """
                SELECT id
                FROM outbox
                WHERE status='dead_letter'
                ORDER BY id
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            if not rows:
                return 0
            now = utc_now_iso()
            conn.executemany(
                """
                UPDATE outbox
                SET status='ready', claimed_at=NULL, last_error=NULL, due_at=?
                WHERE id=?
                """,
                [(now, row["id"]) for row in rows],
            )
            return len(rows)

    def upsert_judge_run(
        self,
        *,
        judge_run_key: str,
        partition_key: str,
        window_id: str,
        start_ordinal: int,
        end_ordinal: int,
        window_index: int,
        status: str,
        prompt_version: str,
        labels: list[dict[str, Any]] | None = None,
        decision: dict[str, Any] | None = None,
        model_hint: str | None = None,
        supersedes_judge_run_key: str | None = None,
    ) -> None:
        if status not in _JUDGE_STATUSES:
            raise ValueError(f"invalid judge status: {status}")
        now = utc_now_iso()
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO judge_runs(
                  judge_run_key, partition_key, window_id, start_ordinal, end_ordinal,
                  window_index, status, labels_json, decision_json, prompt_version,
                  model_hint, supersedes_judge_run_key, lease_owner, lease_expires_at,
                  created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?)
                ON CONFLICT(window_id, prompt_version) DO UPDATE SET
                  partition_key=excluded.partition_key,
                  start_ordinal=excluded.start_ordinal,
                  end_ordinal=excluded.end_ordinal,
                  window_index=excluded.window_index,
                  status=excluded.status,
                  labels_json=excluded.labels_json,
                  decision_json=excluded.decision_json,
                  model_hint=excluded.model_hint,
                  supersedes_judge_run_key=excluded.supersedes_judge_run_key,
                  updated_at=excluded.updated_at
                """,
                (
                    judge_run_key,
                    partition_key,
                    window_id,
                    start_ordinal,
                    end_ordinal,
                    window_index,
                    status,
                    json.dumps(labels or [], ensure_ascii=False),
                    json.dumps(decision or {}, ensure_ascii=False),
                    prompt_version,
                    model_hint,
                    supersedes_judge_run_key,
                    now,
                    now,
                ),
            )

    def claim_judge_run(
        self,
        *,
        window_id: str,
        prompt_version: str,
        lease_owner: str,
        lease_expires_at: str,
    ) -> sqlite3.Row | None:
        with self.transaction() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM judge_runs
                WHERE window_id=? AND prompt_version=?
                  AND status='ready'
                  AND (lease_expires_at IS NULL OR lease_expires_at <= ?)
                LIMIT 1
                """,
                (window_id, prompt_version, utc_now_iso()),
            ).fetchone()
            if row is None:
                return None
            conn.execute(
                """
                UPDATE judge_runs
                SET lease_owner=?, lease_expires_at=?, updated_at=?
                WHERE judge_run_key=?
                """,
                (lease_owner, lease_expires_at, utc_now_iso(), row["judge_run_key"]),
            )
            return conn.execute(
                "SELECT * FROM judge_runs WHERE judge_run_key=?",
                (row["judge_run_key"],),
            ).fetchone()

    def heartbeat_judge_run(
        self,
        *,
        judge_run_key: str,
        lease_owner: str,
        lease_expires_at: str,
    ) -> bool:
        with self.transaction() as conn:
            result = conn.execute(
                """
                UPDATE judge_runs
                SET lease_expires_at=?, updated_at=?
                WHERE judge_run_key=? AND lease_owner=?
                """,
                (lease_expires_at, utc_now_iso(), judge_run_key, lease_owner),
            )
            return result.rowcount > 0

    def release_judge_run(self, *, judge_run_key: str, lease_owner: str) -> bool:
        with self.transaction() as conn:
            result = conn.execute(
                """
                UPDATE judge_runs
                SET lease_owner=NULL, lease_expires_at=NULL, updated_at=?
                WHERE judge_run_key=? AND lease_owner=?
                """,
                (utc_now_iso(), judge_run_key, lease_owner),
            )
            return result.rowcount > 0

    def upsert_promotion_candidate(
        self,
        *,
        candidate_key: str,
        window_id: str,
        judge_run_key: str,
        label: str,
        status: str,
        score: float | None,
        slice_fingerprint: str | None,
        reasons: list[str],
        payload: dict[str, Any],
    ) -> None:
        if label not in _CANDIDATE_LABELS:
            raise ValueError(f"invalid candidate label: {label}")
        if status not in _CANDIDATE_STATUSES:
            raise ValueError(f"invalid candidate status: {status}")
        now = utc_now_iso()
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO promotion_candidates(
                  candidate_key, window_id, judge_run_key, label, status, score,
                  slice_fingerprint, reasons_json, payload_json, last_suggested_at,
                  suggestion_seq, resolved_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 0, NULL, ?, ?)
                ON CONFLICT(candidate_key) DO UPDATE SET
                  judge_run_key=CASE
                    WHEN promotion_candidates.status='pending_review' THEN excluded.judge_run_key
                    ELSE promotion_candidates.judge_run_key
                  END,
                  status=CASE
                    WHEN promotion_candidates.status='pending_review' THEN excluded.status
                    ELSE promotion_candidates.status
                  END,
                  score=CASE
                    WHEN promotion_candidates.status='pending_review' THEN excluded.score
                    ELSE promotion_candidates.score
                  END,
                  slice_fingerprint=CASE
                    WHEN promotion_candidates.status='pending_review' THEN excluded.slice_fingerprint
                    ELSE promotion_candidates.slice_fingerprint
                  END,
                  reasons_json=CASE
                    WHEN promotion_candidates.status='pending_review' THEN excluded.reasons_json
                    ELSE promotion_candidates.reasons_json
                  END,
                  payload_json=CASE
                    WHEN promotion_candidates.status='pending_review' THEN excluded.payload_json
                    ELSE promotion_candidates.payload_json
                  END,
                  updated_at=excluded.updated_at
                """,
                (
                    candidate_key,
                    window_id,
                    judge_run_key,
                    label,
                    status,
                    score,
                    slice_fingerprint,
                    json.dumps(reasons, ensure_ascii=False),
                    json.dumps(payload, ensure_ascii=False),
                    now,
                    now,
                ),
            )

    def mark_candidates_suggested(self, candidate_keys: list[str]) -> int:
        if not candidate_keys:
            return 0
        now = utc_now_iso()
        with self.transaction() as conn:
            rows = conn.execute(
                f"""
                SELECT candidate_key, suggestion_seq
                FROM promotion_candidates
                WHERE candidate_key IN ({",".join("?" for _ in candidate_keys)})
                """,
                candidate_keys,
            ).fetchall()
            conn.executemany(
                """
                UPDATE promotion_candidates
                SET last_suggested_at=?, suggestion_seq=?, updated_at=?
                WHERE candidate_key=?
                """,
                [
                    (now, int(row["suggestion_seq"]) + 1, now, row["candidate_key"])
                    for row in rows
                ],
            )
            return len(rows)

    def record_candidate_review(
        self,
        *,
        review_id: str,
        candidate_key: str,
        window_id: str,
        judge_run_key: str,
        ai_labels: list[dict[str, Any]],
        ai_score: dict[str, Any],
        human_verdict: str,
        human_label: str | None,
        review_comment: str | None = None,
        reviewed_by: str | None = None,
    ) -> int:
        if human_verdict not in _HUMAN_VERDICTS:
            raise ValueError(f"invalid human verdict: {human_verdict}")
        if human_verdict == "relabeled" and not human_label:
            raise ValueError("human_label is required for relabeled verdict")
        if human_label is not None and human_label not in _CANDIDATE_LABELS:
            raise ValueError(f"invalid human label: {human_label}")
        reviewed_at = utc_now_iso()
        with self.transaction() as conn:
            candidate = conn.execute(
                """
                SELECT candidate_key, window_id, judge_run_key
                FROM promotion_candidates
                WHERE candidate_key=?
                """,
                (candidate_key,),
            ).fetchone()
            if candidate is None:
                raise ValueError(f"candidate not found: {candidate_key}")
            if candidate["window_id"] != window_id:
                raise ValueError("window_id does not match candidate")
            if candidate["judge_run_key"] != judge_run_key:
                raise ValueError("judge_run_key does not match candidate")
            row = conn.execute(
                "SELECT COALESCE(MAX(review_seq), 0) AS max_seq FROM candidate_reviews WHERE candidate_key=?",
                (candidate_key,),
            ).fetchone()
            review_seq = int(row["max_seq"]) + 1
            conn.execute(
                """
                INSERT INTO candidate_reviews(
                  review_id, candidate_key, review_seq, window_id, judge_run_key,
                  ai_labels_json, ai_score_json, human_verdict, human_label,
                  review_comment, reviewed_by, reviewed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    review_id,
                    candidate_key,
                    review_seq,
                    window_id,
                    judge_run_key,
                    json.dumps(ai_labels, ensure_ascii=False),
                    json.dumps(ai_score, ensure_ascii=False),
                    human_verdict,
                    human_label,
                    review_comment,
                    reviewed_by,
                    reviewed_at,
                ),
            )
            candidate_status = "accepted" if human_verdict == "accepted" else "rejected"
            conn.execute(
                """
                UPDATE promotion_candidates
                SET status=?, resolved_at=?, updated_at=?
                WHERE candidate_key=?
                """,
                (candidate_status, reviewed_at, reviewed_at, candidate_key),
            )
            return review_seq

    def pending_review_candidates(self, *, limit: int = 50) -> list[sqlite3.Row]:
        with self.transaction() as conn:
            return conn.execute(
                """
                SELECT *
                FROM promotion_candidates
                WHERE status='pending_review'
                ORDER BY created_at, candidate_key
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

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
            if envelope.source_layer == "session_launcher" and envelope.correlation_id:
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
            elif _is_anchor_save(envelope):
                queued = ["promotion_planner", "promotion_applier"]
        else:
            status = "collecting"
    elif envelope.aggregate_type == "error":
        status = "ready"
        queued = ["incident_writer"]
    elif envelope.aggregate_type == "compact":
        if envelope.event_name in {"compact_finished", "turn_checkpointed"}:
            status = "ready"
            queued = ["checkpoint_writer"]
            detected = detect_candidates(envelope.summary, envelope.content_excerpt)
            if detected["has_candidates"]:
                queued.append("candidate_writer")
            if _needs_thin_promotion(envelope):
                queued.extend(["promotion_planner", "promotion_applier"])
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


def _is_anchor_save(envelope: EventEnvelope) -> bool:
    if envelope.event_name != "tool_succeeded":
        return False
    note_type = envelope.raw_payload.get("saved_note_type")
    return note_type in {"gap", "knowledge", "adr"}


def _needs_thin_promotion(envelope: EventEnvelope) -> bool:
    if envelope.event_name not in {"compact_finished", "turn_checkpointed"}:
        return False
    state = envelope.aggregate_state
    return bool(state.get("final_hint")) or state.get("checkpoint_kind") == "session_end"


def _assign_checkpoint_identity(conn: sqlite3.Connection, envelope: EventEnvelope) -> None:
    if envelope.aggregate_type != "compact":
        return
    partition_key = envelope.aggregate_state.get("checkpoint_partition_key")
    if not partition_key:
        return
    ordinal = envelope.aggregate_state.get("checkpoint_ordinal")
    if ordinal:
        row = conn.execute(
            "SELECT next_ordinal FROM checkpoint_sequences WHERE partition_key=?",
            (partition_key,),
        ).fetchone()
        next_ordinal = max(int(ordinal) + 1, int(row["next_ordinal"]) if row else 1)
        conn.execute(
            """
            INSERT INTO checkpoint_sequences(partition_key, next_ordinal)
            VALUES (?, ?)
            ON CONFLICT(partition_key) DO UPDATE SET next_ordinal=excluded.next_ordinal
            """,
            (partition_key, next_ordinal),
        )
        return
    row = conn.execute(
        "SELECT next_ordinal FROM checkpoint_sequences WHERE partition_key=?",
        (partition_key,),
    ).fetchone()
    next_ordinal = int(row["next_ordinal"]) if row else 1
    conn.execute(
        """
        INSERT INTO checkpoint_sequences(partition_key, next_ordinal)
        VALUES (?, ?)
        ON CONFLICT(partition_key) DO UPDATE SET next_ordinal=excluded.next_ordinal
        """,
        (partition_key, next_ordinal + 1),
    )
    envelope.aggregate_state["checkpoint_ordinal"] = next_ordinal
    envelope.logical_key = f"compact:{partition_key}:{next_ordinal}"


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
