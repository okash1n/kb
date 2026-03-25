"""SQLite-backed event store and aggregate merge logic."""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any

from kb_mcp.events.candidates import detect_candidates
from kb_mcp.events.schema import schema_locked_connection
from kb_mcp.events.types import DispatchResult, EventEnvelope, utc_now_iso
from kb_mcp.note import generate_ulid

_CANDIDATE_LABELS = {"adr", "gap", "knowledge", "session_thin"}
_CANDIDATE_STATUSES = {"pending_review", "accepted", "rejected", "relabeled", "materialized"}
_JUDGE_STATUSES = {"ready", "judged", "superseded", "failed"}
_HUMAN_VERDICTS = {"accepted", "rejected", "relabeled"}
_MATERIALIZATION_STATUSES = {"planned", "applying", "applied", "repair_pending", "failed", "superseded"}
_REVIEW_MATERIALIZATION_SINKS = ("promotion_planner", "promotion_applier")


def _store_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


class EventStore:
    """Durable event store for hook and middleware events."""

    def append(self, envelope: EventEnvelope) -> DispatchResult:
        """Persist an event and merge it into an aggregate."""
        with self.transaction() as conn:
            version, status, queued = _append_envelope(conn, envelope)
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
            updated = conn.execute(
                """
                UPDATE outbox
                SET status='applied', last_error=NULL
                WHERE id=? AND logical_key=? AND aggregate_version=? AND sink_name=? AND status='claimed'
                """,
                (row_id, logical_key, aggregate_version, sink_name),
            )
            if updated.rowcount == 0:
                return
            conn.execute(
                """
                INSERT OR IGNORE INTO sink_runs(logical_key, aggregate_version, sink_name, receipt, status, created_at)
                VALUES (?, ?, ?, ?, 'applied', ?)
                """,
                (logical_key, aggregate_version, sink_name, receipt, utc_now_iso()),
            )
            logical = conn.execute(
                """
                SELECT aggregate_type
                FROM logical_events
                WHERE logical_key=? AND aggregate_version=?
                """,
                (logical_key, aggregate_version),
            ).fetchone()
            if logical is not None and str(logical["aggregate_type"]) == "review_materialization":
                completed = conn.execute(
                    """
                    SELECT COUNT(DISTINCT sink_name) AS count
                    FROM outbox
                    WHERE logical_key=? AND aggregate_version=? AND status='applied'
                      AND sink_name IN (?, ?)
                    """,
                    (
                        logical_key,
                        aggregate_version,
                        _REVIEW_MATERIALIZATION_SINKS[0],
                        _REVIEW_MATERIALIZATION_SINKS[1],
                    ),
                ).fetchone()
                can_finalize = int(completed["count"]) == len(_REVIEW_MATERIALIZATION_SINKS)
            else:
                remaining = conn.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM outbox
                    WHERE logical_key=? AND aggregate_version=? AND status!='applied'
                    """,
                    (logical_key, aggregate_version),
                ).fetchone()
                can_finalize = int(remaining["count"]) == 0
            if can_finalize:
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

    def checkpoint_partition_keys(self, *, limit: int | None = None) -> list[str]:
        with schema_locked_connection() as conn:
            sql = "SELECT partition_key FROM checkpoint_sequences ORDER BY partition_key"
            params: tuple[Any, ...] = ()
            if limit is not None:
                sql += " LIMIT ?"
                params = (limit,)
            rows = conn.execute(sql, params).fetchall()
            return [str(row["partition_key"]) for row in rows]

    def replay_dead_letters(self, *, limit: int = 50) -> int:
        with self.transaction() as conn:
            rows = conn.execute(
                """
                SELECT id
                FROM outbox
                WHERE status='dead_letter'
                  AND COALESCE(last_error, '') != 'superseded by newer review_materialization'
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

    def enqueue_materialization_resolution(
        self,
        *,
        candidate_key: str,
        review_seq: int,
        effective_label: str,
        materialization_key: str,
        judge_run_key: str,
        window_id: str,
        payload: dict[str, Any],
        promotion_key: str | None = None,
        project: str | None = None,
        cwd: str | None = None,
        repo: str | None = None,
    ) -> DispatchResult:
        if effective_label not in _CANDIDATE_LABELS:
            raise ValueError(f"invalid materialized label: {effective_label}")
        logical_key = f"materialize:{candidate_key}:{effective_label}"
        with self.transaction() as conn:
            existing_record = conn.execute(
                """
                SELECT review_seq, status
                FROM materialization_records
                WHERE materialization_key=?
                LIMIT 1
                """,
                (materialization_key,),
            ).fetchone()
            existing_logical = conn.execute(
                """
                SELECT aggregate_version, aggregate_state_json, status
                FROM logical_events
                WHERE logical_key=?
                LIMIT 1
                """,
                (logical_key,),
            ).fetchone()
            latest_resolution = conn.execute(
                """
                SELECT MAX(review_seq) AS max_review_seq
                FROM materialization_records
                WHERE candidate_key=? AND effective_label=?
                """,
                (candidate_key, effective_label),
            ).fetchone()
            latest_resolution_seq = int(latest_resolution["max_review_seq"] or 0)
            if latest_resolution_seq > review_seq:
                return DispatchResult(
                    event_id="",
                    logical_key=logical_key,
                    aggregate_type="review_materialization",
                    status=str(existing_logical["status"]) if existing_logical is not None else "ready",
                    aggregate_version=int(existing_logical["aggregate_version"]) if existing_logical is not None else 0,
                    queued_sinks=[],
                )
            logical_review_seq = 0
            if existing_logical is not None:
                logical_state = json.loads(existing_logical["aggregate_state_json"])
                logical_review_seq = int(logical_state.get("review_seq") or 0)
            if logical_review_seq >= review_seq:
                current_sink_count = 0
                if existing_logical is not None:
                    sink_row = conn.execute(
                        """
                        SELECT COUNT(DISTINCT sink_name) AS count
                        FROM outbox
                        WHERE logical_key=? AND aggregate_version=?
                          AND status!='dead_letter'
                          AND sink_name IN (?, ?)
                        """,
                        (
                            logical_key,
                            int(existing_logical["aggregate_version"]),
                            _REVIEW_MATERIALIZATION_SINKS[0],
                            _REVIEW_MATERIALIZATION_SINKS[1],
                        ),
                    ).fetchone()
                    current_sink_count = int(sink_row["count"])
                if (
                    existing_record is not None
                    and existing_record["status"] == "applied"
                ) or (
                    existing_record is not None
                    and existing_record["status"] in {"planned", "applying"}
                    and current_sink_count == len(_REVIEW_MATERIALIZATION_SINKS)
                ) or (
                    existing_record is None
                    and current_sink_count == len(_REVIEW_MATERIALIZATION_SINKS)
                ):
                    return DispatchResult(
                        event_id="",
                        logical_key=logical_key,
                        aggregate_type="review_materialization",
                        status=str(existing_logical["status"]) if existing_logical is not None else "ready",
                        aggregate_version=int(existing_logical["aggregate_version"]) if existing_logical is not None else 0,
                        queued_sinks=[],
                    )
            if existing_record is not None and int(existing_record["review_seq"]) >= review_seq:
                if existing_record["status"] == "applied":
                    return DispatchResult(
                        event_id="",
                        logical_key=logical_key,
                        aggregate_type="review_materialization",
                        status="applied",
                        aggregate_version=int(existing_logical["aggregate_version"]) if existing_logical is not None else 0,
                        queued_sinks=[],
                    )
            self._upsert_materialization_record_conn(
                conn,
                materialization_key=materialization_key,
                candidate_key=candidate_key,
                review_seq=review_seq,
                judge_run_key=judge_run_key,
                window_id=window_id,
                materialized_label=effective_label,
                effective_label=effective_label,
                status="planned",
                payload=payload,
                promotion_key=promotion_key,
            )
            envelope = EventEnvelope(
                event_id=generate_ulid(),
                occurred_at=utc_now_iso(),
                received_at=utc_now_iso(),
                source_tool="kb",
                source_client="kb-mcp",
                source_layer="recovery_sweeper",
                event_name="materialization_resolved",
                aggregate_type="review_materialization",
                management_mode="unmanaged",
                logical_key=logical_key,
                correlation_id=None,
                session_id=None,
                summary=f"materialize {effective_label}",
                content_excerpt=None,
                cwd=cwd,
                repo=repo,
                project=project,
                transcript_path=None,
                aggregate_state={
                    "candidate_key": candidate_key,
                    "review_seq": review_seq,
                    "judge_run_key": judge_run_key,
                    "window_id": window_id,
                    "effective_label": effective_label,
                    "materialization_key": materialization_key,
                    "promotion_key": promotion_key,
                },
                raw_payload=dict(payload),
                redacted_payload=dict(payload),
            )
            version, status, queued = _append_envelope(conn, envelope)
            return DispatchResult(
                event_id=envelope.event_id,
                logical_key=envelope.logical_key,
                aggregate_type=envelope.aggregate_type,
                status=status,
                aggregate_version=version,
                queued_sinks=queued,
            )

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
        now = _store_now_iso()
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

    def get_judge_run(self, *, window_id: str, prompt_version: str) -> sqlite3.Row | None:
        with schema_locked_connection() as conn:
            return conn.execute(
                """
                SELECT *
                FROM judge_runs
                WHERE window_id=? AND prompt_version=?
                LIMIT 1
                """,
                (window_id, prompt_version),
            ).fetchone()

    def get_judge_run_by_key(self, judge_run_key: str) -> sqlite3.Row | None:
        with schema_locked_connection() as conn:
            return conn.execute(
                """
                SELECT *
                FROM judge_runs
                WHERE judge_run_key=?
                LIMIT 1
                """,
                (judge_run_key,),
            ).fetchone()

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
                (lease_owner, lease_expires_at, _store_now_iso(), row["judge_run_key"]),
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
                (lease_expires_at, _store_now_iso(), judge_run_key, lease_owner),
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
                (_store_now_iso(), judge_run_key, lease_owner),
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
        now = _store_now_iso()
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

    def upsert_materialization_record(
        self,
        *,
        materialization_key: str,
        candidate_key: str,
        review_seq: int,
        judge_run_key: str,
        window_id: str,
        materialized_label: str,
        effective_label: str,
        status: str,
        payload: dict[str, Any],
        note_id: str | None = None,
        note_path: str | None = None,
        promotion_key: str | None = None,
        supersedes_materialization_key: str | None = None,
        last_error: str | None = None,
        lease_owner: str | None = None,
        lease_expires_at: str | None = None,
        lease_epoch: int | None = None,
    ) -> None:
        with self.transaction() as conn:
            self._upsert_materialization_record_conn(
                conn,
                materialization_key=materialization_key,
                candidate_key=candidate_key,
                review_seq=review_seq,
                judge_run_key=judge_run_key,
                window_id=window_id,
                materialized_label=materialized_label,
                effective_label=effective_label,
                status=status,
                payload=payload,
                note_id=note_id,
                note_path=note_path,
                promotion_key=promotion_key,
                supersedes_materialization_key=supersedes_materialization_key,
                last_error=last_error,
                lease_owner=lease_owner,
                lease_expires_at=lease_expires_at,
                lease_epoch=lease_epoch,
            )

    def get_materialization_record(self, materialization_key: str) -> sqlite3.Row | None:
        with schema_locked_connection() as conn:
            return conn.execute(
                """
                SELECT *
                FROM materialization_records
                WHERE materialization_key=?
                LIMIT 1
                """,
                (materialization_key,),
            ).fetchone()

    def claim_materialization_record(
        self,
        *,
        materialization_key: str,
        lease_owner: str,
        lease_expires_at: str,
    ) -> sqlite3.Row | None:
        with self.transaction() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM materialization_records
                WHERE materialization_key=?
                  AND status IN ('planned', 'repair_pending', 'applying')
                  AND (lease_expires_at IS NULL OR lease_expires_at <= ? OR lease_owner=?)
                LIMIT 1
                """,
                (materialization_key, utc_now_iso(), lease_owner),
            ).fetchone()
            if row is None:
                return None
            next_epoch = int(row["lease_epoch"]) + 1
            conn.execute(
                """
                UPDATE materialization_records
                SET status='applying', lease_owner=?, lease_expires_at=?, lease_epoch=?, updated_at=?
                WHERE materialization_key=?
                """,
                (lease_owner, lease_expires_at, next_epoch, _store_now_iso(), materialization_key),
            )
            return conn.execute(
                "SELECT * FROM materialization_records WHERE materialization_key=?",
                (materialization_key,),
            ).fetchone()

    def heartbeat_materialization_record(
        self,
        *,
        materialization_key: str,
        lease_owner: str,
        lease_epoch: int,
        lease_expires_at: str,
    ) -> bool:
        with self.transaction() as conn:
            result = conn.execute(
                """
                UPDATE materialization_records
                SET lease_expires_at=?, updated_at=?
                WHERE materialization_key=? AND lease_owner=? AND lease_epoch=?
                """,
                (lease_expires_at, _store_now_iso(), materialization_key, lease_owner, lease_epoch),
            )
            return result.rowcount > 0

    def release_materialization_record(
        self,
        *,
        materialization_key: str,
        lease_owner: str,
        lease_epoch: int,
        status: str,
    ) -> bool:
        if status not in _MATERIALIZATION_STATUSES:
            raise ValueError(f"invalid materialization status: {status}")
        with self.transaction() as conn:
            result = conn.execute(
                """
                UPDATE materialization_records
                SET status=?, lease_owner=NULL, lease_expires_at=NULL, updated_at=?
                WHERE materialization_key=? AND lease_owner=? AND lease_epoch=?
                """,
                (status, _store_now_iso(), materialization_key, lease_owner, lease_epoch),
            )
            return result.rowcount > 0

    def mark_materialization_repair_pending(
        self,
        *,
        materialization_key: str,
        expected_lease_epoch: int,
        last_error: str | None = None,
    ) -> bool:
        with self.transaction() as conn:
            row = conn.execute(
                """
                SELECT status, lease_epoch
                FROM materialization_records
                WHERE materialization_key=?
                LIMIT 1
                """,
                (materialization_key,),
            ).fetchone()
            if row is None or str(row["status"]) == "applied":
                return False
            if int(row["lease_epoch"]) > expected_lease_epoch:
                return False
            result = conn.execute(
                """
                UPDATE materialization_records
                SET status='repair_pending',
                    lease_owner=NULL,
                    lease_expires_at=NULL,
                    last_error=?,
                    updated_at=?
                WHERE materialization_key=?
                  AND status!='applied'
                  AND lease_epoch<=?
                """,
                ((last_error or "")[:500] or None, _store_now_iso(), materialization_key, expected_lease_epoch),
            )
            return result.rowcount > 0

    def finalize_materialization_record(
        self,
        *,
        materialization_key: str,
        lease_owner: str,
        lease_epoch: int,
        candidate_key: str,
        note_id: str | None,
        note_path: str | None,
        promotion_key: str | None,
    ) -> bool:
        with self.transaction() as conn:
            result = conn.execute(
                """
                UPDATE materialization_records
                SET status='applied',
                    note_id=COALESCE(?, note_id),
                    note_path=COALESCE(?, note_path),
                    promotion_key=COALESCE(?, promotion_key),
                    lease_owner=NULL,
                    lease_expires_at=NULL,
                    updated_at=?
                WHERE materialization_key=? AND lease_owner=? AND lease_epoch=?
                """,
                (
                    note_id,
                    note_path,
                    promotion_key,
                    _store_now_iso(),
                    materialization_key,
                    lease_owner,
                    lease_epoch,
                ),
            )
            if result.rowcount == 0:
                return False
            now = utc_now_iso()
            conn.execute(
                """
                UPDATE promotion_candidates
                SET status='materialized', resolved_at=COALESCE(resolved_at, ?), updated_at=?
                WHERE candidate_key=?
                """,
                (now, now, candidate_key),
            )
            return True

    def reserve_materialization_note_target(
        self,
        *,
        materialization_key: str,
        lease_owner: str,
        lease_epoch: int,
        note_id: str,
        note_path: str,
    ) -> sqlite3.Row | None:
        with self.transaction() as conn:
            result = conn.execute(
                """
                UPDATE materialization_records
                SET note_id=CASE
                      WHEN note_id IS NULL OR note_path IS NULL THEN ?
                      ELSE note_id
                    END,
                    note_path=CASE
                      WHEN note_id IS NULL OR note_path IS NULL THEN ?
                      ELSE note_path
                    END,
                    updated_at=?
                WHERE materialization_key=? AND lease_owner=? AND lease_epoch=?
                """,
                (
                    note_id,
                    note_path,
                    _store_now_iso(),
                    materialization_key,
                    lease_owner,
                    lease_epoch,
                ),
            )
            if result.rowcount == 0:
                return None
            return conn.execute(
                "SELECT * FROM materialization_records WHERE materialization_key=?",
                (materialization_key,),
            ).fetchone()

    def record_materialization_note_result(
        self,
        *,
        materialization_key: str,
        lease_owner: str,
        lease_epoch: int,
        note_id: str,
        note_path: str,
        promotion_key: str | None,
    ) -> bool:
        with self.transaction() as conn:
            result = conn.execute(
                """
                UPDATE materialization_records
                SET note_id=?,
                    note_path=?,
                    promotion_key=COALESCE(?, promotion_key),
                    updated_at=?
                WHERE materialization_key=? AND lease_owner=? AND lease_epoch=?
                """,
                (
                    note_id,
                    note_path,
                    promotion_key,
                    _store_now_iso(),
                    materialization_key,
                    lease_owner,
                    lease_epoch,
                ),
            )
            return result.rowcount > 0

    def mark_candidate_materialized(self, candidate_key: str, *, resolved_at: str | None = None) -> None:
        with self.transaction() as conn:
            now = resolved_at or utc_now_iso()
            conn.execute(
                """
                UPDATE promotion_candidates
                SET status='materialized', resolved_at=COALESCE(resolved_at, ?), updated_at=?
                WHERE candidate_key=?
                """,
                (now, now, candidate_key),
            )

    def get_promotion_candidate(self, candidate_key: str) -> sqlite3.Row | None:
        with self.transaction() as conn:
            return conn.execute(
                "SELECT * FROM promotion_candidates WHERE candidate_key=?",
                (candidate_key,),
            ).fetchone()

    def latest_candidate_review(self, candidate_key: str) -> sqlite3.Row | None:
        with self.transaction() as conn:
            return conn.execute(
                """
                SELECT *
                FROM candidate_reviews
                WHERE candidate_key=?
                ORDER BY review_seq DESC
                LIMIT 1
                """,
                (candidate_key,),
            ).fetchone()

    def resolve_candidate_materialization(self, candidate_key: str) -> dict[str, Any]:
        candidate = self.get_promotion_candidate(candidate_key)
        if candidate is None or str(candidate["status"]) not in {"accepted", "relabeled"}:
            raise ValueError(f"candidate is not materializable: {candidate_key}")
        review = self.latest_candidate_review(candidate_key)
        if review is None:
            raise ValueError(f"latest candidate review not found: {candidate_key}")
        effective_label = (
            str(review["human_label"])
            if review["human_verdict"] == "relabeled"
            else str(candidate["label"])
        )
        resolution = self._materialization_resolution(
            candidate_key=candidate_key,
            review_seq=int(review["review_seq"]),
            effective_label=effective_label,
            materialization_key=None,
        )
        existing = self.get_materialization_record_for_resolution(
            candidate_key=candidate_key,
            review_seq=resolution["review_seq"],
            effective_label=str(resolution["effective_label"]),
        )
        if existing is not None and str(existing["status"]) == "applied":
            return {**resolution, "result": "already_applied"}
        dispatch = self.enqueue_materialization_resolution(**resolution)
        return {**resolution, "result": "enqueued", "dispatch": dispatch}

    def materializable_candidates(
        self,
        *,
        candidate_key: str | None = None,
        limit: int | None = 50,
    ) -> list[sqlite3.Row]:
        with schema_locked_connection() as conn:
            sql = """
                SELECT *
                FROM promotion_candidates
                WHERE status IN ('accepted', 'relabeled')
            """
            params: list[Any] = []
            if candidate_key is not None:
                sql += "\n  AND candidate_key=?"
                params.append(candidate_key)
            sql += "\nORDER BY resolved_at, created_at, candidate_key"
            if limit is not None:
                sql += "\nLIMIT ?"
                params.append(limit)
            return conn.execute(sql, tuple(params)).fetchall()

    def get_materialization_record_for_resolution(
        self,
        *,
        candidate_key: str,
        review_seq: int,
        effective_label: str,
    ) -> sqlite3.Row | None:
        with schema_locked_connection() as conn:
            return conn.execute(
                """
                SELECT *
                FROM materialization_records
                WHERE candidate_key=? AND review_seq=? AND effective_label=?
                LIMIT 1
                """,
                (candidate_key, review_seq, effective_label),
            ).fetchone()

    def latest_materialization_review_seq(self, *, candidate_key: str, effective_label: str) -> int:
        with schema_locked_connection() as conn:
            row = conn.execute(
                """
                SELECT MAX(review_seq) AS max_review_seq
                FROM materialization_records
                WHERE candidate_key=? AND effective_label=?
                """,
                (candidate_key, effective_label),
            ).fetchone()
            return int(row["max_review_seq"] or 0)

    def retryable_materialization_records(self, *, limit: int = 50) -> list[sqlite3.Row]:
        with schema_locked_connection() as conn:
            return conn.execute(
                """
                SELECT *
                FROM materialization_records
                WHERE status='repair_pending'
                   OR status='failed'
                   OR (status='applying' AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?)
                ORDER BY updated_at, created_at, materialization_key
                LIMIT ?
                """,
                (utc_now_iso(), limit),
            ).fetchall()

    def claim_retryable_materialization(
        self,
        *,
        materialization_key: str,
        lease_owner: str,
        lease_expires_at: str,
    ) -> sqlite3.Row | None:
        with self.transaction() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM materialization_records
                WHERE materialization_key=?
                LIMIT 1
                """,
                (materialization_key,),
            ).fetchone()
            if row is None:
                return None
            status = str(row["status"])
            now = utc_now_iso()
            retryable = (
                status == "repair_pending"
                or status == "failed"
                or (status == "applying" and row["lease_expires_at"] and str(row["lease_expires_at"]) <= now)
            )
            if not retryable:
                return None
            if row["lease_expires_at"] and str(row["lease_expires_at"]) > now and row["lease_owner"] not in {None, lease_owner}:
                return None
            next_epoch = int(row["lease_epoch"]) + 1
            updated = conn.execute(
                """
                UPDATE materialization_records
                SET status='repair_pending',
                    lease_owner=?,
                    lease_expires_at=?,
                    lease_epoch=?,
                    updated_at=?
                WHERE materialization_key=?
                  AND (
                    lease_expires_at IS NULL
                    OR lease_expires_at <= ?
                    OR lease_owner=?
                  )
                """,
                (
                    lease_owner,
                    lease_expires_at,
                    next_epoch,
                    _store_now_iso(),
                    materialization_key,
                    now,
                    lease_owner,
                ),
            )
            if updated.rowcount == 0:
                return None
            return conn.execute(
                "SELECT * FROM materialization_records WHERE materialization_key=?",
                (materialization_key,),
            ).fetchone()

    def retry_materialization(
        self,
        *,
        materialization_key: str,
        lease_owner: str,
        lease_expires_at: str,
    ) -> dict[str, Any] | None:
        claimed = self.claim_retryable_materialization(
            materialization_key=materialization_key,
            lease_owner=lease_owner,
            lease_expires_at=lease_expires_at,
        )
        if claimed is None:
            return None
        try:
            latest_review_seq = self.latest_materialization_review_seq(
                candidate_key=str(claimed["candidate_key"]),
                effective_label=str(claimed["effective_label"]),
            )
            resolution = self._materialization_resolution(
                candidate_key=str(claimed["candidate_key"]),
                review_seq=int(claimed["review_seq"]),
                effective_label=str(claimed["effective_label"]),
                materialization_key=str(claimed["materialization_key"]),
            )
            dispatch = self.enqueue_materialization_resolution(**resolution)
            self.release_materialization_record(
                materialization_key=materialization_key,
                lease_owner=lease_owner,
                lease_epoch=int(claimed["lease_epoch"]),
                status="superseded" if latest_review_seq > int(claimed["review_seq"]) else "planned",
            )
            return {
                **resolution,
                "status": dispatch.status,
                "aggregate_version": dispatch.aggregate_version,
                "queued_sinks": dispatch.queued_sinks,
            }
        except Exception as exc:
            self.release_materialization_record(
                materialization_key=materialization_key,
                lease_owner=lease_owner,
                lease_epoch=int(claimed["lease_epoch"]),
                status="failed",
            )
            with self.transaction() as conn:
                conn.execute(
                    """
                    UPDATE materialization_records
                    SET last_error=?, updated_at=?
                    WHERE materialization_key=?
                    """,
                    (str(exc)[:500], _store_now_iso(), materialization_key),
                )
            raise

    def get_candidate_review(self, candidate_key: str, review_seq: int) -> sqlite3.Row | None:
        with self.transaction() as conn:
            return conn.execute(
                """
                SELECT *
                FROM candidate_reviews
                WHERE candidate_key=? AND review_seq=?
                LIMIT 1
                """,
                (candidate_key, review_seq),
            ).fetchone()

    def get_note_mutation(self, *, note_id: str, request_key: str) -> sqlite3.Row | None:
        with self.transaction() as conn:
            return conn.execute(
                """
                SELECT *
                FROM note_mutations
                WHERE note_id=? AND request_key=?
                LIMIT 1
                """,
                (note_id, request_key),
            ).fetchone()

    def record_note_mutation(
        self,
        *,
        mutation_id: str,
        note_id: str,
        note_path: str,
        mutation_kind: str,
        request_key: str,
        before_sha256: str,
        after_sha256: str,
        payload: dict[str, Any],
    ) -> bool:
        with self.transaction() as conn:
            result = conn.execute(
                """
                INSERT OR IGNORE INTO note_mutations(
                  mutation_id, note_id, note_path, mutation_kind, request_key,
                  before_sha256, after_sha256, payload_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    mutation_id,
                    note_id,
                    note_path,
                    mutation_kind,
                    request_key,
                    before_sha256,
                    after_sha256,
                    json.dumps(payload, ensure_ascii=False),
                    _store_now_iso(),
                ),
            )
            return result.rowcount > 0

    def _upsert_materialization_record_conn(
        self,
        conn: sqlite3.Connection,
        *,
        materialization_key: str,
        candidate_key: str,
        review_seq: int,
        judge_run_key: str,
        window_id: str,
        materialized_label: str,
        effective_label: str,
        status: str,
        payload: dict[str, Any],
        note_id: str | None = None,
        note_path: str | None = None,
        promotion_key: str | None = None,
        supersedes_materialization_key: str | None = None,
        last_error: str | None = None,
        lease_owner: str | None = None,
        lease_expires_at: str | None = None,
        lease_epoch: int | None = None,
    ) -> None:
        if materialized_label not in _CANDIDATE_LABELS:
            raise ValueError(f"invalid materialized label: {materialized_label}")
        if effective_label not in _CANDIDATE_LABELS:
            raise ValueError(f"invalid effective label: {effective_label}")
        if status not in _MATERIALIZATION_STATUSES:
            raise ValueError(f"invalid materialization status: {status}")
        now = _store_now_iso()
        existing = conn.execute(
            """
            SELECT lease_epoch
            FROM materialization_records
            WHERE materialization_key=?
            """,
            (materialization_key,),
        ).fetchone()
        epoch = int(existing["lease_epoch"]) if existing is not None else 0
        if lease_epoch is not None:
            epoch = lease_epoch
        conn.execute(
            """
            INSERT INTO materialization_records(
              materialization_key, candidate_key, review_seq, judge_run_key, window_id,
              materialized_label, effective_label, status, note_id, note_path,
              promotion_key, supersedes_materialization_key, payload_json, last_error,
              lease_owner, lease_expires_at, lease_epoch, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(materialization_key) DO UPDATE SET
              candidate_key=excluded.candidate_key,
              review_seq=excluded.review_seq,
              judge_run_key=excluded.judge_run_key,
              window_id=excluded.window_id,
              materialized_label=excluded.materialized_label,
              effective_label=excluded.effective_label,
              status=excluded.status,
              note_id=COALESCE(excluded.note_id, materialization_records.note_id),
              note_path=COALESCE(excluded.note_path, materialization_records.note_path),
              promotion_key=COALESCE(excluded.promotion_key, materialization_records.promotion_key),
              supersedes_materialization_key=COALESCE(excluded.supersedes_materialization_key, materialization_records.supersedes_materialization_key),
              payload_json=excluded.payload_json,
              last_error=excluded.last_error,
              lease_owner=excluded.lease_owner,
              lease_expires_at=excluded.lease_expires_at,
              lease_epoch=excluded.lease_epoch,
              updated_at=excluded.updated_at
            """,
            (
                materialization_key,
                candidate_key,
                review_seq,
                judge_run_key,
                window_id,
                materialized_label,
                effective_label,
                status,
                note_id,
                note_path,
                promotion_key,
                supersedes_materialization_key,
                json.dumps(payload, ensure_ascii=False),
                last_error,
                lease_owner,
                lease_expires_at,
                epoch,
                now,
                now,
            ),
        )

    def mark_candidates_suggested(self, candidate_keys: list[str]) -> int:
        if not candidate_keys:
            return 0
        now = _store_now_iso()
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
        reviewed_at = _store_now_iso()
        with self.transaction() as conn:
            candidate = conn.execute(
                """
                SELECT candidate_key, window_id, judge_run_key, status
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
            if candidate["status"] != "pending_review":
                raise ValueError(f"candidate is not pending_review: {candidate_key}")
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
            if human_verdict == "accepted":
                candidate_status = "accepted"
            elif human_verdict == "relabeled":
                candidate_status = "relabeled"
            else:
                candidate_status = "rejected"
            conn.execute(
                """
                UPDATE promotion_candidates
                SET status=?, resolved_at=?, updated_at=?
                WHERE candidate_key=?
                """,
                (candidate_status, reviewed_at, reviewed_at, candidate_key),
            )
            return review_seq

    def pending_review_candidates(self, *, limit: int | None = 50) -> list[sqlite3.Row]:
        with schema_locked_connection() as conn:
            sql = """
                SELECT *
                FROM promotion_candidates
                WHERE status='pending_review'
                ORDER BY created_at, candidate_key
            """
            params: tuple[Any, ...] = ()
            if limit is not None:
                sql += "\nLIMIT ?"
                params = (limit,)
            return conn.execute(sql, params).fetchall()

    def get_promotion_candidate(self, candidate_key: str) -> sqlite3.Row | None:
        with schema_locked_connection() as conn:
            return conn.execute(
                """
                SELECT *
                FROM promotion_candidates
                WHERE candidate_key=?
                LIMIT 1
                """,
                (candidate_key,),
            ).fetchone()

    def judge_run_counts(self) -> dict[str, int]:
        with schema_locked_connection() as conn:
            rows = conn.execute(
                """
                SELECT status, COUNT(*) AS count
                FROM judge_runs
                GROUP BY status
                """
            ).fetchall()
        counts = {str(row["status"]): int(row["count"]) for row in rows}
        return {
            "ready": counts.get("ready", 0),
            "judged": counts.get("judged", 0),
            "superseded": counts.get("superseded", 0),
            "failed": counts.get("failed", 0),
        }

    def pending_review_candidate_count(self) -> int:
        with schema_locked_connection() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS count FROM promotion_candidates WHERE status='pending_review'"
            ).fetchone()
            return int(row["count"])

    def candidate_review_count(self) -> int:
        with schema_locked_connection() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS count FROM candidate_reviews"
            ).fetchone()
            return int(row["count"])

    def suggestable_review_candidates(self) -> list[sqlite3.Row]:
        with schema_locked_connection() as conn:
            return conn.execute(
                """
                SELECT *
                FROM promotion_candidates
                WHERE status='pending_review'
                  AND (last_suggested_at IS NULL OR updated_at > last_suggested_at)
                ORDER BY created_at, candidate_key
                """
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

    def _materialization_resolution(
        self,
        *,
        candidate_key: str,
        review_seq: int,
        effective_label: str,
        materialization_key: str | None,
    ) -> dict[str, Any]:
        candidate = self.get_promotion_candidate(candidate_key)
        if candidate is None:
            raise ValueError(f"candidate not found: {candidate_key}")
        candidate_payload = json.loads(str(candidate["payload_json"]))
        window = dict(candidate_payload.get("window") or {})
        existing = self.get_materialization_record_for_resolution(
            candidate_key=candidate_key,
            review_seq=review_seq,
            effective_label=effective_label,
        )
        resolved_materialization_key = materialization_key
        if existing is not None and not resolved_materialization_key:
            resolved_materialization_key = str(existing["materialization_key"])
        if not resolved_materialization_key:
            resolved_materialization_key = f"materialize:{candidate_key}:{review_seq}:{effective_label}"
        return {
            "candidate_key": candidate_key,
            "review_seq": review_seq,
            "effective_label": effective_label,
            "materialization_key": resolved_materialization_key,
            "judge_run_key": str(candidate["judge_run_key"]),
            "window_id": str(candidate["window_id"]),
            "payload": {"candidate_key": candidate_key, "review_seq": review_seq},
            "project": _window_value(window, "project"),
            "cwd": _window_value(window, "cwd"),
            "repo": _window_value(window, "repo"),
        }


def _merge_envelope(conn: sqlite3.Connection, envelope: EventEnvelope) -> tuple[int, str, list[str]]:
    existing = conn.execute(
        "SELECT * FROM logical_events WHERE logical_key=?",
        (envelope.logical_key,),
    ).fetchone()
    state = dict(envelope.aggregate_state)
    existing_state: dict[str, Any] = {}
    if existing:
        existing_state = json.loads(existing["aggregate_state_json"])
        merged_state = dict(existing_state)
        merged_state.update({k: v for k, v in state.items() if v is not None})
        state = merged_state
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
    elif envelope.aggregate_type == "review_materialization":
        incoming_review_seq = int(envelope.aggregate_state.get("review_seq") or 0)
        existing_review_seq = int(existing_state.get("review_seq") or 0) if existing else 0
        if existing and existing_review_seq > incoming_review_seq:
            return int(existing["aggregate_version"]), str(existing["status"]), []
        if existing and incoming_review_seq > existing_review_seq:
            conn.execute(
                """
                UPDATE outbox
                SET status='dead_letter', last_error='superseded by newer review_materialization'
                WHERE logical_key=? AND status!='applied'
                """,
                (envelope.logical_key,),
            )
        status = "ready"
        queued = ["promotion_planner", "promotion_applier"]

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


def _append_envelope(conn: sqlite3.Connection, envelope: EventEnvelope) -> tuple[int, str, list[str]]:
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
    return _merge_envelope(conn, envelope)


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


def _window_value(window: dict[str, Any], key: str) -> str | None:
    checkpoints = list(window.get("checkpoints") or [])
    for checkpoint in reversed(checkpoints):
        if not isinstance(checkpoint, dict):
            continue
        value = checkpoint.get(key)
        if isinstance(value, str) and value:
            return value
    return None
