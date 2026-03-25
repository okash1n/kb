"""Worker that drains outbox sinks."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from kb_mcp.events.policies.candidate_writer import write_candidates
from kb_mcp.events.policies.checkpoint_writer import write_checkpoint
from kb_mcp.events.policies.incident_writer import write_incident
from kb_mcp.events.policies.promotion_applier import apply_promotion
from kb_mcp.events.policies.promotion_planner import write_promotion_plan
from kb_mcp.events.policies.session_finalizer import finalize_session
from kb_mcp.events.store import EventStore
from kb_mcp.note import generate_ulid

SINK_HANDLERS = {
    "session_finalizer": finalize_session,
    "incident_writer": write_incident,
    "checkpoint_writer": write_checkpoint,
    "candidate_writer": write_candidates,
    "promotion_planner": write_promotion_plan,
    "promotion_applier": apply_promotion,
}


def run_once(*, maintenance: bool = False, limit: int = 50) -> dict[str, int]:
    """Drain due outbox rows once."""
    store = EventStore()
    claimed = store.ready_sinks(maintenance=maintenance, limit=limit)
    result = {"claimed": len(claimed), "applied": 0, "failed": 0}
    for item in claimed:
        logical = store.fetch_logical_event(item["logical_key"])
        if logical is None:
            store.mark_sink_failed(item["id"], "missing logical_event row")
            result["failed"] += 1
            continue
        handler = SINK_HANDLERS.get(item["sink_name"])
        if handler is None:
            store.mark_sink_failed(item["id"], f"unknown sink {item['sink_name']}")
            result["failed"] += 1
            continue
        try:
            receipt = handler(logical)
            store.mark_sink_succeeded(
                item["id"],
                item["logical_key"],
                int(item["aggregate_version"]),
                item["sink_name"],
                receipt,
            )
            result["applied"] += 1
        except Exception as exc:  # pragma: no cover - exercised via integration
            store.mark_sink_failed(item["id"], str(exc))
            result["failed"] += 1
    return result


def retry_failed_materializations(*, limit: int = 50) -> dict[str, object]:
    """Requeue retryable materialization records."""
    store = EventStore()
    retried: list[dict[str, object]] = []
    skipped_records: list[dict[str, object]] = []
    skipped = 0
    lease_expires_at = (
        datetime.now(timezone.utc) + timedelta(minutes=5)
    ).isoformat(timespec="seconds")
    retry_run_id = generate_ulid()
    for record in store.retryable_materialization_records(limit=limit):
        try:
            result = store.retry_materialization(
                materialization_key=str(record["materialization_key"]),
                lease_owner=f"retry:{retry_run_id}:{record['materialization_key']}",
                lease_expires_at=lease_expires_at,
            )
        except Exception as exc:
            skipped += 1
            skipped_records.append(
                {
                    "materialization_key": str(record["materialization_key"]),
                    "candidate_key": str(record["candidate_key"]),
                    "error": str(exc),
                }
            )
            continue
        if result is None:
            skipped += 1
            skipped_records.append(
                {
                    "materialization_key": str(record["materialization_key"]),
                    "candidate_key": str(record["candidate_key"]),
                    "error": "record is no longer retryable",
                }
            )
            continue
        retried.append(
            {
                "materialization_key": str(record["materialization_key"]),
                "candidate_key": str(record["candidate_key"]),
                "effective_label": str(record["effective_label"]),
                "status": result["status"],
                "aggregate_version": result["aggregate_version"],
                "queued_sinks": result["queued_sinks"],
            }
        )
    return {
        "retried": len(retried),
        "skipped": skipped,
        "records": retried,
        "skipped_records": skipped_records,
    }
