"""Worker that drains outbox sinks."""

from __future__ import annotations

from kb_mcp.events.policies.candidate_writer import write_candidates
from kb_mcp.events.policies.checkpoint_writer import write_checkpoint
from kb_mcp.events.policies.incident_writer import write_incident
from kb_mcp.events.policies.promotion_applier import apply_promotion
from kb_mcp.events.policies.promotion_planner import write_promotion_plan
from kb_mcp.events.policies.session_finalizer import finalize_session
from kb_mcp.events.store import EventStore

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
