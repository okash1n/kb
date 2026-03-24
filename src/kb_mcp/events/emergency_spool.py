"""Emergency spool for payload capture during storage failures."""

from __future__ import annotations

import json
from pathlib import Path

from kb_mcp.config import runtime_events_dir
from kb_mcp.events.types import EventEnvelope


def spool_dir() -> Path:
    path = runtime_events_dir() / "spool"
    path.mkdir(parents=True, exist_ok=True)
    return path


def spool_event(envelope: EventEnvelope) -> Path:
    """Persist a redacted event payload for later replay."""
    path = spool_dir() / f"{envelope.event_id}.json"
    data = {
        "event_id": envelope.event_id,
        "occurred_at": envelope.occurred_at,
        "received_at": envelope.received_at,
        "source_tool": envelope.source_tool,
        "source_client": envelope.source_client,
        "source_layer": envelope.source_layer,
        "event_name": envelope.event_name,
        "payload": envelope.redacted_payload,
    }
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path
