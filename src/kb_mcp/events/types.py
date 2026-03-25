"""Core event pipeline types."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal

SourceTool = Literal["claude", "copilot", "codex", "kb"]
SourceLayer = Literal[
    "client_hook",
    "session_launcher",
    "server_middleware",
    "recovery_sweeper",
    "transcript_fallback",
]
AggregateType = Literal["session", "tool", "error", "compact", "review_materialization"]
LogicalStatus = Literal["collecting", "pending_finalization", "ready", "applied", "dead_letter"]
ManagementMode = Literal["launcher", "hook", "unmanaged"]

SESSION_EVENTS = {
    "session_started",
    "session_ended",
    "user_prompt_submitted",
    "process_exit",
}
TOOL_EVENTS = {"tool_started", "tool_succeeded", "tool_failed"}
ERROR_EVENTS = {"agent_error"}
COMPACT_EVENTS = {"compact_started", "compact_finished"}
CHECKPOINT_EVENTS = {"turn_checkpointed"}
KNOWN_EVENTS = SESSION_EVENTS | TOOL_EVENTS | ERROR_EVENTS | COMPACT_EVENTS | CHECKPOINT_EVENTS


def utc_now_iso() -> str:
    """Return the current UTC time in ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass(slots=True)
class EventEnvelope:
    """Normalized event envelope shared by all producers."""

    event_id: str
    occurred_at: str
    received_at: str
    source_tool: str
    source_client: str
    source_layer: SourceLayer
    event_name: str
    aggregate_type: AggregateType
    management_mode: ManagementMode
    logical_key: str
    correlation_id: str | None
    session_id: str | None
    tool_call_id: str | None = None
    error_fingerprint: str | None = None
    summary: str | None = None
    content_excerpt: str | None = None
    cwd: str | None = None
    repo: str | None = None
    project: str | None = None
    transcript_path: str | None = None
    aggregate_state: dict[str, Any] = field(default_factory=dict)
    raw_payload: dict[str, Any] = field(default_factory=dict)
    redacted_payload: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class DispatchResult:
    """Result returned by dispatch / append_event."""

    event_id: str
    logical_key: str
    aggregate_type: AggregateType
    status: LogicalStatus
    aggregate_version: int
    queued_sinks: list[str] = field(default_factory=list)
