"""Normalize raw hook / middleware payloads into event envelopes."""

from __future__ import annotations

from typing import Any

from kb_mcp.events.identity import (
    compact_logical_key,
    checkpoint_partition_key,
    correlation_id_for_session,
    error_logical_key,
    session_logical_key,
    tool_logical_key,
)
from kb_mcp.events.redaction import redact_payload
from kb_mcp.events.transcript_reader import read_transcript_excerpt
from kb_mcp.events.types import (
    CHECKPOINT_EVENTS,
    COMPACT_EVENTS,
    ERROR_EVENTS,
    EventEnvelope,
    KNOWN_EVENTS,
    SESSION_EVENTS,
    TOOL_EVENTS,
    utc_now_iso,
)
from kb_mcp.note import generate_ulid

ALLOW_TOOLS = {"claude", "copilot", "codex", "kb"}
ALLOW_LAYERS = {"client_hook", "session_launcher", "server_middleware", "recovery_sweeper", "transcript_fallback"}


def _management_mode(layer: str, session_id: str | None) -> str:
    if layer == "session_launcher":
        return "launcher"
    if session_id:
        return "hook"
    return "unmanaged"


def normalize_event(
    *,
    tool: str,
    client: str,
    layer: str,
    event: str,
    payload: dict[str, Any],
) -> EventEnvelope:
    """Normalize a raw event payload."""
    if tool not in ALLOW_TOOLS:
        raise ValueError(f"Unsupported tool: {tool}")
    if layer not in ALLOW_LAYERS:
        raise ValueError(f"Unsupported layer: {layer}")
    if event not in KNOWN_EVENTS:
        raise ValueError(f"Unsupported event: {event}")

    session_id = (
        payload.get("session_id")
        or payload.get("conversation_id")
        or payload.get("chat_id")
    )
    tool_call_id = payload.get("tool_call_id") or payload.get("request_id")
    error_fingerprint = payload.get("error_fingerprint") or payload.get("event_id")
    transcript_path = payload.get("transcript_path")
    content_excerpt = (
        payload.get("content")
        or read_transcript_excerpt(transcript_path)
        or payload.get("summary")
        or payload.get("message")
    )
    summary = (
        payload.get("summary")
        or payload.get("last_assistant_message")
        or payload.get("message")
        or event
    )
    received_at = utc_now_iso()
    occurred_at = payload.get("occurred_at") or received_at
    correlation_id = correlation_id_for_session(tool, client, session_id)
    management_mode = _management_mode(layer, session_id)
    partition_key: str | None = None
    ordinal: int | None = None

    if event in SESSION_EVENTS:
        aggregate_type = "session"
        logical_key = session_logical_key(correlation_id)
    elif event in TOOL_EVENTS:
        aggregate_type = "tool"
        logical_key = tool_logical_key(
            correlation_id,
            tool_call_id or generate_ulid(),
            source_tool=tool,
            source_client=client,
        )
    elif event in ERROR_EVENTS:
        aggregate_type = "error"
        logical_key = error_logical_key(
            correlation_id,
            error_fingerprint or generate_ulid(),
            source_tool=tool,
            source_client=client,
        )
    elif event in COMPACT_EVENTS:
        aggregate_type = "compact"
        partition_key = checkpoint_partition_key(
            correlation_id,
            source_tool=tool,
            source_client=client,
            cwd=payload.get("cwd"),
            transcript_path=transcript_path,
            occurred_at=occurred_at,
        )
        ordinal = int(payload.get("ordinal") or payload.get("sequence") or 1)
        logical_key = compact_logical_key(partition_key, ordinal)
    elif event in CHECKPOINT_EVENTS:
        aggregate_type = "compact"
        partition_key = checkpoint_partition_key(
            correlation_id,
            source_tool=tool,
            source_client=client,
            cwd=payload.get("cwd"),
            transcript_path=transcript_path,
            occurred_at=occurred_at,
        )
        ordinal = int(payload.get("checkpoint_ordinal") or payload.get("ordinal") or 0)
        logical_key = compact_logical_key(partition_key, ordinal)
    else:  # pragma: no cover
        raise AssertionError(f"Unhandled event: {event}")

    state = {
        "event_name": event,
        "tool_name": payload.get("tool_name"),
        "exit_code": payload.get("exit_code"),
        "signal": payload.get("signal"),
        "reason": payload.get("reason"),
        "checkpoint_kind": payload.get("checkpoint_kind") or "turn",
        "final_hint": bool(payload.get("final_hint", False)),
        "checkpoint_partition_key": partition_key if event in (CHECKPOINT_EVENTS | COMPACT_EVENTS) else None,
        "checkpoint_ordinal": ordinal if event in (CHECKPOINT_EVENTS | COMPACT_EVENTS) and ordinal else None,
    }
    return EventEnvelope(
        event_id=generate_ulid(),
        occurred_at=occurred_at,
        received_at=received_at,
        source_tool=tool,
        source_client=client,
        source_layer=layer,
        event_name=event,
        aggregate_type=aggregate_type,
        management_mode=management_mode,
        logical_key=logical_key,
        correlation_id=correlation_id,
        session_id=session_id,
        tool_call_id=tool_call_id,
        error_fingerprint=error_fingerprint,
        summary=str(summary)[:200],
        content_excerpt=(str(content_excerpt)[:4000] if content_excerpt else None),
        cwd=payload.get("cwd"),
        repo=payload.get("repo"),
        project=payload.get("project"),
        transcript_path=transcript_path,
        aggregate_state={k: v for k, v in state.items() if v is not None},
        raw_payload=dict(payload),
        redacted_payload=redact_payload(payload),
    )
