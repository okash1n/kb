"""Record runtime learning packet applications."""

from __future__ import annotations

from kb_mcp.events.store import EventStore
from kb_mcp.note import generate_ulid


def record_learning_application(
    *,
    packet_id: str,
    tool_name: str,
    tool_call_id: str,
    source_tool: str,
    source_client: str,
    session_id: str | None,
    save_request_id: str | None,
    saved_note_id: str | None,
    saved_note_path: str | None,
    store: EventStore | None = None,
) -> str:
    application_id = generate_ulid()
    (store or EventStore()).record_learning_application(
        application_id=application_id,
        packet_id=packet_id,
        tool_name=tool_name,
        tool_call_id=tool_call_id,
        source_tool=source_tool,
        source_client=source_client,
        session_id=session_id,
        save_request_id=save_request_id,
        saved_note_id=saved_note_id,
        saved_note_path=saved_note_path,
    )
    return application_id
