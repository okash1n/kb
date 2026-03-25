"""Request-context wrapper for kb-owned MCP tool events."""

from __future__ import annotations

import os
import inspect
from typing import Any, Callable, TypeVar
from uuid import uuid4

from mcp.server.fastmcp import Context

from kb_mcp.events.request_context import REQUEST_CONTEXT
from kb_mcp.events.normalize import normalize_event
from kb_mcp.events.store import EventStore
from kb_mcp.events.worker import run_once

F = TypeVar("F", bound=Callable[..., Any])

def _build_request_context(tool_name: str, ctx: Context | None) -> dict[str, Any]:
    request_id = None
    meta = None
    if ctx is not None:
        request_id = str(ctx.request_id)
        meta = ctx.request_context.meta
    correlation_id = os.environ.get("KB_SESSION_CORRELATION_ID") or None
    vendor_session_id = os.environ.get("KB_VENDOR_SESSION_ID") or None
    tool_call_id = request_id or str(uuid4())
    return {
        "tool_name": tool_name,
        "request_id": request_id,
        "meta": meta,
        "correlation_id": correlation_id,
        "vendor_session_id": vendor_session_id,
        "tool_call_id": tool_call_id,
        "save_request_id": str(uuid4()) if tool_name in {"adr", "gap", "knowledge", "session", "draft"} else None,
    }


def emit_tool_event(
    *,
    source_tool: str,
    source_client: str,
    event_name: str,
    tool_name: str,
    ctx: Context | None = None,
    payload: dict[str, Any] | None = None,
) -> None:
    """Emit an authoritative tool event."""
    request_context = REQUEST_CONTEXT.get() or _build_request_context(tool_name, ctx)
    body = {
        "tool_name": tool_name,
        "tool_call_id": request_context["tool_call_id"],
        "session_id": request_context["vendor_session_id"],
    }
    if payload:
        body.update(payload)
    envelope = normalize_event(
        tool=source_tool,
        client=source_client,
        layer="server_middleware",
        event=event_name,
        payload=body,
    )
    EventStore().append(envelope)
    run_once()


def with_tool_events(source_tool: str, source_client: str, tool_name: str, fn: F) -> F:
    """Wrap a tool handler with before/after/error event emission."""

    if inspect.iscoroutinefunction(fn):
        async def wrapped_async(*args: Any, **kwargs: Any) -> Any:
            call_kwargs = dict(kwargs)
            ctx = call_kwargs.pop("ctx", None)
            token = REQUEST_CONTEXT.set(_build_request_context(tool_name, ctx))
            emit_tool_event(
                source_tool=source_tool,
                source_client=source_client,
                event_name="tool_started",
                tool_name=tool_name,
                ctx=ctx,
            )
            try:
                result = await fn(*args, **call_kwargs)
            except Exception as exc:
                emit_tool_event(
                    source_tool=source_tool,
                    source_client=source_client,
                    event_name="tool_failed",
                    tool_name=tool_name,
                    ctx=ctx,
                    payload={"message": str(exc)},
                )
                REQUEST_CONTEXT.reset(token)
                raise
            emit_tool_event(
                source_tool=source_tool,
                source_client=source_client,
                event_name="tool_succeeded",
                tool_name=tool_name,
                ctx=ctx,
                payload=_success_payload(),
            )
            REQUEST_CONTEXT.reset(token)
            return result

        return wrapped_async  # type: ignore[return-value]

    def wrapped(*args: Any, **kwargs: Any) -> Any:
        call_kwargs = dict(kwargs)
        ctx = call_kwargs.pop("ctx", None)
        token = REQUEST_CONTEXT.set(_build_request_context(tool_name, ctx))
        emit_tool_event(
            source_tool=source_tool,
            source_client=source_client,
            event_name="tool_started",
            tool_name=tool_name,
            ctx=ctx,
        )
        try:
            result = fn(*args, **call_kwargs)
        except Exception as exc:
            emit_tool_event(
                source_tool=source_tool,
                source_client=source_client,
                event_name="tool_failed",
                tool_name=tool_name,
                ctx=ctx,
                payload={"message": str(exc)},
            )
            REQUEST_CONTEXT.reset(token)
            raise
        emit_tool_event(
            source_tool=source_tool,
            source_client=source_client,
            event_name="tool_succeeded",
            tool_name=tool_name,
            ctx=ctx,
            payload=_success_payload(),
        )
        REQUEST_CONTEXT.reset(token)
        return result

    return wrapped  # type: ignore[return-value]


def _success_payload() -> dict[str, Any]:
    current = REQUEST_CONTEXT.get() or {}
    payload: dict[str, Any] = {}
    for key in ["save_request_id", "saved_note_id", "saved_note_path", "saved_note_type"]:
        value = current.get(key)
        if value:
            payload[key] = value
    return payload
