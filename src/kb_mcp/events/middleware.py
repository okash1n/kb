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
from kb_mcp.learning.application_trace import record_learning_application
from kb_mcp.learning.models import ResolverInput
from kb_mcp.learning.packet_builder import build_learning_packet
from kb_mcp.learning.resolver import resolve_learning_assets

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


def _prepare_learning_packet(
    *,
    source_tool: str,
    source_client: str,
    tool_name: str,
    call_kwargs: dict[str, Any],
) -> dict[str, Any]:
    current = dict(REQUEST_CONTEXT.get() or {})
    request = ResolverInput(
        source_tool=source_tool,
        source_client=source_client,
        session_id=current.get("vendor_session_id"),
        project=call_kwargs.get("project"),
        cwd=call_kwargs.get("cwd"),
        repo=call_kwargs.get("repo"),
    )
    assets = resolve_learning_assets(request)
    packet = build_learning_packet(request, tool_name=tool_name, assets=assets)
    if packet is None:
        return current
    current["packet_id"] = packet["packet_id"]
    current["applied_asset_keys"] = packet["asset_keys"]
    return current


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
    if request_context.get("packet_id"):
        body["packet_id"] = request_context["packet_id"]
        body["applied_asset_keys"] = list(request_context.get("applied_asset_keys") or [])
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
            request_context = _build_request_context(tool_name, ctx)
            token = REQUEST_CONTEXT.set(request_context)
            REQUEST_CONTEXT.set(
                _prepare_learning_packet(
                    source_tool=source_tool,
                    source_client=source_client,
                    tool_name=tool_name,
                    call_kwargs=call_kwargs,
                )
            )
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
            _record_learning_application_from_context(
                source_tool=source_tool,
                source_client=source_client,
                tool_name=tool_name,
            )
            REQUEST_CONTEXT.reset(token)
            return result

        return wrapped_async  # type: ignore[return-value]

    def wrapped(*args: Any, **kwargs: Any) -> Any:
        call_kwargs = dict(kwargs)
        ctx = call_kwargs.pop("ctx", None)
        request_context = _build_request_context(tool_name, ctx)
        token = REQUEST_CONTEXT.set(request_context)
        REQUEST_CONTEXT.set(
            _prepare_learning_packet(
                source_tool=source_tool,
                source_client=source_client,
                tool_name=tool_name,
                call_kwargs=call_kwargs,
            )
        )
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
        _record_learning_application_from_context(
            source_tool=source_tool,
            source_client=source_client,
            tool_name=tool_name,
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


def _record_learning_application_from_context(
    *,
    source_tool: str,
    source_client: str,
    tool_name: str,
) -> None:
    current = REQUEST_CONTEXT.get() or {}
    packet_id = current.get("packet_id")
    tool_call_id = current.get("tool_call_id")
    if not packet_id or not tool_call_id:
        return
    record_learning_application(
        packet_id=str(packet_id),
        tool_name=tool_name,
        tool_call_id=str(tool_call_id),
        source_tool=source_tool,
        source_client=source_client,
        session_id=current.get("vendor_session_id"),
        save_request_id=current.get("save_request_id"),
        saved_note_id=current.get("saved_note_id"),
        saved_note_path=current.get("saved_note_path"),
    )
