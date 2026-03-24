"""Session launcher that emits lifecycle events around a child process."""

from __future__ import annotations

import os
import subprocess
import uuid
from typing import Any

from kb_mcp.events.normalize import normalize_event
from kb_mcp.events.store import EventStore
from kb_mcp.events.worker import run_once


def launch_session(
    *,
    tool: str,
    client: str,
    command: list[str],
    cwd: str | None = None,
    env: dict[str, str] | None = None,
) -> int:
    """Run a tool session under launcher management."""
    effective_cwd = cwd or os.getcwd()
    session_id = uuid.uuid4().hex
    payload: dict[str, Any] = {"cwd": effective_cwd, "session_id": session_id}
    store = EventStore()
    started = normalize_event(
        tool=tool,
        client=client,
        layer="session_launcher",
        event="session_started",
        payload=payload,
    )
    store.append(started)
    child_env = os.environ.copy()
    child_env.update(env or {})
    child_env["KB_SESSION_CORRELATION_ID"] = started.correlation_id or ""
    child_env["KB_VENDOR_SESSION_ID"] = session_id
    child_env["KB_SOURCE_TOOL"] = tool
    child_env["KB_SOURCE_CLIENT"] = client
    completed = subprocess.run(command, cwd=effective_cwd, env=child_env, check=False)
    store.append(
        normalize_event(
            tool=tool,
            client=client,
            layer="session_launcher",
            event="process_exit",
            payload={
                "cwd": effective_cwd,
                "session_id": session_id,
                "exit_code": completed.returncode,
            },
        )
    )
    store.append(
        normalize_event(
            tool=tool,
            client=client,
            layer="session_launcher",
            event="session_ended",
            payload={
                "cwd": effective_cwd,
                "session_id": session_id,
                "summary": f"{tool} session exited with code {completed.returncode}",
            },
        )
    )
    run_once(maintenance=True)
    return completed.returncode
