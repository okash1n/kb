"""Session finalizer sink."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from kb_mcp.events.identity import sink_receipt
from kb_mcp.config import inbox_dir, projects_dir, safe_resolve
from kb_mcp.tools.save import _resolve_or_error, kb_draft, kb_session


def _receipt_exists(directory: Path, receipt: str) -> bool:
    for path in directory.glob("*.md"):
        try:
            if f"sink_receipt: {receipt}" in path.read_text(encoding="utf-8"):
                return True
        except OSError:
            continue
    return False


def finalize_session(row: sqlite3.Row) -> str:
    """Materialize a session aggregate into an immutable session note."""
    receipt = sink_receipt("session_finalizer", row["logical_key"], int(row["aggregate_version"]))
    state = json.loads(row["aggregate_state_json"])
    summary = row["summary"] or "Session ended"
    content = row["content_excerpt"] or state.get("excerpt") or "Session finalized by hook pipeline."
    cwd = row["cwd"]
    repo = row["repo"]
    project = row["project"]

    if not project and cwd:
        try:
            project, repo = _resolve_or_error(None, cwd, repo)
        except ValueError:
            project = None
    if project:
        target_dir = safe_resolve(projects_dir(), project, "session-log")
        if _receipt_exists(target_dir, receipt):
            return receipt
        kb_session(
            summary=summary[:200],
            content=content,
            ai_tool=row["source_tool"],
            ai_client=row["source_client"],
            project=project,
            cwd=cwd,
            repo=repo,
            tags=["hook", "session-log"],
            extra_fields={"sink_receipt": receipt},
        )
        return receipt
    if _receipt_exists(inbox_dir(), receipt):
        return receipt
    kb_draft(
        slug="degraded-session-finalizer",
        summary=summary[:200],
        content=content,
        ai_tool=row["source_tool"],
        ai_client=row["source_client"],
        cwd=cwd,
        repo=repo,
        tags=["degraded", "session-finalizer"],
        extra_fields={"sink_receipt": receipt},
    )
    return receipt
