"""Incident writer sink for tool failures and agent errors."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from kb_mcp.config import inbox_dir, projects_dir, safe_resolve
from kb_mcp.events.identity import sink_receipt
from kb_mcp.tools.save import kb_draft


def _receipt_exists(directory: Path, receipt: str) -> bool:
    for path in directory.glob("*.md"):
        try:
            if f"sink_receipt: {receipt}" in path.read_text(encoding="utf-8"):
                return True
        except OSError:
            continue
    return False


def write_incident(row: sqlite3.Row) -> str:
    """Persist an incident draft for a failed tool or agent error."""
    receipt = sink_receipt("incident_writer", row["logical_key"], int(row["aggregate_version"]))
    state = json.loads(row["aggregate_state_json"])
    event_name = state.get("event_name", row["aggregate_type"])
    summary = row["summary"] or f"{event_name} detected"
    content_lines = [
        f"- aggregate: {row['aggregate_type']}",
        f"- logical_key: {row['logical_key']}",
        f"- source_tool: {row['source_tool']}",
        f"- source_client: {row['source_client']}",
    ]
    if row["cwd"]:
        content_lines.append(f"- cwd: {row['cwd']}")
    if row["content_excerpt"]:
        content_lines.append("")
        content_lines.append("## Excerpt")
        content_lines.append("")
        content_lines.append(row["content_excerpt"])
    directory = inbox_dir()
    if row["project"]:
        directory = safe_resolve(projects_dir(), row["project"], "draft")
    if _receipt_exists(directory, receipt):
        return receipt
    kb_draft(
        slug=f"incident-{row['aggregate_type']}",
        summary=summary[:200],
        content="\n".join(content_lines),
        ai_tool=row["source_tool"],
        ai_client=row["source_client"],
        project=row["project"],
        cwd=row["cwd"],
        repo=row["repo"],
        tags=["incident", row["aggregate_type"]],
        extra_fields={"sink_receipt": receipt},
    )
    return receipt
