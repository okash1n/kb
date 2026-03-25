"""Materialize planned session promotions into immutable session notes."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from kb_mcp.config import projects_dir, runtime_events_dir, safe_resolve
from kb_mcp.events.identity import sink_receipt
from kb_mcp.events.request_context import REQUEST_CONTEXT
from kb_mcp.note import parse_frontmatter
from kb_mcp.tools.save import kb_session


def apply_promotion(row: sqlite3.Row) -> str:
    """Create a session-log note from a persisted promotion plan."""
    receipt = sink_receipt("promotion_applier", row["logical_key"], int(row["aggregate_version"]))
    if _receipt_exists(row["project"], receipt):
        return receipt

    plan = _load_plan(row["logical_key"])
    context: dict[str, str] = {}
    token = REQUEST_CONTEXT.set(context)
    try:
        record = _load_record(str(plan["promotion_key"]))
        extra_fields = {
            "density": str(plan["density"]),
            "promotion_key": str(plan["promotion_key"]),
            "promotion_version": str(int(record.get("promotion_version", 0)) + 1),
            "sink_receipt": receipt,
        }
        previous_id = record.get("note_id")
        if previous_id:
            extra_fields["supersedes"] = str(previous_id)
        kb_session(
            summary=str(plan["summary"])[:200],
            content=str(plan["content"]),
            ai_tool=str(plan["ai_tool"]),
            ai_client=str(plan["ai_client"]) if plan.get("ai_client") else None,
            project=str(plan["project"]),
            cwd=str(plan["cwd"]) if plan.get("cwd") else None,
            repo=str(plan["repo"]) if plan.get("repo") else None,
            tags=list(plan.get("tags") or []),
            related=list(plan.get("related") or []),
            extra_fields=extra_fields,
        )
        _write_record(
            str(plan["promotion_key"]),
            {
                "logical_key": row["logical_key"],
                "aggregate_version": int(row["aggregate_version"]),
                "note_id": context.get("saved_note_id"),
                "note_path": context.get("saved_note_path"),
                "promotion_version": extra_fields["promotion_version"],
                "density": plan["density"],
            },
        )
        return receipt
    finally:
        REQUEST_CONTEXT.reset(token)


def _load_plan(logical_key: str) -> dict[str, object]:
    safe_name = logical_key.replace(":", "__")
    path = runtime_events_dir() / "promotions" / f"{safe_name}.json"
    return json.loads(path.read_text(encoding="utf-8"))


def _records_dir() -> Path:
    path = runtime_events_dir() / "promotion-records"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _record_path(promotion_key: str) -> Path:
    safe_name = promotion_key.replace(":", "__")
    return _records_dir() / f"{safe_name}.json"


def _load_record(promotion_key: str) -> dict[str, object]:
    path = _record_path(promotion_key)
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _write_record(promotion_key: str, payload: dict[str, object]) -> None:
    _record_path(promotion_key).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def _receipt_exists(project: str | None, receipt: str) -> bool:
    if not project:
        return False
    directory = safe_resolve(projects_dir(), project, "session-log")
    for path in directory.glob("*.md"):
        try:
            frontmatter = parse_frontmatter(path.read_text(encoding="utf-8")) or {}
        except OSError:
            continue
        if frontmatter.get("sink_receipt") == receipt:
            return True
    return False
