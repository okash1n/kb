"""Build session-promotion plans from anchors and final checkpoints."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from kb_mcp.config import runtime_events_dir
from kb_mcp.events.identity import sink_receipt
from kb_mcp.events.schema import schema_locked_connection
from kb_mcp.note import parse_frontmatter


def write_promotion_plan(row: sqlite3.Row) -> str:
    """Persist a promotion plan artifact for later materialization."""
    plan = _build_plan(row)
    plans_dir = runtime_events_dir() / "promotions"
    plans_dir.mkdir(parents=True, exist_ok=True)
    safe_name = row["logical_key"].replace(":", "__")
    path = plans_dir / f"{safe_name}.json"
    path.write_text(json.dumps(plan, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return sink_receipt("promotion_planner", row["logical_key"], int(row["aggregate_version"]))


def _build_plan(row: sqlite3.Row) -> dict[str, object]:
    if row["aggregate_type"] == "tool":
        return _build_rich_plan(row)
    return _build_thin_plan(row)


def _build_rich_plan(row: sqlite3.Row) -> dict[str, object]:
    payload = _latest_event_payload(row["logical_key"])
    note_path = Path(payload["saved_note_path"])
    note_text = note_path.read_text(encoding="utf-8")
    frontmatter = parse_frontmatter(note_text) or {}
    project = _project_from_saved_path(note_path) or row["project"]
    ai_tool = frontmatter.get("ai_tool") or "codex"
    ai_client = frontmatter.get("ai_client")
    checkpoints = _recent_checkpoints(
        project=project,
        cwd=row["cwd"],
        source_tool=ai_tool,
        limit=5,
    )
    summary = str(frontmatter.get("summary") or row["summary"] or "Promoted session")
    content = _render_session_content(
        title=f"Anchor: {summary}",
        checkpoints=checkpoints,
        footer=f"anchor_note: {payload['saved_note_type']} {frontmatter.get('id')}",
    )
    return {
        "density": "rich",
        "summary": summary,
        "content": content,
        "project": project,
        "cwd": row["cwd"],
        "repo": frontmatter.get("repo") or row["repo"],
        "ai_tool": ai_tool,
        "ai_client": ai_client,
        "related": [frontmatter["id"]] if frontmatter.get("id") else [],
        "tags": ["promotion", "session-log", "rich", str(payload["saved_note_type"])],
        "promotion_key": f"rich:{payload['saved_note_type']}:{frontmatter.get('id') or row['logical_key']}",
    }


def _build_thin_plan(row: sqlite3.Row) -> dict[str, object]:
    state = json.loads(row["aggregate_state_json"])
    partition_key = str(state.get("checkpoint_partition_key") or row["logical_key"])
    ordinal = int(state.get("checkpoint_ordinal") or row["aggregate_version"])
    checkpoints = _partition_checkpoints(partition_key=partition_key, upto_ordinal=ordinal, limit=5)
    summary = str(row["summary"] or "Thin session promotion")
    content = _render_session_content(
        title=f"Thin session window: {summary}",
        checkpoints=checkpoints,
        footer=f"bucket_end_ordinal: {ordinal}",
    )
    return {
        "density": "thin",
        "summary": summary,
        "content": content,
        "project": row["project"],
        "cwd": row["cwd"],
        "repo": row["repo"],
        "ai_tool": row["source_tool"],
        "ai_client": row["source_client"],
        "related": [],
        "tags": ["promotion", "session-log", "thin"],
        "promotion_key": f"thin:{partition_key}:{ordinal}",
    }


def _render_session_content(*, title: str, checkpoints: list[sqlite3.Row], footer: str) -> str:
    lines = [title, ""]
    if checkpoints:
        lines.append("Context:")
        for checkpoint in checkpoints:
            summary = checkpoint["summary"] or "(no summary)"
            excerpt = checkpoint["content_excerpt"] or ""
            lines.append(f"- {summary}")
            if excerpt:
                lines.append(f"  {excerpt}")
    else:
        lines.append("Context: checkpoint がまだ十分に集まってへんかった。")
    lines.extend(["", footer])
    return "\n".join(lines)


def _latest_event_payload(logical_key: str) -> dict[str, object]:
    with schema_locked_connection() as conn:
        row = conn.execute(
            """
            SELECT raw_payload_json
            FROM events
            WHERE logical_key=?
            ORDER BY rowid DESC
            LIMIT 1
            """,
            (logical_key,),
        ).fetchone()
    return json.loads(row["raw_payload_json"]) if row else {}


def _recent_checkpoints(*, project: str | None, cwd: str | None, source_tool: str | None, limit: int) -> list[sqlite3.Row]:
    with schema_locked_connection() as conn:
        return conn.execute(
            """
            SELECT *
            FROM logical_events
            WHERE aggregate_type='compact'
              AND (? IS NULL OR project=?)
              AND (? IS NULL OR cwd=?)
              AND (? IS NULL OR source_tool=?)
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (project, project, cwd, cwd, source_tool, source_tool, limit),
        ).fetchall()[::-1]


def _partition_checkpoints(*, partition_key: str, upto_ordinal: int, limit: int) -> list[sqlite3.Row]:
    pattern = f"compact:{partition_key}:%"
    with schema_locked_connection() as conn:
        return conn.execute(
            """
            SELECT *
            FROM logical_events
            WHERE aggregate_type='compact'
              AND logical_key LIKE ?
              AND CAST(json_extract(aggregate_state_json, '$.checkpoint_ordinal') AS INTEGER) <= ?
            ORDER BY CAST(json_extract(aggregate_state_json, '$.checkpoint_ordinal') AS INTEGER) DESC
            LIMIT ?
            """,
            (pattern, upto_ordinal, limit),
        ).fetchall()[::-1]


def _project_from_saved_path(path: Path) -> str | None:
    parts = list(path.parts)
    if "projects" not in parts:
        return None
    idx = parts.index("projects")
    if idx + 1 >= len(parts):
        return None
    return parts[idx + 1]
