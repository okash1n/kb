"""Build promotion plans for session and review-led materialization."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from kb_mcp.config import runtime_events_dir
from kb_mcp.events.identity import sink_receipt
from kb_mcp.events.schema import schema_locked_connection
from kb_mcp.events.store import EventStore
from kb_mcp.note import parse_frontmatter


def write_promotion_plan(row: sqlite3.Row) -> str:
    """Persist a promotion plan artifact for later materialization."""
    plan = _build_plan(row)
    path = _plan_path(row)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(plan, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return sink_receipt("promotion_planner", row["logical_key"], int(row["aggregate_version"]))


def _build_plan(row: sqlite3.Row) -> dict[str, object]:
    if row["aggregate_type"] == "review_materialization":
        return _build_materialization_plan(row)
    if row["aggregate_type"] == "tool":
        return _build_rich_plan(row)
    return _build_thin_plan(row)


def _build_materialization_plan(row: sqlite3.Row) -> dict[str, object]:
    state = json.loads(row["aggregate_state_json"])
    store = EventStore()
    candidate = store.get_promotion_candidate(str(state["candidate_key"]))
    if candidate is None:
        raise ValueError(f"missing promotion candidate: {state['candidate_key']}")
    review = store.get_candidate_review(str(state["candidate_key"]), int(state["review_seq"]))
    if review is None:
        raise ValueError(f"missing candidate review: {state['candidate_key']}")
    judge_run = store.get_judge_run_by_key(str(state["judge_run_key"]))
    payload = json.loads(candidate["payload_json"])
    window = dict(payload.get("window") or {})
    decision = dict(payload.get("decision") or {})
    checkpoints = list(window.get("checkpoints") or [])
    effective_label = str(state["effective_label"])
    summary = _materialization_summary(effective_label, checkpoints)
    ai_tool, ai_client = _materialization_actor(judge_run, checkpoints)
    plan = {
        "note_type": _note_type_for_label(effective_label),
        "summary": summary,
        "slug": summary,
        "content": _render_materialization_content(
            effective_label=effective_label,
            checkpoints=checkpoints,
            reasons=json.loads(candidate["reasons_json"]),
            review=review,
        ),
        "project": _window_value(checkpoints, "project") or row["project"],
        "cwd": row["cwd"],
        "repo": _window_value(checkpoints, "repo") or row["repo"],
        "ai_tool": ai_tool,
        "ai_client": ai_client,
        "related": [],
        "tags": ["promotion", effective_label],
        "promotion_key": (
            _session_thin_promotion_key(window)
            if effective_label == "session_thin"
            else f"materialize:{state['candidate_key']}:{effective_label}:{int(state['review_seq'])}"
        ),
        "candidate_key": str(state["candidate_key"]),
        "materialization_key": str(state["materialization_key"]),
        "review_seq": int(state["review_seq"]),
        "effective_label": effective_label,
        "status": "accepted" if effective_label == "adr" else None,
    }
    supersede_target = decision.get("supersede_target")
    if effective_label == "adr" and isinstance(supersede_target, dict):
        plan["supersede_target"] = {
            "note_id": supersede_target.get("note_id"),
            "note_path": supersede_target.get("note_path"),
            "materialization_key": supersede_target.get("materialization_key"),
        }
    return plan


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
        "note_type": "session-log",
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
        "note_type": "session-log",
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
        lines.append("Context: checkpoint がまだ十分に集まっていなかった。")
    lines.extend(["", footer])
    return "\n".join(lines)


def _render_materialization_content(
    *,
    effective_label: str,
    checkpoints: list[dict[str, object]],
    reasons: list[str],
    review: sqlite3.Row,
) -> str:
    lines = [f"{effective_label} materialization", "", "Window context:"]
    if checkpoints:
        for checkpoint in checkpoints:
            summary = str(checkpoint.get("summary") or "(no summary)")
            excerpt = str(checkpoint.get("content_excerpt") or "")
            lines.append(f"- {summary}")
            if excerpt:
                lines.append(f"  {excerpt}")
    else:
        lines.append("- checkpoint context unavailable")
    if reasons:
        lines.extend(["", f"reasons: {', '.join(reasons)}"])
    lines.extend(["", f"review_verdict: {review['human_verdict']}"])
    if review["human_label"]:
        lines.append(f"review_label: {review['human_label']}")
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


def _plan_path(row: sqlite3.Row) -> Path:
    prefix = "materialize__" if row["aggregate_type"] == "review_materialization" else "session__"
    safe_name = row["logical_key"].replace(":", "__")
    return runtime_events_dir() / "promotions" / f"{prefix}{safe_name}__v{int(row['aggregate_version'])}.json"


def _note_type_for_label(label: str) -> str:
    mapping = {
        "adr": "adr",
        "gap": "gap",
        "knowledge": "knowledge",
        "session_thin": "session-log",
    }
    return mapping[label]


def _materialization_summary(label: str, checkpoints: list[dict[str, object]]) -> str:
    if checkpoints:
        summary = str(checkpoints[-1].get("summary") or "").strip()
        if summary:
            return summary[:200]
    defaults = {
        "adr": "Promoted ADR",
        "gap": "Promoted gap",
        "knowledge": "Promoted knowledge",
        "session_thin": "Promoted thin session",
    }
    return defaults[label]


def _window_value(checkpoints: list[dict[str, object]], key: str) -> str | None:
    for checkpoint in reversed(checkpoints):
        value = checkpoint.get(key)
        if value:
            return str(value)
    return None


def _session_thin_promotion_key(window: dict[str, Any]) -> str:
    return f"thin:{window['partition_key']}:{window['end_ordinal']}"


def _materialization_actor(
    judge_run: sqlite3.Row | None,
    checkpoints: list[dict[str, object]],
) -> tuple[str, str | None]:
    if checkpoints:
        source = _checkpoint_source(str(checkpoints[-1].get("logical_key") or ""))
        if source is not None and source[0] in {"claude", "copilot", "codex"}:
            return source
    model_hint = str(judge_run["model_hint"]) if judge_run is not None and judge_run["model_hint"] else "codex"
    client_map = {
        "claude": "claude-code",
        "copilot": "copilot-cli",
        "codex": "codex-cli",
    }
    if model_hint not in client_map:
        return "codex", "codex-cli"
    return model_hint, client_map[model_hint]


def _checkpoint_source(logical_key: str) -> tuple[str, str | None] | None:
    if not logical_key:
        return None
    with schema_locked_connection() as conn:
        row = conn.execute(
            """
            SELECT source_tool, source_client
            FROM logical_events
            WHERE logical_key=?
            LIMIT 1
            """,
            (logical_key,),
        ).fetchone()
    if row is None:
        return None
    return str(row["source_tool"]), str(row["source_client"]) if row["source_client"] else None
