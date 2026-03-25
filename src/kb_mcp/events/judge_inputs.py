"""Deterministic window reconstruction for judge inputs."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from kb_mcp.events.schema import schema_locked_connection

TOPIC_SHIFT_PATTERNS = (
    "でも",
    "やっぱり",
    "じゃあ",
    "一旦",
    "話を戻す",
)
CAUSE_PATTERNS = ("原因", "根本原因", "because", "failed because", "failure caused by")
CONSTRAINT_PATTERNS = ("必要", "must", "required", "only", "制約", "前提")
FACT_PATTERNS = ("確認できた", "判明", "仕様", "挙動", "registered", "enabled")
COMPARISON_PATTERNS = ("比較", "方針", "採用", "これでいこう", "それでOK")
ANCHOR_PATTERNS = (
    "これでいこう",
    "それでいいよ",
    "その方針で",
    "それでOK",
    "その方針でOK",
    "違う",
    "ストップして",
    "止めて",
    "待って",
)


@dataclass(slots=True)
class CheckpointInput:
    logical_key: str
    partition_key: str
    ordinal: int
    occurred_at: str | None
    summary: str
    content_excerpt: str
    project: str | None
    repo: str | None
    session_id: str | None
    transcript_path: str | None
    aggregate_state: dict[str, Any]
    raw_payload: dict[str, Any]


@dataclass(slots=True)
class WindowInput:
    partition_key: str
    window_index: int
    start_ordinal: int
    end_ordinal: int
    checkpoints: list[CheckpointInput]
    carry_forward: bool


def load_partition_checkpoints(partition_key: str) -> list[CheckpointInput]:
    """Load compact logical events for one partition in ordinal order."""
    pattern = f"compact:{partition_key}:%"
    with schema_locked_connection() as conn:
        rows = conn.execute(
            """
            SELECT
              le.logical_key,
              le.summary,
              le.content_excerpt,
              le.project,
              le.repo,
              le.session_id,
              le.transcript_path,
              le.aggregate_state_json,
              ev.raw_payload_json,
              ev.occurred_at
            FROM logical_events le
            LEFT JOIN events ev ON ev.logical_key = le.logical_key
            WHERE le.aggregate_type='compact' AND le.logical_key LIKE ?
            ORDER BY CAST(json_extract(le.aggregate_state_json, '$.checkpoint_ordinal') AS INTEGER) ASC,
                     ev.rowid ASC
            """,
            (pattern,),
        ).fetchall()
    checkpoints: list[CheckpointInput] = []
    seen: set[str] = set()
    for row in rows:
        if row["logical_key"] in seen:
            continue
        seen.add(row["logical_key"])
        state = json.loads(row["aggregate_state_json"])
        raw_payload = json.loads(row["raw_payload_json"]) if row["raw_payload_json"] else {}
        checkpoints.append(
            CheckpointInput(
                logical_key=row["logical_key"],
                partition_key=partition_key,
                ordinal=int(state.get("checkpoint_ordinal") or 0),
                occurred_at=row["occurred_at"],
                summary=row["summary"] or "",
                content_excerpt=row["content_excerpt"] or "",
                project=row["project"],
                repo=row["repo"],
                session_id=row["session_id"],
                transcript_path=row["transcript_path"],
                aggregate_state=state,
                raw_payload=raw_payload,
            )
        )
    return checkpoints


def build_windows(
    partition_key: str,
    *,
    checkpoint_limit: int = 10,
    idle_seconds: int = 20 * 60,
) -> list[WindowInput]:
    """Group checkpoints into deterministic windows for judge input."""
    checkpoints = load_partition_checkpoints(partition_key)
    windows: list[WindowInput] = []
    current: list[CheckpointInput] = []
    window_index = 1
    carry_forward = False
    for checkpoint in checkpoints:
        if not current:
            current = [checkpoint]
            continue
        if _is_window_boundary(current[-1], checkpoint, idle_seconds=idle_seconds):
            windows.append(_build_window(partition_key, window_index, current, carry_forward))
            window_index += 1
            current = [checkpoint]
            carry_forward = False
            continue
        current.append(checkpoint)
        if len(current) == checkpoint_limit and not _has_anchor(current):
            carry_forward = True
            windows.append(_build_window(partition_key, window_index, current, carry_forward))
            window_index += 1
            current = []
    if current:
        windows.append(_build_window(partition_key, window_index, current, carry_forward))
    return windows


def build_window_payload(window: WindowInput) -> dict[str, Any]:
    """Return normalized judge input payload for one window."""
    knowledge = detect_window_knowledge(window)
    return {
        "partition_key": window.partition_key,
        "window_index": window.window_index,
        "start_ordinal": window.start_ordinal,
        "end_ordinal": window.end_ordinal,
        "carry_forward": window.carry_forward,
        "checkpoints": [
            {
                "logical_key": item.logical_key,
                "ordinal": item.ordinal,
                "occurred_at": item.occurred_at,
                "summary": item.summary,
                "content_excerpt": item.content_excerpt,
                "project": item.project,
                "repo": item.repo,
                "session_id": item.session_id,
                "transcript_path": item.transcript_path,
                "topic_shift_candidate": detect_topic_shift(item.summary, item.content_excerpt),
            }
            for item in window.checkpoints
        ],
        "knowledge_signals": knowledge,
    }


def detect_topic_shift(summary: str | None, content: str | None) -> bool:
    """Return True when deterministic topic-shift hints are found."""
    text = "\n".join(part for part in [summary or "", content or ""] if part).strip()
    if not text:
        return False
    return any(pattern in text for pattern in TOPIC_SHIFT_PATTERNS)


def detect_window_knowledge(window: WindowInput) -> dict[str, bool]:
    """Derive deterministic knowledge helper flags from tool events and text."""
    tool_events = _load_related_tool_events(window)
    texts = "\n".join(
        "\n".join(part for part in [item.summary, item.content_excerpt] if part).strip()
        for item in window.checkpoints
    )
    has_tool_success = any(row["event_name"] == "tool_succeeded" for row in tool_events)
    has_tool_failure = any(row["event_name"] == "tool_failed" for row in tool_events)
    comparison_count = sum(1 for token in COMPARISON_PATTERNS if token in texts)
    return {
        "fact_confirmed": has_tool_success and _contains_any(texts, FACT_PATTERNS),
        "constraint_confirmed": (has_tool_success or has_tool_failure) and _contains_any(texts, CONSTRAINT_PATTERNS),
        "cause_identified": has_tool_failure and _contains_any(texts, CAUSE_PATTERNS),
        "comparison_settled": comparison_count >= 2,
    }


def _build_window(
    partition_key: str,
    window_index: int,
    checkpoints: list[CheckpointInput],
    carry_forward: bool,
) -> WindowInput:
    return WindowInput(
        partition_key=partition_key,
        window_index=window_index,
        start_ordinal=checkpoints[0].ordinal,
        end_ordinal=checkpoints[-1].ordinal,
        checkpoints=list(checkpoints),
        carry_forward=carry_forward,
    )


def _is_window_boundary(previous: CheckpointInput, current: CheckpointInput, *, idle_seconds: int) -> bool:
    if previous.project != current.project:
        return True
    if previous.repo != current.repo:
        return True
    if previous.transcript_path != current.transcript_path:
        return True
    if detect_topic_shift(current.summary, current.content_excerpt):
        return True
    previous_dt = _parse_dt(previous.occurred_at)
    current_dt = _parse_dt(current.occurred_at)
    if previous_dt and current_dt:
        return (current_dt - previous_dt).total_seconds() > idle_seconds
    return False


def _has_anchor(checkpoints: list[CheckpointInput]) -> bool:
    text = "\n".join(
        "\n".join(part for part in [item.summary, item.content_excerpt] if part)
        for item in checkpoints
    )
    return any(token in text for token in ANCHOR_PATTERNS)


def _load_related_tool_events(window: WindowInput) -> list[dict[str, Any]]:
    first = window.checkpoints[0]
    last = window.checkpoints[-1]
    first_dt = _load_previous_checkpoint_dt(window.partition_key, window.start_ordinal)
    last_dt = _parse_dt(last.occurred_at)
    with schema_locked_connection() as conn:
        rows = conn.execute(
            """
            SELECT event_name, occurred_at, raw_payload_json
            FROM events
            WHERE aggregate_type='tool'
              AND source_layer='server_middleware'
              AND (? IS NULL OR session_id=?)
              AND (? IS NULL OR project=?)
              AND (? IS NULL OR cwd=?)
            ORDER BY occurred_at ASC, rowid ASC
            """,
            (
                first.session_id,
                first.session_id,
                first.project,
                first.project,
                first.raw_payload.get("cwd") or first.aggregate_state.get("cwd"),
                first.raw_payload.get("cwd") or first.aggregate_state.get("cwd"),
            ),
        ).fetchall()
    results: list[dict[str, Any]] = []
    for row in rows:
        occurred = _parse_dt(row["occurred_at"])
        if first_dt and occurred and occurred < first_dt:
            continue
        if last_dt and occurred and occurred > last_dt:
            continue
        results.append(
            {
                "event_name": row["event_name"],
                "occurred_at": row["occurred_at"],
                "raw_payload": json.loads(row["raw_payload_json"]) if row["raw_payload_json"] else {},
            }
        )
    return results


def _load_previous_checkpoint_dt(partition_key: str, start_ordinal: int) -> datetime | None:
    if start_ordinal <= 1:
        return None
    logical_key = f"compact:{partition_key}:{start_ordinal - 1}"
    with schema_locked_connection() as conn:
        row = conn.execute(
            """
            SELECT occurred_at
            FROM events
            WHERE logical_key=?
            ORDER BY rowid DESC
            LIMIT 1
            """,
            (logical_key,),
        ).fetchone()
    if not row:
        return None
    return _parse_dt(row["occurred_at"])


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _contains_any(text: str, patterns: tuple[str, ...]) -> bool:
    if not text:
        return False
    return any(pattern in text for pattern in patterns)
