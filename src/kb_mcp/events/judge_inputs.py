"""Deterministic window reconstruction for judge inputs."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
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
TOOL_EVENT_LOOKBACK_SECONDS = 20 * 60
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
    carry_chain_index: int
    carry_chain_terminal: bool


def load_partition_checkpoints(partition_key: str) -> list[CheckpointInput]:
    """Load compact logical events for one partition in ordinal order."""
    pattern = f"compact:{partition_key}:%"
    with schema_locked_connection() as conn:
        rows = conn.execute(
            """
            WITH latest_payload_events AS (
              SELECT logical_key, MAX(rowid) AS latest_rowid
              FROM events
              GROUP BY logical_key
            ),
            first_occurrence_events AS (
              SELECT logical_key, MIN(rowid) AS first_rowid
              FROM events
              GROUP BY logical_key
            )
            SELECT
              le.logical_key,
              le.summary,
              le.content_excerpt,
              le.project,
              le.repo,
              le.session_id,
              le.transcript_path,
              le.aggregate_state_json,
              latest.raw_payload_json,
              first_event.occurred_at
            FROM logical_events le
            LEFT JOIN latest_payload_events latest_idx ON latest_idx.logical_key = le.logical_key
            LEFT JOIN events latest ON latest.rowid = latest_idx.latest_rowid
            LEFT JOIN first_occurrence_events first_idx ON first_idx.logical_key = le.logical_key
            LEFT JOIN events first_event ON first_event.rowid = first_idx.first_rowid
            WHERE le.aggregate_type='compact' AND le.logical_key LIKE ?
            ORDER BY CAST(json_extract(le.aggregate_state_json, '$.checkpoint_ordinal') AS INTEGER) ASC,
                     first_idx.first_rowid ASC
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
    carry_chain_count = 0
    continuing_carry = False
    for checkpoint in checkpoints:
        if not current:
            current = [checkpoint]
            continue
        if checkpoint.ordinal != current[-1].ordinal + 1:
            _record_ordinal_gap_observation(
                partition_key=partition_key,
                previous_ordinal=current[-1].ordinal,
                current_ordinal=checkpoint.ordinal,
            )
            windows.append(
                _build_window(
                    partition_key,
                    window_index,
                    current,
                    carry_forward=False,
                    carry_chain_index=(carry_chain_count + 1) if continuing_carry else 0,
                    carry_chain_terminal=False,
                )
            )
            window_index += 1
            current = [checkpoint]
            carry_chain_count = 0
            continuing_carry = False
            continue
        if _is_window_boundary(current[-1], checkpoint, idle_seconds=idle_seconds):
            windows.append(
                _build_window(
                    partition_key,
                    window_index,
                    current,
                    carry_forward=False,
                    carry_chain_index=(carry_chain_count + 1) if continuing_carry else 0,
                    carry_chain_terminal=False,
                )
            )
            window_index += 1
            current = [checkpoint]
            carry_chain_count = 0
            continuing_carry = False
        else:
            current.append(checkpoint)
        if _is_anchor_checkpoint(current[-1]):
            windows.append(
                _build_window(
                    partition_key,
                    window_index,
                    current,
                    carry_forward=False,
                    carry_chain_index=(carry_chain_count + 1) if continuing_carry else 0,
                    carry_chain_terminal=False,
                )
            )
            window_index += 1
            current = []
            carry_chain_count = 0
            continuing_carry = False
            continue
        if len(current) == checkpoint_limit and not _has_anchor(current):
            next_chain_index = carry_chain_count + 1
            carry_chain_terminal = next_chain_index >= 3
            windows.append(
                _build_window(
                    partition_key,
                    window_index,
                    current,
                    carry_forward=not carry_chain_terminal,
                    carry_chain_index=next_chain_index,
                    carry_chain_terminal=carry_chain_terminal,
                )
            )
            window_index += 1
            current = []
            if carry_chain_terminal:
                carry_chain_count = 0
                continuing_carry = False
            else:
                carry_chain_count = next_chain_index
                continuing_carry = True
    if current:
        windows.append(
            _build_window(
                partition_key,
                window_index,
                current,
                carry_forward=False,
                carry_chain_index=(carry_chain_count + 1) if continuing_carry else 0,
                carry_chain_terminal=False,
            )
        )
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
        "carry_chain_index": window.carry_chain_index,
        "carry_chain_terminal": window.carry_chain_terminal,
        "window_id": _window_id(window.partition_key, window.start_ordinal, window.end_ordinal),
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
                "anchor_labels": _detect_anchor_labels(item.summary, item.content_excerpt),
            }
            for item in window.checkpoints
        ],
        "knowledge_signals": knowledge,
        "anchor_matches": sorted({label for item in window.checkpoints for label in _detect_anchor_labels(item.summary, item.content_excerpt)}),
    }


def detect_topic_shift(summary: str | None, content: str | None) -> bool:
    """Return True when deterministic topic-shift hints are found."""
    parts = [part.strip() for part in [summary or "", content or ""] if part and part.strip()]
    fragments = [
        _strip_topic_preface(fragment)
        for part in parts
        for fragment in _split_sentences(part)
        if _strip_topic_preface(fragment)
    ]
    return any(fragment.startswith(pattern) for fragment in fragments for pattern in TOPIC_SHIFT_PATTERNS)


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
    carry_chain_index: int,
    carry_chain_terminal: bool,
) -> WindowInput:
    return WindowInput(
        partition_key=partition_key,
        window_index=window_index,
        start_ordinal=checkpoints[0].ordinal,
        end_ordinal=checkpoints[-1].ordinal,
        checkpoints=list(checkpoints),
        carry_forward=carry_forward,
        carry_chain_index=carry_chain_index,
        carry_chain_terminal=carry_chain_terminal,
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


def _is_anchor_checkpoint(checkpoint: CheckpointInput) -> bool:
    text = "\n".join(part for part in [checkpoint.summary, checkpoint.content_excerpt] if part)
    return any(token in text for token in ANCHOR_PATTERNS)


def _load_related_tool_events(window: WindowInput) -> list[dict[str, Any]]:
    first = window.checkpoints[0]
    last = window.checkpoints[-1]
    first_dt = _load_previous_checkpoint_dt(window.partition_key, window.start_ordinal)
    if first_dt is None:
        current_first_dt = _parse_dt(first.occurred_at)
        if current_first_dt is not None:
            first_dt = current_first_dt - timedelta(seconds=TOOL_EVENT_LOOKBACK_SECONDS)
    last_dt = _parse_dt(last.occurred_at)
    match_without_session = first.session_id is None and window.partition_key.startswith("standalone:")
    if first.session_id is None and not match_without_session:
        return []
    with schema_locked_connection() as conn:
        if match_without_session:
            rows = conn.execute(
                """
                SELECT event_name, occurred_at, raw_payload_json
                FROM events
                WHERE aggregate_type='tool'
                  AND source_layer='server_middleware'
                  AND session_id IS NULL
                  AND (? IS NULL OR project=?)
                  AND (? IS NULL OR repo=?)
                  AND (? IS NULL OR cwd=?)
                ORDER BY occurred_at ASC, rowid ASC
                """,
                (
                    first.project,
                    first.project,
                    first.repo,
                    first.repo,
                    first.raw_payload.get("cwd") or first.aggregate_state.get("cwd"),
                    first.raw_payload.get("cwd") or first.aggregate_state.get("cwd"),
                ),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT event_name, occurred_at, raw_payload_json
                FROM events
                WHERE aggregate_type='tool'
                  AND source_layer='server_middleware'
                  AND session_id=?
                  AND (? IS NULL OR project=?)
                  AND (? IS NULL OR repo=?)
                  AND (? IS NULL OR cwd=?)
                ORDER BY occurred_at ASC, rowid ASC
                """,
                (
                    first.session_id,
                    first.project,
                    first.project,
                    first.repo,
                    first.repo,
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
            ORDER BY rowid ASC
            LIMIT 1
            """,
            (logical_key,),
        ).fetchone()
    if not row:
        return None
    return _parse_dt(row["occurred_at"])


def _record_ordinal_gap_observation(*, partition_key: str, previous_ordinal: int, current_ordinal: int) -> None:
    key = _sha256_hex(f"{partition_key}\x1f{previous_ordinal}\x1f{current_ordinal}")
    with schema_locked_connection() as conn:
        conn.execute(
            """
            INSERT INTO runtime_observations(observation_key, severity, message, details_json, expires_at, updated_at)
            VALUES (?, 'warning', ?, ?, NULL, CURRENT_TIMESTAMP)
            ON CONFLICT(observation_key) DO UPDATE SET
              severity=excluded.severity,
              message=excluded.message,
              details_json=excluded.details_json,
              updated_at=excluded.updated_at
            """,
            (
                f"judge_inputs:ordinal_gap:{key}",
                "checkpoint ordinal gap detected while building judge window",
                json.dumps(
                    {
                        "partition_key": partition_key,
                        "previous_ordinal": previous_ordinal,
                        "current_ordinal": current_ordinal,
                    },
                    ensure_ascii=False,
                ),
            ),
        )
        conn.commit()


def _detect_anchor_labels(summary: str | None, content: str | None) -> list[str]:
    text = "\n".join(part for part in [summary or "", content or ""] if part)
    labels: list[str] = []
    if any(token in text for token in ("これでいこう", "それでいいよ", "その方針で", "それでOK", "その方針でOK")):
        labels.append("adr")
    if any(token in text for token in ("違う", "ストップして", "止めて", "待って")):
        labels.append("gap")
    return labels


def _window_id(partition_key: str, start_ordinal: int, end_ordinal: int) -> str:
    return _sha256_hex(f"{partition_key}\x1f{start_ordinal}\x1f{end_ordinal}")


def _sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _split_sentences(text: str) -> list[str]:
    normalized = text
    for delimiter in ("\n", "。", "！", "？", ".", "!", "?"):
        normalized = normalized.replace(delimiter, "\n")
    return normalized.splitlines()


def _strip_topic_preface(text: str) -> str:
    stripped = text.strip()
    for delimiter in ("、", ","):
        if delimiter in stripped:
            prefix, remainder = stripped.split(delimiter, 1)
            if len(prefix.strip()) <= 6:
                return remainder.strip()
    return stripped


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
