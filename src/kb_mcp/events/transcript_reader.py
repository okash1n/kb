"""Transcript excerpt helpers."""

from __future__ import annotations

import json
from pathlib import Path


def read_transcript_excerpt(path: str | None, *, line_limit: int = 80, char_limit: int = 4000) -> str:
    """Read a bounded excerpt from a transcript file."""
    if not path:
        return ""
    transcript = Path(path)
    if not transcript.exists():
        return ""
    try:
        text = transcript.read_text(encoding="utf-8")
    except OSError:
        return ""
    jsonl_excerpt = _read_jsonl_message_excerpt(text, char_limit=char_limit)
    if jsonl_excerpt:
        return jsonl_excerpt
    lines = text.splitlines()
    excerpt = "\n".join(lines[-line_limit:])
    if len(excerpt) > char_limit:
        excerpt = excerpt[-char_limit:]
    return excerpt


def _read_jsonl_message_excerpt(text: str, *, char_limit: int) -> str:
    """Extract a readable excerpt from Codex-style session JSONL logs."""
    messages: list[str] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            return ""
        extracted = _extract_message_text(record)
        if extracted:
            messages.append(extracted)
    if not messages:
        return ""
    deduped: list[str] = []
    for message in messages:
        if not deduped or deduped[-1] != message:
            deduped.append(message)
    excerpt = "\n\n".join(deduped[-6:])
    if len(excerpt) > char_limit:
        excerpt = excerpt[-char_limit:]
    return excerpt


def _extract_message_text(record: dict) -> str:
    """Return human-readable text from one JSONL event, if any."""
    payload = record.get("payload")
    if not isinstance(payload, dict):
        return ""

    event_type = record.get("type")
    if event_type == "event_msg":
        message_type = payload.get("type")
        if message_type in {"agent_message", "user_message"}:
            message = payload.get("message")
            return str(message).strip() if message else ""

    if event_type == "response_item" and payload.get("type") == "message":
        content = payload.get("content")
        if not isinstance(content, list):
            return ""
        parts: list[str] = []
        for item in content:
            if not isinstance(item, dict):
                continue
            if item.get("type") == "output_text" and item.get("text"):
                parts.append(str(item["text"]).strip())
        return "\n".join(part for part in parts if part).strip()

    return ""
