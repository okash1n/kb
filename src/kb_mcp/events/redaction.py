"""Payload redaction for hook events."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

REDACT_KEYS = {
    "content",
    "transcript",
    "transcript_text",
    "messages",
    "authorization",
    "api_key",
    "token",
}


def _shorten_text(text: str, limit: int = 400) -> str:
    if len(text) <= limit:
        return text
    return f"{text[:limit]}…"


def redact_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return a redacted copy of a payload."""
    result: dict[str, Any] = {}
    for key, value in payload.items():
        if key in REDACT_KEYS:
            if isinstance(value, str) and value:
                result[key] = {"redacted": True, "excerpt": _shorten_text(value)}
            elif value:
                result[key] = {"redacted": True}
            else:
                result[key] = value
            continue
        if isinstance(value, Mapping):
            result[key] = redact_payload(value)
            continue
        if isinstance(value, list):
            redacted_items = []
            for item in value[:5]:
                if isinstance(item, Mapping):
                    redacted_items.append(redact_payload(item))
                else:
                    redacted_items.append(item)
            if len(value) > 5:
                redacted_items.append({"truncated_items": len(value) - 5})
            result[key] = redacted_items
            continue
        result[key] = value
    return result
