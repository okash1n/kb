"""Claude hook payload normalization helpers."""

from __future__ import annotations

from typing import Any


def normalize_claude_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Normalize Claude hook payload shape."""
    normalized = dict(payload)
    if "session_id" not in normalized and "sessionId" in normalized:
        normalized["session_id"] = normalized["sessionId"]
    if "summary" not in normalized and "last_assistant_message" in normalized:
        normalized["summary"] = normalized["last_assistant_message"]
    return normalized
