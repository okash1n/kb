"""Copilot hook payload normalization helpers."""

from __future__ import annotations

from typing import Any


def normalize_copilot_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Normalize Copilot hook payload shape."""
    normalized = dict(payload)
    if "session_id" not in normalized and "conversation_id" in normalized:
        normalized["session_id"] = normalized["conversation_id"]
    if "summary" not in normalized and "message" in normalized:
        normalized["summary"] = normalized["message"]
    return normalized
