"""Helpers for normalizing flexible MCP input shapes."""

from __future__ import annotations

import json
from collections.abc import Iterable


def normalize_string_list(value: str | Iterable[str] | None) -> list[str] | None:
    """Normalize string or string-list input into a cleaned list.

    Accepts:
    - ``None``
    - a comma-separated string such as ``"a, b"``
    - a JSON-like string such as ``'["a", "b"]'``
    - any iterable of strings
    """
    if value is None:
        return None
    if isinstance(value, str):
        return _normalize_string(value)
    return _normalize_iterable(value)


def _normalize_string(value: str) -> list[str] | None:
    text = value.strip()
    if not text:
        return None
    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            text = text[1:-1]
        else:
            if isinstance(parsed, list):
                return _normalize_iterable(parsed)
    return _dedupe(_split_csv(text))


def _normalize_iterable(values: Iterable[object]) -> list[str] | None:
    items: list[str] = []
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            items.append(text)
    normalized = _dedupe(items)
    return normalized or None


def _split_csv(value: str) -> list[str]:
    return [
        chunk.strip().strip("\"'")
        for chunk in value.split(",")
        if chunk.strip().strip("\"'")
    ]


def _dedupe(values: list[str]) -> list[str]:
    deduped: list[str] = []
    for value in values:
        if value not in deduped:
            deduped.append(value)
    return deduped
