"""Project active learning assets into user/project policy entries."""

from __future__ import annotations

import json
from typing import Any

from kb_mcp.events.store import EventStore


def build_policy_projections(*, store: EventStore | None = None) -> dict[str, list[dict[str, Any]]]:
    rows = (store or EventStore()).list_learning_assets()
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if str(row["lifecycle"]) != "active":
            continue
        if str(row["memory_class"]) == "session_thin":
            continue
        target = _target_for_row(row)
        if target is None:
            continue
        grouped.setdefault(target, []).append(
            {
                "asset_key": str(row["asset_key"]),
                "memory_class": str(row["memory_class"]),
                "update_target": str(row["update_target"]),
                "scope": str(row["scope"]),
                "force": str(row["force"]),
                "confidence": str(row["confidence"]),
                "note_id": _maybe_str(row["note_id"]),
                "note_path": _maybe_str(row["note_path"]),
                "provenance": _json_dict(row["provenance_json"]),
                "traceability": _json_dict(row["traceability_json"]),
                "revocation_path": _json_dict(row["revocation_path_json"]),
            }
        )
    for key in grouped:
        grouped[key] = sorted(
            grouped[key],
            key=lambda item: (
                str(item["update_target"]),
                str(item["memory_class"]),
                str(item["force"]),
                str(item["asset_key"]),
            ),
        )
    return grouped


def _target_for_row(row: object) -> str | None:
    scope = str(getattr(row, "__getitem__")("scope"))
    provenance = _json_dict(getattr(row, "__getitem__")("provenance_json"))
    if scope == "project_local":
        project = provenance.get("project")
        if isinstance(project, str) and project:
            return f"project:{project}"
        return None
    if scope in {"user_global", "general"}:
        return "user:global"
    return None


def _json_dict(value: object) -> dict[str, Any]:
    if not value:
        return {}
    loaded = json.loads(str(value))
    return loaded if isinstance(loaded, dict) else {}


def _maybe_str(value: object) -> str | None:
    return str(value) if value is not None else None
