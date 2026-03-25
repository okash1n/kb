"""Deterministic defaults for governed runtime learning contract fields."""

from __future__ import annotations

from typing import Any


def default_candidate_semantics(label: str) -> dict[str, Any]:
    if label == "session_thin":
        return {
            "memory_class": "session_thin",
            "update_target": "session_summary_only",
            "scope": "session_local",
            "force": "hint",
            "confidence": "candidate",
            "lifecycle": "candidate",
            "learning_state_visibility": "candidate",
        }
    return {
        "memory_class": label,
        "update_target": _default_update_target(label),
        "scope": "project_local",
        "force": "hint",
        "confidence": "candidate",
        "lifecycle": "candidate",
        "learning_state_visibility": "candidate",
    }


def default_backfilled_asset_fields(
    *,
    memory_class: str,
    source_status: str,
) -> dict[str, str]:
    semantics = default_candidate_semantics(memory_class)
    confidence = "stable" if source_status == "materialized" else "reviewed"
    lifecycle = "active" if source_status == "materialized" else "candidate"
    visibility = "active" if source_status == "materialized" else "candidate"
    return {
        "memory_class": semantics["memory_class"],
        "update_target": semantics["update_target"],
        "scope": semantics["scope"],
        "force": semantics["force"],
        "confidence": confidence,
        "lifecycle": lifecycle,
        "learning_state_visibility": visibility,
    }


def _default_update_target(memory_class: str) -> str:
    mapping = {
        "gap": "behavior_style",
        "knowledge": "fact_model",
        "adr": "decision_policy",
        "session_thin": "session_summary_only",
    }
    return mapping.get(memory_class, "behavior_style")
