"""Typed models for learning asset resolution."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class ResolverInput:
    source_tool: str
    source_client: str
    session_id: str | None = None
    project: str | None = None
    cwd: str | None = None
    repo: str | None = None


@dataclass(slots=True)
class LearningAssetView:
    asset_key: str
    memory_class: str
    update_target: str
    scope: str
    force: str
    confidence: str
    lifecycle: str
    learning_state_visibility: str
    candidate_key: str | None
    review_id: str | None
    materialization_key: str | None
    note_id: str | None
    note_path: str | None
    updated_at: str
    provenance: dict[str, object]
    traceability: dict[str, object]
    revocation_path: dict[str, object]
