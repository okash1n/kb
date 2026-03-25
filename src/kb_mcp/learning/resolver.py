"""Resolve applicable learning assets for one runtime request."""

from __future__ import annotations

import json
from typing import Iterable

from kb_mcp.events.store import EventStore
from kb_mcp.learning.models import LearningAssetView, ResolverInput
from kb_mcp.resolver import resolve_project

_SCOPE_ORDER = {
    "session_local": 0,
    "client_local": 1,
    "project_local": 2,
    "user_global": 3,
    "general": 4,
}
_CONFIDENCE_ORDER = {
    "stable": 0,
    "reviewed": 1,
    "candidate": 2,
    "observed": 3,
    "stale": 4,
}
_FORCE_ORDER = {
    "guardrail": 0,
    "default": 1,
    "preferred": 2,
    "hint": 3,
}
_APPLICABLE_LIFECYCLES = {"active", "candidate"}


def resolve_learning_assets(
    request: ResolverInput,
    *,
    store: EventStore | None = None,
) -> list[LearningAssetView]:
    resolved_project, _ = resolve_project(
        project=request.project,
        cwd=request.cwd,
        repo=request.repo,
    )
    rows = (store or EventStore()).list_learning_assets()
    matched = [
        _row_to_view(row)
        for row in rows
        if _matches_scope(_row_to_view(row), request=request, resolved_project=resolved_project)
    ]
    return sorted(
        matched,
        key=lambda item: (
            _SCOPE_ORDER[item.scope],
            _CONFIDENCE_ORDER[item.confidence],
            _FORCE_ORDER[item.force],
            -_updated_rank(item.updated_at),
            item.asset_key,
        ),
    )


def _matches_scope(
    asset: LearningAssetView,
    *,
    request: ResolverInput,
    resolved_project: str | None,
) -> bool:
    if asset.lifecycle not in _APPLICABLE_LIFECYCLES:
        return False
    if not _distribution_allows(asset):
        return False
    provenance = asset.provenance
    if asset.scope == "session_local":
        return bool(request.session_id) and provenance.get("session_id") == request.session_id
    if asset.scope == "client_local":
        return provenance.get("source_client") == request.source_client
    if asset.scope == "project_local":
        return bool(resolved_project) and provenance.get("project") == resolved_project
    if asset.scope in {"user_global", "general"}:
        return True
    return False


def _distribution_allows(asset: LearningAssetView) -> bool:
    if asset.scope in {"session_local", "client_local", "project_local"}:
        return True
    distribution_allowed = asset.traceability.get("distribution_allowed")
    if distribution_allowed is False:
        return False
    secrecy_boundary = asset.traceability.get("secrecy_boundary")
    if asset.scope == "user_global":
        return secrecy_boundary in {None, "user", "general"}
    if asset.scope == "general":
        return secrecy_boundary in {None, "general"}
    return True


def _row_to_view(row: object) -> LearningAssetView:
    provenance = _json_obj(getattr(row, "__getitem__")("provenance_json"))
    traceability = _json_obj(getattr(row, "__getitem__")("traceability_json"))
    revocation_path = _json_obj(getattr(row, "__getitem__")("revocation_path_json"))
    return LearningAssetView(
        asset_key=str(getattr(row, "__getitem__")("asset_key")),
        memory_class=str(getattr(row, "__getitem__")("memory_class")),
        update_target=str(getattr(row, "__getitem__")("update_target")),
        scope=str(getattr(row, "__getitem__")("scope")),
        force=str(getattr(row, "__getitem__")("force")),
        confidence=str(getattr(row, "__getitem__")("confidence")),
        lifecycle=str(getattr(row, "__getitem__")("lifecycle")),
        learning_state_visibility=str(getattr(row, "__getitem__")("learning_state_visibility")),
        candidate_key=_maybe_str(getattr(row, "__getitem__")("candidate_key")),
        review_id=_maybe_str(getattr(row, "__getitem__")("review_id")),
        materialization_key=_maybe_str(getattr(row, "__getitem__")("materialization_key")),
        note_id=_maybe_str(getattr(row, "__getitem__")("note_id")),
        note_path=_maybe_str(getattr(row, "__getitem__")("note_path")),
        updated_at=str(getattr(row, "__getitem__")("updated_at")),
        provenance=provenance,
        traceability=traceability,
        revocation_path=revocation_path,
    )


def _json_obj(value: object) -> dict[str, object]:
    if not value:
        return {}
    loaded = json.loads(str(value))
    return loaded if isinstance(loaded, dict) else {}


def _maybe_str(value: object) -> str | None:
    return str(value) if value is not None else None


def _updated_rank(updated_at: str) -> int:
    digits = "".join(ch for ch in updated_at if ch.isdigit())
    return int(digits or "0")


def resolver_orders() -> dict[str, Iterable[str]]:
    return {
        "scope": _SCOPE_ORDER.keys(),
        "confidence": _CONFIDENCE_ORDER.keys(),
        "force": _FORCE_ORDER.keys(),
    }
