"""Promote narrow learning assets into wider scopes."""

from __future__ import annotations

import json

from kb_mcp.events.store import EventStore
from kb_mcp.learning.distribution import scope_distribution_metadata


def promote_learning_scopes(*, store: EventStore | None = None) -> dict[str, object]:
    active_store = store or EventStore()
    rows = active_store.list_learning_assets()
    grouped: dict[tuple[str, str], list[object]] = {}
    for row in rows:
        if str(row["lifecycle"]) != "active" or str(row["scope"]) != "project_local":
            continue
        memory_class = str(row["memory_class"])
        if memory_class == "session_thin":
            continue
        grouped.setdefault((memory_class, str(row["update_target"])), []).append(row)

    promoted: list[dict[str, str]] = []
    for (memory_class, update_target), assets in grouped.items():
        projects = sorted(
            {
                str(_json_dict(row["provenance_json"]).get("project"))
                for row in assets
                if _json_dict(row["provenance_json"]).get("project")
            }
        )
        metadata = scope_distribution_metadata(memory_class)
        if not metadata["distribution_allowed"] or len(projects) < 2:
            continue
        traceability = _traceability(metadata, assets, projects)
        user_asset_key = f"promoted:user_global:{memory_class}:{update_target}"
        active_store.upsert_learning_asset(
            asset_key=user_asset_key,
            candidate_key=None,
            review_id=None,
            materialization_key=None,
            note_id=None,
            note_path=None,
            memory_class=memory_class,
            update_target=update_target,
            scope="user_global",
            force="default",
            confidence="reviewed",
            lifecycle="active",
            provenance={"projects": projects, "promotion_scope": "user_global"},
            traceability=traceability,
            revocation_path={"rollback_scope": "user_global", "promoted_from": "project_local"},
            learning_state_visibility="active",
            source_status="promoted",
        )
        promoted.append({"asset_key": user_asset_key, "scope": "user_global"})
        if metadata["secrecy_boundary"] == "general" and len(projects) >= 3:
            general_asset_key = f"promoted:general:{memory_class}:{update_target}"
            active_store.upsert_learning_asset(
                asset_key=general_asset_key,
                candidate_key=None,
                review_id=None,
                materialization_key=None,
                note_id=None,
                note_path=None,
                memory_class=memory_class,
                update_target=update_target,
                scope="general",
                force="default",
                confidence="stable",
                lifecycle="active",
                provenance={"projects": projects, "promotion_scope": "general"},
                traceability=traceability,
                revocation_path={"rollback_scope": "general", "promoted_from": "project_local"},
                learning_state_visibility="active",
                source_status="promoted",
            )
            promoted.append({"asset_key": general_asset_key, "scope": "general"})
    return {"promoted": len(promoted), "results": promoted}
def _traceability(
    metadata: dict[str, object],
    assets: list[object],
    projects: list[str],
) -> dict[str, object]:
    return {
        "promotion_source": "scope_promotion",
        "distribution_allowed": metadata["distribution_allowed"],
        "secrecy_boundary": metadata["secrecy_boundary"],
        "source_asset_keys": sorted(str(getattr(row, "__getitem__")("asset_key")) for row in assets),
        "source_projects": projects,
    }


def _json_dict(value: object) -> dict[str, object]:
    if not value:
        return {}
    loaded = json.loads(str(value))
    return loaded if isinstance(loaded, dict) else {}
