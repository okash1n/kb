"""Build runtime learning packets from resolved assets."""

from __future__ import annotations

from kb_mcp.events.store import EventStore
from kb_mcp.learning.client_capabilities import adjust_asset_for_client
from kb_mcp.learning.models import LearningAssetView, ResolverInput
from kb_mcp.learning.revocation import packet_expires_at
from kb_mcp.learning.resolver import asset_matches_request
from kb_mcp.note import generate_ulid
from kb_mcp.resolver import resolve_project


def build_learning_packet(
    request: ResolverInput,
    *,
    tool_name: str,
    assets: list[LearningAssetView],
    store: EventStore | None = None,
) -> dict[str, object] | None:
    if not assets:
        return None
    resolved_project, _ = resolve_project(
        project=request.project,
        cwd=request.cwd,
        repo=request.repo,
    )
    effective_assets: list[LearningAssetView] = []
    downgraded_asset_keys: list[str] = []
    omitted_asset_keys: list[str] = []
    for asset in assets:
        if not asset_matches_request(asset, request=request, resolved_project=resolved_project):
            omitted_asset_keys.append(asset.asset_key)
            continue
        adjusted = adjust_asset_for_client(asset, source_client=request.source_client)
        if adjusted is None:
            omitted_asset_keys.append(asset.asset_key)
            continue
        if adjusted.force != asset.force:
            downgraded_asset_keys.append(asset.asset_key)
        effective_assets.append(adjusted)
    if not effective_assets:
        return None
    packet_id = generate_ulid()
    active_store = store or EventStore()
    asset_keys = [asset.asset_key for asset in effective_assets]
    active_store.create_learning_packet(
        packet_id=packet_id,
        source_tool=request.source_tool,
        source_client=request.source_client,
        tool_name=tool_name,
        session_id=request.session_id,
        project=resolved_project,
        repo=request.repo,
        cwd=request.cwd,
        asset_keys=asset_keys,
        expires_at=packet_expires_at(),
    )
    return {
        "packet_id": packet_id,
        "asset_keys": asset_keys,
        "assets": effective_assets,
        "downgraded_asset_keys": downgraded_asset_keys,
        "omitted_asset_keys": omitted_asset_keys,
    }
