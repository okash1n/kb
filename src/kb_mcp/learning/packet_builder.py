"""Build runtime learning packets from resolved assets."""

from __future__ import annotations

from kb_mcp.events.store import EventStore
from kb_mcp.learning.models import LearningAssetView, ResolverInput
from kb_mcp.learning.revocation import packet_expires_at
from kb_mcp.note import generate_ulid


def build_learning_packet(
    request: ResolverInput,
    *,
    tool_name: str,
    assets: list[LearningAssetView],
    store: EventStore | None = None,
) -> dict[str, object] | None:
    if not assets:
        return None
    packet_id = generate_ulid()
    active_store = store or EventStore()
    asset_keys = [asset.asset_key for asset in assets]
    active_store.create_learning_packet(
        packet_id=packet_id,
        source_tool=request.source_tool,
        source_client=request.source_client,
        tool_name=tool_name,
        session_id=request.session_id,
        project=request.project,
        repo=request.repo,
        cwd=request.cwd,
        asset_keys=asset_keys,
        expires_at=packet_expires_at(),
    )
    return {
        "packet_id": packet_id,
        "asset_keys": asset_keys,
        "assets": assets,
    }
