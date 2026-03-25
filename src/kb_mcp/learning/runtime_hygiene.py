"""Repair and hygiene helpers for runtime learning artifacts."""

from __future__ import annotations

from kb_mcp.events.store import EventStore
from kb_mcp.learning.revocation import invalidate_expired_packets

DEFAULT_SESSION_LOCAL_DAYS = 1
DEFAULT_CLIENT_LOCAL_DAYS = 7


def repair_learning_runtime(
    *,
    session_local_days: int = DEFAULT_SESSION_LOCAL_DAYS,
    client_local_days: int = DEFAULT_CLIENT_LOCAL_DAYS,
    store: EventStore | None = None,
) -> dict[str, int]:
    if session_local_days < 0:
        raise ValueError("session_local_days must be >= 0")
    if client_local_days < 0:
        raise ValueError("client_local_days must be >= 0")
    active_store = store or EventStore()
    invalidated = invalidate_expired_packets(store=active_store)["invalidated_packets"]
    backfilled_legacy_wide_scope_assets = active_store.backfill_learning_traceability_defaults()
    stale_session_asset_keys = active_store.list_stale_learning_asset_keys(
        scope="session_local",
        older_than_days=session_local_days,
    )
    stale_client_asset_keys = active_store.list_stale_learning_asset_keys(
        scope="client_local",
        older_than_days=client_local_days,
    )
    repaired_packet_counts = active_store.repair_learning_packet_asset_counts()
    removed_orphan_applications = active_store.delete_orphan_learning_applications()
    invalidated_stale_asset_packets = 0
    expired_session_local_assets = 0
    expired_client_local_assets = 0
    for asset_key in stale_session_asset_keys:
        invalidated_count = active_store.invalidate_learning_packets(
            asset_keys=[asset_key],
            reason="stale_session_local_asset",
        )
        active_store.expire_learning_asset_runtime_hygiene(
            asset_key=asset_key,
            reason="stale_session_local_asset",
            invalidated_packet_count=invalidated_count,
        )
        invalidated_stale_asset_packets += invalidated_count
        expired_session_local_assets += 1
    for asset_key in stale_client_asset_keys:
        invalidated_count = active_store.invalidate_learning_packets(
            asset_keys=[asset_key],
            reason="stale_client_local_asset",
        )
        active_store.expire_learning_asset_runtime_hygiene(
            asset_key=asset_key,
            reason="stale_client_local_asset",
            invalidated_packet_count=invalidated_count,
        )
        invalidated_stale_asset_packets += invalidated_count
        expired_client_local_assets += 1
    return {
        "invalidated_expired_packets": invalidated,
        "invalidated_stale_asset_packets": invalidated_stale_asset_packets,
        "backfilled_legacy_wide_scope_assets": backfilled_legacy_wide_scope_assets,
        "repaired_packet_asset_counts": repaired_packet_counts,
        "removed_orphan_applications": removed_orphan_applications,
        "expired_session_local_assets": expired_session_local_assets,
        "expired_client_local_assets": expired_client_local_assets,
    }
