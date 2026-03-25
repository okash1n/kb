"""Learning asset revocation helpers."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json

from kb_mcp.events.store import EventStore
from kb_mcp.note import generate_ulid

DEFAULT_PACKET_TTL_SECONDS = 3600


def packet_expires_at(*, now: datetime | None = None, ttl_seconds: int = DEFAULT_PACKET_TTL_SECONDS) -> str:
    base = now or datetime.now(UTC)
    return (base + timedelta(seconds=ttl_seconds)).isoformat()


def retract_learning_asset(
    *,
    asset_key: str,
    actor: str,
    reason: str,
    store: EventStore | None = None,
) -> dict[str, object]:
    active_store = store or EventStore()
    row = active_store.get_learning_asset(asset_key)
    if row is None:
        raise ValueError(f"learning asset not found: {asset_key}")
    current = _json_obj(row["revocation_path_json"])
    invalidated = active_store.invalidate_learning_packets(
        asset_keys=[asset_key],
        reason=f"retract:{asset_key}",
    )
    updated = active_store.update_learning_asset(
        asset_key=asset_key,
        lifecycle="retracted",
        learning_state_visibility="retracted",
        revocation_path={
            **current,
            "action": "retract",
            "actor": actor,
            "reason": reason,
            "retracted_at": _now_iso(),
        },
        source_status="revoked",
        confidence="stale",
    )
    active_store.record_learning_revocation(
        revocation_id=generate_ulid(),
        asset_key=asset_key,
        action="retract",
        actor=actor,
        reason=reason,
        replacement_asset_key=None,
        invalidated_packet_count=invalidated,
    )
    return _result(updated, invalidated)


def supersede_learning_asset(
    *,
    asset_key: str,
    replacement_asset_key: str,
    actor: str,
    reason: str,
    store: EventStore | None = None,
) -> dict[str, object]:
    active_store = store or EventStore()
    row = active_store.get_learning_asset(asset_key)
    if row is None:
        raise ValueError(f"learning asset not found: {asset_key}")
    replacement = active_store.get_learning_asset(replacement_asset_key)
    if replacement is None:
        raise ValueError(f"replacement learning asset not found: {replacement_asset_key}")
    current = _json_obj(row["revocation_path_json"])
    invalidated = active_store.invalidate_learning_packets(
        asset_keys=[asset_key],
        reason=f"supersede:{asset_key}",
    )
    updated = active_store.update_learning_asset(
        asset_key=asset_key,
        lifecycle="superseded",
        learning_state_visibility="superseded",
        revocation_path={
            **current,
            "action": "supersede",
            "actor": actor,
            "reason": reason,
            "superseded_by": replacement_asset_key,
            "superseded_at": _now_iso(),
        },
        source_status="superseded",
        confidence="stale",
    )
    active_store.record_learning_revocation(
        revocation_id=generate_ulid(),
        asset_key=asset_key,
        action="supersede",
        actor=actor,
        reason=reason,
        replacement_asset_key=replacement_asset_key,
        invalidated_packet_count=invalidated,
    )
    return _result(updated, invalidated)


def expire_learning_assets(
    *,
    before: str,
    actor: str,
    reason: str,
    limit: int = 100,
    store: EventStore | None = None,
) -> dict[str, object]:
    active_store = store or EventStore()
    rows = active_store.list_expirable_learning_assets(before=before, limit=limit)
    results: list[dict[str, object]] = []
    for row in rows:
        asset_key = str(row["asset_key"])
        invalidated = active_store.invalidate_learning_packets(
            asset_keys=[asset_key],
            reason=f"expire:{asset_key}",
        )
        updated = active_store.update_learning_asset(
            asset_key=asset_key,
            lifecycle="expired",
            learning_state_visibility="expired",
            revocation_path={
                **_json_obj(row["revocation_path_json"]),
                "action": "expire",
                "actor": actor,
                "reason": reason,
                "expired_at": _now_iso(),
            },
            source_status="expired",
            confidence="stale",
        )
        active_store.record_learning_revocation(
            revocation_id=generate_ulid(),
            asset_key=asset_key,
            action="expire",
            actor=actor,
            reason=reason,
            replacement_asset_key=None,
            invalidated_packet_count=invalidated,
        )
        results.append(_result(updated, invalidated))
    return {
        "expired": len(results),
        "results": results,
    }


def invalidate_expired_packets(
    *,
    store: EventStore | None = None,
) -> dict[str, int]:
    count = (store or EventStore()).invalidate_expired_learning_packets()
    return {"invalidated_packets": count}


def _result(row: object, invalidated_packets: int) -> dict[str, object]:
    return {
        "asset_key": str(getattr(row, "__getitem__")("asset_key")),
        "lifecycle": str(getattr(row, "__getitem__")("lifecycle")),
        "learning_state_visibility": str(getattr(row, "__getitem__")("learning_state_visibility")),
        "invalidated_packets": invalidated_packets,
    }


def _json_obj(value: object) -> dict[str, object]:
    if not value:
        return {}
    loaded = json.loads(str(value))
    return loaded if isinstance(loaded, dict) else {}


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()
