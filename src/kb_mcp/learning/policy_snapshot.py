"""Persist policy projections as runtime snapshots."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from kb_mcp.config import runtime_dir
from kb_mcp.events.store import EventStore
from kb_mcp.learning.policy_projection import build_policy_projections


def policy_snapshot_root() -> Path:
    return runtime_dir() / "learning"


def build_policy_snapshots(*, store: EventStore | None = None) -> dict[str, Any]:
    projections = build_policy_projections(store=store)
    root = policy_snapshot_root()
    generated_at = datetime.now(UTC).isoformat()
    written: list[str] = []
    for target, items in projections.items():
        path = _snapshot_path(root, target)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "target": target,
                    "generated_at": generated_at,
                    "policy_count": len(items),
                    "policies": items,
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        written.append(str(path))
    return {
        "generated_at": generated_at,
        "targets": len(projections),
        "paths": sorted(written),
    }


def load_policy_snapshots() -> list[dict[str, Any]]:
    root = policy_snapshot_root()
    if not root.exists():
        return []
    snapshots: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*.json")):
        snapshots.append(
            json.loads(path.read_text(encoding="utf-8")) | {"path": str(path)}
        )
    return snapshots


def _snapshot_path(root: Path, target: str) -> Path:
    kind, name = target.split(":", 1)
    return root / kind / f"{name}.json"
