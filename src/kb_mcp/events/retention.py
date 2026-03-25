"""Runtime artifact retention helpers."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from kb_mcp.config import runtime_events_dir


def cleanup_runtime_artifacts(
    *,
    checkpoint_days: int = 7,
    candidate_days: int = 14,
    promotion_days: int = 30,
    record_days: int = 30,
) -> dict[str, int]:
    """Delete stale runtime JSON artifacts and return removed counts."""
    root = runtime_events_dir()
    return {
        "checkpoints": _cleanup_dir(root / "checkpoints", checkpoint_days),
        "candidates": _cleanup_dir(root / "candidates", candidate_days),
        "promotions": _cleanup_dir(root / "promotions", promotion_days),
        "promotion_records": _cleanup_dir(root / "promotion-records", record_days),
    }


def _cleanup_dir(path: Path, days: int) -> int:
    if not path.exists():
        return 0
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    removed = 0
    for item in path.glob("*.json"):
        try:
            modified = datetime.fromtimestamp(item.stat().st_mtime, tz=timezone.utc)
        except OSError:
            continue
        if modified < cutoff:
            item.unlink(missing_ok=True)
            removed += 1
    return removed
