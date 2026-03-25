"""Runtime checkpoint sink for compact events."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from kb_mcp.config import runtime_events_dir
from kb_mcp.events.candidates import detect_candidates
from kb_mcp.events.identity import sink_receipt


def write_checkpoint(row: sqlite3.Row) -> str:
    """Persist a runtime checkpoint artifact."""
    checkpoints_dir = runtime_events_dir() / "checkpoints"
    checkpoints_dir.mkdir(parents=True, exist_ok=True)
    safe_name = row["logical_key"].replace(":", "__")
    path = checkpoints_dir / f"{safe_name}.json"
    payload = {
        "logical_key": row["logical_key"],
        "aggregate_version": int(row["aggregate_version"]),
        "summary": row["summary"],
        "content_excerpt": row["content_excerpt"],
        "state": json.loads(row["aggregate_state_json"]),
        "candidates": detect_candidates(row["summary"], row["content_excerpt"]),
        "updated_at": row["updated_at"],
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return sink_receipt("checkpoint_writer", row["logical_key"], int(row["aggregate_version"]))
