"""Runtime candidate sink for compact events."""

from __future__ import annotations

import json
import sqlite3

from kb_mcp.config import runtime_events_dir
from kb_mcp.events.candidates import detect_candidates
from kb_mcp.events.identity import sink_receipt


def write_candidates(row: sqlite3.Row) -> str:
    """Persist candidate artifacts for a checkpoint."""
    detected = detect_candidates(row["summary"], row["content_excerpt"])
    if not detected["has_candidates"]:
        return sink_receipt("candidate_writer", row["logical_key"], int(row["aggregate_version"]))
    candidates_dir = runtime_events_dir() / "candidates"
    candidates_dir.mkdir(parents=True, exist_ok=True)
    safe_name = row["logical_key"].replace(":", "__")
    path = candidates_dir / f"{safe_name}.json"
    payload = {
        "logical_key": row["logical_key"],
        "aggregate_version": int(row["aggregate_version"]),
        "summary": row["summary"],
        "content_excerpt": row["content_excerpt"],
        "items": detected["items"],
        "updated_at": row["updated_at"],
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return sink_receipt("candidate_writer", row["logical_key"], int(row["aggregate_version"]))
