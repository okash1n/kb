"""Identity helpers for normalized events."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone


def _sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def correlation_id_for_session(source_tool: str, source_client: str, session_id: str | None) -> str | None:
    """Build deterministic correlation id from session identity."""
    if not session_id:
        return None
    return _sha256_hex(f"{source_tool}\x1f{source_client}\x1f{session_id}")


def session_logical_key(correlation_id: str | None) -> str:
    if not correlation_id:
        return "session:standalone"
    return f"session:{correlation_id}"


def tool_logical_key(
    correlation_id: str | None,
    tool_call_id: str,
    *,
    source_tool: str,
    source_client: str,
) -> str:
    if correlation_id:
        return f"tool:{correlation_id}:{tool_call_id}"
    return f"tool:standalone:{source_tool}:{source_client}:{tool_call_id}"


def error_logical_key(
    correlation_id: str | None,
    error_fingerprint: str,
    *,
    source_tool: str,
    source_client: str,
) -> str:
    if correlation_id:
        return f"error:{correlation_id}:{error_fingerprint}"
    return f"error:standalone:{source_tool}:{source_client}:{error_fingerprint}"


def checkpoint_partition_key(
    correlation_id: str | None,
    *,
    source_tool: str,
    source_client: str,
    cwd: str | None,
    transcript_path: str | None,
    occurred_at: str | None,
) -> str:
    if correlation_id:
        return correlation_id
    bucket = _occurred_at_bucket(occurred_at)
    digest = _sha256_hex(
        "\x1f".join(
            [
                source_tool,
                source_client,
                cwd or "",
                transcript_path or "",
                bucket,
            ]
        )
    )
    return f"standalone:{digest}"


def compact_logical_key(partition_key: str, ordinal: int) -> str:
    return f"compact:{partition_key}:{ordinal}"


def _occurred_at_bucket(occurred_at: str | None) -> str:
    if not occurred_at:
        return "unknown"
    try:
        dt = datetime.fromisoformat(occurred_at.replace("Z", "+00:00"))
    except ValueError:
        return "unknown"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    utc = dt.astimezone(timezone.utc)
    return utc.strftime("%Y%m%d%H")


def sink_receipt(sink_name: str, logical_key: str, aggregate_version: int) -> str:
    """Stable receipt for external side effects."""
    return _sha256_hex(f"{sink_name}\x1f{logical_key}\x1f{aggregate_version}")
