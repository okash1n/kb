"""Transcript excerpt helpers."""

from __future__ import annotations

from pathlib import Path


def read_transcript_excerpt(path: str | None, *, line_limit: int = 80, char_limit: int = 4000) -> str:
    """Read a bounded excerpt from a transcript file."""
    if not path:
        return ""
    transcript = Path(path)
    if not transcript.exists():
        return ""
    try:
        lines = transcript.read_text(encoding="utf-8").splitlines()
    except OSError:
        return ""
    excerpt = "\n".join(lines[-line_limit:])
    if len(excerpt) > char_limit:
        excerpt = excerpt[-char_limit:]
    return excerpt
