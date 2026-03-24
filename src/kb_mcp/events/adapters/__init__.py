"""Tool-specific payload adapters."""

from kb_mcp.events.adapters.claude import normalize_claude_payload
from kb_mcp.events.adapters.codex import normalize_codex_payload
from kb_mcp.events.adapters.copilot import normalize_copilot_payload

__all__ = [
    "normalize_claude_payload",
    "normalize_codex_payload",
    "normalize_copilot_payload",
]
