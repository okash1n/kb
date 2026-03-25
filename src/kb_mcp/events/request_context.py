"""Shared request context for kb-owned tool events."""

from __future__ import annotations

from contextvars import ContextVar
from typing import Any

REQUEST_CONTEXT: ContextVar[dict[str, Any] | None] = ContextVar("kb_request_context", default=None)
