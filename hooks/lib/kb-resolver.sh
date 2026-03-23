#!/usr/bin/env bash
set -euo pipefail

# kb-resolver.sh — Thin wrapper around Python project resolver
# Source this file from hook scripts.
#
# Single source of truth is src/kb_mcp/resolver.py.
# This wrapper calls the Python implementation to avoid logic drift.

# Resolve the kb repo root (hooks/lib/ -> repo root)
_KB_REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# kb_resolve_project [explicit_project]
#
# Output (tab-separated): project_name\trepo_identifier
# Either field may be empty if unresolved.
#
# Environment variables:
#   KB_CWD  — working directory (default: pwd)
#   KB_REPO — explicit repo identifier
#
# Exit code:
#   0 — at least project was resolved
#   1 — project could not be resolved
kb_resolve_project() {
  local explicit_project="${1:-}"
  local cwd="${KB_CWD:-$(pwd)}"
  local repo="${KB_REPO:-}"

  KB_EXPLICIT_PROJECT="${explicit_project}" KB_CWD="${cwd}" KB_REPO="${repo}" \
    uv run --project "${_KB_REPO_ROOT}" python3 - <<'PY'
import os
from kb_mcp.resolver import resolve_project

def _get_env(name):
    value = os.environ.get(name)
    if not value:
        return None
    return value

p, r = resolve_project(
    project=_get_env("KB_EXPLICIT_PROJECT"),
    cwd=_get_env("KB_CWD"),
    repo=_get_env("KB_REPO"),
)
print(f"{p or ''}\t{r or ''}")
if not p:
    raise SystemExit(1)
PY
}
