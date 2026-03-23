#!/usr/bin/env bash
set -euo pipefail

# kb-utils.sh — Shared shell functions for kb hooks
# Source this file from hook scripts.

# Generate a ULID (uppercase).
# Tries python-ulid first, falls back to a pure-Python implementation.
kb_generate_ulid() {
  python3 -c "from ulid import ULID; print(str(ULID()).upper())" 2>/dev/null \
    || python3 -c "
import time, os
t = int(time.time() * 1000)
r = os.urandom(10)
E = '0123456789ABCDEFGHJKMNPQRSTVWXYZ'
enc = lambda d, l: ''.join(E[d // 32**i % 32] for i in range(l - 1, -1, -1))
print(enc(t, 10) + enc(int.from_bytes(r, 'big'), 16))
"
}

# Return the current JST timestamp in ISO 8601 format.
# Example: 2026-03-22T23:15+09:00
kb_now_jst() {
  TZ=Asia/Tokyo date '+%Y-%m-%dT%H:%M%z' | sed 's/\([0-9][0-9]\)$/:\1/'
}

# Return a filename-safe JST timestamp.
# Example: 20260322-2315
kb_now_jst_filename() {
  TZ=Asia/Tokyo date '+%Y%m%d-%H%M'
}

# Return the absolute path to the notes/ directory.
# Resolved relative to this script's location (hooks/lib/ -> repo root -> notes/).
kb_notes_dir() {
  local script_dir
  script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  local repo_root
  repo_root="$(cd "${script_dir}/../.." && pwd)"
  echo "${repo_root}/notes"
}

# Write a session log file with proper frontmatter and naming.
#
# Usage:
#   kb_write_session_log <project> <summary> <ai_tool> <content> [ai_client] [repo]
#
# Arguments:
#   project    — project name (directory under notes/projects/)
#   summary    — one-line summary of the session
#   ai_tool    — AI vendor (claude, copilot, codex)
#   content    — markdown body of the session log
#   ai_client  — (optional) specific client (claude-code, copilot-cli, codex-cli)
#   repo       — (optional) repository identifier
#
# Writes to: notes/projects/{project}/session-log/{timestamp}--{ULID}.md
kb_write_session_log() {
  local project="${1:?project is required}"
  local summary="${2:?summary is required}"
  local ai_tool="${3:?ai_tool is required}"
  local content="${4:?content is required}"
  local ai_client="${5:-}"
  local repo="${6:-}"

  local ulid
  ulid="$(kb_generate_ulid)"

  local timestamp
  timestamp="$(kb_now_jst)"

  local ts_filename
  ts_filename="$(kb_now_jst_filename)"

  local notes_dir
  notes_dir="$(kb_notes_dir)"

  local session_dir="${notes_dir}/projects/${project}/session-log"
  mkdir -p "${session_dir}"

  local filepath="${session_dir}/${ts_filename}--${ulid}.md"

  # Escape summary for YAML (replace " with \" inside quoted string)
  local safe_summary="${summary//\"/\\\"}"

  # Build frontmatter
  {
    echo "---"
    echo "id: ${ulid}"
    echo "summary: \"${safe_summary}\""
    echo "ai_tool: ${ai_tool}"
    if [[ -n "${ai_client}" ]]; then
      echo "ai_client: ${ai_client}"
    fi
    if [[ -n "${repo}" ]]; then
      echo "repo: ${repo}"
    fi
    echo "tags: []"
    echo "related: []"
    echo "created: ${timestamp}"
    echo "updated: ${timestamp}"
    echo "---"
    echo ""
    echo "${content}"
  } > "${filepath}"

  chmod 444 "${filepath}"  # read-only — session logs are immutable

  echo "${filepath}"
}
