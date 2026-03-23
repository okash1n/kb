#!/usr/bin/env bash
set -euo pipefail

# codex-adapter.sh — Codex CLI Stop hook adapter
#
# Converts Codex CLI hook payload (JSON via stdin) into on-session-end.sh arguments.
#
# Codex CLI Stop hooks receive a JSON payload with fields like:
#   session_id, cwd, transcript_path, model, etc.
#
# NOTE: Codex CLI hooks are still experimental. This adapter may need updates
#       as the hook API stabilizes.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HOOKS_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Read JSON payload from stdin (if available)
INPUT=""
if [[ ! -t 0 ]]; then
  INPUT="$(cat)"
fi

# Extract fields from JSON payload
cwd=""
transcript_path=""
if [[ -n "${INPUT}" ]]; then
  cwd="$(echo "${INPUT}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('cwd',''))" 2>/dev/null)" || true
  transcript_path="$(echo "${INPUT}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('transcript_path',''))" 2>/dev/null)" || true
fi

# Fallback to environment / defaults
cwd="${cwd:-$(pwd)}"
summary="Codex session ended"
content="no content available"

# If transcript is available, use last 100 lines as content
if [[ -n "${transcript_path}" && -f "${transcript_path}" ]]; then
  content="$(tail -100 "${transcript_path}")"
  # Try to extract a summary from the first few lines
  first_line="$(head -1 "${transcript_path}" | cut -c1-100)"
  if [[ -n "${first_line}" ]]; then
    summary="Codex: ${first_line}"
  fi
fi

export KB_CWD="${cwd}"
export SUMMARY="${summary}"
export AI_TOOL="codex"
export AI_CLIENT="codex-cli"
export CONTENT="${content}"

exec "${HOOKS_DIR}/on-session-end.sh"
