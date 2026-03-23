#!/usr/bin/env bash
set -euo pipefail

# copilot-adapter.sh — Copilot CLI sessionEnd hook adapter
#
# Converts Copilot CLI hook context into on-session-end.sh arguments.
#
# Copilot CLI sessionEnd hooks receive JSON via stdin with session context.
# This adapter parses the JSON payload and falls back to environment variables
# for any missing fields.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HOOKS_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Read JSON payload from stdin (if available)
INPUT=""
if [[ ! -t 0 ]]; then
  INPUT="$(cat)"
fi

# Extract fields from JSON payload, fall back to env vars
project=""
summary=""
content=""
cwd=""
repo=""

if [[ -n "${INPUT}" ]]; then
  cwd="$(echo "${INPUT}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('cwd',''))" 2>/dev/null)" || true
  summary="$(echo "${INPUT}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('summary',''))" 2>/dev/null)" || true
  content="$(echo "${INPUT}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('content',''))" 2>/dev/null)" || true
  project="$(echo "${INPUT}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('project',''))" 2>/dev/null)" || true
  repo="$(echo "${INPUT}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('repo',''))" 2>/dev/null)" || true
fi

# Fall back to environment variables
cwd="${cwd:-${COPILOT_CWD:-$(pwd)}}"
summary="${summary:-${COPILOT_SUMMARY:-session ended}}"
content="${content:-${COPILOT_CONTENT:-no content available}}"
project="${project:-${COPILOT_PROJECT:-}}"
repo="${repo:-${COPILOT_REPO:-}}"

export KB_CWD="${cwd}"
export KB_REPO="${repo}"
export SUMMARY="${summary}"
export AI_TOOL="copilot"
export AI_CLIENT="copilot-cli"
export CONTENT="${content}"
export PROJECT="${project}"
export REPO="${repo}"

exec "${HOOKS_DIR}/on-session-end.sh"
