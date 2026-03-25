#!/usr/bin/env bash
set -euo pipefail

# on-session-end.sh — Legacy-compatible session-end hook shim
#
# Existing hook installations may still call this script directly.
# It now forwards payloads to `kb-mcp hook dispatch` so the Python event
# pipeline is the source of truth while keeping the old entry point alive.
#
# Arguments (positional) or environment variables:
#   SUMMARY    — one-line session summary (required)
#   AI_TOOL    — AI vendor: claude, copilot, codex (required)
#   CONTENT    — session log body in markdown (required)
#   PROJECT    — project name (optional, auto-resolved from KB_CWD/KB_REPO)
#   AI_CLIENT  — specific client: claude-code, copilot-cli, etc. (optional)
#   REPO       — repository identifier (optional)
#   KB_CWD     — working directory for project resolution (optional, default: pwd)
#   KB_REPO    — explicit repo for project resolution (optional)
#
# Make this file executable:
#   chmod +x hooks/on-session-end.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/kb-resolver.sh"

# Accept positional args or fall back to environment variables
summary="${1:-${SUMMARY:-}}"
ai_tool="${2:-${AI_TOOL:-}}"
content="${3:-${CONTENT:-}}"
project="${4:-${PROJECT:-}}"
ai_client="${5:-${AI_CLIENT:-}}"
repo="${6:-${REPO:-}}"

if [[ -z "${summary}" || -z "${ai_tool}" || -z "${content}" ]]; then
  echo "Usage: on-session-end.sh <summary> <ai_tool> <content> [project] [ai_client] [repo]" >&2
  echo "  Or set SUMMARY, AI_TOOL, CONTENT (and optionally PROJECT, AI_CLIENT, REPO, KB_CWD, KB_REPO)." >&2
  exit 1
fi

# Resolve project if not explicitly provided
if [[ -z "${project}" ]]; then
  resolved="$(kb_resolve_project 2>/dev/null)" || true
  if [[ -n "${resolved}" ]]; then
    IFS=$'\t' read -r project resolved_repo <<< "${resolved}"
    if [[ -z "${repo}" && -n "${resolved_repo}" ]]; then
      repo="${resolved_repo}"
    fi
  fi
fi

case "${ai_tool}" in
  claude)
    client="${ai_client:-claude-code}"
    ;;
  copilot)
    client="${ai_client:-copilot-cli}"
    ;;
  codex)
    client="${ai_client:-codex-cli}"
    ;;
  *)
    client="${ai_client:-${ai_tool}-hook}"
    ;;
esac
kb_cmd="${KB_MCP_BIN:-kb-mcp}"
export SUMMARY="${summary}"
export CONTENT="${content}"
export PROJECT="${project}"
export REPO="${repo}"
RAW_INPUT=""
if [[ ! -t 0 ]]; then
  RAW_INPUT="$(cat)"
fi
export RAW_INPUT

payload="$(python3 - <<'PY'
import json
import os

payload = {}
raw = os.environ.get("RAW_INPUT", "").strip()
if raw:
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        payload = {"content": raw}
payload.setdefault("summary", os.environ["SUMMARY"])
payload.setdefault("content", os.environ["CONTENT"])
payload.setdefault("project", os.environ.get("PROJECT") or None)
payload.setdefault("repo", os.environ.get("REPO") or None)
payload.setdefault("session_id", os.environ.get("KB_VENDOR_SESSION_ID") or None)
payload.setdefault("correlation_id", os.environ.get("KB_SESSION_CORRELATION_ID") or None)
payload.setdefault("cwd", os.environ.get("KB_CWD") or os.getcwd())
print(json.dumps(payload, ensure_ascii=False))
PY
)"

printf '%s' "${payload}" | "${kb_cmd}" hook dispatch \
  --tool "${ai_tool}" \
  --client "${client}" \
  --layer client_hook \
  --event turn_checkpointed \
  --run-worker
