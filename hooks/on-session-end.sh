#!/usr/bin/env bash
set -euo pipefail

# on-session-end.sh — Generic session-end hook for kb
#
# Saves a session log when an AI session ends.
# Can be invoked directly or called by AI-tool-specific hooks.
#
# Project is auto-resolved via kb_resolve_project if not explicitly provided.
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
source "${SCRIPT_DIR}/lib/kb-utils.sh"
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

if [[ -z "${project}" ]]; then
  echo "Error: Could not resolve project. Set PROJECT or KB_CWD/KB_REPO." >&2
  exit 1
fi

filepath="$(kb_write_session_log "${project}" "${summary}" "${ai_tool}" "${content}" "${ai_client}" "${repo}")"

echo "Session log saved: ${filepath}" >&2
