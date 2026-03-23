#!/usr/bin/env bash
set -euo pipefail

# install.sh — Installation instructions for Codex CLI session-end hook
#
# NOTE: Codex CLI hooks are experimental and the configuration format
#       may change. The adapter reads JSON from stdin as per the current
#       hook schema (cwd, transcript_path, last_assistant_message).
#
# Priority for Codex integration: skills > MCP > AGENTS.md > hooks

KB_HOOKS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cat <<INSTRUCTIONS
=== Codex CLI Session-End Hook Installation (Experimental) ===

WARNING: Codex CLI hooks are experimental and may change.
         Prioritize skills + MCP integration for stable Codex support.

Codex CLI hooks are discovered from hooks.json files.
The Stop hook receives JSON via stdin with fields like:
  cwd, transcript_path, last_assistant_message, session_id, model

To enable (when hooks become stable):

1. Check Codex documentation for the current hook configuration location.
   As of writing, hooks are configured via hooks.json files discovered
   by the Codex engine.

2. Register the adapter for the Stop event:

   {
     "Stop": [
       {
         "command": "${KB_HOOKS_DIR}/adapters/codex-adapter.sh"
       }
     ]
   }

3. Ensure the scripts are executable:

   chmod +x "${KB_HOOKS_DIR}/adapters/codex-adapter.sh"
   chmod +x "${KB_HOOKS_DIR}/on-session-end.sh"

The adapter reads JSON from stdin, extracts cwd and transcript_path,
and delegates to on-session-end.sh. If transcript_path is available,
the last 100 lines are used as session content.

Hooks directory: ${KB_HOOKS_DIR}
INSTRUCTIONS
