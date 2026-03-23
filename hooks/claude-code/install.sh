#!/usr/bin/env bash
set -euo pipefail

# install.sh — Installation instructions for Claude Code session-end hook
#
# Claude Code uses Stop hooks configured in ~/.claude/settings.json.
# This script prints the instructions; it does NOT modify settings.json
# automatically for safety.

KB_HOOKS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cat <<INSTRUCTIONS
=== Claude Code Session-End Hook Installation ===

Claude Code supports "Stop" hooks that run when a session ends.
To enable auto-saving session logs, add the following to your
~/.claude/settings.json under the "hooks" key.

1. Open ~/.claude/settings.json

2. Add or merge into the "hooks" section:

{
  "hooks": {
    "Stop": [
      {
        "matcher": "",
        "hooks": [
          {
            "type": "command",
            "command": "SUMMARY=\"\${SUMMARY:-no summary}\" AI_TOOL=claude AI_CLIENT=claude-code CONTENT=\"\${CONTENT:-session ended}\" ${KB_HOOKS_DIR}/on-session-end.sh"
          }
        ]
      }
    ]
  }
}

3. Ensure the hook script is executable:

   chmod +x ${KB_HOOKS_DIR}/on-session-end.sh

4. Set environment variables in your shell profile or per-session:

   export KB_CWD="\$PWD"        # working directory for project resolution
   export KB_REPO="owner/repo"  # optional: explicit repo identifier

   PROJECT is auto-resolved from KB_CWD via the project resolver.
   You can also set PROJECT explicitly to override.

   SUMMARY and CONTENT will typically be provided by the hook context
   or can be set as defaults.

NOTE: ai_tool is set to "claude" (vendor) and ai_client to "claude-code" (client).
      The Stop hook runs when Claude Code finishes a conversation turn.

Hooks directory: ${KB_HOOKS_DIR}
INSTRUCTIONS
