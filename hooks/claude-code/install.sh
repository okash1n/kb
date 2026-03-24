#!/usr/bin/env bash
set -euo pipefail

# install.sh — Compatibility instructions for Claude Code hook install

KB_HOOKS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cat <<INSTRUCTIONS
=== Claude Code Session-End Hook Installation ===

推奨:
  kb-mcp install hooks --claude
  kb-mcp install hooks --claude --execute

`kb-mcp` がまだ PATH に無い場合の手動設定例:

1. Open ~/.claude/settings.json

{
  "hooks": {
    "Stop": [
      {
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

この command は互換 shim `on-session-end.sh` を呼び、内部では `kb-mcp hook dispatch`
へ転送される。Claude 側で `SUMMARY` / `CONTENT` が渡らない場合も default を維持する。

Hooks directory: ${KB_HOOKS_DIR}
INSTRUCTIONS
