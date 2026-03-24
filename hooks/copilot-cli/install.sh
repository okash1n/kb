#!/usr/bin/env bash
set -euo pipefail

# install.sh — Compatibility instructions for Copilot CLI hook install

KB_HOOKS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cat <<INSTRUCTIONS
=== Copilot CLI Session-End Hook Installation ===

推奨:
  kb-mcp install hooks --copilot
  kb-mcp install hooks --copilot --execute

`kb-mcp` がまだ PATH に無い場合の手動設定例:

1. Open ~/.copilot/config.json

2. Add:

   {
     "hooks": {
       "session-end": [
         {
           "bash": "${KB_HOOKS_DIR}/adapters/copilot-adapter.sh"
         }
       ]
     }
   }

adapter は stdin JSON を読み、`on-session-end.sh` 経由で `dispatch` へ流す。

Hooks directory: ${KB_HOOKS_DIR}
INSTRUCTIONS
