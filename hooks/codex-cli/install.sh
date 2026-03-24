#!/usr/bin/env bash
set -euo pipefail

# install.sh — Compatibility instructions for Codex CLI hook install

KB_HOOKS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cat <<INSTRUCTIONS
=== Codex CLI Session-End Hook Installation (Experimental) ===

WARNING: Codex CLI hooks are experimental and may change.
         推奨は `kb-mcp install hooks --codex` で snippet を出し、
         実適用は手動で行うことです。

Codex CLI hooks are discovered from hooks.json files.
The Stop hook receives JSON via stdin with fields like:
  cwd, transcript_path, last_assistant_message, session_id, model

手動設定例:

1. Register the adapter or wrapper for the Stop event:

   {
     "Stop": [
       {
         "command": "${KB_HOOKS_DIR}/adapters/codex-adapter.sh"
       }
     ]
   }

adapter は JSON stdin から `cwd` / `transcript_path` を読み、
`on-session-end.sh` 経由で `dispatch` へ転送する。

Hooks directory: ${KB_HOOKS_DIR}
INSTRUCTIONS
