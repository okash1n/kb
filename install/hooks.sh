#!/usr/bin/env bash
set -euo pipefail

# install/hooks.sh — Compatibility wrapper for `kb-mcp install hooks`
#
# Usage:
#   bash install/hooks.sh           # Show instructions for all supported tools
#   bash install/hooks.sh claude    # Claude Code only
#   bash install/hooks.sh copilot   # Copilot CLI only
#   bash install/hooks.sh codex     # Codex CLI only

target="${1:-all}"

if ! command -v kb-mcp >/dev/null 2>&1; then
  case "${target}" in
    claude)
      bash "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/hooks/claude-code/install.sh"
      ;;
    copilot)
      bash "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/hooks/copilot-cli/install.sh"
      ;;
    codex)
      bash "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/hooks/codex-cli/install.sh"
      ;;
    all)
      bash "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/hooks/claude-code/install.sh"
      echo ""
      bash "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/hooks/copilot-cli/install.sh"
      echo ""
      bash "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/hooks/codex-cli/install.sh"
      ;;
    *)
      echo "Usage: bash install/hooks.sh [claude|copilot|codex|all]" >&2
      exit 1
      ;;
  esac
  exit 0
fi

case "${target}" in
  claude)
    kb-mcp install hooks --claude
    ;;
  copilot)
    kb-mcp install hooks --copilot
    ;;
  codex)
    kb-mcp install hooks --codex
    ;;
  all)
    kb-mcp install hooks --all
    ;;
  *)
    echo "Usage: bash install/hooks.sh [claude|copilot|codex|all]" >&2
    exit 1
    ;;
esac
