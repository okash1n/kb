#!/usr/bin/env bash
set -euo pipefail

# install/hooks.sh — Show hook installation instructions for AI tools
#
# Usage:
#   bash install/hooks.sh           # Show instructions for all supported tools
#   bash install/hooks.sh claude    # Claude Code only
#   bash install/hooks.sh copilot   # Copilot CLI only
#   bash install/hooks.sh codex     # Codex CLI only

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HOOKS_DIR="$(cd "${SCRIPT_DIR}/../hooks" && pwd)"

# Ensure all hook scripts are executable
chmod +x "${HOOKS_DIR}/on-session-end.sh" 2>/dev/null || true
chmod +x "${HOOKS_DIR}/adapters/"*.sh 2>/dev/null || true

target="${1:-all}"

case "${target}" in
  claude)
    bash "${HOOKS_DIR}/claude-code/install.sh"
    ;;
  copilot)
    bash "${HOOKS_DIR}/copilot-cli/install.sh"
    ;;
  codex)
    bash "${HOOKS_DIR}/codex-cli/install.sh"
    ;;
  all)
    bash "${HOOKS_DIR}/claude-code/install.sh"
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    bash "${HOOKS_DIR}/copilot-cli/install.sh"
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    bash "${HOOKS_DIR}/codex-cli/install.sh"
    ;;
  *)
    echo "Usage: bash install/hooks.sh [claude|copilot|codex|all]" >&2
    exit 1
    ;;
esac
