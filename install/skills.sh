#!/usr/bin/env bash
set -euo pipefail

# install/skills.sh — Install kb skills to AI tool global directories
#
# Usage:
#   bash install/skills.sh           # Install to all supported tools
#   bash install/skills.sh claude    # Install to Claude Code only
#   bash install/skills.sh copilot   # Install to Copilot CLI only
#   bash install/skills.sh codex     # Install to Codex CLI only

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SKILLS_DIR="$(cd "${SCRIPT_DIR}/../skills" && pwd)"
SKILLS=(kb-gap kb-session kb-draft kb-knowledge kb-adr)

target="${1:-all}"

install_claude() {
  echo "=== Claude Code ==="
  local dest="${HOME}/.claude/skills"
  mkdir -p "${dest}"
  for skill in "${SKILLS[@]}"; do
    ln -sfn "${SKILLS_DIR}/${skill}" "${dest}/${skill}"
    echo "  ${dest}/${skill} -> ${SKILLS_DIR}/${skill}"
  done
  echo "  Done. Use /kb-gap, /kb-session, etc."
  echo ""
}

install_copilot() {
  echo "=== Copilot CLI ==="
  # agentskills standard: ~/.agents/skills/ (cross-tool)
  local dest_agents="${HOME}/.agents/skills"
  mkdir -p "${dest_agents}"
  for skill in "${SKILLS[@]}"; do
    ln -sfn "${SKILLS_DIR}/${skill}" "${dest_agents}/${skill}"
    echo "  ${dest_agents}/${skill} -> ${SKILLS_DIR}/${skill}"
  done
  # Copilot-specific: ~/.copilot/skills/
  local dest_copilot="${HOME}/.copilot/skills"
  mkdir -p "${dest_copilot}"
  for skill in "${SKILLS[@]}"; do
    ln -sfn "${SKILLS_DIR}/${skill}" "${dest_copilot}/${skill}"
    echo "  ${dest_copilot}/${skill} -> ${SKILLS_DIR}/${skill}"
  done
  echo "  Done."
  echo ""
}

install_codex() {
  echo "=== Codex CLI ==="
  local codex_home="${CODEX_HOME:-${HOME}/.codex}"
  local dest="${codex_home}/skills"
  mkdir -p "${dest}"
  for skill in "${SKILLS[@]}"; do
    ln -sfn "${SKILLS_DIR}/${skill}" "${dest}/${skill}"
    echo "  ${dest}/${skill} -> ${SKILLS_DIR}/${skill}"
  done
  echo ""
  echo "  If skills are not auto-discovered, add to ${codex_home}/config.toml:"
  echo ""
  for skill in "${SKILLS[@]}"; do
    echo "  [[skills.config]]"
    echo "  enabled = true"
    echo "  path = \"${SKILLS_DIR}/${skill}\""
    echo ""
  done
  echo ""
  echo "  Done."
  echo ""
}

case "${target}" in
  claude)  install_claude ;;
  copilot) install_copilot ;;
  codex)   install_codex ;;
  all)
    install_claude
    install_copilot
    install_codex
    ;;
  *)
    echo "Usage: bash install/skills.sh [claude|copilot|codex|all]" >&2
    exit 1
    ;;
esac

echo "Skills source: ${SKILLS_DIR}"
echo "To update, run this script again (symlinks will be refreshed)."
