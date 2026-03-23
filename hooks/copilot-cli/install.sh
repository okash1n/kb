#!/usr/bin/env bash
set -euo pipefail

# install.sh — Installation instructions for Copilot CLI session-end hook
#
# Copilot CLI supports hooks via .github/hooks/*.json files.
# This script prints the instructions.

KB_HOOKS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cat <<INSTRUCTIONS
=== Copilot CLI Session-End Hook Installation ===

Copilot CLI supports hooks configured in .github/hooks/*.json files.
To enable auto-saving session logs:

1. Create .github/hooks/ directory in your repository:

   mkdir -p .github/hooks

2. Create .github/hooks/session-end.json:

   {
     "version": 1,
     "hooks": {
       "sessionEnd": [
         {
           "bash": "${KB_HOOKS_DIR}/adapters/copilot-adapter.sh"
         }
       ]
     }
   }

   Note: Copilot CLI passes session context as JSON via stdin to the hook.
   The adapter parses this payload and delegates to on-session-end.sh.

3. Ensure the scripts are executable:

   chmod +x "${KB_HOOKS_DIR}/adapters/copilot-adapter.sh"
   chmod +x "${KB_HOOKS_DIR}/on-session-end.sh"

The adapter sets AI_TOOL=copilot and AI_CLIENT=copilot-cli automatically.
Project is auto-resolved from the working directory via the JSON payload.

Hooks directory: ${KB_HOOKS_DIR}
INSTRUCTIONS
