---
name: kb-knowledge
description: >
  Save knowledge learned during development.
  Use when a useful pattern, gotcha, workaround, or technique is
  discovered during development. Also use when a non-obvious solution
  is found, or when debugging reveals behavior worth documenting.
compatibility: Requires kb MCP server (Python 3.12+, uv)
metadata:
  mcp_server: kb
  mcp_tool: knowledge
---

# kb-knowledge: 知識の記録

You are saving a knowledge note — something useful learned during development that's worth preserving for future reference.

## Step 1: Capture What Was Learned

Ask the user (or infer from the conversation):

1. **What did you learn?** — The core insight, pattern, gotcha, or technique
2. **In what context?** — What were you working on when you discovered this?
3. **Any caveats or edge cases?** — Things to watch out for

If the knowledge is obvious from the current conversation (e.g., a workaround was just discovered), pre-fill and confirm with the user.

## Step 2: Determine the Project

Pass `cwd` (current working directory) to the MCP tool. The resolver will auto-detect the project from the git remote URL. If you know the project name, pass it as `project` directly.

If the knowledge is general/cross-project, still assign it to the originating project (it can be graduated to general later via `graduate`).

## Step 3: Generate Slug, Tags, and Content

- Generate a descriptive slug in English (kebab-case, e.g., `obsidian-cli-vault-path-resolution`)
- Generate relevant tags (e.g., `["obsidian", "cli", "path-resolution"]`)
- Write the content in Markdown:

```markdown
## What

(the knowledge — what was learned)

## Context

(when/where this was discovered)

## Details

(deeper explanation, code examples, links)

## Caveats

(edge cases, limitations, things to watch out for — optional)
```

## Step 4: Save

Call the MCP tool `knowledge` with:
- `cwd`: the current working directory
- `ai_tool`: your vendor name (`claude`, `copilot`, or `codex`)
- `ai_client`: your specific client name (`claude-code`, `copilot-cli`, `codex-cli`, etc.) — optional
- `project`: only if explicitly known; otherwise let the resolver handle it from cwd
- `slug`: the generated slug
- `summary`: a one-line summary of the knowledge
- `content`: the formatted Markdown content
- `repo`: only if explicitly known; otherwise let the resolver handle it from cwd
- `tags`: the generated tags
- `related`: ULIDs of related notes if any

## Important

- Knowledge notes should be reusable. Write them so that someone (or an AI) encountering the same situation in the future can benefit.
- Include code examples when they help clarify the knowledge.
- Write in the same language the user is communicating in.
