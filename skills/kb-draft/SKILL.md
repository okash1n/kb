---
name: kb-draft
description: >
  Save a draft idea or "want to do" memo.
  Use when the user mentions an idea, a future plan, or something
  to try later that isn't actionable right now. Also use when the
  user says "wouldn't it be nice if..." or "maybe we should...".
compatibility: Requires kb MCP server (Python 3.12+, uv)
metadata:
  mcp_server: kb
  mcp_tool: draft
---

# kb-draft: アイデア・やりたいことメモ

You are saving a draft — an idea, a "want to do", or a rough thought that the user wants to capture before it's lost.

## Step 1: Gather the Idea

Ask the user:

1. **What's the idea?** — What do you want to do or try? It can be rough and unpolished.
2. **Any additional context?** — Why does this matter? What triggered the thought?

Keep it lightweight. Drafts are meant to be quick captures, not detailed plans.

## Step 2: Determine Project or Inbox

Pass `cwd` (current working directory) to the MCP tool. The resolver will auto-detect the project from the git remote URL. If you know the project name, pass it as `project` directly.

If this is a general idea with no specific project, omit `project` — it goes to inbox.

## Step 3: Generate Slug and Content

- Generate a concise slug in English (kebab-case, e.g., `auto-tag-suggestion-feature`)
- Write the content in Markdown. Keep it simple:

```markdown
## Idea

(the core idea)

## Context

(why this came up, what triggered it — optional)

## Notes

(any additional thoughts — optional)
```

## Step 4: Save

Call the MCP tool `draft` with:
- `cwd`: the current working directory
- `ai_tool`: your vendor name (`claude`, `copilot`, or `codex`)
- `ai_client`: your specific client name (`claude-code`, `copilot-cli`, `codex-cli`, etc.) — optional
- `project`: only if explicitly known; otherwise let the resolver handle it from cwd (omit entirely for inbox items)
- `slug`: the generated slug
- `summary`: a one-line summary of the idea
- `content`: the formatted Markdown content
- `tags`: relevant tags if any

## Important

- Drafts should be fast. Do not over-engineer the content.
- The user may give you just one sentence. That's fine — capture it and move on.
- Write in the same language the user is communicating in.
- Drafts are the lightest note type. Don't over-engineer them.
