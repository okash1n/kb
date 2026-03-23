---
name: kb-gap
description: >
  Record a gap between what the user wanted and what AI actually did.
  Use when the user corrects the AI, expresses frustration with AI output,
  or explicitly says the result was wrong. Also use proactively when you
  notice your output significantly diverged from the user's intent.
compatibility: Requires kb MCP server (Python 3.12+, uv)
metadata:
  mcp_server: kb
  mcp_tool: gap
---

# kb-gap: ギャップ記録

You are recording a gap — a mismatch between what the user wanted and what the AI did. This is one of the most valuable types of feedback in the kb system.

## Step 1: Gather Information

Ask the user the following questions interactively. Do NOT skip any of them.

1. **What happened?** — What did the AI propose or do?
2. **What did you actually want?** — What was the correct behavior or output?
3. **Why did the gap occur?** — Was it a misunderstanding of context? Missing information? Wrong assumption? A known limitation?

If the gap is obvious from the current conversation context (e.g., the user just corrected you), you may pre-fill answers and confirm with the user instead of asking from scratch.

## Step 2: Determine the Project

Pass `cwd` (current working directory) to the MCP tool. The resolver will auto-detect the project from the git remote URL. If you know the project name, pass it as `project` directly.

## Step 3: Generate Slug and Content

- Generate a concise, descriptive slug in English (kebab-case, e.g., `wrong-import-path-suggestion`)
- Write the content in Markdown with these sections:

```markdown
## What AI Did

(description of the AI's behavior)

## What User Wanted

(description of the correct behavior)

## Why the Gap Occurred

(analysis of root cause)

## How to Avoid

(concrete guidance for future sessions)
```

## Step 4: Save

Call the MCP tool `gap` with:
- `cwd`: the current working directory
- `ai_tool`: your vendor name (`claude`, `copilot`, or `codex`)
- `ai_client`: your specific client name (`claude-code`, `copilot-cli`, `codex-cli`, etc.) — optional
- `project`: only if explicitly known; otherwise let the resolver handle it from cwd
- `slug`: the generated slug
- `summary`: a one-line summary of the gap (Japanese is fine if the user communicates in Japanese)
- `content`: the formatted Markdown content
- `repo`: only if explicitly known; otherwise let the resolver handle it from cwd
- `tags`: relevant tags (e.g., ["prompt-misunderstanding", "context-missing"])
- `related`: ULIDs of related notes if any

## Important

- Be honest about the gap. The purpose is improvement, not blame.
- If the gap happened in the current session, record which AI tool caused it (it might be you).
- Write the summary and content in the same language the user is communicating in.
