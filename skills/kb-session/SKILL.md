---
name: kb-session
description: >
  Save a session log summarizing the current working session.
  Use at the end of a working session, when the user says they're done,
  wrapping up, switching tasks, or when context is about to be compacted.
  Pay special attention to recording any gaps that occurred.
compatibility: Requires kb MCP server (Python 3.12+, uv)
metadata:
  mcp_server: kb
  mcp_tool: session
---

# kb-session: セッションログ保存

You are saving a session log — a record of what happened during this working session.

## Step 1: Analyze the Current Session

Review the conversation history and summarize:

1. **What was worked on** — Features implemented, bugs fixed, investigations done
2. **Decisions made** — Any architectural or design decisions (even small ones)
3. **Gaps encountered** — Did the user correct the AI at any point? Were there misunderstandings? This is especially important to capture.
4. **Notable context** — Anything that would be useful for a future session to know (blockers, unfinished work, next steps)

Present this summary to the user and ask if anything should be added or changed.

## Step 2: Determine the Project

Pass `cwd` (current working directory) to the MCP tool. The resolver will auto-detect the project from the git remote URL. If you know the project name, pass it as `project` directly.

## Step 3: Format Content

Write the content in Markdown:

```markdown
## Summary

(1-3 sentence overview)

## What Was Done

- (bulleted list of work items)

## Decisions Made

- (bulleted list, or "None" if no decisions)

## Gaps Encountered

- (bulleted list describing any gaps, or "None")

## Next Steps

- (what should be done next, or "None")
```

## Step 4: Save

Call the MCP tool `session` with:
- `cwd`: the current working directory
- `ai_tool`: your vendor name (`claude`, `copilot`, or `codex`)
- `ai_client`: your specific client name (`claude-code`, `copilot-cli`, `codex-cli`, etc.) — optional
- `project`: only if explicitly known; otherwise let the resolver handle it from cwd
- `summary`: a one-line summary of the session
- `content`: the formatted Markdown content
- `repo`: only if explicitly known; otherwise let the resolver handle it from cwd
- `tags`: relevant tags reflecting the work done
- `related`: ULIDs of any notes created or referenced during the session

## Important

- Pay special attention to recording gaps. They are the most valuable feedback.
- The session log is a factual record. Do not editorialize.
- Write in the same language the user has been communicating in.
- If gaps were recorded separately via `gap` during the session, reference their ULIDs in `related`.
