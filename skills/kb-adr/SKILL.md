---
name: kb-adr
description: >
  Record an Architecture Decision Record (ADR).
  Use when a significant technical decision is made or discussed,
  such as choosing a framework, changing architecture, or establishing
  a convention. Focus on WHY the decision was made, not WHAT.
  Also use when revisiting or superseding a previous decision.
compatibility: Requires kb MCP server (Python 3.12+, uv)
metadata:
  mcp_server: kb
  mcp_tool: adr
---

# kb-adr: 意思決定ログ

You are recording an Architecture Decision Record (ADR) — a record of a significant decision made during the project.

## Core Principle

ADRs are about **WHY**, not **WHAT**. The "what" belongs in project documentation. The ADR captures:
- The context that led to the decision
- The decision itself
- Why this option was chosen over alternatives
- What trade-offs were accepted

## Step 1: Gather the Decision

Ask the user:

1. **What decision was made?** — A clear, concise statement of the decision
2. **What was the context?** — What problem or situation prompted this decision?
3. **What alternatives were considered?** — What other options were on the table?
4. **Why was this option chosen?** — What were the deciding factors?
5. **What are the trade-offs?** — What downsides or risks were accepted?

If the decision was just made in the current conversation, pre-fill answers and confirm with the user.

## Step 2: Determine the Project

Pass `cwd` (current working directory) to the MCP tool. The resolver will auto-detect the project from the git remote URL. If you know the project name, pass it as `project` directly.

## Step 3: Check for Superseded ADRs

Search for existing ADRs that this decision might supersede:
- Call the MCP tool `search` with `note_type: "adr"` and the project name
- Review results for related decisions
- If this supersedes an existing ADR, note the old ADR's ULID for the `related` field

## Step 4: Generate Slug and Content

- Generate a descriptive slug in English (kebab-case, e.g., `use-fastmcp-over-raw-protocol`)
- Write the content in Markdown:

```markdown
## Context

(the situation or problem that prompted the decision)

## Decision

(the decision itself — clear and concise)

## Alternatives Considered

### Alternative 1: (name)
- Pros: ...
- Cons: ...

### Alternative 2: (name)
- Pros: ...
- Cons: ...

(add more as needed)

## Why This Choice

(the reasoning — the most important section)

## Trade-offs

(what downsides were accepted)

## Consequences

(expected impact of this decision — optional)
```

## Step 5: Save

Call the MCP tool `adr` with:
- `cwd`: the current working directory
- `ai_tool`: your vendor name (`claude`, `copilot`, or `codex`)
- `ai_client`: your specific client name (`claude-code`, `copilot-cli`, `codex-cli`, etc.) — optional
- `project`: only if explicitly known; otherwise let the resolver handle it from cwd
- `slug`: the generated slug
- `summary`: a one-line summary of the decision
- `content`: the formatted Markdown content
- `repo`: only if explicitly known; otherwise let the resolver handle it from cwd
- `tags`: relevant tags
- `related`: ULIDs of related or superseded ADRs
- `status`: "accepted" (default), or "superseded" if updating an old ADR

## Important

- Focus on the WHY. If you find yourself describing implementation details, move those to a knowledge note instead.
- If this decision supersedes a previous ADR, make sure to link them via `related`.
- Write in the same language the user is communicating in.
