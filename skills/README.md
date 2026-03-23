# kb Skills

Skills are predefined prompt templates that wrap kb MCP tools with a no-argument interactive interface.

The user just types a command like `/kb-gap` and the AI handles everything: asking the right questions, determining the project, formatting the content, and calling the MCP tool.

## Available Skills

| Skill | Description |
|-------|-------------|
| `kb-gap` | Record a gap between what user wanted and what AI did |
| `kb-session` | Save a session log summarizing the current working session |
| `kb-draft` | Save a draft idea or "want to do" memo |
| `kb-knowledge` | Save knowledge learned during development |
| `kb-adr` | Record an Architecture Decision Record |

## Installation

### Quick Install (All Tools)

```bash
bash install/skills.sh
```

### Per-Tool Install

```bash
bash install/skills.sh claude    # Claude Code
bash install/skills.sh copilot   # Copilot CLI
bash install/skills.sh codex     # Codex CLI
```

### Manual Installation

#### Claude Code

Symlink to `~/.claude/skills/`:

```bash
for skill in kb-gap kb-session kb-draft kb-knowledge kb-adr; do
  ln -sfn "$(pwd)/skills/$skill" ~/.claude/skills/$skill
done
```

Use with `/kb-gap`, `/kb-session`, etc.

#### Copilot CLI

Symlink to `~/.copilot/skills/` and `~/.agents/skills/`:

```bash
for skill in kb-gap kb-session kb-draft kb-knowledge kb-adr; do
  ln -sfn "$(pwd)/skills/$skill" ~/.copilot/skills/$skill
  ln -sfn "$(pwd)/skills/$skill" ~/.agents/skills/$skill
done
```

#### Codex CLI

Symlink to `$CODEX_HOME/skills/` (default: `~/.codex/skills/`):

```bash
for skill in kb-gap kb-session kb-draft kb-knowledge kb-adr; do
  ln -sfn "$(pwd)/skills/$skill" ~/.codex/skills/$skill
done
```

If skills are not auto-discovered, add to `~/.codex/config.toml`:

```toml
[[skills.config]]
enabled = true
path = "/path/to/kb/skills/kb-gap"

[[skills.config]]
enabled = true
path = "/path/to/kb/skills/kb-session"

# ... repeat for each skill
```

## Prerequisites

- kb MCP server must be connected to the AI tool (see README.md for setup)
- Python 3.12+, uv

## Philosophy

- **Zero arguments**: The user types `/kb-gap` and the AI asks the right questions
- **Context-aware**: Skills infer the project from the working environment via the resolver
- **Interactive**: Skills guide a conversation, not execute a fixed script
- **Language-flexible**: Skills respond in whatever language the user is communicating in
- **Tool-agnostic**: SKILL.md format works across Claude Code, Copilot CLI, and Codex CLI
