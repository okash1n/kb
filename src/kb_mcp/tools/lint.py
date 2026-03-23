"""kb_lint — Rule compliance check."""

import re

from kb_mcp.config import kb_data_root, projects_dir, safe_resolve
from kb_mcp.note import parse_frontmatter

REQUIRED_FIELDS = ["id", "summary", "ai_tool", "created", "updated"]
VALID_AI_TOOLS = ["claude", "copilot", "codex"]
RECOMMENDED_AI_CLIENTS = [
    "claude-code", "copilot-cli", "copilot-vscode", "codex-cli",
]
ULID_PATTERN = re.compile(r"^[0-9A-Z]{26}$")
TIMESTAMP_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?:[+-]\d{2}:\d{2}|[+-]\d{4})$"
)
SLUG_FILENAME_PATTERN = re.compile(r"^.+--[0-9A-Z]{26}\.md$")
SESSION_FILENAME_PATTERN = re.compile(r"^\d{8}-\d{4}--[0-9A-Z]{26}\.md$")

# Obsidian unintended tag patterns:
# - #<digits><non-digit> like #1) or #4） — observed to be tagged despite ) not being
#   an official tag character (confirmed via Obsidian v1.8+ tag pane, see ADR config-dir-env-vars-and-tag-lint)
# - #L<digits> like #L42 — GitHub line number references, valid tag per Obsidian spec
UNINTENDED_TAG_PATTERN = re.compile(r'(?<!\S)#(?:\d+[^\s\d]|L\d+)')
CODE_BLOCK_PATTERN = re.compile(r'```[\s\S]*?```')
INLINE_CODE_PATTERN = re.compile(r'`[^`]+`')

# Legacy ai_tool values that should be migrated
AI_TOOL_MIGRATION = {
    "claude-code": "claude",
    "copilot-cli": "copilot",
    "codex-cli": "codex",
}


def kb_lint(project: str | None = None) -> str:
    """Check notes for rule compliance.

    Validates:
    - Required frontmatter fields
    - ULID format
    - Timestamp format (any ISO 8601 offset, not just +09:00)
    - ai_tool values (with migration hints for legacy values)
    - ai_client recommendations
    - Filename conventions
    - ULID in filename matches frontmatter id
    - Session logs are read-only
    - .kb-project.yml presence for projects
    - Array fields use inline format
    """
    root_dir = kb_data_root()
    if project:
        search_dir = safe_resolve(projects_dir(), project)
    else:
        search_dir = root_dir

    if not search_dir.exists():
        return f"Directory not found: {search_dir}"

    issues: list[str] = []
    warnings: list[str] = []

    # Check .kb-project.yml for projects
    project_root = projects_dir()
    if project_root.exists():
        for project_dir in project_root.iterdir():
            if not project_dir.is_dir():
                continue
            if project and project_dir.name != project:
                continue
            yml_path = project_dir / ".kb-project.yml"
            if not yml_path.exists():
                warnings.append(
                    f"projects/{project_dir.name}: missing .kb-project.yml "
                    f"(run kb_init to create)"
                )

    for md_file in search_dir.rglob("*.md"):
        # Skip history.md and non-note files
        if md_file.name == "history.md":
            continue

        rel = md_file.relative_to(root_dir)
        text = md_file.read_text(encoding="utf-8")
        fm = parse_frontmatter(text)

        if fm is None:
            issues.append(f"{rel}: missing frontmatter")
            continue

        # Check required fields
        for field in REQUIRED_FIELDS:
            if field not in fm:
                issues.append(f"{rel}: missing required field '{field}'")

        # Validate ULID
        ulid_val = fm.get("id", "")
        if ulid_val and not ULID_PATTERN.match(ulid_val):
            issues.append(f"{rel}: invalid ULID format '{ulid_val}'")

        # Validate ai_tool
        ai_tool = fm.get("ai_tool", "")
        if ai_tool:
            if ai_tool in AI_TOOL_MIGRATION:
                warnings.append(
                    f"{rel}: ai_tool '{ai_tool}' should be migrated to "
                    f"'{AI_TOOL_MIGRATION[ai_tool]}' (move '{ai_tool}' to ai_client)"
                )
            elif ai_tool not in VALID_AI_TOOLS:
                issues.append(f"{rel}: invalid ai_tool '{ai_tool}'")

        # Validate ai_client (recommendation only)
        ai_client = fm.get("ai_client", "")
        if ai_client and ai_client not in RECOMMENDED_AI_CLIENTS:
            warnings.append(
                f"{rel}: ai_client '{ai_client}' is not a recognized client "
                f"(known: {', '.join(RECOMMENDED_AI_CLIENTS)})"
            )

        # Check array fields use inline format (not multiline list)
        raw_fm = text.split("---", 2)[1] if text.startswith("---") else ""
        for array_field in ["tags", "related"]:
            in_field = False
            for line in raw_fm.split("\n"):
                stripped = line.strip()
                if stripped.startswith(f"{array_field}:"):
                    val = stripped[len(array_field) + 1:].strip()
                    if not val or val == "[]":
                        break
                    if val.startswith("["):
                        break
                    in_field = True
                    continue
                if in_field:
                    if stripped.startswith("- "):
                        warnings.append(
                            f"{rel}: '{array_field}' uses multiline list format. "
                            f"Use inline format: {array_field}: [a, b]"
                        )
                        break
                    else:
                        break

        # Validate timestamps (any ISO 8601 offset)
        for ts_field in ["created", "updated"]:
            ts_val = fm.get(ts_field, "")
            if ts_val and not TIMESTAMP_PATTERN.match(ts_val):
                issues.append(f"{rel}: invalid timestamp format in '{ts_field}'")

        # Validate filename
        parent_name = md_file.parent.name
        if parent_name == "session-log":
            if not SESSION_FILENAME_PATTERN.match(md_file.name):
                issues.append(f"{rel}: session-log filename should be yyyymmdd-hhmm--ULID.md")
        else:
            if not SLUG_FILENAME_PATTERN.match(md_file.name):
                issues.append(f"{rel}: filename should be slug--ULID.md")

        # Check ULID in filename matches frontmatter
        if ulid_val and f"--{ulid_val}.md" not in md_file.name:
            issues.append(f"{rel}: ULID in filename doesn't match frontmatter id")

        # Session logs must be read-only (immutable)
        if parent_name == "session-log" and md_file.stat().st_mode & 0o222:
            issues.append(f"{rel}: session-log should be read-only (chmod 444)")

        # Obsidian unintended tag detection
        # Strip frontmatter, code blocks, and inline code before checking
        body = text.split("---", 2)[2] if text.startswith("---") and text.count("---") >= 2 else text
        body_stripped = CODE_BLOCK_PATTERN.sub("", body)
        body_stripped = INLINE_CODE_PATTERN.sub("", body_stripped)
        for line_no, line in enumerate(body_stripped.split("\n"), start=1):
            for m in UNINTENDED_TAG_PATTERN.finditer(line):
                tag_text = m.group()
                warnings.append(
                    f"{rel}:{line_no}: possible unintended Obsidian tag '{tag_text}' "
                    f"— wrap in backticks to prevent (e.g. `{tag_text}`)"
                )

    result_parts = []
    if issues:
        result_parts.append(f"Found {len(issues)} issue(s):")
        result_parts.extend(f"  - {i}" for i in issues)
    if warnings:
        result_parts.append(f"Found {len(warnings)} warning(s):")
        result_parts.extend(f"  - {w}" for w in warnings)
    if not result_parts:
        return "All notes pass lint checks."

    return "\n".join(result_parts)
