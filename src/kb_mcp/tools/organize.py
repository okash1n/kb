"""kb_organize — Discover and suggest missing links between notes."""

from collections import defaultdict

from kb_mcp.config import kb_data_root, safe_resolve
from kb_mcp.note import parse_frontmatter
from kb_mcp import obsidian


async def kb_organize(project: str | None = None) -> str:
    """Analyze notes and suggest missing links.

    Finds:
    - Orphan notes (no incoming links)
    - Dead-end notes (no outgoing links)
    - Notes sharing tags but not linked via 'related'

    Returns suggestions as readable markdown.
    """
    suggestions: list[str] = []

    # 1. Orphans and deadends from Obsidian CLI
    try:
        orphan_result = await obsidian.orphans()
        if orphan_result.strip():
            orphan_lines = [
                line.strip() for line in orphan_result.strip().split("\n")
                if line.strip() and _in_scope(line.strip(), project)
            ]
            if orphan_lines:
                suggestions.append("## Orphan notes (no incoming links)\n")
                for line in orphan_lines:
                    suggestions.append(f"- {line}")
                suggestions.append("")
    except Exception as e:
        suggestions.append(f"(Could not check orphans: {e})\n")

    try:
        deadend_result = await obsidian.deadends()
        if deadend_result.strip():
            deadend_lines = [
                line.strip() for line in deadend_result.strip().split("\n")
                if line.strip() and _in_scope(line.strip(), project)
            ]
            if deadend_lines:
                suggestions.append("## Dead-end notes (no outgoing links)\n")
                for line in deadend_lines:
                    suggestions.append(f"- {line}")
                suggestions.append("")
    except Exception as e:
        suggestions.append(f"(Could not check deadends: {e})\n")

    # 2. Tag-based link suggestions
    tag_suggestions = _find_tag_based_links(project)
    if tag_suggestions:
        suggestions.append("## Potential links (shared tags, not linked)\n")
        for suggestion in tag_suggestions:
            suggestions.append(f"- {suggestion}")
        suggestions.append("")

    if not suggestions:
        return "No suggestions. All notes look well-organized."

    return "\n".join(suggestions)


def _in_scope(path: str, project: str | None) -> bool:
    """Check if a path is in scope for the given project filter."""
    if not project:
        return True
    return f"projects/{project}/" in path


def _find_tag_based_links(project: str | None = None) -> list[str]:
    """Find notes that share tags but don't have each other in 'related'."""
    root_dir = kb_data_root()
    if project:
        search_dir = safe_resolve(root_dir, "projects", project)
    else:
        search_dir = root_dir

    if not search_dir.exists():
        return []

    notes: list[dict] = []
    for md_file in search_dir.rglob("*.md"):
        if md_file.name == "history.md":
            continue
        text = md_file.read_text(encoding="utf-8")
        fm = parse_frontmatter(text)
        if not fm:
            continue
        note_tags = fm.get("tags", [])
        if isinstance(note_tags, str):
            note_tags = [note_tags] if note_tags else []
        note_related = fm.get("related", [])
        if isinstance(note_related, str):
            note_related = [note_related] if note_related else []
        note_id = fm.get("id", "")
        if note_tags and note_id:
            notes.append({
                "id": note_id,
                "path": str(md_file.relative_to(root_dir)),
                "tags": set(note_tags),
                "related": set(note_related),
                "summary": fm.get("summary", ""),
            })

    tag_groups: dict[str, list[dict]] = defaultdict(list)
    for note in notes:
        for tag in note["tags"]:
            tag_groups[tag].append(note)

    suggestions = []
    seen_pairs: set[tuple[str, str]] = set()
    for tag, group in tag_groups.items():
        if len(group) < 2:
            continue
        for i, a in enumerate(group):
            for b in group[i + 1:]:
                pair = (min(a["id"], b["id"]), max(a["id"], b["id"]))
                if pair in seen_pairs:
                    continue
                seen_pairs.add(pair)
                if a["id"] not in b["related"] and b["id"] not in a["related"]:
                    suggestions.append(
                        f"[{tag}] {a['path']} ↔ {b['path']}"
                    )

    return suggestions
