"""kb_search, kb_read — Reference tools."""

import json
import re

from kb_mcp.config import kb_data_root, projects_dir, safe_resolve
from kb_mcp.input_normalization import normalize_string_list
from kb_mcp import obsidian

ULID_PATTERN = re.compile(r"^[0-9A-Z]{26}$")


async def kb_search(
    query: str,
    project: str | None = None,
    tags: list[str] | str | None = None,
    note_type: str | None = None,
    limit: int = 20,
) -> str:
    """Search notes by text, with optional filters.

    Args:
        query: Search text
        project: Filter by project name
        tags: Filter by tags (notes must have all specified tags)
        note_type: Filter by type (adr, gap, knowledge, session-log)
        limit: Max results (default 20)
    """
    # Validate project name via safe_resolve (prevents traversal + normalizes)
    if project:
        projects_root = projects_dir().resolve()
        resolved = safe_resolve(projects_root, project).resolve()
        rel_project = resolved.relative_to(projects_root)
        project = str(rel_project)

    # Build path filter
    search_path = None
    if project and note_type:
        search_path = f"projects/{project}/{note_type}"
    elif project:
        search_path = f"projects/{project}"
    elif note_type:
        pass

    result = await obsidian.search(query=query, path=search_path, limit=limit)

    # If tags filter specified, post-filter results
    normalized_tags = normalize_string_list(tags)
    if normalized_tags and result:
        try:
            data = json.loads(result)
            filtered = []
            for item in data:
                filepath = item.get("path", item.get("file", ""))
                try:
                    props = await obsidian.run("properties", f"path={filepath}", "format=json")
                    props_data = json.loads(props)
                    note_tags = normalize_string_list(props_data.get("tags", [])) or []
                    if all(t in note_tags for t in normalized_tags):
                        filtered.append(item)
                except Exception:
                    continue
            return json.dumps(filtered, ensure_ascii=False, indent=2)
        except json.JSONDecodeError:
            pass

    return result


async def kb_read(id: str) -> str:
    """Read a note by its ULID.

    Searches for a file containing the ULID in its filename,
    then reads its content via Obsidian CLI.
    """
    if not ULID_PATTERN.match(id):
        return f"Invalid ULID format: {id}"

    root_dir = kb_data_root()

    for md_file in root_dir.rglob(f"*--{id}.md"):
        rel_path = md_file.relative_to(root_dir)
        content = await obsidian.read(path=str(rel_path))
        return content

    return f"Note not found: {id}"
