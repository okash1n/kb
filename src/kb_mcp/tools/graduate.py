"""kb_graduate — Propose promoting project notes to general/."""

from collections import defaultdict
from pathlib import Path

from kb_mcp.config import general_dir, kb_data_root, projects_dir
from kb_mcp.learning.policy_snapshot import load_policy_snapshots
from kb_mcp.note import parse_frontmatter


def kb_graduate() -> str:
    """Analyze notes across projects and propose promotions to general/.

    Scans knowledge/ and gap/ in all projects to find:
    - Knowledge that appears in 2+ projects (-> general/knowledge/)
    - Recurring gaps that suggest a user preference (-> general/requirements/)

    Returns proposals as readable markdown. Does NOT perform any writes.
    """
    project_root = projects_dir()
    if not project_root.exists():
        return "No projects found."

    snapshot_lines = _render_policy_snapshots()
    if snapshot_lines:
        return "\n".join(snapshot_lines)

    knowledge_notes: list[dict] = []
    gap_notes: list[dict] = []

    for project_dir in project_root.iterdir():
        if not project_dir.is_dir():
            continue
        project_name = project_dir.name

        knowledge_subdir = project_dir / "knowledge"
        if knowledge_subdir.exists():
            for md_file in knowledge_subdir.glob("*.md"):
                note = _read_note(md_file, project_name)
                if note:
                    knowledge_notes.append(note)

        gap_subdir = project_dir / "gap"
        if gap_subdir.exists():
            for md_file in gap_subdir.glob("*.md"):
                note = _read_note(md_file, project_name)
                if note:
                    gap_notes.append(note)

    proposals: list[str] = []

    knowledge_target = str(general_dir().relative_to(kb_data_root()) / "knowledge") + "/"
    knowledge_proposals = _find_cross_project_overlap(
        knowledge_notes, knowledge_target
    )
    if knowledge_proposals:
        proposals.append(f"## Knowledge candidates for {knowledge_target}\n")
        proposals.extend(knowledge_proposals)
        proposals.append("")

    requirements_target = str(general_dir().relative_to(kb_data_root()) / "requirements") + "/"
    gap_proposals = _find_cross_project_overlap(
        gap_notes, requirements_target
    )
    if gap_proposals:
        proposals.append(f"## Gap patterns for {requirements_target}\n")
        proposals.extend(gap_proposals)
        proposals.append("")

    if not proposals:
        return "No graduation candidates found. Need notes in 2+ projects to detect cross-project patterns."

    return "\n".join(proposals)


def _render_policy_snapshots() -> list[str]:
    snapshots = load_policy_snapshots()
    if not snapshots:
        return []
    lines = ["## Runtime policy snapshots", ""]
    for snapshot in snapshots:
        target = str(snapshot.get("target", "unknown"))
        count = int(snapshot.get("policy_count", 0))
        path = str(snapshot.get("path", ""))
        lines.append(f"- **{target}** ({count} policies)")
        lines.append(f"  - snapshot: {path}")
    return lines


def _read_note(md_file: Path, project: str) -> dict | None:
    """Read a note file and return structured data."""
    root_dir = kb_data_root()
    text = md_file.read_text(encoding="utf-8")
    fm = parse_frontmatter(text)
    if not fm:
        return None
    tags = fm.get("tags", [])
    if isinstance(tags, str):
        tags = [tags] if tags else []
    return {
        "id": fm.get("id", ""),
        "project": project,
        "path": str(md_file.relative_to(root_dir)),
        "tags": set(tags),
        "summary": fm.get("summary", ""),
    }


def _find_cross_project_overlap(
    notes: list[dict], target_dir: str
) -> list[str]:
    """Find notes sharing tags across different projects."""
    tag_projects: dict[str, dict[str, list[dict]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for note in notes:
        for tag in note["tags"]:
            tag_projects[tag][note["project"]].append(note)

    proposals = []
    for tag, projects in tag_projects.items():
        if len(projects) < 2:
            continue
        project_names = sorted(projects.keys())
        source_notes = []
        for proj, proj_notes in projects.items():
            for note in proj_notes:
                source_notes.append(f"  - [{proj}] {note['path']} — {note['summary']}")

        proposals.append(
            f"- **Tag: {tag}** (found in {', '.join(project_names)}) -> {target_dir}"
        )
        proposals.extend(source_notes)

    return proposals
