"""kb_init — Project initialization."""

from kb_mcp.config import PROJECT_SUBDIRS, kb_data_root, projects_dir, safe_resolve
from kb_mcp.resolver import _git_remote_url, _normalize_remote_url


def _write_kb_project_yml(project_dir, repos: list[str]) -> None:
    """Write .kb-project.yml with repos list."""
    yml_path = project_dir / ".kb-project.yml"
    lines = ["repos:"]
    for r in repos:
        lines.append(f"  - {r}")
    yml_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def kb_init(
    project: str,
    cwd: str | None = None,
    repo: str | None = None,
) -> str:
    """Initialize a new project directory structure.

    Creates notes/projects/{project}/ with subdirectories and .kb-project.yml.

    If the project already exists but .kb-project.yml is missing, backfills it.

    Args:
        project: Project name
        cwd: Working directory (used to detect git remote for .kb-project.yml)
        repo: Explicit repo identifier
    """
    project_dir = safe_resolve(projects_dir(), project)
    yml_path = project_dir / ".kb-project.yml"

    # Resolve repo identifier
    repos = []
    if repo:
        repos.append(_normalize_remote_url(repo))
    elif cwd:
        remote = _git_remote_url(cwd)
        if remote:
            repos.append(remote)

    if project_dir.exists():
        # Backfill .kb-project.yml if missing
        if not yml_path.exists() and repos:
            _write_kb_project_yml(project_dir, repos)
            return (
                f"Project '{project}' already exists. "
                f"Created .kb-project.yml with repos: {repos}"
            )
        # Update repos if new repo is provided and not already listed
        if yml_path.exists() and repos:
            from kb_mcp.resolver import _parse_kb_project_yml
            existing_repos = _parse_kb_project_yml(yml_path)
            new_repos = [r for r in repos if r not in existing_repos]
            if new_repos:
                _write_kb_project_yml(project_dir, existing_repos + new_repos)
                return (
                    f"Project '{project}' already exists. "
                    f"Added repos: {new_repos}"
                )
        rel = project_dir.relative_to(kb_data_root())
        return f"Project '{project}' already exists at {rel}"

    # Create new project
    for subdir in PROJECT_SUBDIRS:
        (project_dir / subdir).mkdir(parents=True, exist_ok=True)

    if repos:
        _write_kb_project_yml(project_dir, repos)

    return (
        f"Created project '{project}' with subdirectories: "
        f"{', '.join(PROJECT_SUBDIRS)}"
        + (f" and repos: {repos}" if repos else "")
    )
