"""Project resolver — resolve kb project name from context."""

import subprocess
from pathlib import Path

from kb_mcp.config import projects_dir, safe_resolve

try:
    import yaml as _yaml
    _has_yaml = True
except ImportError:
    _has_yaml = False


def _parse_kb_project_yml(path: Path) -> list[str]:
    """Read .kb-project.yml and return repos list."""
    if not path.exists():
        return []
    text = path.read_text(encoding="utf-8")
    if _has_yaml:
        data = _yaml.safe_load(text)
        if not data or not isinstance(data, dict):
            return []
        raw_repos = data.get("repos", [])
        if isinstance(raw_repos, str):
            iterable = [raw_repos]
        elif isinstance(raw_repos, (list, tuple, set)):
            iterable = raw_repos
        else:
            return []
        return [str(item).strip() for item in iterable if isinstance(item, str) and item.strip()]
    # Simple fallback parser for repos list
    repos = []
    in_repos = False
    for line in text.split("\n"):
        stripped = line.strip()
        if stripped == "repos:":
            in_repos = True
            continue
        if in_repos:
            if stripped.startswith("- "):
                repos.append(stripped[2:].strip())
            elif stripped and not stripped.startswith("#"):
                break
    return repos


def _git_remote_url(cwd: str) -> str | None:
    """Get normalized git remote origin URL from a directory."""
    try:
        result = subprocess.run(
            ["git", "remote", "get-url", "origin"],
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode != 0:
            return None
        return _normalize_remote_url(result.stdout.strip())
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return None


def _normalize_remote_url(url: str) -> str:
    """Normalize git remote URL to github.com/owner/repo format."""
    # Remove protocol prefixes
    for prefix in ("https://", "http://", "ssh://"):
        if url.startswith(prefix):
            url = url[len(prefix):]
    # ssh shorthand: git@github.com:owner/repo.git
    if url.startswith("git@"):
        url = url.replace("git@", "", 1).replace(":", "/", 1)
    # Remove .git suffix
    if url.endswith(".git"):
        url = url[:-4]
    # Remove trailing slash
    url = url.rstrip("/")
    return url


def _find_project_by_repo(repo_id: str) -> str | None:
    """Find project name by matching repo identifier against .kb-project.yml files."""
    base_dir = projects_dir()
    if not base_dir.exists():
        return None
    for project_dir in base_dir.iterdir():
        if not project_dir.is_dir():
            continue
        yml_path = project_dir / ".kb-project.yml"
        repos = _parse_kb_project_yml(yml_path)
        if repo_id in repos:
            return project_dir.name
    return None


def _find_project_by_repo_fallback(repo_id: str) -> str | None:
    """Fallback: find project by scanning note frontmatter repo fields.

    Used when .kb-project.yml is missing (pre-Phase 1 projects).
    """
    base_dir = projects_dir()
    if not base_dir.exists():
        return None
    from kb_mcp.note import parse_frontmatter
    for project_dir in base_dir.iterdir():
        if not project_dir.is_dir():
            continue
        # Skip if .kb-project.yml exists (primary method handles it)
        if (project_dir / ".kb-project.yml").exists():
            continue
        for md_file in project_dir.rglob("*.md"):
            if md_file.name == "history.md":
                continue
            try:
                text = md_file.read_text(encoding="utf-8")
            except OSError:
                continue
            fm = parse_frontmatter(text)
            if fm and fm.get("repo") == repo_id:
                return project_dir.name
    return None


def resolve_project(
    project: str | None = None,
    cwd: str | None = None,
    repo: str | None = None,
) -> tuple[str | None, str | None]:
    """Resolve kb project name and repo identifier.

    Args:
        project: Explicit project name (highest priority)
        cwd: Working directory (used to detect git remote)
        repo: Explicit repo identifier (e.g. github.com/owner/repo)

    Returns:
        (project_name_or_none, repo_identifier_or_none)

    Raises:
        ValueError: if explicit project is given but doesn't exist
    """
    # Resolve repo identifier from inputs
    repo_id = None
    if repo:
        repo_id = _normalize_remote_url(repo)
    elif cwd:
        repo_id = _git_remote_url(cwd)

    # Explicit project: validate existence
    if project:
        base_dir = projects_dir()
        project_dir = safe_resolve(base_dir, project)
        if not project_dir.exists():
            raise ValueError(
                f"Project '{project}' not found in {base_dir}. "
                f"Run kb_init first."
            )
        # If we don't have repo_id yet, try to get it from cwd
        if not repo_id and cwd:
            repo_id = _git_remote_url(cwd)
        return (project, repo_id)

    # No explicit project: try to resolve from repo
    if repo_id:
        # Primary: match against .kb-project.yml
        found = _find_project_by_repo(repo_id)
        if found:
            return (found, repo_id)
        # Fallback: match against note frontmatter (pre-Phase 1 projects)
        found = _find_project_by_repo_fallback(repo_id)
        if found:
            return (found, repo_id)
        return (None, repo_id)

    return (None, None)
