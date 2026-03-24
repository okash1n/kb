"""Save tools — kb_adr, kb_gap, kb_knowledge, kb_session, kb_draft."""

from pathlib import Path

from kb_mcp.config import inbox_dir, kb_data_root, projects_dir, safe_resolve
from kb_mcp.vault_git import vault_git_sync
from kb_mcp.note import (
    build_filename,
    build_frontmatter,
    build_session_filename,
    generate_ulid,
    slugify,
)
from kb_mcp.resolver import resolve_project


def _resolve_or_error(
    project: str | None,
    cwd: str | None,
    repo: str | None,
) -> tuple[str, str | None]:
    """Resolve project, raising ValueError if not found."""
    project_name, repo_id = resolve_project(project=project, cwd=cwd, repo=repo)
    if not project_name:
        raise ValueError(
            "Could not resolve kb project. "
            "Specify 'project' explicitly or run kb_init first."
        )
    return project_name, repo_id


def _ensure_project_dir(project: str, subdir: str) -> Path:
    """Ensure project subdirectory exists, return its path."""
    d = safe_resolve(projects_dir(), project, subdir)
    if not d.exists():
        raise ValueError(
            f"Project '{project}' not initialized. Run kb_init first."
        )
    return d


def _write_note(
    *,
    project: str,
    subdir: str,
    slug: str,
    summary: str,
    content: str,
    ai_tool: str,
    ai_client: str | None = None,
    repo: str | None = None,
    tags: list[str] | None = None,
    related: list[str] | None = None,
    status: str | None = None,
    extra_fields: dict[str, str] | None = None,
) -> str:
    """Write a note file and return confirmation."""
    d = _ensure_project_dir(project, subdir)
    ulid = generate_ulid()
    filename = build_filename(slug=slugify(slug), ulid=ulid)
    fm = build_frontmatter(
        ulid=ulid,
        summary=summary,
        ai_tool=ai_tool,
        ai_client=ai_client,
        repo=repo,
        tags=tags,
        related=related,
        status=status,
        extra_fields=extra_fields,
    )
    filepath = d / filename
    filepath.write_text(f"{fm}\n\n{content}\n", encoding="utf-8")
    rel = filepath.resolve().relative_to(kb_data_root().resolve())
    msg = f"Saved: {rel} (id: {ulid})"
    git_msg = vault_git_sync(filepath)
    if git_msg:
        msg += f"\n{git_msg}"
    return msg


def kb_adr(
    slug: str,
    summary: str,
    content: str,
    ai_tool: str,
    ai_client: str | None = None,
    project: str | None = None,
    cwd: str | None = None,
    repo: str | None = None,
    tags: list[str] | None = None,
    related: list[str] | None = None,
    status: str = "accepted",
    extra_fields: dict[str, str] | None = None,
) -> str:
    """Save an Architecture Decision Record.

    Records a decision made during the project — the context, the decision itself,
    and why it was chosen over alternatives.

    When a decision supersedes a previous ADR, set the old ADR's status to 'superseded'
    and link them via 'related'.
    """
    project_name, repo_id = _resolve_or_error(project, cwd, repo)
    return _write_note(
        project=project_name,
        subdir="adr",
        slug=slug,
        summary=summary,
        content=content,
        ai_tool=ai_tool,
        ai_client=ai_client,
        repo=repo_id,
        tags=tags,
        related=related,
        status=status,
        extra_fields=extra_fields,
    )


def kb_gap(
    slug: str,
    summary: str,
    content: str,
    ai_tool: str,
    ai_client: str | None = None,
    project: str | None = None,
    cwd: str | None = None,
    repo: str | None = None,
    tags: list[str] | None = None,
    related: list[str] | None = None,
    extra_fields: dict[str, str] | None = None,
) -> str:
    """Save a gap record — what the user actually wanted vs what AI did.

    Records situations where AI's output didn't match the user's intent:
    - What AI proposed or did
    - What the user actually wanted
    - Why the gap occurred
    - How to avoid it in the future
    """
    project_name, repo_id = _resolve_or_error(project, cwd, repo)
    return _write_note(
        project=project_name,
        subdir="gap",
        slug=slug,
        summary=summary,
        content=content,
        ai_tool=ai_tool,
        ai_client=ai_client,
        repo=repo_id,
        tags=tags,
        related=related,
        extra_fields=extra_fields,
    )


def kb_knowledge(
    slug: str,
    summary: str,
    content: str,
    ai_tool: str,
    ai_client: str | None = None,
    project: str | None = None,
    cwd: str | None = None,
    repo: str | None = None,
    tags: list[str] | None = None,
    related: list[str] | None = None,
    extra_fields: dict[str, str] | None = None,
) -> str:
    """Save a knowledge note — something learned during development.

    Records technical knowledge, patterns, gotchas, or insights
    that are worth preserving for future reference.
    """
    project_name, repo_id = _resolve_or_error(project, cwd, repo)
    return _write_note(
        project=project_name,
        subdir="knowledge",
        slug=slug,
        summary=summary,
        content=content,
        ai_tool=ai_tool,
        ai_client=ai_client,
        repo=repo_id,
        tags=tags,
        related=related,
        extra_fields=extra_fields,
    )


def kb_session(
    summary: str,
    content: str,
    ai_tool: str,
    ai_client: str | None = None,
    project: str | None = None,
    cwd: str | None = None,
    repo: str | None = None,
    tags: list[str] | None = None,
    related: list[str] | None = None,
    extra_fields: dict[str, str] | None = None,
) -> str:
    """Save a session log — record of a working session.

    Automatically generated at compact/quit timing or on user request.
    Captures what was worked on, decisions made, gaps encountered,
    and any notable context from the session.
    """
    project_name, repo_id = _resolve_or_error(project, cwd, repo)
    d = _ensure_project_dir(project_name, "session-log")
    ulid = generate_ulid()
    filename = build_session_filename(ulid=ulid)
    fm = build_frontmatter(
        ulid=ulid,
        summary=summary,
        ai_tool=ai_tool,
        ai_client=ai_client,
        repo=repo_id,
        tags=tags,
        related=related,
        extra_fields=extra_fields,
    )
    filepath = d / filename
    filepath.write_text(f"{fm}\n\n{content}\n", encoding="utf-8")
    filepath.chmod(0o444)  # read-only — session logs are immutable
    rel = filepath.resolve().relative_to(kb_data_root().resolve())
    msg = f"Saved: {rel} (id: {ulid})"
    git_msg = vault_git_sync(filepath)
    if git_msg:
        msg += f"\n{git_msg}"
    return msg


def kb_draft(
    slug: str,
    summary: str,
    content: str,
    ai_tool: str,
    ai_client: str | None = None,
    project: str | None = None,
    cwd: str | None = None,
    repo: str | None = None,
    tags: list[str] | None = None,
    related: list[str] | None = None,
    extra_fields: dict[str, str] | None = None,
) -> str:
    """Save a draft — an idea, a want-to-do, or a casual memo.

    If project is resolved, saves to notes/projects/{project}/draft/.
    If project cannot be resolved, saves to notes/inbox/.
    """
    project_name, repo_id = resolve_project(project=project, cwd=cwd, repo=repo)

    if project_name:
        d = safe_resolve(projects_dir(), project_name, "draft")
    else:
        d = inbox_dir()
    d.mkdir(parents=True, exist_ok=True)

    ulid = generate_ulid()
    filename = build_filename(slug=slugify(slug), ulid=ulid)
    fm = build_frontmatter(
        ulid=ulid,
        summary=summary,
        ai_tool=ai_tool,
        ai_client=ai_client,
        repo=repo_id,
        tags=tags,
        related=related,
        extra_fields=extra_fields,
    )
    filepath = d / filename
    filepath.write_text(f"{fm}\n\n{content}\n", encoding="utf-8")
    rel = filepath.resolve().relative_to(kb_data_root().resolve())
    msg = f"Saved: {rel} (id: {ulid})"
    git_msg = vault_git_sync(filepath)
    if git_msg:
        msg += f"\n{git_msg}"
    return msg
