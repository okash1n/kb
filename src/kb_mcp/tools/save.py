"""Save tools — kb_adr, kb_gap, kb_knowledge, kb_session, kb_draft."""

from pathlib import Path
from typing import Callable

from kb_mcp.config import inbox_dir, kb_data_root, projects_dir, safe_resolve
from kb_mcp.events.request_context import REQUEST_CONTEXT
from kb_mcp.input_normalization import normalize_string_list
from kb_mcp.vault_git import vault_git_sync
from kb_mcp.note import (
    build_filename,
    build_frontmatter,
    build_session_filename,
    generate_ulid,
    slugify,
)
from kb_mcp.resolver import resolve_project

SaveFunc = Callable[..., str]


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
    slug: str | None,
    summary: str,
    content: str,
    ai_tool: str,
    ai_client: str | None = None,
    repo: str | None = None,
    tags: list[str] | str | None = None,
    related: list[str] | str | None = None,
    status: str | None = None,
    extra_fields: dict[str, str] | None = None,
    fixed_ulid: str | None = None,
    fixed_filename: str | None = None,
) -> str:
    """Write a note file and return confirmation."""
    d = _ensure_project_dir(project, subdir)
    ulid = fixed_ulid or generate_ulid()
    normalized_tags = normalize_string_list(tags)
    normalized_related = normalize_string_list(related)
    effective_slug = slug or summary
    filename = fixed_filename or build_filename(slug=slugify(effective_slug), ulid=ulid)
    context = REQUEST_CONTEXT.get()
    note_extra_fields = dict(extra_fields or {})
    if context and context.get("save_request_id"):
        note_extra_fields.setdefault("save_request_id", context["save_request_id"])
    fm = build_frontmatter(
        ulid=ulid,
        summary=summary,
        ai_tool=ai_tool,
        ai_client=ai_client,
        repo=repo,
        tags=normalized_tags,
        related=normalized_related,
        status=status,
        extra_fields=note_extra_fields or None,
    )
    filepath = d / filename
    filepath.write_text(f"{fm}\n\n{content}\n", encoding="utf-8")
    _update_request_context(context, ulid=ulid, filepath=filepath, note_type=subdir)
    rel = filepath.resolve().relative_to(kb_data_root().resolve())
    msg = f"Saved: {rel} (id: {ulid})"
    git_msg = vault_git_sync(filepath)
    if git_msg:
        msg += f"\n{git_msg}"
    return msg


def kb_adr(
    slug: str | None = None,
    summary: str | None = None,
    content: str | None = None,
    ai_tool: str | None = None,
    ai_client: str | None = None,
    project: str | None = None,
    cwd: str | None = None,
    repo: str | None = None,
    tags: list[str] | str | None = None,
    related: list[str] | str | None = None,
    status: str = "accepted",
    extra_fields: dict[str, str] | None = None,
    fixed_ulid: str | None = None,
    fixed_filename: str | None = None,
) -> str:
    """Save an Architecture Decision Record.

    Records a decision made during the project — the context, the decision itself,
    and why it was chosen over alternatives.

    When a decision supersedes a previous ADR, set the old ADR's status to 'superseded'
    and link them via 'related'.
    """
    if summary is None or content is None or ai_tool is None:
        raise TypeError("summary, content, and ai_tool are required")
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
        fixed_ulid=fixed_ulid,
        fixed_filename=fixed_filename,
    )


def kb_gap(
    slug: str | None = None,
    summary: str | None = None,
    content: str | None = None,
    ai_tool: str | None = None,
    ai_client: str | None = None,
    project: str | None = None,
    cwd: str | None = None,
    repo: str | None = None,
    tags: list[str] | str | None = None,
    related: list[str] | str | None = None,
    extra_fields: dict[str, str] | None = None,
    fixed_ulid: str | None = None,
    fixed_filename: str | None = None,
) -> str:
    """Save a gap record — what the user actually wanted vs what AI did.

    Records situations where AI's output didn't match the user's intent:
    - What AI proposed or did
    - What the user actually wanted
    - Why the gap occurred
    - How to avoid it in the future
    """
    if summary is None or content is None or ai_tool is None:
        raise TypeError("summary, content, and ai_tool are required")
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
        fixed_ulid=fixed_ulid,
        fixed_filename=fixed_filename,
    )


def kb_knowledge(
    slug: str | None = None,
    summary: str | None = None,
    content: str | None = None,
    ai_tool: str | None = None,
    ai_client: str | None = None,
    project: str | None = None,
    cwd: str | None = None,
    repo: str | None = None,
    tags: list[str] | str | None = None,
    related: list[str] | str | None = None,
    extra_fields: dict[str, str] | None = None,
    fixed_ulid: str | None = None,
    fixed_filename: str | None = None,
) -> str:
    """Save a knowledge note — something learned during development.

    Records technical knowledge, patterns, gotchas, or insights
    that are worth preserving for future reference.
    """
    if summary is None or content is None or ai_tool is None:
        raise TypeError("summary, content, and ai_tool are required")
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
        fixed_ulid=fixed_ulid,
        fixed_filename=fixed_filename,
    )


def kb_session(
    summary: str,
    content: str,
    ai_tool: str,
    ai_client: str | None = None,
    project: str | None = None,
    cwd: str | None = None,
    repo: str | None = None,
    tags: list[str] | str | None = None,
    related: list[str] | str | None = None,
    extra_fields: dict[str, str] | None = None,
    fixed_ulid: str | None = None,
    fixed_filename: str | None = None,
) -> str:
    """Save a session log — record of a working session.

    Automatically generated at compact/quit timing or on user request.
    Captures what was worked on, decisions made, gaps encountered,
    and any notable context from the session.
    """
    project_name, repo_id = _resolve_or_error(project, cwd, repo)
    d = _ensure_project_dir(project_name, "session-log")
    ulid = fixed_ulid or generate_ulid()
    filename = fixed_filename or build_session_filename(ulid=ulid)
    normalized_tags = normalize_string_list(tags)
    normalized_related = normalize_string_list(related)
    context = REQUEST_CONTEXT.get()
    note_extra_fields = dict(extra_fields or {})
    if context and context.get("save_request_id"):
        note_extra_fields.setdefault("save_request_id", context["save_request_id"])
    fm = build_frontmatter(
        ulid=ulid,
        summary=summary,
        ai_tool=ai_tool,
        ai_client=ai_client,
        repo=repo_id,
        tags=normalized_tags,
        related=normalized_related,
        extra_fields=note_extra_fields or None,
    )
    filepath = d / filename
    filepath.write_text(f"{fm}\n\n{content}\n", encoding="utf-8")
    filepath.chmod(0o444)  # read-only — session logs are immutable
    _update_request_context(context, ulid=ulid, filepath=filepath, note_type="session-log")
    rel = filepath.resolve().relative_to(kb_data_root().resolve())
    msg = f"Saved: {rel} (id: {ulid})"
    git_msg = vault_git_sync(filepath)
    if git_msg:
        msg += f"\n{git_msg}"
    return msg


def kb_draft(
    slug: str | None = None,
    summary: str | None = None,
    content: str | None = None,
    ai_tool: str | None = None,
    ai_client: str | None = None,
    project: str | None = None,
    cwd: str | None = None,
    repo: str | None = None,
    tags: list[str] | str | None = None,
    related: list[str] | str | None = None,
    extra_fields: dict[str, str] | None = None,
) -> str:
    """Save a draft — an idea, a want-to-do, or a casual memo.

    If project is resolved, saves to notes/projects/{project}/draft/.
    If project cannot be resolved, saves to notes/inbox/.
    """
    if summary is None or content is None or ai_tool is None:
        raise TypeError("summary, content, and ai_tool are required")
    project_name, repo_id = resolve_project(project=project, cwd=cwd, repo=repo)

    if project_name:
        d = safe_resolve(projects_dir(), project_name, "draft")
    else:
        d = inbox_dir()
    d.mkdir(parents=True, exist_ok=True)

    ulid = generate_ulid()
    normalized_tags = normalize_string_list(tags)
    normalized_related = normalize_string_list(related)
    effective_slug = slug or summary
    filename = build_filename(slug=slugify(effective_slug), ulid=ulid)
    context = REQUEST_CONTEXT.get()
    note_extra_fields = dict(extra_fields or {})
    if context and context.get("save_request_id"):
        note_extra_fields.setdefault("save_request_id", context["save_request_id"])
    fm = build_frontmatter(
        ulid=ulid,
        summary=summary,
        ai_tool=ai_tool,
        ai_client=ai_client,
        repo=repo_id,
        tags=normalized_tags,
        related=normalized_related,
        extra_fields=note_extra_fields or None,
    )
    filepath = d / filename
    filepath.write_text(f"{fm}\n\n{content}\n", encoding="utf-8")
    _update_request_context(context, ulid=ulid, filepath=filepath, note_type="draft")
    rel = filepath.resolve().relative_to(kb_data_root().resolve())
    msg = f"Saved: {rel} (id: {ulid})"
    git_msg = vault_git_sync(filepath)
    if git_msg:
        msg += f"\n{git_msg}"
    return msg


def save_note_by_type(
    *,
    note_type: str,
    summary: str,
    content: str,
    ai_tool: str,
    ai_client: str | None = None,
    project: str | None = None,
    cwd: str | None = None,
    repo: str | None = None,
    slug: str | None = None,
    tags: list[str] | str | None = None,
    related: list[str] | str | None = None,
    status: str | None = None,
    extra_fields: dict[str, str] | None = None,
    fixed_ulid: str | None = None,
    fixed_filename: str | None = None,
) -> str:
    kwargs = {
        "summary": summary,
        "content": content,
        "ai_tool": ai_tool,
        "ai_client": ai_client,
        "project": project,
        "cwd": cwd,
        "repo": repo,
        "tags": tags,
        "related": related,
        "extra_fields": extra_fields,
        "fixed_ulid": fixed_ulid,
        "fixed_filename": fixed_filename,
    }
    save_func: SaveFunc
    if note_type == "adr":
        save_func = kb_adr
        kwargs["slug"] = slug or summary
        if status is not None:
            kwargs["status"] = status
    elif note_type == "gap":
        save_func = kb_gap
        kwargs["slug"] = slug or summary
    elif note_type == "knowledge":
        save_func = kb_knowledge
        kwargs["slug"] = slug or summary
    elif note_type == "session-log":
        save_func = kb_session
    else:
        raise ValueError(f"unsupported note_type: {note_type}")
    return save_func(**kwargs)


def _update_request_context(context: dict | None, *, ulid: str, filepath: Path, note_type: str) -> None:
    if context is None:
        return
    context["saved_note_id"] = ulid
    context["saved_note_path"] = str(filepath)
    context["saved_note_type"] = note_type
