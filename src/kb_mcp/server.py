"""kb MCP server."""

from mcp.server.fastmcp import Context, FastMCP

from kb_mcp.events.middleware import with_tool_events
from kb_mcp.tools.init import kb_init
from kb_mcp.tools.save import kb_adr, kb_draft, kb_gap, kb_knowledge, kb_session
from kb_mcp.tools.search import kb_search, kb_read
from kb_mcp.tools.lint import kb_lint
from kb_mcp.tools.organize import kb_organize
from kb_mcp.tools.graduate import kb_graduate

mcp = FastMCP("kb")

_init = with_tool_events("kb", "kb-mcp", "init", kb_init)
_adr = with_tool_events("kb", "kb-mcp", "adr", kb_adr)
_gap = with_tool_events("kb", "kb-mcp", "gap", kb_gap)
_knowledge = with_tool_events("kb", "kb-mcp", "knowledge", kb_knowledge)
_session = with_tool_events("kb", "kb-mcp", "session", kb_session)
_draft = with_tool_events("kb", "kb-mcp", "draft", kb_draft)
_search = with_tool_events("kb", "kb-mcp", "search", kb_search)
_read = with_tool_events("kb", "kb-mcp", "read", kb_read)
_lint = with_tool_events("kb", "kb-mcp", "lint", kb_lint)
_organize = with_tool_events("kb", "kb-mcp", "organize", kb_organize)
_graduate = with_tool_events("kb", "kb-mcp", "graduate", kb_graduate)

# --- Init ---

@mcp.tool()
def init(
    project: str,
    cwd: str | None = None,
    repo: str | None = None,
    ctx: Context | None = None,
) -> str:
    """Initialize a new project in the kb store (NOT in the current repository).

    Creates <kb-store>/projects/{project}/ with subdirectories (adr/, gap/,
    session-log/, knowledge/, draft/) and .kb-project.yml.

    Notes are stored in the external Obsidian Vault configured by `kb-mcp setup`,
    not in the current working directory or repository.

    If the project already exists, backfills .kb-project.yml if missing.

    Args:
        project: Project name to create
        cwd: Working directory — used ONLY to detect git remote for .kb-project.yml association, NOT as a save destination
        repo: Explicit repo identifier (e.g. github.com/owner/repo) — used for .kb-project.yml association
    """
    return _init(project=project, cwd=cwd, repo=repo, ctx=ctx)


# --- Save tools ---

@mcp.tool()
def adr(
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
    ctx: Context | None = None,
) -> str:
    """Save an Architecture Decision Record (ADR).

    Records a decision made during the project — the context,
    the decision itself, and why it was chosen over alternatives.

    When superseding a previous ADR, update the old one's status to 'superseded'
    and link them via 'related'.

    Notes are saved to the kb store (Obsidian Vault), not to the current repository.
    Project is auto-resolved from cwd/repo if not specified.
    cwd/repo are used only for project resolution, not as save destinations.
    """
    return _adr(
        slug=slug, summary=summary, content=content,
        ai_tool=ai_tool, ai_client=ai_client,
        project=project, cwd=cwd, repo=repo,
        tags=tags, related=related, status=status,
        ctx=ctx,
    )


@mcp.tool()
def gap(
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
    ctx: Context | None = None,
) -> str:
    """Save a gap record — what the user actually wanted vs what AI did.

    Use this when the user corrects AI behavior. Record:
    - What AI proposed or did
    - What the user actually wanted
    - Why the gap occurred
    - How to avoid it in the future

    Notes are saved to the kb store (Obsidian Vault), not to the current repository.
    Project is auto-resolved from cwd/repo if not specified.
    cwd/repo are used only for project resolution, not as save destinations.
    """
    return _gap(
        slug=slug, summary=summary, content=content,
        ai_tool=ai_tool, ai_client=ai_client,
        project=project, cwd=cwd, repo=repo,
        tags=tags, related=related,
        ctx=ctx,
    )


@mcp.tool()
def knowledge(
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
    ctx: Context | None = None,
) -> str:
    """Save a knowledge note — something learned during development.

    Records technical knowledge, patterns, gotchas, or insights
    worth preserving for future reference. No gap involved — just
    useful information encountered during work.

    Notes are saved to the kb store (Obsidian Vault), not to the current repository.
    Project is auto-resolved from cwd/repo if not specified.
    cwd/repo are used only for project resolution, not as save destinations.
    """
    return _knowledge(
        slug=slug, summary=summary, content=content,
        ai_tool=ai_tool, ai_client=ai_client,
        project=project, cwd=cwd, repo=repo,
        tags=tags, related=related,
        ctx=ctx,
    )


@mcp.tool()
def session(
    summary: str,
    content: str,
    ai_tool: str,
    ai_client: str | None = None,
    project: str | None = None,
    cwd: str | None = None,
    repo: str | None = None,
    tags: list[str] | None = None,
    related: list[str] | None = None,
    ctx: Context | None = None,
) -> str:
    """Save a session log — record of a working session.

    Created at compact/quit timing or on user request.
    Captures what was worked on, decisions made, gaps encountered,
    and notable context. Pay special attention to recording gaps.

    Notes are saved to the kb store (Obsidian Vault), not to the current repository.
    Project is auto-resolved from cwd/repo if not specified.
    cwd/repo are used only for project resolution, not as save destinations.
    """
    return _session(
        summary=summary, content=content,
        ai_tool=ai_tool, ai_client=ai_client,
        project=project, cwd=cwd, repo=repo,
        tags=tags, related=related,
        ctx=ctx,
    )


@mcp.tool()
def draft(
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
    ctx: Context | None = None,
) -> str:
    """Save a draft — an idea, a want-to-do, or a casual memo.

    If project is resolved (from explicit name, cwd, or repo),
    saves to the project's draft/ directory in the kb store.
    If project cannot be resolved, saves to inbox/ in the kb store.

    Notes are saved to the kb store (Obsidian Vault), not to the current repository.
    Project is auto-resolved from cwd/repo if not specified.
    cwd/repo are used only for project resolution, not as save destinations.
    """
    return _draft(
        slug=slug, summary=summary, content=content,
        ai_tool=ai_tool, ai_client=ai_client,
        project=project, cwd=cwd, repo=repo,
        tags=tags, related=related,
        ctx=ctx,
    )


# --- Reference tools ---

@mcp.tool()
async def search(
    query: str,
    project: str | None = None,
    tags: list[str] | None = None,
    note_type: str | None = None,
    limit: int = 20,
    ctx: Context | None = None,
) -> str:
    """Search kb notes by text with optional filters.

    Args:
        query: Search text
        project: Filter by project name
        tags: Filter by tags (notes must have all specified tags)
        note_type: Filter by type: adr, gap, knowledge, session-log
        limit: Max results (default 20)
    """
    return await _search(
        query=query, project=project, tags=tags, note_type=note_type, limit=limit,
        ctx=ctx,
    )


@mcp.tool()
async def read(id: str, ctx: Context | None = None) -> str:
    """Read a note by its ULID."""
    return await _read(id=id, ctx=ctx)


# --- Maintenance tools ---

@mcp.tool()
def lint(project: str | None = None, ctx: Context | None = None) -> str:
    """Check notes for rule compliance.

    Validates frontmatter fields, ULID format, timestamps,
    ai_tool values, filename conventions, and .kb-project.yml presence.
    """
    return _lint(project=project, ctx=ctx)


@mcp.tool()
async def organize(project: str | None = None, ctx: Context | None = None) -> str:
    """Discover and suggest missing links between notes.

    Finds orphan notes (no incoming links), dead-end notes (no outgoing links),
    and notes that share tags but aren't linked via 'related'.

    Optionally filter by project name.
    """
    return await _organize(project=project, ctx=ctx)


@mcp.tool()
def graduate(ctx: Context | None = None) -> str:
    """Propose promoting project notes to general/.

    Scans knowledge/ and gap/ across all projects to find:
    - Knowledge appearing in 2+ projects → general/knowledge/
    - Recurring gaps suggesting user preferences → general/requirements/

    Returns proposals only — does not perform writes.
    """
    return _graduate(ctx=ctx)


# --- Update tools ---

@mcp.tool()
def update_check() -> str:
    """Check if a newer version of kb-mcp is available on PyPI.

    Makes an outbound HTTPS request to pypi.org to fetch the latest version.
    No changes are made to the system.
    """
    from kb_mcp.update import current_version, latest_version, is_outdated

    cur = current_version()
    if cur is None:
        return "Cannot determine current version (dev install?)"

    latest, err = latest_version()
    if err:
        return f"Version check failed: {err}"
    if latest is None:
        return "Version check failed: unknown error"

    outdated = is_outdated(cur, latest)
    if outdated is None:
        return f"kb-mcp {cur} (current), {latest} (available) — version comparison failed"
    if outdated:
        return f"kb-mcp {cur} (current) → {latest} (available)\nUse kb_update_apply to upgrade."
    return f"kb-mcp {cur} — already up to date."


@mcp.tool()
def update_apply() -> str:
    """Upgrade kb-mcp to the latest version using uv.

    Downloads the package from a Python package index (outbound network access required).
    This is a subprocess call to uv, which manages its own network access
    independently of any AI client URL restrictions.

    Prerequisites:
    - kb-mcp must be installed via 'uv tool install'
    - uv must be available in PATH
    - No other kb_update_apply must be running on this host

    After upgrade, the MCP server process in THIS session remains on the old version.
    The new version takes effect on next session start (new MCP server process).
    """
    from kb_mcp.update import (
        current_version, latest_version, is_outdated,
        is_uv_managed, upgrade_lock, run_upgrade,
    )

    cur = current_version()
    if cur is None:
        return "Cannot determine current version (dev install?). Manual upgrade: uv tool upgrade kb-mcp"

    # Check if update is needed
    latest, err = latest_version()
    if err:
        return f"Version check failed: {err}"
    if latest is None:
        return "Version check failed: unknown error"

    outdated = is_outdated(cur, latest)
    if outdated is False:
        return f"kb-mcp {cur} — already up to date."
    if outdated is None:
        return f"Version comparison failed: {cur} vs {latest}"

    # Check if uv-managed
    managed, uv_or_reason = is_uv_managed()
    if not managed:
        return (
            f"Cannot auto-upgrade: {uv_or_reason}\n"
            f"Manual upgrade: uv tool upgrade kb-mcp (if uv-managed) "
            f"or pip install --upgrade kb-mcp"
        )

    uv_path = uv_or_reason  # When managed=True, second element is uv_path

    # Acquire lock
    with upgrade_lock() as acquired:
        if not acquired:
            return "Another session is currently upgrading kb-mcp. Try again later."

        success, msg = run_upgrade(uv_path)

    if success:
        return (
            f"✓ kb-mcp upgraded: {cur} → {latest}\n"
            f"⚠ This session's MCP server remains on {cur}.\n"
            f"  Start a new session or reconnect MCP to use {latest}."
        )
    return f"Upgrade failed: {msg}"


if __name__ == "__main__":
    mcp.run(transport="stdio")
