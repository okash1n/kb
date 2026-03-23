"""kb MCP server."""

from mcp.server.fastmcp import FastMCP

from kb_mcp.tools.init import kb_init
from kb_mcp.tools.save import kb_adr, kb_draft, kb_gap, kb_knowledge, kb_session
from kb_mcp.tools.search import kb_search, kb_read
from kb_mcp.tools.lint import kb_lint
from kb_mcp.tools.organize import kb_organize
from kb_mcp.tools.graduate import kb_graduate

mcp = FastMCP("kb")

# --- Init ---

@mcp.tool()
def init(
    project: str,
    cwd: str | None = None,
    repo: str | None = None,
) -> str:
    """Initialize a new project in kb.

    Creates notes/projects/{project}/ with subdirectories and .kb-project.yml.
    If the project already exists, backfills .kb-project.yml if missing.

    Args:
        project: Project name
        cwd: Working directory (used to detect git remote for .kb-project.yml)
        repo: Explicit repo identifier (e.g. github.com/owner/repo)
    """
    return kb_init(project=project, cwd=cwd, repo=repo)


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
) -> str:
    """Save an Architecture Decision Record (ADR).

    Records a decision made during the project — the context,
    the decision itself, and why it was chosen over alternatives.

    When superseding a previous ADR, update the old one's status to 'superseded'
    and link them via 'related'.

    Project is auto-resolved from cwd/repo if not specified.
    """
    return kb_adr(
        slug=slug, summary=summary, content=content,
        ai_tool=ai_tool, ai_client=ai_client,
        project=project, cwd=cwd, repo=repo,
        tags=tags, related=related, status=status,
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
) -> str:
    """Save a gap record — what the user actually wanted vs what AI did.

    Use this when the user corrects AI behavior. Record:
    - What AI proposed or did
    - What the user actually wanted
    - Why the gap occurred
    - How to avoid it in the future

    Project is auto-resolved from cwd/repo if not specified.
    """
    return kb_gap(
        slug=slug, summary=summary, content=content,
        ai_tool=ai_tool, ai_client=ai_client,
        project=project, cwd=cwd, repo=repo,
        tags=tags, related=related,
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
) -> str:
    """Save a knowledge note — something learned during development.

    Records technical knowledge, patterns, gotchas, or insights
    worth preserving for future reference. No gap involved — just
    useful information encountered during work.

    Project is auto-resolved from cwd/repo if not specified.
    """
    return kb_knowledge(
        slug=slug, summary=summary, content=content,
        ai_tool=ai_tool, ai_client=ai_client,
        project=project, cwd=cwd, repo=repo,
        tags=tags, related=related,
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
) -> str:
    """Save a session log — record of a working session.

    Created at compact/quit timing or on user request.
    Captures what was worked on, decisions made, gaps encountered,
    and notable context. Pay special attention to recording gaps.

    Project is auto-resolved from cwd/repo if not specified.
    """
    return kb_session(
        summary=summary, content=content,
        ai_tool=ai_tool, ai_client=ai_client,
        project=project, cwd=cwd, repo=repo,
        tags=tags, related=related,
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
) -> str:
    """Save a draft — an idea, a want-to-do, or a casual memo.

    If project is resolved (from explicit name, cwd, or repo),
    saves to the project's draft/ directory.
    If project cannot be resolved, saves to inbox/.

    Project is auto-resolved from cwd/repo if not specified.
    """
    return kb_draft(
        slug=slug, summary=summary, content=content,
        ai_tool=ai_tool, ai_client=ai_client,
        project=project, cwd=cwd, repo=repo,
        tags=tags, related=related,
    )


# --- Reference tools ---

@mcp.tool()
async def search(
    query: str,
    project: str | None = None,
    tags: list[str] | None = None,
    note_type: str | None = None,
    limit: int = 20,
) -> str:
    """Search kb notes by text with optional filters.

    Args:
        query: Search text
        project: Filter by project name
        tags: Filter by tags (notes must have all specified tags)
        note_type: Filter by type: adr, gap, knowledge, session-log
        limit: Max results (default 20)
    """
    return await kb_search(
        query=query, project=project, tags=tags, note_type=note_type, limit=limit,
    )


@mcp.tool()
async def read(id: str) -> str:
    """Read a note by its ULID."""
    return await kb_read(id=id)


# --- Maintenance tools ---

@mcp.tool()
def lint(project: str | None = None) -> str:
    """Check notes for rule compliance.

    Validates frontmatter fields, ULID format, timestamps,
    ai_tool values, filename conventions, and .kb-project.yml presence.
    """
    return kb_lint(project=project)


@mcp.tool()
async def organize(project: str | None = None) -> str:
    """Discover and suggest missing links between notes.

    Finds orphan notes (no incoming links), dead-end notes (no outgoing links),
    and notes that share tags but aren't linked via 'related'.

    Optionally filter by project name.
    """
    return await kb_organize(project=project)


@mcp.tool()
def graduate() -> str:
    """Propose promoting project notes to general/.

    Scans knowledge/ and gap/ across all projects to find:
    - Knowledge appearing in 2+ projects → general/knowledge/
    - Recurring gaps suggesting user preferences → general/requirements/

    Returns proposals only — does not perform writes.
    """
    return kb_graduate()


if __name__ == "__main__":
    mcp.run(transport="stdio")
