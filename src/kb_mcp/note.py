"""Note creation utilities."""

from datetime import datetime
from zoneinfo import ZoneInfo

from ulid import ULID

from kb_mcp.config import timezone


def generate_ulid() -> str:
    """Generate a new ULID string (uppercase)."""
    return str(ULID()).upper()


def now_local() -> str:
    """Return current local time as ISO 8601 with timezone offset."""
    dt = datetime.now(ZoneInfo(timezone()))
    return dt.isoformat(timespec="minutes")


def now_local_filename() -> str:
    """Return current local time as yyyymmdd-hhmm for session-log filenames."""
    dt = datetime.now(ZoneInfo(timezone()))
    return dt.strftime("%Y%m%d-%H%M")


def now_jst() -> str:
    """Backward-compatible alias for now_local()."""
    return now_local()


def now_jst_filename() -> str:
    """Backward-compatible alias for now_local_filename()."""
    return now_local_filename()


def slugify(text: str) -> str:
    """Convert text to a URL-friendly slug.

    Keeps ascii lowercase, digits, and hyphens.
    """
    import re
    slug = text.lower().strip()
    slug = re.sub(r"[^a-z0-9\s-]", "", slug)
    slug = re.sub(r"[\s_]+", "-", slug)
    slug = re.sub(r"-+", "-", slug)
    slug = slug.strip("-")
    return slug or "untitled"


def build_filename(*, slug: str, ulid: str) -> str:
    """Build filename: {slug}--{ULID}.md"""
    return f"{slug}--{ulid}.md"


def build_session_filename(*, ulid: str) -> str:
    """Build session-log filename: {yyyymmdd-hhmm}--{ULID}.md"""
    ts = now_local_filename()
    return f"{ts}--{ulid}.md"


def parse_frontmatter(text: str) -> dict | None:
    """Extract frontmatter as a dict from markdown text."""
    if not text.startswith("---"):
        return None
    parts = text.split("---", 2)
    if len(parts) < 3:
        return None

    fm = {}
    for line in parts[1].strip().split("\n"):
        if ":" in line:
            key, _, value = line.partition(":")
            key = key.strip()
            value = value.strip()
            if value.startswith("[") and value.endswith("]"):
                value = [v.strip() for v in value[1:-1].split(",") if v.strip()]
            fm[key] = value
    return fm


def build_frontmatter(
    *,
    ulid: str,
    summary: str,
    ai_tool: str,
    ai_client: str | None = None,
    repo: str | None = None,
    tags: list[str] | None = None,
    related: list[str] | None = None,
    status: str | None = None,
    extra_fields: dict[str, str] | None = None,
) -> str:
    """Build YAML frontmatter string."""
    now = now_local()
    lines = [
        "---",
        f"id: {ulid}",
        f"summary: {summary}",
        f"ai_tool: {ai_tool}",
    ]
    if ai_client:
        lines.append(f"ai_client: {ai_client}")
    if repo:
        lines.append(f"repo: {repo}")
    if tags:
        tag_str = ", ".join(tags)
        lines.append(f"tags: [{tag_str}]")
    if related:
        rel_str = ", ".join(related)
        lines.append(f"related: [{rel_str}]")
    if status:
        lines.append(f"status: {status}")
    if extra_fields:
        for key, value in extra_fields.items():
            lines.append(f"{key}: {value}")
    lines.extend([
        f"created: {now}",
        f"updated: {now}",
        "---",
    ])
    return "\n".join(lines)
