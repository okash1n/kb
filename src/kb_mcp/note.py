"""Note creation and update utilities."""

import hashlib
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from ulid import ULID
import yaml

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


def parse_markdown_note(text: str) -> tuple[dict, str]:
    """Parse markdown note into frontmatter dict and body text."""
    if not text.startswith("---\n"):
        return {}, text
    _, _, remainder = text.partition("---\n")
    frontmatter_block, separator, body = remainder.partition("\n---\n")
    if not separator:
        return {}, text
    data = yaml.safe_load(frontmatter_block) or {}
    if not isinstance(data, dict):
        raise ValueError("frontmatter must be a mapping")
    return dict(data), body.lstrip("\n").rstrip("\n")


def render_markdown_note(frontmatter: dict, body: str) -> str:
    """Render frontmatter + body into markdown text."""
    lines = ["---"]
    preferred = ["id", "summary", "ai_tool", "ai_client", "repo", "tags", "related", "status"]
    emitted: set[str] = set()
    for key in preferred + list(frontmatter.keys()):
        if key in emitted or key not in frontmatter:
            continue
        emitted.add(key)
        value = frontmatter[key]
        if value is None:
            continue
        if isinstance(value, list):
            lines.append(f"{key}: [{', '.join(str(item) for item in value)}]")
        else:
            lines.append(f"{key}: {value}")
    lines.append("---")
    return "\n".join(lines) + f"\n\n{body.rstrip()}\n"


def sha256_text(text: str) -> str:
    """Return sha256 of text."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def update_markdown_note(
    path: Path,
    *,
    frontmatter_patch: dict | None = None,
    body_replace: str | None = None,
) -> dict[str, str]:
    """Atomically update markdown note frontmatter/body."""
    original = path.read_text(encoding="utf-8")
    frontmatter, body = parse_markdown_note(original)
    note_id = str(frontmatter.get("id") or "")
    if not note_id:
        raise ValueError("target note is missing frontmatter id")
    merged = _merge_frontmatter(frontmatter, frontmatter_patch or {})
    if merged.get("id") != note_id:
        raise ValueError("note id cannot be changed")
    if frontmatter.get("created"):
        merged["created"] = frontmatter["created"]
    merged["updated"] = now_local()
    next_body = body if body_replace is None else body_replace
    before_sha256 = sha256_text(original)
    updated = render_markdown_note(merged, next_body)
    after_sha256 = sha256_text(updated)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(updated, encoding="utf-8")
    tmp.replace(path)
    from kb_mcp.vault_git import vault_git_sync

    vault_git_sync(path)
    return {
        "note_id": note_id,
        "note_path": str(path),
        "before_sha256": before_sha256,
        "after_sha256": after_sha256,
    }


def _merge_frontmatter(current: dict, patch: dict) -> dict:
    merged = dict(current)
    for key, value in patch.items():
        if key == "created":
            continue
        if key == "id" and value is not None and current.get("id") not in (None, value):
            raise ValueError("note id cannot be changed")
        if isinstance(value, list):
            existing = merged.get(key)
            items: list[str] = []
            if isinstance(existing, list):
                items.extend(str(item) for item in existing)
            elif existing not in (None, ""):
                items.append(str(existing))
            for item in value:
                item_str = str(item)
                if item_str not in items:
                    items.append(item_str)
            merged[key] = items
        else:
            merged[key] = value
    return merged


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
