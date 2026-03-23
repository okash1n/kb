"""Obsidian CLI wrapper."""

import asyncio
import os
import platform
import shutil
from pathlib import Path

from kb_mcp.config import load_config


def _detect_obsidian_cli() -> str | None:
    """Detect Obsidian CLI path from config, env, PATH, or platform default."""
    config_value = load_config().get("obsidian_cli", "")
    if config_value and config_value != "auto":
        if Path(config_value).exists():
            return config_value
        # Config value is invalid, fall through to other detection methods

    if env := os.environ.get("OBSIDIAN_CLI"):
        return env

    if found := shutil.which("obsidian-cli"):
        return found

    if platform.system() == "Darwin":
        default = "/Applications/Obsidian.app/Contents/MacOS/Obsidian"
        if Path(default).exists():
            return default

    return None


async def run(*args: str, timeout: float = 10.0) -> str:
    """Run an Obsidian CLI command and return stdout."""
    obsidian_cli = _detect_obsidian_cli()
    if not obsidian_cli:
        raise RuntimeError(
            "Obsidian CLI not found. Check config 'obsidian_cli', "
            "$OBSIDIAN_CLI, PATH, or platform default installation."
        )

    try:
        proc = await asyncio.create_subprocess_exec(
            obsidian_cli,
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(f"Obsidian CLI not found: {obsidian_cli}") from exc

    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        proc.kill()
        raise RuntimeError("Obsidian CLI timed out. Is Obsidian running?")

    if proc.returncode != 0:
        err = stderr.decode().strip()
        raise RuntimeError(f"Obsidian CLI error: {err}")

    return stdout.decode().strip()


async def create(*, name: str, content: str, path: str | None = None) -> str:
    """Create a new note."""
    args = ["create", f"name={name}", f"content={content}"]
    if path:
        args.append(f"path={path}")
    return await run(*args)


async def read(*, file: str | None = None, path: str | None = None) -> str:
    """Read a note's content."""
    args = ["read"]
    if path:
        args.append(f"path={path}")
    elif file:
        args.append(f"file={file}")
    else:
        raise ValueError("Either file or path must be specified")
    return await run(*args)


async def property_set(
    *, name: str, value: str, file: str | None = None, path: str | None = None
) -> str:
    """Set a property on a note."""
    args = ["property:set", f"name={name}", f"value={value}"]
    if path:
        args.append(f"path={path}")
    elif file:
        args.append(f"file={file}")
    return await run(*args)


async def property_read(
    *, name: str, file: str | None = None, path: str | None = None
) -> str:
    """Read a property value from a note."""
    args = ["property:read", f"name={name}"]
    if path:
        args.append(f"path={path}")
    elif file:
        args.append(f"file={file}")
    return await run(*args)


async def search(
    *, query: str, path: str | None = None, limit: int | None = None, format: str = "json"
) -> str:
    """Search vault for text."""
    args = ["search", f"query={query}", f"format={format}"]
    if path:
        args.append(f"path={path}")
    if limit:
        args.append(f"limit={limit}")
    return await run(*args)


async def search_context(
    *, query: str, path: str | None = None, limit: int | None = None, format: str = "json"
) -> str:
    """Search with matching line context."""
    args = ["search:context", f"query={query}", f"format={format}"]
    if path:
        args.append(f"path={path}")
    if limit:
        args.append(f"limit={limit}")
    return await run(*args)


async def backlinks(*, file: str | None = None, path: str | None = None, format: str = "json") -> str:
    """List backlinks to a file."""
    args = ["backlinks", f"format={format}"]
    if path:
        args.append(f"path={path}")
    elif file:
        args.append(f"file={file}")
    return await run(*args)


async def links(*, file: str | None = None, path: str | None = None) -> str:
    """List outgoing links from a file."""
    args = ["links"]
    if path:
        args.append(f"path={path}")
    elif file:
        args.append(f"file={file}")
    return await run(*args)


async def orphans() -> str:
    """List files with no incoming links."""
    return await run("orphans")


async def deadends() -> str:
    """List files with no outgoing links."""
    return await run("deadends")


async def unresolved() -> str:
    """List unresolved links in vault."""
    return await run("unresolved")


async def tags(*, format: str = "json") -> str:
    """List tags in the vault."""
    return await run("tags", f"format={format}")


async def files(*, folder: str | None = None) -> str:
    """List files in the vault."""
    args = ["files"]
    if folder:
        args.append(f"folder={folder}")
    return await run(*args)
