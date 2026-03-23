"""kb-mcp update utilities — version check, uv management detection, and upgrade."""

from __future__ import annotations

import fcntl
import importlib.metadata
import json
import shutil
import socket
import subprocess
import sys
import urllib.error
import urllib.request
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, IO

from packaging.version import Version, InvalidVersion


def current_version() -> str:
    """Get the currently installed kb-mcp version."""
    return importlib.metadata.version("kb-mcp")


def latest_version(timeout: int = 5) -> tuple[str | None, str | None]:
    """Fetch latest version from PyPI.

    Returns (version, None) on success, (None, error_reason) on failure.
    Makes HTTPS request to pypi.org.
    """
    url = "https://pypi.org/pypi/kb-mcp/json"
    req = urllib.request.Request(url, headers={"User-Agent": "kb-mcp"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if resp.status != 200:
                return None, f"PyPI returned HTTP {resp.status}"
            data = json.loads(resp.read().decode("utf-8"))
            return data["info"]["version"], None
    except urllib.error.URLError as e:
        reason = e.reason
        if isinstance(reason, socket.timeout):
            return None, f"request timed out ({timeout}s)"
        return None, f"network unreachable ({reason})"
    except (socket.timeout, TimeoutError):
        return None, f"request timed out ({timeout}s)"
    except (json.JSONDecodeError, KeyError) as e:
        return None, f"invalid PyPI response ({e})"


def is_outdated(cur: str, latest: str) -> bool | None:
    """Compare versions using PEP 440.

    Returns True if outdated, False if up to date, None if comparison failed.
    """
    try:
        return Version(cur) < Version(latest)
    except InvalidVersion:
        return None


def is_uv_managed() -> tuple[bool, str | None]:
    """Check if the currently running kb-mcp is installed via uv tool.

    Two-step verification:
    1. 'uv tool dir --bin' to get uv's tool bin directory
    2. Compare with the actual path of the running kb-mcp entry point

    Returns (True, uv_path) or (False, reason).
    """
    uv_path = shutil.which("uv")
    if not uv_path:
        return False, "uv not found in PATH"

    # Step 1: Get uv's tool bin directory
    try:
        result = subprocess.run(
            [uv_path, "tool", "dir", "--bin"],
            capture_output=True,
            text=True,
            timeout=10,
            shell=False,
        )
        if result.returncode != 0:
            return False, f"uv tool dir --bin failed (exit {result.returncode})"
        uv_bin_dir = Path(result.stdout.strip()).resolve()
    except subprocess.TimeoutExpired:
        return False, "uv tool dir timed out"
    except FileNotFoundError:
        return False, f"uv not found at {uv_path}"

    # Step 2: Resolve the currently running kb-mcp entry point
    raw_entry = sys.argv[0]
    if not raw_entry:
        return False, "cannot determine current kb-mcp entry point (sys.argv[0] is empty)"

    if not Path(raw_entry).is_absolute():
        resolved_via_which = shutil.which(raw_entry)
        if resolved_via_which:
            raw_entry = resolved_via_which
    kb_resolved = Path(raw_entry).resolve()

    # Diagnostic: check for PATH mismatch
    kb_which = shutil.which("kb-mcp")
    kb_which_resolved = Path(kb_which).resolve() if kb_which else None
    if kb_which_resolved and kb_which_resolved != kb_resolved:
        return False, (
            f"PATH mismatch detected: "
            f"running from {kb_resolved}, "
            f"but PATH resolves kb-mcp to {kb_which_resolved}. "
            f"Refusing upgrade for safety — resolve the ambiguity first."
        )

    # Step 3: Check if the running kb-mcp is inside uv's bin dir
    try:
        kb_resolved.relative_to(uv_bin_dir)
        return True, uv_path
    except ValueError:
        return False, (
            f"kb-mcp is running from {kb_resolved} "
            f"which is outside uv tool dir ({uv_bin_dir}). "
            f"Not upgrading — this installation is not managed by uv."
        )


LOCK_FILE = Path.home() / ".local" / "state" / "kb-mcp" / "upgrade.lock"


@contextmanager
def upgrade_lock() -> Generator[bool, None, None]:
    """Non-blocking upgrade lock using flock.

    Usage:
        with upgrade_lock() as acquired:
            if not acquired:
                return "Another session is upgrading"
            # ... do upgrade ...

    Lock is held for the duration of the with block and
    released on exit (including on exception).
    """
    LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    fd: IO | None = None
    try:
        fd = open(LOCK_FILE, "w")
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield True
    except (OSError, IOError):
        yield False
    finally:
        if fd is not None:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            except (OSError, IOError):
                pass
            fd.close()


def run_upgrade(uv_path: str) -> tuple[bool, str]:
    """Run uv tool upgrade. Returns (success, message)."""
    try:
        result = subprocess.run(
            [uv_path, "tool", "upgrade", "kb-mcp"],
            capture_output=True,
            text=True,
            timeout=60,
            shell=False,
        )
        if result.returncode == 0:
            return True, result.stdout.strip()
        else:
            return False, f"uv upgrade failed (exit {result.returncode}): {result.stderr.strip()}"
    except subprocess.TimeoutExpired:
        return False, "uv upgrade timed out (60s)"
    except FileNotFoundError:
        return False, f"uv not found at {uv_path}"
