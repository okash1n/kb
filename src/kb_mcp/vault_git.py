"""Vault git sync — auto commit+push when notes are saved."""

import subprocess
import tempfile
from contextlib import contextmanager
from pathlib import Path

from kb_mcp.config import load_config

LOCK_PATH = Path(tempfile.gettempdir()) / "kb-vault-git.lock"


def _load_vault_git_config() -> tuple[bool, Path | None]:
    """Load vault_git flag and vault_path in one call."""
    config = load_config()
    enabled = config.get("vault_git", False) is True
    if not enabled:
        return False, None
    vault_path = config.get("vault_path")
    if not vault_path:
        return False, None
    return True, Path(vault_path).expanduser().resolve()


def _is_git_repo(path: Path) -> bool:
    """Check if path is inside a git repository."""
    try:
        result = subprocess.run(
            ["git", "-C", str(path), "rev-parse", "--is-inside-work-tree"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        return False


def _git_run(vault: Path, *args: str) -> subprocess.CompletedProcess:
    """Run a git command in the vault directory."""
    return subprocess.run(
        ["git", "-C", str(vault), *args],
        capture_output=True,
        text=True,
        timeout=30,
    )


@contextmanager
def _git_lock():
    """Acquire a file lock for serializing git operations."""
    try:
        import fcntl
        fd = open(LOCK_PATH, "a+")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            fd.close()
    except ImportError:
        # Windows: no fcntl, proceed without locking
        yield


def vault_git_sync(filepath: Path) -> str | None:
    """Commit and push a saved note if vault_git is enabled.

    Returns a status message, or None if vault_git is disabled.
    """
    enabled, vault = _load_vault_git_config()
    if not enabled or vault is None:
        return None

    if not _is_git_repo(vault):
        return "vault_git: vault is not a git repository"

    # Make path relative to vault for the commit message
    try:
        rel = filepath.relative_to(vault)
    except ValueError:
        return f"vault_git: file {filepath} is outside vault"

    try:
        with _git_lock():
            # git add the specific file
            result = _git_run(vault, "add", str(rel))
            if result.returncode != 0:
                return f"vault_git: git add failed: {result.stderr.strip()}"

            # Check if there's anything to commit
            status = _git_run(vault, "diff", "--cached", "--quiet")
            if status.returncode == 0:
                return None

            # Commit
            result = _git_run(vault, "commit", "-m", f"kb: {rel}")
            if result.returncode != 0:
                return f"vault_git: git commit failed: {result.stderr.strip()}"

            # Push (best-effort, don't fail the save)
            push_result = _git_run(vault, "push")
            if push_result.returncode != 0:
                return f"vault_git: committed but push failed: {push_result.stderr.strip()}"

            return f"vault_git: synced {rel}"

    except subprocess.TimeoutExpired:
        return "vault_git: git operation timed out"
