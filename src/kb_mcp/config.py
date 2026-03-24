"""kb configuration — lazy evaluation with dev fallback."""

import os
import warnings
from functools import lru_cache
from pathlib import Path

import yaml


def config_dir() -> Path:
    """Return kb config directory path."""
    if env := os.environ.get("KB_CONFIG_DIR"):
        return Path(env)
    xdg = os.environ.get("XDG_CONFIG_HOME", str(Path.home() / ".config"))
    return Path(xdg) / "kb"


def runtime_dir() -> Path:
    """Return kb runtime directory path."""
    return config_dir() / "runtime"


def runtime_events_dir() -> Path:
    """Return runtime directory for event pipeline state."""
    return runtime_dir() / "events"


def runtime_events_db_path() -> Path:
    """Return SQLite path for event pipeline state."""
    return runtime_events_dir() / "events.sqlite3"


@lru_cache(maxsize=1)
def load_config() -> dict:
    """Load config from config.yml. Returns empty dict if not configured."""
    config_path = config_dir() / "config.yml"
    if not config_path.exists():
        return {}
    return yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}


def _validate_config(config: dict) -> None:
    """Validate config values. Raises ValueError on invalid input."""
    vault_path = config.get("vault_path", "")
    if not vault_path:
        raise RuntimeError("kb is not configured. Run 'kb-mcp setup' first.")
    vp = Path(vault_path).expanduser()
    if not vp.is_absolute():
        raise ValueError(f"vault_path must be absolute, got: {vault_path}")
    kb_root = config.get("kb_root", "")
    if kb_root:
        kr = Path(kb_root)
        if kr.is_absolute():
            raise ValueError(f"kb_root must be relative, got: {kb_root}")
        if ".." in kr.parts:
            raise ValueError(f"kb_root must not contain '..', got: {kb_root}")


def _dev_fallback() -> tuple[Path, str] | None:
    """Development fallback: if notes/ exists in repo root, use it."""
    repo_root = Path(__file__).resolve().parent.parent.parent
    notes_dir = repo_root / "notes"
    if notes_dir.exists() and (notes_dir / "projects").exists():
        warnings.warn(
            "Using development fallback (notes/ in repo). "
            "Run 'kb-mcp setup' for proper configuration.",
            stacklevel=3,
        )
        return (notes_dir, "")
    return None


def require_config() -> dict:
    """Load config, validate, and raise if not configured.

    Falls back to dev mode if config.yml is absent but notes/ exists in repo.
    """
    config = load_config()
    if config.get("vault_path"):
        _validate_config(config)
        return config
    # Try dev fallback
    fallback = _dev_fallback()
    if fallback:
        vault_path, kb_root = fallback
        fallback_config = {
            "vault_path": str(vault_path),
            "kb_root": kb_root,
            "timezone": "Asia/Tokyo",
        }
        _validate_config(fallback_config)
        return fallback_config
    raise RuntimeError("kb is not configured. Run 'kb-mcp setup' first.")


def kb_data_root() -> Path:
    """Return the effective kb data root: vault_path / kb_root."""
    config = require_config()
    vault = Path(config["vault_path"]).expanduser()
    kb_root = config.get("kb_root", "")
    root = vault / kb_root if kb_root else vault
    if not root.exists():
        raise RuntimeError(f"kb data root not found: {root}. Run 'kb-mcp setup' first.")
    return root


def projects_dir() -> Path:
    """Return the projects directory path."""
    return kb_data_root() / "projects"


def general_dir() -> Path:
    """Return the general directory path."""
    return kb_data_root() / "general"


def inbox_dir() -> Path:
    """Return the inbox directory path."""
    return kb_data_root() / "inbox"


def timezone() -> str:
    """Return configured timezone. Defaults to Asia/Tokyo."""
    return load_config().get("timezone", "Asia/Tokyo")


def safe_resolve(base: Path, *parts: str) -> Path:
    """Resolve path parts under base, rejecting traversal outside base."""
    resolved = (base / Path(*parts)).resolve()
    base_resolved = base.resolve()
    try:
        resolved.relative_to(base_resolved)
    except ValueError:
        raise ValueError(
            f"Path traversal detected: {'/'.join(parts)} escapes {base}"
        ) from None
    return resolved


PROJECT_SUBDIRS = ["adr", "gap", "session-log", "knowledge", "draft"]
