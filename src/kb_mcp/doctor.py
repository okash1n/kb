"""Doctor checks for hook/event pipeline installation."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

from kb_mcp.config import config_dir, load_config, runtime_events_db_path
from kb_mcp.events.scheduler import scheduler_installed, scheduler_platform
from kb_mcp.install_hooks import (
    claude_config_dir,
    claude_config_json,
    codex_home,
    copilot_home,
    hooks_lib_dir,
)


def run_doctor(*, no_version_check: bool = False) -> str:
    """Return a human-readable doctor report."""
    lines: list[str] = []
    kb_cmd = shutil.which("kb-mcp")
    lines.append(_fmt("kb-mcp command", kb_cmd or "not found", bool(kb_cmd)))

    cfg_path = config_dir() / "config.yml"
    lines.append(_fmt("Config", str(cfg_path), cfg_path.exists()))
    config = load_config()
    vault_path = config.get("vault_path")
    if vault_path:
        lines.append(_fmt("Vault", vault_path, Path(vault_path).expanduser().exists()))
    else:
        lines.append(_fmt("Vault", "not configured", False))

    db_path = runtime_events_db_path()
    lines.append(_fmt("Event DB", str(db_path), db_path.exists()))
    lines.append(_fmt("Scheduler", scheduler_platform(), scheduler_installed()))
    lines.append("")
    lines.extend(_tool_checks())
    return "\n".join(lines)


def _fmt(label: str, value: str, ok: bool) -> str:
    mark = "✓" if ok else "✗"
    return f"{label}: {value} {mark}"


def _tool_checks() -> list[str]:
    lines = ["Tooling:"]
    claude_settings = claude_config_dir() / "settings.json"
    claude_ok = False
    if claude_settings.exists():
        try:
            data = json.loads(claude_settings.read_text(encoding="utf-8"))
            stops = data.get("hooks", {}).get("Stop", [])
            for block in stops:
                for hook in block.get("hooks", []):
                    command = hook.get("command", "")
                    if "claude-session-end.sh" in command:
                        claude_ok = True
                        break
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            claude_ok = False
    lines.append(f"  Claude hooks: {claude_settings} {'✓' if claude_ok else '✗'}")
    mcp_ok, mcp_detail = check_mcp_registered("claude")
    lines.append(f"  Claude MCP: {mcp_detail} {'✓' if mcp_ok else '✗'}")

    copilot_config = copilot_home() / "config.json"
    copilot_ok = False
    if copilot_config.exists():
        try:
            data = json.loads(copilot_config.read_text(encoding="utf-8"))
            hooks = data.get("hooks", {}).get("session-end", [])
            copilot_ok = any("copilot-session-end.sh" in item.get("bash", "") for item in hooks)
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            copilot_ok = False
    lines.append(f"  Copilot hooks: {copilot_config} {'✓' if copilot_ok else '✗'}")
    mcp_ok, mcp_detail = check_mcp_registered("copilot")
    lines.append(f"  Copilot MCP: {mcp_detail} {'✓' if mcp_ok else '✗'}")

    codex_dir = codex_home()
    lines.append(f"  Codex home: {codex_dir or 'invalid CODEX_HOME'} {'✓' if codex_dir else '✗'}")
    mcp_ok, mcp_detail = check_mcp_registered("codex")
    lines.append(f"  Codex MCP: {mcp_detail} {'✓' if mcp_ok else '✗'}")

    claude_mcp = claude_config_json()
    lines.append(f"  Claude MCP config: {claude_mcp} {'✓' if claude_mcp.exists() else '✗'}")
    lines.append(f"  Wrapper dir: {hooks_lib_dir()} {'✓' if hooks_lib_dir().exists() else '✗'}")

    legacy_paths = [
        Path("hooks/on-session-end.sh"),
        Path("install/hooks.sh"),
    ]
    for path in legacy_paths:
        lines.append(f"  Legacy path present: {path} {'✓' if path.exists() else '✗'}")
    return lines


def check_mcp_registered(tool: str) -> tuple[bool, str]:
    """Compatibility wrapper for CLI diagnostics."""
    if tool == "claude":
        path = claude_config_json()
        if not path.exists():
            return False, f"{path} not found"
        data = json.loads(path.read_text(encoding="utf-8"))
        servers = data.get("mcpServers", {})
        return ("kb" in servers, f"registered in {path}" if "kb" in servers else f"not registered in {path}")
    if tool == "copilot":
        path = copilot_home() / "mcp-config.json"
        if not path.exists():
            return False, f"{path} not found"
        data = json.loads(path.read_text(encoding="utf-8"))
        servers = data.get("mcpServers", data.get("servers", {}))
        return ("kb" in servers, f"registered in {path}" if "kb" in servers else f"not registered in {path}")
    if tool == "codex":
        home = codex_home()
        if home is None:
            return False, "invalid CODEX_HOME"
        path = home / "config.toml"
        if not path.exists():
            return False, f"{path} not found"
        text = path.read_text(encoding="utf-8")
        return ("[mcp_servers.kb]" in text, f"registered in {path}" if "[mcp_servers.kb]" in text else f"not registered in {path}")
    return False, "unknown tool"
