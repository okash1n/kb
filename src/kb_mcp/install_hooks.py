"""Hook installer for lifecycle dispatch pipeline."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

from kb_mcp.config import runtime_dir


def hooks_lib_dir() -> Path:
    """Return wrapper install directory."""
    return Path.home() / ".local" / "lib" / "kb-mcp" / "hooks"


def claude_config_dir() -> Path:
    return Path.home() / ".claude"


def claude_config_json() -> Path:
    return Path.home() / ".claude.json"


def codex_home() -> Path | None:
    from kb_mcp.cli import _codex_home

    return _codex_home()


def copilot_home() -> Path:
    from kb_mcp.cli import _copilot_home

    return _copilot_home()


def write_wrapper_script(
    *,
    name: str,
    kb_mcp_path: str,
    tool: str,
    client: str,
    suppress_stdout: bool = False,
) -> Path:
    """Generate a versioned wrapper script around hook dispatch."""
    hooks_dir = hooks_lib_dir()
    hooks_dir.mkdir(parents=True, exist_ok=True)
    script = hooks_dir / f"{name}.sh"
    dispatch_command = (
        f'printf "%s" "${{PAYLOAD}}" | "{kb_mcp_path}" hook dispatch '
        f"--tool {tool} --client {client} --layer client_hook --event turn_checkpointed --judge-fastpath --run-worker"
    )
    if suppress_stdout:
        dispatch_command += " >/dev/null"

    lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        'RAW_INPUT=""',
        'JUDGE_FASTPATH_FLAG=""',
        'if [[ -n "${KB_JUDGE_FASTPATH_COMMAND:-}" ]]; then',
        '  JUDGE_FASTPATH_FLAG="--judge-fastpath"',
        "fi",
        'if [[ ! -t 0 ]]; then',
        '  RAW_INPUT="$(cat)"',
        "fi",
        'PAYLOAD="$(RAW_INPUT="${RAW_INPUT}" python3 - <<\'PY\'',
        "import json",
        "import os",
        "",
        'raw = os.environ.get("RAW_INPUT", "").strip()',
        "payload = {}",
        "if raw:",
        "    try:",
        "        payload = json.loads(raw)",
        "    except json.JSONDecodeError:",
        '        payload = {"content": raw}',
        "payload.setdefault('summary', payload.get('last_assistant_message') or payload.get('message') or os.environ.get('SUMMARY') or 'session ended')",
        "payload.setdefault('content', os.environ.get('CONTENT') or '')",
        "payload.setdefault('project', os.environ.get('PROJECT') or None)",
        "payload.setdefault('repo', os.environ.get('REPO') or None)",
        "payload.setdefault('cwd', os.environ.get('KB_CWD') or os.getcwd())",
        "payload.setdefault('session_id', os.environ.get('KB_VENDOR_SESSION_ID') or None)",
        "payload.setdefault('correlation_id', os.environ.get('KB_SESSION_CORRELATION_ID') or None)",
        "print(json.dumps(payload, ensure_ascii=False))",
        "PY",
        ')"',
        dispatch_command.replace("--judge-fastpath", '${JUDGE_FASTPATH_FLAG}'),
        "",
    ]
    script.write_text("\n".join(lines), encoding="utf-8")
    script.chmod(0o755)
    return script


def _ensure_kb_path() -> str:
    kb_mcp_path = shutil.which("kb-mcp")
    if not kb_mcp_path:
        raise RuntimeError("kb-mcp not found in PATH")
    return kb_mcp_path


def _merge_json_file(path: Path, transform) -> None:
    if path.exists():
        data = json.loads(path.read_text(encoding="utf-8"))
    else:
        data = {}
    updated = transform(data)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(updated, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def inspect_codex_hook_state() -> dict[str, object]:
    """Return the current Codex hook/config state."""
    home = codex_home()
    hooks_path = (home / "hooks.json") if home else Path("~/.codex/hooks.json")
    config_path = (home / "config.toml") if home else Path("~/.codex/config.toml")

    hooks_exists = hooks_path.exists() if home else False
    hooks_unreadable = False
    hook_registered = False
    if hooks_exists:
        try:
            hooks_data = json.loads(hooks_path.read_text(encoding="utf-8"))
            stop_groups = hooks_data.get("hooks", {}).get("Stop", [])
            commands: list[str] = []
            for group in stop_groups:
                if not isinstance(group, dict):
                    continue
                for item in group.get("hooks", []):
                    if isinstance(item, dict):
                        commands.append(item.get("command", ""))
            if not commands:
                stop_hooks = hooks_data.get("Stop", [])
                commands.extend(
                    item.get("command", "")
                    for item in stop_hooks
                    if isinstance(item, dict)
                )
            hook_registered = any("codex-session-end.sh" in command for command in commands)
        except (OSError, TypeError, ValueError, json.JSONDecodeError):
            hooks_unreadable = True

    config_exists = config_path.exists() if home else False
    config_unreadable = False
    feature_enabled = False
    if config_exists:
        try:
            config_text = config_path.read_text(encoding="utf-8")
            feature_enabled = "codex_hooks = true" in config_text
        except OSError:
            config_unreadable = True

    return {
        "home": home,
        "hooks_path": hooks_path,
        "hooks_exists": hooks_exists,
        "hooks_unreadable": hooks_unreadable,
        "hook_registered": hook_registered,
        "config_path": config_path,
        "config_exists": config_exists,
        "config_unreadable": config_unreadable,
        "feature_enabled": feature_enabled,
    }


def install_claude(*, execute: bool) -> str:
    """Install or print Claude hook instructions."""
    kb_mcp_path = _ensure_kb_path()
    wrapper = write_wrapper_script(
        name="claude-session-end",
        kb_mcp_path=kb_mcp_path,
        tool="claude",
        client="claude-code",
    )
    command = (
        f'SUMMARY="${{SUMMARY:-no summary}}" '
        f'AI_TOOL=claude AI_CLIENT=claude-code '
        f'CONTENT="${{CONTENT:-session ended}}" '
        f'{wrapper}'
    )
    snippet = {
        "hooks": {
            "Stop": [
                {
                    "hooks": [
                        {
                            "type": "command",
                            "command": command,
                        }
                    ]
                }
            ]
        }
    }
    if not execute:
        return json.dumps(snippet, ensure_ascii=False, indent=2)

    def transform(data: dict) -> dict:
        hooks = data.setdefault("hooks", {})
        stop = hooks.setdefault("Stop", [])
        if not stop:
            stop.append({"hooks": []})
        hook_list = stop[0].setdefault("hooks", [])
        if not any(item.get("command") == command for item in hook_list):
            hook_list.append({"type": "command", "command": command})
        return data

    settings_path = claude_config_dir() / "settings.json"
    _merge_json_file(settings_path, transform)
    return f"Claude hook installed: {settings_path}"


def install_copilot(*, execute: bool) -> str:
    """Install or print Copilot hook instructions."""
    kb_mcp_path = _ensure_kb_path()
    wrapper = write_wrapper_script(
        name="copilot-session-end",
        kb_mcp_path=kb_mcp_path,
        tool="copilot",
        client="copilot-cli",
    )
    config_path = copilot_home() / "config.json"
    if not execute:
        return json.dumps(
            {
                "hooks": {
                    "session-end": [{"bash": str(wrapper)}],
                }
            },
            ensure_ascii=False,
            indent=2,
        )

    def transform(data: dict) -> dict:
        hooks = data.setdefault("hooks", {})
        session_end = hooks.setdefault("session-end", [])
        if not any(item.get("bash") == str(wrapper) for item in session_end):
            session_end.append({"bash": str(wrapper)})
        return data

    _merge_json_file(config_path, transform)
    return f"Copilot hook installed: {config_path}"


def install_codex(*, execute: bool) -> str:
    """Install or print Codex hook instructions."""
    kb_mcp_path = _ensure_kb_path()
    wrapper = write_wrapper_script(
        name="codex-session-end",
        kb_mcp_path=kb_mcp_path,
        tool="codex",
        client="codex-cli",
        suppress_stdout=True,
    )
    state = inspect_codex_hook_state()
    hooks_path = state["hooks_path"]
    config_path = state["config_path"]
    snippet = {
        "hooks": {
            "Stop": [
                {
                    "hooks": [
                        {
                            "type": "command",
                            "command": str(wrapper),
                        }
                    ]
                }
            ]
        }
    }

    hook_status = "installed" if state["hook_registered"] else "missing"
    if state["hooks_unreadable"]:
        hook_status = "unreadable"
    elif not state["hooks_exists"]:
        hook_status = "not found"

    feature_status = "enabled" if state["feature_enabled"] else "missing"
    if state["config_unreadable"]:
        feature_status = "unreadable"
    elif not state["config_exists"]:
        feature_status = "not found"

    header = "Codex hook already configured." if state["hook_registered"] else "Codex hook is not auto-installed."
    status_lines = [
        header,
        "Current status:",
        f"- hooks.json: {hooks_path} ({hook_status})",
        f"- config.toml: {config_path} (codex_hooks {feature_status})",
    ]
    next_steps = [
        "Next step:",
        f"1. Edit: {hooks_path}",
        "2. Ensure Stop includes this JSON:",
        json.dumps(snippet, ensure_ascii=False, indent=2),
        "",
        f"3. If your Codex build requires it, confirm {config_path} contains:",
        "[features]",
        "codex_hooks = true",
    ]
    if not execute:
        return "\n".join(status_lines + [""] + next_steps)
    path = runtime_dir() / "codex-hooks.json.example"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(snippet, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return "\n".join(
        [f"Codex hook snippet written: {path}", ""] + status_lines + [""] + next_steps
    )
