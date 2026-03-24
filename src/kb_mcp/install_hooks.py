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


def write_wrapper_script(*, name: str, kb_mcp_path: str, tool: str, client: str) -> Path:
    """Generate a versioned wrapper script around hook dispatch."""
    hooks_dir = hooks_lib_dir()
    hooks_dir.mkdir(parents=True, exist_ok=True)
    script = hooks_dir / f"{name}.sh"
    script.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                'RAW_INPUT=""',
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
                "payload.setdefault('summary', os.environ.get('SUMMARY') or 'session ended')",
                "payload.setdefault('content', os.environ.get('CONTENT') or '')",
                "payload.setdefault('project', os.environ.get('PROJECT') or None)",
                "payload.setdefault('repo', os.environ.get('REPO') or None)",
                "payload.setdefault('cwd', os.environ.get('KB_CWD') or os.getcwd())",
                "payload.setdefault('session_id', os.environ.get('KB_VENDOR_SESSION_ID') or None)",
                "payload.setdefault('correlation_id', os.environ.get('KB_SESSION_CORRELATION_ID') or None)",
                "print(json.dumps(payload, ensure_ascii=False))",
                "PY",
                ')"',
                f'printf "%s" "${{PAYLOAD}}" | "{kb_mcp_path}" hook dispatch --tool {tool} --client {client} --layer client_hook --event session_ended --run-worker',
                "",
            ]
        ),
        encoding="utf-8",
    )
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
    )
    snippet = {"Stop": [{"command": str(wrapper)}]}
    if not execute:
        return json.dumps(snippet, ensure_ascii=False, indent=2)
    path = runtime_dir() / "codex-hooks.json.example"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(snippet, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return f"Codex hook snippet written: {path} (manual apply required)"
