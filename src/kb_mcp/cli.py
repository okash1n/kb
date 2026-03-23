"""kb-mcp CLI — setup, serve, config get, and more."""

import argparse
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import yaml

from kb_mcp.config import (
    PROJECT_SUBDIRS,
    _validate_config,
    config_dir,
    load_config,
)


def cmd_serve(_args: argparse.Namespace) -> None:
    """Start the MCP server (stdio transport)."""
    from kb_mcp.server import mcp

    mcp.run(transport="stdio")


def cmd_config_get(args: argparse.Namespace) -> None:
    """Print a config value to stdout (for shell scripts)."""
    from kb_mcp.config import kb_data_root, timezone

    key = args.key
    config = load_config()

    if key == "vault-path":
        vp = config.get("vault_path", "")
        if not vp:
            print("ERROR: not configured", file=sys.stderr)
            sys.exit(1)
        print(Path(vp).expanduser())
    elif key == "kb-root":
        print(config.get("kb_root", ""))
    elif key == "kb-data-root":
        try:
            print(kb_data_root())
        except RuntimeError as e:
            print(f"ERROR: {e}", file=sys.stderr)
            sys.exit(1)
    elif key == "obsidian-cli":
        print(config.get("obsidian_cli", "auto"))
    elif key == "timezone":
        print(timezone())
    elif key == "now":
        tz = ZoneInfo(timezone())
        print(datetime.now(tz).strftime("%Y-%m-%dT%H:%M%z"))
    elif key == "now-filename":
        tz = ZoneInfo(timezone())
        print(datetime.now(tz).strftime("%Y%m%d-%H%M"))
    else:
        print(f"Unknown key: {key}", file=sys.stderr)
        sys.exit(1)


def _detect_obsidian_cli() -> str | None:
    """Auto-detect Obsidian CLI path."""
    # Environment variable
    if env := os.environ.get("OBSIDIAN_CLI"):
        return env

    # PATH search
    found = shutil.which("obsidian-cli")
    if found:
        return found

    # Platform defaults
    import platform

    if platform.system() == "Darwin":
        default = "/Applications/Obsidian.app/Contents/MacOS/Obsidian"
        if Path(default).exists():
            return default

    return None


def _prompt(message: str, default: str = "") -> str:
    """Prompt user for input with optional default."""
    if default:
        raw = input(f"{message} [{default}]: ").strip()
        return raw if raw else default
    return input(f"{message}: ").strip()


def _prompt_choice(message: str, choices: list[str]) -> int:
    """Prompt user to choose from a list. Returns 0-based index."""
    print(message)
    for i, choice in enumerate(choices, 1):
        print(f"  [{i}] {choice}")
    while True:
        raw = input("  > ").strip()
        try:
            idx = int(raw)
            if 1 <= idx <= len(choices):
                return idx - 1
        except ValueError:
            pass
        print(f"  Please enter a number between 1 and {len(choices)}")


def cmd_setup(args: argparse.Namespace) -> None:
    """Interactive or non-interactive setup."""
    # Determine if non-interactive
    non_interactive = bool(args.vault_path)

    if non_interactive:
        vault_path = str(Path(args.vault_path).expanduser().resolve())
        kb_root = args.kb_root if args.kb_root is not None else ""
        obsidian_cli = args.obsidian_cli or "auto"
        tz = args.timezone or "Asia/Tokyo"
    else:
        print("Welcome to kb setup!\n")

        choice = _prompt_choice(
            "? Create a new vault or use an existing one?",
            [
                "Create new kb-only vault at ~/kb-vault",
                "Use existing Obsidian vault (kb subdirectory will be created)",
                "Specify custom path",
            ],
        )

        if choice == 0:
            vault_path = str(Path("~/kb-vault").expanduser())
            kb_root = ""
        elif choice == 1:
            raw = _prompt("? Enter your Obsidian vault path")
            vault_path = str(Path(raw).expanduser().resolve())
            kb_root = "kb"
            print(f"  → kb will use {vault_path}/{kb_root}/ as its root")
        else:
            raw = _prompt("? Enter custom path for kb data")
            vault_path = str(Path(raw).expanduser().resolve())
            kb_root = ""

        # Obsidian CLI
        detected = _detect_obsidian_cli()
        if detected:
            yn = _prompt(f"? Obsidian CLI detected at {detected}\n  Use this?", "Y")
            obsidian_cli = detected if yn.upper() != "N" else _prompt("? Enter Obsidian CLI path")
        else:
            print("? Obsidian CLI not found (kb will run in degraded mode)")
            obsidian_cli = "auto"

        # Timezone
        tz = _prompt("? Timezone", "Asia/Tokyo")

    # Build config
    config_data = {
        "vault_path": vault_path,
        "kb_root": kb_root,
        "obsidian_cli": obsidian_cli,
        "timezone": tz,
    }

    # Validate before writing
    try:
        _validate_config(config_data)
    except (RuntimeError, ValueError) as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    # Validate timezone
    from zoneinfo import ZoneInfo
    try:
        ZoneInfo(tz)
    except (KeyError, Exception):
        print(f"ERROR: Unknown timezone: {tz}", file=sys.stderr)
        sys.exit(1)

    # Validate obsidian_cli if explicitly specified
    if obsidian_cli and obsidian_cli != "auto":
        if not Path(obsidian_cli).exists():
            print(f"WARNING: Obsidian CLI not found at {obsidian_cli} (kb will run in degraded mode)", file=sys.stderr)

    # Write config (atomic: temp file + rename)
    import tempfile

    cfg_dir = config_dir()
    cfg_dir.mkdir(parents=True, exist_ok=True)
    cfg_path = cfg_dir / "config.yml"

    fd, tmp_path = tempfile.mkstemp(dir=str(cfg_dir), suffix=".yml")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            yaml.dump(config_data, f, default_flow_style=False, allow_unicode=True)
        os.chmod(tmp_path, 0o600)
        os.replace(tmp_path, str(cfg_path))
    except BaseException:
        os.unlink(tmp_path)
        raise

    # Clear config cache
    load_config.cache_clear()

    print(f"✓ Config written to {cfg_path}")

    # Create directories
    data_root = Path(vault_path)
    if kb_root:
        data_root = data_root / kb_root

    dirs_to_create = [
        data_root / "projects",
        data_root / "general" / "knowledge",
        data_root / "general" / "requirements",
        data_root / "inbox",
    ]

    for d in dirs_to_create:
        d.mkdir(parents=True, exist_ok=True)
        rel = d.relative_to(Path(vault_path))
        print(f"✓ Created {rel}/ in vault")

    print(f"\nkb is ready! Next steps:")
    print(f"  kb-mcp install hooks --claude     # Install hooks for Claude Code")
    print(f"  kb-mcp install hooks --claude     # Install hooks for Claude Code")


def _get_assets_dir() -> Path:
    """Get path to bundled assets directory via importlib.resources."""
    from importlib.resources import files

    return Path(str(files("kb_mcp.assets")))


def _resolve_tool_targets(args: argparse.Namespace) -> list[str]:
    """Resolve which tools to target from args."""
    if getattr(args, "all", False):
        return ["claude", "copilot", "codex"]
    tools = []
    for t in ("claude", "copilot", "codex"):
        if getattr(args, t, False):
            tools.append(t)
    if not tools:
        print("ERROR: specify --claude, --copilot, --codex, or --all", file=sys.stderr)
        sys.exit(1)
    return tools


def _hooks_lib_dir() -> Path:
    """Return the hooks wrapper script directory."""
    return Path.home() / ".local" / "lib" / "kb-mcp" / "hooks"


def _resolve_hooks_targets(args: argparse.Namespace) -> list[str]:
    """Resolve hook targets. --all means Claude/Codex only (Copilot is per-repo)."""
    if getattr(args, "all", False):
        return ["claude", "codex"]  # Copilot excluded — per-repo
    tools = []
    for t in ("claude", "copilot", "codex"):
        if getattr(args, t, False):
            tools.append(t)
    if not tools:
        print("ERROR: specify --claude, --copilot, --codex, or --all", file=sys.stderr)
        sys.exit(1)
    return tools


def cmd_install_hooks(args: argparse.Namespace) -> None:
    """Install hooks for specified AI tool(s)."""
    kb_mcp_path = shutil.which("kb-mcp")
    if not kb_mcp_path:
        print("ERROR: kb-mcp not found in PATH", file=sys.stderr)
        sys.exit(1)

    hooks_dir = _hooks_lib_dir()
    hooks_dir.mkdir(parents=True, exist_ok=True)

    tools = _resolve_hooks_targets(args)

    for tool in tools:
        if tool == "copilot":
            repo = getattr(args, "repo", None)
            if not repo:
                print(
                    "Copilot hooks are per-repo. Specify --repo <path>:\n"
                    f"  kb-mcp install hooks --copilot --repo /path/to/project"
                )
                continue
            _install_copilot_hook(kb_mcp_path, hooks_dir, repo)
        elif tool == "claude":
            _install_claude_hook(kb_mcp_path, hooks_dir)
        elif tool == "codex":
            _install_codex_hook(kb_mcp_path, hooks_dir)


def _write_wrapper_script(hooks_dir: Path, name: str, kb_mcp_path: str, tool: str, client: str) -> Path:
    """Generate a wrapper script that calls kb-mcp hook session-end."""
    import shlex

    script = hooks_dir / f"{name}.sh"
    quoted_path = shlex.quote(kb_mcp_path)
    script.write_text(
        f"#!/usr/bin/env bash\nexec {quoted_path} hook session-end --tool {tool} --client {client} \"$@\"\n",
        encoding="utf-8",
    )
    script.chmod(0o755)
    return script


def _install_claude_hook(kb_mcp_path: str, hooks_dir: Path) -> None:
    """Install Claude Code session-end hook."""
    wrapper = _write_wrapper_script(hooks_dir, "claude-session-end", kb_mcp_path, "claude", "claude-code")
    print(f"✓ Wrapper script: {wrapper}")
    print(
        "Add to ~/.claude/settings.json hooks section:\n"
        '  "hooks": {\n'
        '    "Stop": [\n'
        f'      {{"command": "{wrapper}"}}\n'
        "    ]\n"
        "  }"
    )


def _install_copilot_hook(kb_mcp_path: str, hooks_dir: Path, repo: str) -> None:
    """Install Copilot session-end hook for a specific repo."""
    wrapper = _write_wrapper_script(hooks_dir, "copilot-session-end", kb_mcp_path, "copilot", "copilot-cli")
    hook_dir = Path(repo) / ".github" / "hooks"
    hook_dir.mkdir(parents=True, exist_ok=True)
    hook_json = hook_dir / "session-end.json"
    import json
    import shlex
    hook_data = {
        "version": 1,
        "hooks": {
            "sessionEnd": [
                {"bash": shlex.quote(str(wrapper))}
            ]
        }
    }
    hook_json.write_text(json.dumps(hook_data, indent=2) + "\n", encoding="utf-8")
    print(f"✓ Wrapper script: {wrapper}")
    print(f"✓ Hook JSON written to {hook_json}")


def _install_codex_hook(kb_mcp_path: str, hooks_dir: Path) -> None:
    """Install Codex CLI session-end hook."""
    wrapper = _write_wrapper_script(hooks_dir, "codex-session-end", kb_mcp_path, "codex", "codex-cli")
    print(f"✓ Wrapper script: {wrapper}")
    print(
        "Add to your Codex hooks.json:\n"
        '  {\n'
        '    "Stop": [\n'
        f'      {{"command": "{wrapper}"}}\n'
        "    ]\n"
        "  }"
    )


def cmd_hook_session_end(args: argparse.Namespace) -> None:
    """Handle session-end hook invocation."""
    tool = args.tool
    client = args.client

    # Read stdin if available (some tools pass JSON payload)
    import select
    payload = {}
    if select.select([sys.stdin], [], [], 0.0)[0]:
        import json
        try:
            payload = json.loads(sys.stdin.read())
        except (json.JSONDecodeError, OSError):
            pass

    cwd = payload.get("cwd", os.getcwd())
    summary = payload.get("summary", payload.get("last_assistant_message", "Session ended"))
    content = payload.get("content", "")

    # If no content, try to build from transcript
    transcript_path = payload.get("transcript_path")
    if not content and transcript_path:
        tp = Path(transcript_path)
        if tp.exists():
            lines = tp.read_text(encoding="utf-8").splitlines()
            content = "\n".join(lines[-100:])

    if not content:
        content = f"Session ended. Tool: {tool}, Client: {client}"

    # Save session log via MCP tool
    from kb_mcp.tools.save import kb_session
    try:
        result = kb_session(
            summary=summary[:200],
            content=content,
            ai_tool=tool,
            ai_client=client,
            cwd=cwd,
        )
        print(result)
    except Exception as e:
        print(f"Warning: failed to save session log: {e}", file=sys.stderr)


def cmd_doctor(args: argparse.Namespace) -> None:
    """Diagnose installation state."""
    import platform as plat

    repo = getattr(args, "repo", None)

    # kb-mcp command
    kb_cmd = shutil.which("kb-mcp")
    _print_check("kb-mcp command", kb_cmd or "not found", bool(kb_cmd))

    # Config
    cfg_path = config_dir() / "config.yml"
    _print_check("Config", str(cfg_path), cfg_path.exists())

    # Vault & data root
    config = load_config()
    vault_path = config.get("vault_path", "")
    if vault_path:
        vp = Path(vault_path).expanduser()
        _print_check("Vault", str(vp), vp.exists())
        kb_root = config.get("kb_root", "")
        data_root = vp / kb_root if kb_root else vp
        _print_check("kb data root", str(data_root), data_root.exists())
    else:
        _print_check("Vault", "not configured", False)

    # Obsidian CLI
    from kb_mcp.obsidian import _detect_obsidian_cli
    obs_cli = _detect_obsidian_cli()
    _print_check("Obsidian CLI", obs_cli or "not found (degraded mode)", bool(obs_cli))

    # Per-tool status
    print()
    for tool, label in [("claude", "Claude Code"), ("copilot", "Copilot"), ("codex", "Codex")]:
        print(f"  {label}:")

        # MCP registration
        mcp_ok, mcp_detail = _check_mcp_registered(tool, repo=repo)
        _print_check("    MCP server", mcp_detail, mcp_ok, indent=4)

        # Hooks
        if tool == "copilot":
            if repo:
                import json as _json
                import shlex as _shlex

                hook_json = Path(repo) / ".github" / "hooks" / "session-end.json"
                json_ok = hook_json.exists()
                wrapper_path = "n/a"
                wrapper_ok = False
                if json_ok:
                    try:
                        data = _json.loads(hook_json.read_text(encoding="utf-8"))
                        bash_cmd = data.get("hooks", {}).get("sessionEnd", [{}])[0].get("bash", "")
                        parts = _shlex.split(bash_cmd) if bash_cmd else []
                        wrapper_path = parts[0] if parts else "n/a"
                        wrapper_ok = Path(wrapper_path).exists() if wrapper_path != "n/a" else False
                    except (KeyError, IndexError, _json.JSONDecodeError, OSError, ValueError):
                        wrapper_path = "invalid JSON"
                        wrapper_ok = False
                _print_check("    Hook JSON", str(hook_json), json_ok, indent=4)
                _print_check("    Wrapper", wrapper_path, wrapper_ok, indent=4)
            else:
                print("    Hooks:         [repo_required] per-repo — use --repo to check")
        else:
            wrapper = _hooks_lib_dir() / f"{tool}-session-end.sh"
            _print_check("    Hooks", str(wrapper), wrapper.exists(), indent=4)

        # Windows hooks warning
        if plat.system() == "Windows":
            print("    (hooks are Unix/macOS only in this version)")
        print()


def _check_mcp_registered(tool: str, repo: str | None = None) -> tuple[bool, str]:
    """Check if kb MCP server is registered for a given tool."""
    import json as _json

    if tool == "claude":
        # Check ~/.claude.json for mcpServers.kb
        claude_config = Path.home() / ".claude.json"
        if not claude_config.exists():
            return (False, "~/.claude.json not found")
        try:
            data = _json.loads(claude_config.read_text(encoding="utf-8"))
            servers = data.get("mcpServers", {})
            if "kb" in servers:
                return (True, "registered in ~/.claude.json")
            return (False, "not registered in ~/.claude.json")
        except (_json.JSONDecodeError, OSError):
            return (False, "~/.claude.json unreadable")

    elif tool == "copilot":
        # User-level: Copilot CLI config + VS Code mcp.json (default profile only)
        user_paths = [
            Path.home() / ".copilot" / "mcp-config.json",
            Path.home() / ".vscode" / "mcp.json",
            Path.home() / ".config" / "Code" / "User" / "mcp.json",
            Path.home() / "Library" / "Application Support" / "Code" / "User" / "mcp.json",
        ]
        for mcp_path in user_paths:
            if mcp_path.exists():
                try:
                    data = _json.loads(mcp_path.read_text(encoding="utf-8"))
                    servers = data.get("servers", data.get("mcpServers", {}))
                    if "kb" in servers:
                        return (True, f"registered in {mcp_path}")
                except (_json.JSONDecodeError, OSError):
                    continue

        # Workspace-level: check <repo>/.vscode/mcp.json if --repo is given
        if repo:
            workspace_mcp = Path(repo) / ".vscode" / "mcp.json"
            if workspace_mcp.exists():
                try:
                    data = _json.loads(workspace_mcp.read_text(encoding="utf-8"))
                    servers = data.get("servers", data.get("mcpServers", {}))
                    if "kb" in servers:
                        return (True, f"registered in {workspace_mcp} (workspace)")
                except (_json.JSONDecodeError, OSError):
                    pass

        return (False, "not found (checked: ~/.copilot/mcp-config.json, VS Code default profile mcp.json"
                + (f", {Path(repo) / '.vscode/mcp.json'}" if repo else "") + ")")

    elif tool == "codex":
        # Check Codex config for MCP — structure-based parse
        codex_home = Path(os.environ.get("CODEX_HOME", Path.home() / ".codex"))

        # config.json: look for mcpServers or mcp_servers key containing "kb"
        config_json = codex_home / "config.json"
        if config_json.exists():
            try:
                data = _json.loads(config_json.read_text(encoding="utf-8"))
                for key in ("mcpServers", "mcp_servers", "servers"):
                    servers = data.get(key, {})
                    if isinstance(servers, dict) and "kb" in servers:
                        return (True, f"registered in {config_json} [{key}]")
            except (_json.JSONDecodeError, OSError):
                pass

        # config.toml: parse as TOML if available, fallback to section-aware grep
        config_toml = codex_home / "config.toml"
        if config_toml.exists():
            try:
                text = config_toml.read_text(encoding="utf-8")
                # Look for [mcp_servers.kb] or [mcpServers.kb] section headers
                import re
                if re.search(r'\[(mcp_servers|mcpServers)\.kb\]', text):
                    return (True, f"registered in {config_toml}")
                # Also check for server entries like name = "kb"
                if re.search(r'name\s*=\s*["\']kb["\']', text):
                    return (True, f"registered in {config_toml}")
            except OSError:
                pass

        return (False, f"not found in {codex_home}")

    return (False, "unknown tool")


def _print_check(label: str, value: str, ok: bool, indent: int = 2) -> None:
    """Print a diagnostic check line."""
    mark = "✓" if ok else "✗"
    print(f"{'  ' * (indent // 2)}{label}: {value} {mark}")


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser."""
    parser = argparse.ArgumentParser(
        prog="kb-mcp",
        description="kb — AI cross-context synchronization layer",
    )
    sub = parser.add_subparsers(dest="command")

    # serve
    sub.add_parser("serve", help="Start MCP server (stdio)")

    # config get
    config_parser = sub.add_parser("config", help="Configuration commands")
    config_sub = config_parser.add_subparsers(dest="config_command")
    get_parser = config_sub.add_parser("get", help="Get a config value")
    get_parser.add_argument(
        "key",
        choices=[
            "vault-path",
            "kb-root",
            "kb-data-root",
            "obsidian-cli",
            "timezone",
            "now",
            "now-filename",
        ],
        help="Config key to retrieve",
    )

    # setup
    setup_parser = sub.add_parser("setup", help="Interactive setup")
    setup_parser.add_argument("--vault-path", help="Vault path (non-interactive)")
    setup_parser.add_argument("--kb-root", help="kb root within vault (default: empty)")
    setup_parser.add_argument("--obsidian-cli", help="Obsidian CLI path or 'auto'")
    setup_parser.add_argument("--timezone", help="Timezone (default: Asia/Tokyo)")

    # install
    install_parser = sub.add_parser("install", help="Install hooks")
    install_sub = install_parser.add_subparsers(dest="install_command")

    hooks_parser = install_sub.add_parser("hooks", help="Install hooks for AI tools")
    hooks_parser.add_argument("--claude", action="store_true")
    hooks_parser.add_argument("--copilot", action="store_true")
    hooks_parser.add_argument("--codex", action="store_true")
    hooks_parser.add_argument("--all", action="store_true", help="Claude/Codex only (Copilot is per-repo)")
    hooks_parser.add_argument("--repo", help="Repository path (required for Copilot)")

    # hook (execution entry point)
    hook_parser = sub.add_parser("hook", help="Hook execution commands")
    hook_sub = hook_parser.add_subparsers(dest="hook_command")
    session_end_parser = hook_sub.add_parser("session-end", help="Session end hook")
    session_end_parser.add_argument("--tool", required=True, choices=["claude", "copilot", "codex"])
    session_end_parser.add_argument("--client", required=True)

    # doctor
    doctor_parser = sub.add_parser("doctor", help="Diagnose installation state")
    doctor_parser.add_argument("--repo", help="Repository path for per-repo hook check")

    return parser


def main() -> None:
    """CLI entry point."""
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "serve":
        cmd_serve(args)
    elif args.command == "config":
        if args.config_command == "get":
            cmd_config_get(args)
        else:
            parser.parse_args(["config", "--help"])
    elif args.command == "setup":
        cmd_setup(args)
    elif args.command == "install":
        if args.install_command == "hooks":
            cmd_install_hooks(args)
        else:
            parser.parse_args(["install", "--help"])
    elif args.command == "hook":
        if args.hook_command == "session-end":
            cmd_hook_session_end(args)
        else:
            parser.parse_args(["hook", "--help"])
    elif args.command == "doctor":
        cmd_doctor(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
