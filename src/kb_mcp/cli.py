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


# --- AI tool config directory helpers ---

def _claude_config_dir() -> Path:
    """Resolve Claude Code config directory (CLAUDE_CONFIG_DIR or ~/.claude)."""
    return Path(os.environ.get("CLAUDE_CONFIG_DIR") or Path.home() / ".claude")


def _claude_config_json() -> Path:
    """Resolve Claude Code main config file.

    .claude.json is always at HOME — CLAUDE_CONFIG_DIR controls settings.json etc.
    """
    return Path.home() / ".claude.json"


def _codex_home() -> Path | None:
    """Resolve Codex CLI config directory (CODEX_HOME or ~/.codex).

    Matches Codex CLI behavior:
    - Empty string is treated as unset (falls back to ~/.codex)
    - Non-empty value must be an existing directory (resolve + canonicalize)
    - Returns None if CODEX_HOME is set but invalid (Codex itself would error)
    """
    val = os.environ.get("CODEX_HOME", "")
    if val:
        p = Path(val)
        if p.is_dir():
            return p.resolve()
        return None  # Codex would error
    return Path.home() / ".codex"


def _copilot_home() -> Path:
    """Resolve Copilot CLI config directory (COPILOT_HOME or ~/.copilot)."""
    val = os.environ.get("COPILOT_HOME", "")
    if val:
        return Path(val).resolve()
    return Path.home() / ".copilot"


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
    """Resolve hook targets."""
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
            _install_copilot_hook(kb_mcp_path, hooks_dir)
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
        f"Add to {_claude_config_dir() / 'settings.json'} hooks section:\n"
        '  "hooks": {\n'
        '    "Stop": [\n'
        f'      {{"command": "{wrapper}"}}\n'
        "    ]\n"
        "  }"
    )


def _install_copilot_hook(kb_mcp_path: str, hooks_dir: Path) -> None:
    """Install Copilot session-end hook to global config.json."""
    import json

    wrapper = _write_wrapper_script(hooks_dir, "copilot-session-end", kb_mcp_path, "copilot", "copilot-cli")
    config_path = _copilot_home() / "config.json"
    config_path.parent.mkdir(parents=True, exist_ok=True)

    if config_path.exists():
        config = json.loads(config_path.read_text(encoding="utf-8"))
    else:
        config = {}

    hooks = config.setdefault("hooks", {})
    session_end = hooks.setdefault("session-end", [])

    # Check if already installed
    wrapper_str = str(wrapper)
    already = any(wrapper_str in h.get("bash", "") for h in session_end)
    if already:
        print(f"✓ Copilot hook already configured in {config_path}")
        return

    session_end.append({"bash": wrapper_str})

    # Atomic write
    tmp = config_path.with_suffix(".tmp")
    tmp.write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
    tmp.replace(config_path)

    print(f"✓ Wrapper script: {wrapper}")
    print(f"✓ Hook added to {config_path}")


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

    repo = None  # reserved for future use

    # kb-mcp command + version
    kb_cmd = shutil.which("kb-mcp")
    try:
        from kb_mcp.update import current_version
        ver = current_version()
    except Exception:
        ver = None
    ver_label = f"v{ver}" if ver else "dev"
    _print_check("kb-mcp command", f"{kb_cmd} ({ver_label})" if kb_cmd else "not found", bool(kb_cmd))

    # Version check
    if not getattr(args, "no_version_check", False) and ver:
        try:
            from kb_mcp.update import latest_version, is_outdated
            latest, err = latest_version(timeout=2)
            if err:
                print(f"  version check: skipped ({err})")
            elif latest:
                outdated = is_outdated(ver, latest)
                if outdated:
                    print(f"  → v{latest} available. Run: uv tool upgrade kb-mcp")
                elif outdated is False:
                    print(f"  version: up to date")
                else:
                    print(f"  version: {latest} available (comparison failed)")
        except Exception as e:
            print(f"  version check: skipped ({e})")
    elif not ver:
        print(f"  version check: skipped (dev install, no package metadata)")
    else:
        print(f"  version check: skipped (--no-version-check)")

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
            import json as _json
            copilot_config = _copilot_home() / "config.json"
            hook_ok = False
            wrapper_path = "n/a"
            if copilot_config.exists():
                try:
                    data = _json.loads(copilot_config.read_text(encoding="utf-8"))
                    hooks = data.get("hooks", {}).get("session-end", [])
                    wrapper_path = hooks[0].get("bash", "") if hooks else ""
                    hook_ok = bool(wrapper_path) and Path(wrapper_path).exists()
                except (KeyError, IndexError, _json.JSONDecodeError, OSError, ValueError):
                    wrapper_path = "invalid config"
            _print_check("    Hooks", wrapper_path or "not configured", hook_ok, indent=4)
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
        claude_config = _claude_config_json()
        if not claude_config.exists():
            return (False, f"{claude_config} not found")
        try:
            data = _json.loads(claude_config.read_text(encoding="utf-8"))
            servers = data.get("mcpServers", {})
            if "kb" in servers:
                return (True, f"registered in {claude_config}")
            return (False, f"not registered in {claude_config}")
        except (_json.JSONDecodeError, OSError):
            return (False, f"{claude_config} unreadable")

    elif tool == "copilot":
        # Copilot CLI config (primary)
        copilot_dir = _copilot_home()
        mcp_config = copilot_dir / "mcp-config.json"
        if mcp_config.exists():
            try:
                data = _json.loads(mcp_config.read_text(encoding="utf-8"))
                servers = data.get("servers", data.get("mcpServers", {}))
                if "kb" in servers:
                    return (True, f"registered in {mcp_config}")
            except (_json.JSONDecodeError, OSError):
                pass
        return (False, f"not found in {mcp_config} (Copilot CLI only — VS Code mcp.json is not checked)")

    elif tool == "codex":
        codex_dir = _codex_home()
        if codex_dir is None:
            codex_home_val = os.environ.get("CODEX_HOME", "")
            return (False, f"CODEX_HOME={codex_home_val!r} is invalid (not a directory). Codex would error.")

        # config.toml (primary)
        config_toml = codex_dir / "config.toml"
        if config_toml.exists():
            try:
                import re
                text = config_toml.read_text(encoding="utf-8")
                if re.search(r'\[(mcp_servers|mcpServers)\.kb\]', text):
                    return (True, f"registered in {config_toml}")
            except OSError:
                pass

        # config.json (legacy fallback)
        config_json = codex_dir / "config.json"
        if config_json.exists():
            try:
                data = _json.loads(config_json.read_text(encoding="utf-8"))
                for key in ("mcpServers", "mcp_servers"):
                    servers = data.get(key, {})
                    if isinstance(servers, dict) and "kb" in servers:
                        return (True, f"registered in {config_json} (legacy — consider migrating to config.toml)")
            except (_json.JSONDecodeError, OSError):
                pass

        return (False, f"not found in {codex_dir}")

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
    hooks_parser.add_argument("--all", action="store_true", help="All tools")

    # hook (execution entry point)
    hook_parser = sub.add_parser("hook", help="Hook execution commands")
    hook_sub = hook_parser.add_subparsers(dest="hook_command")
    session_end_parser = hook_sub.add_parser("session-end", help="Session end hook")
    session_end_parser.add_argument("--tool", required=True, choices=["claude", "copilot", "codex"])
    session_end_parser.add_argument("--client", required=True)

    # doctor
    doctor_parser = sub.add_parser("doctor", help="Diagnose installation state")
    doctor_parser.add_argument("--no-version-check", action="store_true", help="Skip PyPI version check")

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
