"""kb-mcp CLI — setup, serve, config get, and more."""

import argparse
import json
import os
import shutil
import subprocess
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

    # Detect vault git
    vault_git = False
    vault_p = Path(vault_path)
    if vault_p.exists():
        try:
            git_check = subprocess.run(
                ["git", "-C", str(vault_p), "rev-parse", "--is-inside-work-tree"],
                capture_output=True, text=True, timeout=5,
            )
        except subprocess.TimeoutExpired:
            git_check = None
        if git_check and git_check.returncode == 0:
            if non_interactive:
                vault_git = args.vault_git if args.vault_git is not None else True
            else:
                yn = _prompt("? Vault is a git repo. Auto commit+push on save?", "Y")
                vault_git = yn.upper() != "N"

    # Build config
    config_data = {
        "vault_path": vault_path,
        "kb_root": kb_root,
        "obsidian_cli": obsidian_cli,
        "timezone": tz,
        "vault_git": vault_git,
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
    print(f"  kb-mcp install hooks --codex      # Install hooks for Codex CLI")


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
    from kb_mcp.events.scheduler import install_scheduler_marker
    from kb_mcp.install_hooks import install_claude, install_codex, install_copilot

    tools = _resolve_hooks_targets(args)
    for tool in tools:
        if tool == "copilot":
            print(install_copilot(execute=args.execute))
        elif tool == "claude":
            print(install_claude(execute=args.execute))
        elif tool == "codex":
            print(install_codex(execute=args.execute))
    if args.execute:
        install_scheduler_marker()


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
    dispatch_args = argparse.Namespace(
        tool=args.tool,
        client=args.client,
        layer="client_hook",
        event="turn_checkpointed",
        payload_file=None,
        run_worker=True,
    )
    cmd_hook_dispatch(dispatch_args)


def _read_stdin_payload() -> dict:
    """Read JSON payload from stdin when available."""
    import select

    if not select.select([sys.stdin], [], [], 0.0)[0]:
        return {}
    raw = sys.stdin.read()
    if not raw.strip():
        return {}
    import json

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"content": raw}


def cmd_hook_dispatch(args: argparse.Namespace) -> None:
    """Normalize, persist, and optionally drain a hook event."""
    from kb_mcp.events.adapters import (
        normalize_claude_payload,
        normalize_codex_payload,
        normalize_copilot_payload,
    )
    from kb_mcp.events.emergency_spool import spool_event
    from kb_mcp.events.judge_runner import review_latest_window_fastpath
    from kb_mcp.events.normalize import normalize_event
    from kb_mcp.events.store import EventStore
    from kb_mcp.events.worker import run_once

    payload = _read_stdin_payload()
    if args.payload_file:
        payload.update(json.loads(Path(args.payload_file).read_text(encoding="utf-8")))
    payload.setdefault("cwd", os.getcwd())

    if args.tool == "claude":
        payload = normalize_claude_payload(payload)
    elif args.tool == "codex":
        payload = normalize_codex_payload(payload)
    elif args.tool == "copilot":
        payload = normalize_copilot_payload(payload)

    try:
        envelope = normalize_event(
            tool=args.tool,
            client=args.client,
            layer=args.layer,
            event=args.event,
            payload=payload,
        )
        result = EventStore().append(envelope)
    except Exception as exc:
        if "envelope" in locals():
            spool_event(envelope)
        print(f"dispatch failed: {exc}", file=sys.stderr)
        raise

    fastpath_result = None
    if (
        getattr(args, "judge_fastpath", False)
        and args.layer == "client_hook"
        and args.event == "turn_checkpointed"
    ):
        partition_key = envelope.aggregate_state.get("checkpoint_partition_key")
        if partition_key:
            try:
                fastpath_result = review_latest_window_fastpath(
                    partition_key=str(partition_key),
                    source_tool=args.tool,
                    source_client=args.client,
                    model_hint=args.client,
                )
            except Exception as exc:
                try:
                    EventStore().put_runtime_observation(
                        key=f"judge_fastpath_warning:{result.logical_key}",
                        severity="warning",
                        message="judge fast-path skipped after error",
                        details={"logical_key": result.logical_key, "error": str(exc)},
                    )
                except Exception:
                    pass

    if getattr(args, "run_worker", False):
        worker_result = run_once(maintenance=args.event == "session_ended")
        exit_code = 0 if worker_result["failed"] == 0 else 2
        print(
            json.dumps(
                {
                    "event_id": result.event_id,
                    "logical_key": result.logical_key,
                    "status": result.status,
                    "aggregate_version": result.aggregate_version,
                    "queued_sinks": result.queued_sinks,
                    "judge_fastpath": fastpath_result,
                    "worker": worker_result,
                },
                ensure_ascii=False,
            )
        )
        if exit_code:
            raise SystemExit(exit_code)
        return

    print(
        json.dumps(
            {
                "event_id": result.event_id,
                "logical_key": result.logical_key,
                "status": result.status,
                "aggregate_version": result.aggregate_version,
                "queued_sinks": result.queued_sinks,
                "judge_fastpath": fastpath_result,
            },
            ensure_ascii=False,
        )
    )


def cmd_worker(args: argparse.Namespace) -> None:
    """Run worker maintenance or drain once."""
    from kb_mcp.events.worker import run_once
    from kb_mcp.events.retention import cleanup_runtime_artifacts
    from kb_mcp.events.store import EventStore

    if args.worker_command == "run-once":
        result = run_once(maintenance=args.maintenance)
        print(json.dumps(result, ensure_ascii=False))
        return
    if args.worker_command == "drain":
        result = run_once(maintenance=True, limit=args.limit)
        print(json.dumps(result, ensure_ascii=False))
        return
    if args.worker_command == "replay-dead-letter":
        replayed = EventStore().replay_dead_letters(limit=args.limit)
        print(json.dumps({"replayed": replayed}, ensure_ascii=False))
        return
    if args.worker_command == "cleanup-runtime":
        result = cleanup_runtime_artifacts(
            checkpoint_days=args.checkpoints_days,
            candidate_days=args.candidates_days,
            promotion_days=args.promotions_days,
            record_days=args.records_days,
        )
        print(json.dumps(result, ensure_ascii=False))
        return
    raise ValueError(f"Unknown worker command: {args.worker_command}")


def cmd_session_run(args: argparse.Namespace) -> None:
    """Launch a managed session command."""
    from kb_mcp.events.session_launcher import launch_session

    command_args = list(args.command_args)
    if command_args and command_args[0] == "--":
        command_args = command_args[1:]
    if not command_args:
        print("ERROR: specify a command after --", file=sys.stderr)
        sys.exit(1)
    exit_code = launch_session(
        tool=args.tool,
        client=args.client,
        command=command_args,
        cwd=args.cwd,
    )
    raise SystemExit(exit_code)


def cmd_doctor(args: argparse.Namespace) -> None:
    """Diagnose installation state."""
    from kb_mcp.doctor import run_doctor

    print(run_doctor(no_version_check=args.no_version_check))


def cmd_judge_review_candidates(args: argparse.Namespace) -> None:
    """Judge checkpoint windows and print pending review candidates."""
    from kb_mcp.events.judge_runner import review_candidates

    print(
        json.dumps(
            review_candidates(
                display_limit=args.limit,
                model_hint=args.model_hint,
            ),
            ensure_ascii=False,
        )
    )


def _candidate_review_payload(candidate_key: str) -> dict[str, object]:
    from kb_mcp.events.store import EventStore

    store = EventStore()
    candidate = store.get_promotion_candidate(candidate_key)
    if candidate is None:
        raise ValueError(f"candidate not found: {candidate_key}")
    judge_row = store.get_judge_run_by_key(str(candidate["judge_run_key"]))
    if judge_row is None:
        raise ValueError(f"judge run not found for candidate: {candidate_key}")
    labels = json.loads(str(judge_row["labels_json"]))
    return {
        "store": store,
        "candidate": candidate,
        "judge_run": judge_row,
        "ai_labels": labels,
        "ai_score": {
            "label": candidate["label"],
            "score": candidate["score"],
        },
    }


def _cmd_judge_review(args: argparse.Namespace, *, verdict: str) -> None:
    from kb_mcp.events.store import EventStore
    from kb_mcp.note import generate_ulid

    try:
        payload = _candidate_review_payload(args.candidate_key)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
    store = payload["store"]
    assert isinstance(store, EventStore)
    candidate = payload["candidate"]
    judge_run = payload["judge_run"]
    human_label = getattr(args, "label", None)
    try:
        review_seq = store.record_candidate_review(
            review_id=generate_ulid(),
            candidate_key=args.candidate_key,
            window_id=str(candidate["window_id"]),
            judge_run_key=str(judge_run["judge_run_key"]),
            ai_labels=list(payload["ai_labels"]),
            ai_score=dict(payload["ai_score"]),
            human_verdict=verdict,
            human_label=human_label,
            review_comment=getattr(args, "comment", None),
            reviewed_by=getattr(args, "reviewed_by", None),
        )
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
    updated = store.get_promotion_candidate(args.candidate_key)
    print(
        json.dumps(
            {
                "candidate_key": args.candidate_key,
                "review_seq": review_seq,
                "human_verdict": verdict,
                "human_label": human_label,
                "status": updated["status"] if updated is not None else None,
            },
            ensure_ascii=False,
        )
    )


def cmd_judge_accept(args: argparse.Namespace) -> None:
    """Accept a promotion candidate."""
    _cmd_judge_review(args, verdict="accepted")


def cmd_judge_reject(args: argparse.Namespace) -> None:
    """Reject a promotion candidate."""
    _cmd_judge_review(args, verdict="rejected")


def cmd_judge_relabel(args: argparse.Namespace) -> None:
    """Relabel a promotion candidate."""
    _cmd_judge_review(args, verdict="relabeled")


def cmd_judge_materialize(args: argparse.Namespace) -> None:
    """Resolve reviewed candidates into materialization aggregates."""
    from kb_mcp.events.store import EventStore

    store = EventStore()
    candidates = store.materializable_candidates(
        candidate_key=args.candidate_key,
        limit=(None if args.candidate_key else args.limit),
    )
    if args.candidate_key and not candidates:
        print(f"ERROR: candidate is not materializable: {args.candidate_key}", file=sys.stderr)
        raise SystemExit(1)

    materialized: list[dict[str, object]] = []
    for candidate in candidates:
        try:
            resolved = store.resolve_candidate_materialization(str(candidate["candidate_key"]))
        except ValueError as exc:
            if args.candidate_key:
                print(f"ERROR: {exc}", file=sys.stderr)
                raise SystemExit(1) from exc
            materialized.append(
                {
                    "candidate_key": str(candidate["candidate_key"]),
                    "result": "skipped",
                    "error": str(exc),
                }
            )
            continue
        row = {
            "candidate_key": str(resolved["candidate_key"]),
            "review_seq": int(resolved["review_seq"]),
            "effective_label": str(resolved["effective_label"]),
            "materialization_key": str(resolved["materialization_key"]),
            "result": str(resolved["result"]),
        }
        dispatch = resolved.get("dispatch")
        if dispatch is not None:
            row.update(
                {
                    "status": dispatch.status,
                    "aggregate_version": dispatch.aggregate_version,
                    "queued_sinks": dispatch.queued_sinks,
                }
            )
        materialized.append(row)
    print(
        json.dumps(
            {
                "selected": len(candidates),
                "materialized": len(materialized),
                "results": materialized,
            },
            ensure_ascii=False,
        )
    )


def cmd_judge_learning_state(args: argparse.Namespace) -> None:
    """Print current learning asset visibility state."""
    from kb_mcp.events.store import EventStore

    store = EventStore()
    rows = store.list_learning_assets()
    limited = rows[: args.limit]
    print(
        json.dumps(
            {
                "total": len(rows),
                "results": [
                    {
                        "asset_key": str(row["asset_key"]),
                        "memory_class": str(row["memory_class"]),
                        "update_target": str(row["update_target"]),
                        "scope": str(row["scope"]),
                        "force": str(row["force"]),
                        "confidence": str(row["confidence"]),
                        "lifecycle": str(row["lifecycle"]),
                        "learning_state_visibility": str(row["learning_state_visibility"]),
                    }
                    for row in limited
                ],
            },
            ensure_ascii=False,
        )
    )


def cmd_judge_retry_failed_materializations(args: argparse.Namespace) -> None:
    """Requeue repairable materialization records."""
    from kb_mcp.events.worker import retry_failed_materializations

    print(
        json.dumps(
            retry_failed_materializations(limit=args.limit),
            ensure_ascii=False,
        )
    )


def _version_text() -> str:
    """Return a human-readable kb-mcp version string."""
    from kb_mcp.update import current_version

    version = current_version()
    if version:
        return f"kb-mcp {version}"
    return "kb-mcp (dev)"


def cmd_version(_args: argparse.Namespace) -> None:
    """Print the current kb-mcp version."""
    print(_version_text())


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
    parser.add_argument(
        "--version",
        action="version",
        version=_version_text(),
    )
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("version", help="Print kb-mcp version")

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
    setup_parser.add_argument("--vault-git", action="store_true", default=None, help="Enable vault git sync")
    setup_parser.add_argument("--no-vault-git", dest="vault_git", action="store_false", help="Disable vault git sync")

    # install
    install_parser = sub.add_parser("install", help="Install hooks")
    install_sub = install_parser.add_subparsers(dest="install_command")

    hooks_parser = install_sub.add_parser("hooks", help="Install hooks for AI tools")
    hooks_parser.add_argument("--claude", action="store_true")
    hooks_parser.add_argument("--copilot", action="store_true")
    hooks_parser.add_argument("--codex", action="store_true")
    hooks_parser.add_argument("--all", action="store_true", help="All tools")
    hooks_parser.add_argument("--execute", action="store_true", help="Write config files where supported")

    # hook (execution entry point)
    hook_parser = sub.add_parser("hook", help="Hook execution commands")
    hook_sub = hook_parser.add_subparsers(dest="hook_command")
    session_end_parser = hook_sub.add_parser("session-end", help="Session end hook")
    session_end_parser.add_argument("--tool", required=True, choices=["claude", "copilot", "codex"])
    session_end_parser.add_argument("--client", required=True)
    dispatch_parser = hook_sub.add_parser("dispatch", help="Durable hook dispatch")
    dispatch_parser.add_argument("--tool", required=True, choices=["claude", "copilot", "codex", "kb"])
    dispatch_parser.add_argument("--client", required=True)
    dispatch_parser.add_argument("--layer", required=True, choices=["client_hook", "session_launcher", "server_middleware"])
    dispatch_parser.add_argument("--event", required=True)
    dispatch_parser.add_argument("--payload-file")
    dispatch_parser.add_argument("--run-worker", action="store_true", help="Drain worker after dispatch")
    dispatch_parser.add_argument("--judge-fastpath", action="store_true", help="Attempt fast-path judge before worker drain")

    # worker
    worker_parser = sub.add_parser("worker", help="Event worker commands")
    worker_sub = worker_parser.add_subparsers(dest="worker_command")
    run_once_parser = worker_sub.add_parser("run-once", help="Drain due sinks once")
    run_once_parser.add_argument("--maintenance", action="store_true", help="Promote pending finalization before drain")
    drain_parser = worker_sub.add_parser("drain", help="Maintenance drain")
    drain_parser.add_argument("--limit", type=int, default=50)
    replay_parser = worker_sub.add_parser("replay-dead-letter", help="Requeue dead-letter sinks")
    replay_parser.add_argument("--limit", type=int, default=50)
    cleanup_parser = worker_sub.add_parser("cleanup-runtime", help="Delete stale runtime artifacts")
    cleanup_parser.add_argument("--checkpoints-days", type=int, default=7)
    cleanup_parser.add_argument("--candidates-days", type=int, default=14)
    cleanup_parser.add_argument("--promotions-days", type=int, default=30)
    cleanup_parser.add_argument("--records-days", type=int, default=30)

    # session run
    session_parser = sub.add_parser("session", help="Managed session commands")
    session_sub = session_parser.add_subparsers(dest="session_command")
    session_run_parser = session_sub.add_parser("run", help="Launch a managed session")
    session_run_parser.add_argument("--tool", required=True, choices=["claude", "copilot", "codex"])
    session_run_parser.add_argument("--client", required=True)
    session_run_parser.add_argument("--cwd")
    session_run_parser.add_argument("command_args", nargs=argparse.REMAINDER)

    # doctor
    doctor_parser = sub.add_parser("doctor", help="Diagnose installation state")
    doctor_parser.add_argument("--no-version-check", action="store_true", help="Skip PyPI version check")

    # judge
    judge_parser = sub.add_parser("judge", help="Judge and review candidate commands")
    judge_sub = judge_parser.add_subparsers(dest="judge_command")
    judge_review_parser = judge_sub.add_parser("review-candidates", help="Build judge runs and print pending review candidates")
    judge_review_parser.add_argument("--limit", type=int, default=50)
    judge_review_parser.add_argument("--model-hint")
    judge_accept_parser = judge_sub.add_parser("accept", help="Accept a pending candidate")
    judge_accept_parser.add_argument("candidate_key")
    judge_accept_parser.add_argument("--comment")
    judge_accept_parser.add_argument("--reviewed-by")
    judge_reject_parser = judge_sub.add_parser("reject", help="Reject a pending candidate")
    judge_reject_parser.add_argument("candidate_key")
    judge_reject_parser.add_argument("--comment")
    judge_reject_parser.add_argument("--reviewed-by")
    judge_relabel_parser = judge_sub.add_parser("relabel", help="Relabel a pending candidate")
    judge_relabel_parser.add_argument("candidate_key")
    judge_relabel_parser.add_argument("--label", required=True, choices=["adr", "gap", "knowledge", "session_thin"])
    judge_relabel_parser.add_argument("--comment")
    judge_relabel_parser.add_argument("--reviewed-by")
    judge_materialize_parser = judge_sub.add_parser("materialize", help="Enqueue materialization for reviewed candidates")
    judge_materialize_parser.add_argument("--candidate-key")
    judge_materialize_parser.add_argument("--limit", type=int, default=50)
    judge_learning_state_parser = judge_sub.add_parser("learning-state", help="Show learning asset visibility state")
    judge_learning_state_parser.add_argument("--limit", type=int, default=50)
    judge_retry_parser = judge_sub.add_parser("retry-failed-materializations", help="Requeue repairable materializations")
    judge_retry_parser.add_argument("--limit", type=int, default=50)

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
        elif args.hook_command == "dispatch":
            cmd_hook_dispatch(args)
        else:
            parser.parse_args(["hook", "--help"])
    elif args.command == "worker":
        cmd_worker(args)
    elif args.command == "session":
        if args.session_command == "run":
            cmd_session_run(args)
        else:
            parser.parse_args(["session", "--help"])
    elif args.command == "doctor":
        cmd_doctor(args)
    elif args.command == "judge":
        if args.judge_command == "review-candidates":
            cmd_judge_review_candidates(args)
        elif args.judge_command == "accept":
            cmd_judge_accept(args)
        elif args.judge_command == "reject":
            cmd_judge_reject(args)
        elif args.judge_command == "relabel":
            cmd_judge_relabel(args)
        elif args.judge_command == "materialize":
            cmd_judge_materialize(args)
        elif args.judge_command == "learning-state":
            cmd_judge_learning_state(args)
        elif args.judge_command == "retry-failed-materializations":
            cmd_judge_retry_failed_materializations(args)
        else:
            parser.parse_args(["judge", "--help"])
    elif args.command == "version":
        cmd_version(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
