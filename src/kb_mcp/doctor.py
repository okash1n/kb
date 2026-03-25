"""Doctor checks for hook/event pipeline installation."""

from __future__ import annotations

import json
import sqlite3
import shutil
from pathlib import Path

from kb_mcp.config import config_dir, load_config, runtime_events_db_path
from kb_mcp.events.judge_runner import fastpath_backend_status, fastpath_breaker_status
from kb_mcp.events.store import EventStore
from kb_mcp.events.scheduler import scheduler_installed, scheduler_platform
from kb_mcp.events.schema import SCHEMA_VERSION
from kb_mcp.install_hooks import (
    claude_config_dir,
    claude_config_json,
    codex_home,
    copilot_home,
    hooks_lib_dir,
    inspect_codex_hook_state,
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
    lines.append(_fmt("Event schema", f"v{SCHEMA_VERSION}", db_path.exists()))
    lines.append(_fmt("Scheduler", scheduler_platform(), scheduler_installed()))
    lines.extend(_runtime_checks())
    lines.append("")
    lines.extend(_tool_checks())
    return "\n".join(lines)


def _fmt(label: str, value: str, ok: bool) -> str:
    mark = "✓" if ok else "✗"
    return f"{label}: {value} {mark}"


def _fmt_info(label: str, value: str) -> str:
    return f"{label}: {value}"


def _legacy_path_check_line(display_path: str, *, present: bool) -> str:
    if present:
        return (
            f"  Legacy path present: {display_path} present ✗ "
            "(legacy repo path detected; cleanup if unused)"
        )
    return f"  Legacy path present: {display_path} not present ✓"


def _source_checkout_root() -> Path | None:
    root = Path(__file__).resolve().parents[2]
    if (root / "pyproject.toml").exists() and (root / "src" / "kb_mcp" / "doctor.py").exists():
        return root
    return None


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
    codex_hooks_ok, codex_hooks_detail = check_codex_hooks()
    lines.append(f"  Codex hooks: {codex_hooks_detail} {'✓' if codex_hooks_ok else '✗'}")

    claude_mcp = claude_config_json()
    lines.append(f"  Claude MCP config: {claude_mcp} {'✓' if claude_mcp.exists() else '✗'}")
    lines.append(f"  Wrapper dir: {hooks_lib_dir()} {'✓' if hooks_lib_dir().exists() else '✗'}")

    source_root = _source_checkout_root()
    legacy_paths = [
        ("hooks/on-session-end.sh", (source_root / "hooks" / "on-session-end.sh") if source_root else None),
        ("install/hooks.sh", (source_root / "install" / "hooks.sh") if source_root else None),
    ]
    for display_path, check_path in legacy_paths:
        lines.append(
            _legacy_path_check_line(
                display_path,
                present=check_path.exists() if check_path is not None else False,
            )
        )
    return lines


def _runtime_checks() -> list[str]:
    root = config_dir() / "runtime" / "events"
    checkpoints = len(list((root / "checkpoints").glob("*.json"))) if (root / "checkpoints").exists() else 0
    candidates = len(list((root / "candidates").glob("*.json"))) if (root / "candidates").exists() else 0
    promotions = len(list((root / "promotions").glob("*.json"))) if (root / "promotions").exists() else 0
    records = len(list((root / "promotion-records").glob("*.json"))) if (root / "promotion-records").exists() else 0
    judge_counts = {"ready": 0, "judged": 0, "superseded": 0, "failed": 0}
    pending_reviews = 0
    review_count = 0
    dead_letters = 0
    materialization_counts: dict[str, int] | None = {
        "total": 0,
        "repair_pending": 0,
        "failed": 0,
        "expired_applying": 0,
    }
    learning_asset_counts: dict[str, int] | None = {"total": 0}
    learning_visibility_counts: dict[str, int] | None = {
        "candidate": 0,
        "active": 0,
        "held": 0,
        "retractable": 0,
        "superseded": 0,
        "retracted": 0,
        "expired": 0,
    }
    learning_packet_counts: dict[str, int] | None = {"packets": 0, "applications": 0}
    learning_revocation_count: int | None = 0
    learning_outcomes: dict[str, int] | None = {
        "same_gap_recurrence": 0,
        "knowledge_requery": 0,
        "adr_rediscussion": 0,
        "cross_client_consistency": 0,
    }
    learning_hygiene: dict[str, int] | None = {
        "expired_active_packets": 0,
        "packet_asset_mismatches": 0,
        "orphan_applications": 0,
        "legacy_wide_scope_fallbacks": 0,
        "unknown_client_packets": 0,
        "stale_session_local_assets": 0,
        "stale_client_local_assets": 0,
    }
    judge_metrics_error = None
    runtime_metrics_error = None
    fastpath_metrics_error = None
    if runtime_events_db_path().exists():
        try:
            store = EventStore()
            dead_letters = store.dead_letter_count()
            judge_counts = store.judge_run_counts()
            pending_reviews = store.pending_review_candidate_count()
            review_count = store.candidate_review_count()
            materialization_counts = store.materialization_counts()
            learning_asset_counts = store.learning_asset_counts()
            learning_visibility_counts = store.learning_visibility_counts()
            learning_packet_counts = store.learning_packet_counts()
            learning_revocation_count = store.learning_revocation_count()
            learning_outcomes = store.learning_outcome_metrics()
            learning_hygiene = store.learning_runtime_hygiene_metrics()
        except sqlite3.Error as exc:
            runtime_metrics_error = exc.__class__.__name__
            judge_metrics_error = exc.__class__.__name__
            materialization_counts = None
            learning_asset_counts = None
            learning_visibility_counts = None
            learning_packet_counts = None
            learning_revocation_count = None
            learning_outcomes = None
            learning_hygiene = None
    if materialization_counts is None:
        materialization_lines = [
            _fmt("Materialization records", runtime_metrics_error or "error", False),
            _fmt("Materializations repair pending", runtime_metrics_error or "error", False),
            _fmt("Materializations failed", runtime_metrics_error or "error", False),
            _fmt("Materializations applying expired", runtime_metrics_error or "error", False),
        ]
    else:
        materialization_lines = [
            _fmt_info("Materialization records", str(materialization_counts["total"])),
            _fmt("Materializations repair pending", str(materialization_counts["repair_pending"]), materialization_counts["repair_pending"] == 0),
            _fmt("Materializations failed", str(materialization_counts["failed"]), materialization_counts["failed"] == 0),
            _fmt("Materializations applying expired", str(materialization_counts["expired_applying"]), materialization_counts["expired_applying"] == 0),
        ]
    if (
        learning_asset_counts is None
        or learning_visibility_counts is None
        or learning_packet_counts is None
        or learning_revocation_count is None
        or learning_outcomes is None
        or learning_hygiene is None
    ):
        learning_lines = [
            _fmt("Learning assets", runtime_metrics_error or "error", False),
            _fmt("Learning packets", runtime_metrics_error or "error", False),
            _fmt("Learning applications", runtime_metrics_error or "error", False),
            _fmt("Learning revocations", runtime_metrics_error or "error", False),
            _fmt("Learning outcomes", runtime_metrics_error or "error", False),
            _fmt("Learning runtime hygiene", runtime_metrics_error or "error", False),
        ]
    else:
        learning_lines = [
            _fmt_info("Learning assets", str(learning_asset_counts["total"])),
            _fmt_info("Learning packets", str(learning_packet_counts["packets"])),
            _fmt_info("Learning packets invalidated", str(learning_packet_counts.get("invalidated_packets", 0))),
            _fmt_info("Learning applications", str(learning_packet_counts["applications"])),
            _fmt_info("Learning revocations", str(learning_revocation_count)),
            _fmt_info("Learning candidate assets", str(learning_visibility_counts["candidate"])),
            _fmt_info("Learning active assets", str(learning_visibility_counts["active"])),
            _fmt_info("Learning held assets", str(learning_visibility_counts["held"])),
            _fmt_info("Learning retractable assets", str(learning_visibility_counts["retractable"])),
            _fmt_info("Learning same-gap recurrence", str(learning_outcomes["same_gap_recurrence"])),
            _fmt_info("Learning knowledge re-query", str(learning_outcomes["knowledge_requery"])),
            _fmt_info("Learning ADR re-discussion", str(learning_outcomes["adr_rediscussion"])),
            _fmt_info("Learning cross-client consistency", str(learning_outcomes["cross_client_consistency"])),
            _fmt("Learning expired active packets", str(learning_hygiene["expired_active_packets"]), learning_hygiene["expired_active_packets"] == 0),
            _fmt("Learning packet asset mismatches", str(learning_hygiene["packet_asset_mismatches"]), learning_hygiene["packet_asset_mismatches"] == 0),
            _fmt("Learning orphan applications", str(learning_hygiene["orphan_applications"]), learning_hygiene["orphan_applications"] == 0),
            _fmt("Learning legacy wide-scope fallbacks", str(learning_hygiene["legacy_wide_scope_fallbacks"]), learning_hygiene["legacy_wide_scope_fallbacks"] == 0),
            _fmt_info("Learning packets using unknown-client fallback", str(learning_hygiene["unknown_client_packets"])),
            _fmt("Learning stale session-local assets", str(learning_hygiene["stale_session_local_assets"]), learning_hygiene["stale_session_local_assets"] == 0),
            _fmt("Learning stale client-local assets", str(learning_hygiene["stale_client_local_assets"]), learning_hygiene["stale_client_local_assets"] == 0),
        ]
    fastpath_backend = fastpath_backend_status()
    try:
        fastpath_breakers = (
            fastpath_breaker_status() if runtime_events_db_path().exists() else {"total": 0, "open": 0}
        )
    except sqlite3.Error as exc:
        fastpath_breakers = {"total": 0, "open": 0}
        fastpath_metrics_error = exc.__class__.__name__
    return [
        _fmt("Checkpoints", str(checkpoints), True),
        _fmt("Candidates", str(candidates), True),
        _fmt("Promotion plans", str(promotions), True),
        _fmt("Promotion records", str(records), True),
        *learning_lines,
        *materialization_lines,
        _fmt_info(
            "Fast-path judge backend",
            "configured" if fastpath_backend["configured"] else "not configured",
        ),
        _fmt(
            "Fast-path breakers open",
            str(fastpath_breakers["open"]),
            fastpath_metrics_error is None and fastpath_breakers["open"] == 0,
        ),
        _fmt_info("Fast-path breakers tracked", str(fastpath_breakers["total"])),
        _fmt("Fast-path breaker metrics", fastpath_metrics_error or "ok", fastpath_metrics_error is None),
        _fmt_info("Judge runs pending", str(judge_counts["ready"])),
        _fmt_info("Review candidates pending", str(pending_reviews)),
        _fmt_info("Candidate reviews", str(review_count)),
        _fmt("Judge failures", str(judge_counts["failed"]), judge_counts["failed"] == 0),
        _fmt("Judge metrics", judge_metrics_error or "ok", judge_metrics_error is None),
        _fmt("Runtime metrics", runtime_metrics_error or "ok", runtime_metrics_error is None),
        _fmt("Dead letters", str(dead_letters), dead_letters == 0),
    ]


def check_mcp_registered(tool: str) -> tuple[bool, str]:
    """Compatibility wrapper for CLI diagnostics."""
    if tool == "claude":
        path = claude_config_json()
        if not path.exists():
            return False, f"{path} not found"
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return False, f"{path} unreadable"
        servers = data.get("mcpServers", {})
        return ("kb" in servers, f"registered in {path}" if "kb" in servers else f"not registered in {path}")
    if tool == "copilot":
        path = copilot_home() / "mcp-config.json"
        if not path.exists():
            return False, f"{path} not found"
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return False, f"{path} unreadable"
        servers = data.get("mcpServers", data.get("servers", {}))
        return ("kb" in servers, f"registered in {path}" if "kb" in servers else f"not registered in {path}")
    if tool == "codex":
        home = codex_home()
        if home is None:
            return False, "invalid CODEX_HOME"
        path = home / "config.toml"
        if not path.exists():
            return False, f"{path} not found"
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            return False, f"{path} unreadable"
        return ("[mcp_servers.kb]" in text, f"registered in {path}" if "[mcp_servers.kb]" in text else f"not registered in {path}")
    return False, "unknown tool"


def check_codex_hooks() -> tuple[bool, str]:
    """Check Codex hook config in config.toml and hooks.json."""
    state = inspect_codex_hook_state()
    home = state["home"]
    if home is None:
        return False, "invalid CODEX_HOME"
    hooks_path = state["hooks_path"]
    config_path = state["config_path"]
    if not state["hooks_exists"]:
        return False, f"{hooks_path} not found"
    if state["hooks_unreadable"]:
        return False, f"{hooks_path} unreadable"
    if not state["hook_registered"]:
        return False, f"codex-session-end.sh not registered in {hooks_path}"
    if not state["config_exists"]:
        return True, f"registered in {hooks_path}; {config_path} not found"
    if state["config_unreadable"]:
        return True, f"registered in {hooks_path}; {config_path} unreadable"
    if state["feature_enabled"]:
        return True, f"registered in {hooks_path}; codex_hooks enabled in {config_path}"
    return True, f"registered in {hooks_path}; codex_hooks flag not found in {config_path}"
