"""Scheduler support for maintenance worker runs."""

from __future__ import annotations

import platform
from pathlib import Path

from kb_mcp.config import runtime_dir


def scheduler_platform() -> str:
    system = platform.system()
    if system == "Darwin":
        return "launchd"
    if system == "Linux":
        return "systemd"
    return "manual"


def scheduler_marker_path() -> Path:
    return runtime_dir() / "scheduler-enabled"


def install_scheduler_marker() -> Path:
    marker = scheduler_marker_path()
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(scheduler_platform() + "\n", encoding="utf-8")
    if scheduler_platform() == "launchd":
        launch_agents = Path.home() / "Library" / "LaunchAgents"
        launch_agents.mkdir(parents=True, exist_ok=True)
        plist = launch_agents / "io.github.okash1n.kb-mcp.worker.plist"
        plist.write_text(
            "\n".join(
                [
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
                    "<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">",
                    "<plist version=\"1.0\"><dict>",
                    "<key>Label</key><string>io.github.okash1n.kb-mcp.worker</string>",
                    "<key>ProgramArguments</key><array><string>kb-mcp</string><string>worker</string><string>run-once</string><string>--maintenance</string></array>",
                    "<key>StartInterval</key><integer>60</integer>",
                    "<key>RunAtLoad</key><true/>",
                    "</dict></plist>",
                    "",
                ]
            ),
            encoding="utf-8",
        )
    elif scheduler_platform() == "systemd":
        unit_dir = Path.home() / ".config" / "systemd" / "user"
        unit_dir.mkdir(parents=True, exist_ok=True)
        (unit_dir / "kb-mcp-worker.service").write_text(
            "[Unit]\nDescription=kb-mcp maintenance worker\n\n[Service]\nType=oneshot\nExecStart=kb-mcp worker run-once --maintenance\n",
            encoding="utf-8",
        )
        (unit_dir / "kb-mcp-worker.timer").write_text(
            "[Unit]\nDescription=Run kb-mcp maintenance every minute\n\n[Timer]\nOnBootSec=30\nOnUnitActiveSec=60\nUnit=kb-mcp-worker.service\n\n[Install]\nWantedBy=timers.target\n",
            encoding="utf-8",
        )
    return marker


def scheduler_installed() -> bool:
    platform_name = scheduler_platform()
    if platform_name == "launchd":
        return (Path.home() / "Library" / "LaunchAgents" / "io.github.okash1n.kb-mcp.worker.plist").exists()
    if platform_name == "systemd":
        unit_dir = Path.home() / ".config" / "systemd" / "user"
        return (unit_dir / "kb-mcp-worker.service").exists() and (unit_dir / "kb-mcp-worker.timer").exists()
    return scheduler_marker_path().exists()
