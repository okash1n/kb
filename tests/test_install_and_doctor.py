from __future__ import annotations

import os
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import yaml

from kb_mcp.config import load_config
from kb_mcp.events.scheduler import scheduler_marker_path
from kb_mcp.doctor import run_doctor
from kb_mcp.install_hooks import install_claude, install_codex, install_copilot


class InstallAndDoctorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        root = Path(self.tmpdir.name)
        self.home = root / "home"
        self.config_dir = root / "config"
        self.vault = root / "vault"
        self.vault.mkdir(parents=True, exist_ok=True)
        for subdir in ["projects", "general/knowledge", "general/requirements", "inbox"]:
            (self.vault / subdir).mkdir(parents=True, exist_ok=True)
        os.environ["HOME"] = str(self.home)
        os.environ["KB_CONFIG_DIR"] = str(self.config_dir)
        self.addCleanup(lambda: os.environ.pop("KB_CONFIG_DIR", None))
        cfg_path = self.config_dir / "config.yml"
        cfg_path.parent.mkdir(parents=True, exist_ok=True)
        cfg_path.write_text(
            yaml.safe_dump(
                {
                    "vault_path": str(self.vault),
                    "kb_root": "",
                    "timezone": "Asia/Tokyo",
                    "obsidian_cli": "auto",
                    "vault_git": False,
                }
            ),
            encoding="utf-8",
        )
        load_config.cache_clear()

    @mock.patch("shutil.which", return_value="/tmp/kb-mcp")
    def test_install_hooks_execute_writes_supported_configs(self, _which: mock.Mock) -> None:
        result = install_claude(execute=True)
        self.assertIn("Claude hook installed", result)
        claude_settings = Path(os.environ["HOME"]) / ".claude" / "settings.json"
        self.assertTrue(claude_settings.exists())

        result = install_copilot(execute=True)
        self.assertIn("Copilot hook installed", result)
        copilot_config = Path(os.environ["HOME"]) / ".copilot" / "config.json"
        self.assertTrue(copilot_config.exists())

    @mock.patch("shutil.which", return_value="/tmp/kb-mcp")
    def test_install_codex_prints_manual_steps(self, _which: mock.Mock) -> None:
        result = install_codex(execute=False)
        self.assertIn("Current status:", result)
        self.assertIn("Next step:", result)
        self.assertIn("Edit:", result)
        self.assertIn("hooks.json", result)
        self.assertIn('"hooks"', result)
        self.assertIn('"type": "command"', result)
        self.assertIn("codex-session-end.sh", result)

    @mock.patch("shutil.which", return_value="/tmp/kb-mcp")
    def test_install_codex_reports_existing_state(self, _which: mock.Mock) -> None:
        codex_home = Path(os.environ["HOME"]) / ".codex"
        codex_home.mkdir(parents=True, exist_ok=True)
        (codex_home / "hooks.json").write_text(
            json.dumps(
                {
                    "hooks": {
                        "Stop": [
                            {
                                "hooks": [
                                    {
                                        "type": "command",
                                        "command": "/tmp/codex-session-end.sh",
                                    }
                                ]
                            }
                        ]
                    }
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        (codex_home / "config.toml").write_text("[features]\ncodex_hooks = true\n", encoding="utf-8")

        result = install_codex(execute=False)

        self.assertIn("Codex hook already configured.", result)
        self.assertIn("installed", result)
        self.assertIn("enabled", result)

    @mock.patch("shutil.which", return_value="/tmp/kb-mcp")
    def test_doctor_reports_event_db_and_tooling(self, _which: mock.Mock) -> None:
        install_claude(execute=True)
        codex_home = Path(os.environ["HOME"]) / ".codex"
        codex_wrapper = Path(os.environ["HOME"]) / ".local" / "lib" / "kb-mcp" / "hooks" / "codex-session-end.sh"
        codex_home.mkdir(parents=True, exist_ok=True)
        (codex_home / "hooks.json").write_text(
            json.dumps(
                {
                    "hooks": {
                        "Stop": [
                            {
                                "hooks": [
                                    {
                                        "type": "command",
                                        "command": str(codex_wrapper),
                                    }
                                ]
                            }
                        ]
                    }
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        (codex_home / "config.toml").write_text(
            '[features]\ncodex_hooks = true\n[mcp_servers.kb]\ncommand = "kb-mcp"\nargs = ["serve"]\n',
            encoding="utf-8",
        )
        report = run_doctor(no_version_check=True)
        self.assertIn("Event DB", report)
        self.assertIn("Claude hooks", report)
        self.assertIn("Codex hooks", report)

    @mock.patch("shutil.which", return_value="/tmp/kb-mcp")
    def test_install_codex_wrapper_suppresses_stdout(self, _which: mock.Mock) -> None:
        install_codex(execute=False)
        wrapper = Path(os.environ["HOME"]) / ".local" / "lib" / "kb-mcp" / "hooks" / "codex-session-end.sh"
        content = wrapper.read_text(encoding="utf-8")
        self.assertIn(">/dev/null", content)

    @mock.patch("shutil.which", return_value="/tmp/kb-mcp")
    def test_dry_run_does_not_install_scheduler_marker(self, _which: mock.Mock) -> None:
        install_claude(execute=False)
        self.assertFalse(scheduler_marker_path().exists())

    def test_on_session_end_shim_preserves_stdin_and_launcher_session_id(self) -> None:
        capture_script = Path(self.tmpdir.name) / "fake-kb-mcp.sh"
        capture_json = Path(self.tmpdir.name) / "captured.json"
        capture_script.write_text(
            "\n".join(
                [
                    "#!/usr/bin/env bash",
                    "set -euo pipefail",
                    f"cat > {capture_json}",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        capture_script.chmod(0o755)
        env = os.environ.copy()
        env["KB_MCP_BIN"] = str(capture_script)
        env["KB_VENDOR_SESSION_ID"] = "launcher-session"
        env["SUMMARY"] = "fallback summary"
        env["CONTENT"] = "fallback content"
        script = Path(__file__).resolve().parent.parent / "hooks" / "on-session-end.sh"
        subprocess.run(
            [str(script), "summary", "codex", "content"],
            input=json.dumps({"transcript_path": "/tmp/transcript.txt"}),
            text=True,
            check=False,
            env=env,
        )
        payload = json.loads(capture_json.read_text(encoding="utf-8"))
        self.assertEqual(payload["session_id"], "launcher-session")
        self.assertEqual(payload["transcript_path"], "/tmp/transcript.txt")
