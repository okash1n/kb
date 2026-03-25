from __future__ import annotations

import os
import json
import sqlite3
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import yaml

from kb_mcp.config import load_config, runtime_events_db_path
from kb_mcp.events.scheduler import scheduler_marker_path
from kb_mcp.events.store import EventStore
from kb_mcp.doctor import _legacy_path_check_line, run_doctor
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
        self.assertIn("Dead letters", report)
        self.assertIn("Promotion plans", report)
        self.assertIn("Claude hooks", report)
        self.assertIn("Codex hooks", report)

    def test_doctor_reports_nonzero_dead_letters(self) -> None:
        store = EventStore()
        with store.transaction() as conn:
            conn.execute(
                """
                INSERT INTO logical_events(
                  logical_key, aggregate_type, correlation_id, session_id, management_mode,
                  source_tool, source_client, status, aggregate_version, summary, content_excerpt,
                  cwd, repo, project, transcript_path, final_outcome, debug_only_reason,
                  aggregate_state_json, ready_at, updated_at
                ) VALUES (
                  'compact:test:1', 'compact', NULL, NULL, 'hook',
                  'codex', 'codex-cli', 'ready', 1, 'summary', 'excerpt',
                  NULL, NULL, NULL, NULL, NULL, NULL,
                  '{}', '2026-03-25T00:00:00+00:00', '2026-03-25T00:00:00+00:00'
                )
                """
            )
            conn.execute(
                """
                INSERT INTO outbox(logical_key, aggregate_version, sink_name, status, due_at, claimed_at, last_error, created_at)
                VALUES ('compact:test:1', 1, 'checkpoint_writer', 'dead_letter', '2026-03-25T00:00:00+00:00', NULL, 'boom', '2026-03-25T00:00:00+00:00')
                """
            )
        report = run_doctor(no_version_check=True)
        self.assertIn("Dead letters: 1 ✗", report)

    def test_doctor_handles_unreadable_mcp_config(self) -> None:
        claude_json = Path(os.environ["HOME"]) / ".claude.json"
        claude_json.parent.mkdir(parents=True, exist_ok=True)
        claude_json.write_text("{broken", encoding="utf-8")

        report = run_doctor(no_version_check=True)

        self.assertIn(f"Claude MCP: {claude_json} unreadable ✗", report)

    def test_doctor_handles_unreadable_codex_mcp_config(self) -> None:
        codex_home = Path(os.environ["HOME"]) / ".codex"
        codex_home.mkdir(parents=True, exist_ok=True)
        config_path = codex_home / "config.toml"
        config_path.write_text("[mcp_servers.kb]\ncommand = 'kb-mcp'\n", encoding="utf-8")

        original_read_text = Path.read_text

        def _read_text_with_codex_failure(path: Path, *args: object, **kwargs: object) -> str:
            if path == config_path:
                raise OSError("permission denied")
            return original_read_text(path, *args, **kwargs)

        with mock.patch("pathlib.Path.read_text", autospec=True, side_effect=_read_text_with_codex_failure):
            report = run_doctor(no_version_check=True)

        self.assertIn(f"Codex MCP: {config_path} unreadable ✗", report)

    def test_legacy_path_check_line_reports_absent_as_ok(self) -> None:
        line = _legacy_path_check_line("hooks/on-session-end.sh", present=False)

        self.assertEqual(
            line,
            "  Legacy path present: hooks/on-session-end.sh not present ✓",
        )

    def test_legacy_path_check_line_reports_present_as_cleanup_candidate(self) -> None:
        line = _legacy_path_check_line("install/hooks.sh", present=True)

        self.assertEqual(
            line,
            "  Legacy path present: install/hooks.sh present ✗ (legacy repo path detected; cleanup if unused)",
        )

    def test_doctor_reports_judge_and_review_counts(self) -> None:
        store = EventStore()
        store.upsert_judge_run(
            judge_run_key="judge-1",
            partition_key="project:demo",
            window_id="window-1",
            start_ordinal=1,
            end_ordinal=2,
            window_index=1,
            status="ready",
            prompt_version="judge-review-candidates.v1",
            labels=[],
            decision={},
        )
        store.upsert_judge_run(
            judge_run_key="judge-2",
            partition_key="project:demo",
            window_id="window-2",
            start_ordinal=3,
            end_ordinal=4,
            window_index=2,
            status="failed",
            prompt_version="judge-review-candidates.v1",
            labels=[],
            decision={},
        )
        store.upsert_promotion_candidate(
            candidate_key="candidate-1",
            window_id="window-1",
            judge_run_key="judge-1",
            label="adr",
            status="pending_review",
            score=0.9,
            slice_fingerprint="fp-1",
            reasons=["agreement"],
            payload={"window_id": "window-1"},
        )
        store.record_candidate_review(
            review_id="review-1",
            candidate_key="candidate-1",
            window_id="window-1",
            judge_run_key="judge-1",
            ai_labels=[{"label": "adr", "score": 0.9}],
            ai_score={"label": "adr", "score": 0.9},
            human_verdict="accepted",
            human_label=None,
        )

        report = run_doctor(no_version_check=True)

        self.assertIn("Judge runs pending: 1", report)
        self.assertIn("Review candidates pending: 0", report)
        self.assertIn("Candidate reviews: 1", report)
        self.assertIn("Materialization records: 0", report)
        self.assertIn("Judge failures: 1 ✗", report)
        self.assertIn("Judge metrics: ok ✓", report)
        self.assertIn("Runtime metrics: ok ✓", report)

    def test_doctor_reports_materialization_runtime_counts(self) -> None:
        store = EventStore()
        store.upsert_judge_run(
            judge_run_key="judge-materialize",
            partition_key="project:demo",
            window_id="window-materialize",
            start_ordinal=1,
            end_ordinal=2,
            window_index=1,
            status="judged",
            prompt_version="judge-review-candidates.v1",
            labels=[],
            decision={},
        )
        store.upsert_promotion_candidate(
            candidate_key="candidate-materialize",
            window_id="window-materialize",
            judge_run_key="judge-materialize",
            label="gap",
            status="accepted",
            score=0.9,
            slice_fingerprint="fp-materialize",
            reasons=["user_correction"],
            payload={"window_id": "window-materialize"},
        )
        store.upsert_materialization_record(
            materialization_key="mat-repair",
            candidate_key="candidate-materialize",
            review_seq=1,
            judge_run_key="judge-materialize",
            window_id="window-materialize",
            materialized_label="gap",
            effective_label="gap",
            status="repair_pending",
            payload={"candidate_key": "candidate-materialize"},
        )
        store.upsert_materialization_record(
            materialization_key="mat-failed",
            candidate_key="candidate-materialize",
            review_seq=2,
            judge_run_key="judge-materialize",
            window_id="window-materialize",
            materialized_label="gap",
            effective_label="gap",
            status="failed",
            payload={"candidate_key": "candidate-materialize"},
        )
        store.upsert_materialization_record(
            materialization_key="mat-expired",
            candidate_key="candidate-materialize",
            review_seq=3,
            judge_run_key="judge-materialize",
            window_id="window-materialize",
            materialized_label="gap",
            effective_label="gap",
            status="applying",
            payload={"candidate_key": "candidate-materialize"},
            lease_owner="worker-1",
            lease_expires_at="2026-03-25T00:00:00+00:00",
        )

        report = run_doctor(no_version_check=True)

        self.assertIn("Materialization records: 3", report)
        self.assertIn("Materializations repair pending: 1 ✗", report)
        self.assertIn("Materializations failed: 1 ✗", report)
        self.assertIn("Materializations applying expired: 1 ✗", report)

    def test_doctor_reports_learning_runtime_counts(self) -> None:
        store = EventStore()
        store.upsert_learning_asset(
            asset_key="asset-active",
            candidate_key=None,
            review_id=None,
            materialization_key=None,
            note_id=None,
            note_path=None,
            memory_class="gap",
            update_target="behavior_style",
            scope="project_local",
            force="preferred",
            confidence="reviewed",
            lifecycle="active",
            provenance={"project": "demo"},
            traceability={},
            revocation_path={},
            learning_state_visibility="active",
            source_status="materialized",
        )
        store.create_learning_packet(
            packet_id="packet-1",
            source_tool="kb",
            source_client="kb-mcp",
            tool_name="gap",
            session_id="session-1",
            project="demo",
            repo=None,
            cwd=None,
            asset_keys=["asset-active"],
        )
        store.record_learning_application(
            application_id="application-1",
            packet_id="packet-1",
            tool_name="gap",
            tool_call_id="call-1",
            source_tool="kb",
            source_client="kb-mcp",
            session_id="session-1",
            save_request_id=None,
            saved_note_id=None,
            saved_note_path=None,
        )

        report = run_doctor(no_version_check=True)

        self.assertIn("Learning assets: 1", report)
        self.assertIn("Learning packets: 1", report)
        self.assertIn("Learning packets invalidated: 0", report)
        self.assertIn("Learning applications: 1", report)
        self.assertIn("Learning revocations: 0", report)
        self.assertIn("Learning active assets: 1", report)
        self.assertIn("Learning expired active packets: 0 ✓", report)
        self.assertIn("Learning packet asset mismatches: 0 ✓", report)
        self.assertIn("Learning orphan applications: 0 ✓", report)
        self.assertIn("Learning legacy wide-scope fallbacks: 0 ✓", report)
        self.assertIn("Learning packets using unknown-client fallback: 0", report)
        self.assertIn("Learning stale session-local assets: 0 ✓", report)
        self.assertIn("Learning stale client-local assets: 0 ✓", report)

    @mock.patch("kb_mcp.doctor.EventStore")
    def test_doctor_handles_judge_metric_query_failure(self, store_cls: mock.Mock) -> None:
        db_path = runtime_events_db_path()
        db_path.parent.mkdir(parents=True, exist_ok=True)
        db_path.touch()
        store = store_cls.return_value
        store.dead_letter_count.return_value = 0
        store.judge_run_counts.side_effect = sqlite3.OperationalError("broken")

        report = run_doctor(no_version_check=True)

        self.assertIn("Judge metrics: OperationalError ✗", report)
        self.assertIn("Runtime metrics: OperationalError ✗", report)
        self.assertIn("Materialization records: OperationalError ✗", report)
        self.assertIn("Materializations repair pending: OperationalError ✗", report)
        self.assertIn("Fast-path breaker metrics: ok ✓", report)

    @mock.patch("kb_mcp.doctor.fastpath_breaker_status", side_effect=sqlite3.OperationalError("broken"))
    def test_doctor_handles_fastpath_breaker_metric_query_failure(self, _fastpath_status: mock.Mock) -> None:
        db_path = runtime_events_db_path()
        db_path.parent.mkdir(parents=True, exist_ok=True)
        db_path.touch()

        report = run_doctor(no_version_check=True)

        self.assertIn("Fast-path breaker metrics: OperationalError ✗", report)

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
