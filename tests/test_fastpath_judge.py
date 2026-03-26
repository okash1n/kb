from __future__ import annotations

import argparse
import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

import yaml

from kb_mcp.cli import cmd_hook_dispatch
from kb_mcp.config import load_config
from kb_mcp.doctor import run_doctor
from kb_mcp.events.judge_backend import JudgeDecision
from kb_mcp.events.judge_runner import review_candidates, review_latest_window_fastpath
from kb_mcp.events.normalize import normalize_event
from kb_mcp.events.schema import schema_locked_connection
from kb_mcp.events.store import EventStore


class FastpathJudgeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        root = Path(self.tmpdir.name)
        self.home = root / "home"
        self.config_dir = root / "config"
        self.vault = root / "vault"
        self.project = "demo"
        os.environ["HOME"] = str(self.home)
        os.environ["KB_CONFIG_DIR"] = str(self.config_dir)
        self.addCleanup(lambda: os.environ.pop("KB_CONFIG_DIR", None))
        self.addCleanup(lambda: os.environ.pop("KB_JUDGE_FASTPATH_COMMAND", None))
        self.addCleanup(lambda: os.environ.pop("KB_JUDGE_BACKEND_COMMAND", None))
        self.vault.mkdir(parents=True, exist_ok=True)
        for subdir in [
            "projects/demo/session-log",
            "projects/demo/draft",
            "projects/demo/adr",
            "projects/demo/gap",
            "projects/demo/knowledge",
            "general/knowledge",
            "general/requirements",
            "inbox",
        ]:
            (self.vault / subdir).mkdir(parents=True, exist_ok=True)
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
        self.current_version_patcher = mock.patch("kb_mcp.doctor.current_version", return_value="0.17.3")
        self.latest_version_patcher = mock.patch("kb_mcp.doctor.latest_version", return_value=("0.17.4", None))
        self.current_version_patcher.start()
        self.latest_version_patcher.start()
        self.addCleanup(self.current_version_patcher.stop)
        self.addCleanup(self.latest_version_patcher.stop)

    def test_fastpath_without_backend_falls_back_to_heuristic(self) -> None:
        partition_key = self._append_checkpoint(
            session_id="session-no-backend",
            summary="これでいこう",
            content="その方針で進める",
            occurred_at="2026-03-25T00:00:00+00:00",
        )

        result = review_latest_window_fastpath(
            partition_key=partition_key,
            source_tool="codex",
            source_client="codex-cli",
            model_hint="codex-cli",
        )

        self.assertEqual(result["mode"], "fallback")
        self.assertEqual(result["judged_windows"], 1)
        with schema_locked_connection() as conn:
            row = conn.execute(
                "SELECT label FROM promotion_candidates ORDER BY candidate_key LIMIT 1"
            ).fetchone()
        self.assertEqual(row["label"], "adr")

    def test_fastpath_success_uses_command_backend(self) -> None:
        partition_key = self._append_checkpoint(
            session_id="session-fastpath",
            summary="通常会話",
            content="相談を続ける",
            occurred_at="2026-03-25T00:00:00+00:00",
        )
        script = self._write_backend_script(
            """
import json, sys
request = json.load(sys.stdin)
json.dump(
    {
        "contract_version": request["contract_version"],
        "labels": [{"label": "knowledge", "score": 0.92, "reasons": ["fact_confirmed"]}],
        "should_emit_thin_session": False,
        "carry_forward": False,
        "notes": "fastpath ok",
    },
    sys.stdout,
)
"""
        )
        os.environ["KB_JUDGE_FASTPATH_COMMAND"] = str(script)

        result = review_latest_window_fastpath(
            partition_key=partition_key,
            source_tool="codex",
            source_client="codex-cli",
            model_hint="codex-cli",
        )

        self.assertEqual(result["mode"], "fastpath")
        with schema_locked_connection() as conn:
            candidate = conn.execute(
                "SELECT label FROM promotion_candidates ORDER BY candidate_key LIMIT 1"
            ).fetchone()
        self.assertEqual(candidate["label"], "knowledge")

    def test_fastpath_timeout_records_warning_and_breaker(self) -> None:
        partition_key = self._append_checkpoint(
            session_id="session-timeout",
            summary="これでいこう",
            content="その方針で進める",
            occurred_at="2026-03-25T00:00:00+00:00",
        )
        script = self._write_backend_script(
            """
import time
time.sleep(2.0)
"""
        )
        os.environ["KB_JUDGE_FASTPATH_COMMAND"] = str(script)

        result = review_latest_window_fastpath(
            partition_key=partition_key,
            source_tool="codex",
            source_client="codex-cli",
            model_hint="codex-cli",
        )

        self.assertEqual(result["mode"], "fallback")
        report = run_doctor()
        self.assertIn("Fast-path judge backend: configured", report)
        self.assertIn("Fast-path breakers tracked: 1", report)

    def test_fastpath_fallback_does_not_block_later_review_candidates(self) -> None:
        partition_key = self._append_checkpoint(
            session_id="session-fallback-later",
            summary="通常会話",
            content="相談を続ける",
            occurred_at="2026-03-25T00:00:00+00:00",
        )
        script = self._write_backend_script("raise RuntimeError('boom')\n")
        os.environ["KB_JUDGE_FASTPATH_COMMAND"] = str(script)

        result = review_latest_window_fastpath(
            partition_key=partition_key,
            source_tool="codex",
            source_client="codex-cli",
            model_hint="codex-cli",
        )
        self.assertEqual(result["mode"], "fallback")

        class _LaterBackend:
            def prompt_version(self) -> str:
                return "judge-review-candidates.v1"

            def review_window(self, payload, *, prompt_version: str, model_hint: str | None = None):
                return JudgeDecision(
                    labels=[{"label": "knowledge", "score": 0.95, "reasons": ["fact_confirmed"]}],
                    should_emit_thin_session=False,
                    carry_forward=False,
                    notes="later",
                )

        with mock.patch("kb_mcp.events.judge_runner.build_backend", return_value=_LaterBackend()):
            reviewed = review_candidates(partition_limit=10, display_limit=10, model_hint="codex-cli")

        self.assertEqual(reviewed["judged_windows"], 1)
        with schema_locked_connection() as conn:
            rows = conn.execute(
                "SELECT prompt_version, status FROM judge_runs ORDER BY prompt_version ASC"
            ).fetchall()
        prompt_versions = [str(row["prompt_version"]) for row in rows]
        self.assertIn("judge-review-candidates.v1", prompt_versions)
        self.assertIn("judge-review-candidates.v1+fastpath-fallback", prompt_versions)

    def test_fastpath_breaker_opens_after_threshold(self) -> None:
        script = self._write_backend_script("raise RuntimeError('boom')\n")
        os.environ["KB_JUDGE_FASTPATH_COMMAND"] = str(script)

        for idx in range(3):
            partition_key = self._append_checkpoint(
                session_id=f"session-breaker-{idx}",
                summary="これでいこう",
                content="その方針で進める",
                occurred_at=f"2026-03-25T00:0{idx}:00+00:00",
            )
            review_latest_window_fastpath(
                partition_key=partition_key,
                source_tool="codex",
                source_client="codex-cli",
                model_hint="codex-cli",
            )

        report = run_doctor()
        self.assertIn("Fast-path breakers open: 1 ✗", report)

    def test_fastpath_still_falls_back_when_observation_write_fails(self) -> None:
        partition_key = self._append_checkpoint(
            session_id="session-fallback-observation",
            summary="これでいこう",
            content="その方針で進める",
            occurred_at="2026-03-25T00:00:00+00:00",
        )
        script = self._write_backend_script("raise RuntimeError('boom')\n")
        os.environ["KB_JUDGE_FASTPATH_COMMAND"] = str(script)

        with mock.patch("kb_mcp.events.store.EventStore.put_runtime_observation", side_effect=RuntimeError("db locked")):
            result = review_latest_window_fastpath(
                partition_key=partition_key,
                source_tool="codex",
                source_client="codex-cli",
                model_hint="codex-cli",
            )

        self.assertEqual(result["mode"], "fallback")
        self.assertEqual(result["judged_windows"], 1)

    def test_hook_dispatch_fastpath_does_not_fail_when_judge_errors(self) -> None:
        script = self._write_backend_script("raise RuntimeError('boom')\n")
        os.environ["KB_JUDGE_FASTPATH_COMMAND"] = str(script)
        payload = {
            "project": self.project,
            "repo": "demo/repo",
            "cwd": str(self.vault),
            "session_id": "session-hook",
            "summary": "通常会話",
            "content": "相談を続ける",
            "occurred_at": "2026-03-25T00:00:00+00:00",
        }
        args = argparse.Namespace(
            tool="codex",
            client="codex-cli",
            layer="client_hook",
            event="turn_checkpointed",
            payload_file=None,
            run_worker=False,
            judge_fastpath=True,
        )

        with mock.patch("kb_mcp.cli._read_stdin_payload", return_value=payload):
            buf = io.StringIO()
            with redirect_stdout(buf):
                cmd_hook_dispatch(args)

        result = json.loads(buf.getvalue())
        self.assertEqual(result["status"], "ready")
        self.assertIsNotNone(result["judge_fastpath"])
        with schema_locked_connection() as conn:
            logical = conn.execute("SELECT COUNT(*) AS count FROM logical_events").fetchone()
        self.assertEqual(int(logical["count"]), 1)

    def test_hook_dispatch_ignores_fastpath_warning_write_failure(self) -> None:
        payload = {
            "project": self.project,
            "repo": "demo/repo",
            "cwd": str(self.vault),
            "session_id": "session-hook-warning",
            "summary": "通常会話",
            "content": "相談を続ける",
            "occurred_at": "2026-03-25T00:00:00+00:00",
        }
        args = argparse.Namespace(
            tool="codex",
            client="codex-cli",
            layer="client_hook",
            event="turn_checkpointed",
            payload_file=None,
            run_worker=False,
            judge_fastpath=True,
        )

        with (
            mock.patch("kb_mcp.cli._read_stdin_payload", return_value=payload),
            mock.patch("kb_mcp.events.judge_runner.review_latest_window_fastpath", side_effect=RuntimeError("boom")),
            mock.patch("kb_mcp.events.store.EventStore.put_runtime_observation", side_effect=RuntimeError("db locked")),
        ):
            buf = io.StringIO()
            with redirect_stdout(buf):
                cmd_hook_dispatch(args)

        result = json.loads(buf.getvalue())
        self.assertEqual(result["status"], "ready")

    def _append_checkpoint(
        self,
        *,
        session_id: str,
        summary: str,
        content: str,
        occurred_at: str,
    ) -> str:
        store = EventStore()
        payload = {
            "project": self.project,
            "repo": "demo/repo",
            "cwd": str(self.vault),
            "session_id": session_id,
            "summary": summary,
            "content": content,
            "occurred_at": occurred_at,
        }
        envelope = normalize_event(
            tool="codex",
            client="codex-cli",
            layer="client_hook",
            event="turn_checkpointed",
            payload=payload,
        )
        store.append(envelope)
        return str(envelope.aggregate_state["checkpoint_partition_key"])

    def _write_backend_script(self, body: str) -> Path:
        script = Path(self.tmpdir.name) / f"backend-{len(list(Path(self.tmpdir.name).glob('backend-*.py')))}.py"
        script.write_text(
            "#!/usr/bin/env python3\n"
            "from __future__ import annotations\n"
            "import json\n"
            "import sys\n"
            f"{body}",
            encoding="utf-8",
        )
        script.chmod(0o755)
        return script
