from __future__ import annotations

import io
import json
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

import yaml

from kb_mcp.config import load_config
from kb_mcp.events.normalize import normalize_event
from kb_mcp.events.schema import schema_locked_connection
from kb_mcp.events.store import EventStore


class _StubBackend:
    def prompt_version(self) -> str:
        return "judge-review-candidates.v1"

    def review_window(
        self,
        payload: dict[str, object],
        *,
        prompt_version: str,
        model_hint: str | None = None,
    ):
        from kb_mcp.events.judge_backend import JudgeDecision

        return JudgeDecision(
            labels=[{"label": "adr", "score": 0.91, "reasons": ["agreement"]}],
            should_emit_thin_session=False,
            carry_forward=False,
            notes="",
        )


class JudgeReviewCliTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        root = Path(self.tmpdir.name)
        self.config_dir = root / "config"
        self.vault = root / "vault"
        self.project = "demo"
        os.environ["KB_CONFIG_DIR"] = str(self.config_dir)
        self.addCleanup(lambda: os.environ.pop("KB_CONFIG_DIR", None))
        self.vault.mkdir(parents=True, exist_ok=True)
        for subdir in ["projects/demo/session-log", "projects/demo/draft", "projects/demo/adr", "projects/demo/gap", "projects/demo/knowledge", "general/knowledge", "general/requirements", "inbox"]:
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

    def test_judge_accept_records_review_and_marks_candidate_accepted(self) -> None:
        candidate_key = self._seed_candidate()

        result = self._run_cli(
            "judge",
            "accept",
            candidate_key,
            "--comment",
            "confirmed",
            "--reviewed-by",
            "okash1n",
        )

        self.assertEqual(result["human_verdict"], "accepted")
        self.assertEqual(result["status"], "accepted")
        with schema_locked_connection() as conn:
            candidate = conn.execute(
                "SELECT status FROM promotion_candidates WHERE candidate_key=?",
                (candidate_key,),
            ).fetchone()
            review = conn.execute(
                """
                SELECT human_verdict, human_label, review_comment, reviewed_by
                FROM candidate_reviews
                WHERE candidate_key=?
                ORDER BY review_seq DESC
                LIMIT 1
                """,
                (candidate_key,),
            ).fetchone()
        self.assertEqual(candidate["status"], "accepted")
        self.assertEqual(review["human_verdict"], "accepted")
        self.assertIsNone(review["human_label"])
        self.assertEqual(review["review_comment"], "confirmed")
        self.assertEqual(review["reviewed_by"], "okash1n")

    def test_judge_reject_records_review_and_marks_candidate_rejected(self) -> None:
        candidate_key = self._seed_candidate()

        result = self._run_cli("judge", "reject", candidate_key)

        self.assertEqual(result["human_verdict"], "rejected")
        self.assertEqual(result["status"], "rejected")
        with schema_locked_connection() as conn:
            review = conn.execute(
                """
                SELECT human_verdict, human_label
                FROM candidate_reviews
                WHERE candidate_key=?
                ORDER BY review_seq DESC
                LIMIT 1
                """,
                (candidate_key,),
            ).fetchone()
        self.assertEqual(review["human_verdict"], "rejected")
        self.assertIsNone(review["human_label"])

    def test_judge_relabel_records_review_and_marks_candidate_rejected(self) -> None:
        candidate_key = self._seed_candidate()

        result = self._run_cli("judge", "relabel", candidate_key, "--label", "knowledge")

        self.assertEqual(result["human_verdict"], "relabeled")
        self.assertEqual(result["human_label"], "knowledge")
        self.assertEqual(result["status"], "rejected")
        with schema_locked_connection() as conn:
            review = conn.execute(
                """
                SELECT human_verdict, human_label
                FROM candidate_reviews
                WHERE candidate_key=?
                ORDER BY review_seq DESC
                LIMIT 1
                """,
                (candidate_key,),
            ).fetchone()
        self.assertEqual(review["human_verdict"], "relabeled")
        self.assertEqual(review["human_label"], "knowledge")

    def test_judge_accept_rejects_resolved_candidate(self) -> None:
        candidate_key = self._seed_candidate()
        self._run_cli("judge", "accept", candidate_key)

        stderr = io.StringIO()
        with self.assertRaises(SystemExit) as exc:
            self._run_cli("judge", "accept", candidate_key, stderr=stderr)

        self.assertEqual(exc.exception.code, 1)
        self.assertIn("candidate is not pending_review", stderr.getvalue())

    def test_judge_reject_exits_when_candidate_missing(self) -> None:
        stderr = io.StringIO()

        with self.assertRaises(SystemExit) as exc:
            self._run_cli("judge", "reject", "missing-candidate", stderr=stderr)

        self.assertEqual(exc.exception.code, 1)
        self.assertIn("candidate not found", stderr.getvalue())

    def test_judge_accept_exits_when_judge_run_missing(self) -> None:
        candidate_key = self._seed_candidate()
        stderr = io.StringIO()

        with patch("kb_mcp.events.store.EventStore.get_judge_run_by_key", return_value=None):
            with self.assertRaises(SystemExit) as exc:
                self._run_cli("judge", "accept", candidate_key, stderr=stderr)

        self.assertEqual(exc.exception.code, 1)
        self.assertIn("judge run not found", stderr.getvalue())

    def _seed_candidate(self) -> str:
        store = EventStore()
        base = {
            "project": self.project,
            "repo": "demo/repo",
            "cwd": str(self.vault),
            "session_id": "session-review",
            "transcript_path": str(self.vault / "session-review.jsonl"),
        }
        for idx, payload in enumerate(
            [
                {"summary": "比較した", "content": "案Aと案Bを比べる"},
                {"summary": "これでいこう", "content": "案Bにする"},
            ]
        ):
            store.append(
                normalize_event(
                    tool="codex",
                    client="codex-cli",
                    layer="client_hook",
                    event="turn_checkpointed",
                    payload={
                        **base,
                        **payload,
                        "occurred_at": f"2026-03-25T00:0{idx}:00+00:00",
                    },
                )
            )
        self._run_review_candidates()
        with schema_locked_connection() as conn:
            row = conn.execute(
                "SELECT candidate_key FROM promotion_candidates ORDER BY candidate_key LIMIT 1"
            ).fetchone()
        return str(row["candidate_key"])

    def _run_review_candidates(self) -> dict[str, object]:
        from kb_mcp import cli

        buf = io.StringIO()
        with patch("kb_mcp.events.judge_runner.build_backend", return_value=_StubBackend()):
            with patch("sys.argv", ["kb-mcp", "judge", "review-candidates", "--limit", "10"]):
                with redirect_stdout(buf):
                    cli.main()
        return json.loads(buf.getvalue())

    def _run_cli(self, *argv: str, stderr: io.StringIO | None = None) -> dict[str, object]:
        from kb_mcp import cli

        buf = io.StringIO()
        with patch("sys.argv", ["kb-mcp", *argv]):
            with redirect_stdout(buf):
                with redirect_stderr(stderr or sys.stderr):
                    cli.main()
        return json.loads(buf.getvalue())
