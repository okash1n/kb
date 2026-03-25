from __future__ import annotations

import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

import yaml

from kb_mcp.config import load_config
from kb_mcp.events.normalize import normalize_event
from kb_mcp.events.schema import schema_locked_connection
from kb_mcp.events.store import EventStore


class _StubBackend:
    def __init__(self, decision: dict[str, object]) -> None:
        self._decision = decision

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
            labels=list(self._decision.get("labels", [])),
            should_emit_thin_session=bool(self._decision.get("should_emit_thin_session", False)),
            carry_forward=bool(self._decision.get("carry_forward", False)),
            notes=str(self._decision.get("notes", "")),
        )


class _FailingOnceBackend:
    def __init__(self) -> None:
        self._failed = False

    def prompt_version(self) -> str:
        return "judge-review-candidates.v1"

    def review_window(
        self,
        payload: dict[str, object],
        *,
        prompt_version: str,
        model_hint: str | None = None,
    ):
        if not self._failed:
            self._failed = True
            raise RuntimeError("boom")
        from kb_mcp.events.judge_backend import JudgeDecision

        return JudgeDecision(
            labels=[{"label": "adr", "score": 0.9, "reasons": ["agreement"]}],
            should_emit_thin_session=False,
            carry_forward=False,
            notes="recovered",
        )


class JudgeCliTest(unittest.TestCase):
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

    def test_review_candidates_writes_judge_run_and_candidate(self) -> None:
        self._append_checkpoint_sequence(
            session_id="session-adr",
            payloads=[
                {"summary": "比較した", "content": "案Aと案Bを比べる", "occurred_at": "2026-03-25T00:00:00+00:00"},
                {"summary": "これでいこう", "content": "案Bにする", "occurred_at": "2026-03-25T00:01:00+00:00"},
            ],
        )
        result = self._run_review_candidates(
            backend=_StubBackend(
                {
                    "labels": [{"label": "adr", "score": 0.9, "reasons": ["agreement"]}],
                    "carry_forward": False,
                }
            ),
            limit=10,
            model_hint="codex-cli",
        )
        self.assertEqual(result["judged_windows"], 1)
        self.assertEqual(result["pending_review"], 1)
        self.assertEqual(result["candidates"][0]["label"], "adr")
        self.assertEqual(result["backend_kind"], "_StubBackend")
        with schema_locked_connection() as conn:
            judge_row = conn.execute("SELECT status, model_hint FROM judge_runs").fetchone()
            candidate_row = conn.execute("SELECT label, status, payload_json FROM promotion_candidates").fetchone()
        self.assertEqual(judge_row["status"], "judged")
        self.assertEqual(judge_row["model_hint"], "codex-cli")
        self.assertEqual(candidate_row["label"], "adr")
        self.assertEqual(candidate_row["status"], "pending_review")
        payload = json.loads(candidate_row["payload_json"])
        self.assertEqual(payload["semantics"]["memory_class"], "adr")
        self.assertEqual(payload["semantics"]["update_target"], "decision_policy")
        self.assertEqual(payload["semantics"]["scope"], "project_local")
        self.assertEqual(payload["semantics"]["force"], "hint")

    def test_review_candidates_skips_existing_judged_window_without_force(self) -> None:
        self._append_checkpoint_sequence(
            session_id="session-skip",
            payloads=[{"summary": "これでいこう", "content": "案Bにする", "occurred_at": "2026-03-25T00:00:00+00:00"}],
        )
        backend = _StubBackend(
            {
                "labels": [{"label": "adr", "score": 0.9, "reasons": ["agreement"]}],
                "carry_forward": False,
            }
        )
        first = self._run_review_candidates(backend=backend, limit=10)
        second = self._run_review_candidates(backend=backend, limit=10)
        self.assertEqual(first["judged_windows"], 1)
        self.assertEqual(second["judged_windows"], 0)
        self.assertEqual(second["skipped_windows"], 1)
        with schema_locked_connection() as conn:
            row = conn.execute("SELECT COUNT(*) AS count FROM promotion_candidates").fetchone()
        self.assertEqual(int(row["count"]), 1)

    def test_review_candidates_creates_session_thin_candidate(self) -> None:
        payloads = [
            {
                "summary": f"checkpoint {idx}",
                "content": "相談を続ける",
                "occurred_at": f"2026-03-25T00:{idx:02d}:00+00:00",
            }
            for idx in range(30)
        ]
        self._append_checkpoint_sequence(session_id="session-thin", payloads=payloads)
        result = self._run_review_candidates(
            backend=_StubBackend({"labels": [], "should_emit_thin_session": True, "carry_forward": False}),
            limit=50,
        )
        self.assertGreaterEqual(result["upserted_candidates"], 1)
        with schema_locked_connection() as conn:
            row = conn.execute(
                "SELECT label, status, payload_json FROM promotion_candidates WHERE label='session_thin'"
            ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["status"], "pending_review")
        payload = json.loads(row["payload_json"])
        self.assertEqual(payload["semantics"]["scope"], "session_local")
        self.assertEqual(payload["semantics"]["update_target"], "session_summary_only")

    def test_review_candidates_continues_after_one_window_failure(self) -> None:
        self._append_checkpoint_sequence(
            session_id="session-fail-first",
            payloads=[{"summary": "これでいこう", "content": "案Bにする", "occurred_at": "2026-03-25T00:00:00+00:00"}],
        )
        self._append_checkpoint_sequence(
            session_id="session-pass-second",
            payloads=[{"summary": "これでいこう", "content": "案Cにする", "occurred_at": "2026-03-25T00:01:00+00:00"}],
        )

        result = self._run_review_candidates(
            backend=_FailingOnceBackend(),
            limit=10,
        )

        self.assertEqual(result["failed_windows"], 1)
        self.assertEqual(result["judged_windows"], 1)
        with schema_locked_connection() as conn:
            row = conn.execute("SELECT COUNT(*) AS count FROM promotion_candidates").fetchone()
        self.assertEqual(int(row["count"]), 1)

    def test_review_candidates_marks_suggested_when_threshold_reached(self) -> None:
        for idx in range(5):
            self._append_checkpoint_sequence(
                session_id=f"session-suggest-{idx}",
                payloads=[{"summary": "これでいこう", "content": "案Bにする", "occurred_at": f"2026-03-25T00:0{idx}:00+00:00"}],
            )
        result = self._run_review_candidates(
            backend=_StubBackend(
                {
                    "labels": [{"label": "adr", "score": 0.9, "reasons": ["agreement"]}],
                    "carry_forward": False,
                }
            ),
            limit=1,
        )
        self.assertEqual(result["pending_review"], 5)
        self.assertEqual(result["suggested"], 5)
        self.assertEqual(len(result["candidates"]), 1)
        with schema_locked_connection() as conn:
            rows = conn.execute(
                "SELECT suggestion_seq FROM promotion_candidates ORDER BY candidate_key"
            ).fetchall()
        self.assertTrue(all(int(row["suggestion_seq"]) == 1 for row in rows))

    def test_review_candidates_resuggests_backlog_when_new_candidate_arrives(self) -> None:
        for idx in range(5):
            self._append_checkpoint_sequence(
                session_id=f"session-resuggest-{idx}",
                payloads=[{"summary": "これでいこう", "content": "案Bにする", "occurred_at": f"2026-03-25T00:1{idx}:00+00:00"}],
            )
        first = self._run_review_candidates(
            backend=_StubBackend(
                {
                    "labels": [{"label": "adr", "score": 0.9, "reasons": ["agreement"]}],
                    "carry_forward": False,
                }
            ),
            limit=10,
        )
        self.assertEqual(first["suggested"], 5)

        self._append_checkpoint_sequence(
            session_id="session-resuggest-new",
            payloads=[{"summary": "これでいこう", "content": "案Cにする", "occurred_at": "2026-03-25T00:20:00+00:00"}],
        )
        second = self._run_review_candidates(
            backend=_StubBackend(
                {
                    "labels": [{"label": "adr", "score": 0.9, "reasons": ["agreement"]}],
                    "carry_forward": False,
                }
            ),
            limit=10,
        )

        self.assertEqual(second["pending_review"], 6)
        self.assertEqual(second["suggested"], 6)
        with schema_locked_connection() as conn:
            rows = conn.execute(
                "SELECT suggestion_seq FROM promotion_candidates ORDER BY candidate_key"
            ).fetchall()
        seqs = [int(row["suggestion_seq"]) for row in rows]
        self.assertEqual(seqs.count(2), 5)
        self.assertEqual(seqs.count(1), 1)

    def _append_checkpoint_sequence(self, *, session_id: str, payloads: list[dict[str, str]]) -> None:
        store = EventStore()
        base = {
            "project": self.project,
            "repo": "demo/repo",
            "cwd": str(self.vault),
            "session_id": session_id,
            "transcript_path": str(self.vault / f"{session_id}.jsonl"),
        }
        for payload in payloads:
            store.append(
                normalize_event(
                    tool="codex",
                    client="codex-cli",
                    layer="client_hook",
                    event="turn_checkpointed",
                    payload={**base, **payload},
                )
            )

    def _run_review_candidates(self, *, backend: _StubBackend, limit: int, model_hint: str | None = None) -> dict[str, object]:
        from kb_mcp import cli

        buf = io.StringIO()
        argv = ["kb-mcp", "judge", "review-candidates", "--limit", str(limit)]
        if model_hint:
            argv.extend(["--model-hint", model_hint])
        with patch("kb_mcp.events.judge_runner.build_backend", return_value=backend):
            with patch("sys.argv", argv):
                with redirect_stdout(buf):
                    cli.main()
        return json.loads(buf.getvalue())
