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
from kb_mcp.events.store import EventStore


class MaterializeCliTest(unittest.TestCase):
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

    def test_materialize_enqueues_latest_accepted_candidate(self) -> None:
        candidate_key = self._seed_candidate(label="adr")
        self._run_cli("judge", "accept", candidate_key)

        result = self._run_cli("judge", "materialize", "--candidate-key", candidate_key)

        self.assertEqual(result["selected"], 1)
        self.assertEqual(result["materialized"], 1)
        row = result["results"][0]
        self.assertEqual(row["candidate_key"], candidate_key)
        self.assertEqual(row["effective_label"], "adr")
        self.assertEqual(row["result"], "enqueued")
        store = EventStore()
        record = store.get_materialization_record(str(row["materialization_key"]))
        self.assertIsNotNone(record)
        self.assertEqual(record["status"], "planned")

    def test_materialize_uses_latest_relabel_target_label(self) -> None:
        candidate_key = self._seed_candidate(label="gap")
        self._run_cli("judge", "relabel", candidate_key, "--label", "knowledge")

        result = self._run_cli("judge", "materialize", "--candidate-key", candidate_key)

        self.assertEqual(result["results"][0]["effective_label"], "knowledge")

    def test_materialize_exits_when_candidate_is_not_materializable(self) -> None:
        candidate_key = self._seed_candidate(label="adr")
        stderr = io.StringIO()

        with self.assertRaises(SystemExit) as exc:
            self._run_cli("judge", "materialize", "--candidate-key", candidate_key, stderr=stderr)

        self.assertEqual(exc.exception.code, 1)
        self.assertIn("candidate is not materializable", stderr.getvalue())

    def test_materialize_bulk_skips_broken_candidate_and_continues(self) -> None:
        good = self._seed_candidate(label="adr")
        broken = self._seed_candidate(label="gap")
        self._run_cli("judge", "accept", good)
        store = EventStore()
        with store.transaction() as conn:
            conn.execute(
                "UPDATE promotion_candidates SET status='accepted' WHERE candidate_key=?",
                (broken,),
            )

        result = self._run_cli("judge", "materialize", "--limit", "10")

        indexed = {row["candidate_key"]: row for row in result["results"]}
        self.assertEqual(indexed[good]["result"], "enqueued")
        self.assertEqual(indexed[broken]["result"], "skipped")
        self.assertIn("latest candidate review not found", indexed[broken]["error"])

    def test_retry_failed_materializations_requeues_failed_record(self) -> None:
        candidate_key = self._seed_candidate(label="gap")
        self._run_cli("judge", "accept", candidate_key)
        enqueue = self._run_cli("judge", "materialize", "--candidate-key", candidate_key)
        materialization_key = str(enqueue["results"][0]["materialization_key"])
        logical_key = "materialize:candidate-gap:gap"
        store = EventStore()
        with store.transaction() as conn:
            conn.execute(
                """
                UPDATE materialization_records
                SET status='failed', last_error='boom'
                WHERE materialization_key=?
                """,
                (materialization_key,),
            )
            conn.execute(
                """
                UPDATE outbox
                SET status='dead_letter', last_error='boom'
                WHERE logical_key=?
                """,
                (logical_key,),
            )

        result = self._run_cli("judge", "retry-failed-materializations", "--limit", "10")

        self.assertEqual(result["retried"], 1)
        self.assertEqual(result["skipped"], 0)
        record = store.get_materialization_record(materialization_key)
        self.assertEqual(record["status"], "planned")
        with store.transaction() as conn:
            outbox_count = conn.execute(
                "SELECT COUNT(*) AS count FROM outbox WHERE logical_key=?",
                (logical_key,),
            ).fetchone()
            ready_count = conn.execute(
                "SELECT COUNT(*) AS count FROM outbox WHERE logical_key=? AND status='ready'",
                (logical_key,),
            ).fetchone()
        self.assertEqual(int(outbox_count["count"]), 4)
        self.assertEqual(int(ready_count["count"]), 2)

    def test_retry_failed_materializations_recovers_expired_applying_record(self) -> None:
        candidate_key = self._seed_candidate(label="knowledge")
        self._run_cli("judge", "accept", candidate_key)
        enqueue = self._run_cli("judge", "materialize", "--candidate-key", candidate_key)
        materialization_key = str(enqueue["results"][0]["materialization_key"])
        logical_key = "materialize:candidate-knowledge:knowledge"
        store = EventStore()
        with store.transaction() as conn:
            conn.execute(
                """
                UPDATE materialization_records
                SET status='applying',
                    lease_owner='stale-worker',
                    lease_expires_at='2026-03-25T00:00:00+00:00'
                WHERE materialization_key=?
                """,
                (materialization_key,),
            )
            conn.execute("DELETE FROM outbox WHERE logical_key=?", (logical_key,))

        result = self._run_cli("judge", "retry-failed-materializations", "--limit", "10")

        self.assertEqual(result["retried"], 1)
        record = store.get_materialization_record(materialization_key)
        self.assertEqual(record["status"], "planned")
        self.assertIsNone(record["lease_owner"])
        self.assertIsNone(record["lease_expires_at"])

    def test_retry_failed_materializations_marks_stale_resolution_superseded(self) -> None:
        candidate_key = self._seed_candidate(label="gap")
        self._run_cli("judge", "accept", candidate_key)
        enqueue = self._run_cli("judge", "materialize", "--candidate-key", candidate_key)
        materialization_key = str(enqueue["results"][0]["materialization_key"])
        store = EventStore()
        store.upsert_materialization_record(
            materialization_key="materialize:candidate-gap:2:gap",
            candidate_key=candidate_key,
            review_seq=2,
            judge_run_key="judge-gap",
            window_id="window-gap",
            materialized_label="gap",
            effective_label="gap",
            status="planned",
            payload={"candidate_key": candidate_key, "review_seq": 2},
        )
        with store.transaction() as conn:
            conn.execute(
                "UPDATE materialization_records SET status='failed' WHERE materialization_key=?",
                (materialization_key,),
            )

        result = self._run_cli("judge", "retry-failed-materializations", "--limit", "10")

        self.assertEqual(result["retried"], 1)
        record = store.get_materialization_record(materialization_key)
        self.assertEqual(record["status"], "superseded")

    def test_materialize_uses_latest_window_metadata(self) -> None:
        candidate_key = self._seed_candidate(
            label="gap",
            checkpoints=[
                {
                    "summary": "old summary",
                    "content_excerpt": "old content",
                    "project": self.project,
                    "repo": "github.com/example/old",
                    "cwd": "/tmp/old",
                },
                {
                    "summary": "new summary",
                    "content_excerpt": "new content",
                    "project": self.project,
                    "repo": "github.com/example/new",
                    "cwd": "/tmp/new",
                },
            ],
        )
        self._run_cli("judge", "accept", candidate_key)

        self._run_cli("judge", "materialize", "--candidate-key", candidate_key)

        with EventStore().transaction() as conn:
            row = conn.execute(
                """
                SELECT cwd, repo
                FROM logical_events
                WHERE logical_key='materialize:candidate-gap:gap'
                LIMIT 1
                """
            ).fetchone()
        self.assertEqual(row["cwd"], "/tmp/new")
        self.assertEqual(row["repo"], "github.com/example/new")

    def _seed_candidate(self, *, label: str, checkpoints: list[dict[str, object]] | None = None) -> str:
        store = EventStore()
        judge_run_key = f"judge-{label}"
        candidate_key = f"candidate-{label}"
        window_id = f"window-{label}"
        store.upsert_judge_run(
            judge_run_key=judge_run_key,
            partition_key=f"partition-{label}",
            window_id=window_id,
            start_ordinal=1,
            end_ordinal=2,
            window_index=1,
            status="judged",
            prompt_version="judge-review-candidates.v1",
            labels=[{"label": label, "score": 0.91, "reasons": ["seed"]}],
            decision={},
            model_hint="codex-cli",
        )
        store.upsert_promotion_candidate(
            candidate_key=candidate_key,
            window_id=window_id,
            judge_run_key=judge_run_key,
            label=label,
            status="pending_review",
            score=0.91,
            slice_fingerprint=window_id,
            reasons=["seed"],
            payload={
                "window": {
                    "window_id": window_id,
                    "partition_key": f"partition-{label}",
                    "start_ordinal": 1,
                    "end_ordinal": 2,
                    "checkpoints": checkpoints
                    or [
                        {
                            "summary": f"{label} summary",
                            "content_excerpt": f"{label} content",
                            "project": self.project,
                            "repo": "github.com/example/repo",
                            "cwd": str(self.vault),
                        }
                    ],
                },
                "decision": {},
            },
        )
        return candidate_key

    def _run_cli(self, *argv: str, stderr: io.StringIO | None = None) -> dict[str, object]:
        from kb_mcp import cli

        buf = io.StringIO()
        with patch("sys.argv", ["kb-mcp", *argv]):
            with redirect_stdout(buf):
                with redirect_stderr(stderr or sys.stderr):
                    cli.main()
        return json.loads(buf.getvalue())
