from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml

from kb_mcp.config import load_config
from kb_mcp.events.judge_inputs import build_window_payload, build_windows
from kb_mcp.events.normalize import normalize_event
from kb_mcp.events.policies.promotion_applier import _heartbeat_loop, _load_plan, _materialization_target
from kb_mcp.events.store import EventStore
from kb_mcp.events.worker import run_once
from kb_mcp.note import parse_frontmatter
from kb_mcp.tools.save import save_note_by_type


class _SequenceStopEvent:
    def __init__(self, responses: list[bool]) -> None:
        self._responses = list(responses)

    def wait(self, _seconds: int) -> bool:
        if self._responses:
            return self._responses.pop(0)
        return True


class _HeartbeatStore:
    def __init__(self, responses: list[bool]) -> None:
        self._responses = list(responses)
        self.calls: list[tuple[str, str, int, str]] = []

    def heartbeat_materialization_record(
        self,
        *,
        materialization_key: str,
        lease_owner: str,
        lease_epoch: int,
        lease_expires_at: str,
    ) -> bool:
        self.calls.append((materialization_key, lease_owner, lease_epoch, lease_expires_at))
        if self._responses:
            return self._responses.pop(0)
        return True


class _ExceptionalHeartbeatStore:
    def heartbeat_materialization_record(
        self,
        *,
        materialization_key: str,
        lease_owner: str,
        lease_epoch: int,
        lease_expires_at: str,
    ) -> bool:
        raise RuntimeError("sqlite busy")


class _Row(dict):
    def __getitem__(self, key: str) -> object:
        return super().__getitem__(key)


class MaterializeApplierTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        root = Path(self.tmpdir.name)
        self.config_dir = root / "config"
        self.vault = root / "vault"
        self.project = "demo"
        self.maxDiff = None
        self._old_env = None
        import os

        self._old_env = os.environ.get("KB_CONFIG_DIR")
        os.environ["KB_CONFIG_DIR"] = str(self.config_dir)
        load_config.cache_clear()
        self.addCleanup(self._restore_env)
        self.vault.mkdir(parents=True, exist_ok=True)
        for subdir in [
            "projects/demo/session-log",
            "projects/demo/adr",
            "projects/demo/gap",
            "projects/demo/knowledge",
            "projects/demo/draft",
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
                }
            ),
            encoding="utf-8",
        )

    def _restore_env(self) -> None:
        import os

        if self._old_env is None:
            os.environ.pop("KB_CONFIG_DIR", None)
        else:
            os.environ["KB_CONFIG_DIR"] = self._old_env
        load_config.cache_clear()

    def test_gap_materialization_creates_gap_note_and_marks_candidate_materialized(self) -> None:
        store = EventStore()
        judge_run_key = "judge-gap"
        candidate_key = "candidate-gap"
        store.upsert_judge_run(
            judge_run_key=judge_run_key,
            partition_key="partition-gap",
            window_id="window-gap",
            start_ordinal=1,
            end_ordinal=2,
            window_index=1,
            status="judged",
            prompt_version="v1",
            labels=[{"label": "gap", "score": 0.92, "reasons": ["user_correction"]}],
            decision={},
            model_hint="codex",
        )
        window = {
            "window_id": "window-gap",
            "partition_key": "partition-gap",
            "start_ordinal": 1,
            "end_ordinal": 2,
            "checkpoints": [
                {
                    "summary": "方針が違う",
                    "content_excerpt": "ユーザーが違うと言っている",
                    "project": self.project,
                    "repo": "github.com/example/repo",
                }
            ],
        }
        store.upsert_promotion_candidate(
            candidate_key=candidate_key,
            window_id="window-gap",
            judge_run_key=judge_run_key,
            label="gap",
            status="pending_review",
            score=0.92,
            slice_fingerprint="window-gap",
            reasons=["user_correction"],
            payload={"window": window, "decision": {}},
        )
        review_seq = store.record_candidate_review(
            review_id="review-gap",
            candidate_key=candidate_key,
            judge_run_key=judge_run_key,
            window_id="window-gap",
            ai_labels=[{"label": "gap", "score": 0.92}],
            ai_score={"gap": 0.92},
            human_verdict="accepted",
            human_label=None,
            review_comment=None,
            reviewed_by="tester",
        )
        store.enqueue_materialization_resolution(
            candidate_key=candidate_key,
            review_seq=review_seq,
            effective_label="gap",
            materialization_key="mat-gap",
            judge_run_key=judge_run_key,
            window_id="window-gap",
            payload={"candidate_key": candidate_key},
            project=self.project,
            cwd=str(self.vault),
            repo="github.com/example/repo",
        )

        result = run_once()

        self.assertEqual(result["failed"], 0)
        gap_files = sorted((self.vault / "projects" / self.project / "gap").glob("*.md"))
        self.assertEqual(len(gap_files), 1)
        gap_text = gap_files[0].read_text(encoding="utf-8")
        gap_fm = parse_frontmatter(gap_text) or {}
        self.assertEqual(gap_fm["summary"], "方針が違う")
        self.assertEqual(gap_fm["candidate_key"], "candidate-gap")
        with store.transaction() as conn:
            candidate = conn.execute(
                "SELECT status FROM promotion_candidates WHERE candidate_key=?",
                (candidate_key,),
            ).fetchone()
            record = conn.execute(
                "SELECT status, note_id, note_path FROM materialization_records WHERE materialization_key='mat-gap'"
            ).fetchone()
        self.assertEqual(candidate["status"], "materialized")
        self.assertEqual(record["status"], "applied")
        self.assertEqual(record["note_id"], gap_fm["id"])

    def test_relabel_materialization_creates_knowledge_note(self) -> None:
        store = EventStore()
        judge_run_key = "judge-knowledge"
        candidate_key = "candidate-knowledge"
        store.upsert_judge_run(
            judge_run_key=judge_run_key,
            partition_key="partition-knowledge",
            window_id="window-knowledge",
            start_ordinal=1,
            end_ordinal=2,
            window_index=1,
            status="judged",
            prompt_version="v1",
            labels=[{"label": "gap", "score": 0.8, "reasons": ["verified_fact"]}],
            decision={},
            model_hint="codex",
        )
        window = {
            "window_id": "window-knowledge",
            "partition_key": "partition-knowledge",
            "start_ordinal": 1,
            "end_ordinal": 2,
            "checkpoints": [
                {
                    "summary": "原因が分かった",
                    "content_excerpt": "設定不足が原因だった",
                    "project": self.project,
                    "repo": "github.com/example/repo",
                }
            ],
        }
        store.upsert_promotion_candidate(
            candidate_key=candidate_key,
            window_id="window-knowledge",
            judge_run_key=judge_run_key,
            label="gap",
            status="pending_review",
            score=0.8,
            slice_fingerprint="window-knowledge",
            reasons=["verified_fact"],
            payload={"window": window, "decision": {}},
        )
        review_seq = store.record_candidate_review(
            review_id="review-knowledge",
            candidate_key=candidate_key,
            judge_run_key=judge_run_key,
            window_id="window-knowledge",
            ai_labels=[{"label": "gap", "score": 0.8}],
            ai_score={"gap": 0.8},
            human_verdict="relabeled",
            human_label="knowledge",
            review_comment=None,
            reviewed_by="tester",
        )
        store.enqueue_materialization_resolution(
            candidate_key=candidate_key,
            review_seq=review_seq,
            effective_label="knowledge",
            materialization_key="mat-knowledge",
            judge_run_key=judge_run_key,
            window_id="window-knowledge",
            payload={"candidate_key": candidate_key},
            project=self.project,
            cwd=str(self.vault),
            repo="github.com/example/repo",
        )

        result = run_once()

        self.assertEqual(result["failed"], 0)
        knowledge_files = sorted((self.vault / "projects" / self.project / "knowledge").glob("*.md"))
        self.assertEqual(len(knowledge_files), 1)
        text = knowledge_files[0].read_text(encoding="utf-8")
        fm = parse_frontmatter(text) or {}
        self.assertEqual(fm["summary"], "原因が分かった")
        self.assertEqual(fm["candidate_key"], "candidate-knowledge")

    def test_session_thin_materialization_adopts_existing_session_note(self) -> None:
        store = EventStore()
        base = {
            "project": self.project,
            "cwd": str(self.vault),
            "session_id": "session-thin",
        }
        store.append(
            normalize_event(
                tool="codex",
                client="codex-cli",
                layer="client_hook",
                event="turn_checkpointed",
                payload={**base, "summary": "最初のやりとり", "content": "現状を確認した"},
            )
        )
        store.append(
            normalize_event(
                tool="codex",
                client="codex-cli",
                layer="client_hook",
                event="turn_checkpointed",
                payload={
                    **base,
                    "summary": "ここで一区切り",
                    "content": "thin log にまとまる",
                    "final_hint": True,
                    "checkpoint_kind": "session_end",
                },
            )
        )
        initial = run_once(maintenance=True)
        self.assertEqual(initial["failed"], 0)
        session_files = sorted((self.vault / "projects" / self.project / "session-log").glob("*.md"))
        self.assertEqual(len(session_files), 1)
        session_fm = parse_frontmatter(session_files[0].read_text(encoding="utf-8")) or {}

        partition_key = store.checkpoint_partition_keys(limit=1)[0]
        windows = build_windows(partition_key)
        payload = build_window_payload(windows[0])
        judge_run_key = "judge-session-thin"
        candidate_key = "candidate-session-thin"
        store.upsert_judge_run(
            judge_run_key=judge_run_key,
            partition_key=payload["partition_key"],
            window_id=payload["window_id"],
            start_ordinal=payload["start_ordinal"],
            end_ordinal=payload["end_ordinal"],
            window_index=payload["window_index"],
            status="judged",
            prompt_version="v1",
            labels=[],
            decision={"should_emit_thin_session": True},
            model_hint="codex",
        )
        store.upsert_promotion_candidate(
            candidate_key=candidate_key,
            window_id=payload["window_id"],
            judge_run_key=judge_run_key,
            label="session_thin",
            status="pending_review",
            score=1.0,
            slice_fingerprint=payload["window_id"],
            reasons=["carry_chain_terminal", "no_strong_labels"],
            payload={"window": payload, "decision": {"should_emit_thin_session": True}},
        )
        review_seq = store.record_candidate_review(
            review_id="review-session-thin",
            candidate_key=candidate_key,
            judge_run_key=judge_run_key,
            window_id=payload["window_id"],
            ai_labels=[{"label": "session_thin", "score": 1.0}],
            ai_score={"session_thin": 1.0},
            human_verdict="accepted",
            human_label=None,
            review_comment=None,
            reviewed_by="tester",
        )
        store.enqueue_materialization_resolution(
            candidate_key=candidate_key,
            review_seq=review_seq,
            effective_label="session_thin",
            materialization_key="mat-session-thin",
            judge_run_key=judge_run_key,
            window_id=payload["window_id"],
            payload={"candidate_key": candidate_key},
            project=self.project,
            cwd=str(self.vault),
            repo=None,
        )

        result = run_once()

        self.assertEqual(result["failed"], 0)
        session_files_after = sorted((self.vault / "projects" / self.project / "session-log").glob("*.md"))
        self.assertEqual(len(session_files_after), 1)
        with store.transaction() as conn:
            record = conn.execute(
                "SELECT status, note_id, note_path FROM materialization_records WHERE materialization_key='mat-session-thin'"
            ).fetchone()
            candidate = conn.execute(
                "SELECT status FROM promotion_candidates WHERE candidate_key=?",
                (candidate_key,),
            ).fetchone()
        self.assertEqual(record["status"], "applied")
        self.assertEqual(record["note_id"], session_fm["id"])
        self.assertEqual(candidate["status"], "materialized")

    def test_materialization_repair_pending_adopts_existing_note_by_materialization_key(self) -> None:
        store = EventStore()
        judge_run_key = "judge-repair"
        candidate_key = "candidate-repair"
        store.upsert_judge_run(
            judge_run_key=judge_run_key,
            partition_key="partition-repair",
            window_id="window-repair",
            start_ordinal=1,
            end_ordinal=2,
            window_index=1,
            status="judged",
            prompt_version="v1",
            labels=[{"label": "gap", "score": 0.9, "reasons": ["user_correction"]}],
            decision={},
            model_hint="codex",
        )
        window = {
            "window_id": "window-repair",
            "partition_key": "partition-repair",
            "start_ordinal": 1,
            "end_ordinal": 2,
            "checkpoints": [
                {
                    "summary": "修正方針を確定した",
                    "content_excerpt": "既存の返答を短くする",
                    "project": self.project,
                    "repo": "github.com/example/repo",
                }
            ],
        }
        store.upsert_promotion_candidate(
            candidate_key=candidate_key,
            window_id="window-repair",
            judge_run_key=judge_run_key,
            label="gap",
            status="pending_review",
            score=0.9,
            slice_fingerprint="window-repair",
            reasons=["user_correction"],
            payload={"window": window, "decision": {}},
        )
        review_seq = store.record_candidate_review(
            review_id="review-repair",
            candidate_key=candidate_key,
            judge_run_key=judge_run_key,
            window_id="window-repair",
            ai_labels=[{"label": "gap", "score": 0.9}],
            ai_score={"gap": 0.9},
            human_verdict="accepted",
            human_label=None,
            review_comment=None,
            reviewed_by="tester",
        )
        save_note_by_type(
            note_type="gap",
            slug="修正方針を確定した",
            summary="修正方針を確定した",
            content="既存の返答を短くする",
            ai_tool="codex",
            ai_client="codex-cli",
            project=self.project,
            cwd=str(self.vault),
            repo="github.com/example/repo",
            tags=["promotion", "gap"],
            related=[],
            extra_fields={"materialization_key": "mat-repair"},
        )
        gap_files = sorted((self.vault / "projects" / self.project / "gap").glob("*.md"))
        existing_fm = parse_frontmatter(gap_files[0].read_text(encoding="utf-8")) or {}
        store.upsert_materialization_record(
            materialization_key="mat-repair",
            candidate_key=candidate_key,
            review_seq=review_seq,
            judge_run_key=judge_run_key,
            window_id="window-repair",
            materialized_label="gap",
            effective_label="gap",
            status="repair_pending",
            payload={"candidate_key": candidate_key},
        )
        store.enqueue_materialization_resolution(
            candidate_key=candidate_key,
            review_seq=review_seq,
            effective_label="gap",
            materialization_key="mat-repair",
            judge_run_key=judge_run_key,
            window_id="window-repair",
            payload={"candidate_key": candidate_key},
            project=self.project,
            cwd=str(self.vault),
            repo="github.com/example/repo",
        )

        result = run_once()

        self.assertEqual(result["failed"], 0)
        gap_files_after = sorted((self.vault / "projects" / self.project / "gap").glob("*.md"))
        self.assertEqual(len(gap_files_after), 1)
        with store.transaction() as conn:
            record = conn.execute(
                "SELECT status, note_id FROM materialization_records WHERE materialization_key='mat-repair'"
            ).fetchone()
        self.assertEqual(record["status"], "applied")
        self.assertEqual(record["note_id"], existing_fm["id"])

    def test_materialization_uses_review_seq_snapshot(self) -> None:
        store = EventStore()
        judge_run_key = "judge-snapshot"
        candidate_key = "candidate-snapshot"
        store.upsert_judge_run(
            judge_run_key=judge_run_key,
            partition_key="partition-snapshot",
            window_id="window-snapshot",
            start_ordinal=1,
            end_ordinal=2,
            window_index=1,
            status="judged",
            prompt_version="v1",
            labels=[{"label": "gap", "score": 0.9, "reasons": ["user_correction"]}],
            decision={},
            model_hint="codex",
        )
        window = {
            "window_id": "window-snapshot",
            "partition_key": "partition-snapshot",
            "start_ordinal": 1,
            "end_ordinal": 2,
            "checkpoints": [
                {
                    "summary": "短く返してほしい",
                    "content_excerpt": "返答が長すぎる",
                    "project": self.project,
                    "repo": "github.com/example/repo",
                }
            ],
        }
        store.upsert_promotion_candidate(
            candidate_key=candidate_key,
            window_id="window-snapshot",
            judge_run_key=judge_run_key,
            label="gap",
            status="pending_review",
            score=0.9,
            slice_fingerprint="window-snapshot",
            reasons=["user_correction"],
            payload={"window": window, "decision": {}},
        )
        review_seq = store.record_candidate_review(
            review_id="review-snapshot-1",
            candidate_key=candidate_key,
            judge_run_key=judge_run_key,
            window_id="window-snapshot",
            ai_labels=[{"label": "gap", "score": 0.9}],
            ai_score={"gap": 0.9},
            human_verdict="accepted",
            human_label=None,
            review_comment=None,
            reviewed_by="tester",
        )
        with store.transaction() as conn:
            conn.execute(
                """
                INSERT INTO candidate_reviews(
                  review_id, candidate_key, review_seq, window_id, judge_run_key,
                  ai_labels_json, ai_score_json, human_verdict, human_label,
                  review_comment, reviewed_by, reviewed_at
                ) VALUES (
                  'review-snapshot-2', ?, 2, 'window-snapshot', ?, '[]', '{}',
                  'rejected', NULL, NULL, 'tester', '2026-03-25T00:00:00+00:00'
                )
                """,
                (candidate_key, judge_run_key),
            )
            conn.execute(
                """
                UPDATE promotion_candidates
                SET status='rejected'
                WHERE candidate_key=?
                """,
                (candidate_key,),
            )
        store.enqueue_materialization_resolution(
            candidate_key=candidate_key,
            review_seq=review_seq,
            effective_label="gap",
            materialization_key="mat-snapshot",
            judge_run_key=judge_run_key,
            window_id="window-snapshot",
            payload={"candidate_key": candidate_key},
            project=self.project,
            cwd=str(self.vault),
            repo="github.com/example/repo",
        )

        result = run_once()

        self.assertEqual(result["failed"], 0)
        gap_files = sorted((self.vault / "projects" / self.project / "gap").glob("*.md"))
        text = gap_files[0].read_text(encoding="utf-8")
        self.assertIn("review_verdict: accepted", text)

    def test_materialization_finalize_failure_marks_repair_pending(self) -> None:
        store = EventStore()
        judge_run_key = "judge-finalize-loss"
        candidate_key = "candidate-finalize-loss"
        store.upsert_judge_run(
            judge_run_key=judge_run_key,
            partition_key="partition-finalize-loss",
            window_id="window-finalize-loss",
            start_ordinal=1,
            end_ordinal=2,
            window_index=1,
            status="judged",
            prompt_version="v1",
            labels=[{"label": "gap", "score": 0.91, "reasons": ["user_correction"]}],
            decision={},
            model_hint="codex",
        )
        window = {
            "window_id": "window-finalize-loss",
            "partition_key": "partition-finalize-loss",
            "start_ordinal": 1,
            "end_ordinal": 2,
            "checkpoints": [
                {
                    "summary": "方向性が違う",
                    "content_excerpt": "ユーザーが違うと言っている",
                    "project": self.project,
                    "repo": "github.com/example/repo",
                }
            ],
        }
        store.upsert_promotion_candidate(
            candidate_key=candidate_key,
            window_id="window-finalize-loss",
            judge_run_key=judge_run_key,
            label="gap",
            status="pending_review",
            score=0.91,
            slice_fingerprint="window-finalize-loss",
            reasons=["user_correction"],
            payload={"window": window, "decision": {}},
        )
        review_seq = store.record_candidate_review(
            review_id="review-finalize-loss",
            candidate_key=candidate_key,
            judge_run_key=judge_run_key,
            window_id="window-finalize-loss",
            ai_labels=[{"label": "gap", "score": 0.91}],
            ai_score={"gap": 0.91},
            human_verdict="accepted",
            human_label=None,
            review_comment=None,
            reviewed_by="tester",
        )
        store.enqueue_materialization_resolution(
            candidate_key=candidate_key,
            review_seq=review_seq,
            effective_label="gap",
            materialization_key="mat-finalize-loss",
            judge_run_key=judge_run_key,
            window_id="window-finalize-loss",
            payload={"candidate_key": candidate_key},
            project=self.project,
            cwd=str(self.vault),
            repo="github.com/example/repo",
        )

        with patch(
            "kb_mcp.events.policies.promotion_applier.EventStore.finalize_materialization_record",
            return_value=False,
        ):
            with patch(
                "kb_mcp.events.policies.promotion_applier.EventStore.release_materialization_record",
                return_value=False,
            ):
                result = run_once()

        self.assertEqual(result["failed"], 1)
        record = store.get_materialization_record("mat-finalize-loss")
        candidate = store.get_promotion_candidate(candidate_key)
        self.assertEqual(record["status"], "repair_pending")
        self.assertIsNone(record["lease_owner"])
        self.assertEqual(record["last_error"], "lease lost before repair_pending release")
        self.assertEqual(candidate["status"], "accepted")

    def test_heartbeat_loop_extends_lease_while_running(self) -> None:
        store = _HeartbeatStore([True])
        stop_event = _SequenceStopEvent([False, True])
        lost_lease = threading.Event()

        _heartbeat_loop(
            store,
            "mat-heartbeat",
            "worker-a",
            3,
            stop_event,
            lost_lease,
        )

        self.assertEqual(len(store.calls), 1)
        self.assertEqual(store.calls[0][0], "mat-heartbeat")
        self.assertEqual(store.calls[0][1], "worker-a")
        self.assertEqual(store.calls[0][2], 3)
        self.assertFalse(lost_lease.is_set())

    def test_heartbeat_loop_marks_lost_lease_on_failed_refresh(self) -> None:
        store = _HeartbeatStore([False])
        stop_event = _SequenceStopEvent([False])
        lost_lease = threading.Event()

        _heartbeat_loop(
            store,
            "mat-heartbeat-fail",
            "worker-b",
            4,
            stop_event,
            lost_lease,
        )

        self.assertEqual(len(store.calls), 1)
        self.assertTrue(lost_lease.is_set())

    def test_heartbeat_loop_marks_lost_lease_on_refresh_exception(self) -> None:
        store = _ExceptionalHeartbeatStore()
        stop_event = _SequenceStopEvent([False])
        lost_lease = threading.Event()

        _heartbeat_loop(
            store,
            "mat-heartbeat-error",
            "worker-c",
            5,
            stop_event,
            lost_lease,
        )

        self.assertTrue(lost_lease.is_set())

    def test_load_plan_falls_back_to_legacy_path(self) -> None:
        promotions_dir = self.config_dir / "runtime" / "events" / "promotions"
        promotions_dir.mkdir(parents=True, exist_ok=True)
        legacy_path = promotions_dir / "materialize__candidate-legacy__gap.json"
        legacy_path.write_text('{"legacy": true}\n', encoding="utf-8")

        row = _Row(
            logical_key="materialize:candidate-legacy:gap",
            aggregate_type="review_materialization",
            aggregate_version=1,
        )

        plan = _load_plan(row)  # type: ignore[arg-type]
        self.assertEqual(plan, {"legacy": True})

    def test_materialization_target_resolves_project_from_cwd_and_repo(self) -> None:
        store = EventStore()
        store.upsert_judge_run(
            judge_run_key="judge-target",
            partition_key="partition-target",
            window_id="window-target",
            start_ordinal=1,
            end_ordinal=2,
            window_index=1,
            status="judged",
            prompt_version="v1",
        )
        store.upsert_promotion_candidate(
            candidate_key="cand-target",
            window_id="window-target",
            judge_run_key="judge-target",
            label="gap",
            status="accepted",
            score=0.9,
            slice_fingerprint="window-target",
            reasons=["user_correction"],
            payload={"window_id": "window-target"},
        )
        store.upsert_materialization_record(
            materialization_key="mat-target",
            candidate_key="cand-target",
            review_seq=1,
            judge_run_key="judge-target",
            window_id="window-target",
            materialized_label="gap",
            effective_label="gap",
            status="applying",
            payload={"candidate_key": "cand-target"},
            lease_owner="worker-target",
            lease_expires_at="2026-03-25T00:10:00+00:00",
            lease_epoch=1,
        )
        record = store.get_materialization_record("mat-target")
        plan = {
            "note_type": "gap",
            "slug": "予約先を決める",
            "project": None,
            "cwd": str(self.vault),
            "repo": "github.com/example/repo",
            "materialization_key": "mat-target",
        }

        with patch(
            "kb_mcp.events.policies.promotion_applier.resolve_project",
            return_value=(self.project, "github.com/example/repo"),
        ):
            target = _materialization_target(store, record, plan, "worker-target")  # type: ignore[arg-type]

        self.assertTrue(target["note_path"].endswith(f"/projects/{self.project}/gap/{target['filename']}"))

    def test_materialization_target_repairs_partial_reserved_pair(self) -> None:
        store = EventStore()
        store.upsert_judge_run(
            judge_run_key="judge-target-partial",
            partition_key="partition-target-partial",
            window_id="window-target-partial",
            start_ordinal=1,
            end_ordinal=2,
            window_index=1,
            status="judged",
            prompt_version="v1",
        )
        store.upsert_promotion_candidate(
            candidate_key="cand-target-partial",
            window_id="window-target-partial",
            judge_run_key="judge-target-partial",
            label="gap",
            status="accepted",
            score=0.9,
            slice_fingerprint="window-target-partial",
            reasons=["user_correction"],
            payload={"window_id": "window-target-partial"},
        )
        store.upsert_materialization_record(
            materialization_key="mat-target-partial",
            candidate_key="cand-target-partial",
            review_seq=1,
            judge_run_key="judge-target-partial",
            window_id="window-target-partial",
            materialized_label="gap",
            effective_label="gap",
            status="applying",
            payload={"candidate_key": "cand-target-partial"},
            note_id="01PARTIALNOTEID00000000000000",
            note_path=None,
            lease_owner="worker-target",
            lease_expires_at="2026-03-25T00:10:00+00:00",
            lease_epoch=1,
        )
        record = store.get_materialization_record("mat-target-partial")
        plan = {
            "note_type": "gap",
            "slug": "予約先を再整合する",
            "project": self.project,
            "cwd": str(self.vault),
            "repo": "github.com/example/repo",
            "materialization_key": "mat-target-partial",
        }

        target = _materialization_target(store, record, plan, "worker-target")  # type: ignore[arg-type]
        repaired = store.get_materialization_record("mat-target-partial")

        self.assertEqual(repaired["note_id"], target["note_id"])
        self.assertEqual(Path(str(repaired["note_path"])).name, target["filename"])
        self.assertNotEqual(target["note_id"], "01PARTIALNOTEID00000000000000")
