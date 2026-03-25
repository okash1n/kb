from __future__ import annotations

import json
import os
import sqlite3
import stat
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml

from kb_mcp.config import load_config
from kb_mcp.events.candidates import detect_candidates
from kb_mcp.events.middleware import with_tool_events
from kb_mcp.events.normalize import normalize_event
from kb_mcp.events.schema import db_path, ensure_schema, schema_locked_connection
from kb_mcp.events.store import EventStore
from kb_mcp.events.types import EventEnvelope, utc_now_iso
from kb_mcp.events.worker import run_once
from kb_mcp.note import parse_frontmatter


class EventPipelineTest(unittest.TestCase):
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
        for subdir in ["projects/demo/session-log", "projects/demo/draft", "projects/demo/adr", "projects/demo/gap", "projects/demo/knowledge", "projects/demo/draft", "general/knowledge", "general/requirements", "inbox"]:
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

    def test_session_events_finalize_immutable_note(self) -> None:
        store = EventStore()
        payload = {
            "project": self.project,
            "cwd": str(self.vault),
            "session_id": "session-1",
            "summary": "Finished a session",
            "content": "Body from hook payload",
        }
        store.append(normalize_event(tool="codex", client="codex-cli", layer="session_launcher", event="session_started", payload=payload))
        store.append(normalize_event(tool="codex", client="codex-cli", layer="session_launcher", event="process_exit", payload={**payload, "exit_code": 0}))
        store.append(normalize_event(tool="codex", client="codex-cli", layer="session_launcher", event="session_ended", payload=payload))
        result = run_once(maintenance=True)
        self.assertGreaterEqual(result["applied"], 1)
        files = sorted((self.vault / "projects" / self.project / "session-log").glob("*.md"))
        self.assertEqual(len(files), 1)
        mode = files[0].stat().st_mode
        self.assertFalse(mode & stat.S_IWUSR)

    def test_hook_session_end_writes_checkpoint_without_finalizing_note(self) -> None:
        store = EventStore()
        payload = {
            "project": self.project,
            "cwd": str(self.vault),
            "session_id": "session-hook-only",
            "summary": "Turn finished",
            "content": "Short turn excerpt",
        }
        store.append(normalize_event(tool="codex", client="codex-cli", layer="client_hook", event="turn_checkpointed", payload=payload))
        result = run_once(maintenance=True)
        self.assertGreaterEqual(result["applied"], 1)
        files = sorted((self.vault / "projects" / self.project / "session-log").glob("*.md"))
        self.assertEqual(len(files), 0)
        checkpoints = sorted((self.config_dir / "runtime" / "events" / "checkpoints").glob("*.json"))
        self.assertEqual(len(checkpoints), 1)

    def test_checkpoint_events_accumulate_multiple_ordinals(self) -> None:
        store = EventStore()
        base = {
            "project": self.project,
            "cwd": str(self.vault),
            "session_id": "session-many",
            "summary": "Turn finished",
            "content": "Short turn excerpt",
        }
        first = normalize_event(tool="codex", client="codex-cli", layer="client_hook", event="turn_checkpointed", payload=base)
        second = normalize_event(tool="codex", client="codex-cli", layer="client_hook", event="turn_checkpointed", payload=base)
        result1 = store.append(first)
        result2 = store.append(second)
        self.assertNotEqual(result1.logical_key, result2.logical_key)
        self.assertTrue(result1.logical_key.endswith(":1"))
        self.assertTrue(result2.logical_key.endswith(":2"))
        with EventStore().transaction() as conn:
            keys = [
                row["logical_key"]
                for row in conn.execute(
                    "SELECT logical_key FROM events WHERE summary='Turn finished' ORDER BY rowid"
                ).fetchall()
            ]
        self.assertEqual(keys[-2:], [result1.logical_key, result2.logical_key])

    def test_unmanaged_checkpoint_uses_distinct_partition_key(self) -> None:
        one = normalize_event(
            tool="codex",
            client="codex-cli",
            layer="client_hook",
            event="turn_checkpointed",
            payload={"cwd": "/tmp/a", "summary": "one", "occurred_at": "2026-03-25T00:00:00+00:00"},
        )
        two = normalize_event(
            tool="codex",
            client="codex-cli",
            layer="client_hook",
            event="turn_checkpointed",
            payload={"cwd": "/tmp/b", "summary": "two", "occurred_at": "2026-03-25T00:00:00+00:00"},
        )
        self.assertNotEqual(
            one.aggregate_state["checkpoint_partition_key"],
            two.aggregate_state["checkpoint_partition_key"],
        )
        store = EventStore()
        result_one = store.append(one)
        result_two = store.append(two)
        self.assertNotEqual(result_one.logical_key, result_two.logical_key)

    def test_unmanaged_compact_events_do_not_collapse_to_single_key(self) -> None:
        one = normalize_event(
            tool="codex",
            client="codex-cli",
            layer="client_hook",
            event="compact_finished",
            payload={"cwd": "/tmp/a", "summary": "one", "occurred_at": "2026-03-25T00:00:00+00:00"},
        )
        two = normalize_event(
            tool="codex",
            client="codex-cli",
            layer="client_hook",
            event="compact_finished",
            payload={"cwd": "/tmp/b", "summary": "two", "occurred_at": "2026-03-25T00:00:00+00:00"},
        )
        self.assertNotEqual(one.logical_key, two.logical_key)

    def test_gap_candidate_is_embedded_in_checkpoint(self) -> None:
        store = EventStore()
        payload = {
            "project": self.project,
            "cwd": str(self.vault),
            "session_id": "session-gap",
            "summary": "長すぎて読まれへんわ",
            "content": "そうじゃなくて、もっと短く出すべきや",
        }
        store.append(normalize_event(tool="codex", client="codex-cli", layer="client_hook", event="turn_checkpointed", payload=payload))
        run_once(maintenance=True)
        checkpoints = sorted((self.config_dir / "runtime" / "events" / "checkpoints").glob("*.json"))
        data = json.loads(checkpoints[0].read_text(encoding="utf-8"))
        self.assertTrue(data["candidates"]["has_candidates"])
        self.assertEqual(data["candidates"]["items"][0]["kind"], "gap")
        candidate_files = sorted((self.config_dir / "runtime" / "events" / "candidates").glob("*.json"))
        self.assertEqual(len(candidate_files), 1)

    def test_knowledge_candidate_requires_stronger_signal(self) -> None:
        detected = detect_candidates(
            "原因は schema 上の制約や",
            "config.toml で feature が必要やと確認できた",
        )
        self.assertTrue(detected["has_candidates"])
        self.assertEqual(detected["items"][0]["kind"], "knowledge")

    def test_error_event_writes_incident_draft(self) -> None:
        store = EventStore()
        payload = {
            "project": self.project,
            "cwd": str(self.vault),
            "session_id": "session-2",
            "message": "Something went wrong",
            "error_fingerprint": "boom",
        }
        store.append(normalize_event(tool="claude", client="claude-code", layer="client_hook", event="agent_error", payload=payload))
        result = run_once()
        self.assertGreaterEqual(result["applied"], 1)
        files = sorted((self.vault / "projects" / self.project / "draft").glob("*.md"))
        self.assertEqual(len(files), 1)
        self.assertIn("incident", files[0].read_text(encoding="utf-8"))

    def test_tool_wrapper_emits_authoritative_events(self) -> None:
        observed: list[str] = []

        def sample(*, project: str) -> str:
            observed.append(project)
            return project

        wrapped = with_tool_events("kb", "kb-mcp", "sample", sample)
        result = wrapped(project=self.project, ctx=None)
        self.assertEqual(result, self.project)
        self.assertEqual(observed, [self.project])
        logical_rows = []
        with EventStore().transaction() as conn:
            logical_rows = conn.execute(
                "SELECT event_name, logical_key, tool_call_id FROM events WHERE aggregate_type='tool' ORDER BY rowid"
            ).fetchall()
        self.assertEqual([row["event_name"] for row in logical_rows], ["tool_started", "tool_succeeded"])
        self.assertEqual(logical_rows[0]["logical_key"], logical_rows[1]["logical_key"])
        self.assertEqual(logical_rows[0]["tool_call_id"], logical_rows[1]["tool_call_id"])

    def test_gap_save_emits_save_request_id_and_saved_note_metadata(self) -> None:
        from kb_mcp.server import gap

        with patch("kb_mcp.tools.save._resolve_or_error", return_value=(self.project, "github.com/example/repo")):
            gap(
                slug="test-gap",
                summary="summary",
                content="content",
                ai_tool="codex",
                ai_client="codex-cli",
                project=self.project,
                ctx=None,
            )

        with EventStore().transaction() as conn:
            rows = conn.execute(
                "SELECT event_name, raw_payload_json FROM events WHERE aggregate_type='tool' ORDER BY rowid"
            ).fetchall()
        payloads = [json.loads(row["raw_payload_json"]) for row in rows if row["event_name"] == "tool_succeeded"]
        self.assertEqual(len(payloads), 1)
        self.assertIn("save_request_id", payloads[0])
        self.assertIn("saved_note_id", payloads[0])
        self.assertEqual(payloads[0]["saved_note_type"], "gap")
        note_files = sorted((self.vault / "projects" / self.project / "gap").glob("*.md"))
        self.assertIn("save_request_id:", note_files[0].read_text(encoding="utf-8"))

    def test_draft_save_emits_saved_note_metadata(self) -> None:
        from kb_mcp.server import draft

        with patch("kb_mcp.tools.save.resolve_project", return_value=(self.project, "github.com/example/repo")):
            draft(
                slug="test-draft",
                summary="summary",
                content="content",
                ai_tool="codex",
                ai_client="codex-cli",
                project=self.project,
                ctx=None,
            )

        with EventStore().transaction() as conn:
            rows = conn.execute(
                "SELECT event_name, raw_payload_json FROM events WHERE aggregate_type='tool' ORDER BY rowid DESC"
            ).fetchall()
        payloads = [json.loads(row["raw_payload_json"]) for row in rows if row["event_name"] == "tool_succeeded"]
        self.assertEqual(payloads[0]["saved_note_type"], "draft")

    def test_gap_save_promotes_rich_session_log(self) -> None:
        store = EventStore()
        base = {
            "project": self.project,
            "cwd": str(self.vault),
            "session_id": "session-rich",
        }
        store.append(
            normalize_event(
                tool="codex",
                client="codex-cli",
                layer="client_hook",
                event="turn_checkpointed",
                payload={**base, "summary": "最初の相談", "content": "ここで前提を揃えた"},
            )
        )
        store.append(
            normalize_event(
                tool="codex",
                client="codex-cli",
                layer="client_hook",
                event="turn_checkpointed",
                payload={**base, "summary": "方針が固まった", "content": "session log は薄くしたい"},
            )
        )

        from kb_mcp.server import gap

        with patch("kb_mcp.tools.save._resolve_or_error", return_value=(self.project, "github.com/example/repo")):
            gap(
                slug="too-long",
                summary="長すぎて読まれへん",
                content="もっと短く返してほしい",
                ai_tool="codex",
                ai_client="codex-cli",
                project=self.project,
                ctx=None,
            )

        gap_files = sorted((self.vault / "projects" / self.project / "gap").glob("*.md"))
        gap_fm = parse_frontmatter(gap_files[0].read_text(encoding="utf-8")) or {}
        session_files = sorted((self.vault / "projects" / self.project / "session-log").glob("*.md"))
        self.assertEqual(len(session_files), 1)
        text = session_files[0].read_text(encoding="utf-8")
        fm = parse_frontmatter(text) or {}
        self.assertEqual(fm.get("density"), "rich")
        self.assertIn(gap_fm["id"], fm.get("related", []))
        self.assertIn("最初の相談", text)
        self.assertIn("方針が固まった", text)

    def test_final_hint_checkpoint_promotes_thin_session_log(self) -> None:
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
                payload={**base, "summary": "最初のやりとり", "content": "まず現状を確認した"},
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
                    "content": "thin log にまとまるはずや",
                    "final_hint": True,
                    "checkpoint_kind": "session_end",
                },
            )
        )

        result = run_once(maintenance=True)
        self.assertGreaterEqual(result["applied"], 1)
        session_files = sorted((self.vault / "projects" / self.project / "session-log").glob("*.md"))
        self.assertEqual(len(session_files), 1)
        text = session_files[0].read_text(encoding="utf-8")
        fm = parse_frontmatter(text) or {}
        self.assertEqual(fm.get("density"), "thin")
        self.assertIn("最初のやりとり", text)
        self.assertIn("ここで一区切り", text)

    def test_dead_letter_can_be_replayed(self) -> None:
        store = EventStore()
        payload = {
            "project": self.project,
            "cwd": str(self.vault),
            "session_id": "session-dead-letter",
            "summary": "Turn finished",
            "content": "Short turn excerpt",
        }
        result = store.append(
            normalize_event(tool="codex", client="codex-cli", layer="client_hook", event="turn_checkpointed", payload=payload)
        )
        with store.transaction() as conn:
            conn.execute(
                "UPDATE outbox SET status='dead_letter', last_error='boom' WHERE logical_key=? AND sink_name='checkpoint_writer'",
                (result.logical_key,),
            )
        self.assertEqual(store.dead_letter_count(), 1)
        replayed = store.replay_dead_letters(limit=10)
        self.assertEqual(replayed, 1)
        with store.transaction() as conn:
            row = conn.execute(
                "SELECT status, last_error FROM outbox WHERE logical_key=? AND sink_name='checkpoint_writer'",
                (result.logical_key,),
            ).fetchone()
        self.assertEqual(row["status"], "ready")
        self.assertIsNone(row["last_error"])

    def test_cleanup_runtime_artifacts_removes_stale_files(self) -> None:
        from kb_mcp.events.retention import cleanup_runtime_artifacts

        checkpoints = self.config_dir / "runtime" / "events" / "checkpoints"
        checkpoints.mkdir(parents=True, exist_ok=True)
        old_file = checkpoints / "old.json"
        old_file.write_text("{}", encoding="utf-8")
        stale = time.time() - 10 * 24 * 60 * 60
        os.utime(old_file, (stale, stale))

        removed = cleanup_runtime_artifacts(checkpoint_days=7)
        self.assertEqual(removed["checkpoints"], 1)
        self.assertFalse(old_file.exists())

    def test_schema_version_5_creates_learning_asset_table(self) -> None:
        with EventStore().transaction() as conn:
            version = conn.execute(
                "SELECT value FROM schema_meta WHERE key='schema_version'"
            ).fetchone()
            tables = {
                row["name"]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
        self.assertEqual(version["value"], "5")
        self.assertTrue(
            {
                "judge_runs",
                "promotion_candidates",
                "candidate_reviews",
                "materialization_records",
                "note_mutations",
                "learning_assets",
            }
            <= tables
        )

    def test_schema_upgrade_from_v3_preserves_existing_rows(self) -> None:
        db_path().parent.mkdir(parents=True, exist_ok=True)
        raw = sqlite3.connect(db_path())
        try:
            raw.execute("CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
            raw.execute("INSERT INTO schema_meta(key, value) VALUES('schema_version', '3')")
            raw.execute(
                """
                CREATE TABLE judge_runs (
                  judge_run_key TEXT PRIMARY KEY,
                  partition_key TEXT NOT NULL,
                  window_id TEXT NOT NULL,
                  start_ordinal INTEGER NOT NULL,
                  end_ordinal INTEGER NOT NULL,
                  window_index INTEGER NOT NULL,
                  status TEXT NOT NULL,
                  labels_json TEXT NOT NULL,
                  decision_json TEXT NOT NULL,
                  prompt_version TEXT NOT NULL,
                  model_hint TEXT,
                  supersedes_judge_run_key TEXT,
                  lease_owner TEXT,
                  lease_expires_at TEXT,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  UNIQUE(window_id, prompt_version)
                )
                """
            )
            raw.execute(
                """
                CREATE TABLE promotion_candidates (
                  candidate_key TEXT PRIMARY KEY,
                  window_id TEXT NOT NULL,
                  judge_run_key TEXT NOT NULL,
                  label TEXT NOT NULL,
                  status TEXT NOT NULL,
                  score REAL,
                  slice_fingerprint TEXT,
                  reasons_json TEXT NOT NULL,
                  payload_json TEXT NOT NULL,
                  last_suggested_at TEXT,
                  suggestion_seq INTEGER NOT NULL DEFAULT 0,
                  resolved_at TEXT,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                )
                """
            )
            raw.execute(
                """
                CREATE TABLE candidate_reviews (
                  review_id TEXT PRIMARY KEY,
                  candidate_key TEXT NOT NULL,
                  review_seq INTEGER NOT NULL,
                  window_id TEXT NOT NULL,
                  judge_run_key TEXT NOT NULL,
                  ai_labels_json TEXT NOT NULL,
                  ai_score_json TEXT NOT NULL,
                  human_verdict TEXT NOT NULL,
                  human_label TEXT,
                  review_comment TEXT,
                  reviewed_by TEXT,
                  reviewed_at TEXT NOT NULL,
                  UNIQUE(candidate_key, review_seq)
                )
                """
            )
            raw.execute(
                """
                INSERT INTO judge_runs(
                  judge_run_key, partition_key, window_id, start_ordinal, end_ordinal,
                  window_index, status, labels_json, decision_json, prompt_version,
                  model_hint, supersedes_judge_run_key, lease_owner, lease_expires_at,
                  created_at, updated_at
                ) VALUES (
                  'legacy-judge', 'partition-legacy', 'window-legacy', 1, 2,
                  1, 'judged', '[]', '{}', 'v1',
                  NULL, NULL, NULL, NULL,
                  '2026-03-25T00:00:00+00:00', '2026-03-25T00:00:00+00:00'
                )
                """
            )
            raw.execute(
                """
                INSERT INTO promotion_candidates(
                  candidate_key, window_id, judge_run_key, label, status, score,
                  slice_fingerprint, reasons_json, payload_json, last_suggested_at,
                  suggestion_seq, resolved_at, created_at, updated_at
                ) VALUES (
                  'legacy-candidate', 'window-legacy', 'legacy-judge', 'gap', 'accepted', 0.9,
                  'slice', '[]', '{}', NULL,
                  0, NULL, '2026-03-25T00:00:00+00:00', '2026-03-25T00:00:00+00:00'
                )
                """
            )
            raw.execute(
                """
                INSERT INTO candidate_reviews(
                  review_id, candidate_key, review_seq, window_id, judge_run_key,
                  ai_labels_json, ai_score_json, human_verdict, human_label,
                  review_comment, reviewed_by, reviewed_at
                ) VALUES (
                  'legacy-review', 'legacy-candidate', 1, 'window-legacy', 'legacy-judge',
                  '[]', '{}', 'accepted', NULL,
                  NULL, NULL, '2026-03-25T00:00:00+00:00'
                )
                """
            )
            raw.commit()
        finally:
            raw.close()

        with schema_locked_connection() as conn:
            version = conn.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()
            judge = conn.execute("SELECT judge_run_key FROM judge_runs WHERE judge_run_key='legacy-judge'").fetchone()
            candidate = conn.execute("SELECT candidate_key FROM promotion_candidates WHERE candidate_key='legacy-candidate'").fetchone()
            review = conn.execute("SELECT review_id FROM candidate_reviews WHERE review_id='legacy-review'").fetchone()
            tables = {
                row["name"]
                for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            }
        self.assertEqual(version["value"], "5")
        self.assertEqual(judge["judge_run_key"], "legacy-judge")
        self.assertEqual(candidate["candidate_key"], "legacy-candidate")
        self.assertEqual(review["review_id"], "legacy-review")
        self.assertIn("materialization_records", tables)
        self.assertIn("note_mutations", tables)
        self.assertIn("learning_assets", tables)
        with schema_locked_connection() as conn:
            asset = conn.execute(
                """
                SELECT memory_class, update_target, scope, confidence, lifecycle, learning_state_visibility
                FROM learning_assets
                WHERE candidate_key='legacy-candidate'
                """
            ).fetchone()
        self.assertIsNotNone(asset)
        self.assertEqual(asset["memory_class"], "gap")
        self.assertEqual(asset["update_target"], "behavior_style")
        self.assertEqual(asset["scope"], "project_local")
        self.assertEqual(asset["confidence"], "reviewed")
        self.assertEqual(asset["lifecycle"], "candidate")
        self.assertEqual(asset["learning_state_visibility"], "candidate")

    def test_schema_backfill_learning_assets_is_idempotent(self) -> None:
        store = EventStore()
        store.upsert_judge_run(
            judge_run_key="judge-backfill",
            partition_key="partition-backfill",
            window_id="window-backfill",
            start_ordinal=1,
            end_ordinal=1,
            window_index=1,
            status="judged",
            prompt_version="v1",
        )
        store.upsert_promotion_candidate(
            candidate_key="candidate-backfill",
            window_id="window-backfill",
            judge_run_key="judge-backfill",
            label="adr",
            status="pending_review",
            score=0.91,
            slice_fingerprint="window-backfill",
            reasons=["decision made"],
            payload={"window_id": "window-backfill"},
        )
        review_seq = store.record_candidate_review(
            review_id="review-backfill",
            candidate_key="candidate-backfill",
            window_id="window-backfill",
            judge_run_key="judge-backfill",
            ai_labels=[{"label": "adr", "score": 0.91}],
            ai_score={"adr": 0.91},
            human_verdict="accepted",
            human_label=None,
        )
        self.assertEqual(review_seq, 1)
        with store.transaction() as conn:
            conn.execute(
                "UPDATE promotion_candidates SET status='accepted' WHERE candidate_key='candidate-backfill'"
            )
        with schema_locked_connection() as conn:
            ensure_schema(conn)
            ensure_schema(conn)
            row = conn.execute(
                "SELECT COUNT(*) AS count FROM learning_assets WHERE candidate_key='candidate-backfill'"
            ).fetchone()
        self.assertEqual(int(row["count"]), 1)

    def test_event_store_upserts_learning_asset(self) -> None:
        store = EventStore()
        store.upsert_learning_asset(
            asset_key="learning:test:1:gap:project_local",
            candidate_key=None,
            review_id=None,
            materialization_key=None,
            note_id=None,
            note_path=None,
            memory_class="gap",
            update_target="behavior_style",
            scope="project_local",
            force="hint",
            confidence="reviewed",
            lifecycle="candidate",
            provenance={"candidate_key": "candidate-gap"},
            traceability={"served": []},
            revocation_path={"rollback_scope": "project_local"},
            learning_state_visibility="candidate",
            source_status="accepted",
        )
        row = store.get_learning_asset("learning:test:1:gap:project_local")
        self.assertIsNotNone(row)
        self.assertEqual(row["memory_class"], "gap")
        counts = store.learning_asset_counts()
        self.assertEqual(counts["total"], 1)
        self.assertEqual(counts["candidate"], 1)

    def test_schema_upgrade_refreshes_candidate_status_check_for_relabeled(self) -> None:
        root = Path(self.tmpdir.name)
        db_file = root / "events-v4-old.sqlite3"
        raw = sqlite3.connect(db_file)
        try:
            raw.execute(
                """
                CREATE TABLE schema_meta (
                  key TEXT PRIMARY KEY,
                  value TEXT NOT NULL
                )
                """
            )
            raw.execute(
                """
                CREATE TABLE judge_runs (
                  judge_run_key TEXT PRIMARY KEY,
                  partition_key TEXT NOT NULL,
                  window_id TEXT NOT NULL,
                  start_ordinal INTEGER NOT NULL,
                  end_ordinal INTEGER NOT NULL,
                  window_index INTEGER NOT NULL,
                  status TEXT NOT NULL,
                  labels_json TEXT NOT NULL,
                  decision_json TEXT NOT NULL,
                  prompt_version TEXT NOT NULL,
                  model_hint TEXT,
                  supersedes_judge_run_key TEXT,
                  lease_owner TEXT,
                  lease_expires_at TEXT,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                )
                """
            )
            raw.execute(
                """
                CREATE TABLE promotion_candidates (
                  candidate_key TEXT PRIMARY KEY,
                  window_id TEXT NOT NULL,
                  judge_run_key TEXT NOT NULL REFERENCES judge_runs(judge_run_key),
                  label TEXT NOT NULL CHECK (label IN ('adr', 'gap', 'knowledge', 'session_thin')),
                  status TEXT NOT NULL CHECK (status IN ('pending_review', 'accepted', 'rejected', 'materialized')),
                  score REAL,
                  slice_fingerprint TEXT,
                  reasons_json TEXT NOT NULL,
                  payload_json TEXT NOT NULL,
                  last_suggested_at TEXT,
                  suggestion_seq INTEGER NOT NULL DEFAULT 0,
                  resolved_at TEXT,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                )
                """
            )
            raw.execute(
                """
                INSERT INTO judge_runs(
                  judge_run_key, partition_key, window_id, start_ordinal, end_ordinal, window_index,
                  status, labels_json, decision_json, prompt_version, model_hint, supersedes_judge_run_key,
                  lease_owner, lease_expires_at, created_at, updated_at
                ) VALUES (
                  'judge-old', 'partition-old', 'window-old', 1, 2, 1,
                  'judged', '[]', '{}', 'v1', NULL, NULL,
                  NULL, NULL, '2026-03-25T00:00:00+00:00', '2026-03-25T00:00:00+00:00'
                )
                """
            )
            raw.execute(
                """
                INSERT INTO promotion_candidates(
                  candidate_key, window_id, judge_run_key, label, status, score, slice_fingerprint,
                  reasons_json, payload_json, last_suggested_at, suggestion_seq, resolved_at, created_at, updated_at
                ) VALUES (
                  'cand-old', 'window-old', 'judge-old', 'gap', 'accepted', 0.9, 'fp',
                  '[]', '{}', NULL, 0, NULL, '2026-03-25T00:00:00+00:00', '2026-03-25T00:00:00+00:00'
                )
                """
            )
            raw.commit()
        finally:
            raw.close()

        migrated = sqlite3.connect(db_file)
        migrated.row_factory = sqlite3.Row
        try:
            ensure_schema(migrated)
            migrated.execute(
                """
                UPDATE promotion_candidates
                SET status='relabeled'
                WHERE candidate_key='cand-old'
                """
            )
            row = migrated.execute(
                "SELECT status FROM promotion_candidates WHERE candidate_key='cand-old'"
            ).fetchone()
        finally:
            migrated.close()
        self.assertEqual(row["status"], "relabeled")

    def test_enqueue_materialization_resolution_creates_logical_event_and_outbox(self) -> None:
        store = EventStore()
        store.upsert_judge_run(
            judge_run_key="judge-materialize-1",
            partition_key="partition-1",
            window_id="window-materialize-1",
            start_ordinal=1,
            end_ordinal=2,
            window_index=1,
            status="judged",
            prompt_version="v1",
        )
        store.upsert_promotion_candidate(
            candidate_key="cand-materialize-1",
            window_id="window-materialize-1",
            judge_run_key="judge-materialize-1",
            label="knowledge",
            status="accepted",
            score=0.91,
            slice_fingerprint="window-materialize-1",
            reasons=["confirmed_fact"],
            payload={"window_id": "window-materialize-1"},
        )

        result = store.enqueue_materialization_resolution(
            candidate_key="cand-materialize-1",
            review_seq=1,
            effective_label="knowledge",
            materialization_key="mat-1",
            judge_run_key="judge-materialize-1",
            window_id="window-materialize-1",
            payload={"candidate_key": "cand-materialize-1"},
            project=self.project,
            cwd=str(self.vault),
            repo="demo/repo",
        )

        self.assertEqual(result.aggregate_type, "review_materialization")
        with store.transaction() as conn:
            logical = conn.execute(
                "SELECT aggregate_type, status, project, repo, cwd, aggregate_state_json FROM logical_events WHERE logical_key=?",
                ("materialize:cand-materialize-1:knowledge",),
            ).fetchone()
            outbox = conn.execute(
                """
                SELECT sink_name
                FROM outbox
                WHERE logical_key=?
                ORDER BY sink_name
                """,
                ("materialize:cand-materialize-1:knowledge",),
            ).fetchall()
            record = conn.execute(
                """
                SELECT status, review_seq, effective_label
                FROM materialization_records
                WHERE materialization_key='mat-1'
                """
            ).fetchone()
        self.assertEqual(logical["aggregate_type"], "review_materialization")
        self.assertEqual(logical["status"], "ready")
        self.assertEqual(logical["project"], self.project)
        self.assertEqual(logical["repo"], "demo/repo")
        self.assertEqual(logical["cwd"], str(self.vault))
        self.assertEqual([row["sink_name"] for row in outbox], ["promotion_applier", "promotion_planner"])
        self.assertEqual(record["status"], "planned")
        self.assertEqual(record["review_seq"], 1)
        self.assertEqual(record["effective_label"], "knowledge")

    def test_enqueue_materialization_resolution_skips_older_review_seq_when_logical_event_exists(self) -> None:
        store = EventStore()
        store.upsert_judge_run(
            judge_run_key="judge-materialize-2",
            partition_key="partition-2",
            window_id="window-materialize-2",
            start_ordinal=1,
            end_ordinal=2,
            window_index=1,
            status="judged",
            prompt_version="v1",
        )
        store.upsert_promotion_candidate(
            candidate_key="cand-materialize-2",
            window_id="window-materialize-2",
            judge_run_key="judge-materialize-2",
            label="gap",
            status="accepted",
            score=0.95,
            slice_fingerprint="window-materialize-2",
            reasons=["user_correction"],
            payload={"window_id": "window-materialize-2"},
        )
        store.enqueue_materialization_resolution(
            candidate_key="cand-materialize-2",
            review_seq=2,
            effective_label="gap",
            materialization_key="mat-2",
            judge_run_key="judge-materialize-2",
            window_id="window-materialize-2",
            payload={"candidate_key": "cand-materialize-2"},
        )
        with store.transaction() as conn:
            before = conn.execute(
                "SELECT COUNT(*) AS count FROM outbox WHERE logical_key='materialize:cand-materialize-2:gap'"
            ).fetchone()

        result = store.enqueue_materialization_resolution(
            candidate_key="cand-materialize-2",
            review_seq=1,
            effective_label="gap",
            materialization_key="mat-2",
            judge_run_key="judge-materialize-2",
            window_id="window-materialize-2",
            payload={"candidate_key": "cand-materialize-2"},
        )

        self.assertEqual(result.aggregate_version, 1)
        with store.transaction() as conn:
            outbox = conn.execute(
                "SELECT COUNT(*) AS count FROM outbox WHERE logical_key='materialize:cand-materialize-2:gap'"
            ).fetchone()
        self.assertEqual(outbox["count"], before["count"])

    def test_enqueue_materialization_resolution_recovers_when_record_exists_without_logical_event(self) -> None:
        store = EventStore()
        store.upsert_judge_run(
            judge_run_key="judge-materialize-3",
            partition_key="partition-3",
            window_id="window-materialize-3",
            start_ordinal=1,
            end_ordinal=2,
            window_index=1,
            status="judged",
            prompt_version="v1",
        )
        store.upsert_promotion_candidate(
            candidate_key="cand-materialize-3",
            window_id="window-materialize-3",
            judge_run_key="judge-materialize-3",
            label="knowledge",
            status="accepted",
            score=0.87,
            slice_fingerprint="window-materialize-3",
            reasons=["confirmed_fact"],
            payload={"window_id": "window-materialize-3"},
        )
        store.upsert_materialization_record(
            materialization_key="mat-3",
            candidate_key="cand-materialize-3",
            review_seq=1,
            judge_run_key="judge-materialize-3",
            window_id="window-materialize-3",
            materialized_label="knowledge",
            effective_label="knowledge",
            status="planned",
            payload={"candidate_key": "cand-materialize-3"},
        )

        result = store.enqueue_materialization_resolution(
            candidate_key="cand-materialize-3",
            review_seq=1,
            effective_label="knowledge",
            materialization_key="mat-3",
            judge_run_key="judge-materialize-3",
            window_id="window-materialize-3",
            payload={"candidate_key": "cand-materialize-3"},
        )

        self.assertGreater(result.aggregate_version, 0)
        with store.transaction() as conn:
            logical = conn.execute(
                "SELECT status FROM logical_events WHERE logical_key='materialize:cand-materialize-3:knowledge'"
            ).fetchone()
            outbox = conn.execute(
                "SELECT COUNT(*) AS count FROM outbox WHERE logical_key='materialize:cand-materialize-3:knowledge'"
            ).fetchone()
        self.assertEqual(logical["status"], "ready")
        self.assertEqual(outbox["count"], 2)

    def test_enqueue_materialization_resolution_recovers_when_logical_event_exists_without_outbox(self) -> None:
        store = EventStore()
        store.upsert_judge_run(
            judge_run_key="judge-materialize-5",
            partition_key="partition-5",
            window_id="window-materialize-5",
            start_ordinal=1,
            end_ordinal=2,
            window_index=1,
            status="judged",
            prompt_version="v1",
        )
        store.upsert_promotion_candidate(
            candidate_key="cand-materialize-5",
            window_id="window-materialize-5",
            judge_run_key="judge-materialize-5",
            label="knowledge",
            status="accepted",
            score=0.88,
            slice_fingerprint="window-materialize-5",
            reasons=["confirmed_fact"],
            payload={"window_id": "window-materialize-5"},
        )
        store.enqueue_materialization_resolution(
            candidate_key="cand-materialize-5",
            review_seq=1,
            effective_label="knowledge",
            materialization_key="mat-5",
            judge_run_key="judge-materialize-5",
            window_id="window-materialize-5",
            payload={"candidate_key": "cand-materialize-5"},
        )
        with store.transaction() as conn:
            conn.execute(
                "DELETE FROM outbox WHERE logical_key='materialize:cand-materialize-5:knowledge'"
            )

        result = store.enqueue_materialization_resolution(
            candidate_key="cand-materialize-5",
            review_seq=1,
            effective_label="knowledge",
            materialization_key="mat-5",
            judge_run_key="judge-materialize-5",
            window_id="window-materialize-5",
            payload={"candidate_key": "cand-materialize-5"},
        )

        self.assertGreater(result.aggregate_version, 1)
        with store.transaction() as conn:
            outbox = conn.execute(
                "SELECT COUNT(*) AS count FROM outbox WHERE logical_key='materialize:cand-materialize-5:knowledge'"
            ).fetchone()
        self.assertEqual(outbox["count"], 2)

    def test_enqueue_materialization_resolution_does_not_roll_back_to_older_review_seq(self) -> None:
        store = EventStore()
        store.upsert_judge_run(
            judge_run_key="judge-materialize-4",
            partition_key="partition-4",
            window_id="window-materialize-4",
            start_ordinal=1,
            end_ordinal=2,
            window_index=1,
            status="judged",
            prompt_version="v1",
        )
        store.upsert_promotion_candidate(
            candidate_key="cand-materialize-4",
            window_id="window-materialize-4",
            judge_run_key="judge-materialize-4",
            label="adr",
            status="accepted",
            score=0.91,
            slice_fingerprint="window-materialize-4",
            reasons=["agreement"],
            payload={"window_id": "window-materialize-4"},
        )
        store.enqueue_materialization_resolution(
            candidate_key="cand-materialize-4",
            review_seq=2,
            effective_label="adr",
            materialization_key="mat-4-new",
            judge_run_key="judge-materialize-4",
            window_id="window-materialize-4",
            payload={"candidate_key": "cand-materialize-4", "review_seq": 2},
        )

        result = store.enqueue_materialization_resolution(
            candidate_key="cand-materialize-4",
            review_seq=1,
            effective_label="adr",
            materialization_key="mat-4-old",
            judge_run_key="judge-materialize-4",
            window_id="window-materialize-4",
            payload={"candidate_key": "cand-materialize-4", "review_seq": 1},
        )

        self.assertEqual(result.aggregate_version, 1)
        with store.transaction() as conn:
            logical = conn.execute(
                """
                SELECT aggregate_version, aggregate_state_json
                FROM logical_events
                WHERE logical_key='materialize:cand-materialize-4:adr'
                """
            ).fetchone()
            rows = conn.execute(
                """
                SELECT materialization_key, review_seq
                FROM materialization_records
                WHERE candidate_key='cand-materialize-4'
                ORDER BY review_seq
                """
            ).fetchall()
        state = json.loads(logical["aggregate_state_json"])
        self.assertEqual(logical["aggregate_version"], 1)
        self.assertEqual(state["review_seq"], 2)
        self.assertEqual([(row["materialization_key"], row["review_seq"]) for row in rows], [("mat-4-new", 2)])

    def test_enqueue_materialization_resolution_skips_older_review_when_newer_record_exists_without_logical(self) -> None:
        store = EventStore()
        store.upsert_judge_run(
            judge_run_key="judge-materialize-4b",
            partition_key="partition-4b",
            window_id="window-materialize-4b",
            start_ordinal=1,
            end_ordinal=2,
            window_index=1,
            status="judged",
            prompt_version="v1",
        )
        store.upsert_promotion_candidate(
            candidate_key="cand-materialize-4b",
            window_id="window-materialize-4b",
            judge_run_key="judge-materialize-4b",
            label="adr",
            status="accepted",
            score=0.91,
            slice_fingerprint="window-materialize-4b",
            reasons=["agreement"],
            payload={"window_id": "window-materialize-4b"},
        )
        store.upsert_materialization_record(
            materialization_key="mat-4b-new",
            candidate_key="cand-materialize-4b",
            review_seq=2,
            judge_run_key="judge-materialize-4b",
            window_id="window-materialize-4b",
            materialized_label="adr",
            effective_label="adr",
            status="planned",
            payload={"candidate_key": "cand-materialize-4b", "review_seq": 2},
        )

        result = store.enqueue_materialization_resolution(
            candidate_key="cand-materialize-4b",
            review_seq=1,
            effective_label="adr",
            materialization_key="mat-4b-old",
            judge_run_key="judge-materialize-4b",
            window_id="window-materialize-4b",
            payload={"candidate_key": "cand-materialize-4b", "review_seq": 1},
        )

        self.assertEqual(result.aggregate_version, 0)
        with store.transaction() as conn:
            records = conn.execute(
                """
                SELECT materialization_key, review_seq
                FROM materialization_records
                WHERE candidate_key='cand-materialize-4b'
                ORDER BY review_seq
                """
            ).fetchall()
            outbox = conn.execute(
                "SELECT COUNT(*) AS count FROM outbox WHERE logical_key='materialize:cand-materialize-4b:adr'"
            ).fetchone()
        self.assertEqual([(row["materialization_key"], row["review_seq"]) for row in records], [("mat-4b-new", 2)])
        self.assertEqual(outbox["count"], 0)

    def test_enqueue_materialization_resolution_newer_review_seq_requeues_and_bumps_version(self) -> None:
        store = EventStore()
        store.upsert_judge_run(
            judge_run_key="judge-materialize-6",
            partition_key="partition-6",
            window_id="window-materialize-6",
            start_ordinal=1,
            end_ordinal=2,
            window_index=1,
            status="judged",
            prompt_version="v1",
        )
        store.upsert_promotion_candidate(
            candidate_key="cand-materialize-6",
            window_id="window-materialize-6",
            judge_run_key="judge-materialize-6",
            label="gap",
            status="accepted",
            score=0.91,
            slice_fingerprint="window-materialize-6",
            reasons=["user_correction"],
            payload={"window_id": "window-materialize-6"},
        )
        first = store.enqueue_materialization_resolution(
            candidate_key="cand-materialize-6",
            review_seq=1,
            effective_label="gap",
            materialization_key="mat-6-v1",
            judge_run_key="judge-materialize-6",
            window_id="window-materialize-6",
            payload={"candidate_key": "cand-materialize-6", "review_seq": 1},
        )
        second = store.enqueue_materialization_resolution(
            candidate_key="cand-materialize-6",
            review_seq=2,
            effective_label="gap",
            materialization_key="mat-6-v2",
            judge_run_key="judge-materialize-6",
            window_id="window-materialize-6",
            payload={"candidate_key": "cand-materialize-6", "review_seq": 2},
        )

        self.assertEqual(first.aggregate_version, 1)
        self.assertEqual(second.aggregate_version, 2)
        with store.transaction() as conn:
            logical = conn.execute(
                """
                SELECT aggregate_version, aggregate_state_json
                FROM logical_events
                WHERE logical_key='materialize:cand-materialize-6:gap'
                """
            ).fetchone()
            ready = conn.execute(
                """
                SELECT COUNT(*) AS count
                FROM outbox
                WHERE logical_key='materialize:cand-materialize-6:gap' AND status='ready'
                """
            ).fetchone()
            superseded = conn.execute(
                """
                SELECT COUNT(*) AS count
                FROM outbox
                WHERE logical_key='materialize:cand-materialize-6:gap' AND status='dead_letter'
                """
            ).fetchone()
        replayed = store.replay_dead_letters(limit=10)
        state = json.loads(logical["aggregate_state_json"])
        self.assertEqual(logical["aggregate_version"], 2)
        self.assertEqual(state["review_seq"], 2)
        self.assertEqual(ready["count"], 2)
        self.assertEqual(superseded["count"], 2)
        self.assertEqual(replayed, 0)

    def test_enqueue_materialization_resolution_recovers_missing_current_outbox_even_if_older_version_exists(self) -> None:
        store = EventStore()
        store.upsert_judge_run(
            judge_run_key="judge-materialize-7",
            partition_key="partition-7",
            window_id="window-materialize-7",
            start_ordinal=1,
            end_ordinal=2,
            window_index=1,
            status="judged",
            prompt_version="v1",
        )
        store.upsert_promotion_candidate(
            candidate_key="cand-materialize-7",
            window_id="window-materialize-7",
            judge_run_key="judge-materialize-7",
            label="knowledge",
            status="accepted",
            score=0.93,
            slice_fingerprint="window-materialize-7",
            reasons=["confirmed_fact"],
            payload={"window_id": "window-materialize-7"},
        )
        store.enqueue_materialization_resolution(
            candidate_key="cand-materialize-7",
            review_seq=1,
            effective_label="knowledge",
            materialization_key="mat-7-v1",
            judge_run_key="judge-materialize-7",
            window_id="window-materialize-7",
            payload={"candidate_key": "cand-materialize-7", "review_seq": 1},
        )
        store.enqueue_materialization_resolution(
            candidate_key="cand-materialize-7",
            review_seq=2,
            effective_label="knowledge",
            materialization_key="mat-7-v2",
            judge_run_key="judge-materialize-7",
            window_id="window-materialize-7",
            payload={"candidate_key": "cand-materialize-7", "review_seq": 2},
        )
        with store.transaction() as conn:
            conn.execute(
                """
                DELETE FROM outbox
                WHERE logical_key='materialize:cand-materialize-7:knowledge'
                  AND aggregate_version=2
                """
            )

        result = store.enqueue_materialization_resolution(
            candidate_key="cand-materialize-7",
            review_seq=2,
            effective_label="knowledge",
            materialization_key="mat-7-v2",
            judge_run_key="judge-materialize-7",
            window_id="window-materialize-7",
            payload={"candidate_key": "cand-materialize-7", "review_seq": 2},
        )

        self.assertGreater(result.aggregate_version, 2)
        with store.transaction() as conn:
            outbox_versions = conn.execute(
                """
                SELECT aggregate_version, status
                FROM outbox
                WHERE logical_key='materialize:cand-materialize-7:knowledge'
                ORDER BY aggregate_version
                """
            ).fetchall()
        self.assertEqual(
            [(row["aggregate_version"], row["status"]) for row in outbox_versions],
            [(1, "dead_letter"), (1, "dead_letter"), (3, "ready"), (3, "ready")],
        )

    def test_enqueue_materialization_resolution_recovers_partial_current_outbox_loss(self) -> None:
        store = EventStore()
        store.upsert_judge_run(
            judge_run_key="judge-materialize-8",
            partition_key="partition-8",
            window_id="window-materialize-8",
            start_ordinal=1,
            end_ordinal=2,
            window_index=1,
            status="judged",
            prompt_version="v1",
        )
        store.upsert_promotion_candidate(
            candidate_key="cand-materialize-8",
            window_id="window-materialize-8",
            judge_run_key="judge-materialize-8",
            label="gap",
            status="accepted",
            score=0.95,
            slice_fingerprint="window-materialize-8",
            reasons=["user_correction"],
            payload={"window_id": "window-materialize-8"},
        )
        store.enqueue_materialization_resolution(
            candidate_key="cand-materialize-8",
            review_seq=1,
            effective_label="gap",
            materialization_key="mat-8-v1",
            judge_run_key="judge-materialize-8",
            window_id="window-materialize-8",
            payload={"candidate_key": "cand-materialize-8", "review_seq": 1},
        )
        with store.transaction() as conn:
            conn.execute(
                """
                DELETE FROM outbox
                WHERE logical_key='materialize:cand-materialize-8:gap'
                  AND aggregate_version=1
                  AND sink_name='promotion_applier'
                """
            )

        result = store.enqueue_materialization_resolution(
            candidate_key="cand-materialize-8",
            review_seq=1,
            effective_label="gap",
            materialization_key="mat-8-v1",
            judge_run_key="judge-materialize-8",
            window_id="window-materialize-8",
            payload={"candidate_key": "cand-materialize-8", "review_seq": 1},
        )

        self.assertEqual(result.aggregate_version, 2)
        with store.transaction() as conn:
            rows = conn.execute(
                """
                SELECT aggregate_version, sink_name, status
                FROM outbox
                WHERE logical_key='materialize:cand-materialize-8:gap'
                ORDER BY aggregate_version, sink_name
                """
            ).fetchall()
        self.assertEqual(
            [(row["aggregate_version"], row["sink_name"], row["status"]) for row in rows],
            [
                (1, "promotion_planner", "ready"),
                (2, "promotion_applier", "ready"),
                (2, "promotion_planner", "ready"),
            ],
        )

    def test_claim_materialization_record_reclaims_with_incremented_epoch(self) -> None:
        store = EventStore()
        store.upsert_judge_run(
            judge_run_key="judge-materialize-7",
            partition_key="partition-7",
            window_id="window-materialize-7",
            start_ordinal=1,
            end_ordinal=2,
            window_index=1,
            status="judged",
            prompt_version="v1",
        )
        store.upsert_promotion_candidate(
            candidate_key="cand-materialize-7",
            window_id="window-materialize-7",
            judge_run_key="judge-materialize-7",
            label="knowledge",
            status="accepted",
            score=0.88,
            slice_fingerprint="window-materialize-7",
            reasons=["confirmed_fact"],
            payload={"window_id": "window-materialize-7"},
        )
        store.upsert_materialization_record(
            materialization_key="mat-7",
            candidate_key="cand-materialize-7",
            review_seq=1,
            judge_run_key="judge-materialize-7",
            window_id="window-materialize-7",
            materialized_label="knowledge",
            effective_label="knowledge",
            status="planned",
            payload={"candidate_key": "cand-materialize-7"},
        )

        first = store.claim_materialization_record(
            materialization_key="mat-7",
            lease_owner="worker-a",
            lease_expires_at="2026-03-25T00:10:00+00:00",
        )
        self.assertEqual(first["lease_epoch"], 1)
        self.assertTrue(
            store.heartbeat_materialization_record(
                materialization_key="mat-7",
                lease_owner="worker-a",
                lease_epoch=1,
                lease_expires_at="2026-03-25T00:11:00+00:00",
            )
        )
        with store.transaction() as conn:
            conn.execute(
                """
                UPDATE materialization_records
                SET lease_expires_at='2000-01-01T00:00:00+00:00'
                WHERE materialization_key='mat-7'
                """
            )
        second = store.claim_materialization_record(
            materialization_key="mat-7",
            lease_owner="worker-b",
            lease_expires_at="2026-03-25T00:12:00+00:00",
        )
        self.assertEqual(second["lease_epoch"], 2)
        self.assertFalse(
            store.heartbeat_materialization_record(
                materialization_key="mat-7",
                lease_owner="worker-a",
                lease_epoch=1,
                lease_expires_at="2026-03-25T00:13:00+00:00",
            )
        )
        self.assertFalse(
            store.release_materialization_record(
                materialization_key="mat-7",
                lease_owner="worker-a",
                lease_epoch=1,
                status="applied",
            )
        )
        self.assertTrue(
            store.release_materialization_record(
                materialization_key="mat-7",
                lease_owner="worker-b",
                lease_epoch=2,
                status="applied",
            )
        )

    def test_mark_materialization_repair_pending_clears_stale_lease(self) -> None:
        store = EventStore()
        store.upsert_judge_run(
            judge_run_key="judge-materialize-9",
            partition_key="partition-9",
            window_id="window-materialize-9",
            start_ordinal=1,
            end_ordinal=2,
            window_index=1,
            status="judged",
            prompt_version="v1",
        )
        store.upsert_promotion_candidate(
            candidate_key="cand-materialize-9",
            window_id="window-materialize-9",
            judge_run_key="judge-materialize-9",
            label="gap",
            status="accepted",
            score=0.9,
            slice_fingerprint="window-materialize-9",
            reasons=["user_correction"],
            payload={"window_id": "window-materialize-9"},
        )
        store.upsert_materialization_record(
            materialization_key="mat-9",
            candidate_key="cand-materialize-9",
            review_seq=1,
            judge_run_key="judge-materialize-9",
            window_id="window-materialize-9",
            materialized_label="gap",
            effective_label="gap",
            status="applying",
            payload={"candidate_key": "cand-materialize-9"},
            lease_owner="worker-a",
            lease_expires_at="2026-03-25T00:10:00+00:00",
            lease_epoch=1,
        )

        self.assertTrue(
            store.mark_materialization_repair_pending(
                materialization_key="mat-9",
                expected_lease_epoch=1,
                last_error="lease lost during apply",
            )
        )
        row = store.get_materialization_record("mat-9")
        self.assertEqual(row["status"], "repair_pending")
        self.assertIsNone(row["lease_owner"])
        self.assertIsNone(row["lease_expires_at"])
        self.assertEqual(row["last_error"], "lease lost during apply")

    def test_mark_materialization_repair_pending_does_not_override_newer_lease(self) -> None:
        store = EventStore()
        store.upsert_judge_run(
            judge_run_key="judge-materialize-10",
            partition_key="partition-10",
            window_id="window-materialize-10",
            start_ordinal=1,
            end_ordinal=2,
            window_index=1,
            status="judged",
            prompt_version="v1",
        )
        store.upsert_promotion_candidate(
            candidate_key="cand-materialize-10",
            window_id="window-materialize-10",
            judge_run_key="judge-materialize-10",
            label="gap",
            status="accepted",
            score=0.9,
            slice_fingerprint="window-materialize-10",
            reasons=["user_correction"],
            payload={"window_id": "window-materialize-10"},
        )
        store.upsert_materialization_record(
            materialization_key="mat-10",
            candidate_key="cand-materialize-10",
            review_seq=1,
            judge_run_key="judge-materialize-10",
            window_id="window-materialize-10",
            materialized_label="gap",
            effective_label="gap",
            status="applying",
            payload={"candidate_key": "cand-materialize-10"},
            lease_owner="worker-b",
            lease_expires_at="2026-03-25T00:10:00+00:00",
            lease_epoch=2,
        )

        self.assertFalse(
            store.mark_materialization_repair_pending(
                materialization_key="mat-10",
                expected_lease_epoch=1,
                last_error="stale worker cleanup",
            )
        )
        row = store.get_materialization_record("mat-10")
        self.assertEqual(row["status"], "applying")
        self.assertEqual(row["lease_owner"], "worker-b")
        self.assertEqual(row["lease_epoch"], 2)

    def test_mark_sink_succeeded_ignores_stale_dead_lettered_row(self) -> None:
        store = EventStore()
        store.append(
            EventEnvelope(
                event_id="evt-stale-sink-1",
                occurred_at=utc_now_iso(),
                received_at=utc_now_iso(),
                source_tool="kb",
                source_client="kb-mcp",
                source_layer="recovery_sweeper",
                event_name="materialization_resolved",
                aggregate_type="review_materialization",
                management_mode="managed",
                logical_key="materialize:cand-stale:gap",
                correlation_id="corr-stale",
                session_id=None,
                tool_call_id=None,
                error_fingerprint=None,
                summary="stale sink test",
                content_excerpt=None,
                cwd="/tmp/project",
                repo="github.com/example/project",
                project="kb",
                transcript_path=None,
                aggregate_state={"review_seq": 1},
                raw_payload={},
                redacted_payload={},
            )
        )
        row = store.ready_sinks(limit=1)[0]
        store.mark_sink_failed(row["id"], "superseded by newer review_materialization")

        store.mark_sink_succeeded(
            row_id=row["id"],
            logical_key=row["logical_key"],
            aggregate_version=row["aggregate_version"],
            sink_name=row["sink_name"],
            receipt="late-success",
        )

        with store.transaction() as conn:
            outbox = conn.execute(
                "SELECT status FROM outbox WHERE id=?",
                (row["id"],),
            ).fetchone()
            sink_run = conn.execute(
                """
                SELECT COUNT(*) AS count
                FROM sink_runs
                WHERE logical_key=? AND aggregate_version=? AND sink_name=?
                """,
                (row["logical_key"], row["aggregate_version"], row["sink_name"]),
            ).fetchone()
        self.assertEqual(outbox["status"], "dead_letter")
        self.assertEqual(sink_run["count"], 0)

    def test_mark_sink_succeeded_does_not_finalize_review_materialization_when_sink_missing(self) -> None:
        store = EventStore()
        store.upsert_judge_run(
            judge_run_key="judge-finalize-gap",
            partition_key="partition-finalize-gap",
            window_id="window-finalize-gap",
            start_ordinal=1,
            end_ordinal=2,
            window_index=1,
            status="judged",
            prompt_version="v1",
        )
        store.upsert_promotion_candidate(
            candidate_key="cand-finalize-gap",
            window_id="window-finalize-gap",
            judge_run_key="judge-finalize-gap",
            label="gap",
            status="accepted",
            score=0.9,
            slice_fingerprint="window-finalize-gap",
            reasons=["user_correction"],
            payload={"window_id": "window-finalize-gap"},
        )
        store.enqueue_materialization_resolution(
            candidate_key="cand-finalize-gap",
            review_seq=1,
            effective_label="gap",
            materialization_key="mat-finalize-gap",
            judge_run_key="judge-finalize-gap",
            window_id="window-finalize-gap",
            payload={"candidate_key": "cand-finalize-gap"},
        )
        with store.transaction() as conn:
            row = conn.execute(
                """
                SELECT id, logical_key, aggregate_version, sink_name
                FROM outbox
                WHERE logical_key='materialize:cand-finalize-gap:gap'
                ORDER BY sink_name
                LIMIT 1
                """
            ).fetchone()
            conn.execute(
                """
                DELETE FROM outbox
                WHERE logical_key='materialize:cand-finalize-gap:gap'
                  AND sink_name='promotion_applier'
                """
            )
        store.mark_sink_succeeded(
            row_id=row["id"],
            logical_key=row["logical_key"],
            aggregate_version=row["aggregate_version"],
            sink_name=row["sink_name"],
            receipt="planner-only",
        )
        with store.transaction() as conn:
            logical = conn.execute(
                """
                SELECT status
                FROM logical_events
                WHERE logical_key='materialize:cand-finalize-gap:gap'
                """
            ).fetchone()
        self.assertEqual(logical["status"], "ready")

    def test_upsert_and_claim_judge_run(self) -> None:
        store = EventStore()
        store.upsert_judge_run(
            judge_run_key="judge-1",
            partition_key="partition-1",
            window_id="window-1",
            start_ordinal=1,
            end_ordinal=10,
            window_index=1,
            status="ready",
            prompt_version="v1",
            labels=[{"label": "gap", "score": 0.9, "reasons": ["user_correction"]}],
            decision={"carry_forward": False},
            model_hint="codex",
        )
        claimed = store.claim_judge_run(
            window_id="window-1",
            prompt_version="v1",
            lease_owner="runner-1",
            lease_expires_at="2026-03-25T00:10:00+00:00",
        )
        self.assertIsNotNone(claimed)
        self.assertEqual(claimed["lease_owner"], "runner-1")
        self.assertTrue(
            store.heartbeat_judge_run(
                judge_run_key="judge-1",
                lease_owner="runner-1",
                lease_expires_at="2026-03-25T00:11:00+00:00",
            )
        )
        self.assertTrue(store.release_judge_run(judge_run_key="judge-1", lease_owner="runner-1"))
        with store.transaction() as conn:
            row = conn.execute(
                "SELECT lease_owner, lease_expires_at FROM judge_runs WHERE judge_run_key='judge-1'"
            ).fetchone()
        self.assertIsNone(row["lease_owner"])
        self.assertIsNone(row["lease_expires_at"])

    def test_judge_run_upsert_uses_window_and_prompt_unique_pair(self) -> None:
        store = EventStore()
        store.upsert_judge_run(
            judge_run_key="judge-a",
            partition_key="partition-1",
            window_id="window-1",
            start_ordinal=1,
            end_ordinal=5,
            window_index=1,
            status="ready",
            prompt_version="v1",
        )
        store.upsert_judge_run(
            judge_run_key="judge-b",
            partition_key="partition-1",
            window_id="window-1",
            start_ordinal=1,
            end_ordinal=6,
            window_index=1,
            status="judged",
            prompt_version="v1",
            labels=[{"label": "gap", "score": 0.9}],
        )
        with store.transaction() as conn:
            rows = conn.execute(
                "SELECT judge_run_key, end_ordinal, status FROM judge_runs WHERE window_id='window-1' AND prompt_version='v1'"
            ).fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["judge_run_key"], "judge-a")
        self.assertEqual(rows[0]["end_ordinal"], 6)
        self.assertEqual(rows[0]["status"], "judged")

    def test_claim_judge_run_only_claims_ready_rows(self) -> None:
        store = EventStore()
        store.upsert_judge_run(
            judge_run_key="judge-done",
            partition_key="partition-2",
            window_id="window-2",
            start_ordinal=1,
            end_ordinal=4,
            window_index=1,
            status="judged",
            prompt_version="v1",
        )
        claimed = store.claim_judge_run(
            window_id="window-2",
            prompt_version="v1",
            lease_owner="runner-2",
            lease_expires_at="2026-03-25T00:10:00+00:00",
        )
        self.assertIsNone(claimed)

    def test_candidate_upsert_preserves_resolved_rows(self) -> None:
        store = EventStore()
        store.upsert_judge_run(
            judge_run_key="judge-1",
            partition_key="partition-1",
            window_id="window-1",
            start_ordinal=1,
            end_ordinal=3,
            window_index=1,
            status="ready",
            prompt_version="v1",
        )
        store.upsert_judge_run(
            judge_run_key="judge-2",
            partition_key="partition-1",
            window_id="window-1",
            start_ordinal=1,
            end_ordinal=3,
            window_index=1,
            status="ready",
            prompt_version="v2",
        )
        store.upsert_promotion_candidate(
            candidate_key="cand-1",
            window_id="window-1",
            judge_run_key="judge-1",
            label="gap",
            status="pending_review",
            score=0.8,
            slice_fingerprint="slice-1",
            reasons=["user_correction"],
            payload={"window_id": "window-1"},
        )
        review_seq = store.record_candidate_review(
            review_id="review-1",
            candidate_key="cand-1",
            window_id="window-1",
            judge_run_key="judge-1",
            ai_labels=[{"label": "gap", "score": 0.8}],
            ai_score={"gap": 0.8},
            human_verdict="accepted",
            human_label=None,
        )
        self.assertEqual(review_seq, 1)
        store.upsert_promotion_candidate(
            candidate_key="cand-1",
            window_id="window-1",
            judge_run_key="judge-2",
            label="gap",
            status="pending_review",
            score=0.3,
            slice_fingerprint="slice-1",
            reasons=["low_confidence"],
            payload={"window_id": "window-1", "changed": True},
        )
        with store.transaction() as conn:
            row = conn.execute(
                "SELECT judge_run_key, status, score, payload_json, resolved_at FROM promotion_candidates WHERE candidate_key='cand-1'"
            ).fetchone()
            review = conn.execute(
                "SELECT review_seq, human_verdict FROM candidate_reviews WHERE candidate_key='cand-1'"
            ).fetchone()
        self.assertEqual(row["judge_run_key"], "judge-1")
        self.assertEqual(row["status"], "accepted")
        self.assertEqual(review["review_seq"], 1)
        self.assertEqual(review["human_verdict"], "accepted")
        self.assertIsNotNone(row["resolved_at"])

    def test_pending_candidates_can_be_marked_suggested(self) -> None:
        store = EventStore()
        store.upsert_judge_run(
            judge_run_key="judge-2",
            partition_key="partition-2",
            window_id="window-2",
            start_ordinal=1,
            end_ordinal=3,
            window_index=1,
            status="ready",
            prompt_version="v1",
        )
        store.upsert_promotion_candidate(
            candidate_key="cand-2",
            window_id="window-2",
            judge_run_key="judge-2",
            label="knowledge",
            status="pending_review",
            score=0.81,
            slice_fingerprint="slice-2",
            reasons=["verified_fact"],
            payload={"window_id": "window-2"},
        )
        rows = store.pending_review_candidates(limit=10)
        self.assertEqual(len(rows), 1)
        updated = store.mark_candidates_suggested(["cand-2"])
        self.assertEqual(updated, 1)
        with store.transaction() as conn:
            row = conn.execute(
                "SELECT suggestion_seq, last_suggested_at FROM promotion_candidates WHERE candidate_key='cand-2'"
            ).fetchone()
        self.assertEqual(row["suggestion_seq"], 1)
        self.assertIsNotNone(row["last_suggested_at"])

    def test_record_candidate_review_rejects_missing_candidate(self) -> None:
        store = EventStore()
        with self.assertRaisesRegex(ValueError, "candidate not found"):
            store.record_candidate_review(
                review_id="review-missing",
                candidate_key="missing",
                window_id="window-x",
                judge_run_key="judge-x",
                ai_labels=[{"label": "gap", "score": 0.8}],
                ai_score={"gap": 0.8},
                human_verdict="accepted",
                human_label=None,
            )

    def test_record_candidate_review_validates_verdict_and_label(self) -> None:
        store = EventStore()
        store.upsert_judge_run(
            judge_run_key="judge-3",
            partition_key="partition-3",
            window_id="window-3",
            start_ordinal=1,
            end_ordinal=3,
            window_index=1,
            status="ready",
            prompt_version="v1",
        )
        store.upsert_promotion_candidate(
            candidate_key="cand-3",
            window_id="window-3",
            judge_run_key="judge-3",
            label="gap",
            status="pending_review",
            score=0.8,
            slice_fingerprint="slice-3",
            reasons=["user_correction"],
            payload={"window_id": "window-3"},
        )
        with self.assertRaisesRegex(ValueError, "invalid human verdict"):
            store.record_candidate_review(
                review_id="review-bad-verdict",
                candidate_key="cand-3",
                window_id="window-3",
                judge_run_key="judge-3",
                ai_labels=[{"label": "gap", "score": 0.8}],
                ai_score={"gap": 0.8},
                human_verdict="maybe",
                human_label=None,
            )
        with self.assertRaisesRegex(ValueError, "human_label is required"):
            store.record_candidate_review(
                review_id="review-missing-label",
                candidate_key="cand-3",
                window_id="window-3",
                judge_run_key="judge-3",
                ai_labels=[{"label": "gap", "score": 0.8}],
                ai_score={"gap": 0.8},
                human_verdict="relabeled",
                human_label=None,
            )
        with self.assertRaisesRegex(ValueError, "window_id does not match candidate"):
            store.record_candidate_review(
                review_id="review-bad-window",
                candidate_key="cand-3",
                window_id="window-other",
                judge_run_key="judge-3",
                ai_labels=[{"label": "gap", "score": 0.8}],
                ai_score={"gap": 0.8},
                human_verdict="accepted",
                human_label=None,
            )
        with self.assertRaisesRegex(ValueError, "judge_run_key does not match candidate"):
            store.record_candidate_review(
                review_id="review-bad-judge",
                candidate_key="cand-3",
                window_id="window-3",
                judge_run_key="judge-other",
                ai_labels=[{"label": "gap", "score": 0.8}],
                ai_score={"gap": 0.8},
                human_verdict="accepted",
                human_label=None,
            )

    def test_transcript_excerpt_wins_over_summary_when_content_missing(self) -> None:
        transcript = self.vault / "transcript.txt"
        transcript.write_text("line1\nline2\nimportant transcript line\n", encoding="utf-8")
        envelope = normalize_event(
            tool="codex",
            client="codex-cli",
            layer="client_hook",
            event="turn_checkpointed",
            payload={
                "session_id": "session-3",
                "summary": "short summary",
                "transcript_path": str(transcript),
            },
        )
        self.assertIn("important transcript line", envelope.content_excerpt or "")

    def test_codex_jsonl_excerpt_extracts_messages_only(self) -> None:
        transcript = self.vault / "codex.jsonl"
        transcript.write_text(
            "\n".join(
                [
                    json.dumps(
                        {
                            "timestamp": "2026-03-25T00:00:00Z",
                            "type": "event_msg",
                            "payload": {"type": "user_message", "message": "最初の質問"},
                        },
                        ensure_ascii=False,
                    ),
                    json.dumps(
                        {
                            "timestamp": "2026-03-25T00:00:01Z",
                            "type": "response_item",
                            "payload": {
                                "type": "function_call_output",
                                "output": '{"status":"ok"}',
                            },
                        },
                        ensure_ascii=False,
                    ),
                    json.dumps(
                        {
                            "timestamp": "2026-03-25T00:00:02Z",
                            "type": "event_msg",
                            "payload": {"type": "agent_message", "message": "途中の返答"},
                        },
                        ensure_ascii=False,
                    ),
                    json.dumps(
                        {
                            "timestamp": "2026-03-25T00:00:03Z",
                            "type": "response_item",
                            "payload": {
                                "type": "message",
                                "role": "assistant",
                                "content": [{"type": "output_text", "text": "最後の返答"}],
                            },
                        },
                        ensure_ascii=False,
                    ),
                ]
            )
            + "\n",
            encoding="utf-8",
        )

        envelope = normalize_event(
            tool="codex",
            client="codex-cli",
            layer="client_hook",
            event="turn_checkpointed",
            payload={
                "session_id": "session-4",
                "summary": "session ended",
                "transcript_path": str(transcript),
            },
        )

        self.assertIn("最初の質問", envelope.content_excerpt or "")
        self.assertIn("途中の返答", envelope.content_excerpt or "")
        self.assertIn("最後の返答", envelope.content_excerpt or "")
        self.assertNotIn("function_call_output", envelope.content_excerpt or "")
