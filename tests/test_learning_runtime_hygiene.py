from __future__ import annotations

import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

import yaml

from kb_mcp.config import load_config, runtime_events_db_path
from kb_mcp.events.schema import schema_locked_connection
from kb_mcp.events.store import EventStore
from kb_mcp.cli import main
from kb_mcp.learning.runtime_hygiene import repair_learning_runtime


class LearningRuntimeHygieneTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        root = Path(self.tmpdir.name)
        self.config_dir = root / "config"
        self.vault = root / "vault"
        os.environ["KB_CONFIG_DIR"] = str(self.config_dir)
        self.addCleanup(lambda: os.environ.pop("KB_CONFIG_DIR", None))
        self.vault.mkdir(parents=True, exist_ok=True)
        (self.vault / "projects" / "demo" / "gap").mkdir(parents=True, exist_ok=True)
        cfg_path = self.config_dir / "config.yml"
        cfg_path.parent.mkdir(parents=True, exist_ok=True)
        cfg_path.write_text(
            yaml.safe_dump(
                {"vault_path": str(self.vault), "kb_root": "", "timezone": "Asia/Tokyo", "obsidian_cli": "auto", "vault_git": False}
            ),
            encoding="utf-8",
        )
        load_config.cache_clear()

    def test_learning_runtime_hygiene_metrics_detect_issues(self) -> None:
        store = EventStore()
        store.upsert_learning_asset(
            asset_key="asset-stale-session",
            candidate_key=None,
            review_id=None,
            materialization_key=None,
            note_id=None,
            note_path=None,
            memory_class="gap",
            update_target="behavior_style",
            scope="session_local",
            force="preferred",
            confidence="reviewed",
            lifecycle="active",
            provenance={"project": "demo", "session_id": "session-1"},
            traceability={},
            revocation_path={},
            learning_state_visibility="active",
            source_status="materialized",
            updated_at="2026-03-20T00:00:00+00:00",
        )
        store.upsert_learning_asset(
            asset_key="asset-legacy-general",
            candidate_key=None,
            review_id=None,
            materialization_key=None,
            note_id=None,
            note_path=None,
            memory_class="knowledge",
            update_target="resolver_behavior",
            scope="general",
            force="default",
            confidence="reviewed",
            lifecycle="active",
            provenance={"project": "demo"},
            traceability={},
            revocation_path={},
            learning_state_visibility="active",
            source_status="promoted",
        )
        store.create_learning_packet(
            packet_id="packet-1",
            source_tool="copilot",
            source_client="mystery-client",
            tool_name="knowledge",
            session_id="session-1",
            project="demo",
            repo=None,
            cwd=None,
            asset_keys=["asset-legacy-general"],
            expires_at="2026-03-20T00:00:00+00:00",
        )
        with schema_locked_connection() as conn:
            conn.execute("PRAGMA foreign_keys=OFF")
            conn.execute("UPDATE learning_packets SET asset_count=2 WHERE packet_id='packet-1'")
            conn.execute(
                """
                INSERT INTO learning_applications(
                  application_id, packet_id, tool_name, tool_call_id, source_tool, source_client,
                  session_id, save_request_id, saved_note_id, saved_note_path, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "application-orphan",
                    "missing-packet",
                    "knowledge",
                    "call-1",
                    "copilot",
                    "mystery-client",
                    "session-1",
                    None,
                    None,
                    None,
                    "2026-03-26T00:00:00+00:00",
                ),
            )
            conn.commit()
            conn.execute("PRAGMA foreign_keys=ON")

        metrics = store.learning_runtime_hygiene_metrics(session_local_days=1, client_local_days=7)

        self.assertEqual(metrics["expired_active_packets"], 1)
        self.assertEqual(metrics["packet_asset_mismatches"], 1)
        self.assertEqual(metrics["orphan_applications"], 1)
        self.assertEqual(metrics["legacy_wide_scope_fallbacks"], 1)
        self.assertEqual(metrics["unknown_client_packets"], 1)
        self.assertEqual(metrics["stale_session_local_assets"], 1)

    def test_repair_learning_runtime_backfills_null_traceability_values(self) -> None:
        store = EventStore()
        store.upsert_learning_asset(
            asset_key="asset-null-traceability",
            candidate_key=None,
            review_id=None,
            materialization_key=None,
            note_id=None,
            note_path=None,
            memory_class="knowledge",
            update_target="resolver_behavior",
            scope="general",
            force="default",
            confidence="reviewed",
            lifecycle="active",
            provenance={"project": "demo"},
            traceability={"distribution_allowed": None, "secrecy_boundary": None},
            revocation_path={},
            learning_state_visibility="active",
            source_status="promoted",
        )

        result = repair_learning_runtime(store=store)

        self.assertEqual(result["backfilled_legacy_wide_scope_assets"], 1)
        metrics = store.learning_runtime_hygiene_metrics()
        self.assertEqual(metrics["legacy_wide_scope_fallbacks"], 0)

    def test_learning_runtime_hygiene_handles_malformed_traceability_json(self) -> None:
        store = EventStore()
        with schema_locked_connection() as conn:
            conn.execute(
                """
                INSERT INTO learning_assets(
                  asset_key, candidate_key, review_id, materialization_key, note_id, note_path,
                  memory_class, update_target, scope, force, confidence, lifecycle, provenance_json,
                  traceability_json, revocation_path_json, learning_state_visibility, source_status,
                  created_at, updated_at
                ) VALUES (
                  'asset-malformed-traceability', NULL, NULL, NULL, NULL, NULL,
                  'knowledge', 'resolver_behavior', 'general', 'default', 'reviewed', 'active', '{}',
                  '{broken', '{}', 'active', 'promoted',
                  '2026-03-26T00:00:00+00:00', '2026-03-26T00:00:00+00:00'
                )
                """
            )
            conn.commit()

        metrics = store.learning_runtime_hygiene_metrics()
        self.assertEqual(metrics["legacy_wide_scope_fallbacks"], 1)

        result = repair_learning_runtime(store=store)
        self.assertEqual(result["backfilled_legacy_wide_scope_assets"], 1)

        asset = store.get_learning_asset("asset-malformed-traceability")
        self.assertIsNotNone(asset)
        repaired_traceability = json.loads(str(asset["traceability_json"]))
        self.assertIn("distribution_allowed", repaired_traceability)
        self.assertIn("secrecy_boundary", repaired_traceability)

    def test_repair_learning_runtime_repairs_and_expires(self) -> None:
        store = EventStore()
        store.upsert_learning_asset(
            asset_key="asset-stale-session",
            candidate_key=None,
            review_id=None,
            materialization_key=None,
            note_id=None,
            note_path=None,
            memory_class="gap",
            update_target="behavior_style",
            scope="session_local",
            force="preferred",
            confidence="reviewed",
            lifecycle="active",
            provenance={"project": "demo", "session_id": "session-1"},
            traceability={},
            revocation_path={},
            learning_state_visibility="active",
            source_status="materialized",
            updated_at="2026-03-20T00:00:00+00:00",
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
            asset_keys=["asset-stale-session"],
            expires_at="2026-04-20T00:00:00+00:00",
        )
        with schema_locked_connection() as conn:
            conn.execute("PRAGMA foreign_keys=OFF")
            conn.execute("UPDATE learning_packets SET asset_count=2 WHERE packet_id='packet-1'")
            conn.execute(
                """
                INSERT INTO learning_applications(
                  application_id, packet_id, tool_name, tool_call_id, source_tool, source_client,
                  session_id, save_request_id, saved_note_id, saved_note_path, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "application-orphan",
                    "missing-packet",
                    "gap",
                    "call-1",
                    "kb",
                    "kb-mcp",
                    "session-1",
                    None,
                    None,
                    None,
                    "2026-03-26T00:00:00+00:00",
                ),
            )
            conn.commit()
            conn.execute("PRAGMA foreign_keys=ON")

        result = repair_learning_runtime(session_local_days=1, client_local_days=7, store=store)

        self.assertEqual(result["invalidated_expired_packets"], 0)
        self.assertEqual(result["invalidated_stale_asset_packets"], 1)
        self.assertEqual(result["repaired_packet_asset_counts"], 1)
        self.assertEqual(result["removed_orphan_applications"], 1)
        self.assertEqual(result["expired_session_local_assets"], 1)
        self.assertEqual(result["expired_client_local_assets"], 0)
        packet = store.get_learning_packet("packet-1")
        self.assertEqual(packet["status"], "invalidated")
        self.assertEqual(packet["asset_count"], 1)
        asset = store.get_learning_asset("asset-stale-session")
        self.assertEqual(asset["lifecycle"], "expired")
        self.assertEqual(asset["learning_state_visibility"], "expired")
        metrics = store.learning_runtime_hygiene_metrics(session_local_days=1, client_local_days=7)
        self.assertEqual(metrics["expired_active_packets"], 0)
        self.assertEqual(metrics["packet_asset_mismatches"], 0)
        self.assertEqual(metrics["orphan_applications"], 0)

    def test_repair_learning_runtime_rejects_negative_days(self) -> None:
        with self.assertRaisesRegex(ValueError, "session_local_days must be >= 0"):
            repair_learning_runtime(session_local_days=-1)

    def test_learning_runtime_hygiene_metrics_rejects_negative_days(self) -> None:
        with self.assertRaisesRegex(ValueError, "session_local_days must be >= 0"):
            EventStore().learning_runtime_hygiene_metrics(session_local_days=-1)

    def test_worker_repair_learning_runtime_rejects_negative_days_in_cli(self) -> None:
        with self.assertRaises(SystemExit) as exc:
            with mock.patch("sys.argv", ["kb-mcp", "worker", "repair-learning-runtime", "--session-local-days", "-1"]):
                main()
        self.assertEqual(exc.exception.code, 2)

    def test_worker_repair_learning_runtime_command_routes_arguments(self) -> None:
        buffer = io.StringIO()
        with mock.patch("kb_mcp.learning.runtime_hygiene.repair_learning_runtime", return_value={"ok": 1}) as repair_mock:
            with redirect_stdout(buffer):
                with mock.patch("sys.argv", ["kb-mcp", "worker", "repair-learning-runtime", "--session-local-days", "2", "--client-local-days", "9"]):
                    main()
        payload = json.loads(buffer.getvalue())
        self.assertEqual(payload, {"ok": 1})
        repair_mock.assert_called_once_with(session_local_days=2, client_local_days=9)

    def test_repair_learning_runtime_expires_client_local_assets(self) -> None:
        store = EventStore()
        store.upsert_learning_asset(
            asset_key="asset-stale-client",
            candidate_key=None,
            review_id=None,
            materialization_key=None,
            note_id=None,
            note_path=None,
            memory_class="gap",
            update_target="behavior_style",
            scope="client_local",
            force="preferred",
            confidence="reviewed",
            lifecycle="active",
            provenance={"project": "demo", "source_client": "codex-cli"},
            traceability={},
            revocation_path={},
            learning_state_visibility="active",
            source_status="materialized",
            updated_at="2026-03-10T00:00:00+00:00",
        )

        result = repair_learning_runtime(client_local_days=7, store=store)

        self.assertEqual(result["expired_client_local_assets"], 1)
        asset = store.get_learning_asset("asset-stale-client")
        self.assertEqual(asset["lifecycle"], "expired")
