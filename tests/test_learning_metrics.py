from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

import yaml

from kb_mcp.config import load_config
from kb_mcp.events.store import EventStore
from kb_mcp.learning.metrics import compute_learning_outcome_metrics


class LearningMetricsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        root = Path(self.tmpdir.name)
        self.config_dir = root / "config"
        self.vault = root / "vault"
        os.environ["KB_CONFIG_DIR"] = str(self.config_dir)
        self.addCleanup(lambda: os.environ.pop("KB_CONFIG_DIR", None))
        self.vault.mkdir(parents=True, exist_ok=True)
        cfg_path = self.config_dir / "config.yml"
        cfg_path.parent.mkdir(parents=True, exist_ok=True)
        cfg_path.write_text(
            yaml.safe_dump(
                {"vault_path": str(self.vault), "kb_root": "", "timezone": "Asia/Tokyo", "obsidian_cli": "auto", "vault_git": False}
            ),
            encoding="utf-8",
        )
        load_config.cache_clear()

    def test_compute_learning_outcome_metrics(self) -> None:
        store = EventStore()
        self._seed_asset(store, "gap-1", "gap", "behavior_style", "demo-1")
        self._seed_asset(store, "gap-2", "gap", "behavior_style", "demo-2")
        self._seed_asset(store, "adr-1", "adr", "decision", "demo-1")
        self._seed_asset(store, "adr-2", "adr", "decision", "demo-2")
        self._seed_asset(store, "knowledge-1", "knowledge", "setup", "demo-1")
        store.create_learning_packet(
            packet_id="packet-1",
            source_tool="kb",
            source_client="codex-cli",
            tool_name="knowledge",
            session_id="s1",
            project="demo-1",
            repo=None,
            cwd=None,
            asset_keys=["knowledge-1"],
        )
        store.create_learning_packet(
            packet_id="packet-2",
            source_tool="kb",
            source_client="claude-code",
            tool_name="knowledge",
            session_id="s2",
            project="demo-1",
            repo=None,
            cwd=None,
            asset_keys=["knowledge-1"],
        )
        store.record_learning_application(
            application_id="app-1",
            packet_id="packet-1",
            tool_name="knowledge",
            tool_call_id="call-1",
            source_tool="kb",
            source_client="codex-cli",
            session_id="s1",
            save_request_id=None,
            saved_note_id=None,
            saved_note_path=None,
        )
        store.record_learning_application(
            application_id="app-2",
            packet_id="packet-2",
            tool_name="knowledge",
            tool_call_id="call-2",
            source_tool="kb",
            source_client="claude-code",
            session_id="s2",
            save_request_id=None,
            saved_note_id=None,
            saved_note_path=None,
        )

        with store.transaction() as conn:
            metrics = compute_learning_outcome_metrics(conn)

        self.assertEqual(metrics["same_gap_recurrence"], 1)
        self.assertEqual(metrics["knowledge_requery"], 1)
        self.assertEqual(metrics["adr_rediscussion"], 1)
        self.assertEqual(metrics["cross_client_consistency"], 1)

    def _seed_asset(self, store: EventStore, asset_key: str, memory_class: str, update_target: str, project: str) -> None:
        store.upsert_learning_asset(
            asset_key=asset_key,
            candidate_key=None,
            review_id=None,
            materialization_key=None,
            note_id=None,
            note_path=None,
            memory_class=memory_class,
            update_target=update_target,
            scope="project_local",
            force="preferred",
            confidence="reviewed",
            lifecycle="active",
            provenance={"project": project},
            traceability={},
            revocation_path={},
            learning_state_visibility="active",
            source_status="materialized",
        )
