from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path

import yaml

from kb_mcp.config import load_config
from kb_mcp.events.middleware import with_tool_events
from kb_mcp.events.store import EventStore


class LearningApplicationTraceTest(unittest.TestCase):
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

    def test_tool_middleware_records_packet_and_application(self) -> None:
        os.environ["KB_VENDOR_SESSION_ID"] = "session-trace"
        self.addCleanup(lambda: os.environ.pop("KB_VENDOR_SESSION_ID", None))
        store = EventStore()
        store.upsert_learning_asset(
            asset_key="asset-project",
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

        def sample(*, project: str) -> str:
            return project

        wrapped = with_tool_events("kb", "kb-mcp", "sample", sample)
        result = wrapped(project="demo", ctx=None)

        self.assertEqual(result, "demo")
        counts = store.learning_packet_counts()
        self.assertEqual(counts["packets"], 1)
        self.assertEqual(counts["applications"], 1)
        with store.transaction() as conn:
            row = conn.execute(
                "SELECT raw_payload_json FROM events WHERE aggregate_type='tool' AND event_name='tool_started' ORDER BY rowid DESC LIMIT 1"
            ).fetchone()
        payload = json.loads(row["raw_payload_json"])
        self.assertIn("packet_id", payload)
