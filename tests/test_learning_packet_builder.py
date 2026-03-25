from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

import yaml

from kb_mcp.config import load_config
from kb_mcp.events.store import EventStore
from kb_mcp.learning.models import ResolverInput
from kb_mcp.learning.packet_builder import build_learning_packet
from kb_mcp.learning.resolver import resolve_learning_assets


class LearningPacketBuilderTest(unittest.TestCase):
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

    def test_build_learning_packet_persists_packet_and_assets(self) -> None:
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
        request = ResolverInput(source_tool="kb", source_client="kb-mcp", project="demo")
        assets = resolve_learning_assets(request, store=store)

        packet = build_learning_packet(request, tool_name="gap", assets=assets, store=store)

        self.assertIsNotNone(packet)
        counts = store.learning_packet_counts()
        self.assertEqual(counts["packets"], 1)
        packet_row = store.get_learning_packet(str(packet["packet_id"]))
        self.assertEqual(packet_row["status"], "active")
        self.assertIsNotNone(packet_row["expires_at"])
