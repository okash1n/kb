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
from kb_mcp.learning.revocation import expire_learning_assets, retract_learning_asset, supersede_learning_asset


class LearningRevocationTest(unittest.TestCase):
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

    def test_retract_learning_asset_invalidates_packets_and_hides_asset(self) -> None:
        store = EventStore()
        self._seed_asset(store, asset_key="asset-1")
        request = ResolverInput(source_tool="kb", source_client="kb-mcp", project="demo")
        packet = build_learning_packet(request, tool_name="gap", assets=resolve_learning_assets(request, store=store), store=store)

        result = retract_learning_asset(asset_key="asset-1", actor="tester", reason="bad learning", store=store)

        self.assertEqual(result["lifecycle"], "retracted")
        self.assertEqual(result["invalidated_packets"], 1)
        row = store.get_learning_asset("asset-1")
        self.assertIsNotNone(row)
        self.assertEqual(row["lifecycle"], "retracted")
        self.assertEqual(row["learning_state_visibility"], "retracted")
        packet_row = store.get_learning_packet(str(packet["packet_id"]))
        self.assertEqual(packet_row["status"], "invalidated")
        self.assertEqual(len(store.list_learning_revocations()), 1)
        self.assertEqual(resolve_learning_assets(request, store=store), [])

    def test_supersede_learning_asset_marks_old_asset_stale(self) -> None:
        store = EventStore()
        self._seed_asset(store, asset_key="asset-old", updated_at="2026-03-26T00:00:00+00:00")
        self._seed_asset(store, asset_key="asset-new", updated_at="2026-03-26T00:10:00+00:00")
        request = ResolverInput(source_tool="kb", source_client="kb-mcp", project="demo")
        build_learning_packet(request, tool_name="gap", assets=resolve_learning_assets(request, store=store), store=store)

        result = supersede_learning_asset(
            asset_key="asset-old",
            replacement_asset_key="asset-new",
            actor="tester",
            reason="newer rule wins",
            store=store,
        )

        self.assertEqual(result["lifecycle"], "superseded")
        old_row = store.get_learning_asset("asset-old")
        self.assertEqual(old_row["confidence"], "stale")
        resolved = resolve_learning_assets(request, store=store)
        self.assertEqual([item.asset_key for item in resolved], ["asset-new"])

    def test_expire_learning_assets_marks_old_rows_expired(self) -> None:
        store = EventStore()
        self._seed_asset(store, asset_key="asset-old", updated_at="2026-03-26T00:00:00+00:00")
        self._seed_asset(store, asset_key="asset-new", updated_at="2026-03-26T00:30:00+00:00")
        request = ResolverInput(source_tool="kb", source_client="kb-mcp", project="demo")
        build_learning_packet(request, tool_name="gap", assets=resolve_learning_assets(request, store=store), store=store)

        result = expire_learning_assets(
            before="2026-03-26T00:05:00+00:00",
            actor="tester",
            reason="ttl elapsed",
            store=store,
        )

        self.assertEqual(result["expired"], 1)
        old_row = store.get_learning_asset("asset-old")
        self.assertEqual(old_row["lifecycle"], "expired")
        resolved = resolve_learning_assets(request, store=store)
        self.assertEqual([item.asset_key for item in resolved], ["asset-new"])

    def _seed_asset(
        self,
        store: EventStore,
        *,
        asset_key: str,
        updated_at: str = "2026-03-26T00:00:00+00:00",
    ) -> None:
        store.upsert_learning_asset(
            asset_key=asset_key,
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
            created_at="2026-03-26T00:00:00+00:00",
            updated_at=updated_at,
        )
