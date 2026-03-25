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

from kb_mcp.config import load_config, runtime_dir
from kb_mcp.events.store import EventStore
from kb_mcp.learning.policy_projection import build_policy_projections
from kb_mcp.learning.policy_snapshot import build_policy_snapshots, load_policy_snapshots
from kb_mcp.tools.graduate import kb_graduate


class PolicyProjectionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        root = Path(self.tmpdir.name)
        self.config_dir = root / "config"
        self.vault = root / "vault"
        os.environ["KB_CONFIG_DIR"] = str(self.config_dir)
        self.addCleanup(lambda: os.environ.pop("KB_CONFIG_DIR", None))
        self.vault.mkdir(parents=True, exist_ok=True)
        for subdir in [
            "projects/demo/gap",
            "projects/demo/knowledge",
            "projects/demo/adr",
            "general/knowledge",
            "general/requirements",
        ]:
            (self.vault / subdir).mkdir(parents=True, exist_ok=True)
        cfg_path = self.config_dir / "config.yml"
        cfg_path.parent.mkdir(parents=True, exist_ok=True)
        cfg_path.write_text(
            yaml.safe_dump(
                {"vault_path": str(self.vault), "kb_root": "", "timezone": "Asia/Tokyo", "obsidian_cli": "auto", "vault_git": False}
            ),
            encoding="utf-8",
        )
        load_config.cache_clear()

    def test_build_policy_projections_groups_active_assets(self) -> None:
        store = EventStore()
        self._seed_asset(store, asset_key="asset-project", scope="project_local", memory_class="gap", provenance={"project": "demo"})
        self._seed_asset(store, asset_key="asset-user", scope="user_global", memory_class="knowledge", provenance={"project": "demo"})

        projections = build_policy_projections(store=store)

        self.assertEqual(set(projections.keys()), {"project:demo", "user:global"})
        self.assertEqual(projections["project:demo"][0]["asset_key"], "asset-project")
        self.assertEqual(projections["user:global"][0]["asset_key"], "asset-user")

    def test_build_policy_snapshots_writes_runtime_json(self) -> None:
        store = EventStore()
        self._seed_asset(store, asset_key="asset-project", scope="project_local", memory_class="gap", provenance={"project": "demo"})

        result = build_policy_snapshots(store=store)

        self.assertEqual(result["targets"], 1)
        snapshot_path = runtime_dir() / "learning" / "project" / "demo.json"
        self.assertTrue(snapshot_path.exists())
        payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
        self.assertEqual(payload["target"], "project:demo")
        self.assertEqual(payload["policy_count"], 1)
        self.assertEqual(load_policy_snapshots()[0]["target"], "project:demo")

    def test_cli_build_policy_snapshots_and_graduate_reads_snapshots(self) -> None:
        store = EventStore()
        self._seed_asset(store, asset_key="asset-project", scope="project_local", memory_class="gap", provenance={"project": "demo"})
        from kb_mcp import cli

        buf = io.StringIO()
        with patch("sys.argv", ["kb-mcp", "judge", "build-policy-snapshots"]):
            with redirect_stdout(buf):
                cli.main()
        payload = json.loads(buf.getvalue())
        self.assertEqual(payload["targets"], 1)

        report = kb_graduate()
        self.assertIn("Runtime policy snapshots", report)
        self.assertIn("project:demo", report)

    def _seed_asset(
        self,
        store: EventStore,
        *,
        asset_key: str,
        scope: str,
        memory_class: str,
        provenance: dict[str, object],
    ) -> None:
        store.upsert_learning_asset(
            asset_key=asset_key,
            candidate_key=None,
            review_id=None,
            materialization_key=None,
            note_id=None,
            note_path=None,
            memory_class=memory_class,
            update_target="behavior_style",
            scope=scope,
            force="preferred",
            confidence="reviewed",
            lifecycle="active",
            provenance=provenance,
            traceability={},
            revocation_path={},
            learning_state_visibility="active",
            source_status="materialized",
        )
