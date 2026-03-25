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
from kb_mcp.events.store import EventStore


class LearningCliTest(unittest.TestCase):
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

    def test_judge_learning_state_prints_assets(self) -> None:
        store = EventStore()
        store.upsert_learning_asset(
            asset_key="asset-1",
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
        from kb_mcp import cli

        buf = io.StringIO()
        with patch("sys.argv", ["kb-mcp", "judge", "learning-state", "--limit", "10"]):
            with redirect_stdout(buf):
                cli.main()
        payload = json.loads(buf.getvalue())
        self.assertEqual(payload["total"], 1)
        self.assertEqual(payload["results"][0]["asset_key"], "asset-1")
