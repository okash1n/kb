from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import yaml

from kb_mcp.config import load_config
from kb_mcp.events.store import EventStore
from kb_mcp.learning.models import ResolverInput
from kb_mcp.learning.resolver import resolve_learning_assets
from kb_mcp.learning.scope_promotion import promote_learning_scopes


class ScopePromotionTest(unittest.TestCase):
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
            "projects/demo-1/gap",
            "projects/demo-2/gap",
            "projects/demo-3/gap",
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

    def test_promote_learning_scopes_creates_user_global_asset(self) -> None:
        store = EventStore()
        self._seed_asset(store, asset_key="a1", project="demo-1", memory_class="gap")
        self._seed_asset(store, asset_key="a2", project="demo-2", memory_class="gap")

        result = promote_learning_scopes(store=store)

        self.assertEqual(result["promoted"], 1)
        row = store.get_learning_asset("promoted:user_global:gap:behavior_style")
        self.assertIsNotNone(row)
        self.assertEqual(row["scope"], "user_global")

    def test_promote_learning_scopes_creates_general_asset_for_knowledge(self) -> None:
        store = EventStore()
        self._seed_asset(store, asset_key="k1", project="demo-1", memory_class="knowledge")
        self._seed_asset(store, asset_key="k2", project="demo-2", memory_class="knowledge")
        self._seed_asset(store, asset_key="k3", project="demo-3", memory_class="knowledge")

        result = promote_learning_scopes(store=store)

        self.assertEqual(result["promoted"], 2)
        general = store.get_learning_asset("promoted:general:knowledge:behavior_style")
        self.assertIsNotNone(general)
        self.assertEqual(general["scope"], "general")

    def test_resolver_rejects_distribution_disallowed_general_asset(self) -> None:
        store = EventStore()
        store.upsert_learning_asset(
            asset_key="general-adr",
            candidate_key=None,
            review_id=None,
            materialization_key=None,
            note_id=None,
            note_path=None,
            memory_class="adr",
            update_target="decision",
            scope="general",
            force="default",
            confidence="stable",
            lifecycle="active",
            provenance={"projects": ["demo-1", "demo-2"]},
            traceability={"distribution_allowed": False, "secrecy_boundary": "project"},
            revocation_path={},
            learning_state_visibility="active",
            source_status="promoted",
        )

        resolved = resolve_learning_assets(
            ResolverInput(source_tool="kb", source_client="kb-mcp", project="demo-1"),
            store=store,
        )
        self.assertEqual(resolved, [])

    def test_store_import_succeeds_in_fresh_python_process(self) -> None:
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "import kb_mcp.events.store as store; print(store.__name__)",
            ],
            cwd=str(Path(__file__).resolve().parents[1]),
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertEqual(result.stdout.strip(), "kb_mcp.events.store")

    def test_scope_promotion_import_succeeds_in_fresh_python_process(self) -> None:
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "import kb_mcp.learning.scope_promotion as module; print(callable(module.promote_learning_scopes))",
            ],
            cwd=str(Path(__file__).resolve().parents[1]),
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertEqual(result.stdout.strip(), "True")

    def test_promote_scopes_help_succeeds_in_fresh_python_process(self) -> None:
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "kb_mcp.cli",
                "judge",
                "promote-scopes",
                "--help",
            ],
            cwd=str(Path(__file__).resolve().parents[1]),
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("promote-scopes", result.stdout)

    def _seed_asset(self, store: EventStore, *, asset_key: str, project: str, memory_class: str) -> None:
        store.upsert_learning_asset(
            asset_key=asset_key,
            candidate_key=None,
            review_id=None,
            materialization_key=None,
            note_id=None,
            note_path=None,
            memory_class=memory_class,
            update_target="behavior_style",
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
