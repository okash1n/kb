from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

import yaml

from kb_mcp.config import load_config
from kb_mcp.events.store import EventStore
from kb_mcp.learning.models import ResolverInput
from kb_mcp.learning.resolver import resolve_learning_assets


class LearningResolverTest(unittest.TestCase):
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
        for subdir in [
            "projects/demo/session-log",
            "projects/demo/draft",
            "projects/demo/adr",
            "projects/demo/gap",
            "projects/demo/knowledge",
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
                    "timezone": "Asia/Tokyo",
                    "obsidian_cli": "auto",
                    "vault_git": False,
                }
            ),
            encoding="utf-8",
        )
        load_config.cache_clear()

    def test_resolver_orders_scopes_narrow_first(self) -> None:
        store = EventStore()
        self._seed_asset(
            store,
            asset_key="asset-general",
            scope="general",
            confidence="stable",
            force="default",
            provenance={"project": self.project, "source_client": "codex-cli", "session_id": "session-1"},
            traceability={"distribution_allowed": True, "secrecy_boundary": "general"},
        )
        self._seed_asset(
            store,
            asset_key="asset-project",
            scope="project_local",
            confidence="reviewed",
            force="preferred",
            provenance={"project": self.project, "source_client": "codex-cli", "session_id": "session-1"},
        )
        self._seed_asset(
            store,
            asset_key="asset-client",
            scope="client_local",
            confidence="reviewed",
            force="preferred",
            provenance={"project": self.project, "source_client": "codex-cli", "session_id": "session-1"},
        )
        self._seed_asset(
            store,
            asset_key="asset-session",
            scope="session_local",
            confidence="candidate",
            force="hint",
            provenance={"project": self.project, "source_client": "codex-cli", "session_id": "session-1"},
        )

        resolved = resolve_learning_assets(
            ResolverInput(
                source_tool="codex",
                source_client="codex-cli",
                session_id="session-1",
                project=self.project,
            ),
            store=store,
        )

        self.assertEqual(
            [item.asset_key for item in resolved],
            ["asset-session", "asset-client", "asset-project", "asset-general"],
        )

    def test_resolver_uses_confidence_then_force_then_updated_at(self) -> None:
        store = EventStore()
        self._seed_asset(
            store,
            asset_key="asset-stable",
            scope="project_local",
            confidence="stable",
            force="hint",
            updated_at="2026-03-26T00:00:00+00:00",
            provenance={"project": self.project},
        )
        self._seed_asset(
            store,
            asset_key="asset-guardrail",
            scope="project_local",
            confidence="reviewed",
            force="guardrail",
            updated_at="2026-03-26T00:10:00+00:00",
            provenance={"project": self.project},
        )
        self._seed_asset(
            store,
            asset_key="asset-newer",
            scope="project_local",
            confidence="reviewed",
            force="preferred",
            updated_at="2026-03-26T00:20:00+00:00",
            provenance={"project": self.project},
        )

        resolved = resolve_learning_assets(
            ResolverInput(source_tool="codex", source_client="codex-cli", project=self.project),
            store=store,
        )

        self.assertEqual(
            [item.asset_key for item in resolved],
            ["asset-stable", "asset-guardrail", "asset-newer"],
        )

    def test_resolver_filters_by_scope_context(self) -> None:
        store = EventStore()
        self._seed_asset(
            store,
            asset_key="asset-other-session",
            scope="session_local",
            provenance={"project": self.project, "source_client": "codex-cli", "session_id": "session-2"},
        )
        self._seed_asset(
            store,
            asset_key="asset-other-client",
            scope="client_local",
            provenance={"project": self.project, "source_client": "claude-code"},
        )
        self._seed_asset(
            store,
            asset_key="asset-other-project",
            scope="project_local",
            provenance={"project": "another-project"},
        )
        self._seed_asset(
            store,
            asset_key="asset-match",
            scope="project_local",
            provenance={"project": self.project},
        )

        resolved = resolve_learning_assets(
            ResolverInput(
                source_tool="codex",
                source_client="codex-cli",
                session_id="session-1",
                project=self.project,
            ),
            store=store,
        )

        self.assertEqual([item.asset_key for item in resolved], ["asset-match"])

    def test_resolver_enforces_client_specific_distribution(self) -> None:
        store = EventStore()
        self._seed_asset(
            store,
            asset_key="asset-general",
            scope="general",
            provenance={"project": self.project},
            traceability={"distribution_allowed": True, "secrecy_boundary": "general"},
        )
        self._seed_asset(
            store,
            asset_key="asset-user-global",
            scope="user_global",
            provenance={"project": self.project},
            traceability={"distribution_allowed": True, "secrecy_boundary": "user"},
        )

        copilot = resolve_learning_assets(
            ResolverInput(source_tool="copilot", source_client="copilot-cli", project=self.project),
            store=store,
        )
        claude = resolve_learning_assets(
            ResolverInput(source_tool="claude", source_client="claude-code", project=self.project),
            store=store,
        )

        self.assertEqual([item.asset_key for item in copilot], ["asset-user-global"])
        self.assertEqual([item.asset_key for item in claude], ["asset-user-global", "asset-general"])

    def test_resolver_applies_compatibility_fallback_for_wide_scope_metadata(self) -> None:
        store = EventStore()
        self._seed_asset(
            store,
            asset_key="asset-general-legacy",
            memory_class="knowledge",
            scope="general",
            provenance={"project": self.project},
            traceability={},
        )

        resolved = resolve_learning_assets(
            ResolverInput(source_tool="claude", source_client="claude-code", project=self.project),
            store=store,
        )

        self.assertEqual([item.asset_key for item in resolved], ["asset-general-legacy"])

    def test_resolver_keeps_narrow_scope_assets_without_boundary_for_copilot(self) -> None:
        store = EventStore()
        self._seed_asset(
            store,
            asset_key="asset-project-knowledge",
            memory_class="knowledge",
            scope="project_local",
            provenance={"project": self.project},
            traceability={},
        )

        resolved = resolve_learning_assets(
            ResolverInput(source_tool="copilot", source_client="copilot-cli", project=self.project),
            store=store,
        )

        self.assertEqual([item.asset_key for item in resolved], ["asset-project-knowledge"])

    def _seed_asset(
        self,
        store: EventStore,
        *,
        asset_key: str,
        memory_class: str = "gap",
        scope: str,
        confidence: str = "reviewed",
        force: str = "preferred",
        updated_at: str = "2026-03-26T00:00:00+00:00",
        provenance: dict[str, object],
        traceability: dict[str, object] | None = None,
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
            force=force,
            confidence=confidence,
            lifecycle="active",
            provenance=provenance,
            traceability=traceability or {},
            revocation_path={},
            learning_state_visibility="active",
            source_status="materialized",
            created_at="2026-03-26T00:00:00+00:00",
            updated_at=updated_at,
        )
