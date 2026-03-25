from __future__ import annotations

import unittest

from kb_mcp.learning.client_capabilities import (
    adjust_asset_for_client,
    effective_secrecy_boundary,
    get_client_capabilities,
)
from kb_mcp.learning.models import LearningAssetView


def _asset(*, scope: str, force: str = "preferred", secrecy_boundary: str | None = None) -> LearningAssetView:
    traceability: dict[str, object] = {"distribution_allowed": True}
    if secrecy_boundary is not None:
        traceability["secrecy_boundary"] = secrecy_boundary
    return LearningAssetView(
        asset_key=f"asset:{scope}:{force}",
        memory_class="gap",
        update_target="behavior_style",
        scope=scope,
        force=force,
        confidence="reviewed",
        lifecycle="active",
        learning_state_visibility="active",
        candidate_key=None,
        review_id=None,
        materialization_key=None,
        note_id=None,
        note_path=None,
        updated_at="2026-03-26T00:00:00+00:00",
        provenance={"project": "demo"},
        traceability=traceability,
        revocation_path={},
    )


class ClientCapabilitiesTest(unittest.TestCase):
    def test_known_clients_have_capability_rows(self) -> None:
        self.assertEqual(get_client_capabilities("claude-code").client, "claude-code")
        self.assertEqual(get_client_capabilities("copilot-cli").client, "copilot-cli")
        self.assertEqual(get_client_capabilities("copilot-vscode").client, "copilot-vscode")
        self.assertEqual(get_client_capabilities("codex-cli").client, "codex-cli")

    def test_unknown_client_fails_closed(self) -> None:
        capabilities = get_client_capabilities("future-client")

        self.assertEqual(capabilities.allowed_scopes, frozenset({"session_local", "client_local", "project_local"}))
        self.assertEqual(capabilities.allowed_secrecy_boundaries, frozenset({"project"}))

    def test_client_family_alias_uses_known_capabilities(self) -> None:
        capabilities = get_client_capabilities("copilot-enterprise")

        self.assertEqual(capabilities.client, "copilot-vscode")

    def test_copilot_omits_general_assets(self) -> None:
        asset = _asset(scope="general", secrecy_boundary="general")

        adjusted = adjust_asset_for_client(asset, source_client="copilot-cli")

        self.assertIsNone(adjusted)

    def test_wide_scope_requires_explicit_secrecy_boundary(self) -> None:
        asset = _asset(scope="user_global", secrecy_boundary="user")
        asset.traceability.pop("secrecy_boundary")

        adjusted = adjust_asset_for_client(asset, source_client="claude-code")

        self.assertIsNotNone(adjusted)
        self.assertEqual(effective_secrecy_boundary(adjusted), "user")

    def test_copilot_downgrades_user_global_force(self) -> None:
        asset = _asset(scope="user_global", force="preferred", secrecy_boundary="user")

        adjusted = adjust_asset_for_client(asset, source_client="copilot-cli")

        self.assertIsNotNone(adjusted)
        self.assertEqual(adjusted.force, "hint")

    def test_copilot_downgrades_user_global_default_to_preferred(self) -> None:
        asset = _asset(scope="user_global", force="default", secrecy_boundary="user")

        adjusted = adjust_asset_for_client(asset, source_client="copilot-vscode")

        self.assertIsNotNone(adjusted)
        self.assertEqual(adjusted.force, "preferred")

    def test_claude_preserves_general_assets(self) -> None:
        asset = _asset(scope="general", force="default", secrecy_boundary="general")

        adjusted = adjust_asset_for_client(asset, source_client="claude-code")

        self.assertIsNotNone(adjusted)
        self.assertEqual(adjusted.force, "default")
