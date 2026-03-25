"""Client capability rules for runtime learning packets."""

from __future__ import annotations

from dataclasses import dataclass, replace

from kb_mcp.learning.models import LearningAssetView
from kb_mcp.learning.scope_promotion import scope_distribution_metadata


@dataclass(frozen=True, slots=True)
class ClientCapabilities:
    client: str
    allowed_scopes: frozenset[str]
    allowed_secrecy_boundaries: frozenset[str]
    force_downgrades: dict[tuple[str, str], str]


_FULL_SCOPES = frozenset({"session_local", "client_local", "project_local", "user_global", "general"})
_USER_SCOPES = frozenset({"session_local", "client_local", "project_local", "user_global"})
_LOCAL_SCOPES = frozenset({"session_local", "client_local", "project_local"})
_FULL_BOUNDARIES = frozenset({"project", "user", "general"})
_USER_BOUNDARIES = frozenset({"project", "user"})
_PROJECT_BOUNDARIES = frozenset({"project"})

_UNKNOWN_CLIENT = ClientCapabilities(
    client="unknown",
    allowed_scopes=_LOCAL_SCOPES,
    allowed_secrecy_boundaries=_PROJECT_BOUNDARIES,
    force_downgrades={},
)

_CAPABILITIES: dict[str, ClientCapabilities] = {
    "kb-mcp": ClientCapabilities(
        client="kb-mcp",
        allowed_scopes=_FULL_SCOPES,
        allowed_secrecy_boundaries=_FULL_BOUNDARIES,
        force_downgrades={},
    ),
    "claude-code": ClientCapabilities(
        client="claude-code",
        allowed_scopes=_FULL_SCOPES,
        allowed_secrecy_boundaries=_FULL_BOUNDARIES,
        force_downgrades={},
    ),
    "codex-cli": ClientCapabilities(
        client="codex-cli",
        allowed_scopes=_FULL_SCOPES,
        allowed_secrecy_boundaries=_FULL_BOUNDARIES,
        force_downgrades={},
    ),
    "copilot-cli": ClientCapabilities(
        client="copilot-cli",
        allowed_scopes=_USER_SCOPES,
        allowed_secrecy_boundaries=_USER_BOUNDARIES,
        force_downgrades={
            ("user_global", "default"): "preferred",
            ("user_global", "preferred"): "hint",
        },
    ),
    "copilot-vscode": ClientCapabilities(
        client="copilot-vscode",
        allowed_scopes=_USER_SCOPES,
        allowed_secrecy_boundaries=_USER_BOUNDARIES,
        force_downgrades={
            ("user_global", "default"): "preferred",
            ("user_global", "preferred"): "hint",
        },
    ),
}

_CLIENT_FAMILY_ALIASES = {
    "claude-": "claude-code",
    "copilot-": "copilot-vscode",
    "codex-": "codex-cli",
}


def get_client_capabilities(source_client: str) -> ClientCapabilities:
    if source_client in _CAPABILITIES:
        return _CAPABILITIES[source_client]
    for prefix, canonical in _CLIENT_FAMILY_ALIASES.items():
        if source_client.startswith(prefix):
            return _CAPABILITIES[canonical]
    return _UNKNOWN_CLIENT


def client_allows_asset(asset: LearningAssetView, *, source_client: str) -> bool:
    capabilities = get_client_capabilities(source_client)
    if asset.scope not in capabilities.allowed_scopes:
        return False
    if asset.scope in {"session_local", "client_local", "project_local"}:
        return True
    secrecy_boundary = effective_secrecy_boundary(asset)
    if secrecy_boundary is None:
        return False
    return str(secrecy_boundary) in capabilities.allowed_secrecy_boundaries


def adjust_asset_for_client(asset: LearningAssetView, *, source_client: str) -> LearningAssetView | None:
    if not client_allows_asset(asset, source_client=source_client):
        return None
    capabilities = get_client_capabilities(source_client)
    downgraded_force = capabilities.force_downgrades.get((asset.scope, asset.force))
    if downgraded_force is None:
        return asset
    return replace(asset, force=downgraded_force)


def effective_distribution_allowed(asset: LearningAssetView) -> bool:
    raw = asset.traceability.get("distribution_allowed")
    if raw is not None:
        return bool(raw)
    return bool(scope_distribution_metadata(asset.memory_class)["distribution_allowed"])


def effective_secrecy_boundary(asset: LearningAssetView) -> str | None:
    raw = asset.traceability.get("secrecy_boundary")
    if raw is not None:
        return str(raw)
    metadata = scope_distribution_metadata(asset.memory_class)
    boundary = metadata.get("secrecy_boundary")
    return str(boundary) if boundary is not None else None
