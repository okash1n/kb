"""Distribution metadata helpers for learning assets."""

from __future__ import annotations


def scope_distribution_metadata(memory_class: str) -> dict[str, object]:
    if memory_class == "knowledge":
        return {"distribution_allowed": True, "secrecy_boundary": "general"}
    if memory_class == "gap":
        return {"distribution_allowed": True, "secrecy_boundary": "user"}
    if memory_class == "adr":
        return {"distribution_allowed": False, "secrecy_boundary": "project"}
    return {"distribution_allowed": False, "secrecy_boundary": "project"}
