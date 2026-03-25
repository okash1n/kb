"""Heuristic candidate detection for gap / knowledge promotion."""

from __future__ import annotations

import re

GAP_PATTERNS = [
    (re.compile(r"(ちゃう|違う|そうじゃない|そういうことじゃない|じゃなくて|ではなく)"), "user_correction"),
    (re.compile(r"(してほしい|してほしかった|べき|見るべき|読むべき|出すべき)"), "explicit_preference"),
    (re.compile(r"(長すぎ|多すぎ|わかりにくい|読まれへん|見えてるか|守れてへん)"), "ux_complaint"),
]

KNOWLEDGE_PATTERNS = [
    (re.compile(r"(原因は|根本原因|仕様|schema|ロジック|前提|挙動)"), "technical_explanation"),
    (re.compile(r"(必要がある|必要や|必要です|must|required|only|だけ)"), "constraint"),
    (re.compile(r"(判明|確認した|確認できた|切り替わってる|registered|enabled)"), "verified_fact"),
]


def detect_candidates(summary: str | None, content: str | None) -> dict[str, object]:
    """Detect promotion candidates from a checkpoint excerpt."""
    text = "\n".join(part.strip() for part in [summary or "", content or ""] if part and part.strip())
    gap_reasons = _matched_reasons(text, GAP_PATTERNS)
    knowledge_reasons = _matched_reasons(text, KNOWLEDGE_PATTERNS)
    candidates: list[dict[str, object]] = []
    if len(gap_reasons) >= 1:
        candidates.append({"kind": "gap", "score": len(gap_reasons), "reasons": gap_reasons[:3]})
    if len(knowledge_reasons) >= 2:
        candidates.append(
            {"kind": "knowledge", "score": len(knowledge_reasons), "reasons": knowledge_reasons[:3]}
        )
    return {"has_candidates": bool(candidates), "items": candidates}


def _matched_reasons(text: str, patterns: list[tuple[re.Pattern[str], str]]) -> list[str]:
    reasons: list[str] = []
    if not text:
        return reasons
    for pattern, label in patterns:
        if pattern.search(text):
            reasons.append(label)
    return reasons
