"""Judge backend contract and default heuristic backend."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json
import os
import subprocess
from typing import Any, Protocol

from importlib.resources import files


@dataclass(slots=True)
class JudgeDecision:
    labels: list[dict[str, Any]]
    should_emit_thin_session: bool
    carry_forward: bool
    notes: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "labels": self.labels,
            "should_emit_thin_session": self.should_emit_thin_session,
            "carry_forward": self.carry_forward,
            "notes": self.notes,
        }


class JudgeBackend(Protocol):
    def prompt_version(self) -> str: ...

    def review_window(
        self,
        payload: dict[str, Any],
        *,
        prompt_version: str,
        model_hint: str | None = None,
    ) -> JudgeDecision: ...


def load_prompt_template() -> str:
    resource = files("kb_mcp.assets").joinpath("judge/review-candidates.md")
    return Path(str(resource)).read_text(encoding="utf-8")


def load_prompt_version() -> str:
    for line in load_prompt_template().splitlines():
        if line.startswith("prompt_version:"):
            return line.split(":", 1)[1].strip()
    raise RuntimeError("prompt_version not found in judge prompt template")


class HeuristicJudgeBackend:
    """Deterministic backend used until an interactive client backend is wired in."""

    def prompt_version(self) -> str:
        return load_prompt_version()

    def review_window(
        self,
        payload: dict[str, Any],
        *,
        prompt_version: str,
        model_hint: str | None = None,
    ) -> JudgeDecision:
        labels: list[dict[str, Any]] = []
        anchor_matches = set(payload.get("anchor_matches") or [])
        knowledge_signals = payload.get("knowledge_signals") or {}

        if "adr" in anchor_matches:
            labels.append(
                {
                    "label": "adr",
                    "score": 0.9,
                    "reasons": ["anchor phrase indicates agreement on one direction"],
                }
            )
        if "gap" in anchor_matches:
            labels.append(
                {
                    "label": "gap",
                    "score": 0.9,
                    "reasons": ["anchor phrase indicates explicit user correction"],
                }
            )

        knowledge_reasons = [
            key
            for key in (
                "fact_confirmed",
                "constraint_confirmed",
                "cause_identified",
                "comparison_settled",
            )
            if knowledge_signals.get(key)
        ]
        if knowledge_reasons:
            labels.append(
                {
                    "label": "knowledge",
                    "score": 0.8,
                    "reasons": knowledge_reasons,
                }
            )

        should_emit_thin_session = (
            bool(payload.get("carry_chain_terminal"))
            and not labels
            and not any(
                checkpoint.get("final_hint") or checkpoint.get("checkpoint_kind") == "session_end"
                for checkpoint in payload.get("checkpoints", [])
            )
        )
        notes = f"heuristic backend ({model_hint or 'active-client'}, {prompt_version})"
        return JudgeDecision(
            labels=labels,
            should_emit_thin_session=should_emit_thin_session,
            carry_forward=bool(payload.get("carry_forward")),
            notes=notes,
        )


class CommandJudgeBackend:
    """External command backend for active-client model execution."""

    def __init__(self, command: str) -> None:
        self._command = command

    def prompt_version(self) -> str:
        return load_prompt_version()

    def review_window(
        self,
        payload: dict[str, Any],
        *,
        prompt_version: str,
        model_hint: str | None = None,
    ) -> JudgeDecision:
        body = json.dumps(
            {
                "prompt_version": prompt_version,
                "prompt_template": load_prompt_template(),
                "window": payload,
                "model_hint": model_hint,
            },
            ensure_ascii=False,
        )
        completed = subprocess.run(
            self._command,
            input=body,
            text=True,
            shell=True,
            check=True,
            capture_output=True,
        )
        response = json.loads(completed.stdout)
        return JudgeDecision(
            labels=list(response.get("labels", [])),
            should_emit_thin_session=bool(response.get("should_emit_thin_session", False)),
            carry_forward=bool(response.get("carry_forward", payload.get("carry_forward", False))),
            notes=str(response.get("notes", "")),
        )


def build_backend(model_hint: str | None = None) -> JudgeBackend:
    command = os.environ.get("KB_JUDGE_BACKEND_COMMAND", "").strip()
    if command:
        return CommandJudgeBackend(command)
    return HeuristicJudgeBackend()
