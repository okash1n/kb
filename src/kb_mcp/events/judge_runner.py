"""Judge command runner for promotion candidates."""

from __future__ import annotations

from collections import Counter
import hashlib
import json
import threading
from datetime import datetime, timedelta, timezone
from typing import Any

from kb_mcp.events.judge_backend import (
    HeuristicJudgeBackend,
    build_backend,
    build_fastpath_backend,
    fastpath_backend_command_hash,
)
from kb_mcp.events.learning_contract import default_candidate_semantics
from kb_mcp.events.judge_inputs import build_window_payload, build_windows
from kb_mcp.events.store import EventStore
from kb_mcp.events.types import utc_now_iso
from kb_mcp.note import generate_ulid

LEASE_SECONDS = 5 * 60
HEARTBEAT_SECONDS = 60
SUGGESTION_THRESHOLD = 5
DEFAULT_PARTITION_LIMIT = 50
CANDIDATE_SCORE_THRESHOLD = 0.75
FASTPATH_BREAKER_THRESHOLD = 3
FASTPATH_BREAKER_SECONDS = 10 * 60
FASTPATH_FALLBACK_PROMPT_SUFFIX = "+fastpath-fallback"
LABEL_ORDER = {"adr": 0, "gap": 1, "knowledge": 2, "session_thin": 3}
LABEL_DISPLAY = {
    "adr": "ADR",
    "gap": "gap",
    "knowledge": "knowledge",
    "session_thin": "session-log",
}


def review_candidates(
    *,
    partition_limit: int = DEFAULT_PARTITION_LIMIT,
    display_limit: int = DEFAULT_PARTITION_LIMIT,
    model_hint: str | None = None,
) -> dict[str, Any]:
    backend = build_backend(model_hint=model_hint)
    prompt_version = backend.prompt_version()
    store = EventStore()
    judged_windows = 0
    skipped_windows = 0
    failed_windows = 0
    upserted_candidates = 0

    for partition_key in store.checkpoint_partition_keys(limit=partition_limit):
        for window in build_windows(partition_key):
            try:
                outcome = _review_window_once(
                    store=store,
                    partition_key=partition_key,
                    window_payload=build_window_payload(window),
                    backend=backend,
                    prompt_version=prompt_version,
                    model_hint=model_hint,
                )
            except Exception:
                failed_windows += 1
                continue
            judged_windows += outcome["judged_windows"]
            skipped_windows += outcome["skipped_windows"]
            failed_windows += outcome["failed_windows"]
            upserted_candidates += outcome["upserted_candidates"]

    pending_review_count = store.pending_review_candidate_count()
    suggestable = store.suggestable_review_candidates()
    suggested = 0
    suggestion_bundles: list[dict[str, Any]] = []
    if pending_review_count >= SUGGESTION_THRESHOLD and suggestable:
        pending_rows = store.pending_review_candidates(limit=None)
        suggested = store.mark_candidates_suggested([row["candidate_key"] for row in pending_rows])
        suggestion_bundles = _build_proposal_bundles(pending_rows)

    listed_rows = store.pending_review_candidates(limit=display_limit)
    return {
        "prompt_version": prompt_version,
        "model_hint": model_hint,
        "backend_kind": backend.__class__.__name__,
        "judged_windows": judged_windows,
        "skipped_windows": skipped_windows,
        "failed_windows": failed_windows,
        "upserted_candidates": upserted_candidates,
        "pending_review": pending_review_count,
        "suggested": suggested,
        "suggestion_bundles": suggestion_bundles,
        "candidates": [
            {
                "candidate_key": row["candidate_key"],
                "window_id": row["window_id"],
                "label": row["label"],
                "status": row["status"],
                "score": row["score"],
            }
            for row in listed_rows
        ],
    }


def review_latest_window_fastpath(
    *,
    partition_key: str,
    source_tool: str,
    source_client: str,
    model_hint: str | None = None,
) -> dict[str, Any]:
    store = EventStore()
    windows = build_windows(partition_key)
    if not windows:
        return {"mode": "none", "reason": "no_windows", "suggested": 0, "proposal_bundles": []}
    payload = build_window_payload(windows[-1])
    backend_hash = fastpath_backend_command_hash()
    breaker_key = _breaker_key(source_tool, source_client, backend_hash)
    fallback_backend = HeuristicJudgeBackend()
    fastpath_backend = build_fastpath_backend(model_hint=model_hint)
    fallback_prompt_version = f"{fallback_backend.prompt_version()}{FASTPATH_FALLBACK_PROMPT_SUFFIX}"
    if fastpath_backend is None or _breaker_open(store, breaker_key):
        outcome = _review_window_once(
            store=store,
            partition_key=partition_key,
            window_payload=payload,
            backend=fallback_backend,
            prompt_version=fallback_prompt_version,
            model_hint=model_hint,
        )
        return _fastpath_response(
            store=store,
            partition_key=partition_key,
            payload=payload,
            breaker_key=breaker_key,
            mode="fallback",
            outcome=outcome,
        )
    try:
        outcome = _review_window_once(
            store=store,
            partition_key=partition_key,
            window_payload=payload,
            backend=fastpath_backend,
            prompt_version=fastpath_backend.prompt_version(),
            model_hint=model_hint,
        )
        try:
            store.clear_runtime_observation(breaker_key)
        except Exception:
            pass
        return _fastpath_response(
            store=store,
            partition_key=partition_key,
            payload=payload,
            breaker_key=breaker_key,
            mode="fastpath",
            outcome=outcome,
        )
    except Exception as exc:
        try:
            _record_breaker_failure(store, breaker_key, error=str(exc))
            store.put_runtime_observation(
                key=f"judge_fastpath_warning:{payload['window_id']}",
                severity="warning",
                message="judge fast-path degraded to heuristic backend",
                details={"window_id": payload["window_id"], "error": str(exc), "breaker_key": breaker_key},
            )
        except Exception:
            pass
        outcome = _review_window_once(
            store=store,
            partition_key=partition_key,
            window_payload=payload,
            backend=fallback_backend,
            prompt_version=fallback_prompt_version,
            model_hint=model_hint,
        )
        return _fastpath_response(
            store=store,
            partition_key=partition_key,
            payload=payload,
            breaker_key=breaker_key,
            mode="fallback",
            outcome=outcome,
        )


def _fastpath_response(
    *,
    store: EventStore,
    partition_key: str,
    payload: dict[str, Any],
    breaker_key: str | None,
    mode: str,
    outcome: dict[str, Any],
) -> dict[str, Any]:
    suggestion_rows: list[Any] = []
    suggested = 0
    if _is_proposal_timing(payload, outcome.get("decision")):
        pending_rows = store.pending_review_candidates(limit=None)
        suggestion_rows = [
            row
            for row in pending_rows
            if _candidate_partition_key(row) == partition_key
            and (
                _candidate_window_id(row) == payload["window_id"]
                or _candidate_is_suggestable(row)
            )
        ]
        if suggestion_rows:
            suggested = store.mark_candidates_suggested([row["candidate_key"] for row in suggestion_rows])
    return {
        "mode": mode,
        "breaker_key": breaker_key,
        "suggested": suggested,
        "proposal_bundles": _build_proposal_bundles(suggestion_rows),
        **outcome,
    }


def _upsert_candidate(
    *,
    store: EventStore,
    window_payload: dict[str, Any],
    judge_run_key: str,
    label: str,
    score: float,
    reasons: list[str],
    payload: dict[str, Any],
) -> int:
    semantics = default_candidate_semantics(label)
    store.upsert_promotion_candidate(
        candidate_key=_candidate_key(window_payload["window_id"], label),
        window_id=window_payload["window_id"],
        judge_run_key=judge_run_key,
        label=label,
        status="pending_review",
        score=score,
        slice_fingerprint=window_payload["window_id"],
        reasons=reasons,
        payload={**payload, "semantics": semantics},
    )
    return 1


def _review_window_once(
    *,
    store: EventStore,
    partition_key: str,
    window_payload: dict[str, Any],
    backend: Any,
    prompt_version: str,
    model_hint: str | None,
) -> dict[str, Any]:
    existing = store.get_judge_run(window_id=window_payload["window_id"], prompt_version=prompt_version)
    if existing and existing["status"] == "judged":
        return {
            "judged_windows": 0,
            "skipped_windows": 1,
            "failed_windows": 0,
            "upserted_candidates": 0,
            "decision": None,
        }

    judge_run_key = existing["judge_run_key"] if existing else generate_ulid()
    store.upsert_judge_run(
        judge_run_key=judge_run_key,
        partition_key=partition_key,
        window_id=window_payload["window_id"],
        start_ordinal=window_payload["start_ordinal"],
        end_ordinal=window_payload["end_ordinal"],
        window_index=window_payload["window_index"],
        status="ready",
        prompt_version=prompt_version,
        labels=[],
        decision={},
        model_hint=model_hint,
    )
    lease_owner = generate_ulid()
    claimed = store.claim_judge_run(
        window_id=window_payload["window_id"],
        prompt_version=prompt_version,
        lease_owner=lease_owner,
        lease_expires_at=_lease_expires_at(),
    )
    if claimed is None:
        return {
            "judged_windows": 0,
            "skipped_windows": 1,
            "failed_windows": 0,
            "upserted_candidates": 0,
            "decision": None,
        }

    stop_heartbeat = threading.Event()
    heartbeat_thread = threading.Thread(
        target=_heartbeat_loop,
        args=(store, claimed["judge_run_key"], lease_owner, stop_heartbeat),
        daemon=True,
    )
    heartbeat_thread.start()
    try:
        decision = backend.review_window(
            window_payload,
            prompt_version=prompt_version,
            model_hint=model_hint,
        ).as_dict()
        labels = decision.get("labels", [])
        store.upsert_judge_run(
            judge_run_key=claimed["judge_run_key"],
            partition_key=partition_key,
            window_id=window_payload["window_id"],
            start_ordinal=window_payload["start_ordinal"],
            end_ordinal=window_payload["end_ordinal"],
            window_index=window_payload["window_index"],
            status="judged",
            prompt_version=prompt_version,
            labels=labels,
            decision=decision,
            model_hint=model_hint,
        )
        store.release_judge_run(judge_run_key=claimed["judge_run_key"], lease_owner=lease_owner)
        upserted_candidates = 0
        for label_entry in labels:
            if float(label_entry.get("score") or 0.0) < CANDIDATE_SCORE_THRESHOLD:
                continue
            upserted_candidates += _upsert_candidate(
                store=store,
                window_payload=window_payload,
                judge_run_key=claimed["judge_run_key"],
                label=label_entry["label"],
                score=float(label_entry.get("score") or 0.0),
                reasons=list(label_entry.get("reasons") or []),
                payload={"window": window_payload, "decision": decision},
            )
        if decision.get("should_emit_thin_session"):
            upserted_candidates += _upsert_candidate(
                store=store,
                window_payload=window_payload,
                judge_run_key=claimed["judge_run_key"],
                label="session_thin",
                score=1.0,
                reasons=["carry_chain_terminal", "no_strong_labels"],
                payload={"window": window_payload, "decision": decision},
            )
        return {
            "judged_windows": 1,
            "skipped_windows": 0,
            "failed_windows": 0,
            "upserted_candidates": upserted_candidates,
            "decision": decision,
        }
    except Exception as exc:
        store.upsert_judge_run(
            judge_run_key=claimed["judge_run_key"],
            partition_key=partition_key,
            window_id=window_payload["window_id"],
            start_ordinal=window_payload["start_ordinal"],
            end_ordinal=window_payload["end_ordinal"],
            window_index=window_payload["window_index"],
            status="failed",
            prompt_version=prompt_version,
            labels=[],
            decision={"error": str(exc)},
            model_hint=model_hint,
        )
        store.release_judge_run(judge_run_key=claimed["judge_run_key"], lease_owner=lease_owner)
        store.put_runtime_observation(
            key=f"judge_failure:{window_payload['window_id']}",
            severity="warning",
            message="judge review-candidates failed",
            details={"window_id": window_payload["window_id"], "error": str(exc)},
        )
        raise
    finally:
        stop_heartbeat.set()
        heartbeat_thread.join(timeout=HEARTBEAT_SECONDS)


def _build_proposal_bundles(rows: list[Any]) -> list[dict[str, Any]]:
    if not rows:
        return []
    grouped: dict[str, list[Any]] = {}
    for row in rows:
        grouped.setdefault(_candidate_window_id(row), []).append(row)
    bundles: list[dict[str, Any]] = []
    for window_id, bundle_rows in grouped.items():
        if not window_id:
            continue
        bundles.append(_build_proposal_bundle(window_id, bundle_rows))
    bundles.sort(key=lambda item: (int(item["window_index"] or 0), item["window_id"]))
    return bundles


def _build_proposal_bundle(window_id: str, rows: list[Any]) -> dict[str, Any]:
    payload = _candidate_payload(rows[0])
    window = dict(payload.get("window") or {})
    checkpoints = list(window.get("checkpoints") or [])
    label_counts = Counter(str(row["label"]) for row in rows)
    labels = sorted(label_counts.keys(), key=lambda label: LABEL_ORDER.get(label, 99))
    return {
        "bundle_key": window_id,
        "window_id": window_id,
        "partition_key": window.get("partition_key"),
        "window_index": window.get("window_index"),
        "project": _window_value(checkpoints, "project"),
        "repo": _window_value(checkpoints, "repo"),
        "session_id": _window_value(checkpoints, "session_id"),
        "headline": _proposal_headline(label_counts),
        "summary": _proposal_summary(label_counts, checkpoints),
        "labels": labels,
        "candidate_keys": [str(row["candidate_key"]) for row in rows],
        "candidates": [
            {
                "candidate_key": str(row["candidate_key"]),
                "label": str(row["label"]),
                "score": float(row["score"]),
                "status": str(row["status"]),
                "suggestion_seq": int(row["suggestion_seq"]),
            }
            for row in sorted(rows, key=lambda row: (LABEL_ORDER.get(str(row["label"]), 99), str(row["candidate_key"])))
        ],
        "checkpoint_count": len(checkpoints),
        "checkpoint_summaries": [
            str(checkpoint.get("summary") or "(no summary)")
            for checkpoint in checkpoints[:3]
        ],
        "timing": {
            "final_hint": any(bool(checkpoint.get("final_hint")) for checkpoint in checkpoints),
            "session_end": any(checkpoint.get("checkpoint_kind") == "session_end" for checkpoint in checkpoints),
            "carry_chain_terminal": bool(window.get("carry_chain_terminal")),
        },
    }


def _proposal_headline(label_counts: Counter[str]) -> str:
    if set(label_counts.keys()) == {"session_thin"}:
        return "この区切りを session-log 候補として提案できます。"
    return f"この区切りで {_render_label_counts(label_counts)} をまとめてレビューできます。"


def _proposal_summary(label_counts: Counter[str], checkpoints: list[dict[str, Any]]) -> str:
    context = " / ".join(
        str(checkpoint.get("summary") or "(no summary)")
        for checkpoint in checkpoints[:2]
    )
    if context:
        return f"{_render_label_counts(label_counts)} が見つかりました。文脈: {context}"
    return f"{_render_label_counts(label_counts)} が見つかりました。"


def _render_label_counts(label_counts: Counter[str]) -> str:
    parts = [
        f"{LABEL_DISPLAY.get(label, label)} 候補 {label_counts[label]} 件"
        for label in sorted(label_counts.keys(), key=lambda label: LABEL_ORDER.get(label, 99))
    ]
    return "、".join(parts)


def _candidate_payload(row: Any) -> dict[str, Any]:
    return json.loads(str(row["payload_json"])) if row["payload_json"] else {}


def _candidate_window_id(row: Any) -> str:
    return str(row["window_id"])


def _candidate_partition_key(row: Any) -> str | None:
    payload = _candidate_payload(row)
    window = dict(payload.get("window") or {})
    partition_key = window.get("partition_key")
    return str(partition_key) if partition_key else None


def _candidate_is_suggestable(row: Any) -> bool:
    last_suggested_at = row["last_suggested_at"]
    updated_at = row["updated_at"]
    return last_suggested_at is None or (updated_at is not None and str(updated_at) > str(last_suggested_at))


def _window_value(checkpoints: list[dict[str, Any]], key: str) -> str | None:
    for checkpoint in checkpoints:
        value = checkpoint.get(key)
        if value:
            return str(value)
    return None


def _is_proposal_timing(window_payload: dict[str, Any], decision: dict[str, Any] | None) -> bool:
    if decision and bool(decision.get("should_emit_thin_session")):
        return True
    return any(
        checkpoint.get("final_hint") or checkpoint.get("checkpoint_kind") == "session_end"
        for checkpoint in window_payload.get("checkpoints", [])
    )


def _candidate_key(window_id: str, label: str) -> str:
    return hashlib.sha256(f"{window_id}\x1f{label}".encode("utf-8")).hexdigest()


def _lease_expires_at() -> str:
    return (
        datetime.fromisoformat(utc_now_iso()).astimezone(timezone.utc) + timedelta(seconds=LEASE_SECONDS)
    ).isoformat(timespec="seconds")


def _heartbeat_loop(
    store: EventStore,
    judge_run_key: str,
    lease_owner: str,
    stop_event: threading.Event,
) -> None:
    while not stop_event.wait(HEARTBEAT_SECONDS):
        if not store.heartbeat_judge_run(
            judge_run_key=judge_run_key,
            lease_owner=lease_owner,
            lease_expires_at=_lease_expires_at(),
        ):
            return


def _breaker_key(source_tool: str, source_client: str, backend_hash: str | None) -> str:
    suffix = backend_hash or "none"
    return f"judge_fastpath_breaker:{source_tool}:{source_client}:{suffix}"


def _breaker_open(store: EventStore, breaker_key: str) -> bool:
    row = store.get_runtime_observation(breaker_key)
    if row is None:
        return False
    details = json.loads(str(row["details_json"]))
    until = details.get("until")
    return isinstance(until, str) and until > utc_now_iso()


def _record_breaker_failure(store: EventStore, breaker_key: str, *, error: str) -> None:
    row = store.get_runtime_observation(breaker_key)
    failure_count = 1
    if row is not None:
        details = json.loads(str(row["details_json"]))
        failure_count = int(details.get("failures", 0)) + 1
    details = {"failures": failure_count, "error": error}
    if failure_count >= FASTPATH_BREAKER_THRESHOLD:
        details["until"] = (
            datetime.fromisoformat(utc_now_iso()).astimezone(timezone.utc)
            + timedelta(seconds=FASTPATH_BREAKER_SECONDS)
        ).isoformat(timespec="seconds")
    store.put_runtime_observation(
        key=breaker_key,
        severity="warning",
        message="judge fast-path breaker state",
        details=details,
    )


def fastpath_backend_status() -> dict[str, Any]:
    backend_hash = fastpath_backend_command_hash()
    return {
        "configured": backend_hash is not None,
        "backend_hash": backend_hash,
    }


def fastpath_breaker_status() -> dict[str, Any]:
    store = EventStore()
    rows = store.list_runtime_observations(prefix="judge_fastpath_breaker:")
    total = len(rows)
    open_keys: list[str] = []
    for row in rows:
        details = json.loads(str(row["details_json"]))
        until = details.get("until")
        if isinstance(until, str) and until > utc_now_iso():
            open_keys.append(str(row["observation_key"]))
    return {
        "total": total,
        "open": len(open_keys),
        "keys": open_keys,
    }
