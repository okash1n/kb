"""Judge command runner for promotion candidates."""

from __future__ import annotations

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
    if pending_review_count >= SUGGESTION_THRESHOLD and suggestable:
        pending_rows = store.pending_review_candidates(limit=None)
        suggested = store.mark_candidates_suggested([row["candidate_key"] for row in pending_rows])

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
        return {"mode": "none", "reason": "no_windows"}
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
        return {"mode": "fallback", "breaker_key": breaker_key, **outcome}
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
        return {"mode": "fastpath", "breaker_key": breaker_key, **outcome}
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
        return {"mode": "fallback", "breaker_key": breaker_key, **outcome}


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
) -> dict[str, int]:
    existing = store.get_judge_run(window_id=window_payload["window_id"], prompt_version=prompt_version)
    if existing and existing["status"] == "judged":
        return {"judged_windows": 0, "skipped_windows": 1, "failed_windows": 0, "upserted_candidates": 0}

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
        return {"judged_windows": 0, "skipped_windows": 1, "failed_windows": 0, "upserted_candidates": 0}

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
        return {"judged_windows": 1, "skipped_windows": 0, "failed_windows": 0, "upserted_candidates": upserted_candidates}
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
