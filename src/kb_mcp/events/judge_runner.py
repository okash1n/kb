"""Judge command runner for promotion candidates."""

from __future__ import annotations

import hashlib
import threading
from datetime import datetime, timedelta, timezone
from typing import Any

from kb_mcp.events.judge_backend import build_backend
from kb_mcp.events.judge_inputs import build_window_payload, build_windows
from kb_mcp.events.store import EventStore
from kb_mcp.events.types import utc_now_iso
from kb_mcp.note import generate_ulid

LEASE_SECONDS = 5 * 60
HEARTBEAT_SECONDS = 60
SUGGESTION_THRESHOLD = 5
DEFAULT_PARTITION_LIMIT = 50
CANDIDATE_SCORE_THRESHOLD = 0.75


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
            payload = build_window_payload(window)
            existing = store.get_judge_run(window_id=payload["window_id"], prompt_version=prompt_version)
            if existing and existing["status"] == "judged":
                skipped_windows += 1
                continue

            judge_run_key = existing["judge_run_key"] if existing else generate_ulid()
            store.upsert_judge_run(
                judge_run_key=judge_run_key,
                partition_key=partition_key,
                window_id=payload["window_id"],
                start_ordinal=payload["start_ordinal"],
                end_ordinal=payload["end_ordinal"],
                window_index=payload["window_index"],
                status="ready",
                prompt_version=prompt_version,
                labels=[],
                decision={},
                model_hint=model_hint,
            )
            lease_owner = generate_ulid()
            claimed = store.claim_judge_run(
                window_id=payload["window_id"],
                prompt_version=prompt_version,
                lease_owner=lease_owner,
                lease_expires_at=_lease_expires_at(),
            )
            if claimed is None:
                skipped_windows += 1
                continue

            stop_heartbeat = threading.Event()
            heartbeat_thread = threading.Thread(
                target=_heartbeat_loop,
                args=(store, claimed["judge_run_key"], lease_owner, stop_heartbeat),
                daemon=True,
            )
            heartbeat_thread.start()
            try:
                decision = backend.review_window(
                    payload,
                    prompt_version=prompt_version,
                    model_hint=model_hint,
                ).as_dict()
                stop_heartbeat.set()
                heartbeat_thread.join(timeout=HEARTBEAT_SECONDS)
                labels = decision.get("labels", [])
                store.upsert_judge_run(
                    judge_run_key=claimed["judge_run_key"],
                    partition_key=partition_key,
                    window_id=payload["window_id"],
                    start_ordinal=payload["start_ordinal"],
                    end_ordinal=payload["end_ordinal"],
                    window_index=payload["window_index"],
                    status="judged",
                    prompt_version=prompt_version,
                    labels=labels,
                    decision=decision,
                    model_hint=model_hint,
                )
                store.release_judge_run(judge_run_key=claimed["judge_run_key"], lease_owner=lease_owner)
                judged_windows += 1

                for label_entry in labels:
                    if float(label_entry.get("score") or 0.0) < CANDIDATE_SCORE_THRESHOLD:
                        continue
                    upserted_candidates += _upsert_candidate(
                        store=store,
                        window_payload=payload,
                        judge_run_key=claimed["judge_run_key"],
                        label=label_entry["label"],
                        score=float(label_entry.get("score") or 0.0),
                        reasons=list(label_entry.get("reasons") or []),
                        payload={"window": payload, "decision": decision},
                    )

                if decision.get("should_emit_thin_session"):
                    upserted_candidates += _upsert_candidate(
                        store=store,
                        window_payload=payload,
                        judge_run_key=claimed["judge_run_key"],
                        label="session_thin",
                        score=1.0,
                        reasons=["carry_chain_terminal", "no_strong_labels"],
                        payload={"window": payload, "decision": decision},
                    )
            except Exception as exc:
                stop_heartbeat.set()
                heartbeat_thread.join(timeout=HEARTBEAT_SECONDS)
                failed_windows += 1
                store.upsert_judge_run(
                    judge_run_key=claimed["judge_run_key"],
                    partition_key=partition_key,
                    window_id=payload["window_id"],
                    start_ordinal=payload["start_ordinal"],
                    end_ordinal=payload["end_ordinal"],
                    window_index=payload["window_index"],
                    status="failed",
                    prompt_version=prompt_version,
                    labels=[],
                    decision={"error": str(exc)},
                    model_hint=model_hint,
                )
                store.release_judge_run(judge_run_key=claimed["judge_run_key"], lease_owner=lease_owner)
                store.put_runtime_observation(
                    key=f"judge_failure:{payload['window_id']}",
                    severity="warning",
                    message="judge review-candidates failed",
                    details={"window_id": payload["window_id"], "error": str(exc)},
                )

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
    store.upsert_promotion_candidate(
        candidate_key=_candidate_key(window_payload["window_id"], label),
        window_id=window_payload["window_id"],
        judge_run_key=judge_run_key,
        label=label,
        status="pending_review",
        score=score,
        slice_fingerprint=window_payload["window_id"],
        reasons=reasons,
        payload=payload,
    )
    return 1


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
