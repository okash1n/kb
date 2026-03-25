"""Materialize planned promotions into notes."""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

from kb_mcp.config import projects_dir, runtime_events_dir, safe_resolve
from kb_mcp.events.identity import sink_receipt
from kb_mcp.events.request_context import REQUEST_CONTEXT
from kb_mcp.events.store import EventStore
from kb_mcp.note import build_filename, build_session_filename, generate_ulid, parse_frontmatter, slugify
from kb_mcp.resolver import resolve_project
from kb_mcp.tools.save import kb_session, save_note_by_type

HEARTBEAT_SECONDS = 60


def apply_promotion(row: sqlite3.Row) -> str:
    """Create a note from a persisted promotion plan."""
    if row["aggregate_type"] == "review_materialization":
        return _apply_materialization(row)
    return _apply_session_promotion(row)


def _apply_session_promotion(row: sqlite3.Row) -> str:
    receipt = sink_receipt("promotion_applier", row["logical_key"], int(row["aggregate_version"]))
    if _receipt_exists(row["project"], receipt):
        return receipt

    plan = _load_plan(row)
    context: dict[str, str] = {}
    token = REQUEST_CONTEXT.set(context)
    try:
        record = _load_record(str(plan["promotion_key"]))
        extra_fields = {
            "density": str(plan["density"]),
            "promotion_key": str(plan["promotion_key"]),
            "promotion_version": str(int(record.get("promotion_version", 0)) + 1),
            "sink_receipt": receipt,
        }
        previous_id = record.get("note_id")
        if previous_id:
            extra_fields["supersedes"] = str(previous_id)
        kb_session(
            summary=str(plan["summary"])[:200],
            content=str(plan["content"]),
            ai_tool=str(plan["ai_tool"]),
            ai_client=str(plan["ai_client"]) if plan.get("ai_client") else None,
            project=str(plan["project"]),
            cwd=str(plan["cwd"]) if plan.get("cwd") else None,
            repo=str(plan["repo"]) if plan.get("repo") else None,
            tags=list(plan.get("tags") or []),
            related=list(plan.get("related") or []),
            extra_fields=extra_fields,
        )
        _write_record(
            str(plan["promotion_key"]),
            {
                "logical_key": row["logical_key"],
                "aggregate_version": int(row["aggregate_version"]),
                "note_id": context.get("saved_note_id"),
                "note_path": context.get("saved_note_path"),
                "promotion_version": extra_fields["promotion_version"],
                "density": plan["density"],
            },
        )
        return receipt
    finally:
        REQUEST_CONTEXT.reset(token)


def _apply_materialization(row: sqlite3.Row) -> str:
    receipt = sink_receipt("promotion_applier", row["logical_key"], int(row["aggregate_version"]))
    plan = _load_plan(row)
    store = EventStore()
    materialization_key = str(plan["materialization_key"])
    record = store.claim_materialization_record(
        materialization_key=materialization_key,
        lease_owner=receipt,
        lease_expires_at=_lease_expires_at(),
    )
    if record is None:
        existing = store.get_materialization_record(materialization_key)
        if existing is not None and existing["status"] == "applied":
            return receipt
        raise ValueError(f"materialization record is not claimable: {materialization_key}")

    stop_heartbeat = threading.Event()
    lost_lease = threading.Event()
    heartbeat_thread = threading.Thread(
        target=_heartbeat_loop,
        args=(store, materialization_key, receipt, int(record["lease_epoch"]), stop_heartbeat, lost_lease),
        daemon=True,
    )
    heartbeat_thread.start()
    try:
        adopted = _adopt_existing_note(plan)
        if adopted is not None:
            _ensure_lease_held(lost_lease)
            _record_materialized_note(store, record, plan, adopted["note_id"], adopted["note_path"], receipt)
            return receipt

        context: dict[str, str] = {}
        token = REQUEST_CONTEXT.set(context)
        try:
            target = _materialization_target(store, record, plan, receipt)
            save_note_by_type(
                note_type=str(plan["note_type"]),
                slug=str(plan["slug"]),
                summary=str(plan["summary"]),
                content=str(plan["content"]),
                ai_tool=str(plan["ai_tool"]),
                ai_client=str(plan["ai_client"]) if plan.get("ai_client") else None,
                project=str(plan["project"]) if plan.get("project") else None,
                cwd=str(plan["cwd"]) if plan.get("cwd") else None,
                repo=str(plan["repo"]) if plan.get("repo") else None,
                tags=list(plan.get("tags") or []),
                related=list(plan.get("related") or []),
                status=str(plan["status"]) if plan.get("status") else None,
                extra_fields={
                    "materialization_key": materialization_key,
                    "candidate_key": str(plan["candidate_key"]),
                    "judge_run_key": str(record["judge_run_key"]),
                    "sink_receipt": receipt,
                },
                fixed_ulid=target["note_id"],
                fixed_filename=target["filename"],
            )
            _ensure_lease_held(lost_lease)
            _record_materialized_note(
                store,
                record,
                plan,
                context.get("saved_note_id"),
                context.get("saved_note_path"),
                receipt,
            )
            return receipt
        finally:
            REQUEST_CONTEXT.reset(token)
    except Exception:
        released = store.release_materialization_record(
            materialization_key=materialization_key,
            lease_owner=receipt,
            lease_epoch=int(record["lease_epoch"]),
            status="repair_pending",
        )
        if not released:
            store.mark_materialization_repair_pending(
                materialization_key=materialization_key,
                expected_lease_epoch=int(record["lease_epoch"]),
                last_error="lease lost before repair_pending release",
            )
        raise
    finally:
        stop_heartbeat.set()
        heartbeat_thread.join(timeout=HEARTBEAT_SECONDS)


def _record_materialized_note(
    store: EventStore,
    record: sqlite3.Row,
    plan: dict[str, object],
    note_id: str | None,
    note_path: str | None,
    lease_owner: str,
) -> None:
    finalized = store.finalize_materialization_record(
        materialization_key=str(plan["materialization_key"]),
        lease_owner=lease_owner,
        lease_epoch=int(record["lease_epoch"]),
        candidate_key=str(plan["candidate_key"]),
        note_id=note_id,
        note_path=note_path,
        promotion_key=str(plan["promotion_key"]),
    )
    if not finalized:
        raise RuntimeError("materialization lease was lost before finalize")


def _load_plan(row: sqlite3.Row) -> dict[str, object]:
    safe_name = row["logical_key"].replace(":", "__")
    prefix = "materialize__" if row["aggregate_type"] == "review_materialization" else "session__"
    path = runtime_events_dir() / "promotions" / f"{prefix}{safe_name}__v{int(row['aggregate_version'])}.json"
    if not path.exists():
        legacy_path = runtime_events_dir() / "promotions" / f"{safe_name}.json"
        if legacy_path.exists():
            path = legacy_path
    return json.loads(path.read_text(encoding="utf-8"))


def _records_dir() -> Path:
    path = runtime_events_dir() / "promotion-records"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _record_path(promotion_key: str) -> Path:
    safe_name = promotion_key.replace(":", "__")
    return _records_dir() / f"{safe_name}.json"


def _load_record(promotion_key: str) -> dict[str, object]:
    path = _record_path(promotion_key)
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _write_record(promotion_key: str, payload: dict[str, object]) -> None:
    _record_path(promotion_key).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def _receipt_exists(project: str | None, receipt: str) -> bool:
    if not project:
        return False
    directory = safe_resolve(projects_dir(), project, "session-log")
    for path in directory.glob("*.md"):
        try:
            frontmatter = parse_frontmatter(path.read_text(encoding="utf-8")) or {}
        except OSError:
            continue
        if frontmatter.get("sink_receipt") == receipt:
            return True
    return False


def _adopt_existing_note(plan: dict[str, object]) -> dict[str, str] | None:
    adopted = _find_note_by_materialization_key(
        project=str(plan["project"]) if plan.get("project") else None,
        note_type=str(plan["note_type"]),
        materialization_key=str(plan["materialization_key"]),
    )
    if adopted is not None:
        return adopted
    if str(plan["note_type"]) != "session-log":
        return None
    record = _load_record(str(plan["promotion_key"]))
    note_path = record.get("note_path")
    note_id = record.get("note_id")
    if not note_path or not note_id:
        return None
    path = Path(str(note_path))
    if not path.exists():
        return None
    return {"note_id": str(note_id), "note_path": str(path)}


def _find_note_by_materialization_key(
    *,
    project: str | None,
    note_type: str,
    materialization_key: str,
) -> dict[str, str] | None:
    if not project:
        return None
    subdir = _note_subdir(note_type)
    directory = safe_resolve(projects_dir(), project, subdir)
    for path in directory.glob("*.md"):
        try:
            frontmatter = parse_frontmatter(path.read_text(encoding="utf-8")) or {}
        except OSError:
            continue
        if frontmatter.get("materialization_key") == materialization_key and frontmatter.get("id"):
            return {"note_id": str(frontmatter["id"]), "note_path": str(path)}
    return None


def _materialization_target(
    store: EventStore,
    record: sqlite3.Row,
    plan: dict[str, object],
    lease_owner: str,
) -> dict[str, str]:
    note_id = str(record["note_id"]) if record["note_id"] else None
    note_path = str(record["note_path"]) if record["note_path"] else None
    if note_id and note_path:
        return {"note_id": note_id, "filename": Path(note_path).name, "note_path": note_path}

    reserved_id = generate_ulid()
    reserved_filename = _reserved_filename(str(plan["note_type"]), str(plan["slug"]), reserved_id)
    resolved_project, _ = resolve_project(
        project=str(plan["project"]) if plan.get("project") else None,
        cwd=str(plan["cwd"]) if plan.get("cwd") else None,
        repo=str(plan["repo"]) if plan.get("repo") else None,
    )
    if not resolved_project:
        raise ValueError("could not resolve project for materialization note target")
    reserved_path = str(_note_directory(resolved_project, str(plan["note_type"])) / reserved_filename)
    updated = store.reserve_materialization_note_target(
        materialization_key=str(plan["materialization_key"]),
        lease_owner=lease_owner,
        lease_epoch=int(record["lease_epoch"]),
        note_id=reserved_id,
        note_path=reserved_path,
    )
    if updated is None:
        raise RuntimeError("materialization lease was lost before reserving note target")
    return {
        "note_id": str(updated["note_id"]),
        "filename": Path(str(updated["note_path"])).name,
        "note_path": str(updated["note_path"]),
    }


def _note_subdir(note_type: str) -> str:
    mapping = {
        "adr": "adr",
        "gap": "gap",
        "knowledge": "knowledge",
        "session-log": "session-log",
    }
    return mapping[note_type]


def _note_directory(project: str, note_type: str) -> Path:
    return safe_resolve(projects_dir(), project, _note_subdir(note_type))


def _reserved_filename(note_type: str, slug: str, note_id: str) -> str:
    if note_type == "session-log":
        return build_session_filename(ulid=note_id)
    return build_filename(slug=slugify(slug), ulid=note_id)


def _lease_expires_at() -> str:
    return (
        datetime.now(timezone.utc) + timedelta(minutes=5)
    ).isoformat(timespec="seconds")


def _heartbeat_loop(
    store: EventStore,
    materialization_key: str,
    lease_owner: str,
    lease_epoch: int,
    stop_event: threading.Event,
    lost_lease: threading.Event,
) -> None:
    while not stop_event.wait(HEARTBEAT_SECONDS):
        try:
            refreshed = store.heartbeat_materialization_record(
                materialization_key=materialization_key,
                lease_owner=lease_owner,
                lease_epoch=lease_epoch,
                lease_expires_at=_lease_expires_at(),
            )
        except Exception:
            lost_lease.set()
            return
        if not refreshed:
            lost_lease.set()
            return


def _ensure_lease_held(lost_lease: threading.Event) -> None:
    if lost_lease.is_set():
        raise RuntimeError("materialization lease was lost during apply")
