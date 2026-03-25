from __future__ import annotations

import os
import stat
import tempfile
import unittest
import json
from pathlib import Path

import yaml

from kb_mcp.config import load_config
from kb_mcp.events.candidates import detect_candidates
from kb_mcp.events.middleware import with_tool_events
from kb_mcp.events.normalize import normalize_event
from kb_mcp.events.store import EventStore
from kb_mcp.events.worker import run_once


class EventPipelineTest(unittest.TestCase):
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
        for subdir in ["projects/demo/session-log", "projects/demo/draft", "projects/demo/adr", "projects/demo/gap", "projects/demo/knowledge", "projects/demo/draft", "general/knowledge", "general/requirements", "inbox"]:
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

    def test_session_events_finalize_immutable_note(self) -> None:
        store = EventStore()
        payload = {
            "project": self.project,
            "cwd": str(self.vault),
            "session_id": "session-1",
            "summary": "Finished a session",
            "content": "Body from hook payload",
        }
        store.append(normalize_event(tool="codex", client="codex-cli", layer="session_launcher", event="session_started", payload=payload))
        store.append(normalize_event(tool="codex", client="codex-cli", layer="session_launcher", event="process_exit", payload={**payload, "exit_code": 0}))
        store.append(normalize_event(tool="codex", client="codex-cli", layer="session_launcher", event="session_ended", payload=payload))
        result = run_once(maintenance=True)
        self.assertGreaterEqual(result["applied"], 1)
        files = sorted((self.vault / "projects" / self.project / "session-log").glob("*.md"))
        self.assertEqual(len(files), 1)
        mode = files[0].stat().st_mode
        self.assertFalse(mode & stat.S_IWUSR)

    def test_hook_session_end_writes_checkpoint_without_finalizing_note(self) -> None:
        store = EventStore()
        payload = {
            "project": self.project,
            "cwd": str(self.vault),
            "session_id": "session-hook-only",
            "summary": "Turn finished",
            "content": "Short turn excerpt",
        }
        store.append(normalize_event(tool="codex", client="codex-cli", layer="client_hook", event="turn_checkpointed", payload=payload))
        result = run_once(maintenance=True)
        self.assertGreaterEqual(result["applied"], 1)
        files = sorted((self.vault / "projects" / self.project / "session-log").glob("*.md"))
        self.assertEqual(len(files), 0)
        checkpoints = sorted((self.config_dir / "runtime" / "events" / "checkpoints").glob("*.json"))
        self.assertEqual(len(checkpoints), 1)

    def test_checkpoint_events_accumulate_multiple_ordinals(self) -> None:
        store = EventStore()
        base = {
            "project": self.project,
            "cwd": str(self.vault),
            "session_id": "session-many",
            "summary": "Turn finished",
            "content": "Short turn excerpt",
        }
        first = normalize_event(tool="codex", client="codex-cli", layer="client_hook", event="turn_checkpointed", payload=base)
        second = normalize_event(tool="codex", client="codex-cli", layer="client_hook", event="turn_checkpointed", payload=base)
        result1 = store.append(first)
        result2 = store.append(second)
        self.assertNotEqual(result1.logical_key, result2.logical_key)
        self.assertTrue(result1.logical_key.endswith(":1"))
        self.assertTrue(result2.logical_key.endswith(":2"))
        with EventStore().transaction() as conn:
            keys = [
                row["logical_key"]
                for row in conn.execute(
                    "SELECT logical_key FROM events WHERE summary='Turn finished' ORDER BY rowid"
                ).fetchall()
            ]
        self.assertEqual(keys[-2:], [result1.logical_key, result2.logical_key])

    def test_unmanaged_checkpoint_uses_distinct_partition_key(self) -> None:
        one = normalize_event(
            tool="codex",
            client="codex-cli",
            layer="client_hook",
            event="turn_checkpointed",
            payload={"cwd": "/tmp/a", "summary": "one", "occurred_at": "2026-03-25T00:00:00+00:00"},
        )
        two = normalize_event(
            tool="codex",
            client="codex-cli",
            layer="client_hook",
            event="turn_checkpointed",
            payload={"cwd": "/tmp/b", "summary": "two", "occurred_at": "2026-03-25T00:00:00+00:00"},
        )
        self.assertNotEqual(
            one.aggregate_state["checkpoint_partition_key"],
            two.aggregate_state["checkpoint_partition_key"],
        )
        store = EventStore()
        result_one = store.append(one)
        result_two = store.append(two)
        self.assertNotEqual(result_one.logical_key, result_two.logical_key)

    def test_unmanaged_compact_events_do_not_collapse_to_single_key(self) -> None:
        one = normalize_event(
            tool="codex",
            client="codex-cli",
            layer="client_hook",
            event="compact_finished",
            payload={"cwd": "/tmp/a", "summary": "one", "occurred_at": "2026-03-25T00:00:00+00:00"},
        )
        two = normalize_event(
            tool="codex",
            client="codex-cli",
            layer="client_hook",
            event="compact_finished",
            payload={"cwd": "/tmp/b", "summary": "two", "occurred_at": "2026-03-25T00:00:00+00:00"},
        )
        self.assertNotEqual(one.logical_key, two.logical_key)

    def test_gap_candidate_is_embedded_in_checkpoint(self) -> None:
        store = EventStore()
        payload = {
            "project": self.project,
            "cwd": str(self.vault),
            "session_id": "session-gap",
            "summary": "長すぎて読まれへんわ",
            "content": "そうじゃなくて、もっと短く出すべきや",
        }
        store.append(normalize_event(tool="codex", client="codex-cli", layer="client_hook", event="turn_checkpointed", payload=payload))
        run_once(maintenance=True)
        checkpoints = sorted((self.config_dir / "runtime" / "events" / "checkpoints").glob("*.json"))
        data = json.loads(checkpoints[0].read_text(encoding="utf-8"))
        self.assertTrue(data["candidates"]["has_candidates"])
        self.assertEqual(data["candidates"]["items"][0]["kind"], "gap")

    def test_knowledge_candidate_requires_stronger_signal(self) -> None:
        detected = detect_candidates(
            "原因は schema 上の制約や",
            "config.toml で feature が必要やと確認できた",
        )
        self.assertTrue(detected["has_candidates"])
        self.assertEqual(detected["items"][0]["kind"], "knowledge")

    def test_error_event_writes_incident_draft(self) -> None:
        store = EventStore()
        payload = {
            "project": self.project,
            "cwd": str(self.vault),
            "session_id": "session-2",
            "message": "Something went wrong",
            "error_fingerprint": "boom",
        }
        store.append(normalize_event(tool="claude", client="claude-code", layer="client_hook", event="agent_error", payload=payload))
        result = run_once()
        self.assertGreaterEqual(result["applied"], 1)
        files = sorted((self.vault / "projects" / self.project / "draft").glob("*.md"))
        self.assertEqual(len(files), 1)
        self.assertIn("incident", files[0].read_text(encoding="utf-8"))

    def test_tool_wrapper_emits_authoritative_events(self) -> None:
        observed: list[str] = []

        def sample(*, project: str) -> str:
            observed.append(project)
            return project

        wrapped = with_tool_events("kb", "kb-mcp", "sample", sample)
        result = wrapped(project=self.project, ctx=None)
        self.assertEqual(result, self.project)
        self.assertEqual(observed, [self.project])
        logical_rows = []
        with EventStore().transaction() as conn:
            logical_rows = conn.execute(
                "SELECT event_name FROM events WHERE aggregate_type='tool' ORDER BY rowid"
            ).fetchall()
        self.assertEqual([row["event_name"] for row in logical_rows], ["tool_started", "tool_succeeded"])

    def test_transcript_excerpt_wins_over_summary_when_content_missing(self) -> None:
        transcript = self.vault / "transcript.txt"
        transcript.write_text("line1\nline2\nimportant transcript line\n", encoding="utf-8")
        envelope = normalize_event(
            tool="codex",
            client="codex-cli",
            layer="client_hook",
            event="turn_checkpointed",
            payload={
                "session_id": "session-3",
                "summary": "short summary",
                "transcript_path": str(transcript),
            },
        )
        self.assertIn("important transcript line", envelope.content_excerpt or "")

    def test_codex_jsonl_excerpt_extracts_messages_only(self) -> None:
        transcript = self.vault / "codex.jsonl"
        transcript.write_text(
            "\n".join(
                [
                    json.dumps(
                        {
                            "timestamp": "2026-03-25T00:00:00Z",
                            "type": "event_msg",
                            "payload": {"type": "user_message", "message": "最初の質問"},
                        },
                        ensure_ascii=False,
                    ),
                    json.dumps(
                        {
                            "timestamp": "2026-03-25T00:00:01Z",
                            "type": "response_item",
                            "payload": {
                                "type": "function_call_output",
                                "output": '{"status":"ok"}',
                            },
                        },
                        ensure_ascii=False,
                    ),
                    json.dumps(
                        {
                            "timestamp": "2026-03-25T00:00:02Z",
                            "type": "event_msg",
                            "payload": {"type": "agent_message", "message": "途中の返答"},
                        },
                        ensure_ascii=False,
                    ),
                    json.dumps(
                        {
                            "timestamp": "2026-03-25T00:00:03Z",
                            "type": "response_item",
                            "payload": {
                                "type": "message",
                                "role": "assistant",
                                "content": [{"type": "output_text", "text": "最後の返答"}],
                            },
                        },
                        ensure_ascii=False,
                    ),
                ]
            )
            + "\n",
            encoding="utf-8",
        )

        envelope = normalize_event(
            tool="codex",
            client="codex-cli",
            layer="client_hook",
            event="turn_checkpointed",
            payload={
                "session_id": "session-4",
                "summary": "session ended",
                "transcript_path": str(transcript),
            },
        )

        self.assertIn("最初の質問", envelope.content_excerpt or "")
        self.assertIn("途中の返答", envelope.content_excerpt or "")
        self.assertIn("最後の返答", envelope.content_excerpt or "")
        self.assertNotIn("function_call_output", envelope.content_excerpt or "")
