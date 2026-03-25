from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

import yaml

from kb_mcp.config import load_config
from kb_mcp.events.judge_inputs import build_window_payload, build_windows, detect_topic_shift
from kb_mcp.events.normalize import normalize_event
from kb_mcp.events.schema import schema_locked_connection
from kb_mcp.events.store import EventStore


class JudgeInputsTest(unittest.TestCase):
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
        for subdir in ["projects/demo/session-log", "projects/demo/draft", "projects/demo/adr", "projects/demo/gap", "projects/demo/knowledge", "general/knowledge", "general/requirements", "inbox"]:
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

    def test_detect_topic_shift_uses_transition_phrases(self) -> None:
        self.assertTrue(detect_topic_shift("でも方針を変える", ""))
        self.assertTrue(detect_topic_shift("", "やっぱり別案にする"))
        self.assertTrue(detect_topic_shift("比較を終えた。じゃあ実装する", ""))
        self.assertTrue(detect_topic_shift("うーん、でも別案にする", ""))
        self.assertFalse(detect_topic_shift("最初の相談", "前提を確認した"))

    def test_build_windows_splits_on_topic_shift_and_idle_gap(self) -> None:
        store = EventStore()
        base = {
            "project": self.project,
            "cwd": str(self.vault),
            "session_id": "session-window",
            "transcript_path": str(self.vault / "session.jsonl"),
        }
        payloads = [
            {**base, "summary": "最初の相談", "content": "前提を確認する", "occurred_at": "2026-03-25T00:00:00+00:00"},
            {**base, "summary": "実装案を詰める", "content": "詳細を確認する", "occurred_at": "2026-03-25T00:05:00+00:00"},
            {**base, "summary": "でも別件を見る", "content": "topic shift", "occurred_at": "2026-03-25T00:06:00+00:00"},
            {**base, "summary": "しばらく後の再開", "content": "idle gap", "occurred_at": "2026-03-25T00:40:01+00:00"},
        ]
        for payload in payloads:
            store.append(
                normalize_event(
                    tool="codex",
                    client="codex-cli",
                    layer="client_hook",
                    event="turn_checkpointed",
                    payload=payload,
                )
            )
        partition_key = normalize_event(
            tool="codex",
            client="codex-cli",
            layer="client_hook",
            event="turn_checkpointed",
            payload=base,
        ).aggregate_state["checkpoint_partition_key"]
        windows = build_windows(partition_key)
        self.assertEqual([(window.start_ordinal, window.end_ordinal) for window in windows], [(1, 2), (3, 3), (4, 4)])

    def test_build_window_payload_extracts_knowledge_signals_from_tool_events(self) -> None:
        store = EventStore()
        base = {
            "project": self.project,
            "cwd": str(self.vault),
            "session_id": "session-knowledge",
            "transcript_path": str(self.vault / "session.jsonl"),
        }
        store.append(
            normalize_event(
                tool="codex",
                client="codex-cli",
                layer="client_hook",
                event="turn_checkpointed",
                payload={**base, "summary": "原因は設定不足や", "content": "doctor で制約を確認できた", "occurred_at": "2026-03-25T00:00:00+00:00"},
            )
        )

        store.append(
            normalize_event(
                tool="kb",
                client="kb-mcp",
                layer="server_middleware",
                event="tool_started",
                payload={
                    "project": self.project,
                    "cwd": str(self.vault),
                    "tool_name": "doctor",
                    "tool_call_id": "tool-1",
                    "occurred_at": "2026-03-24T23:59:40+00:00",
                    "session_id": "session-knowledge",
                },
            )
        )
        store.append(
            normalize_event(
                tool="kb",
                client="kb-mcp",
                layer="server_middleware",
                event="tool_succeeded",
                payload={
                    "project": self.project,
                    "cwd": str(self.vault),
                    "tool_name": "doctor",
                    "tool_call_id": "tool-1",
                    "occurred_at": "2026-03-24T23:59:50+00:00",
                    "session_id": "session-knowledge",
                },
            )
        )

        partition_key = normalize_event(
            tool="codex",
            client="codex-cli",
            layer="client_hook",
            event="turn_checkpointed",
            payload=base,
        ).aggregate_state["checkpoint_partition_key"]
        windows = build_windows(partition_key)
        payload = build_window_payload(windows[0])
        self.assertTrue(payload["knowledge_signals"]["constraint_confirmed"])
        self.assertTrue(payload["knowledge_signals"]["fact_confirmed"])

    def test_build_window_payload_ignores_stale_tool_events_for_first_window(self) -> None:
        store = EventStore()
        base = {
            "project": self.project,
            "cwd": str(self.vault),
            "session_id": "session-stale",
            "transcript_path": str(self.vault / "session-stale.jsonl"),
        }
        store.append(
            normalize_event(
                tool="codex",
                client="codex-cli",
                layer="client_hook",
                event="turn_checkpointed",
                payload={**base, "summary": "確認を始める", "content": "doctor で制約を確認する", "occurred_at": "2026-03-25T01:00:00+00:00"},
            )
        )
        store.append(
            normalize_event(
                tool="kb",
                client="kb-mcp",
                layer="server_middleware",
                event="tool_succeeded",
                payload={
                    "project": self.project,
                    "cwd": str(self.vault),
                    "repo": "demo/repo",
                    "tool_name": "doctor",
                    "tool_call_id": "tool-stale",
                    "occurred_at": "2026-03-24T23:00:00+00:00",
                    "session_id": "session-stale",
                },
            )
        )
        partition_key = normalize_event(
            tool="codex",
            client="codex-cli",
            layer="client_hook",
            event="turn_checkpointed",
            payload=base,
        ).aggregate_state["checkpoint_partition_key"]
        windows = build_windows(partition_key)
        payload = build_window_payload(windows[0])
        self.assertFalse(payload["knowledge_signals"]["constraint_confirmed"])

    def test_build_windows_marks_carry_forward_when_limit_reached_without_anchor(self) -> None:
        store = EventStore()
        base = {
            "project": self.project,
            "cwd": str(self.vault),
            "session_id": "session-carry",
        }
        for idx in range(10):
            store.append(
                normalize_event(
                    tool="codex",
                    client="codex-cli",
                    layer="client_hook",
                    event="turn_checkpointed",
                    payload={
                        **base,
                        "summary": f"checkpoint {idx}",
                        "content": "相談を続ける",
                        "occurred_at": f"2026-03-25T00:{idx:02d}:00+00:00",
                    },
                )
            )
        partition_key = normalize_event(
            tool="codex",
            client="codex-cli",
            layer="client_hook",
            event="turn_checkpointed",
            payload=base,
        ).aggregate_state["checkpoint_partition_key"]
        windows = build_windows(partition_key)
        self.assertEqual(len(windows), 1)
        self.assertTrue(windows[0].carry_forward)
        self.assertEqual(windows[0].carry_chain_index, 1)

    def test_build_windows_caps_carry_chain_at_three_windows(self) -> None:
        store = EventStore()
        base = {
            "project": self.project,
            "cwd": str(self.vault),
            "session_id": "session-long",
        }
        for idx in range(30):
            store.append(
                normalize_event(
                    tool="codex",
                    client="codex-cli",
                    layer="client_hook",
                    event="turn_checkpointed",
                    payload={
                        **base,
                        "summary": f"checkpoint {idx}",
                        "content": "相談を続ける",
                        "occurred_at": f"2026-03-25T00:{idx:02d}:00+00:00",
                    },
                )
            )
        partition_key = normalize_event(
            tool="codex",
            client="codex-cli",
            layer="client_hook",
            event="turn_checkpointed",
            payload=base,
        ).aggregate_state["checkpoint_partition_key"]
        windows = build_windows(partition_key)
        self.assertEqual(len(windows), 3)
        self.assertEqual([window.carry_chain_index for window in windows], [1, 2, 3])
        self.assertEqual([window.carry_forward for window in windows], [True, True, False])
        self.assertEqual([window.carry_chain_terminal for window in windows], [False, False, True])

    def test_anchor_checkpoint_stays_in_current_window(self) -> None:
        store = EventStore()
        base = {
            "project": self.project,
            "cwd": str(self.vault),
            "session_id": "session-anchor",
            "transcript_path": str(self.vault / "anchor.jsonl"),
        }
        for payload in [
            {**base, "summary": "比較する", "content": "案Aと案Bを比べる", "occurred_at": "2026-03-25T00:00:00+00:00"},
            {**base, "summary": "これでいこう", "content": "案Bを採用する", "occurred_at": "2026-03-25T00:01:00+00:00"},
            {**base, "summary": "次の話題", "content": "別件に移る", "occurred_at": "2026-03-25T00:02:00+00:00"},
        ]:
            store.append(
                normalize_event(
                    tool="codex",
                    client="codex-cli",
                    layer="client_hook",
                    event="turn_checkpointed",
                    payload=payload,
                )
            )
        partition_key = normalize_event(
            tool="codex",
            client="codex-cli",
            layer="client_hook",
            event="turn_checkpointed",
            payload=base,
        ).aggregate_state["checkpoint_partition_key"]
        windows = build_windows(partition_key)
        self.assertEqual([(window.start_ordinal, window.end_ordinal) for window in windows], [(1, 2), (3, 3)])
        payload = build_window_payload(windows[0])
        self.assertEqual(payload["anchor_matches"], ["adr"])
        self.assertEqual(payload["checkpoints"][-1]["anchor_labels"], ["adr"])

    def test_build_windows_uses_same_rules_for_all_clients(self) -> None:
        for tool, client in [
            ("claude", "claude-code"),
            ("copilot", "copilot-cli"),
            ("codex", "codex-cli"),
        ]:
            with self.subTest(tool=tool):
                store = EventStore()
                base = {
                    "project": self.project,
                    "cwd": str(self.vault),
                    "session_id": f"session-{tool}",
                    "transcript_path": str(self.vault / f"{tool}.jsonl"),
                }
                for payload in [
                    {**base, "summary": "相談を始める", "content": "前提を整理する", "occurred_at": "2026-03-25T00:00:00+00:00"},
                    {**base, "summary": "でも方針を変える", "content": "別案に切り替える", "occurred_at": "2026-03-25T00:01:00+00:00"},
                ]:
                    store.append(
                        normalize_event(
                            tool=tool,
                            client=client,
                            layer="client_hook",
                            event="turn_checkpointed",
                            payload=payload,
                        )
                    )
                partition_key = normalize_event(
                    tool=tool,
                    client=client,
                    layer="client_hook",
                    event="turn_checkpointed",
                    payload=base,
                ).aggregate_state["checkpoint_partition_key"]
                windows = build_windows(partition_key)
                self.assertEqual([(window.start_ordinal, window.end_ordinal) for window in windows], [(1, 1), (2, 2)])

    def test_build_windows_splits_on_ordinal_gap_and_records_observation(self) -> None:
        store = EventStore()
        base = {
            "project": self.project,
            "cwd": str(self.vault),
            "session_id": "session-gap",
        }
        for ordinal in [1, 2, 4]:
            store.append(
                normalize_event(
                    tool="codex",
                    client="codex-cli",
                    layer="client_hook",
                    event="turn_checkpointed",
                    payload={
                        **base,
                        "checkpoint_ordinal": ordinal,
                        "summary": f"checkpoint {ordinal}",
                        "content": "相談を続ける",
                        "occurred_at": f"2026-03-25T00:0{ordinal}:00+00:00",
                    },
                )
            )
        partition_key = normalize_event(
            tool="codex",
            client="codex-cli",
            layer="client_hook",
            event="turn_checkpointed",
            payload=base,
        ).aggregate_state["checkpoint_partition_key"]
        windows = build_windows(partition_key)
        self.assertEqual([(window.start_ordinal, window.end_ordinal) for window in windows], [(1, 2), (4, 4)])
        with schema_locked_connection() as conn:
            row = conn.execute(
                """
                SELECT observation_key
                FROM runtime_observations
                WHERE observation_key LIKE 'judge_inputs:ordinal_gap:%'
                """
            ).fetchone()
        self.assertIsNotNone(row)

    def test_load_partition_checkpoints_keeps_first_timestamp_and_latest_summary(self) -> None:
        store = EventStore()
        base = {
            "project": self.project,
            "cwd": str(self.vault),
            "session_id": "session-replay",
            "transcript_path": str(self.vault / "replay.jsonl"),
            "checkpoint_ordinal": 1,
        }
        store.append(
            normalize_event(
                tool="codex",
                client="codex-cli",
                layer="client_hook",
                event="turn_checkpointed",
                payload={**base, "summary": "最初の要約", "content": "最初の本文", "occurred_at": "2026-03-25T00:00:00+00:00"},
            )
        )
        store.append(
            normalize_event(
                tool="codex",
                client="codex-cli",
                layer="client_hook",
                event="turn_checkpointed",
                payload={**base, "summary": "更新後の要約", "content": "更新後の本文", "occurred_at": "2026-03-25T00:10:00+00:00"},
            )
        )
        partition_key = normalize_event(
            tool="codex",
            client="codex-cli",
            layer="client_hook",
            event="turn_checkpointed",
            payload=base,
        ).aggregate_state["checkpoint_partition_key"]
        windows = build_windows(partition_key)
        payload = build_window_payload(windows[0])
        self.assertEqual(payload["checkpoints"][0]["occurred_at"], "2026-03-25T00:00:00+00:00")
        self.assertEqual(payload["checkpoints"][0]["summary"], "更新後の要約")

    def test_previous_checkpoint_lower_bound_uses_first_event_timestamp(self) -> None:
        store = EventStore()
        base = {
            "project": self.project,
            "cwd": str(self.vault),
            "session_id": "session-lower-bound",
            "transcript_path": str(self.vault / "lower-bound.jsonl"),
        }
        store.append(
            normalize_event(
                tool="codex",
                client="codex-cli",
                layer="client_hook",
                event="turn_checkpointed",
                payload={**base, "checkpoint_ordinal": 1, "summary": "最初", "content": "最初の本文", "occurred_at": "2026-03-25T00:00:00+00:00"},
            )
        )
        store.append(
            normalize_event(
                tool="codex",
                client="codex-cli",
                layer="client_hook",
                event="turn_checkpointed",
                payload={**base, "checkpoint_ordinal": 1, "summary": "最初の再送", "content": "再送後の本文", "occurred_at": "2026-03-25T00:10:00+00:00"},
            )
        )
        store.append(
            normalize_event(
                tool="kb",
                client="kb-mcp",
                layer="server_middleware",
                event="tool_succeeded",
                payload={
                    "project": self.project,
                    "cwd": str(self.vault),
                    "tool_name": "doctor",
                    "tool_call_id": "tool-boundary",
                    "occurred_at": "2026-03-25T00:05:00+00:00",
                    "session_id": "session-lower-bound",
                },
            )
        )
        store.append(
            normalize_event(
                tool="codex",
                client="codex-cli",
                layer="client_hook",
                event="turn_checkpointed",
                payload={**base, "checkpoint_ordinal": 2, "summary": "次", "content": "doctor で制約を確認できた", "occurred_at": "2026-03-25T00:20:00+00:00"},
            )
        )
        partition_key = normalize_event(
            tool="codex",
            client="codex-cli",
            layer="client_hook",
            event="turn_checkpointed",
            payload=base,
        ).aggregate_state["checkpoint_partition_key"]
        windows = build_windows(partition_key)
        payload = build_window_payload(windows[-1])
        self.assertTrue(payload["knowledge_signals"]["constraint_confirmed"])

    def test_build_window_payload_skips_tool_event_match_without_session_id(self) -> None:
        store = EventStore()
        checkpoint = {
            "project": self.project,
            "cwd": str(self.vault),
            "summary": "制約を確認したい",
            "content": "doctor で制約を確認できた",
            "occurred_at": "2026-03-25T00:00:00+00:00",
        }
        store.append(
            normalize_event(
                tool="codex",
                client="codex-cli",
                layer="client_hook",
                event="turn_checkpointed",
                payload=checkpoint,
            )
        )
        store.append(
            normalize_event(
                tool="kb",
                client="kb-mcp",
                layer="server_middleware",
                event="tool_succeeded",
                payload={
                    "project": self.project,
                    "cwd": str(self.vault),
                    "tool_name": "doctor",
                    "tool_call_id": "tool-orphan",
                    "occurred_at": "2026-03-24T23:59:50+00:00",
                    "session_id": "different-session",
                },
            )
        )
        partition_key = normalize_event(
            tool="codex",
            client="codex-cli",
            layer="client_hook",
            event="turn_checkpointed",
            payload=checkpoint,
        ).aggregate_state["checkpoint_partition_key"]
        windows = build_windows(partition_key)
        payload = build_window_payload(windows[0])
        self.assertFalse(payload["knowledge_signals"]["constraint_confirmed"])

    def test_build_window_payload_uses_standalone_tool_events_when_scope_matches(self) -> None:
        store = EventStore()
        checkpoint = {
            "project": self.project,
            "repo": "demo/repo",
            "cwd": str(self.vault),
            "summary": "制約を確認した",
            "content": "doctor で制約を確認できた",
            "occurred_at": "2026-03-25T00:00:00+00:00",
        }
        store.append(
            normalize_event(
                tool="copilot",
                client="copilot-cli",
                layer="client_hook",
                event="turn_checkpointed",
                payload=checkpoint,
            )
        )
        store.append(
            normalize_event(
                tool="kb",
                client="kb-mcp",
                layer="server_middleware",
                event="tool_succeeded",
                payload={
                    "project": self.project,
                    "repo": "demo/repo",
                    "cwd": str(self.vault),
                    "tool_name": "doctor",
                    "tool_call_id": "tool-standalone",
                    "occurred_at": "2026-03-24T23:59:50+00:00",
                },
            )
        )
        partition_key = normalize_event(
            tool="copilot",
            client="copilot-cli",
            layer="client_hook",
            event="turn_checkpointed",
            payload=checkpoint,
        ).aggregate_state["checkpoint_partition_key"]
        windows = build_windows(partition_key)
        payload = build_window_payload(windows[0])
        self.assertTrue(payload["knowledge_signals"]["constraint_confirmed"])

    def test_build_window_payload_skips_standalone_fallback_when_transcript_exists(self) -> None:
        store = EventStore()
        checkpoint = {
            "project": self.project,
            "repo": "demo/repo",
            "cwd": str(self.vault),
            "transcript_path": str(self.vault / "standalone.jsonl"),
            "summary": "制約を確認した",
            "content": "doctor で制約を確認できた",
            "occurred_at": "2026-03-25T00:00:00+00:00",
        }
        store.append(
            normalize_event(
                tool="copilot",
                client="copilot-cli",
                layer="client_hook",
                event="turn_checkpointed",
                payload=checkpoint,
            )
        )
        store.append(
            normalize_event(
                tool="kb",
                client="kb-mcp",
                layer="server_middleware",
                event="tool_succeeded",
                payload={
                    "project": self.project,
                    "repo": "demo/repo",
                    "cwd": str(self.vault),
                    "tool_name": "doctor",
                    "tool_call_id": "tool-standalone-transcript",
                    "occurred_at": "2026-03-24T23:59:50+00:00",
                },
            )
        )
        partition_key = normalize_event(
            tool="copilot",
            client="copilot-cli",
            layer="client_hook",
            event="turn_checkpointed",
            payload=checkpoint,
        ).aggregate_state["checkpoint_partition_key"]
        windows = build_windows(partition_key)
        payload = build_window_payload(windows[0])
        self.assertFalse(payload["knowledge_signals"]["constraint_confirmed"])
