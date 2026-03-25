from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

import yaml

from kb_mcp.config import load_config
from kb_mcp.events.judge_inputs import build_window_payload, build_windows, detect_topic_shift
from kb_mcp.events.normalize import normalize_event
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
