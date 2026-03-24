from __future__ import annotations

import os
import stat
import tempfile
import unittest
from pathlib import Path

import yaml

from kb_mcp.config import load_config
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
        store.append(normalize_event(tool="codex", client="codex-cli", layer="client_hook", event="session_started", payload=payload))
        store.append(normalize_event(tool="codex", client="codex-cli", layer="client_hook", event="session_ended", payload=payload))
        result = run_once(maintenance=True)
        self.assertGreaterEqual(result["applied"], 1)
        files = sorted((self.vault / "projects" / self.project / "session-log").glob("*.md"))
        self.assertEqual(len(files), 1)
        mode = files[0].stat().st_mode
        self.assertFalse(mode & stat.S_IWUSR)

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
            event="session_ended",
            payload={
                "session_id": "session-3",
                "summary": "short summary",
                "transcript_path": str(transcript),
            },
        )
        self.assertIn("important transcript line", envelope.content_excerpt or "")
