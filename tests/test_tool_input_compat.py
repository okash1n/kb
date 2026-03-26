from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import anyio
import yaml

from kb_mcp.config import load_config
from kb_mcp.server import mcp
from kb_mcp.tools.save import kb_adr, kb_draft, kb_gap, kb_knowledge, kb_session
from kb_mcp.tools.search import kb_search


class ToolInputCompatTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        root = Path(self.tmpdir.name).resolve()
        self.config_dir = root / "config"
        self.vault = root / "vault"
        self.project = "demo"
        self._old_env = os.environ.get("KB_CONFIG_DIR")
        os.environ["KB_CONFIG_DIR"] = str(self.config_dir)
        self.addCleanup(self._restore_env)
        for subdir in [
            "projects/demo/gap",
            "projects/demo/session-log",
            "projects/demo/adr",
            "projects/demo/knowledge",
            "projects/demo/draft",
            "inbox",
        ]:
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

    def _restore_env(self) -> None:
        if self._old_env is None:
            os.environ.pop("KB_CONFIG_DIR", None)
        else:
            os.environ["KB_CONFIG_DIR"] = self._old_env
        load_config.cache_clear()

    def test_gap_derives_slug_from_summary_when_omitted(self) -> None:
        result = kb_gap(
            summary="日本語だけ",
            content="body",
            ai_tool="codex",
            project=self.project,
        )

        self.assertIn("Saved:", result)
        files = sorted((self.vault / "projects" / self.project / "gap").glob("*.md"))
        self.assertEqual(len(files), 1)
        self.assertTrue(files[0].name.startswith("untitled--"))

    def test_save_tools_preserve_legacy_positional_slug_order(self) -> None:
        gap_result = kb_gap("legacy-gap", "summary", "body", "codex", project=self.project)
        adr_result = kb_adr("legacy-adr", "summary", "body", "codex", project=self.project)
        knowledge_result = kb_knowledge("legacy-knowledge", "summary", "body", "codex", project=self.project)
        draft_result = kb_draft("legacy-draft", "summary", "body", "codex", project=self.project)

        self.assertIn("Saved:", gap_result)
        self.assertIn("Saved:", adr_result)
        self.assertIn("Saved:", knowledge_result)
        self.assertIn("Saved:", draft_result)
        gap_files = sorted((self.vault / "projects" / self.project / "gap").glob("*.md"))
        adr_files = sorted((self.vault / "projects" / self.project / "adr").glob("*.md"))
        knowledge_files = sorted((self.vault / "projects" / self.project / "knowledge").glob("*.md"))
        draft_files = sorted((self.vault / "projects" / self.project / "draft").glob("*.md"))
        self.assertTrue(gap_files[0].name.startswith("legacy-gap--"))
        self.assertTrue(adr_files[0].name.startswith("legacy-adr--"))
        self.assertTrue(knowledge_files[0].name.startswith("legacy-knowledge--"))
        self.assertTrue(draft_files[0].name.startswith("legacy-draft--"))

    def test_session_accepts_comma_separated_tags(self) -> None:
        result = kb_session(
            summary="session",
            content="body",
            ai_tool="codex",
            project=self.project,
            tags="alpha, beta, alpha",
        )

        self.assertIn("Saved:", result)
        files = sorted((self.vault / "projects" / self.project / "session-log").glob("*.md"))
        text = files[0].read_text(encoding="utf-8")
        self.assertIn("tags: [alpha, beta]", text)

    def test_save_tools_accept_json_tags_and_string_related(self) -> None:
        kb_knowledge(
            summary="knowledge",
            content="body",
            ai_tool="codex",
            project=self.project,
            tags='["alpha", "beta"]',
            related="01AAA, 01BBB",
        )
        kb_draft(
            summary="draft",
            content="body",
            ai_tool="codex",
            project=self.project,
            tags='["draft-tag"]',
        )

        knowledge_file = next((self.vault / "projects" / self.project / "knowledge").glob("*.md"))
        draft_file = next((self.vault / "projects" / self.project / "draft").glob("*.md"))
        knowledge_text = knowledge_file.read_text(encoding="utf-8")
        draft_text = draft_file.read_text(encoding="utf-8")
        self.assertIn("tags: [alpha, beta]", knowledge_text)
        self.assertIn("related: [01AAA, 01BBB]", knowledge_text)
        self.assertIn("tags: [draft-tag]", draft_text)

    def test_search_accepts_string_tags_and_normalizes_note_tags(self) -> None:
        search_result = json.dumps(
            [
                {"path": "projects/demo/gap/a.md"},
                {"path": "projects/demo/gap/b.md"},
            ]
        )

        async def fake_search(*, query: str, path: str | None, limit: int) -> str:
            self.assertEqual(query, "schema")
            self.assertEqual(path, "projects/demo/gap")
            self.assertEqual(limit, 5)
            return search_result

        async def fake_run(command: str, *args: str) -> str:
            self.assertEqual(command, "properties")
            path_arg = next(arg for arg in args if arg.startswith("path="))
            if path_arg.endswith("a.md"):
                return json.dumps({"tags": "kb, schema"})
            return json.dumps({"tags": ["kb"]})

        with patch("kb_mcp.tools.search.obsidian.search", side_effect=fake_search):
            with patch("kb_mcp.tools.search.obsidian.run", side_effect=fake_run):
                result = anyio.run(
                    kb_search,
                    "schema",
                    self.project,
                    "kb, schema",
                    "gap",
                    5,
                )

        data = json.loads(result)
        self.assertEqual(data, [{"path": "projects/demo/gap/a.md"}])

    def test_mcp_schema_marks_gap_slug_optional_and_accepts_string_tags(self) -> None:
        async def collect_schema() -> dict[str, dict]:
            tools = await mcp.list_tools()
            return {tool.name: tool.inputSchema for tool in tools}

        schema = anyio.run(collect_schema)
        gap_schema = schema["gap"]
        adr_schema = schema["adr"]
        knowledge_schema = schema["knowledge"]
        draft_schema = schema["draft"]
        session_schema = schema["session"]
        search_schema = schema["search"]

        self.assertNotIn("slug", gap_schema["required"])
        self.assertNotIn("slug", adr_schema["required"])
        self.assertNotIn("slug", knowledge_schema["required"])
        self.assertNotIn("slug", draft_schema["required"])
        self.assertEqual(gap_schema["properties"]["summary"]["type"], "string")
        self.assertEqual(
            gap_schema["properties"]["tags"]["anyOf"],
            [
                {"items": {"type": "string"}, "type": "array"},
                {"type": "string"},
                {"type": "null"},
            ],
        )
        self.assertEqual(
            session_schema["properties"]["related"]["anyOf"],
            [
                {"items": {"type": "string"}, "type": "array"},
                {"type": "string"},
                {"type": "null"},
            ],
        )
        self.assertEqual(
            search_schema["properties"]["tags"]["anyOf"],
            [
                {"items": {"type": "string"}, "type": "array"},
                {"type": "string"},
                {"type": "null"},
            ],
        )
