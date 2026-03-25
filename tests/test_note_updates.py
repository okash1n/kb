from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from kb_mcp.note import parse_markdown_note, update_markdown_note


class NoteUpdateTest(unittest.TestCase):
    def test_update_markdown_note_merges_frontmatter_and_updates_body(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "note.md"
            path.write_text(
                "---\n"
                "id: 01TESTNOTE000000000000000000\n"
                "summary: Original\n"
                "ai_tool: codex\n"
                "tags: [a]\n"
                "related: [X]\n"
                "created: 2026-03-25T10:00+09:00\n"
                "updated: 2026-03-25T10:00+09:00\n"
                "---\n\n"
                "original body\n",
                encoding="utf-8",
            )

            result = update_markdown_note(
                path,
                frontmatter_patch={
                    "status": "superseded",
                    "tags": ["b"],
                    "related": ["Y"],
                },
                body_replace="updated body",
            )

            updated_text = path.read_text(encoding="utf-8")
            frontmatter, body = parse_markdown_note(updated_text)
            self.assertEqual(frontmatter["id"], "01TESTNOTE000000000000000000")
            self.assertEqual(frontmatter["created"], "2026-03-25T10:00+09:00")
            self.assertEqual(frontmatter["status"], "superseded")
            self.assertEqual(frontmatter["tags"], ["a", "b"])
            self.assertEqual(frontmatter["related"], ["X", "Y"])
            self.assertEqual(body, "updated body")
            self.assertEqual(result["note_id"], "01TESTNOTE000000000000000000")
            self.assertNotEqual(result["before_sha256"], result["after_sha256"])

    def test_update_markdown_note_rejects_id_change(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "note.md"
            path.write_text(
                "---\n"
                "id: 01TESTNOTE000000000000000000\n"
                "summary: Original\n"
                "ai_tool: codex\n"
                "created: 2026-03-25T10:00+09:00\n"
                "updated: 2026-03-25T10:00+09:00\n"
                "---\n\n"
                "body\n",
                encoding="utf-8",
            )

            with self.assertRaises(ValueError):
                update_markdown_note(
                    path,
                    frontmatter_patch={"id": "01ANOTHERNOTE0000000000000000"},
                )
