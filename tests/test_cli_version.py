from __future__ import annotations

import io
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch


class CliVersionTest(unittest.TestCase):
    def test_dash_dash_version_prints_installed_version(self) -> None:
        from kb_mcp import cli

        buf = io.StringIO()
        with patch("kb_mcp.update.current_version", return_value="0.5.1"):
            with patch("sys.argv", ["kb-mcp", "--version"]):
                with redirect_stdout(buf):
                    with self.assertRaises(SystemExit) as cm:
                        cli.main()
        self.assertEqual(cm.exception.code, 0)
        self.assertEqual(buf.getvalue().strip(), "kb-mcp 0.5.1")

    def test_version_subcommand_prints_installed_version(self) -> None:
        from kb_mcp import cli

        buf = io.StringIO()
        with patch("kb_mcp.update.current_version", return_value="0.5.1"):
            with patch("sys.argv", ["kb-mcp", "version"]):
                with redirect_stdout(buf):
                    cli.main()
        self.assertEqual(buf.getvalue().strip(), "kb-mcp 0.5.1")

    def test_dash_dash_version_prints_dev_when_metadata_missing(self) -> None:
        from kb_mcp import cli

        buf = io.StringIO()
        with patch("kb_mcp.update.current_version", return_value=None):
            with patch("sys.argv", ["kb-mcp", "--version"]):
                with redirect_stdout(buf):
                    with self.assertRaises(SystemExit) as cm:
                        cli.main()
        self.assertEqual(cm.exception.code, 0)
        self.assertEqual(buf.getvalue().strip(), "kb-mcp (dev)")

    def test_version_subcommand_prints_dev_when_metadata_missing(self) -> None:
        from kb_mcp import cli

        buf = io.StringIO()
        with patch("kb_mcp.update.current_version", return_value=None):
            with patch("sys.argv", ["kb-mcp", "version"]):
                with redirect_stdout(buf):
                    cli.main()
        self.assertEqual(buf.getvalue().strip(), "kb-mcp (dev)")
