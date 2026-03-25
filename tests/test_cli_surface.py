from __future__ import annotations

import unittest


class CliSurfaceTest(unittest.TestCase):
    def test_root_commands_include_learning_runtime_surface(self) -> None:
        from kb_mcp.cli import build_parser

        parser = build_parser()
        subparsers_action = next(
            action for action in parser._actions if getattr(action, "choices", None)  # noqa: SLF001
        )
        commands = set(subparsers_action.choices.keys())

        self.assertTrue(
            {
                "version",
                "serve",
                "config",
                "setup",
                "install",
                "hook",
                "worker",
                "session",
                "doctor",
                "judge",
            }.issubset(commands)
        )

    def test_worker_commands_include_runtime_hygiene_operations(self) -> None:
        from kb_mcp.cli import build_parser

        parser = build_parser()
        worker_parser = next(
            action.choices["worker"]
            for action in parser._actions
            if getattr(action, "choices", None) and "worker" in action.choices  # noqa: SLF001
        )
        worker_subparsers = next(
            action for action in worker_parser._actions if getattr(action, "choices", None)  # noqa: SLF001
        )
        commands = set(worker_subparsers.choices.keys())

        self.assertEqual(
            commands,
            {"run-once", "drain", "replay-dead-letter", "cleanup-runtime", "repair-learning-runtime"},
        )

    def test_judge_commands_include_learning_governance_operations(self) -> None:
        from kb_mcp.cli import build_parser

        parser = build_parser()
        judge_parser = next(
            action.choices["judge"]
            for action in parser._actions
            if getattr(action, "choices", None) and "judge" in action.choices  # noqa: SLF001
        )
        judge_subparsers = next(
            action for action in judge_parser._actions if getattr(action, "choices", None)  # noqa: SLF001
        )
        commands = set(judge_subparsers.choices.keys())

        self.assertEqual(
            commands,
            {
                "review-candidates",
                "accept",
                "reject",
                "relabel",
                "materialize",
                "learning-state",
                "retract-learning",
                "supersede-learning",
                "expire-learning",
                "build-policy-snapshots",
                "promote-scopes",
                "retry-failed-materializations",
            },
        )
