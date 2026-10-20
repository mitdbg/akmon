"""AT-08: executable, outcome-led instructions and live command names."""

from __future__ import annotations

import argparse
from pathlib import Path
import re
import shlex

from okg.substrate.cli.main import _build_parser
from tests.mirror_scope import shipped


ROOT = Path(__file__).resolve().parents[2]
# The agent runbook and src/okg/AGENTS.md are this repository's own; the public
# copy checks the shipped operator page (tests/mirror_scope.py).
INSTRUCTION_PAGES = tuple(shipped((
    ROOT / "docs/runbooks/agents/okg-usage.md",
    ROOT / "docs/operate/index.md",
)))
COMMAND_PAGES = tuple(shipped(INSTRUCTION_PAGES + (ROOT / "src/okg/AGENTS.md",)))
_CODE = re.compile(r"```[A-Za-z0-9_-]*\n(.*?)```|`([^`]+)`", re.DOTALL)
_OKG = re.compile(r"(?:^|\s)(?:uv\s+run\s+)?okg\s+([^|&;\n]+)")


def _commands(text: str) -> list[str]:
    commands: list[str] = []
    for match in _CODE.finditer(text):
        region = match.group(1) or match.group(2) or ""
        region = re.sub(r"\\\s*\n\s*", " ", region)
        for command in _OKG.findall(region):
            commands.append(command.strip())
    return commands


def _top_level_choices(parser: argparse.ArgumentParser) -> set[str]:
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            return set(action.choices)
    return set()


def test_at08_operator_and_agent_instructions_name_action_success_and_failure() -> None:
    for path in INSTRUCTION_PAGES:
        lowered = path.read_text(encoding="utf-8").lower()
        assert _commands(path.read_text(encoding="utf-8")), f"{path} has no runnable command"
        assert any(
            token in lowered
            for token in ("expected", "healthy", "published", "success", "passes")
        ), f"{path} does not say what success looks like"
        assert any(
            token in lowered
            for token in ("if it fails", "if a command fails", "diagnostic", "blocker", "error")
        ), f"{path} does not tell readers where to start on failure"


def test_at08_documented_top_level_commands_exist_in_live_argparse() -> None:
    parser = _build_parser([])
    choices = _top_level_choices(parser)
    assert choices, "could not find live CLI subparser choices"
    for path in COMMAND_PAGES:
        for command in _commands(path.read_text(encoding="utf-8")):
            try:
                top = shlex.split(command)[0]
            except ValueError as exc:
                raise AssertionError(f"malformed documented command in {path}: {command}") from exc
            assert top in choices, f"{path} names unsupported CLI verb: okg {top}"


def test_at08_source_agent_instructions_point_to_the_canonical_current_guides() -> None:
    text = (ROOT / "src/okg/AGENTS.md").read_text(encoding="utf-8").lower()
    assert "docs/develop/architecture/pipeline.md" in text
    assert "docs/develop/architecture/overview.md" in text
    assert "use `query`" in text or "query` operation" in text
    assert "run_sql" in text  # retained only to explain the retired/internal name
