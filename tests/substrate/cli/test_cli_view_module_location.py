"""Regression test: view handlers live in cli/view.py and are absent
from cli/main.py.

Pinned by ``extract-cli-view``.
"""
from __future__ import annotations

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
CLI_MAIN = REPO_ROOT / "src/okg/substrate/cli/main.py"
CLI_VIEW = REPO_ROOT / "src/okg/substrate/cli/view.py"

EXPECTED_HANDLERS = (
    "_cmd_view",
    "_cmd_view_list",
    "_cmd_view_show",
    "_cmd_view_inspect",
)

DEF_PATTERN = re.compile(r"^def (_cmd_view\w*)\(", re.MULTILINE)


def test_view_handlers_live_in_view_module() -> None:
    text = CLI_VIEW.read_text()
    defined = set(DEF_PATTERN.findall(text))
    missing = set(EXPECTED_HANDLERS) - defined
    assert missing == set(), f"cli/view.py missing: {sorted(missing)}"


def test_view_handlers_not_defined_in_main() -> None:
    text = CLI_MAIN.read_text()
    defined = set(DEF_PATTERN.findall(text))
    assert defined == set(), (
        f"cli/main.py re-inlined view handlers: {sorted(defined)} "
        f"— they belong in cli/view.py per extract-cli-view"
    )


def test_main_imports_cmd_view() -> None:
    text = CLI_MAIN.read_text()
    assert "_cmd_view" in text, "cli/main.py does not reference _cmd_view"
