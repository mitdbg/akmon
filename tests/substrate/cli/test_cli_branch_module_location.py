"""Regression test: ``_cmd_branch`` lives in ``cli/branch.py`` and
is absent from ``cli/main.py``.

Pinned by ``extract-cli-branch``.
"""
from __future__ import annotations

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
CLI_MAIN = REPO_ROOT / "src/okg/substrate/cli/main.py"
CLI_BRANCH = REPO_ROOT / "src/okg/substrate/cli/branch.py"

DEF_PATTERN = re.compile(r"^def (_cmd_branch)\(", re.MULTILINE)


def test_branch_handler_lives_in_branch_module() -> None:
    text = CLI_BRANCH.read_text()
    assert DEF_PATTERN.search(text), (
        "cli/branch.py is missing _cmd_branch"
    )


def test_branch_handler_not_defined_in_main() -> None:
    text = CLI_MAIN.read_text()
    assert not DEF_PATTERN.search(text), (
        "cli/main.py re-inlined _cmd_branch — it belongs in "
        "cli/branch.py per extract-cli-branch"
    )


def test_main_imports_cmd_branch() -> None:
    text = CLI_MAIN.read_text()
    assert "_cmd_branch" in text, (
        "cli/main.py does not reference _cmd_branch; the dispatch "
        "table will fail to resolve the branch subcommand"
    )
