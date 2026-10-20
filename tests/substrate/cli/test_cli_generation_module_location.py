"""Regression test: every ``_cmd_generation*`` handler is defined in
``cli/generation.py`` and absent from ``cli/main.py``.

Pinned by ``extract-cli-generation``. Catches a future regression that
re-inlines a generation handler into the main dispatch module.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[3]
CLI_MAIN = REPO_ROOT / "src/okg/substrate/cli/main.py"
CLI_GENERATION = REPO_ROOT / "src/okg/substrate/cli/generation.py"

EXPECTED_HANDLERS = (
    "_cmd_generation",
    "_cmd_generation_list",
)

DEF_PATTERN = re.compile(r"^def (_cmd_generation[\w_]*)\(", re.MULTILINE)


def test_generation_handlers_live_in_generation_module() -> None:
    text = CLI_GENERATION.read_text()
    defined = set(DEF_PATTERN.findall(text))
    missing = set(EXPECTED_HANDLERS) - defined
    assert missing == set(), (
        f"cli/generation.py is missing handlers expected to live "
        f"there: {sorted(missing)}"
    )


def test_generation_handlers_not_defined_in_main() -> None:
    text = CLI_MAIN.read_text()
    defined = set(DEF_PATTERN.findall(text))
    assert defined == set(), (
        f"cli/main.py re-inlined generation handlers: "
        f"{sorted(defined)} — they belong in cli/generation.py per "
        f"extract-cli-generation"
    )


@pytest.mark.parametrize("handler", EXPECTED_HANDLERS)
def test_main_imports_generation_handler(handler: str) -> None:
    text = CLI_MAIN.read_text()
    assert handler in text, (
        f"cli/main.py does not reference {handler!r}; the dispatch "
        f"table will fail to resolve the generation subcommand"
    )
