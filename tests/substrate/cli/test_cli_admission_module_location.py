"""Regression test: every admission handler is defined in
``cli/admission.py`` and absent from ``cli/main.py``.

Pinned by ``extract-cli-admission``. Catches a future regression
that re-inlines one of these handlers back into the main dispatch
module.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[3]
CLI_MAIN = REPO_ROOT / "src/okg/substrate/cli/main.py"
CLI_ADMISSION = REPO_ROOT / "src/okg/substrate/cli/admission.py"

EXPECTED_HANDLERS = (
    "_cmd_admission",
    "_cmd_admission_submit",
    "_cmd_admission_decide_promote",
    "_cmd_admission_decide",
)

DEF_PATTERN = re.compile(
    r"^def (_cmd_admission\w*)\(",
    re.MULTILINE,
)


def test_admission_handlers_live_in_admission_module() -> None:
    text = CLI_ADMISSION.read_text()
    defined = set(DEF_PATTERN.findall(text))
    missing = set(EXPECTED_HANDLERS) - defined
    assert missing == set(), (
        f"cli/admission.py is missing handlers expected to live there: "
        f"{sorted(missing)}"
    )


def test_admission_handlers_not_defined_in_main() -> None:
    text = CLI_MAIN.read_text()
    defined = set(DEF_PATTERN.findall(text))
    assert defined == set(), (
        f"cli/main.py re-inlined admission handlers: {sorted(defined)} "
        f"— they belong in cli/admission.py per extract-cli-admission"
    )


@pytest.mark.parametrize("handler", ["_cmd_admission"])
def test_main_imports_cmd_admission(handler: str) -> None:
    """The dispatch-table reference in cli/main.py MUST resolve."""
    text = CLI_MAIN.read_text()
    assert handler in text, (
        f"cli/main.py does not reference {handler!r}; the dispatch "
        f"table will fail to resolve the admission subcommand"
    )
