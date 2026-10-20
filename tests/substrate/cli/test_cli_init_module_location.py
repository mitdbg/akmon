"""Regression test: every init/new-module/new-source handler is
defined in ``cli/init.py`` and absent from ``cli/main.py``.

``_cmd_new_source`` only refuses since issue #2479, but it is still the
dispatch target for the retired verb, so it stays pinned here.

Pinned by ``extract-cli-init``. Catches a future regression that
re-inlines one of these handlers back into the main dispatch module.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[3]
CLI_MAIN = REPO_ROOT / "src/okg/substrate/cli/main.py"
CLI_INIT = REPO_ROOT / "src/okg/substrate/cli/init.py"

EXPECTED_HANDLERS = (
    "_cmd_init",
    "_cmd_init_from_template",
    "_cmd_init_from_profile",
    "_cmd_new_module",
    "_cmd_new_source",
)

DEF_PATTERN = re.compile(
    r"^def (_cmd_init|_cmd_init_from_\w+|_cmd_new_\w+)\(",
    re.MULTILINE,
)


def test_init_handlers_live_in_init_module() -> None:
    text = CLI_INIT.read_text()
    defined = set(DEF_PATTERN.findall(text))
    missing = set(EXPECTED_HANDLERS) - defined
    assert missing == set(), (
        f"cli/init.py is missing handlers expected to live there: "
        f"{sorted(missing)}"
    )


def test_init_handlers_not_defined_in_main() -> None:
    text = CLI_MAIN.read_text()
    defined = set(DEF_PATTERN.findall(text))
    assert defined == set(), (
        f"cli/main.py re-inlined init handlers: {sorted(defined)} "
        f"— they belong in cli/init.py per extract-cli-init"
    )


@pytest.mark.parametrize(
    "handler", ["_cmd_init", "_cmd_new_module", "_cmd_new_source"],
)
def test_main_imports_cmd_handler(handler: str) -> None:
    """The dispatch-table references in cli/main.py MUST resolve.

    Each top-level dispatch entry (`init`, `new-module`, `new-source`)
    references a Python symbol; the import statement at the top of
    main.py is what makes the symbol resolvable.
    """
    text = CLI_MAIN.read_text()
    assert handler in text, (
        f"cli/main.py does not reference {handler!r}; the dispatch "
        f"table will fail to resolve"
    )
