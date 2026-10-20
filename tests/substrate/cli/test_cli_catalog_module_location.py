"""Regression test: every ``_cmd_catalog*`` handler is defined in
``cli/catalog.py`` and absent from ``cli/main.py``.

Pinned by ``extract-cli-catalog``. Catches a future regression that
re-inlines a catalog handler into the main dispatch module.
"""
from __future__ import annotations

import re
from pathlib import Path



REPO_ROOT = Path(__file__).resolve().parents[3]
CLI_MAIN = REPO_ROOT / "src/okg/substrate/cli/main.py"
CLI_CATALOG = REPO_ROOT / "src/okg/substrate/cli/catalog.py"

EXPECTED_HANDLERS = (
    "_cmd_catalog",
    "_cmd_catalog_history",
    "_cmd_catalog_load",
)

DEF_PATTERN = re.compile(r"^def (_cmd_catalog[\w_]*)\(", re.MULTILINE)


def test_catalog_handlers_live_in_catalog_module() -> None:
    """Every expected ``_cmd_catalog*`` handler MUST be defined in
    ``cli/catalog.py``.
    """
    text = CLI_CATALOG.read_text()
    defined = set(DEF_PATTERN.findall(text))
    missing = set(EXPECTED_HANDLERS) - defined
    assert missing == set(), (
        f"cli/catalog.py is missing handlers expected to live there: "
        f"{sorted(missing)}"
    )


def test_catalog_handlers_not_defined_in_main() -> None:
    """No ``_cmd_catalog*`` handler MUST be defined in ``cli/main.py``.
    Re-inlining one is a regression.
    """
    text = CLI_MAIN.read_text()
    defined = set(DEF_PATTERN.findall(text))
    assert defined == set(), (
        f"cli/main.py re-inlined catalog handlers: {sorted(defined)} "
        f"— they belong in cli/catalog.py per extract-cli-catalog"
    )


def test_main_imports_top_level_dispatcher() -> None:
    """``cli/main.py`` MUST reference ``_cmd_catalog`` (the top-level
    sub-dispatcher); sub-handlers (history / load / ownership) are
    routed internally by ``_cmd_catalog`` itself and don't need
    main.py references.
    """
    text = CLI_MAIN.read_text()
    assert "_cmd_catalog" in text, (
        "cli/main.py does not reference _cmd_catalog; the dispatch "
        "table will fail to resolve the catalog subcommand"
    )
