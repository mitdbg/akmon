"""Regression test: every ``_cmd_runtime*`` handler is defined in
``cli/runtime.py`` and absent from ``cli/main.py``.

Pinned by ``extract-cli-runtime``. Catches a future regression that
re-inlines a runtime handler into the main dispatch module.
"""
from __future__ import annotations

import re
from pathlib import Path



REPO_ROOT = Path(__file__).resolve().parents[3]
CLI_MAIN = REPO_ROOT / "src/okg/substrate/cli/main.py"
CLI_RUNTIME = REPO_ROOT / "src/okg/substrate/cli/runtime.py"

EXPECTED_HANDLERS = (
    "_cmd_runtime",
    "_cmd_runtime_escalation",
)

DEF_PATTERN = re.compile(r"^def (_cmd_runtime[\w_]*)\(", re.MULTILINE)


def test_runtime_handlers_live_in_runtime_module() -> None:
    text = CLI_RUNTIME.read_text()
    defined = set(DEF_PATTERN.findall(text))
    missing = set(EXPECTED_HANDLERS) - defined
    assert missing == set(), (
        f"cli/runtime.py is missing handlers expected to live there: "
        f"{sorted(missing)}"
    )


def test_runtime_handlers_not_defined_in_main() -> None:
    text = CLI_MAIN.read_text()
    defined = set(DEF_PATTERN.findall(text))
    assert defined == set(), (
        f"cli/main.py re-inlined runtime handlers: {sorted(defined)} "
        f"— they belong in cli/runtime.py per extract-cli-runtime"
    )


def test_main_imports_top_level_dispatcher() -> None:
    """``cli/main.py`` MUST reference ``_cmd_runtime`` (the top-level
    sub-dispatcher); sub-handlers (escalation, …) are routed
    internally and don't need main.py references.
    """
    text = CLI_MAIN.read_text()
    assert "_cmd_runtime" in text, (
        "cli/main.py does not reference _cmd_runtime; the dispatch "
        "table will fail to resolve the runtime subcommand"
    )
