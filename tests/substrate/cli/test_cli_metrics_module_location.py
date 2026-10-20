"""Regression test: metrics handlers live in cli/metrics.py and
are absent from cli/main.py.

Pinned by ``extract-cli-metrics``.
"""
from __future__ import annotations

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
CLI_MAIN = REPO_ROOT / "src/okg/substrate/cli/main.py"
CLI_METRICS = REPO_ROOT / "src/okg/substrate/cli/metrics.py"

EXPECTED_HANDLERS = (
    "_cmd_metrics",
    "_cmd_metrics_qa_snapshot",
)

DEF_PATTERN = re.compile(r"^def (_cmd_metrics\w*)\(", re.MULTILINE)


def test_metrics_handlers_live_in_metrics_module() -> None:
    text = CLI_METRICS.read_text()
    defined = set(DEF_PATTERN.findall(text))
    missing = set(EXPECTED_HANDLERS) - defined
    assert missing == set(), f"cli/metrics.py missing: {sorted(missing)}"


def test_metrics_handlers_not_defined_in_main() -> None:
    text = CLI_MAIN.read_text()
    defined = set(DEF_PATTERN.findall(text))
    assert defined == set(), (
        f"cli/main.py re-inlined metrics handlers: {sorted(defined)}"
    )


def test_main_imports_cmd_metrics() -> None:
    text = CLI_MAIN.read_text()
    assert "_cmd_metrics" in text, (
        "cli/main.py does not reference _cmd_metrics"
    )
