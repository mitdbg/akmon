"""Regression test: deployment handlers live in cli/deployment.py
and are absent from cli/main.py.

Pinned by ``extract-cli-deployment``.
"""
from __future__ import annotations

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
CLI_MAIN = REPO_ROOT / "src/okg/substrate/cli/main.py"
CLI_DEPLOYMENT = REPO_ROOT / "src/okg/substrate/cli/deployment.py"

EXPECTED_HANDLERS = (
    "_cmd_deployment",
    "_cmd_deployment_show",
    "_cmd_deployment_bug_report",
)

DEF_PATTERN = re.compile(r"^def (_cmd_deployment\w*)\(", re.MULTILINE)


def test_deployment_handlers_live_in_deployment_module() -> None:
    text = CLI_DEPLOYMENT.read_text()
    defined = set(DEF_PATTERN.findall(text))
    missing = set(EXPECTED_HANDLERS) - defined
    assert missing == set(), (
        f"cli/deployment.py missing: {sorted(missing)}"
    )


def test_deployment_handlers_not_defined_in_main() -> None:
    text = CLI_MAIN.read_text()
    defined = set(DEF_PATTERN.findall(text))
    assert defined == set(), (
        f"cli/main.py re-inlined deployment handlers: "
        f"{sorted(defined)} — they belong in cli/deployment.py "
        f"per extract-cli-deployment"
    )


def test_main_imports_cmd_deployment() -> None:
    text = CLI_MAIN.read_text()
    assert "_cmd_deployment" in text, (
        "cli/main.py does not reference _cmd_deployment"
    )
