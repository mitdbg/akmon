"""MCP tests must not leak `sys.path` entries into later tests or children.

A serial run of `tests/substrate/mcp` grew `sys.path` by two entries per test
that resolved a deployment under `tmp_path`. `_subprocess_env` in the
expansion e2e tests copied all of it into the child's PYTHONPATH, which passed
Linux's 128 KiB per-string limit, and starting the stdio server failed with
`OSError: [Errno 7] Argument list too long`.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

from tests.substrate.mcp import test_tool_list_expansion_e2e as e2e

pytest_plugins = ["pytester"]

_MCP_CONFTEST = Path(__file__).with_name("conftest.py")


def test_mcp_conftest_restores_sys_path_between_tests(
    pytester: pytest.Pytester,
) -> None:
    pytester.makeconftest(_MCP_CONFTEST.read_text())
    pytester.makepyfile(
        test_order=(
            "import sys\n"
            "\n"
            "def test_a_adds_an_entry():\n"
            "    sys.path.insert(0, '/okg-leaked-entry')\n"
            "\n"
            "def test_b_does_not_see_it():\n"
            "    assert '/okg-leaked-entry' not in sys.path\n"
        ),
    )
    result = pytester.runpytest_subprocess("-p", "no:randomly", "-p", "no:xdist")
    result.assert_outcomes(passed=2)


def test_subprocess_env_ignores_a_grown_sys_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    leaked = [f"/okg-leaked/{i:06d}/" + "x" * 80 for i in range(12_000)]
    monkeypatch.setattr(sys, "path", leaked + list(sys.path))

    entries = e2e._subprocess_env()["PYTHONPATH"].split(os.pathsep)

    assert not any(entry.startswith("/okg-leaked/") for entry in entries)
