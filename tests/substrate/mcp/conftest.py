"""Shared fixtures for the MCP test suite."""
from __future__ import annotations

import sys
from collections.abc import Iterator

import pytest


@pytest.fixture(autouse=True)
def _restore_sys_path() -> Iterator[None]:
    """Undo every `sys.path` entry a test adds.

    `inject_deployment_path` prepends a deployment dir and its parent and
    never removes them. Each test that resolves a deployment under its own
    `tmp_path` adds two new entries, so one serial run grew `sys.path` from
    9 to over 1,300 entries. Entries added at import or by wider-scoped
    fixtures are set up before this snapshot and survive it.
    """
    saved = list(sys.path)
    yield
    sys.path[:] = saved
