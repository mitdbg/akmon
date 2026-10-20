"""Shared fixtures for Nomos substrate tests."""

from __future__ import annotations

import pytest

from okg.substrate.nomos.runtime_enforcement import (
    _clear_nomos_enforcement_mode_cache,
)


@pytest.fixture(autouse=True)
def _clear_nomos_enforcement_cache():
    """Isolate the process-level enforcement-mode memos between tests.

    ``deployment_nomos_enforcement_mode`` memoizes the resolved deployment dir
    and mode per ``(OKG_DEPLOYMENTS_DIR, name)``. Tests build deployments of the
    same name under fresh ``tmp_path``s, so clearing before AND after each test
    keeps isolation independent of collection order. (The cache-key fix already
    prevents stale cross-root hits; this also guards same-root reuse.)
    """
    _clear_nomos_enforcement_mode_cache()
    yield
    _clear_nomos_enforcement_mode_cache()
