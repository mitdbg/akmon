"""SourceHealth accepts the acquisition outcomes used by ensure_checkout.

A source that manages its own checkout reports `acquire_stale` (could not
fast-forward; indexed the existing tree) or `acquire_failed` (could not
materialise the checkout; emitted zero facts). These must construct without
raising, while the frozen-set guard still rejects genuinely unknown statuses.
"""
from __future__ import annotations

import pytest

from okg.substrate.library.sources.base import PREFLIGHT_STATUSES, SourceHealth


def test_acquire_statuses_construct() -> None:
    for status in ("acquire_stale", "acquire_failed"):
        assert status in PREFLIGHT_STATUSES
        health = SourceHealth(status=status, reason="checkout could not fast-forward")
        assert health.status == status


def test_unknown_status_still_raises() -> None:
    with pytest.raises(ValueError, match="unknown source health status"):
        SourceHealth(status="not_a_real_status")
