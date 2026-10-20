"""Unit tests for `fan_out_sealed_brief` deterministic helpers
(S-A7-R4 idempotency).

The DB-backed end-to-end test lives in
tests/substrate/mcp/test_fan_out_pg.py and runs against a real
Publisher.
"""
from __future__ import annotations

from datetime import datetime, timezone


from okg.substrate.mcp.fan_out import (
    _compute_dispatch_id,
    _sub_agent_session_id,
)


def test_dispatch_id_is_deterministic_within_a_second() -> None:
    """S-A7-R4 idempotency: two calls within the same second
    using the same brief + workers yield the same dispatch_id."""
    when = datetime(2026, 5, 18, 12, 0, 0, tzinfo=timezone.utc)
    a = _compute_dispatch_id(
        brief_id="brief:wave-b-1",
        worker_ids=["w1", "w2"],
        observed_at=when,
    )
    b = _compute_dispatch_id(
        brief_id="brief:wave-b-1",
        worker_ids=["w1", "w2"],
        observed_at=when,
    )
    assert a == b
    assert a.startswith("dispatch:")


def test_dispatch_id_differs_for_distinct_briefs() -> None:
    when = datetime(2026, 5, 18, 12, 0, 0, tzinfo=timezone.utc)
    a = _compute_dispatch_id(
        brief_id="brief:wave-b-1",
        worker_ids=["w1"],
        observed_at=when,
    )
    b = _compute_dispatch_id(
        brief_id="brief:wave-b-2",
        worker_ids=["w1"],
        observed_at=when,
    )
    assert a != b


def test_dispatch_id_ignores_worker_order() -> None:
    """`workers` is a list but the dispatch_id must be invariant
    under reordering — otherwise two semantically-identical calls
    would emit two distinct audit rows."""
    when = datetime(2026, 5, 18, 12, 0, 0, tzinfo=timezone.utc)
    a = _compute_dispatch_id(
        brief_id="b",
        worker_ids=["w1", "w2", "w3"],
        observed_at=when,
    )
    b = _compute_dispatch_id(
        brief_id="b",
        worker_ids=["w3", "w1", "w2"],
        observed_at=when,
    )
    assert a == b


def test_dispatch_id_changes_across_seconds() -> None:
    a = _compute_dispatch_id(
        brief_id="b",
        worker_ids=["w1"],
        observed_at=datetime(2026, 5, 18, 12, 0, 0, tzinfo=timezone.utc),
    )
    b = _compute_dispatch_id(
        brief_id="b",
        worker_ids=["w1"],
        observed_at=datetime(2026, 5, 18, 12, 0, 1, tzinfo=timezone.utc),
    )
    assert a != b


def test_sub_agent_session_id_format() -> None:
    sid = _sub_agent_session_id(
        dispatch_id="dispatch:abc123", worker_id="w1",
    )
    assert sid == "sub_agent_session:abc123:w1"
