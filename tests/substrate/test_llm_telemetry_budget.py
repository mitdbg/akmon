"""Telemetry borrows from the pool, and its exhaustion is loud and counted.

`req.unify-connection-budget.telemetry-in-budget` moves `record_llm_call` off
its own `psycopg.connect` and onto the shared pool; the point is that a
model-call fan-out of width N stops costing N unaccounted connections.
`req.unify-connection-budget.telemetry-drop-is-loud` then requires that a drop
caused by BUDGET exhaustion is distinguishable from a database fault — those
have different fixes, and conflating them makes an exhausted budget look like
an outage.

The best-effort contract is load-bearing in the other direction too: telemetry
must never propagate a failure to the caller, because the model's result has
already been paid for.
"""
from __future__ import annotations

import logging
import threading

import pytest

from okg.substrate.llm import tracker


@pytest.fixture(autouse=True)
def _reset_counter():
    tracker._BUDGET_DROPS.reset()
    yield
    tracker._BUDGET_DROPS.reset()


def _record(**overrides):
    kwargs = dict(
        dsn="postgres://unused/telemetry",
        caller="enricher:test",
        model="test/model",
        fragment_id="frag-1",
        prompt_tokens=10,
        completion_tokens=5,
        total_tokens=15,
        cost_usd=0.01,
        latency_ms=42,
        success=True,
    )
    kwargs.update(overrides)
    return tracker.record_llm_call(**kwargs)


def test_telemetry_uses_the_shared_pool(monkeypatch) -> None:
    """The write borrows a pooled connection instead of opening its own."""
    opened: list[str] = []

    class _Cur:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def execute(self, *a, **k): return None

    class _Conn:
        def cursor(self): return _Cur()

    class _Pool:
        def connection(self, **kw):
            from contextlib import contextmanager

            @contextmanager
            def _cm():
                opened.append("pooled")
                yield _Conn()

            return _cm()

    monkeypatch.setattr(
        "okg.substrate.db.pool.get_pool", lambda dsn, **kw: _Pool()
    )

    def _boom(*a, **k):
        raise AssertionError("record_llm_call opened its own connection")

    monkeypatch.setattr(tracker, "db_connect", _boom)
    monkeypatch.setattr(tracker, "record_span", lambda *a, **k: None)

    _record()
    assert opened == ["pooled"]


def test_budget_exhaustion_is_counted_and_records_the_digest_VALUE(
    monkeypatch, caplog
) -> None:
    """A slot timeout must be counted, and the drop record must carry the
    digest VALUE — not merely the word.

    The first version of this test asserted `"outcome_key_digest" in message`,
    which matched the literal format string and therefore passed no matter what
    the code did. The value is the only part an operator can use to repair the
    broken call-to-outcome join, so it is what gets asserted.
    """
    monkeypatch.setattr(
        "okg.substrate.db.pool.get_pool", lambda dsn, **kw: object()
    )
    exhausted = threading.BoundedSemaphore(1)
    exhausted.acquire()
    monkeypatch.setattr(tracker, "_telemetry_slots", lambda: exhausted)
    monkeypatch.setattr(tracker, "_SLOT_WAIT_SECONDS", 0.01)

    with caplog.at_level(logging.ERROR):
        _record(outcome_key_digest="deadbeefcafe1234")

    assert tracker.telemetry_budget_drops() == 1
    message = caplog.text
    assert "DROPPED" in message
    assert "connection-budget exhaustion" in message
    assert "deadbeefcafe1234" in message, (
        "the drop record must interpolate the digest value, not just name it"
    )


def test_pool_timeout_is_not_diagnosed_as_a_healthy_database(
    monkeypatch, caplog
) -> None:
    """psycopg_pool raises PoolTimeout for an unreachable database as well as
    for contention, so the log must NOT assert 'not a database fault' — that
    sends an operator to resize a pool while Postgres is down."""
    from psycopg_pool import PoolTimeout

    class _Pool:
        def connection(self, **kw):
            raise PoolTimeout("pool exhausted or database unreachable")

    monkeypatch.setattr(
        "okg.substrate.db.pool.get_pool", lambda dsn, **kw: _Pool()
    )
    monkeypatch.setattr(
        tracker, "_telemetry_slots", lambda: threading.BoundedSemaphore(4)
    )

    with caplog.at_level(logging.ERROR):
        _record()

    message = caplog.text
    assert "unreachable database" in message
    assert "not a database fault" not in message
    # A pool-side timeout is NOT this process overspending its own budget.
    assert tracker.telemetry_budget_drops() == 0


def test_budget_exhaustion_never_raises_to_the_caller(monkeypatch) -> None:
    """The model result has already been paid for; telemetry must not turn a
    successful call into a failure."""
    monkeypatch.setattr(
        "okg.substrate.db.pool.get_pool", lambda dsn, **kw: object()
    )
    exhausted = threading.BoundedSemaphore(1)
    exhausted.acquire()
    monkeypatch.setattr(tracker, "_telemetry_slots", lambda: exhausted)
    monkeypatch.setattr(tracker, "_SLOT_WAIT_SECONDS", 0.01)

    assert _record() is None


def test_database_failure_is_not_counted_as_budget_exhaustion(monkeypatch) -> None:
    """A genuine DB error keeps the old best-effort path and must NOT inflate
    the budget-drop counter — otherwise the two conditions are
    indistinguishable again, which is what this requirement exists to fix."""
    class _Pool:
        def connection(self):
            raise RuntimeError("relation okg.llm_calls does not exist")

    monkeypatch.setattr(
        "okg.substrate.db.pool.get_pool", lambda dsn, **kw: _Pool()
    )
    monkeypatch.setattr(tracker, "_telemetry_slots", lambda: threading.BoundedSemaphore(4))

    assert _record() is None
    assert tracker.telemetry_budget_drops() == 0


def test_slot_width_comes_from_the_registry(monkeypatch) -> None:
    """The cap is the `llm.telemetry` term, not a constant in this module."""
    from okg.substrate.db import concurrency_budget

    concurrency_budget.set_override("llm.telemetry", 2)
    try:
        slots = tracker._telemetry_slots()
        assert slots.acquire(timeout=0.01)
        assert slots.acquire(timeout=0.01)
        assert not slots.acquire(timeout=0.01)
        slots.release()
        slots.release()
    finally:
        concurrency_budget.clear_overrides()


def test_a_later_override_is_not_silently_ignored() -> None:
    """The semaphore must track the declared width rather than freezing it.

    Caching it at first use meant any telemetry call before the manifest was
    applied froze the default, and the later override was silently ignored —
    the same silent divergence between declaration and behavior that get_pool
    was changed to refuse. It also leaked the cached object across tests.
    """
    from okg.substrate.db import concurrency_budget

    concurrency_budget.set_override("llm.telemetry", 1)
    first = tracker._telemetry_slots()
    try:
        concurrency_budget.set_override("llm.telemetry", 3)
        second = tracker._telemetry_slots()
        assert second is not first, "width change must rebuild the semaphore"
        acquired = [second.acquire(timeout=0.01) for _ in range(3)]
        assert all(acquired)
        assert not second.acquire(timeout=0.01)
        for _ in range(3):
            second.release()
    finally:
        concurrency_budget.clear_overrides()
