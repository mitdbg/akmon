"""The publish cycle's pool reservation is enforced at CHECKOUT, not only
at declaration (issue #1598).

`check_feasibility` already refused width combinations whose borrowers could
not fit alongside the reservation. That is arithmetic about DECLARED widths.
It said nothing about who actually holds a connection at any moment, so a
long-running scheduled workflow and a few ingest lanes could still occupy
every slot while a publish preflight waited out its checkout timeout and died
on a bare `PoolTimeout` — which is what happened on okg-workspace on
2026-09-04, five hours into a repair run.

These tests drive a fake pool so slots can be held open deterministically.
Slots are held on SEPARATE THREADS, matching how ingest lanes run: the
admission slot is reentrant within one thread by design (see
`test_a_nested_borrow_reuses_the_thread_s_slot`).
"""
from __future__ import annotations

import threading
from contextlib import contextmanager

import pytest

from okg.substrate.db import pool as pool_module
from okg.substrate.db.concurrency_budget import (
    general_admission_width,
    reserved_slots,
    width,
)


DSN = "postgresql://fake/reservation"


class _FakeConnection:
    pass


class _FakePool:
    """Counts concurrent checkouts and refuses more than `max_size`."""

    def __init__(self, max_size: int) -> None:
        self.max_size = max_size
        self.name = "fake-pool"
        self.closed = False
        self._lock = threading.Lock()
        self.in_use = 0
        self.peak_in_use = 0

    @contextmanager
    def connection(self, timeout=None):
        with self._lock:
            if self.in_use >= self.max_size:
                raise AssertionError(
                    "the fake pool was oversubscribed; the admission "
                    "semaphore should have refused this borrower first"
                )
            self.in_use += 1
            self.peak_in_use = max(self.peak_in_use, self.in_use)
        try:
            yield _FakeConnection()
        finally:
            with self._lock:
                self.in_use -= 1


@pytest.fixture
def fake_pool(monkeypatch):
    pool = _FakePool(max_size=width("db.shared_pool"))
    monkeypatch.setattr(pool_module, "_POOLS", {})
    monkeypatch.setattr(pool_module, "_ADMISSION", {})
    monkeypatch.setattr(pool_module, "_HELD", threading.local())
    monkeypatch.setattr(pool_module, "get_pool", lambda dsn, **kwargs: pool)
    return pool


class _Holder:
    """Holds a general slot on its own thread until released."""

    def __init__(self) -> None:
        self.acquired = threading.Event()
        self.release = threading.Event()
        self.error: Exception | None = None
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        try:
            with pool_module.pooled_connection(
                DSN, purpose=pool_module.GENERAL, timeout=5.0,
            ):
                self.acquired.set()
                self.release.wait(timeout=10)
        except Exception as exc:  # noqa: BLE001 - surfaced by the test
            self.error = exc
            self.acquired.set()

    def start(self) -> "_Holder":
        self._thread.start()
        assert self.acquired.wait(timeout=5), "holder never started"
        assert self.error is None, self.error
        return self

    def stop(self) -> None:
        self.release.set()
        self._thread.join(timeout=5)


@contextmanager
def _all_general_slots_held():
    holders = [_Holder().start() for _ in range(general_admission_width(
        "db.shared_pool",
    ))]
    try:
        yield holders
    finally:
        for holder in holders:
            holder.stop()


def test_the_general_gate_leaves_room_for_the_whole_publish_side():
    """The two halves of the budget must agree.

    The publish side holds more than the reservation: the publisher's own
    cycle connection plus the invariant-gate workers. Sizing the general gate
    as `max_size - reserved` would hand out slots those already claim.
    """
    pool_width = width("db.shared_pool")
    general = general_admission_width("db.shared_pool")
    publish_side = reserved_slots("db.shared_pool") + width("gate.invariants")
    telemetry = width("llm.telemetry")
    assert general + publish_side + telemetry <= pool_width
    assert general < pool_width - reserved_slots("db.shared_pool")


def test_general_borrowers_cannot_take_the_reserved_slots(fake_pool):
    """Hold every general slot; the next general borrow is REFUSED."""
    with _all_general_slots_held():
        assert fake_pool.in_use == general_admission_width("db.shared_pool")
        with pytest.raises(pool_module.PoolReservationExhausted) as exc:
            with pool_module.pooled_connection(
                DSN, purpose=pool_module.GENERAL, timeout=0.05,
            ):
                pytest.fail("a general borrower took a reserved slot")
    message = str(exc.value)
    assert "publish cycle" in message
    assert "db.shared_pool" in message


def test_publish_path_still_gets_a_connection_when_general_slots_are_full(
    fake_pool,
):
    """The failure this reservation exists to prevent."""
    with _all_general_slots_held():
        held = fake_pool.in_use
        with pool_module.pooled_connection(
            DSN, purpose=pool_module.RESERVED,
        ) as conn:
            assert conn is not None
            assert fake_pool.in_use == held + 1


def test_the_publish_side_can_hold_more_than_one_connection_at_once(
    fake_pool,
):
    """The publisher's cycle connection AND its gate workers, together."""
    with _all_general_slots_held():
        publish_side = (
            reserved_slots("db.shared_pool") + width("gate.invariants")
        )
        borrows = []
        try:
            for _ in range(publish_side):
                cm = pool_module.pooled_connection(
                    DSN, purpose=pool_module.RESERVED,
                )
                cm.__enter__()
                borrows.append(cm)
            assert fake_pool.in_use <= fake_pool.max_size
        finally:
            for cm in borrows:
                cm.__exit__(None, None, None)


def test_a_nested_borrow_reuses_the_thread_s_slot(fake_pool):
    """A source adapter borrowing inside its own source run must not wait
    for a slot that run is already holding."""
    with _all_general_slots_held():
        pass
    outer = pool_module.pooled_connection(DSN, purpose=pool_module.GENERAL)
    outer.__enter__()
    try:
        # Fill every remaining general slot from other threads, then borrow
        # again on this thread: reentrancy, not a second slot.
        others = [
            _Holder().start()
            for _ in range(general_admission_width("db.shared_pool") - 1)
        ]
        try:
            with pool_module.pooled_connection(
                DSN, purpose=pool_module.GENERAL, timeout=0.05,
            ) as nested:
                assert nested is not None
        finally:
            for holder in others:
                holder.stop()
    finally:
        outer.__exit__(None, None, None)
    # The reentrant borrow released its depth, so the thread is clean again.
    with pool_module.pooled_connection(DSN, purpose=pool_module.GENERAL):
        pass


def test_general_slot_is_released_when_the_body_raises(fake_pool):
    """A failing borrower must not leak its admission slot."""
    for _ in range(20):
        with pytest.raises(RuntimeError):
            with pool_module.pooled_connection(
                DSN, purpose=pool_module.GENERAL,
            ):
                raise RuntimeError("borrower failed")
    assert fake_pool.in_use == 0
    with _all_general_slots_held():
        assert fake_pool.in_use == general_admission_width("db.shared_pool")


def test_general_borrower_waits_for_a_slot_rather_than_failing_at_once(
    fake_pool,
):
    """Contention is throttling, not refusal, while a slot frees up."""
    holders = [_Holder().start() for _ in range(general_admission_width(
        "db.shared_pool",
    ))]

    def _release_one_soon():
        holders.pop().stop()

    releaser = threading.Timer(0.1, _release_one_soon)
    releaser.start()
    try:
        with pool_module.pooled_connection(
            DSN, purpose=pool_module.GENERAL, timeout=5.0,
        ) as conn:
            assert conn is not None
    finally:
        releaser.join()
        for holder in holders:
            holder.stop()


def test_unknown_purpose_is_refused(fake_pool):
    with pytest.raises(ValueError, match="unknown pooled_connection purpose"):
        with pool_module.pooled_connection(DSN, purpose="whatever"):
            pytest.fail("an unknown purpose must not borrow")


def test_never_oversubscribes_under_concurrent_general_borrowers(fake_pool):
    """The fake pool asserts if the semaphore ever lets too many through."""
    errors: list[Exception] = []
    barrier = threading.Barrier(24)

    def _borrow():
        try:
            barrier.wait(timeout=5)
            with pool_module.pooled_connection(
                DSN, purpose=pool_module.GENERAL, timeout=10.0,
            ):
                pass
        except Exception as exc:  # noqa: BLE001 - reported below
            errors.append(exc)

    threads = [threading.Thread(target=_borrow) for _ in range(24)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    assert fake_pool.peak_in_use <= general_admission_width("db.shared_pool")
    assert fake_pool.in_use == 0
