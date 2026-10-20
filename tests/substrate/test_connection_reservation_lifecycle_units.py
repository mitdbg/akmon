from __future__ import annotations

import threading

import psycopg

from okg.substrate.db import capacity


def _owner(*, handler) -> capacity._ReservationOwner:
    return capacity._ReservationOwner(
        dsn="postgresql://not-used",
        application_name="okg-test-owner",
        pid=1234,
        heartbeat_interval_seconds=60,
        stale_after_seconds=300,
        lease_lost_handler=handler,
        stop=threading.Event(),
        last_success_monotonic=0.0,
    )


def test_production_heartbeat_ratio_and_daemon_thread() -> None:
    assert capacity.RESERVATION_HEARTBEAT_INTERVAL_SECONDS == 60
    assert capacity.RESERVATION_STALE_AFTER_SECONDS == 300
    assert (
        capacity.RESERVATION_STALE_AFTER_SECONDS
        / capacity.RESERVATION_HEARTBEAT_INTERVAL_SECONDS
    ) == 5

    owner = _owner(handler=lambda _reason: None)
    thread = capacity._new_reservation_heartbeat_thread(owner)
    assert thread.daemon is True


def test_heartbeat_failure_self_fences_before_stale_horizon(
    monkeypatch,
) -> None:
    reasons: list[str] = []
    owner = _owner(handler=reasons.append)

    def fail_connect(*_args, **_kwargs):
        raise psycopg.OperationalError("database unavailable")

    monkeypatch.setattr(psycopg, "connect", fail_connect)
    monkeypatch.setattr(capacity.time, "monotonic", lambda: 240.0)

    capacity._refresh_reservation(owner)

    assert owner.stop.is_set()
    assert len(reasons) == 1
    assert "self-fence threshold 240.0s" in reasons[0]


class _Cursor:
    rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def execute(self, *_args, **_kwargs) -> None:
        return None


class _Connection:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def cursor(self) -> _Cursor:
        return _Cursor()


def test_vanished_owned_row_self_fences(monkeypatch) -> None:
    reasons: list[str] = []
    owner = _owner(handler=reasons.append)
    monkeypatch.setattr(psycopg, "connect", lambda *_a, **_kw: _Connection())

    capacity._refresh_reservation(owner)

    assert owner.stop.is_set()
    assert reasons and "updated=0" in reasons[0]


def test_intentional_release_does_not_self_fence_on_zero_row(monkeypatch) -> None:
    reasons: list[str] = []
    owner = _owner(handler=reasons.append)
    owner.stop.set()
    monkeypatch.setattr(psycopg, "connect", lambda *_a, **_kw: _Connection())

    capacity._refresh_reservation(owner)

    assert reasons == []


def test_heartbeat_loop_caps_retry_wait_at_self_fence_deadline(
    monkeypatch,
) -> None:
    clock = [0.0]
    waits: list[float] = []
    refreshes: list[float] = []
    reasons: list[str] = []

    class FakeStop:
        stopped = False

        def is_set(self) -> bool:
            return self.stopped

        def set(self) -> None:
            self.stopped = True

        def wait(self, timeout: float) -> bool:
            waits.append(timeout)
            clock[0] += timeout
            return self.stopped

    owner = _owner(handler=reasons.append)
    owner.stop = FakeStop()

    def failed_refresh(_owner) -> None:
        refreshes.append(clock[0])
        clock[0] += 5.0

    monkeypatch.setattr(capacity.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(capacity, "_refresh_reservation", failed_refresh)

    capacity._reservation_heartbeat_loop(owner)

    assert refreshes == [60.0, 125.0, 190.0]
    assert waits == [60, 60, 60, 45]
    assert clock[0] == 240.0
    assert reasons and "self-fence threshold 240.0s" in reasons[0]
