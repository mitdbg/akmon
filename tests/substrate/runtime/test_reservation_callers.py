from __future__ import annotations

import inspect
import signal
import sys
from types import SimpleNamespace

import pytest


def test_worker_outer_boundary_releases_on_return_and_exception(
    monkeypatch,
) -> None:
    from okg.substrate.db import capacity
    from okg.substrate.runtime import worker
    from okg.substrate.runtime.config import RuntimeConfig

    released: list[str] = []
    monkeypatch.setattr(
        capacity,
        "release_process_reservations",
        lambda *, application_name: released.append(application_name),
    )
    monkeypatch.setattr(
        worker,
        "_preflight_embedding_plan",
        lambda _deployment: object(),
    )
    wrapped = worker._own_connection_reservation(lambda **_kwargs: 7)

    assert wrapped(
        deployment="cms",
        dsn="postgresql://not-used",
        runtime=RuntimeConfig(enabled=True),
    ) == 7
    assert released == ["okg-worker-cms"]

    released.clear()

    def fail(**_kwargs):
        raise RuntimeError("worker setup failed")

    wrapped = worker._own_connection_reservation(fail)
    with pytest.raises(RuntimeError, match="worker setup failed"):
        wrapped(
            deployment="cms",
            dsn="postgresql://not-used",
            runtime=RuntimeConfig(enabled=True),
        )
    assert released == ["okg-worker-cms"]


def test_worker_setup_sigterm_unwinds_and_releases(monkeypatch) -> None:
    from okg.substrate.db import capacity
    from okg.substrate.runtime import worker
    from okg.substrate.runtime.config import RuntimeConfig

    released: list[str] = []
    handlers = {signal.SIGTERM: signal.SIG_DFL}

    def fake_getsignal(signum):
        return handlers[signum]

    def fake_signal(signum, handler):
        previous = handlers[signum]
        handlers[signum] = handler
        return previous

    def interrupt_setup(**_kwargs):
        handlers[signal.SIGTERM](signal.SIGTERM, None)

    monkeypatch.setattr(worker.signal, "getsignal", fake_getsignal)
    monkeypatch.setattr(worker.signal, "signal", fake_signal)
    monkeypatch.setattr(
        worker,
        "_preflight_embedding_plan",
        lambda _deployment: object(),
    )
    wrapped = worker._own_connection_reservation(interrupt_setup)
    monkeypatch.setattr(
        capacity,
        "release_process_reservations",
        lambda *, application_name: released.append(application_name),
    )

    with pytest.raises(SystemExit) as exc:
        wrapped(
            deployment="cms",
            dsn="postgresql://not-used",
            runtime=RuntimeConfig(enabled=True),
        )

    assert exc.value.code == 128 + signal.SIGTERM
    assert released == ["okg-worker-cms"]
    assert handlers[signal.SIGTERM] is signal.SIG_DFL


def test_worker_owner_start_failure_does_not_boot_unreserved(monkeypatch) -> None:
    from okg.substrate.db import capacity
    from okg.substrate.runtime import engine

    monkeypatch.setattr(capacity, "admit_process_demand", lambda *_a, **_kw: {})
    monkeypatch.setattr(
        capacity,
        "reserve_process_demand",
        lambda *_a, **_kw: (_ for _ in ()).throw(
            capacity.ReservationLifecycleError("owner did not start")
        ),
    )

    with pytest.raises(capacity.ReservationLifecycleError, match="did not start"):
        engine._admit_connection_demand("postgresql://not-used", "cms")


def test_short_lived_dbos_context_releases_after_setup_failure(
    monkeypatch,
) -> None:
    import dbos

    from okg.substrate.runtime import engine
    from okg.substrate.runtime.config import RuntimeConfig

    released: list[str] = []
    monkeypatch.setattr(engine, "defensive_destroy", lambda: None)
    monkeypatch.setattr(
        engine,
        "build_dbos_config",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("config failed")),
    )
    monkeypatch.setattr(dbos.DBOS, "destroy", lambda: None)
    monkeypatch.setattr(
        engine,
        "release_connection_demand",
        released.append,
    )

    with pytest.raises(RuntimeError, match="config failed"):
        with engine.with_dbos_singleton(
            deployment="cms",
            dsn="postgresql://not-used",
            runtime=RuntimeConfig(enabled=True),
        ):
            pytest.fail("setup failure must not enter the context")

    assert released == ["cms"]


def test_reset_releases_after_operation_failure(monkeypatch) -> None:
    from okg.substrate.runtime import dev, engine
    from okg.substrate.runtime.config import RuntimeConfig

    released: list[str] = []

    class FakeDBOS:
        def __init__(self, *, config) -> None:
            self.config = config

        @staticmethod
        def reset_system_database() -> None:
            raise RuntimeError("reset failed")

        @staticmethod
        def destroy() -> None:
            return None

    monkeypatch.setitem(sys.modules, "dbos", SimpleNamespace(DBOS=FakeDBOS))
    monkeypatch.setattr(engine, "defensive_destroy", lambda: None)
    monkeypatch.setattr(dev, "build_dbos_config", lambda **_kwargs: {})
    monkeypatch.setattr(dev, "release_connection_demand", released.append)

    with pytest.raises(RuntimeError, match="reset failed"):
        dev.reset_dbos(
            deployment="cms",
            dsn="postgresql://not-used",
            runtime=RuntimeConfig(enabled=True),
            confirm=True,
        )

    assert released == ["cms"]


def test_parent_recovery_probes_use_releasing_context() -> None:
    from okg.substrate.runtime import recovery_test

    kill9_source = inspect.getsource(recovery_test.measure_kill9_recovery)
    sigstop_source = inspect.getsource(recovery_test.measure_sigstop_behavior)

    assert "with with_dbos_singleton(" in kill9_source
    assert "with with_dbos_singleton(" in sigstop_source
