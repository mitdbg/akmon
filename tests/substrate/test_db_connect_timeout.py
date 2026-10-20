"""Tests for the substrate's `connect()` helper's connect_timeout
default + the workflow_timeout pinned in the publisher schedule.

Both are protections against the Phase 0/1 cutover-verification
observation that a `psycopg.connect()` with no timeout could hang
forever, blocking a `ThreadPoolExecutor.__exit__` join, which holds
the publisher's partition-queue slot indefinitely.
"""
from __future__ import annotations

import inspect



def test_connect_default_timeout_is_finite():
    """The substrate's `connect()` helper SHOULD have a finite default
    connect_timeout so a hung server can't block a worker forever."""
    from okg.substrate.db.connection import (
        DEFAULT_CONNECT_TIMEOUT_SECONDS,
        connect,
    )

    assert isinstance(DEFAULT_CONNECT_TIMEOUT_SECONDS, int)
    assert 1 <= DEFAULT_CONNECT_TIMEOUT_SECONDS <= 60, (
        "connect_timeout should be small enough to fail fast but "
        "long enough not to trigger on legitimate cold-start "
        "Postgres servers"
    )

    sig = inspect.signature(connect)
    assert "connect_timeout" in sig.parameters
    default = sig.parameters["connect_timeout"].default
    assert default == DEFAULT_CONNECT_TIMEOUT_SECONDS


def test_connect_passes_timeout_to_psycopg():
    """Source inspection: the helper passes `connect_timeout` through
    to `psycopg.connect`. If a future refactor breaks the wiring, the
    test catches it."""
    from okg.substrate.db import connection

    src = inspect.getsource(connection.connect)
    assert "connect_timeout" in src
    assert "connection_kwargs" in src


def test_pool_passes_timeout_to_psycopg_pool():
    """Source inspection: shared pools inherit the same finite
    connect-timeout/keepalive kwargs as direct connections."""
    from okg.substrate.db import pool

    src = inspect.getsource(pool.get_pool)
    assert "connection_kwargs" in src
    assert "connect_timeout" in src


def test_publisher_workflow_timeout_is_pinned():
    """Operators rely on the workflow timeout being set; pin it."""
    from okg.substrate.runtime.publisher_schedule import (
        PUBLISHER_WORKFLOW_TIMEOUT_SECONDS,
    )

    assert isinstance(PUBLISHER_WORKFLOW_TIMEOUT_SECONDS, float)
    assert PUBLISHER_WORKFLOW_TIMEOUT_SECONDS == 1800.0


def test_publisher_schedule_enqueue_includes_workflow_timeout():
    """Source inspection: the publisher schedule enqueue passes a
    workflow timeout. `publisher_schedule` hands
    `workflow_timeout_seconds` to `make_enqueue_schedule`, and the
    factory translates it into the DBOS `workflow_timeout` enqueue
    payload. The DBOS row will record a deadline; if a cycle blows past
    it, DBOS marks the workflow CANCELLED."""
    from okg.substrate.runtime import _schedule_factory, publisher_schedule

    src = inspect.getsource(publisher_schedule)
    assert "PUBLISHER_WORKFLOW_TIMEOUT_SECONDS" in src
    assert "workflow_timeout_seconds=PUBLISHER_WORKFLOW_TIMEOUT_SECONDS" in src

    factory_src = inspect.getsource(_schedule_factory)
    assert "workflow_timeout=" in factory_src




def test_chronos_graph_store_uses_managed_context():
    """Chronos-native graph writes no longer use projector worker
    connections; pin that the runtime graph path goes through the
    Chronos store boundary."""
    from okg.substrate.graph_store import ChronosGraphStore

    src = inspect.getsource(ChronosGraphStore.connect)
    assert "ChronosBranchContext.connect" in src
    assert "backend" in src
