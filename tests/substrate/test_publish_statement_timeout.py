"""harden-okg-ci-runtime / req.publish-resilience.

A runaway query under the publisher's session advisory lock must be aborted by
the publish statement_timeout, and the lock MUST be released afterward so the
next publish cycle can proceed. Pre-fix (okg-ci 2026-06-16) a single chronos
dirty-edge delta query held the lock CPU-bound for 2h26m with no bound, wedging
all publishing for ~5h while starving the source-sync lane.
"""
from __future__ import annotations

from uuid import uuid4

import psycopg
import pytest

from okg.substrate.publisher import Publisher
from okg.substrate.publisher.publisher import (
    DEFAULT_PUBLISH_STATEMENT_TIMEOUT_SECONDS,
    _advisory_keys,
)

pytestmark = pytest.mark.postgres


def _lock_is_free(dsn: str, deployment: str) -> bool:
    """True iff the deployment's publisher advisory lock can be acquired now
    (i.e. it was released). Releases it again so the probe is side-effect-free."""
    classid, objid = _advisory_keys(deployment)
    with psycopg.connect(dsn) as probe:
        got = probe.execute(
            "SELECT pg_try_advisory_lock(%s, %s)", (classid, objid),
        ).fetchone()[0]
        if got:
            probe.execute("SELECT pg_advisory_unlock(%s, %s)", (classid, objid))
        return bool(got)


def test_runaway_query_under_lock_is_bounded_and_lock_released(
    okg_dsn: str,
) -> None:
    # Tiny bound so a 5s sleep is killed in a fraction of a second.
    dep = f"test-bound-publish-{uuid4().hex[:8]}"
    pub = Publisher(
        dsn=okg_dsn, deployment_name=dep, coordination_mode="dbos",
        hard_statement_timeout_ms=200,
    )

    # A statement that exceeds the bound is cancelled, not run forever.
    with pytest.raises(psycopg.errors.QueryCanceled):
        with pub._locked_connection() as conn:
            conn.execute("SELECT pg_sleep(5)")

    # ...and the advisory lock is free afterward (the finally released it),
    # so the next publish cycle's acquirer succeeds immediately.
    assert _lock_is_free(okg_dsn, dep), (
        "publisher advisory lock was NOT released after the statement timeout"
    )


def test_timeout_zero_disables_the_bound(
    okg_dsn: str,
) -> None:
    dep = f"test-bound-publish-off-{uuid4().hex[:8]}"
    pub = Publisher(
        dsn=okg_dsn, deployment_name=dep, coordination_mode="dbos",
        hard_statement_timeout_ms=0,
    )
    assert pub._publish_statement_timeout_ms() == 0

    # Disabled bound: a short query under the lock completes normally, and the
    # lock still releases on exit.
    with pub._locked_connection() as conn:
        assert conn.execute("SELECT 1").fetchone()[0] == 1
    assert _lock_is_free(okg_dsn, dep)


def test_default_timeout_is_finite_and_generous() -> None:
    # No DB needed: Publisher.__init__ does not connect.
    pub = Publisher(dsn="postgres://unused", deployment_name="bound-default")
    ms = pub._publish_statement_timeout_ms()
    assert ms == int(DEFAULT_PUBLISH_STATEMENT_TIMEOUT_SECONDS * 1000)
    # Finite + multi-minute: kills a 2.5h runaway, never a legitimate publish.
    assert 60_000 <= ms <= 3_600_000
