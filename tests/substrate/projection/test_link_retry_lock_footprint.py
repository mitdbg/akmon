"""The alias-landing advisory lock has a bounded footprint (fix-link-retry-alias-lock-footprint).

Transaction advisory locks live in the server-wide lock table, which holds
``max_locks_per_transaction * (max_connections + max_prepared_transactions)``
entries (6,400 by default). ``mark_link_retries_ready_for_aliases`` used to
take one lock per landed alias value and hold it to commit, so a catalog apply
landing more values than the table holds died with ``out of shared memory``
and so did whatever else was running. Both sides of that lock now meet on a
stripe derived from the value, so the footprint is bounded and a landing and a
recording of the same value still exclude each other.
"""
from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor

import psycopg
import pytest

from okg.substrate.projection.retry import (
    LINK_RETRY_ALIAS_LOCK_NAMESPACE,
    LINK_RETRY_ALIAS_LOCK_STRIPES,
    LinkRetryKey,
    link_retry_alias_lock_stripe,
    mark_link_retries_ready_for_aliases,
    record_link_retry_misses,
)


pytestmark = pytest.mark.postgres


def _advisory_locks(watcher: psycopg.Connection, pid: int) -> list[tuple[int, int]]:
    with watcher.cursor() as cur:
        cur.execute(
            """
            SELECT classid::bigint, objid::bigint
              FROM pg_locks
             WHERE pid = %s AND locktype = 'advisory' AND granted
             ORDER BY 1, 2
            """,
            (pid,),
        )
        return [(int(a), int(b)) for a, b in cur.fetchall()]


def test_waking_many_aliases_holds_at_most_one_lock_per_stripe(clean_runtime: str) -> None:
    values = [f"alias-{i:05d}" for i in range(4 * LINK_RETRY_ALIAS_LOCK_STRIPES)]
    assert len(values) > LINK_RETRY_ALIAS_LOCK_STRIPES, "the batch is larger than the bound"
    with psycopg.connect(clean_runtime) as waker, psycopg.connect(clean_runtime, autocommit=True) as watcher:
        with waker.transaction():
            mark_link_retries_ready_for_aliases(waker, values)
            held = _advisory_locks(watcher, waker.pgconn.backend_pid)
    expected = sorted({link_retry_alias_lock_stripe(v) for v in values})
    assert 0 < len(held) <= LINK_RETRY_ALIAS_LOCK_STRIPES
    assert held == [(LINK_RETRY_ALIAS_LOCK_NAMESPACE, stripe) for stripe in expected]


def test_stripes_are_stable_and_in_range() -> None:
    assert link_retry_alias_lock_stripe("Sample.Alias") == link_retry_alias_lock_stripe("Sample.Alias")
    assert link_retry_alias_lock_stripe("Sample.Alias") != link_retry_alias_lock_stripe("sample.alias")
    stripes = {link_retry_alias_lock_stripe(f"v{i}") for i in range(4000)}
    assert all(0 <= stripe < LINK_RETRY_ALIAS_LOCK_STRIPES for stripe in stripes)
    assert len(stripes) == LINK_RETRY_ALIAS_LOCK_STRIPES


def _wait_for_lock_wait(watcher: psycopg.Connection, pid: int, future, timeout: float = 15.0) -> bool:
    """True once backend ``pid`` waits on a lock; a recorder that finished or
    failed instead surfaces its own outcome rather than a timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if future.done():
            error = future.exception()
            if error is not None:
                raise error
            raise AssertionError("the recorder finished without waiting on the waker")
        with watcher.cursor() as cur:
            cur.execute("SELECT wait_event_type FROM pg_stat_activity WHERE pid = %s", (pid,))
            row = cur.fetchone()
        if row and row[0] == "Lock":
            return True
        time.sleep(0.05)
    return False


def test_a_recorder_waits_for_the_waker_holding_its_value(clean_runtime: str) -> None:
    """Same value, same stripe: the recorder blocks until the landing commits.

    A recorder for a value in a different stripe is not blocked, which is the
    fine-grained behaviour small alias additions keep.
    """
    value = "shared-alias-value"
    stripe = link_retry_alias_lock_stripe(value)
    other = next(
        f"other-{i}" for i in range(10_000) if link_retry_alias_lock_stripe(f"other-{i}") != stripe
    )
    blocked = psycopg.connect(clean_runtime, autocommit=True)
    pool = ThreadPoolExecutor(max_workers=1)
    future = None
    try:
        with psycopg.connect(clean_runtime) as waker, \
                psycopg.connect(clean_runtime, autocommit=True) as watcher:
            with waker.transaction():
                mark_link_retries_ready_for_aliases(waker, [value])

                with psycopg.connect(clean_runtime, autocommit=True) as free:
                    record_link_retry_misses(
                        free, [LinkRetryKey("dep", "default", "node:free", "Code", other)]
                    )
                    with free.cursor() as cur:
                        cur.execute(
                            "SELECT count(*) FROM okg.link_retry WHERE entity_value = %s", (other,)
                        )
                        assert cur.fetchone()[0] == 1, "a different stripe records without waiting"

                # The recorder for the held value must wait on the waker's lock,
                # so it runs in a thread and is only joined after the waker's
                # transaction has left this block (which commits it).
                future = pool.submit(
                    record_link_retry_misses,
                    blocked,
                    [LinkRetryKey("dep", "default", "node:blocked", "Code", value)],
                )
                assert _wait_for_lock_wait(watcher, blocked.pgconn.backend_pid, future), \
                    "the same stripe waits on the waker"
                assert not future.done()
                assert _advisory_locks(watcher, waker.pgconn.backend_pid) == [
                    (LINK_RETRY_ALIAS_LOCK_NAMESPACE, stripe)
                ]
            # The waker committed; the recorder proceeds and records its row.
            future.result(timeout=15)
        with blocked.cursor() as cur:
            cur.execute("SELECT count(*) FROM okg.link_retry WHERE entity_value = %s", (value,))
            assert cur.fetchone()[0] == 1
    finally:
        if future is not None and not future.done():
            future.cancel()
        pool.shutdown(wait=False, cancel_futures=True)
        blocked.close()
