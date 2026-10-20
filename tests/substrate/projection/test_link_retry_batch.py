"""Batched link_retry miss recording (PACT lane-sg-link-retry-batch).

``record_link_retry_misses`` used to commit once per miss: under the
autocommit connection ``okg run --once`` gives the publisher, a re-extraction
of 1.9 million unresolved mentions paid 1.9 million WAL flushes. It now
records the misses in batches, one transaction per batch, and leaves a row
whose readiness did not change untouched. These tests pin the batch
boundaries, the no-rewrite rule, that an alias landing during a recording is
never lost, and that a recorder, a waker and a deleter running together do
not deadlock.
"""
from __future__ import annotations

import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import psycopg
import pytest

from okg.substrate.catalog.aliases import add_alias
from okg.substrate.projection.retry import (
    LinkRetryKey,
    acknowledge_link_retries,
    link_retry_alias_lock_stripe,
    mark_link_retries_ready_for_aliases,
    record_link_retry_misses,
)

pytestmark = pytest.mark.postgres


def _seed_catalog(conn: psycopg.Connection) -> None:
    conn.execute(
        "INSERT INTO okg.catalog_versions (status, applied_at) VALUES ('applied', now())"
    )


def _key(value: str, node: str = "page:one") -> LinkRetryKey:
    return LinkRetryKey("test", "default", node, "concept", value)


def _sorted_like_recorder(keys: list[LinkRetryKey]) -> list[LinkRetryKey]:
    return sorted(set(keys), key=lambda k: (link_retry_alias_lock_stripe(k.entity_value), k))


def _rows(conn: psycopg.Connection) -> dict[LinkRetryKey, tuple[str, bool, object, object]]:
    """key -> (xmin, ready, ready_at, recorded_at)."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT deployment_name, branch_name, node_id, entity_type, entity_value, "
            "xmin::text, ready_at IS NOT NULL, ready_at, recorded_at FROM okg.link_retry"
        )
        return {
            LinkRetryKey(*map(str, row[:5])): (str(row[5]), bool(row[6]), row[7], row[8])
            for row in cur.fetchall()
        }


def _wait_for_lock_wait(watcher: psycopg.Connection, pid: int, future, timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if future.done():
            error = future.exception()
            if error is not None:
                raise error
            raise AssertionError("the other session finished without waiting on the lock")
        with watcher.cursor() as cur:
            cur.execute("SELECT wait_event_type FROM pg_stat_activity WHERE pid = %s", (pid,))
            row = cur.fetchone()
        if row and row[0] == "Lock":
            return
        time.sleep(0.05)
    raise AssertionError("the other session never waited on a lock")


# --------------------------------------------------------------------------- #
# Batch boundaries                                                            #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(("count", "batch_size", "batches"), [
    (7, 3, 3),   # 3 + 3 + 1
    (6, 3, 2),   # exactly two full batches, no empty third
    (7, 1, 7),
    (7, 7, 1),
    (7, 100, 1),
])
def test_each_batch_is_one_transaction(
    clean_runtime: str, count: int, batch_size: int, batches: int,
) -> None:
    keys = [_key(f"value-{i}") for i in range(count)]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog(conn)
        record_link_retry_misses(conn, keys + keys[:2], batch_size=batch_size)
        rows = _rows(conn)
    assert set(rows) == set(keys), "every distinct miss is recorded once"
    ordered = _sorted_like_recorder(keys)
    # Rows written by one transaction share its xmin; consecutive batches of
    # the stripe-sorted misses are separate transactions.
    groups = [ordered[start:start + batch_size] for start in range(0, count, batch_size)]
    assert len(groups) == batches
    group_xmins = [{rows[k][0] for k in group} for group in groups]
    assert all(len(xmins) == 1 for xmins in group_xmins), group_xmins
    assert len({next(iter(xmins)) for xmins in group_xmins}) == batches


def test_an_empty_batch_size_is_refused(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with pytest.raises(ValueError, match="batch_size must be at least 1"):
            record_link_retry_misses(conn, [_key("value")], batch_size=0)
        assert _rows(conn) == {}


def test_no_misses_records_nothing(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        record_link_retry_misses(conn, [])
        assert _rows(conn) == {}


# --------------------------------------------------------------------------- #
# Unchanged rows are not rewritten                                            #
# --------------------------------------------------------------------------- #


def test_an_unchanged_row_is_not_rewritten(clean_runtime: str) -> None:
    keys = [_key(f"value-{i}") for i in range(5)]
    flipped = keys[2]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog(conn)
        record_link_retry_misses(conn, keys, batch_size=2)
        first = _rows(conn)
        assert all(not ready for _x, ready, _r, _t in first.values())

        record_link_retry_misses(conn, keys, batch_size=2)
        assert _rows(conn) == first, "same key, same readiness: no new row version"

        # An alias for one value lands without the waker (a bare insert), so
        # only this recording can make its row ready.
        conn.execute(
            "INSERT INTO okg.aliases (alias, canonical, scope, catalog_version_id) "
            "SELECT %s, 'concept:x', NULL, max(catalog_version_id) "
            "FROM okg.catalog_versions WHERE status = 'applied'",
            (flipped.entity_value,),
        )
        record_link_retry_misses(conn, keys, batch_size=2)
        second = _rows(conn)
        assert second[flipped][1] is True
        assert second[flipped][0] != first[flipped][0], "a readiness change is written"
        assert {k: v for k, v in second.items() if k != flipped} == {
            k: v for k, v in first.items() if k != flipped
        }

        # Ready again: the ready row keeps its ready_at, not refreshed to now().
        record_link_retry_misses(conn, keys, batch_size=2)
        assert _rows(conn) == second

        # And back to unready when the route already saw that alias.
        record_link_retry_misses(conn, keys, batch_size=2, seen_by_route=lambda _v, _i: True)
        third = _rows(conn)
        assert third[flipped][1] is False
        assert third[flipped][0] != second[flipped][0]


# --------------------------------------------------------------------------- #
# An alias landing during a recording is never lost                           #
# --------------------------------------------------------------------------- #


def _values_around(stripe_floor: int) -> tuple[str, list[str], list[str]]:
    """A target value and values in strictly lower and higher stripes."""
    candidates = [f"value-{i}" for i in range(5000)]
    target = next(v for v in candidates if stripe_floor <= link_retry_alias_lock_stripe(v) < 200)
    stripe = link_retry_alias_lock_stripe(target)
    lower = [v for v in candidates if link_retry_alias_lock_stripe(v) < stripe][:4]
    higher = [v for v in candidates if link_retry_alias_lock_stripe(v) > stripe][:3]
    assert len(lower) == 4 and len(higher) == 3
    return target, lower, higher


def test_an_alias_landing_while_a_batch_waits_is_seen_by_the_recheck(
    clean_runtime: str,
) -> None:
    """The waker lands the alias and holds its stripe; the recorder commits
    the batches before it, waits on that stripe, then rechecks the aliases
    with a fresh snapshot and records the row ready. Failure case: an alias
    read taken before the lock (or in the waiting statement's snapshot)
    records it unready after the waker already ran, and the wakeup is lost."""
    target, lower, higher = _values_around(64)
    keys = [_key(v) for v in lower + [target] + higher]
    ordered = _sorted_like_recorder(keys)
    batch_size = 2
    target_batch_start = (ordered.index(_key(target)) // batch_size) * batch_size
    assert target_batch_start >= 2, "at least one batch commits before the target's"
    with psycopg.connect(clean_runtime, autocommit=True) as setup:
        _seed_catalog(setup)

    pool = ThreadPoolExecutor(max_workers=1)
    recorder = psycopg.connect(clean_runtime, autocommit=True)
    try:
        with psycopg.connect(clean_runtime) as waker, \
                psycopg.connect(clean_runtime, autocommit=True) as watcher:
            # The alias lands in the waker's open transaction, which holds the
            # target's stripe lock (add_alias wakes through
            # mark_link_retries_ready_for_aliases).
            assert add_alias(waker, alias=target, canonical="concept:x", scope=None)["landed"]
            future = pool.submit(
                record_link_retry_misses, recorder, keys, batch_size=batch_size,
            )
            _wait_for_lock_wait(watcher, recorder.pgconn.backend_pid, future)
            # The batches before the target's are committed while it waits:
            # a landing holds up at most the batch that needs its stripe.
            assert set(_rows(watcher)) == set(ordered[:target_batch_start])
            waker.commit()
        future.result(timeout=15)
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            ready = {k: v[1] for k, v in _rows(conn).items()}
        assert ready == {k: k.entity_value == target for k in keys}
    finally:
        pool.shutdown(wait=False, cancel_futures=True)
        recorder.close()


def test_a_waker_that_waits_for_a_recording_batch_marks_its_row_ready(
    clean_runtime: str,
) -> None:
    """The other order: the recorder's batches, inside a caller's open
    transaction, hold the stripe; the alias lands and the waker waits; once
    the recorder commits, the waker's UPDATE sees the unready row and marks
    it ready."""
    target, lower, higher = _values_around(64)
    keys = [_key(v) for v in lower + [target] + higher]
    with psycopg.connect(clean_runtime, autocommit=True) as setup:
        _seed_catalog(setup)
    pool = ThreadPoolExecutor(max_workers=1)
    waker = psycopg.connect(clean_runtime, autocommit=True)
    try:
        with psycopg.connect(clean_runtime) as recorder, \
                psycopg.connect(clean_runtime, autocommit=True) as watcher:
            recorder.execute("SELECT 1")  # open the caller's transaction
            record_link_retry_misses(recorder, keys, batch_size=2)
            assert _rows(watcher) == {}, "the batches are savepoints of the open transaction"
            future = pool.submit(
                add_alias, waker, alias=target, canonical="concept:x", scope=None,
            )
            _wait_for_lock_wait(watcher, waker.pgconn.backend_pid, future)
            recorder.commit()
        assert future.result(timeout=15)["landed"]
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            ready = {k: v[1] for k, v in _rows(conn).items()}
        assert ready == {k: k.entity_value == target for k in keys}
    finally:
        pool.shutdown(wait=False, cancel_futures=True)
        waker.close()


# --------------------------------------------------------------------------- #
# No deadlock with a concurrent waker and deleter                             #
# --------------------------------------------------------------------------- #


def test_recorder_waker_and_deleter_do_not_deadlock(clean_runtime: str) -> None:
    """A recorder whose caller's transaction spans many batches (it keeps
    every stripe and row lock to the end), a waker and a deleter, each
    working on random subsets across all stripes, run together. Every side
    takes its stripes in ascending order before touching a row, so no round
    fails with a deadlock."""
    values = [f"value-{i}" for i in range(600)]
    keys = [_key(v, node=f"page:{i % 7}") for i, v in enumerate(values)]
    with psycopg.connect(clean_runtime, autocommit=True) as setup:
        _seed_catalog(setup)
    rounds = 12
    errors: list[BaseException] = []
    start = threading.Barrier(3)

    def recorder() -> None:
        rng = random.Random(1)
        with psycopg.connect(clean_runtime) as conn:
            start.wait()
            for _ in range(rounds):
                with conn.transaction():
                    record_link_retry_misses(conn, rng.sample(keys, 400), batch_size=16)

    def waker() -> None:
        rng = random.Random(2)
        with psycopg.connect(clean_runtime) as conn:
            start.wait()
            for _ in range(rounds):
                with conn.transaction():
                    mark_link_retries_ready_for_aliases(conn, rng.sample(values, 300))

    def deleter() -> None:
        rng = random.Random(3)
        with psycopg.connect(clean_runtime) as conn:
            start.wait()
            for _ in range(rounds):
                with conn.transaction():
                    acknowledge_link_retries(conn, rng.sample(keys, 300))

    def run(fn) -> None:
        try:
            fn()
        except BaseException as error:  # noqa: BLE001 -- reported below
            errors.append(error)

    threads = [threading.Thread(target=run, args=(fn,)) for fn in (recorder, waker, deleter)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=120)
    assert not any(thread.is_alive() for thread in threads), "a session hung"
    assert errors == []
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert set(_rows(conn)) <= set(keys)
