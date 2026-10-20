"""okg#1718: folding a whole generation into the interval table is linear.

A full reanchor (the first publish and the every-20th cadence) and the full
interval sync fold a generation that was written moments earlier. PostgreSQL
has not analyzed those rows yet, so it estimates one row for the new
generation. The old statements then probed the snapshot table once per open
interval and read the whole generation on every probe, and the first fold
re-scanned the interval table once per row it inserted. Both are quadratic:
102 s for one statement at 20k targets, and past the 900 s statement timeout
at 300k.

These tests reproduce the stale statistics on purpose (the snapshot table is
analyzed while it holds only an older generation) and then count the blocks
each fold requests inside its own transaction: from the two shared tables, and
from every temporary table or index the fold creates on the way (a fold that
stages rows into a temp table and then scans that table once per interval is
just as quadratic). The count does not depend on timing: a linear fold asks
for a small constant number of blocks per target, the quadratic plans ask for
hundreds.
"""
from __future__ import annotations

import uuid
from collections.abc import Iterator

import pytest

psycopg = pytest.importorskip("psycopg")
from okg.substrate.attributes import (  # noqa: E402
    ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE,
    ATTRIBUTE_EFFECTIVE_VIEW_TABLE,
    EFFECTIVE_INTERVAL_OPEN_ORD,
    ensure_attribute_schema,
    reanchor_effective_interval_view_for_generation,
    sync_effective_interval_view_for_generation,
)
from okg.substrate.db.connection import connect as db_connect  # noqa: E402
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402

pytestmark = pytest.mark.postgres

# Targets per generation. Large enough that every quadratic plan measured so
# far requests several times the bound, small enough to run in seconds.
_TARGETS = 6000
# Measured on the fixed code at 4,000 targets: at most 3.1 shared heap blocks
# and 6.7 temp-relation blocks per target per fold, independent of size. The
# quadratic plans grow with size; at 4,000 they measured 44 to 700 shared
# blocks per target on dev, and 31 temp blocks per target when the staged
# table lost its unique index.
_MAX_BLOCKS_PER_TARGET = 12

_HEAP_TABLES = (ATTRIBUTE_EFFECTIVE_VIEW_TABLE, ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE)


def _db(conn):
    return ChronosGraphStore.from_connection(conn, ensure_schema=False).context.db


def _register_generations(cur, branch: str, count: int) -> list[str]:
    generations = [f"generation:{branch}:g{ordv}" for ordv in range(count)]
    for ordv, generation in enumerate(generations):
        cur.execute(
            "INSERT INTO okg.generation_ordinals "
            "(checkpoint_id, branch_id, ordinal) VALUES (%s, %s, %s)",
            (generation, branch, ordv),
        )
    return generations


def _write_generation(
    cur, generation: str, *, suffix: str, changed_below: int, value: str
) -> None:
    """Write a full node snapshot of ``_TARGETS`` rows for ``generation``.

    Targets numbered below ``changed_below`` carry ``value``; the rest carry
    the constant ``"v"``, so consecutive generations differ in a known slice.
    """
    cur.execute(
        f"""
        WITH src AS (
          SELECT 'doc:' || g || ':' || %(sfx)s AS target_id,
                 jsonb_build_object(
                   'k', CASE WHEN g < %(changed)s THEN %(value)s ELSE 'v' END
                 ) AS attrs
            FROM generate_series(0, %(n)s - 1) AS g
        ), hashed AS (
          SELECT target_id, attrs,
                 okg.attribute_content_hash(attrs, NULL::jsonb) AS attrs_hash
            FROM src
        ), versions AS (
          INSERT INTO okg.attribute_versions
            (target_kind, target_id, attrs_hash, attrs, raw_attrs)
          SELECT 'node', target_id, attrs_hash, attrs, NULL FROM hashed
          ON CONFLICT (target_kind, target_id, attrs_hash) DO NOTHING
        )
        INSERT INTO {ATTRIBUTE_EFFECTIVE_VIEW_TABLE}
          (target_kind, target_id, generation_id, namespace, subtype,
           overlay_count, attrs_hash)
        SELECT 'node', target_id, %(gen)s, 'default', 'doc', 0, attrs_hash
          FROM hashed
        """,
        {
            "sfx": suffix,
            "changed": changed_below,
            "value": value,
            "n": _TARGETS,
            "gen": generation,
        },
    )


def _heap_blocks_requested(cur) -> int:
    """Blocks this transaction has requested from the two heaps so far."""
    cur.execute(
        "SELECT sum(pg_stat_get_xact_blocks_fetched(to_regclass(t)))::bigint "
        "FROM unnest(%s::text[]) AS t",
        (list(_HEAP_TABLES),),
    )
    return int(cur.fetchone()[0] or 0)


def _temp_blocks_by_relation(conn) -> dict[int, int]:
    """Blocks this transaction has requested from each live temp relation.

    Covers the session's temp tables and their indexes. A temp relation's
    counters disappear when it is dropped, so the tracking cursor below reads
    them before every statement, including the ``DROP``.
    """
    with psycopg.Cursor(conn) as cur:
        cur.execute(
            "SELECT oid::bigint, pg_stat_get_xact_blocks_fetched(oid) "
            "FROM pg_class WHERE relpersistence = 't' AND relkind IN ('r', 'i')"
        )
        return {int(oid): int(n or 0) for oid, n in cur.fetchall()}


class _TempBlockTracker:
    """Keeps the highest block count seen for every temp relation."""

    def __init__(self) -> None:
        self.seen: dict[int, int] = {}

    def sample(self, conn) -> None:
        for oid, blocks in _temp_blocks_by_relation(conn).items():
            self.seen[oid] = max(blocks, self.seen.get(oid, 0))

    def total(self, baseline: dict[int, int]) -> int:
        return sum(n - baseline.get(oid, 0) for oid, n in self.seen.items())


def _tracking_cursor_factory(tracker: _TempBlockTracker):
    class _TrackingCursor(psycopg.Cursor):
        def execute(self, query, params=None, **kwargs):
            tracker.sample(self.connection)
            return super().execute(query, params, **kwargs)

    return _TrackingCursor


def _measured(conn, fold) -> tuple[dict, dict[str, int]]:
    """Run ``fold`` inside one transaction.

    Returns its result and the blocks it requested: ``shared`` from the two
    shared heaps, ``temp`` from temp tables and indexes.
    """
    tracker = _TempBlockTracker()
    with conn.cursor() as cur:
        before = _heap_blocks_requested(cur)
    baseline = _temp_blocks_by_relation(conn)
    previous_factory = conn.cursor_factory
    conn.cursor_factory = _tracking_cursor_factory(tracker)
    try:
        result = fold()
    finally:
        conn.cursor_factory = previous_factory
    tracker.sample(conn)
    with conn.cursor() as cur:
        after = _heap_blocks_requested(cur)
    conn.commit()
    return result, {"shared": after - before, "temp": tracker.total(baseline)}


def _open_intervals(conn, branch: str) -> dict[int, int]:
    """from_ord -> count of OPEN intervals on ``branch``."""
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT from_ord, count(*) FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
            "WHERE branch_id = %s AND to_ord = %s GROUP BY from_ord",
            (branch, EFFECTIVE_INTERVAL_OPEN_ORD),
        )
        return {int(from_ord): int(n) for from_ord, n in cur.fetchall()}



@pytest.fixture
def autovacuum_paused(clean_runtime: str) -> Iterator[str]:
    """Keep autovacuum from analyzing the two shared tables mid-test.

    The test depends on statistics that do not know the folded generations.
    An autovacuum ANALYZE landing between the setup and a fold would hand the
    old plans correct estimates and let a regression pass.
    """
    with db_connect(clean_runtime, autocommit=True) as conn:
        ensure_attribute_schema(conn)
        for table in _HEAP_TABLES:
            conn.execute(f"ALTER TABLE {table} SET (autovacuum_enabled = false)")
    try:
        yield clean_runtime
    finally:
        with db_connect(clean_runtime, autocommit=True) as conn:
            for table in _HEAP_TABLES:
                conn.execute(f"ALTER TABLE {table} RESET (autovacuum_enabled)")


def test_full_folds_stay_linear_when_statistics_are_stale(
    autovacuum_paused: str,
) -> None:
    clean_runtime = autovacuum_paused
    suffix = uuid.uuid4().hex[:8]
    first_branch = f"fold_first_{suffix}"
    branch = f"fold_scan_{suffix}"
    changed = _TARGETS // 10
    bound = _MAX_BLOCKS_PER_TARGET * _TARGETS
    with db_connect(clean_runtime, autocommit=True) as setup:
        ensure_attribute_schema(setup)
        with setup.cursor() as cur:
            (a0,) = _register_generations(cur, first_branch, 1)
            b0, b1, b2 = _register_generations(cur, branch, 3)
            # An older generation the statistics will describe. Every folded
            # generation is written AFTER this ANALYZE, which is the state a
            # publish is in: its generation is seconds old.
            _write_generation(
                cur, f"generation:{branch}:prior", suffix=suffix,
                changed_below=0, value="v",
            )
            cur.execute(f"ANALYZE {ATTRIBUTE_EFFECTIVE_VIEW_TABLE}")
            cur.execute(f"ANALYZE {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE}")
            for generation in (a0, b0):
                _write_generation(
                    cur, generation, suffix=suffix, changed_below=0, value="v"
                )
            _write_generation(
                cur, b1, suffix=suffix, changed_below=changed, value="b1"
            )
            _write_generation(
                cur, b2, suffix=suffix, changed_below=changed, value="b2"
            )

    blocks: dict[str, dict[str, int]] = {}
    with db_connect(clean_runtime) as conn:
        db = _db(conn)
        # 1. The first publish: every target opens into an empty front.
        first, blocks["first_fold_into_empty_front"] = _measured(
            conn,
            lambda: reanchor_effective_interval_view_for_generation(
                conn, db, generation_id=a0
            ),
        )
        assert first["counts"]["node"]["opened"] == _TARGETS, first
        assert _open_intervals(conn, first_branch) == {0: _TARGETS}
        # The interval statistics now describe one populated branch, not the
        # one folded next, so the planner expects that branch's front empty.
        with conn.cursor() as cur:
            cur.execute(f"ANALYZE {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE}")
        conn.commit()
        _, blocks["first_fold_beside_other_branch"] = _measured(
            conn,
            lambda: reanchor_effective_interval_view_for_generation(
                conn, db, generation_id=b0
            ),
        )
        # 2. A cadence reanchor: most targets unchanged, one tenth changed.
        second, blocks["cadence_reanchor"] = _measured(
            conn,
            lambda: reanchor_effective_interval_view_for_generation(
                conn, db, generation_id=b1
            ),
        )
        assert second["counts"]["node"] == {
            "deleted_current": 0,
            "closed": changed,
            "opened": changed,
        }, second
        assert _open_intervals(conn, branch) == {
            0: _TARGETS - changed,
            1: changed,
        }
        # 3. The full sync (no dirty scope) over the same shape.
        third, blocks["full_sync"] = _measured(
            conn,
            lambda: sync_effective_interval_view_for_generation(
                conn, db, generation_id=b2
            ),
        )
        assert third["counts"]["node"] == {"closed": changed, "opened": changed}
        assert _open_intervals(conn, branch) == {
            0: _TARGETS - changed,
            2: changed,
        }

    over = {
        f"{fold}.{kind}": count
        for fold, counts in blocks.items()
        for kind, count in counts.items()
        if count > bound
    }
    assert not over, (
        f"a full fold requested more than {_MAX_BLOCKS_PER_TARGET} blocks per "
        f"target ({bound} for {_TARGETS} targets) from {sorted(over)}: {blocks}. "
        "It is probing rows once per interval instead of looking them up; "
        "see okg#1718."
    )
