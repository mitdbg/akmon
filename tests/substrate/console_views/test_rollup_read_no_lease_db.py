"""A maintained-rollup read must write nothing (issue #2202).

Every console count read -- the Sources page, the ontology sizing, records by
type -- is a read of a maintained rollup AS-OF a generation checkpoint. Those
reads used to go through `ChronosGraphStore.checkout_generation`, which takes
a generation read LEASE before it checks the checkpoint out: it deletes up to
1,000 expired rows from `okg.history_generation_pins` and inserts a pin. So a
page load wrote to the database, and a read-only session was refused with
`cannot execute DELETE in a read-only transaction`.

A lease protects a long, multi-query historical session from history
compaction. A rollup read is ONE statement. Compaction commits its tombstone
(and removes the checkpoint row) before it deletes a single interval row, so
re-checking the tombstone after the statement proves that the rows it read
were intact. These tests pin both halves: the read writes nothing, and it
still refuses a compacted generation -- including one retired between the
statement and the re-check.
"""
from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager

import psycopg
import pytest

from okg.substrate.console_views import console_count_rollup_read as rollup_read
from okg.substrate.console_views.console_count_rollup_read import (
    CONSOLE_COUNT_ROLLUP_TABLE,
    ConsoleCountRollupCache,
    ConsoleCountRollupUnavailable,
    read_console_count_rollup,
    read_console_count_subtype_rows,
    read_producer_emission,
)
from okg.substrate.derived_artifacts.maintained_derivation import is_sentinel_row
from okg.substrate.derived_artifacts.maintained_derivation_constants import (
    COUNT_KIND_SOURCE,
    COUNT_KIND_SUBTYPE,
)
from okg.substrate.errors import GenerationCompacted
from okg.substrate.graph_store import DEFAULT_BRANCH, ChronosGraphStore
from okg.substrate.history_compaction import ensure_history_compaction_schema
from okg.substrate.publisher.console_count_rollup import (
    maintain_console_count_rollup,
)

pytestmark = pytest.mark.postgres

_NODES = [
    {"node_id": "n:a1", "subtype": "alpha", "source": "srcA"},
    {"node_id": "n:a2", "subtype": "alpha", "source": "srcA"},
    {"node_id": "n:b1", "subtype": "beta", "source": "srcA"},
    {"node_id": "n:c1", "subtype": "alpha", "source": "srcB"},
]
_EDGES = [
    {"edge_id": 1, "src": "n:a1", "dst": "n:a2", "edge_type": "rel_one"},
]


def _publish(dsn: str) -> str:
    """Seed a graph, maintain its count rollup, and checkpoint a generation."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
        # The pin table must exist: without it the lease path writes nothing
        # and these tests could not tell the two paths apart.
        ensure_history_compaction_schema(conn)
        session = store.checkout(DEFAULT_BRANCH)
        with session.transaction():
            session.upsert_rows("okg.graph_nodes", _NODES)
            session.upsert_rows("okg.graph_edges", _EDGES)
        maintain_console_count_rollup(
            conn,
            generation_id=1,
            changed_node_ids=[],
            changed_edge_ids=[],
            prior_generation_checkpoint=None,
            force_full=True,
        )
        return store.checkpoint_generation(
            branch=DEFAULT_BRANCH,
            deployment_name="default",
            status="published",
            catalog_version_id=1,
            metadata={},
        )


@contextmanager
def _read_only(dsn: str) -> Iterator[psycopg.Connection]:
    """A session PostgreSQL itself refuses to let write."""
    with psycopg.connect(
        dsn, autocommit=True, options="-c default_transaction_read_only=on"
    ) as conn:
        yield conn


def _pin_rows(dsn: str) -> list[tuple]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        return conn.execute(
            "SELECT deployment_name, branch_id, pin_name, checkpoint_id "
            "FROM okg.history_generation_pins ORDER BY pin_name"
        ).fetchall()


def _tombstone(dsn: str, checkpoint_id: str) -> None:
    """Retire a checkpoint the way compaction's barrier starts: tombstone first."""
    plan_id = str(uuid.uuid4())
    with psycopg.connect(dsn, autocommit=True) as conn, conn.transaction():
        conn.execute(
            "INSERT INTO okg.history_compaction_plans "
            "(plan_id, deployment_name, chronos_branch_id, status, plan) "
            "VALUES (%s, 'default', %s, 'applying', '{}'::jsonb)",
            (plan_id, DEFAULT_BRANCH),
        )
        conn.execute(
            "INSERT INTO okg.compacted_generations "
            "(checkpoint_id, deployment_name, branch_id, plan_id) "
            "VALUES (%s, 'default', %s, %s)",
            (checkpoint_id, DEFAULT_BRANCH, plan_id),
        )


def test_every_rollup_read_shape_succeeds_on_a_read_only_session(
    clean_runtime: str,
) -> None:
    """Each read shape the console uses, on a session that cannot write.

    Before the fix each of these raised: PostgreSQL refuses the lease write in
    a read-only transaction.
    """
    generation = _publish(clean_runtime)
    pins_before = _pin_rows(clean_runtime)

    with _read_only(clean_runtime) as conn:
        everything = read_console_count_rollup(conn, generation_id=generation)
        sources = read_console_count_rollup(
            conn, generation_id=generation, kinds={COUNT_KIND_SOURCE}
        )
        subtype_rows = read_console_count_subtype_rows(
            conn, generation_id=generation
        )
        emission = read_producer_emission(conn, generation_id=generation)
        cache = ConsoleCountRollupCache(conn, generation_id=generation)
        cache.prime({COUNT_KIND_SUBTYPE})
        cached = cache.read({COUNT_KIND_SOURCE})

    assert sources == {
        (COUNT_KIND_SOURCE, "srcA"): 3,
        (COUNT_KIND_SOURCE, "srcB"): 1,
    }
    assert {
        key: count for key, count in everything.items()
        if key[0] == COUNT_KIND_SOURCE
    } == sources
    assert sorted((r["subtype"], r["c"]) for r in subtype_rows) == [
        ("alpha", 3),
        ("beta", 1),
    ]
    assert ("srcA", "alpha", 2) in emission.rows
    assert emission.dimension_available
    assert cached == sources
    assert cache.reads == 1
    # Nothing was written: not a pin, not a sweep of expired pins.
    assert _pin_rows(clean_runtime) == pins_before


def test_the_read_returns_exactly_what_the_leased_checkout_returns(
    clean_runtime: str,
) -> None:
    """Dropping the lease changes what is written, never what is read."""
    generation = _publish(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        leased = store.checkout_generation(generation).query(
            "SELECT dimension_kind, dimension_value, item_count "
            f"FROM {CONSOLE_COUNT_ROLLUP_TABLE}"
        )
    expected = {
        (str(r["dimension_kind"]), str(r["dimension_value"])): int(r["item_count"])
        for r in leased
        if not is_sentinel_row(r, rollup_read._COUNT_SENTINEL)
    }
    assert expected

    with _read_only(clean_runtime) as conn:
        got = read_console_count_rollup(conn, generation_id=generation)
        filtered = read_console_count_rollup(
            conn, generation_id=generation, kinds={COUNT_KIND_SOURCE}
        )

    assert got == expected
    assert filtered == {
        key: count for key, count in expected.items()
        if key[0] == COUNT_KIND_SOURCE
    }


def test_a_compacted_generation_is_refused_not_read(clean_runtime: str) -> None:
    generation = _publish(clean_runtime)
    _tombstone(clean_runtime, generation)

    with _read_only(clean_runtime) as conn, pytest.raises(GenerationCompacted):
        read_console_count_rollup(
            conn, generation_id=generation, kinds={COUNT_KIND_SOURCE}
        )


def test_compaction_committed_after_the_statement_is_refused(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The race a lease used to cover: compaction retires the generation while
    the read is in flight. Without a lease, the tombstone re-check after the
    statement must catch it -- the rows must not be served."""
    from chronos_core.branching import _runtime as chronos_runtime

    generation = _publish(clean_runtime)
    original = chronos_runtime.BranchSession.query
    fired: list[str] = []

    def query_then_compact(self, sql, params=None):
        rows = original(self, sql, params)
        if CONSOLE_COUNT_ROLLUP_TABLE in sql and not fired:
            fired.append(sql)
            _tombstone(clean_runtime, generation)
        return rows

    monkeypatch.setattr(chronos_runtime.BranchSession, "query", query_then_compact)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with pytest.raises(GenerationCompacted):
            read_console_count_rollup(
                conn, generation_id=generation, kinds={COUNT_KIND_SOURCE}
            )
    assert fired, "the rollup statement never ran"


def test_an_absent_checkpoint_still_fails_loud(clean_runtime: str) -> None:
    _publish(clean_runtime)
    with _read_only(clean_runtime) as conn:
        with pytest.raises(ConsoleCountRollupUnavailable) as caught:
            read_console_count_rollup(
                conn, generation_id="generation:999999", kinds={COUNT_KIND_SOURCE}
            )
    assert caught.value.reason == "checkpoint_absent"


def test_an_unregistered_table_still_reads_as_unregistered(
    clean_runtime: str,
) -> None:
    """The statement's own failure keeps its reason; only a missing
    checkpoint is `checkpoint_absent`."""
    from okg.substrate.derived_artifacts.maintained_derivation import (
        MaintainedDerivationUnavailable,
        read_maintained_rows,
    )

    generation = _publish(clean_runtime)
    with _read_only(clean_runtime) as conn:
        with pytest.raises(MaintainedDerivationUnavailable) as caught:
            read_maintained_rows(
                conn,
                table="okg.no_such_maintained_table",
                generation_id=generation,
                select_columns=("dimension_kind",),
                sentinel_spec=rollup_read._COUNT_SENTINEL,
                row_mapper=dict,
            )
    assert caught.value.reason == "table_unregistered"
