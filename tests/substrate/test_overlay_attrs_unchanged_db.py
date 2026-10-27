"""The per-generation overlay table is written exactly as before (PostgreSQL).

PACT lane-s-storage-redesign-s2, requirement ``nomos-unchanged``: the table
Nomos reads (``okg.attribute_effective_attrs``) is written exactly as before by
every publish. S2 added a marker write and a ranges sync to the materializer;
a regression there would show first in this table.

PACT lane-s-storage-drop-copies: the per-generation overlay table is still
written exactly as before after the three attribute copies were dropped, and no
publish writes a copy. The golden rows are written by hand from the script's
semantics, not read back from a run:

* g0: published with no overlay.
* an overlay on ``a.path`` recorded at g0, so g1 carries it.
* at g1, ``a.path`` is superseded and ``b.rank`` is added, so g2 carries both.
* ``b.rank`` is rejected after g2, so g3 carries only ``a.path``.
* g4 publishes with nothing changed in the overlays.
"""
from __future__ import annotations

import uuid
from typing import Any

import psycopg
import pytest

from okg.substrate.attributes import (
    ATTRIBUTE_EFFECTIVE_TABLE,
    record_attribute_observation,
    reject_attribute_observation,
)
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import (
    SubtypeSpec,
    bind_test_catalog_writer,
    seed_catalog,
)

pytestmark = pytest.mark.postgres

_PRODUCER = "test:overlay-attrs-unchanged"


class _Graph:
    """Six documents and a clock node; every publish ticks the clock, so each
    one is a real (non-reused) generation."""

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self.suffix = uuid.uuid4().hex[:8]
        self.tick = 0
        self.hashes: dict[str, str] = {}  # observation id -> value hash from the write
        with psycopg.connect(dsn, autocommit=True) as conn:
            seed_catalog(
                conn,
                subtypes=[SubtypeSpec(
                    name="doc", metadata={"filterable_attrs": {"path": "text"}},
                )],
            )
            ChronosGraphStore.from_connection(conn).apply_facts(
                [
                    NodeFact(self.id(name), "doc", {"path": f"{name}.py", "rank": i},
                             {"id": self.id(name)}, {"v": 1})
                    for i, name in enumerate("abcdef")
                ],
                source="default", strict_endpoints=True,
            )

    def id(self, name: str) -> str:
        return f"doc:{name}:{self.suffix}"

    def name(self, target_id: str) -> str:
        return target_id.split(":")[1]

    def publish(self) -> str:
        self.tick += 1
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            ChronosGraphStore.from_connection(conn).apply_facts(
                [NodeFact(self.id("clock"), "doc", {"tick": self.tick},
                          {"id": self.id("clock")}, {"v": self.tick})],
                source="default", strict_endpoints=True,
            )
        out = Publisher(
            self.dsn, "default",
            catalog_identity=bind_test_catalog_writer(self.dsn, "default"),
        ).cycle()
        assert out["status"] == "published", out
        assert not out.get("noop"), out
        return str(out["generation_id"])

    def overlay(self, generation: str, name: str, key: str, value: Any,
                *, supersedes: str | None = None) -> str:
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            observation = record_attribute_observation(
                conn, target_kind="node", target_id=self.id(name),
                generation_id=generation, attr_key=key, value=value,
                producer=_PRODUCER, supersedes=supersedes,
            )
        self.hashes[observation.observation_id] = observation.value_hash
        return observation.observation_id

    def reject(self, observation_id: str) -> None:
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            reject_attribute_observation(conn, observation_id, reason="test")

    def rows(self, generation: str) -> list[tuple]:
        """The generation's overlay rows as (target name, kind, namespace,
        key, value, producer, observation id, value hash), ordered. Timestamps
        are left out: they differ on every run."""
        with psycopg.connect(self.dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT target_id, target_kind, namespace, attr_key, value, producer, "
                "observation_id, value_hash "
                f"FROM {ATTRIBUTE_EFFECTIVE_TABLE} WHERE generation_id = %s "
                "ORDER BY target_id, attr_key",
                (generation,),
            )
            return sorted(
                (self.name(tid), kind, ns, key, value, producer, obs, vh)
                for tid, kind, ns, key, value, producer, obs, vh in cur.fetchall()
            )


def _row(graph: _Graph, name: str, key: str, value: Any, observation: str) -> tuple:
    """The golden row: the observation id comes from the write that created it,
    and the hash from that write's own result, so an equal value recorded by a
    different observation does not match."""
    return (name, "node", "default", key, value, _PRODUCER, observation,
            graph.hashes[observation])


def test_the_overlay_table_holds_the_expected_rows_after_each_publish(
    clean_runtime: str,
) -> None:
    graph = _Graph(clean_runtime)
    g0 = graph.publish()
    first = graph.overlay(g0, "a", "path", "first.py")
    g1 = graph.publish()
    # Both are recorded at g1, so g2 carries them.
    second = graph.overlay(g1, "a", "path", "second.py", supersedes=first)
    rank = graph.overlay(g1, "b", "rank", 100)
    g2 = graph.publish()
    graph.reject(rank)
    g3 = graph.publish()
    g4 = graph.publish()  # nothing changed in the overlays

    assert len({g0, g1, g2, g3, g4}) == 5
    assert graph.rows(g0) == []
    assert graph.rows(g1) == [_row(graph, "a", "path", "first.py", first)]
    assert graph.rows(g2) == [
        _row(graph, "a", "path", "second.py", second),
        _row(graph, "b", "rank", 100, rank),
    ]
    assert graph.rows(g3) == [_row(graph, "a", "path", "second.py", second)]
    assert graph.rows(g4) == [_row(graph, "a", "path", "second.py", second)]
    # No other generation received a row: the table holds exactly the rows
    # above (0 + 1 + 2 + 1 + 1).
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {ATTRIBUTE_EFFECTIVE_TABLE}")
        assert cur.fetchone() == (0 + 1 + 2 + 1 + 1,)


def _rows(dsn: str, sql: str, params: Any = None) -> list[tuple]:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def test_a_publish_writes_none_of_the_three_copy_tables(
    clean_runtime: str,
) -> None:
    """PACT lane-s-storage-drop-copies: the payload versions, the
    per-generation view snapshots and the interval front are gone, and no
    publish -- first, overlay-carrying or graph-changing -- creates or writes
    any of them, nor the integrity receipt tables that guarded them. Failure
    case: the per-generation overlay table, which stays, does get rows."""
    copies = (
        "okg.attribute_effective_view",
        "okg.attribute_effective_view_intervals",
        "okg.attribute_versions",
        "okg.attribute_effective_integrity_state",
        "okg.attribute_effective_integrity_receipts",
        "okg.attribute_effective_integrity_transitions",
    )

    def present() -> list[str]:
        return [
            table for table in copies
            if _rows(clean_runtime, "SELECT to_regclass(%s)", (table,))[0][0] is not None
        ]

    dsn = clean_runtime
    graph = _Graph(dsn)
    assert present() == []
    g0 = graph.publish()
    graph.overlay(g0, "a", "path", "over-a.py")
    g1 = graph.publish()
    with psycopg.connect(dsn, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [NodeFact(graph.id("b"), "doc", {"path": "b2.py", "rank": 100},
                      {"id": graph.id("b")}, {"v": 2})],
            source="default", strict_endpoints=True,
        )
    g2 = graph.publish()
    assert present() == []
    per_generation = dict(_rows(
        dsn,
        f"SELECT generation_id, count(*) FROM {ATTRIBUTE_EFFECTIVE_TABLE} "
        "GROUP BY generation_id",
    ))
    assert [per_generation.get(g, 0) for g in (g0, g1, g2)] == [0, 1, 1]
