"""The overlay materialization marker and the overlay validity ranges.

PACT lane-s-storage-redesign-s2, requirements ``overlay-retention`` (S2 part)
and ``pinned-parity`` (the marker refusal); design section 5. Real
``Publisher.cycle()`` publishes on a PostgreSQL fixture: every publish writes
a marker (also with no overlays), folds its overlay rows into the branch's
ranges after its ordinal, and the ranges equal the per-generation overlay rows
at every generation while overlays are added, changed and removed. The
per-generation table is still copied forward each publish; nothing else is
copied (PACT lane-s-storage-drop-copies dropped the three attribute copies).
"""
from __future__ import annotations

import uuid
from importlib import resources
from typing import Any

import psycopg
import pytest

from okg.substrate.attributes import (
    ATTRIBUTE_EFFECTIVE_TABLE,
    OVERLAY_GENERATION_MARKER_TABLE,
    OVERLAY_RANGE_OPEN_ORD,
    OVERLAY_RANGES_TABLE,
    materialize_effective_attributes_for_generation,
    overlay_range_parity,
    record_attribute_observation,
    reject_attribute_observation,
    require_overlay_generation_marker,
    sync_overlay_ranges_for_generation,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.mcp import server as server_mod
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import (
    SubtypeSpec,
    bind_test_catalog_writer,
    seed_catalog,
)

pytestmark = pytest.mark.postgres

MIGRATION_FILE = "0044_attribute_overlay_generations.sql"


class _Graph:
    """Six documents and a clock node; every publish ticks the clock, so each
    one is a real (non-reused) generation."""

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self.suffix = uuid.uuid4().hex[:8]
        self.tick = 0
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

    def publish(self, *, change: bool = True) -> str:
        if change:
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
            return record_attribute_observation(
                conn, target_kind="node", target_id=self.id(name),
                generation_id=generation, attr_key=key, value=value,
                producer="test:overlay-ranges", supersedes=supersedes,
            ).observation_id

    def reject(self, observation_id: str) -> None:
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            reject_attribute_observation(conn, observation_id, reason="test")


def _query(dsn: str, sql: str, params: Any = None) -> list[tuple]:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall() if cur.description else []


def _marker(dsn: str, generation: str) -> tuple | None:
    rows = _query(
        dsn,
        f"SELECT overlay_rows, chronos_branch_id, ordinal, ranges_synced, source "
        f"FROM {OVERLAY_GENERATION_MARKER_TABLE} "
        "WHERE generation_id = %s AND namespace = 'default'",
        (generation,),
    )
    return rows[0] if rows else None


def _ordinal(dsn: str, generation: str) -> int:
    return int(_query(
        dsn, "SELECT ordinal FROM okg.generation_ordinals WHERE checkpoint_id = %s",
        (generation,),
    )[0][0])


def _ranges(dsn: str, target_id: str, key: str) -> list[tuple]:
    return _query(
        dsn,
        f"SELECT from_ord, to_ord, value FROM {OVERLAY_RANGES_TABLE} "
        "WHERE target_id = %s AND attr_key = %s ORDER BY from_ord",
        (target_id, key),
    )


def _parity(dsn: str) -> list[dict]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        return overlay_range_parity(conn)


def _count(dsn: str, table: str) -> int:
    return int(_query(dsn, f"SELECT count(*) FROM {table}")[0][0])


def test_a_publish_with_no_overlays_writes_a_marker(clean_runtime: str) -> None:
    graph = _Graph(clean_runtime)
    g0 = graph.publish()
    overlay_rows, branch_id, ordinal, synced, source = _marker(clean_runtime, g0)
    assert overlay_rows == 0
    assert (branch_id, ordinal, synced, source) == (
        "default", _ordinal(clean_runtime, g0), True, "materializer",
    )
    assert _count(clean_runtime, OVERLAY_RANGES_TABLE) == 0
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        require_overlay_generation_marker(conn, g0)


def test_an_unmarked_generation_is_refused_with_the_overlay_step_cause(
    clean_runtime: str,
) -> None:
    """The marker check itself, below the MCP restatement: a generation with
    no marker is refused by name in the same words, citing no tracker, and the
    hint says the next publish writes the marker and names no retired read
    switch."""
    from okg.substrate.errors import SubstrateError

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with pytest.raises(SubstrateError) as caught:
            require_overlay_generation_marker(conn, "okg:t:gen:unmarked")
    err = caught.value
    assert err.code == "overlay_generation_unmaterialized"
    assert "its overlay step never ran" in str(err) and "#" not in str(err)
    assert "next publish" in (err.fix_hint or "")
    assert "OKG_EFFECTIVE_READ_PATH" not in (err.fix_hint or "")
    assert err.data["marker_table_present"] is True


def _materialize(dsn: str, generation: str) -> dict[str, Any]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        return materialize_effective_attributes_for_generation(conn, generation_id=generation)


def _filter_ids(dsn: str, generation: str, predicate: Any) -> list[str]:
    """Node ids the filter tool returns at ``generation`` (the graph path,
    the only read path since PACT lane-s-storage-cutover)."""
    mcp = server_mod.build_server(dsn=dsn, deployment_name="default")
    try:
        out = mcp._tool_manager._tools["filter"].fn(
            input={"kind": "nodes", "subtype": "doc"}, predicate=predicate,
            generation_id=generation, limit=10,
        )
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()
    return [r["node_id"] for r in out["items"]]


def test_every_materializer_exit_writes_the_marker(clean_runtime: str) -> None:
    """The marker is written at the empty-state exit, the normal exit with no
    overlay rows, and the already-materialized exit; each test deletes it
    first and expects it back with the generation's current row count."""
    dsn = clean_runtime
    graph = _Graph(dsn)
    g0 = graph.publish()  # no observation exists: the empty-state exit
    _query(dsn, f"DELETE FROM {OVERLAY_GENERATION_MARKER_TABLE}")
    assert _materialize(dsn, g0)["reason"] == "empty_attribute_state"
    assert _marker(dsn, g0)[0] == 0
    # An observation exists (for another generation), g1 has no overlay row.
    observation = graph.overlay(g0, "a", "path", "x.py")
    g1 = graph.publish()
    graph.reject(observation)
    g2 = graph.publish()
    assert _query(dsn, f"SELECT count(*) FROM {ATTRIBUTE_EFFECTIVE_TABLE} "
                  "WHERE generation_id = %s", (g2,)) == [(0,)]
    _query(dsn, f"DELETE FROM {OVERLAY_GENERATION_MARKER_TABLE} WHERE generation_id = %s",
           (g2,))
    result = _materialize(dsn, g2)
    assert (result["materialized"], result["overlaid"]) == (True, 0)
    assert _marker(dsn, g2)[0] == 0
    # g1 holds a row: the already-materialized exit rewrites its marker.
    _query(dsn, f"DELETE FROM {OVERLAY_GENERATION_MARKER_TABLE} WHERE generation_id = %s",
           (g1,))
    assert _materialize(dsn, g1)["reason"] == "already_materialized"
    assert _marker(dsn, g1)[0] == 1


def test_rematerializing_a_zero_overlay_generation_adds_its_overlays(
    clean_runtime: str,
) -> None:
    """Design 5.1: the already-materialized test stays row-based, so a
    generation published with no overlay rows (and a marker of 0) still takes
    an overlay recorded after its publish when re-materialized; the marker
    count follows and the read serves the overlay at that pin. A
    marker-based test would skip it and leave the read on the graph
    value."""
    dsn = clean_runtime
    graph = _Graph(dsn)
    g0 = graph.publish()
    assert _marker(dsn, g0)[0] == 0
    predicate = {"op": "eq", "field": "attrs.path", "value": "late.py"}
    assert _filter_ids(dsn, g0, predicate) == []
    graph.overlay(g0, "b", "path", "late.py")
    result = _materialize(dsn, g0)
    assert (result["materialized"], result["overlaid"]) == (True, 1)
    assert _marker(dsn, g0)[0] == 1
    assert _filter_ids(dsn, g0, predicate) == [graph.id("b")]


def test_a_bare_checkpoint_is_refused_by_name_with_its_cause(
    clean_runtime: str,
) -> None:
    """A published generation minted by a bare checkpoint, outside the
    publisher, a fast-forward, a compaction and a rollover (as the bench
    branching experiment mints its heads), has no overlay marker. With the
    graph path the only read path (PACT lane-s-storage-cutover), a read
    pinned there, or a read with no pin while it is the head, is refused by
    name with its cause and the read that serves (issue #4049); a marked
    generation (here the one before it) is still served. The checkpoint carries the
    active catalog version, as every published generation does, so the
    server's start-time pin read accepts it and the read itself refuses."""
    from okg.substrate.catalog.aliases import active_catalog_version_id

    dsn = clean_runtime
    graph = _Graph(dsn)
    before = graph.publish()
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact(graph.id("g"), "doc", {"path": "g.py", "rank": 9},
                      {"id": graph.id("g")}, {"v": 1})],
            source="default", strict_endpoints=True,
        )
        forwarded = store.checkpoint_generation(
            deployment_name="default", status="published",
            catalog_version_id=active_catalog_version_id(conn),
        )
    assert _marker(dsn, forwarded) is None
    predicate = {"op": "eq", "field": "attrs.path", "value": "a.py"}
    for pin in (forwarded, None):
        with pytest.raises(SubstrateError) as exc_info:
            _filter_ids(dsn, pin, predicate)
        error = exc_info.value
        assert error.code == "overlay_generation_unmaterialized", pin
        assert error.data["cause"] == "overlay_step_not_run", error.data
        assert error.data["generation_id"] == forwarded, error.data
        assert "older release" in error.message and "okg generation list" in error.fix_hint
        # Shipped text names the fact and the next step, no tracker reference.
        assert "#" not in error.message and "#" not in error.fix_hint
        # The hint does not promise that the next publish succeeds.
        assert "can itself be refused" in error.fix_hint
        assert "OKG_EFFECTIVE_READ_PATH" not in error.fix_hint
    assert _marker(dsn, before) is not None
    assert _filter_ids(dsn, before, predicate) == [graph.id("a")]


def test_a_generation_without_its_marker_refuses_the_graph_read(
    clean_runtime: str,
) -> None:
    graph = _Graph(clean_runtime)
    g0 = graph.publish()
    graph.overlay(g0, "a", "path", "overlaid.py")
    g1 = graph.publish()
    _query(
        clean_runtime,
        f"DELETE FROM {OVERLAY_GENERATION_MARKER_TABLE} WHERE generation_id = %s",
        (g1,),
    )
    mcp = server_mod.build_server(dsn=clean_runtime, deployment_name="default")
    tool = mcp._tool_manager._tools["filter"].fn
    predicate = {"op": "eq", "field": "attrs.path", "value": "overlaid.py"}
    try:
        with pytest.raises(SubstrateError) as exc_info:
            tool(input={"kind": "nodes", "subtype": "doc"},
                 predicate=predicate, generation_id=g1, limit=10)
        assert exc_info.value.code == "overlay_generation_unmaterialized"
        assert exc_info.value.data["generation_id"] == g1
        # The generation that kept its marker is still served.
        served = tool(input={"kind": "nodes", "subtype": "doc"},
                      predicate={"op": "eq", "field": "attrs.path", "value": "a.py"},
                      generation_id=g0, limit=10)
        assert [r["node_id"] for r in served["items"]] == [graph.id("a")]
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


def test_ranges_equal_the_per_generation_rows_as_overlays_change(
    clean_runtime: str,
) -> None:
    dsn = clean_runtime
    graph = _Graph(dsn)
    g0 = graph.publish()
    a1 = graph.overlay(g0, "a", "path", "first.py")
    b1 = graph.overlay(g0, "b", "rank", 100)
    g1 = graph.publish()  # a and b overlaid
    graph.overlay(g1, "a", "path", "second.py", supersedes=a1)  # changed
    graph.overlay(g1, "c", "path", "added.py")  # added
    g2 = graph.publish()
    graph.reject(b1)  # removed
    g3 = graph.publish()
    g4 = graph.publish()  # unchanged overlays

    assert _parity(dsn) == []
    o1, o2, o3 = (_ordinal(dsn, g) for g in (g1, g2, g3))
    assert _ranges(dsn, graph.id("a"), "path") == [
        (o1, o2, "first.py"), (o2, OVERLAY_RANGE_OPEN_ORD, "second.py"),
    ]
    assert _ranges(dsn, graph.id("b"), "rank") == [(o1, o3, 100)]
    assert _ranges(dsn, graph.id("c"), "path") == [(o2, OVERLAY_RANGE_OPEN_ORD, "added.py")]
    # The per-generation table is still copied forward every publish.
    per_generation = dict(_query(
        dsn,
        f"SELECT generation_id, count(*) FROM {ATTRIBUTE_EFFECTIVE_TABLE} "
        "GROUP BY generation_id",
    ))
    assert [per_generation.get(g, 0) for g in (g0, g1, g2, g3, g4)] == [0, 2, 3, 2, 2]
    assert [_marker(dsn, g)[0] for g in (g0, g1, g2, g3, g4)] == [0, 2, 3, 2, 2]


def test_a_planted_range_row_breaks_parity(clean_runtime: str) -> None:
    dsn = clean_runtime
    graph = _Graph(dsn)
    g0 = graph.publish()
    graph.overlay(g0, "a", "path", "x.py")
    g1 = graph.publish()
    assert _parity(dsn) == []
    _query(
        dsn,
        f"INSERT INTO {OVERLAY_RANGES_TABLE} (chronos_branch_id, target_kind, target_id, "
        "namespace, attr_key, from_ord, to_ord, observation_id, value, value_hash) "
        "VALUES ('default', 'node', %s, 'default', 'path', %s, %s, 'planted', "
        "'\"y.py\"', 'sha256:planted')",
        (graph.id("b"), _ordinal(dsn, g1), OVERLAY_RANGE_OPEN_ORD),
    )
    [planted] = _parity(dsn)
    assert planted["generation_id"] == g1
    assert (planted["missing_from_ranges"], planted["extra_in_ranges"]) == (0, 1)
    _query(dsn, f"DELETE FROM {OVERLAY_RANGES_TABLE} WHERE target_id = %s",
           (graph.id("a"),))
    [both] = _parity(dsn)
    assert (both["missing_from_ranges"], both["extra_in_ranges"]) == (1, 1)


def test_unchanged_publishes_do_not_grow_the_ranges(clean_runtime: str) -> None:
    dsn = clean_runtime
    graph = _Graph(dsn)
    g0 = graph.publish()
    for name in "abc":
        graph.overlay(g0, name, "path", f"{name}-overlay.py")
    graph.publish()
    assert _count(dsn, OVERLAY_RANGES_TABLE) == 3
    per_generation_before = _count(dsn, ATTRIBUTE_EFFECTIVE_TABLE)
    for _ in range(10):
        graph.publish()
    assert _count(dsn, OVERLAY_RANGES_TABLE) == 3
    # Recorded, not bounded in S2: the per-generation copy-forward still adds
    # every live overlay on every publish (its bound is S3's).
    assert _count(dsn, ATTRIBUTE_EFFECTIVE_TABLE) == per_generation_before + 3 * 10
    assert _parity(dsn) == []


def test_ranges_left_behind_are_rebuilt_by_the_next_publish(
    clean_runtime: str,
) -> None:
    dsn = clean_runtime
    graph = _Graph(dsn)
    g0 = graph.publish()
    a1 = graph.overlay(g0, "a", "path", "first.py")
    g1 = graph.publish()
    graph.overlay(g1, "a", "path", "second.py", supersedes=a1)
    graph.publish()
    expected = _query(dsn, f"SELECT * FROM {OVERLAY_RANGES_TABLE} ORDER BY 1, 2, 3, 4, 5, 6")
    _query(dsn, f"DELETE FROM {OVERLAY_RANGES_TABLE}")
    _query(dsn, f"UPDATE {OVERLAY_GENERATION_MARKER_TABLE} SET ranges_synced = FALSE")
    g3 = graph.publish()
    assert _parity(dsn) == []
    rebuilt = _query(dsn, f"SELECT * FROM {OVERLAY_RANGES_TABLE} ORDER BY 1, 2, 3, 4, 5, 6")
    assert rebuilt == expected  # nothing changed in g3, so the same ranges
    assert _marker(dsn, g3)[3] is True


def test_syncing_an_older_generation_is_refused(clean_runtime: str) -> None:
    graph = _Graph(clean_runtime)
    g0 = graph.publish()
    graph.overlay(g0, "a", "path", "x.py")
    g1 = graph.publish()
    g2 = graph.publish()
    before = _query(clean_runtime, f"SELECT * FROM {OVERLAY_RANGES_TABLE} ORDER BY 3, 6")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # The latest generation re-syncs idempotently ...
        again = sync_overlay_ranges_for_generation(conn, g2)
        assert (again["mode"], again["closed"], again["opened"]) == ("step", 0, 0)
        # ... an older one is refused: its later ranges are already closed.
        with pytest.raises(SubstrateError) as exc_info:
            sync_overlay_ranges_for_generation(conn, g1)
    assert exc_info.value.code == "overlay_ranges_out_of_order"
    assert _query(clean_runtime, f"SELECT * FROM {OVERLAY_RANGES_TABLE} ORDER BY 3, 6") == before


def test_a_fork_opens_its_own_ranges_and_leaves_the_parents(
    clean_runtime: str,
) -> None:
    """A fork's first generation carries its parent's overlays forward under
    its own branch; its ordinal step opens ranges on the fork only."""
    dsn = clean_runtime
    graph = _Graph(dsn)
    g0 = graph.publish()
    graph.overlay(g0, "a", "path", "x.py")
    g1 = graph.publish()
    parent_before = _query(
        dsn, f"SELECT * FROM {OVERLAY_RANGES_TABLE} WHERE chronos_branch_id = 'default' ORDER BY 3",
    )
    fork_gen = f"fork-gen-{graph.suffix}"
    _query(
        dsn,
        f"""
        INSERT INTO {ATTRIBUTE_EFFECTIVE_TABLE}
          (target_kind, target_id, generation_id, namespace, attr_key,
           observation_id, value, value_hash, producer)
        SELECT target_kind, target_id, %s, namespace, attr_key, observation_id,
               value, value_hash, producer
          FROM {ATTRIBUTE_EFFECTIVE_TABLE} WHERE generation_id = %s
        """,
        (fork_gen, g1),
    )
    _query(
        dsn,
        f"INSERT INTO {OVERLAY_GENERATION_MARKER_TABLE} "
        "(generation_id, namespace, previous_generation_id, overlay_rows, source) "
        "VALUES (%s, 'default', %s, 1, 'materializer')",
        (fork_gen, g1),
    )
    _query(
        dsn,
        "INSERT INTO okg.generation_ordinals "
        "(checkpoint_id, branch_id, ordinal, previous_checkpoint_id, deployment_name) "
        "VALUES (%s, %s, %s, %s, 'default')",
        (fork_gen, f"fork-{graph.suffix}", _ordinal(dsn, g1) + 1, g1),
    )
    with psycopg.connect(dsn, autocommit=True) as conn:
        result = sync_overlay_ranges_for_generation(conn, fork_gen)
    assert result["mode"] == "step" and result["opened"] == 1
    assert _query(
        dsn, f"SELECT * FROM {OVERLAY_RANGES_TABLE} WHERE chronos_branch_id = 'default' ORDER BY 3",
    ) == parent_before
    assert _query(
        dsn,
        f"SELECT target_id, from_ord, to_ord FROM {OVERLAY_RANGES_TABLE} "
        "WHERE chronos_branch_id = %s",
        (f"fork-{graph.suffix}",),
    ) == [(graph.id("a"), _ordinal(dsn, g1) + 1, OVERLAY_RANGE_OPEN_ORD)]
    assert _parity(dsn) == []


def test_migration_backfill_writes_markers_only_where_proved(
    clean_runtime: str,
) -> None:
    dsn = clean_runtime
    graph = _Graph(dsn)
    # No overlays: before migration 0046 its effective-view and interval
    # rows proved its overlay step; with the copies dropped nothing does, so
    # the backfill writes it no marker (0044 skips the absent tables).
    g0 = graph.publish()
    graph.overlay(g0, "a", "path", "x.py")
    g1 = graph.publish()
    _query(dsn, f"DELETE FROM {OVERLAY_GENERATION_MARKER_TABLE}")
    _query(dsn, f"DELETE FROM {OVERLAY_RANGES_TABLE}")
    # A generation with an ordinal but no proof that its overlay step ran: no
    # per-generation rows. The backfill must skip it.
    unproved = f"unproved-gen-{graph.suffix}"
    _query(
        dsn,
        "INSERT INTO okg.generation_ordinals "
        "(checkpoint_id, branch_id, ordinal, previous_checkpoint_id, deployment_name) "
        "VALUES (%s, %s, 1, NULL, 'default')",
        (unproved, f"unproved-{graph.suffix}"),
    )
    sql = (
        resources.files("okg.substrate.db.migrations")
        .joinpath(MIGRATION_FILE).read_text(encoding="utf-8")
    )
    with psycopg.connect(dsn) as conn:  # SET LOCAL needs a transaction
        conn.execute(sql)
        conn.commit()
        conn.execute(sql)  # idempotent
        conn.commit()
    m0, m1 = _marker(dsn, g0), _marker(dsn, g1)
    assert m0 is None
    assert m1 is not None and m1[0] == 1 and m1[4] == "backfill"
    assert (m1[1], m1[2], m1[3]) == ("default", _ordinal(dsn, g1), False)
    assert _query(
        dsn,
        f"SELECT count(*) FROM {OVERLAY_GENERATION_MARKER_TABLE} "
        "WHERE generation_id <> %s",
        (g1,),
    ) == [(0,)]
    assert _marker(dsn, unproved) is None
    _query(dsn, "DELETE FROM okg.generation_ordinals WHERE checkpoint_id = %s", (unproved,))
    # The next publish finds the ranges behind and rebuilds them.
    graph.publish()
    assert _count(dsn, OVERLAY_RANGES_TABLE) == 1
    assert _parity(dsn) == []
