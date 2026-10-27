"""Whole-graph parity and GIN correctness on the graph read path (PostgreSQL).

PACT lane-s-storage-redesign-s2, design section 10 item 9 and requirements
``overlay-parity``, ``gate-rows-equal`` and ``gin``.

* Every node and every edge of a small graph, at the head and at older pins,
  through filter (the page), map, aggregate, expand (filtered and
  unfiltered, both directions, from every node) and the search supplement:
  the graph path (since PACT lane-s-storage-cutover the only read path)
  returns the same rows in the same order as a Python recompute from the
  Chronos rows plus that generation's overlays. (PACT
  lane-s-storage-drop-copies dropped the materialized copy the page was
  also compared with.)
* GIN: the supplement's graph statement does not reach S1's GIN index on
  node attrs even with sequential scans disabled on the test connection,
  while the two-phase GIN form does (the positive control); and pages are
  the same with the GIN index present and dropped.
"""
from __future__ import annotations

import json
from collections import Counter
from typing import Any

import psycopg
import pytest

from okg.substrate.mcp import graph_read
from okg.substrate.mcp.semantic_readiness import resolve_node_segment
from tests.substrate.mcp import test_graph_two_phase_db as two_phase
from tests.substrate.mcp.test_graph_aggregate_expand_db import (
    _groups,
    _with_server,
)
from tests.substrate.mcp.test_graph_read_path_db import (
    _both,
    _projected,
    _recompute,
    _World,
)
from tests.substrate.mcp.test_graph_search_supplement_db import (
    _TextWorld,
    _recompute as _supplement_recompute,
    _supplement,
    search_mod,
)

pytestmark = pytest.mark.postgres


@pytest.fixture
def world(clean_runtime: str) -> _World:
    return _World(clean_runtime)


def test_every_node_and_edge_through_the_page(world: _World, monkeypatch) -> None:
    """Every row, at every pin, equals the recompute from the Chronos rows
    plus that generation's overlays: same ids in the same order, same
    attrs. (The materialized copy this once also compared with is dropped by
    migration 0046; the planted cases in ``test_graph_read_planted_db`` show
    the comparison fails when the answers differ.)"""
    # The recompute is the only reference now, so pin what it must hold at
    # the first pin: all seven seeded docs, the three edges, and a's owner
    # from its overlay. A bug that hid rows from the page and the recompute
    # alike would fail here instead of comparing two empty lists.
    g0 = world.gens[0]
    g0_nodes = {r["id"]: r for r in _recompute(world, g0, "nodes", None)}
    assert set(world.ids.values()) <= set(g0_nodes)
    assert g0_nodes[world.ids["a"]]["attrs"].get("owner") == "zed"
    assert sorted(int(r["id"]) for r in _recompute(world, g0, "edges", None)) == (
        world.edge_ids
    )
    assert len(world.edge_ids) == 3
    for gen in world.gens:
        for kind, id_field in (("nodes", "node_id"), ("edges", "edge_id")):
            expected = _recompute(world, gen, kind, None)
            graph = _both(world, monkeypatch, gen, kind, None)["graph"]
            assert [str(r[id_field]) for r in graph] == [r["id"] for r in expected]
            assert [r["attrs"] for r in graph] == [r["attrs"] for r in expected]


def test_every_attribute_grouped_over_every_target(world: _World, monkeypatch) -> None:
    by_keys = {"nodes": ["subtype", "attrs.owner", "attrs.path", "attrs.rank", "attrs.tag"],
               "edges": ["edge_type", "attrs.weight", "attrs.label"]}
    for gen in world.gens:
        for kind, keys in by_keys.items():
            rows = _recompute(world, gen, kind, None)
            for by in keys:
                def value(row: dict) -> Any:
                    return row["attrs"].get(by[6:]) if by.startswith("attrs.") \
                        else row.get(by)
                expected = Counter(json.dumps([value(r)], sort_keys=True) for r in rows)
                graph = _groups(world, monkeypatch, "graph", gen, kind, [by], None)
                # A missing key and JSON null are two groups in SQL and both
                # read as None in Python, so the counts are summed per value.
                summed: Counter = Counter()
                for key, n in graph:
                    summed[json.dumps(list(key), sort_keys=True)] += n
                assert summed == expected, (gen, kind, by)


def _tool(world: _World, monkeypatch, mode: str, tool: str, **arguments: Any) -> dict:
    return _with_server(world, monkeypatch, mode,
                        lambda mcp: mcp._tool_manager._tools[tool].fn(**arguments))


def test_map_over_every_node_and_edge(world: _World, monkeypatch) -> None:
    for gen in world.gens:
        for kind, id_field, projection in (
            ("nodes", "node_id", ["node_id", "attrs.owner", "attrs.path", "attrs.tag"]),
            ("edges", "edge_id", ["edge_id", "attrs.weight"]),
        ):
            arguments = dict(input={"kind": kind}, projection=projection,
                             generation_id=gen, limit=500)
            graph = _tool(world, monkeypatch, "graph", "map", **arguments)
            expected = _recompute(world, gen, kind, None)
            assert [str(i[id_field]) for i in graph["items"]] == [
                r["id"] for r in expected], (gen, kind)
            for field in projection[1:]:
                key = field[6:]
                assert [_projected(i, key) for i in graph["items"]] == [
                    r["attrs"].get(key) for r in expected], (gen, kind, field)


_EXPAND_EDGE_FIELDS = ("edge_id", "src", "dst", "edge_type", "attrs", "direction")


@pytest.mark.parametrize("direction", ["out", "in"])
@pytest.mark.parametrize("filtered", [True, False], ids=["filtered", "unfiltered"])
def test_expand_from_every_node(world: _World, monkeypatch, direction: str,
                                filtered: bool) -> None:
    extra = ({"filter": {"op": "not", "clause": {"op": "exists", "field": "attrs.zz"}}}
             if filtered else {})
    for gen in world.gens:
        edges = _recompute(world, gen, "edges", None)
        for key, node in world.ids.items():
            near = "src" if direction == "out" else "dst"
            expected = [r["id"] for r in edges if r[near] == node]
            # Since #3666 a 1-hop row is the linked record; select link fields to
            # read edge rows on both paths.
            arguments = dict(input={"node_id": node}, direction=direction,
                             generation_id=gen, select=list(_EXPAND_EDGE_FIELDS),
                             **extra)
            graph = _tool(world, monkeypatch, "graph", "expand", **arguments)
            assert [str(n["edge_id"]) for n in graph["neighbors"]] == expected, (gen, key)


@pytest.fixture
def text_world(clean_runtime: str) -> _TextWorld:
    built = _TextWorld(clean_runtime)
    built.gens.append(built.publish())
    return built


def test_the_supplement_for_every_attribute_value(text_world: _TextWorld,
                                                  monkeypatch) -> None:
    """Every (key, value) any node holds after its overlays, as a one-key
    filter with a term every text holds, at the head and at both older pins."""
    with psycopg.connect(text_world.dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT DISTINCT attrs FROM {resolve_node_segment(conn)} WHERE subtype = 'doc'"
        )
        values = {(k, json.dumps(v, sort_keys=True)) for (a,) in cur.fetchall()
                  for k, v in a.items() if k != "text"}
    values |= {("owner", '"alice"'), ("tag", '"x"'), ("rank", "2")}
    for gen in text_world.gens:
        for key, raw in sorted(values):
            filters = {key: json.loads(raw)}
            expected = _supplement_recompute(text_world, gen, filters, "a")
            graph = _supplement(text_world, monkeypatch, "graph", gen, filters, "a")
            assert [str(r["node_id"]) for r in graph] == expected, (gen, filters)


# --------------------------------------------------------------------------
# GIN correctness
# --------------------------------------------------------------------------


def _node_gin(conn: psycopg.Connection) -> tuple[str, str] | None:
    segment = resolve_node_segment(conn)
    schema, _, table = segment.rpartition(".")
    row = conn.execute(
        "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = %s "
        "AND tablename = %s AND indexdef ILIKE '%%USING gin (attrs jsonb_path_ops)%%'",
        (schema or "public", table),
    ).fetchone()
    return (row[0], row[1]) if row else None


def _ensure_gin(conn: psycopg.Connection) -> tuple[str, str]:
    found = _node_gin(conn)
    if found is None:
        segment = resolve_node_segment(conn)
        conn.execute(
            f"CREATE INDEX lane_s_test_node_attrs_gin ON {segment} "
            "USING gin (attrs jsonb_path_ops) WHERE deleted = false"
        )
        conn.execute(f"ANALYZE {segment}")
        found = _node_gin(conn)
    assert found is not None
    return found


def _plans(conn: psycopg.Connection, gin_name: str,
           statements: list[tuple[str, Any]]) -> list[str]:
    """EXPLAIN each statement with the GIN index the only index left on the
    node table and sequential scans off, inside a transaction that is rolled
    back (the test database only): any plan that can use the GIN index will."""
    segment = resolve_node_segment(conn)
    schema, _, table = segment.rpartition(".")
    plans: list[str] = []
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT i.indexname FROM pg_indexes i WHERE i.schemaname = %s "
                "AND i.tablename = %s AND i.indexname <> %s AND NOT EXISTS ("
                "SELECT 1 FROM pg_constraint c WHERE c.conname = i.indexname)",
                (schema or "public", table, gin_name),
            )
            for (name,) in cur.fetchall():
                cur.execute(f'DROP INDEX "{schema}"."{name}"' if schema else f'DROP INDEX "{name}"')
            cur.execute("SET LOCAL enable_seqscan = off")
            for sql, params in statements:
                cur.execute("EXPLAIN " + sql, params, prepare=False)
                plans.append("\n".join(r[0] for r in cur.fetchall()))
    finally:
        conn.rollback()
        conn.autocommit = True
    return plans


def test_the_supplement_never_reaches_the_gin_index(text_world: _TextWorld,
                                                    monkeypatch) -> None:
    with psycopg.connect(text_world.dsn, autocommit=True) as conn:
        gin_name, _definition = _ensure_gin(conn)
    seen: list[tuple[str, Any]] = []
    real = search_mod.execute_with_custom_plan

    def capture(cur: Any, sql: Any, params: Any = None) -> Any:
        seen.append((str(sql), params))
        return real(cur, sql, params)

    monkeypatch.setattr(search_mod, "execute_with_custom_plan", capture)
    rows = _supplement(text_world, monkeypatch, "graph", text_world.gens[-1],
                       {"owner": "alice", "tag": "x"}, "alpha")
    assert rows
    [(sql, params)] = seen
    assert "n.attrs @>" not in sql
    with psycopg.connect(text_world.dsn, autocommit=True) as conn:
        # Positive control: the same containment written as object
        # containment on the graph rows does reach the GIN index.
        control_sql = (
            f"SELECT node_id FROM {resolve_node_segment(conn)} n "
            "WHERE n.deleted = false AND n.attrs @> %(f)s::jsonb"
        )
        plan, control = _plans(conn, gin_name, [
            (sql, params),
            (control_sql, {"f": json.dumps({"owner": "alice", "tag": "x"})}),
        ])
    assert gin_name not in plan, plan
    assert gin_name in control, control


@pytest.mark.parametrize("gin_present", [True, False], ids=["with-gin", "without-gin"])
def test_two_phase_pages_are_the_same_with_and_without_the_gin_index(
    clean_runtime: str, monkeypatch, gin_present: bool,
) -> None:
    """The GIN form of phase 2 is the only reader of the index; with the
    index dropped the same statement scans, and the pages are unchanged."""
    monkeypatch.setattr(graph_read, "TWO_PHASE_CUT", two_phase.CUT)
    world = two_phase._World(clean_runtime, red={1, 30, 35},
                             overlays={36: "red", 30: "blue"})
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        name, definition = _ensure_gin(conn)
        if not gin_present:
            conn.execute(f"DROP INDEX {name}")
    try:
        monkeypatch.setattr(graph_read, "subtype_row_estimate", lambda *a, **k: 1.0)
        meta = two_phase._both(world, monkeypatch, two_phase.RED)
        assert meta["two_phase"]["forms"] == ["phase1", "gin"]
        for offset in (0, 2):
            two_phase._both(world, monkeypatch, two_phase.RED, offset=offset)
    finally:
        if not gin_present:
            with psycopg.connect(clean_runtime, autocommit=True) as conn:
                conn.execute(definition)


# --------------------------------------------------------------------------
# Declared-index narrowing on the graph path (where it narrows is checked
# in test_filter_index_driven, which reads every page ids first)
# --------------------------------------------------------------------------

from tests.substrate.mcp import test_filter_index_driven as fid  # noqa: E402
from tests.substrate.mcp.test_filter_index_driven import published  # noqa: E402, F401


def _graph_narrowed(statements: list[tuple[str, Any]]) -> bool:
    """The graph-id branch reads the declared candidate set: as ``IN`` (edges,
    the unordered count) or, on an ordered node page, as a join on the
    distinct candidate ids sorted by id."""
    return any("%(dai_bp)s" in sql and (
        "graph_rows.target_id IN (" in sql or _CANDIDATE_JOIN in sql)
        for sql, _params in statements)


_CANDIDATE_JOIN = "AS candidate_ids(target_id) ORDER BY 1) AS candidates JOIN "


@pytest.mark.parametrize(
    ("label", "scope", "predicate", "pushed"),
    [case for case in fid._PARITY_CASES if case[3]],
    ids=[case[0] for case in fid._PARITY_CASES if case[3]],
)
def test_the_candidate_join_reads_each_candidate_once(
    published: tuple[str, str, str], monkeypatch, label: str, scope: dict[str, Any],  # noqa: F811
    predicate: dict[str, Any], pushed: bool,
) -> None:
    """The ordered node page joins the candidate ids rather than testing
    ``IN``; ``x IN (S)`` is that join only on DISTINCT S. A candidate set
    that lists every id twice must page exactly like the unnarrowed read."""
    from okg.substrate.mcp import attr_filter_source as afs

    dsn, generation, _suffix = published
    scanned, _ = fid._run_filter(dsn, generation, predicate, monkeypatch=monkeypatch,
                                 scan_only=True, scope=scope)
    real = afs.plan_expression_candidates

    def doubled(*args: Any, **kwargs: Any):
        plan, uncovered = real(*args, **kwargs)
        if plan is not None:
            plan = afs.ExpressionCandidates(
                sql=f"({plan.sql}) UNION ALL ({plan.sql})",
                params=plan.params, attrs=plan.attrs,
            )
        return plan, uncovered

    monkeypatch.setattr(afs, "plan_expression_candidates", doubled)
    graph, graph_sql = fid._run_filter(dsn, generation, predicate,
                                       monkeypatch=monkeypatch, scan_only=False,
                                       scope=scope)
    assert fid._answer(graph) == fid._answer(scanned), label
    if scope["kind"] == "nodes" and _graph_narrowed(graph_sql):
        assert any(_CANDIDATE_JOIN in sql for sql, _params in graph_sql), graph_sql


@pytest.mark.parametrize(
    ("label", "scope", "predicate", "pushed"),
    [case for case in fid._PARITY_CASES if case[3]],
    ids=[case[0] for case in fid._PARITY_CASES if case[3]],
)
def test_the_narrowed_count_equals_the_unnarrowed_count(
    published: tuple[str, str, str], monkeypatch, label: str, scope: dict[str, Any],  # noqa: F811
    predicate: dict[str, Any], pushed: bool,
) -> None:
    """A one-row page forces the bounded count. On the graph path a declared
    candidate set narrows the count's graph-id branch inside a fenced scan
    whose output a hashed subplan of overlaid ids tests (graph_read
    id_stream_branches, ``anti == "fenced"``); its total equals the
    unnarrowed read's total."""
    dsn, generation, _suffix = published
    scanned, _ = fid._run_filter(dsn, generation, predicate, monkeypatch=monkeypatch,
                                 scan_only=True, scope=scope, limit=1)
    counts: list[str] = []
    real = psycopg.Cursor.execute

    def capture(self, query, params=None, **kw):
        if "bounded_operator_count" in str(query):
            counts.append(str(query))
        return real(self, query, params, **kw)

    monkeypatch.setattr(psycopg.Cursor, "execute", capture)
    graph, graph_sql = fid._run_filter(dsn, generation, predicate,
                                       monkeypatch=monkeypatch, scan_only=False,
                                       scope=scope, limit=1)
    monkeypatch.setattr(psycopg.Cursor, "execute", real)
    assert fid._answer(graph) == fid._answer(scanned), label
    assert (graph.get("total_count"), graph.get("total_count_basis")) == (
        scanned.get("total_count"), scanned.get("total_count_basis")), label
    if scanned.get("truncated"):
        assert counts, label
    if _graph_narrowed(graph_sql) and counts:
        assert all("OFFSET 0) AS graph_scan WHERE graph_scan.id NOT IN (" in c
                   for c in counts), counts
