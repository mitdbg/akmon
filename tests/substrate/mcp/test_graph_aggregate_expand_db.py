"""The graph read path for aggregate, reference groups and expand (PostgreSQL).

PACT lane-s-storage-redesign-s2, design sections 2.5 and 2.6; since PACT
lane-s-storage-cutover the graph path is the only read path. Each grouped
count is compared, as a set of groups and counts, with a Python recompute
from the Chronos rows at the pin plus that generation's overlays. The split
aggregate keeps its ``NOT IN`` hashed subplan and inlines the grouped key;
expand reads the anchor's edges and far nodes from the Chronos rows, each
merged with its overlays, in numeric edge order. Fixture: the three-generation
world of ``test_graph_read_path_db``.
"""
from __future__ import annotations

import json
from collections import Counter
from typing import Any

import psycopg
import pytest

from okg.substrate.attributes import materialize_effective_attributes_for_generation
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.mcp import graph_read, open_session
from okg.substrate.mcp import server as server_mod
from tests.substrate.mcp.test_graph_read_path_db import (
    _matches,
    _nomos_rows,
    _recompute,
    _World,
)

pytestmark = pytest.mark.postgres


@pytest.fixture(name="world")
def _world_fixture(clean_runtime: str) -> _World:
    return _World(clean_runtime)


def _with_server(world: _World, monkeypatch, mode: str, fn):
    assert mode == "graph", mode
    mcp = server_mod.build_server(dsn=world.dsn, deployment_name="default")
    try:
        return fn(mcp)
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


def _groups(world: _World, monkeypatch, mode: str, gen: str, kind: str, by: list[str],
            predicate: Any, member_ids: list[str] | None = None) -> Any:
    def run(mcp):
        with psycopg.connect(world.dsn, autocommit=True) as conn:
            session = open_session(conn, deployment_name="default",
                                   requested_generation=gen)
            return mcp._okg_effective_view_aggregate_groups(
                conn, session, kind=kind, by=by, predicate=predicate,
                member_ids=member_ids,
            )
    result = _with_server(world, monkeypatch, mode, run)
    assert result is not None
    groups, _fingerprint = result
    return groups


def _expected(world: _World, gen: str, kind: str, by: list[str], predicate: Any,
              member_ids: list[str] | None) -> Counter:
    rows = _recompute(world, gen, kind, predicate)
    if member_ids is not None:
        rows = [r for r in rows if r["id"] in member_ids]

    def value(row: dict, field: str) -> Any:
        if field.startswith("attrs."):
            return row["attrs"].get(field[6:])
        return row.get(field)

    return Counter(
        json.dumps([value(r, f) for f in by], sort_keys=True) for r in rows
    )


def _as_counter(groups: list) -> Counter:
    return Counter({json.dumps(list(key), sort_keys=True): n for key, n in groups})


def _assert_groups(world: _World, monkeypatch, kind: str, by: list[str], predicate: Any,
                   member_ids: list[str] | None = None) -> None:
    for gen in world.gens:
        got = _groups(world, monkeypatch, "graph", gen, kind, by, predicate, member_ids)
        expected = _expected(world, gen, kind, by, predicate, member_ids)
        assert _as_counter(got) == expected, (gen, by, predicate)


DOC = {"op": "eq", "field": "subtype", "value": "doc"}
REFS = {"op": "eq", "field": "edge_type", "value": "references"}


@pytest.mark.parametrize("by,predicate", [
    pytest.param(["attrs.owner"], DOC, id="by-overlaid-key"),
    pytest.param(["attrs.tag"], {"op": "and", "clauses": [
        DOC, {"op": "eq", "field": "attrs.owner", "value": "alice"}]}, id="by-and-filter"),
    pytest.param(["attrs.owner", "attrs.tag"], DOC, id="two-keys"),
    pytest.param(["attrs.path"], {"op": "and", "clauses": [
        DOC, {"op": "not", "clause": {"op": "exists", "field": "attrs.tag"}}]}, id="not-exists"),
    pytest.param(["attrs.rank"], {"op": "and", "clauses": [
        DOC, {"op": "eq", "field": "attrs", "value": {"rank": 3}}]}, id="whole-attrs"),
    pytest.param(["subtype"], None, id="stable-only"),
    pytest.param(["subtype"], {"op": "eq", "field": "attrs.owner", "value": "alice"},
                 id="stable-by-attr-filter"),
])
def test_node_groups_equal_the_recompute(
    world: _World, monkeypatch, by: list[str], predicate: Any,
) -> None:
    _assert_groups(world, monkeypatch, "nodes", by, predicate)


@pytest.mark.parametrize("by,predicate", [
    pytest.param(["attrs.weight"], REFS, id="by-overlaid-key"),
    pytest.param(["edge_type"], None, id="stable-only"),
    pytest.param(["src"], {"op": "gt", "field": "attrs.weight", "value": 2}, id="attr-filter"),
])
def test_edge_groups_equal(world: _World, monkeypatch, by, predicate) -> None:
    _assert_groups(world, monkeypatch, "edges", by, predicate)


def test_reference_groups_count_only_the_members(world: _World, monkeypatch) -> None:
    members = [world.ids["a"], world.ids["c"], world.ids["e"], world.ids["f"]]
    _assert_groups(world, monkeypatch, "nodes", ["attrs.owner"], None, member_ids=members)
    _assert_groups(world, monkeypatch, "nodes", ["subtype"], None, member_ids=members)


def test_the_split_aggregate_keeps_its_hashed_subplan_and_inlines_the_key(
    world: _World, monkeypatch,
) -> None:
    captured: list[tuple[str, Any, Any]] = []
    real = psycopg.Cursor.execute

    def capture(self, query, params=None, **kw):
        captured.append((str(query), params, kw.get("prepare")))
        return real(self, query, params, **kw)

    monkeypatch.setattr(psycopg.Cursor, "execute", capture)
    _groups(world, monkeypatch, "graph", world.gens[2], "nodes", ["attrs.owner"], DOC)
    monkeypatch.setattr(psycopg.Cursor, "execute", real)
    [(sql, params, prepare)] = [c for c in captured if "AS split" in c[0]]
    assert prepare is False
    assert "-> 'owner')" in sql  # the inlined literal the statistics match
    assert "NOT IN (" in sql
    with psycopg.connect(world.dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("EXPLAIN " + sql, params, prepare=False)
        plan = "\n".join(r[0] for r in cur.fetchall())
    assert "hashed SubPlan" in plan, plan
    assert not [q for q, _p, _r in captured
                if q.lstrip().upper().startswith("SET ") or "set_config('enable" in q
                or "plan_cache_mode" in q]


def _expand(world: _World, monkeypatch, mode: str, gen: str, **kw: Any) -> dict:
    def run(mcp):
        return mcp._tool_manager._tools["expand"].fn(generation_id=gen, **kw)
    return _with_server(world, monkeypatch, mode, run)


#: Since issue #3666 a 1-hop row is the linked record; a select of link
#: fields asks for link rows (the read `_expand_one_hop_effective_within`
#: serves).
LINKS = ["edge_id", "src", "dst", "edge_type", "attrs", "direction"]
SHAPES = {"links": {"select": LINKS}, "records": {}}


def _far_ids(page: dict, shape: str, direction: str = "out") -> list[str]:
    if shape == "records":
        return [str(n["node_id"]) for n in page["neighbors"]]
    return [str(n["dst" if direction == "out" else "src"]) for n in page["neighbors"]]


@pytest.mark.parametrize("shape", list(SHAPES))
@pytest.mark.parametrize("anchor,kw", [
    pytest.param("a", {"filter": {"op": "exists", "field": "attrs.path"}}, id="exists"),
    pytest.param("c", {"filter": {"op": "eq", "field": "attrs.path", "value": "a.py"}},
                 id="overlay-value-on-destination"),
    pytest.param("c", {"filter": {"op": "eq", "field": "attrs.path", "value": "d.py"}},
                 id="graph-value-overlaid-away"),
    pytest.param("b", {"order_by": "attrs.rank desc"}, id="order"),
    pytest.param("c", {"filter": {"op": "exists", "field": "attrs.owner"},
                       "direction": "in"}, id="direction-in"),
])
def test_expand_with_a_destination_filter_or_order_is_equal(
    world: _World, monkeypatch, anchor: str, kw: dict, shape: str,
) -> None:
    """Both row shapes (lane SG dev merge, 10-19): the record rows read the
    far records through the page with the far ends as a ``member_sql`` set,
    which the graph page must keep to. At every pin the graph's far records
    are the far ends of its own link rows that pass the filter, so a page
    that ignored the set, or far ends read from a copy the pin no longer
    holds, differs here."""
    direction = kw.get("direction", "out")
    for gen in world.gens:
        graph = _expand(world, monkeypatch, "graph", gen,
                        input={"node_id": world.ids[anchor]}, **kw, **SHAPES[shape])
        if shape == "links":
            # The far ends the recompute admits (Chronos rows plus overlays).
            edges = _recompute(world, gen, "edges", None)
            nodes = {r["id"]: r for r in _recompute(world, gen, "nodes", None)}
            near, far = ("src", "dst") if direction == "out" else ("dst", "src")
            want = sorted(
                e[far] for e in edges if e[near] == world.ids[anchor]
                and _matches(nodes.get(e[far], {"attrs": {}}), kw.get("filter"))
            )
            assert sorted(_far_ids(graph, shape, direction)) == want, (gen, kw)
        if shape == "records":
            links = _expand(world, monkeypatch, "graph", gen,
                            input={"node_id": world.ids[anchor]}, **kw, **SHAPES["links"])
            assert sorted(_far_ids(graph, shape)) == sorted(
                set(_far_ids(links, "links", direction))), (gen, kw)
    if kw.get("filter", {}).get("value") == "a.py":
        assert _far_ids(graph, shape, direction) == [world.ids["d"]]
    if kw.get("filter", {}).get("value") == "d.py":
        assert graph["neighbors"] == []


def test_expand_hands_nomos_the_s2_link_rows(world: _World, monkeypatch) -> None:
    """Requirement ``nomos-unchanged``: the link rows a filtered expand
    hands the Nomos read filter are S2's row dicts (``_nomos_rows``) with
    the integer edge id and the row's direction."""
    real = server_mod.filter_edge_rows_for_session
    calls: list = []

    def edges(conn, session, rows, **kw):
        rows = list(rows)
        calls.append(json.loads(json.dumps(rows, default=str)))
        return real(conn, session, rows, **kw)

    gen, anchor = world.gens[2], world.ids["b"]
    monkeypatch.setattr(server_mod, "filter_edge_rows_for_session", edges)
    _expand(world, monkeypatch, "graph", gen, input={"node_id": anchor},
            filter={"op": "exists", "field": "attrs.path"}, select=LINKS)
    monkeypatch.setattr(server_mod, "filter_edge_rows_for_session", real)
    nodes = {r["id"]: r for r in _recompute(world, gen, "nodes", None)}
    expected = [
        {**row, "edge_id": int(row["edge_id"]), "direction": "out"}
        for row in _nomos_rows(world, gen, "edges")
        if row["src"] == anchor and "path" in nodes.get(row["dst"], {"attrs": {}})["attrs"]
    ]
    assert expected, "the fixture links b to a record with a path"
    assert calls == [expected], calls


def test_expand_statements_are_unprepared_and_set_nothing(world: _World, monkeypatch) -> None:
    captured: list[tuple[str, Any]] = []
    real = psycopg.Cursor.execute

    def capture(self, query, params=None, **kw):
        captured.append((str(query), kw.get("prepare")))
        return real(self, query, params, **kw)

    monkeypatch.setattr(psycopg.Cursor, "execute", capture)
    _expand(world, monkeypatch, "graph", world.gens[2], input={"node_id": world.ids["a"]},
            filter={"op": "exists", "field": "attrs.path"}, select=LINKS)
    monkeypatch.setattr(psycopg.Cursor, "execute", real)
    reads = [(q, p) for q, p in captured if "expand_edge" in q and "graph_rows" not in q
             and "fetched" in q]
    assert reads and all(p is False for _q, p in reads)
    assert "expand_edge.edge_num ASC" in reads[0][0]
    assert not [q for q, _p in captured if q.lstrip().upper().startswith("SET ")
                or "set_config('enable" in q or "plan_cache_mode" in q]


def test_expand_record_statements_read_the_graph_and_are_unprepared(
    world: _World, monkeypatch,
) -> None:
    """The record rows of a filtered 1-hop expand (issue #3666) on the graph
    path: the page and its far-end set read the Chronos rows, sent
    unprepared, and never the materialized copy."""
    captured: list[tuple[str, Any]] = []
    real = psycopg.Cursor.execute

    def capture(self, query, params=None, **kw):
        captured.append((str(query), kw.get("prepare")))
        return real(self, query, params, **kw)

    monkeypatch.setattr(psycopg.Cursor, "execute", capture)
    page = _expand(world, monkeypatch, "graph", world.gens[2],
                   input={"node_id": world.ids["a"]},
                   filter={"op": "exists", "field": "attrs.path"})
    monkeypatch.setattr(psycopg.Cursor, "execute", real)
    assert _far_ids(page, "records") == [world.ids["b"]]
    reads = [(q, p) for q, p in captured if "graph_rows" in q and "expand_link" in q]
    assert reads and all(p is False for _q, p in reads)
    assert not [q for q, _p in reads if "attribute_effective_view" in q]


# --------------------------------------------------------------------------
# Counts per list element (issue #3666 each()) and unprepared counts
# --------------------------------------------------------------------------

#: Graph lists, then overlays: one replaces a list, one sets a list where the
#: graph has none, one turns a list into a string.
_TAGS = {"a": ["x", "y", "x"], "b": ["y"], "c": "not-a-list", "d": [], "e": None,
         "f": ["z", None]}


def _each_world(world: _World) -> list[str]:
    ids = world.ids
    with psycopg.connect(world.dsn, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [NodeFact(ids[k], "doc", {"path": f"{k}.py", "tags": v}, {"id": ids[k]},
                      {"v": 2}) for k, v in _TAGS.items()],
            source="default", strict_endpoints=True,
        )
    gen = world.publish()
    world.overlay(gen, "node", ids["a"], "tags", ["q"])
    world.overlay(gen, "node", ids["g"], "tags", ["x", "q"])
    world.overlay(gen, "node", ids["b"], "tags", "now-a-string")
    with psycopg.connect(world.dsn, autocommit=True) as conn:
        materialize_effective_attributes_for_generation(conn, generation_id=gen)
    return [gen, world.publish()]


def _each_groups(world: _World, monkeypatch, mode: str, gen: str) -> tuple[Any, dict]:
    report: dict[str, Any] = {}

    def run(mcp):
        with psycopg.connect(world.dsn, autocommit=True) as conn:
            session = open_session(conn, deployment_name="default",
                                   requested_generation=gen)
            return mcp._okg_effective_view_aggregate_groups(
                conn, session, kind="nodes", by=["each(attrs.tags)"],
                predicate=DOC, each_report=report,
            )

    groups, _fingerprint = _with_server(world, monkeypatch, mode, run)
    return groups, report


def test_each_counts_equal_the_recompute_with_overlaid_lists(
    world: _World, monkeypatch,
) -> None:
    """``each(attrs.tags)`` on the graph path (lane SG dev merge, 10-19): one
    group per distinct element of each row's list (JSON null left out), a
    row with no element under None, a value that is not a list left out of
    the groups and counted in the facts. A target overlaid on the list is
    counted on its merged list. Groups and facts equal a recompute from the
    Chronos rows plus the overlays."""
    gens = _each_world(world)
    for gen in gens:
        groups, report = _each_groups(world, monkeypatch, "graph", gen)
        expected: Counter = Counter()
        none = not_a_list = 0
        rows = _recompute(world, gen, "nodes", DOC)
        for row in rows:
            value = row["attrs"].get("tags")
            if isinstance(value, list):
                elements = {json.dumps(e, sort_keys=True) for e in value if e is not None}
                for element in elements:
                    expected[json.dumps([json.loads(element)], sort_keys=True)] += 1
                if not elements:
                    none += 1
            elif value is None:
                none += 1
            else:
                not_a_list += 1
        if none:
            expected[json.dumps([None])] += none
        assert _as_counter(groups) == expected, gen
        assert report["rows_counted"] == len(rows), report
        assert report["rows_with_none"] == none, report
        assert report["rows_not_a_list"] == not_a_list, report
        if gen == gens[-1]:
            # Only the last generation shows the overlays (the first is read
            # before they are visible): c and b="now-a-string" are not lists,
            # a's graph list is gone, g's overlaid list counts.
            assert not_a_list == 2, report
            assert expected[json.dumps(["q"])] == 2
            assert expected[json.dumps(["y"])] == 0


def test_graph_counts_under_the_count_budget_are_unprepared(
    world: _World, monkeypatch,
) -> None:
    """A link count and an each() count run under the count budget
    (``aggregate_each.run_within_budget``); on the graph path both the group
    statement and the facts statement are sent unprepared, like every other
    graph-path statement (design 2.8)."""
    gens = _each_world(world)
    captured: list[tuple[str, Any]] = []
    real = psycopg.Cursor.execute

    def capture(self, query, params=None, **kw):
        captured.append((str(query), kw.get("prepare")))
        return real(self, query, params, **kw)

    monkeypatch.setattr(psycopg.Cursor, "execute", capture)
    _groups(world, monkeypatch, "graph", gens[-1], "edges", ["attrs.weight"], REFS)
    _each_groups(world, monkeypatch, "graph", gens[-1])
    monkeypatch.setattr(psycopg.Cursor, "execute", real)
    split = [(q, p) for q, p in captured if "AS split" in q]
    assert any("each_element" in q for q, _p in split), split
    assert any("FILTER (WHERE" in q for q, _p in split), split
    assert any("edge_num" in q for q, _p in split), split
    assert split and all(p is False for _q, p in split), split


# --------------------------------------------------------------------------
# Broad-scope split aggregate: the graph branch reads by a sequential scan
# --------------------------------------------------------------------------

#: The graph branch's visibility test and scope column as no index serves
#: them (graph_read._VISIBILITY_UNINDEXED, graph_read._SCOPE_UNINDEXED).
_SEQ_VISIBLE = "(live_lo + 0) <= %(graph_bp)s AND %(graph_bp)s < (live_hi + 0)"
_SEQ_SCOPE = {"nodes": "(subtype || ''::text) AS subtype",
              "edges": "(edge_type || ''::text) AS edge_type"}


def _split_statements(monkeypatch, fn) -> list[str]:
    """The split statements (groups, and each() facts) ``fn`` sends."""
    captured: list[str] = []
    real = psycopg.Cursor.execute

    def capture(self, query, params=None, **kw):
        captured.append(str(query))
        return real(self, query, params, **kw)

    monkeypatch.setattr(psycopg.Cursor, "execute", capture)
    try:
        fn()
    finally:
        monkeypatch.setattr(psycopg.Cursor, "execute", real)
    return [q for q in captured if "AS split" in q]


def _assert_branch_forms(statement: str, kind: str, *, sequential: bool) -> None:
    graph_branch, sep, overlay_branch = statement.partition(" UNION ALL ")
    assert sep, statement
    assert (_SEQ_VISIBLE in graph_branch) is sequential, graph_branch
    assert (_SEQ_SCOPE[kind] in graph_branch) is sequential, graph_branch
    # The overlay branch fetches its few targets by id either way.
    assert "+ 0)" not in overlay_branch and "|| ''::text" not in overlay_branch


def _broad_scope_calls(monkeypatch, broad: bool) -> list[str]:
    calls: list[str] = []

    def fake(cur, *, kind, segment, predicate):
        calls.append(kind)
        return {"scope_rows": 1e9, "segment_pages": 1} if broad else None

    monkeypatch.setattr(graph_read, "broad_scope", fake)
    return calls


@pytest.mark.parametrize("broad", [True, False], ids=["broad", "narrow"])
@pytest.mark.parametrize("kind,by,predicate", [
    pytest.param("nodes", ["attrs.owner"], DOC, id="nodes-by-overlaid-key"),
    pytest.param("nodes", ["attrs.tag"], {"op": "and", "clauses": [
        DOC, {"op": "eq", "field": "attrs.owner", "value": "alice"}]}, id="nodes-and-filter"),
    pytest.param("edges", ["attrs.weight"], REFS, id="edges-by-overlaid-key"),
])
def test_a_broad_scope_count_reads_the_graph_branch_sequentially(
    world: _World, monkeypatch, broad: bool, kind: str, by: list[str], predicate: Any,
) -> None:
    """Lane SG AGG-DIAG (f_aggregate, 10-20): a keyed count whose scope covers
    about every heap page (``graph_read.broad_scope``) writes its graph
    branch's visibility test and scope column through expressions no index
    serves, so that branch, which detoasts every attrs blob of the scope, is
    read in heap order instead of a node-id-order index walk. A narrow scope
    keeps the indexable form. The groups equal the recompute at every pin
    either way."""
    calls = _broad_scope_calls(monkeypatch, broad)
    statements = _split_statements(
        monkeypatch, lambda: _assert_groups(world, monkeypatch, kind, by, predicate),
    )
    assert statements and calls and set(calls) == {kind}, (calls, statements)
    for statement in statements:
        _assert_branch_forms(statement, kind, sequential=broad)


@pytest.mark.parametrize("broad", [True, False], ids=["broad", "narrow"])
def test_a_broad_scope_each_count_reads_both_statements_sequentially(
    world: _World, monkeypatch, broad: bool,
) -> None:
    """The each() groups and its facts statement read their graph branch the
    same way, and give the same groups and facts either way at every pin."""
    gens = _each_world(world)
    calls = _broad_scope_calls(monkeypatch, broad)
    got: dict[str, Any] = {}

    def run() -> None:
        for gen in gens:
            got[gen] = _each_groups(world, monkeypatch, "graph", gen)

    statements = _split_statements(monkeypatch, run)
    monkeypatch.setattr(graph_read, "broad_scope", lambda *_a, **_k: (
        {"scope_rows": 1e9, "segment_pages": 1} if not broad else None))
    for gen in gens:
        other_form = _each_groups(world, monkeypatch, "graph", gen)
        assert (sorted(map(str, got[gen][0])), got[gen][1]) == (
            sorted(map(str, other_form[0])), other_form[1]), gen
    groups = [s for s in statements if "GROUP BY" in s]
    facts = [s for s in statements if "GROUP BY" not in s]
    assert groups and facts, statements
    # One estimate per graph count (each pin), shared by its two statements.
    assert calls == ["nodes"] * len(gens), calls
    for statement in statements:
        _assert_branch_forms(statement, "nodes", sequential=broad)


def test_the_broad_scope_form_needs_attrs_and_no_reference_ids(
    world: _World, monkeypatch,
) -> None:
    """A count that reads no attribute (one GROUP BY over the graph rows) and
    a result reference's count (its ids narrow the rows) never ask for the
    scope estimate and keep the indexable form, even when every scope is
    called broad. A keyed count over the whole scope, run after them, is the
    only one that asks."""
    calls = _broad_scope_calls(monkeypatch, True)
    members = [world.ids["a"], world.ids["c"], world.ids["e"], world.ids["f"]]
    before = [0]

    def run() -> None:
        _assert_groups(world, monkeypatch, "nodes", ["subtype"], DOC)
        _assert_groups(world, monkeypatch, "nodes", ["attrs.owner"], DOC, member_ids=members)
        before[0] = len(calls)
        _groups(world, monkeypatch, "graph", world.gens[-1], "nodes", ["attrs.owner"], DOC)

    captured: list[str] = []
    real = psycopg.Cursor.execute

    def capture(self, query, params=None, **kw):
        captured.append(str(query))
        return real(self, query, params, **kw)

    monkeypatch.setattr(psycopg.Cursor, "execute", capture)
    try:
        run()
    finally:
        monkeypatch.setattr(psycopg.Cursor, "execute", real)
    graph_reads = [q for q in captured if "%(graph_bp)s" in q]
    assert graph_reads, captured
    keyed_whole_scope = graph_reads[-1]
    assert "AS split" in keyed_whole_scope, keyed_whole_scope
    assert not [q for q in graph_reads[:-1] if "+ 0)" in q or "|| ''::text" in q]
    _assert_branch_forms(keyed_whole_scope, "nodes", sequential=True)
    assert before[0] == 0 and calls == ["nodes"], (before, calls)


def test_an_analyzed_scope_takes_the_sequential_form_without_forcing(
    world: _World, monkeypatch,
) -> None:
    """With planner statistics, the fixture's ``doc`` scope holds more rows
    than its heap pages, so the real estimate calls it broad; the groups
    still equal the recompute."""
    from tests.substrate.mcp.test_graph_read_path_db import _analyze

    _analyze(world)
    with psycopg.connect(world.dsn, autocommit=True) as conn, conn.cursor() as cur:
        from okg.substrate.mcp.semantic_readiness import resolve_node_segment

        assert graph_read.broad_scope(
            cur, kind="nodes", segment=resolve_node_segment(conn), predicate=DOC,
        ) is not None
    statements = _split_statements(
        monkeypatch, lambda: _assert_groups(world, monkeypatch, "nodes", ["attrs.owner"], DOC),
    )
    assert statements
    for statement in statements:
        _assert_branch_forms(statement, "nodes", sequential=True)
