"""The graph read path for filter, map, lookup and references (PostgreSQL).

PACT lane-s-storage-redesign-s2, requirements ``read-single-key``,
``read-multi-key``, ``raw-attrs``, ``plan-cache`` (unprepared part),
``gate-production-settings``, ``pinned-parity`` (graph part) and
``nomos-unchanged``; design sections 2.2-2.3 and 2.8.

Since PACT lane-s-storage-cutover the graph path is the only read path.
Every page read here is compared with an independent recompute in Python
from the Chronos rows at the pin plus that generation's overlay rows
(PACT lane-s-storage-drop-copies dropped the materialized copy it was also
compared with). The
fixture holds overlays that move a value into and out of a match, an overlay
equal to the graph value, keys missing and JSON null, node ids whose order
differs between byte order and the database collation, edges, and three
generations whose overlays change, each read pinned.
"""
from __future__ import annotations

import json
import re
import uuid
from typing import Any

import psycopg
import pytest

from okg.substrate import declared_attr_indexes as dai
from okg.substrate.attributes import (
    ATTRIBUTE_EFFECTIVE_TABLE,
    ATTRIBUTE_OBSERVATION_TABLE,
    RESERVED_EFFECTIVE_ATTR_KEYS,
    materialize_effective_attributes_for_generation,
    record_attribute_observation,
    reject_attribute_observation,
)
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.mcp import graph_read, open_session
from okg.substrate.mcp import server as server_mod
from okg.substrate.mcp.passage_rollup import resolve_edge_segment
from okg.substrate.mcp.semantic_readiness import resolve_node_segment
from okg.substrate.nomos.source_lineage import LINEAGE_ATTR
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import (
    SubtypeSpec,
    bind_test_catalog_writer,
    seed_catalog,
)

pytestmark = pytest.mark.postgres

_ABSENT = object()


# --------------------------------------------------------------------------
# Fixture: three generations with changing overlays
# --------------------------------------------------------------------------


class _World:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        s = uuid.uuid4().hex[:6]
        # Ids whose byte order and collation order can differ (case,
        # punctuation, digits).
        self.ids = {
            "a": f"doc:Alpha.Py:{s}", "b": f"doc:alpha-py:{s}",
            "c": f"doc:_under:{s}", "d": f"doc:Zeta:{s}",
            "e": f"doc:beta/1:{s}", "f": f"doc:beta.10:{s}", "g": f"doc:beta.9:{s}",
        }
        graph_attrs = {
            "a": {"path": "a.py", "owner": "alice", "rank": 1, "tag": "x"},
            "b": {"path": "b.py", "owner": None, "rank": 2},
            "c": {"path": "c.py", "rank": 3},
            "d": {"path": "d.py", "owner": "bob", "rank": 4, "tag": "y"},
            "e": {"owner": "alice", "rank": 5},
            "f": {"path": "f.py", "owner": "carol", "rank": 6, "tag": "x"},
            "g": {"path": "g.py", "owner": "alice", "rank": 7},
        }
        self.clock = f"clock:{s}"
        self.tick = 0
        with psycopg.connect(dsn, autocommit=True) as conn:
            seed_catalog(conn, subtypes=[
                SubtypeSpec(name="doc", metadata={
                    "filterable_attrs": {"path": "text", "rank": "num"},
                }),
                SubtypeSpec(name="clock"),
            ])
            store = ChronosGraphStore.from_connection(conn)
            store.apply_facts(
                [NodeFact(self.ids[k], "doc", v, {"id": self.ids[k]}, {"v": 1})
                 for k, v in graph_attrs.items()],
                source="default", strict_endpoints=True,
            )
            store.apply_facts(
                [
                    EdgeFact(self.ids["a"], self.ids["b"], "references",
                             {"weight": 1, "label": "ab"},
                             source_record_id={"id": f"ab{s}"}, source_revision={"v": 1}),
                    EdgeFact(self.ids["b"], self.ids["c"], "references",
                             {"weight": 2},
                             source_record_id={"id": f"bc{s}"}, source_revision={"v": 1}),
                    EdgeFact(self.ids["c"], self.ids["d"], "references",
                             {"weight": 3, "label": "cd"},
                             source_record_id={"id": f"cd{s}"}, source_revision={"v": 1}),
                ],
                source="default",
            )
        self.gens: list[str] = []
        self.gens.append(self.publish())
        g0 = self.gens[0]
        self.edge_ids = sorted(
            int(r[0]) for r in self.query(
                g0, "SELECT edge_id FROM okg.graph_edges", kind_rows=True,
            )
        )
        # Overlays at g0, materialized into g0 (the standard post-publish path).
        self.obs = {
            "a.owner": self.overlay(g0, "node", self.ids["a"], "owner", "zed"),
            "c.owner": self.overlay(g0, "node", self.ids["c"], "owner", "alice"),
            "d.path": self.overlay(g0, "node", self.ids["d"], "path", "a.py"),
            "a.path": self.overlay(g0, "node", self.ids["a"], "path", "a.py"),
            "f.tag": self.overlay(g0, "node", self.ids["f"], "tag", "y"),
            "e1.weight": self.overlay(g0, "edge", str(self.edge_ids[0]), "weight", 99),
        }
        with psycopg.connect(dsn, autocommit=True) as conn:
            materialize_effective_attributes_for_generation(conn, generation_id=g0)
        self.gens.append(self.publish())  # g1: carried forward
        g1 = self.gens[1]
        self.overlay(g1, "node", self.ids["a"], "owner", "alice",
                     supersedes=self.obs["a.owner"])  # back into the match
        with psycopg.connect(dsn, autocommit=True) as conn:
            reject_attribute_observation(conn, self.obs["f.tag"], reason="test")
        self.gens.append(self.publish())  # g2: a.owner changed, f.tag gone

    def publish(self) -> str:
        self.tick += 1
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            ChronosGraphStore.from_connection(conn).apply_facts(
                [NodeFact(self.clock, "clock", {"tick": self.tick},
                          {"id": self.clock}, {"v": self.tick})],
                source="default", strict_endpoints=True,
            )
        out = Publisher(
            self.dsn, "default",
            catalog_identity=bind_test_catalog_writer(self.dsn, "default"),
        ).cycle()
        assert out["status"] == "published" and not out.get("noop"), out
        return str(out["generation_id"])

    def overlay(self, gen: str, kind: str, target: str, key: str, value: Any,
                *, supersedes: str | None = None) -> str:
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            return record_attribute_observation(
                conn, target_kind=kind, target_id=target, generation_id=gen,
                attr_key=key, value=value, producer="test:graph-read",
                supersedes=supersedes,
            ).observation_id

    def query(self, gen: str, sql: str, *, kind_rows: bool = False) -> list:
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            checkout = ChronosGraphStore.from_connection(
                conn, ensure_schema=False,
            ).checkout_generation(gen)
            rows = checkout.query(sql, {})
            return [tuple(r.values()) if isinstance(r, dict) else tuple(r) for r in rows]


@pytest.fixture
def world(clean_runtime: str) -> _World:
    return _World(clean_runtime)


# --------------------------------------------------------------------------
# Independent recompute
# --------------------------------------------------------------------------


def _json_eq(a: Any, b: Any) -> bool:
    if isinstance(a, bool) or isinstance(b, bool):
        return type(a) is type(b) and a == b
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return a == b
    if isinstance(a, dict) and isinstance(b, dict):
        return a.keys() == b.keys() and all(_json_eq(a[k], b[k]) for k in a)
    if isinstance(a, list) and isinstance(b, list):
        return len(a) == len(b) and all(_json_eq(x, y) for x, y in zip(a, b))
    return type(a) is type(b) and a == b


def _field(row: dict, field: str) -> Any:
    if field == "attrs":
        return row["attrs"]
    if field.startswith("attrs."):
        return row["attrs"].get(field[6:], _ABSENT)
    return row.get({"node_id": "id", "edge_id": "id"}.get(field, field), _ABSENT)


def _matches(row: dict, p: Any) -> bool:
    if p is None:
        return True
    op = p["op"]
    if op == "and":
        return all(_matches(row, c) for c in p["clauses"])
    if op == "or":
        return any(_matches(row, c) for c in p["clauses"])
    if op == "not":
        return not _matches(row, p["clause"])
    if op == "contains":
        return all(_matches(row, {"op": "eq", "field": f"attrs.{k}", "value": v})
                   for k, v in p["value"].items())
    value = _field(row, p["field"])
    if op == "exists":
        # Presence, JSON null included (the lowerer's `attrs ? key`); a
        # stable column exists when it is not NULL.
        if p["field"].startswith("attrs."):
            return value is not _ABSENT
        return value is not _ABSENT and value is not None
    if op == "eq":
        if p["value"] is None:
            return value is _ABSENT or value is None
        return value is not _ABSENT and _json_eq(value, p["value"])
    if op == "in":
        return any(_matches(row, {"op": "eq", "field": p["field"], "value": v})
                   for v in p["values"])
    if op == "gt":
        return (isinstance(value, (int, float)) and not isinstance(value, bool)
                and value > p["value"])
    if op == "prefix":
        return isinstance(value, str) and value.startswith(p["value"])
    raise AssertionError(f"recompute has no rule for {op}")


def _recompute(world: _World, gen: str, kind: str, predicate: Any) -> list[dict]:
    if kind == "nodes":
        graph = world.query(gen, "SELECT node_id, subtype, attrs FROM okg.graph_nodes")
        rows = {str(i): {"id": str(i), "subtype": st, "attrs": dict(a)} for i, st, a in graph}
    else:
        graph = world.query(
            gen, "SELECT edge_id, src, dst, edge_type, attrs FROM okg.graph_edges",
        )
        rows = {str(i): {"id": str(i), "src": s, "dst": d, "edge_type": t,
                         "attrs": dict(a)} for i, s, d, t, a in graph}
    with psycopg.connect(world.dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT target_id, attr_key, value FROM {ATTRIBUTE_EFFECTIVE_TABLE} "
            "WHERE generation_id = %s AND target_kind = %s AND namespace = 'default'",
            (gen, "node" if kind == "nodes" else "edge"),
        )
        for target, key, value in cur.fetchall():
            if key not in RESERVED_EFFECTIVE_ATTR_KEYS and target in rows:
                rows[target]["attrs"][key] = value
        matched = [r for r in rows.values() if _matches(r, predicate)]
        if kind == "edges":
            return sorted(matched, key=lambda r: int(r["id"]))
        cur.execute("SELECT x FROM unnest(%s::text[]) AS x ORDER BY x",
                    ([r["id"] for r in matched],))
        order = [r[0] for r in cur.fetchall()]
    return sorted(matched, key=lambda r: order.index(r["id"]))


# --------------------------------------------------------------------------
# The page read
# --------------------------------------------------------------------------


def _page(world: _World, monkeypatch, mode: str, gen: str, kind: str,
          predicate: Any, **kwargs: Any) -> list[dict]:
    """The page as ``_effective_view_sql_page`` reads it. ``mode`` is kept
    for the callers' symmetry: the graph path is the only one."""
    assert mode == "graph", mode
    mcp = server_mod.build_server(dsn=world.dsn, deployment_name="default")
    try:
        with psycopg.connect(world.dsn, autocommit=True) as conn:
            session = open_session(conn, deployment_name="default",
                                   requested_generation=gen)
            rows, _more = mcp._okg_effective_view_sql_page(
                conn, session, kind=kind, predicate=predicate,
                limit=kwargs.pop("limit", 500), offset=0, **kwargs,
            )
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()
    return rows


def _both(world: _World, monkeypatch, gen: str, kind: str, predicate: Any,
          **kwargs: Any) -> dict[str, Any]:
    """``{"graph": the page}``: the graph path is the only read path (PACT
    lane-s-storage-cutover); callers compare it with the recompute."""
    return {"graph": _page(world, monkeypatch, "graph", gen, kind, predicate,
                           **dict(kwargs))}


def _scoped(predicate: Any, kind: str) -> Any:
    scope = ({"op": "eq", "field": "subtype", "value": "doc"} if kind == "nodes"
             else {"op": "eq", "field": "edge_type", "value": "references"})
    return scope if predicate is None else {"op": "and", "clauses": [scope, predicate]}


def _assert_parity(world: _World, monkeypatch, kind: str, predicate: Any) -> None:
    id_field = "node_id" if kind == "nodes" else "edge_id"
    scoped = _scoped(predicate, kind)
    for gen in world.gens:
        expected = _recompute(world, gen, kind, scoped)
        got = _both(world, monkeypatch, gen, kind, scoped)["graph"]
        assert [str(r[id_field]) for r in got] == [r["id"] for r in expected], (gen, predicate)
        assert [r["attrs"] for r in got] == [r["attrs"] for r in expected], (gen, predicate)


NODE_PREDICATES = [
    pytest.param(None, id="no-predicate"),
    pytest.param({"op": "eq", "field": "attrs.owner", "value": "alice"}, id="undeclared-eq"),
    pytest.param({"op": "eq", "field": "attrs.path", "value": "a.py"}, id="declared-eq"),
    pytest.param({"op": "not", "clause": {"op": "eq", "field": "attrs.owner", "value": "alice"}},
                 id="not-eq"),
    pytest.param({"op": "eq", "field": "attrs.owner", "value": None}, id="null-eq"),
    pytest.param({"op": "eq", "field": "attrs.path", "value": None}, id="declared-null-eq"),
    pytest.param({"op": "exists", "field": "attrs.owner"}, id="exists"),
    pytest.param({"op": "not", "clause": {"op": "exists", "field": "attrs.tag"}},
                 id="not-exists"),
    pytest.param({"op": "and", "clauses": [
        {"op": "eq", "field": "attrs.owner", "value": "alice"},
        {"op": "eq", "field": "attrs.tag", "value": "x"}]}, id="and-two-keys"),
    pytest.param({"op": "or", "clauses": [
        {"op": "eq", "field": "attrs.owner", "value": "alice"},
        {"op": "eq", "field": "attrs.tag", "value": "y"}]}, id="or-two-keys"),
    # a is overlaid on both keys (owner at g0 and g1, path at g0): it must
    # appear once, whether one or both branches of the OR match.
    pytest.param({"op": "or", "clauses": [
        {"op": "eq", "field": "attrs.owner", "value": "alice"},
        {"op": "eq", "field": "attrs.path", "value": "a.py"}]}, id="or-two-overlaid-keys"),
    pytest.param({"op": "and", "clauses": [
        {"op": "eq", "field": "attrs.owner", "value": "alice"},
        {"op": "eq", "field": "attrs.path", "value": "a.py"}]}, id="and-two-overlaid-keys"),
    pytest.param({"op": "contains", "field": "attrs", "value": {"owner": "alice"}},
                 id="contains"),
    pytest.param({"op": "gt", "field": "attrs.rank", "value": 3}, id="declared-range"),
    pytest.param({"op": "in", "field": "attrs.path", "values": ["a.py", "g.py"]}, id="in"),
    pytest.param({"op": "prefix", "field": "attrs.path", "value": "a"}, id="prefix"),
]


@pytest.mark.parametrize("predicate", NODE_PREDICATES)
def test_node_pages_equal_the_recompute(
    world: _World, monkeypatch, predicate: Any,
) -> None:
    _assert_parity(world, monkeypatch, "nodes", predicate)


def test_whole_attrs_equality_sends_every_overlaid_target_to_the_overlay_branch(
    world: _World, monkeypatch,
) -> None:
    """c's overlay adds owner=alice. An equality with c's graph attrs must not
    match (its effective attrs differ) and one with its effective attrs must,
    at every generation; a version that treats the whole-attrs predicate as
    keyed (no overlay on a predicate key) answers c from its graph value."""
    target = world.ids["c"]
    for gen in world.gens:
        graph_attrs = dict(next(
            a for i, a in world.query(gen, "SELECT node_id, attrs FROM okg.graph_nodes")
            if i == target
        ))
        [effective] = _recompute(world, gen, "nodes", {
            "op": "eq", "field": "node_id", "value": target})
        assert effective["attrs"] != graph_attrs
        for value, expected in ((graph_attrs, []), (effective["attrs"], [target])):
            predicate = _scoped({"op": "eq", "field": "attrs", "value": value}, "nodes")
            pages = _both(world, monkeypatch, gen, "nodes", predicate)
            assert [r["node_id"] for r in pages["graph"]] == expected, gen


@pytest.mark.parametrize("predicate", [
    pytest.param(None, id="no-predicate"),
    pytest.param({"op": "eq", "field": "attrs.weight", "value": 99}, id="overlaid-eq"),
    pytest.param({"op": "gt", "field": "attrs.weight", "value": 1}, id="range"),
    pytest.param({"op": "exists", "field": "attrs.label"}, id="exists"),
])
def test_edge_pages_are_in_numeric_id_order_and_equal(
    world: _World, monkeypatch, predicate: Any,
) -> None:
    _assert_parity(world, monkeypatch, "edges", predicate)


def _crossing_edge_ends(world: _World) -> list[str]:
    """Four new node ids whose edges from ``a`` have ids that order
    differently as text and as numbers: the first edge's id has 18 digits and
    a leading digit of 2 or more, the other three have 19 digits and lead
    with 1 (a text sort puts those three first)."""
    from okg.substrate.graph_store_internal.common import edge_id

    short: list[str] = []
    long: list[str] = []
    n = 0
    while len(short) < 1 or len(long) < 3:
        n += 1
        end = f"doc:edge-end-{n}:{world.ids['a']}"
        text = str(edge_id(world.ids["a"], "references", end))
        if len(text) == 18 and text[0] >= "2" and not short:
            short.append(end)
        elif len(text) == 19 and text[0] == "1" and len(long) < 3:
            long.append(end)
    return short + long


@pytest.mark.parametrize("predicate", [
    pytest.param({"op": "eq", "field": "attrs.weight", "value": 5}, id="eq"),
    pytest.param({"op": "not", "clause": {"op": "eq", "field": "attrs.weight", "value": 7}},
                 id="not-eq"),
    pytest.param({"op": "eq", "field": "attrs.label", "value": None}, id="null-eq"),
])
def test_edge_ids_that_sort_differently_as_text_page_in_numeric_order(
    world: _World, monkeypatch, predicate: Any,
) -> None:
    """Edges A (18-digit id) and B, C, D (19-digit ids led by 1): numeric
    order puts A first, text order last. Overlays move B into the weight
    match and D out of the null-label match; each page holds A and at least
    one of B, C and equals the recompute in edge-id
    order at every generation."""
    ends = _crossing_edge_ends(world)
    with psycopg.connect(world.dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact(end, "doc", {"path": f"end{i}.py"}, {"id": end}, {"v": 1})
             for i, end in enumerate(ends)],
            source="default", strict_endpoints=True,
        )
        store.apply_facts(
            [EdgeFact(world.ids["a"], end, "references", attrs,
                      source_record_id={"id": end}, source_revision={"v": 1})
             for end, attrs in zip(ends, (
                 {"weight": 5},                     # A
                 {"weight": 7},                     # B: overlaid to 5
                 {"weight": 5, "label": None},      # C
                 {"weight": 8},                     # D: label overlaid
             ))],
            source="default",
        )
    gen = world.publish()
    world.gens.append(gen)
    from okg.substrate.graph_store_internal.common import edge_id

    a, b, c, d = (str(edge_id(world.ids["a"], "references", end)) for end in ends)
    world.overlay(gen, "edge", b, "weight", 5)
    world.overlay(gen, "edge", d, "label", "d")
    with psycopg.connect(world.dsn, autocommit=True) as conn:
        materialize_effective_attributes_for_generation(conn, generation_id=gen)
    assert sorted([a, b, c, d]) != sorted([a, b, c, d], key=int)
    _assert_parity(world, monkeypatch, "edges", predicate)
    page = [str(r["edge_id"]) for r in _both(
        world, monkeypatch, gen, "edges", _scoped(predicate, "edges"))["graph"]]
    assert a in page and ({b, c} & set(page)), page
    assert page == sorted(page, key=int) and page != sorted(page), page


def _raw_attrs_by_mode(world: _World, monkeypatch, gen: str) -> dict[str, dict[str, Any]]:
    """``{mode: {target_id: raw_attrs}}`` captured at the SQL page (before
    Nomos) for every doc node at ``gen``."""
    seen: dict[str, dict[str, Any]] = {}
    real = psycopg.Cursor.execute

    for mode in ("graph",):
        captured: list[dict] = []

        def capture(self, query, params=None, **kw):
            out = real(self, query, params, **kw)
            if "effective_page.raw_attrs" in str(query):
                names = [d.name for d in self.description]
                captured.extend(r if isinstance(r, dict) else dict(zip(names, r))
                                for r in self.fetchall())
                real(self, query, params, **kw)
            return out

        monkeypatch.setattr(psycopg.Cursor, "execute", capture)
        _page(world, monkeypatch, mode, gen, "nodes", _scoped(None, "nodes"))
        monkeypatch.setattr(psycopg.Cursor, "execute", real)
        seen[mode] = {r["target_id"]: r["raw_attrs"] for r in captured}
    return seen


def test_raw_attrs_is_the_graph_attrs_exactly_when_an_overlay_row_exists(
    world: _World, monkeypatch,
) -> None:
    """Captured at the SQL page (before Nomos): no overlay -> NULL; an overlay
    that changes a value -> graph attrs; an overlay equal to the graph value
    -> graph attrs (a NULLIF read fails here)."""
    seen = _raw_attrs_by_mode(world, monkeypatch, world.gens[1])
    graph = seen["graph"]
    stored = {
        i: dict(a) for i, a in world.query(
            world.gens[1], "SELECT node_id, attrs FROM okg.graph_nodes",
        )
    }
    assert graph[world.ids["b"]] is None  # no overlay
    assert graph[world.ids["d"]] == stored[world.ids["d"]]  # path overlaid away
    assert graph[world.ids["d"]]["path"] == "d.py"
    # a: path overlaid with its own graph value (and owner changed).
    assert graph[world.ids["a"]] == stored[world.ids["a"]]
    assert graph[world.ids["a"]]["owner"] == "alice"


def test_an_overlay_only_on_a_reserved_key_leaves_raw_attrs_null(
    world: _World, monkeypatch,
) -> None:
    """b has no overlay; a planted per-generation row on the reserved lineage
    key (rows the materializer no longer writes, but older generations may
    hold) is not an overlay: raw_attrs stays NULL and attrs stay the graph
    attrs (the graph row's own lineage stamp kept), equal to
    the recompute. A graph path that counts reserved rows as overlays
    returns the graph attrs as raw_attrs and merges the planted value."""
    gen = world.gens[-1]
    target = world.ids["b"]
    with psycopg.connect(world.dsn, autocommit=True) as conn:
        observation = record_attribute_observation(
            conn, target_kind="node", target_id=target, generation_id=gen,
            attr_key=LINEAGE_ATTR, value={"source": "planted"},
            producer="test:graph-read",
        ).observation_id
        conn.execute(
            f"INSERT INTO {ATTRIBUTE_EFFECTIVE_TABLE} (target_kind, target_id, "
            "generation_id, namespace, attr_key, observation_id, value, value_hash, "
            "producer) SELECT target_kind, target_id, generation_id, namespace, "
            f"attr_key, observation_id, value, value_hash, producer "
            f"FROM {ATTRIBUTE_OBSERVATION_TABLE} WHERE observation_id = %s",
            (observation,),
        )
        reserved_rows = conn.execute(
            f"SELECT attr_key FROM {ATTRIBUTE_EFFECTIVE_TABLE} "
            "WHERE generation_id = %s AND target_id = %s",
            (gen, target),
        ).fetchall()
    assert reserved_rows == [(LINEAGE_ATTR,)]
    seen = _raw_attrs_by_mode(world, monkeypatch, gen)
    assert seen["graph"][target] is None
    predicate = _scoped({"op": "eq", "field": "node_id", "value": target}, "nodes")
    [expected] = _recompute(world, gen, "nodes", predicate)
    pages = _both(world, monkeypatch, gen, "nodes", predicate)
    assert [r["attrs"] for r in pages["graph"]] == [expected["attrs"]]
    # The graph row carries its own lineage stamp; the planted row never
    # replaces it.
    stamp = pages["graph"][0]["attrs"].get(LINEAGE_ATTR)
    assert stamp is not None and stamp != {"source": "planted"}, stamp


def test_member_ids_restrict_both_paths_equally(world: _World, monkeypatch) -> None:
    members = [world.ids["a"], world.ids["c"], world.ids["e"]]
    predicate = _scoped({"op": "eq", "field": "attrs.owner", "value": "alice"}, "nodes")
    for gen in world.gens:
        pages = _both(world, monkeypatch, gen, "nodes", predicate, member_ids=members)
        expected = [r["id"] for r in _recompute(world, gen, "nodes", predicate)
                    if r["id"] in members]
        assert [r["node_id"] for r in pages["graph"]] == expected, gen


# --------------------------------------------------------------------------
# The bounded count and the merged scope (the S2 gate's five slow shapes)
# --------------------------------------------------------------------------

EDGE_PREDICATES = [
    None,
    {"op": "eq", "field": "attrs.weight", "value": 99},
    {"op": "gt", "field": "attrs.weight", "value": 1},
    {"op": "exists", "field": "attrs.label"},
    {"op": "eq", "field": "attrs", "value": {"weight": 2}},
]


@pytest.fixture
def mcp(world: _World):
    """One server for a test: the switch is read on every call."""
    built = server_mod.build_server(dsn=world.dsn, deployment_name="default")
    try:
        yield built
    finally:
        built._okg_pool.close()
        if built._okg_write_pool:
            built._okg_write_pool.close()


def _read(world: _World, mcp, monkeypatch, mode: str, gen: str, kind: str,
          predicate: Any, **kwargs: Any,
          ) -> tuple[list[dict], dict, tuple[int | None, str]] | None:
    """One row of the page, its page_meta and the bounded count of its whole
    set. ``mode`` is kept for the callers' symmetry: the graph path is the
    only one (PACT lane-s-storage-cutover)."""
    assert mode == "graph", mode
    with psycopg.connect(world.dsn, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default",
                               requested_generation=gen)
        meta: dict[str, Any] = {}
        rows, _more = mcp._okg_effective_view_sql_page(
            conn, session, kind=kind, predicate=predicate,
            limit=kwargs.pop("limit", 1), offset=kwargs.pop("offset", 0),
            page_meta=meta, **kwargs,
        )
        counted = mcp._okg_bounded_operator_count(
            conn, from_where_sql=meta["count_from_where_sql"],
            params=meta["params"],
        )
    return rows, meta, counted


def _all_cases() -> list[tuple[str, Any]]:
    return ([("nodes", p.values[0]) for p in NODE_PREDICATES]
            + [("nodes", {"op": "eq", "field": "attrs", "value": {"rank": 3}})]
            + [("edges", p) for p in EDGE_PREDICATES])


def _set_rates(monkeypatch, *, merged: bool) -> None:
    """Force the merged scope read (an overlay read priced far above any
    scope) or ids first (priced at nothing)."""
    monkeypatch.setattr(graph_read, "OVERLAY_SCAN_US_PER_ROW", 1e9 if merged else 0.0)


def _analyze(world: _World) -> None:
    """Planner statistics for the scope estimate (the test database only)."""
    with psycopg.connect(world.dsn, autocommit=True) as conn:
        for table in (resolve_node_segment(conn), resolve_edge_segment(conn),
                      ATTRIBUTE_EFFECTIVE_TABLE):
            conn.execute(f"ANALYZE {table}")


@pytest.mark.parametrize(("merged", "sequential"), [
    (False, False), (False, True), (True, False)],
    ids=["ids-first", "ids-first-sequential-count", "scope-merged"])
def test_the_bounded_count_is_the_page_set_on_both_paths(
    world: _World, mcp, monkeypatch, merged: bool, sequential: bool,
) -> None:
    """The bounded count over ``count_from_where_sql`` equals the recompute's
    row count, for every predicate form, both kinds,
    every generation, with and without result-reference members; the graph
    path's form is the one forced. Ids first counts the graph-id branch
    unordered with no fetch (graph_read.count_relation_sql): a count that
    lets an overlaid target through the graph-id branch, or drops the
    overlay-id branch, differs here."""
    _analyze(world)
    _set_rates(monkeypatch, merged=merged)
    if not sequential:
        monkeypatch.setattr(graph_read, "broad_scope", lambda *a, **k: None)
    members = [world.ids["a"], world.ids["c"], world.ids["d"], world.ids["f"]]
    for kind, predicate in _all_cases():
        scoped = _scoped(predicate, kind)
        for gen in world.gens:
            expected = _recompute(world, gen, kind, scoped)
            for member_ids in (None, members if kind == "nodes" else None):
                want = [r for r in expected
                        if member_ids is None or r["id"] in member_ids]
                extra = {} if member_ids is None else {"member_ids": member_ids}
                rows, meta, counted = _read(world, mcp, monkeypatch, "graph", gen, kind,
                                            scoped, **extra)
                assert counted == (len(want), "exact"), (kind, predicate, gen, member_ids)
                assert meta["graph_form"]["form"] == (
                    "scope_merged" if merged else "ids_first"), (kind, predicate)
                assert [str(r["node_id" if kind == "nodes" else "edge_id"]) for r in rows] \
                    == [r["id"] for r in want][:1], (kind, predicate, gen)
                count_sql = meta["count_from_where_sql"]
                # The broad-scope count reads the scope column through an
                # expression no index serves; analyzed, this fixture's scope
                # holds more rows than its one heap page.
                unindexed = ("(subtype || ''::text) AS subtype" if kind == "nodes"
                             else "(edge_type || ''::text) AS edge_type")
                # Index or path candidates narrow the ids, so the count never
                # turns the scope column unindexed (lane SG review finding).
                scanned = sequential and not meta["graph_form"]["candidates"]
                assert meta["graph_form"]["count_scan"] == (
                    "sequential" if scanned else "planner"), (kind, predicate)
                assert (unindexed in count_sql) is scanned, count_sql
                assert unindexed not in meta["from_where_sql"]
                keyed = bool(graph_read.predicate_attr_keys(scoped)) or \
                    graph_read.predicate_reads_whole_attrs(scoped)
                if merged:
                    # The page's own FROM/WHERE over the merged relation.
                    assert count_sql == meta["from_where_sql"]
                    assert "graph_rows" not in count_sql
                elif keyed:
                    assert count_sql.startswith(
                        "FROM (WITH counted_graph AS MATERIALIZED ("), count_sql
                    assert "counted_overlay AS MATERIALIZED (" in count_sql
                    assert "ORDER BY" not in count_sql
                    assert f"NOT EXISTS (SELECT 1 FROM {ATTRIBUTE_EFFECTIVE_TABLE}" \
                        not in count_sql
                    assert " NOT IN (SELECT o.target_id FROM " in count_sql
                else:
                    assert "counted_overlay" not in count_sql
                    assert "ORDER BY" not in count_sql


def test_declared_candidates_keep_the_count_off_the_sequential_scan(
    world: _World, mcp, monkeypatch,
) -> None:
    """Lane SG review finding: a broad scope plus a declared-key equality
    (the gate shape a_node_decl_eq) counts the few ids the declared index
    already narrowed; it must not turn the scope column unindexed, which
    forces a sequential count of the whole segment. ``broad_scope`` is
    forced to call every scope broad; an undeclared key, with no
    candidates, still takes the sequential count. The declared expression
    indexes are built (the reconciler's ``ready``) and the read is at the
    head, where the candidate set can be used."""
    report = dai.reconcile(world.dsn, deployment="default", apply=True, enable=True)
    assert report["status"] == "converged", report
    monkeypatch.delenv("OKG_ATTR_FILTER_INDEX", raising=False)
    _analyze(world)
    _set_rates(monkeypatch, merged=False)
    monkeypatch.setattr(graph_read, "broad_scope",
                        lambda *a, **k: {"forced_broad": True})
    unindexed = "(subtype || ''::text) AS subtype"
    gen = world.gens[-1]
    declared = _scoped({"op": "eq", "field": "attrs.path", "value": "a.py"}, "nodes")
    rows, meta, counted = _read(world, mcp, monkeypatch, "graph", gen, "nodes", declared)
    assert meta["graph_form"]["candidates"] is True, meta["graph_form"]
    assert meta["graph_form"]["count_scan"] == "planner", meta["graph_form"]
    assert unindexed not in meta["count_from_where_sql"]
    expected = [r["id"] for r in _recompute(world, gen, "nodes", declared)]
    assert counted == (len(expected), "exact")
    assert [r["node_id"] for r in rows] == expected[:1]

    undeclared = _scoped({"op": "eq", "field": "attrs.owner", "value": "alice"}, "nodes")
    rows, meta, counted = _read(world, mcp, monkeypatch, "graph", gen, "nodes", undeclared)
    assert meta["graph_form"]["candidates"] is False, meta["graph_form"]
    assert meta["graph_form"]["count_scan"] == "sequential", meta["graph_form"]
    assert unindexed in meta["count_from_where_sql"]
    expected = [r["id"] for r in _recompute(world, gen, "nodes", undeclared)]
    assert counted == (len(expected), "exact")
    assert [r["node_id"] for r in rows] == expected[:1]


@pytest.mark.parametrize("merged", [False, True], ids=["ids-first", "scope-merged"])
def test_both_graph_forms_page_like_the_recompute(
    world: _World, mcp, monkeypatch, merged: bool,
) -> None:
    """Every predicate form, both kinds, every generation, in pages of two at
    every offset: the merged scope read and ids first return the recompute's
    rows in its order."""
    _analyze(world)
    _set_rates(monkeypatch, merged=merged)
    for kind, predicate in _all_cases():
        id_field = "node_id" if kind == "nodes" else "edge_id"
        scoped = _scoped(predicate, kind)
        for gen in world.gens:
            expected = [r["id"] for r in _recompute(world, gen, kind, scoped)]
            for offset in range(0, len(expected) + 1, 2):
                rows, meta, _counted = _read(world, mcp, monkeypatch, "graph", gen, kind,
                                             scoped, limit=2, offset=offset)
                assert [str(r[id_field]) for r in rows] == expected[offset: offset + 2], (
                    kind, predicate, gen, offset)
                assert meta["graph_form"]["form"] == (
                    "scope_merged" if merged else "ids_first")
                if merged:
                    assert "graph_rows" not in meta["from_where_sql"]
                    assert "LEFT JOIN LATERAL" in meta["from_where_sql"]


def test_the_scope_choice_follows_the_statistics(world: _World) -> None:
    """Never analyzed: no estimate, ids first. Analyzed: the merged scope read
    exactly when merging the scope is priced no higher than reading the
    generation's overlay ids, and only for a pinned subtype or edge type."""
    doc = _scoped(None, "nodes")
    with psycopg.connect(world.dsn, autocommit=True) as conn, conn.cursor() as cur:
        nodes = resolve_node_segment(conn)
        cur.execute("SELECT reltuples FROM pg_class WHERE oid = to_regclass(%s)",
                    (ATTRIBUTE_EFFECTIVE_TABLE,))
        if float(cur.fetchone()[0]) < 0:
            assert graph_read.small_scope(cur, kind="nodes", segment=nodes,
                                          predicate=doc) is None
    _analyze(world)
    with psycopg.connect(world.dsn, autocommit=True) as conn, conn.cursor() as cur:
        nodes = resolve_node_segment(conn)
        edges = resolve_edge_segment(conn)
        cur.execute("SELECT reltuples FROM pg_class WHERE oid = to_regclass(%s)",
                    (ATTRIBUTE_EFFECTIVE_TABLE,))
        overlay_rows = float(cur.fetchone()[0])
        scope_rows = graph_read.subtype_row_estimate(cur, nodes, "doc")
        assert overlay_rows > 0 and scope_rows and scope_rows > 0
        # Priced at the measured rates the small overlay table of this
        # fixture never pays for merging a scope.
        assert graph_read.small_scope(cur, kind="nodes", segment=nodes,
                                      predicate=doc) is None
        ratio = graph_read.MERGED_US_PER_SCOPE_ROW * scope_rows / overlay_rows
        for factor, chosen in ((1.001, True), (0.999, False)):
            graph_read.OVERLAY_SCAN_US_PER_ROW, saved = (
                ratio * factor, graph_read.OVERLAY_SCAN_US_PER_ROW)
            try:
                got = graph_read.small_scope(cur, kind="nodes", segment=nodes,
                                             predicate=doc)
                edge_got = graph_read.small_scope(
                    cur, kind="edges", segment=edges, predicate=_scoped(None, "edges"))
                unpinned = graph_read.small_scope(
                    cur, kind="nodes", segment=nodes,
                    predicate={"op": "eq", "field": "attrs.owner", "value": "alice"})
            finally:
                graph_read.OVERLAY_SCAN_US_PER_ROW = saved
            assert (got is not None) is chosen, factor
            if chosen:
                assert got == {"scope_rows": scope_rows, "overlay_rows": overlay_rows}
            assert unpinned is None
            assert edge_got is None or edge_got["overlay_rows"] == overlay_rows


def test_non_default_order_is_equal(world: _World, monkeypatch) -> None:
    for kind, order in (("nodes", "attrs.rank desc"), ("edges", "src")):
        for gen in world.gens:
            _both(world, monkeypatch, gen, kind, _scoped(None, kind), order_by=order)


# --------------------------------------------------------------------------
# Issue #3666 page arguments on the graph path (lane SG dev merge, 10-19)
# --------------------------------------------------------------------------


def _member_sql(ids: list[str]):
    """A ``member_sql`` set as an expand passes it: binds its own parameter
    and returns a one-column query of node ids."""
    def members(params: dict[str, Any]) -> str:
        params["ms_ids"] = list(ids)
        return "SELECT unnest(%(ms_ids)s::text[])"
    return members


@pytest.mark.parametrize("merged", [False, True], ids=["ids-first", "scope-merged"])
def test_a_member_sql_set_and_a_keyset_page_read_the_same_rows_and_count(
    world: _World, mcp, monkeypatch, merged: bool,
) -> None:
    """``member_sql`` (the far records of an expand) keeps the page and its
    bounded count to the set; ``after_id`` (a keyset page) to the rows after
    it in the default id order. Every predicate form, both kinds where the
    argument applies, every generation, both graph forms: the graph rows and
    count equal the recompute's. Before the graph id stream
    carried them, the graph page ignored ``member_sql`` (rows outside the
    set) and its split count ignored ``after_id`` (rows before the cursor
    counted)."""
    _analyze(world)
    _set_rates(monkeypatch, merged=merged)
    members = [world.ids["a"], world.ids["c"], world.ids["d"], world.ids["f"]]
    for kind, predicate in _all_cases():
        id_field = "node_id" if kind == "nodes" else "edge_id"
        scoped = _scoped(predicate, kind)
        for gen in world.gens:
            expected = [r["id"] for r in _recompute(world, gen, kind, scoped)]
            reads: list[tuple[str, dict[str, Any], list[str]]] = []
            if expected:
                reads.append(("after_id", {"after_id": expected[0]}, expected[1:]))
            if kind == "nodes":
                reads.append(("member_sql", {"member_sql": _member_sql(members)},
                              [i for i in expected if i in members]))
            for name, extra, want in reads:
                rows, meta, counted = _read(world, mcp, monkeypatch, "graph", gen, kind,
                                            scoped, limit=500, **extra)
                case = (name, kind, predicate, gen)
                assert [str(r[id_field]) for r in rows] == want, case
                assert counted == (len(want), "exact"), case
                assert meta["graph_form"]["form"] == (
                    "scope_merged" if merged else "ids_first"), case


def test_an_undeclared_order_sorts_as_text_and_says_so(
    world: _World, mcp, monkeypatch,
) -> None:
    """Issue #3666 (2c): with the access rules off, an order on an attribute
    with no declared type sorts as text and the page says so
    (``sorted_as_text``). The page holds the recompute's rows, its stored
    values in that order; before S2's fix, the graph page refused the order
    as not declared."""
    for kind, order in (("nodes", "attrs.owner"), ("nodes", "attrs.tag desc"),
                        ("edges", "attrs.label")):
        id_field = "node_id" if kind == "nodes" else "edge_id"
        key, descending = order.split()[0][6:], order.endswith(" desc")
        for gen in world.gens:
            rows, meta, _counted = _read(world, mcp, monkeypatch, "graph", gen, kind,
                                         _scoped(None, kind), limit=500, order_by=order)
            assert meta.get("sorted_as_text") == order.split()[0], (kind, order, gen)
            expected = _recompute(world, gen, kind, _scoped(None, kind))
            assert sorted(str(r[id_field]) for r in rows) == sorted(
                r["id"] for r in expected), (kind, order, gen)
            values = [str(r["attrs"][key]) for r in rows
                      if r["attrs"].get(key) is not None]
            assert values == sorted(values, reverse=descending), (kind, order, gen)


def test_a_two_phase_page_records_where_a_keyset_token_resumes(
    world: _World, mcp, monkeypatch,
) -> None:
    """The two-phase read (design 2.4) records the page's last row and the
    rows it consumed (#3666 step 3b) as the one-statement page does, and a
    keyset page after it reads only the rows after the cursor. Without them
    a filter past the offset cap offered no token after a two-phase page."""
    _set_rates(monkeypatch, merged=False)
    predicate = _scoped({"op": "eq", "field": "attrs.owner", "value": "alice"}, "nodes")
    for gen in world.gens:
        expected = [r["id"] for r in _recompute(world, gen, "nodes", predicate)]
        assert len(expected) >= 2, gen
        _rows, meta, _counted = _read(world, mcp, monkeypatch, "graph", gen, "nodes",
                                      predicate, limit=1)
        assert "two_phase" in meta, gen
        assert (meta["last_id"], meta["consumed"]) == (expected[0], 1), gen
        rows, meta, counted = _read(world, mcp, monkeypatch, "graph", gen, "nodes",
                                    predicate, limit=500, after_id=expected[0])
        assert "two_phase" in meta, gen
        assert [r["node_id"] for r in rows] == expected[1:], gen
        assert counted == (len(expected) - 1, "exact"), gen


# --------------------------------------------------------------------------
# Tool level: filter, map, lookup and references
# --------------------------------------------------------------------------


def test_filter_and_map_through_the_tools_are_served_and_equal_the_recompute(
    world: _World, monkeypatch,
) -> None:
    owner = {"op": "eq", "field": "attrs.owner", "value": "alice"}
    mcp = server_mod.build_server(dsn=world.dsn, deployment_name="default")
    try:
        tools = mcp._tool_manager._tools
        filtered = tools["filter"].fn(
            input={"kind": "nodes", "subtype": "doc"}, predicate=owner,
            generation_id=world.gens[2], limit=2,
        )
        mapped = tools["map"].fn(
            input={"kind": "edges", "edge_type": "references"},
            projection=["edge_id", "attrs.weight"],
            generation_id=world.gens[1], limit=50,
        )
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()
    owners = [r["id"] for r in _recompute(world, world.gens[2], "nodes",
                                          _scoped(owner, "nodes"))]
    assert [i["node_id"] for i in filtered["items"]] == owners[:2]
    assert filtered.get("total_count") == len(owners)
    edges = _recompute(world, world.gens[1], "edges", _scoped(None, "edges"))
    assert [str(i["edge_id"]) for i in mapped["items"]] == [r["id"] for r in edges]
    assert [_projected(i, "weight") for i in mapped["items"]] == [
        r["attrs"].get("weight") for r in edges], mapped["items"]


def _projected(item: dict, key: str) -> Any:
    """A map item's projected ``attrs.<key>``, whatever name the item gives
    it, with an untrusted-data fence taken off a text value."""
    value = None
    for name in (f"attrs.{key}", f"attrs_{key}"):
        if name in item:
            value = item[name]
            break
    else:
        value = (item.get("attrs") or {}).get(key)
    if isinstance(value, str):
        lines = value.splitlines()
        if len(lines) >= 3 and lines[0].startswith("⟦UNTRUSTED_DATA"):
            value = "\n".join(lines[1:-1])
    return value


# --------------------------------------------------------------------------
# Production settings, plan cache, Nomos rows
# --------------------------------------------------------------------------

_SETTING = re.compile(
    r"\b(SET\s+(LOCAL\s+)?|set_config\s*\(\s*')\s*"
    r"(enable_\w+|plan_cache_mode|max_parallel\w*|parallel_\w+|jit\w*|"
    r"random_page_cost|seq_page_cost|cpu_\w+|work_mem|join_collapse_limit|"
    r"from_collapse_limit|geqo\w*)",
    re.IGNORECASE,
)


def test_the_graph_path_sets_no_planner_parallel_or_plan_cache_option(
    world: _World, monkeypatch,
) -> None:
    statements: list[tuple[str, Any]] = []
    real = psycopg.Cursor.execute

    def capture(self, query, params=None, **kw):
        statements.append((str(query), kw.get("prepare")))
        return real(self, query, params, **kw)

    monkeypatch.setattr(psycopg.Cursor, "execute", capture)
    for predicate in (None, {"op": "eq", "field": "attrs.owner", "value": "alice"},
                      {"op": "eq", "field": "attrs", "value": {"rank": 3}}):
        _page(world, monkeypatch, "graph", world.gens[2], "nodes",
              _scoped(predicate, "nodes"))
    monkeypatch.setattr(psycopg.Cursor, "execute", real)
    pages = [(q, p) for q, p in statements if "graph_rows" in q]
    assert pages, "no graph-path page statement was captured"
    assert all(p is False for _q, p in pages), "a graph page was sent preparable"
    offending = [q for q, _p in statements if _SETTING.search(q)]
    assert offending == []
    # The detector itself catches a planted setting.
    assert _SETTING.search("SET LOCAL enable_seqscan = off")
    assert _SETTING.search("SELECT set_config('plan_cache_mode', 'force_custom_plan', true)")


def test_ten_runs_leave_no_prepared_statement(world: _World, monkeypatch) -> None:
    mcp = server_mod.build_server(dsn=world.dsn, deployment_name="default")
    try:
        with psycopg.connect(world.dsn, autocommit=True) as conn:
            session = open_session(conn, deployment_name="default",
                                   requested_generation=world.gens[2])
            predicate = _scoped({"op": "eq", "field": "attrs.owner", "value": "alice"},
                                "nodes")
            for _ in range(10):
                meta: dict[str, Any] = {}
                mcp._okg_effective_view_sql_page(
                    conn, session, kind="nodes", predicate=predicate,
                    limit=2, offset=0, page_meta=meta,
                )
                mcp._okg_bounded_operator_count(
                    conn, from_where_sql=meta["from_where_sql"], params=meta["params"],
                )
            with conn.cursor() as cur:
                cur.execute("SELECT statement FROM pg_prepared_statements")
                prepared = [r[0] for r in cur.fetchall()]
            # The failing case: the same page through the driver's default
            # prepare threshold leaves one.
            with conn.cursor() as cur:
                for _ in range(6):
                    cur.execute(meta_sql := "SELECT 1 " + meta["from_where_sql"]
                                + " LIMIT 1", meta["params"])
                cur.execute("SELECT count(*) FROM pg_prepared_statements")
                with_default = int(cur.fetchone()[0])
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()
    assert [s for s in prepared if "graph_rows" in s] == []
    assert with_default >= 1, meta_sql


def _nomos_rows(world: _World, gen: str, kind: str) -> list[dict]:
    """The row dicts the page hands the Nomos read filter (S2 design 1.3 and
    1.4), built from the graph rows and that generation's overlay rows: the
    id, ``attrs`` (the graph attrs when the target has an overlay row, else
    the effective attrs), ``_effective_attrs``, and the subtype or the
    link's ends and type. The node's ``text_hash`` is left out here."""
    id_field = "node_id" if kind == "nodes" else "edge_id"
    expected = _recompute(world, gen, kind, _scoped(None, kind))
    stored = {str(i): dict(a) for i, a in world.query(
        gen, f"SELECT {id_field}, attrs FROM okg.graph_{kind}")}
    with psycopg.connect(world.dsn, autocommit=True) as conn:
        overlaid = {str(t) for (t, key) in conn.execute(
            f"SELECT target_id, attr_key FROM {ATTRIBUTE_EFFECTIVE_TABLE} "
            "WHERE generation_id = %s AND target_kind = %s AND namespace = 'default'",
            (gen, "node" if kind == "nodes" else "edge"),
        ).fetchall() if key not in RESERVED_EFFECTIVE_ATTR_KEYS}
    rows = []
    for r in expected:
        row = {id_field: r["id"],
               "attrs": stored[r["id"]] if r["id"] in overlaid else r["attrs"],
               "_effective_attrs": r["attrs"]}
        if kind == "nodes":
            row["subtype"] = r["subtype"]
        else:
            row.update(src=r["src"], dst=r["dst"], edge_type=r["edge_type"])
        rows.append(row)
    return json.loads(json.dumps(rows, default=str))


def test_nomos_sees_the_s2_row_dicts(world: _World, monkeypatch) -> None:
    """Requirement ``nomos-unchanged``: the rows handed to the Nomos read
    filter are the row dicts S2 defined (``_nomos_rows``), with Nomos off and
    with runtime evaluation forced on. The first rows handed for each kind
    are the whole page here (one batch)."""
    real_nodes = server_mod.filter_node_rows_for_session
    real_edges = server_mod.filter_edge_rows_for_session
    for runtime in (False, True):
        handed: dict[str, list] = {}
        for mode in ("graph",):
            calls: list = []

            def nodes(conn, session, rows, **kw):
                rows = list(rows)
                calls.append(("nodes", json.loads(json.dumps(rows, default=str))))
                return real_nodes(conn, session, rows, **kw)

            def edges(conn, session, rows, **kw):
                rows = list(rows)
                calls.append(("edges", json.loads(json.dumps(rows, default=str))))
                return real_edges(conn, session, rows, **kw)

            monkeypatch.setattr(server_mod, "filter_node_rows_for_session", nodes)
            monkeypatch.setattr(server_mod, "filter_edge_rows_for_session", edges)
            for kind in ("nodes", "edges"):
                _page(world, monkeypatch, mode, world.gens[1], kind,
                      _scoped(None, kind), nomos_runtime=runtime)
            handed[mode] = calls
        monkeypatch.setattr(server_mod, "filter_node_rows_for_session", real_nodes)
        monkeypatch.setattr(server_mod, "filter_edge_rows_for_session", real_edges)
        assert handed["graph"], runtime
        for kind in ("nodes", "edges"):
            expected = _nomos_rows(world, world.gens[1], kind)
            by_id = {row["node_id" if kind == "nodes" else "edge_id"]: row
                     for row in expected}
            batches = []
            for k, rows in handed["graph"]:
                if k != kind:
                    continue
                if kind == "nodes":
                    assert all("text_hash" in row for row in rows), (runtime, rows)
                    rows = [{f: v for f, v in row.items() if f != "text_hash"}
                            for row in rows]
                batches.append(rows)
            assert batches, (runtime, kind)
            if not runtime:
                # Nomos off: one batch, the whole page.
                assert batches == [expected], (runtime, kind)
            # Every row handed, every batch, is S2's row dict of its id (with
            # runtime evaluation forced on, the batch loop also hands the
            # final page's rows again).
            for rows in batches:
                for row in rows:
                    key = row["node_id" if kind == "nodes" else "edge_id"]
                    assert row == by_id[key], (runtime, kind, row)


class _StatsCursor:
    """Answers the three catalog reads of :func:`graph_read.broad_scope` in
    order: the segment's reltuples, its column statistics, its relpages."""

    def __init__(self, reltuples: float, stats: Any, relpages: int) -> None:
        self._answers = [("okg", "seg", reltuples), stats, (relpages,)]

    def execute(self, sql: str, params: Any = None) -> None:
        self._row = self._answers.pop(0)

    def fetchone(self) -> Any:
        return self._row


def test_broad_scope_needs_column_statistics() -> None:
    """reltuples is set by VACUUM or CREATE INDEX without ANALYZE: with no
    column statistics there is no estimate, so the count keeps the planner's
    own choice (no scan form forced)."""
    predicate = {"op": "and", "clauses": [
        {"op": "eq", "field": "subtype", "value": "doc"},
        {"op": "eq", "field": "attrs.owner", "value": "alice"},
    ]}
    assert graph_read.broad_scope(
        _StatsCursor(1000.0, None, 10), kind="nodes", segment="seg", predicate=predicate,
    ) is None
    analyzed = graph_read.broad_scope(
        _StatsCursor(1000.0, (["doc"], [0.5], 2), 10),
        kind="nodes", segment="seg", predicate=predicate,
    )
    assert analyzed == {"scope_rows": 500.0, "segment_pages": 10}


# --------------------------------------------------------------------------
# A nested `and` plans flat on the graph path only (lane SG r1019 fix)
# --------------------------------------------------------------------------


def test_the_planning_predicate_splices_plain_nested_ands_only() -> None:
    """``graph_read.planning_predicate`` splices a plain nested ``and`` (no
    key but ``op`` and a non-empty ``clauses``) into a top-level ``and``,
    recursively, and leaves everything else as it is; it never changes its
    argument."""
    s = {"op": "eq", "field": "subtype", "value": "doc"}
    a = {"op": "eq", "field": "attrs.path", "value": "a.py"}
    b = {"op": "gt", "field": "attrs.rank", "value": 0}
    c = {"op": "eq", "field": "attrs.owner", "value": "alice"}
    nested = {"op": "and", "clauses": [s, {"op": "and", "clauses": [a, b]}]}
    before = json.loads(json.dumps(nested))
    assert graph_read.planning_predicate(nested) == {"op": "and", "clauses": [s, a, b]}
    assert nested == before
    deep = {"op": "and", "clauses": [{"op": "and", "clauses": [
        a, {"op": "and", "clauses": [b, c]}]}]}
    assert graph_read.planning_predicate(deep) == {"op": "and", "clauses": [a, b, c]}
    # Kept as they are: an `and` with another key, an empty `and`, an
    # `and` under `or` or `not`, a top level that is not an `and`.
    for kept in (
        {"op": "and", "clauses": [s, {"op": "and", "clauses": [a, b], "note": 1}]},
        {"op": "and", "clauses": [s, {"op": "and", "clauses": []}]},
        {"op": "and", "clauses": [s, {"op": "or", "clauses": [
            a, {"op": "and", "clauses": [b, c]}]}]},
        {"op": "and", "clauses": [s, {"op": "not", "clause": {
            "op": "and", "clauses": [a, b]}}]},
        {"op": "and", "clauses": [s, a]},
        {"op": "or", "clauses": [{"op": "and", "clauses": [a, b]}, c]},
        a, None,
    ):
        assert graph_read.planning_predicate(kept) is kept, kept


def _nested_and() -> tuple[Any, Any]:
    """and(subtype, and(path eq, rank gt)) as the filter tool composes it,
    and the same conjuncts flat; both keys declared."""
    path = {"op": "eq", "field": "attrs.path", "value": "a.py"}
    rank = {"op": "gt", "field": "attrs.rank", "value": 0}
    nested = _scoped({"op": "and", "clauses": [path, rank]}, "nodes")
    flat = {"op": "and", "clauses": [nested["clauses"][0], path, rank]}
    return nested, flat


#: The declared expression-index candidate set's own parameter
#: (attr_filter_source.plan_expression_candidates, prefix ``dai``).
_DECLARED_CANDIDATES = "%(dai_bp)s"


def test_a_nested_and_narrows_the_graph_page_by_its_declared_keys(
    world: _World, mcp, monkeypatch,
) -> None:
    """Gate shape h_and_2keys: the filter tool sends and(subtype, and(a, b))
    with both keys declared. The graph path plans it flat, so the declared
    candidate set narrows its page and count (Krama head10: before, the page
    walked every visible row, 3.8-4.6 s). Rows and count equal the
    recompute and the flat form's, and the caller's predicate is not
    changed. Without the fix the graph page has no candidate set."""
    report = dai.reconcile(world.dsn, deployment="default", apply=True, enable=True)
    assert report["status"] == "converged", report
    monkeypatch.delenv("OKG_ATTR_FILTER_INDEX", raising=False)
    _analyze(world)
    _set_rates(monkeypatch, merged=False)
    gen = world.gens[-1]
    nested, flat = _nested_and()
    before = json.loads(json.dumps(nested))
    expected = [r["id"] for r in _recompute(world, gen, "nodes", nested)]
    assert expected, "the fixture must match at least one row"
    for limit in (1, 500):
        rows, meta, counted = _read(world, mcp, monkeypatch, "graph", gen, "nodes",
                                    nested, limit=limit)
        assert meta["graph_form"]["candidates"] is True, meta["graph_form"]
        assert _DECLARED_CANDIDATES in meta["from_where_sql"]
        assert _DECLARED_CANDIDATES in meta["count_from_where_sql"]
        assert [r["node_id"] for r in rows] == expected[:limit]
        assert counted == (len(expected), "exact")
        flat_rows, flat_meta, flat_counted = _read(world, mcp, monkeypatch, "graph", gen,
                                                   "nodes", flat, limit=limit)
        assert flat_meta["graph_form"]["candidates"] is True
        assert (flat_rows, flat_counted) == (rows, counted)
    assert nested == before


def _bounded_count_statements_of_one_tool_call(
    world: _World, monkeypatch,
) -> tuple[list[tuple[str, Any]], set[Any], int]:
    """Run ONE ``filter`` tool call whose first page is over ``max_bytes`` (so
    the tool body runs again at a smaller limit) and return its bounded-count
    statements as (text, prepare argument), the distinct page limits the body
    ran with, and the number of items served."""
    import anyio
    from mcp.shared.memory import create_connected_server_and_client_session

    from tests.substrate.mcp._result_payload import JSON_META

    with psycopg.connect(world.dsn, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [NodeFact(f"doc:bulk-{index:02d}:{uuid.uuid4().hex[:6]}", "doc",
                      {"path": f"bulk{index}.py", "owner": "bulk", "rank": 100 + index,
                       "pad": "x" * 600},
                      {"id": f"bulk-{index}"}, {"v": 1})
             for index in range(40)],
            source="default", strict_endpoints=True,
        )
    gen = world.publish()
    statements: list[tuple[str, Any, Any]] = []
    real = psycopg.Cursor.execute

    def capture(self, query, params=None, **kw):
        statements.append((str(query), params, kw.get("prepare", "not passed")))
        return real(self, query, params, **kw)

    mcp = server_mod.build_server(dsn=world.dsn, deployment_name="default")
    replies: dict[str, Any] = {}

    async def drive() -> None:
        async with create_connected_server_and_client_session(mcp) as client:
            replies["page"] = await client.call_tool(
                "filter",
                {"input": {"kind": "nodes", "subtype": "doc"},
                 "predicate": {"op": "eq", "field": "attrs.owner", "value": "bulk"},
                 "generation_id": gen, "limit": 30, "max_bytes": 4096},
                meta=JSON_META,
            )

    monkeypatch.setattr(psycopg.Cursor, "execute", capture)
    try:
        anyio.run(drive)
    finally:
        monkeypatch.setattr(psycopg.Cursor, "execute", real)
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()
    body = json.loads(replies["page"].content[0].text)
    assert not replies["page"].isError and body.get("items"), json.dumps(body)[:600]
    assert body.get("total_count") == 40 or body.get("total_count_basis"), body
    counts = [(q, p) for q, _params, p in statements if "bounded_operator_count" in q]
    pages = [q_params for q, q_params, _p in statements
             if "bounded_operator_count" not in q
             and ("graph_rows" in q or "effective_page" in q)
             and isinstance(q_params, dict) and "page_limit" in q_params]
    limits = {params["page_limit"] for params in pages}
    return counts, limits, len(body["items"])


def test_a_tool_call_counts_once_unprepared_across_its_byte_budget_reruns(
    world: _World, monkeypatch,
) -> None:
    """Through a real ``filter`` call: a call whose first page is over
    ``max_bytes`` runs the tool body again at a smaller limit; its bounded
    count runs ONCE and is sent unprepared (``_bounded_operator_count``
    keeps the outcome for the call's reruns, and every graph-path statement
    is sent unprepared, S2 design 2.8). Calling
    ``_okg_bounded_operator_count`` directly, as the other tests here do,
    cannot see the reruns."""
    counts, limits, served = _bounded_count_statements_of_one_tool_call(world, monkeypatch)
    assert len(limits) >= 2, ("the call was not run again to fit max_bytes", limits)
    assert served < 30
    assert [p for _q, p in counts] == [False], counts
