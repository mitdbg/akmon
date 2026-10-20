"""Operator parity on real publishes (#3201).

One published fixture with an accepted overlay that makes one node's
effective `status` differ from its stored one. Against it:

- old-versus-new: the pre-change run_sql helpers (frozen in
  operator_compiler_legacy.py) and the rerouted tools return the same rows,
  order and counts for every predicate that reads no attribute; the old
  effective-view renderer and the compiler select the same rows for every
  leaf. INTENDED DIFFERENCES are asserted as differences, by name, below.
- hostile operands return exactly the Python evaluator's rows, and a NUL
  operand never reaches the database;
- one path: an ordered filter, a set aggregate and a filtered one-hop expand
  compare effective values; with the snapshot gone each is refused by name
  and run_sql is never called;
- counts: exact only from a complete page or a bounded COUNT with nothing
  dropped; a dropped row gives lower_bound/unknown; a timed-out COUNT is
  unknown, never zero.
"""
from __future__ import annotations

import json
import os
import re
import statistics
import time
import uuid
from typing import Any, Iterator

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.attributes import (  # noqa: E402
    materialize_effective_attributes_for_generation,
    record_attribute_observation,
)
from okg.substrate.errors import SubstrateError  # noqa: E402
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import EdgeFact, NodeFact  # noqa: E402
from okg.substrate.mcp import open_session  # noqa: E402
from okg.substrate.mcp import server as server_mod  # noqa: E402
from okg.substrate.mcp.sql_safety import RunSqlInvalid  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from tests.pytest.catalog_fixture_builders import (  # noqa: E402
    bind_test_catalog_writer,
    seed_catalog,
)
from tests.substrate.mcp import operator_compiler_legacy as legacy  # noqa: E402

pytestmark = pytest.mark.postgres

DECLARED = {"due": "ts", "size": "num", "title": "text"}
HOSTILE = {
    "a": "O'Brien",
    "b": "back\\slash\\",
    "c": "100% sure_",
    "d": "%(pv0)s",
    "e": "日本語 ünï",
    "f": "%s %%",
}


@pytest.fixture
def world(clean_runtime: str) -> Iterator[dict[str, Any]]:
    sfx = uuid.uuid4().hex[:8]
    ids = {key: f"n:{sfx}:{key}" for key in "abcdefg"}
    hub = f"n:{sfx}:hub"
    attrs = {
        "a": {"status": "open", "size": 3, "title": "alpha", "path": "lib/a.py",
              "due": "2026-01-01T00:00:00Z"},
        "b": {"status": "closed", "size": 7, "title": "beta", "path": "lib/b.py",
              "due": "2026-01-01T00:00:00+00:00"},
        "c": {"status": "open", "size": 12.5, "title": "gamma", "path": "notes/c.md",
              "due": "not a date"},
        # The overlay below makes d's EFFECTIVE status "open".
        "d": {"status": "closed", "title": "delta", "path": "lib/d.py"},
        "e": {"status": "stale", "size": 1, "title": "eta", "path": "lib/e.py", "due": 5},
        "f": {"status": None, "title": "zeta"},
        "g": {"title": "theta"},
    }
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["item", "hub"],
            narrowings=[("hub", "links", "item"), ("item", "links", "item")],
        )
        facts: list[Any] = [
            NodeFact(hub, "hub", {"text": "hub", "status": "hub"}, {"id": hub}, {"v": 1}),
        ]
        for key, node in ids.items():
            body = {"text": f"item {key}", **attrs[key]}
            if key in HOSTILE:
                body["note"] = HOSTILE[key]
            facts.append(NodeFact(node, "item", body, {"id": node}, {"v": 1}))
        for i, (key, node) in enumerate(ids.items()):
            facts.append(EdgeFact(
                src=hub, dst=node, edge_type="links", attrs={"weight": i},
                source_record_id={"src": hub, "dst": node}, source_revision={"v": 1},
            ))
        facts.append(EdgeFact(
            src=ids["a"], dst=ids["b"], edge_type="links", attrs={"weight": 99},
            source_record_id={"src": ids["a"], "dst": ids["b"]}, source_revision={"v": 1},
        ))
        ChronosGraphStore.from_connection(conn).apply_facts(facts, source="default")
    identity = bind_test_catalog_writer(clean_runtime, "default")
    out = Publisher(clean_runtime, "default", catalog_identity=identity).cycle()
    assert out["status"] == "published", out
    gen = str(out["generation_id"])
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        record_attribute_observation(
            conn, target_kind="node", target_id=ids["d"], generation_id=gen,
            attr_key="status", value="open", producer="test:overlay",
        )
        materialize_effective_attributes_for_generation(conn, generation_id=gen)
    mcp = server_mod.build_server(dsn=clean_runtime)
    try:
        yield {
            "dsn": clean_runtime,
            "gen": gen,
            "ids": ids,
            "hub": hub,
            "mcp": mcp,
            "tools": {name: tool.fn for name, tool in mcp._tool_manager._tools.items()},
        }
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


def _old(world: dict, fn, **kwargs) -> dict:
    with psycopg.connect(world["dsn"], autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        return fn(conn, session, **kwargs)


def _item_ids(page: dict, key: str = "node_id") -> list[str]:
    return [row[key] for row in page["items"]]


# ---------------------------------------------------------------- old vs new

STABLE_FILTERS = [
    None,
    {"op": "exists", "field": "node_id"},
    {"op": "not", "clause": {"op": "eq", "field": "subtype", "value": "hub"}},
]


@pytest.mark.parametrize("order_by", ["node_id", "node_id desc", "subtype", "text_hash"])
@pytest.mark.parametrize("predicate", STABLE_FILTERS, ids=["none", "exists", "not"])
def test_ordered_filter_old_and_new_agree(world: dict, order_by: str, predicate: Any) -> None:
    """Path B (run_sql) versus the effective path: same rows, same order,
    same count, at every page size."""
    scope = {"kind": "nodes", "subtype": "item"}
    for limit in (2, 3, 100):
        old = _old(world, legacy.filter_ordered, input=scope, predicate=predicate,
                   order_by=order_by, limit=limit, offset=0, select=["node_id"])
        new = world["tools"]["filter"](input=scope, predicate=predicate, order_by=order_by,
                                       limit=limit, select=["node_id"])
        assert _item_ids(new) == _item_ids(old), (limit, old, new)
        assert new["total_count"] == old["total_count"] == 7
        assert new["total_count_basis"] == old["total_count_basis"] == "exact"
        assert new["truncated"] is old["truncated"]
        assert new["next_offset"] == old["next_offset"]
    edges = {"kind": "edges", "edge_type": "links"}
    old = _old(world, legacy.filter_ordered, input=edges, predicate=None, order_by="edge_id",
               limit=3, offset=3, select=["edge_id"])
    new = world["tools"]["filter"](input=edges, predicate=None, order_by="edge_id",
                                   limit=3, offset=3, select=["edge_id"])
    assert _item_ids(new, "edge_id") == _item_ids(old, "edge_id")
    assert new["total_count"] == old["total_count"] == 8


@pytest.mark.parametrize("kind,by", [
    ("nodes", ["subtype"]),
    ("edges", ["edge_type"]),
    ("edges", ["src"]),
    ("edges", ["src", "edge_type"]),
])
def test_set_aggregate_old_and_new_agree(world: dict, kind: str, by: list[str]) -> None:
    scope = {"kind": kind}
    old = _old(world, legacy.aggregate_set, input=scope, by=by, metrics=["count"], limit=50)
    new = world["tools"]["aggregate"](input=scope, by=by, metrics=["count"], limit=50)

    def groups(page: dict) -> list[tuple]:
        return sorted((tuple(g[f] for f in by), g["count"]) for g in page["groups"])

    assert groups(new) == groups(old)
    assert new["total_count"] == old["total_count"] == len(new["groups"])
    assert new["total_count_basis"] == old["total_count_basis"] == "exact"
    # Both order by count desc; tied groups are ordered by key either way.
    assert [g["count"] for g in new["groups"]] == [g["count"] for g in old["groups"]]


_AGGREGATE_SQL = re.compile(r"count\(\*\) AS n FROM .*effective_aggregate", re.S)
_NARROW_BASE = re.compile(r"FROM (okg\.attribute_effective_view(?:_intervals)?) WHERE")


@pytest.fixture
def executed_sql(monkeypatch) -> list[tuple[str, Any]]:
    """Every (statement, params) any cursor runs during the test."""
    seen: list[tuple[str, Any]] = []
    real = psycopg.Cursor.execute

    def spy(self, query, params=None, *args, **kwargs):
        seen.append((str(query), params))
        return real(self, query, params, *args, **kwargs)

    monkeypatch.setattr(psycopg.Cursor, "execute", spy)
    return seen


@pytest.mark.parametrize("interval_view", ["1", "0"])
@pytest.mark.parametrize("scope,by", [
    ({"kind": "nodes"}, ["subtype"]),
    ({"kind": "nodes", "subtype": "item",
      "filter": {"op": "not", "clause": {"op": "eq", "field": "node_id", "value": "x"}}},
     ["subtype"]),
    ({"kind": "edges"}, ["edge_type"]),
    ({"kind": "edges", "edge_type": "links"}, ["src", "edge_type"]),
])
def test_an_aggregate_reading_no_attrs_skips_the_payload_join(
    world: dict, executed_sql: list, monkeypatch, interval_view: str, scope: dict, by: list,
) -> None:
    """#3235 speed fix: with no attrs in `by` or the filter, the aggregate
    reads the pinned base table alone. The same statement with the payload
    join put back counts the same groups, and the legacy path agrees."""
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", interval_view)
    new = world["tools"]["aggregate"](input=scope, by=by, metrics=["count"], limit=50)
    statements = [(q, p) for q, p in executed_sql if _AGGREGATE_SQL.search(q)]
    assert len(statements) == 1, [q for q, _ in statements]
    narrow_sql, params = statements[0]
    assert "attribute_versions" not in narrow_sql
    assert "attribute_effective_attrs" not in narrow_sql
    joined_sql, replaced = _NARROW_BASE.subn(
        r"FROM \1 JOIN okg.attribute_versions "
        r"USING (target_kind, target_id, attrs_hash) WHERE",
        narrow_sql,
    )
    assert replaced == 1, narrow_sql
    with psycopg.connect(world["dsn"], autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(narrow_sql, params)
        narrow_rows = sorted(cur.fetchall(), key=repr)
        cur.execute(joined_sql, params)
        joined_rows = sorted(cur.fetchall(), key=repr)
    assert narrow_rows, "the aggregate counted nothing; the comparison proves nothing"
    assert narrow_rows == joined_rows
    old = _old(world, legacy.aggregate_set, input=scope, by=by, metrics=["count"], limit=50)
    assert sorted((tuple(g[f] for f in by), g["count"]) for g in new["groups"]) == sorted(
        (tuple(g[f] for f in by), g["count"]) for g in old["groups"]
    )


@pytest.mark.parametrize("scope,by", [
    ({"kind": "nodes"}, ["attrs.status"]),
    ({"kind": "nodes", "filter": {"op": "eq", "field": "attrs.status", "value": "open"}},
     ["subtype"]),
])
def test_an_aggregate_reading_attrs_keeps_the_payload_join_and_overlay(
    world: dict, executed_sql: list, scope: dict, by: list,
) -> None:
    """The failure case for the narrowing: an attrs aggregate must still read
    the payload and the live overlay, or it would count stored, not
    effective, values (d's effective status is "open")."""
    out = world["tools"]["aggregate"](input=scope, by=by, metrics=["count"], limit=50)
    statements = [q for q, _ in executed_sql if _AGGREGATE_SQL.search(q)]
    assert len(statements) == 1, statements
    assert "JOIN okg.attribute_versions" in statements[0]
    assert "okg.attribute_effective_attrs" in statements[0]
    if by == ["attrs.status"]:
        counts = {g["attrs.status"]: g["count"] for g in out["groups"]}
        assert counts["open"] == 3
    else:
        assert [g["count"] for g in out["groups"]] == [3]


@pytest.mark.parametrize("dest_filter,order_by", [
    ({"op": "eq", "field": "subtype", "value": "item"}, None),
    ({"op": "in", "field": "node_id", "values": ["A", "C", "E"]}, "node_id"),
    (None, "node_id desc"),
    ({"op": "not", "clause": {"op": "eq", "field": "node_id", "value": "B"}}, None),
])
def test_filtered_expand_old_and_new_agree(world: dict, dest_filter: Any, order_by: Any) -> None:
    ids = world["ids"]
    named = json.loads(
        json.dumps(dest_filter)
        .replace('"A"', json.dumps(ids["a"]))
        .replace('"B"', json.dumps(ids["b"]))
        .replace('"C"', json.dumps(ids["c"]))
        .replace('"E"', json.dumps(ids["e"]))
    )
    for limit, offset in ((3, 0), (3, 3), (50, 0)):
        old = _old(world, legacy.expand_filtered, node_id=world["hub"], edge_type="links",
                   direction="out", dest_filter=named, order_by=order_by,
                   limit=limit, offset=offset, select=None)
        new = world["tools"]["expand"](input={"node_id": world["hub"]}, edge_type="links",
                                       direction="out", filter=named, order_by=order_by,
                                       limit=limit, offset=offset,
                                       # Issue #3666: only the link's own
                                       # names ask for link rows; this list
                                       # is the whole link row, as before.
                                       select=["edge_id", "src", "dst", "edge_type",
                                               "direction", "link.attrs", "read_provenance"])
        # The same call on record rows (the default): the SQL page over the
        # far records serves exactly the records the legacy links reach.
        records = world["tools"]["expand"](
            input={"node_id": world["hub"]}, edge_type="links", direction="out",
            filter=named, order_by=order_by, limit=limit, offset=offset)
        if offset == 0 and limit == 50:
            got = [r["node_id"] for r in records["neighbors"]]
            want_ids = [r["dst"] for r in old["neighbors"]]
            assert (got == want_ids) if order_by else (sorted(got) == sorted(want_ids))
            assert records["total_count"] == old["total_count"]
            assert all(r["link"]["edge_type"] == "links" and r["link_count"] == 1
                       for r in records["neighbors"])
        assert [r["edge_id"] for r in new["neighbors"]] == [r["edge_id"] for r in old["neighbors"]]
        # The new rows also name their neighbour (#2675, #3072); the legacy
        # reader predates that field, so it is compared without it.
        assert all("neighbor" in r for r in new["neighbors"])
        assert [frozenset(r) - {"neighbor"} for r in new["neighbors"]] == [
            frozenset(r) for r in old["neighbors"]
        ]
        assert new["total_count"] == old["total_count"]
        assert new["next_offset"] == old["next_offset"]


def _relation(world: dict, conn) -> tuple[str, dict[str, Any]]:
    mcp = world["mcp"]
    params: dict[str, Any] = {}
    base, source, gen_where, gen_params = mcp._okg_effective_view_source(
        conn, world["gen"], target_kind="node",
    )
    from_clause = mcp._okg_effective_view_runtime_source(
        base_table=base, source_clause=source, gen_where=gen_where, gen_params=gen_params,
        target_kind="node", generation_id=world["gen"], relation="rel", params=params,
    )
    return from_clause, params


def _select_ids(conn, from_clause: str, where: str, params: dict) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT rel.target_id FROM {from_clause} WHERE rel.target_kind = %(tk)s "
            f"AND rel.namespace = 'default' AND rel.subtype = 'item' AND ({where})",
            params,
        )
        return {row[0] for row in cur.fetchall()}


def _python_ids(world: dict, conn, from_clause: str, params: dict, predicate: Any) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT rel.target_id, rel.subtype, rel.raw_text_hash, rel.attrs FROM {from_clause} "
            "WHERE rel.target_kind = %(tk)s AND rel.namespace = 'default' AND rel.subtype = 'item'",
            params,
        )
        rows = cur.fetchall()
    matches = world["mcp"]._okg_operator_predicate_matches
    return {
        r[0] for r in rows
        if matches({"node_id": r[0], "subtype": r[1], "text_hash": r[2], "attrs": r[3] or {}},
                   predicate, declared=DECLARED)
    }


def _eq(field: str, value: Any) -> dict:
    return {"op": "eq", "field": field, "value": value}


TRUTH_CASES = [
    _eq("attrs.status", "open"),
    _eq("attrs.status", None),
    {"op": "not", "clause": _eq("attrs.status", None)},
    _eq("attrs.size", 3),
    {"op": "in", "field": "attrs.status", "values": ["open", "stale", None]},
    {"op": "not", "clause": {"op": "in", "field": "attrs.status", "values": ["open"]}},
    {"op": "exists", "field": "attrs.size"},
    {"op": "not", "clause": {"op": "exists", "field": "attrs.status"}},
    {"op": "contains", "field": "attrs", "value": {"status": "closed", "title": "beta"}},
    {"op": "prefix", "field": "attrs.path", "value": "lib/"},
    {"op": "not", "clause": {"op": "suffix", "field": "attrs.path", "value": ".py"}},
    {"op": "gt", "field": "attrs.size", "value": 3},
    {"op": "between", "field": "attrs.size", "values": [2, 12.5]},
    {"op": "not", "clause": {"op": "lt", "field": "attrs.size", "value": 7}},
    _eq("attrs.due", "2026-01-01T00:00:00Z"),
    {"op": "between", "field": "attrs.due", "values": ["2025-12-31", "2026-01-02"]},
    {"op": "or", "clauses": [_eq("attrs.status", "stale"), {"op": "gt", "field": "attrs.size", "value": 10}]},
    {"op": "and", "clauses": [_eq("subtype", "item"), {"op": "not", "clause": _eq("attrs.status", "closed")}]},
]
#: search's query-less lookup leaf (`search_where.RAW_TEXT_CONTAINS`). The
#: Python evaluator has no such op, so it is compared old against new only.
LOOKUP_TEXT_CASES = [
    {"op": "_raw_text_contains", "field": "attrs.path", "value": "lib/"},
    {"op": "_raw_text_contains", "field": "attrs.path", "value": ".py"},
    {"op": "_raw_text_contains", "field": "attrs.size", "value": "3"},
    {"op": "not", "clause": {"op": "_raw_text_contains", "field": "attrs.path", "value": "lib/"}},
]
#: INTENDED DIFFERENCE 1 (a fix): the old renderer's ts-declared equality was
#: NULL, not FALSE, on a row without the key, so NOT dropped rows the Python
#: evaluator keeps. The compiler is two-valued there.
OLD_TS_EQ_THREE_VALUED = {"op": "not", "clause": _eq("attrs.due", "2026-01-01T00:00:00Z")}


@pytest.mark.parametrize("predicate", TRUTH_CASES, ids=[json.dumps(c)[:70] for c in TRUTH_CASES])
def test_compiled_sql_selects_what_the_old_renderer_and_python_select(world: dict, predicate) -> None:
    with psycopg.connect(world["dsn"], autocommit=True) as conn:
        from_clause, params = _relation(world, conn)
        old_params = dict(params)
        old_sql = legacy.legacy_effective_predicate_sql(
            "nodes", predicate, old_params, declared=DECLARED, relation="rel")
        compiled = world["mcp"]._okg_compile_operator_predicate(
            "nodes", predicate, params, declared=DECLARED, relation="rel")
        assert old_sql is not None and compiled is not None
        new_params = dict(params)
        new_sql = compiled.bind_into(new_params)
        old = _select_ids(conn, from_clause, old_sql or "TRUE", old_params)
        new = _select_ids(conn, from_clause, new_sql, new_params)
        python = _python_ids(world, conn, from_clause, params, predicate)
    assert new == old == python, (predicate, old, new, python)


@pytest.mark.parametrize("predicate", LOOKUP_TEXT_CASES,
                         ids=[json.dumps(c)[:70] for c in LOOKUP_TEXT_CASES])
def test_lookup_text_leaf_selects_what_the_old_renderer_selected(world: dict, predicate) -> None:
    with psycopg.connect(world["dsn"], autocommit=True) as conn:
        from_clause, params = _relation(world, conn)
        old_params = dict(params)
        old_sql = legacy.legacy_effective_predicate_sql(
            "nodes", predicate, old_params, declared=DECLARED, relation="rel")
        compiled = world["mcp"]._okg_compile_operator_predicate(
            "nodes", predicate, params, declared=DECLARED, relation="rel")
        assert old_sql is not None and compiled is not None
        new_params = dict(params)
        new = _select_ids(conn, from_clause, compiled.bind_into(new_params), new_params)
        old = _select_ids(conn, from_clause, old_sql, old_params)
    assert new == old, (predicate, old, new)
    if predicate["op"] != "not" and predicate["field"] == "attrs.path":
        assert new, "the fixture holds a matching path"


#: `contains` with text on one attribute (#3666): the stored text holds it,
#: ASCII case ignored. The old renderer had no such leaf, so the compiled SQL
#: is compared with the Python evaluator, and each case names its rows. The
#: hostile operands (`%`, `_`, a backslash, a quote, a parameter marker)
#: are plain text: a pattern reading of any of them would select more rows.
TEXT_CONTAINS_CASES = [
    ({"op": "contains", "field": "attrs.path", "value": "LIB/"}, "abde"),
    ({"op": "contains", "field": "attrs.title", "value": "ETA"}, "befg"),
    ({"op": "not", "clause": {"op": "contains", "field": "attrs.path", "value": ".py"}}, "cfg"),
    ({"op": "contains", "field": "attrs.size", "value": "3"}, ""),
    ({"op": "contains", "field": "attrs.note", "value": "0% S"}, "c"),
    ({"op": "contains", "field": "attrs.note", "value": "_"}, "c"),
    ({"op": "contains", "field": "attrs.note", "value": "%"}, "cdf"),
    ({"op": "contains", "field": "attrs.note", "value": "K\\S"}, "b"),
    ({"op": "contains", "field": "attrs.note", "value": "o'brien"}, "a"),
    ({"op": "contains", "field": "attrs.note", "value": "%(PV0)S"}, "d"),
    ({"op": "contains", "field": "attrs.note", "value": "ünï"}, "e"),
    # Only A-Z fold: Ü is not ü.
    ({"op": "contains", "field": "attrs.note", "value": "ÜNÏ"}, ""),
]


@pytest.mark.parametrize("predicate,keys", TEXT_CONTAINS_CASES,
                         ids=[json.dumps(c[0])[:70] for c in TEXT_CONTAINS_CASES])
def test_text_contains_selects_what_python_selects(world: dict, predicate, keys) -> None:
    ids = world["ids"]
    with psycopg.connect(world["dsn"], autocommit=True) as conn:
        from_clause, params = _relation(world, conn)
        compiled = world["mcp"]._okg_compile_operator_predicate(
            "nodes", predicate, params, declared=DECLARED, relation="rel")
        assert compiled is not None, predicate
        new_params = dict(params)
        new = _select_ids(conn, from_clause, compiled.bind_into(new_params), new_params)
        python = _python_ids(world, conn, from_clause, params, predicate)
    assert new == python, (predicate, new, python)
    assert new & set(ids.values()) == {ids[key] for key in keys}, predicate


#: Stored values of one attribute, by row, for text `contains` on a list and
#: on every other JSON type (#3666 review P1-1): a list matches by one of its
#: string elements; a number, true/false, null, an object or a nested list
#: never matches.
LIST_ROWS = {
    "a": {"tags": ["Lib-Core", "x"]},
    "b": {"tags": "src/lib-core.py"},
    "c": {"tags": [5, True, None]},
    "d": {"tags": [["lib-core"]]},
    "e": {"tags": {"k": "lib-core"}},
    "f": {},
    "g": {"tags": 5},
    "h": {"tags": []},
}
LIST_CASES = [
    ({"op": "contains", "field": "attrs.tags", "value": "LIB-CORE"}, "ab"),
    ({"op": "not", "clause": {"op": "contains", "field": "attrs.tags", "value": "lib-core"}},
     "cdefgh"),
    ({"op": "contains", "field": "attrs.tags", "value": "5"}, ""),
    ({"op": "contains", "field": "attrs.tags", "value": "x"}, "a"),
]


@pytest.mark.parametrize("predicate,keys", LIST_CASES,
                         ids=[json.dumps(c[0])[:70] for c in LIST_CASES])
def test_text_contains_on_lists_and_other_types_selects_what_python_selects(
    clean_runtime: str, predicate, keys,
) -> None:
    from okg.substrate.mcp.operator_sql import (
        compile_read_predicate,
        effective_attr_lowerer,
        effective_field_map_identity,
    )

    compiled = compile_read_predicate(
        predicate, leaf_lowerer=effective_attr_lowerer("nodes", relation="rel"),
        field_map_identity=effective_field_map_identity("nodes", None), prefix="pv",
    )
    assert compiled.qualification == "exact" and compiled.truth_exact
    params: dict[str, Any] = {"rows": json.dumps(LIST_ROWS)}
    where = compiled.bind_into(params)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        selected = {
            row[0] for row in conn.execute(
                "SELECT rel.target_id FROM jsonb_each(%(rows)s::jsonb) "
                f"AS rel(target_id, attrs) WHERE {where}",
                params,
            ).fetchall()
        }
    mcp = server_mod.build_server(dsn=clean_runtime)
    try:
        matches = mcp._okg_operator_predicate_matches
        python = {key for key, attrs in LIST_ROWS.items()
                  if matches({"attrs": attrs}, predicate)}
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()
    assert selected == python == set(keys), (predicate, selected, python)


def test_intended_difference_ts_equality_under_not(world: dict) -> None:
    predicate = OLD_TS_EQ_THREE_VALUED
    with psycopg.connect(world["dsn"], autocommit=True) as conn:
        from_clause, params = _relation(world, conn)
        old_params = dict(params)
        old_sql = legacy.legacy_effective_predicate_sql(
            "nodes", predicate, old_params, declared=DECLARED, relation="rel")
        compiled = world["mcp"]._okg_compile_operator_predicate(
            "nodes", predicate, params, declared=DECLARED, relation="rel")
        new_params = dict(params)
        new = _select_ids(conn, from_clause, compiled.bind_into(new_params), new_params)
        old = _select_ids(conn, from_clause, old_sql, old_params)
        python = _python_ids(world, conn, from_clause, params, predicate)
    ids = world["ids"]
    assert new == python
    # Rows without `due` (d, f, g) are kept now; the old SQL dropped them.
    assert python - old == {ids["d"], ids["f"], ids["g"]}


def test_overlay_corrected_values_are_what_every_path_compares(world: dict) -> None:
    """INTENDED DIFFERENCE 2 (the point of #3201): the run_sql path compared
    STORED attributes. d is stored `closed`, effectively `open`."""
    ids, tools = world["ids"], world["tools"]
    is_open = _eq("attrs.status", "open")
    want = {ids["a"], ids["c"], ids["d"]}
    scope = {"kind": "nodes", "subtype": "item"}

    unordered = tools["filter"](input=scope, predicate=is_open, select=["node_id"])
    ordered = tools["filter"](input=scope, predicate=is_open, order_by="node_id",
                              select=["node_id"])
    assert set(_item_ids(unordered)) == set(_item_ids(ordered)) == want
    assert unordered["predicate_fingerprint"] == ordered["predicate_fingerprint"]

    by_status = tools["aggregate"](input=scope, by=["attrs.status"], metrics=["count"])
    filtered_count = tools["aggregate"](
        input={**scope, "filter": is_open}, by=["subtype"], metrics=["count"])
    assert {g["attrs.status"]: g["count"] for g in by_status["groups"]}["open"] == 3
    assert filtered_count["groups"] == [{"subtype": "item", "count": 3}]

    # INTENDED DIFFERENCE 3: the old run_sql expand could not filter on attrs
    # at all. Its join exposed two `attrs` columns, so the rendered
    # `attrs @> ...` was ambiguous and every attrs destination filter failed.
    with pytest.raises(RunSqlInvalid, match="ambiguous"):
        _old(world, legacy.expand_filtered, node_id=world["hub"], edge_type="links",
             direction="out", dest_filter=is_open, order_by=None, limit=50, offset=0,
             select=None)
    new = tools["expand"](input={"node_id": world["hub"]}, edge_type="links",
                          direction="out", filter=is_open, limit=50)
    assert {r["node_id"] for r in new["neighbors"]} == want  # #3666: rows are the linked records
    # Every neighbour expand returns is one whose SERVED attrs pass the filter.
    served = tools["filter"](
        input={"kind": "nodes"},
        predicate={"op": "in", "field": "node_id", "values": sorted(want)},
        select=["node_id", "attrs.status"],
    )
    # Served values arrive fenced as untrusted data; the value inside is it.
    unfenced = {
        re.sub(r"⟦/?UNTRUSTED_DATA[^⟧]*⟧", "", row["attrs_status"]).strip()
        for row in served["items"]
    }
    assert len(served["items"]) == 3 and unfenced == {"open"}


# ---------------------------------------------------------- hostile operands


def _python_note_ids(world: dict, op: str, value: str) -> set[str]:
    with psycopg.connect(world["dsn"], autocommit=True) as conn:
        from_clause, params = _relation(world, conn)
        return _python_ids(world, conn, from_clause, params,
                           {"op": op, "field": "attrs.note", "value": value})


@pytest.mark.parametrize("key", sorted(HOSTILE))
def test_hostile_operands_select_the_python_rows(world: dict, key: str) -> None:
    value, tools, ids = HOSTILE[key], world["tools"], world["ids"]
    scope = {"kind": "nodes", "subtype": "item"}
    for op in ("eq", "prefix", "suffix"):
        want = _python_note_ids(world, op, value)
        assert ids[key] in want
        predicate = {"op": op, "field": "attrs.note", "value": value}
        got = tools["filter"](input=scope, predicate=predicate, select=["node_id"])
        ordered = tools["filter"](input=scope, predicate=predicate, order_by="node_id desc",
                                  select=["node_id"])
        assert set(_item_ids(got)) == set(_item_ids(ordered)) == want
        expanded = tools["expand"](input={"node_id": world["hub"]}, edge_type="links",
                                   filter=predicate, limit=50)
        assert {r["node_id"] for r in expanded["neighbors"]} == want  # #3666: record rows
        counted = tools["aggregate"](input={**scope, "filter": predicate}, by=["subtype"],
                                     metrics=["count"])
        assert sum(g["count"] for g in counted["groups"]) == len(want)


@pytest.fixture
def bound_params(monkeypatch) -> list[Any]:
    """Every parameter set any cursor sends during the test."""
    seen: list[Any] = []
    real = psycopg.Cursor.execute

    def spy(self, query, params=None, *args, **kwargs):
        seen.append(params)
        return real(self, query, params, *args, **kwargs)

    monkeypatch.setattr(psycopg.Cursor, "execute", spy)
    return seen


def test_nul_operand_returns_no_rows_and_never_reaches_the_database(
    world: dict, bound_params: list[Any],
) -> None:
    """The failure case: PostgreSQL cannot hold NUL, so a NUL parameter fails
    the query. The compiler decides it without sending it."""
    tools = world["tools"]
    scope = {"kind": "nodes", "subtype": "item"}
    nul = {"op": "eq", "field": "attrs.note", "value": "O'Brien\x00"}
    assert tools["filter"](input=scope, predicate=nul, select=["node_id"])["items"] == []
    assert tools["filter"](input=scope, predicate=nul, order_by="node_id",
                           select=["node_id"])["items"] == []
    everyone = tools["filter"](input=scope, predicate={"op": "not", "clause": nul},
                               select=["node_id"], limit=100)
    assert len(everyone["items"]) == 7
    assert tools["expand"](input={"node_id": world["hub"]}, filter=nul)["neighbors"] == []
    assert tools["aggregate"](input={**scope, "filter": nul}, by=["subtype"],
                              metrics=["count"])["groups"] == []
    key_nul = {"op": "exists", "field": "attrs.no\x00te"}
    assert tools["filter"](input=scope, predicate=key_nul, select=["node_id"])["items"] == []
    assert bound_params, "the spy saw no query"
    assert "\x00" not in json.dumps(bound_params, default=str)


# ------------------------------------------------------------------ one path


def test_missing_snapshot_is_refused_by_name_and_never_reaches_run_sql(
    world: dict, monkeypatch,
) -> None:
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "0")
    with psycopg.connect(world["dsn"], autocommit=True) as conn:
        conn.execute(
            "DELETE FROM okg.attribute_effective_view WHERE generation_id = %s",
            (world["gen"],),
        )
    calls: list[str] = []

    def no_run_sql(*_a, **_k):
        calls.append("run_sql")
        raise AssertionError("a rerouted operator reached run_sql")

    monkeypatch.setattr("okg.substrate.mcp.run_sql.run_sql", no_run_sql)
    tools = world["tools"]
    attempts = {
        "filter": lambda: tools["filter"](input={"kind": "nodes", "subtype": "item"},
                                          predicate=None, order_by="node_id"),
        "aggregate": lambda: tools["aggregate"](input={"kind": "nodes"}, by=["subtype"],
                                                metrics=["count"]),
        "expand": lambda: tools["expand"](input={"node_id": world["hub"]},
                                          filter={"op": "eq", "field": "subtype",
                                                  "value": "item"}),
    }
    for name, attempt in attempts.items():
        with pytest.raises(SubstrateError) as refused:
            attempt()
        assert refused.value.code == "unsupported_operator_form", name
        assert refused.value.data["generation_id"] == world["gen"], name
        assert refused.value.data["target_kind"] in {"node", "edge"}, name
    assert calls == []


def test_mismatched_scope_narrowing_is_refused_on_every_set_path(world: dict) -> None:
    tools = world["tools"]
    with pytest.raises(ValueError, match="subtype applies to kind='nodes'"):
        tools["aggregate"](input={"kind": "edges", "subtype": "item"}, by=["edge_type"],
                           metrics=["count"])
    refused = tools["filter"](input={"kind": "edges", "subtype": "item"}, predicate=None,
                              order_by="edge_id")
    assert refused["error"]["code"] == "unsupported_operator_form"


# -------------------------------------------------------------------- counts


def _walk(fetch, *, limit: int) -> list[dict]:
    pages, offset = [], 0
    for _ in range(50):
        page = fetch(limit=limit, offset=offset)
        pages.append(page)
        if page["next_offset"] is None:
            return pages
        assert page["next_offset"] == offset + limit
        offset = page["next_offset"]
    raise AssertionError("walk did not end")


def test_counts_are_exact_from_a_complete_page_or_a_bounded_count(world: dict) -> None:
    tools = world["tools"]
    scope = {"kind": "nodes", "subtype": "item"}

    def ordered(**kw):
        return tools["filter"](input=scope, predicate=None, order_by="node_id",
                               select=["node_id"], **kw)

    whole = ordered(limit=100)
    assert whole["total_count"] == 7 and whole["total_count_basis"] == "exact"
    assert whole["coverage"]["delivery"]["complete"] is True
    pages = _walk(ordered, limit=2)
    assert [p["total_count"] for p in pages] == [7, 7, 7, 7]
    assert {p["total_count_basis"] for p in pages} == {"exact"}
    assert sum((_item_ids(p) for p in pages), []) == _item_ids(whole)
    assert pages[0]["coverage"]["retrieval"]["status"] == "exhaustive"

    mapped = tools["map"](input=scope, limit=3)
    assert mapped["total_count"] == 7 and mapped["total_count_basis"] == "exact"

    def expand(**kw):
        return tools["expand"](input={"node_id": world["hub"]}, order_by="node_id", **kw)

    expanded = _walk(expand, limit=3)
    assert [p["total_count"] for p in expanded] == [7, 7, 7]
    assert all("coverage" in p for p in expanded)
    assert expanded[0]["coverage"]["delivery"]["complete"] is False
    assert expanded[-1]["coverage"]["delivery"]["complete"] is True


def test_a_dropped_row_is_never_an_exact_count(world: dict, monkeypatch) -> None:
    """The read check dropped a row: a SQL count could include rows this
    reader cannot see, so a cut first page is a lower bound and a later page
    is unknown."""
    real_nodes = server_mod.filter_node_rows_for_session
    real_edges = server_mod.filter_edge_rows_for_session
    hidden = world["ids"]["a"]

    def hide_nodes(conn, session, rows, **kw):
        return [r for r in real_nodes(conn, session, rows, **kw) if r.get("node_id") != hidden]

    def hide_edges(conn, session, rows, **kw):
        return [r for r in real_edges(conn, session, rows, **kw) if r.get("dst") != hidden]

    monkeypatch.setattr(server_mod, "filter_node_rows_for_session", hide_nodes)
    monkeypatch.setattr(server_mod, "filter_edge_rows_for_session", hide_edges)
    tools = world["tools"]
    scope = {"kind": "nodes", "subtype": "item"}
    first = tools["filter"](input=scope, predicate=None, order_by="node_id", limit=2)
    assert first["total_count_basis"] == "lower_bound"
    assert hidden not in _item_ids(first)
    hub_first = tools["expand"](input={"node_id": world["hub"]}, order_by="node_id", limit=2)
    assert hidden not in {r["node_id"] for r in hub_first["neighbors"]}  # #3666: record rows
    assert hub_first["total_count_basis"] == "lower_bound"
    # Where reads really are hidden, the reader's policy is evaluated: then
    # no SQL count is used at all, and a later page's count is unknown.
    monkeypatch.setattr(server_mod, "deployment_requires_nomos_runtime", lambda _name: True)
    later = tools["filter"](input=scope, predicate=None, order_by="node_id", limit=2,
                            offset=2)
    assert later["total_count"] is None
    assert later["total_count_basis"] == "unknown"


def test_a_timed_out_count_is_unknown_never_zero(world: dict, monkeypatch) -> None:
    monkeypatch.setattr(server_mod, "_OPERATOR_COUNT_TIMEOUT_MS", 100)
    real = psycopg.Cursor.execute

    def slow_count(self, query, params=None, *args, **kwargs):
        if isinstance(query, str) and query.startswith("SELECT count(*) FROM (SELECT 1 "):
            query, params = "SELECT count(*) FROM pg_sleep(2)", None
        return real(self, query, params, *args, **kwargs)

    monkeypatch.setattr(psycopg.Cursor, "execute", slow_count)
    tools = world["tools"]
    page = tools["filter"](input={"kind": "nodes", "subtype": "item"}, predicate=None,
                           order_by="node_id", limit=2, select=["node_id"])
    assert page["total_count"] is None
    assert page["total_count_basis"] == "unknown"
    assert len(page["items"]) == 2 and page["next_offset"] == 2
    assert page["coverage"]["selection"]["size"] is None
    # The connection survived the cancelled count: the next read works.
    after = tools["filter"](input={"kind": "nodes", "subtype": "item"}, predicate=None,
                            order_by="node_id", limit=100, select=["node_id"])
    assert after["total_count"] == 7


def test_a_count_past_its_row_bound_is_a_lower_bound(world: dict, monkeypatch) -> None:
    monkeypatch.setattr(server_mod, "_OPERATOR_COUNT_CAP", 3)
    page = world["tools"]["filter"](input={"kind": "nodes", "subtype": "item"},
                                    predicate=None, order_by="node_id", limit=2)
    assert page["total_count"] == 3
    assert page["total_count_basis"] == "lower_bound"
    assert page["coverage"]["retrieval"]["status"] == "bounded"


def test_bounded_count_seam_reports_a_timeout_as_unknown(world: dict, monkeypatch) -> None:
    monkeypatch.setattr(server_mod, "_OPERATOR_COUNT_TIMEOUT_MS", 50)
    with psycopg.connect(world["dsn"], autocommit=True) as conn:
        counted = world["mcp"]._okg_bounded_operator_count(
            conn, from_where_sql="FROM pg_sleep(1) AS s WHERE TRUE", params={},
        )
        assert counted == (None, "unknown")
        with conn.cursor() as cur:
            cur.execute("SHOW statement_timeout")
            assert cur.fetchone()[0] in {"0", "0ms"}


# ------------------------------------------------------------- fingerprints


def test_every_rerouted_response_carries_the_predicate_fingerprint(world: dict) -> None:
    tools = world["tools"]
    scope = {"kind": "nodes", "subtype": "item"}
    pred = _eq("attrs.status", "open")
    a = tools["filter"](input=scope, predicate=pred, order_by="node_id")
    b = tools["map"](input={**scope, "filter": pred})
    c = tools["aggregate"](input={**scope, "filter": pred}, by=["subtype"], metrics=["count"])
    d = tools["expand"](input={"node_id": world["hub"]}, filter=pred)
    other = tools["filter"](input=scope, predicate=_eq("attrs.status", "closed"),
                            order_by="node_id")
    for page in (a, b, c, d, other):
        assert len(page["predicate_fingerprint"]) == 64
    assert a["predicate_fingerprint"] == b["predicate_fingerprint"] == c["predicate_fingerprint"]
    assert other["predicate_fingerprint"] != a["predicate_fingerprint"]


# ----------------------------------------------------------------- latency


def test_rerouted_path_latency_is_recorded(world: dict) -> None:
    """Old (run_sql helpers) versus new (effective SQL) on this fixture.

    No threshold: this records the measurement. Set
    OKG_PARITY_LATENCY_OUT to a path to keep the JSON."""
    tools = world["tools"]
    scope = {"kind": "nodes", "subtype": "item"}
    hub = world["hub"]
    report: dict[str, dict[str, float]] = {}
    # The old helpers reuse one open connection, as the tools reuse their pool.
    with psycopg.connect(world["dsn"], autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        pairs = {
            "filter_order_by": (
                lambda: legacy.filter_ordered(
                    conn, session, input=scope, predicate=None, order_by="node_id",
                    limit=3, offset=0, select=["node_id"]),
                lambda: tools["filter"](input=scope, predicate=None, order_by="node_id",
                                        limit=3, select=["node_id"]),
            ),
            "aggregate_set": (
                lambda: legacy.aggregate_set(
                    conn, session, input={"kind": "edges"}, by=["src"],
                    metrics=["count"], limit=2),
                lambda: tools["aggregate"](input={"kind": "edges"}, by=["src"],
                                           metrics=["count"], limit=2),
            ),
            "expand_filtered": (
                lambda: legacy.expand_filtered(
                    conn, session, hub, edge_type="links", direction="out",
                    dest_filter=_eq("subtype", "item"), order_by="node_id",
                    limit=3, offset=0, select=None),
                lambda: tools["expand"](input={"node_id": hub}, edge_type="links",
                                        filter=_eq("subtype", "item"), order_by="node_id",
                                        limit=3),
            ),
        }
        for name, (old, new) in pairs.items():
            timings: dict[str, list[float]] = {"old": [], "new": []}
            for _ in range(12):
                for label, call in (("old", old), ("new", new)):
                    start = time.perf_counter()
                    call()
                    timings[label].append((time.perf_counter() - start) * 1000)
            report[name] = {
                f"{label}_median_ms": round(statistics.median(values[2:]), 2)
                for label, values in timings.items()
            }
    out = os.environ.get("OKG_PARITY_LATENCY_OUT")
    if out:
        with open(out, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2, sort_keys=True)
    assert set(report) == set(pairs)
