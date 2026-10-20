"""Shared seeding and reads for the okg#3112 step 2 tests (PACT
lane-h-3112-step2). Not a test module."""

from __future__ import annotations

from typing import Any

import psycopg
from psycopg.rows import dict_row

from okg.substrate import declared_attr_indexes as dai
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import (
    SubtypeSpec,
    bind_test_catalog_writer,
    seed_catalog,
)

DEPLOYMENT = "default"
#: The E11 declarations: one attribute of every value type.
DECL = {"t_text": "text", "t_num": "num", "t_bool": "bool", "t_ts": "ts"}
SOURCE = "dattr-test"
ID_COLUMN = {"node": "node_id", "edge": "edge_id"}

#: E11's value pools: well-typed and mistyped values of every declared type.
POOL: dict[str, list[Any]] = {
    "t_text": ["alpha", "", "Ünïcode ✓", "x" * 600, 42, True, None, {"o": 1}, "beta", "gamma"],
    "t_num": [1, 1.5, -0.0, 1e20, 123456789012345678901234567890, "42", True, None, 7, 0.000001],
    "t_bool": [True, False, "true", 1, None, True, False],
    "t_ts": [
        "2024-01-02", "2024-01-02T03:04:05Z", "2024-01-02 10:00:00+02",
        "2024-01-02T10:00:00.123456+05:30", "01/02/2024", "not a date", 5,
        "2024-01-02 10:00 EST", "today", "epoch", "2024-13-45", None,
        "2024-01-02T03:04:05.123456789Z", "2024-01-02t03:04:05z",
    ],
}


def seed(dsn: str, decl: dict[str, str] | None = None) -> Any:
    """One applied catalog: ``thing`` declares ``decl``, ``other`` declares
    nothing (its declared keys are still admitted: admission is
    attribute-global)."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=[
                SubtypeSpec(name="thing", metadata={"filterable_attrs": dict(decl or DECL)}),
                SubtypeSpec(name="other"),
            ],
            narrowings=[("thing", "rel", "thing"), ("thing", "to_other", "other")],
        )
    return bind_test_catalog_writer(dsn, DEPLOYMENT)


def redeclare(dsn: str, decl: dict[str, str]) -> Any:
    """A new applied catalog version with other declarations; returns the
    writer identity bound to it."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=[
                SubtypeSpec(name="thing", metadata={"filterable_attrs": dict(decl)}),
                SubtypeSpec(name="other"),
            ],
            narrowings=[("thing", "rel", "thing"), ("thing", "to_other", "other")],
        )
    return bind_test_catalog_writer(dsn, DEPLOYMENT)


def apply(dsn: str, facts: list[Any]) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            facts, source=SOURCE, strict_endpoints=True
        )


def publish(dsn: str, identity: Any) -> dict[str, Any]:
    res = Publisher(dsn, DEPLOYMENT, catalog_identity=identity).cycle()
    assert res.get("status") == "published", res
    return res


def nid(i: int) -> str:
    return f"dattr:thing:{i:04d}"


def oid(i: int) -> str:
    return f"dattr:other:{i:04d}"


def node_attrs(i: int, rev: int, *, drop: str | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {"label": f"n{i} r{rev}"}
    for key, pool in POOL.items():
        if key == drop or (i + rev) % 11 == len(key):
            continue
        out[key] = pool[(i * 3 + rev * 5 + len(key)) % len(pool)]
    return out


def node(i: int, rev: int, **kw: Any) -> NodeFact:
    return NodeFact(nid(i), "thing", node_attrs(i, rev, **kw), {"row": i}, {"rev": rev})


def edges(rng: range, rev: int) -> list[EdgeFact]:
    out = []
    for i in rng:
        attrs: dict[str, Any] = (
            {"t_text": f"e{i % 7}", "t_num": i % 5} if i % 3
            else {"t_bool": bool(i % 2), "t_ts": "2024-03-04T05:06:07Z"}
        )
        if i % 10 == 0:
            attrs = {"t_num": "mistyped"}
        if rev > 1 and i % 4 == 0:
            attrs = {**attrs, "t_text": f"e{i % 7}-r{rev}"}
        out.append(EdgeFact(
            src=nid(i), dst=nid(i + 1), edge_type="rel", attrs=attrs,
            source_record_id={"e": i}, source_revision={"rev": rev},
        ))
    return out


def generation_1() -> tuple[list[Any], list[Any]]:
    nodes: list[Any] = [node(i, 1) for i in range(300)]
    nodes += [
        NodeFact(oid(i), "other", {"t_text": f"o{i % 4}", "t_num": i, "t_ts": "2024-05-06"},
                 {"o": i}, {"rev": 1})
        for i in range(40)
    ]
    rels = edges(range(250), 1) + [
        EdgeFact(src=nid(i), dst=oid(i % 40), edge_type="to_other", attrs={"t_text": "to-o"})
        for i in range(0, 300, 7)
    ]
    return nodes, rels


def generation_2() -> tuple[list[Any], list[Any]]:
    """Changes, tombstones (and the incident edges they take), explicit edge
    retractions, new nodes."""
    nodes: list[Any] = [node(i, 2) for i in range(0, 120, 2)]
    nodes += [NodeFact(nid(i), "thing", {}, {"row": i}, {"rev": 2}, op="R") for i in range(200, 220)]
    nodes += [node(i, 2) for i in range(300, 330)]
    retract = [
        EdgeFact(src=nid(i), dst=nid(i + 1), edge_type="rel", attrs={},
                 source_record_id={"e": i}, source_revision={"rev": 2}, op="R")
        for i in range(150, 165)
    ]
    return nodes, retract + edges(range(0, 120, 2), 2)


def generation_3() -> tuple[list[Any], list[Any]]:
    """Reopened nodes, a removed key, retyped values, more changes."""
    nodes: list[Any] = [node(i, 3) for i in range(200, 210)]
    nodes += [node(i, 3, drop="t_num") for i in range(10, 20)]
    nodes += [
        NodeFact(oid(i), "other", {"t_text": i, "t_num": f"n{i}"}, {"o": i}, {"rev": 3})
        for i in range(0, 40, 3)
    ]
    nodes += [node(i, 3) for i in range(1, 60, 2)]
    return nodes, []


def physical(dsn: str) -> dict[str, str]:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor(row_factory=dict_row) as cur:
        tables = dai._graph_tables(cur)
    assert tables is not None
    return tables


def reconcile(dsn: str, **kw: Any) -> dict[str, Any]:
    kw.setdefault("apply", True)
    kw.setdefault("enable", True)
    return dai.reconcile(dsn, deployment=DEPLOYMENT, **kw)


def manifest(dsn: str) -> dict[str, dict[str, Any]]:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT * FROM okg.declared_attr_indexes ORDER BY index_name")
        return {r["index_name"]: dict(r) for r in cur.fetchall()}


def catalog(dsn: str) -> dict[str, dict[str, Any]]:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor(row_factory=dict_row) as cur:
        return dai._dattr_catalog(cur)


def ordinals(dsn: str) -> list[dict[str, Any]]:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor(row_factory=dict_row) as cur:
        return dai._ordinals(cur, "default")


def typed(attr: str, value_type: str, alias: str) -> str:
    return dai._typed_text(value_type, f"{alias}.attrs -> {dai._literal(attr)}")


def graph_set(
    cur: Any, *, table: str, kind: str, attr: str, value_type: str, bp: int
) -> list[tuple[Any, ...]]:
    """G: the graph rows visible at ``bp`` that admit the key, read from the
    heap (index scans off)."""
    target = f"g.{ID_COLUMN[kind]}::text"
    cur.execute("SET enable_indexscan = off")
    cur.execute("SET enable_bitmapscan = off")
    cur.execute("SET enable_indexonlyscan = off")
    try:
        cur.execute(
            f"SELECT {target}, {typed(attr, value_type, 'g')} FROM public.\"{table}\" g "
            f"WHERE g.deleted = false AND jsonb_typeof(g.attrs -> %s) = %s "
            f"AND g.live_lo <= %s AND %s < g.live_hi",
            (attr, dai._JSON_TYPE[value_type], bp, bp),
        )
        rows = [tuple(r.values()) if isinstance(r, dict) else tuple(r) for r in cur.fetchall()]
    finally:
        cur.execute("RESET enable_indexscan")
        cur.execute("RESET enable_bitmapscan")
        cur.execute("RESET enable_indexonlyscan")
    return sorted(r for r in rows if r[1] is not None)


def g_equals_x(dsn: str) -> dict[str, Any]:
    """G and X for every (ordinal, kind, declared attribute) with an index,
    the mismatches, and the plans that did not read through the index."""
    tables = physical(dsn)
    rows = manifest(dsn)
    out: dict[str, Any] = {"pairs": [], "mismatches": [], "plans_without_index": []}
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor(row_factory=dict_row) as cur:
        for o in ordinals(dsn):
            for row in rows.values():
                kind, attr, vt = row["target_kind"], row["attr_id"], row["value_type"]
                g = graph_set(cur, table=tables[kind], kind=kind, attr=attr,
                              value_type=vt, bp=int(o["bp"]))
                x, plan = index_set(cur, index_name=row["index_name"], table=tables[kind],
                                    kind=kind, attr=attr, value_type=vt, bp=int(o["bp"]))
                out["pairs"].append((int(o["ordinal"]), kind, attr, len(g)))
                if g != x:
                    out["mismatches"].append({
                        "ordinal": int(o["ordinal"]), "kind": kind, "attr": attr,
                        "g_minus_x": sorted(set(g) - set(x))[:5],
                        "x_minus_g": sorted(set(x) - set(g))[:5],
                    })
                if row["index_name"] not in plan:
                    out["plans_without_index"].append((kind, attr, plan))
    return out


def index_set(
    cur: Any, *, index_name: str, table: str, kind: str, attr: str, value_type: str, bp: int
) -> tuple[list[tuple[Any, ...]], str]:
    """X: the rows an index holds that are visible at ``bp``, read through it.

    The WHERE clause is the index's OWN predicate and key, read back from
    the catalog (``pg_get_expr``), so a wrongly built index shows up here.
    Returns the rows and the plan."""
    cur.execute(
        """
        SELECT pg_get_expr(i.indpred, i.indrelid) AS pred,
               pg_get_indexdef(i.indexrelid, 1, true) AS key
          FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
         WHERE c.relname = %s
        """,
        (index_name,),
    )
    row = cur.fetchone()
    pred = row["pred"] if isinstance(row, dict) else row[0]
    key = row["key"] if isinstance(row, dict) else row[1]
    target = f"{ID_COLUMN[kind]}::text"
    statement = (
        f"SELECT {target}, {typed(attr, value_type, 'g')} FROM public.\"{table}\" g "
        f"WHERE {pred} AND ({key}) IS NOT NULL AND live_lo <= %s AND %s < live_hi"
    )
    cur.execute("BEGIN")
    try:
        cur.execute("SET LOCAL enable_seqscan = off")
        cur.execute("EXPLAIN (COSTS OFF) " + statement, (bp, bp))
        plan = "\n".join(
            str(next(iter(r.values())) if isinstance(r, dict) else r[0]) for r in cur.fetchall()
        )
        cur.execute(statement, (bp, bp))
        rows = [tuple(r.values()) if isinstance(r, dict) else tuple(r) for r in cur.fetchall()]
    finally:
        cur.execute("ROLLBACK")
    return sorted(rows), plan
