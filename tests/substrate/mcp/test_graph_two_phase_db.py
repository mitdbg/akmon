"""The two-phase read for an undeclared-key equality (PostgreSQL).

PACT lane-s-storage-redesign-s2, requirements ``two-phase-read`` and ``gin``
(S2 part); design section 2.4 and the parent's 1.4.1. The cut K is lowered to
4 (a test seam on ``graph_read.TWO_PHASE_CUT``) and the page size to 2 or 3,
so each phase-2 path is reached on a 40-node fixture; the subtype row
estimate is replaced where a case needs a given threshold M. Every page is
compared with a Python recompute (since PACT lane-s-storage-cutover the graph
path is the only read path).
"""
from __future__ import annotations

import uuid
from typing import Any

import psycopg
import pytest

from okg.substrate.attributes import (
    ATTRIBUTE_EFFECTIVE_TABLE,
    OVERLAY_RANGE_OPEN_ORD,
    OVERLAY_RANGES_TABLE,
    materialize_effective_attributes_for_generation,
    record_attribute_observation,
)
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.mcp import graph_read, open_session
from okg.substrate.mcp import server as server_mod
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import (
    SubtypeSpec,
    bind_test_catalog_writer,
    seed_catalog,
)

pytestmark = pytest.mark.postgres

CUT = 4
RED = {"op": "eq", "field": "attrs.color", "value": "red"}


class _World:
    """40 ``doc`` nodes (ids sort the same under any collation) whose graph
    ``color`` is red at ``red``; ``overlays`` maps an index to an overlay
    value of ``color`` (None: no overlay). Plus ``four`` (exactly CUT nodes,
    never red) and ``small`` (3 nodes)."""

    def __init__(self, dsn: str, red: set[int], overlays: dict[int, str] | None = None,
                 *, extra: dict[int, dict[str, Any]] | None = None,
                 key_overlays: list[tuple[int, str, Any]] | None = None):
        self.dsn = dsn
        extra = extra or {}
        self.s = uuid.uuid4().hex[:6]
        with psycopg.connect(dsn, autocommit=True) as conn:
            seed_catalog(conn, subtypes=[
                SubtypeSpec(name="doc", metadata={"filterable_attrs": {"rank": "num"}}),
                SubtypeSpec(name="four"), SubtypeSpec(name="small"),
            ])
            store = ChronosGraphStore.from_connection(conn)
            facts = [
                NodeFact(self.doc(i), "doc",
                         {"color": "red" if i in red else "blue", "rank": i,
                          **extra.get(i, {})},
                         {"id": self.doc(i)}, {"v": 1})
                for i in range(40)
            ]
            facts += [NodeFact(f"four:{i}:{self.s}", "four", {"color": "blue"},
                               {"id": f"four:{i}:{self.s}"}, {"v": 1}) for i in range(CUT)]
            facts += [NodeFact(f"small:{i}:{self.s}", "small", {"color": "blue"},
                               {"id": f"small:{i}:{self.s}"}, {"v": 1}) for i in range(3)]
            store.apply_facts(facts, source="default", strict_endpoints=True)
            store.apply_facts([EdgeFact(self.doc(0), self.doc(1), "references",
                                        {"color": "red"},
                                        source_record_id={"id": f"e{self.s}"},
                                        source_revision={"v": 1})],
                              source="default")
        out = Publisher(dsn, "default",
                        catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()
        assert out["status"] == "published", out
        self.gen = str(out["generation_id"])
        planted = [(i, "color", v) for i, v in (overlays or {}).items()]
        planted += key_overlays or []
        if planted:
            with psycopg.connect(dsn, autocommit=True) as conn:
                for i, key, value in planted:
                    record_attribute_observation(
                        conn, target_kind="node", target_id=self.doc(i),
                        generation_id=self.gen, attr_key=key, value=value,
                        producer="test:two-phase",
                    )
                materialize_effective_attributes_for_generation(conn, generation_id=self.gen)

    def doc(self, i: int) -> str:
        return f"doc:{i:02d}:{self.s}"

    def expected(self, subtype: str, value: Any, key: str = "color") -> list[str]:
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            checkout = ChronosGraphStore.from_connection(
                conn, ensure_schema=False).checkout_generation(self.gen)
            rows = {r["node_id"]: dict(r["attrs"]) for r in checkout.query(
                "SELECT node_id, attrs FROM okg.graph_nodes WHERE subtype = :s",
                {"s": subtype})}
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT target_id, attr_key, value FROM {ATTRIBUTE_EFFECTIVE_TABLE} "
                    "WHERE generation_id = %s AND target_kind = 'node'",
                    (self.gen,),
                )
                for target, k, v in cur.fetchall():
                    if target in rows:
                        rows[target][k] = v
        if value is None:
            return sorted(i for i, a in rows.items() if a.get(key) is None)
        return sorted(
            i for i, a in rows.items()
            if key in a and a[key] == value and type(a[key]) is not bool
        )


def _page(world: _World, monkeypatch, mode: str, predicate: Any, *, kind: str = "nodes",
          limit: int = 2, offset: int = 0, **kw: Any) -> tuple[list[dict], dict]:
    assert mode == "graph", mode
    mcp = server_mod.build_server(dsn=world.dsn, deployment_name="default")
    meta: dict[str, Any] = {}
    try:
        with psycopg.connect(world.dsn, autocommit=True) as conn:
            session = open_session(conn, deployment_name="default",
                                   requested_generation=world.gen)
            rows, more = mcp._okg_effective_view_sql_page(
                conn, session, kind=kind, predicate=predicate, limit=limit,
                offset=offset, page_meta=meta, **kw,
            )
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()
    meta["has_more"] = more
    return rows, meta


def _scoped(predicate: Any, subtype: str = "doc") -> Any:
    return {"op": "and", "clauses": [
        {"op": "eq", "field": "subtype", "value": subtype}, predicate]}


def _both(world: _World, monkeypatch, predicate: Any, *, subtype: str = "doc",
          limit: int = 2, offset: int = 0) -> dict:
    """Graph page (with its two-phase record) == the recompute's slice."""
    scoped = _scoped(predicate, subtype)
    graph, meta = _page(world, monkeypatch, "graph", scoped, limit=limit, offset=offset)
    expected = world.expected(subtype, predicate.get("value"),
                              key=predicate["field"].split(".", 1)[1])
    assert [r["node_id"] for r in graph] == expected[offset: offset + limit]
    assert meta["has_more"] is (len(expected) > offset + limit)
    return meta


@pytest.fixture(autouse=True)
def _small_cut(monkeypatch):
    monkeypatch.setattr(graph_read, "TWO_PHASE_CUT", CUT)


def _estimate(monkeypatch, value: float | None) -> None:
    monkeypatch.setattr(graph_read, "subtype_row_estimate", lambda *a, **k: value)


def test_phase_one_fills_the_page(clean_runtime, monkeypatch) -> None:
    world = _World(clean_runtime, red={0, 1, 2, 3})
    meta = _both(world, monkeypatch, RED)
    assert meta["two_phase"]["forms"] == ["phase1"]
    assert meta["two_phase"]["statements"] == 1


def test_a_stream_shorter_than_the_cut_is_final(clean_runtime, monkeypatch) -> None:
    world = _World(clean_runtime, red=set())
    meta = _both(world, monkeypatch, RED, subtype="small")
    assert meta["two_phase"]["forms"] == ["phase1"]
    assert meta["two_phase"]["phase1_ids"] == 3


def test_a_stream_of_exactly_the_cut_runs_phase_two(clean_runtime, monkeypatch) -> None:
    """n_ids == K proves nothing (there may be more ids), so phase 2 runs; a
    version that treats it as complete would stop after phase 1."""
    world = _World(clean_runtime, red=set())
    _estimate(monkeypatch, 1e9)
    meta = _both(world, monkeypatch, RED, subtype="four")
    assert meta["two_phase"]["phase1_ids"] == CUT
    assert len(meta["two_phase"]["forms"]) > 1


def test_continuation_reads_the_rest_of_the_stream(clean_runtime, monkeypatch) -> None:
    # Phase 1 (ids 0-3) holds one match; M is 1, so continuation; chunks
    # 4-7 (none), 8-11 (9) and 12-15 (13) fill a 3-row target.
    world = _World(clean_runtime, red={1, 13}, overlays={9: "red", 2: "green"})
    _estimate(monkeypatch, 1e9)
    meta = _both(world, monkeypatch, RED)
    record = meta["two_phase"]
    assert record["threshold"] == 1 and record["m"] == 1
    assert record["forms"] == ["phase1", "continuation"]
    assert record["statements"] == 4


def test_a_chunk_boundary_neither_repeats_nor_skips(clean_runtime, monkeypatch) -> None:
    """Matches at the last id of the phase-1 cut (3), the first id after it
    (4), the last of chunk 1 (7) and the first of chunk 2 (8). A version
    using id >= last would return 3 twice."""
    world = _World(clean_runtime, red={3, 4, 7, 8})
    _estimate(monkeypatch, 1e9)
    meta = _both(world, monkeypatch, RED, limit=3)
    assert meta["two_phase"]["forms"] == ["phase1", "continuation"]


def test_continuation_stops_at_its_cap_then_runs_the_gin_form(
    clean_runtime, monkeypatch,
) -> None:
    world = _World(clean_runtime, red={1, 30, 35})
    _estimate(monkeypatch, 1e9)
    meta = _both(world, monkeypatch, RED)
    record = meta["two_phase"]
    assert record["forms"] == ["phase1", "continuation", "gin"]
    assert record["continuation_capped"] is True
    assert record["statements"] == 1 + graph_read.CONTINUATION_CHUNK_CAP + 1


def test_few_matches_against_a_small_threshold_take_the_gin_form(
    clean_runtime, monkeypatch,
) -> None:
    world = _World(clean_runtime, red={1, 30, 35}, overlays={36: "red", 30: "blue"})
    _estimate(monkeypatch, 1.0)  # GIN cheap: M above m
    meta = _both(world, monkeypatch, RED)
    record = meta["two_phase"]
    assert record["m"] < record["threshold"]
    assert record["forms"] == ["phase1", "gin"]


def test_the_constructed_worst_case_at_fixture_scale(clean_runtime, monkeypatch) -> None:
    """A value absent from the first K ids but on every later one: phase 1
    finds none, so the computed threshold picks the GIN form, which is
    correct (the residual gap is cost, not rows)."""
    world = _World(clean_runtime, red=set(range(CUT, 40)))
    meta = _both(world, monkeypatch, RED)
    assert meta["two_phase"]["m"] == 0
    assert meta["two_phase"]["forms"][-1] == "gin"


def test_scalar_types_and_an_offset(clean_runtime, monkeypatch) -> None:
    world = _World(clean_runtime, red={5, 6, 20, 21, 22})
    _estimate(monkeypatch, 1e9)
    _both(world, monkeypatch, RED, limit=2, offset=2)
    for value in (7, 7.0):  # a number value, 7 against 7.0
        meta = _both(world, monkeypatch, {"op": "eq", "field": "attrs.rank", "value": value})
        assert "two_phase" not in meta  # rank is declared: never two-phase
    meta = _both(world, monkeypatch, {"op": "eq", "field": "attrs.color", "value": "blue"})
    assert meta["two_phase"]["forms"] == ["phase1"]


def test_the_gin_form_keeps_json_types_and_array_values_apart(
    clean_runtime, monkeypatch,
) -> None:
    """The GIN form on an undeclared key whose rows mix the string "1", the
    number 1 (and 1.0), arrays holding either, and an object; overlays turn
    a string into a number and back. Phase 1 (ids 0-3) finds no match, so
    every page is the GIN form's, and each equals
    the recompute: "1" never matches 1, an array holding the value never
    matches it. A version that binds the GIN value as text, or tests the
    key's value by containment (an array contains its element), fails."""
    extra = {
        5: {"code": 1}, 8: {"code": "1"}, 11: {"code": 1.0}, 14: {"code": [1]},
        17: {"code": ["1"]}, 20: {"code": {"v": 1}}, 23: {"code": "1"},
        26: {"code": 1}, 29: {"code": "1"}, 32: {"code": 1}, 35: {"code": [1, 2]},
        38: {"code": [1]},
    }
    world = _World(clean_runtime, red=set(), extra=extra, key_overlays=[
        (26, "code", "2"),   # a number 1 overlaid away
        (29, "code", 1),     # a string "1" overlaid to the number
        (32, "code", "1"),   # a number 1 overlaid to the string
        (35, "code", 1),     # an array overlaid to the number
        (38, "code", [1]),   # an array overlaid with an equal array
    ])
    _estimate(monkeypatch, 1.0)  # m = 0 in phase 1: the GIN form decides
    by_value = {"1": [8, 23, 32], 1: [5, 11, 29, 35]}
    for value, indexes in by_value.items():
        predicate = {"op": "eq", "field": "attrs.code", "value": value}
        meta = _both(world, monkeypatch, predicate, limit=10)
        assert meta["two_phase"]["m"] == 0
        assert meta["two_phase"]["forms"] == ["phase1", "gin"], value
        assert world.expected("doc", value, key="code") == [world.doc(i) for i in indexes]
    # An array value never takes the two-phase read; its rows are equal too.
    array = {"op": "eq", "field": "attrs.code", "value": [1]}
    meta = _both(world, monkeypatch, array, limit=10)
    assert "two_phase" not in meta
    assert world.expected("doc", [1], key="code") == [world.doc(14), world.doc(38)]


@pytest.mark.parametrize("predicate,kind,kw", [
    pytest.param({"op": "eq", "field": "attrs.color", "value": None}, "nodes", {}, id="null-eq"),
    pytest.param(RED, "edges", {}, id="edges"),
    pytest.param(RED, "nodes", {"order_by": "attrs.rank desc"}, id="non-default-order"),
    pytest.param({"op": "eq", "field": "attrs.rank", "value": 3}, "nodes", {}, id="declared"),
    pytest.param({"op": "or", "clauses": [RED, {"op": "eq", "field": "attrs.color",
                                                "value": "green"}]}, "nodes", {}, id="or"),
])
def test_other_shapes_use_the_single_statement_and_never_gin(
    clean_runtime, monkeypatch, predicate, kind, kw,
) -> None:
    world = _World(clean_runtime, red={30})
    statements: list[str] = []
    real = psycopg.Cursor.execute

    def capture(self, query, params=None, **k):
        statements.append(str(query))
        return real(self, query, params, **k)

    monkeypatch.setattr(psycopg.Cursor, "execute", capture)
    scope = ({"op": "eq", "field": "edge_type", "value": "references"} if kind == "edges"
             else {"op": "eq", "field": "subtype", "value": "doc"})
    scoped = {"op": "and", "clauses": [scope, predicate]}
    graph, meta = _page(world, monkeypatch, "graph", scoped, kind=kind, limit=5, **kw)
    monkeypatch.setattr(psycopg.Cursor, "execute", real)
    # Rows: the same page read with the two-phase key withheld, which takes
    # the single statement whatever the shape.
    monkeypatch.setattr(graph_read, "two_phase_key", lambda *_a, **_k: None)
    single, _ = _page(world, monkeypatch, "graph", scoped, kind=kind, limit=5, **kw)
    assert graph == single
    assert "two_phase" not in meta
    assert not [q for q in statements if "@>" in q or "WITH cut AS" in q]


def test_nomos_runtime_streams_in_the_batch_loop(clean_runtime, monkeypatch) -> None:
    world = _World(clean_runtime, red={1, 30, 35})
    rows, meta = _page(world, monkeypatch, "graph", _scoped(RED), nomos_runtime=True)
    assert "two_phase" not in meta
    # The read filter decides what is served (forced on here with no policy
    # bundle); whatever it serves is a match, in page order.
    served = [r["node_id"] for r in rows]
    assert set(served) <= set(world.expected("doc", RED["value"])), served
    assert served == sorted(served, key=world.expected("doc", RED["value"]).index)


def test_the_gin_form_is_never_the_first_statement_and_all_are_unprepared(
    clean_runtime, monkeypatch,
) -> None:
    world = _World(clean_runtime, red={1, 30, 35})
    _estimate(monkeypatch, 1.0)
    calls: list[tuple[str, Any]] = []
    real = psycopg.Cursor.execute

    def capture(self, query, params=None, **k):
        calls.append((str(query), k.get("prepare")))
        return real(self, query, params, **k)

    monkeypatch.setattr(psycopg.Cursor, "execute", capture)
    _page(world, monkeypatch, "graph", _scoped(RED))
    monkeypatch.setattr(psycopg.Cursor, "execute", real)
    reads = [(q, p) for q, p in calls if "graph_rows" in q and "LIMIT %(tp_" in q]
    assert reads and "WITH cut AS MATERIALIZED" in reads[0][0]
    assert "@>" not in reads[0][0]
    gin = [q for q, _p in reads if "@>" in q]
    assert len(gin) == 1 and reads.index((gin[0], False)) > 0
    assert all(p is False for _q, p in reads)
    assert not [q for q, _p in calls if q.lstrip().upper().startswith("SET ")
                or "set_config('enable" in q or "plan_cache_mode" in q]


def test_the_cut_is_applied_before_the_fetch(clean_runtime, monkeypatch) -> None:
    """EXPLAIN of the phase-1 statement: the LIMIT sits on the id stream in
    the CTE, below the fetch and the predicate."""
    world = _World(clean_runtime, red={1, 30})
    _estimate(monkeypatch, 1e9)
    captured: list[tuple[str, Any]] = []
    real = psycopg.Cursor.execute

    def capture(self, query, params=None, **k):
        if "WITH cut AS MATERIALIZED" in str(query):
            captured.append((str(query), dict(params)))
        return real(self, query, params, **k)

    monkeypatch.setattr(psycopg.Cursor, "execute", capture)
    _page(world, monkeypatch, "graph", _scoped(RED))
    monkeypatch.setattr(psycopg.Cursor, "execute", real)
    statement, bound = captured[0]
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("EXPLAIN " + statement, bound, prepare=False)
        plan = "\n".join(r[0] for r in cur.fetchall())
    cte = plan.split("CTE cut", 1)[1].split("->  Aggregate", 1)[0]
    assert "Limit" in cte, plan
    assert statement.index("LIMIT %(tp_cut)s") < statement.index("jsonb_object_agg")


def test_a_duplicate_overlay_range_changes_no_page_row(clean_runtime, monkeypatch) -> None:
    """S2 reads overlays from the per-generation table, never the ranges: a
    planted duplicate open range for an overlaid target changes nothing."""
    world = _World(clean_runtime, red={1, 13}, overlays={9: "red"})
    _estimate(monkeypatch, 1e9)
    before = _both(world, monkeypatch, RED)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            f"INSERT INTO {OVERLAY_RANGES_TABLE} (chronos_branch_id, target_kind, target_id, "
            "namespace, attr_key, from_ord, to_ord, observation_id, value, value_hash) "
            "VALUES ('default', 'node', %s, 'default', 'color', 0, %s, 'dup', "
            "'\"red\"', 'sha256:dup'), ('default', 'node', %s, 'default', 'color', 0, %s, "
            "'dup2', '\"red\"', 'sha256:dup2')",
            (world.doc(9), OVERLAY_RANGE_OPEN_ORD, world.doc(5), OVERLAY_RANGE_OPEN_ORD),
        )
    after = _both(world, monkeypatch, RED)
    assert after["two_phase"]["forms"] == before["two_phase"]["forms"]


@pytest.mark.parametrize("estimate", [1e9, 1.0])
def test_a_target_overlaid_on_two_filter_keys_appears_once(
    clean_runtime, monkeypatch, estimate: float,
) -> None:
    """Doc 9 is red in the graph and overlaid on both ``color`` and ``tag``; doc 1 is red in the
    graph and overlaid on ``tag``. Filtering on one key, or on both with an
    ``or``, returns each target once on both read paths and equals the
    recompute. A read that joins one row per overlay key, or that lets both
    branches of the ``or`` emit the target, lists it twice."""
    world = _World(
        clean_runtime, red={1, 9}, overlays={9: "red"},
        key_overlays=[(9, "tag", "x"), (1, "tag", "x"), (5, "tag", "x")],
    )
    _estimate(monkeypatch, estimate)
    # One filter key: the page equals the recompute and lists no id twice.
    meta = _both(world, monkeypatch, RED, limit=10)
    # The one-key read took the two-phase path (the gin form is never first).
    assert "two_phase" in meta
    assert meta["two_phase"]["forms"][0] == "phase1"
    graph, _ = _page(world, monkeypatch, "graph", _scoped(RED), limit=10)
    ids = [r["node_id"] for r in graph]
    assert ids == sorted({world.doc(1), world.doc(9)})
    # Two filter keys joined by ``or``: all three overlaid targets, once each.
    tag = {"op": "eq", "field": "attrs.tag", "value": "x"}
    either = _scoped({"op": "or", "clauses": [RED, tag]})
    graph, _ = _page(world, monkeypatch, "graph", either, limit=10)
    want = sorted(set(world.expected("doc", "red")) | set(world.expected("doc", "x", key="tag")))
    assert want == sorted({world.doc(1), world.doc(5), world.doc(9)})
    ids = [r["node_id"] for r in graph]
    assert ids == want
    assert len(ids) == len(set(ids))


def test_the_threshold_is_computed_from_the_estimate_not_fixed() -> None:
    def expected(n: float, cut: int, target: int) -> int:
        for m in range(1, target + 1):
            if 26.0 * cut * (target - m) / m <= 18.0 * (m / cut) * n:
                return m
        return target + 1

    large = graph_read.continuation_threshold(rows_estimate=320_000, cut=2000, target=51)
    small = graph_read.continuation_threshold(rows_estimate=20_000, cut=2000, target=51)
    assert large == expected(320_000, 2000, 51)
    assert small == expected(20_000, 2000, 51)
    assert large != small
    assert 15 <= large <= 30  # the parent's "about 23 at 320K rows"
    assert graph_read.continuation_threshold(rows_estimate=None, cut=2000, target=51) == 1


def test_the_row_estimate_follows_analyze(clean_runtime) -> None:
    from okg.substrate.mcp.semantic_readiness import resolve_node_segment

    _World(clean_runtime, red=set())
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        segment = resolve_node_segment(conn)
        conn.execute(f"ANALYZE {segment}")
        with conn.cursor() as cur:
            doc = graph_read.subtype_row_estimate(cur, segment, "doc")
            four = graph_read.subtype_row_estimate(cur, segment, "four")
    assert doc is not None and four is not None
    assert 20 <= doc <= 80 and doc > four


# --------------------------------------------------------------------------
# The split count of a two-phase-key equality (lane SG r1019 fix)
# --------------------------------------------------------------------------

#: Graph values of the undeclared key ``code`` per doc index: JSON types and
#: values the containment must keep apart or together.
_CODES: dict[int, dict[str, Any]] = {
    33: {"code": "1"}, 4: {"code": 1}, 6: {"code": 1.0}, 7: {"code": True},
    9: {"code": False}, 10: {"code": "true"}, 12: {"code": 2.5}, 13: {"code": [1]},
    15: {"code": {"v": 1}}, 16: {"code": None}, 18: {"code": 1}, 19: {"code": True},
    21: {"code": "1"}, 24: {"code": 2.5}, 27: {"code": 0}, 30: {"code": [True]},
}
_CODE_OVERLAYS: list[tuple[int, str, Any]] = [
    (18, "code", "x"),     # the number 1 overlaid away
    (19, "code", False),   # true overlaid to false
    (21, "code", 1),       # the string "1" overlaid to the number
    (25, "code", True),    # no code in the graph, overlaid to true
    (28, "code", 1.0),     # no code in the graph, overlaid to 1.0
    (24, "code", 2.5),     # an overlay equal to the graph value
    (4, "note", "other"),  # an overlay on another key only
]


def _same_json(a: Any, b: Any) -> bool:
    """``jsonb =`` on two scalars: a boolean equals only a boolean, numbers
    compare as numbers (1 and 1.0), anything else by type and value."""
    if isinstance(a, bool) or isinstance(b, bool):
        return type(a) is type(b) and a == b
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return a == b
    return type(a) is type(b) and a == b


def _effective_matches(world: _World, key: str, value: Any) -> list[str]:
    """The doc ids whose effective ``key`` equals ``value`` (the recompute)."""
    with psycopg.connect(world.dsn, autocommit=True) as conn:
        checkout = ChronosGraphStore.from_connection(
            conn, ensure_schema=False).checkout_generation(world.gen)
        rows = {r["node_id"]: dict(r["attrs"]) for r in checkout.query(
            "SELECT node_id, attrs FROM okg.graph_nodes WHERE subtype = 'doc'", {})}
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT target_id, attr_key, value FROM {ATTRIBUTE_EFFECTIVE_TABLE} "
                "WHERE generation_id = %s AND target_kind = 'node'", (world.gen,),
            )
            for target, k, v in cur.fetchall():
                if target in rows:
                    rows[target][k] = v
    return sorted(i for i, a in rows.items() if key in a and _same_json(a[key], value))


def _counted(world: _World, monkeypatch, mode: str, predicate: Any, *,
             kind: str = "nodes") -> tuple[tuple[int | None, str], dict]:
    """A one-row page's meta and its bounded count, as the tools run it."""
    assert mode == "graph", mode
    mcp = server_mod.build_server(dsn=world.dsn, deployment_name="default")
    meta: dict[str, Any] = {}
    try:
        with psycopg.connect(world.dsn, autocommit=True) as conn:
            session = open_session(conn, deployment_name="default",
                                   requested_generation=world.gen)
            mcp._okg_effective_view_sql_page(
                conn, session, kind=kind, predicate=predicate, limit=1, offset=0,
                page_meta=meta,
            )
            counted = mcp._okg_bounded_operator_count(
                conn, from_where_sql=meta["count_from_where_sql"],
                params=meta["params"],
            )
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()
    return counted, meta


def _node_gin(conn: psycopg.Connection) -> str:
    """The node table's ``jsonb_path_ops`` GIN index on ``attrs`` (built here
    when the test database has none)."""
    from okg.substrate.mcp.semantic_readiness import resolve_node_segment

    segment = resolve_node_segment(conn)
    schema, _, table = segment.rpartition(".")

    def find() -> str | None:
        row = conn.execute(
            "SELECT indexname FROM pg_indexes WHERE schemaname = %s AND tablename = %s "
            "AND indexdef ILIKE '%%USING gin (attrs jsonb_path_ops)%%'",
            (schema or "public", table),
        ).fetchone()
        return None if row is None else str(row[0])

    found = find()
    if found is None:
        conn.execute(f"CREATE INDEX lane_sg_test_node_attrs_gin ON {segment} "
                     "USING gin (attrs jsonb_path_ops) WHERE deleted = false")
        conn.execute(f"ANALYZE {segment}")
        found = find()
    assert found is not None
    return found


def _count_through_the_gin_index(world: _World, meta: dict) -> tuple[int, str]:
    """The count statement run with the GIN index the only index left on the
    node table and sequential scans off, in a transaction rolled back (the
    test database only): its plan and its count."""
    from okg.substrate.mcp.semantic_readiness import resolve_node_segment

    with psycopg.connect(world.dsn, autocommit=True) as conn:
        gin = _node_gin(conn)
    sql = f"SELECT count(*) FROM (SELECT 1 {meta['count_from_where_sql']}) AS c"
    with psycopg.connect(world.dsn) as conn:
        try:
            with conn.cursor() as cur:
                segment = resolve_node_segment(conn)
                schema, _, table = segment.rpartition(".")
                cur.execute(
                    "SELECT i.indexname FROM pg_indexes i WHERE i.schemaname = %s "
                    "AND i.tablename = %s AND i.indexname <> %s AND NOT EXISTS ("
                    "SELECT 1 FROM pg_constraint c WHERE c.conname = i.indexname)",
                    (schema or "public", table, gin),
                )
                for (name,) in cur.fetchall():
                    cur.execute(f'DROP INDEX "{schema}"."{name}"' if schema
                                else f'DROP INDEX "{name}"')
                cur.execute("SET LOCAL enable_seqscan = off")
                cur.execute("EXPLAIN " + sql, meta["params"], prepare=False)
                plan = "\n".join(r[0] for r in cur.fetchall())
                cur.execute(sql, meta["params"], prepare=False)
                n = int(cur.fetchone()[0])
        finally:
            conn.rollback()
    assert gin in plan, plan
    return n, plan


def test_the_split_count_of_a_rare_undeclared_equality_reads_the_gin_containment(
    clean_runtime, monkeypatch,
) -> None:
    """Gate shape b_undecl_rare: after a two-phase page whose phase 1 found
    no match in its cut, the split count's graph-id branch takes the
    two-phase key's containment pre-filter (Krama head10: before, that
    count walked every visible row, 4.3-4.9 s for 636 rows). For strings,
    integers, floats, 1 against 1.0, true and false, and an overlay that
    moves a row into, out of or onto its graph value, the count equals the
    recompute's count, also when the GIN index alone answers the
    containment. A value phase 1 finds in its cut (``blue`` on every row,
    ``early`` in the cut only) keeps the walk (b_undecl_all: the
    containment was slower there), with the same count. Without the fix no
    count has the containment."""
    world = _World(clean_runtime, red={20, 22}, extra={**_CODES, 1: {"code": "early"}},
                   key_overlays=_CODE_OVERLAYS)
    _estimate(monkeypatch, 1e9)
    cases: list[tuple[str, Any, list[int] | None, bool]] = [
        ("code", "1", [33], True), ("code", 1, [4, 6, 21, 28], True),
        ("code", 1.0, [4, 6, 21, 28], True), ("code", True, [7, 25], True),
        ("code", False, [9, 19], True), ("code", "true", [10], True),
        ("code", 2.5, [12, 24], True), ("code", 0, [27], True), ("code", "x", [18], True),
        ("color", "red", [20, 22], True),
        ("color", "blue", None, False), ("code", "early", [1], False),
    ]
    for key, value, indexes, gin in cases:
        predicate = _scoped({"op": "eq", "field": f"attrs.{key}", "value": value})
        expected = _effective_matches(world, key, value)
        if indexes is not None:
            assert expected == [world.doc(i) for i in indexes], (key, value)
        else:
            assert len(expected) == 38, (key, value)
        graph, meta = _counted(world, monkeypatch, "graph", predicate)
        case = (key, value)
        assert "two_phase" in meta, case
        assert meta["graph_form"].get("count_gin") is gin, (case, meta["graph_form"],
                                                             meta["two_phase"])
        assert ("@> jsonb_build_object(%(tc_gin_key)s::text, %(tc_gin_value)s::jsonb)"
                in meta["count_from_where_sql"]) is gin, case
        assert graph == (len(expected), "exact"), case
        if gin:
            through_gin, _plan = _count_through_the_gin_index(world, meta)
            assert through_gin == len(expected), case


def test_the_gin_count_follows_phase_one() -> None:
    """``gin_count_wanted``: a whole cut read, the page not filled, and at
    most 1% of the cut matching; anything else, or no record, walks."""
    def report(**kw: Any) -> dict[str, Any]:
        return {"cut": 2000, "phase1_ids": 2000, "m": 0, "target": 51, **kw}

    assert graph_read.gin_count_wanted(report())
    assert graph_read.gin_count_wanted(report(m=20))
    assert not graph_read.gin_count_wanted(report(m=21))
    assert not graph_read.gin_count_wanted(report(m=51))           # page filled
    assert not graph_read.gin_count_wanted(report(m=5, target=5))  # filled, small page
    assert not graph_read.gin_count_wanted(report(phase1_ids=1999))  # short stream
    assert not graph_read.gin_count_wanted({})
    assert not graph_read.gin_count_wanted({"cut": 2000})


@pytest.mark.parametrize("predicate,kind", [
    pytest.param({"op": "eq", "field": "attrs.code", "value": None}, "nodes", id="null-eq"),
    pytest.param({"op": "eq", "field": "attrs.code", "value": [1]}, "nodes", id="array"),
    pytest.param({"op": "eq", "field": "attrs.code", "value": {"v": 1}}, "nodes",
                 id="object"),
    pytest.param({"op": "eq", "field": "attrs.rank", "value": 3}, "nodes", id="declared"),
    pytest.param({"op": "or", "clauses": [RED, {"op": "eq", "field": "attrs.color",
                                                "value": "green"}]}, "nodes", id="or"),
    pytest.param(RED, "edges", id="edges"),
])
def test_other_shapes_count_without_the_containment(
    clean_runtime, monkeypatch, predicate, kind,
) -> None:
    """Null (it also matches a missing key), arrays and objects (containment
    is not equality for them), a declared key, an ``or`` and edges keep the
    count without the containment; the count is that of the whole set (a
    page large enough to hold it)."""
    world = _World(clean_runtime, red={30}, extra=_CODES, key_overlays=_CODE_OVERLAYS)
    scope = ({"op": "eq", "field": "edge_type", "value": "references"} if kind == "edges"
             else {"op": "eq", "field": "subtype", "value": "doc"})
    scoped = {"op": "and", "clauses": [scope, predicate]}
    graph, meta = _counted(world, monkeypatch, "graph", scoped, kind=kind)
    assert not meta["graph_form"].get("count_gin"), meta["graph_form"]
    assert "%(tc_gin_value)s" not in meta["count_from_where_sql"]
    whole, _meta = _page(world, monkeypatch, "graph", scoped, kind=kind, limit=500)
    assert graph == (len(whole), "exact")


def test_a_rerun_in_one_call_counts_once(clean_runtime, monkeypatch) -> None:
    """Inside one MCP call (``response_bounds.date_call``, the clock the
    byte-budget wrapper opens for every call, date or not) a rerun of the
    page at a smaller limit takes the first run's total: one count
    statement per call, sent unprepared; the next call counts again. The
    totals are the same. Without the S2 fix the graph path counted on
    every run. (Krama head10, through the tools: one count statement per
    call on every gate shape.)"""
    from okg.substrate.mcp.response_bounds import current_date_call, date_call

    world = _World(clean_runtime, red=set())
    predicate = _scoped({"op": "exists", "field": "attrs.color"})
    counts: list[Any] = []
    real = psycopg.Cursor.execute

    def capture(self, query, params=None, **kw):
        if "AS bounded_operator_count" in str(query):
            counts.append(kw.get("prepare", "not passed"))
        return real(self, query, params, **kw)

    mcp = server_mod.build_server(dsn=world.dsn, deployment_name="default")
    per_call: list[list[Any]] = []
    totals: list[tuple[int | None, str]] = []
    try:
        with psycopg.connect(world.dsn, autocommit=True) as conn:
            session = open_session(conn, deployment_name="default",
                                   requested_generation=world.gen)
            monkeypatch.setattr(psycopg.Cursor, "execute", capture)
            for _call in range(2):
                with date_call():
                    assert current_date_call() is not None
                    for limit in (30, 12):
                        meta: dict[str, Any] = {}
                        mcp._okg_effective_view_sql_page(
                            conn, session, kind="nodes", predicate=predicate,
                            limit=limit, offset=0, page_meta=meta,
                        )
                        totals.append(mcp._okg_bounded_operator_count(
                            conn, from_where_sql=meta["count_from_where_sql"],
                            params=meta["params"], date_budget=meta.get("date_budget"),
                        ))
                per_call.append(list(counts))
                counts.clear()
    finally:
        monkeypatch.setattr(psycopg.Cursor, "execute", real)
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()
    assert totals == [(40, "exact")] * 4, totals
    assert per_call == [[False], [False]], per_call


def test_a_rerun_whose_smaller_limit_fills_phase_one_counts_once(
    clean_runtime, monkeypatch,
) -> None:
    """S2 review nit N1: after a two-phase page the count's form (GIN
    containment or walk) follows ``m < target``, and target is offset +
    limit + 1. Here phase 1 finds 2 matches in its cut of 4: at limit 10
    the page is not filled (GIN form), at limit 1 it is (walk form). A
    rerun of one call at the smaller limit keeps the first run's form, so
    the count statement is the same and the call counts once. Without the
    fix the second run sent the walk form and counted again."""
    from okg.substrate.mcp.response_bounds import date_call

    monkeypatch.setattr(graph_read, "GIN_COUNT_MAX_PHASE1_SHARE", 1.0)
    _estimate(monkeypatch, 1.0)
    world = _World(clean_runtime, red={1, 2, 30})
    predicate = _scoped(RED)
    counts: list[Any] = []
    real = psycopg.Cursor.execute

    def capture(self, query, params=None, **kw):
        if "AS bounded_operator_count" in str(query):
            counts.append(kw.get("prepare", "not passed"))
        return real(self, query, params, **kw)

    mcp = server_mod.build_server(dsn=world.dsn, deployment_name="default")
    wanted: list[bool] = []
    forms: list[Any] = []
    totals: list[tuple[int | None, str]] = []
    try:
        with psycopg.connect(world.dsn, autocommit=True) as conn:
            session = open_session(conn, deployment_name="default",
                                   requested_generation=world.gen)
            monkeypatch.setattr(psycopg.Cursor, "execute", capture)
            with date_call():
                for limit in (10, 1):
                    meta: dict[str, Any] = {}
                    mcp._okg_effective_view_sql_page(
                        conn, session, kind="nodes", predicate=predicate,
                        limit=limit, offset=0, page_meta=meta,
                    )
                    assert "two_phase" in meta, limit
                    wanted.append(graph_read.gin_count_wanted(meta["two_phase"]))
                    forms.append(meta["graph_form"].get("count_gin"))
                    totals.append(mcp._okg_bounded_operator_count(
                        conn, from_where_sql=meta["count_from_where_sql"],
                        params=meta["params"], date_budget=meta.get("date_budget"),
                    ))
    finally:
        monkeypatch.setattr(psycopg.Cursor, "execute", real)
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()
    # The two limits fall on either side of the threshold ...
    assert wanted == [True, False], wanted
    # ... and the rerun kept the first run's form and its count.
    assert forms == [True, True], forms
    assert totals == [(3, "exact")] * 2, totals
    assert counts == [False], counts
