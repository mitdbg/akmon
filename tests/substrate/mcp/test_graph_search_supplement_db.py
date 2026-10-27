"""The search lexical supplement on the graph read path (PostgreSQL).

PACT lane-s-storage-redesign-s2, design section 2.7 and requirements
``read-multi-key``, ``gin`` and ``gate-rows-equal``. The supplement (the
``attr_filters`` + text read search unions into its lexical candidates)
reads the graph path, the only read path since PACT lane-s-storage-cutover,
for MCP and direct callers alike, and is compared with a Python recompute
from the Chronos rows at the pin plus that generation's overlays.

Fixture: one publish (g0) whose overlays are recorded and materialized after
it (the post-publish case of design 1.5, which the retired materialized
supplement read stale), then a second publish (g1) that carries them.
"""
from __future__ import annotations

import importlib
import uuid
from typing import Any

import psycopg
import pytest

from okg.substrate import declared_attr_indexes as dai
from okg.substrate.attributes import (
    ATTRIBUTE_EFFECTIVE_TABLE,
    OVERLAY_GENERATION_MARKER_TABLE,
    RESERVED_EFFECTIVE_ATTR_KEYS,
    materialize_effective_attributes_for_generation,
    record_attribute_observation,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.mcp import open_session
from okg.substrate.mcp import graph_read
from okg.substrate.mcp import server as server_mod
from okg.substrate.mcp.semantic_readiness import resolve_node_segment
from okg.substrate.bm25_node_search import branch_point
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import (
    SubtypeSpec,
    bind_test_catalog_writer,
    seed_catalog,
)
from tests.substrate.mcp.test_graph_read_path_db import _json_eq

pytestmark = pytest.mark.postgres

search_mod = importlib.import_module("okg.substrate.mcp.search")


class _TextWorld:
    def __init__(self, dsn: str, declared: dict[str, str] | None = None) -> None:
        self.dsn = dsn
        s = uuid.uuid4().hex[:6]
        self.ids = {k: f"doc:{k}:{s}" for k in "abcdefgh"}
        graph_attrs = {
            "a": {"text": "alpha one", "owner": "alice", "tag": "x",
                  "labels": ["red", "blue"], "meta": {"k": 1, "j": 2}},
            "b": {"text": "alpha two", "owner": "bob", "tag": "x"},
            "c": {"text": "alpha three", "owner": "alice", "labels": "red"},
            "d": {"text": "beta", "owner": "alice", "tag": "x"},
            "e": {"owner": "alice", "tag": "x"},
            "f": {"text": "alpha five", "owner": "carol", "tag": "y"},
            "g": {"text": "Alpha six", "owner": "alice", "tag": "x", "rank": 1},
            "h": {"text": "alpha eight", "owner": "alice", "meta": {"k": [1]}},
        }
        self.clock = f"clock:{s}"
        self.tick = 0
        with psycopg.connect(dsn, autocommit=True) as conn:
            seed_catalog(conn, subtypes=[
                SubtypeSpec(name="doc", metadata=(
                    {"filterable_attrs": declared} if declared else None)),
                SubtypeSpec(name="clock"),
            ])
            ChronosGraphStore.from_connection(conn).apply_facts(
                [NodeFact(self.ids[k], "doc", v, {"id": self.ids[k]}, {"v": 1})
                 for k, v in graph_attrs.items()],
                source="default", strict_endpoints=True,
            )
        self.gens = [self.publish()]
        g0 = self.gens[0]
        # Recorded after g0's publish (design 1.5: the retired materialized
        # supplement read them stale at g0); g1's publish carries them.
        for key, attr, value in (
            ("b", "owner", "alice"),        # enters an owner filter
            ("a", "owner", "zed"),          # leaves it
            ("d", "text", "alpha four"),    # enters a text match
            ("e", "text", "alpha seven"),   # gains text
            ("f", "tag", "x"),              # enters a two-key filter's second key
            ("g", "rank", 2),               # a key no filter reads: still merged
        ):
            self.overlay(g0, self.ids[key], attr, value)
        with psycopg.connect(dsn, autocommit=True) as conn:
            materialize_effective_attributes_for_generation(conn, generation_id=g0)
        self.gens.append(self.publish())
        if declared:
            report = dai.reconcile(dsn, deployment="default", apply=True, enable=True)
            assert report["status"] == "converged", report

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

    def overlay(self, gen: str, target: str, key: str, value: Any) -> None:
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            record_attribute_observation(
                conn, target_kind="node", target_id=target, generation_id=gen,
                attr_key=key, value=value, producer="test:graph-supplement",
            )


@pytest.fixture
def world(clean_runtime: str) -> _TextWorld:
    return _TextWorld(clean_runtime)


@pytest.fixture
def declared_world(clean_runtime: str, monkeypatch: pytest.MonkeyPatch) -> _TextWorld:
    """The same world with ``owner`` and ``tag`` declared and their
    expression indexes built (the reconciler's ``ready``)."""
    monkeypatch.delenv("OKG_ATTR_FILTER_INDEX", raising=False)
    return _TextWorld(clean_runtime, declared={"owner": "text", "tag": "text"})


# --------------------------------------------------------------------------
# Independent recompute
# --------------------------------------------------------------------------


def _contains(a: Any, b: Any) -> bool:
    """PostgreSQL jsonb containment one level below the top (no raw-scalar
    exception): same type; objects key by key; each element of an array in
    some element of the other; scalars equal."""
    if isinstance(b, dict):
        return isinstance(a, dict) and all(k in a and _contains(a[k], v) for k, v in b.items())
    if isinstance(b, list):
        return isinstance(a, list) and all(any(_contains(x, y) for x in a) for y in b)
    if isinstance(a, (dict, list)):
        return False
    return _json_eq(a, b)


def _recompute(world: _TextWorld, gen: str, attr_filters: dict, query: str, *,
               overlays: bool = True, exclude: set[str] = frozenset()) -> list[str]:
    with psycopg.connect(world.dsn, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default", requested_generation=gen)
        bp = branch_point(conn, session)
        segment = resolve_node_segment(conn)
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT node_id, attrs FROM {segment} WHERE subtype = 'doc' "
                "AND deleted = FALSE AND live_lo <= %(bp)s AND %(bp)s < live_hi",
                {"bp": bp},
            )
            rows = {str(i): dict(a) for i, a in cur.fetchall()}
            if overlays:
                cur.execute(
                    f"SELECT target_id, attr_key, value FROM {ATTRIBUTE_EFFECTIVE_TABLE} "
                    "WHERE generation_id = %s AND target_kind = 'node' "
                    "AND namespace = 'default'",
                    (gen,),
                )
                for target, key, value in cur.fetchall():
                    if key not in RESERVED_EFFECTIVE_ATTR_KEYS and target in rows:
                        rows[target][key] = value
            terms = [t.lower() for t in query.split()]
            matched = [
                i for i, attrs in rows.items()
                if i not in exclude and "text" in attrs
                and _contains(attrs, attr_filters)
                and all(t in str(attrs["text"]).lower() for t in terms)
            ]
            cur.execute("SELECT x FROM unnest(%s::text[]) AS x ORDER BY x", (matched,))
            return [r[0] for r in cur.fetchall()]


def _attrs_recompute(world: _TextWorld, gen: str, node_id: str) -> dict:
    with psycopg.connect(world.dsn, autocommit=True) as conn, conn.cursor() as cur:
        session = open_session(conn, deployment_name="default", requested_generation=gen)
        cur.execute(
            f"SELECT attrs FROM {resolve_node_segment(conn)} WHERE node_id = %(id)s "
            "AND deleted = FALSE AND live_lo <= %(bp)s AND %(bp)s < live_hi",
            {"id": node_id, "bp": branch_point(conn, session)},
        )
        attrs = dict(cur.fetchone()[0])
        cur.execute(
            f"SELECT attr_key, value FROM {ATTRIBUTE_EFFECTIVE_TABLE} WHERE generation_id = %s "
            "AND target_kind = 'node' AND target_id = %s AND namespace = 'default'",
            (gen, node_id),
        )
        for key, value in cur.fetchall():
            if key not in RESERVED_EFFECTIVE_ATTR_KEYS:
                attrs[key] = value
    return attrs


# --------------------------------------------------------------------------
# The supplement
# --------------------------------------------------------------------------


def _supplement(world: _TextWorld, monkeypatch, mode: str, gen: str, attr_filters: dict,
                query: str, *, exclude: set[str] = frozenset()) -> list[dict]:
    """The supplement as search reads it, for MCP and direct callers alike.
    ``mode`` is kept for the callers' symmetry: the graph path is the only
    one."""
    assert mode == "graph", mode
    with psycopg.connect(world.dsn, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default",
                               requested_generation=gen)
        return search_mod._effective_view_lexical_candidates(
            conn, session, query=query,
            filter={"subtype": "doc", "attr_filters": attr_filters},
            segment=resolve_node_segment(conn),
            branch_point_value=branch_point(conn, session),
            exclude_node_ids=set(exclude),
        )


def _ids(rows: list[dict]) -> list[str]:
    return [str(r["node_id"]) for r in rows]


CASES = [
    pytest.param({"owner": "alice"}, "alpha", id="one-key"),
    pytest.param({"owner": "alice", "tag": "x"}, "alpha", id="two-keys"),
    pytest.param({"labels": ["red"]}, "alpha", id="array-in-array"),
    pytest.param({"labels": "red"}, "alpha", id="scalar-not-in-array"),
    pytest.param({"meta": {"k": 1}}, "alpha", id="nested-object"),
    pytest.param({"tag": "x"}, "ALPHA six", id="two-terms-case"),
    pytest.param({"owner": "nobody"}, "alpha", id="no-match"),
]


@pytest.mark.parametrize("attr_filters,query", CASES)
def test_graph_supplement_equals_the_recompute(
    world: _TextWorld, monkeypatch, attr_filters: dict, query: str,
) -> None:
    for gen in world.gens:
        expected = _recompute(world, gen, attr_filters, query)
        graph = _supplement(world, monkeypatch, "graph", gen, attr_filters, query)
        assert _ids(graph) == expected, (gen, attr_filters, query)


def test_array_value_against_a_scalar_is_object_containment(
    world: _TextWorld, monkeypatch,
) -> None:
    """``{"labels": ["red", "blue"]}`` does not contain ``{"labels": "red"}``
    (object containment allows an array to hold a scalar only at the top), so
    the per-key form must not either; a scalar ``"red"`` does."""
    g1 = world.gens[1]
    assert _ids(_supplement(world, monkeypatch, "graph", g1, {"labels": "red"}, "alpha")) \
        == [world.ids["c"]]
    assert _ids(_supplement(world, monkeypatch, "graph", g1, {"labels": ["red"]}, "alpha")) \
        == [world.ids["a"]]
    assert _ids(_supplement(world, monkeypatch, "graph", g1, {"meta": {"k": 1}}, "alpha")) \
        == [world.ids["a"]]


def test_rows_carry_the_merged_attrs(world: _TextWorld, monkeypatch) -> None:
    g1 = world.gens[1]
    rows = _supplement(world, monkeypatch, "graph", g1, {"tag": "x"}, "six")
    assert _ids(rows) == [world.ids["g"]]
    assert rows[0]["attrs"] == _attrs_recompute(world, g1, world.ids["g"])
    assert rows[0]["attrs"]["rank"] == 2  # overlay on a key no filter reads


def test_exclusions_hold_inside_the_graph_read(world: _TextWorld, monkeypatch) -> None:
    g1 = world.gens[1]
    exclude = {world.ids["b"], world.ids["g"]}
    expected = _recompute(world, g1, {"owner": "alice"}, "alpha", exclude=exclude)
    rows = _supplement(world, monkeypatch, "graph", g1, {"owner": "alice"}, "alpha",
                       exclude=exclude)
    assert _ids(rows) == expected


def test_post_publish_overlays_are_seen(
    world: _TextWorld, monkeypatch,
) -> None:
    """Design 1.5: at g0 the overlays were recorded after publish (the
    retired materialized supplement still read g0's graph values there);
    the graph path equals the recompute with them, not without them."""
    g0 = world.gens[0]
    graph = _ids(_supplement(world, monkeypatch, "graph", g0, {"owner": "alice"}, "alpha"))
    assert graph == _recompute(world, g0, {"owner": "alice"}, "alpha")
    assert world.ids["b"] in graph and world.ids["a"] not in graph
    assert graph != _recompute(world, g0, {"owner": "alice"}, "alpha", overlays=False)


def test_a_generation_without_its_marker_is_refused_not_answered(
    world: _TextWorld, monkeypatch,
) -> None:
    g1 = world.gens[1]
    with psycopg.connect(world.dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            f"DELETE FROM {OVERLAY_GENERATION_MARKER_TABLE} WHERE generation_id = %s", (g1,),
        )
    with pytest.raises(SubstrateError) as exc_info:
        _supplement(world, monkeypatch, "graph", g1, {"owner": "alice"}, "alpha")
    assert exc_info.value.code == "overlay_generation_unmaterialized"


def test_the_graph_statement_reads_no_gin_containment_and_runs_unprepared(
    world: _TextWorld, monkeypatch,
) -> None:
    """Design 2.7 keeps the GIN index out: the graph branch tests each key
    (``attrs -> key``), never ``n.attrs @> ...``; every statement is sent
    unprepared."""
    seen: list[tuple[str, Any]] = []
    real = search_mod.execute_with_custom_plan

    def capture(cur: Any, sql: Any, params: Any = None) -> Any:
        seen.append((str(sql), params))
        return real(cur, sql, params)

    monkeypatch.setattr(search_mod, "execute_with_custom_plan", capture)
    rows = _supplement(world, monkeypatch, "graph", world.gens[1],
                       {"owner": "alice", "tag": "x"}, "alpha")
    assert rows
    [(sql, params)] = seen
    assert "n.attrs @>" not in sql
    assert "m.attrs @> %(attr_filters)s" in sql
    assert sorted(params["graph_overlay_keys"]) == ["owner", "tag", "text"]


def test_containment_compiles_through_the_one_compiler() -> None:
    params: dict[str, Any] = {"sc0": "taken"}
    sql = graph_read.containment_sql({"a": [1], "b": {"c": "d"}}, params, "n.attrs")
    assert sql.count("jsonb_typeof") == 4 and sql.count("@>") == 2
    assert "sc0" in params and params["sc0"] == "taken"
    assert {v for k, v in params.items() if k != "sc0"} == {"a", "b", "[1]", '{"c": "d"}'}
    with pytest.raises(ValueError):
        graph_read.containment_sql({"a": float("nan")}, {}, "n.attrs")


def test_retain_under_the_graph_path_needs_only_the_marker(
    world: _TextWorld, monkeypatch,
) -> None:
    """``search(retain=true)`` checks the overlay marker on the graph path (its
    consumers read the graph path), and is refused by name without it."""
    g1 = world.gens[1]
    mcp = server_mod.build_server(dsn=world.dsn, deployment_name="default")
    try:
        search_tool = mcp._tool_manager._tools["search"].fn
        retained = search_tool(query="alpha", retain=True, generation_id=g1)
        assert retained.get("status") != "error", retained
        with psycopg.connect(world.dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {OVERLAY_GENERATION_MARKER_TABLE} WHERE generation_id = %s",
                (g1,),
            )
        with pytest.raises(SubstrateError) as exc_info:
            search_tool(query="alpha", retain=True, generation_id=g1)
        assert exc_info.value.code == "overlay_generation_unmaterialized"
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


# --------------------------------------------------------------------------
# Statement shape: what drives the graph branch
# --------------------------------------------------------------------------


def _graph_statement(world: _TextWorld, monkeypatch, attr_filters: dict,
                     query: str) -> tuple[str, Any]:
    seen: list[tuple[str, Any]] = []
    real = search_mod.execute_with_custom_plan

    def capture(cur: Any, sql: Any, params: Any = None) -> Any:
        seen.append((str(sql), params))
        return real(cur, sql, params)

    monkeypatch.setattr(search_mod, "execute_with_custom_plan", capture)
    _supplement(world, monkeypatch, "graph", world.gens[1], attr_filters, query)
    [statement] = seen
    return statement


def _index_names(world: _TextWorld, sql: str, params: Any) -> set[str]:
    """The indexes the plan reads with sequential scans priced out (test
    only: it shows which indexes CAN drive the statement)."""
    def walk(node: dict) -> set[str]:
        names = {node["Index Name"]} if "Index Name" in node else set()
        for child in node.get("Plans", []):
            names |= walk(child)
        return names

    with psycopg.connect(world.dsn) as conn, conn.cursor() as cur:
        cur.execute("SET LOCAL enable_seqscan = off")
        cur.execute("EXPLAIN (FORMAT JSON) " + sql, params, prepare=False)
        return walk(cur.fetchone()[0][0]["Plan"])


def test_the_text_trigram_index_never_drives_the_graph_branch(
    world: _TextWorld, monkeypatch,
) -> None:
    """The trigram index holds every row version: a common term rechecked
    643,388 of them for 1,699 visible rows at Krama (11.1 s). The graph
    branch tests the terms only on rows that passed every other clause (an
    ``OFFSET 0`` subquery), so even with sequential scans priced out the
    index is not in the plan."""
    with psycopg.connect(world.dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT indexname FROM pg_indexes "
                    "WHERE indexname ~ 'node_text_trgm_visible$'")
        trigram = {r[0] for r in cur.fetchall()}
    assert trigram, "the world has no text trigram index: the check would be vacuous"
    sql, params = _graph_statement(world, monkeypatch, {"owner": "alice"}, "alpha")
    assert "OFFSET 0) n WHERE ((n.attrs->>'text') ILIKE %(term_0)s)" in sql
    assert not (_index_names(world, sql, params) & trigram)


def test_a_declared_filter_key_drives_the_graph_branch(
    declared_world: _TextWorld, monkeypatch,
) -> None:
    """With the filter's key declared and its index ready, the graph branch
    reads the declared index's candidates, as the filter tool does, and the
    rows equal the recompute."""
    world = declared_world
    sql, params = _graph_statement(world, monkeypatch, {"owner": "alice"}, "alpha")
    assert "n.node_id IN ((SELECT g.node_id AS target_id" in sql
    assert params["sdai_bp"] == params["bp"]
    assert any(name.startswith("okg_dattr_")
               for name in _index_names(world, sql, params))
    monkeypatch.setenv("OKG_ATTR_FILTER_INDEX", "off")
    sql_off, params_off = _graph_statement(world, monkeypatch, {"owner": "alice"}, "alpha")
    assert "sdai_bp" not in params_off and "SELECT g.node_id" not in sql_off
    monkeypatch.delenv("OKG_ATTR_FILTER_INDEX")


@pytest.mark.parametrize("attr_filters,query", CASES)
def test_declared_candidates_keep_the_rows(
    declared_world: _TextWorld, monkeypatch, attr_filters: dict, query: str,
) -> None:
    world = declared_world
    for gen in world.gens:
        expected = _recompute(world, gen, attr_filters, query)
        assert _ids(_supplement(world, monkeypatch, "graph", gen, attr_filters, query)) \
            == expected, (gen, attr_filters, query)


def test_a_pinned_read_probes_the_declared_index_at_its_own_branch_point(
    declared_world: _TextWorld, monkeypatch,
) -> None:
    """Below the head the graph branch still reads the declared index, at the
    pin's own branch point (attr_filter_source ``at_branch_point``). ``c``'s
    graph value of ``owner`` changes in g2, so a probe at the head's branch
    point would drop ``c`` from the g1 read and admit it nowhere; the pinned
    rows equal the recompute at g1."""
    world = declared_world
    with psycopg.connect(world.dsn, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [NodeFact(world.ids["c"], "doc",
                      {"text": "alpha three", "owner": "bob", "labels": "red"},
                      {"id": world.ids["c"]}, {"v": 2})],
            source="default", strict_endpoints=True,
        )
    world.gens.append(world.publish())
    g1, g2 = world.gens[1], world.gens[2]
    filters = {"owner": "alice"}
    expected_g1 = _recompute(world, g1, filters, "alpha")
    assert world.ids["c"] in expected_g1
    assert world.ids["c"] not in _recompute(world, g2, filters, "alpha")

    seen: list[tuple[str, Any]] = []
    real = search_mod.execute_with_custom_plan

    def capture(cur: Any, sql: Any, params: Any = None) -> Any:
        seen.append((str(sql), params))
        return real(cur, sql, params)

    monkeypatch.setattr(search_mod, "execute_with_custom_plan", capture)
    graph = _supplement(world, monkeypatch, "graph", g1, filters, "alpha")
    monkeypatch.setattr(search_mod, "execute_with_custom_plan", real)
    [(sql, params)] = seen
    assert "n.node_id IN ((SELECT g.node_id AS target_id" in sql
    assert params["sdai_bp"] == params["bp"]
    with psycopg.connect(world.dsn, autocommit=True) as conn:
        head = branch_point(conn, open_session(
            conn, deployment_name="default", requested_generation=g2))
    assert params["sdai_bp"] != head
    assert _ids(graph) == expected_g1
    for gen in world.gens:
        for attr_filters, query in ((filters, "alpha"), ({"owner": "alice", "tag": "x"},
                                                          "alpha")):
            assert _ids(_supplement(world, monkeypatch, "graph", gen, attr_filters,
                                    query)) == _recompute(world, gen, attr_filters, query)
