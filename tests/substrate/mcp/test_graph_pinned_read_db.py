"""Pinned reads on the graph path (PostgreSQL).

PACT lane-s-storage-redesign-s2, design section 5.3 and requirement
``pinned-parity``; since PACT lane-s-storage-cutover the graph path is the
only read path. A read pinned to N-1 or N-2 reads the Chronos rows at that
pin's branch point plus that generation's overlays, through the real tools
(filter, map, aggregate, expand, a retained reference) and the search
supplement, and equals a Python recompute, including a pin below the old
materialized interval retention window. A pin without its overlay marker, or
one retired by history compaction, is refused by name, never answered from
graph values. Declared-index narrowing stays head-only
(``pinned_generation``); a pinned read is not narrowed.
"""
from __future__ import annotations

import uuid
from collections import Counter
from collections.abc import Iterator
from typing import Any

import psycopg
import pytest

from okg.substrate.attributes import OVERLAY_GENERATION_MARKER_TABLE
from okg.substrate.errors import GenerationCompacted, SubstrateError
from okg.substrate.mcp import open_session, result_references
from okg.substrate.mcp import server as server_mod
from okg.substrate.mcp.result_references import ResultReferenceStore
from tests.substrate.mcp.test_graph_read_path_db import _recompute, _World
from tests.substrate.mcp.test_graph_search_supplement_db import (
    _TextWorld,
    _recompute as _supplement_recompute,
    _supplement,
)

pytestmark = pytest.mark.postgres

DOC = {"kind": "nodes", "subtype": "doc"}
ALICE = {"op": "eq", "field": "attrs.owner", "value": "alice"}


@pytest.fixture
def world(clean_runtime: str) -> Iterator[_World]:
    previous = result_references.install_store(ResultReferenceStore())
    try:
        yield _World(clean_runtime)
    finally:
        result_references.install_store(previous)


class _Server:
    def __init__(self, world: _World, monkeypatch, mode: str) -> None:
        assert mode == "graph", mode
        self.mcp = server_mod.build_server(dsn=world.dsn, deployment_name="default")

    def __enter__(self) -> "_Server":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.mcp._okg_pool.close()
        if self.mcp._okg_write_pool:
            self.mcp._okg_write_pool.close()

    def __call__(self, tool: str, **arguments: Any) -> dict:
        return self.mcp._tool_manager._tools[tool].fn(**arguments)


def _reads(server: _Server, gen: str, anchor: str) -> dict[str, Any]:
    """Each read pinned to ``gen``, reduced to what the recompute decides."""
    filtered = server("filter", input=DOC, predicate=ALICE, generation_id=gen,
                      limit=50, select=["node_id", "attrs.owner"])
    mapped = server("map", input={**DOC, "filter": ALICE},
                    projection=["node_id", "attrs.owner"], generation_id=gen, limit=50)
    grouped = server("aggregate", input=DOC, by=["attrs.owner"], metrics=["count"],
                     generation_id=gen)
    expanded = server("expand", input={"node_id": anchor},
                      filter={"op": "exists", "field": "attrs.rank"},
                      generation_id=gen)
    first = server("filter", input=DOC, predicate=ALICE, generation_id=gen,
                   limit=2, select=["node_id"])
    return {
        "filter": [i["node_id"] for i in filtered["items"]],
        "map": [i["node_id"] for i in mapped["items"]],
        "aggregate": sorted(
            (repr(g.get("key", g)), g.get("count")) for g in grouped["groups"]
        ),
        "expand": [str(n.get("edge_id") or n.get("node_id")) for n in expanded["neighbors"]],
        "first_page": [i["node_id"] for i in first["items"]],
    }


def _expected(world: _World, gen: str) -> dict[str, Any]:
    alice = [r["id"] for r in _recompute(world, gen, "nodes", {"op": "and", "clauses": [
        {"op": "eq", "field": "subtype", "value": "doc"}, ALICE]})]
    owners = Counter(
        repr(r["attrs"].get("owner")) for r in _recompute(
            world, gen, "nodes", {"op": "eq", "field": "subtype", "value": "doc"})
    )
    return {"filter": alice, "map": alice,
            "first_page": alice[:2], "owner_counts": owners}


def test_pinned_reads_equal_the_recompute(
    world: _World, monkeypatch,
) -> None:
    """Every pin, the oldest (N-2) included: the materialized read refused
    it, below its interval retention window (S2 design 5.3)."""
    anchor = world.ids["a"]
    for depth, gen in zip(("N-2", "N-1", "N"), world.gens):
        with _Server(world, monkeypatch, "graph") as graph_server:
            graph = _reads(graph_server, gen, anchor)
        expected = _expected(world, gen)
        for read in ("filter", "map", "first_page"):
            assert graph[read] == expected[read], (depth, read)
        assert sum(n for _key, n in graph["aggregate"]) == sum(
            expected["owner_counts"].values()), depth
        assert len(graph["expand"]) == 1, depth  # a -> b, b has a rank


def test_a_continuation_page_after_a_publish_reads_its_pin(
    world: _World, monkeypatch,
) -> None:
    pinned = world.gens[1]
    expected = _expected(world, pinned)["filter"]
    with _Server(world, monkeypatch, "graph") as server:
        first = server("filter", input=DOC, predicate=ALICE, generation_id=pinned,
                       limit=2, select=["node_id"])
        world.overlay(world.gens[-1], "node", world.ids["b"], "owner", "alice")
        world.gens.append(world.publish())  # a later publish moves the head
        second = server("filter", input=DOC, predicate=ALICE, generation_id=pinned,
                        limit=2, offset=2, select=["node_id"])
    assert [i["node_id"] for i in first["items"]] == expected[:2]
    assert [i["node_id"] for i in second["items"]] == expected[2:4]
    assert world.ids["b"] not in [i["node_id"] for i in second["items"]]


def test_a_pin_without_its_overlay_marker_is_refused_by_every_read(
    world: _World, monkeypatch,
) -> None:
    gen = world.gens[1]
    with psycopg.connect(world.dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            f"DELETE FROM {OVERLAY_GENERATION_MARKER_TABLE} WHERE generation_id = %s", (gen,),
        )
    calls = {
        "filter": dict(input=DOC, predicate=ALICE, generation_id=gen, limit=5),
        "map": dict(input={**DOC, "filter": ALICE}, projection=["node_id"],
                    generation_id=gen, limit=5),
        "aggregate": dict(input=DOC, by=["attrs.owner"], metrics=["count"],
                          generation_id=gen),
        # A filtered expand reads effective attributes (the unfiltered one-hop
        # read is a Chronos read outside the switch).
        "expand": dict(input={"node_id": world.ids["a"]},
                       filter={"op": "exists", "field": "attrs.path"}, generation_id=gen),
    }
    with _Server(world, monkeypatch, "graph") as server:
        for tool, arguments in calls.items():
            try:
                answered = server(tool, **arguments)
            except SubstrateError as exc:
                assert exc.code == "overlay_generation_unmaterialized", tool
            else:
                raise AssertionError(f"{tool} answered without a marker: {answered!r}"[:2000])


@pytest.fixture
def text_world(clean_runtime: str) -> _TextWorld:
    built = _TextWorld(clean_runtime)
    built.gens.append(built.publish())  # g0 = N-2, g1 = N-1, g2 = N
    return built


def test_the_search_supplement_pinned_to_older_generations(
    text_world: _TextWorld, monkeypatch,
) -> None:
    for gen in text_world.gens:
        rows = _supplement(text_world, monkeypatch, "graph", gen,
                           {"owner": "alice"}, "alpha")
        assert [str(r["node_id"]) for r in rows] == _supplement_recompute(
            text_world, gen, {"owner": "alice"}, "alpha"), gen


def test_a_compacted_pin_is_refused_by_the_lease_less_direct_caller(
    text_world: _TextWorld, monkeypatch,
) -> None:
    """The direct search caller holds no read lease, so the graph path checks
    compaction itself before its first statement."""
    from okg.substrate.bm25_node_search import branch_point
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.mcp.semantic_readiness import resolve_node_segment
    from tests.substrate.mcp.test_graph_search_supplement_db import search_mod

    gen = text_world.gens[0]
    with psycopg.connect(text_world.dsn, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default", requested_generation=gen)
        segment, bp = resolve_node_segment(conn), branch_point(conn, session)
        checkpoint = ChronosGraphStore._generation_checkpoint_id(gen)
        branch = conn.execute(
            "SELECT branch_id FROM _chronos_branch_interval_checkpoints "
            "WHERE checkpoint_id = %s", (checkpoint,),
        ).fetchone()[0]
        plan_id = str(uuid.uuid4())
        with conn.transaction():
            conn.execute(
                "INSERT INTO okg.history_compaction_plans "
                "(plan_id, deployment_name, chronos_branch_id, status, plan) "
                "VALUES (%s, 'default', %s, 'applying', '{}'::jsonb)",
                (plan_id, branch),
            )
            conn.execute(
                "INSERT INTO okg.compacted_generations "
                "(checkpoint_id, deployment_name, branch_id, plan_id) "
                "VALUES (%s, 'default', %s, %s)",
                (checkpoint, branch, plan_id),
            )
        with pytest.raises(GenerationCompacted):
            search_mod._effective_view_lexical_candidates(
                conn, session, query="alpha",
                filter={"subtype": "doc", "attr_filters": {"owner": "alice"}},
                segment=segment, branch_point_value=bp, exclude_node_ids=set(),
            )


def test_a_retained_search_reference_is_read_at_older_pins(
    text_world: _TextWorld, monkeypatch,
) -> None:
    """search(retain=true) at N-2, N-1 and N, then the reference read
    through map and filter, all on the graph path: the members come back in
    their frozen order and the reference filter equals the recompute of the
    members at that pin."""
    previous = result_references.install_store(ResultReferenceStore())
    try:
        mcp = server_mod.build_server(dsn=text_world.dsn, deployment_name="default")
        try:
            tools = mcp._tool_manager._tools
            for gen in text_world.gens:
                page = tools["search"].fn(query="alpha", retain=True, generation_id=gen,
                                          limit=20)
                ref = page["result_reference"]["id"]
                members = [str(r["node_id"]) for r in page["results"]]
                assert members, gen
                read = tools["map"].fn(input={"ref": ref}, projection=["node_id"],
                                       generation_id=gen, limit=50)
                assert [i["node_id"] for i in read["items"]] == members, gen
                owned = tools["filter"].fn(input={"ref": ref}, predicate=ALICE,
                                           generation_id=gen, limit=50,
                                           select=["node_id"])
                alice = set(_supplement_recompute(text_world, gen, {"owner": "alice"}, ""))
                assert sorted(i["node_id"] for i in owned["items"]) == sorted(
                    m for m in members if m in alice), gen
        finally:
            mcp._okg_pool.close()
            if mcp._okg_write_pool:
                mcp._okg_write_pool.close()
    finally:
        result_references.install_store(previous)


def test_unfiltered_expand_shapes_refuse_an_unmarked_pin_too(
    world: _World, monkeypatch,
) -> None:
    """The one-hop, depth and path expands merge overlays from the same
    per-generation rows; they refuse the pins the filtered read refuses
    (design 2.6), and answer when the marker is there."""
    gen = world.gens[1]
    shapes = {
        # A 1-hop row is the linked record since issue #3666; a select of
        # link fields still asks for link rows.
        "one-hop": dict(input={"node_id": world.ids["a"]}),
        "one-hop-links": dict(input={"node_id": world.ids["a"]},
                              select=["edge_id", "src", "dst"]),
        "depth": dict(input={"node_id": world.ids["a"]}, depth=2),
    }
    with _Server(world, monkeypatch, "graph") as server:
        for name, arguments in shapes.items():
            server("expand", generation_id=gen, **arguments)  # answers: marker present
    with psycopg.connect(world.dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            f"DELETE FROM {OVERLAY_GENERATION_MARKER_TABLE} WHERE generation_id = %s", (gen,),
        )
    with _Server(world, monkeypatch, "graph") as server:
        for name, arguments in shapes.items():
            with pytest.raises(SubstrateError) as exc_info:
                server("expand", generation_id=gen, **arguments)
            assert exc_info.value.code == "overlay_generation_unmaterialized", name
