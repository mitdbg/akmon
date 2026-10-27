"""Planted failures for the S2 parity checks and the graph read path (PostgreSQL).

PACT lane-s-storage-redesign-s2, requirements ``overlay-parity`` and
``gate-rows-equal`` (CI part); PACT lane-s-storage-cutover (the graph path is
the only read path).

* The whole-graph parity checks of ``test_graph_parity_db`` must FAIL when
  the answers really differ: an overlay value only the graph path reads
  differs from the recompute. (PACT lane-s-storage-drop-copies deleted the
  case that planted a stale row in the materialized copy, which is dropped.)
* An error raised by a real graph-path statement fails the tool call; no
  other read path exists to answer it.
"""
from __future__ import annotations

from typing import Any

import psycopg
import pytest

from okg.substrate.attributes import ATTRIBUTE_EFFECTIVE_TABLE
from okg.substrate.mcp import graph_read
from okg.substrate.mcp import server as server_mod
from tests.substrate.mcp import test_graph_parity_db as parity
from tests.substrate.mcp.test_graph_read_path_db import (
    _World,
    _both,
    _page,
    _recompute,
    _scoped,
)

pytestmark = pytest.mark.postgres


@pytest.fixture
def world(clean_runtime: str) -> _World:
    return _World(clean_runtime)


_PLANTED_TABLE = "okg.s2_planted_overlay_copy"


def _owner_at_head(world: _World, monkeypatch, node: str) -> Any:
    """``attrs.owner`` of ``node`` on the head SQL page (before the tool's
    result wrapping)."""
    [row] = _page(world, monkeypatch, "graph", world.gens[-1], "nodes",
                  _scoped({"op": "eq", "field": "node_id", "value": node}, "nodes"))
    return row["attrs"].get("owner")


def _recomputed_owner(world: _World, node: str) -> Any:
    [row] = _recompute(world, world.gens[-1], "nodes",
                       {"op": "eq", "field": "node_id", "value": node})
    return row["attrs"].get("owner")


def test_the_parity_checks_pass_on_an_untouched_graph(world: _World, monkeypatch) -> None:
    """Control for the planted cases below: the same checks, no plant."""
    parity.test_map_over_every_node_and_edge(world, monkeypatch)
    parity.test_every_node_and_edge_through_the_page(world, monkeypatch)


def test_a_planted_differing_overlay_value_fails_the_parity_checks(
    world: _World, monkeypatch,
) -> None:
    """The graph path is pointed at a copy of the per-generation overlay
    table in which a's owner overlay at the head holds a different value.
    (The recompute reads the real per-generation table, so a value changed
    there moves the graph path and the recompute together; only an overlay
    the graph path alone reads can make them differ.) The graph page
    returns the planted value, the recompute does not, and both
    whole-graph parity checks (page and map) fail."""
    head, a = world.gens[-1], world.ids["a"]
    with psycopg.connect(world.dsn, autocommit=True) as conn:
        conn.execute(f"CREATE TABLE {_PLANTED_TABLE} "
                     f"(LIKE {ATTRIBUTE_EFFECTIVE_TABLE} INCLUDING ALL)")
        conn.execute(f"INSERT INTO {_PLANTED_TABLE} SELECT * FROM {ATTRIBUTE_EFFECTIVE_TABLE}")
        conn.execute(f"GRANT SELECT ON {_PLANTED_TABLE} TO okg_mcp")  # the tools' read role
        changed = conn.execute(
            f"UPDATE {_PLANTED_TABLE} SET value = '\"planted\"'::jsonb "
            "WHERE generation_id = %s AND target_id = %s AND attr_key = 'owner'",
            (head, a),
        ).rowcount
    assert changed == 1
    monkeypatch.setattr(graph_read, "ATTRIBUTE_EFFECTIVE_TABLE", _PLANTED_TABLE)
    assert _owner_at_head(world, monkeypatch, a) == "planted"
    assert _recomputed_owner(world, a) == "alice"
    with pytest.raises(AssertionError):
        parity.test_every_node_and_edge_through_the_page(world, monkeypatch)
    with pytest.raises(AssertionError):
        parity.test_map_over_every_node_and_edge(world, monkeypatch)


_MISSING_TABLE = "okg.s2_planted_missing_overlay_table"


@pytest.mark.parametrize("tool", ["filter", "map"])
def test_an_error_inside_a_real_graph_read_fails_the_call(
    world: _World, monkeypatch, tool: str,
) -> None:
    """The graph path's overlay table name is pointed at a table that does
    not exist, so the graph statement fails in PostgreSQL, and the tool
    call fails with that error. Control: the same call with the real name
    answers."""
    arguments: dict[str, Any] = {
        "filter": dict(input={"kind": "nodes", "subtype": "doc"},
                       predicate={"op": "eq", "field": "attrs.owner", "value": "alice"},
                       generation_id=world.gens[-1], limit=10),
        "map": dict(input={"kind": "nodes", "subtype": "doc"},
                    projection=["node_id", "attrs.owner"],
                    generation_id=world.gens[-1], limit=10),
    }[tool]

    def call() -> Any:
        mcp = server_mod.build_server(dsn=world.dsn, deployment_name="default")
        try:
            return mcp._tool_manager._tools[tool].fn(**arguments)
        except Exception as exc:  # noqa: BLE001 - the outcome under test
            return exc
        finally:
            mcp._okg_pool.close()
            if mcp._okg_write_pool:
                mcp._okg_write_pool.close()

    served = call()
    assert isinstance(served, dict) and served["items"], served
    monkeypatch.setattr(graph_read, "ATTRIBUTE_EFFECTIVE_TABLE", _MISSING_TABLE)
    failed = call()
    assert isinstance(failed, Exception), (
        f"graph read with a failing statement answered: {failed!r}")
    chain, seen = [], failed
    while seen is not None:
        chain.append(f"{type(seen).__name__}: {seen}")
        seen = seen.__cause__ or seen.__context__
    assert any(_MISSING_TABLE.split(".")[1] in text for text in chain), chain


def test_the_error_case_reads_the_graph_path_when_nothing_is_planted(
    world: _World, monkeypatch,
) -> None:
    """Control: the same filter with the real table name answers the
    recompute's rows."""
    predicate = _scoped({"op": "eq", "field": "attrs.owner", "value": "alice"}, "nodes")
    page = _both(world, monkeypatch, world.gens[-1], "nodes", predicate)["graph"]
    assert page and [r["node_id"] for r in page] == [
        r["id"] for r in _recompute(world, world.gens[-1], "nodes", predicate)]
