"""The byte cut does not move with the outcome of the total count (real `filter`).

PACT ``mcp-byte-fit-count-stable``. A `filter` page near its byte budget is
served as the same rows, with the same `next_offset`, whether the bounded
COUNT finished exactly, hit its row cap (lower bound) or timed out
(unknown). On dev the three outcomes render different envelope bytes
(coverage.selection.size, retrieval status, count fact), so at some budgets
they keep a different number of rows.
"""
from __future__ import annotations

import uuid
from typing import Any, Iterator

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.attributes import (  # noqa: E402
    materialize_effective_attributes_for_generation,
)
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import NodeFact  # noqa: E402
from okg.substrate.mcp import server as server_mod  # noqa: E402
from okg.substrate.mcp.response_bounds import public_call_tool_result_bytes  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from tests.pytest.catalog_fixture_builders import (  # noqa: E402
    bind_test_catalog_writer,
    seed_catalog,
)

pytestmark = pytest.mark.postgres

TOTAL = 100


@pytest.fixture
def fat_world(clean_runtime: str) -> Iterator[dict[str, Any]]:
    sfx = uuid.uuid4().hex[:8]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["item"], narrowings=[])
        facts = [
            NodeFact(
                f"n:{sfx}:{i:03d}", "item",
                {"text": f"{i:03d} " + "x" * 300, "status": "open"},
                {"id": f"n:{sfx}:{i:03d}"}, {"v": 1},
            )
            for i in range(TOTAL)
        ]
        ChronosGraphStore.from_connection(conn).apply_facts(facts, source="default")
    identity = bind_test_catalog_writer(clean_runtime, "default")
    out = Publisher(clean_runtime, "default", catalog_identity=identity).cycle()
    assert out["status"] == "published", out
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        materialize_effective_attributes_for_generation(
            conn, generation_id=str(out["generation_id"]),
        )
    mcp = server_mod.build_server(dsn=clean_runtime)
    try:
        yield {
            "tools": {name: tool.fn for name, tool in mcp._tool_manager._tools.items()},
        }
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


def _serve(world: dict, outcome: str, budget: int, monkeypatch) -> dict[str, Any]:
    """One `filter` call with the count forced to `outcome`."""
    with monkeypatch.context() as patch:
        if outcome == "lower_bound":
            patch.setattr(server_mod, "_OPERATOR_COUNT_CAP", 3)
        elif outcome == "unknown":
            patch.setattr(server_mod, "_OPERATOR_COUNT_TIMEOUT_MS", 20)
            real = psycopg.Cursor.execute

            def slow_count(self, query, params=None, *args, **kwargs):
                if isinstance(query, str) and query.startswith(
                    "SELECT count(*) FROM (SELECT 1 "
                ):
                    query, params = "SELECT count(*) FROM pg_sleep(2)", None
                return real(self, query, params, *args, **kwargs)

            patch.setattr(psycopg.Cursor, "execute", slow_count)
        return world["tools"]["filter"](
            input={"kind": "nodes", "subtype": "item"}, predicate=None,
            order_by="node_id", limit=60, max_bytes=budget,
        )


def test_the_cut_is_the_same_whatever_the_count_outcome(
    fat_world: dict, monkeypatch,
) -> None:
    outcomes = ("exact", "lower_bound", "unknown")
    cut_seen = False
    # The page at the smallest budget shows n rows. Budgets around the size
    # of an n-row page, for each outcome, take in every place the outcomes'
    # different envelopes could move the cut; steps are smaller than the gap.
    first = _serve(fat_world, "exact", 4_096, monkeypatch)
    shown = len(first["items"]) + 1  # the next row up: the cut is at its edge
    sizes = []
    for outcome in outcomes:
        page = _serve(fat_world, outcome, 262_144, monkeypatch)
        sizes.append(public_call_tool_result_bytes(
            {**page, "items": page["items"][:shown]},
        ))
    low, high = max(4_096, min(sizes) - 300), max(sizes) + 300

    def rows_at(budget: int) -> int:
        return len(_serve(fat_world, "exact", budget, monkeypatch)["items"])

    # Where the exact page gains a row, by bisection (the rows only grow with
    # the budget; the check below holds that to the coarse sweep). Every
    # outcome is then served at the budget before, at and after each gain,
    # where an envelope that moved the cut would show, and across the whole
    # range in steps of 99 bytes.
    gains: list[int] = []

    def locate(lo: int, hi: int, rows_lo: int, rows_hi: int) -> None:
        if rows_lo == rows_hi:
            return
        if hi - lo == 1:
            gains.append(hi)
            return
        mid = (lo + hi) // 2
        rows_mid = rows_at(mid)
        locate(lo, mid, rows_lo, rows_mid)
        locate(mid, hi, rows_mid, rows_hi)

    locate(low, high, rows_at(low), rows_at(high))
    budgets = sorted({
        *range(low, high, 99), high,
        *(g + step for g in gains for step in (-1, 0, 1) if low <= g + step <= high),
    })
    exact_rows: list[int] = []
    for budget in budgets:
        pages = {o: _serve(fat_world, o, budget, monkeypatch) for o in outcomes}
        assert pages["exact"]["total_count_basis"] == "exact"
        assert pages["lower_bound"]["total_count_basis"] == "lower_bound"
        assert pages["unknown"]["total_count_basis"] == "unknown"
        rows = {o: [r["node_id"] for r in p["items"]] for o, p in pages.items()}
        cursor = {o: p["next_offset"] for o, p in pages.items()}
        assert rows["exact"] == rows["lower_bound"] == rows["unknown"], (budget, cursor)
        assert cursor["exact"] == cursor["lower_bound"] == cursor["unknown"], budget
        cut_seen = cut_seen or "response_byte_budget" in pages["exact"]
        exact_rows.append(len(rows["exact"]))
    assert exact_rows == sorted(exact_rows), "rows must only grow with the budget"
    assert cut_seen, (
        "no budget in the scan cut the page; make the rows wider", shown, sizes,
    )
