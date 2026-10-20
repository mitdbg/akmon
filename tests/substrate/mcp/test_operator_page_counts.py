"""A truncated filter, map or aggregate page never presents its size as the set
total (issue #3190).

Before this change the MCP tools set `total_count` to the page size (or the
limit) whenever a page was cut, with no basis, so the text result read
"1-1 of 1" over a twelve-row set. `envelope.py` retired that convention for
the set operators (#1440); these are the tool paths it did not reach:
filter and map over attributes (the effective-attribute page), filter and
map without (the lowered program), and aggregate grouped by an attribute.

Each test reads one set whole and then a cut first page, and asserts the cut
page's count is either exact and equal to the whole, or a lower bound no
larger than it, and never "exact" while smaller than the set.
"""

from __future__ import annotations

import uuid
from typing import Any, Iterator

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import NodeFact  # noqa: E402
from okg.substrate.mcp import server as server_mod  # noqa: E402
from okg.substrate.mcp.result_text import render  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from tests.pytest.catalog_fixture_builders import (  # noqa: E402
    bind_test_catalog_writer,
    seed_catalog,
)

pytestmark = pytest.mark.postgres

_SCOPE = {"kind": "nodes", "subtype": "page"}


@pytest.fixture
def tools(clean_runtime: str) -> Iterator[dict[str, Any]]:
    """Six statuses, two nodes each (twelve rows, six groups)."""
    suffix = uuid.uuid4().hex[:8]
    statuses = [f"s{i}-{suffix}" for i in range(6)]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["page"])
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(
                    f"n:{suffix}:{status}:{copy}", "page",
                    {"text": "counted", "status": status, "details": "evidence " * 64},
                    {"id": f"n:{suffix}:{status}:{copy}"}, {"v": 1},
                )
                for status in statuses
                for copy in range(2)
            ],
            source="default",
        )
    identity = bind_test_catalog_writer(clean_runtime, "default")
    out = Publisher(clean_runtime, "default", catalog_identity=identity).cycle()
    assert out["status"] == "published", out
    mcp = server_mod.build_server(dsn=clean_runtime)
    try:
        yield {name: tool.fn for name, tool in mcp._tool_manager._tools.items()}
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


def _assert_cut_page_is_honest(
    whole: dict, cut: dict, *, rows_key: str, tool: str,
) -> None:
    assert "error" not in whole and "error" not in cut, (whole, cut)
    assert whole["truncated"] is False
    assert whole["total_count_basis"] == "exact"
    assert whole["total_count"] == len(whole[rows_key]) > 1
    assert cut["truncated"] is True
    assert whole["coverage"]["delivery"]["complete"] is True
    assert cut["coverage"]["delivery"]["complete"] is False
    # Delivered rows and the cursor are the page's own keys, not repeated.
    assert not {"returned", "next_available"} & set(cut["coverage"]["delivery"])
    assert cut.get("next_offset") is not None or cut.get("continuation_token")
    assert len(cut[rows_key]) == 1
    basis = cut["total_count_basis"]
    assert basis in ("exact", "lower_bound"), cut
    if basis == "exact":
        assert cut["total_count"] == whole["total_count"]
    else:
        assert cut["total_count"] <= whole["total_count"]
    # The text a model reads says the same: "of 6", or "more follow" for a
    # floor that is only this page's rows (#3666 step 2b), never "of 1".
    text = render(cut, tool=tool)
    head = text.split("\n", 1)[0]
    if basis == "exact":
        assert f"of {cut['total_count']}" in head, text
    else:
        assert ", more follow" in head or f"of {cut['total_count']}+" in head, text
        assert f"of {cut['total_count']} " not in head, text


_ATTR_PREDICATE = {"op": "exists", "field": "attrs.status"}
_PLAIN_PREDICATE = {"op": "eq", "field": "subtype", "value": "page"}


@pytest.mark.parametrize("predicate", [_ATTR_PREDICATE, _PLAIN_PREDICATE],
                         ids=["attrs", "plain"])
def test_a_cut_filter_page_does_not_report_its_size_as_the_total(
    tools, predicate,
) -> None:
    def call(limit: int) -> dict:
        return tools["filter"](
            input=_SCOPE, predicate=predicate, limit=limit, select=["node_id"],
        )

    _assert_cut_page_is_honest(call(100), call(1), rows_key="items", tool="filter")


@pytest.mark.parametrize("projection", [["node_id", "attrs.status"], ["node_id"]],
                         ids=["attrs", "plain"])
def test_a_cut_map_page_does_not_report_its_size_as_the_total(
    tools, projection,
) -> None:
    def call(limit: int) -> dict:
        return tools["map"](input=_SCOPE, projection=projection, limit=limit)

    _assert_cut_page_is_honest(call(100), call(1), rows_key="items", tool="map")


def test_a_cut_aggregate_page_reports_the_exact_group_count(tools) -> None:
    def call(limit: int) -> dict:
        return tools["aggregate"](
            input={**_SCOPE, "filter": _ATTR_PREDICATE},
            by=["attrs.status"], metrics=["count"], limit=limit,
        )

    whole, cut = call(100), call(1)
    _assert_cut_page_is_honest(whole, cut, rows_key="groups", tool="aggregate")
    assert cut["total_count_basis"] == "exact"
    assert cut["total_count"] == 6
    assert cut["coverage"]["selection"]["unit"] == "group"


_LATER_PAGE_CALLS = [
    ("filter", {"predicate": _ATTR_PREDICATE, "select": ["node_id"]}),
    ("filter", {"predicate": _PLAIN_PREDICATE, "select": ["node_id"]}),
    ("map", {"projection": ["node_id", "attrs.status"]}),
    ("map", {"projection": ["node_id"]}),
]
_LATER_PAGE_IDS = ["filter-attrs", "filter-plain", "map-attrs", "map-plain"]


@pytest.mark.parametrize("tool,arguments", _LATER_PAGE_CALLS, ids=_LATER_PAGE_IDS)
def test_a_later_page_says_its_count_is_unknown(tools, tool, arguments, monkeypatch) -> None:
    """Rows the reader may not see are dropped after the SQL offset, so a
    page past the first cannot count the set; it says so instead of
    guessing. That holds wherever the reader's policy is evaluated: since
    #3201 the effective-attribute path counts the set with a bounded COUNT
    only when no row can be hidden (see the test below)."""
    # Nomos audit with no active bundle: the reader view (slice 7a) serves
    # every row in SQL, as the no-bundle default decision does, and the read
    # filter may still drop rows after the offset. Stubbing only
    # deployment_requires_nomos_runtime left the deployment's enforcement
    # mode unset, so the view's no-bundle default decision served no row.
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "audit")
    # Count semantics only: rows could be hidden, but this reader sees them all.
    monkeypatch.setattr(
        server_mod, "filter_node_rows_for_session",
        lambda conn, session, rows, **_: [dict(row) for row in rows],
    )
    page = tools[tool](input=_SCOPE, limit=2, offset=2, **arguments)
    assert "error" not in page, page
    assert len(page["items"]) == 2
    assert page["total_count"] is None
    assert page["total_count_basis"] == "unknown"
    assert page["coverage"]["selection"]["size"] is None
    assert page["coverage"]["delivery"]["complete"] is False
    assert "count unknown" in render(page, tool=tool)
    # Coverage is the one home of count provenance: no top-level copies.
    assert not {"total_count_set", "total_count_unit", "delivery_last_page"} & set(page)
    # The last page of the twelve rows: still no count, but provably the end.
    final = tools[tool](input=_SCOPE, limit=2, offset=10, **arguments)
    assert "error" not in final, final
    assert len(final["items"]) == 2
    assert final["truncated"] is False
    assert final.get("next_offset") is None
    assert final["total_count_basis"] == "unknown"
    assert final["coverage"]["delivery"]["complete"] is True
    # The page ending says it; coverage prints no delivery fact of its own.
    assert "complete" in render(final, tool=tool).splitlines()[0].split(" · ")


@pytest.mark.parametrize("tool,arguments", _LATER_PAGE_CALLS[::2], ids=_LATER_PAGE_IDS[::2])
def test_a_later_effective_page_is_counted_when_nothing_can_hide(tools, tool, arguments) -> None:
    """#3201: with Nomos off no row can be hidden, so a later page on the
    effective-attribute path counts the set with a bounded COUNT over its own
    SQL, and its last page from the page itself."""
    page = tools[tool](input=_SCOPE, limit=2, offset=2, **arguments)
    assert page["total_count"] == 12
    assert page["total_count_basis"] == "exact"
    assert page["coverage"]["delivery"]["complete"] is False
    assert "of 12" in render(page, tool=tool)
    final = tools[tool](input=_SCOPE, limit=2, offset=10, **arguments)
    assert final["total_count"] == 12
    assert final["total_count_basis"] == "exact"
    assert final["coverage"]["delivery"]["complete"] is True


def test_actual_byte_fitted_map_refreshes_delivered_rows(tools):
    whole = tools["map"](input=_SCOPE, projection=["node_id", "attrs.details"], limit=100)
    cut = tools["map"](input=_SCOPE, projection=["node_id", "attrs.details"],
                       limit=100, max_bytes=4096)
    assert "error" not in cut, cut
    assert 0 < len(cut["items"]) < len(whole["items"])
    assert cut["coverage"]["delivery"]["complete"] is False
    assert cut["coverage"]["delivery"]["bounds"] == ["output_bytes"]
    assert cut["next_offset"] == len(cut["items"])


def test_attrs_lookup_whole_cut_and_later_counts_are_qualified(tools):
    def call(limit, offset=0):
        return tools["search"](method="attrs", attr_filters={"text": "counted"},
                               subtype_filter="page", limit=limit, offset=offset)
    whole, cut, later = call(100), call(1), call(1, 1)
    _assert_cut_page_is_honest(whole, cut, rows_key="results", tool="search")
    assert whole["total_count"] == 12
    assert whole["coverage"]["retrieval"]["status"] == "exhaustive"
    assert cut["total_count_basis"] == "lower_bound"
    assert later["total_count"] is None
    assert later["total_count_basis"] == "unknown"
    assert later["coverage"]["delivery"]["complete"] is False
    # An uncut later page: the lookup read past the last row and found none.
    last = call(1, 11)
    assert "error" not in last, last
    assert len(last["results"]) == 1
    assert last.get("next_offset") is None
    assert last["total_count_basis"] == "unknown"
    assert last["coverage"]["delivery"]["complete"] is True
    # An offset clamped to the 10,000-row ceiling is not the requested page,
    # so its empty, uncut result proves nothing about the end of the set.
    clamped = call(1, 20_000)
    assert "error" not in clamped, clamped
    assert clamped["pagination_capped"] is True
    assert clamped["offset"] == 10_000
    assert clamped["coverage"]["delivery"]["complete"] is not True
