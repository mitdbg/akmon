"""Byte-budget paging predicts its rerun limit from the first page.

PACT ``lane-h-fit-to-budget-once``. When a pageable read exceeds its MCP
byte budget, ``_fit_to_budget`` used to binary-search ``limit`` by rerunning
the operator four or five times. It now measures prefixes of the first page
to predict the largest limit that fits, reruns ONCE at that limit, and
searches below it only on a miss. Every served page is still a real rerun.
"""

from __future__ import annotations

import logging
import math
from typing import Any

import pytest

from okg.substrate.mcp import response_bounds
from okg.substrate.mcp.response_bounds import (
    BYTE_TRUNCATION_REASON,
    bound_read_tool,
    public_call_tool_result_bytes,
)

_STAMPED = (
    "truncated",
    "truncation_reason",
    "response_byte_budget",
    "requested_limit",
    "effective_limit",
)


def _search_like(
    calls: list[dict[str, Any]],
    *,
    total: int = 40,
    row_chars: int = 600,
    rerun_padding: int = 0,
):
    """A ranked operator whose page is a pure function of (limit, offset, pin).

    `rerun_padding` adds a page-level field to every call made with a limit
    below 20, so a rerun weighs more than the same rows cut from the first
    page -- the shape that makes a prediction miss.
    """

    def search(
        query: str,
        limit: int = 20,
        offset: int = 0,
        generation_id: str | None = None,
        max_bytes: int | None = None,
    ) -> dict[str, Any]:
        calls.append({"limit": limit, "offset": offset, "pin": generation_id})
        pin = generation_id or "gen:7"
        rows = [
            {"node_id": f"doc:{i:03d}", "excerpt": f"{i:03d}" + "é" * row_chars}
            for i in range(offset, min(offset + limit, total))
        ]
        truncated = offset + limit < total
        page: dict[str, Any] = {
            "query": query,
            "results": rows,
            "limit": limit,
            "offset": offset,
            "next_offset": offset + limit if truncated else None,
            "total_count": total,
            "truncated": truncated,
            "rollup_contributing_chunks": {row["node_id"]: 3 for row in rows},
            "pinned_generation_id": pin,
        }
        if rerun_padding and limit < 20:
            # A warning notice: the search text prints it (#3666). A search
            # reply in text no longer prints unplaced page fields (the old
            # `notes` padding), so padding there no longer weighs a rerun.
            page["warning"] = {"code": "padding", "message": "p" * rerun_padding}
        return page

    return search


def _direct(fn, budget: int, requested: int, **arguments: Any) -> dict[str, Any]:
    """What a caller gets by calling the operator at the served limit."""
    page = fn(**arguments)
    page.update(
        truncated=True,
        truncation_reason=BYTE_TRUNCATION_REASON,
        response_byte_budget=budget,
        requested_limit=requested,
        effective_limit=arguments["limit"],
    )
    return page


def _largest_fitting_limit(fn, budget: int, requested: int) -> int:
    best = 0
    for limit in range(1, requested):
        page = _direct(fn, budget, requested, query="q", limit=limit,
                       generation_id="gen:7")
        if public_call_tool_result_bytes(page) <= budget:
            best = limit
    return best


def test_a_predicted_hit_runs_the_operator_twice_and_serves_the_rerun() -> None:
    calls: list[dict[str, Any]] = []
    search = _search_like(calls)

    out = bound_read_tool("search", search)(query="q", limit=20, max_bytes=8_192)

    assert len(calls) == 2, calls
    assert calls[0] == {"limit": 20, "offset": 0, "pin": None}
    served_limit = out["effective_limit"]
    assert calls[1] == {"limit": served_limit, "offset": 0, "pin": "gen:7"}
    assert public_call_tool_result_bytes(out) <= 8_192

    # Field by field, the served page IS the page a direct call at the
    # served limit returns -- rows, order, cursor, totals and per-row maps.
    expected = _direct(
        _search_like([]), 8_192, 20,
        query="q", limit=served_limit, generation_id="gen:7",
    )
    assert set(out) == set(expected)
    for key in expected:
        assert out[key] == expected[key], key
    assert out["next_offset"] == served_limit
    assert len(out["results"]) == served_limit
    # Uniform rows: the prediction lands on the largest limit that fits.
    assert served_limit == _largest_fitting_limit(_search_like([]), 8_192, 20)


def test_a_missed_prediction_searches_below_it_with_a_bounded_rerun_count(
) -> None:
    calls: list[dict[str, Any]] = []
    search = _search_like(calls, rerun_padding=2_500)

    out = bound_read_tool("search", search)(query="q", limit=20, max_bytes=8_192)

    predicted = calls[1]["limit"]
    # First call + the predicted rerun + a binary search over [1, predicted-1].
    bound = 2 + math.ceil(math.log2(predicted))
    assert 3 <= len(calls) <= bound, calls
    assert all(call["limit"] < predicted for call in calls[2:])
    assert all(call["pin"] == "gen:7" for call in calls[1:])
    assert public_call_tool_result_bytes(out) <= 8_192
    served_limit = out["effective_limit"]
    assert served_limit < predicted
    expected = _direct(
        _search_like([], rerun_padding=2_500), 8_192, 20,
        query="q", limit=served_limit, generation_id="gen:7",
    )
    assert out == expected
    # Below the miss, the search still finds the largest limit that fits.
    assert served_limit == _largest_fitting_limit(
        _search_like([], rerun_padding=2_500), 8_192, 20,
    )


def test_a_prediction_that_errs_short_is_served_as_it_is() -> None:
    """The disclosed trade-off: a page can be shorter than the largest fit.

    Here every rerun below the requested limit sheds a 3 KB page-level field
    the first page carries, so the first page's prefixes over-estimate every
    rerun and the prediction comes in short. It is served without a second
    rerun.
    """
    calls: list[dict[str, Any]] = []

    def search(
        query: str,
        limit: int = 20,
        offset: int = 0,
        generation_id: str | None = None,
        max_bytes: int | None = None,
    ) -> dict[str, Any]:
        calls.append({"limit": limit})
        page = _search_like([])(
            query=query, limit=limit, offset=offset, generation_id=generation_id,
        )
        if limit == 20:
            # A printed notice (#3666: unplaced page fields are not printed
            # on a search reply, so the old `page_summary` weighed nothing).
            page["warning"] = {"code": "padding", "message": "s" * 3_000}
        return page

    out = bound_read_tool("search", search)(query="q", limit=20, max_bytes=8_192)

    assert len(calls) == 2
    largest = max(
        limit for limit in range(1, 20)
        if public_call_tool_result_bytes(
            _direct(search, 8_192, 20, query="q", limit=limit,
                    generation_id="gen:7")
        ) <= 8_192
    )
    assert out["effective_limit"] < largest
    assert public_call_tool_result_bytes(out) <= 8_192


def test_nomos_raw_position_paging_serves_the_rerun_not_a_prefix() -> None:
    """`expand` under Nomos: visible rows are fewer than raw positions.

    A prefix of the first page's visible rows is NOT what a smaller rerun
    returns (hidden raw positions move the cursor). The prediction only picks
    the limit; the served page is the rerun's own.
    """
    calls: list[int] = []
    hidden = {1, 4, 5, 9}

    def expand(
        input: Any,
        limit: int = 12,
        offset: int = 0,
        generation_id: str | None = None,
        depth: int = 1,
        path: list[str] | None = None,
        max_bytes: int | None = None,
    ) -> dict[str, Any]:
        calls.append(limit)
        window = range(offset, offset + limit)
        return {
            "neighbors": [
                {"edge_id": f"edge:{i}", "attrs": {"text": "é" * 700}}
                for i in window if i not in hidden
            ],
            "limit": limit,
            "offset": offset,
            "next_offset": offset + limit,
            "truncated": True,
            "pinned_generation_id": generation_id or "gen:raw",
        }

    out = bound_read_tool("expand", expand)(
        input={"node_id": "n:start"}, limit=12, max_bytes=6_000,
    )

    assert len(calls) == 2
    expected = _direct(
        expand, 6_000, 12,
        input={"node_id": "n:start"}, limit=out["effective_limit"],
        generation_id="gen:raw",
    )
    assert out == expected
    assert out["next_offset"] == out["effective_limit"]
    assert public_call_tool_result_bytes(out) <= 6_000


def test_no_prefix_row_fits_tries_limit_one_once_then_cuts_its_row() -> None:
    """#3666 step 3b: the limit-1 page is served with its one row cut and
    marked, keeping its cursor; it is never refused for size alone."""
    calls: list[int] = []

    def filter_rows(
        input: Any,
        predicate: dict[str, Any],
        limit: int = 8,
        offset: int = 0,
        max_bytes: int | None = None,
    ) -> dict[str, Any]:
        calls.append(limit)
        return {
            "items": [{"text": "x" * 6_000} for _ in range(limit)],
            "limit": limit,
            "offset": offset,
            "next_offset": offset + limit,
            "truncated": True,
        }

    out = bound_read_tool("filter", filter_rows)(
        input={"kind": "nodes"}, predicate={}, limit=8, max_bytes=4_096,
    )

    assert calls == [8, 1]
    assert out.get("status") != "refused", out
    assert out["next_offset"] == 1
    (row,) = out["items"]
    assert row["values_cut"]["text"]["chars"] == 6_000
    assert row["text"] == "x" * row["values_cut"]["text"]["served_chars"]
    assert public_call_tool_result_bytes(out, tool="filter") <= 4_096


def test_an_unknown_page_shape_searches_the_whole_range_and_says_so(
    caplog: pytest.LogCaptureFixture,
) -> None:
    calls: list[int] = []

    def map_rows(
        input: Any,
        projection: Any = None,
        limit: int = 16,
        offset: int = 0,
        max_bytes: int | None = None,
    ) -> dict[str, Any]:
        calls.append(limit)
        return {
            "records": [{"text": "é" * 500} for _ in range(limit)],
            "limit": limit,
            "offset": offset,
            "next_offset": offset + limit,
            "truncated": True,
        }

    with caplog.at_level(logging.INFO, logger=response_bounds.__name__):
        out = bound_read_tool("map", map_rows)(
            input={"kind": "nodes"}, limit=16, max_bytes=6_000,
        )

    assert "found no row list" in caplog.text
    # The pre-prediction search: first call plus ceil(log2(15)) reruns.
    assert len(calls) == 1 + math.ceil(math.log2(16 - 1))
    assert public_call_tool_result_bytes(out) <= 6_000
    assert out["truncation_reason"] == BYTE_TRUNCATION_REASON


#: The run bound `_fit_to_budget` states: first call + predicted rerun + a
#: binary search over the rest of [1, limit-1].
def _run_bound(limit: int) -> int:
    return 2 + math.ceil(math.log2(limit - 1))


def _raw_position_expand(calls: list[int], *, hidden: set[int], chars: int):
    """One-hop `expand` under Nomos, as `tools.py` pages it.

    The window is RAW positions `[offset, offset+limit)`; hidden positions
    are dropped from `neighbors` but still consumed, so
    `next_offset = offset + limit`.
    """

    def expand(
        input: Any,
        limit: int = 20,
        offset: int = 0,
        generation_id: str | None = None,
        depth: int = 1,
        path: list[str] | None = None,
        max_bytes: int | None = None,
    ) -> dict[str, Any]:
        calls.append(limit)
        return {
            "neighbors": [
                {"edge_id": f"edge:{i:03d}", "attrs": {"text": "x" * chars}}
                for i in range(offset, offset + limit) if i not in hidden
            ],
            "limit": limit,
            "offset": offset,
            "next_offset": offset + limit,
            "truncated": True,
            "pinned_generation_id": generation_id or "gen:raw",
        }

    return expand


def _serve(
    operator_name: str,
    fn,
    monkeypatch: pytest.MonkeyPatch | None = None,
    *,
    blind: bool = False,
    **arguments: Any,
) -> dict[str, Any]:
    """Serve one call; `blind=True` is the origin/dev search (no prediction)."""
    if blind:
        assert monkeypatch is not None
        with monkeypatch.context() as patch:
            patch.setattr(
                response_bounds, "_predict_fitting_limit",
                lambda *_a, **_k: None,
            )
            return bound_read_tool(operator_name, fn)(**arguments)
    return bound_read_tool(operator_name, fn)(**arguments)


_HIDDEN_PATTERNS = {
    # The review probe: the first twelve raw positions are hidden.
    "prefix_0_11": set(range(12)),
    # The review probe: every other raw position is hidden.
    "alternating": set(range(1, 40, 2)),
    "tail_hidden": set(range(12, 40)),
    "scattered": {0, 2, 3, 7, 8, 9, 13, 17, 18},
    "none": set(),
}


@pytest.mark.parametrize("pattern", sorted(_HIDDEN_PATTERNS))
def test_raw_position_paging_serves_at_least_the_blind_search_rows(
    pattern: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hidden = _HIDDEN_PATTERNS[pattern]
    arguments = {"input": {"node_id": "n:start"}, "limit": 20, "max_bytes": 6_000}
    blind_calls: list[int] = []
    blind = _serve(
        "expand", _raw_position_expand(blind_calls, hidden=hidden, chars=1_500),
        monkeypatch, blind=True, **arguments,
    )
    calls: list[int] = []
    fn = _raw_position_expand(calls, hidden=hidden, chars=1_500)
    out = _serve("expand", fn, **arguments)

    assert public_call_tool_result_bytes(out) <= 6_000
    assert len(calls) <= _run_bound(20), calls
    # The NUMBER of rows served, not only that something was served.
    assert len(out["neighbors"]) >= len(blind["neighbors"]), (
        calls, blind_calls,
    )
    if blind["neighbors"]:
        assert out["neighbors"], "an empty page while fitting rows remain"
    # Still a real rerun.
    expected = _direct(
        fn, 6_000, 20, input={"node_id": "n:start"},
        limit=out["effective_limit"], generation_id="gen:raw",
    )
    assert out == expected


def test_hidden_prefix_probe_serves_three_rows_not_an_empty_page() -> None:
    """The review's exact probe: limit 20, raw 0-11 hidden, ~1.5 KB rows."""
    calls: list[int] = []
    out = _serve(
        "expand",
        _raw_position_expand(calls, hidden=set(range(12)), chars=1_500),
        input={"node_id": "n:start"}, limit=20, max_bytes=6_000,
    )

    assert len(out["neighbors"]) == 3
    assert out["effective_limit"] == 15
    assert out["next_offset"] == 15
    assert len(calls) <= _run_bound(20)


def _last_page_filter(calls: list[int], *, total: int, chars: int):
    """A `filter` whose first page is its LAST page (no cursor).

    Stamps the pagination block the way `_stamp_operator` does, so a
    truncated rerun carries it and the untruncated first page does not.
    """
    from okg.substrate.mcp.server import add_pagination_hint

    def filter_rows(
        input: Any,
        predicate: dict[str, Any],
        limit: int = 30,
        offset: int = 0,
        generation_id: str | None = None,
        max_bytes: int | None = None,
    ) -> dict[str, Any]:
        calls.append(limit)
        rows = [
            {"node_id": f"n:{i:03d}", "text": "y" * chars}
            for i in range(offset, min(offset + limit, total))
        ]
        truncated = offset + limit < total
        page = {
            "items": rows,
            "total_count": len(rows),
            "truncated": truncated,
            "limit": limit,
            "offset": offset,
            "next_offset": offset + limit if truncated else None,
            "pinned_generation_id": generation_id or "gen:last",
        }
        return add_pagination_hint(page, "filter")

    return filter_rows


#: The review sweep: 27 budgets from 4,096 to 5,188 bytes.
_LAST_PAGE_BUDGETS = list(range(4_096, 5_201, 42))
#: Budgets in the sweep that must finish within three runs: all 27 (each
#: measured at two runs). With the cursor unmodelled, 8 of 27 did, and the
#: other 19 took six.
_LAST_PAGE_WITHIN_THREE = 27


def test_a_last_first_page_predicts_with_the_cursor_it_will_carry() -> None:
    """Before the cursor was modelled, 19 of these 27 budgets missed."""
    assert len(_LAST_PAGE_BUDGETS) == 27
    runs: dict[int, int] = {}
    for budget in _LAST_PAGE_BUDGETS:
        calls: list[int] = []
        fn = _last_page_filter(calls, total=25, chars=330)
        out = _serve(
            "filter", fn,
            input={"kind": "nodes"}, predicate={}, limit=30, max_bytes=budget,
        )
        assert public_call_tool_result_bytes(out) <= budget
        assert out["items"], budget
        runs[budget] = len(calls)
    within_three = sum(1 for count in runs.values() if count <= 3)
    assert within_three >= _LAST_PAGE_WITHIN_THREE, runs
    assert max(runs.values()) <= _run_bound(30), runs


class _Live:
    """A prepared `expand(live=...)` step that counts its calls."""

    generation_id = "gen:live"

    def __init__(self) -> None:
        self.runs = 0
        self.refusals: list[str] = []

    def reserve_envelope(self, graph: dict[str, Any]) -> dict[str, Any]:
        return {"relation": "status", "rows": [], "reserve": "r" * 400}

    def refuse(self, reason: str) -> None:
        self.refusals.append(reason)

    def run(self, graph: dict[str, Any], room: Any) -> dict[str, Any]:
        self.runs += 1
        envelope = {"relation": "status", "rows": [{"state": "ok"}]}
        assert room.fits(envelope)
        return envelope


def test_expand_live_still_makes_its_live_call_once() -> None:
    graph_calls: list[dict[str, Any]] = []
    live = _Live()

    def expand(
        input: Any,
        limit: int = 20,
        offset: int = 0,
        generation_id: str | None = None,
        depth: int = 1,
        path: list[str] | None = None,
        live: Any = None,
        max_bytes: int | None = None,
    ) -> dict[str, Any]:
        graph_calls.append({"limit": limit, "live": live, "pin": generation_id})
        return {
            "neighbors": [
                {"edge_id": f"edge:{i}", "attrs": {"text": "é" * 600}}
                for i in range(offset, offset + limit)
            ],
            "limit": limit,
            "offset": offset,
            "next_offset": offset + limit,
            "truncated": True,
            "pinned_generation_id": generation_id,
        }

    out = bound_read_tool("expand", expand, live_step=lambda _args: live)(
        input={"node_id": "n:start"},
        limit=20,
        live={"relation": "status", "args": {}},
        max_bytes=8_192,
    )

    assert live.runs == 1
    assert live.refusals == []
    assert len(graph_calls) == 2
    assert all(call["live"] is None for call in graph_calls)
    assert all(call["pin"] == "gen:live" for call in graph_calls)
    assert out["external_live"] == {"relation": "status", "rows": [{"state": "ok"}]}
    assert public_call_tool_result_bytes(out) <= 8_192


# --- Real passage-ranked search over a synthetic graph (PostgreSQL) --------

#: Stamped per call rather than by paging; two calls legitimately differ here.
_PER_CALL_FIELDS = frozenset({"elapsed_ms", "call_id", "trace_id"})


def publish_passage_corpus(dsn: str, root: Any, *, docs: int, chunks: int) -> str:
    """Publish `docs` parents, each with `chunks` matching passages.

    Synthetic text only. Every chunk names the fixture term so every parent
    ranks, and each body is long enough that a 20-row page overflows a small
    byte budget, as the passage-ranked searches that motivated this change do.
    """
    from tests.substrate.mcp.erag_rollup_fixture import (
        DOC,
        NARROWINGS,
        SUBTYPES,
        TERM,
        _edge,
        _fact,
    )
    from tests.substrate.mcp.search_fixture import (
        make_search_deployment,
        publish_nodes,
    )

    deployment = make_search_deployment(root, dsn=dsn)
    nodes: list[Any] = []
    edges: list[Any] = []
    for d in range(docs):
        doc_id = f"doc:{d:03d}"
        nodes.append(
            _fact(doc_id, DOC.name, {"path": f"corpus/{d:03d}-{TERM}-ticket.txt"})
        )
        for c in range(chunks):
            chunk_id = f"{doc_id}:chunk:{c}"
            weight = 1 + ((d * 7 + c * 3) % 5)
            body = " ".join(
                [f"ticket {d} passage {c} records"]
                + [TERM] * weight
                + ["cladding tolerance drift observed during the run"] * 6
            )
            nodes.append(_fact(chunk_id, "chunk", {"text": body}))
            edges.append(_edge(doc_id, chunk_id, "contains"))
            edges.append(_edge(chunk_id, doc_id, "member_of"))
    publish_nodes(
        dsn,
        deployment,
        subtypes=SUBTYPES,
        narrowings=NARROWINGS,
        nodes=nodes,
        edges=edges,
    )
    return deployment


def _differing_fields(
    served: dict[str, Any], direct: dict[str, Any],
) -> dict[str, tuple[Any, Any]]:
    """Fields where the served page and a direct call disagree.

    Ignored: the byte-truncation stamp (absent on a page that fit),
    per-call identifiers, and `publish_freshness.publish_age_seconds`, the
    wall-clock age at the moment of each call; the rest of that block must
    agree.
    """

    def comparable(page: dict[str, Any]) -> dict[str, Any]:
        out = {
            key: value for key, value in page.items()
            if key not in _STAMPED and key not in _PER_CALL_FIELDS
        }
        freshness = out.get("publish_freshness")
        if isinstance(freshness, dict):
            out["publish_freshness"] = {
                key: value for key, value in freshness.items()
                if key != "publish_age_seconds"
            }
        return out

    left, right = comparable(served), comparable(direct)
    return {
        key: (left.get(key), right.get(key))
        for key in sorted(set(left) | set(right))
        if left.get(key) != right.get(key)
    }


@pytest.mark.postgres
def test_passage_ranked_search_keeps_every_row_in_one_execution(
    clean_runtime: str,
    okg_mcp_can_log_in: str,
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    from okg.substrate.mcp import server as server_module
    from okg.substrate.mcp.server import build_server
    from tests.substrate.mcp.erag_rollup_fixture import TERM

    deployment = publish_passage_corpus(clean_runtime, tmp_path, docs=40, chunks=3)
    executions: list[int] = []
    real_search = server_module._search

    def counted(*args: Any, **kwargs: Any) -> dict[str, Any]:
        executions.append(int(kwargs["limit"]))
        return real_search(*args, **kwargs)

    monkeypatch.setattr(server_module, "_search", counted)
    mcp = build_server(
        dsn=clean_runtime, deployment_name=deployment, auto_advance="always",
    )
    try:
        search = mcp._tool_manager._tools["search"].fn
        arguments = {
            "query": TERM,
            "subtype_filter": "doc",
            "rank_by": "passages",
            "limit": 20,
        }
        # Small enough that the 20-row page does not fit whole.
        served = search(**arguments, max_bytes=8_000)
        fitted_executions = list(executions)
        executions.clear()
        assert served.get("status") != "refused", served
        direct = search(**arguments, max_bytes=262_144)
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()

    # PACT read-passage-rows-3666 (D4): a search page over its budget keeps
    # every requested row and shortens its values; it is never read again
    # at a smaller limit. (Before: reran once at the predicted limit and
    # served that many rows; the rerun predictor is still exercised by the
    # filter, map and expand tests above.)
    assert fitted_executions == [20], fitted_executions
    assert executions == [20]
    assert direct["pinned_generation_id"] == served["pinned_generation_id"]
    assert public_call_tool_result_bytes(served) <= 8_000
    assert [row["node_id"] for row in served["results"]] == [
        row["node_id"] for row in direct["results"]
    ]
    assert len(served["results"]) == 20
    assert "passages_shortened_to" in served
    assert "effective_limit" not in served and "truncation_reason" not in served
    assert served["next_offset"] == direct["next_offset"]
    request.node.user_properties.extend([
        ("executions", len(fitted_executions)),
        ("served_bytes", public_call_tool_result_bytes(served)),
        ("total_count", served["total_count"]),
    ])


# --- the count outcome must not move the cut ---------------------------------
# PACT ``mcp-byte-fit-count-stable``.

_ROW_KEYS = {
    "search": "results", "filter": "items", "map": "items",
    "expand": "neighbors", "aggregate": "groups",
}
_OUTCOMES = ("exact", "lower_bound", "unknown", "timed_out")


def _row_shape(operator_name: str, i: int, chars: int) -> dict[str, Any]:
    """A row of the shape each operator's page carries."""
    text = f"{i:03d}" + "é" * chars
    if operator_name == "search":
        return {"node_id": f"doc:{i:03d}", "excerpt": text, "score": 0.5}
    if operator_name in {"filter", "map"}:
        return {"node_id": f"doc:{i:03d}", "subtype": "item",
                "attrs": {"text": text, "status": "open", "size": i}}
    if operator_name == "expand":
        return {"edge_id": i, "edge_type": "links", "node_id": f"doc:{i:03d}",
                "attrs": {"text": text}}
    return {"key": {"status": f"s{i}"}, "count": i, "note": text}


def _counted_page(
    operator_name: str, outcome: str, *, limit: int = 20, offset: int = 0,
    total: int = 40, row_chars: int = 300, generation_id: str | None = None,
    lower_total: int | None = None,
) -> dict[str, Any]:
    """A page in the shape the producer sends for each count outcome.

    exact: `retrieval_exhaustive` True. lower_bound: no exhaustive marker.
    unknown: no total. timed_out: unknown plus the date call's reason. The
    page passes through `with_read_coverage`, as a producer's does.
    """
    from okg.substrate.mcp.envelope import with_read_coverage

    rows = [
        _row_shape(operator_name, i, row_chars)
        for i in range(offset, min(offset + limit, total))
    ]
    truncated = offset + limit < total
    page: dict[str, Any] = {
        "operator_name": operator_name,
        _ROW_KEYS[operator_name]: rows,
        "limit": limit,
        "offset": offset,
        "next_offset": offset + limit if truncated else None,
        "truncated": truncated,
        "pinned_generation_id": generation_id or "gen:7",
    }
    if outcome == "exact":
        page.update(total_count=total, total_count_basis="exact",
                    retrieval_exhaustive=True)
    elif outcome == "lower_bound":
        page.update(
            total_count=total if lower_total is None else lower_total,
            total_count_basis="lower_bound",
        )
    else:
        page.update(total_count=None, total_count_basis="unknown")
        if outcome == "timed_out":
            page["total_count_reason"] = "count_not_computed_in_time"
    return with_read_coverage(page)


def _counted_search(*, outcome: str, noted: bool = True, edit=None):
    """A search whose count came from a bounded COUNT.

    `noted` mimics the server: `_bounded_operator_count` calls
    `note_count_may_vary()`. A page whose count is fixed by the read itself
    (`noted=False`) is sized as it is. `edit` changes the page first.
    """
    def search(
        query: str,
        limit: int = 20,
        offset: int = 0,
        generation_id: str | None = None,
        max_bytes: int | None = None,
    ) -> dict[str, Any]:
        if noted:
            response_bounds.note_count_may_vary()
        page = _counted_page(
            "search", outcome, limit=limit, offset=offset,
            generation_id=generation_id,
        )
        return edit(page) if edit is not None else page

    return search


@pytest.mark.parametrize("operator_name", sorted(_ROW_KEYS))
def test_the_stable_size_bounds_every_count_outcome(operator_name: str) -> None:
    """`stable` is one number, `>= measure` of each outcome's page of the same rows.

    Per-field accounting: selection size and count fact, the timed-out
    notice, `retrieval_exhaustive` and coverage.retrieval status (exhaustive,
    bounded, unknown) are all count-dependent, and all live in the envelope.
    """
    measure = public_call_tool_result_bytes
    stable = response_bounds._count_stable(operator_name, measure)
    pages = {o: _counted_page(operator_name, o, limit=12) for o in _OUTCOMES}
    sizes = {o: measure(p) for o, p in pages.items()}
    assert len(set(sizes.values())) > 1, sizes
    # Every outcome's page sizes to the same number: that is what makes the
    # cut independent of the outcome, not just safe.
    assert len({stable(p) for p in pages.values()}) == 1, {
        o: stable(p) for o, p in pages.items()
    }
    for outcome, page in pages.items():
        assert stable(page) >= max(sizes.values()), (outcome, sizes, stable(page))
        # The served page itself is never changed by sizing.
        assert page == _counted_page(operator_name, outcome, limit=12)


def test_the_same_request_returns_the_same_page_whatever_the_count_outcome() -> None:
    """Exact, lower-bound and unknown counts render different sizes; the cut ignores that.

    On dev's `_fit_to_budget` the size test sees the page's own count, so at
    a budget between the outcomes' renderings of the same rows the runs keep
    different row counts and their `next_offset` differs. The fit now adds
    the largest count-dependent envelope growth to every size test.
    """
    limit = 12
    sizes = {
        o: public_call_tool_result_bytes(
            _direct(_counted_search(outcome=o), 8_192, 20,
                    query="q", limit=limit, generation_id="gen:7")
        )
        for o in _OUTCOMES
    }
    # Premise: the outcomes really render differently at the same rows.
    assert len(set(sizes.values())) > 1, sizes
    # Budgets between the smallest and the largest rendering.
    lo, hi = min(sizes.values()), max(sizes.values())
    assert lo >= 4_096, sizes  # the tool raises a max_bytes under 4096
    for budget in range(lo - 5, hi + 5, 3):
        served = {
            outcome: bound_read_tool("search", _counted_search(outcome=outcome))(
                query="q", limit=20, max_bytes=budget,
            )
            for outcome in _OUTCOMES
        }
        rows = {o: [r["node_id"] for r in p["results"]] for o, p in served.items()}
        cursors = {o: p["next_offset"] for o, p in served.items()}
        assert len({tuple(r) for r in rows.values()}) == 1, (budget, rows)
        assert len(set(cursors.values())) == 1, (budget, cursors)
        for outcome, page in served.items():
            assert public_call_tool_result_bytes(page) <= budget, (budget, outcome)
    # The served count is the page's own, never a sizing form.
    assert served["exact"]["total_count"] == 40
    assert served["unknown"]["total_count"] is None


def test_a_budget_far_from_the_cut_serves_the_page_unchanged() -> None:
    first = _counted_search(outcome="unknown")(query="q", limit=20)
    out = bound_read_tool("search", _counted_search(outcome="unknown"))(
        query="q", limit=20, max_bytes=262_144,
    )
    assert out == first
    assert out["total_count"] is None
    assert out["total_count_basis"] == "unknown"
    assert "response_byte_budget" not in out


def test_an_untruncated_page_near_the_budget_is_served_whole_as_on_dev() -> None:
    """A page that holds the whole set has one form (exact without counting).

    Its size test must be dev's, so a result that fits whole is never paged
    because a count form it cannot take is a few bytes larger.
    """
    page = _counted_page("search", "exact", limit=20, total=12)
    assert page["truncated"] is False and page["next_offset"] is None
    assert response_bounds._count_variants("search", page) == []
    exact = public_call_tool_result_bytes(page)
    assert exact >= 4_096, exact

    def search(query: str, limit: int = 20, offset: int = 0,
               generation_id: str | None = None, max_bytes: int | None = None):
        return _counted_page("search", "exact", limit=limit, offset=offset, total=12)

    out = bound_read_tool("search", search)(query="q", limit=20, max_bytes=exact)
    assert out == page


def test_count_variants_apply_only_to_count_bearing_row_pages_with_more() -> None:
    page = _counted_page("search", "exact", limit=3)
    variants = response_bounds._count_variants("search", page)
    assert len(variants) == 4
    exact, lower, unknown, timed_out = variants
    assert exact["total_count_basis"] == "exact"
    assert exact["coverage"]["retrieval"]["status"] == "exhaustive"
    assert "size" not in exact["coverage"]["selection"]
    assert lower["total_count_basis"] == "lower_bound"
    assert lower["coverage"]["retrieval"]["status"] == "bounded"
    # A real lower-bound page says `size: null` (envelope.py fresh_selection).
    assert lower["coverage"]["selection"]["size"] is None
    assert unknown["coverage"]["selection"]["size"] is None
    assert unknown["coverage"]["retrieval"]["status"] == "unknown"
    assert timed_out["total_count_reason"] == "count_not_computed_in_time"
    # The page itself is untouched.
    assert page == _counted_page("search", "exact", limit=3)
    # Refused, error, no-row-list, no-count and non-count operators: none.
    assert response_bounds._count_variants("search", {**page, "status": "refused"}) == []
    assert response_bounds._count_variants("search", {**page, "error": {"code": "x"}}) == []
    assert response_bounds._count_variants("search", {"total_count": 3}) == []
    no_count = {k: v for k, v in page.items() if k != "total_count"}
    assert response_bounds._count_variants("search", no_count) == []
    assert response_bounds._count_variants("node", page) == []


def _full_stable(operator_name: str, measure, page: dict[str, Any]) -> int:
    """The reference: render the page and all four count forms in full."""
    return max([
        measure(page),
        *(measure(v) for v in response_bounds._count_variants(operator_name, page)),
    ])


@pytest.mark.parametrize("operator_name", sorted(_ROW_KEYS))
@pytest.mark.parametrize("outcome", _OUTCOMES)
@pytest.mark.parametrize("rows", [1, 2, 3, 7, 9, 10, 11, 12, 25, 99, 100, 101])
def test_the_fast_stable_size_is_the_full_five_render_max(
    operator_name: str, outcome: str, rows: int,
) -> None:
    """The stand-in extra gives the same size as rendering every form in full.

    Row counts cross the digit widths of the span ("1-9", "1-10", "1-100"),
    and for the lower bound the total is set equal to the rows shown (the
    "more follow" text), one more, and one fewer.
    """
    measure = public_call_tool_result_bytes
    totals = [None] if outcome != "lower_bound" else [rows, rows + 1, max(rows - 1, 1)]
    for lower_total in totals:
        page = _counted_page(
            operator_name, outcome, limit=rows, total=rows + 5, row_chars=20,
            lower_total=lower_total,
        )
        assert len(page[_ROW_KEYS[operator_name]]) == rows
        assert page["next_offset"] == rows  # more rows follow: the forms apply
        fast = response_bounds._count_stable(operator_name, measure)(page)
        assert fast == _full_stable(operator_name, measure, page), (
            operator_name, outcome, rows, lower_total,
        )
        # A second page at an offset (different span text) and a repeat call
        # of the same closure (the per-call memo) give the same answer.
        later = _counted_page(
            operator_name, outcome, limit=rows, offset=rows, total=3 * rows + 5,
            row_chars=20, lower_total=lower_total and lower_total + rows,
        )
        stable = response_bounds._count_stable(operator_name, measure)
        assert stable(later) == _full_stable(operator_name, measure, later)
        assert stable(later) == _full_stable(operator_name, measure, later)


@pytest.mark.parametrize("operator_name", sorted(_ROW_KEYS))
@pytest.mark.parametrize("outcome", _OUTCOMES)
def test_count_forms_match_the_count_fields_operator_count_fields_returns(
    operator_name: str, outcome: str,
) -> None:
    """Each form has the count fields `_operator_count_fields` returns for
    its outcome (server.py: `total_count_basis`, `retrieval_exhaustive`,
    `total_count_reason`; exact sets `retrieval_exhaustive`, timed out sets
    the reason) and the coverage block `with_read_coverage` builds from them.
    The reference page here is the test's own `_counted_page`, which builds
    those fields as server.py does; it is not the server's output (the
    PostgreSQL test through the real `filter` checks that end to end). The
    total itself is the widest number.
    """
    page = _counted_page(operator_name, "exact", limit=4)
    forms = dict(zip(_OUTCOMES, response_bounds._count_variants(operator_name, page)))
    real = _counted_page(operator_name, outcome, limit=4)
    form = forms[outcome]
    assert form["coverage"] == real["coverage"]
    assert form["total_count_basis"] == real["total_count_basis"]
    assert form.get("total_count_reason") == real.get("total_count_reason")
    assert form.get("retrieval_exhaustive") == real.get("retrieval_exhaustive")
    assert (form["total_count"] is None) == (real["total_count"] is None)


def test_the_search_lookup_exact_note_is_in_the_exact_form_only() -> None:
    """server.py adds `notes: [{count: exact, ...}]` only when exact.

    An exact page carries it, an unknown page of the same call does not; the
    sizes still agree, because the exact form adds it for a call that asked
    for `completeness="exact"` and no other form carries it.
    """
    measure = public_call_tool_result_bytes
    exact = _counted_page("search", "exact", limit=6)
    exact["notes"] = [dict(response_bounds.COUNT_EXACT_NOTE)]
    unknown = _counted_page("search", "unknown", limit=6)
    stable = response_bounds._count_stable("search", measure, exact_note=True)
    assert stable(exact) == stable(unknown)
    for page in (exact, unknown):
        forms = response_bounds._count_variants("search", page, exact_note=True)
        assert [bool(f.get("notes")) for f in forms] == [True, False, False, False]
        assert stable(page) == max(measure(page), *(measure(f) for f in forms))
    # Without the call asking for it, an unknown page adds no note.
    forms = response_bounds._count_variants("search", unknown)
    assert not any(f.get("notes") for f in forms)


def _lexical_page(*, capped: bool, bounded_mode: bool) -> dict[str, Any]:
    """A lexical search page built by search.py's own helpers."""
    import importlib

    # `okg.substrate.mcp.search` as an attribute is the tool function.
    search_mod = importlib.import_module("okg.substrate.mcp.search")
    cap = search_mod._IndexScanCap(
        None, segment="s", index_name="i", exact=not bounded_mode,
    )
    page = _counted_page("search", "exact", limit=6, row_chars=20)
    page.update(search_mod._count_basis(
        rank_by=search_mod.RANK_BY_SELF, rank_capped=capped,
        rollup_chunk_cap_reached=False, fold=None,
    ))
    page["rank_capped"] = capped
    page["completeness"] = cap.mode
    page["coverage"] = {"retrieval": search_mod._lexical_retrieval(
        cap, rank_capped=capped, work_capped=False,
        count_exact=page["total_count_basis"] == "exact",
    )}
    return page


@pytest.mark.parametrize("shape", [
    "lexical_bounded", "lexical_capped", "not_a_total", "reference_read", "aggregate",
])
def test_pages_whose_count_is_fixed_are_sized_as_they_are(shape: str) -> None:
    """Lexical search, a capped rank, not_a_total, a result-reference read and
    an aggregate never ran the bounded COUNT, so no form is added: the size is
    the plain `measure` (dev's), even though the forms could be built."""
    from okg.substrate.mcp.envelope import with_read_coverage

    operator_name = "search"
    if shape == "lexical_bounded":
        page = _lexical_page(capped=False, bounded_mode=True)
    elif shape == "lexical_capped":
        page = _lexical_page(capped=True, bounded_mode=False)
    elif shape == "not_a_total":
        page = _counted_page("search", "unknown", limit=6)
        page["not_a_total"] = {"reason": "scan_bound", "records_read": 50,
                               "for_a_total": None}
    elif shape == "reference_read":
        page = _counted_page("search", "exact", limit=6)
        page.update(retrieval_basis="result_reference_members", reference_members=40)
        page.pop("coverage")
        page = with_read_coverage(page)
    else:
        operator_name = "aggregate"
        page = _counted_page("aggregate", "exact", limit=6)
    measure = public_call_tool_result_bytes
    fixed = response_bounds._count_stable(
        operator_name, measure, may_vary=lambda: False,
    )
    assert fixed(page) == measure(page)
    # The fast size equals the full five renders where the forms are built.
    varying = response_bounds._count_stable(operator_name, measure)
    assert varying(page) == _full_stable(operator_name, measure, page)


def test_a_page_with_no_bounded_count_is_cut_as_on_dev_and_one_with_it_is_not() -> None:
    """At a budget equal to a page's own size the page is served whole when
    its count is fixed; the same page from a bounded COUNT is paged down,
    because its other count forms are larger."""
    page = _counted_search(outcome="exact")(query="q", limit=12)
    budget = public_call_tool_result_bytes(page)
    assert budget >= 4_096
    fixed = bound_read_tool("search", _counted_search(outcome="exact", noted=False))(
        query="q", limit=12, max_bytes=budget,
    )
    assert fixed == page
    counted = bound_read_tool("search", _counted_search(outcome="exact"))(
        query="q", limit=12, max_bytes=budget,
    )
    assert counted.get("truncated") is True and "response_byte_budget" in counted
    assert len(counted["results"]) < 12


def test_a_page_with_mixed_type_dict_keys_is_sized_without_a_memo_error() -> None:
    """`json.dumps(sort_keys=True)` raises on keys of mixed types (and
    `default=` does not apply to keys); the memo key then is None and the
    size is computed directly, so a call never fails because of it."""
    measure = public_call_tool_result_bytes
    page = _counted_page("search", "exact", limit=6, row_chars=20)
    page["by_kind"] = {1: "a", "b": 2}
    stable = response_bounds._count_stable("search", measure)
    assert stable(page) == _full_stable("search", measure, page)
    assert stable(page) == stable(page)


def test_row_flags_are_kept_in_the_stand_in_and_the_size_stays_exact() -> None:
    """A row-level bool (hidden, redacted, dropped) may change how a row is
    shown; the stand-in keeps it, and the fast size still equals the full."""
    measure = public_call_tool_result_bytes
    for outcome in _OUTCOMES:
        page = _counted_page("search", outcome, limit=6, row_chars=20)
        for i, row in enumerate(page["results"]):
            row["hidden"] = bool(i % 2)
            row["attrs"] = {"nomos_redacted": bool(i % 3), "text": "t"}
        stable = response_bounds._count_stable("search", measure)
        assert stable(page) == _full_stable("search", measure, page), outcome
    stand = response_bounds._placeholder({"a": True, "b": False, "c": 3, "d": "x"})
    assert stand == {"a": True, "b": False, "c": 0, "d": ""}


def test_the_exact_note_is_not_added_for_a_page_that_stopped_at_the_paging_cap() -> None:
    """server.py gives the exact count (and its note) only with a cursor; a
    page with `pagination_capped` never has the note, so no form adds it."""
    page = _counted_page("search", "unknown", limit=6)
    page["pagination_capped"] = True
    forms = response_bounds._count_variants("search", page, exact_note=True)
    assert not any(f.get("notes") for f in forms)


def test_a_grep_page_is_sized_as_it_is() -> None:
    """grep counts on its own (grep_search.py `count_grep_matches`), with
    other count fields; it never notes a bounded COUNT, so the fit does not
    add count forms to it."""
    page = _counted_page("search", "unknown", limit=6)
    page.update(method="grep", total_count_basis="timed_out",
                count_basis_reasons=["timeout"], page_basis="timed_out")
    measure = public_call_tool_result_bytes

    def grep(query: str, limit: int = 20, offset: int = 0,
             generation_id: str | None = None, max_bytes: int | None = None):
        return _counted_page("search", "unknown", limit=limit, offset=offset)

    out = bound_read_tool("search", grep)(
        query="q", limit=12, max_bytes=measure(grep("q", limit=12)),
    )
    assert out == grep("q", limit=12)
    fixed = response_bounds._count_stable("search", measure, may_vary=lambda: False)
    assert fixed(page) == measure(page)
