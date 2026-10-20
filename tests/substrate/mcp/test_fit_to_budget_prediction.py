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
