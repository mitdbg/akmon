from __future__ import annotations

from tests.substrate.mcp._result_payload import (
    payload as _result_payload,
    result_text as _result_text,
)

import json
from typing import Any

import anyio
import psycopg
import pytest
from mcp.shared.memory import create_connected_server_and_client_session

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.mcp.operators import OPERATOR_TOOLS
from okg.substrate.mcp.operator_models import AggregateScope, NodeAnchor
from okg.substrate.mcp.response_bounds import (
    BYTE_TRUNCATION_REASON,
    MAX_RESPONSE_BYTES,
    ByteBoundedFastMCP,
    bound_read_tool,
    public_call_tool_result_bytes,
    served_text,
)
from okg.substrate.mcp.server import build_server
from okg.substrate.mcp.tracing import _extract_call_cost
from okg.substrate.publisher import Publisher
from tests.substrate._db_helpers import seed_catalog_version
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402
from tests.substrate.mcp._result_payload import (
    json_client_sessions,
)



@pytest.fixture(autouse=True)
def _json_result_format(monkeypatch):
    """Program-side test: every call here reads the JSON result format
    (issue #3059), through request `_meta` or the in-process scope."""
    json_client_sessions(monkeypatch)


def test_multibyte_accounting_measures_the_public_call_tool_result() -> None:
    payload = {"items": [{"text": "é" * 200}], "truncated": False}

    # The served form of a `filter` result, as the tracer measures it.
    measured = public_call_tool_result_bytes(payload, tool="filter")

    assert measured > len(json.dumps(payload, ensure_ascii=False).encode("utf-8"))
    assert measured > len(str(payload))

    trace_cost = _extract_call_cost(
        args={},
        response=payload,
        elapsed_ms=1,
        result_count=1,
        error_code=None,
        tool_name="filter",
    )
    assert trace_cost["response_bytes"] == measured


def test_page_retry_preserves_the_native_cursor() -> None:
    calls: list[tuple[int, int, str | None]] = []

    def expand(
        input: Any,
        limit: int = 4,
        offset: int = 0,
        generation_id: str | None = None,
        depth: int = 1,
        path: list[str] | None = None,
        max_bytes: int = 6_000,
    ) -> dict[str, Any]:
        calls.append((limit, offset, generation_id))
        return {
            "neighbors": [
                {"edge_id": f"edge:{offset + i}", "attrs": {"text": "é" * 900}}
                for i in range(limit)
            ],
            "limit": limit,
            "offset": offset,
            # Simulate Nomos consuming two raw positions per visible row.
            "next_offset": offset + (2 * limit),
            "truncated": True,
            "pinned_generation_id": generation_id or "gen:pinned",
        }

    bounded = bound_read_tool("expand", expand)
    out = bounded(input={"node_id": "n:start"}, limit=4, max_bytes=6_000)

    assert out["truncated"] is True
    assert out["truncation_reason"] == "response_byte_budget"
    assert out["effective_limit"] < 4
    assert out["next_offset"] == 2 * out["effective_limit"]
    assert public_call_tool_result_bytes(out) <= 6_000
    assert calls[0] == (4, 0, None)
    assert all(pin == "gen:pinned" for _limit, _offset, pin in calls[1:])


def test_single_wide_row_is_cut_and_unpaged_payload_offers_a_served_retry() -> None:
    """#3666 step 3b: size alone never refuses. A one-row page over its
    budget is served with that row cut and marked `cut`, its cursor kept; a
    payload with no row to cut stays refused, with the same call at a
    budget it fits, and that call is served."""
    def search(
        query: str,
        limit: int = 1,
        generation_id: str | None = None,
        max_bytes: int = 4_096,
    ) -> dict[str, Any]:
        # The wide value is the row's excerpt: a search row in text prints
        # its passage (or, without one, its excerpt) and no other row key
        # (#3666), so a wide `text` key no longer weighs on the reply.
        return {
            "results": [{"node_id": "doc:1", "excerpt": "é" * 10_000}],
            "limit": limit,
            "offset": 0,
            "next_offset": 1,
            "truncated": True,
            "pinned_generation_id": generation_id or "gen:one",
        }

    one_row = bound_read_tool("search", search)(query="wide", max_bytes=4_096)
    assert one_row.get("status") != "refused", one_row
    assert one_row["truncated"] is True
    assert one_row["truncation_reason"] == "response_byte_budget"
    assert one_row["next_offset"] == 1
    row = one_row["results"][0]
    assert row["node_id"] == "doc:1"
    assert row["values_cut"]["excerpt"]["chars"] == 10_000
    assert 0 < row["values_cut"]["excerpt"]["served_chars"] < 10_000
    assert row["excerpt"] == "é" * row["values_cut"]["excerpt"]["served_chars"]
    assert public_call_tool_result_bytes(one_row) <= 4_096

    def inspect(
        target: dict[str, Any],
        max_bytes: int = 4_096,
    ) -> dict[str, Any]:
        return {"node": {"attrs": {"text": "x" * 10_000}}}

    read = bound_read_tool("inspect", inspect)
    unpaged = read(target={"kind": "node"}, max_bytes=4_096)
    assert unpaged["status"] == "refused"
    assert unpaged["error"]["code"] == "response_byte_budget_exceeded"
    assert "continuation cursor" in unpaged["error"]["message"]
    assert public_call_tool_result_bytes(unpaged) <= 4_096
    retry = unpaged["error"]["retry_call"]
    assert retry["tool"] == "inspect" and retry["mode"] == "corrected"
    assert retry["arguments"]["target"] == {"kind": "node"}
    assert retry["arguments"]["max_bytes"] > unpaged["error"]["needed_bytes"]
    served = read(**retry["arguments"])
    assert served.get("status") != "refused", served
    assert served["node"]["attrs"]["text"] == "x" * 10_000


def test_a_size_refusal_above_the_ceiling_carries_no_retry_call() -> None:
    """The offered budget must be one the server accepts: past the
    ceiling the refusal keeps its words and offers no call (failure case)."""
    def inspect(target: dict[str, Any], max_bytes: int = 4_096) -> dict[str, Any]:
        return {"node": {"attrs": {"text": "x" * 10_000}}}

    refused = bound_read_tool(
        "inspect", inspect, deployment_ceiling_bytes=8_192,
    )(target={"kind": "graph"}, max_bytes=4_096)
    assert refused["status"] == "refused"
    assert refused["error"]["code"] == "response_byte_budget_exceeded"
    assert refused["error"]["needed_bytes"] > 8_192
    assert "retry_call" not in refused["error"]


# ── read-passage-rows-3666: R9 (no retry above the default budget), D3/D4 ──


def _search_rows(
    texts: list[str], *, calls: list[int] | None = None, title: str = "a title",
    fields: list[list[str]] | None = None, **extra: Any,
) -> Any:
    """A fake search whose rows carry a matched passage (`passage.text`)
    with the match, MARK, in the middle of each text. `calls` collects the
    limit of each run."""
    def search(
        query: str = "q",
        limit: int = 10,
        offset: int = 0,
        select: list[str] | None = None,
        max_chars: int | None = None,
        generation_id: str | None = None,
        max_bytes: int | None = None,
    ) -> dict[str, Any]:
        if calls is not None:
            calls.append(limit)
        rows = []
        for index, text in enumerate(texts[offset:offset + limit]):
            row: dict[str, Any] = {
                "node_id": f"rec:{offset + index}",
                "head": {"kind": "ticket", "id": f"rec:{offset + index}",
                         "fields": fields or [["status", "Open"]], "title": title},
                "passage": {"text": text, "whole": True, "where": "description"},
            }
            if select and "attrs" in select:
                row["attrs"] = {"description": text, "status": "Open"}
            rows.append(row)
        more = offset + limit < len(texts)
        return {"results": rows, "limit": limit, "offset": offset,
                "next_offset": offset + limit if more else None,
                "pinned_generation_id": "gen:1", **extra}

    return search


def _middle_marked(n: int) -> str:
    half = "a" * (n // 2)
    return half + "MARK" + half


def _names_max_bytes(error: dict[str, Any]) -> bool:
    words = " ".join(str(error.get(key) or "") for key in ("remedy", "fix_hint", "message_hint"))
    call = error.get("retry_call") or {}
    return "max_bytes" in words or "max_bytes" in (call.get("arguments") or {})


def test_a_search_page_over_budget_keeps_every_row_and_shortens_passages() -> None:
    """D4: ten rows whose passages do not fit 8 KiB are all served, each
    passage cut to one shared length around its middle, and the page says
    the length. No row is dropped and no cursor is invented."""
    search = bound_read_tool("search", _search_rows([_middle_marked(3_000)] * 10))
    page = search(query="q", limit=10, max_bytes=8_192)
    assert page.get("status") != "refused", page
    assert [row["node_id"] for row in page["results"]] == [f"rec:{i}" for i in range(10)]
    kept = page["passages_shortened_to"]
    assert 0 < kept < 3_004
    for row in page["results"]:
        assert len(row["passage"]["text"]) == kept
        assert "MARK" in row["passage"]["text"]  # the middle is kept
        assert row["passage"]["whole"] is False
    assert page["next_offset"] is None
    assert public_call_tool_result_bytes(page, tool="search") <= 8_192


def test_a_search_page_that_fits_is_not_marked_shortened() -> None:
    """Failure case for the mark: a page within its budget is served as it
    is, with no `passages_shortened_to`."""
    page = bound_read_tool("search", _search_rows(["short text"] * 3))(query="q", limit=3)
    assert "passages_shortened_to" not in page
    assert page["results"][0]["passage"]["text"] == "short text"


def test_a_search_page_falls_back_to_head_lines_before_dropping_a_row() -> None:
    """D4: when no passage length fits, the rows are served as head lines
    (`passages_shortened_to` 0), still every row. The head lines must
    outweigh the smallest budget the server accepts (4 KiB): a smaller
    max_bytes is raised to it, and 40 short rows fit it with passages."""
    search = _search_rows([_middle_marked(400)] * 100)
    bounded = bound_read_tool("search", search)
    whole = search(query="q", limit=100)
    heads = {**whole, "results": [{**row, "passage": None} for row in whole["results"]],
             "passages_shortened_to": 0}
    budget = public_call_tool_result_bytes(heads, tool="search") + 40
    assert budget > 4_096
    page = bounded(query="q", limit=100, max_bytes=budget)
    assert page.get("status") != "refused", page
    assert len(page["results"]) == 100
    assert page["passages_shortened_to"] == 0
    assert all(row["passage"] is None for row in page["results"])
    assert "head_lines_shortened" not in page


def _ticket_search(n: int, *, title: str, url: bool = True, source: bool = True) -> Any:
    """A fake search of `n` ticket rows shaped like a real ticket search
    page: a head line with dates and a title, a citation URL, a row source
    name, and a 400-character matched passage."""
    def search(
        query: str = "q", limit: int = 10, offset: int = 0,
        select: list[str] | None = None, max_chars: int | None = None,
        generation_id: str | None = None, max_bytes: int | None = None,
    ) -> dict[str, Any]:
        rows = []
        for i in range(offset, min(offset + limit, n)):
            row: dict[str, Any] = {
                "node_id": f"ticket:T-{i}",
                "head": {"kind": "ticket", "id": f"ticket:T-{i}",
                         "fields": [["status", "Closed"], ["created", "2025-02-11"],
                                    ["updated", "2025-11-17"]],
                         "title": title,
                         "citation_url": f"https://tickets.example/browse/T-{i}" if url else None},
                "passage": {"text": _middle_marked(400), "whole": True, "where": "description"},
            }
            if source:
                row["provenance"] = {"source": "tickets.jira", "trust": "trusted"}
            rows.append(row)
        more = offset + limit < n
        return {"results": rows, "limit": limit, "offset": offset,
                "next_offset": offset + limit if more else None,
                "pinned_generation_id": "gen:1"}

    return search


def _without(page: dict[str, Any], *, links: bool = False, passages: bool = True) -> dict[str, Any]:
    rows = []
    for row in page["results"]:
        row = {**row, "passage": None} if passages else dict(row)
        if links:
            row["head"] = {**row["head"], "citation_url": None}
        rows.append(row)
    return {**page, "results": rows, "passages_shortened_to": 0}


def test_a_ticket_page_whose_head_lines_overflow_drops_source_links_not_rows() -> None:
    """D4, the shape of a real 50-ticket search that served 48 rows: with
    the passages dropped, 50 head lines plus their `source: <url>` lines
    are over a 12,000-byte max_bytes (about 230 bytes a row). The server
    leaves the source links out and serves all 50 rows; the reply says so
    and prints no `source:` line."""
    search = _ticket_search(50, title="Deletion of container level production rules")
    whole = search(query="q", limit=50)
    with_links = public_call_tool_result_bytes(_without(whole), tool="search")
    no_links = public_call_tool_result_bytes(_without(whole, links=True), tool="search")
    budget = (with_links + no_links) // 2
    assert budget > 4_096
    page = bound_read_tool("search", search)(query="q", limit=50, max_bytes=budget)
    assert page.get("status") != "refused", page
    assert [row["node_id"] for row in page["results"]] == [f"ticket:T-{i}" for i in range(50)]
    assert page["passages_shortened_to"] == 0
    assert page["head_lines_shortened"] == {"source_links": True}
    assert page["next_offset"] is None
    text = served_text(page, tool="search")
    status = text.split("\n", 1)[0]
    assert "head lines shortened to fit: no source links" in status, status
    assert "    source: " not in text
    assert "[50] ticket:T-49" in text
    assert public_call_tool_result_bytes(page, tool="search") <= budget


def test_a_page_whose_head_lines_still_overflow_cuts_titles_evenly() -> None:
    """D4: after the source links and names are left out, long titles are
    cut to one shared length (an ellipsis marks each cut) before any row
    is dropped; the reply names the sources once and the title length."""
    long_title = "A very long ticket title " * 6
    search = _ticket_search(60, title=long_title)
    whole = search(query="q", limit=60)
    no_links = public_call_tool_result_bytes(_without(whole, links=True), tool="search")
    budget = no_links - 60 * 80
    assert budget > 4_096
    page = bound_read_tool("search", search)(query="q", limit=60, max_bytes=budget)
    assert page.get("status") != "refused", page
    assert len(page["results"]) == 60
    said = page["head_lines_shortened"]
    assert said["source_links"] is True and said["source_names"] is True
    kept = said["title_chars"]
    assert 24 <= kept < len(long_title)
    for row in page["results"]:
        assert len(row["head"]["title"]) <= kept
        assert row["head"]["title"].endswith("…")
        assert row["provenance"]["source"] is None
    assert page["row_sources"] == ["tickets.jira"]
    status = served_text(page, tool="search").split("\n", 1)[0]
    assert f"titles cut to {kept} chars" in status, status
    assert "source tickets.jira" in status, status
    assert public_call_tool_result_bytes(page, tool="search") <= budget


def test_a_whole_attrs_search_page_over_budget_keeps_every_requested_row() -> None:
    """Gate 1 (D6): `select` naming whole `attrs` serves every attribute
    string; over the budget each is shortened like the passages, and every
    requested row stays."""
    search = bound_read_tool("search", _search_rows([_middle_marked(2_000)] * 8))
    page = search(query="q", limit=8, select=["node_id", "attrs"], max_bytes=8_192)
    assert page.get("status") != "refused", page
    assert len(page["results"]) == 8
    kept = page["passages_shortened_to"]
    for row in page["results"]:
        assert len(row["attrs"]["description"]) <= kept
        assert row["attrs"]["status"] == "Open"
    assert "effective_limit" not in page


def test_a_search_whose_head_lines_do_not_fit_is_read_again_with_fewer_rows() -> None:
    """D4: only when even the head lines of every requested row exceed the
    budget is the search read again, at the largest limit whose head lines
    fit, with its text shortened and `offset` for the rest. Never a slice of
    the first page: the served page is a rerun at that limit."""
    calls: list[int] = []
    search = _search_rows(["x" * 200] * 200, calls=calls)
    page = bound_read_tool("search", search, deployment_default_bytes=8_192)(
        query="q", limit=200,
    )
    assert page.get("status") != "refused", page
    shown = len(page["results"])
    assert 1 <= shown < 200
    # The first run, then at most four reruns, the last of which is served.
    assert calls[0] == 200 and calls[-1] == shown and len(calls) <= 5, calls
    assert page["next_offset"] == shown
    assert public_call_tool_result_bytes(page, tool="search") <= 8_192


def test_a_search_row_whose_head_line_alone_does_not_fit_is_refused() -> None:
    """D4 failure case: one row whose head line alone exceeds the budget
    even at its shortest cannot be shortened or paged. A long title is cut
    (see the test above); a head field is not. It is refused, and the
    refusal names no max_bytes (D3)."""
    search = _search_rows(["x" * 200], fields=[["status", "s" * 9_000]])
    refused = bound_read_tool("search", search)(query="q", limit=1, max_bytes=4_096)
    assert refused["status"] == "refused", refused
    error = refused["error"]
    assert error["code"] == "response_byte_budget_exceeded"
    assert not _names_max_bytes(error), error
    assert error["remedy"] == "pass a smaller limit and page with offset"


def test_no_search_size_refusal_names_max_bytes() -> None:
    """Gate 1 (D3): search does not publish max_bytes, so no refusal of a
    search names it, in words or in its retry, even when the caller sent a
    small max_bytes that a larger budget would serve."""
    def search(query: str = "q", limit: int = 1, select: list[str] | None = None,
               max_bytes: int | None = None) -> dict[str, Any]:
        return {"results": [{"node_id": "n" * 6_000}], "limit": limit, "offset": 0,
                "next_offset": None}

    for arguments in ({"query": "q"}, {"query": "q", "select": ["node_id"]}):
        refused = bound_read_tool("search", search)(**arguments, max_bytes=4_096)
        assert refused["status"] == "refused", refused
        assert not _names_max_bytes(refused["error"]), refused["error"]


def test_a_refusal_past_the_default_budget_offers_a_narrower_call() -> None:
    """R9: an inspect(node) needing more than the 64 KiB default is not
    told to pass a larger max_bytes; its retry reads the record's text page
    by page. Under the default (the case above) the old retry stands."""
    def inspect(target: dict[str, Any], max_bytes: int | None = None) -> dict[str, Any]:
        return {"node": {"attrs": {"text": "x" * 100_000}}}

    read = bound_read_tool("inspect", inspect)
    refused = read(target={"kind": "node", "node_id": "rec:1"}, max_bytes=8_192)
    assert refused["status"] == "refused", refused
    error = refused["error"]
    assert error["needed_bytes"] > 65_536
    assert error["retry_call"] == {
        "tool": "inspect", "mode": "corrected",
        "arguments": {"target": {"kind": "text", "node_id": "rec:1"}},
    }
    assert "max_bytes of at least" not in error["remedy"]
    assert "65536-byte default" in error["remedy"]
    # The deployment default is the line, not the substrate maximum.
    lower = bound_read_tool("inspect", inspect, deployment_default_bytes=16_384)
    error = lower(target={"kind": "node", "node_id": "rec:1"}, max_bytes=4_096)["error"]
    assert error["retry_call"]["arguments"] == {"target": {"kind": "text", "node_id": "rec:1"}}


def test_a_refusal_within_the_default_budget_still_offers_it() -> None:
    """R9 keeps the old retry when the budget it needs is within the
    default: a caller who asked for 4 KiB is offered the size that fits."""
    def inspect(target: dict[str, Any], max_bytes: int | None = None) -> dict[str, Any]:
        return {"node": {"attrs": {"text": "x" * 20_000}}}

    error = bound_read_tool("inspect", inspect)(
        target={"kind": "graph"}, max_bytes=4_096,
    )["error"]
    offered = error["retry_call"]["arguments"]["max_bytes"]
    assert error["needed_bytes"] < offered <= 65_536
    assert f"pass max_bytes of at least {offered}" in error["remedy"]


def test_a_query_retry_never_asks_past_the_default_budget() -> None:
    """R9 for query: a one-row page needing more than the default carries no
    larger-budget retry; its remedy names the narrower SQL forms."""
    def query(sql: str, max_bytes: int | None = None) -> dict[str, Any]:
        return {"rows": [{"t": "y" * 90_000}], "row_count": 1}

    refused = bound_read_tool("query", query)(sql="SELECT t FROM x", max_bytes=16_384)
    assert refused["status"] == "refused", refused
    error = refused["error"]
    assert "retry_call" not in error, error
    assert "LIMIT" in error["remedy"]
    assert "max_bytes of at least" not in error["remedy"]


def test_a_cut_row_keeps_the_rows_identity_and_its_cursor() -> None:
    """A filter page of one wide row: `node_id`, `subtype` and the cursor
    stay whole, the largest attribute is cut first, and a list is left out
    whole when cutting text is not enough."""
    def filter_rows(
        input: Any, predicate: dict[str, Any], limit: int = 5, offset: int = 0,
        max_bytes: int = 4_096,
    ) -> dict[str, Any]:
        return {
            "items": [
                {"node_id": f"n:{i}", "subtype": "doc", "attrs": {
                    "title": "short title",
                    "body": "b" * 9_000,
                    "tags": ["t" * 50] * 40,
                }}
                for i in range(offset, offset + limit)
            ],
            "limit": limit, "offset": offset,
            "next_offset": offset + limit, "truncated": True,
        }

    out = bound_read_tool("filter", filter_rows)(
        input={"kind": "nodes"}, predicate={}, limit=5, max_bytes=4_096,
    )
    assert out.get("status") != "refused", out
    assert out["next_offset"] == 1
    (row,) = out["items"]
    assert (row["node_id"], row["subtype"]) == ("n:0", "doc")
    assert row["attrs"]["title"] == "short title"
    assert set(row["values_cut"]) <= {"attrs.body", "attrs.tags"}
    assert row["values_cut"]["attrs.body"]["chars"] == 9_000
    assert public_call_tool_result_bytes(out, tool="filter") <= 4_096


def test_the_ontology_over_its_budget_is_served_with_metadata_cut() -> None:
    """Measured: the comp-ops ontology needed 68,706 B against the 65,536 B
    default and was refused on every call. Every subtype name and every
    narrowing is served; the largest metadata goes first, each marked."""
    def inspect(target: dict[str, Any], max_bytes: int = 4_096) -> dict[str, Any]:
        return {
            "catalog_version_id": 1,
            "subtypes": [
                {"name": f"type_{i:02d}", "metadata": {"description": "d" * (300 * (i % 5))}}
                for i in range(30)
            ],
            "narrowings": [{"parent": "type_00", "edge": "links", "child": "type_01"}],
            "alias_count": 0,
        }

    out = bound_read_tool("inspect", inspect)(target={"kind": "ontology"}, max_bytes=8_192)
    assert out.get("status") != "refused", out
    assert [s["name"] for s in out["subtypes"]] == [f"type_{i:02d}" for i in range(30)]
    assert out["narrowings"] == [{"parent": "type_00", "edge": "links", "child": "type_01"}]
    cut = [s for s in out["subtypes"] if s.get("cut") == "metadata"]
    assert cut and all(s["metadata"] is None for s in cut)
    assert out["metadata_cut"]["subtypes"] == len(cut)
    # The largest went first: no kept metadata is larger than a cut one was.
    kept = [len(s["metadata"]["description"]) for s in out["subtypes"] if s.get("cut") is None]
    assert max(kept) <= min(300 * (int(s["name"][-2:]) % 5) for s in cut)
    assert out["truncated"] is True
    assert public_call_tool_result_bytes(out, tool="inspect") <= 8_192


def test_empty_retry_with_nonprogressing_cursor_is_refused() -> None:
    def filter_rows(
        input: Any,
        predicate: dict[str, Any],
        limit: int = 3,
        offset: int = 0,
        max_bytes: int = 4_096,
    ) -> dict[str, Any]:
        if limit == 1:
            return {
                "items": [],
                "limit": limit,
                "offset": offset,
                "next_offset": offset,
                "truncated": True,
                "padding": "",
            }
        return {
            "items": [{"text": "x" * 5_000} for _ in range(limit)],
            "limit": limit,
            "offset": offset,
            "next_offset": offset + limit,
            "truncated": True,
        }

    out = bound_read_tool("filter", filter_rows)(
        input={"kind": "nodes"},
        predicate={},
        max_bytes=4_096,
    )

    assert out["status"] == "refused"
    assert out["error"]["code"] == "response_byte_budget_exceeded"


@pytest.mark.parametrize(
    ("operator_name", "arguments", "pageable"),
    [
        ("search", {"query": "wide"}, True),
        ("filter", {"input": {"kind": "nodes"}, "predicate": {}}, True),
        ("map", {"input": {"kind": "nodes"}, "projection": ["attrs"]}, True),
        ("expand", {"input": {"node_id": "n:start"}}, True),
        (
            "aggregate",
            {"input": {"kind": "nodes"}, "by": ["attrs.kind"]},
            True,
        ),
        ("inspect", {"target": {"kind": "node"}}, False),
        ("query", {"sql": "SELECT * FROM okg.v_nodes"}, False),
        (
            "expand",
            {"input": {"node_id": "n:start"}, "path": ["contains:out"]},
            False,
        ),
        (
            "expand",
            {"input": {"node_id": "n:start"}, "depth": 2},
            False,
        ),
        (
            "aggregate",
            {
                "input": {"kind": "edges"},
                "by": ["src_subtype", "edge_type", "dst_subtype"],
            },
            False,
        ),
    ],
)
def test_operator_paging_classification_is_explicit(
    operator_name: str,
    arguments: dict[str, Any],
    pageable: bool,
) -> None:
    def read_tool(
        input: Any = None,
        query: str | None = None,
        predicate: dict[str, Any] | None = None,
        projection: Any = None,
        sql: str | None = None,
        target: dict[str, Any] | None = None,
        by: list[str] | None = None,
        limit: int = 3,
        offset: int = 0,
        generation_id: str | None = None,
        depth: int = 1,
        path: list[str] | None = None,
        max_bytes: Any = 4_096,
    ) -> dict[str, Any]:
        del input, query, predicate, projection, sql, target, by, depth, path, max_bytes
        # `excerpt`, which every shape prints (a search row prints no other
        # row key, #3666), so each operator's reply weighs the same.
        return {
            "items": [
                {"id": f"item:{offset + index}", "excerpt": "é" * 1_200}
                for index in range(limit)
            ],
            "limit": limit,
            "offset": offset,
            "next_offset": offset + limit,
            "truncated": True,
            "pinned_generation_id": generation_id or "gen:matrix",
        }

    out = bound_read_tool(operator_name, read_tool)(
        **arguments,
        limit=3,
        max_bytes=4_096,
    )

    assert public_call_tool_result_bytes(out) <= 4_096
    if pageable:
        assert out["truncation_reason"] == "response_byte_budget"
        assert out["effective_limit"] < 3
        assert out["next_offset"] > out["offset"]
    else:
        assert out["status"] == "refused"
        assert out["error"]["code"] == "response_byte_budget_exceeded"


def test_invalid_or_over_ceiling_budget_is_refused_without_running_tool() -> None:
    calls = 0

    def inspect(
        target: dict[str, Any],
        max_bytes: int = 64 * 1024,
    ) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return {"target": target}

    bounded = bound_read_tool("inspect", inspect)
    too_large = bounded(target={"kind": "graph"}, max_bytes=MAX_RESPONSE_BYTES + 1)
    wrong_type = bounded(target={"kind": "graph"}, max_bytes="x" * 100_000)

    assert too_large["error"]["code"] == "invalid_response_byte_budget"
    assert wrong_type["error"]["code"] == "invalid_response_byte_budget"
    assert public_call_tool_result_bytes(wrong_type) < 4_096
    # An integer under the floor is raised, not refused (#3153): see
    # test_a_budget_under_the_floor_is_raised_said_and_still_fits.
    assert calls == 0


def test_public_call_bounds_an_oversized_operator_exception() -> None:
    server = ByteBoundedFastMCP("bounded-error-test")

    def query(
        sql: str,
        max_bytes: Any = 4_096,
    ) -> dict[str, Any]:
        del sql, max_bytes
        raise RuntimeError("database rejected request: " + ("x" * 100_000))

    server.tool(name="query", structured_output=False)(
        bound_read_tool("query", query)
    )

    async def drive() -> None:
        async with create_connected_server_and_client_session(server) as session:
            result = await session.call_tool(
                "query",
                {"sql": "SELECT broken", "max_bytes": 4_096},
            )

        encoded = result.model_dump_json(
            by_alias=True,
            exclude_none=True,
        ).encode("utf-8")
        assert len(encoded) <= 4_096
        assert result.isError is True
        # Issue #3059: the result is sent ONCE, as one text block (here the
        # JSON result format), counted inside the same 4 KiB above.
        assert result.structuredContent is None
        assert len(result.content) == 1
        assert '"operator_execution_failed"' in result.content[0].text
        payload = _result_payload(result)
        assert payload["status"] == "refused"
        assert payload["error"]["code"] == "operator_execution_failed"
        assert "x" * 100 not in _result_text(result)

    anyio.run(drive)


@pytest.mark.parametrize("cut_at_cap", [True, False])
def test_a_query_refusal_offers_a_larger_budget_only_below_the_tool_cap(
    cut_at_cap: bool,
) -> None:
    """Issue #3666 (M12 on 3b): a `query` page the tool itself cut at its
    256 KiB cap (`payload_truncated`) cannot be served whole at any budget,
    so its size refusal carries no retry call; 3b's general retry must not
    add one. Cut only by the caller's max_bytes, the refusal carries the
    same query at a budget it fits (failure case: the cap). The remedy says
    the same, in the JSON and the text the model reads: at the cap it never
    says to pass a larger max_bytes."""
    if cut_at_cap:
        # Rows of about 80 KB: three fit the tool's cap, the rest were cut.
        rows = [[f"n:{i}", "y" * 80_000] for i in range(3)]
        budget = 65_536
    else:
        rows = [["n:big", "y" * 20_000]]
        budget = 4_096
    out: dict[str, Any] = {}

    def query(sql: str, max_bytes: Any = None) -> dict[str, Any]:
        del max_bytes
        page: dict[str, Any] = {
            "columns": ["node_id", "t"], "rows": rows, "returned_count": len(rows),
        }
        if cut_at_cap:
            page.update(
                truncated=True, payload_truncated=True,
                payload_byte_cap=262_144, payload_rows_omitted=12,
            )
        return page

    server = ByteBoundedFastMCP("query-cap-retry-test")
    server.tool(name="query", structured_output=False)(
        bound_read_tool("query", query)
    )

    async def drive() -> None:
        async with create_connected_server_and_client_session(server) as session:
            call = {"sql": "SELECT node_id, t FROM big", "max_bytes": budget}
            out["refused"] = _result_payload(await session.call_tool("query", call))
            # No result-format meta: the plain text a model reads.
            out["text"] = _result_text(await session.call_tool("query", call, meta={}))
            retry = out["refused"]["error"].get("retry_call")
            if retry is not None:
                out["served"] = _result_payload(
                    await session.call_tool("query", retry["arguments"])
                )

    anyio.run(drive)
    refused = out["refused"]
    assert refused["status"] == "refused"
    assert refused["error"]["code"] == "response_byte_budget_exceeded"
    remedy, text = refused["error"]["remedy"], out["text"]
    assert text.startswith("query refused response_byte_budget_exceeded"), text
    # Each format measures its own size, so the numbers differ; the words do not.
    for words in ("LIMIT", "fewer columns", "left(attrs->>'text', 200)"):
        assert words in remedy and words in text, (remedy, text)
    if cut_at_cap:
        assert "retry_call" not in refused["error"], refused["error"]
        assert refused["payload_truncated"] is True
        assert refused["payload_byte_cap"] == 262_144
        assert "served" not in out
        for words in (
            "262144-byte payload cap was reached",
            "a larger max_bytes will not help",
            "page by key",
        ):
            assert words in remedy and words in text, (remedy, text)
        assert "pass max_bytes" not in remedy and "pass max_bytes" not in text, text
        return
    assert "cap was reached" not in text, text
    retry = refused["error"]["retry_call"]
    assert f"pass max_bytes of at least {retry['arguments']['max_bytes']}" in remedy
    assert "pass max_bytes of at least" in text, text
    assert retry["tool"] == "query" and retry["mode"] == "corrected"
    assert retry["arguments"]["max_bytes"] > refused["error"]["needed_bytes"]
    assert out["served"].get("status") != "refused", out["served"]
    assert out["served"]["rows"] == rows


def test_untyped_operator_exception_is_logged_under_the_refusals_error_id(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Issue #2793: an untyped failure used to leave no server-side trace.

    The refusal carries only an id, never the exception text; the log line
    carries the same id and the traceback, so an operator can match them.
    """
    server = ByteBoundedFastMCP("untyped-error-log-test")

    def search(query: str, max_bytes: Any = 4_096) -> dict[str, Any]:
        del query, max_bytes
        raise ValueError("internal-detail-7f3a must stay server-side")

    server.tool(name="search", structured_output=False)(
        bound_read_tool("search", search)
    )

    async def drive() -> Any:
        async with create_connected_server_and_client_session(server) as session:
            return await session.call_tool("search", {"query": "x"})

    with caplog.at_level("ERROR", logger="okg.substrate.mcp.response_bounds"):
        result = anyio.run(drive)

    payload = _result_payload(result)
    assert result.isError is True
    assert payload["error"]["code"] == "operator_execution_failed"
    error_id = payload["error"]["data"]["error_id"]
    assert isinstance(error_id, str) and len(error_id) == 12
    assert "internal-detail-7f3a" not in _result_text(result)

    records = [
        record
        for record in caplog.records
        if getattr(record, "okg_error_id", None) == error_id
    ]
    assert len(records) == 1, [r.getMessage() for r in caplog.records]
    record = records[0]
    assert record.levelname == "ERROR"
    assert record.okg_operator == "search"
    assert error_id in record.getMessage()
    assert record.exc_info is not None
    rendered = caplog.text
    assert "Traceback" in rendered
    assert "ValueError: internal-detail-7f3a" in rendered


def test_public_call_preserves_a_bounded_typed_operator_exception() -> None:
    server = ByteBoundedFastMCP("bounded-typed-error-test")

    class InvalidPath(SubstrateError, ValueError):
        code = "invalid_path_direction"

    def expand(
        input: Any,
        max_bytes: Any = 4_096,
    ) -> dict[str, Any]:
        del input, max_bytes
        raise InvalidPath(
            "hop 1 uses unsupported direction 'sideways'",
            data={
                "hop_index": 1,
                "edge_type": "contains",
                "direction": "sideways",
                "pinned_generation_id": "gen:pinned",
                "authority_fingerprint": "authority:safe-digest",
            },
            fix_hint="use 'out' or 'in' and retry",
        )

    server.tool(name="expand", structured_output=False)(
        bound_read_tool("expand", expand)
    )

    async def drive() -> None:
        async with create_connected_server_and_client_session(server) as session:
            result = await session.call_tool(
                "expand",
                {"input": {"node_id": "n:start"}, "max_bytes": 4_096},
            )

        assert result.isError is True
        payload = _result_payload(result)
        assert payload["error"]["code"] == "invalid_path_direction"
        assert payload["error"]["data"]["hop_index"] == 1
        assert payload["error"]["data"]["pinned_generation_id"] == "gen:pinned"
        assert "operator_execution_failed" not in _result_text(result)
        assert public_call_tool_result_bytes(payload, is_error=True) <= 4_096

    anyio.run(drive)


def _publish_wide_rows(dsn: str) -> str:
    with psycopg.connect(dsn, autocommit=True) as conn:
        cv = seed_catalog_version(conn)
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.subtypes (name, metadata, catalog_version_id) "
                "VALUES ('wide_doc', '{}'::jsonb, %s)",
                (cv,),
            )
            cur.execute(
                "INSERT INTO okg.subtype_ancestors "
                "(catalog_version_id, subtype_name, ancestor_name, depth) "
                "VALUES (%s, 'wide_doc', 'wide_doc', 0)",
                (cv,),
            )
        facts = [
            NodeFact(
                node_id=f"wide:{index}",
                subtype="wide_doc",
                attrs={"text": (f"row-{index}:" + ("é" * 2_500))},
                source_record_id={"id": f"wide:{index}"},
                source_revision={"v": 1},
            )
            for index in range(4)
        ]
        ChronosGraphStore.from_connection(conn).apply_facts(
            facts,
            source="test:wide-response",
            strict_endpoints=True,
        )

    published = Publisher(dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()
    assert published["status"] == "published"
    assert published["nodes_added"] == 4
    return str(published["generation_id"])


@pytest.mark.postgres
def test_registered_filter_with_wide_rows_is_byte_bounded_and_reports_truncation(
    clean_runtime: str,
    request: pytest.FixtureRequest,
) -> None:
    generation_id = _publish_wide_rows(clean_runtime)
    server = build_server(dsn=clean_runtime)
    proof: dict[str, Any] = {}

    async def drive() -> None:
        async with create_connected_server_and_client_session(server) as session:
            listed = {tool.name: tool for tool in (await session.list_tools()).tools}
            for operator_name in OPERATOR_TOOLS:
                # read-passage-rows-3666 (D3): search accepts max_bytes but
                # no longer publishes it; its pages shorten text to fit.
                published = "max_bytes" in listed[operator_name].inputSchema["properties"]
                assert published is (operator_name != "search"), operator_name
                # Issue #3059: no operator publishes an output schema; the
                # result is sent once, as one text block, and the byte
                # bound measures that block.
                assert listed[operator_name].outputSchema is None

            invalid = await session.call_tool(
                "filter",
                {
                    "input": {"kind": "nodes"},
                    "predicate": {},
                    "max_bytes": "x" * 100_000,
                },
            )
            invalid_payload = _result_payload(invalid)
            assert invalid_payload["status"] == "refused"
            # Typed `Optional[int]` since issue #2156: the declared input
            # model refuses a non-integer by field name, and the refusal is
            # still byte-bounded (asserted below) although the argument was
            # 100 KB.
            assert invalid_payload["error"]["code"] == (
                "invalid_operator_arguments"
            )
            assert "max_bytes" in invalid_payload["error"]["data"][
                "invalid_fields"
            ]
            assert len(
                invalid.model_dump_json(
                    by_alias=True,
                    exclude_none=True,
                ).encode("utf-8")
            ) < 4_096

            first = await session.call_tool(
                "filter",
                {
                    "input": {"kind": "nodes", "subtype": "wide_doc"},
                    "predicate": {},
                    "select": ["node_id", "attrs"],
                    "generation_id": generation_id,
                    "limit": 4,
                    "max_bytes": 12_000,
                },
            )
            response_bytes = len(
                first.model_dump_json(
                    by_alias=True,
                    exclude_none=True,
                ).encode("utf-8")
            )
            assert response_bytes <= 12_000
            # Sent once, as one text block (issue #3059; here the JSON
            # result format), and that block is what the budget counts.
            assert first.structuredContent is None
            assert len(first.content) == 1
            payload = _result_payload(first)
            assert payload["truncated"] is True
            assert payload["truncation_reason"] == "response_byte_budget"
            assert payload["next_offset"] is not None
            assert payload["items"]
            assert "rows" not in payload
            assert "columns" not in payload
            proof.update(
                response_bytes=response_bytes,
                max_bytes=12_000,
                truncated=payload["truncated"],
                truncation_reason=payload["truncation_reason"],
                returned_items=len(payload["items"]),
                next_offset=payload["next_offset"],
                structured_content_present=first.structuredContent is not None,
            )

            second = await session.call_tool(
                "filter",
                {
                    "input": {"kind": "nodes", "subtype": "wide_doc"},
                    "predicate": {},
                    "select": ["node_id", "attrs"],
                    "generation_id": generation_id,
                    "limit": 4,
                    "offset": payload["next_offset"],
                    "max_bytes": 12_000,
                },
            )
            assert len(
                second.model_dump_json(
                    by_alias=True,
                    exclude_none=True,
                ).encode("utf-8")
            ) <= 12_000
            next_payload = _result_payload(second)
            first_ids = {item["node_id"] for item in payload["items"]}
            second_ids = {item["node_id"] for item in next_payload["items"]}
            assert first_ids.isdisjoint(second_ids)
            assert second_ids

    try:
        anyio.run(drive)
        for key, value in proof.items():
            request.node.user_properties.append((key, value))
    finally:
        server._okg_pool.close()
        if server._okg_write_pool:
            server._okg_write_pool.close()


def _search_over_the_wire(dsn: str, arguments: dict[str, Any]) -> dict[str, Any]:
    server = build_server(dsn=dsn)
    out: dict[str, Any] = {}

    async def drive() -> None:
        async with create_connected_server_and_client_session(server) as session:
            out["page"] = _result_payload(await session.call_tool("search", arguments))

    try:
        anyio.run(drive)
    finally:
        server._okg_pool.close()  # type: ignore[attr-defined]
    return out["page"]


@pytest.mark.postgres
def test_a_whole_attrs_search_over_the_wire_keeps_every_requested_row(
    clean_runtime: str,
) -> None:
    """Gate 1 (read-passage-rows-3666, D4/D6): four rows whose whole `attrs`
    need about 20 KB, under an 8 KiB budget. Every row is served, each
    attribute text shortened to the length the page states."""
    generation_id = _publish_wide_rows(clean_runtime)
    page = _search_over_the_wire(clean_runtime, {
        "where": {"type": "wide_doc"}, "select": ["node_id", "attrs"],
        "limit": 4, "max_bytes": 8_192, "generation_id": generation_id,
    })
    assert page.get("status") != "refused", page
    assert sorted(row["node_id"] for row in page["results"]) == [
        f"wide:{index}" for index in range(4)
    ]
    from okg.substrate.mcp.response_bounds import _text_length

    kept = page["passages_shortened_to"]
    assert 0 <= kept < 2_500
    for row in page["results"]:
        # The untrusted-data fence around the text is kept whole, uncounted.
        assert _text_length(row["attrs"]["text"]) <= kept


@pytest.mark.postgres
def test_a_dotted_select_is_served_over_the_wire(clean_runtime: str) -> None:
    """Gate 1 (D6): `select=["node_id", "attrs.text"]` is served, the key's
    value on each row, not refused as an unknown field."""
    generation_id = _publish_wide_rows(clean_runtime)
    page = _search_over_the_wire(clean_runtime, {
        "where": {"type": "wide_doc"}, "select": ["node_id", "attrs.text"],
        "limit": 4, "generation_id": generation_id,
    })
    assert page.get("status") != "refused", page
    assert len(page["results"]) == 4
    for row in page["results"]:
        selected = row["selected"]
        value = selected.get("attrs.text", selected.get("text"))
        # An untrusted row's value is fenced as its attrs are.
        assert isinstance(value, str) and "\nrow-" in "\n" + value, row


@pytest.mark.postgres
def test_registered_filter_pages_under_the_budget_in_the_text_format(
    clean_runtime: str,
    request: pytest.FixtureRequest,
) -> None:
    """The same wide-row page as above, read as a model reads it: the
    default text format. The served text fits the budget, says the page
    was cut, and its `next:` fact pages to new rows (review of 2102d20798:
    text paging under the budget had no PostgreSQL test)."""
    from okg.substrate.mcp import result_text

    generation_id = _publish_wide_rows(clean_runtime)
    server = build_server(dsn=clean_runtime)
    proof: dict[str, Any] = {}

    def next_offset(text: str) -> int:
        paging = result_text.fact_value(result_text.status_facts(text), "next:")
        assert isinstance(paging, str) and paging.startswith("offset="), text[:300]
        return int(paging[len("offset="):])

    async def drive() -> None:
        async with create_connected_server_and_client_session(server) as session:
            arguments = {
                "input": {"kind": "nodes", "subtype": "wide_doc"},
                "predicate": {},
                "select": ["node_id", "attrs"],
                "generation_id": generation_id,
                "limit": 4,
                "max_bytes": 12_000,
            }
            # `meta={}` names no format, so the server sends the text form.
            first = await session.call_tool("filter", arguments, meta={})
            served = len(first.model_dump_json(by_alias=True, exclude_none=True).encode())
            assert served <= 12_000
            assert first.isError is False
            assert first.structuredContent is None and len(first.content) == 1
            text = first.content[0].text
            assert result_text.parse_status_line(text) == result_text.StatusLine("filter", "ok")
            assert "\n! page cut to fit max_bytes=12000" in text, text[:400]
            first_ids = {card.ident for card in result_text.result_cards(text)}
            assert first_ids
            offset = next_offset(text)
            second = await session.call_tool(
                "filter", {**arguments, "offset": offset}, meta={},
            )
            assert len(
                second.model_dump_json(by_alias=True, exclude_none=True).encode()
            ) <= 12_000
            second_ids = {
                card.ident for card in result_text.result_cards(second.content[0].text)
            }
            assert second_ids and first_ids.isdisjoint(second_ids)
            proof.update(
                response_bytes=served, max_bytes=12_000, returned_rows=len(first_ids),
                next_offset=offset, second_page_rows=len(second_ids),
            )

    try:
        anyio.run(drive)
        for key, value in proof.items():
            request.node.user_properties.append((key, value))
    finally:
        server._okg_pool.close()
        if server._okg_write_pool:
            server._okg_write_pool.close()


def test_a_typed_aggregate_over_budget_pages_instead_of_refusing() -> None:
    """Regression guard for a defect the typing introduced (issue #2009).

    `_is_pageable` decides whether an over-budget result can be retried with
    a smaller page or has to be refused outright. It inspects the arguments
    with `.get(...)` and `isinstance(value, dict)` — and once
    `aggregate.input` became a typed model, the SDK began handing the tool an
    `AggregateScope` rather than a dict. Every real MCP call therefore
    answered "not pageable", and a set aggregate over the byte budget came
    back as a flat `response_byte_budget_exceeded` claiming there was "no
    safe continuation cursor", for a shape that had a perfectly good one.

    The model instance IS the defect, so this passes one in directly. No
    database is involved: the byte bound and the paging decision are pure
    functions of the arguments and the payload, which is what makes the
    regression reproducible in-process.
    """
    def aggregate(
        input: Any,
        by: list[str],
        metrics: list[str],
        limit: int = 4,
        offset: int = 0,
        max_bytes: int = 4_096,
    ) -> dict[str, Any]:
        # Sized so four rows exceed the budget and one or two fit, under
        # BOTH response measurements: the text form this branch serves, and
        # the smaller structured form that PACT `operator-output-contract`
        # (issue #2010) moves to. A fixture that only just overflowed the
        # text form would stop overflowing anything once the measurement
        # shrank, and the test would pass while proving nothing.
        return {
            "items": [{"group": "g" * 4_000} for _ in range(limit)],
            "limit": limit,
            "offset": offset,
            "next_offset": offset + limit,
            "total_count": 16,
            "truncated": True,
        }

    out = bound_read_tool("aggregate", aggregate)(
        input=AggregateScope(kind="nodes", subtype="wide_doc"),
        by=["attrs.text"],
        metrics=["count"],
        max_bytes=8_192,
    )

    assert out.get("error", {}).get("code") != "response_byte_budget_exceeded", (
        "a set aggregate that can page was refused as unpageable; "
        f"_is_pageable saw a model instead of a mapping: {out}"
    )
    assert out["truncated"] is True
    assert out["truncation_reason"] == BYTE_TRUNCATION_REASON
    assert out["next_offset"] > 0
    assert public_call_tool_result_bytes(out) <= 8_192


def test_a_typed_single_node_aggregate_is_still_not_pageable() -> None:
    """Non-vacuity partner. Normalizing the arguments must not make
    everything look pageable: a single-node anchor has no cursor, and
    answering it with a smaller page would be inventing one."""
    def aggregate(
        input: Any,
        by: list[str],
        metrics: list[str],
        limit: int = 4,
        offset: int = 0,
        max_bytes: int = 4_096,
    ) -> dict[str, Any]:
        return {"rollup": {"text": "x" * 20_000}}

    out = bound_read_tool("aggregate", aggregate)(
        input=NodeAnchor(node_id="n:1"),
        by=["subtype"],
        metrics=["count"],
        max_bytes=4_096,
    )

    assert out["error"]["code"] == "response_byte_budget_exceeded"


def test_the_paging_retry_still_receives_a_live_context() -> None:
    """`ctx` survives the over-budget retry.

    The normalization above exists so `_is_pageable` can read typed models,
    but `as_mapping` would flatten a live FastMCP `Context` to `{}`. The
    retry is therefore built from the untouched bound arguments. No operator
    reads `ctx` today, so a regression here would be invisible until one
    did — and then only on the retry path.
    """
    seen: list[Any] = []

    class _Context:
        """Stands in for a FastMCP Context: not a mapping, not a model."""

    def filter_rows(
        input: Any,
        predicate: dict[str, Any],
        limit: int = 4,
        offset: int = 0,
        max_bytes: int = 4_096,
        ctx: Any = None,
    ) -> dict[str, Any]:
        seen.append(ctx)
        return {
            "items": [{"text": "x" * 3_000} for _ in range(limit)],
            "limit": limit,
            "offset": offset,
            "next_offset": offset + limit,
            "truncated": True,
        }

    context = _Context()
    bound_read_tool("filter", filter_rows)(
        input={"kind": "nodes"},
        predicate={},
        max_bytes=8_192,
        ctx=context,
    )

    assert len(seen) > 1, "the retry path never ran; this proves nothing"
    assert all(item is context for item in seen), (
        f"the retry lost the context: {seen}"
    )


@pytest.mark.postgres
def test_a_typed_aggregate_over_the_wire_pages_instead_of_refusing(
    clean_runtime: str,
) -> None:
    """The same guard, end to end.

    The hermetic test above passes an `AggregateScope` in by hand. Only this
    one proves the SDK really hands the tool a model on a live call, which
    is the premise the whole defect rests on. Both are kept deliberately:
    the hermetic test localizes the fault, this one shows it is real.
    """
    generation_id = _publish_wide_rows(clean_runtime)
    server = build_server(dsn=clean_runtime)
    seen: dict[str, Any] = {}

    async def drive() -> None:
        async with create_connected_server_and_client_session(server) as session:
            result = await session.call_tool(
                "aggregate",
                {
                    "input": {"kind": "nodes", "subtype": "wide_doc"},
                    "by": ["attrs.text"],
                    "metrics": ["count"],
                    "generation_id": generation_id,
                    "limit": 4,
                    # Big enough that ONE group fits but four do not, so a
                    # smaller page is a real answer. At the 4 KiB floor even
                    # a single 2,500-character row is too wide, and the
                    # honest response there is the refusal.
                    "max_bytes": 12_000,
                },
            )
            structured = getattr(result, "structuredContent", None)
            seen["payload"] = (
                structured
                if structured is not None
                else json.loads(result.content[0].text)
            )

    anyio.run(drive)
    payload = seen["payload"]
    assert payload.get("error", {}).get("code") != "response_byte_budget_exceeded", (
        f"a set aggregate that can page was refused as unpageable: {payload}"
    )
    assert payload.get("truncated") is True
    assert payload.get("truncation_reason") == BYTE_TRUNCATION_REASON
    assert isinstance(payload.get("next_offset"), int)


def test_a_budget_under_the_floor_is_raised_said_and_still_fits() -> None:
    """Issue #3153: agents pass max_bytes under 4 KiB. The call is served at
    the 4 KiB floor, says so in `max_bytes_raised` and a text notice, and the
    page with that disclosure fits the applied budget."""
    from okg.substrate.mcp.result_text import render

    def filter(
        input: Any = None,
        predicate: dict[str, Any] | None = None,
        limit: int = 10,
        offset: int = 0,
        generation_id: str | None = None,
        max_bytes: Any = 4_096,
    ) -> dict[str, Any]:
        del input, predicate, max_bytes
        return {
            "items": [
                {"node_id": f"n:{offset + i}", "subtype": "t", "attrs": {"text": "é" * 900}}
                for i in range(limit)
            ],
            "limit": limit, "offset": offset, "next_offset": offset + limit,
            "truncated": True, "pinned_generation_id": generation_id or "gen:1",
        }

    for asked in (1, 1000, 4095):
        out = bound_read_tool("filter", filter)(
            input={"kind": "nodes"}, limit=10, max_bytes=asked,
        )
        assert out.get("status") != "refused", out
        assert out["max_bytes_raised"] == {"requested": asked, "applied": 4096}
        assert out["items"], out
        assert public_call_tool_result_bytes(out) <= 4_096
        assert f"! max_bytes {asked} raised to 4096" in render(out, tool="filter")
    # At the floor nothing is raised, and an over-ceiling value still refuses.
    at_floor = bound_read_tool("filter", filter)(input={"kind": "nodes"}, max_bytes=4096)
    assert "max_bytes_raised" not in at_floor
    over = bound_read_tool("filter", filter)(
        input={"kind": "nodes"}, max_bytes=MAX_RESPONSE_BYTES + 1,
    )
    assert over["error"]["code"] == "invalid_response_byte_budget"


def test_an_oversize_node_is_served_partial_within_its_budget() -> None:
    """#3647 (`nodepartial A`): lane X agents read Jira nodes at 4-12 KB
    that needed 12 KB at the median, and every one was refused. The node
    now comes back within max_bytes, its largest values cut (text keeps its
    start, an untrusted fence stays whole, a list is left out), each listed
    in attrs_cut with its full size, never marked complete."""
    import json

    from okg.substrate.mcp.result_text import ResultContext, render, result_context

    fence_open = "⟦UNTRUSTED_DATA source=jira⟧\n"
    fence_close = "\n⟦/UNTRUSTED_DATA⟧"
    node = {
        "node_id": "jira:BIG-1", "subtype": "jira_issue",
        "attrs": {
            "description": fence_open + "d" * 9000 + fence_close,
            "summary": "s" * 2500, "comments": ["c" * 3000] * 3, "key": "BIG-1",
        },
    }

    def inspect(target: dict[str, Any], max_bytes: int = 4_096) -> dict[str, Any]:
        return json.loads(json.dumps(node))

    for fmt in ("json", "text"):
        with result_context(ResultContext(format=fmt, tool="inspect")):
            page = bound_read_tool("inspect", inspect)(
                target={"kind": "node", "node_id": "jira:BIG-1"}, max_bytes=4_096,
            )
            # Measured in the call's own format, the form that is served.
            assert public_call_tool_result_bytes(page) <= 4_096
        assert page.get("status") != "refused", page
        assert page["truncated"] is True
        assert page["truncation_reason"] == "response_byte_budget"
        attrs, cut = page["attrs"], page["attrs_cut"]
        assert attrs["key"] == "BIG-1"
        assert attrs["description"].startswith(fence_open + "ddd")
        assert attrs["description"].endswith(fence_close)
        assert attrs["comments"] is None
        assert cut["comments"]["bytes"] == 9012 and cut["comments"]["served_chars"] == 0
        # A list left out whole is in the select=["attrs"] read, not the text.
        assert cut["comments"]["read"]["select"] == ["attrs"]
        assert cut["description"]["chars"] == 9000
        assert 0 < cut["description"]["served_chars"] < 9000
        assert page["read_whole"] == {
            "tool": "inspect", "target": {"kind": "text", "node_id": "jira:BIG-1"}}
    head = render(page, tool="inspect").split("\n", 1)[0]
    assert "bound reached (max_bytes 4096)" in head and "complete" not in head


def test_a_large_node_with_equal_values_and_an_unserved_copy_is_still_cut() -> None:
    """Review findings on #3647: two values of the same size (a record's
    text and its searchable copy are equal by construction) used to shrink
    256 characters a pass and run out of passes, so an 84 KB node was still
    refused; and the copy the reader never gets was cut and listed. A fence
    that ends in whitespace keeps its closer."""
    import json

    from okg.substrate.mcp.response_shape import PRODUCER_TEXT_ATTR
    from okg.substrate.mcp.result_text import ResultContext, result_context

    fence_open = "⟦UNTRUSTED_DATA source=jira⟧\n"
    fence_close = "\n⟦/UNTRUSTED_DATA⟧ \n"
    node = {
        "node_id": "jira:BIG-2", "subtype": "jira_issue",
        "attrs": {
            "text": "t" * 40_000, "body": "b" * 40_000,
            "notes": fence_open + "n" * 3_000 + fence_close,
            PRODUCER_TEXT_ATTR: "t" * 40_000, "key": "BIG-2",
        },
    }

    def inspect(target: dict[str, Any], max_bytes: int = 4_096) -> dict[str, Any]:
        return json.loads(json.dumps(node))

    # Measured in the format it was cut for (lane ri, tokens: the text form's
    # page-cut line now names the calls that read the cut parts, so a page
    # cut for JSON no longer happens to fit as text too).
    for budget, fmt in ((4_096, "json"), (12_288, "json"), (4_096, "text"), (12_288, "text")):
        with result_context(ResultContext(format=fmt, tool="inspect")):
            page = bound_read_tool("inspect", inspect)(
                target={"kind": "node", "node_id": "jira:BIG-2"}, max_bytes=budget,
            )
            assert page.get("status") != "refused", page
            assert public_call_tool_result_bytes(page) <= budget
        attrs, cut = page["attrs"], page["attrs_cut"]
        assert PRODUCER_TEXT_ATTR not in cut and PRODUCER_TEXT_ATTR not in attrs
        assert cut["text"]["chars"] == cut["body"]["chars"] == 40_000
        # equal values share the room: neither is emptied for the other
        assert cut["text"]["served_chars"] >= 64 and cut["body"]["served_chars"] >= 64
        assert attrs["text"] == "t" * cut["text"]["served_chars"]
        if "notes" in cut:
            assert attrs["notes"].startswith(fence_open) and attrs["notes"].endswith(fence_close)


@pytest.mark.parametrize("attrs", [
    # three equal values: none is emptied for the other two
    {"text": "t" * 40_000, "description": "d" * 40_000, "body": "b" * 40_000},
    # one large value of three-byte characters: its start still fits
    {"text": "\u6f22" * 80_000},
])
def test_a_cut_node_keeps_the_start_of_every_long_value(attrs: dict[str, str]) -> None:
    """Review finding on #3647: the cut is counted in characters but sized
    from bytes, and values of the same size must share the room."""
    import json

    from okg.substrate.mcp.result_text import ResultContext, result_context

    node = {"node_id": "jira:BIG-3", "subtype": "jira_issue", "attrs": {**attrs, "key": "BIG-3"}}

    def inspect(target: dict[str, Any], max_bytes: int = 4_096) -> dict[str, Any]:
        return json.loads(json.dumps(node))

    for fmt in ("json", "text"):
        with result_context(ResultContext(format=fmt, tool="inspect")):
            page = bound_read_tool("inspect", inspect)(
                target={"kind": "node", "node_id": "jira:BIG-3"}, max_bytes=4_096,
            )
            # Measured in the call's own format, the form that is served.
            assert public_call_tool_result_bytes(page) <= 4_096
        assert page.get("status") != "refused", page
        for name, value in attrs.items():
            served = page["attrs_cut"][name]["served_chars"]
            assert served >= 64, (fmt, name, page["attrs_cut"])
            assert page["attrs"][name] == value[:served]


def _ticket_with_thread() -> dict[str, Any]:
    """A ticket node whose thread (`record_text`) is most of its size, the
    shape lane RD saw refused: about 17 KB of thread beside 1.3 KB of
    attributes."""
    return {
        "node_id": "ticket:DEMO-1", "subtype": "ticket",
        "generation_id": "gen:1", "pinned_generation_id": "gen:1",
        "attrs": {
            "key": "DEMO-1", "summary": "A demo ticket", "status": "Open",
            "notes": "n" * 1_200,
        },
        "record_text": {
            "fields": {"summary": "A demo ticket", "status": "Open"},
            "description": "First line of the description. " + "d" * 5_000,
            "comments": [
                {"author": f"user{i}", "date": f"2026-01-{i + 1:02d}", "text": f"comment {i} " + "c" * 1_000}
                for i in range(12)
            ],
            "comments_total": 12, "comments_shown": 12, "comments_left": 0,
        },
    }


def _repository_with_live_relations(count: int, name_width: int = 0) -> dict[str, Any]:
    """A repository node whose subtype declares `count` live relations, each
    with an argument schema: the listing is most of the node's size."""
    schema = {
        "args": {
            f"arg{j}": {"type": "string", "description": "an argument of this relation " * 3}
            for j in range(8)
        },
        "description": "a live relation of this repository " * 4,
    }
    return {
        "node_id": "software_repository:acme/tool", "subtype": "software_repository",
        "generation_id": "gen:1", "pinned_generation_id": "gen:1",
        "attrs": {"name": "acme/tool", "url": "https://example.invalid/acme/tool"},
        "live_relations": {
            f"relation_{i:03d}" + "x" * name_width: schema for i in range(count)
        },
    }


def _served_node(node: dict[str, Any], fmt: str, budget: int, **kwargs: Any) -> dict[str, Any]:
    import json

    from okg.substrate.mcp.result_text import ResultContext, result_context

    def inspect(
        target: dict[str, Any], select: Any = None, max_bytes: int = 4_096,
    ) -> dict[str, Any]:
        return json.loads(json.dumps(node))

    with result_context(ResultContext(format=fmt, tool="inspect")):
        page = bound_read_tool("inspect", inspect)(
            target={"kind": "node", "node_id": node["node_id"]}, max_bytes=budget, **kwargs,
        )
        assert public_call_tool_result_bytes(page) <= budget, fmt
    return page


@pytest.mark.parametrize("fmt", ["json", "text"])
def test_an_oversize_node_has_its_record_text_cut_not_refused(fmt: str) -> None:
    """Lane RD (#3666): a ticket read at 12,000 bytes whose thread needs
    17 KB was refused as having "no safe continuation cursor", though the
    description promises a cut page. The thread now keeps its start (the
    description, then the oldest comments), is named in `parts_cut` with
    its full size, the characters served of how many, and the one call
    that pages it; the page is marked truncated."""
    from okg.substrate.mcp.result_text import render

    node = _ticket_with_thread()
    page = _served_node(node, fmt, 12_000)
    assert page.get("status") != "refused", page
    assert page["truncated"] is True and page["truncation_reason"] == "response_byte_budget"
    text_call = {"tool": "inspect", "target": {"kind": "text", "node_id": "ticket:DEMO-1"}}
    entry = page["parts_cut"]["record_text"]
    assert entry["bytes"] > 12_000 and entry["chars"] == 5_031 + sum(
        len(c["text"]) for c in node["record_text"]["comments"])
    assert 0 < entry["served_chars"] < entry["chars"]
    assert entry["read_whole"] == text_call
    thread = page["record_text"]
    assert thread["whole"] is False
    assert thread["description"] == node["record_text"]["description"]
    assert thread["comments"][0]["text"].startswith("comment 0 ")
    assert thread["comments_shown"] + thread["comments_left"] == 12
    assert thread["comments_left"] > 0
    # The attributes are smaller than the thread and keep their values.
    assert page["attrs"]["key"] == "DEMO-1"
    out = render(page, tool="inspect")
    head = out.split("\n", 1)[0]
    assert "bound reached (max_bytes 12000)" in head and "complete" not in head
    shown = thread["comments_shown"]
    assert entry["comments"] == {
        "total": 12, "shown": shown, "first": 1, "earlier_left": 0,
        "later_left": 12 - shown, "last_cut": entry["comments"]["last_cut"],
    }
    assert (
        f"! record text cut to fit max_bytes: {entry['served_chars']:,} of the whole "
        f"thread's {entry['chars']:,} chars are shown: the description and comments "
        f"1-{shown} of 12"
        + (", the last one cut short" if entry["comments"]["last_cut"] else "")
        + f"; {12 - shown} later comments are left out; read it page by page with "
        'inspect(target={"kind":"text","node_id":"ticket:DEMO-1"})'
    ) in out
    assert "parts_cut" not in out and "read_whole" not in out


def _ticket_with_fitted_thread(extra_attrs: dict[str, str]) -> tuple[dict[str, Any], int]:
    """A ticket node as the node read builds it for a 20-comment thread
    whose 24 KB fit (`fit_record_text`) already kept only the newest 9
    comments: comments 12-20 of 20, 11 left out before them."""
    from okg.substrate.mcp.record_text import fit_record_text

    whole = {
        "fields": {"summary": "A long ticket", "status": "Open"},
        "description": "The description. " + "d" * 2_000,
        "comments": [
            {"author": f"user{i}", "date": f"2026-02-{i + 1:02d}",
             "text": f"comment {i + 1} " + "c" * 1_000}
            for i in range(20)
        ],
        "comments_total": 20, "comments_shown": 20, "comments_left": 0,
    }
    fitted = fit_record_text(whole, lambda candidate: len(candidate["comments"]) <= 9)
    assert fitted is not None and fitted["comments_left"] == 11
    node = {
        "node_id": "ticket:LONG-1", "subtype": "ticket",
        "generation_id": "gen:1", "pinned_generation_id": "gen:1",
        "attrs": {"key": "LONG-1", "summary": "A long ticket", **extra_attrs},
        "record_text": fitted,
    }
    whole_chars = len(whole["description"]) + sum(len(c["text"]) for c in whole["comments"])
    return node, whole_chars


@pytest.mark.parametrize("fmt", ["json", "text"])
def test_a_long_thread_cut_names_the_comments_it_shows_of_the_whole_thread(fmt: str) -> None:
    """Review finding 1 on read stack PR 2 (#3666): the node read keeps the
    newest comments of a long thread; a max_bytes cut then keeps the
    description and the oldest of THOSE. The reply said "its first N of M
    chars are shown", M counting only the fitted block. It now counts the
    whole thread and names the comments shown by position, with how many
    are left out before and after them."""
    from okg.substrate.mcp.result_text import render

    node, whole_chars = _ticket_with_fitted_thread({})
    page = _served_node(node, fmt, 6_000)
    assert page.get("status") != "refused", page
    entry = page["parts_cut"]["record_text"]
    thread = page["record_text"]
    shown = thread["comments_shown"]
    assert shown >= 1
    # The comments served are the oldest of the 8 the node read kept.
    assert thread["comments"][0]["text"].startswith("comment 12 ")
    assert thread["comments_left"] == 20 - shown
    assert "comments_left_chars" not in thread
    assert entry["chars"] == whole_chars
    assert entry["comments"] == {
        "total": 20, "shown": shown, "first": 12, "earlier_left": 11,
        "later_left": 9 - shown, "last_cut": entry["comments"]["last_cut"],
    }
    assert entry["description"] == {"chars": 2_017, "served_chars": 2_017}
    out = render(page, tool="inspect")
    span = "comment 12 of 20" if shown == 1 else f"comments 12-{11 + shown} of 20"
    assert (
        f"! record text cut to fit max_bytes: {entry['served_chars']:,} of the whole "
        f"thread's {whole_chars:,} chars are shown: the description and {span}"
    ) in out
    assert f"; 11 earlier and {9 - shown} later comments are left out; read it" in out
    assert "its first" not in out
    assert f"! {shown} of 20 comments shown" in out


def test_a_text_cut_leaves_the_attributes_it_does_not_print_alone() -> None:
    """Review finding 2 on read stack PR 2 (#3666), checked: the text form
    of a node with a thread prints the thread, not the attributes, but the
    cut ranked parts by JSON size. A ticket whose attributes repeat the
    thread had them cut first (freeing nothing) and printed "! attributes
    cut" about values the reply never shows; with many attributes the cut
    ran out of passes and the read was refused. The text cut now shortens
    the thread and leaves those attributes whole."""
    from okg.substrate.mcp.result_text import render

    repeated = {"recent_comments": "r" * 15_000, "description": "e" * 9_000}
    many = {f"field_{i:03d}": f"value {i} " + "v" * 400 for i in range(300)}
    for extra in (repeated, many):
        node, _whole = _ticket_with_fitted_thread(extra)
        page = _served_node(node, "text", 6_000)
        assert page.get("status") != "refused", str(page)[:600]
        assert "attrs_cut" not in page
        assert page["attrs"] == node["attrs"]
        assert page["parts_cut"]["record_text"]["served_chars"] > 0
        out = render(page, tool="inspect")
        assert "! attributes cut" not in out
        assert "! record text cut to fit max_bytes" in out


@pytest.mark.parametrize("fmt", ["json", "text"])
def test_an_oversize_node_has_its_live_relation_listing_cut_to_names(fmt: str) -> None:
    """Lane RD (#3666): a repository read at 8,000 bytes whose subtype's
    live relation listing (every relation's argument schema) needs 25 KB was
    refused. The listing now shrinks to the relation names, named in
    `parts_cut` with its full size and the inspect(kind=subtype) call that
    reads the schemas; the attributes stay whole."""
    from okg.substrate.mcp.result_text import render

    node = _repository_with_live_relations(30)
    page = _served_node(node, fmt, 8_000, select=["node_id", "attrs"])
    assert page.get("status") != "refused", page
    assert page["truncated"] is True
    assert "live_relations" not in page
    entry = page["parts_cut"]["live_relations"]
    assert entry["relations"] == 30 and entry["bytes"] > 20_000
    assert entry["names"] == list(node["live_relations"])
    assert entry["read_whole"] == {
        "tool": "inspect", "target": {"kind": "subtype", "subtype": "software_repository"}}
    assert page["attrs"] == node["attrs"] and "attrs_cut" not in page
    out = render(page, tool="inspect")
    assert "! live relations listed by name only to fit max_bytes (30; their argument schemas," in out
    assert "relation_000, relation_001" in out
    assert 'read their arguments with inspect(target={"kind":"subtype","subtype":"software_repository"})' in out
    assert "parts_cut" not in out and "--- live_relations" not in out


def test_a_listing_whose_names_do_not_fit_is_left_out_with_its_count() -> None:
    """When even the relation names do not fit, the listing is left out,
    its relation count and size said, with the call that reads it."""
    from okg.substrate.mcp.result_text import render

    node = _repository_with_live_relations(120, name_width=60)
    page = _served_node(node, "text", 4_096)
    assert page.get("status") != "refused", page
    entry = page["parts_cut"]["live_relations"]
    assert "names" not in entry and entry["relations"] == 120
    out = render(page, tool="inspect")
    assert "! live relations left out to fit max_bytes (120 relations," in out


@pytest.mark.parametrize("fmt", ["json", "text"])
def test_an_oversize_node_lists_its_derived_documents_by_id(fmt: str) -> None:
    """Review P2-2 on lane RD: a node whose derived-document list was the
    large part was still refused. The list is now named by document id in
    `parts_cut` with its full size and the incoming `derived_from` expand
    that reads it."""
    from okg.substrate.mcp.result_text import render

    node = _repository_with_live_relations(0)
    node.pop("live_relations")
    node["documents"] = [
        {"node_id": f"doc:acme/tool/{i:02d}", "chunk_count": 3, "title": "t" * 400}
        for i in range(30)
    ]
    page = _served_node(node, fmt, 4_096)
    assert page.get("status") != "refused", page
    assert page["truncated"] is True and "documents" not in page
    entry = page["parts_cut"]["documents"]
    assert entry["count"] == 30 and entry["bytes"] > 12_000
    assert entry["names"] == [row["node_id"] for row in node["documents"]]
    assert entry["read_whole"] == {
        "tool": "expand", "input": "software_repository:acme/tool",
        "edge_type": "derived_from", "direction": "in"}
    out = render(page, tool="inspect", kind="node")
    assert "! derived documents listed by name only to fit max_bytes (30; " in out
    assert (
        'read them with expand(input="software_repository:acme/tool", '
        'edge_type="derived_from", direction="in")'
    ) in out
    assert "parts_cut" not in out


@pytest.mark.parametrize("fmt", ["json", "text"])
def test_an_oversize_node_lists_its_live_overlay_by_field(fmt: str) -> None:
    """Review P2-2 on lane RD: a hydrated node whose live overlay was the
    large part was still refused. The overlay is now named by field with
    its full size and the call that hydrates those fields alone."""
    from okg.substrate.mcp.result_text import render

    node = _repository_with_live_relations(0)
    node.pop("live_relations")
    node["external_live"] = {
        "schema": "external_live/v1", "node_id": node["node_id"],
        "fetched_at": "2026-01-01T00:00:00+00:00",
        "fields": {f"field_{i}": {"current": "v" * 800} for i in range(12)},
    }
    page = _served_node(node, fmt, 4_096)
    assert page.get("status") != "refused", page
    assert "external_live" not in page
    entry = page["parts_cut"]["external_live"]
    assert entry["count"] == 12 and entry["names"] == [f"field_{i}" for i in range(12)]
    assert entry["read_whole"] == {
        "tool": "inspect",
        "target": {"kind": "node", "node_id": "software_repository:acme/tool",
                   "hydrate": entry["names"]},
        "select": ["node_id"]}
    out = render(page, tool="inspect", kind="node")
    assert "! live fields listed by name only to fit max_bytes (12; " in out
    assert 'read them with inspect(target={"kind":"node","node_id":"software_repository:acme/tool","hydrate":[' in out


@pytest.mark.parametrize("fmt", ["json", "text"])
def test_a_left_out_list_attribute_points_at_the_read_that_returns_it(fmt: str) -> None:
    """Review P2-3 on lane RD: a list attribute left out whole pointed at
    inspect(kind=text), which does not return it. It now names the
    select=["attrs"] read at a max_bytes the whole attributes fit."""
    from okg.substrate.mcp.result_text import render

    node = _repository_with_live_relations(0)
    node.pop("live_relations")
    node["attrs"]["labels"] = [f"label-{i}-" + "l" * 300 for i in range(20)]
    page = _served_node(node, fmt, 4_096)
    assert page.get("status") != "refused", page
    entry = page["attrs_cut"]["labels"]
    assert entry["served_chars"] == 0 and page["attrs"]["labels"] is None
    read = entry["read"]
    assert read["tool"] == "inspect" and read["select"] == ["attrs"]
    assert read["target"] == {"kind": "node", "node_id": "software_repository:acme/tool"}
    assert entry["bytes"] < read["max_bytes"] <= 262_144 and read["max_bytes"] % 1024 == 0
    out = render(page, tool="inspect", kind="node")
    line = next(x for x in out.split("\n") if x.startswith("! attributes cut"))
    assert (
        f"labels left out ({entry['bytes']:,} B; read it with "
        'inspect(target={"kind":"node","node_id":"software_repository:acme/tool"}, '
        f'select=["attrs"], max_bytes={read["max_bytes"]}))'
    ) in line
    # Only a text value cut sends the reader to the record's text.
    assert '"kind":"text"' not in line


def test_a_node_whose_envelope_alone_does_not_fit_is_still_refused() -> None:
    """Refused only when the node's identity and envelope alone are over
    the budget: here its provenance block, which is never cut."""
    node = _ticket_with_thread()
    node["provenance"] = {"source": "tickets", "note": "p" * 9_000}
    import json

    from okg.substrate.mcp.result_text import ResultContext, result_context

    def inspect(target: dict[str, Any], max_bytes: int = 4_096) -> dict[str, Any]:
        return json.loads(json.dumps(node))

    with result_context(ResultContext(format="json", tool="inspect")):
        page = bound_read_tool("inspect", inspect)(
            target={"kind": "node", "node_id": "ticket:DEMO-1"}, max_bytes=8_000,
        )
    assert page["status"] == "refused"
    assert page["error"]["code"] == "response_byte_budget_exceeded"


@pytest.mark.postgres
def test_a_served_oversize_node_comes_back_partial_and_fenced(clean_runtime: str) -> None:
    """#3647 (`nodepartial A`) on a real publish: an untrusted wide node read
    at 4,096 bytes comes back within the budget, its largest values cut
    inside their fences, attrs_cut naming each cut value's full size, and
    the page never reads as complete, in both formats."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = seed_catalog_version(conn)
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.subtypes (name, metadata, catalog_version_id) "
                "VALUES ('wide_ticket', '{}'::jsonb, %s)", (cv,),
            )
            cur.execute(
                "INSERT INTO okg.subtype_ancestors "
                "(catalog_version_id, subtype_name, ancestor_name, depth) "
                "VALUES (%s, 'wide_ticket', 'wide_ticket', 0)", (cv,),
            )
        ChronosGraphStore.from_connection(conn).apply_facts([NodeFact(
            node_id="ticket:wide", subtype="wide_ticket",
            attrs={"key": "WIDE-1", "summary": "Wide ticket", "text": "t" * 9_000,
                   "description": "d" * 6_000, "labels": ["l" * 500] * 4},
            source_record_id={"id": "ticket:wide"}, source_revision={"v": 1},
        )], source="test:wide-ticket", strict_endpoints=True)
    published = Publisher(
        clean_runtime, "default",
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    assert published["status"] == "published"
    server = build_server(dsn=clean_runtime)
    pages: dict[str, Any] = {}

    async def drive() -> None:
        async with create_connected_server_and_client_session(server) as session:
            for fmt in ("json", "text"):
                result = await session.call_tool(
                    "inspect",
                    {"target": {"kind": "node", "node_id": "ticket:wide"}, "max_bytes": 4_096},
                    meta={"okg/result_format": fmt},
                )
                assert result.isError is not True, _result_text(result)[:400]
                assert len(result.model_dump_json(
                    by_alias=True, exclude_none=True).encode("utf-8")) <= 4_096
                pages[fmt] = result

    anyio.run(drive)
    page = _result_payload(pages["json"])
    assert page["truncated"] is True and page.get("complete") is not True
    cut = page["attrs_cut"]
    assert set(cut) >= {"text", "description"}
    assert cut["text"]["chars"] == 9_000 and cut["text"]["served_chars"] < 9_000
    attrs = page["attrs"]
    assert attrs["key"] == "WIDE-1" or "WIDE-1" in str(attrs["key"])
    for name in ("text", "description"):
        value = attrs[name]
        assert value.startswith("⟦UNTRUSTED_DATA") and value.endswith("⟦/UNTRUSTED_DATA⟧"), value[:80]
    assert page["read_whole"]["target"] == {"kind": "text", "node_id": "ticket:wide"}
    head = _result_text(pages["text"]).split("\n", 1)[0]
    assert "bound reached (max_bytes" in head and " complete" not in head, head
