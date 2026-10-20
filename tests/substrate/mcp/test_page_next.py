"""One page ending and a shared `next` token (PACT read-model-s6-page-next).

Design `read-model` section 3.5: every operator's text status line ends with
exactly one of `complete`; `bound reached (<what>) · next: <...>`; or, on a
refusal, `retry: <change>`. Counts read `N`, `N+` or `count unknown`. A
`next` token is routed by its kind, and a token that cannot continue a read
is refused by name.

Hermetic: payloads are rendered directly and tokens are signed in process.
The server round trip on PostgreSQL is in `test_page_next_db.py`. New
symbols are imported inside the tests that use them, so on a tree without
this change each test fails on its own assertion rather than at import.
"""
from __future__ import annotations

import pytest

from okg.substrate.mcp import continuation
from okg.substrate.mcp.continuation import (
    ContinuationTokenInvalid,
    consume_continuation,
    issue_continuation,
)
from okg.substrate.mcp.result_text import (
    StatusLine,
    parse_status_line,
    render,
    status_facts,
)

GEN = "okg:demo:branch:default:gen:20260101T000000000000Z:0123456789ab"


def _first(text: str) -> str:
    return text.split("\n", 1)[0]


def _ending(text: str) -> list[str]:
    """The status line's ending facts: one (`complete`, `retry: ...`) or two.

    The ending is the last fact before `generation`, which stays last.
    """
    facts = status_facts(text) or _first(text).split(" · ")[1:]
    if facts and facts[-1].startswith("generation "):
        facts = facts[:-1]
    if facts and facts[-1].startswith("next: "):
        return facts[-2:]
    return facts[-1:]


# ── every operator ends one way ──────────────────────────────────────────


def test_search_ends_complete_or_bound_reached_with_next_offset() -> None:
    whole = {
        "method": "lexical", "results": [{"node_id": "n:1", "subtype": "t"}],
        "total_count": 1, "total_count_basis": "exact", "limit": 10, "offset": 0,
        "next_offset": None, "truncated": False, "pinned_generation_id": GEN,
    }
    text = render(whole, tool="search")
    # #3666 (read-passage-rows): a whole first page says "N records, all
    # shown" where it said "1-1 of 1 · complete"; the claim is the same.
    # D8 (PACT read-passage-rows-3666): no read reply names its generation.
    assert _first(text) == "search ok · lexical · 1 record, all shown"

    paged = dict(whole, total_count=50, next_offset=10, truncated=True,
                 results=[{"node_id": f"n:{i}", "subtype": "t"} for i in range(10)])
    text = render(paged, tool="search")
    # #3666: a search page that continues names its rows and the call
    # that continues it; "bound reached (limit 10)" was the call's own
    # limit. The continuation is unchanged.
    assert _first(text) == (
        "search ok · lexical · rows 1-10 of 50 · next: offset=10"
    )
    assert parse_status_line(text) == StatusLine("search", "ok")


def test_a_rank_capped_search_ends_as_its_delivery_says() -> None:
    """Delivery is independent of retrieval (design section 14): a rank cap
    never makes the ending `bound reached` by itself. The cap shows in the
    coverage facts; the ending follows `coverage.delivery.complete`."""
    from okg.substrate.mcp.envelope import with_read_coverage

    page = {
        "operator_name": "search",
        "method": "lexical", "results": [{"node_id": "n:1", "subtype": "t"}],
        "total_count": 1000, "total_count_basis": "lower_bound", "rank_capped": True,
        "ranked_cap": {"rows": 1000}, "limit": 10, "offset": 990,
        "next_offset": None, "truncated": False, "pinned_generation_id": GEN,
    }
    # Nothing proves the capped candidate set was all delivered: unknown.
    out = with_read_coverage(page)
    assert out["coverage"]["delivery"]["complete"] is None
    text = render(out, tool="search")
    assert _ending(text) == ["completion unknown"]
    # #3666: the bound names (candidate_cap) are internals no longer
    # printed; a capped count still says it is a floor.
    assert "of 1000+ (cap 1000 rows)" in _first(text)
    # An exact selected set, all on this page, beside the rank cap: complete.
    exact = with_read_coverage(dict(page, total_count=991, total_count_basis="exact",
                                    truncated=True))
    assert exact["coverage"]["delivery"]["complete"] is True
    text = render(exact, tool="search")
    assert _ending(text) == ["complete"]
    # #3666: complete over a bounded retrieval says so, never "all shown".
    assert "retrieval bounded" in _first(text) and "all shown" not in text
    assert "next:" not in _first(text)


def test_a_byte_cut_page_names_max_bytes() -> None:
    page = {
        "method": "lexical", "results": [{"node_id": "n:1", "subtype": "t"}],
        "total_count": 9, "total_count_basis": "exact", "limit": 5, "offset": 0,
        "next_offset": 1, "truncated": True, "truncation_reason": "response_byte_budget",
        "response_byte_budget": 4096, "pinned_generation_id": GEN,
    }
    # #3666: search does not publish max_bytes (D3), so its status line
    # never names it; the byte cut is the page-cut notice, the continuation
    # is unchanged. Other tools keep `bound reached (max_bytes N)`.
    text = render(page, tool="search")
    assert _ending(text) == ["rows 1-1 of 9", "next: offset=1"]
    assert "max_bytes" not in text
    assert "! page cut to fit the reply size" in text


@pytest.mark.parametrize("tool", ["filter", "map"])
def test_filter_and_map_pages_count_n_n_plus_or_unknown(tool: str) -> None:
    rows = [{"node_id": "n:1", "subtype": "t"}, {"node_id": "n:2", "subtype": "t"}]
    whole = {"items": rows, "total_count": 2, "total_count_basis": "exact",
             "truncated": False, "limit": 50, "offset": 0, "next_offset": None,
             "pinned_generation_id": GEN}
    assert _first(render(whole, tool=tool)) == (
        f"{tool} ok · 1-2 of 2 · complete"
    )
    first = dict(whole, total_count_basis="lower_bound", truncated=True,
                 limit=2, next_offset=2)
    assert _first(render(first, tool=tool)) == (
        f"{tool} ok · 1-2, more follow · bound reached (limit 2) · next: offset=2"
    )
    # A later page cannot count its set (PR #3213's basis `unknown`).
    later = dict(first, total_count=None, total_count_basis="unknown",
                 offset=2, next_offset=4)
    text = render(later, tool=tool)
    assert _first(text) == (
        f"{tool} ok · 3-4 of count unknown · bound reached (limit 2) · next: offset=4"
    )
    assert "total_count_basis" not in text


def test_aggregate_ends_and_counts_its_groups() -> None:
    page = {"groups": [{"subtype": "a", "count": 3}], "by": ["subtype"],
            "metrics": [{"op": "count"}], "total_count": 1, "total_count_basis": "exact",
            "truncated": False, "limit": 50, "offset": 0, "next_offset": None,
            "pinned_generation_id": GEN}
    assert _ending(render(page, tool="aggregate")) == ["complete"]


def test_query_counts_a_whole_result_and_names_its_row_cap() -> None:
    whole = {"rows": [["a"], ["b"]], "columns": ["x"], "truncated": False,
             "returned_count": 2, "pinned_generation_id": GEN}
    assert _first(render(whole, tool="query")) == (
        "query ok · 1-2 of 2 · complete"
    )
    capped = dict(whole, truncated=True)
    assert _first(render(capped, tool="query")) == (
        "query ok · 1-2 of count unknown · bound reached (row cap) · next: none"
    )


def test_inspect_text_ends_with_its_token_as_next() -> None:
    page = {"node_id": "doc:1", "subtype": "document", "order_basis": "chunk_ordinal",
            "coverage": "complete", "items": [{"chunk_id": "c:0", "char_offset": 0,
                                               "text": "hello"}],
            "complete": False, "truncated": True, "continuation_token": "TOK.sig",
            "pinned_generation_id": GEN}
    text = render(page, tool="inspect", kind="text")
    assert _first(text) == (
        "inspect ok · text of doc:1 (document) · 1 segment · bound reached (max_bytes) · next: TOK.sig"
    )
    assert "incomplete" not in text and "continuation_token" not in text.split("\n", 1)[1]
    done = dict(page, complete=True, truncated=False)
    del done["continuation_token"]
    assert _ending(render(done, tool="inspect", kind="text")) == ["complete"]


def test_inspect_subtype_and_expand_path_pages_end_with_their_token() -> None:
    subtype = {"subtype": "document", "attr_keys": [{"name": "a"}], "truncated": True,
               "truncated_sections": {"attributes": True}, "section_page_size": 1,
               "continuation_token": "SUB.sig", "pinned_generation_id": GEN}
    assert _ending(render(subtype, tool="inspect", kind="subtype")) == [
        "bound reached (page_size 1)", "next: SUB.sig",
    ]
    path = {"start_node": "n:s", "path": ["first:out"], "results": [{"node_id": "n:a"}],
            "total_count": 1, "truncated": True, "continuation_token": "PATH.sig",
            "pinned_generation_id": GEN}
    assert _ending(render(path, tool="expand")) == ["bound reached (page)", "next: PATH.sig"]


def test_a_node_read_and_a_non_dictionary_result_end_complete() -> None:
    node = {"node_id": "n:1", "subtype": "t", "attrs": {"a": 1}, "pinned_generation_id": GEN}
    assert _first(render(node, tool="inspect", kind="node")).endswith(" · complete")
    assert _first(render("plain", tool="x")) == "x ok · complete"


def test_a_refusal_ends_with_its_retry_and_keeps_its_first_words() -> None:
    refusal = {"status": "refused", "operator_name": "search",
               "error": {"code": "semantic_search_unavailable",
                         "message": "search method='semantic' requires an embedder",
                         "fix_hint": "retry with method='lexical'",
                         "fallback_method": "lexical"},
               "pinned_generation_id": GEN}
    text = render(refusal, tool="search")
    assert _first(text) == (
        "search refused semantic_search_unavailable · retry: retry with method='lexical'"
    )
    # The error block keeps every field: the hint is also on its own line.
    assert "fix_hint: retry with method='lexical'" in text.splitlines()
    assert parse_status_line(text) == StatusLine(
        "search", "refused", "semantic_search_unavailable",
    )
    no_hint = {"status": "refused", "error": {"code": "c", "fallback_operator": "query"}}
    assert _first(render(no_hint, tool="filter")) == "filter refused c · retry: use query"
    raised = render({"error": {"code": "boom", "message": "m"}}, tool="expand", raised=True)
    assert _first(raised) == "expand error boom · retry: none given"


# ── tokens: routing, and refusals by name ────────────────────────────────


def _binding(kind: str, **extra) -> dict:
    binding = {
        "kind": kind, "deployment": "d", "database": "db", "branch": "main",
        "generation": GEN, "catalog_version": 7, "subtype": None, "path": None,
        "sort": "s", "projection": {"node_id": "doc:1"}, "authority": "auth",
        "policy": None, "key_id": continuation.signing_key_id(),
    }
    binding.update(extra)
    return binding


@pytest.mark.parametrize(("kind", "tool"), [
    ("inspect_text", "inspect"),
    ("inspect_subtype", "inspect"),
    ("expand_path", "expand"),
])
def test_next_routes_each_token_kind_to_its_tool(kind: str, tool: str) -> None:
    from okg.substrate.mcp.continuation import NEXT_ROUTES, route_next

    assert NEXT_ROUTES[kind] == tool
    token = issue_continuation(_binding(kind), cursor={"segment": 0, "char": 0})
    assert route_next(token, tool=tool)["kind"] == kind


def test_a_token_for_another_tool_is_refused_with_a_retry_naming_it() -> None:
    from okg.substrate.mcp.continuation import route_next

    token = issue_continuation(_binding("expand_path"), cursor={"after_node_id": "n:a"})
    with pytest.raises(ContinuationTokenInvalid) as refused:
        route_next(token, tool="inspect")
    assert refused.value.data["reason"] == "wrong_tool"
    assert refused.value.data["tool"] == "expand"
    assert refused.value.fix_hint == "call expand(next=<this token>)"


def test_a_kind_no_tool_continues_is_refused_by_name() -> None:
    """`grep_page` lives on held branches: until it lands, nothing routes it."""
    from okg.substrate.mcp.continuation import route_next

    token = issue_continuation(_binding("grep_page"), cursor={"after": 1})
    with pytest.raises(ContinuationTokenInvalid) as refused:
        route_next(token, tool="search")
    assert refused.value.data == {"reason": "kind_unroutable", "token_kind": "grep_page"}


def test_a_forged_token_is_refused_as_a_bad_signature() -> None:
    from okg.substrate.mcp.continuation import route_next

    token = issue_continuation(_binding("inspect_text"), cursor={"segment": 0, "char": 0})
    body, signature = token.split(".")
    forged = body + "." + ("A" if signature[0] != "A" else "B") + signature[1:]
    with pytest.raises(ContinuationTokenInvalid) as refused:
        route_next(forged, tool="inspect")
    assert refused.value.data["reason"] == "signature_invalid"
    with pytest.raises(ContinuationTokenInvalid) as malformed:
        route_next("not-a-token", tool="inspect")
    assert malformed.value.data["reason"] == "malformed"


def test_a_token_signed_before_a_restart_says_so(monkeypatch: pytest.MonkeyPatch) -> None:
    """The per-process key is new after a restart; the refusal names that."""
    from okg.substrate.mcp.continuation import route_next

    monkeypatch.delenv("OKG_MCP_CONTINUATION_SIGNING_KEY", raising=False)
    monkeypatch.setattr(continuation, "_PROCESS_SIGNING_KEY", b"\x01" * 32)
    token = issue_continuation(_binding("inspect_text"), cursor={"segment": 0, "char": 0})
    monkeypatch.setattr(continuation, "_PROCESS_SIGNING_KEY", b"\x02" * 32)
    with pytest.raises(ContinuationTokenInvalid) as refused:
        route_next(token, tool="inspect")
    assert refused.value.data["reason"] == "signing_key_changed"
    assert "restarted" in str(refused.value)


def test_a_stale_or_wrong_kind_token_names_what_differs() -> None:
    token = issue_continuation(_binding("inspect_text"), cursor={"segment": 0, "char": 0})
    newer = _binding("inspect_text", generation=GEN.replace("0123", "9999"))
    with pytest.raises(ContinuationTokenInvalid) as stale:
        consume_continuation(token, expected_binding=newer)
    assert stale.value.data["reason"] == "binding_mismatch"
    assert stale.value.data["mismatched"] == ["generation"]
    assert "(generation differs)" in str(stale.value)

    with pytest.raises(ContinuationTokenInvalid) as wrong:
        consume_continuation(token, expected_binding=_binding(
            "expand_path", projection={"start_node": "doc:1"},
        ))
    assert wrong.value.data["mismatched"] == ["kind", "target"]
    assert wrong.value.data["token_kind"] == "inspect_text"

    other_reader = _binding("inspect_text", authority="other", policy="p2")
    with pytest.raises(ContinuationTokenInvalid) as authority:
        consume_continuation(token, expected_binding=other_reader)
    assert authority.value.data["mismatched"] == ["authority"]


def test_a_matching_token_still_returns_its_cursor() -> None:
    token = issue_continuation(_binding("inspect_text"), cursor={"segment": 2, "char": 5})
    assert consume_continuation(token, expected_binding=_binding("inspect_text")) == {
        "segment": 2, "char": 5,
    }


@pytest.mark.parametrize("total", [0, 123])
def test_unknown_basis_never_prints_a_numeric_total(total: int) -> None:
    text = render({"results": [{"node_id": "n:1", "subtype": "t"}],
                   "total_count": total, "total_count_basis": "unknown"}, tool="filter")
    assert "1-1 of count unknown" in _first(text)
    assert f"of {total}" not in text


@pytest.mark.parametrize("kind", ["inspect_text", "inspect_subtype", "inspect_text_batch"])
def test_next_restores_signed_budget_before_outer_response_fitting(kind: str) -> None:
    from okg.substrate.mcp.response_bounds import bound_read_tool

    token = issue_continuation(_binding(kind, projection={"max_bytes": 131072}), cursor={})
    calls = []

    seen = []

    def inspect(target=None, max_bytes=None, next=None):
        calls.append(max_bytes)
        seen.append(next)
        return {"blob": "x" * 70000}

    wrapped = bound_read_tool("inspect", inspect)
    from okg.substrate.mcp.result_text import ResultContext, result_context

    with result_context(ResultContext(format="json", tool="inspect")):
        result = wrapped(next=token)
    assert result.get("blob") == "x" * 70000
    assert calls == [131072]
    # #3605 (`nextbytes A`): the signed size beside `next` is read as omitted.
    with result_context(ResultContext(format="json", tool="inspect")):
        assert wrapped(next=token, max_bytes=131072).get("blob") == "x" * 70000
    assert calls == [131072, 131072]
    assert seen[-1] == token
    # #3634 (`nextresize A`): another valid size goes on from the same cursor
    # at that size: the operator gets the token re-signed with it.
    with result_context(ResultContext(format="json", tool="inspect")):
        assert wrapped(next=token, max_bytes=200000).get("blob") == "x" * 70000
    assert calls[-1] == 200000
    resized = continuation.open_continuation(seen[-1])
    original = continuation.open_continuation(token)
    assert resized["binding"]["projection"]["max_bytes"] == 200000
    assert resized["cursor"] == original["cursor"]
    assert {k: v for k, v in resized["binding"].items() if k != "projection"} == {
        k: v for k, v in original["binding"].items() if k != "projection"}
    # An invalid size is still refused, naming the read's own page size.
    for bad in (100, 10**7, True):
        refused = wrapped(next=token, max_bytes=bad)["error"]
        assert refused["code"] == "next_with_read_arguments"
        assert refused["signed_max_bytes"] == 131072
    assert len(calls) == 3
    limited = bound_read_tool("inspect", inspect, deployment_ceiling_bytes=65536)
    for extra in ({}, {"max_bytes": 131072}, {"max_bytes": 100000}):
        refused = limited(next=token, **extra)
        assert refused["status"] == "refused"
        assert "blob" not in refused
    assert len(calls) == 3, "a ceiling refusal must happen before database work"
    # The size asked for is checked against the ceiling before re-signing,
    # and a token signed over the ceiling is told to go on under it.
    over = limited(next=token, max_bytes=100000)["error"]
    assert "from 4096 to 65536 (this deployment's response ceiling), got 100000" in over["message"]
    restored = limited(next=token)["error"]
    assert restored["retry_call"] == {
        "tool": "inspect", "mode": "continue",
        "arguments": {"next": continuation.TOKEN_PLACEHOLDER},
        "max_bytes": {"min": 4096, "max": 65536},
    }
    with result_context(ResultContext(format="json", tool="inspect")):
        limited(next=token, max_bytes=65536)
    assert calls[-1] == 65536


def test_expand_next_restores_the_signed_path_budget_before_fitting() -> None:
    """A path page served under 262144 bytes resumes under 262144 by `next`.

    The legacy spelling (`continuation_token` with max_bytes omitted) resumes
    under it too, so it signs the same binding the first page did.
    """
    from okg.substrate.mcp.response_bounds import bound_read_tool
    from okg.substrate.mcp.result_text import ResultContext, result_context

    token = issue_continuation(
        _binding("expand_path", projection={
            "start_node": "doc:1", "hops": [[None, "out"], [None, "out"]],
            "max_bytes": 262144,
        }),
        cursor={"after_node_id": "n:a"},
    )
    calls = []

    def expand(input=None, path=None, depth=1, limit=50, continuation_token=None,
               max_bytes=None, next=None):
        calls.append(max_bytes)
        return {"results": [{"node_id": "n:b", "blob": "x" * 150_000}]}

    wrapped = bound_read_tool("expand", expand)
    with result_context(ResultContext(format="json", tool="expand")):
        by_next = wrapped(next=token)
        legacy = wrapped(input={"node_id": "doc:1"}, depth=2, continuation_token=token)
    assert by_next["results"][0]["node_id"] == "n:b", by_next
    assert legacy["results"][0]["node_id"] == "n:b", legacy
    assert calls == [262144, 262144]


def test_expand_next_beside_max_bytes_is_an_operator_refusal() -> None:
    from okg.substrate.mcp.response_bounds import bound_read_tool

    token = issue_continuation(
        _binding("expand_path", projection={"start_node": "doc:1", "max_bytes": 65536}),
        cursor={"after_node_id": "n:a"},
    )

    def expand(input=None, max_bytes=None, next=None):
        raise AssertionError("refused before the operator runs")

    refused = bound_read_tool("expand", expand)(next=token, max_bytes=262144)
    assert refused["status"] == "refused"
    # #3605: expand is never told to repeat the signed size (only inspect
    # accepts it), even when the size given equals it.
    for size in (262144, 65536):
        error = bound_read_tool("expand", expand)(next=token, max_bytes=size)["error"]
        assert error["message"] == (
            "expand: `next` carries the whole read and cannot be combined with max_bytes")
        assert "signed_max_bytes" not in error
    assert refused["operator_name"] == "expand"
    assert refused["pinned_generation_id"] == GEN
    assert refused["error"]["code"] == "next_with_read_arguments"
    assert refused["error"]["fix_hint"].startswith("call expand(next=<token>)")


@pytest.mark.parametrize(("kind", "projection"), [
    ("expand_path", {"start_node": "doc:1", "hops": [["contains", "out"]],
                     "limit": 1, "select": ["node_id"]}),
    ("expand_path", {"start_node": "doc:1", "hops": [[None, "out"], [None, "out"]],
                     "limit": 1}),
    ("inspect_text", {"node_id": "doc:1"}),
])
@pytest.mark.parametrize("generation", [GEN, 42])
def test_a_continue_retry_names_the_pinned_generation_and_authority(
    kind: str, projection: dict, generation: object,
) -> None:
    """The legacy call opens the session default; the retry pins the token's
    generation, asks for the same authority, and its hint stays one line."""
    import json

    from okg.substrate.mcp.continuation import budget_retry_call
    from okg.substrate.mcp.response_bounds import MAX_RESPONSE_BYTES, _name_budget_retry
    from okg.substrate.mcp.result_text import is_block

    call = budget_retry_call(_binding(kind, generation=generation, projection=projection))
    assert call is not None and call["mode"] == "continue"
    assert call["arguments"]["generation_id"] == generation
    assert call["repeat"] == ["authority"]
    if projection.get("hops") == [[None, "out"], [None, "out"]]:
        assert "path" not in call["arguments"]
        assert (call["arguments"]["direction"], call["arguments"]["depth"]) == ("out", 2)
    error: dict = {}
    _name_budget_retry(error, call, low=4097, high=MAX_RESPONSE_BYTES)
    assert not is_block(error["fix_hint"]), error["fix_hint"]
    assert "generation_id=" in error["fix_hint"] and "authority" in error["fix_hint"]
    assert f"generation_id={json.dumps(generation)}" in error["remedy"]
    assert "same authority" in error["remedy"]
    assert error["retry_call"]["max_bytes"] == {"min": 4097, "max": MAX_RESPONSE_BYTES}


def test_a_subtype_retry_restarts_without_a_pin() -> None:
    from okg.substrate.mcp.continuation import budget_retry_call

    call = budget_retry_call(_binding(
        "inspect_subtype", subtype="document", projection={"page_size": 1},
    ))
    assert call == {"tool": "inspect", "mode": "restart",
                    "arguments": {"target": {"kind": "subtype", "subtype": "document",
                                             "page_size": 1}}}
