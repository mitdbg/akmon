"""The lexical `completeness` option is refused where it cannot apply (PACT
lexical-index-scan-cap-3162, policy option A).

Hermetic: argument validation runs before any connection, and the MCP tool is
built with `OKG_MCP_SURFACE_PROBE=1` (unopened pools). An option a method
cannot honour is refused rather than ignored: a caller who asked for exact
recall and got an ordinary page would believe it was exact.
"""
from __future__ import annotations

import pytest

from okg.substrate.mcp.envelope import with_read_coverage
from okg.substrate.mcp.result_text import render
from okg.substrate.mcp.search import (
    BOUNDED_RECOVERY,
    SearchCompletenessRefused,
    _ExactLexicalCount,
    _IndexScanCap,
    _lexical_retrieval,
    search,
    validate_completeness,
)
from okg.substrate.mcp.server import add_pagination_hint


@pytest.mark.parametrize(
    ("method", "kwargs"),
    [("semantic", {"embedder_spec": "probe:model"}), ("alias", {})],
)
def test_search_refuses_exact_on_a_method_without_a_match_set(method, kwargs) -> None:
    """#3646: `exact` reads every match; similarity ranking has no such set.
    `bounded`, every method's current reading, is accepted everywhere."""
    with pytest.raises(SearchCompletenessRefused) as caught:
        search(None, None, "x", method=method, completeness="exact", **kwargs)
    assert caught.value.code == "search_completeness_refused"
    assert caught.value.data["supported_methods"] == ["lexical", "grep", "attrs"]
    assert "method='lexical' or method='grep'" in caught.value.fix_hint


def test_search_refuses_an_unknown_completeness_value() -> None:
    with pytest.raises(SearchCompletenessRefused) as caught:
        search(None, None, "x", method="lexical", completeness="complete")
    assert caught.value.data["values"] == ["bounded", "exact"]


@pytest.mark.parametrize("method", ["identifier_mentions", "semantic", "alias"])
def test_validation_refuses_exact_where_no_match_set_exists(method) -> None:
    validate_completeness(None, method=method)
    validate_completeness("bounded", method=method)
    with pytest.raises(SearchCompletenessRefused):
        validate_completeness("exact", method=method)


@pytest.mark.parametrize("method", ["lexical", "grep", "attrs"])
def test_validation_accepts_both_modes_where_every_match_can_be_read(method) -> None:
    validate_completeness("bounded", method=method)
    validate_completeness("exact", method=method)


def test_exact_pagination_hint_keeps_the_mode() -> None:
    exact = add_pagination_hint(
        {"truncated": True, "offset": 0, "limit": 5, "next_offset": 5,
         "completeness": "exact"},
        "search",
    )
    assert exact["pagination"]["completeness"] == "exact"
    assert 'completeness="exact"' in exact["pagination_hint"]
    bounded = add_pagination_hint(
        {"truncated": True, "offset": 0, "limit": 5, "next_offset": 5,
         "completeness": "bounded"},
        "search",
    )
    assert "completeness" not in bounded["pagination"]


def _bounded_retrieval() -> dict:
    # No `bounds`: basis bm25_index under status bounded already says the
    # index scan may have stopped (lead decision, byte trim).
    return {"status": "bounded", "basis": "bm25_index",
            "recovery": {"completeness": "exact"}}


def _page(completeness: str, retrieval: dict, **extra) -> dict:
    # Through `with_read_coverage`, as the MCP envelope serves it: the
    # producer's pre-filled `coverage.retrieval` is kept, selection and
    # delivery are computed (PACT operator-page-counts-3190).
    return with_read_coverage({
        "operator_name": "search", "method": "lexical", "rank_by": "records",
        "results": [{"node_id": "n:1", "subtype": "doc", "excerpt": "alpha"}],
        "offset": 0, "next_offset": 1, "truncated": True,
        "pinned_generation_id": "g1", "completeness": completeness,
        "coverage": {"retrieval": retrieval}, **extra,
    })


def test_coverage_keeps_the_lexical_retrieval_fact() -> None:
    page = _page("bounded", _bounded_retrieval(),
                 total_count=None, total_count_basis="unknown")
    assert page["coverage"]["retrieval"] == _bounded_retrieval()
    assert page["coverage"]["selection"]["basis"] == "retrieved_candidates"
    # One retrieval vocabulary on the page: no second `completeness.retrieval`.
    assert page["completeness"] == "bounded"


def test_renderer_says_bounded_once_and_names_the_recovery() -> None:
    text = render(
        _page("bounded", _bounded_retrieval(),
              total_count=None, total_count_basis="unknown"),
        tool="search",
    )
    first = text.splitlines()[0]
    assert "count unknown" in first and "next: offset=1" in first
    # #3666: a page that continues says "count unknown" and its next call;
    # the retrieval word and its bounds are internals no longer printed.
    assert "retrieval" not in first and "bounds" not in first
    assert 'completeness="exact"' not in first
    # "count unknown" is said once, on the status line (review L2).
    assert text.count("count unknown") == 1
    # D8 (PACT read-passage-rows-3666, Jason 2026-10-15): the exact-rerun
    # advice is off by default in text; the JSON keeps coverage.retrieval's
    # `recovery`.
    assert "rerun from offset 0" not in text
    assert "completeness" not in text.partition("--- other fields")[2]


def test_renderer_pages_an_exact_read_exactly() -> None:
    text = render(
        _page("exact", {"status": "exhaustive", "basis": "exact_scalar"},
              total_count=3, total_count_basis="exact"),
        tool="search",
    )
    first = text.splitlines()[0]
    assert 'next: offset=1 completeness="exact"' in first
    # #3666: exhaustive retrieval is the normal state and prints nothing;
    # the exact continuation is what says the next page is exact too.
    assert "retrieval" not in first
    assert "rerun with" not in text
    assert "completeness" not in text.partition("--- other fields")[2]


def test_renderer_reads_a_direct_search_payload_with_only_retrieval() -> None:
    """CLI and console callers get `search()` without `with_read_coverage`:
    coverage holds only the pre-filled retrieval fact."""
    text = render({
        "operator_name": "search", "method": "lexical", "results": [],
        "offset": 0, "next_offset": None, "pinned_generation_id": "g1",
        "total_count": None, "total_count_basis": "unknown",
        "completeness": "bounded", "coverage": {"retrieval": _bounded_retrieval()},
    }, tool="search")
    first = text.splitlines()[0]
    assert "retrieval bounded" in first and "bounds" not in first
    assert "count set" not in first and "delivery" not in first


def test_renderer_omits_a_null_count_before_diversity() -> None:
    text = render(
        _page("bounded", _bounded_retrieval(), total_count=None,
              total_count_basis="unknown", diversity_applied=True,
              max_per_parent=1, suppressed_sibling_count=4,
              total_count_before_diversity=None),
        tool="search",
    )
    assert "! 4 sibling rows suppressed (max 1 per parent)" in text
    assert "null before" not in text


def test_renderer_claims_no_exact_prefix_or_passage_total_under_bounded() -> None:
    row = {"node_id": "n:1", "subtype": "doc", "excerpt": "alpha",
           "matching_passages": 3}
    bounded = render(
        _page("bounded", _bounded_retrieval(), total_count=None,
              total_count_basis="unknown", results=[row],
              ordered_exactly_through=None, next_offset_reason="beyond_exact_order"),
        tool="search",
    )
    # #3666: a row no longer prints a matching-passage count at all (the
    # passage row shows the matched passage), so no count can read as a
    # total under bounded retrieval.
    assert "matching passages" not in bounded
    assert "order exact only within retrieved candidates" in bounded
    assert "through record null" not in bounded
    exact = render(
        _page("exact", {"status": "exhaustive", "basis": "exact_scalar"},
              total_count=1, total_count_basis="exact", results=[row]),
        tool="search",
    )
    assert "matching passages" not in exact
    assert "among retrieved candidates" not in exact


def _cap(*, exact: bool, withhold: bool = False) -> _IndexScanCap:
    return _IndexScanCap(None, segment="s", index_name="i",
                         withhold_counts=withhold, exact=exact)


def test_retrieval_is_bounded_with_recovery_by_default() -> None:
    assert BOUNDED_RECOVERY == {"completeness": "exact"}
    cap = _cap(exact=False)
    cap.record("self", [{}] * 3, limit=200, count=None)
    assert _lexical_retrieval(
        cap, rank_capped=False, work_capped=False, count_exact=True,
    ) == {"status": "bounded", "basis": "bm25_index",
          "recovery": {"completeness": "exact"}}
    # `bounds` appears only for the candidate and work caps.
    cap.record("self_expanded", [{}] * 200, limit=200, count=None)
    assert _lexical_retrieval(
        cap, rank_capped=True, work_capped=True, count_exact=False,
    ) == {"status": "bounded", "basis": "bm25_index",
          "bounds": ["candidate_cap", "work_cap"],
          "recovery": {"completeness": "exact"}}


def test_a_filled_effective_attribute_read_is_saturated_but_not_native() -> None:
    """Review MEDIUM-1: the effective-attribute read carries its own 200-row
    limit. Filled, it denies exhaustive; it is not a BM25 read, so the basis
    stays the exact form's."""
    cap = _cap(exact=True)
    cap.record("self", [{}] * 5, limit=200,
               count=_ExactLexicalCount(candidates=50, matches=5, timeout_ms=5000))
    cap.record("effective_expanded", [{}] * 200, limit=200, count=None, native=False)
    assert cap.saturated == {"effective_expanded"} and cap.native == set()
    assert _lexical_retrieval(
        cap, rank_capped=False, work_capped=False, count_exact=True,
    ) == {"status": "bounded", "basis": "exact_scalar", "bounds": ["candidate_cap"]}
    # Exactly 200 counts as filled (over cautious; a native read has no count).
    short = _cap(exact=True)
    short.record("effective", [{}] * 199, limit=200, count=None, native=False)
    assert short.saturated == set()


def test_enforced_retrieval_never_names_a_raw_saturation() -> None:
    """Under enforced Nomos raw saturation is measured before Nomos hides
    rows: `candidate_cap` would say 200 or more hidden rows match. Bounded
    keeps the rollup's own public work cap; exact says exhaustive or unknown,
    nothing more."""
    bounded = _cap(exact=False, withhold=True)
    bounded.record("self", [{}] * 200, limit=200, count=None)
    assert _lexical_retrieval(
        bounded, rank_capped=True, work_capped=False, count_exact=False,
    ) == {"status": "bounded", "basis": "bm25_index",
          "recovery": {"completeness": "exact"}}
    bounded.record("chunks", [{}] * 200, limit=200, count=None)
    assert "bounds" not in _lexical_retrieval(
        bounded, rank_capped=True, work_capped=False, count_exact=False,
    )
    assert _lexical_retrieval(
        bounded, rank_capped=True, work_capped=True, count_exact=False,
    )["bounds"] == ["work_cap"]
    filled = _cap(exact=True, withhold=True)
    filled.record("self", [{}] * 200, limit=200, count=None)
    assert _lexical_retrieval(
        filled, rank_capped=True, work_capped=False, count_exact=False,
    ) == {"status": "unknown"}
    whole = _cap(exact=True, withhold=True)
    whole.record("self", [{}] * 3, limit=200,
                 count=_ExactLexicalCount(candidates=9, matches=3, timeout_ms=5000))
    assert _lexical_retrieval(
        whole, rank_capped=False, work_capped=False, count_exact=True,
    ) == {"status": "exhaustive", "basis": "exact_scalar"}


def test_the_pre_join_executor_names_no_recovery_it_cannot_honour(monkeypatch) -> None:
    """A bundle step has no `completeness` argument: its page stays bounded
    but names no rerun its caller cannot make."""
    import importlib

    from okg.substrate.mcp.framework.executor import _execute_step
    from okg.substrate.mcp.framework.schema import PreJoinBundleStep

    search_module = importlib.import_module("okg.substrate.mcp.search")
    monkeypatch.setattr(search_module, "search", lambda conn, session, **kw: {
        "results": [], "completeness": "bounded",
        "coverage": {"retrieval": {"status": "bounded", "basis": "bm25_index",
                                   "recovery": {"completeness": "exact"}}},
    })
    page = _execute_step(
        step=PreJoinBundleStep(call="search", args={"query": "q"}, emit_as="hits"),
        tool_name="t", index=0, conn=None, session=None, params={}, bundles={},
        ctx=None, resolvers=None,
    )
    assert page["coverage"]["retrieval"] == {"status": "bounded", "basis": "bm25_index"}
    assert "rerun" not in render(
        {"operator_name": "search", "method": "lexical", **page, "offset": 0,
         "pinned_generation_id": "g1", "total_count": None,
         "total_count_basis": "unknown"},
        tool="search",
    )


def test_exact_retrieval_is_exhaustive_only_without_a_full_read() -> None:
    whole = _cap(exact=True)
    whole.record("self", [{}] * 30, limit=200,
                 count=_ExactLexicalCount(candidates=900, matches=30, timeout_ms=5000))
    assert _lexical_retrieval(
        whole, rank_capped=False, work_capped=False, count_exact=True,
    ) == {"status": "exhaustive", "basis": "exact_scalar"}
    # A native read that filled its limit, then filtered down to 30 rows
    # (review H1): the read was not whole, whatever the filtered length.
    filled = _cap(exact=True)
    filled.record("self", [{}] * 200, limit=200, count=None)
    assert filled.saturated == {"self"}
    assert _lexical_retrieval(
        filled, rank_capped=False, work_capped=False, count_exact=True,
    ) == {"status": "bounded", "basis": "bm25_index", "bounds": ["candidate_cap"]}
    # An exact read that selected its limit is saturated only past it.
    at_limit = _cap(exact=True)
    at_limit.record("self", [{}] * 200, limit=200,
                    count=_ExactLexicalCount(candidates=900, matches=200, timeout_ms=5000))
    assert at_limit.saturated == set()
    past = _cap(exact=True)
    past.record("self", [{}] * 200, limit=200,
                count=_ExactLexicalCount(candidates=900, matches=201, timeout_ms=5000))
    assert past.saturated == {"self"}
    assert _lexical_retrieval(
        past, rank_capped=True, work_capped=False, count_exact=False,
    ) == {"status": "bounded", "basis": "exact_scalar", "bounds": ["candidate_cap"]}


def test_enforced_refusal_does_not_say_which_bound_stopped_the_read() -> None:
    open_ = _cap(exact=True)
    assert open_._refuse("scoped_work_limit", 250_000, 5000).data["reason"] == (
        "scoped_work_limit"
    )
    enforced = _cap(exact=True, withhold=True)
    work = enforced._refuse("scoped_work_limit", 250_000, 5000).data
    timeout = enforced._refuse("exact_read_timeout", None, 5000).data
    # Review L3: one reason for both, so neither reveals the scoped row count.
    assert work["reason"] == timeout["reason"] == "exact_read_bound"
    assert work["scoped_candidates"] is None
    assert enforced._refuse("plan_uses_bm25_index").data["reason"] == (
        "plan_uses_bm25_index"
    )


# ---------- the MCP tool ----------


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
def server(monkeypatch: pytest.MonkeyPatch):
    from okg.substrate.mcp import server as server_mod
    from okg.substrate.mcp.server import build_server
    from okg.substrate.mcp.surface_budget import PROBE_DSN

    monkeypatch.setenv("OKG_MCP_SURFACE_PROBE", "1")
    monkeypatch.setenv("OKG_MCP_TRACE_ENABLED", "0")
    monkeypatch.delenv("OKG_MCP_TOOL_ALLOWLIST", raising=False)
    monkeypatch.delenv("OKG_MCP_ALLOW_WRITES", raising=False)
    monkeypatch.setattr(
        server_mod,
        "_server_dsn_resolution",
        lambda _dsn, _name: {
            "dsn": PROBE_DSN, "authority": {"source": "surface_probe_stub"},
        },
    )
    mcp = build_server(dsn=PROBE_DSN)
    try:
        yield mcp
    finally:
        mcp._okg_pool.close()


@pytest.mark.anyio
async def test_mcp_schema_publishes_the_option(server) -> None:
    schema = {tool.name: tool.inputSchema for tool in await server.list_tools()}["search"]
    node = schema["properties"]["completeness"]
    members = node.get("anyOf") or [node]
    enums = [member.get("enum") for member in members if member.get("enum")]
    assert enums == [["bounded", "exact"]], node


@pytest.mark.anyio
@pytest.mark.parametrize(
    "arguments",
    [
        {"query": "x", "method": "identifier_mentions", "completeness": "exact"},
        {"query": "x", "method": "alias", "completeness": "exact"},
        {"query": "x", "method": "semantic", "completeness": "exact"},
    ],
    ids=["identifier_mentions", "alias", "semantic"],
)
async def test_mcp_tool_refuses_the_option_off_lexical(server, arguments) -> None:
    """Refused before any connection opens (the probe pools are unopened).

    The probe server has no alias catalog, and #3151's capability guard runs
    first, so `alias` is refused as unavailable there: still a refusal, never
    a page that ignored the option. `search()` itself refuses completeness
    on alias (`test_search_refuses_completeness_on_other_methods`)."""
    from mcp.server.fastmcp.exceptions import ToolError

    try:
        result = await server._tool_manager.call_tool("search", arguments)
    except ToolError as exc:
        unavailable = (
            arguments.get("method") == "alias"
            and "is not available on this server" in str(exc)
        )
        assert unavailable or "completeness" in str(exc), str(exc)
        return
    rendered = str(result)
    assert "search_completeness_refused" in rendered, rendered


@pytest.mark.anyio
async def test_mcp_schema_refuses_an_unknown_value(server) -> None:
    from mcp.server.fastmcp.exceptions import ToolError

    with pytest.raises(ToolError) as caught:
        await server._tool_manager.call_tool(
            "search", {"query": "x", "completeness": "complete"},
        )
    assert "completeness" in str(caught.value)
