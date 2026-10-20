"""Secret-versus-control parity of the read operators under Nomos `enforce`.

PACT lookup-count-after-read-filter-3208 (issue #3208). A reader may not learn
whether a value Nomos hides from it exists. For each read operator that can
filter on an attribute, a value stored only on rows this reader cannot see
(the node is hidden) or cannot read (the attribute is masked) must answer
exactly as a value nobody stored: same rows, same `truncated`, `next_offset`,
`total_count`, `total_count_basis`, coverage and page-ending fields.

Each secret value is shared by more rows than the page `limit`, so a count or
`truncated` taken before the read filter would show. The comparison is the
whole payload, with the queried value itself swapped for the control and the
fields that echo time or a call identity removed.

Hiding and masking run under `enforce`; `audit` never hides a row.

The second half pins that only returned rows get `read` decisions. The `or`,
`not` and many-valued `in` shapes this fix refused under Nomos run since
read-model slice 7a (test_nomos_reader_view_predicates.py), as does the scan
bound test.
"""
from __future__ import annotations

import base64
import json
import zlib
from typing import Any, Callable

import psycopg
import pytest

from tests.pytest.catalog_fixture_builders import SubtypeSpec
from tests.substrate.mcp.search_fixture import make_search_deployment, publish_nodes
from tests.substrate.mcp.test_search_records import (
    MAIN,
    MASKED,
    NARROWINGS,
    OPEN,
    QUERY,
    READER,
    SECRET,
    SUBTYPES,
    _activate,
    _allow,
    _edge,
    _node,
    _publish,
)

from tests.substrate.openrouter_dns import openrouter_dns  # noqa: E402,F401

pytestmark = pytest.mark.postgres

HIDDEN_VALUE = "hidden-owner@x"
MASKED_VALUE = "masked-owner@x"
CONTROL_VALUE = "nobody-owner@x"
VISIBLE_VALUE = "visible-owner@x"
SHARED_VALUE = "shared-owner@x"
# `code` (unclassified, so mask-tokened for masked rows). The token
# "[nomos:masked]" sorts after upper case and before lower case, so a stored
# lower-case value is probed with `lt` and an upper-case one with `gt`.
READABLE_CODE = "alpha"
MASKED_CODES = ["kilo", "KILO", "zulu", "zulu"]
# `shared` rows interleave by id: readable odd, hidden even.
SHARED_READABLE = [f"comment:shared:{i}" for i in (1, 3, 5, 7, 9)]
SHARED_HIDDEN = [f"comment:shared:{i}" for i in (0, 2, 4, 6, 8)]
LIMIT = 2
SHARED = LIMIT + 2  # rows per secret value: more than one page plus one.

_VOLATILE_KEYS = frozenset({
    "elapsed_ms", "duration_ms", "latency_ms", "timing", "timings",
    "request_id", "call_id", "trace_id", "audit_id", "decision_id",
    "evaluated_at", "served_at", "generated_at", "publish_age_seconds",
    # A hash of the caller's own predicate (#3201), so it carries the queried
    # value; it says nothing about the stored rows.
    "predicate_fingerprint",
})


def _parity_deployment(tmp_path, dsn: str) -> str:
    """A hub ticket whose comments carry `assignee`.

    * `SECRET` comments (no allow rule: hidden) share HIDDEN_VALUE;
    * `MASKED` comments (redacted; `assignee` is free text) share MASKED_VALUE;
    * one readable comment carries VISIBLE_VALUE, so every operator has a
      readable row to return and the fixture proves the filter does match;
    * `code` is unclassified on `comment`, so for the masked comments it is
      mask-TOKENED (`[nomos:masked]`), not dropped: the masked rows hold
      MASKED_CODES, the readable comment READABLE_CODE (N1, #3208).
    """
    groups = {
        OPEN: [
            _node("ticket:hub", "ticket", f"{QUERY} hub ticket."),
            _node("comment:visible", "comment", f"{QUERY} visible comment.",
                  assignee=VISIBLE_VALUE, code=READABLE_CODE),
            *[_node(node_id, "comment", f"{QUERY} shared comment.",
                    assignee=SHARED_VALUE) for node_id in SHARED_READABLE],
        ],
        SECRET: [
            _node(f"comment:hidden:{i}", "comment", f"{QUERY} hidden comment {i}.",
                  assignee=HIDDEN_VALUE)
            for i in range(SHARED)
        ] + [
            _node(node_id, "comment", f"{QUERY} shared hidden comment.",
                  assignee=SHARED_VALUE) for node_id in SHARED_HIDDEN
        ],
        MASKED: [
            _node(f"comment:masked:{i}", "comment", f"{QUERY} masked comment {i}.",
                  assignee=MASKED_VALUE, code=MASKED_CODES[i])
            for i in range(SHARED)
        ],
    }
    children = ["comment:visible"] + [
        f"comment:{kind}:{i}" for kind in ("hidden", "masked") for i in range(SHARED)
    ]
    main = [_edge("ticket:hub", child) for child in children]
    deployment = _publish(tmp_path, dsn, groups=groups, main=main)
    _activate(dsn, deployment, _reader_rules())
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "UPDATE okg.subtypes SET metadata = coalesce(metadata, '{}'::jsonb)"
            " || '{\"pii_classes\": {\"assignee\": \"free_text\"}}'::jsonb"
            " WHERE name = 'comment'"
        )
    return deployment


def _reader_rules() -> list[dict[str, Any]]:
    """READER may call tools and read OPEN and MAIN; MASKED is redacted;
    everything else (SECRET) is hidden by the deny default."""
    return [_allow(OPEN), _allow(MAIN), {
        "rule_id": "allow_tool_call", "effect": "allow", "actions": ["mcp.tool.call"],
        "subjects": [READER], "match": {}, "priority": 100,
    }, {
        "rule_id": "allow_read_boundary", "effect": "allow",
        "actions": ["graph.read.mcp"], "subjects": [READER],
        "match": {"resource_kind": "mcp_graph_read_boundary"}, "priority": 100,
    }, {
        "rule_id": "allow_sql_admin", "effect": "allow",
        "actions": ["graph.read.sql_admin"], "subjects": [READER],
        "match": {}, "priority": 100,
    }, {
        "rule_id": "redact_masked", "effect": "redact", "actions": ["graph.read.mcp"],
        "subjects": [READER], "match": {"source_id": MASKED}, "priority": 100,
        "obligations": [{"type": "redaction"}, {"type": "audit"}],
    }]


def _query_sql(value: str) -> str:
    sql = "SELECT node_id FROM okg.v_nodes WHERE subtype = 'comment' "
    return sql + f"AND attrs->>'assignee' = '{value}' ORDER BY node_id"


def _eq(value: str) -> dict[str, Any]:
    return {"op": "eq", "field": "attrs.assignee", "value": value}


# name -> (tool, kwargs builder). Every case filters on `assignee` == value.
_CASES: dict[str, tuple[str, Callable[[str], dict[str, Any]]]] = {
    "search_attrs_attr_filters": ("search", lambda v: {
        "method": "attrs", "limit": LIMIT,
        "filter": {"subtype": "comment", "attr_filters": {"assignee": v}},
    }),
    "search_attrs_where": ("search", lambda v: {
        "method": "attrs", "limit": LIMIT, "subtype_filter": "comment",
        "where": _eq(v),
    }),
    "search_attrs_attr_filters_page2": ("search", lambda v: {
        "method": "attrs", "limit": LIMIT, "offset": LIMIT,
        "filter": {"subtype": "comment", "attr_filters": {"assignee": v}},
    }),
    "search_lexical_attr_filters": ("search", lambda v: {
        "query": QUERY, "method": "lexical", "limit": LIMIT,
        "filter": {"subtype": "comment", "attr_filters": {"assignee": v}},
    }),
    "search_lexical_where": ("search", lambda v: {
        "query": QUERY, "method": "lexical", "limit": LIMIT,
        "subtype_filter": "comment", "where": _eq(v),
    }),
    # #3201: the other spellings and pages lexical accepts.
    "search_lexical_top_attr_filters": ("search", lambda v: {
        "query": QUERY, "method": "lexical", "limit": LIMIT,
        "subtype_filter": "comment", "attr_filters": {"assignee": v},
    }),
    "search_lexical_attr_filters_page2": ("search", lambda v: {
        "query": QUERY, "method": "lexical", "limit": LIMIT, "offset": LIMIT,
        "filter": {"subtype": "comment", "attr_filters": {"assignee": v}},
    }),
    "search_lexical_where_page2": ("search", lambda v: {
        "query": QUERY, "method": "lexical", "limit": LIMIT, "offset": LIMIT,
        "subtype_filter": "comment", "where": _eq(v),
    }),
    "search_lexical_self_attr_filters": ("search", lambda v: {
        "query": QUERY, "method": "lexical", "limit": LIMIT, "rank_by": "self",
        "filter": {"subtype": "comment", "attr_filters": {"assignee": v}},
    }),
    "search_lexical_record_scope_where": ("search", lambda v: {
        "query": QUERY, "method": "lexical", "limit": LIMIT,
        "scope": {"record": "ticket:hub"}, "where": _eq(v),
    }),
    # #3201: grep filtered by the same attribute.
    "search_grep_attr_filters": ("search", lambda v: {
        "query": QUERY, "method": "grep", "limit": LIMIT,
        "filter": {"subtype": "comment", "attr_filters": {"assignee": v}},
    }),
    "search_grep_top_attr_filters": ("search", lambda v: {
        "query": QUERY, "method": "grep", "limit": LIMIT,
        "subtype_filter": "comment", "attr_filters": {"assignee": v},
    }),
    "search_grep_where": ("search", lambda v: {
        "query": QUERY, "method": "grep", "limit": LIMIT,
        "subtype_filter": "comment", "where": _eq(v),
    }),
    "search_grep_attr_filters_page2": ("search", lambda v: {
        "query": QUERY, "method": "grep", "limit": LIMIT, "offset": LIMIT,
        "filter": {"subtype": "comment", "attr_filters": {"assignee": v}},
    }),
    "filter": ("filter", lambda v: {
        "input": {"kind": "nodes", "subtype": "comment"},
        "predicate": _eq(v), "limit": LIMIT,
    }),
    "map": ("map", lambda v: {
        "input": {"kind": "nodes", "subtype": "comment", "filter": _eq(v)},
        "projection": ["node_id", "attrs.assignee"], "limit": LIMIT,
    }),
    "aggregate": ("aggregate", lambda v: {
        "input": {"kind": "nodes", "subtype": "comment", "filter": _eq(v)},
        "by": ["subtype"], "metrics": ["count"], "limit": LIMIT,
    }),
    "expand_destination_filter": ("expand", lambda v: {
        "input": "ticket:hub", "edge_type": "contains", "direction": "out",
        "filter": _eq(v), "limit": LIMIT,
    }),
    "query": ("query", lambda v: {"sql": _query_sql(v), "row_cap": LIMIT}),
}

# Operators that refuse an attribute predicate under `enforce` whatever the
# value, so the reader learns nothing; the case still compares the two
# refusals and checks a readable value is refused the same way.
_REFUSED_UNDER_ENFORCE: dict[str, str] = {
    # "counting is not available under this deployment's access rules".
    "aggregate": "SubstrateError",
    # run_sql refuses graph-backed SQL under enforcement.
    "query": "RunSqlInvalid",
}

# Read operators with no attribute-filter path: nothing to compare, so they
# get no case. They used to be emitted as `skip` parameters, which asserted
# nothing and made the CI test-results gate refuse every change to this file
# (skipped nodes not in scripts/ci/permanent_skips.py; issue #3666). The
# reasons stay here, and the registry check below keeps the lists apart.
_NO_ATTR_FILTER: dict[str, str] = {
    "inspect_node": "inspect(kind='node') takes a node id, not an attribute "
                    "predicate; hidden-versus-missing ids are pinned elsewhere",
    "inspect_subtype": "inspect(kind='subtype') takes no value or predicate",
}


def _scrub(payload: Any, secret: str) -> Any:
    """The payload with the queried value replaced by the control's and the
    time/call-identity fields dropped."""
    text = json.dumps(payload, sort_keys=True, default=str)
    text = text.replace(secret, CONTROL_VALUE)

    def strip(value: Any) -> Any:
        if isinstance(value, dict):
            return {k: strip(v) for k, v in value.items() if k not in _VOLATILE_KEYS}
        if isinstance(value, list):
            return [strip(v) for v in value]
        return value

    return strip(json.loads(text))


def _diff(left: Any, right: Any, path: str = "") -> list[str]:
    """Leaf paths where two scrubbed payloads differ, for the failure text."""
    if isinstance(left, dict) and isinstance(right, dict):
        out: list[str] = []
        for key in sorted(set(left) | set(right), key=str):
            out += _diff(left.get(key, "<absent>"), right.get(key, "<absent>"),
                         f"{path}.{key}")
        return out
    if isinstance(left, list) and isinstance(right, list) and len(left) == len(right):
        out = []
        for i, (a, b) in enumerate(zip(left, right)):
            out += _diff(a, b, f"{path}[{i}]")
        return out
    if left == right:
        return []
    return [f"{path}: secret={json.dumps(left, default=str)[:160]} "
            f"control={json.dumps(right, default=str)[:160]}"]


def _call(mcp, tool: str, kwargs: dict[str, Any]) -> dict[str, Any]:
    fn = mcp._tool_manager._tools[tool].fn
    try:
        return {"ok": fn(**kwargs)}
    except Exception as error:  # a refusal must be identical too
        return {"error": type(error).__name__, "code": getattr(error, "code", None),
                "message": str(error), "data": getattr(error, "data", None)}


# Known leaks, pinned so a fix shows up as an XPASS (strict) and must
# remove the entry. The expand destination filter (#3393) is fixed by PACT
# read-model-slice7a-expand-3393 (test_nomos_expand_reader_view.py).
_KNOWN_LEAKS: dict[str, str] = {}


def _params():
    out = []
    for name in _CASES:
        for secret in ("hidden", "masked"):
            marks = ([pytest.mark.xfail(strict=True, raises=AssertionError,
                                        reason=_KNOWN_LEAKS[name])]
                     if name in _KNOWN_LEAKS else [])
            out.append(pytest.param(name, secret, id=f"{name}-{secret}", marks=marks))
    return out


def test_the_case_registries_name_only_real_cases():
    """An operator is either a compared case or excused in `_NO_ATTR_FILTER`,
    never both; the refusal and known-leak lists name only compared cases."""
    assert not set(_NO_ATTR_FILTER) & set(_CASES)
    assert set(_REFUSED_UNDER_ENFORCE) <= set(_CASES)
    assert set(_KNOWN_LEAKS) <= set(_CASES)
    assert all(reason.strip() for reason in _NO_ATTR_FILTER.values())


@pytest.mark.parametrize("case,secret", _params())
def test_a_hidden_or_masked_value_answers_as_an_absent_one(
    clean_runtime, tmp_path, monkeypatch, case, secret,
):
    from okg.substrate.mcp import server as srv

    deployment = _parity_deployment(tmp_path, clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", READER)
    tool, build = _CASES[case]
    if tool == "query":
        # Under access rules the server withholds and refuses `query`
        # (issue #3666); this case proves run_sql's own run-time refusal
        # below that, which still guards run_sql's other callers.
        monkeypatch.setattr(
            "okg.substrate.mcp.effective_surface.query_withheld_by_access_rules",
            lambda _name: None,
        )
    value = HIDDEN_VALUE if secret == "hidden" else MASKED_VALUE
    mcp = srv.build_server(dsn=clean_runtime, deployment_name=deployment)
    try:
        secret_page = _call(mcp, tool, build(value))
        control_page = _call(mcp, tool, build(CONTROL_VALUE))
        visible_page = _call(mcp, tool, build(VISIBLE_VALUE))
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()
    differences = _diff(_scrub(secret_page, value), _scrub(control_page, CONTROL_VALUE))
    assert not differences, "\n".join(differences)
    if case in _REFUSED_UNDER_ENFORCE:
        # Refused for every value, the readable one included.
        assert visible_page.get("error") == _REFUSED_UNDER_ENFORCE[case], visible_page
        return
    # The control is not a refusal both sides share by accident: the same
    # call on a readable value answers and returns the readable row.
    assert "ok" in visible_page, visible_page
    if not build(VISIBLE_VALUE).get("offset"):  # a first page shows the row
        assert "comment:visible" in json.dumps(visible_page["ok"], default=str)


# ---------- paging over readable rows: lookup, filter, map (#3208) ----------

def _page(mcp, operator: str, value: str, *, limit: int, offset: int = 0) -> dict[str, Any]:
    tools = mcp._tool_manager._tools
    if operator == "lookup":
        return tools["search"].fn(
            method="attrs", limit=limit, offset=offset,
            filter={"subtype": "comment", "attr_filters": {"assignee": value}},
        )
    if operator == "lookup_where":
        return tools["search"].fn(
            method="attrs", limit=limit, offset=offset,
            subtype_filter="comment", where=_eq(value),
        )
    if operator == "filter":
        return tools["filter"].fn(
            input={"kind": "nodes", "subtype": "comment"},
            predicate=_eq(value), limit=limit, offset=offset,
        )
    assert operator == "map"
    return tools["map"].fn(
        input={"kind": "nodes", "subtype": "comment", "filter": _eq(value)},
        projection=["node_id", "attrs.assignee"], limit=limit, offset=offset,
    )


def _page_ids(page: dict[str, Any]) -> list[str]:
    rows = page["results"] if "results" in page else page["items"]
    return [str(row["node_id"]).strip() for row in rows]


def _server(srv, dsn: str, deployment: str):
    return srv.build_server(dsn=dsn, deployment_name=deployment)


def _close(mcp) -> None:
    mcp._okg_pool.close()
    if mcp._okg_write_pool:
        mcp._okg_write_pool.close()


_PAGED = ["lookup", "lookup_where", "filter", "map"]


@pytest.mark.parametrize("operator", _PAGED)
def test_paging_over_readable_rows_never_skips_or_repeats_one(
    clean_runtime, tmp_path, monkeypatch, operator,
):
    """`offset` counts readable rows: hidden rows interleaved with readable
    ones sharing the value never shorten a page that has more readable rows
    behind it, never skip or repeat one across pages, and the last page says
    it is the last."""
    from okg.substrate.mcp import server as srv

    deployment = _parity_deployment(tmp_path, clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", READER)
    mcp = _server(srv, clean_runtime, deployment)
    pages = []
    try:
        offset: int | None = 0
        while offset is not None:
            page = _page(mcp, operator, SHARED_VALUE, limit=LIMIT, offset=offset)
            pages.append(page)
            offset = page["next_offset"]
            assert len(pages) <= 5, "paging did not end"
    finally:
        _close(mcp)
    assert [_page_ids(page) for page in pages] == [
        SHARED_READABLE[0:2], SHARED_READABLE[2:4], SHARED_READABLE[4:5],
    ]
    assert [page["truncated"] for page in pages] == [True, True, False]
    assert [page["next_offset"] for page in pages] == [2, 4, None]
    if operator != "lookup_where":  # a `where` lookup reports no count
        assert (pages[0]["total_count"], pages[0]["total_count_basis"]) == (2, "lower_bound")
        assert pages[-1]["total_count_basis"] == "unknown"


def test_keyset_pages_continue_when_every_page_meets_the_scan_bound(
    clean_runtime, tmp_path, monkeypatch,
):
    """#3666 step 3b review: past the offset cap, a keyset page whose scan
    reaches its bound resumes after the last stored row scanned, so `next`
    reads on instead of ending. The offset cap is lowered to 0 and the scan
    bound to 1 stored row, so every page meets the bound. Under enforce the
    reader view's SQL skips hidden rows, so no page here is all hidden
    rows. Every readable row comes once, in id order, and no hidden one is
    served."""
    from okg.substrate.mcp import server as srv

    deployment = _parity_deployment(tmp_path, clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", READER)
    monkeypatch.setattr(srv, "MAX_OFFSET", 0)
    monkeypatch.setattr(srv, "READ_FILTER_SCAN_CAP", 1)
    expected = sorted(
        [f"comment:masked:{i}" for i in range(SHARED)]
        + SHARED_READABLE + ["comment:visible"]
    )
    mcp = _server(srv, clean_runtime, deployment)
    pages = []
    try:
        tool = mcp._tool_manager._tools["filter"].fn
        page = tool(input={"kind": "nodes", "subtype": "comment"}, predicate=None, limit=1)
        pages.append(page)
        while page.get("continuation_token"):
            page = tool(next=page["continuation_token"], limit=1)
            pages.append(page)
            assert len(pages) <= 3 * len(expected) + 10, "paging did not end"
    finally:
        _close(mcp)
    served = [node_id for page in pages for node_id in _page_ids(page)]
    assert served == expected
    # Every page stopped at the scan bound and still continued, saying why
    # (before this fix the first such page ended the read). Under enforce
    # the reader view's SQL already skips the hidden rows, so the bound is
    # what each page meets here, not a page of nothing but hidden rows.
    bounded = [page for page in pages if page.get("read_filter_scan_bound") is not None]
    assert len(bounded) >= len(expected) - 1, [p.get("pagination_notice") for p in pages]
    assert all(
        "`next` continues" in page["pagination_notice"] and page.get("continuation_token")
        for page in bounded if page is not pages[-1]
    )
    assert not pages[-1].get("continuation_token")


def test_a_scan_bound_short_of_the_offset_stops_paging_without_a_token(
    clean_runtime, tmp_path, monkeypatch,
):
    """#3666 step 3b re-review: a page whose offset passes the offset cap's
    edge but whose bounded scan found fewer readable rows than `offset` has
    not reached its own position. A token there would serve the next
    readable row as if it stood at that offset (and overstate the total), so
    the page offers none and says paging stops."""
    from okg.substrate.mcp import server as srv

    deployment = _parity_deployment(tmp_path, clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", READER)
    monkeypatch.setattr(srv, "MAX_OFFSET", 2)
    monkeypatch.setattr(srv, "READ_FILTER_SCAN_CAP", 1)
    mcp = _server(srv, clean_runtime, deployment)
    try:
        tool = mcp._tool_manager._tools["filter"].fn
        page = tool(
            input={"kind": "nodes", "subtype": "comment"}, predicate=None,
            limit=1, offset=2,
        )
    finally:
        _close(mcp)
    assert _page_ids(page) == []
    assert page.get("read_filter_scan_bound") == 1, page
    assert not page.get("continuation_token"), page
    assert page["next_offset"] is None
    assert page["pagination_capped"] is True
    assert "paging stops here" in page["pagination_notice"]


def test_a_scan_bound_exactly_at_the_offset_resumes_after_the_last_row_scanned(
    clean_runtime, tmp_path, monkeypatch,
):
    """#3666 step 3b re-review, the edge of the case above: the bounded scan
    found exactly `offset` readable rows, so it reached this page's position.
    The empty page offers a token, and `next` reads on from the row after
    the last stored row scanned, to the end of the set, in id order."""
    from okg.substrate.mcp import server as srv

    deployment = _parity_deployment(tmp_path, clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", READER)
    monkeypatch.setattr(srv, "MAX_OFFSET", 1)
    monkeypatch.setattr(srv, "READ_FILTER_SCAN_CAP", 1)
    expected = sorted(
        [f"comment:masked:{i}" for i in range(SHARED)]
        + SHARED_READABLE + ["comment:visible"]
    )
    mcp = _server(srv, clean_runtime, deployment)
    try:
        tool = mcp._tool_manager._tools["filter"].fn
        first = tool(
            input={"kind": "nodes", "subtype": "comment"}, predicate=None,
            limit=1, offset=1,
        )
        pages = [first]
        page = first
        while page.get("continuation_token"):
            page = tool(next=page["continuation_token"], limit=1)
            pages.append(page)
            assert len(pages) <= 3 * len(expected) + 10, "paging did not end"
    finally:
        _close(mcp)
    assert _page_ids(first) == []
    assert first.get("read_filter_scan_bound") == 1, first
    assert first.get("continuation_token"), first
    assert "`next` continues" in first["pagination_notice"]
    served = [node_id for page in pages[1:] for node_id in _page_ids(page)]
    assert served == expected[1:]


@pytest.mark.parametrize("operator", ["lookup", "filter", "map"])
def test_under_nomos_off_the_page_is_the_stored_page(
    clean_runtime, tmp_path, monkeypatch, operator,
):
    """Nomos `off` keeps the one-page SQL read: a stored value is the read
    value, so the page facts come straight from it, as before #3208. (The
    PACT also carries a before-and-after payload diff of these calls.)
    `filter` and `map` count the whole set under `off` (#3201); the lookup
    keeps the page-only lower bound."""
    from okg.substrate.mcp import server as srv

    deployment = _parity_deployment(tmp_path, clean_runtime)
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    mcp = _server(srv, clean_runtime, deployment)
    try:
        first = _page(mcp, operator, HIDDEN_VALUE, limit=LIMIT)
        last = _page(mcp, operator, HIDDEN_VALUE, limit=LIMIT, offset=LIMIT)
        shared = _page(mcp, operator, SHARED_VALUE, limit=LIMIT)
    finally:
        _close(mcp)
    assert _page_ids(first) == ["comment:hidden:0", "comment:hidden:1"]
    assert (first["truncated"], first["next_offset"]) == (True, LIMIT)
    expected_count = (LIMIT, "lower_bound") if operator == "lookup" else (4, "exact")
    assert (first["total_count"], first["total_count_basis"]) == expected_count
    assert _page_ids(last) == ["comment:hidden:2", "comment:hidden:3"]
    assert last["truncated"] is False and last["next_offset"] is None
    # Stored order, hidden rows included: nothing is filtered under off.
    assert _page_ids(shared) == ["comment:shared:0", "comment:shared:1"]
    for page in (first, last, shared):
        assert "read_filter_scan_bound" not in page


# ---------- or / not / in over attrs (#3208; refused until slice 7a) ----------

# Two readable rows the reader may page; the cap below equals their count.
_TWO_READABLE = ["comment:shared:1", "comment:shared:3"]
_HIDDEN_X = "comment:hidden:0"  # hidden; its stored assignee is HIDDEN_VALUE


def _h1(value: str) -> dict[str, Any]:
    """Scan-bound oracle: the stable branch matches exactly the scan cap's
    worth of stored rows, so one more stored match (hidden node X holding the
    guessed value) is what reaches the bound."""
    return {"op": "or", "clauses": [
        {"op": "in", "field": "node_id", "values": list(_TWO_READABLE)},
        {"op": "and", "clauses": [
            {"op": "eq", "field": "node_id", "value": _HIDDEN_X},
            _eq(value),
        ]},
    ]}


def _h2(value: str) -> dict[str, Any]:
    """Negation oracle: SQL evaluates `not` on STORED values, so a masked
    row drops out exactly when the guess is right."""
    return {"op": "not", "clause": _eq(value)}


def _in_shape(value: str) -> dict[str, Any]:
    """`in` over an attribute is a disjunction: refused like `or`."""
    return {"op": "in", "field": "attrs.assignee", "values": [VISIBLE_VALUE, value]}


_SHAPES = {
    "or_scan_bound": (_h1, HIDDEN_VALUE, "predicate.clauses[1].clauses[1]", "or"),
    "not_masked": (_h2, MASKED_VALUE, "predicate.clause", "not"),
    "in_many": (_in_shape, HIDDEN_VALUE, "predicate", "in"),
}


def _shape_call(mcp, operator: str, predicate: dict[str, Any]) -> dict[str, Any]:
    if operator == "filter":
        return _call(mcp, "filter", {
            "input": {"kind": "nodes", "subtype": "comment"},
            "predicate": predicate, "limit": LIMIT,
        })
    return _call(mcp, "map", {
        "input": {"kind": "nodes", "subtype": "comment", "filter": predicate},
        "projection": ["node_id", "attrs.assignee"], "limit": LIMIT,
    })


@pytest.mark.parametrize("shape", sorted(_SHAPES))
@pytest.mark.parametrize("operator", ["filter", "map"])
def test_attr_clauses_under_or_not_or_in_still_run_with_nomos_off(
    clean_runtime, tmp_path, monkeypatch, operator, shape,
):
    """No refusal without the Nomos runtime: a stored value is the read
    value, so these shapes answer as before."""
    from okg.substrate.mcp import server as srv

    deployment = _parity_deployment(tmp_path, clean_runtime)
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    build, secret, _, _ = _SHAPES[shape]
    mcp = _server(srv, clean_runtime, deployment)
    try:
        page = _shape_call(mcp, operator, build(secret))
    finally:
        _close(mcp)
    assert "ok" in page, page
    assert page["ok"].get("status") != "refused"


# ---------- decision log: `read` only for returned rows (M2) ----------

def _decision_kinds(dsn: str, deployment: str) -> dict[str, set[str]]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = conn.execute(
            "SELECT target->>'node_id', target->>'kind' FROM okg.nomos_decisions"
            " WHERE deployment_name = %s AND target->>'node_id' IS NOT NULL",
            (deployment,),
        ).fetchall()
    out: dict[str, set[str]] = {"read": set(), "count": set()}
    for node_id, kind in rows:
        if str(kind).endswith("_read"):
            out["read"].add(node_id)
        elif str(kind).endswith("_count"):
            out["count"].add(node_id)
    return out


@pytest.mark.parametrize("mode", ["enforce", "audit"])
@pytest.mark.parametrize("operator", ["lookup", "filter", "map"])
def test_scanned_rows_not_returned_get_count_decisions_not_read(
    clean_runtime, tmp_path, monkeypatch, operator, mode,
):
    """The page step reads past the page (rows before the offset, the
    lookahead, hidden rows up to the bound) to decide its page facts. Those
    rows are counted, not read: a `read` decision is recorded only for a row
    the call returns."""
    from okg.substrate.mcp import server as srv

    deployment = _parity_deployment(tmp_path, clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", mode)
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", READER)
    mcp = _server(srv, clean_runtime, deployment)
    try:
        page = _page(mcp, operator, SHARED_VALUE, limit=1, offset=1)
    finally:
        _close(mcp)
    returned = set(_page_ids(page))
    assert len(returned) == 1
    kinds = _decision_kinds(clean_runtime, deployment)
    assert kinds["read"] == returned
    # The skipped row before the offset and the lookahead were counted.
    assert len(kinds["count"]) >= 2


# ---------- N1: a range on a mask-tokened attribute never matches ----------

# (op, value just below the masked row's stored code, value just above it)
_BISECT = {
    "lt": ("kiln", "kilp"),   # stored "kilo"; the token is < both
    "gt": ("KILN", "KILP"),   # stored "KILO"; the token is > both
}


def _range_call(mcp, operator: str, op: str, value: str) -> dict[str, Any]:
    clause = {"op": op, "field": "attrs.code", "value": value}
    if operator == "filter":
        return _call(mcp, "filter", {
            "input": {"kind": "nodes", "subtype": "comment"},
            "predicate": clause, "limit": 50,
        })
    return _call(mcp, "map", {
        "input": {"kind": "nodes", "subtype": "comment", "filter": clause},
        "projection": ["node_id", "attrs.code"], "limit": 50,
    })


@pytest.mark.parametrize("mode", ["enforce", "off"])
@pytest.mark.parametrize("op", sorted(_BISECT))
@pytest.mark.parametrize("operator", ["filter", "map"])
def test_a_range_on_a_masked_value_answers_the_same_on_both_sides_of_it(
    clean_runtime, tmp_path, monkeypatch, operator, op, mode,
):
    """N1 bisection probe. SQL tests the range on the stored value, then the
    page used to test the mask token against the same range, so a masked row
    came back exactly when its stored value passed: two bounds just below and
    just above the stored value recovered it. Under enforce a clause on a
    withheld value now never matches, so the two answers are identical. With
    Nomos off the row is read in the clear and the range applies normally."""
    from okg.substrate.mcp import server as srv

    deployment = _parity_deployment(tmp_path, clean_runtime)
    if mode == "off":
        monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    else:
        monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", mode)
        monkeypatch.setenv("OKG_NOMOS_SUBJECT", READER)
    below, above = _BISECT[op]
    mcp = _server(srv, clean_runtime, deployment)
    try:
        low = _range_call(mcp, operator, op, below)
        high = _range_call(mcp, operator, op, above)
    finally:
        _close(mcp)
    masked_row = "comment:masked:0" if op == "lt" else "comment:masked:1"
    if mode == "off":
        ids = {"lt": (set(), {masked_row}), "gt": ({masked_row}, set())}[op]
        assert (masked_row in json.dumps(low), masked_row in json.dumps(high)) == (
            bool(ids[0]), bool(ids[1]))
        return
    differences = _diff(_scrub(low, below), _scrub(high, above))
    assert not differences, "\n".join(differences)
    assert "comment:masked" not in json.dumps(low["ok"], default=str)
    if op == "lt":  # the readable row still matches on its readable value
        assert "comment:visible" in json.dumps(low["ok"], default=str)


@pytest.mark.parametrize("operator", ["filter", "map"])
def test_eq_on_a_masked_value_answers_as_an_absent_value(
    clean_runtime, tmp_path, monkeypatch, operator,
):
    """N1 guard: `eq` on the masked row's stored code and on a code nobody
    stored give the same answer under enforce."""
    from okg.substrate.mcp import server as srv

    deployment = _parity_deployment(tmp_path, clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", READER)
    mcp = _server(srv, clean_runtime, deployment)
    try:
        secret = _range_call(mcp, operator, "eq", "kilo")
        absent = _range_call(mcp, operator, "eq", "nobody-code")
    finally:
        _close(mcp)
    differences = _diff(_scrub(secret, "kilo"), _scrub(absent, "nobody-code"))
    assert not differences, "\n".join(differences)


# ---------- N4: identity-claim sessions count, not read, scanned rows ----------

def test_identity_claim_decisions_are_counts_for_rows_not_returned(
    clean_runtime, monkeypatch,
):
    """M2 on the identity-claim path: the per-source identity decision of a
    row scanned only to decide page facts is a `_count` one; an
    `identity_source_read` decision is recorded only for the returned row."""
    from okg.substrate.mcp.server import build_server
    from tests.substrate.mcp.test_nomos_source_lineage_mcp import (
        _activate_identity_source_policy,
        _configure_mit_identity_grants,
        _mit_touchstone_claim,
        _publish_lineage_graph,
    )

    _publish_lineage_graph(clean_runtime)
    _activate_identity_source_policy(clean_runtime)
    _configure_mit_identity_grants(clean_runtime, source_ids=["public_docs"])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:env-bypass")
    # A single-user server evaluates a caller-supplied claim only on this
    # launch-time opt-in (as in test_nomos_source_lineage_mcp.py).
    monkeypatch.setenv("OKG_MCP_TRUST_CALLER_IDENTITY_CLAIMS", "1")
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        page = mcp._tool_manager._tools["filter"].fn(
            input={"kind": "nodes"},
            predicate={"op": "eq", "field": "attrs.classification", "value": "internal"},
            select=["node_id"], limit=1, offset=1,
            authority={"nomos_identity_claim": _mit_touchstone_claim()},
        )
    finally:
        _close(mcp)
    returned = {str(row["node_id"]).strip() for row in page["items"]}
    assert returned == {"node:public-more"}
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        rows = conn.execute(
            "SELECT target->>'node_id', target->>'kind', context FROM okg.nomos_decisions"
            " WHERE deployment_name = 'default' AND target->>'kind' LIKE %s",
            ("%_node_identity_source_%",),
        ).fetchall()
    read = {node_id for node_id, kind, _ in rows if kind.endswith("_identity_source_read")}
    counted = {node_id for node_id, kind, _ in rows if kind.endswith("_identity_source_count")}
    assert read == returned
    assert {"node:public-docs", "node:google-drive"} <= counted
    for node_id, kind, context in rows:
        if kind.endswith("_count"):
            assert (context or {}).get("nomos_read_purpose") == "count", (node_id, context)


# ---------- #3201: text matches in hidden, masked or dropped rows ----------
#
# The cases above filter on an attribute. These search for a TERM that only
# rows the reader cannot read hold: hidden (SECRET), mask-tokened (`note` and
# `comment` text is `sensitive_attribute`, served as `[nomos:masked]`) or
# dropped (`page` and `memo` text is `free_text`, not served at all). Each
# kind sits in standalone records AND in passages under a readable record
# (`ticket:hub` for hidden and masked comments, `folder:hub` for dropped
# memos), and each secret term is held by more rows than the page `limit`.
# `lexical`, `grep` and the `grep` record windows must answer for it exactly
# as for a term nobody stored. Every case first checks, with Nomos off, that
# the secret term does find its rows, so no comparison is empty to empty.

GREP_HIDDEN = "quasarhid"
GREP_MASKED = "quasarmsk"
GREP_DROPPED = "quasardrp"
GREP_CONTROL = "quasarnil"
GREP_VISIBLE = "quasarvis"
_GREP_SECRETS = {"hidden": GREP_HIDDEN, "masked": GREP_MASKED, "dropped": GREP_DROPPED}
# `mix` notes interleave by id: readable odd ones hold GREP_VISIBLE, hidden
# even ones GREP_HIDDEN, in otherwise equal text.
MIX_READABLE = [f"note:mix:{i}" for i in (1, 3, 5, 7, 9)]
MIX_HIDDEN = [f"note:mix:{i}" for i in (0, 2, 4, 6, 8)]
# A record id nobody stored, the control for a hidden record id. (A masked
# or dropped record is a readable row whose existence the reader is shown,
# so only a hidden one must answer as absent.) Both ids sort alike among the
# scope's readable ids, so an echoed, sorted scope compares position for
# position.
ABSENT_RECORD = "note:absent:0"
HIDDEN_RECORD = "note:hidden:0"
# Per kind: the readable record whose passages hold the kind's term, the
# subtype of the kind's standalone records, and (masked and dropped only) one
# such record a scope may name: its existence is public, its text is not.
_HUB = {"hidden": "ticket:hub", "masked": "ticket:hub", "dropped": "folder:hub"}
_KIND_SUBTYPE = {"hidden": "note", "masked": "note", "dropped": "page"}
_OWN_RECORD = {"masked": "note:masked:0", "dropped": "page:dropped:0"}

FOLDER = SubtypeSpec("folder", metadata={
    "searchable_text_fields": ["text"],
    "passage_rollup": {"edge_type": "contains", "child_subtype": "memo"},
})
MEMO = SubtypeSpec("memo", metadata={"searchable_text_fields": ["text"]})


def _grep_deployment(tmp_path, dsn: str) -> str:
    groups = {
        OPEN: [
            _node("ticket:hub", "ticket", "zirconium hub ticket."),
            _node("comment:vis", "comment", f"zirconium visible comment {GREP_VISIBLE}."),
            _node("folder:hub", "folder", "zirconium hub folder."),
            _node("memo:vis", "memo", f"zirconium visible memo {GREP_VISIBLE}."),
            _node("note:vis", "note", f"zirconium visible note {GREP_VISIBLE}."),
            _node("page:vis", "page", f"zirconium visible page {GREP_VISIBLE}."),
            *[_node(node_id, "note", f"zirconium mixed note {GREP_VISIBLE}.")
              for node_id in MIX_READABLE],
        ],
        SECRET: [
            *[_node(f"note:hidden:{i}", "note", f"zirconium secret note {GREP_HIDDEN} {i}.")
              for i in range(SHARED)],
            *[_node(f"comment:hidden:{i}", "comment",
                    f"zirconium secret comment {GREP_HIDDEN} {i}.") for i in range(SHARED)],
            *[_node(node_id, "note", f"zirconium mixed note {GREP_HIDDEN}.")
              for node_id in MIX_HIDDEN],
        ],
        MASKED: [
            *[_node(f"note:masked:{i}", "note", f"zirconium masked note {GREP_MASKED} {i}.")
              for i in range(SHARED)],
            *[_node(f"comment:masked:{i}", "comment",
                    f"zirconium masked comment {GREP_MASKED} {i}.") for i in range(SHARED)],
            *[_node(f"page:dropped:{i}", "page", f"zirconium dropped page {GREP_DROPPED} {i}.")
              for i in range(SHARED)],
            *[_node(f"memo:dropped:{i}", "memo", f"zirconium dropped memo {GREP_DROPPED} {i}.")
              for i in range(SHARED)],
        ],
    }
    edges = [_edge("ticket:hub", child) for child in [
        "comment:vis",
        *[f"comment:{kind}:{i}" for kind in ("hidden", "masked") for i in range(SHARED)],
    ]] + [_edge("folder:hub", child) for child in [
        "memo:vis", *[f"memo:dropped:{i}" for i in range(SHARED)],
    ]]
    deployment = make_search_deployment(tmp_path, dsn=dsn)
    publish_nodes(
        dsn, deployment, subtypes=[*SUBTYPES, FOLDER, MEMO],
        narrowings=[*NARROWINGS, ("folder", "contains", "memo")],
        nodes=[], edges=edges, node_groups=list(groups.items()),
    )
    _activate(dsn, deployment, _reader_rules())
    with psycopg.connect(dsn, autocommit=True) as conn:
        for subtype, pii in (("note", "sensitive_attribute"),
                             ("comment", "sensitive_attribute"),
                             ("page", "free_text"), ("memo", "free_text")):
            conn.execute(
                "UPDATE okg.subtypes SET metadata = coalesce(metadata, '{}'::jsonb)"
                " || jsonb_build_object('pii_classes', jsonb_build_object('text', %s::text))"
                " WHERE name = %s",
                (pii, subtype),
            )
    return deployment


def _enforce(monkeypatch) -> None:
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", READER)


def _nomos_off(monkeypatch) -> None:
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)


def _token_body(token: str) -> dict[str, Any]:
    """A continuation token as the reader can read it. The signed body is
    compressed but not encrypted, so its binding (the echoed query included)
    and a windows cursor (`{"position": ...}`) compare in the clear. A grep
    cursor is sealed (compressed, then encrypted), so only its length is
    comparable: a length that moved with hidden rows would show here."""
    encoded = token.split(".", 1)[0]
    raw = base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4))
    body = json.loads(zlib.decompress(raw))
    cursor = dict(body.get("cursor") or {})
    if "sealed" in cursor:
        cursor["sealed"] = {"sealed_length": len(cursor["sealed"])}
    binding = json.loads(json.dumps(body.get("binding")))
    projection = binding.get("projection") if isinstance(binding, dict) else None
    if isinstance(projection, str):
        # A grep binding's projection is a SHA-256 over the call's own query,
        # filter, compiled `where`, path prefix and scope
        # (grep_search._continuation_binding): it differs whenever the
        # queried value does, and reads no stored row. Checked for shape only.
        assert len(projection) == 64, projection
        binding["projection"] = "<sha256 of the call arguments>"
    scanner = projection.get("scanner") if isinstance(projection, dict) else None
    if isinstance(scanner, dict) and "identity" in scanner:
        # SHA-256 over the call's own query, scope and authority binding
        # (read_scanner.make_scan_context): it differs whenever the named
        # scope ids do, and reads no stored row. Checked for shape only.
        assert len(scanner["identity"]) == 64, scanner
        scanner["identity"] = "<sha256 of the call arguments>"
    return {"v": body.get("v"), "binding": binding, "cursor": cursor}


def _token_view(payload: Any) -> Any:
    if isinstance(payload, dict):
        return {k: (_token_body(v) if k == "continuation_token" and isinstance(v, str)
                    else _token_view(v))
                for k, v in payload.items()}
    if isinstance(payload, list):
        return [_token_view(v) for v in payload]
    return payload


def _scrub_all(payload: Any, secrets: list[str]) -> Any:
    """`_scrub` for each echoed spelling of the queried value, longest first,
    with every continuation token opened (see `_token_body`)."""
    out = _token_view(payload)
    for secret in sorted(secrets, key=len, reverse=True):
        out = _scrub(out, secret)
    return out


_MAX_PAGES = 20


def _pages(mcp, kwargs: dict[str, Any]) -> list[dict[str, Any]]:
    """Every page of a search: a continuation token is followed with the same
    arguments, else a `next_offset`. A refusal ends the list."""
    pages = [_call(mcp, "search", kwargs)]
    while "ok" in pages[-1] and len(pages) < _MAX_PAGES:
        page = pages[-1]["ok"]
        if page.get("continuation_token"):
            follow = {k: v for k, v in kwargs.items() if k != "offset"}
            follow["continuation_token"] = page["continuation_token"]
        elif page.get("next_offset") is not None:
            follow = {**kwargs, "offset": page["next_offset"]}
        else:
            break
        pages.append(_call(mcp, "search", follow))
    assert len(pages) < _MAX_PAGES, "paging did not end"
    return pages


def _row_ids(pages: list[dict[str, Any]]) -> list[str]:
    return [str(row.get("node_id") or row.get("record_id"))
            for page in pages if "ok" in page for row in page["ok"].get("results", [])]


def _windows(query: str, scope: dict[str, Any], **extra) -> dict[str, Any]:
    return {"query": query, "method": "grep", "view": "windows", "scope": scope,
            "max_bytes": 131072, **extra}


_ALL_KINDS = ("hidden", "masked", "dropped")
_OWN_KINDS = ("masked", "dropped")

# name -> (kwargs builder over the searched term and the kind, kinds it runs for).
_TERM_CASES: dict[str, tuple[Callable[[str, str], dict[str, Any]], tuple[str, ...]]] = {
    "lexical": (lambda t, k: {"query": t, "method": "lexical", "limit": LIMIT}, _ALL_KINDS),
    "lexical_page2": (lambda t, k: {"query": t, "method": "lexical", "limit": LIMIT,
                                    "offset": LIMIT}, _ALL_KINDS),
    "lexical_self": (lambda t, k: {"query": t, "method": "lexical", "limit": LIMIT,
                                   "rank_by": "self"}, _ALL_KINDS),
    # The subtype filter adds the zero-hit diagnosis an empty page carries.
    "lexical_subtype": (lambda t, k: {"query": t, "method": "lexical", "limit": LIMIT,
                                      "filter": {"subtype": _KIND_SUBTYPE[k]}}, _ALL_KINDS),
    "lexical_hub_scope": (lambda t, k: {"query": t, "method": "lexical", "limit": LIMIT,
                                        "scope": {"record": _HUB[k]}}, _ALL_KINDS),
    "lexical_own_record_scope": (lambda t, k: {
        "query": t, "method": "lexical", "limit": LIMIT,
        "scope": {"record": _OWN_RECORD[k]}}, _OWN_KINDS),
    "grep": (lambda t, k: {"query": t, "method": "grep", "limit": LIMIT}, _ALL_KINDS),
    "grep_page2": (lambda t, k: {"query": t, "method": "grep", "limit": LIMIT,
                                 "offset": LIMIT}, _ALL_KINDS),
    "grep_subtype": (lambda t, k: {"query": t, "method": "grep", "limit": LIMIT,
                                   "filter": {"subtype": _KIND_SUBTYPE[k]}}, _ALL_KINDS),
    "grep_passage_subtype": (lambda t, k: {
        "query": t, "method": "grep", "limit": LIMIT,
        "filter": {"subtype": "folder" if k == "dropped" else "ticket"}}, _ALL_KINDS),
    "grep_regex": (lambda t, k: {"query": f"re:/{t}/", "method": "grep", "limit": LIMIT},
                   _ALL_KINDS),
    "grep_and": (lambda t, k: {"query": f"zirconium {t}", "method": "grep", "limit": LIMIT},
                 _ALL_KINDS),
    # The grep page scan bound lowered to 3 rows (see `_BOUND3`).
    "grep_bound3": (lambda t, k: {"query": t, "method": "grep", "limit": LIMIT},
                    _ALL_KINDS),
    "grep_subtype_bound3": (lambda t, k: {"query": t, "method": "grep", "limit": LIMIT,
                                          "filter": {"subtype": _KIND_SUBTYPE[k]}},
                            _ALL_KINDS),
    "windows_hub_passages": (lambda t, k: _windows(t, {"records": [_HUB[k]]}), _ALL_KINDS),
    "windows_two_records": (lambda t, k: _windows(
        f"zirconium {t}", {"records": [_HUB[k], "note:vis"]}, limit=1), _ALL_KINDS),
    "windows_own_record": (lambda t, k: _windows(t, {"record": _OWN_RECORD[k]}), _OWN_KINDS),
}

# Fixed leaks, now regular cases: lexical matched a term only masked or
# dropped text held and served the row (#3377); grep charged unreadable
# matches against the page scan bound, so a term only they held reached it
# and offered a continuation token (#3384, the `_bound3` cases).
_BOUND3 = 3


def _term_params():
    return [
        pytest.param(name, kind, id=f"{name}-{kind}")
        for name, (_, kinds) in _TERM_CASES.items() for kind in kinds
    ]


def _holds(pages: list[dict[str, Any]], kind: str, term: str) -> bool:
    """Whether a page list served a row (or passage) of `kind` or its term."""
    text = json.dumps([page.get("ok") for page in pages], default=str)
    return f":{kind}:" in text or term in text


@pytest.mark.parametrize("case,kind", _term_params())
def test_a_term_only_unreadable_rows_hold_answers_as_an_absent_term(
    clean_runtime, tmp_path, monkeypatch, case, kind,
):
    from okg.substrate.mcp import server as srv

    deployment = _grep_deployment(tmp_path, clean_runtime)
    build, _ = _TERM_CASES[case]
    secret = _GREP_SECRETS[kind]
    if case.endswith("_bound3"):
        from okg.substrate.mcp import grep_search

        monkeypatch.setattr(grep_search, "PAGE_SCAN_BOUND", _BOUND3)
    _nomos_off(monkeypatch)
    mcp = _server(srv, clean_runtime, deployment)
    try:
        published = _pages(mcp, build(secret, kind))
    finally:
        _close(mcp)
    # Not empty to empty: with Nomos off the secret term finds its rows.
    assert "ok" in published[0], published[0]
    assert _row_ids(published) and _holds(published, kind, secret), published
    _enforce(monkeypatch)
    mcp = _server(srv, clean_runtime, deployment)
    try:
        secret_pages = _pages(mcp, build(secret, kind))
        control_pages = _pages(mcp, build(GREP_CONTROL, kind))
        visible_pages = _pages(mcp, build(GREP_VISIBLE, kind))
    finally:
        _close(mcp)
    differences = _diff(_scrub_all(secret_pages, [secret]),
                        _scrub_all(control_pages, [GREP_CONTROL]))
    assert not differences, "\n".join(differences)
    # Not a refusal both sides share by accident: the readable term answers,
    # on every page the case reads (a page-2 case included).
    assert "ok" in visible_pages[0], visible_pages[0]
    if case not in ("lexical_own_record_scope", "windows_own_record"):
        assert visible_pages[0]["ok"]["results"], visible_pages[0]
    text = json.dumps(secret_pages, default=str)
    for unreadable in ("note:hidden", "comment:hidden", "secret note", "secret comment",
                       GREP_MASKED + " ", GREP_DROPPED + " "):
        assert unreadable not in text


# ---------- #3201: a hidden record named in a search scope ----------

# name -> (builder over the record id, readable ids the secret call serves,
#          readable ids the same call serves with note:mix:3 in place).
_RECORD_CASES: dict[str, tuple[Callable[[str], dict[str, Any]], set[str], set[str]]] = {
    "windows_record": (lambda r: _windows("zirconium", {"record": r}),
                       set(), {"note:mix:3"}),
    "windows_records_with_readable": (lambda r: _windows(
        "zirconium", {"records": ["note:vis", r]}), {"note:vis"}, {"note:vis", "note:mix:3"}),
    "windows_records_limit1": (lambda r: _windows(
        "zirconium", {"records": ["note:vis", r, "note:mix:1"]}, limit=1),
        {"note:vis", "note:mix:1"}, {"note:vis", "note:mix:3", "note:mix:1"}),
    "lexical_record_scope_id": (lambda r: {
        "query": "zirconium", "method": "lexical", "limit": LIMIT, "scope": {"record": r}},
        set(), {"note:mix:3"}),
}


@pytest.mark.parametrize("case", sorted(_RECORD_CASES))
def test_a_hidden_record_in_scope_answers_as_an_absent_one(
    clean_runtime, tmp_path, monkeypatch, case,
):
    from okg.substrate.mcp import server as srv

    deployment = _grep_deployment(tmp_path, clean_runtime)
    build, served, served_visible = _RECORD_CASES[case]
    _nomos_off(monkeypatch)
    mcp = _server(srv, clean_runtime, deployment)
    try:
        published = _pages(mcp, build(HIDDEN_RECORD))
    finally:
        _close(mcp)
    assert HIDDEN_RECORD in _row_ids(published), published  # stored and indexed
    _enforce(monkeypatch)
    mcp = _server(srv, clean_runtime, deployment)
    try:
        secret_pages = _pages(mcp, build(HIDDEN_RECORD))
        control_pages = _pages(mcp, build(ABSENT_RECORD))
        visible_pages = _pages(mcp, build("note:mix:3"))
    finally:
        _close(mcp)
    differences = _diff(_scrub_all(secret_pages, [HIDDEN_RECORD]),
                        _scrub_all(control_pages, [ABSENT_RECORD]))
    assert not differences, "\n".join(differences)
    assert set(_row_ids(secret_pages)) == served
    assert set(_row_ids(visible_pages)) == served_visible
    if case == "windows_records_limit1":
        # A continuation was offered and followed: the token check ran.
        assert len(secret_pages) == len(served) and len(visible_pages) == 3
        assert secret_pages[0]["ok"]["continuation_token"], secret_pages[0]


# ---------- #3201: readable and unreadable matches together ----------

# The secret pattern matches the readable rows and the unreadable ones of a
# kind; the control matches exactly the same readable rows, with the same
# match lengths, and nothing else. A reader under enforce must get the
# control's answer: the answer on a graph where the unreadable matches do
# not exist. Two spellings: a group (`quasar(vis|hid)`, ordered by node id)
# and an alternation of literal words (`quasarvis|quasarhid`, ordered by
# BM25 over its words). The literal one also runs with the grep page scan
# bound lowered to 3 rows, so pages stop at the bound and continue by token.
_MIX_PATTERNS = {
    "group": ("re:/quasar(vis|nil)/", {"hidden": "re:/quasar(vis|hid)/",
                                       "masked": "re:/quasar(vis|msk)/",
                                       "dropped": "re:/quasar(vis|drp)/"}),
    "literal": ("re:/quasarvis|quasarnil/", {"hidden": "re:/quasarvis|quasarhid/",
                                             "masked": "re:/quasarvis|quasarmsk/",
                                             "dropped": "re:/quasarvis|quasardrp/"}),
}
_MIX_READABLE_RECORDS = {"ticket:hub", "folder:hub", "note:vis", "page:vis", *MIX_READABLE}

_MIX_CASES: dict[str, tuple[Callable[[str], dict[str, Any]], set[str], tuple[str, ...]]] = {
    "grep": (lambda q: {"query": q, "method": "grep", "limit": LIMIT},
             _MIX_READABLE_RECORDS, _ALL_KINDS),
    "grep_note": (lambda q: {"query": q, "method": "grep", "limit": LIMIT,
                             "filter": {"subtype": "note"}},
                  {"note:vis", *MIX_READABLE}, ("hidden", "masked")),
    "grep_page": (lambda q: {"query": q, "method": "grep", "limit": LIMIT,
                             "filter": {"subtype": "page"}},
                  {"page:vis"}, ("dropped",)),
}
_MIX_RUNS = [("group", None), ("literal", None), ("literal", _BOUND3)]


def _mix_params():
    out = []
    for name, (_, _, kinds) in _MIX_CASES.items():
        for kind in kinds:
            for pattern, bound in _MIX_RUNS:
                run = f"{pattern}-bound{bound}" if bound else pattern
                out.append(pytest.param(name, kind, pattern, bound,
                                        id=f"{name}-{kind}-{run}"))
    return out


@pytest.mark.parametrize("case,kind,pattern,bound", _mix_params())
def test_readable_matches_beside_unreadable_ones_answer_as_if_those_did_not_exist(
    clean_runtime, tmp_path, monkeypatch, case, kind, pattern, bound,
):
    from okg.substrate.mcp import grep_search
    from okg.substrate.mcp import server as srv

    deployment = _grep_deployment(tmp_path, clean_runtime)
    build, readable, _ = _MIX_CASES[case]
    control, secrets = _MIX_PATTERNS[pattern]
    secret = secrets[kind]
    if bound is not None:
        monkeypatch.setattr(grep_search, "PAGE_SCAN_BOUND", bound)
    _nomos_off(monkeypatch)
    mcp = _server(srv, clean_runtime, deployment)
    try:
        published = _pages(mcp, build(secret))
    finally:
        _close(mcp)
    # The unreadable matches are stored and indexed (the hidden `mix` notes
    # included), and the readable ones are found.
    published_ids = set(_row_ids(published))
    assert any(f":{kind}:" in node_id for node_id in published_ids), published
    assert readable <= published_ids, published
    if kind == "hidden" and case != "grep_page":
        assert set(MIX_HIDDEN) <= published_ids, published
    _enforce(monkeypatch)
    mcp = _server(srv, clean_runtime, deployment)
    try:
        secret_pages = _pages(mcp, build(secret))
        control_pages = _pages(mcp, build(control))
    finally:
        _close(mcp)
    # The pattern is echoed whole, without its slashes, and word by word.
    differences = _diff(
        _scrub_all(secret_pages, [secret, secret[4:-1], _GREP_SECRETS[kind]]),
        _scrub_all(control_pages, [control, control[4:-1], GREP_CONTROL]),
    )
    assert not differences, "\n".join(differences)
    ids = _row_ids(secret_pages)
    assert len(ids) == len(set(ids)) and set(ids) == readable
    assert not set(ids) & set(MIX_HIDDEN)
    for unreadable in (":hidden:", ":masked:", ":dropped:"):
        assert unreadable not in json.dumps(ids)


@pytest.mark.parametrize("secret", ["hidden", "masked"])
def test_grep_continuations_at_a_low_scan_bound_answer_alike_for_a_withheld_value(
    clean_runtime, tmp_path, monkeypatch, secret,
):
    """Every comment holds the query words, and the scan bound is 2 admitted
    units; the attribute filter names a value only hidden or masked
    comments hold, or one nobody stored. Every page and token must match
    (binding in the clear, sealed cursor by length), and the readable
    `shared` value shows the bound does stop pages and continue by token."""
    from okg.substrate.mcp import grep_search
    from okg.substrate.mcp import server as srv

    deployment = _parity_deployment(tmp_path, clean_runtime)
    monkeypatch.setattr(grep_search, "PAGE_SCAN_BOUND", 2)
    value = HIDDEN_VALUE if secret == "hidden" else MASKED_VALUE

    def build(v: str) -> dict[str, Any]:
        return {"query": QUERY, "method": "grep", "limit": LIMIT,
                "filter": {"subtype": "comment", "attr_filters": {"assignee": v}}}

    _enforce(monkeypatch)
    mcp = _server(srv, clean_runtime, deployment)
    try:
        secret_pages = _pages(mcp, build(value))
        control_pages = _pages(mcp, build(CONTROL_VALUE))
        shared_pages = _pages(mcp, build(SHARED_VALUE))
    finally:
        _close(mcp)
    differences = _diff(_scrub_all(secret_pages, [value]),
                        _scrub_all(control_pages, [CONTROL_VALUE]))
    assert not differences, "\n".join(differences)
    # The bound does stop pages here: the readable `shared` comments fill
    # it, their first page carries a token, and following tokens reaches
    # every one. Only admitted units are charged (#3384), so the control,
    # which admits none, ends on its first page.
    assert shared_pages[0]["ok"].get("continuation_token"), shared_pages[0]
    assert len(shared_pages) >= 2
    assert sorted(_row_ids(shared_pages)) == sorted(SHARED_READABLE)
    assert len(control_pages) == 1 and not _row_ids(control_pages)


@pytest.mark.parametrize("scope_kind", ["record", "records"])
def test_windows_over_readable_and_hidden_mixed_records_answer_as_the_readable_ones(
    clean_runtime, tmp_path, monkeypatch, scope_kind,
):
    """A windows scope naming readable and hidden `mix` notes answers the
    secret pattern as the control: the hidden notes' match is never seen."""
    from okg.substrate.mcp import server as srv

    deployment = _grep_deployment(tmp_path, clean_runtime)
    _enforce(monkeypatch)
    scope = ({"record": "note:mix:0"} if scope_kind == "record"
             else {"records": ["note:mix:0", "note:mix:1", "note:mix:2", "note:mix:3"]})
    control, secrets = _MIX_PATTERNS["group"]
    secret = secrets["hidden"]
    mcp = _server(srv, clean_runtime, deployment)
    try:
        secret_pages = _pages(mcp, _windows(secret, scope))
        control_pages = _pages(mcp, _windows(control, scope))
        readable_pages = _pages(mcp, _windows(secret, {"record": "note:mix:1"}))
    finally:
        _close(mcp)
    differences = _diff(_scrub_all(secret_pages, [secret, secret[4:-1]]),
                        _scrub_all(control_pages, [control, control[4:-1]]))
    assert not differences, "\n".join(differences)
    # The same pattern on a readable mix note does match it.
    assert _row_ids(readable_pages) == ["note:mix:1"]
    expected = [] if scope_kind == "record" else ["note:mix:1", "note:mix:3"]
    assert sorted(_row_ids(secret_pages)) == expected


def _served_items(mcp, subtype: str) -> list[dict[str, Any]]:
    return mcp._tool_manager._tools["filter"].fn(
        input={"kind": "nodes", "subtype": subtype},
        predicate={"op": "exists", "field": "node_id"}, limit=50)["items"]


def test_the_grep_fixture_masks_and_drops_text(clean_runtime, tmp_path, monkeypatch):
    """Fixture guard: masked notes and comments are served with the mask
    token, dropped pages and memos without a `text`, and all are readable
    rows (so the term cases test masking and dropping, not hiding)."""
    from okg.substrate.mcp import server as srv

    deployment = _grep_deployment(tmp_path, clean_runtime)
    _enforce(monkeypatch)
    mcp = _server(srv, clean_runtime, deployment)
    try:
        rows = {row["node_id"]: row
                for subtype in ("note", "comment", "page", "memo")
                for row in _served_items(mcp, subtype)}
    finally:
        _close(mcp)
    assert not any(":hidden:" in node_id for node_id in rows)
    for masked in ("note:masked:0", "comment:masked:0"):
        assert "[nomos:masked]" in str(rows[masked]["attrs"].get("text"))
    for dropped in ("page:dropped:0", "memo:dropped:0"):
        assert "text" not in rows[dropped]["attrs"]
    served = json.dumps(rows, default=str)
    assert GREP_MASKED not in served and GREP_DROPPED not in served


# `filter` and `inspect` node served `text_hash`, the unsalted MD5 of the
# stored text, on a row whose text Nomos masks or drops (#3378, fixed by
# #3475); every operator below now guards against its return.


def _hash_reads(mcp, operator: str, node_id: str, subtype: str) -> Any:
    tools = mcp._tool_manager._tools
    if operator == "filter":
        return [r for r in _served_items(mcp, subtype) if r["node_id"] == node_id]
    if operator == "map":
        return tools["map"].fn(
            input={"kind": "nodes", "subtype": subtype,
                   "filter": {"op": "eq", "field": "node_id", "value": node_id}},
            projection=["node_id", "text_hash"], limit=5)
    if operator == "inspect_node":
        return tools["inspect"].fn(target={"kind": "node", "node_id": node_id})
    if operator == "search_attrs":
        return tools["search"].fn(method="attrs", limit=50, subtype_filter=subtype,
                                  where={"op": "eq", "field": "node_id", "value": node_id})
    assert operator == "search_lexical"
    return tools["search"].fn(query="zirconium", method="lexical", limit=50,
                              filter={"subtype": subtype})


def _hash_params():
    out = []
    for operator in ("filter", "map", "inspect_node", "search_attrs", "search_lexical"):
        for kind, node_id, subtype, text in (
            ("masked", "note:masked:0", "note", f"zirconium masked note {GREP_MASKED} 0."),
            ("dropped", "page:dropped:0", "page", f"zirconium dropped page {GREP_DROPPED} 0."),
        ):
            out.append(pytest.param(operator, node_id, subtype, text,
                                    id=f"{operator}-{kind}"))
    return out


@pytest.mark.parametrize("operator,node_id,subtype,text", _hash_params())
def test_a_withheld_text_is_not_served_as_its_hash(
    clean_runtime, tmp_path, monkeypatch, operator, node_id, subtype, text,
):
    """A withheld text and an absent one answer alike only if no digest of
    the stored text is served: the hash of a guess must not match. With
    Nomos off the same read does serve the row (and its hash), so the
    check is not vacuous."""
    import hashlib

    from okg.substrate.mcp import server as srv

    deployment = _grep_deployment(tmp_path, clean_runtime)
    guess = hashlib.md5(text.encode("utf-8")).hexdigest()
    _nomos_off(monkeypatch)
    mcp = _server(srv, clean_runtime, deployment)
    try:
        published = _hash_reads(mcp, operator, node_id, subtype)
    finally:
        _close(mcp)
    assert node_id in json.dumps(published, default=str), published
    _enforce(monkeypatch)
    mcp = _server(srv, clean_runtime, deployment)
    try:
        served = _hash_reads(mcp, operator, node_id, subtype)
    finally:
        _close(mcp)
    assert guess not in json.dumps(served, default=str)


# ---------- #3201: a mask-tokened attribute value in lexical and grep ----------

# `code` is unclassified on `comment`, so a masked comment's code is served
# as the mask token. "zulu" is stored on two masked comments, more than the
# page limit of one; "nobody-code" on none; READABLE_CODE on the readable one.
_CODE_SECRET = "zulu"
_CODE_CONTROL = "nobody-code"
_CODE_CASES: dict[str, Callable[[str], dict[str, Any]]] = {
    "attrs_attr_filters": lambda v: {
        "method": "attrs", "limit": 1,
        "filter": {"subtype": "comment", "attr_filters": {"code": v}}},
    "lexical_attr_filters": lambda v: {
        "query": QUERY, "method": "lexical", "limit": 1,
        "filter": {"subtype": "comment", "attr_filters": {"code": v}}},
    "lexical_where": lambda v: {
        "query": QUERY, "method": "lexical", "limit": 1, "subtype_filter": "comment",
        "where": {"op": "eq", "field": "attrs.code", "value": v}},
    "grep_attr_filters": lambda v: {
        "query": QUERY, "method": "grep", "limit": 1,
        "filter": {"subtype": "comment", "attr_filters": {"code": v}}},
    "grep_where": lambda v: {
        "query": QUERY, "method": "grep", "limit": 1, "subtype_filter": "comment",
        "where": {"op": "eq", "field": "attrs.code", "value": v}},
}


@pytest.mark.parametrize("case", sorted(_CODE_CASES))
def test_a_mask_tokened_value_answers_as_an_absent_one(
    clean_runtime, tmp_path, monkeypatch, case,
):
    from okg.substrate.mcp import server as srv

    deployment = _parity_deployment(tmp_path, clean_runtime)
    _enforce(monkeypatch)
    build = _CODE_CASES[case]
    mcp = _server(srv, clean_runtime, deployment)
    try:
        secret_page = _call(mcp, "search", build(_CODE_SECRET))
        control_page = _call(mcp, "search", build(_CODE_CONTROL))
        visible_page = _call(mcp, "search", build(READABLE_CODE))
    finally:
        _close(mcp)
    differences = _diff(_scrub_all(secret_page, [_CODE_SECRET]),
                        _scrub_all(control_page, [_CODE_CONTROL]))
    assert not differences, "\n".join(differences)
    assert "ok" in visible_page, visible_page
    assert "comment:visible" in json.dumps(visible_page["ok"], default=str)


# ---------- #3377 / #3384: failure cases for the readable-text check ----------
#
# A matched unit counts only when the reader reads its text as stored (the
# shared check in `readable_text`). The cases below pin the ways that check
# could be bypassed: a query for the mask token's own words, Nomos `audit`
# (which still hides rows without source lineage and masked text), a row
# with no stored read class, masked hits that outrank readable ones on a
# later page, and a continuation token whose length could count hidden rows.

def _set_nodes(dsn: str, sql: str, like: list[str]) -> int:
    """Run `UPDATE <node segment> SET <sql> WHERE node_id LIKE ANY(like)`."""
    from okg.substrate.mcp.semantic_readiness import resolve_node_segment

    with psycopg.connect(dsn, autocommit=True) as conn:
        segment = resolve_node_segment(conn)
        return conn.execute(
            f"UPDATE {segment} SET {sql} WHERE node_id LIKE ANY(%s)", (like,),
        ).rowcount


def _strip_lineage(dsn: str, like: list[str]) -> None:
    """Rows with no source lineage: no `source` and no lineage attribute.
    Even `audit` hides such a row (decision 123a)."""
    from okg.substrate.nomos.source_lineage import LINEAGE_ATTR

    assert _set_nodes(dsn, f"source = NULL, attrs = attrs - '{LINEAGE_ATTR}'", like)


def _secret_vs_control(mcp, build, secret: str, control: str) -> tuple[list, list]:
    secret_pages = _pages(mcp, build(secret))
    control_pages = _pages(mcp, build(control))
    differences = _diff(_scrub_all(secret_pages, [secret]),
                        _scrub_all(control_pages, [control]))
    assert not differences, "\n".join(differences)
    return secret_pages, control_pages


_MASK_TERM_CASES = ("lexical", "lexical_page2", "lexical_self", "lexical_subtype", "grep",
                    "grep_bound3")


@pytest.mark.parametrize("term", ["MASKED", "NOMOS"])
@pytest.mark.parametrize("case", _MASK_TERM_CASES)
def test_the_mask_token_s_words_answer_as_an_absent_term(
    clean_runtime, tmp_path, monkeypatch, case, term,
):
    """"masked" is in the stored text of every masked note and comment AND in
    the mask token `[nomos:masked]` the reader is served; "nomos" is only in
    the token. Neither may find a masked row: the hit is dropped, never
    re-matched against the masked text. Upper case, so the scrub of the
    echoed query cannot touch a lower-case field name."""
    from okg.substrate.mcp import grep_search
    from okg.substrate.mcp import server as srv

    deployment = _grep_deployment(tmp_path, clean_runtime)
    build, _ = _TERM_CASES[case]
    if case.endswith("_bound3"):
        monkeypatch.setattr(grep_search, "PAGE_SCAN_BOUND", _BOUND3)
    if term == "MASKED":
        _nomos_off(monkeypatch)
        mcp = _server(srv, clean_runtime, deployment)
        try:
            published = _pages(mcp, build(term, "masked"))
        finally:
            _close(mcp)
        assert any(":masked:" in node_id for node_id in _row_ids(published)), published
    _enforce(monkeypatch)
    mcp = _server(srv, clean_runtime, deployment)
    try:
        secret_pages, _ = _secret_vs_control(
            mcp, lambda t: build(t, "masked"), term, GREP_CONTROL,
        )
    finally:
        _close(mcp)
    assert not _row_ids(secret_pages)


_AUDIT_CASES = ("lexical", "lexical_page2", "lexical_self", "lexical_subtype", "grep",
                "grep_page2", "grep_bound3", "grep_subtype_bound3")


@pytest.mark.parametrize("kind", ["masked", "dropped", "nolineage"])
@pytest.mark.parametrize("case", _AUDIT_CASES)
def test_under_audit_a_term_only_rows_audit_hides_hold_answers_as_an_absent_term(
    clean_runtime, tmp_path, monkeypatch, case, kind,
):
    """`audit` shows a refused row, but still hides a row with no source
    lineage and still masks or drops text. A term only such rows hold must
    answer as a term nobody stored: rows, page facts, the grep count (no
    stored-row SQL count under audit) and the page scan bound."""
    from okg.substrate.mcp import grep_search
    from okg.substrate.mcp import server as srv

    deployment = _grep_deployment(tmp_path, clean_runtime)
    if kind == "nolineage":
        # Every row holding GREP_HIDDEN, and only those, loses its lineage.
        _strip_lineage(clean_runtime, ["note:hidden:%", "comment:hidden:%",
                                       *MIX_HIDDEN])
    secret = GREP_HIDDEN if kind == "nolineage" else _GREP_SECRETS[kind]
    term_kind = "hidden" if kind == "nolineage" else kind
    build, _ = _TERM_CASES[case]
    if case.endswith("_bound3"):
        monkeypatch.setattr(grep_search, "PAGE_SCAN_BOUND", _BOUND3)
    _nomos_off(monkeypatch)
    mcp = _server(srv, clean_runtime, deployment)
    try:
        published = _pages(mcp, build(secret, term_kind))
    finally:
        _close(mcp)
    assert _row_ids(published) and _holds(published, term_kind, secret), published
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "audit")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", READER)
    mcp = _server(srv, clean_runtime, deployment)
    try:
        secret_pages, _ = _secret_vs_control(
            mcp, lambda t: build(t, term_kind), secret, GREP_CONTROL,
        )
        # Not a refusal both sides share: under audit a refused but
        # lineage-carrying hidden row IS shown.
        if kind != "nolineage":
            shown = _pages(mcp, build(GREP_HIDDEN, "hidden"))
            assert any(":hidden:" in node_id for node_id in _row_ids(shown)), shown
    finally:
        _close(mcp)
    assert not _row_ids(secret_pages)


@pytest.mark.parametrize("case", ["lexical", "lexical_self", "grep", "grep_bound3"])
def test_a_masked_row_with_no_stored_read_class_is_dropped(
    clean_runtime, tmp_path, monkeypatch, case,
):
    """Fail closed: with its stored read class cleared, a masked row is still
    decided on what the reader reads, and its masked text still matches
    nothing."""
    from okg.substrate.mcp import grep_search
    from okg.substrate.mcp import server as srv

    deployment = _grep_deployment(tmp_path, clean_runtime)
    _set_nodes(clean_runtime, "nomos_read_class = NULL, nomos_read_class_version = NULL",
               ["note:masked:%", "comment:masked:%"])
    build, _ = _TERM_CASES[case]
    if case.endswith("_bound3"):
        monkeypatch.setattr(grep_search, "PAGE_SCAN_BOUND", _BOUND3)
    _enforce(monkeypatch)
    mcp = _server(srv, clean_runtime, deployment)
    try:
        secret_pages, _ = _secret_vs_control(
            mcp, lambda t: build(t, "masked"), GREP_MASKED, GREP_CONTROL,
        )
    finally:
        _close(mcp)
    assert not _row_ids(secret_pages)


_RANKED_MIX_CASES: dict[str, Callable[[str], dict[str, Any]]] = {
    "lexical": lambda q: {"query": q, "method": "lexical", "limit": LIMIT},
    "lexical_page2": lambda q: {"query": q, "method": "lexical", "limit": LIMIT,
                                "offset": LIMIT},
    "lexical_self": lambda q: {"query": q, "method": "lexical", "limit": LIMIT,
                               "rank_by": "self"},
}


@pytest.mark.parametrize("kind", ["masked", "dropped"])
@pytest.mark.parametrize("case", sorted(_RANKED_MIX_CASES))
def test_masked_hits_ranked_above_readable_ones_leave_the_readable_pages_alone(
    clean_runtime, tmp_path, monkeypatch, case, kind,
):
    """Both words of the secret query rank: "zirconium", which every row
    holds, and the masked (or dropped) rows' rare word, which puts them on
    top of the stored ranking. The control swaps the rare word for one
    nobody stored. Every page, the second included, must be the control's:
    the unreadable hits neither take a place nor move a readable row."""
    from okg.substrate.mcp import server as srv

    deployment = _grep_deployment(tmp_path, clean_runtime)
    build = _RANKED_MIX_CASES[case]
    secret = f"zirconium {_GREP_SECRETS[kind]}"
    control = f"zirconium {GREP_CONTROL}"
    _nomos_off(monkeypatch)
    mcp = _server(srv, clean_runtime, deployment)
    try:
        first = _call(mcp, "search", {"query": secret, "method": "lexical",
                                      "limit": LIMIT, "rank_by": "self"})
    finally:
        _close(mcp)
    # The unreadable rows lead the stored ranking.
    assert all(f":{kind}:" in str(row["node_id"]) for row in first["ok"]["results"]), first
    _enforce(monkeypatch)
    mcp = _server(srv, clean_runtime, deployment)
    try:
        secret_pages, _ = _secret_vs_control(mcp, build, secret, control)
    finally:
        _close(mcp)
    ids = _row_ids(secret_pages)
    assert ids, secret_pages
    for unreadable in (":hidden:", ":masked:", ":dropped:"):
        assert unreadable not in json.dumps(ids)


def test_a_sealed_cursor_s_length_does_not_count_its_resume_point():
    """A grep continuation's resume point counts every row the scan read,
    hidden ones included; it travels as a fixed-width integer outside the
    compressed cursor, so the token's length is the same whatever it is."""
    from okg.substrate.mcp.continuation import seal_cursor, unseal_cursor

    cursor = {"listed": ["note:mix:1", "note:mix:3"], "path": {"id_set": True}}
    lengths = set()
    for resume in (0, 3, 7, 12, 99, 4_567, 10**9, 2**63):
        sealed = seal_cursor(cursor, fixed={"resume": resume})
        lengths.add(len(sealed))
        assert unseal_cursor(sealed) == {**cursor, "resume": resume}
    assert len(lengths) == 1, lengths


def test_the_readable_text_check_keeps_only_text_read_as_stored():
    """The shared check, row by row: a row the read dropped, masked, or
    served without its text is left out; a row with no stored read class
    is judged the same way (fail closed); other masked attributes do not
    matter; with no redacting policy the read rows pass unchanged."""
    from okg.substrate.mcp.readable_text import keep_readable_text

    stored = [
        {"node_id": "kept", "attrs": {"text": "alpha", "code": "k"}},
        {"node_id": "masked", "attrs": {"text": "secret words"}},
        {"node_id": "dropped", "attrs": {"text": "secret words"}},
        {"node_id": "hidden", "attrs": {"text": "secret words"}},
        {"node_id": "noclass", "attrs": {"text": "secret words"}, "nomos_read_class": None},
    ]
    read = [
        {"node_id": "kept", "attrs": {"text": "alpha", "code": "[nomos:masked]"}},
        {"node_id": "masked", "attrs": {"text": "[nomos:masked]"}},
        {"node_id": "dropped", "attrs": {}},
        {"node_id": "noclass", "attrs": {"text": "[nomos:masked]"}},
    ]
    assert [r["node_id"] for r in keep_readable_text(stored, read, redaction=True)] == ["kept"]
    assert keep_readable_text(stored, read, redaction=False) == read


def test_semantic_search_never_ranks_a_row_by_text_the_reader_cannot_read(
    clean_runtime, tmp_path, monkeypatch,
):
    """#3377 for semantic search: the ranking is over embeddings of the
    stored text, so a masked or dropped row ranked by its hidden text would
    confirm what that text says. Querying with a masked note's exact text
    puts that note first with Nomos off; under enforce no masked or dropped
    row is served at all, and readable rows still are."""
    from okg.substrate.embeddings import HashEmbedder
    from okg.substrate.mcp import open_session, search
    from okg.substrate.mcp.semantic_readiness import resolve_node_segment

    deployment = _grep_deployment(tmp_path, clean_runtime)
    embedder = HashEmbedder()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        segment = resolve_node_segment(conn)
        rows = conn.execute(
            f"SELECT node_id, attrs->>'text' FROM {segment} "
            "WHERE deleted = FALSE AND attrs ? 'text'"
        ).fetchall()
        for node_id, text in rows:
            vec = embedder.embed(text)
            conn.execute(
                f"UPDATE {segment} SET embedding = %s::vector, embedding_model = %s "
                "WHERE node_id = %s",
                ("[" + ",".join(f"{v:.6f}" for v in vec) + "]", embedder.model_id, node_id),
            )
    query = f"zirconium masked note {GREP_MASKED} 0."

    def ids(**session_kwargs) -> list[str]:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            session = open_session(conn, deployment_name=deployment, **session_kwargs)
            page = search(conn, session, query, method="semantic", embedder=embedder,
                          limit=50)
        if session_kwargs:
            # The query echoes the secret word; no served row may hold it.
            assert GREP_MASKED not in json.dumps(page["results"], default=str)
        return [str(row["node_id"]) for row in page["results"]]

    _nomos_off(monkeypatch)
    assert ids()[0] == "note:masked:0"
    _enforce(monkeypatch)
    served = ids(nomos_subject=READER)
    assert served and "note:vis" in served
    assert not [i for i in served if ":masked:" in i or ":dropped:" in i or ":hidden:" in i]


def test_hnsw_semantic_search_never_ranks_a_row_the_reader_cannot_read(
    clean_runtime, tmp_path, monkeypatch,
):
    """The same guarantee through the HNSW read (PACT
    openrouter-embeddings-hnsw), which queries the node segment directly:
    the Nomos read filter still runs on every ranked row."""
    from okg.substrate.embeddings import HashEmbedder
    from okg.substrate.mcp import open_session, search
    from okg.substrate.mcp.semantic_readiness import resolve_node_segment
    from okg.substrate.semantic_ann import ann_spec_for_model_id, ensure_ann_index

    model_id = "openrouter:google/gemini-embedding-2@768"
    native = HashEmbedder(dimension=768)

    class _Padded:
        dimension = 2560

        def __init__(self) -> None:
            self.model_id = model_id

        def embed(self, text: str) -> list[float]:
            return native.embed(text) + [0.0] * (2560 - 768)

    embedder = _Padded()
    deployment = _grep_deployment(tmp_path, clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        segment = resolve_node_segment(conn)
        rows = conn.execute(
            f"SELECT node_id, attrs->>'text' FROM {segment} "
            "WHERE deleted = FALSE AND attrs ? 'text'"
        ).fetchall()
        for node_id, text in rows:
            vec = embedder.embed(text)
            conn.execute(
                f"UPDATE {segment} SET embedding = %s::vector, embedding_model = %s "
                "WHERE node_id = %s",
                ("[" + ",".join(f"{v:.6f}" for v in vec) + "]", model_id, node_id),
            )
    ensure_ann_index(clean_runtime, ann_spec_for_model_id(model_id))
    query = f"zirconium masked note {GREP_MASKED} 0."

    def ids(**session_kwargs) -> list[str]:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            session = open_session(conn, deployment_name=deployment, **session_kwargs)
            page = search(conn, session, query, method="semantic", embedder=embedder,
                          limit=50)
        if session_kwargs:
            assert GREP_MASKED not in json.dumps(page["results"], default=str)
        return [str(row["node_id"]) for row in page["results"]]

    _nomos_off(monkeypatch)
    assert ids()[0] == "note:masked:0"
    _enforce(monkeypatch)
    served = ids(nomos_subject=READER)
    assert served and "note:vis" in served
    assert not [i for i in served if ":masked:" in i or ":dropped:" in i or ":hidden:" in i]


@pytest.mark.parametrize("exact_max, ranking", [(5_000, "exact"), (1, "approximate")])
def test_vector_set_semantic_search_never_ranks_a_row_the_reader_cannot_read(
    clean_runtime, tmp_path, monkeypatch, exact_max, ranking,
):
    """The same guarantee through a vector set (PACT
    openrouter-embeddings-hnsw, design v3.2), whose scope -- the reader's
    access rules included -- is applied before ranking, in both the exact
    and the index-walk ranking. Under enforced rules the page carries no
    coverage counts."""
    import okg.substrate.semantic_ann as semantic_ann
    from okg.substrate.embeddings import HashEmbedder
    from okg.substrate.embeddings.config import (
        build_embedding_execution_plan,
        parse_embedding_config,
    )
    from okg.substrate.embeddings.store import (
        ensure_set_index,
        record_fill_completion,
        register_sets,
        write_node_documents,
        write_vectors,
    )
    from okg.substrate.mcp import open_session, search
    from okg.substrate.mcp.semantic_readiness import resolve_node_segment
    from okg.substrate.publisher import Publisher
    from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer

    monkeypatch.setattr(semantic_ann, "EXACT_SCOPE_MAX", exact_max)
    deployment = _grep_deployment(tmp_path, clean_runtime)
    block = {"enabled": True, "sets": {"main": {"backend": "hash"}}}
    plan = build_embedding_execution_plan(parse_embedding_config(plural=block))
    vector_set = plan.config.search_vector_set
    hashed = HashEmbedder()
    with psycopg.connect(clean_runtime) as conn:
        register_sets(conn, [vector_set])
        conn.commit()
    ensure_set_index(clean_runtime, vector_set)
    with psycopg.connect(clean_runtime) as conn:
        segment = resolve_node_segment(conn)
        rows = [
            {"node_id": r[0], "subtype": r[1], "attrs": r[2]}
            for r in conn.execute(
                f"SELECT DISTINCT ON (node_id) node_id, subtype, attrs FROM {segment} "
                "WHERE deleted = FALSE AND attrs ? 'text' ORDER BY node_id, live_lo DESC"
            ).fetchall()
        ]
        # The rows were written before the set existed: link them now, as
        # the graph writer would have.
        assert write_node_documents(conn, rows) > 0
        texts = {row["node_id"]: row["attrs"]["text"] for row in rows}
        links = conn.execute(
            "SELECT node_id, doc_hash FROM okg.node_documents WHERE set_id = %s",
            (vector_set.set_id,),
        ).fetchall()
        write_vectors(conn, vector_set.set_id, [
            (str(doc), hashed.embed(texts[str(node_id)])) for node_id, doc in links
        ])
        # Every document has its vector: the fill is complete, so the
        # next watermark routes semantic search to the set.
        record_fill_completion(conn, vector_set.set_id, "fixture")
        conn.commit()
    published = Publisher(
        clean_runtime, deployment,
        catalog_identity=bind_test_catalog_writer(clean_runtime, deployment),
    ).cycle()
    assert published["status"] == "published", published
    query = f"zirconium masked note {GREP_MASKED} 0."

    def page(**session_kwargs) -> dict:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            session = open_session(conn, deployment_name=deployment, **session_kwargs)
            out = search(conn, session, query, method="semantic", embedder=plan, limit=50)
        assert out["vector_source"] == "vector_set" and out["ranking"] == ranking
        if session_kwargs:
            assert GREP_MASKED not in json.dumps(out["results"], default=str)
        return out

    _nomos_off(monkeypatch)
    open_page = page()
    assert open_page["results"][0]["node_id"] == "note:masked:0"
    assert "embedding_coverage" in open_page
    _enforce(monkeypatch)
    enforced = page(nomos_subject=READER)
    served = [str(row["node_id"]) for row in enforced["results"]]
    assert served and "note:vis" in served
    assert not [i for i in served if ":masked:" in i or ":dropped:" in i or ":hidden:" in i]
    assert enforced["counts_withheld"] == "nomos_enforce"
    assert "embedding_coverage" not in enforced and "rank_capped" not in enforced


def test_must_fail_a_filling_vector_set_page_shows_no_count_under_access_rules(
    clean_runtime, tmp_path, monkeypatch,
):
    """PACT openrouter-embeddings-hnsw, req.read: under enforced access rules
    no coverage count is shown. A set with vectors but no completed fill
    ("filling") leaves the read on the node columns, and the page's
    `vector_set` names the set's state. Its coverage counts every vector of
    the set at the pin, documents of unreadable nodes included, so it is
    withheld from an enforced reader, and so is the status that tells
    "filling" from "no_vectors" (`unfilled`). Must-fail (review P7 B2): the
    page carried `vector_set.coverage.vectors_at_pin` under enforcement;
    (review P8 S2) its status said "filling"."""
    from okg.substrate.embeddings import HashEmbedder
    from okg.substrate.embeddings.config import (
        build_embedding_execution_plan,
        parse_embedding_config,
    )
    from okg.substrate.embeddings.store import (
        register_sets,
        write_node_documents,
        write_vectors,
    )
    from okg.substrate.mcp import open_session, search
    from okg.substrate.mcp.semantic_readiness import resolve_node_segment
    from okg.substrate.publisher import Publisher
    from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer

    deployment = _grep_deployment(tmp_path, clean_runtime)
    block = {"enabled": True, "sets": {"main": {
        "backend": "hash", "index": {"kind": "none"},
    }}}
    config = parse_embedding_config(plural=block)
    plan = build_embedding_execution_plan(config)
    vector_set = config.search_vector_set
    hashed = HashEmbedder()
    with psycopg.connect(clean_runtime) as conn:
        register_sets(conn, [vector_set])
        conn.commit()
    with psycopg.connect(clean_runtime) as conn:
        segment = resolve_node_segment(conn)
        rows = [
            {"node_id": r[0], "subtype": r[1], "attrs": r[2]}
            for r in conn.execute(
                f"SELECT DISTINCT ON (node_id) node_id, subtype, attrs FROM {segment} "
                "WHERE deleted = FALSE AND attrs ? 'text' ORDER BY node_id, live_lo DESC"
            ).fetchall()
        ]
        # The node columns hold the hash vectors (the old single-backend
        # form), so they can answer while the set fills.
        for row in rows:
            vec = hashed.embed(row["attrs"]["text"])
            conn.execute(
                f"UPDATE {segment} SET embedding = %s::vector, embedding_model = %s "
                "WHERE node_id = %s",
                ("[" + ",".join(f"{v:.6f}" for v in vec) + "]", hashed.model_id,
                 row["node_id"]),
            )
        assert write_node_documents(conn, rows) > 0
        # One vector, of an unreadable node's document, and no completed fill.
        (doc,) = conn.execute(
            "SELECT doc_hash FROM okg.node_documents "
            "WHERE set_id = %s AND node_id = 'note:masked:0'",
            (vector_set.set_id,),
        ).fetchone()
        write_vectors(conn, vector_set.set_id, [(str(doc), hashed.embed("x"))])
        conn.commit()
    published = Publisher(
        clean_runtime, deployment,
        catalog_identity=bind_test_catalog_writer(clean_runtime, deployment),
    ).cycle()
    assert published["status"] == "published", published
    query = f"zirconium masked note {GREP_MASKED} 0."

    def page(status: str, **session_kwargs) -> dict:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            session = open_session(conn, deployment_name=deployment, **session_kwargs)
            out = search(conn, session, query, method="semantic", embedder=plan, limit=50)
        assert out["vector_source"] == "node_columns"
        assert out["vector_set"]["status"] == status, out["vector_set"]
        return out

    _nomos_off(monkeypatch)
    open_page = page("filling")
    assert open_page["vector_set"]["coverage"] == {
        "vectors_at_pin": 1, "count_bound": 10_000,
    }
    _enforce(monkeypatch)
    # Review P8 S2: "filling" versus "no_vectors" would tell the reader
    # whether the set holds any vector at the pin, a set-wide bit over
    # documents of nodes it cannot read; both read "unfilled".
    enforced = page("unfilled", nomos_subject=READER)
    assert enforced["counts_withheld"] == "nomos_enforce"
    assert "coverage" not in enforced["vector_set"], enforced["vector_set"]
    assert "vectors_at_pin" not in json.dumps(enforced, default=str)
    served = [str(row["node_id"]) for row in enforced["results"]]
    assert served
    assert not [i for i in served if ":masked:" in i or ":dropped:" in i or ":hidden:" in i]


def test_backfill_writes_no_node_rows_and_masked_rows_stay_unserved(
    clean_runtime, tmp_path, monkeypatch,
):
    """PACT openrouter-embeddings-hnsw (design v3.2): `okg db embed-backfill`
    writes vectors and document links only. Rows written before the set
    existed get their links from it; no node row version is written, so every
    stored Nomos read class (hidden, masked and dropped rows included) is
    untouched, and a masked row ranked by its own vector is still not served
    to the reader."""
    from okg.substrate.embeddings.backfill import run_set_backfill
    from okg.substrate.embeddings.config import (
        build_embedding_execution_plan,
        parse_embedding_config,
    )
    from okg.substrate.embeddings.store import register_sets
    from okg.substrate.mcp import open_session, search
    from okg.substrate.mcp.semantic_readiness import resolve_node_segment
    from okg.substrate.publisher import Publisher
    from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer

    deployment = _grep_deployment(tmp_path, clean_runtime)
    # The document is the body, so a query equal to a node's text ranks it first.
    block = {"enabled": True, "sets": {"main": {
        "backend": "hash", "document": {"default": {"header": [], "body": "text"}},
        "index": {"kind": "none"},
    }}}
    config = parse_embedding_config(plural=block)
    with psycopg.connect(clean_runtime) as conn:
        register_sets(conn, list(config.vector_sets))
        conn.commit()

    def versions() -> tuple[dict[str, tuple], int]:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            segment = resolve_node_segment(conn)
            rows = conn.execute(
                f"SELECT DISTINCT ON (node_id) node_id, nomos_read_class, "
                f"nomos_read_class_version FROM {segment} "
                "WHERE deleted = FALSE AND attrs ? 'text' "
                "ORDER BY node_id, live_lo DESC"
            ).fetchall()
            total = conn.execute(f"SELECT count(*) FROM {segment}").fetchone()[0]
        return {r[0]: (r[1], r[2]) for r in rows}, int(total)

    before = versions()
    unreadable = [
        n for n in before[0] if ":hidden:" in n or ":masked:" in n or ":dropped:" in n
    ]
    assert unreadable and all(before[0][n][0] is not None for n in unreadable)

    report = run_set_backfill(
        clean_runtime, deployment=deployment, config=config, set_name="main",
        max_usd=None,
    )
    assert report["links_written"] == len(before[0])
    assert report["documents_embedded"] > 0 and report["node_rows_written"] == 0
    assert versions() == before

    published = Publisher(
        clean_runtime, deployment,
        catalog_identity=bind_test_catalog_writer(clean_runtime, deployment),
    ).cycle()
    assert published["status"] == "published" and not published.get("reused_generation")
    assert versions()[0] == before[0]
    plan = build_embedding_execution_plan(config)
    query = f"zirconium masked note {GREP_MASKED} 0."

    def ids(**session_kwargs) -> list[str]:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            session = open_session(conn, deployment_name=deployment, **session_kwargs)
            page = search(conn, session, query, method="semantic",
                          embedder=plan, limit=50)
        assert page["vector_source"] == "vector_set"
        if session_kwargs:
            assert GREP_MASKED not in json.dumps(page["results"], default=str)
        return [str(row["node_id"]) for row in page["results"]]

    _nomos_off(monkeypatch)
    assert ids()[0] == "note:masked:0"
    _enforce(monkeypatch)
    served = ids(nomos_subject=READER)
    assert served
    assert not [i for i in served if ":masked:" in i or ":dropped:" in i or ":hidden:" in i]


@pytest.mark.parametrize("exact_max, ranking", [(5_000, "exact"), (1, "approximate")])
def test_the_access_rule_predicate_fills_a_vector_set_page_ahead_of_unreadable_rows(
    clean_runtime, tmp_path, monkeypatch, exact_max, ranking,
):
    """Every unreadable row (hidden, masked, dropped) sits nearer the query
    than every readable one, and the raw window is held at `limit + 1`
    rows. The access rules applied in SQL before ranking leave only readable
    rows to rank, so the page is full. Must-fail variant in the same test:
    with the SQL predicate removed (the per-row filter still runs after
    ranking), the window holds only unreadable rows and the search cannot
    serve a page."""
    import importlib

    from okg.substrate.errors import SubstrateError

    import okg.substrate.semantic_ann as semantic_ann
    from okg.substrate.embeddings import HashEmbedder
    # `okg.substrate.mcp.search` the attribute is the function; the module:
    search_module = importlib.import_module("okg.substrate.mcp.search")
    from okg.substrate.embeddings.config import (
        build_embedding_execution_plan,
        parse_embedding_config,
    )
    from okg.substrate.embeddings.store import (
        ensure_set_index,
        record_fill_completion,
        register_sets,
        write_node_documents,
        write_vectors,
    )
    from okg.substrate.mcp import open_session, search
    from okg.substrate.mcp.semantic_readiness import resolve_node_segment
    from okg.substrate.publisher import Publisher
    from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer

    monkeypatch.setattr(semantic_ann, "EXACT_SCOPE_MAX", exact_max)
    limit = 3
    monkeypatch.setattr(search_module, "_SEMANTIC_RAW_FETCH_CAP", limit + 1)
    deployment = _grep_deployment(tmp_path, clean_runtime)
    block = {"enabled": True, "sets": {"main": {"backend": "hash"}}}
    plan = build_embedding_execution_plan(parse_embedding_config(plural=block))
    vector_set = plan.config.search_vector_set
    query = "which rows sit nearest"
    q = plan.embed(query)
    noise = HashEmbedder()

    def toward(node_id: str, sign: float) -> list[float]:
        values = [sign * a + 0.3 * b for a, b in zip(q, noise.embed(f"noise:{node_id}"))]
        norm = sum(v * v for v in values) ** 0.5
        return [v / norm for v in values]

    with psycopg.connect(clean_runtime) as conn:
        register_sets(conn, [vector_set])
        conn.commit()
    ensure_set_index(clean_runtime, vector_set)
    with psycopg.connect(clean_runtime) as conn:
        segment = resolve_node_segment(conn)
        rows = [
            {"node_id": r[0], "subtype": r[1], "attrs": r[2]}
            for r in conn.execute(
                f"SELECT DISTINCT ON (node_id) node_id, subtype, attrs FROM {segment} "
                "WHERE deleted = FALSE AND attrs ? 'text' ORDER BY node_id, live_lo DESC"
            ).fetchall()
        ]
        assert write_node_documents(conn, rows) > 0
        links = conn.execute(
            "SELECT node_id, doc_hash FROM okg.node_documents WHERE set_id = %s",
            (vector_set.set_id,),
        ).fetchall()
        unreadable = {
            str(n) for n, _d in links
            if ":hidden:" in n or ":masked:" in n or ":dropped:" in n or n in MIX_HIDDEN
        }
        assert len(unreadable) > limit
        write_vectors(conn, vector_set.set_id, [
            (str(doc), toward(str(node_id), 1.0 if str(node_id) in unreadable else -1.0))
            for node_id, doc in links
        ])
        record_fill_completion(conn, vector_set.set_id, "fixture")
        conn.commit()
    published = Publisher(
        clean_runtime, deployment,
        catalog_identity=bind_test_catalog_writer(clean_runtime, deployment),
    ).cycle()
    assert published["status"] == "published", published

    def served() -> list[str]:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            session = open_session(conn, deployment_name=deployment, nomos_subject=READER)
            page = search(conn, session, query, method="semantic", embedder=plan,
                          limit=limit)
        assert page["vector_source"] == "vector_set" and page["ranking"] == ranking
        ids = [str(row["node_id"]) for row in page["results"]]
        assert not set(ids) & unreadable
        return ids

    _enforce(monkeypatch)
    assert len(served()) == limit
    monkeypatch.setattr(search_module, "_reader_predicate_sql", lambda conn, session: None)
    # Without the SQL predicate the unreadable rows fill the whole window,
    # the read filter drops them all, and the page cannot be served.
    with pytest.raises(SubstrateError, match="bounded rank window could not reach"):
        served()
