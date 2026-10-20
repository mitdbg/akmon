"""What a read reply's text leaves out by default (PACT read-passage-rows-3666).

* D8 (Jason, 2026-10-15): the generation id, the stale-publish notice and the
  exact-rerun advice are optional and off by default, on every reply,
  including a session's first call. The JSON keeps the keys, and
  inspect(target={"kind": "graph"}) still prints the generation and the
  publish freshness block.
* (f): the recency-annotation notice is an internal of the read.
* (c), gate 1: a search refusal never names `max_bytes`, which search does
  not publish: not in its retry call (the caller's own hidden arguments were
  echoed back) and not as a `max_bytes: N` line.

Hermetic: payloads are rendered or served directly; no database.
"""
from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

from okg.substrate.mcp.lean_surface import HIDDEN_ARGUMENTS
from okg.substrate.mcp.response_bounds import ByteBoundedFastMCP
from okg.substrate.mcp.result_text import render

GEN = "okg:demo:branch:default:gen:20260101T000000000000Z:0123456789ab"
STALE = {"stale": True, "publish_lag_seconds": 47150.8}


def _search_page(**extra: Any) -> dict[str, Any]:
    return {
        "query": "deploy", "method": "lexical", "offset": 0, "total_count": 1,
        "results": [{"node_id": "doc:1", "subtype": "doc", "excerpt": "deploy it"}],
        "pinned_generation_id": GEN, "publish_freshness": STALE,
        "coverage": {"retrieval": {"status": "bounded", "basis": "bm25_index",
                                   "recovery": {"completeness": "exact"}},
                     "delivery": {"complete": True}},
        **extra,
    }


def test_a_first_search_reply_names_no_generation_stale_publish_or_rerun() -> None:
    page = _search_page()
    for state in ({}, None):
        text = render(page, tool="search", session_state=state)
        assert GEN not in text and "generation" not in text, text
        assert "publish is stale" not in text and "publish" not in text, text
        assert "rerun from offset 0" not in text, text
    # The JSON keeps the keys.
    assert page["pinned_generation_id"] == GEN and page["publish_freshness"] == STALE


def test_other_read_replies_name_no_generation() -> None:
    node = {"node_id": "doc:1", "subtype": "doc", "attrs": {"title": "T"},
            "pinned_generation_id": GEN, "publish_freshness": STALE}
    refusal = {"status": "refused", "error": {"code": "x", "message": "no"},
               "session_default_generation_id": GEN}
    batch = {"items": [], "pinned_generation_id": GEN}
    for text in (
        render(node, tool="inspect", kind="node", session_state={}),
        render(refusal, tool="search", session_state={}),
        render(batch, tool="inspect", kind="texts", session_state={}),
        render({"results": [], "pinned_generation_id": GEN}, tool="expand"),
    ):
        assert GEN not in text and "publish is stale" not in text, text


def test_inspect_graph_still_names_the_generation_and_publish_freshness() -> None:
    """Failure case: the one place a reader asks for them keeps both."""
    graph = {"status": "ok", "operator_name": "inspect", "deployment_name": "d",
             "generation_id": GEN, "subtype_counts": {"doc": 1}, "node_count": 1,
             "publish_freshness": STALE}
    state: dict[str, Any] = {}
    for _ in range(2):
        text = render(graph, tool="inspect", kind="graph", session_state=state)
        assert f"generation {GEN}" in text.splitlines()[0], text
        assert "47150.8" in text, text


def test_the_recency_notice_is_not_printed() -> None:
    page = _search_page(recency={"annotated": True, "attribute": "updated",
                                 "derivation": "attr", "basis": "row"})
    text = render(page, tool="search")
    assert "recency" not in text, text
    assert page["recency"]["annotated"] is True


# ── (c): a search refusal never names max_bytes ─────────────────────────

_SENT = {"query": "overwrite FTS", "limit": 10, "max_bytes": 12000,
         "select": ["node_id", "subtype", "handle", "attrs"]}


def _select_refusal(arguments: dict[str, Any], mode: str = "corrected") -> dict[str, Any]:
    # The q007 refusal as the replay served it (select of `type`, `title`).
    return {
        "status": "refused", "operator_name": "search",
        "error": {
            "code": "select_unknown_field",
            "message": "search(method=lexical): unknown field(s) in select: ['type']",
            "fix_hint": "type: use 'subtype'",
            "max_bytes": 12000,
            "retry_call": {"tool": "search", "mode": mode, "arguments": dict(arguments)},
        },
    }


def _served(server: Any, name: str, payload: dict[str, Any]) -> str:
    result = ByteBoundedFastMCP._okg_serve_payload(server, name, payload, is_error=True)
    return result.content[0].text


def test_a_search_refusal_retry_names_no_hidden_argument() -> None:
    """Must-fail on f9634f77389: `retry: search(..., max_bytes=12000)`."""
    lean = SimpleNamespace(_okg_lean_hidden_lines=("x",))
    payload = _select_refusal(_SENT)
    text = _served(lean, "search", payload)
    assert "max_bytes" not in text, text
    retry = payload["error"]["retry_call"]["arguments"]
    assert retry == {k: v for k, v in _SENT.items() if k != "max_bytes"}
    assert "max_bytes" in HIDDEN_ARGUMENTS["search"]
    assert 'retry: search(query="overwrite FTS", limit=10, select=' in text.splitlines()[0]
    # The JSON keeps the reply budget.
    assert payload["error"]["max_bytes"] == 12000


def test_a_retry_keeps_what_the_surface_publishes() -> None:
    """Failure cases: nothing is taken out where it is published or where
    the retry continues a token as sent."""
    plain = SimpleNamespace()  # no lean surface: max_bytes is published
    payload = _select_refusal(_SENT)
    _served(plain, "search", payload)
    assert payload["error"]["retry_call"]["arguments"]["max_bytes"] == 12000
    lean = SimpleNamespace(_okg_lean_hidden_lines=("x",))
    # A hidden argument that carries the call's scope or conditions is never
    # dropped: a filter retry kept as the caller sent it (`predicate`,
    # `input` are hidden spellings on filter).
    sent = {"input": {"kind": "nodes"}, "predicate": {"op": "eq", "field": "subtype",
                                                      "value": "doc"}}
    other = {"status": "refused", "operator_name": "filter",
             "error": {"code": "x", "message": "no", "retry_call": {
                 "tool": "filter", "mode": "corrected", "arguments": dict(sent)}}}
    _served(lean, "filter", other)
    assert other["error"]["retry_call"]["arguments"] == sent
    # On search too, only the size control goes.
    scoped = _select_refusal({"query": "q", "generation_id": "g", "max_bytes": 9000})
    _served(lean, "search", scoped)
    assert scoped["error"]["retry_call"]["arguments"] == {"query": "q", "generation_id": "g"}
    continued = _select_refusal({"continuation_token": "t", "max_bytes": 9000}, "continue")
    _served(lean, "search", continued)
    assert json.dumps(continued["error"]["retry_call"]["arguments"]) == (
        '{"continuation_token": "t", "max_bytes": 9000}'
    )
