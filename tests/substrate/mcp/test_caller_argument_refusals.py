"""A caller's fixable argument mistake is a named refusal (issue #2809).

Before, the search, expand and inspect checks raised a bare `ValueError`.
`ByteBoundedFastMCP.call_tool` serves an untyped exception as the generic
`operator_execution_failed` ("the operator failed before producing a bounded
MCP response") and logs a traceback at ERROR, so the agent never saw the
message that says how to fix the call. Each check now raises
`InvalidOperatorArguments`, served as `invalid_operator_arguments` with that
message and a retry hint, and nothing is logged as a server fault.
"""
from __future__ import annotations

import json
import logging

import anyio
import pytest

from okg.substrate.mcp.response_bounds import InvalidOperatorArguments


def test_the_typed_refusal_is_still_a_value_error_with_a_retry_hint() -> None:
    # Internal callers that catch ValueError (filter, map) keep working.
    error = InvalidOperatorArguments("search: query must be a non-empty string")
    assert isinstance(error, ValueError)
    assert error.code == "invalid_operator_arguments"
    assert error.fix_hint == "correct the argument the message names, then retry"
    assert InvalidOperatorArguments("x", fix_hint="do y").fix_hint == "do y"


@pytest.fixture
def served(clean_runtime, okg_mcp_can_log_in, tmp_path):
    from tests.substrate.mcp.test_search_list import Served
    from tests.substrate.mcp.test_search_records import _basic_facts, _publish

    deployment = _publish(tmp_path, clean_runtime, groups={}, main=_basic_facts())
    server = Served(clean_runtime, deployment)
    try:
        yield server
    finally:
        server.close()


def _call(served, tool: str, arguments: dict) -> tuple[bool, dict, str]:
    """One call through an in-memory MCP client: (isError, JSON body, text card)."""
    from mcp.shared.memory import create_connected_server_and_client_session

    out: list = []

    async def drive() -> None:
        async with create_connected_server_and_client_session(served.mcp) as client:
            for result_format in ("json", "text"):
                out.append(await client.call_tool(
                    tool, arguments, meta={"okg/result_format": result_format},
                ))

    anyio.run(drive)
    as_json, as_text = out
    return as_json.isError, json.loads(as_json.content[0].text), as_text.content[0].text


@pytest.mark.postgres
@pytest.mark.parametrize(("tool", "arguments", "phrase"), [
    pytest.param("inspect", {"target": {"kind": "view", "name": "no such view!"}},
                 "bare okg-schema view name", id="inspect-malformed-view"),
    pytest.param("inspect", {"target": {"kind": "view", "name": "no_such_view"}},
                 "not found;", id="inspect-unknown-view"),
    pytest.param("search", {"query": ""}, "query must be non-empty", id="search-empty-query"),
    pytest.param("search", {"query": "zirconium", "method": "attrs"},
                 "query-less attrs lookup", id="search-attrs-with-query"),
    pytest.param("search", {"query": "zirconium", "method": "grep", "path_prefix": ""},
                 "path_prefix must be a non-empty string", id="grep-empty-path-prefix"),
    pytest.param("expand", {"input": {"node_id": "ticket:long"}, "order_by": "title asc extra"},
                 "order_by must be", id="expand-malformed-order-by"),
    pytest.param("aggregate", {"input": {"kind": "edges", "subtype": "ticket"},
                               "by": ["edge_type"], "metrics": ["count"]},
                 "subtype applies to kind='nodes'", id="aggregate-subtype-on-edges"),
])
def test_a_fixable_mistake_is_a_named_refusal_with_its_message(
    served, caplog, tool, arguments, phrase,
):
    with caplog.at_level(logging.ERROR):
        is_error, body, text = _call(served, tool, arguments)

    assert is_error is True
    error = body["error"]
    assert error["code"] == "invalid_operator_arguments", error
    assert phrase in error["message"]
    assert error["fix_hint"] == "correct the argument the message names, then retry"
    data = error.get("data") or {}
    assert "error_id" not in data
    # The registry's data keys for this code (docs/reference/error_codes.yaml).
    assert data["tool"] == tool
    assert {"invalid_fields", "validation_kinds"} <= data.keys()
    assert f"{tool} refused invalid_operator_arguments" in text
    assert phrase in text
    # A caller mistake is not a server fault: no traceback at ERROR.
    assert not [r for r in caplog.records if "untyped exception" in r.getMessage()]


def test_an_unknown_view_message_fits_its_cap_and_says_what_it_left_out() -> None:
    from okg.substrate.mcp.tools import _UNKNOWN_VIEW_MESSAGE_CHARS, _unknown_view_message

    views = [f"v_view_number_{i:02d}" for i in range(80)] + ["v_nodes"]
    message = _unknown_view_message("v_node", sorted(views))
    assert len(message) <= _UNKNOWN_VIEW_MESSAGE_CHARS
    assert "81 views; closest: v_nodes" in message
    shown = message.split("others: ", 1)[1].split(" (+", 1)[0].split(", ")
    assert message.endswith(f"(+{80 - len(shown)} more not shown)")
    few = _unknown_view_message("zzz", ["v_edges", "v_nodes"])
    assert few == "inspect view: okg.zzz not found; 2 views; others: v_edges, v_nodes"
