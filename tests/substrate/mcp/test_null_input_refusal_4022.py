"""A string `input` that decodes to JSON null is a plain refusal (issue #4022).

`aggregate(input="null")` used to fail as `operator_execution_failed`: the
argument checks saw the string "null" (a node id), then FastMCP decoded it to
None inside `Tool.run`, and `aggregate`'s required `input` refused None with an
untyped validation error. The string is now decoded once, first, and refused
by name as `invalid_operator_arguments` before the tool runs.
"""
from __future__ import annotations

import json
import logging

import anyio
import pytest

from okg.substrate.mcp import live_resolver

from tests.substrate.mcp.test_caller_argument_refusals import served  # noqa: F401


# -- the decoder (no database) ---------------------------------------------


@pytest.mark.parametrize("text", ["null", " null", "null\n", "\tnull "])
def test_an_aggregate_input_string_that_decodes_to_null_is_refused_by_name(text) -> None:
    with pytest.raises(live_resolver.JsonStringRefused, match="decodes to null") as caught:
        live_resolver.decode_json_arguments("aggregate", {"input": text})
    assert caught.value.tool == "aggregate"
    assert "`input`" in str(caught.value)
    assert "an object or a node id" in str(caught.value)
    assert "leave `input` out to count every record" in str(caught.value)
    assert "leave `input` out" in caught.value.fix_hint


@pytest.mark.parametrize("tool,text", [
    # Strings that are not JSON null stay node ids, as they were.
    ("aggregate", "nullable"),
    ("aggregate", "NULL"),
    ("aggregate", "node:abc"),
    ("aggregate", "nul"),
    ("aggregate", "\x0bnull"),
    # JSON scalars FastMCP never decodes (it keeps str, int, float and bool
    # inputs as the string sent) stay node ids.
    ("aggregate", "1"),
    ("aggregate", "true"),
    ("aggregate", '"abc"'),
    # expand's `input` is optional: "null" there is the absent input, which
    # expand's own check answers (and which `next` continues).
    ("expand", "null"),
    # filter and map never take a string `input`.
    ("filter", "null"),
    ("map", "null"),
])
def test_other_strings_are_left_exactly_as_sent(tool, text) -> None:
    sent = {"input": text}
    assert live_resolver.decode_json_arguments(tool, sent) is sent


# -- through a real in-memory MCP client -----------------------------------


def _call(served, tool: str, arguments: dict):  # noqa: F811
    """One call through an in-memory MCP client: (isError, JSON body)."""
    from mcp.shared.memory import create_connected_server_and_client_session

    out: list = []

    async def drive() -> None:
        async with create_connected_server_and_client_session(served.mcp) as client:
            out.append(await client.call_tool(
                tool, arguments, meta={"okg/result_format": "json"},
            ))

    anyio.run(drive)
    return out[0].isError, json.loads(out[0].content[0].text)


_AGG = {"by": ["subtype"], "metrics": ["count"]}


@pytest.mark.postgres
@pytest.mark.parametrize("text", ["null", "  null\n"])
def test_aggregate_null_input_is_an_argument_refusal_not_a_server_fault(
    served, caplog, text,  # noqa: F811
) -> None:
    with caplog.at_level(logging.ERROR):
        is_error, body = _call(served, "aggregate", {"input": text, **_AGG})
    assert is_error
    error = body["error"]
    assert error["code"] == "invalid_operator_arguments", body
    assert error["data"]["invalid_fields"] == ["input"], body
    assert error["data"]["validation_kinds"] == ["json_string"], body
    assert "`input`" in error["message"] and "an object or a node id" in error["message"], body
    assert "leave `input` out to count every record" in error["message"], body
    assert "operator_execution_failed" not in json.dumps(body)
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR], caplog.text


@pytest.mark.postgres
@pytest.mark.parametrize("tool,rest", [
    ("expand", {"edge_type": "contains"}),
    ("filter", {"where": {"type": "page"}}),
    ("map", {"where": {"type": "page"}, "select": ["node_id"]}),
])
def test_null_input_on_the_other_tools_is_still_a_plain_argument_refusal(
    served, tool, rest,  # noqa: F811
) -> None:
    is_error, body = _call(served, tool, {"input": "null", **rest})
    assert is_error
    assert body["error"]["code"] == "invalid_operator_arguments", body


@pytest.mark.postgres
def test_valid_aggregate_inputs_answer_as_before(served) -> None:  # noqa: F811
    omitted = _call(served, "aggregate", dict(_AGG))
    as_object = _call(served, "aggregate", {"input": {"subtype": "ticket"}, **_AGG})
    as_string = _call(served, "aggregate", {"input": '{"subtype": "ticket"}', **_AGG})
    assert not omitted[0] and not as_object[0] and not as_string[0]
    assert as_string[1]["groups"] == as_object[1]["groups"]
    assert as_object[1]["groups"], "the stored ticket type has rows"


@pytest.mark.postgres
def test_a_null_input_in_a_run_step_is_refused_plainly(served, caplog) -> None:  # noqa: F811
    """A `run` step reaches `aggregate` through the same call path; the step
    is refused by name, not served as `operator_execution_failed`."""

    step = {"aggregate": {"input": "null", **_AGG}}
    with caplog.at_level(logging.ERROR):
        _, body = _call(served, "run", {"steps": [step]})
    text = json.dumps(body)
    assert "invalid_operator_arguments" in text, body
    assert "json_string" in text, body
    assert "operator_execution_failed" not in text, body
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR], caplog.text
