"""A refusal's corrected call, read as an MCP client receives it (issue #3666).

Every other test of `error.retry_call` asserts it on the JSON reply. An
agent reads the TEXT reply, at the default byte budget, and a client may
read `structuredContent`. Here each refusal that is meant to carry a call
is sent through a real served tool in both result formats, with no
`max_bytes`, and the call is taken only from what came back over the
client session: the structured content if there is any, else the text
block. That call is then sent, and must return rows.

Part (i) uses only what the condition-grammar step (2a) has, so this file
can be run on a checkout without the `select` vocabulary; part (ii) skips
there.
"""
from __future__ import annotations

import importlib.util
import json
from typing import Any, Optional

import anyio
import pytest

from tests.substrate.mcp.test_search_list import TERM, Served, _corpus

pytestmark = pytest.mark.postgres

HAS_SELECT_VOCABULARY = (
    importlib.util.find_spec("okg.substrate.mcp.select_vocabulary") is not None
)


@pytest.fixture
def server(clean_runtime, okg_mcp_can_log_in, tmp_path):
    served = Served(clean_runtime, _corpus(tmp_path, clean_runtime))
    try:
        yield served
    finally:
        served.close()


def _call(served: Served, tool: str, arguments: dict, result_format: str) -> Any:
    """One call through an in-memory MCP client session; the raw result."""
    from mcp.shared.memory import create_connected_server_and_client_session

    out: list[Any] = []

    async def drive() -> None:
        async with create_connected_server_and_client_session(served.mcp) as client:
            out.append(await client.call_tool(
                tool, arguments, meta={"okg/result_format": result_format},
            ))

    anyio.run(drive)
    return out[0]


def _text(result: Any) -> str:
    return "\n".join(
        block.text for block in result.content if getattr(block, "text", None) is not None
    )


def _find(value: Any, key: str) -> Any:
    if isinstance(value, dict):
        if key in value:
            return value[key]
        for item in value.values():
            found = _find(item, key)
            if found is not None:
                return found
    elif isinstance(value, list):
        for item in value:
            found = _find(item, key)
            if found is not None:
                return found
    return None


def received_retry_call(result: Any) -> Optional[dict]:
    """The corrected call in a reply, as a client can read it.

    From `structuredContent` when the result has one; from the JSON text
    block (`error.retry_call`); from the text reply, where it is one
    `retry_call: {...}` line, or a `--- retry_call` line followed by the
    call's JSON. None when the reply carries no call.
    """
    structured = getattr(result, "structuredContent", None)
    if structured:
        found = _find(structured, "retry_call")
        if isinstance(found, dict):
            return found
    text = _text(result)
    try:
        reply = json.loads(text)
    except ValueError:
        reply = None
    if isinstance(reply, dict):
        error = reply.get("error")
        found = error.get("retry_call") if isinstance(error, dict) else None
        return found if isinstance(found, dict) else None
    lines = text.splitlines()
    for index, line in enumerate(lines):
        body: Optional[str] = None
        if line.startswith("retry_call: "):
            body = line[len("retry_call: "):]
        elif line.strip() == "--- retry_call" and index + 1 < len(lines):
            body = lines[index + 1]
        if body is not None:
            try:
                found = json.loads(body)
            except ValueError:
                return None
            return found if isinstance(found, dict) else None
    return None


def _refused_then_retried(served: Served, tool: str, sent: dict, rows: str, fmt: str) -> dict:
    """Send `sent`, read the call the refusal carries from what came back,
    send that call, and return its JSON reply."""
    result = _call(served, tool, sent, fmt)
    text = _text(result)
    assert result.isError, text[:600]
    retry = received_retry_call(result)
    assert retry is not None, (
        f"no retry_call in the {fmt} reply the client received:\n{text[:1500]}"
    )
    assert retry.get("mode") in {"corrected", "guess"}, retry
    assert isinstance(retry.get("arguments"), dict), retry
    if fmt == "text":
        # What a model reads first: the refusal's `retry:` ending names the
        # call itself, not a pointer to another field.
        head = text.splitlines()[0]
        assert "retry: " in head and f"{retry['tool']}(" in head, head
    repeated = {name: sent[name] for name in retry.get("repeat", [])}
    again = _call(served, retry["tool"], {**retry["arguments"], **repeated}, "json")
    reply = json.loads(_text(again))
    assert not again.isError and reply.get("status") != "refused", (retry, reply)
    assert reply.get(rows), (retry, reply)
    return reply


#: Refusals the condition-grammar step (2a) answers with a corrected call:
#: the tool, the call, and the reply key that holds the offered call's rows.
CONDITION_RETRIES = [
    ("filter", {"where": {"Type": "ticket"}}, "items"),
    ("filter", {"where": {"type": "doc", "attrs.path": {"suffix": "/001.md"}}}, "items"),
    ("filter", {"where": {"or": [{"id": "doc:001"}, {"id": "ticket:1"}]}}, "items"),
    ("map", {"where": {"Type": "ticket"}}, "items"),
    ("search", {"query": TERM, "where": {"Type": "ticket"}}, "results"),
    ("expand", {"input": {"node_id": "doc:000"}, "edge_type": "contains",
                "where": {"Type": "doc_chunk", "attrs.text": {"prefix": "zirconium passage 1"}}},
     "neighbors"),
]


@pytest.mark.parametrize("fmt", ["json", "text"])
@pytest.mark.parametrize("case", range(len(CONDITION_RETRIES)))
def test_a_condition_refusals_corrected_call_reaches_the_client_and_is_served(
        server, case, fmt):
    tool, sent, rows = CONDITION_RETRIES[case]
    _refused_then_retried(server, tool, sent, rows, fmt)


#: Refusals the select-vocabulary step (3a) answers with a call.
SELECT_RETRIES = [
    # Records asked of identifier_mentions: grep for the same query.
    ("search", {"query": TERM, "method": "identifier_mentions", "limit": 5,
                "select": ["node_id", "subtype", "handle", "excerpt", "value", "id_type"]},
     "results"),
    ("search", {"query": TERM, "method": "identifier_mentions",
                "select": ["node_id", "value", "id_type", "evidence_kind"]}, "results"),
    # Another letter case of a field, and a second name.
    ("search", {"query": TERM, "limit": 3, "select": ["SCORE", "node_id"]}, "results"),
    ("search", {"query": TERM, "limit": 3, "select": ["id", "excerpt"]}, "results"),
    # A second name on a 1-hop row: its rows are the linked records (#3666),
    # so `id` and `type` are the record's node_id and subtype.
    ("expand", {"input": {"node_id": "doc:000"}, "edge_type": "contains",
                "select": ["id", "type"]}, "neighbors"),
]


@pytest.mark.skipif(not HAS_SELECT_VOCABULARY, reason="needs the select vocabulary (step 3a)")
@pytest.mark.parametrize("fmt", ["json", "text"])
@pytest.mark.parametrize("case", range(len(SELECT_RETRIES)))
def test_a_select_refusals_call_reaches_the_client_and_is_served(server, case, fmt):
    tool, sent, rows = SELECT_RETRIES[case]
    _refused_then_retried(server, tool, sent, rows, fmt)


@pytest.mark.skipif(not HAS_SELECT_VOCABULARY, reason="needs the select vocabulary (step 3a)")
@pytest.mark.parametrize("fmt", ["json", "text"])
def test_a_refusal_says_its_sentence_once_and_names_the_callers_query(server, fmt):
    """Four record fields asked of identifier_mentions: one sentence about
    its rows, not four, and the caller's query where a placeholder stood."""
    sent = {"query": TERM, "method": "identifier_mentions", "limit": 5,
            "select": ["node_id", "subtype", "handle", "excerpt", "value", "id_type"]}
    result = _call(server, "search", sent, fmt)
    text = _text(result)
    assert result.isError
    # Once in the whole reply: the message states it, the hint only acts.
    assert text.count("spans of your query text") == 1, text[:2000]
    assert "<value>" not in text, text[:2000]
    assert f'query="{TERM}"' in text or f'"query":"{TERM}"' in text, text[:2000]
    assert '"truncated":true' not in text and "truncated: true" not in text, text[:2000]


#: `aggregate` group-key refusals that carry a call (#3666), and the groups
#: the offered call returns: a link key gets the count of those links, with
#: a condition on record ids moved onto the near end; a second name gets
#: the key it names.
AGGREGATE_RETRIES = [
    ({"where": {"id": "doc:000"}, "by": ["link:contains:out"], "metrics": ["count"]},
     "dst", {f"doc_chunk:000-{j}": 1 for j in range(3)}),
    ({"input": {"kind": "nodes"}, "by": ["link(contains:in)"], "metrics": ["count"]},
     "src", {**{f"doc:{i:03d}": 3 for i in range(5)}, "doc:passages-only": 3}),
    ({"where": {"type": "doc_chunk"}, "by": ["type"], "metrics": ["count"]},
     "subtype", {"doc_chunk": 18}),
]


@pytest.mark.parametrize("fmt", ["json", "text"])
@pytest.mark.parametrize("case", range(len(AGGREGATE_RETRIES)))
def test_a_group_key_refusals_call_reaches_the_client_and_is_served(server, case, fmt):
    sent, key, groups = AGGREGATE_RETRIES[case]
    reply = _refused_then_retried(server, "aggregate", sent, "groups", fmt)
    assert {group[key]: group["count"] for group in reply["groups"]} == groups


@pytest.mark.parametrize("fmt", ["json", "text"])
def test_an_unknown_group_key_is_refused_by_name_with_the_keys_of_the_set(server, fmt):
    result = _call(server, "aggregate", {
        "where": {"type": "doc"}, "by": ["dst.project"], "metrics": ["count"]}, fmt)
    text = _text(result)
    assert result.isError, text[:600]
    assert "`dst.project` is not a group key of a set of records" in text, text[:1500]
    assert "node_id, subtype, text_hash, attrs.<key>, each(attrs.<key>)" in text, text[:1500]
    assert received_retry_call(result) is None
    # A typed record set grouped by a link: the count of links cannot keep
    # the type yet, so the refusal says so and offers no call.
    result = _call(server, "aggregate", {
        "where": {"type": "doc"}, "by": ["link:contains:out"], "metrics": ["count"]}, fmt)
    text = _text(result)
    assert result.isError, text[:600]
    assert received_retry_call(result) is None
    assert "this call's type cannot be carried over" in text, text[:1500]
    # each() on a bare word: the fix is said, and no call is offered, since
    # only the read knows whether the attribute holds a list.
    result = _call(server, "aggregate", {
        "where": {"type": "doc"}, "by": ["each(path)"], "metrics": ["count"]}, fmt)
    text = _text(result)
    assert result.isError, text[:600]
    assert received_retry_call(result) is None
    assert "each(attrs.path)" in text, text[:1500]
