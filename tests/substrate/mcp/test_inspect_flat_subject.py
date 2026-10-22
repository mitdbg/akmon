"""`inspect` called with `kind` / `node_id` at the top level (issue #3961).

No database. A model sent `inspect {"kind": "text", "node_id": ...}` instead of
`inspect(target={"kind": "text", "node_id": ...})`. The argument stays refused
(it is never served silently), but the refusal now carries the same call with
the subject fields inside `target`, checked servable before it is shown.
"""

from __future__ import annotations

import copy
from typing import Any

import anyio
import pytest

from okg.substrate.mcp.read_grammar import GrammarRefusal, rewrite_call
from okg.substrate.mcp.server import build_server
from tests.substrate.mcp._result_payload import call_json, payload

NODE = "fde19ba0a7f8776a"

# A port nothing listens on: the argument checks run before any connection.
DEAD_DSN = "postgresql://postgres" + ":okg" + "@localhost:1/okg_probe"


@pytest.fixture
def mcp_server(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("OKG_MCP_SURFACE_PROBE", "1")
    monkeypatch.setenv("OKG_MCP_RO_DSN", DEAD_DSN)
    server = build_server(dsn=DEAD_DSN)
    try:
        yield server
    finally:
        server._okg_pool.close()
        if server._okg_write_pool:
            server._okg_write_pool.close()


def _refusal(arguments: dict[str, Any]) -> GrammarRefusal:
    before = copy.deepcopy(arguments)
    with pytest.raises(GrammarRefusal) as refused:
        rewrite_call("inspect", arguments)
    assert arguments == before, "the caller's arguments are never changed in place"
    return refused.value


def _refused_reply(server, arguments: dict[str, Any]) -> dict[str, Any]:
    async def run():
        return await call_json(server, "inspect", arguments)

    result = anyio.run(run)
    assert result.isError is True
    return payload(result)


FLAT_SHAPES = [
    pytest.param(
        {"node_id": NODE},
        {"target": {"kind": "node", "node_id": NODE}},
        "guess",
        id="node_id_only_is_a_node_read",
    ),
    pytest.param(
        {"kind": "text", "node_id": NODE},
        {"target": {"kind": "text", "node_id": NODE}},
        "corrected",
        id="text_by_node_id",
    ),
    pytest.param(
        {"kind": "node", "node_id": NODE},
        {"target": {"kind": "node", "node_id": NODE}},
        "corrected",
        id="node_by_node_id",
    ),
    pytest.param(
        {"kind": "text", "path": "tools/example.sh", "source": "src"},
        {"target": {"kind": "text", "path": "tools/example.sh", "source": "src"}},
        "corrected",
        id="text_by_path_and_source",
    ),
    pytest.param(
        {"kind": "text", "node_id": NODE, "lines": [[1, 5]]},
        {"target": {"kind": "text", "node_id": NODE, "lines": [[1, 5]]}},
        "corrected",
        id="text_lines_move_with_the_subject",
    ),
    pytest.param(
        {"kind": "text", "node_id": NODE, "at": {"chunk": "c1", "offset": 40}},
        {"target": {"kind": "text", "node_id": NODE, "at": {"chunk": "c1", "offset": 40}}},
        "corrected",
        id="text_at",
    ),
]


@pytest.mark.parametrize("sent,offered,mode", FLAT_SHAPES)
def test_a_flat_subject_is_refused_with_the_call_that_has_it_inside_target(
        sent, offered, mode) -> None:
    refusal = _refusal(sent)
    assert refusal.retry_arguments == offered
    assert refusal.retry_mode == mode
    assert "must be inside `target`" in refusal.message
    assert set(refusal.fields) == set(sent)


@pytest.mark.parametrize("sent,offered,mode", FLAT_SHAPES)
def test_the_offered_call_is_served_as_written(mcp_server, sent, offered, mode) -> None:
    reply = _refused_reply(mcp_server, sent)
    assert reply["error"]["code"] == "invalid_operator_arguments"
    retry = reply["error"]["retry_call"]
    assert retry["tool"] == "inspect"
    assert retry["mode"] == mode
    assert retry["arguments"] == offered
    # Every check a dispatched call meets takes the offered call, with no note.
    assert mcp_server._okg_takes_as_written("inspect", retry["arguments"])
    assert not rewrite_call("inspect", retry["arguments"]).adjusted


def test_the_refusal_text_shows_the_copy_ready_call(mcp_server) -> None:
    reply = _refused_reply(mcp_server, {"kind": "text", "node_id": NODE})
    assert reply["error"]["fix_hint"] == (
        f'inspect(target={{"kind": "text", "node_id": "{NODE}"}})'
    )
    assert reply["error"]["data"]["invalid_fields"] == ["kind", "node_id"]


def test_select_and_max_bytes_stay_beside_target() -> None:
    refusal = _refusal({
        "kind": "node", "node_id": NODE, "select": ["attrs"], "max_bytes": 20000,
    })
    assert refusal.retry_arguments == {
        "target": {"kind": "node", "node_id": NODE},
        "select": ["attrs"], "max_bytes": 20000,
    }


def test_a_select_the_kind_does_not_take_is_not_offered(mcp_server) -> None:
    # A text read has no `select`: the call with it would be refused, so it is
    # not shown as a call; the refusal still names the move.
    sent = {"kind": "text", "node_id": NODE, "select": ["attrs"]}
    reply = _refused_reply(mcp_server, sent)
    assert "retry_call" not in reply["error"]
    assert "must be inside `target`" in reply["error"]["message"]


def test_flat_kind_that_differs_from_target_is_refused_naming_both() -> None:
    refusal = _refusal({"kind": "text", "target": {"kind": "node", "node_id": NODE}})
    assert "`kind` is given beside `target` and inside it" in refusal.message
    assert refusal.fields == ("kind", "target.kind")
    assert refusal.retry_arguments is None


def test_flat_node_id_that_differs_from_target_is_refused_naming_both() -> None:
    refusal = _refusal({
        "kind": "node", "node_id": "aaaa",
        "target": {"kind": "node", "node_id": NODE},
    })
    assert "`node_id` is given beside `target` and inside it" in refusal.message
    assert refusal.fields == ("node_id", "target.node_id")


def test_the_two_readings_refusal_reaches_the_caller(mcp_server) -> None:
    reply = _refused_reply(
        mcp_server, {"node_id": "aaaa", "target": {"kind": "node", "node_id": NODE}},
    )
    assert reply["error"]["code"] == "invalid_operator_arguments"
    assert "beside `target` and inside it" in reply["error"]["message"]


@pytest.mark.parametrize("sent", [
    {"target": {"kind": "node", "node_id": NODE}},
    {"target": {"kind": "graph"}, "select": ["subtypes"]},
    {"kind": "no_such_kind", "node_id": NODE},
    {"kind": 7, "node_id": NODE},
    {"path": "tools/example.sh"},
    {"node_id": NODE, "next": "token"},
], ids=["plain", "graph", "unknown_kind", "kind_not_text", "path_alone", "paged_read"])
def test_other_calls_are_not_touched(sent) -> None:
    before = copy.deepcopy(sent)
    assert rewrite_call("inspect", sent).arguments == before


def test_a_flat_field_that_agrees_with_target_is_left_to_the_unknown_argument_refusal(
        mcp_server) -> None:
    sent = {"kind": "node", "target": {"kind": "node", "node_id": NODE}}
    assert rewrite_call("inspect", sent).arguments == sent
    reply = _refused_reply(mcp_server, sent)
    assert reply["error"]["code"] == "unknown_operator_argument"


def test_an_unknown_flat_kind_keeps_the_unknown_argument_refusal(mcp_server) -> None:
    reply = _refused_reply(mcp_server, {"kind": "no_such_kind", "node_id": NODE})
    assert reply["error"]["code"] == "unknown_operator_argument"
