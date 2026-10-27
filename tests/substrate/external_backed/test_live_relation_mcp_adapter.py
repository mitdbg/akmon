"""The built-in `mcp` live-relation adapter (PACT live-relation-mcp-adapter).

Two halves, and the split is deliberate:

* The first half is HERMETIC: it parses bindings and runs the bound runner
  directly against `fake_mcp_upstream.FakeMcpUpstream`, a real MCP SDK
  server on a loopback port. It proves the byte cap, the call and its error
  mapping, and every load refusal, with no database.
* The second half goes through `build_server` on a real deployment manifest
  and drives `expand(live=...)` over a real client session, against a real
  published generation. It proves the adapter is REACHED from production --
  registered, gated, bounded and sampled -- which no direct call can.

The byte cap is proved first, because the rest is worth nothing without
it: a 60 MB reply must be refused without being assembled.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import time
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import anyio
import psycopg
import pytest

from okg.substrate.external_backed.live_relations import (
    LiveBinding,
    LiveRelationArg,
)
from tests.substrate.external_backed.fake_mcp_upstream import (
    FakeMcpUpstream,
    echo_arguments,
    json_text,
    sized_rows,
    structured,
    text_blocks,
)

TOKEN = "fake-mcp-live-relation-token-never-logged"
MIB = 1024 * 1024
#: One network read. The cap may be overshot by at most this much before
#: the counter trips, and never by more.
CHUNK_SLACK = 2 * 65536

#: The relation's arguments, both constrained, as the adapter requires.
ARGS = (
    LiveRelationArg(
        name="name", type_name="string", required=True,
        pattern="^[A-Za-z0-9*_/-]{1,80}$", max_length=80,
    ),
    LiveRelationArg(
        name="limit", type_name="integer", default=5, minimum=1, maximum=50,
    ),
)


def _declared(args: tuple = ARGS) -> Any:
    return SimpleNamespace(args=args)


def mcp_config(url: str, **overrides: Any) -> dict[str, Any]:
    config: dict[str, Any] = {
        "server_url": url,
        "allow_non_global": True,
        "tool": "datasets",
        "arguments": {
            "pattern": {"$arg": "name"},
            "limit": {"$arg": "limit"},
            "detail": True,
        },
        "rows_path": "rows",
    }
    config.update(overrides)
    return {k: v for k, v in config.items() if v is not None}


def _binding(config: dict[str, Any], *, secret: bool = False,
             credential: str | None = "service") -> LiveBinding:
    if secret:
        config = {**config, "auth_header": "Authorization"}
    return LiveBinding(
        relation="mcp_hits", kind="mcp", config=config,
        secret_refs=("env:OKG_MCP_TEST_TOKEN",) if secret else (),
        upstream_credential=credential,
    )


def _runner(config: dict[str, Any], *, secret: bool = False,
            args: tuple = ARGS, monkeypatch: Any = None):
    from okg.substrate.external_backed.mcp_adapter import (
        validate_mcp_binding_configs,
    )

    if secret:
        monkeypatch.setenv("OKG_MCP_TEST_TOKEN", f"Bearer {TOKEN}")
    runners = validate_mcp_binding_configs(
        {"mcp_hits": _binding(config, secret=secret)},
        {"mcp_hits": _declared(args)}, runners={},
    )
    return runners["mcp_hits"]


def _call(runner, *, args: dict | None = None, deadline: float = 15.0):
    return runner(
        config={}, args=args if args is not None else {"name": "x", "limit": 5},
        node_id="n:hub", deadline_seconds=deadline,
    )


#: Loggers that belong to the FAKE upstream, which runs in this process only
#: because the test put it here. A real MCP server is another process, and
#: what it logs about its own replies is not okg's log.
_UPSTREAM_LOGGERS = ("sse_starlette", "mcp.server", "uvicorn")


def _okg_side_records(records: list[logging.LogRecord]) -> list[logging.LogRecord]:
    return [r for r in records if not r.name.startswith(_UPSTREAM_LOGGERS)]


def _failure(runner, **kwargs) -> Any:
    from okg.substrate.external_backed.live_dispatch import LiveAdapterError

    with pytest.raises(LiveAdapterError) as caught:
        _call(runner, **kwargs)
    return caught.value


# ═══════════════════════════════ the byte cap ═══════════════════════════

@pytest.mark.parametrize("json_response", [False, True], ids=["sse", "json"])
def test_a_60_mb_reply_is_refused_at_the_default_cap_without_being_read(
    json_response: bool,
):
    """The risk the PACT named first, proved first.

    The counter sits in the transport, BELOW the MCP SDK, so the SDK never
    holds more than the cap plus one network read. The refusal reports
    where reading stopped; that number is the proof the 60 MB was never
    assembled, in either of the two reply framings streamable HTTP allows
    (an SSE event stream, or one JSON body).
    """
    rows = sized_rows(60 * MIB)
    with FakeMcpUpstream(json_response=json_response) as fake:
        fake.tool("datasets", structured(rows))
        runner = _runner(mcp_config(fake.url))
        started = time.monotonic()
        error = _failure(runner)
        elapsed = time.monotonic() - started
        closes = fake.wait_for_closes(1)

    assert error.failure == "response_over_cap", error.excerpt
    stopped = int(re.search(r"stopped at (\d+) bytes", error.excerpt).group(1))
    default_cap = 8 * MIB
    assert default_cap < stopped <= default_cap + CHUNK_SLACK, stopped
    assert "not parsed" in error.excerpt
    assert fake.counters.calls == ["datasets"], "exactly one tool call"
    assert closes == 1, "the session is closed after the refusal"
    assert elapsed < 10, elapsed


def test_a_20_mb_reply_under_a_32_mib_cap_is_read_whole():
    """The partner row: a raised cap is honoured, not a hidden lower one."""
    rows = sized_rows(20 * MIB)
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", structured(rows))
        runner = _runner(mcp_config(fake.url, max_response_bytes=32 * MIB))
        got = _call(runner)
        closes = fake.wait_for_closes(1)

    assert len(got) == len(rows)
    assert got[0] == rows[0] and got[-1] == rows[-1]
    assert closes == 1


def test_the_cap_counts_the_whole_session_not_only_the_result():
    """The counter is per SESSION, as documented: a cap the tool result
    alone would fit under still trips once `initialize` is counted too."""
    rows = [{"n": 1, "blob": "y" * 800}]
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", structured(rows))
        result_only = len(json.dumps({"rows": rows}))
        assert result_only < 1024 < result_only + 400
        runner = _runner(mcp_config(fake.url, max_response_bytes=1024))
        error = _failure(runner)

    assert error.failure == "response_over_cap"


# ═══════════════════════════════ the call ═══════════════════════════════

def test_rows_come_from_structured_content_at_the_rows_path():
    rows = [{"dataset": "/A/B/C", "size": 1}, {"dataset": "/D/E/F", "size": 2}]
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", structured(rows, path=("result", "datasets")))
        runner = _runner(mcp_config(fake.url, rows_path="result.datasets"))
        got = _call(runner)
        closes = fake.wait_for_closes(1)

    assert got == rows
    assert fake.counters.sessions == 1
    assert fake.counters.calls == ["datasets"], "one call, no retry, no list"
    assert closes == 1, "the session is closed after success"


def test_structured_content_wins_over_a_text_duplicate():
    """FastMCP servers send the structured result AND a text copy of it;
    two content blocks are only a problem when there is no structure."""
    from mcp import types

    def both(_args):
        return types.CallToolResult(
            content=[types.TextContent(type="text", text="copy one"),
                     types.TextContent(type="text", text="copy two")],
            structuredContent={"rows": [{"a": 1}]},
        )

    with FakeMcpUpstream() as fake:
        fake.tool("datasets", both)
        got = _call(_runner(mcp_config(fake.url)))

    assert got == [{"a": 1}]


def _structured_document(document: Any):
    """`structuredContent` set to `document` exactly, with no text copy."""
    from mcp import types

    def behaviour(_args):
        return types.CallToolResult(content=[], structuredContent=document)

    return behaviour


#: A get-by-name tool (`example_get_item`) answers with one object, not a list.
EXAMPLE_ITEM = {
    "status": "ok",
    "data": {"scope": "demo", "name": "/P/Q/RAW", "type": "DATASET", "bytes": 7},
}


def test_a_structured_object_at_a_rows_path_is_one_row():
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", _structured_document(EXAMPLE_ITEM))
        got = _call(_runner(mcp_config(fake.url, rows_path="data")))

    assert got == [EXAMPLE_ITEM["data"]]


@pytest.mark.parametrize(
    "document",
    [
        pytest.param({"result": [{"unit": "T1"}, {"unit": "T2"}]},
                     id="fastmcp-typed-list-wrapper"),
        pytest.param(EXAMPLE_ITEM["data"], id="one-object"),
    ],
)
def test_a_structured_object_at_the_root_is_refused_naming_rows_path(document):
    """MCP structuredContent is always an object, and FastMCP wraps a typed
    list as `{"result": [...]}`: read at `.`, the whole list would become
    one row that `max_rows` cannot cut. The rc1 refusal stays, and says
    what to set."""
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", _structured_document(document))
        error = _failure(_runner(mcp_config(fake.url, rows_path=".")))

    assert error.failure == "result_unusable", error.excerpt
    assert "set `rows_path`" in error.excerpt
    assert f"`{next(iter(document))}`" in error.excerpt, "the keys are named"


@pytest.mark.parametrize(
    "behaviour",
    [
        pytest.param(_structured_document(
            {"data": {"items": [{"unit": "T1"}, {"unit": "T2"}]}}),
            id="structured"),
        pytest.param(json_text(
            {"data": {"items": [{"unit": "T1"}, {"unit": "T2"}]}}),
            id="one-text-block"),
    ],
)
def test_a_wrapper_object_at_a_named_path_is_refused_naming_the_key(behaviour):
    """One key holding a list: the binding stopped one level short."""
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", behaviour)
        error = _failure(_runner(mcp_config(fake.url, rows_path="data")))

    assert error.failure == "result_unusable", error.excerpt
    assert "set `rows_path: data.items`" in error.excerpt


def test_an_empty_object_at_a_named_path_is_no_rows():
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", _structured_document({"status": "ok", "data": {}}))
        got = _call(_runner(mcp_config(fake.url, rows_path="data")))

    assert got == []


def test_max_rows_with_an_object_result_is_the_one_row():
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", _structured_document(EXAMPLE_ITEM))
        got = _call(_runner(mcp_config(fake.url, rows_path="data", max_rows=1)))

    assert got == [EXAMPLE_ITEM["data"]]


def test_row_fields_project_an_object_row_and_a_missing_field_is_unusable():
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", _structured_document(EXAMPLE_ITEM))
        got = _call(_runner(mcp_config(
            fake.url, rows_path="data", row_fields=["scope", "name"],
        )))
        error = _failure(_runner(mcp_config(
            fake.url, rows_path="data", row_fields=["scope", "site"],
        )))

    assert got == [{"scope": "demo", "name": "/P/Q/RAW"}]
    assert error.failure == "result_unusable"
    assert "`site`" in error.excerpt


def test_an_object_row_carrying_the_token_is_refused_and_not_repeated(
    monkeypatch,
):
    with FakeMcpUpstream(token=TOKEN) as fake:
        fake.tool("datasets", _structured_document(
            {"status": "ok", "data": {"note": f"saw {TOKEN}"}}))
        error = _failure(_runner(mcp_config(fake.url, rows_path="data"),
                                 secret=True, monkeypatch=monkeypatch))

    assert error.failure == "response_unusable"
    assert TOKEN not in (error.excerpt or "")
    assert "credential" in error.excerpt


def test_a_single_json_text_object_at_a_rows_path_is_one_row():
    """The text path reads `rows_path` the same way as structured content."""
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", json_text(EXAMPLE_ITEM))
        got = _call(_runner(mcp_config(fake.url, rows_path="data")))

    assert got == [EXAMPLE_ITEM["data"]]


@pytest.mark.parametrize(
    "document, rows_path",
    [
        pytest.param({"status": "ok", "data": 7}, "data", id="int-at-path"),
        pytest.param({"status": "ok", "data": "x"}, "data", id="str-at-path"),
        pytest.param({"status": "ok", "data": None}, "data", id="null-at-path"),
    ],
)
def test_a_structured_scalar_is_still_refused(document, rows_path):
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", _structured_document(document))
        error = _failure(_runner(mcp_config(fake.url, rows_path=rows_path)))

    assert error.failure == "result_unusable", error.excerpt
    assert "not a list of rows or one object" in error.excerpt


def test_a_structured_object_too_deep_to_shape_is_still_refused():
    from okg.substrate.external_backed.live_dispatch import MAX_RESULT_DEPTH

    deep: Any = {"leaf": 1}
    for _ in range(MAX_RESULT_DEPTH + 2):
        deep = {"k": deep}
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", _structured_document({"data": deep}))
        error = _failure(_runner(mcp_config(fake.url, rows_path="data")))

    assert error.failure == "result_unusable", error.excerpt
    assert "nesting deeper than" in error.excerpt


def test_a_structured_list_at_a_rows_path_is_still_the_rows():
    rows = [{"unit": "T1"}, {"unit": "T2"}]
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", _structured_document({"data": rows}))
        got = _call(_runner(mcp_config(fake.url, rows_path="data")))

    assert got == rows


def test_rows_come_from_a_single_json_text_block():
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", json_text({"rows": [{"unit": "U2_AA_ONE"}]}))
        got = _call(_runner(mcp_config(fake.url)))

    assert got == [{"unit": "U2_AA_ONE"}]


def test_a_root_rows_path_reads_a_top_level_json_array():
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", json_text([{"unit": "T1"}, {"unit": "T2"}]))
        got = _call(_runner(mcp_config(fake.url, rows_path=".")))

    assert got == [{"unit": "T1"}, {"unit": "T2"}]


@pytest.mark.parametrize(
    "behaviour, rows_path, why",
    [
        pytest.param(text_blocks("[1]", "[2]"), "rows", "no one document",
                     id="two-blocks-under-a-path"),
        pytest.param(text_blocks('{"a": 1}', "[2]"), ".",
                     "not one JSON object", id="two-blocks-one-a-list"),
        pytest.param(text_blocks('{"a": 1}', "not json"), ".", "not JSON",
                     id="two-blocks-one-not-json"),
        pytest.param(text_blocks("7"), ".", "not a list",
                     id="root-scalar"),
        pytest.param(text_blocks("not json at all"), "rows", "not JSON",
                     id="non-json-text"),
        pytest.param(json_text({"other": []}), "rows", "has no `rows`",
                     id="missing-path"),
        pytest.param(json_text({"rows": "a"}), "rows", "not a list",
                     id="path-holds-a-scalar"),
        pytest.param(text_blocks(), "rows", "0 text blocks",
                     id="no-content-under-a-path"),
    ],
)
def test_an_unusable_result_is_result_unusable(behaviour, rows_path, why):
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", behaviour)
        error = _failure(_runner(mcp_config(fake.url, rows_path=rows_path)))
        closes = fake.wait_for_closes(1)

    assert error.failure == "result_unusable", error.excerpt
    assert why in error.excerpt
    assert closes == 1, "the session is closed after an error"


def test_one_json_object_per_text_block_is_one_row_each_in_order():
    """A FastMCP tool returning `-> Any` sends a list
    as one text block per item and no structuredContent, as some servers
    do. Each block is one row, in the order sent."""
    blocks = [json.dumps({"dataset": f"/P{i}/Q/RAW", "n": i}) for i in range(4)]
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", text_blocks(*blocks))
        got = _call(_runner(mcp_config(fake.url, rows_path=".")))
        closes = fake.wait_for_closes(1)

    assert got == [json.loads(b) for b in blocks]
    assert fake.counters.calls == ["datasets"]
    assert closes == 1


def test_a_single_bare_object_at_the_root_is_one_row():
    """The one-item list from such a tool: one block holding one object."""
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", json_text({"dataset": "/P/Q/RAW"}))
        got = _call(_runner(mcp_config(fake.url, rows_path=".")))

    assert got == [{"dataset": "/P/Q/RAW"}]


def test_no_content_at_the_root_is_no_rows():
    """The empty list from such a tool sends no block at all."""
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", text_blocks())
        got = _call(_runner(mcp_config(fake.url, rows_path=".")))

    assert got == []


def test_a_non_text_block_without_structured_content_is_unusable():
    from mcp import types

    def image(_args):
        return types.CallToolResult(content=[
            types.TextContent(type="text", text='{"a": 1}'),
            types.ImageContent(type="image", data="aGk=", mimeType="image/png"),
        ])

    with FakeMcpUpstream() as fake:
        fake.tool("datasets", image)
        error = _failure(_runner(mcp_config(fake.url, rows_path=".")))

    assert error.failure == "result_unusable"
    assert "non-text" in error.excerpt


def test_the_byte_cap_holds_for_one_block_per_row_replies():
    """60 MB sent as one block per row is refused on the wire like any other
    reply: the cap counts the session, not the shape."""
    blocks = [json.dumps(row) for row in sized_rows(60 * MIB)]
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", text_blocks(*blocks))
        error = _failure(_runner(mcp_config(fake.url, rows_path=".")))

    assert error.failure == "response_over_cap"


def test_row_fields_select_and_a_missing_field_is_unusable():
    rows = [{"unit": "T1", "kind": "disk", "noise": "x" * 50}]
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", structured(rows))
        got = _call(_runner(mcp_config(fake.url, row_fields=["unit", "kind"])))
        error = _failure(_runner(mcp_config(fake.url, row_fields=["unit", "site"])))

    assert got == [{"unit": "T1", "kind": "disk"}]
    assert error.failure == "result_unusable"
    assert "`site`" in error.excerpt


def test_max_rows_cuts_by_count():
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", structured([{"n": i} for i in range(30)]))
        got = _call(_runner(mcp_config(fake.url, max_rows=4)))

    assert got == [{"n": i} for i in range(4)]


def _listing_reply(rows: list, truncated: Any) -> dict:
    """A list tool's reply shape: the rows under `items` and the
    tool's own cut flag beside them."""
    return {"status": "ok", "count": len(rows), "truncated": truncated,
            "items": rows}


@pytest.mark.parametrize(
    "behaviour",
    [
        pytest.param(lambda doc: _structured_document(doc), id="structured"),
        pytest.param(lambda doc: json_text(doc), id="one-text-block"),
    ],
)
def test_a_reply_marked_truncated_beside_its_rows_comes_back_marked_cut(behaviour):
    """R7 shape: a list tool stopped at its
    `limit` of 200 and said `truncated: true`; the adapter returned the 200
    rows as if whole. Now the rows come back marked, with the count sent."""
    from okg.substrate.external_backed.live_dispatch import UpstreamCutRows

    rows = [{"unit": f"U2_X_{i}"} for i in range(7)]
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", behaviour(_listing_reply(rows, True)))
        got = _call(_runner(mcp_config(fake.url, rows_path="items")))

    assert isinstance(got, UpstreamCutRows)
    assert list(got) == rows and got.received == 7


@pytest.mark.parametrize("flag", [False, None], ids=["false", "null"])
def test_a_reply_marked_whole_is_a_plain_list(flag):
    from okg.substrate.external_backed.live_dispatch import UpstreamCutRows

    rows = [{"unit": "U1_A"}]
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", _structured_document(_listing_reply(rows, flag)))
        got = _call(_runner(mcp_config(fake.url, rows_path="items")))

    assert got == rows and not isinstance(got, UpstreamCutRows)


def test_the_cut_counts_the_rows_sent_before_max_rows_cuts_them():
    from okg.substrate.external_backed.live_dispatch import UpstreamCutRows

    rows = [{"n": i} for i in range(9)]
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", _structured_document(_listing_reply(rows, True)))
        got = _call(_runner(mcp_config(fake.url, rows_path="items", max_rows=4)))

    assert isinstance(got, UpstreamCutRows)
    assert list(got) == rows[:4] and got.received == 9


@pytest.mark.parametrize("flag", ["yes", 1, [True]], ids=["text", "number", "list"])
def test_a_truncated_flag_that_is_not_a_boolean_is_unusable(flag):
    """Must-fail partner: "are these all the rows?" is never guessed."""
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", _structured_document(_listing_reply([{"a": 1}], flag)))
        error = _failure(_runner(mcp_config(fake.url, rows_path="items")))

    assert error.failure == "result_unusable", error.excerpt
    assert "`truncated` beside `items`" in error.excerpt


def test_a_truncated_key_that_is_not_beside_the_rows_is_row_data():
    """Only the object HOLDING the rows is read; a row's own `truncated`
    field is data."""
    from okg.substrate.external_backed.live_dispatch import UpstreamCutRows

    rows = [{"name": "/A/B/C", "truncated": True}]
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", _structured_document({"status": "ok", "items": rows}))
        got = _call(_runner(mcp_config(fake.url, rows_path="items")))

    assert got == rows and not isinstance(got, UpstreamCutRows)


def test_an_is_error_result_is_upstream_unavailable_with_a_bounded_excerpt():
    """A 2 MB exception text, as Lane CM measured, comes back as 256 chars."""
    text = "Traceback: backend exploded " + "z" * (2 * MIB)
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", text_blocks(text, is_error=True))
        error = _failure(_runner(mcp_config(fake.url)))
        closes = fake.wait_for_closes(1)

    assert error.failure == "upstream_unavailable"
    assert error.excerpt.startswith("Traceback: backend exploded")
    assert len(error.excerpt) <= 256
    assert closes == 1


#: A list tool's reply for an expression that selects no unit, in the
#: wording a live server returned for one.
EMPTY_SET = (
    "Error executing tool example_list_units: Provided unit expression is "
    "considered invalid.\nDetails: Unit expression resulted in an empty set."
)


def test_an_is_error_empty_set_reply_is_its_own_outcome_not_unavailable():
    """The server is up and answered "nothing matched" as an error. That is
    `upstream_empty_result` with the server's own message, never
    `upstream_unavailable`, which sends the caller looking for an outage."""
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", text_blocks(EMPTY_SET, is_error=True))
        error = _failure(_runner(mcp_config(fake.url)))

    assert error.failure == "upstream_empty_result"
    assert "resulted in an empty set" in error.excerpt
    assert fake.counters.calls == ["datasets"], "one call, no retry"


def test_an_is_error_reply_without_the_empty_marker_stays_unavailable():
    """The marker is read only as an empty match; any other tool error keeps
    its failure, so the new outcome cannot swallow a real error."""
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", text_blocks(
            "Error executing tool: ExampleError Code:114 wrong pattern",
            is_error=True,
        ))
        error = _failure(_runner(mcp_config(fake.url)))

    assert error.failure == "upstream_unavailable"
    assert "Code:114" in error.excerpt


def test_a_crash_that_mentions_an_empty_set_stays_a_failure():
    """Review M1 (lane ri, E1): "empty set" anywhere in the error text read
    as "matched nothing", so a server crash such as `KeyError: 'pop from an
    empty set'` would be served as a clean 0 rows. Only the whole phrase
    "resulted in an empty set" is an empty match."""
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", text_blocks(
            "Error executing tool example_list_units: Traceback (most recent call "
            "last):\n  File \"server.py\", line 88\nKeyError: 'pop from an empty set'",
            is_error=True,
        ))
        error = _failure(_runner(mcp_config(fake.url)))

    assert error.failure == "upstream_unavailable"
    assert "pop from an empty set" in error.excerpt


def test_an_empty_set_inside_a_successful_result_is_data_not_a_failure():
    """Only an `isError` result is classified; the same words in a normal
    reply are row text."""
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", structured([{"note": "an empty set is fine"}]))
        got = _call(_runner(mcp_config(fake.url)))

    assert got == [{"note": "an empty set is fine"}]


#: "What you named is not there" replies, shaped as a live list/get server
#: sent them in recorded runs.
#: R1 (a name sent without its leading "/").
ITEM_NOT_FOUND = (
    "Error executing tool example_get_item: Item not found.\n"
    "Details: Item 'demo:alpha-sample_N-2_MD-10000_T5-"
    "gen8/Campaign3-130X_v15-v4/"
    "FORMATA' not found"
)
#: R2.
ITEM_RECORDS_NOT_FOUND = (
    "Error executing tool example_list_item_records: Item not found.\n"
    "Details: Item 'demo:beta-sample_M-5000_"
    "13x_gen8/Campaign2-Reprocess17_"
    "102X_asymptotic_v6-v1/FORMATB' not found"
)
#: R4 (R5 is the same text with c128171c...).
RECORD_NOT_FOUND = (
    "Error executing tool example_get_record: No record "
    "found.\nDetails: No record with the id 3fe781fc37294eafb22c0c9eb9a584d6 "
    "found"
)
UNIT_NOT_FOUND = (
    "Error executing tool example_list_unit_attributes: Unit does not exist.\n"
    "Details: Unit 'U3_BB_LAB' cannot be found in group 'def'"
)
SCOPE_NOT_FOUND = (
    "Error executing tool example_list_scopes_for_account: Scope does not "
    "exist.\nDetails: no scopes found for account ID 'demo_group'"
)
#: A real outage from the same runs: it must keep `upstream_unavailable`.
READ_TIMEOUT = (
    "Error executing tool example_list_items: HTTPConnectionPool("
    "host='upstream.example.test', port=80): Read timed out. (read timeout=110)"
)


@pytest.mark.parametrize(
    "text, words",
    [
        pytest.param(ITEM_NOT_FOUND, "alpha-sample", id="R1-item"),
        pytest.param(ITEM_RECORDS_NOT_FOUND, "beta-sample", id="R2-item-records"),
        pytest.param(RECORD_NOT_FOUND, "3fe781fc37294eafb22c0c9eb9a584d6",
                     id="R4-rule"),
        pytest.param(UNIT_NOT_FOUND, "U3_BB_LAB", id="unit"),
        pytest.param(SCOPE_NOT_FOUND, "demo_group", id="scope"),
    ],
)
def test_a_not_found_reply_is_its_own_outcome_not_unavailable(text, words):
    """The server answered that the named object is not there. That is
    `upstream_not_found` with the server's own words, never
    `upstream_unavailable` ("refused the connection"), which sent agents
    looking for an outage and read a missing "/" as "no longer exists"."""
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", text_blocks(text, is_error=True))
        error = _failure(_runner(mcp_config(fake.url)))

    assert error.failure == "upstream_not_found", error.excerpt
    assert words in error.excerpt
    assert fake.counters.calls == ["datasets"], "one call, no retry"


@pytest.mark.parametrize(
    "text",
    [
        pytest.param(READ_TIMEOUT, id="read-timeout"),
        pytest.param("Error executing tool example_list_items: Response ended "
                     "prematurely", id="ended-early"),
        # "not found" past the first sentence is not the error's summary: a
        # credential problem stays an upstream failure.
        pytest.param("Error executing tool example_whoami: Cannot authenticate."
                     "\nDetails: x509 proxy file not found", id="auth"),
    ],
)
def test_an_outage_reply_stays_unavailable_even_with_not_found_later(text):
    """Must-fail partner: the new outcome cannot swallow a real outage."""
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", text_blocks(text, is_error=True))
        error = _failure(_runner(mcp_config(fake.url)))

    assert error.failure == "upstream_unavailable", error.excerpt


def test_not_found_inside_a_successful_result_is_data_not_a_failure():
    """Only an `isError` result is classified; the same words in a normal
    reply are row text."""
    rows = [{"note": "Item not found", "state": "No record found"}]
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", structured(rows))
        got = _call(_runner(mcp_config(fake.url)))

    assert got == rows


#: A tool's reply to a unit expression it cannot parse, as recorded
#: in earlier runs.
UNIT_EXPRESSION_INVALID = (
    "Error executing tool example_list_units: Provided unit expression is "
    "considered invalid.\nDetails: Expression does not comply to unit "
    "expression syntax"
)
#: The same first sentence, but the expression parsed and selects no unit
#: (for example a filter that matches nothing): a real empty result.
UNIT_EXPRESSION_EMPTY = (
    "Error executing tool example_list_units: Provided unit expression is "
    "considered invalid.\nDetails: Unit expression resulted in an empty set."
)
#: A call whose arguments break the tool's schema.
SCHEMA_MISMATCH = (
    "Error executing tool example_list_units: 1 validation error for "
    "call[example_list_units]\nlimit\n  Input should be a valid integer"
)


@pytest.mark.parametrize(
    "text, words",
    [
        pytest.param(UNIT_EXPRESSION_INVALID, "does not comply",
                     id="unit-syntax"),
        pytest.param(SCHEMA_MISMATCH, "validation error", id="schema"),
        pytest.param("Error executing tool x: Object does not match schema.",
                     "does not match schema", id="does-not-match-schema"),
    ],
)
def test_an_argument_the_server_rejects_is_its_own_outcome(text, words):
    """The server answered that the ARGUMENT is invalid. Before: some of
    these were `upstream_unavailable` ("refused the connection"), others
    `upstream_empty_result` ("matched nothing"); both are untrue."""
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", text_blocks(text, is_error=True))
        error = _failure(_runner(mcp_config(fake.url)))

    assert error.failure == "upstream_rejected_arguments", error.excerpt
    assert words in error.excerpt
    assert fake.counters.calls == ["datasets"], "one call, no retry"


def test_an_expression_that_parses_and_selects_nothing_stays_empty():
    """Must-fail partner: a real empty set keeps `upstream_empty_result`,
    even behind the same "considered invalid" first sentence."""
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", text_blocks(UNIT_EXPRESSION_EMPTY,
                                          is_error=True))
        error = _failure(_runner(mcp_config(fake.url)))

    assert error.failure == "upstream_empty_result", error.excerpt


@pytest.mark.parametrize(
    "text",
    [
        pytest.param(READ_TIMEOUT, id="read-timeout"),
        # "invalid" past the first sentence is not the error's summary.
        pytest.param("Error executing tool example_whoami: Cannot authenticate."
                     "\nDetails: x509 proxy is invalid", id="auth"),
    ],
)
def test_an_outage_reply_stays_unavailable_even_with_invalid_later(text):
    """Must-fail partner: the new outcome cannot swallow a real outage."""
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", text_blocks(text, is_error=True))
        error = _failure(_runner(mcp_config(fake.url)))

    assert error.failure == "upstream_unavailable", error.excerpt


def test_invalid_inside_a_successful_result_is_data_not_a_failure():
    """Only an `isError` result is classified."""
    rows = [{"note": "Provided unit expression is considered invalid"}]
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", structured(rows))
        got = _call(_runner(mcp_config(fake.url)))

    assert got == rows


def test_an_unknown_tool_is_upstream_unavailable():
    with FakeMcpUpstream() as fake:
        fake.tool("something_else", structured([]))
        error = _failure(_runner(mcp_config(fake.url)))

    assert error.failure == "upstream_unavailable"
    assert "Unknown tool" in error.excerpt


def test_a_refused_port_is_upstream_unavailable():
    import socket

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    # Nothing listens there now.
    error = _failure(_runner(mcp_config(f"http://127.0.0.1:{port}/mcp")))
    assert error.failure == "upstream_unavailable"


def test_a_non_mcp_endpoint_is_refused_at_once_not_at_the_deadline():
    """A plain web page at `server_url` is a wrong endpoint, not a slow one."""
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    import threading

    class Page(BaseHTTPRequestHandler):
        def log_message(self, *_a):
            return

        def do_POST(self):  # noqa: N802
            self.rfile.read(int(self.headers.get("Content-Length") or 0))
            body = b"<html>hello</html>"
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Page)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        runner = _runner(mcp_config(f"http://127.0.0.1:{server.server_address[1]}/mcp"))
        started = time.monotonic()
        error = _failure(runner, deadline=10)
        elapsed = time.monotonic() - started
    finally:
        server.shutdown()
        server.server_close()

    assert error.failure == "response_unusable"
    assert elapsed < 5, elapsed


def test_a_deadline_shorter_than_the_tool_is_a_timeout_and_the_session_closes():
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", structured([{"a": 1}]), delay=4.0)
        runner = _runner(mcp_config(fake.url))
        started = time.monotonic()
        error = _failure(runner, deadline=1.5)
        elapsed = time.monotonic() - started
        closes = fake.wait_for_closes(1)

    assert error.failure == "timeout"
    # Inside the declared deadline: the adapter cut its own call early
    # enough that the session's close also fit.
    assert elapsed <= 1.5 + 0.25, elapsed
    assert closes == 1, "the fake saw the session closed after the deadline"
    assert fake.counters.calls == ["datasets"]
    # The timeout names the step that hung: the server opened the session
    # and then sat on the tool call.
    assert "answered `initialize` in" in error.excerpt, error.excerpt
    assert "sent no tool result within" in error.excerpt, error.excerpt


def test_a_server_silent_on_initialize_times_out_naming_initialize():
    """A slow backend's hang, reproduced: a server that runs tool calls one at a
    time answers not even `initialize` while another client's call runs. The
    deadline is unchanged; the report says which step never got an answer,
    and that no tool call was sent."""
    with FakeMcpUpstream(initialize_delay=4.0) as fake:
        fake.tool("datasets", structured([{"a": 1}]))
        runner = _runner(mcp_config(fake.url))
        started = time.monotonic()
        error = _failure(runner, deadline=1.5)
        elapsed = time.monotonic() - started

    assert error.failure == "timeout"
    assert elapsed <= 1.5 + 0.25, elapsed
    assert "did not answer `initialize` within" in error.excerpt, error.excerpt
    assert "no tool call was sent" in error.excerpt
    assert fake.counters.sessions == 1
    assert fake.counters.calls == [], "no tool call reached the server"
    assert len(error.excerpt) <= 256


def test_a_slow_initialize_inside_the_deadline_is_waited_for_not_cut():
    """The other side of the same row: okg does not give up on a slow
    session start early. A server busy for 1.5 s still answers inside a 6 s
    deadline, as a direct client waiting the full time would see."""
    with FakeMcpUpstream(initialize_delay=1.5) as fake:
        fake.tool("datasets", structured([{"a": 1}]))
        started = time.monotonic()
        got = _call(_runner(mcp_config(fake.url)), deadline=6.0)
        elapsed = time.monotonic() - started

    assert got == [{"a": 1}]
    assert 1.5 <= elapsed < 6.0, elapsed
    assert fake.counters.calls == ["datasets"]


def test_the_arguments_reach_the_tool_typed_and_beside_the_operators_own():
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", echo_arguments())
        got = _call(_runner(mcp_config(fake.url)),
                    args={"name": "*HI*", "limit": 7})

    assert got == [{"received": {"pattern": "*HI*", "limit": 7, "detail": True}}]
    assert isinstance(got[0]["received"]["limit"], int)


def test_a_slotted_value_that_looks_like_a_slot_reaches_the_tool_as_a_string():
    """One pass, never rescanned: the caller's text is data, not grammar."""
    loose = (
        LiveRelationArg(name="name", type_name="string", required=True,
                        pattern="^.{1,80}$", max_length=80),
        ARGS[1],
    )
    tricky = '{"$arg": "x"}'
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", echo_arguments())
        runner = _runner(mcp_config(fake.url), args=loose)
        got = _call(runner, args={"name": tricky, "limit": 1})

    assert got[0]["received"]["pattern"] == tricky
    assert isinstance(got[0]["received"]["pattern"], str)


# ═══════════════════════════════ egress ═════════════════════════════════

def test_a_redirect_is_refused_and_never_followed():
    """The SDK follows a same-origin 307 by itself; the transport does not."""
    with FakeMcpUpstream(redirect=True) as fake:
        error = _failure(_runner(mcp_config(fake.url)))

    assert error.failure == "redirect_refused"
    assert error.status == 302
    assert fake.counters.http_requests == ["POST"], "the target was not asked"


def test_the_bearer_header_is_sent_and_a_wrong_one_is_refused(monkeypatch):
    with FakeMcpUpstream(token=TOKEN) as fake:
        fake.tool("datasets", structured([{"a": 1}]))
        got = _call(_runner(mcp_config(fake.url), secret=True,
                            monkeypatch=monkeypatch))
        sent = set(fake.counters.authorization)
    assert got == [{"a": 1}]
    assert sent == {f"Bearer {TOKEN}"}

    with FakeMcpUpstream(token="some-other-token-entirely") as fake:
        fake.tool("datasets", structured([{"a": 1}]))
        error = _failure(_runner(mcp_config(fake.url), secret=True,
                                 monkeypatch=monkeypatch))
    assert error.failure == "upstream_unavailable"
    assert error.status == 401
    assert TOKEN not in json.dumps(
        {"excerpt": error.excerpt, "message": str(error)},
    )


def test_a_reply_carrying_the_token_is_refused_and_not_repeated(monkeypatch):
    with FakeMcpUpstream(token=TOKEN) as fake:
        fake.tool("datasets", json_text({"rows": [{"leak": f"saw {TOKEN}"}]}))
        error = _failure(_runner(mcp_config(fake.url), secret=True,
                                 monkeypatch=monkeypatch))

    assert error.failure == "response_unusable"
    assert TOKEN not in (error.excerpt or "")
    assert "credential" in error.excerpt


def test_the_token_reaches_no_log_record(monkeypatch, caplog):
    """Every record any logger wrote, at DEBUG, across success and failure."""
    caplog.set_level(logging.DEBUG)
    with FakeMcpUpstream(token=TOKEN) as fake:
        fake.tool("datasets", structured([{"a": 1}]))
        fake.tool("boom", text_blocks("bad", is_error=True))
        runner = _runner(mcp_config(fake.url), secret=True, monkeypatch=monkeypatch)
        _call(runner)
        _failure(_runner(mcp_config(fake.url, tool="boom"), secret=True,
                         monkeypatch=monkeypatch))
    with FakeMcpUpstream(token="wrong-token-for-this-row") as fake:
        _failure(_runner(mcp_config(fake.url), secret=True, monkeypatch=monkeypatch))

    records = _okg_side_records(caplog.records)
    assert any(r.name.startswith("mcp.client") for r in records), (
        "the capture must not be vacuous: the MCP client logged nothing"
    )
    for record in records:
        text = record.getMessage() + repr(record.__dict__)
        assert TOKEN not in text, record.name


# ═══════════════════════════════ load refusals ══════════════════════════

def _parse(config: dict[str, Any], args: tuple = ARGS, **binding: Any):
    from okg.substrate.external_backed.mcp_adapter import parse_mcp_binding

    return parse_mcp_binding("mcp_hits", _binding(config, **binding), _declared(args))


URL = "http://127.0.0.1:5952/mcp"


@pytest.mark.parametrize(
    "overrides, field",
    [
        pytest.param({"method": "POST"}, "method", id="unknown-key"),
        pytest.param({"server_url": {"$arg": "name"}}, "server_url",
                     id="slot-in-server-url"),
        pytest.param({"tool": {"$arg": "name"}}, "tool", id="slot-in-tool"),
        pytest.param({"rows_path": {"$arg": "name"}}, "rows_path",
                     id="slot-in-rows-path"),
        pytest.param({"row_fields": [{"$arg": "name"}]}, "row_fields",
                     id="slot-in-row-fields"),
        pytest.param({"arguments": {"{\"$arg\": \"name\"}": 1,
                                    "limit": {"$arg": "limit"}}},
                     "arguments.{\"$arg\": \"name\"}", id="slot-as-key"),
        pytest.param({"arguments": {"pattern": {"$arg": "name", "default": "x"},
                                    "limit": {"$arg": "limit"}}},
                     "arguments.pattern", id="slot-with-siblings"),
        pytest.param({"arguments": {"pattern": "prefix-$arg-name",
                                    "limit": {"$arg": "limit"}}},
                     "arguments.pattern", id="slot-inside-a-string"),
        pytest.param({"arguments": {"pattern": {"$arg": "nmae"},
                                    "limit": {"$arg": "limit"}}},
                     "arguments", id="undeclared-argument"),
        pytest.param({"arguments": {"limit": {"$arg": "limit"}}},
                     "arguments", id="unused-argument"),
        pytest.param({"arguments": None}, "arguments", id="no-arguments"),
        pytest.param({"tool": "has space"}, "tool", id="bad-tool-name"),
        pytest.param({"rows_path": "a..b"}, "rows_path", id="bad-rows-path"),
        pytest.param({"max_response_bytes": 512}, "max_response_bytes",
                     id="cap-too-small"),
        pytest.param({"max_response_bytes": 33 * MIB}, "max_response_bytes",
                     id="cap-too-big"),
        pytest.param({"max_rows": 0}, "max_rows", id="max-rows-zero"),
        pytest.param({"auth_header": "Mcp-Session-Id"}, "auth_header",
                     id="sdk-owned-header"),
        pytest.param({"server_url": URL + "?x=1"}, "server_url",
                     id="query-in-url"),
    ],
)
def test_each_malformed_binding_is_refused_at_load_naming_the_field(
    overrides, field,
):
    from okg.substrate.external_backed.http_adapter import (
        LiveAdapterConfigInvalid,
    )

    with pytest.raises(LiveAdapterConfigInvalid) as caught:
        _parse(mcp_config(URL, **overrides))
    assert caught.value.data["relation"] == "mcp_hits"
    assert caught.value.data["field"] == field, caught.value.data


@pytest.mark.parametrize(
    "arg",
    [
        pytest.param(LiveRelationArg(name="name", type_name="string",
                                     required=True, max_length=80),
                     id="string-without-pattern"),
        pytest.param(LiveRelationArg(name="name", type_name="number",
                                     required=True, minimum=0),
                     id="number-without-maximum"),
    ],
)
def test_an_unconstrained_slotted_argument_is_refused(arg):
    from okg.substrate.external_backed.http_adapter import (
        LiveAdapterArgUnconstrained,
    )

    with pytest.raises(LiveAdapterArgUnconstrained) as caught:
        _parse(mcp_config(URL), args=(arg, ARGS[1]))
    assert caught.value.data == {
        "relation": "mcp_hits", "argument": "name", "type": arg.type_name,
    }


def test_a_well_formed_binding_parses_with_its_defaults():
    parsed = _parse(mcp_config(URL))
    assert parsed.url == URL
    assert parsed.tool == "datasets"
    assert parsed.max_response_bytes == 8 * MIB
    assert parsed.max_rows is None
    assert parsed.allowed == ((parsed.address, 5952),)
    assert parsed.narrowing_args == ("limit", "name")


@pytest.mark.parametrize(
    "overrides, reason",
    [
        pytest.param({"allow_non_global": None}, "non_global",
                     id="loopback-without-opt-in"),
        pytest.param({"allowed_hosts": ["127.0.0.1:5953"]}, "host_not_allowed",
                     id="outside-allowed-hosts"),
        pytest.param({"server_url": "http://archi.example/mcp"},
                     "host_not_literal", id="hostname"),
        pytest.param({"server_url": "ftp://127.0.0.1/mcp"}, "scheme",
                     id="scheme"),
        pytest.param({"server_url": "http://u:p4ssw0rd@127.0.0.1:5952/mcp"},
                     "userinfo", id="userinfo"),
    ],
)
def test_each_egress_refusal_fails_the_load(overrides, reason):
    from okg.substrate.external_backed.http_adapter import LiveAdapterHostRefused

    with pytest.raises(LiveAdapterHostRefused) as caught:
        _parse(mcp_config(URL, **overrides))
    assert caught.value.data["reason"] == reason
    assert "p4ssw0rd" not in str(caught.value) + json.dumps(caught.value.data)


def test_the_central_egress_validator_decides_every_server_url(monkeypatch, caplog):
    """The load runs `assert_public_http_url` on the server address, with the
    declared `allow_non_global` as its logged operator exception, and a
    refusal from it fails the load."""
    import logging

    from okg.substrate.external_backed import mcp_adapter
    from okg.substrate.external_backed.http_adapter import LiveAdapterHostRefused
    from okg.substrate.library.sources._url_safety import UnsafeFetchURL

    seen: list[tuple[str, tuple]] = []
    real = mcp_adapter.assert_public_http_url

    def spy(url, *, allow_hosts=()):
        seen.append((url, tuple(allow_hosts)))
        return real(url, allow_hosts=allow_hosts)

    monkeypatch.setattr(mcp_adapter, "assert_public_http_url", spy)
    with caplog.at_level(logging.WARNING):
        _parse(mcp_config(URL, allow_non_global=True))
    assert seen == [("http://127.0.0.1:5952", ("127.0.0.1",))]
    assert "egress allowlist exception" in caplog.text

    seen.clear()
    _parse(mcp_config("http://8.8.8.8:443/mcp", allow_non_global=False))
    assert seen == [("http://8.8.8.8:443", ())]

    def refuse(url, *, allow_hosts=()):
        raise UnsafeFetchURL("refused by the central validator")

    monkeypatch.setattr(mcp_adapter, "assert_public_http_url", refuse)
    with pytest.raises(LiveAdapterHostRefused) as caught:
        _parse(mcp_config("http://8.8.8.8:443/mcp", allow_non_global=False))
    assert caught.value.data["reason"] == "egress_validator"


def test_a_secret_ref_without_auth_header_and_vice_versa_are_refused():
    from okg.substrate.external_backed.http_adapter import (
        LiveAdapterConfigInvalid,
    )

    with pytest.raises(LiveAdapterConfigInvalid) as caught:
        _parse(mcp_config(URL, auth_header="Authorization"))
    assert caught.value.data["field"] == "auth_header"
    binding = LiveBinding(
        relation="mcp_hits", kind="mcp", config=mcp_config(URL),
        secret_refs=("env:OKG_MCP_TEST_TOKEN",), upstream_credential="service",
    )
    from okg.substrate.external_backed.mcp_adapter import parse_mcp_binding

    with pytest.raises(LiveAdapterConfigInvalid) as caught:
        parse_mcp_binding("mcp_hits", binding, _declared())
    assert caught.value.data["field"] == "auth_header"


def test_a_caller_supplied_mcp_runner_skips_the_config_parser():
    from okg.substrate.external_backed.mcp_adapter import (
        validate_mcp_binding_configs,
    )

    bad = _binding({"nonsense": True})
    assert validate_mcp_binding_configs(
        {"mcp_hits": bad}, {"mcp_hits": _declared()}, runners={"mcp": object()},
    ) == {}


# ═══════════════════════════════ credentials ════════════════════════════

def test_the_manifest_parser_refuses_a_missing_or_unknown_credential():
    from okg.substrate.external_backed.live_relations import (
        LiveRelationInvalid,
        parse_live_bindings,
    )

    for body in ({"kind": "mcp", "config": {}},
                 {"kind": "mcp", "config": {}, "upstream_credential": "mine"}):
        with pytest.raises(LiveRelationInvalid) as caught:
            parse_live_bindings({"mcp_hits": body})
        assert caught.value.data["field"] == "upstream_credential"
        assert caught.value.data["relation"] == "mcp_hits"
    with pytest.raises(LiveRelationInvalid) as caught:
        parse_live_bindings({"mcp_hits": {"kind": "http", "config": {},
                                      "upstream_credential": "service"}})
    assert caught.value.data["kind"] == "http"
    parsed = parse_live_bindings({"mcp_hits": {
        "kind": "mcp", "config": {}, "upstream_credential": "personal",
    }})
    assert parsed["mcp_hits"].upstream_credential == "personal"


@pytest.mark.parametrize("lifecycle", ["prototype", "demo"])
def test_a_personal_credential_is_admitted_on_prototype_and_demo(lifecycle):
    from okg.substrate.external_backed.mcp_adapter import (
        assert_mcp_credentials_admitted,
    )

    assert_mcp_credentials_admitted(
        {"mcp_hits": _binding({}, credential="personal")}, lifecycle=lifecycle,
    )


@pytest.mark.parametrize(
    "lifecycle", ["production", "staging", "archived", "blocked", None],
)
def test_a_personal_credential_is_refused_everywhere_else(lifecycle):
    from okg.substrate.external_backed.mcp_adapter import (
        LiveAdapterPersonalCredentialRefused,
        assert_mcp_credentials_admitted,
    )

    with pytest.raises(LiveAdapterPersonalCredentialRefused) as caught:
        assert_mcp_credentials_admitted(
            {"mcp_hits": _binding({}, credential="personal")}, lifecycle=lifecycle,
        )
    assert caught.value.code == "live_adapter_personal_credential_refused"
    assert caught.value.data["relation"] == "mcp_hits"
    assert caught.value.data["lifecycle"] == lifecycle
    assert "mcp_hits" in str(caught.value)
    assert str(lifecycle if lifecycle else "(none declared)") in str(caught.value)


def test_a_service_credential_is_admitted_under_production():
    from okg.substrate.external_backed.mcp_adapter import (
        assert_mcp_credentials_admitted,
    )

    assert_mcp_credentials_admitted(
        {"mcp_hits": _binding({}, credential="service")}, lifecycle="production",
    )


# ═══════════════════════════════ structure ══════════════════════════════

def test_no_raise_in_an_except_block_chains_implicitly():
    """The `http` adapter's rule, applied to this module: a wrapped
    exception's message is how a pasted secret reaches a log."""
    from tests.substrate.external_backed.test_http_adapter_structure import (
        unchained_raises,
    )

    source = (
        Path(__file__).resolve().parents[3]
        / "src/okg/substrate/external_backed/mcp_adapter.py"
    ).read_text(encoding="utf-8")
    assert unchained_raises(source) == []


def test_mcp_is_a_known_kind_and_a_built_in_runner():
    from okg.substrate.external_backed import mcp_adapter  # noqa: F401
    from okg.substrate.external_backed.handle import ADAPTER_KINDS
    from okg.substrate.external_backed.live_dispatch import builtin_live_adapters

    assert "mcp" in ADAPTER_KINDS
    assert set(builtin_live_adapters()) >= {"http", "mcp"}
    for kind in ("sql", "command", "python"):
        assert kind not in builtin_live_adapters()


# ════════════════ through build_server and expand(live=...) ═════════════
#
# Everything below needs a database: a real catalog, a real published
# generation, the real sample writer. The manifest is written to a temp
# deployments root and `build_server` reads it, so deleting the `mcp`
# wiring from `_build_server` reds these rows.

#: The declaration the catalog holds for the PostgreSQL rows.
RELATION = {
    "action": "external.example.items.read",
    "principal_mode": "service",
    "sample_policy": "inline",
    "retention_class": "warm",
    "deadline_seconds": 10,
    "args": {
        "name": {"type": "string", "required": True,
                 "pattern": "^[A-Za-z0-9*_/-]{1,80}$", "maxLength": 80},
        "limit": {"type": "integer", "default": 5, "minimum": 1,
                  "maximum": 50},
    },
}

_BUILT: list[Any] = []


@pytest.fixture(autouse=True)
def _close_built_servers():
    yield
    while _BUILT:
        server = _BUILT.pop()
        for attr in ("_okg_pool", "_okg_write_pool"):
            pool = getattr(server, attr, None)
            if pool is not None:
                pool.close()


def _publish_and_declare(dsn: str, relation: dict | None = None) -> str:
    from okg.substrate.external_backed.live_relations import (
        ANNOTATION_KEY,
        parse_live_relations,
    )
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import EdgeFact, NodeFact
    from okg.substrate.publisher import Publisher
    from tests.pytest.catalog_fixture_builders import (
        SubtypeSpec,
        bind_test_catalog_writer,
        seed_catalog,
    )

    sfx = uuid.uuid4().hex[:8]
    hub, peer = f"n:hub-{sfx}", f"n:peer-{sfx}"
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=[SubtypeSpec("page")],
                     narrowings=[("page", "cites", "page")])
        ChronosGraphStore.from_connection(conn).apply_facts([
            NodeFact(hub, "page", {"text": "hub"}, {"id": hub}, {"v": 1}),
            NodeFact(peer, "page", {"text": "peer"}, {"id": peer}, {"v": 1}),
            EdgeFact(src=peer, dst=hub, edge_type="cites", attrs={},
                     source_record_id={"src": peer, "dst": hub},
                     source_revision={"v": 1}),
        ])
    Publisher(
        dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default"),
    ).cycle()
    declared = parse_live_relations(
        {ANNOTATION_KEY: {"mcp_hits": dict(relation or RELATION)}}, subtype="page",
    )
    stored = {name: rel.to_metadata() for name, rel in declared.items()}
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE okg.subtypes SET metadata = COALESCE(metadata, '{}'::jsonb)"
            " || %s::jsonb WHERE name = 'page'",
            (json.dumps({"live_relations": stored}),),
        )
    return hub


@pytest.fixture
def graph(clean_runtime: str):
    return clean_runtime, _publish_and_declare(clean_runtime)


def _manifest(root: Path, bindings: dict, *, lifecycle: str | None = None,
              response_bytes: dict | None = None) -> None:
    import yaml

    d = root / "default"
    d.mkdir(parents=True, exist_ok=True)
    (d / "schemas").mkdir(exist_ok=True)
    (d / "source_registry.yaml").write_text("sources: {}\n", encoding="utf-8")
    doc: dict[str, Any] = {
        "name": "default",
        "description": "mcp adapter fixture",
        "postgres": {"dsn": "${OKG_MCPLIVE_DSN}"},
        "schema_dir": "schemas",
        "source_registry": "source_registry.yaml",
        "modules": [],
        "enrichers": [],
        "agent_surface": {
            "served": ["inspect", "search", "expand", "filter", "map",
                       "aggregate"],
            "live_bindings": bindings,
            **({"response_bytes": response_bytes} if response_bytes else {}),
        },
    }
    if lifecycle is not None:
        doc["contract"] = {"version": 1, "lifecycle": lifecycle,
                           "owner": "lane-rr", "intended_usage": "test"}
    (d / "deployment.yaml").write_text(
        yaml.safe_dump(doc, sort_keys=False), encoding="utf-8",
    )


def build(dsn: str, bindings: dict, *, monkeypatch, tmp_path, adapters=None,
          lifecycle: str | None = None, response_bytes: dict | None = None,
          gate: bool | None = True):
    """`build_server` on a manifest that really carries the bindings.

    `live_adapters` defaults to NOTHING: the production shape, where `mcp`
    must come from the built-in registry.
    """
    from okg.substrate.mcp.server import build_server

    _manifest(tmp_path, bindings, lifecycle=lifecycle,
              response_bytes=response_bytes)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path))
    monkeypatch.setenv("OKG_MCPLIVE_DSN", dsn)
    kwargs = {} if adapters is None else {"live_adapters": adapters}
    server = build_server(dsn=dsn, deployment_name="default", **kwargs)
    _BUILT.append(server)
    if gate is not None:
        server._okg_live_gate = _gate(gate)
    return server


def _gate(allowed: bool):
    from okg.substrate.external_backed.live_dispatch import GateOutcome

    def gate(relation, principal, node_id):
        return GateOutcome(
            allowed=allowed, reason="allow" if allowed else "test.deny",
            principal=principal or "service:okg-runtime",
            action=relation.action,
        )
    return gate


def mcp_binding(url: str, *, credential: str = "service", secret: bool = False,
                **overrides: Any) -> dict:
    config = mcp_config(url, **overrides)
    binding: dict[str, Any] = {
        "kind": "mcp", "config": config, "upstream_credential": credential,
    }
    if secret:
        config["auth_header"] = "Authorization"
        binding["secret_refs"] = ["env:OKG_MCP_TEST_TOKEN"]
    return {"mcp_hits": binding}


def drive(server, hub: str, *, args: dict | None = None,
          max_bytes: int | None = None, text: bool = False,
          relation: str = "mcp_hits") -> dict[str, Any]:
    """One `expand(live=...)` over a real client session.

    JSON format unless `text`: the client then names no format and the
    server sends the plain-text rendering a model reads.
    """
    from mcp.shared.memory import create_connected_server_and_client_session

    from tests.substrate.mcp._result_payload import JSON_META, payload

    out: dict[str, Any] = {}

    async def run() -> None:
        async with create_connected_server_and_client_session(server) as client:
            result = await client.call_tool("expand", {
                "input": {"node_id": hub}, "direction": "in",
                "live": {"relation": relation,
                         "args": args if args is not None else {"name": "x"}},
                **({"max_bytes": max_bytes} if max_bytes is not None else {}),
            }, meta={} if text else JSON_META)
            out["result"] = result
            out["served"] = len(
                result.model_dump_json(by_alias=True, exclude_none=True).encode()
            )
            if text:
                out["text"] = result.content[0].text
            else:
                out["payload"] = payload(result)

    anyio.run(run)
    return out


def live_block(out: dict[str, Any]) -> dict[str, Any]:
    block = out["payload"].get("external_live") or {}
    assert block, out["payload"]
    return block


def _samples(dsn: str) -> list[dict[str, Any]]:
    from okg.substrate.external_backed.call_sample import table_for_class

    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT status, decision, policy, declared_policy, payload, attrs "
            f"FROM {table_for_class('warm')} WHERE relation = 'mcp_hits' "
            f"ORDER BY observed_at",
        )
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def _independent_digest(rows: list[dict[str, Any]], relation: str = "mcp_hits") -> str:
    """sha256 of the full row set as the caller would have received it.

    Computed here, not by the dispatcher's helper: each string fenced with
    the read path's own marker function, then the canonical JSON the call
    sample uses (sorted keys, default separators) hashed with hashlib.
    """
    from okg.substrate.mcp.provenance import _fence_value

    source = f"external_live:{relation}"
    fenced = [
        {key: _fence_value(value, source=source) for key, value in row.items()}
        for row in rows
    ]
    blob = json.dumps(fenced, sort_keys=True, default=str).encode("utf-8")
    return "sha256:" + hashlib.sha256(blob).hexdigest()


# ── the registry ────────────────────────────────────────────────────────

@pytest.mark.postgres
def test_a_declared_mcp_relation_dispatches_with_no_caller_supplied_adapter(
    graph, monkeypatch, tmp_path,
):
    dsn, hub = graph
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", structured([{"dataset": "/A/B/C"}]))
        server = build(dsn, mcp_binding(fake.url), monkeypatch=monkeypatch,
                       tmp_path=tmp_path)
        block = live_block(drive(server, hub, args={"name": "*HI*", "limit": 3}))
        closes = fake.wait_for_closes(1)

    assert block["state"] == "ok", block
    assert block["source"] == "mcp"
    assert "/A/B/C" in json.dumps(block["rows"])
    assert fake.counters.calls == ["datasets"]
    assert fake.counters.arguments == [
        {"pattern": "*HI*", "limit": 3, "detail": True},
    ]
    assert closes == 1
    rows = _samples(dsn)
    assert len(rows) == 1 and rows[0]["status"] == "ok"
    assert rows[0]["attrs"]["adapter_kind"] == "mcp"
    assert rows[0]["attrs"]["adapter_ran"] is True


@pytest.mark.postgres
@pytest.mark.parametrize("kind", ["python", "sql", "command"])
def test_a_kind_with_no_runner_still_fails_the_build(graph, monkeypatch,
                                                     tmp_path, kind):
    from okg.substrate.external_backed.live_dispatch import LiveAdapterUnavailable

    dsn, _hub = graph
    with pytest.raises(LiveAdapterUnavailable) as caught:
        build(dsn, {"mcp_hits": {"kind": kind, "config": {}}},
              monkeypatch=monkeypatch, tmp_path=tmp_path)
    assert caught.value.data["kind"] == kind
    assert {"http", "mcp"} <= set(caught.value.data["registered"])


@pytest.mark.postgres
def test_a_bad_mcp_config_fails_the_build(graph, monkeypatch, tmp_path):
    from okg.substrate.external_backed.http_adapter import (
        LiveAdapterConfigInvalid,
    )

    dsn, _hub = graph
    with pytest.raises(LiveAdapterConfigInvalid) as caught:
        build(dsn, mcp_binding(URL, tool={"$arg": "name"}),
              monkeypatch=monkeypatch, tmp_path=tmp_path)
    assert caught.value.data["field"] == "tool"


@pytest.mark.postgres
def test_a_nomos_deny_never_reaches_the_server(graph, monkeypatch, tmp_path):
    dsn, hub = graph
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", structured([{"a": 1}]))
        server = build(dsn, mcp_binding(fake.url), monkeypatch=monkeypatch,
                       tmp_path=tmp_path, gate=False)
        block = live_block(drive(server, hub))

    assert block["state"] == "denied"
    assert fake.counters.requests == 0, fake.counters.http_requests
    rows = _samples(dsn)
    assert rows[0]["decision"] == "deny"
    assert rows[0]["attrs"]["adapter_ran"] is False


@pytest.mark.postgres
def test_no_bundle_on_disk_denies_by_default_and_reaches_nothing(
    graph, monkeypatch, tmp_path,
):
    """The real gate, not a test gate: no bundle is a default deny."""
    dsn, hub = graph
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", structured([{"a": 1}]))
        server = build(dsn, mcp_binding(fake.url), monkeypatch=monkeypatch,
                       tmp_path=tmp_path, gate=None)
        block = live_block(drive(server, hub))

    assert block["state"] == "denied", block
    assert fake.counters.requests == 0


@pytest.mark.postgres
def test_a_deadline_shorter_than_the_tool_times_out_and_closes_the_session(
    clean_runtime, monkeypatch, tmp_path,
):
    hub = _publish_and_declare(clean_runtime, {**RELATION, "deadline_seconds": 2})
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", structured([{"a": 1}]), delay=6.0)
        server = build(clean_runtime, mcp_binding(fake.url),
                       monkeypatch=monkeypatch, tmp_path=tmp_path)
        started = time.monotonic()
        block = live_block(drive(server, hub))
        elapsed = time.monotonic() - started
        closes = fake.wait_for_closes(1)

    assert block["state"] == "timeout", block
    assert block["error"]["reason"] == "live_relation_timeout"
    assert elapsed < 5, elapsed
    assert closes == 1, "the fake saw the session closed"


@pytest.mark.postgres
def test_a_full_slot_pool_refuses_with_the_concurrency_error(
    graph, monkeypatch, tmp_path,
):
    import threading

    from okg.substrate.external_backed import live_dispatch as ld

    dsn, hub = graph
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", structured([{"a": 1}]))
        server = build(dsn, mcp_binding(fake.url), monkeypatch=monkeypatch,
                       tmp_path=tmp_path)
        original = ld._LIVE_SLOTS
        ld._LIVE_SLOTS = threading.BoundedSemaphore(1)
        try:
            assert ld.acquire_live_slot() is True
            block = live_block(drive(server, hub))
        finally:
            ld._LIVE_SLOTS = original

    assert block["state"] == "error"
    assert block["error"]["reason"] == "live_concurrency_exhausted"
    assert fake.counters.requests == 0


# ── egress, through the tool ───────────────────────────────────────────

@pytest.mark.postgres
def test_the_token_is_in_no_envelope_sample_or_log(graph, monkeypatch, tmp_path,
                                                   caplog):
    caplog.set_level(logging.DEBUG)
    monkeypatch.setenv("OKG_MCP_TEST_TOKEN", f"Bearer {TOKEN}")
    dsn, hub = graph
    with FakeMcpUpstream(token=TOKEN) as fake:
        fake.tool("datasets", text_blocks(f"echo {TOKEN}"))
        server = build(dsn, mcp_binding(fake.url, secret=True),
                       monkeypatch=monkeypatch, tmp_path=tmp_path)
        out = drive(server, hub)

    block = live_block(out)
    assert block["state"] == "error"
    assert block["error"]["failure"] == "response_unusable"
    assert TOKEN not in json.dumps(out["payload"])
    assert TOKEN not in json.dumps(_samples(dsn), default=str)
    records = _okg_side_records(caplog.records)
    # The SDK logged the reply that carried the token, at DEBUG; the
    # redaction filter is what keeps it out of that line.
    assert any("<redacted>" in r.getMessage() for r in records)
    for record in records:
        assert TOKEN not in record.getMessage() + repr(record.__dict__), record.name
    assert repr(server._okg_live_relation_runners).find(TOKEN) == -1


# ── credentials, at server build ───────────────────────────────────────

@pytest.mark.postgres
@pytest.mark.parametrize("lifecycle", ["prototype", "demo"])
def test_a_personal_credential_builds_on_prototype_and_demo(
    graph, monkeypatch, tmp_path, lifecycle,
):
    dsn, _hub = graph
    server = build(dsn, mcp_binding(URL, credential="personal"),
                   monkeypatch=monkeypatch, tmp_path=tmp_path,
                   lifecycle=lifecycle)
    assert "mcp_hits" in server._okg_live_relation_runners


@pytest.mark.postgres
@pytest.mark.parametrize("lifecycle", ["production", "staging", "archived", None])
def test_a_personal_credential_refuses_the_build_elsewhere(
    graph, monkeypatch, tmp_path, lifecycle,
):
    from okg.substrate.external_backed.mcp_adapter import (
        LiveAdapterPersonalCredentialRefused,
    )

    dsn, _hub = graph
    with pytest.raises(LiveAdapterPersonalCredentialRefused) as caught:
        build(dsn, mcp_binding(URL, credential="personal"),
              monkeypatch=monkeypatch, tmp_path=tmp_path, lifecycle=lifecycle)
    assert caught.value.data["relation"] == "mcp_hits"
    assert caught.value.data["lifecycle"] == lifecycle


@pytest.mark.postgres
def test_a_service_credential_builds_under_production(graph, monkeypatch,
                                                      tmp_path):
    dsn, _hub = graph
    server = build(dsn, mcp_binding(URL, credential="service"),
                   monkeypatch=monkeypatch, tmp_path=tmp_path,
                   lifecycle="production")
    assert "mcp_hits" in server._okg_live_relation_runners


@pytest.mark.postgres
def test_a_missing_credential_refuses_the_build(graph, monkeypatch, tmp_path):
    from okg.substrate.external_backed.live_relations import LiveRelationInvalid

    dsn, _hub = graph
    binding = mcp_binding(URL)
    del binding["mcp_hits"]["upstream_credential"]
    with pytest.raises(LiveRelationInvalid) as caught:
        build(dsn, binding, monkeypatch=monkeypatch, tmp_path=tmp_path,
              lifecycle="prototype")
    assert "upstream_credential" in str(caught.value)


@pytest.mark.postgres
def test_the_credential_rule_holds_even_for_a_caller_supplied_runner(
    graph, monkeypatch, tmp_path,
):
    from okg.substrate.external_backed.mcp_adapter import (
        LiveAdapterPersonalCredentialRefused,
    )

    dsn, _hub = graph
    with pytest.raises(LiveAdapterPersonalCredentialRefused):
        build(dsn, mcp_binding(URL, credential="personal"),
              monkeypatch=monkeypatch, tmp_path=tmp_path,
              adapters={"mcp": lambda **_k: []}, lifecycle="production")


# ── bounds: the #2141 bound, end to end ────────────────────────────────

BUDGET = 32_768


@pytest.mark.postgres
def test_20_mb_of_rows_is_served_within_budget_with_an_exact_digest(
    graph, monkeypatch, tmp_path,
):
    dsn, hub = graph
    rows = sized_rows(20 * MIB)
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", structured(rows))
        server = build(dsn, mcp_binding(fake.url, max_response_bytes=32 * MIB),
                       monkeypatch=monkeypatch, tmp_path=tmp_path)
        out = drive(server, hub, max_bytes=BUDGET)
        text_out = drive(server, hub, max_bytes=BUDGET, text=True)

    assert out["served"] <= BUDGET, out["served"]
    block = live_block(out)
    assert block["state"] == "ok", block
    assert block["truncated"] is True
    assert block["truncation_reason"] == "response_byte_budget"
    assert 0 <= block["rows_returned"] < len(rows)
    assert block["budget_bytes"] <= BUDGET
    assert block["summary"]["row_count"] == len(rows)
    # In upstream order: the prefix is the first rows, unchanged but fenced.
    for index, row in enumerate(block.get("rows") or []):
        assert row["n"] == index
    assert block["digest"] == _independent_digest(rows)

    assert text_out["served"] <= BUDGET, text_out["served"]
    # The text form keeps every row and cuts each one to fit (#3666, D4); it
    # prints no digest or truncation internals.
    status = text_out["text"].splitlines()[0]
    assert f"{len(rows):,} rows, all shown" in status, status
    assert status.endswith("to fit"), status

    samples = _samples(dsn)
    assert len(samples) == 2
    assert samples[0]["attrs"]["full_set_digest"] == block["digest"]


@pytest.mark.postgres
def test_60_mb_under_the_default_cap_is_refused_within_budget_naming_the_args(
    graph, monkeypatch, tmp_path,
):
    dsn, hub = graph
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", structured(sized_rows(60 * MIB)))
        server = build(dsn, mcp_binding(fake.url), monkeypatch=monkeypatch,
                       tmp_path=tmp_path)
        out = drive(server, hub, max_bytes=BUDGET)
        text_out = drive(server, hub, max_bytes=BUDGET, text=True)

    assert out["served"] <= BUDGET
    block = live_block(out)
    assert block["state"] == "error"
    error = block["error"]
    assert error["failure"] == "response_over_cap"
    assert error["narrow_with"] == ["limit", "name"]
    assert "narrower" in error["remedy"]
    assert len(json.dumps(error)) <= 1024
    assert text_out["served"] <= BUDGET
    assert "response_over_cap" in text_out["text"]
    assert _samples(dsn)[0]["status"] == "error"


@pytest.mark.postgres
def test_a_2_mb_is_error_text_is_served_as_a_bounded_excerpt(
    graph, monkeypatch, tmp_path,
):
    dsn, hub = graph
    text = "backend: query failed " + "q" * (2 * MIB)
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", text_blocks(text, is_error=True))
        server = build(dsn, mcp_binding(fake.url), monkeypatch=monkeypatch,
                       tmp_path=tmp_path)
        out = drive(server, hub, max_bytes=BUDGET)
        text_out = drive(server, hub, max_bytes=BUDGET, text=True)

    block = live_block(out)
    assert block["state"] == "error"
    assert block["error"]["failure"] == "upstream_unavailable"
    assert "backend: query failed" in block["error"]["excerpt"]
    assert len(json.dumps(block["error"])) <= 1024
    assert out["served"] <= BUDGET
    assert text_out["served"] <= BUDGET


@pytest.mark.postgres
@pytest.mark.parametrize(
    "text, words",
    [
        pytest.param(ITEM_NOT_FOUND, "Item not found", id="R1"),
        pytest.param(ITEM_RECORDS_NOT_FOUND, "Item not found",
                     id="R2"),
        pytest.param(RECORD_NOT_FOUND,
                     "No record with the id 3fe781fc37294eafb22c0c9eb9a584d6",
                     id="R4"),
        pytest.param(RECORD_NOT_FOUND.replace(
            "3fe781fc37294eafb22c0c9eb9a584d6",
            "c128171c36b343fb88dc9fa965b5a1f2"),
            "No record with the id c128171c36b343fb88dc9fa965b5a1f2", id="R5"),
    ],
)
def test_the_reply_a_model_reads_says_not_found_not_an_outage(
    graph, monkeypatch, tmp_path, text, words,
):
    """Replay of the recorded replies through `expand(live=...)`,
    in the plain-text form a model reads. Before: `failure:
    upstream_unavailable` and "refused the connection or returned an error
    status" above the not-found excerpt. Then `upstream_not_found` as an
    error. Now (PACT live-nothing-answers, lane ri review M2): "not found" is
    the system's answer, 0 rows with its own words, and the call is `ok`."""
    dsn, hub = graph
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", text_blocks(text, is_error=True))
        server = build(dsn, mcp_binding(fake.url), monkeypatch=monkeypatch,
                       tmp_path=tmp_path)
        reply = drive(server, hub, text=True)["text"]

    assert reply.startswith("expand ok "), reply
    assert "found nothing named so" in reply, reply
    assert words in reply
    assert "refused the connection" not in reply
    assert "upstream_unavailable" not in reply
    rows = _samples(dsn)
    assert len(rows) == 1 and rows[0]["status"] == "ok"


@pytest.mark.postgres
@pytest.mark.parametrize(
    "text, label",
    [
        pytest.param(UNIT_EXPRESSION_INVALID, "upstream_rejected_arguments",
                     id="syntax-rejected"),
        pytest.param(UNIT_EXPRESSION_EMPTY, "upstream_empty_result",
                     id="empty-set-stays-empty"),
    ],
)
def test_the_reply_a_model_reads_names_a_rejected_argument(
    graph, monkeypatch, tmp_path, text, label,
):
    """Replay of recorded unit-expression replies through `expand(live=...)`
    in the text a model reads. Before: the syntax error read `failure:
    upstream_unavailable` "refused the connection". Now (PACT
    live-nothing-answers): a rejected argument is a failed live read, named
    first on the status line; an empty set is the system's answer, 0 rows."""
    dsn, hub = graph
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", text_blocks(text, is_error=True))
        server = build(dsn, mcp_binding(fake.url), monkeypatch=monkeypatch,
                       tmp_path=tmp_path)
        reply = drive(server, hub, text=True)["text"]

    assert "refused the connection" not in reply
    rows = _samples(dsn)
    if label == "upstream_rejected_arguments":
        assert reply.startswith(f"expand error {label} "), reply
        assert "rejected these arguments as invalid" in reply
        assert "not an outage" in reply
        assert "does not comply to unit expression syntax" in reply
        assert "matched nothing" not in reply
        assert len(rows) == 1 and rows[0]["status"] == "error"
    else:
        assert reply.startswith("expand ok "), reply
        assert "matched nothing" in reply
        assert "resulted in an empty set" in reply
        assert "rejected these arguments" not in reply
        assert len(rows) == 1 and rows[0]["status"] == "ok"
    assert "upstream_unavailable" not in reply


@pytest.mark.postgres
def test_a_reply_the_tool_cut_says_so_and_the_raised_limit_call_runs(
    graph, monkeypatch, tmp_path,
):
    """Replay of R7's shape: the tool sends `limit` rows and `truncated:
    true`. Before: "3 rows, all shown". Now the status line gives no total,
    the cut line says where the tool stopped and the call that asks for
    more -- and that call, made as written, is served whole."""
    from mcp import types

    def listing_like(args):
        limit = int(args["limit"])
        rows = [{"unit": f"U2_X_{i}"} for i in range(min(limit, 7))]
        return types.CallToolResult(
            content=[], structuredContent=_listing_reply(rows, limit < 7),
        )

    dsn, hub = graph
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", listing_like)
        server = build(dsn, mcp_binding(fake.url, rows_path="items"),
                       monkeypatch=monkeypatch, tmp_path=tmp_path)
        cut = drive(server, hub, args={"name": "x", "limit": 3}, text=True)["text"]
        block = live_block(drive(server, hub, args={"name": "x", "limit": 3}))
        # The call the cut line names: the same arguments, limit 50.
        whole = drive(server, hub, args={"name": "x", "limit": 50}, text=True)["text"]

    assert "all shown" not in cut, cut
    assert "rows 1-3 of count unknown" in cut
    assert "the external system cut its answer at 3 rows" in cut
    assert "ask again with limit up to 50 (this call had 3)" in cut
    assert block["truncated"] is True
    assert block["upstream_cut"] == {"rows": 3, "limit": 3, "limit_max": 50}
    assert "7 rows, all shown" in whole, whole
    assert "cut its answer" not in whole
    assert fake.counters.arguments[-1]["limit"] == 50
    samples = _samples(dsn)
    assert [s["attrs"].get("upstream_cut_rows") for s in samples] == [3, 3, None]


@pytest.mark.postgres
def test_an_explicit_budget_over_the_ceiling_is_refused_not_clamped(
    graph, monkeypatch, tmp_path,
):
    dsn, hub = graph
    max_bytes = 10 * MIB
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", structured([{"a": 1}]))
        server = build(dsn, mcp_binding(fake.url), monkeypatch=monkeypatch,
                       tmp_path=tmp_path)
        out = drive(server, hub, max_bytes=max_bytes)

    error = out["payload"].get("error") or {}
    assert error.get("code") == "invalid_response_byte_budget", out["payload"]
    assert error.get("max_bytes") == max_bytes
    assert fake.counters.requests == 0, "refused before anything was dialled"


@pytest.mark.postgres
def test_an_explicit_budget_under_the_floor_is_raised_and_says_so(
    graph, monkeypatch, tmp_path,
):
    """Every read tool raises a `max_bytes` under 4 KiB to 4 KiB and
    discloses it in `max_bytes_raised`, rather than refusing; a live call is
    served under that disclosed budget, never a silent one."""
    from okg.substrate.mcp.response_bounds import MIN_RESPONSE_BYTES

    dsn, hub = graph
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", structured([{"a": 1}]))
        server = build(dsn, mcp_binding(fake.url), monkeypatch=monkeypatch,
                       tmp_path=tmp_path)
        out = drive(server, hub, max_bytes=1024)

    payload = out["payload"]
    assert payload.get("max_bytes_raised") == {
        "requested": 1024, "applied": MIN_RESPONSE_BYTES,
    }, payload
    assert out["served"] <= MIN_RESPONSE_BYTES, out["served"]
    assert live_block(out)["state"] == "ok"


# ═════════════════════ the live-mcp-example fixture ═════════════════════
#
# tests/substrate/e2e/fixtures/live-mcp-example is a test-only deployment
# with neutral names (req.mcp.real-upstream). Here it is checked without a
# network, then staged against the fake server and walked through
# build_server. The helpers below take the fixture directory and deployment
# name as keyword arguments, so a site that keeps its own copy of the tree
# (for a hand-run proof against real servers) can drive the same code.

REPO_ROOT = Path(__file__).resolve().parents[3]
EXAMPLE_FIXTURE = REPO_ROOT / "tests/substrate/e2e/fixtures/live-mcp-example"
EXAMPLE_NAME = "live-mcp-example"
#: The variable the fixture's `secret_refs` names. It holds the whole header
#: value; the tests set it in-process and never write it anywhere.
EXAMPLE_AUTH_ENV = "OKG_MCP_EXAMPLE_GATE_AUTH"
#: Which subtype carries which relation, as the fixture declares them.
EXAMPLE_RELATIONS = {
    "catalog_instance": ("item", "items"),
    "store_instance": ("locations",),
}
#: Subtype -> the node id published for it.
EXAMPLE_NODES = {
    "catalog_instance": "catalog_instance:main",
    "store_instance": "store_instance:main",
}


def masked_dsn(raw: str):
    """`raw` parsed as a `Dsn`: `repr` and `str` mask the password, and
    `.render()` gives the real string back to whoever needs to connect."""
    from okg.substrate.db.dsn import parse

    return parse(raw)


def example_declarations() -> dict[str, Any]:
    """Every relation the fixture's schema declares, compiled, by name."""
    import yaml

    from okg.substrate.external_backed.live_relations import (
        ANNOTATION_KEY,
        parse_live_relations,
    )

    doc = yaml.safe_load(
        (EXAMPLE_FIXTURE / "schemas/example_live_systems.yaml").read_text(encoding="utf-8"),
    )
    declared: dict[str, Any] = {}
    for class_name, subtype in (("CatalogInstance", "catalog_instance"),
                                ("StoreInstance", "store_instance")):
        annotations = doc["classes"][class_name]["annotations"]
        declared.update(parse_live_relations(
            {ANNOTATION_KEY: annotations[ANNOTATION_KEY]}, subtype=subtype,
        ))
    return declared


def example_manifest() -> dict[str, Any]:
    import yaml

    return yaml.safe_load(
        (EXAMPLE_FIXTURE / "deployment.yaml").read_text(encoding="utf-8"),
    )


def stage_example_fixture(
    root: Path,
    *,
    urls: dict[str, str] | None = None,
    fixture: Path = EXAMPLE_FIXTURE,
    name: str = EXAMPLE_NAME,
) -> Path:
    """Copy `fixture` under `root/<name>`, repointing bindings in `urls`.

    `urls` maps a relation to the server URL it should dial instead of the
    committed one, and the allowlist follows it. Nothing else is changed:
    tool, arguments, rows path, caps and credential are used as committed.
    """
    import shutil

    import yaml

    target = root / name
    shutil.copytree(fixture, target)
    if urls:
        manifest = yaml.safe_load(
            (fixture / "deployment.yaml").read_text(encoding="utf-8"),
        )
        bindings = manifest["agent_surface"]["live_bindings"]
        for relation, url in urls.items():
            config = bindings[relation]["config"]
            assert config["server_url"].startswith("http://127.0.0.1:"), relation
            config["server_url"] = url
            config["allowed_hosts"] = [url.split("//", 1)[1].split("/", 1)[0]]
        (target / "deployment.yaml").write_text(
            yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8",
        )
    return target


def publish_example_systems(
    dsn: str,
    *,
    fixture: Path = EXAMPLE_FIXTURE,
    name: str = EXAMPLE_NAME,
    nodes: dict[str, str] | None = None,
) -> dict[str, str]:
    """One published node of each system subtype; returns subtype -> node id.

    The catalog rows carry the metadata the COMPOSER produces from the
    fixture's own schema, so `live_relations` is the compiled declaration,
    not a hand-written copy.
    """
    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.publisher import Publisher
    from tests.pytest.catalog_fixture_builders import (
        SubtypeSpec,
        bind_test_catalog_writer,
        seed_catalog,
    )

    composed = compose_catalog(fixture)
    nodes = dict(nodes or EXAMPLE_NODES)
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=[
            SubtypeSpec(subtype, metadata=dict(composed.subtype_metadata[subtype]))
            for subtype in nodes
        ])
        ChronosGraphStore.from_connection(conn).apply_facts([
            NodeFact(node_id, subtype, {"instance_name": node_id.split(":")[1]},
                     {"id": node_id}, {"v": 1})
            for subtype, node_id in nodes.items()
        ])
    Publisher(
        dsn, name, catalog_identity=bind_test_catalog_writer(dsn, name),
    ).cycle()
    return nodes


def build_example_server(
    dsn: str, deployments_root: Path, monkeypatch, *, name: str = EXAMPLE_NAME,
):
    """`build_server` on the staged fixture, with its REAL policy bundle."""
    from okg.substrate.mcp.server import build_server

    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(deployments_root))
    monkeypatch.setenv("OKG_DSN", dsn)
    server = build_server(dsn=dsn, deployment_name=name)
    _BUILT.append(server)
    return server


def deny_every_live_call(
    server, empty_root: Path, *, name: str = EXAMPLE_NAME,
) -> None:
    """Point the server's gate at a deployments root with no bundle."""
    from okg.substrate.external_backed.live_dispatch import build_live_gate

    (empty_root / name).mkdir(parents=True, exist_ok=True)
    server._okg_live_gate = build_live_gate(name, deployments_root=empty_root)


def example_samples(dsn: str) -> list[dict[str, Any]]:
    """Every sample the fixture's relations wrote (all are `hot`)."""
    from okg.substrate.external_backed.call_sample import table_for_class

    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT relation, status, decision, policy, attrs "
            f"FROM {table_for_class('hot')} ORDER BY observed_at",
        )
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def test_the_example_fixture_composes_two_system_subtypes_with_their_relations():
    from okg.substrate.catalog.modules_compose import compose_catalog

    composed = compose_catalog(EXAMPLE_FIXTURE)
    for subtype, relations in EXAMPLE_RELATIONS.items():
        assert subtype in composed.subtypes
        live = composed.subtype_metadata[subtype]["live_relations"]
        assert sorted(live) == sorted(relations), live


def test_the_example_fixture_binds_every_declared_relation_and_nothing_else():
    from okg.substrate.external_backed.live_relations import parse_live_bindings

    bindings = parse_live_bindings(
        example_manifest()["agent_surface"]["live_bindings"],
    )
    declared = example_declarations()
    assert sorted(bindings) == sorted(declared) == ["item", "items", "locations"]
    for name, binding in bindings.items():
        assert binding.kind == "mcp", name
        assert binding.upstream_credential == "personal", name
        assert binding.secret_refs == (f"env:{EXAMPLE_AUTH_ENV}",), name
        assert binding.config["server_url"].startswith("http://127.0.0.1:"), name


def test_the_example_bindings_pass_every_load_check(monkeypatch):
    from okg.substrate.external_backed.live_relations import parse_live_bindings
    from okg.substrate.external_backed.mcp_adapter import (
        DEFAULT_MAX_RESPONSE_BYTES,
        LiveAdapterPersonalCredentialRefused,
        assert_mcp_credentials_admitted,
        parse_mcp_binding,
        validate_mcp_binding_configs,
    )

    monkeypatch.setenv(EXAMPLE_AUTH_ENV, f"Bearer {TOKEN}")
    bindings = parse_live_bindings(
        example_manifest()["agent_surface"]["live_bindings"],
    )
    declared = example_declarations()
    runners = validate_mcp_binding_configs(bindings, declared, runners={})
    assert sorted(runners) == ["item", "items", "locations"]

    wide = parse_mcp_binding("items", bindings["items"], declared["items"])
    # The wide question is bounded by the DEFAULT cap, which is the bound
    # a 60 MB answer must meet.
    assert wide.max_response_bytes == DEFAULT_MAX_RESPONSE_BYTES
    assert wide.tool == "catalog_list_items"
    assert wide.narrowing_args == ("detail", "name_pattern")

    lifecycle = example_manifest()["contract"]["lifecycle"]
    assert lifecycle == "prototype"
    assert_mcp_credentials_admitted(bindings, lifecycle=lifecycle)
    with pytest.raises(LiveAdapterPersonalCredentialRefused):
        assert_mcp_credentials_admitted(bindings, lifecycle="production")


def test_the_example_bundle_allows_each_relation_only_on_its_own_subtype():
    import dataclasses

    from okg.substrate.external_backed.live_dispatch import build_live_gate

    gate = build_live_gate(EXAMPLE_NAME, deployments_root=EXAMPLE_FIXTURE.parent)
    declared = example_declarations()
    for name, relation in declared.items():
        outcome = gate(relation, "service:okg-runtime", f"{relation.subtype}:x")
        assert outcome.allowed, (name, outcome)
        assert not gate(relation, "service:someone-else", "n").allowed, name
    # The same relation name on the other subtype matches no rule.
    moved = dataclasses.replace(declared["locations"], subtype="catalog_instance")
    assert not gate(moved, "service:okg-runtime", "catalog_instance:x").allowed


@pytest.fixture
def example_graph(clean_runtime: str, monkeypatch, tmp_path):
    """The staged fixture's graph, published, with the gate token set."""
    monkeypatch.setenv(EXAMPLE_AUTH_ENV, f"Bearer {TOKEN}")
    return clean_runtime, publish_example_systems(clean_runtime)


@pytest.mark.postgres
def test_the_example_fixture_walk_against_the_fake_server(
    example_graph, monkeypatch, tmp_path,
):
    """The proof's steps, against the fake: a narrow store call
    served whole, the wide catalog call refused within budget, a deny that never
    reaches the server, and one hot sample per call."""
    dsn, nodes = example_graph
    locations = [{"location": f"SITE_{i}", "noise": "x"} for i in range(5)]
    with FakeMcpUpstream(token=TOKEN) as fake:
        fake.tool("store_list_locations", structured(locations, path=("items",)))
        fake.tool("catalog_list_items", json_text(sized_rows(60 * MIB)))
        staged = stage_example_fixture(tmp_path / "deployments", urls={
            name: fake.url for name in ("item", "items", "locations")
        })
        server = build_example_server(dsn, staged.parent, monkeypatch)

        narrow = drive(server, nodes["store_instance"], relation="locations",
                       args={"expression": "zone=1"}, max_bytes=BUDGET)
        wide = drive(server, nodes["catalog_instance"], relation="items",
                     args={"name_pattern": "*big*", "detail": True},
                     max_bytes=BUDGET)
        calls_before_deny = list(fake.counters.calls)
        requests_before_deny = fake.counters.requests
        deny_every_live_call(server, tmp_path / "no-bundle")
        denied = drive(server, nodes["catalog_instance"], relation="items",
                       args={"name_pattern": "*big*", "detail": True},
                       max_bytes=BUDGET)

    block = live_block(narrow)
    assert block["state"] == "ok", block
    assert block.get("truncated") is not True, block
    assert len(block["rows"]) == len(locations)
    assert all(set(row) == {"location"} for row in block["rows"]), "row_fields kept"
    assert fake.counters.arguments[0] == {"expression": "zone=1", "limit": 50}
    assert fake.counters.authorization[0] == f"Bearer {TOKEN}"

    assert wide["served"] <= BUDGET
    error = live_block(wide)["error"]
    assert error["failure"] == "response_over_cap", error
    assert error["narrow_with"] == ["detail", "name_pattern"]

    assert live_block(denied)["state"] == "denied"
    assert fake.counters.calls == calls_before_deny
    assert fake.counters.requests == requests_before_deny

    samples = example_samples(dsn)
    assert [(s["relation"], s["decision"]) for s in samples] == [
        ("locations", "allow"), ("items", "allow"), ("items", "deny"),
    ]
    assert samples[0]["status"] == "ok"
    assert samples[1]["status"] == "error"
    assert samples[2]["attrs"]["adapter_ran"] is False


@pytest.mark.postgres
def test_the_example_narrow_catalog_call_returns_its_one_row_whole(
    example_graph, monkeypatch, tmp_path,
):
    """req.mcp.real-upstream (b) against the fake, in the shape a real MCP
    server sends: one item is one text block holding one object. The
    pattern relation sends the operator's detail=false with it."""
    dsn, nodes = example_graph
    name = "/Alpha0/Run1-v1/RAW"

    def one_item(args):
        from mcp import types

        assert args == {"path": name, "detail": False}
        return types.CallToolResult(content=[types.TextContent(
            type="text", text=json.dumps({"item": name, "size": 7}),
        )])

    with FakeMcpUpstream(token=TOKEN) as fake:
        fake.tool("catalog_list_items", one_item)
        staged = stage_example_fixture(tmp_path / "deployments", urls={
            relation: fake.url for relation in ("item", "items", "locations")
        })
        server = build_example_server(dsn, staged.parent, monkeypatch)
        out = drive(server, nodes["catalog_instance"], relation="item",
                    args={"path": name}, max_bytes=BUDGET)

    block = live_block(out)
    assert block["state"] == "ok", block
    assert block.get("truncated") is not True, block
    assert len(block["rows"]) == 1
    assert name in json.dumps(block["rows"])
    assert [(s["relation"], s["status"]) for s in example_samples(dsn)] == [
        ("item", "ok"),
    ]


@pytest.mark.postgres
def test_many_one_object_blocks_are_bounded_like_any_rows(
    example_graph, monkeypatch, tmp_path,
):
    """A wide catalog answer under the cap, one block per item: served as the
    longest prefix that fits plus a digest of the full set."""
    dsn, nodes = example_graph
    rows = [{"item": f"/P{i}/Q/RAW", "pad": "y" * 400} for i in range(300)]

    with FakeMcpUpstream(token=TOKEN) as fake:
        fake.tool("catalog_list_items",
                  text_blocks(*(json.dumps(row) for row in rows)))
        staged = stage_example_fixture(tmp_path / "deployments", urls={
            relation: fake.url for relation in ("item", "items", "locations")
        })
        server = build_example_server(dsn, staged.parent, monkeypatch)
        out = drive(server, nodes["catalog_instance"], relation="items",
                    args={"name_pattern": "P*"}, max_bytes=BUDGET)

    assert out["served"] <= BUDGET
    block = live_block(out)
    assert block["state"] == "ok", block
    assert block["truncated"] is True
    assert block["summary"]["row_count"] == len(rows)
    assert block["digest"] == _independent_digest(rows, relation="items")
    for index, row in enumerate(block.get("rows") or []):
        assert f"/P{index}/Q/RAW" in json.dumps(row)


# ═════════════════ adversarial review of 5c6b325eef ═════════════════════
#
# Each row below fails against that commit. They need no database.

def _session_closing_with(reply_for_delete):
    """A fake whose tools/call is ordinary and whose DELETE is `reply`."""
    def raw(method, _rpc):
        return reply_for_delete if method == "DELETE" else None
    return raw


def test_a_compressed_reply_to_the_closing_delete_is_refused():
    """H1. The SDK reads the DELETE reply whole, and httpx would inflate it
    without a bound: a few kilobytes of gzip become 64 MiB in memory."""
    import gzip

    bomb = gzip.compress(b"0" * (64 * MIB))
    reply = (200, [(b"content-type", b"application/json"),
                   (b"content-encoding", b"gzip")], bomb)
    with FakeMcpUpstream(raw=_session_closing_with(reply)) as fake:
        fake.tool("datasets", structured([{"a": 1}]))
        error = _failure(_runner(mcp_config(fake.url)))

    assert len(bomb) < MIB
    assert fake.counters.http_requests.count("DELETE") == 1
    assert error.failure == "response_unusable", error.excerpt
    assert "compressed" in error.excerpt


def test_a_redirected_closing_delete_is_refused_and_not_followed(monkeypatch):
    """H1. A same-origin 307 on the DELETE would be followed with the bearer
    header on it; it is refused, and nothing is sent to the target."""
    reply = (307, [(b"location", b"/mcp-elsewhere")], b"")
    with FakeMcpUpstream(token=TOKEN, raw=_session_closing_with(reply)) as fake:
        fake.tool("datasets", structured([{"a": 1}]))
        error = _failure(_runner(mcp_config(fake.url), secret=True,
                                 monkeypatch=monkeypatch))

    assert error.failure == "redirect_refused"
    assert error.status == 307
    assert fake.counters.http_requests.count("DELETE") == 1, (
        fake.counters.http_requests
    )


def test_a_json_rpc_error_echoing_the_token_is_refused_and_not_repeated(
    monkeypatch,
):
    """H2. A JSON-RPC error message is upstream text, and it is scanned for
    the credential like a tool result."""
    from tests.substrate.external_backed.fake_mcp_upstream import sse

    def raw(method, rpc):
        if method == "POST" and rpc and rpc.get("method") == "tools/call":
            return sse({"jsonrpc": "2.0", "id": rpc["id"], "error": {
                "code": -32602, "message": f"refused header Bearer {TOKEN}",
            }})
        return None

    with FakeMcpUpstream(token=TOKEN, raw=raw) as fake:
        error = _failure(_runner(mcp_config(fake.url), secret=True,
                                 monkeypatch=monkeypatch))

    assert error.failure == "response_unusable", error.excerpt
    assert "credential" in error.excerpt
    assert TOKEN not in json.dumps(
        {"excerpt": error.excerpt, "message": str(error)},
    )


def test_too_many_content_blocks_are_refused_before_any_is_parsed(monkeypatch):
    """M1. Tiny blocks under the byte cap still make one object each; more
    blocks than any answer may have rows is refused unread."""
    from mcp.types import CallToolResult

    from okg.substrate.external_backed import mcp_adapter
    from okg.substrate.external_backed.http_adapter import MAX_MAX_ROWS

    parsed: list[int] = []
    dumped: list[int] = []
    real_parse = mcp_adapter._parse_text_block
    real_dump = CallToolResult.model_dump_json

    def counting_parse(text):
        parsed.append(1)
        return real_parse(text)

    def counting_dump(self, *args, **kwargs):
        dumped.append(1)
        return real_dump(self, *args, **kwargs)

    monkeypatch.setattr(mcp_adapter, "_parse_text_block", counting_parse)
    monkeypatch.setattr(CallToolResult, "model_dump_json", counting_dump)
    blocks = ["{}"] * (MAX_MAX_ROWS + 1)
    with FakeMcpUpstream(token=TOKEN) as fake:
        fake.tool("datasets", text_blocks(*blocks))
        error = _failure(_runner(
            mcp_config(fake.url, rows_path=".", max_response_bytes=32 * MIB),
            secret=True, monkeypatch=monkeypatch,
        ), deadline=60.0)

    assert error.failure == "result_unusable", error.excerpt
    assert f"{MAX_MAX_ROWS + 1} content blocks" in error.excerpt
    assert parsed == [] and dumped == [], "refused before any per-block work"


def test_max_rows_stops_parsing_one_block_per_row_replies(monkeypatch):
    """M1. With one row per block, the blocks past `max_rows` are not parsed."""
    from okg.substrate.external_backed import mcp_adapter

    parsed: list[int] = []
    real_parse = mcp_adapter._parse_text_block

    def counting_parse(text):
        parsed.append(1)
        return real_parse(text)

    monkeypatch.setattr(mcp_adapter, "_parse_text_block", counting_parse)
    blocks = [json.dumps({"n": i}) for i in range(50)]
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", text_blocks(*blocks))
        got = _call(_runner(mcp_config(fake.url, rows_path=".", max_rows=5)))

    assert got == [{"n": i} for i in range(5)]
    assert len(parsed) == 5


def test_a_traceback_quoting_the_token_reaches_no_log_record(monkeypatch, caplog):
    """M2. The SDK logs a reply it cannot parse with `logger.exception`, and
    the pydantic error in that traceback quotes the reply -- here, the
    credential the server echoed."""
    from tests.substrate.external_backed.fake_mcp_upstream import sse_text

    def raw(method, rpc):
        if method == "POST" and rpc and rpc.get("method") == "tools/call":
            # `result` must be an object; a string is a validation error
            # whose text quotes the value, and pydantic only shortens values
            # longer than this token.
            return sse_text(json.dumps({
                "jsonrpc": "2.0", "id": rpc["id"], "result": TOKEN,
            }))
        return None

    caplog.set_level(logging.DEBUG)
    with FakeMcpUpstream(token=TOKEN, raw=raw) as fake:
        error = _failure(_runner(mcp_config(fake.url), secret=True,
                                 monkeypatch=monkeypatch), deadline=4.0)

    records = _okg_side_records(caplog.records)
    tracebacks = [r for r in records if r.exc_info or r.exc_text]
    assert tracebacks, "the capture must hold the SDK's traceback record"
    assert any("Error parsing SSE message" in r.getMessage() for r in tracebacks)
    formatter = logging.Formatter()
    for record in records:
        text = (
            formatter.format(record) + repr(record.__dict__)
            + str(record.exc_text or "")
        )
        assert TOKEN not in text, record.name
    assert TOKEN not in str(error.excerpt or "")


def test_an_argument_json_cannot_carry_is_refused_at_load_naming_the_relation():
    """L3. YAML reads an unquoted 2026-10-06 as a date."""
    import datetime

    from okg.substrate.external_backed.http_adapter import (
        LiveAdapterConfigInvalid,
    )

    config = mcp_config(URL)
    config["arguments"] = {**config["arguments"],
                           "since": datetime.date(2026, 10, 6)}
    with pytest.raises(LiveAdapterConfigInvalid) as caught:
        _parse(config)
    assert caught.value.data["relation"] == "mcp_hits"
    assert caught.value.data["field"] == "arguments"
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None


def test_a_compressed_tool_reply_is_refused():
    """L5. Compression on the tools/call reply, not only on the close."""
    import gzip

    def raw(method, rpc):
        if method == "POST" and rpc and rpc.get("method") == "tools/call":
            body = gzip.compress(json.dumps({
                "jsonrpc": "2.0", "id": rpc["id"],
                "result": {"content": [], "structuredContent": {"rows": []}},
            }).encode())
            return 200, [(b"content-type", b"application/json"),
                         (b"content-encoding", b"gzip")], body
        return None

    with FakeMcpUpstream(raw=raw) as fake:
        error = _failure(_runner(mcp_config(fake.url)))

    assert error.failure == "response_unusable"
    assert "compressed" in error.excerpt


@pytest.mark.parametrize("method, params", [
    ("sampling/createMessage", {"messages": [], "maxTokens": 1}),
    ("elicitation/create", {"message": "who are you",
                            "requestedSchema": {"type": "object",
                                                "properties": {}}}),
    ("roots/list", {}),
], ids=["sampling", "elicitation", "roots"])
def test_a_server_initiated_request_is_refused_at_once(method, params):
    """L5. A server that asks the client something instead of answering is
    refused when it asks, not when the deadline runs out."""
    from tests.substrate.external_backed.fake_mcp_upstream import sse

    def raw(http_method, rpc):
        if http_method == "POST" and rpc and rpc.get("method") == "tools/call":
            return sse({"jsonrpc": "2.0", "id": "server-1", "method": method,
                        "params": params})
        return None

    with FakeMcpUpstream(raw=raw) as fake:
        started = time.monotonic()
        error = _failure(_runner(mcp_config(fake.url)), deadline=20.0)
        elapsed = time.monotonic() - started

    assert error.failure == "response_unusable", error.excerpt
    assert method in error.excerpt
    assert elapsed < 10.0, f"took {elapsed:.1f}s: it waited for the deadline"


def test_a_group_of_a_cancellation_and_an_error_leaves_as_a_declared_failure(
    monkeypatch,
):
    """L6. A task group can end with a cancellation BESIDE an error. That
    group is a BaseExceptionGroup, not an Exception, and must still leave
    the adapter as `LiveAdapterError`."""
    import anyio

    from okg.substrate.external_backed import mcp_adapter

    real = mcp_adapter._refusing_session_class()

    class Breaking(real):
        async def initialize(self):  # type: ignore[override]
            raise BaseExceptionGroup("reader and watcher", [
                anyio.get_cancelled_exc_class()(),
                RuntimeError("the reader failed"),
            ])

    monkeypatch.setattr(mcp_adapter, "_refusing_session_class", lambda: Breaking)
    with FakeMcpUpstream() as fake:
        fake.tool("datasets", structured([{"a": 1}]))
        error = _failure(_runner(mcp_config(fake.url)))

    assert error.failure == "upstream_unavailable"
    assert "RuntimeError" in error.excerpt


@pytest.mark.parametrize("raw", [
    "postgresql://okg:fake-proof-pw-4f1c@127.0.0.1:6521/okg_test_lanei",
    "host=127.0.0.1 port=6521 user=okg password=fake-proof-pw-4f1c dbname=okg_t",
], ids=["url", "keyword"])
def test_a_live_proofs_dsn_value_never_prints_its_password(raw):
    """A hand-run proof receives its database as this value, and a failing
    test prints its arguments: the password must not be in either text."""
    value = masked_dsn(raw)

    assert "fake-proof-pw-4f1c" not in repr(value)
    assert "fake-proof-pw-4f1c" not in str(value)
    assert "fake-proof-pw-4f1c" in value.render(), "connecting still works"
