"""Every tool result is sent once, as one text block (issue #3059).

PACT `mcp-success-text-3059`. Before this change a successful okg MCP call
returned its result only in `structuredContent` with `content=[]` (issue
#2010), so a client that reads only `content` -- langchain-mcp-adapters, upstream
Open WebUI -- saw nothing, and a refusal the tool RETURNED arrived with
`isError: false` and no text, indistinguishable from an empty answer.

Now every result carries exactly one text block and no structured copy; no
tool publishes an `outputSchema`. The block is the plain-text rendering by
default and the compact JSON of the same dictionary when the request's
`_meta` names `okg/result_format: json`. A payload with `status: "refused"`,
returned or raised, is served with `isError: true` and carries every fix-it
field in its one block.

These tests drive `call_tool` and a real in-memory client session, not
`tool.fn`, because the direct function bypasses the result building
entirely and so proves nothing about the wire.

Hermetic: `OKG_MCP_SURFACE_PROBE=1` builds both pools UNOPENED.
"""
from __future__ import annotations

import json
from typing import Any

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.mcp.operators import OPERATOR_TOOLS
from okg.substrate.mcp.response_bounds import public_call_tool_result_bytes
from okg.substrate.mcp.result_text import (
    RESULT_FORMAT_META_KEY,
    ResultContext,
    StatusLine,
    parse_status_line,
    result_context,
    result_format_scope,
)

pytestmark = pytest.mark.anyio

JSON_META = {RESULT_FORMAT_META_KEY: "json"}

FIXTURE_PAGE = {
    "items": [
        {
            "node_id": f"file:okg:src/okg/substrate/mcp/module_{i:02d}.py",
            "subtype": "source_file",
            "attrs": {"path": f"src/okg/substrate/mcp/module_{i:02d}.py"},
        }
        for i in range(20)
    ],
    "columns": ["node_id", "subtype", "attrs"],
    "total_count": 20,
    "truncated": False,
    "generation_id": "g-2026-09-19",
    "pinned_generation_id": "g-2026-09-19",
    "logical_session_id": None,
}


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture(autouse=True)
def _hermetic_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OKG_MCP_SURFACE_PROBE", "1")
    monkeypatch.setenv("OKG_MCP_TRACE_ENABLED", "0")
    monkeypatch.delenv("OKG_MCP_TOOL_ALLOWLIST", raising=False)
    monkeypatch.delenv("OKG_MCP_ALLOW_WRITES", raising=False)


def _build(monkeypatch: pytest.MonkeyPatch, *, allow_writes: bool = False):
    from okg.substrate.mcp import server as server_mod
    from okg.substrate.mcp.server import build_server
    from okg.substrate.mcp.surface_budget import PROBE_DSN

    monkeypatch.setattr(
        server_mod,
        "_server_dsn_resolution",
        lambda _dsn, _name: {
            "dsn": PROBE_DSN, "authority": {"source": "surface_probe_stub"},
        },
    )
    return build_server(dsn=PROBE_DSN, allow_writes=allow_writes)


@pytest.fixture
def server(monkeypatch: pytest.MonkeyPatch):
    mcp = _build(monkeypatch)
    try:
        yield mcp
    finally:
        mcp._okg_pool.close()


def _one_text(result: Any) -> str:
    """The one text block; fails unless that is all the result carries."""
    assert result.structuredContent is None, (
        f"a result still carries a structured copy: {result.structuredContent!r}"
    )
    assert len(result.content) == 1 and result.content[0].type == "text", (
        f"a result must carry exactly one text block: {result.content!r}"
    )
    text = result.content[0].text
    assert text, "the text block is empty"
    return text


async def _client_call(server: Any, name: str, arguments: dict, *, meta=None):
    from mcp.shared.memory import create_connected_server_and_client_session

    async with create_connected_server_and_client_session(
        server._mcp_server,
    ) as session:
        return await session.call_tool(name, arguments, meta=meta)


def _minimal_arguments(name: str) -> dict:
    return {
        "inspect": {"target": {"kind": "graph"}},
        "search": {"query": "x"},
        "expand": {"input": "n:1"},
        "filter": {"input": {"kind": "nodes"}, "predicate": {}},
        "map": {"input": {"kind": "nodes"}},
        "aggregate": {"input": {"kind": "nodes"}, "by": ["subtype"],
                      "metrics": ["count"]},
        "query": {"sql": "SELECT 1"},
        # `run` refuses an empty step list itself, before any step.
        "run": {"steps": []},
    }[name]


# ── no output schema, on any tool ───────────────────────────────────────

@pytest.mark.parametrize("allow_writes", (False, True))
async def test_no_tool_publishes_an_output_schema(
    monkeypatch: pytest.MonkeyPatch, allow_writes: bool,
) -> None:
    mcp = _build(monkeypatch, allow_writes=allow_writes)
    try:
        listed = await mcp.list_tools()
    finally:
        mcp._okg_pool.close()
    assert listed, "the server listed no tools"
    with_schema = [tool.name for tool in listed if tool.outputSchema is not None]
    assert not with_schema, f"these tools still publish an outputSchema: {with_schema}"
    if allow_writes:
        assert {"record", "propose", "list_sources"} <= {tool.name for tool in listed}


# ── a success: one text block, and JSON on request ──────────────────────

async def test_a_successful_call_sends_one_text_block(
    server, monkeypatch: pytest.MonkeyPatch,
) -> None:
    page = dict(FIXTURE_PAGE)
    monkeypatch.setattr(
        server._tool_manager._tools["filter"], "fn", lambda *a, **k: dict(page),
    )
    result = await _client_call(
        server, "filter", {"input": {"kind": "nodes"}, "predicate": {}},
    )
    text = _one_text(result)
    assert result.isError is False
    assert parse_status_line(text) == StatusLine("filter", "ok")
    assert "1-20 of 20" in text.split("\n", 1)[0]
    for row in page["items"]:
        assert row["node_id"] in text


async def test_json_through_request_meta_reads_back_equal(
    server, monkeypatch: pytest.MonkeyPatch,
) -> None:
    page = dict(FIXTURE_PAGE)
    monkeypatch.setattr(
        server._tool_manager._tools["filter"], "fn", lambda *a, **k: dict(page),
    )
    result = await _client_call(
        server, "filter", {"input": {"kind": "nodes"}, "predicate": {}},
        meta=JSON_META,
    )
    assert json.loads(_one_text(result)) == page
    assert result.isError is False


async def test_an_unknown_result_format_is_refused_not_ignored(server) -> None:
    result = await _client_call(
        server, "inspect", {"target": {"kind": "graph"}},
        meta={RESULT_FORMAT_META_KEY: "yaml"},
    )
    text = _one_text(result)
    assert result.isError is True
    assert parse_status_line(text) == StatusLine("inspect", "refused", "invalid_result_format")
    assert "fix_hint:" in text


async def test_a_non_operator_dictionary_is_one_text_block_too(server) -> None:
    def widget_status() -> dict:
        return {"status": "writes_disabled", "answer": "read-only server"}

    server.tool(name="widget_status", structured_output=False)(widget_status)
    result = await _client_call(server, "widget_status", {})
    text = _one_text(result)
    assert result.isError is False
    assert text.startswith("widget_status status=writes_disabled")
    assert "answer: read-only server" in text


# ── refusals: isError, one block, every fix-it field ────────────────────

@pytest.mark.parametrize("name", sorted(OPERATOR_TOOLS))
async def test_a_refused_call_is_an_error_with_its_code_in_the_text(
    server, name: str,
) -> None:
    """The probe pool is never opened, so these calls refuse."""
    result = await _client_call(server, name, _minimal_arguments(name))
    text = _one_text(result)
    assert result.isError is True
    line = parse_status_line(text)
    assert line is not None and line.tool == name and line.status == "refused", text
    assert line.code, text


async def test_a_returned_refusal_is_an_error_with_every_fix_it_field(
    server, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The heart of #3059: a RETURNED refusal used to go out as a success."""
    refusal = {
        "status": "refused",
        "operator_name": "search",
        "error": {
            "code": "semantic_search_unavailable",
            "message": "search method='semantic' requires an embedder",
            "fix_hint": "retry with method='lexical'",
            "fallback_method": "lexical",
            "fallback_operator": "search",
            "schema_hint": "Use search(query=..., method='lexical')",
            "example": {"query": "x", "method": "lexical"},
            "allowed_operators": ["search"],
        },
        "answer": "Semantic search is not available here.",
        "warning": {"code": "w", "message": "m"},
    }
    monkeypatch.setattr(
        server._tool_manager._tools["search"], "fn", lambda *a, **k: json.loads(json.dumps(refusal)),
    )
    result = await _client_call(server, "search", {"query": "x"})
    text = _one_text(result)
    assert result.isError is True
    assert parse_status_line(text) == StatusLine("search", "refused", "semantic_search_unavailable")
    lines = text.splitlines()
    for key in (
        "answer", "message", "fix_hint", "fallback_method", "fallback_operator",
        "allowed_operators", "example", "schema_hint", "warning",
    ):
        assert any(line.startswith(f"{key}: ") for line in lines), (key, text)
    assert "fix_hint: retry with method='lexical'" in lines

    as_json = await _client_call(server, "search", {"query": "x"}, meta=JSON_META)
    assert as_json.isError is True
    payload = json.loads(_one_text(as_json))
    assert payload["error"] == refusal["error"]


async def test_a_session_hidden_tool_is_still_refused_as_hidden(
    server, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The result is built downstream of the whole dispatch chain, so the
    session projection still refuses a hidden tool before it runs."""
    from okg.substrate.mcp.session_tool_surface import SessionToolSurface

    monkeypatch.setattr(server, "_okg_session_tool_surface", SessionToolSurface(
        base=frozenset({"inspect"}), ceiling=frozenset(OPERATOR_TOOLS),
    ))
    result = await server.call_tool("query", {"sql": "SELECT 1"})
    line = parse_status_line(_one_text(result))
    assert line == StatusLine("query", "refused", "mcp_tool_not_in_session_surface")


#: Shaped like the deployment release-skew error that prompted the cap.
OVERSIZED_ERROR_DATA = {
    "pending_migrations": [
        {"name": f"2026090{index:03d}_add_column", "checksum": "c" * 64}
        for index in range(40)
    ],
    "deployment": "default",
}


async def test_an_oversized_error_data_object_is_capped_and_marked(
    server, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import pydantic_core

    from okg.substrate.mcp.response_bounds import ERROR_DATA_CAP_BYTES

    # Local on purpose: a module-level SubstrateError subclass would be seen
    # by the error-code registry parity test as an unregistered code.
    class _ReleaseSkew(SubstrateError):
        code = "deployment_release_skew"

    def raise_skew(*_args, **_kwargs):
        raise _ReleaseSkew(
            "the deployment is behind its release",
            data=dict(OVERSIZED_ERROR_DATA),
            fix_hint="run okg deployment apply, then retry",
        )

    monkeypatch.setattr(server._tool_manager._tools["query"], "fn", raise_skew)
    with result_format_scope("json"):
        result = await server.call_tool("query", {"sql": "SELECT 1"})
    error = json.loads(_one_text(result))["error"]
    assert error["code"] == "deployment_release_skew"
    assert error["fix_hint"] == "run okg deployment apply, then retry"
    assert error["data"]["truncated"] is True
    assert "pending_migrations" not in error["data"]
    assert len(pydantic_core.to_json(error["data"], fallback=str)) <= ERROR_DATA_CAP_BYTES


async def test_a_returned_oversized_refusal_is_capped_and_an_error(
    server, monkeypatch: pytest.MonkeyPatch,
) -> None:
    refusal = {
        "status": "refused",
        "operator_name": "query",
        "error": {
            "code": "deployment_release_skew",
            "message": "the deployment is behind its release",
            "data": dict(OVERSIZED_ERROR_DATA),
            "fix_hint": "run okg deployment apply, then retry",
        },
    }
    monkeypatch.setattr(
        server._tool_manager._tools["query"], "fn", lambda *_a, **_k: refusal,
    )
    with result_format_scope("json"):
        result = await server.call_tool("query", {"sql": "SELECT 1"})
    assert result.isError is True
    error = json.loads(_one_text(result))["error"]
    assert error["code"] == "deployment_release_skew"
    assert error["data"]["truncated"] is True


async def test_a_returned_refusal_is_fitted_to_the_minimum_budget(
    server, monkeypatch: pytest.MonkeyPatch,
) -> None:
    refusal = {
        "status": "refused",
        "operator_name": "query",
        "error": {
            "code": "run_sql_invalid",
            "message": "m" * 3_000,
            "fix_hint": "h" * 3_000,
            "data": {"details": "d" * 900},
        },
    }
    monkeypatch.setattr(
        server._tool_manager._tools["query"], "fn",
        lambda *_a, **_k: json.loads(json.dumps(refusal)),
    )
    result = await _client_call(server, "query", {"sql": "SELECT 1", "max_bytes": 4_096})
    served = result.model_dump_json(by_alias=True, exclude_none=True)
    assert len(served.encode("utf-8")) <= 4_096
    assert result.isError is True
    assert parse_status_line(_one_text(result)).code == "run_sql_invalid"


def test_the_cap_leaves_a_payload_under_it_identical() -> None:
    from okg.substrate.mcp.response_bounds import bound_error_data

    payload = {
        "status": "refused",
        "error": {"code": "c", "message": "m", "data": {"field": "page_size"}},
    }
    assert bound_error_data(payload) is payload


def test_the_cap_names_the_trace_when_one_is_active() -> None:
    from okg.substrate.mcp import response_bounds

    payload = {
        "status": "refused",
        "error": {"code": "c", "message": "m", "data": dict(OVERSIZED_ERROR_DATA)},
    }
    without = response_bounds.bound_error_data(payload)["error"]["data"]
    assert "trace_id" not in without
    original = response_bounds._active_trace_id
    response_bounds._active_trace_id = lambda: "trace-abc123"
    try:
        with_trace = response_bounds.bound_error_data(payload)["error"]["data"]
    finally:
        response_bounds._active_trace_id = original
    assert with_trace["trace_id"] == "trace-abc123"


async def test_an_unexpected_sdk_shape_becomes_a_refusal(
    server, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An SDK drift is loud, never a quiet passthrough."""
    from okg.substrate.mcp.response_bounds import _RawResultFastMCP

    async def changed_shape(_self, _name, _arguments):
        return ["the SDK stopped returning the raw dictionary"]

    monkeypatch.setattr(_RawResultFastMCP, "call_tool", changed_shape)
    with result_format_scope("json"):
        result = await server.call_tool("filter", _minimal_arguments("filter"))
    assert result.isError is True
    payload = json.loads(_one_text(result))
    assert payload["error"]["code"] == "tool_result_shape_changed"
    assert payload["error"]["data"] == {"tool": "filter", "result_type": "list"}


# ── every error path, over a real client session ───────────────────────


def _raise(exc: BaseException):
    def fn(*_args: Any, **_kwargs: Any) -> Any:
        raise exc
    return fn


class _TypedRefusal(SubstrateError):
    code = "run_sql_invalid"


def _prepare_error_path(server: Any, monkeypatch: pytest.MonkeyPatch, path: str):
    """Arrange one error path; return the (tool, arguments) that reach it."""
    if path == "typed_operator_error":
        monkeypatch.setattr(
            server._tool_manager._tools["query"], "fn", _raise(_TypedRefusal(
                'run_sql plan-time error: column "dst_id" does not exist',
                fix_hint="check selected columns and allowed okg.v_* view names",
            )),
        )
        return "query", {"sql": "select dst_id from okg.v_edges"}
    if path == "untyped_operator_error":
        monkeypatch.setattr(
            server._tool_manager._tools["aggregate"], "fn",
            _raise(RuntimeError("internal-detail must stay server-side")),
        )
        return "aggregate", _minimal_arguments("aggregate")
    if path == "oversized_typed_error":
        monkeypatch.setattr(
            server._tool_manager._tools["query"], "fn", _raise(_TypedRefusal(
                "m" * 5_000,
                data=dict(OVERSIZED_ERROR_DATA),
                fix_hint="h" * 5_000,
            )),
        )
        return "query", {"sql": "SELECT 1", "max_bytes": 4_096}
    if path == "unknown_argument":
        return "search", {"query": "x", "scope": "everything"}
    if path == "invalid_arguments":
        return "search", {"query": ["not", "a", "string"]}
    if path == "hidden_from_session":
        from okg.substrate.mcp.session_tool_surface import SessionToolSurface

        monkeypatch.setattr(server, "_okg_session_tool_surface", SessionToolSurface(
            base=frozenset({"inspect"}), ceiling=frozenset(OPERATOR_TOOLS),
        ))
        return "query", {"sql": "SELECT 1"}
    if path == "schema_behind_write":
        from okg.substrate.mcp.server import read_only_release_from_report

        def widget_write(widget_id: str) -> dict:
            return {"widget_id": widget_id}

        server.tool(name="widget_write", structured_output=False)(widget_write)
        monkeypatch.setattr(server, "_okg_read_only_release", (
            read_only_release_from_report({
                "verdict": "read_only",
                "schema_behind_by": 1,
                "behind_migrations": ["20260901000_add_column"],
                "verdict_message": "read-only; 1 migration behind",
                "fix_hint": "run `okg up default`",
            })
        ))
        return "widget_write", {"widget_id": "w1"}
    if path == "sdk_shape_changed":
        from okg.substrate.mcp.response_bounds import _RawResultFastMCP

        async def changed_shape(_self, _name, _arguments):
            return ["the SDK stopped returning the raw dictionary"]

        monkeypatch.setattr(_RawResultFastMCP, "call_tool", changed_shape)
        return "filter", _minimal_arguments("filter")
    raise AssertionError(path)


ERROR_PATHS = (
    "typed_operator_error",
    "untyped_operator_error",
    "oversized_typed_error",
    "unknown_argument",
    "invalid_arguments",
    "hidden_from_session",
    "schema_behind_write",
    "sdk_shape_changed",
)


@pytest.mark.parametrize("path", ERROR_PATHS)
async def test_every_error_carries_its_code_and_hint_in_its_one_block(
    server, monkeypatch: pytest.MonkeyPatch, path: str,
) -> None:
    name, arguments = _prepare_error_path(server, monkeypatch, path)
    result = await _client_call(server, name, arguments)
    text = _one_text(result)
    assert result.isError is True
    line = parse_status_line(text)
    assert line is not None and line.status == "refused" and line.code, text
    lines = text.splitlines()
    # A value over 200 characters prints as a `--- <key>` block instead of a
    # key line; either way it is in the one block the client reads.
    assert any(line.startswith(("message: ", "--- message")) for line in lines), text
    assert any(
        line.startswith(("fix_hint: ", "remedy: ", "--- fix_hint")) for line in lines
    ), text
    if path == "untyped_operator_error":
        assert line.code == "operator_execution_failed"
        assert "internal-detail" not in text
    if path == "schema_behind_write":
        assert "schema_behind_by: 1" in lines
    if path == "oversized_typed_error":
        served = result.model_dump_json(by_alias=True, exclude_none=True)
        assert len(served.encode("utf-8")) <= 4_096
        assert line.code == "run_sql_invalid"


@pytest.mark.parametrize("path", ("unknown_tool", "non_operator_tool_raises"))
async def test_an_sdk_built_error_is_one_text_block(server, path: str) -> None:
    """A write or admin tool's raised exception stays the SDK's own text,
    which has no status line (the classifiers count it as failed)."""
    name = "no_such_tool"
    if path == "non_operator_tool_raises":
        def widget_write() -> dict:
            raise RuntimeError("widget store unavailable")

        server.tool(name="widget_write", structured_output=False)(widget_write)
        name = "widget_write"
    result = await _client_call(server, name, {})
    text = _one_text(result)
    assert result.isError is True
    assert parse_status_line(text) is None


# ── the measure equals the served bytes ─────────────────────────────────

@pytest.mark.parametrize("fmt", ("text", "json"))
def test_the_budget_measures_exactly_what_is_served(fmt: str) -> None:
    from okg.substrate.mcp.response_bounds import (
        _fit_error_payload,
        served_call_tool_result,
    )

    with result_context(ResultContext(format=fmt, tool="filter")):
        success = served_call_tool_result(FIXTURE_PAGE)
        assert success.structuredContent is None and success.isError is False
        assert public_call_tool_result_bytes(FIXTURE_PAGE) == len(
            success.model_dump_json(by_alias=True, exclude_none=True).encode()
        )
        refusal = {
            "status": "refused",
            "operator_name": "filter",
            "error": {
                "code": "run_sql_invalid",
                "message": "m" * 1_024,
                "fix_hint": "h" * 1_024,
                "data": {"details": "d" * 900},
                "max_bytes": 4_096,
            },
        }
        error = served_call_tool_result(refusal, is_error=True)
        assert error.isError is True
        assert public_call_tool_result_bytes(refusal, is_error=True) == len(
            error.model_dump_json(by_alias=True, exclude_none=True).encode()
        )
        fitted = _fit_error_payload(json.loads(json.dumps(refusal)), budget=4_096)
        assert public_call_tool_result_bytes(fitted, is_error=True) <= 4_096
        assert fitted["error"]["code"] == "run_sql_invalid"


def test_text_is_smaller_than_json_on_a_row_page() -> None:
    with result_context(ResultContext(format="text", tool="filter")):
        text = public_call_tool_result_bytes(FIXTURE_PAGE)
    with result_context(ResultContext(format="json", tool="filter")):
        as_json = public_call_tool_result_bytes(FIXTURE_PAGE)
    assert text < as_json
