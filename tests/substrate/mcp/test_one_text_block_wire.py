"""The wire behavior issue #3059 fixes, asserted with modules dev already has.

Review N3 on 58c3678f40: the main contract and renderer tests import the new
`result_text` module, so on origin/dev they fail at import, which proves
only that the module is new. These two tests import nothing #3059 adds. They
drive a real in-memory MCP client session against the hermetic probe build,
so on origin/dev they fail on their ASSERTIONS: a success there carries no
text block, and a returned refusal there is not an error.

Hermetic: `OKG_MCP_SURFACE_PROBE=1` builds both pools UNOPENED.
"""
from __future__ import annotations

import pytest

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
def server(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("OKG_MCP_SURFACE_PROBE", "1")
    monkeypatch.setenv("OKG_MCP_TRACE_ENABLED", "0")
    monkeypatch.delenv("OKG_MCP_TOOL_ALLOWLIST", raising=False)
    monkeypatch.delenv("OKG_MCP_ALLOW_WRITES", raising=False)
    from okg.substrate.mcp import server as server_mod
    from okg.substrate.mcp.surface_budget import PROBE_DSN

    monkeypatch.setattr(
        server_mod,
        "_server_dsn_resolution",
        lambda _dsn, _name: {
            "dsn": PROBE_DSN, "authority": {"source": "surface_probe_stub"},
        },
    )
    mcp = server_mod.build_server(dsn=PROBE_DSN)
    try:
        yield mcp
    finally:
        mcp._okg_pool.close()


async def _call(server, name: str, arguments: dict):
    from mcp.shared.memory import create_connected_server_and_client_session

    async with create_connected_server_and_client_session(
        server._mcp_server,
    ) as session:
        return await session.call_tool(name, arguments)


async def test_a_success_reaches_a_content_only_client_as_text(
    server, monkeypatch: pytest.MonkeyPatch,
) -> None:
    page = {
        "items": [{"node_id": "n:needle-1", "subtype": "t"}],
        "total_count": 1,
        "pinned_generation_id": "g1",
    }
    monkeypatch.setattr(
        server._tool_manager._tools["filter"], "fn", lambda *a, **k: dict(page),
    )
    result = await _call(server, "filter", {"input": {"kind": "nodes"}, "predicate": {}})
    texts = [block.text for block in result.content if block.type == "text"]
    assert result.isError is False
    assert len(result.content) == 1, f"one content block only: {result.content!r}"
    assert len(texts) == 1, f"a success must carry one text block: {result.content!r}"
    assert "n:needle-1" in texts[0]
    assert result.structuredContent is None


async def test_a_returned_refusal_is_an_error_with_its_code_in_the_text(
    server, monkeypatch: pytest.MonkeyPatch,
) -> None:
    refusal = {
        "status": "refused",
        "operator_name": "search",
        "error": {
            "code": "semantic_search_unavailable",
            "message": "search method='semantic' requires an embedder",
            "fix_hint": "retry with method='lexical'",
        },
    }
    monkeypatch.setattr(
        server._tool_manager._tools["search"], "fn", lambda *a, **k: dict(refusal),
    )
    result = await _call(server, "search", {"query": "x"})
    texts = [block.text for block in result.content if block.type == "text"]
    assert result.isError is True, "a returned refusal must be served as an error"
    assert len(result.content) == 1, f"one content block only: {result.content!r}"
    assert len(texts) == 1 and "semantic_search_unavailable" in texts[0]
    assert "method='lexical'" in texts[0]
    assert result.structuredContent is None


async def test_no_tool_publishes_an_output_schema_over_the_wire(server) -> None:
    """tools/list over a real client session: no okg tool advertises an
    outputSchema, so no client validator expects a structured copy."""
    from mcp.shared.memory import create_connected_server_and_client_session

    async with create_connected_server_and_client_session(
        server._mcp_server,
    ) as session:
        listed = await session.list_tools()
    assert listed.tools, "the server listed no tools"
    with_schema = [tool.name for tool in listed.tools if tool.outputSchema is not None]
    assert with_schema == [], with_schema
