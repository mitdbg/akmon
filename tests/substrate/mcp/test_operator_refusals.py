from __future__ import annotations

import pytest

pytest.importorskip("mcp")

from okg.substrate.mcp.server import build_server  # noqa: E402


def test_query_disabled_refusal_does_not_suggest_query(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    allowlist = ("inspect", "search", "expand", "filter", "map", "aggregate")
    monkeypatch.setenv("OKG_MCP_TOOL_ALLOWLIST", ",".join(allowlist))
    monkeypatch.setenv("OKG_MCP_SURFACE_PROBE", "1")
    mcp = build_server(dsn="postgresql://postgres:okg@localhost:1/okg_probe")
    try:
        refused = mcp._tool_manager._tools["filter"].fn(
            input={"kind": "nodes"},
            predicate={"op": "regex", "field": "node_id", "value": "n:.*"},
        )
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()

    assert refused["status"] == "refused"
    error = refused["error"]
    assert error["code"] == "unsupported_operator_form"
    assert "fallback_operator" not in error
    assert error["fallback_suppressed"] is True
    assert "query" not in error["allowed_operators"]
    assert "schema_hint" in error
    assert "query" not in error["fix_hint"].lower()
    assert "regex" in error["message"]
