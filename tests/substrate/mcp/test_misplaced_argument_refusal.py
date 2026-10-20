"""A top-level argument placed inside another argument says where it goes (#3066).

In the 09-30 CMS comp-ops run, 5 `inspect` calls put `max_bytes` inside
`target`. The refusal said only "inspect input validation failed at
'target.node.max_bytes'" and "correct the named field(s)", so the agent could
not tell the argument exists one level up. The refusal keeps its code and
data and now names the move. No database: validation runs before the tool
body opens a connection.
"""
from __future__ import annotations

from typing import Any

import anyio
import pytest

pytest.importorskip("mcp")

from okg.substrate.mcp.response_bounds import InvalidOperatorArguments  # noqa: E402
from okg.substrate.mcp.server import build_server  # noqa: E402

# A port nothing listens on: validation runs before any connection. Built
# from pieces so the public-copy export does not read it as a credential.
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


def _refusal(server: Any, name: str, arguments: dict[str, Any]) -> InvalidOperatorArguments:
    with pytest.raises(InvalidOperatorArguments) as refused:
        server._validate_operator_arguments(name, arguments)
    return refused.value


@pytest.mark.parametrize("target", [
    {"kind": "node", "node_id": "n:1", "max_bytes": 6000},
    {"kind": "text", "node_id": "n:1", "max_bytes": 6000},
])
def test_max_bytes_inside_target_names_the_top_level(mcp_server, target) -> None:
    error = _refusal(mcp_server, "inspect", {"target": target})
    assert error.data["validation_kinds"] == ["extra_forbidden"]
    assert error.data["invalid_fields"] == [f"target.{target['kind']}.max_bytes"]
    assert (
        "max_bytes goes at the top level of the inspect call, beside target, "
        "not inside it." in error.message
    )
    assert error.fix_hint == "move max_bytes out of target to the top level of the call"


def test_an_unknown_nested_key_keeps_the_plain_refusal(mcp_server) -> None:
    error = _refusal(
        mcp_server, "inspect", {"target": {"kind": "node", "node_id": "n:1", "nope": 1}},
    )
    assert error.message == "inspect input validation failed at 'target.node.nope'."
    assert error.fix_hint == (
        "correct the named field(s), or re-read inspect's inputSchema in tools/list"
    )


def test_the_served_refusal_carries_the_move(mcp_server) -> None:
    # A nested `max_bytes` alone is lifted out and served with a note (issue
    # #3666); beside a different top-level value it is still refused.
    async def run() -> Any:
        return await mcp_server.call_tool(
            "inspect",
            {"target": {"kind": "node", "node_id": "n:1", "max_bytes": 6000},
             "max_bytes": 8192},
        )

    text = anyio.run(run).content[0].text
    assert "inspect refused invalid_operator_arguments" in text
    assert "retry: move max_bytes out of target to the top level of the call" in text


def test_a_misplaced_key_with_another_error_keeps_both_hints(mcp_server) -> None:
    # The other error is a node_id of the wrong type. (A node target with no
    # node_id is no longer a field error: `path` may name it instead, #3666.)
    error = _refusal(
        mcp_server, "inspect", {"target": {"kind": "node", "node_id": 5, "max_bytes": 6000}},
    )
    assert "max_bytes goes at the top level" in error.message
    assert error.fix_hint == (
        "move max_bytes out of target to the top level of the call, "
        "and correct the other named field(s)"
    )
