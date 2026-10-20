"""A real MCP server process carrying the substrate's session projection.

Issue #1458. Run as a subprocess by `test_tool_list_expansion_e2e.py`:

    python _expansion_e2e_server.py stdio
    python _expansion_e2e_server.py streamable-http <port>

It builds a `SessionScopedFastMCP` and installs a `SessionToolSurface`
through `install_session_tool_projection` — the same production classes and
the same installer `okg.substrate.mcp.server` uses — over two trivial tools
instead of the graph algebra. The graph tools need Postgres, and a database
would make this a test of the graph rather than of the protocol behavior
under test: capability advertisement, per-session visibility, the
`notifications/tools/list_changed` frame, and calling a newly exposed tool.

The filename is underscore-prefixed so pytest does not collect it.
"""
from __future__ import annotations

import sys
from typing import Any

from okg.substrate.mcp.session_tool_surface import (
    SessionScopedFastMCP,
    SessionToolSurface,
    install_session_tool_projection,
)


BASE_TOOLS = frozenset({"inspect"})
CEILING_TOOLS = frozenset({"inspect", "wide_only"})


def build() -> SessionScopedFastMCP:
    mcp = SessionScopedFastMCP("okg-expansion-e2e")
    surface = SessionToolSurface(
        base=BASE_TOOLS,
        ceiling=CEILING_TOOLS,
        base_profile="e2e-base",
        ceiling_profile="e2e-ceiling",
    )

    @mcp.tool(name="inspect", structured_output=False)
    def inspect(target: dict[str, Any] | None = None) -> dict[str, Any]:
        """Read this session's surface, or widen it."""
        target = target or {}
        identity = mcp.okg_current_session()
        if target.get("action") == "expand":
            outcome = surface.expand(identity)
            return {
                "status": outcome.status,
                "served_tools": list(outcome.visible_tools),
                "added_tools": list(outcome.added_tools),
            }
        return {
            "status": "ok",
            "served_tools": sorted(surface.visible_for(identity)),
            "expanded": surface.is_expanded(identity),
        }

    @mcp.tool(name="wide_only", structured_output=False)
    def wide_only() -> dict[str, Any]:
        """Reachable only by a session that has expanded."""
        return {"status": "ok", "tool": "wide_only"}

    install_session_tool_projection(mcp, surface)
    return mcp


def main(argv: list[str]) -> int:
    mcp = build()
    transport = argv[1] if len(argv) > 1 else "stdio"
    if transport == "stdio":
        mcp.run()
        return 0
    mcp.settings.host = "127.0.0.1"
    mcp.settings.port = int(argv[2])
    mcp.settings.log_level = "WARNING"
    mcp.run(transport="streamable-http")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
