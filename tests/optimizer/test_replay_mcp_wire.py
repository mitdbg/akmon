"""The replay runner's real MCP client over streamable HTTP (PACT akb-replay-runner, #4279).

A small FastMCP server runs in a thread on a loopback port. The runner's
``mcp_connect`` must send ``X-OKG-Question`` / ``X-OKG-Try`` on every request
of a try's session, read the served surface, and read the generation from
``inspect(target={kind: graph})``. No database, no model call.
"""
from __future__ import annotations

import asyncio
import contextlib
import socket
import threading
import time
from collections.abc import Iterator

import pytest

pytest.importorskip("uvicorn")

from mcp.server.fastmcp import Context, FastMCP  # noqa: E402

from okg.optimizer.replay import agent as agent_mod  # noqa: E402
from okg.optimizer.replay.serve import mcp_connect, probe_surface, read_generation  # noqa: E402

GEN = "okg:wire:branch:default:gen:20261030T000000000000Z:abcdefabcdef"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@contextlib.contextmanager
def _serve(mcp: FastMCP) -> Iterator[str]:
    import uvicorn

    port = _free_port()
    server = uvicorn.Server(uvicorn.Config(mcp.streamable_http_app(), host="127.0.0.1", port=port,
                                           log_level="error"))
    thread = threading.Thread(target=server.run, name="replay-wire-uvicorn", daemon=True)
    thread.start()
    deadline = time.monotonic() + 20.0
    while not server.started:
        assert thread.is_alive(), "uvicorn exited before it started"
        assert time.monotonic() < deadline, "uvicorn did not start in 20 s"
        time.sleep(0.02)
    try:
        yield f"http://127.0.0.1:{port}/mcp"
    finally:
        server.should_exit = True
        thread.join(timeout=40.0)


def _app(seen: list[tuple[str | None, str | None]]) -> FastMCP:
    mcp = FastMCP("replay-wire", instructions="Wire test instructions.")

    @mcp.tool()
    def inspect(target: dict, ctx: Context) -> str:
        """Report the graph."""
        headers = ctx.request_context.request.headers
        seen.append((headers.get("x-okg-question"), headers.get("x-okg-try")))
        return f"inspect ok · generation {GEN}"

    @mcp.tool()
    def search(query: str, ctx: Context) -> str:
        """Search."""
        headers = ctx.request_context.request.headers
        seen.append((headers.get("x-okg-question"), headers.get("x-okg-try")))
        return "search ok · 0 rows"

    return mcp


def test_real_session_labels_requests_and_reads_surface_and_generation() -> None:
    seen: list[tuple[str | None, str | None]] = []
    with _serve(_app(seen)) as url:
        async def main():
            surface = await probe_surface(mcp_connect, url, {})
            generation = await read_generation(mcp_connect, url, {})
            labelled = {agent_mod.QUESTION_HEADER: "q-7", agent_mod.TRY_HEADER: "3"}
            async with mcp_connect(url, labelled, 30.0) as session:
                text, is_error = await session.call_tool("search", {"query": "x"})
            return surface, generation, text, is_error

        surface, generation, text, is_error = asyncio.run(main())
    assert surface.instructions == "Wire test instructions."
    assert surface.tool_names == ["inspect", "search"]
    assert generation == GEN
    assert text == "search ok · 0 rows" and is_error is False
    assert seen[0] == (None, None)
    assert seen[-1] == ("q-7", "3")
