"""An in-process streamable-HTTP MCP server on loopback, for the `mcp` adapter.

PACT live-relation-mcp-adapter. Built with the MCP SDK's own low-level
`Server` and `StreamableHTTPSessionManager`, served by uvicorn on an
ephemeral 127.0.0.1 port in a background thread, so the adapter talks to the
same server code an Archi MCP container runs, over a real socket.

An ASGI wrapper in front of the SDK app does what the SDK cannot be asked
to: it checks the bearer token, answers every request with a redirect in
redirect mode, and COUNTS -- HTTP requests by method, sessions opened
(`initialize`), tool calls (`tools/call`) and sessions closed (`DELETE`).
The deny rows assert those counters stay at zero, and the deadline rows
assert the close arrived.

Each tool is a Python callable returning a `CallToolResult`, registered per
test with :meth:`FakeMcpUpstream.tool`. The helpers below build the result
shapes the adapter must tell apart.
"""
from __future__ import annotations

import contextlib
import json
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable

import anyio
from mcp import types
from mcp.server.lowlevel import Server
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from mcp.server.transport_security import TransportSecuritySettings

ToolBehaviour = Callable[[dict[str, Any]], types.CallToolResult]

#: A raw reply: status, headers, body. Returned by a `raw` hook to answer a
#: request itself instead of handing it to the SDK server.
RawReply = tuple[int, list[tuple[bytes, bytes]], bytes]
#: Called with the HTTP method and the parsed JSON-RPC message (None for a
#: DELETE); returns a `RawReply`, or None to let the SDK server answer.
RawHook = Callable[[str, "dict[str, Any] | None"], "RawReply | None"]


@dataclass
class _Tool:
    behaviour: ToolBehaviour
    delay: float = 0.0


@dataclass
class Counters:
    """What the fake saw. Read by the tests; written only by the server."""

    http_requests: list[str] = field(default_factory=list)
    sessions: int = 0
    calls: list[str] = field(default_factory=list)
    closes: int = 0
    arguments: list[dict[str, Any]] = field(default_factory=list)
    authorization: list[str | None] = field(default_factory=list)

    @property
    def requests(self) -> int:
        return len(self.http_requests)


class FakeMcpUpstream:
    """One MCP server on an ephemeral loopback port, counting what it served."""

    def __init__(
        self,
        *,
        token: str | None = None,
        json_response: bool = False,
        redirect: bool = False,
        path: str = "/mcp",
        raw: RawHook | None = None,
        initialize_delay: float = 0.0,
    ) -> None:
        self.raw = raw
        #: Seconds the server sits on an `initialize` before answering it, as
        #: a server busy with another client's blocking tool call does.
        self.initialize_delay = initialize_delay
        self.token = token
        self.json_response = json_response
        self.redirect = redirect
        self.path = path
        self.counters = Counters()
        self._tools: dict[str, _Tool] = {}
        self._lock = threading.Lock()
        self._server: Any = None
        self._thread: threading.Thread | None = None
        self.port = 0

    # ------------------------------------------------------------ tools

    def tool(self, name: str, behaviour: ToolBehaviour, *, delay: float = 0.0) -> None:
        self._tools[name] = _Tool(behaviour=behaviour, delay=delay)

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}{self.path}"

    def wait_for_closes(self, count: int, *, timeout: float = 5.0) -> int:
        """Wait until `count` sessions have been closed, then return the count."""
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if self.counters.closes >= count:
                break
            time.sleep(0.02)
        return self.counters.closes

    # ------------------------------------------------------------ the app

    def _mcp_server(self) -> Server:
        server: Server = Server("okg-fake-mcp-upstream")
        outer = self

        @server.list_tools()
        async def list_tools() -> list[types.Tool]:
            return [
                types.Tool(name=name, inputSchema={"type": "object"})
                for name in sorted(outer._tools)
            ]

        @server.call_tool(validate_input=False)
        async def call_tool(name: str, arguments: dict[str, Any]) -> Any:
            with outer._lock:
                outer.counters.arguments.append(dict(arguments))
            tool = outer._tools.get(name)
            if tool is None:
                raise ValueError(f"Unknown tool: {name}")
            if tool.delay:
                await anyio.sleep(tool.delay)
            return tool.behaviour(dict(arguments))

        return server

    def _asgi(self) -> Callable[..., Any]:
        manager = StreamableHTTPSessionManager(
            app=self._mcp_server(),
            json_response=self.json_response,
            security_settings=TransportSecuritySettings(
                enable_dns_rebinding_protection=False,
            ),
        )
        outer = self

        async def send_raw(send, reply: RawReply) -> None:
            status, headers, payload = reply
            await send({
                "type": "http.response.start", "status": status,
                "headers": [(b"content-length", str(len(payload)).encode()),
                            *headers],
            })
            await send({"type": "http.response.body", "body": payload})

        async def send_plain(send, status: int, extra: list | None = None) -> None:
            await send({
                "type": "http.response.start", "status": status,
                "headers": [(b"content-length", b"0"), *(extra or [])],
            })
            await send({"type": "http.response.body", "body": b""})

        async def app(scope, receive, send) -> None:
            if scope["type"] == "lifespan":
                stack = contextlib.AsyncExitStack()
                while True:
                    message = await receive()
                    if message["type"] == "lifespan.startup":
                        await stack.enter_async_context(manager.run())
                        await send({"type": "lifespan.startup.complete"})
                    elif message["type"] == "lifespan.shutdown":
                        await stack.aclose()
                        await send({"type": "lifespan.shutdown.complete"})
                        return
            if scope["type"] != "http":
                return
            method = scope["method"]
            headers = {k.decode().lower(): v.decode() for k, v in scope["headers"]}
            with outer._lock:
                outer.counters.http_requests.append(method)
                outer.counters.authorization.append(headers.get("authorization"))
            if outer.redirect:
                await send_plain(send, 302, [(b"location", b"/elsewhere")])
                return
            if outer.token is not None and headers.get("authorization") != (
                f"Bearer {outer.token}"
            ):
                await send_plain(send, 401)
                return
            if scope["path"] != outer.path:
                await send_plain(send, 404)
                return
            if method == "DELETE":
                with outer._lock:
                    outer.counters.closes += 1
                reply = outer.raw("DELETE", None) if outer.raw else None
                if reply is not None:
                    await send_raw(send, reply)
                    return
                await manager.handle_request(scope, receive, send)
                return
            body = b""
            if method == "POST":
                more = True
                while more:
                    message = await receive()
                    body += message.get("body", b"")
                    more = message.get("more_body", False)
                try:
                    rpc = json.loads(body or b"{}")
                except ValueError:
                    rpc = {}
                rpc_method = rpc.get("method") if isinstance(rpc, dict) else None
                with outer._lock:
                    if rpc_method == "initialize":
                        outer.counters.sessions += 1
                    elif rpc_method == "tools/call":
                        outer.counters.calls.append(
                            str((rpc.get("params") or {}).get("name")),
                        )
                reply = (
                    outer.raw("POST", rpc if isinstance(rpc, dict) else None)
                    if outer.raw else None
                )
                if reply is not None:
                    await send_raw(send, reply)
                    return
                if rpc_method == "initialize" and outer.initialize_delay:
                    await anyio.sleep(outer.initialize_delay)
                replayed = [False]

                async def replay():
                    if not replayed[0]:
                        replayed[0] = True
                        return {"type": "http.request", "body": body,
                                "more_body": False}
                    return await receive()

                await manager.handle_request(scope, replay, send)
                return
            await manager.handle_request(scope, receive, send)

        return app

    # ------------------------------------------------------------ lifecycle

    def __enter__(self) -> "FakeMcpUpstream":
        import uvicorn

        config = uvicorn.Config(
            self._asgi(), host="127.0.0.1", port=0, log_level="warning",
            lifespan="on", access_log=False,
        )
        self._server = uvicorn.Server(config)
        self._thread = threading.Thread(target=self._server.run, daemon=True)
        self._thread.start()
        end = time.monotonic() + 10
        while not self._server.started:
            if time.monotonic() > end or not self._thread.is_alive():
                raise RuntimeError("the fake MCP upstream did not start")
            time.sleep(0.01)
        self.port = self._server.servers[0].sockets[0].getsockname()[1]
        return self

    def __exit__(self, *_exc: Any) -> None:
        if self._server is not None:
            self._server.should_exit = True
        if self._thread is not None:
            self._thread.join(timeout=10)


# ----------------------------------------------------------- result shapes

def structured(rows: list[Any], *, path: tuple[str, ...] = ("rows",)) -> ToolBehaviour:
    """Rows in `structuredContent` at `path`, with no text duplicate."""
    def behaviour(_args: dict[str, Any]) -> types.CallToolResult:
        node: Any = rows
        for token in reversed(path):
            node = {token: node}
        return types.CallToolResult(content=[], structuredContent=node)
    return behaviour


def json_text(document: Any) -> ToolBehaviour:
    """One JSON text block and no structuredContent."""
    def behaviour(_args: dict[str, Any]) -> types.CallToolResult:
        return types.CallToolResult(
            content=[types.TextContent(type="text", text=json.dumps(document))],
        )
    return behaviour


def text_blocks(*texts: str, is_error: bool = False) -> ToolBehaviour:
    def behaviour(_args: dict[str, Any]) -> types.CallToolResult:
        return types.CallToolResult(
            content=[types.TextContent(type="text", text=t) for t in texts],
            isError=is_error,
        )
    return behaviour


def sized_rows(total_bytes: int, *, row_bytes: int = 10_000) -> list[dict[str, Any]]:
    """Rows whose JSON is about `total_bytes`, each `{"n": i, "blob": ...}`.

    Few, wide rows: the dispatcher refuses more than 50,000 values, so a
    20 MB answer is built from a few thousand ten-kilobyte strings.
    """
    blob = "x" * row_bytes
    count = max(1, total_bytes // (row_bytes + 20))
    return [{"n": index, "blob": blob} for index in range(count)]


def echo_arguments() -> ToolBehaviour:
    """One row: the arguments the tool received, as structured content."""
    def behaviour(args: dict[str, Any]) -> types.CallToolResult:
        return types.CallToolResult(
            content=[], structuredContent={"rows": [{"received": args}]},
        )
    return behaviour


def sse(*messages: dict[str, Any]) -> RawReply:
    """A 200 `text/event-stream` reply carrying `messages` as SSE events."""
    body = "".join(
        f"event: message\ndata: {json.dumps(m)}\n\n" for m in messages
    ).encode("utf-8")
    return 200, [(b"content-type", b"text/event-stream")], body


def sse_text(data: str) -> RawReply:
    """A 200 SSE reply with one event whose data is `data`, verbatim."""
    body = f"event: message\ndata: {data}\n\n".encode("utf-8")
    return 200, [(b"content-type", b"text/event-stream")], body
