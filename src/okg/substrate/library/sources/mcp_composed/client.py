"""Sync wrapper around the official `mcp` SDK's `ClientSession`.

OKG composes with locally-installed third-party MCP servers
(Gmail / Slack / Calendar / etc.) by spawning the server as a
subprocess and ingesting via its tool calls. The MCP SDK is
async-first; the substrate's `Source.run()` ingest path is
synchronous. This module bridges the two by owning a private
`asyncio` event loop for the lifetime of a `with McpClient(...)`
block and calling `loop.run_until_complete` on each async call.

We deliberately delegate the wire-protocol details — JSON-RPC
framing, `initialize` handshake, capability negotiation,
graceful shutdown — to the SDK rather than reimplementing them.
That keeps OKG aligned with the spec's evolution.

`tools/list` is cached, so a server that widens its tool list
mid-session would otherwise leave this client working from a
list taken before the change (issue #1458). The SDK does not
invalidate anything on our behalf, so the session is opened with
a message handler that drops the cache on
`notifications/tools/list_changed`; the next `list_tools()`
re-fetches. Nothing is refreshed eagerly: the notification says
the list changed, not that this client needs it right now.
"""
from __future__ import annotations

import asyncio
import logging
from contextlib import AsyncExitStack
from datetime import timedelta
from dataclasses import dataclass, field
from typing import Any, Optional

from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client


_log = logging.getLogger(__name__)


class McpServerError(RuntimeError):
    """Structured failure surface for any MCP-subprocess problem.

    `stage` is one of: `spawn` (subprocess wouldn't start),
    `handshake` (initialize failed), `tool_call` (server returned
    an error frame or the SDK raised mid-call), `transport`
    (subprocess died unexpectedly).
    """

    def __init__(
        self,
        message: str,
        *,
        stage: str = "unknown",
        code: Optional[int] = None,
    ) -> None:
        super().__init__(message)
        self.stage = stage
        self.code = code


@dataclass
class McpClient:
    """Sync context manager wrapping `mcp.ClientSession`.

    Usage:

        with McpClient(["npx", "-y", "@vendor/x-mcp@1.2.3"]) as cli:
            tools = cli.list_tools()
            out = cli.call_tool("get_message", {"id": "abc"})

    Parameters:
      - `command`: argv to spawn (first element is the executable).
      - `env`: extra env vars merged into the subprocess's env
        (e.g. OAuth tokens passed via env).
      - `cwd`: subprocess working directory.
      - `subprocess_timeout`: per-call read timeout in seconds.
        Applies uniformly to `initialize`, `list_tools`, and each
        `call_tool` unless an override is passed.
      - `client_info`: dict for the MCP `clientInfo` field. Give it a
        `name`; the `version` is the installed okg version (`okg.__version__`)
        unless the caller sets one, so a release bump never edits it.
    """

    command: list[str]
    env: Optional[dict[str, str]] = None
    cwd: Optional[str] = None
    subprocess_timeout: float = 30.0
    client_info: dict[str, Any] = field(default_factory=lambda: {
        "name": "okg-substrate",
    })

    _loop: Optional[asyncio.AbstractEventLoop] = field(
        default=None, init=False,
    )
    _exit_stack: Optional[AsyncExitStack] = field(default=None, init=False)
    _session: Optional[ClientSession] = field(default=None, init=False)
    _tools_cache: Optional[list[dict]] = field(default=None, init=False)
    #: Counts `tools/list_changed` frames seen on this session. Diagnostic,
    #: and the thing a test can assert on: "the cache is None" cannot tell a
    #: notification that arrived from one that never did.
    _tool_list_changed_count: int = field(default=0, init=False)
    _server_info: Optional[dict] = field(default=None, init=False)
    _capabilities: Optional[dict] = field(default=None, init=False)
    _instructions: Optional[str] = field(default=None, init=False)

    # ---------- context manager ----------

    def __enter__(self) -> "McpClient":
        if not self.command:
            raise McpServerError(
                "mcp_server_command is empty", stage="spawn",
            )
        self._loop = asyncio.new_event_loop()
        try:
            self._loop.run_until_complete(self._aenter())
        except McpServerError:
            self._close_loop()
            raise
        except FileNotFoundError as e:
            self._close_loop()
            raise McpServerError(
                f"MCP server command not found: {self.command[0]}: {e}",
                stage="spawn",
            ) from e
        except Exception as e:  # noqa: BLE001 — subprocess infra layer
            self._close_loop()
            raise McpServerError(
                f"failed to enter MCP client context: {e}",
                stage="handshake",
            ) from e
        return self

    def __exit__(self, *exc) -> None:
        if self._loop is None:
            return
        try:
            if self._exit_stack is not None:
                self._loop.run_until_complete(self._exit_stack.aclose())
        except Exception as e:  # noqa: BLE001
            _log.debug("mcp_client: exit-stack close errored: %s", e)
        finally:
            self._close_loop()
            self._exit_stack = None
            self._session = None

    # ---------- async lifecycle ----------

    async def _aenter(self) -> None:
        params = StdioServerParameters(
            command=self.command[0],
            args=list(self.command[1:]),
            env=self.env,
            cwd=self.cwd,
        )
        self._exit_stack = AsyncExitStack()
        try:
            streams = await self._exit_stack.enter_async_context(
                stdio_client(params),
            )
        except FileNotFoundError:
            # Surface as spawn-stage error so callers (the source
            # base class) emit a structured `SourceRun` rather
            # than treating this as an internal bug.
            raise
        read_stream, write_stream = streams
        self._session = await self._exit_stack.enter_async_context(
            ClientSession(
                read_stream,
                write_stream,
                read_timeout_seconds=timedelta(
                    seconds=self.subprocess_timeout,
                ),
                client_info=self._build_implementation(),
                message_handler=self._handle_server_message,
            ),
        )
        init_result = await self._session.initialize()
        if init_result.serverInfo is not None:
            self._server_info = {
                "name": init_result.serverInfo.name,
                "version": init_result.serverInfo.version,
            }
        if init_result.capabilities is not None:
            self._capabilities = init_result.capabilities.model_dump(
                exclude_none=True,
            )
        self._instructions = init_result.instructions

    async def _handle_server_message(self, message: Any) -> None:
        """Drop the tool cache when the server says the tool list changed.

        Registered on the `ClientSession` so it runs on the SDK's own
        receive loop. It must never raise: an exception here would tear down
        the session over a notification, which is a strictly worse outcome
        than a stale tool list.
        """

        try:
            from mcp.types import ToolListChangedNotification

            root = getattr(message, "root", None)
            if isinstance(root, ToolListChangedNotification):
                self._tool_list_changed_count += 1
                self._tools_cache = None
                _log.debug(
                    "mcp_client: tools/list_changed received; tool cache "
                    "invalidated (next list_tools re-fetches)",
                )
        except Exception as e:  # noqa: BLE001 — a notification must not kill the session.
            _log.warning(
                "mcp_client: server message handler errored: %s", e,
            )

    def invalidate_tools_cache(self) -> None:
        """Force the next `list_tools()` to go back to the server."""

        self._tools_cache = None

    @property
    def tool_list_changed_count(self) -> int:
        """How many `tools/list_changed` frames this session has seen."""

        return self._tool_list_changed_count

    def _build_implementation(self):
        # `client_info` is plumbed through to MCP's `Implementation`
        # type. We construct it lazily here so callers can pass a
        # plain dict to `McpClient(...)` without importing MCP.
        import okg
        from mcp.types import Implementation

        version = self.client_info.get("version")
        return Implementation(
            name=str(self.client_info.get("name", "okg-substrate")),
            version=str(okg.__version__ if version is None else version),
        )

    # ---------- public RPC surface ----------

    def list_tools(self, *, force_refresh: bool = False) -> list[dict]:
        """Return the server's declared tools.

        Cached, but no longer permanently: the cache is dropped when the
        server sends `tools/list_changed`, so a widened surface is picked up
        on the next call instead of being invisible for the life of the
        session. `force_refresh=True` re-fetches unconditionally, for a
        caller that has reason to distrust the cache without a notification.
        """
        if self._tools_cache is not None and not force_refresh:
            return self._tools_cache
        if self._session is None:
            raise McpServerError(
                "McpClient.list_tools called outside `with`",
                stage="transport",
            )
        result = self._run(self._session.list_tools())
        tools: list[dict] = []
        for t in result.tools:
            tools.append({
                "name": t.name,
                "description": t.description,
                "inputSchema": (
                    t.inputSchema
                    if isinstance(t.inputSchema, dict)
                    else (
                        t.inputSchema.model_dump()
                        if t.inputSchema is not None else None
                    )
                ),
            })
        self._tools_cache = tools
        return tools

    def call_tool(
        self,
        name: str,
        args: dict,
        *,
        timeout: Optional[float] = None,
        meta: Optional[dict[str, Any]] = None,
    ) -> dict:
        """Call an MCP tool. On success, returns a dict shaped like
        `{isError: False, content: [{type, text|json|...}, ...]}`
        — matching what the previous in-house client returned, so
        existing subclasses don't notice the SDK switch. On an
        error frame, raises `McpServerError(stage='tool_call')`.

        `meta` is sent as the request's `_meta`. An okg server reads
        `{"okg/result_format": "json"}` there to return the compact JSON of
        the result instead of its text rendering (issue #3059).
        """
        if self._session is None:
            raise McpServerError(
                "McpClient.call_tool called outside `with`",
                stage="transport",
            )
        kwargs: dict[str, Any] = {"arguments": args}
        if meta is not None:
            kwargs["meta"] = dict(meta)
        if timeout is not None:
            kwargs["read_timeout_seconds"] = timedelta(seconds=timeout)
        try:
            result = self._run(self._session.call_tool(name, **kwargs))
        except McpServerError:
            raise
        except Exception as e:  # noqa: BLE001
            raise McpServerError(
                f"tool call {name!r} failed: {e}",
                stage="tool_call",
            ) from e
        # MCP's `CallToolResult.isError` signals a server-reported
        # tool error; surface as McpServerError so subclass code
        # doesn't have to special-case it on every call site.
        if getattr(result, "isError", False):
            text_chunks: list[str] = []
            for c in result.content or []:
                t = getattr(c, "text", None)
                if t:
                    text_chunks.append(t)
            raise McpServerError(
                " | ".join(text_chunks)
                or f"tool {name!r} returned isError",
                stage="tool_call",
            )
        return {
            "isError": False,
            "content": [self._content_to_dict(c) for c in (result.content or [])],
            "structuredContent": getattr(result, "structuredContent", None),
        }

    @staticmethod
    def _content_to_dict(c: Any) -> dict:
        # MCP's content types are pydantic models — TextContent,
        # ImageContent, EmbeddedResource, etc. `.model_dump` always
        # works; we use it to get a plain dict for downstream code.
        try:
            return c.model_dump(exclude_none=True)
        except AttributeError:
            return {"type": "unknown", "raw": repr(c)}

    @property
    def server_info(self) -> Optional[dict]:
        return self._server_info

    @property
    def capabilities(self) -> Optional[dict]:
        return self._capabilities

    @property
    def instructions(self) -> Optional[str]:
        return self._instructions

    # ---------- async-sync bridge ----------

    def _run(self, coro):
        """Run an async coro on the client's private event loop.
        The loop is kept alive between calls so the underlying
        `ClientSession`'s task scope + memory streams stay open
        across `list_tools` / `call_tool` invocations."""
        assert self._loop is not None
        return self._loop.run_until_complete(coro)

    def _close_loop(self) -> None:
        if self._loop is None:
            return
        try:
            # Cancel any leftover async tasks before closing so we
            # don't get "Task was destroyed but pending" warnings.
            pending = asyncio.all_tasks(self._loop)
            for task in pending:
                task.cancel()
            if pending:
                self._loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True),
                )
        except Exception as e:  # noqa: BLE001
            _log.debug("mcp_client: task drain errored: %s", e)
        try:
            self._loop.close()
        except Exception as e:  # noqa: BLE001
            _log.debug("mcp_client: loop close errored: %s", e)
        self._loop = None
