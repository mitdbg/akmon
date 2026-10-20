"""Per-session MCP tool projection, and the notification that follows it.

Issue #1458. Static profiles (`tool_profiles.py`) bound what a launch
REGISTERS; this module bounds what one SESSION SEES of that registration, so
a session can start on a minimal surface and widen without a restart and
without touching any other session.

Three facts about the pinned SDK (`mcp` 1.28.1) shape everything here.

**`FastMCP` never passes `NotificationOptions`.** `run_stdio_async`,
`sse_app` and `StreamableHTTPSessionManager` all call
`Server.create_initialization_options()` with no arguments, so
`get_capabilities` reads `NotificationOptions()` and every session is told
`tools.listChanged: false`. `advertise_tool_list_changed` replaces that one
bound method on the lowlevel server instance, which is the single point all
three transports funnel through, and then PROVES the capability flipped
rather than assuming it.

**`ToolManager._tools` is process-wide.** There is no SDK hook for a
per-session tool view. The projection therefore lives one layer up, in the
two FastMCP methods that read the registry: `list_tools` (what a session is
told it has) and `call_tool` (what a session may actually invoke). Filtering
only the listing would be cosmetic -- a hidden tool would still run for
anyone who guessed its name -- so both are filtered, and an invocation of a
tool this session cannot see is a typed refusal naming the way to widen.

**`Server._tool_cache` is process-wide too, and `list_tools` rebuilds it.**
The lowlevel `ListToolsRequest` handler clears the cache and refills it from
whatever the last listing returned, and `call_tool` reads it for
input/output-schema validation. Under a per-session listing that cache would
carry one session's view into another's validation step. That is not a
visibility leak -- the projection above decides visibility, not the cache --
but it would make schema validation depend on which session listed last, so
`install_session_tool_projection` also repoints
`Server._get_cached_tool_definition` at the full registry, where a tool's
SCHEMA is a process-wide fact. Visibility stays per session; definitions stay
whole.

Everything here is inert unless a launch configures an expansion ceiling. A
server built without one registers, lists, calls and advertises exactly what
it did before this module existed.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import weakref
from dataclasses import dataclass
from typing import Any

from mcp.server.fastmcp import FastMCP
from mcp.server.lowlevel.server import NotificationOptions

from okg.substrate.errors import SubstrateError


log = logging.getLogger(__name__)


class ToolNotVisibleInSession(SubstrateError):
    """A session invoked a tool its own projection does not contain.

    A refusal rather than a silent execution: a tool the session was never
    told about must not run because the caller guessed its name. The message
    names the widening action, so the fix is reachable from the refusal.
    """

    code = "mcp_tool_not_in_session_surface"


class ToolSurfaceExpansionUnavailable(SubstrateError):
    """This launch cannot widen a session's tool surface.

    Two causes, both stated rather than absorbed: the launch configured no
    expansion ceiling, or the transport supplies no durable session identity
    (stateless HTTP builds a new `ServerSession` per request, so a widening
    would evaporate before the next call).
    """

    code = "mcp_tool_surface_expansion_unavailable"


class ToolListChangedNotAdvertised(SubstrateError):
    """The SDK did not accept the `tools.listChanged` capability.

    A build-time post-condition. Emitting `notifications/tools/list_changed`
    to a client that was told the server never sends them is a protocol lie,
    so the launch refuses instead of serving one.
    """

    code = "mcp_tool_list_changed_not_advertised"


class SessionProjectionUnsupported(SubstrateError):
    """The pinned SDK lacks a hook per-session projection depends on.

    Separate from the capability refusal above because the cause and the
    reader's next step differ: this one says the SDK moved, not that a
    capability was rejected. Both refuse the launch; neither degrades to a
    server that half-projects.
    """

    code = "mcp_session_projection_unsupported"


class StatelessHttpExpansionUnsupported(SubstrateError):
    """A ceiling was configured on a stateless-HTTP server.

    Stateless HTTP builds a fresh `ServerSession` for every request, so
    `okg_current_session()` reports no identity, no session can ever widen,
    and no `tools/list_changed` frame can ever be sent -- yet advertising the
    ceiling would tell every client at `initialize` that the server sends
    them. That is the same protocol lie this module refuses in the other
    direction, so the launch refuses rather than advertising a notification
    that cannot exist.
    """

    code = "mcp_tool_surface_expansion_stateless_http"


@dataclass
class _SessionSurfaceState:
    """One session's projection of the process-wide registry."""

    visible: frozenset[str]
    expanded: bool = False
    #: One un-sent `tools/list_changed`, owed to this session. Set inside the
    #: registry's own mutation and taken exactly once, so two concurrent
    #: calls that both observe the widening cannot each emit a frame: a
    #: before/after comparison at the call boundary would do that, because
    #: `call_tool` yields at its await and both callers can read the narrow
    #: set before either widens it.
    notification_pending: bool = False
    #: Set when `send_tool_list_changed` raised. The widening still happened;
    #: the client was not told about it, and must re-list to catch up. Read
    #: back through `inspect(target={"kind":"surface"})` so a dropped
    #: notification is visible rather than inferred from a stalled client.
    notification_failed: bool = False


@dataclass(frozen=True)
class ExpansionOutcome:
    """What one expansion request did."""

    status: str
    visible_tools: tuple[str, ...]
    added_tools: tuple[str, ...]


class SessionToolSurface:
    """Weak, per-session tool visibility over one process-wide registry.

    `base` is what a session sees before it asks for anything; `ceiling` is
    the widest set it can reach and is exactly what the server REGISTERS. A
    session absent from the map is on `base` -- a new session inherits the
    minimal surface, never another session's widening, which is the property
    issue #1458 requires under streamable-http.

    Weak-keyed on the SDK `ServerSession` object, like
    `LogicalSessionRegistry`: never on a deployment, a process id, an
    authenticated identity, or the raw `mcp-session-id` header.
    """

    def __init__(
        self,
        *,
        base: frozenset[str] | set[str],
        ceiling: frozenset[str] | set[str],
        base_profile: str | None = None,
        ceiling_profile: str | None = None,
    ) -> None:
        base_set = frozenset(base)
        ceiling_set = frozenset(ceiling)
        if not base_set <= ceiling_set:
            raise ValueError(
                "the session base tool surface must be a subset of the "
                f"ceiling; {sorted(base_set - ceiling_set)} are outside it"
            )
        self.base = base_set
        self.ceiling = ceiling_set
        self.base_profile = base_profile
        self.ceiling_profile = ceiling_profile
        self._lock = threading.RLock()
        self._states: weakref.WeakKeyDictionary[Any, _SessionSurfaceState] = (
            weakref.WeakKeyDictionary()
        )

    @staticmethod
    def supports(identity: object | None) -> bool:
        """Whether `identity` is usable as a weak map key."""

        if identity is None:
            return False
        try:
            hash(identity)
            weakref.ref(identity)
        except (TypeError, ValueError):
            return False
        return True

    def visible_for(self, identity: object | None) -> frozenset[str]:
        """The tools `identity` may list and call. Unknown session -> base."""

        if not self.supports(identity):
            return self.base
        with self._lock:
            state = self._states.get(identity)
            return state.visible if state is not None else self.base

    def is_expanded(self, identity: object | None) -> bool:
        if not self.supports(identity):
            return False
        with self._lock:
            state = self._states.get(identity)
            return bool(state and state.expanded)

    def notification_failed(self, identity: object | None) -> bool:
        if not self.supports(identity):
            return False
        with self._lock:
            state = self._states.get(identity)
            return bool(state and state.notification_failed)

    def expand(self, identity: object | None) -> ExpansionOutcome:
        """Widen `identity` to the ceiling. Idempotent, and never narrows.

        Returns `already_expanded` rather than raising on a repeat call: an
        agent that lost track of its own surface should be able to ask again
        and get the truth, and a second `tools/list_changed` for a list that
        did not change would train a client to ignore the notification.
        """

        if not self.supports(identity):
            raise ToolSurfaceExpansionUnavailable(
                "in-session tool expansion requires a stable stateful MCP "
                "ServerSession identity; this transport supplies none, so a "
                "widening could not survive to the next call",
                fix_hint=(
                    "use stdio or a stateful streamable-http session, or "
                    "relaunch with the wider --tools"
                ),
            )
        with self._lock:
            state = self._states.get(identity)
            if state is None:
                state = _SessionSurfaceState(visible=self.base)
                self._states[identity] = state
            before = state.visible
            if before == self.ceiling:
                return ExpansionOutcome(
                    status="already_expanded",
                    visible_tools=tuple(sorted(before)),
                    added_tools=(),
                )
            state.visible = self.ceiling
            state.expanded = True
            state.notification_pending = True
            return ExpansionOutcome(
                status="expanded",
                visible_tools=tuple(sorted(self.ceiling)),
                added_tools=tuple(sorted(self.ceiling - before)),
            )

    def take_pending_notification(self, identity: object | None) -> bool:
        """Claim this session's owed `tools/list_changed`, at most once."""

        if not self.supports(identity):
            return False
        with self._lock:
            state = self._states.get(identity)
            if state is None or not state.notification_pending:
                return False
            state.notification_pending = False
            return True

    def mark_notification_failed(self, identity: object | None) -> None:
        if not self.supports(identity):
            return
        with self._lock:
            state = self._states.get(identity)
            if state is not None:
                state.notification_failed = True

    def __len__(self) -> int:
        """Testing/diagnostic count of sessions holding non-default state."""

        with self._lock:
            return len(self._states)


class SessionScopedToolsMixin:
    """Filter `list_tools` and `call_tool` by the calling session.

    Mixed in ahead of `FastMCP` so `super()` reaches the SDK implementation.
    With no `_okg_session_tool_surface` installed every method is a pass-
    through, which is what keeps a launch without an expansion ceiling
    byte-identical to the one before this class existed.
    """

    _okg_session_tool_surface: SessionToolSurface | None = None

    def sse_app(self, *args, **kwargs):
        if getattr(self, "_okg_principal_mapping", None) is not None:
            raise RuntimeError(
                "multi-user MCP requires streamable-http session ownership; legacy SSE is unsupported"
            )
        return super().sse_app(*args, **kwargs)

    def streamable_http_app(self):
        app = super().streamable_http_app()
        mapping = getattr(self, "_okg_principal_mapping", None)
        if mapping is not None:
            from starlette.middleware import Middleware
            from starlette.middleware.authentication import AuthenticationMiddleware
            from okg.substrate.mcp.principal_session import PrincipalSessionMiddleware

            for index, middleware in enumerate(app.user_middleware):
                if middleware.cls is AuthenticationMiddleware:
                    app.user_middleware.insert(
                        index + 1,
                        Middleware(
                            PrincipalSessionMiddleware,
                            deployment_name=self._okg_deployment_name,
                            mapping=mapping,
                        ),
                    )
                    break
            else:
                raise RuntimeError(
                    "multi-user MCP requires authenticated session middleware"
                )
        return app

    async def _okg_check_dispatch_principal(self, name):
        """Resolve the caller's principal before a list or call.

        The gate borrows a pool connection and writes refusal audit rows, so
        on a server that runs blocking steps off the loop (issue #3493,
        ``_okg_run_blocking``) it runs in a worker thread in the request's
        context; a slow checkout then holds one worker, not every session.
        """
        gate = getattr(self, "_okg_dispatch_principal_gate", None)
        if gate is None:
            return
        run_blocking = getattr(self, "_okg_run_blocking", None)
        if run_blocking is None:
            gate(name)
            return
        await run_blocking(gate, name)

    def _okg_before_tool_dispatch(
        self,
        name: str,
        arguments: dict[str, Any],
    ) -> None:
        """Run call validation after authorization and session visibility."""

    # ---------- session identity ----------

    def okg_current_session(self) -> object | None:
        """The SDK `ServerSession` for the in-flight request, or None.

        Stateless HTTP is deliberately reported as "no session": the SDK
        builds a fresh `ServerSession` per request there, so treating it as
        durable would let a widening be recorded against an object that is
        discarded before the client can use it.
        """

        try:
            context = self.get_context()  # type: ignore[attr-defined]
        except (LookupError, RuntimeError, ValueError):
            return None
        if context is None:
            return None
        try:
            if bool(self.settings.stateless_http):  # type: ignore[attr-defined]
                return None
            return context.session
        except (AttributeError, LookupError, RuntimeError, ValueError):
            return None

    def okg_visible_tools(self) -> frozenset[str] | None:
        surface = self._okg_session_tool_surface
        if surface is None:
            return None
        return surface.visible_for(self.okg_current_session())

    # ---------- the two registry readers ----------

    async def list_tools(self) -> list[Any]:
        await self._okg_check_dispatch_principal("tools/list")
        tools = await super().list_tools()  # type: ignore[misc]
        visible = self.okg_visible_tools()
        if visible is None:
            return tools
        return [tool for tool in tools if tool.name in visible]

    async def call_tool(self, name: str, arguments: dict[str, Any]) -> Any:
        await self._okg_check_dispatch_principal(name)
        surface = self._okg_session_tool_surface
        if surface is None:
            self._okg_before_tool_dispatch(name, arguments)
            return await super().call_tool(name, arguments)  # type: ignore[misc]

        identity = self.okg_current_session()
        visible = surface.visible_for(identity)
        if name not in visible:
            raise ToolNotVisibleInSession(
                f"tool {name!r} is not in this session's tool surface "
                f"({sorted(visible)}). It is registered by this server and "
                "can be added to THIS session without a restart.",
                data={
                    "tool": name,
                    "visible_tools": sorted(visible),
                    "expandable_tools": sorted(surface.ceiling - visible),
                },
                fix_hint=(
                    "call inspect(target={\"kind\": \"surface\", "
                    "\"action\": \"expand\"}), then re-list tools"
                ),
            )

        # Emitted HERE rather than inside the tool that widened the surface:
        # the read tools are synchronous and run in a worker thread (issue
        # #3493), so none of them can await a notification. This is the one place that
        # sees both a completed widening and an awaitable context.
        #
        # The claim is taken from the registry rather than derived from a
        # before/after comparison. `call_tool` yields at its await, so two
        # concurrent calls in one session can both observe the narrow set
        # first and would then both emit a frame for a single widening. The
        # registry sets the claim inside the same locked mutation that
        # widens, and hands it out once.
        #
        # Drained on the FAILING path too. The widening is committed to the
        # registry before the call returns, so anything raising downstream of
        # it -- an oversize refusal that raises, a stamper failure -- used to
        # strand the claim: the session's visible set was already wider and
        # no frame was sent until its next SUCCESSFUL call, which a client
        # holding a stale list may never make.
        try:
            self._okg_before_tool_dispatch(name, arguments)
            result = await super().call_tool(name, arguments)  # type: ignore[misc]
        except asyncio.CancelledError:
            # The one exception. A cancelled task must not await anything
            # more; the claim survives for the session's next call rather
            # than being dropped here.
            raise
        except BaseException:
            if identity is not None and surface.take_pending_notification(identity):
                await self._okg_send_tool_list_changed(surface, identity)
            raise

        if identity is not None and surface.take_pending_notification(identity):
            await self._okg_send_tool_list_changed(surface, identity)
        return result

    async def _okg_send_tool_list_changed(
        self,
        surface: SessionToolSurface,
        identity: Any,
    ) -> None:
        """Tell ONE session its tool list changed.

        Addressed to the session that widened, never broadcast: another
        session's list did not change, and telling it otherwise would cost a
        pointless `tools/list` round trip on every expansion anywhere.

        A send failure does not fail the call that caused it -- the widening
        is already true, and the tool result is already computed -- but it is
        not swallowed either: it is logged and recorded on the session, and
        `inspect(target={"kind":"surface"})` reports
        `list_changed_notification_failed` so a client that never re-listed
        can be told why.
        """

        try:
            await identity.send_tool_list_changed()
        except Exception as exc:  # noqa: BLE001 - a dropped notification must not fail the call that widened.
            surface.mark_notification_failed(identity)
            log.warning(
                "MCP tool surface widened for this session but the "
                "notifications/tools/list_changed frame could not be sent "
                "(%s); the client must re-list tools to see the wider "
                "surface",
                exc,
            )


class SessionScopedFastMCP(SessionScopedToolsMixin, FastMCP):
    """A plain FastMCP carrying the per-session projection.

    The substrate server uses `ByteBoundedFastMCP`, which mixes the same
    class in. This one exists so the mechanism can be exercised end to end
    against a real server process without a database.
    """


def advertise_tool_list_changed(mcp: FastMCP) -> None:
    """Make this server tell clients it sends `tools/list_changed`.

    `FastMCP` calls `Server.create_initialization_options()` with no
    `NotificationOptions` on every transport, which hardcodes
    `tools.listChanged: false`. Binding the option onto the instance is the
    smallest change that reaches all three transports, because all three call
    that one method.

    Called ONLY where the server can actually widen a surface. Advertising a
    notification a launch will never send is the same class of untrue
    statement as not advertising one it does.
    """

    server = mcp._mcp_server  # type: ignore[attr-defined]
    original = server.create_initialization_options
    options = NotificationOptions(tools_changed=True)

    def create_initialization_options(
        notification_options: NotificationOptions | None = None,
        experimental_capabilities: dict[str, dict[str, Any]] | None = None,
    ):
        return original(
            notification_options or options,
            experimental_capabilities,
        )

    server.create_initialization_options = create_initialization_options

    # Proved, not assumed: this reaches into SDK internals, and a version
    # that stopped honoring the argument would otherwise leave the server
    # sending a notification it had declared it never sends.
    capabilities = server.create_initialization_options().capabilities
    tools_capability = getattr(capabilities, "tools", None)
    if not getattr(tools_capability, "listChanged", False):
        raise ToolListChangedNotAdvertised(
            "the MCP SDK did not accept tools.listChanged=true, so this "
            "server would emit tool-list-changed notifications to clients "
            "told it never sends them; refusing to start",
            data={"mcp_sdk_capabilities": capabilities.model_dump(exclude_none=True)},
            fix_hint="report this as an okg substrate defect against the pinned mcp SDK",
        )


def install_session_tool_projection(
    mcp: FastMCP,
    surface: SessionToolSurface,
) -> None:
    """Turn per-session visibility on for one server, and advertise it.

    Also repoints the lowlevel `_get_cached_tool_definition` at the full
    registry. That cache is process-wide and is rebuilt by every
    `tools/list`, so under a per-session listing a tool's schema validation
    would otherwise depend on which session listed most recently. Visibility
    is decided by `SessionToolSurface`; a tool's SCHEMA is not session state.
    """

    if not isinstance(mcp, SessionScopedToolsMixin):
        raise TypeError(
            "per-session tool projection requires a FastMCP built on "
            "SessionScopedToolsMixin"
        )
    # Refused BEFORE the capability is advertised, and before the surface is
    # installed: under stateless HTTP the SDK discards the ServerSession
    # after every request, so `okg_current_session()` reports none, every
    # session stays on the base set forever, and no frame can be sent. A
    # server that started anyway would tell every client at `initialize`
    # that it sends `tools/list_changed`.
    if bool(getattr(getattr(mcp, "settings", None), "stateless_http", False)):
        raise StatelessHttpExpansionUnsupported(
            "an in-session tool-surface ceiling "
            f"({surface.ceiling_profile!r}) was configured on a "
            "stateless-HTTP server. Stateless HTTP builds a new MCP session "
            "per request, so no session could ever widen and no "
            "notifications/tools/list_changed frame could ever be sent, "
            "while every client would be told at initialize that this "
            "server sends them. Refusing to start.",
            data={
                "base_profile": surface.base_profile,
                "ceiling_profile": surface.ceiling_profile,
                "stateless_http": True,
            },
            fix_hint=(
                "serve stateful streamable-http or stdio, or drop "
                "--tools-ceiling and launch with the wider "
                "--tools"
            ),
        )
    mcp._okg_session_tool_surface = surface  # type: ignore[attr-defined]
    advertise_tool_list_changed(mcp)

    server = mcp._mcp_server  # type: ignore[attr-defined]
    if not hasattr(server, "_get_cached_tool_definition"):
        # A guard, not a fallback: the patch below is the only thing keeping
        # one session's listing out of another's schema validation, so an SDK
        # that renamed the hook must be noticed here rather than silently
        # reintroducing the coupling.
        raise SessionProjectionUnsupported(
            "the pinned mcp SDK has no Server._get_cached_tool_definition, "
            "so a per-session tools/list would leave schema validation "
            "dependent on which session listed last; refusing to start",
            fix_hint="report this as an okg substrate defect against the pinned mcp SDK",
        )

    async def _get_cached_tool_definition(tool_name: str):
        from mcp.types import Tool as MCPTool

        info = mcp._tool_manager.get_tool(tool_name)  # type: ignore[attr-defined]
        if info is None:
            return None
        return MCPTool(
            name=info.name,
            title=info.title,
            description=info.description,
            inputSchema=info.parameters,
            outputSchema=info.output_schema,
            annotations=info.annotations,
            icons=info.icons,
            _meta=info.meta,
        )

    server._get_cached_tool_definition = _get_cached_tool_definition
