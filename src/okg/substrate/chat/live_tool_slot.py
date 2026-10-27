"""The live-tool connection slot (#1183 S4).

*Terms.* **Live tool** — a tool that reads a CURRENT external system rather
than the published graph. **Graph operator** — one of the OKG read/write
operators ``okg mcp-serve`` advertises. **Slot** — the manifest block
binding a closed list of live-tool names to one endpoint.

**Why a second connection and not a wider filter.** Umbrella decision 6:
graph tools and live tools carry different authority, and a subject allowed
to search the graph does not thereby receive tools that call operational
systems. Open WebUI's ``function_name_filter_list`` is a property of the
CONNECTION — the vendor reads it off the connection and applies it to every
spec that connection serves — so a single registration serves exactly one
tool set and cannot hold two authorities separably. Two registrations can.
The graph connection's filter therefore never names a live tool and the
live connection's filter never names a graph operator, and
:func:`assert_transports_disjoint` asserts BOTH directions at the wire
rather than trusting the refusals below to have covered every path.

**What this does not settle.** The design note's fallback of "one connection
per audience" was called a category error by its own review, and it stays
one here: per-connection filters segment the TOOL SURFACE, while every
connection resolves to one static client id, so Nomos returns identical rows
whichever connection asked. This slot separates what a subject may CALL. It
does not, and cannot at this layer, separate what a subject may SEE.

**What this does not implement.** Nothing here is a live tool. The slot binds
names to an endpoint URL the instance supplies; the MCP server that reaches
an operational system is somebody else's process, and if it is not running
the connection registers and its verify probe fails, which is the honest
outcome.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

from okg.substrate.errors import SubstrateError


__all__ = [
    "LIVE_CONNECTION_SUFFIX",
    "ChatLiveToolSlotError",
    "LiveToolSlot",
    "assert_transports_disjoint",
    "resolve_live_tool_slot",
]


#: Appended to the deployment's MCP server id to name the live registration.
#: A separate id is what lets one reconcile drop, rewrite and withdraw the
#: live connection without touching the graph one. Underscore: Open WebUI
#: prefixes every tool name with the server id, so a server id uses one
#: separator throughout (``okg.substrate.chat.sync.MCP_SERVER_ID_PREFIX``).
LIVE_CONNECTION_SUFFIX = "_live"


class ChatLiveToolSlotError(SubstrateError):
    """The live-tool slot cannot be resolved. Nothing is applied.

    Its own code rather than ``chat_sync_failed``: the slot resolves for
    ``okg chat plan`` too, which never contacts an instance and must not
    refuse under a code whose fix hint says to re-run a sync.
    """

    code = "chat_live_tool_slot_refused"


@dataclass(frozen=True)
class LiveToolSlot:
    """One deployment's resolved live-tool connection.

    ``allowed`` is the manifest's closed ceiling; ``granted`` is what any
    audience actually asked for and is what the connection's filter carries.
    They differ whenever the deployment allows a name no audience wants, and
    keeping them apart is what makes "allowed but not granted" a state an
    operator can read instead of inferring.
    """

    server_id: str
    url: str
    allowed: tuple[str, ...]
    granted: tuple[str, ...]
    audiences: tuple[str, ...]
    auth_token_env: str | None = None

    @property
    def registered(self) -> bool:
        """True when some audience is granted a live tool.

        NEVER by default. A slot with no audience grant is a declaration
        that the deployment permits these names, not that anyone has them,
        and the connection is not written.
        """
        return bool(self.granted)


def _graph_operator_universe() -> frozenset[str]:
    """Every name a graph operator can occupy, read from its own module."""
    from okg.substrate.mcp.operators import (
        AKMON_READ_TOOLS,
        QUERY_ESCAPE_TOOLS,
        SOURCE_ADMIN_TOOLS,
        WRITE_EFFECT_TOOLS,
    )

    return frozenset(
        (*AKMON_READ_TOOLS, *QUERY_ESCAPE_TOOLS, *WRITE_EFFECT_TOOLS,
         *SOURCE_ADMIN_TOOLS)
    )


def resolve_live_tool_slot(
    chat: Any, *, deployment: str, mcp_server_id: str,
) -> LiveToolSlot | None:
    """The slot ``chat.mcp.live_tools`` describes, or ``None``.

    Pure: no environment, no instance, no clock. ``plan``, ``sync`` and
    ``readback`` all call this, so the three cannot hold different opinions
    of one slot -- the failure S3 closed for bundle composition, kept closed
    here.

    ``chat`` is already the COMPOSED block wherever a bundle was selected,
    so the bundle ceiling has been applied before this runs.

    An audience with no ``live_tools`` key is not a participant in the
    two-audience rule: it grants no live tool, so it is excluded from
    ``granting`` and cannot make two audiences disagree. Only audiences
    that actually name live tools are compared.
    """
    mcp = getattr(chat, "mcp", None)
    live = getattr(mcp, "live_tools", None) if mcp is not None else None
    if live is None:
        return None
    allowed = tuple(live.names)
    shadowed = sorted(frozenset(allowed) & _graph_operator_universe())
    if shadowed:
        from okg.substrate.chat.bundle_composition import ChatBundleLiveToolConflict

        # S3 registered this code for a BUNDLE declaring one name under both
        # transports and recorded that a manifest could not reach it, because
        # `chat.mcp.tools` is closed to the operator universe. This slot is
        # the path that reaches it: `chat.mcp.live_tools.names` is deliberately
        # NOT closed to that universe -- a live tool is not an OKG operator --
        # so a manifest can now name a live tool at all, and the same defect
        # (one name, two transports, whichever binds wins silently) arrives
        # from the manifest. Same defect, same code.
        raise ChatBundleLiveToolConflict(
            f"deployment {deployment!r} declares {shadowed} under "
            "chat.mcp.live_tools.names, and each is already a graph operator "
            "name. One name cannot carry two transports: a chat client would "
            "hold two specs called the same thing, reaching different systems "
            "with different authority, and which one answered would depend on "
            "registration order.",
            data={
                "reason": "live_tool_shadows_graph_tool",
                "tool": shadowed[0],
                "deployment": deployment,
                "shadowed": shadowed,
            },
            fix_hint=(
                f"rename the live tool(s) {shadowed} to names no OKG operator "
                "uses"
            ),
        )
    granting = tuple(
        (a.name, tuple(a.live_tools))
        for a in (getattr(mcp, "audiences", ()) or ())
        if getattr(a, "live_tools", ())
    )
    for name, wanted in granting:
        undeclared = sorted(frozenset(wanted) - frozenset(allowed))
        if undeclared:
            raise ChatLiveToolSlotError(
                f"chat.mcp.audiences.{name}.live_tools grants {undeclared}, "
                "which chat.mcp.live_tools.names does not allow. The slot is "
                "the closed list of live tools this deployment permits; an "
                "audience may be given some of them and never more, because "
                "the grant is the narrower statement and widening it here "
                "would let an audience key add reach the slot never declared.",
                data={
                    "reason": "audience_grants_undeclared_live_tool",
                    "deployment": deployment,
                    "audience": name,
                    "allowed": list(allowed),
                    "granted": {a: list(t) for a, t in granting},
                },
                fix_hint=(
                    f"declare {undeclared} under chat.mcp.live_tools.names, or "
                    f"drop them from chat.mcp.audiences.{name}.live_tools"
                ),
            )
    distinct = {frozenset(wanted) for _, wanted in granting}
    if len(distinct) > 1:
        raise ChatLiveToolSlotError(
            f"deployment {deployment!r} grants DIFFERENT live-tool sets to "
            "different audiences over one live connection: "
            + "; ".join(f"{n}={sorted(t)}" for n, t in granting)
            + ". Open WebUI's per-tool filter belongs to the connection, so "
            "one live registration serves exactly one set -- the same reason "
            "chat.mcp.audiences may not split the graph connection. Refused "
            "with nothing applied, rather than projecting one audience's set "
            "and leaving the other silently wrong.",
            data={
                "reason": "two_audiences_one_live_connection",
                "deployment": deployment,
                "allowed": list(allowed),
                "granted": {a: list(t) for a, t in granting},
            },
            fix_hint=(
                "grant one live-tool set, or run a second live tool server "
                "and declare its own slot in a second deployment"
            ),
        )
    granted = tuple(sorted(next(iter(distinct)))) if distinct else ()
    return LiveToolSlot(
        server_id=f"{mcp_server_id}{LIVE_CONNECTION_SUFFIX}",
        url=live.url,
        allowed=allowed,
        granted=granted,
        audiences=tuple(sorted(name for name, _ in granting)),
        auth_token_env=live.auth_token_env,
    )


def assert_transports_disjoint(
    graph_filter: Sequence[str],
    slot: LiveToolSlot | None,
    *,
    deployment: str,
) -> None:
    """The two connections' filters share no name. Asserted BOTH ways.

    A cheap check at the point the payloads are built, on the values that
    actually reach the wire rather than on the manifest they came from. The
    refusals in :func:`resolve_live_tool_slot` should make it unreachable;
    that is exactly why it is here. A separation that holds only because a
    check two modules away happens to run is a separation one edit removes,
    and the thing it would remove is the reason this slice exists.

    ``graph_filter`` carries the vendor's ``!name`` deny entries as well as
    allow entries, so the ``!`` is stripped before comparing -- a live tool
    appearing as a DENY on the graph connection is still the graph
    connection knowing a live tool's name.
    """
    if slot is None:
        return
    # `removeprefix`, not `lstrip("!")`: `lstrip` strips a SET of characters
    # and would eat every leading `!`, so a hypothetical `!!name` would
    # compare as `name`. One deny marker, removed once.
    graph_names = frozenset(str(n).removeprefix("!") for n in graph_filter if n)
    overlap = sorted(graph_names & frozenset((*slot.allowed, *slot.granted)))
    if not overlap:
        return
    from okg.substrate.chat.bundle_composition import ChatBundleLiveToolConflict

    raise ChatBundleLiveToolConflict(
        f"deployment {deployment!r} would register {overlap} on BOTH the "
        f"graph connection and the live connection {slot.server_id!r}. Graph "
        "tools and live tools carry different authority and must never share "
        "a name or a connection.",
        data={
            "reason": "live_tool_shadows_graph_tool",
            "tool": overlap[0],
            "deployment": deployment,
            "shadowed": overlap,
        },
        fix_hint="rename the live tool(s) to names no OKG operator uses",
    )
