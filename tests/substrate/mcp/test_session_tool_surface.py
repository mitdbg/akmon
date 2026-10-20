"""Per-session tool projection on the substrate MCP server (issue #1458).

Three concerns, deliberately separated:

* the projection itself — a data structure, provable without a server;
* the launch contract — which ceilings a real `build_server` accepts and
  refuses, and what it then advertises;
* the substrate server's own refusal envelope for a tool a session cannot
  see, which the end-to-end test cannot observe because a bare FastMCP has no
  such envelope.

The wire behavior — the notification frame, re-listing, calling the newly
exposed tool, and two concurrent sessions not seeing each other — is measured
against real server processes in `test_tool_list_expansion_e2e.py`. Nothing
here duplicates that; a test that only exercised Python objects would prove
none of it.
"""
from __future__ import annotations

from tests.substrate.mcp._result_payload import (
    result_text as _result_text,
)

import asyncio
import pytest

from pathlib import Path

from okg.substrate.mcp.operators import OPERATOR_TOOLS
from okg.substrate.mcp.session_tool_surface import (
    SessionScopedFastMCP,
    SessionToolSurface,
    StatelessHttpExpansionUnsupported,
    ToolSurfaceExpansionUnavailable,
    install_session_tool_projection,
)
from okg.substrate.mcp.tool_profiles import (
    BUILTIN_TOOL_PROFILES,
    ToolSurfaceExpansionAllowlistConflict,
    ToolSurfaceExpansionInvalid,
    assert_expansion_is_coherent,
    resolve_expansion_ceiling,
    resolve_tool_surface,
)


class _FakeSession:
    """A weak-referenceable, hashable stand-in for an SDK ServerSession."""


# ---------- the projection ----------


def _surface() -> SessionToolSurface:
    return SessionToolSurface(
        base=frozenset({"inspect", "search"}),
        ceiling=frozenset({"inspect", "search", "expand", "query"}),
        base_profile="base",
        ceiling_profile="ceiling",
    )


def test_an_unknown_session_sees_the_base_surface() -> None:
    """The default has to be the MINIMAL set, not the last one asked for.

    A session that inherited a widening it never requested is exactly the
    process-wide leak issue #1458 exists to prevent.
    """
    surface = _surface()
    assert surface.visible_for(_FakeSession()) == surface.base
    assert surface.visible_for(None) == surface.base


def test_expanding_one_session_leaves_every_other_on_the_base() -> None:
    surface = _surface()
    widened, untouched = _FakeSession(), _FakeSession()

    outcome = surface.expand(widened)

    assert outcome.status == "expanded"
    assert set(outcome.added_tools) == {"expand", "query"}
    assert surface.visible_for(widened) == surface.ceiling
    assert surface.visible_for(untouched) == surface.base
    assert surface.visible_for(_FakeSession()) == surface.base


def test_a_repeat_expansion_reports_that_nothing_changed() -> None:
    """Idempotent, and says so: the notification is sent only on a change."""
    surface = _surface()
    session = _FakeSession()
    surface.expand(session)

    repeat = surface.expand(session)

    assert repeat.status == "already_expanded"
    assert repeat.added_tools == ()


def test_only_one_caller_can_claim_a_widening_notification() -> None:
    """One widening owes exactly one frame, however many callers saw it.

    `call_tool` yields at its await, so two concurrent calls in one session
    can both read the narrow set before either widens it. Deriving the
    notification from a before/after comparison there would emit two frames
    for one change; the claim is taken from the registry instead.
    """
    surface = _surface()
    session = _FakeSession()

    surface.expand(session)
    surface.expand(session)

    assert surface.take_pending_notification(session) is True
    assert surface.take_pending_notification(session) is False


def test_expansion_without_a_session_identity_refuses() -> None:
    """Stateless transports report no identity, and a widening recorded
    against a per-request object would evaporate before the next call."""
    with pytest.raises(ToolSurfaceExpansionUnavailable):
        _surface().expand(None)


def test_a_session_state_does_not_keep_its_session_alive() -> None:
    """Weak-keyed, like the logical-session registry: a long-lived server
    must not accumulate one state per session that ever connected."""
    import gc

    surface = _surface()
    session = _FakeSession()
    surface.expand(session)
    assert len(surface) == 1

    del session
    gc.collect()

    assert len(surface) == 0


def test_a_base_outside_the_ceiling_is_a_construction_error() -> None:
    with pytest.raises(ValueError):
        SessionToolSurface(base=frozenset({"query"}), ceiling=frozenset({"inspect"}))


# ---------- the launch contract ----------


def test_a_ceiling_on_an_unnarrowed_launch_refuses() -> None:
    """Nothing is withheld, so nothing could be expanded into, and the
    server would advertise a tool list it can never change."""
    with pytest.raises(ToolSurfaceExpansionInvalid) as excinfo:
        resolve_expansion_ceiling(
            "reader",
            base=resolve_tool_surface(requested_profile=None, env_allowlist=None),
        )
    assert "narrows nothing" in str(excinfo.value)


def test_a_ceiling_over_an_environment_allowlist_refuses() -> None:
    """The env allowlist is a per-tool boundary, and the ceiling is what the
    server REGISTERS, so accepting both served the ceiling and discarded the
    allowlist entirely: `OKG_MCP_TOOL_ALLOWLIST=inspect` with
    `--tools-ceiling reader` registered all seven read tools."""
    with pytest.raises(ToolSurfaceExpansionAllowlistConflict) as excinfo:
        resolve_expansion_ceiling(
            "reader",
            base=resolve_tool_surface(
                requested_profile=None, env_allowlist={"inspect"},
            ),
        )
    # Both selectors are named, so the operator can see which one to drop.
    assert "OKG_MCP_TOOL_ALLOWLIST=inspect" in str(excinfo.value)
    assert "reader" in str(excinfo.value)
    assert excinfo.value.data["env_allowlist"] == ["inspect"]
    # Still catchable as the general expansion refusal it specializes.
    assert isinstance(excinfo.value, ToolSurfaceExpansionInvalid)


def test_a_ceiling_equal_to_the_base_refuses() -> None:
    with pytest.raises(ToolSurfaceExpansionInvalid) as excinfo:
        assert_expansion_is_coherent(
            base_tools={"inspect", "search"},
            ceiling_tools={"inspect", "search"},
            base_profile="a",
            ceiling_profile="b",
        )
    assert "no session could ever widen" in str(excinfo.value)


def test_a_ceiling_narrower_than_the_base_refuses() -> None:
    """A ceiling may only widen. Taking a tool away mid-session would
    withdraw something a caller was already told it had."""
    with pytest.raises(ToolSurfaceExpansionInvalid):
        assert_expansion_is_coherent(
            base_tools=set(OPERATOR_TOOLS),
            ceiling_tools=set(BUILTIN_TOOL_PROFILES["discovery"]),
            base_profile="reader",
            ceiling_profile="discovery",
        )


def test_a_base_without_inspect_refuses_because_expansion_is_unreachable() -> None:
    with pytest.raises(ToolSurfaceExpansionInvalid) as excinfo:
        assert_expansion_is_coherent(
            base_tools={"search", "aggregate"},
            ceiling_tools=set(OPERATOR_TOOLS),
            base_profile="no-door",
            ceiling_profile="reader",
        )
    assert "unreachable expansion" in str(excinfo.value)


# ---------- a real (database-free) substrate server ----------
#
# `OKG_MCP_SURFACE_PROBE` builds both pools UNOPENED, so `build_server`
# registers the real surface against an unreachable DSN with no database.


@pytest.fixture
def probe(monkeypatch: pytest.MonkeyPatch):
    from okg.substrate.mcp import server as server_mod
    from okg.substrate.mcp.surface_budget import PROBE_DSN

    monkeypatch.setenv("OKG_MCP_SURFACE_PROBE", "1")
    monkeypatch.setenv("OKG_MCP_TRACE_ENABLED", "0")
    monkeypatch.delenv("OKG_MCP_TOOL_ALLOWLIST", raising=False)
    monkeypatch.delenv("OKG_MCP_ALLOW_WRITES", raising=False)
    monkeypatch.setattr(
        server_mod,
        "_server_dsn_resolution",
        lambda _dsn, _name: {
            "dsn": PROBE_DSN, "authority": {"source": "surface_probe_stub"},
        },
    )

    built: list[object] = []

    def _build(**kwargs):
        from okg.substrate.mcp.server import build_server

        mcp = build_server(dsn=PROBE_DSN, **kwargs)
        built.append(mcp)
        return mcp

    try:
        yield _build
    finally:
        for mcp in built:
            mcp._okg_pool.close()


def _capabilities(mcp) -> object:
    return mcp._mcp_server.create_initialization_options().capabilities


def test_a_launch_without_a_ceiling_is_unchanged(probe) -> None:
    """The whole mechanism is inert by default.

    `tools.listChanged` stays false, because this server's tool list really
    cannot change, and no projection is installed, so `list_tools` and
    `call_tool` keep the SDK's own path.
    """
    mcp = probe()

    assert _capabilities(mcp).tools.listChanged is False
    assert mcp._okg_session_tool_surface_registry is None
    assert mcp._okg_session_tool_surface is None
    surface = mcp._okg_surface_freshness["tool_surface"]
    assert surface["profile_change_requires_restart"] is True
    assert surface["tool_list_changed_advertised"] is False
    assert surface["expansion"]["available"] is False


def test_a_ceiling_registers_wide_and_starts_narrow(probe) -> None:
    mcp = probe(tool_profile="discovery", tool_profile_ceiling="reader")

    assert mcp._okg_session_base_tools == ("inspect", "search", "aggregate")
    assert set(mcp._okg_served_tools) == set(OPERATOR_TOOLS)
    assert _capabilities(mcp).tools.listChanged is True

    registry = mcp._okg_session_tool_surface_registry
    assert registry is not None
    assert registry.base == frozenset({"inspect", "search", "aggregate"})
    assert registry.ceiling == frozenset(OPERATOR_TOOLS)


def test_the_listing_a_session_receives_is_the_base_not_the_registry(
    probe,
) -> None:
    """Registration is wide; the listing is not. Without this the ceiling
    would simply be a wider launch."""
    mcp = probe(tool_profile="discovery", tool_profile_ceiling="reader")

    listed = {tool.name for tool in asyncio.run(mcp.list_tools())}

    assert listed == {"inspect", "search", "aggregate"}


def test_a_hidden_tool_call_is_a_typed_refusal_not_an_execution(probe) -> None:
    """Filtering only the LISTING would be cosmetic: a caller that guessed
    the name would still run the tool."""
    mcp = probe(tool_profile="discovery", tool_profile_ceiling="reader")

    result = asyncio.run(mcp.call_tool("query", {"sql": "SELECT 1"}))

    text = _result_text(result)
    assert result.isError is True
    # The typed code, what the session CAN call, what it could reach, and the
    # action that would get it there. A refusal that named none of these
    # would leave a caller unable to tell a withheld tool from a missing one.
    assert "mcp_tool_not_in_session_surface" in text
    assert "expandable_tools" in text
    # Model-side: the one text block the model reads (issue #3059). A value
    # holding a quote prints as a JSON string, so its quotes are escaped.
    assert '\\"action\\": \\"expand\\"' in text


def test_the_instructions_describe_the_starting_surface_and_the_way_up(
    probe,
) -> None:
    """Instructions naming a tool the session cannot yet call are the defect
    the profile work exists to prevent; silence about the wider set would
    make the ceiling unusable."""
    mcp = probe(tool_profile="discovery", tool_profile_ceiling="reader")

    instructions = mcp.instructions or ""

    assert "notifications/tools/list_changed" in instructions
    assert '"action": "expand"' in instructions


def test_the_surface_readback_reports_the_expansion_contract(probe) -> None:
    mcp = probe(tool_profile="discovery", tool_profile_ceiling="reader")

    surface = mcp._okg_surface_freshness["tool_surface"]

    assert surface["profile_change_requires_restart"] is False
    assert surface["tool_list_changed_advertised"] is True
    assert surface["expansion"]["available"] is True
    assert surface["expansion"]["ceiling_profile"] == "reader"
    assert set(surface["expansion"]["expandable_tools"]) == {
        "expand", "filter", "map", "query",
    }


def test_the_schema_cache_survives_a_per_session_listing(probe) -> None:
    """The SDK's `_tool_cache` is process-wide and every `tools/list`
    rebuilds it, so under a per-session listing a tool's schema validation
    would otherwise depend on which session listed last. Definitions come
    from the full registry instead."""
    mcp = probe(tool_profile="discovery", tool_profile_ceiling="reader")

    asyncio.run(mcp.list_tools())  # a narrow listing, as a base session makes
    definition = asyncio.run(
        mcp._mcp_server._get_cached_tool_definition("query")
    )

    assert definition is not None
    assert definition.name == "query"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"tool_profile_ceiling": "reader"},
        {"tool_profile": "reader", "tool_profile_ceiling": "reader"},
        {"tool_profile": "reader", "tool_profile_ceiling": "discovery"},
    ],
    ids=["no-base-narrowing", "ceiling-equals-base", "ceiling-narrower"],
)
def test_an_incoherent_ceiling_refuses_the_launch(probe, kwargs) -> None:
    with pytest.raises(ToolSurfaceExpansionInvalid):
        probe(**kwargs)


def _write_deployment(root: Path, *, name: str, body: str) -> None:
    directory = root / name
    (directory / "schemas").mkdir(parents=True)
    (directory / "source_registry.yaml").write_text("", encoding="utf-8")
    (directory / "deployment.yaml").write_text(
        f"name: {name}\n"
        "description: test\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n" + body,
        encoding="utf-8",
    )


def test_an_unreachable_ceiling_refuses_the_launch(
    probe, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`inspect` is the tool that widens a session; without it the ceiling
    could never be reached.

    The base is a deployment-declared profile rather than
    `OKG_MCP_TOOL_ALLOWLIST`, because a ceiling over an env allowlist is now
    refused one step earlier (see the allowlist conflict above) and would
    never reach this arm.
    """
    root = tmp_path / "deployments"
    _write_deployment(
        root,
        name="nodoor",
        body=(
            "mcp:\n"
            "  tool_profiles:\n"
            "    nodoor:\n"
            "      tools: [search, aggregate]\n"
            "  default_tool_profile: nodoor\n"
        ),
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))

    with pytest.raises(ToolSurfaceExpansionInvalid) as excinfo:
        probe(deployment_name="nodoor", tool_profile_ceiling="reader")
    assert "unreachable expansion" in str(excinfo.value)


def test_a_ceiling_over_an_environment_allowlist_refuses_the_launch(
    probe, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The blocking review finding, at the launch boundary.

    Measured before the fix: `OKG_MCP_TOOL_ALLOWLIST=inspect` with
    `--tools-ceiling reader` started a server that registered all
    seven read tools, let any session widen to all seven, and reported
    `_okg_tool_allowlist` as the ceiling rather than the value the operator
    set. The allowlist is a hard boundary; a flag must not widen past it.
    """
    monkeypatch.setenv("OKG_MCP_TOOL_ALLOWLIST", "inspect")
    with pytest.raises(ToolSurfaceExpansionAllowlistConflict):
        probe(tool_profile_ceiling="reader")


# ---------- the two error paths the projection must not strand ----------


class _RecordingSession:
    """A weak-referenceable session that counts the frames sent to it."""

    def __init__(self) -> None:
        self.frames = 0

    async def send_tool_list_changed(self) -> None:
        self.frames += 1


class _FixedSessionFastMCP(SessionScopedFastMCP):
    """A real projected server whose session identity is supplied directly.

    The identity normally comes from the in-flight SDK request context, which
    a direct `call_tool` has none of. Everything else -- the projection, the
    notification claim, the error path -- is the production code.
    """

    def __init__(self, session: object, **kwargs) -> None:
        super().__init__("expansion-error-path", **kwargs)
        self._okg_fixed_session = session

    def okg_current_session(self) -> object | None:
        return self._okg_fixed_session


def test_a_failing_call_still_delivers_the_widening_notification() -> None:
    """The widening is committed before the call returns, so a raise after it
    must not strand the frame.

    Before this fix the claim survived unsent until the session's next
    SUCCESSFUL call -- which a client holding a stale tool list may never
    make, because the tool it wants to call is the one it cannot see.
    """
    session = _RecordingSession()
    surface = SessionToolSurface(
        base=frozenset({"widen"}), ceiling=frozenset({"widen", "wide_only"}),
    )
    server = _FixedSessionFastMCP(session)

    @server.tool()
    def widen() -> str:
        surface.expand(session)
        raise RuntimeError("a downstream failure after the widening")

    @server.tool()
    def wide_only() -> str:
        return "wide"

    install_session_tool_projection(server, surface)

    with pytest.raises(Exception):  # noqa: B017 - the SDK's own ToolError type
        asyncio.run(server.call_tool("widen", {}))

    assert session.frames == 1
    assert surface.take_pending_notification(session) is False
    assert surface.visible_for(session) == surface.ceiling


def test_a_ceiling_on_a_stateless_http_server_refuses_the_launch() -> None:
    """Stateless HTTP discards the session after every request, so no
    session could widen and no frame could be sent -- while every client
    would be told at initialize that this server sends them."""
    surface = SessionToolSurface(
        base=frozenset({"inspect"}),
        ceiling=frozenset({"inspect", "query"}),
        base_profile="discovery",
        ceiling_profile="reader",
    )
    server = SessionScopedFastMCP("stateless-probe", stateless_http=True)

    with pytest.raises(StatelessHttpExpansionUnsupported) as excinfo:
        install_session_tool_projection(server, surface)

    assert excinfo.value.data["ceiling_profile"] == "reader"
    # Refused BEFORE anything was installed or advertised, so the half-built
    # server cannot serve the protocol lie either.
    assert server._okg_session_tool_surface is None
    capabilities = server._mcp_server.create_initialization_options().capabilities
    assert getattr(getattr(capabilities, "tools", None), "listChanged", False) is False
