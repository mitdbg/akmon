"""Unit tests for `McpClient` (`agent-led-mcp-onboarding` Phase 2).

Drives the sync wrapper against the local `stub_mcp_server.py`
fixture, which speaks line-delimited JSON over stdio per the MCP
stdio transport spec. We exercise the four shapes the production
composition relies on:

  - initialize handshake (serverInfo + capabilities exposed)
  - tools/list (declared tools, cached after first call)
  - tools/call ok path (echo roundtrip)
  - tools/call error path (`isError: True` → McpServerError)
  - subprocess crash mid-call → McpServerError
  - missing-executable spawn failure → McpServerError(stage='spawn')
  - clean shutdown (subprocess exits when the `with` block exits)
"""
from __future__ import annotations

import importlib.metadata
import json
import sys
from pathlib import Path

import pytest

from okg.substrate.library.sources.mcp_composed.client import (
    McpClient, McpServerError,
)


STUB_PATH = Path(__file__).parent / "stub_mcp_server.py"


def _stub_cmd(*extra: str) -> list[str]:
    return [sys.executable, str(STUB_PATH), *extra]


def test_initialize_handshake() -> None:
    with McpClient(_stub_cmd()) as cli:
        info = cli.server_info
        assert info is not None
        assert info.get("name") == "stub-mcp-server"
        caps = cli.capabilities
        assert isinstance(caps, dict)
        assert "tools" in caps
        assert cli.instructions == "stub server instructions"


def test_list_tools_returns_declared_tools() -> None:
    with McpClient(_stub_cmd()) as cli:
        tools = cli.list_tools()
        names = {t["name"] for t in tools}
        assert names == {"echo", "boom"}


def test_list_tools_is_cached() -> None:
    with McpClient(_stub_cmd()) as cli:
        t1 = cli.list_tools()
        t2 = cli.list_tools()
        # We don't promise object identity — the SDK rebuilds
        # response objects on each call — but the cached return
        # must be equal AND the second access must not have
        # called the server.
        assert t1 == t2
        assert cli._tools_cache is t2


def test_call_tool_roundtrips_payload() -> None:
    with McpClient(_stub_cmd()) as cli:
        result = cli.call_tool("echo", {"payload": {"x": 1, "y": "two"}})
        assert result.get("isError") is False
        content = result.get("content") or []
        # echo always sends one text frame containing the JSON-
        # encoded payload — confirm we can round-trip it.
        text = next(
            c.get("text") for c in content
            if c.get("type") == "text"
        )
        assert json.loads(text) == {"x": 1, "y": "two"}


def test_call_tool_error_frame_raises() -> None:
    with McpClient(_stub_cmd()) as cli:
        with pytest.raises(McpServerError) as exc_info:
            cli.call_tool("boom", {})
        err = exc_info.value
        assert err.stage == "tool_call"
        assert "boom" in str(err)


def test_subprocess_crash_surfaces_as_mcp_server_error() -> None:
    cli = McpClient(_stub_cmd("--crash-on-tool"), subprocess_timeout=3.0)
    cli.__enter__()
    try:
        # The stub exits non-zero on the first tool call instead
        # of responding — the SDK's read stream observes the
        # subprocess closing stdout; the client surfaces it as
        # `McpServerError` rather than hanging or throwing an
        # internal SDK exception.
        with pytest.raises(McpServerError):
            cli.call_tool("echo", {"payload": {}})
    finally:
        cli.__exit__(None, None, None)


def test_clean_shutdown_terminates_subprocess() -> None:
    cli = McpClient(_stub_cmd())
    cli.__enter__()
    # The SDK manages the subprocess internally — confirm the
    # cleanup path runs without raising and clears the loop +
    # session state, ready for GC.
    cli.__exit__(None, None, None)
    assert cli._loop is None
    assert cli._session is None


def test_missing_executable_raises_spawn_error() -> None:
    cli = McpClient(["definitely-not-a-real-command-xyz123"])
    with pytest.raises(McpServerError) as exc_info:
        cli.__enter__()
    assert exc_info.value.stage == "spawn"


def test_empty_command_raises_spawn_error() -> None:
    cli = McpClient([])
    with pytest.raises(McpServerError) as exc_info:
        cli.__enter__()
    assert exc_info.value.stage == "spawn"
    assert "empty" in str(exc_info.value).lower()


def _pretend_okg_version(monkeypatch: pytest.MonkeyPatch, version: str) -> None:
    """Make the installed okg distribution report `version`."""
    real_version = importlib.metadata.version

    def _version(name: str) -> str:
        return version if name == "okg" else real_version(name)

    monkeypatch.setattr(importlib.metadata, "version", _version)


def test_client_identity_reports_the_installed_okg_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """okg announces its own release to MCP servers, read from the package."""
    _pretend_okg_version(monkeypatch, "9.8.7")

    default = McpClient(["unused"])._build_implementation()
    named = McpClient(
        ["unused"], client_info={"name": "okg-probe"},
    )._build_implementation()
    explicit = McpClient(
        ["unused"], client_info={"name": "okg-probe", "version": "4.5.6"},
    )._build_implementation()

    assert (default.name, default.version) == ("okg-substrate", "9.8.7")
    assert (named.name, named.version) == ("okg-probe", "9.8.7")
    assert (explicit.name, explicit.version) == ("okg-probe", "4.5.6")


class _RecordingClient:
    """Stands in for `McpClient`: records the identity, then refuses to start."""

    seen: list[dict] = []

    def __init__(self, command, **kwargs) -> None:
        _RecordingClient.seen.append(kwargs.get("client_info"))

    def __enter__(self):
        raise McpServerError("recorded; not started", stage="spawn")

    def __exit__(self, *_args) -> bool:
        return False


def test_okg_probes_announce_the_installed_okg_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The surface doctor and readiness probe carry no version of their own."""
    from okg.substrate import environment_readiness
    from okg.substrate.library.sources.mcp_composed import client as client_module
    from okg.substrate.mcp.surface_fleet import (
        RegisteredMcpServer,
        probe_registered_server,
    )

    _pretend_okg_version(monkeypatch, "9.8.7")
    _RecordingClient.seen = []
    monkeypatch.setattr(client_module, "McpClient", _RecordingClient)

    server = RegisteredMcpServer(
        name="okg", deployment="d", command=["okg-mcp"], env={}, source="test",
    )
    with pytest.raises(McpServerError):
        probe_registered_server(server, timeout=1.0)
    readiness = environment_readiness._probe_mcp_registration(
        None,
        registration={"ok": True, "command": ["okg-mcp"]},
        expected_release={},
        expected_generation_id=None,
        timeout_seconds=1.0,
    )
    assert readiness["status"] == "probe_failed"

    identities = [
        McpClient(["unused"], client_info=info)._build_implementation()
        for info in _RecordingClient.seen
    ]
    assert [(item.name, item.version) for item in identities] == [
        ("okg-surface-freshness-doctor", "9.8.7"),
        ("okg-environment-readiness", "9.8.7"),
    ]
