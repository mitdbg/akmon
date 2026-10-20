"""`apply_admitted_tool_surface` — boundary pruning, narrowing, readbacks.

PACT `mcp-tool-surface-profiles` (issue #1457), req.mcp.fail-closed-narrowing.

`tool_registration.py` had no dedicated test module: its behavior was covered
incidentally by the upload-provisioning tests, which exercise the conditional
arm only. This covers the narrowing arm directly — the one that used to drop
unknown names in silence.

Hermetic: the boundary logic only touches `_tool_manager._tools` and writes
readback attributes, so a stub stands in for FastMCP and no database is
involved.
"""
from __future__ import annotations

import pytest

from okg.substrate.mcp.operators import (
    CONDITIONAL_ADMIN_TOOLS,
    OPERATOR_TOOLS,
    WRITE_ADMIN_TOOLS,
)
from okg.substrate.mcp.tool_profiles import ToolProfileToolAbsent
from okg.substrate.mcp.tool_registration import apply_admitted_tool_surface


class _StubToolManager:
    def __init__(self, names: list[str]) -> None:
        self._tools = {name: object() for name in names}


class _StubServer:
    """Stands in for FastMCP: `apply_admitted_tool_surface` only ever
    touches `_tool_manager._tools` and writes readback attributes."""

    def __init__(self, names: list[str]) -> None:
        self._tool_manager = _StubToolManager(names)


def _read_only_server() -> _StubServer:
    return _StubServer([*OPERATOR_TOOLS, *WRITE_ADMIN_TOOLS])


# ---------- the boundary is unchanged ----------

def test_a_read_only_launch_serves_exactly_the_read_algebra() -> None:
    server = _read_only_server()
    apply_admitted_tool_surface(server, allow_writes=False)
    assert set(server._tool_manager._tools) == set(OPERATOR_TOOLS)
    assert server._okg_served_tools == tuple(sorted(OPERATOR_TOOLS))
    assert set(server._okg_boundary_pruned_tools) == set(WRITE_ADMIN_TOOLS)


def test_a_write_launch_serves_the_write_admin_set_too() -> None:
    server = _read_only_server()
    apply_admitted_tool_surface(server, allow_writes=True)
    assert set(server._tool_manager._tools) == (
        set(OPERATOR_TOOLS) | set(WRITE_ADMIN_TOOLS)
    )
    assert server._okg_missing_admitted_tools == ()


def test_a_profile_cannot_widen_past_the_read_only_boundary() -> None:
    """The narrowing runs AFTER boundary pruning, so naming a write tool
    cannot grant it. It refuses instead — the tool is genuinely not served,
    and pretending otherwise would make the profile a privilege door."""
    server = _read_only_server()
    with pytest.raises(ToolProfileToolAbsent):
        apply_admitted_tool_surface(
            server,
            allow_writes=False,
            tool_allowlist={"inspect", "record"},
            tool_profile="widen",
        )


# ---------- fail-closed narrowing ----------

def test_a_correct_narrowing_serves_exactly_the_named_tools() -> None:
    server = _read_only_server()
    apply_admitted_tool_surface(
        server,
        allow_writes=False,
        tool_allowlist={"inspect", "search", "aggregate"},
        tool_profile="discovery",
        tool_profile_source="cli",
    )
    assert server._okg_served_tools == ("aggregate", "inspect", "search")
    assert server._okg_tool_profile == "discovery"
    assert server._okg_tool_profile_source == "cli"
    assert set(server._okg_pruned_tools) == set(OPERATOR_TOOLS) - {
        "inspect", "search", "aggregate",
    }


def test_a_misspelled_tool_refuses_instead_of_narrowing_silently() -> None:
    """Regression pin for the measured defect: at dev 39ddd4a58f
    `OKG_MCP_TOOL_ALLOWLIST=inspect,serach` served only `inspect`, with no
    refusal and no warning, so a typo read as agreement."""
    server = _read_only_server()
    with pytest.raises(ToolProfileToolAbsent) as excinfo:
        apply_admitted_tool_surface(
            server, allow_writes=False, tool_allowlist={"inspect", "serach"},
        )
    assert "serach" in str(excinfo.value)


def test_the_refusal_happens_before_anything_is_pruned() -> None:
    """A refusal that had already mutated the registry would leave a
    half-narrowed server behind if a caller swallowed the error."""
    server = _read_only_server()
    with pytest.raises(ToolProfileToolAbsent):
        apply_admitted_tool_surface(
            server, allow_writes=False, tool_allowlist={"inspect", "serach"},
        )
    assert set(server._tool_manager._tools) == set(OPERATOR_TOOLS)


def test_no_narrowing_leaves_the_allowlist_readbacks_unset() -> None:
    """An unnarrowed server must not report a narrowing it did not apply."""
    server = _read_only_server()
    apply_admitted_tool_surface(server, allow_writes=False)
    assert not hasattr(server, "_okg_tool_allowlist")
    assert server._okg_tool_profile is None
    assert server._okg_tool_profile_source == "unnarrowed"


# ---------- conditional tools stay outside the narrowing story ----------

def test_an_unprovisioned_conditional_tool_is_not_a_narrowing_failure() -> None:
    """`upload` absent because the deployment does not provision it is not
    a defect and must not read as one."""
    server = _StubServer([*OPERATOR_TOOLS, *WRITE_ADMIN_TOOLS])
    apply_admitted_tool_surface(
        server, allow_writes=True, provisioned_conditional_tools=frozenset(),
    )
    assert server._okg_unprovisioned_conditional_tools == tuple(
        sorted(CONDITIONAL_ADMIN_TOOLS)
    )
    assert server._okg_missing_admitted_tools == ()


def test_a_narrowing_may_name_a_provisioned_conditional_tool() -> None:
    server = _StubServer([*OPERATOR_TOOLS, *WRITE_ADMIN_TOOLS, "upload"])
    apply_admitted_tool_surface(
        server,
        allow_writes=True,
        tool_allowlist={"inspect", "upload"},
        provisioned_conditional_tools=frozenset({"upload"}),
    )
    assert server._okg_served_tools == ("inspect", "upload")


def test_naming_an_unprovisioned_conditional_tool_refuses() -> None:
    """It is genuinely not served, so the narrowing cannot be honored — and
    the operator needs to know their deployment does not provision it."""
    server = _StubServer([*OPERATOR_TOOLS, *WRITE_ADMIN_TOOLS])
    with pytest.raises(ToolProfileToolAbsent) as excinfo:
        apply_admitted_tool_surface(
            server,
            allow_writes=True,
            tool_allowlist={"inspect", "upload"},
            provisioned_conditional_tools=frozenset(),
        )
    assert "upload" in str(excinfo.value)
