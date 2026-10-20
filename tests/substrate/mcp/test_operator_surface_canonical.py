"""Regression test: the substrate MCP server exposes ONLY the
canonical AKMON operator surface by default. The explicit write/admin
launch path adds the source-admin/setup and write-effect tools, but
deployment-extension and generated-program declarations remain outside
this registry. The 22 legacy compatibility aliases that
``shrink-mcp-to-operator-surface`` removed MUST NOT be registered.

This test is the load-bearing contract for the canonical-surface
invariant: any change that re-registers a legacy tool, re-adds the
``tool_surface`` filter machinery, or accidentally exposes a removed
operator alias will fail this test.
"""
from __future__ import annotations

import pytest

from okg.substrate.mcp.operators import (
    CURRENT_TOOL_MIGRATIONS,
    OPERATOR_TOOLS,
    WRITE_ADMIN_TOOLS,
)


pytestmark = pytest.mark.postgres


def test_default_server_registers_only_operator_tools(
    clean_runtime: str,
) -> None:
    """With no deployment-side extensions loaded, the registered tool
    set MUST equal ``OPERATOR_TOOLS``.
    """
    pytest.importorskip("mcp")
    from okg.substrate.mcp.server import build_server

    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        registered = set(mcp._tool_manager._tools.keys())  # type: ignore[attr-defined]
        assert registered == set(OPERATOR_TOOLS), (
            f"non-canonical tools leaked into the operator surface: "
            f"{sorted(registered - set(OPERATOR_TOOLS))} "
            f"(canonical set is {sorted(OPERATOR_TOOLS)})"
        )
        assert mcp._okg_boundary_pruned_tools == ()  # type: ignore[attr-defined]
        assert mcp._okg_missing_admitted_tools == ()  # type: ignore[attr-defined]
    finally:
        mcp._okg_pool.close()  # type: ignore[attr-defined]


def test_no_legacy_alias_is_registered(clean_runtime: str) -> None:
    """Every name in ``CURRENT_TOOL_MIGRATIONS`` MUST NOT appear in
    the registered tool set. The migration table is the historical
    record of what was removed; the operator surface owes nothing to
    it.
    """
    pytest.importorskip("mcp")
    from okg.substrate.mcp.server import build_server

    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        registered = set(mcp._tool_manager._tools.keys())  # type: ignore[attr-defined]
        leaked = registered & set(CURRENT_TOOL_MIGRATIONS)
        assert leaked == set(), (
            f"legacy compatibility aliases are still registered: "
            f"{sorted(leaked)} (these were removed by "
            f"shrink-mcp-to-operator-surface)"
        )
    finally:
        mcp._okg_pool.close()  # type: ignore[attr-defined]


def test_write_admin_launch_registers_non_default_tools(
    clean_runtime: str,
) -> None:
    """The broader write/admin surface is explicit: it appears only when the
    operator launches the server with write/admin consent.
    """
    pytest.importorskip("mcp")
    from okg.substrate.mcp.server import build_server

    mcp = build_server(
        dsn=clean_runtime,
        deployment_name="default",
        allow_writes=True,
    )
    try:
        registered = set(mcp._tool_manager._tools.keys())  # type: ignore[attr-defined]
        expected = set(OPERATOR_TOOLS) | set(WRITE_ADMIN_TOOLS)
        assert registered == expected, (
            f"unexpected tools leaked into the explicit write/admin "
            f"surface: {sorted(registered - expected)}"
        )
        assert mcp._okg_boundary_pruned_tools == ()  # type: ignore[attr-defined]
        assert mcp._okg_missing_admitted_tools == ()  # type: ignore[attr-defined]
    finally:
        mcp._okg_pool.close()  # type: ignore[attr-defined]
        if mcp._okg_write_pool:  # type: ignore[attr-defined]
            mcp._okg_write_pool.close()  # type: ignore[attr-defined]


def test_tool_surface_filter_machinery_is_gone() -> None:
    """The ``tool_surface`` filter (parameter, env var, helpers, and
    inspect-kind dispatch) MUST NOT come back. This test catches a
    future regression that tries to reintroduce the legacy surface-
    mode toggle.
    """
    from okg.substrate.mcp import operators, server

    # operators.py: filter functions removed.
    assert not hasattr(operators, "hidden_tools_for_operator_surface")
    assert not hasattr(operators, "normalize_surface_mode")
    assert not hasattr(operators, "tool_surface_catalog")
    assert not hasattr(operators, "VALID_SURFACE_MODES")

    # server.build_server: tool_surface kwarg removed.
    import inspect as _stdlib_inspect

    sig = _stdlib_inspect.signature(server.build_server)
    assert "tool_surface" not in sig.parameters, (
        "the `tool_surface` parameter on build_server was removed by "
        "shrink-mcp-to-operator-surface; do not re-add it"
    )
    sig_run = _stdlib_inspect.signature(server.run_stdio)
    assert "tool_surface" not in sig_run.parameters, (
        "the `tool_surface` parameter on run_stdio was removed by "
        "shrink-mcp-to-operator-surface; do not re-add it"
    )


def test_classify_tool_still_translates_legacy_names_for_tracing() -> None:
    """``classify_tool`` survives as a historical translator so trace
    events that name a legacy tool (e.g., replayed from a pre-cleanup
    OKG) still get a canonical operator label. The translation map
    MUST cover every name in ``CURRENT_TOOL_MIGRATIONS``.
    """
    from okg.substrate.mcp.operators import classify_tool

    for legacy_name, migration in CURRENT_TOOL_MIGRATIONS.items():
        operator, replacement = classify_tool(legacy_name)
        expected = migration.replacement_operator
        assert operator == expected, (
            f"classify_tool({legacy_name!r}) returned {operator!r} as "
            f"operator; expected {expected!r}"
        )
        assert replacement == expected, (
            f"classify_tool({legacy_name!r}) returned {replacement!r} "
            f"as replacement; expected {expected!r}"
        )

    # Canonical names classify to themselves with no replacement.
    for canonical in OPERATOR_TOOLS:
        operator, replacement = classify_tool(canonical)
        assert operator == canonical
        assert replacement is None
