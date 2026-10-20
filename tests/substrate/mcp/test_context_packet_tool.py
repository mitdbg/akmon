"""Tests for the ``generate_context_packet`` MCP tool wiring
(Layer 3B of ``add-portable-memex-deployments``).

Coverage (non-postgres):
  * ``_deployment_loads_module`` returns True/False based on
    the deployment manifest's ``modules:`` list.
  * The tool is NOT registered when reasoning is absent.
  * The tool is also NOT registered when reasoning is present; generated
    programs need a separately admitted invocation surface, so the helper
    is cataloged as skipped before any MCP decoration happens.

The full end-to-end test (calling the tool against a live
graph, checking ``okg.mcp_call_log`` rows) lives under a
postgres mark in a sibling test file because it requires
a real DB + Publisher cycle to seed nodes.
"""
from __future__ import annotations

from pathlib import Path

import pytest


def _write_minimal_deployment(
    root: Path, *, name: str, modules: list[str],
) -> None:
    """Write a minimal deployment.yaml that the manifest loader
    will accept. Only ``modules:`` matters for the gate test.
    """
    dep = root / name
    (dep / "schemas").mkdir(parents=True)
    (dep / "source_registry.yaml").write_text("", encoding="utf-8")
    modules_yaml = "\n".join(f"  - {m}" for m in modules)
    (dep / "deployment.yaml").write_text(
        f"name: {name}\n"
        f"description: gate test fixture\n"
        f"schema_dir: schemas\n"
        f"source_registry: source_registry.yaml\n"
        f"modules:\n{modules_yaml}\n",
        encoding="utf-8",
    )


def test_deployment_loads_module_returns_true_when_present(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.deployments import (
        deployment_loads_module as _deployment_loads_module,
    )

    deployments_root = tmp_path / "deployments"
    _write_minimal_deployment(
        deployments_root,
        name="with-reasoning",
        modules=["openspec", "reasoning"],
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(deployments_root))
    assert _deployment_loads_module("with-reasoning", "reasoning") is True
    assert _deployment_loads_module("with-reasoning", "openspec") is True


def test_deployment_loads_module_returns_false_when_absent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.deployments import (
        deployment_loads_module as _deployment_loads_module,
    )

    deployments_root = tmp_path / "deployments"
    _write_minimal_deployment(
        deployments_root,
        name="no-reasoning",
        modules=["openspec", "bookkeeping"],
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(deployments_root))
    assert _deployment_loads_module("no-reasoning", "reasoning") is False
    # And missing deployment returns False (does not raise).
    assert _deployment_loads_module(
        "does-not-exist", "reasoning",
    ) is False


def test_deployment_loads_module_returns_false_for_malformed_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.deployments import (
        deployment_loads_module as _deployment_loads_module,
    )

    deployments_root = tmp_path / "deployments"
    dep = deployments_root / "bad-manifest"
    dep.mkdir(parents=True)
    (dep / "deployment.yaml").write_text(
        "this is not valid yaml: : :", encoding="utf-8",
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(deployments_root))
    # Malformed manifest = treat as "module unavailable" rather
    # than raising — the gate stays safe and silent so manifest
    # bugs surface via the load path, not via tool registration.
    assert _deployment_loads_module("bad-manifest", "reasoning") is False


# ---- postgres-marked tests below (require `mcp` SDK + live DB) ----
#
# Importing `build_server` pulls in the `mcp` SDK; only run these
# when both are available. `mcp` is not in the substrate's base
# requirements; the operator installs it in an MCP-server-running
# environment.


def _maybe_skip_for_mcp() -> None:
    pytest.importorskip("mcp")


@pytest.mark.postgres
def test_build_server_omits_context_packet_when_reasoning_loaded(
    clean_runtime: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Smoke test: with `reasoning` in the deployment manifest,
    `build_server` loads the reasoning module but does not expose
    `generate_context_packet` on the default MCP boundary.
    """
    _maybe_skip_for_mcp()
    from okg.substrate.mcp.server import build_server

    deployments_root = tmp_path / "deployments"
    _write_minimal_deployment(
        deployments_root,
        name="with-reasoning",
        modules=["reasoning"],
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(deployments_root))

    mcp = build_server(
        dsn=clean_runtime,
        deployment_name="with-reasoning",
    )
    try:
        names = set(mcp._tool_manager._tools.keys())  # type: ignore[attr-defined]
        assert "generate_context_packet" not in names
        assert mcp._okg_reasoning_loaded is True  # type: ignore[attr-defined]
        assert mcp._okg_boundary_pruned_tools == ()  # type: ignore[attr-defined]
        skipped = mcp._okg_skipped_module_tools  # type: ignore[attr-defined]
        assert {
            "name": "generate_context_packet",
            "module": "reasoning",
            "boundary": "generated_program_internal",
            "reason": (
                "reasoning context packets require a separately reviewed "
                "generated-program MCP surface"
            ),
        } in skipped
    finally:
        mcp._okg_pool.close()  # type: ignore[attr-defined]


@pytest.mark.postgres
def test_build_server_omits_context_packet_when_reasoning_absent(
    clean_runtime: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without `reasoning`, the tool MUST NOT appear in the
    advertised tool list — per design.md "no tool exists but
    always errors".
    """
    _maybe_skip_for_mcp()
    from okg.substrate.mcp.server import build_server

    deployments_root = tmp_path / "deployments"
    _write_minimal_deployment(
        deployments_root,
        name="no-reasoning",
        modules=["openspec"],
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(deployments_root))

    mcp = build_server(
        dsn=clean_runtime,
        deployment_name="no-reasoning",
    )
    try:
        names = set(mcp._tool_manager._tools.keys())  # type: ignore[attr-defined]
        assert "generate_context_packet" not in names
        assert mcp._okg_reasoning_loaded is False  # type: ignore[attr-defined]
        assert mcp._okg_skipped_module_tools == ()  # type: ignore[attr-defined]
        assert mcp._okg_boundary_pruned_tools == ()  # type: ignore[attr-defined]
    finally:
        mcp._okg_pool.close()  # type: ignore[attr-defined]
