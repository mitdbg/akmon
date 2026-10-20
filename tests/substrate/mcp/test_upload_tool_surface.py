"""PACT chat-upload-source — task.chat-upload-source.chat-affordance.

The projection readback, taken off a REAL `build_server` rather than a
stub: the tool set a chat client would receive from `tools/list` is
`mcp._tool_manager._tools`, and these tests read exactly that after
building the server from a temporary deployment on disk. Registration
therefore travels the whole path — ``source_registry.yaml`` →
`provisioned_conditional_tools` → registration → boundary — instead of
being asserted at a seam.

Hermetic: ``OKG_MCP_SURFACE_PROBE=1`` builds both connection pools
unopened, and the DSN names a dead port so an accidental connection
attempt fails loudly instead of reaching a real graph.
"""

from __future__ import annotations

import contextlib
from pathlib import Path
from typing import Any, Iterator

import pytest
import yaml

# `mcp` is a CORE dependency (pyproject `dependencies`, not an extra), so a
# hard import is correct here. `importorskip` would let this whole
# projection readback vanish into a green run — the PACT asks for the
# tool's absence to be PROVEN, and a skipped absence test proves nothing.
import mcp as _mcp_pkg  # noqa: F401,E402
assert _mcp_pkg.__name__ == "mcp"

from okg.substrate.library.sources.upload import (  # noqa: E402
    upload_source_registry_entry,
)
from okg.substrate.mcp.operators import (  # noqa: E402
    CONDITIONAL_ADMIN_TOOLS,
    OPERATOR_TOOLS,
    WRITE_ADMIN_TOOLS,
)


#: Unreachable by construction (port 1). Under the surface probe nothing
#: dials it; naming a dead endpoint keeps an accidental connection loud.
_DEAD_DSN = "postgresql://postgres:postgres@127.0.0.1:1/nodb"


def _posture() -> dict[str, Any]:
    return {
        "deployment_class": "dogfood_workspace",
        "read_principals": ["human_operator", "agent"],
        "write_principals": ["human_operator", "source_adapter"],
        "mcp_write_default": "review",
        "export_policy": "internal_only",
        "secret_handling": "secret_ref_required",
        "live_call_default": "review",
        "allowed_source_sensitivities": ["local_private"],
        "allowed_data_classifications": ["private"],
        "allowed_exportability": ["no_export"],
        "privacy_obligations": ["audit", "no_export"],
        "readiness_profiles": ["dogfood"],
    }


def _make_deployment(root: Path, name: str, *, register_upload: bool) -> None:
    dep = root / name
    (dep / "schema").mkdir(parents=True, exist_ok=True)
    sources: dict[str, Any] = {}
    if register_upload:
        sources["upload"] = upload_source_registry_entry(
            ownership_id=f"{name}.upload",
        )
    (dep / "source_registry.yaml").write_text(
        yaml.safe_dump({"sources": sources}, sort_keys=True)
    )
    (dep / "deployment.yaml").write_text(
        yaml.safe_dump(
            {
                "name": name,
                "schema_dir": "schema",
                "source_registry": "source_registry.yaml",
                "postgres": {"dsn": _DEAD_DSN, "database": "nodb"},
                "nomos": {"deployment_posture": _posture()},
            },
            sort_keys=True,
        )
    )


@pytest.fixture
def deployments_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> Iterator[Path]:
    root = tmp_path / "deployments"
    root.mkdir()
    _make_deployment(root, "with-upload", register_upload=True)
    _make_deployment(root, "without-upload", register_upload=False)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    monkeypatch.setenv("OKG_MCP_SURFACE_PROBE", "1")
    monkeypatch.setenv("OKG_MCP_TRACE_ENABLED", "0")
    monkeypatch.delenv("OKG_MCP_ALLOW_WRITES", raising=False)
    monkeypatch.delenv("OKG_MCP_TOOL_ALLOWLIST", raising=False)
    yield root


@contextlib.contextmanager
def _built(deployment: str, *, allow_writes: bool):
    from okg.substrate.mcp.server import build_server

    mcp = build_server(
        dsn=_DEAD_DSN,
        deployment_name=deployment,
        allow_writes=allow_writes,
    )
    try:
        yield mcp
    finally:
        mcp._okg_pool.close()
        if getattr(mcp, "_okg_write_pool", None) is not None:
            mcp._okg_write_pool.close()


def test_registered_deployment_serves_the_upload_tool(
    deployments_root: Path,
) -> None:
    with _built("with-upload", allow_writes=True) as mcp:
        names = set(mcp._tool_manager._tools)
        assert "upload" in names
        assert names == set(OPERATOR_TOOLS) | set(WRITE_ADMIN_TOOLS) | {"upload"}
        assert mcp._okg_provisioned_conditional_tools == ("upload",)
        assert mcp._okg_unprovisioned_conditional_tools == ()
        assert mcp._okg_boundary_pruned_tools == ()
        assert mcp._okg_missing_admitted_tools == ()


def test_unregistered_deployment_serves_no_upload_tool(
    deployments_root: Path,
) -> None:
    """scn.chat-upload-source.unregistered-deployment-refuses, surface arm.

    Paired with the test above so the absence is attributable to the
    missing registration and not to the tool never existing.
    """
    with _built("without-upload", allow_writes=True) as mcp:
        names = set(mcp._tool_manager._tools)
        assert "upload" not in names
        assert names == set(OPERATOR_TOOLS) | set(WRITE_ADMIN_TOOLS)
        assert mcp._okg_provisioned_conditional_tools == ()
        assert mcp._okg_unprovisioned_conditional_tools == ("upload",)
        # Not a defect: an unprovisioned conditional tool must not read as
        # an admitted tool that failed to register.
        assert mcp._okg_missing_admitted_tools == ()


def test_read_only_launch_serves_no_upload_tool_even_when_registered(
    deployments_root: Path,
) -> None:
    """The affordance rides the write/admin surface. A read-only server on
    a fully registered deployment still offers nothing to upload with —
    and says so with the RIGHT reason: the deployment provisions the tool,
    this server withholds it. Reporting the deployment as unprovisioned
    here would be a false statement about the deployment."""
    with _built("with-upload", allow_writes=False) as mcp:
        assert set(mcp._tool_manager._tools) == set(OPERATOR_TOOLS)
        assert mcp._okg_provisioned_conditional_tools == ("upload",)
        assert mcp._okg_unprovisioned_conditional_tools == ()
        assert mcp._okg_conditional_tools_withheld_read_only == ("upload",)


def test_read_only_launch_on_unregistered_deployment_reports_absence(
    deployments_root: Path,
) -> None:
    """The partner reading: nothing withheld, everything unprovisioned."""
    with _built("without-upload", allow_writes=False) as mcp:
        assert set(mcp._tool_manager._tools) == set(OPERATOR_TOOLS)
        assert mcp._okg_unprovisioned_conditional_tools == tuple(
            sorted(CONDITIONAL_ADMIN_TOOLS)
        )
        assert mcp._okg_conditional_tools_withheld_read_only == ()
        reasons = mcp._okg_conditional_provisioning_reasons
        assert reasons["upload"]["code"] == "upload_source_not_registered"


def test_a_malformed_registry_is_distinguishable_from_an_absent_one(
    deployments_root: Path,
) -> None:
    """Deny-by-default is right, but the two denials must not read alike:
    a broken registry is a defect, an absent registration is not."""
    (deployments_root / "with-upload" / "source_registry.yaml").write_text(
        "sources: [this is not a mapping]\n"
    )
    with _built("with-upload", allow_writes=True) as mcp:
        assert "upload" not in mcp._tool_manager._tools
        reason = mcp._okg_conditional_provisioning_reasons["upload"]
        assert reason["code"] in {
            "provisioning_error",
            "upload_source_not_registered",
        }
        assert reason["detail"]


def test_upload_tool_description_states_the_interim_identity_posture(
    deployments_root: Path,
) -> None:
    """A pre-principal-mapping interim must say so in the tool description
    rather than posing as the final shape (PACT ordering note)."""
    with _built("with-upload", allow_writes=True) as mcp:
        tool = mcp._tool_manager._tools["upload"]
        description = str(getattr(tool, "description", "") or "")
        assert "INTERIM" in description
        assert "mcp-principal-mapping" in description
        assert "uploaded_by" in description
        # The egress posture is stated where the caller reads it.
        assert "egress guard" in description


def test_upload_tool_parameters_are_derived_from_the_signature(
    deployments_root: Path,
) -> None:
    with _built("with-upload", allow_writes=True) as mcp:
        params = mcp._tool_manager._tools["upload"].parameters["properties"]
        assert set(params) >= {
            "uploaded_by",
            "filename",
            "content_base64",
            "url",
            "deployment",
        }
