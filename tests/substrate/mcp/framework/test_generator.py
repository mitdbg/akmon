"""Tests for the MCP tool generator (deployment-load registration).

Per S-A7-R1 + R3 (registration + isolation scenarios):

- A deployment YAML's pre-join tools register as FastMCP-shaped
  callables at MCP server bootstrap; one tool per validated spec
  + one SkippedPreJoinTool per malformed entry.
- Tool request schemas are generated from the `params` block
  (kind -> JSON-schema type; `required` populated).
- Schema-invalid tools are isolated; valid ones still register
  in the same file.
- Tools referencing unknown resolver slugs are isolated with
  PreJoinResolverUnknown code.
- Deployment-registered resolvers are imported at bootstrap.
"""
from __future__ import annotations

import textwrap
from pathlib import Path
from typing import Any

import pytest

from okg.substrate.deployments import DeploymentManifest
from okg.substrate.mcp.framework import (
    PreJoinToolError,
    build_pre_join_tools_for_manifest,
    build_request_schema_for_tool,
)
from okg.substrate.mcp.framework.schema import (
    PreJoinBundleStep,
    PreJoinParam,
    PreJoinToolSpec,
)


# Stub resolver used by deployment-registered tests
def _stub_resolver(text, ctx):
    return f"node:{text}" if text else None


# A minimal manifest builder helper. We don't need the full
# DeploymentManifest surface — just the fields the generator
# reads.
def _build_manifest(
    *,
    tmp_path: Path,
    yaml_text: str,
    resolvers: dict[str, Any] | None = None,
) -> DeploymentManifest:
    pre_join = tmp_path / "mcp_pre_join_tools.yaml"
    pre_join.write_text(yaml_text)
    return DeploymentManifest(
        name="test",
        description="",
        deployment_dir=tmp_path,
        schema_dir=tmp_path / "schemas",
        source_registry=tmp_path / "source_registry.yaml",
        enricher_registry=None,
        taxonomy=None,
        aliases=None,
        mcp_pre_join_tools=pre_join,
        pre_join_resolvers=resolvers or {},
    )


def test_build_request_schema_for_tool_has_required_field() -> None:
    spec = PreJoinToolSpec(
        name="t",
        description="docs",
        params=(
            PreJoinParam(name="topic", kind="text", required=True),
            PreJoinParam(name="limit", kind="int", required=False),
            PreJoinParam(name="enabled", kind="bool", required=False),
        ),
        bundle=(
            PreJoinBundleStep(
                call="get_node",
                args={"node_id": {"resolve": "alias_table", "input": "topic"}},
                emit_as="row",
            ),
        ),
    )
    schema = build_request_schema_for_tool(spec)
    assert schema["type"] == "object"
    assert schema["properties"]["topic"]["type"] == "string"
    assert schema["properties"]["limit"]["type"] == "integer"
    assert schema["properties"]["enabled"]["type"] == "boolean"
    assert schema["required"] == ["topic"]
    assert schema["additionalProperties"] is False


def test_build_for_manifest_isolates_schema_failures(tmp_path: Path) -> None:
    """One valid + one invalid tool: valid one registers, invalid
    one ends up in `skipped` (S-A7-R1 isolation scenario)."""
    yaml_text = textwrap.dedent("""\
        tools:
          - name: good_tool
            description: Valid.
            params:
              - {name: t, kind: text, required: true}
            bundle:
              - call: get_node
                args:
                  node_id: {resolve: alias_table, input: t}
                emit_as: row
          - name: bad_tool
            description: Missing bundle.
    """)

    def _connect():  # pragma: no cover - never invoked in this test
        raise AssertionError("connection should not be opened during build")

    def _open_session(conn):  # pragma: no cover
        raise AssertionError(
            "session should not be opened during build"
        )

    manifest = _build_manifest(tmp_path=tmp_path, yaml_text=yaml_text)
    registered, skipped = build_pre_join_tools_for_manifest(
        manifest, connect=_connect, open_session=_open_session,
    )
    names = sorted(r.name for r in registered)
    assert names == ["good_tool"]
    skipped_names = sorted(
        (s.name or "") for s in skipped
    )
    assert skipped_names == ["bad_tool"]
    assert skipped[0].code == "pre_join_tool_schema_error"


def test_build_for_manifest_isolates_unknown_resolver(tmp_path: Path) -> None:
    """A tool referencing an unknown resolver slug is isolated
    with code `pre_join_tool_resolver_unknown` and the other
    tools still register (S-A7-R3 negative scenario)."""
    yaml_text = textwrap.dedent("""\
        tools:
          - name: good_tool
            description: Substrate built-in resolver.
            params:
              - {name: t, kind: text, required: true}
            bundle:
              - call: get_node
                args:
                  node_id: {resolve: alias_table, input: t}
                emit_as: row
          - name: bad_tool
            description: Unknown resolver.
            params:
              - {name: t, kind: text, required: true}
            bundle:
              - call: get_node
                args:
                  node_id: {resolve: ghost_resolver, input: t}
                emit_as: row
    """)

    def _connect():  # pragma: no cover
        raise AssertionError
    def _open_session(conn):  # pragma: no cover
        raise AssertionError

    manifest = _build_manifest(tmp_path=tmp_path, yaml_text=yaml_text)
    registered, skipped = build_pre_join_tools_for_manifest(
        manifest, connect=_connect, open_session=_open_session,
    )
    assert [r.name for r in registered] == ["good_tool"]
    assert [s.name for s in skipped] == ["bad_tool"]
    assert skipped[0].code == "pre_join_tool_resolver_unknown"


def test_build_for_manifest_loads_deployment_resolver(tmp_path: Path) -> None:
    """Deployment-registered resolvers (resolvers: in
    deployment.yaml) are imported at bootstrap and become valid
    references in pre-join YAML (S-A7-R3)."""
    yaml_text = textwrap.dedent("""\
        tools:
          - name: stub_tool
            description: Uses deployment resolver.
            params:
              - {name: t, kind: text, required: true}
            bundle:
              - call: get_node
                args:
                  node_id: {resolve: stub, input: t}
                emit_as: row
    """)
    manifest = _build_manifest(
        tmp_path=tmp_path,
        yaml_text=yaml_text,
        resolvers={
            "stub": {
                "module": "tests.substrate.mcp.framework.test_generator",
                "function": "_stub_resolver",
            }
        },
    )

    def _connect():  # pragma: no cover
        raise AssertionError
    def _open_session(conn):  # pragma: no cover
        raise AssertionError

    registered, skipped = build_pre_join_tools_for_manifest(
        manifest, connect=_connect, open_session=_open_session,
    )
    assert [r.name for r in registered] == ["stub_tool"]
    assert skipped == []


def test_build_for_manifest_returns_empty_when_no_yaml(tmp_path: Path) -> None:
    manifest = DeploymentManifest(
        name="test",
        description="",
        deployment_dir=tmp_path,
        schema_dir=tmp_path / "schemas",
        source_registry=tmp_path / "source_registry.yaml",
        enricher_registry=None,
        taxonomy=None,
        aliases=None,
        mcp_pre_join_tools=None,
        pre_join_resolvers={},
    )

    def _connect():  # pragma: no cover
        raise AssertionError
    def _open_session(conn):  # pragma: no cover
        raise AssertionError

    registered, skipped = build_pre_join_tools_for_manifest(
        manifest, connect=_connect, open_session=_open_session,
    )
    assert registered == []
    assert skipped == []


def test_registered_callable_signature_only_accepts_declared_params(
    tmp_path: Path,
) -> None:
    """The generated tool function filters out any kwargs that
    weren't declared in `params:`. Defensive against FastMCP
    schema-bypass scenarios."""
    yaml_text = textwrap.dedent("""\
        tools:
          - name: smoke_tool
            description: A tool whose call we never actually run end-to-end here.
            params:
              - {name: topic, kind: text, required: true}
            bundle:
              - call: get_node
                args:
                  node_id: {resolve: alias_table, input: topic}
                emit_as: row
    """)
    manifest = _build_manifest(tmp_path=tmp_path, yaml_text=yaml_text)

    # We pass a connect() that raises so we know the callable
    # would attempt to open a connection. The required-param
    # validation in execute_pre_join_tool catches the missing
    # `topic` BEFORE connect() runs, so the missing-param failure
    # arrives without DB activity.
    def _connect():  # pragma: no cover - reached only via PreJoinToolError early-out below
        raise AssertionError(
            "connect() called even though required param missing"
        )
    def _open_session(conn):  # pragma: no cover
        raise AssertionError

    registered, _ = build_pre_join_tools_for_manifest(
        manifest, connect=_connect, open_session=_open_session,
    )
    assert len(registered) == 1
    fn = registered[0].callable
    with pytest.raises(PreJoinToolError, match="topic"):
        fn(other="ignored")  # `other` is not a declared param
