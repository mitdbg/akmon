"""Tests for the pre-join MCP framework resolver registry.

Per S-A7-R3:
- substrate ships `alias_table` and `identifier_pattern`
- deployments register additional resolvers; manifest entries
  must declare module + function and must not collide with a
  substrate built-in
- a tool referencing an unknown resolver slug surfaces
  PreJoinResolverUnknown and is omitted from the registration set
"""
from __future__ import annotations

import textwrap

import pytest

from okg.substrate.mcp.framework import (
    PreJoinResolverUnknown,
    PreJoinSchemaError,
    parse_pre_join_tools,
)
from okg.substrate.mcp.framework.resolvers import (
    ResolverRegistry,
    build_substrate_resolvers,
    validate_resolver_refs,
)


# Stub deployment-side resolver used by import tests
def _stub_resolver(text, ctx):  # pragma: no cover - imported lazily
    return f"node:{text}" if text else None


def test_substrate_builtins_are_alias_table_and_identifier_pattern() -> None:
    builtins = build_substrate_resolvers()
    assert set(builtins) == {"alias_table", "identifier_pattern"}
    assert callable(builtins["alias_table"])
    assert callable(builtins["identifier_pattern"])


def test_registry_seeds_substrate_builtins() -> None:
    reg = ResolverRegistry.with_substrate_builtins()
    assert "alias_table" in reg
    assert "identifier_pattern" in reg
    assert reg.get("alias_table") is not None


def test_manifest_registration_imports_callable() -> None:
    reg = ResolverRegistry.with_substrate_builtins()
    reg.register_from_manifest({
        "stub": {
            "module": "tests.substrate.mcp.framework.test_resolvers",
            "function": "_stub_resolver",
        },
    })
    assert "stub" in reg
    fn = reg.get("stub")
    assert fn is not None
    # Stub callable doesn't need a real ctx for this smoke check.
    assert fn("hello", None) == "node:hello"


def test_manifest_registration_rejects_collision_with_substrate_builtin() -> None:
    reg = ResolverRegistry.with_substrate_builtins()
    with pytest.raises(PreJoinSchemaError, match="collides"):
        reg.register_from_manifest({
            "alias_table": {
                "module": "tests.substrate.mcp.framework.test_resolvers",
                "function": "_stub_resolver",
            },
        })


def test_manifest_registration_fails_loud_on_unknown_module() -> None:
    reg = ResolverRegistry.with_substrate_builtins()
    with pytest.raises(PreJoinSchemaError, match="could not import"):
        reg.register_from_manifest({
            "ghost": {
                "module": "ghosts.do.not.import",
                "function": "anything",
            },
        })


def test_manifest_registration_fails_loud_on_missing_function() -> None:
    reg = ResolverRegistry.with_substrate_builtins()
    with pytest.raises(PreJoinSchemaError, match="no attribute"):
        reg.register_from_manifest({
            "no_function": {
                "module": "tests.substrate.mcp.framework.test_resolvers",
                "function": "this_function_does_not_exist",
            },
        })


def test_manifest_registration_requires_module_and_function() -> None:
    reg = ResolverRegistry.with_substrate_builtins()
    with pytest.raises(PreJoinSchemaError, match="missing `module`"):
        reg.register_from_manifest({"bad": {"function": "x"}})
    with pytest.raises(PreJoinSchemaError, match="missing `function`"):
        reg.register_from_manifest({"bad": {"module": "x"}})


def test_validate_resolver_refs_isolates_unknown_slug_tools() -> None:
    """A spec referencing an unknown resolver slug surfaces
    PreJoinResolverUnknown and is omitted from the passing set;
    sibling specs with known resolvers are not affected
    (S-A7-R3 negative scenario)."""
    yaml_text = textwrap.dedent("""\
        tools:
          - name: good_tool
            description: Uses substrate built-in.
            params:
              - {name: t, kind: text, required: true}
            bundle:
              - call: get_node
                args:
                  node_id: {resolve: alias_table, input: t}
                emit_as: row
          - name: bad_tool
            description: Uses an unknown resolver slug.
            params:
              - {name: t, kind: text, required: true}
            bundle:
              - call: get_node
                args:
                  node_id: {resolve: nowhere_resolver, input: t}
                emit_as: row
    """)
    import yaml
    specs, schema_failures = parse_pre_join_tools(yaml.safe_load(yaml_text))
    assert schema_failures == []
    reg = ResolverRegistry.with_substrate_builtins()
    passing, failing = validate_resolver_refs(specs, reg)
    passing_names = sorted(s.name for s in passing)
    failing_names = sorted(f.tool_name for f in failing)
    assert passing_names == ["good_tool"]
    assert failing_names == ["bad_tool"]
    assert isinstance(failing[0].error, PreJoinResolverUnknown)
    assert "nowhere_resolver" in str(failing[0].error)
