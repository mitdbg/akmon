from __future__ import annotations

import pytest

from okg.substrate.mcp.operators import (
    ALL_BUILTIN_TOOLS,
    AKMON_READ_TOOLS,
    CURRENT_TOOL_MIGRATIONS,
    OPERATOR_METADATA,
    OPERATOR_TOOLS,
    QUERY_ESCAPE_TOOLS,
    SOURCE_ADMIN_TOOLS,
    WRITE_EFFECT_TOOLS,
    classify_tool,
    migration_rows,
)


def test_every_builtin_tool_has_metadata() -> None:
    assert set(OPERATOR_METADATA) == set(ALL_BUILTIN_TOOLS)
    for name in ALL_BUILTIN_TOOLS:
        metadata = OPERATOR_METADATA[name]
        assert metadata.name == name
        assert metadata.boundary
        assert metadata.pin_behavior
        assert metadata.layer


def test_default_operator_tools_are_read_surface_only() -> None:
    assert OPERATOR_TOOLS == AKMON_READ_TOOLS + QUERY_ESCAPE_TOOLS
    assert {OPERATOR_METADATA[name].layer for name in AKMON_READ_TOOLS} == {
        "core_read",
    }
    assert {OPERATOR_METADATA[name].layer for name in QUERY_ESCAPE_TOOLS} == {
        "query_escape",
    }
    assert set(OPERATOR_TOOLS).isdisjoint(WRITE_EFFECT_TOOLS)
    assert set(OPERATOR_TOOLS).isdisjoint(SOURCE_ADMIN_TOOLS)


def test_non_default_tools_are_not_core_algebra() -> None:
    assert {OPERATOR_METADATA[name].layer for name in WRITE_EFFECT_TOOLS} == {
        "write_effect",
    }
    assert {OPERATOR_METADATA[name].layer for name in SOURCE_ADMIN_TOOLS} == {
        "source_admin",
    }


def test_current_substrate_tools_have_migration_targets() -> None:
    """The migration map records every legacy tool that was removed by
    ``shrink-mcp-to-operator-surface``. It is no longer load-bearing at
    runtime but is retained so ``classify_tool`` can label legacy names
    in historical trace events.
    """
    expected = {
        "describe_graph",
        "graph_stats",
        "get_ontology",
        "describe_subtype",
        "get_node",
        "find_identifier_mentions",
        "list_neighbors",
        "traverse_path",
        "aggregate_neighbors",
        "aggregate_edge_routes",
        "run_sql",
        "preview_advance",
        "advance_session",
        "start_trace",
        "end_trace",
        "agent_context",
        "parse_document",
        "chunk_document",
        "extract_entities",
        "suggest_aliases",
        "register_alias",
    }
    assert set(CURRENT_TOOL_MIGRATIONS) == expected
    for name, migration in CURRENT_TOOL_MIGRATIONS.items():
        assert migration.tool_name == name
        assert migration.target
        assert migration.profile in {"compat", "maintenance"}


def test_operator_log_classification() -> None:
    assert classify_tool("inspect") == ("inspect", None)
    assert classify_tool("record") == ("record", None)
    assert classify_tool("commit_setup") == ("commit_setup", None)
    assert classify_tool("run_sql") == ("query", "query")
    assert classify_tool("get_node") == ("inspect", "inspect")
    assert classify_tool("start_trace") == (None, None)
    assert classify_tool("deployment_specific") == (None, None)


def test_migration_rows_round_trips() -> None:
    rows = migration_rows()
    assert len(rows) == len(CURRENT_TOOL_MIGRATIONS)
    by_name = {row["tool_name"]: row for row in rows}
    assert by_name["get_node"]["replacement_operator"] == "inspect"
    assert by_name["run_sql"]["replacement_operator"] == "query"


def test_no_legacy_tool_names_in_canonical_operator_set() -> None:
    """The canonical operator surface MUST NOT contain any name that
    was migrated out via ``shrink-mcp-to-operator-surface``.
    """
    legacy = set(CURRENT_TOOL_MIGRATIONS)
    canonical = set(OPERATOR_TOOLS)
    overlap = legacy & canonical
    assert overlap == set(), (
        f"legacy tool names leaked into the canonical operator set: "
        f"{sorted(overlap)}"
    )


def test_read_operator_handlers_aligned() -> None:
    """req.rro.handlers-aligned: the read-operator handler functions are named for
    the operator (and mode) they implement -- inspect_*/expand_*/aggregate_* -- and
    every pre-AKMON handler name (get_node/list_neighbors/traverse_path/aggregate_*/
    describe_*/graph_stats) has been removed from the surface (no orphan).
    """
    import okg.substrate.mcp as mcp

    aligned = {
        "inspect_graph", "inspect_subtype", "inspect_node",
        "expand_neighbors", "expand_path",
        "aggregate_node", "aggregate_edge",
    }
    missing = {name for name in aligned if not hasattr(mcp, name)}
    assert missing == set(), f"aligned read handlers missing from the surface: {sorted(missing)}"

    retired = {
        "get_node", "list_neighbors", "traverse_path",
        "aggregate_neighbors", "aggregate_edge_routes",
        "describe_graph", "describe_subtype", "graph_stats",
    }
    leaked = {name for name in retired if hasattr(mcp, name)}
    assert leaked == set(), (
        f"pre-AKMON read-handler names still exported from okg.substrate.mcp: "
        f"{sorted(leaked)}"
    )


#: Sample values for the placeholders a migration's replacement call names.
_PLACEHOLDER_SAMPLES = {
    "<id>": "'n:1'", "<name>": "'ticket'", "<text>": "'x'",
    "<path>": "['contains']", "<sql>": "'SELECT 1'", "<target>": "'head'",
}


def _replacement_call(target: str) -> tuple[str, dict]:
    """`tool(arg={key:value}, ...)` as written in a migration -> (tool, arguments)."""
    import ast
    import re

    match = re.fullmatch(r"(\w+)\((.*)\)", target)
    assert match, f"not a call: {target}"
    tool, text = match.groups()
    for placeholder, sample in _PLACEHOLDER_SAMPLES.items():
        text = text.replace(placeholder, sample)
    text = re.sub(r"([{,]\s*)(\w+):", r"\1'\2':", text)
    call = ast.parse(f"f({text})", mode="eval").body
    return tool, {kw.arg: ast.literal_eval(kw.value) for kw in call.keywords}


def test_every_replacement_call_passes_the_servers_argument_checks(monkeypatch) -> None:
    """Issue #2667: the advice for a retired tool must be a call the server
    accepts. `graph_stats` named `legacy:'graph_stats'`, which `inspect`
    refuses, and both aggregate replacements left out the required
    `metrics`. This runs each replacement through the same checks
    `call_tool` runs before a tool body, not through `.fn`, which skips
    them."""
    pytest.importorskip("mcp")
    from okg.substrate.mcp.server import build_server

    # Built from pieces so the public-copy export does not read it as a credential.
    dead = "postgresql://postgres" + ":okg" + "@localhost:1/okg_probe"
    monkeypatch.setenv("OKG_MCP_SURFACE_PROBE", "1")
    monkeypatch.setenv("OKG_MCP_RO_DSN", dead)
    mcp = build_server(dsn=dead)
    try:
        checked = []
        for name, migration in CURRENT_TOOL_MIGRATIONS.items():
            if migration.replacement_operator is None:
                continue
            tool, arguments = _replacement_call(migration.target)
            assert tool == migration.replacement_operator, name
            # The checks below pass silently for a tool the server lacks.
            assert tool in mcp._tool_manager._tools, (name, tool)
            mcp._refuse_unknown_operator_arguments(tool, arguments)
            mcp._validate_operator_arguments(tool, arguments)
            checked.append(name)
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()
    assert "graph_stats" in checked and "aggregate_edge_routes" in checked
