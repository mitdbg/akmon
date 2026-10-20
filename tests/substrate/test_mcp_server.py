"""M4 — MCP server smoke tests.

The server is a thin FastMCP wrapper over the tools. We don't
drive stdio here (that's an integration test); we verify:
- build_server() registers the expected tool set
- each tool's wrapper invokes its underlying substrate function
  with the session correctly pinned
"""
from __future__ import annotations

import hashlib
import json
import sys
import uuid
from pathlib import Path
from types import SimpleNamespace

import psycopg
import pytest

pytest.importorskip("mcp")

from okg.substrate.embeddings import (
    EmbeddingConfig,
    HashEmbedder,
    build_embedding_execution_plan,
)
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.mcp.deployment_entry import _verified_embedder_for_manifest
from okg.substrate.mcp.operators import OPERATOR_TOOLS
from okg.substrate.mcp.provenance import UNTRUSTED_DATA_CONTRACT
from okg.substrate.mcp.server import build_server
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402
from tests.substrate.mcp._result_payload import payload as _result_payload


pytestmark = pytest.mark.postgres


@pytest.fixture(autouse=True)
def _json_result_format(monkeypatch: pytest.MonkeyPatch):
    """These tests assert on the result dictionary, so every call asks for
    the JSON result format (issue #3059: the default is one text block).
    Client sessions send the request _meta key; in-process
    FastMCP.call_tool calls run inside result_format_scope."""
    from okg.substrate.mcp.result_text import result_format_scope
    from tests.substrate.mcp._result_payload import json_client_sessions

    json_client_sessions(monkeypatch)
    with result_format_scope("json"):
        yield


def _hash_plan():
    return build_embedding_execution_plan(
        EmbeddingConfig(enabled=True, backend="hash")
    )


def test_build_server_rejects_raw_unverified_embedder() -> None:
    with pytest.raises(TypeError, match="verified EmbeddingExecutionPlan"):
        build_server(dsn="postgres://unused", embedder=HashEmbedder())


def _seed(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        cv = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO okg.subtypes (name, metadata, catalog_version_id)
            VALUES ('page', '{}'::jsonb, %s)
            """,
            (cv,),
        )
        cur.execute(
            """
            INSERT INTO okg.subtype_ancestors
              (catalog_version_id, subtype_name, ancestor_name, depth)
            VALUES (%s, 'page', 'page', 0)
            ON CONFLICT DO NOTHING
            """,
            (cv,),
        )
        cur.execute(
            """
            INSERT INTO okg.narrowings
              (parent_subtype, edge_type, child_subtype,
               catalog_version_id)
            VALUES ('page', 'links', 'page', %s)
            """,
            (cv,),
        )
    # Seed the graph through the canonical Chronos write so a chronos_native
    # publish checkpoints a populated head. The legacy okg.node_facts path leaves
    # the Chronos checkout -- which the read operators query -- empty.
    ChronosGraphStore.from_connection(conn).apply_facts(
        [
            NodeFact("n:a", "page", {"text": "alpha"}, {"id": "n:a"}, {"v": 1}),
            NodeFact("n:b", "page", {"text": "beta"}, {"id": "n:b"}, {"v": 1}),
        ],
        source="default",
        strict_endpoints=True,
    )


def _seed_edge(conn: psycopg.Connection) -> None:
    # Adds n:a --links--> n:b on the Chronos head (endpoints seeded by _seed).
    ChronosGraphStore.from_connection(conn).apply_facts(
        [
            EdgeFact(
                "n:a", "n:b", "links",
                source_record_id={"id": "n:a-links-n:b"},
                source_revision={"v": 1},
            ),
        ],
        source="default",
        strict_endpoints=True,
    )


def _registered_tool_names(mcp) -> set[str]:
    # FastMCP exposes tools via _tool_manager._tools
    return set(mcp._tool_manager._tools.keys())


def _write_deployment_with_tools(
    tmp_path: Path,
    *,
    deployment_name: str = "demo",
    module_name: str = "tools",
    tools_py: str,
    agent_tools: str,
) -> Path:
    deployments_root = tmp_path / "deployments"
    demo = deployments_root / deployment_name
    (demo / "schemas").mkdir(parents=True)
    (demo / "source_registry.yaml").write_text("", encoding="utf-8")
    (demo / f"{module_name}.py").write_text(tools_py, encoding="utf-8")
    (demo / "deployment.yaml").write_text(
        f"""
name: {deployment_name}
description: test
schema_dir: schemas
source_registry: source_registry.yaml
agent_tools:
{agent_tools}
""".lstrip(),
        encoding="utf-8",
    )
    return deployments_root


def test_build_server_registers_expected_tools(clean_runtime: str) -> None:
    mcp = build_server(dsn=clean_runtime, embedder=_hash_plan())
    try:
        names = _registered_tool_names(mcp)
        assert names == set(OPERATOR_TOOLS)
        assert mcp._okg_boundary_pruned_tools == ()
        assert mcp._okg_missing_admitted_tools == ()
    finally:
        mcp._okg_pool.close()


def test_operator_tool_schemas_accept_string_generation_ids(
    clean_runtime: str,
) -> None:
    # `compact_tool_schemas` drops the `{"type": "null"}` branch of every
    # optional INPUT parameter on purpose (operator_models.compact_schema):
    # leaving `generation_id` out of `required` already says "may be omitted",
    # and pydantic still accepts an explicit null at runtime. What this test
    # guards is that a string id is accepted and the parameter stays optional.
    mcp = build_server(dsn=clean_runtime, embedder=_hash_plan())
    try:
        from okg.substrate.mcp.lean_surface import HIDDEN_ARGUMENTS

        for name in OPERATOR_TOOLS:
            tool = mcp._tool_manager._tools[name]
            schema = tool.parameters
            assert "generation_id" not in schema.get("required", []), name
            if "generation_id" in HIDDEN_ARGUMENTS.get(name, ()):
                # Accepted, no longer published (#3666, M10): the model
                # still takes a string id.
                assert "generation_id" not in schema["properties"], name
                tool.fn_metadata.arg_model.model_validate(
                    {"generation_id": "g:1", **_minimal_arguments(name)}
                )
                continue
            generation_schema = schema["properties"]["generation_id"]
            assert generation_schema["anyOf"] == [
                {"type": "integer"},
                {"type": "string"},
            ]
    finally:
        mcp._okg_pool.close()
    write_mcp = build_server(
        dsn=clean_runtime,
        embedder=_hash_plan(),
        allow_writes=True,
    )
    try:
        schema = write_mcp._tool_manager._tools["propose"].parameters
        generation_schema = schema["properties"]["generation_id"]
        assert generation_schema["anyOf"] == [
            {"type": "integer"},
            {"type": "string"},
        ]
        assert "generation_id" not in schema.get("required", [])
    finally:
        write_mcp._okg_pool.close()
        if write_mcp._okg_write_pool:  # type: ignore[attr-defined]
            write_mcp._okg_write_pool.close()  # type: ignore[attr-defined]


def _minimal_arguments(name: str) -> dict:
    """The arguments each read tool's model requires, besides generation_id."""
    return {
        "inspect": {}, "search": {}, "expand": {},
        "filter": {"input": {"kind": "nodes"}, "predicate": {}},
        "map": {"input": {"kind": "nodes"}},
        "aggregate": {"input": {"kind": "nodes"}, "by": ["subtype"], "metrics": ["count"]},
    }[name]


def _close_server(mcp) -> None:
    mcp._okg_pool.close()
    if mcp._okg_write_pool:  # type: ignore[attr-defined]
        mcp._okg_write_pool.close()  # type: ignore[attr-defined]


# `inspect(target={"kind": "surface"})` is served only by a write/admin launch
# or a launch with a tool-profile ceiling (effective_surface.
# CEILING_INSPECT_KINDS): `surface` with `action="expand"` mutates the
# session's tool list. A read-only launch refuses it by name, so the surface
# tests below build the write/admin launch that serves it.


def test_inspect_surface_reports_running_instruction_hash(
    clean_runtime: str,
) -> None:
    instructions = "operator instructions\n\n" + UNTRUSTED_DATA_CONTRACT
    changed = instructions + "\nextra runtime line"
    mcp = build_server(dsn=clean_runtime, instructions=instructions, allow_writes=True)
    changed_mcp = build_server(dsn=clean_runtime, instructions=changed, allow_writes=True)
    stale_mcp = build_server(
        dsn=clean_runtime, instructions="old surface", allow_writes=True
    )
    try:
        report = mcp._tool_manager._tools["inspect"].fn(  # type: ignore[attr-defined]
            target={"kind": "surface"},
        )
        changed_report = changed_mcp._tool_manager._tools["inspect"].fn(  # type: ignore[attr-defined]
            target={"kind": "surface"},
        )
        stale_report = stale_mcp._tool_manager._tools["inspect"].fn(  # type: ignore[attr-defined]
            target={"kind": "surface"},
        )

        assert report["source"] == "running_process"
        assert report["instructions_hash"] == (
            "sha256:"
            + hashlib.sha256(instructions.encode("utf-8")).hexdigest()
        )
        assert changed_report["instructions_hash"] != report["instructions_hash"]
        assert report["untrusted_data_contract_present"] is True
        assert stale_report["untrusted_data_contract_present"] is False
        assert report["package_name"] == "okg"
    finally:
        _close_server(mcp)
        _close_server(changed_mcp)
        _close_server(stale_mcp)


def test_inspect_surface_skips_trace_side_effects(
    clean_runtime: str,
    monkeypatch,
) -> None:
    from okg.substrate.mcp import tracing

    instructions = "offline instructions\n\n" + UNTRUSTED_DATA_CONTRACT

    def fail_trace(*args, **kwargs):
        raise AssertionError("surface introspection must not write trace rows")

    monkeypatch.setattr(tracing, "trace_tool_call", fail_trace)
    monkeypatch.setattr(tracing, "log_call_to_log", fail_trace)

    mcp = build_server(dsn=clean_runtime, instructions=instructions, allow_writes=True)
    try:
        tool = mcp._tool_manager._tools["inspect"]  # type: ignore[attr-defined]
        report = tool.fn(target={"kind": "surface"})

        assert report["source"] == "running_process"
        assert report["instructions_hash"] == (
            "sha256:"
            + hashlib.sha256(instructions.encode("utf-8")).hexdigest()
        )
        assert report["untrusted_data_contract_present"] is True
    finally:
        _close_server(mcp)


def test_surface_probe_mode_does_not_open_database(monkeypatch) -> None:
    instructions = "surface probe instructions\n\n" + UNTRUSTED_DATA_CONTRACT
    monkeypatch.setenv("OKG_MCP_SURFACE_PROBE", "1")
    mcp = build_server(
        dsn="postgres://postgres:okg@127.0.0.1:1/okg_missing",
        instructions=instructions,
        allow_writes=True,
    )
    try:
        tool = mcp._tool_manager._tools["inspect"]  # type: ignore[attr-defined]
        report = tool.fn(target={"kind": "surface"})

        assert report["source"] == "running_process"
        assert report["untrusted_data_contract_present"] is True
    finally:
        _close_server(mcp)


def test_session_start_carries_surface_report(clean_runtime: str) -> None:
    instructions = "session start instructions\n\n" + UNTRUSTED_DATA_CONTRACT
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn)
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    mcp = build_server(dsn=clean_runtime, instructions=instructions, allow_writes=True)
    try:
        tools = mcp._tool_manager._tools  # type: ignore[attr-defined]
        surface = tools["inspect"].fn(target={"kind": "surface"})
        session_start = tools["inspect"].fn(target={"kind": "session_start"})

        assert session_start["surface"] == {
            key: surface[key]
            for key in session_start["surface"]
        }
        assert session_start["surface"]["instructions_hash"] == surface[
            "instructions_hash"
        ]
        assert session_start["surface"]["untrusted_data_contract_present"] is True
        # #3470 (`ssfit A`): the running code and its instructions only;
        # the full readback stays one call away.
        assert set(session_start["surface"]) == {
            "deployment_name", "package_version", "git_revision",
            "instructions_hash", "instructions_bytes",
            "untrusted_data_contract_present", "dataset_card_state",
        } & set(surface)
        assert "capabilities" in surface and "capabilities" not in session_start["surface"]
        assert session_start["surface_full"] == 'inspect(target={"kind":"surface"})'
    finally:
        _close_server(mcp)


def test_read_only_launch_serves_session_start_and_accepts_null_generation_id(
    clean_runtime: str,
) -> None:
    """Two guards the surface tests above moved off the read-only launch.

    `session_start` is a read kind (effective_surface.READ_INSPECT_KINDS), so
    a read-only launch must still serve it. And the published schema dropping
    the null branch of `generation_id` is only safe because the runtime still
    ACCEPTS an explicit null: this goes through `call_tool`, which validates
    the arguments against the tool's model, not through `.fn`.
    """
    import asyncio

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn)
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    mcp = build_server(dsn=clean_runtime)
    try:
        result = asyncio.run(
            mcp.call_tool(
                "inspect",
                {"target": {"kind": "session_start"}, "generation_id": None},
            )
        )
        assert not getattr(result, "isError", False), result
        payload = _result_payload(result)
        assert isinstance(payload, dict), result
        assert "error" not in payload, payload
        assert "surface" in payload, sorted(payload)
    finally:
        _close_server(mcp)


# `test_build_server_compat_surface_keeps_legacy_tools` was deleted by
# `shrink-mcp-to-operator-surface`. The "compat" surface that kept the
# 22 legacy tools registered is gone; the operator surface is the only
# surface, and the canonical-set regression test
# (`test_only_operator_tools_are_registered`) lives next to it.


def test_deployment_entry_builds_verified_manifest_embedder(monkeypatch) -> None:
    monkeypatch.setenv("OKG_MCP_EMBEDDER", "hash")
    manifest = SimpleNamespace(
        embedding_config=EmbeddingConfig(enabled=True, backend="hash")
    )
    with pytest.warns(UserWarning, match="deprecated"):
        embedder = _verified_embedder_for_manifest(manifest)
    assert embedder.enabled is True
    assert isinstance(embedder.embedder, HashEmbedder)
    assert embedder.dimension == 2560
    assert embedder.fingerprint.startswith("sha256:")


def test_server_graph_stats_auto_pins_to_latest(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn)
    r = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    mcp = build_server(dsn=clean_runtime)
    # Migrated: graph_stats → inspect(kind="graph", legacy="graph_stats")
    tool = mcp._tool_manager._tools["inspect"]
    # FastMCP wraps the fn; .fn is the underlying callable.
    out = tool.fn(target={"kind": "graph", "legacy": "graph_stats"})
    assert out["generation_id"] == r["generation_id"]
    assert out["node_count"] == 2


def test_server_accepts_explicit_generation_id(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn)
    r1 = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    # Add a node on the Chronos head + publish gen 2.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [NodeFact("n:c", "page", {"text": "gamma"}, {"id": "n:c"}, {"v": 1})],
            source="default",
            strict_endpoints=True,
        )
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    mcp = build_server(dsn=clean_runtime)
    # Migrated: graph_stats → inspect(kind="graph", legacy="graph_stats")
    tool = mcp._tool_manager._tools["inspect"]

    # Pinning to r1 sees 2 nodes; default (latest) sees 3.
    pinned = tool.fn(
        target={"kind": "graph", "legacy": "graph_stats"},
        generation_id=r1["generation_id"],
    )
    latest = tool.fn(target={"kind": "graph", "legacy": "graph_stats"})
    assert pinned["node_count"] == 2
    assert latest["node_count"] == 3


def test_describe_graph_minimal_schema_fallback_keeps_counts(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.mcp import schema_digest

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn)
        _seed_edge(conn)
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    monkeypatch.setattr(schema_digest, "MAX_DIGEST_BYTES", 1)
    mcp = build_server(dsn=clean_runtime)
    # Migrated: describe_graph → inspect(kind="graph"). The minimal-
    # schema fallback path adds the same schema payload; we exercise
    # the non-legacy inspect shape here so the schema block survives.
    tool = mcp._tool_manager._tools["inspect"]

    out = tool.fn(target={"kind": "graph"})
    schema = out["schema"]
    assert schema["minimal"] is True
    assert schema["minimal_reason"]
    assert schema["subtypes"]["page"]["node_count"] == 2
    assert schema["edge_types"] == ["links"]
    assert schema["edge_type_counts"] == {"links": 1}
    assert schema["score_functions"]


def test_server_loads_readside_metadata_and_node_hints(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deployments_root = tmp_path / "deployments"
    demo = deployments_root / "demo"
    (demo / "schemas").mkdir(parents=True)
    (demo / "docs").mkdir()
    (demo / "skills").mkdir()
    (demo / "source_registry.yaml").write_text("", encoding="utf-8")
    (demo / "docs" / "mcp-instructions.md").write_text(
        "Use the demo read-side instructions.",
        encoding="utf-8",
    )
    (demo / "docs" / "audit-questions.md").write_text(
        "1. Which pages are connected?",
        encoding="utf-8",
    )
    (demo / "skills" / "lookup.md").write_text(
        "# lookup\n\nFetch a node.",
        encoding="utf-8",
    )
    (demo / "docs" / "skill-triggers.yaml").write_text(
        """
version: 1
intents:
  lookup:
    skill: lookup
    description: Fetch a single node.
    subtypes: ["*"]
  survey:
    skill: survey
    description: Survey page nodes.
    subtypes: ["page"]
""".lstrip(),
        encoding="utf-8",
    )
    (demo / "deployment.yaml").write_text(
        """
name: demo
description: test
schema_dir: schemas
source_registry: source_registry.yaml
mcp_instructions: docs/mcp-instructions.md
skills_dir: skills
audit_questions: docs/audit-questions.md
skill_triggers: docs/skill-triggers.yaml
""".lstrip(),
        encoding="utf-8",
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(deployments_root))

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn)
    Publisher(clean_runtime, "demo", catalog_identity=bind_test_catalog_writer(clean_runtime, "demo")).cycle()

    mcp = build_server(
        dsn=clean_runtime, deployment_name="demo",
    )
    try:
        metadata = mcp._okg_readside_metadata  # type: ignore[attr-defined]
        assert metadata["instructions"] == "Use the demo read-side instructions."
        assert metadata["audit_questions"] == "1. Which pages are connected?"
        assert sorted(metadata["skills"]) == ["lookup"]
        assert sorted(metadata["skill_triggers"]["intents"]) == [
            "lookup",
            "survey",
        ]

        # Migrated: get_node → inspect(kind="node", node_id=...)
        tool = mcp._tool_manager._tools["inspect"]  # type: ignore[attr-defined]
        out = tool.fn(target={"kind": "node", "node_id": "n:a"})
        assert out["node_id"] == "n:a"
        assert out["subtype"] == "page"
        # #3076 (decision 5): the hints live on the type, not on every node.
        assert "skill_hints" not in out
        subtype = tool.fn(target={"kind": "subtype", "subtype": "page"})
        assert subtype["skill_hints"] == [
            {
                "intent": "lookup",
                "skill": "lookup",
                "description": "Fetch a single node.",
            },
            {
                "intent": "survey",
                "skill": "survey",
                "description": "Survey page nodes.",
            },
        ]
    finally:
        mcp._okg_pool.close()  # type: ignore[attr-defined]


def test_server_catalogs_deployment_tool_without_registering_it(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _write_deployment_with_tools(
        tmp_path,
        module_name="demo_tools_valid",
        tools_py="""
def echo(value: str) -> dict:
    return {"value": value}
""".lstrip(),
        agent_tools="""
  demo_echo:
    module: demo_tools_valid
    function: echo
    boundary: external_read
    description: Echo input from a deployment-owned tool.
""".rstrip(),
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    saved_path = list(sys.path)
    try:
        core_mcp = build_server(dsn=clean_runtime)
        try:
            core = _registered_tool_names(core_mcp)
        finally:
            core_mcp._okg_pool.close()  # type: ignore[attr-defined]
        mcp = build_server(
            dsn=clean_runtime, deployment_name="demo",
        )
        names = _registered_tool_names(mcp)
        assert names == core
        assert "demo_echo" not in names
        assert mcp._okg_deployment_tools == []
        skipped = {
            row.name: row
            for row in mcp._okg_skipped_deployment_tools  # type: ignore[attr-defined]
        }
        assert skipped["demo_echo"].reason == (
            "compatibility_only_excluded_from_operator_surface"
        )
        assert skipped["demo_echo"].boundary == "external_read"
        assert skipped["demo_echo"].profile == "compat"
        assert skipped["demo_echo"].generation_pinned is True
        mcp._okg_pool.close()  # type: ignore[attr-defined]
    finally:
        sys.path[:] = saved_path


def test_deployment_tool_catalog_validation_failures(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.deployments import DeploymentConfigError

    cases = [
        (
            "bad_boundary",
            """
  demo_bad_boundary:
    module: demo_tools_bad_boundary
    function: echo
    boundary: not_a_boundary
    description: Bad boundary.
""",
            "boundary must be one of",
        ),
        (
            "bad_name",
            """
  live_condor_search:
    module: demo_tools_bad_name
    function: echo
    boundary: external_read
    description: Bad name.
""",
            "prefixed with 'demo_'",
        ),
        (
            "missing_module_field",
            """
  demo_missing:
    function: echo
    boundary: external_read
    description: Missing module.
""",
            "module must",
        ),
        (
            "missing_function_field",
            """
  demo_env:
    module: demo_tools_missing_env
    boundary: external_read
    description: Missing function.
""",
            "function must",
        ),
    ]
    for module_suffix, agent_tools, message in cases:
        root = _write_deployment_with_tools(
            tmp_path / module_suffix,
            module_name=f"demo_tools_{module_suffix}",
            tools_py="""
def echo(value: str = "ok") -> dict:
    return {"value": value}
""".lstrip(),
            agent_tools=agent_tools.rstrip(),
        )
        monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
        saved_path = list(sys.path)
        try:
            with pytest.raises(DeploymentConfigError, match=message):
                build_server(
                    dsn=clean_runtime,
                    deployment_name="demo",
                )
        finally:
            sys.path[:] = saved_path


def test_deployment_agent_tools_must_be_mapping(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.deployments import DeploymentConfigError

    deployments_root = tmp_path / "deployments"
    demo = deployments_root / "demo"
    (demo / "schemas").mkdir(parents=True)
    (demo / "source_registry.yaml").write_text("", encoding="utf-8")
    (demo / "deployment.yaml").write_text(
        """
name: demo
description: test
schema_dir: schemas
source_registry: source_registry.yaml
agent_tools: []
""".lstrip(),
        encoding="utf-8",
    )

    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(deployments_root))
    with pytest.raises(DeploymentConfigError, match="agent_tools must be a mapping"):
        build_server(
            dsn=clean_runtime,
            deployment_name="demo",
        )


def test_deployment_tool_core_name_collision_fails(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A deployment tool whose name collides with a canonical operator
    (e.g., `inspect`) MUST be rejected at build_server time.

    Pre-cleanup this used `get_node` as the collision target;
    `shrink-mcp-to-operator-surface` removed `get_node`, so the test
    now uses `inspect` (the canonical replacement) — a real
    OPERATOR_TOOLS name.
    """
    from okg.substrate.deployments import DeploymentConfigError

    root = _write_deployment_with_tools(
        tmp_path,
        deployment_name="list",
        module_name="list_tools_collision",
        tools_py="""
def echo(value: str = "ok") -> dict:
    return {"value": value}
""".lstrip(),
        agent_tools="""
  list_sources:
    module: list_tools_collision
    function: echo
    boundary: external_read
    description: Collision with a core MCP operator.
""".rstrip(),
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    saved_path = list(sys.path)
    try:
        with pytest.raises(DeploymentConfigError, match="collides"):
            build_server(
                dsn=clean_runtime,
                deployment_name="list",
            )
    finally:
        sys.path[:] = saved_path


def test_credentialed_deployment_tool_is_cataloged_without_secret(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _write_deployment_with_tools(
        tmp_path,
        module_name="demo_tools_optional",
        tools_py="""
def echo() -> dict:
    return {"value": "ok"}
""".lstrip(),
        agent_tools="""
  demo_optional:
    module: demo_tools_optional
    function: echo
    boundary: external_read
    requires_env: [DEMO_OPTIONAL_TOKEN]
    optional: true
    description: Optional tool.
""".rstrip(),
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    saved_path = list(sys.path)
    try:
        mcp = build_server(
            dsn=clean_runtime, deployment_name="demo",
        )
        assert "demo_optional" not in _registered_tool_names(mcp)
        skipped = {
            row.name: row
            for row in mcp._okg_skipped_deployment_tools  # type: ignore[attr-defined]
        }
        assert skipped["demo_optional"].reason == (
            "compatibility_only_excluded_from_operator_surface"
        )
        assert skipped["demo_optional"].boundary == "external_read"
        mcp._okg_pool.close()  # type: ignore[attr-defined]
    finally:
        sys.path[:] = saved_path


def test_external_live_registration_helper_validates_result_contract(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _write_deployment_with_tools(
        tmp_path,
        module_name="demo_tools_live",
        tools_py="""
def live_search(query: str) -> dict:
    return {
        "boundary": "external_live",
        "source": "fixture_live_service",
        "observed_at": "2026-05-06T12:00:00Z",
        "query": {"text": query},
        "time_window": {"from": "now-1h", "to": "now"},
        "results": [{"id": "doc-1"}],
    }
""".lstrip(),
        agent_tools="""
  demo_live_search:
    module: demo_tools_live
    function: live_search
    boundary: external_live
    requires_env: [DEMO_LIVE_TOKEN]
    description: Live external fixture search, not generation-pinned OKG evidence.
""".rstrip(),
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    monkeypatch.setenv("DEMO_LIVE_TOKEN", "super-secret-token")
    saved_path = list(sys.path)
    try:
        from mcp.server.fastmcp import FastMCP
        from okg.substrate.mcp.deployment_tools import (
            register_deployment_tools,
        )

        mcp = FastMCP("deployment-tool-fixture")
        register_deployment_tools(
            mcp,
            deployment_name="demo",
            tracer_factory=lambda _name: lambda fn: fn,
        )
        out = mcp._tool_manager._tools["demo_live_search"].fn(query="site:T1")
        assert out["boundary"] == "external_live"
        assert out["source"] == "fixture_live_service"
        assert "super-secret-token" not in json.dumps(out["query"])
    finally:
        sys.path[:] = saved_path


def test_server_search_dispatch(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn)
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    mcp = build_server(dsn=clean_runtime, embedder=_hash_plan())
    tool = mcp._tool_manager._tools["search"]
    out = tool.fn(query="alpha", method="lexical")
    assert out["method"] == "lexical"
    ids = [r["node_id"] for r in out["results"]]
    assert "n:a" in ids


def test_server_semantic_search_without_embedder_refuses_structured(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn)
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    mcp = build_server(dsn=clean_runtime, embedder=None)
    tool = mcp._tool_manager._tools["search"]
    out = tool.fn(query="x", method="semantic")
    assert out["status"] == "refused"
    assert out["error"]["code"] == "semantic_search_unavailable"
    assert out["error"]["fallback_method"] == "lexical"
    # #3151 slice 1: the refusal also carries the start-time probe's view.
    capability = dict(out["error"]["capability"])
    available = capability.pop("available_methods")
    assert capability == {
        "method": "semantic",
        "embedder_configured": False,
        "status": "unavailable",
        "reason": "no embedder is configured",
    }
    assert "semantic" not in available
    assert "lexical" in available


def test_server_uses_connection_pool_and_pin_cache(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """M4.5 (design review P1 #7): build_server attaches a pool +
    pin cache; pin cache serves the second call within the TTL."""
    from okg.substrate.mcp import server as mcp_server

    monkeypatch.setattr(mcp_server, "LATEST_PIN_CACHE_TTL_S", 60.0)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn)
    r = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    g = r["generation_id"]

    mcp = build_server(
        dsn=clean_runtime,
        min_conns=1,
        max_conns=3,
    )
    assert hasattr(mcp, "_okg_pool")
    assert hasattr(mcp, "_okg_pin_cache")

    # Migrated: graph_stats → inspect(kind="graph", legacy="graph_stats")
    tool = mcp._tool_manager._tools["inspect"]
    # First call populates the cache.
    out1 = tool.fn(target={"kind": "graph", "legacy": "graph_stats"})
    assert out1["generation_id"] == g
    assert "default" in mcp._okg_pin_cache
    cached_gen = mcp._okg_pin_cache["default"][1]
    assert cached_gen == g

    # Second call (within 500ms TTL) returns the cached pin even
    # after we publish a newer generation; a fresh explicit pin
    # still bypasses.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            rid = str(uuid.uuid4())
            cur.execute(
                """
                INSERT INTO okg.node_facts
                  (node_id, subtype, attrs, source, source_record_id,
                   source_revision, observed_at, dedupe_key, run_id, op)
                VALUES ('n:c','page','{"text":"gamma"}'::jsonb,
                        'fixture','{"id":"n:c"}'::jsonb,
                        '{"v":1}'::jsonb, now(), %s, %s, 'I')
                """,
                (f"{rid}:n:c", rid),
            )
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    # Immediately call again — the cache is still warm; returns
    # the stale pin.
    out2 = tool.fn(target={"kind": "graph", "legacy": "graph_stats"})
    assert out2["generation_id"] == g, (
        "pin cache should have been hit within the 500ms TTL"
    )

    mcp._okg_pool.close()
