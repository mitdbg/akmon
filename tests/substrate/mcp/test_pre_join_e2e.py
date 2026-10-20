"""End-to-end integration tests for the pre-join MCP framework
plus the substrate primitives built on it (fan_out_sealed_brief,
review_as).

Per the impl brief Phase 10 acceptance gate: deployment-load a
fixture deployment with `mcp_pre_join_tools.yaml`; invoke one
declared tool; invoke `fan_out_sealed_brief`; invoke `review_as`.
All return non-empty bundles or land typed nodes correctly.

These tests run against a real Publisher via the
`tests/substrate/conftest.py::clean_runtime` fixture. They're
slow (real Postgres) but small (one-tool YAML, two workers,
stub LLM).
"""
from __future__ import annotations

import json
import textwrap
import uuid
from typing import Any

import psycopg
import pytest

from okg.substrate.mcp import open_session
from okg.substrate.mcp.fan_out import (
    WorkerDeclaration,
    fan_out_sealed_brief,
)
from okg.substrate.mcp.framework import (
    ResolverRegistry,
    execute_pre_join_tool,
    parse_pre_join_tools,
)
from okg.substrate.mcp.review_as import review_as
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def _published_graph_query(dsn: str, generation_id, sql: str, params=None):
    """Run `sql` against a Chronos checkout pinned to a published
    generation's checkpoint. The chronos-native cutover dropped
    okg.nodes_live / okg.edges_live; the graph at a published generation
    is read through this checkout over okg.graph_nodes / okg.graph_edges."""
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).checkout_generation(generation_id)
        return graph.query(sql, params or {})


def _seed_catalog(conn: psycopg.Connection) -> int:
    """Seed a minimal catalog: brief + sub_agent_session +
    paper + critique subtypes + the narrowings the tests need."""
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        cv = cur.fetchone()[0]
        for name in (
            "brief", "sub_agent_session", "brief_dispatch",
            "paper", "critique", "persona",
        ):
            cur.execute(
                "INSERT INTO okg.subtypes (name, metadata, "
                "catalog_version_id) VALUES (%s, '{}'::jsonb, %s)",
                (name, cv),
            )
            cur.execute(
                """
                INSERT INTO okg.subtype_ancestors
                  (catalog_version_id, subtype_name, ancestor_name, depth)
                VALUES (%s, %s, %s, 0)
                ON CONFLICT DO NOTHING
                """,
                (cv, name, name),
            )
        narrowings = [
            ("brief_dispatch", "references", "brief"),
            ("brief_dispatch", "references", "sub_agent_session"),
            ("critique", "critiques", "paper"),
        ]
        for parent, edge, child in narrowings:
            cur.execute(
                "INSERT INTO okg.narrowings "
                "(parent_subtype, edge_type, child_subtype, "
                " catalog_version_id) VALUES (%s, %s, %s, %s)",
                (parent, edge, child, cv),
            )
    return cv


def _insert_node_fact(
    conn: psycopg.Connection,
    *,
    node_id: str,
    subtype: str,
    attrs: dict[str, Any],
) -> None:
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    run_id = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.node_facts
              (node_id, subtype, attrs, source,
               source_record_id, source_revision,
               observed_at, dedupe_key, run_id, op)
            VALUES (%s, %s, %s::jsonb, 'fixture',
                    %s::jsonb, %s::jsonb,
                    now(), %s, %s, 'I')
            """,
            (
                node_id, subtype, json.dumps(attrs),
                json.dumps({"id": node_id}),
                json.dumps({"v": 1}),
                f"fixture:{node_id}:{uuid.uuid4().hex[:8]}",
                run_id,
            ),
        )
    # Post chronos-native cutover the publisher checkpoints the Chronos
    # branch head; it no longer projects node_facts. Write the graph node
    # through the canonical Chronos path so get_node / the pre-join bundle
    # resolve it at the published generation.
    ChronosGraphStore.from_connection(conn).apply_facts(
        [NodeFact(node_id, subtype, dict(attrs), {"id": node_id}, {"v": 1})],
        source="fixture",
        strict_endpoints=True,
    )


def _build_drafting_tool() -> Any:
    """Parse a single pre-join tool spec for use by tests."""
    yaml_text = textwrap.dedent("""\
        tools:
          - name: review_target_neighborhood
            description: Substrate-shipped neighborhood lookup used by review_as.
            params:
              - {name: target_node_id, kind: node_id, required: true}
            bundle:
              - call: get_node
                args:
                  node_id: {resolve: alias_table, input: target_node_id}
                emit_as: target
    """)
    import yaml
    specs, failures = parse_pre_join_tools(yaml.safe_load(yaml_text))
    assert failures == []
    assert len(specs) == 1
    return specs[0]


def test_pre_join_executor_against_real_publisher(
    clean_runtime: str,
) -> None:
    """End-to-end: a single get_node-based pre-join tool runs
    against a published generation and returns a non-empty
    target bundle."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_catalog(conn)
        _insert_node_fact(
            conn, node_id="paper:foo", subtype="paper",
            attrs={"title": "Transformers Scale With Data", "text": "x"},
        )
        # Alias entry so alias_table resolver succeeds.
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.aliases "
                "(alias, canonical, catalog_version_id) "
                "VALUES (%s, %s, %s)",
                ("paper:foo", "paper:foo", cv),
            )
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    spec = _build_drafting_tool()
    resolvers = ResolverRegistry.with_substrate_builtins()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        out = execute_pre_join_tool(
            spec,
            conn=conn,
            session=session,
            params={"target_node_id": "paper:foo"},
            resolvers=resolvers,
        )
    assert out["generation_id"] == session.generation_id
    target = out["bundles"]["target"]
    assert target is not None
    assert target["node_id"] == "paper:foo"
    assert target["subtype"] == "paper"


def test_fan_out_sealed_brief_writes_audit_nodes(
    clean_runtime: str,
) -> None:
    """fan_out_sealed_brief with two workers writes:
      1 BriefDispatch + 2 SubAgentSession placeholders + 3 edges
    Per S-A7-R4."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_catalog(conn)
        _insert_node_fact(
            conn, node_id="brief:wave-b-7", subtype="brief",
            attrs={
                "brief_id": "brief:wave-b-7",
                "title": "Wave B brief",
                "pre_join_tool_for_per_worker_context":
                    "review_target_neighborhood",
                "text": "Brief body",
            },
        )
        _insert_node_fact(
            conn, node_id="paper:bar", subtype="paper",
            attrs={"title": "Other paper", "text": "y"},
        )
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.aliases "
                "(alias, canonical, catalog_version_id) "
                "VALUES (%s, %s, %s)",
                ("paper:bar", "paper:bar", cv),
            )
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    spec = _build_drafting_tool()
    pre_join_tools = {spec.name: spec}
    resolvers = ResolverRegistry.with_substrate_builtins()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        result = fan_out_sealed_brief(
            conn,
            session,
            brief_id="brief:wave-b-7",
            workers=[
                WorkerDeclaration(
                    worker_id="w1",
                    param_overrides={"target_node_id": "paper:bar"},
                ),
                WorkerDeclaration(
                    worker_id="w2",
                    param_overrides={"target_node_id": "paper:bar"},
                ),
            ],
            pre_join_tools=pre_join_tools,
            resolvers=resolvers,
        )

    assert result.dispatch_id.startswith("dispatch:")
    assert len(result.per_worker) == 2
    for w in result.per_worker:
        assert w.bundle["bundles"]["target"]["node_id"] == "paper:bar"

    # Confirm the audit rows landed in the Chronos graph at the published
    # generation (read through a checkout; the legacy live tables are dropped).
    published = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    gen = published["generation_id"]
    dispatch_nodes = int(_published_graph_query(
        clean_runtime, gen,
        "SELECT count(*) AS n FROM okg.graph_nodes "
        "WHERE subtype = 'brief_dispatch' AND node_id = :nid",
        {"nid": result.dispatch_id},
    )[0]["n"])
    assert dispatch_nodes == 1
    sub_agent_nodes = int(_published_graph_query(
        clean_runtime, gen,
        "SELECT count(*) AS n FROM okg.graph_nodes "
        "WHERE subtype = 'sub_agent_session' "
        "AND attrs->>'dispatch_node_ref' = :nid",
        {"nid": result.dispatch_id},
    )[0]["n"])
    assert sub_agent_nodes == 2
    reference_edges = int(_published_graph_query(
        clean_runtime, gen,
        "SELECT count(*) AS n FROM okg.graph_edges "
        "WHERE src = :src AND edge_type = 'references'",
        {"src": result.dispatch_id},
    )[0]["n"])
    assert reference_edges == 3  # 1 brief + 2 sub-agent


def test_review_as_emits_critique_node_and_critiques_edge(
    clean_runtime: str,
) -> None:
    """review_as with a stub LLM emits one Critique + one edge.
    Verifies the Critique lands at the next publish."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_catalog(conn)
        _insert_node_fact(
            conn, node_id="paper:foo", subtype="paper",
            attrs={"title": "Hi", "text": "h"},
        )
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.aliases "
                "(alias, canonical, catalog_version_id) "
                "VALUES (%s, %s, %s)",
                ("paper:foo", "paper:foo", cv),
            )
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    # Use the substrate's own canonical persona to satisfy resolve.
    # The persona's default pre_join_tool_for_input is
    # substrate_review_target_neighborhood; we name our test tool
    # exactly that so the persona resolves to it.
    yaml_text = textwrap.dedent("""\
        tools:
          - name: substrate_review_target_neighborhood
            description: Test stand-in.
            params:
              - {name: target_node_id, kind: node_id, required: true}
            bundle:
              - call: get_node
                args:
                  node_id: {resolve: alias_table, input: target_node_id}
                emit_as: target
    """)
    import yaml
    specs, _ = parse_pre_join_tools(yaml.safe_load(yaml_text))
    pre_join_tools = {specs[0].name: specs[0]}
    resolvers = ResolverRegistry.with_substrate_builtins()

    captured_payloads: list[dict[str, Any]] = []

    def stub_llm(system_prompt: str, user_payload: dict[str, Any]):
        captured_payloads.append(user_payload)
        return {
            "verdict": "conditional",
            "severity": "minor",
            "text": "I'd want a measurement on a real workload.",
            "confidence": 0.7,
        }

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        out = review_as(
            conn,
            session,
            persona="vonneumann",
            target_node_id="paper:foo",
            llm_call=stub_llm,
            pre_join_tools=pre_join_tools,
            resolvers=resolvers,
        )

    assert out.persona == "vonneumann"
    assert out.target_node_id == "paper:foo"
    assert out.verdict == "conditional"
    assert out.severity == "minor"
    assert out.confidence == 0.7
    assert captured_payloads, "stub LLM was not invoked"
    payload = captured_payloads[0]
    assert payload["target_node_id"] == "paper:foo"
    assert payload["request_type"]  # falls back to persona default
    assert payload["neighborhood"]["bundles"]["target"]["node_id"] == "paper:foo"

    published = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    gen = published["generation_id"]
    critique_nodes = int(_published_graph_query(
        clean_runtime, gen,
        "SELECT count(*) AS n FROM okg.graph_nodes "
        "WHERE subtype = 'critique' AND node_id = :nid",
        {"nid": out.critique_id},
    )[0]["n"])
    assert critique_nodes == 1
    critique_edges = int(_published_graph_query(
        clean_runtime, gen,
        "SELECT count(*) AS n FROM okg.graph_edges "
        "WHERE src = :src AND dst = 'paper:foo' AND edge_type = 'critiques'",
        {"src": out.critique_id},
    )[0]["n"])
    assert critique_edges == 1
