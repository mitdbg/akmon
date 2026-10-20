from __future__ import annotations

import json
import sys
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg
import pytest

pytest.importorskip("mcp")

from okg.deployment.testing import mock_dbos_context
from okg.substrate.catalog.identifier_patterns import (
    IdentifierPattern,
    write_patterns,
)
from okg.substrate.embeddings import (
    EmbeddingConfig,
    build_embedding_execution_plan,
)
from okg.substrate.mcp.operators import OPERATOR_TOOLS
from okg.substrate.mcp.server import build_server
from okg.substrate.nomos import (
    IdentityProvider,
    PermissionGrant,
    SubjectBinding,
    activate_policy_bundle,
    compile_policy_graph,
    upsert_identity_provider,
    upsert_permission_grant,
    upsert_subject_binding,
)
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres

# Tests that open the real MCP read pool log in as ``okg_mcp``. A fresh
# migration creates that role NOLOGIN, so without this fixture they pass
# only when an earlier test in the same session enabled the login.
_MCP_READ_POOL_LOGIN = pytest.mark.usefixtures("okg_mcp_can_log_in")


def _registered_tool_names(mcp) -> set[str]:
    return set(mcp._tool_manager._tools.keys())


def _published_graph_query(dsn: str, generation_id, sql: str, params=None):
    """Run `sql` against a Chronos checkout pinned to a published
    generation's checkpoint. The chronos-native cutover dropped
    okg.nodes_live / okg.edges_live; the graph at a published generation
    is read through this checkout over okg.graph_nodes / okg.graph_edges."""
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        graph = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).checkout_generation(generation_id)
        return graph.query(sql, params or {})


def _activate_operator_fixture_policy(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
) -> None:
    scope = f"deployment:{deployment_name}"
    bundle = compile_policy_graph({
        "policy_bundle_id": f"nomos:test:{deployment_name}:generation-publish",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_fixture_generation_publish",
                "effect": "allow",
                "actions": ["generation.publish"],
                "subjects": ["okg_service:publisher"],
                "priority": 100,
                "match": {
                    "deployment": deployment_name,
                    "scope": scope,
                },
                "reason_code": "nomos.test.generation_publish_allowed",
            },
            {
                "rule_id": "allow_fixture_mcp_tool_call",
                "effect": "allow",
                "actions": ["mcp.tool.call"],
                "subjects": ["subject:test-operator"],
                "priority": 100,
                "match": {
                    "deployment": deployment_name,
                    "scope": scope,
                },
                "reason_code": "nomos.test.mcp_tool_call_allowed",
            },
            {
                "rule_id": "allow_fixture_mcp_graph_read",
                "effect": "allow",
                "actions": ["graph.read.mcp"],
                "subjects": ["subject:test-operator"],
                "priority": 100,
                "match": {
                    "deployment": deployment_name,
                    "scope": scope,
                },
                "reason_code": "nomos.test.mcp_graph_read_allowed",
            },
        ],
    })
    activate_policy_bundle(
        conn,
        deployment_name=deployment_name,
        bundle=bundle,
        scope=scope,
        activated_by="test:operator-surface-fixture",
        bootstrap=True,
    )


def _seed_operator_fixture(
    dsn: str,
    *,
    deployment_name: str = "default",
) -> dict[str, int]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.catalog_versions (status, applied_at) "
                "VALUES ('applied', now()) RETURNING catalog_version_id"
            )
            cv = int(cur.fetchone()[0])
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
            cur.execute(
                """
                INSERT INTO okg.aliases
                  (alias, canonical, scope, catalog_version_id)
                VALUES ('alpha-alias', 'n:a', NULL, %s)
                """,
                (cv,),
            )
        write_patterns(conn, [IdentifierPattern("ticket", "CMS-[0-9]+")], cv)
        if deployment_name == "okg-workspace":
            _activate_operator_fixture_policy(
                conn,
                deployment_name=deployment_name,
            )

        run_id = str(uuid.uuid4())
        node_specs = [
            ("n:a", "alpha ticket CMS-1234", "doc"),
            ("n:b", "beta", "doc"),
        ]
        with conn.cursor() as cur:
            for node_id, text, kind in node_specs:
                cur.execute(
                    """
                    INSERT INTO okg.node_facts
                      (node_id, subtype, attrs, source, source_record_id,
                       source_revision, observed_at, dedupe_key, run_id, op)
                    VALUES
                      (%s, 'page', %s::jsonb, 'fixture', %s::jsonb,
                       '{"v":1}'::jsonb, now(), %s, %s, 'I')
                    """,
                    (
                        node_id,
                        json.dumps({"text": text, "kind": kind}),
                        json.dumps({"id": node_id}),
                        f"{run_id}:{node_id}",
                        run_id,
                    ),
                )
            edge_id = (hash("n:a|links|n:b") & ((1 << 63) - 1)) or 1
            cur.execute(
                """
                INSERT INTO okg.edge_facts
                  (edge_id, src, dst, edge_type, attrs, provenance,
                   confidence, source, source_record_id, source_revision,
                   observed_at, dedupe_key, run_id, op)
                VALUES
                  (%s, 'n:a', 'n:b', 'links', '{}'::jsonb, 'explicit',
                   1.0, 'fixture', '{"id":"n:a-links-n:b"}'::jsonb,
                   '{"v":1}'::jsonb, now(), %s, %s, 'I')
                """,
                (edge_id, f"{run_id}:n:a:links:n:b", run_id),
            )
        # Post chronos-native cutover the publisher checkpoints the Chronos
        # branch head; it does NOT project node_facts/edge_facts into a graph.
        # So write the graph through the canonical Chronos path (apply_facts)
        # alongside the audit-log fact rows above.
        from okg.substrate.graph_store import ChronosGraphStore
        from okg.substrate.library.sources.base import EdgeFact, NodeFact

        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(
                    node_id,
                    "page",
                    {"text": text, "kind": kind},
                    {"id": node_id},
                    {"v": 1},
                )
                for node_id, text, kind in node_specs
            ]
            + [
                EdgeFact(
                    "n:a",
                    "n:b",
                    "links",
                    source_record_id={"id": "n:a-links-n:b"},
                    source_revision={"v": 1},
                )
            ],
            source="fixture",
            strict_endpoints=True,
        )
    result = Publisher(dsn, deployment_name, catalog_identity=bind_test_catalog_writer(dsn, deployment_name)).cycle()
    assert result["status"] == "published"
    assert result["nodes_added"] == 2
    assert result["edges_added"] == 1
    return {
        # chronos_native: generation_id is a string checkpoint id, not an int.
        "generation_id": result["generation_id"],
        "catalog_version_id": cv,
    }


@_MCP_READ_POOL_LOGIN
def test_inspect_unsupported_kind_is_refused(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The canonical inspect operator MUST refuse a target kind that does
    not exist, with a stable error code, on both call paths.

    Since PACT effective-surface-declaration (commit 73b8fcbd3d, 2026-09-20)
    the inspect body runs a kind guard before its dispatch, so the
    dispatch's old ``unsupported_inspect_target`` answer is no longer
    reachable for a kind outside the declared list. The two paths now answer
    differently, and this test pins both:

    * Over the wire (``call_tool``, the path a real client takes), a kind
      that is in no inspect union fails the input model, and the validation
      seam answers with the guard's named refusal
      (``agent_surface_inspect_kind_unserved``, ``known_kind`` false) rather
      than ``invalid_operator_arguments`` / ``union_tag_invalid`` (Jason
      179a). The same code for a real kind that this launch withholds is
      pinned in test_effective_surface.py.
    * A direct Python call of the tool body skips the input model and hits
      the guard, which raises ``InspectKindNotServed`` and says the kind is
      not an inspect kind at all (``known_kind`` false). With the guard
      removed the body falls through to ``unsupported_inspect_target`` and
      this half fails.
    """
    import asyncio

    from okg.substrate.mcp.effective_surface import InspectKindNotServed
    from tests.substrate.mcp._result_payload import call_json, payload

    _seed_operator_fixture(clean_runtime, deployment_name="okg-workspace")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:test-operator")
    mcp = build_server(
        dsn=clean_runtime,
        dsn_authority={
            "dsn": clean_runtime,
            "authority": {
                "mode": "test_fixture",
                "deployment": "okg-workspace",
            },
            "target": None,
        },
        deployment_name="okg-workspace",
    )
    try:
        # Since #3059 a result is one text block; ask for the JSON format
        # to read the refusal envelope as a program would.
        result = asyncio.run(
            call_json(mcp, "inspect", {"target": {"kind": "not-a-target"}})
        )
        refusal = payload(result)
        assert refusal["status"] == "refused"
        error = refusal["error"]
        assert error["code"] == "agent_surface_inspect_kind_unserved"
        assert error["data"]["kind"] == "not-a-target"
        assert error["data"]["known_kind"] is False
        assert "not an inspect target kind at all" in error["message"]

        with pytest.raises(InspectKindNotServed) as excinfo:
            mcp._tool_manager._tools["inspect"].fn(
                target={"kind": "not-a-target"},
            )
        assert excinfo.value.code == "agent_surface_inspect_kind_unserved"
        assert excinfo.value.data["kind"] == "not-a-target"
        assert excinfo.value.data["known_kind"] is False
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


@_MCP_READ_POOL_LOGIN
def test_operator_surface_answers_fixture_generation(
    clean_runtime: str,
) -> None:
    """Smoke-test that each canonical operator returns the correct
    answer against a fixture generation. Previously this test compared
    the operator output to the legacy (``describe_graph``,
    ``list_neighbors``, ``run_sql``, ...) tool output; the legacy
    tools were removed by ``shrink-mcp-to-operator-surface``, so the
    test now exercises only the canonical operators.
    """
    fixture = _seed_operator_fixture(clean_runtime)
    gen = fixture["generation_id"]
    mcp = build_server(
        dsn=clean_runtime,
        embedder=build_embedding_execution_plan(
            EmbeddingConfig(enabled=True, backend="hash")
        ),
    )
    tools = mcp._tool_manager._tools
    try:
        inspected_graph = tools["inspect"].fn(
            target={"kind": "graph"},
            generation_id=gen,
        )
        assert inspected_graph["node_count"] == 2
        assert inspected_graph["edge_count"] == 1

        inspected_ontology = tools["inspect"].fn(
            target={"kind": "ontology"},
            generation_id=gen,
        )
        assert "subtypes" in inspected_ontology
        assert "narrowings" in inspected_ontology

        inspected_subtype = tools["inspect"].fn(
            target={"kind": "subtype", "subtype": "page"},
            generation_id=gen,
        )
        assert inspected_subtype["operator_name"] == "inspect"

        inspected_node = tools["inspect"].fn(
            target={"kind": "node", "node_id": "n:a"},
            generation_id=gen,
        )
        assert inspected_node["node_id"] == "n:a"
        assert "attrs" in inspected_node

        expanded = tools["expand"].fn(
            input={"node_id": "n:a"},
            generation_id=gen,
        )
        assert "neighbors" in expanded

        expanded_path = tools["expand"].fn(
            input={"node_id": "n:a"},
            path=["links:out"],
            generation_id=gen,
        )
        assert "results" in expanded_path

        aggregate_counts = tools["aggregate"].fn(
            input={"node_id": "n:a"},
            by=["edge_type", "dst_subtype"],
            metrics=["count"],
            generation_id=gen,
        )
        assert "counts" in aggregate_counts

        aggregate_routes = tools["aggregate"].fn(
            input={"kind": "edges"},
            by=["src_subtype", "edge_type", "dst_subtype"],
            metrics=["count"],
            generation_id=gen,
        )
        assert "routes" in aggregate_routes

        sql = "SELECT node_id FROM okg.v_nodes ORDER BY node_id"
        query_sql = tools["query"].fn(sql=sql, generation_id=gen)
        assert query_sql["rows"] == [["n:a"], ["n:b"]]

        searched_mentions = tools["search"].fn(
            query="See CMS-1234",
            method="identifier_mentions",
            generation_id=gen,
        )
        assert "mentions" in searched_mentions
        assert "results" in searched_mentions

        lexical = tools["search"].fn(
            query="alpha",
            method="lexical",
            generation_id=gen,
        )
        assert [row["node_id"] for row in lexical["results"]] == ["n:a"]
        alias = tools["search"].fn(
            query="alpha-alias",
            method="alias",
            generation_id=gen,
        )
        assert alias["results"][0]["canonical"] == "n:a"
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


@_MCP_READ_POOL_LOGIN
def test_filter_and_map_match_query_and_refuse_unsupported_forms(
    clean_runtime: str,
) -> None:
    fixture = _seed_operator_fixture(clean_runtime)
    gen = fixture["generation_id"]
    mcp = build_server(dsn=clean_runtime)
    tools = mcp._tool_manager._tools
    try:
        filtered = tools["filter"].fn(
            input={"kind": "nodes"},
            predicate={"op": "eq", "field": "attrs.kind", "value": "doc"},
            select=["node_id", "subtype"],
            generation_id=gen,
        )
        equivalent_filter = tools["query"].fn(
            sql=(
                "SELECT node_id, subtype FROM okg.v_nodes "
                'WHERE attrs @> \'{"kind":"doc"}\'::jsonb '
                "ORDER BY 1 LIMIT 50 OFFSET 0"
            ),
            generation_id=gen,
        )
        assert filtered["items"] == [
            {"node_id": "n:a", "subtype": "page"},
            {"node_id": "n:b", "subtype": "page"},
        ]
        assert [list(item.values()) for item in filtered["items"]] == (
            equivalent_filter["rows"]
        )
        assert "rows" not in filtered
        assert "columns" not in filtered

        mapped = tools["map"].fn(
            input={"kind": "nodes"},
            projection=["node_id", "subtype"],
            generation_id=gen,
        )
        equivalent_map = tools["query"].fn(
            sql=(
                "SELECT node_id AS node_id, subtype AS subtype "
                "FROM okg.v_nodes ORDER BY 1 LIMIT 50 OFFSET 0"
            ),
            generation_id=gen,
        )
        assert [list(item.values()) for item in mapped["items"]] == (
            equivalent_map["rows"]
        )
        assert "rows" not in mapped
        assert "columns" not in mapped

        bad_filter = tools["filter"].fn(
            input={"kind": "nodes"},
            predicate={"op": "regex", "field": "node_id", "value": "n:.*"},
            generation_id=gen,
        )
        assert bad_filter["status"] == "refused"
        assert bad_filter["error"]["code"] == "unsupported_operator_form"
        assert bad_filter["error"]["fallback_operator"] == "query"

        bad_map = tools["map"].fn(
            input={"kind": "nodes"},
            projection={"bad-alias!": "node_id"},
            generation_id=gen,
        )
        assert bad_map["status"] == "refused"
        assert bad_map["error"]["code"] == "unsupported_operator_form"
        assert bad_map["error"]["fallback_operator"] == "query"
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


@_MCP_READ_POOL_LOGIN
def test_filter_empty_predicate_preserves_subtype_scope(clean_runtime: str) -> None:
    fixture = _seed_operator_fixture(clean_runtime)
    gen = fixture["generation_id"]
    mcp = build_server(dsn=clean_runtime)
    try:
        filtered = mcp._tool_manager._tools["filter"].fn(
            input={"kind": "nodes", "subtype": "project_goal"},
            predicate={},
            select=["node_id", "subtype"],
            generation_id=gen,
        )
        assert filtered["items"] == []
        assert "rows" not in filtered
        assert "columns" not in filtered
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


@_MCP_READ_POOL_LOGIN
def test_every_filter_schema_hint_example_is_accepted_as_written(
    clean_runtime: str,
) -> None:
    """#3666: a refusal's schema_hint taught `value` for and/or/not/in/between
    and left out order_by, so following it was refused again. Each example the
    hint now shows is sent through the public tool call, exactly as written."""
    import anyio

    from okg.substrate.mcp.server import FILTER_HINT_EXAMPLES, FILTER_SCHEMA_HINT
    from tests.substrate.mcp._result_payload import call_json, payload

    fixture = _seed_operator_fixture(clean_runtime)
    gen = fixture["generation_id"]
    mcp = build_server(dsn=clean_runtime)

    async def send(arguments: dict) -> tuple[bool, dict]:
        result = await call_json(mcp, "filter", arguments)
        return bool(result.isError), payload(result)

    try:
        assert len(FILTER_HINT_EXAMPLES) >= 6
        for example in FILTER_HINT_EXAMPLES:
            # The hint shows the call as JSON; the call sent is that JSON.
            shown = ", ".join(
                f"{name}={json.dumps(value, separators=(',', ':'))}"
                for name, value in example.items()
            )
            assert f"filter({shown})" in FILTER_SCHEMA_HINT
            is_error, body = anyio.run(send, {**example, "generation_id": gen})
            assert not is_error, (example, body)
            assert body.get("status") != "refused", (example, body)
            assert "items" in body, (example, body)
        ops = {
            example["predicate"]["op"] for example in FILTER_HINT_EXAMPLES
        }
        assert {"in", "between", "and", "contains"} <= ops
        assert "order_by" in FILTER_SCHEMA_HINT
        assert "where" in FILTER_SCHEMA_HINT
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


@_MCP_READ_POOL_LOGIN
def test_the_order_by_refusal_names_what_can_be_sorted(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#3666: ordering by an undeclared attribute asked the reader to "declare
    its value_type", a step a reader cannot take. The refusal now names what
    sorts: here no attribute is declared, so the stable fields. Sent under the
    access rules (audit), where an undeclared attribute stays refused also
    once such an attribute sorts as text with the rules off."""
    import anyio

    from tests.substrate.mcp._result_payload import call_json, payload

    fixture = _seed_operator_fixture(clean_runtime, deployment_name="okg-workspace")
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "audit")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:test-operator")
    mcp = build_server(
        dsn=clean_runtime,
        dsn_authority={
            "dsn": clean_runtime,
            "authority": {"mode": "test_fixture", "deployment": "okg-workspace"},
            "target": None,
        },
        deployment_name="okg-workspace",
    )

    async def send(arguments: dict) -> object:
        return await call_json(mcp, "filter", arguments)

    try:
        result = anyio.run(send, {
            "input": {"kind": "nodes"}, "predicate": {},
            "order_by": "attrs.kind", "generation_id": fixture["generation_id"],
        })
        assert result.isError
        message = json.dumps(payload(result))
        assert "declare its value_type" not in message
        assert "no sortable attribute" in message, message
        assert "node_id, subtype, text_hash" in message, message
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


def test_the_order_by_refusal_names_the_first_twenty_sortable_attributes() -> None:
    """#3666: with declared attributes the refusal names the first 20, then
    how many more, and the stable fields of the kind."""
    from okg.substrate.mcp.server import _order_by_not_sortable_message

    declared = {f"k{i:02d}": "num" for i in range(25)}
    message = _order_by_not_sortable_message("missing", kind="nodes", declared=declared)
    named = ", ".join(f"attrs.k{i:02d}" for i in range(20))
    assert f"Sortable: {named} and 5 more; or a field: node_id, subtype, text_hash" in message
    assert "attrs.k20" not in message
    edges = _order_by_not_sortable_message("x", kind="edges", declared={"b": "ts", "a": "num"})
    assert "Sortable: attrs.a, attrs.b; or a field: edge_id, src, dst, edge_type" in edges
    assert "more" not in edges


def test_eval_hardening_flags_a_filter_scope_leak() -> None:
    """The agent-memory benchmark's scope-leak detector, split out of the test above.

    The benchmark (src/okg/bench/agent_memory/) is this repository's own; the
    public copy deselects this test (release/public-mirror/overlay/tests/
    mirror_deselect.txt). Imported here, not at module level, so the rest of
    this file still loads there.
    """
    from okg.bench.agent_memory import eval_hardening as hardening

    leaking_empty_predicate = {
        "status": "ok",
        "items": [
            {"node_id": "goal:keep", "subtype": "project_goal"},
            {"node_id": "action:leak", "subtype": "action_item"},
        ],
    }
    assert (
        hardening.filter_scope_issue(leaking_empty_predicate, "project_goal")
        == "filter_scope_leak"
    )

    safe_refusal = {
        "status": "refused",
        "error": {"code": "unsupported_empty_predicate"},
    }
    assert hardening.filter_scope_issue(safe_refusal, "project_goal") is None

    scoped_result = {
        "status": "ok",
        "items": [
            {"node_id": "goal:one", "subtype": "project_goal"},
            {"node_id": "goal:two", "subtype": "project_goal"},
        ],
    }
    assert hardening.filter_scope_issue(scoped_result, "project_goal") is None


def test_agent_tool_allowlist_excludes_query_by_default(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    allowlist = ("inspect", "search", "expand", "filter", "map", "aggregate")
    monkeypatch.setenv("OKG_MCP_TOOL_ALLOWLIST", ",".join(allowlist))
    mcp = build_server(dsn=clean_runtime)
    try:
        names = _registered_tool_names(mcp)
        assert set(allowlist) <= names
        assert "query" not in names
        assert mcp._okg_tool_allowlist == tuple(sorted(allowlist))
        assert "query" in mcp._okg_pruned_tools
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


def test_propose_requires_authority_and_publishes_accepted_payload(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.admission import apply_generation_feedback
    from okg.substrate.runtime import admission_workflows

    _seed_review_catalog(
        clean_runtime,
        parent="alpha",
        edge_type="links_to",
        child="beta",
    )
    with mock_dbos_context():
        monkeypatch.setattr(
            admission_workflows,
            "_resolve_dsn",
            lambda _deployment: clean_runtime,
        )
        admission_workflows.register_admission_workflows(
            deployment="review-test",
        )
        # A default server does not advertise propose writes at all; the
        # agent cannot self-grant by passing authority fields.
        ro_mcp = build_server(
            dsn=clean_runtime,
            deployment_name="review-test",
        )
        try:
            assert "propose" not in ro_mcp._tool_manager._tools
        finally:
            ro_mcp._okg_pool.close()
            if ro_mcp._okg_write_pool:
                ro_mcp._okg_write_pool.close()

        mcp = build_server(
            dsn=clean_runtime,
            deployment_name="review-test",
            allow_writes=True,
        )
        tools = mcp._tool_manager._tools
        try:
            authority = {
                "producer_id": "agent:test",
                "producer_kind": "agent",
            }
            payload = {
                "nodes": [
                    {
                        "node_id": "review:alpha",
                        "subtype": "alpha",
                        "attrs": {"text": "alpha"},
                    },
                    {
                        "node_id": "review:beta",
                        "subtype": "beta",
                        "attrs": {"text": "beta"},
                    },
                ],
                "edges": [
                    {
                        "src": "review:alpha",
                        "dst": "review:beta",
                        "edge_type": "links_to",
                    },
                ],
            }
            submitted = tools["propose"].fn(
                action="submit",
                payload=payload,
                authority=authority,
                idempotency_key="review-valid",
            )
            duplicate = tools["propose"].fn(
                action="submit",
                payload=payload,
                authority=authority,
                idempotency_key="review-valid",
            )
            assert submitted["status"] == "pending"
            assert duplicate["admission_id"] == submitted["admission_id"]
            assert duplicate["inserted"] is False

            decided = tools["propose"].fn(
                action="decide",
                payload={
                    "admission_id": submitted["admission_id"],
                    "decision": "accept",
                    "reason": "fixture",
                },
                authority={"actor": "reviewer", "allow_write": True},
            )
            assert decided["status"] == "accepted"
            assert decided["emitted"]["node_facts_inserted"] == 2
            assert decided["emitted"]["edge_facts_inserted"] == 1

            published = Publisher(clean_runtime, "review-test", catalog_identity=bind_test_catalog_writer(clean_runtime, "review-test")).cycle()
            assert published["status"] == "published"
            gen = published["generation_id"]  # chronos_native: a string checkpoint id
            with psycopg.connect(clean_runtime, autocommit=True) as conn:
                with conn.transaction():
                    feedback = apply_generation_feedback(
                        conn,
                        deployment_name="review-test",
                        generation_id=gen,
                    )
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT status, target_generation_ref
                          FROM okg.admission_envelopes
                         WHERE admission_id = %s::uuid
                        """,
                        (submitted["admission_id"],),
                    )
                    status, target_generation_ref = cur.fetchone()
            # The accepted payload's nodes/edge are visible in the Chronos
            # graph at the published generation (read through a checkout over
            # okg.graph_nodes / okg.graph_edges -- the legacy live tables are
            # dropped).
            live_nodes = int(
                _published_graph_query(
                    clean_runtime,
                    gen,
                    "SELECT count(*) AS n FROM okg.graph_nodes "
                    "WHERE node_id IN ('review:alpha', 'review:beta')",
                )[0]["n"]
            )
            live_edges = int(
                _published_graph_query(
                    clean_runtime,
                    gen,
                    "SELECT count(*) AS n FROM okg.graph_edges "
                    "WHERE src = 'review:alpha' AND dst = 'review:beta' "
                    "AND edge_type = 'links_to'",
                )[0]["n"]
            )
            assert feedback["updated"] == 1
            assert live_nodes == 2
            assert live_edges == 1
            assert status == "published"
            assert target_generation_ref == gen
        finally:
            mcp._okg_pool.close()
            if mcp._okg_write_pool:
                mcp._okg_write_pool.close()


def test_propose_rejects_narrowing_violation_without_live_visibility(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.runtime import admission_workflows

    _seed_review_catalog(
        clean_runtime,
        parent="alpha",
        edge_type="links_to",
        child="beta",
    )
    with mock_dbos_context():
        monkeypatch.setattr(
            admission_workflows,
            "_resolve_dsn",
            lambda _deployment: clean_runtime,
        )
        admission_workflows.register_admission_workflows(
            deployment="review-test",
        )
        mcp = build_server(
            dsn=clean_runtime,
            deployment_name="review-test",
            allow_writes=True,
        )
        tools = mcp._tool_manager._tools
        try:
            authority = {
                "producer_id": "agent:test",
                "producer_kind": "agent",
            }
            submitted = tools["propose"].fn(
                action="submit",
                payload={
                    "nodes": [
                        {"node_id": "bad:alpha", "subtype": "alpha"},
                        {"node_id": "bad:beta", "subtype": "beta"},
                    ],
                    "edges": [
                        {
                            "src": "bad:alpha",
                            "dst": "bad:beta",
                            "edge_type": "not_allowed",
                        },
                    ],
                },
                authority=authority,
                idempotency_key="review-invalid",
            )
            decided = tools["propose"].fn(
                action="decide",
                payload={
                    "admission_id": submitted["admission_id"],
                    "decision": "accept",
                },
                authority={"actor": "reviewer", "allow_write": True},
            )
            assert decided["status"] == "rejected"
            assert decided["payload_rejected"] is True

            with psycopg.connect(clean_runtime, autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT status, verdict
                          FROM okg.admission_envelopes
                         WHERE admission_id = %s::uuid
                        """,
                        (submitted["admission_id"],),
                    )
                    status, verdict = cur.fetchone()
                    cur.execute(
                        """
                        SELECT count(*)
                          FROM okg.edge_facts
                         WHERE source = %s
                        """,
                        ("agent:test",),
                    )
                    edge_facts = int(cur.fetchone()[0])
            assert status == "rejected"
            assert verdict["code"] == "narrowing_violation"
            assert edge_facts == 0
        finally:
            mcp._okg_pool.close()
            if mcp._okg_write_pool:
                mcp._okg_write_pool.close()


@_MCP_READ_POOL_LOGIN
def test_record_observation_templates_and_publish(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.admission import apply_generation_feedback
    from okg.substrate.runtime import admission_workflows

    _seed_agent_memory_catalog(clean_runtime, tmp_path)
    deployments_root = tmp_path / "deployments"
    record_dep = deployments_root / "record-test"
    (record_dep / "schemas").mkdir(parents=True)
    (record_dep / "source_registry.yaml").write_text("", encoding="utf-8")
    (record_dep / "deployment.yaml").write_text(
        """
name: record-test
description: record operator fixture
schema_dir: schemas
source_registry: source_registry.yaml
record_prefixes:
  - prefix: "fit:"
    target_subtype: "hep_fit"
    owner: "deployment"
session_start_context:
  slices:
    observations:
      sql: >
        SELECT node_id, subtype
        FROM okg.v_nodes
        WHERE subtype = 'agent_observation'
        ORDER BY node_id
        LIMIT 10
      limit: 5
""".lstrip(),
        encoding="utf-8",
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(deployments_root))
    with mock_dbos_context():
        monkeypatch.setattr(
            admission_workflows,
            "_resolve_dsn",
            lambda _deployment: clean_runtime,
        )
        admission_workflows.register_admission_workflows(
            deployment="record-test",
        )
        ro_mcp = build_server(
            dsn=clean_runtime,
            deployment_name="record-test",
        )
        try:
            assert "record" not in ro_mcp._tool_manager._tools
        finally:
            ro_mcp._okg_pool.close()
            if ro_mcp._okg_write_pool:
                ro_mcp._okg_write_pool.close()

        mcp = build_server(
            dsn=clean_runtime,
            deployment_name="record-test",
            allow_writes=True,
        )
        tools = mcp._tool_manager._tools
        try:
            templates = tools["inspect"].fn(
                target={
                    "kind": "record_templates",
                    "record_kind": "observation",
                },
            )
            assert templates["count"] == 1
            template = templates["templates"][0]
            assert template["kind"] == "observation"
            assert template["target_subtype"] == "agent_observation"
            assert "summary" in template["required_fields"]

            prefixes = tools["inspect"].fn(target={"kind": "record_prefixes"})
            by_prefix = {row["prefix"]: row for row in prefixes["prefixes"]}
            assert by_prefix["study:"]["target_subtype"] == "study"
            assert by_prefix["study:"]["active"] is True
            assert by_prefix["region:"]["target_subtype"] == "domain_entity"
            assert by_prefix["region:"]["active"] is True
            assert by_prefix["fit:"]["target_subtype"] == "hep_fit"
            assert by_prefix["fit:"]["active"] is False
            assert by_prefix["fit:"]["inactive_reason"] == ("target_subtype_missing")

            authority = {
                "producer_id": "agent:record-test",
                "producer_kind": "agent",
            }
            invalid = tools["record"].fn(
                kind="observation",
                payload={},
                authority=authority,
            )
            assert invalid["status"] == "refused"
            assert invalid["error"]["code"] == "invalid_record_payload"
            assert invalid["error"]["field"] == "summary"

            payload = {
                "summary": "The closure table was produced.",
                "confidence_kind": "deterministic",
                "study_ref": "study:closure-demo",
                "domain_refs": ["fit:abcd-v1"],
                "occurred_at": "2026-05-20T16:00:00Z",
            }
            submitted = tools["record"].fn(
                kind="observation",
                payload=payload,
                authority=authority,
            )
            duplicate = tools["record"].fn(
                kind="observation",
                payload=payload,
                authority=authority,
            )
            assert submitted["status"] == "pending"
            assert submitted["kind"] == "observation"
            assert submitted["template_id"] == ("builtin.agent_memory.observation.v1")
            assert submitted["edge_count"] == 0
            assert "unresolved_record_ref:study:closure-demo" in submitted["warnings"]
            assert (
                "unsupported_record_reference:domain_refs:fit:abcd-v1"
                in submitted["warnings"]
            )
            assert all(
                not warning.startswith("unrecognized_node_id:fit:")
                for warning in submitted["warnings"]
            )
            assert submitted["suppressed_edge_count"] == 2
            assert {
                diagnostic["reason"]
                for diagnostic in submitted["reference_diagnostics"]
            } == {"endpoint_not_live", "target_subtype_missing"}
            assert duplicate["admission_id"] == submitted["admission_id"]
            assert duplicate["inserted"] is False

            inspected = tools["inspect"].fn(
                target={
                    "kind": "record",
                    "record_id": submitted["record_id"],
                },
            )
            assert inspected["count"] == 1
            assert (
                inspected["items"][0]["check_summary"]["record"]["record_id"]
                == submitted["record_id"]
            )

            decided = tools["propose"].fn(
                action="decide",
                payload={
                    "admission_id": submitted["admission_id"],
                    "decision": "accept",
                    "reason": "fixture",
                },
                authority={"actor": "reviewer", "allow_write": True},
            )
            assert decided["status"] == "accepted"
            assert decided["emitted"]["node_facts_inserted"] == 1

            published = Publisher(clean_runtime, "record-test", catalog_identity=bind_test_catalog_writer(clean_runtime, "record-test")).cycle()
            assert published["status"] == "published"
            gen = published["generation_id"]
            with psycopg.connect(clean_runtime, autocommit=True) as conn:
                with conn.transaction():
                    feedback = apply_generation_feedback(
                        conn,
                        deployment_name="record-test",
                        generation_id=gen,
                    )
            rows = _published_graph_query(
                clean_runtime,
                gen,
                "SELECT subtype, attrs->>'summary' AS summary, "
                "attrs->>'confidence_kind' AS confidence_kind, "
                "attrs->>'extraction_method' AS extraction_method "
                "FROM okg.graph_nodes WHERE node_id = :nid",
                {"nid": submitted["record_id"]},
            )
            assert feedback["updated"] == 1
            assert len(rows) == 1
            row = rows[0]
            assert (
                row["subtype"],
                row["summary"],
                row["confidence_kind"],
                row["extraction_method"],
            ) == (
                "agent_observation",
                "The closure table was produced.",
                "deterministic",
                "record_operator",
            )
            session_start = tools["inspect"].fn(target={"kind": "session_start"})
            assert session_start["configured"] is True
            assert session_start["diagnostics"] == []
            assert session_start["slices"]["observations"]["columns"] == [
                "node_id",
                "subtype",
            ]
            assert submitted["record_id"] in {
                row[0] for row in session_start["slices"]["observations"]["rows"]
            }

            hypothesis = tools["record"].fn(
                kind="hypothesis",
                payload={
                    "statement": "The closure table is available.",
                    "evidence_refs": [submitted["record_id"]],
                    "occurred_at": "2026-05-20T16:05:00Z",
                },
                authority=authority,
            )
            assert hypothesis["status"] == "pending"
            assert hypothesis["edge_count"] == 1
            decided_hypothesis = tools["propose"].fn(
                action="decide",
                payload={
                    "admission_id": hypothesis["admission_id"],
                    "decision": "accept",
                    "reason": "fixture",
                },
                authority={"actor": "reviewer", "allow_write": True},
            )
            assert decided_hypothesis["status"] == "accepted"
            assert decided_hypothesis["emitted"]["edge_facts_inserted"] == 1

            second_publish = Publisher(clean_runtime, "record-test", catalog_identity=bind_test_catalog_writer(clean_runtime, "record-test")).cycle()
            assert second_publish["status"] == "published"
            supported_by_edges = int(
                _published_graph_query(
                    clean_runtime,
                    second_publish["generation_id"],
                    "SELECT count(*) AS n FROM okg.graph_edges "
                    "WHERE src = :src AND dst = :dst AND edge_type = 'supported_by'",
                    {"src": hypothesis["record_id"], "dst": submitted["record_id"]},
                )[0]["n"]
            )
            assert supported_by_edges == 1
        finally:
            mcp._okg_pool.close()
            if mcp._okg_write_pool:
                mcp._okg_write_pool.close()


@_MCP_READ_POOL_LOGIN
def test_record_iam_resolved_subject_overwrites_caller_provenance(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.runtime import admission_workflows

    deployment = "nomos-record-test"
    scope = f"deployment:{deployment}"
    provider = IdentityProvider(
        provider_id="cern-sso",
        provider_type="oidc",
        issuer="https://auth.cern.ch/auth/realms/cern",
        audiences=("akmon-cms",),
        key_reference="secret://cern-sso/jwks",
        freshness_seconds=3600,
        group_claims_approved=True,
    )
    bundle = compile_policy_graph(
        {
            "policy_bundle_id": "nomos:test:mcp-record",
            "policy_version": "v1",
            "default_effect": "deny",
            "rules": [
                {
                    "rule_id": "allow_cms_operator_record",
                    "effect": "allow",
                    "priority": 100,
                    "subjects": ["subject:cms:operator"],
                    "actions": ["mcp.record"],
                    "match": {
                        "deployment": deployment,
                        "scope": scope,
                        "identity_provider_id": "cern-sso",
                    },
                    "reason_code": "nomos.test.mcp_record_allowed",
                }
            ],
        }
    )
    with psycopg.connect(clean_runtime) as conn:
        activate_policy_bundle(
            conn,
            deployment_name=deployment,
            bundle=bundle,
            scope=scope,
            activated_by="test:nomos-record",
        )
        upsert_identity_provider(
            conn,
            deployment_name=deployment,
            provider=provider,
        )
        upsert_subject_binding(
            conn,
            deployment_name=deployment,
            binding=SubjectBinding(
                binding_id="binding:cern:cms-operator",
                provider_id=provider.provider_id,
                issuer=provider.issuer,
                claim_path="groups",
                claim_value="cms-comp-ops",
                subject="subject:cms:operator",
                scope=scope,
            ),
        )
        upsert_permission_grant(
            conn,
            deployment_name=deployment,
            grant=PermissionGrant(
                grant_id="grant:cms:record",
                subject="subject:cms:operator",
                actions=("mcp.record",),
                scope=scope,
                resource_match={"deployment": deployment},
            ),
            governing_rule_ids=("allow_cms_operator_record",),
        )
        conn.commit()

    _seed_agent_memory_catalog(clean_runtime, tmp_path)
    with mock_dbos_context():
        monkeypatch.setattr(
            admission_workflows,
            "_resolve_dsn",
            lambda _deployment: clean_runtime,
        )
        admission_workflows.register_admission_workflows(deployment=deployment)
        mcp = build_server(
            dsn=clean_runtime,
            deployment_name=deployment,
            allow_writes=True,
        )
        tools = mcp._tool_manager._tools
        try:
            allowed = tools["record"].fn(
                kind="observation",
                payload={"summary": "IAM provenance is server resolved."},
                authority={
                    "actor": "subject:forged-actor",
                    "subject": "subject:forged-subject",
                    "producer_id": "subject:forged-producer",
                    "nomos_identity_claim": {
                        "provider_id": provider.provider_id,
                        "verification_status": "valid",
                        "issuer": provider.issuer,
                        "claims": {
                            "sub": "cern-user-123",
                            "aud": "akmon-cms",
                            "groups": ["cms-comp-ops"],
                            "exp": (datetime.now(tz=UTC) + timedelta(hours=1)).isoformat(),
                            # The provider declares freshness_seconds,
                            # so the claim has to show when the user
                            # actually authenticated (issue #1007).
                            "auth_time": (datetime.now(tz=UTC) - timedelta(minutes=1)).isoformat(),
                        },
                    },
                },
            )
            denied = tools["record"].fn(
                kind="observation",
                payload={"summary": "This should not be admitted."},
                authority={
                    "nomos_identity_claim": {
                        "provider_id": provider.provider_id,
                        "verification_status": "valid",
                        "issuer": provider.issuer,
                        "claims": {
                            "sub": "cern-user-456",
                            "aud": "akmon-cms",
                            "groups": ["cms-visitors"],
                            "exp": (datetime.now(tz=UTC) + timedelta(hours=1)).isoformat(),
                            # The provider declares freshness_seconds,
                            # so the claim has to show when the user
                            # actually authenticated (issue #1007).
                            "auth_time": (datetime.now(tz=UTC) - timedelta(minutes=1)).isoformat(),
                        },
                    },
                },
            )
        finally:
            mcp._okg_pool.close()
            if mcp._okg_write_pool:
                mcp._okg_write_pool.close()

    assert allowed["status"] == "pending"
    assert allowed["nomos_decision"]["effect"] == "allow"
    assert allowed["nomos_decision"]["reason_code"] == ("nomos.test.mcp_record_allowed")
    assert denied["status"] == "refused"
    assert denied["error"]["code"] == "unauthorized_write"
    assert denied["error"]["authority"]["nomos_decision"]["effect"] == "deny"
    assert denied["error"]["authority"]["nomos_decision"]["reason_code"] == (
        "nomos.identity.no_subject_binding"
    )

    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT effect, reason_code, target->>'tool'
              FROM okg.nomos_decisions
             WHERE deployment_name = %s
             ORDER BY created_at, decision_row_id
            """,
            (deployment,),
        )
        decisions = cur.fetchall()
        cur.execute(
            """
            SELECT producer_id, authority
              FROM okg.admission_envelopes
             WHERE deployment_name = %s AND admission_id = %s
            """,
            (deployment, allowed["admission_id"]),
        )
        producer_id, admitted_authority = cur.fetchone()

    assert decisions == [
        # Policy activation now records its own decision evidence (PACT
        # nomos-activation-decision-required); for an opt-out deployment this is
        # the runtime opt-out activation allow, whose target carries no 'tool'.
        ("allow", "nomos.runtime.opt_out.activation", None),
        ("allow", "nomos.test.mcp_record_allowed", "record"),
        ("deny", "nomos.identity.no_subject_binding", "record"),
    ]
    assert producer_id == "subject:cms:operator"
    assert admitted_authority["actor"] == "subject:cms:operator"
    assert admitted_authority["subject"] == "subject:cms:operator"
    assert admitted_authority["producer_id"] == "subject:cms:operator"


@_MCP_READ_POOL_LOGIN
def test_record_reference_compatibility_publishes_supported_edges(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.runtime import admission_workflows

    _seed_all_modules_catalog(clean_runtime, tmp_path)
    _insert_node_facts(
        clean_runtime,
        [
            ("project:cooper", "project", {"project_id": "project:cooper"}),
            ("study:closure", "study", {"study_id": "study:closure"}),
            (
                "region:SR",
                "domain_entity",
                {
                    "domain_entity_id": "region:SR",
                    "domain_kind": "region",
                },
            ),
            (
                "file:demo:src/app.py",
                "source_file",
                {"path": "src/app.py"},
            ),
            (
                "commit:demo:abc123",
                "git_commit",
                {"commit_sha": "abc123"},
            ),
            (
                "test-report:demo",
                "test_report",
                {"report_id": "test-report:demo"},
            ),
            (
                "doc:demo:README.md",
                "document",
                {"document_id": "doc:demo:README.md"},
            ),
            (
                "artifact:closure-v3/report.json",
                "artifact",
                {"artifact_id": "artifact:closure-v3/report.json"},
            ),
            (
                "task:harden-record-reference-compatibility:5.1",
                "proposal_task",
                {
                    "task_id": ("task:harden-record-reference-compatibility:5.1"),
                },
            ),
        ],
    )
    seed_publish = Publisher(clean_runtime, "record-compat-test", catalog_identity=bind_test_catalog_writer(clean_runtime, "record-compat-test")).cycle()
    assert seed_publish["status"] == "published"

    with mock_dbos_context():
        monkeypatch.setattr(
            admission_workflows,
            "_resolve_dsn",
            lambda _deployment: clean_runtime,
        )
        admission_workflows.register_admission_workflows(
            deployment="record-compat-test",
        )
        mcp = build_server(
            dsn=clean_runtime,
            deployment_name="record-compat-test",
            allow_writes=True,
        )
        tools = mcp._tool_manager._tools
        authority = {
            "producer_id": "agent:record-compat-test",
            "producer_kind": "agent",
        }
        try:
            prefixes = tools["inspect"].fn(target={"kind": "record_prefixes"})
            by_prefix = {row["prefix"]: row for row in prefixes["prefixes"]}
            assert by_prefix["artifact:"]["active"] is True
            assert by_prefix["plot:"]["active"] is True
            assert by_prefix["self-play-run:"]["active"] is False
            assert by_prefix["self-play-run:"]["inactive_reason"] == (
                "target_subtype_missing"
            )

            observation = tools["record"].fn(
                kind="observation",
                payload={
                    "summary": "Closure context is available.",
                    "project_ref": "project:cooper",
                    "study_ref": "study:closure",
                    "domain_refs": ["region:SR"],
                    "code_refs": [
                        "file:demo:src/app.py",
                        "commit:demo:abc123",
                    ],
                    "artifact_refs": ["artifact:closure-v3/report.json"],
                    "reference_refs": ["doc:demo:README.md"],
                },
                authority=authority,
            )
            assert observation["status"] == "pending"
            assert observation["edge_count"] == 7
            assert observation["suppressed_edge_count"] == 0
            _accept_record(tools, observation["admission_id"])
            first_publish = Publisher(
                clean_runtime,
                "record-compat-test",
                catalog_identity=bind_test_catalog_writer(clean_runtime, "record-compat-test"),
            ).cycle()
            assert first_publish["status"] == "published"

            hypothesis = tools["record"].fn(
                kind="hypothesis",
                payload={
                    "statement": "Closure context has test evidence.",
                    "evidence_refs": [
                        observation["record_id"],
                        "test-report:demo",
                    ],
                },
                authority=authority,
            )
            verification = tools["record"].fn(
                kind="verification",
                payload={
                    "summary": "Task evidence was checked.",
                    "verdict": "verified",
                    "target_refs": [
                        "task:harden-record-reference-compatibility:5.1",
                    ],
                },
                authority=authority,
            )
            experiment = tools["record"].fn(
                kind="experiment_run",
                payload={
                    "summary": "Produced the closure report.",
                    "produced_refs": ["artifact:closure-v3/report.json"],
                },
                authority=authority,
            )
            assert hypothesis["edge_count"] == 2
            assert verification["edge_count"] == 1
            assert experiment["edge_count"] == 1
            for submitted in (hypothesis, verification, experiment):
                _accept_record(tools, submitted["admission_id"])

            second_publish = Publisher(
                clean_runtime,
                "record-compat-test",
                catalog_identity=bind_test_catalog_writer(clean_runtime, "record-compat-test"),
            ).cycle()
            assert second_publish["status"] == "published"
            with psycopg.connect(clean_runtime, autocommit=True) as conn:
                # Read the published edges from the Chronos graph at the publish
                # checkpoint (okg.edges_live was dropped by the cutover).
                from okg.substrate.graph_store import ChronosGraphStore

                graph = ChronosGraphStore.from_connection(
                    conn,
                    ensure_schema=False,
                ).checkout_generation(second_publish["generation_id"])
                srcs = [
                    observation["record_id"],
                    hypothesis["record_id"],
                    verification["record_id"],
                    experiment["record_id"],
                ]
                live_edges = {
                    (row["src"], row["edge_type"], row["dst"])
                    for row in graph.query(
                        "SELECT src, edge_type, dst FROM okg.graph_edges "
                        "WHERE src = ANY(:srcs) ORDER BY src, edge_type, dst",
                        {"srcs": srcs},
                    )
                }
                with conn.cursor() as cur:
                    cur.execute("SELECT count(*) FROM okg.edge_fact_strandings")
                    strandings = int(cur.fetchone()[0])

            assert strandings == 0
            assert {
                (observation["record_id"], "references", "project:cooper"),
                (observation["record_id"], "references", "study:closure"),
                (observation["record_id"], "references", "region:SR"),
                (
                    observation["record_id"],
                    "references",
                    "file:demo:src/app.py",
                ),
                (
                    observation["record_id"],
                    "references",
                    "commit:demo:abc123",
                ),
                (
                    observation["record_id"],
                    "references",
                    "artifact:closure-v3/report.json",
                ),
                (
                    observation["record_id"],
                    "references",
                    "doc:demo:README.md",
                ),
                (
                    hypothesis["record_id"],
                    "supported_by",
                    observation["record_id"],
                ),
                (
                    hypothesis["record_id"],
                    "supported_by",
                    "test-report:demo",
                ),
                (
                    verification["record_id"],
                    "verifies",
                    "task:harden-record-reference-compatibility:5.1",
                ),
                (
                    experiment["record_id"],
                    "produces",
                    "artifact:closure-v3/report.json",
                ),
            }.issubset(live_edges)

            unsupported = tools["record"].fn(
                kind="observation",
                payload={
                    "summary": "Unsupported references are suppressed.",
                    "study_ref": "study:missing",
                    "domain_refs": ["commit:demo:abc123"],
                    "reference_refs": ["self-play-run:demo"],
                },
                authority=authority,
            )
            assert unsupported["edge_count"] == 0
            assert unsupported["suppressed_edge_count"] == 3
            assert {
                diagnostic["reason"]
                for diagnostic in unsupported["reference_diagnostics"]
            } == {
                "endpoint_not_live",
                "field_target_not_allowed",
                "target_subtype_missing",
            }
            decided = _accept_record(tools, unsupported["admission_id"])
            assert decided["emitted"]["edge_facts_inserted"] == 0
            third_publish = Publisher(
                clean_runtime,
                "record-compat-test",
                catalog_identity=bind_test_catalog_writer(clean_runtime, "record-compat-test"),
            ).cycle()
            assert third_publish["status"] == "published"
            with psycopg.connect(clean_runtime, autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT count(*)
                          FROM okg.edge_fact_strandings
                        """,
                    )
                    assert int(cur.fetchone()[0]) == 0
        finally:
            mcp._okg_pool.close()
            if mcp._okg_write_pool:
                mcp._okg_write_pool.close()


def test_external_live_deployment_tool_is_cataloged_not_default_tool(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deployments_root = _write_deployment_with_tools(
        tmp_path,
        module_name="demo_tools_live_surface",
        tools_py="""
def live_search(query: str) -> dict:
    return {
        "boundary": "external_live",
        "source": "fixture_live_service",
        "observed_at": "2026-05-15T12:00:00Z",
        "query": {"text": query},
        "time_window": {"from": "now-1h", "to": "now"},
        "results": [{"id": "doc-1"}],
    }
""".lstrip(),
        agent_tools="""
  demo_live_search:
    module: demo_tools_live_surface
    function: live_search
    boundary: external_live
    optional: true
    requires_env: [DEMO_LIVE_TOKEN]
    description: Live external fixture search, not generation-pinned OKG evidence.
""".rstrip(),
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(deployments_root))
    saved_path = list(sys.path)
    try:
        operator_mcp = build_server(
            dsn=clean_runtime,
            deployment_name="demo",
        )
        try:
            # External_live deployment tools are NEVER registered on the
            # canonical operator surface; they are recorded in the
            # `_okg_skipped_deployment_tools` catalog for diagnostic
            # discovery instead. (Pre-cleanup this catalog was reached
            # via inspect(kind='tool_surface'); that surface was removed
            # by `shrink-mcp-to-operator-surface`.)
            assert "demo_live_search" not in _registered_tool_names(
                operator_mcp,
            )
            skipped = {
                row.name: row
                for row in operator_mcp._okg_skipped_deployment_tools  # type: ignore[attr-defined]
            }
            assert skipped["demo_live_search"].boundary == "external_live"
            assert skipped["demo_live_search"].generation_pinned is False
            assert skipped["demo_live_search"].profile == "compat"
        finally:
            operator_mcp._okg_pool.close()
    finally:
        sys.path[:] = saved_path


def test_deployment_macros_and_recipes_are_catalog_only(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deployments_root = _write_deployment_with_tools(
        tmp_path,
        module_name="demo_tools_catalog_only",
        tools_py="""
def live_search(query: str) -> dict:
    return {
        "boundary": "external_live",
        "source": "fixture_live_service",
        "observed_at": "2026-05-15T12:00:00Z",
        "query": {"text": query},
        "time_window": {"from": "now-1h", "to": "now"},
        "results": [{"id": "doc-1"}],
    }
""".lstrip(),
        agent_tools="""
  demo_neighbor_macro:
    kind: operator_macro
    operator: expand
    boundary: okg_graph
    description: Expand page neighbors through the standard operator.
  demo_top_pages:
    kind: query_recipe
    boundary: okg_graph
    sql: SELECT node_id FROM okg.v_nodes ORDER BY node_id LIMIT 10
    description: Query recipe for top page identifiers.
  demo_live_search:
    module: demo_tools_catalog_only
    function: live_search
    boundary: external_live
    optional: true
    requires_env: [DEMO_LIVE_TOKEN]
    description: Live external fixture search, not generation-pinned OKG evidence.
""".rstrip(),
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(deployments_root))
    saved_path = list(sys.path)
    try:
        operator_mcp = build_server(
            dsn=clean_runtime,
            deployment_name="demo",
        )
        try:
            # Operator-macro and query-recipe deployment declarations are
            # NEVER registered as MCP tools — they live in the
            # `_okg_skipped_deployment_tools` catalog for diagnostic
            # discovery so an agent can see what the deployment intended
            # to expose without polluting the canonical operator surface.
            assert _registered_tool_names(operator_mcp) == set(OPERATOR_TOOLS)
            skipped = {
                row.name: row
                for row in operator_mcp._okg_skipped_deployment_tools  # type: ignore[attr-defined]
            }
            assert skipped["demo_neighbor_macro"].kind == "operator_macro"
            assert skipped["demo_neighbor_macro"].operator == "expand"
            assert skipped["demo_neighbor_macro"].profile == "operator"
            assert skipped["demo_top_pages"].kind == "query_recipe"
            assert skipped["demo_top_pages"].sql.startswith(
                "SELECT node_id",
            )
            assert skipped["demo_live_search"].profile == "compat"
        finally:
            operator_mcp._okg_pool.close()
    finally:
        sys.path[:] = saved_path


def test_invalid_arbitrary_default_deployment_tool_fails_closed(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.deployments import DeploymentConfigError

    deployments_root = _write_deployment_with_tools(
        tmp_path,
        module_name="demo_tools_bad_default",
        tools_py="",
        agent_tools="""
  demo_bad_default:
    description: Missing kind or callable registration details.
""".rstrip(),
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(deployments_root))
    with pytest.raises(DeploymentConfigError, match="module must"):
        build_server(
            dsn=clean_runtime,
            deployment_name="demo",
        )


def _seed_review_catalog(
    dsn: str,
    *,
    parent: str,
    edge_type: str,
    child: str,
) -> int:
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.catalog_versions (status, applied_at) "
                "VALUES ('applied', now()) RETURNING catalog_version_id"
            )
            cv = int(cur.fetchone()[0])
            for subtype in (parent, child):
                cur.execute(
                    "INSERT INTO okg.subtypes (name, catalog_version_id) "
                    "VALUES (%s, %s)",
                    (subtype, cv),
                )
                cur.execute(
                    """
                    INSERT INTO okg.subtype_ancestors
                      (catalog_version_id, subtype_name, ancestor_name, depth)
                    VALUES (%s, %s, %s, 0)
                    ON CONFLICT DO NOTHING
                    """,
                    (cv, subtype, subtype),
                )
            cur.execute(
                """
                INSERT INTO okg.narrowings
                  (parent_subtype, edge_type, child_subtype,
                   catalog_version_id)
                VALUES (%s, %s, %s, %s)
                """,
                (parent, edge_type, child, cv),
            )
    return cv


def _seed_agent_memory_catalog(dsn: str, tmp_path: Path) -> int:
    import yaml

    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog

    dep = tmp_path / "record-agent-memory-dep"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(
        yaml.safe_dump(
            {
                "name": "record-agent-memory-dep",
                "description": "record operator agent-memory fixture",
                "modules": ["agent_memory"],
            }
        ),
        encoding="utf-8",
    )
    (dep / "schemas").mkdir()
    composed = compose_catalog(dep)
    with psycopg.connect(dsn) as conn:
        return int(apply_composed_catalog(conn, composed))


def _seed_all_modules_catalog(dsn: str, tmp_path: Path) -> int:
    import yaml

    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import (
        compose_catalog,
        load_modules_root,
    )

    modules_root = load_modules_root()
    modules = sorted(
        path.name for path in modules_root.iterdir() if (path / "schema.yaml").is_file()
    )
    dep = tmp_path / "record-all-modules-dep"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(
        yaml.safe_dump(
            {
                "name": "record-all-modules-dep",
                "description": "record operator all-modules fixture",
                "modules": modules,
            }
        ),
        encoding="utf-8",
    )
    (dep / "schemas").mkdir()
    composed = compose_catalog(dep)
    with psycopg.connect(dsn) as conn:
        return int(apply_composed_catalog(conn, composed))


def _insert_node_facts(
    dsn: str,
    nodes: list[tuple[str, str, dict[str, object]]],
) -> None:
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    run_id = str(uuid.uuid4())
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            for node_id, subtype, attrs in nodes:
                cur.execute(
                    """
                    INSERT INTO okg.node_facts
                      (node_id, subtype, attrs, source, source_record_id,
                       source_revision, observed_at, dedupe_key, run_id, op)
                    VALUES
                      (%s, %s, %s::jsonb, 'fixture', %s::jsonb,
                       '{"v":1}'::jsonb, now(), %s, %s, 'I')
                    """,
                    (
                        node_id,
                        subtype,
                        json.dumps(attrs),
                        json.dumps({"id": node_id}),
                        f"{run_id}:{node_id}",
                        run_id,
                    ),
                )
        # Write the graph through the canonical Chronos path so the
        # chronos-native publisher's checkpoint sees these nodes (it no
        # longer projects node_facts into a graph).
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(node_id, subtype, dict(attrs), {"id": node_id}, {"v": 1})
                for node_id, subtype, attrs in nodes
            ],
            source="fixture",
            strict_endpoints=True,
        )


def _accept_record(tools: dict[str, object], admission_id: str) -> dict:
    decided = tools["propose"].fn(
        action="decide",
        payload={
            "admission_id": admission_id,
            "decision": "accept",
            "reason": "fixture",
        },
        authority={"actor": "reviewer", "allow_write": True},
    )
    assert decided["status"] == "accepted"
    return decided


def _write_deployment_with_tools(
    tmp_path: Path,
    *,
    deployment_name: str = "demo",
    module_name: str,
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


@pytest.fixture(autouse=True)
def _restore_admission_workflow_registry() -> Iterator[None]:
    """Undo ``register_admission_workflows`` after each test.

    Several tests here register the admission workflows for a test-only
    deployment such as ``record-compat-test``. The registry is module-global,
    so without this a later MCP ``record`` call in the same process, in
    another test module, submits through that deployment and fails with
    ``DeploymentResolutionError``.
    """
    from okg.substrate.runtime import admission_workflows

    saved = dict(admission_workflows._DECORATED)
    yield
    admission_workflows._DECORATED.clear()
    admission_workflows._DECORATED.update(saved)


@pytest.fixture(autouse=True)
def _trust_caller_identity_claims(monkeypatch: pytest.MonkeyPatch) -> None:
    """These rows evaluate caller-supplied identity claims on a single-user
    server, which PACT mcp-oidc-principal-policy makes an explicit launch-time
    opt-in; without it the claim is refused (nomos_identity_claim_untrusted)."""
    monkeypatch.setenv("OKG_MCP_TRUST_CALLER_IDENTITY_CLAIMS", "1")
