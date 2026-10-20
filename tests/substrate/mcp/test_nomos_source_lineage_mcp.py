from __future__ import annotations

import importlib.util
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg
import pytest

from okg.substrate.attributes import (
    materialize_effective_attributes_for_generation,
    record_attribute_observation,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.mcp import expand_neighbors, inspect_node, open_session
from okg.substrate.mcp.run_sql import RunSqlInvalid, run_sql
from okg.substrate.mcp.search import search
from okg.substrate.mcp.server import build_server
from okg.substrate.nomos import (
    PermissionGrant,
    SubjectBinding,
    activate_policy_bundle,
    compile_policy_graph,
    institutional_provider_fixture,
    upsert_identity_provider,
    upsert_permission_grant,
    upsert_subject_binding,
)
from okg.substrate.nomos.source_lineage import LINEAGE_ATTR, normalize_source_lineage
from okg.substrate.publisher import Publisher
from tests.substrate._db_helpers import seed_catalog_version
from tests.pytest.catalog_fixture_builders import seed_catalog
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def _load_ci_triage():
    path = Path(__file__).resolve().parents[3] / "deployments" / "okg-ci" / "triage.py"
    spec = importlib.util.spec_from_file_location("okg_ci_triage_nomos_test", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class _InProcessMcpClient:
    def __init__(self, mcp) -> None:
        self._mcp = mcp

    def call(self, tool: str, arguments: dict):
        return self._mcp._tool_manager._tools[tool].fn(**arguments)

    def close(self) -> None:
        pass


def test_mcp_row_reads_hide_denied_source_lineage(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _publish_lineage_graph(clean_runtime)
    _activate_source_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mcp-client")

    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")
        public_node = inspect_node(conn, session, "node:public-docs")
        google_node = inspect_node(conn, session, "node:google-drive")
        neighbors = expand_neighbors(conn, session, "node:public-docs")
        hits = search(conn, session, "docs", method="lexical", limit=10)

    assert public_node is not None
    assert google_node is None
    assert [row["dst"] for row in neighbors["neighbors"]] == ["node:public-more"]
    assert [row["node_id"] for row in hits["results"]] == ["node:public-docs"]


def test_mcp_graph_sql_refuses_unfiltered_lineage_under_nomos(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _publish_lineage_graph(clean_runtime)
    _activate_source_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mcp-client")

    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")
        with pytest.raises(RunSqlInvalid, match="source-lineage filtered"):
            run_sql(conn, session, "SELECT node_id FROM okg.v_nodes")


def test_mcp_filter_tool_hides_denied_source_lineage(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _publish_lineage_graph(clean_runtime)
    _activate_source_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mcp-client")

    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        tool = mcp._tool_manager._tools["filter"]
        nodes = tool.fn(
            input={"kind": "nodes"},
            predicate={},
            select=["node_id"],
            limit=10,
        )
        edges = tool.fn(
            input={"kind": "edges"},
            predicate={},
            select=["edge_id", "dst"],
            limit=10,
        )
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()

    assert [item["node_id"] for item in nodes["items"]] == [
        "node:public-docs",
        "node:public-more",
    ]
    assert nodes["total_count"] == 2
    assert [item["dst"] for item in edges["items"]] == ["node:public-more"]
    assert edges["total_count"] == 1


def test_ci_triage_composition_excludes_denied_jobs_and_history(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The real typed MCP boundary filters each join input before triage.

    A denied passing history row must not cross the boundary. Free-text job and
    runner names remain fenced, so cross-run classification stays explicitly
    inconclusive rather than using those values as query controls.
    """
    sha = "a" * 40
    old_sha = "b" * 40
    run_id = "ci-workflow:visible"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    run_id,
                    "ci_workflow",
                    {
                        "name": "CI",
                        "run_id": "101",
                        "sha": sha,
                        "status": "completed",
                        "conclusion": "failure",
                        "created_at": "2026-07-11T08:00:00Z",
                        "updated_at": "2026-07-11T08:05:00Z",
                    },
                    {"id": run_id},
                    {"v": 1},
                ),
                NodeFact(
                    "ci-job:visible",
                    "ci_job",
                    {
                        "name": "test",
                        "sha": sha,
                        "status": "completed",
                        "conclusion": "failure",
                        "runner_name": "linux",
                    },
                    {"id": "ci-job:visible"},
                    {"v": 1},
                ),
                EdgeFact(
                    run_id,
                    "ci-job:visible",
                    "contains",
                    {},
                    source_record_id={"id": "visible-contains"},
                    source_revision={"v": 1},
                ),
            ],
            source="public_docs",
        )
        store.apply_facts(
            [
                NodeFact(
                    "ci-workflow:denied-history",
                    "ci_workflow",
                    {
                        "name": "CI",
                        "run_id": "100",
                        "sha": old_sha,
                        "status": "completed",
                        "conclusion": "success",
                        "created_at": "2026-07-10T08:00:00Z",
                    },
                    {"id": "ci-workflow:denied-history"},
                    {"v": 1},
                ),
                NodeFact(
                    "ci-job:denied-passing-history",
                    "ci_job",
                    {
                        "name": "test",
                        "sha": old_sha,
                        "status": "completed",
                        "conclusion": "success",
                        "runner_name": "linux",
                    },
                    {"id": "ci-job:denied-passing-history"},
                    {"v": 1},
                ),
                EdgeFact(
                    "ci-workflow:denied-history",
                    "ci-job:denied-passing-history",
                    "contains",
                    {},
                    source_record_id={"id": "denied-contains"},
                    source_revision={"v": 1},
                ),
            ],
            source="google_drive",
        )
    published = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert published["status"] == "published"
    _activate_source_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mcp-client")

    triage = _load_ci_triage()
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        graph = triage.Graph(clean_runtime, client=_InProcessMcpClient(mcp))
        run = next(
            {"node_id": node["node_id"], **node["attrs"]}
            for node in graph.nodes("ci_workflow")
            if node["node_id"] == run_id
        )
        report = triage.build_operator_report(graph, run, since_iso=None)
        with psycopg.connect(clean_runtime) as conn:
            session = open_session(conn, deployment_name="default")
            with pytest.raises(RunSqlInvalid, match="source-lineage filtered"):
                run_sql(conn, session, "SELECT node_id FROM okg.v_nodes")
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()

    assert report["ok"] is True
    assert report["job_count"] == 1
    assert report["failing_job_count"] == 1
    assert report["jobs"][0]["node_id"] == "ci-job:visible"
    classification = report["diagnoses"][0]["classification"]
    assert classification["verdict"] == "inconclusive"
    assert classification["conclusive"] is False


def test_mcp_filter_uses_identity_claim_source_grants_for_lineage(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _publish_lineage_graph(clean_runtime)
    _activate_identity_source_policy(clean_runtime)
    _configure_mit_identity_grants(clean_runtime, source_ids=["public_docs"])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:env-bypass")

    authority = {"nomos_identity_claim": _mit_touchstone_claim()}
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        tools = mcp._tool_manager._tools
        filter_tool = tools["filter"]
        search_tool = tools["search"]
        public_only = filter_tool.fn(
            input={"kind": "nodes"},
            predicate={},
            select=["node_id"],
            limit=10,
            authority=authority,
        )
        public_edges = filter_tool.fn(
            input={"kind": "edges"},
            predicate={},
            select=["edge_id", "dst"],
            limit=10,
            authority=authority,
        )
        public_search = search_tool.fn(
            query="docs",
            method="lexical",
            limit=10,
            authority=authority,
        )
        _configure_mit_identity_grants(
            clean_runtime,
            source_ids=["public_docs", "google_drive"],
        )
        both_sources = filter_tool.fn(
            input={"kind": "nodes"},
            predicate={},
            select=["node_id"],
            limit=10,
            authority=authority,
        )
        both_edges = filter_tool.fn(
            input={"kind": "edges"},
            predicate={},
            select=["edge_id", "dst"],
            limit=10,
            authority=authority,
        )
        both_search = search_tool.fn(
            query="docs",
            method="lexical",
            limit=10,
            authority=authority,
        )
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()

    assert [item["node_id"] for item in public_only["items"]] == [
        "node:public-docs",
        "node:public-more",
    ]
    assert [item["dst"] for item in public_edges["items"]] == ["node:public-more"]
    assert [item["node_id"] for item in public_search["results"]] == [
        "node:public-docs",
    ]
    assert [item["node_id"] for item in both_sources["items"]] == [
        "node:google-drive",
        "node:mixed-derived",
        "node:public-docs",
        "node:public-more",
    ]
    assert {item["dst"] for item in both_edges["items"]} == {
        "node:google-drive",
        "node:public-more",
    }
    assert {item["node_id"] for item in both_search["results"]} == {
        "node:google-drive",
        "node:mixed-derived",
        "node:public-docs",
    }


def test_mcp_identity_claim_fail_closed_without_runtime_subject_bypass(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _publish_lineage_graph(clean_runtime)
    _activate_identity_source_policy(clean_runtime)
    _configure_mit_identity_grants(
        clean_runtime,
        source_ids=["public_docs"],
        include_boundary=False,
    )
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mit:scientist")

    authority = {"nomos_identity_claim": _mit_touchstone_claim()}
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        tool = mcp._tool_manager._tools["filter"]
        with pytest.raises(SubstrateError) as invalid_claim:
            tool.fn(
                input={"kind": "nodes"},
                predicate={},
                select=["node_id"],
                limit=10,
                authority={"nomos_identity_claim": "not-an-object"},
            )
        with pytest.raises(SubstrateError) as empty_claim:
            tool.fn(
                input={"kind": "nodes"},
                predicate={},
                select=["node_id"],
                limit=10,
                authority={"nomos_identity_claim": {}},
            )
        with pytest.raises(SubstrateError) as missing_boundary:
            tool.fn(
                input={"kind": "nodes"},
                predicate={},
                select=["node_id"],
                limit=10,
                authority=authority,
            )

        _configure_mit_identity_grants(
            clean_runtime,
            source_ids=["public_docs"],
            include_boundary=True,
        )
        with pytest.raises(SubstrateError) as unbound_claim:
            tool.fn(
                input={"kind": "nodes"},
                predicate={},
                select=["node_id"],
                limit=10,
                authority={
                    "nomos_identity_claim": _mit_touchstone_claim(
                        kerberos_username="unknown",
                    ),
                },
            )
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()

    assert invalid_claim.value.code == "nomos_identity_claim_invalid"
    assert empty_claim.value.code == "authority_evaluation_failed"
    empty_evidence = empty_claim.value.data["authority"]
    assert empty_evidence["evaluated"] is False
    assert "authorized" not in empty_evidence
    assert missing_boundary.value.code == "nomos_identity_denied"
    assert unbound_claim.value.code == "nomos_identity_denied"


@pytest.mark.parametrize(
    ("error_type", "error_message"),
    [
        (
            psycopg.OperationalError,
            "AdminShutdown: terminating connection due to administrator command",
        ),
        (psycopg.InterfaceError, "the connection is closed"),
    ],
    ids=["operational-error", "interface-error"],
)
def test_mcp_identity_claim_evaluation_failure_is_infrastructure_refusal(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    error_type: type[Exception],
    error_message: str,
) -> None:
    _publish_lineage_graph(clean_runtime)
    _activate_identity_source_policy(clean_runtime)
    _configure_mit_identity_grants(clean_runtime, source_ids=["public_docs"])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mit:scientist")

    def fail_evaluation(*_args: object, **_kwargs: object) -> None:
        raise error_type(error_message)

    monkeypatch.setattr(
        "okg.substrate.nomos.evaluate_identity_claim_from_postgres",
        fail_evaluation,
    )
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        with pytest.raises(SubstrateError) as failure:
            mcp._tool_manager._tools["filter"].fn(
                input={"kind": "nodes"},
                predicate={},
                select=["node_id"],
                limit=10,
                authority={"nomos_identity_claim": _mit_touchstone_claim()},
            )
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()

    assert failure.value.code == "authority_unavailable"
    evidence = failure.value.data["authority"]
    assert evidence["evaluated"] is False
    assert "authorized" not in evidence
    assert evidence["nomos"]["code"] == "nomos_authority_unavailable"
    assert error_message in evidence["nomos"]["message"]


def test_mcp_identity_claim_evaluation_failure_is_non_infrastructure_refusal(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _publish_lineage_graph(clean_runtime)
    _activate_identity_source_policy(clean_runtime)
    _configure_mit_identity_grants(clean_runtime, source_ids=["public_docs"])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mit:scientist")

    def fail_evaluation(*_args: object, **_kwargs: object) -> None:
        raise ValueError("internal evaluation bug")

    monkeypatch.setattr(
        "okg.substrate.nomos.evaluate_identity_claim_from_postgres",
        fail_evaluation,
    )
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        with pytest.raises(SubstrateError) as failure:
            mcp._tool_manager._tools["filter"].fn(
                input={"kind": "nodes"},
                predicate={},
                select=["node_id"],
                limit=10,
                authority={"nomos_identity_claim": _mit_touchstone_claim()},
            )
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()

    assert failure.value.code == "authority_evaluation_failed"
    evidence = failure.value.data["authority"]
    assert evidence["evaluated"] is False
    assert "authorized" not in evidence
    assert evidence["nomos"]["code"] == "nomos_authority_evaluation_failed"
    assert "internal evaluation bug" in evidence["nomos"]["message"]


def test_mcp_generation_delta_preserves_identity_claim_source_grants(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_generation = _publish_lineage_graph(
        clean_runtime,
        include_restricted=False,
    )
    _activate_identity_source_policy(clean_runtime)
    _configure_mit_identity_grants(clean_runtime, source_ids=["public_docs"])
    _append_restricted_lineage_graph(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mit:scientist")

    authority = {"nomos_identity_claim": _mit_touchstone_claim()}
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        tool = mcp._tool_manager._tools["inspect"]
        public_only = tool.fn(
            target={
                "kind": "generation_delta",
                "from_generation": first_generation,
                "to": "latest",
            },
            authority=authority,
        )
        _configure_mit_identity_grants(
            clean_runtime,
            source_ids=["public_docs", "google_drive"],
        )
        both_sources = tool.fn(
            target={
                "kind": "generation_delta",
                "from_generation": first_generation,
                "to": "latest",
            },
            authority=authority,
        )
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()

    assert public_only["added_node_count"] == 0
    assert public_only["added_edge_count"] == 0
    assert both_sources["added_node_count"] == 2
    assert both_sources["added_edge_count"] == 1


def test_effective_attrs_cannot_spoof_source_lineage_for_search(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _publish_lineage_graph(clean_runtime)
    _activate_source_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mcp-client")

    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")
        record_attribute_observation(
            conn,
            target_kind="node",
            target_id="node:google-drive",
            generation_id=session.generation_id,
            attr_key=LINEAGE_ATTR,
            value=normalize_source_lineage(source_id="public_docs"),
            producer="test:spoof",
        )
        materialized = materialize_effective_attributes_for_generation(
            conn,
            generation_id=session.generation_id,
        )
        hits = search(conn, session, "docs", method="lexical", limit=10)
        google_node = inspect_node(conn, session, "node:google-drive")

    assert materialized["overlaid"] == 0
    assert google_node is None
    assert [row["node_id"] for row in hits["results"]] == ["node:public-docs"]


def test_attr_sensitive_set_operators_hide_denied_source_lineage(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _publish_lineage_graph(clean_runtime)
    _activate_source_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mcp-client")

    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")
        record_attribute_observation(
            conn,
            target_kind="node",
            target_id="node:google-drive",
            generation_id=session.generation_id,
            attr_key="derived_flag",
            value=True,
            producer="test:overlay",
        )
        materialize_effective_attributes_for_generation(
            conn,
            generation_id=session.generation_id,
        )

    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        tools = mcp._tool_manager._tools
        filtered = tools["filter"].fn(
            input={"kind": "nodes"},
            predicate={
                "op": "eq",
                "field": "attrs.derived_flag",
                "value": True,
            },
            select=["node_id", "attrs.derived_flag"],
        )
        mapped = tools["map"].fn(
            input={"kind": "nodes"},
            projection=["node_id", "attrs.derived_flag"],
        )
        with pytest.raises(SubstrateError) as aggregate_exc:
            tools["aggregate"].fn(
                input={"kind": "nodes"},
                by=["attrs.derived_flag"],
                metrics=["count"],
            )
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()

    assert filtered["items"] == []
    assert "node:google-drive" not in {item["node_id"] for item in mapped["items"]}
    assert aggregate_exc.value.code == "unsupported_operator_form"


def test_deployment_ready_uses_filtered_mcp_sample_under_nomos(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _publish_lineage_graph(clean_runtime)
    _activate_source_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")

    from okg.substrate.cli.deployment import _ready_mcp_status

    with psycopg.connect(clean_runtime) as conn:
        status = _ready_mcp_status(conn, "default")

    assert status["ok"] is True
    assert status["visible_sample_count"] == 2
    assert status["inspect_graph"]["status"] == "skipped"
    assert status["nomos"] == {
        "runtime_enforcement": True,
        "probe": "source_lineage_filtered_mcp_sample",
        "subject": "subject:mcp_client",
    }
    assert "run_sql" not in status


def _publish_lineage_graph(dsn: str, *, include_restricted: bool = True) -> str:
    with psycopg.connect(dsn, autocommit=True) as conn:
        # Path expansion now validates every requested edge against the catalog
        # pinned with the generation. Keep this fixture a valid graph instead of
        # relying on the pre-contract undeclared-edge fallback.
        seed_catalog(
            conn,
            subtypes=["page"],
            narrowings=[("page", "references", "page")],
        )
        store = ChronosGraphStore.from_connection(conn)
        _apply_public_lineage_facts(store)
        if include_restricted:
            _apply_restricted_lineage_facts(store)
    result = Publisher(dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()
    assert result["status"] == "published"
    return str(result["generation_id"])


def _append_restricted_lineage_graph(dsn: str) -> str:
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        _apply_restricted_lineage_facts(store)
    result = Publisher(dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()
    assert result["status"] == "published"
    return str(result["generation_id"])


def _apply_public_lineage_facts(store: ChronosGraphStore) -> None:
    store.apply_facts(
        [
            NodeFact(
                "node:public-docs",
                "page",
                {"classification": "internal", "text": "public docs"},
                {"id": "node:public-docs"},
                {"v": 1},
            ),
            NodeFact(
                "node:public-more",
                "page",
                {"classification": "internal", "text": "public more"},
                {"id": "node:public-more"},
                {"v": 1},
            ),
            EdgeFact(
                "node:public-docs",
                "node:public-more",
                "references",
                {"classification": "internal"},
                source_record_id={"id": "public-edge"},
                source_revision={"v": 1},
            ),
        ],
        source="public_docs",
    )


def _apply_restricted_lineage_facts(store: ChronosGraphStore) -> None:
    store.apply_facts(
        [
            NodeFact(
                "node:google-drive",
                "page",
                {"classification": "internal", "text": "google docs"},
                {"id": "node:google-drive"},
                {"v": 1},
            ),
            EdgeFact(
                "node:public-docs",
                "node:google-drive",
                "references",
                {"classification": "internal"},
                source_record_id={"id": "google-edge"},
                source_revision={"v": 1},
            ),
        ],
        source="google_drive",
    )
    store.apply_facts(
        [
            NodeFact(
                "node:mixed-derived",
                "page",
                {
                    "classification": "internal",
                    "text": "mixed docs",
                    LINEAGE_ATTR: normalize_source_lineage(
                        ["public_docs", "google_drive"]
                    ),
                },
                {"id": "node:mixed-derived"},
                {"v": 1},
            ),
        ],
        source=None,
    )


def _activate_source_policy(dsn: str) -> None:
    bundle = compile_policy_graph(
        {
            "policy_bundle_id": "nomos:test:mcp-source-lineage",
            "policy_version": "v1",
            "default_effect": "deny",
            "rules": [
                {
                    "rule_id": "allow_mcp_tool_call",
                    "effect": "allow",
                    "actions": ["mcp.tool.call"],
                    "subjects": ["subject:mcp-client", "subject:mcp_client"],
                    "match": {
                        "deployment": "default",
                        "scope": "deployment:default",
                        "resource_kind": "mcp_tool",
                    },
                    "priority": 100,
                },
                {
                    "rule_id": "allow_mcp_graph_boundary",
                    "effect": "allow",
                    "actions": ["graph.read.mcp"],
                    "subjects": ["subject:mcp-client", "subject:mcp_client"],
                    "match": {
                        "deployment": "default",
                        "scope": "deployment:default",
                        "resource_kind": "mcp_graph_read_boundary",
                    },
                    "priority": 100,
                },
                {
                    "rule_id": "allow_mcp_graph_read_operator_boundary",
                    "effect": "allow",
                    "actions": ["graph.read.mcp"],
                    "subjects": ["subject:mcp-client", "subject:mcp_client"],
                    "match": {
                        "deployment": "default",
                        "scope": "deployment:default",
                        "operator_name": "mcp.read",
                    },
                    "priority": 100,
                },
                {
                    "rule_id": "allow_public_docs_mcp_read",
                    "effect": "allow",
                    "actions": ["graph.read.mcp"],
                    "subjects": ["subject:mcp-client", "subject:mcp_client"],
                    "match": {
                        "deployment": "default",
                        "scope": "deployment:default",
                        "source_id": "public_docs",
                    },
                    "priority": 100,
                },
            ],
        }
    )
    with psycopg.connect(dsn) as conn:
        activate_policy_bundle(
            conn,
            deployment_name="default",
            bundle=bundle,
            scope="deployment:default",
            activated_by="test:operator",
        )
        conn.commit()


def _activate_identity_source_policy(dsn: str) -> None:
    bundle = compile_policy_graph(
        {
            "policy_bundle_id": "nomos:test:mcp-identity-source-lineage",
            "policy_version": "v1",
            "default_effect": "deny",
            "rules": [
                {
                    "rule_id": "allow_mit_mcp_tool_call",
                    "effect": "allow",
                    "actions": ["mcp.tool.call"],
                    "subjects": ["subject:mit:scientist"],
                    "match": {
                        "deployment": "default",
                        "scope": "deployment:default",
                        "resource_kind": "mcp_tool",
                    },
                    "priority": 100,
                },
                {
                    "rule_id": "allow_mit_mcp_graph_boundary",
                    "effect": "allow",
                    "actions": ["graph.read.mcp"],
                    "subjects": ["subject:mit:scientist"],
                    "match": {
                        "deployment": "default",
                        "scope": "deployment:default",
                        "resource_kind": "mcp_graph_read_boundary",
                    },
                    "priority": 100,
                },
                {
                    "rule_id": "allow_mit_public_docs_read",
                    "effect": "allow",
                    "actions": ["graph.read.mcp"],
                    "subjects": ["subject:mit:scientist"],
                    "match": {
                        "deployment": "default",
                        "scope": "deployment:default",
                        "source_id": "public_docs",
                    },
                    "priority": 100,
                },
                {
                    "rule_id": "allow_mit_google_drive_read",
                    "effect": "allow",
                    "actions": ["graph.read.mcp"],
                    "subjects": ["subject:mit:scientist"],
                    "match": {
                        "deployment": "default",
                        "scope": "deployment:default",
                        "source_id": "google_drive",
                    },
                    "priority": 100,
                },
            ],
        }
    )
    with psycopg.connect(dsn) as conn:
        activate_policy_bundle(
            conn,
            deployment_name="default",
            bundle=bundle,
            scope="deployment:default",
            activated_by="test:operator",
        )
        conn.commit()


def _configure_mit_identity_grants(
    dsn: str,
    *,
    source_ids: list[str],
    include_boundary: bool = True,
) -> None:
    scope = "deployment:default"
    provider = institutional_provider_fixture(
        "mit-touchstone",
        audience="akmon-mit-lab",
        provider_id="mit-touchstone",
    )
    with psycopg.connect(dsn) as conn:
        upsert_identity_provider(conn, deployment_name="default", provider=provider)
        upsert_subject_binding(
            conn,
            deployment_name="default",
            binding=SubjectBinding(
                binding_id="binding:mit:scientist",
                provider_id=provider.provider_id,
                issuer=provider.issuer,
                claim_path="kerberos_username",
                claim_value="jdoe",
                subject="subject:mit:scientist",
                scope=scope,
                required_assurance=("duo_mfa",),
                priority=10,
            ),
        )
        if include_boundary:
            upsert_permission_grant(
                conn,
                deployment_name="default",
                grant=PermissionGrant(
                    grant_id="grant:mit:mcp-tool",
                    subject="subject:mit:scientist",
                    actions=("mcp.tool.call",),
                    scope=scope,
                    resource_match={
                        "deployment": "default",
                        "resource_kind": "mcp_tool",
                    },
                    classifications=("*",),
                    priority=20,
                ),
            )
            upsert_permission_grant(
                conn,
                deployment_name="default",
                grant=PermissionGrant(
                    grant_id="grant:mit:mcp-boundary",
                    subject="subject:mit:scientist",
                    actions=("graph.read.mcp",),
                    scope=scope,
                    resource_match={
                        "deployment": "default",
                        "resource_kind": "mcp_graph_read_boundary",
                    },
                    classifications=("*",),
                    priority=20,
                ),
            )
        for source_id in source_ids:
            upsert_permission_grant(
                conn,
                deployment_name="default",
                grant=PermissionGrant(
                    grant_id=f"grant:mit:{source_id}",
                    subject="subject:mit:scientist",
                    actions=("graph.read.mcp",),
                    scope=scope,
                    resource_match={"source_id": source_id},
                    classifications=("*",),
                    priority=10,
                ),
            )
        conn.commit()


def _mit_touchstone_claim(*, kerberos_username: str = "jdoe") -> dict:
    # The MCP tool surface takes no clock, so these claims are evaluated
    # against real time (issue #1007). A live login produces live
    # timestamps; hardcoded ones would only ever have passed because the
    # temporal checks were being skipped.
    now = datetime.now(tz=UTC)
    return {
        "provider_id": "mit-touchstone",
        "verification_status": "valid",
        "issuer": "https://touchstone.mit.edu/",
        "claims": {
            "kerberos_username": kerberos_username,
            "aud": "akmon-mit-lab",
            "affiliations": ["researcher"],
            "acr": ["duo_mfa"],
            "exp": (now + timedelta(hours=1)).isoformat(),
            "auth_time": (now - timedelta(minutes=1)).isoformat(),
        },
    }


@pytest.fixture(autouse=True)
def _trust_caller_identity_claims(monkeypatch: pytest.MonkeyPatch) -> None:
    """These rows evaluate caller-supplied identity claims on a single-user
    server, which PACT mcp-oidc-principal-policy makes an explicit launch-time
    opt-in; without it the claim is refused (nomos_identity_claim_untrusted)."""
    monkeypatch.setenv("OKG_MCP_TRUST_CALLER_IDENTITY_CLAIMS", "1")
