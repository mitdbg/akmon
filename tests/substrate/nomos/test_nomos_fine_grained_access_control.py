from __future__ import annotations

import psycopg
import pytest

from okg.substrate.graph_store import ChronosGraphStore, _edge_id
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.mcp import expand_neighbors, inspect_node, open_session, search
from okg.substrate.nomos import activate_policy_bundle, compile_policy_graph
from okg.substrate.nomos.read_filter import filter_node_rows_for_session
from okg.substrate.nomos.source_lineage import (
    LINEAGE_ATTR,
    combine_source_lineages,
    graph_object_ref,
    horizon_ref,
    named_set_ref,
    normalize_source_lineage,
    source_record_contributors_from_lineage,
    source_record_ref,
    source_record_refs_from_lineage,
)
from okg.substrate.publisher import Publisher
from tests.substrate._db_helpers import seed_catalog_version
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


ALLOWED_TICKET = {"key": "OKG-100"}
DENIED_TICKET = {"key": "OKG-200"}
GRAPH_DENIED_TICKET = {"key": "OKG-300"}
FUTURE_TICKET = {"key": "OKG-400"}
VISIBLE_NOTE = {"id": "note-visible"}
EDGE_DENIED_NOTE = {"id": "note-edge-denied"}
DENIED_EDGE_RECORD = {"id": "edge-denied"}


def test_source_record_refs_are_generic_policy_facts() -> None:
    lineage = normalize_source_lineage(
        {
            "sources": [
                {
                    "source_id": "jira",
                    "source_record_ids": [{"key": "CMS-123"}],
                    "source_revisions": [{"updated_at": "2026-06-01T12:00:00Z"}],
                },
                {
                    "source_id": "git-files",
                    "source_record_ids": [
                        {"repo": "example-org/example-repo", "path": "src/okg/example.py"}
                    ],
                    "source_revisions": [{"sha": "abc123"}],
                },
                {
                    "source_id": "fixture_source",
                    "source_record_ids": ["row-7"],
                },
            ]
        }
    )

    refs = source_record_refs_from_lineage(lineage)
    expected = tuple(
        sorted(
            ref
            for ref in (
                source_record_ref("jira", {"key": "CMS-123"}),
                source_record_ref(
                    "git-files",
                    {"repo": "example-org/example-repo", "path": "src/okg/example.py"},
                ),
                source_record_ref("fixture_source", "row-7"),
            )
            if ref is not None
        )
    )
    contributors = source_record_contributors_from_lineage(lineage)

    assert refs == expected
    assert {contributor["source_id"] for contributor in contributors} == {
        "fixture_source",
        "git-files",
        "jira",
    }
    assert all("source_record_ref" in contributor for contributor in contributors)
    assert all("jira_ticket_key" not in contributor for contributor in contributors)


def test_single_mapping_source_record_ids_are_not_iterated_as_keys() -> None:
    lineage = normalize_source_lineage(
        {"sources": [{"source_id": "jira", "source_record_ids": {"key": "CMS-123"}}]}
    )

    assert source_record_refs_from_lineage(lineage) == (
        source_record_ref("jira", {"key": "CMS-123"}),
    )


def test_graph_node_and_edge_refs_are_policy_facts() -> None:
    assert graph_object_ref("node", "node:jira:CMS-123") == "node:node:jira:CMS-123"
    assert graph_object_ref(
        "edge",
        {
            "src": "node:jira:CMS-123",
            "edge_type": "duplicates",
            "dst": "node:jira:CMS-122",
        },
    ) == (
        'edge:{"dst":"node:jira:CMS-122","edge_type":"duplicates",'
        '"src":"node:jira:CMS-123"}'
    )
    assert named_set_ref("benchmark-fold:42") == "set:benchmark-fold:42"
    assert horizon_ref("2026-06-01T00:00:00Z") == "horizon:2026-06-01T00:00:00Z"


def test_same_source_record_revisions_stay_paired_for_horizons() -> None:
    old = normalize_source_lineage(
        source_id="jira",
        source_record_id={"key": "CMS-100"},
        source_revision={"updated_at": "2026-05-01T00:00:00Z"},
    )
    future = normalize_source_lineage(
        source_id="jira",
        source_record_id={"key": "CMS-400"},
        source_revision={"updated_at": "2026-07-01T00:00:00Z"},
    )

    contributors = {
        contributor["source_record_ref"]: contributor
        for contributor in source_record_contributors_from_lineage(
            combine_source_lineages([old, future])
        )
    }

    assert contributors[source_record_ref("jira", {"key": "CMS-100"})][
        "source_revision"
    ] == {"updated_at": "2026-05-01T00:00:00Z"}
    assert contributors[source_record_ref("jira", {"key": "CMS-400"})][
        "source_revision"
    ] == {"updated_at": "2026-07-01T00:00:00Z"}


@pytest.mark.postgres
def test_denied_lineage_contributor_hides_direct_and_derived_rows(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _publish_fine_grained_graph(clean_runtime)
    _activate_fine_grained_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:benchmark")

    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")
        visible = inspect_node(conn, session, "node:ticket-allowed")
        record_denied = inspect_node(conn, session, "node:ticket-record-denied")
        graph_denied = inspect_node(conn, session, "node:ticket-graph-denied")
        derived_mixed = inspect_node(conn, session, "node:derived-mixed")
        derived_allowed = inspect_node(conn, session, "node:derived-allowed")
        hits = search(conn, session, "ticket", method="lexical", limit=10)
        neighbors = expand_neighbors(conn, session, "node:ticket-allowed")

    assert visible is not None
    assert record_denied is None
    assert graph_denied is None
    assert derived_mixed is None
    assert derived_allowed is not None
    assert sorted(row["node_id"] for row in hits["results"]) == [
        "node:derived-allowed",
        "node:derived-future",
        "node:ticket-allowed",
        "node:ticket-future",
    ]
    assert [row["dst"] for row in neighbors["neighbors"]] == [
        "node:note-visible",
    ]


def test_malformed_fine_grained_lineage_does_not_broaden_access() -> None:
    malformed = normalize_source_lineage(
        {
            "status": "malformed",
            "sources": [
                {
                    "source_id": "jira",
                    "source_record_ids": [{"key": "CMS-123"}],
                }
            ],
        }
    )
    mixed = combine_source_lineages(
        [
            normalize_source_lineage(
                source_id="jira",
                source_record_id={"key": "CMS-100"},
            ),
            malformed,
        ]
    )

    assert source_record_refs_from_lineage(malformed) == ()
    assert mixed["status"] == "malformed"
    assert source_record_refs_from_lineage(mixed) == ()


@pytest.mark.postgres
def test_named_holdout_set_and_as_of_horizon_hide_records_generically(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation_id = _publish_fine_grained_graph(clean_runtime)
    _activate_fine_grained_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    context = {
        "context_id": "ctx:fold-alpha",
        "target_refs": [source_record_ref("jira_fixture", ALLOWED_TICKET)],
        "holdout_set_id": "benchmark-holdout",
        "holdout_refs": [source_record_ref("notes_fixture", VISIBLE_NOTE)],
        "as_of": "2026-06-15T00:00:00Z",
    }

    with psycopg.connect(clean_runtime) as conn:
        session = open_session(
            conn,
            deployment_name="default",
            requested_generation=generation_id,
            nomos_subject="subject:benchmark",
            nomos_access_context=context,
        )
        target = inspect_node(conn, session, "node:ticket-allowed")
        future = inspect_node(conn, session, "node:ticket-future")
        holdout = inspect_node(conn, session, "node:note-visible")
        derived = inspect_node(conn, session, "node:derived-allowed")
        derived_future = inspect_node(conn, session, "node:derived-future")
        neighbor_not_in_set = inspect_node(conn, session, "node:note-edge-denied")

    assert target is None
    assert future is None
    assert holdout is None
    assert derived is None
    assert derived_future is None
    assert neighbor_not_in_set is not None


@pytest.mark.postgres
def test_as_of_horizon_hides_same_source_derived_future_contributor(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation_id = _publish_fine_grained_graph(clean_runtime)
    _activate_fine_grained_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    context = {
        "context_id": "ctx:horizon-only",
        "as_of": "2026-06-15T00:00:00Z",
    }

    with psycopg.connect(clean_runtime) as conn:
        session = open_session(
            conn,
            deployment_name="default",
            requested_generation=generation_id,
            nomos_subject="subject:benchmark",
            nomos_access_context=context,
        )
        allowed = inspect_node(conn, session, "node:ticket-allowed")
        derived_allowed = inspect_node(conn, session, "node:derived-allowed")
        future = inspect_node(conn, session, "node:ticket-future")
        derived_future = inspect_node(conn, session, "node:derived-future")

    assert allowed is not None
    assert derived_allowed is not None
    assert future is None
    assert derived_future is None


@pytest.mark.postgres
def test_unaddressable_fine_grained_lineage_fails_closed_for_horizon_context(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation_id = _publish_fine_grained_graph(clean_runtime)
    _activate_fine_grained_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    context = {
        "context_id": "ctx:horizon-unaddressable",
        "as_of": "2026-06-15T00:00:00Z",
    }

    with psycopg.connect(clean_runtime) as conn:
        session = open_session(
            conn,
            deployment_name="default",
            requested_generation=generation_id,
            nomos_subject="subject:benchmark",
            nomos_access_context=context,
        )
        visible = filter_node_rows_for_session(
            conn,
            session,
            [
                {
                    "node_id": "node:legacy-source-only",
                    "subtype": "page",
                    "attrs": {"classification": "internal"},
                    "source": "jira_fixture",
                    "source_record_id": None,
                    "source_revision": None,
                }
            ],
        )

    assert visible == []


@pytest.mark.postgres
def test_unaddressable_context_lineage_fails_closed_under_default_allow_policy(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _publish_fine_grained_graph(clean_runtime)
    _activate_default_allow_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    context = {
        "context_id": "ctx:default-allow-horizon",
        "as_of": "2026-06-15T00:00:00Z",
    }

    with psycopg.connect(clean_runtime) as conn:
        session = open_session(
            conn,
            deployment_name="default",
            nomos_subject="subject:benchmark",
            nomos_access_context=context,
        )
        visible = filter_node_rows_for_session(
            conn,
            session,
            [
                {
                    "node_id": "node:legacy-source-only",
                    "subtype": "page",
                    "attrs": {"classification": "internal"},
                    "source": "jira_fixture",
                    "source_record_id": None,
                    "source_revision": None,
                }
            ],
        )

    assert visible == []


@pytest.mark.postgres
def test_named_set_can_hold_source_records_nodes_and_edges(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation_id = _publish_fine_grained_graph(clean_runtime)
    _activate_fine_grained_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    visible_edge_ref = graph_object_ref(
        "edge",
        _edge_id("node:ticket-allowed", "references", "node:note-visible"),
    )
    context = {
        "context_id": "ctx:mixed-set",
        "named_sets": {
            "benchmark-holdout": {
                "refs": [
                    source_record_ref("jira_fixture", FUTURE_TICKET),
                    graph_object_ref("node", "node:note-edge-denied"),
                    visible_edge_ref,
                ]
            }
        },
    }

    with psycopg.connect(clean_runtime) as conn:
        session = open_session(
            conn,
            deployment_name="default",
            requested_generation=generation_id,
            nomos_subject="subject:benchmark",
            nomos_access_context=context,
        )
        ticket = inspect_node(conn, session, "node:ticket-allowed")
        future = inspect_node(conn, session, "node:ticket-future")
        mixed_node_ref = inspect_node(conn, session, "node:note-edge-denied")
        visible_node = inspect_node(conn, session, "node:note-visible")
        neighbors = expand_neighbors(conn, session, "node:ticket-allowed")

    assert ticket is not None
    assert future is None
    assert mixed_node_ref is None
    assert visible_node is not None
    assert neighbors["neighbors"] == []


@pytest.mark.postgres
def test_evaluation_contexts_share_chronos_generation_with_different_holdouts(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation_id = _publish_fine_grained_graph(clean_runtime)
    _activate_fine_grained_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    context_a = {
        "context_id": "ctx:question-a",
        "target_refs": [source_record_ref("jira_fixture", ALLOWED_TICKET)],
        "as_of": "2026-06-15T00:00:00Z",
    }
    context_b = {
        "context_id": "ctx:question-b",
        "target_refs": [source_record_ref("jira_fixture", FUTURE_TICKET)],
        "as_of": "2026-08-01T00:00:00Z",
    }

    with psycopg.connect(clean_runtime) as conn:
        session_a = open_session(
            conn,
            deployment_name="default",
            requested_generation=generation_id,
            nomos_subject="subject:benchmark",
            nomos_access_context=context_a,
        )
        session_b = open_session(
            conn,
            deployment_name="default",
            requested_generation=generation_id,
            nomos_subject="subject:benchmark",
            nomos_access_context=context_b,
        )
        allowed_in_a = inspect_node(conn, session_a, "node:ticket-allowed")
        future_in_a = inspect_node(conn, session_a, "node:ticket-future")
        allowed_in_b = inspect_node(conn, session_b, "node:ticket-allowed")
        future_in_b = inspect_node(conn, session_b, "node:ticket-future")

    assert session_a.generation_id == generation_id
    assert session_b.generation_id == generation_id
    assert session_a.branch_name == session_b.branch_name == "default"
    assert allowed_in_a is None
    assert future_in_a is None
    assert allowed_in_b is not None
    assert future_in_b is None


@pytest.mark.postgres
def test_evaluation_context_decisions_record_generation_context_and_policy(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation_id = _publish_fine_grained_graph(clean_runtime)
    _activate_fine_grained_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    target_ref = source_record_ref("jira_fixture", ALLOWED_TICKET)
    context = {
        "context_id": "ctx:audit-keys",
        "target_refs": [target_ref],
        "as_of": "2026-06-15T00:00:00Z",
    }

    with psycopg.connect(clean_runtime) as conn:
        session = open_session(
            conn,
            deployment_name="default",
            requested_generation=generation_id,
            nomos_subject="subject:benchmark",
            nomos_access_context=context,
        )
        assert inspect_node(conn, session, "node:ticket-allowed") is None
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT policy_fingerprint, resource, target
                  FROM okg.nomos_decisions
                 WHERE deployment_name = 'default'
                   AND subject = 'subject:benchmark'
                   AND effect = 'deny'
                   AND reason_code = 'deny_context_target_refs'
                   AND resource->>'context_id' = 'ctx:audit-keys'
                   AND resource->>'source_record_ref' = %s
                 ORDER BY decision_row_id DESC
                 LIMIT 1
                """,
                (target_ref,),
            )
            row = cur.fetchone()

    assert row is not None
    policy_fingerprint, resource, target = row
    assert policy_fingerprint
    # Volatile publish coordinates stay OUT of decision identity (issue
    # #635); the audit-only target records them, including on fine-grained
    # source-record decisions like this one.
    assert "generation_id" not in resource
    assert "branch_name" not in resource
    assert "catalog_version_id" not in resource
    assert target["generation_id"] == generation_id
    assert target["branch_name"] == "default"
    assert target["catalog_version_id"] is not None
    assert resource["context_id"] == "ctx:audit-keys"
    assert resource["is_context_target"] is True


def _publish_fine_grained_graph(dsn: str) -> str:
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog_version(conn)
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    "node:ticket-allowed",
                    "page",
                    {"classification": "internal", "text": "allowed ticket"},
                    ALLOWED_TICKET,
                    {"updated_at": "2026-05-01T00:00:00Z"},
                ),
                NodeFact(
                    "node:ticket-record-denied",
                    "page",
                    {"classification": "internal", "text": "denied ticket"},
                    DENIED_TICKET,
                    {"updated_at": "2026-06-01T00:00:00Z"},
                ),
                NodeFact(
                    "node:ticket-graph-denied",
                    "page",
                    {"classification": "internal", "text": "graph denied ticket"},
                    GRAPH_DENIED_TICKET,
                    {"updated_at": "2026-05-15T00:00:00Z"},
                ),
                NodeFact(
                    "node:ticket-future",
                    "page",
                    {"classification": "internal", "text": "future ticket"},
                    FUTURE_TICKET,
                    {"updated_at": "2026-07-01T00:00:00Z"},
                ),
            ],
            source="jira_fixture",
        )
        store.apply_facts(
            [
                NodeFact(
                    "node:note-visible",
                    "page",
                    {"classification": "internal", "text": "visible note"},
                    VISIBLE_NOTE,
                    {"updated_at": "2026-05-01T00:00:00Z"},
                ),
                NodeFact(
                    "node:note-edge-denied",
                    "page",
                    {"classification": "internal", "text": "edge denied note"},
                    EDGE_DENIED_NOTE,
                    {"updated_at": "2026-05-01T00:00:00Z"},
                ),
                EdgeFact(
                    "node:ticket-allowed",
                    "node:note-visible",
                    "references",
                    {"classification": "internal"},
                    source_record_id={"id": "edge-visible"},
                    source_revision={"updated_at": "2026-05-01T00:00:00Z"},
                ),
                EdgeFact(
                    "node:ticket-allowed",
                    "node:note-edge-denied",
                    "references",
                    {"classification": "internal"},
                    source_record_id=DENIED_EDGE_RECORD,
                    source_revision={"updated_at": "2026-05-01T00:00:00Z"},
                ),
            ],
            source="notes_fixture",
        )
        allowed_lineage = normalize_source_lineage(
            source_id="jira_fixture",
            source_record_id=ALLOWED_TICKET,
            source_revision={"updated_at": "2026-05-01T00:00:00Z"},
        )
        denied_lineage = normalize_source_lineage(
            source_id="jira_fixture",
            source_record_id=DENIED_TICKET,
            source_revision={"updated_at": "2026-06-01T00:00:00Z"},
        )
        note_lineage = normalize_source_lineage(
            source_id="notes_fixture",
            source_record_id=VISIBLE_NOTE,
            source_revision={"updated_at": "2026-05-01T00:00:00Z"},
        )
        future_lineage = normalize_source_lineage(
            source_id="jira_fixture",
            source_record_id=FUTURE_TICKET,
            source_revision={"updated_at": "2026-07-01T00:00:00Z"},
        )
        store.apply_facts(
            [
                NodeFact(
                    "node:derived-mixed",
                    "page",
                    {
                        "classification": "internal",
                        "text": "derived ticket mixed",
                        LINEAGE_ATTR: combine_source_lineages(
                            [allowed_lineage, denied_lineage]
                        ),
                    },
                    {"id": "derived-mixed"},
                    {"updated_at": "2026-06-01T00:00:00Z"},
                ),
                NodeFact(
                    "node:derived-allowed",
                    "page",
                    {
                        "classification": "internal",
                        "text": "derived ticket allowed",
                        LINEAGE_ATTR: combine_source_lineages(
                            [allowed_lineage, note_lineage]
                        ),
                    },
                    {"id": "derived-allowed"},
                    {"updated_at": "2026-05-01T00:00:00Z"},
                ),
                NodeFact(
                    "node:derived-future",
                    "page",
                    {
                        "classification": "internal",
                        "text": "derived future ticket",
                        LINEAGE_ATTR: combine_source_lineages(
                            [allowed_lineage, future_lineage]
                        ),
                    },
                    {"id": "derived-future"},
                    {"updated_at": "2026-07-01T00:00:00Z"},
                ),
            ],
            source=None,
        )
    result = Publisher(dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()
    assert result["status"] == "published"
    return str(result["generation_id"])


def _activate_fine_grained_policy(dsn: str) -> None:
    denied_record_ref = source_record_ref("jira_fixture", DENIED_TICKET)
    denied_node_ref = graph_object_ref("node", "node:ticket-graph-denied")
    denied_edge_ref = graph_object_ref(
        "edge",
        _edge_id("node:ticket-allowed", "references", "node:note-edge-denied"),
    )
    bundle = compile_policy_graph(
        {
            "policy_bundle_id": "nomos:test:fine-grained-access-control",
            "policy_version": "v1",
            "default_effect": "deny",
            "rules": [
                {
                    "rule_id": "allow_mcp_tool_call",
                    "effect": "allow",
                    "actions": ["mcp.tool.call"],
                    "subjects": ["subject:benchmark", "subject:operator"],
                    "match": {
                        "deployment": "default",
                        "scope": "deployment:default",
                        "resource_kind": "mcp_tool",
                    },
                    "priority": 200,
                },
                {
                    "rule_id": "allow_mcp_graph_boundary",
                    "effect": "allow",
                    "actions": ["graph.read.mcp"],
                    "subjects": ["subject:benchmark", "subject:operator"],
                    "match": {
                        "deployment": "default",
                        "scope": "deployment:default",
                        "resource_kind": "mcp_graph_read_boundary",
                    },
                    "priority": 200,
                },
                {
                    "rule_id": "allow_mcp_graph_read_operator_boundary",
                    "effect": "allow",
                    "actions": ["graph.read.mcp"],
                    "subjects": ["subject:benchmark", "subject:operator"],
                    "match": {
                        "deployment": "default",
                        "scope": "deployment:default",
                        "operator_name": "mcp.read",
                    },
                    "priority": 200,
                },
                {
                    "rule_id": "allow_cli_search_boundary",
                    "effect": "allow",
                    "actions": ["graph.read.cli_search"],
                    "subjects": ["subject:benchmark", "subject:operator"],
                    "match": {
                        "resource_kind": "cli_search_boundary",
                        "query": "ticket",
                        "method": "lexical",
                    },
                    "priority": 200,
                },
                {
                    "rule_id": "deny_context_target_refs",
                    "effect": "deny",
                    "actions": ["graph.read.mcp"],
                    "subjects": ["subject:benchmark"],
                    "match": {"is_context_target": True},
                    "priority": 300,
                },
                {
                    "rule_id": "deny_context_holdout_sets",
                    "effect": "deny",
                    "actions": ["graph.read.mcp"],
                    "subjects": ["subject:benchmark"],
                    "match": {"set_ids": "benchmark-holdout"},
                    "priority": 300,
                },
                {
                    "rule_id": "deny_context_future_records",
                    "effect": "deny",
                    "actions": ["graph.read.mcp"],
                    "subjects": ["subject:benchmark"],
                    "match": {"is_after_as_of": True},
                    "priority": 300,
                },
                {
                    "rule_id": "deny_denied_ticket_record",
                    "effect": "deny",
                    "actions": ["graph.read.mcp"],
                    "subjects": ["subject:benchmark"],
                    "match": {"source_record_ref": denied_record_ref},
                    "priority": 300,
                },
                {
                    "rule_id": "deny_denied_ticket_node",
                    "effect": "deny",
                    "actions": ["graph.read.mcp"],
                    "subjects": ["subject:benchmark"],
                    "match": {"graph_ref": denied_node_ref},
                    "priority": 300,
                },
                {
                    "rule_id": "deny_denied_edge",
                    "effect": "deny",
                    "actions": ["graph.read.mcp"],
                    "subjects": ["subject:benchmark"],
                    "match": {"graph_ref": denied_edge_ref},
                    "priority": 300,
                },
                {
                    "rule_id": "allow_jira_fixture_source",
                    "effect": "allow",
                    "actions": ["graph.read.mcp"],
                    "subjects": ["subject:benchmark"],
                    "match": {
                        "deployment": "default",
                        "scope": "deployment:default",
                        "source_id": "jira_fixture",
                    },
                    "priority": 100,
                },
                {
                    "rule_id": "allow_notes_fixture_source",
                    "effect": "allow",
                    "actions": ["graph.read.mcp"],
                    "subjects": ["subject:benchmark"],
                    "match": {
                        "deployment": "default",
                        "scope": "deployment:default",
                        "source_id": "notes_fixture",
                    },
                    "priority": 100,
                },
                {
                    "rule_id": "allow_operator_jira_fixture_source",
                    "effect": "allow",
                    "actions": ["graph.read.mcp"],
                    "subjects": ["subject:operator"],
                    "match": {
                        "deployment": "default",
                        "scope": "deployment:default",
                        "source_id": "jira_fixture",
                    },
                    "priority": 100,
                },
                {
                    "rule_id": "allow_operator_notes_fixture_source",
                    "effect": "allow",
                    "actions": ["graph.read.mcp"],
                    "subjects": ["subject:operator"],
                    "match": {
                        "deployment": "default",
                        "scope": "deployment:default",
                        "source_id": "notes_fixture",
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


def _activate_default_allow_policy(dsn: str) -> None:
    bundle = compile_policy_graph(
        {
            "policy_bundle_id": "nomos:test:fine-grained-default-allow",
            "policy_version": "v1",
            "default_effect": "allow",
            "rules": [
                {
                    "rule_id": "allow_unrelated_subject",
                    "effect": "allow",
                    "actions": ["graph.read.mcp"],
                    "subjects": ["subject:someone-else"],
                    "match": {"deployment": "default"},
                    "priority": 1,
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
