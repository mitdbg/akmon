from __future__ import annotations

import psycopg
import pytest

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.nomos import (
    PermissionGrant,
    SubjectBinding,
    activate_policy_bundle,
    compile_policy_graph,
    institutional_provider_fixture,
    read_nodes_with_active_policy,
    read_nodes_with_identity_claim_from_postgres,
    upsert_identity_provider,
    upsert_permission_grant,
    upsert_subject_binding,
)
from okg.substrate.nomos.source_lineage import (
    LINEAGE_ATTR,
    normalize_source_lineage,
)


pytestmark = pytest.mark.postgres


def test_graph_writes_stamp_source_lineage_on_nodes_and_edges(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    "doc:a",
                    "document",
                    {"classification": "public", "text": "A"},
                    {"id": "doc:a"},
                    {"rev": "1"},
                ),
                NodeFact(
                    "doc:b",
                    "document",
                    {"classification": "public", "text": "B"},
                    {"id": "doc:b"},
                    {"rev": "1"},
                ),
                EdgeFact(
                    "doc:a",
                    "doc:b",
                    "references",
                    {"classification": "public"},
                    source_record_id={"id": "doc:a->doc:b"},
                    source_revision={"rev": "1"},
                ),
            ],
            source="public_docs",
        )
        graph = store.checkout()
        node = graph.query(
            "SELECT attrs FROM okg.graph_nodes WHERE node_id = 'doc:a'",
        )[0]["attrs"]
        edge = graph.query(
            "SELECT attrs FROM okg.graph_edges WHERE src = 'doc:a' AND dst = 'doc:b'",
        )[0]["attrs"]
        conn.commit()

    assert node[LINEAGE_ATTR]["source_ids"] == ["public_docs"]
    assert node[LINEAGE_ATTR]["sources"][0]["source_record_ids"] == [{"id": "doc:a"}]
    assert edge[LINEAGE_ATTR]["source_ids"] == ["public_docs"]
    assert edge[LINEAGE_ATTR]["sources"][0]["source_record_ids"] == [
        {"id": "doc:a->doc:b"}
    ]


def test_graph_writes_override_source_supplied_lineage_attrs(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    "doc:spoofed",
                    "document",
                    {
                        "classification": "internal",
                        "text": "spoofed",
                        LINEAGE_ATTR: normalize_source_lineage("public_docs"),
                    },
                    {"id": "doc:spoofed"},
                    {"rev": "1"},
                ),
            ],
            source="google_drive",
        )
        graph = store.checkout()
        attrs = graph.query(
            "SELECT attrs FROM okg.graph_nodes WHERE node_id = 'doc:spoofed'",
        )[0]["attrs"]
        conn.commit()

    assert attrs[LINEAGE_ATTR]["source_ids"] == ["google_drive"]
    assert attrs[LINEAGE_ATTR]["sources"][0]["source_record_ids"] == [
        {"id": "doc:spoofed"}
    ]


def test_read_policy_hides_rows_when_any_source_lineage_is_denied(
    clean_runtime: str,
) -> None:
    deployment = "nomos-source-lineage-read"
    scope = f"deployment:{deployment}"
    bundle = compile_policy_graph(_source_read_policy(deployment))
    with psycopg.connect(clean_runtime) as conn:
        activate_policy_bundle(
            conn,
            deployment_name=deployment,
            bundle=bundle,
            scope=scope,
            activated_by="test:source-lineage",
        )
        store = ChronosGraphStore.from_connection(conn)
        _seed_source_lineage_nodes(store)
        gen = store.checkpoint_generation(0, deployment_name=deployment)
        rows = read_nodes_with_active_policy(
            conn,
            deployment_name=deployment,
            scope=scope,
            subject="subject:source-reader",
            generation_id=gen,
        )
        conn.commit()

    assert [row["node_id"] for row in rows] == ["node:public-docs"]
    assert rows[0]["attrs"][LINEAGE_ATTR]["source_ids"] == ["public_docs"]


def test_identity_source_grants_require_every_lineage_source(
    clean_runtime: str,
) -> None:
    deployment = "nomos-source-lineage-identity"
    scope = f"deployment:{deployment}"
    provider = institutional_provider_fixture(
        "mit-touchstone",
        audience="akmon-mit-lab",
        provider_id="mit-touchstone",
    )
    bundle = compile_policy_graph(_identity_read_policy(deployment))
    with psycopg.connect(clean_runtime) as conn:
        activate_policy_bundle(
            conn,
            deployment_name=deployment,
            bundle=bundle,
            scope=scope,
            activated_by="test:source-lineage",
        )
        upsert_identity_provider(conn, deployment_name=deployment, provider=provider)
        upsert_subject_binding(
            conn,
            deployment_name=deployment,
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
        upsert_permission_grant(
            conn,
            deployment_name=deployment,
            grant=_source_grant(
                "grant:mit:public-docs",
                source_id="public_docs",
                scope=scope,
            ),
        )
        store = ChronosGraphStore.from_connection(conn)
        _seed_source_lineage_nodes(store)
        gen = store.checkpoint_generation(0, deployment_name=deployment)
        claim = _mit_touchstone_claim(provider.provider_id, provider.issuer)
        public_only = read_nodes_with_identity_claim_from_postgres(
            conn,
            deployment_name=deployment,
            scope=scope,
            claim=claim,
            generation_id=gen,
            now="2026-05-25T12:00:00Z",
        )
        upsert_permission_grant(
            conn,
            deployment_name=deployment,
            grant=_source_grant(
                "grant:mit:google-drive",
                source_id="google_drive",
                scope=scope,
            ),
        )
        both_sources = read_nodes_with_identity_claim_from_postgres(
            conn,
            deployment_name=deployment,
            scope=scope,
            claim=claim,
            generation_id=gen,
            now="2026-05-25T12:00:00Z",
        )
        conn.commit()

    assert [row["node_id"] for row in public_only] == ["node:public-docs"]
    assert [row["node_id"] for row in both_sources] == [
        "node:google-drive",
        "node:mixed-derived",
        "node:public-docs",
    ]


def _seed_source_lineage_nodes(store: ChronosGraphStore) -> None:
    store.apply_facts(
        [
            NodeFact(
                "node:public-docs",
                "document",
                {"classification": "internal", "text": "public docs"},
                {"id": "public"},
                {"rev": "1"},
            ),
        ],
        source="public_docs",
    )
    store.apply_facts(
        [
            NodeFact(
                "node:google-drive",
                "document",
                {"classification": "internal", "text": "gdrive"},
                {"id": "gdrive"},
                {"rev": "1"},
            ),
        ],
        source="google_drive",
    )
    store.apply_facts(
        [
            NodeFact(
                "node:mixed-derived",
                "document",
                {
                    "classification": "internal",
                    "text": "mixed",
                    LINEAGE_ATTR: normalize_source_lineage(
                        ["public_docs", "google_drive"]
                    ),
                },
                {"id": "mixed"},
                {"rev": "1"},
            ),
            NodeFact(
                "node:missing-lineage",
                "document",
                {"classification": "internal", "text": "missing"},
                {"id": "missing"},
                {"rev": "1"},
            ),
        ],
        source=None,
    )


def _source_read_policy(deployment: str) -> dict:
    return {
        "policy_bundle_id": f"nomos:test:{deployment}",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_public_docs_read",
                "effect": "allow",
                "priority": 100,
                "subjects": ["subject:source-reader"],
                "actions": ["graph.read"],
                "match": {
                    "deployment": deployment,
                    "scope": f"deployment:{deployment}",
                    "source_id": "public_docs",
                },
            },
        ],
    }


def _identity_read_policy(deployment: str) -> dict:
    return {
        "policy_bundle_id": f"nomos:test:{deployment}",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_mit_graph_read",
                "effect": "allow",
                "priority": 100,
                "subjects": ["subject:mit:scientist"],
                "actions": ["graph.read"],
                "match": {
                    "deployment": deployment,
                    "scope": f"deployment:{deployment}",
                },
            },
        ],
    }


def _source_grant(
    grant_id: str,
    *,
    source_id: str,
    scope: str,
) -> PermissionGrant:
    return PermissionGrant(
        grant_id=grant_id,
        subject="subject:mit:scientist",
        actions=("graph.read",),
        scope=scope,
        classifications=("*",),
        resource_match={"source_id": source_id},
        priority=10,
    )


def _mit_touchstone_claim(provider_id: str, issuer: str) -> dict:
    return {
        "provider_id": provider_id,
        "verification_status": "valid",
        "issuer": issuer,
        "claims": {
            "kerberos_username": "jdoe",
            "aud": "akmon-mit-lab",
            "affiliations": ["researcher"],
            "acr": ["duo_mfa"],
            "exp": "2026-05-25T13:00:00Z",
            "auth_time": "2026-05-25T11:59:00Z",
        },
    }
