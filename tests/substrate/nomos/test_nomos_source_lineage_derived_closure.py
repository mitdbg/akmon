from __future__ import annotations

import psycopg
import pytest

from okg.substrate.enrichers.derived_edges import (
    DerivedEdgeCandidate,
    insert_deterministic_edges,
)
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.ingest import SourceRunner
from okg.substrate.library.sources.base import NodeFact, SourceRun
from okg.substrate.nomos import (
    activate_policy_bundle,
    compile_policy_graph,
    read_nodes_with_active_policy,
)
from okg.substrate.nomos.source_lineage import (
    LINEAGE_ATTR,
    combine_source_lineages,
    normalize_source_lineage,
)


pytestmark = pytest.mark.postgres


class _SingleNodeAdapter:
    name = "policy-docs"
    profile = "discovery_crawl"

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: dict | None = None,
    ) -> SourceRun:
        del run_id, cursor
        return SourceRun(
            facts=[
                NodeFact(
                    "doc:policy",
                    "document",
                    {"classification": "internal", "text": "policy"},
                    {"id": "doc:policy"},
                    {"rev": "1"},
                )
            ],
            completed_scope=True,
            run_mode=mode,
        )


def test_source_runner_stamps_active_policy_fingerprint(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fingerprint = _activate_source_policy(clean_runtime, source_name="policy_docs")
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")

    SourceRunner(
        clean_runtime,
        source_name="policy_docs",
        deployment_name="default",
    ).run(_SingleNodeAdapter(), mode="scope_complete")

    with psycopg.connect(clean_runtime) as conn:
        graph = ChronosGraphStore.from_connection(conn, ensure_schema=False).checkout()
        attrs = graph.query(
            "SELECT attrs FROM okg.graph_nodes WHERE node_id = 'doc:policy'"
        )[0]["attrs"]

    assert attrs[LINEAGE_ATTR]["source_ids"] == ["policy_docs"]
    assert attrs[LINEAGE_ATTR]["sources"][0]["policy_fingerprints"] == [
        fingerprint
    ]


def test_deterministic_derived_edges_inherit_endpoint_lineage(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    "doc:public",
                    "document",
                    {"classification": "internal", "text": "public"},
                    {"id": "doc:public"},
                    {"rev": "1"},
                )
            ],
            source="public_docs",
            policy_fingerprint="sha256:public-policy",
        )
        store.apply_facts(
            [
                NodeFact(
                    "doc:google",
                    "document",
                    {"classification": "internal", "text": "google"},
                    {"id": "doc:google"},
                    {"rev": "1"},
                )
            ],
            source="google_drive",
            policy_fingerprint="sha256:google-policy",
        )
        with conn.cursor() as cur:
            result = insert_deterministic_edges(
                cur,
                source="_enricher:test",
                candidates=[
                    DerivedEdgeCandidate(
                        src="doc:public",
                        edge_type="references",
                        dst="doc:google",
                    )
                ],
            )
            cur.execute(
                """
                SELECT attrs
                  FROM okg.edge_facts
                 WHERE src = 'doc:public'
                   AND dst = 'doc:google'
                   AND edge_type = 'references'
                """
            )
            attrs = cur.fetchone()[0]

    assert result.inserted == 1
    lineage = attrs[LINEAGE_ATTR]
    assert lineage["status"] == "ok"
    assert lineage["source_ids"] == ["google_drive", "public_docs"]
    assert {
        source["source_id"]: source["policy_fingerprints"]
        for source in lineage["sources"]
    } == {
        "google_drive": ["sha256:google-policy"],
        "public_docs": ["sha256:public-policy"],
    }


def test_partial_source_authorization_hides_mixed_derived_node(
    clean_runtime: str,
) -> None:
    deployment = "nomos-derived-closure"
    scope = f"deployment:{deployment}"
    bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:derived-closure",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_public_docs",
                "effect": "allow",
                "priority": 100,
                "subjects": ["subject:reader"],
                "actions": ["graph.read"],
                "match": {
                    "deployment": deployment,
                    "scope": scope,
                    "source_id": "public_docs",
                },
            },
        ],
    })
    with psycopg.connect(clean_runtime) as conn:
        activate_policy_bundle(
            conn,
            deployment_name=deployment,
            bundle=bundle,
            scope=scope,
            activated_by="test:derived-closure",
        )
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    "doc:public",
                    "document",
                    {"classification": "internal", "text": "visible"},
                    {"id": "doc:public"},
                    {"rev": "1"},
                )
            ],
            source="public_docs",
        )
        store.apply_facts(
            [
                NodeFact(
                    "doc:mixed",
                    "document",
                    {
                        "classification": "internal",
                        "text": "mixed summary",
                        LINEAGE_ATTR: combine_source_lineages(
                            [
                                normalize_source_lineage("public_docs"),
                                normalize_source_lineage("google_drive"),
                            ]
                        ),
                    },
                    {"id": "doc:mixed"},
                    {"rev": "1"},
                )
            ],
            source=None,
        )
        generation_id = store.checkpoint_generation(0, deployment_name=deployment)
        rows = read_nodes_with_active_policy(
            conn,
            deployment_name=deployment,
            scope=scope,
            subject="subject:reader",
            generation_id=generation_id,
        )
        conn.commit()

    assert [row["node_id"] for row in rows] == ["doc:public"]


def _activate_source_policy(dsn: str, *, source_name: str) -> str:
    bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:source-lineage-derived",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_source_ingest",
                "effect": "allow",
                "priority": 100,
                "subjects": [f"source:{source_name}"],
                "actions": ["source.ingest", "source.emit"],
                "match": {
                    "deployment": "default",
                    "scope": "deployment:default",
                },
                "obligations": [{"type": "audit"}],
            },
        ],
    })
    with psycopg.connect(dsn) as conn:
        activate_policy_bundle(
            conn,
            deployment_name="default",
            bundle=bundle,
            scope="deployment:default",
            activated_by="test:derived-closure",
            bootstrap=True,
        )
        conn.commit()
    return bundle.policy_fingerprint
