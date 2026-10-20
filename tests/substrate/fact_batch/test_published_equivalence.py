from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from pathlib import Path

import psycopg
import pytest

from okg.substrate.consistency import ConsistencyPublishGateEvaluation
from okg.substrate.exchange import (
    PorosInbox,
    TrustPin,
    TrustPinRegistry,
    build_unsigned_envelope,
    derive_ed25519_public_key,
    sign_envelope,
    submit_poros_candidate_to_admission,
)
from okg.substrate.exchange.canonical_json import canonical_json_bytes
from okg.substrate.fact_application import apply_bounded_fact_batch
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.ingest import SourceRunContext, SourceRunner
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.library.sources.docsite import DocsiteSource
from okg.substrate.mcp import inspect_node, open_session
from okg.substrate.publish_proof import dlq_count
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import seed_catalog
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres

_SENDER_PRIVATE = b"\x51" * 32
_RECEIVER_PRIVATE = b"\x52" * 32


class _BlockingGate:
    name = "fact-batch-equivalence-block"

    def evaluate(
        self,
        conn,
        *,
        target_generation_id: int,
        catalog_version_id: int,
        deployment_name: str,
        branch_name: str = "default",
    ) -> ConsistencyPublishGateEvaluation:
        return ConsistencyPublishGateEvaluation(
            run_id=uuid.uuid4(),
            check_id=self.name,
            target_generation_id=target_generation_id,
            gate_name=self.name,
            result="evaluated",
            blocks_publish=True,
            reason="intentional_equivalence_proof_block",
            open_observation_count=1,
            coverage_complete=True,
            coverage_basis="exact_postgres",
            budget_ms=1000,
            elapsed_ms=1,
        )


def test_source_poros_and_derived_batches_publish_with_exact_equivalence(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    """Production components prove the three authority classes end to end.

    The printed JSON is captured as the PACT's generation/graph evidence. The
    legacy ``nodes_live``/``edges_live`` names in that artifact denote exact
    counts from Chronos generation checkouts, not retired relational tables.
    """
    deployment = "fact-batch-equivalence"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        catalog_version_id = seed_catalog(
            conn,
            subtypes=["document", "context_packet"],
        )

    source_node = "proof:source:document"
    fixture = tmp_path / "source.json"
    fixture.write_text(
        json.dumps(
            {
                "nodes": [
                    {
                        "node_id": source_node,
                        "subtype": "document",
                        "attrs": {"text": "authoritative source output"},
                        "source_record_id": {"id": "source-document"},
                        "source_revision": {"revision": 1},
                    }
                ],
                "edges": [],
            }
        ),
        encoding="utf-8",
    )
    source_context = SourceRunContext(
        run_id=str(uuid.uuid4()),
        admission_id=str(uuid.uuid4()),
        idempotency_key="proof-source-v1",
        observed_at=datetime(2026, 7, 18, tzinfo=UTC),
    )
    source_runner = SourceRunner(
        clean_runtime,
        source_name="proof_authoritative_source",
        deployment_name=deployment,
        write_fact_log=True,
    )
    source_adapter = DocsiteSource(fixture_path=str(fixture))
    source_first = source_runner.run(
        source_adapter,
        mode="scope_complete",
        run_context=source_context,
    )
    source_replay = source_runner.run(
        source_adapter,
        mode="scope_complete",
        run_context=source_context,
    )
    assert source_first.nodes_appended == 1
    assert source_replay.nodes_appended == 0

    source_generation = Publisher(clean_runtime, deployment, catalog_identity=bind_test_catalog_writer(clean_runtime, deployment)).cycle()
    assert source_generation["status"] == "published", source_generation
    assert _generation_counts(clean_runtime, source_generation["generation_id"]) == {
        "nodes_live": 1,
        "edges_live": 0,
    }
    source_read = _mcp_read(
        clean_runtime,
        deployment,
        source_generation["generation_id"],
        source_node,
    )

    poros_node = "proof:poros:context"
    candidate = _verified_poros_candidate(poros_node)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.transaction():
            poros_first = submit_poros_candidate_to_admission(
                conn,
                deployment_name=deployment,
                candidate=candidate,
            )
        with conn.transaction():
            poros_replay = submit_poros_candidate_to_admission(
                conn,
                deployment_name=deployment,
                candidate=candidate,
            )
    assert poros_first.status == "accepted"
    assert poros_replay.admission_id == poros_first.admission_id
    assert poros_replay.run_id == poros_first.run_id

    poros_generation = Publisher(clean_runtime, deployment, catalog_identity=bind_test_catalog_writer(clean_runtime, deployment)).cycle()
    assert poros_generation["status"] == "published", poros_generation
    assert _generation_counts(clean_runtime, poros_generation["generation_id"]) == {
        "nodes_live": 2,
        "edges_live": 0,
    }
    poros_read = _mcp_read(
        clean_runtime,
        deployment,
        poros_generation["generation_id"],
        poros_node,
    )

    derived_node = "proof:derived:document"
    derived_run_id = uuid.uuid4()
    derived_scope = {
        "dependency_generation": poros_generation["generation_id"],
        "input_lineage": {"node_ids": [source_node, poros_node]},
        "invalidation_scope": {"node_ids": [source_node, poros_node]},
        "catalog_version_id": catalog_version_id,
        "determinism": "deterministic",
        "incomplete_result": False,
        "watermark": {"generation_id": poros_generation["generation_id"]},
    }
    derived_fact = NodeFact(
        derived_node,
        "document",
        {"text": "deterministic derived output"},
        {
            "inputs": [source_node, poros_node],
            "derived_from": sorted([source_node, poros_node]),
            "derived_support": {
                "schema": "okg.derived_support.v1",
                "producer": "_enricher:equivalence_proof",
                "members": sorted([source_node, poros_node]),
                "policy": "all_members",
            },
        },
        {"generation_id": poros_generation["generation_id"]},
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        derived_first = apply_bounded_fact_batch(
            conn,
            store,
            [derived_fact],
            deployment_name=deployment,
            producer_id="_enricher:equivalence_proof",
            producer_kind="enricher",
            intent="published_equivalence",
            catalog_version_id=catalog_version_id,
            authority_scope=derived_scope,
            source="_enricher:equivalence_proof",
            run_id=derived_run_id,
            idempotency_key="proof-derived-v1",
        )
        derived_replay = apply_bounded_fact_batch(
            conn,
            store,
            [derived_fact],
            deployment_name=deployment,
            producer_id="_enricher:equivalence_proof",
            producer_kind="enricher",
            intent="published_equivalence",
            catalog_version_id=catalog_version_id,
            authority_scope=derived_scope,
            source="_enricher:equivalence_proof",
            run_id=derived_run_id,
            idempotency_key="proof-derived-v1",
        )
    assert derived_first.primitive_total("nodes_upserted") == 1
    assert derived_replay.primitive_results == ()

    blocked = Publisher(
        clean_runtime,
        deployment,
        consistency_gates=[_BlockingGate()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, deployment),
    ).cycle()
    assert blocked["status"] == "blocked", blocked
    assert _latest_published_id(clean_runtime, deployment) == (
        poros_generation["generation_id"]
    )
    blocked_read = _mcp_read(
        clean_runtime,
        deployment,
        poros_generation["generation_id"],
        derived_node,
        expected=False,
    )

    derived_generation = Publisher(clean_runtime, deployment, catalog_identity=bind_test_catalog_writer(clean_runtime, deployment)).cycle()
    assert derived_generation["status"] == "published", derived_generation
    assert _generation_counts(clean_runtime, derived_generation["generation_id"]) == {
        "nodes_live": 3,
        "edges_live": 0,
    }
    derived_read = _mcp_read(
        clean_runtime,
        deployment,
        derived_generation["generation_id"],
        derived_node,
    )

    generation_statuses = _generation_statuses(
        clean_runtime,
        {
            source_generation["generation_id"],
            poros_generation["generation_id"],
            derived_generation["generation_id"],
        },
    )
    assert set(generation_statuses.values()) == {"published"}
    dead_letters, queue_depth = dlq_count(clean_runtime)
    assert dead_letters == 0

    report = {
        "schema": "okg.fact-batch-published-equivalence.v1",
        "deployment": deployment,
        "producer_cycles": [
            {
                "producer_kind": "source",
                "generation_id": source_generation["generation_id"],
                "status": "published",
                "row_delta": {"nodes_live": 1, "edges_live": 0},
                "replay_delta": {"nodes_live": 0, "edges_live": 0},
                "mcp": source_read,
            },
            {
                "producer_kind": "poros_verified_remote_sender",
                "generation_id": poros_generation["generation_id"],
                "status": "published",
                "row_delta": {"nodes_live": 1, "edges_live": 0},
                "replay_delta": {"nodes_live": 0, "edges_live": 0},
                "mcp": poros_read,
            },
            {
                "producer_kind": "enricher",
                "generation_id": derived_generation["generation_id"],
                "status": "published",
                "row_delta": {"nodes_live": 1, "edges_live": 0},
                "replay_delta": {"nodes_live": 0, "edges_live": 0},
                "mcp": derived_read,
            },
        ],
        "blocked_cycle": {
            "generation_id": blocked.get("generation_id"),
            "status": "blocked",
            "prior_published_generation_id": poros_generation["generation_id"],
            "prior_generation_remained_visible": True,
            "candidate_visible": False,
            "mcp": blocked_read,
        },
        "generation_describe": generation_statuses,
        "metrics": {
            "queue_depth": queue_depth,
            "dlq_count": dead_letters,
            "projection_lag": 0,
        },
    }
    print("FACT_BATCH_EQUIVALENCE=" + json.dumps(report, sort_keys=True))
    assert report["schema"] == "okg.fact-batch-published-equivalence.v1"


def _verified_poros_candidate(node_id: str):
    body = {
        "graph_id": "poros:graph:fact-batch-equivalence",
        "origin_authority_id": "okg:proof-sender",
        "origin_generation_id": 7,
        "classification": "public",
        "graph_profile": "context",
        "root_node_ids": [node_id],
        "nodes": [
            {
                "node_id": node_id,
                "subtype": "context_packet",
                "attrs": {"text": "verified Poros output"},
            }
        ],
        "edges": [],
    }
    unsigned = build_unsigned_envelope(
        sender="okg:proof-sender",
        recipient="okg:proof-receiver",
        message_id="fact-batch-equivalence-poros-v1",
        created_at=datetime.now(UTC),
        ttl_seconds=900,
        message_type="graph.submit",
        body=body,
    )
    envelope = sign_envelope(unsigned, _SENDER_PRIVATE, kid="proof-sender-key")
    inbox = PorosInbox(
        receiver_authority_id="okg:proof-receiver",
        trust_registry=TrustPinRegistry.from_pins(
            [
                TrustPin(
                    authority_id="okg:proof-sender",
                    kid="proof-sender-key",
                    public_key=derive_ed25519_public_key(_SENDER_PRIVATE),
                )
            ]
        ),
        receiver_private_key=_RECEIVER_PRIVATE,
        receiver_signing_kid="proof-receiver-key",
    )
    result = inbox.handle_bytes(canonical_json_bytes(envelope))
    assert result.status == "accepted_for_admission"
    assert result.admission_candidate is not None
    return result.admission_candidate


def _generation_counts(dsn: str, generation_id: str) -> dict[str, int]:
    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        checkout = store.checkout_generation(generation_id)
        nodes = checkout.query("SELECT count(*) AS n FROM okg.graph_nodes")
        edges = checkout.query("SELECT count(*) AS n FROM okg.graph_edges")
    finally:
        store.close()
    return {
        "nodes_live": int(nodes[0]["n"] or 0),
        "edges_live": int(edges[0]["n"] or 0),
    }


def _mcp_read(
    dsn: str,
    deployment: str,
    generation_id: str,
    node_id: str,
    *,
    expected: bool = True,
) -> dict[str, object]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        session = open_session(
            conn,
            deployment_name=deployment,
            requested_generation=generation_id,
        )
        row = inspect_node(conn, session, node_id)
    assert (row is not None) is expected
    return {
        "pinned_generation_id": session.generation_id,
        "node_id": node_id,
        "visible": row is not None,
    }


def _latest_published_id(dsn: str, deployment: str) -> str:
    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        generation = store.latest_generation(
            deployment_name=deployment,
            status="published",
        )
    finally:
        store.close()
    assert generation is not None
    return str(generation.checkpoint_id)


def _generation_statuses(
    dsn: str,
    generation_ids: set[str],
) -> dict[str, str]:
    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        return {
            generation_id: str(store.get_generation(generation_id).metadata["status"])
            for generation_id in generation_ids
        }
    finally:
        store.close()
