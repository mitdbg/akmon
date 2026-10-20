from __future__ import annotations

import uuid

import psycopg
import pytest

from okg.substrate.consistency import ConsistencyPublishGateEvaluation
from okg.substrate.fact_application import apply_bounded_fact_batch
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import seed_catalog
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


class _BlockingGate:
    name = "fact-batch-visibility-block"

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
            reason="intentional_test_block",
            open_observation_count=1,
            coverage_complete=True,
            coverage_basis="exact_postgres",
            budget_ms=1000,
            elapsed_ms=1,
        )


def test_candidate_and_blocked_facts_remain_invisible_until_publisher(
    clean_runtime: str,
) -> None:
    deployment = "visibility-test"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = seed_catalog(conn, subtypes=["document"])
        store = ChronosGraphStore.from_connection(conn)
        _apply_system_node(
            conn,
            store,
            deployment=deployment,
            catalog_version_id=cv,
            node_id="visibility:baseline",
        )
    baseline = Publisher(clean_runtime, deployment, catalog_identity=bind_test_catalog_writer(clean_runtime, deployment)).cycle()
    assert baseline["status"] == "published", baseline

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        _apply_system_node(
            conn,
            store,
            deployment=deployment,
            catalog_version_id=cv,
            node_id="visibility:candidate",
        )
        candidate_rows = store.checkout().query(
            "SELECT node_id FROM okg.graph_nodes "
            "WHERE node_id = 'visibility:candidate'"
        )
        pinned_before = store.checkout_generation(
            baseline["generation_id"]
        ).query(
            "SELECT node_id FROM okg.graph_nodes "
            "WHERE node_id = 'visibility:candidate'"
        )

    assert candidate_rows == [{"node_id": "visibility:candidate"}]
    assert pinned_before == []

    blocked = Publisher(
        clean_runtime,
        deployment,
        consistency_gates=[_BlockingGate()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, deployment),
    ).cycle()
    assert blocked["status"] == "blocked", blocked

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        current_published = store.latest_generation(
            deployment_name=deployment,
            status="published",
        )
        visible_while_blocked = store.checkout_generation(
            current_published.checkpoint_id
        ).query(
            "SELECT node_id FROM okg.graph_nodes "
            "WHERE node_id = 'visibility:candidate'"
        )
    finally:
        store.close()
    assert str(current_published.checkpoint_id) == baseline["generation_id"]
    assert visible_while_blocked == []

    published = Publisher(clean_runtime, deployment, catalog_identity=bind_test_catalog_writer(clean_runtime, deployment)).cycle()
    assert published["status"] == "published", published
    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        visible_after_publish = store.checkout_generation(
            published["generation_id"]
        ).query(
            "SELECT node_id FROM okg.graph_nodes "
            "WHERE node_id = 'visibility:candidate'"
        )
    finally:
        store.close()
    assert visible_after_publish == [{"node_id": "visibility:candidate"}]


def _apply_system_node(
    conn,
    store,
    *,
    deployment: str,
    catalog_version_id: int,
    node_id: str,
) -> None:
    apply_bounded_fact_batch(
        conn,
        store,
        [
            NodeFact(
                node_id,
                "document",
                {"text": node_id},
                {"node_id": node_id},
                {"revision": 1},
            )
        ],
        deployment_name=deployment,
        producer_id="system:visibility_test",
        producer_kind="system",
        intent="visibility_test",
        catalog_version_id=catalog_version_id,
        authority_scope={"node_id": node_id},
        source="system:visibility_test",
    )
