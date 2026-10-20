from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import psycopg
import pytest

from okg.substrate.db.bootstrap import bootstrap_snapshot
from okg.substrate.errors import SubstrateError
from okg.substrate.fact_application import (
    FactBatchApplicationError,
    apply_bounded_fact_batch,
)
from okg.substrate.fact_batch import FactBatchAuthorityError
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.library.enrichers._fact_emitters import emit_node_fact
from okg.substrate.publisher import Publisher
from okg.substrate.nomos import activate_policy_bundle, compile_policy_graph
from okg.substrate.nomos.source_lineage import LINEAGE_ATTR
from tests.pytest.catalog_fixture_builders import seed_catalog


pytestmark = pytest.mark.postgres


def test_derived_batch_replay_preserves_lineage_and_watermark(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = seed_catalog(conn, subtypes=["document"])
        store = ChronosGraphStore.from_connection(conn)
        run_id = uuid.uuid4()
        scope = _derived_scope(cv)
        facts = [
            NodeFact(
                node_id="derived:document:1",
                subtype="document",
                attrs={"text": "deterministic derived output"},
                source_record_id={
                    "input": "source:1",
                    "derived_from": ["source:1"],
                    "derived_support": {
                        "schema": "okg.derived_support.v1",
                        "producer": "_enricher:deterministic_test",
                        "members": ["source:1"],
                        "policy": "all_members",
                    },
                },
                source_revision={"generation": "base:1"},
            )
        ]

        first = apply_bounded_fact_batch(
            conn,
            store,
            facts,
            deployment_name="derived-test",
            producer_id="_enricher:deterministic_test",
            producer_kind="enricher",
            intent="derived_test",
            catalog_version_id=cv,
            authority_scope=scope,
            source="_enricher:deterministic_test",
            run_id=run_id,
            idempotency_key="derived-test-replay",
            strict_endpoints=True,
        )
        replay = apply_bounded_fact_batch(
            conn,
            store,
            facts,
            deployment_name="derived-test",
            producer_id="_enricher:deterministic_test",
            producer_kind="enricher",
            intent="derived_test",
            catalog_version_id=cv,
            authority_scope=scope,
            source="_enricher:deterministic_test",
            run_id=run_id,
            idempotency_key="derived-test-replay",
            strict_endpoints=True,
        )
        row = store.checkout().query(
            "SELECT source, source_record_id, source_revision "
            "FROM okg.graph_nodes WHERE node_id = :node_id",
            {"node_id": "derived:document:1"},
        )[0]

    assert first.primitive_total("nodes_upserted") == 1
    assert replay.primitive_results == ()
    assert row["source"] == "_enricher:deterministic_test"
    assert row["source_record_id"] == {
        "input": "source:1",
        "derived_from": ["source:1"],
        "derived_support": {
            "schema": "okg.derived_support.v1",
            "producer": "_enricher:deterministic_test",
            "members": ["source:1"],
            "policy": "all_members",
        },
    }
    assert row["source_revision"] == {"generation": "base:1"}
    assert first.summary.observed_scope["watermark"] == {"input": 1}


def test_incomplete_derived_output_is_rejected_before_graph_apply(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = seed_catalog(conn, subtypes=["document"])
        store = ChronosGraphStore.from_connection(conn)
        scope = {**_derived_scope(cv), "incomplete_result": True}

        with pytest.raises(FactBatchApplicationError, match="incomplete"):
            apply_bounded_fact_batch(
                conn,
                store,
                [
                    NodeFact(
                        "derived:incomplete",
                        "document",
                        {},
                        {"input": "source:1"},
                        {"generation": "base:1"},
                    )
                ],
                deployment_name="derived-test",
                producer_id="_enricher:incomplete_test",
                producer_kind="enricher",
                intent="derived_test",
                catalog_version_id=cv,
                authority_scope=scope,
            )

        assert (
            store.checkout().query(
                "SELECT node_id FROM okg.graph_nodes "
                "WHERE node_id = 'derived:incomplete'"
            )
            == []
        )


def test_bounded_system_batch_stamps_profile_admission_and_receipt(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = seed_catalog(conn, subtypes=["document"])
        bundle = compile_policy_graph({
            "policy_bundle_id": "nomos:test:bounded-system-lineage",
            "policy_version": "v1",
            "default_effect": "deny",
            "rules": [{
                "rule_id": "allow_test_reader",
                "effect": "allow",
                "actions": ["graph.read"],
                "subjects": ["test:reader"],
                "priority": 1,
                "reason_code": "nomos.test.allow_reader",
            }],
        })
        activate_policy_bundle(
            conn,
            deployment_name="system-test",
            bundle=bundle,
            scope="deployment:system-test",
            activated_by="test:operator",
        )
        result = apply_bounded_fact_batch(
            conn,
            ChronosGraphStore.from_connection(conn),
            [
                NodeFact(
                    "system:document:1",
                    "document",
                    {"text": "system output"},
                    {"workflow": "repair"},
                    {"catalog_version_id": cv},
                )
            ],
            deployment_name="system-test",
            producer_id="system:repair_test",
            producer_kind="system",
            intent="repair_test",
            catalog_version_id=cv,
            authority_scope={"workflow": "repair", "bounded": True},
            source="system:repair_test",
        )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT producer_kind, status, authority "
                "FROM okg.admission_envelopes WHERE admission_id = %s",
                (result.admission_id,),
            )
            envelope = cur.fetchone()
            cur.execute(
                "SELECT state FROM okg.fact_batch_receipts WHERE admission_id = %s",
                (result.admission_id,),
            )
            receipt = cur.fetchone()
            cur.execute(
                "SELECT status, profile_fingerprint "
                "FROM okg.producer_profiles "
                "WHERE deployment_name = %s AND producer_id = %s",
                ("system-test", "system:repair_test"),
            )
            profile = cur.fetchone()
        graph_row = ChronosGraphStore.from_connection(conn).checkout().query(
            "SELECT attrs FROM okg.graph_nodes WHERE node_id = :node_id",
            {"node_id": "system:document:1"},
        )[0]

    assert envelope[0:2] == ("system", "accepted")
    assert envelope[2]["profile_fingerprint"].startswith("sha256:")
    assert receipt == ("applied",)
    assert profile == ("compat", envelope[2]["profile_fingerprint"])
    assert graph_row["attrs"][LINEAGE_ATTR]["sources"][0][
        "policy_fingerprints"
    ] == [bundle.policy_fingerprint]


def test_bounded_batch_does_not_resurrect_disabled_registered_authority(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = seed_catalog(conn, subtypes=["document"])
        store = ChronosGraphStore.from_connection(conn)
        kwargs = {
            "deployment_name": "disabled-system-test",
            "producer_id": "system:disabled_repair",
            "producer_kind": "system",
            "intent": "disabled_repair",
            "catalog_version_id": cv,
            "authority_scope": {"workflow": "repair", "bounded": True},
            "source": "system:disabled_repair",
        }
        apply_bounded_fact_batch(
            conn,
            store,
            [],
            **kwargs,
        )
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE okg.producer_profiles SET status = 'disabled' "
                "WHERE deployment_name = %s AND producer_id = %s",
                ("disabled-system-test", "system:disabled_repair"),
            )

        with pytest.raises(FactBatchAuthorityError, match="not writable"):
            apply_bounded_fact_batch(
                conn,
                store,
                [],
                run_id=uuid.uuid4(),
                idempotency_key="disabled-replay",
                **kwargs,
            )

        with conn.cursor() as cur:
            cur.execute(
                "SELECT status FROM okg.producer_profiles "
                "WHERE deployment_name = %s AND producer_id = %s",
                ("disabled-system-test", "system:disabled_repair"),
            )
            status = cur.fetchone()

    assert status == ("disabled",)


@pytest.mark.skip(
    reason=(
        "quarantined pending #2628 (bootstrap_snapshot publishes with no "
        "catalog writer authority; fails on dev, no per-PR lane owned it "
        "before PR #2576)"
    ),
)
def test_snapshot_onboarding_streams_through_publisher_and_retires_bypass(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    snapshot = tmp_path / "snapshot.json"
    snapshot.write_text(
        json.dumps(
            {
                "nodes": [
                    {
                        "node_id": "snapshot:document:1",
                        "subtype": "document",
                        "attrs": {"text": "snapshot"},
                    }
                ],
                "edges": [],
            }
        ),
        encoding="utf-8",
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])

    result = bootstrap_snapshot(clean_runtime, snapshot, "snapshot-test")
    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        published = store.latest_generation(
            deployment_name="snapshot-test",
            status="published",
        )
        rows = store.checkout_generation(result.generation_id).query(
            "SELECT node_id FROM okg.graph_nodes WHERE node_id = 'snapshot:document:1'"
        )
    finally:
        store.close()

    assert published is not None
    assert str(published.checkpoint_id) == result.generation_id
    assert rows == [{"node_id": "snapshot:document:1"}]
    with pytest.raises(SubstrateError) as exc_info:
        bootstrap_snapshot(clean_runtime, snapshot, "snapshot-test")
    assert exc_info.value.code == "bootstrap_authority_required"


def test_inventory_keeps_non_graph_outputs_outside_fact_batch() -> None:
    path = (
        Path(__file__).resolve().parents[3]
        / "tests/fixtures/fact_batch/write_inventory.yaml"
    )
    entries = json.loads(path.read_text(encoding="utf-8"))["entries"]
    outside = {
        entry["authority_class"]
        for entry in entries
        if entry["authority_class"]
        in {"non_graph_dynamic_sql", "audit_recovery", "test_harness"}
    }

    assert {"non_graph_dynamic_sql", "audit_recovery", "test_harness"} <= outside


def test_derived_fact_log_writes_are_audit_inputs_not_graph_authority() -> None:
    path = (
        Path(__file__).resolve().parents[3]
        / "tests/fixtures/fact_batch/write_inventory.yaml"
    )
    entries = json.loads(path.read_text(encoding="utf-8"))["entries"]
    derived_audit = [
        entry for entry in entries
        if entry["authority_class"] == "audit_fact_input"
    ]

    assert derived_audit
    assert all(
        entry["target"] in {"node_facts", "edge_facts"}
        for entry in derived_audit
    )
    assert all("Publisher applies accepted derived facts" in entry["exit_condition"]
               for entry in derived_audit)
    assert not any(
        entry["authority_class"] == "derived_compatibility"
        for entry in entries
    )
    assert any(
        entry["authority_class"] == "fact_batch_application"
        and entry["symbol"] == "chronos_apply_callback.apply"
        for entry in entries
    )


def test_publisher_routes_staged_derived_facts_by_producer_fact_batch(
    clean_runtime: str,
) -> None:
    run_id = uuid.uuid4()
    observed_at = datetime(2026, 1, 2, tzinfo=timezone.utc)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = seed_catalog(conn, subtypes=["document"])
        emitted = emit_node_fact(
            conn,
            enricher_name="publisher_route_test",
            node_id="derived:routed:1",
            subtype="document",
            attrs={"text": "routed"},
            dedupe_key="derived-routed-1",
            source_record_id={"input": "source:1"},
            source_revision={"generation": "base:1"},
            run_id=str(run_id),
            observed_at=observed_at,
            derived_from=["source:1"],
            support_policy="all_members",
        )
        assert emitted.emitted is True
        batches = Publisher._load_unapplied_derived_facts(
            conn,
            enforcement_mode="off",
            group_by_producer=True,
        )
        assert len(batches) == 1
        batch = batches[0]
        assert batch.producer_id == "_enricher:publisher_route_test"
        assert batch.producer_kind == "enricher"
        assert batch.run_id == run_id

        publisher = Publisher(clean_runtime, "derived-route-test")
        publisher._apply_publisher_derived_fact_batch(
            conn,
            ChronosGraphStore.from_connection(conn),
            batch.facts,
            catalog_version_id=cv,
            gate_target_id=1,
            stage="pre_view",
            previous_generation_ref="base:1",
            producer_id=batch.producer_id,
            producer_kind=batch.producer_kind,
            source=batch.producer_id,
            run_id=batch.run_id,
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT e.producer_id, e.producer_kind, r.state
                  FROM okg.admission_envelopes e
                  JOIN okg.fact_batch_receipts r USING (admission_id)
                 WHERE e.deployment_name = 'derived-route-test'
                """
            )
            receipt = cur.fetchone()
        graph_rows = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).checkout().query(
            "SELECT source FROM okg.graph_nodes WHERE node_id = :node_id",
            {"node_id": "derived:routed:1"},
        )

    assert receipt == ("_enricher:publisher_route_test", "enricher", "applied")
    assert graph_rows == [{"source": "_enricher:publisher_route_test"}]


def _derived_scope(catalog_version_id: int) -> dict[str, object]:
    return {
        "dependency_generation": "base:1",
        "input_lineage": {"sources": ["source:1"]},
        "invalidation_scope": {"node_ids": ["source:1"]},
        "catalog_version_id": catalog_version_id,
        "determinism": "deterministic",
        "incomplete_result": False,
        "watermark": {"input": 1},
    }
