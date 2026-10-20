from __future__ import annotations

import json
import queue
import threading
import time

import psycopg
import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.cli.main import main
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.projection.remediation import (
    mark_entity_merge_published,
    plan_entity_merge,
    remediate_entity_merge,
)
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import seed_catalog
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres

DEPLOYMENT = "entity-merge-test"
SURVIVOR = "concept:signal-region"
RETIRED = "concept:signal-region-batch-7"
RETIRED_B = "concept:signal-region-batch-8"
OTHER = "paper:cooper"
BATCH_DEPLOYMENT = "entity-merge-batch-test"


def _seed_catalog(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions "
            "(status, applied_at, deployment_name) "
            "VALUES ('applied', now(), %s) RETURNING catalog_version_id",
            (DEPLOYMENT,),
        )
        return int(cur.fetchone()[0])


def _seed_graph(
    conn: psycopg.Connection,
    *,
    conflicting_target: bool = False,
    self_loop: bool = False,
) -> tuple[ChronosGraphStore, int, str]:
    from chronos_core.branching._common import clear_all_registry_caches
    from okg.substrate.graph_store_internal.common import clear_schema_ensured_cache

    clear_all_registry_caches()
    clear_schema_ensured_cache()
    cv = _seed_catalog(conn)
    store = ChronosGraphStore.from_connection(conn)
    # The shared test fixture drops Chronos metadata between tests; make this
    # integration fixture self-contained even when process-level ensure caches
    # were warm before that reset.
    store.context.ensure_metadata()
    store.ensure_schema()
    store.ensure_branch("default", from_branch="main")
    store.apply_facts(
        [
            NodeFact(
                SURVIVOR,
                "concept",
                {"name": "Signal Region", "curated": True},
                {"id": SURVIVOR},
                {"v": 1},
            ),
            NodeFact(
                RETIRED,
                "concept",
                {"name": "signal region", "extractor": "cooper-llm"},
                {"id": RETIRED},
                {"v": 7},
            ),
        ],
        branch="default",
        source="concept-source",
    )
    store.apply_facts(
        [
            NodeFact(
                OTHER,
                "paper",
                {"title": "Cooper fixture"},
                {"id": OTHER},
                {"v": 1},
            )
        ],
        branch="default",
        source="paper-source",
    )
    edge_facts = [
        EdgeFact(
            OTHER,
            RETIRED,
            "mentions",
            attrs={"grounding": "section-2"},
            provenance="extracted_model",
            confidence=0.91,
            source_record_id={"paper": OTHER, "span": 12},
            source_revision={"run": 7},
        ),
        EdgeFact(
            RETIRED,
            OTHER,
            "grounded_in",
            attrs={"reason": "fixture"},
            provenance="derived_deterministic",
            confidence=1.0,
            source_record_id={"concept": RETIRED},
            source_revision={"run": 7},
        ),
    ]
    if self_loop:
        edge_facts.append(
            EdgeFact(
                RETIRED,
                SURVIVOR,
                "related_to",
                source_record_id={"id": "self-loop-after-merge"},
                source_revision={"v": 1},
            )
        )
    store.apply_facts(
        edge_facts,
        branch="default",
        source="extracted-source",
    )
    if conflicting_target:
        store.apply_facts(
            [
                EdgeFact(
                    OTHER,
                    SURVIVOR,
                    "mentions",
                    attrs={"grounding": "different"},
                    provenance="explicit",
                    confidence=1.0,
                    source_record_id={"paper": OTHER, "span": 99},
                    source_revision={"run": 1},
                )
            ],
            branch="default",
            source="curated-source",
        )
    checkpoint = store.checkpoint_generation(
        branch="default",
        deployment_name=DEPLOYMENT,
        catalog_version_id=cv,
        status="published",
        metadata={"fixture": "entity-merge-before"},
    )
    return store, cv, str(checkpoint)


def _graph_rows(conn: psycopg.Connection) -> tuple[list[dict], list[dict]]:
    graph = ChronosGraphStore.from_connection(conn, ensure_schema=False).checkout(
        "default"
    )
    nodes = [
        dict(row)
        for row in graph.query(
            "SELECT node_id, subtype, attrs, source FROM okg.graph_nodes ORDER BY node_id"
        )
    ]
    edges = [
        dict(row)
        for row in graph.query(
            "SELECT src, dst, edge_type, attrs, provenance, confidence, source, "
            "source_record_id, source_revision FROM okg.graph_edges "
            "ORDER BY src, edge_type, dst"
        )
    ]
    return nodes, edges


def _seed_batch_graph(conn: psycopg.Connection) -> ChronosGraphStore:
    from chronos_core.branching._common import clear_all_registry_caches
    from okg.substrate.graph_store_internal.common import clear_schema_ensured_cache

    clear_all_registry_caches()
    clear_schema_ensured_cache()
    seed_catalog(
        conn,
        subtypes=["concept", "paper"],
        narrowings=[
            ("paper", "mentions", "concept"),
            ("concept", "grounded_in", "paper"),
        ],
    )
    store = ChronosGraphStore.from_connection(conn)
    store.context.ensure_metadata()
    store.ensure_schema()
    store.ensure_branch("default", from_branch="main")
    node_specs = (
        (SURVIVOR, "Signal Region", "base"),
        (RETIRED, "signal region", "retiree-a"),
        (RETIRED_B, "signal  region", "retiree-b"),
    )
    store.apply_facts(
        [
            NodeFact(
                node_id,
                "concept",
                {"name": name, "evidence": [evidence]},
                {"node_id": node_id},
                {"fixture": "entity-merge-batch"},
            )
            for node_id, name, evidence in node_specs
        ],
        branch="default",
        source="batch-concept-source",
    )
    store.apply_facts(
        [
            NodeFact(
                OTHER,
                "paper",
                {"title": "Cooper batch fixture"},
                {"paper": OTHER},
                {"fixture": "entity-merge-batch"},
            )
        ],
        branch="default",
        source="batch-paper-source",
    )

    edges: list[EdgeFact] = []
    for node_id, _name, _evidence in node_specs:
        base_record = {
            "chunk_node_id": OTHER,
            "enricher": "entity_merge_batch_fixture",
            "node_id": node_id,
        }
        edges.extend(
            [
                EdgeFact(
                    OTHER,
                    node_id,
                    "mentions",
                    attrs={"grounding": "section-2"},
                    provenance="extracted_model",
                    confidence=0.91,
                    source_record_id={**base_record, "assertion": "mentions"},
                    source_revision={"fixture": "entity-merge-batch"},
                ),
                EdgeFact(
                    node_id,
                    OTHER,
                    "grounded_in",
                    attrs={"reason": "fixture"},
                    provenance="derived_deterministic",
                    confidence=1.0,
                    source_record_id={**base_record, "assertion": "grounded_in"},
                    source_revision={"fixture": "entity-merge-batch"},
                ),
            ]
        )
    store.apply_facts(
        edges,
        branch="default",
        source="batch-edge-source",
    )
    return store


def _batch_row_counts(conn: psycopg.Connection) -> dict[str, int]:
    graph = ChronosGraphStore.from_connection(conn, ensure_schema=False).checkout(
        "default"
    )
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.entity_merge_receipts WHERE deployment_name = %s",
            (BATCH_DEPLOYMENT,),
        )
        receipts = int(cur.fetchone()[0])
    return {
        "nodes": int(graph.query("SELECT count(*) AS n FROM okg.graph_nodes")[0]["n"]),
        "edges": int(graph.query("SELECT count(*) AS n FROM okg.graph_edges")[0]["n"]),
        "receipts": receipts,
    }


def _merge_side_effect_counts(conn: psycopg.Connection) -> tuple[int, int]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.entity_merge_receipts WHERE deployment_name = %s",
            (DEPLOYMENT,),
        )
        receipts = int(cur.fetchone()[0])
        cur.execute(
            "SELECT count(*) FROM okg.admission_envelopes "
            "WHERE deployment_name = %s AND producer_id = 'system:entity_merge'",
            (DEPLOYMENT,),
        )
        admissions = int(cur.fetchone()[0])
    return receipts, admissions


def test_multi_merge_batch_replans_evolved_branch_and_publishes_every_merge(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_batch_graph(conn)

    before_publish = Publisher(
        clean_runtime,
        BATCH_DEPLOYMENT,
        enrichers=[],
        catalog_identity=bind_test_catalog_writer(clean_runtime, BATCH_DEPLOYMENT),
    ).cycle()
    assert before_publish["status"] == "published", before_publish

    plans: list[str] = []
    merge_ids: list[str] = []
    write_keys_by_plan: dict[str, set[str]] = {}
    row_counts: list[dict[str, int]] = []
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        row_counts.append(_batch_row_counts(conn))
        for retired in (RETIRED, RETIRED_B):
            plan = plan_entity_merge(
                conn,
                survivor_node_id=SURVIVOR,
                retired_node_id=retired,
                deployment_name=BATCH_DEPLOYMENT,
                attribute_dispositions={
                    "evidence": "union",
                    "name": "keep_survivor",
                },
            )
            assert plan.pinned_checkpoint == str(before_publish["generation_id"])
            assert plan.blocking_conflicts == ()
            result = remediate_entity_merge(
                conn,
                survivor_node_id=SURVIVOR,
                retired_node_id=retired,
                deployment_name=BATCH_DEPLOYMENT,
                expected_plan_digest=plan.plan_digest,
                attribute_dispositions={
                    "evidence": "union",
                    "name": "keep_survivor",
                },
                actor="pytest-batch",
                apply=True,
            )
            plans.append(plan.plan_digest)
            merge_ids.append(result.details["receipt"]["merge_id"])
            row_counts.append(_batch_row_counts(conn))
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT idempotency_key FROM okg.admission_envelopes "
                    "WHERE deployment_name = %s AND producer_id = 'system:entity_merge' "
                    "AND authority->>'plan_digest' = %s",
                    (BATCH_DEPLOYMENT, plan.plan_digest),
                )
                write_keys_by_plan.setdefault(plan.plan_digest, set()).update(
                    str(row[0]) for row in cur.fetchall()
                )

        assert row_counts == [
            {"nodes": 4, "edges": 6, "receipts": 0},
            {"nodes": 3, "edges": 4, "receipts": 1},
            {"nodes": 2, "edges": 2, "receipts": 2},
        ]
        assert [
            {
                key: row_counts[index + 1][key] - row_counts[index][key]
                for key in ("nodes", "edges", "receipts")
            }
            for index in range(2)
        ] == [
            {"nodes": -1, "edges": -2, "receipts": 1},
            {"nodes": -1, "edges": -2, "receipts": 1},
        ]
        assert len(set(plans)) == 2
        assert len(set(merge_ids)) == 2
        assert {len(keys) for keys in write_keys_by_plan.values()} == {3}
        assert len(set.union(*write_keys_by_plan.values())) == 6
        plan_write_sets = list(write_keys_by_plan.values())
        assert plan_write_sets[0].isdisjoint(plan_write_sets[1])

        graph = ChronosGraphStore.from_connection(conn, ensure_schema=False).checkout(
            "default"
        )
        live_nodes = graph.query(
            "SELECT node_id, attrs FROM okg.graph_nodes ORDER BY node_id"
        )
        survivor = next(row for row in live_nodes if row["node_id"] == SURVIVOR)
        assert survivor["attrs"]["name"] == "Signal Region"
        assert survivor["attrs"]["evidence"] == [
            "base",
            "retiree-a",
            "retiree-b",
        ]
        assert {row["node_id"] for row in live_nodes} == {SURVIVOR, OTHER}
        final_edges = graph.query(
            "SELECT src, dst, edge_type, attrs, provenance, confidence, source, "
            "source_record_id, source_revision FROM okg.graph_edges "
            "ORDER BY src, edge_type, dst"
        )
        assert len(final_edges) == 2
        assert final_edges[0]["src"] == SURVIVOR
        assert final_edges[0]["dst"] == OTHER
        assert final_edges[0]["edge_type"] == "grounded_in"
        assert final_edges[0]["attrs"]["reason"] == "fixture"
        assert final_edges[0]["attrs"]["nomos_source_lineage"]["status"] == "ok"
        assert [
            source["source_id"]
            for source in final_edges[0]["attrs"]["nomos_source_lineage"]["sources"]
        ] == ["batch-edge-source"]
        assert final_edges[0]["provenance"] == "derived_deterministic"
        assert final_edges[0]["confidence"] == pytest.approx(1.0)
        assert final_edges[0]["source"] == "batch-edge-source"
        assert final_edges[0]["source_record_id"] == {
            "assertion": "grounded_in",
            "chunk_node_id": OTHER,
            "enricher": "entity_merge_batch_fixture",
            "node_id": SURVIVOR,
        }
        assert final_edges[0]["source_revision"] == {"fixture": "entity-merge-batch"}
        assert final_edges[1]["src"] == OTHER
        assert final_edges[1]["dst"] == SURVIVOR
        assert final_edges[1]["edge_type"] == "mentions"
        assert final_edges[1]["attrs"]["grounding"] == "section-2"
        assert final_edges[1]["attrs"]["nomos_source_lineage"]["status"] == "ok"
        assert [
            source["source_id"]
            for source in final_edges[1]["attrs"]["nomos_source_lineage"]["sources"]
        ] == ["batch-edge-source"]
        assert final_edges[1]["provenance"] == "extracted_model"
        assert final_edges[1]["confidence"] == pytest.approx(0.91)
        assert final_edges[1]["source"] == "batch-edge-source"
        assert final_edges[1]["source_record_id"] == {
            "assertion": "mentions",
            "chunk_node_id": OTHER,
            "enricher": "entity_merge_batch_fixture",
            "node_id": SURVIVOR,
        }
        assert final_edges[1]["source_revision"] == {"fixture": "entity-merge-batch"}
        with conn.cursor() as cur:
            cur.execute(
                "SELECT retired_node_id, plan FROM okg.entity_merge_receipts "
                "WHERE deployment_name = %s ORDER BY retired_node_id",
                (BATCH_DEPLOYMENT,),
            )
            receipt_plans = cur.fetchall()
        assert {str(retired) for retired, _plan in receipt_plans} == {
            RETIRED,
            RETIRED_B,
        }
        for retired, raw_plan in receipt_plans:
            plan_payload = (
                json.loads(raw_plan) if isinstance(raw_plan, str) else raw_plan
            )
            for entry in plan_payload["deduplicated_edges"]:
                union = entry["provenance_union"]
                assert union["schema_version"] == "okg.entity_merge.provenance_union.v1"
                record_ids = union["source_record_ids"]
                assert any(record.get("node_id") == SURVIVOR for record in record_ids)
                assert any(
                    record.get("node_id") == str(retired) for record in record_ids
                )

    after_publish = Publisher(
        clean_runtime,
        BATCH_DEPLOYMENT,
        enrichers=[],
        catalog_identity=bind_test_catalog_writer(clean_runtime, BATCH_DEPLOYMENT),
    ).cycle(force_enrichers=True)
    assert after_publish["status"] == "published", after_publish
    assert after_publish["generation_id"] != before_publish["generation_id"]

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        for merge_id in merge_ids:
            mark_entity_merge_published(
                conn,
                merge_id=merge_id,
                repair_generation_id=str(after_publish["generation_id"]),
            )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, repair_generation_id, count(*) "
                "FROM okg.entity_merge_receipts "
                "WHERE deployment_name = %s "
                "GROUP BY status, repair_generation_id",
                (BATCH_DEPLOYMENT,),
            )
            assert cur.fetchall() == [
                ("published", str(after_publish["generation_id"]), 2)
            ]
        published = ChronosGraphStore.from_connection(
            conn, ensure_schema=False
        ).checkout_generation(str(after_publish["generation_id"]))
        assert published.query("SELECT count(*) AS n FROM okg.graph_nodes")[0]["n"] == 2
        assert published.query("SELECT count(*) AS n FROM okg.graph_edges")[0]["n"] == 2


def test_dry_run_then_apply_rehomes_edges_preserves_provenance_and_replays(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store, cv, before_generation = _seed_graph(conn)
        before_nodes, before_edges = _graph_rows(conn)
        dry = remediate_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            accept_survivor_attributes=True,
            apply=False,
        )
        plan = dry.details["plan"]
        assert dry.applied is False
        assert _graph_rows(conn) == (before_nodes, before_edges)
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM okg.entity_merge_receipts")
            assert int(cur.fetchone()[0]) == 0

        applied = remediate_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            expected_plan_digest=plan["plan_digest"],
            accept_survivor_attributes=True,
            actor="pytest",
            apply=True,
        )
        nodes, edges = _graph_rows(conn)
        assert RETIRED not in {row["node_id"] for row in nodes}
        assert SURVIVOR in {row["node_id"] for row in nodes}
        assert not any(RETIRED in {row["src"], row["dst"]} for row in edges)
        inbound = next(
            row
            for row in edges
            if row["src"] == OTHER
            and row["dst"] == SURVIVOR
            and row["edge_type"] == "mentions"
        )
        assert inbound["attrs"]["grounding"] == "section-2"
        assert inbound["provenance"] == "extracted_model"
        assert inbound["confidence"] == pytest.approx(0.91)
        assert inbound["source"] == "extracted-source"
        assert inbound["source_record_id"] == {"paper": OTHER, "span": 12}
        assert inbound["source_revision"] == {"run": 7}

        replay = remediate_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            expected_plan_digest=plan["plan_digest"],
            apply=True,
        )
        assert replay.affected_count == 0
        assert replay.details["replayed"] is True
        assert _graph_rows(conn) == (nodes, edges)
        with pytest.raises(ValueError, match="different survivor"):
            remediate_entity_merge(
                conn,
                survivor_node_id="concept:different-survivor",
                retired_node_id=RETIRED,
                deployment_name=DEPLOYMENT,
                expected_plan_digest=plan["plan_digest"],
                apply=True,
            )
        with pytest.raises(ValueError, match="replay --apply requires"):
            remediate_entity_merge(
                conn,
                survivor_node_id=SURVIVOR,
                retired_node_id=RETIRED,
                deployment_name=DEPLOYMENT,
                apply=True,
            )

        repair_generation = store.checkpoint_generation(
            branch="default",
            deployment_name=DEPLOYMENT,
            catalog_version_id=cv,
            status="published",
            metadata={"fixture": "entity-merge-after"},
        )
        merge_id = applied.details["receipt"]["merge_id"]
        with pytest.raises(ValueError, match="predates the merge"):
            mark_entity_merge_published(
                conn,
                merge_id=merge_id,
                repair_generation_id=before_generation,
            )
        wrong_deployment_generation = store.checkpoint_generation(
            branch="default",
            deployment_name="different-deployment",
            catalog_version_id=cv,
            status="published",
        )
        with pytest.raises(ValueError, match="another deployment or branch"):
            mark_entity_merge_published(
                conn,
                merge_id=merge_id,
                repair_generation_id=str(wrong_deployment_generation),
            )
        mark_entity_merge_published(
            conn,
            merge_id=merge_id,
            repair_generation_id=str(repair_generation),
        )
        mark_entity_merge_published(
            conn,
            merge_id=merge_id,
            repair_generation_id=str(repair_generation),
        )
        later_generation = store.checkpoint_generation(
            branch="default",
            deployment_name=DEPLOYMENT,
            catalog_version_id=cv,
            status="published",
        )
        with pytest.raises(ValueError, match="already generation-bound"):
            mark_entity_merge_published(
                conn,
                merge_id=merge_id,
                repair_generation_id=str(later_generation),
            )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT survivor_node_id, retired_snapshot, status, "
                "repair_generation_id FROM okg.entity_merge_receipts "
                "WHERE retired_node_id = %s",
                (RETIRED,),
            )
            survivor, retired_snapshot, status, generation = cur.fetchone()
        assert survivor == SURVIVOR
        assert retired_snapshot["attrs"]["extractor"] == "cooper-llm"
        assert status == "published"
        assert generation == str(repair_generation)


def test_edge_conflict_and_self_loop_refuse_without_writes(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_graph(conn, conflicting_target=True, self_loop=True)
        before = _graph_rows(conn)
        plan = plan_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            accept_survivor_attributes=True,
        )
        kinds = {item["kind"] for item in plan.blocking_conflicts}
        assert kinds == {"edge_identity_conflict", "self_loop_after_rehome"}
        with pytest.raises(SubstrateError, match="blocking edge conflicts"):
            remediate_entity_merge(
                conn,
                survivor_node_id=SURVIVOR,
                retired_node_id=RETIRED,
                deployment_name=DEPLOYMENT,
                expected_plan_digest=plan.plan_digest,
                accept_survivor_attributes=True,
                apply=True,
            )
        assert _graph_rows(conn) == before


def test_footprint_drift_refuses_without_touching_merge_scope(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store, _cv, _before = _seed_graph(conn)
        plan = plan_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            accept_survivor_attributes=True,
        )
        store.apply_facts(
            [
                EdgeFact(
                    SURVIVOR,
                    OTHER,
                    "described_by",
                    attrs={"drift": True},
                    source_record_id={"id": "drift"},
                    source_revision={"v": 1},
                )
            ],
            branch="default",
            source="drift-source",
        )
        before_apply = _graph_rows(conn)
        before_side_effects = _merge_side_effect_counts(conn)
        with pytest.raises(
            ValueError,
            match="unpublished or concurrent writes.*replan",
        ):
            remediate_entity_merge(
                conn,
                survivor_node_id=SURVIVOR,
                retired_node_id=RETIRED,
                deployment_name=DEPLOYMENT,
                expected_plan_digest=plan.plan_digest,
                accept_survivor_attributes=True,
                apply=True,
            )
        assert _graph_rows(conn) == before_apply
        assert _merge_side_effect_counts(conn) == before_side_effects


def test_waiting_merge_refreshes_checkout_after_external_writer(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as setup_conn:
        _seed_graph(setup_conn)
        plan = plan_entity_merge(
            setup_conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            accept_survivor_attributes=True,
        )
    with psycopg.connect(clean_runtime, autocommit=True) as snapshot_conn:
        before_merge_side_effects = _merge_side_effect_counts(snapshot_conn)

    outcome: queue.Queue[Exception | object] = queue.Queue()
    merge_started = threading.Event()

    def run_waiting_merge() -> None:
        try:
            with psycopg.connect(clean_runtime, autocommit=True) as merge_conn:
                merge_started.set()
                result = remediate_entity_merge(
                    merge_conn,
                    survivor_node_id=SURVIVOR,
                    retired_node_id=RETIRED,
                    deployment_name=DEPLOYMENT,
                    expected_plan_digest=plan.plan_digest,
                    accept_survivor_attributes=True,
                    apply=True,
                )
                outcome.put(result)
        except Exception as exc:
            outcome.put(exc)

    with psycopg.connect(clean_runtime, autocommit=False) as writer_conn:
        writer_store = ChronosGraphStore.from_connection(
            writer_conn, ensure_schema=False, autocommit=False
        )
        with writer_conn.transaction():
            writer_store._lock_branch_write("default")
            worker = threading.Thread(target=run_waiting_merge, daemon=True)
            worker.start()
            assert merge_started.wait(timeout=2)
            # Confirm the merge reached its advisory-lock wait before the
            # external writer commits graph state outside the merge footprint.
            for _attempt in range(200):
                with writer_conn.cursor() as cur:
                    cur.execute(
                        "SELECT count(*) FROM pg_stat_activity "
                        "WHERE datname = current_database() AND pid <> pg_backend_pid() "
                        "AND wait_event_type = 'Lock' AND wait_event = 'advisory'"
                    )
                    if int(cur.fetchone()[0]) > 0:
                        break
                time.sleep(0.01)
            else:
                raise AssertionError("merge did not reach the advisory-lock wait")
            writer_store.apply_facts(
                [
                    NodeFact(
                        "paper:concurrent",
                        "paper",
                        {"title": "Concurrent writer"},
                        {"id": "paper:concurrent"},
                        {"v": 1},
                    ),
                    EdgeFact(
                        OTHER,
                        "paper:concurrent",
                        "cites",
                        attrs={"writer": "external"},
                        source_record_id={"id": "concurrent-edge"},
                        source_revision={"v": 1},
                    ),
                ],
                branch="default",
                source="concurrent-writer",
            )
            writer_graph = writer_store.checkout("default")
            expected_after_writer = (
                [
                    dict(row)
                    for row in writer_graph.query(
                        "SELECT node_id, subtype, attrs, source "
                        "FROM okg.graph_nodes ORDER BY node_id"
                    )
                ],
                [
                    dict(row)
                    for row in writer_graph.query(
                        "SELECT src, dst, edge_type, attrs, provenance, confidence, "
                        "source, source_record_id, source_revision "
                        "FROM okg.graph_edges ORDER BY src, edge_type, dst"
                    )
                ],
            )

    worker.join(timeout=5)
    assert not worker.is_alive()
    error = outcome.get_nowait()
    assert isinstance(error, Exception), f"merge unexpectedly returned: {error!r}"
    assert "replan" in str(error).lower(), f"non-actionable merge refusal: {error!r}"
    with psycopg.connect(clean_runtime, autocommit=True) as check_conn:
        assert _graph_rows(check_conn) == expected_after_writer
        assert _merge_side_effect_counts(check_conn) == before_merge_side_effects


@pytest.mark.parametrize("drift_kind", ["catalog", "published_checkpoint"])
def test_catalog_and_published_checkpoint_drift_refuse(
    clean_runtime: str,
    drift_kind: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store, cv, _before = _seed_graph(conn)
        plan = plan_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            accept_survivor_attributes=True,
        )
        if drift_kind == "catalog":
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO okg.catalog_versions "
                    "(status, applied_at, deployment_name) "
                    "VALUES ('applied', now(), %s)",
                    (DEPLOYMENT,),
                )
            expected = "catalog version drifted"
        else:
            store.checkpoint_generation(
                branch="default",
                deployment_name=DEPLOYMENT,
                catalog_version_id=cv,
                status="published",
                metadata={"fixture": "unexpected-new-publish"},
            )
            expected = "plan digest changed"
        before_apply = _graph_rows(conn)
        error_type = SubstrateError if drift_kind == "catalog" else ValueError
        with pytest.raises(error_type, match=expected):
            remediate_entity_merge(
                conn,
                survivor_node_id=SURVIVOR,
                retired_node_id=RETIRED,
                deployment_name=DEPLOYMENT,
                expected_plan_digest=plan.plan_digest,
                accept_survivor_attributes=True,
                apply=True,
            )
        assert _graph_rows(conn) == before_apply


def test_unrelated_deployment_catalog_does_not_invalidate_plan(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_graph(conn)
        plan = plan_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            accept_survivor_attributes=True,
        )
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.catalog_versions (status, applied_at) "
                "VALUES ('applied', now())"
            )
        result = remediate_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            expected_plan_digest=plan.plan_digest,
            accept_survivor_attributes=True,
            apply=True,
        )
        assert result.applied is True
        assert result.affected_count == 1


def test_merge_rehomes_distinct_mint_claims_without_unique_collision(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_graph(conn)
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO okg.entity_identity_claims "
                "(deployment_name, branch_name, subtype, normalized_key, "
                "canonical_node_id, raw_name, resolution_method, "
                "policy_fingerprint, graph_token) "
                "VALUES (%s, 'default', 'concept', %s, %s, %s, 'mint', "
                "'fixture-policy', 'fixture-ref')",
                [
                    (DEPLOYMENT, "signal region", SURVIVOR, "Signal Region"),
                    (
                        DEPLOYMENT,
                        "signal region batch seven",
                        RETIRED,
                        "Signal Region Batch Seven",
                    ),
                ],
            )
        plan = plan_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            accept_survivor_attributes=True,
        )
        remediate_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            expected_plan_digest=plan.plan_digest,
            accept_survivor_attributes=True,
            apply=True,
        )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT normalized_key, canonical_node_id, resolution_method "
                "FROM okg.entity_identity_claims ORDER BY normalized_key"
            )
            claims = cur.fetchall()
    assert claims == [
        ("signal region", SURVIVOR, "mint"),
        ("signal region batch seven", SURVIVOR, "merged"),
    ]


def test_fault_after_graph_edits_rolls_back_graph_ledger_and_receipts(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_graph(conn)
        before = _graph_rows(conn)
        plan = plan_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            accept_survivor_attributes=True,
        )
        with pytest.raises(RuntimeError, match="injected entity-merge fault"):
            remediate_entity_merge(
                conn,
                survivor_node_id=SURVIVOR,
                retired_node_id=RETIRED,
                deployment_name=DEPLOYMENT,
                expected_plan_digest=plan.plan_digest,
                accept_survivor_attributes=True,
                apply=True,
                _fault_after_graph=True,
            )
        assert _graph_rows(conn) == before
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM okg.entity_merge_receipts")
            assert int(cur.fetchone()[0]) == 0
            cur.execute(
                "SELECT count(*) FROM okg.admission_envelopes "
                "WHERE producer_id = 'system:entity_merge'"
            )
            assert int(cur.fetchone()[0]) == 0


def test_plan_json_is_stable_and_contains_explicit_attribute_disposition(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_graph(conn)
        plan = plan_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            accept_survivor_attributes=True,
        )
        encoded = json.dumps(plan.as_dict(), sort_keys=True, default=str)
        assert plan.plan_digest in encoded
        assert plan.attribute_conflicts
        assert {item["disposition"] for item in plan.attribute_conflicts} == {
            "keep_survivor"
        }


def test_entity_merge_cli_dry_run_apply_and_retired_trace(
    clean_runtime: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_graph(conn)
    rc = main(
        [
            "remediate",
            "entity-merge",
            "--survivor",
            SURVIVOR,
            "--retire",
            RETIRED,
            "--deployment-name",
            DEPLOYMENT,
            "--accept-survivor-attrs",
            "--dsn",
            clean_runtime,
            "--json",
        ]
    )
    assert rc == 0
    dry = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    digest = dry["details"]["plan"]["plan_digest"]
    rc = main(
        [
            "remediate",
            "entity-merge",
            "--survivor",
            SURVIVOR,
            "--retire",
            RETIRED,
            "--deployment-name",
            DEPLOYMENT,
            "--accept-survivor-attrs",
            "--expected-plan-digest",
            digest,
            "--apply",
            "--dsn",
            clean_runtime,
            "--json",
        ]
    )
    assert rc == 0
    applied = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert applied["details"]["receipt"]["plan_digest"] == digest

    rc = main(
        [
            "--json",
            "trace",
            "node",
            RETIRED,
            "--dsn",
            clean_runtime,
        ]
    )
    assert rc == 0
    trace = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert trace["retired_by_merges"][0]["survivor_node_id"] == SURVIVOR


def test_merge_removes_a_shared_node_and_every_declaration_of_it(
    clean_runtime: str,
) -> None:
    """Issue #1901: the merged-away node is removed, not handed to another
    source that also declares it, and no declaration of it survives."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store, cv, _before = _seed_graph(conn)
        store.apply_facts(
            [
                NodeFact(
                    RETIRED,
                    "concept",
                    {"name": "signal region", "seen_in": "second"},
                    {"id": RETIRED, "repo": "second"},
                    {"v": 1},
                    ownership_mode="substitute",
                )
            ],
            branch="default",
            source="concept-source-b",
        )
        store.checkpoint_generation(
            branch="default",
            deployment_name=DEPLOYMENT,
            catalog_version_id=cv,
            status="published",
            metadata={"fixture": "entity-merge-shared-declarer"},
        )
        graph = store.checkout("default")
        assert [
            row["source"]
            for row in graph.query(
                "SELECT source FROM okg.graph_node_declarers "
                "WHERE node_id = :node_id",
                {"node_id": RETIRED},
            )
        ] == ["concept-source-b"]

        dry = remediate_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            accept_survivor_attributes=True,
            apply=False,
        )
        remediate_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            expected_plan_digest=dry.details["plan"]["plan_digest"],
            accept_survivor_attributes=True,
            actor="pytest",
            apply=True,
        )
        nodes, _edges = _graph_rows(conn)
        assert RETIRED not in {row["node_id"] for row in nodes}, (
            "the merged-away node was handed to another declarer"
        )
        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).checkout("default")
        assert graph.query(
            "SELECT source FROM okg.graph_node_declarers "
            "WHERE node_id = :node_id",
            {"node_id": RETIRED},
        ) == []
