"""Production-path regression proof for unresolved-route recovery (#685)."""
from __future__ import annotations

from uuid import uuid4
from concurrent.futures import ThreadPoolExecutor, TimeoutError

import psycopg
import pytest

from okg.substrate.catalog.aliases import add_alias
from okg.substrate.catalog.linker_config import LinkerRoute
from okg.substrate.graph_store import ChronosGraphStore, NodeFact
from okg.substrate.producer_authority import materialize_producer_profile
from okg.substrate.projection import RegexEntityExtractor
from okg.substrate.projection.retry import (
    LINK_RETRY_ALIAS_LOCK_NAMESPACE,
    LinkRetryKey,
    link_retry_alias_lock_stripe,
    queued_extractor_node_ids,
    ready_link_retry_node_ids,
    record_link_retry_misses,
)
from okg.substrate.publisher import Publisher
from okg.substrate.types import PhaseBResult
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def _seed_catalog_and_graph(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        catalog_version_id = int(cur.fetchone()[0])
        cur.executemany(
            "INSERT INTO okg.subtypes (name, metadata, catalog_version_id) "
            "VALUES (%s, '{}'::jsonb, %s)",
            [
                ("page", catalog_version_id),
                ("concept", catalog_version_id),
            ],
        )
        cur.executemany(
            "INSERT INTO okg.subtype_ancestors "
            "(catalog_version_id, subtype_name, ancestor_name, depth) "
            "VALUES (%s, %s, %s, 0)",
            [
                (catalog_version_id, "page", "page"),
                (catalog_version_id, "concept", "concept"),
            ],
        )
        cur.execute(
            "INSERT INTO okg.narrowings "
            "(parent_subtype, edge_type, child_subtype, catalog_version_id) "
            "VALUES ('page', 'mentions', 'concept', %s)",
            (catalog_version_id,),
        )

    store = ChronosGraphStore.from_connection(conn)
    store.apply_facts(
        [
            NodeFact("page:one", "page", {"text": "patient has dm"}, {}, {}),
            NodeFact("concept:diabetes", "concept", {"name": "diabetes"}, {}, {}),
        ],
        source="alias-recovery-fixture",
        run_id=str(uuid4()),
    )

    materialize_producer_profile(
        conn,
        deployment_name="test",
        producer_id="alias-recovery-fixture",
        producer_kind="source",
        trust_label="repo-local-trusted",
        admission_mode="fast_track",
        authority_scope={"source_family": "alias-recovery-fixture"},
        allowed_record_kinds=(),
        output_signature={},
        policy_clauses=(),
        output_scope_summary={"summary": "test input"},
        source_ref={"fixture": __name__},
        catalog_version_id=None,
    )
    materialize_producer_profile(
        conn,
        deployment_name="test",
        producer_id="_extractor:concept",
        producer_kind="extractor",
        trust_label="derived-deterministic-trusted",
        admission_mode="fast_track",
        authority_scope={"source_family": "_extractor:concept"},
        allowed_record_kinds=("edge",),
        output_signature={
            "edges": [{
                "src_subtype": "page",
                "edge_type": "mentions",
                "dst_subtype": "concept",
                "clause_id": "allow_concept_mentions",
            }],
        },
        policy_clauses=({
            "id": "allow_concept_mentions",
            "effect": "allow",
            "record_kind": "edge",
            "triple": ["page", "mentions", "concept"],
        },),
        output_scope_summary={"summary": "test extractor"},
        source_ref={"fixture": __name__},
        catalog_version_id=catalog_version_id,
    )


def _publisher(dsn: str) -> Publisher:
    return Publisher(
        dsn,
        "test",
        extractor=RegexEntityExtractor([("concept", r"\b(dm)\b")]),
        extractor_source_subtypes=["page"],
        routes=[LinkerRoute(
            entity_type="concept",
            src_subtype=None,
            edge_type="mentions",
            dst_template="{resolved}",
        )],
        catalog_identity=bind_test_catalog_writer(dsn, "test"),
    )


def _retry_rows(conn: psycopg.Connection) -> list[tuple]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT deployment_name, branch_name, node_id, entity_type, "
            "entity_value, ready_at IS NOT NULL FROM okg.link_retry ORDER BY 1,2,3"
        )
        return cur.fetchall()


def test_alias_only_change_recovers_in_production_publisher_and_acks_after_publish(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_and_graph(conn)

    publisher = _publisher(clean_runtime)
    unresolved = publisher.cycle()
    assert unresolved["status"] == "published"
    assert unresolved["extractor_edges_emitted"] == 0

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _retry_rows(conn) == [
            ("test", "default", "page:one", "concept", "dm", False),
        ]
        landed = add_alias(
            conn,
            alias="dm",
            canonical="concept:diabetes",
            scope="concept",
        )
        assert landed["link_cache_invalidated"] in {0, 1}
        assert _retry_rows(conn) == [
            ("test", "default", "page:one", "concept", "dm", True),
        ]

    import okg.substrate.publisher.publisher as publisher_module

    real_evaluate_gates = publisher_module.evaluate_gates
    monkeypatch.setattr(
        publisher_module,
        "evaluate_gates",
        lambda *args, **kwargs: PhaseBResult(
            source_local={},
            cross_source=[],
            excluded_sources={},
            blocked=True,
            blocked_reason={"reason": "forced_retry_ack_boundary_test"},
            total_duration_ms=0,
            total_budget_exceeded=False,
        ),
    )
    blocked = publisher.cycle()
    assert blocked["status"] == "blocked"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _retry_rows(conn) == [
            ("test", "default", "page:one", "concept", "dm", True),
        ]

    monkeypatch.setattr(publisher_module, "evaluate_gates", real_evaluate_gates)
    recovered = publisher.cycle()
    assert recovered["status"] == "published"
    assert recovered.get("reused_generation") is not True

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _retry_rows(conn) == []
        checkout = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).checkout_generation(recovered["generation_id"])
        edges = checkout.query(
            "SELECT src, edge_type, dst FROM okg.graph_edges "
            "WHERE src = 'page:one' AND edge_type = 'mentions'"
        )
        assert edges == [{
            "src": "page:one",
            "edge_type": "mentions",
            "dst": "concept:diabetes",
        }]


def test_zero_ready_retry_probe_is_index_backed_and_within_budget(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.link_retry "
                "(deployment_name, branch_name, node_id, entity_type, entity_value) "
                "SELECT 'test', 'default', 'node:' || n, 'concept', 'missing:' || n "
                "FROM generate_series(1, 100000) n"
            )
            cur.execute(
                "INSERT INTO okg.extractor_retry_queue "
                "(deployment_name, branch_name, node_id) "
                "SELECT 'other', 'default', 'queued:' || n "
                "FROM generate_series(1, 100000) n"
            )
            cur.execute("ANALYZE okg.link_retry")
            cur.execute("ANALYZE okg.extractor_retry_queue")
            cur.execute(
                "EXPLAIN (ANALYZE, FORMAT JSON) "
                "SELECT DISTINCT node_id FROM okg.link_retry "
                "WHERE deployment_name = 'test' AND branch_name = 'default' "
                "AND ready_at IS NOT NULL ORDER BY node_id"
            )
            plan = cur.fetchone()[0][0]
            cur.execute(
                "EXPLAIN (ANALYZE, FORMAT JSON) "
                "SELECT node_id FROM okg.extractor_retry_queue "
                "WHERE deployment_name = 'test' AND branch_name = 'default' "
                "ORDER BY node_id"
            )
            queue_plan = cur.fetchone()[0][0]

        assert ready_link_retry_node_ids(
            conn,
            deployment_name="test",
            branch_name="default",
        ) == []
        assert queued_extractor_node_ids(
            conn,
            deployment_name="test",
            branch_name="default",
        ) == []

    assert "link_retry_ready_idx" in str(plan["Plan"])
    assert "extractor_retry_queue_pkey" in str(queue_plan["Plan"])
    assert (
        float(plan["Execution Time"]) + float(queue_plan["Execution Time"])
    ) <= 5.0


def test_processed_node_clears_stale_ready_retry_after_text_changes(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_and_graph(conn)
    publisher = _publisher(clean_runtime)
    assert publisher.cycle()["status"] == "published"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        add_alias(
            conn, alias="dm", canonical="concept:diabetes", scope="concept",
        )
        assert ready_link_retry_node_ids(
            conn, deployment_name="test", branch_name="default",
        ) == ["page:one"]
        ChronosGraphStore.from_connection(conn, ensure_schema=False).apply_facts(
            [NodeFact(
                "page:one", "page", {"text": "patient recovered"}, {}, {"v": 2},
            )],
            source="alias-recovery-fixture",
        )

    assert publisher.cycle()["status"] == "published"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _retry_rows(conn) == []


def test_explicit_queue_acknowledges_only_nodes_actually_processed(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_and_graph(conn)
        ChronosGraphStore.from_connection(conn, ensure_schema=False).apply_facts(
            [
                NodeFact("page:textless", "page", {}, {}, {}),
                NodeFact("concept:wrong", "concept", {"text": "dm"}, {}, {}),
            ],
            source="alias-recovery-fixture",
        )
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO okg.extractor_retry_queue "
                "(deployment_name, branch_name, node_id) VALUES ('test','default',%s)",
                [
                    ("page:one",),
                    ("page:textless",),
                    ("concept:wrong",),
                    ("page:missing",),
                ],
            )

    assert _publisher(clean_runtime).cycle()["status"] == "published"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert queued_extractor_node_ids(
            conn, deployment_name="test", branch_name="default",
        ) == ["concept:wrong", "page:missing", "page:textless"]


def test_alias_landing_and_miss_recording_cannot_lose_wakeup(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as setup:
        _seed_catalog_and_graph(setup)

    key = LinkRetryKey("test", "default", "page:one", "concept", "dm")
    with (
        psycopg.connect(clean_runtime, autocommit=False) as landing,
        ThreadPoolExecutor(max_workers=1) as pool,
    ):
        # The lock an alias landing holds for "dm": the recorder must meet it
        # on the same stripe (fix-link-retry-alias-lock-footprint).
        landing.execute(
            "SELECT pg_advisory_xact_lock(%s, %s)",
            (LINK_RETRY_ALIAS_LOCK_NAMESPACE, link_retry_alias_lock_stripe("dm")),
        )

        def record_after_observed_miss() -> None:
            with psycopg.connect(clean_runtime, autocommit=True) as recorder:
                record_link_retry_misses(recorder, [key])

        future = pool.submit(record_after_observed_miss)
        with pytest.raises(TimeoutError):
            future.result(timeout=0.1)

        # Alias lands after resolution observed the miss but before the durable
        # miss record can acquire the per-alias serialization lock.
        add_alias(
            landing, alias="dm", canonical="concept:diabetes", scope="concept",
        )
        landing.commit()
        future.result(timeout=2)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _retry_rows(conn) == [
            ("test", "default", "page:one", "concept", "dm", True),
        ]
