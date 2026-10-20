from __future__ import annotations

import json
from collections import Counter
from collections.abc import Iterator

import psycopg
import pytest

from okg.viz import reads_system


pytestmark = pytest.mark.postgres

_DEPLOYMENT = "test"
_GENERATION_ID = 42
_SOURCE = "fixture.catalog"
_LIVE_SRC_SUBTYPE = "publication"
_LIVE_DST_SUBTYPE = "organization"
_LIVE_EDGE_TYPE = "owned_by"


def _select_live_edge_shape(
    conn: psycopg.Connection,
) -> tuple[str, str, str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH active_cv AS (
              SELECT max(catalog_version_id) AS catalog_version_id
                FROM okg.catalog_versions
            )
            SELECT n.parent_subtype, n.edge_type, n.child_subtype
              FROM okg.narrowings n
              JOIN active_cv cv
                ON cv.catalog_version_id = n.catalog_version_id
              JOIN okg.subtype_ancestors sa_src
                ON sa_src.catalog_version_id = n.catalog_version_id
               AND sa_src.subtype_name = n.parent_subtype
               AND sa_src.ancestor_name = n.parent_subtype
              JOIN okg.subtype_ancestors sa_dst
                ON sa_dst.catalog_version_id = n.catalog_version_id
               AND sa_dst.subtype_name = n.child_subtype
               AND sa_dst.ancestor_name = n.child_subtype
             ORDER BY (n.parent_subtype = n.child_subtype),
                      n.parent_subtype,
                      n.edge_type,
                      n.child_subtype
             LIMIT 1
            """
        )
        row = cur.fetchone()
    if row is None:
        return ("publication", "owned_by", "organization")
    return (str(row[0]), str(row[1]), str(row[2]))


def _seed_system_snapshot_fixture(conn: psycopg.Connection) -> None:
    global _LIVE_SRC_SUBTYPE, _LIVE_DST_SUBTYPE, _LIVE_EDGE_TYPE
    _LIVE_SRC_SUBTYPE, _LIVE_EDGE_TYPE, _LIVE_DST_SUBTYPE = (
        _select_live_edge_shape(conn)
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM okg.enricher_progress
             WHERE deployment_name = %(deployment)s
               AND enricher_name = 'fixture-enricher'
            """,
            {"deployment": _DEPLOYMENT},
        )
        cur.execute(
            """
            DELETE FROM okg.llm_calls
             WHERE deployment_name = %(deployment)s
               AND caller = 'fixture:reader'
               AND model = 'stub-model'
            """,
            {"deployment": _DEPLOYMENT},
        )
        cur.execute(
            """
            INSERT INTO okg.graph_generations (
              generation_id, deployment_name, status, catalog_version_id,
              published_at, correlation_id
            ) VALUES (
              %(generation_id)s, %(deployment)s, 'published', 1, now(),
              %(correlation_id)s
            )
            """,
            {
                "generation_id": _GENERATION_ID,
                "deployment": _DEPLOYMENT,
                "correlation_id": f"{_DEPLOYMENT}:{_GENERATION_ID}",
            },
        )
        cur.execute(
            """
            INSERT INTO okg.graph_generations (
              generation_id, deployment_name, status, catalog_version_id,
              correlation_id
            ) VALUES (
              %(generation_id)s + 1, %(deployment)s, 'pending', 1,
              %(pending_correlation_id)s
            )
            """,
            {
                "generation_id": _GENERATION_ID,
                "deployment": _DEPLOYMENT,
                "pending_correlation_id": f"{_DEPLOYMENT}:pending",
            },
        )
        cur.execute(
            """
            INSERT INTO okg.graph_generations (
              generation_id, deployment_name, status, catalog_version_id,
              blocked_reason, correlation_id
            ) VALUES (
              %(generation_id)s + 2, %(deployment)s, 'blocked', 1,
              '{"invariant":"fixture_gate"}'::jsonb,
              %(blocked_correlation_id)s
            )
            """,
            {
                "generation_id": _GENERATION_ID,
                "deployment": _DEPLOYMENT,
                "blocked_correlation_id": f"{_DEPLOYMENT}:blocked",
            },
        )
        cur.execute(
            """
            INSERT INTO okg.source_watermarks (
              source, projection_cursor, last_reconcile_at
            ) VALUES (%(source)s, 7, now())
            """,
            {"source": _SOURCE},
        )
        cur.execute(
            """
            INSERT INTO okg.projection_pending
              (generation_id, source, new_cursor, batch_index)
            VALUES (%(generation_id)s, %(source)s, 8, 0)
            """,
            {"generation_id": _GENERATION_ID, "source": _SOURCE},
        )
        cur.execute(
            """
            INSERT INTO okg.enricher_progress (
              deployment_name, enricher_name, fact_watermark, edge_watermark,
              generation_id, last_status
            ) VALUES (
              %(deployment)s, 'fixture-enricher', 7, 1,
              %(generation_id)s, 'success'
            )
            """,
            {"deployment": _DEPLOYMENT, "generation_id": _GENERATION_ID},
        )
        cur.execute(
            """
            INSERT INTO okg.llm_calls (
              deployment_name, caller, model, prompt_tokens, completion_tokens,
              total_tokens, cost_usd, success, error_kind
            ) VALUES
              (%(deployment)s, 'fixture:reader', 'stub-model',
               10, 5, 15, 0.00010000, TRUE, NULL),
              (%(deployment)s, 'fixture:reader', 'stub-model',
               NULL, NULL, NULL, NULL, FALSE, 'fixture_error')
            """,
            {"deployment": _DEPLOYMENT},
        )

    # Chronos cutover: `fetch_live_graph` reads the Chronos graph at the
    # published generation's checkout, not okg.nodes_live/edges_live.
    # Mirror the live-table fixture into the Chronos graph and checkpoint
    # the published generation so checkout_generation(42) resolves.
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import EdgeFact, NodeFact
    from okg.substrate.publisher.console_count_rollup import (
        maintain_console_count_rollup,
    )

    store = ChronosGraphStore.from_connection(conn)
    try:
        store.apply_facts(
            [
                NodeFact(
                    "fixture:src", _LIVE_SRC_SUBTYPE,
                    {"title": "Alpha"}, {"id": "pub:alpha"}, {"rev": 1},
                ),
                NodeFact(
                    "fixture:dst", _LIVE_DST_SUBTYPE,
                    {"name": "Operations"}, {"id": "org:ops"}, {"rev": 1},
                ),
                EdgeFact(
                    "fixture:src", "fixture:dst", _LIVE_EDGE_TYPE,
                    source_record_id={"id": "edge:1001"},
                    source_revision={"rev": 1},
                ),
            ],
            source=_SOURCE,
            strict_endpoints=True,
        )
        # A publish maintains the count rollup before it cuts the
        # checkpoint; the source panel reads per-source counts from it.
        maintain_console_count_rollup(
            conn,
            generation_id=_GENERATION_ID,
            changed_node_ids=[],
            changed_edge_ids=[],
            prior_generation_checkpoint=None,
            force_full=True,
        )
        store.checkpoint_generation(
            _GENERATION_ID,
            deployment_name=_DEPLOYMENT,
            catalog_version_id=1,
        )
    finally:
        store.close()


@pytest.fixture
def seeded_conn(clean_runtime: str) -> Iterator[psycopg.Connection]:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_system_snapshot_fixture(conn)
        yield conn


def test_database_panel_reads_return_fixture_signals(
    seeded_conn: psycopg.Connection,
) -> None:
    generation = reads_system.fetch_generation(seeded_conn, _DEPLOYMENT)
    assert generation["published"]["generation_id"] == _GENERATION_ID
    assert generation["published"]["catalog_version_id"] == 1
    assert generation["pending"]["generation_id"] == _GENERATION_ID + 1
    assert generation["blocked"]["generation_id"] == _GENERATION_ID + 2
    assert generation["blocked"]["reason"]["invariant"] == "fixture_gate"

    catalog = reads_system.fetch_catalog(seeded_conn)
    assert catalog["applied"]["catalog_version_id"] >= 1
    assert isinstance(catalog["subtypes"], int)
    assert isinstance(catalog["narrowings"], int)

    live = reads_system.fetch_live_graph(seeded_conn, _GENERATION_ID)
    assert live["node_count"] == 2
    assert live["edge_count"] == 1
    expected_subtypes = Counter([_LIVE_SRC_SUBTYPE, _LIVE_DST_SUBTYPE])
    assert {
        row["subtype"]: row["count"] for row in live["top_subtypes"]
    } == dict(expected_subtypes)

    sources = reads_system.fetch_sources(seeded_conn, _GENERATION_ID)
    assert sources["counts_error"] is None
    assert sources["rows"] == [{
        "source": _SOURCE,
        "last_reconcile_at": sources["rows"][0]["last_reconcile_at"],
        "projection_cursor": 7,
        "n_node_facts": 2,
        "n_edge_facts": 1,
        "last_observed_at": None,
    }]
    assert sources["rows"][0]["last_reconcile_at"]

    storage = reads_system.fetch_storage(seeded_conn)
    assert storage["db_size_bytes"] > 0
    assert {"okg.graph_nodes", "okg.graph_edges"} <= {
        row["table"] for row in storage["tables"]
    }
    assert all(row["size_bytes"] >= 0 for row in storage["tables"])


def test_inflight_panel_reads_return_fixture_signals(
    seeded_conn: psycopg.Connection,
) -> None:
    queue = reads_system.fetch_queue(seeded_conn)
    assert queue["by_status"]["queued"] == 0
    assert queue["by_status"]["leased"] == 0
    assert queue["by_status"]["dead_letter"] == 0
    assert queue["source"] == "okg.v_runtime_queue_health"
    assert "runtime_queue_health" in queue
    assert queue["oldest_queued_age_s"] is None

    projection = reads_system.fetch_projection_backlog(seeded_conn)
    assert projection["pending"] == 1
    assert projection["oldest_age_s"] is not None

    enrichers = reads_system.fetch_enrichers(seeded_conn, _DEPLOYMENT)
    assert enrichers == [{
        "enricher_name": "fixture-enricher",
        "fact_watermark": 7,
        "edge_watermark": 1,
        "generation_id": _GENERATION_ID,
        "last_run_at": enrichers[0]["last_run_at"],
        "last_status": "success",
    }]
    assert enrichers[0]["last_run_at"]


def test_operations_panel_reads_return_fixture_signals(
    seeded_conn: psycopg.Connection,
) -> None:
    doctor = reads_system.fetch_doctor(seeded_conn)
    assert isinstance(doctor["ok"], bool)
    assert doctor["extensions"]
    assert {row["name"] for row in doctor["roles"]} == {
        "app_rw", "app_ro", "catalog_admin",
    }

    spend = reads_system.fetch_llm_spend(seeded_conn, _DEPLOYMENT)
    assert spend["calls"] == 1
    assert spend["prompt_tokens"] == 10
    assert spend["completion_tokens"] == 5
    assert spend["cost_usd"] == pytest.approx(0.0001)
    assert spend["unpriced_calls"] == 0
    assert spend["top_models"] == [{
        "model": "stub-model",
        "calls": 1,
        "cost_usd": pytest.approx(0.0001),
        "priced_cost_usd": pytest.approx(0.0001),
        "unpriced_calls": 0,
    }]
    assert spend["top_callers"] == [{
        "caller": "fixture:reader",
        "calls": 1,
        "cost_usd": pytest.approx(0.0001),
        "priced_cost_usd": pytest.approx(0.0001),
        "unpriced_calls": 0,
    }]

    errors = reads_system.fetch_recent_errors(seeded_conn, _DEPLOYMENT)
    assert {row["kind"] for row in errors} == {"llm_error"}
    summaries = {row["kind"]: row["summary"] for row in errors}
    assert "fixture:reader" in summaries["llm_error"]


def test_snapshot_assembles_all_panel_sections(
    seeded_conn: psycopg.Connection,
) -> None:
    snapshot = reads_system.fetch_snapshot(seeded_conn, _DEPLOYMENT)
    assert snapshot["deployment"] == _DEPLOYMENT
    assert snapshot["generation"]["published"]["generation_id"] == 42
    assert snapshot["live_graph"]["node_count"] == 2
    assert snapshot["live_graph"]["edge_count"] == 1
    assert snapshot["queue"]["by_status"]["queued"] == 0
    assert snapshot["queue"]["by_status"]["leased"] == 0
    assert snapshot["queue"]["by_status"]["dead_letter"] == 0
    assert snapshot["queue"]["source"] == "okg.v_runtime_queue_health"
    assert snapshot["projection"]["pending"] == 1
    assert snapshot["enrichers"][0]["enricher_name"] == "fixture-enricher"
    assert snapshot["llm_spend"]["calls"] == 1
    assert {key for key in snapshot} >= {
        "generation",
        "catalog",
        "live_graph",
        "sources",
        "storage",
        "queue",
        "projection",
        "enrichers",
        "doctor",
        "llm_spend",
        "recent_errors",
    }


def test_fetch_queue_uses_runtime_queue_health(
    seeded_conn: psycopg.Connection,
) -> None:
    with seeded_conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS dbos")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS dbos.workflow_status (
              workflow_uuid text PRIMARY KEY,
              queue_name text,
              status text NOT NULL,
              name text,
              created_at bigint NOT NULL,
              updated_at bigint NOT NULL,
              started_at_epoch_ms bigint,
              priority int
            )
            """
        )
        cur.execute(
            """
            ALTER TABLE dbos.workflow_status
              ADD COLUMN IF NOT EXISTS queue_name text,
              ADD COLUMN IF NOT EXISTS name text,
              ADD COLUMN IF NOT EXISTS created_at timestamptz,
              ADD COLUMN IF NOT EXISTS updated_at timestamptz,
              ADD COLUMN IF NOT EXISTS started_at_epoch_ms bigint,
              ADD COLUMN IF NOT EXISTS priority int
            """
        )
        cur.execute(
            """
            SELECT column_name, data_type
              FROM information_schema.columns
             WHERE table_schema = 'dbos'
               AND table_name = 'workflow_status'
               AND column_name IN ('created_at', 'updated_at')
            """
        )
        time_column_types = {row[0]: row[1] for row in cur.fetchall()}
        numeric_time_types = {
            "bigint", "integer", "numeric", "double precision", "real",
        }
        created_at_expr = (
            "(extract(epoch FROM now()) * 1000)::bigint"
            if time_column_types.get("created_at") in numeric_time_types
            else "now()"
        )
        updated_at_expr = (
            "(extract(epoch FROM now()) * 1000)::bigint"
            if time_column_types.get("updated_at") in numeric_time_types
            else "now()"
        )
        cur.execute("DELETE FROM dbos.workflow_status")
        cur.execute(
            f"""
            INSERT INTO dbos.workflow_status (
              workflow_uuid, queue_name, status, name, created_at, updated_at,
              started_at_epoch_ms, priority
            )
            VALUES
              (
                'viz-waiting', 'okg-test-source-sync', 'ENQUEUED',
                'okg_source_sync',
                {created_at_expr},
                {updated_at_expr},
                NULL,
                100
              ),
              (
                'viz-running', 'okg-test-source-sync', 'PENDING',
                'okg_source_sync',
                {created_at_expr},
                {updated_at_expr},
                (extract(epoch FROM now()) * 1000)::bigint, 20
              ),
              (
                'viz-failed', 'okg-test-source-sync', 'ERROR',
                'okg_source_sync',
                {created_at_expr},
                {updated_at_expr},
                NULL,
                100
              )
            """
        )

    queue = reads_system.fetch_queue(seeded_conn)

    assert queue["source"] == "okg.v_runtime_queue_health"
    assert queue["by_status"]["queued"] == 1
    assert queue["by_status"]["leased"] == 1
    assert queue["by_status"]["dead_letter"] == 1
    assert queue["runtime_queue_health"][0]["queue_name"] == (
        "okg-test-source-sync"
    )


@pytest.mark.perf
def test_system_snapshot_queries_stay_under_fixture_budget(
    seeded_conn: psycopg.Connection,
    record_property,
) -> None:
    profiles = reads_system.assert_snapshot_query_budget(
        seeded_conn,
        _DEPLOYMENT,
        max_execution_ms=500.0,
    )

    required_callers = {
        "fetch_generation",
        "fetch_catalog",
        "fetch_live_graph",
        "fetch_sources",
        "fetch_storage",
        "fetch_queue",
        "fetch_projection_backlog",
        "fetch_enrichers",
        "fetch_doctor",
        "fetch_llm_spend",
        "fetch_recent_errors",
    }
    assert required_callers <= {p["caller"] for p in profiles}
    assert all(p["execution_ms"] >= 0 for p in profiles)
    record_property(
        "system_snapshot_query_profiles",
        json.dumps(
            [
                {
                    "label": p["label"],
                    "execution_ms": p["execution_ms"],
                    "buffers": p["buffers"],
                }
                for p in profiles
            ],
            sort_keys=True,
        ),
    )
