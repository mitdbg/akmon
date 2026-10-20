from __future__ import annotations

from typing import Any

import psycopg
import pytest

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.publisher.native_view_stage import NativeViewStage
from okg.substrate.publisher.publisher import Publisher
from tests.substrate.test_publisher_native_views import (
    _materializer,
    _seed_publication_node,
    _seed_view_cites_catalog,
)
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def test_citation_projection_failure_is_degraded_and_retry_recovers(
    clean_runtime: str,
    monkeypatch,
) -> None:
    view_id, materializer = _materializer()
    cited_node_id = "publication:issue-979-citation"
    materializer.anchor_resolver = lambda anchor_id: (
        cited_node_id if anchor_id == "summary" else None
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_view_cites_catalog(conn)
        _seed_publication_node(conn, cited_node_id)

    original = NativeViewStage._project_view_cites
    phase_events: list[tuple[str, dict[str, Any]]] = []

    def fail_projection(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("injected citation projection failure")

    def capture_phase_event(
        _timings: dict[str, object],
        name: str,
        payload: dict[str, Any],
    ) -> None:
        phase_events.append((name, payload))

    monkeypatch.setattr(NativeViewStage, "_project_view_cites", fail_projection)
    monkeypatch.setattr(
        "okg.substrate.publisher.publisher._emit_phase_event_with_timing",
        capture_phase_event,
    )
    degraded = Publisher(
        clean_runtime,
        "default",
        materializers=[materializer],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()

    expected = {
        "status": "degraded",
        "cause": "view_cites_projection_failed",
        "omitted_work": ["view_cites_node_projection"],
        "scope": {
            "view_id": view_id,
            "generation_id": degraded["compat_generation_id"],
        },
        "retryable": True,
        "freshness": "degraded",
    }
    view = degraded["views"][0]
    assert {key: view[key] for key in expected} == expected
    assert view["error"] is None

    refresh = degraded["native_view_refresh"]
    assert refresh["status"] == "materialized_with_degradation"
    assert refresh["degraded_view_count"] == 1
    assert {key: refresh["degraded_views"][0][key] for key in expected} == expected
    view_event = next(
        payload
        for name, payload in phase_events
        if name == "post_publish_views_complete"
    )
    assert view_event["degraded_view_count"] == 1
    assert view_event["degraded_view_ids"] == [view_id]
    assert [
        {key: item[key] for key in expected}
        for item in view_event["degraded_views"]
    ] == [expected]

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT metadata->'degraded_outcomes'
                  FROM okg.derived_output_manifests
                 WHERE metadata ? 'degraded_outcomes'
                 ORDER BY promoted_at DESC
                 LIMIT 1
                """,
            )
            durable_degradations = cur.fetchone()[0]
    assert durable_degradations == [
        {key: expected[key] for key in expected if key != "freshness"}
        | {"freshness": "degraded"}
    ]

    monkeypatch.setattr(NativeViewStage, "_project_view_cites", original)
    recovered = Publisher(
        clean_runtime,
        "default",
        materializers=[materializer],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()

    assert recovered["views"][0]["status"] == "success"
    assert recovered["views"][0]["freshness"] == "fresh"
    assert recovered["native_view_refresh"]["degraded_view_count"] == 0
    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        rows = store.checkout_generation(recovered["generation_id"]).query(
            """
            SELECT dst
              FROM okg.graph_edges
             WHERE src = :src
               AND edge_type = 'view_cites_node'
            """,
            {"src": f"view:{view_id}"},
        )
    finally:
        store.close()
    assert [row["dst"] for row in rows] == [cited_node_id]


def test_citation_replacement_is_atomic_when_insert_sql_fails(
    clean_runtime: str,
) -> None:
    view_id, materializer = _materializer()
    original_cited = "publication:issue-979-original-citation"
    replacement_cited = "publication:issue-979-replacement-citation"
    materializer.anchor_resolver = lambda anchor_id: (
        original_cited if anchor_id == "summary" else None
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_view_cites_catalog(conn)
        _seed_publication_node(conn, original_cited)
        _seed_publication_node(conn, replacement_cited)

    baseline = Publisher(
        clean_runtime,
        "default",
        materializers=[materializer],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    assert baseline["views"][0]["status"] == "success"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE FUNCTION okg.issue_979_reject_citation_insert()
                RETURNS trigger
                LANGUAGE plpgsql
                AS $$
                BEGIN
                  IF NEW.edge_type = 'view_cites_node' AND NEW.op = 'I' THEN
                    RAISE EXCEPTION 'injected citation insert failure';
                  END IF;
                  RETURN NEW;
                END
                $$
                """,
            )
            cur.execute(
                """
                CREATE TRIGGER issue_979_reject_citation_insert
                BEFORE INSERT ON okg.edge_facts
                FOR EACH ROW
                EXECUTE FUNCTION okg.issue_979_reject_citation_insert()
                """,
            )

    replacement_materializer = _materializer()[1]
    replacement_materializer.anchor_resolver = lambda anchor_id: (
        replacement_cited if anchor_id == "summary" else None
    )
    try:
        with psycopg.connect(clean_runtime) as caller_conn:
            assert caller_conn.autocommit is False
            degraded = Publisher(
                clean_runtime,
                "default",
                materializers=[replacement_materializer],
                catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
            ).cycle(
                conn=caller_conn,
                force_native_view_refresh=True,
            )
    finally:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DROP TRIGGER issue_979_reject_citation_insert "
                    "ON okg.edge_facts",
                )
                cur.execute(
                    "DROP FUNCTION okg.issue_979_reject_citation_insert()",
                )

    assert degraded["views"][0]["status"] == "degraded"
    assert degraded["views"][0]["cause"] == "view_cites_projection_failed"
    failed_generation = str(degraded["compat_generation_id"])
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT op
                  FROM okg.edge_facts
                 WHERE edge_type = 'view_cites_node'
                   AND dedupe_key LIKE %(generation_marker)s
                """,
                {"generation_marker": f"%gen:{failed_generation}|%"},
            )
            assert cur.fetchall() == []
            cur.execute(
                """
                SELECT metadata->'degraded_outcomes'
                  FROM okg.derived_output_manifests
                 WHERE metadata ? 'degraded_outcomes'
                 ORDER BY promoted_at DESC
                 LIMIT 1
                """,
            )
            durable_degradations = cur.fetchone()[0]
    assert durable_degradations[0]["cause"] == "view_cites_projection_failed"
    assert durable_degradations[0]["freshness"] == "degraded"

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        rows = store.checkout("default").query(
            """
            SELECT dst
              FROM okg.graph_edges
             WHERE src = :src
               AND edge_type = 'view_cites_node'
            """,
            {"src": f"view:{view_id}"},
        )
    finally:
        store.close()
    assert [row["dst"] for row in rows] == [original_cited]
