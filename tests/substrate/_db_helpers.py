"""Small DB helpers for substrate tests."""
from __future__ import annotations

import psycopg


def seed_catalog_version(conn: psycopg.Connection) -> int:
    """Insert an `applied` ``okg.catalog_versions`` row and return its id.

    Hoisted from 14 near-duplicate `_seed_catalog_version` definitions
    that lived in individual test files. The substrate migration's
    bootstrap seed already inserts `catalog_version_id=1`; this helper
    appends a fresh row so tests get a known-distinct id without
    relying on the bootstrap row being present (some `clean_runtime`
    paths TRUNCATE catalog_versions to enforce a deterministic
    starting state).
    """
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) "
            "RETURNING catalog_version_id"
        )
        return cur.fetchone()[0]


def ensure_published_generation(
    conn: psycopg.Connection,
    generation_id: int = 1,
    *,
    deployment_name: str = "test",
    catalog_version_id: int | None = None,
) -> int:
    """Ensure a live-row FK target and Chronos checkpoint exist."""
    with conn.cursor() as cur:
        if catalog_version_id is None:
            cur.execute(
                "SELECT catalog_version_id "
                "  FROM okg.catalog_versions "
                " WHERE status = 'applied' "
                " ORDER BY catalog_version_id DESC LIMIT 1"
            )
            row = cur.fetchone()
            if row is None:
                cur.execute(
                    "INSERT INTO okg.catalog_versions "
                    "  (status, applied_at) "
                    "VALUES ('applied', now()) "
                    "RETURNING catalog_version_id"
                )
                catalog_version_id = cur.fetchone()[0]
            else:
                catalog_version_id = row[0]

        cur.execute(
            """
            INSERT INTO okg.graph_generations
              (generation_id, deployment_name, status,
               fact_watermark, edge_fact_watermark,
               catalog_version_id, published_at, correlation_id,
               invariant_summary, excluded_sources)
            VALUES
              (%s, %s, 'published', 0, 0, %s, now(), %s,
               '{}'::jsonb, '[]'::jsonb)
            ON CONFLICT (generation_id) DO UPDATE SET
              deployment_name = EXCLUDED.deployment_name,
              status = EXCLUDED.status,
              catalog_version_id = EXCLUDED.catalog_version_id,
              published_at = EXCLUDED.published_at,
              correlation_id = EXCLUDED.correlation_id
            """,
            (
                generation_id,
                deployment_name,
                catalog_version_id,
                f"test:{deployment_name}:generation:{generation_id}",
            ),
        )
        cur.execute(
            "SELECT setval("
            "  pg_get_serial_sequence('okg.graph_generations', "
            "                         'generation_id'), "
            "  GREATEST(%s, (SELECT max(generation_id) "
            "                  FROM okg.graph_generations))"
            ")",
            (generation_id,),
        )
    from okg.substrate.graph_store import ChronosGraphStore

    ChronosGraphStore.from_connection(conn).checkpoint_generation(
        generation_id,
        deployment_name=deployment_name,
        branch="default",
        catalog_version_id=catalog_version_id,
        metadata={"correlation_id": f"test:{deployment_name}:generation:{generation_id}"},
    )
    return generation_id
