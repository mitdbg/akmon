"""Chronos-native publisher checks for the old projection-watermark area.

The default publisher no longer dispatches Phase A projector batches or
advances ``source_watermarks.projection_cursor``. These tests assert that
graph publication comes from the Chronos branch, while keeping one direct
unit check for the legacy sweep helper.
"""
from __future__ import annotations

import json
import uuid

import psycopg
import pytest

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.projection.projector import (
    Projector,
)
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def _seed_catalog(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        return cur.fetchone()[0]


def _seed_node_fact(
    conn: psycopg.Connection,
    *, node_id: str, source: str,
) -> int:
    run_id = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.node_facts
              (node_id, subtype, attrs, source, source_record_id,
               source_revision, observed_at, dedupe_key, run_id, op)
            VALUES (%s, 'concept', %s::jsonb, %s,
                    %s::jsonb, %s::jsonb,
                    now(), %s, %s, 'I')
            RETURNING fact_id
            """,
            (
                node_id, json.dumps({"text": f"text:{node_id}"}),
                source,
                json.dumps({"id": node_id}),
                json.dumps({"v": 1, "rev": run_id}),
                f"{run_id}:{node_id}",
                run_id,
            ),
        )
        return cur.fetchone()[0]


def _apply_chronos_nodes(
    dsn: str,
    *,
    source: str,
    prefix: str,
    n: int,
) -> None:
    facts = [
        NodeFact(
            node_id=f"{prefix}:{i}",
            subtype="concept",
            attrs={"text": f"text:{prefix}:{i}"},
            source_record_id={"id": f"{prefix}:{i}"},
            source_revision={"v": 1},
        )
        for i in range(n)
    ]
    store = ChronosGraphStore.connect(dsn)
    try:
        store.apply_facts(facts, source=source)
    finally:
        store.close()


def test_multi_source_cycle_advances_per_source_cursors(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog(conn)
    _apply_chronos_nodes(clean_runtime, source="src_A", prefix="n:a", n=8)
    _apply_chronos_nodes(clean_runtime, source="src_B", prefix="n:b", n=8)
    _apply_chronos_nodes(clean_runtime, source="src_C", prefix="n:c", n=8)

    out = Publisher(
        clean_runtime, "default",
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    assert out["status"] == "published", out

    store = ChronosGraphStore.connect(clean_runtime)
    try:
        checkout = store.checkout_generation(out["generation_id"])
        rows = checkout.query(
            "SELECT source, count(*) AS n FROM okg.graph_nodes "
            " WHERE source IN ('src_A','src_B','src_C') "
            " GROUP BY source ORDER BY source"
        )
    finally:
        store.close()
    assert {row["source"]: row["n"] for row in rows} == {
        "src_A": 8, "src_B": 8, "src_C": 8,
    }


def test_projection_pending_empty_after_cycle(
    clean_runtime: str,
) -> None:
    """Chronos-native cycles do not create projection_pending rows."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog(conn)
    _apply_chronos_nodes(clean_runtime, source="sweepy", prefix="n:x", n=4)

    out = Publisher(
        clean_runtime, "default",
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    assert out["status"] == "published", out

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.projection_pending"
        )
        assert cur.fetchone()[0] == 0




def test_sweep_helper_returns_distinct_source_count(
    clean_runtime: str,
) -> None:
    """`Projector.sweep_pending_watermarks` returns the count
    of distinct sources whose cursors advanced. (Useful for
    operators tracing whether a particular cycle moved any
    cursors.)"""
    proj = Projector(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog(conn)
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO okg.projection_pending
                  (generation_id, source, new_cursor, batch_index)
                VALUES (%s, %s, %s, %s)
                """,
                [
                    (101, "alpha", 2, 0),
                    (101, "alpha", 4, 1),
                    (101, "beta", 3, 2),
                ],
            )
        n = proj.sweep_pending_watermarks(
            conn, generation_id=101,
        )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT source, projection_cursor "
                "FROM okg.source_watermarks ORDER BY source"
            )
            cursors = dict(cur.fetchall())
            cur.execute(
                "SELECT count(*) FROM okg.projection_pending "
                "WHERE generation_id = 101"
            )
            pending = cur.fetchone()[0]
        n_after = proj.sweep_pending_watermarks(
            conn, generation_id=101,
        )
    assert n == 2
    assert cursors == {"alpha": 4, "beta": 3}
    assert pending == 0
    assert n_after == 0
