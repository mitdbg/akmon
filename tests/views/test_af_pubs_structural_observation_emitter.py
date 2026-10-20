from __future__ import annotations

import importlib.util
from pathlib import Path

import psycopg
import pytest
from psycopg.types.json import Jsonb

pytestmark = pytest.mark.postgres

_REPO_ROOT = Path(__file__).resolve().parents[2]
_EMITTER_PATH = (
    _REPO_ROOT
    / "deployments"
    / "af_pubs"
    / "enrichers"
    / "structural_observation_emitter.py"
)
if not _EMITTER_PATH.is_file():
    pytest.skip(
        "af_pubs structural observation emitter is not present in this checkout",
        allow_module_level=True,
    )
_SPEC = importlib.util.spec_from_file_location(
    "af_pubs_structural_observation_emitter",
    _EMITTER_PATH,
)
assert _SPEC is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
assert _SPEC.loader is not None
_SPEC.loader.exec_module(_MODULE)
StructuralObservationEmitter = _MODULE.StructuralObservationEmitter


def _ensure_narrowing(
    cur: psycopg.Cursor,
    parent_subtype: str,
    edge_type: str,
    child_subtype: str,
) -> None:
    cur.execute(
        """
        INSERT INTO okg.narrowings
          (parent_subtype, edge_type, child_subtype, catalog_version_id)
        VALUES (
          %s, %s, %s,
          COALESCE((
            SELECT max(catalog_version_id)
              FROM okg.catalog_versions
             WHERE status = 'applied'
          ), 1)
        )
        ON CONFLICT DO NOTHING
        """,
        (parent_subtype, edge_type, child_subtype),
    )


def _insert_generation(
    conn: psycopg.Connection,
    generation_id: int,
    *,
    status: str = "published",
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.graph_generations
              (generation_id, deployment_name, status,
               catalog_version_id, correlation_id, published_at)
            VALUES (%s, 'test', %s, 1, %s, now())
            ON CONFLICT (generation_id) DO NOTHING
            """,
            (generation_id, status, f"test-gen-{generation_id}"),
        )


def _seed_r3_surface(conn: psycopg.Connection) -> None:
    _insert_generation(conn, 1)
    with conn.cursor() as cur:
        _ensure_narrowing(cur, "publication", "supersedes", "publication")
        for node_id in ("publication:src", "publication:dst"):
            cur.execute(
                """
                INSERT INTO okg.nodes_live
                  (node_id, subtype, attrs, valid_from_gen)
                VALUES (
                  %s, 'publication',
                  '{"status":"current"}'::jsonb,
                  1
                )
                """,
                (node_id,),
            )
        cur.execute(
            """
            INSERT INTO okg.edges_live
              (edge_id, src, dst, edge_type, attrs, valid_from_gen)
            VALUES (
              1, 'publication:src', 'publication:dst',
              'supersedes', '{}'::jsonb, 1
            )
            """
        )


def test_structural_observation_heartbeat_does_not_reversion(
    clean_runtime: str,
) -> None:
    emitter = StructuralObservationEmitter()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_r3_surface(conn)

        _insert_generation(conn, 2)
        first = emitter.enrich(conn, generation_id=2)
        assert first.n_edges_emitted == 0

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)
                  FROM okg.node_facts
                 WHERE subtype = 'observation'
                """
            )
            first_fact_count = cur.fetchone()[0]
            cur.execute(
                """
                SELECT attrs->>'last_confirmed_gen'
                  FROM okg.nodes_live
                 WHERE node_id = %s
                   AND valid_from_gen = 2
                   AND valid_to_gen IS NULL
                """,
                ("observation:r3:publication:dst:publication:src",),
            )
            assert cur.fetchone()[0] == "2"

        _insert_generation(conn, 3)
        emitter.enrich(conn, generation_id=3)

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)
                  FROM okg.node_facts
                 WHERE subtype = 'observation'
                """
            )
            assert cur.fetchone()[0] == first_fact_count
            cur.execute(
                """
                SELECT valid_from_gen, valid_to_gen,
                       attrs->>'last_confirmed_gen'
                  FROM okg.nodes_live
                 WHERE node_id = %s
                 ORDER BY valid_from_gen
                """,
                ("observation:r3:publication:dst:publication:src",),
            )
            rows = cur.fetchall()

    assert rows == [(2, None, "3")]


def test_structural_observation_same_generation_retry_updates_in_place(
    clean_runtime: str,
) -> None:
    emitter = StructuralObservationEmitter()
    obs_id = "observation:r3:publication:dst:publication:src"
    stale_attrs = {
        "observation_id": obs_id,
        "rule_id": "r3",
        "rule_kind": "structural",
        "severity": "info",
        "target_pub_id": "publication:dst",
        "src_pub_id": "publication:src",
        "edge_id": 999,
        "verdict": "n/a",
        "verifier_version": 0,
        "detected_at_gen": 2,
        "last_confirmed_gen": 2,
    }
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_r3_surface(conn)
        _insert_generation(conn, 2, status="pending")
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.nodes_live
                  (node_id, subtype, attrs, valid_from_gen)
                VALUES (%s, 'observation', %s::jsonb, 2)
                """,
                (
                    obs_id,
                    Jsonb(stale_attrs),
                ),
            )

        emitter.enrich(conn, generation_id=2)

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT valid_from_gen, valid_to_gen, attrs->>'edge_id'
                  FROM okg.nodes_live
                 WHERE node_id = %s
                 ORDER BY valid_from_gen
                """,
                (obs_id,),
            )
            rows = cur.fetchall()

    assert rows == [(2, None, "1")]
