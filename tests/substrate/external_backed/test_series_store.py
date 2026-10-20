"""req.external-backed-nodes.series-store.

node_series is a TimescaleDB hypertable (+ retention) keyed by node_id, outside
the Chronos graph and branch-agnostic; the probe cache key includes a
backing_fingerprint so divergent per-branch handles never collide.
"""
from __future__ import annotations

import psycopg
import psycopg.errors
import pytest

pytestmark = pytest.mark.postgres

_DEPLOY = "okg-workspace"
_NODE = "node:cluster:gpu07"


def _conn(dsn: str) -> psycopg.Connection:
    return psycopg.connect(dsn, autocommit=True)


def test_node_series_is_a_hypertable_with_retention(clean_runtime: str) -> None:
    with _conn(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM timescaledb_information.hypertables "
            "WHERE hypertable_schema='okg' AND hypertable_name='node_series'"
        )
        assert cur.fetchone()[0] == 1
        cur.execute(
            "SELECT count(*) FROM timescaledb_information.jobs "
            "WHERE hypertable_schema='okg' AND hypertable_name='node_series' "
            "AND proc_name='policy_retention'"
        )
        assert cur.fetchone()[0] >= 1


def test_samples_insert_and_read_by_node_id(clean_runtime: str) -> None:
    with _conn(clean_runtime) as conn, conn.cursor() as cur:
        for v in (60.0, 64.0, 71.0):
            cur.execute(
                "INSERT INTO okg.node_series "
                "(deployment_name, node_id, metric, value, source) "
                "VALUES (%s,%s,'gpu_util',%s,'collector')",
                (_DEPLOY, _NODE, v),
            )
        cur.execute(
            "SELECT value FROM okg.node_series "
            "WHERE deployment_name=%s AND node_id=%s AND metric='gpu_util' "
            "ORDER BY observed_at DESC, sample_id DESC LIMIT 1",
            (_DEPLOY, _NODE),
        )
        assert cur.fetchone()[0] == 71.0


def test_series_not_in_graph_fact_tables(clean_runtime: str) -> None:
    with _conn(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.node_series (deployment_name, node_id, metric, value) "
            "VALUES (%s,%s,'gpu_util',64.0)",
            (_DEPLOY, _NODE),
        )
        cur.execute("SELECT count(*) FROM okg.graph_nodes WHERE node_id=%s", (_NODE,))
        assert cur.fetchone()[0] == 0
        cur.execute("SELECT count(*) FROM okg.node_facts WHERE node_id=%s", (_NODE,))
        assert cur.fetchone()[0] == 0


def test_status_enum_rejects_bad_value(clean_runtime: str) -> None:
    with _conn(clean_runtime) as conn, conn.cursor() as cur:
        with pytest.raises(psycopg.errors.CheckViolation):
            cur.execute(
                "INSERT INTO okg.node_series "
                "(deployment_name, node_id, metric, value, status) "
                "VALUES (%s,%s,'gpu_util',1.0,'bogus')",
                (_DEPLOY, _NODE),
            )


def test_probe_cache_key_includes_backing_fingerprint(clean_runtime: str) -> None:
    # Two rows that differ ONLY by backing_fingerprint must coexist (different
    # branch handles for the same node read different cached probes).
    with _conn(clean_runtime) as conn, conn.cursor() as cur:
        for fp, payload in (("fpA", '{"v":"A"}'), ("fpB", '{"v":"B"}')):
            cur.execute(
                """
                INSERT INTO okg.external_probe_cache
                  (deployment_name, node_id, backing_fingerprint, field,
                   observed_at, expires_at, status, payload)
                VALUES (%s,%s,%s,'status', now(), now()+interval '60s', 'ok', %s::jsonb)
                """,
                (_DEPLOY, _NODE, fp, payload),
            )
        cur.execute(
            "SELECT count(*), count(DISTINCT backing_fingerprint) "
            "FROM okg.external_probe_cache WHERE node_id=%s",
            (_NODE,),
        )
        count, distinct_fp = cur.fetchone()
        assert count == 2 and distinct_fp == 2


def test_probe_cache_pk_collapses_same_fingerprint(clean_runtime: str) -> None:
    # Same (node, fingerprint, field): the PK forces one row (upsert target).
    with _conn(clean_runtime) as conn, conn.cursor() as cur:
        for payload in ('{"v":1}', '{"v":2}'):
            cur.execute(
                """
                INSERT INTO okg.external_probe_cache
                  (deployment_name, node_id, backing_fingerprint, field,
                   observed_at, expires_at, status, payload)
                VALUES (%s,%s,'fp','status', now(), now()+interval '60s', 'ok', %s::jsonb)
                ON CONFLICT (deployment_name, node_id, backing_fingerprint, field)
                DO UPDATE SET payload = EXCLUDED.payload
                """,
                (_DEPLOY, _NODE, payload),
            )
        cur.execute(
            "SELECT count(*), max(payload->>'v') FROM okg.external_probe_cache "
            "WHERE node_id=%s AND backing_fingerprint='fp'",
            (_NODE,),
        )
        count, latest = cur.fetchone()
        assert count == 1 and latest == "2"
