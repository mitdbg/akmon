"""Witness for req.bound-disk.telemetry-compression: compressing a
TimescaleDB chunk changes bytes, never rows.

The live repair (2026-07-17) fixed the never-initialized job scheduler and
compressed 9 chunks across four hypertables (graph_report evidence on the
pact). This hermetic witness pins the correctness half of the acceptance:
queries over a compressed chunk return exactly the same rows as before
compression.
"""
from __future__ import annotations

import pytest

psycopg = pytest.importorskip("psycopg")

pytestmark = pytest.mark.postgres


def test_compressed_chunk_returns_identical_rows(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS compress_witness CASCADE")
            cur.execute(
                """
                CREATE TABLE compress_witness (
                    ts   timestamptz NOT NULL,
                    kind text NOT NULL,
                    val  double precision NOT NULL
                )
                """
            )
            cur.execute(
                "SELECT create_hypertable('compress_witness', 'ts', "
                "chunk_time_interval => interval '1 day')"
            )
            cur.execute(
                """
                ALTER TABLE compress_witness SET (
                    timescaledb.compress,
                    timescaledb.compress_segmentby = 'kind'
                )
                """
            )
            # Two chunks: an old one (compressible) and a recent one.
            cur.execute(
                """
                INSERT INTO compress_witness
                SELECT '2026-06-01'::timestamptz + (i || ' minutes')::interval,
                       'k' || (i % 3), i * 1.5
                  FROM generate_series(1, 500) AS i
                """
            )
            cur.execute(
                """
                INSERT INTO compress_witness
                SELECT now() - (i || ' minutes')::interval, 'recent', i * 2.0
                  FROM generate_series(1, 50) AS i
                """
            )

            cur.execute(
                "SELECT count(*), round(sum(val)::numeric, 3) FROM compress_witness"
            )
            before_all = cur.fetchone()
            cur.execute(
                """
                SELECT kind, count(*), round(sum(val)::numeric, 3)
                  FROM compress_witness
                 WHERE ts < '2026-06-02'
                 GROUP BY kind ORDER BY kind
                """
            )
            before_old = cur.fetchall()

            # Compress every chunk older than the recent window — the same
            # operation the (now-repaired) policy performs on schedule.
            cur.execute(
                """
                SELECT compress_chunk(c.chunk_schema || '.' || c.chunk_name)
                  FROM timescaledb_information.chunks c
                 WHERE c.hypertable_name = 'compress_witness'
                   AND c.range_end < now() - interval '7 days'
                """
            )
            cur.execute(
                """
                SELECT count(*) FILTER (WHERE is_compressed), count(*)
                  FROM timescaledb_information.chunks
                 WHERE hypertable_name = 'compress_witness'
                """
            )
            compressed, total = cur.fetchone()
            assert compressed >= 1, "no chunk compressed — witness is vacuous"
            assert total >= 2

            cur.execute(
                "SELECT count(*), round(sum(val)::numeric, 3) FROM compress_witness"
            )
            assert cur.fetchone() == before_all
            cur.execute(
                """
                SELECT kind, count(*), round(sum(val)::numeric, 3)
                  FROM compress_witness
                 WHERE ts < '2026-06-02'
                 GROUP BY kind ORDER BY kind
                """
            )
            assert cur.fetchall() == before_old

            cur.execute("DROP TABLE compress_witness CASCADE")
