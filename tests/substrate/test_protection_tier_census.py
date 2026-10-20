"""Protection-unit census: coverage, fail-closed unruled handling, and
the chronos cold-fraction measurement (chronos-tiered-rollback-proof,
task.tiered.census)."""
from __future__ import annotations

import psycopg
import pytest
from psycopg import sql

from okg.substrate.deployment_protection_census import (
    POSTGRES_OPEN_INTERVAL_SENTINEL,
    generate_protection_census,
)


pytestmark = pytest.mark.postgres


@pytest.fixture()
def census_conn(clean_runtime: str):
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        yield conn


def _census(conn):
    return generate_protection_census(conn)


def test_census_covers_every_relation_exactly_once(census_conn) -> None:
    census = _census(census_conn)
    keys = [(e["schema"], e["name"]) for e in census["entries"]]
    assert len(keys) == len(set(keys)), "no relation may be double-assigned"
    with census_conn.cursor() as cur:
        cur.execute(
            """
            SELECT n.nspname, c.relname
              FROM pg_class c
              JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE c.relkind IN ('r', 'm', 'p', 'S')
               AND n.nspname NOT IN
                   ('pg_catalog', 'information_schema', 'pg_toast')
               AND n.nspname NOT LIKE 'pg_temp%'
               AND n.nspname NOT LIKE 'pg_toast_temp%'
            """
        )
        live = {(row[0], row[1]) for row in cur.fetchall()}
    assert set(keys) == live, "census must cover every data-bearing relation"
    assert all(e["tier"] == "core" for e in census["entries"]), (
        "census v1 grants no tier cheaper than core; candidacies are "
        "recorded separately"
    )
    assert all(
        e["byte_allocation"]["core_bytes"] == e["total_bytes"]
        and e["byte_allocation"]["sealed_candidate_bytes"] == 0
        and e["byte_allocation"]["rebuildable_candidate_bytes"] == 0
        for e in census["entries"]
    ), "every physical byte remains fail-closed in core in census v1"
    assert census["byte_totals"] == {
        "total_bytes": sum(e["total_bytes"] for e in census["entries"]),
        "core_bytes": sum(e["total_bytes"] for e in census["entries"]),
        "sealed_candidate_bytes": 0,
        "rebuildable_candidate_bytes": 0,
        "fully_assigned": True,
    }
    assert census["observed_at"].endswith("+00:00")


def test_unruled_relation_fails_closed_to_core(census_conn) -> None:
    with census_conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS public.census_unruled_probe")
        cur.execute(
            "CREATE TABLE public.census_unruled_probe (id BIGINT PRIMARY KEY)"
        )
    try:
        census = _census(census_conn)
        probe = [
            e for e in census["entries"]
            if e["name"] == "census_unruled_probe"
        ]
        assert len(probe) == 1
        assert probe[0]["tier"] == "core"
        assert probe[0]["unruled"] is True
        assert probe[0]["rule"] is None
        assert {"schema": "public", "name": "census_unruled_probe"} in (
            census["unruled"]
        )
        assert census["unruled_count"] >= 1
    finally:
        with census_conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS public.census_unruled_probe")


def test_schema_fingerprint_moves_when_relation_shape_changes(census_conn) -> None:
    with census_conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS public.census_fingerprint_probe")
        cur.execute(
            "CREATE TABLE public.census_fingerprint_probe (id BIGINT)"
        )
    try:
        before = _census(census_conn)["schema_fingerprint"]
        with census_conn.cursor() as cur:
            cur.execute(
                "ALTER TABLE public.census_fingerprint_probe "
                "ADD COLUMN payload TEXT"
            )
        after = _census(census_conn)["schema_fingerprint"]
        assert after != before, "in-place DDL must move the fingerprint"
    finally:
        with census_conn.cursor() as cur:
            cur.execute(
                "DROP TABLE IF EXISTS public.census_fingerprint_probe"
            )


def test_chronos_interval_tables_measure_cold_fraction(census_conn) -> None:
    with census_conn.cursor() as cur:
        cur.execute(
            "SELECT c.relname FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = 'public' "
            "  AND c.relname LIKE '\\_chronos\\_b\\_interval\\_%' "
            "  AND c.relkind = 'r'"
        )
        chronos_tables = [row[0] for row in cur.fetchall()]
    if not chronos_tables:
        pytest.skip("test database has no chronos interval tables")
    census = _census(census_conn)
    measured = [
        e for e in census["entries"]
        if e.get("rule") == "chronos_interval_history"
    ]
    assert measured, "chronos interval tables must match the history rule"
    for entry in measured:
        assert entry["sealed_candidate"] is True
        assert entry["tier"] == "core", (
            "sealed is a candidacy, not a granted tier"
        )
        coldness = entry["coldness"]
        assert coldness["row_count"] >= (
            coldness["closed_non_deleted_row_count"]
            + coldness["deleted_row_count"]
        )
        assert (
            0.0 <= coldness["closed_non_deleted_row_fraction"] <= 1.0
        )
        assert coldness["measurement_basis"] == (
            "row_count_only_not_byte_share"
        )


def test_cold_fraction_counts_closed_non_deleted_rows(census_conn) -> None:
    """Seed an interval table and verify closed non-deleted rows."""
    sentinel = POSTGRES_OPEN_INTERVAL_SENTINEL
    with census_conn.cursor() as cur:
        cur.execute(
            "DROP TABLE IF EXISTS public._chronos_b_interval_census_probe"
        )
        cur.execute(
            """
            CREATE TABLE public._chronos_b_interval_census_probe (
                pk BIGINT,
                live_lo NUMERIC(32,0) NOT NULL,
                live_hi NUMERIC(32,0) NOT NULL,
                writer_segment_id BIGINT NOT NULL,
                deleted BOOLEAN NOT NULL DEFAULT FALSE
            )
            """
        )
        cur.execute(
            f"""
            INSERT INTO public._chronos_b_interval_census_probe
                (pk, live_lo, live_hi, writer_segment_id, deleted)
            VALUES
                (1, 0, 5, 1, FALSE),
                (2, 0, {sentinel}, 1, FALSE),
                (3, 0, 7, 1, FALSE),
                (4, 0, 9, 1, TRUE)
            """
        )
    try:
        census = _census(census_conn)
        probe = [
            e for e in census["entries"]
            if e["name"] == "_chronos_b_interval_census_probe"
        ][0]
        assert probe["coldness"] == {
            "row_count": 4,
            "closed_non_deleted_row_count": 2,
            "deleted_row_count": 1,
            "closed_non_deleted_row_fraction": 0.5,
            "measurement_basis": "row_count_only_not_byte_share",
        }
    finally:
        with census_conn.cursor() as cur:
            cur.execute(
                "DROP TABLE IF EXISTS public._chronos_b_interval_census_probe"
            )


def test_quoted_interval_relation_name_is_measured_safely(census_conn) -> None:
    name = '_chronos_b_interval_quote"probe'
    qualified = sql.Identifier("public", name)
    with census_conn.cursor() as cur:
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(qualified))
        cur.execute(
            sql.SQL(
                """
                CREATE TABLE {} (
                    live_lo NUMERIC(32,0) NOT NULL,
                    live_hi NUMERIC(32,0) NOT NULL,
                    writer_segment_id BIGINT NOT NULL,
                    deleted BOOLEAN NOT NULL DEFAULT FALSE
                )
                """
            ).format(qualified)
        )
    try:
        census = _census(census_conn)
        probe = [e for e in census["entries"] if e["name"] == name]
        assert len(probe) == 1
        assert probe[0]["rule"] == "chronos_interval_history"
        assert probe[0]["coldness"]["row_count"] == 0
    finally:
        with census_conn.cursor() as cur:
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(qualified))
