"""Populated-deployment upgrade: converged migrate does no pathological work.

Covers task.migrate-idempotent.populated-proof (CI layer) of
``make-migrate-idempotent`` (#669), design ruling 6 / edit 7. Seeds a populated
database at scale (>=100k ``okg.nomos_decisions`` plus >=100k rows in an FK
child table, current-checksum ledger) and asserts an unchanged upgrade by
SHAPE, with NO wall-clock gate (this repo's shared CI Postgres is too flaky for
a timing bound):

  * the schema body is not executed (the 0000 snapshot row is not rewritten);
  * the five composite nomos FKs are not dropped and re-added (oids stable), so
    the ``ADD CONSTRAINT`` full-table FK re-validation never runs;
  * no data is lost;
  * ``okg doctor --check integrity`` is clean afterward.

The wall-clock is recorded for information only. The operator layer (an actual
run against a real populated deployment copy, minutes-not-hours) is attached as
command_output evidence on the task, not gated here.
"""
from __future__ import annotations

import json
import time
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from okg.substrate.cli.main import main
from okg.substrate.db.migrate import migrate
from okg.substrate.dsn_form import dsn_with_dbname

_SEED_ROWS = 100_000
_EXECUTABLE_CHECKSUM_KEY = "executable_schema_checksum"
_NOMOS_FK_CONSTRAINTS = (
    "federation_connection_requests_nomos_decision_id_fkey",
    "federation_peer_trust_pins_nomos_decision_id_fkey",
    "federation_route_events_nomos_decision_id_fkey",
    "poros_incorporation_events_nomos_decision_id_fkey",
    "nomos_external_claims_nomos_decision_id_fkey",
)


def _seed_populated_nomos(dsn: str, rows: int) -> None:
    """Insert `rows` parent decisions + `rows` FK-referencing child rows."""
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SET statement_timeout = '120s'")
        cur.execute(
            """
            INSERT INTO okg.nomos_decisions (
              deployment_name, decision_id, input_hash, subject, action,
              classification, effect, reason_code, enforcement_point
            )
            SELECT 'default', 'dec_' || g, 'h' || g, 's', 'a',
                   'c', 'allow', 'r', 'ep'
              FROM generate_series(1, %s) AS g
            """,
            (rows,),
        )
        # >=100k child rows in one FK child table, each referencing a real
        # decision so the FK is genuinely populated (the ADD CONSTRAINT the
        # pre-fix block re-ran would full-scan and re-validate all of these).
        cur.execute(
            """
            INSERT INTO okg.federation_route_events (
              route_event_id, local_authority_id, remote_authority_id,
              subject, action, target_address, decision, reason_code,
              nomos_decision_id, nomos_decision_deployment_name
            )
            SELECT 're_' || g, 'la', 'ra', 's', 'a', 'addr', 'allow', 'r',
                   'dec_' || g, 'default'
              FROM generate_series(1, %s) AS g
            """,
            (rows,),
        )


def _snapshot_applied_at(dsn: str):
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT applied_at, result_metadata FROM okg.schema_migrations "
            "WHERE migration_id = '0000_schema_snapshot'"
        )
        return cur.fetchone()


def _nomos_fk_oids(dsn: str) -> dict[str, int]:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT conname, oid FROM pg_constraint WHERE conname = ANY(%s)",
            (list(_NOMOS_FK_CONSTRAINTS),),
        )
        return {row[0]: int(row[1]) for row in cur.fetchall()}


def _row_counts(dsn: str) -> tuple[int, int]:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM okg.nomos_decisions")
        decisions = int(cur.fetchone()[0])
        cur.execute("SELECT count(*) FROM okg.federation_route_events")
        events = int(cur.fetchone()[0])
    return decisions, events


@pytest.mark.postgres
@pytest.mark.slow
def test_converged_upgrade_on_populated_db_does_no_body_or_fk_work(
    okg_dsn: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    dbname = f"okg_populated_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        migrate(dsn)
        _seed_populated_nomos(dsn, _SEED_ROWS)

        applied_before = _snapshot_applied_at(dsn)
        assert applied_before[1].get(_EXECUTABLE_CHECKSUM_KEY), (
            "seeded ledger row must carry the current executable checksum"
        )
        fks_before = _nomos_fk_oids(dsn)
        counts_before = _row_counts(dsn)
        assert counts_before == (_SEED_ROWS, _SEED_ROWS)

        started = time.perf_counter()
        migrate(dsn)  # unchanged upgrade over the populated DB
        elapsed = time.perf_counter() - started
        # Informational only — NOT a gate (flaky shared CI Postgres).
        print(f"\n[populated-proof] converged upgrade wall-clock: {elapsed:.3f}s")

        applied_after = _snapshot_applied_at(dsn)
        assert applied_after[0] == applied_before[0], (
            "schema body was replayed on the populated converged upgrade "
            "(0000 snapshot row rewritten)"
        )
        assert _nomos_fk_oids(dsn) == fks_before, (
            "composite nomos FKs were dropped and re-added on a converged "
            "upgrade — the pathological ADD-CONSTRAINT re-validation ran"
        )
        assert _row_counts(dsn) == counts_before, "converged upgrade lost rows"

        doctor_rc = main([
            "doctor",
            "--json",
            "--check",
            "integrity",
            "--dsn",
            dsn,
        ])
        captured = capsys.readouterr()
        doctor_output = captured.out.strip().splitlines()
        assert doctor_output, "doctor emitted no JSON result"
        # A refusal goes to stderr and leaves only this test's own print on
        # stdout; report it rather than a JSONDecodeError on that print.
        assert doctor_output[-1].startswith("{"), (doctor_rc, captured.err)
        doctor = json.loads(doctor_output[-1])
        assert doctor_rc == 0, doctor
        assert doctor["ok"] is True, doctor
        assert doctor["errors"] == [], doctor
    finally:
        _drop_disposable_database(okg_dsn, dbname)


# --------------------------------------------------------------------------
# Helpers.
# --------------------------------------------------------------------------


def _create_disposable_database(base_dsn: str, dbname: str) -> str:
    info = conninfo_to_dict(base_dsn)
    info["dbname"] = "postgres"
    maintenance_dsn = make_conninfo(**info)
    with psycopg.connect(
        maintenance_dsn, autocommit=True, connect_timeout=5,
    ) as conn, conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
    # A URL, not make_conninfo's keyword form: `okg doctor --dsn` refuses the
    # keyword form (PACT s15-2001-deployment-dsn-form), and this DSN reaches it.
    dsn = dsn_with_dbname(base_dsn, dbname)
    with psycopg.connect(dsn, autocommit=True, connect_timeout=5) as conn, \
            conn.cursor() as cur:
        for extension in (
            "btree_gist",
            "timescaledb",
            "vector",
            "pg_textsearch",
            "pg_trgm",
            "fuzzystrmatch",
        ):
            cur.execute(
                sql.SQL("CREATE EXTENSION IF NOT EXISTS {}").format(
                    sql.Identifier(extension)
                )
            )
    return dsn


def _drop_disposable_database(base_dsn: str, dbname: str) -> None:
    info = conninfo_to_dict(base_dsn)
    info["dbname"] = "postgres"
    maintenance_dsn = make_conninfo(**info)
    with psycopg.connect(
        maintenance_dsn, autocommit=True, connect_timeout=5,
    ) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT pg_terminate_backend(pid)
              FROM pg_stat_activity
             WHERE datname = %s
               AND pid <> pg_backend_pid()
            """,
            (dbname,),
        )
        cur.execute(
            sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(dbname))
        )
