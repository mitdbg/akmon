"""A destructive migration's recorded postcondition must stay true (issue #1054).

`0001_retire_job_runtime_ledger` recorded seven relation-absence flags as true.
The relations exist today on okg-workspace holding 4,263 rows. Two independent
gaps let that stand: nothing asserted the postcondition before recording it, and
nothing ever re-checked the recorded claim afterwards.

These tests cover both, plus the vacuity paths that would let either check go
quiet -- an absent postcondition key, a postcondition that is not a mapping, and
a flag with no declared way to be re-evaluated.
"""
from __future__ import annotations

from typing import Any
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.types.json import Jsonb

from okg.substrate.db.management import (
    BLOCKING_POSTCONDITION_DRIFT_STATUSES,
    KnownMigration,
    POSTCONDITION_UNVERIFIED_KEY,
    ensure_schema_migrations,
    migration_status,
    record_known_migration,
    record_unverified_migration,
)
from okg.substrate.db.migration_postcondition import (
    POSTCONDITION_FLAGS,
    PostconditionUnevaluable,
    UnknownPostconditionFlag,
    evaluate_postcondition,
)
from okg.substrate.errors import SubstrateError


pytestmark = pytest.mark.postgres


DESTRUCTIVE = KnownMigration(
    migration_id="9001_test_destructive",
    description="test destructive migration",
    checksum="0" * 64,
    destructive=True,
)
HARMLESS = KnownMigration(
    migration_id="9002_test_harmless",
    description="test non-destructive migration",
    checksum="1" * 64,
    destructive=False,
)


def _create_disposable_database(admin_dsn: str, dbname: str) -> str:
    with psycopg.connect(admin_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
    params = conninfo_to_dict(admin_dsn)
    params["dbname"] = dbname
    return make_conninfo(**params)


def _drop_disposable_database(admin_dsn: str, dbname: str) -> None:
    with psycopg.connect(admin_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                sql.Identifier(dbname)
            )
        )


@pytest.fixture(scope="module")
def _postcondition_database(okg_dsn: str):
    """ONE disposable database for this module, not one per test.

    CREATE/DROP DATABASE is the dominant cost of these tests on the shared CI
    Postgres, and the substrate-db lane runs inside its CI job's measured time
    limit (tests/ci/test_lane_timeout_headroom.py). Per-test databases would
    spend that time. Each test still starts from a bare schema — see the reset
    below — so isolation is unchanged.
    """
    dbname = f"okg_postcondition_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        yield dsn
    finally:
        _drop_disposable_database(okg_dsn, dbname)


@pytest.fixture()
def bare_dsn(_postcondition_database: str) -> str:
    """A database whose `okg` schema is empty apart from the ledger table.

    Dropping and recreating the schema is far cheaper than recreating the
    database, and it is the same starting state: these tests create their own
    relations (okg.job_runs, marker views) and must not see each other's.
    """
    with psycopg.connect(_postcondition_database, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS okg CASCADE")
            cur.execute("CREATE SCHEMA okg")
    with psycopg.connect(_postcondition_database) as conn:
        ensure_schema_migrations(conn)
        conn.commit()
    return _postcondition_database


def _ledger_rows(dsn: str, migration_id: str) -> list[dict[str, Any]]:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT status, result_metadata FROM okg.schema_migrations "
            "WHERE migration_id = %s",
            (migration_id,),
        )
        return [{"status": row[0], "result_metadata": row[1]} for row in cur.fetchall()]


# --- write-time refusals ---------------------------------------------------


def test_falsy_postcondition_is_refused_and_records_nothing(bare_dsn: str) -> None:
    with psycopg.connect(bare_dsn) as conn:
        with pytest.raises(SubstrateError, match="postcondition failed") as excinfo:
            record_known_migration(
                conn,
                DESTRUCTIVE,
                result_metadata={
                    "postcondition": {
                        "job_runs_absent": False,
                        "job_chunks_absent": True,
                    }
                },
            )
        assert excinfo.value.data["failed_flags"] == ["job_runs_absent"]
        conn.commit()
    assert _ledger_rows(bare_dsn, DESTRUCTIVE.migration_id) == []


def test_absent_postcondition_is_refused(bare_dsn: str) -> None:
    """Key-absent must not pass vacuously -- 0001 asserted nothing at all."""
    with psycopg.connect(bare_dsn) as conn:
        with pytest.raises(SubstrateError, match="recorded no postcondition"):
            record_known_migration(
                conn, DESTRUCTIVE, result_metadata={"applied_via": "test"}
            )
        conn.commit()
    assert _ledger_rows(bare_dsn, DESTRUCTIVE.migration_id) == []


def test_non_mapping_postcondition_is_refused(bare_dsn: str) -> None:
    """A bare string cannot be re-checked later, so it is not a postcondition."""
    with psycopg.connect(bare_dsn) as conn:
        with pytest.raises(SubstrateError, match="not a flag mapping"):
            record_known_migration(
                conn,
                DESTRUCTIVE,
                result_metadata={"postcondition": "job_runtime_subsystem_absent"},
            )
        conn.commit()
    assert _ledger_rows(bare_dsn, DESTRUCTIVE.migration_id) == []


def test_undeclared_flag_is_refused(bare_dsn: str) -> None:
    """A flag nobody can re-evaluate is not evidence."""
    with psycopg.connect(bare_dsn) as conn:
        with pytest.raises(SubstrateError, match="undeclared postcondition flags"):
            record_known_migration(
                conn,
                DESTRUCTIVE,
                result_metadata={"postcondition": {"invented_flag_absent": True}},
            )
        conn.commit()
    assert _ledger_rows(bare_dsn, DESTRUCTIVE.migration_id) == []


def test_non_boolean_postcondition_value_is_refused(bare_dsn: str) -> None:
    """The string "false" is truthy in Python and would record as a pass."""
    with psycopg.connect(bare_dsn) as conn:
        with pytest.raises(SubstrateError, match="non-boolean"):
            record_known_migration(
                conn,
                DESTRUCTIVE,
                result_metadata={"postcondition": {"job_runs_absent": "false"}},
            )
        conn.commit()
    assert _ledger_rows(bare_dsn, DESTRUCTIVE.migration_id) == []


def test_declaring_unverified_while_claiming_applied_is_refused(
    bare_dsn: str,
) -> None:
    """The escape hatch must not be a way to record "not checked" as green."""
    with psycopg.connect(bare_dsn) as conn:
        with pytest.raises(SubstrateError, match="declaring its postcondition unverified"):
            record_known_migration(
                conn,
                DESTRUCTIVE,
                result_metadata={
                    POSTCONDITION_UNVERIFIED_KEY: "could not check",
                },
            )
        conn.commit()
    assert _ledger_rows(bare_dsn, DESTRUCTIVE.migration_id) == []
    assert migration_status(bare_dsn)["postcondition_drift"] == []


def test_unverified_downgrades_an_existing_applied_row(bare_dsn: str) -> None:
    """Correcting a falsely-applied row is the whole subject of issue #1054."""
    with psycopg.connect(bare_dsn) as conn:
        record_known_migration(
            conn,
            DESTRUCTIVE,
            result_metadata={"postcondition": {"job_runs_absent": True}},
        )
        conn.commit()
    assert _ledger_rows(bare_dsn, DESTRUCTIVE.migration_id)[0]["status"] == "applied"

    with psycopg.connect(bare_dsn) as conn:
        record_unverified_migration(
            conn, DESTRUCTIVE, reason="recorded claim contradicted by the database"
        )
        conn.commit()
    row = _ledger_rows(bare_dsn, DESTRUCTIVE.migration_id)[0]
    assert row["status"] == "failed"
    assert (
        row["result_metadata"][POSTCONDITION_UNVERIFIED_KEY]
        == "recorded claim contradicted by the database"
    )


def test_non_destructive_migration_needs_no_postcondition(bare_dsn: str) -> None:
    with psycopg.connect(bare_dsn) as conn:
        record_known_migration(
            conn, HARMLESS, result_metadata={"applied_via": "ordered_runner"}
        )
        conn.commit()
    rows = _ledger_rows(bare_dsn, HARMLESS.migration_id)
    assert [row["status"] for row in rows] == ["applied"]


def test_a_row_recorded_applied_but_unverified_clears_ok(bare_dsn: str) -> None:
    """Legacy/hand-written rows in that shape are the defect, not an exemption."""
    with psycopg.connect(bare_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.schema_migrations (
              migration_id, checksum, description, destructive,
              status, result_metadata
            ) VALUES (%s, %s, %s, true, 'applied', %s)
            """,
            (
                "9003_legacy_unverified",
                "3" * 64,
                "hand-written unverified row",
                Jsonb({POSTCONDITION_UNVERIFIED_KEY: "never checked"}),
            ),
        )
    status = migration_status(bare_dsn)
    entry = next(
        item
        for item in status["postcondition_drift"]
        if item["migration_id"] == "9003_legacy_unverified"
    )
    assert entry["status"] == "unverified"
    assert status["ok"] is False


def test_destructive_row_with_unusable_metadata_is_reported(bare_dsn: str) -> None:
    """A destructive row whose metadata is not a mapping cannot be re-checked."""
    with psycopg.connect(bare_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.schema_migrations (
              migration_id, checksum, description, destructive,
              status, result_metadata
            ) VALUES (%s, %s, %s, true, 'applied', %s)
            """,
            (
                "9004_scalar_metadata",
                "4" * 64,
                "scalar result_metadata",
                Jsonb("not a mapping"),
            ),
        )
    status = migration_status(bare_dsn)
    entry = next(
        item
        for item in status["postcondition_drift"]
        if item["migration_id"] == "9004_scalar_metadata"
    )
    assert entry["status"] == "unparseable"
    assert status["ok"] is False


def test_a_superseded_migration_is_not_re_derived(bare_dsn: str) -> None:
    """0002 deliberately drops the marker table 0001 recorded present.

    Re-checking the superseded row would leave a deployment that correctly
    applied 0002 permanently red with nothing able to clear it.
    """
    _force_record(bare_dsn, {"marker_present": True, "job_runs_absent": True})
    with psycopg.connect(bare_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.schema_migrations (
              migration_id, checksum, description, destructive,
              status, result_metadata
            ) VALUES (%s, %s, %s, true, 'applied', %s)
            """,
            (
                "0002_remove_job_runtime_subsystem",
                "5" * 64,
                "remove the job-runtime subsystem",
                Jsonb({"postcondition": {"job_runs_absent": True}}),
            ),
        )
    status = migration_status(bare_dsn)
    entry = next(
        item
        for item in status["postcondition_drift"]
        if item["migration_id"] == "0001_retire_job_runtime_ledger"
    )
    assert entry["status"] == "superseded"
    # `superseded` must not clear `ok` on its own. (The synthetic 0002 row's
    # placeholder checksum trips the unrelated checksum gate, so assert on the
    # drift term directly rather than the composite flag.)
    blocking = [
        item
        for item in status["postcondition_drift"]
        if item["status"] in BLOCKING_POSTCONDITION_DRIFT_STATUSES
    ]
    assert blocking == []


def test_before_supersession_the_false_claim_still_drifts(bare_dsn: str) -> None:
    """Supersession must not become a blanket amnesty for the 0001 row."""
    _seed_job_runs(bare_dsn, 2)
    _force_record(bare_dsn, {"marker_present": True, "job_runs_absent": True})
    status = migration_status(bare_dsn)
    assert any(
        item.get("flag") == "job_runs_absent" and item["status"] == "drifted"
        for item in status["postcondition_drift"]
    )
    assert status["ok"] is False


def test_unverified_records_failed_with_a_reason(bare_dsn: str) -> None:
    """"Could not check" gets its own outcome, never "checked and passed"."""
    with psycopg.connect(bare_dsn) as conn:
        record_unverified_migration(
            conn, DESTRUCTIVE, reason="catalog unreadable under this role"
        )
        conn.commit()
    rows = _ledger_rows(bare_dsn, DESTRUCTIVE.migration_id)
    assert rows[0]["status"] == "failed"
    assert (
        rows[0]["result_metadata"][POSTCONDITION_UNVERIFIED_KEY]
        == "catalog unreadable under this role"
    )
    status = migration_status(bare_dsn)
    applied_ids = {
        row["migration_id"]
        for row in status["applied"]
        if row.get("status") == "applied"
    }
    assert DESTRUCTIVE.migration_id not in applied_ids


def test_unverified_requires_a_reason(bare_dsn: str) -> None:
    with psycopg.connect(bare_dsn) as conn:
        with pytest.raises(SubstrateError, match="requires a reason"):
            record_unverified_migration(conn, DESTRUCTIVE, reason="   ")
        conn.commit()


# --- read-time re-verification --------------------------------------------


def _seed_job_runs(dsn: str, row_count: int) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS okg")
        cur.execute("CREATE TABLE okg.job_runs (id bigint, status text)")
        for index in range(row_count):
            cur.execute(
                "INSERT INTO okg.job_runs (id, status) VALUES (%s, 'retryable')",
                (index,),
            )


def _force_record(dsn: str, postcondition: dict[str, Any]) -> None:
    """Write the ledger row the way 0001 did -- bypassing the new refusal.

    Live deployments carry rows written before the refusal existed, so the
    re-check has to work on rows this code would no longer produce.
    """
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.schema_migrations (
              migration_id, checksum, description, destructive,
              status, result_metadata
            ) VALUES (%s, %s, %s, true, 'applied', %s)
            ON CONFLICT (migration_id) DO UPDATE
              SET result_metadata = EXCLUDED.result_metadata
            """,
            (
                "0001_retire_job_runtime_ledger",
                "2" * 64,
                "retire the OKG job-runtime ledger",
                Jsonb({"postcondition": postcondition}),
            ),
        )


def test_recorded_absence_contradicted_by_the_database_is_drift(
    bare_dsn: str,
) -> None:
    """The exact okg-workspace shape: recorded absent, holds rows."""
    _seed_job_runs(bare_dsn, 3)
    _force_record(bare_dsn, {"job_runs_absent": True})

    status = migration_status(bare_dsn)
    drift = status["postcondition_drift"]
    entry = next(item for item in drift if item.get("flag") == "job_runs_absent")
    assert entry["migration_id"] == "0001_retire_job_runtime_ledger"
    assert entry["status"] == "drifted"
    assert entry["target"] == "okg.job_runs"
    assert entry["recorded"] is True
    assert entry["actual"] is False
    assert entry["row_count"] == 3
    assert status["ok"] is False


def test_an_honest_ledger_reports_no_drift(bare_dsn: str) -> None:
    _force_record(bare_dsn, {"job_runs_absent": True})
    status = migration_status(bare_dsn)
    assert status["postcondition_drift"] == []


def test_a_flag_with_no_declared_recheck_is_unresolved_not_silent(
    bare_dsn: str,
) -> None:
    _force_record(bare_dsn, {"invented_flag_absent": True})
    status = migration_status(bare_dsn)
    entry = status["postcondition_drift"][0]
    assert entry["status"] == "unresolved"
    assert entry["flag"] == "invented_flag_absent"
    assert status["ok"] is False


def test_the_legacy_bare_string_postcondition_is_healed_not_condemned(
    bare_dsn: str,
) -> None:
    """Deployments provisioned before this change carry the string form.

    The fresh-install baseline recorded the flag NAME as the whole
    postcondition. Reading that as unparseable would leave every such
    deployment permanently red with no command able to rewrite the row -- so a
    string that names a declared flag is treated as the claim it is.
    """
    _force_record(bare_dsn, "job_runtime_subsystem_absent")  # type: ignore[arg-type]
    assert migration_status(bare_dsn)["postcondition_drift"] == []

    # ... and it is a real re-check, not a rubber stamp: put the subsystem back
    # and the same recorded string now drifts.
    _seed_job_runs(bare_dsn, 1)
    status = migration_status(bare_dsn)
    entry = next(
        item
        for item in status["postcondition_drift"]
        if item["flag"] == "job_runtime_subsystem_absent"
    )
    assert entry["status"] == "drifted"
    assert status["ok"] is False


def test_an_unparseable_postcondition_is_reported(bare_dsn: str) -> None:
    _force_record(bare_dsn, ["job_runs_absent"])  # type: ignore[arg-type]
    status = migration_status(bare_dsn)
    entry = status["postcondition_drift"][0]
    assert entry["status"] == "unparseable"
    assert status["ok"] is False


def test_a_non_destructive_row_is_not_re_derived(bare_dsn: str) -> None:
    """0003 records shape assertions from its own vocabulary under the same key.

    Re-deriving those through this registry would report `unresolved` on a row
    that never made an absence claim -- a permanent red nothing can clear.
    """
    with psycopg.connect(bare_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.schema_migrations (
              migration_id, checksum, description, destructive,
              status, result_metadata
            ) VALUES (%s, %s, %s, false, 'applied', %s)
            """,
            (
                "0003_deployment_promotion_fence",
                "6" * 64,
                "deployment promotion fence",
                Jsonb(
                    {
                        "postcondition": {
                            "shape_matches_migration": True,
                            "deployment_promotion_fences_present": True,
                        }
                    }
                ),
            ),
        )
    assert migration_status(bare_dsn)["postcondition_drift"] == []


# --- registry coverage -----------------------------------------------------


# Registry coverage and typo detection need no database and must run on every
# PR, so they live in tests/ci/test_postcondition_flag_registry.py rather than
# behind this file's `postgres` marker.


def test_evaluating_an_undeclared_flag_raises(bare_dsn: str) -> None:
    """Emission goes through the registry, so it cannot outrun re-verification."""
    with psycopg.connect(bare_dsn) as conn, conn.cursor() as cur:
        with pytest.raises(UnknownPostconditionFlag):
            evaluate_postcondition(cur, ("invented_flag_absent",))


def test_a_broken_flag_does_not_poison_the_transaction(bare_dsn: str) -> None:
    """`migration_status` keeps using the connection after re-verification.

    A failed statement aborts the whole Postgres transaction, so without a
    savepoint the caller dies on its NEXT query instead of reporting the flag it
    could not check.
    """
    with psycopg.connect(bare_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS okg")
        # A view over a missing table: to_regclass resolves it, selecting fails.
        cur.execute("CREATE TABLE okg.job_runtime_ledger_retirement_marker (id int)")
        cur.execute(
            "CREATE VIEW okg._broken AS SELECT id FROM "
            "okg.job_runtime_ledger_retirement_marker"
        )
        cur.execute("DROP TABLE okg.job_runtime_ledger_retirement_marker CASCADE")
        cur.execute("CREATE VIEW okg.job_runtime_ledger_retirement_marker AS SELECT 1")
        cur.execute("DROP VIEW okg.job_runtime_ledger_retirement_marker")
        cur.execute(
            "CREATE VIEW okg.job_runtime_ledger_retirement_marker AS "
            "SELECT 1 AS id WHERE 1/0 = 1"
        )
    _force_record(bare_dsn, {"marker_present": True})
    # The point: this returns a report instead of raising InFailedSqlTransaction.
    status = migration_status(bare_dsn)
    entry = next(
        item for item in status["postcondition_drift"] if item["flag"] == "marker_present"
    )
    assert entry["status"] in {"unevaluable", "drifted"}
    assert status["ok"] is False


def test_declared_flags_all_evaluate_against_a_real_database(
    bare_dsn: str,
) -> None:
    """Every declaration is executable -- a typo'd target is a hard failure.

    Without this, a mistyped relation in the registry silently evaluates
    "absent: true" forever and the re-check reports agreement it never made.
    """
    with psycopg.connect(bare_dsn) as conn, conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS okg")
        evaluated: dict[str, Any] = {}
        for name in POSTCONDITION_FLAGS:
            try:
                evaluated[name] = evaluate_postcondition(cur, (name,))[name]
            except PostconditionUnevaluable as exc:
                # An honest refusal is a valid outcome; a broken declaration is
                # not. Both are recorded, only the latter fails.
                evaluated[name] = f"unevaluable: {exc}"
            except Exception as exc:  # noqa: BLE001 - the assertion IS the report
                pytest.fail(f"flag {name!r} is not evaluable: {exc}")
        assert set(evaluated) == set(POSTCONDITION_FLAGS)
