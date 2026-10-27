"""Migration 0046_attribute_copies_teardown (okg#3112 S3+S4, PACT
lane-s-storage-drop-copies).

Native PostgreSQL. A fresh install (which still runs 0013-0015 and 0020, then
0046) carries none of the three attribute copies, the three integrity receipt
tables or their functions, and the runtime schema ensure does not recreate
them. A database at the state before 0046 -- the copies and the receipt
tables re-created by the bodies of 0013-0015 and 0020 plus the snapshot and
interval DDL the old schema ensure ran, plus the recovery journal (0016-0017)
and the declared-index drift table and columns (0037), each seeded with a
row -- loses all of them when the 0046 body runs, keeps its overlay rows, and
the census raises
one WARNING per table. A second run changes nothing. Failure case: while
another session holds a lock on the interval table, 0046 stops on its 5 s lock
timeout and leaves every object in place. Through migrate(): from a 0044
ledger 0046 applies once; from a 0043 ledger (okg-workspace's path) 0044's
marker backfill reads the copies before 0046 drops them.
"""
from __future__ import annotations

import importlib.resources
from typing import Any

import pytest

psycopg = pytest.importorskip("psycopg")

pytestmark = pytest.mark.postgres

MIGRATION_ID = "0046_attribute_copies_teardown"
COPIES = (
    "okg.attribute_effective_view",
    "okg.attribute_effective_view_intervals",
    "okg.attribute_versions",
)
RECEIPTS = (
    "okg.attribute_effective_integrity_receipts",
    "okg.attribute_effective_integrity_transitions",
    "okg.attribute_effective_integrity_state",
)
# The storage of the two retired checks that read the copies.
RETIRED = (
    "okg.effective_attribute_recovery_attempts",
    "okg.declared_attr_index_drift_checks",
)
TABLES = (*COPIES, *RECEIPTS, *RETIRED)
DRIFT_COLUMNS = (
    ("declared_attr_indexes", "drift"),
    ("declared_attr_indexes", "drift_detected_at"),
    ("declared_attr_indexes", "drift_detail"),
    ("declared_attr_index_state", "drift_checked_through_ordinal"),
)
FUNCTIONS = (
    "begin_attribute_integrity_transition",
    "attribute_integrity_transition_matches",
    "finish_attribute_integrity_transition",
    "abort_attribute_integrity_transition",
    "register_attribute_integrity_scope",
    "mark_attribute_integrity_scope_dirty",
    "finalize_attribute_integrity_receipt",
    "mark_attribute_effective_integrity_dirty",
    "mark_attribute_effective_integrity_dirty_rows",
    "mark_attribute_effective_integrity_dirty_update",
    "reject_attribute_integrity_receipt_mutation",
)

# The snapshot and interval DDL the pre-0046 runtime schema ensure ran (the
# narrow shape 0020 converged to). 0020 itself authors okg.attribute_versions.
_PRE_0046_COPY_DDL = """
CREATE TABLE IF NOT EXISTS okg.attribute_effective_view (
  target_kind TEXT NOT NULL CHECK (target_kind IN ('node', 'edge')),
  target_id TEXT NOT NULL,
  generation_id TEXT NOT NULL,
  namespace TEXT NOT NULL DEFAULT 'default',
  subtype TEXT, src TEXT, dst TEXT, edge_type TEXT,
  overlay_count INT NOT NULL DEFAULT 0,
  raw_text_hash TEXT,
  materialized_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  attrs_hash BYTEA NOT NULL,
  PRIMARY KEY (target_kind, target_id, generation_id, namespace),
  CONSTRAINT attribute_effective_view_version_fkey
    FOREIGN KEY (target_kind, target_id, attrs_hash)
    REFERENCES okg.attribute_versions (target_kind, target_id, attrs_hash)
    ON DELETE RESTRICT
);
CREATE TABLE IF NOT EXISTS okg.attribute_effective_view_intervals (
  target_kind TEXT NOT NULL CHECK (target_kind IN ('node', 'edge')),
  target_id TEXT NOT NULL,
  namespace TEXT NOT NULL DEFAULT 'default',
  branch_id TEXT NOT NULL,
  from_ord BIGINT NOT NULL,
  to_ord BIGINT NOT NULL DEFAULT 9223372036854775807,
  subtype TEXT, src TEXT, dst TEXT, edge_type TEXT,
  overlay_count INT NOT NULL DEFAULT 0,
  raw_text_hash TEXT,
  materialized_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  attrs_hash BYTEA NOT NULL,
  CHECK (from_ord < to_ord),
  PRIMARY KEY (target_kind, target_id, namespace, branch_id, from_ord),
  CONSTRAINT attribute_effective_view_intervals_version_fkey
    FOREIGN KEY (target_kind, target_id, attrs_hash)
    REFERENCES okg.attribute_versions (target_kind, target_id, attrs_hash)
    ON DELETE RESTRICT
)
"""


# The drift-check storage 0037 authored, which 0046 drops.
_PRE_0046_DRIFT_DDL = """
ALTER TABLE okg.declared_attr_indexes
  ADD COLUMN IF NOT EXISTS drift BOOLEAN NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS drift_detected_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS drift_detail JSONB;
ALTER TABLE okg.declared_attr_index_state
  ADD COLUMN IF NOT EXISTS drift_checked_through_ordinal BIGINT;
CREATE TABLE IF NOT EXISTS okg.declared_attr_index_drift_checks (
  check_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  check_kind TEXT NOT NULL CHECK (check_kind IN ('dirty', 'slice', 'full')),
  branch_name TEXT NOT NULL,
  ordinal BIGINT NOT NULL,
  slice_index BIGINT,
  slice_count BIGINT,
  targets_checked BIGINT NOT NULL,
  differences BIGINT NOT NULL,
  by_attr JSONB NOT NULL DEFAULT '{}'::jsonb,
  sample JSONB NOT NULL DEFAULT '[]'::jsonb,
  checked_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""


def _migration(name: str) -> str:
    return (
        importlib.resources.files("okg.substrate.db")
        .joinpath(f"migrations/{name}.sql")
        .read_text()
    )


def _present(conn: Any) -> dict[str, bool]:
    out = {
        rel: conn.execute("SELECT to_regclass(%s) IS NOT NULL", (rel,)).fetchone()[0]
        for rel in TABLES
    }
    out["functions"] = bool(
        conn.execute(
            "SELECT count(*) FROM pg_proc p JOIN pg_namespace n "
            "ON n.oid = p.pronamespace WHERE n.nspname = 'okg' "
            "AND p.proname = ANY(%s)",
            (list(FUNCTIONS),),
        ).fetchone()[0]
    )
    out["drift_columns"] = bool(
        conn.execute(
            "SELECT count(*) FROM information_schema.columns "
            "WHERE table_schema = 'okg' "
            "AND (table_name, column_name) IN "
            "(SELECT * FROM unnest(%s::text[], %s::text[]))",
            ([t for t, _ in DRIFT_COLUMNS], [c for _, c in DRIFT_COLUMNS]),
        ).fetchone()[0]
    )
    return out


def _absent() -> dict[str, bool]:
    return {**{rel: False for rel in TABLES}, "functions": False, "drift_columns": False}


def _all_present() -> dict[str, bool]:
    return {**{rel: True for rel in TABLES}, "functions": True, "drift_columns": True}


def _run_in_transaction(conn: Any, body: str) -> list[str]:
    """Run a migration body the way the runner does (one transaction, so its
    SET LOCALs apply) and return the WARNINGs it raised."""
    warnings: list[str] = []

    def handler(diag: Any) -> None:
        if diag.severity_nonlocalized == "WARNING":
            warnings.append(diag.message_primary or "")

    conn.add_notice_handler(handler)
    try:
        with conn.transaction():
            conn.execute(body)
    finally:
        conn.remove_notice_handler(handler)
    return warnings


def _restore_pre_0046_state(conn: Any) -> None:
    """The copies and receipts as 0013-0015, 0020 and the old ensure left
    them, each seeded with a row, plus one overlay row that must survive."""
    _run_in_transaction(conn, _migration("0020_interval_narrow_split"))
    conn.execute(_PRE_0046_COPY_DDL)
    for name in (
        "0013_attribute_integrity_receipts",
        "0014_attribute_integrity_transition_token",
        "0015_attribute_integrity_authority_hardening",
    ):
        _run_in_transaction(conn, _migration(name))
    conn.execute(
        "INSERT INTO okg.attribute_versions (target_kind, target_id, attrs_hash, attrs) "
        "VALUES ('node', 'doc:1', '\\x01'::bytea, '{\"k\": 1}'::jsonb)"
    )
    conn.execute(
        "INSERT INTO okg.attribute_effective_view (target_kind, target_id, "
        "generation_id, attrs_hash) VALUES ('node', 'doc:1', 'okg:t:gen:1', '\\x01'::bytea)"
    )
    conn.execute(
        "INSERT INTO okg.attribute_effective_view_intervals (target_kind, target_id, "
        "branch_id, from_ord, attrs_hash) VALUES ('node', 'doc:1', 'default', 0, "
        "'\\x01'::bytea)"
    )
    conn.execute(
        "INSERT INTO okg.attribute_effective_integrity_state "
        "(deployment_name, branch_id, namespace) VALUES ('t', 'default', 'default')"
    )
    conn.execute(
        "INSERT INTO okg.attribute_effective_integrity_transitions "
        "(deployment_name, branch_id, namespace, backend_pid, backend_start) "
        "VALUES ('t', 'default', 'default', 1, now())"
    )
    # The two retired checks' storage: 0016-0017's journal and 0037's
    # drift-check table and drift columns (a fresh install now drops them, so
    # re-create the shape 0037 authored).
    for name in (
        "0016_effective_attribute_recovery_attempt",
        "0017_effective_attribute_recovery_consumption",
    ):
        _run_in_transaction(conn, _migration(name))
    conn.execute(_PRE_0046_DRIFT_DDL)
    conn.execute(
        "INSERT INTO okg.effective_attribute_recovery_attempts "
        "(classification_binding_digest, deployment_name, branch_name, "
        "predecessor_generation_id, expected_classification, stage) "
        "VALUES ('sha256:x', 't', 'default', 'okg:t:gen:1', '{}'::jsonb, 'classified')"
    )
    conn.execute(
        "INSERT INTO okg.declared_attr_index_drift_checks "
        "(check_kind, branch_name, ordinal, targets_checked, differences) "
        "VALUES ('full', 'default', 0, 1, 0)"
    )
    conn.execute(
        "INSERT INTO okg.attribute_effective_attrs (target_kind, target_id, "
        "generation_id, namespace, attr_key, observation_id, value, value_hash, "
        "producer) VALUES ('node', 'doc:1', 'okg:t:gen:1', 'default', 'k', 'obs:1', "
        "'1'::jsonb, 'h', 'test')"
    )
    for rel in TABLES:
        conn.execute(f"ANALYZE {rel}")


def test_a_fresh_install_has_none_of_the_objects_and_records_0046(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _present(conn) == _absent()
        recorded = conn.execute(
            "SELECT count(*) FROM okg.schema_migrations WHERE migration_id = %s",
            (MIGRATION_ID,),
        ).fetchone()[0]
    assert recorded == 1


def test_the_schema_ensure_does_not_recreate_the_copies(clean_runtime: str) -> None:
    from okg.substrate.attributes import ensure_attribute_schema

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ensure_attribute_schema(conn)
        assert _present(conn) == _absent()
        for kept in (
            "okg.attribute_effective_attrs",
            "okg.attribute_overlay_generations",
            "okg.attribute_overlay_ranges",
        ):
            assert conn.execute(
                "SELECT to_regclass(%s) IS NOT NULL", (kept,)
            ).fetchone()[0], kept


def test_0046_drops_seeded_copies_and_receipts_and_keeps_the_overlays(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _restore_pre_0046_state(conn)
        assert _present(conn) == _all_present()
        for rel in (*COPIES, RECEIPTS[1], RECEIPTS[2], *RETIRED):
            assert conn.execute(f"SELECT count(*) FROM {rel}").fetchone()[0] == 1, rel

        warnings = _run_in_transaction(conn, _migration(MIGRATION_ID))

        assert _present(conn) == _absent()
        assert conn.execute(
            "SELECT count(*) FROM okg.attribute_effective_attrs "
            "WHERE generation_id = 'okg:t:gen:1'"
        ).fetchone()[0] == 1
        census = [w for w in warnings if "attribute copies teardown drops" in w]
        assert len(census) == len(TABLES), warnings
        for rel in TABLES:
            (line,) = [w for w in census if f"drops {rel}:" in w]
            assert "reltuples estimate" in line and "total bytes" in line, line
        for rel in (*COPIES, RECEIPTS[1], RECEIPTS[2], *RETIRED):
            (line,) = [w for w in census if f"drops {rel}:" in w]
            assert line.endswith("holds rows true"), line

        # A second run is a no-op: nothing to drop, each table named absent.
        again = _run_in_transaction(conn, _migration(MIGRATION_ID))
        assert sorted(again) == sorted(
            f"attribute copies teardown: {rel} is absent; nothing to drop" for rel in TABLES
        )
        assert _present(conn) == _absent()


def test_0046_stops_on_its_lock_timeout_and_changes_nothing(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _restore_pre_0046_state(conn)
    holder = psycopg.connect(clean_runtime)
    try:
        # An old reader: a transaction that has read the interval front.
        holder.execute(
            "SELECT count(*) FROM okg.attribute_effective_view_intervals"
        ).fetchone()
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            with pytest.raises(psycopg.errors.LockNotAvailable):
                _run_in_transaction(conn, _migration(MIGRATION_ID))
            assert _present(conn) == _all_present()
    finally:
        holder.rollback()
        holder.close()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _run_in_transaction(conn, _migration(MIGRATION_ID))
        assert _present(conn) == _absent()


# The checksums the pre-0046 release recorded for 0013 and 0015, before this
# change's audited edit to their trigger loops (management.py registers them).
_PRE_0046_LEDGER_CHECKSUMS = {
    "0013_attribute_integrity_receipts": (
        "bce81a578940dafa440c305dd7393f6531d4e0afcb6b0673e16b9a9172fc4f80"
    ),
    "0015_attribute_integrity_authority_hardening": (
        "8c473680bd28f1bfb00fd51b11b9eaa0046b2383e598463ad7d2e3fa9b8acfb1"
    ),
}


def test_migrate_on_a_database_at_0044_applies_0046_through_the_runner(
    clean_runtime: str,
) -> None:
    """The upgrade path through the real ordered runner, not the bare body.

    A database the pre-0046 release left: every object 0046 drops, seeded
    with rows, a ledger without 0046, and the old 0013/0015 checksums.
    ``migrate()`` reports 0046 as the only pending migration and the two old
    checksums as audited compatibilities (not mismatches), applies 0046,
    keeps the overlay rows, and a second ``migrate()`` changes nothing.
    """
    from okg.substrate.db.management import migration_status
    from okg.substrate.db.migrate import migrate

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _restore_pre_0046_state(conn)
        conn.execute(
            "DELETE FROM okg.schema_migrations WHERE migration_id = %s",
            (MIGRATION_ID,),
        )
        for migration_id, checksum in _PRE_0046_LEDGER_CHECKSUMS.items():
            conn.execute(
                "UPDATE okg.schema_migrations SET checksum = %s "
                "WHERE migration_id = %s",
                (checksum, migration_id),
            )
        assert _present(conn) == _all_present()

    before = migration_status(clean_runtime)
    assert [row["migration_id"] for row in before["pending"]] == [MIGRATION_ID]
    assert before["checksum_mismatches"] == []
    assert {
        row["migration_id"] for row in before["checksum_compatibilities"]
    } == set(_PRE_0046_LEDGER_CHECKSUMS)

    migrate(clean_runtime)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _present(conn) == _absent()
        assert conn.execute(
            "SELECT count(*) FROM okg.attribute_effective_attrs "
            "WHERE generation_id = 'okg:t:gen:1'"
        ).fetchone()[0] == 1
        (applied_at,) = conn.execute(
            "SELECT applied_at FROM okg.schema_migrations WHERE migration_id = %s",
            (MIGRATION_ID,),
        ).fetchone()
    after = migration_status(clean_runtime)
    assert after["pending"] == []
    assert after["checksum_mismatches"] == []

    migrate(clean_runtime)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _present(conn) == _absent()
        assert conn.execute(
            "SELECT applied_at FROM okg.schema_migrations WHERE migration_id = %s",
            (MIGRATION_ID,),
        ).fetchone() == (applied_at,), "a second migrate does not re-apply 0046"


_MARKER_TABLE = "okg.attribute_overlay_generations"
_RANGES_TABLE = "okg.attribute_overlay_ranges"
_BACKFILL_MIGRATION_ID = "0044_attribute_overlay_generations"
# The overlay-proved generation _restore_pre_0046_state seeds (an overlay
# row, a snapshot row), one proved only by an interval row stabbed at its
# ordinal, and one ordinal below the interval with no proof at all.
_OVERLAY_PROVED = "okg:t:gen:1"
_INTERVAL_PROVED = "okg:t:gen:interval"
_UNPROVED = "okg:t:gen:unproved"


def _markers(conn: Any) -> dict[str, tuple]:
    return {
        row[0]: tuple(row[1:])
        for row in conn.execute(
            f"SELECT generation_id, overlay_rows, chronos_branch_id, ordinal, "
            f"source FROM {_MARKER_TABLE} WHERE namespace = 'default'"
        ).fetchall()
    }


def test_an_upgrade_from_0043_backfills_markers_then_0046_keeps_them(
    clean_runtime: str,
) -> None:
    """okg-workspace's upgrade path, 0043 -> 0044 -> 0046, through migrate().

    A database as 0043 left it: the copies with one generation proved by its
    overlay and snapshot rows, one proved only by an interval row whose range
    holds its ordinal, an ordinal on the same branch below every interval
    with no proof, an empty marker table (the schema body creates it before
    the ordered migrations run), and a ledger without 0044 and 0046 that has
    the old 0013/0015 checksums. migrate() applies 0044, whose backfill marks
    both proved generations with their branch and ordinal and not the
    unproved one (failure case: a backfill that marked every ordinal of a
    branch with interval rows, or ignored the interval range, would mark
    it), then applies 0046, which drops the copies and leaves the markers.
    """
    from okg.substrate.db.management import migration_status
    from okg.substrate.db.migrate import migrate

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _restore_pre_0046_state(conn)
        # The seeded interval row now starts at ordinal 5: it proves the
        # generations at 5 and 7 and not the one at 3.
        conn.execute("UPDATE okg.attribute_effective_view_intervals SET from_ord = 5")
        for checkpoint, ordinal in (
            (_UNPROVED, 3), (_INTERVAL_PROVED, 5), (_OVERLAY_PROVED, 7),
        ):
            conn.execute(
                "INSERT INTO okg.generation_ordinals (checkpoint_id, branch_id, "
                "ordinal, previous_checkpoint_id, deployment_name) "
                "VALUES (%s, 'default', %s, NULL, 'default')",
                (checkpoint, ordinal),
            )
        conn.execute(f"TRUNCATE {_MARKER_TABLE}, {_RANGES_TABLE}")
        conn.execute(
            "DELETE FROM okg.schema_migrations WHERE migration_id = ANY(%s)",
            ([_BACKFILL_MIGRATION_ID, MIGRATION_ID],),
        )
        for migration_id, checksum in _PRE_0046_LEDGER_CHECKSUMS.items():
            conn.execute(
                "UPDATE okg.schema_migrations SET checksum = %s "
                "WHERE migration_id = %s",
                (checksum, migration_id),
            )
        assert _markers(conn) == {}

    before = migration_status(clean_runtime)
    assert [row["migration_id"] for row in before["pending"]] == [
        _BACKFILL_MIGRATION_ID, MIGRATION_ID,
    ]
    assert before["checksum_mismatches"] == []

    migrate(clean_runtime)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        markers = _markers(conn)
        assert markers == {
            _OVERLAY_PROVED: (1, "default", 7, "backfill"),
            _INTERVAL_PROVED: (0, "default", 5, "backfill"),
        }, markers
        assert _UNPROVED not in markers
        # 0046 ran after 0044 in the same migrate: the copies are gone and
        # the markers the backfill wrote from them survive.
        assert _present(conn) == _absent()
        recorded = {
            row[0] for row in conn.execute(
                "SELECT migration_id FROM okg.schema_migrations "
                "WHERE migration_id = ANY(%s)",
                ([_BACKFILL_MIGRATION_ID, MIGRATION_ID],),
            ).fetchall()
        }
        assert recorded == {_BACKFILL_MIGRATION_ID, MIGRATION_ID}
    assert migration_status(clean_runtime)["pending"] == []


def test_clone_still_purges_the_dropped_tables() -> None:
    """A source database without 0046 still has the copies; the clone
    allowlist fails closed on an unlisted okg table, so their names stay as
    purge-if-present. Failure case: an unlisted name is ``unknown``."""
    from okg.substrate.environment_clone import clone_table_disposition

    for rel in TABLES:
        assert clone_table_disposition("okg", rel.split(".", 1)[1]) == "purge", rel
    assert clone_table_disposition("okg", "attribute_not_a_real_copy") == "unknown"
