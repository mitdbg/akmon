"""migrations/manifest.yaml is consulted at apply time.

Covers req.schema-migration-hardening.manifest-authority (audit F4/F6):

  1. Typed refusals BEFORE any execution: a pending ordered migration with no
     manifest entry, a file checksum that disagrees with the manifest's
     ``checksum_sha256``, or a ``destructive`` flag that disagrees with the
     filename heuristic.
  2. Destructive classification comes FROM the manifest, not the filename.
  3. Applied-but-unknown ledger rows (the retired ``0001`` is live evidence)
     are surfaced as ``unknown_applied`` in migration_status and as a doctor
     finding instead of being silently ignored.
  4. The real tree is green: the shipped manifest matches the shipped files,
     so no false refusals on a converged database.
"""
from __future__ import annotations

from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from okg.substrate.db import management
from okg.substrate.db.management import (
    KnownMigration,
    current_known_migrations,
    database_health_findings,
    inspect_database,
    load_migrations_manifest,
    migration_status,
    run_pending_ordered_migrations,
    validate_pending_against_manifest,
)
from okg.substrate.db.migrate import migrate
from okg.substrate.errors import (
    MigrationManifestChecksumMismatch,
    MigrationManifestDestructiveDisagreement,
    MigrationManifestEntryMissing,
    MigrationManifestPrerequisiteUnmet,
)

RECEIPT_ID = "0018_promotion_fence_release_receipt"


# --------------------------------------------------------------------------
# Helpers.
# --------------------------------------------------------------------------


def _schema_text() -> str:
    import importlib.resources

    return (
        importlib.resources.files("okg.substrate.db")
        .joinpath("schema.sql")
        .read_text()
    )


def _views_text() -> str:
    import importlib.resources

    return (
        importlib.resources.files("okg.substrate.db")
        .joinpath("views.sql")
        .read_text()
    )


def _create_disposable_database(base_dsn: str, dbname: str) -> str:
    info = conninfo_to_dict(base_dsn)
    info["dbname"] = "postgres"
    maintenance_dsn = make_conninfo(**info)
    with psycopg.connect(
        maintenance_dsn, autocommit=True, connect_timeout=5,
    ) as conn, conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
    info["dbname"] = dbname
    dsn = make_conninfo(**info)
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


def _mark_pending(dsn: str, migration_id: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "DELETE FROM okg.schema_migrations WHERE migration_id = %s",
            (migration_id,),
        )


# --------------------------------------------------------------------------
# 1. Hermetic refusal coverage (no DB): each refusal class, by type.
# --------------------------------------------------------------------------


def _synthetic(migration_id: str, checksum: str = "abc123") -> KnownMigration:
    return KnownMigration(
        migration_id=migration_id,
        description=f"ordered migration {migration_id}",
        checksum=checksum,
        destructive=False,
    )


def _ddl_loader(_migration_id: str) -> str:
    """Forward-SQL loader for synthetic pending migrations (no file on
    disk): plain DDL, so the data-motion classifier stays quiet and the
    check under test is the one that fires."""
    return "CREATE TABLE IF NOT EXISTS okg_widget (id BIGINT PRIMARY KEY)"


def test_missing_manifest_entry_is_typed_refusal() -> None:
    with pytest.raises(MigrationManifestEntryMissing) as excinfo:
        validate_pending_against_manifest(
            [_synthetic("0099_unlisted_migration")], manifest={},
        )
    assert excinfo.value.data["migration_id"] == "0099_unlisted_migration"


def test_manifest_checksum_mismatch_is_typed_refusal() -> None:
    manifest = {
        "0099_widget": {
            "migration_id": "0099_widget",
            "checksum_sha256": "not-the-file-checksum",
            "destructive": False,
        }
    }
    with pytest.raises(MigrationManifestChecksumMismatch) as excinfo:
        validate_pending_against_manifest(
            [_synthetic("0099_widget", checksum="abc123")], manifest=manifest,
        )
    assert excinfo.value.data["file_checksum"] == "abc123"
    assert excinfo.value.data["manifest_checksum"] == "not-the-file-checksum"


def test_destructive_flag_disagreement_is_typed_refusal() -> None:
    # Manifest says destructive, but the slug carries no destructive prefix.
    manifest = {
        "0099_widget": {
            "migration_id": "0099_widget",
            "checksum_sha256": "abc123",
            "destructive": True,
        }
    }
    with pytest.raises(MigrationManifestDestructiveDisagreement):
        validate_pending_against_manifest(
            [_synthetic("0099_widget", checksum="abc123")], manifest=manifest,
        )
    # And the inverse: heuristically-destructive slug, manifest says not.
    manifest = {
        "0099_drop_widget": {
            "migration_id": "0099_drop_widget",
            "checksum_sha256": "abc123",
            "destructive": False,
        }
    }
    with pytest.raises(MigrationManifestDestructiveDisagreement):
        validate_pending_against_manifest(
            [_synthetic("0099_drop_widget", checksum="abc123")],
            manifest=manifest,
        )


def test_unmet_declared_prerequisite_is_typed_refusal() -> None:
    """The machine-checkable prerequisite form (``migration <id> is
    applied``) refuses when the named migration is neither applied nor
    scheduled earlier in the batch; prose prerequisites stay informational."""
    entry = {
        "migration_id": "0091_widget",
        "checksum_sha256": "abc123",
        "destructive": False,
        "prerequisites": [
            "the okg schema exists",  # prose: never evaluated
            "migration 0090_base is applied",
        ],
    }
    manifest = {"0091_widget": entry}
    pending = [_synthetic("0091_widget", checksum="abc123")]

    # Neither applied nor in the batch: typed refusal.
    with pytest.raises(MigrationManifestPrerequisiteUnmet) as excinfo:
        validate_pending_against_manifest(
            pending, manifest=manifest, applied_ids=set(),
            forward_sql_loader=_ddl_loader,
        )
    assert excinfo.value.data["prerequisite_id"] == "0090_base"

    # Applied in the ledger: satisfied.
    validate_pending_against_manifest(
        pending, manifest=manifest, applied_ids={"0090_base"},
        forward_sql_loader=_ddl_loader,
    )

    # Scheduled earlier in the SAME batch (non-destructive): satisfied.
    base_entry = {
        "migration_id": "0090_base",
        "checksum_sha256": "def456",
        "destructive": False,
    }
    both = {"0090_base": base_entry, "0091_widget": entry}
    validate_pending_against_manifest(
        [_synthetic("0090_base", checksum="def456"),
         _synthetic("0091_widget", checksum="abc123")],
        manifest=both,
        applied_ids=set(),
        forward_sql_loader=_ddl_loader,
    )

    # Scheduled earlier but DESTRUCTIVE (the runner will skip it, so it
    # cannot satisfy the prerequisite): typed refusal.
    destructive_base = {
        "migration_id": "0090_drop_base",
        "checksum_sha256": "def456",
        "destructive": True,
    }
    needs_destructive = dict(entry)
    needs_destructive["prerequisites"] = [
        "migration 0090_drop_base is applied",
    ]
    with pytest.raises(MigrationManifestPrerequisiteUnmet):
        validate_pending_against_manifest(
            [_synthetic("0090_drop_base", checksum="def456"),
             _synthetic("0091_widget", checksum="abc123")],
            manifest={
                "0090_drop_base": destructive_base,
                "0091_widget": needs_destructive,
            },
            applied_ids=set(),
            forward_sql_loader=_ddl_loader,
        )


@pytest.mark.postgres
def test_runner_refuses_unmet_prerequisite(
    okg_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dbname = f"okg_test_manifest_pr_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        migrate(dsn)
        _mark_pending(dsn, RECEIPT_ID)

        ghosted = {
            key: dict(value)
            for key, value in load_migrations_manifest().items()
        }
        ghosted[RECEIPT_ID]["prerequisites"] = [
            "migration 0099_ghost is applied",
        ]
        monkeypatch.setattr(
            management, "load_migrations_manifest", lambda: ghosted,
        )
        with psycopg.connect(dsn) as conn:
            with pytest.raises(MigrationManifestPrerequisiteUnmet):
                run_pending_ordered_migrations(
                    conn,
                    schema_sql=_schema_text(),
                    views_sql=_views_text(),
                )
    finally:
        _drop_disposable_database(okg_dsn, dbname)


def test_shipped_tree_is_green_no_false_refusals() -> None:
    """The shipped manifest matches every shipped ordered file — the shipped
    tree must never refuse itself. This also pins that new migrations land
    WITH their manifest entry."""
    ordered = [
        migration
        for migration in current_known_migrations()
        if migration.migration_id != "0000_schema_snapshot"
    ]
    assert ordered, "expected shipped ordered migrations"
    validate_pending_against_manifest(ordered)  # must not raise


def test_destructive_classification_comes_from_manifest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Flipping ONLY the manifest flag flips KnownMigration.destructive —
    proving the classification source is the manifest, not the filename."""
    real = load_migrations_manifest()
    assert real[RECEIPT_ID]["destructive"] is False
    baseline = {
        m.migration_id: m.destructive for m in current_known_migrations()
    }
    assert baseline[RECEIPT_ID] is False

    flipped = {
        key: dict(value) for key, value in real.items()
    }
    flipped[RECEIPT_ID]["destructive"] = True
    monkeypatch.setattr(
        management, "load_migrations_manifest", lambda: flipped,
    )
    reclassified = {
        m.migration_id: m.destructive for m in current_known_migrations()
    }
    assert reclassified[RECEIPT_ID] is True, (
        "destructive classification did not come from the manifest"
    )


# --------------------------------------------------------------------------
# 2. Apply-time enforcement on a real database.
# --------------------------------------------------------------------------


@pytest.mark.postgres
def test_runner_refuses_pending_migration_without_manifest_entry(
    okg_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dbname = f"okg_test_manifest_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        migrate(dsn)
        _mark_pending(dsn, RECEIPT_ID)

        pruned = {
            key: dict(value)
            for key, value in load_migrations_manifest().items()
            if key != RECEIPT_ID
        }
        monkeypatch.setattr(
            management, "load_migrations_manifest", lambda: pruned,
        )
        with psycopg.connect(dsn) as conn:
            with pytest.raises(MigrationManifestEntryMissing):
                run_pending_ordered_migrations(
                    conn,
                    schema_sql=_schema_text(),
                    views_sql=_views_text(),
                )
        # Refused BEFORE execution: still pending, no ledger row.
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.schema_migrations "
                "WHERE migration_id = %s",
                (RECEIPT_ID,),
            )
            assert cur.fetchone()[0] == 0
    finally:
        _drop_disposable_database(okg_dsn, dbname)


@pytest.mark.postgres
def test_runner_refuses_manifest_checksum_mismatch(
    okg_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dbname = f"okg_test_manifest_ck_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        migrate(dsn)
        _mark_pending(dsn, RECEIPT_ID)

        tampered = {
            key: dict(value)
            for key, value in load_migrations_manifest().items()
        }
        tampered[RECEIPT_ID]["checksum_sha256"] = "0" * 64
        monkeypatch.setattr(
            management, "load_migrations_manifest", lambda: tampered,
        )
        with psycopg.connect(dsn) as conn:
            with pytest.raises(MigrationManifestChecksumMismatch):
                run_pending_ordered_migrations(
                    conn,
                    schema_sql=_schema_text(),
                    views_sql=_views_text(),
                )
    finally:
        _drop_disposable_database(okg_dsn, dbname)


@pytest.mark.postgres
def test_runner_refuses_destructive_flag_disagreement(
    okg_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dbname = f"okg_test_manifest_dx_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        migrate(dsn)
        _mark_pending(dsn, RECEIPT_ID)

        flipped = {
            key: dict(value)
            for key, value in load_migrations_manifest().items()
        }
        flipped[RECEIPT_ID]["destructive"] = True
        monkeypatch.setattr(
            management, "load_migrations_manifest", lambda: flipped,
        )
        with psycopg.connect(dsn) as conn:
            with pytest.raises(MigrationManifestDestructiveDisagreement):
                run_pending_ordered_migrations(
                    conn,
                    schema_sql=_schema_text(),
                    views_sql=_views_text(),
                )
        # In particular it was NOT silently routed to skipped_destructive
        # (which pre-fix behaviour would have done had the classification
        # been trusted without the cross-check): still pending, unrecorded.
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.schema_migrations "
                "WHERE migration_id = %s",
                (RECEIPT_ID,),
            )
            assert cur.fetchone()[0] == 0
    finally:
        _drop_disposable_database(okg_dsn, dbname)


@pytest.mark.postgres
def test_converged_real_tree_applies_cleanly_with_manifest_enforcement(
    okg_dsn: str,
) -> None:
    """No false refusals: a full migrate on the real tree (which now runs the
    manifest validation on every pending file) converges and a second migrate
    is a clean no-op."""
    dbname = f"okg_test_manifest_ok_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        migrate(dsn)
        migrate(dsn)
        status = migration_status(dsn)
        assert status["ok"] is True
        assert status["pending"] == []
    finally:
        _drop_disposable_database(okg_dsn, dbname)


# --------------------------------------------------------------------------
# 3. Applied-but-unknown ledger rows are surfaced, not ignored.
# --------------------------------------------------------------------------


@pytest.mark.postgres
def test_unknown_applied_rows_surface_in_status_and_doctor(
    okg_dsn: str,
) -> None:
    dbname = f"okg_test_unknown_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        migrate(dsn)
        # Synthetic retired ledger row: the file for it no longer ships and
        # the manifest never listed it (the deleted 0001 incident shape).
        with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.schema_migrations
                  (migration_id, checksum, description, destructive)
                VALUES ('0001_retired_migration', 'deadbeef',
                        'synthetic retired row', false)
                """
            )

        status = migration_status(dsn)
        unknown_ids = [
            row["migration_id"] for row in status["unknown_applied"]
        ]
        assert unknown_ids == ["0001_retired_migration"]
        # Surfacing, not blocking: ok stays checksum-driven.
        assert status["ok"] is True

        # Known legacy manifest history (e.g. retire_legacy_runtime) must NOT
        # be flagged: "known" = files UNION manifest entries.
        assert "retire_legacy_runtime" in load_migrations_manifest()

        # Doctor path: inspect_database -> database_health_findings surfaces
        # the same row as a warning finding.
        inspected = inspect_database(dsn)
        assert [
            row["migration_id"]
            for row in inspected["migration_status"]["unknown_applied"]
        ] == ["0001_retired_migration"]
        findings = database_health_findings(inspected)
        unknown_findings = [
            finding
            for finding in findings
            if finding["code"] == "schema_migration_unknown_applied"
        ]
        assert len(unknown_findings) == 1
        assert unknown_findings[0]["severity"] == "warning"
        assert unknown_findings[0]["observed"] == ["0001_retired_migration"]
    finally:
        _drop_disposable_database(okg_dsn, dbname)


@pytest.mark.postgres
def test_manifest_listed_legacy_rows_are_not_flagged_unknown(
    okg_dsn: str,
) -> None:
    """A ledger row whose migration ships no NNNN file but IS in the manifest
    history (retire_legacy_runtime) is known — not noise in unknown_applied."""
    dbname = f"okg_test_legacyrow_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        migrate(dsn)
        with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.schema_migrations
                  (migration_id, checksum, description, destructive)
                VALUES ('retire_legacy_runtime', 'cafebabe',
                        'legacy operator migration', true)
                """
            )
        status = migration_status(dsn)
        assert status["unknown_applied"] == []
    finally:
        _drop_disposable_database(okg_dsn, dbname)
