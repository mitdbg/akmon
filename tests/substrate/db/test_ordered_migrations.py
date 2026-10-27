"""Ordered, irreversible cleanup migrations + reconcile-on-change.

Covers task.proof.ordered-migrations-and-rollback of the
``deployment-publish-proof`` PACT (req.proof.ordered-migrations):

  1. Byte-identical proof — ``current_known_migrations()`` still returns the
     exact (migration_id, checksum, destructive) tuples captured before this
     task, so no schema drift was smuggled in.
  2. ``migrate-down`` / ``migrate_down`` refuses schema snapshots and cleanup
     loudly; deleted job runtimes cannot be structurally resurrected.
  3. Reconcile-on-change — a no-op catalog apply (unchanged fingerprint) does
     NOT stamp a new ``okg.catalog_versions`` row.
"""
from __future__ import annotations

import hashlib
import importlib.resources
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg import sql

from okg.substrate.db.management import (
    REMOVE_JOB_RUNTIME_SUBSYSTEM_MIGRATION_ID,
    SCHEMA_SNAPSHOT_MIGRATION_ID,
    current_known_migrations,
    migrate_down,
    schema_snapshot_checksum,
)
from okg.substrate.db.migrate import migrate
from okg.substrate.errors import SubstrateError


# The removal migration is static and its checksum must never drift. The
# ``0000_schema_snapshot`` checksum, by contrast, is a rolling hash of schema +
# ``views.sql`` that changes on EVERY legitimate schema/views edit — pinning its
# literal value only churns (it has been re-pinned mechanically several times,
# and a reviewer cannot meaningfully audit the opaque hash), so it is derived
# from the packaged DDL rather than pinned. See deployment-publish-proof
# req.proof.ordered-migrations.
REMOVE_JOB_RUNTIME_SUBSYSTEM_BASELINE_CHECKSUM = (
    "d9eb400b8ca520f4eedd878ddb14e3e208a2cb33ef562a5cc441a8b92e032b40"
)
DEPLOYMENT_PROMOTION_FENCE_BASELINE_CHECKSUM = (
    "2189be32b689869225a5b75b23ae8f99cf733b79d5ec250f518edfb3817fcd86"
)
DEPLOYMENT_PROMOTION_FENCE_LEGACY_CHECKSUM = (
    "62d3e8b167509386144464ddf5270f4b43765211bb2f1bd77346b375e5f823bc"
)


# --------------------------------------------------------------------------
# 1. Byte-identical proof (hermetic — no DB).
# --------------------------------------------------------------------------


def test_current_known_migrations_are_byte_identical() -> None:
    """Pin the exact ordered-migration set, in order, with destructive flags.

    Merge-train note: this list and the matching file set in
    ``tests/ci/test_schema_migration_manifest.py`` GROW BY TWO when the PRs
    holding ordinals 0021 and 0022 land ahead of 0023. Resolve that conflict by
    TAKING BOTH SIDES — every entry from each branch, in ordinal order. Never
    renumber an ordinal that has already been applied to a database: the runner
    refuses a migration whose recorded checksum no longer matches, and a
    renumbered file reads to it as an edited applied migration. Renumbering is
    only safe while the migration is unmerged and unapplied, which is why this
    one moved 0022 -> 0023 when PR #1074 took 0019 on ``dev``. PR #1055 has
    since taken 0020, which cost this branch nothing further: 0023 was already
    clear of it.
    """
    migrations = current_known_migrations()
    by_id = {m.migration_id: m for m in migrations}

    # The durable contract: exactly these known migrations, in this order,
    # with these destructive flags (catches a smuggled added/removed/reclassified
    # migration).
    assert [(m.migration_id, m.destructive) for m in migrations] == [
        (SCHEMA_SNAPSHOT_MIGRATION_ID, False),
        (REMOVE_JOB_RUNTIME_SUBSYSTEM_MIGRATION_ID, True),
        ("0003_deployment_promotion_fence", False),
        ("0004_effective_view_raw_attrs_nullable", False),
        ("0005_chronos_hot_index_manifest", False),
        ("0006_chronos_history_compaction", False),
        ("0007_chronos_history_read_coordination", False),
        ("0008_history_read_lease_role_api", False),
        ("0009_compacted_generation_read_api", False),
        ("0010_compacted_generation_safe_numeric", False),
        ("0011_compacted_generation_deployment_scope", False),
        ("0012_history_read_lease_scope", False),
        ("0013_attribute_integrity_receipts", False),
        ("0014_attribute_integrity_transition_token", False),
        ("0015_attribute_integrity_authority_hardening", False),
        ("0016_effective_attribute_recovery_attempt", False),
        ("0017_effective_attribute_recovery_consumption", False),
        ("0018_promotion_fence_release_receipt", False),
        ("0019_runtime_dlq_acknowledgement_events", False),
        ("0020_interval_narrow_split", False),
        ("0021_attr_filter_index", False),
        ("0022_pact_ledger_schema_qualification", False),
        # 0023 DROPs okg.admission_envelopes.target_generation_id but is NOT
        # flagged destructive: the runner derives that flag from the filename
        # heuristic and auto-SKIPS a destructive migration, which would leave
        # upgraded databases permanently diverged from a fresh install and red
        # the migration-convergence lane. The manifest entry carries the full
        # argument and the ban-drop-column squawk acknowledgement.
        ("0023_generation_identity_text_refs", False),
        # 0024 DROPs five retired views and seven functions, and is likewise
        # NOT flagged destructive, for the same runner reason. Nothing it drops
        # holds a fact: the views projected rows that remain readable from the
        # relations they read.
        ("0024_canonical_read_path_view_cleanup", False),
        # 0025 DROPs okg.pagerank_scores, okg.verifier_runs, and the two
        # PageRank functions but is NOT flagged destructive, for the same reason
        # as 0023: the runner auto-SKIPS a destructive migration, which would
        # leave upgraded databases carrying relations no fresh install has and
        # red the migration-convergence lane permanently. The manifest entry
        # carries the full argument and the ban-drop-table acknowledgement.
        #
        # Both ordinals were reserved by different in-flight lanes; the merge
        # UNIONS them in ordinal order. Never renumber one that a database has
        # already applied.
        ("0025_dead_relation_and_constraint_convergence", False),
        # 0026 only RENAMEs columns, so it is non-destructive in the runner's
        # sense (no relation or column is dropped; ALTER TABLE ... RENAME
        # COLUMN carries dependent indexes, constraints and view bodies with
        # it).
        ("0026_identity_branch_and_deployment_names", False),
        # 0027 only CREATEs (okg.grant_registry, okg.pending_grants), so it is
        # non-destructive and, being IF NOT EXISTS against relations schema.sql
        # already authors, a no-op on a fresh install.
        ("0027_grant_registry", False),
        ("0028_enricher_input_quarantine", False),
        ("0029_workflow_operational_outcomes", False),
        ("0030_external_action_review", False),
        # 0031 only CREATEs (ten okg.source_record_authority* relations, two
        # indexes, three trigger functions) and re-creates its own triggers with
        # DROP TRIGGER IF EXISTS. No relation or column is dropped, so it is
        # non-destructive in the runner's sense, and being IF NOT EXISTS against
        # relations schema.sql already authors it is a no-op on a fresh install.
        # Recounted from the tracked files on disk after this ordinal landed,
        # not carried across from either side of a merge.
        ("0031_bounded_record_authority", False),
        # 0032 widens ONE check constraint
        # (okg.enricher_progress.last_status accepts a `withheld:%` value
        # alongside `never`/`success`/`failed:%`) by dropping and re-adding it
        # under the same name. Nothing is dropped that is not immediately
        # restored, no row can violate the wider predicate, and no relation or
        # column is touched -- non-destructive in the runner's sense.
        # Recounted from the tracked files on disk after this ordinal landed.
        ("0032_withheld_enricher_progress_status", False),
        ("0033_external_automation_run_identity", False),
        # 0034 only CREATE OR REPLACEs two existing functions (issue #1925).
        ("0034_external_action_claim_expiry", False),
        # 0035 adds three nullable columns, two constraints and one table
        # for pieced record-set authority (issue #3008); nothing is dropped.
        ("0035_record_set_blob_pieces", False),
        # 0036 only CREATEs three tables, grants one, and queues rebuild
        # requests (issue #3029).
        ("0036_attr_index_coverage", False),
        # 0037 only CREATEs three functions and three tables and grants one
        # (okg#3112 step 2); it builds no index on a graph table.
        ("0037_declared_attr_indexes", False),
        # 0039 only CREATE OR REPLACEs the snapshot function (issue #2341).
        # 0038 is reserved by another lane.
        ("0039_external_action_snapshot_columns", False),
        # 0040 only CREATEs the Nomos read-class catalog table and grants it
        # (PACT lane-h-nomos-read-class-sql).
        ("0040_nomos_read_classes", False),
        # 0041 only CREATEs okg.blobs, its cleanup index and the image-only
        # view okg.image_blobs, and grants app_rw (issue #3046).
        ("0041_blob_store", False),
        # 0043 DROPs okg.attr_index, its three issue-#3029 side tables and
        # okg.attr_index_try_ts (okg#3112 step 4b). Not destructive by
        # decision 304 (D4): the rows are derived from interval rows that
        # stay, and a skipped drop would keep upgraded databases apart from
        # fresh installs. 0042 is reserved by another lane.
        ("0043_attr_index_teardown", False),
        # 0044 only CREATEs the overlay marker and ranges tables and backfills
        # markers (PACT lane-s-storage-redesign-s2).
        ("0044_attribute_overlay_generations", False),
        # 0045 only COMMENTs on the operational-outcome table and is the
        # publisher's outcome cutover (PACT publisher-starvation-4039).
        ("0045_publisher_operational_outcomes", False),
        # 0046 DROPs the three attribute copies and the integrity receipt
        # tables (PACT lane-s-storage-drop-copies). Not destructive by
        # decision 304 (D4), as 0043: derived rows, RESTRICT drops.
        ("0046_attribute_copies_teardown", False),
    ]

    # Anti-tamper on the shipped static destructive migration file.
    assert (
        by_id[REMOVE_JOB_RUNTIME_SUBSYSTEM_MIGRATION_ID].checksum
        == REMOVE_JOB_RUNTIME_SUBSYSTEM_BASELINE_CHECKSUM
    )
    assert (
        by_id["0003_deployment_promotion_fence"].checksum
        == DEPLOYMENT_PROMOTION_FENCE_BASELINE_CHECKSUM
    )

    # The rolling snapshot checksum is NOT pinned (it changes on every schema
    # edit); instead independently recompute it from the packaged DDL and assert
    # the migration's checksum is correctly wired to it.
    schema_sql = (
        importlib.resources.files("okg.substrate.db")
        .joinpath("schema.sql")
        .read_text()
    )
    views_sql = (
        importlib.resources.files("okg.substrate.db")
        .joinpath("views.sql")
        .read_text()
    )
    assert by_id[SCHEMA_SNAPSHOT_MIGRATION_ID].checksum == schema_snapshot_checksum(
        schema_sql, views_sql
    )


def test_legacy_fence_checksum_differs_only_by_session_scope() -> None:
    canonical = (
        importlib.resources.files("okg.substrate.db")
        .joinpath("migrations/0003_deployment_promotion_fence.sql")
        .read_text()
    )
    legacy = canonical.replace("SET LOCAL lock_timeout", "SET lock_timeout").replace(
        "SET LOCAL statement_timeout", "SET statement_timeout"
    )
    assert legacy != canonical
    assert hashlib.sha256(legacy.encode("utf-8")).hexdigest() == (
        DEPLOYMENT_PROMOTION_FENCE_LEGACY_CHECKSUM
    )


def test_known_migration_reversibility_flags() -> None:
    by_id = {m.migration_id: m for m in current_known_migrations()}
    # The whole-schema snapshot has NO safe inverse.
    assert by_id[SCHEMA_SNAPSHOT_MIGRATION_ID].reversible is False
    assert by_id[SCHEMA_SNAPSHOT_MIGRATION_ID].down_sql_path is None
    remove = by_id[REMOVE_JOB_RUNTIME_SUBSYSTEM_MIGRATION_ID]
    assert remove.reversible is False
    assert remove.down_sql_path is None
    fence = by_id["0003_deployment_promotion_fence"]
    assert fence.reversible is False
    assert fence.down_sql_path is None
    hot_manifest = by_id["0005_chronos_hot_index_manifest"]
    assert hot_manifest.reversible is False
    assert hot_manifest.down_sql_path is None
    read_coordination = by_id["0007_chronos_history_read_coordination"]
    assert read_coordination.reversible is False
    assert read_coordination.down_sql_path is None
    read_lease_api = by_id["0008_history_read_lease_role_api"]
    assert read_lease_api.reversible is False
    assert read_lease_api.down_sql_path is None
    refusal_lookup = by_id["0009_compacted_generation_read_api"]
    assert refusal_lookup.reversible is False
    assert refusal_lookup.down_sql_path is None
    safe_numeric = by_id["0010_compacted_generation_safe_numeric"]
    assert safe_numeric.reversible is False
    assert safe_numeric.down_sql_path is None
    deployment_scope = by_id["0011_compacted_generation_deployment_scope"]
    assert deployment_scope.reversible is False
    assert deployment_scope.down_sql_path is None
    lease_scope = by_id["0012_history_read_lease_scope"]
    assert lease_scope.reversible is False
    assert lease_scope.down_sql_path is None
    attribute_receipts = by_id["0013_attribute_integrity_receipts"]
    assert attribute_receipts.reversible is False
    assert attribute_receipts.down_sql_path is None
    fence_release = by_id["0018_promotion_fence_release_receipt"]
    assert fence_release.reversible is False
    assert fence_release.down_sql_path is None
    dlq_acknowledgements = by_id["0019_runtime_dlq_acknowledgement_events"]
    assert dlq_acknowledgements.reversible is False
    assert dlq_acknowledgements.down_sql_path is None


# --------------------------------------------------------------------------
# 2a. Loud refuse on missing inverse (hermetic — refuses before connecting).
# --------------------------------------------------------------------------


def test_migrate_down_refuses_loudly_without_inverse() -> None:
    # The schema snapshot has no inverse; migrate_down must RAISE (loud),
    # never silently succeed / no-op. Refusal happens before any DB connect,
    # so an unreachable/garbage DSN is fine here.
    with pytest.raises(SubstrateError) as excinfo:
        migrate_down("postgres://unused", SCHEMA_SNAPSHOT_MIGRATION_ID)
    assert "no reversible inverse" in str(excinfo.value)


def test_migrate_down_rejects_unknown_migration() -> None:
    with pytest.raises(SubstrateError) as excinfo:
        migrate_down("postgres://unused", "9999_does_not_exist")
    assert "unknown migration id" in str(excinfo.value)


def test_migrate_down_refuses_job_runtime_data_resurrection() -> None:
    with pytest.raises(SubstrateError) as excinfo:
        migrate_down(
            "postgres://unused",
            REMOVE_JOB_RUNTIME_SUBSYSTEM_MIGRATION_ID,
            confirm=True,
        )
    assert "no reversible inverse" in str(excinfo.value)


@pytest.mark.postgres
def test_migrate_down_snapshot_refuses_loudly_against_real_db(
    okg_dsn: str,
) -> None:
    dbname = f"okg_migdown_snap_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        migrate(dsn)
        # The snapshot is applied in the ledger, yet has no inverse — the
        # rollback must REFUSE LOUDLY, leaving the schema intact.
        with pytest.raises(SubstrateError) as excinfo:
            migrate_down(dsn, SCHEMA_SNAPSHOT_MIGRATION_ID, confirm=True)
        assert "no reversible inverse" in str(excinfo.value)
        # Schema is still present (no silent drop).
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute("SELECT to_regclass('okg.graph_generations')")
            assert cur.fetchone()[0] is not None
    finally:
        _drop_disposable_database(okg_dsn, dbname)


# --------------------------------------------------------------------------
# 3. Reconcile-on-change: a no-op catalog apply stamps NO new cv.
# --------------------------------------------------------------------------


@pytest.mark.postgres
def test_no_op_catalog_apply_stamps_no_new_catalog_version(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import okg.substrate.deployment_upgrade as du
    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import (
        ComposedCatalog,
        NarrowingRow,
    )
    from okg.substrate.catalog.ownership import catalog_identity_for

    # Unique deployment name per run so a shared test DB's persisted
    # okg.catalog_versions rows from a prior run never leak into the count.
    deployment = f"reconcile-noop-{uuid4().hex[:10]}"
    composed = ComposedCatalog(
        deployment_name=deployment,
        modules=["mod_a"],
        subtypes={"concept", "document"},
        subtype_origin={"concept": "mod_a", "document": "mod_a"},
        narrowings=[
            NarrowingRow(
                parent_subtype="document",
                edge_archetype="contains",
                child_subtype="concept",
                origin="mod_a",
                narrowing_name="DocumentContainsConcept",
            ),
        ],
        aliases=[],
        identifier_patterns=[],
    )
    identity = catalog_identity_for(
        composed=composed,
        deployment_dir=Path("/tmp/reconcile-noop-deploy"),
        command="test seed",
    )

    # Seed one applied cv for this deployment with a known fingerprint.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        apply_composed_catalog(conn, composed, identity=identity)

    cvs_before = _count_catalog_versions(clean_runtime, deployment)
    assert cvs_before >= 1

    # The latest-applied fingerprint helper sees the seeded cv.
    assert (
        du._latest_applied_catalog_fingerprint(clean_runtime, deployment)
        == identity.catalog_fingerprint
    )

    # Drive _phase_catalog_apply with a context whose composed fingerprint
    # MATCHES the seeded one. The phase must short-circuit: no command runs,
    # no new cv is stamped.
    calls: list[list[str]] = []

    def _recording_runner(argv: list[str], env: dict[str, str]):
        calls.append(argv)
        return du.CommandResult(0, "{}", "")

    class _FakeManifest:
        deployment_dir = Path("/tmp/reconcile-noop-deploy")

    monkeypatch.setattr(
        du, "_resolve_upgrade_deployment", lambda ctx: _FakeManifest(),
    )
    monkeypatch.setattr(
        du,
        "_composed_catalog_fingerprint",
        lambda deployment_dir: identity.catalog_fingerprint,
    )
    monkeypatch.setattr(
        du,
        "_revalidate_effect_authority",
        lambda ctx, backup: {"ok": True},
    )

    ctx = du.UpgradeContext(
        options=du.UpgradeOptions(deployment=deployment),
        dsn=clean_runtime,
        resolution={},
        env={},
        state_file=Path("/tmp/unused-state.json"),
        backup_dir=Path("/tmp"),
        command_runner=_recording_runner,
    )

    # `catalog_apply` runs after `migrate` in PHASES, so by the time it is
    # reached the mutation-time backup revalidation has already run and cached
    # its result. Seed that precondition: this test covers catalog fingerprint
    # reconciliation, not the backup gate (which has its own coverage in
    # tests/substrate/test_deployment_upgrade.py).
    ctx.data["backup_mutation_revalidation"] = {"ok": True}

    out = du._phase_catalog_apply(ctx)
    assert out["ok"] is True
    assert out.get("catalog_unchanged") is True
    # No `catalog load --apply` subprocess was launched.
    assert calls == []

    cvs_after = _count_catalog_versions(clean_runtime, deployment)
    assert cvs_after == cvs_before  # no new cv stamped on a no-op up


@pytest.mark.postgres
def test_changed_catalog_apply_runs_the_command(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Counterpart: a CHANGED fingerprint must NOT short-circuit — the apply
    # command runs. (We stub the runner so no real subprocess is needed.)
    import okg.substrate.deployment_upgrade as du

    deployment = "reconcile-changed-deploy"
    calls: list[list[str]] = []

    def _recording_runner(argv: list[str], env: dict[str, str]):
        calls.append(argv)
        return du.CommandResult(0, "{}", "")

    root = tmp_path / deployment
    root.mkdir()
    (root / "deployment.yaml").write_text(f"name: {deployment}\nmodules: []\n")

    class _FakeManifest:
        deployment_dir = root

    monkeypatch.setattr(
        du, "_resolve_upgrade_deployment", lambda ctx: _FakeManifest(),
    )
    # No applied cv for this deployment, so latest fingerprint is None and
    # the probe returns a fresh fingerprint -> the apply runs.
    monkeypatch.setattr(
        du,
        "_composed_catalog_fingerprint",
        lambda deployment_dir: "sha256:changed",
    )
    monkeypatch.setattr(
        du,
        "_revalidate_effect_authority",
        lambda ctx, backup: {"ok": True},
    )

    ctx = du.UpgradeContext(
        options=du.UpgradeOptions(deployment=deployment),
        dsn=clean_runtime,
        resolution={},
        env={},
        state_file=Path("/tmp/unused-state.json"),
        backup_dir=Path("/tmp"),
        command_runner=_recording_runner,
    )

    # Same precondition as the no-op counterpart: the backup gate has already
    # been satisfied by the preceding `migrate` phase.
    ctx.data["backup_mutation_revalidation"] = {"ok": True}

    out = du._phase_catalog_apply(ctx)
    assert out["ok"] is True
    assert out.get("catalog_unchanged") is None
    assert len(calls) == 1
    assert "catalog" in calls[0] and "load" in calls[0]


# --------------------------------------------------------------------------
# Helpers.
# --------------------------------------------------------------------------


def _count_catalog_versions(dsn: str, deployment: str) -> int:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.catalog_versions "
            "WHERE deployment_name = %s",
            (deployment,),
        )
        return int(cur.fetchone()[0])


def _create_disposable_database(base_dsn: str, dbname: str) -> str:
    info = conninfo_to_dict(base_dsn)
    info["dbname"] = "postgres"
    maintenance_dsn = make_conninfo(**info)
    with psycopg.connect(
        maintenance_dsn, autocommit=True, connect_timeout=5,
    ) as conn, conn.cursor() as cur:
        cur.execute(
            sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname))
        )
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
            sql.SQL("DROP DATABASE IF EXISTS {}").format(
                sql.Identifier(dbname)
            )
        )


@pytest.mark.postgres
def test_fresh_migrate_installs_projection_remediator_graph_grants(
    okg_dsn: str,
) -> None:
    dbname = f"okg_projection_role_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        migrate(dsn)
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            for relation in ("okg.graph_nodes", "okg.graph_edges"):
                cur.execute("SELECT to_regclass(%s)", (relation,))
                assert cur.fetchone()[0] is not None
                cur.execute(
                    "SELECT has_table_privilege("
                    "'projection_remediator', %s, 'SELECT,INSERT,UPDATE,DELETE')",
                    (relation,),
                )
                assert cur.fetchone()[0] is True
    finally:
        _drop_disposable_database(okg_dsn, dbname)
