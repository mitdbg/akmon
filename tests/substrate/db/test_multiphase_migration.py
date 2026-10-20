"""The declared multi-phase migration class.

Covers req.schema-migration-hardening.multiphase-class:

  1. Classifier: top-level data-motion statements are counted with
     dollar-quote awareness (function/DO bodies are one statement); the
     shipped ordered tree contains no undeclared data migration; an
     UNDECLARED multi-statement data migration is a typed refusal, a
     declared one passes.
  2. Ordered-runner exclusion: a pending ``class: multi_phase``
     migration is never executed inside migrate's transaction — it is
     surfaced as ``pending_multiphase`` (the single-commit invariant
     covers ordinary migrations only).
  3. Crash-resume on a populated scratch copy: kill -9 mid-run leaves
     exact committed checkpoints; migration_status and doctor surface
     the mid-state; a resume continues from the watermark (never
     restarts) and completes with a correct final state and a ledger
     completion record distinct from single-transaction migrations.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

import multiphase_reference as reference
from okg.substrate.db import management
from okg.substrate.db.management import (
    current_known_migrations,
    database_health_findings,
    inspect_database,
    load_migrations_manifest,
    migration_status,
    run_pending_ordered_migrations,
    validate_pending_against_manifest,
)
from okg.substrate.db.migrate import migrate
from okg.substrate.db.migration_class import (
    classify_data_motion,
    is_undeclared_data_migration,
    top_level_statements,
)
from okg.substrate.db.multiphase import run_multiphase_migration
from okg.substrate.errors import (
    MigrationClassInvalid,
    MigrationClassUndeclared,
    MultiphaseMigrationInvalid,
)

# --------------------------------------------------------------------------
# Helpers (disposable-database pattern shared with the other migrate tests).
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


def _patched_reference_tree(
    monkeypatch: pytest.MonkeyPatch, *, declare_multiphase: bool
) -> None:
    """Inject the reference migration through the runner's real seams:
    enumeration (current_known_migrations), the manifest, and the
    forward-SQL loader — the shipped migrations directory stays clean."""
    real_known = management.current_known_migrations
    real_manifest = load_migrations_manifest()
    real_loader = management._ordered_migration_forward_sql

    def known_with_reference(**kwargs):
        return [*real_known(**kwargs), reference.known_migration()]

    patched_manifest = {
        key: dict(value) for key, value in real_manifest.items()
    }
    patched_manifest[reference.REFERENCE_MIGRATION_ID] = (
        reference.manifest_entry(declare_multiphase=declare_multiphase)
    )

    def loader_with_reference(migration_id: str) -> str:
        if migration_id == reference.REFERENCE_MIGRATION_ID:
            return reference.fixture_sql()
        return real_loader(migration_id)

    monkeypatch.setattr(
        management, "current_known_migrations", known_with_reference
    )
    monkeypatch.setattr(
        management, "load_migrations_manifest", lambda: patched_manifest
    )
    monkeypatch.setattr(
        management, "_ordered_migration_forward_sql", loader_with_reference
    )


# --------------------------------------------------------------------------
# 1. Classifier (hermetic).
# --------------------------------------------------------------------------


def test_classifier_counts_top_level_dml_only() -> None:
    """DML inside dollar-quoted function/DO bodies is one CREATE/DO
    statement, not top-level data motion; top-level DML is counted."""
    function_body = """
    CREATE FUNCTION okg_widget_touch() RETURNS void
    LANGUAGE plpgsql AS $$
    BEGIN
      UPDATE okg_widget SET n = n + 1;
      DELETE FROM okg_widget WHERE n > 10;
      INSERT INTO okg_widget (n) VALUES (1);
    END
    $$;
    DO $tag$
    BEGIN
      UPDATE okg_widget SET n = 0;
    END
    $tag$;
    """
    counts = classify_data_motion(function_body)
    assert counts == {"statement_count": 2, "data_motion_statements": 0}
    assert is_undeclared_data_migration(function_body) is None

    batched = reference.fixture_sql()
    counts = classify_data_motion(batched)
    assert counts["data_motion_statements"] == 2
    assert is_undeclared_data_migration(batched) == counts

    # Quoting edge cases: semicolons and dollar signs inside strings,
    # quoted identifiers, and comments do not split statements.
    tricky = (
        "INSERT INTO t (s) VALUES ('a;b'',$$;');\n"
        '-- UPDATE commented; out\n'
        "/* UPDATE in /* nested */ block; */\n"
        'UPDATE "weird;name" SET x = 1'
    )
    assert len(top_level_statements(tricky)) == 2
    assert classify_data_motion(tricky)["data_motion_statements"] == 2

    # One seed INSERT among DDL is an ordinary migration, not data motion.
    seeded_ddl = (
        "CREATE TABLE t (i int);\n"
        "INSERT INTO t VALUES (1);\n"
        "CREATE INDEX t_i ON t (i)"
    )
    assert is_undeclared_data_migration(seeded_ddl) is None

    # The CTE-wrapped backfill idiom and data-modifying CTEs are data
    # motion (adversarial-review finding: WITH-led DML evaded the
    # leading-keyword scan).
    cte_backfill = (
        "WITH batch AS (SELECT id FROM t LIMIT 100) "
        "UPDATE t SET x = 1 WHERE id IN (SELECT id FROM batch);\n"
        "WITH gone AS (DELETE FROM t WHERE x = 0 RETURNING id) "
        "SELECT count(*) FROM gone"
    )
    assert classify_data_motion(cte_backfill)["data_motion_statements"] == 2
    assert is_undeclared_data_migration(cte_backfill) is not None

    # A pure read-only WITH ... SELECT is not data motion, even when a
    # string literal mentions DML.
    with_select = (
        "WITH counts AS (SELECT count(*) AS n FROM t) "
        "SELECT n, 'UPDATE nothing' FROM counts;\n"
        "CREATE INDEX t_x ON t (x)"
    )
    assert classify_data_motion(with_select)["data_motion_statements"] == 0

    # COPY FROM loads rows (data motion); COPY TO only reads.
    copies = (
        "COPY t (id, x) FROM '/tmp/t.csv' WITH (FORMAT csv);\n"
        "COPY t (id, x) FROM '/tmp/t2.csv' WITH (FORMAT csv)"
    )
    assert classify_data_motion(copies)["data_motion_statements"] == 2
    copy_out = (
        "COPY t TO '/tmp/out.csv' WITH (FORMAT csv);\n"
        "COPY t TO '/tmp/out2.csv' WITH (FORMAT csv)"
    )
    assert classify_data_motion(copy_out)["data_motion_statements"] == 0


def test_shipped_ordered_tree_has_no_undeclared_data_migrations() -> None:
    """Real tree green: every shipped ordered migration passes the
    data-motion classifier under its shipped manifest entry."""
    ordered = [
        migration
        for migration in current_known_migrations()
        if migration.migration_id != "0000_schema_snapshot"
    ]
    assert ordered, "expected shipped ordered migrations"
    # The full manifest check (which now includes the class check) must
    # not refuse the shipped tree.
    validate_pending_against_manifest(ordered)


def test_undeclared_multistatement_data_migration_is_refused() -> None:
    """The mutation fixture: the reference backfill WITHOUT
    `class: multi_phase` is a typed refusal; WITH it, validation passes."""
    pending = [reference.known_migration()]
    undeclared = {
        reference.REFERENCE_MIGRATION_ID: reference.manifest_entry(
            declare_multiphase=False
        )
    }
    with pytest.raises(MigrationClassUndeclared) as excinfo:
        validate_pending_against_manifest(
            pending,
            manifest=undeclared,
            forward_sql_loader=lambda _mid: reference.fixture_sql(),
        )
    assert excinfo.value.data["data_motion_statements"] == 2

    declared = {
        reference.REFERENCE_MIGRATION_ID: reference.manifest_entry(
            declare_multiphase=True
        )
    }
    validate_pending_against_manifest(
        pending,
        manifest=declared,
        forward_sql_loader=lambda _mid: reference.fixture_sql(),
    )


def test_unrecognized_or_contradictory_class_is_refused() -> None:
    pending = [reference.known_migration()]
    bogus = {
        reference.REFERENCE_MIGRATION_ID: {
            **reference.manifest_entry(declare_multiphase=False),
            "class": "definitely_not_a_class",
        }
    }
    with pytest.raises(MigrationClassInvalid):
        validate_pending_against_manifest(
            pending,
            manifest=bogus,
            forward_sql_loader=lambda _mid: reference.fixture_sql(),
        )
    # Destructive + multi_phase is contradictory. Use a destructive slug
    # so the destructive flag AGREES with the filename heuristic and the
    # class contradiction is the check that fires.
    from okg.substrate.db.management import KnownMigration

    destructive_id = "0097_drop_widget_batches"
    destructive_pending = [
        KnownMigration(
            migration_id=destructive_id,
            description=f"ordered migration {destructive_id}",
            checksum="abc123",
            destructive=True,
        )
    ]
    destructive_multiphase = {
        destructive_id: {
            "migration_id": destructive_id,
            "checksum_sha256": "abc123",
            "destructive": True,
            "class": "multi_phase",
        }
    }
    with pytest.raises(MigrationClassInvalid):
        validate_pending_against_manifest(
            destructive_pending,
            manifest=destructive_multiphase,
            forward_sql_loader=lambda _mid: reference.fixture_sql(),
        )


# --------------------------------------------------------------------------
# 2. Ordered-runner exclusion + apply-path refusal (real database).
# --------------------------------------------------------------------------


@pytest.mark.postgres
def test_runner_excludes_declared_multiphase_and_refuses_undeclared(
    okg_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dbname = f"okg_test_mp_runner_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        migrate(dsn)

        # Declared: surfaced as pending_multiphase, NEVER executed inline.
        # (The scratch table does not exist in this database, so inline
        # execution of the fixture SQL would error loudly — its absence
        # from the error stream is itself proof of exclusion.)
        _patched_reference_tree(monkeypatch, declare_multiphase=True)
        with psycopg.connect(dsn) as conn:
            outcome = run_pending_ordered_migrations(
                conn,
                schema_sql=_schema_text(),
                views_sql=_views_text(),
            )
            conn.commit()
        assert [
            row["migration_id"] for row in outcome["pending_multiphase"]
        ] == [reference.REFERENCE_MIGRATION_ID]
        assert reference.REFERENCE_MIGRATION_ID not in outcome["executed"]
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.schema_migrations "
                "WHERE migration_id = %s",
                (reference.REFERENCE_MIGRATION_ID,),
            )
            assert cur.fetchone()[0] == 0

        # A green `okg up` must never read as "the multi-phase migration
        # ran": the pending declared migration is surfaced by
        # migration_status AND doctor until its completion ledger row
        # exists (adversarial-review finding: silently-unrun pending).
        status = migration_status(dsn)
        assert [
            row["migration_id"] for row in status["multiphase_pending"]
        ] == [reference.REFERENCE_MIGRATION_ID]
        findings = database_health_findings(inspect_database(dsn))
        pending_findings = [
            finding
            for finding in findings
            if finding["code"] == "multiphase_migration_pending"
        ]
        assert len(pending_findings) == 1
        assert pending_findings[0]["severity"] == "warning"

        # Undeclared: the SAME file with no class declaration is refused
        # by the manifest check at the real apply path (mutation red).
        _patched_reference_tree(monkeypatch, declare_multiphase=False)
        with psycopg.connect(dsn) as conn:
            with pytest.raises(MigrationClassUndeclared):
                run_pending_ordered_migrations(
                    conn,
                    schema_sql=_schema_text(),
                    views_sql=_views_text(),
                )

        # Driver contract enforcement, on the same migrated DB:
        # a phase that commits internally, and a phase whose watermark
        # does not advance, are both typed refusals — never a torn
        # checkpoint or an infinite lock-holding loop.
        from okg.substrate.db.multiphase import (
            MultiphasePhase,
            MultiphaseSpec,
        )

        manifest = {
            reference.REFERENCE_MIGRATION_ID: reference.manifest_entry(
                declare_multiphase=True
            )
        }
        with psycopg.connect(dsn) as conn:
            reference.seed_scratch(conn)

        def committing_batch(batch_conn, watermark):
            batch_conn.commit()
            return 50

        committing_spec = MultiphaseSpec(
            migration_id=reference.REFERENCE_MIGRATION_ID,
            phases=(
                MultiphasePhase(
                    name=reference.BACKFILL_PHASE,
                    run_batch=committing_batch,
                ),
            ),
        )
        with pytest.raises(MultiphaseMigrationInvalid) as excinfo:
            run_multiphase_migration(
                dsn,
                reference.known_migration(),
                committing_spec,
                manifest=manifest,
            )
        assert "commit" in str(excinfo.value)

        def stuck_batch(batch_conn, watermark):
            return 50  # never advances past the first checkpoint

        stuck_spec = MultiphaseSpec(
            migration_id=reference.REFERENCE_MIGRATION_ID,
            phases=(
                MultiphasePhase(
                    name=reference.BACKFILL_PHASE,
                    run_batch=stuck_batch,
                ),
            ),
        )
        with pytest.raises(MultiphaseMigrationInvalid) as excinfo:
            run_multiphase_migration(
                dsn,
                reference.known_migration(),
                stuck_spec,
                manifest=manifest,
            )
        assert "unchanged watermark" in str(excinfo.value)
    finally:
        _drop_disposable_database(okg_dsn, dbname)


# --------------------------------------------------------------------------
# 2b. The read layer waits for the ordinals the barrier deferred.
# --------------------------------------------------------------------------

#: The retired view whose stored definition, standing on a database that has
#: not yet applied 0024, depends on okg.v_nodes. Re-created here by hand
#: because views.sql no longer declares it — which is precisely why the
#: frozen-shape convergence pass may not drop it.
_RETIRED_DEPENDANT = "okg.v_changes"


def _views_and_grants(dsn: str, name: str) -> list[tuple[str, str]]:
    """(view, grantee) for a stored okg view, empty if it is not there."""
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT 'okg.' || c.relname, a.grantee::regrole::text
              FROM pg_class c
              JOIN pg_namespace n ON n.oid = c.relnamespace
              CROSS JOIN aclexplode(c.relacl) AS a
             WHERE n.nspname = 'okg' AND c.relname = %s AND c.relkind = 'v'
               AND a.privilege_type = 'SELECT'
               AND a.grantee <> c.relowner
             ORDER BY 2
            """,
            (name,),
        )
        return [tuple(row) for row in cur.fetchall()]


@pytest.mark.postgres
def test_read_layer_install_waits_for_deferred_ordinals(
    okg_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """migrate must not install HEAD's views over a prefix of HEAD's schema.

    The failure this pins (migration-convergence-linux, dev 1daf6b497): the
    ordered runner's barrier defers every ordinal behind a pending
    ``class: multi_phase`` migration, so 0024 — which retires
    ``okg.v_changes`` — had not run when the first pass installed views.sql.
    HEAD's ``okg.v_nodes`` shape is unreachable by CREATE OR REPLACE VIEW on
    a pre-0024 database, and the convergence pass rightly refuses to drop
    ``okg.v_changes`` to get there: the scripts no longer declare it, and
    dropping what nobody asked to drop is how a cleanup becomes an outage.
    So migrate aborted, and every upgrade across 0020 was blocked.

    Built from a real migrated database rewound to that state rather than
    from the pinned convergence dump, so the mechanism is pinned here even
    if the dump is re-rolled.
    """
    from okg.substrate.db import migrate as migrate_mod

    dbname = f"okg_test_mp_readlayer_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        migrate(dsn)

        # Rewind to a pre-0020 deployment: 0020 pending again (so the
        # barrier engages) and 0024 pending behind it (so the retired view
        # survives into the first pass).
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                "DELETE FROM okg.schema_migrations WHERE migration_id = ANY(%s)",
                (["0020_interval_narrow_split",
                  "0024_canonical_read_path_view_cleanup"],),
            )
            # A stored okg.v_nodes carrying a column HEAD's definition does
            # not have: CREATE OR REPLACE VIEW may append columns, never
            # drop them, so this is exactly the shape migrate cannot reach.
            cur.execute("DROP VIEW IF EXISTS okg.v_nodes CASCADE")
            cur.execute(
                "CREATE VIEW okg.v_nodes AS "
                "SELECT *, NULL::bigint AS legacy_valid_to_gen "
                "  FROM okg.graph_nodes"
            )
            cur.execute(
                f"CREATE VIEW {_RETIRED_DEPENDANT} AS "
                "SELECT * FROM okg.v_nodes"
            )
            conn.commit()

        # Observe the decision, not just the outcome: record what the runner
        # deferred on each pass, which pass installed the read layer, and what
        # the deferred pass left standing once it had committed.
        passes: list[list[str]] = []
        install_passes: list[int] = []
        observed_after_deferred_pass: list[tuple[str, str]] = []
        real_runner = management.run_pending_ordered_migrations
        real_install = migrate_mod._install_views_sql
        real_driver = migrate_mod._run_pending_multiphase

        def traced_runner(conn, **kwargs):
            outcome = real_runner(conn, **kwargs)
            passes.append(list(outcome["deferred_post_multiphase"]))
            return outcome

        def traced_install(*args, **kwargs):
            install_passes.append(len(passes))
            return real_install(*args, **kwargs)

        def traced_driver(driver_dsn, **kwargs):
            # Runs between the deferred pass's commit and the second pass:
            # the exact window a real deployment spends in the backfill.
            observed_after_deferred_pass.extend(
                _views_and_grants(driver_dsn, "v_generations")
            )
            return real_driver(driver_dsn, **kwargs)

        # The runner is imported from management inside _migrate_once's body,
        # so the module attribute is the seam both passes resolve through.
        monkeypatch.setattr(
            management, "run_pending_ordered_migrations", traced_runner
        )
        monkeypatch.setattr(migrate_mod, "_install_views_sql", traced_install)
        monkeypatch.setattr(
            migrate_mod, "_run_pending_multiphase", traced_driver
        )
        migrate(dsn)
        monkeypatch.undo()

        # The deferred pass must leave the read layer as it found it. The
        # Chronos checkpoint rebuild drops okg.v_generations early in EVERY
        # pass; a deferred pass that did not put it back would commit a hole
        # lasting the whole driver run — okg.v_generations is the MCP
        # sidecar's startup probe and is read by okg.source_frontier_state(),
        # whose plpgsql body carries no tracked dependency to protect it.
        assert observed_after_deferred_pass == [("okg.v_generations", "app_ro")], (
            "the deferred pass did not leave okg.v_generations (and its grant) "
            f"standing: {observed_after_deferred_pass}"
        )

        assert passes and passes[0], (
            "the barrier did not engage; this test no longer exercises the "
            f"deferred-ordinal path (per-pass deferrals: {passes})"
        )
        assert "0024_canonical_read_path_view_cleanup" in passes[0]
        assert install_passes, "the read layer was never installed"
        for pass_index in install_passes:
            assert pass_index >= 1, (
                "views.sql was installed before the ordered runner ran; the "
                "pass bookkeeping below no longer means what it says"
            )
            assert not passes[pass_index - 1], (
                "views.sql was installed on a pass that still had deferred "
                f"ordinals: {passes[pass_index - 1]}"
            )

        # And the end state converged: the retired dependant is gone (0024
        # ran) and okg.v_nodes carries HEAD's shape, not the stale column.
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute("SELECT to_regclass(%s)", (_RETIRED_DEPENDANT,))
            assert cur.fetchone()[0] is None
            cur.execute("SELECT to_regclass('okg.v_nodes')")
            assert cur.fetchone()[0] is not None
            cur.execute(
                """
                SELECT count(*) FROM information_schema.columns
                 WHERE table_schema = 'okg' AND table_name = 'v_nodes'
                   AND column_name = 'legacy_valid_to_gen'
                """
            )
            assert cur.fetchone()[0] == 0, (
                "okg.v_nodes kept the stale column; the frozen-shape "
                "convergence did not reach HEAD's definition"
            )
    finally:
        _drop_disposable_database(okg_dsn, dbname)


# --------------------------------------------------------------------------
# 3. Crash-resume, mid-state visibility, distinct ledger completion.
# --------------------------------------------------------------------------

_KILL_AFTER_BATCHES = 3

_SUBPROCESS_TEMPLATE = """
import sys

sys.path.insert(0, {test_dir!r})

import multiphase_reference as reference
from okg.substrate.db.multiphase import run_multiphase_migration

run_multiphase_migration(
    {dsn!r},
    reference.known_migration(),
    reference.build_spec(kill_after_batches={kill_after}),
    manifest={{
        reference.REFERENCE_MIGRATION_ID: reference.manifest_entry(
            declare_multiphase=True
        )
    }},
)
raise SystemExit("unreachable: the run above must SIGKILL itself")
"""


@pytest.mark.postgres
def test_kill9_midrun_then_resume_completes_with_correct_state(
    okg_dsn: str,
    tmp_path: Path,
) -> None:
    dbname = f"okg_test_mp_crash_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        migrate(dsn)
        with psycopg.connect(dsn) as conn:
            reference.seed_scratch(conn)

        # --- Crash arm: a real subprocess SIGKILLs itself mid-batch. ---
        script = tmp_path / "multiphase_crash_arm.py"
        script.write_text(
            _SUBPROCESS_TEMPLATE.format(
                test_dir=str(Path(__file__).resolve().parent),
                dsn=dsn,
                kill_after=_KILL_AFTER_BATCHES,
            ),
            encoding="utf-8",
        )
        completed = subprocess.run(
            [sys.executable, str(script)],
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        assert completed.returncode == -9, (
            f"crash arm did not die by SIGKILL: rc={completed.returncode}, "
            f"stderr={completed.stderr[-2000:]}"
        )

        expected_watermark = _KILL_AFTER_BATCHES * reference.BATCH_SIZE

        # --- Mid-state: exact committed checkpoints, visible everywhere. ---
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                f"SELECT count(*) FROM {reference.SCRATCH_TABLE} "
                "WHERE dst IS NOT NULL"
            )
            assert cur.fetchone()[0] == expected_watermark, (
                "committed rows must equal the durable watermark: the "
                "killed batch's uncommitted work must be gone"
            )
            cur.execute(
                f"SELECT max(touch_count) FROM {reference.SCRATCH_TABLE}"
            )
            assert cur.fetchone()[0] == 1
            cur.execute(
                "SELECT count(*) FROM okg.schema_migrations "
                "WHERE migration_id = %s",
                (reference.REFERENCE_MIGRATION_ID,),
            )
            assert cur.fetchone()[0] == 0, "no completion record mid-run"

        status = migration_status(dsn)
        mid = status["multiphase_in_progress"]
        assert [row["migration_id"] for row in mid] == [
            reference.REFERENCE_MIGRATION_ID
        ]
        assert mid[0]["phase"] == reference.BACKFILL_PHASE
        assert mid[0]["watermark"] == expected_watermark
        assert mid[0]["batches_committed"] == _KILL_AFTER_BATCHES

        inspected = inspect_database(dsn)
        findings = database_health_findings(inspected)
        mid_findings = [
            finding
            for finding in findings
            if finding["code"] == "multiphase_migration_in_progress"
        ]
        assert len(mid_findings) == 1
        assert mid_findings[0]["severity"] == "warning"

        # --- Resume: continues from the checkpoint, never restarts. ---
        outcome = run_multiphase_migration(
            dsn,
            reference.known_migration(),
            reference.build_spec(),
            manifest={
                reference.REFERENCE_MIGRATION_ID: reference.manifest_entry(
                    declare_multiphase=True
                )
            },
        )
        assert outcome["status"] == "applied"

        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                f"SELECT count(*) FROM {reference.SCRATCH_TABLE} "
                "WHERE dst IS DISTINCT FROM src * 2"
            )
            assert cur.fetchone()[0] == 0, "every row backfilled"
            cur.execute(
                f"SELECT min(touch_count), max(touch_count) "
                f"FROM {reference.SCRATCH_TABLE}"
            )
            assert cur.fetchone() == (1, 1), (
                "each row touched exactly once: the resume continued "
                "from the watermark instead of restarting from zero"
            )
            cur.execute(
                "SELECT result_metadata FROM okg.schema_migrations "
                "WHERE migration_id = %s",
                (reference.REFERENCE_MIGRATION_ID,),
            )
            metadata = cur.fetchone()[0]
        # The completion record is DISTINCT from single-transaction rows.
        assert metadata["applied_via"] == "multiphase_runner"
        assert metadata["migration_class"] == "multi_phase"
        assert metadata["phases"][reference.BACKFILL_PHASE] == (
            reference.TOTAL_ROWS // reference.BATCH_SIZE
        ), (
            "batches_committed must total exactly TOTAL/BATCH across "
            "crash + resume: fewer means lost work, more means re-done "
            "work"
        )

        status = migration_status(dsn)
        assert status["multiphase_in_progress"] == []

        # Idempotent: a re-run is a clean no-op against the ledger.
        rerun = run_multiphase_migration(
            dsn,
            reference.known_migration(),
            reference.build_spec(),
            manifest={
                reference.REFERENCE_MIGRATION_ID: reference.manifest_entry(
                    declare_multiphase=True
                )
            },
        )
        assert rerun["status"] == "already_applied"
    finally:
        _drop_disposable_database(okg_dsn, dbname)


@pytest.mark.postgres
def test_driver_refuses_undeclared_class_and_missing_progress_table(
    okg_dsn: str,
) -> None:
    """Fail-closed driver refusals: a migration not declared multi_phase
    never runs through the driver, and an unmigrated database (no
    progress table) is a typed refusal, not an improvised ensure."""
    dbname = f"okg_test_mp_refuse_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        # No migrate yet: progress table absent.
        with pytest.raises(MultiphaseMigrationInvalid):
            run_multiphase_migration(
                dsn,
                reference.known_migration(),
                reference.build_spec(),
                manifest={
                    reference.REFERENCE_MIGRATION_ID: (
                        reference.manifest_entry(declare_multiphase=True)
                    )
                },
            )
        # Declared single-transaction: the driver refuses to run it.
        with pytest.raises(MultiphaseMigrationInvalid):
            run_multiphase_migration(
                dsn,
                reference.known_migration(),
                reference.build_spec(),
                manifest={
                    reference.REFERENCE_MIGRATION_ID: (
                        reference.manifest_entry(declare_multiphase=False)
                    )
                },
            )
    finally:
        _drop_disposable_database(okg_dsn, dbname)
