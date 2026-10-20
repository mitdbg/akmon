"""M1.1 tests: okg migrate — schema present, idempotent, extensions."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import contextlib
import json
import os
from pathlib import Path
import threading
import uuid

import psycopg
import pytest
from psycopg import sql
from psycopg.rows import tuple_row

from okg.substrate.db.connection import connect as db_connect
from okg.substrate.db.migrate import (
    REQUIRED_EXTENSIONS,
    _ensure_schema_roles,
    _schema_sql,
    _views_sql,
    install_views,
    migrate,
)
from okg.substrate.db.management import views_without_mcp_call_log_storage
from tests.substrate.conftest import _ensure_cluster_runtime_roles


pytestmark = pytest.mark.postgres


def test_schema_role_bootstrap_is_credential_free() -> None:
    class RecordingCursor:
        statements: list[str] = []

        def execute(self, statement: str) -> None:
            self.statements.append(statement)

    cursor = RecordingCursor()
    _ensure_schema_roles(cursor)  # type: ignore[arg-type]

    sql = cursor.statements[0]
    assert "CREATE ROLE app_rw NOLOGIN" in sql
    assert "CREATE ROLE app_ro NOLOGIN" in sql
    assert "CREATE ROLE catalog_admin NOLOGIN" in sql
    assert "CREATE ROLE projection_remediator NOLOGIN" in sql
    assert (
        "CREATE ROLE okg_external_action_api\n"
        "                NOLOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB "
        "NOCREATEROLE INHERIT"
    ) in sql
    assert (
        "CREATE ROLE okg_external_action_function_owner\n"
        "                NOLOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB "
        "NOCREATEROLE NOINHERIT"
    ) in sql
    assert sql.count("WHEN duplicate_object OR unique_violation THEN NULL") == 6
    assert "PASSWORD" not in sql


def test_schema_role_bootstrap_tolerates_concurrent_cluster_role_creation(
    okg_migrated: str,
) -> None:
    class RecordingCursor:
        statements: list[str] = []

        def execute(self, statement: str) -> None:
            self.statements.append(statement)

    cursor = RecordingCursor()
    _ensure_schema_roles(cursor)  # type: ignore[arg-type]

    token = uuid.uuid4().hex[:12]
    role_names = {
        name: f"race_{token}_{name}"
        for name in (
            "app_rw",
            "app_ro",
            "catalog_admin",
            "projection_remediator",
            "okg_external_action_api",
            "okg_external_action_function_owner",
        )
    }
    bootstrap_sql = cursor.statements[0]
    for original, replacement in sorted(
        role_names.items(), key=lambda item: len(item[0]), reverse=True
    ):
        bootstrap_sql = bootstrap_sql.replace(original, replacement)
    # Make both sessions observe the role as absent before either attempts the
    # cluster-global CREATE ROLE. The notice proves one session exercised the
    # duplicate-role handler instead of merely observing the winner's role.
    bootstrap_sql = bootstrap_sql.replace(
        "THEN\n            BEGIN",
        "THEN\n            PERFORM pg_sleep(0.2);\n            BEGIN",
    ).replace(
        "WHEN duplicate_object OR unique_violation THEN NULL;",
        "WHEN duplicate_object OR unique_violation THEN\n"
        "                RAISE NOTICE 'okg_role_race_caught';",
    )
    barrier = threading.Barrier(2)

    def install_roles() -> list[str]:
        notices: list[str] = []
        with db_connect(okg_migrated, autocommit=True) as conn:
            conn.add_notice_handler(
                lambda diagnostic: notices.append(diagnostic.message_primary)
            )
            barrier.wait(timeout=5)
            conn.execute(bootstrap_sql)
        return notices

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            notices = list(executor.map(lambda _index: install_roles(), range(2)))

        assert any(
            message == "okg_role_race_caught"
            for connection_notices in notices
            for message in connection_notices
        )
        with db_connect(okg_migrated) as conn:
            rows = conn.execute(
                "SELECT rolname, rolcanlogin, rolsuper, rolbypassrls, "
                "rolcreatedb, rolcreaterole FROM pg_roles "
                "WHERE rolname = ANY(%s)",
                (list(role_names.values()),),
            ).fetchall()
        assert len(rows) == len(role_names)
        assert all(not any(attributes) for _name, *attributes in rows)
    finally:
        with db_connect(okg_migrated, autocommit=True) as conn:
            for role_name in role_names.values():
                conn.execute(
                    sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(role_name))
                )


def test_test_runtime_role_bootstrap_serializes_concurrent_creation(
    okg_migrated: str,
) -> None:
    """Two fresh xdist sessions cannot race the cluster-wide role create."""
    role_name = f"race_{uuid.uuid4().hex[:12]}_okg_substrate"
    barrier = threading.Barrier(2)

    def install_role() -> None:
        barrier.wait(timeout=5)
        _ensure_cluster_runtime_roles(okg_migrated, role_name=role_name)

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(install_role) for _index in range(2)]
            for future in futures:
                future.result(timeout=10)

        with db_connect(okg_migrated) as conn:
            role_count = conn.execute(
                "SELECT count(*) FROM pg_catalog.pg_roles WHERE rolname = %s",
                (role_name,),
            ).fetchone()[0]
        assert role_count == 1
    finally:
        with db_connect(okg_migrated, autocommit=True) as conn:
            conn.execute(
                sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(role_name))
            )


def _without_sql_line_comments(sql: str) -> str:
    return "\n".join(
        line for line in sql.splitlines() if not line.lstrip().startswith("--")
    )


def test_migrate_creates_expected_tables(okg_migrated: str) -> None:
    with db_connect(okg_migrated) as conn, conn.cursor() as cur:
        cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'okg';")
        tables = {r[0] for r in cur.fetchall()}
    expected = {
        "node_facts",
        "edge_facts",
        # Chronos cutover: the canonical graph store is okg.graph_nodes /
        # okg.graph_edges (Chronos-versioned), created by the migration via
        # ChronosGraphStore.ensure_schema(). The legacy bitemporal
        # okg.nodes_live / okg.edges_live tables are dropped by the migration.
        "graph_nodes",
        "graph_edges",
        "graph_generations",
        "catalog_versions",
        "source_watermarks",
        "link_cache",
        "subtypes",
        "narrowings",
        "aliases",
        "invariants",
        "derived_artifact_definitions",
        "derived_artifact_runs",
        "derived_output_manifests",
        "derived_output_slots",
        "derived_relation_registry",
        "derived_dirty_scopes",
        "derived_dirty_rule_runs",
        "derived_dirty_rule_audits",
        "derived_artifact_dependencies",
        "derived_schedule_plans",
        "derived_schedule_nodes",
        "derived_run_dependency_bindings",
        "consistency_check_definitions",
        "consistency_candidates",
        "consistency_candidate_coverage",
        "consistency_gate_results",
        "observation_events",
        "observation_current",
        "lens_definitions",
        "lens_putback_requests",
        "lens_putback_deltas",
        "lens_overlay_log",
        "telemetry_traces",
        "telemetry_spans",
        "telemetry_events",
        "telemetry_metrics",
        "telemetry_artifacts",
    }
    missing = expected - tables
    assert not missing, f"migrate did not create: {missing}"
    # Chronos and DBOS cutovers: legacy graph/runtime tables must be gone.
    dropped = {"nodes_live", "edges_live", "jobs", "publisher_leases"} & tables
    assert not dropped, f"migrate must drop legacy tables: {dropped}"


def test_migrate_is_idempotent(okg_migrated: str) -> None:
    # Re-run migrate; must converge without error.
    r1 = migrate(okg_migrated)
    r2 = migrate(okg_migrated)
    assert r1.statements_executed == r2.statements_executed
    assert r1.extension_versions == r2.extension_versions


def test_schema_replay_does_not_resurrect_the_legacy_admission_generation_id(
    okg_migrated: str,
) -> None:
    """A schema replay must not undo migration 0023's column drop.

    ``dev`` carried an ``ADD COLUMN IF NOT EXISTS target_generation_id BIGINT``
    in schema.sql so an install missing that column could still build the index
    over it. D2 drops both the column and that index, and schema.sql is replayed
    on every checksum change -- i.e. AFTER 0023 has already run and can never
    run again. Re-adding the compat ALTER would therefore make the column
    reappear on the next migrate, permanently diverging an upgraded database
    from a fresh install. This asserts the drop survives the replay.
    """
    with (
        db_connect(
            okg_migrated,
            autocommit=True,
        ) as conn,
        conn.cursor() as cur,
    ):
        cur.execute(
            "UPDATE okg.schema_migrations SET result_metadata = "
            "jsonb_set(result_metadata, "
            "'{executable_schema_checksum}', '\"force-schema-replay\"') "
            "WHERE migration_id = '0000_schema_snapshot'"
        )

    migrate(okg_migrated)

    with db_connect(okg_migrated) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_schema = 'okg' "
            "AND table_name = 'admission_envelopes' "
            "AND column_name = 'target_generation_id'"
        )
        assert cur.fetchone() is None
        cur.execute("SELECT to_regclass('okg.admission_envelopes_generation_idx')")
        assert cur.fetchone() == (None,)
        # The replacement is live and keyed on the TEXT ref.
        cur.execute(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_schema = 'okg' "
            "AND table_name = 'admission_envelopes' "
            "AND column_name = 'target_generation_ref'"
        )
        assert cur.fetchone() == ("text",)
        cur.execute("SELECT to_regclass('okg.admission_envelopes_generation_ref_idx')")
        assert cur.fetchone() == ("okg.admission_envelopes_generation_ref_idx",)


def test_schema_replay_keeps_the_ir_binding_generation_ref_text(
    okg_migrated: str,
) -> None:
    """A schema replay must not convert the IR generation ref back to BIGINT.

    ``dev`` carried a DO block in schema.sql that converged a legacy TEXT
    ``ir_program_bindings.generation_id`` to BIGINT and raised
    ``datatype_mismatch`` on any nonnumeric value. D2 reverses the canonical
    type, so that block is deleted rather than merged: kept, it would either
    flip the converged column back on every replay or -- once a real Chronos ref
    is stored -- abort every subsequent migrate. A stored Chronos ref is written
    here first so the regression would fail loudly rather than silently.
    """
    with (
        db_connect(
            okg_migrated,
            autocommit=True,
        ) as conn,
        conn.cursor() as cur,
    ):
        cur.execute(
            "INSERT INTO okg.ir_programs "
            "(program_checksum, ir_version, program_kind, determinism) "
            "VALUES ('d2-ir-binding-generation-ref', 'okg-ir-v1', "
            "'read', 'deterministic') "
            "ON CONFLICT (program_checksum) DO UPDATE "
            "SET program_checksum = EXCLUDED.program_checksum "
            "RETURNING program_id"
        )
        program_id = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO okg.ir_program_bindings "
            "(program_id, deployment_name, owner_kind, owner_id, "
            "program_role, generation_id) "
            "VALUES (%s, 'd2-ir-binding', 'graph_api_call', "
            "'d2-owner', 'read', "
            "'okg:d2:branch:default:gen:20260809T000000000000Z:deadbeef') "
            "ON CONFLICT DO NOTHING",
            (program_id,),
        )
        cur.execute(
            "UPDATE okg.schema_migrations SET result_metadata = "
            "jsonb_set(result_metadata, "
            "'{executable_schema_checksum}', '\"force-schema-replay\"') "
            "WHERE migration_id = '0000_schema_snapshot'"
        )

    try:
        migrate(okg_migrated)

        with db_connect(okg_migrated) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT data_type FROM information_schema.columns "
                "WHERE table_schema = 'okg' "
                "AND table_name = 'ir_program_bindings' "
                "AND column_name = 'generation_id'"
            )
            assert cur.fetchone() == ("text",)
            cur.execute(
                "SELECT generation_id FROM okg.ir_program_bindings "
                "WHERE deployment_name = 'd2-ir-binding'"
            )
            assert cur.fetchone() == (
                "okg:d2:branch:default:gen:20260809T000000000000Z:deadbeef",
            )
            # The dedupe index and its view survive the replay intact.
            cur.execute("SELECT to_regclass('okg.ir_program_bindings_dedupe_idx')")
            assert cur.fetchone() == ("okg.ir_program_bindings_dedupe_idx",)
            cur.execute("SELECT to_regclass('okg.v_ir_program_bindings')")
            assert cur.fetchone() == ("okg.v_ir_program_bindings",)
            cur.execute(
                "SELECT data_type FROM information_schema.columns "
                "WHERE table_schema = 'okg' "
                "AND table_name = 'ir_invocations' "
                "AND column_name = 'generation_id'"
            )
            assert cur.fetchone() == ("text",)
    finally:
        with (
            db_connect(
                okg_migrated,
                autocommit=True,
            ) as conn,
            conn.cursor() as cur,
        ):
            cur.execute(
                "DELETE FROM okg.ir_programs "
                "WHERE program_checksum = 'd2-ir-binding-generation-ref'"
            )


def _ir_program_checksums(dsn: str) -> set[str]:
    with db_connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT program_checksum FROM okg.ir_programs")
        return {row[0] for row in cur.fetchall()}


def test_ir_binding_migrate_tests_are_rerunnable_in_one_session(
    okg_migrated: str,
) -> None:
    """The IR-binding tests must add no lasting row to okg.ir_programs.

    ``program_checksum`` is UNIQUE and ``okg_migrated`` is a session-scoped
    SHARED database, so a seed row that outlives its test makes the NEXT run of
    this file abort on ``ir_programs_program_checksum_key``. That defect is
    invisible to a single run -- each test passes the first time -- so this
    calls the seeding test a second time within one session and then requires
    the table to hold exactly the checksums it held before.

    The contract enforced is the observable one: no orphan seed rows. It is
    taken as a before/after snapshot rather than a hand-maintained list of
    literals, so a test that starts seeding a NEW checksum is covered without
    anyone remembering to register it.

    The three legacy bigint-convergence tests this guard also covered were
    deleted with the schema.sql DO block they exercised (D2 makes TEXT the
    canonical type, so there is no bigint convergence left to converge); the
    guard itself stays, pointed at the seeding test that survived.
    """
    before = _ir_program_checksums(okg_migrated)

    test_schema_replay_keeps_the_ir_binding_generation_ref_text(okg_migrated)

    leaked = sorted(_ir_program_checksums(okg_migrated) - before)
    assert not leaked, f"seed rows outlived their tests: {leaked}"


def _restore_chronos_cutover_test_state(
    dsn: str,
    *,
    deployment_name: str,
    source: str,
) -> None:
    """Remove every shared-database object created by the cutover rehearsal."""
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "DROP TRIGGER IF EXISTS edge_facts_narrowing_check ON okg.edge_facts"
        )
        cur.execute("DROP FUNCTION IF EXISTS okg._legacy_edge_fact_guard()")
        cur.execute("DROP TABLE IF EXISTS okg.nodes_live, okg.edges_live CASCADE")
        cur.execute("DELETE FROM okg.edge_facts WHERE source = %s", (source,))
        for table in (
            "subtype_ancestors",
            "narrowings",
            "subtypes",
        ):
            cur.execute(
                f"DELETE FROM okg.{table} "  # noqa: S608 - fixed table allowlist
                "WHERE catalog_version_id IN ("
                "SELECT catalog_version_id FROM okg.catalog_versions "
                "WHERE deployment_name = %s)",
                (deployment_name,),
            )
        cur.execute(
            "DELETE FROM okg.catalog_versions WHERE deployment_name = %s",
            (deployment_name,),
        )

        cur.execute(
            """
            SELECT
              (SELECT count(*)
                 FROM pg_trigger
                WHERE tgname = 'edge_facts_narrowing_check'
                  AND tgrelid = 'okg.edge_facts'::regclass
                  AND NOT tgisinternal),
              to_regprocedure('okg._legacy_edge_fact_guard()'),
              to_regclass('okg.nodes_live'),
              to_regclass('okg.edges_live'),
              (SELECT count(*)
                 FROM okg.catalog_versions
                WHERE deployment_name = %s),
              (SELECT count(*) FROM okg.edge_facts WHERE source = %s)
            """,
            (deployment_name, source),
        )
        assert cur.fetchone() == (0, None, None, None, 0, 0)


@pytest.mark.parametrize("_same_session_run", (1, 2))
def test_chronos_cutover_ignores_dirty_legacy_live_rows(
    okg_migrated: str,
    _same_session_run: int,
) -> None:
    """A real legacy guard must reject before Chronos retires that guard."""
    deployment_name = "test-migrate-issue-1088"
    source = "test:migrate:issue-1088"
    parent_subtype = "issue_1088_parent"
    child_subtype = "issue_1088_child"

    try:
        with (
            db_connect(
                okg_migrated,
                autocommit=True,
            ) as conn,
            conn.cursor() as cur,
        ):
            cur.execute("DROP TABLE IF EXISTS okg.nodes_live, okg.edges_live CASCADE")
            cur.execute(
                """
                CREATE TABLE okg.nodes_live (
                  node_id text,
                  subtype text,
                  valid_from_gen bigint,
                  valid_to_gen bigint
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE okg.edges_live (
                  edge_id bigint,
                  src text,
                  dst text,
                  valid_from_gen bigint,
                  valid_to_gen bigint
                )
                """
            )
            cur.execute(
                """
                INSERT INTO okg.nodes_live
                  (node_id, subtype, valid_from_gen, valid_to_gen)
                VALUES
                  ('issue-1088:parent', %s, 1, NULL),
                  ('issue-1088:child', %s, 1, NULL),
                  ('dirty', %s, 10, 9)
                """,
                (parent_subtype, child_subtype, parent_subtype),
            )
            cur.execute(
                "INSERT INTO okg.edges_live VALUES (1, 'missing:a', 'missing:b', 10, 9)"
            )
            cur.execute(
                """
                INSERT INTO okg.catalog_versions
                  (status, applied_at, deployment_name, catalog_fingerprint)
                VALUES ('applied', now(), %s, 'issue-1088-catalog')
                RETURNING catalog_version_id
                """,
                (deployment_name,),
            )
            catalog_version_id = cur.fetchone()[0]
            cur.execute(
                """
                INSERT INTO okg.subtypes (name, catalog_version_id)
                VALUES (%s, %s), (%s, %s)
                """,
                (
                    parent_subtype,
                    catalog_version_id,
                    child_subtype,
                    catalog_version_id,
                ),
            )
            # A non-empty whitelist activates the historical strict check. The
            # edge inserted below deliberately uses a different edge type.
            cur.execute(
                """
                INSERT INTO okg.narrowings
                  (parent_subtype, edge_type, child_subtype, catalog_version_id)
                VALUES (%s, 'issue_1088_allowed', %s, %s)
                """,
                (parent_subtype, child_subtype, catalog_version_id),
            )
            cur.execute(
                """
                CREATE OR REPLACE FUNCTION okg._legacy_edge_fact_guard()
                RETURNS trigger
                LANGUAGE plpgsql
                AS $$
                DECLARE
                  cv bigint;
                  src_subtype text;
                  dst_subtype text;
                  n_narrowings_at_cv bigint;
                  allowed boolean;
                BEGIN
                  IF NEW.op <> 'I' THEN
                    RETURN NEW;
                  END IF;

                  SELECT MAX(catalog_version_id) INTO cv
                    FROM okg.catalog_versions
                   WHERE status = 'applied';
                  IF cv IS NULL THEN
                    RETURN NEW;
                  END IF;

                  SELECT count(*) INTO n_narrowings_at_cv
                    FROM okg.narrowings
                   WHERE catalog_version_id = cv;
                  IF n_narrowings_at_cv = 0 THEN
                    RETURN NEW;
                  END IF;

                  SELECT subtype INTO src_subtype
                    FROM okg.nodes_live
                   WHERE node_id = NEW.src
                   ORDER BY valid_from_gen DESC
                   LIMIT 1;
                  SELECT subtype INTO dst_subtype
                    FROM okg.nodes_live
                   WHERE node_id = NEW.dst
                   ORDER BY valid_from_gen DESC
                   LIMIT 1;

                  IF src_subtype IS NULL OR dst_subtype IS NULL THEN
                    RAISE EXCEPTION
                      'narrowing_violation: src (%) or dst (%) has no '
                      'nodes_live row at commit',
                      NEW.src, NEW.dst
                      USING ERRCODE = 'check_violation';
                  END IF;

                  SELECT EXISTS (
                    SELECT 1
                      FROM okg.narrowings n
                     WHERE n.parent_subtype = src_subtype
                       AND n.edge_type = NEW.edge_type
                       AND n.child_subtype = dst_subtype
                       AND n.catalog_version_id = cv
                  ) INTO allowed;
                  IF NOT allowed THEN
                    RAISE EXCEPTION
                      'narrowing_violation: (%, %, %) not in narrowings '
                      'at catalog_version %',
                      src_subtype, NEW.edge_type, dst_subtype, cv
                      USING ERRCODE = 'check_violation';
                  END IF;

                  RETURN NEW;
                END;
                $$
                """
            )
            cur.execute(
                "DROP TRIGGER IF EXISTS edge_facts_narrowing_check ON okg.edge_facts"
            )
            cur.execute(
                """
                CREATE CONSTRAINT TRIGGER edge_facts_narrowing_check
                  AFTER INSERT ON okg.edge_facts
                  DEFERRABLE INITIALLY DEFERRED
                  FOR EACH ROW EXECUTE FUNCTION okg._legacy_edge_fact_guard()
                """
            )

        with pytest.raises(
            psycopg.errors.CheckViolation,
            match="narrowing_violation",
        ) as exc_info:
            with db_connect(okg_migrated) as conn, conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO okg.edge_facts
                      (edge_id, src, dst, edge_type, provenance, source,
                       source_record_id, source_revision, observed_at,
                       dedupe_key, run_id, op)
                    VALUES
                      (nextval('okg.edge_facts_edge_id_seq'),
                       'issue-1088:parent', 'issue-1088:child',
                       'issue_1088_forbidden', 'explicit', %s,
                       '{"test":"issue-1088"}'::jsonb, '{"rev":1}'::jsonb,
                       now(), 'issue-1088-forbidden', gen_random_uuid(), 'I')
                    """,
                    (source,),
                )
        assert exc_info.value.sqlstate == "23514"

        migrate(okg_migrated)

        with db_connect(okg_migrated) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  to_regclass('okg.nodes_live'),
                  to_regclass('okg.edges_live'),
                  (SELECT count(*)
                     FROM pg_trigger
                    WHERE tgname = 'edge_facts_narrowing_check'
                      AND tgrelid = 'okg.edge_facts'::regclass
                      AND NOT tgisinternal)
                """
            )
            assert cur.fetchone() == (None, None, 0)
    finally:
        _restore_chronos_cutover_test_state(
            okg_migrated,
            deployment_name=deployment_name,
            source=source,
        )


def test_chronos_schema_keeps_fact_and_generation_tables() -> None:
    """The executable schema is schema.sql itself, and it kept the right half.

    Until `schema-canonical-read-path` this asserted the same property about
    the OUTPUT of `schema_without_legacy_live_table_compatibility`, a Python
    filter that stripped the legacy live complex out of schema.sql on the way
    to the database. The filter is gone because the file no longer contains
    what it stripped, so the same assertions now run against the file — which
    is what `migrate` executes.
    """
    sql = _schema_sql()
    executable_sql = _without_sql_line_comments(sql)

    assert "CREATE TABLE IF NOT EXISTS okg.node_facts" in sql
    assert "CREATE TABLE IF NOT EXISTS okg.edge_facts" in sql
    assert "CREATE TABLE IF NOT EXISTS okg.graph_generations" in sql
    for legacy_pattern in (
        "CREATE TABLE IF NOT EXISTS okg.nodes_live",
        "CREATE TABLE IF NOT EXISTS okg.edges_live",
        "ALTER TABLE okg.nodes_live",
        "ALTER TABLE okg.edges_live",
        "FROM okg.nodes_live",
        "FROM okg.edges_live",
        "ON okg.nodes_live",
        "ON okg.edges_live",
    ):
        assert legacy_pattern not in executable_sql


def test_migrate_idempotent_with_compressed_mcp_trace_chunks(
    okg_migrated: str,
) -> None:
    """Regression (fix-migrate-compressed-hypertable-idempotency).

    The whole-schema re-exec must not issue a table-rewrite ALTER against a
    hypertable that already has compressed chunks. schema.sql's legacy
    ``okg.mcp_trace_events`` ``generation_id``->TEXT conversion previously ran
    unconditionally and raised ``FeatureNotSupported`` ("operation not
    supported on hypertables with compressed chunks") once the compression
    policy had compressed a chunk -- breaking ``okg migrate`` on every aged
    deployment. Guarding the ALTER behind a type check keeps re-exec a no-op.
    """

    def _compressed_chunks(cur: psycopg.Cursor) -> int:
        cur.execute(
            "SELECT count(*) FROM timescaledb_information.chunks "
            "WHERE hypertable_name = 'mcp_trace_events' AND is_compressed"
        )
        return cur.fetchone()[0]

    # Arrange: land a row in an old chunk and compress it.
    with db_connect(okg_migrated, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.mcp_trace_events "
            "  (session_id, deployment_name, tool_name, event_time) "
            "VALUES ('regress', 'regress', 't', now() - interval '60 days')"
        )
        cur.execute(
            "SELECT compress_chunk(c) FROM show_chunks('okg.mcp_trace_events') c"
        )
        assert _compressed_chunks(cur) >= 1, "setup: expected a compressed chunk"

    try:
        # Act: re-running migrate must not raise on the compressed hypertable.
        migrate(okg_migrated)
        # Assert: the guard SKIPPED the ALTER (generation_id is already TEXT),
        # so no rewrite touched the chunk -- it stays compressed.
        with db_connect(okg_migrated) as conn, conn.cursor() as cur:
            assert _compressed_chunks(cur) >= 1, (
                "migrate decompressed/rewrote a compressed chunk; the legacy "
                "ALTER COLUMN TYPE should have been skipped entirely"
            )
    finally:
        # Restore the shared session DB: decompress + drop the test row.
        with (
            db_connect(
                okg_migrated,
                autocommit=True,
            ) as conn,
            conn.cursor() as cur,
        ):
            cur.execute(
                "SELECT decompress_chunk(c, true) "
                "FROM show_chunks('okg.mcp_trace_events') c"
            )
            cur.execute(
                "DELETE FROM okg.mcp_trace_events "
                "WHERE session_id = 'regress' AND deployment_name = 'regress'"
            )


def test_migrate_idempotent_with_compressed_mcp_call_log_chunks(
    okg_migrated: str,
) -> None:
    """Regression (guard-migrate-mcp-call-log-compressed-alter).

    views.sql's ``okg.mcp_call_log`` ``ALTER COLUMN pin TYPE TEXT`` (and the
    additive ADD COLUMN block) ran unconditionally on every whole-schema
    re-exec. ``ALTER COLUMN ... TYPE`` forces a table rewrite, which TimescaleDB
    rejects on a hypertable that already has compressed chunks
    (``FeatureNotSupported``: "operation not supported on hypertables with
    compressed chunks") -- breaking ``okg migrate`` on every aged deployment
    once the 7-day compression policy has compressed a chunk. The
    ``mcp_trace_events`` sibling was guarded in
    ``fix-migrate-compressed-hypertable-idempotency``, but this views.sql site
    was missed (that guard audit covered only schema.sql). Guarding the ALTER
    behind a type check keeps re-exec a no-op; the chunk stays compressed.
    """

    def _compressed_chunks(cur: psycopg.Cursor) -> int:
        cur.execute(
            "SELECT count(*) FROM timescaledb_information.chunks "
            "WHERE hypertable_name = 'mcp_call_log' AND is_compressed"
        )
        return cur.fetchone()[0]

    # Arrange: land a row in an old chunk and compress it.
    with db_connect(okg_migrated, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.mcp_call_log (session_id, tool_name, ts) "
            "VALUES ('regress', 't', now() - interval '60 days')"
        )
        cur.execute("SELECT compress_chunk(c) FROM show_chunks('okg.mcp_call_log') c")
        assert _compressed_chunks(cur) >= 1, "setup: expected a compressed chunk"

    try:
        # Act: re-running migrate must not raise on the compressed hypertable.
        migrate(okg_migrated)
        # Assert: the guards SKIPPED the rewrite (pin is already TEXT, the
        # compat columns already exist), so the chunk stays compressed.
        with db_connect(okg_migrated) as conn, conn.cursor() as cur:
            assert _compressed_chunks(cur) >= 1, (
                "migrate decompressed/rewrote a compressed chunk; the "
                "ALTER COLUMN pin TYPE / ADD COLUMN block should have been "
                "skipped entirely"
            )
    finally:
        # Restore the shared session DB: decompress + drop the test row.
        with (
            db_connect(
                okg_migrated,
                autocommit=True,
            ) as conn,
            conn.cursor() as cur,
        ):
            cur.execute(
                "SELECT decompress_chunk(c, true) "
                "FROM show_chunks('okg.mcp_call_log') c"
            )
            cur.execute("DELETE FROM okg.mcp_call_log WHERE session_id = 'regress'")


def test_migrate_refuses_foreign_checkout_snapshot_write(monkeypatch) -> None:
    """Authority gate (guard-migrate-canonical-snapshot-authority).

    migrate() must refuse to (re-)record a deployment's canonical schema
    snapshot from a non-authoritative checkout, BEFORE touching the database.
    Regression for the 2026-06-14 drift: a foreign worktree ran
    `okg up okg-workspace` against the canonical DB, re-recorded
    0000_schema_snapshot with its own schema.sql checksum, and crash-looped the
    runtime worker on a schema-version skew.
    """
    from okg.substrate.errors import SubstrateError

    def _foreign(deployment, dsn, **kwargs):
        return {
            "allowed": False,
            "mode": "foreign_checkout",
            "reason": f"checkout /tmp/other is not the sync root for {deployment}",
            "legal_moves": [f"okg up {deployment} --target dev"],
        }

    monkeypatch.setattr(
        "okg.substrate.deployment_authority.check_publish_provenance", _foreign
    )

    # A deliberately unreachable DSN proves the guard fires BEFORE psycopg.connect.
    with pytest.raises(SubstrateError) as excinfo:
        migrate(
            "postgres://nobody:nopass@127.0.0.1:1/does_not_exist",
            deployment_name="okg-workspace",
        )
    msg = str(excinfo.value)
    assert "foreign_checkout" in msg and "refusing migrate" in msg


def test_migrate_allows_sync_root_snapshot_write(monkeypatch) -> None:
    """The gate must NOT block the authoritative caller: an allowed provenance
    verdict lets migrate() proceed past the guard (it then fails only on the
    unreachable DSN's connect, not on the authority gate)."""

    def _allowed(deployment, dsn, **kwargs):
        return {"allowed": True, "mode": "sync_root", "reason": None}

    monkeypatch.setattr(
        "okg.substrate.deployment_authority.check_publish_provenance", _allowed
    )
    with pytest.raises(Exception) as excinfo:
        migrate(
            "postgres://nobody:nopass@127.0.0.1:1/does_not_exist",
            deployment_name="okg-workspace",
        )
    # Past the gate: the failure is the connect, NOT the authority refusal.
    assert "refusing migrate" not in str(excinfo.value)


def test_migrate_refuses_foreign_checkout_bare_dsn(monkeypatch) -> None:
    """Bare `okg migrate --dsn ...` with no --deployment must still be gated:
    migrate() derives the deployment from the target database and refuses a
    foreign checkout writing a known canonical deployment DB."""
    from okg.substrate.errors import SubstrateError

    monkeypatch.setattr(
        "okg.substrate.db.migrate._deployment_for_database",
        lambda dsn: "okg-workspace",
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_authority.check_publish_provenance",
        lambda deployment, dsn, **kw: {
            "allowed": False,
            "mode": "foreign_checkout",
            "reason": "foreign",
            "legal_moves": [],
        },
    )
    with pytest.raises(SubstrateError) as excinfo:
        migrate("postgres://nobody:nopass@127.0.0.1:1/okg_workspace")
    assert "refusing migrate" in str(excinfo.value)


def test_views_only_sql_skips_mcp_call_log_storage_ddl() -> None:
    sql = views_without_mcp_call_log_storage(_views_sql())

    assert "CREATE TABLE IF NOT EXISTS okg.mcp_call_log" not in sql
    assert "ALTER TABLE okg.mcp_call_log" not in sql
    assert "create_hypertable(\n  'okg.mcp_call_log'" not in sql
    assert "add_compression_policy(\n  'okg.mcp_call_log'" not in sql
    assert "add_retention_policy(\n  'okg.mcp_call_log'" not in sql
    assert "CREATE OR REPLACE VIEW okg.v_mcp_calls AS" in sql
    assert "CREATE OR REPLACE VIEW okg.v_mcp_call_costs AS" in sql
    assert "CREATE OR REPLACE VIEW okg.v_mcp_operator_costs AS" in sql


def test_install_views_only_refreshes_mcp_cost_views(okg_migrated: str) -> None:
    install_views(okg_migrated)

    with db_connect(okg_migrated) as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass('okg.v_mcp_operator_costs')")
        assert cur.fetchone()[0] == "okg.v_mcp_operator_costs"


def test_migrate_upgrades_legacy_mcp_calls_view(clean_runtime: str) -> None:
    """Re-running migrate must handle the pre-operator-surface
    v_mcp_calls shape, whose fourth column was tool_name."""
    old_view_sql = """
        CREATE VIEW okg.v_mcp_calls AS
        SELECT
          call_id,
          ts,
          session_id,
          tool_name,
          args,
          pin,
          elapsed_ms,
          result_count,
          error_code
        FROM okg.mcp_call_log;
    """
    with db_connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP VIEW IF EXISTS okg.v_mcp_operator_costs;")
            cur.execute("DROP VIEW IF EXISTS okg.v_mcp_call_costs;")
            cur.execute("DROP VIEW IF EXISTS okg.v_mcp_calls;")
            cur.execute(old_view_sql)

    migrate(clean_runtime)

    with db_connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
              FROM information_schema.columns
             WHERE table_schema = 'okg'
               AND table_name = 'v_mcp_calls'
             ORDER BY ordinal_position
            """
        )
        columns = [str(row[0]) for row in cur.fetchall()]

    assert columns[:7] == [
        "call_id",
        "ts",
        "session_id",
        "deployment_name",
        "tool_name",
        "operator_name",
        "replacement_operator",
    ]


def test_migrate_converges_legacy_external_action_deployment_columns(
    clean_runtime: str,
) -> None:
    """The physical identity rename must upgrade stored views as one unit.

    PostgreSQL updates a view's base-column dependency when the table column
    is renamed, but deliberately preserves the view's stored output name.
    Replaying views.sql therefore exercises the frozen-view convergence path:
    it must recreate the ten review views instead of aborting on the rename.
    """
    base_relations = (
        "external_action_proposals",
        "external_action_payload_versions",
        "external_action_auth_audits",
        "external_action_dpop_replay_nonces",
        "external_action_decisions",
        "external_action_transition_idempotency",
        "external_action_executions",
        "external_action_owner_unavailability_attestations",
        "external_action_outcomes",
        "external_action_events",
        "external_action_execution_epoch_leases",
        "external_action_restore_attestations",
        "external_action_restore_fences",
        "external_action_restore_sessions",
        "external_action_restore_completion_receipts",
        "external_action_restore_finalize_idempotency",
        "external_action_restore_checkpoint_staging",
        "external_action_restore_checkpoint_entries",
        "external_action_restore_recoveries",
        "external_action_api_role_bindings",
    )
    review_views = (
        "v_external_action_proposals",
        "v_external_action_payload_versions",
        "v_external_action_decisions",
        "v_external_action_events",
        "v_external_action_executions",
        "v_external_action_outcomes",
        "v_external_action_restore_fences",
        "v_external_action_restore_recoveries",
        "v_external_action_restore_sessions",
        "v_external_action_restore_session_terminal",
    )
    with db_connect(clean_runtime, autocommit=True) as conn:
        for relation in base_relations:
            conn.execute(
                sql.SQL(
                    "ALTER TABLE okg.{} RENAME COLUMN deployment_name TO deployment"
                ).format(sql.Identifier(relation))
            )
        for view in review_views:
            conn.execute(
                sql.SQL(
                    "ALTER VIEW okg.{} RENAME COLUMN deployment_name TO deployment"
                ).format(sql.Identifier(view))
            )
        conn.execute(
            "UPDATE okg.schema_migrations SET result_metadata = "
            "jsonb_set(result_metadata, "
            "'{executable_schema_checksum}', '\"force-schema-replay\"') "
            "WHERE migration_id = '0000_schema_snapshot'"
        )

    result = migrate(clean_runtime)

    assert set(result.converged_views).issuperset(
        {f"okg.{view}" for view in review_views}
    )
    with db_connect(clean_runtime) as conn:
        canonical_columns = conn.execute(
            """
            SELECT table_name
              FROM information_schema.columns
             WHERE table_schema = 'okg'
               AND table_name = ANY(%s)
               AND column_name = 'deployment_name'
            """,
            (list((*base_relations, *review_views)),),
        ).fetchall()
        legacy_columns = conn.execute(
            """
            SELECT table_name
              FROM information_schema.columns
             WHERE table_schema = 'okg'
               AND table_name = ANY(%s)
               AND column_name = 'deployment'
            """,
            (list((*base_relations, *review_views)),),
        ).fetchall()
    assert {row[0] for row in canonical_columns} == set(
        (*base_relations, *review_views)
    )
    assert legacy_columns == []


def test_required_extensions_installed(okg_migrated: str) -> None:
    with db_connect(okg_migrated) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT extname FROM pg_extension WHERE extname = ANY(%s)",
            (list(REQUIRED_EXTENSIONS.keys()),),
        )
        installed = {r[0] for r in cur.fetchall()}
    for name in REQUIRED_EXTENSIONS:
        assert name in installed, f"{name} not installed"


def test_fact_tables_are_hypertables(okg_migrated: str) -> None:
    with db_connect(okg_migrated) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT hypertable_name FROM timescaledb_information.hypertables "
            "WHERE hypertable_schema = 'okg' "
            "ORDER BY hypertable_name;"
        )
        hts = [r[0] for r in cur.fetchall()]
    # Base migrate fact tables + observability hypertables.
    # `escalation_facts` belongs to runtime bootstrap's
    # escalation_schema.sql, not the base substrate migrate.
    # `node_series` is the external-backed-nodes raw-sample hypertable
    # (schema.sql, issue #83); it IS created by the base migrate.
    # `call_sample_hot` / `call_sample_warm` / `call_sample_cold` are the three
    # named operator-call-sample retention classes (schema.sql, issue #2011),
    # one hypertable per class; they too come from the base migrate.
    assert hts == [
        "call_sample_cold",
        "call_sample_hot",
        "call_sample_warm",
        "edge_facts",
        "mcp_call_log",
        "mcp_trace_events",
        "node_facts",
        "node_series",
        "telemetry_events",
        "telemetry_metrics",
        "telemetry_spans",
    ], hts


def test_mcp_call_view_preserves_observability_columns(
    okg_migrated: str,
) -> None:
    with db_connect(okg_migrated) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
              FROM information_schema.columns
             WHERE table_schema = 'okg'
               AND table_name = 'v_mcp_calls'
             ORDER BY ordinal_position
            """
        )
        columns = [row[0] for row in cur.fetchall()]
    assert columns == [
        "call_id",
        "ts",
        "session_id",
        "deployment_name",
        "tool_name",
        "operator_name",
        "replacement_operator",
        "args",
        "cost",
        "pin",
        "elapsed_ms",
        "result_count",
        "error_code",
    ]


def test_retention_and_compression_policies_exist(okg_migrated: str) -> None:
    with db_connect(okg_migrated) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT application_name FROM timescaledb_information.jobs "
            "WHERE hypertable_schema = 'okg';"
        )
        policy_names = [r[0] for r in cur.fetchall()]
    # Each fact table has a retention + a compression (columnstore)
    # policy — four total.
    assert len(policy_names) >= 4, policy_names


def test_seed_catalog_version_exists(okg_migrated: str) -> None:
    with db_connect(okg_migrated) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT status FROM okg.catalog_versions WHERE catalog_version_id = 1;"
        )
        row = cur.fetchone()
    assert row is not None, "seed catalog_version(id=1) missing"
    assert row[0] == "applied"


def test_bootstrap_generation_zero_exists(okg_migrated: str) -> None:
    with db_connect(okg_migrated) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT deployment_name, status, catalog_version_id "
            "  FROM okg.graph_generations "
            " WHERE generation_id = 0"
        )
        row = cur.fetchone()
        cur.execute(
            "SELECT nextval(pg_get_serial_sequence("
            "  'okg.graph_generations', 'generation_id'))"
        )
        next_id = int(cur.fetchone()[0])
    assert row == ("bootstrap", "published", 1)
    assert next_id > 0


# ---------------------------------------------------------------------------
# OPTIONAL_EXTENSIONS handling (substrate retired pg_cron use; missing
# pg_cron must NOT fail migrate). Per the synth-bench commit-replay
# benchmark — fresh-DB migrate against a Postgres cluster whose
# `cron.database_name` is bound elsewhere.
# ---------------------------------------------------------------------------


def test_optional_extensions_includes_pg_cron():
    """Sanity: pg_cron is in OPTIONAL_EXTENSIONS, not REQUIRED."""
    from okg.substrate.db.migrate import (
        OPTIONAL_EXTENSIONS,
        REQUIRED_EXTENSIONS,
    )

    assert "pg_cron" in OPTIONAL_EXTENSIONS
    assert "pg_cron" not in REQUIRED_EXTENSIONS


def test_verify_extensions_tolerates_missing_optional():
    """pg_cron not installed → _verify_extensions returns without
    raising. Returned dict omits pg_cron; all required extensions
    are present."""
    from okg.substrate.db import migrate as _m

    class _FakeCursor:
        def __init__(self):
            self.rows = []

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql, params=None):
            # All REQUIRED extensions installed; pg_cron absent.
            self.rows = [(name, ver) for name, ver in _m.REQUIRED_EXTENSIONS.items()]

        def fetchall(self):
            return self.rows

    class _FakeConn:
        def cursor(self):
            return _FakeCursor()

    installed = _m._verify_extensions(_FakeConn())
    assert "pg_cron" not in installed
    for name in _m.REQUIRED_EXTENSIONS:
        assert name in installed


def test_verify_extensions_still_fails_on_missing_required():
    """A REQUIRED extension missing still raises ExtensionMissing."""
    import pytest as _pytest
    from okg.substrate.db import migrate as _m
    from okg.substrate.errors import ExtensionMissing

    class _FakeCursor:
        def __init__(self):
            self.rows = []

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql, params=None):
            # `vector` is missing on purpose.
            self.rows = [
                (name, ver)
                for name, ver in _m.REQUIRED_EXTENSIONS.items()
                if name != "vector"
            ]

        def fetchall(self):
            return self.rows

    class _FakeConn:
        def cursor(self):
            return _FakeCursor()

    with _pytest.raises(ExtensionMissing) as exc_info:
        _m._verify_extensions(_FakeConn())
    assert "vector" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Issue #1968 -- a role whose NAME matches a schema must not shadow `public`.
# ---------------------------------------------------------------------------


def _maintenance_dsn(dsn: str) -> str:
    """The same server, but connected to `postgres`, for CREATE/DROP DATABASE."""
    info = psycopg.conninfo.conninfo_to_dict(dsn)
    info["dbname"] = "postgres"
    return psycopg.conninfo.make_conninfo(**info)


def _dsn_for_database(dsn: str, dbname: str) -> str:
    info = psycopg.conninfo.conninfo_to_dict(dsn)
    info["dbname"] = dbname
    return psycopg.conninfo.make_conninfo(**info)


def _probe_unqualified_create(conn) -> tuple[bool, bool]:
    """Create an unqualified `_chronos`-style table; say where it landed.

    Returns `(in_public, in_okg)`.
    """
    # `tuple_row` is explicit because the Chronos adapter's connection is
    # opened with `dict_row`, under which positional access raises KeyError.
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute("CREATE TABLE _chronos_search_path_probe (id int)")
        cur.execute(
            "SELECT to_regclass('public._chronos_search_path_probe') IS NOT NULL,"
            "       to_regclass('okg._chronos_search_path_probe') IS NOT NULL"
        )
        row = cur.fetchone()
    return bool(row[0]), bool(row[1])


def test_session_search_path_pin_keeps_unqualified_ddl_in_public(
    okg_dsn: str,
) -> None:
    """A session default that puts `okg` first must not move Chronos's tables.

    This is the mechanism behind issue #1968. A login role named `okg` makes
    PostgreSQL expand the factory-default `"$user", public` to `okg, public`,
    so unqualified `_chronos_*` DDL is created in `okg` while the substrate's
    51 `public._chronos*` readers keep looking in `public`, and a fresh
    `okg migrate --apply` dies partway through.

    The test reproduces that hostile resolution order with a DATABASE-level
    `search_path` default instead of a cluster role named `okg`: it is the
    same session default, reached without creating a cluster-wide role that
    would outlive the test and collide with a real deployment's. The
    end-to-end proof under an actual login role named `okg` is the evidence
    artifact attached to this change.

    Both connection paths are checked: okg's own factory, and Chronos's
    branching adapter, which opens its own connections and so needs the same
    pin rather than inheriting one.
    """
    from chronos_core.branching.sql_adapters import PostgresDatabaseAdapter

    maintenance = _maintenance_dsn(okg_dsn)
    try:
        with psycopg.connect(maintenance, connect_timeout=5):
            pass
    except Exception as exc:  # noqa: BLE001 - any connect failure means skip
        pytest.skip(f"Postgres not reachable at {okg_dsn}: {exc}")

    dbname = f"okg_test_searchpath_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(maintenance, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
    scratch = _dsn_for_database(okg_dsn, dbname)
    try:
        with psycopg.connect(scratch, autocommit=True) as setup:
            setup.execute("CREATE SCHEMA okg")
            setup.execute(
                sql.SQL("ALTER DATABASE {} SET search_path = okg, public").format(
                    sql.Identifier(dbname)
                )
            )
        # The hostile default really is in force for a session that does not
        # pin: without this the test could pass for the wrong reason.
        with psycopg.connect(scratch) as unpinned:
            in_public, in_okg = _probe_unqualified_create(unpinned)
            assert (in_public, in_okg) == (False, True), (
                "the hostile session default was not reproduced; "
                f"in_public={in_public} in_okg={in_okg}"
            )
            unpinned.rollback()

        with db_connect(scratch, autocommit=True) as conn:
            in_public, in_okg = _probe_unqualified_create(conn)
            assert in_public and not in_okg, (
                "okg's connection factory did not pin the session search_path: "
                f"in_public={in_public} in_okg={in_okg}"
            )
            conn.execute("DROP TABLE public._chronos_search_path_probe")

        adapter = PostgresDatabaseAdapter.connect(scratch)
        try:
            in_public, in_okg = _probe_unqualified_create(adapter._conn)
            assert in_public and not in_okg, (
                "the Chronos branching adapter did not pin the session "
                f"search_path: in_public={in_public} in_okg={in_okg}"
            )
        finally:
            adapter._conn.rollback()
            adapter._conn.close()
    finally:
        with psycopg.connect(maintenance, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                    sql.Identifier(dbname)
                )
            )


def test_a_dsn_supplied_search_path_still_overrides_the_pin() -> None:
    """The pin is a default, not a lock: an explicit DSN setting must win.

    libpq applies repeated `-c` settings left to right, so the factory has to
    emit its own pin BEFORE the DSN's own options, not after.
    """
    from okg.substrate.db.connection import connection_kwargs

    options = connection_kwargs(
        dsn="postgresql://u@h/db?options=-csearch_path%3Dtenant_a"
    )["options"]

    assert options.index("-c search_path=public") < options.index(
        "-csearch_path=tenant_a"
    )


_PROBE_ROLE = "okg"
_PROBE_ROLE_PASSWORD = "okg_search_path_probe"


def _unreachable_probe_role(reason: str) -> None:
    """Refuse in CI, skip locally, and always say why.

    This test only exercises the defect when it can log in as a role named
    `okg`. Skipping when it cannot is right on a developer's machine, where a
    role of that name may already exist with a password this test does not
    know. In CI it is wrong: the server there is the one the bug lives on, and
    a silent skip would report a green lane for a check that never ran. `CI` is
    the variable the rest of this suite already uses for that distinction.
    """
    message = f"role {_PROBE_ROLE!r} unreachable: {reason}"
    if os.environ.get("CI"):
        pytest.fail(
            message
            + " -- in CI this is a failure, not a skip: the role-named-okg arm "
            "is the whole point of this test (issue #1968)."
        )
    print(f"SKIP REASON: {message}")
    pytest.skip(message)


def _ensure_probe_login_role(maintenance: str) -> bool:
    """Make sure a login role named `okg` exists. True iff this call made it.

    The role name is the whole mechanism, so it cannot be randomised. It is
    created only when absent and dropped only by the caller that created it,
    so a server where a real deployment already owns the name is left alone.
    """
    with psycopg.connect(maintenance, autocommit=True) as admin:
        with admin.cursor(row_factory=tuple_row) as cur:
            cur.execute(
                "SELECT 1 FROM pg_roles WHERE rolname = %s", (_PROBE_ROLE,)
            )
            if cur.fetchone() is not None:
                return False
        admin.execute(
            sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                sql.Identifier(_PROBE_ROLE), sql.Literal(_PROBE_ROLE_PASSWORD)
            )
        )
    return True


def _run_interval_metadata_ddl(dsn: str) -> tuple[str, tuple[str, ...]]:
    """Run the Chronos interval backend's metadata DDL twice over one session.

    This is the `table_defs` -> `ALTER TABLE ... ADD COLUMN` path that fails
    from both sides when the session search_path is wrong: with `okg` ahead of
    `public` the CREATE lands in the wrong schema, and with `public` missing
    altogether `table_defs` reports no columns for a table that does exist, so
    the ALTER is re-issued and PostgreSQL raises DuplicateColumn. Running it
    twice is what exposes the second failure.

    Returns the schema the metadata table ended up in and its columns.
    """
    from chronos_core.branching._interval_backend import _IntervalBackend
    from chronos_core.branching.sql_adapters import PostgresDatabaseAdapter

    adapter = PostgresDatabaseAdapter.connect(dsn)
    try:
        _IntervalBackend(adapter).ensure()
        _IntervalBackend(adapter).ensure()
        adapter._conn.commit()
        with adapter._conn.cursor(row_factory=tuple_row) as cur:
            cur.execute(
                "SELECT table_schema FROM information_schema.tables "
                "WHERE table_name = '_chronos_branch_interval_branches'"
            )
            schemas = sorted(row[0] for row in cur.fetchall())
        columns, _defs = adapter.table_defs("_chronos_branch_interval_branches")
    finally:
        adapter._conn.close()
    assert len(schemas) == 1, f"expected exactly one copy of the table, got {schemas}"
    return schemas[0], columns


@pytest.mark.parametrize("as_role_okg", [False, True], ids=["ordinary_role", "role_okg"])
def test_chronos_metadata_ddl_survives_a_role_named_okg(
    okg_dsn: str, as_role_okg: bool
) -> None:
    """The interval backend's metadata DDL must work under either login role.

    Acceptance for issue #1968: the pinned session search_path has to contain
    `public` and resolve it first.

    Dropping `public` breaks this path from the other side. `table_defs` looks
    a bare name up along the search path, so without `public` it reports no
    columns for a table that does exist, the backend concludes the column is
    absent and re-issues the ALTER. Which error surfaces depends on what IS
    first: with the session pinned to `pg_catalog, okg` this test fails with
    `InsufficientPrivilege: permission denied to create
    "pg_catalog._chronos_branch_tables"`, because the CREATE lands in
    `pg_catalog`; where `public` is merely shadowed by a role-named schema it
    is `DuplicateColumn` instead. The negative control run for this change
    produced the first of those.
    """
    maintenance = _maintenance_dsn(okg_dsn)
    try:
        with psycopg.connect(maintenance, connect_timeout=5):
            pass
    except Exception as exc:  # noqa: BLE001 - any connect failure means skip
        pytest.skip(f"Postgres not reachable at {okg_dsn}: {exc}")

    created_role = False
    if as_role_okg:
        try:
            created_role = _ensure_probe_login_role(maintenance)
        except psycopg.Error as exc:
            _unreachable_probe_role(f"cannot create it ({exc})")

    dbname = f"okg_test_chronosddl_{uuid.uuid4().hex[:12]}"
    owner = _PROBE_ROLE if as_role_okg else None
    with psycopg.connect(maintenance, autocommit=True) as admin:
        statement = sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname))
        if owner:
            statement = statement + sql.SQL(" OWNER {}").format(sql.Identifier(owner))
        admin.execute(statement)
    try:
        # The okg schema must exist for the collision to be possible at all;
        # a real migration creates it.
        with psycopg.connect(_dsn_for_database(okg_dsn, dbname), autocommit=True) as s:
            s.execute("CREATE SCHEMA IF NOT EXISTS okg")

        info = psycopg.conninfo.conninfo_to_dict(okg_dsn)
        info["dbname"] = dbname
        if as_role_okg:
            info["user"] = _PROBE_ROLE
            info["password"] = _PROBE_ROLE_PASSWORD
        probe_dsn = psycopg.conninfo.make_conninfo(**info)
        if as_role_okg:
            # Separate "this server will not let us log in as okg" from
            # anything the pin itself does: this connection carries NO options,
            # so it can only fail on authentication. Every later connection
            # failure is therefore a real result and is left to raise.
            try:
                with psycopg.connect(probe_dsn, connect_timeout=5):
                    pass
            except psycopg.OperationalError as exc:
                _unreachable_probe_role(
                    f"cannot log in (role pre-existed: {not created_role}): "
                    f"{exc}"
                )
        schema, columns = _run_interval_metadata_ddl(probe_dsn)

        assert schema == "public", (
            "the Chronos metadata table must stay in public; it landed in "
            f"{schema!r}"
        )
        assert "parent_branch_id" in columns and "child_count" in columns, (
            "table_defs did not see the columns the backend just created, so a "
            f"repeat run would re-issue the ALTER; columns={columns}"
        )
    finally:
        with psycopg.connect(maintenance, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                    sql.Identifier(dbname)
                )
            )
            if created_role:
                admin.execute(
                    sql.SQL("DROP ROLE IF EXISTS {}").format(
                        sql.Identifier(_PROBE_ROLE)
                    )
                )
def test_table_defs_reads_the_search_path_schema_not_literal_public(
    clean_runtime: str,
) -> None:
    """Issue #1922: a fresh migration dies when the role is named `okg`.

    `okg migrate` creates a schema named `okg`. With Postgres's default
    `search_path` of `"$user", public`, a login role *also* named `okg`
    then resolves every unqualified table in that schema. Chronos's
    `table_defs` hardcoded `public`, so the "does this column exist yet?"
    guard in `_ensure_interval_physical_storage_columns` read an unrelated
    (usually empty) column set, re-issued the ALTER, and Postgres raised
    `DuplicateColumn: column "parent_branch_id" ... already exists`.

    The role name is not the mechanism — the `search_path` is. This drives
    the same shadowing with an explicit `search_path`, so it needs no extra
    role and proves the resolution, not the naming coincidence.
    """
    from chronos_core.branching.sql_adapters import PostgresDatabaseAdapter

    adapter = PostgresDatabaseAdapter.connect(clean_runtime)
    try:
        adapter.execute("CREATE SCHEMA shadow_1922")
        adapter.execute("CREATE TABLE public.shadowed_1922 (node_id TEXT)")
        adapter.execute(
            "CREATE TABLE shadow_1922.shadowed_1922 "
            "(node_id TEXT, parent_branch_id TEXT)"
        )
        adapter.commit()

        adapter.execute("SET search_path TO shadow_1922, public")

        # The unqualified read must reach the table the surrounding DDL
        # reaches — the one on the search path, not the one in `public`.
        columns, defs = adapter.table_defs("shadowed_1922")
        assert "parent_branch_id" in columns, columns
        assert len(defs) == len(columns)

        # An explicitly qualified name still reads exactly that schema.
        assert adapter.table_defs("public.shadowed_1922")[0] == ("node_id",)

        # A table that resolves nowhere still reports nothing.
        assert adapter.table_defs("absent_1922_table") == ((), ())
    finally:
        adapter.execute("SET search_path TO public")
        adapter.execute("DROP TABLE IF EXISTS shadow_1922.shadowed_1922")
        adapter.execute("DROP TABLE IF EXISTS public.shadowed_1922")
        adapter.execute("DROP SCHEMA IF EXISTS shadow_1922")
        adapter.commit()
        adapter.close()


# ---------------------------------------------------------------------------
# Issue #2128 -- the entry commands answer "is this database ready" in
# operator words instead of dying in a bare traceback.
#
# The class this guards: a command that assumes a migrated schema or a
# usable role and checks neither. On dev, `okg run --once --apply` against
# a scaffolded-but-unmigrated database raised
# `psycopg.errors.InvalidSchemaName: schema "okg" does not exist` with no
# code and no hint, and `okg mcp-serve` did the same from the release-
# contract read. These cases drive the real CLI against a real, empty
# PostgreSQL database and assert a structured refusal.
# ---------------------------------------------------------------------------


def _url_dsn_for_database(dsn: str, dbname: str) -> str:
    """Rewrite a URL DSN to name `dbname`, KEEPING the URL form.

    `_dsn_for_database` round-trips through `make_conninfo`, which emits
    libpq keyword form. The CLI refuses a keyword-form `--dsn` with a
    dsn_form refusal, so a test that drives the CLI must preserve the URL.
    """
    from urllib.parse import urlsplit, urlunsplit

    parts = urlsplit(dsn)
    return urlunsplit(parts._replace(path=f"/{dbname}"))


@contextlib.contextmanager
def _disposable_unmigrated_database(base_dsn: str):
    """A fresh, EMPTY database on the same server. Dropped on exit."""
    name = f"okg_unmigrated_{uuid.uuid4().hex[:12]}"
    maintenance = _maintenance_dsn(base_dsn)
    with db_connect(maintenance, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    try:
        yield _url_dsn_for_database(base_dsn, name)
    finally:
        with db_connect(maintenance, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                    sql.Identifier(name)
                )
            )


def _run_cli_json(argv: list[str], capsys, *, stream: str) -> tuple[int, dict]:
    """Run the real CLI dispatcher and parse its `--json` output.

    Asserts the JSON contract itself: one parseable document on the
    stream the CLI uses for that outcome -- refusals go to stderr
    (``_json_error``), successful payloads to stdout -- and no traceback
    on EITHER stream. A traceback is the defect this guards, so it is
    checked here rather than left to the reader.
    """
    from okg.substrate.cli import main as cli_main

    rc = cli_main.main(argv)
    captured = capsys.readouterr()
    assert "Traceback (most recent call last)" not in captured.out
    assert "Traceback (most recent call last)" not in captured.err
    text = captured.err if stream == "stderr" else captured.out
    return rc, json.loads(text)


def _write_minimal_deployment(root: Path, name: str, dsn: str) -> None:
    """The smallest manifest `resolve_deployment` accepts."""
    target = root / name
    (target / "schemas").mkdir(parents=True)
    (target / "source_registry.yaml").write_text("sources: {}\n")
    (target / "deployment.yaml").write_text(
        "name: " + name + "\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "postgres:\n"
        "  dsn: " + dsn + "\n"
    )


@pytest.mark.parametrize(
    ("command", "extra"),
    [
        ("run", ["--once", "--apply"]),
        ("mcp-serve", []),
    ],
)
def test_entry_commands_refuse_an_unmigrated_database_by_name(
    okg_dsn: str, capsys, monkeypatch, tmp_path, command: str, extra: list[str]
) -> None:
    with _disposable_unmigrated_database(okg_dsn) as dsn:
        root = tmp_path / "deployments"
        root.mkdir()
        _write_minimal_deployment(root, "readyprobe", dsn)
        monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
        argv = ["--json", command, *extra, "--deployment", "readyprobe"]
        rc, payload = _run_cli_json(argv, capsys, stream="stderr")

    assert rc == 1
    error = payload["error"]
    assert error["code"] == "database_not_migrated", error
    assert error["message"].startswith(f"okg {command}")
    # Actionable, and runnable by someone who installed a wheel: `okg
    # migrate` is a console verb, not a path inside a source checkout.
    assert "okg migrate --deployment readyprobe --apply" in error["fix_hint"]
    assert "ops/pg" not in error["fix_hint"]
    # The refusal reports what it measured, not just that something failed.
    assert error["data"]["schema_present"] is False
    assert error["data"]["migrated"] is False


def test_status_reports_an_unmigrated_database_instead_of_a_probe_error(
    okg_dsn: str, capsys, monkeypatch, tmp_path
) -> None:
    """`okg status` REPORTS -- it must not refuse, and must not read as broken.

    Before this change a missing `okg` schema aborted the status
    transaction and the snapshot carried
    `status_probe_failed: current transaction is aborted ...` -- an
    internal message for the most ordinary state a fresh scaffold has.
    """
    with _disposable_unmigrated_database(okg_dsn) as dsn:
        root = tmp_path / "deployments"
        root.mkdir()
        _write_minimal_deployment(root, "readyprobe", dsn)
        monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
        rc, payload = _run_cli_json(
            ["--json", "status", "--deployment", "readyprobe"],
            capsys,
            stream="stdout",
        )

    assert rc == 0
    assert payload["stage"] == "scaffolded"
    assert payload["errors"] == []
    assert "okg migrate" in payload["stage_meaning"]
    assert payload["database_readiness"]["migrated"] is False


def test_a_missing_grep_index_is_pending_work_on_a_migrated_database(
    okg_migrated: str, capsys, monkeypatch, tmp_path
) -> None:
    """Issue #3311, against real `pg_index` state.

    The migration ledger does not list the indexes `ensure_schema` builds,
    so a database upgraded without `okg migrate --apply` read "0 pending"
    while `search(method="grep")` refused every call. Readiness, `okg
    status` and the database health report must all name the missing index.
    """
    from okg.substrate.db.management import database_health_report
    from okg.substrate.db.readiness import read_database_readiness

    with db_connect(okg_migrated, autocommit=True) as conn:
        # The template's migrate built every tool index: nothing is missing.
        assert read_database_readiness(conn).missing_schema_objects == ()
        with conn.cursor(row_factory=tuple_row) as cur:
            cur.execute(
                "SELECT indexname, indexdef FROM pg_indexes "
                "WHERE schemaname = 'public' "
                "AND indexname ~ 'node_text_trgm_visible$'"
            )
            row = cur.fetchone()
            assert row is not None, "the migrated template has no grep index"
            name, indexdef = row
            cur.execute(
                sql.SQL("DROP INDEX public.{}").format(sql.Identifier(name))
            )
    try:
        with db_connect(okg_migrated, autocommit=True) as conn:
            readiness = read_database_readiness(conn)
        assert readiness.pending_migration_count == 0
        assert [
            (m.name, m.state, m.effect) for m in readiness.missing_schema_objects
        ] == [(name, "absent", "refused")]
        assert readiness.pending_work_count == 1

        report = database_health_report(okg_migrated, deployment_name="gripe")
        [finding] = [f for f in report["findings"] if f["code"] == "tool_index_missing"]
        assert finding["index"] == name and finding["severity"] == "error"
        assert report["ok"] is False
        assert "okg migrate --deployment gripe --apply" in finding["fix_hint"]

        root = tmp_path / "deployments"
        root.mkdir()
        _write_minimal_deployment(root, "gripe", okg_migrated)
        monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
        from okg.substrate.cli import main as cli_main

        cli_main.main(["--json", "status", "--deployment", "gripe"])
        captured = capsys.readouterr()
        payload = json.loads(captured.out)
        block = payload["database_readiness"]
        assert block["pending_migration_count"] == 0
        assert block["pending_work_count"] == 1
        assert block["pending_schema_objects"][0]["index"] == name
        assert 'search(method="grep") will refuse every call' in captured.err
    finally:
        with db_connect(okg_migrated, autocommit=True) as conn:
            conn.execute(indexdef.replace("CREATE INDEX", "CREATE INDEX IF NOT EXISTS", 1))


def test_readiness_refuses_a_real_nologin_role_on_a_migrated_database(
    okg_migrated: str,
) -> None:
    """The role half, against real `pg_roles` state.

    A uniquely named disposable role stands in for `okg_mcp`: roles are
    CLUSTER-wide, so a test that flipped the substrate's own role would
    change every other database on the server.
    """
    from okg.substrate.db.readiness import require_database_ready
    from okg.substrate.errors import SubstrateError

    role = f"okg_readiness_probe_{uuid.uuid4().hex[:12]}"
    with db_connect(okg_migrated, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            sql.SQL("CREATE ROLE {} NOLOGIN NOSUPERUSER").format(
                sql.Identifier(role)
            )
        )
    try:
        with pytest.raises(SubstrateError) as caught:
            require_database_ready(
                okg_migrated,
                surface="okg mcp-serve",
                deployment="probe",
                roles=(role,),
            )
        error = caught.value.to_json()["error"]
        assert error["code"] == "mcp_role_cannot_login"
        assert error["data"]["roles_unusable"] == [role]
        assert error["data"]["migrated"] is True
        assert "OKG_MCP_RO_DSN" in error["fix_hint"]
        assert "okg runtime bootstrap --deployment probe" in error["fix_hint"]

        # Granting LOGIN clears the refusal, so the gate is reading the real
        # `rolcanlogin` attribute and not merely the role's existence.
        with db_connect(okg_migrated, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("ALTER ROLE {} WITH LOGIN").format(
                        sql.Identifier(role)
                    )
                )
        readiness = require_database_ready(
            okg_migrated,
            surface="okg mcp-serve",
            deployment="probe",
            roles=(role,),
        )
        assert readiness.migrated
        assert readiness.unusable_roles == ()
    finally:
        with db_connect(okg_migrated, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(role))
                )


def test_readiness_names_a_missing_database_with_the_createdb_command(
    okg_dsn: str,
) -> None:
    """A connect-phase failure must not become a traceback either."""
    from okg.substrate.db.readiness import check_database_readiness
    from okg.substrate.errors import SubstrateError

    absent = _dsn_for_database(okg_dsn, f"okg_absent_{uuid.uuid4().hex[:12]}")
    with pytest.raises(SubstrateError) as caught:
        check_database_readiness(absent, surface="okg run --once")
    error = caught.value.to_json()["error"]
    assert error["code"] == "database_missing"
    assert "createdb" in error["fix_hint"]


def _dsn_as_role(dsn: str, role: str, password: str) -> str:
    """The same server and database, connected as another role."""
    from psycopg.conninfo import conninfo_to_dict, make_conninfo

    fields = conninfo_to_dict(dsn)
    fields["user"] = role
    fields["password"] = password
    return make_conninfo(**fields)


@contextlib.contextmanager
def _disposable_login_role(dsn: str, *, suffix: str):
    """A uniquely named LOGIN role with CONNECT and nothing else.

    Roles are CLUSTER-wide, so the substrate's own `app_ro` / `okg_mcp`
    are never mutated: a disposable role stands in for them and is
    dropped again.
    """
    from psycopg.conninfo import conninfo_to_dict

    role = f"okg_readiness_{suffix}_{uuid.uuid4().hex[:10]}"
    password = "probe_" + uuid.uuid4().hex[:10]
    dbname = str(conninfo_to_dict(dsn).get("dbname") or "")
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                sql.Identifier(role), sql.Literal(password)
            )
        )
        cur.execute(
            sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                sql.Identifier(dbname), sql.Identifier(role)
            )
        )
    try:
        yield role, _dsn_as_role(dsn, role, password)
    finally:
        with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            # The CONNECT grant is a dependent object: `DROP ROLE` refuses
            # with `DependentObjectsStillExist` until it is revoked.
            cur.execute(
                sql.SQL("REVOKE ALL ON DATABASE {} FROM {}").format(
                    sql.Identifier(dbname), sql.Identifier(role)
                )
            )
            cur.execute(
                sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(role))
            )


def test_a_pending_migration_is_reported_and_never_refused(
    okg_migrated: str, monkeypatch
) -> None:
    """The decision of 2026-09-21, against a real migration ledger.

    A database whose schema is applied but whose ledger is behind is
    REPORTED and the command continues; refusal is reserved for an
    absent schema or ledger. Adding a hard refusal here (review mutant
    M12) survived the whole original test set.

    The database is left untouched: the KNOWN set gains one synthetic
    migration instead, so the real ledger read against the real server
    genuinely comes back one migration short. Deleting a row from the
    session-scoped migrated database would leave every later case in
    the file running against a half-rolled-back schema.
    """
    import dataclasses

    from okg.substrate.db import management as mgmt
    from okg.substrate.db.readiness import (
        pending_migration_report,
        pending_migration_warning,
        require_database_ready,
    )

    known_migrations = mgmt.current_known_migrations
    unapplied = f"9999_unapplied_probe_{uuid.uuid4().hex[:8]}"

    def _known_plus_one(**kwargs):
        known = list(known_migrations(**kwargs))
        known.append(dataclasses.replace(known[-1], migration_id=unapplied))
        return known

    monkeypatch.setattr(mgmt, "current_known_migrations", _known_plus_one)

    readiness = require_database_ready(
        okg_migrated, surface="okg run --once", deployment="pendingdep"
    )

    assert readiness.migrated is True
    assert readiness.pending_migration_count == 1
    assert readiness.pending_migrations == (unapplied,)
    block = pending_migration_report(readiness, deployment="pendingdep")
    assert block["pending_migrations"] == [unapplied]
    assert block["migrate_command"] == (
        "okg migrate --deployment pendingdep --apply"
    )
    line = pending_migration_warning(
        readiness, surface="okg run --once", deployment="pendingdep"
    )
    assert unapplied in line
    assert "okg migrate --deployment pendingdep --apply" in line


def test_a_role_that_cannot_read_the_ledger_is_not_called_unreachable(
    okg_migrated: str,
) -> None:
    """A privilege gap is not an outage (review BLOCKER-2).

    Measured on dev: a login role holding the substrate's own `app_ro`
    on a fully migrated, reachable database was refused with
    `database_unreachable` and the hint "start or reach the Postgres
    server", when the real cause was `permission denied for table
    schema_migrations` (SQLSTATE 42501).
    """
    from okg.substrate.db.readiness import check_database_readiness
    from okg.substrate.errors import SubstrateError

    with _disposable_login_role(okg_migrated, suffix="noread") as (role, dsn):
        with pytest.raises(SubstrateError) as caught:
            check_database_readiness(dsn, surface="okg run --once")
    error = caught.value.to_json()["error"]
    assert error["code"] == "database_readiness_unreadable"
    assert error["data"]["role"] == role
    assert error["data"]["sqlstate"] == "42501"
    assert "privilege gap, not an outage" in error["message"]
    assert f"GRANT USAGE ON SCHEMA okg TO {role}" in error["fix_hint"]
    assert "start or reach the Postgres server" not in error["fix_hint"]


def test_a_wrong_mcp_read_dsn_is_refused_instead_of_bypassing_the_check(
    okg_migrated: str, monkeypatch
) -> None:
    """`OKG_MCP_RO_DSN` must be a working credential, not a set variable.

    It suppresses the derived-role check on its mere presence, so before
    this probe a DSN naming a role that cannot log in restored the exact
    pre-fix behaviour: `okg mcp-serve` exited 0, logged "starting
    anyway", and served a graph it could not read.
    """
    from okg.substrate.db.readiness import require_mcp_read_dsn_usable
    from okg.substrate.errors import SubstrateError

    role = f"okg_readiness_nologin_{uuid.uuid4().hex[:10]}"
    with db_connect(okg_migrated, autocommit=True) as conn, conn.cursor() as cur:
        # The role gets the password the DSN carries. A server that checks
        # passwords (CI's does) refuses a role with none as "password
        # authentication failed" before it ever reaches the NOLOGIN check this
        # test is about; only a trust-auth server skipped that step.
        cur.execute(
            sql.SQL("CREATE ROLE {} NOLOGIN NOSUPERUSER PASSWORD {}").format(
                sql.Identifier(role), sql.Literal("unused_pw")
            )
        )
    try:
        monkeypatch.setenv(
            "OKG_MCP_RO_DSN", _dsn_as_role(okg_migrated, role, "unused_pw")
        )
        with pytest.raises(SubstrateError) as caught:
            require_mcp_read_dsn_usable(
                surface="okg mcp-serve", deployment="walkrole"
            )
        error = caught.value.to_json()["error"]
        assert error["code"] == "mcp_read_dsn_unusable"
        assert error["data"]["role"] == role
        assert "not permitted to log in" in error["data"]["detail"]
        assert "unused_pw" not in json.dumps(error)
    finally:
        with db_connect(okg_migrated, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(role))
            )


def test_runtime_worker_refuses_an_unmigrated_database_by_name(
    okg_dsn: str, capsys, monkeypatch, tmp_path
) -> None:
    """`okg runtime worker` is a readiness surface too (review SHOULD-FIX-3).

    It called `enforce_release_contract_if_declared` with no precheck,
    and that release read queries `okg.catalog_versions` unconditionally,
    so a pre-migration worker launch answered a raw psycopg
    `UndefinedTable` traceback while `okg mcp-serve` two frames away
    named `okg migrate --apply`.
    """
    with _disposable_unmigrated_database(okg_dsn) as dsn:
        root = tmp_path / "deployments"
        root.mkdir()
        _write_minimal_deployment(root, "workerprobe", dsn)
        manifest = root / "workerprobe" / "deployment.yaml"
        manifest.write_text(manifest.read_text() + "runtime:\n  enabled: true\n")
        monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
        rc, payload = _run_cli_json(
            ["--json", "runtime", "worker", "--deployment", "workerprobe"],
            capsys,
            stream="stderr",
        )

    assert rc == 1
    error = payload["error"]
    assert error["code"] == "database_not_migrated", error
    assert error["message"].startswith("okg runtime worker")
    assert "okg migrate --deployment workerprobe --apply" in error["fix_hint"]


# ---------------------------------------------------------------------------
# Issue #2239 -- a non-superuser role must be able to run the whole migration.
#
# Every other test here migrates as the superuser the Docker route provides,
# and a superuser skips the checks that stopped a DBA-prepared role:
# `ALTER FUNCTION ... OWNER TO okg_external_action_function_owner` needs the
# NEW owner to hold CREATE on schema okg, and `ALTER ROLE ... NOSUPERUSER
# NOBYPASSRLS` needs the caller to hold those attributes even to restate them.
# ---------------------------------------------------------------------------

_FUNCTION_OWNER = "okg_external_action_function_owner"


def _revoke_memberships_granted_by(cur, role: str) -> None:
    """Undo cluster-wide role grants the disposable migrator made.

    The migration runs `GRANT app_rw, catalog_admin TO projection_remediator`,
    which records the migrator as grantor; `DROP ROLE` refuses until those
    grant records are gone. Grants the superuser made are left untouched.
    """
    cur.execute(
        """
        SELECT granted.rolname, member.rolname
          FROM pg_catalog.pg_auth_members AS membership
          JOIN pg_catalog.pg_roles AS granted ON granted.oid = membership.roleid
          JOIN pg_catalog.pg_roles AS member ON member.oid = membership.member
         WHERE membership.grantor =
               (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = %s)
        """,
        (role,),
    )
    for granted, member in cur.fetchall():
        cur.execute(
            sql.SQL("REVOKE {} FROM {} GRANTED BY {}").format(
                sql.Identifier(granted),
                sql.Identifier(member),
                sql.Identifier(role),
            )
        )


@contextlib.contextmanager
def _non_superuser_migrator(base_dsn: str):
    """A fresh database and a NOSUPERUSER LOGIN role prepared the way a DBA
    would prepare one for `okg install --postgres-dsn`.

    The superuser installs the required extensions and grants: CREATE on the
    database, CREATE on its `public` schema (the Chronos graph tables live
    there), CREATEROLE with ADMIN OPTION on every schema role, and membership
    in `okg_substrate` (views.sql alters that role's default privileges when
    it exists). Nothing more. Yields `(role, role_dsn, superuser_dsn)`; the
    database, the role and every grant the role made are removed on exit.
    """
    from okg.substrate.db.migrate import SCHEMA_ROLES

    suffix = uuid.uuid4().hex[:10]
    dbname = f"okg_nonsuper_migrate_{suffix}"
    role = f"okg_nonsuper_migrator_{suffix}"
    password = "probe_" + uuid.uuid4().hex[:10]
    maintenance = _maintenance_dsn(base_dsn)
    superuser_dsn = _dsn_for_database(base_dsn, dbname)
    with db_connect(maintenance, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
        cur.execute(
            sql.SQL(
                "CREATE ROLE {} LOGIN NOSUPERUSER NOBYPASSRLS CREATEROLE PASSWORD {}"
            ).format(sql.Identifier(role), sql.Literal(password))
        )
        cur.execute(
            sql.SQL("GRANT CONNECT, CREATE ON DATABASE {} TO {}").format(
                sql.Identifier(dbname), sql.Identifier(role)
            )
        )
        cur.execute(
            "SELECT rolname FROM pg_catalog.pg_roles WHERE rolname = ANY(%s)",
            (list(SCHEMA_ROLES) + ["okg_substrate"],),
        )
        present = {row[0] for row in cur.fetchall()}
        assert set(SCHEMA_ROLES) <= present, sorted(set(SCHEMA_ROLES) - present)
        for granted in sorted(present):
            cur.execute(
                sql.SQL("GRANT {} TO {} WITH ADMIN OPTION").format(
                    sql.Identifier(granted), sql.Identifier(role)
                )
            )
    try:
        with db_connect(superuser_dsn, autocommit=True) as conn, conn.cursor() as cur:
            for extension in REQUIRED_EXTENSIONS:
                cur.execute(
                    sql.SQL("CREATE EXTENSION IF NOT EXISTS {}").format(
                        sql.Identifier(extension)
                    )
                )
            cur.execute(
                sql.SQL("GRANT CREATE ON SCHEMA public TO {}").format(
                    sql.Identifier(role)
                )
            )
            cur.execute(
                "SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname = %s",
                (role,),
            )
            assert cur.fetchone() == (False,)
        yield role, _dsn_as_role(superuser_dsn, role, password), superuser_dsn
    finally:
        with db_connect(maintenance, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                    sql.Identifier(dbname)
                )
            )
            _revoke_memberships_granted_by(cur, role)
            cur.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(role)))


def _function_owner_state(superuser_dsn: str) -> tuple[bool, dict[str, int]]:
    """(owner role holds CREATE on okg, SECURITY DEFINER helpers per owner)."""
    with db_connect(superuser_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT has_schema_privilege(%s, 'okg', 'CREATE')", (_FUNCTION_OWNER,)
        )
        holds_create = bool(cur.fetchone()[0])
        cur.execute(
            """
            SELECT pg_catalog.pg_get_userbyid(p.proowner), count(*)
              FROM pg_catalog.pg_proc AS p
             WHERE p.pronamespace = 'okg'::regnamespace
               AND p.proname LIKE 'external\\_action\\_%%'
               AND p.prosecdef
             GROUP BY 1
            """
        )
        owners = {str(name): int(count) for name, count in cur.fetchall()}
    return holds_create, owners


def test_a_non_superuser_role_runs_the_whole_migration(okg_migrated: str) -> None:
    """Issue #2239: the migration finishes as a DBA-prepared NOSUPERUSER role.

    On unfixed code this raises `permission denied for schema okg` at the
    first `ALTER FUNCTION okg.external_action_* OWNER TO
    okg_external_action_function_owner`. It also pins the least-privilege
    posture the fix must keep: every SECURITY DEFINER external-action helper
    ends up owned by the NOLOGIN owner role, and that role holds no CREATE on
    schema okg once the migration commits.
    """
    with _non_superuser_migrator(okg_migrated) as (role, role_dsn, superuser_dsn):
        first = migrate(role_dsn)
        assert first.pending_multiphase == ()
        holds_create, owners = _function_owner_state(superuser_dsn)
        assert holds_create is False
        assert set(owners) == {_FUNCTION_OWNER}, owners
        assert owners[_FUNCTION_OWNER] > 0

        # A replay as the same role converges too (checksum skip path).
        migrate(role_dsn)
        assert _function_owner_state(superuser_dsn) == (holds_create, owners)

        # The views-only refresh runs views.sql's transfers on their own.
        # Hand one helper back to the migrating role, as a views.sql that
        # newly creates it would leave it, so the transfer is a real one
        # (an OWNER TO the current owner is a no-op PostgreSQL never checks).
        with db_connect(superuser_dsn, autocommit=True) as conn:
            conn.execute(
                sql.SQL(
                    "ALTER FUNCTION okg.external_action_session_deployment() "
                    "OWNER TO {}"
                ).format(sql.Identifier(role))
            )
        assert _function_owner_state(superuser_dsn)[1] != owners
        install_views(role_dsn)
        assert _function_owner_state(superuser_dsn) == (holds_create, owners)


def test_a_failing_non_superuser_replay_surfaces_and_keeps_the_committed_owner_state(
    okg_migrated: str,
) -> None:
    """A replay that fails after the grant opened leaves the committed state.

    One good run commits first; its revoke is what leaves the owner role with
    USAGE only. Then remove the role's CREATEROLE: the replay fails at
    `ALTER ROLE okg_mcp SET plan_cache_mode` in views.sql, after the grant was
    opened. The failure must surface, and afterwards the owner role still
    holds no CREATE on okg and still owns every SECURITY DEFINER helper.
    """
    with _non_superuser_migrator(okg_migrated) as (role, role_dsn, superuser_dsn):
        migrate(role_dsn)
        with db_connect(superuser_dsn, autocommit=True) as conn:
            conn.execute(
                sql.SQL("ALTER ROLE {} NOCREATEROLE").format(sql.Identifier(role))
            )
        with pytest.raises(psycopg.errors.InsufficientPrivilege) as caught:
            migrate(role_dsn)
        assert "CREATEROLE" in str(caught.value)
        holds_create, owners = _function_owner_state(superuser_dsn)
        assert holds_create is False
        assert set(owners) == {_FUNCTION_OWNER}, owners


def test_a_non_superuser_role_runs_the_job_runtime_cleanup_view_install(
    okg_migrated: str,
) -> None:
    """`okg db remove-job-runtime-subsystem` executes views.sql too.

    It runs the view script in its own transaction before the restrictive
    drops, so it needs the same transfer window as `migrate()`. The legacy
    objects are seeded AS the migrating role, which owns them on a real
    database that role migrated, and one helper is handed back to that role
    so the view script's transfer is a real one.
    """
    from okg.substrate.db.management import apply_remove_job_runtime_subsystem
    from tests.substrate.test_database_management import (
        _seed_empty_dbos_workflow_status,
        _seed_obsolete_job_runtime_schema,
    )

    with _non_superuser_migrator(okg_migrated) as (role, role_dsn, superuser_dsn):
        migrate(role_dsn)
        holds_create, owners = _function_owner_state(superuser_dsn)
        _seed_obsolete_job_runtime_schema(role_dsn)
        _seed_empty_dbos_workflow_status(role_dsn)
        with db_connect(superuser_dsn, autocommit=True) as conn:
            conn.execute(
                sql.SQL(
                    "ALTER FUNCTION okg.external_action_session_deployment() "
                    "OWNER TO {}"
                ).format(sql.Identifier(role))
            )

        result = apply_remove_job_runtime_subsystem(role_dsn, confirm=True)

        assert all(result["postcondition"].values()), result["postcondition"]
        assert _function_owner_state(superuser_dsn) == (False, owners)
        assert holds_create is False


# ---------------------------------------------------------------------------
# Issue #2280 -- the MCP read role records its own connection reservation, and
# can touch no one else's.
#
# `okg mcp-serve` claims its connection demand in okg.connection_reservations on
# the read DSN, i.e. as okg_mcp. okg_mcp holds NO privilege on that table: a
# table grant would let any holder of the okg_mcp credential delete or backdate
# another process's row (which makes that process exit within one heartbeat) or
# insert an inflated claim that refuses every later start. It claims, refreshes
# and releases through three SECURITY DEFINER functions keyed by a holder token
# only the claiming process knows.
# ---------------------------------------------------------------------------

_RESERVATION_OWNER = "okg_connection_reservation_owner"
_RESERVATION_FUNCTIONS = (
    "okg.connection_reservation_claim(text,integer,text,integer,text)",
    "okg.connection_reservation_heartbeat(text,integer,text)",
    "okg.connection_reservation_release(text,integer,text)",
)
_TABLE_VERBS = (
    "SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER",
)


@contextlib.contextmanager
def _disposable_mcp_login(dsn: str):
    """A LOGIN role that holds exactly what okg_mcp holds.

    Stands in for the MCP principal an install provisions (`okg_mcp` itself,
    or an `OKG_MCP_RO_DSN` role granted okg_mcp) without mutating the
    cluster-wide `okg_mcp` role's login state or password.
    """
    with _disposable_login_role(dsn, suffix="mcp2280") as (role, role_dsn):
        with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(sql.SQL("GRANT okg_mcp TO {}").format(sql.Identifier(role)))
        yield role, role_dsn


def _delete_reservations(dsn: str, *names: str) -> None:
    with db_connect(dsn, autocommit=True) as conn:
        conn.execute(
            "DELETE FROM okg.connection_reservations"
            " WHERE application_name = ANY(%s)",
            (list(names),),
        )


def _reservation_rows(dsn: str, *names: str) -> list[tuple]:
    with db_connect(dsn, autocommit=True) as conn:
        return conn.execute(
            "SELECT application_name, pid, declared_demand, heartbeat_at,"
            "       holder_token_sha256 IS NOT NULL"
            "  FROM okg.connection_reservations"
            " WHERE application_name = ANY(%s)"
            " ORDER BY application_name, pid",
            (list(names),),
        ).fetchall()


def _claim(conn, name: str, pid: int, demand: int, token: str):
    return conn.execute(
        "SELECT other_claims, usable"
        "  FROM okg.connection_reservation_claim(%s, %s, 'd', %s, %s)",
        (name, pid, demand, token),
    ).fetchone()


def test_mcp_serve_records_its_reservation_as_the_mcp_role(
    okg_migrated: str, monkeypatch, caplog
) -> None:
    """A fresh migrate plus the MCP principal: startup is accounted, no warning,
    and closing the server's read pool releases the claim by itself -- no
    caller-side release, so an in-process server cannot leak a live claim."""
    pytest.importorskip("mcp")
    from okg.substrate.db import capacity
    from okg.substrate.db.concurrency_budget import connection_demand
    from okg.substrate.mcp.server import build_server

    deployment = f"resv2280{uuid.uuid4().hex[:8]}"
    application_name = f"okg-mcp-{deployment}"
    with _disposable_mcp_login(okg_migrated) as (_role, role_dsn):
        monkeypatch.setenv("OKG_MCP_RO_DSN", role_dsn)
        caplog.set_level("WARNING", logger="okg.substrate.mcp.server")
        server = build_server(dsn=okg_migrated, deployment_name=deployment)
        try:
            rows = [
                (pid, demand, tokened)
                for _name, pid, demand, _hb, tokened in _reservation_rows(
                    okg_migrated, application_name
                )
            ]
        finally:
            server._okg_pool.close()
        remaining = _reservation_rows(okg_migrated, application_name)
        owned = [
            key for key in (*capacity._RESERVATION_OWNERS, *capacity._HOLDER_TOKENS)
            if key[1] == application_name
        ]

    warnings = [
        record.getMessage()
        for record in caplog.records
        if "reservation" in record.getMessage()
    ]
    assert warnings == [], warnings
    assert rows == [
        (os.getpid(), connection_demand(include_mcp=True)["_total"], True)
    ]
    assert remaining == []
    assert owned == []
    assert callable(server._okg_release_reservation)


def test_mcp_role_holds_no_privilege_on_the_reservation_table(
    okg_migrated: str,
) -> None:
    """okg_mcp reaches the table only through three fixed definer functions."""
    with _disposable_mcp_login(okg_migrated) as (role, role_dsn):
        with db_connect(okg_migrated, autocommit=True) as conn:
            direct = conn.execute(
                "SELECT grantee, privilege_type"
                "  FROM information_schema.role_table_grants"
                " WHERE table_schema = 'okg'"
                "   AND table_name = 'connection_reservations'"
                "   AND grantee = ANY(%s)",
                (["okg_mcp", "app_ro", "PUBLIC", role],),
            ).fetchall()
            effective = {
                verb: conn.execute(
                    "SELECT has_table_privilege(%s, 'okg.connection_reservations', %s)",
                    (who, verb),
                ).fetchone()[0]
                for who in ("okg_mcp", role)
                for verb in _TABLE_VERBS
            }
            execute = {
                (who, fn): conn.execute(
                    "SELECT has_function_privilege(%s, %s, 'EXECUTE')", (who, fn)
                ).fetchone()[0]
                for who in ("okg_mcp", role, "app_ro")
                for fn in _RESERVATION_FUNCTIONS
            }
            public_execute = conn.execute(
                "SELECT count(*) FROM pg_catalog.pg_proc AS p,"
                "       LATERAL aclexplode(coalesce(p.proacl,"
                "                          acldefault('f', p.proowner))) AS a"
                " WHERE p.oid = ANY(%s::regprocedure[]) AND a.grantee = 0",
                (list(_RESERVATION_FUNCTIONS),),
            ).fetchone()[0]
            definers = conn.execute(
                "SELECT p.prosecdef, pg_get_userbyid(p.proowner), p.proconfig"
                "  FROM pg_catalog.pg_proc AS p"
                " WHERE p.oid = ANY(%s::regprocedure[])",
                (list(_RESERVATION_FUNCTIONS),),
            ).fetchall()
            owner_role = conn.execute(
                "SELECT rolcanlogin, rolsuper, rolbypassrls, rolcreatedb,"
                "       rolcreaterole, rolinherit"
                "  FROM pg_catalog.pg_roles WHERE rolname = %s",
                (_RESERVATION_OWNER,),
            ).fetchone()
            owner_tables = conn.execute(
                "SELECT table_name, privilege_type"
                "  FROM information_schema.role_table_grants"
                " WHERE grantee = %s ORDER BY 1, 2",
                (_RESERVATION_OWNER,),
            ).fetchall()
            owner_create = conn.execute(
                "SELECT has_schema_privilege(%s, 'okg', 'CREATE')",
                (_RESERVATION_OWNER,),
            ).fetchone()[0]

        with db_connect(role_dsn, autocommit=True) as conn:
            for statement in (
                "SELECT 1 FROM okg.connection_reservations",
                "INSERT INTO okg.connection_reservations"
                + " (application_name, pid, declared_demand) VALUES ('okg-mcp-x', 1, 1)",
                "UPDATE okg.connection_reservations SET heartbeat_at = now()",
                "DELETE FROM okg.connection_reservations",
                "TRUNCATE okg.connection_reservations",
                "INSERT INTO okg.node_facts DEFAULT VALUES",
                "UPDATE okg.graph_generations SET status = status",
                "DELETE FROM okg.graph_generations",
                "INSERT INTO okg.catalog_versions DEFAULT VALUES",
                "DELETE FROM okg.mcp_call_log",
            ):
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    conn.execute(statement)

    assert direct == []
    assert not any(effective.values()), effective
    assert all(
        allowed for (who, _fn), allowed in execute.items() if who != "app_ro"
    ), execute
    assert not any(
        allowed for (who, _fn), allowed in execute.items() if who == "app_ro"
    ), execute
    assert public_execute == 0
    assert definers == [
        (True, _RESERVATION_OWNER, ["search_path=pg_catalog, okg, pg_temp"])
    ] * 3
    assert owner_role == (False, False, False, False, False, False)
    assert owner_tables == [
        ("connection_reservations", verb)
        for verb in ("DELETE", "INSERT", "SELECT", "UPDATE")
    ]
    assert owner_create is False


def test_mcp_role_cannot_touch_another_holders_reservation(
    okg_migrated: str,
) -> None:
    """A foreign holder token moves nothing: no delete, no backdate, no
    takeover, and a worker's row is out of reach entirely."""
    victim = f"okg-mcp-victim{uuid.uuid4().hex[:8]}"
    worker = f"okg-worker-victim{uuid.uuid4().hex[:8]}"
    token = uuid.uuid4().hex + uuid.uuid4().hex
    forged = uuid.uuid4().hex + uuid.uuid4().hex
    try:
        with db_connect(okg_migrated, autocommit=True) as conn:
            conn.execute(
                "INSERT INTO okg.connection_reservations"
                " (application_name, pid, declared_demand) VALUES (%s, 4242, 3)",
                (worker,),
            )
        with _disposable_mcp_login(okg_migrated) as (_role, role_dsn):
            with db_connect(role_dsn, autocommit=True) as conn:
                _claim(conn, victim, 4343, 2, token)
                before = _reservation_rows(okg_migrated, victim, worker)

                for name, pid in ((victim, 4343), (worker, 4242)):
                    assert conn.execute(
                        "SELECT okg.connection_reservation_heartbeat(%s, %s, %s)",
                        (name, pid, forged),
                    ).fetchone() == (0,)
                    assert conn.execute(
                        "SELECT okg.connection_reservation_release(%s, %s, %s)",
                        (name, pid, forged),
                    ).fetchone() == (0,)
                with pytest.raises(psycopg.Error) as takeover:
                    _claim(conn, victim, 4343, 50, forged)
                assert takeover.value.sqlstate == "OKR04"
                with pytest.raises(psycopg.Error) as worker_claim:
                    _claim(conn, worker, 4242, 0, forged)
                assert worker_claim.value.sqlstate == "OKR03"
                for statement in (
                    "DELETE FROM okg.connection_reservations",
                    "UPDATE okg.connection_reservations"
                    + " SET heartbeat_at = now() - interval '1 hour'",
                ):
                    with pytest.raises(psycopg.errors.InsufficientPrivilege):
                        conn.execute(statement)
                after = _reservation_rows(okg_migrated, victim, worker)

                # The real holder still owns its row.
                assert conn.execute(
                    "SELECT okg.connection_reservation_heartbeat(%s, %s, %s)",
                    (victim, 4343, token),
                ).fetchone() == (1,)
                assert conn.execute(
                    "SELECT okg.connection_reservation_release(%s, %s, %s)",
                    (victim, 4343, token),
                ).fetchone() == (1,)
        released = _reservation_rows(okg_migrated, victim, worker)
    finally:
        _delete_reservations(okg_migrated, victim, worker)

    assert after == before
    assert [row[0] for row in before] == [victim, worker]
    assert [row[0] for row in released] == [worker]


def test_mcp_role_cannot_claim_above_the_cap_or_beyond_the_budget(
    okg_migrated: str,
) -> None:
    """The cap and the budget are enforced inside the function, so a caller
    cannot record a demand that blocks every later start."""
    from okg.substrate.db.capacity import reserve_process_demand
    from okg.substrate.db.concurrency_budget import BudgetError

    name = f"okg-mcp-cap{uuid.uuid4().hex[:8]}"
    filler = f"okg-worker-fill{uuid.uuid4().hex[:8]}"
    token = uuid.uuid4().hex + uuid.uuid4().hex
    try:
        with _disposable_mcp_login(okg_migrated) as (_role, role_dsn):
            with db_connect(role_dsn, autocommit=True) as conn:
                with pytest.raises(psycopg.Error) as above_cap:
                    _claim(conn, name, 4444, 321, token)
                with pytest.raises(psycopg.Error) as negative:
                    _claim(conn, name, 4444, -1, token)
                with pytest.raises(psycopg.Error) as long_name:
                    _claim(conn, name + "x" * 200, 4444, 1, token)
                with pytest.raises(psycopg.Error) as long_deployment:
                    conn.execute(
                        "SELECT * FROM okg.connection_reservation_claim"
                        "(%s, 4444, %s, 1, %s)",
                        (name, "d" * 201, token),
                    )
                usable = conn.execute(
                    "SELECT current_setting('max_connections')::int"
                    " - current_setting('superuser_reserved_connections')::int"
                ).fetchone()[0]
                with pytest.raises(psycopg.Error) as over_budget:
                    _claim(conn, name, 4444, usable + 1, token)
            with db_connect(okg_migrated, autocommit=True) as conn:
                conn.execute(
                    "INSERT INTO okg.connection_reservations"
                    " (application_name, pid, declared_demand) VALUES (%s, 4545, %s)",
                    (filler, usable - 1),
                )
            with pytest.raises(BudgetError, match=filler):
                reserve_process_demand(
                    role_dsn,
                    application_name=name,
                    include_mcp=True,
                    heartbeat_interval_seconds=None,
                    mcp_read_role=True,
                )
            rows = _reservation_rows(okg_migrated, name)
    finally:
        _delete_reservations(okg_migrated, name, filler)

    assert above_cap.value.sqlstate == "OKR02"
    assert negative.value.sqlstate == "OKR03"
    assert "between 0 and 320" in str(negative.value)
    assert long_name.value.sqlstate == "OKR03"
    assert long_deployment.value.sqlstate == "OKR03"
    assert usable + 1 <= 320 or over_budget.value.sqlstate != "OKR02"
    assert over_budget.value.sqlstate in {"OKR01", "OKR02"}
    assert rows == []


def test_reservation_sweep_removes_only_stale_rows(okg_migrated: str) -> None:
    """A claim through the function sweeps rows past the server-side 300 s
    horizon, and nothing fresher."""
    stale_worker = f"okg-worker-stale{uuid.uuid4().hex[:8]}"
    fresh_worker = f"okg-worker-fresh{uuid.uuid4().hex[:8]}"
    stale_mcp = f"okg-mcp-stale{uuid.uuid4().hex[:8]}"
    claimer = f"okg-mcp-sweep{uuid.uuid4().hex[:8]}"
    names = (stale_worker, fresh_worker, stale_mcp, claimer)
    try:
        with db_connect(okg_migrated, autocommit=True) as conn:
            for name, age in (
                (stale_worker, 400), (fresh_worker, 200), (stale_mcp, 301),
            ):
                conn.execute(
                    "INSERT INTO okg.connection_reservations"
                    " (application_name, pid, declared_demand, heartbeat_at)"
                    " VALUES (%s, 4646, 1, now() - make_interval(secs => %s))",
                    (name, age),
                )
        with _disposable_mcp_login(okg_migrated) as (_role, role_dsn):
            with db_connect(role_dsn, autocommit=True) as conn:
                _claim(conn, claimer, 4747, 1, uuid.uuid4().hex + uuid.uuid4().hex)
        left = [row[0] for row in _reservation_rows(okg_migrated, *names)]
    finally:
        _delete_reservations(okg_migrated, *names)

    assert sorted(left) == sorted([fresh_worker, claimer])


def test_mcp_reservation_lifecycle_through_capacity(okg_migrated: str) -> None:
    """capacity.py's MCP mode: claim, re-claim with the same token, heartbeat,
    lease loss when the row disappears, and release."""
    from okg.substrate.db import capacity

    name = f"okg-mcp-life{uuid.uuid4().hex[:8]}"
    lost: list[str] = []
    try:
        with _disposable_mcp_login(okg_migrated) as (_role, role_dsn):
            for _ in range(2):
                capacity.reserve_process_demand(
                    role_dsn,
                    application_name=name,
                    include_mcp=True,
                    heartbeat_interval_seconds=1,
                    lease_lost_handler=lost.append,
                    mcp_read_role=True,
                )
            key = capacity._reservation_owner_key(role_dsn, name)
            owner = capacity._RESERVATION_OWNERS[key]
            owner.stop.set()
            if owner.thread is not None:
                owner.thread.join(timeout=5)
            owner.stop.clear()
            before = _reservation_rows(okg_migrated, name)
            capacity._refresh_reservation(owner)
            refreshed = _reservation_rows(okg_migrated, name)
            assert lost == []
            # An operator (not the MCP role) removes the row: the next
            # heartbeat owns zero rows and the process is told its lease is
            # gone, exactly as on the direct path.
            _delete_reservations(okg_migrated, name)
            capacity._refresh_reservation(owner)
            capacity.release_reservation(role_dsn, application_name=name)
            released = _reservation_rows(okg_migrated, name)
            with pytest.raises(ValueError):
                capacity.reserve_process_demand(
                    role_dsn, application_name=name, mcp_read_role=True,
                    capacity=capacity.ServerCapacity(100, 3, 1),
                )
            with pytest.raises(ValueError):
                capacity.reserve_process_demand(
                    role_dsn, application_name=name, mcp_read_role=True,
                    stale_after_seconds=10, heartbeat_interval_seconds=1,
                )
    finally:
        _delete_reservations(okg_migrated, name)

    assert len(before) == 1 and before[0][1] == os.getpid() and before[0][4]
    assert refreshed[0][3] >= before[0][3]
    assert released == []
    assert len(lost) == 1 and "updated=0" in lost[0]
    assert key not in capacity._HOLDER_TOKENS


def test_reservation_function_constants_match_the_python_protocol() -> None:
    """The TTL, cap and lock key live in views.sql; pin them to capacity.py
    and the concurrency registry so neither side drifts alone."""
    from okg.substrate.db import capacity
    from okg.substrate.db import concurrency_budget as budget
    from okg.substrate.db.migrate import _views_sql

    views = _views_sql()
    # The same terms `connection_demand` sums: a child-process-only term
    # (`ingest.source_process`) is opened by the source children, which the
    # coordinator admits against the live server count, so it is never part
    # of the demand a process reserves.
    largest_mcp_demand = sum(
        budget._MAX_DECLARABLE_WIDTH * term.connections_per_worker
        for term in budget.declared_terms()
        if term.counts_against_connections
        and term.borrows_from is None
        and not term.child_process_only
    )
    assert largest_mcp_demand == sum(
        budget._MAX_DECLARABLE_WIDTH * term.connections_per_worker
        for term in budget.declared_terms()
        if term.name in budget.connection_demand(include_mcp=True)
    )
    assert "IF p_declared_demand > %d THEN" % largest_mcp_demand in views
    assert (
        "heartbeat_at < now() - interval '%d seconds'"
        % capacity.RESERVATION_STALE_AFTER_SECONDS
    ) in views
    assert "pg_advisory_xact_lock(%d)" % capacity._RESERVATION_LOCK_KEY in views


def test_reservation_warning_names_the_repair() -> None:
    """An upgraded install that has not re-migrated, or a custom read role
    outside okg_mcp, is told what to run; any other failure gets no hint."""
    from okg.substrate.mcp.server import _reservation_privilege_fix_hint

    missing = psycopg.errors.UndefinedFunction(
        "function okg.connection_reservation_claim does not exist"
    )
    denied = psycopg.errors.InsufficientPrivilege(
        "permission denied for function connection_reservation_claim"
    )
    assert "okg migrate --deployment walk --apply" in (
        _reservation_privilege_fix_hint(missing, "walk")
    )
    hint = _reservation_privilege_fix_hint(denied, "walk")
    assert "okg migrate --deployment walk --apply" in hint
    assert "GRANT okg_mcp TO" in hint and "NOINHERIT" in hint
    assert _reservation_privilege_fix_hint(OSError("connection refused"), "walk") == ""


def test_mcp_serve_without_execute_warns_with_the_repair(
    okg_migrated: str, monkeypatch, caplog
) -> None:
    """A read role that cannot call the claim function: startup still
    continues, and the one warning it logs names the command that repairs it."""
    pytest.importorskip("mcp")
    from okg.substrate.db.capacity import release_process_reservations
    from okg.substrate.mcp.server import build_server

    deployment = f"resv2280{uuid.uuid4().hex[:8]}"
    claim = _RESERVATION_FUNCTIONS[0]
    with _disposable_mcp_login(okg_migrated) as (_role, role_dsn):
        with db_connect(okg_migrated, autocommit=True) as conn:
            conn.execute(f"REVOKE EXECUTE ON FUNCTION {claim} FROM okg_mcp")
        try:
            monkeypatch.setenv("OKG_MCP_RO_DSN", role_dsn)
            caplog.set_level("WARNING", logger="okg.substrate.mcp.server")
            server = build_server(dsn=okg_migrated, deployment_name=deployment)
            release_process_reservations(application_name=f"okg-mcp-{deployment}")
            server._okg_pool.close()
        finally:
            with db_connect(okg_migrated, autocommit=True) as conn:
                conn.execute(f"GRANT EXECUTE ON FUNCTION {claim} TO okg_mcp")

    warnings = [
        record.getMessage()
        for record in caplog.records
        if "reservation could not be recorded" in record.getMessage()
    ]
    assert len(warnings) == 1, warnings
    assert "permission denied" in warnings[0]
    assert f"okg migrate --deployment {deployment} --apply" in warnings[0]


def _owner_posture(dsn: str) -> dict[str, object]:
    """What a finished migration leaves for both SECURITY DEFINER owners."""
    with db_connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT p.oid::regprocedure::text, pg_get_userbyid(p.proowner),"
            "       (SELECT array_agg(a::text ORDER BY a::text)"
            "          FROM unnest(p.proacl) AS a),"
            "       p.proconfig::text"
            "  FROM pg_catalog.pg_proc AS p"
            " WHERE p.pronamespace = 'okg'::regnamespace"
            "   AND p.proname LIKE 'connection\\_reservation\\_%%'"
            " ORDER BY 1"
        )
        reservation_functions = cur.fetchall()
        cur.execute(
            "SELECT pg_get_userbyid(p.proowner), count(*)::int"
            "  FROM pg_catalog.pg_proc AS p"
            " WHERE p.pronamespace = 'okg'::regnamespace"
            "   AND p.proname LIKE 'external\\_action\\_%%' AND p.prosecdef"
            " GROUP BY 1"
        )
        external_owners = sorted(cur.fetchall())
        cur.execute(
            "SELECT has_schema_privilege(%s, 'okg', 'CREATE'),"
            "       has_schema_privilege(%s, 'okg', 'CREATE'),"
            "       has_table_privilege('okg_mcp', 'okg.connection_reservations',"
            "                           'SELECT,INSERT,UPDATE,DELETE')",
            (_FUNCTION_OWNER, _RESERVATION_OWNER),
        )
        create_and_table = cur.fetchone()
    return {
        "reservation_functions": reservation_functions,
        "external_owners": external_owners,
        "create_and_table": create_and_table,
    }


@pytest.mark.parametrize("owner", [_FUNCTION_OWNER, _RESERVATION_OWNER])
def test_a_non_superuser_migration_regains_set_on_an_owner_it_only_administers(
    okg_migrated: str, owner: str
) -> None:
    """PostgreSQL 16+: a CREATEROLE migration that creates an owner role gets
    only ADMIN OPTION on it, not SET or INHERIT, and ALTER FUNCTION ... OWNER
    TO then fails with `must be able to SET ROLE`. That is what a brand-new
    cluster gives okg_external_action_function_owner, and what an upgrade gives
    okg_connection_reservation_owner (#2359 re-review). The owner roles are
    cluster-wide and shared with every other test database, so this test does
    not drop them; it puts the migrator in that exact privilege state instead.
    Both a first migration and a replay must succeed and leave the same
    posture as the superuser-migrated session database.
    """
    expected = _owner_posture(okg_migrated)
    with _non_superuser_migrator(okg_migrated) as (role, role_dsn, superuser_dsn):
        with db_connect(
            _maintenance_dsn(okg_migrated), autocommit=True
        ) as conn, conn.cursor() as cur:
            cur.execute(
                sql.SQL("GRANT {} TO {} WITH INHERIT FALSE, SET FALSE").format(
                    sql.Identifier(owner), sql.Identifier(role)
                )
            )
            cur.execute(
                "SELECT pg_has_role(%s, %s, 'SET'), pg_has_role(%s, %s, 'USAGE'),"
                "       pg_has_role(%s, %s, 'MEMBER WITH ADMIN OPTION')",
                (role, owner) * 3,
            )
            assert cur.fetchone() == (False, False, True)

        first = migrate(role_dsn)
        assert first.pending_multiphase == ()
        migrate(role_dsn)
        actual = _owner_posture(superuser_dsn)

    assert actual == expected



def _wait_for(predicate, *, timeout: float = 15.0, step: float = 0.2):
    import time

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(step)
    return predicate()


def test_a_swept_mcp_claim_is_reclaimed_not_exited(
    okg_migrated: str, monkeypatch, caplog
) -> None:
    """A row swept after a laptop sleep, or lost to a database outage, must
    not kill the MCP server: the heartbeat re-claims with a new token, and a
    failed attempt is retried. The worker path keeps its hard exit."""
    from okg.substrate.db import capacity

    exits: list[int] = []
    monkeypatch.setattr(capacity.os, "_exit", exits.append)
    real_claim = capacity._claim_through_functions
    calls = {"n": 0}

    def _claim_failing_once(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 2:  # the first re-claim attempt: an outage
            raise psycopg.OperationalError("simulated outage")
        return real_claim(*args, **kwargs)

    monkeypatch.setattr(capacity, "_claim_through_functions", _claim_failing_once)
    caplog.set_level("INFO", logger="okg.substrate.db.capacity")
    name = f"okg-mcp-sweep{uuid.uuid4().hex[:8]}"
    try:
        with _disposable_mcp_login(okg_migrated) as (_role, role_dsn):
            capacity.reserve_process_demand(
                role_dsn,
                application_name=name,
                include_mcp=True,
                heartbeat_interval_seconds=1,
                mcp_read_role=True,
            )
            with db_connect(okg_migrated, autocommit=True) as conn:
                first = conn.execute(
                    "SELECT holder_token_sha256 FROM okg.connection_reservations"
                    " WHERE application_name = %s",
                    (name,),
                ).fetchone()[0]
                # What a sweep by another process does to a row older than 300 s.
                conn.execute(
                    "DELETE FROM okg.connection_reservations"
                    " WHERE application_name = %s",
                    (name,),
                )

            def _reclaimed():
                with db_connect(okg_migrated, autocommit=True) as conn:
                    row = conn.execute(
                        "SELECT holder_token_sha256 FROM okg.connection_reservations"
                        " WHERE application_name = %s",
                        (name,),
                    ).fetchone()
                return row

            second = _wait_for(_reclaimed)
            capacity.release_reservation(role_dsn, application_name=name)
            released = _reservation_rows(okg_migrated, name)
    finally:
        _delete_reservations(okg_migrated, name)

    messages = [record.getMessage() for record in caplog.records]
    assert exits == []
    assert second is not None and bytes(second[0]) != bytes(first)
    assert calls["n"] >= 3
    assert any("was lost" in m and "re-claiming" in m for m in messages), messages
    assert any("simulated outage" in m and "retrying" in m for m in messages)
    assert any("re-claimed" in m and "could not" not in m for m in messages)
    assert released == []


def _reservation_heartbeat_threads(name: str) -> list[str]:
    import threading

    return [
        thread.name for thread in threading.enumerate()
        if thread.is_alive() and thread.name == f"okg-reservation-heartbeat-{name}"
    ]


def test_a_budget_refused_mcp_reclaim_keeps_serving_and_retries(
    okg_migrated: str, monkeypatch, caplog
) -> None:
    """A re-claim refused for budget keeps the server serving, never exits,
    and keeps retrying every interval: the claim comes back once room frees.
    While refused it warns once (then at most every 10 minutes), and it logs
    the recovery once."""
    from okg.substrate.db import capacity

    exits: list[int] = []
    monkeypatch.setattr(capacity.os, "_exit", exits.append)
    caplog.set_level("INFO", logger="okg.substrate.db.capacity")
    name = f"okg-mcp-full{uuid.uuid4().hex[:8]}"
    filler = f"okg-worker-full{uuid.uuid4().hex[:8]}"
    interval = 1
    try:
        with _disposable_mcp_login(okg_migrated) as (_role, role_dsn):
            capacity.reserve_process_demand(
                role_dsn,
                application_name=name,
                include_mcp=True,
                heartbeat_interval_seconds=interval,
                mcp_read_role=True,
            )
            key = capacity._reservation_owner_key(role_dsn, name)
            with db_connect(okg_migrated, autocommit=True) as conn:
                usable = conn.execute(
                    "SELECT current_setting('max_connections')::int"
                    " - current_setting('superuser_reserved_connections')::int"
                ).fetchone()[0]
                conn.execute(
                    "DELETE FROM okg.connection_reservations"
                    " WHERE application_name = %s",
                    (name,),
                )
                conn.execute(
                    "INSERT INTO okg.connection_reservations"
                    " (application_name, pid, declared_demand) VALUES (%s, 4848, %s)",
                    (filler, usable - 1),
                )

            def _refusals():
                return [
                    record.getMessage() for record in caplog.records
                    if "could not be re-claimed" in record.getMessage()
                ]

            assert _wait_for(_refusals)
            # Several more refused attempts: still one warning (rate limited).
            import time

            time.sleep(3 * interval + 0.5)
            refused = _refusals()
            owner_registered = key in capacity._RESERVATION_OWNERS
            rows_while_refused = _reservation_rows(okg_migrated, name)

            with db_connect(okg_migrated, autocommit=True) as conn:
                conn.execute(
                    "DELETE FROM okg.connection_reservations"
                    " WHERE application_name = %s",
                    (filler,),
                )
            # Room is free again: the claim is back within 2 intervals.
            reclaimed = _wait_for(
                lambda: _reservation_rows(okg_migrated, name),
                timeout=2 * interval + 1.0,
            )
            recoveries = [
                record.getMessage() for record in caplog.records
                if "re-claimed after" in record.getMessage()
            ]
            capacity.release_reservation(role_dsn, application_name=name)
            after_close = _reservation_rows(okg_migrated, name)
    finally:
        _delete_reservations(okg_migrated, name, filler)

    messages = [record.getMessage() for record in caplog.records]
    assert exits == []
    assert len(refused) == 1, refused
    assert "retries every" in refused[0] and "raise max_connections" in refused[0]
    assert "restart" not in refused[0]
    assert owner_registered is False
    assert rows_while_refused == []
    assert reclaimed, "the claim did not come back within 2 intervals"
    assert len(recoveries) == 1 and "budget refusal" in recoveries[0], recoveries
    assert after_close == []
    assert not any("permission denied" in m for m in messages), messages


def test_closing_during_a_reclaim_leaves_no_claim_owner_or_thread(
    okg_migrated: str, monkeypatch, caplog
) -> None:
    """A server closing while its re-claim is inside the claim call: after
    close there is no row, no registered owner and no heartbeat thread, and no
    misleading privilege warning."""
    import threading

    from okg.substrate.db import capacity

    exits: list[int] = []
    monkeypatch.setattr(capacity.os, "_exit", exits.append)
    caplog.set_level("INFO", logger="okg.substrate.db.capacity")
    real_claim = capacity._claim_through_functions
    calls = {"n": 0}
    entered = threading.Event()
    gate = threading.Event()

    def _gated_claim(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] >= 2:  # the re-claim
            entered.set()
            gate.wait(10)
        return real_claim(*args, **kwargs)

    monkeypatch.setattr(capacity, "_claim_through_functions", _gated_claim)
    name = f"okg-mcp-close{uuid.uuid4().hex[:8]}"
    try:
        with _disposable_mcp_login(okg_migrated) as (_role, role_dsn):
            capacity.reserve_process_demand(
                role_dsn,
                application_name=name,
                include_mcp=True,
                heartbeat_interval_seconds=1,
                mcp_read_role=True,
            )
            _delete_reservations(okg_migrated, name)  # what a sweep does
            assert entered.wait(15), "the re-claim never started"
            closer = threading.Thread(
                target=capacity.release_reservation,
                args=(role_dsn,),
                kwargs={"application_name": name},
            )
            closer.start()
            import time

            time.sleep(0.5)
            gate.set()
            closer.join(30)
            assert not closer.is_alive()
            time.sleep(2.5)  # two more heartbeat intervals
            rows = _reservation_rows(okg_migrated, name)
            names = capacity._process_reservation_names()
            threads = _reservation_heartbeat_threads(name)
    finally:
        gate.set()
        _delete_reservations(okg_migrated, name)

    messages = [record.getMessage() for record in caplog.records]
    assert exits == []
    assert rows == []
    assert name not in names
    assert threads == []
    assert not any("permission denied" in m for m in messages), messages


def test_release_after_a_lost_claim_removes_the_old_row_through_the_function(
    okg_migrated: str, monkeypatch, caplog
) -> None:
    """The old row a lost claim leaves (database outage: the heartbeat lost
    its lease while the row still exists) is released through the function
    when the server closes, even if the re-claim loop could not release it."""
    import os

    from okg.substrate.db import capacity

    exits: list[int] = []
    monkeypatch.setattr(capacity.os, "_exit", exits.append)
    caplog.set_level("INFO", logger="okg.substrate.db.capacity")

    def _down(*args, **kwargs):
        raise psycopg.OperationalError("simulated outage")

    real_release = capacity._release_through_function
    blocked = {"on": True}

    def _release_during_outage(*args, **kwargs):
        if blocked["on"]:
            return False
        return real_release(*args, **kwargs)

    name = f"okg-mcp-outage{uuid.uuid4().hex[:8]}"
    try:
        with _disposable_mcp_login(okg_migrated) as (_role, role_dsn):
            capacity.reserve_process_demand(
                role_dsn,
                application_name=name,
                include_mcp=True,
                heartbeat_interval_seconds=1,
                mcp_read_role=True,
            )
            monkeypatch.setattr(capacity, "_claim_through_functions", _down)
            monkeypatch.setattr(
                capacity, "_release_through_function", _release_during_outage
            )
            pid = os.getpid()
            with db_connect(okg_migrated, autocommit=True) as conn:
                # Make one heartbeat miss the row, then put it back: the lease
                # is lost while the old row still exists.
                conn.execute(
                    "UPDATE okg.connection_reservations SET pid = %s"
                    " WHERE application_name = %s",
                    (pid + 100000, name),
                )
            assert _wait_for(lambda: any(
                "was lost" in r.getMessage() for r in caplog.records
            ))
            with db_connect(okg_migrated, autocommit=True) as conn:
                conn.execute(
                    "UPDATE okg.connection_reservations SET pid = %s"
                    " WHERE application_name = %s",
                    (pid, name),
                )
            assert _wait_for(lambda: any(
                "retrying" in r.getMessage() for r in caplog.records
            ))
            before_close = _reservation_rows(okg_migrated, name)
            blocked["on"] = False  # the database is back when the server closes
            capacity.release_reservation(role_dsn, application_name=name)
            after_close = _reservation_rows(okg_migrated, name)
            names = capacity._process_reservation_names()
    finally:
        _delete_reservations(okg_migrated, name)

    messages = [record.getMessage() for record in caplog.records]
    assert exits == []
    assert len(before_close) == 1
    assert after_close == []
    assert name not in names
    assert not any("permission denied" in m for m in messages), messages


def test_a_leaked_reservation_is_released_before_fixtures_and_named_after() -> None:
    """tests/conftest.py's leak reporter, driven directly. The claim is
    released BEFORE the test's fixtures tear down (a finalizer may drop the
    login it was made with). The test is named AFTER them, unless a fixture
    asked for that release, and still when a finalizer raises. The DSN is a
    closed port, so no database is touched and release is best-effort."""
    from okg.substrate.db import capacity
    from tests.conftest import _ConnectionReservationLeakReporter

    dsn = "host=127.0.0.1 port=1 dbname=none user=none connect_timeout=1"
    name = "okg-mcp-leaked-by-a-test"

    class _Item:
        nodeid = "tests/example.py::test_example"

        def __init__(self) -> None:
            self.warnings: list[Warning] = []

        def warn(self, warning: Warning) -> None:
            self.warnings.append(warning)

    def _leave_owner() -> None:
        capacity._start_reservation_owner(
            dsn,
            application_name=name,
            heartbeat_interval_seconds=3600,
            stale_after_seconds=7200,
            lease_lost_handler=lambda reason: None,
            holder_token="0" * 64,
        )

    def _teardown(item: _Item, finalizer) -> list[str]:
        hook = _ConnectionReservationLeakReporter().pytest_runtest_teardown(
            item, None
        )
        next(hook)
        # The fixture finalizers run here; the claim is already gone.
        held_during_finalizers = capacity._process_reservation_names()
        try:
            finalizer()
        except RuntimeError as exc:
            with pytest.raises(RuntimeError):
                hook.throw(exc)
        else:
            with pytest.raises(StopIteration):
                hook.send(None)
        return held_during_finalizers

    leaked = _Item()
    _leave_owner()
    assert _teardown(leaked, lambda: None) == []
    assert len(leaked.warnings) == 1
    assert "tests/example.py::test_example" in str(leaked.warnings[0])
    assert name in str(leaked.warnings[0])

    released_by_fixture = _Item()
    _leave_owner()
    assert _teardown(
        released_by_fixture,
        lambda: capacity.release_process_reservations(application_name=name),
    ) == []
    assert released_by_fixture.warnings == []

    finalizer_raised = _Item()
    _leave_owner()

    def _raising_finalizer() -> None:
        raise RuntimeError("finalizer failed")

    assert _teardown(finalizer_raised, _raising_finalizer) == []
    assert len(finalizer_raised.warnings) == 1
    assert capacity._process_reservation_names() == []
