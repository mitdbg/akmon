"""Database ownership proof: run only in an explicitly owned disposable cluster."""
import json
import os

import psycopg
import pytest

from okg.substrate.distributions.install_execution import _database_identity
from okg.substrate.errors import SubstrateError


@pytest.mark.postgres
def test_pristine_extension_metadata_is_not_foreign_application_data():
    dsn = os.environ.get("OKG_INSTALL_TEST_DSN")
    expected = os.environ.get("OKG_INSTALL_TEST_DATABASE_IDENTITY")
    if not dsn or not expected:
        pytest.skip("requires explicit disposable cluster database identity")
    identity = tuple(json.loads(expected))
    assert identity[0].startswith("s14_packaged")
    with psycopg.connect(dsn, autocommit=True) as conn:
        actual = conn.execute(
            "SELECT current_database(),(SELECT oid FROM pg_database WHERE datname=current_database()),"
            "(SELECT system_identifier FROM pg_control_system())"
        ).fetchone()
        assert actual == identity
        assert {row[0] for row in conn.execute("SELECT extname FROM pg_extension")} == {"plpgsql"}
        assert _database_identity(dsn, initial=True, authorize_cluster_bootstrap=True)["oid"] == identity[1]
        conn.execute("CREATE EXTENSION timescaledb")
        try:
            assert _database_identity(dsn, initial=True, authorize_cluster_bootstrap=True)["oid"] == identity[1]
            for create, drop in (
                ("CREATE TABLE public.install_guard_table (id int)", "DROP TABLE public.install_guard_table"),
                ("CREATE VIEW public.install_guard_view AS SELECT 1 AS id", "DROP VIEW public.install_guard_view"),
                ("CREATE SEQUENCE public.install_guard_sequence", "DROP SEQUENCE public.install_guard_sequence"),
                ("CREATE TABLE _timescaledb_catalog.install_guard_user_table (id int)",
                 "DROP TABLE _timescaledb_catalog.install_guard_user_table"),
            ):
                conn.execute(create)
                try:
                    with pytest.raises(SubstrateError) as refused:
                        _database_identity(dsn, initial=True, authorize_cluster_bootstrap=True)
                    assert refused.value.data["reason"] == "initial_database_not_empty"
                finally:
                    conn.execute(drop)
            conn.execute("CREATE EXTENSION hstore")
            try:
                with pytest.raises(SubstrateError) as refused:
                    _database_identity(dsn, initial=True, authorize_cluster_bootstrap=True)
                assert refused.value.data["reason"] == "initial_database_foreign_extension"
            finally:
                conn.execute("DROP EXTENSION hstore")
        finally:
            conn.execute("DROP EXTENSION timescaledb")


@pytest.mark.postgres
def test_source_admission_mutations_share_publication_fence():
    """Use two actual sessions in the explicitly owned cluster; no mocked locks."""
    from uuid import uuid4
    from okg.substrate.admission import (
        FactRangeRef, attach_fact_range_refs, attach_fact_range_refs_for_run, attach_payload_ref,
        ensure_envelope, transition_envelope,
    )
    from okg.substrate.db.migrate import ensure_required_extensions, migrate
    from okg.substrate.ingest.service import _record_review_submitted

    dsn = os.environ.get("OKG_INSTALL_TEST_DSN")
    expected = os.environ.get("OKG_INSTALL_TEST_DATABASE_IDENTITY")
    if not dsn or not expected:
        pytest.skip("requires explicit disposable cluster database identity")
    identity = tuple(json.loads(expected))
    assert identity[0].startswith("s14_packaged")
    with psycopg.connect(dsn, autocommit=True) as check:
        assert check.execute(
            "SELECT current_database(),(SELECT oid FROM pg_database WHERE datname=current_database()),"
            "(SELECT system_identifier FROM pg_control_system())"
        ).fetchone() == identity
    ensure_required_extensions(dsn)
    migrate(dsn)
    key = uuid4().hex
    def create(conn, suffix, kind="source", branch="default"):
        return ensure_envelope(conn, deployment_name="s14-fence", producer_id=key,
            producer_kind=kind, intent="source_sync", admission_mode="fast_track",
            status="pending", idempotency_key=suffix, source=key, target_branch_name=branch)
    lock = "SELECT pg_advisory_lock(hashtext('okg.chronos_graph'),hashtext('default'))"
    unlock = "SELECT pg_advisory_unlock(hashtext('okg.chronos_graph'),hashtext('default'))"
    attempt = "SELECT pg_try_advisory_lock(hashtext('okg.chronos_graph'),hashtext('default'))"
    with psycopg.connect(dsn, autocommit=True) as publisher, psycopg.connect(dsn, autocommit=True) as writer:
        envelope = create(writer, "existing")
        aid = envelope.admission_id
        refs = [FactRangeRef("node_fact_range", "range", key, str(uuid4()), 1, 1, 1)]
        operations = [
            lambda: create(writer, "phantom"),
            lambda: transition_envelope(writer, admission_id=aid, status="rejected"),
            lambda: attach_fact_range_refs(writer, admission_id=aid, refs=refs),
            lambda: attach_fact_range_refs_for_run(writer, admission_id=aid, source=key, run_id=refs[0].run_id),
            lambda: attach_payload_ref(writer, admission_id=aid, payload={"kind": "chronos_graph_run"}),
            lambda: _record_review_submitted(writer, admission_id=aid, actor="test", verdict={"ok": True}, workflow_ref={}),
        ]
        def snapshot():
            # admission-envelope-lint: allow issue #1790 (PACT sprint14-fixture-publish-proof): this read MEANS to match
            # every envelope of the producer, ordered, to prove the publication fence blocked all writers.
            return publisher.execute("SELECT admission_id::text,status,verdict FROM okg.admission_envelopes WHERE producer_id=%s ORDER BY admission_id", (key,)).fetchall(), publisher.execute("SELECT count(*) FROM okg.admission_record_refs WHERE admission_id=%s::uuid", (aid,)).fetchone()
        publisher.execute(lock)
        writer.execute("SET lock_timeout='150ms'")
        before = snapshot()
        try:
            for operation in operations:
                with pytest.raises(psycopg.errors.LockNotAvailable):
                    operation()
                assert snapshot() == before
            lens = create(writer, "lens", kind="lens")
            transition_envelope(writer, admission_id=lens.admission_id, status="rejected")
            create(writer, "other-branch", branch="isolated")
            # Same-session nesting is reentrant, including source status feedback.
            owned = create(publisher, "reentrant")
            transition_envelope(publisher, admission_id=owned.admission_id, status="accepted")
        finally:
            publisher.execute(unlock)
        for operation in operations:
            operation()
        assert create(writer, "phantom").inserted is False
        with pytest.raises(SubstrateError, match="coalesced admission identity"):
            create(writer, "phantom", branch="different")
        # Returning from a nested helper must not release its outer transaction's fence.
        with writer.transaction():
            create(writer, "outer-commit")
            assert publisher.execute(attempt).fetchone() == (False,)
        assert publisher.execute(attempt).fetchone() == (True,)
        publisher.execute(unlock)
        class RollbackProbe(Exception):
            pass

        def probe_rollback() -> None:
            raise RollbackProbe
        with pytest.raises(RollbackProbe):
            with writer.transaction():
                create(writer, "outer-rollback")
                assert publisher.execute(attempt).fetchone() == (False,)
                probe_rollback()
        assert publisher.execute(attempt).fetchone() == (True,)
        publisher.execute(unlock)
        assert create(writer, "outer-rollback").inserted is True
