"""Regression tests for the schema cleanup debt sprint."""
from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest
from psycopg import errors


pytestmark = pytest.mark.postgres

REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_SQL = REPO_ROOT / "src" / "okg" / "substrate" / "db" / "schema.sql"

NOMOS_DECISION_CHILD_FKS = {
    "federation_connection_requests": (
        "federation_connection_requests_nomos_decision_id_fkey",
        "nomos_decision_deployment_name",
    ),
    "federation_peer_trust_pins": (
        "federation_peer_trust_pins_nomos_decision_id_fkey",
        "nomos_decision_deployment_name",
    ),
    "federation_route_events": (
        "federation_route_events_nomos_decision_id_fkey",
        "nomos_decision_deployment_name",
    ),
    "poros_incorporation_events": (
        "poros_incorporation_events_nomos_decision_id_fkey",
        "nomos_decision_deployment_name",
    ),
    "nomos_external_claims": (
        "nomos_external_claims_nomos_decision_id_fkey",
        "deployment_name",
    ),
}

CHILD_INSERTS = {
    "federation_connection_requests": (
        """
        INSERT INTO okg.federation_connection_requests (
          request_id, local_authority_id, requester_authority_id,
          target_authority_id, requested_action, target_address,
          requested_visibility, key_fingerprint, status, nomos_decision_id
        )
        VALUES (
          %s, 'okg:local', 'okg:requester', 'okg:target',
          'federation.message.send', 'okg://target/boards/public',
          'public', 'fingerprint-1', 'approved', %s
        )
        """,
        ("request-schema-cleanup",),
    ),
    "federation_peer_trust_pins": (
        """
        INSERT INTO okg.federation_peer_trust_pins (
          local_authority_id, remote_authority_id, key_fingerprint,
          allowed_actions, status, nomos_decision_id
        )
        VALUES (
          'okg:local', 'okg:remote', %s,
          ARRAY['federation.message.send']::TEXT[], 'active', %s
        )
        """,
        ("fingerprint-schema-cleanup",),
    ),
    "federation_route_events": (
        """
        INSERT INTO okg.federation_route_events (
          route_event_id, local_authority_id, remote_authority_id,
          subject, action, target_address, visibility, decision,
          reason_code, nomos_decision_id
        )
        VALUES (
          %s, 'okg:local', 'okg:remote', 'okg:subject',
          'federation.message.send', 'okg://target/boards/public',
          'public', 'allow', 'schema.cleanup.allow', %s
        )
        """,
        ("route-schema-cleanup",),
    ),
    "poros_incorporation_events": (
        """
        INSERT INTO okg.poros_incorporation_events (
          receiver_authority_id, sender_authority_id, message_id, event_type,
          status, reason_code, nomos_decision_id
        )
        VALUES (
          'okg:receiver', 'okg:sender', %s, 'nomos_decided',
          'allow', 'schema.cleanup.allow', %s
        )
        """,
        ("message-schema-cleanup",),
    ),
    "nomos_external_claims": (
        """
        INSERT INTO okg.nomos_external_claims (
          deployment_name, provider_id, provider_type, issuer,
          external_subject, claim_fingerprint, verification_status,
          nomos_decision_id
        )
        VALUES (
          'default', 'provider-schema-cleanup', 'oidc',
          'https://issuer.example', 'subject-schema-cleanup', %s,
          'verified', %s
        )
        """,
        ("claim-schema-cleanup",),
    ),
}


def _clear_schema_cleanup_tables(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            TRUNCATE
              okg.federation_route_events,
              okg.federation_peer_trust_pins,
              okg.federation_connection_requests,
              okg.poros_incorporation_events,
              okg.nomos_external_claims,
              okg.nomos_decisions
            RESTART IDENTITY CASCADE
            """
        )


@pytest.fixture
def schema_cleanup_dsn(okg_migrated: str) -> Iterator[str]:
    _clear_schema_cleanup_tables(okg_migrated)
    try:
        yield okg_migrated
    finally:
        _clear_schema_cleanup_tables(okg_migrated)


def _insert_decision(
    cur,
    decision_id: str,
    *,
    deployment_name: str = "default",
) -> None:
    cur.execute(
        """
        INSERT INTO okg.nomos_decisions (
          deployment_name, decision_id, input_hash, subject, action,
          classification, effect, reason_code, enforcement_point
        )
        VALUES (
          %s, %s, %s, 'okg:subject', 'schema.cleanup',
          'public', 'allow', 'schema.cleanup.allow', 'schema-test'
        )
        """,
        (deployment_name, decision_id, f"hash:{deployment_name}:{decision_id}"),
    )


def test_nodes_live_indexes_are_gone_from_the_schema_source() -> None:
    """The duplicate-index debt is settled by deletion, not deduplication.

    This asserted that `nodes_live_current_subtype_idx` was declared EXACTLY
    once (it had been declared twice, and the second declaration was the debt).
    `schema-canonical-read-path` deleted okg.nodes_live and every index on it,
    so the right assertion is now zero. Kept rather than removed because the
    original defect was a duplicate CREATE INDEX slipping into the snapshot,
    and this is the file where that class is caught.
    """
    schema = SCHEMA_SQL.read_text(encoding="utf-8")

    assert "nodes_live_current_subtype_idx" not in schema
    assert "ON okg.nodes_live" not in schema
    assert "ON okg.edges_live" not in schema


def test_nodes_live_current_subtype_index_has_no_physical_duplicate(
    okg_migrated: str,
) -> None:
    with psycopg.connect(okg_migrated) as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass('okg.nodes_live')")
        nodes_live_exists = cur.fetchone()[0] is not None
        cur.execute(
            """
            SELECT indexname, indexdef
              FROM pg_indexes
             WHERE schemaname = 'okg'
               AND tablename = 'nodes_live'
               AND indexname = 'nodes_live_current_subtype_idx'
            """
        )
        rows = cur.fetchall()

        cur.execute(
            """
            SELECT indexname
              FROM pg_indexes
             WHERE schemaname = 'okg'
               AND tablename = 'nodes_live'
               AND indexdef LIKE '%ON okg.nodes_live USING btree (subtype, node_id)%'
               AND indexdef LIKE '%valid_to_gen IS NULL%'
            """
        )
        subtype_current_indexes = [row[0] for row in cur.fetchall()]

    if not nodes_live_exists:
        assert rows == []
        assert subtype_current_indexes == []
        return

    assert len(rows) == 1
    assert "USING btree (subtype, node_id)" in rows[0][1]
    assert "valid_to_gen IS NULL" in rows[0][1]
    assert subtype_current_indexes == ["nodes_live_current_subtype_idx"]


def test_nomos_decision_fk_constraints_are_migrated(
    okg_migrated: str,
) -> None:
    expected_constraints = {
        constraint_name
        for constraint_name, _deployment_column in NOMOS_DECISION_CHILD_FKS.values()
    }
    with psycopg.connect(okg_migrated) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT conname, pg_get_constraintdef(oid)
              FROM pg_constraint
             WHERE connamespace = 'okg'::regnamespace
               AND conrelid = 'okg.nomos_decisions'::regclass
               AND pg_get_constraintdef(oid) = 'UNIQUE (deployment_name, decision_id)'
            """
        )
        parent_constraint = cur.fetchone()

        cur.execute(
            """
            SELECT 1
              FROM pg_constraint
             WHERE connamespace = 'okg'::regnamespace
               AND conrelid = 'okg.nomos_decisions'::regclass
               AND conname = 'nomos_decisions_decision_id_key'
            """
        )
        global_decision_key = cur.fetchone()

        cur.execute(
            """
            SELECT child.relname, con.conname, pg_get_constraintdef(con.oid)
              FROM pg_constraint con
              JOIN pg_class child ON child.oid = con.conrelid
              JOIN pg_namespace ns ON ns.oid = child.relnamespace
             WHERE ns.nspname = 'okg'
               AND con.contype = 'f'
               AND con.conname = ANY(%s)
             ORDER BY child.relname
            """,
            (list(expected_constraints),),
        )
        child_constraints = cur.fetchall()

    assert parent_constraint is not None
    assert parent_constraint[1] == "UNIQUE (deployment_name, decision_id)"
    assert global_decision_key is None

    by_table = {row[0]: (row[1], row[2]) for row in child_constraints}
    assert set(by_table) == set(NOMOS_DECISION_CHILD_FKS)
    for table, (
        constraint_name,
        deployment_column,
    ) in NOMOS_DECISION_CHILD_FKS.items():
        actual_name, definition = by_table[table]
        assert actual_name == constraint_name
        assert (
            f"FOREIGN KEY ({deployment_column}, nomos_decision_id)"
            in definition
        )
        assert (
            "REFERENCES okg.nomos_decisions(deployment_name, decision_id)"
            in definition
        )


def test_nomos_decision_ids_can_repeat_across_deployments(
    schema_cleanup_dsn: str,
) -> None:
    decision_id = "nomos:decision:shared-content-hash"
    with psycopg.connect(schema_cleanup_dsn) as conn, conn.cursor() as cur:
        _insert_decision(cur, decision_id, deployment_name="default")
        _insert_decision(cur, decision_id, deployment_name="other")
        cur.execute(CHILD_INSERTS["poros_incorporation_events"][0], (
            *CHILD_INSERTS["poros_incorporation_events"][1],
            decision_id,
        ))
        conn.commit()


def test_nomos_decision_fk_is_deployment_scoped(
    schema_cleanup_dsn: str,
) -> None:
    decision_id = "nomos:decision:default-only"
    with psycopg.connect(schema_cleanup_dsn) as conn, conn.cursor() as cur:
        _insert_decision(cur, decision_id, deployment_name="default")
        with pytest.raises(errors.ForeignKeyViolation):
            cur.execute(
                """
                INSERT INTO okg.poros_incorporation_events (
                  receiver_authority_id, sender_authority_id, message_id,
                  event_type, status, nomos_decision_id,
                  nomos_decision_deployment_name
                )
                VALUES (
                  'okg:receiver', 'okg:sender', 'message-other-deployment',
                  'nomos_decided', 'allow', %s, 'other'
                )
                """,
                (decision_id,),
            )


def _force_schema_body_replay(dsn: str) -> None:
    """Invalidate the recorded executable-schema checksum so the next migrate
    re-runs the schema body (make-migrate-idempotent, design ruling 1).

    The checksum fast path skips ``cur.execute(executable_sql)`` — and therefore
    the Nomos FK install / orphan-validation block — on an unchanged upgrade.
    The orphan guard runs only on a cutover run (checksum mismatch / fresh DB),
    which is exactly what this manually-dropped-FK scenario simulates, so the
    test forces a body replay before each migrate.
    """
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE okg.schema_migrations
               SET result_metadata =
                   jsonb_set(
                     result_metadata,
                     '{executable_schema_checksum}',
                     '"force-cutover-replay"'
                   )
             WHERE migration_id = '0000_schema_snapshot'
            """
        )


def test_nomos_decision_migration_guard_rejects_existing_orphans(
    schema_cleanup_dsn: str,
) -> None:
    from okg.substrate.db.migrate import migrate

    with psycopg.connect(schema_cleanup_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            ALTER TABLE okg.poros_incorporation_events
              DROP CONSTRAINT IF EXISTS
                poros_incorporation_events_nomos_decision_id_fkey
            """
        )
        cur.execute(
            """
            INSERT INTO okg.poros_incorporation_events (
              receiver_authority_id, sender_authority_id, message_id,
              event_type, status, nomos_decision_id,
              nomos_decision_deployment_name
            )
            VALUES (
              'okg:receiver', 'okg:sender', 'message-orphan-before-migrate',
              'nomos_decided', 'allow', 'nomos:decision:orphan', 'default'
            )
            """
        )
        conn.commit()

    try:
        _force_schema_body_replay(schema_cleanup_dsn)
        with pytest.raises(errors.ForeignKeyViolation, match="orphan rows exist"):
            migrate(schema_cleanup_dsn)
    finally:
        with psycopg.connect(schema_cleanup_dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM okg.poros_incorporation_events
                 WHERE message_id = 'message-orphan-before-migrate'
                """
            )
            conn.commit()
        # Force the body to run so the dropped FK is re-installed (a converged
        # migrate would skip the body and leave it dropped).
        _force_schema_body_replay(schema_cleanup_dsn)
        migrate(schema_cleanup_dsn)


@pytest.mark.parametrize("table", sorted(CHILD_INSERTS))
def test_nomos_decision_fk_rejects_orphans(
    schema_cleanup_dsn: str,
    table: str,
) -> None:
    sql_text, params = CHILD_INSERTS[table]
    with psycopg.connect(schema_cleanup_dsn) as conn, conn.cursor() as cur:
        with pytest.raises(errors.ForeignKeyViolation):
            cur.execute(sql_text, (*params, f"nomos:decision:missing:{table}"))


@pytest.mark.parametrize("table", sorted(CHILD_INSERTS))
def test_nomos_decision_fk_accepts_existing_parent(
    schema_cleanup_dsn: str,
    table: str,
) -> None:
    sql_text, params = CHILD_INSERTS[table]
    decision_id = f"nomos:decision:schema-cleanup:{table}"
    with psycopg.connect(schema_cleanup_dsn) as conn, conn.cursor() as cur:
        _insert_decision(cur, decision_id)
        cur.execute(sql_text, (*params, decision_id))
        conn.commit()
