from __future__ import annotations

import psycopg
import pytest
from psycopg import errors

from okg.substrate.exchange import (
    build_unsigned_envelope,
    generate_ed25519_keypair,
    sign_envelope,
)
from okg.substrate.federation.mailbox import MailboxService, PostgresMailboxStore
from okg.substrate.federation.mailbox.store import MAILBOX_SCHEMA_SQL


# Every test in this module uses the `clean_runtime` DB fixture (the
# filename says so). Mark the whole module so the non-postgres CI lane
# (`-k 'not postgres'`) skips it — per the marker-hygiene rule enforced by
# tests/substrate/test_marker_hygiene.py.
pytestmark = pytest.mark.postgres


def test_postgres_mailbox_standalone_schema_enforces_lease_shape(
    clean_runtime: str,
) -> None:
    assert "federation_mailbox_lease_shape_chk" in MAILBOX_SCHEMA_SQL

    conn = psycopg.connect(clean_runtime)
    try:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS okg.federation_mailbox_events")
            cur.execute(
                "DROP TABLE IF EXISTS okg.federation_mailbox_messages CASCADE"
            )
        PostgresMailboxStore.install_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT convalidated
                  FROM pg_constraint
                 WHERE conrelid = 'okg.federation_mailbox_messages'::regclass
                   AND conname = 'federation_mailbox_lease_shape_chk'
                """
            )
            assert cur.fetchone() == (False,)
        with conn.cursor() as cur, pytest.raises(errors.CheckViolation):
            cur.execute(
                """
                INSERT INTO okg.federation_mailbox_messages (
                  message_id, sender_authority_id, recipient_authority_id,
                  payload_mode, packet_sha256, expires_at, status
                )
                VALUES (
                  'msg-standalone-malformed-lease',
                  'okg:sender', 'okg:recipient',
                  'inline', 'packet-standalone-malformed-lease',
                  now() + interval '1 hour', 'leased'
                )
                """
            )
    finally:
        conn.rollback()
        conn.close()


def test_postgres_mailbox_store_records_lease_and_receipt(
    clean_runtime: str,
) -> None:
    keypair = generate_ed25519_keypair("personal-sign")
    envelope = build_unsigned_envelope(
        sender="okg:personal:riley",
        recipient="okg:dsg-lab",
        message_id="msg-postgres",
        ttl_seconds=300,
        body={
            "payload_kind": "federation.message.proposal.v1",
            "graph_id": "personal:dsg-postgres",
            "nodes": [{"node_id": "lab:dsg", "subtype": "organization"}],
            "edges": [],
        },
    )
    envelope = sign_envelope(envelope, keypair.private_key, kid=keypair.kid)

    with psycopg.connect(clean_runtime) as conn:
        service = MailboxService(store=PostgresMailboxStore(conn))
        service.submit_envelope(envelope)
        leased = service.poll("okg:dsg-lab", lease_seconds=30)
        assert len(leased) == 1
        acknowledged = service.ack_message(
            leased[0].message_id,
            lease_token=leased[0].lease_token or "",
            receipt_evidence={
                "receipt_event_id": "poros:receipt:postgres",
                "nomos_decision_id": "nomos:decision:postgres",
            },
        )
        assert acknowledged.acknowledged

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, receipt_evidence->>'receipt_event_id'
                  FROM okg.federation_mailbox_messages
                 WHERE message_id = %s
                """,
                ("msg-postgres",),
            )
            assert cur.fetchone() == ("acknowledged", "poros:receipt:postgres")
            cur.execute(
                """
                SELECT event_type
                  FROM okg.federation_mailbox_events
                 WHERE message_id = %s
                 ORDER BY event_id
                """,
                ("msg-postgres",),
            )
            assert [row[0] for row in cur.fetchall()] == [
                "submitted",
                "claimed",
                "acknowledged",
            ]
