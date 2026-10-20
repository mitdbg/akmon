from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import psycopg
import pytest
from psycopg import errors
from psycopg.types.json import Jsonb

from okg.substrate.exchange.inbox import PorosInboxResult
from okg.substrate.exchange.inbox_store import persist_poros_inbox_preflight


pytestmark = pytest.mark.postgres


def _jsonb(value: Mapping[str, Any]) -> Jsonb:
    return Jsonb(dict(value))


def _assert_check_violation(cur, statement: str, params: tuple[Any, ...]) -> None:
    with pytest.raises(errors.CheckViolation):
        cur.execute(statement, params)


def test_mailbox_payload_mode_shape_constraints(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.federation_mailbox_messages (
              message_id, sender_authority_id, recipient_authority_id,
              payload_mode, packet_sha256, body_sha256, envelope, expires_at
            )
            VALUES (
              'mailbox-inline-valid', 'okg:sender', 'okg:receiver',
              'inline', 'packet-inline-valid', 'body-inline-valid',
              %s, now() + interval '1 hour'
            )
            """,
            (_jsonb({"message_id": "mailbox-inline-valid"}),),
        )
        cur.execute(
            """
            INSERT INTO okg.federation_mailbox_messages (
              message_id, sender_authority_id, recipient_authority_id,
              payload_mode, packet_sha256, pointer, object_uri,
              object_sha256, size_bytes, expires_at
            )
            VALUES (
              'mailbox-detached-valid', 'okg:sender', 'okg:receiver',
              'detached_pointer', 'packet-detached-valid', %s,
              's3://bucket/object', 'object-sha', 42,
              now() + interval '1 hour'
            )
            """,
            (_jsonb({"object_uri": "s3://bucket/object"}),),
        )

        _assert_check_violation(
            cur,
            """
            INSERT INTO okg.federation_mailbox_messages (
              message_id, sender_authority_id, recipient_authority_id,
              payload_mode, packet_sha256, envelope, pointer, object_uri,
              object_sha256, size_bytes, expires_at
            )
            VALUES (
              'mailbox-inline-with-pointer', 'okg:sender', 'okg:receiver',
              'inline', 'packet-inline-with-pointer', %s, %s,
              's3://bucket/object', 'object-sha', 42,
              now() + interval '1 hour'
            )
            """,
            (
                _jsonb({"message_id": "mailbox-inline-with-pointer"}),
                _jsonb({"object_uri": "s3://bucket/object"}),
            ),
        )
        _assert_check_violation(
            cur,
            """
            INSERT INTO okg.federation_mailbox_messages (
              message_id, sender_authority_id, recipient_authority_id,
              payload_mode, packet_sha256, pointer, object_uri,
              object_sha256, size_bytes, expires_at
            )
            VALUES (
              'mailbox-detached-missing-object', 'okg:sender', 'okg:receiver',
              'detached_pointer', 'packet-detached-missing-object',
              %s, NULL, NULL, NULL, now() + interval '1 hour'
            )
            """,
            (_jsonb({"object_uri": "s3://bucket/object"}),),
        )
        _assert_check_violation(
            cur,
            """
            INSERT INTO okg.federation_mailbox_messages (
              message_id, sender_authority_id, recipient_authority_id,
              payload_mode, packet_sha256, envelope, pointer, object_uri,
              object_sha256, size_bytes, expires_at
            )
            VALUES (
              'mailbox-detached-with-envelope', 'okg:sender', 'okg:receiver',
              'detached_pointer', 'packet-detached-with-envelope', %s, %s,
              's3://bucket/object', 'object-sha', 42,
              now() + interval '1 hour'
            )
            """,
            (
                _jsonb({"message_id": "mailbox-detached-with-envelope"}),
                _jsonb({"object_uri": "s3://bucket/object"}),
            ),
        )


def test_mailbox_lifecycle_evidence_constraints(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.federation_mailbox_messages (
              message_id, sender_authority_id, recipient_authority_id,
              payload_mode, packet_sha256, envelope, status,
              lease_token, lease_expires_at, expires_at
            )
            VALUES (
              'mailbox-leased-valid', 'okg:sender', 'okg:receiver',
              'inline', 'packet-leased-valid', %s, 'leased',
              'lease-token', now() + interval '10 minutes',
              now() + interval '1 hour'
            )
            """,
            (_jsonb({"message_id": "mailbox-leased-valid"}),),
        )
        cur.execute(
            """
            INSERT INTO okg.federation_mailbox_messages (
              message_id, sender_authority_id, recipient_authority_id,
              payload_mode, packet_sha256, envelope, status,
              receipt_evidence, expires_at
            )
            VALUES (
              'mailbox-ack-valid', 'okg:sender', 'okg:receiver',
              'inline', 'packet-ack-valid', %s, 'acknowledged',
              %s, now() + interval '1 hour'
            )
            """,
            (
                _jsonb({"message_id": "mailbox-ack-valid"}),
                _jsonb({"receipt_event_id": "receipt-1"}),
            ),
        )
        cur.execute(
            """
            INSERT INTO okg.federation_mailbox_messages (
              message_id, sender_authority_id, recipient_authority_id,
              payload_mode, packet_sha256, envelope, status,
              denial_evidence, deny_reason_code, expires_at
            )
            VALUES (
              'mailbox-denied-valid', 'okg:sender', 'okg:receiver',
              'inline', 'packet-denied-valid', %s, 'denied',
              %s, 'nomos.denied', now() + interval '1 hour'
            )
            """,
            (
                _jsonb({"message_id": "mailbox-denied-valid"}),
                _jsonb({"reason": "policy"}),
            ),
        )

        _assert_check_violation(
            cur,
            """
            INSERT INTO okg.federation_mailbox_messages (
              message_id, sender_authority_id, recipient_authority_id,
              payload_mode, packet_sha256, envelope, status, expires_at
            )
            VALUES (
              'mailbox-leased-no-lease', 'okg:sender', 'okg:receiver',
              'inline', 'packet-leased-no-lease', %s, 'leased',
              now() + interval '1 hour'
            )
            """,
            (_jsonb({"message_id": "mailbox-leased-no-lease"}),),
        )
        _assert_check_violation(
            cur,
            """
            INSERT INTO okg.federation_mailbox_messages (
              message_id, sender_authority_id, recipient_authority_id,
              payload_mode, packet_sha256, envelope, status, expires_at
            )
            VALUES (
              'mailbox-ack-no-receipt', 'okg:sender', 'okg:receiver',
              'inline', 'packet-ack-no-receipt', %s, 'acknowledged',
              now() + interval '1 hour'
            )
            """,
            (_jsonb({"message_id": "mailbox-ack-no-receipt"}),),
        )
        _assert_check_violation(
            cur,
            """
            INSERT INTO okg.federation_mailbox_messages (
              message_id, sender_authority_id, recipient_authority_id,
              payload_mode, packet_sha256, envelope, status,
              denial_evidence, expires_at
            )
            VALUES (
              'mailbox-denied-no-reason', 'okg:sender', 'okg:receiver',
              'inline', 'packet-denied-no-reason', %s, 'denied',
              %s, now() + interval '1 hour'
            )
            """,
            (
                _jsonb({"message_id": "mailbox-denied-no-reason"}),
                _jsonb({"reason": "policy"}),
            ),
        )


def test_poros_inbox_status_vocabulary_constraint(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        for status in (
            "accepted_for_admission",
            "rejected_preflight",
            "duplicate",
            "quarantined",
        ):
            cur.execute(
                """
                INSERT INTO okg.poros_inbox_messages (
                  receiver_authority_id, sender_authority_id, message_id,
                  status, envelope_sha256, envelope
                )
                VALUES (
                  'okg:receiver', 'okg:sender', %s, %s, %s, %s
                )
                """,
                (
                    f"poros-inbox-{status}",
                    status,
                    f"envelope-{status}",
                    _jsonb({"message_id": f"poros-inbox-{status}"}),
                ),
            )

        _assert_check_violation(
            cur,
            """
            INSERT INTO okg.poros_inbox_messages (
              receiver_authority_id, sender_authority_id, message_id,
              status, envelope_sha256, envelope
            )
            VALUES (
              'okg:receiver', 'okg:sender', 'poros-inbox-made-up',
              'made_up', 'envelope-made-up', %s
            )
            """,
            (_jsonb({"message_id": "poros-inbox-made-up"}),),
        )


def test_detached_manifest_classification_and_encryption_constraints(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.poros_detached_object_manifests (
              message_id, body_sha256, authority_id, side, object_id,
              size_bytes, object_sha256, classification, encrypted, manifest
            )
            VALUES (
              'manifest-public-valid', 'body-public', 'okg:sender', 'sender',
              'object-public', 5, 'clear-hash', 'public', false, %s
            )
            """,
            (
                _jsonb({
                    "object_id": "object-public",
                    "size_bytes": 5,
                    "cleartext_sha256": "clear-hash",
                    "classification": "public",
                    "chunks": [],
                }),
            ),
        )
        cur.execute(
            """
            INSERT INTO okg.poros_detached_object_manifests (
              message_id, body_sha256, authority_id, side, object_id,
              size_bytes, object_sha256, classification, encrypted, manifest
            )
            VALUES (
              'manifest-encrypted-valid', 'body-private', 'okg:sender', 'sender',
              'object-private', 5, 'cipher-hash', 'restricted', true, %s
            )
            """,
            (
                _jsonb({
                    "object_id": "object-private",
                    "size_bytes": 5,
                    "ciphertext_sha256": "cipher-hash",
                    "classification": "restricted",
                    "encryption": {"alg": "hpke-test"},
                    "chunks": [],
                }),
            ),
        )

        _assert_check_violation(
            cur,
            """
            INSERT INTO okg.poros_detached_object_manifests (
              message_id, body_sha256, authority_id, side, object_id,
              size_bytes, object_sha256, classification, encrypted, manifest
            )
            VALUES (
              'manifest-bad-classification', 'body-secret', 'okg:sender',
              'sender', 'object-secret', 5, 'secret-hash', 'secret',
              false, %s
            )
            """,
            (
                _jsonb({
                    "object_id": "object-secret",
                    "size_bytes": 5,
                    "cleartext_sha256": "secret-hash",
                    "classification": "secret",
                    "chunks": [],
                }),
            ),
        )
        _assert_check_violation(
            cur,
            """
            INSERT INTO okg.poros_detached_object_manifests (
              message_id, body_sha256, authority_id, side, object_id,
              size_bytes, object_sha256, classification, encrypted, manifest
            )
            VALUES (
              'manifest-classification-mismatch', 'body-mismatch',
              'okg:sender', 'sender', 'object-mismatch', 5,
              'clear-mismatch', 'public', false, %s
            )
            """,
            (
                _jsonb({
                    "object_id": "object-mismatch",
                    "size_bytes": 5,
                    "cleartext_sha256": "clear-mismatch",
                    "classification": "restricted",
                    "chunks": [],
                }),
            ),
        )
        _assert_check_violation(
            cur,
            """
            INSERT INTO okg.poros_detached_object_manifests (
              message_id, body_sha256, authority_id, side, object_id,
              size_bytes, object_sha256, classification, encrypted, manifest
            )
            VALUES (
              'manifest-encrypted-without-crypto', 'body-bad-encrypted',
              'okg:sender', 'sender', 'object-bad-encrypted', 5,
              'clear-hash', 'restricted', true, %s
            )
            """,
            (
                _jsonb({
                    "object_id": "object-bad-encrypted",
                    "size_bytes": 5,
                    "cleartext_sha256": "clear-hash",
                    "classification": "restricted",
                    "chunks": [],
                }),
            ),
        )


def test_rejected_preflight_with_malformed_detached_manifest_keeps_evidence(
    clean_runtime: str,
) -> None:
    envelope = {
        "message_id": "poros-rejected-malformed-detached",
        "sender": "okg:sender",
        "recipient": "okg:receiver",
        "expires_at": "2030-01-01T00:00:00Z",
        "body": {
            "payload_mode": "detached",
            "objects": [
                {
                    "object_id": "object-missing-hash",
                    "size_bytes": 5,
                    "classification": "public",
                    "chunks": [],
                },
                {
                    "object_id": "object-bad-size",
                    "size_bytes": "bad",
                    "classification": "public",
                    "cleartext_sha256": "object-bad-size-hash",
                    "chunks": [],
                },
                {
                    "object_id": "object-empty-classification",
                    "size_bytes": 5,
                    "classification": "",
                    "cleartext_sha256": "object-empty-classification-hash",
                    "chunks": [],
                },
                {
                    "object_id": "object-null-classification",
                    "size_bytes": 5,
                    "classification": None,
                    "cleartext_sha256": "object-null-classification-hash",
                    "chunks": [],
                }
            ],
        },
    }
    result = PorosInboxResult(
        status="rejected_preflight",
        reason_code="invalid_detached_object_manifest",
        admission_candidate=None,
        receipt={
            "type": "receipt",
            "status": "rejected_preflight",
            "reason_code": "invalid_detached_object_manifest",
        },
    )

    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        persisted = persist_poros_inbox_preflight(
            conn,
            receiver_authority_id="okg:receiver",
            envelope=envelope,
            result=result,
        )

        assert persisted.status == "rejected_preflight"
        assert persisted.reason_code == "invalid_detached_object_manifest"
        cur.execute(
            """
            SELECT status, reason_code, envelope->'body'->'objects'->0->>'object_id'
              FROM okg.poros_inbox_messages
             WHERE message_id = 'poros-rejected-malformed-detached'
            """
        )
        assert cur.fetchone() == (
            "rejected_preflight",
            "invalid_detached_object_manifest",
            "object-missing-hash",
        )
        cur.execute(
            """
            SELECT count(*)
              FROM okg.poros_detached_object_manifests
             WHERE message_id = 'poros-rejected-malformed-detached'
            """
        )
        assert cur.fetchone() == (0,)
        cur.execute(
            """
            SELECT event_type, status, reason_code
              FROM okg.poros_incorporation_events
             WHERE message_id = 'poros-rejected-malformed-detached'
            """
        )
        assert cur.fetchall() == [
            (
                "preflight_rejected",
                "rejected_preflight",
                "invalid_detached_object_manifest",
            )
        ]


def test_exchange_boundary_violation_view_reports_legacy_rows(
    clean_runtime: str,
) -> None:
    constraint_sql = """
        ALTER TABLE okg.poros_inbox_messages
          ADD CONSTRAINT poros_inbox_messages_status_chk
            CHECK (status IN (
              'accepted_for_admission',
              'rejected_preflight',
              'duplicate',
              'quarantined'
            )) NOT VALID
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            ALTER TABLE okg.poros_inbox_messages
              DROP CONSTRAINT IF EXISTS poros_inbox_messages_status_chk
            """
        )
        try:
            cur.execute(
                """
                INSERT INTO okg.poros_inbox_messages (
                  receiver_authority_id, sender_authority_id, message_id,
                  status, envelope_sha256, envelope
                )
                VALUES (
                  'okg:receiver', 'okg:sender', 'poros-inbox-legacy-bad',
                  'legacy_bad_status', 'envelope-legacy-bad', %s
                )
                """,
                (_jsonb({"message_id": "poros-inbox-legacy-bad"}),),
            )
            cur.execute(constraint_sql)
            cur.execute(
                """
                SELECT relation_name, record_id, violation_code
                  FROM okg.v_exchange_boundary_violations
                 WHERE relation_name = 'poros_inbox_messages'
                   AND record_id = 'okg:receiver:poros-inbox-legacy-bad'
                """
            )
            assert cur.fetchone() == (
                "poros_inbox_messages",
                "okg:receiver:poros-inbox-legacy-bad",
                "status_vocabulary",
            )
            _assert_check_violation(
                cur,
                """
                INSERT INTO okg.poros_inbox_messages (
                  receiver_authority_id, sender_authority_id, message_id,
                  status, envelope_sha256, envelope
                )
                VALUES (
                  'okg:receiver', 'okg:sender', 'poros-inbox-future-bad',
                  'future_bad_status', 'envelope-future-bad', %s
                )
                """,
                (_jsonb({"message_id": "poros-inbox-future-bad"}),),
            )
        finally:
            cur.execute(
                """
                DELETE FROM okg.poros_inbox_messages
                 WHERE message_id IN (
                   'poros-inbox-legacy-bad',
                   'poros-inbox-future-bad'
                 )
                """
            )
            cur.execute(
                """
                ALTER TABLE okg.poros_inbox_messages
                  DROP CONSTRAINT IF EXISTS poros_inbox_messages_status_chk
                """
            )
            cur.execute(constraint_sql)
