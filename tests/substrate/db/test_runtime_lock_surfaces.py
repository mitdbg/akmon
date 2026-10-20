"""Schema contract tests for current runtime lock/lease audit surfaces."""
from __future__ import annotations

import psycopg
import pytest
from psycopg import errors


pytestmark = pytest.mark.postgres


def test_runtime_lock_surface_inventory_distinguishes_authorities(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT surface_id,
                   surface_kind,
                   storage_relation,
                   owner_column,
                   expires_column,
                   active_status,
                   visibility_view,
                   table_backed
              FROM okg.v_runtime_lock_surface_inventory
             ORDER BY surface_id
            """
        )
        rows = {row[0]: row[1:] for row in cur.fetchall()}

    assert set(rows) == {
        "catalog_apply_advisory_lock",
        "legacy_publisher_lease_audit",
        "mailbox_message_claim",
        "publisher_advisory_lock",
    }
    assert rows["publisher_advisory_lock"] == (
        "session_advisory_lock",
        None,
        None,
        None,
        None,
        None,
        False,
    )
    assert rows["catalog_apply_advisory_lock"][0] == "session_advisory_lock"
    assert rows["legacy_publisher_lease_audit"] == (
        "retired_audit_log",
        "okg.lease_audit",
        "holder_id",
        None,
        None,
        None,
        True,
    )
    assert rows["mailbox_message_claim"] == (
        "table_backed_lease",
        "okg.federation_mailbox_messages",
        "lease_token",
        "lease_expires_at",
        "leased",
        "okg.v_runtime_stale_locks",
        True,
    )


def test_runtime_stale_locks_view_redacts_mailbox_tokens(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.federation_mailbox_messages (
                  message_id, sender_authority_id, recipient_authority_id,
                  payload_mode, envelope, packet_sha256, expires_at, status,
                  lease_token, lease_expires_at
                )
                VALUES (
                  'msg-stale-lock-view', 'okg:sender', 'okg:recipient',
                  'inline', '{}'::jsonb, 'packet-stale-lock-view',
                  now() + interval '1 hour', 'leased',
                  'secret-token-value', now() - interval '10 seconds'
                )
                """
            )
            cur.execute(
                """
                SELECT surface_id,
                       resource_id,
                       owner_ref,
                       token_present,
                       stale_reason,
                       stale_for_seconds,
                       metadata
                  FROM okg.v_runtime_stale_locks
                """
            )
            row = cur.fetchone()

    assert row[0] == "mailbox_message_claim"
    assert row[1] == "msg-stale-lock-view"
    assert row[2] == "okg:recipient"
    assert row[3] is True
    assert row[4] == "expired"
    assert row[5] >= 0
    assert row[6]["message_id"] == "msg-stale-lock-view"
    assert "lease_token" not in row[6]
    assert "secret-token-value" not in str(row[6])


def test_active_mailbox_claim_requires_token_and_expiry(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur, pytest.raises(errors.CheckViolation):
            cur.execute(
                """
                INSERT INTO okg.federation_mailbox_messages (
                  message_id, sender_authority_id, recipient_authority_id,
                  payload_mode, envelope, packet_sha256, expires_at, status
                )
                VALUES (
                  'msg-malformed-lease', 'okg:sender', 'okg:recipient',
                  'inline', '{}'::jsonb, 'packet-malformed-lease',
                  now() + interval '1 hour', 'leased'
                )
                """
            )
