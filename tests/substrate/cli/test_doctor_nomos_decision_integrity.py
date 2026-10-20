"""Tests for the nomos_decision_id referential-integrity doctor check.

Pinned by ``add-nomos-decision-id-referential-integrity``. Schema FKs now
prevent new orphans, but the doctor remains the legacy/pre-migration integrity
scan operators use when an existing deployment cannot add the constraints yet.
"""
from __future__ import annotations

import argparse
import json
from collections.abc import Iterator

import psycopg
import pytest


pytestmark = pytest.mark.postgres

POROS_DECISION_FK = "poros_incorporation_events_nomos_decision_id_fkey"


def test_external_action_audit_chain_is_in_the_doctor_inventory() -> None:
    from okg.substrate.cli.doctor import _NOMOS_DECISION_CHILD_TABLES

    external = {
        (table, decision_column)
        for table, _pk, _deployment_column, decision_column in _NOMOS_DECISION_CHILD_TABLES
        if table.startswith("external_action_")
    }
    assert external == {
        ("external_action_auth_audits", "nomos_decision_id"),
        ("external_action_decisions", "nomos_decision_id"),
        ("external_action_transition_idempotency", "nomos_decision_id"),
        ("external_action_executions", "nomos_decision_id"),
        ("external_action_outcomes", "nomos_decision_id"),
        ("external_action_events", "nomos_decision_ref"),
        ("external_action_owner_unavailability_attestations", "nomos_decision_id"),
        ("external_action_execution_epoch_leases", "nomos_decision_id"),
        ("external_action_restore_attestations", "nomos_decision_id"),
        ("external_action_restore_sessions", "nomos_decision_id"),
    }


def _doctor_args(dsn: str) -> argparse.Namespace:
    return argparse.Namespace(dsn=dsn, deployment=None)


def _clear_nomos_decision_tables(dsn: str) -> None:
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
def nomos_decision_dsn(okg_migrated: str) -> Iterator[str]:
    _clear_nomos_decision_tables(okg_migrated)
    try:
        yield okg_migrated
    finally:
        _clear_nomos_decision_tables(okg_migrated)


def _restore_poros_decision_fk(cur: psycopg.Cursor) -> None:
    cur.execute(
        f"""
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT 1
              FROM pg_constraint
             WHERE connamespace = 'okg'::regnamespace
               AND conrelid = 'okg.poros_incorporation_events'::regclass
               AND conname = '{POROS_DECISION_FK}'
          ) THEN
            ALTER TABLE okg.poros_incorporation_events
              ADD CONSTRAINT {POROS_DECISION_FK}
              FOREIGN KEY (
                nomos_decision_deployment_name, nomos_decision_id
              )
              REFERENCES okg.nomos_decisions(deployment_name, decision_id)
              ON DELETE RESTRICT;
          END IF;
        END
        $$;
        """
    )


def test_clean_db_reports_ok(
    nomos_decision_dsn: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Empty child tables → ok:true, exit code 0."""
    from okg.substrate.cli.doctor import (
        _doctor_nomos_decision_referential_integrity,
    )

    rc = _doctor_nomos_decision_referential_integrity(
        _doctor_args(nomos_decision_dsn), as_json=True,
    )
    out = capsys.readouterr().out
    parsed = json.loads(out)
    assert rc == 0
    assert parsed["ok"] is True
    assert parsed["orphan_count"] == 0
    assert parsed["findings"] == []


def test_orphan_reference_in_poros_incorporation_events_detected(
    nomos_decision_dsn: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A poros_incorporation_events row with a non-existent
    nomos_decision_id MUST surface as an orphan finding on legacy schemas."""
    from okg.substrate.cli.doctor import (
        _doctor_nomos_decision_referential_integrity,
    )

    try:
        # Simulate a pre-FK deployment: drop only this test's FK, insert an
        # orphan, then restore the constraint in the finally block.
        with psycopg.connect(nomos_decision_dsn) as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                ALTER TABLE okg.poros_incorporation_events
                  DROP CONSTRAINT IF EXISTS {POROS_DECISION_FK}
                """
            )
            cur.execute(
                """
                INSERT INTO okg.poros_incorporation_events
                  (receiver_authority_id, sender_authority_id,
                   message_id, event_type, status,
                   nomos_decision_id)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    "okg:test:receiver",
                    "okg:test:sender",
                    "msg-orphan-001",
                    "nomos_decided",
                    "decided",
                    "nomos:decision:dead",
                ),
            )
            conn.commit()

        rc = _doctor_nomos_decision_referential_integrity(
            _doctor_args(nomos_decision_dsn), as_json=True,
        )
        out = capsys.readouterr().out
        parsed = json.loads(out)
        assert rc == 1
        assert parsed["ok"] is False
        assert parsed["orphan_count"] == 1
        assert len(parsed["findings"]) == 1
        finding = parsed["findings"][0]
        assert finding["child_table"] == "okg.poros_incorporation_events"
        assert finding["deployment_name"] == "default"
        assert finding["nomos_decision_id"] == "nomos:decision:dead"
    finally:
        with psycopg.connect(nomos_decision_dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM okg.poros_incorporation_events
                 WHERE nomos_decision_id = 'nomos:decision:dead'
                """
            )
            _restore_poros_decision_fk(cur)
            conn.commit()


def test_resolved_reference_does_not_surface(
    nomos_decision_dsn: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A child row whose nomos_decision_id has a matching parent in
    okg.nomos_decisions MUST NOT be flagged."""
    from okg.substrate.cli.doctor import (
        _doctor_nomos_decision_referential_integrity,
    )

    with psycopg.connect(nomos_decision_dsn) as conn, conn.cursor() as cur:
        # Insert parent decision.
        cur.execute(
            """
            INSERT INTO okg.nomos_decisions
              (deployment_name, decision_id, input_hash, subject,
               action, classification, effect, reason_code,
               enforcement_point)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                "default",
                "nomos:decision:live",
                "hash-1",
                "okg:test:subject",
                "test.action",
                "public",
                "allow",
                "test.allow",
                "test",
            ),
        )
        # Insert child row referencing the live decision.
        cur.execute(
            """
            INSERT INTO okg.poros_incorporation_events
              (receiver_authority_id, sender_authority_id,
               message_id, event_type, status,
               nomos_decision_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                "okg:test:receiver",
                "okg:test:sender",
                "msg-live-001",
                "nomos_decided",
                "decided",
                "nomos:decision:live",
            ),
        )
        conn.commit()

    rc = _doctor_nomos_decision_referential_integrity(
        _doctor_args(nomos_decision_dsn), as_json=True,
    )
    out = capsys.readouterr().out
    parsed = json.loads(out)
    assert rc == 0
    assert parsed["ok"] is True
    assert parsed["orphan_count"] == 0
