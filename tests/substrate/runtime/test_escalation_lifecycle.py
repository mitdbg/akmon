"""Tests for the substrate-side escalation lifecycle (Phase 2 §1.4.1).

Verifies:
  - create_escalation: writes pending row, idempotent on retry.
  - transition_to_decided: closes pending row, writes decided row with
    decision payload + decided_by + decided_at.
  - transition_to_applied: decided → applied (closes decided row).
  - transition_to_timed_out: pending → timed_out (closes pending row).
  - Illegal transitions (e.g. pending → applied) raise
    EscalationStateError.
  - Bitemporal property: a historical pin sees the historical status.

Live integration with real Postgres; marked dbos_integration.
"""
from __future__ import annotations


import psycopg
import pytest
from tests.substrate.runtime._helpers import _swap_dbname


pytestmark = pytest.mark.dbos_integration


@pytest.fixture
def test_db_dsn(admin_dsn: str) -> str:
    """Per-test DB with the substrate state the lifecycle reads."""
    db_name = "okg_escalation_lifecycle_test"
    dsn = _swap_dbname(admin_dsn, db_name)

    with psycopg.connect(admin_dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_terminate_backend(pid) "
                "FROM pg_stat_activity WHERE datname = %s "
                "AND pid <> pg_backend_pid()",
                (db_name,),
            )
            cur.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
            cur.execute(f'CREATE DATABASE "{db_name}"')

    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS btree_gist")
            cur.execute("CREATE SCHEMA okg")
            # Minimum substrate state.
            cur.execute(
                """
                CREATE TABLE okg.catalog_versions (
                  catalog_version_id BIGSERIAL PRIMARY KEY,
                  status TEXT NOT NULL,
                  applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute(
                "INSERT INTO okg.catalog_versions (status) VALUES ('applied')"
            )
            cur.execute(
                """
                CREATE TABLE okg.graph_generations (
                  generation_id BIGSERIAL PRIMARY KEY,
                  deployment_name TEXT,
                  status TEXT NOT NULL CHECK (status IN
                    ('pending','published','blocked','worker_write')),
                  catalog_version_id BIGINT NOT NULL
                    REFERENCES okg.catalog_versions(catalog_version_id),
                  correlation_id TEXT NOT NULL,
                  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            # escalation_facts + escalation_live (subset matching the
            # production schema; no Timescale hypertable here so the
            # test runs anywhere with btree_gist).
            cur.execute(
                """
                CREATE TABLE okg.escalation_facts (
                  fact_id BIGSERIAL,
                  escalation_id TEXT NOT NULL,
                  subject_node_id TEXT NOT NULL,
                  attrs JSONB NOT NULL DEFAULT '{}'::jsonb,
                  op CHAR(1) NOT NULL DEFAULT 'I',
                  source TEXT NOT NULL,
                  observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                  dedupe_key TEXT NOT NULL,
                  run_id TEXT NOT NULL,
                  PRIMARY KEY (fact_id, observed_at),
                  UNIQUE (dedupe_key, observed_at)
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE okg.escalation_live (
                  escalation_id TEXT NOT NULL,
                  subject_node_id TEXT NOT NULL,
                  attrs JSONB NOT NULL DEFAULT '{}'::jsonb,
                  valid_from_gen BIGINT NOT NULL,
                  valid_to_gen BIGINT,
                  last_fact_id BIGINT,
                  PRIMARY KEY (escalation_id, valid_from_gen),
                  CONSTRAINT escalation_live_valid_interval_chk CHECK
                    (valid_to_gen IS NULL OR valid_to_gen > valid_from_gen),
                  CONSTRAINT escalation_live_no_validity_overlap_excl
                    EXCLUDE USING gist (
                      escalation_id WITH =,
                      (int8range(valid_from_gen, valid_to_gen, '[)')) WITH &&
                    )
                )
                """
            )

    yield dsn

    with psycopg.connect(admin_dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_backend_pid()",
                (db_name,),
            )
            cur.execute(f'DROP DATABASE IF EXISTS "{db_name}"')


def _create_test_escalation(conn, **overrides):
    """Helper for tests; returns an Escalation in 'pending'."""
    from okg.substrate.runtime.escalation import create_escalation

    defaults = dict(
        deployment="test",
        workflow_id="wf-1",
        step_id="step-1",
        subject_node_id="candidate-42",
        reason="verifier needs operator review",
        view_ref="view:af_pubs.verification:42",
        decision_schema_ref="schema:operator-verdict:1.0",
        timeout_at=None,
    )
    defaults.update(overrides)
    return create_escalation(conn, **defaults)


def test_create_escalation_writes_pending(test_db_dsn) -> None:
    with psycopg.connect(test_db_dsn) as conn:
        esc = _create_test_escalation(conn)

    assert esc.status == "pending"
    assert esc.escalation_id.startswith("esc-")
    assert esc.subject_node_id == "candidate-42"
    assert esc.requested_by_workflow == "wf-1"
    assert esc.reason == "verifier needs operator review"
    assert esc.view_ref == "view:af_pubs.verification:42"
    assert esc.decision_schema_ref == "schema:operator-verdict:1.0"
    assert esc.decided_at is None
    assert esc.decision is None


def test_create_escalation_idempotent_on_retry(test_db_dsn) -> None:
    """Second call with same (workflow_id, step_id) returns the same
    escalation_id and does not create a duplicate."""
    with psycopg.connect(test_db_dsn) as conn:
        first = _create_test_escalation(conn)
        second = _create_test_escalation(conn)

        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.escalation_live "
                "WHERE escalation_id = %s",
                (first.escalation_id,),
            )
            count = cur.fetchone()[0]

    assert first.escalation_id == second.escalation_id
    assert count == 1, (
        f"expected exactly one live row for the idempotent retry, got {count}"
    )


def test_transition_to_decided_writes_decision(test_db_dsn) -> None:
    from okg.substrate.runtime.escalation import transition_to_decided

    with psycopg.connect(test_db_dsn) as conn:
        esc = _create_test_escalation(conn)
        decision = {"label": "stale", "rationale": "no support in section 3"}
        after = transition_to_decided(
            conn,
            deployment="test",
            escalation_id=esc.escalation_id,
            decision=decision,
            decided_by="reviewer@example.edu",
        )

    assert after.status == "decided"
    assert after.decision == decision
    assert after.decided_by == "reviewer@example.edu"
    assert after.decided_at is not None
    assert after.valid_from_gen > esc.valid_from_gen


def test_transition_to_applied(test_db_dsn) -> None:
    from okg.substrate.runtime.escalation import (
        transition_to_applied,
        transition_to_decided,
    )

    with psycopg.connect(test_db_dsn) as conn:
        esc = _create_test_escalation(conn)
        transition_to_decided(
            conn,
            deployment="test",
            escalation_id=esc.escalation_id,
            decision={"label": "verified"},
            decided_by="approver@example.edu",
        )
        after = transition_to_applied(
            conn,
            deployment="test",
            escalation_id=esc.escalation_id,
        )

    assert after.status == "applied"
    # Decision carried forward.
    assert after.decision == {"label": "verified"}
    assert after.decided_by == "approver@example.edu"


def test_transition_to_timed_out(test_db_dsn) -> None:
    from okg.substrate.runtime.escalation import transition_to_timed_out

    with psycopg.connect(test_db_dsn) as conn:
        esc = _create_test_escalation(conn)
        after = transition_to_timed_out(
            conn,
            deployment="test",
            escalation_id=esc.escalation_id,
        )

    assert after.status == "timed_out"
    assert after.decided_at is None
    assert after.decision is None


def test_illegal_transition_raises(test_db_dsn) -> None:
    """pending → applied is not a legal transition; must raise."""
    from okg.substrate.runtime.escalation import (
        EscalationStateError,
        transition_to_applied,
    )

    with psycopg.connect(test_db_dsn) as conn:
        esc = _create_test_escalation(conn)
        with pytest.raises(EscalationStateError):
            transition_to_applied(
                conn,
                deployment="test",
                escalation_id=esc.escalation_id,
            )


def test_bitemporal_history_visible_at_pinned_generation(test_db_dsn) -> None:
    """A historical pin sees the historical status."""
    from okg.substrate.runtime.escalation import transition_to_decided

    with psycopg.connect(test_db_dsn) as conn:
        esc = _create_test_escalation(conn)
        after = transition_to_decided(
            conn,
            deployment="test",
            escalation_id=esc.escalation_id,
            decision={"label": "stale"},
            decided_by="approver@example.edu",
        )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT attrs->>'status'
                  FROM okg.escalation_live
                 WHERE escalation_id = %s
                   AND valid_from_gen <= %s
                   AND (valid_to_gen IS NULL OR valid_to_gen > %s)
                """,
                (esc.escalation_id, esc.valid_from_gen, esc.valid_from_gen),
            )
            status_at_pending_gen = cur.fetchone()[0]

            cur.execute(
                """
                SELECT attrs->>'status'
                  FROM okg.escalation_live
                 WHERE escalation_id = %s
                   AND valid_from_gen <= %s
                   AND (valid_to_gen IS NULL OR valid_to_gen > %s)
                """,
                (esc.escalation_id, after.valid_from_gen, after.valid_from_gen),
            )
            status_at_decided_gen = cur.fetchone()[0]

    assert status_at_pending_gen == "pending"
    assert status_at_decided_gen == "decided"


def test_get_escalation_raises_for_unknown_id(test_db_dsn) -> None:
    from okg.substrate.runtime.escalation import (

        EscalationNotFoundError,
        get_escalation,
    )

    with psycopg.connect(test_db_dsn) as conn:
        with pytest.raises(EscalationNotFoundError):
            get_escalation(conn, escalation_id="esc-does-not-exist")
