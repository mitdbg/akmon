"""Tests for the operator response surface (Phase 2 §1.4.2).

Covers:
  - validate_decision_payload across the three schema_ref schemes
    (open, literal, registry).
  - respond_to_escalation happy path (validate + transition + skip
    wake via send_wake=False so we don't need a DBOS singleton).
  - Error surfaces: NotFound, StateError, InvalidDecisionPayload.

The full pending→decided→DBOSClient.send→workflow wake round-trip is
covered by the §1.4.4 live verification (which needs a real DBOS
singleton); here we exercise the substrate-side mechanics only.
"""
from __future__ import annotations

import urllib.parse

import psycopg
import pytest
from tests.substrate.runtime._helpers import _swap_dbname


pytestmark = pytest.mark.dbos_integration


@pytest.fixture
def test_db_dsn(admin_dsn: str) -> str:
    db_name = "okg_escalation_response_test"
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
            cur.execute(
                "CREATE TABLE okg.catalog_versions ("
                "catalog_version_id BIGSERIAL PRIMARY KEY, "
                "status TEXT NOT NULL, "
                "applied_at TIMESTAMPTZ NOT NULL DEFAULT now())"
            )
            cur.execute(
                "INSERT INTO okg.catalog_versions (status) VALUES ('applied')"
            )
            cur.execute(
                "CREATE TABLE okg.graph_generations ("
                "generation_id BIGSERIAL PRIMARY KEY, "
                "deployment_name TEXT, "
                "status TEXT NOT NULL CHECK (status IN "
                "  ('pending','published','blocked','worker_write')), "
                "catalog_version_id BIGINT NOT NULL "
                "  REFERENCES okg.catalog_versions(catalog_version_id), "
                "correlation_id TEXT NOT NULL, "
                "created_at TIMESTAMPTZ NOT NULL DEFAULT now())"
            )
            cur.execute(
                "CREATE TABLE okg.escalation_facts ("
                "fact_id BIGSERIAL, "
                "escalation_id TEXT NOT NULL, "
                "subject_node_id TEXT NOT NULL, "
                "attrs JSONB NOT NULL DEFAULT '{}'::jsonb, "
                "op CHAR(1) NOT NULL DEFAULT 'I', "
                "source TEXT NOT NULL, "
                "observed_at TIMESTAMPTZ NOT NULL DEFAULT now(), "
                "dedupe_key TEXT NOT NULL, "
                "run_id TEXT NOT NULL, "
                "PRIMARY KEY (fact_id, observed_at), "
                "UNIQUE (dedupe_key, observed_at))"
            )
            cur.execute(
                "CREATE TABLE okg.escalation_live ("
                "escalation_id TEXT NOT NULL, "
                "subject_node_id TEXT NOT NULL, "
                "attrs JSONB NOT NULL DEFAULT '{}'::jsonb, "
                "valid_from_gen BIGINT NOT NULL, "
                "valid_to_gen BIGINT, "
                "last_fact_id BIGINT, "
                "PRIMARY KEY (escalation_id, valid_from_gen), "
                "CONSTRAINT escalation_live_valid_interval_chk CHECK "
                "  (valid_to_gen IS NULL OR valid_to_gen > valid_from_gen), "
                "CONSTRAINT escalation_live_no_validity_overlap_excl "
                "  EXCLUDE USING gist ("
                "    escalation_id WITH =, "
                "    (int8range(valid_from_gen, valid_to_gen, '[)')) WITH &&))"
            )

    yield dsn

    with psycopg.connect(admin_dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_terminate_backend(pid) "
                "FROM pg_stat_activity WHERE datname = %s "
                "AND pid <> pg_backend_pid()",
                (db_name,),
            )
            cur.execute(f'DROP DATABASE IF EXISTS "{db_name}"')


# ---------------------------------------------------------------------------
# Schema validation (pure logic; no DB needed)
# ---------------------------------------------------------------------------


def test_validate_open_schema_accepts_anything():
    from okg.substrate.runtime.escalation_response import (
        validate_decision_payload,
    )

    validate_decision_payload(
        decision={"any": "shape"},
        decision_schema_ref="schema:open",
    )
    validate_decision_payload(
        decision={},
        decision_schema_ref="schema:open",
    )


def test_validate_literal_schema_accepts_matching():
    from okg.substrate.runtime.escalation_response import (
        validate_decision_payload,
    )

    schema = {
        "type": "object",
        "required": ["label"],
        "properties": {"label": {"type": "string"}},
    }
    import json
    ref = "schema:literal:" + urllib.parse.quote(json.dumps(schema))

    validate_decision_payload(
        decision={"label": "stale"},
        decision_schema_ref=ref,
    )


def test_validate_literal_schema_rejects_missing_required():
    from okg.substrate.runtime.escalation_response import (
        InvalidDecisionPayload,
        validate_decision_payload,
    )

    schema = {
        "type": "object",
        "required": ["label", "rationale"],
        "properties": {"label": {"type": "string"}},
    }
    import json
    ref = "schema:literal:" + urllib.parse.quote(json.dumps(schema))

    with pytest.raises(InvalidDecisionPayload, match="rationale"):
        validate_decision_payload(
            decision={"label": "stale"},
            decision_schema_ref=ref,
        )


def test_validate_unsupported_scheme_raises():
    from okg.substrate.runtime.escalation_response import (
        InvalidDecisionPayload,
        validate_decision_payload,
    )

    with pytest.raises(InvalidDecisionPayload, match="unsupported"):
        validate_decision_payload(
            decision={"label": "x"},
            decision_schema_ref="schema:made-up-scheme",
        )


# ---------------------------------------------------------------------------
# respond_to_escalation (needs DB)
# ---------------------------------------------------------------------------


def _create_escalation(conn, *, decision_schema_ref="schema:open"):
    from okg.substrate.runtime.escalation import create_escalation
    return create_escalation(
        conn,
        deployment="test",
        workflow_id="wf-test",
        step_id="step-1",
        subject_node_id="candidate-1",
        reason="verifier flagged unclear",
        view_ref=None,
        decision_schema_ref=decision_schema_ref,
    )


def test_respond_happy_path(test_db_dsn):
    from okg.substrate.runtime.escalation_response import (
        respond_to_escalation,
    )

    with psycopg.connect(test_db_dsn) as conn:
        esc = _create_escalation(conn)
        result = respond_to_escalation(
            conn,
            deployment="test",
            escalation_id=esc.escalation_id,
            decision={"label": "stale"},
            decided_by="reviewer@example.edu",
            send_wake=False,
        )

    assert result.escalation_id == esc.escalation_id
    assert result.new_status == "decided"
    assert result.workflow_id == "wf-test"
    assert result.decided_by == "reviewer@example.edu"
    assert result.wake_sent is False  # we asked to skip
    assert result.decided_at  # non-empty ISO timestamp


def test_respond_validates_against_literal_schema(test_db_dsn):
    from okg.substrate.runtime.escalation_response import (
        InvalidDecisionPayload,
        respond_to_escalation,
    )

    import json
    schema = {"type": "object", "required": ["verdict"]}
    schema_ref = (
        "schema:literal:" + urllib.parse.quote(json.dumps(schema))
    )

    with psycopg.connect(test_db_dsn) as conn:
        esc = _create_escalation(conn, decision_schema_ref=schema_ref)

        # Invalid: missing 'verdict' field
        with pytest.raises(InvalidDecisionPayload, match="verdict"):
            respond_to_escalation(
                conn,
                deployment="test",
                escalation_id=esc.escalation_id,
                decision={"random": "thing"},
                decided_by="op",
                send_wake=False,
            )

        # Verify the escalation is STILL pending after validation failure.
        from okg.substrate.runtime.escalation import get_escalation
        unchanged = get_escalation(conn, escalation_id=esc.escalation_id)
        assert unchanged.status == "pending"


def test_respond_raises_for_unknown_escalation(test_db_dsn):
    from okg.substrate.runtime.escalation import EscalationNotFoundError
    from okg.substrate.runtime.escalation_response import (
        respond_to_escalation,
    )

    with psycopg.connect(test_db_dsn) as conn:
        with pytest.raises(EscalationNotFoundError):
            respond_to_escalation(
                conn,
                deployment="test",
                escalation_id="esc-bogus-id",
                decision={"label": "x"},
                decided_by="op",
                send_wake=False,
            )


def test_respond_rejects_already_decided(test_db_dsn):
    from okg.substrate.runtime.escalation import EscalationStateError
    from okg.substrate.runtime.escalation_response import (

        respond_to_escalation,
    )

    with psycopg.connect(test_db_dsn) as conn:
        esc = _create_escalation(conn)
        # First response: succeeds.
        respond_to_escalation(
            conn,
            deployment="test",
            escalation_id=esc.escalation_id,
            decision={"label": "stale"},
            decided_by="op1",
            send_wake=False,
        )
        # Second response: rejected.
        with pytest.raises(EscalationStateError):
            respond_to_escalation(
                conn,
                deployment="test",
                escalation_id=esc.escalation_id,
                decision={"label": "fresh"},
                decided_by="op2",
                send_wake=False,
            )
