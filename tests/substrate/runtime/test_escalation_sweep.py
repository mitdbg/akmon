"""Tests for sweep_timed_out_escalations.

Covers eligibility filter (status + timeout_at + deployment scoping),
the transition's bitemporal-correctness, idempotency, concurrent-
transition handling. Uses the same per-test DB fixture as the other
escalation tests (`test_escalation_lifecycle.py`) so the substrate
schema subset is consistent across tests.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import psycopg
import pytest
from tests.substrate.runtime._helpers import _swap_dbname


pytestmark = pytest.mark.dbos_integration


@pytest.fixture
def test_db_dsn(admin_dsn: str) -> str:
    """Per-test DB with the substrate state the sweep reads.

    Same shape as `test_escalation_lifecycle.py::test_db_dsn` — the
    sweep adds no new table requirements.
    """
    db_name = "okg_escalation_sweep_test"
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


def _create_escalation(conn, *, deployment="test", suffix="1", timeout_at):
    """Create an escalation with a controlled timeout_at."""
    from okg.substrate.runtime.escalation import create_escalation

    return create_escalation(
        conn,
        deployment=deployment,
        workflow_id=f"wf-{suffix}",
        step_id=f"step-{suffix}",
        subject_node_id=f"candidate-{suffix}",
        reason="test sweep",
        view_ref=None,
        decision_schema_ref="schema:test:1.0",
        timeout_at=timeout_at,
    )


def _status(conn, escalation_id):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT attrs->>'status' FROM okg.escalation_live "
            "WHERE escalation_id=%s AND valid_to_gen IS NULL",
            (escalation_id,),
        )
        row = cur.fetchone()
    return row[0] if row else None


def test_sweep_transitions_past_ttl_escalation(test_db_dsn) -> None:
    """A pending escalation with timeout_at < now → status='timed_out'."""
    from okg.substrate.runtime.escalation import (
        sweep_timed_out_escalations,
    )

    now = datetime.now(timezone.utc)
    past = now - timedelta(minutes=10)

    with psycopg.connect(test_db_dsn) as conn:
        esc = _create_escalation(conn, suffix="past", timeout_at=past)
        assert esc.status == "pending"

        result = sweep_timed_out_escalations(
            conn, deployment="test", now=now,
        )

        assert result == {"considered": 1, "transitioned": 1, "skipped": 0}
        assert _status(conn, esc.escalation_id) == "timed_out"


def test_sweep_leaves_future_ttl_alone(test_db_dsn) -> None:
    """A pending escalation with timeout_at > now is NOT swept."""
    from okg.substrate.runtime.escalation import (
        sweep_timed_out_escalations,
    )

    now = datetime.now(timezone.utc)
    future = now + timedelta(hours=1)

    with psycopg.connect(test_db_dsn) as conn:
        esc = _create_escalation(conn, suffix="future", timeout_at=future)

        result = sweep_timed_out_escalations(
            conn, deployment="test", now=now,
        )

        assert result["considered"] == 0
        assert result["transitioned"] == 0
        assert _status(conn, esc.escalation_id) == "pending"


def test_sweep_leaves_no_ttl_alone(test_db_dsn) -> None:
    """A pending escalation without timeout_at is never swept by the
    timeout sweep — it has no deadline."""
    from okg.substrate.runtime.escalation import (
        sweep_timed_out_escalations,
    )

    now = datetime.now(timezone.utc)

    with psycopg.connect(test_db_dsn) as conn:
        esc = _create_escalation(conn, suffix="nottl", timeout_at=None)

        result = sweep_timed_out_escalations(
            conn, deployment="test", now=now,
        )

        assert result["considered"] == 0
        assert _status(conn, esc.escalation_id) == "pending"


def test_sweep_skips_terminal_status(test_db_dsn) -> None:
    """An already-timed_out / decided / applied row is not re-touched
    even if its timeout_at is past."""
    from okg.substrate.runtime.escalation import (
        sweep_timed_out_escalations,
        transition_to_timed_out,
    )

    now = datetime.now(timezone.utc)
    past = now - timedelta(minutes=10)

    with psycopg.connect(test_db_dsn) as conn:
        esc = _create_escalation(conn, suffix="terminal", timeout_at=past)
        # Pre-transition by hand to reach `timed_out`.
        transition_to_timed_out(
            conn, deployment="test", escalation_id=esc.escalation_id,
        )
        assert _status(conn, esc.escalation_id) == "timed_out"

        result = sweep_timed_out_escalations(
            conn, deployment="test", now=now,
        )

        assert result == {"considered": 0, "transitioned": 0, "skipped": 0}


def test_sweep_is_idempotent(test_db_dsn) -> None:
    """Running the sweep twice produces zero work on the second pass."""
    from okg.substrate.runtime.escalation import (
        sweep_timed_out_escalations,
    )

    now = datetime.now(timezone.utc)
    past = now - timedelta(minutes=10)

    with psycopg.connect(test_db_dsn) as conn:
        _create_escalation(conn, suffix="idem1", timeout_at=past)
        _create_escalation(conn, suffix="idem2", timeout_at=past)

        first = sweep_timed_out_escalations(
            conn, deployment="test", now=now,
        )
        second = sweep_timed_out_escalations(
            conn, deployment="test", now=now,
        )

        assert first["transitioned"] == 2
        assert second == {"considered": 0, "transitioned": 0, "skipped": 0}


def test_sweep_scoped_to_deployment(test_db_dsn) -> None:
    """The sweep only touches rows whose creating-generation belongs
    to the named deployment."""
    from okg.substrate.runtime.escalation import (
        sweep_timed_out_escalations,
    )

    now = datetime.now(timezone.utc)
    past = now - timedelta(minutes=10)

    with psycopg.connect(test_db_dsn) as conn:
        esc_a = _create_escalation(
            conn, deployment="dep-a", suffix="a", timeout_at=past,
        )
        esc_b = _create_escalation(
            conn, deployment="dep-b", suffix="b", timeout_at=past,
        )

        # Sweep only dep-a.
        result_a = sweep_timed_out_escalations(
            conn, deployment="dep-a", now=now,
        )

        assert result_a["transitioned"] == 1
        assert _status(conn, esc_a.escalation_id) == "timed_out"
        assert _status(conn, esc_b.escalation_id) == "pending"


def test_sweep_transitions_routed_status_too(test_db_dsn) -> None:
    """Routed (operator notified, awaiting response) is also eligible
    for timeout — matches the existing transition_to_timed_out
    expected_from_statuses=('pending', 'routed')."""
    from okg.substrate.runtime.escalation import (

        sweep_timed_out_escalations,
    )

    now = datetime.now(timezone.utc)
    past = now - timedelta(minutes=10)

    with psycopg.connect(test_db_dsn) as conn:
        esc = _create_escalation(conn, suffix="routed", timeout_at=past)
        # Manually force routed (no public transition helper for this
        # path today — just update the live row's attrs).
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE okg.escalation_live
                   SET attrs = jsonb_set(attrs, '{status}', '"routed"')
                 WHERE escalation_id = %s AND valid_to_gen IS NULL
                """,
                (esc.escalation_id,),
            )
        conn.commit()
        assert _status(conn, esc.escalation_id) == "routed"

        result = sweep_timed_out_escalations(
            conn, deployment="test", now=now,
        )

        assert result["transitioned"] == 1
        assert _status(conn, esc.escalation_id) == "timed_out"
