"""Real PostgreSQL checks for shared cost-budget authority."""
from __future__ import annotations

import threading
import uuid

import psycopg
import pytest

from tests.substrate.runtime._helpers import _swap_dbname

pytestmark = [pytest.mark.postgres, pytest.mark.serial_db]


@pytest.fixture
def cost_budget_db(okg_dsn: str):
    admin_dsn = _swap_dbname(okg_dsn, "postgres")
    db_name = f"okg_cost_budget_{uuid.uuid4().hex[:12]}"
    dsn = _swap_dbname(admin_dsn, db_name)
    with psycopg.connect(admin_dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(f'CREATE DATABASE "{db_name}"')
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA okg")
            cur.execute(
                "CREATE TABLE okg.deployment_cost_caps ("
                "scope TEXT PRIMARY KEY, cap_usd NUMERIC NOT NULL, "
                "updated_at TIMESTAMPTZ NOT NULL DEFAULT now())"
            )
            cur.execute(
                "CREATE TABLE okg.deployment_cost_ledger ("
                "scope TEXT NOT NULL, consumption_id TEXT NOT NULL, "
                "usd_consumed NUMERIC NOT NULL, workflow_id TEXT, "
                "observed_at TIMESTAMPTZ NOT NULL DEFAULT now(), "
                "PRIMARY KEY (scope, consumption_id))"
            )
    try:
        yield dsn
    finally:
        with psycopg.connect(admin_dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE datname = %s AND pid <> pg_backend_pid()",
                    (db_name,),
                )
                cur.execute(f'DROP DATABASE IF EXISTS "{db_name}"')


def test_shared_scope_live_cap_retry_and_serialization(cost_budget_db):
    from okg.deployment import cost_budget
    from okg.deployment._internal.context import bind_test_context
    from okg.deployment._internal.cost_budget import BudgetExhaustedError

    scope = f"shared.{uuid.uuid4().hex}"
    with psycopg.connect(cost_budget_db) as conn:
        with bind_test_context(deployment="test", conn=conn):
            first = cost_budget(usd=1.0, scope=scope)
            with first:
                first.consume(1.0, consumption_id="first")
            with cost_budget(usd=10.0, scope=scope):
                assert first.cap == pytest.approx(10.0)
                assert first.available == pytest.approx(9.0)
                assert not first.exhausted
                first.consume(2.0, consumption_id="raised")
            with cost_budget(usd=1.0, scope=scope):
                assert first.cap == pytest.approx(1.0)
                assert first.available == pytest.approx(0.0)
                assert first.exhausted
                with pytest.raises(BudgetExhaustedError):
                    first.consume(0.1, consumption_id="new")
                assert first.consume(2.0, consumption_id="raised") == pytest.approx(3.0)

    # Two independent transactions serialize on the shared cap row.
    concurrent_scope = f"concurrent.{uuid.uuid4().hex}"
    with psycopg.connect(cost_budget_db, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.deployment_cost_caps(scope, cap_usd) "
                "VALUES (%s, 1)",
                (concurrent_scope,),
            )
    barrier = threading.Barrier(2)
    outcomes: list[str] = []

    def consume_once(identifier: str) -> None:
        try:
            with psycopg.connect(cost_budget_db) as conn:
                with bind_test_context(deployment="test", conn=conn):
                    barrier.wait(timeout=5)
                    try:
                        cost_budget(usd=1.0, scope=concurrent_scope).consume(
                            0.75, consumption_id=identifier,
                        )
                    except BudgetExhaustedError:
                        outcomes.append("refused")
                    else:
                        outcomes.append("admitted")
        except Exception as exc:  # pragma: no cover - makes thread failures visible
            outcomes.append(f"error:{type(exc).__name__}")

    threads = [threading.Thread(target=consume_once, args=(str(i),)) for i in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)
    assert sorted(outcomes) == ["admitted", "refused"]
