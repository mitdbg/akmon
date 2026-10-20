"""Tests for SDK external-integration helpers (Phase 2 §5).

Covers the pieces that can be exercised without a running DBOS
singleton or live LLM provider:

  * ``correlate_workflow`` / ``find_workflow_by_correlation`` write
    and read ``okg.deployment_correlations``.
  * ``DerivedEdgeSpec`` shape stability.
  * ``LLMResponse`` shape stability.

End-to-end tests for ``llm_call`` (real provider), ``wait_for_signal``
(DBOS.recv), ``request_republish`` (DBOSClient.start_workflow), and
stream wrappers (DBOS.write_stream / read_stream) need a live DBOS
singleton plus optional cloud credentials — they belong in
deployment-level integration suites (e.g. the af_pubs verifier in §7).
"""
from __future__ import annotations

import sys
from types import SimpleNamespace
from unittest.mock import Mock

import psycopg
import pytest
from tests.substrate.runtime._helpers import _swap_dbname


pytestmark = pytest.mark.dbos_integration


@pytest.fixture(name="test_db_dsn")
def sdk_test_db_dsn(okg_dsn: str):
    admin_dsn = _swap_dbname(okg_dsn, "postgres")
    db_name = "okg_sdk_external_test"
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
            cur.execute("CREATE SCHEMA okg")
            cur.execute(
                "CREATE TABLE okg.deployment_correlations ("
                "deployment TEXT NOT NULL, "
                "topic TEXT NOT NULL, "
                "correlation_key TEXT NOT NULL, "
                "workflow_id TEXT NOT NULL, "
                "registered_at TIMESTAMPTZ NOT NULL DEFAULT now(), "
                "PRIMARY KEY (deployment, topic, correlation_key))"
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
# correlate_workflow / find_workflow_by_correlation
# ---------------------------------------------------------------------------


@pytest.mark.postgres
def test_correlate_writes_row(test_db_dsn):
    from okg.deployment import correlate_workflow
    from okg.deployment._internal.context import bind_test_context

    with psycopg.connect(test_db_dsn) as conn:
        with bind_test_context(
            deployment="test", conn=conn, workflow_id="wf-abc",
        ):
            correlate_workflow(
                topic="webhook.github.pr",
                correlation_key="PR-42",
            )

        with conn.cursor() as cur:
            cur.execute(
                "SELECT workflow_id FROM okg.deployment_correlations "
                "WHERE deployment='test' AND topic='webhook.github.pr' "
                "AND correlation_key='PR-42'"
            )
            row = cur.fetchone()

    assert row is not None
    assert row[0] == "wf-abc"


@pytest.mark.postgres
def test_correlate_is_idempotent_on_retry(test_db_dsn):
    """Re-registering with the same key UPSERTs (replaces) the
    workflow_id, exactly one row remains."""
    from okg.deployment import correlate_workflow
    from okg.deployment._internal.context import bind_test_context

    with psycopg.connect(test_db_dsn) as conn:
        # First registration.
        with bind_test_context(
            deployment="test", conn=conn, workflow_id="wf-1",
        ):
            correlate_workflow(topic="t", correlation_key="k")
        # Same workflow re-registers (recovery path).
        with bind_test_context(
            deployment="test", conn=conn, workflow_id="wf-1",
        ):
            correlate_workflow(topic="t", correlation_key="k")

        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*), max(workflow_id) "
                "FROM okg.deployment_correlations "
                "WHERE deployment='test' AND topic='t' "
                "AND correlation_key='k'"
            )
            count, wf = cur.fetchone()

    assert count == 1
    assert wf == "wf-1"


@pytest.mark.postgres
def test_correlate_upsert_replaces_workflow_id(test_db_dsn):
    """A different workflow registering under the same key replaces
    the workflow_id (intentional: e.g. handoff between workflows)."""
    from okg.deployment import correlate_workflow
    from okg.deployment._internal.context import bind_test_context

    with psycopg.connect(test_db_dsn) as conn:
        with bind_test_context(
            deployment="test", conn=conn, workflow_id="wf-1",
        ):
            correlate_workflow(topic="t", correlation_key="k")
        with bind_test_context(
            deployment="test", conn=conn, workflow_id="wf-2",
        ):
            correlate_workflow(topic="t", correlation_key="k")

        with conn.cursor() as cur:
            cur.execute(
                "SELECT workflow_id FROM okg.deployment_correlations "
                "WHERE deployment='test' AND topic='t' "
                "AND correlation_key='k'"
            )
            assert cur.fetchone()[0] == "wf-2"


@pytest.mark.postgres
def test_find_by_correlation_returns_workflow_id(test_db_dsn):
    from okg.deployment import (
        correlate_workflow,
        find_workflow_by_correlation,
    )
    from okg.deployment._internal.context import bind_test_context

    with psycopg.connect(test_db_dsn) as conn:
        with bind_test_context(
            deployment="test", conn=conn, workflow_id="wf-find",
        ):
            correlate_workflow(topic="t", correlation_key="k")
            wf = find_workflow_by_correlation(
                deployment="test", topic="t", correlation_key="k",
            )
    assert wf == "wf-find"


@pytest.mark.postgres
def test_find_by_correlation_returns_none_for_missing(test_db_dsn):
    from okg.deployment import find_workflow_by_correlation
    from okg.deployment._internal.context import bind_test_context

    with psycopg.connect(test_db_dsn) as conn:
        with bind_test_context(deployment="test", conn=conn):
            wf = find_workflow_by_correlation(
                deployment="test", topic="t",
                correlation_key="never-registered",
            )
    assert wf is None


# ---------------------------------------------------------------------------
# DerivedEdgeSpec
# ---------------------------------------------------------------------------


def test_derived_edge_spec_construction():
    from okg.deployment import DerivedEdgeSpec

    spec = DerivedEdgeSpec(
        src="n-1", dst="n-2", edge_type="rel",
        attrs={"k": "v"}, dedupe_key="d",
        confidence=0.8, provenance="derived_semantic",
    )
    assert spec.src == "n-1"
    assert spec.confidence == 0.8
    assert spec.provenance == "derived_semantic"
    # Frozen dataclass.
    with pytest.raises(Exception):  # FrozenInstanceError
        spec.src = "n-9"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# LLMResponse shape
# ---------------------------------------------------------------------------


def test_llm_response_shape():
    from okg.deployment import LLMResponse

    r = LLMResponse(
        text="hello",
        parsed=None,
        cost_usd=0.001,
        input_tokens=10,
        output_tokens=5,
        model="gpt-4o-mini",
    )
    assert r.text == "hello"
    assert r.cost_usd == 0.001


# ---------------------------------------------------------------------------
# llm_call pricing settlement
# ---------------------------------------------------------------------------


def _litellm_response(*, usage=True):
    return SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content="answer"))],
        usage=(
            SimpleNamespace(prompt_tokens=100, completion_tokens=50)
            if usage
            else None
        ),
        model="provider/resolved-model",
    )


def _install_litellm(monkeypatch, *, price, usage=True):
    response = _litellm_response(usage=usage)
    completion_cost = Mock()
    if isinstance(price, BaseException):
        completion_cost.side_effect = price
    else:
        completion_cost.return_value = price
    completion = Mock(return_value=response)
    monkeypatch.setitem(
        sys.modules,
        "litellm",
        SimpleNamespace(
            completion=completion,
            completion_cost=completion_cost,
        ),
    )
    return completion_cost


@pytest.mark.parametrize(
    "price",
    [None, float("nan"), float("inf"), -0.01, "invalid"],
    ids=["missing", "nan", "infinite", "negative", "invalid"],
)
def test_llm_call_rejects_unsettled_price(monkeypatch, price):
    from okg.deployment import LLMPricingError, llm_call

    completion_cost = _install_litellm(monkeypatch, price=price)
    budget = SimpleNamespace(consume=Mock())
    response_schema = SimpleNamespace(validate=Mock())

    with pytest.raises(LLMPricingError) as caught:
        llm_call(
            "prompt",
            model="requested-model",
            budget=budget,
            response_schema=response_schema,
        )

    error = caught.value
    assert error.pricing_status == "unsettled"
    assert error.requested_model == "requested-model"
    assert error.resolved_model == "provider/resolved-model"
    assert error.input_tokens == 100
    assert error.output_tokens == 50
    assert "supported by LiteLLM pricing" in error.guidance
    assert "pricing policy" in str(error)
    completion_cost.assert_called_once()
    budget.consume.assert_not_called()
    response_schema.validate.assert_not_called()


def test_llm_call_surfaces_pricing_service_exception(monkeypatch):
    from okg.deployment import LLMPricingError, llm_call

    _install_litellm(
        monkeypatch,
        price=RuntimeError("pricing service unavailable"),
    )

    with pytest.raises(LLMPricingError) as caught:
        llm_call("prompt", model="unknown-model")

    assert "pricing service unavailable" in caught.value.reason
    assert isinstance(caught.value.__cause__, RuntimeError)


def test_llm_call_keeps_missing_usage_unknown_on_pricing_error(monkeypatch):
    from okg.deployment import LLMPricingError, llm_call

    _install_litellm(monkeypatch, price=None, usage=False)

    with pytest.raises(LLMPricingError) as caught:
        llm_call("prompt", model="unknown-model")

    assert caught.value.input_tokens is None
    assert caught.value.output_tokens is None


def test_llm_call_explicit_zero_is_settled_and_consumed(monkeypatch):
    from okg.deployment import llm_call

    completion_cost = _install_litellm(monkeypatch, price=0.0)
    budget = SimpleNamespace(consume=Mock())

    response = llm_call("prompt", model="requested-model", budget=budget)

    assert response.cost_usd == 0.0
    assert response.pricing_status == "settled"
    budget.consume.assert_called_once_with(0.0)
    completion_cost.assert_called_once()


def test_llm_call_keeps_missing_usage_unknown_when_price_is_settled(monkeypatch):
    from okg.deployment import llm_call

    _install_litellm(monkeypatch, price=0.0025, usage=False)

    response = llm_call("prompt", model="requested-model")

    assert response.input_tokens is None
    assert response.output_tokens is None
    assert response.pricing_status == "settled"


def test_llm_call_positive_price_behavior_is_unchanged(monkeypatch):
    from okg.deployment import llm_call

    completion_cost = _install_litellm(monkeypatch, price=0.0025)
    budget = SimpleNamespace(consume=Mock())

    response = llm_call("prompt", model="requested-model", budget=budget)

    assert response.text == "answer"
    assert response.cost_usd == 0.0025
    assert response.input_tokens == 100
    assert response.output_tokens == 50
    assert response.model == "provider/resolved-model"
    assert response.pricing_status == "settled"
    budget.consume.assert_called_once_with(0.0025)
    completion_cost.assert_called_once()


def test_pricing_error_is_exported_from_sdk():
    from okg import deployment

    assert "LLMPricingError" in deployment.__all__
    assert deployment.LLMPricingError.__module__ == (
        "okg.deployment._internal.external"
    )
