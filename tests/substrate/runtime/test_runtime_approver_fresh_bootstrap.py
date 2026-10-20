"""#2639: a runtime-approver submit works in a process that never bootstrapped.

The first MCP ``record`` in a fresh ``okg mcp-serve`` process used to fail
with "Explicit commit() forbidden within a Transaction context": the Chronos
schema bootstrap, which ends in an explicit commit, ran inside the submit's
``conn.transaction()``. This file proves the fix on a real database. It is its
own file so the blocking per-PR ``substrate-db`` lane can own it; the ordering
check that needs no database lives in ``test_admission_workflows.py``.
"""
from __future__ import annotations

import psycopg
import pytest

# The rule and producer policy below match the ones in
# test_admission_workflows.py: one agent, one "alpha" node, accepted by rule.
_RUNTIME_APPROVER_CONFIG = {
    "approver": {
        "enabled": True,
        "actor": "runtime.approver",
        "rules": [{
            "id": "bounded.alpha",
            "producer_ids": ["agent:review"],
            "producer_kinds": ["agent"],
            "intents": ["record_observation"],
            "node_subtypes": ["alpha"],
            "edge_types": [],
            "max_nodes": 1,
            "max_edges": 1,
        }],
    },
}

_REVIEW_ALPHA_POLICY = {
    "producer_id": "agent:review",
    "producer_kind": "agent",
    "trust_label": "review-trusted",
    "admission_mode": "review",
    "allowed_record_kinds": ["node"],
    "output_signature": {
        "nodes": [{"subtype": "alpha", "clause_id": "allow_alpha"}],
    },
    "policy_clauses": [{
        "id": "allow_alpha",
        "effect": "allow",
        "record_kind": "node",
        "subtype": "alpha",
    }],
}


@pytest.mark.postgres
def test_runtime_approver_submits_in_a_process_that_never_bootstrapped(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#2639 regression on a real database: an empty bootstrap cache.

    A fresh ``okg mcp-serve`` process has not bootstrapped the Chronos schema,
    so its first ``record`` used to fail with "Explicit commit() forbidden
    within a Transaction context". Clearing the process cache reproduces that
    state whatever ran earlier in this pytest process.
    """
    from okg.substrate.graph_store_internal.common import (
        clear_schema_ensured_cache,
    )
    from okg.substrate.runtime import admission_workflows

    monkeypatch.setattr(
        admission_workflows,
        "_resolve_dsn",
        lambda _deployment: clean_runtime,
    )
    clear_schema_ensured_cache()
    submitted = admission_workflows._run_submit_with_runtime_approver(
        deployment="review-test",
        payload={
            "node": {
                "node_id": "review:fresh-process",
                "subtype": "alpha",
                "attrs": {},
            },
        },
        producer_id="agent:review",
        producer_kind="agent",
        intent="record_observation",
        idempotency_key="runtime-approver-fresh-process",
        authority={"producer_policy": dict(_REVIEW_ALPHA_POLICY)},
        check_summary=None,
        approver_config=_RUNTIME_APPROVER_CONFIG,
    )

    assert submitted["status"] == "accepted"
    assert submitted["inserted"] is True
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            "SELECT status FROM okg.admission_envelopes "
            "WHERE admission_id = %s::uuid",
            (submitted["admission_id"],),
        )
        row = cur.fetchone()
    assert row == ("accepted",)
