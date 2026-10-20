"""Tests for review-mode admission DBOS workflow registration."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import psycopg
import pytest
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


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


def test_register_admission_workflows_caches_handles():
    from okg.deployment.testing import mock_dbos_context

    with mock_dbos_context():
        from okg.substrate.runtime.admission_workflows import (
            ADMISSION_DECIDE_WORKFLOW_NAME,
            ADMISSION_SUBMIT_WORKFLOW_NAME,
            _DECORATED,
            admission_decide_workflow,
            admission_submit_workflow,
            register_admission_workflows,
        )

        register_admission_workflows(deployment="test")

        assert ADMISSION_SUBMIT_WORKFLOW_NAME in _DECORATED
        assert ADMISSION_DECIDE_WORKFLOW_NAME in _DECORATED
        assert callable(admission_submit_workflow())
        assert callable(admission_decide_workflow())


def test_admission_workflow_accessors_raise_when_unregistered():
    from okg.deployment.testing import mock_dbos_context

    with mock_dbos_context():
        from okg.substrate.runtime.admission_workflows import (
            _DECORATED,
            admission_decide_workflow,
            admission_submit_workflow,
        )

        _DECORATED.clear()

        with pytest.raises(RuntimeError, match="not registered"):
            admission_submit_workflow()
        with pytest.raises(RuntimeError, match="not registered"):
            admission_decide_workflow()


def test_admission_workflow_names_are_stable():
    from okg.substrate.runtime.admission_workflows import (
        ADMISSION_DECIDE_WORKFLOW_NAME,
        ADMISSION_SUBMIT_WORKFLOW_NAME,
    )

    assert ADMISSION_SUBMIT_WORKFLOW_NAME == "okg_admission_submit"
    assert ADMISSION_DECIDE_WORKFLOW_NAME == "okg_admission_decide"


def test_admin_enqueue_helpers_exist():
    from okg.substrate.runtime import admin

    assert hasattr(admin, "client_enqueue_admission_submit")
    assert hasattr(admin, "client_enqueue_admission_decide")
    assert "client_enqueue_admission_submit" in admin.__all__
    assert "client_enqueue_admission_decide" in admin.__all__


def test_admin_submit_rejects_invalid_intent_before_client_build(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.runtime import admin

    monkeypatch.setattr(
        admin,
        "_build_client",
        lambda **_kwargs: pytest.fail("invalid intent built a DBOS client"),
    )
    with pytest.raises(ValueError, match="intent must match"):
        admin.client_enqueue_admission_submit(
            deployment="review-test",
            dsn="postgres://unused",
            payload={},
            producer_id="agent:review",
            intent="not an intent",
        )


@pytest.mark.postgres
def test_runtime_approver_accepts_one_complete_matching_rule(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.runtime import admission_workflows

    monkeypatch.setattr(
        admission_workflows,
        "_resolve_dsn",
        lambda _deployment: clean_runtime,
    )
    submitted = admission_workflows._run_submit_with_runtime_approver(
        deployment="review-test",
        payload={
            "nodes": [{
                "node_id": "review:alpha",
                "subtype": "alpha",
                "attrs": {"text": "approved"},
            }],
        },
        producer_id="agent:review",
        producer_kind="agent",
        intent="record_observation",
        idempotency_key="runtime-approver-one",
        authority={"producer_policy": dict(_REVIEW_ALPHA_POLICY)},
        check_summary={"record": {"kind": "observation"}},
        approver_config=_RUNTIME_APPROVER_CONFIG,
    )

    assert submitted["status"] == "accepted"
    assert submitted["runtime_approver"] == {
        "eligible": True,
        "reason_code": "runtime_approver_rule_match",
        "matched_rule_ids": ["bounded.alpha"],
        "node_count": 1,
        "edge_count": 0,
        "node_subtypes": ["alpha"],
        "edge_types": [],
        "actor": "runtime.approver",
    }
    assert submitted["emitted"]["node_facts_inserted"] == 1
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            "SELECT check_summary->'runtime_approver', status "
            "FROM okg.admission_envelopes "
            "WHERE admission_id = %s::uuid",
            (submitted["admission_id"],),
        )
        evaluation, status = cur.fetchone()
    assert status == "accepted"
    assert evaluation["matched_rule_ids"] == ["bounded.alpha"]
    assert evaluation["eligible"] is True


@pytest.mark.parametrize(
    ("config", "payload", "reason"),
    [
        (
            {
                "approver": {
                    **_RUNTIME_APPROVER_CONFIG["approver"],
                    "rules": [
                        _RUNTIME_APPROVER_CONFIG["approver"]["rules"][0],
                        {
                            **_RUNTIME_APPROVER_CONFIG["approver"]["rules"][0],
                            "id": "also.alpha",
                        },
                    ],
                },
            },
            {"node": {"node_id": "n:1", "subtype": "alpha"}},
            "runtime_approver_ambiguous_match",
        ),
        (
            _RUNTIME_APPROVER_CONFIG,
            {
                "nodes": [
                    {"node_id": "n:1", "subtype": "alpha"},
                    {"node_id": "n:2", "subtype": "alpha"},
                ],
            },
            "runtime_approver_over_budget",
        ),
        (
            _RUNTIME_APPROVER_CONFIG,
            {
                "node": {
                    "node_id": "n:1",
                    "subtype": "alpha",
                    "op": "R",
                },
            },
            "runtime_approver_retraction_forbidden",
        ),
        (
            _RUNTIME_APPROVER_CONFIG,
            {"node": {"subtype": "alpha"}},
            "runtime_approver_payload_malformed",
        ),
        (
            _RUNTIME_APPROVER_CONFIG,
            {"node": {"node_id": "   ", "subtype": "alpha"}},
            "runtime_approver_payload_malformed",
        ),
        (
            _RUNTIME_APPROVER_CONFIG,
            {
                "edge": {
                    "src": "n:1",
                    "dst": "n:2",
                    "edge_type": "links_to",
                    "provenance": "invalid",
                },
            },
            "runtime_approver_payload_malformed",
        ),
        (
            _RUNTIME_APPROVER_CONFIG,
            {
                "edge": {
                    "src": "n:1",
                    "dst": "n:2",
                    "edge_type": "links_to",
                    "confidence": "not-a-number",
                },
            },
            "runtime_approver_payload_malformed",
        ),
    ],
)
def test_runtime_approver_refuses_ambiguous_over_budget_and_retraction_payloads(
    config: dict,
    payload: dict,
    reason: str,
) -> None:
    from okg.substrate.runtime.admission_workflows import (
        evaluate_runtime_approver,
    )

    result = evaluate_runtime_approver(
        approver_config=config,
        payload=payload,
        producer_id="agent:review",
        producer_kind="agent",
        intent="record_observation",
        authority={"producer_policy": dict(_REVIEW_ALPHA_POLICY)},
    )
    assert result["eligible"] is False
    assert result["reason_code"] == reason


def test_runtime_approver_accepts_authoritative_derived_semantic_provenance(
) -> None:
    from okg.substrate.runtime.admission_workflows import (
        evaluate_runtime_approver,
    )

    config = {
        "approver": {
            "enabled": True,
            "actor": "runtime.approver",
            "rules": [{
                "id": "bounded.semantic-edge",
                "producer_ids": ["agent:review"],
                "producer_kinds": ["agent"],
                "intents": ["record_observation"],
                "node_subtypes": [],
                "edge_types": ["links_to"],
                "max_nodes": 1,
                "max_edges": 1,
            }],
        },
    }
    result = evaluate_runtime_approver(
        approver_config=config,
        payload={
            "edge": {
                "src": "n:1",
                "dst": "n:2",
                "edge_type": "links_to",
                "provenance": "derived_semantic",
            },
        },
        producer_id="agent:review",
        producer_kind="agent",
        intent="record_observation",
        authority={"producer_policy": {"admission_mode": "review"}},
    )

    assert result["eligible"] is True
    assert result["reason_code"] == "runtime_approver_rule_match"


@pytest.mark.postgres
def test_runtime_approver_duplicate_is_side_effect_free(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.runtime import admission_workflows

    monkeypatch.setattr(
        admission_workflows,
        "_resolve_dsn",
        lambda _deployment: clean_runtime,
    )
    kwargs = {
        "deployment": "review-test",
        "payload": {
            "node": {
                "node_id": "review:duplicate",
                "subtype": "alpha",
                "attrs": {},
            },
        },
        "producer_id": "agent:review",
        "producer_kind": "agent",
        "intent": "record_observation",
        "idempotency_key": "runtime-approver-duplicate",
        "authority": {"producer_policy": dict(_REVIEW_ALPHA_POLICY)},
        "check_summary": None,
        "approver_config": _RUNTIME_APPROVER_CONFIG,
    }
    first = admission_workflows._run_submit_with_runtime_approver(**kwargs)
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            "SELECT (SELECT count(*) FROM okg.admission_events), "
            "(SELECT count(*) FROM okg.admission_record_refs), "
            "(SELECT count(*) FROM okg.fact_batch_receipts), "
            "(SELECT count(*) FROM okg.node_facts)"
        )
        before = cur.fetchone()
    duplicate = admission_workflows._run_submit_with_runtime_approver(**kwargs)
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            "SELECT (SELECT count(*) FROM okg.admission_events), "
            "(SELECT count(*) FROM okg.admission_record_refs), "
            "(SELECT count(*) FROM okg.fact_batch_receipts), "
            "(SELECT count(*) FROM okg.node_facts)"
        )
        after = cur.fetchone()
    assert first["status"] == duplicate["status"] == "accepted"
    assert duplicate["inserted"] is False
    assert duplicate["runtime_approver"]["reason_code"] == (
        "runtime_approver_duplicate_existing"
    )
    assert after == before


def test_runtime_approver_bootstraps_schema_before_its_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#2639: the Chronos bootstrap commits, so it must run outside the block.

    Database-free: a fake connection records whether each step ran inside
    ``conn.transaction()``, and the fake bootstrap raises the same error
    psycopg raises when a commit is attempted inside that block.
    """
    from okg.substrate.db import connection as db_connection
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.runtime import admission_workflows

    events: list[tuple[str, bool]] = []

    class _FakeCursor:
        def __enter__(self) -> "_FakeCursor":
            return self

        def __exit__(self, *_exc: object) -> bool:
            return False

        def execute(self, *_args: object) -> None:
            events.append(("advisory_lock", fake_conn.in_transaction))

    class _FakeTransaction:
        def __enter__(self) -> None:
            events.append(("transaction_open", fake_conn.in_transaction))
            fake_conn.in_transaction = True

        def __exit__(self, *_exc: object) -> bool:
            fake_conn.in_transaction = False
            return False

    class _FakeConn:
        in_transaction = False

        def __enter__(self) -> "_FakeConn":
            return self

        def __exit__(self, *_exc: object) -> bool:
            return False

        def transaction(self) -> _FakeTransaction:
            return _FakeTransaction()

        def cursor(self) -> _FakeCursor:
            return _FakeCursor()

    fake_conn = _FakeConn()

    def _fake_bootstrap(conn: _FakeConn, **_kwargs: object) -> None:
        events.append(("bootstrap", conn.in_transaction))
        if conn.in_transaction:
            raise psycopg.ProgrammingError(
                "Explicit commit() forbidden within a Transaction context."
            )

    def _fake_submit(**kwargs: object) -> dict:
        events.append(("submit", fake_conn.in_transaction))
        assert kwargs["connection"] is fake_conn
        return {"status": "rejected", "admission_id": "not-used"}

    connect_calls: list[dict[str, object]] = []

    def _fake_connect(*_args: object, **kwargs: object) -> _FakeConn:
        connect_calls.append(dict(kwargs))
        return fake_conn

    monkeypatch.setattr(db_connection, "connect", _fake_connect)
    monkeypatch.setattr(ChronosGraphStore, "from_connection", _fake_bootstrap)
    monkeypatch.setattr(admission_workflows, "_resolve_dsn", lambda _d: "fake")
    monkeypatch.setattr(
        admission_workflows,
        "_existing_runtime_approver_submission",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(admission_workflows, "_run_submit", _fake_submit)

    result = admission_workflows._run_submit_with_runtime_approver(
        deployment="review-test",
        payload={"node": {"node_id": "review:boot", "subtype": "alpha"}},
        producer_id="agent:review",
        producer_kind="agent",
        intent="record_observation",
        idempotency_key="runtime-approver-bootstrap-order",
        authority=None,
        check_summary=None,
        approver_config=_RUNTIME_APPROVER_CONFIG,
    )

    assert result["status"] == "rejected"
    # Autocommit keeps the bootstrap's own reads from opening an implicit
    # transaction; without it `conn.transaction()` would be a savepoint and
    # the advisory lock would outlive the block.
    assert [call.get("autocommit") for call in connect_calls] == [True]
    assert events == [
        ("bootstrap", False),
        ("transaction_open", False),
        ("advisory_lock", True),
        ("submit", True),
    ]


@pytest.mark.postgres
def test_runtime_approver_decision_failure_rolls_back_submit(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.runtime import admission_workflows

    monkeypatch.setattr(
        admission_workflows,
        "_resolve_dsn",
        lambda _deployment: clean_runtime,
    )
    monkeypatch.setattr(
        admission_workflows,
        "_run_decide",
        lambda **_kwargs: (_ for _ in ()).throw(
            RuntimeError("fault between submit and decision")
        ),
    )
    with pytest.raises(RuntimeError, match="fault between"):
        admission_workflows._run_submit_with_runtime_approver(
            deployment="review-test",
            payload={
                "node": {
                    "node_id": "review:atomic",
                    "subtype": "alpha",
                    "attrs": {},
                },
            },
            producer_id="agent:review",
            producer_kind="agent",
            intent="record_observation",
            idempotency_key="runtime-approver-atomic-fault",
            authority={"producer_policy": dict(_REVIEW_ALPHA_POLICY)},
            check_summary=None,
            approver_config=_RUNTIME_APPROVER_CONFIG,
        )
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.admission_envelopes "
            "WHERE idempotency_key = 'runtime-approver-atomic-fault'"
        )
        assert int(cur.fetchone()[0]) == 0
        cur.execute(
            "SELECT count(*) FROM okg.node_facts "
            "WHERE node_id = 'review:atomic'"
        )
        assert int(cur.fetchone()[0]) == 0


@pytest.mark.postgres
def test_runtime_approver_concurrent_replay_has_one_event_sequence(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.runtime import admission_workflows

    monkeypatch.setattr(
        admission_workflows,
        "_resolve_dsn",
        lambda _deployment: clean_runtime,
    )
    kwargs = {
        "deployment": "review-test",
        "payload": {
            "node": {
                "node_id": "review:concurrent",
                "subtype": "alpha",
                "attrs": {},
            },
        },
        "producer_id": "agent:review",
        "producer_kind": "agent",
        "intent": "record_observation",
        "idempotency_key": "runtime-approver-concurrent",
        "authority": {"producer_policy": dict(_REVIEW_ALPHA_POLICY)},
        "check_summary": None,
        "approver_config": _RUNTIME_APPROVER_CONFIG,
    }
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(
            lambda _index: (
                admission_workflows._run_submit_with_runtime_approver(**kwargs)
            ),
            range(2),
        ))
    assert sorted(result["inserted"] for result in results) == [False, True]
    assert {result["status"] for result in results} == {"accepted"}
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT event_type, count(*)
              FROM okg.admission_events
             WHERE admission_id = (
               SELECT admission_id FROM okg.admission_envelopes
                WHERE idempotency_key = 'runtime-approver-concurrent'
             )
             GROUP BY event_type
             ORDER BY event_type
            """
        )
        event_counts = dict(cur.fetchall())
    assert "duplicate_coalesced" not in event_counts
    assert event_counts["submitted"] == 1
    assert event_counts["accepted"] == 1


@pytest.mark.postgres
def test_submit_honors_fast_track_policy_and_emits_facts(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.runtime import admission_workflows

    monkeypatch.setattr(
        admission_workflows,
        "_resolve_dsn",
        lambda _deployment: clean_runtime,
    )
    submitted = admission_workflows._run_submit(
        deployment="review-test",
        payload={
            "nodes": [
                {
                    "node_id": "fast:alpha",
                    "subtype": "alpha",
                    "attrs": {"text": "alpha"},
                },
            ],
        },
        producer_id="agent:fast",
        producer_kind="agent",
        intent="record_observation",
        idempotency_key="fast-valid",
        authority={
            "producer_policy": {
                "producer_id": "agent:fast",
                "producer_kind": "agent",
                "trust_label": "fast-track-trusted",
                "admission_mode": "fast_track",
                "allowed_record_kinds": ["node"],
                "output_signature": {
                    "nodes": [
                        {"subtype": "alpha", "clause_id": "allow_alpha"},
                    ],
                },
                "policy_clauses": [
                    {
                        "id": "allow_alpha",
                        "effect": "allow",
                        "record_kind": "node",
                        "subtype": "alpha",
                    },
                ],
            },
        },
        check_summary={"record": {"kind": "observation"}},
    )

    assert submitted["status"] == "accepted"
    assert submitted["admission_mode"] == "fast_track"
    assert submitted["emitted"]["node_facts_inserted"] == 1

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT admission_mode, status
              FROM okg.admission_envelopes
             WHERE admission_id = %s::uuid
            """,
            (submitted["admission_id"],),
        )
        assert cur.fetchone() == ("fast_track", "accepted")
        cur.execute(
            """
            SELECT count(*)
              FROM okg.node_facts
             WHERE source = %s
            """,
            ("agent:fast",),
        )
        assert int(cur.fetchone()[0]) == 1


@pytest.mark.postgres
@pytest.mark.parametrize(
    "payload",
    [
        {
            "nodes": [{
                "node_id": "fast:retract",
                "subtype": "alpha",
                "attrs": {},
                "op": "R",
            }],
        },
        {
            "edges": [{
                "src": "fast:a",
                "dst": "fast:b",
                "edge_type": "links_to",
                "attrs": {},
                "op": "R",
            }],
        },
    ],
)
def test_generic_admission_payload_cannot_self_authorize_retraction(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    payload: dict,
) -> None:
    from okg.substrate.runtime import admission_workflows
    from tests.pytest.catalog_fixture_builders import seed_catalog

    monkeypatch.setattr(
        admission_workflows,
        "_resolve_dsn",
        lambda _deployment: clean_runtime,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["alpha"],
            narrowings=[("alpha", "links_to", "alpha")],
        )
    policy = dict(_FAST_TRACK_ALPHA_POLICY)
    policy["allowed_record_kinds"] = ["node", "edge"]
    policy["output_signature"] = {
        "nodes": [{"subtype": "alpha", "clause_id": "allow_alpha"}],
        "edges": [{
            "src_subtype": "alpha",
            "edge_type": "links_to",
            "dst_subtype": "alpha",
            "clause_id": "allow_links",
        }],
    }
    result = admission_workflows._run_submit(
        deployment="review-test",
        payload=payload,
        producer_id="agent:fast",
        producer_kind="agent",
        intent="record_observation",
        idempotency_key=f"generic-retraction-{next(iter(payload))}",
        authority={"producer_policy": policy},
        check_summary={"record": {"kind": "observation"}},
    )
    assert result["status"] == "rejected"
    assert result["payload_rejected"] is True
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM okg.fact_batch_receipts")
        assert int(cur.fetchone()[0]) == 0
        cur.execute("SELECT count(*) FROM okg.node_facts")
        assert int(cur.fetchone()[0]) == 0
        cur.execute("SELECT count(*) FROM okg.edge_facts")
        assert int(cur.fetchone()[0]) == 0


@pytest.mark.postgres
def test_fast_track_graph_receipt_and_ledger_roll_back_together(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.runtime import admission_workflows

    monkeypatch.setattr(
        admission_workflows,
        "_resolve_dsn",
        lambda _deployment: clean_runtime,
    )
    from okg.substrate import admission

    real_transition = admission.transition_envelope

    def fail_accept(*args, **kwargs):
        if kwargs.get("status") == "accepted":
            raise RuntimeError("fault after graph apply")
        return real_transition(*args, **kwargs)

    monkeypatch.setattr(admission, "transition_envelope", fail_accept)
    with pytest.raises(RuntimeError, match="fault after graph apply"):
        admission_workflows._run_submit(
            deployment="review-test",
            payload={
                "nodes": [{
                    "node_id": "fast:atomic",
                    "subtype": "alpha",
                    "attrs": {"text": "atomic"},
                }],
            },
            producer_id="agent:fast",
            producer_kind="agent",
            intent="record_observation",
            idempotency_key="fast-atomic-fault",
            authority={"producer_policy": dict(_FAST_TRACK_ALPHA_POLICY)},
            check_summary={"record": {"kind": "observation"}},
        )
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM okg.admission_envelopes")
        assert int(cur.fetchone()[0]) == 0
        cur.execute("SELECT count(*) FROM okg.fact_batch_receipts")
        assert int(cur.fetchone()[0]) == 0
        cur.execute("SELECT count(*) FROM okg.node_facts")
        assert int(cur.fetchone()[0]) == 0
        rows = ChronosGraphStore.from_connection(conn).checkout("default").query(
            "SELECT node_id FROM okg.graph_nodes WHERE node_id = :node_id",
            {"node_id": "fast:atomic"},
        )
        assert rows == []


_FAST_TRACK_ALPHA_POLICY = {
    "producer_id": "agent:fast",
    "producer_kind": "agent",
    "trust_label": "fast-track-trusted",
    "admission_mode": "fast_track",
    "allowed_record_kinds": ["node"],
    "output_signature": {
        "nodes": [{"subtype": "alpha", "clause_id": "allow_alpha"}],
    },
    "policy_clauses": [
        {
            "id": "allow_alpha",
            "effect": "allow",
            "record_kind": "node",
            "subtype": "alpha",
        },
    ],
}


@pytest.mark.postgres
def test_fork_write_stamps_durable_promote_intent(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # worktree-fork-writes req.promote-intent: an agent's explicitly-durable FORK
    # write is stamped promote_intent="durable" on its fork facts (queryable by the
    # P3 merge-back), while a scratch fork write (the default) carries NO marker.
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.runtime import admission_workflows

    monkeypatch.setattr(
        admission_workflows, "_resolve_dsn", lambda _deployment: clean_runtime,
    )
    fork = "experiment:promote-test"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).create_branch(
            fork, from_branch="default",
        )

    def _submit(
        node_id: str, key: str, *, durable: bool, attrs: dict | None = None,
    ) -> dict:
        authority: dict = {
            "producer_policy": dict(_FAST_TRACK_ALPHA_POLICY),
            # The server normalizes promote_intent onto the authority for fork
            # writes (deny-by-default); here we pass the post-normalization shape.
            "target_branch_name": fork,
        }
        if durable:
            authority["promote_intent"] = "durable"
        return admission_workflows._run_submit(
            deployment="review-test",
            payload={
                "nodes": [
                    {"node_id": node_id, "subtype": "alpha",
                     "attrs": attrs or {"text": node_id}},
                ],
            },
            producer_id="agent:fast",
            producer_kind="agent",
            intent="record_observation",
            idempotency_key=key,
            authority=authority,
            check_summary={"record": {"kind": "observation"}},
        )

    durable = _submit("fork:durable", "promote-durable", durable=True)
    scratch = _submit("fork:scratch", "promote-scratch", durable=False)
    # B1 regression: a SCRATCH write that tries to SMUGGLE the marker through its
    # own node attrs must NOT be honored -- promote_intent is server-only and is
    # stripped from caller attrs in _normalize_node_payload.
    smuggle = _submit(
        "fork:smuggle", "promote-smuggle", durable=False,
        attrs={"text": "x", "promote_intent": "durable"},
    )
    assert durable["status"] == "accepted", durable
    assert scratch["status"] == "accepted", scratch
    assert smuggle["status"] == "accepted", smuggle

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = ChronosGraphStore.from_connection(conn).checkout(branch=fork)

        def intent_of(node_id: str) -> str | None:
            rows = list(session.query(
                "SELECT attrs FROM okg.graph_nodes WHERE node_id = %(n)s",
                {"n": node_id},
            ))
            return (dict(rows[0]["attrs"]) if rows else {}).get("promote_intent")

        # The durable fork fact carries the marker; the scratch one does NOT, and
        # the smuggled caller attr was stripped.
        assert intent_of("fork:durable") == "durable"
        assert intent_of("fork:scratch") is None
        assert intent_of("fork:smuggle") is None

        # Queryable: the merge-back filter selects exactly the server-stamped
        # durable fact -- not the scratch one, and NOT the smuggling attempt.
        durable_ids = {
            str(r["node_id"]) for r in session.query(
                "SELECT node_id FROM okg.graph_nodes "
                "WHERE attrs->>'promote_intent' = 'durable'",
                {},
            )
        }
        assert durable_ids == {"fork:durable"}, durable_ids


# ---------------------------------------------------------------------------
# NUL admission for the agent write-back (PACT
# sanitize-fact-text-at-admission, adversarial-review follow-up).
# `okg record` accepts JSON, and a legal \u0000 escape decodes to a
# real NUL — pre-fix that aborted the submit (payload-ref jsonb) or
# the apply (node_facts insert) with UntranslatableCharacter.
# ---------------------------------------------------------------------------


@pytest.mark.postgres
def test_submit_scrubs_json_nul_escape_and_publishes(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An `okg record`-shaped submission whose JSON carries \\u0000 in
    an attr string must be ACCEPTED with the value scrubbed (count on
    the receipt + envelope) and the generation must publish."""
    import json

    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.publisher import Publisher
    from okg.substrate.runtime import admission_workflows
    from tests.pytest.catalog_fixture_builders import seed_catalog

    monkeypatch.setattr(
        admission_workflows, "_resolve_dsn", lambda _d: clean_runtime,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["alpha"], narrowings=[])

    raw_json = (
        '{"nodes": [{"node_id": "fast:nul-attr", "subtype": "alpha", '
        '"attrs": {"text": "agent\\u0000note", "n": 1}}]}'
    )
    payload = json.loads(raw_json)
    # The live re-occurrence vector: a real NUL after JSON decoding.
    assert "\x00" in payload["nodes"][0]["attrs"]["text"]

    submitted = admission_workflows._run_submit(
        deployment="review-test",
        payload=payload,
        producer_id="agent:fast",
        producer_kind="agent",
        intent="record_observation",
        idempotency_key="nul-attr-1",
        authority={"producer_policy": dict(_FAST_TRACK_ALPHA_POLICY)},
        check_summary={"record": {"kind": "observation"}},
    )
    assert submitted["status"] == "accepted", submitted
    assert submitted["sanitized_value_count"] == 1
    assert submitted["emitted"]["node_facts_inserted"] == 1

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            "SELECT attrs->>'text' FROM okg.node_facts "
            "WHERE node_id = 'fast:nul-attr'",
        )
        assert cur.fetchone()[0] == "agent�note"
        # Loud-count is durable on the envelope.
        cur.execute(
            """
            SELECT check_summary->'sanitize'->>'sanitized_value_count'
              FROM okg.admission_envelopes
             WHERE admission_id = %s::uuid
            """,
            (submitted["admission_id"],),
        )
        assert cur.fetchone()[0] == "1"

    out = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert out["status"] == "published", out
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).checkout_generation(out["generation_id"])
        rows = graph.query(
            "SELECT attrs FROM okg.graph_nodes "
            "WHERE node_id = :node_id",
            {"node_id": "fast:nul-attr"},
        )
    assert rows
    assert rows[0]["attrs"]["text"] == "agent�note"


@pytest.mark.postgres
def test_submit_rejects_nul_identity_before_any_write(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A NUL in an identity-bearing payload field rejects the WHOLE
    submission loudly before anything is stored — identity is never
    rewritten, and a scrubbed audit copy would be a lie."""
    from okg.substrate.errors import SubstrateError
    from okg.substrate.runtime import admission_workflows

    monkeypatch.setattr(
        admission_workflows, "_resolve_dsn", lambda _d: clean_runtime,
    )
    with pytest.raises(SubstrateError, match="NUL"):
        admission_workflows._run_submit(
            deployment="review-test",
            payload={
                "nodes": [
                    {"node_id": "fast:\x00bad", "subtype": "alpha",
                     "attrs": {"text": "x"}},
                ],
            },
            producer_id="agent:fast",
            producer_kind="agent",
            intent="record_observation",
            idempotency_key="nul-id-1",
            authority={"producer_policy": dict(_FAST_TRACK_ALPHA_POLICY)},
            check_summary=None,
        )
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM okg.admission_envelopes")
        assert int(cur.fetchone()[0]) == 0
        cur.execute("SELECT count(*) FROM okg.node_facts")
        assert int(cur.fetchone()[0]) == 0


def test_sanitize_normalized_fact_scrubs_attrs_rejects_identity() -> None:
    """Apply-side defense unit: attrs scrub with a count; identity NUL
    raises a structured nul_in_identity rejection naming the field."""
    from okg.substrate.runtime.admission_workflows import (
        _ReviewPayloadRejected,
        _sanitize_normalized_fact,
    )

    node = {
        "node_id": "n:1", "subtype": "alpha",
        "attrs": {"text": "a\x00b"},
        "source_record_id": {"id": "n:1"},
        "source_revision": {"v": 1},
        "op": "I",
    }
    count = _sanitize_normalized_fact(
        node, kind="node", payload_index=0, ordinal=0,
    )
    assert count == 1
    assert node["attrs"]["text"] == "a�b"

    # provenance is a closed vocabulary (edge_facts_provenance_check):
    # a scrubbed value could never be valid, so NUL there REJECTS.
    nul_prov_edge = {
        "src": "n:1", "dst": "n:2", "edge_type": "links_to",
        "attrs": {}, "provenance": "prov\x00enance",
        "source_record_id": {}, "source_revision": {}, "op": "I",
    }
    with pytest.raises(_ReviewPayloadRejected) as prov_exc:
        _sanitize_normalized_fact(
            nul_prov_edge, kind="edge", payload_index=0, ordinal=0,
        )
    assert prov_exc.value.verdict["fields"] == ["provenance"]

    bad_edge = {
        "src": "n:1", "dst": "n:\x002", "edge_type": "links_to",
        "attrs": {}, "provenance": "explicit",
        "source_record_id": {}, "source_revision": {}, "op": "I",
    }
    with pytest.raises(_ReviewPayloadRejected) as exc:
        _sanitize_normalized_fact(
            bad_edge, kind="edge", payload_index=2, ordinal=3,
        )
    assert exc.value.reason == "nul_in_identity"
    assert exc.value.verdict["fields"] == ["dst"]
    assert exc.value.verdict["payload_index"] == 2
    assert exc.value.verdict["ordinal"] == 3


@pytest.mark.postgres
def test_review_accept_emits_facts_and_feedback_handles_payload_ref(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.admission import apply_generation_feedback
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.nomos.source_lineage import (
        LINEAGE_ATTR,
        source_ids_from_lineage,
    )
    from okg.substrate.runtime import admission_workflows

    monkeypatch.setattr(
        admission_workflows,
        "_resolve_dsn",
        lambda _deployment: clean_runtime,
    )
    _seed_catalog_with_narrowing(
        clean_runtime,
        parent="alpha",
        edge_type="links_to",
        child="beta",
    )
    submitted = admission_workflows._run_submit(
        deployment="review-test",
        payload={
            "nodes": [
                {
                    "node_id": "review:alpha",
                    "subtype": "alpha",
                    "attrs": {"text": "alpha"},
                },
                {
                    "node_id": "review:beta",
                    "subtype": "beta",
                    "attrs": {"text": "beta"},
                },
            ],
            "edges": [
                {
                    "src": "review:alpha",
                    "dst": "review:beta",
                    "edge_type": "links_to",
                },
            ],
        },
        producer_id="agent:test",
        producer_kind="agent",
        intent="review_proposal",
        idempotency_key="review-valid",
        authority={
            "producer_policy": {
                "producer_id": "agent:test",
                "producer_kind": "agent",
                "trust_label": "review-trusted",
                "admission_mode": "review",
                "allowed_record_kinds": ["node", "edge"],
                "output_signature": {
                    "nodes": [
                        {"subtype": "alpha", "clause_id": "allow_alpha"},
                        {"subtype": "beta", "clause_id": "allow_beta"},
                    ],
                    "edges": [
                        {
                            "src_subtype": "alpha",
                            "edge_type": "links_to",
                            "dst_subtype": "beta",
                            "clause_id": "allow_alpha_links_beta",
                        },
                    ],
                },
                "policy_clauses": [
                    {
                        "id": "allow_alpha",
                        "effect": "allow",
                        "record_kind": "node",
                        "subtype": "alpha",
                    },
                    {
                        "id": "allow_beta",
                        "effect": "allow",
                        "record_kind": "node",
                        "subtype": "beta",
                    },
                    {
                        "id": "allow_alpha_links_beta",
                        "effect": "allow",
                        "record_kind": "edge",
                        "triple": ["alpha", "links_to", "beta"],
                    },
                ],
            },
        },
        check_summary=None,
    )
    decided = admission_workflows._run_decide(
        deployment="review-test",
        admission_id=submitted["admission_id"],
        decision="accept",
        actor="reviewer",
        reason="fixture",
        verdict=None,
    )

    assert decided["status"] == "accepted"
    assert decided["emitted"]["node_facts_inserted"] == 2
    assert decided["emitted"]["edge_facts_inserted"] == 1

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, source, run_id::text
                  FROM okg.admission_envelopes
                 WHERE admission_id = %s::uuid
                """,
                (submitted["admission_id"],),
            )
            status, source, run_id = cur.fetchone()
            cur.execute(
                """
                SELECT ref_kind, record_count
                  FROM okg.admission_record_refs
                 WHERE admission_id = %s::uuid
                 ORDER BY ref_kind
                """,
                (submitted["admission_id"],),
            )
            refs = cur.fetchall()
            cur.execute(
                "SELECT max(fact_id) FROM okg.node_facts WHERE run_id=%s",
                (run_id,),
            )
            node_watermark = int(cur.fetchone()[0])
            cur.execute(
                "SELECT max(fact_id) FROM okg.edge_facts WHERE run_id=%s",
                (run_id,),
            )
            edge_watermark = int(cur.fetchone()[0])
            graph = ChronosGraphStore.from_connection(conn).checkout()
            graph_nodes = list(graph.query(
                """
                SELECT source, run_id::text, attrs
                  FROM okg.graph_nodes
                 WHERE node_id = %(node_id)s
                """,
                {"node_id": "review:alpha"},
            ))
            graph_edges = list(graph.query(
                """
                SELECT source, run_id::text, attrs
                  FROM okg.graph_edges
                 WHERE src = %(src)s
                   AND dst = %(dst)s
                   AND edge_type = %(edge_type)s
                """,
                {
                    "src": "review:alpha",
                    "dst": "review:beta",
                    "edge_type": "links_to",
                },
            ))
            generation_id = _insert_generation(
                cur,
                deployment_name="review-test",
                status="published",
                fact_watermark=node_watermark,
                edge_fact_watermark=edge_watermark,
            )
        with conn.transaction():
            feedback = apply_generation_feedback(
                conn,
                deployment_name="review-test",
                generation_id=generation_id,
            )
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, target_generation_ref, verdict
                  FROM okg.admission_envelopes
                 WHERE admission_id = %s::uuid
                """,
                (submitted["admission_id"],),
            )
            final_status, target_generation_ref, final_verdict = cur.fetchone()

    assert status == "accepted"
    assert source == "agent:test"
    assert graph_nodes
    assert graph_edges
    assert graph_nodes[0]["source"] == source
    assert graph_nodes[0]["run_id"] == run_id
    assert source_ids_from_lineage(
        graph_nodes[0]["attrs"].get(LINEAGE_ATTR),
    ) == (source,)
    assert graph_edges[0]["source"] == source
    assert graph_edges[0]["run_id"] == run_id
    assert source_ids_from_lineage(
        graph_edges[0]["attrs"].get(LINEAGE_ATTR),
    ) == (source,)
    assert refs == [
        ("edge_fact_range", 1),
        ("node_fact_range", 2),
        ("payload", 1),
    ]
    assert feedback["updated"] == 1
    assert final_status == "published"
    assert target_generation_ref == str(generation_id)
    assert "allow_alpha_links_beta" in (
        final_verdict["producer_policy"]["matched_clause_ids"]
    )
    assert final_verdict["catalog"]["narrowings"] == [
        {
            "src_subtype": "alpha",
            "edge_type": "links_to",
            "dst_subtype": "beta",
            "status": "matched",
        },
    ]


@pytest.mark.postgres
def test_review_accept_fault_rolls_back_graph_facts_refs_and_envelope(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#695: a post-graph-apply fault leaves no partial durable effect."""
    from okg.substrate import admission
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.runtime import admission_workflows

    monkeypatch.setattr(
        admission_workflows,
        "_resolve_dsn",
        lambda _deployment: clean_runtime,
    )
    _seed_catalog_with_narrowing(
        clean_runtime,
        parent="alpha",
        edge_type="links_to",
        child="beta",
    )
    submitted = admission_workflows._run_submit(
        deployment="review-test",
        payload={
            "nodes": [
                {
                    "node_id": "review:rollback",
                    "subtype": "alpha",
                    "attrs": {"text": "must roll back"},
                },
            ],
        },
        producer_id="agent:rollback",
        producer_kind="agent",
        intent="review_proposal",
        idempotency_key="review-rollback-fault",
        authority={
            "producer_policy": {
                "producer_id": "agent:rollback",
                "producer_kind": "agent",
                "trust_label": "review-trusted",
                "admission_mode": "review",
                "allowed_record_kinds": ["node"],
                "output_signature": {
                    "nodes": [
                        {"subtype": "alpha", "clause_id": "allow_alpha"},
                    ],
                },
                "policy_clauses": [
                    {
                        "id": "allow_alpha",
                        "effect": "allow",
                        "record_kind": "node",
                        "subtype": "alpha",
                    },
                ],
            },
        },
        check_summary=None,
    )

    def _fail_after_graph_apply(*_args, **_kwargs):
        raise RuntimeError("fault after graph apply")

    monkeypatch.setattr(
        admission,
        "attach_fact_range_refs_for_run",
        _fail_after_graph_apply,
    )
    with pytest.raises(RuntimeError, match="fault after graph apply"):
        admission_workflows._run_decide(
            deployment="review-test",
            admission_id=submitted["admission_id"],
            decision="accept",
            actor="reviewer",
            reason="injected fault",
            verdict=None,
        )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status
                  FROM okg.admission_envelopes
                 WHERE admission_id = %s::uuid
                """,
                (submitted["admission_id"],),
            )
            (status,) = cur.fetchone()
            cur.execute(
                "SELECT count(*) FROM okg.node_facts WHERE source = 'agent:rollback'",
            )
            (fact_count,) = cur.fetchone()
            cur.execute(
                """
                SELECT count(*)
                  FROM okg.admission_record_refs
                 WHERE admission_id = %s::uuid
                """,
                (submitted["admission_id"],),
            )
            (ref_count,) = cur.fetchone()
        graph_rows = (
            ChronosGraphStore.from_connection(conn)
            .checkout()
            .query(
                """
            SELECT count(*) AS n
              FROM okg.graph_nodes
             WHERE node_id = 'review:rollback'
            """,
            )
        )

    assert status == "pending"
    assert int(fact_count) == 0
    assert int(ref_count) == 1, "only the submit-time payload ref remains"
    assert graph_rows == [{"n": 0}]


@pytest.mark.postgres
def test_admission_decide_nomos_denial_blocks_before_transition(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.runtime import admission_workflows

    monkeypatch.setattr(
        admission_workflows,
        "_resolve_dsn",
        lambda _deployment: clean_runtime,
    )
    submitted = admission_workflows._run_submit(
        deployment="review-test",
        payload={
            "nodes": [
                {
                    "node_id": "review:nomos-block",
                    "subtype": "alpha",
                    "attrs": {"text": "held"},
                },
            ],
        },
        producer_id="agent:review",
        producer_kind="agent",
        intent="review_proposal",
        idempotency_key="review-nomos-block",
        authority={
            "producer_policy": {
                "producer_id": "agent:review",
                "producer_kind": "agent",
                "trust_label": "review-trusted",
                "admission_mode": "review",
                "allowed_record_kinds": ["node"],
                "output_signature": {
                    "nodes": [
                        {"subtype": "alpha", "clause_id": "allow_alpha"},
                    ],
                },
                "policy_clauses": [
                    {
                        "id": "allow_alpha",
                        "effect": "allow",
                        "record_kind": "node",
                        "subtype": "alpha",
                    },
                ],
            },
        },
        check_summary=None,
    )
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")

    decided = admission_workflows._run_decide(
        deployment="review-test",
        admission_id=submitted["admission_id"],
        decision="accept",
        actor="reviewer",
        reason="fixture",
        verdict=None,
    )

    assert decided["status"] == "refused"
    assert decided["reason"] == "nomos_admin_override_denied"
    assert decided["nomos_decision"]["effect"] == "deny"
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT status
              FROM okg.admission_envelopes
             WHERE admission_id = %s::uuid
            """,
            (submitted["admission_id"],),
        )
        (status,) = cur.fetchone()
        cur.execute(
            """
            SELECT action, effect, target->>'kind'
              FROM okg.nomos_decisions
             WHERE deployment_name = 'review-test'
             ORDER BY created_at, decision_row_id
            """
        )
        decisions = cur.fetchall()

    assert status == "pending"
    assert decisions == [("admin.override", "deny", "admission_admin_override")]


@pytest.mark.postgres
def test_review_accept_uses_subtype_ancestor_narrowing(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.publisher import Publisher
    from okg.substrate.runtime import admission_workflows

    monkeypatch.setattr(
        admission_workflows,
        "_resolve_dsn",
        lambda _deployment: clean_runtime,
    )
    cv = _seed_catalog_with_narrowing(
        clean_runtime,
        parent="alpha",
        edge_type="links_to",
        child="beta",
    )
    _insert_refinement_subtype(clean_runtime, cv, "alpha_concrete", "alpha")
    _insert_refinement_subtype(clean_runtime, cv, "beta_concrete", "beta")

    submitted = admission_workflows._run_submit(
        deployment="review-test",
        payload={
            "nodes": [
                {
                    "node_id": "review:alpha-concrete",
                    "subtype": "alpha_concrete",
                    "attrs": {"text": "alpha concrete"},
                },
                {
                    "node_id": "review:beta-concrete",
                    "subtype": "beta_concrete",
                    "attrs": {"text": "beta concrete"},
                },
            ],
            "edges": [
                {
                    "src": "review:alpha-concrete",
                    "dst": "review:beta-concrete",
                    "edge_type": "links_to",
                },
            ],
        },
        producer_id="agent:test",
        producer_kind="agent",
        intent="review_proposal",
        idempotency_key="review-refinement",
        authority={
            "producer_id": "agent:test",
            "producer_kind": "agent",
            "allow_write": True,
        },
        check_summary=None,
    )
    decided = admission_workflows._run_decide(
        deployment="review-test",
        admission_id=submitted["admission_id"],
        decision="accept",
        actor="reviewer",
        reason="fixture",
        verdict=None,
    )

    assert decided["status"] == "accepted"
    assert decided["emitted"]["edge_facts_inserted"] == 1
    published = Publisher(clean_runtime, "review-test", catalog_identity=bind_test_catalog_writer(clean_runtime, "review-test")).cycle()
    if published["status"] != "published":
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT blocked_reason
                      FROM okg.graph_generations
                     WHERE generation_id = %s
                    """,
                    (published["generation_id"],),
                )
                blocked_reason = cur.fetchone()[0]
        pytest.fail(f"publish blocked: {blocked_reason}")

    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        generation = store.checkout_generation(published["generation_id"])
        assert generation.query(
            """
            SELECT count(*) AS count
              FROM okg.graph_edges
             WHERE src = 'review:alpha-concrete'
               AND dst = 'review:beta-concrete'
               AND edge_type = 'links_to'
            """
        ) == [{"count": 1}]
    finally:
        store.close()


@pytest.mark.postgres
def test_review_accept_rejects_narrowing_invalid_edge(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.runtime import admission_workflows

    monkeypatch.setattr(
        admission_workflows,
        "_resolve_dsn",
        lambda _deployment: clean_runtime,
    )
    _seed_catalog_with_narrowing(
        clean_runtime,
        parent="alpha",
        edge_type="links_to",
        child="beta",
    )
    submitted = admission_workflows._run_submit(
        deployment="review-test",
        payload={
            "nodes": [
                {"node_id": "bad:alpha", "subtype": "alpha"},
                {"node_id": "bad:beta", "subtype": "beta"},
            ],
            "edges": [
                {
                    "src": "bad:alpha",
                    "dst": "bad:beta",
                    "edge_type": "not_allowed",
                },
            ],
        },
        producer_id="agent:test",
        producer_kind="agent",
        intent="review_proposal",
        idempotency_key="review-invalid",
        authority=None,
        check_summary=None,
    )
    decided = admission_workflows._run_decide(
        deployment="review-test",
        admission_id=submitted["admission_id"],
        decision="accept",
        actor="reviewer",
        reason=None,
        verdict=None,
    )

    assert decided["status"] == "rejected"
    assert decided["payload_rejected"] is True

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, verdict
              FROM okg.admission_envelopes
             WHERE admission_id = %s::uuid
            """,
            (submitted["admission_id"],),
        )
        status, verdict = cur.fetchone()
        cur.execute(
            """
            SELECT reason
              FROM okg.admission_events
             WHERE admission_id = %s::uuid
               AND event_type = 'rejected'
             ORDER BY event_id DESC
             LIMIT 1
            """,
            (submitted["admission_id"],),
        )
        reason = cur.fetchone()[0]
        cur.execute(
            """
            SELECT count(*)
              FROM okg.edge_facts
             WHERE source = %s
            """,
            ("agent:test",),
        )
        edge_facts = int(cur.fetchone()[0])

    assert status == "rejected"
    assert verdict["code"] == "narrowing_violation"
    assert reason == "narrowing_violation"
    assert edge_facts == 0


@pytest.mark.postgres
def test_review_accept_rejects_producer_policy_invalid_node(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.runtime import admission_workflows

    monkeypatch.setattr(
        admission_workflows,
        "_resolve_dsn",
        lambda _deployment: clean_runtime,
    )
    submitted = admission_workflows._run_submit(
        deployment="review-test",
        payload={
            "nodes": [
                {"node_id": "bad:beta", "subtype": "beta"},
            ],
        },
        producer_id="agent:policy",
        producer_kind="agent",
        intent="review_proposal",
        idempotency_key="review-policy-invalid",
        authority={
            "producer_policy": {
                "producer_id": "agent:policy",
                "producer_kind": "agent",
                "trust_label": "review-trusted",
                "admission_mode": "review",
                "allowed_record_kinds": ["node"],
                "output_signature": {
                    "nodes": [
                        {
                            "subtype": "alpha",
                            "clause_id": "allow_alpha",
                        },
                    ],
                },
                "policy_clauses": [
                    {
                        "id": "allow_alpha",
                        "effect": "allow",
                        "record_kind": "node",
                        "subtype": "alpha",
                    },
                ],
            },
        },
        check_summary=None,
    )
    decided = admission_workflows._run_decide(
        deployment="review-test",
        admission_id=submitted["admission_id"],
        decision="accept",
        actor="reviewer",
        reason=None,
        verdict=None,
    )

    assert decided["status"] == "rejected"
    assert decided["payload_rejected"] is True

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT verdict
              FROM okg.admission_envelopes
             WHERE admission_id = %s::uuid
            """,
            (submitted["admission_id"],),
        )
        verdict = cur.fetchone()[0]
        cur.execute(
            """
            SELECT count(*)
              FROM okg.node_facts
             WHERE source = %s
            """,
            ("agent:policy",),
        )
        node_facts = int(cur.fetchone()[0])

    assert verdict["code"] == "producer_node_subtype_not_allowed"
    assert verdict["producer_policy"]["profile_fingerprint"].startswith(
        "sha256:",
    )
    assert node_facts == 0


def _seed_catalog_with_narrowing(
    dsn: str,
    *,
    parent: str,
    edge_type: str,
    child: str,
) -> int:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        cv = int(cur.fetchone()[0])
        for subtype in (parent, child):
            cur.execute(
                "INSERT INTO okg.subtypes (name, catalog_version_id) "
                "VALUES (%s, %s)",
                (subtype, cv),
            )
            cur.execute(
                """
                INSERT INTO okg.subtype_ancestors
                  (catalog_version_id, subtype_name, ancestor_name, depth)
                VALUES (%s, %s, %s, 0)
                ON CONFLICT DO NOTHING
                """,
                (cv, subtype, subtype),
            )
        cur.execute(
            """
            INSERT INTO okg.narrowings
              (parent_subtype, edge_type, child_subtype,
               catalog_version_id)
            VALUES (%s, %s, %s, %s)
            """,
            (parent, edge_type, child, cv),
        )
        return cv


def _insert_refinement_subtype(
    dsn: str,
    cv: int,
    subtype: str,
    parent: str,
) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.subtypes
              (name, parent_subtype, catalog_version_id)
            VALUES (%s, %s, %s)
            """,
            (subtype, parent, cv),
        )
        cur.execute(
            """
            INSERT INTO okg.subtype_ancestors
              (catalog_version_id, subtype_name, ancestor_name, depth)
            VALUES
              (%s, %s, %s, 0),
              (%s, %s, %s, 1)
            ON CONFLICT DO NOTHING
            """,
            (cv, subtype, subtype, cv, subtype, parent),
        )


def _insert_generation(
    cur,
    *,
    deployment_name: str,
    status: str,
    fact_watermark: int = 0,
    edge_fact_watermark: int = 0,
) -> int:
    cur.execute(
        """
        INSERT INTO okg.graph_generations
          (deployment_name, status, fact_watermark, edge_fact_watermark,
           catalog_version_id, published_at, correlation_id)
        VALUES
          (%s, %s, %s, %s,
           (SELECT max(catalog_version_id)
              FROM okg.catalog_versions
             WHERE status = 'applied'),
           now(), %s)
        RETURNING generation_id
        """,
        (
            deployment_name,
            status,
            fact_watermark,
            edge_fact_watermark,
            "review-admission-test",
        ),
    )
    return int(cur.fetchone()[0])


def _submit_node(admission_workflows, dsn: str, node: dict):
    """Fast-track-submit a single node via the admission path."""
    return admission_workflows._run_submit(
        deployment="review-test",
        payload={"nodes": [node]},
        producer_id="agent:fast",
        producer_kind="agent",
        intent="record_observation",
        idempotency_key=f"transcript-guard-{node['node_id']}",
        authority={
            "producer_policy": {
                "producer_id": "agent:fast",
                "producer_kind": "agent",
                "trust_label": "fast-track-trusted",
                "admission_mode": "fast_track",
                "allowed_record_kinds": ["node"],
                "output_signature": {
                    "nodes": [{"subtype": node["subtype"], "clause_id": "allow"}],
                },
                "policy_clauses": [
                    {
                        "id": "allow",
                        "effect": "allow",
                        "record_kind": "node",
                        "subtype": node["subtype"],
                    },
                ],
            },
        },
        check_summary={"record": {"kind": "observation"}},
    )


@pytest.mark.postgres
def test_submit_refuses_agent_authored_transcript_nodes(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An admission write (record/propose path) may not hand-mint transcript
    nodes: they are populated only by the agent-sessions ingest source, and
    authoring one would collide with -- but not dedup against -- the
    source-ingested node, double-counting the transcript. The submit must be
    REJECTED and land NO facts; legitimate agent writes are unaffected.
    """
    from okg.substrate.runtime import admission_workflows

    monkeypatch.setattr(
        admission_workflows, "_resolve_dsn", lambda _d: clean_runtime,
    )

    # Rejected: by transcript subtype, by reserved id prefix, and tool events.
    rejected = [
        _submit_node(admission_workflows, clean_runtime,
                     {"node_id": "x:1", "subtype": "agent_session", "attrs": {"text": "t"}}),
        _submit_node(admission_workflows, clean_runtime,
                     {"node_id": "session:abc", "subtype": "alpha", "attrs": {"text": "t"}}),
        _submit_node(admission_workflows, clean_runtime,
                     {"node_id": "y:1", "subtype": "tool_use_event", "attrs": {"text": "t"}}),
        _submit_node(admission_workflows, clean_runtime,
                     {"node_id": "message:s:0", "subtype": "alpha", "attrs": {"text": "t"}}),
    ]
    for r in rejected:
        assert r["status"] == "rejected", r
        assert r.get("payload_rejected") is True, r

    # No facts landed for any rejected envelope.
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.node_facts WHERE source = %s",
            ("agent:fast",),
        )
        assert int(cur.fetchone()[0]) == 0

    # Control: a legitimate agent node (non-transcript subtype, non-reserved id)
    # is still accepted and emitted.
    ok = _submit_node(admission_workflows, clean_runtime,
                      {"node_id": "fast:alpha", "subtype": "alpha", "attrs": {"text": "alpha"}})
    assert ok["status"] == "accepted"
    assert ok["emitted"]["node_facts_inserted"] == 1


@pytest.mark.postgres
def test_inline_cli_submit_serializes_concurrent_replays(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`okg admission submit` on a runtime-disabled deployment runs the
    queued path's body inline: two concurrent submits with one idempotency
    key produce one envelope, one submitted event and one payload ref, and
    the replay returns before any profile or payload write."""
    from types import SimpleNamespace

    from okg.substrate.cli import admission as admission_cli
    from okg.substrate.runtime import admission_workflows

    def no_manifest_dsn(_deployment):
        raise AssertionError("inline submit must use the caller's --dsn")

    monkeypatch.setattr(admission_workflows, "_resolve_dsn", no_manifest_dsn)
    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda _deployment: SimpleNamespace(
            runtime=SimpleNamespace(enabled=False),
        ),
    )
    enqueued: list[dict] = []
    monkeypatch.setattr(
        "okg.substrate.runtime.admin.client_enqueue_admission_submit",
        lambda **kwargs: enqueued.append(kwargs),
    )
    materialized: list[str] = []
    real_materialize = admission_workflows._run_submit

    def counting_run_submit(**kwargs):
        materialized.append(kwargs["idempotency_key"])
        return real_materialize(**kwargs)

    monkeypatch.setattr(admission_workflows, "_run_submit", counting_run_submit)
    args = SimpleNamespace(
        deployment="inline-test",
        payload_json='{"node": {"node_id": "inline:alpha", "subtype": "alpha"}}',
        payload_file=None,
        authority_json=None,
        check_summary_json=None,
        producer_id="agent:inline",
        producer_kind="agent",
        intent="review_proposal",
        idempotency_key="inline-concurrent",
        max_wait_seconds=1,
        poll_interval_seconds=0.1,
    )
    with ThreadPoolExecutor(max_workers=2) as pool:
        codes = list(pool.map(
            lambda _index: admission_cli._cmd_admission_submit(
                args, dsn=clean_runtime, as_json=True,
            ),
            range(2),
        ))

    assert codes == [0, 0]
    assert enqueued == []
    # The replay returned from the existence read, before _run_submit.
    assert materialized == ["inline-concurrent"]
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT admission_id::text, status
              FROM okg.admission_envelopes
             WHERE deployment_name = 'inline-test'
               AND idempotency_key = 'inline-concurrent'
            """
        )
        envelopes = cur.fetchall()
        assert len(envelopes) == 1
        assert envelopes[0][1] == "pending"
        cur.execute(
            """
            SELECT event_type, count(*)
              FROM okg.admission_events
             WHERE admission_id = %s::uuid
             GROUP BY event_type
            """,
            (envelopes[0][0],),
        )
        events = dict(cur.fetchall())
        cur.execute(
            "SELECT count(*) FROM okg.admission_record_refs WHERE admission_id = %s::uuid",
            (envelopes[0][0],),
        )
        refs = int(cur.fetchone()[0])
    assert "duplicate_coalesced" not in events, events
    assert events.get("submitted") == 1, events
    assert refs == 1
