"""Nomos admission override sees the source the accept will write.

An agent ``record`` is submitted through ``admission_submit``, which never
writes the envelope's ``source`` column. When it is accepted,
``_emit_review_payload_facts`` stamps its facts with the envelope's source,
else its producer id, else ``admission:<id>``. Before the fix, the
``admin.override`` / ``quarantine.release`` check sent the stored column,
``None``, and Nomos refused every such accept as
``nomos.invalid_identity_fact``, whatever the policy said.

The check now sends the effective source, computed by the same helper the
emitter uses. These tests run the real path on PostgreSQL: an active policy
bundle, the enforce-mode switch, and the submit-then-accept transaction.

- A sourceless agent record is accepted when the policy grants the override,
  and the written facts carry the source the policy saw.
- A deny scoped to a source refuses an agent envelope whose producer id is
  that source name (the caller chooses its producer id), and writes nothing.
- A deny scoped to a source refuses that source's envelope for every
  source-bound intent.
- A ``source_sync`` envelope is applied under its stored source, so a
  missing or blank one is still refused; so is any blank effective source.
- A ``source: null`` deny rule never matches: the effective source is never
  null for an envelope that reaches a rule.
- Submit refuses a caller-chosen identity the server assigns: a blank or
  padded producer id, a server prefix, the id of a registered or
  database-known source, and the ``source`` producer kind. A legitimate
  agent id still works end to end.
"""
from __future__ import annotations

import uuid
from typing import Any

import psycopg
import pytest

pytestmark = pytest.mark.postgres

_DEPLOYMENT = "review-test"
_SCOPE = f"deployment:{_DEPLOYMENT}"
_APPROVER = "runtime.approver"
_OVERRIDE_ACTIONS = ["admin.override", "quarantine.release"]


def _approver_config(
    producer_id: str,
    *,
    actor: str = _APPROVER,
) -> dict[str, Any]:
    # Same rule as test_admission_workflows.py: one agent, one "alpha" node.
    return {
        "approver": {
            "enabled": True,
            "actor": actor,
            "rules": [{
                "id": "bounded.alpha",
                "producer_ids": [producer_id],
                "producer_kinds": ["agent"],
                "intents": ["record_observation"],
                "node_subtypes": ["alpha"],
                "edge_types": [],
                "max_nodes": 1,
                "max_edges": 1,
            }],
        },
    }


def _review_alpha_policy(producer_id: str) -> dict[str, Any]:
    return {
        "producer_id": producer_id,
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


def _allow_override_rule(**match: Any) -> dict[str, Any]:
    rule: dict[str, Any] = {
        "rule_id": "allow_runtime_approver_override",
        "effect": "allow",
        "actions": list(_OVERRIDE_ACTIONS),
        "subjects": [_APPROVER],
    }
    if match:
        rule["match"] = dict(match)
    return rule


def _deny_source_rule(source: Any) -> dict[str, Any]:
    return {
        "rule_id": "deny_source_override",
        "effect": "deny",
        "actions": list(_OVERRIDE_ACTIONS),
        "subjects": ["*"],
        "match": {"source": source},
        "priority": 10,
    }


def _activate(dsn: str, *rules: dict[str, Any], version: str) -> None:
    from okg.substrate.nomos import activate_policy_bundle, compile_policy_graph

    bundle = compile_policy_graph({
        "policy_bundle_id": f"nomos:test:sourceless-override:{version}",
        "policy_version": version,
        "default_effect": "deny",
        "rules": list(rules),
    })
    with psycopg.connect(dsn) as conn:
        activate_policy_bundle(
            conn,
            deployment_name=_DEPLOYMENT,
            bundle=bundle,
            scope=_SCOPE,
            activated_by="test:operator",
        )
        conn.commit()


def _enforce(monkeypatch: pytest.MonkeyPatch, dsn: str) -> Any:
    from okg.substrate.runtime import admission_workflows

    monkeypatch.setattr(
        admission_workflows,
        "_resolve_dsn",
        lambda _deployment: dsn,
    )
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")
    return admission_workflows


def _submit_record(
    admission_workflows: Any,
    key: str,
    *,
    producer_id: str = "agent:review",
) -> dict[str, Any]:
    return admission_workflows._run_submit_with_runtime_approver(
        deployment=_DEPLOYMENT,
        payload={
            "nodes": [{
                "node_id": f"review:{key}",
                "subtype": "alpha",
                "attrs": {"text": "approved"},
            }],
        },
        producer_id=producer_id,
        producer_kind="agent",
        intent="record_observation",
        idempotency_key=key,
        authority={"producer_policy": _review_alpha_policy(producer_id)},
        check_summary={"record": {"kind": "observation"}},
        approver_config=_approver_config(producer_id),
    )


def _envelope_status(dsn: str, admission_id: str) -> str:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT status FROM okg.admission_envelopes "
            "WHERE admission_id = %s::uuid",
            (admission_id,),
        )
        (status,) = cur.fetchone()
    return str(status)


def _node_fact_sources(dsn: str, node_id: str) -> list[str]:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT source FROM okg.node_facts WHERE node_id = %s",
            (node_id,),
        )
        return [str(row[0]) for row in cur.fetchall()]


def _override_decisions(dsn: str) -> list[tuple[str, str, str]]:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT action, effect, reason_code
              FROM okg.nomos_decisions
             WHERE deployment_name = %s
               AND action = ANY(%s)
             ORDER BY created_at, decision_row_id
            """,
            (_DEPLOYMENT, _OVERRIDE_ACTIONS),
        )
        return [tuple(row) for row in cur.fetchall()]


def _insert_review_envelope(
    dsn: str,
    *,
    producer_id: str,
    producer_kind: str,
    intent: str,
    source: str | None,
    run_id: str | None,
    key: str,
) -> str:
    """Write a pending review envelope with an exact identity shape."""
    from okg.substrate.admission import ensure_envelope

    with psycopg.connect(dsn) as conn:
        envelope = ensure_envelope(
            conn,
            deployment_name=_DEPLOYMENT,
            producer_id=producer_id,
            producer_kind=producer_kind,
            intent=intent,
            admission_mode="review",
            status="pending",
            idempotency_key=key,
            source=source,
            run_id=run_id,
        )
        conn.commit()
    return str(envelope.admission_id)


def _decide_accept(admission_workflows: Any, admission_id: str) -> dict:
    return admission_workflows._run_decide(
        deployment=_DEPLOYMENT,
        admission_id=admission_id,
        decision="accept",
        actor=_APPROVER,
        reason="fixture",
        verdict=None,
    )


def test_runtime_approver_accepts_sourceless_agent_record_under_enforce(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The reported bug: a policy that grants the override must allow it.

    The facts are written under the source the policy was shown.
    """
    _activate(clean_runtime, _allow_override_rule(), version="allow-v1")
    admission_workflows = _enforce(monkeypatch, clean_runtime)

    submitted = _submit_record(admission_workflows, "sourceless-allowed")

    assert submitted.get("reason") != "nomos_admin_override_denied", (
        submitted.get("nomos_decision", {}).get("reason_code")
    )
    assert submitted["status"] == "accepted"
    assert submitted["emitted"]["node_facts_inserted"] == 1
    assert submitted["emitted"]["source"] == "agent:review"
    assert _node_fact_sources(
        clean_runtime, "review:sourceless-allowed",
    ) == ["agent:review"]
    assert _envelope_status(
        clean_runtime, submitted["admission_id"],
    ) == "accepted"
    assert _override_decisions(clean_runtime) == [
        (
            "admin.override",
            "allow",
            "allow_runtime_approver_override",
        ),
        (
            "quarantine.release",
            "allow",
            "allow_runtime_approver_override",
        ),
    ]
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            """
            SELECT resource->>'source', resource->>'producer_id',
                   resource->>'producer_kind'
              FROM okg.nomos_decisions
             WHERE deployment_name = %s AND action = 'admin.override'
            """,
            (_DEPLOYMENT,),
        )
        rows = cur.fetchall()
    assert rows == [("agent:review", "agent:review", "agent")]


def test_policy_without_override_grant_still_denies_agent_record(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sending a usable source must not turn a deny into an allow."""
    _activate(
        clean_runtime,
        {
            "rule_id": "allow_someone_else_override",
            "effect": "allow",
            "actions": list(_OVERRIDE_ACTIONS),
            "subjects": ["subject:someone-else"],
        },
        version="no-grant-v1",
    )
    admission_workflows = _enforce(monkeypatch, clean_runtime)

    submitted = _submit_record(admission_workflows, "sourceless-no-grant")

    assert submitted["status"] == "refused"
    assert submitted["reason"] == "nomos_admin_override_denied"
    assert submitted["nomos_decision"]["reason_code"] == "nomos.default_deny"
    assert _envelope_status(
        clean_runtime, submitted["admission_id"],
    ) == "pending"
    assert _node_fact_sources(clean_runtime, "review:sourceless-no-grant") == []


def test_source_scoped_allow_does_not_match_another_source(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An allow rule that names a source does not cover another producer."""
    _activate(
        clean_runtime,
        _allow_override_rule(source="git-files"),
        version="scoped-allow-v1",
    )
    admission_workflows = _enforce(monkeypatch, clean_runtime)

    submitted = _submit_record(admission_workflows, "sourceless-scoped")

    assert submitted["status"] == "refused"
    assert submitted["reason"] == "nomos_admin_override_denied"
    assert submitted["nomos_decision"]["reason_code"] == "nomos.default_deny"
    assert submitted["nomos_decision"]["resource"]["source"] == "agent:review"
    assert _envelope_status(
        clean_runtime, submitted["admission_id"],
    ) == "pending"


def test_source_scoped_deny_sees_the_producer_id_the_accept_writes(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Round-2 review: the policy decides on the source the facts will carry.

    A sourceless agent envelope is written under its producer id, so a deny
    scoped to that id must refuse it and nothing may be written.
    """
    _activate(
        clean_runtime,
        _deny_source_rule("agent:named"),
        _allow_override_rule(),
        version="collision-v1",
    )
    admission_workflows = _enforce(monkeypatch, clean_runtime)

    submitted = _submit_record(
        admission_workflows,
        "collision",
        producer_id="agent:named",
    )

    assert submitted["status"] == "refused"
    assert submitted["reason"] == "nomos_admin_override_denied"
    assert submitted["nomos_decision"]["reason_code"] == "deny_source_override"
    assert submitted["nomos_decision"]["resource"]["source"] == "agent:named"
    assert _envelope_status(
        clean_runtime, submitted["admission_id"],
    ) == "pending"
    assert _node_fact_sources(clean_runtime, "review:collision") == []


_REGISTRY = """sources:
  git-files:
    module: okg.substrate.library.sources.git_graph_history
    class: GitHistorySource
    ownership_id: review-test.git-files-owner
    admission_policy:
      producer_id: policy.git-files
      producer_kind: source
      trust_label: implicit_legacy_trusted
      admission_mode: fast_track
      authority_scope:
        source_family: fam-registry
"""


def _write_deployment(
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
    *,
    registry_ref: str = "source_registry.yaml",
    registry_text: str | None = _REGISTRY,
    extra: str = "",
) -> None:
    """Give review-test a deployment directory under OKG_DEPLOYMENTS_DIR."""
    deployment = tmp_path / _DEPLOYMENT
    deployment.mkdir()
    (deployment / "schemas").mkdir()
    if registry_text is not None:
        (deployment / "source_registry.yaml").write_text(registry_text)
    (deployment / "deployment.yaml").write_text(
        f"name: {_DEPLOYMENT}\n"
        "schema_dir: schemas\n"
        f"source_registry: {registry_ref}\n"
        "modules: []\n"
        + extra
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path))


@pytest.fixture
def registered_git_files(
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Give review-test a deployment directory registering one source."""
    _write_deployment(
        tmp_path,
        monkeypatch,
        extra=(
            "derived_producers:\n"
            "  example:\n"
            "    authority_scope:\n"
            "      source_family: fam-manifest\n"
        ),
    )


def _assert_submit_refused(
    dsn: str,
    admission_workflows: Any,
    key: str,
    *,
    producer_id: str,
    reason: str,
    producer_kind: str = "agent",
) -> None:
    from okg.substrate.runtime.admission_workflows import (
        AdmissionProducerIdentityReserved,
    )

    with pytest.raises(AdmissionProducerIdentityReserved) as refused:
        admission_workflows._run_submit_with_runtime_approver(
            deployment=_DEPLOYMENT,
            payload={
                "nodes": [{
                    "node_id": f"review:{key}",
                    "subtype": "alpha",
                    "attrs": {"text": "approved"},
                }],
            },
            producer_id=producer_id,
            producer_kind=producer_kind,
            intent="record_observation",
            idempotency_key=key,
            authority={"producer_policy": {
                **_review_alpha_policy(producer_id),
                "producer_kind": producer_kind,
            }},
            check_summary={"record": {"kind": "observation"}},
            # No approver: its config schema refuses a blank id before the
            # submit runs, and the refusal under test must not depend on it.
            approver_config=None,
        )
    assert refused.value.code == "admission_producer_identity_reserved"
    assert refused.value.data["reason"] == reason
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.admission_envelopes "
            "WHERE deployment_name = %s AND idempotency_key = %s",
            (_DEPLOYMENT, key),
        )
        (envelopes,) = cur.fetchone()
    assert envelopes == 0
    assert _node_fact_sources(dsn, f"review:{key}") == []


@pytest.mark.parametrize(
    "producer_id",
    [
        "git-files",
        "review-test.git-files",
        "review-test.git-files-owner",
        "policy.git-files",
        "fam-registry",
        "fam-manifest",
    ],
    ids=[
        "name",
        "qualified-name",
        "ownership-id",
        "policy-producer-id",
        "registry-source-family",
        "manifest-source-family",
    ],
)
def test_submit_refuses_producer_id_of_a_registered_source(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    registered_git_files: None,
    producer_id: str,
) -> None:
    """Round-3 review (MEDIUM): an allow scoped to a source must not be
    reachable by an agent that names itself after that source."""
    _activate(
        clean_runtime,
        _allow_override_rule(source=producer_id),
        version="scoped-allow-mirror-v1",
    )
    admission_workflows = _enforce(monkeypatch, clean_runtime)

    _assert_submit_refused(
        clean_runtime,
        admission_workflows,
        f"registered-{producer_id}",
        producer_id=producer_id,
        reason="producer_id_is_registered_source",
    )


def test_submit_refuses_producer_id_of_a_source_the_database_knows(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A source that has run has a watermark even if the manifest is gone."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO okg.source_watermarks (source) VALUES (%s)",
            ("retired-source",),
        )
    admission_workflows = _enforce(monkeypatch, clean_runtime)

    _assert_submit_refused(
        clean_runtime,
        admission_workflows,
        "db-known-source",
        producer_id="retired-source",
        reason="producer_id_is_registered_source",
    )


def test_submit_refuses_producer_id_of_a_source_producer_profile(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A producer profile of kind source also marks the id as a source."""
    from okg.substrate.producer_authority import materialize_producer_profile

    with psycopg.connect(clean_runtime) as conn:
        materialize_producer_profile(
            conn,
            deployment_name=_DEPLOYMENT,
            producer_id="profiled-source",
            producer_kind="source",
            trust_label="implicit_legacy_trusted",
            admission_mode="fast_track",
            authority_scope={},
            allowed_record_kinds=(),
            output_signature={},
            policy_clauses=(),
            output_scope_summary={},
            source_ref={},
            catalog_version_id=None,
        )
        conn.commit()
    admission_workflows = _enforce(monkeypatch, clean_runtime)

    _assert_submit_refused(
        clean_runtime,
        admission_workflows,
        "profiled-source",
        producer_id="profiled-source",
        reason="producer_id_is_registered_source",
    )


@pytest.mark.parametrize(
    "prefix",
    [
        "admission:",
        "poros:",
        "_lens:",
        "_extractor:",
        "_enricher:",
        "_catalog:",
        "catalog:",
        "system:",
        "source:",
        "_publisher_derived",
        "_anything",
        "Admission:",
        "SYSTEM:",
    ],
)
def test_submit_refuses_a_server_producer_prefix(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    prefix: str,
) -> None:
    """Round-3 review (LOW): server-assigned names are not caller-choosable."""
    admission_workflows = _enforce(monkeypatch, clean_runtime)

    _assert_submit_refused(
        clean_runtime,
        admission_workflows,
        f"prefix-{prefix.strip(':_')}",
        producer_id=f"{prefix}{uuid.uuid4()}",
        reason="producer_id_reserved_prefix",
    )


@pytest.mark.parametrize(
    ("producer_id", "reason"),
    [
        ("", "producer_id_blank"),
        ("   ", "producer_id_blank"),
        (" agent:padded", "producer_id_padded"),
    ],
    ids=["empty", "whitespace", "padded"],
)
def test_submit_refuses_blank_or_padded_producer_id(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    producer_id: str,
    reason: str,
) -> None:
    """An empty producer id used to fall through to `admission:<id>`."""
    admission_workflows = _enforce(monkeypatch, clean_runtime)

    _assert_submit_refused(
        clean_runtime,
        admission_workflows,
        f"blank-{reason}",
        producer_id=producer_id,
        reason=reason,
    )


def test_submit_refuses_caller_source_producer_kind(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Round-3 review (MEDIUM): only the ingest service admits sources."""
    admission_workflows = _enforce(monkeypatch, clean_runtime)

    _assert_submit_refused(
        clean_runtime,
        admission_workflows,
        "source-kind",
        producer_id="agent:claims-source",
        producer_kind="source",
        reason="producer_kind_server_only",
    )


def test_gatemem_shaped_agent_accepted_under_deployment_scoped_grant(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """Lane P's GateMem caller: a `subject:` producer id, an empty source
    registry, and a grant to a named approver scoped by deployment and
    scope, with no source-scoped rule. `subject:` names Nomos principals and
    is not a server-assigned producer id, so submit accepts it."""
    deployment = tmp_path / _DEPLOYMENT
    deployment.mkdir()
    (deployment / "schemas").mkdir()
    (deployment / "source_registry.yaml").write_text("{}\n")
    (deployment / "deployment.yaml").write_text(
        f"name: {_DEPLOYMENT}\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "modules: []\n"
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path))
    approver = "runtime.approver.gatemem"
    producer_id = "subject:gatemem:x"
    _activate(
        clean_runtime,
        {
            "rule_id": "allow_runtime_approver_decide",
            "effect": "allow",
            "actions": list(_OVERRIDE_ACTIONS),
            "subjects": [approver],
            "match": {"deployment": _DEPLOYMENT, "scope": _SCOPE},
        },
        version="gatemem-v1",
    )
    admission_workflows = _enforce(monkeypatch, clean_runtime)

    submitted = admission_workflows._run_submit_with_runtime_approver(
        deployment=_DEPLOYMENT,
        payload={
            "nodes": [{
                "node_id": "review:gatemem",
                "subtype": "alpha",
                "attrs": {"text": "approved"},
            }],
        },
        producer_id=producer_id,
        producer_kind="agent",
        intent="record_observation",
        idempotency_key="gatemem",
        authority={"producer_policy": _review_alpha_policy(producer_id)},
        check_summary={"record": {"kind": "observation"}},
        approver_config=_approver_config(producer_id, actor=approver),
    )

    assert submitted["status"] == "accepted", submitted.get("nomos_decision")
    assert _node_fact_sources(clean_runtime, "review:gatemem") == [producer_id]
    assert _override_decisions(clean_runtime) == [
        ("admin.override", "allow", "allow_runtime_approver_decide"),
        ("quarantine.release", "allow", "allow_runtime_approver_decide"),
    ]


def test_legitimate_agent_still_accepted_with_registry_present(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    registered_git_files: None,
) -> None:
    """A non-colliding agent id works end to end under an override grant."""
    _activate(clean_runtime, _allow_override_rule(), version="allow-v1")
    admission_workflows = _enforce(monkeypatch, clean_runtime)

    submitted = _submit_record(
        admission_workflows,
        "legit-agent",
        producer_id="agent:legit",
    )

    assert submitted["status"] == "accepted"
    assert _node_fact_sources(
        clean_runtime, "review:legit-agent",
    ) == ["agent:legit"]


def test_source_scoped_deny_still_allows_an_unrelated_record(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A deny on source X blocks X and not a record written as another source."""
    _activate(
        clean_runtime,
        _deny_source_rule("git-files"),
        _allow_override_rule(),
        version="scoped-deny-v1",
    )
    admission_workflows = _enforce(monkeypatch, clean_runtime)

    allowed = _submit_record(admission_workflows, "not-git-files")

    assert allowed["status"] == "accepted"
    assert _node_fact_sources(
        clean_runtime, "review:not-git-files",
    ) == ["agent:review"]


@pytest.mark.parametrize(
    "intent",
    [
        "source_sync",
        "legacy_connector_ingest",
        "poros.graph.submit",
        "derived_node_emission",
        "derived_edge_emission",
        "snapshot_onboarding",
    ],
)
def test_source_scoped_deny_applies_to_every_source_bound_intent(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    intent: str,
) -> None:
    """A stored source is what both the policy and the writer use."""
    _activate(
        clean_runtime,
        _deny_source_rule("git-files"),
        _allow_override_rule(),
        version="intent-deny-v1",
    )
    admission_workflows = _enforce(monkeypatch, clean_runtime)
    admission_id = _insert_review_envelope(
        clean_runtime,
        producer_id="source:other-producer",
        producer_kind="source",
        intent=intent,
        source="git-files",
        run_id=str(uuid.uuid4()),
        key=f"intent-{intent}",
    )

    decided = _decide_accept(admission_workflows, admission_id)

    assert decided["status"] == "refused"
    assert decided["nomos_decision"]["reason_code"] == "deny_source_override"
    assert decided["nomos_decision"]["resource"]["source"] == "git-files"
    assert _envelope_status(clean_runtime, admission_id) == "pending"


def test_empty_stored_source_is_judged_as_the_producer_id(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stored "" is written as the producer id, so the policy sees that."""
    _activate(
        clean_runtime,
        _deny_source_rule("git-files"),
        _allow_override_rule(),
        version="empty-source-v1",
    )
    admission_workflows = _enforce(monkeypatch, clean_runtime)
    admission_id = _insert_review_envelope(
        clean_runtime,
        producer_id="git-files",
        producer_kind="agent",
        intent="review_proposal",
        source="",
        run_id=None,
        key="empty-stored-source",
    )

    decided = _decide_accept(admission_workflows, admission_id)

    assert decided["status"] == "refused"
    assert decided["nomos_decision"]["reason_code"] == "deny_source_override"
    assert decided["nomos_decision"]["resource"]["source"] == "git-files"


@pytest.mark.parametrize(
    ("producer_id", "intent", "source", "with_run_id"),
    [
        # A source run is applied under its stored source: it must have one.
        ("source:git-files", "source_sync", None, True),
        ("source:git-files", "source_sync", "", True),
        ("source:git-files", "source_sync", "   ", True),
        ("agent:review", "source_sync", None, False),
        # Any blank effective source, on any envelope.
        ("agent:review", "review_proposal", "   ", False),
    ],
    ids=[
        "source-sync-null",
        "source-sync-empty",
        "source-sync-whitespace",
        "agent-source-sync-null",
        "agent-whitespace-source",
    ],
)
def test_unusable_effective_source_is_refused(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    producer_id: str,
    intent: str,
    source: str | None,
    with_run_id: bool,
) -> None:
    """Fail closed: no usable written source means no override."""
    _activate(clean_runtime, _allow_override_rule(), version="allow-v1")
    admission_workflows = _enforce(monkeypatch, clean_runtime)
    admission_id = _insert_review_envelope(
        clean_runtime,
        producer_id=producer_id,
        producer_kind="source" if producer_id.startswith("source:") else "agent",
        intent=intent,
        source=source,
        run_id=str(uuid.uuid4()) if with_run_id else None,
        key=f"unusable-{intent}-{with_run_id}-{source!r}-{producer_id!r}",
    )

    decided = _decide_accept(admission_workflows, admission_id)

    assert decided["status"] == "refused"
    assert decided["reason"] == "nomos_admin_override_denied"
    assert decided["nomos_decision"]["reason_code"] == (
        "nomos.invalid_identity_fact"
    )
    assert _envelope_status(clean_runtime, admission_id) == "pending"


def test_null_source_deny_rule_never_matches(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A `source: null` rule cannot match: the effective source is never null
    for a request that reaches the rules (a null one is refused first)."""
    _activate(
        clean_runtime,
        _deny_source_rule(None),
        _allow_override_rule(),
        version="null-deny-v1",
    )
    admission_workflows = _enforce(monkeypatch, clean_runtime)

    submitted = _submit_record(admission_workflows, "null-rule")

    assert submitted["status"] == "accepted"
    assert _override_decisions(clean_runtime)[0] == (
        "admin.override",
        "allow",
        "allow_runtime_approver_override",
    )


def _submit_pending(admission_workflows: Any, key: str, producer_id: str) -> str:
    """Submit a review envelope with no runtime approver, so it stays pending."""
    submitted = admission_workflows._run_submit_with_runtime_approver(
        deployment=_DEPLOYMENT,
        payload={
            "nodes": [{
                "node_id": f"review:{key}",
                "subtype": "alpha",
                "attrs": {"text": "held"},
            }],
        },
        producer_id=producer_id,
        producer_kind="agent",
        intent="record_observation",
        idempotency_key=key,
        authority={"producer_policy": _review_alpha_policy(producer_id)},
        check_summary={"record": {"kind": "observation"}},
        approver_config=None,
    )
    assert submitted["status"] == "pending"
    return str(submitted["admission_id"])


def _assert_decide_refused(dsn: str, decided: dict, admission_id: str) -> None:
    assert decided["status"] == "refused"
    assert decided["reason"] == "admission_producer_identity_reserved"
    assert decided["error"]["code"] == "admission_producer_identity_reserved"
    assert decided["envelope_status"] == "pending"
    assert _envelope_status(dsn, admission_id) == "pending"
    # The decide refusal speaks to the operator, not the submitter.
    expected_hint = (
        "reject this envelope (okg admission decide --deployment "
        f"{_DEPLOYMENT} {admission_id} --decision reject) and have the "
        "producer resubmit under its own id such as agent:<name>"
    )
    assert decided["fix_hint"] == expected_hint
    assert decided["error"]["fix_hint"] == expected_hint


def test_cli_prints_decide_refusal_reason_and_operator_hint(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Non-JSON `okg admission decide` shows why an accept was refused."""
    from okg.substrate.cli.admission import _print_decide_outcome

    _print_decide_outcome({
        "status": "refused",
        "reason": "admission_producer_identity_reserved",
        "fix_hint": "reject this envelope (okg admission decide ...)",
    })
    _print_decide_outcome({"status": "accepted"})

    assert capsys.readouterr().out.splitlines() == [
        "admission_status: refused",
        "reason: admission_producer_identity_reserved",
        "fix_hint: reject this envelope (okg admission decide ...)",
        "admission_status: accepted",
    ]


def test_decide_refuses_producer_id_that_became_a_source_after_submit(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Round-3 review (MEDIUM): submit is not the only gate.

    `late-src` is not a source at submit. A source of that name then runs
    (its watermark appears). Accepting the held envelope would write its
    fact under the real source, so decide refuses and writes nothing.
    """
    _activate(
        clean_runtime,
        _allow_override_rule(source="late-src"),
        version="late-src-v1",
    )
    admission_workflows = _enforce(monkeypatch, clean_runtime)
    admission_id = _submit_pending(admission_workflows, "late", "late-src")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO okg.source_watermarks (source) VALUES (%s)",
            ("late-src",),
        )

    decided = _decide_accept(admission_workflows, admission_id)

    _assert_decide_refused(clean_runtime, decided, admission_id)
    assert decided["error"]["data"]["reason"] == (
        "producer_id_is_registered_source"
    )
    assert _node_fact_sources(clean_runtime, "review:late") == []
    assert _override_decisions(clean_runtime) == []


@pytest.mark.parametrize(
    ("producer_id", "producer_kind", "reason"),
    [
        ("admission:other-envelope", "agent", "producer_id_reserved_prefix"),
        ("_publisher_derived", "agent", "producer_id_reserved_prefix"),
        ("agent:claims-source", "source", "producer_kind_server_only"),
        ("agent:claims-enricher", "enricher", "producer_kind_server_only"),
        ("   ", "agent", "producer_id_blank"),
    ],
    ids=[
        "admission-prefix",
        "publisher-derived",
        "source-kind",
        "enricher-kind",
        "blank",
    ],
)
def test_decide_refuses_pending_envelope_created_before_the_submit_rules(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    producer_id: str,
    producer_kind: str,
    reason: str,
) -> None:
    """Envelopes written before this change never passed the submit rules."""
    _activate(clean_runtime, _allow_override_rule(), version="allow-v1")
    admission_workflows = _enforce(monkeypatch, clean_runtime)
    admission_id = _insert_review_envelope(
        clean_runtime,
        producer_id=producer_id,
        producer_kind=producer_kind,
        intent="review_proposal",
        source=None,
        run_id=None,
        key=f"pre-change-{reason}-{producer_kind}",
    )

    decided = _decide_accept(admission_workflows, admission_id)

    _assert_decide_refused(clean_runtime, decided, admission_id)
    assert decided["error"]["data"]["reason"] == reason
    assert _override_decisions(clean_runtime) == []


def test_decide_identity_rule_gates_accepts_only(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A reject writes nothing, so the decide-time rule lets it through."""
    admission_workflows = _enforce(monkeypatch, clean_runtime)

    decided = admission_workflows._decide_producer_identity_refusal(
        None,
        deployment=_DEPLOYMENT,
        admission_id="00000000-0000-0000-0000-000000000002",
        status="rejected",
        current={
            "status": "pending",
            "intent": "review_proposal",
            "source": None,
            "producer_id": "admission:other-envelope",
            "producer_kind": "agent",
        },
    )

    assert decided is None


def test_decide_exempts_server_lens_promotion(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Lens promotions are the server's own sourceless review envelopes."""
    admission_workflows = _enforce(monkeypatch, clean_runtime)
    lens = {
        "status": "pending",
        "intent": "promote_lens_overlay",
        "source": None,
        "producer_id": "_lens:overlay-1",
        "producer_kind": "lens",
    }

    exempt = admission_workflows._decide_producer_identity_refusal(
        None,
        deployment=_DEPLOYMENT,
        admission_id="00000000-0000-0000-0000-000000000001",
        status="accepted",
        current=lens,
    )
    forged = admission_workflows._decide_producer_identity_refusal(
        None,
        deployment=_DEPLOYMENT,
        admission_id="00000000-0000-0000-0000-000000000001",
        status="accepted",
        current={**lens, "intent": "review_proposal"},
    )

    assert exempt is None
    assert forged is not None
    assert forged["error"]["data"]["reason"] == "producer_id_reserved_prefix"


def test_submit_resolves_manifest_dir_placeholder_in_registry_path(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """Round-3 review (LOW): `${manifest_dir}` must not hide the registry."""
    _write_deployment(
        tmp_path,
        monkeypatch,
        registry_ref="${manifest_dir}/source_registry.yaml",
    )
    admission_workflows = _enforce(monkeypatch, clean_runtime)

    _assert_submit_refused(
        clean_runtime,
        admission_workflows,
        "placeholder-registry",
        producer_id="git-files",
        reason="producer_id_is_registered_source",
    )


def test_submit_fails_closed_when_declared_registry_is_missing(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """Round-3 review (LOW): a declared registry that does not exist is a
    configuration error, not an empty list of sources."""
    from okg.substrate.errors import SubstrateError

    _write_deployment(
        tmp_path,
        monkeypatch,
        registry_ref="missing_registry.yaml",
        registry_text=None,
    )
    admission_workflows = _enforce(monkeypatch, clean_runtime)

    with pytest.raises(SubstrateError, match="declared source registry"):
        admission_workflows._run_submit_with_runtime_approver(
            deployment=_DEPLOYMENT,
            payload={"nodes": [{
                "node_id": "review:missing-registry",
                "subtype": "alpha",
                "attrs": {"text": "held"},
            }]},
            producer_id="agent:legit",
            producer_kind="agent",
            intent="record_observation",
            idempotency_key="missing-registry",
            authority={"producer_policy": _review_alpha_policy("agent:legit")},
            check_summary={"record": {"kind": "observation"}},
            approver_config=None,
        )
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.admission_envelopes "
            "WHERE deployment_name = %s",
            (_DEPLOYMENT,),
        )
        (envelopes,) = cur.fetchone()
    assert envelopes == 0
