from __future__ import annotations

import pytest

from okg.substrate.nomos import (
    DecisionInput,
    IdentityProvider,
    NomosPolicyError,
    PermissionGrant,
    SubjectBinding,
    build_refinement_state,
    build_refinement_state_from_postgres_rows,
    check_noninterference_trace,
    compile_policy_graph,
    evaluate,
    evaluate_identity_claim,
    normalize_identity_claim,
)
from okg.substrate.nomos.demo import demo_policy_graph
from okg.substrate.nomos.refinement import NomosTraceError


def test_compile_policy_graph_is_deterministic() -> None:
    policy = demo_policy_graph()

    first = compile_policy_graph(policy)
    second = compile_policy_graph(policy)

    assert first.policy_fingerprint == second.policy_fingerprint
    assert [rule.rule_id for rule in first.rules] == [
        rule.rule_id for rule in second.rules
    ]


def test_compile_rejects_duplicate_rule_ids() -> None:
    policy = demo_policy_graph()
    policy["rules"].append(dict(policy["rules"][0]))

    with pytest.raises(NomosPolicyError, match="duplicate policy rule"):
        compile_policy_graph(policy)


def test_compile_rejects_conflicting_rules_without_tiebreaker() -> None:
    policy = {
        "policy_bundle_id": "nomos:test:conflict",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_same_shape",
                "effect": "allow",
                "priority": 10,
                "actions": ["graph.read"],
                "subjects": ["subject:a"],
                "match": {"classification": "public"},
            },
            {
                "rule_id": "deny_same_shape",
                "effect": "deny",
                "priority": 10,
                "actions": ["graph.read"],
                "subjects": ["subject:a"],
                "match": {"classification": "public"},
            },
        ],
    }

    with pytest.raises(NomosPolicyError, match="conflicting policy rules"):
        compile_policy_graph(policy)


def test_evaluate_decision_vocabulary_for_demo_policy() -> None:
    bundle = compile_policy_graph(demo_policy_graph())
    inputs = [
        DecisionInput(
            subject="source:demo",
            action="source.emit",
            classification="public",
            resource={"node_subtype": "public_note"},
        ),
        DecisionInput(
            subject="okg:lab-a",
            action="poros.graph.submit",
            classification="public",
            resource={
                "root_pattern": "proposal",
                "graph_profile": "fact_bundle",
                "node_subtypes": ["proposal"],
                "root_subtypes": ["proposal"],
                "requested_incorporation_mode": "fast_track",
            },
        ),
        DecisionInput(
            subject="agent:curator",
            action="graph.export",
            classification="restricted",
            resource={"redaction_evidence": True},
        ),
        DecisionInput(
            subject="agent:curator",
            action="policy.activate",
            classification="internal",
            resource={"self_expansion": True},
        ),
    ]

    effects = [evaluate(bundle, item).effect for item in inputs]

    assert effects == ["allow", "quarantine", "redact", "review"]


def test_evaluator_supports_all_decision_effects() -> None:
    policy = {
        "policy_bundle_id": "nomos:test:all-effects",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": f"rule_{effect}",
                "effect": effect,
                "actions": [f"action.{effect}"],
                "subjects": ["subject:test"],
                "priority": 1,
            }
            for effect in [
                "allow",
                "review",
                "quarantine",
                "redact",
                "declassify",
            ]
        ],
    }
    bundle = compile_policy_graph(policy)

    observed = {
        effect: evaluate(
            bundle,
            DecisionInput(
                subject="subject:test",
                action=f"action.{effect}",
                resource={},
            ),
        ).effect
        for effect in [
            "allow",
            "review",
            "quarantine",
            "redact",
            "declassify",
        ]
    }
    deny = evaluate(
        bundle,
        DecisionInput(
            subject="subject:test",
            action="action.unmatched",
            resource={},
        ),
    )

    assert observed == {
        "allow": "allow",
        "review": "review",
        "quarantine": "quarantine",
        "redact": "redact",
        "declassify": "declassify",
    }
    assert deny.effect == "deny"
    assert deny.input_hash.startswith("sha256:")


def test_missing_bundle_defaults_to_deny() -> None:
    decision = evaluate(
        None,
        DecisionInput(
            subject="source:demo",
            action="source.emit",
            classification="public",
            resource={"node_subtype": "public_note"},
        ),
    )

    assert decision.effect == "deny"
    assert decision.reason_code == "nomos.default_deny.no_active_bundle"


@pytest.mark.parametrize(
    "field",
    [
        "deployment",
        "scope",
        "source",
        "graph_branch",
        "resource_kind",
        "classification",
        "principal_kind",
    ],
)
@pytest.mark.parametrize(
    "invalid_value",
    [["expected"], {"expected": True}, None, ""],
    ids=["list", "mapping", "null", "empty"],
)
def test_invalid_identity_facts_override_allow_default(
    field: str,
    invalid_value: object,
) -> None:
    policy = {
        "policy_bundle_id": "nomos:test:identity-shape",
        "policy_version": "v1",
        "default_effect": "allow",
        "rules": [{
            "rule_id": "allow_exact_identity",
            "effect": "deny",
            "actions": ["source.emit"],
            "subjects": ["source:fixture"],
            "match": {field: "expected"},
        }],
    }
    resource: dict[str, object] = {}
    context: dict[str, object] = {}
    classification: object = "internal"
    if field == "classification":
        classification = invalid_value
    elif field == "principal_kind":
        context[field] = invalid_value
    else:
        resource[field] = invalid_value

    decision = evaluate(
        compile_policy_graph(policy),
        DecisionInput(
            subject="source:fixture",
            action="source.emit",
            resource=resource,
            classification=classification,  # type: ignore[arg-type]
            context=context,
        ),
    )

    assert decision.effect == "deny"
    assert decision.reason_code == "nomos.invalid_identity_fact"


def test_list_match_semantics_remain_available_for_non_identity_collections() -> None:
    bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:list-match",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [{
            "rule_id": "allow_declared_classification_and_sources",
            "effect": "allow",
            "actions": ["graph.read"],
            "subjects": ["subject:reader"],
            "match": {
                "classification": ["internal", "public"],
                "source_ids": ["source:a", "source:b"],
            },
        }],
    })

    decision = evaluate(
        bundle,
        DecisionInput(
            subject="subject:reader",
            action="graph.read",
            resource={"source_ids": ["source:a"]},
            classification="internal",
        ),
    )

    assert decision.effect == "allow"


def test_container_identity_fact_cannot_be_hidden_by_context_precedence() -> None:
    bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:identity-precedence",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [{
            "rule_id": "allow_exact_source",
            "effect": "allow",
            "actions": ["source.emit"],
            "subjects": ["source:fixture"],
            "match": {"source": "fixture"},
        }],
    })

    decision = evaluate(
        bundle,
        DecisionInput(
            subject="source:fixture",
            action="source.emit",
            resource={"source": ["fixture"]},
            context={"source": "fixture"},
        ),
    )

    assert decision.effect == "deny"


def test_graph_profile_hint_cannot_broaden_authority() -> None:
    bundle = compile_policy_graph(demo_policy_graph())
    decision = evaluate(
        bundle,
        DecisionInput(
            subject="okg:lab-a",
            action="poros.graph.submit",
            classification="public",
            resource={
                "root_pattern": "proposal",
                "graph_profile": "fact_bundle",
                "node_subtypes": ["proposal"],
                "root_subtypes": ["proposal"],
                "requested_incorporation_mode": "fast_track",
            },
        ),
    )

    assert decision.effect == "quarantine"
    assert decision.matched_rule_ids == ("quarantine_proposal_shaped_poros",)


def test_refinement_noninterference_passes_and_fails_mutant() -> None:
    bundle = compile_policy_graph(demo_policy_graph())
    decision = evaluate(
        bundle,
        DecisionInput(
            subject="agent:curator",
            action="graph.export",
            classification="restricted",
            resource={"redaction_evidence": True},
        ),
    )
    state = build_refinement_state(
        [decision],
        compiled_bundles=[bundle.policy_fingerprint],
        active_bundle=bundle.policy_fingerprint,
        public_outputs=[decision.decision_id],
    )

    check_noninterference_trace(state)

    mutant = dict(state)
    unsafe = dict(decision.to_dict())
    unsafe["effect"] = "allow"
    mutant["decisions"] = [unsafe]
    with pytest.raises(NomosTraceError, match="unsafe effect"):
        check_noninterference_trace(mutant)


def test_refinement_maps_postgres_policy_state_rows() -> None:
    bundle = compile_policy_graph(demo_policy_graph())
    policy_decision = evaluate(
        bundle,
        DecisionInput(
            subject="agent:curator",
            action="policy.activate",
            classification="internal",
            resource={"policy_bundle_id": bundle.policy_bundle_id},
        ),
    )
    config_decision = evaluate(
        bundle,
        DecisionInput(
            subject="agent:curator",
            action="config.activate",
            classification="internal",
            resource={"config_fingerprint": "sha256:config-demo"},
        ),
    )
    config_fingerprint = "sha256:config-demo"
    state = build_refinement_state_from_postgres_rows({
        "policy_graphs": [
            {"policy_bundle_id": bundle.policy_bundle_id},
        ],
        "config_graphs": [
            {"config_graph_id": "nomos:config:demo"},
        ],
        "policy_bundles": [
            {"policy_fingerprint": bundle.policy_fingerprint},
        ],
        "config_sets": [
            {"config_fingerprint": config_fingerprint},
        ],
        "activations": [
            {
                "scope": "deployment:nomos-demo",
                "policy_fingerprint": bundle.policy_fingerprint,
                "config_fingerprint": config_fingerprint,
                "policy_activation_decision_id": policy_decision.decision_id,
                "config_activation_decision_id": config_decision.decision_id,
            },
        ],
        "subject_bindings": [
            {"binding_id": "binding:nomos-demo:curator"},
        ],
        "permission_grants": [
            {"grant_id": "grant:nomos-demo:policy-activate"},
        ],
        "decisions": [
            policy_decision.to_dict(),
            config_decision.to_dict(),
        ],
        "generation_policy_pins": [
            {
                "generation_id": "demo-generation-1",
                "policy_fingerprint": bundle.policy_fingerprint,
            },
        ],
    })

    assert state["configGraphs"] == ["nomos:config:demo"]
    assert state["compiledConfigSets"] == [config_fingerprint]
    assert state["activeConfig"] == {"deployment:nomos-demo": config_fingerprint}
    assert state["configActivationDecisions"] == {
        "deployment:nomos-demo": config_decision.decision_id,
    }
    assert state["subjectBindings"] == ["binding:nomos-demo:curator"]
    assert state["permissionGrants"] == ["grant:nomos-demo:policy-activate"]
    assert state["postgresPolicyState"] == {
        "deployment:nomos-demo": {
            "activationDecision": config_decision.decision_id,
            "bundle": bundle.policy_fingerprint,
            "configActivationDecision": config_decision.decision_id,
            "config": config_fingerprint,
            "policyActivationDecision": policy_decision.decision_id,
        },
    }


def test_refinement_row_mapper_filters_inactive_iam_relations() -> None:
    state = build_refinement_state_from_postgres_rows({
        "subject_bindings": [
            {"binding_id": "binding:active", "status": "active"},
            {"binding_id": "binding:disabled", "status": "disabled"},
            {"binding_id": "binding:legacy-without-status"},
        ],
        "permission_grants": [
            {"grant_id": "grant:active", "status": "active"},
            {"grant_id": "grant:revoked", "status": "revoked"},
            {"grant_id": "grant:legacy-without-status"},
        ],
    })

    assert state["subjectBindings"] == [
        "binding:active",
        "binding:legacy-without-status",
    ]
    assert state["permissionGrants"] == [
        "grant:active",
        "grant:legacy-without-status",
    ]


@pytest.mark.parametrize(
    "nullable_field",
    ["policy_activation_decision_id", "config_activation_decision_id"],
)
def test_refinement_rejects_nullable_activation_decision_rows(
    nullable_field: str,
) -> None:
    bundle = compile_policy_graph(demo_policy_graph())
    row = {
        "scope": "deployment:nomos-demo",
        "policy_fingerprint": bundle.policy_fingerprint,
        "config_fingerprint": "sha256:config-demo",
        "policy_activation_decision_id": "nomos:decision:policy",
        "config_activation_decision_id": "nomos:decision:config",
    }
    row[nullable_field] = None

    with pytest.raises(NomosTraceError, match=nullable_field):
        build_refinement_state_from_postgres_rows({
            "policy_bundles": [
                {"policy_fingerprint": bundle.policy_fingerprint},
            ],
            "activations": [row],
        })


def test_identity_claim_maps_cern_sso_role_to_nomos_subject() -> None:
    provider = _cern_provider()
    bundle = compile_policy_graph(_identity_policy())
    claim = normalize_identity_claim(
        {
            "provider_id": "cern-sso",
            "verification_status": "valid",
            "issuer": provider.issuer,
            "raw_token": "bearer-secret-material",
            "claims": {
                "sub": "cern-user-123",
                "aud": "akmon-cms",
                "groups": ["cms-comp-ops"],
                "roles": ["reader"],
                "acr": "mfa",
                "exp": "2026-05-25T13:00:00Z",
            },
        },
        [provider],
        now="2026-05-25T12:00:00Z",
    )

    result = evaluate_identity_claim(
        bundle,
        claim,
        bindings=[
            SubjectBinding(
                binding_id="binding:cern:cms-operator",
                provider_id="cern-sso",
                issuer=provider.issuer,
                claim_path="groups",
                claim_value="cms-comp-ops",
                subject="subject:cms:operator",
                scope="deployment:cms",
                priority=10,
            )
        ],
        permission_grants=[
            PermissionGrant(
                grant_id="grant:cms:operator-record",
                subject="subject:cms:operator",
                actions=("mcp.record",),
                scope="deployment:cms",
                classifications=("internal",),
            )
        ],
        action="mcp.record",
        resource={"deployment": "cms", "surface": "mcp"},
        scope="deployment:cms",
        classification="internal",
        enforcement_point="mcp.record",
    )

    assert result.resolved_subject == "subject:cms:operator"
    assert result.decision.effect == "allow"
    assert result.decision.matched_rule_ids == ("allow_cms_operator_record",)
    assert result.decision.context["identity_provider_id"] == "cern-sso"
    assert result.decision.context["identity_claim_fingerprint"].startswith("sha256:")
    assert result.decision.context["subject_binding_ids"] == [
        "binding:cern:cms-operator"
    ]
    assert result.decision.context["permission_grant_ids"] == [
        "grant:cms:operator-record"
    ]
    assert "bearer-secret-material" not in str(claim.to_dict())


def test_external_role_string_under_different_issuer_grants_nothing() -> None:
    provider = IdentityProvider(
        provider_id="mit-sso",
            provider_type="oidc",
            issuer="https://sso.example.org/",
            audiences=("akmon-cms",),
            group_claims_approved=True,
        )
    bundle = compile_policy_graph(_identity_policy())
    claim = normalize_identity_claim(
        {
            "provider_id": "mit-sso",
            "verification_status": "valid",
            "issuer": provider.issuer,
            "claims": {
                "sub": "mit-user-1",
                "aud": "akmon-cms",
                "groups": ["cms-comp-ops"],
                "exp": "2026-05-25T13:00:00Z",
            },
        },
        [provider],
        now="2026-05-25T12:00:00Z",
    )

    result = evaluate_identity_claim(
        bundle,
        claim,
        bindings=[
            SubjectBinding(
                binding_id="binding:cern:cms-operator",
                provider_id="cern-sso",
                issuer="https://auth.cern.ch/auth/realms/cern",
                claim_path="groups",
                claim_value="cms-comp-ops",
                subject="subject:cms:operator",
                scope="deployment:cms",
            )
        ],
        permission_grants=[
            PermissionGrant(
                grant_id="grant:cms:operator-record",
                subject="subject:cms:operator",
                actions=("mcp.record",),
                scope="deployment:cms",
            )
        ],
        action="mcp.record",
        resource={"deployment": "cms", "surface": "mcp"},
        scope="deployment:cms",
    )

    assert result.decision.effect == "deny"
    assert result.decision.reason_code == "nomos.identity.no_subject_binding"
    assert result.resolved_subject is None


def test_expired_identity_claim_fails_closed_without_storing_raw_token() -> None:
    provider = _cern_provider()
    bundle = compile_policy_graph(_identity_policy())
    claim = normalize_identity_claim(
        {
            "provider_id": "cern-sso",
            "verification_status": "valid",
            "issuer": provider.issuer,
            "access_token": "raw-access-token",
            "claims": {
                "sub": "cern-user-123",
                "aud": "akmon-cms",
                "groups": ["cms-comp-ops"],
                "exp": "2026-05-25T11:00:00Z",
            },
        },
        [provider],
        now="2026-05-25T12:00:00Z",
    )

    result = evaluate_identity_claim(
        bundle,
        claim,
        bindings=[],
        permission_grants=[],
        action="mcp.record",
        resource={"deployment": "cms"},
        scope="deployment:cms",
    )

    assert claim.verification_status == "expired"
    assert result.decision.effect == "deny"
    assert result.decision.reason_code == "nomos.identity.expired_claim"
    serialized_claim = str(claim.to_dict())
    assert "raw-access-token" not in serialized_claim
    assert claim.claim_fingerprint.startswith("sha256:")


@pytest.mark.parametrize(
    ("raw_claim", "expected_status", "expected_reason"),
    [
        (
            {
                "provider_id": "cern-sso",
                "verification_status": "valid",
                "issuer": "https://auth.cern.ch/auth/realms/cern",
                "claims": {
                    "sub": "cern-user-123",
                    "aud": "wrong-audience",
                    "groups": ["cms-comp-ops"],
                    "exp": "2026-05-25T13:00:00Z",
                },
            },
            "audience_mismatch",
            "nomos.identity.audience_mismatch",
        ),
        (
            {
                "provider_id": "unknown-sso",
                "verification_status": "valid",
                "issuer": "https://unknown.example/idp",
                "claims": {
                    "sub": "external-user-1",
                    "aud": "akmon-cms",
                    "groups": ["cms-comp-ops"],
                    "exp": "2026-05-25T13:00:00Z",
                },
            },
            "unknown_issuer",
            "nomos.identity.unknown_issuer",
        ),
    ],
)
def test_untrusted_identity_claims_fail_closed(
    raw_claim: dict,
    expected_status: str,
    expected_reason: str,
) -> None:
    provider = _cern_provider()
    bundle = compile_policy_graph(_identity_policy())
    claim = normalize_identity_claim(
        raw_claim,
        [provider],
        now="2026-05-25T12:00:00Z",
    )

    result = evaluate_identity_claim(
        bundle,
        claim,
        bindings=[],
        permission_grants=[],
        action="mcp.record",
        resource={"deployment": "cms"},
        scope="deployment:cms",
    )

    assert claim.verification_status == expected_status
    assert result.decision.effect == "deny"
    assert result.decision.reason_code == expected_reason


def test_ambiguous_identity_subject_binding_fails_closed() -> None:
    provider = _cern_provider()
    bundle = compile_policy_graph(_identity_policy())
    claim = normalize_identity_claim(
        {
            "provider_id": "cern-sso",
            "verification_status": "valid",
            "issuer": provider.issuer,
            "claims": {
                "sub": "cern-user-123",
                "aud": "akmon-cms",
                "groups": ["cms-comp-ops"],
                "exp": "2026-05-25T13:00:00Z",
            },
        },
        [provider],
        now="2026-05-25T12:00:00Z",
    )

    result = evaluate_identity_claim(
        bundle,
        claim,
        bindings=[
            SubjectBinding(
                binding_id="binding:cern:cms-operator",
                provider_id="cern-sso",
                issuer=provider.issuer,
                claim_path="groups",
                claim_value="cms-comp-ops",
                subject="subject:cms:operator",
                scope="deployment:cms",
                priority=10,
            ),
            SubjectBinding(
                binding_id="binding:cern:cms-admin",
                provider_id="cern-sso",
                issuer=provider.issuer,
                claim_path="groups",
                claim_value="cms-comp-ops",
                subject="subject:cms:admin",
                scope="deployment:cms",
                priority=10,
            ),
        ],
        permission_grants=[],
        action="mcp.record",
        resource={"deployment": "cms"},
        scope="deployment:cms",
    )

    assert result.decision.effect == "deny"
    assert result.decision.reason_code == (
        "nomos.identity.ambiguous_subject_binding"
    )
    assert sorted(result.binding_ids) == [
        "binding:cern:cms-admin",
        "binding:cern:cms-operator",
    ]


def test_refinement_maps_identity_provider_and_claim_rows() -> None:
    bundle = compile_policy_graph(_identity_policy())
    claim_fingerprint = "sha256:cern-claim"
    decision = evaluate(
        bundle,
        DecisionInput(
            subject="subject:cms:operator",
            action="mcp.record",
            classification="internal",
            resource={"deployment": "cms"},
            context={
                "identity_provider_id": "cern-sso",
                "identity_claim_fingerprint": claim_fingerprint,
            },
        ),
    )

    state = build_refinement_state_from_postgres_rows({
        "identity_providers": [
            {"provider_id": "cern-sso"},
        ],
        "external_claims": [
            {"claim_fingerprint": claim_fingerprint},
        ],
        "subject_bindings": [
            {"binding_id": "binding:cern:cms-operator"},
        ],
        "permission_grants": [
            {"grant_id": "grant:cms:operator-record"},
        ],
        "decisions": [decision.to_dict()],
    })

    assert state["identityProviders"] == ["cern-sso"]
    assert state["externalClaimEnvelopes"] == [claim_fingerprint]
    assert state["subjectBindings"] == ["binding:cern:cms-operator"]
    assert state["permissionGrants"] == ["grant:cms:operator-record"]


def _cern_provider() -> IdentityProvider:
    return IdentityProvider(
        provider_id="cern-sso",
        provider_type="oidc",
        issuer="https://auth.cern.ch/auth/realms/cern",
        audiences=("akmon-cms",),
        group_claims=("groups",),
        role_claims=("roles",),
        assurance_claim="acr",
        key_reference="secret://cern-sso/jwks",
        group_claims_approved=True,
    )


def _identity_policy() -> dict:
    return {
        "policy_bundle_id": "nomos:test:identity",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_cms_operator_record",
                "effect": "allow",
                "priority": 100,
                "subjects": ["subject:cms:operator"],
                "actions": ["mcp.record"],
                "match": {
                    "deployment": "cms",
                    "scope": "deployment:cms",
                    "identity_provider_id": "cern-sso",
                },
                "reason_code": "nomos.test.cms_operator_record_allowed",
            }
        ],
    }
