from __future__ import annotations

from dataclasses import replace

import pytest

from okg.substrate.nomos import (
    NomosPolicyError,
    PermissionGrant,
    SubjectBinding,
    compile_policy_graph,
    evaluate_identity_claim,
    institutional_provider_fixture,
    normalize_identity_claim,
)


def test_cern_oidc_role_fixture_maps_to_scoped_nomos_subject() -> None:
    provider = institutional_provider_fixture(
        "cern-oidc",
        audience="akmon-cms",
        provider_id="cern-sso",
    )
    claim = normalize_identity_claim(
        {
            "provider_id": "cern-sso",
            "verification_status": "valid",
            "issuer": provider.issuer,
            "raw_token": "opaque-cern-token",
            "claims": {
                "sub": "cern-user-123",
                "aud": "akmon-cms",
                "cern_roles": ["cms-operator"],
                "acr": "mfa",
                "exp": "2026-05-25T13:00:00Z",
                "auth_time": "2026-05-25T11:59:00Z",
            },
        },
        [provider],
        now="2026-05-25T12:00:00Z",
    )

    result = evaluate_identity_claim(
        compile_policy_graph(_institutional_policy("cern-sso")),
        claim,
        bindings=[
            SubjectBinding(
                binding_id="binding:cern:cms-operator",
                provider_id="cern-sso",
                issuer=provider.issuer,
                claim_path="roles",
                claim_value="cms-operator",
                subject="subject:cms:operator",
                scope="deployment:cms",
                required_assurance=("mfa",),
                priority=10,
            )
        ],
        permission_grants=[_grant()],
        action="poros.graph.submit",
        resource={"deployment": "cms"},
        scope="deployment:cms",
    )

    assert claim.roles == ("cms-operator",)
    assert claim.mfa_satisfied is True
    assert result.resolved_subject == "subject:cms:operator"
    assert result.decision.effect == "allow"
    assert result.decision.context["identity_provider_id"] == "cern-sso"
    assert "opaque-cern-token" not in str(result.claim.to_dict())


def test_cern_saml_roles_use_same_binding_shape() -> None:
    provider = institutional_provider_fixture(
        "cern-saml",
        audience="akmon-cms",
        provider_id="cern-saml",
    )
    claim = normalize_identity_claim(
        {
            "provider_id": "cern-saml",
            "verification_status": "valid",
            "issuer": provider.issuer,
            "raw_assertion": "opaque-saml-assertion",
            "claims": {
                "NameID": "cern-user-123",
                "aud": "akmon-cms",
                "CernRoles": ["cms-operator"],
                "AuthnContextClassRef": ["mfa"],
                "exp": "2026-05-25T13:00:00Z",
                "auth_time": "2026-05-25T11:59:00Z",
            },
        },
        [provider],
        now="2026-05-25T12:00:00Z",
    )

    result = evaluate_identity_claim(
        compile_policy_graph(_institutional_policy("cern-saml")),
        claim,
        bindings=[
            SubjectBinding(
                binding_id="binding:cern-saml:cms-operator",
                provider_id="cern-saml",
                issuer=provider.issuer,
                claim_path="roles",
                claim_value="cms-operator",
                subject="subject:cms:operator",
                scope="deployment:cms",
                required_assurance=("mfa",),
            )
        ],
        permission_grants=[_grant()],
        action="poros.graph.submit",
        resource={"deployment": "cms"},
        scope="deployment:cms",
    )

    assert claim.roles == ("cms-operator",)
    assert result.decision.effect == "allow"
    assert "opaque-saml-assertion" not in str(result.claim.to_dict())


def test_unapproved_cern_group_claim_cannot_grant_authority() -> None:
    provider = institutional_provider_fixture("cern-oidc", audience="akmon-cms")
    claim = normalize_identity_claim(
        {
            "provider_id": provider.provider_id,
            "verification_status": "valid",
            "issuer": provider.issuer,
            "claims": {
                "sub": "cern-user-123",
                "aud": "akmon-cms",
                "groups": ["cms-comp-ops"],
                "acr": "mfa",
                "exp": "2026-05-25T13:00:00Z",
                "auth_time": "2026-05-25T11:59:00Z",
            },
        },
        [provider],
        now="2026-05-25T12:00:00Z",
    )

    result = evaluate_identity_claim(
        compile_policy_graph(_institutional_policy(provider.provider_id)),
        claim,
        bindings=[
            SubjectBinding(
                binding_id="binding:cern:unapproved-group",
                provider_id=provider.provider_id,
                issuer=provider.issuer,
                claim_path="groups",
                claim_value="cms-comp-ops",
                subject="subject:cms:operator",
                scope="deployment:cms",
            )
        ],
        permission_grants=[_grant()],
        action="poros.graph.submit",
        resource={"deployment": "cms"},
        scope="deployment:cms",
    )

    assert claim.groups == ()
    assert claim.verification_error == "group_claims_unapproved"
    assert result.decision.effect == "deny"
    assert result.decision.reason_code == "nomos.identity.group_claims_unapproved"


def test_mit_touchstone_duo_claim_maps_kerberos_username() -> None:
    provider = institutional_provider_fixture(
        "mit-touchstone",
        audience="akmon-mit-lab",
    )
    claim = normalize_identity_claim(
        {
            "provider_id": provider.provider_id,
            "verification_status": "valid",
            "issuer": provider.issuer,
            "claims": {
                "kerberos_username": "jdoe",
                "aud": "akmon-mit-lab",
                "affiliations": ["researcher"],
                "acr": ["duo_mfa"],
                "exp": "2026-05-25T13:00:00Z",
                "auth_time": "2026-05-25T11:59:00Z",
            },
        },
        [provider],
        now="2026-05-25T12:00:00Z",
    )

    result = evaluate_identity_claim(
        compile_policy_graph(_institutional_policy(provider.provider_id)),
        claim,
        bindings=[
            SubjectBinding(
                binding_id="binding:mit:scientist",
                provider_id=provider.provider_id,
                issuer=provider.issuer,
                claim_path="kerberos_username",
                claim_value="jdoe",
                subject="subject:cms:operator",
                scope="deployment:cms",
                required_assurance=("duo_mfa",),
            )
        ],
        permission_grants=[_grant()],
        action="poros.graph.submit",
        resource={"deployment": "cms"},
        scope="deployment:cms",
    )

    assert claim.external_subject == "jdoe"
    assert claim.assurance_values == ("duo_mfa", "mfa")
    assert result.decision.effect == "allow"


def test_production_provider_requires_trusted_verifier_evidence() -> None:
    provider = replace(
        institutional_provider_fixture(
            "cern-oidc",
            audience="akmon-cms",
            provider_id="cern-sso",
            key_reference="secret://cern-sso/prod-jwks",
            metadata_fingerprint="sha256:prod-cern-metadata",
        ),
        metadata={
            "production": True,
            "trusted_verifiers": ["verifier:cern-sso-gateway"],
            "max_verifier_age_seconds": 300,
        },
    )
    base_claim = {
        "provider_id": provider.provider_id,
        "verification_status": "valid",
        "issuer": provider.issuer,
        "claims": {
            "sub": "cern-user-123",
            "aud": "akmon-cms",
            "cern_roles": ["cms-operator"],
            "acr": "mfa",
            "exp": "2026-05-25T13:00:00Z",
            "auth_time": "2026-05-25T11:59:00Z",
        },
    }

    missing = normalize_identity_claim(
        base_claim,
        [provider],
        now="2026-05-25T12:00:00Z",
    )
    untrusted = normalize_identity_claim(
        {
            **base_claim,
            "verified_by": "verifier:unknown",
            "verification_method": "oidc-jwks",
            "verified_at": "2026-05-25T11:59:30Z",
        },
        [provider],
        now="2026-05-25T12:00:00Z",
    )
    stale = normalize_identity_claim(
        {
            **base_claim,
            "verified_by": "verifier:cern-sso-gateway",
            "verification_method": "oidc-jwks",
            "verified_at": "2026-05-25T11:50:00Z",
        },
        [provider],
        now="2026-05-25T12:00:00Z",
    )
    trusted = normalize_identity_claim(
        {
            **base_claim,
            "verified_by": "verifier:cern-sso-gateway",
            "verification_method": "oidc-jwks",
            "verified_at": "2026-05-25T11:59:30Z",
        },
        [provider],
        now="2026-05-25T12:00:00Z",
    )

    assert missing.verification_status == "missing_verifier_evidence"
    assert untrusted.verification_status == "untrusted_verifier"
    assert stale.verification_status == "stale_verifier_evidence"
    assert trusted.verification_status == "valid"

    result = evaluate_identity_claim(
        compile_policy_graph(_institutional_policy(provider.provider_id)),
        trusted,
        bindings=[
            SubjectBinding(
                binding_id="binding:cern:cms-operator",
                provider_id=provider.provider_id,
                issuer=provider.issuer,
                claim_path="roles",
                claim_value="cms-operator",
                subject="subject:cms:operator",
                scope="deployment:cms",
                required_assurance=("mfa",),
            )
        ],
        permission_grants=[_grant()],
        action="poros.graph.submit",
        resource={"deployment": "cms"},
        scope="deployment:cms",
    )

    assert result.decision.effect == "allow"
    assert result.decision.context["identity_verified_by"] == (
        "verifier:cern-sso-gateway"
    )
    assert result.decision.context["identity_verification_method"] == "oidc-jwks"


def test_mit_kerberos_gssapi_fixture_rejects_wrong_realm() -> None:
    provider = institutional_provider_fixture(
        "mit-kerberos-gssapi",
        audience="HTTP/okg-gateway.example.org@ATHENA.MIT.EDU",
    )
    claim = normalize_identity_claim(
        {
            "provider_id": provider.provider_id,
            "verification_status": "valid",
            "issuer": provider.issuer,
            "realm": "EXAMPLE.ORG",
            "claims": {
                "kerberos_username": "jdoe",
                "aud": "HTTP/okg-gateway.example.org@ATHENA.MIT.EDU",
                "assurance": ["kerberos_gssapi"],
                "service_principal": provider.service_principal,
                "auth_time": "2026-05-25T11:59:00Z",
            },
        },
        [provider],
        now="2026-05-25T12:00:00Z",
    )

    result = evaluate_identity_claim(
        compile_policy_graph(_institutional_policy(provider.provider_id)),
        claim,
        bindings=[],
        permission_grants=[],
        action="poros.graph.submit",
        resource={"deployment": "cms"},
        scope="deployment:cms",
    )

    assert claim.verification_status == "realm_mismatch"
    assert result.decision.effect == "deny"


@pytest.mark.parametrize(
    ("raw_updates", "provider_status", "expected_status", "expected_reason"),
    [
        (
            {"claims": {"service_principal": "HTTP/wrong.example.org@ATHENA.MIT.EDU"}},
            "active",
            "service_principal_mismatch",
            "nomos.identity.service_principal_mismatch",
        ),
        (
            {},
            "revoked",
            "provider_revoked",
            "nomos.identity.provider_revoked",
        ),
        (
            {"verification_status": "delegation_not_allowed"},
            "active",
            "delegation_not_allowed",
            "nomos.identity.delegation_not_allowed",
        ),
        (
            {"verification_status": "replay_or_stale_ticket"},
            "active",
            "replay_or_stale_ticket",
            "nomos.identity.replay_or_stale_ticket",
        ),
    ],
)
def test_mit_kerberos_gssapi_failure_modes_close_authority(
    raw_updates: dict,
    provider_status: str,
    expected_status: str,
    expected_reason: str,
) -> None:
    provider = institutional_provider_fixture(
        "mit-kerberos-gssapi",
        audience="HTTP/okg-gateway.example.org@ATHENA.MIT.EDU",
    )
    if provider_status != "active":
        provider = replace(provider, status=provider_status)
    claims = {
        "kerberos_username": "jdoe",
        "aud": "HTTP/okg-gateway.example.org@ATHENA.MIT.EDU",
        "assurance": ["kerberos_gssapi"],
        "service_principal": provider.service_principal,
        "auth_time": "2026-05-25T11:59:00Z",
    }
    claims.update(raw_updates.get("claims", {}))
    claim = normalize_identity_claim(
        {
            "provider_id": provider.provider_id,
            "verification_status": "valid",
            "issuer": provider.issuer,
            "realm": provider.realm,
            "claims": claims,
            **{
                key: value
                for key, value in raw_updates.items()
                if key != "claims"
            },
        },
        [provider],
        now="2026-05-25T12:00:00Z",
    )

    result = evaluate_identity_claim(
        compile_policy_graph(_institutional_policy(provider.provider_id)),
        claim,
        bindings=[],
        permission_grants=[],
        action="poros.graph.submit",
        resource={"deployment": "cms"},
        scope="deployment:cms",
    )

    assert claim.verification_status == expected_status
    assert result.decision.effect == "deny"
    assert result.decision.reason_code == expected_reason


def test_mit_kerberos_gssapi_fixture_requires_keytab_reference() -> None:
    provider = institutional_provider_fixture(
        "mit-kerberos-gssapi",
        audience="HTTP/okg-gateway.example.org@ATHENA.MIT.EDU",
    )

    with pytest.raises(NomosPolicyError, match="keytab secret reference"):
        replace(provider, key_reference=None, secret_references=())


@pytest.mark.parametrize(
    ("claim_updates", "expected_status", "expected_reason"),
    [
        (
            {"claims": {"aud": "wrong-audience"}},
            "audience_mismatch",
            "nomos.identity.audience_mismatch",
        ),
        (
            {"claims": {"acr": "password"}},
            "missing_assurance",
            "nomos.identity.missing_assurance",
        ),
        (
            {"provider": "revoked"},
            "provider_revoked",
            "nomos.identity.provider_revoked",
        ),
        (
            {"provider": "disabled"},
            "provider_disabled",
            "nomos.identity.provider_disabled",
        ),
        (
            {"claims": {"nbf": "2026-05-25T12:30:00Z"}},
            "not_yet_valid",
            "nomos.identity.not_yet_valid",
        ),
        (
            {"claims": {"auth_time": "2026-05-25T10:00:00Z"}},
            "stale_claim",
            "nomos.identity.stale_claim",
        ),
        (
            {"issuer": "https://wrong.example.test/"},
            "issuer_mismatch",
            "nomos.identity.issuer_mismatch",
        ),
    ],
)
def test_institutional_claim_failures_close_authority(
    claim_updates: dict,
    expected_status: str,
    expected_reason: str,
) -> None:
    provider = institutional_provider_fixture(
        "mit-touchstone",
        audience="akmon-mit-lab",
    )
    if claim_updates.get("provider") == "revoked":
        provider = replace(provider, status="revoked")
    if claim_updates.get("provider") == "disabled":
        provider = replace(provider, status="disabled")
    claims = {
        "kerberos_username": "jdoe",
        "aud": "akmon-mit-lab",
        "acr": ["duo_mfa"],
        "exp": "2026-05-25T13:00:00Z",
        "auth_time": "2026-05-25T11:59:00Z",
    }
    claims.update(claim_updates.get("claims", {}))
    claim = normalize_identity_claim(
        {
            "provider_id": provider.provider_id,
            "verification_status": "valid",
            "issuer": claim_updates.get("issuer", provider.issuer),
            "claims": claims,
        },
        [provider],
        now="2026-05-25T12:00:00Z",
    )

    result = evaluate_identity_claim(
        compile_policy_graph(_institutional_policy(provider.provider_id)),
        claim,
        bindings=[],
        permission_grants=[],
        action="poros.graph.submit",
        resource={"deployment": "cms"},
        scope="deployment:cms",
    )

    assert claim.verification_status == expected_status
    assert result.decision.effect == "deny"
    assert result.decision.reason_code == expected_reason


def test_provider_metadata_rejects_raw_credential_material() -> None:
    with pytest.raises(NomosPolicyError, match="raw credential material"):
        replace(
            institutional_provider_fixture("cern-oidc", audience="akmon-cms"),
            metadata={"raw_token": "do-not-store"},
        )


def test_nested_claim_credential_material_is_rejected() -> None:
    provider = institutional_provider_fixture("mit-touchstone", audience="akmon")

    with pytest.raises(NomosPolicyError, match="raw credential material"):
        normalize_identity_claim(
            {
                "provider_id": provider.provider_id,
                "verification_status": "valid",
                "issuer": provider.issuer,
                "claims": {
                    "kerberos_username": "jdoe",
                    "aud": "akmon",
                    "acr": ["duo_mfa"],
                    "raw_token": "do-not-store",
                },
            },
            [provider],
        )


def test_malformed_claims_object_is_rejected() -> None:
    provider = institutional_provider_fixture("mit-touchstone", audience="akmon")

    with pytest.raises(NomosPolicyError, match="claims"):
        normalize_identity_claim(
            {
                "provider_id": provider.provider_id,
                "verification_status": "valid",
                "issuer": provider.issuer,
                "claims": ["not", "an", "object"],
            },
            [provider],
        )


def _grant() -> PermissionGrant:
    return PermissionGrant(
        grant_id="grant:cms:operator-submit",
        subject="subject:cms:operator",
        actions=("poros.graph.submit",),
        scope="deployment:cms",
        classifications=("internal",),
    )


def _institutional_policy(provider_id: str) -> dict:
    return {
        "policy_bundle_id": f"nomos:test:{provider_id}",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_cms_operator_submit",
                "effect": "allow",
                "priority": 100,
                "subjects": ["subject:cms:operator"],
                "actions": ["poros.graph.submit"],
                "match": {
                    "deployment": "cms",
                    "scope": "deployment:cms",
                    "identity_provider_id": provider_id,
                },
                "reason_code": "nomos.test.cms_operator_submit_allowed",
            }
        ],
    }
