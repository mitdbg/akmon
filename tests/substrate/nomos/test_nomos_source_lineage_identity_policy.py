from __future__ import annotations

from okg.substrate.nomos import (
    IdentityProvider,
    PermissionGrant,
    SubjectBinding,
    compile_policy_graph,
    evaluate_identity_claim,
    institutional_provider_fixture,
    normalize_identity_claim,
)


NOW = "2026-05-25T12:00:00Z"
DEPLOYMENT = "source-lineage-identity"
SCOPE = f"deployment:{DEPLOYMENT}"


def test_google_workspace_source_id_grant_allows_only_that_source() -> None:
    provider = _google_workspace_provider()
    binding = _binding(
        provider,
        binding_id="binding:google:drive-team",
        claim_path="groups",
        claim_value="drive-team",
        subject="subject:google:drive-reader",
        assurance=("mfa",),
    )
    grant = _grant(
        "grant:google-drive",
        subject=binding.subject,
        resource_match={"source_id": "google_drive"},
    )

    allowed = _evaluate(
        provider,
        _google_workspace_claim(provider),
        binding=binding,
        grants=[grant],
        resource={"source_id": "google_drive"},
    )
    denied = _evaluate(
        provider,
        _google_workspace_claim(provider),
        binding=binding,
        grants=[grant],
        resource={"source_id": "cern:drive"},
    )

    assert allowed.decision.effect == "allow"
    assert denied.decision.effect == "deny"
    assert denied.decision.reason_code == "nomos.identity.no_permission_grant"


def test_cern_oidc_source_family_grant_requires_all_sources_in_family() -> None:
    provider = institutional_provider_fixture(
        "cern-oidc",
        audience="akmon-cms",
        provider_id="cern-sso",
    )
    binding = _binding(
        provider,
        binding_id="binding:cern:cms-operator",
        claim_path="roles",
        claim_value="cms-operator",
        subject="subject:cern:operator",
        assurance=("mfa",),
    )
    grant = _grant(
        "grant:cern-family",
        subject=binding.subject,
        resource_match={"source_family": "cern"},
    )

    cern_only = _evaluate(
        provider,
        _cern_oidc_claim(provider),
        binding=binding,
        grants=[grant],
        resource={"source_ids": ["cern:drive", "cern:indico"]},
    )
    mixed = _evaluate(
        provider,
        _cern_oidc_claim(provider),
        binding=binding,
        grants=[grant],
        resource={"source_ids": ["cern:drive", "google_drive"]},
    )

    assert cern_only.decision.effect == "allow"
    assert mixed.decision.effect == "deny"
    assert mixed.decision.reason_code == "nomos.identity.no_permission_grant"


def test_cern_saml_source_bundle_grant_requires_bundle_source_set() -> None:
    provider = institutional_provider_fixture(
        "cern-saml",
        audience="akmon-cms",
        provider_id="cern-saml",
    )
    binding = _binding(
        provider,
        binding_id="binding:cern-saml:cms-operator",
        claim_path="roles",
        claim_value="cms-operator",
        subject="subject:cern-saml:operator",
        assurance=("mfa",),
    )
    grant = _grant(
        "grant:cms-private-bundle",
        subject=binding.subject,
        resource_match={
            "source_bundle": {
                "bundle_id": "cms-private",
                "source_ids": ["cern:drive", "cern:indico"],
            },
        },
    )

    bundled = _evaluate(
        provider,
        _cern_saml_claim(provider),
        binding=binding,
        grants=[grant],
        resource={
            "source_ids": ["cern:drive", "cern:indico"],
            "source_bundle_ids": ["cms-private"],
        },
    )
    outside_bundle = _evaluate(
        provider,
        _cern_saml_claim(provider),
        binding=binding,
        grants=[grant],
        resource={"source_ids": ["cern:drive", "google_drive"]},
    )

    assert bundled.decision.effect == "allow"
    assert outside_bundle.decision.effect == "deny"
    assert outside_bundle.decision.reason_code == "nomos.identity.no_permission_grant"


def test_mit_touchstone_and_kerberos_claims_grant_mit_source_family() -> None:
    touchstone = institutional_provider_fixture(
        "mit-touchstone",
        audience="akmon-mit-lab",
        provider_id="mit-touchstone",
    )
    kerberos = institutional_provider_fixture(
        "mit-kerberos-gssapi",
        audience="HTTP/okg-gateway.example.org@ATHENA.MIT.EDU",
        provider_id="mit-kerberos",
    )
    grant = _grant(
        "grant:mit-family",
        subject="subject:mit:researcher",
        resource_match={"source_family": "mit"},
    )

    for provider, claim, assurance in (
        (touchstone, _mit_touchstone_claim(touchstone), ("duo_mfa",)),
        (kerberos, _mit_kerberos_claim(kerberos), ("kerberos_gssapi",)),
    ):
        result = _evaluate(
            provider,
            claim,
            binding=_binding(
                provider,
                binding_id=f"binding:{provider.provider_id}:researcher",
                claim_path="kerberos_username",
                claim_value="jdoe",
                subject="subject:mit:researcher",
                assurance=assurance,
            ),
            grants=[grant],
            resource={"source_ids": ["mit:touchstone-drive", "mit:kerberos-home"]},
        )
        assert result.decision.effect == "allow"


def test_local_operator_can_receive_explicit_source_bundle_grant() -> None:
    provider = _local_operator_provider()
    binding = _binding(
        provider,
        binding_id="binding:local:owner",
        claim_path="roles",
        claim_value="owner",
        subject="subject:local:operator",
    )
    grant = _grant(
        "grant:local:workspace-private",
        subject=binding.subject,
        resource_match={
            "source_bundle": {
                "bundle_id": "workspace-private",
                "source_ids": ["public_docs", "google_drive"],
            },
        },
    )

    result = _evaluate(
        provider,
        _local_operator_claim(provider),
        binding=binding,
        grants=[grant],
        resource={"source_ids": ["public_docs", "google_drive"]},
    )

    assert result.decision.effect == "allow"
    assert result.resolved_subject == "subject:local:operator"


def test_wrong_provider_claim_cannot_unlock_source_grant() -> None:
    cern = institutional_provider_fixture(
        "cern-oidc",
        audience="akmon-cms",
        provider_id="cern-sso",
    )
    claim = normalize_identity_claim(
        {
            "provider_id": "google-workspace",
            "verification_status": "valid",
            "issuer": "https://accounts.google.com",
            "claims": {
                "email": "alice@example.com",
                "aud": "akmon-cms",
                "groups": ["drive-team"],
                "acr": "mfa",
            },
        },
        [cern],
        now=NOW,
    )

    result = evaluate_identity_claim(
        _policy(["subject:cern:operator"]),
        claim,
        bindings=[
            _binding(
                cern,
                binding_id="binding:cern:cms-operator",
                claim_path="roles",
                claim_value="cms-operator",
                subject="subject:cern:operator",
                assurance=("mfa",),
            )
        ],
        permission_grants=[
            _grant(
                "grant:cern-family",
                subject="subject:cern:operator",
                resource_match={"source_family": "cern"},
            )
        ],
        action="graph.read",
        resource=_resource({"source_id": "cern:drive"}),
        scope=SCOPE,
        classification="internal",
        now=NOW,
    )

    assert claim.verification_status == "unknown_issuer"
    assert result.decision.effect == "deny"
    assert result.decision.reason_code == "nomos.identity.unknown_issuer"


def test_stale_missing_assurance_or_wrong_realm_claims_deny_source_grants() -> None:
    stale_provider = institutional_provider_fixture(
        "mit-touchstone",
        audience="akmon-mit-lab",
        provider_id="mit-touchstone",
    )
    missing_assurance_provider = _google_workspace_provider()
    wrong_realm_provider = institutional_provider_fixture(
        "mit-kerberos-gssapi",
        audience="HTTP/okg-gateway.example.org@ATHENA.MIT.EDU",
        provider_id="mit-kerberos",
    )
    cases = [
        (
            stale_provider,
            _mit_touchstone_claim(
                stale_provider,
                auth_time="2026-05-25T10:00:00Z",
            ),
            "kerberos_username",
            "jdoe",
            ("duo_mfa",),
            "stale_claim",
            "nomos.identity.stale_claim",
        ),
        (
            missing_assurance_provider,
            _google_workspace_claim(missing_assurance_provider, acr=None),
            "groups",
            "drive-team",
            ("mfa",),
            "missing_assurance",
            "nomos.identity.missing_assurance",
        ),
        (
            wrong_realm_provider,
            _mit_kerberos_claim(wrong_realm_provider, realm="WRONG.EXAMPLE"),
            "kerberos_username",
            "jdoe",
            ("kerberos_gssapi",),
            "realm_mismatch",
            "nomos.identity.realm_mismatch",
        ),
    ]

    for provider, raw_claim, claim_path, claim_value, assurance, status, reason in cases:
        claim = normalize_identity_claim(raw_claim, [provider], now=NOW)
        result = evaluate_identity_claim(
            _policy(["subject:source-reader"]),
            claim,
            bindings=[
                _binding(
                    provider,
                    binding_id=f"binding:{provider.provider_id}:reader",
                    claim_path=claim_path,
                    claim_value=claim_value,
                    subject="subject:source-reader",
                    assurance=assurance,
                )
            ],
            permission_grants=[
                _grant(
                    f"grant:{provider.provider_id}:source",
                    subject="subject:source-reader",
                    resource_match={"source_id": "google_drive"},
                )
            ],
            action="graph.read",
            resource=_resource({"source_id": "google_drive"}),
            scope=SCOPE,
            classification="internal",
            now=NOW,
        )
        assert claim.verification_status == status
        assert result.decision.effect == "deny"
        assert result.decision.reason_code == reason


def _evaluate(
    provider: IdentityProvider,
    raw_claim: dict,
    *,
    binding: SubjectBinding,
    grants: list[PermissionGrant],
    resource: dict,
):
    claim = normalize_identity_claim(raw_claim, [provider], now=NOW)
    return evaluate_identity_claim(
        _policy([binding.subject]),
        claim,
        bindings=[binding],
        permission_grants=grants,
        action="graph.read",
        resource=_resource(resource),
        scope=SCOPE,
        classification="internal",
        now=NOW,
    )


def _policy(subjects: list[str]) -> object:
    return compile_policy_graph({
        "policy_bundle_id": "nomos:test:source-lineage-identity",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_source_read_for_bound_subjects",
                "effect": "allow",
                "priority": 100,
                "subjects": sorted(subjects),
                "actions": ["graph.read"],
                "match": {"deployment": DEPLOYMENT, "scope": SCOPE},
                "obligations": [{"type": "audit"}],
            }
        ],
    })


def _binding(
    provider: IdentityProvider,
    *,
    binding_id: str,
    claim_path: str,
    claim_value: str,
    subject: str,
    assurance: tuple[str, ...] = (),
) -> SubjectBinding:
    return SubjectBinding(
        binding_id=binding_id,
        provider_id=provider.provider_id,
        issuer=provider.issuer,
        claim_path=claim_path,
        claim_value=claim_value,
        subject=subject,
        scope=SCOPE,
        required_assurance=assurance,
        priority=10,
    )


def _grant(
    grant_id: str,
    *,
    subject: str,
    resource_match: dict,
) -> PermissionGrant:
    return PermissionGrant(
        grant_id=grant_id,
        subject=subject,
        actions=("graph.read",),
        scope=SCOPE,
        classifications=("*",),
        resource_match=resource_match,
        priority=10,
    )


def _resource(values: dict) -> dict:
    return {"deployment": DEPLOYMENT, "scope": SCOPE, **values}


def _google_workspace_provider() -> IdentityProvider:
    return IdentityProvider(
        provider_id="google-workspace",
        provider_type="oidc",
        issuer="https://accounts.google.com",
        audiences=("akmon-google",),
        subject_claim="email",
        group_claims=("groups",),
        role_claims=("roles",),
        entitlement_claims=("entitlements",),
        assurance_claim="acr",
        required_assurance=("mfa",),
        mfa_required=True,
        group_claims_approved=True,
        freshness_seconds=3600,
    )


def _local_operator_provider() -> IdentityProvider:
    return IdentityProvider(
        provider_id="local-operator",
        provider_type="agent_local",
        issuer="local://okg",
        audiences=("okg-local",),
        subject_claim="sub",
        group_claims=(),
        role_claims=("roles",),
        entitlement_claims=(),
        assurance_claim=None,
    )


def _google_workspace_claim(
    provider: IdentityProvider,
    *,
    acr: str | None = "mfa",
) -> dict:
    claims = {
        "email": "alice@example.com",
        "aud": provider.audiences[0],
        "groups": ["drive-team"],
        "exp": "2026-05-25T13:00:00Z",
        "auth_time": "2026-05-25T11:59:00Z",
    }
    if acr is not None:
        claims["acr"] = acr
    return {
        "provider_id": provider.provider_id,
        "verification_status": "valid",
        "issuer": provider.issuer,
        "claims": claims,
    }


def _cern_oidc_claim(provider: IdentityProvider) -> dict:
    return {
        "provider_id": provider.provider_id,
        "verification_status": "valid",
        "issuer": provider.issuer,
        "claims": {
            "sub": "cern-user-123",
            "aud": provider.audiences[0],
            "cern_roles": ["cms-operator"],
            "acr": "mfa",
            "exp": "2026-05-25T13:00:00Z",
            "auth_time": "2026-05-25T11:59:00Z",
        },
    }


def _cern_saml_claim(provider: IdentityProvider) -> dict:
    return {
        "provider_id": provider.provider_id,
        "verification_status": "valid",
        "issuer": provider.issuer,
        "claims": {
            "NameID": "cern-user-123",
            "aud": provider.audiences[0],
            "CernRoles": ["cms-operator"],
            "AuthnContextClassRef": ["mfa"],
            "exp": "2026-05-25T13:00:00Z",
            "auth_time": "2026-05-25T11:59:00Z",
        },
    }


def _mit_touchstone_claim(
    provider: IdentityProvider,
    *,
    auth_time: str = "2026-05-25T11:59:00Z",
) -> dict:
    return {
        "provider_id": provider.provider_id,
        "verification_status": "valid",
        "issuer": provider.issuer,
        "claims": {
            "kerberos_username": "jdoe",
            "aud": provider.audiences[0],
            "affiliations": ["researcher"],
            "acr": ["duo_mfa"],
            "exp": "2026-05-25T13:00:00Z",
            "auth_time": auth_time,
        },
    }


def _mit_kerberos_claim(
    provider: IdentityProvider,
    *,
    realm: str | None = None,
) -> dict:
    return {
        "provider_id": provider.provider_id,
        "verification_status": "valid",
        "issuer": provider.issuer,
        "realm": realm or provider.realm,
        "service_principal": provider.service_principal,
        "claims": {
            "kerberos_username": "jdoe",
            "aud": provider.audiences[0],
            "assurance": ["kerberos_gssapi"],
            "exp": "2026-05-25T13:00:00Z",
            "auth_time": "2026-05-25T11:59:00Z",
        },
    }


def _local_operator_claim(provider: IdentityProvider) -> dict:
    return {
        "provider_id": provider.provider_id,
        "verification_status": "valid",
        "issuer": provider.issuer,
        "claims": {
            "sub": "operator:alice",
            "aud": provider.audiences[0],
            "roles": ["owner"],
        },
    }
