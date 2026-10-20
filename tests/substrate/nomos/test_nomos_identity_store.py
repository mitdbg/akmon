from __future__ import annotations

import psycopg
import pytest

from okg.substrate.nomos import (
    IdentityProvider,
    PermissionGrant,
    SubjectBinding,
    build_refinement_state_from_postgres_rows,
    compile_policy_graph,
    evaluate_identity_claim_from_postgres,
    upsert_identity_provider,
    upsert_permission_grant,
    upsert_subject_binding,
)
from okg.substrate.nomos.identity import institutional_provider_fixture


pytestmark = pytest.mark.postgres


def test_postgres_identity_resolution_persists_allow_and_scoped_deny(
    clean_runtime: str,
) -> None:
    bundle = compile_policy_graph(_identity_policy())
    cern_provider = _cern_provider()
    mit_provider = IdentityProvider(
        provider_id="mit-sso",
        provider_type="oidc",
        issuer="https://sso.example.org/",
        audiences=("akmon-cms",),
        key_reference="secret://mit-sso/jwks",
        group_claims_approved=True,
    )

    with psycopg.connect(clean_runtime) as conn:
        upsert_identity_provider(
            conn,
            deployment_name="cms",
            provider=cern_provider,
        )
        upsert_identity_provider(
            conn,
            deployment_name="cms",
            provider=mit_provider,
        )
        upsert_subject_binding(
            conn,
            deployment_name="cms",
            binding=SubjectBinding(
                binding_id="binding:cern:cms-operator",
                provider_id="cern-sso",
                issuer=cern_provider.issuer,
                claim_path="groups",
                claim_value="cms-comp-ops",
                subject="subject:cms:operator",
                scope="deployment:cms",
                priority=10,
            ),
        )
        upsert_permission_grant(
            conn,
            deployment_name="cms",
            grant=PermissionGrant(
                grant_id="grant:cms:operator-record",
                subject="subject:cms:operator",
                actions=("mcp.record",),
                scope="deployment:cms",
                classifications=("internal",),
            ),
            governing_rule_ids=("allow_cms_operator_record",),
        )

        allowed = evaluate_identity_claim_from_postgres(
            conn,
            deployment_name="cms",
            bundle=bundle,
            claim={
                "provider_id": "cern-sso",
                "verification_status": "valid",
                "issuer": cern_provider.issuer,
                "raw_token": "cern-secret-token",
                "claims": {
                    "sub": "cern-user-123",
                    "aud": "akmon-cms",
                    "groups": ["cms-comp-ops"],
                    "exp": "2026-05-25T13:00:00Z",
                },
            },
            action="mcp.record",
            resource={"deployment": "cms", "surface": "mcp"},
            scope="deployment:cms",
            now="2026-05-25T12:00:00Z",
            target={"kind": "mcp_action", "tool": "record"},
        )
        denied = evaluate_identity_claim_from_postgres(
            conn,
            deployment_name="cms",
            bundle=bundle,
            claim={
                "provider_id": "mit-sso",
                "verification_status": "valid",
                "issuer": mit_provider.issuer,
                "raw_token": "mit-secret-token",
                "claims": {
                    "sub": "mit-user-1",
                    "aud": "akmon-cms",
                    "groups": ["cms-comp-ops"],
                    "exp": "2026-05-25T13:00:00Z",
                },
            },
            action="mcp.record",
            resource={"deployment": "cms", "surface": "mcp"},
            scope="deployment:cms",
            now="2026-05-25T12:00:00Z",
            target={"kind": "mcp_action", "tool": "record"},
        )
        conn.commit()

    assert allowed.resolved_subject == "subject:cms:operator"
    assert allowed.decision.effect == "allow"
    assert denied.resolved_subject is None
    assert denied.decision.effect == "deny"
    assert denied.decision.reason_code == "nomos.identity.no_subject_binding"

    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT effect, reason_code
              FROM okg.nomos_decisions
             WHERE deployment_name = 'cms'
             ORDER BY created_at, decision_row_id
            """
        )
        decision_rows = cur.fetchall()
        cur.execute(
            """
            SELECT provider_id, claim_fingerprint, evidence::text
              FROM okg.nomos_external_claims
             WHERE deployment_name = 'cms'
             ORDER BY provider_id
            """
        )
        claim_rows = cur.fetchall()
        cur.execute(
            """
            SELECT provider_id
              FROM okg.v_nomos_identity_providers
             WHERE deployment_name = 'cms'
             ORDER BY provider_id
            """
        )
        provider_rows = [row[0] for row in cur.fetchall()]

    assert decision_rows == [
        ("allow", "nomos.test.cms_operator_record_allowed"),
        ("deny", "nomos.identity.no_subject_binding"),
    ]
    assert provider_rows == ["cern-sso", "mit-sso"]
    assert {row[0] for row in claim_rows} == {"cern-sso", "mit-sso"}
    assert all(row[1].startswith("sha256:") for row in claim_rows)
    evidence_text = "\n".join(row[2] for row in claim_rows)
    assert "cern-secret-token" not in evidence_text
    assert "mit-secret-token" not in evidence_text


def test_postgres_identity_rows_map_to_nomos_refinement_state(
    clean_runtime: str,
) -> None:
    bundle = compile_policy_graph(_identity_policy())
    cern_provider = _cern_provider()

    with psycopg.connect(clean_runtime) as conn:
        upsert_identity_provider(
            conn,
            deployment_name="cms",
            provider=cern_provider,
        )
        upsert_subject_binding(
            conn,
            deployment_name="cms",
            binding=SubjectBinding(
                binding_id="binding:cern:cms-operator",
                provider_id="cern-sso",
                issuer=cern_provider.issuer,
                claim_path="groups",
                claim_value="cms-comp-ops",
                subject="subject:cms:operator",
                scope="deployment:cms",
            ),
        )
        upsert_permission_grant(
            conn,
            deployment_name="cms",
            grant=PermissionGrant(
                grant_id="grant:cms:operator-record",
                subject="subject:cms:operator",
                actions=("mcp.record",),
                scope="deployment:cms",
            ),
        )
        result = evaluate_identity_claim_from_postgres(
            conn,
            deployment_name="cms",
            bundle=bundle,
            claim={
                "provider_id": "cern-sso",
                "verification_status": "valid",
                "issuer": cern_provider.issuer,
                "claims": {
                    "sub": "cern-user-123",
                    "aud": "akmon-cms",
                    "groups": ["cms-comp-ops"],
                    "exp": "2026-05-25T13:00:00Z",
                },
            },
            action="mcp.record",
            resource={"deployment": "cms", "surface": "mcp"},
            scope="deployment:cms",
            now="2026-05-25T12:00:00Z",
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT provider_id
                  FROM okg.nomos_identity_providers
                 WHERE deployment_name = 'cms'
                """
            )
            providers = [{"provider_id": row[0]} for row in cur.fetchall()]
            cur.execute(
                """
                SELECT claim_fingerprint
                  FROM okg.nomos_external_claims
                 WHERE deployment_name = 'cms'
                """
            )
            claims = [{"claim_fingerprint": row[0]} for row in cur.fetchall()]
            cur.execute(
                """
                SELECT binding_id
                  FROM okg.nomos_subject_bindings
                 WHERE deployment_name = 'cms'
                """
            )
            bindings = [{"binding_id": row[0]} for row in cur.fetchall()]
            cur.execute(
                """
                SELECT grant_id
                  FROM okg.nomos_permission_grants
                 WHERE deployment_name = 'cms'
                """
            )
            grants = [{"grant_id": row[0]} for row in cur.fetchall()]
        conn.commit()

    state = build_refinement_state_from_postgres_rows({
        "identity_providers": providers,
        "external_claims": claims,
        "subject_bindings": bindings,
        "permission_grants": grants,
        "decisions": [result.decision.to_dict()],
    })

    assert state["identityProviders"] == ["cern-sso"]
    assert state["externalClaimEnvelopes"] == [
        result.claim.claim_fingerprint,
    ]
    assert state["subjectBindings"] == ["binding:cern:cms-operator"]
    assert state["permissionGrants"] == ["grant:cms:operator-record"]


def test_provider_profiles_store_statuses_and_secret_refs_only(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime) as conn:
        for status in ("active", "disabled", "rotating", "revoked"):
            provider = institutional_provider_fixture(
                "mit-touchstone",
                audience="akmon-mit-lab",
                provider_id=f"mit-touchstone-{status}",
                key_reference=f"secret://mit-touchstone/{status}/jwks",
            )
            upsert_identity_provider(
                conn,
                deployment_name="mit-lab",
                provider=IdentityProvider(
                    **{
                        **provider.to_dict(),
                        "status": status,
                        "metadata": {
                            "expected_status": status,
                            "expected_metadata_fingerprint": None,
                        },
                    },
                ),
            )
        conn.commit()

    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT provider_id, provider_type, status, key_reference,
                   required_assurance, mfa_required, metadata::text
              FROM okg.nomos_identity_providers
             WHERE deployment_name = 'mit-lab'
             ORDER BY provider_id
            """
        )
        rows = cur.fetchall()

    assert {row[2] for row in rows} == {"active", "disabled", "rotating", "revoked"}
    assert {row[1] for row in rows} == {"touchstone_oidc"}
    assert all(row[3].startswith("secret://") for row in rows)
    assert all("duo_mfa" in row[4] for row in rows)
    assert all(row[5] is True for row in rows)
    provider_text = "\n".join(str(row) for row in rows)
    assert "raw_token" not in provider_text
    assert "saml_assertion" not in provider_text
    assert "keytab" not in provider_text


def _cern_provider() -> IdentityProvider:
    return IdentityProvider(
        provider_id="cern-sso",
        provider_type="oidc",
        issuer="https://auth.cern.ch/auth/realms/cern",
        audiences=("akmon-cms",),
        key_reference="secret://cern-sso/jwks",
        group_claims_approved=True,
    )


def _identity_policy() -> dict:
    return {
        "policy_bundle_id": "nomos:test:identity-store",
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
