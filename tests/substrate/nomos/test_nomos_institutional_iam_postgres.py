from __future__ import annotations

from dataclasses import replace

import psycopg
import pytest

from okg.substrate.nomos import (
    IdentityProvider,
    PermissionGrant,
    SubjectBinding,
    activate_config_set,
    activate_policy_bundle,
    build_refinement_state_from_postgres,
    compile_config_graph,
    compile_policy_graph,
    evaluate_identity_claim_from_postgres,
    institutional_provider_fixture,
    read_nodes_with_identity_claim_from_postgres,
    upsert_identity_provider,
    upsert_permission_grant,
    upsert_subject_binding,
)
from okg.substrate.nomos.doctor import nomos_doctor_report


pytestmark = pytest.mark.postgres


def _chronos_graph_counts(conn) -> tuple[int, int]:
    """Return (graph_nodes, graph_edges) counts from the Chronos graph head.

    The chronos-native cutover dropped okg.nodes_live / okg.edges_live; the
    graph now lives in okg.graph_nodes / okg.graph_edges, read through a
    Chronos checkout (raw counts are reconstructed by the checkout)."""
    from okg.substrate.graph_store import ChronosGraphStore

    graph = ChronosGraphStore.from_connection(
        conn, ensure_schema=False,
    ).checkout()
    nodes = int(graph.query("SELECT count(*) AS n FROM okg.graph_nodes")[0]["n"])
    edges = int(graph.query("SELECT count(*) AS n FROM okg.graph_edges")[0]["n"])
    return nodes, edges


def test_refinement_mapper_consumes_real_institutional_iam_rows(
    clean_runtime: str,
) -> None:
    deployment = "institutional-refinement"
    scope = f"deployment:{deployment}"
    config = compile_config_graph({
        "config_set_id": "nomos:test:institutional-refinement-config",
        "config_version": "v1",
        "entries": [{"config_key": "runtime.max_batch", "value": 100}],
    })
    cern_provider = institutional_provider_fixture(
        "cern-oidc",
        audience="akmon-cms",
        provider_id="cern-sso",
        metadata_fingerprint="sha256:cern-metadata",
    )
    mit_provider = institutional_provider_fixture(
        "mit-touchstone",
        audience="akmon-mit-lab",
        provider_id="mit-touchstone",
        metadata_fingerprint="sha256:mit-metadata",
    )
    bundle = compile_policy_graph(
        _institutional_policy(
            deployment,
            [cern_provider.provider_id, mit_provider.provider_id],
        ),
    )

    with psycopg.connect(clean_runtime) as conn:
        activate_policy_bundle(
            conn,
            deployment_name=deployment,
            bundle=bundle,
            scope=scope,
            activated_by="test:iam-refinement",
        )
        activate_config_set(
            conn,
            deployment_name=deployment,
            config_set=config,
            scope=scope,
            activated_by="test:iam-refinement",
        )
        upsert_identity_provider(
            conn,
            deployment_name=deployment,
            provider=cern_provider,
        )
        upsert_identity_provider(
            conn,
            deployment_name=deployment,
            provider=mit_provider,
        )
        upsert_subject_binding(
            conn,
            deployment_name=deployment,
            binding=_binding(
                provider=cern_provider,
                binding_id="binding:cern:cms-operator",
                claim_path="roles",
                claim_value="cms-operator",
                subject="subject:cms:operator",
                scope=scope,
                assurance=("mfa",),
            ),
        )
        upsert_permission_grant(
            conn,
            deployment_name=deployment,
            grant=_grant(
                grant_id="grant:cern:poros-submit",
                subject="subject:cms:operator",
                actions=("poros.graph.submit",),
                scope=scope,
            ),
        )
        upsert_subject_binding(
            conn,
            deployment_name=deployment,
            binding=_binding(
                provider=cern_provider,
                binding_id="binding:cern:disabled",
                claim_path="roles",
                claim_value="cms-disabled",
                subject="subject:cms:disabled",
                scope=scope,
                assurance=("mfa",),
            ),
            status="disabled",
        )
        upsert_permission_grant(
            conn,
            deployment_name=deployment,
            grant=_grant(
                grant_id="grant:cern:disabled",
                subject="subject:cms:disabled",
                actions=("poros.graph.submit",),
                scope=scope,
            ),
            status="disabled",
        )
        upsert_subject_binding(
            conn,
            deployment_name=deployment,
            binding=_binding(
                provider=mit_provider,
                binding_id="binding:mit:scientist",
                claim_path="kerberos_username",
                claim_value="jdoe",
                subject="subject:mit-touchstone:operator",
                scope=scope,
                assurance=("duo_mfa",),
            ),
        )
        upsert_permission_grant(
            conn,
            deployment_name=deployment,
            grant=_grant(
                grant_id="grant:mit:poros-submit",
                subject="subject:mit-touchstone:operator",
                actions=("poros.graph.submit",),
                scope=scope,
            ),
        )
        cern_result = evaluate_identity_claim_from_postgres(
            conn,
            deployment_name=deployment,
            bundle=bundle,
            claim=_cern_oidc_claim(cern_provider.provider_id, cern_provider.issuer),
            action="poros.graph.submit",
            resource={"deployment": deployment, "scope": scope},
            scope=scope,
            now="2026-05-25T12:00:00Z",
        )
        mit_result = evaluate_identity_claim_from_postgres(
            conn,
            deployment_name=deployment,
            bundle=bundle,
            claim=_mit_touchstone_claim(mit_provider.provider_id, mit_provider.issuer),
            action="poros.graph.submit",
            resource={"deployment": deployment, "scope": scope},
            scope=scope,
            now="2026-05-25T12:00:00Z",
        )
        state = build_refinement_state_from_postgres(
            conn,
            deployment_name=deployment,
        )
        conn.commit()

    assert cern_result.decision.effect == "allow"
    assert mit_result.decision.effect == "allow"
    assert state["identityProviders"] == ["cern-sso", "mit-touchstone"]
    assert state["externalClaimEnvelopes"] == sorted(
        [
            cern_result.claim.claim_fingerprint,
            mit_result.claim.claim_fingerprint,
        ]
    )
    assert state["subjectBindings"] == [
        "binding:cern:cms-operator",
        "binding:mit:scientist",
    ]
    assert state["permissionGrants"] == [
        "grant:cern:poros-submit",
        "grant:mit:poros-submit",
    ]
    assert "binding:cern:disabled" not in state["subjectBindings"]
    assert "grant:cern:disabled" not in state["permissionGrants"]
    assert state["unsupported"] == []
    providers = {
        row["provider_id"]: row
        for row in state["institutionalIam"]["providers"]
    }
    assert providers["cern-sso"]["metadata_fingerprint"] == "sha256:cern-metadata"
    assert providers["mit-touchstone"]["metadata_fingerprint"] == "sha256:mit-metadata"
    claims = {
        row["provider_id"]: row
        for row in state["institutionalIam"]["claims"]
    }
    assert claims["cern-sso"]["verification_status"] == "valid"
    assert claims["cern-sso"]["nomos_decision_id"] == cern_result.decision.decision_id
    assert claims["mit-touchstone"]["nomos_decision_id"] == (
        mit_result.decision.decision_id
    )
    bindings = {
        row["binding_id"]: row
        for row in state["institutionalIam"]["bindings"]
    }
    assert "binding:cern:disabled" not in bindings
    assert bindings["binding:cern:cms-operator"] == {
        "binding_id": "binding:cern:cms-operator",
        "provider_id": "cern-sso",
        "claim_path": "roles",
        "claim_value": "cms-operator",
        "subject": "subject:cms:operator",
        "scope": scope,
        "required_assurance": ["mfa"],
        "status": "active",
    }
    grants = {
        row["grant_id"]: row
        for row in state["institutionalIam"]["grants"]
    }
    assert "grant:cern:disabled" not in grants
    assert grants["grant:cern:poros-submit"] == {
        "grant_id": "grant:cern:poros-submit",
        "subject": "subject:cms:operator",
        "actions": ["poros.graph.submit"],
        "scope": scope,
        "effect": "allow",
        "classifications": ["*"],
        "status": "active",
    }


def test_cern_and_mit_provider_fixtures_persist_decision_evidence(
    clean_runtime: str,
) -> None:
    deployment = "institutional-fixtures"
    scope = f"deployment:{deployment}"
    provider_specs = [
        (
            institutional_provider_fixture(
                "cern-oidc",
                audience="akmon-cms",
                provider_id="cern-sso",
            ),
            _cern_oidc_claim,
            "roles",
            "cms-operator",
            ("mfa",),
        ),
        (
            institutional_provider_fixture(
                "cern-saml",
                audience="akmon-cms",
                provider_id="cern-saml",
            ),
            _cern_saml_claim,
            "roles",
            "cms-operator",
            ("mfa",),
        ),
        (
            institutional_provider_fixture(
                "mit-touchstone",
                audience="akmon-mit-lab",
                provider_id="mit-touchstone",
            ),
            _mit_touchstone_claim,
            "kerberos_username",
            "jdoe",
            ("duo_mfa",),
        ),
        (
            institutional_provider_fixture(
                "mit-kerberos-gssapi",
                audience="HTTP/okg-gateway.example.org@ATHENA.MIT.EDU",
                provider_id="mit-kerberos",
            ),
            _mit_kerberos_claim,
            "kerberos_username",
            "jdoe",
            ("kerberos_gssapi",),
        ),
    ]
    bundle = compile_policy_graph(
        _institutional_policy(
            deployment,
            [provider.provider_id for provider, *_ in provider_specs],
        ),
    )

    with psycopg.connect(clean_runtime) as conn:
        activate_policy_bundle(
            conn,
            deployment_name=deployment,
            bundle=bundle,
            scope=scope,
            activated_by="test:institutional-fixtures",
        )
        for provider, claim_factory, claim_path, claim_value, assurance in provider_specs:
            subject = f"subject:{provider.provider_id}:operator"
            upsert_identity_provider(conn, deployment_name=deployment, provider=provider)
            upsert_subject_binding(
                conn,
                deployment_name=deployment,
                binding=_binding(
                    provider=provider,
                    binding_id=f"binding:{provider.provider_id}",
                    claim_path=claim_path,
                    claim_value=claim_value,
                    subject=subject,
                    scope=scope,
                    assurance=assurance,
                ),
            )
            upsert_permission_grant(
                conn,
                deployment_name=deployment,
                grant=_grant(
                    grant_id=f"grant:{provider.provider_id}:poros",
                    subject=subject,
                    actions=("poros.graph.submit",),
                    scope=scope,
                ),
            )
            result = evaluate_identity_claim_from_postgres(
                conn,
                deployment_name=deployment,
                bundle=bundle,
                claim=claim_factory(provider.provider_id, provider.issuer),
                action="poros.graph.submit",
                resource={"deployment": deployment, "scope": scope},
                scope=scope,
                now="2026-05-25T12:00:00Z",
            )
            assert result.decision.effect == "allow"
            assert result.binding_ids == (f"binding:{provider.provider_id}",)
            assert result.permission_grant_ids == (
                f"grant:{provider.provider_id}:poros",
            )
        conn.commit()

    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT provider_id, claim_fingerprint, evidence::text
              FROM okg.nomos_external_claims
             WHERE deployment_name = %s
             ORDER BY provider_id
            """,
            (deployment,),
        )
        claims = cur.fetchall()
        cur.execute(
            """
            SELECT effect, count(*)
              FROM okg.nomos_decisions
             WHERE deployment_name = %s
               AND context ? 'identity_provider_id'
             GROUP BY effect
            """,
            (deployment,),
        )
        decision_counts = dict(cur.fetchall())

    assert len(claims) == 4
    assert decision_counts == {"allow": 4}
    evidence = "\n".join(row[2] for row in claims)
    for forbidden in [
        "opaque-cern-token",
        "opaque-saml-assertion",
        "kerberos-ticket",
        "keytab",
    ]:
        assert forbidden not in evidence


def test_cern_group_claims_require_approval_and_are_bounded(
    clean_runtime: str,
) -> None:
    deployment = "institutional-cern-groups"
    scope = f"deployment:{deployment}"
    provider = institutional_provider_fixture(
        "cern-oidc",
        audience="akmon-cms",
        provider_id="cern-sso",
    )
    bundle = compile_policy_graph(_institutional_policy(deployment, [provider.provider_id]))
    group_claim = {
        "provider_id": provider.provider_id,
        "verification_status": "valid",
        "issuer": provider.issuer,
        "claims": {
            "sub": "cern-user-123",
            "aud": "akmon-cms",
            "groups": ["atlas-private", "cms-comp-ops"],
            "acr": "mfa",
            "exp": "2026-05-25T13:00:00Z",
            "auth_time": "2026-05-25T11:59:00Z",
        },
    }

    with psycopg.connect(clean_runtime) as conn:
        activate_policy_bundle(
            conn,
            deployment_name=deployment,
            bundle=bundle,
            scope=scope,
            activated_by="test:institutional-cern-groups",
        )
        upsert_identity_provider(conn, deployment_name=deployment, provider=provider)
        upsert_subject_binding(
            conn,
            deployment_name=deployment,
            binding=_binding(
                provider=provider,
                binding_id="binding:cern:cms-group",
                claim_path="groups",
                claim_value="cms-comp-ops",
                subject="subject:cern-sso:operator",
                scope=scope,
                assurance=("mfa",),
            ),
        )
        upsert_permission_grant(
            conn,
            deployment_name=deployment,
            grant=_grant(
                grant_id="grant:cern:cms-group-submit",
                subject="subject:cern-sso:operator",
                actions=("poros.graph.submit",),
                scope=scope,
            ),
        )
        denied = evaluate_identity_claim_from_postgres(
            conn,
            deployment_name=deployment,
            bundle=bundle,
            claim=group_claim,
            action="poros.graph.submit",
            resource={"deployment": deployment, "scope": scope},
            scope=scope,
            now="2026-05-25T12:00:00Z",
        )
        approved_provider = replace(
            provider,
            group_claims_approved=True,
            claim_allowlist=("cms-comp-ops",),
        )
        upsert_identity_provider(
            conn,
            deployment_name=deployment,
            provider=approved_provider,
        )
        allowed = evaluate_identity_claim_from_postgres(
            conn,
            deployment_name=deployment,
            bundle=bundle,
            claim=group_claim,
            action="poros.graph.submit",
            resource={"deployment": deployment, "scope": scope},
            scope=scope,
            now="2026-05-25T12:00:00Z",
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT groups
                  FROM okg.nomos_external_claims
                 WHERE deployment_name = %s
                   AND claim_fingerprint = %s
                """,
                (deployment, allowed.claim.claim_fingerprint),
            )
            persisted_groups = list(cur.fetchone()[0])
        conn.commit()

    assert denied.decision.effect == "deny"
    assert denied.decision.reason_code == "nomos.identity.group_claims_unapproved"
    assert allowed.decision.effect == "allow"
    assert allowed.claim.groups == ("cms-comp-ops",)
    assert persisted_groups == ["cms-comp-ops"]


def test_cern_saml_failure_fixtures_fail_closed_without_graph_mutation(
    clean_runtime: str,
) -> None:
    deployment = "institutional-cern-saml-failures"
    scope = f"deployment:{deployment}"
    provider = institutional_provider_fixture(
        "cern-saml",
        audience="akmon-cms",
        provider_id="cern-saml",
    )
    bundle = compile_policy_graph(_institutional_policy(deployment, [provider.provider_id]))
    base_claim = _cern_saml_claim(provider.provider_id, provider.issuer)
    failure_claims = [
        (
            {**base_claim, "verification_status": "signature_invalid"},
            "nomos.identity.signature_invalid",
        ),
        (
            {**base_claim, "issuer": "https://wrong.example.test/"},
            "nomos.identity.issuer_mismatch",
        ),
        (
            {
                **base_claim,
                "claims": {**base_claim["claims"], "aud": "wrong-audience"},
            },
            "nomos.identity.audience_mismatch",
        ),
        (
            {
                **base_claim,
                "claims": {**base_claim["claims"], "exp": "2026-05-25T11:59:00Z"},
            },
            "nomos.identity.expired_claim",
        ),
        (
            {
                **base_claim,
                "claims": {
                    **base_claim["claims"],
                    "AuthnContextClassRef": ["password"],
                },
            },
            "nomos.identity.missing_assurance",
        ),
    ]

    with psycopg.connect(clean_runtime) as conn:
        activate_policy_bundle(
            conn,
            deployment_name=deployment,
            bundle=bundle,
            scope=scope,
            activated_by="test:institutional-cern-saml-failures",
        )
        upsert_identity_provider(conn, deployment_name=deployment, provider=provider)
        for claim, expected_reason in failure_claims:
            result = evaluate_identity_claim_from_postgres(
                conn,
                deployment_name=deployment,
                bundle=bundle,
                claim=claim,
                action="poros.graph.submit",
                resource={"deployment": deployment, "scope": scope},
                scope=scope,
                now="2026-05-25T12:00:00Z",
            )
            assert result.decision.effect == "deny"
            assert result.decision.reason_code == expected_reason
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM okg.node_facts")
            node_facts = int(cur.fetchone()[0])
            cur.execute("SELECT count(*) FROM okg.edge_facts")
            edge_facts = int(cur.fetchone()[0])
        graph_nodes, graph_edges = _chronos_graph_counts(conn)
        conn.commit()

    # Fail-closed: the denied action mutated neither the audit fact log
    # (node_facts/edge_facts) nor the Chronos graph (graph_nodes/graph_edges).
    assert (node_facts, edge_facts, graph_nodes, graph_edges) == (0, 0, 0, 0)


def test_institutional_failure_fixtures_fail_closed_without_graph_mutation(
    clean_runtime: str,
) -> None:
    deployment = "institutional-failures"
    scope = f"deployment:{deployment}"
    provider = institutional_provider_fixture(
        "mit-touchstone",
        audience="akmon-mit-lab",
    )
    revoked_provider = replace(provider, provider_id="mit-revoked", status="revoked")
    disabled_provider = replace(
        provider,
        provider_id="mit-disabled",
        status="disabled",
    )
    bundle = compile_policy_graph(
        _institutional_policy(
            deployment,
            [
                provider.provider_id,
                revoked_provider.provider_id,
                disabled_provider.provider_id,
            ],
        ),
    )
    failure_claims = [
        (
            provider,
            {
                **_mit_touchstone_claim(provider.provider_id, provider.issuer),
                "claims": {
                    **_mit_touchstone_claim(provider.provider_id, provider.issuer)["claims"],
                    "aud": "wrong-audience",
                },
            },
            "nomos.identity.audience_mismatch",
        ),
        (
            provider,
            {
                **_mit_touchstone_claim(provider.provider_id, provider.issuer),
                "claims": {
                    **_mit_touchstone_claim(provider.provider_id, provider.issuer)["claims"],
                    "acr": "password",
                },
            },
            "nomos.identity.missing_assurance",
        ),
        (
            revoked_provider,
            _mit_touchstone_claim(revoked_provider.provider_id, revoked_provider.issuer),
            "nomos.identity.provider_revoked",
        ),
        (
            disabled_provider,
            _mit_touchstone_claim(
                disabled_provider.provider_id,
                disabled_provider.issuer,
            ),
            "nomos.identity.provider_disabled",
        ),
        (
            provider,
            {
                **_mit_touchstone_claim(provider.provider_id, provider.issuer),
                "issuer": "https://wrong.example.test/",
            },
            "nomos.identity.issuer_mismatch",
        ),
        (
            provider,
            {
                **_mit_touchstone_claim(provider.provider_id, provider.issuer),
                "claims": {
                    **_mit_touchstone_claim(provider.provider_id, provider.issuer)["claims"],
                    "exp": "2026-05-25T11:59:00Z",
                },
            },
            "nomos.identity.expired_claim",
        ),
        (
            provider,
            {
                **_mit_touchstone_claim(provider.provider_id, provider.issuer),
                "claims": {
                    **_mit_touchstone_claim(provider.provider_id, provider.issuer)["claims"],
                    "auth_time": "2026-05-25T10:00:00Z",
                },
            },
            "nomos.identity.stale_claim",
        ),
        (
            provider,
            {
                **_mit_touchstone_claim(provider.provider_id, provider.issuer),
                "claims": {
                    **_mit_touchstone_claim(provider.provider_id, provider.issuer)["claims"],
                    "kerberos_username": "unknown",
                },
            },
            "nomos.identity.no_subject_binding",
        ),
        (
            provider,
            {
                **_mit_touchstone_claim(provider.provider_id, provider.issuer),
                "claims": {
                    **_mit_touchstone_claim(provider.provider_id, provider.issuer)["claims"],
                    "kerberos_username": "ambiguous",
                },
            },
            "nomos.identity.ambiguous_subject_binding",
        ),
    ]

    with psycopg.connect(clean_runtime) as conn:
        activate_policy_bundle(
            conn,
            deployment_name=deployment,
            bundle=bundle,
            scope=scope,
            activated_by="test:institutional-failures",
        )
        for item_provider in (provider, revoked_provider, disabled_provider):
            upsert_identity_provider(
                conn,
                deployment_name=deployment,
                provider=item_provider,
            )
        upsert_subject_binding(
            conn,
            deployment_name=deployment,
            binding=_binding(
                provider=provider,
                binding_id="binding:mit:jdoe",
                claim_path="kerberos_username",
                claim_value="jdoe",
                subject="subject:mit:scientist",
                scope=scope,
                assurance=("duo_mfa",),
            ),
        )
        upsert_permission_grant(
            conn,
            deployment_name=deployment,
            grant=_grant(
                grant_id="grant:mit:jdoe-submit",
                subject="subject:mit:scientist",
                actions=("poros.graph.submit",),
                scope=scope,
            ),
        )
        for index, subject in enumerate(
            ("subject:mit:ambiguous-a", "subject:mit:ambiguous-b"),
            start=1,
        ):
            upsert_subject_binding(
                conn,
                deployment_name=deployment,
                binding=_binding(
                    provider=provider,
                    binding_id=f"binding:mit:ambiguous-{index}",
                    claim_path="kerberos_username",
                    claim_value="ambiguous",
                    subject=subject,
                    scope=scope,
                    assurance=("duo_mfa",),
                ),
            )
        for item_provider, claim, expected_reason in failure_claims:
            result = evaluate_identity_claim_from_postgres(
                conn,
                deployment_name=deployment,
                bundle=bundle,
                claim=claim,
                action="poros.graph.submit",
                resource={"deployment": deployment, "scope": scope},
                scope=scope,
                now="2026-05-25T12:00:00Z",
            )
            assert result.decision.effect == "deny"
            assert result.decision.reason_code == expected_reason
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM okg.node_facts")
            node_facts = int(cur.fetchone()[0])
            cur.execute("SELECT count(*) FROM okg.edge_facts")
            edge_facts = int(cur.fetchone()[0])
        graph_nodes, graph_edges = _chronos_graph_counts(conn)
        conn.commit()

    # Fail-closed: the denied action mutated neither the audit fact log
    # (node_facts/edge_facts) nor the Chronos graph (graph_nodes/graph_edges).
    assert (node_facts, edge_facts, graph_nodes, graph_edges) == (0, 0, 0, 0)


def test_production_verifier_failures_create_no_graph_mutation(
    clean_runtime: str,
) -> None:
    deployment = "institutional-production-verifier"
    scope = f"deployment:{deployment}"
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
    bundle = compile_policy_graph(_institutional_policy(deployment, [provider.provider_id]))
    base_claim = _cern_oidc_claim(provider.provider_id, provider.issuer)
    failure_claims = [
        (base_claim, "nomos.identity.missing_verifier_evidence"),
        (
            {
                **base_claim,
                "verified_by": "verifier:unknown",
                "verification_method": "oidc-jwks",
                "verified_at": "2026-05-25T11:59:30Z",
            },
            "nomos.identity.untrusted_verifier",
        ),
        (
            {
                **base_claim,
                "verified_by": "verifier:cern-sso-gateway",
                "verification_method": "oidc-jwks",
                "verified_at": "2026-05-25T11:50:00Z",
            },
            "nomos.identity.stale_verifier_evidence",
        ),
    ]

    with psycopg.connect(clean_runtime) as conn:
        activate_policy_bundle(
            conn,
            deployment_name=deployment,
            bundle=bundle,
            scope=scope,
            activated_by="test:institutional-production-verifier",
        )
        upsert_identity_provider(conn, deployment_name=deployment, provider=provider)
        upsert_subject_binding(
            conn,
            deployment_name=deployment,
            binding=_binding(
                provider=provider,
                binding_id="binding:cern:operator",
                claim_path="roles",
                claim_value="cms-operator",
                subject="subject:cern-sso:operator",
                scope=scope,
                assurance=("mfa",),
            ),
        )
        upsert_permission_grant(
            conn,
            deployment_name=deployment,
            grant=_grant(
                grant_id="grant:cern:operator-submit",
                subject="subject:cern-sso:operator",
                actions=("poros.graph.submit",),
                scope=scope,
            ),
        )
        for claim, expected_reason in failure_claims:
            result = evaluate_identity_claim_from_postgres(
                conn,
                deployment_name=deployment,
                bundle=bundle,
                claim=claim,
                action="poros.graph.submit",
                resource={"deployment": deployment, "scope": scope},
                scope=scope,
                now="2026-05-25T12:00:00Z",
            )
            assert result.decision.effect == "deny"
            assert result.decision.reason_code == expected_reason
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM okg.node_facts")
            node_facts = int(cur.fetchone()[0])
            cur.execute("SELECT count(*) FROM okg.edge_facts")
            edge_facts = int(cur.fetchone()[0])
        graph_nodes, graph_edges = _chronos_graph_counts(conn)
        conn.commit()

    # Fail-closed: the denied action mutated neither the audit fact log
    # (node_facts/edge_facts) nor the Chronos graph (graph_nodes/graph_edges).
    assert (node_facts, edge_facts, graph_nodes, graph_edges) == (0, 0, 0, 0)


def test_institutional_read_path_maps_mit_claim_to_public_and_audit_views(
    clean_runtime: str,
) -> None:
    deployment = "institutional-read"
    scope = f"deployment:{deployment}"
    provider = institutional_provider_fixture(
        "mit-touchstone",
        audience="akmon-mit-lab",
    )
    bundle = compile_policy_graph(_read_policy(deployment, provider.provider_id))
    with psycopg.connect(clean_runtime) as conn:
        activate_policy_bundle(
            conn,
            deployment_name=deployment,
            bundle=bundle,
            scope=scope,
            activated_by="test:institutional-read",
        )
        upsert_identity_provider(conn, deployment_name=deployment, provider=provider)
        upsert_subject_binding(
            conn,
            deployment_name=deployment,
            binding=_binding(
                provider=provider,
                binding_id="binding:mit:scientist",
                claim_path="kerberos_username",
                claim_value="jdoe",
                subject="subject:mit:scientist",
                scope=scope,
                assurance=("duo_mfa",),
            ),
        )
        upsert_permission_grant(
            conn,
            deployment_name=deployment,
            grant=_grant(
                grant_id="grant:mit:read",
                subject="subject:mit:scientist",
                actions=("graph.read", "graph.read.audit"),
                scope=scope,
            ),
        )
        # The chronos-native cutover dropped okg.nodes_live;
        # read_nodes_with_identity_claim_from_postgres reads okg.graph_nodes
        # through a checkpoint-pinned Chronos checkout. Seed via apply_facts
        # and checkpoint the branch head as the generation we read at.
        from okg.substrate.graph_store import ChronosGraphStore
        from okg.substrate.library.sources.base import NodeFact

        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    "node:public", "document",
                    {"classification": "public", "text": "hello"},
                    {"id": "node:public"}, {"v": 1},
                ),
                NodeFact(
                    "node:restricted", "document",
                    {
                        "classification": "restricted",
                        "text": "secret",
                        "private_body": "hidden",
                        "title": "visible",
                    },
                    {"id": "node:restricted"}, {"v": 1},
                ),
            ],
            source="institutional-read",
            strict_endpoints=True,
        )
        gen = store.checkpoint_generation(0, deployment_name=deployment)
        public_rows = read_nodes_with_identity_claim_from_postgres(
            conn,
            deployment_name=deployment,
            scope=scope,
            claim=_mit_touchstone_claim(provider.provider_id, provider.issuer),
            generation_id=gen,
            now="2026-05-25T12:00:00Z",
        )
        audit_rows = read_nodes_with_identity_claim_from_postgres(
            conn,
            deployment_name=deployment,
            scope=scope,
            claim=_mit_touchstone_claim(provider.provider_id, provider.issuer),
            generation_id=gen,
            audit=True,
            now="2026-05-25T12:00:00Z",
        )
        conn.commit()

    public_by_id = {row["node_id"]: row for row in public_rows}
    audit_by_id = {row["node_id"]: row for row in audit_rows}
    assert public_by_id["node:public"]["attrs"]["text"] == "hello"
    assert public_by_id["node:restricted"]["attrs"]["title"] == "visible"
    assert public_by_id["node:restricted"]["attrs"]["nomos_redacted"] is True
    assert "private_body" not in public_by_id["node:restricted"]["attrs"]
    assert audit_by_id["node:restricted"]["attrs"]["private_body"] == "hidden"


def test_agent_action_is_gated_through_each_provider_family(
    clean_runtime: str,
) -> None:
    deployment = "institutional-agent"
    scope = f"deployment:{deployment}"
    providers = [
        institutional_provider_fixture(
            "cern-oidc",
            audience="akmon-cms",
            provider_id="cern-sso",
        ),
        institutional_provider_fixture(
            "cern-saml",
            audience="akmon-cms",
            provider_id="cern-saml",
        ),
        institutional_provider_fixture(
            "mit-touchstone",
            audience="akmon-mit-lab",
            provider_id="mit-touchstone",
        ),
        institutional_provider_fixture(
            "mit-kerberos-gssapi",
            audience="HTTP/okg-gateway.example.org@ATHENA.MIT.EDU",
            provider_id="mit-kerberos",
        ),
    ]
    claim_factories = {
        "cern-sso": _cern_oidc_claim,
        "cern-saml": _cern_saml_claim,
        "mit-touchstone": _mit_touchstone_claim,
        "mit-kerberos": _mit_kerberos_claim,
    }
    bundle = compile_policy_graph(
        _agent_policy(deployment, [provider.provider_id for provider in providers]),
    )
    with psycopg.connect(clean_runtime) as conn:
        activate_policy_bundle(
            conn,
            deployment_name=deployment,
            bundle=bundle,
            scope=scope,
            activated_by="test:institutional-agent",
        )
        for provider in providers:
            upsert_identity_provider(conn, deployment_name=deployment, provider=provider)
            subject = f"subject:{provider.provider_id}:agent"
            claim_path = (
                "roles"
                if provider.provider_id.startswith("cern")
                else "kerberos_username"
            )
            claim_value = "cms-operator" if provider.provider_id.startswith("cern") else "jdoe"
            assurance = (
                ("mfa",)
                if provider.provider_id.startswith("cern")
                else tuple(provider.required_assurance)
            )
            upsert_subject_binding(
                conn,
                deployment_name=deployment,
                binding=_binding(
                    provider=provider,
                    binding_id=f"binding:{provider.provider_id}:agent",
                    claim_path=claim_path,
                    claim_value=claim_value,
                    subject=subject,
                    scope=scope,
                    assurance=assurance,
                ),
            )
            upsert_permission_grant(
                conn,
                deployment_name=deployment,
                grant=_grant(
                    grant_id=f"grant:{provider.provider_id}:agent",
                    subject=subject,
                    actions=("agent.invoke",),
                    scope=scope,
                ),
            )
            allowed = evaluate_identity_claim_from_postgres(
                conn,
                deployment_name=deployment,
                bundle=bundle,
                claim=claim_factories[provider.provider_id](
                    provider.provider_id,
                    provider.issuer,
                ),
                action="agent.invoke",
                resource={"deployment": deployment, "agent": "curator"},
                scope=scope,
                now="2026-05-25T12:00:00Z",
            )
            denied = evaluate_identity_claim_from_postgres(
                conn,
                deployment_name=deployment,
                bundle=bundle,
                claim={
                    **claim_factories[provider.provider_id](
                        provider.provider_id,
                        provider.issuer,
                    ),
                    "claims": {"aud": "wrong-audience"},
                },
                action="agent.invoke",
                resource={"deployment": deployment, "agent": "curator"},
                scope=scope,
                now="2026-05-25T12:00:00Z",
            )
            assert allowed.decision.effect == "allow"
            assert denied.decision.effect == "deny"
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM okg.node_facts")
            node_facts = int(cur.fetchone()[0])
        conn.commit()

    assert node_facts == 0


def test_institutional_provider_drift_checks_cover_cern_and_mit(
    clean_runtime: str,
) -> None:
    deployment = "institutional-drift"
    cern = replace(
        institutional_provider_fixture("cern-oidc", audience="akmon-cms"),
        issuer="https://wrong.example.test/",
        metadata={
            "expected_issuer": "https://auth.cern.ch/auth/realms/cern",
            "expected_freshness_seconds": 600,
            "expected_metadata_fingerprint": "sha256:cern-expected",
            "expected_required_assurance": ["mfa"],
        },
        metadata_fingerprint="sha256:cern-actual",
    )
    mit = replace(
        institutional_provider_fixture(
            "mit-kerberos-gssapi",
            audience="HTTP/okg-gateway.example.org@ATHENA.MIT.EDU",
        ),
        service_principal="HTTP/wrong.example.org@ATHENA.MIT.EDU",
        metadata={
            "expected_service_principal": (
                "HTTP/okg-gateway.example.org@ATHENA.MIT.EDU"
            ),
        },
    )
    disabled = replace(
        institutional_provider_fixture(
            "mit-touchstone",
            audience="akmon-mit-lab",
            provider_id="mit-touchstone-disabled",
        ),
        status="disabled",
    )

    with psycopg.connect(clean_runtime) as conn:
        upsert_identity_provider(conn, deployment_name=deployment, provider=cern)
        upsert_identity_provider(conn, deployment_name=deployment, provider=mit)
        upsert_identity_provider(conn, deployment_name=deployment, provider=disabled)
        report = nomos_doctor_report(conn, deployment_name=deployment)

    codes = {error["code"] for error in report["errors"]}
    fields = {
        error.get("field")
        for error in report["errors"]
        if error["code"] == "nomos_identity_provider_drift"
    }
    assert "nomos_identity_provider_disabled" in codes
    assert {
        "issuer",
        "freshness_seconds",
        "metadata_fingerprint",
        "service_principal",
    }.issubset(fields)


def _binding(
    *,
    provider: IdentityProvider,
    binding_id: str,
    claim_path: str,
    claim_value: str,
    subject: str,
    scope: str,
    assurance: tuple[str, ...],
) -> SubjectBinding:
    return SubjectBinding(
        binding_id=binding_id,
        provider_id=provider.provider_id,
        issuer=provider.issuer,
        claim_path=claim_path,
        claim_value=claim_value,
        subject=subject,
        scope=scope,
        required_assurance=assurance,
        priority=10,
    )


def _grant(
    *,
    grant_id: str,
    subject: str,
    actions: tuple[str, ...],
    scope: str,
) -> PermissionGrant:
    return PermissionGrant(
        grant_id=grant_id,
        subject=subject,
        actions=actions,
        scope=scope,
        classifications=("*",),
    )


def _cern_oidc_claim(provider_id: str, issuer: str) -> dict:
    return {
        "provider_id": provider_id,
        "verification_status": "valid",
        "issuer": issuer,
        "raw_token": "opaque-cern-token",
        "claims": {
            "sub": "cern-user-123",
            "aud": "akmon-cms",
            "cern_roles": ["cms-operator"],
            "acr": "mfa",
            "exp": "2026-05-25T13:00:00Z",
            "auth_time": "2026-05-25T11:59:00Z",
        },
    }


def _cern_saml_claim(provider_id: str, issuer: str) -> dict:
    return {
        "provider_id": provider_id,
        "verification_status": "valid",
        "issuer": issuer,
        "raw_assertion": "opaque-saml-assertion",
        "claims": {
            "NameID": "cern-user-123",
            "aud": "akmon-cms",
            "CernRoles": ["cms-operator"],
            "AuthnContextClassRef": ["mfa"],
            "exp": "2026-05-25T13:00:00Z",
            "auth_time": "2026-05-25T11:59:00Z",
        },
    }


def _mit_touchstone_claim(provider_id: str, issuer: str) -> dict:
    return {
        "provider_id": provider_id,
        "verification_status": "valid",
        "issuer": issuer,
        "claims": {
            "kerberos_username": "jdoe",
            "aud": "akmon-mit-lab",
            "affiliations": ["researcher"],
            "acr": ["duo_mfa"],
            "exp": "2026-05-25T13:00:00Z",
            "auth_time": "2026-05-25T11:59:00Z",
        },
    }


def _mit_kerberos_claim(provider_id: str, issuer: str) -> dict:
    return {
        "provider_id": provider_id,
        "verification_status": "valid",
        "issuer": issuer,
        "realm": "ATHENA.MIT.EDU",
        "ticket": "kerberos-ticket",
        "claims": {
            "kerberos_username": "jdoe",
            "aud": "HTTP/okg-gateway.example.org@ATHENA.MIT.EDU",
            "assurance": ["kerberos_gssapi"],
            "service_principal": "HTTP/okg-gateway.example.org@ATHENA.MIT.EDU",
            "auth_time": "2026-05-25T11:59:00Z",
        },
    }


def _institutional_policy(deployment: str, provider_ids: list[str]) -> dict:
    rules = []
    for provider_id in provider_ids:
        rules.append({
            "rule_id": f"allow_{provider_id.replace('-', '_')}_poros_submit",
            "effect": "allow",
            "priority": 100,
            "subjects": [
                f"subject:{provider_id}:operator",
                "subject:cms:operator",
            ],
            "actions": ["poros.graph.submit"],
            "match": {
                "deployment": deployment,
                "scope": f"deployment:{deployment}",
                "identity_provider_id": provider_id,
            },
            "reason_code": "nomos.test.institutional_poros_submit_allowed",
        })
    return {
        "policy_bundle_id": f"nomos:test:{deployment}",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": rules,
    }


def _read_policy(deployment: str, provider_id: str) -> dict:
    return {
        "policy_bundle_id": f"nomos:test:{deployment}",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_mit_identity_read_gate",
                "effect": "allow",
                "priority": 300,
                "subjects": ["subject:mit:scientist"],
                "actions": ["graph.read", "graph.read.audit"],
                "match": {
                    "deployment": deployment,
                    "scope": f"deployment:{deployment}",
                    "identity_provider_id": provider_id,
                },
            },
            {
                "rule_id": "allow_public_read",
                "effect": "allow",
                "priority": 200,
                "subjects": ["subject:mit:scientist"],
                "actions": ["graph.read"],
                "match": {"classification": "public"},
            },
            {
                "rule_id": "redact_restricted_public_read",
                "effect": "redact",
                "priority": 150,
                "subjects": ["subject:mit:scientist"],
                "actions": ["graph.read"],
                "match": {"classification": "restricted"},
                "obligations": [{"type": "redaction"}],
            },
            {
                "rule_id": "allow_restricted_audit_read",
                "effect": "allow",
                "priority": 150,
                "subjects": ["subject:mit:scientist"],
                "actions": ["graph.read.audit"],
                "match": {"classification": "restricted"},
            },
        ],
    }


def _agent_policy(deployment: str, provider_ids: list[str]) -> dict:
    return {
        "policy_bundle_id": f"nomos:test:{deployment}",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": f"allow_{provider_id.replace('-', '_')}_agent",
                "effect": "allow",
                "priority": 100,
                "subjects": [f"subject:{provider_id}:agent"],
                "actions": ["agent.invoke"],
                "match": {
                    "deployment": deployment,
                    "identity_provider_id": provider_id,
                },
            }
            for provider_id in provider_ids
        ],
    }
