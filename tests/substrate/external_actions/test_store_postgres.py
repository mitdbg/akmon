"""PostgreSQL proof for the external-action review storage boundary.

The service reference store is intentionally in memory.  This test exercises
the durable boundary that production uses: append-only rows, a login binding,
and the redacted deployment-scoped API views under the NOLOGIN API role.
"""
from __future__ import annotations

import json
import hashlib
import hmac
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4
from collections.abc import Mapping

import psycopg
import pytest
import yaml
from okg.substrate.external_actions.signing import canonical_json_bytes, domain_digest
from okg.substrate.external_actions.action_contracts import (
    action_contract_digest,
    decision_binding_hash,
    effect_instance_key,
    execution_binding_hash,
    submission_content_hash,
)
from okg.substrate.external_actions.store import (
    ExternalActionStoreError,
    PostgresExternalActionStore,
)
from okg.substrate.external_actions.restore_fence import (
    EXTERNAL_ACTION_ROLLOUT_BLOCKED,
    EXTERNAL_ACTION_ROLLOUT_ENABLED,
    external_action_rollout_state,
)
from psycopg.conninfo import make_conninfo
from psycopg import sql
from okg.substrate.db.connection import connect as db_connect


pytestmark = pytest.mark.postgres


# The verifier capability is deliberately test-only material.  Production
# keeps only its digest and generation in the PostgreSQL login binding; the
# clear 256-bit value is supplied transaction-locally by the trusted verifier.
_TEST_VERIFIER_CAPABILITY = "0123456789abcdef" * 4
_TEST_VERIFIER_CAPABILITY_DIGEST = "sha256:" + hashlib.sha256(
    bytes.fromhex(_TEST_VERIFIER_CAPABILITY)
).hexdigest()
_TEST_VERIFIER_CAPABILITY_GENERATION = 1
_VERIFIER_CREDENTIAL_FIELDS = (
    "principal", "separation_subject", "nomos_provider_id",
    "nomos_subject_binding_id", "authentication_method_code",
    "issuer_or_trust_domain_digest", "sender_key_or_certificate_thumbprint",
    "token_or_certificate_id_digest", "dpop_jti_digest", "auth_config_digest",
    "result_code", "credential_expires_at", "provider_status", "binding_status",
    "provider_updated_at", "binding_updated_at",
    "provider_credential_generation_digest",
    "binding_credential_generation_digest", "credential_generation_digest",
    "presented_credential_generation_digest", "token_issued_at",
    "certificate_thumbprint", "certificate_not_before", "certificate_not_after",
)


def _bind_verifier_role(
    conn: psycopg.Connection[Any], deployment: str, *, session_user: str | None = None
) -> None:
    """Install the test capability digest/generation in a login binding.

    ``session_user`` defaults to the login role ``conn`` is actually running
    as, read from the server rather than assumed. The binding is the only
    bridge from a login role to a deployment, so a hard-coded ``postgres``
    binds a role the session is not, and every later call into
    ``external_action_session_deployment()`` refuses with "no unique active
    deployment binding" on any cluster whose administrative login is named
    something else (issue #1973).
    """

    if session_user is None:
        session_user = conn.execute("SELECT session_user").fetchone()[0]

    # The trigger deliberately makes deployment identity immutable.  Most
    # fixtures use the administrative session user but each fixture owns a
    # fresh deployment, so remove only that fixture-local predecessor before
    # installing the new binding.
    existing = conn.execute(
        "SELECT deployment_name FROM okg.external_action_api_role_bindings "
        "WHERE \"session_user\" = %s",
        (session_user,),
    ).fetchone()
    if existing is not None and existing[0] != deployment:
        conn.execute(
            "DELETE FROM okg.external_action_api_role_bindings "
            "WHERE \"session_user\" = %s",
            (session_user,),
        )
    conn.execute(
        """
        INSERT INTO okg.external_action_api_role_bindings
          ("session_user", deployment_name, status,
           verifier_capability_digest, verifier_capability_generation)
        VALUES (%s, %s, 'active', %s, %s)
        ON CONFLICT ("session_user") DO UPDATE
          SET deployment_name = EXCLUDED.deployment_name,
              status = EXCLUDED.status,
              verifier_capability_digest = EXCLUDED.verifier_capability_digest,
              verifier_capability_generation = EXCLUDED.verifier_capability_generation,
              updated_at = now()
        """,
        (
            session_user,
            deployment,
            _TEST_VERIFIER_CAPABILITY_DIGEST,
            _TEST_VERIFIER_CAPABILITY_GENERATION,
        ),
    )


def _set_verifier_capability(
    conn: psycopg.Connection[Any], capability: str = _TEST_VERIFIER_CAPABILITY
) -> None:
    """Set the verifier secret transaction-locally for a raw RPC fixture."""

    conn.execute(
        "SELECT set_config('okg.external_action_verifier_capability', %s, true)",
        (capability,),
    )


def _verifier_credential_digest(audit: Mapping[str, object]) -> str:
    return domain_digest(
        "okg-external-action-credential-evidence-v1",
        {field: audit.get(field) for field in _VERIFIER_CREDENTIAL_FIELDS},
    )


def _verifier_assertion(
    audit: Mapping[str, object],
    *,
    operation: str,
    transition_digest: str,
    nomos_decision_id: str,
    policy_fingerprint: str,
    provider_id: str,
    provider_status: str,
    provider_updated_at: str,
    binding_id: str,
    binding_status: str,
    binding_updated_at: str,
    nomos_input_hash: str,
    authentication_audit_id: str | None = None,
    capability_generation: int = _TEST_VERIFIER_CAPABILITY_GENERATION,
) -> tuple[dict[str, object], str]:
    """Build the closed assertion and HMAC tag accepted by the SQL verifier."""

    issued = datetime.now(UTC) - timedelta(seconds=1)
    expires = issued + timedelta(seconds=20)
    assertion: dict[str, object] = {
        "schema": "okg.external_action.verifier_assertion/v1",
        "deployment": str(audit["deployment"]),
        "capability_generation": capability_generation,
        "authentication_audit_id": authentication_audit_id
        or str(audit.get("authentication_audit_id")),
        "operation": operation,
        "transition_digest": transition_digest,
        "credential_evidence_digest": _verifier_credential_digest(audit),
        "nonce_digest": domain_digest(
            "okg-test-verifier-assertion-nonce-v1", uuid4().hex
        ),
        "issued_at": issued.isoformat(timespec="microseconds").replace("+00:00", "Z"),
        "expires_at": expires.isoformat(timespec="microseconds").replace("+00:00", "Z"),
        "nomos_decision_id": nomos_decision_id,
        "policy_fingerprint": policy_fingerprint,
        "provider_id": provider_id,
        "provider_status": provider_status,
        "provider_updated_at": provider_updated_at,
        "binding_id": binding_id,
        "binding_status": binding_status,
        "binding_updated_at": binding_updated_at,
        "nomos_input_hash": nomos_input_hash,
        "allowed": True,
        "final_effect": "allow",
        "required_obligations_digest": domain_digest(
            "okg-external-action-required-obligations-v1", []
        ),
        "completed_obligations_digest": domain_digest(
            "okg-external-action-completed-obligations-v1", []
        ),
        "missing_obligations_digest": domain_digest(
            "okg-external-action-missing-obligations-v1", []
        ),
    }
    tag = _verifier_tag(assertion)
    return assertion, tag


def _verifier_tag(
    assertion: Mapping[str, object], capability: str = _TEST_VERIFIER_CAPABILITY
) -> str:
    message = (
        b"okg-external-action-verifier-assertion-v1\x00"
        + canonical_json_bytes(assertion)
    )
    return "sha256:" + hmac.new(
        bytes.fromhex(capability), message, hashlib.sha256
    ).hexdigest()


def _proposal_row(deployment: str, proposal_id: str, effect_key: str) -> tuple[object, ...]:
    digest = "sha256:" + ("a" * 64)
    expires = datetime.now(UTC) + timedelta(hours=1)
    return (
        proposal_id,
        deployment,
        7,
        effect_key,
        digest,
        digest,
        digest,
        "subject:proposer",
        "automation",
        "1.0.0",
        "run-1",
        "correlation-1",
        "ticket.update",
        "ticket:T-1",
        "low",
        digest,
        digest,
        digest,
        digest,
        digest,
        digest,
        digest,
        "provider-op",
        digest,
        digest,
        expires,
        expires,
        "review_requested",
        digest,
    )


@pytest.mark.parametrize(
    "message",
    (
        "restore id was reused with a different operator, key, or request",
        "restore batch replay conflicts with the first operator, key, or request",
        "restore finalize replay conflicts with the first operator, key, or request",
    ),
)
def test_restore_idempotency_conflicts_are_not_terminal_classifications(
    message: str,
) -> None:
    """Ordinary replay collisions are reconciliation conflicts, not outcomes."""

    class Cursor:
        def __enter__(self) -> "Cursor":
            return self

        def __exit__(self, *_args: object) -> bool:
            return False

        def execute(self, *_args: object) -> None:
            raise psycopg.errors.SerializationFailure(message)

        def fetchall(self) -> list[object]:
            return []

    class Connection:
        def cursor(self) -> Cursor:
            return Cursor()

    store = object.__new__(PostgresExternalActionStore)
    store._conn = Connection()
    with pytest.raises(ExternalActionStoreError) as raised:
        store._query("SELECT 1")
    assert raised.value.code == "restore_reconciliation_conflict"
    assert "database_restore_terminal_conflict" not in str(raised.value)


def test_rollout_guard_blocks_empty_installed_schema_without_mutating_it(
    okg_migrated: str, tmp_path: Path
) -> None:
    deployment = "external-action-rollout-" + uuid4().hex[:12]
    with db_connect(okg_migrated) as conn:
        before = conn.execute(
            "SELECT count(*) FROM okg.external_action_api_role_bindings "
            "WHERE deployment_name = %s",
            (deployment,),
        ).fetchone()[0]

    assert external_action_rollout_state(
        deployment=deployment,
        deployment_root=tmp_path,
        dsn=okg_migrated,
    ) == EXTERNAL_ACTION_ROLLOUT_BLOCKED

    with db_connect(okg_migrated) as conn:
        after = conn.execute(
            "SELECT count(*) FROM okg.external_action_api_role_bindings "
            "WHERE deployment_name = %s",
            (deployment,),
        ).fetchone()[0]
    assert before == after == 0

    with db_connect(okg_migrated) as conn:
        _bind_verifier_role(conn, deployment)
        conn.commit()

    assert external_action_rollout_state(
        deployment=deployment,
        deployment_root=tmp_path,
        dsn=okg_migrated,
    ) == EXTERNAL_ACTION_ROLLOUT_ENABLED


def test_postgres_store_boundary_is_bound_redacted_and_append_only(
    okg_migrated: str,
) -> None:
    deployment = "external-action-test-" + uuid4().hex[:12]
    other_deployment = deployment + "-other"
    proposal_id = "proposal-" + uuid4().hex
    other_proposal_id = "proposal-" + uuid4().hex
    effect_key = "effect-" + uuid4().hex
    other_effect_key = "effect-" + uuid4().hex

    with db_connect(okg_migrated) as conn:
        _bind_verifier_role(conn, deployment)
        _set_verifier_capability(conn)
        for row in (
            _proposal_row(deployment, proposal_id, effect_key),
            _proposal_row(other_deployment, other_proposal_id, other_effect_key),
        ):
            conn.execute(
                """
                INSERT INTO okg.external_action_proposals (
                  proposal_id, deployment_name, execution_epoch, effect_instance_key,
                  submission_content_hash, submission_request_hash,
                  submission_idempotency_key_hash, proposer_separation_subject,
                  automation_id, automation_version, run_id, correlation_id,
                  action_type, target_ref, risk_tier, action_schema_digest,
                  action_contract_digest, executor_contract_digest,
                  payload_renderer_digest, governed_tool_binding_digest,
                  provider_connection_contract_digest, provider_capability_digest,
                  provider_operation_id, provider_operation_descriptor_digest,
                  original_payload_hash, requested_review_expires_at,
                  effective_review_expires_at, summary_code, current_payload_hash
                ) VALUES (
                  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                row,
            )
        conn.execute(
            """
            INSERT INTO okg.external_action_payload_versions (
              deployment_name, proposal_id, payload_version, payload_hash,
              canonical_payload, difference_field_codes
            ) VALUES (%s, %s, 1, %s, %s, '[]'::jsonb)
            """,
            (deployment, proposal_id, "sha256:" + ("b" * 64), b'{"target":"T-1"}'),
        )

        # The API role has no direct access to the raw ledger.  The fixed
        # helper is the only bridge from session_user to deployment.
        assert conn.execute(
            "SELECT rolcanlogin FROM pg_catalog.pg_roles "
            "WHERE rolname = 'okg_external_action_api'"
        ).fetchone() == (False,)
        assert not conn.execute(
            """SELECT has_table_privilege(
                 'okg_external_action_api',
                 'okg.external_action_proposals', 'SELECT'
               )"""
        ).fetchone()[0]

        conn.execute("SET LOCAL ROLE okg_external_action_api")
        conn.execute("SAVEPOINT unverified_snapshot")
        conn.execute(
            "SELECT set_config('okg.external_action_verifier_capability', '', true)"
        )
        with pytest.raises(
            psycopg.errors.InvalidAuthorizationSpecification
        ) as unverified_snapshot:
            conn.execute("SELECT okg.external_action_store_snapshot()")
        assert unverified_snapshot.value.sqlstate == "28000"
        conn.execute("ROLLBACK TO SAVEPOINT unverified_snapshot")
        conn.execute("SAVEPOINT unverified_owner_attestation")
        conn.execute(
            "SELECT set_config('okg.external_action_verifier_capability', '', true)"
        )
        with pytest.raises(
            psycopg.errors.InvalidAuthorizationSpecification
        ) as unverified_owner_attestation:
            conn.execute(
                "SELECT okg.external_action_owner_unavailability('{}'::jsonb)"
            )
        assert unverified_owner_attestation.value.sqlstate == "28000"
        conn.execute("ROLLBACK TO SAVEPOINT unverified_owner_attestation")
        conn.execute("SAVEPOINT unverified_replay_nonce")
        conn.execute(
            "SELECT set_config('okg.external_action_verifier_capability', '', true)"
        )
        with pytest.raises(
            psycopg.errors.InvalidAuthorizationSpecification
        ) as unverified_replay_nonce:
            conn.execute(
                "SELECT okg.external_action_insert_dpop_replay_nonce("
                "%s, %s, %s, %s, %s, %s, %s, clock_timestamp(), "
                "clock_timestamp() + interval '5 minutes')",
                (
                    deployment,
                    "sha256:" + ("1" * 64),
                    "dpop-thumbprint",
                    "sha256:" + ("2" * 64),
                    "POST",
                    "sha256:" + ("3" * 64),
                    "sha256:" + ("4" * 64),
                ),
            )
        assert unverified_replay_nonce.value.sqlstate == "28000"
        conn.execute("ROLLBACK TO SAVEPOINT unverified_replay_nonce")
        for savepoint, statement, params in (
            (
                "unverified_principal_resolver",
                "SELECT okg.external_action_store_resolve_principal(%s, %s)",
                ("https://issuer.invalid", "subject"),
            ),
            (
                "unverified_policy_bundle",
                "SELECT okg.external_action_nomos_policy_bundle(%s, %s, %s)",
                ("external_action", "provider", "binding"),
            ),
            (
                "unverified_authority_snapshot",
                "SELECT okg.external_action_nomos_authority_snapshot("
                "%s, %s, %s, %s, %s)",
                (
                    deployment,
                    "audit",
                    "decision",
                    "external_action.propose",
                    "sha256:" + ("5" * 64),
                ),
            ),
        ):
            conn.execute(f"SAVEPOINT {savepoint}")
            conn.execute(
                "SELECT set_config('okg.external_action_verifier_capability', '', true)"
            )
            with pytest.raises(
                psycopg.errors.InvalidAuthorizationSpecification
            ) as unverified_read:
                conn.execute(statement, params)
            assert unverified_read.value.sqlstate == "28000"
            conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
        assert conn.execute(
            "SELECT okg.external_action_session_deployment()"
        ).fetchone() == (deployment,)
        visible = conn.execute(
            "SELECT proposal_id, deployment_name FROM okg.v_external_action_proposals "
            "ORDER BY proposal_id"
        ).fetchall()
        assert visible == [(proposal_id, deployment)]

        # Canonical payload bytes and all handoff material stay out of the API
        # projection even when the view is queried by the service role.
        columns = conn.execute(
            """
            SELECT column_name
              FROM information_schema.columns
             WHERE table_schema = 'okg'
               AND table_name = 'v_external_action_proposals'
            """
        ).fetchall()
        assert "canonical_payload" not in {row[0] for row in columns}

        # Leave the role before testing the owner-enforced append-only trigger.
        conn.execute("RESET ROLE")
        conn.execute("SAVEPOINT append_only_probe")
        with pytest.raises(psycopg.errors.ObjectInUse) as raised:
            conn.execute(
                "UPDATE okg.external_action_payload_versions "
                "SET payload_hash = 'tampered' WHERE proposal_id = %s",
                (proposal_id,),
            )
        assert raised.value.sqlstate == "55006"
        conn.execute("ROLLBACK TO SAVEPOINT append_only_probe")
        conn.rollback()


def test_postgres_principal_resolver_returns_all_live_barriers_for_authentication(
    okg_migrated: str,
) -> None:
    """The SQL principal projection is sufficient for both auth transports.

    Stable provider/binding identifiers alone are not an authenticated
    principal.  The resolver must carry the live status, update barriers,
    credential generations, and certificate issuance evidence that the JWT
    and mTLS adapters compare before admitting a request. Presented JWT and
    certificate times remain verifier output; the database mapper must not
    manufacture them from provider metadata.
    """

    suffix = uuid4().hex[:12]
    runtime_role = "okg_ext_resolver_" + suffix
    deployment = "external-action-resolver-" + suffix
    provider_id = "provider-" + suffix
    binding_id = "binding-" + suffix
    subject = "subject:resolver"
    issuer = "https://issuer.resolver.example"
    generation = "sha256:" + "a" * 64
    certificate_thumbprint = "resolver-certificate-thumbprint"
    cert_before = "2026-08-27T00:00:00.000000Z"
    cert_after = "2026-08-28T00:00:00.000000Z"
    runtime_dsn = make_conninfo(okg_migrated, user=runtime_role, password="okg")

    def j(value: object) -> str:
        return json.dumps(value, separators=(",", ":"))

    try:
        with db_connect(okg_migrated) as admin:
            admin.execute(
                sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                    sql.Identifier(runtime_role), sql.Literal("okg")
                )
            )
            admin.execute(
                sql.SQL("GRANT okg_external_action_api TO {}").format(
                    sql.Identifier(runtime_role)
                )
            )
            _bind_verifier_role(admin, deployment, session_user=runtime_role)
            metadata = {
                "credential_generation_digest": generation,
                "certificate_thumbprint": certificate_thumbprint,
                "certificate_not_before": cert_before,
                "certificate_not_after": cert_after,
            }
            admin.execute(
                """
                INSERT INTO okg.nomos_identity_providers
                  (deployment_name, provider_id, provider_type, issuer,
                   subject_claim, status, metadata)
                VALUES (%s, %s, 'oidc', %s, 'sub', 'active', %s::jsonb)
                """,
                (deployment, provider_id, issuer, j(metadata)),
            )
            admin.execute(
                """
                INSERT INTO okg.nomos_subject_bindings
                  (deployment_name, binding_id, provider_id, issuer,
                   claim_path, claim_value, subject, scope, status, metadata)
                VALUES (%s, %s, %s, %s, 'sub', %s, %s,
                        'external_action', 'active', %s::jsonb)
                """,
                (deployment, binding_id, provider_id, issuer, "raw-resolver", subject, j(metadata)),
            )
            provider_updated_at, binding_updated_at = admin.execute(
                """
                SELECT p.updated_at, b.updated_at
                  FROM okg.nomos_identity_providers p
                  JOIN okg.nomos_subject_bindings b
                    ON b.deployment_name = p.deployment_name
                   AND b.provider_id = p.provider_id
                   AND b.issuer = p.issuer
                 WHERE p.deployment_name = %s
                   AND p.provider_id = %s
                   AND b.binding_id = %s
                """,
                (deployment, provider_id, binding_id),
            ).fetchone()
            admin.commit()

        with db_connect(runtime_dsn) as runtime:
            _set_verifier_capability(runtime)
            resolved = runtime.execute(
                "SELECT okg.external_action_store_resolve_principal(%s, %s)",
                (issuer, "raw-resolver"),
            ).fetchone()[0]

        assert resolved["principal"] == subject
        assert resolved["separation_subject"] == subject
        assert resolved["provider_id"] == provider_id
        assert resolved["subject_binding_id"] == binding_id
        assert resolved["issuer"] == issuer
        assert resolved["deployment"] == deployment
        assert resolved["provider_status"] == "active"
        assert resolved["binding_status"] == "active"
        assert datetime.fromisoformat(resolved["provider_updated_at"]) == provider_updated_at
        assert datetime.fromisoformat(resolved["binding_updated_at"]) == binding_updated_at
        assert resolved["provider_credential_generation_digest"] == generation
        assert resolved["binding_credential_generation_digest"] == generation
        assert resolved["provider_certificate_thumbprint"] == certificate_thumbprint
        assert resolved["binding_certificate_thumbprint"] == certificate_thumbprint
    finally:
        with db_connect(okg_migrated) as admin:
            admin.execute(
                "DELETE FROM okg.external_action_api_role_bindings WHERE \"session_user\" = %s",
                (runtime_role,),
            )
            admin.execute(
                sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(runtime_role))
            )
            admin.commit()


def test_postgres_mapped_login_cannot_mint_allow_then_submit_lifecycle_command(
    okg_migrated: str,
) -> None:
    """A mapped login cannot manufacture its own Nomos allow decision.

    The three callable RPCs are individually necessary for the durable
    service, but their composition must still require a real verifier-owned
    authorization transaction.  This attacker mints an ``allow`` decision,
    binds a caller-authored audit to it, and then submits a complete command
    from a LOGIN role that only inherits the public API role.
    """

    suffix = uuid4().hex[:12]
    runtime_role = "okg_ext_minter_" + suffix
    deployment = "external-action-minting-" + suffix
    provider_id = "provider-" + suffix
    binding_id = "binding-" + suffix
    policy_bundle_id = "bundle-" + suffix
    activation_id = "activation-" + suffix
    audit_id = "audit-" + suffix
    decision_id = "decision-" + suffix
    lease_digest = "sha256:" + "e" * 64
    digest = "sha256:" + "a" * 64
    issuer = "https://issuer.mint.example"
    subject = "subject:runtime-mint"
    runtime_dsn = make_conninfo(okg_migrated, user=runtime_role, password="okg")
    now = datetime.now(UTC)

    def j(value: object) -> str:
        return json.dumps(value, separators=(",", ":"))

    compiled_bundle = {
        "policy_bundle_id": policy_bundle_id,
        "policy_version": "1",
        "policy_fingerprint": digest,
        "compiler_version": "hostile-test",
        "source_generation_id": None,
        "scope": {},
        "default_effect": "deny",
        "reason_code_catalog": {"nomos.default_deny": "No matching allow rule."},
        "effective_from": None,
        "effective_until": None,
        "compiled_at": "2026-08-27T00:00:00.000000Z",
        "rules": [
            {
                "rule_id": "deny-other-subject",
                "effect": "deny",
                "actions": ["external_action.propose"],
                "subjects": ["subject:not-runtime-mint"],
                "match": {},
                "priority": 0,
                "reason_code": "deny-other-subject",
                "obligations": [],
            }
        ],
    }

    try:
        with db_connect(okg_migrated) as admin:
            admin.execute(
                sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                    sql.Identifier(runtime_role), sql.Literal("okg")
                )
            )
            admin.execute(
                sql.SQL("GRANT okg_external_action_api TO {}").format(
                    sql.Identifier(runtime_role)
                )
            )
            _bind_verifier_role(admin, deployment, session_user=runtime_role)
            admin.execute(
                """
                INSERT INTO okg.nomos_identity_providers
                  (deployment_name, provider_id, provider_type, issuer,
                   subject_claim, status, metadata)
                VALUES (%s, %s, 'oidc', %s, 'sub', 'active', %s::jsonb)
                """,
                (
                    deployment,
                    provider_id,
                    issuer,
                    j({"credential_generation_digest": digest}),
                ),
            )
            admin.execute(
                """
                INSERT INTO okg.nomos_subject_bindings
                  (deployment_name, binding_id, provider_id, issuer,
                   claim_path, claim_value, subject, scope, status, metadata)
                VALUES (%s, %s, %s, %s, 'sub', %s, %s,
                        'external_actions', 'active', %s::jsonb)
                """,
                (
                    deployment,
                    binding_id,
                    provider_id,
                    issuer,
                    "runtime-mint",
                    subject,
                    j({"credential_generation_digest": digest}),
                ),
            )
            admin.execute(
                """
                INSERT INTO okg.nomos_policy_bundles
                  (deployment_name, policy_bundle_id, policy_version,
                   policy_fingerprint, compiler_version, default_effect,
                   bundle, status)
                VALUES (%s, %s, '1', %s, 'hostile-test', 'deny',
                        %s::jsonb, 'active')
                """,
                (deployment, policy_bundle_id, digest, j(compiled_bundle)),
            )
            admin.execute(
                """
                INSERT INTO okg.nomos_policy_activations
                  (deployment_name, activation_id, scope,
                   policy_fingerprint, activated_by, activation_decision_id)
                VALUES (%s, %s, 'external_actions', %s,
                        'test-fixture', %s)
                """,
                (deployment, activation_id, digest, "activation-" + suffix),
            )
            provider_updated_at, binding_updated_at = admin.execute(
                """
                SELECT p.updated_at, b.updated_at
                  FROM okg.nomos_identity_providers p
                  JOIN okg.nomos_subject_bindings b
                    ON b.deployment_name = p.deployment_name
                   AND b.provider_id = p.provider_id
                   AND b.issuer = p.issuer
                 WHERE p.deployment_name = %s
                   AND p.provider_id = %s
                   AND b.binding_id = %s
                """,
                (deployment, provider_id, binding_id),
            ).fetchone()
            admin.execute(
                """
                INSERT INTO okg.external_action_execution_epoch_leases (
                  deployment_name, epoch, lease_sequence, issued_at, expires_at,
                  signer_separation_subject, nonce, protected_b64, payload_b64,
                  signature_b64, signer_key_id_digest, envelope_digest,
                  verification_source, verification_config_digest,
                  verification_login_binding_digest, lease_chain_digest,
                  store_record
                ) VALUES (%s, 1, 1, %s, %s, 'subject:lease-signer', %s,
                          'protected', 'payload', 'signature', %s, %s,
                          'signed_startup_gate', %s, %s, %s, %s::jsonb)
                """,
                (
                    deployment,
                    now - timedelta(minutes=1),
                    now + timedelta(hours=1),
                    "nonce-" + suffix,
                    digest,
                    lease_digest,
                    digest,
                    digest,
                    digest,
                    j({"envelope_digest": lease_digest}),
                ),
            )
            admin.commit()

        with db_connect(runtime_dsn) as conn:
            _set_verifier_capability(conn)
            authority = conn.execute(
                """
                SELECT okg.external_action_nomos_policy_bundle(
                  'external_actions', %s, %s)
                """,
                (provider_id, binding_id),
            ).fetchone()[0]
            decision = {
                "deployment": deployment,
                "decision_id": decision_id,
                "policy_bundle_id": authority["policy_bundle_id"],
                "policy_version": authority["policy_version"],
                "policy_fingerprint": authority["policy_fingerprint"],
                "input_hash": digest,
                "subject": subject,
                "action": "external_action.propose",
                "resource": {},
                "classification": "external_action",
                "context": {},
                "effect": "allow",
                "reason_code": "caller_chosen_allow",
                "matched_rule_ids": ["caller-chosen-rule"],
                "obligations": [],
                "enforcement_point": "runtime",
                "target": {},
                "policy_scope": "external_actions",
                "provider_id": provider_id,
                "binding_id": binding_id,
                "provider_status": authority["provider_status"],
                "provider_updated_at": authority["provider_updated_at"],
                "binding_status": authority["binding_status"],
                "binding_updated_at": authority["binding_updated_at"],
                # Keep the hostile fixture complete enough to reach the
                # database-policy check.  The mapped login still cannot mint
                # an allow: the active bundle resolves this request to deny
                # before the verifier assertion is considered.
                "allowed": True,
                "final_effect": "allow",
                "required_obligations": [],
                "completed_obligations": [],
                "missing_obligations": [],
            }
            decision_assertion, decision_assertion_tag = _verifier_assertion(
                {"deployment": deployment},
                operation="external_action.propose",
                transition_digest=digest,
                nomos_decision_id=decision_id,
                policy_fingerprint=authority["policy_fingerprint"],
                provider_id=provider_id,
                provider_status=authority["provider_status"],
                provider_updated_at=authority["provider_updated_at"],
                binding_id=binding_id,
                binding_status=authority["binding_status"],
                binding_updated_at=authority["binding_updated_at"],
                nomos_input_hash=digest,
            )
            decision["verifier_assertion"] = decision_assertion
            decision["verifier_assertion_tag"] = decision_assertion_tag
            with pytest.raises(psycopg.errors.InsufficientPrivilege) as rejected:
                conn.execute(
                    "SELECT okg.external_action_nomos_persist_decision(%s::jsonb)",
                    (j(decision),),
                )
            assert rejected.value.sqlstate == "42501"
            conn.rollback()
        with db_connect(okg_migrated) as admin:
            assert admin.execute(
                "SELECT count(*) FROM okg.nomos_decisions "
                "WHERE deployment_name = %s AND decision_id = %s",
                (deployment, decision_id),
            ).fetchone()[0] == 0
            assert admin.execute(
                "SELECT count(*) FROM okg.external_action_auth_audits "
                "WHERE deployment_name = %s AND authentication_audit_id = %s",
                (deployment, audit_id),
            ).fetchone()[0] == 0
            assert admin.execute(
                "SELECT count(*) FROM okg.external_action_events "
                "WHERE deployment_name = %s AND nomos_decision_ref = %s",
                (deployment, decision_id),
            ).fetchone()[0] == 0
    finally:
        with db_connect(okg_migrated) as admin:
            admin.execute(
                "DELETE FROM okg.external_action_api_role_bindings WHERE \"session_user\" = %s",
                (runtime_role,),
            )
            admin.execute(
                sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(runtime_role))
            )
            admin.commit()


def test_postgres_mapped_login_rejects_forged_hmac_assertions_and_replay(
    okg_migrated: str,
) -> None:
    """A runtime login cannot mint, retag, expire, or replay auth evidence."""

    suffix = uuid4().hex[:12]
    runtime_role = "okg_ext_assertion_" + suffix
    deployment = "external-action-assertion-" + suffix
    provider_id = "provider-" + suffix
    binding_id = "binding-" + suffix
    decision_id = "decision-" + suffix
    audit_id = "audit-" + suffix
    issuer = "https://issuer.assertion.example"
    digest = "sha256:" + ("a" * 64)
    runtime_dsn = make_conninfo(okg_migrated, user=runtime_role, password="okg")

    def j(value: object) -> str:
        return json.dumps(value, separators=(",", ":"))

    try:
        with db_connect(okg_migrated) as admin:
            admin.execute(
                sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                    sql.Identifier(runtime_role), sql.Literal("okg")
                )
            )
            admin.execute(
                sql.SQL("GRANT okg_external_action_api TO {}").format(
                    sql.Identifier(runtime_role)
                )
            )
            _bind_verifier_role(admin, deployment, session_user=runtime_role)
            admin.execute(
                """
                INSERT INTO okg.nomos_identity_providers
                  (deployment_name, provider_id, provider_type, issuer,
                   subject_claim, status, metadata)
                VALUES (%s, %s, 'oidc', %s, 'sub', 'active', %s::jsonb)
                """,
                (deployment, provider_id, issuer, j({"credential_generation_digest": digest})),
            )
            admin.execute(
                """
                INSERT INTO okg.nomos_subject_bindings
                  (deployment_name, binding_id, provider_id, issuer,
                   claim_path, claim_value, subject, scope, status, metadata)
                VALUES (%s, %s, %s, %s, 'sub', 'assertion-subject',
                        'assertion-subject', 'external_action', 'active', %s::jsonb)
                """,
                (
                    deployment,
                    binding_id,
                    provider_id,
                    issuer,
                    j({"credential_generation_digest": digest}),
                ),
            )
            admin.execute(
                """
                INSERT INTO okg.nomos_decisions
                  (deployment_name, decision_id, input_hash, subject, action,
                   classification, effect, reason_code, enforcement_point,
                   policy_fingerprint)
                VALUES (%s, %s, %s, 'assertion-subject', 'external_action.propose',
                        'external_action', 'allow', 'assertion_test', 'test', %s)
                """,
                (deployment, decision_id, digest, digest),
            )
            provider_updated_at, binding_updated_at = admin.execute(
                """
                SELECT p.updated_at, b.updated_at
                  FROM okg.nomos_identity_providers p
                  JOIN okg.nomos_subject_bindings b
                    ON b.deployment_name = p.deployment_name
                   AND b.provider_id = p.provider_id
                   AND b.issuer = p.issuer
                 WHERE p.deployment_name = %s AND p.provider_id = %s
                   AND b.binding_id = %s
                """,
                (deployment, provider_id, binding_id),
            ).fetchone()
            admin.commit()

        def timestamp(value: datetime) -> str:
            return value.astimezone(UTC).isoformat(timespec="microseconds").replace(
                "+00:00", "Z"
            )

        audit = {
            "deployment": deployment,
            "authentication_audit_id": audit_id,
            "request_id": "request-" + audit_id,
            "principal": "assertion-subject",
            "separation_subject": "assertion-subject",
            "nomos_provider_id": provider_id,
            "nomos_subject_binding_id": binding_id,
            "authentication_method_code": "jwt_dpop",
            "issuer_or_trust_domain_digest": "sha256:"
            + hashlib.sha256(issuer.encode()).hexdigest(),
            "auth_config_digest": digest,
            "result_code": "authenticated",
            "provider_status": "active",
            "binding_status": "active",
            "provider_updated_at": timestamp(provider_updated_at),
            "binding_updated_at": timestamp(binding_updated_at),
            "provider_credential_generation_digest": digest,
            "binding_credential_generation_digest": digest,
            "credential_generation_digest": digest,
            "presented_credential_generation_digest": digest,
            "token_issued_at": timestamp(max(provider_updated_at, binding_updated_at)),
            "nomos_decision_id": decision_id,
        }
        assertion, tag = _verifier_assertion(
            audit,
            operation="external_action.propose",
            transition_digest=digest,
            nomos_decision_id=decision_id,
            policy_fingerprint=digest,
            provider_id=provider_id,
            provider_status="active",
            provider_updated_at=audit["provider_updated_at"],
            binding_id=binding_id,
            binding_status="active",
            binding_updated_at=audit["binding_updated_at"],
            nomos_input_hash=digest,
            authentication_audit_id=audit_id,
        )

        with db_connect(runtime_dsn) as runtime:
            # A mapped login may set a custom GUC, but a wrong capability can
            # never authenticate a verifier assertion.
            _set_verifier_capability(runtime, "deadbeef" * 8)
            with pytest.raises(psycopg.errors.InvalidAuthorizationSpecification) as wrong_capability:
                runtime.execute(
                    "SELECT okg.external_action_record_auth_audit(%s::jsonb)",
                    (j({**audit, "verifier_assertion": assertion, "verifier_assertion_tag": tag}),),
                )
            assert wrong_capability.value.sqlstate == "28000"
            runtime.rollback()

            _set_verifier_capability(runtime)
            with pytest.raises(psycopg.errors.InvalidAuthorizationSpecification) as wrong_tag:
                runtime.execute(
                    "SELECT okg.external_action_record_auth_audit(%s::jsonb)",
                    (j({**audit, "verifier_assertion": assertion, "verifier_assertion_tag": digest}),),
                )
            assert wrong_tag.value.sqlstate == "28000"
            runtime.rollback()

            stale = {
                **assertion,
                "issued_at": "2020-01-01T00:00:00.000000Z",
                "expires_at": "2020-01-01T00:00:20.000000Z",
            }
            _set_verifier_capability(runtime)
            with pytest.raises(psycopg.errors.InvalidAuthorizationSpecification) as stale_assertion:
                runtime.execute(
                    "SELECT okg.external_action_record_auth_audit(%s::jsonb)",
                    (j({**audit, "verifier_assertion": stale, "verifier_assertion_tag": _verifier_tag(stale)}),),
                )
            assert stale_assertion.value.sqlstate == "28000"
            runtime.rollback()

            _set_verifier_capability(runtime)
            accepted = runtime.execute(
                "SELECT okg.external_action_record_auth_audit(%s::jsonb)",
                (j({**audit, "verifier_assertion": assertion, "verifier_assertion_tag": tag}),),
            ).fetchone()[0]
            assert accepted["authentication_audit_id"] == audit_id
            runtime.commit()

            _set_verifier_capability(runtime)
            runtime.execute("SAVEPOINT conflicting_audit_replay")
            with pytest.raises(psycopg.errors.SerializationFailure) as conflict:
                runtime.execute(
                    "SELECT okg.external_action_record_auth_audit(%s::jsonb)",
                    (j({
                        **audit,
                        "request_id": "different-request",
                        "verifier_assertion": assertion,
                        "verifier_assertion_tag": tag,
                    }),),
                )
            assert conflict.value.sqlstate == "40001"
            runtime.execute("ROLLBACK TO SAVEPOINT conflicting_audit_replay")

            replay_assertion = {**assertion, "authentication_audit_id": audit_id + "-replay"}
            _set_verifier_capability(runtime)
            runtime.execute("SAVEPOINT verifier_nonce_replay")
            with pytest.raises(psycopg.errors.UniqueViolation) as replay:
                runtime.execute(
                    "SELECT okg.external_action_record_auth_audit(%s::jsonb)",
                    (j({
                        **audit,
                        "authentication_audit_id": replay_assertion["authentication_audit_id"],
                        "request_id": "request-replay",
                        "verifier_assertion": replay_assertion,
                        "verifier_assertion_tag": _verifier_tag(replay_assertion),
                    }),),
                )
            assert replay.value.sqlstate == "23505"
            runtime.execute("ROLLBACK TO SAVEPOINT verifier_nonce_replay")
    finally:
        with db_connect(okg_migrated) as admin:
            admin.execute(
                "DELETE FROM okg.external_action_api_role_bindings WHERE \"session_user\" = %s",
                (runtime_role,),
            )
            admin.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(runtime_role)))
            admin.commit()


def test_postgres_api_role_binding_guard_enforces_versioned_rotation_and_identity(
    okg_migrated: str,
) -> None:
    """Role bindings accept only monotonic verifier changes and fixed identity."""

    suffix = uuid4().hex[:12]
    session_user = "okg_ext_guard_" + suffix
    deployment = "external-action-guard-" + suffix
    rotated_capability = "fedcba9876543210" * 4
    rotated_digest = "sha256:" + hashlib.sha256(
        bytes.fromhex(rotated_capability)
    ).hexdigest()

    def rejected_update(statement: str, params: tuple[object, ...] = ()) -> None:
        savepoint = "role_binding_guard_probe"
        conn.execute(f"SAVEPOINT {savepoint}")
        with pytest.raises(psycopg.errors.DatabaseError) as raised:
            conn.execute(statement, params)
        assert raised.value.sqlstate in {"22023", "55006"}
        conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")

    try:
        with db_connect(okg_migrated) as conn:
            # A binding starts at version one and with verifier generation one.
            for invalid_version, invalid_generation in ((2, 1), (1, 2)):
                savepoint = "role_binding_guard_insert_probe"
                conn.execute(f"SAVEPOINT {savepoint}")
                with pytest.raises(psycopg.errors.InvalidParameterValue) as raised:
                    conn.execute(
                        """
                        INSERT INTO okg.external_action_api_role_bindings
                          ("session_user", deployment_name, binding_version,
                           verifier_capability_generation)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (
                            f"{session_user}_{invalid_version}_{invalid_generation}",
                            deployment,
                            invalid_version,
                            invalid_generation,
                        ),
                    )
                assert raised.value.sqlstate == "22023"
                conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")

            conn.execute(
                """
                INSERT INTO okg.external_action_api_role_bindings
                  ("session_user", deployment_name, binding_version,
                   verifier_capability_digest, verifier_capability_generation)
                VALUES (%s, %s, 1, NULL, 1)
                """,
                (session_user, deployment),
            )
            conn.commit()

            # Initial provisioning records the digest without advancing the
            # generation; subsequent rotations must change both together.
            conn.execute(
                """
                UPDATE okg.external_action_api_role_bindings
                   SET verifier_capability_digest = %s
                 WHERE "session_user" = %s AND deployment_name = %s
                """,
                (_TEST_VERIFIER_CAPABILITY_DIGEST, session_user, deployment),
            )
            conn.commit()
            assert conn.execute(
                """
                SELECT binding_version, verifier_capability_digest,
                       verifier_capability_generation
                  FROM okg.external_action_api_role_bindings
                 WHERE "session_user" = %s AND deployment_name = %s
                """,
                (session_user, deployment),
            ).fetchone() == (1, _TEST_VERIFIER_CAPABILITY_DIGEST, 1)

            rejected_update(
                """
                UPDATE okg.external_action_api_role_bindings
                   SET verifier_capability_generation = 2
                 WHERE "session_user" = %s AND deployment_name = %s
                """,
                (session_user, deployment),
            )
            rejected_update(
                """
                UPDATE okg.external_action_api_role_bindings
                   SET verifier_capability_digest = %s
                 WHERE "session_user" = %s AND deployment_name = %s
                """,
                (rotated_digest, session_user, deployment),
            )
            rejected_update(
                """
                UPDATE okg.external_action_api_role_bindings
                   SET verifier_capability_digest = %s,
                       verifier_capability_generation = 3
                 WHERE "session_user" = %s AND deployment_name = %s
                """,
                (rotated_digest, session_user, deployment),
            )

            conn.execute(
                """
                UPDATE okg.external_action_api_role_bindings
                   SET verifier_capability_digest = %s,
                       verifier_capability_generation = 2
                 WHERE "session_user" = %s AND deployment_name = %s
                """,
                (rotated_digest, session_user, deployment),
            )
            conn.commit()
            assert conn.execute(
                """
                SELECT verifier_capability_digest, verifier_capability_generation
                  FROM okg.external_action_api_role_bindings
                 WHERE "session_user" = %s AND deployment_name = %s
                """,
                (session_user, deployment),
            ).fetchone() == (rotated_digest, 2)

            # The login/deployment/version/timestamp tuple is the binding
            # identity; verifier status and credentials are the mutable part.
            rejected_update(
                """
                UPDATE okg.external_action_api_role_bindings
                   SET "session_user" = %s
                 WHERE "session_user" = %s AND deployment_name = %s
                """,
                (session_user + "_other", session_user, deployment),
            )
            rejected_update(
                """
                UPDATE okg.external_action_api_role_bindings
                   SET deployment_name = %s
                 WHERE "session_user" = %s AND deployment_name = %s
                """,
                (deployment + "_other", session_user, deployment),
            )
            rejected_update(
                """
                UPDATE okg.external_action_api_role_bindings
                   SET binding_version = 2
                 WHERE "session_user" = %s AND deployment_name = %s
                """,
                (session_user, deployment),
            )
            rejected_update(
                """
                UPDATE okg.external_action_api_role_bindings
                   SET bound_at = bound_at + interval '1 second'
                 WHERE "session_user" = %s AND deployment_name = %s
                """,
                (session_user, deployment),
            )
    finally:
        with db_connect(okg_migrated) as conn:
            conn.execute(
                "DELETE FROM okg.external_action_api_role_bindings "
                "WHERE \"session_user\" = %s AND deployment_name = %s",
                (session_user, deployment),
            )
            conn.commit()


def test_postgres_trace_refines_formal_model_submit_approve_and_replay(
    okg_migrated: str,
) -> None:
    """Exercise submit, approve, claim, and outcome with durable evidence."""

    deployment = "external-action-lifecycle-" + uuid4().hex[:12]
    provider_id = "provider-" + uuid4().hex[:12]
    proposer_binding_id = "binding-" + uuid4().hex[:12]
    approver_binding_id = "binding-" + uuid4().hex[:12]
    worker_binding_id = "binding-" + uuid4().hex[:12]
    proposer_audit_id = "audit-" + uuid4().hex[:12]
    approver_audit_id = "audit-" + uuid4().hex[:12]
    worker_claim_audit_id = "audit-" + uuid4().hex[:12]
    worker_outcome_audit_id = "audit-" + uuid4().hex[:12]
    proposer_nomos_id = "nomos-" + uuid4().hex[:12]
    approver_nomos_id = "nomos-" + uuid4().hex[:12]
    worker_claim_nomos_id = "nomos-" + uuid4().hex[:12]
    worker_outcome_nomos_id = "nomos-" + uuid4().hex[:12]
    proposal_id = "proposal-" + uuid4().hex
    decision_id = "decision-" + uuid4().hex
    execution_id = "execution-" + uuid4().hex
    digest = "sha256:" + ("a" * 64)
    digest_b = "sha256:" + ("b" * 64)
    digest_c = "sha256:" + ("c" * 64)
    digest_d = "sha256:" + ("d" * 64)
    lease_digest = "sha256:" + ("e" * 64)
    issuer = "https://issuer.example"
    issuer_digest = "sha256:" + hashlib.sha256(issuer.encode()).hexdigest()
    now = datetime.now(UTC)
    later = (now + timedelta(hours=1)).isoformat().replace("+00:00", "Z")
    canonical_payload = {"target": "T1-1"}
    command_contract = {
        "action_type": "ticket.update",
        "action_schema_digest": digest,
        "executor_contract_digest": digest,
        "payload_renderer_digest": digest,
        "governed_tool_binding_ref": "tool:ticket",
        "governed_tool_binding_digest": digest,
        "provider_connection_ref": "connection:ticket",
        "provider_connection_contract_digest": digest,
        "provider_capability_digest": digest,
        "provider_operation_id": "provider-op",
        "provider_operation_descriptor_digest": digest,
        "provider_replay_mode": "lookup_or_idempotent_retry",
        "target_constraint": {},
        "minimum_risk_tier": "low",
        "review_and_execution_limits": {
            "maximum_execution_window_seconds": 7200,
            "maximum_review_lifetime_seconds": 7200,
            "reporting_attention_seconds": 3600,
            "reconcile_not_before_seconds": 1,
        },
        "diff_field_codes": [],
        "allowed_summary_codes": ["review_requested"],
        "transition_reason_codes": [],
        "outcome_reason_rules": {
            "provider_receipt_observed": {
                "allowed_outcomes": ["succeeded"],
                "provider_receipt": "required",
            },
        },
    }
    command_contract_digest = action_contract_digest(command_contract)
    command_payload_hash = domain_digest(
        "okg-external-action-payload-v1", canonical_payload
    )
    command_effect_instance_key = effect_instance_key(
        deployment, "subject-proposer", digest
    )
    command_content_hash = submission_content_hash(
        {
            "automation_id": "automation",
            "automation_version": "1.0.0",
            "run_id": "run-1",
            "correlation_id": "corr-1",
            "action_type": "ticket.update",
            "target_ref": "ticket:T-1",
            "risk_tier": "low",
            "action_schema_digest": digest,
            "action_contract_digest": command_contract_digest,
            "original_payload_hash": command_payload_hash,
            "requested_review_expires_at": later,
            "summary_code": "review_requested",
            "evidence_digests": [digest],
        }
    )
    command_request_hash = domain_digest(
        "okg-external-action-proposal-request-v1",
        {
            "deployment": deployment,
            "execution_epoch": 1,
            "effect_instance_key": command_effect_instance_key,
            "submission_content_hash": command_content_hash,
            "proposer_separation_subject": "subject-proposer",
            "automation_id": "automation",
            "automation_version": "1.0.0",
            "run_id": "run-1",
            "correlation_id": "corr-1",
            "action_type": "ticket.update",
            "target_ref": "ticket:T-1",
            "risk_tier": "low",
            "action_schema_digest": digest,
            "action_contract_digest": command_contract_digest,
            "executor_contract_digest": digest,
            "payload_renderer_digest": digest,
            "governed_tool_binding_digest": digest,
            "provider_connection_contract_digest": digest,
            "provider_capability_digest": digest,
            "provider_operation_id": "provider-op",
            "provider_operation_descriptor_digest": digest,
            "original_payload_hash": command_payload_hash,
            "requested_review_expires_at": later,
            "summary_code": "review_requested",
            "evidence_digests": [digest],
        },
    )
    claim_if_match = (
        f'"lifecycle:2;payload-version:1;payload:{command_payload_hash}"'
    )
    claim_request_hash = domain_digest(
        "okg-external-action-transition-request-v1",
        {
            "worker_separation_subject": "subject-worker",
            "decision_id": decision_id,
            "approved_lifecycle_version": 2,
            "approved_payload_version": 1,
            "approved_payload_hash": command_payload_hash,
            "supported_action_schema_digest": digest,
            "supported_action_contract_digest": command_contract_digest,
            "supported_executor_contract_digest": digest,
            "supported_payload_renderer_digest": digest,
            "supported_governed_tool_binding_digest": digest,
            "supported_provider_connection_contract_digest": digest,
            "supported_provider_capability_digest": digest,
            "supported_provider_operation_id": "provider-op",
            "supported_provider_operation_descriptor_digest": digest,
            "if_match": claim_if_match,
        },
    )
    claim_handoff_digest = domain_digest(
        "okg-external-action-claim-handoff-receipt-envelope-v1",
        {
            "protected_b64": "protected",
            "payload_b64": "payload",
            "signature_b64": "signature",
        },
    )
    outcome_if_match = (
        f'"lifecycle:3;payload-version:1;payload:{command_payload_hash}"'
    )
    canonical_outcome_request_hash = domain_digest(
        "okg-external-action-outcome-request-v1",
        {
            "execution_id": execution_id,
            "effect_instance_key": command_effect_instance_key,
            "worker_or_reconciler_separation_subject": "subject-worker",
            "outcome": "succeeded",
            "outcome_reason_code": "provider_receipt_observed",
            "provider_receipt_digest": digest_c,
            "recovery_journal_digest": digest_d,
            "owner_unavailability_attestation_digest": None,
            "if_match": outcome_if_match,
        },
    )

    def j(value: object) -> str:
        return json.dumps(value, separators=(",", ":"))

    with db_connect(okg_migrated) as conn:
        _bind_verifier_role(conn, deployment)
        _set_verifier_capability(conn)
        conn.execute(
            """
            INSERT INTO okg.nomos_identity_providers
              (deployment_name, provider_id, provider_type, issuer,
               subject_claim, status, metadata)
            VALUES (%s, %s, 'oidc', %s, 'sub', 'active', %s::jsonb)
            """,
            (
                deployment,
                provider_id,
                issuer,
                j({"credential_generation_digest": digest}),
            ),
        )
        for binding_id, subject in (
            (proposer_binding_id, "subject-proposer"),
            (approver_binding_id, "subject-approver"),
            (worker_binding_id, "subject-worker"),
        ):
            conn.execute(
                """
                INSERT INTO okg.nomos_subject_bindings
                  (deployment_name, binding_id, provider_id, issuer,
                   claim_path, claim_value, subject, scope, status, metadata)
                VALUES (%s, %s, %s, %s, 'sub', %s, %s, %s, 'active', %s::jsonb)
                """,
                (
                    deployment,
                    binding_id,
                    provider_id,
                    issuer,
                    subject,
                    subject,
                    deployment,
                    j({"credential_generation_digest": digest}),
                ),
            )
        for decision_id_value, subject, action in (
            (proposer_nomos_id, "subject-proposer", "external_action.propose"),
            (approver_nomos_id, "subject-approver", "external_action.decide"),
            (worker_claim_nomos_id, "subject-worker", "external_action.execute"),
            (worker_outcome_nomos_id, "subject-worker", "external_action.report"),
        ):
            conn.execute(
                """
                INSERT INTO okg.nomos_decisions
                  (deployment_name, decision_id, input_hash, subject, action,
                   classification, effect, reason_code, enforcement_point,
                   policy_fingerprint)
                VALUES (%s, %s, %s, %s, %s, 'external_action', 'allow',
                        'test_allow', 'test', %s)
                """,
                (deployment, decision_id_value, digest, subject, action, digest),
            )

        approve_transition_digest = domain_digest(
            "okg-external-action-transition-request-v1",
            {
                "operation": "approve",
                "action_contract_digest": command_contract_digest,
                "expected_lifecycle_version": None,
                "expected_payload_version": None,
                "expected_payload_hash": None,
                "expected_decision_id": None,
                "expected_decision_binding_hash": None,
                "replacement_payload_hash": None,
                "execute_before": later,
                "reason_code": None,
            },
        )
        transition_digests = {
            proposer_nomos_id: command_request_hash,
            approver_nomos_id: approve_transition_digest,
            worker_claim_nomos_id: claim_request_hash,
            worker_outcome_nomos_id: canonical_outcome_request_hash,
        }
        nomos_operations = {
            proposer_nomos_id: "external_action.propose",
            approver_nomos_id: "external_action.decide",
            worker_claim_nomos_id: "external_action.execute",
            worker_outcome_nomos_id: "external_action.report",
        }

        for audit_id, binding_id, subject, nomos_id in (
            (proposer_audit_id, proposer_binding_id, "subject-proposer", proposer_nomos_id),
            (approver_audit_id, approver_binding_id, "subject-approver", approver_nomos_id),
            (worker_claim_audit_id, worker_binding_id, "subject-worker", worker_claim_nomos_id),
            (worker_outcome_audit_id, worker_binding_id, "subject-worker", worker_outcome_nomos_id),
        ):
            provider_updated_at, binding_updated_at = conn.execute(
                "SELECT p.updated_at, b.updated_at "
                "FROM okg.nomos_identity_providers p "
                "JOIN okg.nomos_subject_bindings b "
                "  ON b.deployment_name = p.deployment_name "
                " AND b.provider_id = p.provider_id AND b.issuer = p.issuer "
                "WHERE p.deployment_name = %s AND p.provider_id = %s "
                "  AND b.binding_id = %s",
                (deployment, provider_id, binding_id),
            ).fetchone()

            def timestamp(value: datetime) -> str:
                return value.astimezone(UTC).isoformat(timespec="microseconds").replace(
                    "+00:00", "Z"
                )

            audit = {
                "deployment": deployment,
                "authentication_audit_id": audit_id,
                "request_id": "request-" + audit_id,
                "principal": subject,
                "separation_subject": subject,
                "nomos_provider_id": provider_id,
                "nomos_subject_binding_id": binding_id,
                "authentication_method_code": "jwt_dpop",
                "issuer_or_trust_domain_digest": issuer_digest,
                "auth_config_digest": digest,
                "result_code": "authenticated",
                "provider_status": "active",
                "binding_status": "active",
                "provider_updated_at": timestamp(provider_updated_at),
                "binding_updated_at": timestamp(binding_updated_at),
                "provider_credential_generation_digest": digest,
                "binding_credential_generation_digest": digest,
                "credential_generation_digest": digest,
                "presented_credential_generation_digest": digest,
                "token_issued_at": timestamp(max(provider_updated_at, binding_updated_at)),
                "nomos_decision_id": nomos_id,
            }
            assertion, assertion_tag = _verifier_assertion(
                audit,
                operation=nomos_operations[nomos_id],
                transition_digest=transition_digests[nomos_id],
                nomos_decision_id=nomos_id,
                policy_fingerprint=digest,
                provider_id=provider_id,
                provider_status="active",
                provider_updated_at=timestamp(provider_updated_at),
                binding_id=binding_id,
                binding_status="active",
                binding_updated_at=timestamp(binding_updated_at),
                nomos_input_hash=digest,
                authentication_audit_id=audit_id,
            )
            audit["verifier_assertion"] = assertion
            audit["verifier_assertion_tag"] = assertion_tag
            conn.execute(
                "SELECT okg.external_action_record_auth_audit(%s::jsonb)",
                (j(audit),),
            )

        # The first claim must bind to a durable same-epoch lease, not merely
        # to a caller-provided digest.
        conn.execute(
            """
            INSERT INTO okg.external_action_execution_epoch_leases (
              deployment_name, epoch, lease_sequence, issued_at, expires_at,
              signer_separation_subject, nonce, protected_b64, payload_b64,
              signature_b64, signer_key_id_digest, envelope_digest,
              verification_source, verification_config_digest,
              verification_login_binding_digest, authentication_audit_id,
              nomos_decision_id, store_record
            ) VALUES (
              %s, 1, 1, %s, %s, 'subject-worker', %s, 'protected',
              'payload', 'signature', %s, %s, 'strict_authenticated_api',
              %s, %s, %s, %s, %s::jsonb
            )
            """,
            (
                deployment,
                now,
                later,
                "nonce-" + uuid4().hex,
                digest,
                lease_digest,
                digest,
                digest,
                worker_claim_audit_id,
                worker_claim_nomos_id,
                j({"envelope_digest": lease_digest}),
            ),
        )
        # The binding is keyed on the login role this session actually has,
        # not on a name assumed for the cluster (issue #1973). `session_user`
        # is unaffected by the `SET LOCAL ROLE` below, so it stays the right
        # key for the whole test.
        login_role = conn.execute("SELECT session_user").fetchone()[0]
        bound_at = conn.execute(
            "SELECT bound_at FROM okg.external_action_api_role_bindings "
            "WHERE \"session_user\" = %s",
            (login_role,),
        ).fetchone()[0]

        command = {
            "deployment": deployment,
            "proposal_id": proposal_id,
            "execution_epoch": 1,
            "effect_instance_key": command_effect_instance_key,
            "submission_content_hash": command_content_hash,
            "submission_request_hash": command_request_hash,
            "submission_idempotency_key_hash": digest,
            "proposer_principal": "subject-proposer",
            "proposer_separation_subject": "subject-proposer",
            "automation_id": "automation",
            "automation_version": "1.0.0",
            "run_id": "run-1",
            "correlation_id": "corr-1",
            "action_type": "ticket.update",
            "target_ref": "ticket:T-1",
            "risk_tier": "low",
            "action_schema_digest": digest,
            "action_contract_digest": command_contract_digest,
            "executor_contract_digest": digest,
            "payload_renderer_digest": digest,
            "governed_tool_binding_digest": digest,
            "provider_connection_contract_digest": digest,
            "provider_capability_digest": digest,
            "provider_operation_id": "provider-op",
            "provider_operation_descriptor_digest": digest,
            "original_payload_hash": command_payload_hash,
            "requested_review_expires_at": later,
            "effective_review_expires_at": later,
            "summary_code": "review_requested",
            "evidence_digests": [digest],
            "current_payload_hash": command_payload_hash,
            "canonical_payload": {"__okg_bytes_b64": "eyJ0YXJnZXQiOiJUMS0xIn0="},
            "authentication_audit_id": proposer_audit_id,
            "nomos_decision_id": proposer_nomos_id,
            "policy_digest": digest,
            "action_contract_record": command_contract,
        }
        conn.execute("SET LOCAL ROLE okg_external_action_api")

        startup_binding = conn.execute(
            "SELECT okg.external_action_startup_login_binding()"
        ).fetchone()[0]
        assert set(startup_binding) == {
            "database_system_identifier", "session_user", "deployment",
            "binding_version", "bound_at", "verifier_capability_generation",
        }
        assert startup_binding["session_user"] == login_role
        assert startup_binding["deployment"] == deployment
        assert startup_binding["binding_version"] == 1
        assert startup_binding["verifier_capability_generation"] == 1
        assert startup_binding["bound_at"] == bound_at.astimezone(UTC).isoformat(
            timespec="microseconds"
        ).replace("+00:00", "Z")
        assert startup_binding["database_system_identifier"].isdigit()

        # The hash helper is an internal owner-only primitive; evaluate the
        # contract vector outside the API role before entering the RPC path.
        conn.execute("RESET ROLE")
        vectors_path = (
            Path(__file__).resolve().parents[3]
            / "tests/fixtures/external_actions/hash-vectors.yaml"
        )
        vectors = yaml.safe_load(vectors_path.read_text())
        submitted_vector = next(item for item in vectors["vectors"] if item["id"] == "submitted_event")
        sql_vector_hash = conn.execute(
            "SELECT okg.external_action_hash(%s, %s::jsonb)",
            (submitted_vector["domain"], j(submitted_vector["value"])),
        ).fetchone()[0]
        assert sql_vector_hash == submitted_vector["digest"]
        conn.execute("SET LOCAL ROLE okg_external_action_api")

        # A mapped login cannot choose a different lifecycle actor.  The SQL
        # boundary derives the proposer from the retained authentication audit
        # before it evaluates idempotency or inserts an event.
        conn.execute("SAVEPOINT forged_actor")
        with pytest.raises(psycopg.errors.InsufficientPrivilege) as forged_actor:
            conn.execute(
                "SELECT okg.external_action_submit(%s::jsonb)",
                (j({**command, "proposer_separation_subject": "subject-forged"}),),
            )
        assert forged_actor.value.sqlstate == "42501"
        conn.execute("ROLLBACK TO SAVEPOINT forged_actor")

        # The stable subject is not enough to authenticate a mapped login:
        # every lifecycle actor field must agree with the retained principal
        # in the audit row as well.
        conn.execute("SAVEPOINT forged_proposer_principal")
        with pytest.raises(psycopg.errors.InsufficientPrivilege) as forged_principal:
            conn.execute(
                "SELECT okg.external_action_submit(%s::jsonb)",
                (j({**command, "proposer_principal": "principal-forged"}),),
            )
        assert forged_principal.value.sqlstate == "42501"
        conn.execute("ROLLBACK TO SAVEPOINT forged_proposer_principal")

        conn.execute("SAVEPOINT supplied_event_hash")
        with pytest.raises(psycopg.Error) as supplied_hash_error:
            conn.execute(
                "SELECT okg.external_action_submit(%s::jsonb)",
                (j({**command, "event_hash": digest_c}),),
            )
        assert supplied_hash_error.value.sqlstate == "22023"
        conn.execute("ROLLBACK TO SAVEPOINT supplied_event_hash")

        conn.execute("SAVEPOINT incomplete_contract")
        with pytest.raises(psycopg.errors.InvalidParameterValue) as incomplete:
            conn.execute(
                "SELECT okg.external_action_submit(%s::jsonb)",
                (j({**command, "action_contract_record": {"action_type": "ticket.update"}}),),
            )
        assert incomplete.value.sqlstate == "22023"
        conn.execute("ROLLBACK TO SAVEPOINT incomplete_contract")

        first = conn.execute(
            "SELECT okg.external_action_submit(%s::jsonb)", (j(command),)
        ).fetchone()[0]
        second = conn.execute(
            "SELECT okg.external_action_submit(%s::jsonb)", (j(command),)
        ).fetchone()[0]
        assert first["replay"] is False
        assert first["review_state"] == "pending"
        assert first["execution_state"] == "unavailable"
        assert first["lifecycle_version"] == 1
        assert first["payload_version"] == 1
        assert second["replay"] is True

        # Modern commands carry a complete action contract.  The SQL boundary
        # must recompute the payload, proposal request, and stable effect
        # binding before idempotency lookup; each caller-supplied digest is
        # therefore hostile input, not an authority assertion.
        modern_contract = command_contract
        modern_key_hash = digest_d
        modern_effect = effect_instance_key(
            deployment, command["proposer_separation_subject"], modern_key_hash
        )
        modern_payload_hash = domain_digest(
            "okg-external-action-payload-v1", {"target": "T-1"}
        )
        modern_contract_digest = action_contract_digest(modern_contract)
        modern_content_hash = submission_content_hash(
            {
                "automation_id": command["automation_id"],
                "automation_version": command["automation_version"],
                "run_id": command["run_id"],
                "correlation_id": command["correlation_id"],
                "action_type": command["action_type"],
                "target_ref": command["target_ref"],
                "risk_tier": command["risk_tier"],
                "action_schema_digest": command["action_schema_digest"],
                "action_contract_digest": modern_contract_digest,
                "original_payload_hash": modern_payload_hash,
                "requested_review_expires_at": later,
                "summary_code": command["summary_code"],
                "evidence_digests": [digest],
            }
        )
        modern_request_hash = domain_digest(
            "okg-external-action-proposal-request-v1",
            {
                "deployment": deployment,
                "execution_epoch": command["execution_epoch"],
                "effect_instance_key": modern_effect,
                "submission_content_hash": modern_content_hash,
                "proposer_separation_subject": command["proposer_separation_subject"],
                "automation_id": command["automation_id"],
                "automation_version": command["automation_version"],
                "run_id": command["run_id"],
                "correlation_id": command["correlation_id"],
                "action_type": command["action_type"],
                "target_ref": command["target_ref"],
                "risk_tier": command["risk_tier"],
                "action_schema_digest": command["action_schema_digest"],
                "action_contract_digest": modern_contract_digest,
                "executor_contract_digest": command["executor_contract_digest"],
                "payload_renderer_digest": command["payload_renderer_digest"],
                "governed_tool_binding_digest": command["governed_tool_binding_digest"],
                "provider_connection_contract_digest": command["provider_connection_contract_digest"],
                "provider_capability_digest": command["provider_capability_digest"],
                "provider_operation_id": command["provider_operation_id"],
                "provider_operation_descriptor_digest": command["provider_operation_descriptor_digest"],
                "original_payload_hash": modern_payload_hash,
                "requested_review_expires_at": later,
                "summary_code": command["summary_code"],
                "evidence_digests": [digest],
            },
        )
        modern_audit_id = "audit-modern-" + uuid4().hex
        conn.execute("RESET ROLE")
        modern_provider_updated_at, modern_binding_updated_at = conn.execute(
            "SELECT p.updated_at, b.updated_at "
            "FROM okg.nomos_identity_providers p "
            "JOIN okg.nomos_subject_bindings b "
            "  ON b.deployment_name = p.deployment_name "
            " AND b.provider_id = p.provider_id AND b.issuer = p.issuer "
            "WHERE p.deployment_name = %s AND p.provider_id = %s "
            "  AND b.binding_id = %s",
            (deployment, provider_id, proposer_binding_id),
        ).fetchone()
        conn.execute("SET LOCAL ROLE okg_external_action_api")
        _set_verifier_capability(conn)
        modern_audit = {
            "deployment": deployment,
            "authentication_audit_id": modern_audit_id,
            "request_id": "request-" + modern_audit_id,
            "principal": "subject-proposer",
            "separation_subject": "subject-proposer",
            "nomos_provider_id": provider_id,
            "nomos_subject_binding_id": proposer_binding_id,
            "authentication_method_code": "jwt_dpop",
            "issuer_or_trust_domain_digest": issuer_digest,
            "auth_config_digest": digest,
            "result_code": "authenticated",
            "provider_status": "active",
            "binding_status": "active",
            "provider_updated_at": timestamp(modern_provider_updated_at),
            "binding_updated_at": timestamp(modern_binding_updated_at),
            "provider_credential_generation_digest": digest,
            "binding_credential_generation_digest": digest,
            "credential_generation_digest": digest,
            "presented_credential_generation_digest": digest,
            "token_issued_at": timestamp(
                max(modern_provider_updated_at, modern_binding_updated_at)
            ),
            "nomos_decision_id": proposer_nomos_id,
        }
        modern_assertion, modern_assertion_tag = _verifier_assertion(
            modern_audit,
            operation="external_action.propose",
            transition_digest=modern_request_hash,
            nomos_decision_id=proposer_nomos_id,
            policy_fingerprint=digest,
            provider_id=provider_id,
            provider_status="active",
            provider_updated_at=modern_audit["provider_updated_at"],
            binding_id=proposer_binding_id,
            binding_status="active",
            binding_updated_at=modern_audit["binding_updated_at"],
            nomos_input_hash=digest,
            authentication_audit_id=modern_audit_id,
        )
        modern_audit["verifier_assertion"] = modern_assertion
        modern_audit["verifier_assertion_tag"] = modern_assertion_tag
        conn.execute(
            "SELECT okg.external_action_record_auth_audit(%s::jsonb)",
            (j(modern_audit),),
        )
        modern_command = {
            **command,
            "proposal_id": "proposal-modern-" + uuid4().hex,
            "effect_instance_key": modern_effect,
            "submission_content_hash": modern_content_hash,
            "submission_request_hash": modern_request_hash,
            "submission_idempotency_key_hash": modern_key_hash,
            "action_contract_digest": modern_contract_digest,
            "original_payload_hash": modern_payload_hash,
            "current_payload_hash": modern_payload_hash,
            # The modern branch hashes the exact retained payload bytes.  The
            # legacy command above intentionally uses a different fixture
            # payload, so carry the canonical bytes for this branch explicitly.
            "canonical_payload": {"__okg_bytes_b64": "eyJ0YXJnZXQiOiJULTEifQ=="},
            "action_contract_record": modern_contract,
            "authentication_audit_id": modern_audit_id,
        }
        conn.execute("SAVEPOINT unverified_semantic_submit")
        conn.execute(
            "SELECT set_config('okg.external_action_verifier_capability', '', true)"
        )
        with pytest.raises(
            psycopg.errors.InvalidAuthorizationSpecification
        ) as unverified_submit:
            conn.execute(
                "SELECT okg.external_action_submit(%s::jsonb)",
                (j(modern_command),),
            )
        assert unverified_submit.value.sqlstate == "28000"
        conn.execute("ROLLBACK TO SAVEPOINT unverified_semantic_submit")
        conn.execute("SAVEPOINT forged_modern_contract_hash")
        with pytest.raises(psycopg.Error) as forged_contract_hash:
            conn.execute(
                "SELECT okg.external_action_submit(%s::jsonb)",
                (j({**modern_command, "action_contract_digest": digest_b}),),
            )
        assert forged_contract_hash.value.sqlstate == "22023"
        conn.execute("ROLLBACK TO SAVEPOINT forged_modern_contract_hash")

        conn.execute("SAVEPOINT forged_modern_payload_hash")
        with pytest.raises(psycopg.Error) as forged_payload_hash:
            conn.execute(
                "SELECT okg.external_action_submit(%s::jsonb)",
                (j({**modern_command, "original_payload_hash": digest_b}),),
            )
        assert forged_payload_hash.value.sqlstate == "22023"
        conn.execute("ROLLBACK TO SAVEPOINT forged_modern_payload_hash")

        conn.execute("SAVEPOINT forged_modern_request_hash")
        with pytest.raises(psycopg.Error) as forged_request_hash:
            conn.execute(
                "SELECT okg.external_action_submit(%s::jsonb)",
                (j({**modern_command, "submission_request_hash": digest_b}),),
            )
        # The authority snapshot binds the retained authentication evidence to
        # the request hash before semantic replay validation.
        assert forged_request_hash.value.sqlstate == "28000"
        conn.execute("ROLLBACK TO SAVEPOINT forged_modern_request_hash")

        conn.execute("SAVEPOINT forged_modern_effect_key")
        with pytest.raises(psycopg.Error) as forged_effect_key:
            conn.execute(
                "SELECT okg.external_action_submit(%s::jsonb)",
                (j({**modern_command, "effect_instance_key": digest_b}),),
            )
        assert forged_effect_key.value.sqlstate == "22023"
        conn.execute("ROLLBACK TO SAVEPOINT forged_modern_effect_key")

        modern_result = conn.execute(
            "SELECT okg.external_action_submit(%s::jsonb)",
            (j(modern_command),),
        ).fetchone()[0]
        assert modern_result["replay"] is False
        assert modern_result["proposal_id"] == modern_command["proposal_id"]

        transition = {
            "deployment": deployment,
            "proposal_id": proposal_id,
            "operation": "approve",
            "actor_principal": "subject-approver",
            "actor_separation_subject": "subject-approver",
            "idempotency_key_hash": digest,
            "transition_request_hash": digest,
            "authentication_audit_id": approver_audit_id,
            "nomos_decision_id": approver_nomos_id,
            "policy_digest": digest,
            "decision_id": decision_id,
            "execute_before": later,
        }
        transition["transition_request_hash"] = approve_transition_digest
        conn.execute("SAVEPOINT forged_transition_actor")
        with pytest.raises(psycopg.errors.InsufficientPrivilege) as forged_transition_actor:
            conn.execute(
                "SELECT okg.external_action_review_transition(%s::jsonb)",
                (j({**transition, "actor_separation_subject": "subject-forged"}),),
            )
        assert forged_transition_actor.value.sqlstate == "42501"
        conn.execute("ROLLBACK TO SAVEPOINT forged_transition_actor")

        conn.execute("SAVEPOINT forged_transition_principal")
        with pytest.raises(psycopg.errors.InsufficientPrivilege) as forged_transition_principal:
            conn.execute(
                "SELECT okg.external_action_review_transition(%s::jsonb)",
                (j({**transition, "actor_principal": "principal-forged"}),),
            )
        assert forged_transition_principal.value.sqlstate == "42501"
        conn.execute("ROLLBACK TO SAVEPOINT forged_transition_principal")

        conn.execute("SAVEPOINT supplied_decision_hash")
        with pytest.raises(psycopg.Error) as supplied_decision_error:
            conn.execute(
                "SELECT okg.external_action_review_transition(%s::jsonb)",
                (j({**transition, "decision_binding_hash": digest_c}),),
            )
        assert supplied_decision_error.value.sqlstate == "22023"
        conn.execute("ROLLBACK TO SAVEPOINT supplied_decision_hash")

        # A Nomos decision id is not a bearer capability.  Mutating the
        # decision subject between lookup and dispatch must reject the command
        # even though the retained audit and request actor still match.
        conn.execute("RESET ROLE")
        conn.execute("SAVEPOINT forged_nomos_subject")
        conn.execute(
            "UPDATE okg.nomos_decisions SET subject = 'subject-forged' "
            "WHERE deployment_name = %s AND decision_id = %s",
            (deployment, approver_nomos_id),
        )
        conn.execute("SET LOCAL ROLE okg_external_action_api")
        with pytest.raises(
            psycopg.errors.InvalidAuthorizationSpecification
        ) as forged_subject:
            conn.execute(
                "SELECT okg.external_action_review_transition(%s::jsonb)",
                (j(transition),),
            )
        assert forged_subject.value.sqlstate == "28000"
        conn.execute("ROLLBACK TO SAVEPOINT forged_nomos_subject")
        conn.execute("SET LOCAL ROLE okg_external_action_api")

        approved = conn.execute(
            "SELECT okg.external_action_review_transition(%s::jsonb)",
            (j(transition),),
        ).fetchone()[0]
        replay = conn.execute(
            "SELECT okg.external_action_review_transition(%s::jsonb)",
            (j(transition),),
        ).fetchone()[0]
        assert approved["review_state"] == "approved"
        assert approved["execution_state"] == "ready"
        assert approved["lifecycle_version"] == 2
        assert approved["payload_version"] == 1
        assert replay["replay"] is True, replay

        # Exact transition replay is still an authorization check.  A stale
        # idempotency row must not make a now-invalid Nomos decision replay.
        conn.execute("RESET ROLE")
        conn.execute("SAVEPOINT stale_transition_authority")
        conn.execute(
            "UPDATE okg.nomos_decisions SET subject = 'subject-forged' "
            "WHERE deployment_name = %s AND decision_id = %s",
            (deployment, approver_nomos_id),
        )
        conn.execute("SET LOCAL ROLE okg_external_action_api")
        with pytest.raises(
            psycopg.errors.InvalidAuthorizationSpecification
        ) as stale_replay:
            conn.execute(
                "SELECT okg.external_action_review_transition(%s::jsonb)",
                (j(transition),),
            )
        assert stale_replay.value.sqlstate == "28000"
        conn.execute("ROLLBACK TO SAVEPOINT stale_transition_authority")
        conn.execute("SET LOCAL ROLE okg_external_action_api")

        claim = {
            "deployment": deployment,
            "proposal_id": proposal_id,
            "execution_id": execution_id,
            "decision_id": decision_id,
            "worker_principal": "subject-worker",
            "worker_separation_subject": "subject-worker",
            "idempotency_key_hash": digest_b,
            "request_hash": claim_request_hash,
            "approved_lifecycle_version": 2,
            "approved_payload_version": 1,
            "approved_payload_hash": command_payload_hash,
            "supported_action_schema_digest": digest,
            "supported_action_contract_digest": command_contract_digest,
            "supported_executor_contract_digest": digest,
            "supported_payload_renderer_digest": digest,
            "supported_governed_tool_binding_digest": digest,
            "supported_provider_connection_contract_digest": digest,
            "supported_provider_capability_digest": digest,
            "supported_provider_operation_id": "provider-op",
            "supported_provider_operation_descriptor_digest": digest,
            "execution_epoch_lease_digest": lease_digest,
            "execution_binding_hash": digest_c,
            "provider_idempotency_key": "",
            "provider_idempotency_key_digest": "",
            "okg_claim_handoff_receipt_digest": claim_handoff_digest,
            "handoff_protected_b64": "protected",
            "handoff_payload_b64": "payload",
            "handoff_signature_b64": "signature",
            "authentication_audit_id": worker_claim_audit_id,
            "nomos_decision_id": worker_claim_nomos_id,
            "policy_digest": digest,
            "if_match": claim_if_match,
        }
        execution_binding_projection = {
            "deployment": deployment,
            "execution_epoch": 1,
            "execution_epoch_lease_digest": lease_digest,
            "effect_instance_key": command["effect_instance_key"],
            "proposal_id": proposal_id,
            "decision_id": decision_id,
            "execution_id": execution_id,
            "worker_separation_subject": "subject-worker",
            "approved_lifecycle_version": 2,
            "approved_payload_version": 1,
            "approved_payload_hash": command_payload_hash,
            "action_type": "ticket.update",
            "target_ref": "ticket:T-1",
            "action_schema_digest": digest,
            "action_contract_digest": command_contract_digest,
            "executor_contract_digest": digest,
            "payload_renderer_digest": digest,
            "governed_tool_binding_digest": digest,
            "provider_connection_contract_digest": digest,
            "provider_capability_digest": digest,
            "provider_operation_id": "provider-op",
            "provider_operation_descriptor_digest": digest,
            "policy_digest": digest,
        }
        provider_idempotency_projection = {
            "deployment": deployment,
            "effect_instance_key": command["effect_instance_key"],
            "approved_payload_hash": command_payload_hash,
            "action_contract_digest": command_contract_digest,
            "provider_connection_contract_digest": digest,
            "provider_operation_id": "provider-op",
            "provider_operation_descriptor_digest": digest,
        }
        conn.execute("RESET ROLE")
        execution_binding_hash = conn.execute(
            "SELECT okg.external_action_hash(%s, %s::jsonb)",
            ("okg-external-action-execution-binding-v1", j(execution_binding_projection)),
        ).fetchone()[0]
        provider_key, provider_digest = conn.execute(
            "SELECT okg.external_action_effect_key(%s::jsonb), "
            "okg.external_action_hash(%s, %s::jsonb)",
            (j(provider_idempotency_projection), "okg-external-action-effect-v1", j(provider_idempotency_projection)),
        ).fetchone()
        claim["execution_binding_hash"] = execution_binding_hash
        claim["provider_idempotency_key"] = provider_key
        claim["provider_idempotency_key_digest"] = provider_digest
        conn.execute("SET LOCAL ROLE okg_external_action_api")
        prepared_claim = conn.execute(
            "SELECT okg.external_action_prepare_claim(%s::jsonb)",
            (j({
                key: claim[key]
                for key in (
                    "deployment", "proposal_id", "execution_id", "decision_id",
                    "worker_principal", "worker_separation_subject",
                    "approved_lifecycle_version", "approved_payload_version",
                    "approved_payload_hash", "execution_epoch_lease_digest",
                    "execution_binding_hash", "request_hash",
                    "authentication_audit_id", "nomos_decision_id", "policy_digest",
                )
            }),),
        ).fetchone()[0]
        assert set(prepared_claim) == {
            "claimed_at", "previous_event_hash", "decision_event_hash",
            "claim_event_hash", "event_chain_head_hash",
            "claim_lifecycle_version", "event_sequence",
        }
        assert prepared_claim["claim_event_hash"].startswith("sha256:")
        assert prepared_claim["event_chain_head_hash"] == prepared_claim["claim_event_hash"]
        claim.update({
            "claimed_at": prepared_claim["claimed_at"],
            "previous_event_hash": prepared_claim["previous_event_hash"],
            "decision_event_hash": prepared_claim["decision_event_hash"],
            "claim_event_hash": prepared_claim["claim_event_hash"],
            "event_chain_head_hash": prepared_claim["event_chain_head_hash"],
            "claim_lifecycle_version": prepared_claim["claim_lifecycle_version"],
            "event_sequence": prepared_claim["event_sequence"],
            "event_hash": prepared_claim["claim_event_hash"],
        })
        conn.execute("SAVEPOINT missing_claim_support")
        missing_support = dict(claim)
        missing_support.pop("supported_action_contract_digest")
        with pytest.raises(psycopg.Error) as missing_claim_support:
            conn.execute(
                "SELECT okg.external_action_claim(%s::jsonb)",
                (j(missing_support),),
            )
        assert missing_claim_support.value.sqlstate == "22023"
        conn.execute("ROLLBACK TO SAVEPOINT missing_claim_support")

        conn.execute("SAVEPOINT mismatched_claim_support")
        with pytest.raises(psycopg.Error) as mismatched_claim_support:
            conn.execute(
                "SELECT okg.external_action_claim(%s::jsonb)",
                (j({**claim, "supported_action_contract_digest": digest_b}),),
            )
        assert mismatched_claim_support.value.sqlstate == "40001"
        conn.execute("ROLLBACK TO SAVEPOINT mismatched_claim_support")

        conn.execute("SAVEPOINT forged_claim_handoff")
        with pytest.raises(psycopg.Error) as forged_claim_handoff:
            conn.execute(
                "SELECT okg.external_action_claim(%s::jsonb)",
                (j({**claim, "handoff_signature_b64": "forged-signature"}),),
            )
        assert forged_claim_handoff.value.sqlstate == "22023"
        conn.execute("ROLLBACK TO SAVEPOINT forged_claim_handoff")

        conn.execute("SAVEPOINT forged_claim_actor")
        with pytest.raises(psycopg.errors.InsufficientPrivilege) as forged_claim_actor:
            conn.execute(
                "SELECT okg.external_action_claim(%s::jsonb)",
                (j({**claim, "worker_separation_subject": "subject-forged"}),),
            )
        assert forged_claim_actor.value.sqlstate == "42501"
        conn.execute("ROLLBACK TO SAVEPOINT forged_claim_actor")

        conn.execute("SAVEPOINT forged_claim_principal")
        with pytest.raises(psycopg.errors.InsufficientPrivilege) as forged_claim_principal:
            conn.execute(
                "SELECT okg.external_action_claim(%s::jsonb)",
                (j({**claim, "worker_principal": "principal-forged"}),),
            )
        assert forged_claim_principal.value.sqlstate == "42501"
        conn.execute("ROLLBACK TO SAVEPOINT forged_claim_principal")

        # The provider execution binding, key, and digest are all derived
        # from the locked proposal/decision.  A caller cannot substitute one
        # and still use the prepared claim envelope.
        conn.execute("SAVEPOINT supplied_execution_binding_hash")
        with pytest.raises(psycopg.Error) as supplied_execution_error:
            conn.execute(
                "SELECT okg.external_action_claim(%s::jsonb)",
                (j({**claim, "execution_binding_hash": digest_d}),),
            )
        assert supplied_execution_error.value.sqlstate == "22023"
        conn.execute("ROLLBACK TO SAVEPOINT supplied_execution_binding_hash")

        claimed = conn.execute(
            "SELECT okg.external_action_claim(%s::jsonb)", (j(claim),)
        ).fetchone()[0]
        claim_replay = conn.execute(
            "SELECT okg.external_action_claim(%s::jsonb)", (j(claim),)
        ).fetchone()[0]
        assert claimed["replay"] is False
        assert claimed["execution_state"] == "claimed"
        assert claimed["lifecycle_version"] == 3
        assert claim_replay["replay"] is True
        conn.execute("RESET ROLE")
        assert conn.execute(
            "SELECT provider_idempotency_key_digest, store_record->>'provider_idempotency_key' "
            "FROM okg.external_action_executions WHERE deployment_name = %s AND execution_id = %s",
            (deployment, execution_id),
        ).fetchone() == (provider_digest, provider_key)
        conn.execute("SET LOCAL ROLE okg_external_action_api")

        claim_conflict = {**claim, "request_hash": digest_c}
        conn.execute("SAVEPOINT claim_conflict")
        with pytest.raises(
            psycopg.errors.InvalidAuthorizationSpecification
        ) as claim_conflict_error:
            conn.execute(
                "SELECT okg.external_action_claim(%s::jsonb)", (j(claim_conflict),)
            )
        # The retained claim audit is bound to the original request hash;
        # authority freshness rejects a conflicting retry before idempotency.
        assert claim_conflict_error.value.sqlstate == "28000"
        conn.execute("ROLLBACK TO SAVEPOINT claim_conflict")

        # The owner-unavailability RPC retains only verifier-approved envelope
        # evidence.  Exercise its full SQL path here (not just the capability
        # gate) so timestamp derivation, subject separation, canonical hashes,
        # response bytes, and exact replay remain executable.
        conn.execute("RESET ROLE")
        claimed_at = conn.execute(
            "SELECT claimed_at FROM okg.external_action_executions "
            "WHERE deployment_name = %s AND execution_id = %s",
            (deployment, execution_id),
        ).fetchone()[0]
        reconcile_not_before = claimed_at + timedelta(seconds=1)
        database_now = conn.execute("SELECT clock_timestamp()").fetchone()[0]
        remaining_delay = (reconcile_not_before - database_now).total_seconds()
        if remaining_delay > 0:
            time.sleep(remaining_delay + 0.01)
        attestation_issued_at = conn.execute(
            "SELECT clock_timestamp()"
        ).fetchone()[0]
        attestation_expires_at = attestation_issued_at + timedelta(minutes=1)
        attestation_components = {
            "protected_b64": "attestation-protected",
            "payload_b64": "attestation-payload",
            "signature_b64": "attestation-signature",
        }
        attestation_digest = domain_digest(
            "okg-external-action-owner-unavailability-attestation-envelope-v1",
            attestation_components,
        )
        attestation_request_hash = domain_digest(
            "okg-external-action-transition-request-v1",
            attestation_components,
        )
        attestation_nomos_id = "nomos-" + uuid4().hex[:12]
        attestation_audit_id = "audit-" + uuid4().hex[:12]
        conn.execute(
            """
            INSERT INTO okg.nomos_decisions
              (deployment_name, decision_id, input_hash, subject, action,
               classification, effect, reason_code, enforcement_point,
               policy_fingerprint)
            VALUES (%s, %s, %s, 'subject-approver',
                    'external_action.attest_owner_unavailable',
                    'external_action', 'allow', 'test_allow', 'test', %s)
            """,
            (deployment, attestation_nomos_id, digest, digest),
        )
        provider_updated_at, binding_updated_at = conn.execute(
            "SELECT p.updated_at, b.updated_at "
            "FROM okg.nomos_identity_providers p "
            "JOIN okg.nomos_subject_bindings b "
            "  ON b.deployment_name = p.deployment_name "
            " AND b.provider_id = p.provider_id AND b.issuer = p.issuer "
            "WHERE p.deployment_name = %s AND p.provider_id = %s "
            "  AND b.binding_id = %s",
            (deployment, provider_id, approver_binding_id),
        ).fetchone()
        attestation_audit = {
            "deployment": deployment,
            "authentication_audit_id": attestation_audit_id,
            "request_id": "request-" + attestation_audit_id,
            "principal": "subject-approver",
            "separation_subject": "subject-approver",
            "nomos_provider_id": provider_id,
            "nomos_subject_binding_id": approver_binding_id,
            "authentication_method_code": "jwt_dpop",
            "issuer_or_trust_domain_digest": issuer_digest,
            "auth_config_digest": digest,
            "result_code": "authenticated",
            "provider_status": "active",
            "binding_status": "active",
            "provider_updated_at": timestamp(provider_updated_at),
            "binding_updated_at": timestamp(binding_updated_at),
            "provider_credential_generation_digest": digest,
            "binding_credential_generation_digest": digest,
            "credential_generation_digest": digest,
            "presented_credential_generation_digest": digest,
            "token_issued_at": timestamp(
                max(provider_updated_at, binding_updated_at)
            ),
            "nomos_decision_id": attestation_nomos_id,
        }
        attestation_assertion, attestation_assertion_tag = _verifier_assertion(
            attestation_audit,
            operation="external_action.attest_owner_unavailable",
            transition_digest=attestation_request_hash,
            nomos_decision_id=attestation_nomos_id,
            policy_fingerprint=digest,
            provider_id=provider_id,
            provider_status="active",
            provider_updated_at=attestation_audit["provider_updated_at"],
            binding_id=approver_binding_id,
            binding_status="active",
            binding_updated_at=attestation_audit["binding_updated_at"],
            nomos_input_hash=digest,
            authentication_audit_id=attestation_audit_id,
        )
        attestation_audit["verifier_assertion"] = attestation_assertion
        attestation_audit["verifier_assertion_tag"] = attestation_assertion_tag
        conn.execute(
            "SELECT okg.external_action_record_auth_audit(%s::jsonb)",
            (j(attestation_audit),),
        )
        attestation_response = {
            **attestation_components,
            "envelope_digest": attestation_digest,
        }
        attestation_command = {
            "deployment": deployment,
            "execution_id": execution_id,
            "claim_owner_separation_subject": "subject-worker",
            "reconcile_not_before": timestamp(reconcile_not_before),
            "issued_at": timestamp(attestation_issued_at),
            "expires_at": timestamp(attestation_expires_at),
            "nonce": "attestation-nonce-" + uuid4().hex,
            "signer_separation_subject": "subject-approver",
            "authenticated_submitter_separation_subject": "subject-approver",
            "idempotency_key_hash": digest_c,
            "request_hash": attestation_request_hash,
            "response_status": 201,
            "response_body_jcs": canonical_json_bytes(
                attestation_response
            ).decode("utf-8"),
            **attestation_components,
            "signer_key_id_digest": digest_d,
            "envelope_digest": attestation_digest,
            "authentication_audit_id": attestation_audit_id,
            "nomos_decision_id": attestation_nomos_id,
        }
        conn.execute("SET LOCAL ROLE okg_external_action_api")
        attestation_result = conn.execute(
            "SELECT okg.external_action_owner_unavailability(%s::jsonb)",
            (j(attestation_command),),
        ).fetchone()[0]
        attestation_replay = conn.execute(
            "SELECT okg.external_action_owner_unavailability(%s::jsonb)",
            (j(attestation_command),),
        ).fetchone()[0]
        assert attestation_result["replay"] is False
        assert attestation_result["envelope_digest"] == attestation_digest
        assert attestation_replay["replay"] is True

        outcome = {
            "deployment": deployment,
            "execution_id": execution_id,
            "effect_instance_key": command["effect_instance_key"],
            "worker_or_reconciler_separation_subject": "subject-worker",
            "outcome": "succeeded",
            "outcome_reason_code": "provider_receipt_observed",
            "provider_receipt_digest": digest_c,
            "recovery_journal_digest": digest_d,
            "outcome_request_hash": canonical_outcome_request_hash,
            "idempotency_key_hash": digest_d,
            "authentication_audit_id": worker_outcome_audit_id,
            "nomos_decision_id": worker_outcome_nomos_id,
            "policy_digest": digest,
            "reconciler": False,
            "if_match": outcome_if_match,
            "expected_lifecycle_version": 3,
            "expected_payload_version": 1,
            "expected_payload_hash": command_payload_hash,
        }
        conn.execute("SAVEPOINT missing_outcome_policy")
        missing_outcome_policy = dict(outcome)
        missing_outcome_policy.pop("policy_digest")
        with pytest.raises(psycopg.Error) as missing_outcome_policy_error:
            conn.execute(
                "SELECT okg.external_action_report_outcome(%s::jsonb)",
                (j(missing_outcome_policy),),
            )
        assert missing_outcome_policy_error.value.sqlstate == "22023"
        conn.execute("ROLLBACK TO SAVEPOINT missing_outcome_policy")

        conn.execute("SAVEPOINT mismatched_outcome_policy")
        with pytest.raises(psycopg.Error) as mismatched_outcome_policy:
            conn.execute(
                "SELECT okg.external_action_report_outcome(%s::jsonb)",
                (j({**outcome, "policy_digest": digest_b}),),
            )
        assert mismatched_outcome_policy.value.sqlstate == "40001"
        conn.execute("ROLLBACK TO SAVEPOINT mismatched_outcome_policy")

        conn.execute("SAVEPOINT forged_outcome_actor")
        with pytest.raises(psycopg.errors.InsufficientPrivilege) as forged_outcome_actor:
            conn.execute(
                "SELECT okg.external_action_report_outcome(%s::jsonb)",
                (j({**outcome, "worker_or_reconciler_separation_subject": "subject-forged"}),),
            )
        assert forged_outcome_actor.value.sqlstate == "42501"
        conn.execute("ROLLBACK TO SAVEPOINT forged_outcome_actor")

        conn.execute("SAVEPOINT forged_outcome_event_hash")
        with pytest.raises(psycopg.Error) as forged_outcome_event_hash:
            conn.execute(
                "SELECT okg.external_action_report_outcome(%s::jsonb)",
                (j({**outcome, "event_hash": digest_b}),),
            )
        assert forged_outcome_event_hash.value.sqlstate == "22023"
        conn.execute("ROLLBACK TO SAVEPOINT forged_outcome_event_hash")

        reported = conn.execute(
            "SELECT okg.external_action_report_outcome(%s::jsonb)", (j(outcome),)
        ).fetchone()[0]
        outcome_replay = conn.execute(
            "SELECT okg.external_action_report_outcome(%s::jsonb)", (j(outcome),)
        ).fetchone()[0]
        assert reported["replay"] is False
        assert reported["outcome"] == "succeeded"
        assert outcome_replay["replay"] is True

        outcome_conflict = {**outcome, "outcome_request_hash": digest_b}
        conn.execute("SAVEPOINT outcome_conflict")
        with pytest.raises(
            psycopg.errors.InvalidAuthorizationSpecification
        ) as outcome_conflict_error:
            conn.execute(
                "SELECT okg.external_action_report_outcome(%s::jsonb)",
                (j(outcome_conflict),),
            )
        # The retained outcome audit is likewise bound to its request hash.
        assert outcome_conflict_error.value.sqlstate == "28000"
        conn.execute("ROLLBACK TO SAVEPOINT outcome_conflict")

        conn.execute("RESET ROLE")
        conn.execute("SET LOCAL ROLE okg_external_action_api")
        claim_owner_status = conn.execute(
            "SELECT okg.external_action_claim_owner_unavailability(%s)",
            (execution_id,),
        ).fetchone()[0]
        assert set(claim_owner_status) == {
            "authentication_audit_id", "unavailable", "updated_at", "active_replacement"
        }
        assert claim_owner_status["authentication_audit_id"] == worker_claim_audit_id
        assert claim_owner_status["unavailable"] is False
        assert claim_owner_status["active_replacement"] is False
        conn.execute("RESET ROLE")
        rows = conn.execute(
            """
            SELECT event_hash, store_record->'event_projection', event_type,
                   effective_at, actor_kind
              FROM okg.external_action_events
             WHERE deployment_name = %s AND effect_instance_key = %s
             ORDER BY sequence
            """,
            (deployment, command["effect_instance_key"]),
        ).fetchall()
        assert len(rows) == 4
        closed_fields = {
            "proposal_id", "execution_epoch", "effect_instance_key", "sequence",
            "lifecycle_version", "event_type", "actor_kind", "actor_principal",
            "actor_separation_subject", "authentication_audit_ref", "nomos_decision_ref",
            "deadline_kind", "policy_digest", "before_review_state", "after_review_state",
            "before_execution_state", "after_execution_state", "payload_version", "payload_hash",
            "decision_id", "decision_binding_hash", "execution_id", "execution_binding_hash",
            "transition_reason_code", "outcome", "outcome_reason_code", "outcome_request_hash",
            "restore_recovery_digest", "effective_at", "recorded_at", "previous_event_hash",
        }
        for event_hash, projection, event_type, effective_at, actor_kind in rows:
            assert event_hash.startswith("sha256:") and len(event_hash) == len("sha256:") + 64
            assert set(projection) == closed_fields
            recomputed = conn.execute(
                "SELECT okg.external_action_hash('okg-external-action-event-v1', %s::jsonb)",
                (j(projection),),
            ).fetchone()[0]
            assert recomputed == event_hash
            if actor_kind == "authenticated_principal":
                assert effective_at is None
            assert projection["event_type"] == event_type

        decision_hash = conn.execute(
            "SELECT decision_binding_hash FROM okg.external_action_decisions WHERE deployment_name = %s AND decision_id = %s",
            (deployment, decision_id),
        ).fetchone()[0]
        assert decision_hash.startswith("sha256:") and len(decision_hash) == len("sha256:") + 64


def test_postgres_nomos_bundle_barrier_rejects_stale_and_racing_mutation(
    okg_migrated: str,
) -> None:
    """Nomos reads and decision writes share one deployment authority fence."""

    deployment = "external-action-nomos-" + uuid4().hex[:12]
    provider_id = "provider-" + uuid4().hex[:12]
    binding_id = "binding-" + uuid4().hex[:12]
    scope = "external_action"
    digest = "sha256:" + ("c" * 64)
    issuer = "https://issuer.example"

    def j(value: object) -> str:
        return json.dumps(value, separators=(",", ":"))

    with db_connect(okg_migrated) as conn:
        _bind_verifier_role(conn, deployment)
        _set_verifier_capability(conn)
        conn.execute(
            """
            INSERT INTO okg.nomos_identity_providers
              (deployment_name, provider_id, provider_type, issuer,
               subject_claim, status, metadata)
            VALUES (%s, %s, 'oidc', %s, 'sub', 'active', %s::jsonb)
            """,
            (
                deployment,
                provider_id,
                issuer,
                json.dumps({"credential_generation_digest": digest}),
            ),
        )
        conn.execute(
            """
            INSERT INTO okg.nomos_subject_bindings
              (deployment_name, binding_id, provider_id, issuer,
               claim_path, claim_value, subject, scope, status, metadata)
            VALUES (%s, %s, %s, %s, 'sub', 'subject', 'subject', %s, 'active', %s::jsonb)
            """,
            (
                deployment,
                binding_id,
                provider_id,
                issuer,
                scope,
                j({"credential_generation_digest": digest}),
            ),
        )
        conn.execute(
            """
            INSERT INTO okg.nomos_policy_bundles
              (deployment_name, policy_bundle_id, policy_version,
               policy_fingerprint, compiler_version, scope, default_effect,
               bundle, status)
            VALUES (%s, 'bundle-test', '1', %s, 'test-compiler', %s,
                    'deny', %s, 'active')
            """,
            (
                deployment,
                digest,
                json.dumps({"scope": scope}),
                json.dumps(
                    {
                        "policy_bundle_id": "bundle-test",
                        "policy_version": "1",
                        "policy_fingerprint": digest,
                        "compiler_version": "test-compiler",
                        "default_effect": "deny",
                        "compiled_at": "2026-01-01T00:00:00Z",
                        "scope": {"scope": scope},
                        "rules": [
                            {
                                "rule_id": "test_allow",
                                "effect": "allow",
                                "actions": ["external_action.propose"],
                                "subjects": ["subject"],
                                "match": {},
                                "priority": 0,
                                "reason_code": "test_allow",
                                "obligations": [],
                            }
                        ],
                    }
                ),
            ),
        )
        conn.execute(
            """
            INSERT INTO okg.nomos_policy_activations
              (deployment_name, activation_id, scope, policy_fingerprint,
               activated_by, activation_decision_id)
            VALUES (%s, 'activation-test', %s, %s, 'operator', 'decision-test')
            """,
            (deployment, scope, digest),
        )
        conn.commit()

        conn.execute("SAVEPOINT immutable_policy_fingerprint")
        with pytest.raises(psycopg.errors.InvalidParameterValue) as immutable:
            conn.execute(
                """
                UPDATE okg.nomos_policy_bundles
                   SET bundle = jsonb_set(bundle, '{default_effect}', '"allow"')
                 WHERE deployment_name = %s AND policy_fingerprint = %s
                """,
                (deployment, digest),
            )
        assert immutable.value.sqlstate == "22023"
        conn.execute("ROLLBACK TO SAVEPOINT immutable_policy_fingerprint")

        conn.execute("SET LOCAL ROLE okg_external_action_api")
        _set_verifier_capability(conn)
        authority = conn.execute(
            """
            SELECT okg.external_action_nomos_policy_bundle(%s, %s, %s)
            """,
            (scope, provider_id, binding_id),
        ).fetchone()[0]
        assert authority["deployment"] == deployment
        assert authority["policy_fingerprint"] == digest
        assert authority["provider_status"] == "active"
        assert authority["binding_status"] == "active"
        assert authority["provider_updated_at"]
        assert authority["binding_updated_at"]

        def canonical_timestamp(value: object) -> str:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            return parsed.astimezone(UTC).isoformat(timespec="microseconds").replace(
                "+00:00", "Z"
            )

        input_projection = {
            "subject": "subject",
            "action": "external_action.propose",
            "resource": {},
            "classification": "external_action",
            "context": {},
        }
        input_hash = "sha256:" + hashlib.sha256(
            canonical_json_bytes(input_projection)
        ).hexdigest()
        decision = {
            "deployment": deployment,
            "decision_id": "nomos-decision-" + uuid4().hex,
            "policy_bundle_id": authority["policy_bundle_id"],
            "policy_version": authority["policy_version"],
            "policy_fingerprint": authority["policy_fingerprint"],
            "input_hash": input_hash,
            "subject": "subject",
            "action": "external_action.propose",
            "resource": {},
            "classification": "external_action",
            "context": {},
            "effect": "allow",
            "reason_code": "test_allow",
            "matched_rule_ids": ["test_allow"],
            "obligations": [],
            "enforcement_point": "test",
            "target": {},
            "policy_scope": authority["scope"],
            "provider_id": authority["provider_id"],
            "binding_id": authority["binding_id"],
            "provider_status": authority["provider_status"],
            "provider_updated_at": authority["provider_updated_at"],
            "binding_status": authority["binding_status"],
            "binding_updated_at": authority["binding_updated_at"],
            "allowed": True,
            "final_effect": "allow",
            "required_obligations": [],
            "completed_obligations": [],
            "missing_obligations": [],
        }
        decision_assertion, decision_assertion_tag = _verifier_assertion(
            {"deployment": deployment},
            operation="external_action.propose",
            transition_digest=domain_digest(
                "okg-test-nomos-transition-v1", decision["decision_id"]
            ),
            nomos_decision_id=decision["decision_id"],
            policy_fingerprint=digest,
            provider_id=provider_id,
            provider_status="active",
            provider_updated_at=canonical_timestamp(authority["provider_updated_at"]),
            binding_id=binding_id,
            binding_status="active",
            binding_updated_at=canonical_timestamp(authority["binding_updated_at"]),
            nomos_input_hash=input_hash,
        )
        decision["verifier_assertion"] = decision_assertion
        decision["verifier_assertion_tag"] = decision_assertion_tag
        persisted = conn.execute(
            "SELECT okg.external_action_nomos_persist_decision(%s::jsonb)",
            (j(decision),),
        ).fetchone()[0]
        assert persisted["replay"] is False
        replayed = conn.execute(
            "SELECT okg.external_action_nomos_persist_decision(%s::jsonb)",
            (j(decision),),
        ).fetchone()[0]
        assert replayed["replay"] is True
        conn.execute("SAVEPOINT conflicting_nomos_replay")
        with pytest.raises(psycopg.errors.SerializationFailure) as conflicting_replay:
            conn.execute(
                "SELECT okg.external_action_nomos_persist_decision(%s::jsonb)",
                (j({**decision, "target": {"different": True}}),),
            )
        assert conflicting_replay.value.sqlstate == "40001"
        conn.execute("ROLLBACK TO SAVEPOINT conflicting_nomos_replay")
        # Auth evidence may be persisted after the Nomos decision.  The
        # decision id is an append-only FK, not a later audit update.
        provider_barrier = canonical_timestamp(authority["provider_updated_at"])
        binding_barrier = canonical_timestamp(authority["binding_updated_at"])
        audit = {
            "deployment": deployment,
            "authentication_audit_id": "audit-after-" + uuid4().hex,
            "request_id": "request-after-" + uuid4().hex,
            "principal": "subject",
            "separation_subject": "subject",
            "nomos_provider_id": provider_id,
            "nomos_subject_binding_id": binding_id,
            "authentication_method_code": "jwt_dpop",
            "issuer_or_trust_domain_digest": "sha256:"
            + hashlib.sha256(issuer.encode()).hexdigest(),
            "auth_config_digest": digest,
            "result_code": "authenticated",
            "provider_status": authority["provider_status"],
            "binding_status": authority["binding_status"],
            "provider_updated_at": provider_barrier,
            "binding_updated_at": binding_barrier,
            "provider_credential_generation_digest": digest,
            "binding_credential_generation_digest": digest,
            "credential_generation_digest": digest,
            "presented_credential_generation_digest": digest,
            "token_issued_at": max(provider_barrier, binding_barrier),
            "nomos_decision_id": decision["decision_id"],
        }
        audit_assertion, audit_assertion_tag = _verifier_assertion(
            audit,
            operation="external_action.propose",
            transition_digest=input_hash,
            nomos_decision_id=decision["decision_id"],
            policy_fingerprint=digest,
            provider_id=provider_id,
            provider_status=authority["provider_status"],
            provider_updated_at=provider_barrier,
            binding_id=binding_id,
            binding_status=authority["binding_status"],
            binding_updated_at=binding_barrier,
            nomos_input_hash=input_hash,
            authentication_audit_id=audit["authentication_audit_id"],
        )
        audit["verifier_assertion"] = audit_assertion
        audit["verifier_assertion_tag"] = audit_assertion_tag
        recorded_audit = conn.execute(
            "SELECT okg.external_action_record_auth_audit(%s::jsonb)",
            (j(audit),),
        ).fetchone()[0]
        assert recorded_audit["nomos_decision_id"] == decision["decision_id"]

        conn.execute("RESET ROLE")
        conn.commit()
        conn.execute(
            """
            UPDATE okg.nomos_identity_providers
               SET metadata = '{"rotation":1}'::jsonb
             WHERE deployment_name = %s AND provider_id = %s
            """,
            (deployment, provider_id),
        )
        conn.commit()
        conn.execute("SET LOCAL ROLE okg_external_action_api")
        _set_verifier_capability(conn)
        with pytest.raises(psycopg.errors.SerializationFailure) as stale:
            conn.execute(
                "SELECT okg.external_action_nomos_persist_decision(%s::jsonb)",
                (j(decision),),
            )
        assert stale.value.sqlstate == "40001"
        conn.rollback()

        conn.execute("RESET ROLE")
        _set_verifier_capability(conn)
        fresh_authority = conn.execute(
            """
            SELECT okg.external_action_nomos_policy_bundle(%s, %s, %s)
            """,
            (scope, provider_id, binding_id),
        ).fetchone()[0]
        racing_decision = dict(decision)
        racing_decision["decision_id"] = "nomos-racing-" + uuid4().hex
        racing_decision.update(
            {
                "provider_updated_at": fresh_authority["provider_updated_at"],
                "binding_updated_at": fresh_authority["binding_updated_at"],
            }
        )
        conn.commit()

    with db_connect(okg_migrated) as holder:
        holder.execute("BEGIN")
        holder.execute(
            """
            UPDATE okg.nomos_identity_providers
               SET metadata = '{"rotation":2}'::jsonb
             WHERE deployment_name = %s AND provider_id = %s
            """,
            (deployment, provider_id),
        )
        result: dict[str, object] = {}

        def racing_call() -> None:
            try:
                with db_connect(okg_migrated) as contender:
                    contender.execute("SET LOCAL ROLE okg_external_action_api")
                    _set_verifier_capability(contender)
                    contender.execute(
                        "SELECT okg.external_action_nomos_persist_decision(%s::jsonb)",
                        (j(racing_decision),),
                    )
            except psycopg.Error as exc:
                result["sqlstate"] = exc.sqlstate

        thread = threading.Thread(target=racing_call)
        thread.start()
        time.sleep(0.2)
        assert thread.is_alive()
        holder.commit()
        thread.join(timeout=5)
        assert not thread.is_alive()
        assert result["sqlstate"] == "40001"


def test_postgres_startup_lease_and_database_clock_expiry_are_durable(
    okg_migrated: str,
) -> None:
    """The pre-bind lease suffix and read-side expiry use fixed SQL RPCs."""

    deployment = "external-action-startup-" + uuid4().hex[:12]
    digest = "sha256:" + ("d" * 64)
    lease_digest = "lease-" + uuid4().hex
    proposal_id = "proposal-expiring-" + uuid4().hex
    effect_key = "effect-expiring-" + uuid4().hex

    def j(value: object) -> str:
        return json.dumps(value, separators=(",", ":"))

    now = datetime.now(UTC)
    with db_connect(okg_migrated) as conn:
        _bind_verifier_role(conn, deployment)
        _set_verifier_capability(conn)
        lease = {
            "deployment": deployment,
            "epoch": 0,
            "lease_sequence": 1,
            "previous_lease_digest": None,
            "previous_epoch_digest": None,
            "issued_at": (now - timedelta(minutes=1)).isoformat(),
            "expires_at": (now + timedelta(hours=1)).isoformat(),
            "signer_separation_subject": "startup-signer",
            "nonce": "nonce-" + uuid4().hex,
            "protected_b64": "cHJvdGVjdGVk",
            "payload_b64": "cGF5bG9hZA",
            "signature_b64": "c2lnbmF0dXJl",
            "signer_key_id_digest": "sha256:" + ("e" * 64),
            "envelope_digest": lease_digest,
            "verification_source": "signed_startup_gate",
            "verification_config_digest": digest,
            "verification_login_binding_digest": digest,
            "lease_chain_digest": "chain-" + uuid4().hex,
        }
        conn.execute("SET LOCAL ROLE okg_external_action_api")
        first = conn.execute(
            "SELECT okg.external_action_store_insert_lease(%s::jsonb)",
            (j(lease),),
        ).fetchone()[0]
        replay = conn.execute(
            "SELECT okg.external_action_store_insert_lease(%s::jsonb)",
            (j(lease),),
        ).fetchone()[0]
        assert first["replay"] is False
        assert replay["replay"] is True

        # Startup acceptance derives mode and quarantine from the installed
        # lease chain.  Re-presenting that exact lease is a true no-op: a
        # restart must not rewrite the acceptance timestamp or store record.
        startup_state = {
            "deployment": deployment,
            "highest_accepted_epoch": lease["epoch"],
            "lease_digest": lease_digest,
            "service_mode": "restore_only",
            "quarantine_state": "blocked",
            "lease_chain_digest": lease["lease_chain_digest"],
            "verification_config_digest": digest,
            "verification_login_binding_digest": digest,
        }
        first_startup = conn.execute(
            "SELECT okg.external_action_store_startup(%s::jsonb)",
            (j(startup_state),),
        ).fetchone()[0]
        assert first_startup == {
            "replay": False,
            "accepted": True,
            "deployment": deployment,
            "service_mode": "normal",
            "quarantine_state": "clear",
            "highest_accepted_epoch": lease["epoch"],
        }
        conn.execute("RESET ROLE")
        accepted_at, store_record = conn.execute(
            "SELECT accepted_at, store_record "
            "FROM okg.external_action_restore_fences WHERE deployment_name = %s",
            (deployment,),
        ).fetchone()
        conn.execute("SET LOCAL ROLE okg_external_action_api")
        replay_startup = conn.execute(
            "SELECT okg.external_action_store_startup(%s::jsonb)",
            (j(startup_state),),
        ).fetchone()[0]
        assert replay_startup == {
            "replay": True,
            "accepted": True,
            "deployment": deployment,
            "service_mode": "normal",
            "quarantine_state": "clear",
            "highest_accepted_epoch": lease["epoch"],
        }
        conn.execute("RESET ROLE")
        assert conn.execute(
            "SELECT accepted_at, store_record "
            "FROM okg.external_action_restore_fences WHERE deployment_name = %s",
            (deployment,),
        ).fetchone() == (accepted_at, store_record)
        conn.execute("SET LOCAL ROLE okg_external_action_api")
        conflict = dict(lease)
        conflict["signer_key_id_digest"] = "sha256:" + ("0" * 64)
        conn.execute("SAVEPOINT lease_replay_conflict")
        with pytest.raises(psycopg.errors.SerializationFailure) as raised:
            conn.execute(
                "SELECT okg.external_action_store_insert_lease(%s::jsonb)",
                (j(conflict),),
            )
        assert raised.value.sqlstate == "40001"
        conn.execute("ROLLBACK TO SAVEPOINT lease_replay_conflict")
        conn.execute("RELEASE SAVEPOINT lease_replay_conflict")
        conn.execute("RESET ROLE")
        assert conn.execute(
            "SELECT epoch, lease_sequence, envelope_digest "
            "FROM okg.external_action_execution_epoch_leases "
            "WHERE deployment_name = %s",
            (deployment,),
        ).fetchone() == (0, 1, lease_digest)

        # A same-epoch renewal is a new immutable suffix.  It must link to
        # the prior envelope, become the sole current lease for new work, and
        # keep the existing normal/clear mode.  Replaying the new startup
        # command is then a no-op, while presenting the superseded lease is a
        # stale startup fence rather than an authorization downgrade.
        renewed_lease_digest = "lease-" + uuid4().hex
        renewed_chain_digest = "chain-" + uuid4().hex
        renewed = {
            **lease,
            "lease_sequence": 2,
            "previous_lease_digest": lease_digest,
            "envelope_digest": renewed_lease_digest,
            "lease_chain_digest": renewed_chain_digest,
            "nonce": "nonce-" + uuid4().hex,
        }
        conn.execute("SET LOCAL ROLE okg_external_action_api")
        renewed_result = conn.execute(
            "SELECT okg.external_action_store_insert_lease(%s::jsonb)",
            (j(renewed),),
        ).fetchone()[0]
        assert renewed_result == {
            "replay": False,
            "deployment": deployment,
            "epoch": 0,
            "lease_sequence": 2,
            "envelope_digest": renewed_lease_digest,
            "lease_chain_digest": renewed_chain_digest,
        }
        renewed_state = {
            **startup_state,
            "lease_digest": renewed_lease_digest,
            "lease_chain_digest": renewed_chain_digest,
        }
        renewed_startup = conn.execute(
            "SELECT okg.external_action_store_startup(%s::jsonb)",
            (j(renewed_state),),
        ).fetchone()[0]
        assert renewed_startup == {
            "replay": True,
            "accepted": True,
            "deployment": deployment,
            "service_mode": "normal",
            "quarantine_state": "clear",
            "highest_accepted_epoch": 0,
        }
        conn.execute("RESET ROLE")
        renewed_accepted_at, renewed_store_record = conn.execute(
            "SELECT accepted_at, store_record "
            "FROM okg.external_action_restore_fences WHERE deployment_name = %s",
            (deployment,),
        ).fetchone()
        assert renewed_accepted_at >= accepted_at
        assert renewed_store_record != store_record
        conn.execute("SET LOCAL ROLE okg_external_action_api")
        assert conn.execute(
            "SELECT okg.external_action_store_startup(%s::jsonb)",
            (j(renewed_state),),
        ).fetchone()[0] == renewed_startup
        conn.execute("RESET ROLE")
        assert conn.execute(
            "SELECT accepted_at, store_record "
            "FROM okg.external_action_restore_fences WHERE deployment_name = %s",
            (deployment,),
        ).fetchone() == (renewed_accepted_at, renewed_store_record)
        conn.execute("SET LOCAL ROLE okg_external_action_api")
        conn.execute("SAVEPOINT stale_startup_lease")
        with pytest.raises(psycopg.errors.SerializationFailure) as stale_startup:
            conn.execute(
                "SELECT okg.external_action_store_startup(%s::jsonb)",
                (j(startup_state),),
            )
        assert stale_startup.value.sqlstate == "40001"
        conn.execute("ROLLBACK TO SAVEPOINT stale_startup_lease")
        conn.execute("RELEASE SAVEPOINT stale_startup_lease")
        conn.execute("RESET ROLE")
        assert conn.execute(
            "SELECT epoch, lease_sequence, envelope_digest, previous_lease_digest "
            "FROM okg.external_action_execution_epoch_leases "
            "WHERE deployment_name = %s ORDER BY lease_sequence",
            (deployment,),
        ).fetchall() == [
            (0, 1, lease_digest, None),
            (0, 2, renewed_lease_digest, lease_digest),
        ]

        expired = _proposal_row(deployment, proposal_id, effect_key)
        expired = (*expired[:25], now - timedelta(hours=2), now - timedelta(hours=1), *expired[27:])
        conn.execute(
            """
            INSERT INTO okg.external_action_proposals (
              proposal_id, deployment_name, execution_epoch, effect_instance_key,
              submission_content_hash, submission_request_hash,
              submission_idempotency_key_hash, proposer_separation_subject,
              automation_id, automation_version, run_id, correlation_id,
              action_type, target_ref, risk_tier, action_schema_digest,
              action_contract_digest, executor_contract_digest,
              payload_renderer_digest, governed_tool_binding_digest,
              provider_connection_contract_digest, provider_capability_digest,
              provider_operation_id, provider_operation_descriptor_digest,
              original_payload_hash, requested_review_expires_at,
              effective_review_expires_at, summary_code, current_payload_hash
            ) VALUES (
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            expired,
        )
        conn.commit()
        _set_verifier_capability(conn)
        conn.execute("SET LOCAL ROLE okg_external_action_api")
        materialized = conn.execute(
            "SELECT okg.external_action_materialize_expiry(%s)",
            (proposal_id,),
        ).fetchone()[0]
        materialized_replay = conn.execute(
            "SELECT okg.external_action_materialize_expiry(%s)",
            (proposal_id,),
        ).fetchone()[0]
        assert materialized["expired"] is True
        assert materialized["replay"] is False
        assert materialized_replay["replay"] is True
        conn.execute("RESET ROLE")
        assert conn.execute(
            "SELECT review_state, execution_state, count(*) OVER () "
            "FROM okg.external_action_proposals p "
            "JOIN okg.external_action_events e USING (deployment_name, effect_instance_key) "
            "WHERE p.deployment_name = %s AND p.proposal_id = %s "
            "AND e.event_type = 'review_expired'",
            (deployment, proposal_id),
        ).fetchone() == ("expired", "unavailable", 1)


def test_postgres_authenticated_lease_requires_authority_and_is_durable(
    okg_migrated: str,
) -> None:
    """The authenticated API lease mode stores refs, but no chain digest."""

    deployment = "external-action-auth-lease-" + uuid4().hex[:12]
    provider_id = "provider-" + uuid4().hex[:12]
    binding_id = "binding-" + uuid4().hex[:12]
    decision_id = "nomos-" + uuid4().hex[:12]
    audit_id = "audit-" + uuid4().hex[:12]
    lease_digest = "sha256:" + ("e" * 64)
    digest = "sha256:" + ("f" * 64)
    issuer = "https://issuer.example"

    def j(value: object) -> str:
        return json.dumps(value, separators=(",", ":"))

    now = datetime.now(UTC)
    with db_connect(okg_migrated) as conn:
        _bind_verifier_role(conn, deployment)
        _set_verifier_capability(conn)
        conn.execute(
            """
            INSERT INTO okg.nomos_identity_providers
              (deployment_name, provider_id, provider_type, issuer,
               subject_claim, status, metadata)
            VALUES (%s, %s, 'oidc', %s, 'sub', 'active', %s::jsonb)
            """,
            (
                deployment,
                provider_id,
                issuer,
                json.dumps({"credential_generation_digest": digest}),
            ),
        )
        conn.execute(
            """
            INSERT INTO okg.nomos_subject_bindings
              (deployment_name, binding_id, provider_id, issuer,
               claim_path, claim_value, subject, scope, status, metadata)
            VALUES (%s, %s, %s, %s, 'sub', 'subject', 'subject',
                    'external_action', 'active', %s::jsonb)
            """,
            (
                deployment,
                binding_id,
                provider_id,
                issuer,
                json.dumps({"credential_generation_digest": digest}),
            ),
        )
        conn.execute(
            """
            INSERT INTO okg.nomos_decisions
              (deployment_name, decision_id, input_hash, subject, action,
               classification, effect, reason_code, enforcement_point,
               policy_fingerprint)
            VALUES (%s, %s, %s, 'subject', 'external_action.execute',
                    'external_action', 'allow', 'test_allow', 'test', %s)
            """,
            (deployment, decision_id, digest, digest),
        )
        conn.commit()

        provider_updated_at, binding_updated_at = conn.execute(
            "SELECT p.updated_at, b.updated_at "
            "FROM okg.nomos_identity_providers p "
            "JOIN okg.nomos_subject_bindings b "
            "  ON b.deployment_name = p.deployment_name "
            " AND b.provider_id = p.provider_id "
            " AND b.issuer = p.issuer "
            "WHERE p.deployment_name = %s AND p.provider_id = %s "
            "  AND b.binding_id = %s",
            (deployment, provider_id, binding_id),
        ).fetchone()
        provider_updated_at_value = provider_updated_at.astimezone(UTC).isoformat(
            timespec="microseconds"
        ).replace("+00:00", "Z")
        binding_updated_at_value = binding_updated_at.astimezone(UTC).isoformat(
            timespec="microseconds"
        ).replace("+00:00", "Z")
        token_issued_at_value = max(provider_updated_at, binding_updated_at).astimezone(
            UTC
        ).isoformat(timespec="microseconds").replace("+00:00", "Z")

        conn.execute("SET LOCAL ROLE okg_external_action_api")
        _set_verifier_capability(conn)
        audit = {
            "deployment": deployment,
            "authentication_audit_id": audit_id,
            "request_id": "request-" + audit_id,
            "principal": "subject",
            "separation_subject": "subject",
            "nomos_provider_id": provider_id,
            "nomos_subject_binding_id": binding_id,
            "authentication_method_code": "jwt_dpop",
            "issuer_or_trust_domain_digest": "sha256:"
            + hashlib.sha256(issuer.encode()).hexdigest(),
            "auth_config_digest": digest,
            "result_code": "authenticated",
            "provider_status": "active",
            "binding_status": "active",
            "provider_updated_at": provider_updated_at_value,
            "binding_updated_at": binding_updated_at_value,
            "provider_credential_generation_digest": digest,
            "binding_credential_generation_digest": digest,
            "credential_generation_digest": digest,
            "presented_credential_generation_digest": digest,
            "token_issued_at": token_issued_at_value,
            "nomos_decision_id": decision_id,
        }
        audit_assertion, audit_assertion_tag = _verifier_assertion(
            audit,
            operation="external_action.execute",
            transition_digest=lease_digest,
            nomos_decision_id=decision_id,
            policy_fingerprint=digest,
            provider_id=provider_id,
            provider_status="active",
            provider_updated_at=provider_updated_at_value,
            binding_id=binding_id,
            binding_status="active",
            binding_updated_at=binding_updated_at_value,
            nomos_input_hash=digest,
            authentication_audit_id=audit_id,
        )
        audit["verifier_assertion"] = audit_assertion
        audit["verifier_assertion_tag"] = audit_assertion_tag
        conn.execute(
            "SELECT okg.external_action_record_auth_audit(%s::jsonb)",
            (j(audit),),
        )
        lease = {
            "deployment": deployment,
            "epoch": 0,
            "lease_sequence": 1,
            "previous_lease_digest": None,
            "previous_epoch_digest": None,
            "issued_at": (now - timedelta(minutes=1)).isoformat(),
            "expires_at": (now + timedelta(hours=1)).isoformat(),
            "signer_separation_subject": "api-signer",
            "nonce": "nonce-" + uuid4().hex,
            "protected_b64": "cHJvdGVjdGVk",
            "payload_b64": "cGF5bG9hZA",
            "signature_b64": "c2lnbmF0dXJl",
            "signer_key_id_digest": digest,
            "envelope_digest": lease_digest,
            "verification_source": "strict_authenticated_api",
            "verification_config_digest": digest,
            "verification_login_binding_digest": digest,
            "lease_chain_digest": None,
            "authentication_audit_id": audit_id,
            "nomos_decision_id": decision_id,
        }
        first = conn.execute(
            "SELECT okg.external_action_store_insert_lease(%s::jsonb)",
            (j(lease),),
        ).fetchone()[0]
        replay = conn.execute(
            "SELECT okg.external_action_store_insert_lease(%s::jsonb)",
            (j(lease),),
        ).fetchone()[0]
        assert first["replay"] is False
        assert replay["replay"] is True
        conn.execute("RESET ROLE")
        assert conn.execute(
            """
            SELECT lease_chain_digest, authentication_audit_id, nomos_decision_id,
                   signer_key_id_digest
              FROM okg.external_action_execution_epoch_leases
             WHERE deployment_name = %s AND envelope_digest = %s
            """,
            (deployment, lease_digest),
        ).fetchone() == (None, audit_id, decision_id, digest)


def test_postgres_restore_finalize_uses_append_only_completion_receipt(
    okg_migrated: str,
) -> None:
    """Prepare/sign/finalize is atomic and leaves staging append-only."""

    deployment = "external-action-restore-finalize-" + uuid4().hex[:12]
    provider_id = "provider-" + uuid4().hex[:12]
    binding_id = "binding-" + uuid4().hex[:12]
    audit_id = "audit-" + uuid4().hex[:12]
    nomos_id = "nomos-" + uuid4().hex[:12]
    restore_id = "restore-" + uuid4().hex[:12]
    attestation_digest = "sha256:" + ("a" * 64)
    digest = "sha256:" + ("c" * 64)
    issuer = "https://issuer.example"
    now = datetime.now(UTC)

    def j(value: object) -> str:
        return json.dumps(value, separators=(",", ":"))

    with db_connect(okg_migrated) as conn:
        _bind_verifier_role(conn, deployment)
        _set_verifier_capability(conn)
        conn.execute(
            """
            INSERT INTO okg.nomos_identity_providers
              (deployment_name, provider_id, provider_type, issuer,
               subject_claim, status, metadata)
            VALUES (%s, %s, 'oidc', %s, 'sub', 'active', %s::jsonb)
            """,
            (
                deployment,
                provider_id,
                issuer,
                json.dumps({"credential_generation_digest": digest}),
            ),
        )
        conn.execute(
            """
            INSERT INTO okg.nomos_subject_bindings
              (deployment_name, binding_id, provider_id, issuer,
               claim_path, claim_value, subject, scope, status, metadata)
            VALUES (%s, %s, %s, %s, 'sub', 'subject-restore',
                    'subject-restore', 'external_action', 'active', %s::jsonb)
            """,
            (
                deployment,
                binding_id,
                provider_id,
                issuer,
                json.dumps({"credential_generation_digest": digest}),
            ),
        )
        conn.execute(
            """
            INSERT INTO okg.nomos_decisions
              (deployment_name, decision_id, input_hash, subject, action,
               classification, effect, reason_code, enforcement_point,
               policy_fingerprint)
            VALUES (%s, %s, %s, 'subject-restore',
                    'external_action.restore_reconcile', 'external_action',
                    'allow', 'restore_finalize_test', 'test', %s)
            """,
            (deployment, nomos_id, digest, digest),
        )
        provider_updated_at, binding_updated_at = conn.execute(
            "SELECT p.updated_at, b.updated_at "
            "FROM okg.nomos_identity_providers p "
            "JOIN okg.nomos_subject_bindings b "
            "  ON b.deployment_name = p.deployment_name "
            " AND b.provider_id = p.provider_id "
            " AND b.issuer = p.issuer "
            "WHERE p.deployment_name = %s AND p.provider_id = %s "
            "  AND b.binding_id = %s",
            (deployment, provider_id, binding_id),
        ).fetchone()
        empty_inventory_digest = domain_digest(
            "okg-external-action-restore-inventory-v1",
            {"sorted_effect_instance_keys": [], "count": 0},
        )
        empty_checkpoint_digest = domain_digest(
            "okg-external-action-restore-checkpoint-set-v1",
            {"sorted_checkpoint_envelope_digests": [], "count": 0},
        )
        empty_event_root_digest = conn.execute(
            "SELECT (okg.external_action_restore_event_root(%s)->>'digest')",
            (deployment,),
        ).fetchone()[0]
        request_values = {
            "restore_attestation_envelope_digest": attestation_digest,
            "expected_inventory_digest": empty_inventory_digest,
            "expected_inventory_count": 0,
            "expected_checkpoint_set_digest": empty_checkpoint_digest,
            "expected_checkpoint_count": 0,
        }
        request_hash = domain_digest(
            "okg-external-action-restore-finalize-request-v1", request_values
        )

        def canonical_timestamp(value: datetime) -> str:
            return value.astimezone(UTC).isoformat(timespec="microseconds").replace(
                "+00:00", "Z"
            )

        audit = {
            "deployment": deployment,
            "authentication_audit_id": audit_id,
            "principal": "subject-restore",
            "separation_subject": "subject-restore",
            "nomos_provider_id": provider_id,
            "nomos_subject_binding_id": binding_id,
            "authentication_method_code": "jwt_dpop",
            "issuer_or_trust_domain_digest": digest,
            "auth_config_digest": digest,
            "result_code": "authenticated",
            "provider_status": "active",
            "binding_status": "active",
            "provider_updated_at": canonical_timestamp(provider_updated_at),
            "binding_updated_at": canonical_timestamp(binding_updated_at),
            "provider_credential_generation_digest": digest,
            "binding_credential_generation_digest": digest,
            "credential_generation_digest": digest,
            "presented_credential_generation_digest": digest,
            "token_issued_at": canonical_timestamp(max(provider_updated_at, binding_updated_at)),
            "nomos_decision_id": nomos_id,
        }
        assertion, assertion_tag = _verifier_assertion(
            audit,
            operation="external_action.restore_reconcile",
            transition_digest=request_hash,
            nomos_decision_id=nomos_id,
            policy_fingerprint=digest,
            provider_id=provider_id,
            provider_status="active",
            provider_updated_at=audit["provider_updated_at"],
            binding_id=binding_id,
            binding_status="active",
            binding_updated_at=audit["binding_updated_at"],
            nomos_input_hash=digest,
            authentication_audit_id=audit_id,
        )
        conn.execute(
            """
            INSERT INTO okg.external_action_auth_audits (
              authentication_audit_id, deployment_name, request_id, principal,
              separation_subject, nomos_provider_id, nomos_subject_binding_id,
              authentication_method_code, issuer_or_trust_domain_digest,
              auth_config_digest, result_code, provider_status, binding_status,
              provider_updated_at, binding_updated_at,
              provider_credential_generation_digest,
              binding_credential_generation_digest,
              credential_generation_digest,
              presented_credential_generation_digest, token_issued_at,
              nomos_decision_id, verifier_assertion_nonce_digest,
              verifier_assertion_expires_at, verifier_transition_digest,
              verifier_policy_input_digest, verifier_required_obligations_digest,
              verifier_completed_obligations_digest, verifier_missing_obligations_digest,
              verifier_assertion_tag, verifier_capability_generation
            ) VALUES (%s, %s, %s, 'subject-restore', 'subject-restore',
                      %s, %s, 'jwt_dpop', %s, %s, 'authenticated',
                      'active', 'active', %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (audit_id, deployment, "request-" + audit_id, provider_id,
             binding_id, digest, digest, provider_updated_at, binding_updated_at,
             digest, digest, digest, digest,
             max(provider_updated_at, binding_updated_at), nomos_id,
             assertion["nonce_digest"], assertion["expires_at"],
             assertion["transition_digest"], assertion["nomos_input_hash"],
             assertion["required_obligations_digest"],
             assertion["completed_obligations_digest"],
             assertion["missing_obligations_digest"], assertion_tag,
             assertion["capability_generation"]),
        )
        conn.execute(
            """
            INSERT INTO okg.external_action_restore_attestations (
              deployment_name, restore_id, old_epoch, new_epoch,
              old_epoch_lease_digest, new_epoch_lease_digest,
              ledger_fence_envelope_digest, fenced_ledger_root_digest,
              fenced_ledger_entry_count, restored_snapshot_digest,
              pre_restore_event_root_digest, pre_restore_event_head_count,
              effect_inventory_digest, effect_inventory_count,
              worker_checkpoint_set_digest, worker_checkpoint_count,
              issued_at, expires_at, nonce, signer_separation_subject,
              protected_b64, payload_b64, signature_b64, signer_key_id_digest,
              envelope_digest, authentication_audit_id, nomos_decision_id
            ) VALUES (%s, %s, 1, 2, %s, %s, %s, %s, 0, %s, %s, 0,
                      %s, 0, %s, 0, %s, %s, %s, 'subject-restore',
                      'protected', 'payload', 'signature', %s, %s, %s, %s)
            """,
            (
                deployment, restore_id, digest, digest, digest, digest,
                digest, empty_event_root_digest, empty_inventory_digest,
                empty_checkpoint_digest, now - timedelta(minutes=1),
                now + timedelta(hours=1), "nonce-" + uuid4().hex, digest,
                attestation_digest, audit_id, nomos_id,
            ),
        )
        conn.execute(
            """
            INSERT INTO okg.external_action_restore_fences (
              deployment_name, highest_accepted_epoch, lease_digest,
              quarantine_state, service_mode, lease_chain_digest,
              verification_config_digest, verification_login_binding_digest
            ) VALUES (%s, 1, %s, 'blocked', 'restore_only', %s, %s, %s)
            """,
            (deployment, digest, digest, digest, digest),
        )
        conn.execute(
            """
            INSERT INTO okg.external_action_restore_sessions (
              deployment_name, restore_id, request_hash, idempotency_key_hash,
              operator_separation_subject, authentication_audit_id,
              nomos_decision_id, restore_attestation_envelope_digest, status,
              effect_inventory, effect_inventory_digest, effect_inventory_count
            ) VALUES (%s, %s, %s, %s, 'subject-restore', %s, %s, %s,
                      'staging', '[]'::jsonb, %s, 0)
            """,
            (deployment, restore_id, digest, digest, audit_id, nomos_id,
             attestation_digest, empty_inventory_digest),
        )
        conn.commit()

        prepare = {
            "deployment": deployment,
            "restore_id": restore_id,
            **request_values,
            "operator_separation_subject": "subject-restore",
            "idempotency_key_hash": digest,
            "request_hash": request_hash,
            "authentication_audit_id": audit_id,
            "nomos_decision_id": nomos_id,
        }
        conn.execute("RESET ROLE")
        conn.execute("SET LOCAL ROLE okg_external_action_api")
        _set_verifier_capability(conn)
        prepared = conn.execute(
            "SELECT okg.external_action_restore_prepare(%s::jsonb)",
            (j(prepare),),
        ).fetchone()[0]
        projection_fields = (
            "deployment", "restore_id", "restore_attestation_envelope_digest",
            "pre_restore_event_root_digest", "pre_restore_event_head_count",
            "restored_event_root_digest", "restored_event_head_count",
            "recovery_set_digest", "recovery_count", "completed_at",
        )
        projection = {field: prepared[field] for field in projection_fields}
        completion_unsigned = {
            "protected_b64": "cA", "payload_b64": "cA", "signature_b64": "cA",
        }
        completion_digest = domain_digest(
            "okg-external-action-restore-completion-receipt-envelope-v1",
            completion_unsigned,
        )
        finalize = {
            **prepare,
            "completion_receipt_envelope_digest": completion_digest,
            "completion_receipt_protected_b64": "cA",
            "completion_receipt_payload_b64": "cA",
            "completion_receipt_signature_b64": "cA",
            "completion_receipt_projection": projection,
            "completion_receipt_projection_digest": domain_digest(
                "okg-external-action-restore-completion-receipt-v1", projection
            ),
        }
        conn.execute("RESET ROLE")
        staging_before_finalize = conn.execute(
            "SELECT count(*) FROM okg.external_action_restore_checkpoint_staging "
            "WHERE deployment_name = %s AND restore_id = %s",
            (deployment, restore_id),
        ).fetchone()[0]
        conn.execute("SET LOCAL ROLE okg_external_action_api")
        first = conn.execute(
            "SELECT okg.external_action_restore_finalize(%s::jsonb)",
            (j(finalize),),
        ).fetchone()[0]
        replay = conn.execute(
            "SELECT okg.external_action_restore_finalize(%s::jsonb)",
            (j(finalize),),
        ).fetchone()[0]
        prepared_replay = conn.execute(
            "SELECT okg.external_action_restore_prepare(%s::jsonb)",
            (j(prepare),),
        ).fetchone()[0]
        assert first["replay"] is False
        assert first["state"] == "complete"
        assert first["recovery_count"] == 0
        assert first["classification_summary"] == {}
        assert first["restore_completion_receipt"]["envelope_digest"] == completion_digest
        assert first["recovery_set_digest"] == projection["recovery_set_digest"]
        # Finalize itself returns the retained canonical body byte-for-byte.
        # The public service discovers the retry through prepare, which adds
        # replay metadata without signing another completion receipt.
        assert replay == first
        assert prepared_replay["replay"] is True
        assert prepared_replay["completion_receipt_envelope_digest"] == completion_digest
        assert prepared_replay["response_body_jcs"]["__okg_bytes_b64"]

        conn.execute("RESET ROLE")
        assert conn.execute(
            "SELECT status, completion_receipt_envelope_digest "
            "FROM okg.external_action_restore_sessions "
            "WHERE deployment_name = %s AND restore_id = %s",
            (deployment, restore_id),
        ).fetchone() == ("staging", None)
        assert conn.execute(
            "SELECT count(*) FROM okg.external_action_restore_checkpoint_staging "
            "WHERE deployment_name = %s AND restore_id = %s",
            (deployment, restore_id),
        ).fetchone()[0] == staging_before_finalize
        assert conn.execute(
            "SELECT quarantine_state, service_mode, highest_accepted_epoch "
            "FROM okg.external_action_restore_fences WHERE deployment_name = %s",
            (deployment,),
        ).fetchone() == ("clear", "normal", 2)
        assert conn.execute(
            "SELECT count(*), authentication_audit_id, nomos_decision_id "
            "FROM okg.external_action_restore_completion_receipts "
            "WHERE deployment_name = %s AND restore_id = %s "
            "GROUP BY authentication_audit_id, nomos_decision_id",
            (deployment, restore_id),
        ).fetchone() == (1, audit_id, nomos_id)

        conn.execute("SAVEPOINT restore_session_append_only")
        with pytest.raises(psycopg.errors.ObjectInUse) as session_mutation:
            conn.execute(
                "UPDATE okg.external_action_restore_sessions "
                "SET status = 'complete' WHERE deployment_name = %s AND restore_id = %s",
                (deployment, restore_id),
            )
        assert session_mutation.value.sqlstate == "55006"
        conn.execute("ROLLBACK TO SAVEPOINT restore_session_append_only")
        conn.execute("SAVEPOINT completion_receipt_append_only")
        with pytest.raises(psycopg.errors.ObjectInUse) as receipt_mutation:
            conn.execute(
                "DELETE FROM okg.external_action_restore_completion_receipts "
                "WHERE deployment_name = %s AND restore_id = %s",
                (deployment, restore_id),
            )
        assert receipt_mutation.value.sqlstate == "55006"
        conn.execute("ROLLBACK TO SAVEPOINT completion_receipt_append_only")

        # A completed restore is terminal: a later checkpoint batch must be
        # rejected before validating or inserting any checkpoint evidence.
        # This also protects the append-only completion receipt from a stale
        # worker retry that still has a previously unseen batch index.
        conn.commit()
        post_completion_batch = {
            "deployment": deployment,
            "restore_id": restore_id,
            "batch_index": 99,
            "request_hash": request_hash,
            "idempotency_key_hash": digest,
            "operator_separation_subject": "subject-restore",
            "restore_attestation_envelope_digest": attestation_digest,
            "checkpoints": [{}],
            "authentication_audit_id": audit_id,
            "nomos_decision_id": nomos_id,
        }
        with pytest.raises(psycopg.errors.SerializationFailure) as stale_batch:
            _set_verifier_capability(conn)
            conn.execute(
                "SELECT okg.external_action_restore_batch(%s::jsonb)",
                (j(post_completion_batch),),
            )
        assert stale_batch.value.sqlstate == "40001"
        assert stale_batch.value.diag.message_detail
        assert "quarantine" in stale_batch.value.diag.message_detail.lower()
        conn.rollback()
        assert conn.execute(
            "SELECT count(*) FROM okg.external_action_restore_checkpoint_staging "
            "WHERE deployment_name = %s AND restore_id = %s",
            (deployment, restore_id),
        ).fetchone()[0] == 0


def test_postgres_runtime_login_rejects_inherited_and_direct_privilege_escape(
    okg_migrated: str,
) -> None:
    """The live runtime login has exactly the closed API privilege surface.

    A role that merely inherits the API role is not sufficient.  Direct
    Nomos/table/schema/function grants and an inherited broad role must all be
    observed through the actual ``session_user`` connection and rejected by
    the store contract.
    """

    suffix = uuid4().hex[:12]
    runtime_role = "okg_ext_runtime_" + suffix
    broad_role = "okg_ext_broad_" + suffix
    owner_broad_role = "okg_ext_owner_broad_" + suffix
    public_function = "hostile_public_security_definer_" + suffix
    public_view = "hostile_public_view_" + suffix
    runtime_dsn = make_conninfo(okg_migrated, user=runtime_role, password="okg")

    def require_rejected(marker: str) -> None:
        with db_connect(runtime_dsn) as runtime_conn:
            store = PostgresExternalActionStore(
                connection=runtime_conn,
                deployment="runtime-contract-test",
                verifier_capability=_TEST_VERIFIER_CAPABILITY,
                check_contract=False,
            )
            with pytest.raises(ExternalActionStoreError, match=marker):
                store.require_runtime_contract()

    try:
        with db_connect(okg_migrated) as admin:
            admin.execute(
                sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                    sql.Identifier(runtime_role), sql.Literal("okg")
                ),
            )
            admin.execute(
                sql.SQL("GRANT okg_external_action_api TO {}").format(
                    sql.Identifier(runtime_role)
                )
            )
            _bind_verifier_role(
                admin, "runtime-contract-test", session_user=runtime_role
            )
            admin.commit()

        # The unmodified runtime connection must satisfy the full contract;
        # otherwise the privilege probes below could be passing for the wrong
        # reason (for example, because the role was already unusable).
        with db_connect(runtime_dsn) as runtime_conn:
            store = PostgresExternalActionStore(
                connection=runtime_conn,
                deployment="runtime-contract-test",
                verifier_capability=_TEST_VERIFIER_CAPABILITY,
                check_contract=False,
            )
            store.require_runtime_contract()

        # A SECURITY DEFINER owner is part of the trust boundary.  Even a
        # NOLOGIN owner must not inherit a broad role that could mutate the
        # relations or helper functions used by the fixed RPCs.
        with db_connect(okg_migrated) as admin:
            admin.execute(
                sql.SQL("CREATE ROLE {} NOLOGIN").format(
                    sql.Identifier(owner_broad_role)
                )
            )
            admin.execute(
                sql.SQL("GRANT {} TO okg_external_action_function_owner").format(
                    sql.Identifier(owner_broad_role)
                )
            )
            admin.commit()
        require_rejected("function_owner inherits unexpected roles")
        with db_connect(okg_migrated) as admin:
            admin.execute(
                sql.SQL(
                    "REVOKE {} FROM okg_external_action_function_owner"
                ).format(sql.Identifier(owner_broad_role))
            )
            admin.commit()

        with db_connect(okg_migrated) as admin:
            admin.execute(
                sql.SQL("GRANT SELECT ON okg.nomos_decisions TO {}").format(
                    sql.Identifier(runtime_role)
                )
            )
            admin.commit()
        require_rejected("runtime login has effective raw Nomos/table privileges")
        with db_connect(okg_migrated) as admin:
            admin.execute(
                sql.SQL("REVOKE SELECT ON okg.nomos_decisions FROM {}").format(
                    sql.Identifier(runtime_role)
                )
            )
            admin.commit()

        with db_connect(okg_migrated) as admin:
            admin.execute(
                sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(broad_role))
            )
            admin.execute(
                sql.SQL("GRANT {} TO {}").format(
                    sql.Identifier(broad_role), sql.Identifier(runtime_role)
                )
            )
            admin.execute(
                sql.SQL("GRANT SELECT ON okg.nomos_decisions TO {}").format(
                    sql.Identifier(broad_role)
                )
            )
            admin.commit()
        require_rejected("runtime login must inherit exactly okg_external_action_api")
        with db_connect(okg_migrated) as admin:
            admin.execute(
                sql.SQL("REVOKE {} FROM {}").format(
                    sql.Identifier(broad_role), sql.Identifier(runtime_role)
                )
            )
            admin.commit()

        with db_connect(okg_migrated) as admin:
            admin.execute(
                sql.SQL("GRANT CREATE ON SCHEMA okg TO {}").format(
                    sql.Identifier(runtime_role)
                )
            )
            admin.commit()
        require_rejected("runtime login has effective CREATE on the okg schema")
        with db_connect(okg_migrated) as admin:
            admin.execute(
                sql.SQL("REVOKE CREATE ON SCHEMA okg FROM {}").format(
                    sql.Identifier(runtime_role)
                )
            )
            admin.commit()

        with db_connect(okg_migrated) as admin:
            admin.execute(
                sql.SQL("GRANT EXECUTE ON FUNCTION okg.external_action_hash(text, jsonb) TO {}").format(
                    sql.Identifier(runtime_role)
                )
            )
            admin.commit()
        require_rejected("function")

        # A mapped login must not gain a second privileged command surface
        # through a SECURITY DEFINER function whose name is outside the
        # external-action prefix.  Grant it through PUBLIC to exercise the
        # effective ACL rather than just direct role membership.
        with db_connect(okg_migrated) as admin:
            admin.execute(
                sql.SQL(
                    "REVOKE EXECUTE ON FUNCTION okg.external_action_hash(text, jsonb) FROM {}"
                ).format(sql.Identifier(runtime_role))
            )
            admin.execute(
                sql.SQL(
                    "CREATE FUNCTION okg.{}() RETURNS integer "
                    "LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog "
                    "AS 'SELECT 1'"
                ).format(sql.Identifier(public_function))
            )
            admin.execute(
                sql.SQL("GRANT EXECUTE ON FUNCTION okg.{}() TO PUBLIC").format(
                    sql.Identifier(public_function)
                )
            )
            admin.commit()
        require_rejected("unrelated SECURITY DEFINER functions")

        # Likewise, effective SELECT on an unrelated view is outside the ten
        # deployment-filtered security-barrier projections.  PUBLIC grants
        # must be visible through has_table_privilege(session_user, ...).
        with db_connect(okg_migrated) as admin:
            admin.execute(
                sql.SQL("REVOKE EXECUTE ON FUNCTION okg.{}() FROM PUBLIC").format(
                    sql.Identifier(public_function)
                )
            )
            admin.execute(
                sql.SQL("CREATE VIEW okg.{} AS SELECT 1 AS sentinel").format(
                    sql.Identifier(public_view)
                )
            )
            admin.execute(
                sql.SQL("GRANT SELECT ON okg.{} TO PUBLIC").format(
                    sql.Identifier(public_view)
                )
            )
            admin.commit()
        require_rejected("unrelated views")
    finally:
        with db_connect(okg_migrated) as admin:
            roles = {
                row[0]
                for row in admin.execute(
                "SELECT rolname FROM pg_catalog.pg_roles "
                    "WHERE rolname IN (%s, %s, %s)",
                    (runtime_role, broad_role, owner_broad_role),
                ).fetchall()
            }
            if {runtime_role, broad_role} <= roles:
                admin.execute(
                    sql.SQL("REVOKE {} FROM {}").format(
                        sql.Identifier(broad_role), sql.Identifier(runtime_role)
                    )
                )
            if runtime_role in roles:
                admin.execute(
                    sql.SQL(
                        "REVOKE EXECUTE ON FUNCTION "
                        "okg.external_action_hash(text, jsonb) FROM {}"
                    ).format(sql.Identifier(runtime_role))
                )
                admin.execute(
                    sql.SQL("REVOKE SELECT ON okg.nomos_decisions FROM {}").format(
                        sql.Identifier(runtime_role)
                    )
                )
            admin.execute(
                sql.SQL("DROP VIEW IF EXISTS okg.{}").format(sql.Identifier(public_view))
            )
            admin.execute(
                sql.SQL("DROP FUNCTION IF EXISTS okg.{}()").format(
                    sql.Identifier(public_function)
                )
            )
            if broad_role in roles:
                admin.execute(
                    sql.SQL("REVOKE SELECT ON okg.nomos_decisions FROM {}").format(
                        sql.Identifier(broad_role)
                    )
                )
            if owner_broad_role in roles:
                admin.execute(
                    sql.SQL(
                        "REVOKE {} FROM okg_external_action_function_owner"
                    ).format(sql.Identifier(owner_broad_role))
                )
            admin.execute(
                sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(runtime_role))
            )
            admin.execute(
                sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(broad_role))
            )
            admin.execute(
                sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(owner_broad_role))
            )
            admin.commit()


def test_postgres_restore_classifier_requires_receipt_for_explicit_failure(
    okg_migrated: str,
) -> None:
    """Restore class comes from signed journal fields, including receipts.

    ``provider_explicit_failure`` is a valid terminal provider outcome only
    when its positive checkpoint carries the provider receipt and journal
    evidence.  An effect-starting checkpoint remains uncertain and cannot
    invent a terminal reason during recovery.
    """

    digest = "sha256:" + ("a" * 64)
    explicit_failure = {
        "journal_phase": "outcome_reported",
        "provider_receipt_digest": digest,
        "outcome": "failed",
        "outcome_reason_code": "provider_explicit_failure",
        "outcome_recorded_at": "2026-08-27T12:00:00.000000Z",
        "effect_started_at": "2026-08-27T11:59:59.000000Z",
        "journal_digest": digest,
        "ledger_entry_digest": digest,
        "provider_idempotency_key_digest": digest,
    }
    uncertain = {
        "journal_phase": "effect_starting",
        "ledger_entry_digest": digest,
        "provider_idempotency_key_digest": digest,
        "effect_started_at": "2026-08-27T12:00:00.000000Z",
        "journal_digest": digest,
    }
    with db_connect(okg_migrated) as conn:
        assert conn.execute(
            "SELECT okg.external_action_restore_classification(%s::jsonb, 'positive')",
            (json.dumps(explicit_failure, separators=(",", ":")),),
        ).fetchone()[0] == "database_restore_failed"
        assert conn.execute(
            "SELECT okg.external_action_restore_classification(%s::jsonb, 'positive')",
            (json.dumps(uncertain, separators=(",", ":")),),
        ).fetchone()[0] == "database_restore_effect_uncertain"


def test_postgres_restore_provider_receipt_is_a_succeeded_recovery(
    okg_migrated: str,
) -> None:
    """A verified provider receipt is sufficient for successful recovery.

    The worker cannot provide a terminal outcome in this phase, but a receipt
    is the provider's authenticated completion evidence.  Recovery must map
    that phase to success and require the receipt digest as part of the
    positive checkpoint projection.
    """

    digest = "sha256:" + ("b" * 64)
    receipt_observed = {
        "journal_phase": "provider_receipt_observed",
        "ledger_entry_digest": digest,
        "provider_idempotency_key_digest": digest,
        "effect_started_at": "2026-08-27T12:00:00.000000Z",
        "provider_receipt_digest": digest,
        "journal_digest": digest,
    }
    with db_connect(okg_migrated) as conn:
        assert conn.execute(
            "SELECT okg.external_action_restore_classification(%s::jsonb, 'positive')",
            (json.dumps(receipt_observed, separators=(",", ":")),),
        ).fetchone()[0] == "database_restore_succeeded"


def test_postgres_restore_finalize_chooses_contract_bounded_reason_deterministically(
    okg_migrated: str,
) -> None:
    """A reason-less effect-starting recovery uses a stable contract reason.

    The checkpoint deliberately omits an outcome reason while its retained
    action contract has two valid indeterminate/receipt-forbidden reasons.
    Finalization must choose the lexicographically first permitted reason and
    persist it as the reconciled outcome, rather than failing nondeterministically
    or accepting a caller-authored reason.
    """

    suffix = uuid4().hex[:12]
    deployment = "external-action-restore-reason-" + suffix
    provider_id = "provider-" + suffix
    binding_id = "binding-" + suffix
    audit_id = "audit-" + suffix
    nomos_id = "nomos-" + suffix
    restore_id = "restore-" + suffix
    proposal_id = "proposal-" + suffix
    decision_id = "decision-" + suffix
    execution_id = "execution-" + suffix
    digest = "sha256:" + ("a" * 64)
    lease_digest = "sha256:" + ("b" * 64)
    ledger_digest = "sha256:" + ("c" * 64)
    attestation_digest = "sha256:" + ("d" * 64)
    checkpoint_digest = "sha256:" + ("e" * 64)
    issuer = "https://issuer.restore-reason.example"
    subject = "subject:restore-reason"
    now = datetime.now(UTC)
    issued_at = now - timedelta(minutes=1)
    checkpoint_at = now - timedelta(seconds=30)
    expires_at = now + timedelta(hours=1)
    execute_before = now + timedelta(minutes=30)

    def j(value: object) -> str:
        return json.dumps(value, separators=(",", ":"))

    def ts(value: datetime) -> str:
        return value.astimezone(UTC).isoformat(timespec="microseconds").replace(
            "+00:00", "Z"
        )

    action_contract = {
        "action_type": "ticket.update",
        "outcome_reason_rules": {
            "indeterminate_alpha": {
                "allowed_outcomes": ["indeterminate"],
                "provider_receipt": "forbidden",
            },
            "indeterminate_zulu": {
                "allowed_outcomes": ["indeterminate"],
                "provider_receipt": "forbidden",
            },
        },
    }
    action_contract_hash = action_contract_digest(action_contract)
    payload = {"target": "T-1"}
    payload_hash = domain_digest("okg-external-action-payload-v1", payload)
    idem_hash = "sha256:" + ("f" * 64)
    effect_key = effect_instance_key(deployment, subject, idem_hash)
    content_hash = submission_content_hash(
        {
            "automation_id": "restore-reason-test",
            "automation_version": "1.0.0",
            "run_id": "run-" + suffix,
            "correlation_id": "corr-" + suffix,
            "action_type": "ticket.update",
            "target_ref": "ticket:T-1",
            "risk_tier": "low",
            "action_schema_digest": digest,
            "action_contract_digest": action_contract_hash,
            "original_payload_hash": payload_hash,
            "requested_review_expires_at": ts(execute_before),
            "summary_code": "review_requested",
            "evidence_digests": [digest],
        }
    )
    proposal_request_hash = domain_digest(
        "okg-external-action-proposal-request-v1",
        {
            "deployment": deployment,
            "execution_epoch": 1,
            "effect_instance_key": effect_key,
            "submission_content_hash": content_hash,
            "proposer_separation_subject": subject,
            "automation_id": "restore-reason-test",
            "automation_version": "1.0.0",
            "run_id": "run-" + suffix,
            "correlation_id": "corr-" + suffix,
            "action_type": "ticket.update",
            "target_ref": "ticket:T-1",
            "risk_tier": "low",
            "action_schema_digest": digest,
            "action_contract_digest": action_contract_hash,
            "executor_contract_digest": digest,
            "payload_renderer_digest": digest,
            "governed_tool_binding_digest": digest,
            "provider_connection_contract_digest": digest,
            "provider_capability_digest": digest,
            "provider_operation_id": "ticket.update",
            "provider_operation_descriptor_digest": digest,
            "original_payload_hash": payload_hash,
            "requested_review_expires_at": ts(execute_before),
            "summary_code": "review_requested",
            "evidence_digests": [digest],
        },
    )
    decision_binding = {
        "deployment": deployment,
        "execution_epoch": 1,
        "effect_instance_key": effect_key,
        "proposal_id": proposal_id,
        "decision_id": decision_id,
        "approved_lifecycle_version": 1,
        "approved_payload_version": 1,
        "approved_payload_hash": payload_hash,
        "action_type": "ticket.update",
        "target_ref": "ticket:T-1",
        "action_schema_digest": digest,
        "action_contract_digest": action_contract_hash,
        "executor_contract_digest": digest,
        "payload_renderer_digest": digest,
        "governed_tool_binding_digest": digest,
        "provider_connection_contract_digest": digest,
        "provider_capability_digest": digest,
        "provider_operation_id": "ticket.update",
        "provider_operation_descriptor_digest": digest,
        "policy_digest": digest,
        "execute_before": ts(execute_before),
    }
    decision_binding_digest = decision_binding_hash(decision_binding)
    execution_binding = {
        "deployment": deployment,
        "execution_epoch": 1,
        "execution_epoch_lease_digest": lease_digest,
        "effect_instance_key": effect_key,
        "proposal_id": proposal_id,
        "decision_id": decision_id,
        "execution_id": execution_id,
        "worker_separation_subject": subject,
        "approved_lifecycle_version": 1,
        "approved_payload_version": 1,
        "approved_payload_hash": payload_hash,
        "action_type": "ticket.update",
        "target_ref": "ticket:T-1",
        "action_schema_digest": digest,
        "action_contract_digest": action_contract_hash,
        "executor_contract_digest": digest,
        "payload_renderer_digest": digest,
        "governed_tool_binding_digest": digest,
        "provider_connection_contract_digest": digest,
        "provider_capability_digest": digest,
        "provider_operation_id": "ticket.update",
        "provider_operation_descriptor_digest": digest,
        "policy_digest": digest,
    }
    execution_binding_digest = execution_binding_hash(execution_binding)
    claimed_at = checkpoint_at - timedelta(seconds=1)
    claim_event_projection = {
        "proposal_id": proposal_id,
        "execution_epoch": 1,
        "effect_instance_key": effect_key,
        "sequence": 1,
        "lifecycle_version": 1,
        "event_type": "execution_claimed",
        "actor_kind": "authenticated_principal",
        "actor_principal": subject,
        "actor_separation_subject": subject,
        "authentication_audit_ref": audit_id,
        "nomos_decision_ref": nomos_id,
        "deadline_kind": None,
        "policy_digest": digest,
        "before_review_state": "approved",
        "after_review_state": "approved",
        "before_execution_state": "ready",
        "after_execution_state": "claimed",
        "payload_version": 1,
        "payload_hash": payload_hash,
        "decision_id": decision_id,
        "decision_binding_hash": decision_binding_digest,
        "execution_id": execution_id,
        "execution_binding_hash": execution_binding_digest,
        "transition_reason_code": None,
        "outcome": None,
        "outcome_reason_code": None,
        "outcome_request_hash": None,
        "restore_recovery_digest": None,
        "effective_at": None,
        "recorded_at": ts(claimed_at),
        "previous_event_hash": None,
    }
    claim_event_hash = domain_digest(
        "okg-external-action-event-v1", claim_event_projection
    )
    inventory = [effect_key]
    inventory_digest = domain_digest(
        "okg-external-action-restore-inventory-v1",
        {"sorted_effect_instance_keys": inventory, "count": 1},
    )
    proof = {
        "effect_instance_key": effect_key,
        "ledger_entry_digest": digest,
        "leaf_count": "1",
        "leaf_index": "0",
    }
    proof_digest = domain_digest(
        "okg-external-action-restore-positive-proof-v1", proof
    )
    checkpoint = {
        "deployment": deployment,
        "effect_instance_key": effect_key,
        "execution_epoch": 1,
        "ledger_fence_envelope_digest": ledger_digest,
        "fenced_ledger_root_digest": digest,
        "fenced_ledger_entry_count": 1,
        "proof_kind": "positive",
        "positive_proof_digest": proof_digest,
        "nonmembership_proof_digest": None,
        "ledger_entry_digest": digest,
        "submission_content_hash": content_hash,
        "proposal_id": proposal_id,
        "proposal_request_hash": proposal_request_hash,
        "original_payload_hash": payload_hash,
        "approved_payload_hash": payload_hash,
        "action_contract_digest": action_contract_hash,
        "decision_id": decision_id,
        "decision_binding_hash": decision_binding_digest,
        "execution_id": execution_id,
        "execution_binding_hash": execution_binding_digest,
        "okg_claim_handoff_receipt_digest": digest,
        "provider_idempotency_key_digest": digest,
        "journal_phase": "effect_starting",
        "effect_started_at": ts(checkpoint_at),
        "provider_receipt_digest": None,
        "outcome": None,
        "outcome_reason_code": None,
        "outcome_recorded_at": None,
        "journal_digest": digest,
        "pre_restore_effect_event_head_hash": claim_event_hash,
        "checkpoint_issued_at": ts(checkpoint_at),
    }
    checkpoint_set_digest = domain_digest(
        "okg-external-action-restore-checkpoint-set-v1",
        {"sorted_checkpoint_envelope_digests": [checkpoint_digest], "count": 1},
    )
    request_values = {
        "restore_attestation_envelope_digest": attestation_digest,
        "expected_inventory_digest": inventory_digest,
        "expected_inventory_count": 1,
        "expected_checkpoint_set_digest": checkpoint_set_digest,
        "expected_checkpoint_count": 1,
    }
    request_hash = domain_digest(
        "okg-external-action-restore-finalize-request-v1", request_values
    )

    with db_connect(okg_migrated) as conn:
        _bind_verifier_role(conn, deployment)
        _set_verifier_capability(conn)
        conn.execute(
            """
            INSERT INTO okg.nomos_identity_providers
              (deployment_name, provider_id, provider_type, issuer,
               subject_claim, status, metadata)
            VALUES (%s, %s, 'oidc', %s, 'sub', 'active', %s::jsonb)
            """,
            (deployment, provider_id, issuer, j({"credential_generation_digest": digest})),
        )
        conn.execute(
            """
            INSERT INTO okg.nomos_subject_bindings
              (deployment_name, binding_id, provider_id, issuer, claim_path,
               claim_value, subject, scope, status, metadata)
            VALUES (%s, %s, %s, %s, 'sub', %s, %s, 'external_action',
                    'active', %s::jsonb)
            """,
            (
                deployment,
                binding_id,
                provider_id,
                issuer,
                subject,
                subject,
                j({"credential_generation_digest": digest}),
            ),
        )
        provider_updated_at, binding_updated_at = conn.execute(
            """
            SELECT p.updated_at, b.updated_at
              FROM okg.nomos_identity_providers p
              JOIN okg.nomos_subject_bindings b
                ON b.deployment_name = p.deployment_name
               AND b.provider_id = p.provider_id
               AND b.issuer = p.issuer
             WHERE p.deployment_name = %s
               AND p.provider_id = %s
               AND b.binding_id = %s
            """,
            (deployment, provider_id, binding_id),
        ).fetchone()
        issuer_digest = "sha256:" + hashlib.sha256(issuer.encode()).hexdigest()
        conn.execute(
            """
            INSERT INTO okg.nomos_decisions
              (deployment_name, decision_id, input_hash, subject, action,
               classification, effect, reason_code, enforcement_point,
               policy_fingerprint)
            VALUES (%s, %s, %s, %s, 'external_action.restore_reconcile',
                    'external_action', 'allow', 'restore-reason-test', 'test', %s)
            """,
            (deployment, nomos_id, digest, subject, digest),
        )
        audit = {
            "deployment": deployment,
            "authentication_audit_id": audit_id,
            "principal": subject,
            "separation_subject": subject,
            "nomos_provider_id": provider_id,
            "nomos_subject_binding_id": binding_id,
            "authentication_method_code": "jwt_dpop",
            "issuer_or_trust_domain_digest": issuer_digest,
            "auth_config_digest": digest,
            "result_code": "authenticated",
            "provider_status": "active",
            "binding_status": "active",
            "provider_updated_at": ts(provider_updated_at),
            "binding_updated_at": ts(binding_updated_at),
            "provider_credential_generation_digest": digest,
            "binding_credential_generation_digest": digest,
            "credential_generation_digest": digest,
            "presented_credential_generation_digest": digest,
            "token_issued_at": ts(max(provider_updated_at, binding_updated_at)),
            "nomos_decision_id": nomos_id,
        }
        assertion, assertion_tag = _verifier_assertion(
            audit,
            operation="external_action.restore_reconcile",
            transition_digest=request_hash,
            nomos_decision_id=nomos_id,
            policy_fingerprint=digest,
            provider_id=provider_id,
            provider_status="active",
            provider_updated_at=audit["provider_updated_at"],
            binding_id=binding_id,
            binding_status="active",
            binding_updated_at=audit["binding_updated_at"],
            nomos_input_hash=digest,
            authentication_audit_id=audit_id,
        )
        conn.execute(
            """
            INSERT INTO okg.external_action_auth_audits (
              authentication_audit_id, deployment_name, request_id, principal,
              separation_subject, nomos_provider_id, nomos_subject_binding_id,
              authentication_method_code, issuer_or_trust_domain_digest,
              auth_config_digest, result_code, provider_status, binding_status,
              provider_updated_at, binding_updated_at,
              provider_credential_generation_digest,
              binding_credential_generation_digest,
              credential_generation_digest, presented_credential_generation_digest,
              token_issued_at, nomos_decision_id,
              verifier_assertion_nonce_digest, verifier_assertion_expires_at,
              verifier_transition_digest, verifier_policy_input_digest,
              verifier_required_obligations_digest,
              verifier_completed_obligations_digest, verifier_missing_obligations_digest,
              verifier_assertion_tag, verifier_capability_generation
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, 'jwt_dpop', %s, %s,
                      'authenticated', 'active', 'active', %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                audit_id,
                deployment,
                "request-" + suffix,
                subject,
                subject,
                provider_id,
                binding_id,
                issuer_digest,
                digest,
                provider_updated_at,
                binding_updated_at,
                digest,
                digest,
                digest,
                digest,
                max(provider_updated_at, binding_updated_at),
                nomos_id,
                assertion["nonce_digest"], assertion["expires_at"],
                assertion["transition_digest"], assertion["nomos_input_hash"],
                assertion["required_obligations_digest"],
                assertion["completed_obligations_digest"],
                assertion["missing_obligations_digest"], assertion_tag,
                assertion["capability_generation"],
            ),
        )
        conn.execute(
            """
            INSERT INTO okg.external_action_proposals (
              proposal_id, deployment_name, execution_epoch, effect_instance_key,
              submission_content_hash, submission_request_hash,
              submission_idempotency_key_hash, proposer_separation_subject,
              automation_id, automation_version, run_id, correlation_id,
              action_type, target_ref, risk_tier, action_schema_digest,
              action_contract_digest, executor_contract_digest,
              payload_renderer_digest, governed_tool_binding_digest,
              provider_connection_contract_digest, provider_capability_digest,
              provider_operation_id, provider_operation_descriptor_digest,
              original_payload_hash, requested_review_expires_at,
              effective_review_expires_at, summary_code, evidence_digests,
              review_state, execution_state, lifecycle_version, payload_version,
              current_payload_hash, action_contract_record
            ) VALUES (%s, %s, 1, %s, %s, %s, %s, %s, %s, '1.0.0', %s, %s,
                      'ticket.update', 'ticket:T-1', 'low', %s, %s, %s, %s,
                      %s, %s, %s, 'ticket.update', %s, %s, %s, %s, %s,
                      %s::jsonb, 'approved', 'claimed', 1, 1, %s, %s::jsonb)
            """,
            (
                proposal_id,
                deployment,
                effect_key,
                content_hash,
                proposal_request_hash,
                idem_hash,
                subject,
                "restore-reason-test",
                "run-" + suffix,
                "corr-" + suffix,
                digest,
                action_contract_hash,
                digest,
                digest,
                digest,
                digest,
                digest,
                digest,
                payload_hash,
                execute_before,
                execute_before,
                "review_requested",
                j([digest]),
                payload_hash,
                j(action_contract),
            ),
        )
        conn.execute(
            """
            INSERT INTO okg.external_action_payload_versions
              (deployment_name, proposal_id, payload_version, payload_hash,
               canonical_payload, difference_field_codes)
            VALUES (%s, %s, 1, %s, %s, '[]'::jsonb)
            """,
            (deployment, proposal_id, payload_hash, b'{"target":"T-1"}'),
        )
        conn.execute(
            """
            INSERT INTO okg.external_action_decisions (
              decision_id, deployment_name, proposal_id, verdict,
              expected_lifecycle_version, expected_payload_version,
              approved_lifecycle_version, approved_payload_version,
              approved_payload_hash, action_type, target_ref,
              action_schema_digest, action_contract_digest, executor_contract_digest,
              payload_renderer_digest, governed_tool_binding_digest,
              provider_connection_contract_digest, provider_capability_digest,
              provider_operation_id, provider_operation_descriptor_digest,
              execution_epoch, effect_instance_key, approver_principal,
              approver_separation_subject, authentication_audit_id,
              nomos_decision_id, policy_digest, execute_before, reason_code,
              decision_binding_hash, request_hash
            ) VALUES (%s, %s, %s, 'approved', 1, 1, 1, 1, %s,
                      'ticket.update', 'ticket:T-1', %s, %s, %s, %s, %s, %s,
                      %s, 'ticket.update', %s, 1, %s, %s, %s, %s, %s, %s,
                      %s, 'approved', %s, %s)
            """,
            (
                decision_id,
                deployment,
                proposal_id,
                payload_hash,
                digest,
                action_contract_hash,
                digest,
                digest,
                digest,
                digest,
                digest,
                digest,
                effect_key,
                subject,
                subject,
                audit_id,
                nomos_id,
                digest,
                execute_before,
                decision_binding_digest,
                digest,
            ),
        )
        conn.execute(
            """
            INSERT INTO okg.external_action_executions (
              execution_id, deployment_name, proposal_id, decision_id, execution_epoch,
              execution_epoch_lease_digest, effect_instance_key,
              worker_separation_subject, approved_lifecycle_version,
              approved_payload_version, approved_payload_hash, action_type,
              target_ref, action_schema_digest, action_contract_digest,
              executor_contract_digest, payload_renderer_digest,
              governed_tool_binding_digest, provider_connection_contract_digest,
              provider_capability_digest, provider_operation_id,
              provider_operation_descriptor_digest, policy_digest,
              execution_binding_hash, request_hash,
              okg_claim_handoff_receipt_digest, handoff_protected_b64,
              handoff_payload_b64, handoff_signature_b64, canonical_payload,
              provider_idempotency_key_digest, authentication_audit_id,
              nomos_decision_id, execute_before
            ) VALUES (%s, %s, %s, %s, 1, %s, %s, %s, 1, 1, %s,
                      'ticket.update', 'ticket:T-1', %s, %s, %s, %s, %s, %s,
                      %s, 'ticket.update', %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s)
            """,
            (
                execution_id,
                deployment,
                proposal_id,
                decision_id,
                lease_digest,
                effect_key,
                subject,
                payload_hash,
                digest,
                action_contract_hash,
                digest,
                digest,
                digest,
                digest,
                digest,
                digest,
                digest,
                execution_binding_digest,
                digest,
                digest,
                "protected",
                "payload",
                "signature",
                b'{"target":"T-1"}',
                digest,
                audit_id,
                nomos_id,
                execute_before,
            ),
        )
        conn.execute(
            """
            INSERT INTO okg.external_action_events (
              deployment_name, proposal_id, execution_epoch, effect_instance_key,
              sequence, lifecycle_version, event_type, actor_kind,
              actor_principal, actor_separation_subject,
              authentication_audit_ref, nomos_decision_ref, policy_digest,
              before_review_state, after_review_state,
              before_execution_state, after_execution_state, payload_version,
              payload_hash, decision_id, decision_binding_hash, execution_id,
              execution_binding_hash, recorded_at, previous_event_hash,
              event_hash, store_record
            ) VALUES (%s, %s, 1, %s, 1, 1, 'execution_claimed',
                      'authenticated_principal', %s, %s, %s, %s, %s,
                      'approved', 'approved', 'ready', 'claimed', 1, %s, %s,
                      %s, %s, %s, %s, NULL, %s, %s::jsonb)
            """,
            (
                deployment,
                proposal_id,
                effect_key,
                subject,
                subject,
                audit_id,
                nomos_id,
                digest,
                payload_hash,
                decision_id,
                decision_binding_digest,
                execution_id,
                execution_binding_digest,
                claimed_at,
                claim_event_hash,
                j({"event_projection": claim_event_projection}),
            ),
        )
        pre_root = conn.execute(
            "SELECT okg.external_action_restore_event_root(%s)", (deployment,)
        ).fetchone()[0]
        conn.execute(
            """
            INSERT INTO okg.external_action_restore_attestations (
              deployment_name, restore_id, old_epoch, new_epoch,
              old_epoch_lease_digest, new_epoch_lease_digest,
              ledger_fence_envelope_digest, fenced_ledger_root_digest,
              fenced_ledger_entry_count, restored_snapshot_digest,
              pre_restore_event_root_digest, pre_restore_event_head_count,
              effect_inventory_digest, effect_inventory_count,
              worker_checkpoint_set_digest, worker_checkpoint_count,
              issued_at, expires_at, nonce, signer_separation_subject,
              protected_b64, payload_b64, signature_b64, signer_key_id_digest,
              envelope_digest, authentication_audit_id, nomos_decision_id
            ) VALUES (%s, %s, 1, 2, %s, %s, %s, %s, 1, %s, %s, %s,
                      %s, 1, %s, 1, %s, %s, %s, %s, 'protected', 'payload',
                      'signature', %s, %s, %s, %s)
            """,
            (
                deployment,
                restore_id,
                lease_digest,
                lease_digest,
                ledger_digest,
                digest,
                digest,
                pre_root["digest"],
                pre_root["count"],
                inventory_digest,
                checkpoint_set_digest,
                issued_at,
                expires_at,
                "nonce-" + suffix,
                subject,
                digest,
                attestation_digest,
                audit_id,
                nomos_id,
            ),
        )
        conn.execute(
            """
            INSERT INTO okg.external_action_restore_fences (
              deployment_name, highest_accepted_epoch, lease_digest,
              quarantine_state, service_mode, lease_chain_digest,
              verification_config_digest, verification_login_binding_digest
            ) VALUES (%s, 1, %s, 'blocked', 'restore_only', %s, %s, %s)
            """,
            (deployment, lease_digest, digest, digest, digest),
        )
        conn.execute(
            """
            INSERT INTO okg.external_action_restore_sessions (
              deployment_name, restore_id, request_hash, idempotency_key_hash,
              operator_separation_subject, authentication_audit_id,
              nomos_decision_id, restore_attestation_envelope_digest, status,
              effect_inventory, effect_inventory_digest, effect_inventory_count
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'staging', %s::jsonb,
                      %s, 1)
            """,
            (
                deployment,
                restore_id,
                digest,
                digest,
                subject,
                audit_id,
                nomos_id,
                attestation_digest,
                j(inventory),
                inventory_digest,
            ),
        )
        conn.execute(
            """
            INSERT INTO okg.external_action_restore_checkpoint_entries (
              deployment_name, restore_id, batch_index, ordinal, effect_instance_key,
              checkpoint_envelope_digest, proof_digest, leaf_index, proof,
              checkpoint_payload
            ) VALUES (%s, %s, 0, 0, %s, %s, %s, 0, %s::jsonb, %s::jsonb)
            """,
            (
                deployment,
                restore_id,
                effect_key,
                checkpoint_digest,
                proof_digest,
                j(proof),
                j(checkpoint),
            ),
        )
        conn.commit()

        prepare = {
            "deployment": deployment,
            "restore_id": restore_id,
            **request_values,
            "operator_separation_subject": subject,
            "idempotency_key_hash": digest,
            "request_hash": request_hash,
            "authentication_audit_id": audit_id,
            "nomos_decision_id": nomos_id,
        }
        conn.execute("RESET ROLE")
        conn.execute("SET LOCAL ROLE okg_external_action_api")
        _set_verifier_capability(conn)
        prepared = conn.execute(
            "SELECT okg.external_action_restore_prepare(%s::jsonb)", (j(prepare),)
        ).fetchone()[0]
        projection_fields = (
            "deployment", "restore_id", "restore_attestation_envelope_digest",
            "pre_restore_event_root_digest", "pre_restore_event_head_count",
            "restored_event_root_digest", "restored_event_head_count",
            "recovery_set_digest", "recovery_count", "completed_at",
        )
        projection = {field: prepared[field] for field in projection_fields}
        completion_unsigned = {
            "protected_b64": "cA", "payload_b64": "cA", "signature_b64": "cA",
        }
        completion_digest = domain_digest(
            "okg-external-action-restore-completion-receipt-envelope-v1",
            completion_unsigned,
        )
        finalize = {
            **prepare,
            "completion_receipt_envelope_digest": completion_digest,
            "completion_receipt_protected_b64": "cA",
            "completion_receipt_payload_b64": "cA",
            "completion_receipt_signature_b64": "cA",
            "completion_receipt_projection": projection,
            "completion_receipt_projection_digest": domain_digest(
                "okg-external-action-restore-completion-receipt-v1", projection
            ),
        }
        finalized = conn.execute(
            "SELECT okg.external_action_restore_finalize(%s::jsonb)",
            (j(finalize),),
        ).fetchone()[0]
        assert finalized["classification_summary"] == {
            "database_restore_effect_uncertain": 1
        }
        conn.execute("RESET ROLE")
        assert conn.execute(
            """
            SELECT outcome, outcome_reason_code, provider_receipt_digest
              FROM okg.external_action_outcomes
             WHERE deployment_name = %s AND execution_id = %s
            """,
            (deployment, execution_id),
        ).fetchone() == ("indeterminate", "indeterminate_alpha", None)


def test_postgres_restore_create_response_binds_digest_count_and_state(
    okg_migrated: str,
) -> None:
    """Create returns one coherent projection and replays it durably.

    Restore creation has two state representations: the public response says
    that checkpoint collection is beginning, while the immutable SQL session
    is staged until a completion receipt is appended.  The response must
    still carry the signed inventory/checkpoint digests and counts that are
    retained by both rows; a retry must not create a second attestation.
    """

    deployment = "external-action-restore-create-" + uuid4().hex[:12]
    provider_id = "provider-" + uuid4().hex[:12]
    binding_id = "binding-" + uuid4().hex[:12]
    audit_id = "audit-" + uuid4().hex[:12]
    nomos_id = "nomos-" + uuid4().hex[:12]
    restore_id = "restore-" + uuid4().hex[:12]
    old_lease_digest = "sha256:" + ("a" * 64)
    new_lease_digest = "sha256:" + ("b" * 64)
    ledger_digest = "sha256:" + ("c" * 64)
    worker_digest = "sha256:" + ("d" * 64)
    provider_fence_digest = "sha256:" + ("e" * 64)
    attestation_digest = "sha256:" + ("f" * 64)
    verification_config_digest = "sha256:" + ("1" * 64)
    verification_binding_digest = "sha256:" + ("2" * 64)
    lease_chain_digest = "sha256:" + ("3" * 64)
    signer_digest = "sha256:" + ("4" * 64)
    now = datetime.now(UTC)
    issued_at = (now - timedelta(minutes=1)).isoformat().replace("+00:00", "Z")
    expires_at = (now + timedelta(hours=1)).isoformat().replace("+00:00", "Z")
    empty_inventory_digest = domain_digest(
        "okg-external-action-restore-inventory-v1",
        {"sorted_effect_instance_keys": [], "count": 0},
    )
    empty_checkpoint_digest = domain_digest(
        "okg-external-action-restore-checkpoint-set-v1",
        {"sorted_checkpoint_envelope_digests": [], "count": 0},
    )

    def j(value: object) -> str:
        return json.dumps(value, separators=(",", ":"))

    request_hash = domain_digest(
        "okg-external-action-restore-create-request-v1",
        {
            "restore_id": restore_id,
            "old_epoch_lease_envelope_digest": old_lease_digest,
            "new_epoch_lease_envelope_digest": new_lease_digest,
            "worker_quiescence_envelope_digest": worker_digest,
            "provider_authority_fence_envelope_digest": provider_fence_digest,
            "ledger_fence_envelope_digest": ledger_digest,
            "restore_attestation_envelope_digest": attestation_digest,
        },
    )

    with db_connect(okg_migrated) as conn:
        _bind_verifier_role(conn, deployment)
        _set_verifier_capability(conn)
        conn.execute(
            """
            INSERT INTO okg.nomos_identity_providers
              (deployment_name, provider_id, provider_type, issuer,
               subject_claim, status)
            VALUES (%s, %s, 'oidc', 'https://issuer.example', 'sub', 'active')
            """,
            (deployment, provider_id),
        )
        conn.execute(
            """
            INSERT INTO okg.nomos_subject_bindings
              (deployment_name, binding_id, provider_id, issuer,
               claim_path, claim_value, subject, scope, status)
            VALUES (%s, %s, %s, 'https://issuer.example', 'sub',
                    'restore-operator', 'restore-operator',
                    'external_action', 'active')
            """,
            (deployment, binding_id, provider_id),
        )
        conn.execute(
            """
            INSERT INTO okg.nomos_decisions
              (deployment_name, decision_id, input_hash, subject, action,
               classification, effect, reason_code, enforcement_point,
               policy_fingerprint)
            VALUES (%s, %s, %s, 'restore-operator',
                    'external_action.restore_reconcile', 'external_action',
                    'allow', 'restore_create_test', 'test', %s)
            """,
            (deployment, nomos_id, signer_digest, signer_digest),
        )
        provider_updated_at, binding_updated_at = conn.execute(
            """
            SELECT p.updated_at, b.updated_at
              FROM okg.nomos_identity_providers p
              JOIN okg.nomos_subject_bindings b
                ON b.deployment_name = p.deployment_name
               AND b.provider_id = p.provider_id
             WHERE p.deployment_name = %s
               AND p.provider_id = %s
               AND b.binding_id = %s
            """,
            (deployment, provider_id, binding_id),
        ).fetchone()
        def canonical_timestamp(value: datetime) -> str:
            return value.astimezone(UTC).isoformat(timespec="microseconds").replace(
                "+00:00", "Z"
            )

        audit = {
            "deployment": deployment,
            "authentication_audit_id": audit_id,
            "principal": "restore-operator",
            "separation_subject": "restore-operator",
            "nomos_provider_id": provider_id,
            "nomos_subject_binding_id": binding_id,
            "authentication_method_code": "mtls",
            "issuer_or_trust_domain_digest": signer_digest,
            "auth_config_digest": signer_digest,
            "result_code": "authenticated",
            "provider_status": "active",
            "binding_status": "active",
            "provider_updated_at": canonical_timestamp(provider_updated_at),
            "binding_updated_at": canonical_timestamp(binding_updated_at),
            "provider_credential_generation_digest": signer_digest,
            "binding_credential_generation_digest": signer_digest,
            "credential_generation_digest": signer_digest,
            "presented_credential_generation_digest": signer_digest,
            "certificate_thumbprint": "cert-restore",
            "certificate_not_before": canonical_timestamp(
                max(provider_updated_at, binding_updated_at)
            ),
            "nomos_decision_id": nomos_id,
        }
        assertion, assertion_tag = _verifier_assertion(
            audit,
            operation="external_action.restore_reconcile",
            transition_digest=request_hash,
            nomos_decision_id=nomos_id,
            policy_fingerprint=signer_digest,
            provider_id=provider_id,
            provider_status="active",
            provider_updated_at=audit["provider_updated_at"],
            binding_id=binding_id,
            binding_status="active",
            binding_updated_at=audit["binding_updated_at"],
            nomos_input_hash=signer_digest,
            authentication_audit_id=audit_id,
        )
        conn.execute(
            """
            INSERT INTO okg.external_action_auth_audits (
              authentication_audit_id, deployment_name, request_id, principal,
              separation_subject, nomos_provider_id, nomos_subject_binding_id,
              authentication_method_code, issuer_or_trust_domain_digest,
              auth_config_digest, result_code, provider_status, binding_status,
              provider_updated_at, binding_updated_at,
              provider_credential_generation_digest,
              binding_credential_generation_digest,
              presented_credential_generation_digest, certificate_thumbprint,
              certificate_not_before, nomos_decision_id,
              verifier_assertion_nonce_digest, verifier_assertion_expires_at,
              verifier_transition_digest, verifier_policy_input_digest,
              verifier_required_obligations_digest,
              verifier_completed_obligations_digest, verifier_missing_obligations_digest,
              verifier_assertion_tag, verifier_capability_generation
            ) VALUES (%s, %s, %s, 'restore-operator', 'restore-operator',
                      %s, %s, 'mtls', %s, %s, 'authenticated', 'active',
                      'active', %s, %s, %s, %s, %s, 'cert-restore', %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                audit_id,
                deployment,
                "request-" + audit_id,
                provider_id,
                binding_id,
                signer_digest,
                signer_digest,
                provider_updated_at,
                binding_updated_at,
                signer_digest,
                signer_digest,
                signer_digest,
                max(provider_updated_at, binding_updated_at),
                nomos_id,
                assertion["nonce_digest"], assertion["expires_at"],
                assertion["transition_digest"], assertion["nomos_input_hash"],
                assertion["required_obligations_digest"],
                assertion["completed_obligations_digest"],
                assertion["missing_obligations_digest"], assertion_tag,
                assertion["capability_generation"],
            ),
        )
        # Restore create reads the two durable epoch leases and the installed
        # quarantined fence.  They are inserted as startup evidence so this
        # test exercises the same row bindings as the production gate.
        conn.execute(
            """
            INSERT INTO okg.external_action_execution_epoch_leases (
              deployment_name, epoch, lease_sequence, previous_lease_digest,
              previous_epoch_digest, issued_at, expires_at,
              signer_separation_subject, nonce, protected_b64, payload_b64,
              signature_b64, signer_key_id_digest, envelope_digest,
              verification_source, verification_config_digest,
              verification_login_binding_digest, lease_chain_digest
            ) VALUES (
              %s, 1, 1, NULL, NULL, %s, %s, 'startup-gate', %s,
              'protected', 'payload', 'signature', %s, %s,
              'signed_startup_gate', %s, %s, %s
            ), (
              %s, 2, 1, NULL, %s, %s, %s, 'startup-gate', %s,
              'protected', 'payload', 'signature', %s, %s,
              'signed_startup_gate', %s, %s, %s
            )
            """,
            (
                deployment,
                now - timedelta(minutes=2),
                now + timedelta(hours=1),
                "nonce-old-" + uuid4().hex,
                signer_digest,
                old_lease_digest,
                verification_config_digest,
                verification_binding_digest,
                ledger_digest,
                deployment,
                old_lease_digest,
                now - timedelta(minutes=1),
                now + timedelta(hours=1),
                "nonce-new-" + uuid4().hex,
                signer_digest,
                new_lease_digest,
                verification_config_digest,
                verification_binding_digest,
                lease_chain_digest,
            ),
        )
        conn.execute(
            """
            INSERT INTO okg.external_action_restore_fences (
              deployment_name, highest_accepted_epoch, lease_digest,
              quarantine_state, service_mode, lease_chain_digest,
              verification_config_digest, verification_login_binding_digest
            ) VALUES (%s, 2, %s, 'blocked', 'restore_only', %s, %s, %s)
            """,
            (
                deployment,
                new_lease_digest,
                lease_chain_digest,
                verification_config_digest,
                verification_binding_digest,
            ),
        )
        conn.commit()

        restore_payload = {
            "deployment": deployment,
            "restore_id": restore_id,
            "old_epoch": "1",
            "new_epoch": "2",
            "old_epoch_lease_digest": old_lease_digest,
            "new_epoch_lease_digest": new_lease_digest,
            "ledger_fence_envelope_digest": ledger_digest,
            "fenced_ledger_root_digest": signer_digest,
            "fenced_ledger_entry_count": "0",
            "restored_snapshot_digest": signer_digest,
            "pre_restore_event_root_digest": signer_digest,
            "pre_restore_event_head_count": "0",
            "effect_inventory_digest": empty_inventory_digest,
            "effect_inventory_count": "0",
            "worker_checkpoint_set_digest": empty_checkpoint_digest,
            "worker_checkpoint_count": "0",
            "issued_at": issued_at,
            "expires_at": expires_at,
            "nonce": "restore-attestation-" + uuid4().hex,
            "signer_separation_subject": "restore-operator",
        }

        def evidence_item(envelope_digest: str, payload: dict[str, str] | None = None) -> dict[str, object]:
            return {
                "envelope": {
                    "protected_b64": "cA",
                    "payload_b64": "cA",
                    "signature_b64": "cA",
                },
                "payload": payload or {},
                "envelope_digest": envelope_digest,
            }

        evidence = {
            "old_epoch_lease": evidence_item(old_lease_digest),
            "new_epoch_lease": evidence_item(new_lease_digest),
            "worker_quiescence": evidence_item(worker_digest),
            "provider_authority_fence": evidence_item(provider_fence_digest),
            "ledger_fence": evidence_item(ledger_digest),
            "restore_attestation": evidence_item(attestation_digest, restore_payload),
        }
        command = {
            **restore_payload,
            "old_epoch": 1,
            "new_epoch": 2,
            "fenced_ledger_entry_count": 0,
            "pre_restore_event_head_count": 0,
            "effect_inventory": [],
            "effect_inventory_count": 0,
            "worker_checkpoint_count": 0,
            "protected_b64": "cA",
            "payload_b64": "cA",
            "signature_b64": "cA",
            "signer_key_id_digest": signer_digest,
            "envelope_digest": attestation_digest,
            "authentication_audit_id": audit_id,
            "nomos_decision_id": nomos_id,
            "operator_separation_subject": "restore-operator",
            "idempotency_key_hash": domain_digest(
                "okg-external-action-idempotency-key-v1", "restore-create"
            ),
            "request_hash": request_hash,
            "old_epoch_lease_envelope_digest": old_lease_digest,
            "new_epoch_lease_envelope_digest": new_lease_digest,
            "worker_quiescence_envelope_digest": worker_digest,
            "provider_authority_fence_envelope_digest": provider_fence_digest,
            "restore_evidence": evidence,
        }
        conn.execute("RESET ROLE")
        conn.execute("SET LOCAL ROLE okg_external_action_api")
        _set_verifier_capability(conn)
        created = conn.execute(
            "SELECT okg.external_action_restore_create(%s::jsonb)",
            (j(command),),
        ).fetchone()[0]
        assert created["replay"] is False
        assert created["deployment"] == deployment
        assert created["restore_id"] == restore_id
        assert created["state"] == "collecting_checkpoints"
        assert created["quarantine_state"] == "blocked"
        assert created["service_mode"] == "restore_only"
        assert created["restore_attestation_envelope_digest"] == attestation_digest
        assert created["effect_inventory_digest"] == empty_inventory_digest
        assert created["effect_inventory_count"] == 0
        assert created["expected_checkpoint_set_digest"] == empty_checkpoint_digest
        assert created["expected_checkpoint_count"] == 0
        assert created["accepted_checkpoint_count"] == 0

        conn.execute("RESET ROLE")
        session = conn.execute(
            """
            SELECT status, restore_attestation_envelope_digest,
                   effect_inventory_digest, effect_inventory_count
              FROM okg.external_action_restore_sessions
             WHERE deployment_name = %s AND restore_id = %s
            """,
            (deployment, restore_id),
        ).fetchone()
        assert session == ("staging", attestation_digest, empty_inventory_digest, 0)
        assert conn.execute(
            """
            SELECT count(*) FROM okg.external_action_restore_attestations
             WHERE deployment_name = %s AND restore_id = %s
            """,
            (deployment, restore_id),
        ).fetchone() == (1,)

        conn.execute("SET LOCAL ROLE okg_external_action_api")
        replay = conn.execute(
            "SELECT okg.external_action_restore_create(%s::jsonb)",
            (j(command),),
        ).fetchone()[0]
        assert replay == {
            "replay": True,
            "deployment": deployment,
            "restore_id": restore_id,
            "state": "collecting_checkpoints",
            "quarantine_state": "blocked",
            "service_mode": "restore_only",
            "restore_attestation_envelope_digest": attestation_digest,
            "effect_inventory_digest": empty_inventory_digest,
            "effect_inventory_count": 0,
            "expected_checkpoint_set_digest": empty_checkpoint_digest,
            "expected_checkpoint_count": 0,
            "accepted_checkpoint_count": 0,
        }


def test_postgres_identity_reenable_barriers_reject_old_jwt_and_mtls(
    okg_migrated: str,
) -> None:
    """Disable -> reenable never revives a credential inside one transaction.

    The exact provider and binding rows are locked by the authority RPC.  A
    token ``iat`` or certificate ``notBefore`` from before the reenable
    barriers is rejected even when both rows are active again; a newly issued
    credential after both barriers succeeds.
    """

    deployment = "external-action-credential-race-" + uuid4().hex[:12]
    provider_id = "provider-" + uuid4().hex[:12]
    binding_id = "binding-" + uuid4().hex[:12]
    decision_id = "decision-" + uuid4().hex[:12]
    old_jwt_audit_id = "audit-jwt-old-" + uuid4().hex[:12]
    new_jwt_audit_id = "audit-jwt-new-" + uuid4().hex[:12]
    old_mtls_audit_id = "audit-mtls-old-" + uuid4().hex[:12]
    new_mtls_audit_id = "audit-mtls-new-" + uuid4().hex[:12]
    old_generation_audit_id = "audit-generation-old-" + uuid4().hex[:12]
    old_not_before_audit_id = "audit-not-before-old-" + uuid4().hex[:12]
    digest_a = "sha256:" + ("a" * 64)
    digest_b = "sha256:" + ("b" * 64)
    issuer = "https://issuer.example"

    with db_connect(okg_migrated) as conn:
        _bind_verifier_role(conn, deployment)
        _set_verifier_capability(conn)
        conn.execute(
            """
            INSERT INTO okg.nomos_identity_providers
              (deployment_name, provider_id, provider_type, issuer,
               subject_claim, status, metadata)
            VALUES (%s, %s, 'oidc', %s, 'sub', 'active',
                    jsonb_build_object(
                      'credential_generation_digest', %s::text,
                      'certificate_thumbprint', 'cert-old'))
            """,
            (deployment, provider_id, issuer, digest_a),
        )
        conn.execute(
            """
            INSERT INTO okg.nomos_subject_bindings
              (deployment_name, binding_id, provider_id, issuer,
               claim_path, claim_value, subject, scope, status, metadata)
            VALUES (%s, %s, %s, %s, 'sub', 'subject', 'subject',
                    'external_action', 'active', jsonb_build_object(
                      'credential_generation_digest', %s::text,
                      'certificate_thumbprint', 'cert-old'))
            """,
            (deployment, binding_id, provider_id, issuer, digest_a),
        )
        conn.execute(
            """
            INSERT INTO okg.nomos_decisions
              (deployment_name, decision_id, input_hash, subject, action,
               classification, effect, reason_code, enforcement_point,
               policy_fingerprint)
            VALUES (%s, %s, %s, 'subject', 'external_action.propose',
                    'external_action', 'allow', 'credential_race_test',
                    'test', %s)
            """,
            (deployment, decision_id, digest_a, digest_a),
        )

        def insert_audit(
            audit_id: str,
            method: str,
            *,
            issued_at: datetime | None = None,
            not_before: datetime | None = None,
            thumbprint: str | None = None,
            generation: str = digest_a,
        ) -> None:
            def timestamp(value: datetime) -> str:
                return value.astimezone(UTC).isoformat(timespec="microseconds").replace(
                    "+00:00", "Z"
                )

            audit = {
                "deployment": deployment,
                "authentication_audit_id": audit_id,
                "principal": "subject",
                "separation_subject": "subject",
                "nomos_provider_id": provider_id,
                "nomos_subject_binding_id": binding_id,
                "authentication_method_code": method,
                "issuer_or_trust_domain_digest": digest_a,
                "sender_key_or_certificate_thumbprint": thumbprint,
                "token_or_certificate_id_digest": digest_a,
                "auth_config_digest": digest_a,
                "result_code": "authenticated",
                "provider_status": "active",
                "binding_status": "active",
                "provider_updated_at": timestamp(provider_barrier),
                "binding_updated_at": timestamp(binding_barrier),
                "provider_credential_generation_digest": digest_b
                if generation == digest_b
                else digest_a,
                "binding_credential_generation_digest": digest_b
                if generation == digest_b
                else digest_a,
                "credential_generation_digest": generation,
                "presented_credential_generation_digest": generation,
                "token_issued_at": timestamp(issued_at) if issued_at else None,
                "certificate_thumbprint": thumbprint,
                "certificate_not_before": timestamp(not_before) if not_before else None,
                "certificate_not_after": None,
            }
            assertion, assertion_tag = _verifier_assertion(
                audit,
                operation="external_action.propose",
                transition_digest=digest_a,
                nomos_decision_id=decision_id,
                policy_fingerprint=digest_a,
                provider_id=provider_id,
                provider_status="active",
                provider_updated_at=timestamp(provider_barrier),
                binding_id=binding_id,
                binding_status="active",
                binding_updated_at=timestamp(binding_barrier),
                nomos_input_hash=digest_a,
                authentication_audit_id=audit_id,
            )
            conn.execute(
                """
                INSERT INTO okg.external_action_auth_audits (
                  authentication_audit_id, deployment_name, request_id, principal,
                  separation_subject, nomos_provider_id,
                  nomos_subject_binding_id, authentication_method_code,
                  issuer_or_trust_domain_digest,
                  sender_key_or_certificate_thumbprint,
                  token_or_certificate_id_digest, auth_config_digest,
                  result_code, provider_status, binding_status,
                  provider_updated_at, binding_updated_at,
                  provider_credential_generation_digest,
                  binding_credential_generation_digest,
                  credential_generation_digest,
                  presented_credential_generation_digest, token_issued_at,
                  certificate_thumbprint, certificate_not_before,
                  nomos_decision_id, verifier_assertion_nonce_digest,
                  verifier_assertion_expires_at, verifier_transition_digest,
                  verifier_policy_input_digest, verifier_required_obligations_digest,
                  verifier_completed_obligations_digest,
                  verifier_missing_obligations_digest, verifier_assertion_tag,
                  verifier_capability_generation
                ) VALUES (
                  %s, %s, %s, 'subject', 'subject', %s, %s, %s, %s,
                  %s, %s, %s, 'authenticated', 'active', 'active',
                  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                  %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    audit_id,
                    deployment,
                    "request-" + audit_id,
                    provider_id,
                    binding_id,
                    method,
                    digest_a,
                    thumbprint,
                    digest_a,
                    digest_a,
                    provider_barrier,
                    binding_barrier,
                    generation,
                    generation,
                    generation,
                    generation,
                    issued_at,
                    thumbprint,
                    not_before,
                    decision_id,
                    assertion["nonce_digest"], assertion["expires_at"],
                    assertion["transition_digest"], assertion["nomos_input_hash"],
                    assertion["required_obligations_digest"],
                    assertion["completed_obligations_digest"],
                    assertion["missing_obligations_digest"], assertion_tag,
                    assertion["capability_generation"],
                ),
            )

        provider_barrier, binding_barrier = conn.execute(
            "SELECT p.updated_at, b.updated_at "
            "FROM okg.nomos_identity_providers p "
            "JOIN okg.nomos_subject_bindings b ON b.provider_id = p.provider_id "
            "AND b.deployment_name = p.deployment_name "
            "WHERE p.deployment_name = %s AND p.provider_id = %s "
            "AND b.binding_id = %s",
            (deployment, provider_id, binding_id),
        ).fetchone()
        initial_issued_at = max(provider_barrier, binding_barrier)
        insert_audit(
            old_jwt_audit_id,
            "jwt_dpop",
            issued_at=initial_issued_at,
            generation=digest_a,
        )
        insert_audit(
            old_mtls_audit_id,
            "mtls",
            not_before=initial_issued_at,
            thumbprint="cert-old",
            generation=digest_a,
        )

        conn.execute("SET LOCAL ROLE okg_external_action_api")
        _set_verifier_capability(conn)
        for audit_id in (old_jwt_audit_id, old_mtls_audit_id):
            assert conn.execute(
                "SELECT okg.external_action_nomos_authority_snapshot(%s, %s, %s, %s, %s)",
                (deployment, audit_id, decision_id, "external_action.propose", digest_a),
            ).fetchone()[0]["effect"] == "allow"

        conn.execute("RESET ROLE")
        # Both updates happen in this transaction.  The database-owned
        # triggers move updated_at monotonically for disable and reenable;
        # there is no committed window in which a stale audit can be reused.
        conn.execute(
            "UPDATE okg.nomos_identity_providers SET status = 'disabled' "
            "WHERE deployment_name = %s AND provider_id = %s",
            (deployment, provider_id),
        )
        conn.execute(
            "UPDATE okg.nomos_subject_bindings SET status = 'disabled' "
            "WHERE deployment_name = %s AND binding_id = %s",
            (deployment, binding_id),
        )
        conn.execute(
            "UPDATE okg.nomos_identity_providers SET status = 'active' "
            ", metadata = jsonb_build_object("
            "'credential_generation_digest', %s::text, "
            "'certificate_thumbprint', 'cert-new') "
            "WHERE deployment_name = %s AND provider_id = %s",
            (digest_b, deployment, provider_id),
        )
        conn.execute(
            "UPDATE okg.nomos_subject_bindings SET status = 'active' "
            ", metadata = jsonb_build_object("
            "'credential_generation_digest', %s::text, "
            "'certificate_thumbprint', 'cert-new') "
            "WHERE deployment_name = %s AND binding_id = %s",
            (digest_b, deployment, binding_id),
        )
        provider_reenabled, binding_reenabled = conn.execute(
            "SELECT p.updated_at, b.updated_at "
            "FROM okg.nomos_identity_providers p "
            "JOIN okg.nomos_subject_bindings b ON b.provider_id = p.provider_id "
            "AND b.deployment_name = p.deployment_name "
            "WHERE p.deployment_name = %s AND p.provider_id = %s "
            "AND b.binding_id = %s",
            (deployment, provider_id, binding_id),
        ).fetchone()
        assert provider_reenabled > provider_barrier
        assert binding_reenabled > binding_barrier

        # Isolate the generation barrier from the timestamps: this credential
        # was presented after the reenable, but it still carries the retired
        # provider/binding generation.  It must not become valid merely because
        # its JWT ``iat`` is fresh.
        new_issued_at = max(provider_reenabled, binding_reenabled) + timedelta(microseconds=1)
        provider_barrier, binding_barrier = provider_reenabled, binding_reenabled
        insert_audit(
            old_generation_audit_id,
            "jwt_dpop",
            issued_at=new_issued_at,
            generation=digest_a,
        )
        insert_audit(
            old_not_before_audit_id,
            "mtls",
            not_before=initial_issued_at,
            thumbprint="cert-new",
            generation=digest_b,
        )
        conn.execute("SET LOCAL ROLE okg_external_action_api")
        for audit_id in (
            old_jwt_audit_id,
            old_mtls_audit_id,
            old_generation_audit_id,
            old_not_before_audit_id,
        ):
            conn.execute("SAVEPOINT stale_credential_authority")
            with pytest.raises(
                psycopg.errors.InvalidAuthorizationSpecification
            ) as stale:
                conn.execute(
                    "SELECT okg.external_action_nomos_authority_snapshot(%s, %s, %s, %s, %s)",
                    (deployment, audit_id, decision_id, "external_action.propose", digest_a),
                )
            assert stale.value.sqlstate == "28000"
            conn.execute("ROLLBACK TO SAVEPOINT stale_credential_authority")

        conn.execute("RESET ROLE")
        insert_audit(
            new_jwt_audit_id,
            "jwt_dpop",
            issued_at=new_issued_at,
            generation=digest_b,
        )
        insert_audit(
            new_mtls_audit_id,
            "mtls",
            not_before=new_issued_at,
            thumbprint="cert-new",
            generation=digest_b,
        )
        conn.execute("SET LOCAL ROLE okg_external_action_api")
        for audit_id in (new_jwt_audit_id, new_mtls_audit_id):
            authority = conn.execute(
                "SELECT okg.external_action_nomos_authority_snapshot(%s, %s, %s, %s, %s)",
                (deployment, audit_id, decision_id, "external_action.propose", digest_a),
            ).fetchone()[0]
            assert authority["effect"] == "allow"


def _approved_claim_past_deadline(
    conn: psycopg.Connection[Any], deployment: str, *, deadline_seconds: float
) -> dict[str, Any]:
    """Submit and approve one proposal through the SQL RPCs, then let
    ``execute_before`` pass.

    Returns the worker's prepare and claim commands.  The claim command's
    prepared fields come from a ``prepare_claim`` made while the deadline was
    still ahead, so a later claim models a worker whose request reaches the
    database after the deadline.  The caller's transaction is left under
    ``okg_external_action_api``; nothing is committed.
    """

    provider_id = "provider-" + uuid4().hex[:12]
    bindings = {
        subject: "binding-" + uuid4().hex[:12]
        for subject in ("subject-proposer", "subject-approver", "subject-worker")
    }
    audits = {
        name: "audit-" + uuid4().hex[:12]
        for name in ("propose", "decide", "execute")
    }
    nomos = {
        name: "nomos-" + uuid4().hex[:12]
        for name in ("propose", "decide", "execute")
    }
    proposal_id = "proposal-" + uuid4().hex
    decision_id = "decision-" + uuid4().hex
    execution_id = "execution-" + uuid4().hex
    digest = "sha256:" + ("a" * 64)
    lease_digest = "sha256:" + ("e" * 64)
    issuer = "https://issuer.example"
    issuer_digest = "sha256:" + hashlib.sha256(issuer.encode()).hexdigest()

    def stamp(value: datetime) -> str:
        return value.astimezone(UTC).isoformat(timespec="microseconds").replace(
            "+00:00", "Z"
        )

    def j(value: object) -> str:
        return json.dumps(value, separators=(",", ":"))

    database_now = conn.execute("SELECT clock_timestamp()").fetchone()[0]
    review_expires = stamp(database_now + timedelta(hours=1))
    execute_before = stamp(database_now + timedelta(seconds=deadline_seconds))
    contract = {
        "action_type": "ticket.update",
        "action_schema_digest": digest,
        "executor_contract_digest": digest,
        "payload_renderer_digest": digest,
        "governed_tool_binding_ref": "tool:ticket",
        "governed_tool_binding_digest": digest,
        "provider_connection_ref": "connection:ticket",
        "provider_connection_contract_digest": digest,
        "provider_capability_digest": digest,
        "provider_operation_id": "provider-op",
        "provider_operation_descriptor_digest": digest,
        "provider_replay_mode": "lookup_or_idempotent_retry",
        "target_constraint": {},
        "minimum_risk_tier": "low",
        "review_and_execution_limits": {
            "maximum_execution_window_seconds": 7200,
            "maximum_review_lifetime_seconds": 7200,
            "reporting_attention_seconds": 3600,
            "reconcile_not_before_seconds": 1,
        },
        "diff_field_codes": [],
        "allowed_summary_codes": ["review_requested"],
        "transition_reason_codes": [],
        "outcome_reason_rules": {
            "provider_receipt_observed": {
                "allowed_outcomes": ["succeeded"],
                "provider_receipt": "required",
            },
        },
    }
    contract_digest = action_contract_digest(contract)
    payload_hash = domain_digest("okg-external-action-payload-v1", {"target": "T1-1"})
    effect_key = effect_instance_key(deployment, "subject-proposer", digest)
    submission = {
        "automation_id": "automation",
        "automation_version": "1.0.0",
        "run_id": "run-1",
        "correlation_id": "corr-1",
        "action_type": "ticket.update",
        "target_ref": "ticket:T-1",
        "risk_tier": "low",
        "action_schema_digest": digest,
        "action_contract_digest": contract_digest,
        "original_payload_hash": payload_hash,
        "requested_review_expires_at": review_expires,
        "summary_code": "review_requested",
        "evidence_digests": [digest],
    }
    content_hash = submission_content_hash(submission)
    binding_fields = {
        "executor_contract_digest": digest,
        "payload_renderer_digest": digest,
        "governed_tool_binding_digest": digest,
        "provider_connection_contract_digest": digest,
        "provider_capability_digest": digest,
        "provider_operation_id": "provider-op",
        "provider_operation_descriptor_digest": digest,
    }
    submit_request_hash = domain_digest(
        "okg-external-action-proposal-request-v1",
        {
            "deployment": deployment,
            "execution_epoch": 1,
            "effect_instance_key": effect_key,
            "submission_content_hash": content_hash,
            "proposer_separation_subject": "subject-proposer",
            **{key: submission[key] for key in (
                "automation_id", "automation_version", "run_id",
                "correlation_id", "action_type", "target_ref", "risk_tier",
                "action_schema_digest", "action_contract_digest",
            )},
            **binding_fields,
            "original_payload_hash": payload_hash,
            "requested_review_expires_at": review_expires,
            "summary_code": "review_requested",
            "evidence_digests": [digest],
        },
    )
    approve_request_hash = domain_digest(
        "okg-external-action-transition-request-v1",
        {
            "operation": "approve",
            "action_contract_digest": contract_digest,
            "expected_lifecycle_version": None,
            "expected_payload_version": None,
            "expected_payload_hash": None,
            "expected_decision_id": None,
            "expected_decision_binding_hash": None,
            "replacement_payload_hash": None,
            "execute_before": execute_before,
            "reason_code": None,
        },
    )
    if_match = f'"lifecycle:2;payload-version:1;payload:{payload_hash}"'
    support = {
        "supported_action_schema_digest": digest,
        "supported_action_contract_digest": contract_digest,
        "supported_executor_contract_digest": digest,
        "supported_payload_renderer_digest": digest,
        "supported_governed_tool_binding_digest": digest,
        "supported_provider_connection_contract_digest": digest,
        "supported_provider_capability_digest": digest,
        "supported_provider_operation_id": "provider-op",
        "supported_provider_operation_descriptor_digest": digest,
    }
    claim_request_hash = domain_digest(
        "okg-external-action-transition-request-v1",
        {
            "worker_separation_subject": "subject-worker",
            "decision_id": decision_id,
            "approved_lifecycle_version": 2,
            "approved_payload_version": 1,
            "approved_payload_hash": payload_hash,
            **support,
            "if_match": if_match,
        },
    )
    request_hashes = {
        "propose": submit_request_hash,
        "decide": approve_request_hash,
        "execute": claim_request_hash,
    }
    subjects = {
        "propose": "subject-proposer",
        "decide": "subject-approver",
        "execute": "subject-worker",
    }

    _bind_verifier_role(conn, deployment)
    _set_verifier_capability(conn)
    conn.execute(
        "INSERT INTO okg.nomos_identity_providers (deployment_name, provider_id,"
        " provider_type, issuer, subject_claim, status, metadata)"
        " VALUES (%s, %s, 'oidc', %s, 'sub', 'active', %s::jsonb)",
        (deployment, provider_id, issuer, j({"credential_generation_digest": digest})),
    )
    for subject, binding_id in bindings.items():
        conn.execute(
            "INSERT INTO okg.nomos_subject_bindings (deployment_name, binding_id,"
            " provider_id, issuer, claim_path, claim_value, subject, scope, status,"
            " metadata) VALUES (%s, %s, %s, %s, 'sub', %s, %s, %s, 'active', %s::jsonb)",
            (
                deployment, binding_id, provider_id, issuer, subject, subject,
                deployment, j({"credential_generation_digest": digest}),
            ),
        )
    for name, subject in subjects.items():
        conn.execute(
            "INSERT INTO okg.nomos_decisions (deployment_name, decision_id,"
            " input_hash, subject, action, classification, effect, reason_code,"
            " enforcement_point, policy_fingerprint) VALUES (%s, %s, %s, %s, %s,"
            " 'external_action', 'allow', 'test_allow', 'test', %s)",
            (deployment, nomos[name], digest, subject, "external_action." + name, digest),
        )
        provider_updated_at, binding_updated_at = conn.execute(
            "SELECT p.updated_at, b.updated_at FROM okg.nomos_identity_providers p"
            " JOIN okg.nomos_subject_bindings b ON b.deployment_name = p.deployment_name"
            " AND b.provider_id = p.provider_id AND b.issuer = p.issuer"
            " WHERE p.deployment_name = %s AND p.provider_id = %s AND b.binding_id = %s",
            (deployment, provider_id, bindings[subject]),
        ).fetchone()
        audit = {
            "deployment": deployment,
            "authentication_audit_id": audits[name],
            "request_id": "request-" + audits[name],
            "principal": subject,
            "separation_subject": subject,
            "nomos_provider_id": provider_id,
            "nomos_subject_binding_id": bindings[subject],
            "authentication_method_code": "jwt_dpop",
            "issuer_or_trust_domain_digest": issuer_digest,
            "auth_config_digest": digest,
            "result_code": "authenticated",
            "provider_status": "active",
            "binding_status": "active",
            "provider_updated_at": stamp(provider_updated_at),
            "binding_updated_at": stamp(binding_updated_at),
            "provider_credential_generation_digest": digest,
            "binding_credential_generation_digest": digest,
            "credential_generation_digest": digest,
            "presented_credential_generation_digest": digest,
            "token_issued_at": stamp(max(provider_updated_at, binding_updated_at)),
            "nomos_decision_id": nomos[name],
        }
        assertion, tag = _verifier_assertion(
            audit,
            operation="external_action." + name,
            transition_digest=request_hashes[name],
            nomos_decision_id=nomos[name],
            policy_fingerprint=digest,
            provider_id=provider_id,
            provider_status="active",
            provider_updated_at=stamp(provider_updated_at),
            binding_id=bindings[subject],
            binding_status="active",
            binding_updated_at=stamp(binding_updated_at),
            nomos_input_hash=digest,
            authentication_audit_id=audits[name],
        )
        audit["verifier_assertion"] = assertion
        audit["verifier_assertion_tag"] = tag
        conn.execute(
            "SELECT okg.external_action_record_auth_audit(%s::jsonb)", (j(audit),)
        )
    conn.execute(
        "INSERT INTO okg.external_action_execution_epoch_leases (deployment_name,"
        " epoch, lease_sequence, issued_at, expires_at, signer_separation_subject,"
        " nonce, protected_b64, payload_b64, signature_b64, signer_key_id_digest,"
        " envelope_digest, verification_source, verification_config_digest,"
        " verification_login_binding_digest, authentication_audit_id,"
        " nomos_decision_id, store_record) VALUES (%s, 1, 1, %s, %s,"
        " 'subject-worker', %s, 'protected', 'payload', 'signature', %s, %s,"
        " 'strict_authenticated_api', %s, %s, %s, %s, %s::jsonb)",
        (
            deployment, database_now, database_now + timedelta(hours=1),
            "nonce-" + uuid4().hex, digest, lease_digest, digest, digest,
            audits["execute"], nomos["execute"], j({"envelope_digest": lease_digest}),
        ),
    )

    conn.execute("SET LOCAL ROLE okg_external_action_api")
    conn.execute(
        "SELECT okg.external_action_submit(%s::jsonb)",
        (j({
            "deployment": deployment,
            "proposal_id": proposal_id,
            "execution_epoch": 1,
            "effect_instance_key": effect_key,
            "submission_content_hash": content_hash,
            "submission_request_hash": submit_request_hash,
            "submission_idempotency_key_hash": digest,
            "proposer_principal": "subject-proposer",
            "proposer_separation_subject": "subject-proposer",
            **submission,
            **binding_fields,
            "effective_review_expires_at": review_expires,
            "current_payload_hash": payload_hash,
            "canonical_payload": {"__okg_bytes_b64": "eyJ0YXJnZXQiOiJUMS0xIn0="},
            "authentication_audit_id": audits["propose"],
            "nomos_decision_id": nomos["propose"],
            "policy_digest": digest,
            "action_contract_record": contract,
        }),),
    )
    approved = conn.execute(
        "SELECT okg.external_action_review_transition(%s::jsonb)",
        (j({
            "deployment": deployment,
            "proposal_id": proposal_id,
            "operation": "approve",
            "actor_principal": "subject-approver",
            "actor_separation_subject": "subject-approver",
            "idempotency_key_hash": digest,
            "transition_request_hash": approve_request_hash,
            "authentication_audit_id": audits["decide"],
            "nomos_decision_id": nomos["decide"],
            "policy_digest": digest,
            "decision_id": decision_id,
            "execute_before": execute_before,
        }),),
    ).fetchone()[0]
    assert (approved["review_state"], approved["execution_state"]) == ("approved", "ready")

    claim: dict[str, Any] = {
        "deployment": deployment,
        "proposal_id": proposal_id,
        "execution_id": execution_id,
        "decision_id": decision_id,
        "worker_principal": "subject-worker",
        "worker_separation_subject": "subject-worker",
        "idempotency_key_hash": "sha256:" + ("b" * 64),
        "request_hash": claim_request_hash,
        "approved_lifecycle_version": 2,
        "approved_payload_version": 1,
        "approved_payload_hash": payload_hash,
        **support,
        "execution_epoch_lease_digest": lease_digest,
        "okg_claim_handoff_receipt_digest": domain_digest(
            "okg-external-action-claim-handoff-receipt-envelope-v1",
            {"protected_b64": "protected", "payload_b64": "payload",
             "signature_b64": "signature"},
        ),
        "handoff_protected_b64": "protected",
        "handoff_payload_b64": "payload",
        "handoff_signature_b64": "signature",
        "authentication_audit_id": audits["execute"],
        "nomos_decision_id": nomos["execute"],
        "policy_digest": digest,
        "if_match": if_match,
    }
    binding_projection = {
        "deployment": deployment,
        "execution_epoch": 1,
        "execution_epoch_lease_digest": lease_digest,
        "effect_instance_key": effect_key,
        "proposal_id": proposal_id,
        "decision_id": decision_id,
        "execution_id": execution_id,
        "worker_separation_subject": "subject-worker",
        "approved_lifecycle_version": 2,
        "approved_payload_version": 1,
        "approved_payload_hash": payload_hash,
        "action_type": "ticket.update",
        "target_ref": "ticket:T-1",
        "action_schema_digest": digest,
        "action_contract_digest": contract_digest,
        **binding_fields,
        "policy_digest": digest,
    }
    provider_projection = {
        "deployment": deployment,
        "effect_instance_key": effect_key,
        "approved_payload_hash": payload_hash,
        "action_contract_digest": contract_digest,
        "provider_connection_contract_digest": digest,
        "provider_operation_id": "provider-op",
        "provider_operation_descriptor_digest": digest,
    }
    conn.execute("RESET ROLE")
    claim["execution_binding_hash"] = conn.execute(
        "SELECT okg.external_action_hash(%s, %s::jsonb)",
        ("okg-external-action-execution-binding-v1", j(binding_projection)),
    ).fetchone()[0]
    claim["provider_idempotency_key"], claim["provider_idempotency_key_digest"] = conn.execute(
        "SELECT okg.external_action_effect_key(%s::jsonb),"
        " okg.external_action_hash(%s, %s::jsonb)",
        (j(provider_projection), "okg-external-action-effect-v1", j(provider_projection)),
    ).fetchone()
    conn.execute("SET LOCAL ROLE okg_external_action_api")
    prepare = {
        key: claim[key]
        for key in (
            "deployment", "proposal_id", "execution_id", "decision_id",
            "worker_principal", "worker_separation_subject",
            "approved_lifecycle_version", "approved_payload_version",
            "approved_payload_hash", "execution_epoch_lease_digest",
            "execution_binding_hash", "request_hash",
            "authentication_audit_id", "nomos_decision_id", "policy_digest",
        )
    }
    prepared = conn.execute(
        "SELECT okg.external_action_prepare_claim(%s::jsonb)", (j(prepare),)
    ).fetchone()[0]
    claim.update({
        "claimed_at": prepared["claimed_at"],
        "previous_event_hash": prepared["previous_event_hash"],
        "decision_event_hash": prepared["decision_event_hash"],
        "claim_event_hash": prepared["claim_event_hash"],
        "event_chain_head_hash": prepared["event_chain_head_hash"],
        "claim_lifecycle_version": prepared["claim_lifecycle_version"],
        "event_sequence": prepared["event_sequence"],
        "event_hash": prepared["claim_event_hash"],
    })

    remaining = (
        datetime.fromisoformat(execute_before.replace("Z", "+00:00"))
        - conn.execute("SELECT clock_timestamp()").fetchone()[0]
    ).total_seconds()
    assert remaining > 0, "the fixture deadline passed before the claim was prepared"
    time.sleep(remaining + 0.05)
    return {
        "proposal_id": proposal_id,
        "prepare": prepare,
        "claim": claim,
        "execute_before": execute_before,
    }


def _committed_expiry_record(dsn: str, proposal_id: str) -> dict[str, Any]:
    """Read the committed expiry record from a separate connection.

    A second connection sees only committed rows, so this proves the
    ``approval_expired`` event survived the refusal rather than living in an
    uncommitted transaction.
    """

    with db_connect(dsn) as observer:
        state = observer.execute(
            "SELECT review_state, execution_state FROM okg.external_action_proposals"
            " WHERE proposal_id = %s",
            (proposal_id,),
        ).fetchone()
        events = observer.execute(
            "SELECT deadline_kind, before_review_state, after_review_state,"
            " before_execution_state, after_execution_state, actor_kind"
            " FROM okg.external_action_events"
            " WHERE proposal_id = %s AND event_type = 'approval_expired'"
            " ORDER BY sequence",
            (proposal_id,),
        ).fetchall()
        claims = observer.execute(
            "SELECT count(*) FROM okg.external_action_executions WHERE proposal_id = %s",
            (proposal_id,),
        ).fetchone()[0]
    return {"state": tuple(state), "events": events, "claims": claims}


@pytest.mark.parametrize("rpc", ["prepare_claim", "claim_command"])
def test_postgres_late_claim_records_expiry_once_and_is_refused_as_expired(
    okg_migrated: str, rpc: str
) -> None:
    """A claim that reaches the database after ``execute_before`` (issue #1925).

    The worker never re-reads the proposal, so only the claim RPC itself can
    notice the deadline.  It must commit exactly one ``approval_expired``
    event and refuse with the typed ``proposal_expired`` code; before the fix
    it raised SQLSTATE 22008, which rolled the refusal back without an event
    and surfaced as the generic unavailable-authority code.  Failures that are
    not the deadline, including a genuine database failure, stay the generic
    code and record nothing.
    """

    deployment = "external-action-late-claim-" + uuid4().hex[:12]
    with db_connect(okg_migrated) as conn:
        late = _approved_claim_past_deadline(conn, deployment, deadline_seconds=2.0)
        conn.commit()
        proposal_id = late["proposal_id"]
        request = late["prepare"] if rpc == "prepare_claim" else late["claim"]
        store = PostgresExternalActionStore(
            connection=conn,
            deployment=deployment,
            verifier_capability=_TEST_VERIFIER_CAPABILITY,
            check_contract=False,
        )
        call = getattr(store, rpc)

        # Failures unrelated to the deadline keep the generic code and record
        # nothing, even though this proposal's deadline has passed: a missing
        # proposal, and a genuine database failure -- a lock wait cut off by
        # lock_timeout while another session holds the deployment lock.
        untouched = {"state": ("approved", "ready"), "events": [], "claims": 0}
        with pytest.raises(ExternalActionStoreError) as unrelated:
            call({**request, "proposal_id": "proposal-" + uuid4().hex})
        assert unrelated.value.code == "review_authority_unavailable"
        assert unrelated.value.sqlstate == "P0002"
        assert _committed_expiry_record(okg_migrated, proposal_id) == untouched
        with db_connect(okg_migrated) as blocker:
            blocker.execute(
                "SELECT pg_advisory_xact_lock("
                "hashtextextended('okg.external_action.deployment:' || %s, 0))",
                (deployment,),
            )
            conn.execute("SET lock_timeout = '200ms'")
            conn.commit()
            with pytest.raises(ExternalActionStoreError) as unavailable:
                call(request)
            assert unavailable.value.code == "review_authority_unavailable"
            assert unavailable.value.sqlstate == "55P03"
            blocker.rollback()
        conn.execute("RESET lock_timeout")
        conn.commit()
        assert _committed_expiry_record(okg_migrated, proposal_id) == untouched

        with pytest.raises(ExternalActionStoreError) as refused:
            call(request)
        assert refused.value.code == "proposal_expired"
        expected = {
            "state": ("expired", "unavailable"),
            "events": [(
                "execute_before", "approved", "expired", "ready", "unavailable",
                "system_clock",
            )],
            "claims": 0,
        }
        assert _committed_expiry_record(okg_migrated, proposal_id) == expected

        # A repeated late call is refused the same way and adds no event.
        with pytest.raises(ExternalActionStoreError) as repeated:
            call(request)
        assert repeated.value.code == "proposal_expired"
        assert _committed_expiry_record(okg_migrated, proposal_id) == expected


def test_service_lock_commits_the_late_claim_refusal_and_rolls_back_others() -> None:
    """The durable service lock commits only a refusal marked to commit.

    ``ExternalActionService.claim`` holds one database transaction through
    ``_DatabaseLock``.  The late-claim refusal must commit the expiry it
    recorded (issue #1925); every other error must still roll back; and the
    lock must be released even when that commit itself fails.
    """

    from types import SimpleNamespace

    from okg.substrate.external_actions.service import (
        ExternalActionError,
        _committed_claim_expiry,
    )
    from okg.substrate.external_actions.store import _DatabaseLock

    # The shared unit of work reads the transaction status before it opens,
    # before and after it commits, and after it rolls back (issue #2574).
    status = SimpleNamespace(transaction_status=psycopg.pq.TransactionStatus.IDLE)

    class Transaction:
        def __init__(self, exits: list[Any], fail_commit: bool) -> None:
            self.exits, self.fail_commit = exits, fail_commit

        def __enter__(self) -> "Transaction":
            status.transaction_status = psycopg.pq.TransactionStatus.INTRANS
            return self

        def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
            status.transaction_status = psycopg.pq.TransactionStatus.IDLE
            self.exits.append("rollback" if exc_type is not None else "commit")
            if exc_type is None and self.fail_commit:
                raise psycopg.OperationalError("commit failed")

    def raise_inside(lock: _DatabaseLock, error: Exception) -> None:
        with lock:
            raise error

    def run(error: Exception, *, fail_commit: bool = False) -> tuple[list[Any], BaseException]:
        exits: list[Any] = []
        store = SimpleNamespace(
            _conn=SimpleNamespace(
                transaction=lambda: Transaction(exits, fail_commit),
                info=status,
            ),
            _active_transaction=False,
            _refresh_snapshot=lambda: None,
        )
        lock = _DatabaseLock(store)  # type: ignore[arg-type]
        # The raise lives in a helper so a static analyzer, which does not
        # know that ``pytest.raises`` swallows the error, still sees the
        # checks below as reachable.
        with pytest.raises(Exception) as raised:
            raise_inside(lock, error)
        # The lock is released on every path.  ``_lock`` is re-entrant, so
        # only a different thread proves this thread let go of it.
        acquired: list[bool] = []

        def other_thread() -> None:
            got = lock._lock.acquire(timeout=2)
            acquired.append(got)
            if got:
                lock._lock.release()

        worker = threading.Thread(target=other_thread)
        worker.start()
        worker.join(timeout=5)
        assert acquired == [True], "the lock was not released"
        assert lock._depth == 0 and lock._transaction is None
        assert store._active_transaction is False
        return exits, raised.value

    refusal = _committed_claim_expiry()
    assert (refusal.code, refusal.status) == ("proposal_expired", 410)
    exits, raised = run(refusal)
    assert exits == ["commit"] and raised is refusal

    plain = ExternalActionError("proposal_expired", "approval has expired", status=410)
    exits, raised = run(plain)
    assert exits == ["rollback"] and raised is plain

    exits, raised = run(_committed_claim_expiry(), fail_commit=True)
    assert exits == ["commit"]
    assert isinstance(raised, psycopg.OperationalError)


def test_service_lock_is_released_and_keeps_the_error_when_its_rollback_raises() -> None:
    """A rollback that raises no longer strands the store lock (issue #2347).

    ``_DatabaseLock.__exit__`` used to call the transaction's rollback with no
    ``try/finally``: a rollback that raised (a dropped connection, or
    psycopg's out-of-order nesting error) skipped the lock release, so every
    later store call in the process waited forever, and the rollback error
    replaced the error that caused it. Now the original error propagates with
    the rollback failure noted, and another thread takes the lock.
    """

    from types import SimpleNamespace

    from okg.substrate.db.unit_of_work import rollback_failed
    from okg.substrate.external_actions.service import ExternalActionError
    from okg.substrate.external_actions.store import _DatabaseLock

    status = SimpleNamespace(transaction_status=psycopg.pq.TransactionStatus.IDLE)

    class Transaction:
        def __enter__(self) -> "Transaction":
            status.transaction_status = psycopg.pq.TransactionStatus.INTRANS
            return self

        def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
            assert exc_type is not None
            status.transaction_status = psycopg.pq.TransactionStatus.UNKNOWN
            raise psycopg.OperationalError("rollback failed: server closed the connection")

    store = SimpleNamespace(
        _conn=SimpleNamespace(transaction=Transaction, info=status),
        _active_transaction=False,
        _refresh_snapshot=lambda: None,
    )
    lock = _DatabaseLock(store)  # type: ignore[arg-type]
    error = ExternalActionError("illegal_proposal_transition", "proposal cannot be reviewed")

    def raise_inside() -> None:
        with lock:
            raise error

    with pytest.raises(ExternalActionError) as raised:
        raise_inside()
    assert raised.value is error
    assert raised.value.code == "illegal_proposal_transition"
    assert rollback_failed(error)
    assert any(
        "the rollback after this error also failed: OperationalError: rollback failed"
        in note
        for note in error.__notes__
    )

    acquired: list[bool] = []

    def other_thread() -> None:
        got = lock._lock.acquire(timeout=2)
        acquired.append(got)
        if got:
            lock._lock.release()

    worker = threading.Thread(target=other_thread)
    worker.start()
    worker.join(timeout=5)
    assert acquired == [True], "the store lock was not released"
    assert lock._depth == 0 and lock._transaction is None
    assert store._active_transaction is False
