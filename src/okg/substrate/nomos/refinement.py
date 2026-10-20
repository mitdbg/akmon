"""Nomos trace/refinement helpers for the formal model boundary."""
from __future__ import annotations

from typing import Any, Iterable, Mapping

import psycopg
from psycopg.rows import dict_row

from okg.substrate.nomos.core import PolicyDecision


class NomosTraceError(ValueError):
    """Raised when decision traces violate a Nomos safety check."""


def build_refinement_state(
    decisions: Iterable[PolicyDecision | Mapping[str, Any]],
    *,
    policy_graphs: Iterable[str] | None = None,
    config_graphs: Iterable[str] | None = None,
    compiled_bundles: Iterable[str] | None = None,
    compiled_config_sets: Iterable[str] | None = None,
    active_bundle: str | None = None,
    active_config: Mapping[str, str] | None = None,
    config_activation_decisions: Mapping[str, str] | None = None,
    identity_providers: Iterable[str] | None = None,
    external_claim_envelopes: Iterable[str] | None = None,
    subject_bindings: Iterable[str] | None = None,
    permission_grants: Iterable[str] | None = None,
    postgres_policy_state: Mapping[str, Mapping[str, Any]] | None = None,
    visible_state: Iterable[str] | None = None,
    quarantine_state: Iterable[str] | None = None,
    public_outputs: Iterable[str] | None = None,
    generation_policy_pins: Mapping[str, str] | None = None,
    unsupported: Iterable[str] | None = None,
) -> dict[str, Any]:
    decision_rows = [_decision_mapping(decision) for decision in decisions]
    state = {
        "model": "formal/nomos/Nomos.tla",
        "policyGraphs": sorted(policy_graphs or []),
        "configGraphs": sorted(config_graphs or []),
        "compiledBundles": sorted(compiled_bundles or []),
        "compiledConfigSets": sorted(compiled_config_sets or []),
        "activeBundle": active_bundle,
        "activeConfig": dict(sorted((active_config or {}).items())),
        "configActivationDecisions": dict(
            sorted((config_activation_decisions or {}).items()),
        ),
        "identityProviders": sorted(identity_providers or []),
        "externalClaimEnvelopes": sorted(external_claim_envelopes or []),
        "subjectBindings": sorted(subject_bindings or []),
        "permissionGrants": sorted(permission_grants or []),
        "postgresPolicyState": _sorted_mapping(postgres_policy_state or {}),
        "decisions": sorted(
            decision_rows,
            key=lambda row: row.get("decision_id") or "",
        ),
        "visibleState": sorted(visible_state or []),
        "quarantineState": sorted(quarantine_state or []),
        "publicOutputs": sorted(public_outputs or []),
        "generationPolicyPins": dict(sorted((generation_policy_pins or {}).items())),
        "unsupported": sorted(unsupported or []),
    }
    _assert_required_fields(state)
    return state


def build_refinement_state_from_postgres_rows(
    rows: Mapping[str, Iterable[Mapping[str, Any]]],
) -> dict[str, Any]:
    """Map row-shaped Nomos evidence into the TLA+ refinement surface.

    This helper deliberately consumes plain mappings rather than opening a
    connection. The database-facing scripts can pass real query rows later,
    while unit tests can exercise the same mapping with row-shaped fixtures.
    """
    activations = list(rows.get("activations", []))
    subject_binding_rows = _active_rows(rows.get("subject_bindings", []))
    permission_grant_rows = _active_rows(rows.get("permission_grants", []))
    active_bundle = _single_active_bundle(activations)
    active_config: dict[str, str] = {}
    config_activation_decisions: dict[str, str] = {}
    postgres_policy_state: dict[str, dict[str, Any]] = {}
    for row in activations:
        scope = _required_row_str(row, "scope")
        bundle = _required_row_str(row, "policy_fingerprint")
        config = _required_row_str(row, "config_fingerprint")
        policy_decision_id = _required_row_str(
            row,
            "policy_activation_decision_id",
        )
        config_decision_id = _required_row_str(
            row,
            "config_activation_decision_id",
        )
        active_config[scope] = config
        config_activation_decisions[scope] = config_decision_id
        postgres_policy_state[scope] = {
            "bundle": bundle,
            "config": config,
            "activationDecision": config_decision_id,
            "policyActivationDecision": policy_decision_id,
            "configActivationDecision": config_decision_id,
        }
    return build_refinement_state(
        rows.get("decisions", []),
        policy_graphs=_row_values(rows.get("policy_graphs", []), "policy_bundle_id"),
        config_graphs=_row_values(rows.get("config_graphs", []), "config_graph_id"),
        compiled_bundles=_row_values(
            rows.get("policy_bundles", []),
            "policy_fingerprint",
        ),
        compiled_config_sets=_row_values(
            rows.get("config_sets", []),
            "config_fingerprint",
        ),
        active_bundle=active_bundle,
        active_config=active_config,
        config_activation_decisions=config_activation_decisions,
        identity_providers=_row_values(
            rows.get("identity_providers", []),
            "provider_id",
        ),
        external_claim_envelopes=_row_values(
            rows.get("external_claims", []),
            "claim_fingerprint",
        ),
        subject_bindings=_row_values(subject_binding_rows, "binding_id"),
        permission_grants=_row_values(permission_grant_rows, "grant_id"),
        postgres_policy_state=postgres_policy_state,
        visible_state=_row_values(rows.get("visible_rows", []), "decision_id"),
        quarantine_state=_row_values(rows.get("quarantine_rows", []), "decision_id"),
        public_outputs=_row_values(rows.get("public_outputs", []), "decision_id"),
        generation_policy_pins={
            _required_row_str(row, "generation_id"): _required_row_str(
                row,
                "policy_fingerprint",
            )
            for row in rows.get("generation_policy_pins", [])
        },
        unsupported=_row_values(rows.get("unsupported", []), "name"),
    )


def build_refinement_state_from_postgres(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
) -> dict[str, Any]:
    """Read real Nomos Postgres rows into the refinement surface.

    The TLA+ model represents institutional IAM with finite provider, claim,
    binding, and grant identifiers. We also carry the concrete descriptors in
    ``institutionalIam`` so missing runtime fields are visible to tests and
    reports rather than silently defaulted away.
    """
    rows = _nomos_rows_from_postgres(conn, deployment_name=deployment_name)
    active_bundle = _single_active_bundle(rows["activations"])
    active_config: dict[str, str] = {}
    config_activation_decisions: dict[str, str] = {}
    postgres_policy_state: dict[str, dict[str, Any]] = {}
    for row in rows["activations"]:
        scope = _required_row_str(row, "scope")
        bundle = _required_row_str(row, "policy_fingerprint")
        config = _optional_row_str(row, "config_fingerprint")
        policy_decision_id = _required_row_str(
            row,
            "policy_activation_decision_id",
        )
        config_decision_id = (
            _required_row_str(row, "config_activation_decision_id")
            if config else None
        )
        if config:
            active_config[scope] = config
            config_activation_decisions[scope] = config_decision_id
        postgres_policy_state[scope] = {
            "bundle": bundle,
            "config": config,
            "activationDecision": config_decision_id or policy_decision_id,
            "policyActivationDecision": policy_decision_id,
            "configActivationDecision": config_decision_id,
            "institutionalProviderIds": _row_values(
                rows["identity_providers"],
                "provider_id",
            ),
            "claimFingerprints": _row_values(
                rows["external_claims"],
                "claim_fingerprint",
            ),
            "bindingIds": _row_values(rows["subject_bindings"], "binding_id"),
            "grantIds": _row_values(rows["permission_grants"], "grant_id"),
        }
    state = build_refinement_state(
        rows["decisions"],
        compiled_bundles=_row_values(rows["policy_bundles"], "policy_fingerprint"),
        compiled_config_sets=_row_values(rows["config_sets"], "config_fingerprint"),
        active_bundle=active_bundle,
        active_config=active_config,
        config_activation_decisions=config_activation_decisions,
        identity_providers=_row_values(rows["identity_providers"], "provider_id"),
        external_claim_envelopes=_row_values(
            rows["external_claims"],
            "claim_fingerprint",
        ),
        subject_bindings=_row_values(rows["subject_bindings"], "binding_id"),
        permission_grants=_row_values(rows["permission_grants"], "grant_id"),
        postgres_policy_state=postgres_policy_state,
        unsupported=[],
    )
    state["institutionalIam"] = {
        "providers": _institutional_provider_descriptors(
            rows["identity_providers"],
        ),
        "claims": _institutional_claim_descriptors(rows["external_claims"]),
        "bindings": _institutional_binding_descriptors(rows["subject_bindings"]),
        "grants": _institutional_grant_descriptors(rows["permission_grants"]),
    }
    _assert_no_missing_institutional_fields(state["institutionalIam"])
    return state


def check_noninterference_trace(state: Mapping[str, Any]) -> None:
    decisions = {
        str(row.get("decision_id")): row
        for row in state.get("decisions", [])
        if isinstance(row, Mapping)
    }
    for decision_id in state.get("publicOutputs", []):
        decision = decisions.get(str(decision_id))
        if decision is None:
            raise NomosTraceError(
                f"public output {decision_id!r} has no decision trace"
            )
        if decision.get("classification") not in {"restricted", "private"}:
            continue
        effect = decision.get("effect")
        if effect not in {"redact", "declassify"}:
            raise NomosTraceError(
                f"restricted/private public output {decision_id!r} "
                f"has unsafe effect {effect!r}"
            )
        obligations = decision.get("obligations") or []
        obligation_types = {
            item.get("type") for item in obligations if isinstance(item, Mapping)
        }
        if not obligation_types.intersection({"redaction", "declassification"}):
            raise NomosTraceError(
                f"restricted/private public output {decision_id!r} "
                "lacks redaction/declassification obligation evidence"
            )


def _decision_mapping(decision: PolicyDecision | Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(decision, PolicyDecision):
        return decision.to_dict()
    return dict(decision)


def _sorted_mapping(value: Mapping[str, Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    return {
        str(key): dict(sorted(dict(nested).items()))
        for key, nested in sorted(value.items(), key=lambda item: str(item[0]))
    }


def _row_values(
    rows: Iterable[Mapping[str, Any]],
    key: str,
) -> list[str]:
    return [_required_row_str(row, key) for row in rows]


def _active_rows(rows: Iterable[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    return [
        row
        for row in rows
        if row.get("status") is None or row.get("status") == "active"
    ]


def _required_row_str(row: Mapping[str, Any], key: str) -> str:
    value = row.get(key)
    if not isinstance(value, str) or not value.strip():
        raise NomosTraceError(f"row field {key!r} is required")
    return value


def _optional_row_str(row: Mapping[str, Any], key: str) -> str | None:
    value = row.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise NomosTraceError(f"row field {key!r} must be null or a string")
    return value


def _single_active_bundle(activations: Iterable[Mapping[str, Any]]) -> str | None:
    fingerprints = {
        _required_row_str(row, "policy_fingerprint")
        for row in activations
    }
    if not fingerprints:
        return None
    if len(fingerprints) > 1:
        raise NomosTraceError(
            "refinement fixture contains multiple active policy fingerprints",
        )
    return next(iter(fingerprints))


def _assert_required_fields(state: Mapping[str, Any]) -> None:
    required = {
        "model",
        "policyGraphs",
        "configGraphs",
        "compiledBundles",
        "compiledConfigSets",
        "activeBundle",
        "activeConfig",
        "configActivationDecisions",
        "identityProviders",
        "externalClaimEnvelopes",
        "subjectBindings",
        "permissionGrants",
        "postgresPolicyState",
        "decisions",
        "visibleState",
        "quarantineState",
        "publicOutputs",
        "generationPolicyPins",
        "unsupported",
    }
    missing = sorted(required.difference(state))
    if missing:
        raise NomosTraceError(f"refinement state missing required fields: {missing}")


def _nomos_rows_from_postgres(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
) -> dict[str, list[dict[str, Any]]]:
    queries = {
        "policy_bundles": """
            SELECT policy_fingerprint
              FROM okg.nomos_policy_bundles
             WHERE deployment_name = %s
             ORDER BY policy_fingerprint
        """,
        "config_sets": """
            SELECT config_fingerprint
              FROM okg.nomos_config_sets
             WHERE deployment_name = %s
             ORDER BY config_fingerprint
        """,
        "activations": """
            SELECT p.scope, p.policy_fingerprint,
                   c.config_fingerprint,
                   p.activation_decision_id AS policy_activation_decision_id,
                   c.activation_decision_id AS config_activation_decision_id
              FROM okg.nomos_policy_activations p
              LEFT JOIN okg.nomos_config_activations c
                ON c.deployment_name = p.deployment_name
               AND c.scope = p.scope
               AND c.status = 'active'
             WHERE p.deployment_name = %s
               AND p.status = 'active'
             ORDER BY p.scope
        """,
        "identity_providers": """
            SELECT provider_id, provider_type, issuer, audiences, status,
                   required_assurance, mfa_required, group_claims_approved,
                   metadata_fingerprint, realm, service_principal,
                   key_reference, secret_references, freshness_seconds
              FROM okg.nomos_identity_providers
             WHERE deployment_name = %s
             ORDER BY provider_id
        """,
        "external_claims": """
            SELECT provider_id, provider_type, issuer, external_subject,
                   audiences, claim_fingerprint, verification_status,
                   verification_error, assurance_values, mfa_satisfied,
                   realm, service_principal, nomos_decision_id
              FROM okg.nomos_external_claims
             WHERE deployment_name = %s
             ORDER BY provider_id, claim_fingerprint
        """,
        "subject_bindings": """
            SELECT binding_id, provider_id, issuer, claim_path, claim_value,
                   subject, scope, required_assurance, status
              FROM okg.nomos_subject_bindings
             WHERE deployment_name = %s
               AND status = 'active'
             ORDER BY binding_id
        """,
        "permission_grants": """
            SELECT grant_id, subject, actions, scope, effect,
                   classifications, status
              FROM okg.nomos_permission_grants
             WHERE deployment_name = %s
               AND status = 'active'
             ORDER BY grant_id
        """,
        "decisions": """
            SELECT decision_id, policy_bundle_id, policy_version,
                   policy_fingerprint, input_hash, subject, action,
                   resource, classification, context, effect, reason_code,
                   matched_rule_ids, obligations, enforcement_point
              FROM okg.nomos_decisions
             WHERE deployment_name = %s
             ORDER BY decision_id
        """,
    }
    out: dict[str, list[dict[str, Any]]] = {}
    with conn.cursor(row_factory=dict_row) as cur:
        for name, query in queries.items():
            cur.execute(query, (deployment_name,))
            out[name] = [dict(row) for row in cur.fetchall()]
    return out


def _institutional_provider_descriptors(
    rows: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    return [
        {
            "provider_id": _required_row_str(row, "provider_id"),
            "provider_type": _required_row_str(row, "provider_type"),
            "issuer": _required_row_str(row, "issuer"),
            "audiences": list(row.get("audiences") or []),
            "status": _required_row_str(row, "status"),
            "required_assurance": list(row.get("required_assurance") or []),
            "mfa_required": bool(row.get("mfa_required")),
            "group_claims_approved": bool(row.get("group_claims_approved")),
            "metadata_fingerprint": row.get("metadata_fingerprint"),
            "realm": row.get("realm"),
            "service_principal": row.get("service_principal"),
            "key_reference": row.get("key_reference"),
            "secret_references": list(row.get("secret_references") or []),
            "freshness_seconds": row.get("freshness_seconds"),
        }
        for row in rows
    ]


def _institutional_claim_descriptors(
    rows: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    return [
        {
            "provider_id": _required_row_str(row, "provider_id"),
            "provider_type": _required_row_str(row, "provider_type"),
            "external_subject": _required_row_str(row, "external_subject"),
            "claim_fingerprint": _required_row_str(row, "claim_fingerprint"),
            "verification_status": _required_row_str(row, "verification_status"),
            "verification_error": row.get("verification_error"),
            "assurance_values": list(row.get("assurance_values") or []),
            "mfa_satisfied": bool(row.get("mfa_satisfied")),
            "realm": row.get("realm"),
            "service_principal": row.get("service_principal"),
            "nomos_decision_id": row.get("nomos_decision_id"),
        }
        for row in rows
    ]


def _institutional_binding_descriptors(
    rows: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    return [
        {
            "binding_id": _required_row_str(row, "binding_id"),
            "provider_id": _required_row_str(row, "provider_id"),
            "claim_path": _required_row_str(row, "claim_path"),
            "claim_value": _required_row_str(row, "claim_value"),
            "subject": _required_row_str(row, "subject"),
            "scope": _required_row_str(row, "scope"),
            "required_assurance": list(row.get("required_assurance") or []),
            "status": _required_row_str(row, "status"),
        }
        for row in rows
    ]


def _institutional_grant_descriptors(
    rows: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    return [
        {
            "grant_id": _required_row_str(row, "grant_id"),
            "subject": _required_row_str(row, "subject"),
            "actions": list(row.get("actions") or []),
            "scope": _required_row_str(row, "scope"),
            "effect": _required_row_str(row, "effect"),
            "classifications": list(row.get("classifications") or []),
            "status": _required_row_str(row, "status"),
        }
        for row in rows
    ]


def _assert_no_missing_institutional_fields(value: Mapping[str, Any]) -> None:
    missing: list[str] = []
    for section, rows in value.items():
        if not isinstance(rows, list):
            continue
        for index, row in enumerate(rows):
            if not isinstance(row, Mapping):
                continue
            for key, child in row.items():
                if child is None and key not in {
                    "metadata_fingerprint",
                    "realm",
                    "service_principal",
                    "verification_error",
                    "nomos_decision_id",
                }:
                    missing.append(f"{section}[{index}].{key}")
    if missing:
        raise NomosTraceError(
            "institutional IAM refinement omitted required row fields: "
            + ", ".join(sorted(missing))
        )
