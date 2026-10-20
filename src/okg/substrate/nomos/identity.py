"""Institutional IAM claim mapping for Nomos.

Nomos consumes verified identity evidence from SSO, directory sync,
service accounts, Poros authorities, or local agent identity. It does
not run the login flow or persist bearer credentials.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Mapping, Sequence

from okg.substrate.nomos.core import (
    CompiledPolicyBundle,
    DecisionInput,
    NomosPolicyError,
    PolicyDecision,
    evaluate,
)


IDENTITY_PROVIDER_TYPES = frozenset({
    "agent_local",
    "deployment_extension",
    "kerberos_gssapi",
    "ldap",
    # PACT mcp-principal-mapping: a principal asserted in a declared request
    # header by a transport-authenticated fronting application, whose own SSO
    # established the person. Named explicitly rather than folded into `oidc`
    # (which would claim a token verification that never happened here) or
    # `deployment_extension` (which says nothing about the trust path). The
    # delegation is only as strong as the app token, so a provider of this
    # type is expected to carry trusted_verifiers naming the transport
    # identities permitted to assert principals.
    "mcp_principal_header",
    "oidc",
    "poros_authority",
    "saml",
    "scim",
    "service_account",
    "touchstone_oidc",
})

IDENTITY_PROVIDER_STATUSES = frozenset({
    "active",
    "disabled",
    "revoked",
    "rotating",
})

VALID_CLAIM_STATUS = "valid"

# The reserved subject a BLOCK binding names (`okg nomos binding block`). A
# claim matched by any in-effect binding to this subject is refused with
# BLOCKED_REASON_CODE before subject resolution, so a block overrides every
# role, group or user binding regardless of priority. It is a subject value,
# not a schema change: blocks are ordinary rows in okg.nomos_subject_bindings.
BLOCKED_SUBJECT = "nomos:blocked"
BLOCKED_REASON_CODE = "nomos.identity.principal_blocked"

# A claim that says nothing about having been verified is not a verified
# claim. Absence gets its own status so it can never compare equal to
# VALID_CLAIM_STATUS, and so operators can tell "nobody vouched for this"
# apart from "somebody vouched and the evidence failed a check".
UNVERIFIED_CLAIM_STATUS = "unverified"

# The closed vocabulary of claim verification statuses. A caller-supplied
# status outside this set is rejected rather than carried through into the
# decision reason code, the audit row, and the projected graph.
IDENTITY_CLAIM_STATUSES = frozenset({
    VALID_CLAIM_STATUS,
    UNVERIFIED_CLAIM_STATUS,
    "audience_mismatch",
    "delegation_not_allowed",
    "expired",
    "issuer_mismatch",
    "missing_assurance",
    "missing_auth_time",
    "missing_subject",
    "missing_verifier_evidence",
    "not_yet_valid",
    "provider_disabled",
    "provider_inactive",
    "provider_revoked",
    "provider_rotating",
    "realm_mismatch",
    "replay_or_stale_ticket",
    "service_principal_mismatch",
    "signature_invalid",
    "stale_claim",
    "stale_sync",
    "stale_verifier_evidence",
    "unknown_issuer",
    "untrusted_verifier",
})

SENSITIVE_CLAIM_KEYS = frozenset({
    "access_token",
    "assertion",
    "cookie",
    "id_token",
    "password",
    "private_key",
    "raw_assertion",
    "raw_token",
    "refresh_token",
    "saml_assertion",
    "session_cookie",
    "ticket",
})


@dataclass(frozen=True)
class IdentityProvider:
    provider_id: str
    provider_type: str
    issuer: str
    audiences: tuple[str, ...]
    subject_claim: str = "sub"
    group_claims: tuple[str, ...] = ("groups",)
    role_claims: tuple[str, ...] = ("roles",)
    entitlement_claims: tuple[str, ...] = ("entitlements",)
    assurance_claim: str | None = "acr"
    key_reference: str | None = None
    secret_references: tuple[str, ...] = ()
    claim_allowlist: tuple[str, ...] = ()
    required_assurance: tuple[str, ...] = ()
    mfa_required: bool = False
    group_claims_approved: bool = False
    metadata_fingerprint: str | None = None
    realm: str | None = None
    service_principal: str | None = None
    freshness_seconds: int | None = None
    status: str | None = None
    active: bool = True
    effective_from: str | None = None
    effective_until: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_nonempty(self.provider_id, "provider_id")
        if self.provider_type not in IDENTITY_PROVIDER_TYPES:
            raise NomosPolicyError(
                "provider_type must be one of "
                f"{sorted(IDENTITY_PROVIDER_TYPES)}; got {self.provider_type!r}"
            )
        _require_nonempty(self.issuer, "issuer")
        if not self.audiences or any(not item for item in self.audiences):
            raise NomosPolicyError("audiences must be a non-empty tuple")
        if self.status_value not in IDENTITY_PROVIDER_STATUSES:
            raise NomosPolicyError(
                "provider status must be one of "
                f"{sorted(IDENTITY_PROVIDER_STATUSES)}; got {self.status_value!r}"
            )
        if self.freshness_seconds is not None and self.freshness_seconds <= 0:
            raise NomosPolicyError("freshness_seconds must be positive when set")
        if self.provider_type == "kerberos_gssapi":
            _require_nonempty(self.realm or self.issuer, "realm")
            _require_nonempty(self.service_principal or "", "service_principal")
            if not self.key_reference and not self.secret_references:
                raise NomosPolicyError(
                    "kerberos_gssapi providers require a keytab secret reference"
                )
        _assert_no_raw_credential_material(self.metadata)
        for reference in (self.key_reference, *self.secret_references):
            if reference:
                _require_secret_reference(reference)

    @property
    def status_value(self) -> str:
        if self.status:
            return self.status
        return "active" if self.active else "disabled"

    @property
    def is_active(self) -> bool:
        return self.status_value == "active"

    def to_dict(self) -> dict[str, Any]:
        return {
            "provider_id": self.provider_id,
            "provider_type": self.provider_type,
            "issuer": self.issuer,
            "audiences": list(self.audiences),
            "subject_claim": self.subject_claim,
            "group_claims": list(self.group_claims),
            "role_claims": list(self.role_claims),
            "entitlement_claims": list(self.entitlement_claims),
            "assurance_claim": self.assurance_claim,
            "key_reference": self.key_reference,
            "secret_references": list(self.secret_references),
            "claim_allowlist": list(self.claim_allowlist),
            "required_assurance": list(self.required_assurance),
            "mfa_required": self.mfa_required,
            "group_claims_approved": self.group_claims_approved,
            "metadata_fingerprint": self.metadata_fingerprint,
            "realm": self.realm,
            "service_principal": self.service_principal,
            "freshness_seconds": self.freshness_seconds,
            "status": self.status_value,
            "active": self.is_active,
            "effective_from": self.effective_from,
            "effective_until": self.effective_until,
            "metadata": _json_safe(self.metadata),
        }


# Every claim path a subject binding may name, mapped to the part of the
# verified claim it reads. Paths that map to the same part are the same claim:
# a binding on ``sub`` matches exactly the users a binding on
# ``external_subject`` matches. ``IdentityClaimEnvelope.claim_values`` reads
# through this table, and binding disable compares claims through it, so the
# two cannot disagree about which rows match one user.
CLAIM_PATH_ALIASES: Mapping[str, str] = {
    "external_subject": "external_subject",
    "subject": "external_subject",
    "sub": "external_subject",
    "kerberos_username": "external_subject",
    "mit_kerberos_username": "external_subject",
    "groups": "groups",
    "group": "groups",
    "roles": "roles",
    "role": "roles",
    "entitlements": "entitlements",
    "entitlement": "entitlements",
    "assurance": "assurance",
    "acr": "assurance",
    "mfa": "mfa",
    "mfa_satisfied": "mfa",
    "realm": "realm",
    "kerberos_realm": "realm",
    "service_principal": "service_principal",
    "spn": "service_principal",
}


def canonical_claim_path(claim_path: str) -> str | None:
    """The claim part ``claim_path`` reads, or ``None`` for an unknown path."""
    return CLAIM_PATH_ALIASES.get(claim_path)


def claim_path_aliases(claim_path: str) -> tuple[str, ...]:
    """Every path that reads the same claim part as ``claim_path``, sorted.

    An unknown path is its own only alias: it matches nothing, so no other
    path names the same claim.
    """
    canonical = canonical_claim_path(claim_path)
    if canonical is None:
        return (claim_path,)
    return tuple(sorted(
        path for path, part in CLAIM_PATH_ALIASES.items() if part == canonical
    ))


@dataclass(frozen=True)
class IdentityClaimEnvelope:
    provider_id: str
    provider_type: str
    issuer: str
    external_subject: str
    audiences: tuple[str, ...]
    groups: tuple[str, ...] = ()
    roles: tuple[str, ...] = ()
    entitlements: tuple[str, ...] = ()
    assurance: str | None = None
    assurance_values: tuple[str, ...] = ()
    mfa_satisfied: bool = False
    realm: str | None = None
    service_principal: str | None = None
    delegation_chain: tuple[str, ...] = ()
    expires_at: str | None = None
    not_before: str | None = None
    auth_time: str | None = None
    verified_by: str | None = None
    verification_method: str | None = None
    verified_at: str | None = None
    claim_fingerprint: str = ""
    verification_status: str = UNVERIFIED_CLAIM_STATUS
    verification_error: str | None = None

    @property
    def is_valid(self) -> bool:
        return self.verification_status == VALID_CLAIM_STATUS

    def claim_values(self, claim_path: str) -> tuple[str, ...]:
        canonical = canonical_claim_path(claim_path)
        if canonical == "external_subject":
            return (self.external_subject,)
        if canonical == "groups":
            return self.groups
        if canonical == "roles":
            return self.roles
        if canonical == "entitlements":
            return self.entitlements
        if canonical == "assurance":
            return self.assurance_values or (
                (self.assurance,) if self.assurance else ()
            )
        if canonical == "mfa":
            return ("true",) if self.mfa_satisfied else ("false",)
        if canonical == "realm":
            return (self.realm,) if self.realm else ()
        if canonical == "service_principal":
            return (self.service_principal,) if self.service_principal else ()
        return ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "provider_id": self.provider_id,
            "provider_type": self.provider_type,
            "issuer": self.issuer,
            "external_subject": self.external_subject,
            "audiences": list(self.audiences),
            "groups": list(self.groups),
            "roles": list(self.roles),
            "entitlements": list(self.entitlements),
            "assurance": self.assurance,
            "assurance_values": list(self.assurance_values),
            "mfa_satisfied": self.mfa_satisfied,
            "realm": self.realm,
            "service_principal": self.service_principal,
            "delegation_chain": list(self.delegation_chain),
            "expires_at": self.expires_at,
            "not_before": self.not_before,
            "auth_time": self.auth_time,
            "verified_by": self.verified_by,
            "verification_method": self.verification_method,
            "verified_at": self.verified_at,
            "claim_fingerprint": self.claim_fingerprint,
            "verification_status": self.verification_status,
            "verification_error": self.verification_error,
        }


@dataclass(frozen=True)
class SubjectBinding:
    binding_id: str
    provider_id: str
    issuer: str
    claim_path: str
    claim_value: str
    subject: str
    scope: str
    priority: int = 0
    required_assurance: tuple[str, ...] = ()
    effective_from: str | None = None
    effective_until: str | None = None

    def matches(
        self,
        claim: IdentityClaimEnvelope,
        *,
        scope: str,
        now: str | None = None,
    ) -> bool:
        if self.provider_id != claim.provider_id:
            return False
        if self.issuer != claim.issuer:
            return False
        if self.scope != scope:
            return False
        if self.claim_value not in claim.claim_values(self.claim_path):
            return False
        if self.required_assurance and not set(self.required_assurance).issubset(
            set(claim.assurance_values),
        ):
            return False
        return _within_effective_range(
            now,
            effective_from=self.effective_from,
            effective_until=self.effective_until,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "binding_id": self.binding_id,
            "provider_id": self.provider_id,
            "issuer": self.issuer,
            "claim_path": self.claim_path,
            "claim_value": self.claim_value,
            "subject": self.subject,
            "scope": self.scope,
            "priority": self.priority,
            "required_assurance": list(self.required_assurance),
            "effective_from": self.effective_from,
            "effective_until": self.effective_until,
        }


@dataclass(frozen=True)
class PermissionGrant:
    grant_id: str
    subject: str
    actions: tuple[str, ...]
    scope: str
    effect: str = "allow"
    resource_match: Mapping[str, Any] = field(default_factory=dict)
    classifications: tuple[str, ...] = ("*",)
    priority: int = 0

    def matches(
        self,
        *,
        subject: str,
        action: str,
        resource: Mapping[str, Any],
        classification: str,
        scope: str,
    ) -> bool:
        if self.subject != subject:
            return False
        if "*" not in self.actions and action not in self.actions:
            return False
        if self.scope != scope:
            return False
        if "*" not in self.classifications and classification not in self.classifications:
            return False
        for key, expected in self.resource_match.items():
            if key == "source_bundle":
                if not _source_bundle_matches(resource, expected):
                    return False
                continue
            if not _resource_match_value_matches(
                _resource_match_actual(resource, key),
                expected,
            ):
                return False
        return True

    def to_dict(self) -> dict[str, Any]:
        return {
            "grant_id": self.grant_id,
            "subject": self.subject,
            "actions": list(self.actions),
            "scope": self.scope,
            "effect": self.effect,
            "resource_match": _json_safe(self.resource_match),
            "classifications": list(self.classifications),
            "priority": self.priority,
        }


@dataclass(frozen=True)
class IdentityAuthorizationResult:
    decision: PolicyDecision
    claim: IdentityClaimEnvelope
    resolved_subject: str | None
    binding_ids: tuple[str, ...]
    permission_grant_ids: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "decision": self.decision.to_dict(),
            "claim": self.claim.to_dict(),
            "resolved_subject": self.resolved_subject,
            "binding_ids": list(self.binding_ids),
            "permission_grant_ids": list(self.permission_grant_ids),
        }


def _resource_match_actual(resource: Mapping[str, Any], key: str) -> Any:
    if key in resource:
        return resource.get(key)
    if key == "source_id":
        return resource.get("source_ids")
    if key == "source_family":
        source_ids = _source_ids_from_resource(resource)
        if not source_ids:
            return resource.get("source_families")
        return tuple(_source_family(source_id) for source_id in source_ids)
    if key in {"source_bundle_id", "source_bundle_ids"}:
        return (
            resource.get("source_bundle_ids")
            or resource.get("source_bundles")
            or resource.get("source_bundle")
        )
    return None


def _resource_match_value_matches(actual: Any, expected: Any) -> bool:
    if expected == "*":
        return True
    if actual is None:
        return expected is None
    if isinstance(expected, Sequence) and not isinstance(expected, (str, bytes)):
        if isinstance(actual, Sequence) and not isinstance(actual, (str, bytes)):
            return set(str(item) for item in actual).issubset(
                {str(item) for item in expected}
            )
        return str(actual) in {str(item) for item in expected}
    if isinstance(actual, Sequence) and not isinstance(actual, (str, bytes)):
        return all(str(item) == str(expected) for item in actual)
    return actual == expected


def _source_bundle_matches(resource: Mapping[str, Any], expected: Any) -> bool:
    if expected == "*":
        return True
    source_ids = _source_ids_from_resource(resource)
    actual_bundle_ids = _str_tuple(
        resource.get("source_bundle_ids")
        or resource.get("source_bundles")
        or resource.get("source_bundle")
        or (),
    )
    if isinstance(expected, Mapping):
        expected_bundle_id = expected.get("bundle_id") or expected.get("id")
        expected_source_ids = expected.get("source_ids") or expected.get("sources")
        if expected_bundle_id and actual_bundle_ids:
            if str(expected_bundle_id) not in {str(item) for item in actual_bundle_ids}:
                return False
        if expected_source_ids is not None:
            return _source_ids_subset(source_ids, expected_source_ids)
        return bool(
            expected_bundle_id
            and str(expected_bundle_id) in {str(item) for item in actual_bundle_ids}
        )
    if isinstance(expected, Sequence) and not isinstance(expected, (str, bytes)):
        return _source_ids_subset(source_ids, expected)
    return bool(
        actual_bundle_ids
        and str(expected) in {str(item) for item in actual_bundle_ids}
    )


def _source_ids_from_resource(resource: Mapping[str, Any]) -> tuple[str, ...]:
    raw = resource.get("source_ids")
    if raw is None:
        raw = resource.get("source_id")
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)):
        return tuple(str(item) for item in raw if str(item))
    if raw is None:
        return ()
    text = str(raw).strip()
    return (text,) if text else ()


def _source_ids_subset(actual: Sequence[str], expected: Any) -> bool:
    if not actual:
        return False
    if isinstance(expected, Sequence) and not isinstance(expected, (str, bytes)):
        expected_set = {str(item) for item in expected}
    else:
        expected_set = {str(expected)}
    return set(str(item) for item in actual).issubset(expected_set)


def _source_family(source_id: str) -> str:
    text = str(source_id)
    for separator in (":", "/"):
        if separator in text:
            return text.split(separator, 1)[0]
    return text


def normalize_identity_claim(
    raw: Mapping[str, Any],
    providers: Sequence[IdentityProvider],
    *,
    now: str | None = None,
) -> IdentityClaimEnvelope:
    """Normalize gateway-verified or locally verified claim evidence.

    The input may contain raw bearer material so the claim fingerprint can
    be computed, but the returned envelope never contains those values.

    Absence never counts as evidence. A claim that omits
    ``verification_status`` normalizes to :data:`UNVERIFIED_CLAIM_STATUS`,
    not to ``valid``: the caller that actually verified the claim is the
    one that has to say so. When ``now`` is omitted the current UTC time
    is resolved and used, so the temporal checks below are decided rather
    than skipped.
    """
    now = now if now is not None else _utc_now_iso()
    _assert_no_raw_credential_material(
        {
            key: value
            for key, value in raw.items()
            if key not in SENSITIVE_CLAIM_KEYS
        },
    )
    provider_id = _required_str(raw, "provider_id")
    providers_by_id = {provider.provider_id: provider for provider in providers}
    provider = providers_by_id.get(provider_id)
    claims = raw.get("claims") or {}
    if not isinstance(claims, Mapping):
        raise NomosPolicyError("identity claim 'claims' must be an object")

    issuer = str(raw.get("issuer") or claims.get("iss") or raw.get("realm") or "")
    provider_type = str(raw.get("provider_type") or "unknown")
    audiences = _str_tuple(raw.get("audiences") or claims.get("aud") or [])
    external_subject = str(raw.get("external_subject") or claims.get("sub") or "")
    verification_status = str(
        raw.get("verification_status") or UNVERIFIED_CLAIM_STATUS
    )
    if verification_status not in IDENTITY_CLAIM_STATUSES:
        raise NomosPolicyError(
            "verification_status must be one of "
            f"{sorted(IDENTITY_CLAIM_STATUSES)}; got {verification_status!r}"
        )
    expires_at = _optional_str(raw.get("expires_at") or claims.get("exp"))
    not_before = _optional_str(raw.get("not_before") or claims.get("nbf"))
    auth_time = _optional_str(raw.get("auth_time") or claims.get("auth_time"))
    verified_by = _optional_plain_str(raw.get("verified_by") or claims.get("verified_by"))
    verification_method = _optional_plain_str(
        raw.get("verification_method") or claims.get("verification_method")
    )
    verified_at = _optional_str(raw.get("verified_at") or claims.get("verified_at"))
    realm = _optional_plain_str(raw.get("realm") or claims.get("realm"))
    service_principal = _optional_plain_str(
        raw.get("service_principal") or claims.get("service_principal")
    )
    delegation_chain = _str_tuple(
        raw.get("delegation_chain") or claims.get("delegation_chain") or []
    )
    # Output only. This is Nomos's own finding about the claim, so it is
    # not read from the claim: a caller-supplied value would land in the
    # audit row and the graph unvalidated, and could suppress the
    # "nobody vouched for this" marker set below.
    verification_error: str | None = None

    if provider is None:
        verification_status = "unknown_issuer"
    else:
        provider_type = provider.provider_type
        if provider.subject_claim:
            external_subject = _claim_value(claims, provider.subject_claim) or (
                external_subject
            )
        if provider.status_value == "revoked":
            verification_status = "provider_revoked"
        elif provider.status_value == "disabled":
            verification_status = "provider_disabled"
        elif provider.status_value == "rotating":
            verification_status = "provider_rotating"
        elif not provider.is_active:
            verification_status = "provider_inactive"
        elif issuer != provider.issuer:
            verification_status = "issuer_mismatch"
        elif not set(audiences).intersection(provider.audiences):
            verification_status = "audience_mismatch"
        elif not external_subject:
            verification_status = "missing_subject"
        elif _is_after(not_before, now):
            verification_status = "not_yet_valid"
        elif _is_at_or_before(expires_at, now):
            verification_status = "expired"
        elif provider.freshness_seconds is not None and not auth_time:
            # The provider declared a freshness requirement, so a claim
            # that carries no authentication time cannot demonstrate it.
            verification_status = "missing_auth_time"
        elif _is_stale_auth_time(auth_time, now, provider.freshness_seconds):
            verification_status = "stale_claim"
        elif _requires_trusted_verifier(provider):
            trusted_verifiers = _str_tuple(
                provider.metadata.get("trusted_verifiers") or ()
            )
            verifier_max_age = _metadata_int(
                provider.metadata.get("max_verifier_age_seconds"),
            )
            if verifier_max_age is None:
                verifier_max_age = provider.freshness_seconds
            if not verified_by:
                verification_status = "missing_verifier_evidence"
            elif trusted_verifiers and verified_by not in trusted_verifiers:
                verification_status = "untrusted_verifier"
            elif verifier_max_age is not None and not verified_at:
                verification_status = "missing_verifier_evidence"
            elif _is_stale_auth_time(verified_at, now, verifier_max_age):
                verification_status = "stale_verifier_evidence"

    groups = _claim_values(claims, provider.group_claims if provider else ("groups",))
    roles = _claim_values(claims, provider.role_claims if provider else ("roles",))
    entitlements = _claim_values(
        claims,
        provider.entitlement_claims if provider else ("entitlements",),
    )
    if provider and groups and not provider.group_claims_approved:
        groups = ()
        verification_error = verification_error or "group_claims_unapproved"
    elif provider and groups and provider.claim_allowlist:
        allowed_claim_values = set(provider.claim_allowlist)
        groups = tuple(value for value in groups if value in allowed_claim_values)
    assurance_values = _assurance_values(
        claims,
        provider.assurance_claim if provider and provider.assurance_claim else "acr",
    )
    assurance = assurance_values[0] if assurance_values else None
    mfa_satisfied = _mfa_satisfied(raw, claims, assurance_values)
    if provider and provider.provider_type == "kerberos_gssapi":
        realm = realm or provider.realm or provider.issuer
        service_principal = service_principal or provider.service_principal
        if realm != (provider.realm or provider.issuer):
            verification_status = "realm_mismatch"
        elif service_principal != provider.service_principal:
            verification_status = "service_principal_mismatch"
    if provider and verification_status == VALID_CLAIM_STATUS:
        if provider.mfa_required and not mfa_satisfied:
            verification_status = "missing_assurance"
        elif provider.required_assurance and not set(
            provider.required_assurance,
        ).issubset(set(assurance_values)):
            verification_status = "missing_assurance"

    if not raw.get("verification_status") and (
        verification_status != UNVERIFIED_CLAIM_STATUS
    ):
        # The cascade found a more specific cause and overwrote the
        # unverified status. Keep the fact that nobody asserted
        # verification: otherwise a bare claim with a stale token reads as
        # "re-issue the token" when the real problem is that no component
        # ever vouched for it.
        verification_error = verification_error or UNVERIFIED_CLAIM_STATUS

    fingerprint = str(raw.get("claim_fingerprint") or "")
    if not fingerprint:
        fingerprint_source = _first_sensitive_value(raw)
        if fingerprint_source is None:
            fingerprint_source = {
                "provider_id": provider_id,
                "issuer": issuer,
                "external_subject": external_subject,
                "audiences": list(audiences),
                "groups": list(groups),
                "roles": list(roles),
                "entitlements": list(entitlements),
                "assurance": assurance,
                "assurance_values": list(assurance_values),
                "mfa_satisfied": mfa_satisfied,
                "realm": realm,
                "service_principal": service_principal,
                "delegation_chain": list(delegation_chain),
                "expires_at": expires_at,
                "not_before": not_before,
                "auth_time": auth_time,
                "verified_by": verified_by,
                "verification_method": verification_method,
                "verified_at": verified_at,
            }
        fingerprint = "sha256:" + _sha256_hex(fingerprint_source)

    return IdentityClaimEnvelope(
        provider_id=provider_id,
        provider_type=provider_type,
        issuer=issuer,
        external_subject=external_subject or "unknown",
        audiences=audiences,
        groups=groups,
        roles=roles,
        entitlements=entitlements,
        assurance=assurance,
        assurance_values=assurance_values,
        mfa_satisfied=mfa_satisfied,
        realm=realm,
        service_principal=service_principal,
        delegation_chain=delegation_chain,
        expires_at=expires_at,
        not_before=not_before,
        auth_time=auth_time,
        verified_by=verified_by,
        verification_method=verification_method,
        verified_at=verified_at,
        claim_fingerprint=fingerprint,
        verification_status=verification_status,
        verification_error=verification_error,
    )


def evaluate_identity_claim(
    bundle: CompiledPolicyBundle | None,
    claim: IdentityClaimEnvelope,
    *,
    bindings: Sequence[SubjectBinding],
    permission_grants: Sequence[PermissionGrant],
    action: str,
    resource: Mapping[str, Any],
    scope: str,
    classification: str = "internal",
    now: str | None = None,
    enforcement_point: str = "nomos.identity",
    extra_context: Mapping[str, Any] | None = None,
) -> IdentityAuthorizationResult:
    """Resolve an identity claim and evaluate the resulting Nomos action.

    When ``now`` is omitted the current UTC time is resolved and used, so
    a binding whose effective window has closed is not selected just
    because no caller supplied a clock.
    """
    now = now if now is not None else _utc_now_iso()
    if not claim.is_valid:
        decision = _deny_decision(
            bundle,
            extra_context=extra_context,
            claim=claim,
            action=action,
            resource=resource,
            scope=scope,
            classification=classification,
            reason_code=_reason_for_claim_status(claim.verification_status),
            enforcement_point=enforcement_point,
        )
        return IdentityAuthorizationResult(decision, claim, None, (), ())

    matching_bindings = sorted(
        (
            binding
            for binding in bindings
            if binding.matches(claim, scope=scope, now=now)
        ),
        key=lambda binding: (-binding.priority, binding.binding_id),
    )
    blocks = tuple(
        binding.binding_id
        for binding in matching_bindings
        if binding.subject == BLOCKED_SUBJECT
    )
    if blocks:
        # A block outranks every other binding whatever its priority: it is
        # checked before priority ordering, so no role or group binding --
        # however high its priority -- can re-admit a blocked user.
        decision = _deny_decision(
            bundle,
            extra_context=extra_context,
            claim=claim,
            action=action,
            resource=resource,
            scope=scope,
            classification=classification,
            reason_code=BLOCKED_REASON_CODE,
            enforcement_point=enforcement_point,
            binding_ids=blocks,
        )
        return IdentityAuthorizationResult(decision, claim, None, blocks, ())
    if not matching_bindings:
        reason_code = "nomos.identity.no_subject_binding"
        if claim.verification_error == "group_claims_unapproved":
            reason_code = "nomos.identity.group_claims_unapproved"
        decision = _deny_decision(
            bundle,
            extra_context=extra_context,
            claim=claim,
            action=action,
            resource=resource,
            scope=scope,
            classification=classification,
            reason_code=reason_code,
            enforcement_point=enforcement_point,
        )
        return IdentityAuthorizationResult(decision, claim, None, (), ())

    top_priority = matching_bindings[0].priority
    top_bindings = [
        binding for binding in matching_bindings if binding.priority == top_priority
    ]
    top_subjects = {binding.subject for binding in top_bindings}
    if len(top_subjects) != 1:
        decision = _deny_decision(
            bundle,
            extra_context=extra_context,
            claim=claim,
            action=action,
            resource=resource,
            scope=scope,
            classification=classification,
            reason_code="nomos.identity.ambiguous_subject_binding",
            enforcement_point=enforcement_point,
        )
        return IdentityAuthorizationResult(
            decision,
            claim,
            None,
            tuple(binding.binding_id for binding in top_bindings),
            (),
        )

    subject = next(iter(top_subjects))
    matching_grants = sorted(
        (
            grant
            for grant in permission_grants
            if grant.matches(
                subject=subject,
                action=action,
                resource=resource,
                classification=classification,
                scope=scope,
            )
        ),
        key=lambda grant: (-grant.priority, grant.grant_id),
    )
    if not matching_grants:
        decision = _deny_decision(
            bundle,
            extra_context=extra_context,
            claim=claim,
            action=action,
            resource=resource,
            scope=scope,
            classification=classification,
            reason_code="nomos.identity.no_permission_grant",
            enforcement_point=enforcement_point,
            resolved_subject=subject,
            binding_ids=tuple(binding.binding_id for binding in top_bindings),
        )
        return IdentityAuthorizationResult(
            decision,
            claim,
            subject,
            tuple(binding.binding_id for binding in top_bindings),
            (),
        )

    top_grant = matching_grants[0]
    if top_grant.effect != "allow":
        decision = _deny_decision(
            bundle,
            extra_context=extra_context,
            claim=claim,
            action=action,
            resource=resource,
            scope=scope,
            classification=classification,
            reason_code=f"nomos.identity.permission_grant_{top_grant.effect}",
            enforcement_point=enforcement_point,
            resolved_subject=subject,
            binding_ids=tuple(binding.binding_id for binding in top_bindings),
            permission_grant_ids=(top_grant.grant_id,),
        )
        return IdentityAuthorizationResult(
            decision,
            claim,
            subject,
            tuple(binding.binding_id for binding in top_bindings),
            (top_grant.grant_id,),
        )

    decision_input = DecisionInput(
        subject=subject,
        action=action,
        classification=classification,
        resource=dict(resource),
        context=_identity_context(
            claim,
            extra_context=extra_context,
            scope=scope,
            binding_ids=tuple(binding.binding_id for binding in top_bindings),
            permission_grant_ids=(top_grant.grant_id,),
        ),
    )
    decision = evaluate(
        bundle,
        decision_input,
        enforcement_point=enforcement_point,
    )
    return IdentityAuthorizationResult(
        decision,
        claim,
        subject,
        tuple(binding.binding_id for binding in top_bindings),
        (top_grant.grant_id,),
    )


def institutional_provider_fixture(
    profile: str,
    *,
    audience: str,
    provider_id: str | None = None,
    issuer: str | None = None,
    key_reference: str | None = None,
    metadata_fingerprint: str | None = None,
    service_principal: str | None = None,
) -> IdentityProvider:
    """Return a non-production provider template for CERN/MIT integration tests.

    The fixture captures the expected shape of a provider profile. Production
    deployments still need institution-owned registration values and metadata
    fingerprints.
    """
    if profile == "cern-oidc":
        return IdentityProvider(
            provider_id=provider_id or "cern-sso-oidc",
            provider_type="oidc",
            issuer=issuer or "https://auth.cern.ch/auth/realms/cern",
            audiences=(audience,),
            role_claims=("cern_roles", "roles"),
            group_claims=("groups",),
            assurance_claim="acr",
            key_reference=key_reference or "secret://nomos/fixtures/cern-oidc-jwks",
            required_assurance=("mfa",),
            mfa_required=True,
            group_claims_approved=False,
            metadata_fingerprint=metadata_fingerprint,
            freshness_seconds=3600,
            metadata={"fixture_profile": profile},
        )
    if profile == "cern-saml":
        return IdentityProvider(
            provider_id=provider_id or "cern-sso-saml",
            provider_type="saml",
            issuer=issuer or "https://auth.cern.ch/auth/realms/cern",
            audiences=(audience,),
            subject_claim="NameID",
            role_claims=("CernRoles",),
            group_claims=("groups",),
            assurance_claim="AuthnContextClassRef",
            key_reference=key_reference or "secret://nomos/fixtures/cern-saml-cert",
            required_assurance=("mfa",),
            mfa_required=True,
            group_claims_approved=False,
            metadata_fingerprint=metadata_fingerprint,
            freshness_seconds=3600,
            metadata={"fixture_profile": profile},
        )
    if profile == "mit-touchstone":
        return IdentityProvider(
            provider_id=provider_id or "mit-touchstone",
            provider_type="touchstone_oidc",
            issuer=issuer or "https://touchstone.mit.edu/",
            audiences=(audience,),
            subject_claim="kerberos_username",
            role_claims=("roles", "affiliations"),
            group_claims=("groups",),
            assurance_claim="acr",
            key_reference=key_reference or "secret://nomos/fixtures/mit-touchstone-jwks",
            required_assurance=("duo_mfa",),
            mfa_required=True,
            group_claims_approved=False,
            metadata_fingerprint=metadata_fingerprint,
            freshness_seconds=3600,
            metadata={"fixture_profile": profile},
        )
    if profile == "mit-kerberos-gssapi":
        spn = service_principal or "HTTP/okg-gateway.example.org@ATHENA.MIT.EDU"
        return IdentityProvider(
            provider_id=provider_id or "mit-kerberos-gssapi",
            provider_type="kerberos_gssapi",
            issuer=issuer or "ATHENA.MIT.EDU",
            audiences=(audience or spn,),
            subject_claim="kerberos_username",
            role_claims=(),
            group_claims=(),
            assurance_claim="assurance",
            key_reference=key_reference or "secret://nomos/fixtures/mit-http-keytab",
            secret_references=(key_reference or "secret://nomos/fixtures/mit-http-keytab",),
            required_assurance=("kerberos_gssapi",),
            mfa_required=False,
            realm="ATHENA.MIT.EDU",
            service_principal=spn,
            metadata_fingerprint=metadata_fingerprint,
            freshness_seconds=300,
            metadata={"fixture_profile": profile},
        )
    raise NomosPolicyError(
        "institutional provider fixture profile must be one of "
        "'cern-oidc', 'cern-saml', 'mit-touchstone', or "
        "'mit-kerberos-gssapi'"
    )


def _deny_decision(
    bundle: CompiledPolicyBundle | None,
    *,
    claim: IdentityClaimEnvelope,
    action: str,
    resource: Mapping[str, Any],
    scope: str,
    classification: str,
    reason_code: str,
    enforcement_point: str,
    resolved_subject: str | None = None,
    binding_ids: tuple[str, ...] = (),
    permission_grant_ids: tuple[str, ...] = (),
    extra_context: Mapping[str, Any] | None = None,
) -> PolicyDecision:
    decision_input = DecisionInput(
        subject=resolved_subject or f"external:{claim.provider_id}:{claim.external_subject}",
        action=action,
        classification=classification,
        resource=dict(resource),
        context=_identity_context(
            claim,
            scope=scope,
            binding_ids=binding_ids,
            permission_grant_ids=permission_grant_ids,
            extra_context=extra_context,
        ),
    )
    input_hash = "sha256:" + _sha256_hex(decision_input.to_dict())
    seed = {
        "policy_fingerprint": bundle.policy_fingerprint if bundle else None,
        "input_hash": input_hash,
        "effect": "deny",
        "reason_code": reason_code,
    }
    return PolicyDecision(
        decision_id="nomos:decision:" + _sha256_hex(seed)[:24],
        policy_bundle_id=bundle.policy_bundle_id if bundle else None,
        policy_version=bundle.policy_version if bundle else None,
        policy_fingerprint=bundle.policy_fingerprint if bundle else None,
        input_hash=input_hash,
        subject=decision_input.subject,
        action=action,
        resource=dict(resource),
        classification=classification,
        context=decision_input.context,
        effect="deny",
        reason_code=reason_code,
        matched_rule_ids=(),
        obligations=(),
        enforcement_point=enforcement_point,
    )


def _identity_context(
    claim: IdentityClaimEnvelope,
    *,
    scope: str,
    binding_ids: tuple[str, ...],
    permission_grant_ids: tuple[str, ...],
    extra_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        **dict(extra_context or {}),
        "identity_provider_id": claim.provider_id,
        "identity_provider_type": claim.provider_type,
        "identity_issuer": claim.issuer,
        "identity_audiences": list(claim.audiences),
        "identity_external_subject": claim.external_subject,
        "identity_claim_fingerprint": claim.claim_fingerprint,
        "identity_verification_status": claim.verification_status,
        "identity_verification_error": claim.verification_error,
        "identity_assurance": claim.assurance,
        "identity_assurance_values": list(claim.assurance_values),
        "identity_mfa_satisfied": claim.mfa_satisfied,
        "identity_realm": claim.realm,
        "identity_service_principal": claim.service_principal,
        "identity_verified_by": claim.verified_by,
        "identity_verification_method": claim.verification_method,
        "identity_verified_at": claim.verified_at,
        "scope": scope,
        "subject_binding_ids": list(binding_ids),
        "permission_grant_ids": list(permission_grant_ids),
    }


def _reason_for_claim_status(status: str) -> str:
    return {
        "audience_mismatch": "nomos.identity.audience_mismatch",
        "expired": "nomos.identity.expired_claim",
        "issuer_mismatch": "nomos.identity.issuer_mismatch",
        "missing_subject": "nomos.identity.missing_subject",
        "missing_assurance": "nomos.identity.missing_assurance",
        "missing_auth_time": "nomos.identity.missing_auth_time",
        "missing_verifier_evidence": "nomos.identity.missing_verifier_evidence",
        UNVERIFIED_CLAIM_STATUS: "nomos.identity.unverified",
        "not_yet_valid": "nomos.identity.not_yet_valid",
        "provider_disabled": "nomos.identity.provider_disabled",
        "provider_inactive": "nomos.identity.provider_inactive",
        "provider_revoked": "nomos.identity.provider_revoked",
        "provider_rotating": "nomos.identity.provider_rotating",
        "realm_mismatch": "nomos.identity.realm_mismatch",
        "service_principal_mismatch": "nomos.identity.service_principal_mismatch",
        "signature_invalid": "nomos.identity.signature_invalid",
        "stale_claim": "nomos.identity.stale_claim",
        "stale_sync": "nomos.identity.stale_sync",
        "stale_verifier_evidence": "nomos.identity.stale_verifier_evidence",
        "untrusted_verifier": "nomos.identity.untrusted_verifier",
        "unknown_issuer": "nomos.identity.unknown_issuer",
    }.get(status, f"nomos.identity.{status}")


def _claim_value(claims: Mapping[str, Any], path: str) -> str | None:
    values = _claim_values(claims, (path,))
    return values[0] if values else None


def _assurance_values(
    claims: Mapping[str, Any],
    assurance_claim: str,
) -> tuple[str, ...]:
    values = set(_claim_values(claims, (assurance_claim,)))
    values.update(_claim_values(claims, ("amr", "assurance", "assurance_values")))
    if any(value.lower() in {"mfa", "duo", "duo_mfa", "otp", "webauthn"} for value in values):
        values.add("mfa")
    return tuple(sorted(values))


def _mfa_satisfied(
    raw: Mapping[str, Any],
    claims: Mapping[str, Any],
    assurance_values: Sequence[str],
) -> bool:
    explicit = raw.get("mfa_satisfied", claims.get("mfa_satisfied"))
    if isinstance(explicit, bool):
        return explicit
    if isinstance(explicit, str):
        return explicit.strip().lower() in {"1", "true", "yes", "mfa", "duo"}
    lowered = {value.lower() for value in assurance_values}
    return bool(lowered.intersection({"mfa", "duo", "duo_mfa", "otp", "webauthn"}))


def _requires_trusted_verifier(provider: IdentityProvider) -> bool:
    trusted_verifiers = _str_tuple(provider.metadata.get("trusted_verifiers") or ())
    return bool(
        trusted_verifiers
        or _metadata_bool(provider.metadata.get("require_verified_by"))
        or _metadata_bool(provider.metadata.get("production"))
    )


def _metadata_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _metadata_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise NomosPolicyError("metadata integer cannot be a boolean")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise NomosPolicyError("metadata integer must be numeric") from exc
    if parsed <= 0:
        raise NomosPolicyError("metadata integer must be positive")
    return parsed


def _claim_values(claims: Mapping[str, Any], paths: Sequence[str]) -> tuple[str, ...]:
    values: list[str] = []
    for path in paths:
        current: Any = claims
        for part in path.split("."):
            if not isinstance(current, Mapping):
                current = None
                break
            current = current.get(part)
        if current is None:
            continue
        if isinstance(current, Sequence) and not isinstance(current, str):
            values.extend(str(item) for item in current if str(item))
        elif str(current):
            values.append(str(current))
    return tuple(sorted(set(values)))


def _is_stale_auth_time(
    auth_time: str | None,
    now: str | None,
    freshness_seconds: int | None,
) -> bool:
    if freshness_seconds is None:
        # No freshness requirement was declared, so there is nothing an
        # absent timestamp could fail.
        return False
    if not auth_time:
        # A requirement was declared and the evidence needed to evaluate
        # it is missing: that is stale, not fresh.
        return True
    if not now:
        now = _utc_now_iso()
    observed = _parse_time(auth_time)
    current = _parse_time(now)
    return (current - observed).total_seconds() > freshness_seconds


def _first_sensitive_value(raw: Mapping[str, Any]) -> Any | None:
    for key in sorted(SENSITIVE_CLAIM_KEYS):
        if key in raw and raw[key]:
            return raw[key]
    return None


def _within_effective_range(
    now: str | None,
    *,
    effective_from: str | None,
    effective_until: str | None,
) -> bool:
    if not effective_from and not effective_until:
        return True
    if now is None:
        # A window was declared but no clock was supplied, so the window
        # cannot be shown to be open. Refuse rather than assume.
        return False
    if effective_from and _is_after(effective_from, now):
        return False
    if effective_until and _is_at_or_before(effective_until, now):
        return False
    return True


def _is_after(value: str | None, now: str | None) -> bool:
    if value is None or now is None:
        return False
    return _parse_time(value) > _parse_time(now)


def _is_at_or_before(value: str | None, now: str | None) -> bool:
    if value is None or now is None:
        return False
    return _parse_time(value) <= _parse_time(now)


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


def _parse_time(value: str) -> datetime:
    try:
        normalized = value.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise NomosPolicyError(f"invalid identity timestamp: {value!r}") from exc
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, int | float):
        return datetime.fromtimestamp(float(value), tz=UTC).isoformat()
    if not isinstance(value, str) or not value.strip():
        raise NomosPolicyError("identity timestamp must be a string or epoch number")
    return value


def _optional_plain_str(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise NomosPolicyError("identity field must be a non-empty string")
    return value


def _required_str(mapping: Mapping[str, Any], key: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value.strip():
        raise NomosPolicyError(f"{key} is required and must be a non-empty string")
    return value


def _require_nonempty(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise NomosPolicyError(f"{field_name} must be a non-empty string")


def _require_secret_reference(value: str) -> None:
    if not value.startswith(("secret://", "vault://", "keyring://", "aws-sm://")):
        raise NomosPolicyError(
            "credential-bearing provider fields must use a secret reference"
        )


def _assert_no_raw_credential_material(value: Any) -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            lowered = str(key).lower()
            if lowered in SENSITIVE_CLAIM_KEYS:
                raise NomosPolicyError(
                    f"raw credential material is not allowed in field {key!r}"
                )
            _assert_no_raw_credential_material(child)
    elif isinstance(value, tuple | list):
        for child in value:
            _assert_no_raw_credential_material(child)


def _str_tuple(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        return (value,)
    if not isinstance(value, Sequence):
        return ()
    return tuple(str(item) for item in value if str(item))


def _sha256_hex(value: Any) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


def _canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        _json_safe(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")


def _json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_safe(child) for key, child in value.items()}
    if isinstance(value, tuple | list):
        return [_json_safe(child) for child in value]
    return value
