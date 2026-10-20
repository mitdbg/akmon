"""Per-request principal mapping for the MCP surface.

PACT ``mcp-principal-mapping``. Before this module the MCP server bound ONE
identity for its whole process (``_resolve_mcp_read_subject``), so every
ordinary caller it served was the same person as far as Nomos was concerned.
The only request-scoped identity channel that existed --
``authority.nomos_identity_claim`` -- is a self-assertion any client of the
port can make.

**Trust model.** This is a DELEGATION, and the delegation is stated rather
than implied. The transport bearer token (PACT ``mcp-http-auth``)
authenticates the fronting APP, not the person; the substrate then trusts
that app's own SSO to name its signed-in user in a header the deployment
DECLARED. Blast radius: compromise of the app token permits impersonation of
every user of that app. That is why

* multi-user mode refuses to start on a transport whose authentication is
  inactive (:func:`multi_user_requires_auth_error`),
* the server -- never the client -- stamps ``verified_by`` /``verified_at``
  from the authenticated transport identity, so the persisted claim records
  "app-token holder X asserted principal P", not a bare "P", and
* the identity provider the mapping names must carry verifier metadata, so
  the claim is checked against a declared trusted verifier instead of being
  accepted because nothing objected.

**Nothing is sniffed.** Header names come from the deployment manifest. A
header the mapping does not declare is not identity and is never read. The
header VALUE is caller-supplied request data: a claim to be verified, never
authorization by itself.

**Nothing degrades.** In multi-user mode a missing, empty, malformed,
ambiguous, or unmappable principal is REFUSED with reason code
``nomos.identity.unidentified_principal`` and persisted under the sentinel
subject ``subject:unidentified`` -- never downgraded to the environment
subject, the deployment baseline ``subject:mcp_client``, or any other
default. The sentinel exists because ``okg.nomos_decisions.subject`` is NOT
NULL and the effect vocabulary has no non-deny refusal value, so "nobody was
identified" has to be *written down* as something; writing it as a distinct
subject + a distinct reason code is what keeps it from reading as an
ordinary policy deny.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Mapping, Sequence

from okg.substrate.errors import SubstrateError

__all__ = [
    "BLOCKED_PRINCIPAL_REASON",
    "CONFLICTING_IDENTITY_CHANNEL_REASON",
    "MCP_PRINCIPAL_HEADER_PROVIDER_TYPE",
    "PRINCIPAL_HEADER_VERIFICATION_METHOD",
    "PrincipalMapping",
    "UNIDENTIFIED_PRINCIPAL_REASON",
    "UNIDENTIFIED_SUBJECT",
    "blocked_principal_error",
    "conflicting_identity_channel_error",
    "current_request_headers",
    "current_transport_identity",
    "header_claim_for_request",
    "multi_user_requires_auth_error",
    "parse_principal_mapping",
    "persist_principal_refusal_decision",
    "principal_mapping_from_manifest",
    "refusal_reason_code",
    "require_explicit_verification_status",
    "unidentified_principal_error",
    "validate_mapping_against_providers",
]


#: The refusal reason for "nobody was identified". Deliberately NOT
#: ``nomos.identity.no_subject_binding``: that code is emitted with effect
#: ``deny`` for a claim the policy rejected, so reusing it would make an
#: unidentified caller indistinguishable from a denied one in the audit
#: record.
UNIDENTIFIED_PRINCIPAL_REASON = "nomos.identity.unidentified_principal"

#: The refusal reason for a caller that tried to supply its own identity
#: through ``authority.nomos_identity_claim`` while the declared header
#: mapping is the sole identity input.
CONFLICTING_IDENTITY_CHANNEL_REASON = "nomos.identity.conflicting_identity_channel"

#: The refusal reason for a user an operator blocked (`okg nomos binding
#: block`). The same code Nomos writes on the identity decision row, so the
#: gate's refusal row and the evaluation row agree.
BLOCKED_PRINCIPAL_REASON = "nomos.identity.principal_blocked"

#: How the request's identity arrived, recorded on each refusal row.
HEADER_IDENTITY_CHANNEL = "declared_principal_header"
OIDC_IDENTITY_CHANNEL = "oidc_bearer_token"

#: Sentinel written to ``okg.nomos_decisions.subject`` for a refusal. It is a
#: subject that can never be granted anything: no binding produces it (
#: bindings resolve real subjects) and no deployment declares it.
UNIDENTIFIED_SUBJECT = "subject:unidentified"

#: The provider type a header mapping registers under. ``IDENTITY_PROVIDER_TYPES``
#: carried no header/proxy type before this PACT; rather than smuggling the
#: mapping in under ``oidc`` (which would claim token verification that never
#: happened) or ``deployment_extension`` (which says nothing), the type is
#: named for what it is: a principal asserted in a request header by an
#: authenticated fronting app.
MCP_PRINCIPAL_HEADER_PROVIDER_TYPE = "mcp_principal_header"

#: Recorded on every header-derived claim so the audit trail says HOW the
#: claim was checked, not merely that it was.
PRINCIPAL_HEADER_VERIFICATION_METHOD = "mcp_transport_delegated_header"

#: Prefix for the ``verified_by`` stamp. The suffix is the authenticated
#: transport client id, so a provider's ``trusted_verifiers`` list names the
#: app whose token may assert principals.
TRANSPORT_VERIFIER_PREFIX = "mcp-transport:"

#: A principal header value is a single printable-ASCII token with no spaces.
#: Deliberately strict: this is an identifier the deployment binds policy to,
#: not free text. The consequence is stated rather than discovered: a
#: deployment whose principals are internationalized identifiers (non-ASCII
#: local parts, IDN domains) cannot use this mapping as written -- such a
#: value is refused as malformed, not silently transliterated into a
#: different principal.
_PRINCIPAL_VALUE_RE = re.compile(r"^[!-~]+$")

#: Long enough for an email address (RFC 5321 caps the whole address at 320).
_PRINCIPAL_VALUE_MAX = 320

#: Claim names an ``extra_claim_headers`` entry may NOT declare.
#:
#: Two distinct hazards, both of which end with the caller deciding something
#: the caller must not decide.
#:
#: *It can BIND a subject.* ``SubjectBinding.matches`` resolves its
#: ``claim_path`` through ``IdentityClaimEnvelope.claim_values``
#: (identity.py:187-208), which answers for ``groups``/``group``,
#: ``roles``/``role`` and ``entitlements``/``entitlement`` -- each populated
#: straight from the caller-supplied claims dict. A binding on
#: ``(claim_path="roles", claim_value="admin")`` would let anyone who can set
#: a declared roles header become ``subject:admin``. ``groups`` at least
#: passes the ``group_claims_approved`` gate and the ``claim_allowlist``
#: filter; ``roles`` and ``entitlements`` pass through unchecked. This is the
#: strictly worse channel: assurance only GATES, roles BIND.
#:
#: *It can satisfy a trust check.* ``amr``/``acr``/``assurance`` feed
#: ``_assurance_values``, ``mfa_satisfied`` feeds ``_mfa_satisfied``, and
#: ``exp``/``nbf``/``auth_time`` drive the freshness gates -- deciding
#: ``provider.mfa_required``, ``provider.required_assurance`` and
#: ``SubjectBinding.required_assurance``.
#:
#: A static list cannot be the whole answer, because ``group_claims`` /
#: ``role_claims`` / ``entitlement_claims`` / ``assurance_claim`` are
#: PER-PROVIDER (a shipped fixture uses ``cern_roles``). The provider's own
#: names are refused too, at load, in
#: :func:`validate_mapping_against_providers`.
_RESERVED_CLAIM_NAMES = frozenset({
    "acr",
    "amr",
    "assurance",
    "assurance_values",
    "aud",
    "audiences",
    "auth_time",
    "delegation_chain",
    "entitlement",
    "entitlements",
    "exp",
    "expires_at",
    "group",
    "groups",
    "iss",
    "issuer",
    "kerberos_realm",
    "kerberos_username",
    "mfa",
    "mfa_satisfied",
    "nbf",
    "not_before",
    "provider_id",
    "realm",
    "role",
    "roles",
    "service_principal",
    "spn",
    "sub",
    "subject",
    "verification_method",
    "verification_status",
    "verified_at",
    "verified_by",
})

_MAPPING_KEYS = frozenset({
    "provider_id",
    "issuer",
    "audience",
    "subject_claim",
    "subject_header",
    "principal_kind",
    "extra_claim_headers",
})

_REQUIRED_MAPPING_KEYS = (
    "provider_id",
    "issuer",
    "audience",
    "subject_claim",
    "subject_header",
    "principal_kind",
)

# An HTTP field name, RFC 9110 token production. Checked so a manifest cannot
# declare a "header" that no client could ever send.
_HEADER_NAME_RE = re.compile(r"^[A-Za-z0-9!#$%&'*+._`|~^-]+$")


@dataclass(frozen=True)
class PrincipalMapping:
    """One deployment's declared header-to-claim mapping.

    Every field is operator configuration read from the manifest. Nothing
    here is derived from a request: the mapping decides WHICH header carries
    WHICH claim before any caller connects, which is what makes an
    undeclared header non-identity rather than a candidate.
    """

    provider_id: str
    issuer: str
    audience: str
    subject_claim: str
    subject_header: str
    principal_kind: str
    extra_claim_headers: Mapping[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "provider_id": self.provider_id,
            "issuer": self.issuer,
            "audience": self.audience,
            "subject_claim": self.subject_claim,
            "subject_header": self.subject_header,
            "principal_kind": self.principal_kind,
            "extra_claim_headers": dict(self.extra_claim_headers),
        }


def parse_principal_mapping(raw: Any) -> PrincipalMapping:
    """Parse and strictly validate one ``nomos.principal_mapping`` block.

    Strict at every key: an unknown key is a refusal, not an ignored typo. A
    mapping whose ``subject_header`` silently defaulted because the operator
    misspelled the key would be a mapping nobody declared.
    """
    if not isinstance(raw, Mapping):
        raise SubstrateError(
            "nomos.principal_mapping must be a mapping",
            data={"got": type(raw).__name__},
            fix_hint=(
                "declare provider_id, issuer, audience, subject_claim, "
                "subject_header and principal_kind under "
                "nomos.principal_mapping"
            ),
        )
    unknown = sorted(set(raw) - _MAPPING_KEYS)
    if unknown:
        raise SubstrateError(
            "nomos.principal_mapping contains unknown key(s): "
            + ", ".join(repr(key) for key in unknown),
            data={"unknown_keys": unknown, "known_keys": sorted(_MAPPING_KEYS)},
            fix_hint="remove the key or correct the spelling",
        )
    missing = [key for key in _REQUIRED_MAPPING_KEYS if not str(raw.get(key) or "").strip()]
    if missing:
        raise SubstrateError(
            "nomos.principal_mapping is missing required key(s): "
            + ", ".join(repr(key) for key in missing),
            data={"missing_keys": missing},
            fix_hint=(
                "a principal mapping must name the provider, its issuer and "
                "audience, the claim the subject lands on, the header that "
                "carries it, and the principal kind the resulting subject "
                "takes"
            ),
        )
    subject_header = str(raw["subject_header"]).strip()
    _require_header_name(subject_header, "subject_header")
    extra_raw = raw.get("extra_claim_headers") or {}
    if not isinstance(extra_raw, Mapping):
        raise SubstrateError(
            "nomos.principal_mapping.extra_claim_headers must be a mapping of "
            "claim name to header name",
            data={"got": type(extra_raw).__name__},
        )
    extra: dict[str, str] = {}
    for claim_name, header_name in extra_raw.items():
        claim_text = str(claim_name).strip()
        header_text = str(header_name or "").strip()
        if not claim_text:
            raise SubstrateError(
                "nomos.principal_mapping.extra_claim_headers has an empty "
                "claim name",
            )
        if claim_text.lower() in _RESERVED_CLAIM_NAMES:
            raise SubstrateError(
                "nomos.principal_mapping.extra_claim_headers may not declare "
                f"the reserved claim {claim_text!r}: it feeds the identity "
                "machinery's own trust decisions (assurance, MFA, freshness, "
                "issuer/audience, verifier evidence), so a header carrying it "
                "would let the caller assert that its own claim was verified",
                data={
                    "claim": claim_text,
                    "reserved_claims": sorted(_RESERVED_CLAIM_NAMES),
                },
                fix_hint=(
                    "an extra claim header may carry a descriptive attribute "
                    "(a department, a cost centre) that policy MATCHES on. It "
                    "may not carry anything the identity machinery BINDS a "
                    "subject from (groups, roles, entitlements) or checks "
                    "trust with (assurance, MFA, expiry, verifier evidence)"
                ),
            )
        _require_header_name(header_text, f"extra_claim_headers[{claim_text}]")
        extra[claim_text] = header_text
    principal_kind = str(raw["principal_kind"]).strip()
    from okg.substrate.nomos.vocabulary import PRINCIPAL_KINDS

    if principal_kind not in PRINCIPAL_KINDS:
        raise SubstrateError(
            "nomos.principal_mapping.principal_kind must be one of "
            f"{sorted(PRINCIPAL_KINDS)}; got {principal_kind!r}",
            data={"principal_kind": principal_kind},
            fix_hint=(
                "name the principal kind the resulting subject takes, e.g. "
                "mcp_client"
            ),
        )
    subject_claim = str(raw["subject_claim"]).strip()
    if subject_claim in extra:
        # Two headers feeding one claim is an ambiguity the request path
        # would have to resolve by precedence -- exactly the collision this
        # PACT exists to decide explicitly rather than leave to chance.
        raise SubstrateError(
            "nomos.principal_mapping declares the subject claim "
            f"{subject_claim!r} in extra_claim_headers as well as "
            "subject_header; one claim must have one source",
            data={"subject_claim": subject_claim},
        )
    return PrincipalMapping(
        provider_id=str(raw["provider_id"]).strip(),
        issuer=str(raw["issuer"]).strip(),
        audience=str(raw["audience"]).strip(),
        subject_claim=subject_claim,
        subject_header=subject_header,
        principal_kind=principal_kind,
        extra_claim_headers=extra,
    )


def _require_header_name(value: str, label: str) -> None:
    if not _HEADER_NAME_RE.match(value):
        raise SubstrateError(
            f"nomos.principal_mapping.{label} is not a valid HTTP header "
            "field name",
            data={"value": value},
            fix_hint="use a token such as X-OKG-Principal",
        )


def principal_mapping_from_manifest(manifest: Any) -> PrincipalMapping | None:
    """Return the deployment's declared mapping, or None when absent.

    Absent is the single-operator case and must stay ordinary: a deployment
    that never declared the block keeps today's behavior exactly.
    """
    nomos_block = getattr(manifest, "nomos", None)
    if not isinstance(nomos_block, Mapping):
        return None
    raw = nomos_block.get("principal_mapping")
    if raw is None:
        return None
    mapping = parse_principal_mapping(raw)
    posture = nomos_block.get("deployment_posture")
    read_principals = (
        posture.get("read_principals") if isinstance(posture, Mapping) else None
    )
    if read_principals is not None:
        admitted = {str(item) for item in read_principals}
        if mapping.principal_kind not in admitted:
            # The deployment's own posture is the authority on which principal
            # kinds may read. A mapping minting subjects of an unadmitted kind
            # would be governed by nothing.
            raise SubstrateError(
                "nomos.principal_mapping.principal_kind "
                f"{mapping.principal_kind!r} is not in this deployment's "
                "nomos.deployment_posture.read_principals",
                data={
                    "principal_kind": mapping.principal_kind,
                    "read_principals": sorted(admitted),
                },
                fix_hint=(
                    "add the kind to read_principals, or map to a kind the "
                    "posture already admits (e.g. mcp_client)"
                ),
            )
    return mapping


def validate_mapping_against_providers(
    mapping: PrincipalMapping,
    providers: Sequence[Any],
    *,
    deployment_name: str,
) -> Any:
    """Refuse a mapping the deployment's identity providers cannot back.

    Load-time, not request-time. Three refusals, each closing a way the
    machinery would otherwise accept an unverified assertion as verified
    evidence:

    1. **No such provider.** ``normalize_identity_claim`` marks an unknown
       provider ``unknown_issuer`` and denies -- correct, but the operator
       would only learn at the first request that multi-user mode never
       worked. Refuse at start.
    2. **No verifier metadata.** The verified_by/verified_at checks in
       ``normalize_identity_claim`` run only when the provider demands them.
       Without that the server's stamps are recorded but never CHECKED, and a
       claim carrying no verifier evidence at all still reads valid. A
       mapping naming such a provider is refused.

       Note what this does NOT delegate to: ``_requires_trusted_verifier``
       also returns True on ``metadata.production``, which turns the checks
       on without naming a single trusted verifier -- and with an EMPTY
       ``trusted_verifiers`` list the membership test at identity.py:545 is
       skipped entirely, so the only surviving check is "verified_by is
       non-empty", which this server always satisfies by construction. An
       empty reference set passes every membership test. The requirement
       names ``trusted_verifiers`` or ``require_verified_by``, so those two
       are checked here directly.
    3. **Issuer / audience / subject-claim skew.** These are declared in the
       manifest so the mapping is readable on its own, which means they can
       drift from the provider row. Skew would surface as a per-request
       ``issuer_mismatch`` deny that looks like a policy decision; catch it
       at load.
    """
    from okg.substrate.nomos.identity import _metadata_bool, _str_tuple

    by_id = {getattr(p, "provider_id", None): p for p in providers}
    provider = by_id.get(mapping.provider_id)
    if provider is None:
        raise SubstrateError(
            f"nomos.principal_mapping names identity provider "
            f"{mapping.provider_id!r}, which is not registered for "
            f"deployment {deployment_name!r}; refusing to start multi-user "
            "mode against a provider that does not exist",
            data={
                "provider_id": mapping.provider_id,
                "deployment_name": deployment_name,
                "registered_provider_ids": sorted(
                    str(key) for key in by_id if key is not None
                ),
            },
            fix_hint=(
                "register the provider with `okg nomos provider add "
                "--provider-type mcp_principal_header` (the issuer, "
                "audience and subject claim this block declares, and "
                "--trusted-verifier for the transport verifier) before "
                "enabling multi-user principals"
            ),
        )
    provider_metadata = getattr(provider, "metadata", None) or {}
    trusted_verifiers = _str_tuple(provider_metadata.get("trusted_verifiers") or ())
    requires_verified_by = _metadata_bool(
        provider_metadata.get("require_verified_by")
    )
    if not trusted_verifiers and not requires_verified_by:
        raise SubstrateError(
            f"nomos.principal_mapping names identity provider "
            f"{mapping.provider_id!r}, whose metadata sets neither "
            "trusted_verifiers nor require_verified_by, so the verifier "
            "evidence this server stamps would never be checked and a claim "
            "with no verifier evidence would read as valid; refusing to "
            "start multi-user mode",
            data={
                "provider_id": mapping.provider_id,
                "deployment_name": deployment_name,
            },
            fix_hint=(
                "set metadata.trusted_verifiers to the transport verifier "
                f"id(s) permitted to assert principals (e.g. "
                f"{TRANSPORT_VERIFIER_PREFIX}okg-mcp-static-bearer), or set "
                "metadata.require_verified_by: true"
            ),
        )
    # The provider names its OWN binding and trust claim paths, so the static
    # reserved list in parse_principal_mapping cannot cover them (a shipped
    # fixture uses `cern_roles`). Refuse here, where the provider row is in
    # hand.
    provider_claim_paths = {
        str(name).strip().lower()
        for group in (
            getattr(provider, "group_claims", ()) or (),
            getattr(provider, "role_claims", ()) or (),
            getattr(provider, "entitlement_claims", ()) or (),
            (getattr(provider, "assurance_claim", None),),
            (getattr(provider, "subject_claim", None),),
        )
        for name in group
        if name
    }
    conflicting = sorted(
        claim
        for claim in mapping.extra_claim_headers
        if claim.strip().lower() in provider_claim_paths
    )
    if conflicting:
        raise SubstrateError(
            "nomos.principal_mapping.extra_claim_headers declares claim(s) "
            + ", ".join(repr(claim) for claim in conflicting)
            + f" that identity provider {mapping.provider_id!r} uses to bind "
            "subjects or to check assurance; a header carrying one of them "
            "would let the caller choose its own subject or vouch for itself",
            data={
                "provider_id": mapping.provider_id,
                "conflicting_claims": conflicting,
                "provider_claim_paths": sorted(provider_claim_paths),
            },
            fix_hint=(
                "map only descriptive attributes the policy MATCHES on, never "
                "the claims the provider BINDS subjects from"
            ),
        )
    skew: dict[str, Any] = {}
    if str(getattr(provider, "issuer", "")) != mapping.issuer:
        skew["issuer"] = {
            "declared": mapping.issuer,
            "provider": str(getattr(provider, "issuer", "")),
        }
    audiences = tuple(getattr(provider, "audiences", ()) or ())
    if mapping.audience not in {str(item) for item in audiences}:
        skew["audience"] = {
            "declared": mapping.audience,
            "provider": [str(item) for item in audiences],
        }
    provider_subject_claim = str(getattr(provider, "subject_claim", "") or "")
    if provider_subject_claim and provider_subject_claim != mapping.subject_claim:
        skew["subject_claim"] = {
            "declared": mapping.subject_claim,
            "provider": provider_subject_claim,
        }
    if skew:
        raise SubstrateError(
            "nomos.principal_mapping disagrees with the registered identity "
            f"provider {mapping.provider_id!r}; every request would be "
            "denied for a mismatch that looks like a policy decision",
            data={"provider_id": mapping.provider_id, "skew": skew},
            fix_hint=(
                "align the manifest block with the registered provider row "
                "(issuer, audience, subject_claim)"
            ),
        )
    return provider


def unidentified_principal_error(
    detail: str,
    *,
    channel: str,
    fix_hint: str | None = None,
    data: Mapping[str, Any] | None = None,
) -> SubstrateError:
    """The refusal for "nobody was identified".

    ``channel`` names WHERE the identification failed (absent header, empty
    value, malformed value, no subject binding), so an operator reading the
    error can tell a misconfigured client from a missing binding without
    reading the server's source.
    """
    err = SubstrateError(
        f"multi-user MCP request refused: {detail}",
        data={
            "reason_code": UNIDENTIFIED_PRINCIPAL_REASON,
            "refusal_kind": "unidentified_principal",
            "channel": channel,
            "subject": UNIDENTIFIED_SUBJECT,
            **dict(data or {}),
        },
        fix_hint=fix_hint
        or (
            "the fronting application must send the deployment's declared "
            "principal header on every request, carrying the signed-in "
            "user's identifier"
        ),
    )
    err.code = "nomos_principal_unidentified"
    return err


def blocked_principal_error(
    *,
    data: Mapping[str, Any] | None = None,
) -> SubstrateError:
    """The refusal for a user an operator blocked.

    Distinct from "nobody was identified": the principal WAS identified, and
    a block outranks every binding that would have admitted them.
    """
    err = SubstrateError(
        "multi-user MCP request refused: this user is blocked for the "
        "deployment; a block outranks every role and user binding",
        data={
            "reason_code": BLOCKED_PRINCIPAL_REASON,
            "refusal_kind": "blocked_principal",
            "channel": "principal_blocked",
            "subject": UNIDENTIFIED_SUBJECT,
            **dict(data or {}),
        },
        fix_hint=(
            "an operator lifts a block with `okg nomos binding unblock`; "
            "`okg nomos binding list` shows blocks (subject nomos:blocked)"
        ),
    )
    err.code = "nomos_principal_blocked"
    return err


def conflicting_identity_channel_error(
    *,
    data: Mapping[str, Any] | None = None,
) -> SubstrateError:
    """The refusal for a caller that supplied its own identity claim.

    Precedence is decided, not left to collide: in multi-user mode the
    declared header mapping is the SOLE identity input. A caller-supplied
    ``authority.nomos_identity_claim`` is rejected outright -- never merged
    with the header-derived claim, never preferred over it, and never
    evaluated (evaluating it would persist a self-assertion as identity
    evidence).
    """
    err = SubstrateError(
        "multi-user MCP request refused: the call supplied "
        "authority.nomos_identity_claim, but in multi-user mode the "
        "deployment's declared principal header is the sole identity input; "
        "a self-asserted identity claim is never merged with it and never "
        "preferred over it",
        data={
            "reason_code": CONFLICTING_IDENTITY_CHANNEL_REASON,
            "refusal_kind": "conflicting_identity_channel",
            "subject": UNIDENTIFIED_SUBJECT,
            **dict(data or {}),
        },
        fix_hint=(
            "drop the `authority.nomos_identity_claim` argument; identity "
            "comes from the transport-authenticated principal header"
        ),
    )
    err.code = "nomos_identity_channel_conflict"
    return err


def multi_user_requires_auth_error(*, transport: str) -> SubstrateError:
    """Refuse multi-user mode on a transport that authenticates nobody.

    A client-asserted principal header is only meaningful once the transport
    has authenticated the CLIENT. On an unauthenticated endpoint any caller
    can assert any user's identity, so the delegation this PACT builds would
    have nothing underneath it.
    """
    err = SubstrateError(
        "refusing to start multi-user principal mapping over transport "
        f"{transport!r} without transport authentication: a principal header "
        "asserted by an unauthenticated client is a claim anyone who can "
        "reach the port may forge, so the server would attribute reads to "
        "users who never made them",
        data={"transport": transport, "auth_active": False},
        fix_hint=(
            "require a bearer token with --auth-token-env VARIABLE_NAME (the "
            "token authenticates the fronting application, whose SSO then "
            "names the user in the declared principal header)"
        ),
    )
    err.code = "mcp_multi_user_requires_auth"
    return err


def refusal_reason_code(error: BaseException) -> str | None:
    """The reason code a principal refusal carries, or None if it is not one.

    The single place that reads the typed field, so callers never re-derive
    "is this a refusal?" from a message string.
    """
    data = getattr(error, "data", None)
    if not isinstance(data, Mapping):
        return None
    reason = data.get("reason_code")
    if reason in {
        UNIDENTIFIED_PRINCIPAL_REASON,
        CONFLICTING_IDENTITY_CHANNEL_REASON,
        BLOCKED_PRINCIPAL_REASON,
    }:
        return str(reason)
    return None


def persist_principal_refusal_decision(
    conn: Any,
    *,
    deployment_name: str,
    reason_code: str,
    action: str,
    scope: str,
    resource: Mapping[str, Any],
    target: Mapping[str, Any] | None = None,
    classification: str = "internal",
    enforcement_point: str = "mcp.principal_mapping",
    channel: str | None = None,
    identity_channel: str = HEADER_IDENTITY_CHANNEL,
) -> str:
    """Write the audit row for a refused request and return its decision id.

    ``okg.nomos_decisions.subject`` is NOT NULL and ``effect`` has no
    non-deny refusal value, so "nobody was identified" cannot be recorded as
    an absence -- it has to be a value. It is recorded as the sentinel
    subject with a refusal-specific reason code, which is what keeps it
    distinguishable from an ordinary policy deny (whose subject is a real
    principal and whose reason names a binding or a grant).

    Identity is content-addressed exactly as elsewhere in Nomos, so a repeat
    identical refusal persists no new row (``ON CONFLICT DO NOTHING``). Row
    COUNTS are therefore not a contract here; row CONTENT is.
    """
    from okg.substrate.nomos.core import DecisionInput, PolicyDecision
    from okg.substrate.nomos.identity import _sha256_hex
    from okg.substrate.nomos.identity_store import persist_nomos_decision

    context: dict[str, Any] = {
        "scope": scope,
        "refusal_kind": {
            CONFLICTING_IDENTITY_CHANNEL_REASON: "conflicting_identity_channel",
            BLOCKED_PRINCIPAL_REASON: "blocked_principal",
        }.get(reason_code, "unidentified_principal"),
        "identity_channel": identity_channel,
    }
    if channel:
        context["refusal_channel"] = channel
    decision_input = DecisionInput(
        subject=UNIDENTIFIED_SUBJECT,
        action=action,
        classification=classification,
        resource=dict(resource),
        context=context,
    )
    input_hash = "sha256:" + _sha256_hex(decision_input.to_dict())
    decision_id = "nomos:decision:" + _sha256_hex(
        {
            "policy_fingerprint": None,
            "input_hash": input_hash,
            "effect": "deny",
            "reason_code": reason_code,
        }
    )[:24]
    decision = PolicyDecision(
        decision_id=decision_id,
        policy_bundle_id=None,
        policy_version=None,
        policy_fingerprint=None,
        input_hash=input_hash,
        subject=UNIDENTIFIED_SUBJECT,
        action=action,
        resource=dict(resource),
        classification=classification,
        context=context,
        effect="deny",
        reason_code=reason_code,
        matched_rule_ids=(),
        obligations=(),
        enforcement_point=enforcement_point,
    )
    persist_nomos_decision(
        conn,
        deployment_name=deployment_name,
        decision=decision,
        target=dict(target or {"kind": "mcp_principal_refusal"}),
        # The refusal must be readable by an auditor even though the caller's
        # request is about to raise, so it is written now, not buffered into a
        # batch that the raising path would discard.
        allow_deferred=False,
    )
    try:
        conn.commit()
    except Exception:  # noqa: BLE001 — audit row is best-effort against a severed conn; the refusal itself still propagates.
        pass
    return decision_id


def current_request_headers() -> Mapping[str, str] | None:
    """Headers of the HTTP request carrying the in-flight tool call.

    Read through the SDK's ``request_ctx`` rather than a contextvar of our
    own. The streamable-HTTP transport hands the ASGI request across a
    stream to the server task, so an ambient contextvar set during the ASGI
    call does NOT survive into the handler; ``RequestContext.request`` is the
    channel the SDK explicitly carries. Returns None off an HTTP transport
    (stdio) or outside a request -- and None is refused in multi-user mode,
    never defaulted.
    """
    try:
        from mcp.server.lowlevel.server import request_ctx
    except Exception:  # pragma: no cover - SDK import shape
        return None
    try:
        context = request_ctx.get()
    except LookupError:
        return None
    request = getattr(context, "request", None)
    headers = getattr(request, "headers", None)
    if headers is None:
        return None
    return headers


def current_transport_identity() -> str | None:
    """The authenticated transport client id for the in-flight request.

    This is the identity the server stamps as ``verified_by``: the holder of
    the app token, i.e. WHO asserted the principal. It is read from the
    request scope the SDK's auth middleware populated, so it cannot be
    supplied by the caller. None means the request arrived on a surface that
    authenticated nobody -- refused in multi-user mode, because a delegated
    claim with no delegator recorded is an anonymous assertion.
    """
    try:
        from mcp.server.lowlevel.server import request_ctx
    except Exception:  # pragma: no cover - SDK import shape
        return None
    try:
        context = request_ctx.get()
    except LookupError:
        return None
    request = getattr(context, "request", None)
    scope = getattr(request, "scope", None)
    if not isinstance(scope, Mapping):
        return None
    user = scope.get("user")
    access_token = getattr(user, "access_token", None)
    client_id = getattr(access_token, "client_id", None)
    if client_id is None or not str(client_id).strip():
        return None
    return f"{TRANSPORT_VERIFIER_PREFIX}{str(client_id).strip()}"


def _principal_value(
    headers: Mapping[str, str],
    header_name: str,
    *,
    label: str,
) -> str:
    # Starlette's Headers.__getitem__ returns the FIRST matching value, so a
    # repeated header would silently discard every later one -- and a front
    # proxy that APPENDS the trusted principal header rather than replacing it
    # would lose to the client's own header. Ask for the whole list when the
    # mapping supports it and refuse more than one, so "which value wins" is
    # never an accident.
    getlist = getattr(headers, "getlist", None)
    if callable(getlist):
        values = [str(item).strip() for item in getlist(header_name)]
        if len(values) > 1:
            raise unidentified_principal_error(
                f"the declared {label} header {header_name!r} was sent "
                f"{len(values)} times; a principal must be unambiguous",
                channel="header_ambiguous",
                data={"header": header_name, "occurrences": len(values)},
            )
    raw = headers.get(header_name)
    if raw is None:
        raise unidentified_principal_error(
            f"the declared {label} header {header_name!r} was not sent",
            channel="header_absent",
            data={"header": header_name},
        )
    value = str(raw).strip()
    if not value:
        raise unidentified_principal_error(
            f"the declared {label} header {header_name!r} was sent empty",
            channel="header_empty",
            data={"header": header_name},
        )
    if "," in value:
        # A comma inside a single value is the same ambiguity by another
        # route: some proxies fold repeats into one comma-joined value
        # instead of emitting the header twice.
        raise unidentified_principal_error(
            f"the declared {label} header {header_name!r} carried more than "
            "one value; a principal must be unambiguous",
            channel="header_ambiguous",
            data={"header": header_name},
        )
    if len(value) > _PRINCIPAL_VALUE_MAX:
        raise unidentified_principal_error(
            f"the declared {label} header {header_name!r} exceeds "
            f"{_PRINCIPAL_VALUE_MAX} characters",
            channel="header_malformed",
            data={"header": header_name, "length": len(value)},
        )
    if not _PRINCIPAL_VALUE_RE.match(value):
        # The value is NOT echoed: it is attacker-controlled request data and
        # this message reaches logs and evidence artifacts.
        raise unidentified_principal_error(
            f"the declared {label} header {header_name!r} carried a "
            "malformed value (a principal is one printable-ASCII token with "
            "no spaces or control characters)",
            channel="header_malformed",
            data={"header": header_name},
        )
    return value


def identity_claim_for_request(
    mapping: Any,
    *,
    headers: Mapping[str, str] | None,
    transport_identity: str | None,
    access_token: Any | None = None,
) -> dict[str, Any]:
    """This request's identity claim, from the channel the mode declares.

    Header mode reads the declared principal header, vouched for by the
    transport token (PACT mcp-principal-mapping). OIDC mode reads the bearer
    token this server verified itself (PACT mcp-oidc-auth): ``access_token``
    when the caller holds it, otherwise the in-flight request's. Both build
    the same claim shape. Every place that turns a request into a claim --
    the session-owner middleware, the tool gate, the resource gate and the
    read path -- asks this one function, so a mode cannot be wired into one
    of them and forgotten in another.
    """
    from okg.substrate.mcp.oidc_auth import OidcAuthConfig, oidc_claim_for_request

    if isinstance(mapping, OidcAuthConfig):
        return oidc_claim_for_request(mapping, access_token=access_token)
    return header_claim_for_request(
        mapping, headers, transport_identity=transport_identity,
    )


def header_claim_for_request(
    mapping: PrincipalMapping,
    headers: Mapping[str, str] | None,
    *,
    transport_identity: str | None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build the identity claim for one request from its declared headers.

    The returned claim is the ONLY identity input the multi-user request path
    accepts. Every field the machinery uses to decide trust --
    ``verification_status``, ``verified_by``, ``verified_at``,
    ``verification_method``, ``auth_time`` -- is set HERE, by the server, from
    the authenticated transport, and none of them can be declared as an extra
    claim header (see ``_RESERVED_CLAIM_NAMES``). So a caller cannot assert
    that its own claim was verified, nor that it satisfied MFA.

    What a caller DOES supply is the header values themselves: the principal
    identifier and any declared extra claims. Those are request data, checked
    for shape here and bound to a subject by Nomos -- never authorization by
    themselves.
    """
    if headers is None:
        raise unidentified_principal_error(
            "the request carried no HTTP headers, so the declared principal "
            "header could not be read (multi-user mode requires an HTTP "
            "transport)",
            channel="no_request_context",
        )
    if not transport_identity:
        raise unidentified_principal_error(
            "the request carried no authenticated transport identity, so the "
            "server cannot record WHO asserted the principal; a delegated "
            "claim with no delegator is an anonymous assertion",
            channel="no_transport_identity",
            fix_hint=(
                "serve with --auth-token-env VARIABLE_NAME so callers "
                "present the application bearer token"
            ),
        )
    subject_value = _principal_value(
        headers, mapping.subject_header, label="principal",
    )
    claims: dict[str, Any] = {mapping.subject_claim: subject_value}
    for claim_name, header_name in sorted(mapping.extra_claim_headers.items()):
        raw = headers.get(header_name)
        if raw is None:
            continue
        claims[claim_name] = _principal_value(
            headers, header_name, label=f"{claim_name} claim",
        )
    stamped_at = (now or datetime.now(UTC)).astimezone(UTC).isoformat()
    return {
        "provider_id": mapping.provider_id,
        "issuer": mapping.issuer,
        "audiences": [mapping.audience],
        "external_subject": subject_value,
        "claims": claims,
        # Explicit, never defaulted. normalize_identity_claim would treat an
        # omitted status as `valid` (identity.py:488) -- an absence reading as
        # a satisfied signal. The server states it, and
        # require_explicit_verification_status refuses any claim on this path
        # that does not.
        "verification_status": "valid",
        "verified_by": transport_identity,
        "verified_at": stamped_at,
        "verification_method": PRINCIPAL_HEADER_VERIFICATION_METHOD,
        # auth_time mirrors verified_at so the provider's freshness_seconds
        # gate has a timestamp to measure; without one every freshness check
        # passes vacuously (identity.py:1086 returns False on a None time).
        "auth_time": stamped_at,
    }


def require_explicit_verification_status(claim: Mapping[str, Any]) -> None:
    """Refuse a header-derived claim that does not state its verification.

    On this path an omitted ``verification_status`` must never reach
    ``normalize_identity_claim``, which defaults it to ``valid``
    (identity.py:488) -- an absent signal read as a satisfied one. The same
    reasoning covers an omitted ``verified_by``/``verified_at``: the
    provider's verifier checks only run when the provider demands them, so a
    claim with no verifier evidence could otherwise be evaluated as though
    the transport had vouched for it.
    """
    if not isinstance(claim, Mapping):
        raise unidentified_principal_error(
            "the header-derived identity claim is not an object",
            channel="claim_malformed",
        )
    raw_status = claim.get("verification_status")
    if raw_status is None or not str(raw_status).strip():
        raise unidentified_principal_error(
            "the header-derived identity claim carries no explicit "
            "verification_status; an unstated verification is refused, never "
            "defaulted to valid",
            channel="claim_unverified",
        )
    if not str(claim.get("verified_by") or "").strip():
        raise unidentified_principal_error(
            "the header-derived identity claim carries no verified_by stamp, "
            "so the audit record could not say who asserted the principal",
            channel="claim_unverified",
        )
    if not str(claim.get("verified_at") or "").strip():
        raise unidentified_principal_error(
            "the header-derived identity claim carries no verified_at stamp, "
            "so every freshness gate would pass vacuously",
            channel="claim_unverified",
        )
