"""Fail-closed authentication and sender-constraint verification.

The review service never trusts a principal from a JSON body or arbitrary
forwarded header.  ``authenticate_request`` accepts only verifier-produced
identity mappings, direct mTLS metadata, or a configured trusted-proxy
assertion.  DPoP nonces are delegated to a durable store before dispatch;
``MemoryReplayNonceStore`` is for tests and explicitly not a production
durability substitute.
"""

from __future__ import annotations

import contextvars
import hashlib
import hmac
import math
import re
import ssl
import time
from dataclasses import dataclass, fields
from datetime import UTC, datetime, timedelta
from typing import Any, Callable, Iterable, Mapping, Protocol, Sequence
from urllib.parse import urlsplit, urlunsplit

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec, padding
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from cryptography.hazmat.primitives.asymmetric.utils import encode_dss_signature
from cryptography import x509

from .signing import SigningError, b64url_decode, b64url_encode, canonical_json_bytes, strict_json_loads


class AuthenticationError(ValueError):
    """Safe wire error; ``detail`` never contains credential material."""

    status_code = 401

    def __init__(self, code: str = "authentication_required", detail: str | None = None) -> None:
        super().__init__(detail or code)
        self.code = code
        self.detail = detail or code


class ReplayNonceStore(Protocol):
    def insert_nonce(
        self,
        *,
        deployment: str,
        issuer_digest: str,
        sender_key_thumbprint: str,
        jti_digest: str,
        request_method: str,
        request_uri_digest: str,
        access_token_hash: str,
        observed_at: datetime,
        expires_at: datetime,
    ) -> bool: ...


@dataclass(frozen=True)
class VerifiedPrincipal:
    principal: str
    separation_subject: str
    provider_id: str
    subject_binding_id: str
    issuer: str | None = None
    auth_method: str = "unknown"
    sender_key_thumbprint: str | None = None
    token_id_digest: str | None = None
    dpop_jti_digest: str | None = None
    credential_expires_at: datetime | None = None
    # These fields are verifier output, never request input.  The provider and
    # binding timestamps are the durable token revocation/reenable barriers.
    # Generation is deliberately represented as a digest: the database owns
    # its interpretation and a certificate cannot invent a generation number.
    provider_status: str | None = None
    binding_status: str | None = None
    provider_updated_at: datetime | None = None
    binding_updated_at: datetime | None = None
    provider_credential_generation_digest: str | None = None
    binding_credential_generation_digest: str | None = None
    credential_generation_digest: str | None = None
    presented_credential_generation_digest: str | None = None
    # A custom verifier may return a fully verified JWT principal instead of
    # calling ``authenticate_request``.  Preserve its issued-at claim so the
    # API can still apply the same durable provider/binding barrier.
    token_issued_at: datetime | None = None
    provider_certificate_thumbprint: str | None = None
    binding_certificate_thumbprint: str | None = None
    certificate_thumbprint: str | None = None
    certificate_not_before: datetime | None = None
    certificate_not_after: datetime | None = None

    def __post_init__(self) -> None:
        for name in ("principal", "separation_subject", "provider_id", "subject_binding_id"):
            value = getattr(self, name)
            if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9._:+\-]{1,256}", value):
                raise AuthenticationError("authentication_required")

    @property
    def actor_principal(self) -> str:
        return self.principal

    @property
    def actor_separation_subject(self) -> str:
        return self.separation_subject


@dataclass(frozen=True)
class SenderConstraint:
    kind: str
    key_thumbprint: str | None = None


@dataclass(frozen=True)
class AuthAudit:
    """Safe data suitable for ``external_action_auth_audits``."""

    principal: str
    separation_subject: str
    provider_id: str
    subject_binding_id: str
    authentication_method_code: str
    issuer_or_trust_domain_digest: str
    sender_key_or_certificate_thumbprint: str | None
    token_or_certificate_id_digest: str | None
    dpop_jti_digest: str | None
    credential_expires_at: datetime | None
    # These values are verified by the authentication boundary and retained
    # with the audit projection.  Lifecycle SQL must compare them with the
    # provider/binding rows again while its transaction lock is held; they are
    # not a trust decision made by the HTTP adapter.
    result_code: str = "authenticated"
    provider_status: str | None = None
    binding_status: str | None = None
    provider_updated_at: datetime | None = None
    binding_updated_at: datetime | None = None
    provider_credential_generation_digest: str | None = None
    binding_credential_generation_digest: str | None = None
    credential_generation_digest: str | None = None
    presented_credential_generation_digest: str | None = None
    token_issued_at: datetime | None = None
    certificate_thumbprint: str | None = None
    certificate_not_before: datetime | None = None
    certificate_not_after: datetime | None = None

    def as_dict(self) -> dict[str, Any]:
        def timestamp(value: datetime | None) -> str | None:
            if value is None:
                return None
            return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")

        return {
            "principal": self.principal,
            "separation_subject": self.separation_subject,
            "nomos_provider_id": self.provider_id,
            "nomos_subject_binding_id": self.subject_binding_id,
            "authentication_method_code": self.authentication_method_code,
            "issuer_or_trust_domain_digest": self.issuer_or_trust_domain_digest,
            "sender_key_or_certificate_thumbprint": self.sender_key_or_certificate_thumbprint,
            "token_or_certificate_id_digest": self.token_or_certificate_id_digest,
            "dpop_jti_digest": self.dpop_jti_digest,
            "credential_expires_at": timestamp(self.credential_expires_at),
            "provider_status": self.provider_status,
            "binding_status": self.binding_status,
            "provider_updated_at": timestamp(self.provider_updated_at),
            "binding_updated_at": timestamp(self.binding_updated_at),
            "provider_credential_generation_digest": self.provider_credential_generation_digest,
            "binding_credential_generation_digest": self.binding_credential_generation_digest,
            "credential_generation_digest": self.credential_generation_digest,
            "presented_credential_generation_digest": self.presented_credential_generation_digest,
            "token_issued_at": timestamp(self.token_issued_at),
            "certificate_thumbprint": self.certificate_thumbprint,
            "certificate_not_before": timestamp(self.certificate_not_before),
            "certificate_not_after": timestamp(self.certificate_not_after),
            "result_code": self.result_code,
        }


_current_principal: contextvars.ContextVar[VerifiedPrincipal | None] = contextvars.ContextVar(
    "external_action_verified_principal", default=None
)
_HEADER_NAMES = {"authorization", "dpop", "idempotency-key", "if-match", "content-length", "content-type"}


def _header_pairs(headers: Mapping[str, str] | Iterable[tuple[str | bytes, str | bytes]]) -> list[tuple[str, str]]:
    if isinstance(headers, Mapping):
        items = list(headers.items())
    else:
        items = list(headers)
    result: list[tuple[str, str]] = []
    for key, value in items:
        if isinstance(key, bytes):
            try:
                key = key.decode("ascii")
            except UnicodeDecodeError as exc:
                raise AuthenticationError("authentication_required") from exc
        if isinstance(value, bytes):
            try:
                value = value.decode("ascii")
            except UnicodeDecodeError as exc:
                raise AuthenticationError("authentication_required") from exc
        if not isinstance(key, str) or not isinstance(value, str) or "\r" in value or "\n" in value:
            raise AuthenticationError("authentication_required")
        result.append((key.lower(), value))
    for name in _HEADER_NAMES:
        values = [value for key, value in result if key == name]
        if len(values) > 1:
            raise AuthenticationError("authentication_required")
        if values and ("," in values[0] or "\r" in values[0] or "\n" in values[0]):
            raise AuthenticationError("authentication_required")
    return result


def _one_header(pairs: Sequence[tuple[str, str]], name: str) -> str | None:
    values = [value for key, value in pairs if key == name.lower()]
    return values[0] if values else None


def _jwt_parts(token: str) -> tuple[str, str, str, Mapping[str, Any], Mapping[str, Any]]:
    if not isinstance(token, str) or token.count(".") != 2:
        raise AuthenticationError("authentication_required")
    head, body, sig = token.split(".")
    if not head or not body or not sig:
        raise AuthenticationError("authentication_required")
    try:
        header = strict_json_loads(b64url_decode(head, field="jwt.header"))
        claims = strict_json_loads(b64url_decode(body, field="jwt.payload"))
        signature = b64url_decode(sig, field="jwt.signature")
    except (SigningError, ValueError) as exc:
        raise AuthenticationError("authentication_required") from exc
    if not isinstance(header, Mapping) or not isinstance(claims, Mapping) or not signature:
        raise AuthenticationError("authentication_required")
    return head, body, sig, header, claims


def _as_time(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise AuthenticationError("authentication_required")
    if not math.isfinite(float(value)) or value < 0 or value > 9_223_372_036:
        raise AuthenticationError("authentication_required")
    return float(value)


def _verify_jwt_signature(signing_input: bytes, signature: bytes, header: Mapping[str, Any], keys: Mapping[str, Any], algorithms: Sequence[str]) -> None:
    alg = header.get("alg")
    kid = header.get("kid")
    if not isinstance(alg, str) or alg not in algorithms or not isinstance(kid, str) or set(header) - {"alg", "kid", "typ"} or ("typ" in header and header.get("typ") != "JWT"):
        raise AuthenticationError("authentication_required")
    key = keys.get(kid)
    if key is None:
        raise AuthenticationError("authentication_required")
    try:
        if alg == "EdDSA":
            raw = b64url_decode(key, field="jwks.key") if isinstance(key, str) else key
            Ed25519PublicKey.from_public_bytes(raw).verify(signature, signing_input)
        elif alg == "RS256":
            # PEM/JWK loading belongs to deployment provisioning. This
            # boundary accepts only an already constructed public key and the
            # exact JWT algorithm parameters.
            key.verify(signature, signing_input, padding.PKCS1v15(), hashes.SHA256())
        elif alg == "ES256":
            # JOSE carries fixed-width raw R||S while cryptography verifies
            # the ASN.1 DER representation.
            if len(signature) != 64:
                raise AuthenticationError("authentication_required")
            der_signature = encode_dss_signature(
                int.from_bytes(signature[:32], "big"),
                int.from_bytes(signature[32:], "big"),
            )
            key.verify(der_signature, signing_input, ec.ECDSA(hashes.SHA256()))
        else:
            raise AuthenticationError("authentication_required")
    except (InvalidSignature, ValueError, TypeError, AttributeError) as exc:
        raise AuthenticationError("authentication_required") from exc


def _principal_mapping(mapping: Mapping[str, Any] | Callable[[str, str], Any], issuer: str, subject: str) -> VerifiedPrincipal:
    try:
        if callable(mapping):
            value = mapping(issuer, subject)
        else:
            composite = mapping.get(f"{issuer}\x00{subject}")
            # A subject is scoped by its issuer.  Accepting a bare ``sub``
            # mapping would let an identically named subject in another
            # provider inherit this deployment's principal and separation
            # identity.
            value = composite
    except Exception as exc:
        raise AuthenticationError("authentication_required") from exc
    if isinstance(value, VerifiedPrincipal):
        return value
    if not isinstance(value, Mapping):
        raise AuthenticationError("authentication_required")
    metadata = value.get("metadata")
    if not isinstance(metadata, Mapping):
        metadata = {}

    def mapped(name: str, *aliases: str) -> Any:
        for candidate in (name, *aliases):
            if candidate in value:
                return value[candidate]
            if candidate in metadata:
                return metadata[candidate]
        return None

    try:
        return VerifiedPrincipal(
            principal=value["principal"],
            separation_subject=value["separation_subject"],
            provider_id=value["provider_id"],
            subject_binding_id=value["subject_binding_id"],
            issuer=issuer,
            auth_method="jwt",
            provider_status=mapped("provider_status"),
            binding_status=mapped("binding_status"),
            provider_updated_at=_coerce_time(mapped("provider_updated_at")),
            binding_updated_at=_coerce_time(mapped("binding_updated_at")),
            provider_credential_generation_digest=mapped(
                "provider_credential_generation_digest",
                "provider_generation_digest",
            ),
            binding_credential_generation_digest=mapped(
                "binding_credential_generation_digest",
                "binding_generation_digest",
            ),
            credential_generation_digest=mapped("credential_generation_digest"),
            presented_credential_generation_digest=mapped(
                "presented_credential_generation_digest", "certificate_generation_digest"
            ),
            token_issued_at=_coerce_time(mapped("token_issued_at", "jwt_issued_at", "credential_issued_at")),
            provider_certificate_thumbprint=mapped("provider_certificate_thumbprint"),
            binding_certificate_thumbprint=mapped("binding_certificate_thumbprint"),
            certificate_thumbprint=mapped("certificate_thumbprint"),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise AuthenticationError("authentication_required") from exc


def _coerce_time(value: Any) -> datetime | None:
    """Parse a deployment-owned RFC3339 timestamp without accepting ambiguity."""

    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise AuthenticationError("authentication_required")
        return value.astimezone(UTC)
    if not isinstance(value, str) or not re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z", value
    ):
        raise AuthenticationError("authentication_required")
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError as exc:
        raise AuthenticationError("authentication_required") from exc


def _principal_values(value: VerifiedPrincipal) -> dict[str, Any]:
    """Return slot-safe dataclass values for an authenticated principal."""

    return {field.name: getattr(value, field.name) for field in fields(value)}


def _validate_identity_barrier(
    principal: VerifiedPrincipal,
    *,
    issued_at: float | None = None,
    certificate_not_before: datetime | None = None,
    certificate_generation_digest: str | None = None,
    certificate_thumbprint: str | None = None,
    require_barrier: bool = False,
) -> None:
    """Apply the current provider/binding barrier to a verifier result.

    Missing provider rows are normally represented by a failed resolver.  If
    a production resolver returns a projection, however, it must include both
    rows when ``require_barrier`` is enabled.  The optional mode keeps the
    small in-memory authentication fixtures useful without weakening the
    production server gate.
    """

    statuses = (principal.provider_status, principal.binding_status)
    barriers = (principal.provider_updated_at, principal.binding_updated_at)
    if require_barrier and any(item is None for item in statuses + barriers):
        raise AuthenticationError("authentication_required")
    if require_barrier and principal.auth_method == "mtls" and certificate_not_before is None:
        raise AuthenticationError("sender_constraint_required")
    if any(item is not None and item not in {"active"} for item in statuses):
        raise AuthenticationError("authentication_required")
    if issued_at is not None:
        issued = datetime.fromtimestamp(issued_at, tz=UTC)
        for barrier in barriers:
            if barrier is not None and issued < barrier:
                raise AuthenticationError("authentication_required")
    if certificate_not_before is not None:
        for barrier in barriers:
            if barrier is not None and certificate_not_before < barrier:
                raise AuthenticationError("sender_constraint_required")
    generations = tuple(
        item
        for item in (
            principal.provider_credential_generation_digest,
            principal.binding_credential_generation_digest,
        )
        if item is not None
    )
    if principal.credential_generation_digest is not None:
        generations += (principal.credential_generation_digest,)
    if certificate_generation_digest is not None:
        if generations and any(not hmac.compare_digest(certificate_generation_digest, item) for item in generations):
            raise AuthenticationError("sender_constraint_required")
        if not generations and require_barrier:
            raise AuthenticationError("sender_constraint_required")
    elif certificate_not_before is not None and generations and require_barrier:
        # A current binding generation without a presented certificate
        # generation is not a proof of the sender credential.  This is the
        # barrier that prevents a pre-disable certificate from being revived.
        raise AuthenticationError("sender_constraint_required")
    expected_thumbprints = tuple(
        item
        for item in (
            principal.provider_certificate_thumbprint,
            principal.binding_certificate_thumbprint,
            principal.certificate_thumbprint,
        )
        if item is not None
    )
    if certificate_thumbprint is not None and expected_thumbprints:
        if any(not hmac.compare_digest(certificate_thumbprint, item) for item in expected_thumbprints):
            raise AuthenticationError("sender_constraint_required")


def _normalize_htu(uri: str) -> str:
    if not isinstance(uri, str) or any(ord(ch) <= 0x20 or ord(ch) > 0x7e for ch in uri):
        raise AuthenticationError("sender_constraint_required")
    try:
        parts = urlsplit(uri)
        if parts.scheme.lower() not in {"http", "https"} or not parts.hostname:
            raise AuthenticationError("sender_constraint_required")
        if parts.username is not None or parts.password is not None:
            raise AuthenticationError("sender_constraint_required")
        host = parts.hostname.lower()
        port = parts.port
    except (ValueError, AttributeError) as exc:
        raise AuthenticationError("sender_constraint_required") from exc
    if ":" in parts.hostname and not host.startswith("["):
        host = f"[{host}]"
    if port and not ((parts.scheme.lower() == "http" and port == 80) or (parts.scheme.lower() == "https" and port == 443)):
        host += f":{port}"
    path = parts.path or "/"
    # RFC 9449 excludes query and fragment.  Preserve percent encoding except
    # for unreserved octets; this avoids URL-decoding reserved delimiters.
    def unreserved(match: re.Match[str]) -> str:
        octet = int(match.group(1), 16)
        # RFC 3986's unreserved production is ASCII-only.  ``str.isalnum``
        # also accepts many non-ASCII code points, which would incorrectly
        # turn a percent-encoded UTF-8 byte into a different URI spelling.
        is_ascii_alnum = (
            0x30 <= octet <= 0x39
            or 0x41 <= octet <= 0x5A
            or 0x61 <= octet <= 0x7A
        )
        return chr(octet) if is_ascii_alnum or octet in b"-._~" else match.group(0).upper()
    path = re.sub(r"%([0-9A-Fa-f]{2})", unreserved, path)
    return urlunsplit((parts.scheme.lower(), host, path, "", ""))


def normalized_request_uri(uri: str) -> str:
    return _normalize_htu(uri)


def _jwk_thumbprint(jwk: Mapping[str, Any]) -> str:
    if set(jwk) != {"kty", "crv", "x"} or jwk.get("kty") != "OKP" or jwk.get("crv") != "Ed25519" or not isinstance(jwk.get("x"), str):
        raise AuthenticationError("sender_constraint_required")
    try:
        raw = b64url_decode(jwk["x"], field="dpop.jwk.x")
        if len(raw) != 32:
            raise AuthenticationError("sender_constraint_required")
        return b64url_encode(hashlib.sha256(canonical_json_bytes({"crv": "Ed25519", "kty": "OKP", "x": jwk["x"]})).digest())
    except (SigningError, ValueError, TypeError) as exc:
        raise AuthenticationError("sender_constraint_required") from exc


def _verify_dpop(
    token: str,
    proof: str,
    *,
    method: str,
    uri: str,
    now: float,
    skew: float,
    token_claims: Mapping[str, Any],
    nonce_store: ReplayNonceStore | None,
    deployment: str,
    issuer: str,
    sender_key_hint: str | None = None,
) -> tuple[str, str]:
    phead, pbody, _, header, claims = _jwt_parts(proof)
    if set(header) != {"alg", "typ", "jwk"} or header.get("alg") != "EdDSA" or header.get("typ") != "dpop+jwt" or not isinstance(header.get("jwk"), Mapping):
        raise AuthenticationError("sender_constraint_required")
    jwk = header["jwk"]
    jkt = _jwk_thumbprint(jwk)
    if sender_key_hint is not None and not hmac.compare_digest(sender_key_hint, jkt):
        raise AuthenticationError("sender_constraint_required")
    try:
        raw = b64url_decode(jwk["x"], field="dpop.jwk.x")
        signature = b64url_decode(proof.rsplit(".", 1)[1], field="dpop.signature")
        Ed25519PublicKey.from_public_bytes(raw).verify(signature, (phead + "." + pbody).encode("ascii"))
    except (InvalidSignature, ValueError, TypeError, KeyError, SigningError) as exc:
        raise AuthenticationError("sender_constraint_required") from exc
    if set(claims) != {"htm", "htu", "iat", "jti", "ath"}:
        raise AuthenticationError("sender_constraint_required")
    if claims.get("htm") != method.upper() or claims.get("htu") != _normalize_htu(uri):
        raise AuthenticationError("sender_constraint_required")
    issued = _as_time(claims.get("iat"))
    if issued < now - skew or issued > now + skew:
        raise AuthenticationError("sender_constraint_required")
    jti = claims.get("jti")
    if not isinstance(jti, str) or not re.fullmatch(r"[A-Za-z0-9._~\-]{8,256}", jti):
        raise AuthenticationError("sender_constraint_required")
    try:
        expected_ath = b64url_encode(hashlib.sha256(token.encode("ascii")).digest())
    except UnicodeEncodeError as exc:
        raise AuthenticationError("sender_constraint_required") from exc
    if not isinstance(claims.get("ath"), str) or not hmac.compare_digest(claims["ath"], expected_ath):
        raise AuthenticationError("sender_constraint_required")
    token_jkt = token_claims.get("cnf", {}).get("jkt") if isinstance(token_claims.get("cnf"), Mapping) else None
    if not isinstance(token_jkt, str) or not hmac.compare_digest(token_jkt, jkt):
        raise AuthenticationError("sender_constraint_required")
    expires = datetime.fromtimestamp(_as_time(token_claims["exp"]), tz=UTC) + timedelta(seconds=skew)
    if nonce_store is None:
        raise AuthenticationError("sender_constraint_required")
    ok = nonce_store.insert_nonce(
        deployment=deployment,
        issuer_digest=_safe_digest(issuer),
        sender_key_thumbprint=jkt,
        jti_digest=_safe_digest(jti),
        request_method=method.upper(),
        request_uri_digest=_safe_digest(_normalize_htu(uri)),
        access_token_hash=_safe_digest(token),
        observed_at=datetime.fromtimestamp(now, tz=UTC),
        expires_at=expires,
    )
    if not ok:
        raise AuthenticationError("sender_constraint_required")
    return jkt, _safe_digest(jti)


def _safe_digest(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def authenticate_request(
    *,
    headers: Mapping[str, str] | Iterable[tuple[str | bytes, str | bytes]],
    method: str,
    uri: str,
    deployment: str,
    jwks: Mapping[str, Any] | None = None,
    issuer: str | None = None,
    audience: str = "deployment_scoped_review_api",
    principal_mapping: Mapping[str, Any] | Callable[[str, str], Any] | None = None,
    allowed_algorithms: Sequence[str] = ("EdDSA",),
    max_token_lifetime_seconds: int = 300,
    clock_skew_seconds: int = 30,
    nonce_store: ReplayNonceStore | None = None,
    now: float | None = None,
    mtls_principal: VerifiedPrincipal | Mapping[str, Any] | None = None,
    trusted_proxy_principal: VerifiedPrincipal | Mapping[str, Any] | None = None,
    trusted_proxy: bool = False,
    static_bearer: str | None = None,
    static_principal: VerifiedPrincipal | Mapping[str, Any] | None = None,
    is_loopback: bool = False,
    require_identity_barrier: bool = False,
) -> VerifiedPrincipal:
    """Authenticate one request and require a sender constraint.

    ``principal_mapping`` is the server-resolved mapping from verified issuer
    and subject to stable Nomos identifiers.  Passing none intentionally makes
    JWT authentication fail closed.
    """

    pairs = _header_pairs(headers)
    method = method.upper()
    if not re.fullmatch(r"[A-Z]+", method):
        raise AuthenticationError("authentication_required")
    try:
        clock = time.time() if now is None else float(now)
    except (TypeError, ValueError, OverflowError) as exc:
        raise AuthenticationError("authentication_required") from exc
    if not math.isfinite(clock):
        raise AuthenticationError("authentication_required")

    # Direct mTLS is a complete sender constraint and may have no HTTP auth
    # headers.  The principal must already be certificate-verified by the
    # TLS/proxy integration; this function does not parse certificate bytes.
    if mtls_principal is not None:
        if _one_header(pairs, "authorization") is not None or _one_header(pairs, "dpop") is not None:
            raise AuthenticationError("sender_constraint_required")
        principal = _coerce_principal(mtls_principal, auth_method="mtls")
        if not principal.sender_key_thumbprint:
            raise AuthenticationError("sender_constraint_required")
        _validate_identity_barrier(
            principal,
            certificate_not_before=principal.certificate_not_before,
            certificate_generation_digest=principal.presented_credential_generation_digest,
            certificate_thumbprint=principal.sender_key_thumbprint,
            require_barrier=require_identity_barrier,
        )
        if principal.certificate_not_after is not None and principal.certificate_not_after < datetime.fromtimestamp(clock, tz=UTC):
            raise AuthenticationError("sender_constraint_required")
        _current_principal.set(principal)
        return principal
    if trusted_proxy:
        # A proxy assertion has no transport-bound credential issuance fact
        # that this verifier can compare with the durable provider/binding
        # barriers.  Sensitive lifecycle deployments therefore disable this
        # mode instead of treating a forwarded assertion as equivalent to a
        # freshly verified JWT or client certificate.
        if require_identity_barrier:
            raise AuthenticationError("authentication_required")
        if trusted_proxy_principal is None:
            raise AuthenticationError("authentication_required")
        principal = _coerce_principal(trusted_proxy_principal, auth_method="trusted_proxy")
        if not principal.sender_key_thumbprint:
            raise AuthenticationError("sender_constraint_required")
        # Trusted proxy integrations must supply the assertion out-of-band;
        # reject public identity headers that could be spoofed.
        for name, _ in pairs:
            if name.startswith("x-forwarded-") or name in {"x-okg-principal", "x-principal", "x-auth-user"}:
                raise AuthenticationError("sender_constraint_required")
        _current_principal.set(principal)
        return principal

    authorization = _one_header(pairs, "authorization")
    dpop = _one_header(pairs, "dpop")
    if authorization is None:
        if static_bearer is None or not is_loopback:
            raise AuthenticationError("authentication_required")
        if dpop is not None:
            raise AuthenticationError("sender_constraint_required")
        if static_principal is None:
            raise AuthenticationError("authentication_required")
        raise AuthenticationError("authentication_required")
    if not isinstance(authorization, str):
        raise AuthenticationError("authentication_required")
    scheme, separator, token = authorization.partition(" ")
    if not separator or not token or token != token.strip() or scheme not in {"DPoP", "Bearer"}:
        raise AuthenticationError("authentication_required")
    if scheme == "Bearer" and static_bearer is not None and is_loopback and dpop is None and static_principal is not None:
        if not hmac.compare_digest(token, static_bearer):
            raise AuthenticationError("authentication_required")
        principal = _coerce_principal(static_principal, auth_method="static_bearer")
        _current_principal.set(principal)
        return principal
    if scheme != "DPoP":
        # Bearer is intentionally not a sender-constrained proof for any
        # mutation/full-payload route.  Callers may use this helper for health
        # probes only by opting out at the route layer; default remains closed.
        raise AuthenticationError("sender_constraint_required")
    if dpop is None:
        raise AuthenticationError("sender_constraint_required")
    if jwks is None or issuer is None or principal_mapping is None:
        raise AuthenticationError("authentication_required")
    head, body, sig, header, claims = _jwt_parts(token)
    _verify_jwt_signature((head + "." + body).encode("ascii"), b64url_decode(sig, field="jwt.signature"), header, jwks, allowed_algorithms)
    if set(claims) - {"iss", "aud", "sub", "exp", "iat", "nbf", "cnf"} or claims.get("iss") != issuer or claims.get("aud") != audience:
        raise AuthenticationError("authentication_required")
    if not isinstance(claims.get("sub"), str) or not claims["sub"]:
        raise AuthenticationError("authentication_required")
    issued = _as_time(claims.get("iat"))
    expiry = _as_time(claims.get("exp"))
    if expiry <= issued or expiry - issued > max_token_lifetime_seconds or issued > clock + clock_skew_seconds or expiry < clock - clock_skew_seconds:
        raise AuthenticationError("authentication_required")
    if "nbf" in claims and _as_time(claims["nbf"]) > clock + clock_skew_seconds:
        raise AuthenticationError("authentication_required")
    principal = _principal_mapping(principal_mapping, issuer, claims["sub"])
    # ``iat`` is part of the verified JWT, not deployment mapping metadata.
    # Carry the claim across the principal boundary so custom downstream
    # adapters and audit projections cannot accidentally drop the durable
    # disable/reenable barrier input.
    principal_values = _principal_values(principal)
    principal_values["token_issued_at"] = datetime.fromtimestamp(issued, tz=UTC)
    principal = VerifiedPrincipal(**principal_values)
    _validate_identity_barrier(
        principal,
        issued_at=issued,
        require_barrier=require_identity_barrier,
    )
    jkt, jti_digest = _verify_dpop(
        token,
        dpop,
        method=method,
        uri=uri,
        now=clock,
        skew=float(clock_skew_seconds),
        token_claims=claims,
        nonce_store=nonce_store,
        deployment=deployment,
        issuer=issuer,
    )
    principal_values = _principal_values(principal)
    principal_values.update({
        "auth_method": "jwt_dpop",
        "sender_key_thumbprint": jkt,
        "token_id_digest": _safe_digest(token),
        "dpop_jti_digest": jti_digest,
        "credential_expires_at": datetime.fromtimestamp(expiry, tz=UTC),
    })
    principal = VerifiedPrincipal(**principal_values)
    _current_principal.set(principal)
    return principal


def _coerce_principal(value: VerifiedPrincipal | Mapping[str, Any], *, auth_method: str) -> VerifiedPrincipal:
    if isinstance(value, VerifiedPrincipal):
        values = _principal_values(value)
        values["auth_method"] = auth_method
        return VerifiedPrincipal(**values)
    metadata = value.get("metadata") if isinstance(value, Mapping) else None
    if not isinstance(metadata, Mapping):
        metadata = {}

    def mapped(name: str, *aliases: str) -> Any:
        for candidate in (name, *aliases):
            if candidate in value:
                return value[candidate]
            if candidate in metadata:
                return metadata[candidate]
        return None

    try:
        return VerifiedPrincipal(
            principal=value["principal"], separation_subject=value["separation_subject"], provider_id=value["provider_id"], subject_binding_id=value["subject_binding_id"], issuer=value.get("issuer"), auth_method=auth_method, sender_key_thumbprint=value.get("sender_key_thumbprint"), token_id_digest=value.get("token_id_digest"), credential_expires_at=_coerce_time(value.get("credential_expires_at")),
            provider_status=mapped("provider_status"),
            binding_status=mapped("binding_status"),
            provider_updated_at=_coerce_time(mapped("provider_updated_at")),
            binding_updated_at=_coerce_time(mapped("binding_updated_at")),
            provider_credential_generation_digest=mapped("provider_credential_generation_digest", "provider_generation_digest"),
            binding_credential_generation_digest=mapped("binding_credential_generation_digest", "binding_generation_digest"),
            credential_generation_digest=mapped("credential_generation_digest"),
            presented_credential_generation_digest=mapped("presented_credential_generation_digest", "certificate_generation_digest"),
            token_issued_at=_coerce_time(mapped("token_issued_at", "jwt_issued_at", "credential_issued_at")),
            provider_certificate_thumbprint=mapped("provider_certificate_thumbprint"),
            binding_certificate_thumbprint=mapped("binding_certificate_thumbprint"),
            certificate_thumbprint=mapped("certificate_thumbprint"),
            certificate_not_before=_coerce_time(mapped("certificate_not_before", "cert_not_before")),
            certificate_not_after=_coerce_time(mapped("certificate_not_after", "cert_not_after")),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise AuthenticationError("authentication_required") from exc


def _certificate_time(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


class ASGIMTLSPrincipalAdapter:
    """Resolve a verified TLS client certificate into a Nomos principal.

    TLS path validation and trust-root selection belong to the listener.  This
    adapter consumes only the peer certificate exposed by the ASGI TLS
    extension (or its ``ssl_object``), checks its validity window, derives a
    certificate sender thumbprint, and resolves exactly one SAN through
    deployment-owned mapping.  It never reads HTTP identity headers.
    """

    def __init__(
        self,
        *,
        principal_mapping: Mapping[str, Any] | Callable[[str, str], Any] | None = None,
        certificate_mapping: Mapping[str, Any] | Callable[[str, x509.Certificate], Any] | None = None,
        issuer: str | None = None,
        require_identity_barrier: bool = True,
    ) -> None:
        self.principal_mapping = principal_mapping
        self.certificate_mapping = certificate_mapping
        self.issuer = issuer
        self.require_identity_barrier = require_identity_barrier

    @staticmethod
    def _certificate_from_scope(scope: Mapping[str, Any]) -> x509.Certificate:
        extensions = scope.get("extensions", {})
        if not isinstance(extensions, Mapping):
            extensions = {}
        tls = extensions.get("tls", scope.get("tls"))
        if not isinstance(tls, Mapping):
            tls = {}
        raw: Any = tls.get("client_cert") or tls.get("client_certificate")
        if raw is None:
            chain = tls.get("client_cert_chain") or tls.get("client_certificate_chain")
            if isinstance(chain, (list, tuple)) and chain:
                raw = chain[0]
        if raw is None:
            ssl_object = scope.get("ssl_object")
            getter = getattr(ssl_object, "getpeercert", None)
            if callable(getter):
                try:
                    raw = getter(binary_form=True)
                except (TypeError, ValueError, OSError):
                    raw = None
        # The adapter is not a replacement for TLS path validation.  Accept
        # an explicit verified marker from a trusted ASGI TLS extension, or a
        # live SSL object configured with CERT_REQUIRED.  A bare certificate
        # blob from an untrusted proxy is never an identity assertion.
        ssl_object = scope.get("ssl_object")
        verified = tls.get("verified", scope.get("tls_verified"))
        if verified is not True:
            verify_mode = getattr(ssl_object, "verify_mode", None)
            if verify_mode != ssl.CERT_REQUIRED:
                raise AuthenticationError("sender_constraint_required")
        if not isinstance(raw, bytes) or not raw:
            raise AuthenticationError("sender_constraint_required")
        try:
            return x509.load_der_x509_certificate(raw)
        except (ValueError, TypeError) as exc:
            raise AuthenticationError("sender_constraint_required") from exc

    @staticmethod
    def _single_san(certificate: x509.Certificate) -> str:
        try:
            extension = certificate.extensions.get_extension_for_class(x509.SubjectAlternativeName)
        except x509.ExtensionNotFound as exc:
            raise AuthenticationError("sender_constraint_required") from exc
        values = [
            str(name.value)
            for name in extension.value
            if isinstance(name, (x509.UniformResourceIdentifier, x509.DNSName))
        ]
        if len(values) != 1 or not values[0] or any(
            ord(ch) <= 0x20 or ord(ch) > 0x7e for ch in values[0]
        ):
            raise AuthenticationError("sender_constraint_required")
        return values[0]

    def _lookup(self, san: str, certificate: x509.Certificate) -> Any:
        mapping = self.certificate_mapping
        if mapping is not None:
            try:
                return mapping(san, certificate) if callable(mapping) else mapping.get(
                    f"san:{san}", mapping.get(san)
                )
            except Exception as exc:
                raise AuthenticationError("authentication_required") from exc
        mapping = self.principal_mapping
        if mapping is None:
            return None
        # A mTLS-only deployment may not declare an OIDC issuer.  In that
        # case the certificate issuer's canonical RFC4514 form is the
        # issuer-scoped mapping key; an explicit deployment issuer still
        # wins when provider rows use a stable URI.
        issuer = self.issuer or certificate.issuer.rfc4514_string()
        try:
            if callable(mapping):
                return mapping(issuer, san)
            return mapping.get(
                f"{issuer}\x00{san}", mapping.get(f"san:{san}", mapping.get(san))
            )
        except Exception as exc:
            raise AuthenticationError("authentication_required") from exc

    def __call__(self, scope: Mapping[str, Any], *, now: float | None = None) -> VerifiedPrincipal:
        certificate = self._certificate_from_scope(scope)
        san = self._single_san(certificate)
        mapped = self._lookup(san, certificate)
        if not isinstance(mapped, (Mapping, VerifiedPrincipal)):
            raise AuthenticationError("authentication_required")
        not_before_value = (
            certificate.not_valid_before_utc
            if hasattr(certificate, "not_valid_before_utc")
            else certificate.not_valid_before
        )
        not_after_value = (
            certificate.not_valid_after_utc
            if hasattr(certificate, "not_valid_after_utc")
            else certificate.not_valid_after
        )
        not_before = _certificate_time(not_before_value)
        not_after = _certificate_time(not_after_value)
        try:
            clock = time.time() if now is None else float(now)
        except (TypeError, ValueError, OverflowError) as exc:
            raise AuthenticationError("authentication_required") from exc
        observed = datetime.fromtimestamp(clock, tz=UTC)
        if observed < not_before or observed > not_after:
            raise AuthenticationError("sender_constraint_required")
        thumbprint = b64url_encode(
            hashlib.sha256(
                certificate.public_key().public_bytes(
                    serialization.Encoding.DER,
                    serialization.PublicFormat.SubjectPublicKeyInfo,
                )
            ).digest()
        )
        extensions = scope.get("extensions", {})
        tls = extensions.get("tls") if isinstance(extensions, Mapping) else None
        if not isinstance(tls, Mapping):
            tls = scope.get("tls") if isinstance(scope.get("tls"), Mapping) else {}
        presented_generation = tls.get(
            "presented_credential_generation_digest",
            tls.get("credential_generation_digest", tls.get("certificate_generation_digest")),
        )
        if isinstance(mapped, VerifiedPrincipal):
            values = _principal_values(mapped)
            if values.get("sender_key_thumbprint") is not None and values["sender_key_thumbprint"] != thumbprint:
                raise AuthenticationError("sender_constraint_required")
            values.update({
                "auth_method": "mtls",
                "sender_key_thumbprint": values.get("sender_key_thumbprint") or thumbprint,
                "token_id_digest": values.get("token_id_digest") or _safe_digest(str(certificate.serial_number)),
                "certificate_not_before": not_before,
                "certificate_not_after": not_after,
            })
            if presented_generation is not None:
                values["presented_credential_generation_digest"] = presented_generation
            principal = VerifiedPrincipal(**values)
            generation = principal.presented_credential_generation_digest
        else:
            values = dict(mapped)
            if values.get("sender_key_thumbprint") is not None and values["sender_key_thumbprint"] != thumbprint:
                raise AuthenticationError("sender_constraint_required")
            values.update({
                "auth_method": "mtls",
                "sender_key_thumbprint": values.get("sender_key_thumbprint") or thumbprint,
                "token_id_digest": values.get("token_id_digest") or _safe_digest(str(certificate.serial_number)),
                "certificate_not_before": not_before,
                "certificate_not_after": not_after,
            })
            if presented_generation is not None:
                values["presented_credential_generation_digest"] = presented_generation
            principal = _coerce_principal(values, auth_method="mtls")
            generation = values.get(
                "presented_credential_generation_digest",
                values.get("certificate_generation_digest"),
            )
        _validate_identity_barrier(
            principal,
            certificate_not_before=not_before,
            certificate_generation_digest=generation,
            certificate_thumbprint=thumbprint,
            require_barrier=self.require_identity_barrier,
        )
        return principal


def mtls_principal_from_asgi_scope(
    scope: Mapping[str, Any],
    *,
    principal_mapping: Mapping[str, Any] | Callable[[str, str], Any] | None = None,
    certificate_mapping: Mapping[str, Any] | Callable[[str, x509.Certificate], Any] | None = None,
    issuer: str | None = None,
    now: float | None = None,
    require_identity_barrier: bool = True,
) -> VerifiedPrincipal:
    """Concrete certificate-only ASGI authentication adapter."""

    return ASGIMTLSPrincipalAdapter(
        principal_mapping=principal_mapping,
        certificate_mapping=certificate_mapping,
        issuer=issuer,
        require_identity_barrier=require_identity_barrier,
    )(scope, now=now)


def validate_verified_principal(
    principal: VerifiedPrincipal,
    *,
    require_identity_barrier: bool = False,
    now: float | None = None,
) -> VerifiedPrincipal:
    """Validate a verifier-produced principal before API dispatch.

    This is for integrations that provide their own certificate/JWT parser.
    It does not elevate body or header fields into identity; it only applies
    the same current provider/binding barrier used by this module's verifier.
    """

    if not isinstance(principal, VerifiedPrincipal):
        raise AuthenticationError("authentication_required")
    certificate_time = principal.certificate_not_before
    if principal.auth_method == "mtls":
        _validate_identity_barrier(
            principal,
            certificate_not_before=certificate_time,
            certificate_generation_digest=principal.presented_credential_generation_digest,
            certificate_thumbprint=principal.sender_key_thumbprint,
            require_barrier=require_identity_barrier,
        )
        if principal.certificate_not_after is not None:
            observed = datetime.fromtimestamp(time.time() if now is None else now, tz=UTC)
            if observed > principal.certificate_not_after:
                raise AuthenticationError("sender_constraint_required")
    elif principal.auth_method in {"jwt", "jwt_dpop"}:
        # Integrations that own JWT parsing must carry the verified ``iat``
        # across this boundary.  Without it, a token issued before a durable
        # disable/reenable barrier could be replayed through the custom
        # authenticator path.
        if principal.token_issued_at is None:
            if require_identity_barrier:
                raise AuthenticationError("authentication_required")
        else:
            _validate_identity_barrier(
                principal,
                issued_at=principal.token_issued_at.timestamp(),
                require_barrier=require_identity_barrier,
            )
    return principal


def current_principal() -> VerifiedPrincipal:
    principal = _current_principal.get()
    if principal is None:
        raise AuthenticationError("authentication_required")
    return principal


def clear_principal() -> None:
    _current_principal.set(None)


class MemoryReplayNonceStore:
    """Test-only process-local nonce fence; production must use PostgreSQL."""

    def __init__(self) -> None:
        self._keys: set[tuple[str, str, str, str]] = set()

    def insert_nonce(self, **kwargs: Any) -> bool:
        key = (kwargs["deployment"], kwargs["issuer_digest"], kwargs["sender_key_thumbprint"], kwargs["jti_digest"])
        if key in self._keys:
            return False
        self._keys.add(key)
        return True


def build_auth_audit(principal: VerifiedPrincipal) -> AuthAudit:
    issuer_digest = _safe_digest(principal.issuer or principal.provider_id)
    return AuthAudit(
        principal=principal.principal,
        separation_subject=principal.separation_subject,
        provider_id=principal.provider_id,
        subject_binding_id=principal.subject_binding_id,
        authentication_method_code=principal.auth_method,
        issuer_or_trust_domain_digest=issuer_digest,
        sender_key_or_certificate_thumbprint=principal.sender_key_thumbprint,
        token_or_certificate_id_digest=principal.token_id_digest,
        dpop_jti_digest=principal.dpop_jti_digest,
        credential_expires_at=principal.credential_expires_at,
        provider_status=principal.provider_status,
        binding_status=principal.binding_status,
        provider_updated_at=principal.provider_updated_at,
        binding_updated_at=principal.binding_updated_at,
        provider_credential_generation_digest=principal.provider_credential_generation_digest,
        binding_credential_generation_digest=principal.binding_credential_generation_digest,
        credential_generation_digest=principal.credential_generation_digest,
        presented_credential_generation_digest=principal.presented_credential_generation_digest,
        token_issued_at=principal.token_issued_at,
        certificate_thumbprint=principal.certificate_thumbprint or principal.sender_key_thumbprint,
        certificate_not_before=principal.certificate_not_before,
        certificate_not_after=principal.certificate_not_after,
    )


__all__ = [
    "AuthenticationError",
    "ReplayNonceStore",
    "VerifiedPrincipal",
    "SenderConstraint",
    "AuthAudit",
    "MemoryReplayNonceStore",
    "ASGIMTLSPrincipalAdapter",
    "authenticate_request",
    "mtls_principal_from_asgi_scope",
    "validate_verified_principal",
    "normalized_request_uri",
    "current_principal",
    "clear_principal",
    "build_auth_audit",
]
