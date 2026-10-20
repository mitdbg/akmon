"""Closed, domain-separated hashes and detached JWS helpers.

This module deliberately has no provider or database dependency.  It is used
by the review API and by the startup restore gate, and therefore treats a
malformed envelope as a hard failure rather than attempting to repair it.
"""

from __future__ import annotations

import base64
import hashlib
import json
import math
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Mapping

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    PublicFormat,
)


class SigningError(ValueError):
    """A closed signed-envelope or canonicalization error."""

    code = "invalid_signed_envelope"


_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_B64_RE = re.compile(r"^[A-Za-z0-9_-]*$")
_KID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/+\-]{0,255}$")
_TIMESTAMP_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$"
)
MAX_SAFE_INTEGER = 9_007_199_254_740_991


def _json_string(value: str) -> str:
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise SigningError("JSON string is not valid UTF-8") from exc
    if any(ord(ch) < 0x20 for ch in value):
        # json.dumps safely escapes these, but the explicit check keeps this
        # routine's I-JSON boundary obvious and rejects lone surrogates.
        pass
    try:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise SigningError("JSON string cannot be canonicalized") from exc


def _number(value: int | float) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        if abs(value) > MAX_SAFE_INTEGER:
            raise SigningError("integer exceeds the I-JSON safe range")
        return str(value)
    if not isinstance(value, float) or not math.isfinite(value):
        raise SigningError("JSON numbers must be finite")
    # Python's shortest-round-trip spelling is close to ECMAScript's.  The
    # normalization below covers the meaningful JCS differences: exponent
    # spelling and the fixed/exponential cutovers.  Deployment payloads are
    # expected to use integers; this nevertheless keeps ordinary finite
    # decimal values interoperable.
    if value == 0.0:
        return "0"
    raw = repr(value).lower()
    mantissa, exponent = (raw.split("e", 1) + [None])[:2] if "e" in raw else (raw, None)
    exp = int(exponent) if exponent is not None else 0
    sign = ""
    if mantissa.startswith("-"):
        sign, mantissa = "-", mantissa[1:]
    if "." in mantissa:
        whole, frac = mantissa.split(".", 1)
        digits = whole + frac
        point = len(whole) + exp
    else:
        digits, point = mantissa, len(mantissa) + exp
    digits = digits.rstrip("0") or "0"
    # ECMAScript uses decimal notation for [1e-6, 1e21), and exponential
    # notation outside it.
    magnitude = abs(value)
    if 1e-6 <= magnitude < 1e21:
        if point <= 0:
            return sign + "0." + "0" * (-point) + digits
        if point >= len(digits):
            return sign + digits + "0" * (point - len(digits))
        return sign + digits[:point] + "." + digits[point:]
    first = digits[0]
    tail = digits[1:].rstrip("0")
    mant = first + (("." + tail) if tail else "")
    # The exponent is the position of the first significant digit relative to
    # the original decimal point.
    js_exp = point - 1
    return sign + mant + "e" + ("+" if js_exp >= 0 else "") + str(js_exp)


def canonical_json_bytes(value: Any) -> bytes:
    """Serialize I-JSON using the RFC-8785 object/key and scalar rules."""

    def render(item: Any) -> str:
        if item is None:
            return "null"
        if item is True:
            return "true"
        if item is False:
            return "false"
        if isinstance(item, (int, float)) and not isinstance(item, bool):
            return _number(item)
        if isinstance(item, str):
            return _json_string(item)
        if isinstance(item, Mapping):
            pairs: list[tuple[str, Any]] = []
            for key, child in item.items():
                if not isinstance(key, str):
                    raise SigningError("JSON object keys must be strings")
                pairs.append((key, child))
            pairs.sort(key=lambda pair: pair[0].encode("utf-16-be"))
            if len({key for key, _ in pairs}) != len(pairs):
                raise SigningError("duplicate JSON object key")
            return "{" + ",".join(_json_string(k) + ":" + render(v) for k, v in pairs) + "}"
        if isinstance(item, (list, tuple)):
            return "[" + ",".join(render(child) for child in item) + "]"
        raise SigningError("value is not representable as I-JSON")

    try:
        return render(value).encode("utf-8")
    except UnicodeEncodeError as exc:
        raise SigningError("value is not valid UTF-8") from exc


def strict_json_loads(raw: bytes | str) -> Any:
    """Parse JSON with duplicate-key, finite-number, and I-JSON checks."""

    if isinstance(raw, str):
        raw_bytes = raw.encode("utf-8")
    elif isinstance(raw, bytes):
        raw_bytes = raw
    else:
        raise SigningError("JSON input must be bytes or text")
    try:
        text = raw_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise SigningError("JSON input is not UTF-8") from exc

    def pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in values:
            if key in result:
                raise SigningError("duplicate JSON object key")
            result[key] = value
        return result

    def parse_float(value: str) -> float:
        result = float(value)
        if not math.isfinite(result):
            raise SigningError("JSON number is not finite")
        return result

    def parse_int(value: str) -> int:
        result = int(value)
        if abs(result) > MAX_SAFE_INTEGER:
            raise SigningError("integer exceeds the I-JSON safe range")
        if value == "-0":
            raise SigningError("negative zero is not canonical I-JSON")
        return result

    try:
        value = json.loads(
            text,
            object_pairs_hook=pairs,
            parse_float=parse_float,
            parse_int=parse_int,
            parse_constant=lambda value: (_ for _ in ()).throw(
                SigningError("non-finite JSON number")
            ),
        )
    except SigningError:
        raise
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise SigningError("malformed JSON") from exc
    # Re-rendering traverses every nested value and catches lone surrogates or
    # unsupported Python values.
    canonical_json_bytes(value)
    return value


def domain_digest(domain: str, value: Any) -> str:
    """Return ``sha256:<hex>`` for the universal contract construction."""

    if not isinstance(domain, str) or not domain or "\x00" in domain:
        raise SigningError("hash domain must be non-empty and contain no NUL")
    material = domain.encode("utf-8") + b"\x00" + canonical_json_bytes(value)
    return "sha256:" + hashlib.sha256(material).hexdigest()


def digest_bytes(value: str) -> bytes:
    if not isinstance(value, str) or not _DIGEST_RE.fullmatch(value):
        raise SigningError("expected a lowercase sha256 digest")
    return bytes.fromhex(value[7:])


def validate_digest(value: str) -> str:
    if not isinstance(value, str) or not _DIGEST_RE.fullmatch(value):
        raise SigningError("expected a lowercase sha256 digest")
    return value


def b64url_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def b64url_decode(value: str, *, field: str = "value") -> bytes:
    if not isinstance(value, str) or not _B64_RE.fullmatch(value) or len(value) % 4 == 1:
        raise SigningError(f"{field} is not unpadded base64url")
    try:
        decoded = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
    except (ValueError, TypeError) as exc:
        raise SigningError(f"{field} is not valid base64url") from exc
    # Python's decoder accepts non-zero unused trailing bits.  Reject those
    # aliases so signed material has one representation at this boundary.
    if b64url_encode(decoded) != value:
        raise SigningError(f"{field} is not canonical base64url")
    return decoded


def format_timestamp(value: datetime) -> str:
    if value.tzinfo is None:
        raise SigningError("timestamp must be timezone-aware")
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def parse_timestamp(value: str) -> datetime:
    if not isinstance(value, str) or not _TIMESTAMP_RE.fullmatch(value):
        raise SigningError("timestamp must use six fractional digits and Z")
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)
    except ValueError as exc:
        raise SigningError("timestamp is not valid UTC") from exc


@dataclass(frozen=True)
class SigningKey:
    kid: str
    purpose: str
    algorithm: str
    public_key_b64: str
    state: str = "current"
    not_before: str | None = None
    not_after: str | None = None
    compromise_revoked_at: str | None = None

    def __post_init__(self) -> None:
        if not _KID_RE.fullmatch(self.kid):
            raise SigningError("invalid signing key id")
        if self.algorithm != "EdDSA":
            raise SigningError("only deployment-pinned EdDSA is supported")
        if self.state not in {"current", "retired_for_signing_historically_valid", "revoked_compromised"}:
            raise SigningError("invalid signing key state")
        if len(b64url_decode(self.public_key_b64, field="public_key_b64")) != 32:
            raise SigningError("EdDSA public key must be 32 bytes")


def _private_key(value: bytes | str) -> Ed25519PrivateKey:
    raw = b64url_decode(value, field="private_key") if isinstance(value, str) else value
    if not isinstance(raw, bytes) or len(raw) != 32:
        raise SigningError("EdDSA private key must be 32 raw bytes")
    return Ed25519PrivateKey.from_private_bytes(raw)


def _public_key(value: bytes | str) -> Ed25519PublicKey:
    raw = b64url_decode(value, field="public_key") if isinstance(value, str) else value
    if not isinstance(raw, bytes) or len(raw) != 32:
        raise SigningError("EdDSA public key must be 32 raw bytes")
    return Ed25519PublicKey.from_public_bytes(raw)


def generate_keypair() -> tuple[str, str]:
    private = Ed25519PrivateKey.generate()
    return (
        b64url_encode(private.private_bytes(Encoding.Raw, PrivateFormat.Raw, NoEncryption())),
        b64url_encode(private.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)),
    )


def signed_input(protected_b64: str, payload_b64: str) -> bytes:
    if not isinstance(protected_b64, str) or not isinstance(payload_b64, str):
        raise SigningError("JWS components must be text")
    # Detached compact JWS signing input is ASCII and has no padding.
    b64url_decode(protected_b64, field="protected_b64")
    b64url_decode(payload_b64, field="payload_b64")
    return protected_b64.encode("ascii") + b"." + payload_b64.encode("ascii")


def build_detached_envelope(
    payload: Mapping[str, Any],
    private_key: bytes | str,
    *,
    kid: str,
    typ: str,
    domain: str,
) -> dict[str, str]:
    """Construct the exact protected/payload/signature envelope."""

    if not _KID_RE.fullmatch(kid):
        raise SigningError("invalid signing key id")
    if not isinstance(typ, str) or not typ or any(ord(c) < 0x21 or ord(c) > 0x7e for c in typ):
        raise SigningError("invalid JWS typ")
    protected = {"alg": "EdDSA", "kid": kid, "typ": typ}
    protected_b64 = b64url_encode(canonical_json_bytes(protected))
    payload_b64 = b64url_encode(canonical_json_bytes(dict(payload)))
    signature_b64 = b64url_encode(_private_key(private_key).sign(signed_input(protected_b64, payload_b64)))
    unsigned = {
        "protected_b64": protected_b64,
        "payload_b64": payload_b64,
        "signature_b64": signature_b64,
    }
    return {**unsigned, "envelope_digest": domain_digest(domain, unsigned)}


def verify_detached_envelope(
    envelope: Mapping[str, Any],
    public_key: bytes | str,
    *,
    expected_kid: str | None = None,
    expected_typ: str | None = None,
    expected_domain: str | None = None,
    expected_fields: tuple[str, ...] | None = None,
) -> dict[str, Any]:
    """Verify a detached envelope and return its closed payload."""

    if not isinstance(envelope, Mapping):
        raise SigningError("signed envelope must be an object")
    keys = set(envelope)
    allowed = {"protected_b64", "payload_b64", "signature_b64", "envelope_digest"}
    if keys != allowed:
        raise SigningError("signed envelope has missing or extra fields")
    protected_b64 = envelope["protected_b64"]
    payload_b64 = envelope["payload_b64"]
    signature_b64 = envelope["signature_b64"]
    digest = envelope["envelope_digest"]
    if not all(isinstance(v, str) for v in (protected_b64, payload_b64, signature_b64, digest)):
        raise SigningError("signed envelope fields must be strings")
    protected = strict_json_loads(b64url_decode(protected_b64, field="protected_b64"))
    payload = strict_json_loads(b64url_decode(payload_b64, field="payload_b64"))
    # Signatures cover the encoded JWS components, but the protocol also
    # fixes their JSON representation to RFC 8785/JCS.  Parsing alone would
    # accept a valid signature over alternate whitespace/key ordering and
    # make semantically identical envelopes have different wire identities.
    # Reject that ambiguity before checking the signature or envelope digest.
    if b64url_encode(canonical_json_bytes(protected)) != protected_b64:
        raise SigningError("JWS protected header is not canonical JCS")
    if b64url_encode(canonical_json_bytes(payload)) != payload_b64:
        raise SigningError("JWS payload is not canonical JCS")
    if not isinstance(protected, Mapping) or set(protected) != {"alg", "kid", "typ"}:
        raise SigningError("JWS protected header is not closed")
    if protected["alg"] != "EdDSA" or not isinstance(protected["kid"], str) or not isinstance(protected["typ"], str):
        raise SigningError("unsupported or malformed JWS protected header")
    if expected_kid is not None and protected["kid"] != expected_kid:
        raise SigningError("unexpected signing key")
    if expected_typ is not None and protected["typ"] != expected_typ:
        raise SigningError("unexpected signed envelope type")
    if expected_fields is not None and (not isinstance(payload, Mapping) or set(payload) != set(expected_fields)):
        raise SigningError("signed payload has missing or extra fields")
    unsigned = {"protected_b64": protected_b64, "payload_b64": payload_b64, "signature_b64": signature_b64}
    if expected_domain is not None and domain_digest(expected_domain, unsigned) != digest:
        raise SigningError("signed envelope digest mismatch")
    try:
        _public_key(public_key).verify(
            b64url_decode(signature_b64, field="signature_b64"),
            signed_input(protected_b64, payload_b64),
        )
    except (InvalidSignature, ValueError) as exc:
        raise SigningError("signed envelope signature is invalid") from exc
    return dict(payload) if isinstance(payload, Mapping) else {"value": payload}


def sign_detached(*args: Any, **kwargs: Any) -> dict[str, str]:
    """Compatibility alias for :func:`build_detached_envelope`."""

    return build_detached_envelope(*args, **kwargs)


def verify_signed_envelope(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return verify_detached_envelope(*args, **kwargs)


def digest_value(domain: str, value: Any) -> str:
    return domain_digest(domain, value)


def hash_value(domain: str, value: Any) -> str:
    return domain_digest(domain, value)


def sign_envelope(
    payload: Mapping[str, Any],
    private_key: bytes | str,
    *,
    kid: str,
    typ: str = "application/okg-external-action+jws",
    domain: str = "okg-external-action-signed-envelope-v1",
) -> dict[str, str]:
    return build_detached_envelope(payload, private_key, kid=kid, typ=typ, domain=domain)


def verify_envelope(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return verify_detached_envelope(*args, **kwargs)


__all__ = [
    "SigningError",
    "SigningKey",
    "MAX_SAFE_INTEGER",
    "canonical_json_bytes",
    "strict_json_loads",
    "domain_digest",
    "digest_value",
    "hash_value",
    "digest_bytes",
    "validate_digest",
    "b64url_encode",
    "b64url_decode",
    "format_timestamp",
    "parse_timestamp",
    "generate_keypair",
    "build_detached_envelope",
    "verify_detached_envelope",
    "sign_detached",
    "verify_signed_envelope",
    "sign_envelope",
    "verify_envelope",
]
