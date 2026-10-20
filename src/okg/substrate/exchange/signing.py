"""Ed25519 signing helpers for Poros envelopes and receipts."""

from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Any

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

from okg.substrate.exchange.base64url import b64url_decode, b64url_encode
from okg.substrate.exchange.canonical_json import canonical_json_bytes
from okg.substrate.exchange.constants import DOMAIN_PREFIX
from okg.substrate.exchange.errors import PorosEncodingError, PorosSignatureError
from okg.substrate.exchange.hashing import canonical_json_sha256_b64url


@dataclass(frozen=True)
class Ed25519KeyPair:
    kid: str
    private_key: str
    public_key: str


def generate_ed25519_keypair(kid: str) -> Ed25519KeyPair:
    private = Ed25519PrivateKey.generate()
    private_raw = private.private_bytes(
        Encoding.Raw,
        PrivateFormat.Raw,
        NoEncryption(),
    )
    public_raw = private.public_key().public_bytes(
        Encoding.Raw,
        PublicFormat.Raw,
    )
    return Ed25519KeyPair(
        kid=kid,
        private_key=b64url_encode(private_raw),
        public_key=b64url_encode(public_raw),
    )


def derive_ed25519_public_key(private_key: bytes | str) -> str:
    private = _private_key(private_key)
    public_raw = private.public_key().public_bytes(
        Encoding.Raw,
        PublicFormat.Raw,
    )
    return b64url_encode(public_raw)


def sign_envelope(
    envelope: dict[str, Any],
    private_key: bytes | str,
    *,
    kid: str,
) -> dict[str, Any]:
    """Return a signed copy of an envelope."""
    signed = copy.deepcopy(envelope)
    if "body" in signed:
        signed["body_sha256"] = canonical_json_sha256_b64url(signed["body"])
    return sign_document(signed, private_key, kid=kid)


def sign_receipt(
    receipt: dict[str, Any],
    private_key: bytes | str,
    *,
    kid: str,
) -> dict[str, Any]:
    """Return a signed copy of a receipt document."""
    return sign_document(receipt, private_key, kid=kid)


def verify_receipt(receipt: dict[str, Any], public_key: bytes | str) -> None:
    verify_signed_document(receipt, public_key)


def sign_document(
    document: dict[str, Any],
    private_key: bytes | str,
    *,
    kid: str,
) -> dict[str, Any]:
    signed = copy.deepcopy(document)
    signed.pop("signature", None)
    signed["signing_key_id"] = kid
    signature = _private_key(private_key).sign(signature_input(signed))
    signed["signature"] = {
        "kid": kid,
        "sig": b64url_encode(signature),
    }
    return signed


def verify_signed_document(
    document: dict[str, Any],
    public_key: bytes | str,
) -> None:
    signature_kid(document)
    signature = document.get("signature")
    if not isinstance(signature, dict):
        raise PorosSignatureError(
            "Poros document has no signature object",
            reason_code="missing_signature",
        )
    raw_sig = b64url_decode(str(signature.get("sig", "")), field="signature.sig")
    if len(raw_sig) != 64:
        raise PorosEncodingError(
            "Ed25519 signature must be 64 bytes",
            reason_code="invalid_signature_encoding",
            data={"field": "signature.sig"},
        )
    unsigned = copy.deepcopy(document)
    unsigned.pop("signature", None)
    try:
        _public_key(public_key).verify(raw_sig, signature_input(unsigned))
    except InvalidSignature as exc:
        raise PorosSignatureError(
            "Poros Ed25519 signature verification failed",
            reason_code="invalid_signature",
        ) from exc


def signature_kid(document: dict[str, Any]) -> str:
    top_level_kid = document.get("signing_key_id")
    if not isinstance(top_level_kid, str) or not top_level_kid:
        raise PorosSignatureError(
            "Poros document is missing signed signing_key_id",
            reason_code="missing_signature_kid",
        )
    signature = document.get("signature")
    if not isinstance(signature, dict) or not isinstance(signature.get("kid"), str):
        raise PorosSignatureError(
            "Poros document signature is missing kid",
            reason_code="missing_signature_kid",
        )
    if signature["kid"] != top_level_kid:
        raise PorosSignatureError(
            "Poros signature kid does not match signed signing_key_id",
            reason_code="signature_kid_mismatch",
        )
    return top_level_kid


def signature_input(unsigned_document: dict[str, Any]) -> bytes:
    return DOMAIN_PREFIX + canonical_json_bytes(unsigned_document)


def _private_key(value: bytes | str) -> Ed25519PrivateKey:
    raw = b64url_decode(value, field="private_key") if isinstance(value, str) else value
    if len(raw) != 32:
        raise PorosEncodingError(
            "Ed25519 private key must be 32 raw bytes",
            reason_code="invalid_key_encoding",
        )
    return Ed25519PrivateKey.from_private_bytes(raw)


def _public_key(value: bytes | str) -> Ed25519PublicKey:
    raw = b64url_decode(value, field="public_key") if isinstance(value, str) else value
    if len(raw) != 32:
        raise PorosEncodingError(
            "Ed25519 public key must be 32 raw bytes",
            reason_code="invalid_key_encoding",
        )
    return Ed25519PublicKey.from_public_bytes(raw)
