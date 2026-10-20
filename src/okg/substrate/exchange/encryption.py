"""HPKE helpers for private Poros envelopes and detached objects."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from cryptography.hazmat.primitives import hpke
from cryptography.hazmat.primitives.asymmetric import x25519
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    PublicFormat,
)

from okg.substrate.exchange.base64url import b64url_decode, b64url_encode
from okg.substrate.exchange.canonical_json import (
    canonical_json_bytes,
    loads_strict_json,
)
from okg.substrate.exchange.constants import POROS_VERSION
from okg.substrate.exchange.errors import PorosEncodingError, PorosPayloadError
from okg.substrate.exchange.hashing import sha256_b64url


HPKE_ALG = "HPKE-v1-X25519-HKDF-SHA256-ChaCha20Poly1305"


@dataclass(frozen=True)
class X25519KeyPair:
    kid: str
    private_key: str
    public_key: str


def generate_x25519_keypair(kid: str) -> X25519KeyPair:
    private = x25519.X25519PrivateKey.generate()
    private_raw = private.private_bytes(
        Encoding.Raw,
        PrivateFormat.Raw,
        NoEncryption(),
    )
    public_raw = private.public_key().public_bytes(
        Encoding.Raw,
        PublicFormat.Raw,
    )
    return X25519KeyPair(
        kid=kid,
        private_key=b64url_encode(private_raw),
        public_key=b64url_encode(public_raw),
    )


def derive_x25519_public_key(private_key: bytes | str) -> str:
    public_raw = _private_key(private_key).public_key().public_bytes(
        Encoding.Raw,
        PublicFormat.Raw,
    )
    return b64url_encode(public_raw)


def encrypt_private_envelope(
    inner_envelope: Mapping[str, Any],
    recipient_public_key: bytes | str,
    *,
    info: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Encrypt a signed inner Poros envelope for one receiver.

    The returned object is intended to be used as the outer
    `encrypted.graph.submit` envelope body. The outer envelope is still
    signed normally, so receivers verify both the transport envelope and
    the decrypted inner envelope.
    """
    hpke_info = _hpke_info("private_envelope", info)
    plaintext = canonical_json_bytes(dict(inner_envelope))
    ciphertext = _suite().encrypt(
        plaintext,
        _public_key(recipient_public_key),
        hpke_info,
    )
    return {
        "kind": "poros.encrypted_envelope",
        "alg": HPKE_ALG,
        "info": dict(info or {}),
        "ciphertext_b64url": b64url_encode(ciphertext),
        "ciphertext_sha256": sha256_b64url(ciphertext),
    }


def decrypt_private_envelope(
    encrypted_body: Mapping[str, Any],
    recipient_private_key: bytes | str,
    *,
    info: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if encrypted_body.get("alg") != HPKE_ALG:
        raise PorosPayloadError(
            "unsupported Poros private-envelope encryption algorithm",
            reason_code="unsupported_encryption_algorithm",
            data={"alg": encrypted_body.get("alg")},
        )
    stored_info = encrypted_body.get("info") or {}
    if not isinstance(stored_info, Mapping):
        raise PorosPayloadError(
            "encrypted Poros envelope info must be an object",
            reason_code="invalid_encrypted_envelope",
        )
    if info is not None and dict(info) != dict(stored_info):
        raise PorosPayloadError(
            "encrypted Poros envelope associated data mismatch",
            reason_code="hpke_associated_data_mismatch",
        )
    ciphertext = _ciphertext_bytes(encrypted_body)
    try:
        plaintext = _suite().decrypt(
            ciphertext,
            _private_key(recipient_private_key),
            _hpke_info("private_envelope", stored_info),
        )
    except Exception as exc:  # noqa: BLE001 - backend-specific auth failure
        raise PorosPayloadError(
            "Poros private-envelope HPKE decryption failed",
            reason_code="hpke_decryption_failed",
        ) from exc
    parsed = loads_strict_json(plaintext)
    if not isinstance(parsed, dict):
        raise PorosPayloadError(
            "decrypted Poros private envelope must be an object",
            reason_code="invalid_encrypted_envelope",
        )
    return parsed


def encrypt_detached_object(
    *,
    object_id: str,
    media_type: str,
    data: bytes,
    chunk_size_bytes: int,
    recipient_authority_id: str,
    recipient_public_key: bytes | str,
    classification: str = "restricted",
    locations: list[dict[str, Any]] | None = None,
) -> tuple[dict[str, Any], bytes]:
    if chunk_size_bytes <= 0:
        raise PorosPayloadError(
            "chunk size must be positive",
            reason_code="invalid_detached_object_manifest",
        )
    ciphertext_chunks: list[bytes] = []
    manifest_chunks: list[dict[str, Any]] = []
    offset = 0
    cleartext_sha = sha256_b64url(data)
    for index, cleartext in enumerate(_chunks(data, chunk_size_bytes)):
        info = _object_chunk_info(
            object_id=object_id,
            index=index,
            cleartext_sha256=cleartext_sha,
        )
        ciphertext = _suite().encrypt(
            cleartext,
            _public_key(recipient_public_key),
            _hpke_info("detached_object_chunk", info),
        )
        ciphertext_chunks.append(ciphertext)
        manifest_chunks.append({
            "index": index,
            "offset_bytes": offset,
            "size_bytes": len(ciphertext),
            "ciphertext_sha256": sha256_b64url(ciphertext),
            "hpke_info": info,
        })
        offset += len(ciphertext)
    ciphertext_blob = b"".join(ciphertext_chunks)
    manifest = {
        "object_id": object_id,
        "media_type": media_type,
        "size_bytes": len(ciphertext_blob),
        "cleartext_size_bytes": len(data),
        "chunk_size_bytes": max((len(chunk) for chunk in ciphertext_chunks), default=1),
        "ciphertext_sha256": sha256_b64url(ciphertext_blob),
        "classification": classification,
        "encryption": {
            "alg": HPKE_ALG,
            "recipient_authority_id": recipient_authority_id,
            "cleartext_sha256": cleartext_sha,
        },
        "chunks": manifest_chunks,
        "locations": list(locations or []),
    }
    return manifest, ciphertext_blob


def decrypt_detached_object(
    manifest: Mapping[str, Any],
    ciphertext: bytes,
    recipient_private_key: bytes | str,
    *,
    receiver_authority_id: str | None = None,
) -> bytes:
    from okg.substrate.exchange.payloads import validate_detached_object_bytes

    validate_detached_object_bytes(dict(manifest), ciphertext)
    metadata = manifest.get("encryption")
    if not isinstance(metadata, Mapping) or metadata.get("alg") != HPKE_ALG:
        raise PorosPayloadError(
            "encrypted detached object requires HPKE metadata",
            reason_code="invalid_encrypted_object_metadata",
        )
    if (
        receiver_authority_id is not None
        and metadata.get("recipient_authority_id") != receiver_authority_id
    ):
        raise PorosPayloadError(
            "encrypted detached object recipient does not match receiver",
            reason_code="encrypted_object_recipient_mismatch",
        )
    cleartext_chunks: list[bytes] = []
    for chunk in manifest.get("chunks") or []:
        if not isinstance(chunk, Mapping):
            raise PorosPayloadError(
                "detached object chunk must be an object",
                reason_code="invalid_detached_chunk",
            )
        offset = int(chunk["offset_bytes"])
        size = int(chunk["size_bytes"])
        encrypted_chunk = ciphertext[offset : offset + size]
        info = chunk.get("hpke_info")
        if not isinstance(info, Mapping):
            raise PorosPayloadError(
                "encrypted detached object chunk missing HPKE info",
                reason_code="invalid_encrypted_object_metadata",
            )
        try:
            cleartext_chunks.append(
                _suite().decrypt(
                    encrypted_chunk,
                    _private_key(recipient_private_key),
                    _hpke_info("detached_object_chunk", info),
                )
            )
        except Exception as exc:  # noqa: BLE001
            raise PorosPayloadError(
                "Poros detached-object HPKE decryption failed",
                reason_code="hpke_decryption_failed",
            ) from exc
    cleartext = b"".join(cleartext_chunks)
    expected_hash = metadata.get("cleartext_sha256")
    if not isinstance(expected_hash, str) or sha256_b64url(cleartext) != expected_hash:
        raise PorosPayloadError(
            "Poros detached-object cleartext hash mismatch",
            reason_code="cleartext_hash_mismatch",
        )
    expected_size = manifest.get("cleartext_size_bytes")
    if expected_size is not None and len(cleartext) != expected_size:
        raise PorosPayloadError(
            "Poros detached-object cleartext size mismatch",
            reason_code="cleartext_size_mismatch",
        )
    return cleartext


def wrap_object_key(*args: Any, **kwargs: Any) -> dict[str, Any]:
    raise PorosPayloadError(
        "Poros object key wrapping is replaced by per-chunk HPKE sealing",
        reason_code="object_key_wrapping_not_used",
    )


def unwrap_object_key(*args: Any, **kwargs: Any) -> bytes:
    raise PorosPayloadError(
        "Poros object key wrapping is replaced by per-chunk HPKE sealing",
        reason_code="object_key_wrapping_not_used",
    )


def _suite() -> hpke.Suite:
    return hpke.Suite(
        hpke.KEM.X25519,
        hpke.KDF.HKDF_SHA256,
        hpke.AEAD.CHACHA20_POLY1305,
    )


def _hpke_info(purpose: str, value: Mapping[str, Any] | None) -> bytes:
    return canonical_json_bytes({
        "poros": POROS_VERSION,
        "purpose": purpose,
        "value": dict(value or {}),
    })


def _object_chunk_info(
    *,
    object_id: str,
    index: int,
    cleartext_sha256: str,
) -> dict[str, Any]:
    return {
        "object_id": object_id,
        "chunk_index": index,
        "cleartext_sha256": cleartext_sha256,
    }


def _ciphertext_bytes(encrypted_body: Mapping[str, Any]) -> bytes:
    value = encrypted_body.get("ciphertext_b64url")
    if not isinstance(value, str):
        raise PorosPayloadError(
            "encrypted Poros envelope missing ciphertext",
            reason_code="invalid_encrypted_envelope",
        )
    ciphertext = b64url_decode(value, field="ciphertext_b64url")
    declared_hash = encrypted_body.get("ciphertext_sha256")
    if declared_hash is not None and sha256_b64url(ciphertext) != declared_hash:
        raise PorosPayloadError(
            "encrypted Poros envelope ciphertext hash mismatch",
            reason_code="ciphertext_hash_mismatch",
        )
    return ciphertext


def _chunks(data: bytes, chunk_size_bytes: int) -> list[bytes]:
    if not data:
        return []
    return [
        data[offset : offset + chunk_size_bytes]
        for offset in range(0, len(data), chunk_size_bytes)
    ]


def _private_key(value: bytes | str) -> x25519.X25519PrivateKey:
    raw = b64url_decode(value, field="private_key") if isinstance(value, str) else value
    if len(raw) != 32:
        raise PorosEncodingError(
            "X25519 private key must be 32 raw bytes",
            reason_code="invalid_key_encoding",
        )
    return x25519.X25519PrivateKey.from_private_bytes(raw)


def _public_key(value: bytes | str) -> x25519.X25519PublicKey:
    raw = b64url_decode(value, field="public_key") if isinstance(value, str) else value
    if len(raw) != 32:
        raise PorosEncodingError(
            "X25519 public key must be 32 raw bytes",
            reason_code="invalid_key_encoding",
        )
    return x25519.X25519PublicKey.from_public_bytes(raw)
