"""Inline and detached payload validation for Poros."""

from __future__ import annotations

from math import ceil
from typing import Any

from okg.substrate.exchange.base64url import b64url_decode
from okg.substrate.exchange.canonical_json import canonical_json_bytes
from okg.substrate.exchange.constants import (
    DETACHED_MAX_BYTES,
    EXCHANGE_CLASSIFICATIONS,
    INLINE_MEDIUM_MAX_BYTES,
    INLINE_SMALL_MAX_BYTES,
)
from okg.substrate.exchange.encryption import HPKE_ALG
from okg.substrate.exchange.errors import PorosPayloadError
from okg.substrate.exchange.hashing import decode_sha256, sha256_b64url


def enforce_payload_policy(
    envelope: dict[str, Any],
    *,
    allow_medium_inline: bool = False,
    receiver_authority_id: str | None = None,
) -> None:
    mode = payload_mode(envelope)
    if mode == "inline":
        _enforce_inline_body(envelope, allow_medium_inline=allow_medium_inline)
        return
    if mode == "detached":
        validate_detached_manifest(
            envelope.get("body"),
            receiver_authority_id=receiver_authority_id,
        )
        return
    raise PorosPayloadError(
        "unsupported Poros payload mode",
        reason_code="unsupported_payload_mode",
        data={"payload_mode": mode},
    )


def payload_mode(envelope: dict[str, Any]) -> str:
    body = envelope.get("body")
    if isinstance(body, dict) and isinstance(body.get("payload_mode"), str):
        return body["payload_mode"]
    mode = envelope.get("payload_mode", "inline")
    return str(mode)


def validate_detached_manifest(
    body: Any,
    *,
    receiver_authority_id: str | None = None,
    validate_embedded_bytes: bool = True,
) -> None:
    if not isinstance(body, dict):
        raise PorosPayloadError(
            "detached payload body must be a manifest object",
            reason_code="invalid_detached_manifest",
        )
    objects = body.get("objects")
    if not isinstance(objects, list) or not objects:
        raise PorosPayloadError(
            "detached manifest must contain at least one object",
            reason_code="invalid_detached_manifest",
        )
    for obj in objects:
        validate_detached_object_manifest(
            obj,
            receiver_authority_id=receiver_authority_id,
            validate_embedded_bytes=validate_embedded_bytes,
        )


def validate_detached_object_manifest(
    obj: Any,
    *,
    receiver_authority_id: str | None = None,
    validate_embedded_bytes: bool = True,
) -> None:
    if not isinstance(obj, dict):
        raise PorosPayloadError(
            "detached object manifest must be an object",
            reason_code="invalid_detached_object_manifest",
        )
    object_id = obj.get("object_id")
    if not isinstance(object_id, str) or not object_id:
        raise PorosPayloadError(
            "detached object requires object_id",
            reason_code="invalid_detached_object_manifest",
        )
    classification = obj.get("classification", "public")
    if not isinstance(classification, str) or classification not in EXCHANGE_CLASSIFICATIONS:
        raise PorosPayloadError(
            "detached object classification is not supported",
            reason_code="invalid_detached_object_manifest",
            data={"object_id": object_id, "classification": classification},
        )
    size = _int_field(obj, "size_bytes", aliases=("size",))
    if size < 0:
        raise PorosPayloadError(
            "detached object size must be non-negative",
            reason_code="invalid_detached_object_manifest",
            data={"object_id": object_id},
        )
    if size > DETACHED_MAX_BYTES:
        raise PorosPayloadError(
            "detached object exceeds Poros v0 maximum size",
            reason_code="payload_too_large",
            data={"object_id": object_id, "size_bytes": size},
        )
    chunk_size = _int_field(obj, "chunk_size_bytes", aliases=("chunk_size",))
    if chunk_size <= 0:
        raise PorosPayloadError(
            "detached object chunk size must be positive",
            reason_code="invalid_detached_object_manifest",
            data={"object_id": object_id},
        )
    hash_field = _object_hash_field(obj)
    decode_sha256(str(obj[hash_field]), field=f"{object_id}.{hash_field}")
    chunks = obj.get("chunks")
    if not isinstance(chunks, list):
        raise PorosPayloadError(
            "detached object chunks must be a list",
            reason_code="invalid_detached_object_manifest",
            data={"object_id": object_id},
        )
    expected_count = 0 if size == 0 else ceil(size / chunk_size)
    if len(chunks) != expected_count:
        raise PorosPayloadError(
            "detached object chunk count does not match size",
            reason_code="chunk_count_mismatch",
            data={
                "object_id": object_id,
                "declared": len(chunks),
                "expected": expected_count,
            },
        )
    offset = 0
    for expected_index, chunk in enumerate(chunks):
        offset = _validate_chunk(
            object_id=object_id,
            chunk=chunk,
            expected_index=expected_index,
            expected_offset=offset,
            chunk_size=chunk_size,
            object_size=size,
        )
    if offset != size:
        raise PorosPayloadError(
            "detached object chunks do not span declared size",
            reason_code="chunk_size_mismatch",
            data={"object_id": object_id, "final_offset": offset, "size": size},
        )
    if "locations" in obj and not isinstance(obj["locations"], list):
        raise PorosPayloadError(
            "detached object locations must be a list",
            reason_code="invalid_detached_object_manifest",
            data={"object_id": object_id},
        )
    _validate_encryption_metadata(
        obj,
        receiver_authority_id=receiver_authority_id,
    )
    if validate_embedded_bytes and isinstance(obj.get("inline_bytes_b64url"), str):
        data = b64url_decode(
            str(obj["inline_bytes_b64url"]),
            field=f"{object_id}.inline_bytes_b64url",
        )
        validate_detached_object_bytes(obj, data)


def validate_detached_object_bytes(obj: dict[str, Any], data: bytes) -> None:
    """Verify fetched bytes against a detached object manifest."""
    validate_detached_object_manifest(obj, validate_embedded_bytes=False)
    object_id = str(obj["object_id"])
    encrypted = "encryption" in obj or "ciphertext_sha256" in obj
    size = _int_field(obj, "size_bytes", aliases=("size",))
    if not encrypted and len(data) != size:
        raise PorosPayloadError(
            "detached object fetched byte length does not match manifest",
            reason_code="object_size_mismatch",
            data={"object_id": object_id, "declared": size, "actual": len(data)},
        )
    hash_field = _object_hash_field(obj)
    if sha256_b64url(data) != obj[hash_field]:
        raise PorosPayloadError(
            "detached object hash does not match manifest",
            reason_code="object_hash_mismatch",
            data={"object_id": object_id, "field": hash_field},
        )
    offset = 0
    for chunk in obj["chunks"]:
        chunk_size = _int_field(chunk, "size_bytes", aliases=("size",))
        chunk_bytes = data[offset : offset + chunk_size]
        chunk_hash_field = _chunk_hash_field(chunk)
        if sha256_b64url(chunk_bytes) != chunk[chunk_hash_field]:
            raise PorosPayloadError(
                "detached object chunk hash does not match manifest",
                reason_code="chunk_hash_mismatch",
                data={"object_id": object_id, "index": chunk.get("index")},
            )
        offset += chunk_size


def build_detached_object_manifest(
    *,
    object_id: str,
    media_type: str,
    data: bytes,
    chunk_size_bytes: int,
    classification: str = "public",
    locations: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if chunk_size_bytes <= 0:
        raise PorosPayloadError(
            "chunk size must be positive",
            reason_code="invalid_detached_object_manifest",
        )
    chunks: list[dict[str, Any]] = []
    for index, offset in enumerate(range(0, len(data), chunk_size_bytes)):
        chunk = data[offset : offset + chunk_size_bytes]
        chunks.append(
            {
                "index": index,
                "offset_bytes": offset,
                "size_bytes": len(chunk),
                "cleartext_sha256": sha256_b64url(chunk),
            }
        )
    return {
        "object_id": object_id,
        "media_type": media_type,
        "size_bytes": len(data),
        "chunk_size_bytes": chunk_size_bytes,
        "cleartext_sha256": sha256_b64url(data),
        "classification": classification,
        "chunks": chunks,
        "locations": list(locations or []),
    }


def _enforce_inline_body(
    envelope: dict[str, Any],
    *,
    allow_medium_inline: bool,
) -> None:
    if "body" not in envelope:
        raise PorosPayloadError(
            "inline Poros envelope must contain body",
            reason_code="missing_body",
        )
    size = len(canonical_json_bytes(envelope["body"]))
    if size <= INLINE_SMALL_MAX_BYTES:
        return
    if size <= INLINE_MEDIUM_MAX_BYTES and allow_medium_inline:
        return
    raise PorosPayloadError(
        "inline Poros payload requires detached object transfer",
        reason_code="payload_requires_detached",
        data={"size_bytes": size},
    )


def _validate_chunk(
    *,
    object_id: str,
    chunk: Any,
    expected_index: int,
    expected_offset: int,
    chunk_size: int,
    object_size: int,
) -> int:
    if not isinstance(chunk, dict):
        raise PorosPayloadError(
            "detached object chunk must be an object",
            reason_code="invalid_detached_chunk",
            data={"object_id": object_id},
        )
    if chunk.get("index") != expected_index:
        raise PorosPayloadError(
            "detached object chunk index mismatch",
            reason_code="chunk_index_mismatch",
            data={"object_id": object_id, "expected": expected_index},
        )
    if "offset_bytes" in chunk and chunk["offset_bytes"] != expected_offset:
        raise PorosPayloadError(
            "detached object chunk offset mismatch",
            reason_code="chunk_offset_mismatch",
            data={"object_id": object_id, "index": expected_index},
        )
    remaining = max(object_size - expected_offset, 0)
    expected_size = min(chunk_size, remaining)
    actual_size = _int_field(chunk, "size_bytes", aliases=("size",))
    if actual_size != expected_size:
        raise PorosPayloadError(
            "detached object chunk size mismatch",
            reason_code="chunk_size_mismatch",
            data={
                "object_id": object_id,
                "index": expected_index,
                "expected": expected_size,
                "actual": actual_size,
            },
        )
    hash_field = _chunk_hash_field(chunk)
    decode_sha256(str(chunk[hash_field]), field=f"{object_id}.chunks[{expected_index}]")
    return expected_offset + actual_size


def _object_hash_field(obj: dict[str, Any]) -> str:
    if "ciphertext_sha256" in obj:
        return "ciphertext_sha256"
    if "cleartext_sha256" in obj:
        return "cleartext_sha256"
    if "sha256" in obj:
        return "sha256"
    raise PorosPayloadError(
        "detached object requires cleartext_sha256 or ciphertext_sha256",
        reason_code="invalid_detached_object_manifest",
        data={"object_id": obj.get("object_id")},
    )


def _validate_encryption_metadata(
    obj: dict[str, Any],
    *,
    receiver_authority_id: str | None,
) -> None:
    encrypted = "encryption" in obj or "ciphertext_sha256" in obj
    if not encrypted:
        return
    object_id = str(obj.get("object_id") or "")
    metadata = obj.get("encryption")
    if not isinstance(metadata, dict):
        raise PorosPayloadError(
            "encrypted detached object requires encryption metadata",
            reason_code="invalid_encrypted_object_metadata",
            data={"object_id": object_id},
        )
    if metadata.get("alg") != HPKE_ALG:
        raise PorosPayloadError(
            "unsupported encrypted detached object algorithm",
            reason_code="unsupported_encryption_algorithm",
            data={"object_id": object_id, "alg": metadata.get("alg")},
        )
    recipient = metadata.get("recipient_authority_id")
    if receiver_authority_id is not None and recipient != receiver_authority_id:
        raise PorosPayloadError(
            "encrypted detached object recipient does not match receiver",
            reason_code="encrypted_object_recipient_mismatch",
            data={"object_id": object_id, "recipient": recipient},
        )
    tag = metadata.get("aead_tag_b64url")
    tag_hash = metadata.get("aead_tag_sha256")
    if tag is None and tag_hash is None:
        return
    if not isinstance(tag, str) or not isinstance(tag_hash, str):
        raise PorosPayloadError(
            "encrypted detached object AEAD tag evidence is incomplete",
            reason_code="invalid_encrypted_object_metadata",
            data={"object_id": object_id},
        )
    tag_bytes = b64url_decode(tag, field=f"{object_id}.aead_tag_b64url")
    decode_sha256(tag_hash, field=f"{object_id}.aead_tag_sha256")
    if sha256_b64url(tag_bytes) != tag_hash:
        raise PorosPayloadError(
            "encrypted detached object AEAD tag hash does not match",
            reason_code="aead_tag_mismatch",
            data={"object_id": object_id},
        )


def _chunk_hash_field(chunk: dict[str, Any]) -> str:
    if "ciphertext_sha256" in chunk:
        return "ciphertext_sha256"
    if "cleartext_sha256" in chunk:
        return "cleartext_sha256"
    if "sha256" in chunk:
        return "sha256"
    raise PorosPayloadError(
        "detached object chunk requires a SHA-256 hash",
        reason_code="invalid_detached_chunk",
        data={"index": chunk.get("index")},
    )


def _int_field(
    obj: dict[str, Any],
    name: str,
    *,
    aliases: tuple[str, ...] = (),
) -> int:
    value = obj.get(name)
    if value is None:
        for alias in aliases:
            if alias in obj:
                value = obj[alias]
                break
    if not isinstance(value, int) or isinstance(value, bool):
        raise PorosPayloadError(
            f"{name} must be an integer",
            reason_code="invalid_detached_object_manifest",
            data={"field": name},
        )
    return value
