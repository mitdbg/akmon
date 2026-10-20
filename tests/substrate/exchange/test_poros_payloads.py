from __future__ import annotations

import copy

import pytest

from okg.substrate.exchange.base64url import b64url_encode
from okg.substrate.exchange.constants import DETACHED_MAX_BYTES
from okg.substrate.exchange.encryption import (
    HPKE_ALG,
    decrypt_detached_object,
    derive_x25519_public_key,
    encrypt_detached_object,
)
from okg.substrate.exchange.envelope import build_unsigned_envelope
from okg.substrate.exchange.errors import PorosPayloadError
from okg.substrate.exchange.hashing import sha256_b64url
from okg.substrate.exchange.payloads import (
    build_detached_object_manifest,
    enforce_payload_policy,
    validate_detached_manifest,
    validate_detached_object_bytes,
)


def test_inline_payload_under_100_kib_is_accepted() -> None:
    envelope = build_unsigned_envelope(
        sender="okg:lab-a",
        recipient="okg:mit",
        body={"kind": "graph.submit", "text": "x" * (80 * 1024)},
    )

    enforce_payload_policy(envelope)


def test_inline_payload_over_1_mib_requires_detached_object() -> None:
    envelope = build_unsigned_envelope(
        sender="okg:lab-a",
        recipient="okg:mit",
        body={"kind": "graph.submit", "text": "x" * (1024 * 1024 + 1)},
    )

    with pytest.raises(PorosPayloadError) as exc:
        enforce_payload_policy(envelope)

    assert exc.value.reason_code == "payload_requires_detached"


def test_detached_manifest_validates_size_and_hashes() -> None:
    data = b'{"nodes":[],"edges":[]}' * 8
    obj = build_detached_object_manifest(
        object_id="obj:demo",
        media_type="application/vnd.okg.graph+json",
        data=data,
        chunk_size_bytes=31,
    )

    validate_detached_manifest({"payload_mode": "detached", "objects": [obj]})
    validate_detached_object_bytes(obj, data)


def test_detached_manifest_rejects_unknown_classification() -> None:
    data = b'{"nodes":[],"edges":[]}'
    obj = build_detached_object_manifest(
        object_id="obj:unknown-classification",
        media_type="application/vnd.okg.graph+json",
        data=data,
        chunk_size_bytes=31,
        classification="secret",
    )

    with pytest.raises(PorosPayloadError) as exc:
        validate_detached_manifest({"payload_mode": "detached", "objects": [obj]})

    assert exc.value.reason_code == "invalid_detached_object_manifest"


@pytest.mark.parametrize("size", [150 * 1024, 1024 * 1024 + 7])
def test_medium_and_large_detached_payloads_pass_manifest_verification(
    size: int,
) -> None:
    data = b"x" * size
    obj = build_detached_object_manifest(
        object_id=f"obj:{size}",
        media_type="application/vnd.okg.graph+json",
        data=data,
        chunk_size_bytes=64 * 1024,
    )
    obj["inline_bytes_b64url"] = b64url_encode(data)
    body = {"payload_mode": "detached", "objects": [obj]}
    envelope = build_unsigned_envelope(
        sender="okg:lab-a",
        recipient="okg:mit",
        body=body,
        payload_mode="detached",
    )

    enforce_payload_policy(envelope, receiver_authority_id="okg:mit")


@pytest.mark.parametrize(
    ("edit", "reason"),
    [
        (lambda obj: obj.update({"size_bytes": obj["size_bytes"] + 1}), "chunk_count_mismatch"),
        (lambda obj: obj.update({"cleartext_sha256": "A" * 43}), "object_hash_mismatch"),
        (
            lambda obj: obj["chunks"][0].update({"cleartext_sha256": "A" * 43}),
            "chunk_hash_mismatch",
        ),
    ],
)
def test_detached_object_tamper_is_rejected(edit, reason) -> None:
    data = b"0123456789abcdef" * 4
    obj = build_detached_object_manifest(
        object_id="obj:demo",
        media_type="application/octet-stream",
        data=data,
        chunk_size_bytes=16,
    )
    tampered = copy.deepcopy(obj)
    edit(tampered)

    with pytest.raises(PorosPayloadError) as exc:
        validate_detached_object_bytes(tampered, data)

    assert exc.value.reason_code == reason


def test_detached_manifest_rejects_objects_over_1_gib() -> None:
    obj = {
        "object_id": "obj:too-large",
        "media_type": "application/octet-stream",
        "size_bytes": DETACHED_MAX_BYTES + 1,
        "chunk_size_bytes": 1024,
        "cleartext_sha256": "A" * 43,
        "chunks": [],
        "locations": [],
    }

    with pytest.raises(PorosPayloadError) as exc:
        validate_detached_manifest({"payload_mode": "detached", "objects": [obj]})

    assert exc.value.reason_code == "payload_too_large"


def test_private_detached_object_wrong_recipient_fails_closed() -> None:
    obj = _encrypted_object_manifest(
        data=b"ciphertext",
        recipient="okg:mit-private",
    )

    with pytest.raises(PorosPayloadError) as exc:
        validate_detached_manifest(
            {"payload_mode": "detached", "objects": [obj]},
            receiver_authority_id="okg:other",
        )

    assert exc.value.reason_code == "encrypted_object_recipient_mismatch"


def test_private_detached_object_aead_tag_tamper_fails_closed() -> None:
    obj = _encrypted_object_manifest(
        data=b"ciphertext",
        recipient="okg:mit-private",
    )
    obj["encryption"]["aead_tag_b64url"] = b64url_encode(b"tampered-tag")

    with pytest.raises(PorosPayloadError) as exc:
        validate_detached_manifest(
            {"payload_mode": "detached", "objects": [obj]},
            receiver_authority_id="okg:mit-private",
        )

    assert exc.value.reason_code == "aead_tag_mismatch"


def test_chunked_private_object_encrypts_and_decrypts_16_mib() -> None:
    receiver_private = b"\x05" * 32
    receiver_public = derive_x25519_public_key(receiver_private)
    data = b"x" * (16 * 1024 * 1024)

    manifest, ciphertext = encrypt_detached_object(
        object_id="obj:private-16mib",
        media_type="application/vnd.okg.graph+json",
        data=data,
        chunk_size_bytes=1024 * 1024,
        recipient_authority_id="okg:mit-private",
        recipient_public_key=receiver_public,
    )

    validate_detached_manifest(
        {"payload_mode": "detached", "objects": [manifest]},
        receiver_authority_id="okg:mit-private",
    )
    decrypted = decrypt_detached_object(
        manifest,
        ciphertext,
        receiver_private,
        receiver_authority_id="okg:mit-private",
    )

    assert decrypted == data


def test_chunked_private_object_tamper_fails_hpke_authentication() -> None:
    receiver_private = b"\x06" * 32
    receiver_public = derive_x25519_public_key(receiver_private)
    data = b"private context" * 100
    manifest, ciphertext = encrypt_detached_object(
        object_id="obj:private-tamper",
        media_type="application/octet-stream",
        data=data,
        chunk_size_bytes=256,
        recipient_authority_id="okg:mit-private",
        recipient_public_key=receiver_public,
    )
    tampered = bytearray(ciphertext)
    tampered[10] ^= 1
    tampered_manifest = copy.deepcopy(manifest)
    tampered_manifest["ciphertext_sha256"] = sha256_b64url(bytes(tampered))
    tampered_manifest["chunks"][0]["ciphertext_sha256"] = sha256_b64url(
        bytes(tampered[: tampered_manifest["chunks"][0]["size_bytes"]])
    )

    with pytest.raises(PorosPayloadError) as exc:
        decrypt_detached_object(
            tampered_manifest,
            bytes(tampered),
            receiver_private,
            receiver_authority_id="okg:mit-private",
        )

    assert exc.value.reason_code == "hpke_decryption_failed"


def test_chunked_private_object_wrong_associated_data_fails() -> None:
    receiver_private = b"\x07" * 32
    receiver_public = derive_x25519_public_key(receiver_private)
    manifest, ciphertext = encrypt_detached_object(
        object_id="obj:private-aad",
        media_type="application/octet-stream",
        data=b"private context",
        chunk_size_bytes=8,
        recipient_authority_id="okg:mit-private",
        recipient_public_key=receiver_public,
    )
    bad_manifest = copy.deepcopy(manifest)
    bad_manifest["chunks"][0]["hpke_info"]["object_id"] = "obj:other"

    with pytest.raises(PorosPayloadError) as exc:
        decrypt_detached_object(
            bad_manifest,
            ciphertext,
            receiver_private,
            receiver_authority_id="okg:mit-private",
        )

    assert exc.value.reason_code == "hpke_decryption_failed"


def test_chunked_private_object_wrong_key_fails() -> None:
    receiver_private = b"\x08" * 32
    receiver_public = derive_x25519_public_key(receiver_private)
    manifest, ciphertext = encrypt_detached_object(
        object_id="obj:private-wrong-key",
        media_type="application/octet-stream",
        data=b"private context",
        chunk_size_bytes=8,
        recipient_authority_id="okg:mit-private",
        recipient_public_key=receiver_public,
    )

    with pytest.raises(PorosPayloadError) as exc:
        decrypt_detached_object(
            manifest,
            ciphertext,
            b"\x09" * 32,
            receiver_authority_id="okg:mit-private",
        )

    assert exc.value.reason_code == "hpke_decryption_failed"


def _encrypted_object_manifest(
    *,
    data: bytes,
    recipient: str,
) -> dict[str, object]:
    tag = b"valid-tag"
    return {
        "object_id": "obj:private",
        "media_type": "application/octet-stream",
        "size_bytes": len(data),
        "chunk_size_bytes": len(data),
        "ciphertext_sha256": sha256_b64url(data),
        "classification": "restricted",
        "encryption": {
            "alg": HPKE_ALG,
            "recipient_authority_id": recipient,
            "aead_tag_b64url": b64url_encode(tag),
            "aead_tag_sha256": sha256_b64url(tag),
        },
        "chunks": [
            {
                "index": 0,
                "offset_bytes": 0,
                "size_bytes": len(data),
                "ciphertext_sha256": sha256_b64url(data),
            }
        ],
        "locations": [{"uri": "s3://private-bucket/object"}],
        "inline_bytes_b64url": b64url_encode(data),
    }
