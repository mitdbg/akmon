"""Hash helpers for Poros manifests and envelopes."""

from __future__ import annotations

import hashlib
from typing import Any

from okg.substrate.exchange.base64url import b64url_decode, b64url_encode
from okg.substrate.exchange.canonical_json import canonical_json_bytes
from okg.substrate.exchange.errors import PorosEncodingError


def sha256_bytes(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def sha256_b64url(data: bytes) -> str:
    return b64url_encode(sha256_bytes(data))


def canonical_json_sha256_b64url(value: Any) -> str:
    return sha256_b64url(canonical_json_bytes(value))


def decode_sha256(value: str, *, field: str) -> bytes:
    digest = b64url_decode(value, field=field)
    if len(digest) != 32:
        raise PorosEncodingError(
            f"{field} must decode to 32 SHA-256 bytes",
            reason_code="invalid_sha256",
            data={"field": field},
        )
    return digest
