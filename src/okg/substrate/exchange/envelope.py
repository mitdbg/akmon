"""Poros signed-envelope construction and validation."""

from __future__ import annotations

import copy
import secrets
from datetime import UTC, datetime, timedelta
from typing import Any

from okg.substrate.exchange.constants import (
    CANONICALIZATION_ALG,
    DIGEST_ALG,
    ENCRYPTED_GRAPH_SUBMIT_TYPE,
    GRAPH_SUBMIT_TYPE,
    LEGACY_BUNDLE_SUBMIT_TYPE,
    POROS_VERSION,
    SIGNATURE_ALG,
)
from okg.substrate.exchange.encryption import HPKE_ALG
from okg.substrate.exchange.errors import (
    PorosPayloadError,
    PorosUnsupportedAlgorithmError,
)
from okg.substrate.exchange.hashing import canonical_json_sha256_b64url


def new_message_id() -> str:
    """Return a 256-bit random base64url message id."""
    return secrets.token_urlsafe(32).rstrip("=")


def now_timestamp() -> str:
    return format_timestamp(datetime.now(UTC))


def format_timestamp(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    value = value.astimezone(UTC).replace(microsecond=0)
    return value.isoformat().replace("+00:00", "Z")


def parse_timestamp(value: str, *, field: str) -> datetime:
    if not isinstance(value, str):
        raise PorosPayloadError(
            f"{field} must be an RFC3339 timestamp",
            reason_code="invalid_timestamp",
            data={"field": field},
        )
    raw = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise PorosPayloadError(
            f"{field} must be an RFC3339 timestamp",
            reason_code="invalid_timestamp",
            data={"field": field},
        ) from exc
    if parsed.tzinfo is None:
        raise PorosPayloadError(
            f"{field} must include a timezone",
            reason_code="invalid_timestamp",
            data={"field": field},
        )
    return parsed.astimezone(UTC)


def default_algorithms() -> dict[str, str]:
    return {
        "canonicalization": CANONICALIZATION_ALG,
        "signature": SIGNATURE_ALG,
        "digest": DIGEST_ALG,
    }


def build_unsigned_envelope(
    *,
    sender: str,
    recipient: str,
    body: dict[str, Any],
    message_type: str = GRAPH_SUBMIT_TYPE,
    message_id: str | None = None,
    created_at: datetime | None = None,
    ttl_seconds: int = 15 * 60,
    payload_mode: str = "inline",
) -> dict[str, Any]:
    """Create an unsigned Poros envelope with the required metadata."""
    created = created_at or datetime.now(UTC)
    expires = created + timedelta(seconds=ttl_seconds)
    envelope = {
        "poros": POROS_VERSION,
        "type": message_type,
        "message_id": message_id or new_message_id(),
        "sender": sender,
        "recipient": recipient,
        "created_at": format_timestamp(created),
        "expires_at": format_timestamp(expires),
        "algorithms": default_algorithms(),
        "payload_mode": payload_mode,
        "body_sha256": canonical_json_sha256_b64url(body),
        "body": copy.deepcopy(body),
    }
    return envelope


def validate_envelope_shape(envelope: dict[str, Any]) -> None:
    if not isinstance(envelope, dict):
        raise PorosPayloadError(
            "Poros envelope must be a JSON object",
            reason_code="invalid_envelope",
        )
    required = [
        "poros",
        "type",
        "message_id",
        "sender",
        "recipient",
        "created_at",
        "expires_at",
        "algorithms",
        "body_sha256",
        "signing_key_id",
    ]
    missing = [field for field in required if field not in envelope]
    if missing:
        raise PorosPayloadError(
            "Poros envelope is missing required fields",
            reason_code="invalid_envelope",
            data={"missing": missing},
        )
    if envelope.get("poros") != POROS_VERSION:
        raise PorosPayloadError(
            "unsupported Poros version",
            reason_code="unsupported_poros_version",
            data={"poros": envelope.get("poros")},
        )
    message_type = envelope.get("type")
    if message_type not in {
        GRAPH_SUBMIT_TYPE,
        LEGACY_BUNDLE_SUBMIT_TYPE,
        ENCRYPTED_GRAPH_SUBMIT_TYPE,
    }:
        raise PorosPayloadError(
            "unsupported Poros message type",
            reason_code="unsupported_message_type",
            data={"type": message_type},
        )


def validate_algorithms(envelope: dict[str, Any]) -> None:
    algorithms = envelope.get("algorithms")
    if not isinstance(algorithms, dict):
        raise PorosUnsupportedAlgorithmError(
            "Poros algorithms must be an object",
            reason_code="unsupported_algorithm",
        )
    expected = default_algorithms()
    for field, expected_value in expected.items():
        actual = algorithms.get(field)
        if actual != expected_value:
            raise PorosUnsupportedAlgorithmError(
                f"unsupported Poros {field}: {actual}",
                reason_code="unsupported_algorithm",
                data={"field": field, "actual": actual, "expected": expected_value},
            )
    encryption = algorithms.get("encryption")
    if encryption is not None and encryption != HPKE_ALG:
        raise PorosUnsupportedAlgorithmError(
            f"unsupported Poros encryption: {encryption}",
            reason_code="unsupported_algorithm",
            data={
                "field": "encryption",
                "actual": encryption,
                "expected": HPKE_ALG,
            },
        )


def validate_body_hash(envelope: dict[str, Any]) -> None:
    if "body" not in envelope:
        raise PorosPayloadError(
            "public Poros envelope must include body",
            reason_code="missing_body",
        )
    expected = canonical_json_sha256_b64url(envelope["body"])
    if envelope.get("body_sha256") != expected:
        raise PorosPayloadError(
            "Poros body hash does not match body",
            reason_code="body_hash_mismatch",
            data={"declared": envelope.get("body_sha256"), "computed": expected},
        )


def verify_envelope(envelope: dict[str, Any], public_key: bytes | str) -> None:
    """Validate algorithms, body hash, and Ed25519 signature."""
    from okg.substrate.exchange.signing import verify_signed_document

    validate_envelope_shape(envelope)
    validate_algorithms(envelope)
    verify_signed_document(envelope, public_key)
    validate_body_hash(envelope)
