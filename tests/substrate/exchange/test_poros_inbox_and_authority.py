from __future__ import annotations

import copy
from datetime import UTC, datetime, timedelta

from okg.substrate.exchange.authority import (
    KeyDescriptor,
    TrustPin,
    TrustPinRegistry,
    build_authority_document,
)
from okg.substrate.exchange.canonical_json import canonical_json_bytes
from okg.substrate.exchange.encryption import encrypt_private_envelope
from okg.substrate.exchange.encryption import derive_x25519_public_key
from okg.substrate.exchange.envelope import build_unsigned_envelope
from okg.substrate.exchange.errors import PorosTrustError
from okg.substrate.exchange.inbox import PorosInbox
from okg.substrate.exchange.signing import (
    derive_ed25519_public_key,
    sign_envelope,
    verify_receipt,
)


SENDER_PRIVATE = b"\x01" * 32
SENDER_PUBLIC = derive_ed25519_public_key(SENDER_PRIVATE)
RECEIVER_PRIVATE = b"\x02" * 32
RECEIVER_PUBLIC = derive_ed25519_public_key(RECEIVER_PRIVATE)
RECEIVER_ENC_PRIVATE = b"\x03" * 32
RECEIVER_ENC_PUBLIC = derive_x25519_public_key(RECEIVER_ENC_PRIVATE)
WRONG_RECEIVER_ENC_PRIVATE = b"\x04" * 32


def _registry() -> TrustPinRegistry:
    return TrustPinRegistry.from_pins(
        [
            TrustPin(
                authority_id="okg:lab-a",
                kid="lab-a-sign-1",
                public_key=SENDER_PUBLIC,
            )
        ]
    )


def _inbox(registry: TrustPinRegistry | None = None) -> PorosInbox:
    return PorosInbox(
        receiver_authority_id="okg:mit",
        trust_registry=registry or _registry(),
        receiver_private_key=RECEIVER_PRIVATE,
        receiver_signing_kid="mit-sign-1",
        receiver_encryption_private_key=RECEIVER_ENC_PRIVATE,
    )


def _signed(ttl_seconds: int = 900, created_at: datetime | None = None) -> dict:
    unsigned = build_unsigned_envelope(
        sender="okg:lab-a",
        recipient="okg:mit",
        message_id="msg-replay",
        created_at=created_at or datetime(2026, 5, 24, 12, 0, tzinfo=UTC),
        ttl_seconds=ttl_seconds,
        body={"graph_id": "poros:graph:lab-a:demo", "classification": "public"},
    )
    return sign_envelope(unsigned, SENDER_PRIVATE, kid="lab-a-sign-1")


def test_inbox_accepts_once_then_returns_signed_duplicate_receipt() -> None:
    inbox = _inbox()
    envelope = _signed()
    now = datetime(2026, 5, 24, 12, 1, tzinfo=UTC)

    first = inbox.handle_envelope(envelope, now=now)
    second = inbox.handle_envelope(envelope, now=now + timedelta(seconds=1))

    assert first.status == "accepted_for_admission"
    assert first.admission_candidate is not None
    assert second.status == "duplicate"
    assert second.admission_candidate is None
    assert second.receipt is not None
    assert second.receipt["in_reply_to"] == "msg-replay"
    assert second.receipt["received_body_sha256"] == envelope["body_sha256"]
    verify_receipt(second.receipt, RECEIVER_PUBLIC)


def test_inbox_rejects_expired_message_with_signed_receipt() -> None:
    envelope = _signed(
        ttl_seconds=60,
        created_at=datetime(2026, 5, 24, 11, 0, tzinfo=UTC),
    )

    result = _inbox().handle_envelope(
        envelope,
        now=datetime(2026, 5, 24, 12, 0, tzinfo=UTC),
    )

    assert result.status == "rejected_preflight"
    assert result.reason_code == "expired_message"
    assert result.receipt is not None
    verify_receipt(result.receipt, RECEIVER_PUBLIC)


def test_inbox_rejects_unpinned_sender_before_admission() -> None:
    result = _inbox(TrustPinRegistry()).handle_envelope(
        _signed(),
        now=datetime(2026, 5, 24, 12, 1, tzinfo=UTC),
    )

    assert result.status == "rejected_preflight"
    assert result.reason_code == "missing_trust_pin"
    assert result.admission_candidate is None


def test_inbox_rejects_unsupported_message_type_before_admission() -> None:
    unsigned = build_unsigned_envelope(
        sender="okg:lab-a",
        recipient="okg:mit",
        message_id="msg-unsupported",
        created_at=datetime(2026, 5, 24, 12, 0, tzinfo=UTC),
        ttl_seconds=900,
        message_type="remote.execute",
        body={"graph_id": "poros:graph:unsupported"},
    )
    result = _inbox().handle_envelope(
        sign_envelope(unsigned, SENDER_PRIVATE, kid="lab-a-sign-1"),
        now=datetime(2026, 5, 24, 12, 1, tzinfo=UTC),
    )

    assert result.status == "rejected_preflight"
    assert result.reason_code == "unsupported_message_type"
    assert result.admission_candidate is None


def test_encrypted_graph_submit_verifies_outer_and_inner_signatures() -> None:
    inner = sign_envelope(
        build_unsigned_envelope(
            sender="okg:lab-a",
            recipient="okg:mit",
            message_id="msg-encrypted",
            created_at=datetime(2026, 5, 24, 12, 0, tzinfo=UTC),
            ttl_seconds=900,
            body={"graph_id": "poros:graph:encrypted-private"},
        ),
        SENDER_PRIVATE,
        kid="lab-a-sign-1",
    )
    encrypted_body = encrypt_private_envelope(inner, RECEIVER_ENC_PUBLIC)
    outer = build_unsigned_envelope(
        sender="okg:lab-a",
        recipient="okg:mit",
        message_id="msg-encrypted",
        created_at=datetime(2026, 5, 24, 12, 0, tzinfo=UTC),
        ttl_seconds=900,
        message_type="encrypted.graph.submit",
        body=encrypted_body,
    )
    outer["algorithms"]["encryption"] = (
        "HPKE-v1-X25519-HKDF-SHA256-ChaCha20Poly1305"
    )
    result = _inbox().handle_envelope(
        sign_envelope(outer, SENDER_PRIVATE, kid="lab-a-sign-1"),
        now=datetime(2026, 5, 24, 12, 1, tzinfo=UTC),
    )

    assert result.status == "accepted_for_admission"
    assert result.admission_candidate is not None
    assert result.admission_candidate.body["graph_id"] == (
        "poros:graph:encrypted-private"
    )


def test_inbox_invalid_signature_returns_signed_preflight_receipt() -> None:
    envelope = copy.deepcopy(_signed())
    envelope["body"]["graph_id"] = "poros:graph:tampered"

    result = _inbox().handle_envelope(
        envelope,
        now=datetime(2026, 5, 24, 12, 1, tzinfo=UTC),
    )

    assert result.status == "rejected_preflight"
    assert result.reason_code == "invalid_signature"
    assert result.receipt is not None
    verify_receipt(result.receipt, RECEIVER_PUBLIC)


def test_canonical_http_bytes_are_required_before_signature() -> None:
    result = _inbox().handle_bytes(
        b'{ "sender": "okg:lab-a", "sender": "okg:lab-b" }',
        now=datetime(2026, 5, 24, 12, 1, tzinfo=UTC),
    )

    assert result.status == "rejected_preflight"
    assert result.reason_code == "duplicate_key"
    assert result.receipt is None


def test_canonical_signed_bytes_are_accepted_by_inbox() -> None:
    result = _inbox().handle_bytes(
        canonical_json_bytes(_signed()),
        now=datetime(2026, 5, 24, 12, 1, tzinfo=UTC),
    )

    assert result.status == "accepted_for_admission"


def test_authority_document_renders_public_metadata_without_secrets() -> None:
    doc = build_authority_document(
        authority_id="okg:mit",
        base_url="https://okg.example.org/",
        display_name="MIT OKG",
        signing_keys=[
            KeyDescriptor(
                kid="mit-sign-1",
                use="sign",
                alg="Ed25519",
                public_key=RECEIVER_PUBLIC,
            )
        ],
        latest_public_generation={"generation_id": 42},
    ).to_dict()

    assert doc["authority_id"] == "okg:mit"
    assert doc["endpoints"]["inbox"] == "https://okg.example.org/poros/v1/inbox"
    assert "graph.submit" in doc["capabilities"]
    assert doc["keys"][0]["public_key"] == RECEIVER_PUBLIC
    assert "private_key" not in str(doc)


def test_discovery_mismatch_is_not_silently_trusted() -> None:
    registry = _registry()
    doc = build_authority_document(
        authority_id="okg:lab-a",
        base_url="https://lab.example",
        signing_keys=[
            KeyDescriptor(
                kid="lab-a-sign-1",
                use="sign",
                alg="Ed25519",
                public_key=RECEIVER_PUBLIC,
            )
        ],
    )

    try:
        registry.check_discovery_document(doc)
    except PorosTrustError as exc:
        assert exc.reason_code == "key_material_mismatch"
    else:  # pragma: no cover
        raise AssertionError("expected key material mismatch")


def test_discovery_rotation_is_pending_not_automatic() -> None:
    registry = _registry()
    doc = build_authority_document(
        authority_id="okg:lab-a",
        base_url="https://lab.example",
        signing_keys=[
            KeyDescriptor(
                kid="lab-a-sign-2",
                use="sign",
                alg="Ed25519",
                public_key=RECEIVER_PUBLIC,
            )
        ],
    )

    assert registry.rotation_candidates(doc) == [
        {
            "status": "pending_rotation",
            "authority_id": "okg:lab-a",
            "kid": "lab-a-sign-2",
            "use": "sign",
            "alg": "Ed25519",
        }
    ]


def test_encrypted_graph_submit_rejects_tampered_inner_signature() -> None:
    inner = sign_envelope(
        build_unsigned_envelope(
            sender="okg:lab-a",
            recipient="okg:mit",
            message_id="msg-inner-tamper",
            created_at=datetime(2026, 5, 24, 12, 0, tzinfo=UTC),
            ttl_seconds=900,
            body={"graph_id": "poros:graph:inner-tamper"},
        ),
        SENDER_PRIVATE,
        kid="lab-a-sign-1",
    )
    inner["body"]["graph_id"] = "poros:graph:tampered"
    encrypted_body = encrypt_private_envelope(inner, RECEIVER_ENC_PUBLIC)
    outer = build_unsigned_envelope(
        sender="okg:lab-a",
        recipient="okg:mit",
        message_id="msg-inner-tamper",
        created_at=datetime(2026, 5, 24, 12, 0, tzinfo=UTC),
        ttl_seconds=900,
        message_type="encrypted.graph.submit",
        body=encrypted_body,
    )
    outer["algorithms"]["encryption"] = (
        "HPKE-v1-X25519-HKDF-SHA256-ChaCha20Poly1305"
    )

    result = _inbox().handle_envelope(
        sign_envelope(outer, SENDER_PRIVATE, kid="lab-a-sign-1"),
        now=datetime(2026, 5, 24, 12, 1, tzinfo=UTC),
    )

    assert result.status == "rejected_preflight"
    assert result.reason_code == "invalid_signature"
    assert result.admission_candidate is None


def test_encrypted_graph_submit_wrong_receiver_key_fails_closed() -> None:
    inner = sign_envelope(
        build_unsigned_envelope(
            sender="okg:lab-a",
            recipient="okg:mit",
            message_id="msg-wrong-receiver",
            created_at=datetime(2026, 5, 24, 12, 0, tzinfo=UTC),
            ttl_seconds=900,
            body={"graph_id": "poros:graph:wrong-receiver"},
        ),
        SENDER_PRIVATE,
        kid="lab-a-sign-1",
    )
    encrypted_body = encrypt_private_envelope(inner, RECEIVER_ENC_PUBLIC)
    outer = build_unsigned_envelope(
        sender="okg:lab-a",
        recipient="okg:mit",
        message_id="msg-wrong-receiver",
        created_at=datetime(2026, 5, 24, 12, 0, tzinfo=UTC),
        ttl_seconds=900,
        message_type="encrypted.graph.submit",
        body=encrypted_body,
    )
    outer["algorithms"]["encryption"] = (
        "HPKE-v1-X25519-HKDF-SHA256-ChaCha20Poly1305"
    )
    inbox = PorosInbox(
        receiver_authority_id="okg:mit",
        trust_registry=_registry(),
        receiver_private_key=RECEIVER_PRIVATE,
        receiver_signing_kid="mit-sign-1",
        receiver_encryption_private_key=WRONG_RECEIVER_ENC_PRIVATE,
    )

    result = inbox.handle_envelope(
        sign_envelope(outer, SENDER_PRIVATE, kid="lab-a-sign-1"),
        now=datetime(2026, 5, 24, 12, 1, tzinfo=UTC),
    )

    assert result.status == "rejected_preflight"
    assert result.reason_code == "hpke_decryption_failed"
