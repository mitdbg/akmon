from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from okg.substrate.external_actions.service import ExternalActionError, ExternalActionService
from okg.substrate.external_actions.signing import (
    SigningError,
    SigningKey,
    b64url_decode,
    b64url_encode,
    build_detached_envelope,
    canonical_json_bytes,
    domain_digest,
    generate_keypair,
    signed_input,
    verify_detached_envelope,
)


ROOT = Path(__file__).resolve().parents[3]
VECTORS = ROOT / "tests/fixtures/external_actions/hash-vectors.yaml"


def _fixture() -> tuple[dict[str, object], str, SigningKey]:
    document = yaml.safe_load(VECTORS.read_text(encoding="utf-8"))
    envelope = next(
        item
        for item in document["signed_envelope_vectors"]
        if item["id"] == "claim_handoff_receipt_envelope"
    )
    key_row = next(
        item
        for item in document["signing_key_fixtures"]
        if item["kid"] == envelope["protected_value"]["kid"]
    )
    key = SigningKey(
        kid=key_row["kid"],
        purpose="claim_and_restore_completion",
        algorithm="EdDSA",
        public_key_b64=key_row["public_key_b64"],
        state="current",
    )
    return dict(envelope["payload_value"]), key_row["private_seed_b64"], key


def _signer(private_key: str, key: SigningKey, *, mutate: bool = False):
    def sign(payload):
        signed_payload = dict(payload)
        if mutate:
            signed_payload["approved_payload_version"] += 1
        return build_detached_envelope(
            signed_payload,
            private_key,
            kid=key.kid,
            typ="okg-claim-handoff+jws",
            domain="okg-external-action-claim-handoff-receipt-envelope-v1",
        )

    return sign


def test_claim_handoff_is_verified_against_the_exact_closed_payload() -> None:
    payload, private_key, key = _fixture()
    service = ExternalActionService(
        handoff_signer=_signer(private_key, key),
        handoff_verification_key=key,
    )

    envelope = service._handoff_envelope(payload)

    assert set(envelope) == {
        "protected_b64",
        "payload_b64",
        "signature_b64",
        "envelope_digest",
    }


def test_claim_handoff_refuses_a_signer_that_changes_the_payload() -> None:
    payload, private_key, key = _fixture()
    service = ExternalActionService(
        handoff_signer=_signer(private_key, key, mutate=True),
        handoff_verification_key=key,
    )

    with pytest.raises(ExternalActionError, match="invalid envelope"):
        service._handoff_envelope(payload)


def test_claim_handoff_refuses_a_signature_from_an_unpinned_key() -> None:
    payload, _, key = _fixture()
    wrong_private, _ = generate_keypair()
    service = ExternalActionService(
        handoff_signer=_signer(wrong_private, key),
        handoff_verification_key=key,
    )

    with pytest.raises(ExternalActionError, match="invalid envelope"):
        service._handoff_envelope(payload)


def test_claim_handoff_refuses_an_unverifiable_signer_boundary() -> None:
    payload, private_key, key = _fixture()
    service = ExternalActionService(handoff_signer=_signer(private_key, key))

    with pytest.raises(ExternalActionError, match="signer is unavailable"):
        service._handoff_envelope(payload)


def test_restore_completion_is_signed_over_the_closed_database_projection() -> None:
    document = yaml.safe_load(VECTORS.read_text(encoding="utf-8"))
    vector = next(
        item for item in document["signed_envelope_vectors"]
        if item["id"] == "restore_completion_receipt_envelope"
    )
    key_row = next(
        item for item in document["signing_key_fixtures"]
        if item["kid"] == vector["protected_value"]["kid"]
    )
    key = SigningKey(
        kid=key_row["kid"],
        purpose="claim_and_restore_completion",
        algorithm="EdDSA",
        public_key_b64=key_row["public_key_b64"],
        state="current",
    )

    def signer(payload):
        return build_detached_envelope(
            payload,
            key_row["private_seed_b64"],
            kid=key.kid,
            typ="okg-restore-completion+jws",
            domain="okg-external-action-restore-completion-receipt-envelope-v1",
        )

    service = ExternalActionService(
        handoff_signer=signer,
        handoff_verification_key=key,
    )
    receipt = service._completion_envelope(vector["payload_value"])
    assert receipt["envelope_digest"] == vector["digest"]


@pytest.mark.parametrize("component", ["protected", "payload"])
def test_detached_jws_rejects_a_re_signed_noncanonical_component(component: str) -> None:
    """A valid Ed25519 signature cannot bypass the JCS wire contract."""

    private, public = generate_keypair()
    protected_value = {"alg": "EdDSA", "kid": "test/key", "typ": "test+jws"}
    payload_value = {"a": 1, "z": 2}
    protected_b64 = b64url_encode(canonical_json_bytes(protected_value))
    payload_b64 = b64url_encode(canonical_json_bytes(payload_value))
    if component == "protected":
        protected_b64 = b64url_encode(b'{"typ":"test+jws", "kid":"test/key", "alg":"EdDSA"}')
    else:
        payload_b64 = b64url_encode(b'{ "z": 2, "a": 1 }')
    signature = Ed25519PrivateKey.from_private_bytes(b64url_decode(private, field="private")).sign(
        signed_input(protected_b64, payload_b64)
    )
    unsigned = {
        "protected_b64": protected_b64,
        "payload_b64": payload_b64,
        "signature_b64": b64url_encode(signature),
    }
    envelope = {**unsigned, "envelope_digest": domain_digest("test-envelope-v1", unsigned)}
    with pytest.raises(SigningError, match="canonical JCS"):
        verify_detached_envelope(
            envelope,
            public,
            expected_kid="test/key",
            expected_typ="test+jws",
            expected_domain="test-envelope-v1",
        )
