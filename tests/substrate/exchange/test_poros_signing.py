from __future__ import annotations

import copy
from datetime import UTC, datetime

import pytest

from okg.substrate.exchange.envelope import build_unsigned_envelope, verify_envelope
from okg.substrate.exchange.errors import PorosSignatureError
from okg.substrate.exchange.signing import derive_ed25519_public_key, sign_envelope


PRIVATE_KEY = bytes(range(32))
PUBLIC_KEY = derive_ed25519_public_key(PRIVATE_KEY)


def _signed_envelope() -> dict:
    unsigned = build_unsigned_envelope(
        sender="okg:lab-a",
        recipient="okg:mit",
        message_id="msg-fixed",
        created_at=datetime(2026, 5, 24, 12, 0, tzinfo=UTC),
        ttl_seconds=900,
        body={
            "graph_id": "poros:graph:lab-a:demo",
            "origin_authority_id": "okg:lab-a",
            "classification": "public",
            "nodes": [],
            "edges": [],
        },
    )
    return sign_envelope(unsigned, PRIVATE_KEY, kid="lab-a-sign-1")


def _tamper_signature(env: dict) -> None:
    sig = env["signature"]["sig"]
    replacement = "A" if sig[0] != "A" else "B"
    env["signature"]["sig"] = replacement + sig[1:]


def test_ed25519_golden_fixture_verifies() -> None:
    envelope = _signed_envelope()

    verify_envelope(envelope, PUBLIC_KEY)


@pytest.mark.parametrize(
    "mutator",
    [
        lambda env: env["body"].update({"graph_id": "poros:graph:tampered"}),
        lambda env: env.update({"sender": "okg:attacker"}),
        lambda env: env["signature"].update({"kid": "lab-a-sign-2"}),
        _tamper_signature,
    ],
)
def test_single_field_tamper_fails_signature(mutator) -> None:
    envelope = _signed_envelope()
    tampered = copy.deepcopy(envelope)
    mutator(tampered)

    with pytest.raises(PorosSignatureError):
        verify_envelope(tampered, PUBLIC_KEY)
