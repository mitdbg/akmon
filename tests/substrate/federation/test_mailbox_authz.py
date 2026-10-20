"""Mailbox recipient-authorization + lease binding (enforce-mailbox-poros-authz).

The mailbox read/finalize HTTP surface requires an authenticated principal bound
to the message's recipient authority: a caller authenticated as recipient A
cannot poll, read, fetch the object of, ack, or deny a message whose recipient
is B, and an unauthenticated caller is rejected (fail closed). At the store
layer, a leaked lease token alone (presented by the wrong principal) cannot
finalize a message.
"""
from __future__ import annotations


import pytest

from okg.substrate.exchange import build_unsigned_envelope, generate_ed25519_keypair, sign_envelope
from okg.substrate.federation.mailbox import (
    InMemoryMailboxStore,
    MailboxService,
    MailboxValidationError,
)
from okg.substrate.federation.mailbox.api import build_mailbox_app
from okg.substrate.federation.mailbox.auth import static_token_authenticator

pytest.importorskip("fastapi")
pytest.importorskip("httpx")
from fastapi.testclient import TestClient  # noqa: E402

_RECIPIENT_A = "okg:research-lab"
_RECIPIENT_B = "okg:other-lab"
_AUTHN = static_token_authenticator({_RECIPIENT_A: "tok-a", _RECIPIENT_B: "tok-b"})
_HDR_A = {"x-okg-mailbox-principal": _RECIPIENT_A, "x-okg-mailbox-token": "tok-a"}
_HDR_B = {"x-okg-mailbox-principal": _RECIPIENT_B, "x-okg-mailbox-token": "tok-b"}


def _envelope(*, message_id: str, recipient: str) -> dict:
    keypair = generate_ed25519_keypair("personal-sign")
    env = build_unsigned_envelope(
        sender="okg:personal:riley",
        recipient=recipient,
        message_id=message_id,
        ttl_seconds=300,
        body={"payload_kind": "federation.message.proposal.v1", "graph_id": "g", "nodes": [], "edges": []},
    )
    return sign_envelope(env, keypair.private_key, kid=keypair.kid)


def _service_and_client():
    service = MailboxService(store=InMemoryMailboxStore())
    service.submit_envelope(_envelope(message_id="msg-a", recipient=_RECIPIENT_A))
    client = TestClient(build_mailbox_app(service, authenticator=_AUTHN))
    return service, client


# ---- fail closed: no authenticator configured ----


def test_unconfigured_app_fails_closed() -> None:
    service = MailboxService(store=InMemoryMailboxStore())
    service.submit_envelope(_envelope(message_id="msg-a", recipient=_RECIPIENT_A))
    client = TestClient(build_mailbox_app(service))  # no authenticator
    assert client.get(f"/inbox/{_RECIPIENT_A}").status_code == 401
    assert client.get("/messages/msg-a").status_code == 401


# ---- cross-recipient denial across the whole read/finalize surface ----


def test_unauthenticated_requests_are_rejected() -> None:
    _service, client = _service_and_client()
    assert client.get(f"/inbox/{_RECIPIENT_A}").status_code == 401
    assert client.get("/messages/msg-a").status_code == 401
    assert client.post("/messages/msg-a/ack", json={}).status_code == 401


def test_principal_a_can_poll_and_read_own_message() -> None:
    _service, client = _service_and_client()
    polled = client.get(f"/inbox/{_RECIPIENT_A}", headers=_HDR_A)
    assert polled.status_code == 200
    assert polled.json()["messages"][0]["message_id"] == "msg-a"
    assert client.get("/messages/msg-a", headers=_HDR_A).status_code == 200


def test_principal_b_cannot_poll_a_inbox() -> None:
    _service, client = _service_and_client()
    # B authenticates as itself but requests A's inbox path -> 403.
    assert client.get(f"/inbox/{_RECIPIENT_A}", headers=_HDR_B).status_code == 403
    # B polling its OWN (empty) inbox is allowed but returns nothing of A's.
    own = client.get(f"/inbox/{_RECIPIENT_B}", headers=_HDR_B)
    assert own.status_code == 200
    assert own.json()["messages"] == []


def test_principal_b_cannot_read_or_finalize_a_message() -> None:
    service, client = _service_and_client()
    # Lease as A so the message is leased.
    leased = service.poll(_RECIPIENT_A)[0]
    assert client.get("/messages/msg-a", headers=_HDR_B).status_code == 403
    ack = client.post(
        "/messages/msg-a/ack",
        json={"lease_token": leased.lease_token, "receipt_evidence": {"x": "y"}},
        headers=_HDR_B,
    )
    assert ack.status_code == 403
    # The message was not acknowledged by the unauthorized attempt.
    assert service.get_message("msg-a").status == "leased"


# ---- store-level lease binding: leaked token alone cannot finalize ----


def test_leaked_lease_token_alone_cannot_finalize() -> None:
    service = MailboxService(store=InMemoryMailboxStore())
    service.submit_envelope(_envelope(message_id="msg-x", recipient=_RECIPIENT_A))
    leased = service.poll(_RECIPIENT_A)[0]
    token = leased.lease_token or ""

    # Same token, wrong principal -> rejected; the message stays leased.
    with pytest.raises(MailboxValidationError, match="not bound to this recipient"):
        service.ack_message(
            "msg-x",
            lease_token=token,
            receipt_evidence={"r": "1"},
            recipient_principal=_RECIPIENT_B,
        )
    assert service.get_message("msg-x").status == "leased"

    # Correct principal + token -> finalizes once.
    acked = service.ack_message(
        "msg-x",
        lease_token=token,
        receipt_evidence={"r": "1"},
        recipient_principal=_RECIPIENT_A,
    )
    assert acked.acknowledged
    # A second finalize is rejected as already final.
    with pytest.raises(MailboxValidationError, match="already final"):
        service.ack_message(
            "msg-x",
            lease_token=token,
            receipt_evidence={"r": "2"},
            recipient_principal=_RECIPIENT_A,
        )
