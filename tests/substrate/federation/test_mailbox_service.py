from __future__ import annotations

import copy
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from okg.substrate.exchange import (
    build_unsigned_envelope,
    generate_ed25519_keypair,
    sign_envelope,
    verify_envelope,
)
from okg.substrate.exchange.canonical_json import canonical_json_bytes
from okg.substrate.exchange.envelope import format_timestamp
from okg.substrate.exchange.hashing import sha256_b64url
from okg.substrate.federation.mailbox import (
    FileObjectStore,
    InMemoryMailboxStore,
    MailboxService,
    MailboxValidationError,
    MemoryObjectStore,
)
from okg.substrate.federation.mailbox.api import build_mailbox_app
from okg.substrate.federation.mailbox.auth import static_token_authenticator

# Recipient principal credential for the authenticated mailbox HTTP surface.
_AUTHN = static_token_authenticator({"okg:research-lab": "tok"})
_AUTH_HEADERS = {
    "x-okg-mailbox-principal": "okg:research-lab",
    "x-okg-mailbox-token": "tok",
}


def _signed_envelope(*, message_id: str = "msg-inline") -> tuple[dict, str]:
    keypair = generate_ed25519_keypair("personal-sign")
    envelope = build_unsigned_envelope(
        sender="okg:personal:riley",
        recipient="okg:research-lab",
        message_id=message_id,
        ttl_seconds=300,
        body={
            "payload_kind": "federation.message.proposal.v1",
            "graph_id": "personal:dsg-proposal",
            "nodes": [
                {
                    "node_id": "person:riley",
                    "subtype": "person",
                    "name": "Riley",
                }
            ],
            "edges": [],
        },
    )
    return sign_envelope(envelope, keypair.private_key, kid=keypair.kid), (
        keypair.public_key
    )


def test_inline_message_poll_validate_ack_lifecycle() -> None:
    service = MailboxService(store=InMemoryMailboxStore())
    envelope, public_key = _signed_envelope()

    submitted = service.submit_envelope(envelope)
    assert submitted.packet_sha256 == "sha256:" + sha256_b64url(
        canonical_json_bytes(envelope)
    )
    duplicate = service.submit_envelope(copy.deepcopy(envelope))
    assert duplicate.message_id == submitted.message_id

    tampered = copy.deepcopy(envelope)
    tampered["body"]["graph_id"] = "personal:tampered"
    with pytest.raises(MailboxValidationError, match="different packet hash"):
        service.submit_envelope(tampered)

    leased = service.poll("okg:research-lab", lease_seconds=30)
    assert len(leased) == 1
    assert leased[0].lease_token
    assert leased[0].envelope is not None
    verify_envelope(dict(leased[0].envelope), public_key)

    acknowledged = service.ack_message(
        leased[0].message_id,
        lease_token=leased[0].lease_token or "",
        receipt_evidence={
            "receipt_event_id": "poros:receipt:1",
            "route_event_id": "federation:route:1",
            "nomos_decision_id": "nomos:decision:allow-receive",
        },
    )
    assert acknowledged.acknowledged
    assert service.poll("okg:research-lab") == []
    assert [event.event_type for event in service.store.events()] == [
        "submitted",
        "claimed",
        "acknowledged",
    ]


def test_detached_pointer_fetch_is_hash_and_size_verified() -> None:
    object_store = MemoryObjectStore()
    service = MailboxService(
        store=InMemoryMailboxStore(),
        object_store=object_store,
    )
    data = b"x" * 150_000
    stored = object_store.put_bytes("dsg-public-context", data)
    assert stored.sha256 == sha256_b64url(data)
    pointer = {
        "message_id": "msg-large",
        "sender_authority_id": "okg:personal:riley",
        "recipient_authority_id": "okg:research-lab",
        "object_uri": stored.uri,
        "object_sha256": "sha256:" + stored.sha256,
        "size_bytes": stored.size_bytes,
        "expires_at": format_timestamp(datetime.now(UTC) + timedelta(minutes=5)),
        "encryption": {"mode": "receiver-key"},
    }

    service.submit_pointer(pointer)
    leased = service.poll("okg:research-lab")
    assert len(leased) == 1
    assert service.fetch_detached_bytes(leased[0]) == data

    bad_pointer = dict(pointer)
    bad_pointer["message_id"] = "msg-large-tampered"
    bad_pointer["object_sha256"] = "not-the-real-hash"
    service.submit_pointer(bad_pointer)
    bad = service.poll("okg:research-lab")[0]
    with pytest.raises(MailboxValidationError, match="hash"):
        service.fetch_detached_bytes(bad)


def test_denied_message_is_final_and_not_reclaimed() -> None:
    service = MailboxService(store=InMemoryMailboxStore())
    envelope, _public_key = _signed_envelope(message_id="msg-deny")
    service.submit_envelope(envelope)
    leased = service.poll("okg:research-lab")[0]

    denied = service.deny_message(
        leased.message_id,
        lease_token=leased.lease_token or "",
        reason_code="nomos.denied",
        denial_evidence={
            "route_event_id": "federation:route:denied",
            "nomos_decision_id": "nomos:decision:deny-receive",
        },
    )
    assert denied.denied
    assert service.poll("okg:research-lab") == []
    with pytest.raises(MailboxValidationError, match="already final"):
        service.ack_message(
            leased.message_id,
            lease_token=leased.lease_token or "",
            receipt_evidence={"receipt_event_id": "late"},
        )


def test_fastapi_mailbox_surface_deposits_polls_and_acknowledges() -> None:
    pytest.importorskip("fastapi")
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient

    service = MailboxService(store=InMemoryMailboxStore())
    app = build_mailbox_app(
        service,
        discovery_document={"authority_id": "okg:dsg-mailbox"},
        authenticator=_AUTHN,
    )
    client = TestClient(app)
    envelope, _public_key = _signed_envelope(message_id="msg-api")

    submitted = client.post("/messages", json={"envelope": envelope})
    assert submitted.status_code == 202
    assert submitted.json()["message_id"] == "msg-api"

    polled = client.get(
        "/inbox/okg:research-lab", params={"lease_seconds": 30}, headers=_AUTH_HEADERS,
    )
    assert polled.status_code == 200
    message = polled.json()["messages"][0]
    assert message["lease_token"]
    assert message["envelope"]["message_id"] == "msg-api"

    fetched = client.get("/messages/msg-api", headers=_AUTH_HEADERS)
    assert fetched.status_code == 200
    assert "lease_token" not in fetched.json()

    acknowledged = client.post(
        "/messages/msg-api/ack",
        json={
            "lease_token": message["lease_token"],
            "receipt_evidence": {
                "receipt_event_id": "poros:receipt:api",
                "nomos_decision_id": "nomos:decision:api",
            },
        },
        headers=_AUTH_HEADERS,
    )
    assert acknowledged.status_code == 200
    assert acknowledged.json()["status"] == "acknowledged"


def test_fastapi_mailbox_surface_can_store_local_detached_object(
    tmp_path: Path,
) -> None:
    pytest.importorskip("fastapi")
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient

    service = MailboxService(
        store=InMemoryMailboxStore(),
        object_store=FileObjectStore(tmp_path / "objects"),
    )
    client = TestClient(build_mailbox_app(service, authenticator=_AUTHN))
    data = b"detached poros packet bytes"

    submitted = client.post(
        "/objects/msg-local-object",
        params={
            "sender_authority_id": "okg:personal:riley",
            "recipient_authority_id": "okg:research-lab",
            "expires_at": format_timestamp(datetime.now(UTC) + timedelta(minutes=5)),
            "metadata_json": '{"kind":"pilot"}',
        },
        content=data,
    )
    assert submitted.status_code == 202
    assert submitted.json()["payload_mode"] == "detached_pointer"
    assert service.health()["object_store_configured"] is True

    polled = client.get(
        "/inbox/okg:research-lab", params={"lease_seconds": 30}, headers=_AUTH_HEADERS,
    )
    message = polled.json()["messages"][0]
    assert message["pointer"]["object_uri"].startswith("poros-file://")
    assert message["pointer"]["object_sha256"].startswith("sha256:")

    fetched = client.get("/messages/msg-local-object/object", headers=_AUTH_HEADERS)
    assert fetched.status_code == 200
    assert fetched.content == data
