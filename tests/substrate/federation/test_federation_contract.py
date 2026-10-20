from __future__ import annotations

import copy

import pytest

from okg.substrate.exchange import generate_ed25519_keypair
from okg.substrate.exchange.readiness import public_key_fingerprint
from okg.substrate.federation import (
    DiscoveryDocument,
    FederationValidationError,
    LocalFederationRegistry,
    RouteRequest,
    build_pull_packet,
    validate_pull_packet,
)
from okg.substrate.federation.registry import (
    FEDERATION_GRAPH_PULL,
    FEDERATION_MESSAGE_SEND,
)


def _discovery() -> tuple[dict, str]:
    keypair = generate_ed25519_keypair("mit-sign")
    public_fingerprint = public_key_fingerprint(keypair.public_key)
    return (
        {
            "authority_id": "okg:mit-shared-okg-fixture",
            "display_label": "MIT Shared OKG Fixture",
            "keys": [
                {
                    "kid": keypair.kid,
                    "use": "sign",
                    "alg": "Ed25519",
                    "public_key": keypair.public_key,
                    "fingerprint": public_fingerprint,
                }
            ],
            "capabilities": [
                "federation.discovery",
                "federation.message.send",
                "federation.graph.pull",
            ],
            "boards": [
                {
                    "address": "okg://mit-shared-okg-fixture/boards/proposals",
                    "visibility": "public",
                    "message_profiles": ["proposal"],
                }
            ],
            "pull_surfaces": [
                {
                    "address": "okg://mit-shared-okg-fixture/pull/public-context",
                    "visibility": "public",
                    "graph_profile": "context",
                    "catalog_reference": "catalog:mit:v1",
                    "latest_generation_id": 42,
                    "max_packet_bytes": 100_000,
                    "redaction_policy_ref": "nomos:redaction:public",
                    "license": "CC-BY-4.0",
                }
            ],
            "mailboxes": [
                {
                    "address": "okg://mit-shared-okg-fixture/mailboxes/inbox",
                    "provider": "generic_queue_object",
                    "queue_ref": "https://mailbox.example/mit-shared/queue",
                    "object_ref": "s3://okg-mailbox/mit-shared/objects/",
                    "max_inline_bytes": 100_000,
                    "max_object_bytes": 1_000_000_000,
                    "retention_seconds": 1_209_600,
                    "ack_mode": "after_durable_receipt",
                    "encryption_required": True,
                }
            ],
            "endpoints": {"mailbox": "https://mailbox.example/mit-shared"},
            "object_refs": {"r2": "s3://okg-mailbox/mit-shared"},
            "visibility_tiers": ["public"],
            "latest_public_generation_id": 42,
            "catalog_reference": "catalog:mit:v1",
        },
        keypair.private_key,
    )


def test_discovery_hash_is_stable_and_tamper_sensitive() -> None:
    raw, _private_key = _discovery()
    doc = DiscoveryDocument.from_mapping(raw)
    same = DiscoveryDocument.from_mapping(copy.deepcopy(raw))
    assert doc.discovery_hash == same.discovery_hash
    assert doc.to_dict()["discovery_hash"] == doc.discovery_hash

    tampered = copy.deepcopy(raw)
    tampered["pull_surfaces"][0]["latest_generation_id"] = 43
    assert DiscoveryDocument.from_mapping(tampered).discovery_hash != doc.discovery_hash

    tampered = copy.deepcopy(raw)
    tampered["mailboxes"][0]["queue_ref"] = "https://mailbox.example/other/queue"
    assert DiscoveryDocument.from_mapping(tampered).discovery_hash != doc.discovery_hash


def test_discovery_rejects_secret_fields_and_duplicate_surfaces() -> None:
    raw, _private_key = _discovery()
    raw["postgres_dsn"] = "postgres://user:password@example/db"
    with pytest.raises(FederationValidationError, match="secret field"):
        DiscoveryDocument.from_mapping(raw)

    raw, _private_key = _discovery()
    raw["pull_surfaces"].append(copy.deepcopy(raw["pull_surfaces"][0]))
    with pytest.raises(FederationValidationError, match="duplicate pull surface"):
        DiscoveryDocument.from_mapping(raw)


def test_discovery_rejects_unsafe_mailbox_locators() -> None:
    raw, _private_key = _discovery()
    raw["mailboxes"][0]["queue_ref"] = (
        "https://mailbox.example/queue?token=do-not-publish"
    )
    with pytest.raises(FederationValidationError, match="embedded credentials"):
        DiscoveryDocument.from_mapping(raw)

    raw, _private_key = _discovery()
    raw["mailboxes"][0]["ack_mode"] = "delete_before_receipt"
    with pytest.raises(FederationValidationError, match="unknown mailbox ack mode"):
        DiscoveryDocument.from_mapping(raw)

    raw, _private_key = _discovery()
    raw["mailboxes"].append(copy.deepcopy(raw["mailboxes"][0]))
    with pytest.raises(FederationValidationError, match="duplicate mailbox"):
        DiscoveryDocument.from_mapping(raw)


def test_registry_discovery_does_not_grant_trust_and_pull_is_scoped() -> None:
    raw, _private_key = _discovery()
    doc = DiscoveryDocument.from_mapping(raw)
    registry = LocalFederationRegistry(local_authority_id="okg:personal:riley")
    registry.discover(doc)

    denied = registry.route_check(
        RouteRequest(
            local_authority_id="okg:personal:riley",
            remote_authority_id=doc.authority_id,
            subject="okg:personal:riley",
            action=FEDERATION_GRAPH_PULL,
            target_address="okg://mit-shared-okg-fixture/pull/public-context",
        )
    )
    assert denied.decision == "deny"
    assert denied.reason_code == "federation.missing_trust_pin"

    registry.accept_peer(
        authority_id=doc.authority_id,
        key_fingerprint=doc.keys[0].fingerprint or "",
        allowed_actions=(FEDERATION_GRAPH_PULL,),
        allowed_pull_surfaces=("okg://mit-shared-okg-fixture/pull/public-context",),
        nomos_decision_id="nomos:decision:allow-pull",
    )
    allowed = registry.route_check(
        RouteRequest(
            local_authority_id="okg:personal:riley",
            remote_authority_id=doc.authority_id,
            subject="okg:personal:riley",
            action=FEDERATION_GRAPH_PULL,
            target_address="okg://mit-shared-okg-fixture/pull/public-context",
        )
    )
    assert allowed.allowed
    assert allowed.nomos_decision_id == "nomos:decision:allow-pull"

    wrong_action = registry.route_check(
        RouteRequest(
            local_authority_id="okg:personal:riley",
            remote_authority_id=doc.authority_id,
            subject="okg:personal:riley",
            action=FEDERATION_MESSAGE_SEND,
            target_address="okg://mit-shared-okg-fixture/boards/proposals",
        )
    )
    assert wrong_action.decision == "deny"
    assert wrong_action.reason_code == "federation.action_not_scoped"


def test_pull_packet_validates_origin_generation_surface_and_signature() -> None:
    raw, private_key = _discovery()
    doc = DiscoveryDocument.from_mapping(raw)
    envelope = build_pull_packet(
        origin_authority_id=doc.authority_id,
        recipient_authority_id="okg:personal:riley",
        remote_generation_id=42,
        pull_surface_address="okg://mit-shared-okg-fixture/pull/public-context",
        catalog_reference="catalog:mit:v1",
        nodes=[{"node_id": "mit:course:6.830", "subtype": "course"}],
        edges=[],
        private_key=private_key,
        signing_key_id=doc.keys[0].kid,
        surface_discovery_hash=doc.discovery_hash,
    )
    validation = validate_pull_packet(
        envelope,
        public_key=doc.keys[0].public_key,
        expected_origin_authority_id=doc.authority_id,
        expected_recipient_authority_id="okg:personal:riley",
        expected_pull_surface_address="okg://mit-shared-okg-fixture/pull/public-context",
        expected_remote_generation_id=42,
        expected_catalog_reference="catalog:mit:v1",
    )
    assert validation.node_count == 1
    assert validation.edge_count == 0
    assert validation.remote_generation_id == 42

    tampered = copy.deepcopy(envelope)
    tampered["body"]["origin_generation_id"] = 43
    with pytest.raises(Exception):
        validate_pull_packet(
            tampered,
            public_key=doc.keys[0].public_key,
            expected_origin_authority_id=doc.authority_id,
            expected_recipient_authority_id="okg:personal:riley",
            expected_pull_surface_address="okg://mit-shared-okg-fixture/pull/public-context",
            expected_remote_generation_id=42,
            expected_catalog_reference="catalog:mit:v1",
        )

    with pytest.raises(FederationValidationError, match="surface mismatch"):
        validate_pull_packet(
            envelope,
            public_key=doc.keys[0].public_key,
            expected_origin_authority_id=doc.authority_id,
            expected_recipient_authority_id="okg:personal:riley",
            expected_pull_surface_address="okg://mit-shared-okg-fixture/pull/private-context",
            expected_remote_generation_id=42,
            expected_catalog_reference="catalog:mit:v1",
        )
