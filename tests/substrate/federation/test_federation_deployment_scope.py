from __future__ import annotations

import psycopg
import pytest

from okg.substrate.exchange.hashing import canonical_json_sha256_b64url
from okg.substrate.exchange import generate_ed25519_keypair
from okg.substrate.exchange.readiness import public_key_fingerprint
from okg.substrate.federation.connections import (
    PostgresFederationRegistry,
    connection_request_id,
    route_event_id,
)
from okg.substrate.federation.models import DiscoveryDocument
from okg.substrate.federation.registry import FEDERATION_MESSAGE_SEND


pytestmark = pytest.mark.postgres


def test_federation_registry_isolates_colliding_ids_by_deployment(
    clean_runtime: str,
) -> None:
    local_authority_id = "okg:personal:riley"
    remote_authority_id = "okg:mit-shared-okg-fixture"
    target_address = "okg://mit-shared-okg-fixture/boards/proposals"
    key_fingerprint = "sha256:shared-peer-key"
    decision_id = "nomos:decision:allow-shared-federation"
    document = DiscoveryDocument.from_mapping(_discovery())
    registry_a = PostgresFederationRegistry(
        clean_runtime,
        deployment_name="deployment-a",
    )
    registry_b = PostgresFederationRegistry(
        clean_runtime,
        deployment_name="deployment-b",
    )

    candidate_a = registry_a.discover(
        local_authority_id=local_authority_id,
        document=document,
    )
    candidate_b = registry_b.discover(
        local_authority_id=local_authority_id,
        document=document,
    )

    assert candidate_a.deployment_name == "deployment-a"
    assert candidate_b.deployment_name == "deployment-b"

    request_a = registry_a.create_request(
        local_authority_id=local_authority_id,
        requester_authority_id=local_authority_id,
        target_authority_id=remote_authority_id,
        requested_action=FEDERATION_MESSAGE_SEND,
        target_address=target_address,
        requested_visibility="public",
        key_fingerprint=key_fingerprint,
        discovery_hash=candidate_a.discovery_hash,
    )
    request_b = registry_b.create_request(
        local_authority_id=local_authority_id,
        requester_authority_id=local_authority_id,
        target_authority_id=remote_authority_id,
        requested_action=FEDERATION_MESSAGE_SEND,
        target_address=target_address,
        requested_visibility="public",
        key_fingerprint=key_fingerprint,
        discovery_hash=candidate_b.discovery_hash,
    )

    assert request_a.request_id != request_b.request_id
    assert request_a.deployment_name == "deployment-a"
    assert request_b.deployment_name == "deployment-b"
    assert registry_a.get_request(request_b.request_id) is None

    approved_a, pin_a = registry_a.approve_request(
        request_id=request_a.request_id,
        reviewer_identity="subject:operator",
        nomos_decision_id=decision_id,
        decision_evidence={"scope": "deployment-a"},
    )

    missing_b = registry_b.route_check(
        local_authority_id=local_authority_id,
        remote_authority_id=remote_authority_id,
        subject="subject:sender",
        action=FEDERATION_MESSAGE_SEND,
        target_address=target_address,
    )
    assert not missing_b.allowed
    assert missing_b.reason_code == "federation.missing_trust_pin"

    approved_b, pin_b = registry_b.approve_request(
        request_id=request_b.request_id,
        reviewer_identity="subject:operator",
        nomos_decision_id=decision_id,
        decision_evidence={"scope": "deployment-b"},
    )

    assert approved_a.nomos_decision_deployment_name == "deployment-a"
    assert approved_b.nomos_decision_deployment_name == "deployment-b"
    assert pin_a.deployment_name == "deployment-a"
    assert pin_b.deployment_name == "deployment-b"

    route_a = registry_a.route_check(
        local_authority_id=local_authority_id,
        remote_authority_id=remote_authority_id,
        subject="subject:sender",
        action=FEDERATION_MESSAGE_SEND,
        target_address=target_address,
    )
    route_b = registry_b.route_check(
        local_authority_id=local_authority_id,
        remote_authority_id=remote_authority_id,
        subject="subject:sender",
        action=FEDERATION_MESSAGE_SEND,
        target_address=target_address,
    )

    assert route_a.route_event_id != route_b.route_event_id
    assert route_a.allowed
    assert route_b.allowed
    assert route_a.nomos_decision_deployment_name == "deployment-a"
    assert route_b.nomos_decision_deployment_name == "deployment-b"

    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT deployment_name
              FROM okg.federation_peer_trust_pins
             WHERE local_authority_id = %s
               AND remote_authority_id = %s
               AND key_fingerprint = %s
             ORDER BY deployment_name
            """,
            (local_authority_id, remote_authority_id, key_fingerprint),
        )
        assert [row[0] for row in cur.fetchall()] == [
            "deployment-a",
            "deployment-b",
        ]
        cur.execute(
            """
            SELECT deployment_name
              FROM okg.nomos_decisions
             WHERE decision_id = %s
             ORDER BY deployment_name
            """,
            (decision_id,),
        )
        assert [row[0] for row in cur.fetchall()] == [
            "deployment-a",
            "deployment-b",
        ]

    _assert_decision_scope_check_violation(
        clean_runtime,
        """
        UPDATE okg.federation_connection_requests
           SET nomos_decision_deployment_name = 'deployment-b'
         WHERE deployment_name = 'deployment-a'
           AND request_id = %s
        """,
        (request_a.request_id,),
    )
    _assert_decision_scope_check_violation(
        clean_runtime,
        """
        UPDATE okg.federation_peer_trust_pins
           SET nomos_decision_deployment_name = 'deployment-b'
         WHERE deployment_name = 'deployment-a'
           AND local_authority_id = %s
           AND remote_authority_id = %s
           AND key_fingerprint = %s
        """,
        (local_authority_id, remote_authority_id, key_fingerprint),
    )
    _assert_decision_scope_check_violation(
        clean_runtime,
        """
        UPDATE okg.federation_route_events
           SET nomos_decision_deployment_name = 'deployment-b'
         WHERE deployment_name = 'deployment-a'
           AND route_event_id = %s
        """,
        (route_a.route_event_id,),
    )


def test_default_deployment_ids_keep_legacy_digest() -> None:
    request_payload = {
        "local_authority_id": "okg:local",
        "requester_authority_id": "okg:requester",
        "target_authority_id": "okg:target",
        "requested_action": FEDERATION_MESSAGE_SEND,
        "target_address": "okg://target/boards/proposals",
        "requested_visibility": "public",
        "key_fingerprint": "sha256:key",
        "discovery_hash": "sha256:discovery",
    }
    route_payload = {
        "local_authority_id": "okg:local",
        "remote_authority_id": "okg:remote",
        "subject": "subject:sender",
        "action": FEDERATION_MESSAGE_SEND,
        "target_address": "okg://target/boards/proposals",
        "visibility": "public",
        "decision": "allow",
        "reason_code": "federation.route_allowed_by_trust_pin",
    }

    legacy_request = (
        "federation:connection:"
        + canonical_json_sha256_b64url(request_payload)[:32]
    )
    legacy_route = (
        "federation:route:"
        + canonical_json_sha256_b64url(route_payload)[:32]
    )

    assert connection_request_id(
        deployment_name="default",
        **request_payload,
    ) == legacy_request
    assert route_event_id(deployment_name="default", **route_payload) == legacy_route
    assert connection_request_id(
        deployment_name="deployment-a",
        **request_payload,
    ) != legacy_request
    assert route_event_id(
        deployment_name="deployment-a",
        **route_payload,
    ) != legacy_route


def _assert_decision_scope_check_violation(
    dsn: str,
    statement: str,
    params: tuple[str, ...],
) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        with pytest.raises(psycopg.errors.CheckViolation):
            cur.execute(statement, params)


def _discovery() -> dict:
    keypair = generate_ed25519_keypair("mit-shared-scope")
    public_key = str(keypair.public_key)
    return {
        "authority_id": "okg:mit-shared-okg-fixture",
        "display_label": "MIT Shared OKG Fixture",
        "keys": [
            {
                "kid": str(keypair.kid),
                "public_key": public_key,
                "fingerprint": public_key_fingerprint(public_key),
            }
        ],
        "capabilities": ["federation.discovery", FEDERATION_MESSAGE_SEND],
        "boards": [
            {
                "address": "okg://mit-shared-okg-fixture/boards/proposals",
                "visibility": "public",
                "message_profiles": ["proposal"],
            }
        ],
        "pull_surfaces": [],
        "mailboxes": [],
        "endpoints": {},
        "visibility_tiers": ["public"],
        "latest_public_generation_id": 42,
        "catalog_reference": "catalog:mit:v1",
    }
