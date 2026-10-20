"""End-to-end demo for the high-level OKG connection lifecycle."""

from __future__ import annotations

import copy
import json
import tempfile
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from okg.substrate.exchange import (
    build_unsigned_envelope,
    generate_ed25519_keypair,
    sign_envelope,
    submit_poros_envelope_bytes,
)
from okg.substrate.exchange.canonical_json import canonical_json_bytes
from okg.substrate.exchange.readiness import public_key_fingerprint
from okg.substrate.federation.connections import (
    PostgresFederationRegistry,
)
from okg.substrate.federation.mailbox import MailboxService, PostgresMailboxStore
from okg.substrate.federation.mailbox.client import MailboxPollingClient
from okg.substrate.federation.mailbox.models import MailboxValidationError
from okg.substrate.federation.models import DiscoveryDocument
from okg.substrate.federation.registry import FEDERATION_MESSAGE_SEND
from okg.substrate.nomos import compile_policy_graph
from okg.substrate.db.connection import connect as db_connect


PERSONAL_AUTHORITY = "okg:personal:riley-demo"
SHARED_AUTHORITY = "okg:mit-shared-okg-fixture"
SHARED_URI = "okg://mit-shared-okg-fixture"
PROPOSAL_BOARD = "okg://mit-shared-okg-fixture/boards/proposals"
PRIVATE_BOARD = "okg://mit-shared-okg-fixture/boards/private"
MAILBOX_ADDRESS = "okg://mit-shared-okg-fixture/mailboxes/inbox"


def run_connection_lifecycle_demo(
    dsn: str,
    *,
    policy_dir: Path | str | None = None,
    mailbox_url: str | None = None,
    mailbox_client: MailboxPollingClient | None = None,
) -> dict[str, Any]:
    """Run a complete local OKG-to-OKG connection demo.

    The demo uses one Postgres database to simulate two OKGs. That keeps the
    evidence repeatable while still using the same durable registry, mailbox,
    Poros, Nomos, admission, publish, and proof paths as the real deployment.
    """
    if policy_dir is None:
        with tempfile.TemporaryDirectory(prefix="okg-connection-demo-") as tmp:
            return _run_connection_lifecycle_demo(
                dsn,
                policy_dir=Path(tmp),
                mailbox_url=mailbox_url,
                mailbox_client=mailbox_client,
            )
    return _run_connection_lifecycle_demo(
        dsn,
        policy_dir=Path(policy_dir),
        mailbox_url=mailbox_url,
        mailbox_client=mailbox_client,
    )


def _run_connection_lifecycle_demo(
    dsn: str,
    *,
    policy_dir: Path,
    mailbox_url: str | None,
    mailbox_client: MailboxPollingClient | None,
) -> dict[str, Any]:
    run_id = uuid.uuid4().hex[:10]
    sender_keys = generate_ed25519_keypair(f"personal-demo-{run_id}")
    receiver_keys = generate_ed25519_keypair(f"shared-demo-{run_id}")
    document = DiscoveryDocument.from_mapping(_discovery_document(receiver_keys))
    registry = PostgresFederationRegistry(dsn)

    candidate = registry.discover(
        local_authority_id=PERSONAL_AUTHORITY,
        document=document,
    )
    unapproved_route = registry.route_check(
        local_authority_id=PERSONAL_AUTHORITY,
        remote_authority_id=SHARED_AUTHORITY,
        subject=PERSONAL_AUTHORITY,
        action=FEDERATION_MESSAGE_SEND,
        target_address=PROPOSAL_BOARD,
    )
    request = registry.create_request(
        local_authority_id=PERSONAL_AUTHORITY,
        requester_authority_id=PERSONAL_AUTHORITY,
        target_authority_id=SHARED_AUTHORITY,
        requested_action=FEDERATION_MESSAGE_SEND,
        target_address=PROPOSAL_BOARD,
        requested_visibility="public",
        key_fingerprint=document.keys[0].fingerprint or "",
        discovery_hash=document.discovery_hash,
    )
    approval_decision_id = f"nomos:decision:connection-demo:{run_id}"
    approved_request, trust_pin = registry.approve_request(
        request_id=request.request_id,
        reviewer_identity="okg:mit-demo-curator",
        nomos_decision_id=approval_decision_id,
        decision_evidence={
            "reason_code": "connection_demo_scoped_board_send_allowed",
        },
    )
    allowed_route = registry.route_check(
        local_authority_id=PERSONAL_AUTHORITY,
        remote_authority_id=SHARED_AUTHORITY,
        subject=PERSONAL_AUTHORITY,
        action=FEDERATION_MESSAGE_SEND,
        target_address=PROPOSAL_BOARD,
    )
    out_of_scope_route = registry.route_check(
        local_authority_id=PERSONAL_AUTHORITY,
        remote_authority_id=SHARED_AUTHORITY,
        subject=PERSONAL_AUTHORITY,
        action=FEDERATION_MESSAGE_SEND,
        target_address=PRIVATE_BOARD,
    )

    accepted_message_id = f"connection-demo-{run_id}-accepted"
    tampered_message_id = f"connection-demo-{run_id}-tampered"
    expired_message_id = f"connection-demo-{run_id}-expired"
    accepted = _signed_envelope(
        sender_keys,
        message_id=accepted_message_id,
        classification="public",
        text="Connection demo proposal: add this public DSG context packet.",
    )
    accepted_node_id = str(accepted["body"]["root_node_ids"][0])
    tampered = copy.deepcopy(accepted)
    tampered["message_id"] = tampered_message_id
    tampered["body"]["nodes"][0]["attrs"]["text"] = "Tampered after signing."
    expired = _signed_envelope(
        sender_keys,
        message_id=expired_message_id,
        classification="public",
        text="Expired packet should not be delivered.",
        ttl_seconds=-30,
    )

    policy_path = _write_allow_public_context_policy(policy_dir)
    trust_pins = {
        "pins": [
            {
                "authority_id": PERSONAL_AUTHORITY,
                "kid": sender_keys.kid,
                "use": "sign",
                "alg": "Ed25519",
                "public_key": sender_keys.public_key,
            }
        ]
    }
    before = {"accepted_live_count": _live_count(dsn, accepted_node_id)}
    if mailbox_client is not None or mailbox_url is not None:
        transport = _HttpMailboxTransport(
            mailbox_client or MailboxPollingClient(str(mailbox_url))
        )
        mailbox_transport = {
            "mode": "http",
            "url": mailbox_url or getattr(mailbox_client, "base_url", None),
        }
        accepted_delivery, tampered_delivery, expired_delivery = _run_mailbox_transport(
            transport,
            accepted=accepted,
            tampered=tampered,
            expired=expired,
            trust_pins=trust_pins,
            receiver_keys=receiver_keys.__dict__,
            dsn=dsn,
            policy_path=policy_path,
        )
    else:
        with db_connect(dsn) as conn:
            PostgresMailboxStore.install_schema(conn)
            transport = _LocalMailboxTransport(
                MailboxService(store=PostgresMailboxStore(conn))
            )
            mailbox_transport = {"mode": "local_postgres"}
            accepted_delivery, tampered_delivery, expired_delivery = (
                _run_mailbox_transport(
                    transport,
                    accepted=accepted,
                    tampered=tampered,
                    expired=expired,
                    trust_pins=trust_pins,
                    receiver_keys=receiver_keys.__dict__,
                    dsn=dsn,
                    policy_path=policy_path,
                )
            )
    oversized_delivery = _oversized_denial(accepted)

    proof = _poros_proof(
        dsn=dsn,
        message_id=accepted_message_id,
    )
    disabled_pin = registry.disable_peer(
        local_authority_id=PERSONAL_AUTHORITY,
        remote_authority_id=SHARED_AUTHORITY,
    )
    disabled_route = registry.route_check(
        local_authority_id=PERSONAL_AUTHORITY,
        remote_authority_id=SHARED_AUTHORITY,
        subject=PERSONAL_AUTHORITY,
        action=FEDERATION_MESSAGE_SEND,
        target_address=PROPOSAL_BOARD,
    )
    after = {"accepted_live_count": _live_count(dsn, accepted_node_id)}

    return {
        "status": "ok",
        "run_id": run_id,
        "story": (
            "personal OKG discovered a shared OKG, requested scoped board "
            "access, received approval, sent a signed graph packet through "
            "a mailbox, and the shared OKG published the admitted fact."
        ),
        "mailbox_transport": mailbox_transport,
        "authorities": {
            "personal": PERSONAL_AUTHORITY,
            "shared": SHARED_AUTHORITY,
        },
        "addresses": {
            "shared": SHARED_URI,
            "proposal_board": PROPOSAL_BOARD,
            "private_board": PRIVATE_BOARD,
            "mailbox": MAILBOX_ADDRESS,
        },
        "discovery": {
            "candidate": candidate.to_dict(),
            "trust_granted": False,
        },
        "connection": {
            "unapproved_route": unapproved_route.to_dict(),
            "request": request.to_dict(),
            "approved_request": approved_request.to_dict(),
            "trust_pin": trust_pin.to_dict(),
            "allowed_route": allowed_route.to_dict(),
            "out_of_scope_route": out_of_scope_route.to_dict(),
            "disabled_pin": disabled_pin.to_dict(),
            "disabled_route": disabled_route.to_dict(),
        },
        "transfer": {
            "message_id": accepted_message_id,
            "node_id": accepted_node_id,
            "before": before,
            "accepted": accepted_delivery,
            "after": after,
            "proof": _proof_summary(proof),
        },
        "denials": {
            "unapproved": {
                "route": unapproved_route.to_dict(),
                "live_row_delta": dict(unapproved_route.live_row_delta),
            },
            "out_of_scope": {
                "route": out_of_scope_route.to_dict(),
                "live_row_delta": dict(out_of_scope_route.live_row_delta),
            },
            "disabled": {
                "route": disabled_route.to_dict(),
                "live_row_delta": dict(disabled_route.live_row_delta),
            },
            "tampered": tampered_delivery,
            "expired": expired_delivery,
            "oversized": oversized_delivery,
        },
        "policy_bundle": str(policy_path),
    }


def _discovery_document(receiver_keys: Any) -> dict[str, Any]:
    return {
        "authority_id": SHARED_AUTHORITY,
        "display_label": "MIT Shared OKG Fixture",
        "keys": [
            {
                "kid": receiver_keys.kid,
                "public_key": receiver_keys.public_key,
                "fingerprint": public_key_fingerprint(receiver_keys.public_key),
            }
        ],
        "capabilities": [
            "federation.discovery",
            "federation.message.send",
            "federation.graph.pull",
        ],
        "boards": [
            {
                "address": PROPOSAL_BOARD,
                "visibility": "public",
                "message_profiles": ["proposal"],
            }
        ],
        "mailboxes": [
            {
                "address": MAILBOX_ADDRESS,
                "queue_ref": "https://mailbox.example/mit-shared",
                "max_inline_bytes": 100_000,
                "max_object_bytes": 1_000_000_000,
            }
        ],
        "endpoints": {"mailbox": "https://mailbox.example/mit-shared"},
        "visibility_tiers": ["public"],
        "latest_public_generation_id": 1,
    }


def _signed_envelope(
    sender_keys: Any,
    *,
    message_id: str,
    classification: str,
    text: str,
    ttl_seconds: int = 900,
) -> dict[str, Any]:
    unsigned = build_unsigned_envelope(
        sender=PERSONAL_AUTHORITY,
        recipient=SHARED_AUTHORITY,
        message_id=message_id,
        created_at=datetime.now(UTC).replace(microsecond=0),
        ttl_seconds=ttl_seconds,
        body=_graph_body(message_id, classification=classification, text=text),
    )
    return sign_envelope(unsigned, sender_keys.private_key, kid=sender_keys.kid)


def _graph_body(
    message_id: str,
    *,
    classification: str,
    text: str,
) -> dict[str, Any]:
    node_id = f"poros:connection-demo:{message_id}"
    return {
        "graph_id": f"poros:graph:connection-demo:{message_id}",
        "origin_authority_id": PERSONAL_AUTHORITY,
        "origin_generation_id": 1,
        "classification": classification,
        "graph_profile": "context",
        "root_node_ids": [node_id],
        "nodes": [
            {
                "node_id": node_id,
                "subtype": "context_packet",
                "attrs": {"text": text},
            }
        ],
        "edges": [],
    }


class _MailboxTransport:
    def submit_envelope(self, envelope: Mapping[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    def poll(
        self,
        recipient_authority_id: str,
        *,
        limit: int = 10,
        lease_seconds: int = 60,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    def ack(
        self,
        message_id: str,
        *,
        lease_token: str,
        receipt_evidence: Mapping[str, Any],
    ) -> dict[str, Any]:
        raise NotImplementedError

    def deny(
        self,
        message_id: str,
        *,
        lease_token: str,
        reason_code: str,
        denial_evidence: Mapping[str, Any],
    ) -> dict[str, Any]:
        raise NotImplementedError

    def get_message(self, message_id: str) -> dict[str, Any]:
        raise NotImplementedError


class _LocalMailboxTransport(_MailboxTransport):
    def __init__(self, service: MailboxService) -> None:
        self.service = service

    def submit_envelope(self, envelope: Mapping[str, Any]) -> dict[str, Any]:
        return self.service.submit_envelope(envelope).to_dict(include_payload=False)

    def poll(
        self,
        recipient_authority_id: str,
        *,
        limit: int = 10,
        lease_seconds: int = 60,
    ) -> list[dict[str, Any]]:
        return [
            record.to_dict(include_payload=True)
            for record in self.service.poll(
                recipient_authority_id,
                limit=limit,
                lease_seconds=lease_seconds,
            )
        ]

    def ack(
        self,
        message_id: str,
        *,
        lease_token: str,
        receipt_evidence: Mapping[str, Any],
    ) -> dict[str, Any]:
        return self.service.ack_message(
            message_id,
            lease_token=lease_token,
            receipt_evidence=receipt_evidence,
        ).to_dict(include_payload=False)

    def deny(
        self,
        message_id: str,
        *,
        lease_token: str,
        reason_code: str,
        denial_evidence: Mapping[str, Any],
    ) -> dict[str, Any]:
        return self.service.deny_message(
            message_id,
            lease_token=lease_token,
            reason_code=reason_code,
            denial_evidence=denial_evidence,
        ).to_dict(include_payload=False)

    def get_message(self, message_id: str) -> dict[str, Any]:
        record = self.service.get_message(message_id)
        if record is None:
            raise RuntimeError(f"mailbox record missing for {message_id}")
        return record.to_dict(include_payload=False)


class _HttpMailboxTransport(_MailboxTransport):
    def __init__(self, client: MailboxPollingClient) -> None:
        self.client = client

    def submit_envelope(self, envelope: Mapping[str, Any]) -> dict[str, Any]:
        return self.client.submit_envelope(envelope)

    def poll(
        self,
        recipient_authority_id: str,
        *,
        limit: int = 10,
        lease_seconds: int = 60,
    ) -> list[dict[str, Any]]:
        return self.client.poll(
            recipient_authority_id,
            limit=limit,
            lease_seconds=lease_seconds,
        )

    def ack(
        self,
        message_id: str,
        *,
        lease_token: str,
        receipt_evidence: Mapping[str, Any],
    ) -> dict[str, Any]:
        return self.client.ack(
            message_id,
            lease_token=lease_token,
            receipt_evidence=receipt_evidence,
        )

    def deny(
        self,
        message_id: str,
        *,
        lease_token: str,
        reason_code: str,
        denial_evidence: Mapping[str, Any],
    ) -> dict[str, Any]:
        return self.client.deny(
            message_id,
            lease_token=lease_token,
            reason_code=reason_code,
            denial_evidence=denial_evidence,
        )

    def get_message(self, message_id: str) -> dict[str, Any]:
        return _without_payload(self.client.get_message(message_id))


def _run_mailbox_transport(
    transport: _MailboxTransport,
    *,
    accepted: Mapping[str, Any],
    tampered: Mapping[str, Any],
    expired: Mapping[str, Any],
    trust_pins: Mapping[str, Any],
    receiver_keys: Mapping[str, Any],
    dsn: str,
    policy_path: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    accepted_delivery = _submit_poll_process(
        transport,
        accepted,
        trust_pins=trust_pins,
        receiver_keys=receiver_keys,
        dsn=dsn,
        policy_path=policy_path,
    )
    tampered_delivery = _submit_poll_process(
        transport,
        tampered,
        trust_pins=trust_pins,
        receiver_keys=receiver_keys,
        dsn=dsn,
        policy_path=policy_path,
    )
    expired_message_id = str(expired["message_id"])
    transport.submit_envelope(expired)
    expired_polled = transport.poll(SHARED_AUTHORITY, limit=100, lease_seconds=120)
    delivered_expired = [
        item for item in expired_polled if _message_id(item) == expired_message_id
    ]
    return (
        accepted_delivery,
        tampered_delivery,
        {
            "message_id": expired_message_id,
            "mailbox_action": "not_delivered_expired"
            if not delivered_expired
            else "unexpectedly_delivered_expired",
            "polled": len(delivered_expired),
            "live_row_delta": {"nodes_live": 0, "edges_live": 0},
            "mailbox": transport.get_message(expired_message_id),
        },
    )


def _submit_poll_process(
    transport: "_MailboxTransport",
    envelope: Mapping[str, Any],
    *,
    trust_pins: Mapping[str, Any],
    receiver_keys: Mapping[str, Any],
    dsn: str,
    policy_path: Path,
) -> dict[str, Any]:
    message_id = str(envelope["message_id"])
    transport.submit_envelope(envelope)
    leased = transport.poll(SHARED_AUTHORITY, limit=100, lease_seconds=120)
    try:
        lease = next(item for item in leased if _message_id(item) == message_id)
    except StopIteration as exc:
        raise RuntimeError(f"submitted message was not leased: {message_id}") from exc
    outcome = submit_poros_envelope_bytes(
        canonical_json_bytes(dict(_envelope(lease))),
        trust_pins=dict(trust_pins),
        receiver_keys=dict(receiver_keys),
        receiver=SHARED_AUTHORITY,
        dsn=dsn,
        deployment="default",
        admission_mode="fast_track",
        persist_preflight=True,
        apply=True,
        publish=True,
        nomos_bundle=policy_path,
    )
    if outcome.exit_code == 0:
        transport.ack(
            message_id,
            lease_token=_lease_token(lease),
            receipt_evidence=_receipt_evidence(outcome.payload),
        )
        mailbox_action = "acknowledged"
        reason_code = None
    else:
        reason_code = _denial_reason(outcome.payload)
        transport.deny(
            message_id,
            lease_token=_lease_token(lease),
            reason_code=reason_code,
            denial_evidence=_denial_evidence(outcome.payload),
        )
        mailbox_action = "denied"
    return {
        "message_id": message_id,
        "mailbox_action": mailbox_action,
        "reason_code": reason_code,
        "poros_status": outcome.payload.get("status"),
        "nomos_decision": _decision_summary(outcome.payload.get("nomos_decision")),
        "admission": outcome.payload.get("admission"),
        "publish": outcome.payload.get("publish"),
        "live_counts": outcome.payload.get("live_counts"),
        "live_row_delta": outcome.payload.get("live_counts")
        or {"nodes_live": 0, "edges_live": 0},
        "mailbox": transport.get_message(message_id),
        "incorporation_events": _incorporation_events(dsn, message_id),
    }


def _message_id(message: Mapping[str, Any]) -> str:
    value = message.get("message_id")
    if not isinstance(value, str) or not value:
        raise RuntimeError("mailbox message did not include message_id")
    return value


def _lease_token(message: Mapping[str, Any]) -> str:
    value = message.get("lease_token")
    if not isinstance(value, str) or not value:
        raise RuntimeError("mailbox message did not include lease_token")
    return value


def _envelope(message: Mapping[str, Any]) -> Mapping[str, Any]:
    value = message.get("envelope")
    if not isinstance(value, Mapping):
        raise RuntimeError("mailbox message did not include inline envelope")
    return value


def _without_payload(message: Mapping[str, Any]) -> dict[str, Any]:
    redacted = dict(message)
    redacted.pop("envelope", None)
    redacted.pop("pointer", None)
    redacted.pop("lease_token", None)
    redacted.pop("lease_expires_at", None)
    return redacted


def _oversized_denial(envelope: Mapping[str, Any]) -> dict[str, Any]:
    service = MailboxService(
        store=PostgresMailboxStore(_NullConnection()),
        max_inline_bytes=1,
    )
    try:
        service.submit_envelope(envelope)
    except MailboxValidationError as exc:
        return {
            "mailbox_action": "denied_before_store",
            "reason_code": "mailbox_inline_payload_oversized",
            "error": str(exc),
            "live_row_delta": {"nodes_live": 0, "edges_live": 0},
        }
    raise RuntimeError("oversized packet unexpectedly accepted")


class _NullConnection:
    def cursor(self) -> Any:  # pragma: no cover - should never be reached.
        raise RuntimeError("oversized demo should fail before touching storage")


def _write_allow_public_context_policy(out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    policy_graph = {
        "policy_bundle_id": "nomos:test:connection-lifecycle-demo",
        "policy_version": "v1",
        "source_generation_id": 0,
        "scope": {"deployment": "default"},
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_personal_public_context",
                "effect": "allow",
                "priority": 100,
                "subjects": [PERSONAL_AUTHORITY],
                "actions": ["poros.graph.submit"],
                "match": {
                    "classification": "public",
                    "graph_profile": "context",
                    "root_subtypes": ["context_packet"],
                    "requested_incorporation_mode": "fast_track",
                },
                "reason_code": "nomos.test.connection_demo.public_context_allowed",
            }
        ],
    }
    bundle = compile_policy_graph(policy_graph).to_dict()
    path = out_dir / "compiled-policy.json"
    path.write_text(
        json.dumps(bundle, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def _receipt_evidence(payload: Mapping[str, Any]) -> dict[str, Any]:
    preflight = _mapping(payload.get("preflight"))
    admission = _mapping(payload.get("admission"))
    publish = _mapping(payload.get("publish"))
    decision = _mapping(payload.get("nomos_decision"))
    return {
        "poros_status": payload.get("status"),
        "poros_inbox_id": preflight.get("inbox_id"),
        "receipt_sha256": preflight.get("receipt_sha256"),
        "nomos_decision_id": decision.get("decision_id"),
        "admission_id": admission.get("admission_id"),
        "receiver_generation_id": publish.get("generation_id"),
        "live_counts": payload.get("live_counts"),
    }


def _denial_evidence(payload: Mapping[str, Any]) -> dict[str, Any]:
    decision = _mapping(payload.get("nomos_decision"))
    return {
        "poros_status": payload.get("status"),
        "reason_code": _denial_reason(payload),
        "preflight": _mapping(payload.get("preflight")),
        "nomos_decision": {
            "decision_id": decision.get("decision_id"),
            "effect": decision.get("effect"),
            "reason_code": decision.get("reason_code"),
        },
        "error": payload.get("error") or payload.get("mutation_error"),
    }


def _denial_reason(payload: Mapping[str, Any]) -> str:
    decision = _mapping(payload.get("nomos_decision"))
    if decision.get("reason_code"):
        return str(decision["reason_code"])
    if payload.get("reason_code"):
        return str(payload["reason_code"])
    return "poros_connection_demo_denied"


def _decision_summary(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    return {
        "decision_id": value.get("decision_id"),
        "effect": value.get("effect"),
        "reason_code": value.get("reason_code"),
        "policy_bundle_id": value.get("policy_bundle_id"),
    }


def _poros_proof(*, dsn: str, message_id: str) -> dict[str, Any]:
    from okg.substrate.cli.poros import _load_poros_proof

    with db_connect(dsn, autocommit=True) as conn:
        return _load_poros_proof(
            conn,
            message_id=message_id,
            graph_id=None,
            body_sha256=None,
            mode="audit",
        )


def _proof_summary(proof: Mapping[str, Any]) -> dict[str, Any]:
    graph = _mapping(proof.get("graph"))
    return {
        "message_id": proof.get("message_id"),
        "state": proof.get("state"),
        "body_sha256": proof.get("body_sha256"),
        "graph": {
            "graph_id": graph.get("graph_id"),
            "node_count": graph.get("node_count"),
            "edge_count": graph.get("edge_count"),
        },
        "lifecycle": _mapping(proof.get("lifecycle")),
    }


def _incorporation_events(dsn: str, message_id: str) -> list[dict[str, Any]]:
    with db_connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT event_type, status, reason_code, admission_id,
                   receiver_generation_id
              FROM okg.poros_incorporation_events
             WHERE message_id = %s
             ORDER BY event_id
            """,
            (message_id,),
        )
        rows = cur.fetchall()
    return [
        {
            "event_type": row[0],
            "status": row[1],
            "reason_code": row[2],
            "admission_id": str(row[3]) if row[3] is not None else None,
            "receiver_generation_id": row[4],
        }
        for row in rows
    ]


def _live_count(dsn: str, node_id: str) -> int:
    from okg.substrate.graph_store import ChronosGraphStore

    # ensure_schema=True: the "before" snapshot can run prior to the
    # first publish that initializes the Chronos branch metadata, so
    # ensure the store/branch exist (idempotent) before checkout.
    store = ChronosGraphStore.connect(dsn)
    try:
        rows = store.checkout().query(
            """
            SELECT count(*) AS n
              FROM okg.graph_nodes
             WHERE node_id = :node_id
            """,
            {"node_id": node_id},
        )
        return int(rows[0]["n"] or 0)
    finally:
        store.close()


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}
