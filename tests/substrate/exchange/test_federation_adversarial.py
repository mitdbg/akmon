"""Permanent forgery/replay adversarial suite for Poros federation admission.

req.exchange-admission.adversarial-suite: prove rejection of
  * a replayed, previously-accepted envelope;
  * an envelope whose canonical form was mutated after signing;
  * a signature from a key outside the trust anchor set;
  * a downgrade attempt that strips the signature to hit the compat path
    without the manifest opt-in.

The first three are inbox-level (crypto) rejections and need no database. The
downgrade case has two faces, both covered: stripping the *envelope* signature
(the inbox refuses before admission) and presenting an edge-bearing graph that
reaches admission with an empty producer output signature (``compat``) and is
refused there without the manifest opt-in.
"""

from __future__ import annotations

import copy
from datetime import UTC, datetime

import psycopg
import pytest

from okg.substrate.exchange import (
    PorosInbox,
    TrustPin,
    TrustPinRegistry,
    build_unsigned_envelope,
    derive_ed25519_public_key,
    sign_envelope,
    submit_poros_candidate_to_admission,
)
from okg.substrate.exchange.canonical_json import canonical_json_bytes
from okg.substrate.exchange.constants import LEGACY_BUNDLE_SUBMIT_TYPE

SENDER_PRIVATE = b"\x51" * 32
SENDER_PUBLIC = derive_ed25519_public_key(SENDER_PRIVATE)
ATTACKER_PRIVATE = b"\x99" * 32
ATTACKER_PUBLIC = derive_ed25519_public_key(ATTACKER_PRIVATE)
RECEIVER_PRIVATE = b"\x52" * 32

KID = "lab-a-sign-1"


def _trust_registry(public_key: str = SENDER_PUBLIC) -> TrustPinRegistry:
    return TrustPinRegistry.from_pins(
        [TrustPin(authority_id="okg:lab-a", kid=KID, public_key=public_key)]
    )


def _inbox(
    *, public_key: str = SENDER_PUBLIC, replay_cache=None
) -> PorosInbox:
    return PorosInbox(
        receiver_authority_id="okg:mit",
        trust_registry=_trust_registry(public_key),
        receiver_private_key=RECEIVER_PRIVATE,
        receiver_signing_kid="mit-sign-1",
        replay_cache=replay_cache,
    )


def _signed_envelope(
    message_id: str,
    *,
    private_key: bytes = SENDER_PRIVATE,
    body: dict | None = None,
    message_type: str = "graph.submit",
) -> dict:
    node_id = f"poros:test:{message_id}"
    unsigned = build_unsigned_envelope(
        sender="okg:lab-a",
        recipient="okg:mit",
        message_id=message_id,
        created_at=datetime.now(UTC),
        ttl_seconds=900,
        message_type=message_type,
        body=body
        or {
            "graph_id": f"poros:graph:{message_id}",
            "origin_authority_id": "okg:lab-a",
            "origin_generation_id": 1,
            "classification": "public",
            "graph_profile": "context",
            "root_node_ids": [node_id],
            "nodes": [
                {
                    "node_id": node_id,
                    "subtype": "context_packet",
                    "attrs": {"text": "adversarial"},
                }
            ],
            "edges": [],
        },
    )
    return sign_envelope(unsigned, private_key, kid=KID)


def test_replayed_envelope_is_rejected_as_duplicate() -> None:
    envelope = _signed_envelope("adv-replay-1")
    raw = canonical_json_bytes(envelope)
    inbox = _inbox()
    now = datetime.now(UTC)

    first = inbox.handle_bytes(raw, now=now)
    assert first.status == "accepted_for_admission"

    second = inbox.handle_bytes(raw, now=now)
    assert second.status == "duplicate"
    assert second.reason_code == "duplicate"
    assert second.admission_candidate is None


def test_envelope_mutated_after_signing_is_rejected() -> None:
    envelope = _signed_envelope("adv-mutate-1")
    # Mutate a signed field after signing. The signature no longer verifies
    # over the canonical form.
    tampered = copy.deepcopy(envelope)
    tampered["recipient"] = "okg:mit"  # unchanged; mutate the body instead
    tampered["body"]["nodes"][0]["attrs"]["text"] = "tampered"

    result = _inbox().handle_bytes(canonical_json_bytes(tampered))
    assert result.status == "rejected_preflight"
    # Either the signature fails (body bytes changed) or the body-hash check
    # fires — both are fail-closed refusals, never acceptance.
    assert result.reason_code in {"invalid_signature", "body_hash_mismatch"}
    assert result.admission_candidate is None


def test_signature_from_key_outside_trust_anchor_is_rejected() -> None:
    # Signed by the real sender key, but the receiver pins DIFFERENT key
    # material for that (authority, kid): the signature is outside the anchor.
    envelope = _signed_envelope("adv-wrongkey-1")
    inbox = _inbox(public_key=ATTACKER_PUBLIC)

    result = inbox.handle_bytes(canonical_json_bytes(envelope))
    assert result.status == "rejected_preflight"
    assert result.reason_code == "invalid_signature"
    assert result.admission_candidate is None


def test_forged_signature_from_untrusted_key_is_rejected() -> None:
    # Attacker signs with its own key; the receiver pins only the real sender.
    envelope = _signed_envelope("adv-forge-1", private_key=ATTACKER_PRIVATE)

    result = _inbox().handle_bytes(canonical_json_bytes(envelope))
    assert result.status == "rejected_preflight"
    assert result.reason_code == "invalid_signature"
    assert result.admission_candidate is None


def test_signature_stripped_envelope_is_rejected_before_admission() -> None:
    envelope = _signed_envelope("adv-strip-1")
    stripped = copy.deepcopy(envelope)
    stripped.pop("signature", None)

    result = _inbox().handle_bytes(canonical_json_bytes(stripped))
    assert result.status == "rejected_preflight"
    # Removing the signature object trips the signed-kid binding check first
    # (the top-level signing_key_id has no matching signature.kid); either way
    # the envelope is refused before admission, never accepted.
    assert result.reason_code in {"missing_signature", "missing_signature_kid"}
    assert result.admission_candidate is None


# --- compat downgrade reaches admission and is refused without opt-in -------


def _compat_candidate(message_id: str):
    """An edge-bearing bundle → empty output signature → compat at admission."""
    a = f"poros:test:advcompat:{message_id}:a"
    b = f"poros:test:advcompat:{message_id}:b"
    body = {
        "bundle_id": "kbundle:adv-compat",
        "origin_authority_id": "okg:lab-a",
        "origin_generation_id": 2,
        "classification": "public",
        "node_facts": [
            {"node_id": a, "subtype": "context_packet", "attrs": {"text": "a"}},
            {"node_id": b, "subtype": "context_packet", "attrs": {"text": "b"}},
        ],
        "edge_facts": [
            {
                "src": a,
                "dst": b,
                "edge_type": "references",
                "src_subtype": "context_packet",
                "dst_subtype": "context_packet",
            }
        ],
    }
    envelope = _signed_envelope(
        message_id, body=body, message_type=LEGACY_BUNDLE_SUBMIT_TYPE
    )
    result = _inbox().handle_bytes(canonical_json_bytes(envelope))
    assert result.status == "accepted_for_admission"
    return result.admission_candidate


def _seed_context_packet_catalog(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        catalog_version_id = int(cur.fetchone()[0])
        cur.execute(
            "INSERT INTO okg.subtypes (name, metadata, catalog_version_id) "
            "VALUES ('context_packet', '{}'::jsonb, %s)",
            (catalog_version_id,),
        )
        cur.execute(
            """
            INSERT INTO okg.subtype_ancestors
              (catalog_version_id, subtype_name, ancestor_name, depth)
            VALUES (%s, 'context_packet', 'context_packet', 0)
            ON CONFLICT DO NOTHING
            """,
            (catalog_version_id,),
        )
        cur.execute(
            """
            INSERT INTO okg.narrowings
              (parent_subtype, edge_type, child_subtype, catalog_version_id)
            VALUES ('context_packet', 'references', 'context_packet', %s)
            """,
            (catalog_version_id,),
        )


@pytest.mark.postgres
def test_compat_downgrade_is_refused_without_manifest_opt_in(
    clean_runtime: str,
) -> None:
    candidate = _compat_candidate("adv-downgrade-1")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_context_packet_catalog(conn)
        with conn.transaction():
            admitted = submit_poros_candidate_to_admission(
                conn,
                deployment_name="default",
                candidate=candidate,
            )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, verdict->>'code' FROM okg.admission_envelopes "
                "WHERE admission_id = %s::uuid",
                (admitted.admission_id,),
            )
            envelope_status, verdict_code = cur.fetchone()
            cur.execute(
                "SELECT count(*) FROM okg.node_facts WHERE run_id = %s::uuid",
                (admitted.run_id,),
            )
            node_fact_count = int(cur.fetchone()[0] or 0)

    assert admitted.status == "rejected"
    assert admitted.compat_rejected is True
    assert envelope_status == "rejected"
    assert verdict_code == "producer_signature_missing"
    assert node_fact_count == 0
