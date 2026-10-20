"""Fail-closed admission for compat (missing-producer-signature) envelopes.

req.exchange-admission.fail-closed: on the canonical target an envelope whose
receiver-owned producer profile has no output signature (``status ==
"compat"``, surfaced as ``producer_signature_missing``) MUST be rejected, not
warned through. Compat acceptance is possible only under an explicit
deployment-manifest opt-in and leaves an audit record naming the envelope and
the reason.

The compat state is reached by any edge-bearing federated graph:
``_receiver_owned_producer_policy`` empties the output signature whenever the
graph carries edge narrowings, so a legacy fact bundle with a node subtype and
an edge lands in ``status == "compat"`` (see the design-review F6). The catalog
is seeded so the edge would otherwise pass narrowing — that isolates the compat
decision from a ``narrowing_violation``: the pre-fix code *accepts* this
candidate (warn-through), so the ``rejected`` assertion is a real regression
proof.
"""

from __future__ import annotations

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
from okg.substrate.exchange.readiness import (
    PorosKeyRef,
    PorosManifest,
    create_manifest,
    resolve_unsigned_compat_admission,
)


pytestmark = pytest.mark.postgres

SENDER_PRIVATE = b"\x41" * 32
SENDER_PUBLIC = derive_ed25519_public_key(SENDER_PRIVATE)
RECEIVER_PRIVATE = b"\x42" * 32


def _compat_bundle_body(message_id: str) -> dict:
    """A legacy fact bundle with an edge → empty output signature → compat."""
    a = f"poros:test:compat:{message_id}:a"
    b = f"poros:test:compat:{message_id}:b"
    return {
        "bundle_id": "kbundle:compat",
        "origin_authority_id": "okg:lab-a",
        "origin_generation_id": 3,
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


def _compat_candidate(message_id: str):
    unsigned = build_unsigned_envelope(
        sender="okg:lab-a",
        recipient="okg:mit",
        message_id=message_id,
        created_at=datetime.now(UTC),
        ttl_seconds=900,
        message_type=LEGACY_BUNDLE_SUBMIT_TYPE,
        body=_compat_bundle_body(message_id),
    )
    envelope = sign_envelope(unsigned, SENDER_PRIVATE, kid="lab-a-sign-1")
    inbox = PorosInbox(
        receiver_authority_id="okg:mit",
        trust_registry=TrustPinRegistry.from_pins(
            [
                TrustPin(
                    authority_id="okg:lab-a",
                    kid="lab-a-sign-1",
                    public_key=SENDER_PUBLIC,
                )
            ]
        ),
        receiver_private_key=RECEIVER_PRIVATE,
        receiver_signing_kid="mit-sign-1",
    )
    result = inbox.handle_bytes(canonical_json_bytes(envelope))
    assert result.status == "accepted_for_admission"
    assert result.admission_candidate is not None
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


def _admission_events(
    conn: psycopg.Connection, admission_id: str
) -> list[tuple[str, str | None, dict]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT event_type, reason, payload
              FROM okg.admission_events
             WHERE admission_id = %s::uuid
             ORDER BY event_id
            """,
            (admission_id,),
        )
        return [(row[0], row[1], row[2] or {}) for row in cur.fetchall()]


def test_compat_envelope_is_rejected_on_canonical_target(
    clean_runtime: str,
) -> None:
    """Default (no manifest opt-in): a compat envelope is rejected, not warned.

    Pre-fix this candidate is ACCEPTED (the ``producer_signature_missing``
    warning is attached and admission continues), so the ``rejected``
    assertion below is the warn-through regression proof.
    """
    candidate = _compat_candidate("poros-compat-reject-1")
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
                """
                SELECT status, verdict->>'code'
                  FROM okg.admission_envelopes
                 WHERE admission_id = %s::uuid
                """,
                (admitted.admission_id,),
            )
            envelope_status, verdict_code = cur.fetchone()
            cur.execute(
                "SELECT count(*) FROM okg.node_facts WHERE run_id = %s::uuid",
                (admitted.run_id,),
            )
            node_fact_count = int(cur.fetchone()[0] or 0)
        events = _admission_events(conn, admitted.admission_id)

    assert admitted.status == "rejected"
    assert admitted.compat_rejected is True
    assert admitted.emitted is None
    assert envelope_status == "rejected"
    assert verdict_code == "producer_signature_missing"
    # Rejected before any fact was emitted.
    assert node_fact_count == 0
    # The rejection is a durable audit record naming the reason.
    rejected = [e for e in events if e[0] == "rejected"]
    assert rejected, events
    assert rejected[-1][1] == "producer_signature_missing"


def test_compat_envelope_accepts_under_manifest_opt_in_with_audit(
    clean_runtime: str,
) -> None:
    """With the manifest compat opt-in, a compat envelope is admitted, and the
    acceptance leaves an audit record naming the envelope and the reason."""
    candidate = _compat_candidate("poros-compat-optin-1")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_context_packet_catalog(conn)
        with conn.transaction():
            admitted = submit_poros_candidate_to_admission(
                conn,
                deployment_name="default",
                candidate=candidate,
                allow_unsigned_compat=True,
            )
        events = _admission_events(conn, admitted.admission_id)

    assert admitted.status == "accepted"
    assert admitted.compat_rejected is False
    assert admitted.emitted is not None
    assert admitted.emitted["node_facts_inserted"] == 2

    audit = [
        e
        for e in events
        if (e[1] == "producer_signature_missing")
        or e[2].get("reason") == "producer_signature_missing"
    ]
    assert audit, events
    payload = audit[-1][2]
    assert payload.get("message_id") == "poros-compat-optin-1"
    assert payload.get("body_sha256") == candidate.body_sha256
    assert payload.get("allow_unsigned_compat") is True


# --- deployment-manifest opt-in resolution (fail-closed, no DB) -------------


def _minimal_manifest(admission_block: dict | None = None) -> dict:
    base = create_manifest(
        authority_id="okg:mit",
        roles=["receiver"],
        signing_key=PorosKeyRef.from_mapping(
            {"kid": "lab-a-sign-1", "public_key": SENDER_PUBLIC}
        ),
        object_store_root="/tmp/poros-objects",
    ).to_dict()
    if admission_block is not None:
        base["admission"] = admission_block
    return base


@pytest.mark.parametrize(
    ("admission_block", "expected"),
    [
        (None, False),
        ({}, False),
        ({"allow_unsigned_compat": False}, False),
        ({"allow_unsigned_compat": True}, True),
        ({"allow_unsigned_compat": "true"}, False),  # non-bool → fail closed
        ({"allow_unsigned_compat": 1}, False),  # non-bool → fail closed
    ],
)
def test_manifest_compat_opt_in_is_fail_closed(
    admission_block: dict | None, expected: bool
) -> None:
    manifest = PorosManifest.from_mapping(_minimal_manifest(admission_block))
    assert manifest.allow_unsigned_compat_admission is expected
    assert resolve_unsigned_compat_admission(manifest) is expected


def test_manifest_opt_in_survives_roundtrip() -> None:
    manifest = PorosManifest.from_mapping(
        _minimal_manifest({"allow_unsigned_compat": True})
    )
    reloaded = PorosManifest.from_mapping(manifest.to_dict())
    assert reloaded.allow_unsigned_compat_admission is True
