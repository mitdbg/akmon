from __future__ import annotations

from okg.substrate.exchange.hardening_harness import (
    PROMOTION_ACTOR,
    SENDER_AUTHORITY,
    _readiness_evidence_from_harness,
    _write_hardening_policy_bundle,
)
from okg.substrate.exchange.canonical_json import canonical_json_bytes
from okg.substrate.exchange.object_store import (
    LocalPorosObjectStore,
    load_detached_graph_body_from_store,
)
from okg.substrate.exchange.payloads import build_detached_object_manifest
from okg.substrate.nomos import DecisionInput, evaluate, load_bundle


def test_hardening_harness_policy_keeps_restricted_paths_distinct(tmp_path):
    run_id = "20260525T000000Z-test"
    bundle_path = _write_hardening_policy_bundle(tmp_path, run_id=run_id)
    bundle = load_bundle(bundle_path)

    base_resource = {
        "graph_profile": "context",
        "root_subtypes": ["context_packet"],
        "recipient": "okg:mit-demo",
        "signing_key_id": "lab-a-sign-hardening",
    }
    quarantine = evaluate(
        bundle,
        DecisionInput(
            subject=SENDER_AUTHORITY,
            action="poros.graph.submit",
            classification="restricted",
            resource={
                **base_resource,
                "graph_id": f"poros:graph:hardening:{run_id}:quarantine",
            },
        ),
        enforcement_point="poros_incorporation",
    )
    redaction = evaluate(
        bundle,
        DecisionInput(
            subject=SENDER_AUTHORITY,
            action="poros.graph.submit",
            classification="restricted",
            resource={
                **base_resource,
                "graph_id": f"poros:graph:hardening:{run_id}:redaction",
            },
        ),
        enforcement_point="poros_incorporation",
    )
    unrelated_restricted = evaluate(
        bundle,
        DecisionInput(
            subject=SENDER_AUTHORITY,
            action="poros.graph.submit",
            classification="restricted",
            resource={**base_resource, "graph_id": "poros:graph:other"},
        ),
        enforcement_point="poros_incorporation",
    )
    promotion = evaluate(
        bundle,
        DecisionInput(
            subject=PROMOTION_ACTOR,
            action="poros.graph.promote",
            classification="internal",
            resource={"original_effect": "review"},
        ),
        enforcement_point="poros_promotion",
    )

    assert quarantine.effect == "quarantine"
    assert redaction.effect == "redact"
    assert [item["type"] for item in redaction.obligations] == ["redaction"]
    assert unrelated_restricted.effect == "deny"
    assert promotion.effect == "allow"


def test_detached_graph_body_is_hydrated_for_receiver_policy(tmp_path):
    graph_body = {
        "graph_id": "poros:graph:detached",
        "origin_authority_id": "okg:lab-a",
        "origin_generation_id": 7,
        "classification": "public",
        "graph_profile": "context",
        "root_node_ids": ["node:detached"],
        "nodes": [
            {
                "node_id": "node:detached",
                "subtype": "context_packet",
                "attrs": {"text": "detached context"},
            }
        ],
        "edges": [],
    }
    data = canonical_json_bytes(graph_body)
    object_store = LocalPorosObjectStore(tmp_path / "objects")
    stored = object_store.put_bytes(object_id="obj:detached", data=data)
    manifest = build_detached_object_manifest(
        object_id="obj:detached",
        media_type="application/vnd.okg.poros.graph+json",
        data=data,
        chunk_size_bytes=1024,
        classification="public",
        locations=[{"uri": stored["uri"]}],
    )
    envelope = {
        "payload_mode": "detached",
        "body": {
            "payload_mode": "detached",
            "graph_id": graph_body["graph_id"],
            "origin_authority_id": graph_body["origin_authority_id"],
            "origin_generation_id": graph_body["origin_generation_id"],
            "classification": graph_body["classification"],
            "graph_profile": graph_body["graph_profile"],
            "root_node_ids": graph_body["root_node_ids"],
            "objects": [manifest],
        },
    }

    hydrated = load_detached_graph_body_from_store(
        envelope,
        object_store=object_store,
    )

    assert hydrated is not None
    assert hydrated["nodes"][0]["subtype"] == "context_packet"
    assert hydrated["objects"][0]["object_id"] == "obj:detached"


def test_hardening_harness_evidence_carries_live_readiness_chain(tmp_path):
    object_store_root = tmp_path / "objects"
    object_store_root.mkdir()
    generation_id = "okg:poros-receiver:branch:default:gen:test"
    allowed = {
        "message_id": "msg-allowed",
        "graph_id": "poros:graph:allowed",
        "body_sha256": "sha256:body",
        "envelope_sha256": "sha256:envelope",
        "row_delta": {"nodes_live": 1, "edges_live": 0},
        "envelope": {
            "body": {
                "root_node_ids": ["node:allowed"],
                "edges": [],
            },
        },
        "receiver_summary": {
            "published_generation": {
                "generation_id": generation_id,
                "status": "published",
            },
            "events": [
                {
                    "event_type": "nomos_decided",
                    "status": "allow",
                    "nomos_decision_id": "nomos:msg-allowed",
                    "policy_fingerprint": "sha256:policy",
                    "nomos_input_hash": "sha256:input",
                },
                {
                    "event_type": "admitted",
                    "status": "accepted",
                    "admission_id": "admission-msg-allowed",
                },
                {
                    "event_type": "published",
                    "status": "published",
                    "receiver_generation_id": generation_id,
                },
            ],
        },
        "sender_delivery": {
            "outbox": {"status": "delivered"},
            "attempts": [
                {
                    "status": "delivered",
                    "transport": "http",
                    "receipt_sha256": "sha256:receipt",
                }
            ],
        },
    }
    non_live = {
        "message_id": "msg-non-live",
        "graph_id": "poros:graph:non-live",
        "body_sha256": "sha256:non-live",
        "row_delta": {"nodes_live": 0, "edges_live": 0},
        "event_counts": {"review": 1},
    }

    evidence = _readiness_evidence_from_harness(
        setup={
            "sender": {"database": {"database": "sender"}},
            "receiver": {"database": {"database": "receiver"}, "doctor": {"ok": True}},
            "separate_databases": True,
        },
        scenarios={
            "allowed": allowed,
            "detached": {
                "message_id": "msg-detached",
                "detached_objects": {"sender": [{}], "receiver": [{}]},
            },
            "duplicate": {**non_live, "event_counts": {"duplicate": 1}},
            "untrusted_peer": {
                **non_live,
                "event_counts": {"preflight_rejected": 1},
            },
            "rejected": {
                **non_live,
                "event_counts": {"preflight_rejected": 1},
            },
            "deny": {
                **non_live,
                "event_counts": {"nomos_decided": 1},
            },
            "review": non_live,
            "quarantine": {
                **non_live,
                "event_counts": {"quarantined": 1},
            },
            "malformed": {
                **non_live,
                "event_counts": {"preflight_rejected": 1},
            },
            "transport_failed": {
                **non_live,
                "event_counts": {"transport_failed": 1},
                "delivery_attempt": {
                    "status": "failed",
                    "reason_code": "delivery_failed",
                    "transport": "http",
                    "receipt_sha256": None,
                },
                "delivery_result": {
                    "status": "failed",
                    "reason_code": "delivery_failed",
                    "outbox_status": "terminal_failed",
                },
                "sender_delivery": {
                    "outbox": {"status": "terminal_failed"},
                    "attempts": [
                        {
                            "status": "failed",
                            "reason_code": "delivery_failed",
                            "transport": "http",
                            "receipt_sha256": None,
                        }
                    ],
                },
                "receiver_summary": {"inbox_status": None},
            },
        },
        policy={"policy_fingerprint": "sha256:policy"},
        object_store_root=object_store_root,
        formal={"status": "pass", "fresh": True},
        receiver_metrics={
            "dlq_count": 0,
            "generation_blocked_count": 0,
            "runtime_queue_unavailable": 0,
        },
    )

    proof = evidence["latest_transfer_proof"]
    assert proof["freshness"]["fresh"] is True
    assert proof["freshness"]["observed_at"]
    assert proof["freshness"]["max_age_seconds"] == 3600
    assert proof["read_surface"]["kind"] == "chronos_generation"
    assert proof["read_surface"]["generation_id"] == generation_id
    assert proof["nomos_decision"]["nomos_decision_id"] == "nomos:msg-allowed"
    assert proof["admission"]["admission_id"] == "admission-msg-allowed"
    assert proof["publication"]["receiver_generation_id"] == generation_id
    assert proof["expected_live_row_delta"] == {"nodes_live": 1, "edges_live": 0}
    assert proof["observed_live_row_delta"] == {"nodes_live": 1, "edges_live": 0}
    assert evidence["failure_fixtures"]["deny"]["event_counts"] == {"nomos_decided": 1}
    assert evidence["failure_fixtures"]["malformed"]["event_counts"] == {
        "preflight_rejected": 1
    }
    assert evidence["failure_fixtures"]["transport_failed"]["event_counts"] == {
        "transport_failed": 1
    }
    assert evidence["failure_fixtures"]["transport_failed"]["delivery_attempt"][
        "reason_code"
    ] == "delivery_failed"
    assert evidence["failure_fixtures"]["transport_failed"]["sender_delivery"][
        "outbox"
    ]["status"] == "terminal_failed"
    assert evidence["failure_fixtures"]["review"]["event_counts"] == {"review": 1}
