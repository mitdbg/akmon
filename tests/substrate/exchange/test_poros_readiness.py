from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from okg.substrate.exchange import generate_ed25519_keypair
from okg.substrate.exchange.readiness import (
    PorosManifest,
    PorosReadinessError,
    add_peer,
    create_manifest,
    disable_peer,
    public_key_fingerprint,
    readiness_report,
    rotate_peer_key,
    save_manifest,
    signing_key_ref,
    trust_pins_payload,
)


def test_manifest_schema_validates_and_redacts_key_paths(tmp_path: Path) -> None:
    key = generate_ed25519_keypair("lab-a-sign")
    manifest = create_manifest(
        authority_id="okg:lab-a",
        roles=["sender"],
        signing_key=signing_key_ref(
            kid=key.kid,
            public_key=key.public_key,
            private_key_path=str(tmp_path / "lab-a.keys.json"),
        ),
        object_store_root=str(tmp_path / "objects"),
    )

    manifest_path = tmp_path / "poros.manifest.json"
    save_manifest(manifest, manifest_path, overwrite=False)
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    restored = PorosManifest.from_mapping(payload)
    public_payload = restored.to_dict(public=True)

    assert restored.authority_id == "okg:lab-a"
    assert restored.signing_keys[0].fingerprint == public_key_fingerprint(
        key.public_key
    )
    assert "private_key_path" in restored.to_dict()["signing_keys"][0]
    assert "private_key_path" not in public_payload["signing_keys"][0]
    assert public_payload["signing_keys"][0]["private_key_path_redacted"] is True


def test_manifest_validation_fails_closed_for_bad_inputs(tmp_path: Path) -> None:
    key = generate_ed25519_keypair("lab-a-sign")
    valid_key = signing_key_ref(
        kid=key.kid,
        public_key=key.public_key,
        private_key_path=str(tmp_path / "lab-a.keys.json"),
    )

    with pytest.raises(PorosReadinessError, match="authority id"):
        create_manifest(
            authority_id="not an authority",
            roles=["sender"],
            signing_key=valid_key,
            object_store_root=str(tmp_path / "objects"),
        )

    with pytest.raises(PorosReadinessError) as missing_role:
        create_manifest(
            authority_id="okg:lab-a",
            roles=[],
            signing_key=valid_key,
            object_store_root=str(tmp_path / "objects"),
        )
    assert missing_role.value.reason_code == "missing_enabled_role"

    with pytest.raises(PorosReadinessError) as missing_root:
        create_manifest(
            authority_id="okg:lab-a",
            roles=["sender"],
            signing_key=valid_key,
            object_store_root=None,
        )
    assert missing_root.value.reason_code == "missing_object_store_root"

    bad_manifest = valid_key.to_dict()
    bad_manifest["public_key"] = "not-valid-key"
    with pytest.raises(PorosReadinessError) as bad_key:
        PorosManifest.from_mapping(
            {
                "poros_manifest_version": 1,
                "authority_id": "okg:lab-a",
                "roles": ["sender"],
                "signing_keys": [valid_key.to_dict()],
                "peers": [
                    {
                        "authority_id": "okg:mit-demo",
                        "keys": [bad_manifest],
                    }
                ],
                "object_store": {
                    "backend": "local",
                    "root": str(tmp_path / "objects"),
                },
                "redaction": {"default_mode": "public"},
                "formal_assurance": {"level": "none"},
            }
        )
    assert bad_key.value.reason_code == "invalid_public_key"


def test_peer_registry_add_disable_rotate_and_trust_pins(tmp_path: Path) -> None:
    local_key = generate_ed25519_keypair("lab-a-sign")
    peer_key = generate_ed25519_keypair("mit-sign-v1")
    peer_key_v2 = generate_ed25519_keypair("mit-sign-v2")
    manifest = create_manifest(
        authority_id="okg:lab-a",
        roles=["sender"],
        signing_key=signing_key_ref(
            kid=local_key.kid,
            public_key=local_key.public_key,
            private_key_path=str(tmp_path / "lab-a.keys.json"),
        ),
        object_store_root=str(tmp_path / "objects"),
    )

    peer = add_peer(
        manifest,
        authority_id="okg:mit-demo",
        kid=peer_key.kid,
        public_key=peer_key.public_key,
        endpoint="http://127.0.0.1:8765/poros/v1/inbox",
    )
    assert peer.status == "active"
    assert len(trust_pins_payload(manifest)["pins"]) == 1
    assert readiness_report(manifest, require_transfer_proof=False)["ready"] is True

    rotated = rotate_peer_key(
        manifest,
        authority_id="okg:mit-demo",
        old_kid=peer_key.kid,
        new_kid=peer_key_v2.kid,
        public_key=peer_key_v2.public_key,
    )
    old = next(key for key in rotated.keys if key.kid == peer_key.kid)
    new = next(key for key in rotated.keys if key.kid == peer_key_v2.kid)
    assert old.status == "retired"
    assert old.replaced_by == peer_key_v2.kid
    assert new.status == "active"
    assert new.rotation_evidence["old_fingerprint"] == old.fingerprint
    assert len(trust_pins_payload(manifest)["pins"]) == 1
    assert trust_pins_payload(manifest)["pins"][0]["kid"] == peer_key_v2.kid

    disable_peer(manifest, authority_id="okg:mit-demo")
    report = readiness_report(manifest)
    assert report["ready"] is False
    assert any(check["id"] == "peers.active" for check in report["checks"])


def test_peer_key_material_change_requires_rotation(tmp_path: Path) -> None:
    local_key = generate_ed25519_keypair("lab-a-sign")
    peer_key = generate_ed25519_keypair("mit-sign")
    peer_key_changed = generate_ed25519_keypair("mit-sign")
    manifest = create_manifest(
        authority_id="okg:lab-a",
        roles=["sender"],
        signing_key=signing_key_ref(
            kid=local_key.kid,
            public_key=local_key.public_key,
            private_key_path=str(tmp_path / "lab-a.keys.json"),
        ),
        object_store_root=str(tmp_path / "objects"),
    )

    add_peer(
        manifest,
        authority_id="okg:mit-demo",
        kid=peer_key.kid,
        public_key=peer_key.public_key,
    )
    with pytest.raises(PorosReadinessError) as exc:
        add_peer(
            manifest,
            authority_id="okg:mit-demo",
            kid=peer_key.kid,
            public_key=peer_key_changed.public_key,
        )
    assert exc.value.reason_code == "peer_key_material_changed"


def test_readiness_fails_ambiguous_peer_key_overlap(tmp_path: Path) -> None:
    local_key = generate_ed25519_keypair("lab-a-sign")
    peer_key = generate_ed25519_keypair("mit-sign-v1")
    peer_key_v2 = generate_ed25519_keypair("mit-sign-v2")
    manifest = create_manifest(
        authority_id="okg:lab-a",
        roles=["sender"],
        signing_key=signing_key_ref(
            kid=local_key.kid,
            public_key=local_key.public_key,
            private_key_path=str(tmp_path / "lab-a.keys.json"),
        ),
        object_store_root=str(tmp_path / "objects"),
    )

    add_peer(
        manifest,
        authority_id="okg:mit-demo",
        kid=peer_key.kid,
        public_key=peer_key.public_key,
    )
    add_peer(
        manifest,
        authority_id="okg:mit-demo",
        kid=peer_key_v2.kid,
        public_key=peer_key_v2.public_key,
    )

    report = readiness_report(manifest)
    assert report["ready"] is False
    active_key_check = next(
        check
        for check in report["checks"]
        if check["id"] == "peer.okg_mit-demo.active_key"
    )
    assert active_key_check["ok"] is False
    assert active_key_check["data"]["active_key_count"] == 2


def test_local_demo_readiness_requires_two_db_transfer_evidence(
    tmp_path: Path,
) -> None:
    manifest = _manifest_with_active_peer(tmp_path)

    missing_evidence = readiness_report(manifest, level="local-demo")
    assert missing_evidence["ready"] is False
    failed = {check["id"] for check in missing_evidence["checks"] if not check["ok"]}
    assert "evidence.separate_databases" in failed
    assert "evidence.transfer_proof" in failed

    manifest.readiness_evidence["database_identities"] = {
        "sender": {"database": "okg_sender"},
        "receiver": {"database": "okg_receiver"},
        "separate_databases": True,
    }
    manifest.readiness_evidence["latest_transfer_proof"] = {
        "message_id": "msg-1",
        "graph_id": "poros:graph:demo",
        "state": "published",
        "sender_outbox_status": "delivered",
        "receiver_generation": {
            "generation_id": 5,
            "status": "published",
        },
        "live_row_delta": {
            "nodes_live": 1,
            "edges_live": 0,
        },
    }

    legacy_shape = readiness_report(manifest, level="local-demo")
    assert legacy_shape["ready"] is False
    transfer_check = next(
        check
        for check in legacy_shape["checks"]
        if check["id"] == "evidence.transfer_proof"
    )
    assert transfer_check["ok"] is False

    manifest.readiness_evidence["latest_transfer_proof"] = _accepted_transfer_proof()

    ready = readiness_report(manifest, level="local-demo")
    assert ready["ready"] is True
    ready_transfer = next(
        check for check in ready["checks"] if check["id"] == "evidence.transfer_proof"
    )
    assert ready_transfer["data"]["latest_transfer_proof"]["read_surface"] == {
        "kind": "chronos_generation",
        "generation_id": "okg:receiver:branch:default:gen:1",
    }


def test_pilot_readiness_requires_operational_and_formal_evidence(
    tmp_path: Path,
) -> None:
    artifact = tmp_path / "refinement-state.json"
    artifact.write_text('{"status":"pass"}\n', encoding="utf-8")
    policy = tmp_path / "nomos.bundle.json"
    policy.write_text('{"policy_bundle_id":"demo"}\n', encoding="utf-8")
    manifest = _manifest_with_active_peer(
        tmp_path,
        roles=["sender", "receiver"],
        receiver_endpoint="http://127.0.0.1:8765/poros/v1/inbox",
        nomos_bundle_path=str(policy),
    )
    manifest.nomos["policy_fingerprint"] = "sha256:policy"
    manifest.readiness_evidence.update(
        {
            "database_identities": {
                "sender": {"database": "okg_sender"},
                "receiver": {"database": "okg_receiver"},
                "separate_databases": True,
            },
            "latest_transfer_proof": _accepted_transfer_proof(),
        }
    )

    incomplete = readiness_report(manifest, level="pilot")
    assert incomplete["ready"] is False
    failed = {check["id"] for check in incomplete["checks"] if not check["ok"]}
    assert "pilot.object_store_verified" in failed
    assert "pilot.service_delivery" in failed
    assert "metrics.clean" in failed
    assert "doctor.clean" in failed
    assert "formal.service_refinement" in failed

    manifest.readiness_evidence.update(
        {
            "object_store": {"verified": True, "backend": "local"},
            "service_delivery": {
                "status": "delivered",
                "transport": "http",
                "message_id": "msg-1",
            },
            "failure_fixtures": {
                "replay": _non_live_fixture("duplicate"),
                "untrusted_or_disabled_peer": _non_live_fixture(
                    "rejected_preflight",
                    event_counts={"preflight_rejected": 1},
                ),
                "tampered_payload_or_object": _non_live_fixture(
                    "rejected_preflight",
                    event_counts={"preflight_rejected": 1},
                ),
                "deny": _non_live_fixture("deny", event_counts={"nomos_decided": 1}),
                "review": _non_live_fixture("review"),
                "quarantine": _non_live_fixture(
                    "quarantine",
                    event_counts={"quarantined": 1},
                ),
                "malformed": _non_live_fixture(
                    "malformed",
                    event_counts={"preflight_rejected": 1},
                ),
                "transport_failed": _non_live_fixture(
                    "transport_failed",
                    event_counts={"transport_failed": 1},
                    transport_failed=True,
                ),
            },
            "metrics": {
                "dlq_count": 0,
                "generation_blocked_count": 0,
                "runtime_queue_unavailable": 0,
            },
            "doctor": {"ok": True},
            "service_refinement": {
                "status": "pass",
                "fresh": True,
                "artifact_path": str(artifact),
            },
        }
    )

    ready = readiness_report(manifest, level="pilot")
    assert ready["ready"] is True


@pytest.mark.parametrize(
    "metrics",
    [
        {"dlq_count": 0, "generation_blocked_count": 0},
        {
            "dlq_count": 0,
            "generation_blocked_count": 0,
            "runtime_queue_unavailable": 1,
        },
    ],
    ids=("runtime-authority-missing", "runtime-authority-unavailable"),
)
def test_pilot_readiness_fails_closed_without_runtime_queue_authority(
    tmp_path: Path,
    metrics: dict[str, int],
) -> None:
    manifest = _pilot_ready_manifest(tmp_path)
    manifest.readiness_evidence["metrics"] = metrics

    report = readiness_report(manifest, level="pilot")

    assert report["ready"] is False
    check = next(item for item in report["checks"] if item["id"] == "metrics.clean")
    assert check["ok"] is False


def test_pilot_readiness_rejects_stale_skipped_or_missing_refinement(
    tmp_path: Path,
) -> None:
    manifest = _pilot_ready_manifest(tmp_path)
    manifest.readiness_evidence["service_refinement"] = {
        "status": "skipped",
        "fresh": True,
    }

    skipped = readiness_report(manifest, level="pilot")
    assert skipped["ready"] is False
    refinement_check = next(
        check
        for check in skipped["checks"]
        if check["id"] == "formal.service_refinement"
    )
    assert refinement_check["ok"] is False

    manifest.readiness_evidence["service_refinement"] = {
        "status": "pass",
        "fresh": False,
    }
    stale = readiness_report(manifest, level="pilot")
    assert stale["ready"] is False

    manifest.readiness_evidence["service_refinement"] = {
        "status": "pass",
        "fresh": True,
        "artifact_path": str(tmp_path / "missing-refinement.json"),
    }
    missing = readiness_report(manifest, level="pilot")
    assert missing["ready"] is False


def test_pilot_readiness_rejects_unanchored_failure_fixtures(
    tmp_path: Path,
) -> None:
    manifest = _pilot_ready_manifest(tmp_path)
    manifest.readiness_evidence["failure_fixtures"]["review"] = {
        "state": "review",
        "durable_evidence": True,
        "live_row_delta": {
            "nodes_live": 0,
            "edges_live": 0,
        },
    }

    report = readiness_report(manifest, level="pilot")

    assert report["ready"] is False
    review_check = next(
        check
        for check in report["checks"]
        if check["id"] == "pilot.failure_fixture.review"
    )
    assert review_check["ok"] is False


def test_pilot_readiness_rejects_mislabeled_failure_fixtures(
    tmp_path: Path,
) -> None:
    manifest = _pilot_ready_manifest(tmp_path)
    manifest.readiness_evidence["failure_fixtures"]["transport_failed"] = (
        _non_live_fixture(
            "transport_failed",
            event_counts={"review": 1},
            transport_failed=True,
        )
    )

    report = readiness_report(manifest, level="pilot")

    assert report["ready"] is False
    transport_check = next(
        check
        for check in report["checks"]
        if check["id"] == "pilot.failure_fixture.transport_failed"
    )
    assert transport_check["ok"] is False


def test_readiness_rejects_stale_or_mismatched_transfer_proof(
    tmp_path: Path,
) -> None:
    manifest = _manifest_with_active_peer(tmp_path)
    manifest.readiness_evidence["database_identities"] = {
        "sender": {"database": "okg_sender"},
        "receiver": {"database": "okg_receiver"},
        "separate_databases": True,
    }
    manifest.readiness_evidence["latest_transfer_proof"] = {
        **_accepted_transfer_proof(),
        "freshness": {"fresh": False},
    }

    stale = readiness_report(manifest, level="local-demo")
    assert stale["ready"] is False

    manifest.readiness_evidence["latest_transfer_proof"] = {
        **_accepted_transfer_proof(),
        "freshness": {
            "fresh": True,
            "source": "test",
            "observed_at": (
                datetime.now(UTC) - timedelta(days=2)
            ).isoformat(),
            "max_age_seconds": 3600,
        },
    }

    expired = readiness_report(manifest, level="local-demo")
    assert expired["ready"] is False

    missing_window = _accepted_transfer_proof()
    missing_window["freshness"] = {
        "fresh": True,
        "source": "test",
        "observed_at": datetime.now(UTC).isoformat(),
    }
    manifest.readiness_evidence["latest_transfer_proof"] = missing_window

    no_window = readiness_report(manifest, level="local-demo")
    assert no_window["ready"] is False

    manifest.readiness_evidence["latest_transfer_proof"] = {
        **_accepted_transfer_proof(),
        "observed_live_row_delta": {"nodes_live": 2, "edges_live": 0},
        "live_row_delta": {"nodes_live": 2, "edges_live": 0},
    }

    mismatched = readiness_report(manifest, level="local-demo")
    assert mismatched["ready"] is False


def test_readiness_rejects_denied_or_blocked_transfer_proof(
    tmp_path: Path,
) -> None:
    manifest = _manifest_with_active_peer(tmp_path)
    manifest.readiness_evidence["database_identities"] = {
        "sender": {"database": "okg_sender"},
        "receiver": {"database": "okg_receiver"},
        "separate_databases": True,
    }

    denied = _accepted_transfer_proof()
    denied["admission"] = {
        "event_type": "admitted",
        "status": "denied",
        "admission_id": "admission-msg-1",
    }
    manifest.readiness_evidence["latest_transfer_proof"] = denied

    denied_report = readiness_report(manifest, level="local-demo")
    assert denied_report["ready"] is False

    blocked_publication = _accepted_transfer_proof()
    blocked_publication["publication"] = {
        "event_type": "published",
        "status": "blocked",
        "receiver_generation_id": "okg:receiver:branch:default:gen:1",
    }
    manifest.readiness_evidence["latest_transfer_proof"] = blocked_publication

    blocked_report = readiness_report(manifest, level="local-demo")
    assert blocked_report["ready"] is False


def test_readiness_requires_chronos_read_surface(
    tmp_path: Path,
) -> None:
    manifest = _manifest_with_active_peer(tmp_path)
    manifest.readiness_evidence["database_identities"] = {
        "sender": {"database": "okg_sender"},
        "receiver": {"database": "okg_receiver"},
        "separate_databases": True,
    }

    missing_surface = _accepted_transfer_proof()
    missing_surface.pop("read_surface")
    manifest.readiness_evidence["latest_transfer_proof"] = missing_surface

    missing = readiness_report(manifest, level="local-demo")
    assert missing["ready"] is False

    wrong_surface = _accepted_transfer_proof()
    wrong_surface["read_surface"] = {
        "kind": "not_chronos",
        "generation_id": "okg:receiver:branch:default:gen:1",
    }
    manifest.readiness_evidence["latest_transfer_proof"] = wrong_surface

    wrong = readiness_report(manifest, level="local-demo")
    assert wrong["ready"] is False


def test_pilot_readiness_requires_sender_delivery_transport_evidence(
    tmp_path: Path,
) -> None:
    manifest = _pilot_ready_manifest(tmp_path)
    fixture = _non_live_fixture(
        "transport_failed",
        event_counts={"transport_failed": 1},
        transport_failed=True,
    )
    fixture.pop("sender_delivery")
    manifest.readiness_evidence["failure_fixtures"]["transport_failed"] = fixture

    report = readiness_report(manifest, level="pilot")

    assert report["ready"] is False
    transport_check = next(
        check
        for check in report["checks"]
        if check["id"] == "pilot.failure_fixture.transport_failed"
    )
    assert transport_check["ok"] is False


def test_pilot_readiness_rejects_transport_failure_outbox_contradiction(
    tmp_path: Path,
) -> None:
    manifest = _pilot_ready_manifest(tmp_path)
    fixture = _non_live_fixture(
        "transport_failed",
        event_counts={"transport_failed": 1},
        transport_failed=True,
    )
    sender_delivery = fixture["sender_delivery"]
    assert isinstance(sender_delivery, dict)
    outbox = sender_delivery["outbox"]
    assert isinstance(outbox, dict)
    outbox["status"] = "delivered"
    manifest.readiness_evidence["failure_fixtures"]["transport_failed"] = fixture

    report = readiness_report(manifest, level="pilot")

    assert report["ready"] is False
    transport_check = next(
        check
        for check in report["checks"]
        if check["id"] == "pilot.failure_fixture.transport_failed"
    )
    assert transport_check["ok"] is False


def _manifest_with_active_peer(
    tmp_path: Path,
    *,
    roles: list[str] | None = None,
    receiver_endpoint: str | None = None,
    nomos_bundle_path: str | None = None,
) -> PorosManifest:
    local_key = generate_ed25519_keypair("lab-a-sign")
    peer_key = generate_ed25519_keypair("mit-sign")
    object_root = tmp_path / "objects"
    object_root.mkdir(parents=True, exist_ok=True)
    manifest = create_manifest(
        authority_id="okg:lab-a",
        roles=roles or ["sender"],
        signing_key=signing_key_ref(
            kid=local_key.kid,
            public_key=local_key.public_key,
            private_key_path=str(tmp_path / "lab-a.keys.json"),
        ),
        receiver_endpoint=receiver_endpoint,
        object_store_root=str(object_root),
        nomos_bundle_path=nomos_bundle_path,
    )
    add_peer(
        manifest,
        authority_id="okg:mit-demo",
        kid=peer_key.kid,
        public_key=peer_key.public_key,
        endpoint="http://127.0.0.1:8765/poros/v1/inbox",
    )
    return manifest


def _pilot_ready_manifest(tmp_path: Path) -> PorosManifest:
    artifact = tmp_path / "refinement-state.json"
    artifact.write_text('{"status":"pass"}\n', encoding="utf-8")
    policy = tmp_path / "nomos.bundle.json"
    policy.write_text('{"policy_bundle_id":"demo"}\n', encoding="utf-8")
    manifest = _manifest_with_active_peer(
        tmp_path,
        roles=["sender", "receiver"],
        receiver_endpoint="http://127.0.0.1:8765/poros/v1/inbox",
        nomos_bundle_path=str(policy),
    )
    manifest.nomos["policy_fingerprint"] = "sha256:policy"
    manifest.readiness_evidence.update(
        {
            "database_identities": {
                "sender": {"database": "okg_sender"},
                "receiver": {"database": "okg_receiver"},
                "separate_databases": True,
            },
            "latest_transfer_proof": _accepted_transfer_proof(),
            "object_store": {"verified": True, "backend": "local"},
            "service_delivery": {
                "status": "delivered",
                "transport": "http",
                "message_id": "msg-1",
            },
            "failure_fixtures": {
                "replay": _non_live_fixture("duplicate"),
                "untrusted_or_disabled_peer": _non_live_fixture(
                    "rejected_preflight",
                    event_counts={"preflight_rejected": 1},
                ),
                "tampered_payload_or_object": _non_live_fixture(
                    "rejected_preflight",
                    event_counts={"preflight_rejected": 1},
                ),
                "deny": _non_live_fixture("deny", event_counts={"nomos_decided": 1}),
                "review": _non_live_fixture("review"),
                "quarantine": _non_live_fixture(
                    "quarantine",
                    event_counts={"quarantined": 1},
                ),
                "malformed": _non_live_fixture(
                    "malformed",
                    event_counts={"preflight_rejected": 1},
                ),
                "transport_failed": _non_live_fixture(
                    "transport_failed",
                    event_counts={"transport_failed": 1},
                    transport_failed=True,
                ),
            },
            "metrics": {
                "dlq_count": 0,
                "generation_blocked_count": 0,
                "runtime_queue_unavailable": 0,
            },
            "doctor": {"ok": True},
            "service_refinement": {
                "status": "pass",
                "fresh": True,
                "artifact_path": str(artifact),
            },
        }
    )
    return manifest


def _non_live_fixture(
    state: str,
    *,
    event_counts: dict[str, int] | None = None,
    transport_failed: bool = False,
) -> dict[str, object]:
    fixture: dict[str, object] = {
        "state": state,
        "durable_evidence": True,
        "message_id": f"msg-{state}",
        "graph_id": f"poros:graph:{state}",
        "body_sha256": f"sha256:{state}",
        "live_row_delta": {
            "nodes_live": 0,
            "edges_live": 0,
        },
        "event_counts": event_counts or {state: 1},
    }
    if transport_failed:
        fixture.update(
            {
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
            }
        )
    return fixture


def _accepted_transfer_proof() -> dict[str, object]:
    generation_id = "okg:receiver:branch:default:gen:1"
    return {
        "proof_kind": "poros_hardening_harness",
        "proof_version": 1,
        "freshness": {
            "fresh": True,
            "source": "test",
            "observed_at": datetime.now(UTC).isoformat(),
            "max_age_seconds": 3600,
        },
        "observed_at": datetime.now(UTC).isoformat(),
        "message_id": "msg-1",
        "graph_id": "poros:graph:demo",
        "state": "published",
        "sender_outbox_status": "delivered",
        "receiver_generation": {
            "generation_id": generation_id,
            "status": "published",
        },
        "read_surface": {
            "kind": "chronos_generation",
            "generation_id": generation_id,
        },
        "nomos_decision": {
            "event_type": "nomos_decided",
            "status": "allow",
            "nomos_decision_id": "nomos:decision:msg-1",
            "policy_fingerprint": "sha256:policy",
            "nomos_input_hash": "sha256:input",
        },
        "admission": {
            "event_type": "admitted",
            "status": "accepted",
            "admission_id": "admission-msg-1",
        },
        "publication": {
            "event_type": "published",
            "status": "published",
            "receiver_generation_id": generation_id,
        },
        "expected_live_row_delta": {
            "nodes_live": 1,
            "edges_live": 0,
        },
        "observed_live_row_delta": {
            "nodes_live": 1,
            "edges_live": 0,
        },
        "live_row_delta": {
            "nodes_live": 1,
            "edges_live": 0,
        },
    }
