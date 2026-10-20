"""Repeatable Poros hardening harness.

The harness intentionally uses the production sender outbox export and
receiver submission paths. It is a local, in-process transport so operators
can run a deterministic safety demonstration without starting an HTTP server.
"""

from __future__ import annotations

import json
import shutil
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from okg.substrate.exchange.canonical_json import canonical_json_bytes
from okg.substrate.exchange.harness import (
    PorosHarnessError,
    database_doctor,
    describe_generation,
    initialize_two_okg_harness,
)
from okg.substrate.exchange.outbox import (
    PorosOutboxExportResult,
    export_published_subgraph_to_outbox,
)
from okg.substrate.exchange.readiness import (
    add_peer,
    create_manifest,
    readiness_report,
    save_manifest,
    signing_key_ref,
)
from okg.substrate.exchange.receiver import (
    PorosReceiverSubmitOutcome,
    promote_poros_message,
    submit_poros_envelope_bytes,
)
from okg.substrate.exchange.refinement import build_refinement_state_from_trace
from okg.substrate.exchange.sender_delivery import deliver_next_outbox_messages
from okg.substrate.exchange.signing import (
    Ed25519KeyPair,
    generate_ed25519_keypair,
)
from okg.substrate.nomos import compile_policy_graph
from okg.substrate.publisher.factory import build_deployment_publisher
from okg.substrate.db.connection import connect as db_connect


SENDER_AUTHORITY = "okg:lab-a"
RECEIVER_AUTHORITY = "okg:mit-demo"
PROMOTION_ACTOR = "okg:mit-reviewer"


class PorosHardeningHarnessError(PorosHarnessError):
    """Raised when the Poros hardening harness cannot run safely."""


@dataclass(frozen=True)
class _ScenarioExport:
    message_id: str
    graph_id: str
    node_id: str
    sender_generation_id: str
    export: PorosOutboxExportResult
    envelope: dict[str, Any]


def run_poros_hardening_harness(
    *,
    sender_dsn: str,
    receiver_dsn: str,
    out_dir: Path | str = ".scratch/poros-hardening-harness",
    sender_deployment: str = "poros-sender",
    receiver_deployment: str = "poros-receiver",
    migrate_schema: bool = True,
    overwrite: bool = False,
    allow_dirty: bool = False,
) -> dict[str, Any]:
    """Run allowed, failure, review, quarantine, promotion, and redaction demos."""
    artifact_dir = Path(out_dir).expanduser().resolve()
    if artifact_dir.exists() and any(artifact_dir.iterdir()):
        if not overwrite:
            raise PorosHardeningHarnessError(
                f"{artifact_dir} already contains files; pass --overwrite"
            )
        shutil.rmtree(artifact_dir)
    artifact_dir.mkdir(parents=True, exist_ok=True)

    if not allow_dirty:
        _assert_clean_poros_runtime(sender_dsn, role="sender")
        _assert_clean_poros_runtime(receiver_dsn, role="receiver")

    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    if migrate_schema:
        _ensure_required_extensions(sender_dsn, role="sender")
        _ensure_required_extensions(receiver_dsn, role="receiver")
    setup = initialize_two_okg_harness(
        sender_dsn=sender_dsn,
        receiver_dsn=receiver_dsn,
        sender_deployment=sender_deployment,
        receiver_deployment=receiver_deployment,
        migrate_schema=migrate_schema,
    )

    sender_keys = generate_ed25519_keypair("lab-a-sign-hardening")
    receiver_keys = generate_ed25519_keypair("mit-demo-sign-hardening")
    trust_pins = _trust_pins(sender_keys)
    _write_json(artifact_dir / "sender.keys.json", sender_keys.__dict__)
    _write_json(artifact_dir / "receiver.keys.json", receiver_keys.__dict__)
    _write_json(artifact_dir / "trust-pins.json", trust_pins)
    policy_path = _write_hardening_policy_bundle(
        artifact_dir / "nomos",
        run_id=run_id,
    )
    policy = json.loads(policy_path.read_text(encoding="utf-8"))
    object_store_root = artifact_dir / "objects"

    scenarios: dict[str, Any] = {}
    scenarios["allowed"] = _run_export_submit_scenario(
        sender_dsn=sender_dsn,
        receiver_dsn=receiver_dsn,
        sender_deployment=sender_deployment,
        receiver_deployment=receiver_deployment,
        run_id=run_id,
        scenario="allowed",
        graph_id=f"poros:graph:hardening:{run_id}:allowed",
        classification="public",
        root_subtype="context_packet",
        text="Allowed Poros context packet from sender OKG.",
        sender_keys=sender_keys,
        receiver_keys=receiver_keys,
        trust_pins=trust_pins,
        policy_path=policy_path,
        object_store_root=object_store_root,
        expected_publication="published",
    )

    scenarios["rejected"] = _run_export_submit_scenario(
        sender_dsn=sender_dsn,
        receiver_dsn=receiver_dsn,
        sender_deployment=sender_deployment,
        receiver_deployment=receiver_deployment,
        run_id=run_id,
        scenario="rejected",
        graph_id=f"poros:graph:hardening:{run_id}:rejected",
        classification="public",
        root_subtype="context_packet",
        text="Rejected Poros context packet tampered after signing.",
        sender_keys=sender_keys,
        receiver_keys=receiver_keys,
        trust_pins=trust_pins,
        policy_path=policy_path,
        object_store_root=object_store_root,
        mutate_envelope=_tamper_signed_body,
        expected_publication="none",
    )

    untrusted_export = _prepare_export(
        sender_dsn=sender_dsn,
        sender_deployment=sender_deployment,
        run_id=run_id,
        scenario="untrusted_peer",
        graph_id=f"poros:graph:hardening:{run_id}:untrusted_peer",
        classification="public",
        root_subtype="context_packet",
        text="Poros context packet from an untrusted sender key.",
        sender_keys=sender_keys,
        detach=False,
        object_store_root=object_store_root,
    )
    before_untrusted = _receiver_counts(receiver_dsn)
    untrusted_outcome = _submit_envelope(
        receiver_dsn=receiver_dsn,
        receiver_deployment=receiver_deployment,
        sender_dsn=sender_dsn,
        receiver_keys=receiver_keys,
        trust_pins={"pins": []},
        envelope=untrusted_export.envelope,
        policy_path=policy_path,
        object_store_root=object_store_root,
        apply=True,
        publish=True,
    )
    scenarios["untrusted_peer"] = _scenario_payload(
        receiver_dsn=receiver_dsn,
        sender_dsn=sender_dsn,
        export=untrusted_export,
        outcome=untrusted_outcome,
        before=before_untrusted,
        after=_receiver_counts(receiver_dsn),
        expected_publication="none",
    )

    scenarios["deny"] = _run_export_submit_scenario(
        sender_dsn=sender_dsn,
        receiver_dsn=receiver_dsn,
        sender_deployment=sender_deployment,
        receiver_deployment=receiver_deployment,
        run_id=run_id,
        scenario="deny",
        graph_id=f"poros:graph:hardening:{run_id}:deny",
        classification="restricted",
        root_subtype="context_packet",
        text="Restricted Poros context packet denied by receiver policy.",
        sender_keys=sender_keys,
        receiver_keys=receiver_keys,
        trust_pins=trust_pins,
        policy_path=policy_path,
        object_store_root=object_store_root,
        expected_publication="none",
    )

    scenarios["malformed"] = _run_malformed_envelope_scenario(
        sender_dsn=sender_dsn,
        receiver_dsn=receiver_dsn,
        sender_deployment=sender_deployment,
        receiver_deployment=receiver_deployment,
        run_id=run_id,
        sender_keys=sender_keys,
        receiver_keys=receiver_keys,
        trust_pins=trust_pins,
        policy_path=policy_path,
        object_store_root=object_store_root,
    )

    duplicate_export = _prepare_export(
        sender_dsn=sender_dsn,
        sender_deployment=sender_deployment,
        run_id=run_id,
        scenario="duplicate",
        graph_id=f"poros:graph:hardening:{run_id}:duplicate",
        classification="public",
        root_subtype="context_packet",
        text="Poros duplicate replay context packet.",
        sender_keys=sender_keys,
        detach=False,
        object_store_root=object_store_root,
    )
    before_first_duplicate = _receiver_counts(receiver_dsn)
    first_duplicate = _submit_envelope(
        receiver_dsn=receiver_dsn,
        receiver_deployment=receiver_deployment,
        sender_dsn=sender_dsn,
        receiver_keys=receiver_keys,
        trust_pins=trust_pins,
        envelope=duplicate_export.envelope,
        policy_path=policy_path,
        object_store_root=object_store_root,
        apply=True,
        publish=True,
    )
    after_first_duplicate = _receiver_counts(receiver_dsn)
    before_replay = after_first_duplicate
    replay_duplicate = _submit_envelope(
        receiver_dsn=receiver_dsn,
        receiver_deployment=receiver_deployment,
        sender_dsn=sender_dsn,
        receiver_keys=receiver_keys,
        trust_pins=trust_pins,
        envelope=duplicate_export.envelope,
        policy_path=policy_path,
        object_store_root=object_store_root,
        apply=True,
        publish=True,
    )
    after_replay = _receiver_counts(receiver_dsn)
    scenarios["duplicate"] = _scenario_payload(
        receiver_dsn=receiver_dsn,
        sender_dsn=sender_dsn,
        export=duplicate_export,
        outcome=first_duplicate,
        before=before_replay,
        after=after_replay,
        expected_publication="first_delivery_only",
        extra={
            "first_delivery": first_duplicate.payload,
            "first_delivery_row_delta": _count_delta(
                before_first_duplicate,
                after_first_duplicate,
            ),
            "replay_delivery": replay_duplicate.payload,
            "replay_row_delta": _count_delta(before_replay, after_replay),
            "replay_created_generation": (
                _latest_published_generation_for_message(
                    receiver_dsn,
                    duplicate_export.message_id,
                )
                if _count_delta(before_replay, after_replay)["published_generations"]
                else None
            ),
        },
    )

    scenarios["transport_failed"] = _run_transport_failure_scenario(
        sender_dsn=sender_dsn,
        receiver_dsn=receiver_dsn,
        sender_deployment=sender_deployment,
        run_id=run_id,
        sender_keys=sender_keys,
        receiver_keys=receiver_keys,
        object_store_root=object_store_root,
    )

    scenarios["detached"] = _run_export_submit_scenario(
        sender_dsn=sender_dsn,
        receiver_dsn=receiver_dsn,
        sender_deployment=sender_deployment,
        receiver_deployment=receiver_deployment,
        run_id=run_id,
        scenario="detached",
        graph_id=f"poros:graph:hardening:{run_id}:detached",
        classification="public",
        root_subtype="context_packet",
        text="Detached Poros context packet. " + ("x" * (140 * 1024)),
        sender_keys=sender_keys,
        receiver_keys=receiver_keys,
        trust_pins=trust_pins,
        policy_path=policy_path,
        object_store_root=object_store_root,
        detach=True,
        expected_publication="published",
    )

    review = _run_export_submit_scenario(
        sender_dsn=sender_dsn,
        receiver_dsn=receiver_dsn,
        sender_deployment=sender_deployment,
        receiver_deployment=receiver_deployment,
        run_id=run_id,
        scenario="review",
        graph_id=f"poros:graph:hardening:{run_id}:review",
        classification="internal",
        root_subtype="context_packet",
        text="Internal Poros context packet that requires receiver review.",
        sender_keys=sender_keys,
        receiver_keys=receiver_keys,
        trust_pins=trust_pins,
        policy_path=policy_path,
        object_store_root=object_store_root,
        expected_publication="none",
    )
    scenarios["review"] = review

    scenarios["quarantine"] = _run_export_submit_scenario(
        sender_dsn=sender_dsn,
        receiver_dsn=receiver_dsn,
        sender_deployment=sender_deployment,
        receiver_deployment=receiver_deployment,
        run_id=run_id,
        scenario="quarantine",
        graph_id=f"poros:graph:hardening:{run_id}:quarantine",
        classification="restricted",
        root_subtype="context_packet",
        text="Restricted Poros context packet held in quarantine.",
        sender_keys=sender_keys,
        receiver_keys=receiver_keys,
        trust_pins=trust_pins,
        policy_path=policy_path,
        object_store_root=object_store_root,
        expected_publication="none",
    )

    promotion_before = _receiver_counts(receiver_dsn)
    promotion_outcome = promote_poros_message(
        dsn=receiver_dsn,
        deployment=receiver_deployment,
        actor=PROMOTION_ACTOR,
        nomos_bundle=policy_path,
        message_id=str(review["message_id"]),
        promotion_envelope=review["envelope"],
        publish=True,
    )
    promotion_after = _receiver_counts(receiver_dsn)
    scenarios["promotion"] = {
        "scenario": "promotion",
        "source_scenario": "review",
        "message_id": review["message_id"],
        "graph_id": review["graph_id"],
        "body_sha256": review["body_sha256"],
        "expected_publication": "published",
        "promotion": promotion_outcome.payload,
        "row_delta": _count_delta(promotion_before, promotion_after),
        "event_counts": _event_counts(receiver_dsn, str(review["message_id"])),
        "receiver_summary": _receiver_message_summary(
            receiver_dsn,
            str(review["message_id"]),
        ),
        "sender_delivery": _sender_delivery_summary(
            sender_dsn,
            str(review["message_id"]),
        ),
    }

    scenarios["redaction"] = _run_export_submit_scenario(
        sender_dsn=sender_dsn,
        receiver_dsn=receiver_dsn,
        sender_deployment=sender_deployment,
        receiver_deployment=receiver_deployment,
        run_id=run_id,
        scenario="redaction",
        graph_id=f"poros:graph:hardening:{run_id}:redaction",
        classification="restricted",
        root_subtype="context_packet",
        text="Restricted detached Poros packet used to prove public redaction. "
        + ("sensitive " * 12_000),
        sender_keys=sender_keys,
        receiver_keys=receiver_keys,
        trust_pins=trust_pins,
        policy_path=policy_path,
        object_store_root=object_store_root,
        detach=True,
        expected_publication="none",
    )

    _assert_publication_expectations(scenarios)
    formal = _write_service_refinement_artifacts(
        sender_dsn=sender_dsn,
        receiver_dsn=receiver_dsn,
        message_id=str(scenarios["allowed"]["message_id"]),
        out_dir=artifact_dir / "formal",
    )
    readiness = _write_readiness_artifacts(
        artifact_dir=artifact_dir,
        setup=setup.to_dict(),
        scenarios=scenarios,
        policy=policy,
        policy_path=policy_path,
        object_store_root=object_store_root,
        sender_keys=sender_keys,
        receiver_keys=receiver_keys,
        formal=formal,
        receiver_metrics=_metrics_snapshot(receiver_dsn),
    )

    payload = {
        "status": "ok",
        "harness": "poros-alpha-hardening",
        "run_id": run_id,
        "artifact_dir": str(artifact_dir),
        "setup": setup.to_dict(),
        "authorities": {
            "sender": SENDER_AUTHORITY,
            "receiver": RECEIVER_AUTHORITY,
            "promotion_actor": PROMOTION_ACTOR,
        },
        "artifacts": {
            "sender_keys": str(artifact_dir / "sender.keys.json"),
            "receiver_keys": str(artifact_dir / "receiver.keys.json"),
            "trust_pins": str(artifact_dir / "trust-pins.json"),
            "nomos_bundle": str(policy_path),
            "object_store_root": str(object_store_root),
            "sender_manifest": readiness["artifacts"]["sender_manifest"],
            "receiver_manifest": readiness["artifacts"]["receiver_manifest"],
            "readiness_local_demo": readiness["artifacts"]["local_demo_report"],
            "readiness_pilot": readiness["artifacts"]["pilot_report"],
            "formal_refinement_trace": formal["trace_path"],
            "formal_refinement_state": formal["artifact_path"],
        },
        "policy_bundle": {
            "policy_bundle_id": policy.get("policy_bundle_id"),
            "policy_fingerprint": policy.get("policy_fingerprint"),
            "path": str(policy_path),
        },
        "readiness": readiness,
        "scenarios": scenarios,
        "totals": {
            "sender": _sender_counts(sender_dsn),
            "receiver": _receiver_counts(receiver_dsn),
            "receiver_event_counts": _all_event_counts(receiver_dsn),
        },
        "doctor": {
            "sender": database_doctor(sender_dsn),
            "receiver": database_doctor(receiver_dsn),
        },
        "metrics": {
            "sender": _metrics_snapshot(sender_dsn),
            "receiver": _metrics_snapshot(receiver_dsn),
        },
        "verification_commands": _verification_commands(
            sender_dsn_placeholder="<sender-dsn>",
            receiver_dsn_placeholder="<receiver-dsn>",
            receiver_deployment=receiver_deployment,
            messages=scenarios,
        ),
    }
    report_path = artifact_dir / "hardening-harness-result.json"
    _write_json(report_path, payload)
    payload["artifacts"]["result"] = str(report_path)
    return payload


def _write_service_refinement_artifacts(
    *,
    sender_dsn: str,
    receiver_dsn: str,
    message_id: str,
    out_dir: Path,
) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    trace_path = out_dir / "runtime-trace.json"
    state_path = out_dir / "refinement-state.json"
    error_path = out_dir / "refinement-error.txt"
    if error_path.exists():
        error_path.unlink()
    try:
        from okg.substrate.exchange.refinement_db import build_trace_from_databases

        trace = build_trace_from_databases(
            sender_dsn=sender_dsn,
            receiver_dsn=receiver_dsn,
            message_id=message_id,
        )
        state = build_refinement_state_from_trace(trace)
    except Exception as exc:  # noqa: BLE001 - harness writes durable error evidence.
        error_path.write_text(str(exc) + "\n", encoding="utf-8")
        raise PorosHardeningHarnessError(
            f"Poros service-trace refinement failed; see {error_path}: {exc}"
        ) from exc
    _write_json(trace_path, trace)
    _write_json(state_path, state)
    return {
        "status": "pass",
        "fresh": True,
        "message_id": message_id,
        "trace_path": str(trace_path),
        "artifact_path": str(state_path),
    }


def _write_readiness_artifacts(
    *,
    artifact_dir: Path,
    setup: dict[str, Any],
    scenarios: dict[str, Any],
    policy: dict[str, Any],
    policy_path: Path,
    object_store_root: Path,
    sender_keys: Ed25519KeyPair,
    receiver_keys: Ed25519KeyPair,
    formal: dict[str, Any],
    receiver_metrics: dict[str, int],
) -> dict[str, Any]:
    sender_manifest_path = artifact_dir / "sender.manifest.json"
    manifest_path = artifact_dir / "receiver.manifest.json"
    local_report_path = artifact_dir / "readiness-local-demo.json"
    pilot_report_path = artifact_dir / "readiness-pilot.json"
    sender_manifest = create_manifest(
        authority_id=SENDER_AUTHORITY,
        roles=["sender"],
        signing_key=signing_key_ref(
            kid=sender_keys.kid,
            public_key=sender_keys.public_key,
            private_key_path=str(artifact_dir / "sender.keys.json"),
        ),
        object_store_root=str(object_store_root),
        nomos_bundle_path=str(policy_path),
        formal_level="local-demo",
    )
    add_peer(
        sender_manifest,
        authority_id=RECEIVER_AUTHORITY,
        kid=receiver_keys.kid,
        public_key=receiver_keys.public_key,
        endpoint="inprocess://poros-hardening-harness",
        display_name="Poros hardening harness receiver",
    )
    receiver_manifest = create_manifest(
        authority_id=RECEIVER_AUTHORITY,
        roles=["receiver"],
        signing_key=signing_key_ref(
            kid=receiver_keys.kid,
            public_key=receiver_keys.public_key,
            private_key_path=str(artifact_dir / "receiver.keys.json"),
        ),
        receiver_endpoint="inprocess://poros-hardening-harness",
        object_store_root=str(object_store_root),
        nomos_bundle_path=str(policy_path),
        formal_level="pilot",
    )
    receiver_manifest.nomos["policy_fingerprint"] = policy.get("policy_fingerprint")
    add_peer(
        receiver_manifest,
        authority_id=SENDER_AUTHORITY,
        kid=sender_keys.kid,
        public_key=sender_keys.public_key,
        display_name="Poros hardening harness sender",
    )
    evidence = _readiness_evidence_from_harness(
        setup=setup,
        scenarios=scenarios,
        policy=policy,
        object_store_root=object_store_root,
        formal=formal,
        receiver_metrics=receiver_metrics,
    )
    sender_manifest.readiness_evidence = evidence
    receiver_manifest.readiness_evidence = evidence
    save_manifest(sender_manifest, sender_manifest_path)
    save_manifest(receiver_manifest, manifest_path)
    local_report = readiness_report(receiver_manifest, level="local-demo")
    pilot_report = readiness_report(receiver_manifest, level="pilot")
    _write_json(local_report_path, local_report)
    _write_json(pilot_report_path, pilot_report)
    if not local_report["ready"] or not pilot_report["ready"]:
        raise PorosHardeningHarnessError(
            "Poros readiness artifact generation produced a non-ready report; "
            f"see {pilot_report_path}"
        )
    return {
        "status": "ok",
        "ready": bool(pilot_report["ready"]),
        "local_demo_ready": bool(local_report["ready"]),
        "pilot_ready": bool(pilot_report["ready"]),
        "sender_manifest": sender_manifest.to_dict(public=True),
        "receiver_manifest": receiver_manifest.to_dict(public=True),
        "evidence": evidence,
        "reports": {
            "local_demo": local_report,
            "pilot": pilot_report,
        },
        "artifacts": {
            "sender_manifest": str(sender_manifest_path),
            "receiver_manifest": str(manifest_path),
            "local_demo_report": str(local_report_path),
            "pilot_report": str(pilot_report_path),
        },
    }


def _readiness_evidence_from_harness(
    *,
    setup: dict[str, Any],
    scenarios: dict[str, Any],
    policy: dict[str, Any],
    object_store_root: Path,
    formal: dict[str, Any],
    receiver_metrics: dict[str, int],
) -> dict[str, Any]:
    allowed = scenarios["allowed"]
    detached = scenarios["detached"]
    allowed_generation = dict(
        allowed.get("receiver_summary", {}).get("published_generation") or {}
    )
    allowed_outbox = dict(allowed.get("sender_delivery", {}).get("outbox") or {})
    allowed_attempt = _first_mapping(allowed.get("sender_delivery", {}).get("attempts"))
    allowed_events = [
        event
        for event in allowed.get("receiver_summary", {}).get("events") or []
        if isinstance(event, dict)
    ]
    allowed_publication = _latest_event(allowed_events, "published")
    allowed_delta = _live_row_delta(allowed.get("row_delta"))
    expected_delta = _expected_live_delta_from_scenario(allowed)
    proof_observed_at = datetime.now(UTC).isoformat()
    return {
        "database_identities": {
            "sender": setup["sender"]["database"],
            "receiver": setup["receiver"]["database"],
            "separate_databases": bool(setup.get("separate_databases")),
        },
        "latest_transfer_proof": {
            "proof_kind": "poros_hardening_harness",
            "proof_version": 1,
            "freshness": {
                "fresh": True,
                "source": "poros_hardening_harness_current_run",
                "observed_at": proof_observed_at,
                "max_age_seconds": 3600,
            },
            "observed_at": proof_observed_at,
            "message_id": allowed["message_id"],
            "graph_id": allowed["graph_id"],
            "state": allowed_generation.get("status"),
            "sender_outbox_status": allowed_outbox.get("status"),
            "receiver_generation": allowed_generation,
            "read_surface": {
                "kind": "chronos_generation",
                "generation_id": allowed_generation.get("generation_id"),
            },
            "nomos_decision": _event_proof_ref(
                _latest_event(allowed_events, "nomos_decided")
            ),
            "admission": _event_proof_ref(_latest_event(allowed_events, "admitted")),
            "publication": _event_proof_ref(allowed_publication),
            "expected_live_row_delta": expected_delta,
            "observed_live_row_delta": allowed_delta,
            "live_row_delta": allowed_delta,
            "body_sha256": allowed.get("body_sha256"),
            "envelope_sha256": allowed.get("envelope_sha256"),
        },
        "object_store": {
            "verified": (
                object_store_root.exists()
                and bool(detached.get("detached_objects", {}).get("sender"))
                and bool(detached.get("detached_objects", {}).get("receiver"))
            ),
            "backend": "local",
            "root": str(object_store_root),
            "detached_message_id": detached["message_id"],
            "sender_object_count": len(
                detached.get("detached_objects", {}).get("sender") or []
            ),
            "receiver_object_count": len(
                detached.get("detached_objects", {}).get("receiver") or []
            ),
        },
        "service_delivery": {
            "status": allowed_outbox.get("status"),
            "attempt_status": allowed_attempt.get("status"),
            "transport": allowed_attempt.get("transport"),
            "message_id": allowed["message_id"],
            "receipt_sha256": allowed_attempt.get("receipt_sha256"),
        },
        "failure_fixtures": {
            "replay": _failure_fixture_from_scenario(
                scenarios["duplicate"],
                delta_key="replay_row_delta",
                state="duplicate",
            ),
            "untrusted_or_disabled_peer": _failure_fixture_from_scenario(
                scenarios["untrusted_peer"],
                state="rejected_preflight",
            ),
            "tampered_payload_or_object": _failure_fixture_from_scenario(
                scenarios["rejected"],
                state="rejected_preflight",
            ),
            "deny": _failure_fixture_from_scenario(
                scenarios["deny"],
                state="deny",
            ),
            "review": _failure_fixture_from_scenario(
                scenarios["review"],
                state="review",
            ),
            "quarantine": _failure_fixture_from_scenario(
                scenarios["quarantine"],
                state="quarantine",
            ),
            "malformed": _failure_fixture_from_scenario(
                scenarios["malformed"],
                state="malformed",
            ),
            "transport_failed": _failure_fixture_from_scenario(
                scenarios["transport_failed"],
                state="transport_failed",
            ),
        },
        "metrics": {
            **receiver_metrics,
            "policy_fingerprint": policy.get("policy_fingerprint"),
        },
        "doctor": setup["receiver"]["doctor"],
        "service_refinement": formal,
    }


def _first_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, list) and value and isinstance(value[0], dict):
        return dict(value[0])
    return {}


def _raise_simulated_transport_timeout(*_args: Any, **_kwargs: Any) -> None:
    raise TimeoutError("simulated receiver transport failure")


def _live_row_delta(value: Any) -> dict[str, int]:
    raw = value if isinstance(value, dict) else {}
    return {
        "nodes_live": int(raw.get("nodes_live") or 0),
        "edges_live": int(raw.get("edges_live") or 0),
    }


def _expected_live_delta_from_scenario(scenario: dict[str, Any]) -> dict[str, int]:
    envelope = scenario.get("envelope") if isinstance(scenario, dict) else {}
    body = envelope.get("body") if isinstance(envelope, dict) else {}
    body = body if isinstance(body, dict) else {}
    roots = (
        body.get("root_node_ids") if isinstance(body.get("root_node_ids"), list) else []
    )
    nodes = body.get("nodes") if isinstance(body.get("nodes"), list) else roots
    edges = body.get("edges") if isinstance(body.get("edges"), list) else []
    return {
        "nodes_live": len(nodes),
        "edges_live": len(edges),
    }


def _latest_event(events: list[dict[str, Any]], event_type: str) -> dict[str, Any]:
    return next(
        (
            dict(event)
            for event in reversed(events)
            if event.get("event_type") == event_type
        ),
        {},
    )


def _event_proof_ref(event: dict[str, Any]) -> dict[str, Any]:
    if not event:
        return {}
    return {
        "event_type": event.get("event_type"),
        "status": event.get("status"),
        "reason_code": event.get("reason_code"),
        "nomos_decision_id": event.get("nomos_decision_id"),
        "policy_fingerprint": event.get("policy_fingerprint"),
        "nomos_input_hash": event.get("nomos_input_hash"),
        "admission_id": event.get("admission_id"),
        "receiver_generation_id": event.get("receiver_generation_id"),
    }


def _failure_fixture_from_scenario(
    scenario: dict[str, Any],
    *,
    state: str,
    delta_key: str = "row_delta",
) -> dict[str, Any]:
    fixture = {
        "state": state,
        "durable_evidence": True,
        "message_id": scenario.get("message_id"),
        "graph_id": scenario.get("graph_id"),
        "body_sha256": scenario.get("body_sha256"),
        "live_row_delta": _live_row_delta(scenario.get(delta_key)),
        "event_counts": dict(scenario.get("event_counts") or {}),
    }
    for key in (
        "delivery_attempt",
        "delivery_result",
        "receiver_summary",
        "sender_delivery",
    ):
        if scenario.get(key) is not None:
            fixture[key] = scenario.get(key)
    return fixture


def _run_export_submit_scenario(
    *,
    sender_dsn: str,
    receiver_dsn: str,
    sender_deployment: str,
    receiver_deployment: str,
    run_id: str,
    scenario: str,
    graph_id: str,
    classification: str,
    root_subtype: str,
    text: str,
    sender_keys: Ed25519KeyPair,
    receiver_keys: Ed25519KeyPair,
    trust_pins: dict[str, Any],
    policy_path: Path,
    object_store_root: Path,
    expected_publication: str,
    detach: bool = False,
    mutate_envelope: Any | None = None,
) -> dict[str, Any]:
    export = _prepare_export(
        sender_dsn=sender_dsn,
        sender_deployment=sender_deployment,
        run_id=run_id,
        scenario=scenario,
        graph_id=graph_id,
        classification=classification,
        root_subtype=root_subtype,
        text=text,
        sender_keys=sender_keys,
        detach=detach,
        object_store_root=object_store_root,
    )
    envelope = (
        mutate_envelope(_json_clone(export.envelope))
        if mutate_envelope is not None
        else export.envelope
    )
    before = _receiver_counts(receiver_dsn)
    outcome = _submit_envelope(
        receiver_dsn=receiver_dsn,
        receiver_deployment=receiver_deployment,
        sender_dsn=sender_dsn,
        receiver_keys=receiver_keys,
        trust_pins=trust_pins,
        envelope=envelope,
        policy_path=policy_path,
        object_store_root=object_store_root,
        apply=True,
        publish=True,
    )
    after = _receiver_counts(receiver_dsn)
    return _scenario_payload(
        receiver_dsn=receiver_dsn,
        sender_dsn=sender_dsn,
        export=export,
        outcome=outcome,
        before=before,
        after=after,
        expected_publication=expected_publication,
        envelope=envelope,
    )


def _run_malformed_envelope_scenario(
    *,
    sender_dsn: str,
    receiver_dsn: str,
    sender_deployment: str,
    receiver_deployment: str,
    run_id: str,
    sender_keys: Ed25519KeyPair,
    receiver_keys: Ed25519KeyPair,
    trust_pins: dict[str, Any],
    policy_path: Path,
    object_store_root: Path,
) -> dict[str, Any]:
    export = _prepare_export(
        sender_dsn=sender_dsn,
        sender_deployment=sender_deployment,
        run_id=run_id,
        scenario="malformed",
        graph_id=f"poros:graph:hardening:{run_id}:malformed",
        classification="public",
        root_subtype="context_packet",
        text="Malformed Poros envelope rejected before admission.",
        sender_keys=sender_keys,
        detach=False,
        object_store_root=object_store_root,
    )
    envelope = _json_clone(export.envelope)
    envelope.pop("recipient", None)
    before = _receiver_counts(receiver_dsn)
    outcome = _submit_envelope(
        receiver_dsn=receiver_dsn,
        receiver_deployment=receiver_deployment,
        sender_dsn=sender_dsn,
        receiver_keys=receiver_keys,
        trust_pins=trust_pins,
        envelope=envelope,
        policy_path=policy_path,
        object_store_root=object_store_root,
        apply=True,
        publish=True,
    )
    after = _receiver_counts(receiver_dsn)
    return _scenario_payload(
        receiver_dsn=receiver_dsn,
        sender_dsn=sender_dsn,
        export=export,
        outcome=outcome,
        before=before,
        after=after,
        expected_publication="none",
        envelope=envelope,
    )


def _run_transport_failure_scenario(
    *,
    sender_dsn: str,
    receiver_dsn: str,
    sender_deployment: str,
    run_id: str,
    sender_keys: Ed25519KeyPair,
    receiver_keys: Ed25519KeyPair,
    object_store_root: Path,
) -> dict[str, Any]:
    export = _prepare_export(
        sender_dsn=sender_dsn,
        sender_deployment=sender_deployment,
        run_id=run_id,
        scenario="transport_failed",
        graph_id=f"poros:graph:hardening:{run_id}:transport_failed",
        classification="public",
        root_subtype="context_packet",
        text="Poros packet whose receiver transport fails before delivery.",
        sender_keys=sender_keys,
        detach=False,
        object_store_root=object_store_root,
    )
    before = _receiver_counts(receiver_dsn)
    receiver_url = "http://receiver.local/poros/v1/inbox"
    delivery_results = deliver_next_outbox_messages(
        dsn=sender_dsn,
        receiver_inbox_url=receiver_url,
        receiver_public_key=receiver_keys.public_key,
        message_id=export.message_id,
        max_attempts=1,
        retry_delay_seconds=0,
        post=_raise_simulated_transport_timeout,
        allow_hosts=("receiver.local",),
    )
    if len(delivery_results) != 1:
        raise PorosHardeningHarnessError(
            "transport failure scenario did not claim exactly one outbox message"
        )
    delivery_result = delivery_results[0].to_dict()
    after = _receiver_counts(receiver_dsn)
    sender_delivery = _sender_delivery_summary(sender_dsn, export.message_id)
    delivery_attempt = _first_mapping(sender_delivery.get("attempts"))
    return {
        "scenario": "transport_failed",
        "message_id": export.message_id,
        "graph_id": export.graph_id,
        "node_id": export.node_id,
        "body_sha256": export.export.body_sha256,
        "envelope_sha256": export.export.envelope_sha256,
        "classification": export.export.classification,
        "payload_mode": export.export.payload_mode,
        "detached_object_count": export.export.detached_object_count,
        "sender_generation_id": export.sender_generation_id,
        "expected_publication": "none",
        "delivery_attempt": delivery_attempt,
        "delivery_result": delivery_result,
        "receiver_inbox_url": receiver_url,
        "row_delta": _count_delta(before, after),
        "event_counts": _transport_failure_event_counts(
            sender_dsn,
            export.message_id,
        ),
        "receiver_summary": _receiver_message_summary(receiver_dsn, export.message_id),
        "sender_delivery": sender_delivery,
        "detached_objects": _detached_object_summary(
            sender_dsn=sender_dsn,
            receiver_dsn=receiver_dsn,
            message_id=export.message_id,
        ),
        "proof_visibility": _proof_visibility_summary(receiver_dsn, export.message_id),
        "envelope": export.envelope,
    }


def _prepare_export(
    *,
    sender_dsn: str,
    sender_deployment: str,
    run_id: str,
    scenario: str,
    graph_id: str,
    classification: str,
    root_subtype: str,
    text: str,
    sender_keys: Ed25519KeyPair,
    detach: bool,
    object_store_root: Path,
) -> _ScenarioExport:
    message_id = f"poros-hardening-{scenario}-{run_id}"
    node_id = f"poros:hardening:{run_id}:{scenario}:root"
    sender_generation_id = _insert_sender_node_and_publish(
        dsn=sender_dsn,
        deployment=sender_deployment,
        run_id=run_id,
        scenario=scenario,
        node_id=node_id,
        subtype=root_subtype,
        text=text,
    )
    export = export_published_subgraph_to_outbox(
        dsn=sender_dsn,
        sender_authority_id=SENDER_AUTHORITY,
        recipient_authority_id=RECEIVER_AUTHORITY,
        sender_generation_id=sender_generation_id,
        graph_id=graph_id,
        root_node_ids=[node_id],
        signing_private_key=sender_keys.private_key,
        signing_key_id=sender_keys.kid,
        classification=classification,
        graph_profile="context",
        message_id=message_id,
        object_store_root=object_store_root,
        detach_payload=detach,
    )
    return _ScenarioExport(
        message_id=message_id,
        graph_id=graph_id,
        node_id=node_id,
        sender_generation_id=sender_generation_id,
        export=export,
        envelope=export.envelope,
    )


def _submit_envelope(
    *,
    receiver_dsn: str,
    receiver_deployment: str,
    sender_dsn: str,
    receiver_keys: Ed25519KeyPair,
    trust_pins: dict[str, Any],
    envelope: dict[str, Any],
    policy_path: Path,
    object_store_root: Path,
    apply: bool,
    publish: bool,
) -> PorosReceiverSubmitOutcome:
    outcome = submit_poros_envelope_bytes(
        canonical_json_bytes(envelope),
        trust_pins=trust_pins,
        receiver_keys=receiver_keys.__dict__,
        receiver=RECEIVER_AUTHORITY,
        dsn=receiver_dsn,
        sender_dsn=sender_dsn,
        deployment=receiver_deployment,
        persist_preflight=True,
        apply=apply,
        publish=publish,
        nomos_bundle=policy_path,
        object_store_root=object_store_root,
    )
    _resolve_sender_outbox_after_callback(
        sender_dsn=sender_dsn,
        message_id=str(envelope["message_id"]),
        outcome=outcome,
    )
    return outcome


def _resolve_sender_outbox_after_callback(
    *,
    sender_dsn: str,
    message_id: str,
    outcome: PorosReceiverSubmitOutcome,
) -> None:
    from okg.substrate.exchange.sender_delivery import resolve_outbox_after_attempt

    payload = outcome.payload
    preflight = (
        payload.get("preflight") if isinstance(payload.get("preflight"), dict) else {}
    )
    attempted_status = str(preflight.get("status") or payload.get("status") or "failed")
    reason_code = preflight.get("reason_code") or payload.get("reason_code")
    desired_status = (
        "delivered"
        if attempted_status in {"accepted_for_admission", "duplicate"}
        else "terminal_failed"
    )
    resolve_outbox_after_attempt(
        dsn=sender_dsn,
        message_id=message_id,
        attempted_status=attempted_status,
        desired_status=desired_status,
        reason_code=str(reason_code) if reason_code is not None else None,
        response=payload,
        max_attempts=1,
        retry_delay_seconds=0,
    )


def _scenario_payload(
    *,
    receiver_dsn: str,
    sender_dsn: str,
    export: _ScenarioExport,
    outcome: PorosReceiverSubmitOutcome,
    before: dict[str, int],
    after: dict[str, int],
    expected_publication: str,
    envelope: dict[str, Any] | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    message_id = export.message_id
    payload = {
        "scenario": message_id.split("-")[2],
        "message_id": message_id,
        "graph_id": export.graph_id,
        "node_id": export.node_id,
        "body_sha256": export.export.body_sha256,
        "envelope_sha256": export.export.envelope_sha256,
        "classification": export.export.classification,
        "payload_mode": export.export.payload_mode,
        "detached_object_count": export.export.detached_object_count,
        "sender_generation_id": export.sender_generation_id,
        "expected_publication": expected_publication,
        "submit_exit_code": outcome.exit_code,
        "submit_http_status_code": outcome.http_status_code,
        "submit": outcome.payload,
        "row_delta": _count_delta(before, after),
        "event_counts": _event_counts(receiver_dsn, message_id),
        "receiver_summary": _receiver_message_summary(receiver_dsn, message_id),
        "sender_delivery": _sender_delivery_summary(sender_dsn, message_id),
        "detached_objects": _detached_object_summary(
            sender_dsn=sender_dsn,
            receiver_dsn=receiver_dsn,
            message_id=message_id,
        ),
        "proof_visibility": _proof_visibility_summary(receiver_dsn, message_id),
        "envelope": envelope or export.envelope,
    }
    if extra:
        payload.update(extra)
    return payload


def _insert_sender_node_and_publish(
    *,
    dsn: str,
    deployment: str,
    run_id: str,
    scenario: str,
    node_id: str,
    subtype: str,
    text: str,
) -> str:
    fact_run_id = str(uuid.uuid4())
    with db_connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.node_facts (
                node_id, subtype, attrs, source, source_record_id,
                source_revision, observed_at, dedupe_key, run_id, op
            )
            VALUES (
                %s, %s, %s, 'poros_hardening_harness', %s, %s,
                now(), %s, %s::uuid, 'I'
            )
            """,
            (
                node_id,
                subtype,
                Jsonb(
                    {
                        "text": text,
                        "poros_hardening_scenario": scenario,
                        "run_id": run_id,
                    }
                ),
                Jsonb(
                    {
                        "kind": "poros_hardening_harness",
                        "scenario": scenario,
                        "run_id": run_id,
                        "node_id": node_id,
                    }
                ),
                Jsonb({"version": 1, "run_id": run_id}),
                f"poros-hardening:{run_id}:{scenario}:{node_id}",
                fact_run_id,
            ),
        )
    publish = build_deployment_publisher(
        dsn=dsn,
        deployment_name=deployment,
        command="poros-hardening-harness",
    ).cycle()
    if publish.get("status") != "published":
        raise PorosHardeningHarnessError(
            f"sender publish for scenario {scenario!r} did not publish: {publish}"
        )
    # Chronos cutover: a published generation is a Chronos checkpoint with a
    # NON-NUMERIC string id. The poros sender_generation_id is TEXT end-to-end,
    # so return the string verbatim (do NOT int()-coerce a chronos generation).
    return str(publish["generation_id"])


def _write_hardening_policy_bundle(out_dir: Path, *, run_id: str) -> Path:
    policy_graph = {
        "policy_bundle_id": "nomos:poros:hardening-harness",
        "policy_version": "v1",
        "source_generation_id": 0,
        "scope": {"deployment": "poros-hardening-harness"},
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_public_poros_context",
                "effect": "allow",
                "priority": 100,
                "subjects": [SENDER_AUTHORITY],
                "actions": ["poros.graph.submit"],
                "match": {
                    "classification": "public",
                    "graph_profile": "context",
                    "root_subtypes": ["context_packet"],
                },
                "reason_code": "nomos.poros_harness.public_context_allowed",
            },
            {
                "rule_id": "review_internal_poros_context",
                "effect": "review",
                "priority": 95,
                "subjects": [SENDER_AUTHORITY],
                "actions": ["poros.graph.submit"],
                "match": {
                    "classification": "internal",
                    "graph_profile": "context",
                    "root_subtypes": ["context_packet"],
                },
                "reason_code": "nomos.poros_harness.internal_context_review",
            },
            {
                "rule_id": "quarantine_restricted_poros_context",
                "effect": "quarantine",
                "priority": 94,
                "subjects": [SENDER_AUTHORITY],
                "actions": ["poros.graph.submit"],
                "match": {
                    "classification": "restricted",
                    "graph_id": f"poros:graph:hardening:{run_id}:quarantine",
                    "graph_profile": "context",
                    "root_subtypes": ["context_packet"],
                },
                "reason_code": "nomos.poros_harness.restricted_context_quarantine",
            },
            {
                "rule_id": "redact_restricted_poros_context",
                "effect": "redact",
                "priority": 93,
                "subjects": [SENDER_AUTHORITY],
                "actions": ["poros.graph.submit"],
                "match": {
                    "classification": "restricted",
                    "graph_id": f"poros:graph:hardening:{run_id}:redaction",
                    "graph_profile": "context",
                    "root_subtypes": ["context_packet"],
                },
                "reason_code": "nomos.poros_harness.restricted_context_redacted",
                "obligations": [
                    {
                        "type": "redaction",
                        "fields": ["body.text", "object_locator"],
                    }
                ],
            },
            {
                "rule_id": "allow_receiver_poros_promotion",
                "effect": "allow",
                "priority": 110,
                "subjects": [PROMOTION_ACTOR],
                "actions": ["poros.graph.promote"],
                "match": {"original_effect": ["review", "quarantine"]},
                "reason_code": "nomos.poros_harness.receiver_promotion_allowed",
            },
        ],
    }
    bundle = compile_policy_graph(policy_graph).to_dict()
    path = out_dir / "compiled-policy.json"
    _write_json(path, bundle)
    return path


def _trust_pins(sender_keys: Ed25519KeyPair) -> dict[str, Any]:
    return {
        "pins": [
            {
                "authority_id": SENDER_AUTHORITY,
                "kid": sender_keys.kid,
                "use": "sign",
                "alg": "Ed25519",
                "public_key": sender_keys.public_key,
            }
        ]
    }


def _tamper_signed_body(envelope: dict[str, Any]) -> dict[str, Any]:
    nodes = envelope.get("body", {}).get("nodes")
    if isinstance(nodes, list) and nodes and isinstance(nodes[0], dict):
        attrs = nodes[0].setdefault("attrs", {})
        if isinstance(attrs, dict):
            attrs["text"] = "tampered after signing"
    return envelope


def _assert_clean_poros_runtime(dsn: str, *, role: str) -> None:
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        tables = [
            "poros_outbox_messages",
            "poros_inbox_messages",
            "poros_incorporation_events",
            "poros_delivery_attempts",
        ]
        dirty: dict[str, int] = {}
        for table in tables:
            cur.execute("SELECT to_regclass(%s)", (f"okg.{table}",))
            if cur.fetchone()[0] is None:
                continue
            cur.execute(f"SELECT count(*) FROM okg.{table}")
            count = int(cur.fetchone()[0] or 0)
            if count:
                dirty[table] = count
    if dirty:
        raise PorosHardeningHarnessError(
            f"{role} Poros tables are not clean: {dirty}; pass --allow-dirty "
            "or use fresh databases"
        )


def _ensure_required_extensions(dsn: str, *, role: str) -> None:
    from okg.substrate.db.migrate import REQUIRED_EXTENSIONS

    try:
        with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            for extension in REQUIRED_EXTENSIONS:
                cur.execute(
                    psycopg.sql.SQL("CREATE EXTENSION IF NOT EXISTS {}").format(
                        psycopg.sql.Identifier(extension)
                    )
                )
    except Exception as exc:  # noqa: BLE001 - converted to harness guidance.
        raise PorosHardeningHarnessError(
            f"could not install required Postgres extensions in {role} DB; "
            "install substrate extensions manually or rerun with a privileged "
            f"DSN: {exc}"
        ) from exc


def _receiver_counts(dsn: str) -> dict[str, int]:
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              (SELECT count(*) FROM okg.node_facts),
              (SELECT count(*) FROM okg.edge_facts),
              (SELECT count(*) FROM okg.admission_envelopes),
              (SELECT count(*) FROM okg.graph_generations WHERE status = 'published'),
              (SELECT count(*) FROM okg.poros_inbox_messages),
              (SELECT count(*) FROM okg.poros_incorporation_events)
            """
        )
        row = cur.fetchone()
        # Chronos cutover: current graph-content counts read the default-branch
        # Chronos store (okg.graph_nodes/graph_edges); the graph_generations
        # count above is metadata and stays on the raw cursor.
        nodes_live, edges_live = _chronos_node_edge_counts(conn)
    keys = [
        "node_facts",
        "edge_facts",
        "admission_envelopes",
        "published_generations",
        "poros_inbox_messages",
        "poros_events",
    ]
    counts = {key: int(value or 0) for key, value in zip(keys, row, strict=True)}
    counts["nodes_live"] = nodes_live
    counts["edges_live"] = edges_live
    return counts


def _sender_counts(dsn: str) -> dict[str, int]:
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              (SELECT count(*) FROM okg.node_facts),
              (SELECT count(*) FROM okg.graph_generations WHERE status = 'published'),
              (SELECT count(*) FROM okg.poros_outbox_messages),
              (SELECT count(*) FROM okg.poros_delivery_attempts),
              (SELECT count(*) FROM okg.poros_detached_object_manifests)
            """
        )
        row = cur.fetchone()
        # Chronos cutover: current node count reads the default-branch Chronos
        # store (okg.graph_nodes); the graph_generations count above is metadata.
        nodes_live, _ = _chronos_node_edge_counts(conn)
    keys = [
        "node_facts",
        "published_generations",
        "poros_outbox_messages",
        "poros_delivery_attempts",
        "poros_detached_object_manifests",
    ]
    counts = {key: int(value or 0) for key, value in zip(keys, row, strict=True)}
    counts["nodes_live"] = nodes_live
    return counts


def _chronos_node_edge_counts(conn: psycopg.Connection) -> tuple[int, int]:
    """Current default-branch graph_nodes/graph_edges counts via Chronos."""
    from okg.substrate.graph_store import ChronosGraphStore

    graph = ChronosGraphStore.from_connection(conn, ensure_schema=False).checkout()
    nodes = int(graph.query("SELECT count(*) AS n FROM okg.graph_nodes")[0]["n"] or 0)
    edges = int(graph.query("SELECT count(*) AS n FROM okg.graph_edges")[0]["n"] or 0)
    return nodes, edges


def _count_delta(before: dict[str, int], after: dict[str, int]) -> dict[str, int]:
    return {
        key: int(after.get(key, 0) - before.get(key, 0))
        for key in sorted(set(before) | set(after))
    }


def _event_counts(dsn: str, message_id: str) -> dict[str, int]:
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT event_type, count(*)
              FROM okg.poros_incorporation_events
             WHERE message_id = %s
             GROUP BY event_type
             ORDER BY event_type
            """,
            (message_id,),
        )
        rows = cur.fetchall()
    return {str(event): int(count or 0) for event, count in rows}


def _transport_failure_event_counts(dsn: str, message_id: str) -> dict[str, int]:
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*)
              FROM okg.poros_delivery_attempts
             WHERE message_id = %s
               AND status = 'failed'
               AND reason_code = 'delivery_failed'
               AND receipt_sha256 IS NULL
               AND transport = 'http'
            """,
            (message_id,),
        )
        count = int(cur.fetchone()[0] or 0)
    return {"transport_failed": count} if count else {}


def _all_event_counts(dsn: str) -> dict[str, int]:
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT event_type, count(*)
              FROM okg.poros_incorporation_events
             GROUP BY event_type
             ORDER BY event_type
            """
        )
        rows = cur.fetchall()
    return {str(event): int(count or 0) for event, count in rows}


def _receiver_message_summary(dsn: str, message_id: str) -> dict[str, Any]:
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, reason_code, body_sha256, envelope
              FROM okg.poros_inbox_messages
             WHERE message_id = %s
            """,
            (message_id,),
        )
        inbox = cur.fetchone()
        cur.execute(
            """
            SELECT event_type, status, reason_code, receiver_generation_id,
                   admission_id, policy_bundle_id, policy_fingerprint
              FROM okg.poros_incorporation_events
             WHERE message_id = %s
             ORDER BY event_id
            """,
            (message_id,),
        )
        events = cur.fetchall()
        cur.execute(
            """
            SELECT receiver_generation_id
              FROM okg.poros_incorporation_events
             WHERE message_id = %s
               AND event_type = 'published'
               AND status = 'published'
             ORDER BY event_id DESC
             LIMIT 1
            """,
            (message_id,),
        )
        generation_row = cur.fetchone()
    envelope = inbox[3] if inbox and isinstance(inbox[3], dict) else {}
    body = envelope.get("body") if isinstance(envelope.get("body"), dict) else {}
    # receiver_generation_id is the chronos-native string checkpoint id (TEXT).
    generation_id = generation_row[0] if generation_row else None
    generation = describe_generation(dsn, generation_id) if generation_id else None
    return {
        "inbox_status": inbox[0] if inbox else None,
        "reason_code": inbox[1] if inbox else None,
        "body_sha256": inbox[2] if inbox else None,
        "graph_id": body.get("graph_id") if isinstance(body, dict) else None,
        "classification": body.get("classification")
        if isinstance(body, dict)
        else None,
        "published_generation_id": generation_id,
        "published_generation": generation,
        "events": [
            {
                "event_type": row[0],
                "status": row[1],
                "reason_code": row[2],
                "receiver_generation_id": row[3],
                "admission_id": str(row[4]) if row[4] is not None else None,
                "policy_bundle_id": row[5],
                "policy_fingerprint": row[6],
            }
            for row in events
        ],
    }


def _sender_delivery_summary(dsn: str, message_id: str) -> dict[str, Any]:
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, reason_code, receipt_sha256, transport
              FROM okg.poros_delivery_attempts
             WHERE message_id = %s
             ORDER BY attempt_id
            """,
            (message_id,),
        )
        attempts = cur.fetchall()
        cur.execute(
            """
            SELECT status, sender_generation_id, graph_id, body_sha256
              FROM okg.poros_outbox_messages
             WHERE message_id = %s
            """,
            (message_id,),
        )
        outbox = cur.fetchone()
    return {
        "outbox": (
            {
                "status": outbox[0],
                "sender_generation_id": str(outbox[1]),
                "graph_id": outbox[2],
                "body_sha256": outbox[3],
            }
            if outbox
            else None
        ),
        "attempts": [
            {
                "status": row[0],
                "reason_code": row[1],
                "receipt_sha256": row[2],
                "transport": row[3],
            }
            for row in attempts
        ],
    }


def _detached_object_summary(
    *,
    sender_dsn: str,
    receiver_dsn: str,
    message_id: str,
) -> dict[str, Any]:
    return {
        "sender": _detached_object_rows(sender_dsn, message_id, side="sender"),
        "receiver": _detached_object_rows(receiver_dsn, message_id, side="receiver"),
    }


def _detached_object_rows(
    dsn: str,
    message_id: str,
    *,
    side: str,
) -> list[dict[str, Any]]:
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT object_id, object_sha256, size_bytes, chunk_count,
                   classification, encrypted
              FROM okg.poros_detached_object_manifests
             WHERE message_id = %s
               AND side = %s
             ORDER BY object_id
            """,
            (message_id, side),
        )
        rows = cur.fetchall()
    return [
        {
            "object_id": row[0],
            "object_sha256": row[1],
            "size_bytes": int(row[2] or 0),
            "chunk_count": int(row[3] or 0),
            "classification": row[4],
            "encrypted": bool(row[5]),
        }
        for row in rows
    ]


def _proof_visibility_summary(dsn: str, message_id: str) -> dict[str, Any]:
    summary = _receiver_message_summary(dsn, message_id)
    protected = summary.get("classification") in {
        "restricted",
        "private",
    } or _message_has_protected_objects(dsn, message_id)
    events = summary.get("events", [])
    audit_allowed = any(
        event["event_type"] == "nomos_decided"
        and event["status"] in {"allow", "redact", "declassify"}
        for event in events
    )
    return {
        "public_redaction_expected": bool(protected),
        "audit_allowed": bool(audit_allowed),
        "audit_denied_reason": (
            None if not protected or audit_allowed else "missing_allowed_nomos_decision"
        ),
    }


def _message_has_protected_objects(dsn: str, message_id: str) -> bool:
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT envelope
              FROM okg.poros_inbox_messages
             WHERE message_id = %s
            """,
            (message_id,),
        )
        row = cur.fetchone()
    envelope = row[0] if row and isinstance(row[0], dict) else {}
    body = envelope.get("body") if isinstance(envelope.get("body"), dict) else {}
    objects = body.get("objects") if isinstance(body.get("objects"), list) else []
    for obj in objects:
        if not isinstance(obj, dict):
            continue
        if obj.get("classification") not in {None, "public"}:
            return True
        if "encryption" in obj or "ciphertext_sha256" in obj:
            return True
        if "inline_bytes_b64url" in obj or "locations" in obj:
            return True
    return False


def _latest_published_generation_for_message(
    dsn: str,
    message_id: str,
) -> str | None:
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT receiver_generation_id
              FROM okg.poros_incorporation_events
             WHERE message_id = %s
               AND event_type = 'published'
               AND status = 'published'
             ORDER BY event_id DESC
             LIMIT 1
            """,
            (message_id,),
        )
        row = cur.fetchone()
    return str(row[0]) if row and row[0] is not None else None


def _metrics_snapshot(dsn: str) -> dict[str, int]:
    from okg.substrate.runtime_queue_health import summarize_runtime_queue_health

    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        runtime = summarize_runtime_queue_health(cur)
        cur.execute(
            """
            SELECT
              count(*)
              FROM okg.graph_generations
             WHERE status = 'blocked'
            """
        )
        blocked = cur.fetchone()[0]
    return {
        "queued_jobs": int(runtime.get("active_queue_depth") or 0),
        "dlq_count": int(runtime.get("dlq_count") or 0),
        "runtime_queue_unavailable": int(
            str(runtime.get("status")) == "unavailable"
        ),
        "generation_blocked_count": int(blocked or 0),
    }


def _assert_publication_expectations(scenarios: dict[str, Any]) -> None:
    expected_with_publication = {"allowed", "detached", "promotion"}
    for name, scenario in scenarios.items():
        published_id = scenario.get("receiver_summary", {}).get(
            "published_generation_id"
        )
        if name in expected_with_publication and published_id is None:
            raise PorosHardeningHarnessError(
                f"scenario {name!r} expected a receiver publication"
            )
        if name not in expected_with_publication | {"duplicate"} and published_id:
            raise PorosHardeningHarnessError(
                f"scenario {name!r} published unexpectedly: {published_id}"
            )
    duplicate = scenarios.get("duplicate", {})
    replay_delta = duplicate.get("replay_row_delta", {})
    if int(replay_delta.get("published_generations", 0) or 0) != 0:
        raise PorosHardeningHarnessError(
            "duplicate replay created a receiver generation"
        )


def _verification_commands(
    *,
    sender_dsn_placeholder: str,
    receiver_dsn_placeholder: str,
    receiver_deployment: str,
    messages: dict[str, Any],
) -> dict[str, str]:
    allowed_message = str(messages["allowed"]["message_id"])
    redaction_message = str(messages["redaction"]["message_id"])
    promotion_message = str(messages["promotion"]["message_id"])
    return {
        "receiver_status_allowed": (
            "okg poros status --side receiver "
            f"--dsn {receiver_dsn_placeholder} --message-id {allowed_message} --json"
        ),
        "sender_status_allowed": (
            "okg poros status --side sender "
            f"--dsn {sender_dsn_placeholder} --message-id {allowed_message} --json"
        ),
        "public_redaction_proof": (
            "okg poros proof --mode public "
            f"--dsn {receiver_dsn_placeholder} --message-id {redaction_message} --json"
        ),
        "audit_redaction_proof": (
            "okg poros proof --mode audit "
            f"--dsn {receiver_dsn_placeholder} --message-id {redaction_message} --json"
        ),
        "promotion_proof": (
            "okg poros proof --mode public "
            f"--dsn {receiver_dsn_placeholder} --sender-dsn {sender_dsn_placeholder} "
            f"--message-id {promotion_message} --json"
        ),
        "receiver_metrics": (
            "okg metrics --qa-snapshot "
            f"--deployment {receiver_deployment} --dsn {receiver_dsn_placeholder} "
            "--json"
        ),
        "receiver_doctor": f"okg doctor --dsn {receiver_dsn_placeholder} --json",
    }


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )


def _json_clone(value: dict[str, Any]) -> dict[str, Any]:
    return json.loads(json.dumps(value, sort_keys=True))
