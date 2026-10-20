"""Receiver-side Poros submission service shared by CLI and HTTP."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from okg.substrate.exchange.admission import submit_poros_candidate_to_admission
from okg.substrate.exchange.authority import TrustPinRegistry
from okg.substrate.exchange.canonical_json import canonical_json_bytes, loads_strict_json
from okg.substrate.exchange.errors import PorosError
from okg.substrate.exchange.hashing import sha256_b64url
from okg.substrate.exchange.graph_shape import classify_graph_body
from okg.substrate.exchange.inbox import PorosAdmissionCandidate, PorosInbox
from okg.substrate.exchange.inbox_store import (
    persist_poros_inbox_preflight,
    record_poros_incorporation_lifecycle,
    record_poros_nomos_decision,
    record_poros_promotion_decision,
    record_poros_rejection_decision,
)
from okg.substrate.exchange.object_store import (
    LocalPorosObjectStore,
    load_detached_graph_body_from_store,
    verify_detached_objects_from_store,
)
from okg.substrate.exchange.outbox import record_delivery_attempt
from okg.substrate.exchange.signing import verify_receipt


@dataclass(frozen=True)
class PorosReceiverSubmitOutcome:
    payload: dict[str, Any]
    exit_code: int
    http_status_code: int


@dataclass(frozen=True)
class PorosPromotionOutcome:
    payload: dict[str, Any]
    exit_code: int


@dataclass(frozen=True)
class PorosRejectionOutcome:
    payload: dict[str, Any]
    exit_code: int


def submit_poros_envelope_bytes(
    raw: bytes,
    *,
    trust_pins: dict[str, Any] | list[dict[str, Any]],
    receiver_keys: dict[str, Any],
    receiver: str | None = None,
    dsn: str | None = None,
    sender_dsn: str | None = None,
    deployment: str = "default",
    admission_mode: str = "fast_track",
    persist_preflight: bool = False,
    apply: bool = False,
    publish: bool = False,
    nomos_bundle: Path | str | None = None,
    nomos_identity_claim: dict[str, Any] | None = None,
    object_store_root: Path | str | None = None,
    allow_unsigned_compat: bool = False,
) -> PorosReceiverSubmitOutcome:
    """Verify, persist, optionally admit, and optionally publish an envelope.

    ``allow_unsigned_compat`` is the resolved deployment-manifest opt-in
    (``admission.allow_unsigned_compat``). It defaults to ``False``: an
    envelope whose producer output signature is empty is rejected at admission
    unless the manifest explicitly opts in. Resolve it with
    ``exchange.readiness.resolve_unsigned_compat_admission(manifest)``.
    """
    try:
        parsed = loads_strict_json(raw)
        if not isinstance(parsed, dict):
            raise ValueError("Poros envelope must be a JSON object")
        receiver_id = receiver or str(parsed.get("recipient") or "")
        if not receiver_id:
            raise ValueError("receiver authority id missing; pass --receiver")
        object_store = (
            LocalPorosObjectStore(object_store_root)
            if object_store_root is not None
            else None
        )
        inbox = PorosInbox(
            receiver_authority_id=receiver_id,
            trust_registry=TrustPinRegistry.from_mapping(trust_pins),
            receiver_private_key=str(receiver_keys["private_key"]),
            receiver_signing_kid=str(receiver_keys["kid"]),
            receiver_encryption_private_key=(
                str(receiver_keys["encryption_private_key"])
                if receiver_keys.get("encryption_private_key")
                else None
            ),
            detached_object_verifier=(
                (
                    lambda envelope: verify_detached_objects_from_store(
                        envelope,
                        object_store=object_store,
                    )
                )
                if object_store is not None
                else None
            ),
        )
        result = inbox.handle_bytes(raw)
        if object_store is not None and result.admission_candidate is not None:
            result = _with_hydrated_detached_candidate(result, object_store)
    except PorosError as exc:
        return PorosReceiverSubmitOutcome(
            payload={
                "status": "error",
                "reason_code": exc.reason_code,
                "error": str(exc),
            },
            exit_code=1,
            http_status_code=400,
        )
    except (OSError, KeyError, ValueError) as exc:
        return PorosReceiverSubmitOutcome(
            payload={"status": "error", "error": str(exc)},
            exit_code=1,
            http_status_code=400,
        )

    receipt_verified = False
    if result.receipt is not None and receiver_keys.get("public_key"):
        verify_receipt(result.receipt, str(receiver_keys["public_key"]))
        receipt_verified = True

    nomos_decision: dict[str, Any] | None = None
    admission_payload: dict[str, Any] | None = None
    publish_payload: dict[str, Any] | None = None
    live_counts: dict[str, int] | None = None
    preflight_payload: dict[str, Any] | None = None
    delivery_payload: dict[str, Any] | None = None

    if persist_preflight and not dsn:
        payload = submit_payload(
            result,
            receipt_verified=receipt_verified,
            admission=None,
            publish=None,
            live_counts=None,
            nomos_decision=nomos_decision,
            preflight=None,
            mutation_error="`persist_preflight` requires dsn",
        )
        return PorosReceiverSubmitOutcome(
            payload=payload,
            exit_code=1,
            http_status_code=400,
        )
    if (apply or publish) and not dsn:
        payload = submit_payload(
            result,
            receipt_verified=receipt_verified,
            admission=None,
            publish=None,
            live_counts=None,
            nomos_decision=nomos_decision,
            preflight=None,
            mutation_error="`apply` or `publish` requires dsn",
        )
        return PorosReceiverSubmitOutcome(
            payload=payload,
            exit_code=1,
            http_status_code=400,
        )

    if dsn:
        from okg.substrate.db.connection import connect as db_connect

        if persist_preflight:
            with db_connect(dsn) as conn:
                with conn.transaction():
                    preflight = persist_poros_inbox_preflight(
                        conn,
                        receiver_authority_id=receiver_id,
                        envelope=parsed,
                        result=result,
                        receiver_private_key=str(receiver_keys["private_key"]),
                        receiver_signing_kid=str(receiver_keys["kid"]),
                    )
                    preflight_payload = preflight.to_dict()
            if sender_dsn:
                delivery_payload = record_delivery_attempt_payload(
                    sender_dsn,
                    parsed,
                    status=str(preflight_payload["status"]),
                    reason_code=preflight_payload.get("reason_code"),
                    receipt_sha256=preflight_payload.get("receipt_sha256"),
                    transport="receiver_service_callback",
                )

        if _should_evaluate_nomos(
            result,
            preflight=preflight_payload,
            nomos_bundle=nomos_bundle,
            nomos_identity_claim=nomos_identity_claim,
        ):
            try:
                nomos_decision = evaluate_nomos_decision_for_result(
                    result,
                    parsed,
                    nomos_bundle=Path(str(nomos_bundle)) if nomos_bundle else None,
                    nomos_identity_claim=nomos_identity_claim,
                    dsn=dsn,
                    deployment=deployment,
                    admission_mode=admission_mode,
                )
            except Exception as exc:  # noqa: BLE001 - surfaced as response payload
                payload = submit_payload(
                    result,
                    receipt_verified=receipt_verified,
                    admission=None,
                    publish=None,
                    live_counts=None,
                    nomos_decision=None,
                    preflight=preflight_payload,
                    delivery=delivery_payload,
                    mutation_error=f"Nomos evaluation failed: {exc}",
                )
                apply_preflight_status_override(payload, preflight_payload)
                return PorosReceiverSubmitOutcome(
                    payload=payload,
                    exit_code=1,
                    http_status_code=500,
                )
            with db_connect(dsn) as conn:
                with conn.transaction():
                    record_nomos_decision_event(
                        conn,
                        deployment=deployment,
                        receiver=receiver_id,
                        envelope=parsed,
                        nomos_decision=nomos_decision,
                    )

        if not apply:
            payload = submit_payload(
                result,
                receipt_verified=receipt_verified,
                admission=None,
                publish=None,
                live_counts=None,
                nomos_decision=nomos_decision,
                preflight=preflight_payload,
                delivery=delivery_payload,
                mutation_error=(
                    None
                    if persist_preflight
                    else (
                        "`okg poros submit --dsn` requires --apply before "
                        "admission or publish side effects"
                    )
                ),
            )
            apply_preflight_status_override(payload, preflight_payload)
            return PorosReceiverSubmitOutcome(
                payload=payload,
                exit_code=0 if persist_preflight else 1,
                http_status_code=_http_status_for_payload(payload),
            )
        if nomos_decision and nomos_decision.get("effect") != "allow":
            lifecycle_event_type = nomos_block_lifecycle_event_type(nomos_decision)
            if lifecycle_event_type is not None:
                with db_connect(dsn) as conn:
                    with conn.transaction():
                        record_lifecycle_event(
                            conn,
                            receiver=receiver_id,
                            envelope=parsed,
                            event_type=lifecycle_event_type,
                            status=str(nomos_decision.get("effect")),
                            reason_code=(
                                str(nomos_decision["reason_code"])
                                if nomos_decision.get("reason_code") is not None
                                else None
                            ),
                            payload={
                                "nomos_decision": nomos_poros_provenance(
                                    nomos_decision
                                ),
                                "graph": redacted_graph_summary(
                                    parsed.get("body")
                                ),
                            },
                        )
            payload = submit_payload(
                result,
                receipt_verified=receipt_verified,
                admission=None,
                publish=None,
                live_counts=None,
                nomos_decision=nomos_decision,
                preflight=preflight_payload,
                delivery=delivery_payload,
                mutation_error=(
                    "Nomos policy blocked admission with effect "
                    f"{nomos_decision.get('effect')!r}"
                ),
            )
            return PorosReceiverSubmitOutcome(
                payload=payload,
                exit_code=1,
                http_status_code=202,
            )
        if preflight_payload and preflight_payload.get("status") == "duplicate":
            payload = submit_payload(
                result,
                receipt_verified=receipt_verified,
                admission=None,
                publish=None,
                live_counts=None,
                nomos_decision=nomos_decision,
                preflight=preflight_payload,
                delivery=delivery_payload,
            )
            apply_preflight_status_override(payload, preflight_payload)
            return PorosReceiverSubmitOutcome(
                payload=payload,
                exit_code=1,
                http_status_code=202,
            )
        if result.status != "accepted_for_admission":
            payload = submit_payload(
                result,
                receipt_verified=receipt_verified,
                admission=None,
                publish=None,
                live_counts=None,
                nomos_decision=nomos_decision,
                preflight=preflight_payload,
                delivery=delivery_payload,
            )
            return PorosReceiverSubmitOutcome(
                payload=payload,
                exit_code=1,
                http_status_code=_http_status_for_payload(payload),
            )
        classification_gate = classification_compatibility_gate(
            result.admission_candidate,
            admission_mode=admission_mode,
            nomos_decision=nomos_decision,
        )
        if classification_gate is not None:
            with db_connect(dsn) as conn:
                with conn.transaction():
                    record_lifecycle_event(
                        conn,
                        receiver=receiver_id,
                        envelope=parsed,
                        event_type="quarantined",
                        status="quarantine",
                        reason_code=str(classification_gate["reason_code"]),
                        payload={
                            "classification_gate": classification_gate,
                            "graph": redacted_graph_summary(parsed.get("body")),
                        },
                    )
            payload = submit_payload(
                result,
                receipt_verified=receipt_verified,
                admission=None,
                publish=None,
                live_counts=None,
                nomos_decision=nomos_decision,
                preflight=preflight_payload,
                delivery=delivery_payload,
                mutation_error=str(classification_gate["message"]),
            )
            return PorosReceiverSubmitOutcome(
                payload=payload,
                exit_code=1,
                http_status_code=202,
            )
        with db_connect(dsn, autocommit=True) as conn:
            with conn.transaction():
                admission = submit_poros_candidate_to_admission(
                    conn,
                    deployment_name=deployment,
                    candidate=result.admission_candidate,
                    admission_mode=admission_mode,
                    poros_provenance=nomos_poros_provenance(nomos_decision),
                    allow_unsigned_compat=allow_unsigned_compat,
                )
                admission_payload = admission_result_payload(admission)
                record_lifecycle_event(
                    conn,
                    receiver=receiver_id,
                    envelope=parsed,
                    event_type=admission_lifecycle_event_type(
                        admission_payload,
                        admission_mode=admission_mode,
                    ),
                    status=str(admission_payload["status"]),
                    reason_code=(
                        "poros_admission"
                        if admission_payload["status"] == "accepted"
                        else str(admission_payload["status"])
                    ),
                    admission_id=str(admission_payload["admission_id"]),
                    payload={"admission": admission_payload},
                )
        if publish and admission_payload["status"] == "accepted":
            from okg.substrate.publisher.factory import build_deployment_publisher

            publisher = build_deployment_publisher(
                dsn=dsn,
                deployment_name=deployment,
                command="okg exchange receive",
            )
            publish_payload = publisher.cycle()
            generation_id = publish_payload.get("generation_id")
            if generation_id is not None and result.admission_candidate:
                live_counts = live_counts_for_candidate(
                    dsn,
                    generation_id=generation_id,
                    candidate=result.admission_candidate,
                )
            record_publish_lifecycle_event(
                dsn,
                receiver=receiver_id,
                envelope=parsed,
                admission=admission_payload,
                publish=publish_payload,
                live_counts=live_counts,
            )
    elif _should_evaluate_nomos(
        result,
        preflight=None,
        nomos_bundle=nomos_bundle,
        nomos_identity_claim=nomos_identity_claim,
    ):
        try:
            if nomos_identity_claim is not None:
                raise ValueError("Nomos identity claim evaluation requires dsn")
            nomos_decision = evaluate_nomos_decision_for_result(
                result,
                parsed,
                nomos_bundle=Path(str(nomos_bundle)) if nomos_bundle else None,
                nomos_identity_claim=None,
                dsn=None,
                deployment=deployment,
                admission_mode=admission_mode,
            )
        except Exception as exc:  # noqa: BLE001 - surfaced as response payload
            payload = submit_payload(
                result,
                receipt_verified=receipt_verified,
                admission=None,
                publish=None,
                live_counts=None,
                nomos_decision=None,
                mutation_error=f"Nomos evaluation failed: {exc}",
            )
            return PorosReceiverSubmitOutcome(
                payload=payload,
                exit_code=1,
                http_status_code=500,
            )

    payload = submit_payload(
        result,
        receipt_verified=receipt_verified,
        admission=admission_payload,
        publish=publish_payload,
        live_counts=live_counts,
        nomos_decision=nomos_decision,
        preflight=preflight_payload,
        delivery=delivery_payload,
    )
    apply_preflight_status_override(payload, preflight_payload)
    return PorosReceiverSubmitOutcome(
        payload=payload,
        exit_code=0 if result.status == "accepted_for_admission" else 1,
        http_status_code=_http_status_for_payload(payload),
    )


def promote_poros_message(
    *,
    dsn: str,
    deployment: str,
    actor: str,
    nomos_bundle: Path | str,
    message_id: str | None = None,
    graph_id: str | None = None,
    body_sha256: str | None = None,
    promotion_envelope: dict[str, Any] | None = None,
    publish: bool = False,
) -> PorosPromotionOutcome:
    """Promote a reviewed/quarantined Poros message through normal admission."""
    if sum(value is not None for value in [message_id, graph_id, body_sha256]) != 1:
        raise ValueError("exactly one promotion lookup key is required")
    from okg.substrate.db.connection import connect as db_connect

    admission_payload: dict[str, Any] | None = None
    publish_payload: dict[str, Any] | None = None
    live_counts: dict[str, int] | None = None
    with db_connect(dsn, autocommit=True) as conn:
        inbox = _load_promotion_inbox_row(
            conn,
            message_id=message_id,
            graph_id=graph_id,
            body_sha256=body_sha256,
        )
        if inbox["status"] != "accepted_for_admission":
            raise ValueError("only preflight-accepted Poros messages can be promoted")
        original_decision = _load_original_blocking_decision(
            conn,
            message_id=str(inbox["message_id"]),
        )
        if original_decision["status"] not in {"review", "quarantine"}:
            raise ValueError(
                "only review or quarantine Poros messages can be promoted"
            )
        candidate = _candidate_from_inbox(inbox)
        supplied_mismatch = _promotion_envelope_mismatch(
            promotion_envelope,
            inbox=inbox,
        )
        if supplied_mismatch is not None:
            return _deny_promotion_gate(
                dsn=dsn,
                inbox=inbox,
                candidate=candidate,
                status="payload_mismatch",
                reason_code="poros_promotion_payload_mismatch",
                payload=supplied_mismatch,
            )
        prior_effect = _load_prior_promotion_effect(
            conn,
            message_id=str(inbox["message_id"]),
        )
        if prior_effect is not None:
            return _deny_promotion_gate(
                dsn=dsn,
                inbox=inbox,
                candidate=candidate,
                status="stale",
                reason_code="poros_promotion_stale",
                payload={"prior_effect": prior_effect},
            )
        promotion_decision = evaluate_promotion_decision_for_candidate(
            candidate,
            original_decision=original_decision,
            actor=actor,
            receiver=str(inbox["receiver_authority_id"]),
            bundle_path=Path(str(nomos_bundle)),
            catalog_version_id=current_catalog_version_id(dsn),
        )
        with conn.transaction():
            record_poros_promotion_decision(
                conn,
                deployment_name=deployment,
                receiver_authority_id=str(inbox["receiver_authority_id"]),
                sender_authority_id=str(inbox["sender_authority_id"]),
                message_id=str(inbox["message_id"]),
                body_sha256=str(inbox["body_sha256"]),
                decision=promotion_decision,
            )
            if promotion_decision.get("effect") != "allow":
                record_lifecycle_event(
                    conn,
                    receiver=str(inbox["receiver_authority_id"]),
                    envelope=candidate.envelope,
                    event_type="promotion_denied",
                    status=str(promotion_decision.get("effect")),
                    reason_code=str(promotion_decision.get("reason_code")),
                    payload={
                        "promotion_decision": nomos_poros_provenance(
                            promotion_decision
                        ),
                        "original_decision": _event_decision_summary(
                            original_decision
                        ),
                        "graph": redacted_graph_summary(candidate.body),
                    },
                )
                return PorosPromotionOutcome(
                    payload={
                        "status": "promotion_denied",
                        "message_id": inbox["message_id"],
                        "body_sha256": inbox["body_sha256"],
                        "promotion_decision": promotion_decision,
                        "original_decision": _event_decision_summary(
                            original_decision
                        ),
                        "admission": None,
                        "publish": None,
                        "live_counts": None,
                    },
                    exit_code=1,
                )
            admission = submit_poros_candidate_to_admission(
                conn,
                deployment_name=deployment,
                candidate=candidate,
                admission_mode="fast_track",
                proposal_review_default=False,
                poros_provenance=poros_promotion_provenance(
                    promotion_decision=promotion_decision,
                    original_decision=original_decision,
                    actor=actor,
                ),
            )
            admission_payload = admission_result_payload(admission)
            record_lifecycle_event(
                conn,
                receiver=str(inbox["receiver_authority_id"]),
                envelope=candidate.envelope,
                event_type=admission_lifecycle_event_type(
                    admission_payload,
                    admission_mode="fast_track",
                ),
                status=str(admission_payload["status"]),
                reason_code=(
                    "poros_promotion_admission"
                    if admission_payload["status"] == "accepted"
                    else str(admission_payload["status"])
                ),
                admission_id=str(admission_payload["admission_id"]),
                payload={
                    "admission": admission_payload,
                    "promotion_decision": nomos_poros_provenance(
                        promotion_decision
                    ),
                    "original_decision": _event_decision_summary(
                        original_decision
                    ),
                },
            )

    if publish and admission_payload and admission_payload["status"] == "accepted":
        from okg.substrate.publisher.factory import build_deployment_publisher

        publisher = build_deployment_publisher(
            dsn=dsn,
            deployment_name=deployment,
            command="okg exchange promote",
        )
        publish_payload = publisher.cycle()
        generation_id = publish_payload.get("generation_id")
        if generation_id is not None:
            # A published generation is a Chronos checkpoint with a STRING id;
            # pass it through (checkout_generation resolves int or str). Do NOT
            # int()-coerce -- the chronos-native publish id is not numeric.
            live_counts = live_counts_for_candidate(
                dsn,
                generation_id=generation_id,
                candidate=candidate,
            )
        record_publish_lifecycle_event(
            dsn,
            receiver=str(inbox["receiver_authority_id"]),
            envelope=candidate.envelope,
            admission=admission_payload,
            publish=publish_payload,
            live_counts=live_counts,
        )

    promoted_status = (
        str(publish_payload.get("status"))
        if publish_payload is not None
        else str((admission_payload or {}).get("status") or "unknown")
    )
    # receiver_generation_id carries the chronos-native published generation
    # (a string checkpoint id) in the TEXT column.
    receiver_generation_id = (
        publish_payload.get("generation_id")
        if publish_payload is not None
        else None
    )
    with db_connect(dsn) as conn:
        with conn.transaction():
            record_lifecycle_event(
                conn,
                receiver=str(inbox["receiver_authority_id"]),
                envelope=candidate.envelope,
                event_type="promoted",
                status=promoted_status,
                reason_code="poros_promoted",
                admission_id=(
                    str(admission_payload["admission_id"])
                    if admission_payload is not None
                    else None
                ),
                receiver_generation_id=receiver_generation_id,
                payload={
                    "promotion_decision": nomos_poros_provenance(
                        promotion_decision
                    ),
                    "original_decision": _event_decision_summary(
                        original_decision
                    ),
                    "admission": admission_payload,
                    "publish": publish_payload,
                    "live_counts": live_counts,
                },
            )

    return PorosPromotionOutcome(
        payload={
            "status": "promoted",
            "message_id": inbox["message_id"],
            "body_sha256": inbox["body_sha256"],
            "promotion_decision": promotion_decision,
            "original_decision": _event_decision_summary(original_decision),
            "admission": admission_payload,
            "publish": publish_payload,
            "live_counts": live_counts,
        },
        exit_code=0 if admission_payload and admission_payload["status"] == "accepted" else 1,
    )


def reject_poros_message(
    *,
    dsn: str,
    deployment: str = "default",
    actor: str,
    nomos_bundle: Path | str,
    message_id: str | None = None,
    graph_id: str | None = None,
    body_sha256: str | None = None,
    rejection_envelope: dict[str, Any] | None = None,
    rejection_reason: str | None = None,
) -> PorosRejectionOutcome:
    """Terminally reject a reviewed/quarantined Poros message without admission."""
    if sum(value is not None for value in [message_id, graph_id, body_sha256]) != 1:
        raise ValueError("exactly one rejection lookup key is required")
    from okg.substrate.db.connection import connect as db_connect

    with db_connect(dsn, autocommit=True) as conn:
        inbox = _load_promotion_inbox_row(
            conn,
            message_id=message_id,
            graph_id=graph_id,
            body_sha256=body_sha256,
        )
        if inbox["status"] != "accepted_for_admission":
            raise ValueError("only preflight-accepted Poros messages can be rejected")
        original_decision = _load_original_blocking_decision(
            conn,
            message_id=str(inbox["message_id"]),
        )
        if original_decision["status"] not in {"review", "quarantine"}:
            raise ValueError(
                "only review or quarantine Poros messages can be rejected"
            )
        candidate = _candidate_from_inbox(inbox)
        supplied_mismatch = _promotion_envelope_mismatch(
            rejection_envelope,
            inbox=inbox,
        )
        if supplied_mismatch is not None:
            return _deny_rejection_gate(
                dsn=dsn,
                inbox=inbox,
                candidate=candidate,
                status="payload_mismatch",
                reason_code="poros_rejection_payload_mismatch",
                payload=supplied_mismatch,
            )
        prior_effect = _load_prior_promotion_effect(
            conn,
            message_id=str(inbox["message_id"]),
        )
        if prior_effect is not None:
            return _deny_rejection_gate(
                dsn=dsn,
                inbox=inbox,
                candidate=candidate,
                status="stale",
                reason_code="poros_rejection_stale",
                payload={"prior_effect": prior_effect},
            )
        rejection_decision = evaluate_rejection_decision_for_candidate(
            candidate,
            original_decision=original_decision,
            actor=actor,
            receiver=str(inbox["receiver_authority_id"]),
            bundle_path=Path(str(nomos_bundle)),
            catalog_version_id=current_catalog_version_id(dsn),
        )
        with conn.transaction():
            record_poros_rejection_decision(
                conn,
                deployment_name=deployment,
                receiver_authority_id=str(inbox["receiver_authority_id"]),
                sender_authority_id=str(inbox["sender_authority_id"]),
                message_id=str(inbox["message_id"]),
                body_sha256=str(inbox["body_sha256"]),
                decision=rejection_decision,
            )
            if rejection_decision.get("effect") != "allow":
                record_lifecycle_event(
                    conn,
                    receiver=str(inbox["receiver_authority_id"]),
                    envelope=candidate.envelope,
                    event_type="rejection_denied",
                    status=str(rejection_decision.get("effect")),
                    reason_code=str(rejection_decision.get("reason_code")),
                    payload={
                        "rejection_decision": nomos_poros_provenance(
                            rejection_decision
                        ),
                        "original_decision": _event_decision_summary(
                            original_decision
                        ),
                        "graph": redacted_graph_summary(candidate.body),
                    },
                )
                return PorosRejectionOutcome(
                    payload={
                        "status": "rejection_denied",
                        "message_id": inbox["message_id"],
                        "body_sha256": inbox["body_sha256"],
                        "rejection_decision": rejection_decision,
                        "original_decision": _event_decision_summary(
                            original_decision
                        ),
                        "admission": None,
                        "publish": None,
                        "live_counts": None,
                    },
                    exit_code=1,
                )
            record_lifecycle_event(
                conn,
                receiver=str(inbox["receiver_authority_id"]),
                envelope=candidate.envelope,
                event_type="rejected",
                status="rejected",
                reason_code="poros_rejected",
                payload={
                    "rejection_reason": rejection_reason or "",
                    "rejection_decision": nomos_poros_provenance(
                        rejection_decision
                    ),
                    "original_decision": _event_decision_summary(
                        original_decision
                    ),
                    "graph": redacted_graph_summary(candidate.body),
                },
            )

    return PorosRejectionOutcome(
        payload={
            "status": "rejected",
            "message_id": inbox["message_id"],
            "body_sha256": inbox["body_sha256"],
            "rejection_decision": rejection_decision,
            "original_decision": _event_decision_summary(original_decision),
            "admission": None,
            "publish": None,
            "live_counts": {"nodes_live": 0, "edges_live": 0},
        },
        exit_code=0,
    )


def submit_payload(
    result,
    *,
    receipt_verified: bool,
    admission: dict[str, Any] | None,
    publish: dict[str, Any] | None,
    live_counts: dict[str, int] | None,
    nomos_decision: dict[str, Any] | None = None,
    preflight: dict[str, Any] | None = None,
    delivery: dict[str, Any] | None = None,
    mutation_error: str | None = None,
) -> dict[str, Any]:
    candidate = result.admission_candidate
    payload: dict[str, Any] = {
        "status": result.status,
        "reason_code": result.reason_code,
        "receipt": result.receipt,
        "receipt_verified": receipt_verified,
        "message_id": candidate.message_id if candidate is not None else None,
        "sender": candidate.sender if candidate is not None else None,
        "recipient": candidate.recipient if candidate is not None else None,
        "body_sha256": candidate.body_sha256 if candidate is not None else None,
        "admission": admission,
        "publish": publish,
        "live_counts": live_counts,
        "nomos_decision": nomos_decision,
        "preflight": preflight,
        "delivery": delivery,
    }
    if mutation_error:
        payload["mutation_error"] = mutation_error
    return payload


def apply_preflight_status_override(
    payload: dict[str, Any],
    preflight: dict[str, Any] | None,
) -> None:
    if not preflight:
        return
    if preflight.get("status") != "duplicate":
        return
    payload["status"] = "duplicate"
    payload["reason_code"] = "duplicate"
    payload["message_id"] = preflight.get("message_id")
    payload["sender"] = preflight.get("sender_authority_id")
    payload["recipient"] = preflight.get("receiver_authority_id")
    payload["body_sha256"] = preflight.get("body_sha256")


def record_delivery_attempt_payload(
    sender_dsn: str,
    envelope: dict[str, Any],
    *,
    status: str,
    reason_code: str | None,
    receipt_sha256: str | None,
    transport: str = "local_cli",
    response: dict[str, Any] | None = None,
) -> dict[str, Any]:
    result = record_delivery_attempt(
        dsn=sender_dsn,
        message_id=str(envelope["message_id"]),
        body_sha256=str(envelope["body_sha256"]),
        envelope_sha256=sha256_b64url(canonical_json_bytes(envelope)),
        status=status,
        reason_code=reason_code,
        receipt_sha256=receipt_sha256,
        response=response
        or {
            "status": status,
            "reason_code": reason_code,
            "receipt_sha256": receipt_sha256,
        },
        transport=transport,
    )
    return result.to_dict()


def record_nomos_decision_event(
    conn,
    *,
    deployment: str = "default",
    receiver: str,
    envelope: dict[str, Any],
    nomos_decision: dict[str, Any],
) -> None:
    record_poros_nomos_decision(
        conn,
        deployment_name=deployment,
        receiver_authority_id=receiver,
        sender_authority_id=str(envelope["sender"]),
        message_id=str(envelope["message_id"]),
        body_sha256=(
            str(envelope["body_sha256"])
            if envelope.get("body_sha256") is not None
            else None
        ),
        decision=nomos_decision,
    )


def record_lifecycle_event(
    conn,
    *,
    receiver: str,
    envelope: dict[str, Any],
    event_type: str,
    status: str,
    reason_code: str | None,
    admission_id: str | None = None,
    receiver_generation_id: int | str | None = None,
    payload: dict[str, Any] | None = None,
) -> None:
    record_poros_incorporation_lifecycle(
        conn,
        receiver_authority_id=receiver,
        sender_authority_id=str(envelope["sender"]),
        message_id=str(envelope["message_id"]),
        body_sha256=(
            str(envelope["body_sha256"])
            if envelope.get("body_sha256") is not None
            else None
        ),
        event_type=event_type,
        status=status,
        reason_code=reason_code,
        admission_id=admission_id,
        receiver_generation_id=receiver_generation_id,
        payload=payload,
    )


def record_publish_lifecycle_event(
    dsn: str,
    *,
    receiver: str,
    envelope: dict[str, Any],
    admission: dict[str, Any],
    publish: dict[str, Any],
    live_counts: dict[str, int] | None,
) -> None:
    from okg.substrate.db.connection import connect as db_connect

    status = str(publish.get("status") or "unknown")
    # receiver_generation_id carries the chronos-native published generation
    # (a string checkpoint id) in the TEXT column.
    generation_id = publish.get("generation_id")
    with db_connect(dsn) as conn:
        with conn.transaction():
            record_lifecycle_event(
                conn,
                receiver=receiver,
                envelope=envelope,
                event_type=("published" if status == "published" else "blocked"),
                status=status,
                reason_code=(
                    "poros_published"
                    if status == "published"
                    else str(publish.get("blocked_reason") or status)
                ),
                admission_id=str(admission["admission_id"]),
                receiver_generation_id=generation_id,
                payload={
                    "admission_id": admission["admission_id"],
                    "publish": publish,
                    "live_counts": live_counts,
                },
            )


def admission_lifecycle_event_type(
    admission: dict[str, Any],
    *,
    admission_mode: str,
) -> str:
    if admission.get("status") == "accepted":
        return "admitted"
    if admission_mode == "review":
        return "review"
    if admission_mode == "quarantine":
        return "quarantined"
    return "blocked"


def _with_hydrated_detached_candidate(
    result,
    object_store: LocalPorosObjectStore,
):
    from dataclasses import replace

    candidate = result.admission_candidate
    if candidate is None:
        return result
    hydrated_body = load_detached_graph_body_from_store(
        candidate.envelope,
        object_store=object_store,
    )
    if hydrated_body is None:
        return result
    return replace(
        result,
        admission_candidate=replace(candidate, body=hydrated_body),
    )


def nomos_poros_provenance(
    nomos_decision: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if nomos_decision is None:
        return None
    return {
        "nomos_decision_id": nomos_decision.get("decision_id"),
        "policy_bundle_id": nomos_decision.get("policy_bundle_id"),
        "policy_fingerprint": nomos_decision.get("policy_fingerprint"),
        "nomos_input_hash": nomos_decision.get("input_hash"),
        "nomos_effect": nomos_decision.get("effect"),
        "nomos_reason_code": nomos_decision.get("reason_code"),
    }


def poros_promotion_provenance(
    *,
    promotion_decision: dict[str, Any],
    original_decision: dict[str, Any],
    actor: str,
) -> dict[str, Any]:
    provenance = dict(nomos_poros_provenance(promotion_decision) or {})
    provenance.update(
        {
            "promotion_decision_id": promotion_decision.get("decision_id"),
            "promotion_policy_bundle_id": promotion_decision.get(
                "policy_bundle_id"
            ),
            "promotion_policy_fingerprint": promotion_decision.get(
                "policy_fingerprint"
            ),
            "promotion_input_hash": promotion_decision.get("input_hash"),
            "promotion_actor": actor,
            "original_nomos_decision_id": original_decision.get(
                "nomos_decision_id"
            ),
            "original_nomos_effect": original_decision.get("status"),
            "original_nomos_reason_code": original_decision.get("reason_code"),
            "original_policy_bundle_id": original_decision.get(
                "policy_bundle_id"
            ),
            "original_policy_fingerprint": original_decision.get(
                "policy_fingerprint"
            ),
            "original_nomos_input_hash": original_decision.get(
                "nomos_input_hash"
            ),
        }
    )
    return provenance


def nomos_block_lifecycle_event_type(
    nomos_decision: dict[str, Any],
) -> str | None:
    effect = str(nomos_decision.get("effect") or "")
    if effect == "review":
        return "review"
    if effect == "quarantine":
        return "quarantined"
    return None


def redacted_graph_summary(body: Any) -> dict[str, Any]:
    body = body if isinstance(body, dict) else {}
    nodes = body.get("nodes") or body.get("node_facts") or []
    edges = body.get("edges") or body.get("edge_facts") or []
    objects = body.get("objects") if isinstance(body.get("objects"), list) else []
    return {
        "graph_id": body.get("graph_id"),
        "classification": body.get("classification"),
        "graph_profile": body.get("graph_profile"),
        "origin_generation_id": body.get("origin_generation_id"),
        "root_node_ids": body.get("root_node_ids") or [],
        "node_count": len(nodes) if isinstance(nodes, list) else 0,
        "edge_count": len(edges) if isinstance(edges, list) else 0,
        "objects": [_redacted_object_summary(obj) for obj in objects],
    }


def _redacted_object_summary(obj: Any) -> dict[str, Any]:
    if not isinstance(obj, dict):
        return {"invalid": True}
    return {
        "redacted": True,
        "classification": obj.get("classification"),
        "encrypted": bool("encryption" in obj or "ciphertext_sha256" in obj),
        "locations_redacted": "locations" in obj,
        "bytes_redacted": "inline_bytes_b64url" in obj,
    }


def _event_decision_summary(event: dict[str, Any]) -> dict[str, Any]:
    return {
        "decision_id": event.get("nomos_decision_id"),
        "effect": event.get("status"),
        "reason_code": event.get("reason_code"),
        "policy_bundle_id": event.get("policy_bundle_id"),
        "policy_fingerprint": event.get("policy_fingerprint"),
        "input_hash": event.get("nomos_input_hash"),
    }


def admission_result_payload(admission) -> dict[str, Any]:
    return {
        "deployment": admission.deployment,
        "admission_id": admission.admission_id,
        "status": admission.status,
        "inserted": admission.inserted,
        "idempotency_key": admission.idempotency_key,
        "source": admission.source,
        "run_id": admission.run_id,
        "payload_ref_inserted": admission.payload_ref_inserted,
        "emitted": admission.emitted,
        "fact_ref_count": admission.fact_ref_count,
        "payload_rejected": admission.payload_rejected,
    }


def live_counts_for_candidate(
    dsn: str,
    *,
    generation_id: int | str,
    candidate: PorosAdmissionCandidate,
) -> dict[str, int]:
    from okg.substrate.db.connection import connect as db_connect

    node_ids = [
        str(node["node_id"])
        for node in (
            candidate.body.get("nodes") or candidate.body.get("node_facts") or []
        )
        if isinstance(node, dict) and node.get("node_id")
    ]
    edge_triples = [
        (str(edge["src"]), str(edge["edge_type"]), str(edge["dst"]))
        for edge in (
            candidate.body.get("edges") or candidate.body.get("edge_facts") or []
        )
        if (
            isinstance(edge, dict)
            and edge.get("src")
            and edge.get("edge_type")
            and edge.get("dst")
        )
    ]
    from okg.substrate.graph_store import ChronosGraphStore

    with db_connect(dsn, autocommit=True) as conn:
        # Count how many of the candidate's nodes/edges are visible in
        # the published generation's graph, read through a Chronos
        # checkout pinned to that generation.
        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).checkout_generation(generation_id)
        if node_ids:
            node_count = int(
                graph.query(
                    """
                    SELECT count(*) AS n
                      FROM okg.graph_nodes
                     WHERE node_id = ANY(:node_ids)
                    """,
                    {"node_ids": node_ids},
                )[0]["n"]
                or 0
            )
        else:
            node_count = 0
        edge_count = 0
        for src, edge_type, dst in edge_triples:
            edge_count += int(
                graph.query(
                    """
                    SELECT count(*) AS n
                      FROM okg.graph_edges
                     WHERE src = :src
                       AND edge_type = :edge_type
                       AND dst = :dst
                    """,
                    {"src": src, "edge_type": edge_type, "dst": dst},
                )[0]["n"]
                or 0
            )
    return {"nodes_live": node_count, "edges_live": edge_count}


def _load_promotion_inbox_row(
    conn,
    *,
    message_id: str | None,
    graph_id: str | None,
    body_sha256: str | None,
) -> dict[str, Any]:
    clauses: list[str] = []
    params: list[str] = []
    if message_id is not None:
        clauses.append("message_id = %s")
        params.append(message_id)
    if graph_id is not None:
        clauses.append("envelope->'body'->>'graph_id' = %s")
        params.append(graph_id)
    if body_sha256 is not None:
        clauses.append("body_sha256 = %s")
        params.append(body_sha256)
    if not clauses:
        raise ValueError("one promotion lookup key is required")
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT receiver_authority_id, sender_authority_id, message_id,
                   status, reason_code, body_sha256, signing_key_id,
                   envelope, received_at
              FROM okg.poros_inbox_messages
             WHERE {' AND '.join(clauses)}
             ORDER BY received_at DESC
             LIMIT 1
            """,
            params,
        )
        row = cur.fetchone()
    if row is None:
        raise LookupError("no Poros inbox evidence matched the promotion lookup")
    return {
        "receiver_authority_id": row[0],
        "sender_authority_id": row[1],
        "message_id": row[2],
        "status": row[3],
        "reason_code": row[4],
        "body_sha256": row[5],
        "signing_key_id": row[6],
        "envelope": row[7],
        "received_at": row[8].isoformat() if row[8] else None,
    }


def _load_original_blocking_decision(conn, *, message_id: str) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, reason_code, nomos_decision_id,
                   policy_bundle_id, policy_fingerprint,
                   nomos_input_hash, payload
              FROM okg.poros_incorporation_events
             WHERE message_id = %s
               AND event_type = 'nomos_decided'
             ORDER BY event_id DESC
             LIMIT 1
            """,
            (message_id,),
        )
        row = cur.fetchone()
    if row is None:
        raise LookupError("promotion requires an original Nomos decision")
    return {
        "message_id": message_id,
        "status": row[0],
        "reason_code": row[1],
        "nomos_decision_id": row[2],
        "policy_bundle_id": row[3],
        "policy_fingerprint": row[4],
        "nomos_input_hash": row[5],
        "payload": row[6] if isinstance(row[6], dict) else {},
    }


def _load_prior_promotion_effect(
    conn,
    *,
    message_id: str,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT event_type, status, reason_code, admission_id::text,
                   receiver_generation_id
              FROM okg.poros_incorporation_events
             WHERE message_id = %s
               AND event_type IN ('admitted', 'published', 'promoted', 'rejected')
             ORDER BY event_id DESC
             LIMIT 1
            """,
            (message_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return {
        "event_type": row[0],
        "status": row[1],
        "reason_code": row[2],
        "admission_id": row[3],
        "receiver_generation_id": row[4],
    }


def _candidate_from_inbox(inbox: dict[str, Any]) -> PorosAdmissionCandidate:
    envelope = inbox["envelope"] if isinstance(inbox.get("envelope"), dict) else {}
    body = envelope.get("body") if isinstance(envelope.get("body"), dict) else {}
    if not isinstance(body, dict):
        raise ValueError("stored Poros envelope has no graph body")
    if envelope.get("body_sha256") != inbox.get("body_sha256"):
        raise ValueError("stored Poros envelope body hash changed after preflight")
    return PorosAdmissionCandidate(
        message_id=str(inbox["message_id"]),
        sender=str(inbox["sender_authority_id"]),
        recipient=str(inbox["receiver_authority_id"]),
        body_sha256=str(inbox["body_sha256"]),
        body=body,
        envelope=envelope,
        received_at=str(inbox.get("received_at") or ""),
    )


def _promotion_envelope_mismatch(
    promotion_envelope: dict[str, Any] | None,
    *,
    inbox: dict[str, Any],
) -> dict[str, Any] | None:
    if promotion_envelope is None:
        return None
    body = (
        promotion_envelope.get("body")
        if isinstance(promotion_envelope.get("body"), dict)
        else {}
    )
    supplied_body_sha256 = (
        sha256_b64url(canonical_json_bytes(body))
        if body
        else None
    )
    mismatches: dict[str, Any] = {}
    if promotion_envelope.get("message_id") != inbox.get("message_id"):
        mismatches["message_id"] = {
            "stored": inbox.get("message_id"),
            "supplied": promotion_envelope.get("message_id"),
        }
    if promotion_envelope.get("body_sha256") != inbox.get("body_sha256"):
        mismatches["declared_body_sha256"] = {
            "stored": inbox.get("body_sha256"),
            "supplied": promotion_envelope.get("body_sha256"),
        }
    if supplied_body_sha256 != inbox.get("body_sha256"):
        mismatches["computed_body_sha256"] = {
            "stored": inbox.get("body_sha256"),
            "supplied": supplied_body_sha256,
        }
    if not mismatches:
        return None
    return {"mismatches": mismatches}


def _deny_promotion_gate(
    *,
    dsn: str,
    inbox: dict[str, Any],
    candidate: PorosAdmissionCandidate,
    status: str,
    reason_code: str,
    payload: dict[str, Any],
) -> PorosPromotionOutcome:
    from okg.substrate.db.connection import connect as db_connect

    with db_connect(dsn) as conn:
        with conn.transaction():
            record_lifecycle_event(
                conn,
                receiver=str(inbox["receiver_authority_id"]),
                envelope=candidate.envelope,
                event_type="promotion_denied",
                status=status,
                reason_code=reason_code,
                payload={
                    **payload,
                    "graph": redacted_graph_summary(candidate.body),
                },
            )
    return PorosPromotionOutcome(
        payload={
            "status": "promotion_denied",
            "message_id": inbox["message_id"],
            "body_sha256": inbox["body_sha256"],
            "reason_code": reason_code,
            "promotion_decision": None,
            "admission": None,
            "publish": None,
            "live_counts": None,
        },
        exit_code=1,
    )


def _deny_rejection_gate(
    *,
    dsn: str,
    inbox: dict[str, Any],
    candidate: PorosAdmissionCandidate,
    status: str,
    reason_code: str,
    payload: dict[str, Any],
) -> PorosRejectionOutcome:
    from okg.substrate.db.connection import connect as db_connect

    with db_connect(dsn) as conn:
        with conn.transaction():
            record_lifecycle_event(
                conn,
                receiver=str(inbox["receiver_authority_id"]),
                envelope=candidate.envelope,
                event_type="rejection_denied",
                status=status,
                reason_code=reason_code,
                payload={
                    **payload,
                    "graph": redacted_graph_summary(candidate.body),
                },
            )
    return PorosRejectionOutcome(
        payload={
            "status": "rejection_denied",
            "message_id": inbox["message_id"],
            "body_sha256": inbox["body_sha256"],
            "reason_code": reason_code,
            "rejection_decision": None,
            "admission": None,
            "publish": None,
            "live_counts": None,
        },
        exit_code=1,
    )


def _should_evaluate_nomos(
    result,
    *,
    preflight: dict[str, Any] | None,
    nomos_bundle: Path | str | None,
    nomos_identity_claim: dict[str, Any] | None = None,
) -> bool:
    if nomos_bundle is None and nomos_identity_claim is None:
        return False
    if result.status != "accepted_for_admission":
        return False
    if result.admission_candidate is None:
        return False
    if preflight is not None and preflight.get("status") != "accepted_for_admission":
        return False
    return True


def evaluate_nomos_decision_for_result(
    result,
    envelope: dict[str, Any],
    *,
    nomos_bundle: Path | None,
    nomos_identity_claim: dict[str, Any] | None = None,
    dsn: str | None,
    deployment: str = "default",
    admission_mode: str = "fast_track",
) -> dict[str, Any]:
    if result.admission_candidate is None:
        raise ValueError("Nomos evaluation requires an admission candidate")
    if nomos_identity_claim is not None:
        if dsn is None:
            raise ValueError("Nomos identity claim evaluation requires dsn")
        return nomos_identity_decision_for_candidate(
            result.admission_candidate,
            claim=nomos_identity_claim,
            bundle_path=nomos_bundle,
            dsn=dsn,
            deployment=deployment,
            signing_key_id=str(envelope.get("signing_key_id") or ""),
            catalog_version_id=(current_catalog_version_id(dsn) if dsn else None),
            requested_incorporation_mode=admission_mode,
        )
    if nomos_bundle is None:
        raise ValueError("Nomos bundle is required without an identity claim")
    decision = nomos_decision_for_candidate(
        result.admission_candidate,
        bundle_path=nomos_bundle,
        signing_key_id=str(envelope.get("signing_key_id") or ""),
        catalog_version_id=(current_catalog_version_id(dsn) if dsn else None),
        requested_incorporation_mode=admission_mode,
    )
    return decision.to_dict()


def nomos_identity_decision_for_candidate(
    candidate: PorosAdmissionCandidate,
    *,
    claim: dict[str, Any],
    bundle_path: Path | None,
    dsn: str,
    deployment: str,
    signing_key_id: str,
    catalog_version_id: int | None,
    requested_incorporation_mode: str = "fast_track",
) -> dict[str, Any]:
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.nomos import (
        evaluate_identity_claim_from_postgres,
        load_bundle,
    )

    body = candidate.body
    graph_shape = classify_graph_body(body)
    scope = str(claim.get("nomos_scope") or f"deployment:{deployment}")
    resource = {
        "deployment": deployment,
        "scope": scope,
        "graph_id": body.get("graph_id"),
        **graph_shape.to_resource_fields(),
        "sender_authority_id": candidate.sender,
        "graph_classification": str(body.get("classification") or "internal"),
        "requested_incorporation_mode": requested_incorporation_mode,
        "sender_graph_profile": body.get("graph_profile"),
        "recipient": candidate.recipient,
        "signing_key_id": signing_key_id,
    }
    with db_connect(dsn) as conn:
        result = evaluate_identity_claim_from_postgres(
            conn,
            deployment_name=deployment,
            bundle=load_bundle(bundle_path) if bundle_path is not None else None,
            claim=claim,
            action="poros.graph.submit",
            resource=resource,
            scope=scope,
            classification=str(body.get("classification") or "internal"),
            enforcement_point="poros_incorporation.identity",
            target={
                "kind": "poros_submit",
                "message_id": candidate.message_id,
                "graph_id": body.get("graph_id"),
                "catalog_version_id": catalog_version_id,
            },
        )
        conn.commit()
    return result.decision.to_dict()


def evaluate_promotion_decision_for_candidate(
    candidate: PorosAdmissionCandidate,
    *,
    original_decision: dict[str, Any],
    actor: str,
    receiver: str,
    bundle_path: Path,
    catalog_version_id: int | None,
) -> dict[str, Any]:
    from okg.substrate.nomos import DecisionInput, evaluate, load_bundle

    body = candidate.body
    graph_shape = classify_graph_body(body)
    decision_input = DecisionInput(
        subject=actor,
        action="poros.graph.promote",
        classification=str(body.get("classification") or "internal"),
        resource={
            "message_id": candidate.message_id,
            "graph_id": body.get("graph_id"),
            **graph_shape.to_resource_fields(),
            "graph_classification": str(body.get("classification") or "internal"),
            "requested_incorporation_mode": "fast_track",
            "sender_graph_profile": body.get("graph_profile"),
            "sender": candidate.sender,
            "recipient": receiver,
            "signing_key_id": candidate.envelope.get("signing_key_id"),
            "original_effect": original_decision.get("status"),
            "original_decision_id": original_decision.get(
                "nomos_decision_id"
            ),
        },
        context={
            "body_sha256": candidate.body_sha256,
            "catalog_version_id": catalog_version_id,
        },
    )
    return evaluate(
        load_bundle(bundle_path),
        decision_input,
        enforcement_point="poros_promotion",
    ).to_dict()


def evaluate_rejection_decision_for_candidate(
    candidate: PorosAdmissionCandidate,
    *,
    original_decision: dict[str, Any],
    actor: str,
    receiver: str,
    bundle_path: Path,
    catalog_version_id: int | None,
) -> dict[str, Any]:
    from okg.substrate.nomos import DecisionInput, evaluate, load_bundle

    body = candidate.body
    graph_shape = classify_graph_body(body)
    decision_input = DecisionInput(
        subject=actor,
        action="poros.graph.reject",
        classification=str(body.get("classification") or "internal"),
        resource={
            "message_id": candidate.message_id,
            "graph_id": body.get("graph_id"),
            **graph_shape.to_resource_fields(),
            "graph_classification": str(body.get("classification") or "internal"),
            "requested_incorporation_mode": "fast_track",
            "sender_graph_profile": body.get("graph_profile"),
            "sender": candidate.sender,
            "recipient": receiver,
            "signing_key_id": candidate.envelope.get("signing_key_id"),
            "original_effect": original_decision.get("status"),
            "original_decision_id": original_decision.get(
                "nomos_decision_id"
            ),
        },
        context={
            "body_sha256": candidate.body_sha256,
            "catalog_version_id": catalog_version_id,
        },
    )
    return evaluate(
        load_bundle(bundle_path),
        decision_input,
        enforcement_point="poros_rejection",
    ).to_dict()


def nomos_decision_for_candidate(
    candidate: PorosAdmissionCandidate,
    *,
    bundle_path: Path,
    signing_key_id: str,
    catalog_version_id: int | None,
    requested_incorporation_mode: str = "fast_track",
):
    from okg.substrate.nomos import DecisionInput, evaluate, load_bundle

    body = candidate.body
    graph_shape = classify_graph_body(body)
    sender_profile = body.get("graph_profile")
    decision_input = DecisionInput(
        subject=candidate.sender,
        action="poros.graph.submit",
        classification=str(body.get("classification") or "internal"),
        resource={
            "graph_id": body.get("graph_id"),
            **graph_shape.to_resource_fields(),
            "sender_authority_id": candidate.sender,
            "graph_classification": str(body.get("classification") or "internal"),
            "requested_incorporation_mode": requested_incorporation_mode,
            "sender_graph_profile": sender_profile,
            "recipient": candidate.recipient,
            "signing_key_id": signing_key_id,
        },
        context={
            "message_id": candidate.message_id,
            "body_sha256": candidate.body_sha256,
            "catalog_version_id": catalog_version_id,
        },
    )
    return evaluate(
        load_bundle(bundle_path),
        decision_input,
        enforcement_point="poros_incorporation",
    )


def classification_compatibility_gate(
    candidate: PorosAdmissionCandidate | None,
    *,
    admission_mode: str,
    nomos_decision: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Default-deny non-public fast-track Poros publication without policy."""
    if candidate is None or admission_mode != "fast_track":
        return None
    classification = str(candidate.body.get("classification") or "internal")
    if classification in {"public", "unspecified"}:
        return None
    if nomos_decision is not None and nomos_decision.get("effect") in {
        "allow",
        "declassify",
    }:
        return None
    return {
        "reason_code": "poros.classification_requires_receiver_policy",
        "classification": classification,
        "required_effects": ["allow", "declassify"],
        "requested_incorporation_mode": admission_mode,
        "message": (
            "Poros fast-track admission for non-public classification "
            f"{classification!r} requires an explicit receiver Nomos "
            "allow or declassify decision"
        ),
    }


def current_catalog_version_id(dsn: str) -> int | None:
    from okg.substrate.db.connection import connect as db_connect

    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT max(catalog_version_id)
              FROM okg.catalog_versions
             WHERE status = 'applied'
            """
        )
        row = cur.fetchone()
    if row is None or row[0] is None:
        return None
    return int(row[0])


def _http_status_for_payload(payload: dict[str, Any]) -> int:
    status = payload.get("status")
    if status in {"accepted_for_admission", "duplicate"}:
        return 202
    if status == "rejected_preflight":
        return 400
    if status == "error":
        return 400
    return 202
