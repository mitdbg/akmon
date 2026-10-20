"""Poros runtime trace/refinement helpers for the formal model boundary."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Any, Iterable, Mapping


class PorosRefinementError(ValueError):
    """Raised when a Poros runtime trace violates the model boundary."""


RECEIPT_STATUSES = frozenset({
    "accepted_for_admission",
    "rejected_preflight",
    "quarantined",
    "duplicate",
})
DECISION_EFFECTS = frozenset({
    "allow",
    "deny",
    "review",
    "quarantine",
    "redact",
    "declassify",
})
ADMISSION_STATUSES = frozenset({
    "pending",
    "accepted",
    "rejected",
    "published",
    "blocked",
    "failed",
    "quarantined",
})
GENERATION_STATUSES = frozenset({"published", "blocked", "failed", "pending"})
LIVE_ROW_KINDS = frozenset({"node", "edge"})
PROOF_VISIBILITIES = frozenset({"public", "audit"})
PROTECTED_CLASSIFICATIONS = frozenset({"restricted", "private"})

TOP_LEVEL_KEYS = frozenset({
    "outbox",
    "delivery_attempts",
    "inbox",
    "receipts",
    "nomos_decisions",
    "promotion_decisions",
    "admissions",
    "generations",
    "live_rows",
    "proofs",
    "unsupported",
})
OUTBOX_KEYS = frozenset({
    "message_id",
    "sender",
    "recipient",
    "sender_generation_id",
    "body_sha256",
    "graph_id",
    "signing_key_id",
    "detached",
    "object_hash_valid",
    "sender_hint",
    "receiver_authorized",
    "classification",
})
DELIVERY_ATTEMPT_KEYS = frozenset({
    "message_id",
    "status",
    "reason_code",
    "receipt_sha256",
    "transport",
    "body_sha256",
    "envelope_sha256",
})
INBOX_KEYS = frozenset({
    "message_id",
    "status",
    "body_sha256",
    "trusted",
    "signature_valid",
    "recipient_valid",
    "body_hash_valid",
    "fresh",
    "detached_object_valid",
    "reason_code",
})
RECEIPT_KEYS = frozenset({
    "message_id",
    "status",
    "received_body_sha256",
    "body_sha256",
    "reason_code",
    "admission_id",
})
DECISION_KEYS = frozenset({
    "message_id",
    "decision_id",
    "effect",
    "input_hash",
    "policy_bundle_id",
    "policy_fingerprint",
    "uses_receiver_policy",
    "classification",
    "obligations",
    "reason_code",
})
ADMISSION_KEYS = frozenset({
    "message_id",
    "admission_id",
    "status",
    "body_sha256",
    "source",
    "run_id",
})
GENERATION_KEYS = frozenset({
    "message_id",
    "generation_id",
    "status",
    "admission_id",
})
LIVE_ROW_KEYS = frozenset({
    "row_id",
    "row_kind",
    "message_id",
    "admission_id",
    "sender_generation_id",
    "receiver_generation_id",
    "body_sha256",
})
PROOF_KEYS = frozenset({
    "message_id",
    "visibility",
    "classification",
    "redacted",
    "declassified",
})


@dataclass(frozen=True)
class _MessageEvidence:
    message_id: str
    body_sha256: str | None
    sender_generation_id: str | None
    classification: str | None
    detached: bool
    object_hash_valid: bool
    sender_hint: bool
    receiver_authorized: bool


def build_refinement_state_from_trace(trace: Mapping[str, Any]) -> dict[str, Any]:
    """Map a strict Poros runtime trace to the TLA+ model vocabulary."""
    _assert_known_keys("trace", trace, TOP_LEVEL_KEYS)
    unsupported = _string_list(trace.get("unsupported", []), "unsupported")
    outbox_rows = _rows(trace.get("outbox", []), "outbox", OUTBOX_KEYS)
    delivery_rows = _rows(
        trace.get("delivery_attempts", []),
        "delivery_attempts",
        DELIVERY_ATTEMPT_KEYS,
    )
    inbox_rows = _rows(trace.get("inbox", []), "inbox", INBOX_KEYS)
    receipt_rows = _rows(trace.get("receipts", []), "receipts", RECEIPT_KEYS)
    decision_rows = _rows(
        trace.get("nomos_decisions", []),
        "nomos_decisions",
        DECISION_KEYS,
    )
    promotion_decision_rows = _rows(
        trace.get("promotion_decisions", []),
        "promotion_decisions",
        DECISION_KEYS,
    )
    admission_rows = _rows(trace.get("admissions", []), "admissions", ADMISSION_KEYS)
    generation_rows = _rows(
        trace.get("generations", []),
        "generations",
        GENERATION_KEYS,
    )
    live_rows = _rows(trace.get("live_rows", []), "live_rows", LIVE_ROW_KEYS)
    proof_rows = _rows(trace.get("proofs", []), "proofs", PROOF_KEYS)

    outbox_evidence = {
        _required_str(row, "message_id", "outbox"): _outbox_evidence(row)
        for row in outbox_rows
    }
    messages = set(outbox_evidence)
    for label, rows in [
        ("delivery_attempts", delivery_rows),
        ("inbox", inbox_rows),
        ("receipts", receipt_rows),
        ("nomos_decisions", decision_rows),
        ("promotion_decisions", promotion_decision_rows),
        ("admissions", admission_rows),
        ("generations", generation_rows),
        ("live_rows", live_rows),
        ("proofs", proof_rows),
    ]:
        for row in rows:
            messages.add(_required_str(row, "message_id", label))

    inbox_by_message = _single_by_message(inbox_rows, "inbox")
    delivery_by_message = _group_by_message(delivery_rows, "delivery_attempts")
    receipt_by_message = _last_by_message(receipt_rows, "receipts")
    decision_by_message = _single_by_message(decision_rows, "nomos_decisions")
    promotion_decision_by_message = _last_by_message(
        promotion_decision_rows,
        "promotion_decisions",
    )
    admissions_by_message = _group_by_message(admission_rows, "admissions")
    generations_by_message = _group_by_message(generation_rows, "generations")
    proof_by_message = _group_by_message(proof_rows, "proofs")

    verified = {
        message_id
        for message_id, row in inbox_by_message.items()
        if row.get("status") == "accepted_for_admission"
    }
    blocked_state = set()
    for message_id, row in inbox_by_message.items():
        if row.get("status") in {"rejected_preflight", "quarantined"}:
            blocked_state.add(message_id)
    for message_id, row in decision_by_message.items():
        if row.get("effect") == "deny":
            blocked_state.add(message_id)
        elif row.get("effect") in {"review", "quarantine"} and not _promotion_allows(
            message_id,
            row,
            promotion_decision_by_message,
        ):
            blocked_state.add(message_id)
    for message_id, rows in admissions_by_message.items():
        if any(row.get("status") in {"rejected", "quarantined", "blocked"} for row in rows):
            blocked_state.add(message_id)

    admissions = {
        message_id
        for message_id, rows in admissions_by_message.items()
        if any(row.get("status") in {"accepted", "published"} for row in rows)
    }
    published_messages = {
        message_id
        for message_id, rows in generations_by_message.items()
        if any(row.get("status") == "published" for row in rows)
    }

    row_message = {}
    row_sender_generation = {}
    row_receiver_generation = {}
    row_admission = {}
    row_body_hash = {}
    for row in live_rows:
        row_id = _required_str(row, "row_id", "live_rows")
        row_message[row_id] = _required_str(row, "message_id", "live_rows")
        row_sender_generation[row_id] = _required_str(
            row,
            "sender_generation_id",
            "live_rows",
        )
        row_receiver_generation[row_id] = _required_str(
            row,
            "receiver_generation_id",
            "live_rows",
        )
        row_admission[row_id] = _required_str(row, "admission_id", "live_rows")
        row_body_hash[row_id] = _required_str(row, "body_sha256", "live_rows")

    message_receiver_generation = {}
    for message_id, rows in generations_by_message.items():
        published = [row for row in rows if row.get("status") == "published"]
        if published:
            message_receiver_generation[message_id] = _required_str(
                published[-1],
                "generation_id",
                "generations",
            )
    for row_id, message_id in row_message.items():
        message_receiver_generation.setdefault(
            message_id,
            row_receiver_generation[row_id],
        )

    public_proof_messages = {
        message_id
        for message_id, rows in proof_by_message.items()
        if any(row.get("visibility") == "public" for row in rows)
    }

    state = {
        "model": "formal/poros/PorosExchange.tla",
        "messages": sorted(messages),
        "outbox": sorted(outbox_evidence),
        "inbox": sorted(inbox_by_message),
        "verified": sorted(verified),
        "replayCache": sorted(inbox_by_message),
        "receipts": dict(sorted(receipt_by_message.items())),
        "deliveryAttempts": dict(sorted(delivery_by_message.items())),
        "nomosDecisions": dict(sorted(decision_by_message.items())),
        "promotionDecisions": dict(sorted(promotion_decision_by_message.items())),
        "admissions": sorted(admissions),
        "blockedState": sorted(blocked_state),
        "publishedMessages": sorted(published_messages),
        "liveRows": sorted(row_message),
        "rowMessage": dict(sorted(row_message.items())),
        "rowSenderGeneration": dict(sorted(row_sender_generation.items())),
        "rowReceiverGeneration": dict(sorted(row_receiver_generation.items())),
        "rowAdmission": dict(sorted(row_admission.items())),
        "rowBodyHash": dict(sorted(row_body_hash.items())),
        "messageReceiverGeneration": dict(sorted(message_receiver_generation.items())),
        "publicProofMessages": sorted(public_proof_messages),
        "outboxEvidence": {
            key: evidence.__dict__ for key, evidence in sorted(outbox_evidence.items())
        },
        "inboxEvidence": dict(sorted(inbox_by_message.items())),
        "admissionRows": sorted(
            admission_rows,
            key=lambda row: (
                str(row.get("message_id") or ""),
                str(row.get("admission_id") or ""),
            ),
        ),
        "generationRows": sorted(
            generation_rows,
            key=lambda row: (
                str(row.get("message_id") or ""),
                str(row.get("generation_id") or ""),
            ),
        ),
        "promotionDecisionRows": sorted(
            promotion_decision_rows,
            key=lambda row: (
                str(row.get("message_id") or ""),
                str(row.get("decision_id") or ""),
            ),
        ),
        "liveRowEvidence": sorted(
            live_rows,
            key=lambda row: str(row.get("row_id") or ""),
        ),
        "proofRows": sorted(
            proof_rows,
            key=lambda row: (
                str(row.get("message_id") or ""),
                str(row.get("visibility") or ""),
            ),
        ),
        "unsupported": unsupported,
    }
    check_refinement_state(state)
    return state


def check_refinement_state(state: Mapping[str, Any]) -> None:
    """Validate a Poros refinement state against checked model obligations."""
    unsupported = list(state.get("unsupported") or [])
    if unsupported:
        raise PorosRefinementError(
            f"Poros refinement trace contains unsupported effects: {unsupported}"
        )

    messages = set(_string_list(state.get("messages", []), "messages"))
    outbox = set(_string_list(state.get("outbox", []), "outbox"))
    inbox = set(_string_list(state.get("inbox", []), "inbox"))
    verified = set(_string_list(state.get("verified", []), "verified"))
    admissions = set(_string_list(state.get("admissions", []), "admissions"))
    blocked_state = set(_string_list(state.get("blockedState", []), "blockedState"))
    published = set(_string_list(state.get("publishedMessages", []), "publishedMessages"))
    live_rows = set(_string_list(state.get("liveRows", []), "liveRows"))
    public_proofs = set(_string_list(state.get("publicProofMessages", []), "publicProofMessages"))

    receipts = _mapping(state.get("receipts", {}), "receipts")
    decisions = _mapping(state.get("nomosDecisions", {}), "nomosDecisions")
    promotion_decisions = _mapping(
        state.get("promotionDecisions", {}),
        "promotionDecisions",
    )
    outbox_evidence = _mapping(state.get("outboxEvidence", {}), "outboxEvidence")
    inbox_evidence = _mapping(state.get("inboxEvidence", {}), "inboxEvidence")
    row_message = _mapping(state.get("rowMessage", {}), "rowMessage")
    row_sender_generation = _mapping(
        state.get("rowSenderGeneration", {}),
        "rowSenderGeneration",
    )
    row_receiver_generation = _mapping(
        state.get("rowReceiverGeneration", {}),
        "rowReceiverGeneration",
    )
    row_admission = _mapping(state.get("rowAdmission", {}), "rowAdmission")
    row_body_hash = _mapping(state.get("rowBodyHash", {}), "rowBodyHash")
    message_receiver_generation = _mapping(
        state.get("messageReceiverGeneration", {}),
        "messageReceiverGeneration",
    )

    admission_counts = Counter(
        _required_str(row, "message_id", "admissionRows")
        for row in _rows(
            state.get("admissionRows", []),
            "admissionRows",
            ADMISSION_KEYS,
        )
        if row.get("status") in {"accepted", "published"}
    )
    publication_counts = Counter(
        _required_str(row, "message_id", "generationRows")
        for row in _rows(
            state.get("generationRows", []),
            "generationRows",
            GENERATION_KEYS,
        )
        if row.get("status") == "published"
    )
    for message_id, count in admission_counts.items():
        if count > 1:
            raise PorosRefinementError(
                f"message {message_id!r} has {count} admission effects"
            )
    for message_id, count in publication_counts.items():
        if count > 1:
            raise PorosRefinementError(
                f"message {message_id!r} has {count} publication effects"
            )

    for message_id in admissions:
        if message_id not in outbox:
            raise PorosRefinementError(
                f"admission for {message_id!r} lacks sender outbox evidence"
            )
        if message_id not in verified:
            raise PorosRefinementError(
                f"admission for {message_id!r} lacks verified preflight state"
            )
        outbox_row = _message_row(outbox_evidence, message_id, "outboxEvidence")
        inbox_row = _message_row(inbox_evidence, message_id, "inboxEvidence")
        if inbox_row.get("body_sha256") != outbox_row.get("body_sha256"):
            raise PorosRefinementError(
                f"admission for {message_id!r} has inbox/outbox body hash mismatch"
            )
        missing_checks = [
            field
            for field in [
                "trusted",
                "signature_valid",
                "recipient_valid",
                "body_hash_valid",
                "fresh",
                "detached_object_valid",
            ]
            if inbox_row.get(field) is not True
        ]
        if missing_checks:
            raise PorosRefinementError(
                f"admission for {message_id!r} lacks preflight checks: "
                f"{missing_checks}"
            )
        receipt = _message_row(receipts, message_id, "receipts")
        if receipt.get("status") != "accepted_for_admission":
            raise PorosRefinementError(
                f"admission for {message_id!r} lacks accepted receipt"
            )
        if receipt.get("bodyHashBound") is not True:
            raise PorosRefinementError(
                f"admission for {message_id!r} lacks body-hash-bound receipt"
            )
        receipt_hash = receipt.get("received_body_sha256") or receipt.get(
            "body_sha256"
        )
        if receipt_hash != outbox_row.get("body_sha256"):
            raise PorosRefinementError(
                f"admission for {message_id!r} has receipt/outbox body hash mismatch"
            )
        decision = _message_row(decisions, message_id, "nomosDecisions")
        original_allow = (
            decision.get("effect") == "allow"
            and decision.get("usesReceiverPolicy") is True
            and decision.get("inputHashBound") is True
        )
        promotion_allow = _promotion_allows(
            message_id,
            decision,
            promotion_decisions,
        )
        if not original_allow and not promotion_allow:
            if decision.get("effect") in {"review", "quarantine"}:
                raise PorosRefinementError(
                    f"admission for {message_id!r} lacks receiver-owned "
                    "allow or promotion decision"
                )
            raise PorosRefinementError(
                f"admission for {message_id!r} has unsafe Nomos effect "
                f"{decision.get('effect')!r}"
            )
        if original_allow and decision.get("usesReceiverPolicy") is not True:
            raise PorosRefinementError(
                f"admission for {message_id!r} lacks receiver-owned policy"
            )

    delivery_attempts = _mapping(state.get("deliveryAttempts", {}), "deliveryAttempts")
    for message_id, attempts in delivery_attempts.items():
        if message_id not in outbox:
            raise PorosRefinementError(
                f"delivery attempts for {message_id!r} lack sender outbox evidence"
            )
        outbox_row = _message_row(outbox_evidence, message_id, "outboxEvidence")
        if not isinstance(attempts, list):
            raise PorosRefinementError(
                f"deliveryAttempts for {message_id!r} must be a list"
            )
        for attempt in attempts:
            if not isinstance(attempt, Mapping):
                raise PorosRefinementError(
                    f"deliveryAttempts for {message_id!r} must contain objects"
                )
            if attempt.get("body_sha256") != outbox_row.get("body_sha256"):
                raise PorosRefinementError(
                    f"delivery attempt for {message_id!r} has outbox body hash mismatch"
                )
            if not isinstance(attempt.get("envelope_sha256"), str) or not attempt.get(
                "envelope_sha256"
            ):
                raise PorosRefinementError(
                    f"delivery attempt for {message_id!r} lacks envelope hash evidence"
                )

    if blocked_state.intersection(admissions):
        raise PorosRefinementError(
            "blocked Poros messages cannot also be admitted: "
            f"{sorted(blocked_state.intersection(admissions))}"
        )
    if blocked_state.intersection(published):
        raise PorosRefinementError(
            "blocked Poros messages cannot be published: "
            f"{sorted(blocked_state.intersection(published))}"
        )

    for message_id in published:
        if message_id not in admissions:
            raise PorosRefinementError(
                f"published message {message_id!r} lacks admission evidence"
            )
        if message_id not in receipts:
            raise PorosRefinementError(
                f"published message {message_id!r} lacks receipt evidence"
            )

    for row_id in live_rows:
        message_id = str(row_message.get(row_id) or "")
        if not message_id:
            raise PorosRefinementError(f"live row {row_id!r} lacks message id")
        if message_id not in messages:
            raise PorosRefinementError(
                f"live row {row_id!r} references unknown message {message_id!r}"
            )
        if message_id in blocked_state:
            raise PorosRefinementError(
                f"live row {row_id!r} derives from blocked message {message_id!r}"
            )
        if message_id not in published:
            raise PorosRefinementError(
                f"live row {row_id!r} derives from unpublished message "
                f"{message_id!r}"
            )
        if str(row_sender_generation.get(row_id) or "") == "":
            raise PorosRefinementError(f"live row {row_id!r} lacks sender generation")
        receiver_generation = str(row_receiver_generation.get(row_id) or "")
        if receiver_generation == "":
            raise PorosRefinementError(f"live row {row_id!r} lacks receiver generation")
        if message_receiver_generation.get(message_id) != receiver_generation:
            raise PorosRefinementError(
                f"live row {row_id!r} receiver generation does not match "
                f"message publication"
            )
        admission_id = str(row_admission.get(row_id) or "")
        if admission_id == "":
            raise PorosRefinementError(f"live row {row_id!r} lacks admission id")
        body_hash = str(row_body_hash.get(row_id) or "")
        outbox_row = _message_row(outbox_evidence, message_id, "outboxEvidence")
        if body_hash != outbox_row.get("body_sha256"):
            raise PorosRefinementError(
                f"live row {row_id!r} body hash does not match outbox evidence"
            )

    for message_id, evidence in outbox_evidence.items():
        if evidence.get("detached") and message_id in admissions:
            if evidence.get("object_hash_valid") is not True:
                raise PorosRefinementError(
                    f"detached admitted message {message_id!r} lacks object hash proof"
                )
        if evidence.get("sender_hint") and not evidence.get("receiver_authorized"):
            decision = decisions.get(message_id)
            promotion_decision = promotion_decisions.get(message_id)
            if isinstance(decision, Mapping) and decision.get("effect") == "allow":
                raise PorosRefinementError(
                    f"sender hint broadened receiver policy for {message_id!r}"
                )
            if (
                isinstance(promotion_decision, Mapping)
                and promotion_decision.get("effect") == "allow"
            ):
                raise PorosRefinementError(
                    f"sender hint broadened promotion policy for {message_id!r}"
                )
            if message_id in admissions or message_id in published:
                raise PorosRefinementError(
                    f"sender-hint-only message {message_id!r} was admitted"
                )

    proof_rows = _rows(state.get("proofRows", []), "proofRows", PROOF_KEYS)
    proof_rows_by_message = _group_by_message(proof_rows, "proofRows")
    for message_id in public_proofs:
        rows = proof_rows_by_message.get(message_id, [])
        public_rows = [row for row in rows if row.get("visibility") == "public"]
        if not public_rows:
            raise PorosRefinementError(
                f"public proof set references {message_id!r} without proof row"
            )
        for row in public_rows:
            classification = str(row.get("classification") or "")
            if classification not in PROTECTED_CLASSIFICATIONS:
                continue
            decision = decisions.get(message_id)
            has_decision_evidence = (
                isinstance(decision, Mapping)
                and (
                    decision.get("hasRedaction") is True
                    or decision.get("hasDeclassification") is True
                )
            )
            has_proof_evidence = (
                row.get("redacted") is True or row.get("declassified") is True
            )
            if not (has_decision_evidence and has_proof_evidence):
                raise PorosRefinementError(
                    f"protected public proof for {message_id!r} lacks "
                    "redaction/declassification evidence"
                )

    unknown_reference_sets = {
        "outbox": outbox,
        "inbox": inbox,
        "verified": verified,
        "admissions": admissions,
        "blockedState": blocked_state,
        "publishedMessages": published,
        "publicProofMessages": public_proofs,
    }
    for label, values in unknown_reference_sets.items():
        unknown = values.difference(messages)
        if unknown:
            raise PorosRefinementError(
                f"{label} references unknown Poros messages: {sorted(unknown)}"
            )


def _outbox_evidence(row: Mapping[str, Any]) -> _MessageEvidence:
    message_id = _required_str(row, "message_id", "outbox")
    return _MessageEvidence(
        message_id=message_id,
        body_sha256=_required_str(row, "body_sha256", "outbox"),
        sender_generation_id=_required_str(
            row,
            "sender_generation_id",
            "outbox",
        ),
        classification=_optional_str(row, "classification"),
        detached=bool(row.get("detached", False)),
        object_hash_valid=bool(row.get("object_hash_valid", False)),
        sender_hint=bool(row.get("sender_hint", False)),
        receiver_authorized=bool(row.get("receiver_authorized", False)),
    )


def _rows(
    value: Any,
    label: str,
    allowed_keys: frozenset[str],
) -> list[dict[str, Any]]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise PorosRefinementError(f"{label} must be a list")
    out: list[dict[str, Any]] = []
    for index, item in enumerate(value):
        if not isinstance(item, Mapping):
            raise PorosRefinementError(f"{label}[{index}] must be an object")
        _assert_known_keys(f"{label}[{index}]", item, allowed_keys)
        row = dict(item)
        _validate_row(label, row)
        out.append(row)
    return out


def _validate_row(label: str, row: Mapping[str, Any]) -> None:
    if label in {
        "outbox",
        "delivery_attempts",
        "inbox",
        "receipts",
        "nomos_decisions",
        "promotion_decisions",
        "admissions",
        "generations",
        "live_rows",
        "proofs",
        "admissionRows",
        "generationRows",
        "proofRows",
    }:
        _required_str(row, "message_id", label)
    if label in {"inbox"}:
        status = _required_str(row, "status", label)
        if status not in RECEIPT_STATUSES:
            raise PorosRefinementError(f"{label} has unsupported status {status!r}")
    if label in {"delivery_attempts"}:
        status = _required_str(row, "status", label)
        if status not in RECEIPT_STATUSES | {"attempted", "failed"}:
            raise PorosRefinementError(f"{label} has unsupported status {status!r}")
        _required_str(row, "body_sha256", label)
        _required_str(row, "envelope_sha256", label)
    if label in {"receipts"}:
        status = _required_str(row, "status", label)
        if status not in RECEIPT_STATUSES:
            raise PorosRefinementError(f"{label} has unsupported status {status!r}")
    if label in {"nomos_decisions", "promotion_decisions"}:
        effect = _required_str(row, "effect", label)
        if effect not in DECISION_EFFECTS:
            raise PorosRefinementError(f"{label} has unsupported effect {effect!r}")
        _required_str(row, "decision_id", label)
        _required_str(row, "input_hash", label)
        if not isinstance(row.get("uses_receiver_policy"), bool):
            raise PorosRefinementError(
                f"{label} row must include boolean uses_receiver_policy"
            )
        obligations = row.get("obligations", [])
        if not isinstance(obligations, list):
            raise PorosRefinementError(f"{label} obligations must be a list")
    if label in {"admissions", "admissionRows"}:
        status = _required_str(row, "status", label)
        if status not in ADMISSION_STATUSES:
            raise PorosRefinementError(f"{label} has unsupported status {status!r}")
        _required_str(row, "admission_id", label)
    if label in {"generations", "generationRows"}:
        status = _required_str(row, "status", label)
        if status not in GENERATION_STATUSES:
            raise PorosRefinementError(f"{label} has unsupported status {status!r}")
        _required_str(row, "generation_id", label)
    if label in {"live_rows"}:
        row_kind = _required_str(row, "row_kind", label)
        if row_kind not in LIVE_ROW_KINDS:
            raise PorosRefinementError(f"{label} has unsupported row kind {row_kind!r}")
        _required_str(row, "row_id", label)
    if label in {"proofs", "proofRows"}:
        visibility = _required_str(row, "visibility", label)
        if visibility not in PROOF_VISIBILITIES:
            raise PorosRefinementError(
                f"{label} has unsupported visibility {visibility!r}"
            )


def _single_by_message(
    rows: Iterable[Mapping[str, Any]],
    label: str,
) -> dict[str, dict[str, Any]]:
    grouped = _group_by_message(rows, label)
    out = {}
    for message_id, items in grouped.items():
        if len(items) != 1:
            raise PorosRefinementError(
                f"{label} has {len(items)} rows for message {message_id!r}"
            )
        out[message_id] = _normalize_trace_row(label, items[0])
    return out


def _last_by_message(
    rows: Iterable[Mapping[str, Any]],
    label: str,
) -> dict[str, dict[str, Any]]:
    grouped = _group_by_message(rows, label)
    return {
        message_id: _normalize_trace_row(label, items[-1])
        for message_id, items in grouped.items()
    }


def _group_by_message(
    rows: Iterable[Mapping[str, Any]],
    label: str,
) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        message_id = _required_str(row, "message_id", label)
        out.setdefault(message_id, []).append(dict(row))
    return out


def _normalize_trace_row(label: str, row: Mapping[str, Any]) -> dict[str, Any]:
    out = dict(row)
    if label == "receipts":
        body_sha256 = out.get("received_body_sha256") or out.get("body_sha256")
        out["bodyHashBound"] = isinstance(body_sha256, str) and bool(body_sha256)
    if label in {"nomos_decisions", "promotion_decisions"}:
        obligations = [
            item for item in out.get("obligations", []) if isinstance(item, Mapping)
        ]
        obligation_types = {str(item.get("type")) for item in obligations}
        out["usesReceiverPolicy"] = out.get("uses_receiver_policy") is True
        out["inputHashBound"] = bool(out.get("input_hash"))
        out["hasRedaction"] = "redaction" in obligation_types
        out["hasDeclassification"] = "declassification" in obligation_types
    return out


def _promotion_allows(
    message_id: str,
    original_decision: Mapping[str, Any],
    promotion_decisions: Mapping[str, Any],
) -> bool:
    if original_decision.get("effect") not in {"review", "quarantine"}:
        return False
    promotion = promotion_decisions.get(message_id)
    return (
        isinstance(promotion, Mapping)
        and promotion.get("effect") == "allow"
        and promotion.get("usesReceiverPolicy") is True
        and promotion.get("inputHashBound") is True
    )


def _message_row(
    mapping: Mapping[str, Any],
    message_id: str,
    label: str,
) -> Mapping[str, Any]:
    row = mapping.get(message_id)
    if not isinstance(row, Mapping):
        raise PorosRefinementError(
            f"message {message_id!r} missing {label} evidence"
        )
    return row


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise PorosRefinementError(f"{label} must be an object")
    return value


def _required_str(row: Mapping[str, Any], key: str, label: str) -> str:
    value = row.get(key)
    if not isinstance(value, str) or not value:
        raise PorosRefinementError(f"{label} row missing string {key!r}")
    return value


def _optional_str(row: Mapping[str, Any], key: str) -> str | None:
    value = row.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise PorosRefinementError(f"row field {key!r} must be a string")
    return value


def _string_list(value: Any, label: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise PorosRefinementError(f"{label} must be a list")
    out = []
    for index, item in enumerate(value):
        if not isinstance(item, str) or not item:
            raise PorosRefinementError(f"{label}[{index}] must be a string")
        out.append(item)
    return out


def _assert_known_keys(
    label: str,
    row: Mapping[str, Any],
    allowed: frozenset[str],
) -> None:
    unknown = sorted(set(row).difference(allowed))
    if unknown:
        raise PorosRefinementError(f"{label} has unsupported fields: {unknown}")
