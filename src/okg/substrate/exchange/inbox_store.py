"""Durable receiver-side Poros preflight evidence."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from okg.substrate.exchange.canonical_json import canonical_json_bytes
from okg.substrate.exchange.constants import EXCHANGE_CLASSIFICATIONS
from okg.substrate.exchange.envelope import parse_timestamp
from okg.substrate.exchange.hashing import sha256_b64url
from okg.substrate.exchange.inbox import PorosInboxResult
from okg.substrate.exchange.receipts import build_receipt


@dataclass(frozen=True)
class PorosInboxPersistResult:
    inbox_id: int
    receiver_authority_id: str
    sender_authority_id: str
    message_id: str
    status: str
    reason_code: str | None
    body_sha256: str | None
    signing_key_id: str | None
    envelope_sha256: str
    receipt_sha256: str | None
    inserted: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "inbox_id": self.inbox_id,
            "receiver_authority_id": self.receiver_authority_id,
            "sender_authority_id": self.sender_authority_id,
            "message_id": self.message_id,
            "status": self.status,
            "reason_code": self.reason_code,
            "body_sha256": self.body_sha256,
            "signing_key_id": self.signing_key_id,
            "envelope_sha256": self.envelope_sha256,
            "receipt_sha256": self.receipt_sha256,
            "inserted": self.inserted,
        }


def persist_poros_inbox_preflight(
    conn: psycopg.Connection,
    *,
    receiver_authority_id: str,
    envelope: dict[str, Any],
    result: PorosInboxResult,
    receiver_private_key: bytes | str | None = None,
    receiver_signing_kid: str | None = None,
) -> PorosInboxPersistResult:
    """Persist preflight outcome and signed receipt without admitting facts."""
    message_id = _required_str(envelope, "message_id")
    sender_authority_id = _required_str(envelope, "sender")
    signing_key_id = (
        str(envelope["signing_key_id"])
        if isinstance(envelope.get("signing_key_id"), str)
        else None
    )
    body_sha256 = (
        str(envelope["body_sha256"])
        if isinstance(envelope.get("body_sha256"), str)
        else None
    )
    envelope_sha256 = sha256_b64url(canonical_json_bytes(envelope))
    now = datetime.now(UTC)
    duplicate = _seen_replay_cache(
        conn,
        receiver_authority_id=receiver_authority_id,
        sender_authority_id=sender_authority_id,
        message_id=message_id,
        now=now,
    )
    status = result.status
    reason_code = result.reason_code
    receipt = result.receipt
    if duplicate:
        status = "duplicate"
        reason_code = "duplicate"
        if receiver_private_key is not None and receiver_signing_kid is not None:
            receipt = build_receipt(
                envelope=envelope,
                receiver=receiver_authority_id,
                status=status,
                reason_code=reason_code,
                private_key=receiver_private_key,
                kid=receiver_signing_kid,
                now=now,
            )
    receipt_sha256 = (
        sha256_b64url(canonical_json_bytes(receipt))
        if receipt is not None
        else None
    )
    admission_candidate = None
    received_at = now
    if result.admission_candidate is not None and not duplicate:
        admission_candidate = {
            "message_id": result.admission_candidate.message_id,
            "sender": result.admission_candidate.sender,
            "recipient": result.admission_candidate.recipient,
            "body_sha256": result.admission_candidate.body_sha256,
            "received_at": result.admission_candidate.received_at,
            "body": result.admission_candidate.body,
        }
        received_at = datetime.fromisoformat(
            result.admission_candidate.received_at.replace("Z", "+00:00")
        )
    accepted = status == "accepted_for_admission"
    with conn.cursor() as cur:
        if duplicate:
            cur.execute(
                """
                SELECT inbox_id
                  FROM okg.poros_inbox_messages
                 WHERE receiver_authority_id = %s
                   AND message_id = %s
                """,
                (receiver_authority_id, message_id),
            )
            row = cur.fetchone()
            if row is not None:
                inbox_id = row[0]
                inserted = False
            else:
                cur.execute(
                    """
                    INSERT INTO okg.poros_inbox_messages (
                        receiver_authority_id, sender_authority_id, message_id,
                        status, reason_code, body_sha256, signing_key_id,
                        envelope_sha256, receipt_sha256, envelope, receipt,
                        admission_candidate, trusted, signature_valid,
                        recipient_valid, body_hash_valid, fresh, received_at
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        false, false, false, false, false, %s
                    )
                    RETURNING inbox_id, true AS inserted
                    """,
                    (
                        receiver_authority_id,
                        sender_authority_id,
                        message_id,
                        status,
                        reason_code,
                        body_sha256,
                        signing_key_id,
                        envelope_sha256,
                        receipt_sha256,
                        Jsonb(envelope),
                        Jsonb(receipt),
                        Jsonb(admission_candidate),
                        received_at,
                    ),
                )
                inbox_id, inserted = cur.fetchone()
        else:
            cur.execute(
                """
                INSERT INTO okg.poros_inbox_messages (
                    receiver_authority_id, sender_authority_id, message_id,
                    status, reason_code, body_sha256, signing_key_id,
                    envelope_sha256, receipt_sha256, envelope, receipt,
                    admission_candidate, trusted, signature_valid,
                    recipient_valid, body_hash_valid, fresh, received_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (receiver_authority_id, message_id)
                DO UPDATE SET
                    status = EXCLUDED.status,
                    reason_code = EXCLUDED.reason_code,
                    body_sha256 = EXCLUDED.body_sha256,
                    signing_key_id = EXCLUDED.signing_key_id,
                    envelope_sha256 = EXCLUDED.envelope_sha256,
                    receipt_sha256 = EXCLUDED.receipt_sha256,
                    envelope = EXCLUDED.envelope,
                    receipt = EXCLUDED.receipt,
                    admission_candidate = EXCLUDED.admission_candidate,
                    trusted = EXCLUDED.trusted,
                    signature_valid = EXCLUDED.signature_valid,
                    recipient_valid = EXCLUDED.recipient_valid,
                    body_hash_valid = EXCLUDED.body_hash_valid,
                    fresh = EXCLUDED.fresh,
                    received_at = EXCLUDED.received_at,
                    updated_at = now()
                RETURNING inbox_id, (xmax = 0) AS inserted
                """,
                (
                    receiver_authority_id,
                    sender_authority_id,
                    message_id,
                    status,
                    reason_code,
                    body_sha256,
                    signing_key_id,
                    envelope_sha256,
                    receipt_sha256,
                    Jsonb(envelope),
                    Jsonb(receipt),
                    Jsonb(admission_candidate),
                    accepted,
                    accepted,
                    accepted,
                    accepted,
                    accepted,
                    received_at,
                ),
            )
            inbox_id, inserted = cur.fetchone()
    _upsert_replay_cache(
        conn,
        receiver_authority_id=receiver_authority_id,
        sender_authority_id=sender_authority_id,
        message_id=message_id,
        body_sha256=body_sha256,
        status=status,
        reason_code=reason_code,
        receipt_sha256=receipt_sha256,
        envelope=envelope,
        duplicate=duplicate,
    )
    _persist_detached_object_manifests(
        conn,
        receiver_authority_id=receiver_authority_id,
        message_id=message_id,
        body_sha256=body_sha256,
        envelope=envelope,
    )
    _record_incorporation_event(
        conn,
        receiver_authority_id=receiver_authority_id,
        sender_authority_id=sender_authority_id,
        message_id=message_id,
        body_sha256=body_sha256,
        event_type=(
            "duplicate"
            if status == "duplicate"
            else (
                "preflight_accepted"
                if status == "accepted_for_admission"
                else "preflight_rejected"
            )
        ),
        status=status,
        reason_code=reason_code,
        payload={
            "inbox_id": int(inbox_id),
            "envelope_sha256": envelope_sha256,
            "receipt_sha256": receipt_sha256,
        },
    )
    return PorosInboxPersistResult(
        inbox_id=int(inbox_id),
        receiver_authority_id=receiver_authority_id,
        sender_authority_id=sender_authority_id,
        message_id=message_id,
        status=status,
        reason_code=reason_code,
        body_sha256=body_sha256,
        signing_key_id=signing_key_id,
        envelope_sha256=envelope_sha256,
        receipt_sha256=receipt_sha256,
        inserted=bool(inserted),
    )


def record_poros_nomos_decision(
    conn: psycopg.Connection,
    *,
    deployment_name: str = "default",
    receiver_authority_id: str,
    sender_authority_id: str,
    message_id: str,
    body_sha256: str | None,
    decision: dict[str, Any],
) -> None:
    """Persist receiver-owned Nomos evidence before Poros admission."""
    _upsert_nomos_decision_mapping(
        conn,
        deployment_name=deployment_name,
        decision=decision,
        target={
            "kind": "poros_message",
            "receiver_authority_id": receiver_authority_id,
            "sender_authority_id": sender_authority_id,
            "message_id": message_id,
        },
    )
    effect = _required_decision_str(decision, "effect")
    reason_code = decision.get("reason_code")
    _record_incorporation_event(
        conn,
        receiver_authority_id=receiver_authority_id,
        sender_authority_id=sender_authority_id,
        message_id=message_id,
        body_sha256=body_sha256,
        event_type="nomos_decided",
        status=effect,
        reason_code=str(reason_code) if reason_code is not None else None,
        payload=decision,
        nomos_decision_id=_required_decision_str(decision, "decision_id"),
        nomos_decision_deployment_name=deployment_name,
        policy_bundle_id=(
            str(decision["policy_bundle_id"])
            if decision.get("policy_bundle_id") is not None
            else None
        ),
        policy_fingerprint=(
            str(decision["policy_fingerprint"])
            if decision.get("policy_fingerprint") is not None
            else None
        ),
        nomos_input_hash=_required_decision_str(decision, "input_hash"),
    )


def record_poros_promotion_decision(
    conn: psycopg.Connection,
    *,
    deployment_name: str = "default",
    receiver_authority_id: str,
    sender_authority_id: str,
    message_id: str,
    body_sha256: str | None,
    decision: dict[str, Any],
) -> None:
    """Persist receiver-owned promotion policy evidence."""
    _upsert_nomos_decision_mapping(
        conn,
        deployment_name=deployment_name,
        decision=decision,
        target={
            "kind": "poros_promotion",
            "receiver_authority_id": receiver_authority_id,
            "sender_authority_id": sender_authority_id,
            "message_id": message_id,
        },
    )
    effect = _required_decision_str(decision, "effect")
    reason_code = decision.get("reason_code")
    _record_incorporation_event(
        conn,
        receiver_authority_id=receiver_authority_id,
        sender_authority_id=sender_authority_id,
        message_id=message_id,
        body_sha256=body_sha256,
        event_type="promotion_decided",
        status=effect,
        reason_code=str(reason_code) if reason_code is not None else None,
        payload=decision,
        nomos_decision_id=_required_decision_str(decision, "decision_id"),
        nomos_decision_deployment_name=deployment_name,
        policy_bundle_id=(
            str(decision["policy_bundle_id"])
            if decision.get("policy_bundle_id") is not None
            else None
        ),
        policy_fingerprint=(
            str(decision["policy_fingerprint"])
            if decision.get("policy_fingerprint") is not None
            else None
        ),
        nomos_input_hash=_required_decision_str(decision, "input_hash"),
    )


def record_poros_rejection_decision(
    conn: psycopg.Connection,
    *,
    deployment_name: str = "default",
    receiver_authority_id: str,
    sender_authority_id: str,
    message_id: str,
    body_sha256: str | None,
    decision: dict[str, Any],
) -> None:
    """Persist receiver-owned curator rejection policy evidence."""
    _upsert_nomos_decision_mapping(
        conn,
        deployment_name=deployment_name,
        decision=decision,
        target={
            "kind": "poros_rejection",
            "receiver_authority_id": receiver_authority_id,
            "sender_authority_id": sender_authority_id,
            "message_id": message_id,
        },
    )
    effect = _required_decision_str(decision, "effect")
    reason_code = decision.get("reason_code")
    _record_incorporation_event(
        conn,
        receiver_authority_id=receiver_authority_id,
        sender_authority_id=sender_authority_id,
        message_id=message_id,
        body_sha256=body_sha256,
        event_type="rejection_decided",
        status=effect,
        reason_code=str(reason_code) if reason_code is not None else None,
        payload=decision,
        nomos_decision_id=_required_decision_str(decision, "decision_id"),
        nomos_decision_deployment_name=deployment_name,
        policy_bundle_id=(
            str(decision["policy_bundle_id"])
            if decision.get("policy_bundle_id") is not None
            else None
        ),
        policy_fingerprint=(
            str(decision["policy_fingerprint"])
            if decision.get("policy_fingerprint") is not None
            else None
        ),
        nomos_input_hash=_required_decision_str(decision, "input_hash"),
    )


def record_poros_incorporation_lifecycle(
    conn: psycopg.Connection,
    *,
    receiver_authority_id: str,
    sender_authority_id: str,
    message_id: str,
    body_sha256: str | None,
    event_type: str,
    status: str,
    reason_code: str | None = None,
    admission_id: str | None = None,
    receiver_generation_id: int | str | None = None,
    payload: dict[str, Any] | None = None,
) -> None:
    """Persist admission/publication lifecycle evidence for a Poros message."""
    if event_type not in {
        "admitted",
        "published",
        "blocked",
        "quarantined",
        "review",
        "promotion_denied",
        "promoted",
        "rejection_denied",
        "rejected",
    }:
        raise ValueError(f"unsupported Poros lifecycle event type: {event_type}")
    _record_incorporation_event(
        conn,
        receiver_authority_id=receiver_authority_id,
        sender_authority_id=sender_authority_id,
        message_id=message_id,
        body_sha256=body_sha256,
        event_type=event_type,
        status=status,
        reason_code=reason_code,
        payload=payload or {},
        admission_id=admission_id,
        receiver_generation_id=receiver_generation_id,
    )


def _required_str(envelope: dict[str, Any], field: str) -> str:
    value = envelope.get(field)
    if not isinstance(value, str) or not value:
        raise ValueError(f"Poros envelope missing required {field!r}")
    return value


def _required_decision_str(decision: Mapping[str, Any], field: str) -> str:
    value = decision.get(field)
    if not isinstance(value, str) or not value:
        raise ValueError(f"Nomos decision missing required {field!r}")
    return value


def _seen_replay_cache(
    conn: psycopg.Connection,
    *,
    receiver_authority_id: str,
    sender_authority_id: str,
    message_id: str,
    now: datetime,
) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
              FROM okg.poros_replay_cache
             WHERE receiver_authority_id = %s
               AND sender_authority_id = %s
               AND message_id = %s
               AND expires_at > %s
            """,
            (receiver_authority_id, sender_authority_id, message_id, now),
        )
        return cur.fetchone() is not None


def _upsert_replay_cache(
    conn: psycopg.Connection,
    *,
    receiver_authority_id: str,
    sender_authority_id: str,
    message_id: str,
    body_sha256: str | None,
    status: str,
    reason_code: str | None,
    receipt_sha256: str | None,
    envelope: dict[str, Any],
    duplicate: bool,
) -> None:
    expires_at = _replay_expires_at(envelope)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.poros_replay_cache (
                receiver_authority_id, sender_authority_id, message_id,
                body_sha256, status, reason_code, receipt_sha256, expires_at,
                duplicate_count
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (
                receiver_authority_id, sender_authority_id, message_id
            )
            DO UPDATE SET
                body_sha256 = EXCLUDED.body_sha256,
                status = EXCLUDED.status,
                reason_code = EXCLUDED.reason_code,
                receipt_sha256 = EXCLUDED.receipt_sha256,
                expires_at = GREATEST(
                    okg.poros_replay_cache.expires_at,
                    EXCLUDED.expires_at
                ),
                last_seen_at = now(),
                duplicate_count = (
                    okg.poros_replay_cache.duplicate_count
                    + CASE WHEN %s THEN 1 ELSE 0 END
                )
            """,
            (
                receiver_authority_id,
                sender_authority_id,
                message_id,
                body_sha256,
                status,
                reason_code,
                receipt_sha256,
                expires_at,
                1 if duplicate else 0,
                duplicate,
            ),
        )


def _replay_expires_at(envelope: dict[str, Any]) -> datetime:
    raw = envelope.get("expires_at")
    if isinstance(raw, str):
        try:
            return parse_timestamp(raw, field="expires_at")
        except Exception:  # noqa: BLE001 - rejected messages still need evidence.
            pass
    return datetime.now(UTC)


def _persist_detached_object_manifests(
    conn: psycopg.Connection,
    *,
    receiver_authority_id: str,
    message_id: str,
    body_sha256: str | None,
    envelope: dict[str, Any],
) -> None:
    body = envelope.get("body")
    if not isinstance(body, dict):
        return
    objects = body.get("objects")
    if not isinstance(objects, list):
        return
    with conn.cursor() as cur:
        for raw in objects:
            if not isinstance(raw, dict):
                continue
            row = _detached_manifest_row(raw)
            if row is None:
                continue
            cur.execute(
                """
                INSERT INTO okg.poros_detached_object_manifests (
                    message_id, body_sha256, authority_id, side,
                    object_id, media_type, size_bytes, object_sha256,
                    chunk_count, classification, encrypted, manifest
                )
                VALUES (%s, %s, %s, 'receiver', %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (side, authority_id, message_id, object_id)
                DO UPDATE SET
                    body_sha256 = EXCLUDED.body_sha256,
                    media_type = EXCLUDED.media_type,
                    size_bytes = EXCLUDED.size_bytes,
                    object_sha256 = EXCLUDED.object_sha256,
                    chunk_count = EXCLUDED.chunk_count,
                    classification = EXCLUDED.classification,
                    encrypted = EXCLUDED.encrypted,
                    manifest = EXCLUDED.manifest
                """,
                (
                    message_id,
                    body_sha256 or "",
                    receiver_authority_id,
                    row["object_id"],
                    row["media_type"],
                    row["size_bytes"],
                    row["object_sha256"],
                    row["chunk_count"],
                    row["classification"],
                    row["encrypted"],
                    Jsonb(raw),
                ),
            )


def _detached_manifest_row(raw: dict[str, Any]) -> dict[str, Any] | None:
    """Return the side-table row only for structurally coherent manifests.

    Rejected preflight records still persist the raw envelope. This table stores
    normalized detached-object evidence, so malformed object entries are skipped
    instead of turning the whole rejected-preflight audit transaction into a
    constraint failure.
    """
    object_id = raw.get("object_id")
    if not isinstance(object_id, str) or not object_id:
        return None
    classification = raw.get("classification", "public")
    if not isinstance(classification, str) or classification not in EXCHANGE_CLASSIFICATIONS:
        return None
    object_hash = (
        raw.get("cleartext_sha256")
        or raw.get("ciphertext_sha256")
        or raw.get("sha256")
    )
    if not isinstance(object_hash, str) or not object_hash:
        return None
    encrypted = "encryption" in raw or "ciphertext_sha256" in raw
    if encrypted:
        if raw.get("ciphertext_sha256") != object_hash:
            return None
        if not isinstance(raw.get("encryption"), Mapping):
            return None
    else:
        if raw.get("cleartext_sha256") != object_hash and raw.get("sha256") != object_hash:
            return None
    size_bytes = _optional_non_negative_int(raw.get("size_bytes", raw.get("size")))
    if size_bytes is None:
        return None
    chunks = raw.get("chunks") or []
    if not isinstance(chunks, list):
        return None
    return {
        "object_id": object_id,
        "media_type": raw.get("media_type"),
        "size_bytes": size_bytes,
        "object_sha256": object_hash,
        "chunk_count": len(chunks),
        "classification": classification,
        "encrypted": encrypted,
    }


def _optional_non_negative_int(value: Any) -> int | None:
    if value is None:
        return 0
    if isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    if parsed < 0:
        return None
    return parsed


def _upsert_nomos_decision_mapping(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    decision: Mapping[str, Any],
    target: Mapping[str, Any],
) -> None:
    matched_rule_ids = decision.get("matched_rule_ids") or []
    obligations = decision.get("obligations") or []
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.nomos_decisions (
              deployment_name, decision_id, policy_bundle_id, policy_version,
              policy_fingerprint, input_hash, subject, action, resource,
              classification, context, effect, reason_code, matched_rule_ids,
              obligations, enforcement_point, target
            )
            VALUES (
              %(deployment_name)s, %(decision_id)s, %(policy_bundle_id)s,
              %(policy_version)s, %(policy_fingerprint)s, %(input_hash)s,
              %(subject)s, %(action)s, %(resource)s, %(classification)s,
              %(context)s, %(effect)s, %(reason_code)s, %(matched_rule_ids)s,
              %(obligations)s, %(enforcement_point)s, %(target)s
            )
            ON CONFLICT (deployment_name, decision_id)
            DO UPDATE SET
              target = EXCLUDED.target
            """,
            {
                "deployment_name": deployment_name,
                "decision_id": _required_decision_str(decision, "decision_id"),
                "policy_bundle_id": decision.get("policy_bundle_id"),
                "policy_version": decision.get("policy_version"),
                "policy_fingerprint": decision.get("policy_fingerprint"),
                "input_hash": _required_decision_str(decision, "input_hash"),
                "subject": _required_decision_str(decision, "subject"),
                "action": _required_decision_str(decision, "action"),
                "resource": Jsonb(dict(decision.get("resource") or {})),
                "classification": _required_decision_str(
                    decision, "classification",
                ),
                "context": Jsonb(dict(decision.get("context") or {})),
                "effect": _required_decision_str(decision, "effect"),
                "reason_code": _required_decision_str(decision, "reason_code"),
                "matched_rule_ids": [str(item) for item in matched_rule_ids],
                "obligations": Jsonb(list(obligations)),
                "enforcement_point": _required_decision_str(
                    decision, "enforcement_point",
                ),
                "target": Jsonb(dict(target)),
            },
        )


def _record_incorporation_event(
    conn: psycopg.Connection,
    *,
    receiver_authority_id: str,
    sender_authority_id: str,
    message_id: str,
    body_sha256: str | None,
    event_type: str,
    status: str,
    reason_code: str | None,
    payload: dict[str, Any],
    nomos_decision_id: str | None = None,
    nomos_decision_deployment_name: str = "default",
    policy_bundle_id: str | None = None,
    policy_fingerprint: str | None = None,
    nomos_input_hash: str | None = None,
    admission_id: str | None = None,
    receiver_generation_id: int | str | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.poros_incorporation_events (
                receiver_authority_id, sender_authority_id, message_id,
                body_sha256, event_type, status, reason_code,
                nomos_decision_id, nomos_decision_deployment_name,
                policy_bundle_id, policy_fingerprint, nomos_input_hash,
                admission_id, receiver_generation_id, payload
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s::uuid, %s, %s
            )
            """,
            (
                receiver_authority_id,
                sender_authority_id,
                message_id,
                body_sha256,
                event_type,
                status,
                reason_code,
                nomos_decision_id,
                nomos_decision_deployment_name,
                policy_bundle_id,
                policy_fingerprint,
                nomos_input_hash,
                admission_id,
                receiver_generation_id,
                Jsonb(payload),
            ),
        )
