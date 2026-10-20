"""Poros inbox preflight handler."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Callable

from okg.substrate.exchange.authority import TrustPinRegistry
from okg.substrate.exchange.canonical_json import loads_canonical_json
from okg.substrate.exchange.constants import ENCRYPTED_GRAPH_SUBMIT_TYPE
from okg.substrate.exchange.encryption import decrypt_private_envelope
from okg.substrate.exchange.envelope import (
    validate_algorithms,
    validate_body_hash,
    validate_envelope_shape,
)
from okg.substrate.exchange.errors import (
    PorosDuplicateMessage,
    PorosError,
    PorosPayloadError,
)
from okg.substrate.exchange.payloads import enforce_payload_policy
from okg.substrate.exchange.receipts import build_receipt
from okg.substrate.exchange.replay import InMemoryReplayCache
from okg.substrate.exchange.signing import signature_kid, verify_signed_document


@dataclass(frozen=True)
class PorosAdmissionCandidate:
    message_id: str
    sender: str
    recipient: str
    body_sha256: str
    body: dict[str, Any]
    envelope: dict[str, Any]
    received_at: str


@dataclass(frozen=True)
class PorosInboxResult:
    status: str
    reason_code: str | None
    admission_candidate: PorosAdmissionCandidate | None
    receipt: dict[str, Any] | None


class PorosInbox:
    """Preflight Poros envelopes without bypassing admission gates."""

    def __init__(
        self,
        *,
        receiver_authority_id: str,
        trust_registry: TrustPinRegistry,
        receiver_private_key: bytes | str,
        receiver_signing_kid: str,
        receiver_encryption_private_key: bytes | str | None = None,
        replay_cache: InMemoryReplayCache | None = None,
        allow_medium_inline: bool = False,
        detached_object_verifier: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self.receiver_authority_id = receiver_authority_id
        self.trust_registry = trust_registry
        self.receiver_private_key = receiver_private_key
        self.receiver_signing_kid = receiver_signing_kid
        self.receiver_encryption_private_key = receiver_encryption_private_key
        self.replay_cache = replay_cache or InMemoryReplayCache()
        self.allow_medium_inline = allow_medium_inline
        self.detached_object_verifier = detached_object_verifier

    def handle_bytes(
        self,
        raw: bytes,
        *,
        now: datetime | None = None,
    ) -> PorosInboxResult:
        try:
            envelope = loads_canonical_json(raw)
            if not isinstance(envelope, dict):
                raise PorosPayloadError(
                    "Poros envelope must be an object",
                    reason_code="invalid_envelope",
                )
        except PorosError as exc:
            return PorosInboxResult(
                status="rejected_preflight",
                reason_code=exc.reason_code,
                admission_candidate=None,
                receipt=None,
            )
        return self.handle_envelope(envelope, now=now)

    def handle_envelope(
        self,
        envelope: dict[str, Any],
        *,
        now: datetime | None = None,
        admission_id: str | None = None,
    ) -> PorosInboxResult:
        current = (now or datetime.now(UTC)).astimezone(UTC)
        try:
            validate_envelope_shape(envelope)
            if envelope.get("recipient") != self.receiver_authority_id:
                raise PorosPayloadError(
                    "Poros envelope recipient does not match this authority",
                    reason_code="recipient_mismatch",
                    data={"recipient": envelope.get("recipient")},
                )
            validate_algorithms(envelope)
            kid = signature_kid(envelope)
            public_key = self.trust_registry.resolve_public_key(
                authority_id=str(envelope.get("sender")),
                kid=kid,
                use="sign",
            )
            verify_signed_document(envelope, public_key)
            validate_body_hash(envelope)
            effective_envelope = self._decrypt_if_private(envelope)
            self.replay_cache.check_and_remember(envelope, now=current)
            enforce_payload_policy(
                effective_envelope,
                allow_medium_inline=self.allow_medium_inline,
                receiver_authority_id=self.receiver_authority_id,
            )
            if self.detached_object_verifier is not None:
                self.detached_object_verifier(effective_envelope)
        except PorosDuplicateMessage as exc:
            return self._result(
                envelope=envelope,
                status="duplicate",
                reason_code=exc.reason_code,
                admission_candidate=None,
                now=current,
            )
        except PorosError as exc:
            return self._result(
                envelope=envelope,
                status="rejected_preflight",
                reason_code=exc.reason_code,
                admission_candidate=None,
                now=current,
            )
        body = effective_envelope.get("body")
        if not isinstance(body, dict):
            return self._result(
                envelope=envelope,
                status="rejected_preflight",
                reason_code="invalid_body",
                admission_candidate=None,
                now=current,
            )
        candidate = PorosAdmissionCandidate(
            message_id=str(effective_envelope["message_id"]),
            sender=str(effective_envelope["sender"]),
            recipient=str(effective_envelope["recipient"]),
            body_sha256=str(effective_envelope["body_sha256"]),
            body=body,
            envelope=effective_envelope,
            received_at=current.isoformat().replace("+00:00", "Z"),
        )
        return self._result(
            envelope=envelope,
            status="accepted_for_admission",
            reason_code=None,
            admission_candidate=candidate,
            admission_id=admission_id,
            now=current,
        )

    def _result(
        self,
        *,
        envelope: dict[str, Any],
        status: str,
        reason_code: str | None,
        admission_candidate: PorosAdmissionCandidate | None,
        now: datetime,
        admission_id: str | None = None,
    ) -> PorosInboxResult:
        receipt = build_receipt(
            envelope=envelope,
            receiver=self.receiver_authority_id,
            status=status,
            reason_code=reason_code,
            admission_id=admission_id,
            private_key=self.receiver_private_key,
            kid=self.receiver_signing_kid,
            now=now,
        )
        return PorosInboxResult(
            status=status,
            reason_code=reason_code,
            admission_candidate=admission_candidate,
            receipt=receipt,
        )

    def _decrypt_if_private(self, envelope: dict[str, Any]) -> dict[str, Any]:
        if envelope.get("type") != ENCRYPTED_GRAPH_SUBMIT_TYPE:
            return envelope
        if self.receiver_encryption_private_key is None:
            raise PorosPayloadError(
                "encrypted Poros envelope requires receiver encryption key",
                reason_code="missing_receiver_encryption_key",
            )
        body = envelope.get("body")
        if not isinstance(body, dict):
            raise PorosPayloadError(
                "encrypted Poros envelope body must be an object",
                reason_code="invalid_encrypted_envelope",
            )
        inner = decrypt_private_envelope(
            body,
            self.receiver_encryption_private_key,
        )
        validate_envelope_shape(inner)
        if inner.get("type") == ENCRYPTED_GRAPH_SUBMIT_TYPE:
            raise PorosPayloadError(
                "nested encrypted Poros envelopes are not supported",
                reason_code="nested_encrypted_envelope",
            )
        if inner.get("message_id") != envelope.get("message_id"):
            raise PorosPayloadError(
                "inner and outer Poros message ids differ",
                reason_code="inner_outer_message_id_mismatch",
            )
        if inner.get("sender") != envelope.get("sender"):
            raise PorosPayloadError(
                "inner and outer Poros senders differ",
                reason_code="inner_outer_sender_mismatch",
            )
        if inner.get("recipient") != self.receiver_authority_id:
            raise PorosPayloadError(
                "inner Poros envelope recipient does not match this authority",
                reason_code="recipient_mismatch",
                data={"recipient": inner.get("recipient")},
            )
        validate_algorithms(inner)
        inner_kid = signature_kid(inner)
        inner_public_key = self.trust_registry.resolve_public_key(
            authority_id=str(inner.get("sender")),
            kid=inner_kid,
            use="sign",
        )
        verify_signed_document(inner, inner_public_key)
        validate_body_hash(inner)
        return inner
