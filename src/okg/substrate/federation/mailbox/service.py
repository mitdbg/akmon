"""Poros mailbox service facade."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping

from okg.substrate.federation.mailbox.models import (
    MAILBOX_PAYLOAD_DETACHED_POINTER,
    DetachedObjectPointer,
    MailboxMessageRecord,
    MailboxValidationError,
    canonical_mailbox_json_bytes,
)
from okg.substrate.federation.mailbox.object_store import (
    ObjectStore,
    fetch_and_verify_pointer,
)
from okg.substrate.federation.mailbox.store import MailboxStore


class MailboxService:
    """Dumb transport for signed Poros packets and detached pointers.

    The service intentionally does not decide trust, run Nomos policy, admit
    facts, or publish graph generations. Receivers must validate packets after
    polling and acknowledge only after recording local durable evidence.
    """

    def __init__(
        self,
        *,
        store: MailboxStore,
        object_store: ObjectStore | None = None,
        max_inline_bytes: int = 100_000,
        max_object_bytes: int = 1_000_000_000,
    ) -> None:
        if max_inline_bytes <= 0:
            raise MailboxValidationError("max_inline_bytes must be positive")
        if max_object_bytes <= 0:
            raise MailboxValidationError("max_object_bytes must be positive")
        if max_inline_bytes > max_object_bytes:
            raise MailboxValidationError(
                "max_inline_bytes must not exceed max_object_bytes"
            )
        self.store = store
        self.object_store = object_store
        self.max_inline_bytes = max_inline_bytes
        self.max_object_bytes = max_object_bytes

    def submit_envelope(
        self,
        envelope: Mapping[str, Any],
    ) -> MailboxMessageRecord:
        packet_size = len(canonical_mailbox_json_bytes(envelope))
        if packet_size > self.max_inline_bytes:
            raise MailboxValidationError(
                "inline Poros packet exceeds mailbox max_inline_bytes"
            )
        record = MailboxMessageRecord.from_envelope(envelope)
        return self.store.submit(record)

    def submit_pointer(
        self,
        pointer: DetachedObjectPointer | Mapping[str, Any],
    ) -> MailboxMessageRecord:
        ptr = (
            pointer
            if isinstance(pointer, DetachedObjectPointer)
            else DetachedObjectPointer.from_mapping(pointer)
        )
        if ptr.size_bytes > self.max_object_bytes:
            raise MailboxValidationError(
                "detached object exceeds mailbox max_object_bytes"
            )
        return self.store.submit(MailboxMessageRecord.from_pointer(ptr))

    def submit_object_bytes(
        self,
        *,
        message_id: str,
        sender_authority_id: str,
        recipient_authority_id: str,
        data: bytes,
        expires_at: datetime,
        packet_sha256: str | None = None,
        encryption: Mapping[str, Any] | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> MailboxMessageRecord:
        if self.object_store is None:
            raise MailboxValidationError("mailbox service has no object store")
        if not data:
            raise MailboxValidationError("detached object body is required")
        if len(data) > self.max_object_bytes:
            raise MailboxValidationError(
                "detached object exceeds mailbox max_object_bytes"
            )
        stored = self.object_store.put_bytes(
            message_id,
            data,
            metadata=dict(metadata or {}),
        )
        pointer = DetachedObjectPointer(
            message_id=message_id,
            sender_authority_id=sender_authority_id,
            recipient_authority_id=recipient_authority_id,
            object_uri=stored.uri,
            object_sha256="sha256:" + stored.sha256,
            size_bytes=stored.size_bytes,
            expires_at=expires_at,
            packet_sha256=packet_sha256,
            encryption=dict(encryption or {}),
            metadata=dict(stored.metadata),
        )
        return self.store.submit(MailboxMessageRecord.from_pointer(pointer))

    def poll(
        self,
        recipient_authority_id: str,
        *,
        limit: int = 10,
        lease_seconds: int = 60,
    ) -> list[MailboxMessageRecord]:
        if not recipient_authority_id:
            raise MailboxValidationError("recipient_authority_id is required")
        return self.store.claim_pending(
            recipient_authority_id,
            limit=limit,
            lease_seconds=lease_seconds,
        )

    def get_message(self, message_id: str) -> MailboxMessageRecord | None:
        return self.store.get(message_id)

    def ack_message(
        self,
        message_id: str,
        *,
        lease_token: str,
        receipt_evidence: Mapping[str, Any],
        recipient_principal: str | None = None,
    ) -> MailboxMessageRecord:
        return self.store.ack(
            message_id,
            lease_token=lease_token,
            receipt_evidence=receipt_evidence,
            recipient_principal=recipient_principal,
        )

    def deny_message(
        self,
        message_id: str,
        *,
        lease_token: str,
        reason_code: str,
        denial_evidence: Mapping[str, Any],
        recipient_principal: str | None = None,
    ) -> MailboxMessageRecord:
        return self.store.deny(
            message_id,
            lease_token=lease_token,
            reason_code=reason_code,
            denial_evidence=denial_evidence,
            recipient_principal=recipient_principal,
        )

    def fetch_detached_bytes(self, record: MailboxMessageRecord) -> bytes:
        if record.payload_mode != MAILBOX_PAYLOAD_DETACHED_POINTER:
            raise MailboxValidationError("mailbox record is not a detached pointer")
        if record.pointer is None:
            raise MailboxValidationError("mailbox record has no detached pointer")
        if self.object_store is None:
            raise MailboxValidationError("mailbox service has no object store")
        return fetch_and_verify_pointer(record.pointer, self.object_store)

    def health(self) -> dict[str, Any]:
        return {
            "status": "ok",
            "max_inline_bytes": self.max_inline_bytes,
            "max_object_bytes": self.max_object_bytes,
            "object_store_configured": self.object_store is not None,
        }
