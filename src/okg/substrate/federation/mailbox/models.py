"""Provider-neutral Poros mailbox transport models."""

from __future__ import annotations

import base64
import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Mapping

from okg.substrate.federation import validation


MAILBOX_STATUS_PENDING = "pending"
MAILBOX_STATUS_LEASED = "leased"
MAILBOX_STATUS_ACKNOWLEDGED = "acknowledged"
MAILBOX_STATUS_DENIED = "denied"

VALID_MAILBOX_STATUSES = frozenset({
    MAILBOX_STATUS_PENDING,
    MAILBOX_STATUS_LEASED,
    MAILBOX_STATUS_ACKNOWLEDGED,
    MAILBOX_STATUS_DENIED,
})

MAILBOX_PAYLOAD_INLINE = "inline"
MAILBOX_PAYLOAD_DETACHED_POINTER = "detached_pointer"
VALID_MAILBOX_PAYLOAD_MODES = frozenset({
    MAILBOX_PAYLOAD_INLINE,
    MAILBOX_PAYLOAD_DETACHED_POINTER,
})
MAX_SAFE_INTEGER = 9_007_199_254_740_991


class MailboxValidationError(ValueError):
    """Raised when a mailbox transport request is malformed or unsafe."""


@dataclass(frozen=True)
class DetachedObjectPointer:
    message_id: str
    sender_authority_id: str
    recipient_authority_id: str
    object_uri: str
    object_sha256: str
    size_bytes: int
    expires_at: datetime
    packet_sha256: str | None = None
    encryption: Mapping[str, Any] = field(default_factory=dict)
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "DetachedObjectPointer":
        size_bytes = _required_int(raw, "size_bytes")
        if size_bytes <= 0:
            raise MailboxValidationError("detached pointer size_bytes must be positive")
        return cls(
            message_id=_required_str(raw, "message_id"),
            sender_authority_id=_required_str(raw, "sender_authority_id", "sender"),
            recipient_authority_id=_required_str(
                raw,
                "recipient_authority_id",
                "recipient",
            ),
            object_uri=_required_str(raw, "object_uri", "uri"),
            object_sha256=_required_str(raw, "object_sha256", "sha256"),
            size_bytes=size_bytes,
            expires_at=_parse_time(_required_str(raw, "expires_at"), "expires_at"),
            packet_sha256=_optional_str(raw, "packet_sha256"),
            encryption=_optional_mapping(raw, "encryption"),
            metadata=_optional_mapping(raw, "metadata"),
        )

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "message_id": self.message_id,
            "sender_authority_id": self.sender_authority_id,
            "recipient_authority_id": self.recipient_authority_id,
            "object_uri": self.object_uri,
            "object_sha256": self.object_sha256,
            "size_bytes": self.size_bytes,
            "expires_at": format_timestamp(self.expires_at),
            "encryption": dict(self.encryption),
            "metadata": dict(self.metadata),
        }
        if self.packet_sha256 is not None:
            out["packet_sha256"] = self.packet_sha256
        return out


@dataclass(frozen=True)
class MailboxMessageRecord:
    message_id: str
    sender_authority_id: str
    recipient_authority_id: str
    payload_mode: str
    packet_sha256: str
    body_sha256: str | None
    expires_at: datetime
    status: str = MAILBOX_STATUS_PENDING
    envelope: Mapping[str, Any] | None = None
    pointer: DetachedObjectPointer | None = None
    lease_token: str | None = None
    lease_expires_at: datetime | None = None
    receipt_evidence: Mapping[str, Any] | None = None
    denial_evidence: Mapping[str, Any] | None = None
    deny_reason_code: str | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    @classmethod
    def from_envelope(cls, envelope: Mapping[str, Any]) -> "MailboxMessageRecord":
        env = dict(envelope)
        message_id = _required_str(env, "message_id")
        sender = _required_str(env, "sender")
        recipient = _required_str(env, "recipient")
        expires_at = _parse_time(_required_str(env, "expires_at"), "expires_at")
        packet_sha256 = _packet_sha256(env)
        body_sha256 = _optional_str(env, "body_sha256")
        return cls(
            message_id=message_id,
            sender_authority_id=sender,
            recipient_authority_id=recipient,
            payload_mode=MAILBOX_PAYLOAD_INLINE,
            packet_sha256=packet_sha256,
            body_sha256=body_sha256,
            expires_at=expires_at,
            envelope=env,
        )

    @classmethod
    def from_pointer(
        cls,
        pointer: DetachedObjectPointer | Mapping[str, Any],
    ) -> "MailboxMessageRecord":
        ptr = (
            pointer
            if isinstance(pointer, DetachedObjectPointer)
            else DetachedObjectPointer.from_mapping(pointer)
        )
        packet_sha256 = ptr.packet_sha256 or _prefixed_sha256(ptr.object_sha256)
        return cls(
            message_id=ptr.message_id,
            sender_authority_id=ptr.sender_authority_id,
            recipient_authority_id=ptr.recipient_authority_id,
            payload_mode=MAILBOX_PAYLOAD_DETACHED_POINTER,
            packet_sha256=packet_sha256,
            body_sha256=None,
            expires_at=ptr.expires_at,
            pointer=ptr,
        )

    @property
    def acknowledged(self) -> bool:
        return self.status == MAILBOX_STATUS_ACKNOWLEDGED

    @property
    def denied(self) -> bool:
        return self.status == MAILBOX_STATUS_DENIED

    def to_dict(self, *, include_payload: bool = False) -> dict[str, Any]:
        out: dict[str, Any] = {
            "message_id": self.message_id,
            "sender_authority_id": self.sender_authority_id,
            "recipient_authority_id": self.recipient_authority_id,
            "payload_mode": self.payload_mode,
            "packet_sha256": self.packet_sha256,
            "body_sha256": self.body_sha256,
            "expires_at": format_timestamp(self.expires_at),
            "status": self.status,
            "lease_token": self.lease_token,
            "lease_expires_at": (
                format_timestamp(self.lease_expires_at)
                if self.lease_expires_at is not None
                else None
            ),
            "deny_reason_code": self.deny_reason_code,
            "created_at": format_timestamp(self.created_at),
            "updated_at": format_timestamp(self.updated_at),
        }
        if self.receipt_evidence is not None:
            out["receipt_evidence"] = dict(self.receipt_evidence)
        if self.denial_evidence is not None:
            out["denial_evidence"] = dict(self.denial_evidence)
        if include_payload and self.envelope is not None:
            out["envelope"] = dict(self.envelope)
        if include_payload and self.pointer is not None:
            out["pointer"] = self.pointer.to_dict()
        return out


@dataclass(frozen=True)
class MailboxEvent:
    message_id: str
    event_type: str
    sender_authority_id: str
    recipient_authority_id: str
    status: str
    reason_code: str | None = None
    payload: Mapping[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        return {
            "message_id": self.message_id,
            "event_type": self.event_type,
            "sender_authority_id": self.sender_authority_id,
            "recipient_authority_id": self.recipient_authority_id,
            "status": self.status,
            "reason_code": self.reason_code,
            "payload": dict(self.payload),
            "created_at": format_timestamp(self.created_at),
        }


def message_is_expired(record: MailboxMessageRecord, *, now: datetime | None = None) -> bool:
    current = (now or datetime.now(UTC)).astimezone(UTC)
    return record.expires_at.astimezone(UTC) <= current


def _packet_sha256(envelope: Mapping[str, Any]) -> str:
    return "sha256:" + _sha256_b64url(_canonical_json_bytes(envelope))


def _prefixed_sha256(value: str) -> str:
    return value if value.startswith("sha256:") else "sha256:" + value


def _required_str(raw: Mapping[str, Any], *keys: str) -> str:
    return validation.required_str_among(raw, keys, exc_type=MailboxValidationError)


def _optional_str(raw: Mapping[str, Any], key: str) -> str | None:
    return validation.optional_str(raw, key, exc_type=MailboxValidationError)


def _required_int(raw: Mapping[str, Any], key: str) -> int:
    return validation.required_int(raw, key, exc_type=MailboxValidationError)


def _optional_mapping(raw: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    return validation.optional_mapping(raw, key, exc_type=MailboxValidationError)


def _parse_time(value: str, field: str) -> datetime:
    try:
        return parse_timestamp(value, field=field)
    except Exception as exc:  # noqa: BLE001 - normalize Poros errors here.
        raise MailboxValidationError(str(exc)) from exc


def format_timestamp(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    value = value.astimezone(UTC).replace(microsecond=0)
    return value.isoformat().replace("+00:00", "Z")


def parse_timestamp(value: str, *, field: str) -> datetime:
    if not isinstance(value, str):
        raise MailboxValidationError(f"{field} must be an RFC3339 timestamp")
    raw = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise MailboxValidationError(
            f"{field} must be an RFC3339 timestamp"
        ) from exc
    if parsed.tzinfo is None:
        raise MailboxValidationError(f"{field} must include a timezone")
    return parsed.astimezone(UTC)


def _canonical_json_bytes(value: Any) -> bytes:
    _validate_json_value(value)
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise MailboxValidationError(
            "value cannot be canonically encoded as Poros JSON"
        ) from exc


def canonical_mailbox_json_bytes(value: Any) -> bytes:
    """Return Poros-compatible canonical JSON bytes for mailbox accounting."""
    return _canonical_json_bytes(value)


def _sha256_b64url(data: bytes) -> str:
    digest = hashlib.sha256(data).digest()
    return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")


def _validate_json_value(value: Any, *, path: str = "$") -> None:
    if value is None or isinstance(value, bool):
        return
    if isinstance(value, int) and not isinstance(value, bool):
        if abs(value) > MAX_SAFE_INTEGER:
            raise MailboxValidationError(
                f"integer exceeds Poros safe JSON range at {path}"
            )
        return
    if isinstance(value, float):
        raise MailboxValidationError(
            f"floating point JSON numbers are not supported at {path}"
        )
    if isinstance(value, str):
        _validate_string(value, path=path)
        return
    if isinstance(value, Mapping):
        for key, child in value.items():
            if not isinstance(key, str):
                raise MailboxValidationError(f"JSON object keys must be strings at {path}")
            _validate_string(key, path=f"{path}.{key}")
            _validate_json_value(child, path=f"{path}.{key}")
        return
    if isinstance(value, Sequence) and not isinstance(value, bytes | bytearray):
        for index, child in enumerate(value):
            _validate_json_value(child, path=f"{path}[{index}]")
        return
    raise MailboxValidationError(
        f"unsupported JSON value type at {path}: {type(value).__name__}"
    )


def _validate_string(value: str, *, path: str) -> None:
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise MailboxValidationError(
            f"JSON string contains an invalid Unicode scalar at {path}"
        ) from exc
