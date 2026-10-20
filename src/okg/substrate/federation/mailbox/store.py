"""Mailbox metadata stores for Poros federation transport."""

from __future__ import annotations

import secrets
import threading
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Mapping, Protocol

if TYPE_CHECKING:
    import psycopg

from okg.substrate.federation.mailbox.models import (
    MAILBOX_STATUS_ACKNOWLEDGED,
    MAILBOX_STATUS_DENIED,
    MAILBOX_STATUS_LEASED,
    MAILBOX_STATUS_PENDING,
    DetachedObjectPointer,
    MailboxEvent,
    MailboxMessageRecord,
    MailboxValidationError,
    message_is_expired,
)


MAILBOX_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS okg.federation_mailbox_messages (
  message_id              TEXT PRIMARY KEY,
  sender_authority_id     TEXT NOT NULL,
  recipient_authority_id  TEXT NOT NULL,
  payload_mode            TEXT NOT NULL
    CHECK (payload_mode IN ('inline', 'detached_pointer')),
  packet_sha256           TEXT NOT NULL,
  body_sha256             TEXT,
  envelope                JSONB,
  pointer                 JSONB,
  object_uri              TEXT,
  object_sha256           TEXT,
  size_bytes              BIGINT CHECK (size_bytes IS NULL OR size_bytes >= 0),
  expires_at              TIMESTAMPTZ NOT NULL,
  status                  TEXT NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'leased', 'acknowledged', 'denied')),
  lease_token             TEXT,
  lease_expires_at        TIMESTAMPTZ,
  receipt_evidence        JSONB,
  denial_evidence         JSONB,
  deny_reason_code        TEXT,
  created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE okg.federation_mailbox_messages
  DROP CONSTRAINT IF EXISTS federation_mailbox_payload_shape_chk;
ALTER TABLE okg.federation_mailbox_messages
  ADD CONSTRAINT federation_mailbox_payload_shape_chk
    CHECK (
      (
        payload_mode = 'inline'
        AND envelope IS NOT NULL
        AND pointer IS NULL
        AND object_uri IS NULL
        AND object_sha256 IS NULL
        AND size_bytes IS NULL
      )
      OR
      (
        payload_mode = 'detached_pointer'
        AND envelope IS NULL
        AND pointer IS NOT NULL
        AND NULLIF(btrim(object_uri), '') IS NOT NULL
        AND NULLIF(btrim(object_sha256), '') IS NOT NULL
        AND size_bytes IS NOT NULL
        AND size_bytes > 0
      )
    ) NOT VALID;
ALTER TABLE okg.federation_mailbox_messages
  DROP CONSTRAINT IF EXISTS federation_mailbox_lifecycle_evidence_chk;
ALTER TABLE okg.federation_mailbox_messages
  ADD CONSTRAINT federation_mailbox_lifecycle_evidence_chk
    CHECK (
      (
        status = 'pending'
        AND receipt_evidence IS NULL
        AND denial_evidence IS NULL
        AND deny_reason_code IS NULL
      )
      OR
      (
        status = 'leased'
        AND lease_token IS NOT NULL
        AND lease_expires_at IS NOT NULL
        AND receipt_evidence IS NULL
        AND denial_evidence IS NULL
        AND deny_reason_code IS NULL
      )
      OR
      (
        status = 'acknowledged'
        AND receipt_evidence IS NOT NULL
        AND denial_evidence IS NULL
        AND deny_reason_code IS NULL
      )
      OR
      (
        status = 'denied'
        AND receipt_evidence IS NULL
        AND denial_evidence IS NOT NULL
        AND NULLIF(btrim(deny_reason_code), '') IS NOT NULL
      )
    ) NOT VALID;

CREATE INDEX IF NOT EXISTS federation_mailbox_recipient_status_idx
  ON okg.federation_mailbox_messages (
    recipient_authority_id, status, expires_at, created_at
  );
CREATE INDEX IF NOT EXISTS federation_mailbox_sender_idx
  ON okg.federation_mailbox_messages (
    sender_authority_id, created_at DESC
  );
CREATE INDEX IF NOT EXISTS federation_mailbox_packet_hash_idx
  ON okg.federation_mailbox_messages (packet_sha256);
CREATE INDEX IF NOT EXISTS federation_mailbox_lease_idx
  ON okg.federation_mailbox_messages (
    status, lease_expires_at
  ) WHERE status = 'leased';

DO $federation_mailbox_lease_shape$
BEGIN
  IF NOT EXISTS (
    SELECT 1
      FROM pg_constraint
     WHERE conrelid = 'okg.federation_mailbox_messages'::regclass
       AND conname = 'federation_mailbox_lease_shape_chk'
  ) THEN
    ALTER TABLE okg.federation_mailbox_messages
      ADD CONSTRAINT federation_mailbox_lease_shape_chk
      CHECK (
        status <> 'leased'
        OR (
          lease_token IS NOT NULL
          AND length(lease_token) > 0
          AND lease_expires_at IS NOT NULL
        )
      ) NOT VALID;
  END IF;
END
$federation_mailbox_lease_shape$;

CREATE TABLE IF NOT EXISTS okg.federation_mailbox_events (
  event_id                BIGSERIAL PRIMARY KEY,
  message_id              TEXT NOT NULL
    REFERENCES okg.federation_mailbox_messages(message_id) ON DELETE CASCADE,
  event_type              TEXT NOT NULL,
  sender_authority_id     TEXT NOT NULL,
  recipient_authority_id  TEXT NOT NULL,
  status                  TEXT NOT NULL,
  reason_code             TEXT,
  payload                 JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS federation_mailbox_events_message_idx
  ON okg.federation_mailbox_events (message_id, created_at DESC);
CREATE INDEX IF NOT EXISTS federation_mailbox_events_recipient_idx
  ON okg.federation_mailbox_events (
    recipient_authority_id, created_at DESC
  );
"""

_SELECT_MESSAGE_COLUMNS = """
message_id,
sender_authority_id,
recipient_authority_id,
payload_mode,
packet_sha256,
body_sha256,
expires_at,
status,
envelope,
pointer,
lease_token,
lease_expires_at,
receipt_evidence,
denial_evidence,
deny_reason_code,
created_at,
updated_at
"""


class MailboxStore(Protocol):
    def submit(self, record: MailboxMessageRecord) -> MailboxMessageRecord:
        """Store a pending mailbox record or return an idempotent duplicate."""

    def claim_pending(
        self,
        recipient_authority_id: str,
        *,
        limit: int = 10,
        lease_seconds: int = 60,
    ) -> list[MailboxMessageRecord]:
        """Lease pending records for a polling receiver."""

    def get(self, message_id: str) -> MailboxMessageRecord | None:
        """Return one mailbox record by message id."""

    def ack(
        self,
        message_id: str,
        *,
        lease_token: str,
        receipt_evidence: Mapping[str, Any],
        recipient_principal: str | None = None,
    ) -> MailboxMessageRecord:
        """Mark a leased message acknowledged after durable local receipt."""

    def deny(
        self,
        message_id: str,
        *,
        lease_token: str,
        reason_code: str,
        denial_evidence: Mapping[str, Any],
        recipient_principal: str | None = None,
    ) -> MailboxMessageRecord:
        """Mark a leased message denied with durable local evidence."""

    def events(self, message_id: str | None = None) -> list[MailboxEvent]:
        """Return mailbox transport events."""


class InMemoryMailboxStore:
    """Mailbox store for tests and local demos."""

    def __init__(self) -> None:
        self._records: dict[str, MailboxMessageRecord] = {}
        self._events: list[MailboxEvent] = []
        self._lock = threading.RLock()

    def submit(self, record: MailboxMessageRecord) -> MailboxMessageRecord:
        with self._lock:
            existing = self._records.get(record.message_id)
            if existing is not None:
                _assert_idempotent_duplicate(existing, record)
                return existing
            now = datetime.now(UTC)
            stored = replace(record, created_at=now, updated_at=now)
            self._records[stored.message_id] = stored
            self._append_event(stored, event_type="submitted")
            return stored

    def claim_pending(
        self,
        recipient_authority_id: str,
        *,
        limit: int = 10,
        lease_seconds: int = 60,
    ) -> list[MailboxMessageRecord]:
        _validate_positive(limit, "limit")
        _validate_positive(lease_seconds, "lease_seconds")
        with self._lock:
            now = datetime.now(UTC)
            leased: list[MailboxMessageRecord] = []
            for record in sorted(self._records.values(), key=lambda r: r.created_at):
                if len(leased) >= limit:
                    break
                if record.recipient_authority_id != recipient_authority_id:
                    continue
                if message_is_expired(record, now=now):
                    continue
                if not _claimable(record, now=now):
                    continue
                lease_token = secrets.token_urlsafe(32)
                updated = replace(
                    record,
                    status=MAILBOX_STATUS_LEASED,
                    lease_token=lease_token,
                    lease_expires_at=now + timedelta(seconds=lease_seconds),
                    updated_at=now,
                )
                self._records[updated.message_id] = updated
                self._append_event(
                    updated,
                    event_type="claimed",
                    payload={"lease_seconds": lease_seconds},
                )
                leased.append(updated)
            return leased

    def get(self, message_id: str) -> MailboxMessageRecord | None:
        with self._lock:
            return self._records.get(message_id)

    def ack(
        self,
        message_id: str,
        *,
        lease_token: str,
        receipt_evidence: Mapping[str, Any],
        recipient_principal: str | None = None,
    ) -> MailboxMessageRecord:
        evidence = _required_mapping(receipt_evidence, "receipt_evidence")
        with self._lock:
            record = self._require_record(message_id)
            _assert_active_lease(
                record,
                lease_token=lease_token,
                recipient_principal=recipient_principal,
            )
            updated = replace(
                record,
                status=MAILBOX_STATUS_ACKNOWLEDGED,
                receipt_evidence=evidence,
                updated_at=datetime.now(UTC),
            )
            self._records[message_id] = updated
            self._append_event(
                updated,
                event_type="acknowledged",
                payload={"receipt_evidence": evidence},
            )
            return updated

    def deny(
        self,
        message_id: str,
        *,
        lease_token: str,
        reason_code: str,
        denial_evidence: Mapping[str, Any],
        recipient_principal: str | None = None,
    ) -> MailboxMessageRecord:
        if not reason_code:
            raise MailboxValidationError("deny reason_code is required")
        evidence = _required_mapping(denial_evidence, "denial_evidence")
        with self._lock:
            record = self._require_record(message_id)
            _assert_active_lease(
                record,
                lease_token=lease_token,
                recipient_principal=recipient_principal,
            )
            updated = replace(
                record,
                status=MAILBOX_STATUS_DENIED,
                denial_evidence=evidence,
                deny_reason_code=reason_code,
                updated_at=datetime.now(UTC),
            )
            self._records[message_id] = updated
            self._append_event(
                updated,
                event_type="denied",
                reason_code=reason_code,
                payload={"denial_evidence": evidence},
            )
            return updated

    def events(self, message_id: str | None = None) -> list[MailboxEvent]:
        with self._lock:
            if message_id is None:
                return list(self._events)
            return [event for event in self._events if event.message_id == message_id]

    def _require_record(self, message_id: str) -> MailboxMessageRecord:
        record = self._records.get(message_id)
        if record is None:
            raise MailboxValidationError(f"unknown mailbox message: {message_id}")
        return record

    def _append_event(
        self,
        record: MailboxMessageRecord,
        *,
        event_type: str,
        reason_code: str | None = None,
        payload: Mapping[str, Any] | None = None,
    ) -> None:
        self._events.append(_event(record, event_type, reason_code, payload))


class PostgresMailboxStore:
    """Postgres-backed mailbox metadata store.

    The object payload remains in the configured object store. Postgres stores
    metadata, leases, idempotency keys, and durable receipt/denial evidence.
    """

    def __init__(self, conn: psycopg.Connection[Any]) -> None:
        self.conn = conn

    @classmethod
    def install_schema(cls, conn: psycopg.Connection[Any]) -> None:
        install_mailbox_schema(conn)

    def submit(self, record: MailboxMessageRecord) -> MailboxMessageRecord:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT packet_sha256
                  FROM okg.federation_mailbox_messages
                 WHERE message_id = %s
                """,
                (record.message_id,),
            )
            existing = cur.fetchone()
            if existing is not None:
                current = self.get(record.message_id)
                if current is None:
                    raise MailboxValidationError(
                        "mailbox duplicate disappeared during submit"
                    )
                _assert_idempotent_duplicate(current, record)
                return current
            ptr = record.pointer.to_dict() if record.pointer is not None else None
            cur.execute(
                f"""
                INSERT INTO okg.federation_mailbox_messages (
                    message_id, sender_authority_id, recipient_authority_id,
                    payload_mode, packet_sha256, body_sha256, envelope,
                    pointer, object_uri, object_sha256, size_bytes,
                    expires_at, status, created_at, updated_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s
                )
                RETURNING {_SELECT_MESSAGE_COLUMNS}
                """,
                (
                    record.message_id,
                    record.sender_authority_id,
                    record.recipient_authority_id,
                    record.payload_mode,
                    record.packet_sha256,
                    record.body_sha256,
                    _jsonb(dict(record.envelope))
                    if record.envelope is not None
                    else None,
                    _jsonb(ptr) if ptr is not None else None,
                    record.pointer.object_uri if record.pointer is not None else None,
                    record.pointer.object_sha256 if record.pointer is not None else None,
                    record.pointer.size_bytes if record.pointer is not None else None,
                    record.expires_at,
                    MAILBOX_STATUS_PENDING,
                    record.created_at,
                    record.updated_at,
                ),
            )
            stored = _record_from_row(cur.fetchone())
            self._append_event(cur, stored, event_type="submitted")
            return stored

    def claim_pending(
        self,
        recipient_authority_id: str,
        *,
        limit: int = 10,
        lease_seconds: int = 60,
    ) -> list[MailboxMessageRecord]:
        _validate_positive(limit, "limit")
        _validate_positive(lease_seconds, "lease_seconds")
        now = datetime.now(UTC)
        lease_expires_at = now + timedelta(seconds=lease_seconds)
        leased: list[MailboxMessageRecord] = []
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT {_SELECT_MESSAGE_COLUMNS}
                  FROM okg.federation_mailbox_messages
                 WHERE recipient_authority_id = %s
                   AND expires_at > %s
                   AND (
                        status = 'pending'
                        OR (
                            status = 'leased'
                            AND lease_expires_at IS NOT NULL
                            AND lease_expires_at <= %s
                        )
                   )
                 ORDER BY created_at ASC
                 LIMIT %s
                 FOR UPDATE SKIP LOCKED
                """,
                (recipient_authority_id, now, now, limit),
            )
            rows = cur.fetchall()
            for row in rows:
                current = _record_from_row(row)
                lease_token = secrets.token_urlsafe(32)
                cur.execute(
                    f"""
                    UPDATE okg.federation_mailbox_messages
                       SET status = %s,
                           lease_token = %s,
                           lease_expires_at = %s,
                           updated_at = %s
                     WHERE message_id = %s
                     RETURNING {_SELECT_MESSAGE_COLUMNS}
                    """,
                    (
                        MAILBOX_STATUS_LEASED,
                        lease_token,
                        lease_expires_at,
                        now,
                        current.message_id,
                    ),
                )
                updated = _record_from_row(cur.fetchone())
                self._append_event(
                    cur,
                    updated,
                    event_type="claimed",
                    payload={"lease_seconds": lease_seconds},
                )
                leased.append(updated)
        return leased

    def get(self, message_id: str) -> MailboxMessageRecord | None:
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT {_SELECT_MESSAGE_COLUMNS}
                  FROM okg.federation_mailbox_messages
                 WHERE message_id = %s
                """,
                (message_id,),
            )
            row = cur.fetchone()
        return _record_from_row(row) if row is not None else None

    def ack(
        self,
        message_id: str,
        *,
        lease_token: str,
        receipt_evidence: Mapping[str, Any],
        recipient_principal: str | None = None,
    ) -> MailboxMessageRecord:
        evidence = _required_mapping(receipt_evidence, "receipt_evidence")
        with self.conn.cursor() as cur:
            record = self._require_locked(cur, message_id)
            _assert_active_lease(
                record,
                lease_token=lease_token,
                recipient_principal=recipient_principal,
            )
            now = datetime.now(UTC)
            cur.execute(
                f"""
                UPDATE okg.federation_mailbox_messages
                   SET status = %s,
                       receipt_evidence = %s,
                       updated_at = %s
                 WHERE message_id = %s
                 RETURNING {_SELECT_MESSAGE_COLUMNS}
                """,
                (
                    MAILBOX_STATUS_ACKNOWLEDGED,
                    _jsonb(evidence),
                    now,
                    message_id,
                ),
            )
            updated = _record_from_row(cur.fetchone())
            self._append_event(
                cur,
                updated,
                event_type="acknowledged",
                payload={"receipt_evidence": evidence},
            )
            return updated

    def deny(
        self,
        message_id: str,
        *,
        lease_token: str,
        reason_code: str,
        denial_evidence: Mapping[str, Any],
        recipient_principal: str | None = None,
    ) -> MailboxMessageRecord:
        if not reason_code:
            raise MailboxValidationError("deny reason_code is required")
        evidence = _required_mapping(denial_evidence, "denial_evidence")
        with self.conn.cursor() as cur:
            record = self._require_locked(cur, message_id)
            _assert_active_lease(
                record,
                lease_token=lease_token,
                recipient_principal=recipient_principal,
            )
            now = datetime.now(UTC)
            cur.execute(
                f"""
                UPDATE okg.federation_mailbox_messages
                   SET status = %s,
                       denial_evidence = %s,
                       deny_reason_code = %s,
                       updated_at = %s
                 WHERE message_id = %s
                 RETURNING {_SELECT_MESSAGE_COLUMNS}
                """,
                (
                    MAILBOX_STATUS_DENIED,
                    _jsonb(evidence),
                    reason_code,
                    now,
                    message_id,
                ),
            )
            updated = _record_from_row(cur.fetchone())
            self._append_event(
                cur,
                updated,
                event_type="denied",
                reason_code=reason_code,
                payload={"denial_evidence": evidence},
            )
            return updated

    def events(self, message_id: str | None = None) -> list[MailboxEvent]:
        params: tuple[Any, ...] = ()
        where = ""
        if message_id is not None:
            where = "WHERE message_id = %s"
            params = (message_id,)
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT message_id, event_type, sender_authority_id,
                       recipient_authority_id, status, reason_code,
                       payload, created_at
                  FROM okg.federation_mailbox_events
                  {where}
                 ORDER BY created_at ASC, event_id ASC
                """,
                params,
            )
            rows = cur.fetchall()
        return [_event_from_row(row) for row in rows]

    def _require_locked(
        self,
        cur: psycopg.Cursor[Any],
        message_id: str,
    ) -> MailboxMessageRecord:
        cur.execute(
            f"""
            SELECT {_SELECT_MESSAGE_COLUMNS}
              FROM okg.federation_mailbox_messages
             WHERE message_id = %s
             FOR UPDATE
            """,
            (message_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise MailboxValidationError(f"unknown mailbox message: {message_id}")
        return _record_from_row(row)

    def _append_event(
        self,
        cur: psycopg.Cursor[Any],
        record: MailboxMessageRecord,
        *,
        event_type: str,
        reason_code: str | None = None,
        payload: Mapping[str, Any] | None = None,
    ) -> None:
        event = _event(record, event_type, reason_code, payload)
        cur.execute(
            """
            INSERT INTO okg.federation_mailbox_events (
                message_id, event_type, sender_authority_id,
                recipient_authority_id, status, reason_code, payload,
                created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                event.message_id,
                event.event_type,
                event.sender_authority_id,
                event.recipient_authority_id,
                event.status,
                event.reason_code,
                _jsonb(dict(event.payload)),
                event.created_at,
            ),
        )


def install_mailbox_schema(conn: psycopg.Connection[Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(MAILBOX_SCHEMA_SQL)


def _jsonb(value: Any) -> Any:
    try:
        from psycopg.types.json import Jsonb
    except ImportError as exc:  # pragma: no cover - exercised in import test.
        raise RuntimeError(
            "psycopg is required for PostgresMailboxStore; install okg[mailbox]"
        ) from exc
    return Jsonb(value)


def _assert_idempotent_duplicate(
    existing: MailboxMessageRecord,
    incoming: MailboxMessageRecord,
) -> None:
    if existing.packet_sha256 != incoming.packet_sha256:
        raise MailboxValidationError(
            "message_id already exists with a different packet hash"
        )


def _claimable(record: MailboxMessageRecord, *, now: datetime) -> bool:
    if record.status == MAILBOX_STATUS_PENDING:
        return True
    if record.status != MAILBOX_STATUS_LEASED:
        return False
    return (
        record.lease_expires_at is not None
        and record.lease_expires_at.astimezone(UTC) <= now
    )


def _assert_active_lease(
    record: MailboxMessageRecord,
    *,
    lease_token: str,
    recipient_principal: str | None = None,
) -> None:
    # Bind finalize to the authenticated recipient principal: a leaked lease
    # token alone (presented by another principal) MUST NOT finalize the
    # message. In-process trusted callers pass recipient_principal=None; the
    # HTTP surface always passes the authenticated principal.
    if (
        recipient_principal is not None
        and record.recipient_authority_id != recipient_principal
    ):
        raise MailboxValidationError(
            "mailbox lease is not bound to this recipient principal"
        )
    if record.status in {MAILBOX_STATUS_ACKNOWLEDGED, MAILBOX_STATUS_DENIED}:
        raise MailboxValidationError("mailbox message is already final")
    if record.status != MAILBOX_STATUS_LEASED:
        raise MailboxValidationError("mailbox message must be leased first")
    if not lease_token or record.lease_token != lease_token:
        raise MailboxValidationError("mailbox lease token does not match")
    if record.lease_expires_at is None:
        raise MailboxValidationError("mailbox lease is missing expiry")
    if record.lease_expires_at.astimezone(UTC) <= datetime.now(UTC):
        raise MailboxValidationError("mailbox lease has expired")


def _required_mapping(
    value: Mapping[str, Any],
    field: str,
) -> dict[str, Any]:
    if not isinstance(value, Mapping) or not value:
        raise MailboxValidationError(f"{field} must be a non-empty object")
    return dict(value)


def _validate_positive(value: int, field: str) -> None:
    if not isinstance(value, int) or value <= 0:
        raise MailboxValidationError(f"{field} must be positive")


def _event(
    record: MailboxMessageRecord,
    event_type: str,
    reason_code: str | None = None,
    payload: Mapping[str, Any] | None = None,
) -> MailboxEvent:
    return MailboxEvent(
        message_id=record.message_id,
        event_type=event_type,
        sender_authority_id=record.sender_authority_id,
        recipient_authority_id=record.recipient_authority_id,
        status=record.status,
        reason_code=reason_code,
        payload=dict(payload or {}),
    )


def _record_from_row(row: tuple[Any, ...]) -> MailboxMessageRecord:
    pointer_raw = row[9]
    pointer = (
        DetachedObjectPointer.from_mapping(pointer_raw)
        if isinstance(pointer_raw, Mapping)
        else None
    )
    envelope = dict(row[8]) if isinstance(row[8], Mapping) else None
    receipt_evidence = dict(row[12]) if isinstance(row[12], Mapping) else None
    denial_evidence = dict(row[13]) if isinstance(row[13], Mapping) else None
    return MailboxMessageRecord(
        message_id=row[0],
        sender_authority_id=row[1],
        recipient_authority_id=row[2],
        payload_mode=row[3],
        packet_sha256=row[4],
        body_sha256=row[5],
        expires_at=row[6],
        status=row[7],
        envelope=envelope,
        pointer=pointer,
        lease_token=row[10],
        lease_expires_at=row[11],
        receipt_evidence=receipt_evidence,
        denial_evidence=denial_evidence,
        deny_reason_code=row[14],
        created_at=row[15],
        updated_at=row[16],
    )


def _event_from_row(row: tuple[Any, ...]) -> MailboxEvent:
    payload = dict(row[6]) if isinstance(row[6], Mapping) else {}
    return MailboxEvent(
        message_id=row[0],
        event_type=row[1],
        sender_authority_id=row[2],
        recipient_authority_id=row[3],
        status=row[4],
        reason_code=row[5],
        payload=payload,
        created_at=row[7],
    )
