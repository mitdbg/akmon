"""Replay and expiry protection for Poros."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from okg.substrate.exchange.constants import (
    DEFAULT_MAX_CLOCK_SKEW_SECONDS,
    DEFAULT_MAX_EXPIRY_SECONDS,
)
from okg.substrate.exchange.envelope import parse_timestamp
from okg.substrate.exchange.errors import PorosDuplicateMessage, PorosReplayError


@dataclass
class InMemoryReplayCache:
    max_clock_skew: timedelta = timedelta(seconds=DEFAULT_MAX_CLOCK_SKEW_SECONDS)
    max_expiry_window: timedelta = timedelta(seconds=DEFAULT_MAX_EXPIRY_SECONDS)
    seen: dict[tuple[str, str], datetime] = field(default_factory=dict)

    def check_and_remember(
        self,
        envelope: dict[str, Any],
        *,
        now: datetime | None = None,
    ) -> None:
        current = (now or datetime.now(UTC)).astimezone(UTC)
        self._prune(current)
        sender = _string_field(envelope, "sender")
        message_id = _string_field(envelope, "message_id")
        created_at = parse_timestamp(str(envelope.get("created_at")), field="created_at")
        expires_at = parse_timestamp(str(envelope.get("expires_at")), field="expires_at")
        if created_at > current + self.max_clock_skew:
            raise PorosReplayError(
                "Poros message created_at is too far in the future",
                reason_code="created_at_in_future",
                data={"created_at": envelope.get("created_at")},
            )
        if expires_at <= current - self.max_clock_skew:
            raise PorosReplayError(
                "Poros message has expired",
                reason_code="expired_message",
                data={"expires_at": envelope.get("expires_at")},
            )
        if expires_at <= created_at:
            raise PorosReplayError(
                "Poros expires_at must be after created_at",
                reason_code="invalid_expiry_window",
            )
        if expires_at - created_at > self.max_expiry_window:
            raise PorosReplayError(
                "Poros expiry window exceeds receiver policy",
                reason_code="expiry_window_too_large",
            )
        key = (sender, message_id)
        if key in self.seen and self.seen[key] > current:
            raise PorosDuplicateMessage(
                "Poros message_id has already been seen for this sender",
                reason_code="duplicate",
                data={"sender": sender, "message_id": message_id},
            )
        self.seen[key] = expires_at + self.max_clock_skew

    def _prune(self, now: datetime) -> None:
        expired = [key for key, until in self.seen.items() if until <= now]
        for key in expired:
            self.seen.pop(key, None)


def _string_field(envelope: dict[str, Any], field: str) -> str:
    value = envelope.get(field)
    if not isinstance(value, str) or not value:
        raise PorosReplayError(
            f"Poros envelope missing {field}",
            reason_code="invalid_envelope",
            data={"field": field},
        )
    return value
