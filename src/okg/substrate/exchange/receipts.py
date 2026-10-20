"""Signed Poros receipts."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from okg.substrate.exchange.constants import POROS_VERSION
from okg.substrate.exchange.envelope import default_algorithms, format_timestamp, new_message_id
from okg.substrate.exchange.signing import sign_receipt


RECEIPT_STATUSES = frozenset(
    {
        "accepted_for_admission",
        "rejected_preflight",
        "quarantined",
        "duplicate",
    }
)


def build_receipt(
    *,
    envelope: dict[str, Any],
    receiver: str,
    status: str,
    private_key: bytes | str,
    kid: str,
    reason_code: str | None = None,
    admission_id: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    if status not in RECEIPT_STATUSES:
        raise ValueError(f"unsupported Poros receipt status: {status}")
    receipt: dict[str, Any] = {
        "poros": POROS_VERSION,
        "type": "receipt",
        "message_id": new_message_id(),
        "in_reply_to": str(envelope.get("message_id", "")),
        "receiver": receiver,
        "sender": str(envelope.get("sender", "")),
        "received_at": format_timestamp(now or datetime.now(UTC)),
        "received_body_sha256": str(
            envelope.get("body_sha256") or envelope.get("ciphertext_sha256") or ""
        ),
        "status": status,
        "reason_code": reason_code,
        "admission_id": admission_id,
        "algorithms": default_algorithms(),
    }
    return sign_receipt(receipt, private_key, kid=kid)
