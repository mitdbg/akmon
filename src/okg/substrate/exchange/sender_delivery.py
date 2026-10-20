"""Sender-side Poros delivery worker helpers."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Callable

from okg.substrate.exchange.canonical_json import canonical_json_bytes
from okg.substrate.exchange.hashing import sha256_b64url
from okg.substrate.exchange.outbox import PorosOutboxError, record_delivery_attempt
from okg.substrate.exchange.signing import verify_receipt
from okg.substrate.library.sources._url_safety import (
    UnsafeFetchURL,
    assert_public_http_url,
)
from okg.substrate.db.connection import connect as db_connect


def _is_transient_transport_error(exc: BaseException) -> bool:
    """Whether a delivery exception is a transient transport failure worth a retry.

    Transient = the receiver may be momentarily unreachable (connection
    refused/reset, DNS resolution hiccup, read/connect timeout), so the same
    message redelivered later can still succeed. These are RETRYABLE.

    Non-transient errors (receipt verification failure, malformed receiver
    response, programming/value errors) are NOT covered here: redelivering
    them only re-incurs the identical failure, so they stay terminal. SSRF
    refusal (UnsafeFetchURL), nomos denial (PorosOutboxError) and
    rejected_preflight are classified terminal in their own dedicated
    branches and never reach this predicate.
    """
    # builtin transient network errors (the stdlib socket layer raises these,
    # and `requests` re-raises a bare TimeoutError in some timeout paths).
    if isinstance(exc, (TimeoutError, ConnectionError)):
        return True
    try:
        import requests
    except Exception:  # pragma: no cover - requests is always installed here
        return False
    return isinstance(
        exc,
        (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.ChunkedEncodingError,
        ),
    )


@dataclass(frozen=True)
class PorosOutboxWorkItem:
    message_id: str
    body_sha256: str
    envelope_sha256: str
    sender_authority_id: str
    recipient_authority_id: str
    sender_generation_id: str
    graph_id: str
    classification: str
    envelope: dict[str, Any]
    prior_attempt_count: int


@dataclass(frozen=True)
class PorosDeliveryResult:
    message_id: str
    status: str
    reason_code: str | None
    receipt_sha256: str | None
    attempt_id: int | None
    outbox_status: str
    attempt_count: int
    max_attempts: int
    next_retry_at: str | None
    terminal_failure_reason: str | None
    response: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "message_id": self.message_id,
            "status": self.status,
            "reason_code": self.reason_code,
            "receipt_sha256": self.receipt_sha256,
            "attempt_id": self.attempt_id,
            "outbox_status": self.outbox_status,
            "attempt_count": self.attempt_count,
            "max_attempts": self.max_attempts,
            "next_retry_at": self.next_retry_at,
            "terminal_failure_reason": self.terminal_failure_reason,
            "response": self.response,
        }


def deliver_next_outbox_messages(
    *,
    dsn: str,
    receiver_inbox_url: str,
    receiver_public_key: str,
    message_id: str | None = None,
    limit: int = 1,
    timeout_seconds: float = 10.0,
    stale_after_seconds: int = 300,
    max_attempts: int = 3,
    retry_delay_seconds: int = 60,
    post: Callable[..., Any] | None = None,
    allow_hosts: Iterable[str] | None = None,
) -> list[PorosDeliveryResult]:
    if limit <= 0:
        raise ValueError("limit must be positive")
    if max_attempts <= 0:
        raise ValueError("max_attempts must be positive")
    if retry_delay_seconds < 0:
        raise ValueError("retry_delay_seconds must be non-negative")
    items = claim_outbox_messages(
        dsn=dsn,
        message_id=message_id,
        limit=limit,
        stale_after_seconds=stale_after_seconds,
        max_attempts=max_attempts,
    )
    return [
        deliver_outbox_item(
            dsn=dsn,
            item=item,
            receiver_inbox_url=receiver_inbox_url,
            receiver_public_key=receiver_public_key,
            timeout_seconds=timeout_seconds,
            max_attempts=max_attempts,
            retry_delay_seconds=retry_delay_seconds,
            post=post,
            allow_hosts=allow_hosts,
        )
        for item in items
    ]


def claim_outbox_messages(
    *,
    dsn: str,
    message_id: str | None = None,
    limit: int = 1,
    stale_after_seconds: int = 300,
    max_attempts: int = 3,
) -> list[PorosOutboxWorkItem]:
    """Claim restart-safe outbox rows using row locks."""
    with db_connect(dsn) as conn, conn.cursor() as cur:
        if message_id is None:
            cur.execute(
                """
                WITH attempt_counts AS (
                    SELECT message_id, count(*) AS attempt_count
                      FROM okg.poros_delivery_attempts
                     GROUP BY message_id
                )
                SELECT o.message_id, o.body_sha256, o.envelope_sha256,
                       o.sender_authority_id, o.recipient_authority_id,
                       o.sender_generation_id, o.graph_id, o.classification,
                       o.envelope, COALESCE(a.attempt_count, 0)
                  FROM okg.poros_outbox_messages o
                  LEFT JOIN attempt_counts a ON a.message_id = o.message_id
                 WHERE COALESCE(a.attempt_count, 0) < %s
                   AND (
                        o.status IN ('created', 'failed')
                        OR (
                             o.status = 'retrying'
                             AND COALESCE(o.next_retry_at, now()) <= now()
                           )
                        OR (
                             o.status = 'delivering'
                             AND o.updated_at < now() - make_interval(secs => %s)
                           )
                       )
                 ORDER BY o.created_at
                 FOR UPDATE SKIP LOCKED
                 LIMIT %s
                """,
                (max_attempts, stale_after_seconds, limit),
            )
        else:
            cur.execute(
                """
                WITH attempt_counts AS (
                    SELECT message_id, count(*) AS attempt_count
                      FROM okg.poros_delivery_attempts
                     GROUP BY message_id
                )
                SELECT o.message_id, o.body_sha256, o.envelope_sha256,
                       o.sender_authority_id, o.recipient_authority_id,
                       o.sender_generation_id, o.graph_id, o.classification,
                       o.envelope, COALESCE(a.attempt_count, 0)
                  FROM okg.poros_outbox_messages o
                  LEFT JOIN attempt_counts a ON a.message_id = o.message_id
                 WHERE o.message_id = %s
                   AND COALESCE(a.attempt_count, 0) < %s
                   AND (
                        o.status IN ('created', 'failed')
                        OR (
                             o.status = 'retrying'
                             AND COALESCE(o.next_retry_at, now()) <= now()
                           )
                        OR (
                             o.status = 'delivering'
                             AND o.updated_at < now() - make_interval(secs => %s)
                           )
                       )
                 FOR UPDATE SKIP LOCKED
                """,
                (message_id, max_attempts, stale_after_seconds),
            )
        rows = cur.fetchall()
        ids = [row[0] for row in rows]
        if ids:
            cur.execute(
                """
                UPDATE okg.poros_outbox_messages
                   SET status = 'delivering',
                       max_delivery_attempts = %s,
                       updated_at = now()
                 WHERE message_id = ANY(%s)
                """,
                (max_attempts, ids),
            )
    return [
        PorosOutboxWorkItem(
            message_id=str(row[0]),
            body_sha256=str(row[1]),
            envelope_sha256=str(row[2]),
            sender_authority_id=str(row[3]),
            recipient_authority_id=str(row[4]),
            sender_generation_id=str(row[5]),
            graph_id=str(row[6]),
            classification=str(row[7] or "public"),
            envelope=row[8] if isinstance(row[8], dict) else {},
            prior_attempt_count=int(row[9] or 0),
        )
        for row in rows
    ]


def deliver_outbox_item(
    *,
    dsn: str,
    item: PorosOutboxWorkItem,
    receiver_inbox_url: str,
    receiver_public_key: str,
    timeout_seconds: float = 10.0,
    max_attempts: int = 3,
    retry_delay_seconds: int = 60,
    post: Callable[..., Any] | None = None,
    allow_hosts: Iterable[str] | None = None,
) -> PorosDeliveryResult:
    import requests

    response_payload: dict[str, Any]
    status: str
    reason_code: str | None = None
    receipt_sha256: str | None = None
    attempt_id: int | None = None
    outbox_status: str
    try:
        # SSRF egress guard: validate the (discovery/operator-provided) receiver
        # URL is a public http(s) URL before POSTing the signed envelope. A
        # loopback / private / link-local / cloud-metadata / non-http(s) target
        # is refused as a terminal delivery failure, never requested (per
        # centralize-egress-url-validation; reused by enforce-mailbox-poros-authz).
        assert_public_http_url(receiver_inbox_url, allow_hosts=allow_hosts)
        _authorize_poros_delivery_with_active_policy(
            dsn=dsn,
            item=item,
            receiver_inbox_url=receiver_inbox_url,
        )
        # Do NOT chase redirects: requests follows 3xx by default, which would
        # re-POST the signed envelope to a redirect-chosen origin (SSRF to a
        # private/metadata host) past the initial-URL check. Federation delivery
        # has exactly one legitimate target, so any 3xx is a terminal failure.
        http_post = post or requests.post
        response = http_post(
            receiver_inbox_url,
            data=canonical_json_bytes(item.envelope),
            headers={"content-type": "application/json"},
            timeout=timeout_seconds,
            allow_redirects=False,
        )
        if 300 <= response.status_code < 400:
            raise UnsafeFetchURL(
                "refusing to follow a redirect from the receiver inbox URL "
                f"{receiver_inbox_url!r} (status {response.status_code}, "
                f"Location={response.headers.get('Location')!r}): a delivery "
                "target must not redirect the signed envelope to another origin"
            )
        response_payload = response.json()
        status = str(response_payload.get("status") or "failed")
        reason_code = (
            str(response_payload["reason_code"])
            if response_payload.get("reason_code") is not None
            else None
        )
        receipt = response_payload.get("receipt")
        if not isinstance(receipt, dict):
            raise ValueError("receiver response did not include a signed receipt")
        verify_receipt(receipt, receiver_public_key)
        receipt_sha256 = sha256_b64url(canonical_json_bytes(receipt))
        if status in {"accepted_for_admission", "duplicate"}:
            outbox_status = "delivered"
        elif status == "rejected_preflight":
            outbox_status = "terminal_failed"
        else:
            outbox_status = "terminal_failed"
    except UnsafeFetchURL as exc:
        # Unsafe destination: record a terminal failure with a specific reason
        # and DO NOT perform the request.
        response_payload = {
            "status": "failed",
            "error": str(exc),
            "receiver_inbox_url": receiver_inbox_url,
        }
        status = "failed"
        reason_code = "unsafe_delivery_url"
        outbox_status = "terminal_failed"
    except PorosOutboxError as exc:
        response_payload = {
            "status": "failed",
            "error": str(exc),
            "receiver_inbox_url": receiver_inbox_url,
        }
        status = "failed"
        reason_code = "nomos_denied"
        outbox_status = "terminal_failed"
    except Exception as exc:  # noqa: BLE001 - durable delivery result surface
        response_payload = {
            "status": "failed",
            "error": str(exc),
            "receiver_inbox_url": receiver_inbox_url,
        }
        status = "failed"
        reason_code = "delivery_failed"
        # Transient transport failures (connection refused/reset, DNS hiccup,
        # read/connect timeout) are not a durable verdict on the message: the
        # receiver may be momentarily unreachable. Classify them as a RETRYABLE
        # outbox status so resolve_outbox_after_attempt re-queues the message
        # while attempt_count < max_attempts, then terminalizes once the bound
        # is exhausted. Non-transient errors (verification failures, malformed
        # receipts, programming errors) stay terminal -- retrying them only
        # re-incurs the same failure. UnsafeFetchURL / PorosOutboxError
        # (SSRF refusal, nomos denial) and rejected_preflight are handled in
        # their own branches above/below and remain terminal.
        if _is_transient_transport_error(exc):
            outbox_status = "transient_failed"
        else:
            outbox_status = "terminal_failed"

    attempt = record_delivery_attempt(
        dsn=dsn,
        message_id=item.message_id,
        body_sha256=item.body_sha256,
        envelope_sha256=item.envelope_sha256,
        status=status,
        reason_code=reason_code,
        receipt_sha256=receipt_sha256,
        response=response_payload,
        transport="http",
    )
    attempt_id = attempt.attempt_id
    resolution = resolve_outbox_after_attempt(
        dsn=dsn,
        message_id=item.message_id,
        attempted_status=status,
        desired_status=outbox_status,
        reason_code=reason_code,
        response=response_payload,
        max_attempts=max_attempts,
        retry_delay_seconds=retry_delay_seconds,
    )
    return PorosDeliveryResult(
        message_id=item.message_id,
        status=status,
        reason_code=reason_code,
        receipt_sha256=receipt_sha256,
        attempt_id=attempt_id,
        outbox_status=str(resolution["status"]),
        attempt_count=int(resolution["attempt_count"]),
        max_attempts=int(resolution["max_attempts"]),
        next_retry_at=resolution["next_retry_at"],
        terminal_failure_reason=resolution["terminal_failure_reason"],
        response=response_payload,
    )


def _authorize_poros_delivery_with_active_policy(
    *,
    dsn: str,
    item: PorosOutboxWorkItem,
    receiver_inbox_url: str,
) -> None:
    deployment_name = _deployment_from_generation_id(item.sender_generation_id)
    if not deployment_name:
        return
    from okg.substrate.nomos import (
        NomosRuntimeRequest,
        deployment_requires_nomos_runtime,
        evaluate_runtime_boundary_with_active_policy,
    )

    if not deployment_requires_nomos_runtime(deployment_name):
        return
    body = item.envelope.get("body")
    if not isinstance(body, dict):
        body = {}
    scope = f"deployment:{deployment_name}"
    with db_connect(dsn) as conn:
        outcome = evaluate_runtime_boundary_with_active_policy(
            conn,
            deployment_name=deployment_name,
            scope=scope,
            request=NomosRuntimeRequest(
                surface="graph.export",
                subject=item.sender_authority_id,
                classification=item.classification,
                resource={
                    "use": "poros.deliver",
                    "message_id": item.message_id,
                    "sender_generation_id": item.sender_generation_id,
                    "graph_id": item.graph_id,
                    "recipient_authority_id": item.recipient_authority_id,
                    "receiver_inbox_url": receiver_inbox_url,
                    "payload_mode": body.get("payload_mode"),
                },
            ),
            target={
                "kind": "poros_delivery",
                "message_id": item.message_id,
                "graph_id": item.graph_id,
                "recipient_authority_id": item.recipient_authority_id,
            },
            performed_obligations=("audit",),
        )
        conn.commit()
    if outcome is not None and not outcome.allowed:
        raise PorosOutboxError(
            "Nomos denied graph.export: "
            f"{outcome.decision.reason_code or outcome.final_effect}"
        )


def _deployment_from_generation_id(generation_id: str) -> str | None:
    parts = str(generation_id).split(":", 3)
    if len(parts) >= 2 and parts[0] == "okg" and parts[1]:
        return parts[1]
    return None


def resolve_outbox_after_attempt(
    *,
    dsn: str,
    message_id: str,
    attempted_status: str,
    desired_status: str,
    reason_code: str | None,
    response: dict[str, Any],
    max_attempts: int,
    retry_delay_seconds: int,
) -> dict[str, Any]:
    if desired_status == "delivered":
        final_status = "delivered"
        terminal_failure_reason = None
        last_error = None
        retry = False
    elif attempted_status == "rejected_preflight":
        final_status = "terminal_failed"
        terminal_failure_reason = reason_code or "rejected_preflight"
        last_error = terminal_failure_reason
        retry = False
    elif desired_status == "transient_failed":
        # Transient transport failure: retry while the attempt bound is not
        # exhausted (the attempt_count < max_attempts check below promotes
        # final_status to 'retrying'); once exhausted it terminalizes here.
        final_status = "terminal_failed"
        terminal_failure_reason = reason_code or str(
            response.get("error") or "delivery_failed"
        )
        last_error = terminal_failure_reason
        retry = True
    elif desired_status == "terminal_failed":
        final_status = "terminal_failed"
        terminal_failure_reason = reason_code or str(
            response.get("error") or "delivery_failed"
        )
        last_error = terminal_failure_reason
        retry = False
    else:
        final_status = "terminal_failed"
        terminal_failure_reason = reason_code or str(
            response.get("error") or "delivery_failed"
        )
        last_error = terminal_failure_reason
        retry = True
    with db_connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*)
              FROM okg.poros_delivery_attempts
             WHERE message_id = %s
            """,
            (message_id,),
        )
        attempt_count = int(cur.fetchone()[0] or 0)
        if retry and attempt_count < max_attempts:
            final_status = "retrying"
            terminal_failure_reason = None
        cur.execute(
            """
            UPDATE okg.poros_outbox_messages
               SET status = %s,
                   max_delivery_attempts = %s,
                   next_retry_at = (
                     CASE
                       WHEN %s = 'retrying'
                       THEN now() + make_interval(secs => %s)
                       ELSE NULL
                     END
                   ),
                   last_error = %s,
                   terminal_failure_reason = %s,
                   updated_at = now()
             WHERE message_id = %s
             RETURNING status, max_delivery_attempts, next_retry_at,
                       last_error, terminal_failure_reason
            """,
            (
                final_status,
                max_attempts,
                final_status,
                retry_delay_seconds,
                last_error,
                terminal_failure_reason,
                message_id,
            ),
        )
        row = cur.fetchone()
    if row is None:
        raise ValueError(f"Poros outbox message not found: {message_id!r}")
    return {
        "status": row[0],
        "max_attempts": int(row[1]),
        "attempt_count": attempt_count,
        "next_retry_at": row[2].isoformat() if row[2] else None,
        "last_error": row[3],
        "terminal_failure_reason": row[4],
    }


def update_outbox_status(*, dsn: str, message_id: str, status: str) -> None:
    if status not in {
        "created",
        "delivering",
        "delivered",
        "acknowledged",
        "failed",
        "retrying",
        "terminal_failed",
    }:
        raise ValueError(f"unsupported outbox status: {status}")
    with db_connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE okg.poros_outbox_messages
               SET status = %s,
                   next_retry_at = NULL,
                   updated_at = now()
             WHERE message_id = %s
            """,
            (status, message_id),
        )
