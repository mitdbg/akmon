"""Optional FastAPI surface for the Poros mailbox service."""

from __future__ import annotations

import json
from typing import Any, Mapping

from okg.substrate.federation import validation
from okg.substrate.federation.mailbox.auth import (
    MailboxAuthenticator,
    MailboxAuthError,
    deny_all_authenticator,
)
from okg.substrate.federation.mailbox.models import (
    MailboxMessageRecord,
    MailboxValidationError,
    parse_timestamp,
)
from okg.substrate.federation.mailbox.service import MailboxService

try:  # FastAPI resolves postponed route annotations from module globals.
    from fastapi import Request as FastAPIRequest
except Exception:  # pragma: no cover - optional extra.
    FastAPIRequest = Any


def build_mailbox_app(
    service: MailboxService,
    *,
    discovery_document: Mapping[str, Any] | Any | None = None,
    authenticator: MailboxAuthenticator | None = None,
) -> Any:
    try:
        from fastapi import FastAPI, HTTPException, Query, Response
    except Exception as exc:  # pragma: no cover - optional extra.
        raise RuntimeError(
            "FastAPI is required to serve Poros mailbox routes"
        ) from exc

    app = FastAPI(title="Poros Mailbox", version="1")

    # Read/finalize endpoints require an authenticated principal bound to the
    # message's recipient authority. With no authenticator configured the app
    # fails closed (every read/finalize is rejected) rather than serving any
    # caller's inbox anonymously (per enforce-mailbox-poros-authz).
    _authenticate = authenticator or deny_all_authenticator

    def _principal(request: Any) -> str:
        try:
            return _authenticate(request)
        except MailboxAuthError as exc:
            raise HTTPException(status_code=401, detail=str(exc)) from exc

    def _authorize_recipient(
        message_id: str, principal: str,
    ) -> MailboxMessageRecord:
        record = service.get_message(message_id)
        if record is None:
            raise HTTPException(status_code=404, detail="message not found")
        if record.recipient_authority_id != principal:
            raise HTTPException(
                status_code=403,
                detail="not authorized for this message's recipient",
            )
        return record

    @app.get("/healthz")
    def healthz() -> dict[str, Any]:
        return service.health()

    @app.get("/.well-known/okg-federation.json")
    def well_known() -> dict[str, Any]:
        if discovery_document is None:
            return {}
        if hasattr(discovery_document, "to_dict"):
            return discovery_document.to_dict()
        return dict(discovery_document)

    @app.post("/messages", status_code=202)
    async def submit_message(request: FastAPIRequest) -> dict[str, Any]:
        body = await request.json()
        if not isinstance(body, Mapping):
            raise HTTPException(status_code=400, detail="request body must be object")
        try:
            if isinstance(body.get("envelope"), Mapping):
                record = service.submit_envelope(body["envelope"])
            elif isinstance(body.get("pointer"), Mapping):
                record = service.submit_pointer(body["pointer"])
            else:
                raise MailboxValidationError(
                    "request body must include envelope or pointer"
                )
        except MailboxValidationError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return _record_to_dict(record, include_payload=False, include_lease=False)

    @app.post("/objects/{message_id}", status_code=202)
    async def submit_object_message(
        message_id: str,
        request: FastAPIRequest,
        sender_authority_id: str = Query(...),
        recipient_authority_id: str = Query(...),
        expires_at: str = Query(...),
        packet_sha256: str | None = Query(default=None),
        metadata_json: str | None = Query(default=None),
        encryption_json: str | None = Query(default=None),
    ) -> dict[str, Any]:
        try:
            record = service.submit_object_bytes(
                message_id=message_id,
                sender_authority_id=sender_authority_id,
                recipient_authority_id=recipient_authority_id,
                data=await request.body(),
                expires_at=parse_timestamp(expires_at, field="expires_at"),
                packet_sha256=packet_sha256,
                metadata=_json_object_param(metadata_json, "metadata_json"),
                encryption=_json_object_param(encryption_json, "encryption_json"),
            )
        except MailboxValidationError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return _record_to_dict(record, include_payload=False, include_lease=False)

    @app.get("/inbox/{recipient_authority:path}")
    def poll_inbox(
        recipient_authority: str,
        request: FastAPIRequest,
        limit: int = Query(default=10, ge=1, le=100),
        lease_seconds: int = Query(default=60, ge=1, le=3600),
    ) -> dict[str, Any]:
        principal = _principal(request)
        # A caller may only poll their own inbox.
        if principal != recipient_authority:
            raise HTTPException(
                status_code=403,
                detail="not authorized to poll this recipient's inbox",
            )
        try:
            records = service.poll(
                recipient_authority,
                limit=limit,
                lease_seconds=lease_seconds,
            )
        except MailboxValidationError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {
            "messages": [
                _record_to_dict(record, include_payload=True, include_lease=True)
                for record in records
            ]
        }

    @app.get("/messages/{message_id}")
    def get_message(message_id: str, request: FastAPIRequest) -> dict[str, Any]:
        principal = _principal(request)
        record = _authorize_recipient(message_id, principal)
        return _record_to_dict(record, include_payload=True, include_lease=False)

    @app.get("/messages/{message_id}/object")
    def get_message_object(message_id: str, request: FastAPIRequest) -> Response:
        principal = _principal(request)
        record = _authorize_recipient(message_id, principal)
        try:
            data = service.fetch_detached_bytes(record)
        except MailboxValidationError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return Response(content=data, media_type="application/octet-stream")

    @app.post("/messages/{message_id}/ack")
    async def ack_message(
        message_id: str,
        request: FastAPIRequest,
    ) -> dict[str, Any]:
        principal = _principal(request)
        _authorize_recipient(message_id, principal)
        body = await request.json()
        if not isinstance(body, Mapping):
            raise HTTPException(status_code=400, detail="request body must be object")
        try:
            record = service.ack_message(
                message_id,
                lease_token=_required_str(body, "lease_token"),
                receipt_evidence=_required_mapping(body, "receipt_evidence"),
                recipient_principal=principal,
            )
        except MailboxValidationError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return _record_to_dict(record, include_payload=False, include_lease=False)

    @app.post("/messages/{message_id}/deny")
    async def deny_message(
        message_id: str,
        request: FastAPIRequest,
    ) -> dict[str, Any]:
        principal = _principal(request)
        _authorize_recipient(message_id, principal)
        body = await request.json()
        if not isinstance(body, Mapping):
            raise HTTPException(status_code=400, detail="request body must be object")
        try:
            record = service.deny_message(
                message_id,
                lease_token=_required_str(body, "lease_token"),
                reason_code=_required_str(body, "reason_code"),
                denial_evidence=_required_mapping(body, "denial_evidence"),
                recipient_principal=principal,
            )
        except MailboxValidationError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return _record_to_dict(record, include_payload=False, include_lease=False)

    return app


def _record_to_dict(
    record: MailboxMessageRecord,
    *,
    include_payload: bool,
    include_lease: bool,
) -> dict[str, Any]:
    payload = record.to_dict(include_payload=include_payload)
    if not include_lease:
        payload.pop("lease_token", None)
        payload.pop("lease_expires_at", None)
    return payload


def _required_str(raw: Mapping[str, Any], key: str) -> str:
    return validation.required_str(
        raw,
        key,
        exc_type=MailboxValidationError,
        message=f"{key} is required",
    )


def _required_mapping(raw: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    return validation.required_nonempty_mapping(
        raw, key, exc_type=MailboxValidationError
    )


def _json_object_param(raw: str | None, field: str) -> dict[str, Any]:
    if raw is None or raw == "":
        return {}
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise MailboxValidationError(f"{field} must be valid JSON") from exc
    if not isinstance(value, Mapping):
        raise MailboxValidationError(f"{field} must be a JSON object")
    return dict(value)
