"""Optional FastAPI router for Poros discovery and inbox endpoints."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from fastapi import APIRouter

try:  # FastAPI resolves future annotations from module globals.
    from fastapi import Request as FastAPIRequest
except Exception:  # pragma: no cover - optional viz extra.
    FastAPIRequest = Any

from okg.substrate.exchange.authority import AuthorityDocument
from okg.substrate.exchange.inbox import PorosInbox
from okg.substrate.exchange.receiver import submit_poros_envelope_bytes


def build_poros_router(
    *,
    authority_document: AuthorityDocument,
    inbox: PorosInbox | None = None,
    trust_pins: dict[str, Any] | list[dict[str, Any]] | None = None,
    receiver_keys: dict[str, Any] | None = None,
    receiver: str | None = None,
    dsn: str | None = None,
    sender_dsn: str | None = None,
    deployment: str = "default",
    persist_preflight: bool = False,
    apply: bool = False,
    publish: bool = False,
    nomos_bundle: Path | str | None = None,
    object_store_root: Path | str | None = None,
) -> "APIRouter":
    try:
        from fastapi import APIRouter
        from fastapi.responses import JSONResponse
    except Exception as exc:  # pragma: no cover - optional viz extra.
        raise RuntimeError("FastAPI is required to serve Poros HTTP routes") from exc

    router = APIRouter()

    @router.get("/.well-known/okg.json")
    def well_known() -> dict:
        return authority_document.to_dict()

    @router.post("/poros/v1/inbox")
    async def poros_inbox(request: FastAPIRequest) -> JSONResponse:
        raw = await request.body()
        if receiver_keys is not None and trust_pins is not None:
            outcome = submit_poros_envelope_bytes(
                raw,
                trust_pins=trust_pins,
                receiver_keys=receiver_keys,
                receiver=receiver or authority_document.authority_id,
                dsn=dsn,
                sender_dsn=sender_dsn,
                deployment=deployment,
                persist_preflight=persist_preflight,
                apply=apply,
                publish=publish,
                nomos_bundle=nomos_bundle,
                object_store_root=object_store_root,
            )
            return JSONResponse(
                outcome.payload,
                status_code=outcome.http_status_code,
            )
        if inbox is None:
            raise RuntimeError(
                "build_poros_router requires either inbox or receiver config"
            )
        result = inbox.handle_bytes(raw)
        status_code = (
            202 if result.status in {"accepted_for_admission", "duplicate"} else 400
        )
        return JSONResponse(
            {
                "status": result.status,
                "reason_code": result.reason_code,
                "receipt": result.receipt,
            },
            status_code=status_code,
        )

    return router
