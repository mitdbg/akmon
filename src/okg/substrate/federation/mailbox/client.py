"""HTTP client for polling a Poros mailbox service."""

from __future__ import annotations

import json
from typing import Any, Mapping


class MailboxPollingClient:
    """Small synchronous client around the optional mailbox HTTP API."""

    def __init__(
        self,
        base_url: str,
        *,
        client: Any | None = None,
        timeout: float = 30.0,
        principal: str | None = None,
        token: str | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        # Credentials presented to the authenticated mailbox read/finalize
        # endpoints (enforce-mailbox-poros-authz). When unset the client sends no
        # auth headers (e.g. submission-only use, or an unauthenticated server).
        self.principal = principal
        self.token = token
        if client is None:
            # Validate the operator/discovery-provided mailbox URL before opening
            # a real network client (centralize-egress-url-validation). Injected
            # clients (tests) bypass this; a dev loopback mailbox must be opted in
            # via OKG_EGRESS_ALLOW_HOSTS.
            from okg.substrate.library.sources._url_safety import (
                assert_public_http_url,
            )
            assert_public_http_url(self.base_url)
            try:
                import httpx
            except ImportError as exc:  # pragma: no cover - optional extra.
                raise RuntimeError(
                    "httpx is required for MailboxPollingClient; install okg[mailbox]"
                ) from exc
            client = httpx.Client(timeout=timeout)
        self.client = client

    def health(self) -> dict[str, Any]:
        return self._json(self.client.get(f"{self.base_url}/healthz"))

    def discovery(self) -> dict[str, Any]:
        return self._json(
            self.client.get(f"{self.base_url}/.well-known/okg-federation.json")
        )

    def submit_envelope(self, envelope: Mapping[str, Any]) -> dict[str, Any]:
        return self._json(
            self.client.post(
                f"{self.base_url}/messages",
                json={"envelope": dict(envelope)},
            )
        )

    def submit_pointer(self, pointer: Mapping[str, Any]) -> dict[str, Any]:
        return self._json(
            self.client.post(
                f"{self.base_url}/messages",
                json={"pointer": dict(pointer)},
            )
        )

    def submit_object(
        self,
        *,
        message_id: str,
        data: bytes,
        sender_authority_id: str,
        recipient_authority_id: str,
        expires_at: str,
        packet_sha256: str | None = None,
        metadata: Mapping[str, Any] | None = None,
        encryption: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "sender_authority_id": sender_authority_id,
            "recipient_authority_id": recipient_authority_id,
            "expires_at": expires_at,
        }
        if packet_sha256:
            params["packet_sha256"] = packet_sha256
        if metadata:
            params["metadata_json"] = json.dumps(
                dict(metadata),
                sort_keys=True,
                separators=(",", ":"),
            )
        if encryption:
            params["encryption_json"] = json.dumps(
                dict(encryption),
                sort_keys=True,
                separators=(",", ":"),
            )
        return self._json(
            self.client.post(
                f"{self.base_url}/objects/{message_id}",
                params=params,
                content=data,
            )
        )

    def fetch_object(self, message_id: str) -> bytes:
        response = self.client.get(
            f"{self.base_url}/messages/{message_id}/object",
            headers=self._auth_headers(),
        )
        response.raise_for_status()
        return bytes(response.content)

    def poll(
        self,
        recipient_authority_id: str,
        *,
        limit: int = 10,
        lease_seconds: int = 60,
    ) -> list[dict[str, Any]]:
        response = self._json(
            self.client.get(
                f"{self.base_url}/inbox/{recipient_authority_id}",
                params={"limit": limit, "lease_seconds": lease_seconds},
                headers=self._auth_headers(),
            )
        )
        messages = response.get("messages")
        if not isinstance(messages, list):
            raise RuntimeError("mailbox poll response did not include messages")
        return messages

    def get_message(self, message_id: str) -> dict[str, Any]:
        return self._json(
            self.client.get(
                f"{self.base_url}/messages/{message_id}",
                headers=self._auth_headers(),
            )
        )

    def ack(
        self,
        message_id: str,
        *,
        lease_token: str,
        receipt_evidence: Mapping[str, Any],
    ) -> dict[str, Any]:
        return self._json(
            self.client.post(
                f"{self.base_url}/messages/{message_id}/ack",
                json={
                    "lease_token": lease_token,
                    "receipt_evidence": dict(receipt_evidence),
                },
                headers=self._auth_headers(),
            )
        )

    def deny(
        self,
        message_id: str,
        *,
        lease_token: str,
        reason_code: str,
        denial_evidence: Mapping[str, Any],
    ) -> dict[str, Any]:
        return self._json(
            self.client.post(
                f"{self.base_url}/messages/{message_id}/deny",
                json={
                    "lease_token": lease_token,
                    "reason_code": reason_code,
                    "denial_evidence": dict(denial_evidence),
                },
                headers=self._auth_headers(),
            )
        )

    def _auth_headers(self) -> dict[str, str]:
        from okg.substrate.federation.mailbox.auth import (
            PRINCIPAL_HEADER,
            TOKEN_HEADER,
        )
        if self.principal and self.token:
            return {PRINCIPAL_HEADER: self.principal, TOKEN_HEADER: self.token}
        return {}

    @staticmethod
    def _json(response: Any) -> dict[str, Any]:
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError("mailbox response was not a JSON object")
        return payload
