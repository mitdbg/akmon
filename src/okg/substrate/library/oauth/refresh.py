"""Refresh-token rotation handler.

Wraps API calls so that 401s transparently trigger a token
refresh + retry. If refresh fails, the operator must re-auth.
"""
from __future__ import annotations

import json
import os
import time
import urllib.parse
import urllib.request
from urllib.parse import urlparse
from typing import Any, Optional

from okg.substrate.library.oauth.token_store import TokenStore
from okg.substrate.library.oauth.flow import OAuthProvider
from okg.substrate.library.sources._url_safety import (
    assert_public_http_url,
    assert_token_request_url,
    open_validated_url,
)


class RefreshHandler:
    """Refreshes access tokens transparently on 401."""

    def __init__(
        self,
        *,
        provider: OAuthProvider,
        deployment_name: str,
        store: Optional[TokenStore] = None,
    ) -> None:
        self.provider = provider
        self.deployment_name = deployment_name
        self.store = store or TokenStore()

    def _allowed_token_hosts(self) -> list[str]:
        """The host set the access token may be sent to: the provider's
        declared API hosts plus its auth/token/refresh hosts. A
        response-provided pagination/delta URL outside this set must not
        receive the OAuth token."""
        hosts: list[str] = list(self.provider.api_hosts)
        for u in (
            self.provider.token_url,
            self.provider.refresh_url,
            self.provider.auth_url,
        ):
            h = urlparse(u or "").hostname
            if h:
                hosts.append(h)
        return hosts

    def access_token(self) -> str:
        """Return a valid access token, refreshing if expired."""
        tokens = self.store.get(
            deployment=self.deployment_name, provider=self.provider.name,
        )
        if not tokens:
            raise RuntimeError(
                f"No OAuth tokens for {self.provider.name} on "
                f"deployment {self.deployment_name}. No okg command "
                "logs in yet; store tokens by running the "
                "authorization-code flow, OAuthFlow(...).start() in "
                "okg.substrate.library.oauth."
            )
        # Check expiry. Token response carries `expires_in`; we
        # stamp `_acquired_at` at store time for tracking.
        acquired = tokens.get("_acquired_at", 0)
        expires_in = tokens.get("expires_in", 3600)
        if time.time() < acquired + expires_in - 60:
            return tokens["access_token"]
        # Expired — refresh.
        return self._refresh(tokens)

    def _refresh(self, tokens: dict) -> str:
        refresh_token = tokens.get("refresh_token")
        if not refresh_token:
            raise RuntimeError(
                f"No refresh_token for {self.provider.name}; no okg "
                "command logs in yet, so run the authorization-code "
                "flow again with OAuthFlow(...).start() in "
                "okg.substrate.library.oauth."
            )
        client_id = os.environ.get(self.provider.client_id_env)
        client_secret = os.environ.get(self.provider.client_secret_env)
        body = urllib.parse.urlencode({
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
        }).encode("utf-8")
        refresh_url = self.provider.refresh_url or self.provider.token_url
        # SSRF guard on the (operator-configured) token endpoint before the POST.
        assert_public_http_url(refresh_url)
        req = urllib.request.Request(
            refresh_url,
            data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        # Bind the client_secret-bearing POST to the token host so a 3xx cannot
        # redirect the credential to another origin; redirect Locations are
        # re-validated, never auto-followed.
        with open_validated_url(
            req, same_origin=self._allowed_token_hosts(), timeout=30,
        ) as resp:
            new_tokens = json.load(resp)
        # Some providers don't re-issue refresh_token; keep the old one.
        if "refresh_token" not in new_tokens:
            new_tokens["refresh_token"] = refresh_token
        new_tokens["_acquired_at"] = time.time()
        self.store.put(
            deployment=self.deployment_name,
            provider=self.provider.name,
            tokens=new_tokens,
        )
        return new_tokens["access_token"]

    def authed_request(
        self,
        url: str,
        *,
        method: str = "GET",
        data: Optional[bytes] = None,
        extra_headers: Optional[dict[str, str]] = None,
    ) -> Any:
        """Make an HTTP request with the Bearer token. Returns
        the JSON-decoded response.

        The URL is validated as a public http(s) URL AND bound to the
        provider's expected origin set BEFORE the token is attached, so a
        response-provided pagination/delta/redirect URL cannot exfiltrate the
        OAuth token to another origin (per `centralize-egress-url-validation`).
        """
        allowed_hosts = self._allowed_token_hosts()
        assert_token_request_url(url, allowed_hosts)
        token = self.access_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
        if extra_headers:
            headers.update(extra_headers)
        req = urllib.request.Request(
            url, data=data, headers=headers, method=method,
        )
        # The Bearer is bound to the provider origin across every redirect hop:
        # a response-provided 3xx Location to another origin is refused, never
        # auto-followed (which would replay the token cross-origin).
        with open_validated_url(req, same_origin=allowed_hosts, timeout=60) as resp:
            return json.load(resp)
