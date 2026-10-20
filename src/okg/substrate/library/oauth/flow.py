"""OAuth 2.0 authorization-code flow handler.

Provider-agnostic. Launches a browser to the provider's
auth_url, listens on `127.0.0.1:<random-port>` for the
redirect, exchanges the captured code for tokens, persists
via TokenStore.

Usage:

    from okg.substrate.library.oauth import OAuthFlow
    from okg.substrate.library.oauth.providers import GMAIL

    flow = OAuthFlow(provider=GMAIL, deployment_name="madden_pi")
    tokens = flow.start()  # blocks while browser flow completes
"""
from __future__ import annotations

import http.server
import secrets
import socket
import threading
import urllib.parse
import urllib.request
import webbrowser
from dataclasses import dataclass, field
from typing import Optional

from okg.substrate.library.oauth.token_store import TokenStore
from okg.substrate.library.sources._url_safety import (
    assert_public_http_url,
    open_validated_url,
)


@dataclass
class OAuthProvider:
    """Per-provider registry entry."""

    name: str
    auth_url: str
    token_url: str
    refresh_url: Optional[str]  # defaults to token_url
    scopes: list[str]
    client_id_env: str
    client_secret_env: str
    redirect_uri_template: str = "http://127.0.0.1:{port}/callback"
    #: Hosts the access token may be sent to in `authed_request` (the API
    #: origin set). Entries may be exact hosts or leading-dot suffix patterns
    #: (`.googleapis.com`). The token/refresh/auth hosts are always allowed in
    #: addition. This binds the OAuth token to its provider so a
    #: response-provided pagination/delta URL cannot redirect it to another
    #: origin (per `centralize-egress-url-validation`).
    api_hosts: list[str] = field(default_factory=list)


class OAuthFlow:
    """Launch a browser flow, capture the code, exchange for tokens.

    Synchronous: `start()` blocks until the redirect arrives
    or the operator cancels. The captured tokens are persisted
    via TokenStore.
    """

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

    def start(self) -> dict:
        """Run the full OAuth flow. Returns the parsed token
        response dict (access_token, refresh_token, expires_in).
        """
        import os
        client_id = os.environ.get(self.provider.client_id_env)
        client_secret = os.environ.get(self.provider.client_secret_env)
        if not client_id or not client_secret:
            raise RuntimeError(
                f"OAuth credentials missing for {self.provider.name}: "
                f"set {self.provider.client_id_env} and "
                f"{self.provider.client_secret_env}"
            )

        # Pick a free local port for the redirect listener.
        port = _free_port()
        redirect_uri = self.provider.redirect_uri_template.format(port=port)
        state = secrets.token_urlsafe(16)

        # Build the auth URL.
        params = {
            "response_type": "code",
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "scope": " ".join(self.provider.scopes),
            "state": state,
            "access_type": "offline",  # Google needs this for refresh
            "prompt": "consent",
        }
        auth_url = f"{self.provider.auth_url}?{urllib.parse.urlencode(params)}"

        # Capture the redirect.
        captured: dict[str, str] = {}

        class CallbackHandler(http.server.BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                qs = urllib.parse.urlparse(self.path).query
                params = urllib.parse.parse_qs(qs)
                captured["code"] = (params.get("code") or [""])[0]
                captured["state"] = (params.get("state") or [""])[0]
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                self.wfile.write(
                    b"<html><body><h1>OKG OAuth flow complete</h1>"
                    b"<p>You can close this tab.</p></body></html>"
                )

            def log_message(self, *_args) -> None:  # noqa: N802
                pass  # silence stdout

        srv = http.server.HTTPServer(("127.0.0.1", port), CallbackHandler)
        t = threading.Thread(target=srv.handle_request)
        t.start()
        webbrowser.open(auth_url)
        t.join(timeout=300)  # 5-minute browser timeout
        srv.server_close()

        if not captured.get("code"):
            raise RuntimeError(
                f"OAuth flow timed out or was cancelled for "
                f"{self.provider.name}"
            )
        if captured.get("state") != state:
            raise RuntimeError("OAuth state mismatch — flow rejected")

        # Exchange code for tokens.
        token_req_body = urllib.parse.urlencode({
            "grant_type": "authorization_code",
            "code": captured["code"],
            "client_id": client_id,
            "client_secret": client_secret,
            "redirect_uri": redirect_uri,
        }).encode("utf-8")
        # SSRF guard on the operator-configured token endpoint before the POST
        # that carries the client_secret, and bind it to the token host so a 3xx
        # cannot redirect the credential to another origin.
        assert_public_http_url(self.provider.token_url)
        token_host = urllib.parse.urlparse(self.provider.token_url).hostname or ""
        token_req = urllib.request.Request(
            self.provider.token_url,
            data=token_req_body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        with open_validated_url(
            token_req, same_origin=token_host, timeout=30,
        ) as resp:
            import json
            tokens = json.load(resp)

        # Persist.
        self.store.put(
            deployment=self.deployment_name,
            provider=self.provider.name,
            tokens=tokens,
        )
        return tokens


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]
