"""OAuth token-origin binding (centralize-egress-url-validation).

RefreshHandler.authed_request must validate the target URL as a public http(s)
URL AND bind it to the provider's expected origin set BEFORE attaching the
Bearer token, so a response-provided pagination/delta/redirect URL cannot
exfiltrate the OAuth token to another origin.
"""
from __future__ import annotations

import io
import time
from unittest import mock

import pytest

from okg.substrate.library.oauth.flow import OAuthProvider
from okg.substrate.library.oauth.providers import MSGRAPH
from okg.substrate.library.oauth.refresh import RefreshHandler
from okg.substrate.library.sources._url_safety import UnsafeFetchURL


class StubStore:
    def __init__(self, tokens: dict) -> None:
        self._t = tokens

    def get(self, *, deployment, provider):
        return self._t

    def put(self, *, deployment, provider, tokens):
        self._t = tokens


def _provider(api_hosts):
    return OAuthProvider(
        name="fake",
        auth_url="https://auth.example.com/a",
        token_url="https://token.example.com/t",
        refresh_url="https://token.example.com/t",
        scopes=[],
        client_id_env="X_ID",
        client_secret_env="X_SECRET",
        api_hosts=list(api_hosts),
    )


def _handler(provider):
    store = StubStore(
        {"access_token": "tok", "_acquired_at": time.time(), "expires_in": 3600}
    )
    return RefreshHandler(provider=provider, deployment_name="d", store=store)


def _public_dns(ip: str = "93.184.216.34"):
    return mock.patch(
        "okg.substrate.library.sources._url_safety.socket.getaddrinfo",
        return_value=[(2, 1, 6, "", (ip, 443))],
    )


def _fake_urlopen(payload: bytes = b'{"ok": true}'):
    """Patch the no-follow opener built inside ``open_validated_url`` so an
    allowed request returns a canned body without touching the network. (The
    opener is built only AFTER the initial-URL validation passes, so a refused
    request never calls ``build_opener`` — ``.called`` distinguishes the two.)"""
    cm = mock.MagicMock()
    cm.__enter__.return_value = io.BytesIO(payload)
    cm.__exit__.return_value = False
    fake_opener = mock.MagicMock()
    fake_opener.open.return_value = cm
    return mock.patch(
        "okg.substrate.library.sources._url_safety.urllib.request.build_opener",
        return_value=fake_opener,
    )


def test_same_origin_request_succeeds() -> None:
    h = _handler(_provider(["api.example.com"]))
    with _public_dns(), _fake_urlopen() as uo:
        out = h.authed_request("https://api.example.com/messages?page=2")
    assert out == {"ok": True}
    assert uo.called


def test_cross_origin_followup_refused_without_request() -> None:
    h = _handler(_provider(["api.example.com"]))
    # SSRF check would pass (public DNS), but the host is off-origin -> refuse
    # and never open the connection.
    with _public_dns(), _fake_urlopen() as uo:
        with pytest.raises(UnsafeFetchURL):
            h.authed_request("https://attacker.example/steal-token")
        assert not uo.called


def test_delta_link_resolving_to_private_host_refused() -> None:
    h = _handler(_provider(["api.example.com"]))
    with mock.patch(
        "okg.substrate.library.sources._url_safety.socket.getaddrinfo",
        return_value=[(2, 1, 6, "", ("10.0.0.5", 443))],
    ), _fake_urlopen() as uo:
        with pytest.raises(UnsafeFetchURL):
            h.authed_request("https://api.example.com/delta")  # resolves private
        assert not uo.called


def test_real_msgraph_provider_binds_to_graph_host() -> None:
    h = _handler(MSGRAPH)
    # A graph.microsoft.com delta/next URL is allowed.
    with _public_dns(), _fake_urlopen() as uo:
        out = h.authed_request("https://graph.microsoft.com/v1.0/me/messages")
    assert out == {"ok": True}
    assert uo.called
    # An attacker @odata.deltaLink is refused before any request.
    with _public_dns(), _fake_urlopen() as uo2:
        with pytest.raises(UnsafeFetchURL):
            h.authed_request("https://graph.microsoft.com.attacker.test/me")
        assert not uo2.called


def test_redirect_to_metadata_does_not_forward_the_bearer(monkeypatch) -> None:
    """End-to-end (NOT mocked): the provider host returns a 302 to the cloud
    metadata endpoint. authed_request must refuse the hop — the Bearer token is
    never replayed to 169.254.169.254. This exercises the real no-follow opener,
    the property the mocked tests above structurally cannot observe."""
    import threading
    from http.server import BaseHTTPRequestHandler, HTTPServer

    seen: list[tuple[str, str | None]] = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):  # noqa: ANN002
            pass

        def do_GET(self):
            seen.append((self.path, self.headers.get("Authorization")))
            self.send_response(302)
            self.send_header("Location", "http://169.254.169.254/latest/meta-data/")
            self.end_headers()

    srv = HTTPServer(("127.0.0.1", 0), Handler)
    port = srv.server_address[1]
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    # Allow the loopback test server as an explicit operator exception so the
    # initial hop is permitted; the metadata host is NOT allowlisted.
    monkeypatch.setenv("OKG_EGRESS_ALLOW_HOSTS", "127.0.0.1")
    try:
        h = _handler(_provider(["127.0.0.1"]))
        with pytest.raises(UnsafeFetchURL):
            h.authed_request(f"http://127.0.0.1:{port}/start")
    finally:
        srv.shutdown()
    # The provider host saw the Bearer on the first hop; the redirect to the
    # metadata host was refused, so only the first request happened.
    assert [p for p, _ in seen] == ["/start"]
    assert seen[0][1] == "Bearer tok"
