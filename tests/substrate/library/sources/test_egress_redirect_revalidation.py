"""Redirect-revalidation tests for the central egress validator
(centralize-egress-url-validation, adversarial-review remediation).

The prior egress tests mocked ``urllib.request.urlopen`` and so could never
exercise the redirect path — they proved only first-hop origin binding. These
tests drive REAL loopback HTTP servers that issue 3xx redirects, proving the
property the PACT claims: a token-bearing / SSRF-sensitive request is NOT
auto-redirected to an attacker-chosen or private/metadata origin, and the
``Authorization`` header is not replayed across a host boundary.

Empirically (see ``.scratch/probe_redirect.py``) the default ``urlopen`` opener
follows 3xx automatically and forwards ``Authorization`` cross-host; these tests
guard the fix (``open_validated_url`` installs a no-follow opener and re-validates
every hop).
"""
from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest import mock

import pytest

from okg.substrate.library.sources._url_safety import (
    UnsafeFetchURL,
    open_validated_url,
)


class _Recorder:
    """Shared mutable state a request handler writes into."""

    def __init__(self) -> None:
        self.requests: list[tuple[str, str | None]] = []  # (path, Authorization)
        self.routes: dict[str, tuple[int, dict[str, str], bytes]] = {}


def _make_server(recorder: _Recorder) -> tuple[HTTPServer, int]:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):  # noqa: ANN002 - quiet test server
            pass

        def _serve(self):
            recorder.requests.append((self.path, self.headers.get("Authorization")))
            code, headers, body = recorder.routes.get(
                self.path, (404, {}, b"not found")
            )
            self.send_response(code)
            for k, v in headers.items():
                self.send_header(k, v)
            self.end_headers()
            if body:
                self.wfile.write(body)

        do_GET = _serve
        do_POST = _serve

    srv = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, srv.server_address[1]


@pytest.fixture
def server():
    rec = _Recorder()
    srv, port = _make_server(rec)
    try:
        yield rec, port
    finally:
        srv.shutdown()


def test_redirect_to_metadata_host_is_refused_and_not_followed(server) -> None:
    rec, port = server
    # An allowed origin redirects to the cloud-metadata endpoint.
    rec.routes["/start"] = (
        302, {"Location": "http://169.254.169.254/latest/meta-data/"}, b""
    )
    with pytest.raises(UnsafeFetchURL):
        open_validated_url(
            f"http://127.0.0.1:{port}/start",
            allow_hosts=["127.0.0.1"],
            timeout=5,
        )
    # The first hop was made (the 302), but the metadata target is never fetched
    # (we raise before re-issuing). Only the /start request was recorded.
    assert [p for p, _ in rec.requests] == ["/start"]


def test_same_host_redirect_is_followed(server) -> None:
    rec, port = server
    rec.routes["/start"] = (302, {"Location": "/target"}, b"")
    rec.routes["/target"] = (200, {}, b"PAYLOAD")
    with open_validated_url(
        f"http://127.0.0.1:{port}/start", allow_hosts=["127.0.0.1"], timeout=5
    ) as resp:
        body = resp.read()
    assert body == b"PAYLOAD"
    assert [p for p, _ in rec.requests] == ["/start", "/target"]


def test_cross_host_redirect_strips_authorization(server) -> None:
    """Uncredentialed sinks (no same_origin): a cross-host redirect is still
    followed (after SSRF re-validation) but the Authorization header is dropped."""
    rec_a, port_a = server
    rec_b = _Recorder()
    srv_b, port_b = _make_server(rec_b)
    rec_b.routes["/target"] = (200, {}, b"B")
    try:
        # 127.0.0.1 (server A) -> localhost (server B): different host string.
        rec_a.routes["/start"] = (
            302, {"Location": f"http://localhost:{port_b}/target"}, b""
        )
        import urllib.request

        req = urllib.request.Request(
            f"http://127.0.0.1:{port_a}/start",
            headers={"Authorization": "Bearer SECRET"},
        )
        with open_validated_url(
            req, allow_hosts=["127.0.0.1", "localhost"], timeout=5
        ) as resp:
            assert resp.read() == b"B"
    finally:
        srv_b.shutdown()
    # Server B got the request, but WITHOUT the Authorization header.
    assert rec_b.requests == [("/target", None)]
    # Server A did see the credential on the first hop (same origin as the start).
    assert rec_a.requests[0] == ("/start", "Bearer SECRET")


def test_credentialed_redirect_to_metadata_refused(server) -> None:
    """With same_origin set (token-bearing), a redirect to a private/metadata
    host is refused — the Bearer never reaches the metadata endpoint."""
    rec, port = server
    rec.routes["/start"] = (
        302, {"Location": "http://169.254.169.254/latest/meta-data/"}, b""
    )
    import urllib.request

    req = urllib.request.Request(
        f"http://127.0.0.1:{port}/start",
        headers={"Authorization": "Bearer SECRET"},
    )
    with pytest.raises(UnsafeFetchURL):
        open_validated_url(
            req, same_origin=["127.0.0.1"], allow_hosts=["127.0.0.1"], timeout=5
        )
    assert [p for p, _ in rec.requests] == ["/start"]


def test_credentialed_cross_origin_public_redirect_refused_no_second_request() -> None:
    """White-box: a redirect to a PUBLIC but off-origin host must be refused for a
    credentialed request, with no second (token-bearing) request issued. Uses a
    fake opener so no network/DNS is needed for the off-origin public host."""
    import urllib.error
    import urllib.request

    calls = {"n": 0}

    class _FakeOpener:
        def open(self, req, timeout=None):  # noqa: ANN001
            calls["n"] += 1
            if calls["n"] == 1:
                hdrs = {"Location": "https://attacker.example/steal"}
                raise urllib.error.HTTPError(
                    req.full_url, 302, "Found", hdrs, None
                )
            raise AssertionError("second (token-bearing) request must not be issued")

    def _resolves_public(host, *a, **k):
        return [(2, 1, 6, "", ("93.184.216.34", 443))]

    with mock.patch(
        "okg.substrate.library.sources._url_safety.urllib.request.build_opener",
        return_value=_FakeOpener(),
    ), mock.patch(
        "okg.substrate.library.sources._url_safety.socket.getaddrinfo",
        side_effect=_resolves_public,
    ):
        req = urllib.request.Request(
            "https://api.example.com/messages",
            headers={"Authorization": "Bearer SECRET"},
        )
        with pytest.raises(UnsafeFetchURL):
            open_validated_url(
                req, same_origin=["api.example.com"], timeout=5
            )
    assert calls["n"] == 1  # exactly one hop; the redirect was refused


def test_redirect_loop_is_bounded(server) -> None:
    rec, port = server
    rec.routes["/loop"] = (302, {"Location": "/loop"}, b"")
    with pytest.raises(UnsafeFetchURL):
        open_validated_url(
            f"http://127.0.0.1:{port}/loop",
            allow_hosts=["127.0.0.1"],
            max_redirects=3,
            timeout=5,
        )
