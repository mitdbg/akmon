"""Operator auth + CSRF on viz mutating routes (secure-viz-operator-routes).

A configured operator token gates every mutating viz route: an unauthenticated
(or cross-origin, token-less) request is rejected with 401 BEFORE any side
effect and produces no audit record; an authenticated request runs and is
audited. Read routes are not gated.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

import okg.substrate.pact.operations as pact_ops
from okg.viz import auth as viz_auth

pytest.importorskip("fastapi")
pytest.importorskip("httpx")
from fastapi.testclient import TestClient  # noqa: E402

from okg.viz.app import build_app  # noqa: E402

_TOK = "s3cret-operator-token"
_HDR = {viz_auth.OPERATOR_TOKEN_HEADER: _TOK}


class _Ctx:
    deployment_name = "demo"


def _client(
    monkeypatch,
    *,
    audit_sink=None,
    captured=None,
    raise_server_exceptions=True,
):
    monkeypatch.setenv("OKG_OPERATOR_NAME", "RealOperator")

    def fake_approve(root, change_id, *, reviewer, decision, notes=None):
        if captured is not None:
            captured["reviewer"] = reviewer
        return SimpleNamespace(evidence=[])

    monkeypatch.setattr(pact_ops, "approve_intent", fake_approve)
    app = build_app(
        ctx_provider=_Ctx,
        is_ready=lambda: True,
        operator_token=_TOK,
        audit_sink=audit_sink,
    )
    return TestClient(app, raise_server_exceptions=raise_server_exceptions)


# ---- helpers (pure) ----


def test_is_mutating_route_classifies_correctly():
    assert viz_auth.is_mutating_route("POST", "/viz/api/repin")
    assert viz_auth.is_mutating_route("POST", "/viz/api/console/pact-review")
    assert viz_auth.is_mutating_route("POST", "/viz/api/portal/lens-overlays/o1/revoke")
    assert viz_auth.is_mutating_route("POST", "/viz/api/views/v1/recheck")
    assert viz_auth.is_mutating_route("POST", "/viz/api/tools/some_tool")
    # Not mutating:
    assert not viz_auth.is_mutating_route("GET", "/viz/api/repin")
    assert not viz_auth.is_mutating_route("GET", "/viz/api/health")
    assert not viz_auth.is_mutating_route("POST", "/viz/api/deployments")


# ---- gate behavior ----


def test_unauthenticated_mutation_is_rejected_without_side_effect(monkeypatch):
    captured: dict = {}
    audit: list = []
    client = _client(monkeypatch, audit_sink=audit, captured=captured)
    resp = client.post(
        "/viz/api/console/pact-review",
        json={"change_id": "c1", "decision": "approved"},
    )
    assert resp.status_code == 401
    assert "reviewer" not in captured  # approve_intent never ran -> no side effect
    assert audit == []  # a denial produces no audit record


def test_cross_origin_tokenless_post_is_rejected(monkeypatch):
    client = _client(monkeypatch)
    resp = client.post(
        "/viz/api/console/pact-review",
        json={"change_id": "c1", "decision": "approved"},
        headers={"Origin": "http://localhost:9999"},  # no operator token
    )
    # A cross-origin page cannot read/attach the custom header -> rejected (CSRF safe).
    assert resp.status_code == 401


def test_authenticated_mutation_runs_and_is_audited(monkeypatch):
    captured: dict = {}
    audit: list = []
    client = _client(monkeypatch, audit_sink=audit, captured=captured)
    resp = client.post(
        "/viz/api/console/pact-review",
        json={"change_id": "c1", "decision": "approved"},
        headers=_HDR,
    )
    assert resp.status_code == 200, resp.text
    assert captured["reviewer"] == "RealOperator"
    assert len(audit) == 1
    assert audit[0]["action"] == "/viz/api/console/pact-review"
    assert audit[0]["principal"] == "operator:viz-token"


@pytest.mark.parametrize(
    "path",
    [
        "/viz/api/health",
        "/viz/api/console/health",
        "/viz/api/console/subtype?name=pact_change",
        "/viz/api/traces",
        "/viz/api/workflows",
    ],
)
def test_read_routes_are_not_gated(monkeypatch, path):
    client = _client(monkeypatch, raise_server_exceptions=False)
    # GET read routes do not require the operator token. Some routes may return
    # 4xx/5xx with this intentionally tiny test context, but never the auth gate.
    resp = client.get(path)
    assert resp.status_code != 401
    assert "operator authentication required" not in resp.text


def _no_token_app(monkeypatch, captured):
    monkeypatch.setenv("OKG_OPERATOR_NAME", "RealOperator")
    monkeypatch.delenv("OKG_VIZ_OPERATOR_TOKEN", raising=False)

    def fake_approve(root, change_id, *, reviewer, decision, notes=None):
        captured["reviewer"] = reviewer
        return SimpleNamespace(evidence=[])

    monkeypatch.setattr(pact_ops, "approve_intent", fake_approve)
    return build_app(ctx_provider=_Ctx, is_ready=lambda: True)


def test_no_token_loopback_peer_allowed(monkeypatch):
    # No token configured: a LOOPBACK peer is still trusted (local-dev).
    captured: dict = {}
    app = _no_token_app(monkeypatch, captured)
    client = TestClient(app, client=("127.0.0.1", 40000))
    resp = client.post(
        "/viz/api/console/pact-review",
        json={"change_id": "c1", "decision": "approved"},
    )
    assert resp.status_code == 200
    assert captured["reviewer"] == "RealOperator"


def test_no_token_remote_peer_refused(monkeypatch):
    # No token configured: a NON-loopback peer is REFUSED (fail closed). This is
    # the network-exposure P0 the prior fail-open default left open — binding
    # 0.0.0.0 without a token must not serve mutations to the network.
    captured: dict = {}
    app = _no_token_app(monkeypatch, captured)
    client = TestClient(app, client=("10.0.0.9", 51111))
    resp = client.post(
        "/viz/api/console/pact-review",
        json={"change_id": "c1", "decision": "approved"},
    )
    assert resp.status_code == 401
    assert "reviewer" not in captured  # no side effect


def test_audit_is_emitted_to_durable_logger_without_injected_sink(monkeypatch, caplog):
    # Production passes no audit_sink; the audit must still be recorded (the prior
    # code only appended to an injected in-memory list, so production audited
    # nothing). Assert the durable okg.viz.audit logger fires.
    captured: dict = {}
    client = _client(monkeypatch, audit_sink=None, captured=captured)
    with caplog.at_level("INFO", logger="okg.viz.audit"):
        resp = client.post(
            "/viz/api/console/pact-review",
            json={"change_id": "c1", "decision": "approved"},
            headers=_HDR,
        )
    assert resp.status_code == 200, resp.text
    audit_lines = [r.getMessage() for r in caplog.records
                   if r.name == "okg.viz.audit"]
    assert any("/viz/api/console/pact-review" in m for m in audit_lines), audit_lines
    assert any("operator:viz-token" in m for m in audit_lines)


# ---- launcher fail-closed bind ----


def test_launcher_refuses_non_loopback_bind_without_token(monkeypatch):
    from okg.viz.__main__ import _register_auth_gated_bind

    # Loopback bind without a token is fine (local dev).
    _register_auth_gated_bind("127.0.0.1", 8765, None)
    # A non-loopback bind WITHOUT a token must refuse to start (fail closed).
    with pytest.raises(SystemExit):
        _register_auth_gated_bind("0.0.0.0", 8765, None)


def test_launcher_allows_non_loopback_bind_with_token(monkeypatch):
    from okg.substrate.cli.connector_security import AUTH_GATED_PORTS_ENV
    from okg.viz.__main__ import _register_auth_gated_bind

    monkeypatch.delenv(AUTH_GATED_PORTS_ENV, raising=False)
    # With a token, a non-loopback bind is allowed and declared auth-gated.
    _register_auth_gated_bind("0.0.0.0", 8765, "tok", api_token="read-tok")
    assert "8765" in (
        __import__("os").environ.get(AUTH_GATED_PORTS_ENV, "")
    )


def test_launcher_disables_proxy_headers(monkeypatch):
    # The no-token loopback gate trusts request.client.host; honoring
    # X-Forwarded-For would let a remote peer spoof a loopback peer. The launcher
    # must call uvicorn with proxy_headers=False so the real TCP peer is used.
    import inspect

    from okg.viz.__main__ import main as viz_main
    src = inspect.getsource(viz_main)
    assert "proxy_headers=False" in src, (
        "viz launcher must pass proxy_headers=False to uvicorn.run so X-Forwarded-For "
        "cannot spoof a loopback peer past the no-token gate"
    )


def test_every_post_route_is_classified_mutating(monkeypatch):
    # Defense-in-depth: a future POST route added without updating
    # is_mutating_route would be served unauthenticated when no token is set.
    # Walk the real route table and assert every POST route is classified
    # mutating (today all viz POST routes are mutations).
    import re

    from starlette.routing import Mount, Route

    app = build_app(ctx_provider=_Ctx, is_ready=lambda: True)

    def _walk(routes, prefix=""):
        for r in routes:
            if isinstance(r, Mount):
                yield from _walk(r.routes, prefix + r.path)
            elif isinstance(r, Route) and r.methods and "POST" in r.methods:
                # Concretize path params so the pattern matcher can classify it.
                yield re.sub(r"\{[^}]+\}", "x", prefix + r.path)

    post_paths = list(_walk(app.routes))
    assert post_paths, "expected viz to expose POST routes"
    unclassified = [
        p for p in post_paths if not viz_auth.is_mutating_route("POST", p)
    ]
    assert not unclassified, (
        f"POST routes not classified as mutating (would be unauthenticated "
        f"with no token): {unclassified}"
    )
