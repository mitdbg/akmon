"""Deployment-probe registry allowlist (secure-viz-operator-routes).

/viz/api/deployments/probe restricts to (loopback) ports the deployment
registry actually lists, so it cannot be used as a localhost port scanner; an
empty registry fails closed.
"""
from __future__ import annotations

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")
from fastapi.testclient import TestClient  # noqa: E402

from okg.viz import app as viz_app  # noqa: E402


class _Ctx:
    deployment_name = "demo"


def _client(monkeypatch, *, registry_entries, calls):
    monkeypatch.setattr(viz_app, "load_registry", lambda **k: registry_entries)
    monkeypatch.setattr(viz_app, "_bound_port", lambda: None)

    def fake_conn(addr, timeout=None):
        calls.append(addr)

        class _S:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        return _S()

    monkeypatch.setattr(viz_app.socket, "create_connection", fake_conn)
    app = viz_app.build_app(ctx_provider=_Ctx, is_ready=lambda: True)
    return TestClient(app)


def test_registry_listed_port_is_probed(monkeypatch):
    calls: list = []
    c = _client(
        monkeypatch,
        registry_entries=[{"name": "x", "port": 9001, "url": "http://127.0.0.1:9001"}],
        calls=calls,
    )
    r = c.get("/viz/api/deployments/probe", params={"url": "http://127.0.0.1:9001"})
    assert r.status_code == 200
    assert calls == [("127.0.0.1", 9001)]


def test_non_registry_port_refused_without_connecting(monkeypatch):
    calls: list = []
    c = _client(monkeypatch, registry_entries=[{"port": 9001}], calls=calls)
    r = c.get("/viz/api/deployments/probe", params={"url": "http://127.0.0.1:9999"})
    assert r.status_code == 403
    assert calls == []  # never opened a socket to the unlisted port


def test_empty_registry_fails_closed(monkeypatch):
    calls: list = []
    c = _client(monkeypatch, registry_entries=[], calls=calls)
    r = c.get("/viz/api/deployments/probe", params={"url": "http://127.0.0.1:9001"})
    assert r.status_code == 403
    assert calls == []


def test_non_loopback_host_refused(monkeypatch):
    calls: list = []
    c = _client(monkeypatch, registry_entries=[{"port": 9001}], calls=calls)
    r = c.get("/viz/api/deployments/probe", params={"url": "http://10.0.0.5:9001"})
    assert r.status_code == 400
    assert calls == []
