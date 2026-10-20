"""viz bind auth-gating in the listener-exposure doctor check
(secure-viz-operator-routes).

A non-loopback unauthenticated viz listener fails evaluate_listeners; the same
port declared in OKG_AUTH_GATED_PORTS (operator-token bind) passes. The viz
launcher registers/warns accordingly.
"""
from __future__ import annotations

from okg.substrate.cli.connector_security import (
    AUTH_GATED_PORTS_ENV,
    ListenerInfo,
    evaluate_listeners,
)
from okg.viz.__main__ import _register_auth_gated_bind


def _viz_listener(*, host: str, auth_gated: bool) -> ListenerInfo:
    return ListenerInfo(
        name="okg.viz", pid=1234, host=host, port=8765, auth_gated=auth_gated,
    )


def test_non_loopback_unauthenticated_viz_listener_fails():
    result = evaluate_listeners([_viz_listener(host="0.0.0.0", auth_gated=False)])
    row = result["listeners"][0] if "listeners" in result else result["rows"][0]
    assert row["ok"] is False


def test_non_loopback_auth_gated_viz_listener_passes():
    result = evaluate_listeners([_viz_listener(host="0.0.0.0", auth_gated=True)])
    row = result["listeners"][0] if "listeners" in result else result["rows"][0]
    assert row["ok"] is True


def test_loopback_viz_listener_passes():
    result = evaluate_listeners([_viz_listener(host="127.0.0.1", auth_gated=False)])
    row = result["listeners"][0] if "listeners" in result else result["rows"][0]
    assert row["ok"] is True


# ---- launcher wiring ----


def test_launcher_registers_auth_gated_port_with_token(monkeypatch):
    monkeypatch.delenv(AUTH_GATED_PORTS_ENV, raising=False)
    _register_auth_gated_bind("0.0.0.0", 8765, "tok", api_token="read-tok")
    import os
    assert "8765" in (os.environ.get(AUTH_GATED_PORTS_ENV) or "")


def test_launcher_refuses_non_loopback_bind_without_token(monkeypatch):
    # Fail closed: a non-loopback bind WITHOUT a token must refuse to start
    # rather than warn-and-serve (the prior silent-insecure-default).
    monkeypatch.delenv(AUTH_GATED_PORTS_ENV, raising=False)
    import os

    import pytest
    with pytest.raises(SystemExit):
        _register_auth_gated_bind("0.0.0.0", 8765, None)
    # And it certainly is NOT registered as auth-gated.
    assert "8765" not in (os.environ.get(AUTH_GATED_PORTS_ENV) or "")


def test_launcher_noop_for_loopback(monkeypatch):
    monkeypatch.delenv(AUTH_GATED_PORTS_ENV, raising=False)
    _register_auth_gated_bind("127.0.0.1", 8765, "tok")
    import os
    assert not (os.environ.get(AUTH_GATED_PORTS_ENV) or "")
