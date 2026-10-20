"""route-canvas-token-to-keychain: static-token keychain custody."""
from __future__ import annotations

import logging

from okg.substrate.library.oauth.static_token import (
    get_static_token,
    set_static_token,
)


class StubStore:
    """In-memory TokenStore stand-in keyed by (deployment, provider)."""

    def __init__(self) -> None:
        self.data: dict[tuple[str, str], dict] = {}

    def put(self, *, deployment, provider, tokens):
        self.data[(deployment, provider)] = tokens

    def get(self, *, deployment, provider):
        return self.data.get((deployment, provider))


def test_resolution_prefers_keychain_over_env(monkeypatch) -> None:
    store = StubStore()
    store.put(
        deployment="pi", provider="canvas",
        tokens={"access_token": "keychain-tok"},
    )
    monkeypatch.setenv("OKG_CANVAS_TOKEN", "env-tok")

    # Explicit param wins outright.
    tok, custody = get_static_token(
        deployment="pi", provider="canvas", env_var="OKG_CANVAS_TOKEN",
        explicit="explicit-tok", store=store,
    )
    assert (tok, custody) == ("explicit-tok", "explicit")

    # Then keychain over env.
    tok, custody = get_static_token(
        deployment="pi", provider="canvas", env_var="OKG_CANVAS_TOKEN",
        store=store,
    )
    assert (tok, custody) == ("keychain-tok", "keychain")


def test_env_fallback_warns_with_remediation(monkeypatch, caplog) -> None:
    store = StubStore()  # empty keychain
    monkeypatch.setenv("OKG_CANVAS_TOKEN", "env-tok")
    with caplog.at_level(logging.WARNING):
        tok, custody = get_static_token(
            deployment="pi", provider="canvas",
            env_var="OKG_CANVAS_TOKEN", store=store,
        )
    assert (tok, custody) == ("env-tok", "env")
    msg = " ".join(r.message for r in caplog.records)
    assert "OKG_CANVAS_TOKEN" in msg
    assert "set-token" in msg


def test_absent_everywhere(monkeypatch) -> None:
    monkeypatch.delenv("OKG_CANVAS_TOKEN", raising=False)
    tok, custody = get_static_token(
        deployment="pi", provider="canvas", env_var="OKG_CANVAS_TOKEN",
        store=StubStore(),
    )
    assert (tok, custody) == (None, "absent")


def test_set_then_get_roundtrip() -> None:
    store = StubStore()
    set_static_token(
        deployment="pi", provider="canvas", token="t0k3n", store=store,
    )
    assert store.data[("pi", "canvas")] == {"access_token": "t0k3n"}
    tok, custody = get_static_token(
        deployment="pi", provider="canvas", env_var="OKG_CANVAS_TOKEN",
        store=store,
    )
    assert (tok, custody) == ("t0k3n", "keychain")


def test_keychain_unavailable_falls_back_to_env(monkeypatch, caplog) -> None:
    from okg.substrate.library.oauth.token_store import KeyringUnavailable

    class BrokenStore:
        def get(self, *, deployment, provider):
            raise KeyringUnavailable(RuntimeError("no backend"))

    monkeypatch.setenv("OKG_CANVAS_TOKEN", "env-tok")
    with caplog.at_level(logging.WARNING):
        tok, custody = get_static_token(
            deployment="pi", provider="canvas",
            env_var="OKG_CANVAS_TOKEN", store=BrokenStore(),
        )
    assert (tok, custody) == ("env-tok", "env")
    assert any("keychain unavailable" in r.message for r in caplog.records)
