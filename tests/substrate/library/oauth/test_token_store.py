"""TokenStore smoke tests against a fake keyring backend.

Per `streamline-academic-pi-install`:
- Round-trip put/get/delete through the keyring backend.
- list_authorized reflects the side-registry that put/delete
  maintain.
- A missing/failing backend raises KeyringUnavailable with a
  fix-hint message (no silent plaintext fallback).
"""
from __future__ import annotations

import pytest
import keyring
from keyring.backend import KeyringBackend
from keyring.errors import KeyringError, PasswordDeleteError

from okg.substrate.library.oauth.token_store import (
    KeyringUnavailable, TokenStore,
)


class _FakeKeyring(KeyringBackend):
    """In-memory keyring backend for unit tests."""

    priority = 100  # > 1 so keyring picks us

    def __init__(self) -> None:
        self._store: dict[tuple[str, str], str] = {}

    def set_password(self, service: str, username: str, password: str) -> None:
        self._store[(service, username)] = password

    def get_password(self, service: str, username: str) -> str | None:
        return self._store.get((service, username))

    def delete_password(self, service: str, username: str) -> None:
        if (service, username) not in self._store:
            raise PasswordDeleteError("not found")
        del self._store[(service, username)]


class _FailingKeyring(KeyringBackend):
    """Backend that simulates Linux-without-SecretService."""

    priority = 100

    def set_password(self, service: str, username: str, password: str) -> None:
        raise KeyringError("No recommended backend was available")

    def get_password(self, service: str, username: str) -> str | None:
        raise KeyringError("No recommended backend was available")

    def delete_password(self, service: str, username: str) -> None:
        raise KeyringError("No recommended backend was available")


@pytest.fixture()
def fake_keyring() -> _FakeKeyring:
    original = keyring.get_keyring()
    fake = _FakeKeyring()
    keyring.set_keyring(fake)
    try:
        yield fake
    finally:
        keyring.set_keyring(original)


@pytest.fixture()
def failing_keyring() -> _FailingKeyring:
    original = keyring.get_keyring()
    fake = _FailingKeyring()
    keyring.set_keyring(fake)
    try:
        yield fake
    finally:
        keyring.set_keyring(original)


def test_put_and_get_round_trips(fake_keyring: _FakeKeyring) -> None:
    store = TokenStore()
    tokens = {"access_token": "ya29.abc", "refresh_token": "1//xyz"}
    store.put(deployment="madden_pi", provider="gmail_api", tokens=tokens)

    got = store.get(deployment="madden_pi", provider="gmail_api")
    assert got == tokens


def test_get_returns_none_for_missing_entry(
    fake_keyring: _FakeKeyring,
) -> None:
    store = TokenStore()
    got = store.get(deployment="nobody", provider="gmail_api")
    assert got is None


def test_delete_removes_entry(fake_keyring: _FakeKeyring) -> None:
    store = TokenStore()
    store.put(
        deployment="madden_pi", provider="slack_api",
        tokens={"access_token": "xoxb-abc"},
    )
    store.delete(deployment="madden_pi", provider="slack_api")
    assert store.get(
        deployment="madden_pi", provider="slack_api",
    ) is None


def test_delete_missing_entry_is_idempotent(
    fake_keyring: _FakeKeyring,
) -> None:
    store = TokenStore()
    # No prior put — delete should not raise.
    store.delete(deployment="madden_pi", provider="gmail_api")


def test_list_authorized_reflects_puts_and_deletes(
    fake_keyring: _FakeKeyring,
) -> None:
    store = TokenStore()
    assert store.list_authorized(deployment="madden_pi") == []

    store.put(
        deployment="madden_pi", provider="gmail_api",
        tokens={"access_token": "a"},
    )
    store.put(
        deployment="madden_pi", provider="slack_api",
        tokens={"access_token": "b"},
    )
    assert store.list_authorized(deployment="madden_pi") == [
        "gmail_api", "slack_api",
    ]

    store.delete(deployment="madden_pi", provider="gmail_api")
    assert store.list_authorized(deployment="madden_pi") == [
        "slack_api",
    ]


def test_failing_backend_raises_keyring_unavailable(
    failing_keyring: _FailingKeyring,
) -> None:
    store = TokenStore()
    with pytest.raises(KeyringUnavailable) as exc_info:
        store.put(
            deployment="madden_pi", provider="gmail_api",
            tokens={"access_token": "a"},
        )
    msg = str(exc_info.value)
    # The error SHALL name the missing dep / fix-hint.
    assert "keychain" in msg.lower()
    assert (
        "SecretService" in msg
        or "gnome-keyring" in msg
        or "kwallet" in msg
    )
