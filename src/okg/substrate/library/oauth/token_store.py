"""Encrypted-at-rest token storage via OS keychain.

Uses the `keyring` Python lib (macOS Keychain / Linux Secret
Service / Windows Credential Manager). One entry per
`(deployment_name, provider)` tuple.

`keyring` is a hard runtime dependency (see `pyproject.toml`). On
machines without a functional backend (typically Linux without
SecretService running) `TokenStore` raises `KeyringUnavailable`
with a fix hint rather than silently downgrading to plaintext.
"""
from __future__ import annotations

import json
from typing import Optional

import keyring
from keyring.errors import KeyringError


_SERVICE_NAME = "okg-oauth"
_REGISTRY_KEY = "_providers_registry"


class KeyringUnavailable(RuntimeError):
    """Raised when the system has no usable keychain backend.

    On Linux this typically means SecretService (gnome-keyring /
    kwallet) isn't running. On macOS this is unexpected — the
    Keychain ships with the OS. On Windows the Credential Manager
    is always available.
    """

    def __init__(self, original: BaseException) -> None:
        hint = (
            "On Linux: install + start a SecretService backend "
            "(`sudo apt install gnome-keyring` or kwallet). On "
            "macOS/Windows the OS keychain ships built-in; check "
            "that `python -c 'import keyring; "
            "keyring.get_keyring()'` returns a real backend, not "
            "`keyring.backends.fail.Keyring`."
        )
        super().__init__(
            f"OAuth token storage requires a system keychain backend "
            f"({original.__class__.__name__}: {original}). {hint}"
        )


class TokenStore:
    """Per-deployment per-provider token storage."""

    def put(
        self,
        *,
        deployment: str,
        provider: str,
        tokens: dict,
    ) -> None:
        key = f"{deployment}:{provider}"
        payload = json.dumps(tokens)
        try:
            keyring.set_password(_SERVICE_NAME, key, payload)
        except KeyringError as exc:
            raise KeyringUnavailable(exc) from exc
        self._register(deployment, provider)

    def get(
        self,
        *,
        deployment: str,
        provider: str,
    ) -> Optional[dict]:
        key = f"{deployment}:{provider}"
        try:
            raw = keyring.get_password(_SERVICE_NAME, key)
        except KeyringError as exc:
            raise KeyringUnavailable(exc) from exc
        return json.loads(raw) if raw else None

    def delete(
        self,
        *,
        deployment: str,
        provider: str,
    ) -> None:
        key = f"{deployment}:{provider}"
        try:
            keyring.delete_password(_SERVICE_NAME, key)
        except keyring.errors.PasswordDeleteError:
            # Already absent — treat as success.
            pass
        except KeyringError as exc:
            raise KeyringUnavailable(exc) from exc
        self._unregister(deployment, provider)

    def list_authorized(self, *, deployment: str) -> list[str]:
        """Return the list of providers this deployment has tokens for.

        keyring backends don't expose enumeration, so we maintain a
        per-deployment side-registry under a reserved keychain entry.
        """
        registry = self._load_registry()
        return sorted(registry.get(deployment, []))

    # --- internals ---

    def _register(self, deployment: str, provider: str) -> None:
        registry = self._load_registry()
        providers = set(registry.get(deployment, []))
        providers.add(provider)
        registry[deployment] = sorted(providers)
        self._save_registry(registry)

    def _unregister(self, deployment: str, provider: str) -> None:
        registry = self._load_registry()
        providers = set(registry.get(deployment, []))
        providers.discard(provider)
        if providers:
            registry[deployment] = sorted(providers)
        else:
            registry.pop(deployment, None)
        self._save_registry(registry)

    def _load_registry(self) -> dict[str, list[str]]:
        try:
            raw = keyring.get_password(_SERVICE_NAME, _REGISTRY_KEY)
        except KeyringError as exc:
            raise KeyringUnavailable(exc) from exc
        return json.loads(raw) if raw else {}

    def _save_registry(self, registry: dict[str, list[str]]) -> None:
        try:
            keyring.set_password(
                _SERVICE_NAME, _REGISTRY_KEY, json.dumps(registry),
            )
        except KeyringError as exc:
            raise KeyringUnavailable(exc) from exc
