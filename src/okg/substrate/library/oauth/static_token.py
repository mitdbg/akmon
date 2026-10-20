"""Keychain custody for static (non-OAuth) API tokens.

Some sources authenticate with a long-lived per-user token rather
than an OAuth refresh flow — Canvas LMS is the first (an instructor
mints their own access token under Account -> Settings). Per
`route-canvas-token-to-keychain`: those tokens get the same
OS-keychain custody as OAuth tokens (the substrate `TokenStore`),
instead of living in an environment variable. An env var remains a
warned fallback so existing setups keep working, with a one-line
remediation pointing at the keychain command.

Stored shape mirrors the OAuth entries: `{"access_token": <token>}`
under `(deployment, provider)` so a single `TokenStore` holds both.
"""
from __future__ import annotations

import logging
import os
from typing import Optional

from okg.substrate.library.oauth.token_store import (
    KeyringUnavailable,
    TokenStore,
)

_log = logging.getLogger(__name__)


def set_static_token(
    *,
    deployment: str,
    provider: str,
    token: str,
    store: Optional[TokenStore] = None,
) -> None:
    """Persist a static token in the OS keychain under
    `(deployment, provider)`."""
    (store or TokenStore()).put(
        deployment=deployment,
        provider=provider,
        tokens={"access_token": token},
    )


def get_static_token(
    *,
    deployment: str,
    provider: str,
    env_var: str,
    explicit: Optional[str] = None,
    store: Optional[TokenStore] = None,
) -> tuple[Optional[str], str]:
    """Resolve a static token, preferring the most secure source.

    Order: explicit param (tests / source_registry) > OS keychain >
    environment variable (warned). Returns `(token, custody)` where
    custody is one of `explicit` / `keychain` / `env` / `absent` so
    callers can report where the credential came from.
    """
    if explicit:
        return explicit, "explicit"

    try:
        stored = (store or TokenStore()).get(
            deployment=deployment, provider=provider,
        )
    except KeyringUnavailable as exc:
        # Keychain unreachable is not fatal here — fall through to
        # the env var, but say so rather than masking it.
        _log.warning(
            "static token: keychain unavailable (%s); falling back "
            "to %s if set",
            exc,
            env_var,
        )
        stored = None
    if stored and stored.get("access_token"):
        return stored["access_token"], "keychain"

    env_token = os.environ.get(env_var)
    if env_token:
        _log.warning(
            "static token for %s@%s is being read from the %s "
            "environment variable, which is less secure than the OS "
            "keychain. Move it with: okg set-token --deployment "
            "%s --provider %s",
            provider,
            deployment,
            env_var,
            deployment,
            provider,
        )
        return env_token, "env"

    return None, "absent"
