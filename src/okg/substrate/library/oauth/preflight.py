"""Shared OAuth preflight helper for the academic-researcher profile's
OAuth-backed source classes (gmail_api / google_calendar_api /
slack_api).

Per dogfood F-7a: `okg doctor --check source-auth` previously
reported these sources as `not_applicable` because they didn't
implement the optional `preflight()` hook. With the hook in
place the operator can answer "is my Gmail / Google Calendar /
Slack OAuth still valid?" from one command.

Status grammar (substrate-defined in `PREFLIGHT_STATUSES`):
  - `ok`                  — env vars set + token in keyring.
  - `missing_credential`  — env vars unset, or no token in
                            keyring. The reason field cites
                            which gate failed.
"""
from __future__ import annotations

import os

from okg.substrate.library.oauth.providers import OAuthProvider
from okg.substrate.library.oauth.token_store import (
    KeyringUnavailable,
    TokenStore,
)
from okg.substrate.library.sources.base import (
    SourcePreflightResult,
)


def oauth_preflight(
    *,
    source_name: str,
    deployment_name: str,
    provider: OAuthProvider,
) -> SourcePreflightResult:
    """Standard OAuth preflight: env vars + keyring token presence.

    Returns a SourcePreflightResult with status:
      - `ok`                  if both env vars + keyring token
      - `missing_credential`  if either is absent
    """
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()

    env_id = os.environ.get(provider.client_id_env)
    env_secret = os.environ.get(provider.client_secret_env)
    if not env_id or not env_secret:
        return SourcePreflightResult(
            source_name=source_name,
            status="missing_credential",
            mode="live",
            required=False,
            credential_refs=(
                provider.client_id_env,
                provider.client_secret_env,
            ),
            reason=(
                f"OAuth env vars not set: "
                f"{provider.client_id_env} and/or "
                f"{provider.client_secret_env}. Register an "
                f"OAuth app with the provider, export the env "
                f"vars, then open your registered agent and say "
                f"`set up my OKG for {deployment_name}` so the "
                f"connector route planner can authorize the "
                f"connection."
            ),
            checked_at=now,
        )

    try:
        store = TokenStore()
        tokens = store.get(
            deployment=deployment_name, provider=provider.name,
        )
    except KeyringUnavailable as exc:
        return SourcePreflightResult(
            source_name=source_name,
            status="missing_credential",
            mode="live",
            required=False,
            credential_refs=(
                provider.client_id_env,
                provider.client_secret_env,
            ),
            reason=f"keyring unavailable: {exc}",
            checked_at=now,
        )

    if not tokens:
        return SourcePreflightResult(
            source_name=source_name,
            status="missing_credential",
            mode="live",
            required=False,
            credential_refs=(
                provider.client_id_env,
                provider.client_secret_env,
            ),
            reason=(
                f"no OAuth token in keyring for "
                f"{provider.name}@{deployment_name}. Open your "
                f"registered agent and say `set up my OKG for "
                f"{deployment_name}` so the connector route planner "
                f"can consent and capture a refresh token."
            ),
            checked_at=now,
        )

    return SourcePreflightResult(
        source_name=source_name,
        status="ok",
        mode="live",
        required=False,
        credential_refs=(
            provider.client_id_env,
            provider.client_secret_env,
        ),
        reason=(
            f"env vars set + refresh token in keyring "
            f"({provider.name}@{deployment_name})"
        ),
        checked_at=now,
    )
