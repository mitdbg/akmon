"""Authenticated-principal binding for the mailbox HTTP surface.

The Poros mailbox transport read/finalize endpoints (poll, direct read, object
fetch, ack, deny) are network-reachable and must require an authenticated
principal that is bound to the message's recipient authority. This module
provides the authenticator contract plus a fail-closed default and a minimal
shared-secret authenticator (per `enforce-mailbox-poros-authz`).
"""
from __future__ import annotations

import hmac
from typing import Any, Callable, Mapping

#: Header carrying the caller's claimed principal authority id.
PRINCIPAL_HEADER = "x-okg-mailbox-principal"
#: Header carrying the caller's principal credential (shared secret / token).
TOKEN_HEADER = "x-okg-mailbox-token"


class MailboxAuthError(Exception):
    """The mailbox caller could not be authenticated as a principal."""


#: Given the inbound request, return the authenticated principal authority id,
#: or raise MailboxAuthError. The HTTP layer maps the error to 401.
MailboxAuthenticator = Callable[[Any], str]


def deny_all_authenticator(request: Any) -> str:
    """Fail-closed default: with no authenticator configured, every
    read/finalize request is rejected rather than served anonymously."""
    raise MailboxAuthError(
        "mailbox authentication is not configured; refusing read/finalize "
        "requests (fail closed)"
    )


def static_token_authenticator(
    tokens: Mapping[str, str],
) -> MailboxAuthenticator:
    """Authenticate a principal by a per-principal shared secret presented in
    request headers. A real deployment would back this with signed requests /
    pinned keys; this is the minimal mechanism that binds a caller to a
    principal id and is sufficient for the authorization boundary."""
    table = {str(k): str(v) for k, v in tokens.items()}

    def _authenticate(request: Any) -> str:
        headers = getattr(request, "headers", {}) or {}
        principal = headers.get(PRINCIPAL_HEADER)
        token = headers.get(TOKEN_HEADER)
        if not principal or not token:
            raise MailboxAuthError(
                "missing mailbox principal/token headers"
            )
        expected = table.get(principal)
        if expected is None or not hmac.compare_digest(expected, str(token)):
            raise MailboxAuthError("invalid mailbox principal credentials")
        return principal

    return _authenticate
