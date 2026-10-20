"""OAuth 2.0 infrastructure for substrate connectors.

Provider-agnostic authorization-code flow with encrypted-at-rest
token storage (via OS keychain), automatic refresh-token
rotation, and a redaction layer. Used by Tier-C connectors
(`gmail_api`, `google_calendar_api`, `slack_api`) per
`refine-academic-pi-with-comms-and-calendar`.

Public API:

    from okg.substrate.library.oauth import (
        OAuthFlow, TokenStore, Redactor,
    )
    from okg.substrate.library.oauth.providers import (
        GMAIL, GOOGLE_CALENDAR, SLACK,
    )

Operator CLI:
    okg oauth login <provider>
    okg oauth status
    okg oauth logout <provider>

Trust model: operator's machine is trusted. Tokens at rest live
in the OS keychain (macOS Keychain / Linux Secret Service via
`keyring`). Inappropriate for shared cluster deployments.
"""
from okg.substrate.library.oauth.flow import OAuthFlow
from okg.substrate.library.oauth.token_store import TokenStore
from okg.substrate.library.oauth.redaction import Redactor

__all__ = ["OAuthFlow", "TokenStore", "Redactor"]
