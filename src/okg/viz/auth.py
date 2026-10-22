"""Operator authentication + CSRF protection for the viz mutating routes.

A single custom-header bearer token gates every operator-facing mutating viz
route. Because it is a CUSTOM request header (not an ambient cookie), it is
inherently CSRF-safe: a cross-origin page cannot read it (CORS) and the browser
will not auto-attach it, so the same token serves three boundaries at once —
authentication, authorization (the token holder is the operator), and the
non-browser-credential CSRF defense (secure-viz-operator-routes).

The token is OPT-IN config (OKG_VIZ_OPERATOR_TOKEN / build_app param). When no
token is configured the server preserves the local-dev loopback-trust behavior;
the listener-exposure doctor check enforces that a NON-loopback bind must be
auth-gated, so the insecure case (exposed + unauthenticated) is caught.
"""
from __future__ import annotations

import base64
import binascii
import hmac
import os

#: Request header carrying the operator credential.
OPERATOR_TOKEN_HEADER = "x-okg-operator-token"
#: Env var holding the configured operator token.
OPERATOR_TOKEN_ENV = "OKG_VIZ_OPERATOR_TOKEN"

# Operator-control transitions are deliberately a separate closed auth set.
# They are listed in the general mutation inventory too for defense-in-depth,
# but unlike the existing local-development mutation policy they always
# require the explicitly configured Viz operator token, including loopback and
# ASGI in-process requests.
OPERATOR_CONTROL_PATHS = frozenset({
    "/viz/api/coordination/control/bootstrap",
    "/viz/api/coordination/control/fallback",
    "/viz/api/coordination/control/reactivation",
})

# Approval decisions are intentionally not setup-control transitions.  Keep
# this second closed set explicit so adding a future coordination endpoint
# cannot accidentally inherit approval authority by prefix matching.
OPERATOR_APPROVAL_PATHS = frozenset({
    "/viz/api/coordination/approval/decide",
    "/viz/api/coordination/approval/revoke",
})

# Setup planning is a separate, authenticated, zero-write surface.  It is not
# another operator-control operation: the control operation set remains the
# three bootstrap/fallback/reactivation transitions below.
OPERATOR_SETUP_PLAN_PATHS = frozenset({
    "/viz/api/coordination/setup/plan",
})

# The only subject/session pair that the native operator adapters may receive.
# These values are stamped by the authenticated transport, never read from a
# request body or caller-provided credential.
OPERATOR_PRINCIPAL = "operator:viz-token"
OPERATOR_SESSION_ID = "operator-control"

#: Exact mutating POST paths that require operator auth.
#:
#: The operator-local chat POST path is deliberately absent: that surface was
#: retired by PACT ``retire-viz-chat`` (archived under
#: ``archive/chat/viz-chat-2026-08-04/``, now only in git at commit
#: ``aa9a7eed8015``). Listing a path no router serves
#: would make this set read as broader coverage than it has.
_MUTATING_POST_PATHS = frozenset({
    "/viz/api/repin",
    "/viz/api/console/pact-review",
    "/viz/api/portal/lens-putback",
    # Also classify the dedicated controls as POST mutations for the
    # defense-in-depth route inventory.  ``operator_auth`` checks
    # ``is_operator_control_route`` first, so these never inherit the generic
    # loopback-trust branch.
    *OPERATOR_CONTROL_PATHS,
    *OPERATOR_APPROVAL_PATHS,
    # The setup plan itself is zero-write, but this inventory is also the
    # defense-in-depth POST auth inventory; classify it so adding the route
    # cannot accidentally bypass the middleware on a no-token deployment.
    *OPERATOR_SETUP_PLAN_PATHS,
})
#: (prefix, suffix) patterns for parametrized mutating POST routes.
_MUTATING_POST_PATTERNS = (
    ("/viz/api/portal/lens-overlays/", "/revoke"),
    ("/viz/api/views/", "/recheck"),
    ("/viz/api/tools/", ""),
)


def resolve_operator_token(explicit: str | None = None) -> str | None:
    """The configured operator token: explicit arg, else OKG_VIZ_OPERATOR_TOKEN."""
    if explicit:
        return explicit
    tok = (os.environ.get(OPERATOR_TOKEN_ENV) or "").strip()
    return tok or None


def is_mutating_route(method: str, path: str) -> bool:
    """True if (method, path) is an operator-facing mutating viz route."""
    if method.upper() != "POST":
        return False
    if path in _MUTATING_POST_PATHS:
        return True
    for prefix, suffix in _MUTATING_POST_PATTERNS:
        if (
            path.startswith(prefix)
            and path.endswith(suffix)
            and len(path) > len(prefix) + len(suffix)
        ):
            return True
    return False


def is_operator_control_route(method: str, path: str) -> bool:
    """Return true only for the three dedicated operator transitions.

    This intentionally has no prefix matching.  A future route below
    ``/coordination/control`` must be opted into explicitly rather than
    inheriting a privileged transition by accident.
    """
    return method.upper() == "POST" and path in OPERATOR_CONTROL_PATHS


def is_operator_approval_route(method: str, path: str) -> bool:
    """Return true only for the two dedicated approval transitions."""
    return method.upper() == "POST" and path in OPERATOR_APPROVAL_PATHS


def is_operator_setup_plan_route(method: str, path: str) -> bool:
    """Return true only for the authenticated, zero-write setup plan route."""
    return method.upper() == "POST" and path in OPERATOR_SETUP_PLAN_PATHS


def token_matches(presented: str | None, configured: str) -> bool:
    """Constant-time comparison of a presented token against the configured one.

    Compares UTF-8 bytes: ``hmac.compare_digest`` raises on a non-ASCII
    ``str``, which would turn a stray header byte into a server error instead
    of a refusal.
    """
    if not presented or not configured:
        return False
    return hmac.compare_digest(
        str(presented).encode("utf-8", "surrogatepass"),
        str(configured).encode("utf-8", "surrogatepass"),
    )


# ---------------------------------------------------------------------------
# Read API authentication (issue #3278)
#
# The operator token above gates only the mutating routes. Every GET under
# /viz/api/* served graph data to anyone who could reach the port, and on a
# shared host binding 127.0.0.1 does not help: every local account reaches
# loopback. ``--auth-token-env VARIABLE_NAME`` (the same flag and the same
# fail-closed resolver as ``okg mcp-serve``) turns on a second gate: every
# request under /viz/api/ must present that token, or it gets 401 before any
# route, cache, or database read runs.
# ---------------------------------------------------------------------------

#: Realm named in the 401 challenge of a gated read.
API_AUTH_REALM = "okg-viz"
#: Path prefix every read-API route lives under.
API_PATH_PREFIX = "/viz/api"


def is_api_path(path: str) -> bool:
    """True for ``/viz/api`` and everything below it."""
    return path == API_PATH_PREFIX or path.startswith(API_PATH_PREFIX + "/")


def presented_api_token(authorization: str | None) -> str | None:
    """The credential in an ``Authorization`` header, or None.

    Accepts ``Bearer <token>`` (scripts, curl, the MCP convention) and
    ``Basic base64(<any user>:<token>)``, the form a browser sends after its
    own sign-in prompt, so the operator UI works without a login page. Every
    malformed header yields None, never an exception.
    """
    if not authorization:
        return None
    scheme, _, value = authorization.strip().partition(" ")
    scheme = scheme.lower()
    value = value.strip()
    if not value:
        return None
    if scheme == "bearer":
        return value
    if scheme == "basic":
        try:
            decoded = base64.b64decode(value, validate=True).decode("utf-8")
        except (binascii.Error, ValueError):
            return None
        _user, sep, password = decoded.partition(":")
        return password if sep and password else None
    return None


def api_request_authorized(authorization: str | None, configured: str) -> bool:
    """Does this ``Authorization`` header carry the configured read token?"""
    return token_matches(presented_api_token(authorization), configured)


def resolve_api_token(env_var_name: str | None) -> str | None:
    """Resolve the read-API token named by ``--auth-token-env``.

    None when the flag was not given (today's behavior). Otherwise the MCP
    resolver decides, so the rules match ``okg mcp-serve`` exactly: the
    argument must be a variable NAME, and an unset, empty, or padded variable
    raises ``SubstrateError`` naming the variable, never its value. A
    configured-but-unresolvable token refuses to start rather than serving
    unauthenticated.
    """
    if env_var_name is None:
        return None
    from okg.substrate.mcp.http_auth import resolve_auth_token

    return resolve_auth_token(env_var_name, surface="viz read API")


def require_read_auth_for_sensitive_deployment(
    deployment: str,
    api_token: str | None,
    *,
    surface: str,
) -> None:
    """Refuse an unauthenticated start for a deployment that declares policy.

    A deployment whose manifest turns Nomos runtime enforcement on
    (``nomos.runtime_enforcement`` is ``audit`` or ``enforce`` in the manifest;
    an ``OKG_NOMOS_RUNTIME_ENFORCEMENT`` override is ignored here, see
    ``deployment_requires_read_auth``) has declared its graph sensitive.
    Serving it with no read token would bypass that policy for every local
    account, so the start is refused even on loopback. A manifest that cannot
    be read or parsed also refuses: the mode is unknown, never assumed off.
    No-op when a token is configured.
    """
    if api_token:
        return
    from okg.substrate.errors import VizReadAuthRequired
    from okg.substrate.nomos.runtime_enforcement import (
        NomosEnforcementConfigError,
        NomosEnforcementResolutionError,
        deployment_requires_read_auth,
    )

    flag_hint = (
        "set a token in an environment variable and pass its NAME: "
        "--auth-token-env VARIABLE_NAME"
    )
    try:
        sensitive = deployment_requires_read_auth(deployment)
    except (NomosEnforcementConfigError, NomosEnforcementResolutionError) as exc:
        raise VizReadAuthRequired(
            f"{surface} refused: cannot tell whether deployment {deployment!r} "
            f"is sensitive ({exc}); without --auth-token-env it will not start",
            data={"deployment": deployment, "reason": "sensitivity_unknown"},
            fix_hint=flag_hint,
        ) from exc
    if sensitive:
        raise VizReadAuthRequired(
            f"{surface} refused: deployment {deployment!r} has Nomos runtime "
            "enforcement on (nomos.runtime_enforcement is audit or enforce), so "
            "its graph is sensitive and every read must be authenticated; "
            "start it with --auth-token-env VARIABLE_NAME",
            data={"deployment": deployment, "reason": "sensitive_deployment"},
            fix_hint=flag_hint,
        )
