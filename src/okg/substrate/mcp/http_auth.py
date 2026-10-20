"""Bearer authentication for the streamable-HTTP MCP transport.

PACT ``mcp-http-auth``. Before this module the MCP HTTP endpoint could not tell
one caller from another, so the substrate compensated with network topology:
``_write_transport_guard`` refused the write surface on non-loopback binds and
merely *warned* on loopback. That is a proxy for identity, not identity. This
module supplies the real check at the transport boundary.

Shape (deliberately the smallest thing that authenticates):

* A **static shared secret** presented as ``Authorization: Bearer <token>``.
  There is no authorization server, no token issuance, no refresh. The
  ``AuthSettings`` this module builds therefore carries
  ``resource_server_url=None`` so the SDK publishes **no** unauthenticated
  ``/.well-known/oauth-protected-resource`` metadata route, and its
  ``issuer_url`` is a documented non-resolvable sentinel rather than a real
  endpoint (nothing dereferences it: no ``auth_server_provider`` is configured
  and no protected-resource route is published).

* The token **value** never appears in argv. ``--auth-token-env`` names an
  environment *variable*; :func:`resolve_auth_token` reads the value from the
  environment and fails closed when the variable is unset, empty, or padded
  with whitespace. A configured-but-unresolvable credential refuses to start
  rather than starting unauthenticated.

* :meth:`StaticTokenVerifier.verify_token` compares with
  :func:`hmac.compare_digest` and returns ``None`` on **every** rejection. It
  must never raise: the SDK's ``AuthenticationMiddleware`` converts a raised
  ``AuthenticationError`` into HTTP 400, which would break the 401 contract and
  make a malformed header distinguishable from a wrong token.

In the pinned ``mcp`` SDK (1.28.1) the token verifier is a **constructor-only**
``FastMCP`` argument -- ``FastMCP.settings`` exposes ``auth`` but not
``token_verifier``, and ``streamable_http_app()`` wraps the ``/mcp`` route in
``RequireAuthMiddleware`` only when ``self._token_verifier`` is set. So auth
cannot be switched on by mutating settings after construction; it is wired in
``build_server``. :func:`server_has_token_verifier` reads that constructed
state, and :func:`mcp_route_is_auth_wrapped` reads the served ASGI app -- the
two checks a caller needs to assert that auth is *active*, never that a flag
was passed or a credential merely resolved.
"""

from __future__ import annotations

import hmac
import os
import re
from typing import TYPE_CHECKING, Any, Mapping, Optional

from okg.substrate.errors import SubstrateError

if TYPE_CHECKING:  # pragma: no cover - typing only
    from mcp.server.auth.provider import AccessToken
    from mcp.server.auth.settings import AuthSettings


__all__ = [
    "AUTH_TOKEN_MIN_BYTES",
    "AUTH_TOKEN_GUIDANCE",
    "StaticTokenVerifier",
    "build_auth_settings",
    "mcp_route_is_auth_wrapped",
    "resolve_auth_token",
    "server_has_token_verifier",
    "stdio_auth_incompatibility_error",
]


#: Operator guidance, not an enforced floor. A shared secret defends only as
#: well as its entropy; 32 bytes is the minimum this surface documents.
AUTH_TOKEN_MIN_BYTES = 32

AUTH_TOKEN_GUIDANCE = (
    f"generate at least {AUTH_TOKEN_MIN_BYTES} bytes of randomness, e.g. "
    f"`export OKG_MCP_AUTH_TOKEN=$(openssl rand -hex {AUTH_TOKEN_MIN_BYTES})`, "
    "then pass the VARIABLE NAME: --auth-token-env OKG_MCP_AUTH_TOKEN"
)

#: POSIX portable environment-variable name. This is a *structural* check on
#: the flag's argument (which is a variable name), NOT a
#: does-this-value-look-like-a-credential heuristic applied to a value -- the
#: PACT forbids the latter. It happens to reject most literal tokens pasted
#: into the flag, which is a bonus, not the mechanism: the mechanism is the
#: explicit ``--auth-token`` refusal in the CLI.
_ENV_VAR_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

#: ``AuthSettings.issuer_url`` is a required ``AnyHttpUrl``. With a static
#: shared secret there IS no issuer: no authorization-server routes are
#: registered (that needs ``auth_server_provider``) and no protected-resource
#: metadata is published (that needs ``resource_server_url``). A reserved,
#: non-resolvable ``.invalid`` name states that honestly instead of naming an
#: endpoint that does not exist.
STATIC_TOKEN_ISSUER_URL = "https://static-bearer.okg.invalid/"

#: Identity reported for a caller that presented the shared secret. There is
#: exactly one such principal; it is surfaced into the MCP request context as
#: the client id, so it must be a non-secret label.
STATIC_TOKEN_CLIENT_ID = "okg-mcp-static-bearer"

#: Placeholder stored in the returned ``AccessToken.token`` field. Nothing in
#: the SDK re-reads it (the transport keys on ``client_id``/``scopes``), and
#: keeping the real secret out of a model that could be logged or serialized
#: narrows the leak surface the PACT's no-leak requirement covers.
_ACCESS_TOKEN_PLACEHOLDER = "okg-mcp-static-bearer-accepted"


class StaticTokenVerifier:
    """Verify a single operator-supplied shared secret.

    Implements the SDK's ``TokenVerifier`` protocol. Every rejection returns
    ``None`` and no rejection raises, so a missing header, a malformed header,
    a wrong token, and a non-UTF-8 token all land on the SDK's identical
    401 + ``WWW-Authenticate`` path and are indistinguishable by the response.
    """

    def __init__(self, token: str) -> None:
        if not isinstance(token, str) or not token.strip():
            # Defensive: resolve_auth_token already rejects these. A verifier
            # built around an empty secret would authenticate the empty string.
            raise SubstrateError(
                "refusing to build an MCP token verifier around an empty "
                "bearer credential",
                fix_hint=AUTH_TOKEN_GUIDANCE,
            )
        self._expected = token.encode("utf-8")

    async def verify_token(self, token: str) -> "Optional[AccessToken]":
        from mcp.server.auth.provider import AccessToken

        try:
            candidate = token.encode("utf-8")
        except Exception:  # pragma: no cover - str.encode on a str
            return None
        try:
            if not hmac.compare_digest(candidate, self._expected):
                return None
        except Exception:  # pragma: no cover - compare_digest type guard
            return None
        return AccessToken(
            token=_ACCESS_TOKEN_PLACEHOLDER,
            client_id=STATIC_TOKEN_CLIENT_ID,
            scopes=[],
        )


def build_auth_settings() -> "AuthSettings":
    """``AuthSettings`` for the static shared-secret shape.

    ``resource_server_url=None`` is load-bearing: it keeps the SDK from
    publishing an unauthenticated protected-resource metadata route, and keeps
    the ``WWW-Authenticate`` challenge free of any server-identifying
    ``resource_metadata`` pointer.
    """
    from mcp.server.auth.settings import AuthSettings

    return AuthSettings(
        issuer_url=STATIC_TOKEN_ISSUER_URL,  # type: ignore[arg-type]
        resource_server_url=None,
        required_scopes=None,
    )


def resolve_auth_token(
    env_var_name: str,
    *,
    environ: Optional[Mapping[str, str]] = None,
    surface: str = "MCP",
) -> str:
    """Resolve the bearer credential named by ``--auth-token-env``.

    ``surface`` names the server in the refusal text (``"MCP"`` by default;
    the viz read API passes its own name) and changes nothing else.

    Fail-closed: a configured-but-unresolvable credential raises rather than
    letting the caller start an unauthenticated server. The raised message and
    ``data`` payload name the VARIABLE, never the value.
    """
    env = os.environ if environ is None else environ
    name = "" if env_var_name is None else str(env_var_name)
    if not _ENV_VAR_NAME_RE.match(name):
        # The rejected argument is NOT echoed. The likeliest way to land here
        # is `--auth-token-env $OKG_MCP_AUTH_TOKEN` -- shell habit expanding
        # the variable the flag wanted named -- in which case the argument IS
        # the token, and repeating it in a message or a JSON error payload
        # would copy it into logs, CI output, and evidence artifacts.
        raise SubstrateError(
            "--auth-token-env takes the NAME of an environment variable "
            "holding the bearer token, and the value supplied is not a valid "
            "environment-variable name (letters, digits and underscore, not "
            "starting with a digit). The value is withheld from this message "
            "because it may itself be the token",
            data={"auth_token_env": "<withheld>", "reason": "not_a_var_name"},
            fix_hint=(
                "pass the variable NAME, unexpanded: --auth-token-env "
                "OKG_MCP_AUTH_TOKEN (not $OKG_MCP_AUTH_TOKEN). argv is "
                f"readable by other local processes. {AUTH_TOKEN_GUIDANCE}"
            ),
        )
    raw = env.get(name)
    if raw is None:
        raise SubstrateError(
            f"{surface} bearer authentication was requested from environment "
            f"variable {name!r}, but that variable is not set; refusing to "
            "start unauthenticated",
            data={"auth_token_env": name, "reason": "unset"},
            fix_hint=AUTH_TOKEN_GUIDANCE,
        )
    if not raw.strip():
        raise SubstrateError(
            f"{surface} bearer authentication was requested from environment "
            f"variable {name!r}, but that variable is empty or whitespace-"
            "only; refusing to start unauthenticated",
            data={"auth_token_env": name, "reason": "empty"},
            fix_hint=AUTH_TOKEN_GUIDANCE,
        )
    if raw != raw.strip():
        # An HTTP header value cannot carry leading/trailing whitespace, so a
        # padded variable (the classic `TOK=$(cat file)` trailing newline)
        # yields a server nobody can authenticate against. Silently stripping
        # would make the accepted token differ from the configured one; refuse
        # instead and name the problem.
        raise SubstrateError(
            f"{surface} bearer authentication was requested from environment "
            f"variable {name!r}, but its value has leading or trailing "
            "whitespace, which cannot be sent in an Authorization header",
            data={"auth_token_env": name, "reason": "padded"},
            fix_hint=(
                "set the variable to the exact token with no surrounding "
                f"whitespace (e.g. `$(openssl rand -hex "
                f"{AUTH_TOKEN_MIN_BYTES})`, not `$(cat token.txt)`)"
            ),
        )
    return raw


def server_has_token_verifier(mcp: Any) -> bool:
    """Is a token verifier installed on this constructed FastMCP instance?

    This is the auth-active signal the write transport guard consumes. It reads
    the CONSTRUCTED server, so a flag that was passed but never wired, or a
    credential that resolved but never reached ``FastMCP(...)``, reads as
    inactive -- which is the fail-closed direction.
    """
    return getattr(mcp, "_token_verifier", None) is not None


def mcp_route_is_auth_wrapped(app: Any) -> bool:
    """Is the served app's ``/mcp`` route wrapped in the SDK auth middleware?

    The strongest available observation: it inspects the actual ASGI app the
    transport serves, not the server object that produced it.
    """
    from mcp.server.auth.middleware.bearer_auth import RequireAuthMiddleware

    for route in getattr(app, "routes", ()) or ():
        endpoint = getattr(route, "endpoint", None)
        if isinstance(endpoint, RequireAuthMiddleware):
            return True
        # Starlette wraps a bare-callable endpoint in a request-response
        # adapter; unwrap one level so this check is not shape-fragile.
        wrapped = getattr(endpoint, "__wrapped__", None)
        if isinstance(wrapped, RequireAuthMiddleware):  # pragma: no cover
            return True
    return False


def stdio_auth_incompatibility_error(env_var_name: str) -> SubstrateError:
    """The explicit refusal for ``--auth-token-env`` + ``--transport stdio``.

    Never silently ignored: an operator who asked for authentication and got a
    server that does not authenticate has been told the opposite of the truth.
    """
    return SubstrateError(
        "--auth-token-env is incompatible with --transport stdio: stdio "
        "carries no HTTP request and therefore no Authorization header, so a "
        "bearer credential cannot be checked. The stdio surface is already "
        "reachable only by the local operator who launched the subprocess.",
        data={"auth_token_env": str(env_var_name), "transport": "stdio"},
        fix_hint=(
            "drop --auth-token-env for stdio, or serve with --transport "
            "streamable-http where the credential is checked at the "
            "transport boundary"
        ),
    )
