"""Per-user OIDC token authentication for the MCP server (PACT mcp-oidc-auth).

Before this module the MCP server could authenticate an APPLICATION (the static
bearer of PACT ``mcp-http-auth``) and could trust that application to name a
person in a header (PACT ``mcp-principal-mapping``). It could not verify a
person itself, so every per-user deployment needed a fronting proxy, and an
agent talking to MCP directly had no per-user path at all.

This module is the missing first step, and ONLY that step:

* :class:`OidcTokenVerifier` implements the SDK's ``TokenVerifier``. It checks
  the bearer JWT against the issuer's published keys, the declared issuer, the
  token's validity window, and the declared audience (in ``aud``, or as
  ``azp`` -- Keycloak issues access tokens whose ``aud`` does not always name
  the client they were issued to). Every rejection returns ``None`` and none
  raises, so a missing, malformed, expired, forged or misaddressed token all
  land on the SDK's identical 401.
* :func:`oidc_claim_for_request` turns the verified token into the SAME
  identity-claim shape the header mode builds, stamped by the server. From
  there nothing is new: ``normalize_identity_claim`` checks it against the
  registered provider, subject bindings (role bindings included) resolve it,
  and the principal gate refuses what does not resolve.

What this module deliberately does NOT do: issue tokens, refresh them, or run
a login. The identity provider owns that. The server is an OAuth resource
server; when ``resource_url`` is declared it publishes protected-resource
metadata naming the issuer, so an MCP client can find where to log in.
"""
from __future__ import annotations

import re
import threading
import time
import urllib.parse
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Callable, Mapping, Optional, Sequence

from okg.substrate.errors import SubstrateError

__all__ = [
    "OIDC_CLIENT_ID_PREFIX",
    "OIDC_TOKEN_VERIFICATION_METHOD",
    "OIDC_VERIFIER_ID",
    "ROLES_CLAIM_KEY",
    "OidcAuthConfig",
    "OidcTokenVerifier",
    "RateLimitedJwkClient",
    "build_oidc_auth_settings",
    "build_oidc_token_verifier",
    "fetch_service_token",
    "oidc_auth_from_raw_manifest",
    "require_login_blocks_agree",
    "oidc_auth_audit_exposed_error",
    "oidc_auth_exclusive_error",
    "oidc_auth_from_manifest",
    "oidc_auth_stdio_error",
    "oidc_auth_undeclared_error",
    "oidc_claim_for_request",
    "parse_oidc_auth",
    "validate_oidc_auth_against_providers",
    "verified_token_from_request",
]


#: The ``verified_by`` stamp on every claim this module builds. A provider's
#: ``metadata.trusted_verifiers`` names it to accept OIDC-verified claims; one
#: that trusts only the header mode's transport verifier refuses them.
OIDC_VERIFIER_ID = "mcp-oidc:jwks"

#: Recorded on every claim so the audit trail says HOW it was checked.
OIDC_TOKEN_VERIFICATION_METHOD = "mcp_oidc_bearer_jwt"

#: Prefix of the SDK ``client_id`` for a token THIS verifier accepted. The
#: claim builder reads only tokens carrying it, so a token accepted by any
#: other verifier can never be read as an OIDC identity.
OIDC_CLIENT_ID_PREFIX = "mcp-oidc-client:"

#: The claim key the verified roles are placed under. The provider's
#: ``role_claims`` must include it, which the start-time check enforces.
ROLES_CLAIM_KEY = "roles"

#: Stored in ``AccessToken.token`` instead of the bearer. The SDK never
#: re-reads the field, and keeping the credential out of a model that can be
#: logged or serialized narrows where it can leak.
_ACCESS_TOKEN_PLACEHOLDER = "mcp-oidc-bearer-accepted"

#: Clock skew tolerated on exp/iat/nbf.
DEFAULT_LEEWAY_SECONDS = 60

#: A bearer longer than this is refused before any parsing work.
_MAX_TOKEN_LENGTH = 16384

_OIDC_AUTH_KEYS = frozenset({
    "provider_id",
    "issuer",
    "audience",
    "subject_claim",
    "roles_claim",
    "principal_kind",
    "resource_url",
})

_REQUIRED_OIDC_AUTH_KEYS = (
    "provider_id",
    "issuer",
    "audience",
    "subject_claim",
    "roles_claim",
    "principal_kind",
)

#: Claims every token carries, a service account's included. A subject taken
#: from one of these would let an application's own token act as a person,
#: which is the one thing the service token must never do.
_UNIVERSAL_TOKEN_CLAIMS = frozenset({
    "sub", "iss", "aud", "azp", "exp", "iat", "nbf", "jti", "typ", "scope",
    "client_id", "clientId",
})

#: Claims that mark a token issued to an APPLICATION rather than to a person,
#: and the one value of ``gty`` that says so. A token carrying any of them may
#: authenticate for transport (listing tools) but never becomes a principal,
#: whatever else it carries -- a client-credentials token can hold an `email`
#: or `preferred_username` that satisfies ``subject_claim``. Keycloak stamps
#: service-account tokens with the client's network origin and id; Auth0 names
#: the grant. A token whose subject IS the client (``sub`` equal to ``azp`` or
#: ``client_id``) is the RFC 9068 client-credentials shape.
_APPLICATION_TOKEN_MARKERS = ("clientHost", "clientAddress", "clientId")
_APPLICATION_GRANT_TYPES = frozenset({"client-credentials", "client_credentials"})

#: The provider's error code is echoed only when it has the RFC 6749 shape. A
#: free-text field could carry anything the provider chose to reflect back,
#: including the client secret, so anything else is reported as unrecognized.
_PROVIDER_ERROR_CODE_RE = re.compile(r"^[A-Za-z0-9_.\-]{1,64}$")

#: Minimum seconds between forced key-set refreshes triggered by a key id the
#: cached set does not contain. Shared by every request.
_UNKNOWN_KID_REFRESH_SECONDS = 60.0

#: A dotted claim path, e.g. ``email`` or ``resource_access.my-app.roles``.
_CLAIM_PATH_RE = re.compile(r"^[A-Za-z0-9_:\-]+(?:\.[A-Za-z0-9_:\-]+)*$")

#: A subject value is one printable token: it is what policy binds to.
_SUBJECT_VALUE_RE = re.compile(r"^[\x21-\x7e]{1,512}$")


@dataclass(frozen=True)
class OidcAuthConfig:
    """One deployment's declared OIDC resource-server configuration."""

    provider_id: str
    issuer: str
    audience: str
    subject_claim: str
    roles_claim: str
    principal_kind: str
    resource_url: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "provider_id": self.provider_id,
            "issuer": self.issuer,
            "audience": self.audience,
            "subject_claim": self.subject_claim,
            "roles_claim": self.roles_claim,
            "principal_kind": self.principal_kind,
            "resource_url": self.resource_url,
        }


def parse_oidc_auth(raw: Any) -> OidcAuthConfig:
    """Parse and strictly validate one ``nomos.oidc_auth`` block.

    Strict at every key, like ``nomos.principal_mapping``: a misspelled key is
    a refusal, not a silently defaulted setting.
    """
    if not isinstance(raw, Mapping):
        raise SubstrateError(
            "nomos.oidc_auth must be a mapping",
            data={"got": type(raw).__name__},
            fix_hint=(
                "declare " + ", ".join(_REQUIRED_OIDC_AUTH_KEYS)
                + " under nomos.oidc_auth"
            ),
        )
    unknown = sorted(str(key) for key in set(raw) - _OIDC_AUTH_KEYS)
    if unknown:
        raise SubstrateError(
            "nomos.oidc_auth contains unknown key(s): "
            + ", ".join(repr(key) for key in unknown),
            data={"unknown_keys": unknown, "known_keys": sorted(_OIDC_AUTH_KEYS)},
            fix_hint="remove the key or correct the spelling",
        )
    missing = [
        key for key in _REQUIRED_OIDC_AUTH_KEYS if not str(raw.get(key) or "").strip()
    ]
    if missing:
        raise SubstrateError(
            "nomos.oidc_auth is missing required key(s): "
            + ", ".join(repr(key) for key in missing),
            data={"missing_keys": missing},
            fix_hint=(
                "name the provider, its issuer, the audience tokens must be "
                "issued for, the claims carrying the subject and the roles, "
                "and the principal kind the subject takes"
            ),
        )
    values = {key: str(raw[key]).strip() for key in _REQUIRED_OIDC_AUTH_KEYS}
    issuer = urllib.parse.urlsplit(values["issuer"])
    if issuer.scheme != "https" or not issuer.netloc:
        raise SubstrateError(
            "nomos.oidc_auth.issuer must be an absolute https URL: token "
            "signing keys are fetched from it, and an http issuer lets anyone "
            "on the path substitute their own",
            data={"path": "nomos.oidc_auth.issuer"},
            fix_hint="declare the issuer exactly as its tokens' `iss` claim",
        )
    for key in ("subject_claim", "roles_claim"):
        if not _CLAIM_PATH_RE.match(values[key]):
            raise SubstrateError(
                f"nomos.oidc_auth.{key} {values[key]!r} is not a claim path "
                "(dot-separated names of letters, digits, _ : or -)",
                data={"path": f"nomos.oidc_auth.{key}"},
            )
    if values["subject_claim"] in _UNIVERSAL_TOKEN_CLAIMS:
        raise SubstrateError(
            f"nomos.oidc_auth.subject_claim {values['subject_claim']!r} is carried "
            "by every token, service accounts' included, so an application's own "
            "token would act as a person. Name a claim only a person's token "
            "carries",
            data={
                "path": "nomos.oidc_auth.subject_claim",
                "observed": values["subject_claim"],
            },
            fix_hint="declare e.g. subject_claim: email",
        )
    from okg.substrate.nomos.vocabulary import PRINCIPAL_KINDS

    if values["principal_kind"] not in PRINCIPAL_KINDS:
        raise SubstrateError(
            "nomos.oidc_auth.principal_kind must be one of "
            f"{sorted(PRINCIPAL_KINDS)}; got {values['principal_kind']!r}",
            data={"principal_kind": values["principal_kind"]},
        )
    resource_url: str | None = None
    if raw.get("resource_url") is not None:
        resource_url = str(raw["resource_url"]).strip()
        parts = urllib.parse.urlsplit(resource_url)
        if parts.scheme not in {"http", "https"} or not parts.netloc:
            raise SubstrateError(
                "nomos.oidc_auth.resource_url must be the absolute http(s) URL "
                "MCP clients use to reach this server",
                data={"path": "nomos.oidc_auth.resource_url"},
                fix_hint="declare e.g. https://okg.example.org/mcp",
            )
    return OidcAuthConfig(resource_url=resource_url, **values)


def oidc_auth_from_manifest(manifest: Any) -> OidcAuthConfig | None:
    """Return the deployment's declared OIDC block, or None when absent.

    Absent is ordinary: a deployment that never declared the block keeps the
    no-auth, static-token and header modes exactly as they were.
    """
    nomos_block = getattr(manifest, "nomos", None)
    if not isinstance(nomos_block, Mapping):
        return None
    raw = nomos_block.get("oidc_auth")
    if raw is None:
        return None
    config = parse_oidc_auth(raw)
    posture = nomos_block.get("deployment_posture")
    read_principals = (
        posture.get("read_principals") if isinstance(posture, Mapping) else None
    )
    if read_principals is not None:
        admitted = {str(item) for item in read_principals}
        if config.principal_kind not in admitted:
            raise SubstrateError(
                "nomos.oidc_auth.principal_kind "
                f"{config.principal_kind!r} is not in this deployment's "
                "nomos.deployment_posture.read_principals",
                data={
                    "principal_kind": config.principal_kind,
                    "read_principals": sorted(admitted),
                },
                fix_hint=(
                    "add the kind to read_principals, or declare a kind the "
                    "posture already admits"
                ),
            )
    return config


def validate_oidc_auth_against_providers(
    config: OidcAuthConfig,
    providers: Sequence[Any],
    *,
    deployment_name: str,
) -> Any:
    """Refuse a block the deployment's identity providers cannot back.

    Start-time, so an operator learns at launch -- not at the first request,
    where each of these would read as a policy deny -- that the server could
    never resolve anyone.
    """
    from okg.substrate.nomos.identity import _metadata_bool, _str_tuple

    by_id = {getattr(p, "provider_id", None): p for p in providers}
    provider = by_id.get(config.provider_id)
    if provider is None:
        raise SubstrateError(
            f"nomos.oidc_auth names identity provider {config.provider_id!r}, "
            f"which is not registered for deployment {deployment_name!r}; "
            "refusing to start OIDC authentication against a provider that "
            "does not exist",
            data={
                "provider_id": config.provider_id,
                "deployment_name": deployment_name,
                "registered_provider_ids": sorted(
                    str(key) for key in by_id if key is not None
                ),
            },
            fix_hint=(
                "register the provider with `okg nomos provider add --dsn "
                "<dsn> --deployment <deployment> --for-oidc-auth`, which "
                "reads this block, before enabling --oidc-auth"
            ),
        )
    metadata = getattr(provider, "metadata", None) or {}
    trusted_verifiers = _str_tuple(metadata.get("trusted_verifiers") or ())
    if not trusted_verifiers and not _metadata_bool(metadata.get("require_verified_by")):
        raise SubstrateError(
            f"identity provider {config.provider_id!r} sets neither "
            "metadata.trusted_verifiers nor metadata.require_verified_by, so "
            "the verification stamp this server records would never be "
            "checked; refusing to start OIDC authentication",
            data={"provider_id": config.provider_id},
            fix_hint=f"set metadata.trusted_verifiers: [{OIDC_VERIFIER_ID}]",
        )
    if trusted_verifiers and OIDC_VERIFIER_ID not in trusted_verifiers:
        raise SubstrateError(
            f"identity provider {config.provider_id!r} does not list "
            f"{OIDC_VERIFIER_ID!r} in metadata.trusted_verifiers, so every "
            "OIDC-verified claim would be denied as untrusted",
            data={
                "provider_id": config.provider_id,
                "trusted_verifiers": list(trusted_verifiers),
            },
            fix_hint=f"add {OIDC_VERIFIER_ID} to metadata.trusted_verifiers",
        )
    role_claims = {str(item) for item in (getattr(provider, "role_claims", ()) or ())}
    if ROLES_CLAIM_KEY not in role_claims:
        raise SubstrateError(
            f"identity provider {config.provider_id!r} does not read roles from "
            f"the {ROLES_CLAIM_KEY!r} claim, so the verified roles would never "
            "reach a role binding",
            data={
                "provider_id": config.provider_id,
                "role_claims": sorted(role_claims),
            },
            fix_hint=f"include {ROLES_CLAIM_KEY!r} in the provider's role_claims",
        )
    skew: dict[str, Any] = {}
    if str(getattr(provider, "issuer", "")) != config.issuer:
        skew["issuer"] = {
            "declared": config.issuer,
            "provider": str(getattr(provider, "issuer", "")),
        }
    audiences = {str(item) for item in (getattr(provider, "audiences", ()) or ())}
    if config.audience not in audiences:
        skew["audience"] = {"declared": config.audience, "provider": sorted(audiences)}
    provider_subject_claim = str(getattr(provider, "subject_claim", "") or "")
    if provider_subject_claim and provider_subject_claim != config.subject_claim:
        skew["subject_claim"] = {
            "declared": config.subject_claim,
            "provider": provider_subject_claim,
        }
    if skew:
        raise SubstrateError(
            "nomos.oidc_auth disagrees with the registered identity provider "
            f"{config.provider_id!r}; every request would be denied for a "
            "mismatch that looks like a policy decision",
            data={"provider_id": config.provider_id, "skew": skew},
            fix_hint=(
                "align nomos.oidc_auth with the provider row (issuer, "
                "audience, subject_claim)"
            ),
        )
    return provider


class RateLimitedJwkClient:
    """A JWKS client that cannot be made to refetch keys on demand.

    ``PyJWKClient.get_signing_key`` refetches the provider's whole key set
    whenever a token names a key id the cache does not hold. That id comes from
    the token's header, chosen by whoever sends it, BEFORE any signature is
    checked -- so an unauthenticated caller could make every request download
    the provider's keys, tying up worker threads and flooding the provider.
    The one legitimate reason for a miss is key rotation, so a refresh is kept
    but capped: at most one per interval, shared by all requests. Inside the
    interval an unknown key id is simply a rejection.
    """

    def __init__(
        self,
        client: Any,
        *,
        min_refresh_interval: float = _UNKNOWN_KID_REFRESH_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._client = client
        self._interval = float(min_refresh_interval)
        self._clock = clock
        self._lock = threading.Lock()
        self._last_refresh: float | None = None

    def get_signing_key_from_jwt(self, token: str) -> Any:
        import jwt

        kid = jwt.get_unverified_header(token).get("kid")
        key = self._client.match_kid(self._client.get_signing_keys(), kid)
        if key is not None:
            return key
        with self._lock:
            now = self._clock()
            if self._last_refresh is not None and now - self._last_refresh < self._interval:
                raise LookupError("signing key id not in the cached key set")
            self._last_refresh = now
        key = self._client.match_kid(self._client.get_signing_keys(refresh=True), kid)
        if key is None:
            raise LookupError("signing key id not in the provider's key set")
        return key


class OidcTokenVerifier:
    """Verify an identity provider's bearer JWT, per request.

    Implements the SDK's ``TokenVerifier`` protocol. Key lookup may touch the
    network (the JWKS is cached), so verification runs off the event loop.
    """

    def __init__(
        self,
        config: OidcAuthConfig,
        *,
        algorithms: Sequence[str],
        jwk_client: Any,
        leeway_seconds: int = DEFAULT_LEEWAY_SECONDS,
    ) -> None:
        if not algorithms:
            raise SubstrateError(
                "refusing to build an OIDC token verifier with no permitted "
                "signing algorithm",
            )
        self._config = config
        self._algorithms = tuple(algorithms)
        self._jwk_client = jwk_client
        self._leeway = int(leeway_seconds)

    @property
    def config(self) -> OidcAuthConfig:
        return self._config

    async def verify_token(self, token: str) -> "Optional[Any]":
        try:
            import anyio

            return await anyio.to_thread.run_sync(self.verify_token_sync, token)
        except Exception:  # noqa: BLE001 - a raise would become HTTP 400, not 401
            return None

    def verify_token_sync(self, token: str) -> "Optional[Any]":
        """The whole check, synchronously. Returns None on EVERY rejection."""
        try:
            return self._verify(token)
        except Exception:  # noqa: BLE001 - see verify_token
            return None

    def _verify(self, token: str) -> "Optional[Any]":
        import jwt
        from mcp.server.auth.provider import AccessToken

        if not isinstance(token, str) or not token or len(token) > _MAX_TOKEN_LENGTH:
            return None
        try:
            signing_key = self._jwk_client.get_signing_key_from_jwt(token)
            claims = jwt.decode(
                token,
                signing_key.key,
                algorithms=list(self._algorithms),
                issuer=self._config.issuer,
                options={
                    "require": ["iss", "sub", "exp", "iat"],
                    # Checked below: `aud` OR `azp` may carry the audience.
                    "verify_aud": False,
                },
                leeway=self._leeway,
            )
        except Exception:  # noqa: BLE001 - PyJWT raises a family of errors
            return None
        if not isinstance(claims, dict):
            return None
        audiences = _str_list(claims.get("aud"))
        azp = claims.get("azp") if isinstance(claims.get("azp"), str) else None
        if self._config.audience not in audiences and azp != self._config.audience:
            return None
        # A token that names no person -- a service account's, which is how
        # operator tooling such as `okg chat sync` proves the server answers --
        # still authenticates the CALLER, so it may list tools. It can never
        # become a principal: the claim builder refuses it, so every tool call
        # and resource read it attempts is refused as unidentified. A subject
        # that IS present must still be a single printable token.
        raw_subject = _claim_path_value(claims, self._config.subject_claim)
        subject: str | None = None
        if raw_subject is not None:
            if not isinstance(raw_subject, str) or not _SUBJECT_VALUE_RE.match(
                raw_subject.strip(),
            ):
                return None
            subject = raw_subject.strip()
        roles = _unique(_str_list(_claim_path_value(claims, self._config.roles_claim)))
        scope = claims.get("scope")
        is_application = (
            any(marker in claims for marker in _APPLICATION_TOKEN_MARKERS)
            or str(claims.get("gty") or "").lower() in _APPLICATION_GRANT_TYPES
            or (
                claims.get("sub") is not None
                and claims.get("sub") in {azp, claims.get("client_id")}
            )
        )
        exp = claims.get("exp")
        return AccessToken(
            token=_ACCESS_TOKEN_PLACEHOLDER,
            client_id=f"{OIDC_CLIENT_ID_PREFIX}{azp or 'unnamed'}",
            scopes=scope.split() if isinstance(scope, str) else [],
            expires_at=int(exp) if isinstance(exp, (int, float)) else None,
            subject=str(claims.get("sub")),
            claims={
                "iss": claims.get("iss"),
                "aud": audiences,
                "azp": azp,
                "subject": subject,
                ROLES_CLAIM_KEY: list(roles),
                # Assurance travels with the claim so a provider that requires
                # it (``required_assurance``, ``mfa_required``) can be met.
                "acr": claims.get("acr"),
                "amr": _str_list(claims.get("amr")),
                "auth_time": claims.get("auth_time"),
                "iat": claims.get("iat"),
                "exp": exp,
                "is_application": is_application,
            },
        )


def build_oidc_token_verifier(
    config: OidcAuthConfig,
    *,
    metadata_loader: Callable[[str], Mapping[str, Any]] | None = None,
    jwk_client_factory: Callable[[str], Any] | None = None,
) -> OidcTokenVerifier:
    """Discover the issuer's keys and build the verifier, or refuse to start."""
    from okg.substrate.nomos.oidc_login import (
        ASYMMETRIC_ID_TOKEN_ALGORITHMS,
        OIDCLoginError,
        discover_oidc_metadata,
    )

    loader = metadata_loader or discover_oidc_metadata
    try:
        metadata = loader(config.issuer)
    except OIDCLoginError as exc:
        raise SubstrateError(
            f"OIDC discovery for issuer {config.issuer!r} failed: {exc}; "
            "refusing to start a server that could verify no token",
            data={"issuer": config.issuer, "reason": "discovery_failed"},
            fix_hint="check nomos.oidc_auth.issuer and that the host can reach it",
        ) from exc
    jwks_uri = metadata.get("jwks_uri")
    if not isinstance(jwks_uri, str) or not jwks_uri.startswith("https://"):
        raise SubstrateError(
            f"OIDC issuer {config.issuer!r} publishes no https jwks_uri, so no "
            "token signature could be checked; refusing to start",
            data={"issuer": config.issuer, "reason": "jwks_uri_invalid"},
        )
    if jwk_client_factory is None:
        import jwt

        from okg.substrate.library.sources._url_safety import assert_public_http_url

        assert_public_http_url(jwks_uri)  # SSRF guard on the published key URL

        def jwk_client_factory(uri: str) -> Any:
            return RateLimitedJwkClient(
                jwt.PyJWKClient(uri, cache_jwk_set=True, lifespan=300, timeout=10),
            )

    advertised = metadata.get("id_token_signing_alg_values_supported") or ()
    if isinstance(advertised, str):
        advertised = (advertised,)
    algorithms = tuple(
        str(item) for item in advertised if str(item) in ASYMMETRIC_ID_TOKEN_ALGORITHMS
    ) or tuple(sorted(ASYMMETRIC_ID_TOKEN_ALGORITHMS))
    return OidcTokenVerifier(
        config, algorithms=algorithms, jwk_client=jwk_client_factory(jwks_uri),
    )


def build_oidc_auth_settings(config: OidcAuthConfig) -> Any:
    """``AuthSettings`` for the OIDC resource-server shape.

    With ``resource_url`` declared the SDK publishes protected-resource
    metadata naming the issuer, which is how an MCP client discovers where to
    log in. The audience is checked by the verifier itself, so the SDK's own
    resource check is off rather than left to warn.
    """
    from mcp.server.auth.settings import AuthSettings

    kwargs: dict[str, Any] = {
        "issuer_url": config.issuer,
        "resource_server_url": config.resource_url,
        "required_scopes": None,
    }
    if config.resource_url is not None:
        kwargs["validate_token_resource"] = False
    return AuthSettings(**kwargs)


def verified_token_from_request() -> Any | None:
    """The OIDC-verified access token of the in-flight request, or None.

    Read from the request scope the SDK's auth middleware populated, so a
    caller cannot supply it. A token accepted by any other verifier is not
    returned.
    """
    try:
        from mcp.server.lowlevel.server import request_ctx
    except Exception:  # pragma: no cover - SDK import shape
        return None
    try:
        context = request_ctx.get()
    except LookupError:
        return None
    scope = getattr(getattr(context, "request", None), "scope", None)
    if not isinstance(scope, Mapping):
        return None
    token = getattr(scope.get("user"), "access_token", None)
    client_id = str(getattr(token, "client_id", "") or "")
    if not client_id.startswith(OIDC_CLIENT_ID_PREFIX):
        return None
    if not isinstance(getattr(token, "claims", None), Mapping):
        return None
    return token


def application_may_list_tools(principal_mapping: Any, operator_name: str) -> bool:
    """Whether this request may list tools without resolving a principal.

    An application's verified token (the chat client's client-credentials
    token, which is how ``okg chat sync`` proves an OIDC-mode server answers)
    authenticates the CALLER for transport. It may list the admitted tool
    surface and nothing else: it never resolves a principal, so the gate
    refuses every tool call and resource read it attempts. Only OIDC mode,
    only ``tools/list``, and only a token this server verified as an
    application's -- a person's token, or no token, goes through the
    principal gate as before.
    """
    if operator_name != "tools/list" or not isinstance(principal_mapping, OidcAuthConfig):
        return False
    token = verified_token_from_request()
    return token is not None and bool(token.claims.get("is_application"))


def oidc_claim_for_request(
    config: OidcAuthConfig,
    *,
    access_token: Any | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build the identity claim for one request from its verified token.

    The same shape ``header_claim_for_request`` builds, so the rest of the
    pipeline is shared. Every trust field is stamped HERE by the server.
    """
    from okg.substrate.mcp.principal_mapping import unidentified_principal_error

    token = access_token if access_token is not None else verified_token_from_request()
    claims = getattr(token, "claims", None) if token is not None else None
    if not isinstance(claims, Mapping):
        raise unidentified_principal_error(
            "the request carried no OIDC-verified bearer token, so no "
            "principal could be identified",
            channel="no_verified_token",
            fix_hint=(
                "send `Authorization: Bearer <access token>` issued by the "
                "deployment's identity provider"
            ),
        )
    if claims.get("iss") != config.issuer:
        raise unidentified_principal_error(
            "the verified token was issued by a different issuer than this "
            "server was configured for",
            channel="token_config_skew",
        )
    if claims.get("is_application"):
        raise unidentified_principal_error(
            "the verified token was issued to an application, not to a person, "
            "so it cannot act as one whatever subject claim it carries",
            channel="token_names_no_person",
            fix_hint=(
                "call as a person; an application's service token may list "
                "tools and nothing else"
            ),
        )
    subject = str(claims.get("subject") or "").strip()
    if not subject:
        raise unidentified_principal_error(
            "the verified token names no person (it carries no "
            f"{config.subject_claim!r} claim), so it cannot act as one",
            channel="token_names_no_person",
            fix_hint=(
                "call as a person, with a token that carries the subject claim; "
                "an application's service token may list tools and nothing else"
            ),
        )
    stamped_at = (now or datetime.now(UTC)).astimezone(UTC)
    forwarded: dict[str, Any] = {
        config.subject_claim: subject,
        ROLES_CLAIM_KEY: [str(role) for role in claims.get(ROLES_CLAIM_KEY) or ()],
    }
    if claims.get("acr"):
        forwarded["acr"] = str(claims["acr"])
    if claims.get("amr"):
        forwarded["amr"] = [str(value) for value in claims["amr"]]
    claim = {
        "provider_id": config.provider_id,
        "issuer": config.issuer,
        "audiences": [config.audience],
        "external_subject": subject,
        "claims": forwarded,
        # Explicit, never defaulted: the verifier above checked signature,
        # issuer, validity window and audience before this claim could exist.
        "verification_status": "valid",
        "verified_by": OIDC_VERIFIER_ID,
        "verified_at": stamped_at.isoformat(),
        "verification_method": OIDC_TOKEN_VERIFICATION_METHOD,
        "expires_at": _epoch_to_iso(claims.get("exp")),
    }
    # The time the PERSON authenticated, and only that. `iat` is when the
    # token was minted -- a refresh re-mints it without any new login -- so
    # standing it in would satisfy a freshness requirement the person never
    # met. Absent, a provider demanding freshness fails closed
    # (`missing_auth_time`), which is the correct answer.
    auth_time = _epoch_to_iso(claims.get("auth_time"))
    if auth_time:
        claim["auth_time"] = auth_time
    return claim


def fetch_service_token(
    issuer: str,
    client_id: str,
    client_secret: str,
    *,
    metadata_loader: Callable[[str], Mapping[str, Any]] | None = None,
    post: Callable[..., Any] | None = None,
    timeout_seconds: float = 20.0,
) -> str:
    """A client-credentials access token for operator tooling.

    The token identifies the APPLICATION, not a person. It passes the server's
    transport authentication -- enough to list tools, which is how
    ``okg chat sync`` proves an OIDC-mode server answers -- and can never
    resolve a principal, so no graph read or tool call succeeds with it. The
    provider must allow the client the client-credentials grant. Neither the
    secret nor the token is ever included in an error.
    """
    from okg.substrate.nomos.oidc_login import OIDCLoginError, discover_oidc_metadata

    loader = metadata_loader or discover_oidc_metadata
    try:
        metadata = loader(issuer)
    except OIDCLoginError as exc:
        raise SubstrateError(
            f"OIDC discovery for issuer {issuer!r} failed: {exc}",
            data={"issuer": issuer, "reason": "discovery_failed"},
        ) from exc
    endpoint = metadata.get("token_endpoint")
    if not isinstance(endpoint, str) or not endpoint.startswith("https://"):
        raise SubstrateError(
            f"OIDC issuer {issuer!r} publishes no https token_endpoint, so no "
            "service token can be requested",
            data={"issuer": issuer, "reason": "token_endpoint_invalid"},
        )
    if post is None:
        import requests

        from okg.substrate.library.sources._url_safety import assert_public_http_url

        assert_public_http_url(endpoint)  # SSRF guard on the published endpoint
        post = requests.post
    try:
        response = post(
            endpoint,
            data={"grant_type": "client_credentials"},
            auth=(client_id, client_secret),
            timeout=timeout_seconds,
            allow_redirects=False,
        )
    except Exception as exc:  # noqa: BLE001 - reported by type, never by text
        raise SubstrateError(
            f"the service token request to {endpoint!r} failed "
            f"({type(exc).__name__})",
            data={"issuer": issuer, "reason": "token_request_failed"},
        ) from None
    try:
        body = response.json()
    except ValueError:
        body = {}
    body = body if isinstance(body, Mapping) else {}
    token = body.get("access_token")
    status = getattr(response, "status_code", None)
    if status != 200 or not isinstance(token, str) or not token:
        raw_error = body.get("error")
        error = (
            raw_error
            if isinstance(raw_error, str)
            and _PROVIDER_ERROR_CODE_RE.match(raw_error)
            and client_secret not in raw_error
            else ("unrecognized" if raw_error else None)
        )
        raise SubstrateError(
            f"the identity provider refused a service token for {client_id!r} "
            f"(HTTP {status}{': ' + error if error else ''})",
            data={
                "issuer": issuer,
                "client_id": client_id,
                "reason": "service_token_refused",
                "error": error,
            },
            fix_hint=(
                "allow the client-credentials grant for this client at the "
                "identity provider"
            ),
        )
    return token


def oidc_auth_from_raw_manifest(raw_manifest: Mapping[str, Any]) -> OidcAuthConfig | None:
    """:func:`oidc_auth_from_manifest` for a manifest held as a plain mapping."""
    nomos_block = raw_manifest.get("nomos") if isinstance(raw_manifest, Mapping) else None

    class _View:
        nomos = nomos_block

    return oidc_auth_from_manifest(_View())


def require_login_blocks_agree(
    oidc_config: OidcAuthConfig | None,
    chat_oidc_login: Any | None,
    *,
    deployment: str,
) -> None:
    """Refuse when ``chat.oidc_login`` and ``nomos.oidc_auth`` describe
    different providers.

    The chat signs people in and forwards their tokens; ``mcp-serve`` verifies
    those same tokens. The two blocks therefore name ONE issuer, ONE client
    (the chat's client is the server's audience) and ONE roles claim. A skew
    lets people sign in and then refuses every request they make, which reads
    as an access-control decision rather than a configuration error -- so it
    refuses at start instead. Either block alone is not checked.
    """
    if oidc_config is None or chat_oidc_login is None:
        return
    skew: dict[str, Any] = {}
    if oidc_config.issuer.rstrip("/") != str(chat_oidc_login.issuer).rstrip("/"):
        skew["issuer"] = {
            "nomos.oidc_auth": oidc_config.issuer,
            "chat.oidc_login": chat_oidc_login.issuer,
        }
    if oidc_config.audience != chat_oidc_login.client_id:
        skew["client"] = {
            "nomos.oidc_auth.audience": oidc_config.audience,
            "chat.oidc_login.client_id": chat_oidc_login.client_id,
        }
    if oidc_config.roles_claim != chat_oidc_login.roles_claim:
        skew["roles_claim"] = {
            "nomos.oidc_auth": oidc_config.roles_claim,
            "chat.oidc_login": chat_oidc_login.roles_claim,
        }
    if not skew:
        return
    err = SubstrateError(
        f"deployment {deployment!r} declares chat.oidc_login and "
        "nomos.oidc_auth for different providers or clients: "
        + ", ".join(sorted(skew))
        + ". People would sign in to the chat and then be refused on every "
        "request",
        data={
            "reason": "oidc_login_blocks_disagree",
            "deployment": deployment,
            "skew": skew,
        },
        fix_hint=(
            "use one issuer, one client (chat.oidc_login.client_id equal to "
            "nomos.oidc_auth.audience) and one roles_claim in both blocks"
        ),
    )
    err.code = "mcp_oidc_login_blocks_disagree"
    raise err


def oidc_auth_stdio_error() -> SubstrateError:
    err = SubstrateError(
        "--oidc-auth is incompatible with --transport stdio: stdio carries no "
        "HTTP request and therefore no bearer token. The stdio surface is "
        "already single-operator -- its caller is the process that launched it.",
        data={"transport": "stdio"},
        fix_hint="serve with --transport streamable-http",
    )
    err.code = "mcp_oidc_auth_stdio_incompatible"
    return err


def oidc_auth_exclusive_error(other_flag: str) -> SubstrateError:
    err = SubstrateError(
        f"--oidc-auth cannot be combined with {other_flag}: one server has "
        "exactly one authentication mode, and OIDC mode already resolves a "
        "per-request principal from the verified token",
        data={"conflicting_flag": other_flag},
        fix_hint=f"drop {other_flag}",
    )
    err.code = "mcp_oidc_auth_exclusive"
    return err


def oidc_auth_audit_exposed_error(
    deployment_name: str, *, host: str, mode: str = "audit",
) -> SubstrateError:
    err = SubstrateError(
        f"--oidc-auth was requested for deployment {deployment_name!r}, whose "
        f"Nomos runtime enforcement is `{mode}`, on non-loopback host "
        f"{host!r}. Users no binding admits are still refused, but only "
        "`enforce` withholds the rows a user's grants do not cover: under "
        "`audit` a denial is recorded and the rows are served anyway, and "
        "under `off` nothing is filtered. Every admitted user would read "
        "everything; refusing to start",
        data={
            "deployment_name": deployment_name,
            "host": host,
            "runtime_enforcement": mode,
        },
        fix_hint=(
            "set nomos.runtime_enforcement: enforce in the deployment "
            "manifest, or bind --host 127.0.0.1 for a local test"
        ),
    )
    err.code = "mcp_oidc_auth_audit_exposed"
    return err


def oidc_auth_undeclared_error(deployment_name: str) -> SubstrateError:
    err = SubstrateError(
        f"--oidc-auth was requested for deployment {deployment_name!r}, which "
        "declares no nomos.oidc_auth block, so there is no issuer to verify "
        "tokens against; refusing to start",
        data={"deployment_name": deployment_name},
        fix_hint=(
            "declare nomos.oidc_auth (" + ", ".join(_REQUIRED_OIDC_AUTH_KEYS)
            + ") in the deployment manifest"
        ),
    )
    err.code = "mcp_oidc_auth_undeclared"
    return err


def _claim_path_value(claims: Mapping[str, Any], path: str) -> Any:
    """Walk a dotted claim path; a flat key containing dots wins if present."""
    if path in claims:
        return claims[path]
    value: Any = claims
    for part in path.split("."):
        if not isinstance(value, Mapping) or part not in value:
            return None
        value = value[part]
    return value


def _str_list(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value] if value.strip() else []
    if isinstance(value, (list, tuple)):
        return [str(item) for item in value if isinstance(item, str) and item.strip()]
    return []


def _unique(values: Sequence[str]) -> tuple[str, ...]:
    seen: dict[str, None] = {}
    for value in values:
        seen.setdefault(value, None)
    return tuple(seen)


def _epoch_to_iso(value: Any) -> str | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return datetime.fromtimestamp(float(value), UTC).isoformat()
