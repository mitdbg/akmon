"""PACT ``mcp-oidc-auth`` -- per-user OIDC bearer tokens on the MCP surface.

Hermetic. Tokens are signed with RSA keys generated for this module and served
through a fake JWKS client, so nothing here reaches the network or a database.
The verifier arms decode REAL signatures: a fake that skipped the signature
would let a forged token pass these tests.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import time
from types import SimpleNamespace
from typing import Any, Iterator

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa

from okg.substrate.errors import SubstrateError
from okg.substrate.mcp.oidc_auth import (
    application_may_list_tools,
    OIDC_CLIENT_ID_PREFIX,
    OIDC_TOKEN_VERIFICATION_METHOD,
    OIDC_VERIFIER_ID,
    ROLES_CLAIM_KEY,
    OidcAuthConfig,
    OidcTokenVerifier,
    build_oidc_auth_settings,
    build_oidc_token_verifier,
    fetch_service_token,
    oidc_auth_from_manifest,
    oidc_claim_for_request,
    parse_oidc_auth,
    validate_oidc_auth_against_providers,
    verified_token_from_request,
)
from okg.substrate.mcp.principal_mapping import UNIDENTIFIED_PRINCIPAL_REASON
from okg.substrate.nomos.identity import (
    IdentityProvider,
    SubjectBinding,
    normalize_identity_claim,
)

ISSUER = "https://auth.example.org/realms/team"
AUDIENCE = "okg-chat"
PROVIDER_ID = "team-sso"
SCOPE = "deployment:default"
SUBJECT = "alice@example.org"
ROLES_PATH = f"resource_access.{AUDIENCE}.roles"

CONFIG = OidcAuthConfig(
    provider_id=PROVIDER_ID,
    issuer=ISSUER,
    audience=AUDIENCE,
    subject_claim="email",
    roles_claim=ROLES_PATH,
    principal_kind="human_operator",
)

_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
_OTHER_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
_DROP = object()


class _FakeJwkClient:
    """Hands back one public key, the way PyJWKClient does after a JWKS fetch."""

    def __init__(self, key: Any = _KEY) -> None:
        self._public = key.public_key()

    def get_signing_key_from_jwt(self, token: str) -> Any:
        return SimpleNamespace(key=self._public)


class _FailingJwkClient:
    def get_signing_key_from_jwt(self, token: str) -> Any:
        raise RuntimeError("JWKS endpoint unreachable")


def _claims(**overrides: Any) -> dict[str, Any]:
    """A Keycloak-shaped access token: roles nested under resource_access."""
    now = int(time.time())
    claims: dict[str, Any] = {
        "iss": ISSUER,
        "sub": "f:0001:alice",
        "aud": [AUDIENCE, "account"],
        "azp": AUDIENCE,
        "iat": now,
        "exp": now + 300,
        "auth_time": now - 30,
        "email": SUBJECT,
        "resource_access": {AUDIENCE: {"roles": ["default-role", "archi-admins"]}},
        "scope": "openid email profile",
    }
    for key, value in overrides.items():
        if value is _DROP:
            claims.pop(key, None)
        else:
            claims[key] = value
    return claims


def _token(claims: dict[str, Any] | None = None, *, key: Any = _KEY) -> str:
    return jwt.encode(
        claims if claims is not None else _claims(),
        key,
        algorithm="RS256",
        headers={"kid": "test"},
    )


def _verifier(jwk_client: Any = None) -> OidcTokenVerifier:
    return OidcTokenVerifier(
        CONFIG, algorithms=("RS256",), jwk_client=jwk_client or _FakeJwkClient(),
    )


def _verify(token: Any, verifier: OidcTokenVerifier | None = None) -> Any:
    return asyncio.run((verifier or _verifier()).verify_token(token))


def _provider(**overrides: Any) -> IdentityProvider:
    fields: dict[str, Any] = {
        "provider_id": PROVIDER_ID,
        "provider_type": "oidc",
        "issuer": ISSUER,
        "audiences": (AUDIENCE,),
        "subject_claim": "email",
        "group_claims": (),
        "role_claims": (ROLES_CLAIM_KEY,),
        "entitlement_claims": (),
        "assurance_claim": None,
        "metadata": {
            "trusted_verifiers": [OIDC_VERIFIER_ID],
            "max_verifier_age_seconds": 300,
        },
    }
    fields.update(overrides)
    return IdentityProvider(**fields)


@contextlib.contextmanager
def _in_request(access_token: Any | None) -> Iterator[None]:
    """Install one HTTP request as the in-flight MCP request, as the SDK does."""
    from mcp.server.auth.middleware.bearer_auth import AuthenticatedUser
    from mcp.server.lowlevel.server import request_ctx
    from mcp.shared.context import RequestContext
    from starlette.requests import Request

    scope: dict[str, Any] = {
        "type": "http",
        "method": "POST",
        "path": "/mcp",
        "headers": [],
        "query_string": b"",
    }
    if access_token is not None:
        scope["user"] = AuthenticatedUser(access_token)
    token = request_ctx.set(
        RequestContext(
            request_id=1,
            meta=None,
            session=None,
            lifespan_context=None,
            request=Request(scope),
        )
    )
    try:
        yield
    finally:
        request_ctx.reset(token)


# ==========================================================================
# The verifier: the server checks the token itself
# ==========================================================================


def test_valid_token_is_accepted_with_subject_and_roles() -> None:
    raw = _token()
    accepted = _verify(raw)
    assert accepted is not None
    assert accepted.client_id == f"{OIDC_CLIENT_ID_PREFIX}{AUDIENCE}"
    assert accepted.claims["subject"] == SUBJECT
    assert accepted.claims[ROLES_CLAIM_KEY] == ["default-role", "archi-admins"]
    # The credential is not carried on the request identity.
    assert accepted.token != raw
    assert raw not in repr(accepted)


@pytest.mark.parametrize(
    "label, make_token",
    [
        ("wrong-signature", lambda: _token(key=_OTHER_KEY)),
        ("wrong-issuer", lambda: _token(_claims(iss="https://evil.example.org/realms/team"))),
        (
            "expired",
            lambda: _token(
                _claims(exp=int(time.time()) - 3600, iat=int(time.time()) - 7200)
            ),
        ),
        ("missing-exp", lambda: _token(_claims(exp=_DROP))),
        ("missing-iat", lambda: _token(_claims(iat=_DROP))),
        ("wrong-audience", lambda: _token(_claims(aud=["someone-else"], azp="someone-else"))),
        ("subject-with-space", lambda: _token(_claims(email="alice smith"))),
        (
            "symmetric-algorithm",
            lambda: jwt.encode(
                _claims(), "a-shared-secret-that-is-long-enough-for-hs256", algorithm="HS256",
            ),
        ),
        ("garbage", lambda: "not.a.jwt"),
        ("empty", lambda: ""),
        ("oversized", lambda: "a" * 20000),
    ],
)
def test_rejected_tokens_return_none_without_raising(label: str, make_token: Any) -> None:
    assert _verify(make_token()) is None, label


def test_audience_named_only_by_azp_is_accepted() -> None:
    """Keycloak access tokens often carry `aud: account` and name the client in azp."""
    assert _verify(_token(_claims(aud="account"))) is not None


def test_audience_named_only_by_aud_is_accepted() -> None:
    assert _verify(_token(_claims(azp="another-client"))) is not None


def test_key_lookup_failure_is_a_rejection_not_an_exception() -> None:
    assert _verify(_token(), _verifier(_FailingJwkClient())) is None


def test_non_string_token_is_refused() -> None:
    assert _verifier().verify_token_sync(None) is None  # type: ignore[arg-type]


def test_token_naming_no_person_authenticates_but_is_never_a_principal() -> None:
    """A service account's token may list tools; it can never act as anyone."""
    accepted = _verify(_token(_claims(email=_DROP)))
    assert accepted is not None
    assert accepted.claims["subject"] is None
    with pytest.raises(SubstrateError) as excinfo:
        oidc_claim_for_request(CONFIG, access_token=accepted)
    assert excinfo.value.data["reason_code"] == UNIDENTIFIED_PRINCIPAL_REASON
    assert excinfo.value.data["channel"] == "token_names_no_person"


def test_roles_absent_from_token_yield_no_roles() -> None:
    accepted = _verify(_token(_claims(resource_access=_DROP)))
    assert accepted is not None
    assert accepted.claims[ROLES_CLAIM_KEY] == []


# ==========================================================================
# The manifest block: declared, never guessed
# ==========================================================================


def _block(**overrides: Any) -> dict[str, Any]:
    block: dict[str, Any] = {
        "provider_id": PROVIDER_ID,
        "issuer": ISSUER,
        "audience": AUDIENCE,
        "subject_claim": "email",
        "roles_claim": ROLES_PATH,
        "principal_kind": "human_operator",
    }
    for key, value in overrides.items():
        if value is _DROP:
            block.pop(key, None)
        else:
            block[key] = value
    return block


def test_block_parses_with_optional_resource_url() -> None:
    assert parse_oidc_auth(_block()) == CONFIG
    parsed = parse_oidc_auth(_block(resource_url="https://okg.example.org/mcp"))
    assert parsed.resource_url == "https://okg.example.org/mcp"


def test_unknown_key_is_refused() -> None:
    with pytest.raises(SubstrateError) as excinfo:
        parse_oidc_auth(_block(roles_claims=ROLES_PATH))
    assert excinfo.value.data["unknown_keys"] == ["roles_claims"]


@pytest.mark.parametrize(
    "key",
    ["provider_id", "issuer", "audience", "subject_claim", "roles_claim", "principal_kind"],
)
def test_each_missing_required_key_is_refused_by_name(key: str) -> None:
    with pytest.raises(SubstrateError) as excinfo:
        parse_oidc_auth(_block(**{key: _DROP}))
    assert excinfo.value.data["missing_keys"] == [key]


@pytest.mark.parametrize(
    "overrides",
    [
        {"issuer": "http://auth.example.org/realms/team"},
        {"principal_kind": "wizard"},
        {"subject_claim": "email address"},
        {"resource_url": "okg.example.org/mcp"},
    ],
)
def test_invalid_values_are_refused(overrides: dict[str, Any]) -> None:
    with pytest.raises(SubstrateError):
        parse_oidc_auth(_block(**overrides))


def test_manifest_without_block_yields_none() -> None:
    assert oidc_auth_from_manifest(SimpleNamespace(nomos={"rollout": {}})) is None
    assert oidc_auth_from_manifest(SimpleNamespace(nomos=None)) is None


def test_principal_kind_must_be_admitted_by_posture() -> None:
    manifest = SimpleNamespace(
        nomos={
            "oidc_auth": _block(),
            "deployment_posture": {"read_principals": ["mcp_client"]},
        }
    )
    with pytest.raises(SubstrateError) as excinfo:
        oidc_auth_from_manifest(manifest)
    assert excinfo.value.data["principal_kind"] == "human_operator"


# ==========================================================================
# Start-time agreement with the registered identity provider
# ==========================================================================


def test_agreeing_provider_is_returned() -> None:
    provider = _provider()
    assert validate_oidc_auth_against_providers(
        CONFIG, [provider], deployment_name="default",
    ) is provider


@pytest.mark.parametrize(
    "label, providers",
    [
        ("missing", []),
        ("no-verifier-metadata", [_provider(metadata={})]),
        (
            "trusts-other-verifiers-only",
            [_provider(metadata={"trusted_verifiers": ["mcp-transport:okg-mcp-static-bearer"]})],
        ),
        ("roles-not-read", [_provider(role_claims=("groups",))]),
        ("issuer-skew", [_provider(issuer="https://other.example.org/realms/team")]),
        ("audience-skew", [_provider(audiences=("another-client",))]),
        ("subject-claim-skew", [_provider(subject_claim="sub")]),
    ],
)
def test_disagreeing_provider_refuses_to_start(label: str, providers: list[Any]) -> None:
    with pytest.raises(SubstrateError):
        validate_oidc_auth_against_providers(CONFIG, providers, deployment_name="default")


# ==========================================================================
# The verified token becomes a Nomos principal
# ==========================================================================


def test_claim_from_verified_token_normalizes_valid() -> None:
    claim = oidc_claim_for_request(CONFIG, access_token=_verify(_token()))
    envelope = normalize_identity_claim(claim, [_provider()])
    assert envelope.is_valid, envelope.verification_status
    assert envelope.external_subject == SUBJECT
    assert envelope.roles == ("archi-admins", "default-role")
    assert envelope.verified_by == OIDC_VERIFIER_ID
    assert envelope.verification_method == OIDC_TOKEN_VERIFICATION_METHOD


def test_role_binding_resolves_the_subject() -> None:
    envelope = normalize_identity_claim(
        oidc_claim_for_request(CONFIG, access_token=_verify(_token())),
        [_provider()],
    )

    def binding(role: str) -> SubjectBinding:
        return SubjectBinding(
            binding_id=f"binding:{role}",
            provider_id=PROVIDER_ID,
            issuer=ISSUER,
            claim_path=ROLES_CLAIM_KEY,
            claim_value=role,
            subject=f"subject:{role}",
            scope=SCOPE,
        )

    assert binding("archi-admins").matches(envelope, scope=SCOPE)
    assert not binding("not-a-member").matches(envelope, scope=SCOPE)


def test_provider_that_does_not_trust_the_oidc_verifier_denies_the_claim() -> None:
    """Non-vacuity: the verifier stamp is actually checked by the machinery."""
    claim = oidc_claim_for_request(CONFIG, access_token=_verify(_token()))
    envelope = normalize_identity_claim(
        claim,
        [_provider(metadata={"trusted_verifiers": ["mcp-transport:someone-else"]})],
    )
    assert envelope.verification_status == "untrusted_verifier"


def test_token_is_read_from_the_request_scope() -> None:
    with _in_request(_verify(_token())):
        claim = oidc_claim_for_request(CONFIG)
    assert claim["external_subject"] == SUBJECT


def test_request_without_verified_token_is_refused_as_unidentified() -> None:
    with _in_request(None):
        with pytest.raises(SubstrateError) as excinfo:
            oidc_claim_for_request(CONFIG)
    assert excinfo.value.data["reason_code"] == UNIDENTIFIED_PRINCIPAL_REASON
    assert excinfo.value.data["channel"] == "no_verified_token"


def test_token_accepted_by_another_verifier_is_not_an_oidc_identity() -> None:
    from mcp.server.auth.provider import AccessToken

    static = AccessToken(token="accepted", client_id="okg-mcp-static-bearer", scopes=[])
    with _in_request(static):
        assert verified_token_from_request() is None
        with pytest.raises(SubstrateError):
            oidc_claim_for_request(CONFIG)


def test_token_issuer_that_differs_from_the_config_is_refused() -> None:
    other = OidcAuthConfig(**{**CONFIG.to_dict(), "issuer": "https://other.example.org/r"})
    with pytest.raises(SubstrateError) as excinfo:
        oidc_claim_for_request(other, access_token=_verify(_token()))
    assert excinfo.value.data["channel"] == "token_config_skew"


# ==========================================================================
# Discovery and SDK settings
# ==========================================================================


def test_settings_publish_resource_metadata_only_when_declared() -> None:
    assert build_oidc_auth_settings(CONFIG).resource_server_url is None
    declared = OidcAuthConfig(
        **{**CONFIG.to_dict(), "resource_url": "https://okg.example.org/mcp"}
    )
    settings = build_oidc_auth_settings(declared)
    assert str(settings.resource_server_url).rstrip("/") == "https://okg.example.org/mcp"
    assert settings.validate_token_resource is False


def test_builder_uses_discovered_keys_and_only_asymmetric_algorithms() -> None:
    seen: list[str] = []

    def factory(uri: str) -> Any:
        seen.append(uri)
        return _FakeJwkClient()

    verifier = build_oidc_token_verifier(
        CONFIG,
        metadata_loader=lambda issuer: {
            "jwks_uri": "https://auth.example.org/realms/team/certs",
            "id_token_signing_alg_values_supported": ["RS256", "HS256"],
        },
        jwk_client_factory=factory,
    )
    assert seen == ["https://auth.example.org/realms/team/certs"]
    assert _verify(_token(), verifier) is not None
    assert _verify(
        jwt.encode(_claims(), "a-shared-secret-that-is-long-enough-for-hs256", algorithm="HS256"),
        verifier,
    ) is None


def test_builder_refuses_a_non_https_key_url() -> None:
    with pytest.raises(SubstrateError) as excinfo:
        build_oidc_token_verifier(
            CONFIG,
            metadata_loader=lambda issuer: {"jwks_uri": "http://auth.example.org/certs"},
            jwk_client_factory=lambda uri: _FakeJwkClient(),
        )
    assert excinfo.value.data["reason"] == "jwks_uri_invalid"


def test_builder_refuses_when_discovery_fails() -> None:
    from okg.substrate.nomos.oidc_login import OIDCLoginError

    def failing(issuer: str) -> Any:
        raise OIDCLoginError("OIDC discovery metadata fetch failed")

    with pytest.raises(SubstrateError) as excinfo:
        build_oidc_token_verifier(CONFIG, metadata_loader=failing)
    assert excinfo.value.data["reason"] == "discovery_failed"


# ==========================================================================
# The served app and run_server's start-time refusals
# ==========================================================================

import okg.substrate.mcp.server as server_mod  # noqa: E402

_DEAD_DSN = "postgresql://postgres:postgres@127.0.0.1:1/nodb"
_BASE_URL = "http://127.0.0.1:8123"
_ACCEPT = "application/json, text/event-stream"
_RESOURCE_URL = "http://127.0.0.1:8123/mcp"


def _served_app(*, session_owner: bool = False) -> Any:
    """A FastMCP app wired exactly as OIDC mode wires it: the real SDK middleware.

    With ``session_owner`` the server's own subclass is used and the verified
    config is installed as the principal mapping, so the session-owner
    middleware runs on every request exactly as ``mcp-serve --oidc-auth``
    runs it. A live deployment answered 500 to every authenticated request
    when that middleware read the OIDC config as a header mapping.
    """
    from mcp.server.auth.middleware.auth_context import get_access_token
    from mcp.server.fastmcp import FastMCP
    from okg.substrate.mcp.session_tool_surface import SessionScopedFastMCP

    config = OidcAuthConfig(**{**CONFIG.to_dict(), "resource_url": _RESOURCE_URL})
    verifier = OidcTokenVerifier(config, algorithms=("RS256",), jwk_client=_FakeJwkClient())
    cls = SessionScopedFastMCP if session_owner else FastMCP
    mcp = cls("oidc-test", token_verifier=verifier, auth=build_oidc_auth_settings(config))
    if session_owner:
        mcp._okg_principal_mapping = config
        mcp._okg_deployment_name = "fixture"

        def gate(operator_name: str) -> None:
            # The server's gate, minus the database: the application listing
            # rule first, then the person must resolve or the request is refused.
            if application_may_list_tools(config, operator_name):
                return
            oidc_claim_for_request(config)

        mcp._okg_dispatch_principal_gate = gate

    @mcp.tool()
    def ping() -> str:
        return "pong"

    @mcp.tool()
    def whoami() -> str:
        return str(get_access_token().subject)

    return mcp.streamable_http_app()


def _initialize(client: Any, headers: dict[str, str]) -> Any:
    return client.post(
        "/mcp",
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "pytest", "version": "0"},
            },
        },
        headers=headers,
    )


#: Built INSIDE the test, never in the parameter list: a token carries
#: timestamps and a signature, so a token in a test id differs between
#: pytest-xdist workers and the run aborts with "Different tests were
#: collected between gw0 and gw1".
_UNUSABLE_TOKENS = {
    "missing": lambda: None,
    "forged": lambda: _token(key=_OTHER_KEY),
    "expired": lambda: _token(
        _claims(exp=int(time.time()) - 3600, iat=int(time.time()) - 7200)
    ),
    "garbage": lambda: "not.a.jwt",
}


@pytest.mark.parametrize("label", sorted(_UNUSABLE_TOKENS))
def test_served_app_refuses_without_a_valid_token(label: str) -> None:
    from starlette.testclient import TestClient

    token = _UNUSABLE_TOKENS[label]()
    headers = {"Accept": _ACCEPT}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    with TestClient(_served_app(), base_url=_BASE_URL) as client:
        response = _initialize(client, headers)
    assert response.status_code == 401, (label, response.text)
    # Points the client at where to log in.
    assert "resource_metadata" in response.headers.get("www-authenticate", "")


def test_served_app_accepts_a_valid_token() -> None:
    from starlette.testclient import TestClient

    headers = {"Accept": _ACCEPT, "Authorization": f"Bearer {_token()}"}
    with TestClient(_served_app(), base_url=_BASE_URL) as client:
        response = _initialize(client, headers)
    assert response.status_code == 200, response.text


def _session_request(client: Any, headers: dict[str, str], body: dict[str, Any]) -> Any:
    """initialize, initialized, then one request on that SDK session; its JSON-RPC body."""
    response = _initialize(client, headers)
    assert response.status_code == 200, response.text
    session = {**headers, "Mcp-Session-Id": response.headers["mcp-session-id"]}
    client.post(
        "/mcp", json={"jsonrpc": "2.0", "method": "notifications/initialized"}, headers=session,
    )
    response = client.post("/mcp", json={"jsonrpc": "2.0", "id": 2, **body}, headers=session)
    assert response.status_code == 200, response.text
    text = response.text
    if text.startswith("event:"):
        text = "".join(line[5:] for line in text.splitlines() if line.startswith("data:"))
    return json.loads(text)


def _call_tool(
    client: Any, headers: dict[str, str], name: str, *, expect_error: bool = False,
) -> str:
    body = _session_request(
        client, headers, {"method": "tools/call", "params": {"name": name, "arguments": {}}},
    )
    if expect_error:
        # A refusal reaches the client as a JSON-RPC error from the dispatch
        # gate or, through the bounded call adapter, as an isError result.
        if "error" in body:
            return body["error"]["message"]
        assert body["result"]["isError"], body
        return body["result"]["content"][0]["text"]
    assert not body["result"].get("isError"), body
    return body["result"]["content"][0]["text"]


def _list_tools(client: Any, headers: dict[str, str]) -> list[str]:
    body = _session_request(client, headers, {"method": "tools/list"})
    return sorted(tool["name"] for tool in body["result"]["tools"])


def test_session_owner_middleware_serves_people_and_applications_in_oidc_mode() -> None:
    """The server's session-owner middleware, on the OIDC config, per person."""
    from starlette.testclient import TestClient

    def headers(claims: dict[str, Any]) -> dict[str, str]:
        return {"Accept": _ACCEPT, "Authorization": f"Bearer {_token(claims)}"}

    alice = headers(_claims())
    bob = headers(_claims(sub="f:0001:bob", email="bob@example.org"))
    application = headers(_claims(clientHost="10.0.0.1", email=_DROP))
    with TestClient(_served_app(session_owner=True), base_url=_BASE_URL) as client:
        owners = {label: _call_tool(client, h, "whoami") for label, h in (
            ("alice", alice), ("bob", bob),
        )}
        # An application lists the admitted surface and can call nothing.
        assert _list_tools(client, application) == ["ping", "whoami"]
        refused = _call_tool(client, application, "ping", expect_error=True)
        assert "issued to an application" in refused
    assert all(owner.startswith("okg-principal:") for owner in owners.values()), owners
    assert len(set(owners.values())) == 2, owners


def test_served_app_publishes_the_issuer_as_authorization_server() -> None:
    from starlette.testclient import TestClient

    app = _served_app()
    paths = [
        getattr(route, "path", "")
        for route in app.routes
        if "oauth-protected-resource" in getattr(route, "path", "")
    ]
    assert paths, "no protected-resource metadata route was published"
    with TestClient(app, base_url=_BASE_URL) as client:
        metadata = client.get(paths[0]).json()
    assert [str(item).rstrip("/") for item in metadata["authorization_servers"]] == [ISSUER]


@pytest.mark.parametrize(
    "kwargs, code",
    [
        ({"transport": "stdio"}, "mcp_oidc_auth_stdio_incompatible"),
        (
            {"transport": "streamable-http", "auth_token_env": "OKG_MCP_AUTH_TOKEN"},
            "mcp_oidc_auth_exclusive",
        ),
        (
            {"transport": "streamable-http", "multi_user_principals": True},
            "mcp_oidc_auth_exclusive",
        ),
    ],
)
def test_run_server_refuses_impossible_oidc_combinations(
    monkeypatch: Any, kwargs: dict[str, Any], code: str,
) -> None:
    def must_not_resolve(name: str) -> Any:
        raise AssertionError("refusal must come before manifest resolution")

    monkeypatch.setattr(server_mod, "resolve_oidc_auth", must_not_resolve)
    with pytest.raises(SubstrateError) as excinfo:
        server_mod.run_server(dsn=_DEAD_DSN, oidc_auth=True, **kwargs)
    assert excinfo.value.code == code


def test_run_server_refuses_an_undeclared_oidc_block(monkeypatch: Any) -> None:
    monkeypatch.setattr(server_mod, "resolve_oidc_auth", lambda name: None)
    with pytest.raises(SubstrateError) as excinfo:
        server_mod.run_server(transport="streamable-http", dsn=_DEAD_DSN, oidc_auth=True)
    assert excinfo.value.code == "mcp_oidc_auth_undeclared"


def test_run_server_refuses_when_the_issuer_cannot_be_discovered(monkeypatch: Any) -> None:
    from okg.substrate.nomos.oidc_login import OIDCLoginError

    def failing(issuer: str, **_: Any) -> Any:
        raise OIDCLoginError("OIDC discovery metadata fetch failed")

    monkeypatch.setattr(server_mod, "resolve_oidc_auth", lambda name: CONFIG)
    monkeypatch.setattr("okg.substrate.nomos.oidc_login.discover_oidc_metadata", failing)
    with pytest.raises(SubstrateError) as excinfo:
        server_mod.run_server(transport="streamable-http", dsn=_DEAD_DSN, oidc_auth=True)
    assert excinfo.value.data["reason"] == "discovery_failed"


def test_default_run_server_does_not_touch_oidc(monkeypatch: Any) -> None:
    """Opt-in: without oidc_auth the manifest block is never even read."""
    import inspect

    assert inspect.signature(server_mod.run_server).parameters["oidc_auth"].default is False
    assert inspect.signature(server_mod.build_server).parameters["oidc_verifier"].default is None


# ==========================================================================
# Service tokens for operator tooling
# ==========================================================================


class _Response:
    def __init__(self, status_code: int, body: Any) -> None:
        self.status_code = status_code
        self._body = body

    def json(self) -> Any:
        return self._body


def _token_endpoint_metadata(issuer: str) -> dict[str, str]:
    return {"token_endpoint": "https://auth.example.org/realms/team/token"}


def test_service_token_is_requested_with_client_credentials() -> None:
    seen: dict[str, Any] = {}

    def post(url: str, **kwargs: Any) -> _Response:
        seen.update(url=url, **kwargs)
        return _Response(200, {"access_token": "service-token"})

    token = fetch_service_token(
        ISSUER, AUDIENCE, "client-secret",
        metadata_loader=_token_endpoint_metadata, post=post,
    )
    assert token == "service-token"
    assert seen["url"] == "https://auth.example.org/realms/team/token"
    assert seen["data"] == {"grant_type": "client_credentials"}
    assert seen["auth"] == (AUDIENCE, "client-secret")
    assert seen["allow_redirects"] is False


def test_refused_service_token_names_the_fix_and_leaks_no_secret() -> None:
    with pytest.raises(SubstrateError) as excinfo:
        fetch_service_token(
            ISSUER, AUDIENCE, "client-secret",
            metadata_loader=_token_endpoint_metadata,
            post=lambda url, **kwargs: _Response(401, {"error": "unauthorized_client"}),
        )
    assert excinfo.value.data["reason"] == "service_token_refused"
    assert excinfo.value.data["error"] == "unauthorized_client"
    assert "client-secret" not in str(excinfo.value)
    assert "client-secret" not in repr(excinfo.value.data)


def test_service_token_needs_an_https_token_endpoint() -> None:
    with pytest.raises(SubstrateError) as excinfo:
        fetch_service_token(
            ISSUER, AUDIENCE, "client-secret",
            metadata_loader=lambda issuer: {"token_endpoint": "http://auth.example.org/token"},
            post=lambda *args, **kwargs: _Response(200, {"access_token": "never"}),
        )
    assert excinfo.value.data["reason"] == "token_endpoint_invalid"


def test_service_token_transport_failure_is_reported_by_type_only() -> None:
    def post(url: str, **kwargs: Any) -> _Response:
        raise ConnectionError("client-secret appears in this text")

    with pytest.raises(SubstrateError) as excinfo:
        fetch_service_token(
            ISSUER, AUDIENCE, "client-secret",
            metadata_loader=_token_endpoint_metadata, post=post,
        )
    assert excinfo.value.data["reason"] == "token_request_failed"
    assert "client-secret" not in str(excinfo.value)


# ==========================================================================
# Adversarial-review regressions
# ==========================================================================


class _CountingJwkClient:
    """Stands in for PyJWKClient: one known key id, and counts forced refreshes."""

    def __init__(self) -> None:
        self.refreshes = 0
        self.known = SimpleNamespace(key_id="test", key=_KEY.public_key())

    def get_signing_keys(self, refresh: bool = False) -> list[Any]:
        if refresh:
            self.refreshes += 1
        return [self.known]

    @staticmethod
    def match_kid(keys: list[Any], kid: str) -> Any:
        return next((key for key in keys if key.key_id == kid), None)


def _token_with_kid(kid: str) -> str:
    return jwt.encode(_claims(), _KEY, algorithm="RS256", headers={"kid": kid})


def test_unknown_key_ids_cannot_force_a_key_download_per_request() -> None:
    """A caller-chosen key id must not make every request refetch the JWKS."""
    from okg.substrate.mcp.oidc_auth import RateLimitedJwkClient

    now = [1000.0]
    inner = _CountingJwkClient()
    client = RateLimitedJwkClient(inner, min_refresh_interval=60, clock=lambda: now[0])
    for index in range(50):
        with pytest.raises(LookupError):
            client.get_signing_key_from_jwt(_token_with_kid(f"random-{index}"))
    assert inner.refreshes == 1
    now[0] += 61  # the provider may have rotated its keys since
    with pytest.raises(LookupError):
        client.get_signing_key_from_jwt(_token_with_kid("still-unknown"))
    assert inner.refreshes == 2


def test_known_key_id_never_triggers_a_refresh() -> None:
    from okg.substrate.mcp.oidc_auth import RateLimitedJwkClient

    inner = _CountingJwkClient()
    client = RateLimitedJwkClient(inner)
    verifier = _verifier(client)
    for _ in range(5):
        assert _verify(_token_with_kid("test"), verifier) is not None
    assert inner.refreshes == 0


@pytest.mark.parametrize("claim", ["sub", "azp", "aud", "client_id"])
def test_subject_claim_every_token_carries_is_refused(claim: str) -> None:
    """Otherwise an application's own service token would act as a person."""
    with pytest.raises(SubstrateError) as excinfo:
        parse_oidc_auth(_block(subject_claim=claim))
    assert excinfo.value.data["path"] == "nomos.oidc_auth.subject_claim"


# ==========================================================================
# PR review: application tokens, assurance, freshness, redaction, agreement
# ==========================================================================


@pytest.mark.parametrize(
    "label, overrides",
    [
        # Keycloak stamps service-account tokens with the client's origin.
        ("keycloak-service-account", {"clientHost": "10.0.0.1", "clientAddress": "10.0.0.1"}),
        ("grant-type", {"gty": "client-credentials"}),
        ("subject-is-the-client", {"sub": AUDIENCE}),
    ],
)
def test_application_token_with_an_email_is_never_a_principal(
    label: str, overrides: dict[str, Any],
) -> None:
    """The subject claim is satisfied, and it still must not act as a person."""
    accepted = _verify(_token(_claims(**overrides)))
    assert accepted is not None, label  # transport: may list tools
    assert accepted.claims["subject"] == SUBJECT
    with pytest.raises(SubstrateError) as excinfo:
        oidc_claim_for_request(CONFIG, access_token=accepted)
    assert excinfo.value.data["channel"] == "token_names_no_person", label


def test_a_persons_token_is_not_mistaken_for_an_application() -> None:
    """Non-vacuity: `client_id` alone is on user tokens too (RFC 9068)."""
    accepted = _verify(_token(_claims(client_id=AUDIENCE)))
    assert accepted.claims["is_application"] is False
    oidc_claim_for_request(CONFIG, access_token=accepted)


def test_assurance_reaches_a_provider_that_requires_mfa() -> None:
    provider = _provider(mfa_required=True)
    with_mfa = oidc_claim_for_request(
        CONFIG, access_token=_verify(_token(_claims(acr="urn:loa:mfa", amr=["pwd", "otp"]))),
    )
    assert normalize_identity_claim(with_mfa, [provider]).is_valid
    without = oidc_claim_for_request(CONFIG, access_token=_verify(_token()))
    assert normalize_identity_claim(without, [provider]).verification_status == "missing_assurance"


def test_token_mint_time_never_stands_in_for_authentication_time() -> None:
    """`iat` is re-issued on refresh; only `auth_time` says when the person logged in."""
    claim = oidc_claim_for_request(
        CONFIG, access_token=_verify(_token(_claims(auth_time=_DROP))),
    )
    assert "auth_time" not in claim
    fresh_provider = _provider(freshness_seconds=300)
    status = normalize_identity_claim(claim, [fresh_provider]).verification_status
    assert status == "missing_auth_time"


@pytest.mark.parametrize(
    "error",
    [
        "invalid_client: the secret client-secret is wrong",
        "client-secret",
        "not a code at all",
    ],
)
def test_provider_error_text_is_never_reflected_verbatim(error: str) -> None:
    with pytest.raises(SubstrateError) as excinfo:
        fetch_service_token(
            ISSUER, AUDIENCE, "client-secret",
            metadata_loader=_token_endpoint_metadata,
            post=lambda url, **kwargs: _Response(401, {"error": error}),
        )
    assert excinfo.value.data["error"] == "unrecognized"
    assert "client-secret" not in str(excinfo.value)
    assert "client-secret" not in repr(excinfo.value.data)


def test_a_well_formed_provider_error_code_is_kept() -> None:
    with pytest.raises(SubstrateError) as excinfo:
        fetch_service_token(
            ISSUER, AUDIENCE, "client-secret",
            metadata_loader=_token_endpoint_metadata,
            post=lambda url, **kwargs: _Response(401, {"error": "unauthorized_client"}),
        )
    assert excinfo.value.data["error"] == "unauthorized_client"


def _chat_login(**overrides: Any) -> Any:
    fields = {"issuer": ISSUER, "client_id": AUDIENCE, "roles_claim": ROLES_PATH}
    fields.update(overrides)
    return SimpleNamespace(**fields)


@pytest.mark.parametrize(
    "overrides, skewed",
    [
        ({"issuer": "https://other.example.org/realms/team"}, "issuer"),
        ({"client_id": "another-client"}, "client"),
        ({"roles_claim": "realm_access.roles"}, "roles_claim"),
    ],
)
def test_disagreeing_login_blocks_are_refused(overrides: dict[str, Any], skewed: str) -> None:
    from okg.substrate.mcp.oidc_auth import require_login_blocks_agree

    with pytest.raises(SubstrateError) as excinfo:
        require_login_blocks_agree(CONFIG, _chat_login(**overrides), deployment="fixture")
    assert excinfo.value.code == "mcp_oidc_login_blocks_disagree"
    assert set(excinfo.value.data["skew"]) == {skewed}


def test_agreeing_or_absent_login_blocks_pass() -> None:
    from okg.substrate.mcp.oidc_auth import require_login_blocks_agree

    require_login_blocks_agree(CONFIG, _chat_login(issuer=ISSUER + "/"), deployment="fixture")
    require_login_blocks_agree(CONFIG, None, deployment="fixture")
    require_login_blocks_agree(None, _chat_login(client_id="anything"), deployment="fixture")
