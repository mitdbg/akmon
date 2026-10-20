"""PACT ``chat-oidc-login`` -- the chat instance signs people in with the
identity provider itself and forwards each person's token to ``mcp-serve``.

Hermetic: the manifest block, the launch spec, and the MCP connection payload.
"""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from okg.substrate.chat.instance import (
    AppDatabase,
    ChatInstanceError,
    build_instance_spec,
    normalize_dsn,
    resolve_instance_port,
)
from okg.substrate.chat.sync import _connection_payload, _mcp_connection_auth_type
from okg.substrate.deployments.chat_block import ChatManifestError, ChatOidcLogin
from tests.substrate.test_chat_sso_config import _chat_block, _parse, _sso_block

from .instance_harness import chat_block, env, free_port
from .test_instance_app_database import APP_ENV, DISTINCT_DSN, GRAPH_DSN, SECRET_ENV

CLIENT_SECRET_ENV = "OKG_TEST_CHAT_OIDC_SECRET"
CLIENT_SECRET = "fixture-client-secret-value"
_DROP = object()


def _login_block(**overrides: Any) -> dict[str, Any]:
    block: dict[str, Any] = {
        "issuer": "https://auth.example.org/realms/team",
        "client_id": "okg-chat",
        "client_secret_env": CLIENT_SECRET_ENV,
        "roles_claim": "resource_access.okg-chat.roles",
        "allowed_roles": ["default-role"],
        "admin_roles": ["archi-admins"],
        "provider_name": "Team SSO",
    }
    for key, value in overrides.items():
        if value is _DROP:
            block.pop(key, None)
        else:
            block[key] = value
    return block


def _login(**overrides: Any) -> ChatOidcLogin:
    parsed = _parse(_chat_block(None, oidc_login=_login_block(**overrides)))
    assert parsed is not None and parsed.oidc_login is not None
    return parsed.oidc_login


# ==========================================================================
# The manifest block
# ==========================================================================


def test_declared_login_parses() -> None:
    login = _login()
    assert login.client_id == "okg-chat"
    assert login.allowed_roles == ("default-role",)
    assert login.admin_roles == ("archi-admins",)
    assert login.discovery_url == (
        "https://auth.example.org/realms/team/.well-known/openid-configuration"
    )


def test_manifest_without_login_is_unchanged() -> None:
    parsed = _parse(_chat_block(None))
    assert parsed is not None and parsed.oidc_login is None


def test_unknown_key_is_refused() -> None:
    with pytest.raises(ChatManifestError):
        _login(client_secret="literal-secrets-are-not-a-key")


@pytest.mark.parametrize(
    "key",
    ["issuer", "client_id", "client_secret_env", "roles_claim", "allowed_roles", "admin_roles"],
)
def test_each_missing_required_key_is_refused_by_name(key: str) -> None:
    with pytest.raises(ChatManifestError) as excinfo:
        _login(**{key: _DROP})
    assert excinfo.value.data["missing"] == [key]


@pytest.mark.parametrize(
    "overrides",
    [
        {"issuer": "http://auth.example.org/realms/team"},
        {"allowed_roles": "default-role"},
        {"admin_roles": ["archi,admins"]},
        {"redirect_uri": "not-a-url"},
        {"client_secret_env": "$OKG_TEST_CHAT_OIDC_SECRET"},
    ],
)
def test_invalid_values_are_refused(overrides: dict[str, Any]) -> None:
    with pytest.raises(ChatManifestError):
        _login(**overrides)


def test_refused_alongside_a_multi_user_proxy_declaration() -> None:
    with pytest.raises(ChatManifestError) as excinfo:
        _parse(_chat_block(_sso_block(), oidc_login=_login_block()))
    assert excinfo.value.data["path"] == "chat.oidc_login"


# ==========================================================================
# The launch spec
# ==========================================================================


def _spec(oidc_login: Any, **environ: str) -> Any:
    return build_instance_spec(
        "oidc-login-fixture",
        app_database=AppDatabase(
            env_var=APP_ENV,
            target=normalize_dsn(DISTINCT_DSN, label="app"),
            dsn=DISTINCT_DSN,
            graph_target=normalize_dsn(GRAPH_DSN, label="graph"),
        ),
        secret_env=SECRET_ENV,
        environ=env(**{SECRET_ENV: "fixture-key", **environ}),
        port_plan=resolve_instance_port(
            "oidc-login-fixture", chat_block(), override=free_port(),
        ),
        oidc_login=oidc_login,
    )


def test_login_settings_are_projected_with_role_management_on() -> None:
    spec = _spec(_login(), **{CLIENT_SECRET_ENV: CLIENT_SECRET})
    assert spec.env["OAUTH_CLIENT_ID"] == "okg-chat"
    assert spec.env["OAUTH_CLIENT_SECRET"] == CLIENT_SECRET
    assert spec.env["OPENID_PROVIDER_URL"].endswith("/.well-known/openid-configuration")
    assert spec.env["ENABLE_OAUTH_SIGNUP"] == "true"
    assert spec.env["ENABLE_OAUTH_ROLE_MANAGEMENT"] == "true"
    assert spec.env["OAUTH_ROLES_CLAIM"] == "resource_access.okg-chat.roles"
    assert spec.env["OAUTH_ALLOWED_ROLES"] == "default-role"
    assert spec.env["OAUTH_ADMIN_ROLES"] == "archi-admins"
    assert spec.env["OAUTH_PROVIDER_NAME"] == "Team SSO"
    assert "OPENID_REDIRECT_URI" not in spec.env


def test_redirect_uri_is_projected_when_declared() -> None:
    spec = _spec(
        _login(redirect_uri="http://localhost:8099/oauth/oidc/login/callback"),
        **{CLIENT_SECRET_ENV: CLIENT_SECRET},
    )
    assert spec.env["OPENID_REDIRECT_URI"] == "http://localhost:8099/oauth/oidc/login/callback"


def test_unset_client_secret_refuses_to_start() -> None:
    with pytest.raises(ChatInstanceError) as excinfo:
        _spec(_login())
    assert excinfo.value.data["reason"] == "oidc_client_secret_env_unset"
    assert excinfo.value.data["env_var"] == CLIENT_SECRET_ENV


def test_launch_argv_carries_names_and_no_values() -> None:
    argv = _spec(_login(), **{CLIENT_SECRET_ENV: CLIENT_SECRET}).run_argv("docker")
    assert "OAUTH_CLIENT_SECRET" in argv
    assert all(CLIENT_SECRET not in arg for arg in argv)


def test_absent_login_starts_exactly_as_before() -> None:
    spec = _spec(None)
    assert not [name for name in spec.env if name.startswith(("OAUTH_", "OPENID_"))]


# ==========================================================================
# The MCP connection
# ==========================================================================


def _payload(auth_type: str) -> dict[str, Any]:
    return _connection_payload(
        url="http://host.docker.internal:8765/mcp",
        server_id="okg-fixture",
        deployment="fixture",
        filter_list=["inspect"],
        grants=[],
        auth_type=auth_type,
    )


def test_connection_forwards_each_persons_token_when_login_is_declared() -> None:
    auth_type = _mcp_connection_auth_type(SimpleNamespace(oidc_login=_login()))
    assert auth_type == "system_oauth"
    payload = _payload(auth_type)
    assert payload["auth_type"] == "system_oauth"
    assert payload["key"] == ""


def test_connection_is_unchanged_without_a_login() -> None:
    """No login declared: the entry keeps whatever credential sync resolved."""
    assert _mcp_connection_auth_type(SimpleNamespace()) is None
    assert _mcp_connection_auth_type(SimpleNamespace(oidc_login=None)) is None
    assert _payload(None)["auth_type"] == "none"


# ==========================================================================
# Sync against an OIDC-mode mcp-serve
# ==========================================================================


def test_models_are_visible_to_every_signed_in_person_when_login_is_declared() -> None:
    from okg.substrate.chat.sync import _signed_in_grants

    assert _signed_in_grants(SimpleNamespace(oidc_login=_login())) == [
        {"principal_type": "user", "principal_id": "*", "permission": "read"}
    ]


def test_models_stay_admin_only_without_a_login() -> None:
    from okg.substrate.chat.sync import _signed_in_grants

    assert _signed_in_grants(SimpleNamespace(oidc_login=None)) == []
    assert _signed_in_grants(SimpleNamespace()) == []


def test_sync_proves_the_server_with_the_clients_service_token() -> None:
    from okg.substrate.chat.sync import SERVICE_TOKEN_ENV, _service_token_environ

    seen: dict[str, str] = {}

    def fetch(issuer: str, client_id: str, secret: str) -> str:
        seen.update(issuer=issuer, client_id=client_id, secret=secret)
        return "service-token"

    secrets: list[str] = []
    name, environ = _service_token_environ(
        _login(), secrets, environ={CLIENT_SECRET_ENV: CLIENT_SECRET}, fetch=fetch,
    )
    assert name == SERVICE_TOKEN_ENV
    assert environ == {SERVICE_TOKEN_ENV: "service-token"}
    assert seen == {
        "issuer": "https://auth.example.org/realms/team",
        "client_id": "okg-chat",
        "secret": CLIENT_SECRET,
    }
    # Both are known to the probe's redaction before anything is dialed.
    assert set(secrets) == {CLIENT_SECRET, "service-token"}


def test_sync_probe_refuses_without_the_client_secret() -> None:
    from okg.substrate.chat.sync import ChatSyncError, _service_token_environ

    with pytest.raises(ChatSyncError) as excinfo:
        _service_token_environ(_login(), [], environ={}, fetch=lambda *args: "unused")
    assert excinfo.value.data["reason"] == "oidc_client_secret_env_unset"


def test_service_token_failure_is_reported_as_a_login_problem(monkeypatch: Any) -> None:
    """Not as `mcp_unreachable`, which would send the operator to a healthy server."""
    from okg.substrate.chat import sync as sync_mod
    from okg.substrate.errors import SubstrateError

    def refused(*args: Any, **kwargs: Any) -> str:
        raise SubstrateError(
            "the identity provider refused a service token (HTTP 401: unauthorized_client)",
            data={"reason": "service_token_refused", "error": "unauthorized_client"},
            fix_hint="allow the client-credentials grant",
        )

    def must_not_dial(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("the MCP server must not be dialed without a token")

    monkeypatch.setenv(CLIENT_SECRET_ENV, CLIENT_SECRET)
    monkeypatch.setattr("okg.substrate.mcp.oidc_auth.fetch_service_token", refused)
    monkeypatch.setattr("okg.substrate.chat.source_admin.read_live_tools", must_not_dial)
    inputs = SimpleNamespace(
        chat=SimpleNamespace(mcp=None, oidc_login=_login()),
        mcp_probe_url="http://host.docker.internal:8765/mcp",
        deployment="fixture",
    )
    report = SimpleNamespace(mcp=None)
    with pytest.raises(sync_mod.ChatSyncError) as excinfo:
        sync_mod._probe_mcp(inputs, report, bound_tools=["inspect"], probe_tools=None)
    assert excinfo.value.data["reason"] == "oidc_service_token_unavailable"
    assert excinfo.value.data["provider_error"] == "unauthorized_client"
    assert CLIENT_SECRET not in str(excinfo.value)
    assert report.mcp["reachable"] is None


# ==========================================================================
# A running instance is never reported as carrying settings it does not have
# ==========================================================================


def test_launch_records_a_settings_fingerprint_without_any_value() -> None:
    from okg.substrate.chat.instance import LABEL_SETTINGS

    spec = _spec(_login(), **{CLIENT_SECRET_ENV: CLIENT_SECRET})
    fingerprint = spec.labels[LABEL_SETTINGS]
    assert fingerprint.startswith("sha256:")
    assert CLIENT_SECRET not in fingerprint
    assert all(CLIENT_SECRET not in arg for arg in spec.run_argv("docker"))


def test_changed_settings_on_a_running_container_are_refused() -> None:
    from okg.substrate.chat.instance import LABEL_SETTINGS, _refuse_changed_settings

    old = _spec(_login(), **{CLIENT_SECRET_ENV: CLIENT_SECRET})
    rotated = _spec(_login(), **{CLIENT_SECRET_ENV: "a-rotated-client-secret"})
    facts = {"labels": dict(old.labels)}
    _refuse_changed_settings("oidc-login-fixture", old, facts)  # unchanged: fine
    with pytest.raises(ChatInstanceError) as excinfo:
        _refuse_changed_settings("oidc-login-fixture", rotated, facts)
    assert excinfo.value.data["reason"] == "instance_settings_changed"
    assert old.labels[LABEL_SETTINGS] != rotated.labels[LABEL_SETTINGS]


def test_pre_fingerprint_container_is_refused_only_when_login_is_added() -> None:
    from okg.substrate.chat.instance import _refuse_changed_settings

    unlabeled = {"labels": {}}
    # Without a login the old behavior stands: no refusal for an old container.
    _refuse_changed_settings("oidc-login-fixture", _spec(None), unlabeled)
    with pytest.raises(ChatInstanceError) as excinfo:
        _refuse_changed_settings(
            "oidc-login-fixture",
            _spec(_login(), **{CLIENT_SECRET_ENV: CLIENT_SECRET}),
            unlabeled,
        )
    assert excinfo.value.data["fingerprint_recorded"] is False


class _ConfigClient:
    def __init__(self, providers: dict[str, str]) -> None:
        self.providers = providers

    def public_config(self) -> dict[str, Any]:
        return {"oauth": {"providers": self.providers}}


def test_sync_refuses_when_the_instance_lacks_the_declared_login() -> None:
    from okg.substrate.chat.sync import ChatSyncError, _require_instance_oidc_login

    inputs = SimpleNamespace(chat=SimpleNamespace(oidc_login=_login()), deployment="fixture")
    with pytest.raises(ChatSyncError) as excinfo:
        _require_instance_oidc_login(inputs, _ConfigClient({}))
    assert excinfo.value.data["reason"] == "instance_oidc_login_absent"
    _require_instance_oidc_login(inputs, _ConfigClient({"oidc": "Team SSO"}))


def test_sync_does_not_read_the_login_config_without_a_declared_login() -> None:
    from okg.substrate.chat.sync import _require_instance_oidc_login

    class _Untouchable:
        def public_config(self) -> Any:
            raise AssertionError("no login declared: nothing to check")

    inputs = SimpleNamespace(chat=SimpleNamespace(oidc_login=None), deployment="fixture")
    _require_instance_oidc_login(inputs, _Untouchable())


def test_static_mcp_credential_is_not_demanded_when_tokens_are_forwarded() -> None:
    """The entry stores no credential, so the static variable is not required."""
    from okg.substrate.chat import sync as sync_mod

    def must_not_resolve(*args: Any, **kwargs: Any) -> str:
        raise AssertionError("no stored credential is needed in OIDC mode")

    original = sync_mod._resolve_connection_credential
    sync_mod._resolve_connection_credential = must_not_resolve
    try:
        auth_type, token = sync_mod._connection_credentials(
            SimpleNamespace(oidc_login=_login()),
        )
    finally:
        sync_mod._resolve_connection_credential = original
    assert (auth_type, token) == ("system_oauth", None)


def test_static_mcp_credential_is_still_required_without_a_login() -> None:
    from okg.substrate.chat import sync as sync_mod

    original = sync_mod._resolve_connection_credential
    sync_mod._resolve_connection_credential = lambda chat, **kw: "static-token"
    try:
        auth_type, token = sync_mod._connection_credentials(SimpleNamespace(oidc_login=None))
    finally:
        sync_mod._resolve_connection_credential = original
    assert (auth_type, token) == (None, "static-token")


def test_account_merging_by_email_is_off_unless_declared() -> None:
    """The pinned instance never checks `email_verified` when it merges."""
    assert _login().merge_accounts_by_email is False
    spec = _spec(_login(), **{CLIENT_SECRET_ENV: CLIENT_SECRET})
    assert spec.env["OAUTH_MERGE_ACCOUNTS_BY_EMAIL"] == "false"
    declared = _spec(_login(merge_accounts_by_email=True), **{CLIENT_SECRET_ENV: CLIENT_SECRET})
    assert declared.env["OAUTH_MERGE_ACCOUNTS_BY_EMAIL"] == "true"


def test_account_merging_must_be_a_boolean() -> None:
    with pytest.raises(ChatManifestError):
        _login(merge_accounts_by_email="yes")
