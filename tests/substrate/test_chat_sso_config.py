"""One SSO in front of both surfaces (PACT chat-sso-integration).

Two halves, deliberately in ONE suite, because ``acc.chat-sso-integration
.one-sso`` asks for the refusal arms and the working arms together: a
refusal proved in a file where nothing ever succeeds is a refusal that might
be firing for the wrong reason.

* **Declaration.** ``chat.sso`` is parsed strictly. The mode is declared,
  never inferred; multi-user with any element missing is refused BY NAME;
  single-operator carrying identity-provider elements is refused too, which
  is the arm that actually stops the silent downgrade.
* **Behavior.** A real ``SsoFrontProxy`` fronts two real upstreams against a
  real stub identity provider, all over real HTTP on real sockets. One login
  reaches both surfaces, an unauthenticated request to either is redirected
  and never served, sign-out ends both, and a client-supplied principal
  header never reaches an upstream.

The harness is three WSGI applications and a thread each. Nothing is mocked
at the boundary under test: the decisions come from
``okg.substrate.chat.sso``, and the transcripts are the responses those
decisions produced.
"""
from __future__ import annotations

import base64
import http.client
import json
import threading
import time
import urllib.parse
from dataclasses import replace
from pathlib import Path
from typing import Any, Iterator
from wsgiref.simple_server import WSGIRequestHandler, make_server

import pytest

from okg.substrate.chat.sso import (
    AUTHORIZED,
    CHAT_SURFACE,
    CONSOLE_EVIDENCE_DECLARED,
    CONSOLE_EVIDENCE_MEASURED,
    CONSOLE_SURFACE,
    ConsoleBuild,
    DENY,
    console_base_awareness,
    GROUP_MEMBERSHIP_ABSENT,
    PROXY_CALLBACK_PATH,
    PROXY_SIGN_IN_PATH,
    PROXY_SIGN_OUT_PATH,
    REDIRECT,
    SERVE,
    SESSION_COOKIE_NAME,
    ChatSsoStartupError,
    OidcDiscoveryClient,
    SessionStore,
    SsoFrontProxy,
    Upstream,
    authorize_surface,
    render_nginx_site,
    render_oauth2_proxy_cfg,
    require_sso_startup_contract,
    resolve_client_secret,
    sanitize_inbound_headers,
    surface_routes_from_deep_links,
)
from okg.substrate.deployments.chat_block import (
    SSO_IDP_ELEMENTS,
    SSO_MODE_MULTI_USER,
    SSO_MODE_SINGLE_OPERATOR,
    ChatSsoError,
    parse_chat_block,
    schema_field_census,
)
from okg.substrate.mcp.principal_mapping import PrincipalMapping


REPO_ROOT = Path(__file__).resolve().parents[2]

DEPLOYMENT = "fixture-deployment"
ADMIN_GROUP = "okg-admins"
USER_GROUP = "okg-users"
PRINCIPAL_HEADER = "X-OKG-Principal"
SECRET_ENV = "OKG_CHAT_OIDC_CLIENT_SECRET"

ADMIN_PRINCIPAL = "ada@example.edu"
USER_PRINCIPAL = "user@example.edu"
OUTSIDER_PRINCIPAL = "nobody@example.edu"


def _sso_block(**overrides: Any) -> dict:
    """A complete, legal ``chat.sso`` declaration."""
    block = {
        "mode": SSO_MODE_MULTI_USER,
        "provider_id": "okg-chat-sso",
        "issuer": "https://idp.example.edu/",
        "audience": "okg-mcp",
        "discovery_endpoint": (
            "https://idp.example.edu/.well-known/openid-configuration"
        ),
        "client_id": "okg-chat",
        "client_secret_env": SECRET_ENV,
        "principal_header": PRINCIPAL_HEADER,
        "groups_claim": "groups",
        "admin_group": ADMIN_GROUP,
        "user_group": USER_GROUP,
    }
    block.update(overrides)
    return block


def _chat_block(sso: dict | None = None, **overrides: Any) -> dict:
    block: dict = {
        "enabled": True,
        # No `theme`: the knob census measured theme.accent and theme.base as
        # not drivable at the pinned Open WebUI version, so chat.ui.theme is a
        # refused field (PR #1040). Declaring one here would make every arm in
        # this file fail on the manifest rather than on what it is testing.
        "ui": {"site_name": "Fixture"},
        "models": {"builtin": ["anthropic/claude-sonnet-5"]},
        "preset": {"display_name": "Fixture"},
        "search_profile": "analyst",
        "mcp": {
            "port": 8101,
            "tools": ["inspect", "search"],
            "auth_token_env": "OKG_CHAT_MCP_TOKEN",
        },
        "access": {"groups": [ADMIN_GROUP, USER_GROUP]},
        "deep_links": {
            "chat_site_base": "https://chat.okg.example",
            "operator_ui_base": "https://console.okg.example",
        },
    }
    if sso is not None:
        block["sso"] = sso
    block.update(overrides)
    return block


def _raw_manifest(chat: dict | None) -> dict:
    return {
        "name": DEPLOYMENT,
        "search": {
            "default_profile": "analyst",
            "profiles": {"analyst": {"subtypes": ["chunk"]}},
        },
        **({"chat": chat} if chat is not None else {}),
    }


def _parse(chat: dict | None):
    # No `repo_root`: the census is bound to the schema by test rather than
    # read from the artifact at load time (PR #1040), because the census
    # lives under pact/ and is not packaged.
    return parse_chat_block(_raw_manifest(chat))


MAPPING = PrincipalMapping(
    provider_id="okg-chat-sso",
    issuer="https://idp.example.edu/",
    audience="okg-mcp",
    subject_claim="email",
    subject_header=PRINCIPAL_HEADER,
    principal_kind="mcp_client",
)


# ==========================================================================
# Declaration: the mode is declared, never inferred
# ==========================================================================


def test_complete_multi_user_declaration_is_accepted() -> None:
    """The positive control. Without it every refusal below could be vacuous."""
    block = _parse(_chat_block(_sso_block()))
    assert block is not None
    assert block.sso is not None
    assert block.sso.multi_user is True
    assert block.sso.mode == SSO_MODE_MULTI_USER
    assert block.sso.projected_groups == (ADMIN_GROUP, USER_GROUP)
    for element in SSO_IDP_ELEMENTS:
        assert getattr(block.sso, element), element


def test_single_operator_declaration_is_accepted_and_carries_no_idp() -> None:
    block = _parse(_chat_block({"mode": SSO_MODE_SINGLE_OPERATOR}))
    assert block is not None
    assert block.sso is not None
    assert block.sso.multi_user is False
    assert block.sso.projected_groups == ()


def test_manifest_without_an_sso_block_still_loads_unchanged() -> None:
    """A pre-change manifest is not retroactively broken."""
    block = _parse(_chat_block())
    assert block is not None
    assert block.sso is None


@pytest.mark.parametrize("missing", SSO_IDP_ELEMENTS)
def test_multi_user_missing_one_element_refuses_and_names_it(
    missing: str,
) -> None:
    """Each single-missing-element arm, refused with the element named.

    Parametrized over the whole contract rather than spot-checked, so an
    element added to the schema without being added to the completeness rule
    fails here instead of shipping half-declared.
    """
    sso = _sso_block()
    sso.pop(missing)
    with pytest.raises(ChatSsoError) as excinfo:
        _parse(_chat_block(sso))
    error = excinfo.value
    assert error.code == "chat_sso_invalid"
    assert missing in error.data["missing"]
    assert missing in error.message
    # And the refusal is about incompleteness, not about the mode being
    # unreadable: the mode WAS declared.
    assert error.data["mode"] == SSO_MODE_MULTI_USER


def test_client_id_without_discovery_endpoint_refuses() -> None:
    """The scenario's own example, spelled out rather than left to the sweep."""
    sso = _sso_block()
    sso.pop("discovery_endpoint")
    with pytest.raises(ChatSsoError) as excinfo:
        _parse(_chat_block(sso))
    assert "discovery_endpoint" in excinfo.value.message
    assert excinfo.value.data["missing"] == ["discovery_endpoint"]


def test_partial_config_never_selects_single_operator_mode() -> None:
    """The no-inference rule, on the shape that would tempt an inference.

    A block carrying a client id and an issuer but no mode is exactly the
    input a "guess from what is present" implementation would read as
    multi-user; a block carrying only a client id is what it would read as
    single-operator. Both refuse, and the refusal names what IS declared so
    the operator can see which inference was declined.
    """
    for present in (
        {"client_id": "okg-chat"},
        {"client_id": "okg-chat", "issuer": "https://idp.example.edu/"},
        {"provider_id": "okg-chat-sso"},
    ):
        with pytest.raises(ChatSsoError) as excinfo:
            _parse(_chat_block(dict(present)))
        error = excinfo.value
        assert error.data["path"] == "chat.sso.mode"
        assert error.data["observed"] == "absent"
        assert error.data["declared_idp_elements"] == sorted(present)
        assert SSO_MODE_SINGLE_OPERATOR in error.message
        assert SSO_MODE_MULTI_USER in error.message


def test_empty_sso_block_refuses_rather_than_defaulting() -> None:
    """The emptiest partial config of all still refuses."""
    with pytest.raises(ChatSsoError) as excinfo:
        _parse(_chat_block({}))
    assert excinfo.value.data["declared_idp_elements"] == []


@pytest.mark.parametrize("element", SSO_IDP_ELEMENTS)
def test_single_operator_with_any_idp_element_refuses(element: str) -> None:
    """The mirror arm: an identity provider under single-operator mode.

    This is the refusal that stops the downgrade. Without it, flipping the
    mode back to single-operator while leaving the provider configured would
    be a working manifest that quietly serves every caller as one principal.
    """
    complete = _sso_block()
    sso = {"mode": SSO_MODE_SINGLE_OPERATOR, element: complete[element]}
    with pytest.raises(ChatSsoError) as excinfo:
        _parse(_chat_block(sso))
    assert excinfo.value.data["declared_idp_elements"] == [element]
    assert SSO_MODE_MULTI_USER in excinfo.value.fix_hint


def test_unknown_key_under_sso_refuses() -> None:
    """Strict at this level too, like every other level of the block."""
    from okg.substrate.deployments.chat_block import ChatManifestError

    with pytest.raises(ChatManifestError) as excinfo:
        _parse(_chat_block(_sso_block(clientid="typo")))
    assert "clientid" in excinfo.value.data["unknown"]
    assert excinfo.value.data["path"] == "chat.sso"


def test_unrecognized_mode_refuses_rather_than_resolving() -> None:
    with pytest.raises(ChatSsoError) as excinfo:
        _parse(_chat_block(_sso_block(mode="sso")))
    assert excinfo.value.data["modes"] == sorted(
        [SSO_MODE_MULTI_USER, SSO_MODE_SINGLE_OPERATOR]
    )


def test_admin_and_user_group_must_differ() -> None:
    with pytest.raises(ChatSsoError) as excinfo:
        _parse(_chat_block(_sso_block(user_group=ADMIN_GROUP)))
    assert "capability tiers" in excinfo.value.message


def test_principal_header_must_be_a_header_name() -> None:
    with pytest.raises(ChatSsoError):
        _parse(_chat_block(_sso_block(principal_header="X OKG Principal")))


def test_principal_header_agrees_with_the_mapping_parser() -> None:
    """One definition of a header name, checked rather than asserted.

    The block imports ``mcp/principal_mapping``'s regex. This proves the
    import is load-bearing: a header this block accepts is a header that
    parser accepts too, so no manifest can load here and refuse there.
    """
    from okg.substrate.mcp.principal_mapping import parse_principal_mapping

    block = _parse(_chat_block(_sso_block(principal_header="X-Okg-Chat-User")))
    assert block is not None and block.sso is not None
    mapping = parse_principal_mapping({
        "provider_id": "p",
        "issuer": "i",
        "audience": "a",
        "subject_claim": "email",
        "subject_header": block.sso.principal_header,
        "principal_kind": "mcp_client",
    })
    assert mapping.subject_header == "X-Okg-Chat-User"


def test_cleartext_discovery_endpoint_on_a_routable_host_refuses() -> None:
    with pytest.raises(ChatSsoError) as excinfo:
        _parse(_chat_block(_sso_block(
            discovery_endpoint="http://idp.example.edu/.well-known/openid-configuration",
        )))
    assert excinfo.value.data["host"] == "idp.example.edu"


def test_loopback_discovery_endpoint_over_cleartext_is_allowed() -> None:
    """A local test IdP stays usable; the refusal is about routable hosts."""
    block = _parse(_chat_block(_sso_block(
        discovery_endpoint="http://127.0.0.1:9999/.well-known/openid-configuration",
    )))
    assert block is not None and block.sso is not None


# ==========================================================================
# Secrets
# ==========================================================================


def test_the_schema_declares_no_credential_bearing_sso_field() -> None:
    """No secret has a place to be written. Mechanical, not a promise."""
    from okg.substrate.deployments.chat_block import _is_credential_shaped

    census = schema_field_census()
    assert "chat.sso" in census
    offenders = sorted(
        name for name in census["chat.sso"] if _is_credential_shaped(name)
    )
    assert offenders == []
    assert "client_secret" not in census["chat.sso"]
    assert "client_secret_env" in census["chat.sso"]


def test_a_literal_secret_in_client_secret_env_is_refused_without_echoing_it() -> None:
    """The likeliest paste, refused, and the value withheld from the error.

    The message and the payload are searched for the literal because an
    error that echoed it would copy the secret into logs, CI output and
    evidence artifacts.
    """
    literal = "sk-live-9f2b41c8d6a74e0b"
    with pytest.raises(ChatSsoError) as excinfo:
        _parse(_chat_block(_sso_block(client_secret_env=literal)))
    error = excinfo.value
    assert literal not in error.message
    assert literal not in json.dumps(error.data)
    assert error.data["observed"] == "<withheld>"


@pytest.mark.parametrize(
    "environ, reason",
    [
        ({}, "unset"),
        ({SECRET_ENV: "   "}, "empty"),
        ({SECRET_ENV: "secret\n"}, "padded"),
    ],
)
def test_unresolvable_client_secret_refuses_to_start(
    environ: dict, reason: str,
) -> None:
    block = _parse(_chat_block(_sso_block()))
    assert block is not None and block.sso is not None
    with pytest.raises(ChatSsoStartupError) as excinfo:
        resolve_client_secret(block.sso, environ=environ)
    assert excinfo.value.data["reason"] == reason
    assert "secret" not in str(excinfo.value.data.get("client_secret_env", ""))


def test_a_resolvable_client_secret_is_returned_verbatim() -> None:
    block = _parse(_chat_block(_sso_block()))
    assert block is not None and block.sso is not None
    assert resolve_client_secret(
        block.sso, environ={SECRET_ENV: "s3cr3t"},
    ) == "s3cr3t"


# ==========================================================================
# Startup contract: the two halves of the identity model must agree
# ==========================================================================


def _decision(chat_overrides: dict | None = None, **kwargs: Any):
    chat = _parse(_chat_block(_sso_block(), **(chat_overrides or {})))
    return require_sso_startup_contract(
        deployment=DEPLOYMENT,
        chat=chat,
        principal_mapping=kwargs.pop("principal_mapping", MAPPING),
        environ={SECRET_ENV: "s3cr3t"},
        **kwargs,
    )


def test_startup_contract_accepts_an_aligned_deployment() -> None:
    decision = _decision()
    assert decision.multi_user is True
    assert decision.principal_header == PRINCIPAL_HEADER
    assert decision.projected_groups == (ADMIN_GROUP, USER_GROUP)
    assert {route.surface for route in decision.routes} == {
        CHAT_SURFACE, CONSOLE_SURFACE,
    }


def test_multi_user_without_a_principal_mapping_refuses() -> None:
    with pytest.raises(ChatSsoStartupError) as excinfo:
        _decision(principal_mapping=None)
    assert excinfo.value.data["reason"] == "principal_mapping_absent"
    assert "mcp-principal-mapping" in excinfo.value.message


def test_single_operator_with_a_principal_mapping_refuses() -> None:
    chat = _parse(_chat_block({"mode": SSO_MODE_SINGLE_OPERATOR}))
    with pytest.raises(ChatSsoStartupError) as excinfo:
        require_sso_startup_contract(
            deployment=DEPLOYMENT, chat=chat, principal_mapping=MAPPING,
        )
    assert excinfo.value.data["reason"] == "mode_mapping_skew"


def test_single_operator_alone_is_accepted_and_needs_no_secret() -> None:
    chat = _parse(_chat_block({"mode": SSO_MODE_SINGLE_OPERATOR}))
    decision = require_sso_startup_contract(
        deployment=DEPLOYMENT, chat=chat, principal_mapping=None, environ={},
    )
    assert decision.multi_user is False
    assert decision.mode == SSO_MODE_SINGLE_OPERATOR


def test_an_undeclared_sso_block_refuses_at_the_sso_surface() -> None:
    chat = _parse(_chat_block())
    with pytest.raises(ChatSsoStartupError) as excinfo:
        require_sso_startup_contract(
            deployment=DEPLOYMENT, chat=chat, principal_mapping=None,
        )
    assert excinfo.value.data["reason"] == "sso_undeclared"


@pytest.mark.parametrize(
    "field, value, key",
    [
        ("subject_header", "X-Other-Principal", "principal_header"),
        ("provider_id", "other-provider", "provider_id"),
        ("issuer", "https://other.example/", "issuer"),
        ("audience", "other-audience", "audience"),
    ],
)
def test_skew_against_the_principal_mapping_refuses(
    field: str, value: str, key: str,
) -> None:
    """Each way the front and the substrate could name different things."""
    with pytest.raises(ChatSsoStartupError) as excinfo:
        _decision(principal_mapping=replace(MAPPING, **{field: value}))
    assert excinfo.value.data["reason"] == "principal_mapping_skew"
    assert key in excinfo.value.data["skew"]


def test_group_claim_declared_as_a_principal_header_refuses() -> None:
    """The SSO-side statement of a refusal mcp-principal-mapping also makes.

    Not a weakening of that refusal and not a replacement for it: this one
    names the SSO reason (groups are projected server-side), and the parser
    over there still refuses the reserved claim on its own.
    """
    mapping = replace(MAPPING, extra_claim_headers={"groups": "X-OKG-Groups"})
    with pytest.raises(ChatSsoStartupError) as excinfo:
        _decision(principal_mapping=mapping)
    assert excinfo.value.data["reason"] == "group_claim_as_header"


def test_the_reserved_group_claim_is_still_refused_by_the_mapping_parser() -> None:
    """Non-weakening control: the merged refusal is untouched by this PACT."""
    from okg.substrate.errors import SubstrateError
    from okg.substrate.mcp.principal_mapping import parse_principal_mapping

    with pytest.raises(SubstrateError) as excinfo:
        parse_principal_mapping({
            "provider_id": "p",
            "issuer": "i",
            "audience": "a",
            "subject_claim": "email",
            "subject_header": PRINCIPAL_HEADER,
            "principal_kind": "mcp_client",
            "extra_claim_headers": {"groups": "X-OKG-Groups"},
        })
    assert "reserved claim" in excinfo.value.message


def test_multi_user_without_transport_authentication_refuses() -> None:
    """Named at bring-up rather than at the first mcp-serve start."""
    chat_block = _chat_block(_sso_block())
    chat_block["mcp"] = {"port": 8101, "tools": ["inspect"]}
    chat = _parse(chat_block)
    with pytest.raises(ChatSsoStartupError) as excinfo:
        require_sso_startup_contract(
            deployment=DEPLOYMENT,
            chat=chat,
            principal_mapping=MAPPING,
            environ={SECRET_ENV: "s3cr3t"},
        )
    assert excinfo.value.data["reason"] == "transport_auth_absent"


def test_access_groups_outside_the_projection_refuse() -> None:
    """One group vocabulary. A third name is an ACL that grants nothing."""
    chat_block = _chat_block(_sso_block())
    chat_block["access"] = {"groups": ["cms-operators"]}
    with pytest.raises(ChatSsoError) as excinfo:
        _parse(chat_block)
    assert excinfo.value.data["stray"] == ["cms-operators"]


# --------------------------------------------------------------------------
# The console path prefix. The TOPOLOGY is sanctioned (design.md section 10
# decision 11); what is gated is whether the bundle behind it is base-aware.
# Both directions are tested, because a gate that only ever refuses is
# indistinguishable from a ban.
# --------------------------------------------------------------------------


def _prefixed(chat_host: str = "okg.example", console_host: str = "okg.example"):
    chat_block = _chat_block(_sso_block())
    chat_block["deep_links"] = {
        "chat_site_base": f"https://{chat_host}",
        "operator_ui_base": f"https://{console_host}/console",
    }
    return _parse(chat_block)


def _contract(chat, **kwargs: Any):
    return require_sso_startup_contract(
        deployment=DEPLOYMENT,
        chat=chat,
        principal_mapping=MAPPING,
        environ={SECRET_ENV: "s3cr3t"},
        **kwargs,
    )


def _console_index(tmp_path: Path, *refs: str) -> Path:
    body = "".join(
        f'<script type="module" crossorigin src="{ref}"></script>'
        for ref in refs
    )
    index = tmp_path / "index.html"
    index.write_text(f"<!doctype html><html><head>{body}</head></html>")
    return index


def test_a_prefixed_console_with_no_evidence_is_refused() -> None:
    """Not a topology ban: a refusal to serve a prefix on no evidence."""
    with pytest.raises(ChatSsoStartupError) as excinfo:
        _contract(_prefixed())
    error = excinfo.value
    assert error.data["reason"] == "console_base_awareness_undeclared"
    assert error.data["console_prefix"] == "/console"
    assert "read-handshake-transcript.md" in error.data["evidence"]
    # The message must not read as "the prefix is forbidden" -- decision 11
    # sanctions it -- and must name both ways to satisfy the gate.
    assert "permitted" in error.message
    assert "--console-bundle-index" in error.fix_hint
    assert "--console-base-aware" in error.fix_hint


def test_a_measured_non_base_aware_bundle_is_refused(tmp_path: Path) -> None:
    """The build the handshake actually measured, refused on what it IS.

    Root-relative asset references are issued by the browser against the
    ORIGIN, so the prefix never sees them: they 404 and the app never boots
    while the front answers 200.
    """
    index = _console_index(
        tmp_path, "/assets/index-bjARkEC7.js", "/assets/index-9St5Ak6Z.css",
    )
    with pytest.raises(ChatSsoStartupError) as excinfo:
        _contract(
            _prefixed(),
            console_build=ConsoleBuild(index_path=index),
        )
    error = excinfo.value
    assert error.data["reason"] == "console_bundle_not_base_aware"
    assert "/assets/index-bjARkEC7.js" in error.data["root_relative_assets"]
    assert "base='/console/'" in error.fix_hint


def test_a_measured_non_base_aware_bundle_is_refused_despite_a_declaration(
    tmp_path: Path,
) -> None:
    """Measurement beats a promise, in the direction that matters."""
    index = _console_index(tmp_path, "/assets/index-bjARkEC7.js")
    with pytest.raises(ChatSsoStartupError) as excinfo:
        _contract(
            _prefixed(),
            console_build=ConsoleBuild(
                declared_base_aware=True, index_path=index,
            ),
        )
    assert excinfo.value.data["reason"] == "console_bundle_not_base_aware"
    assert excinfo.value.data["declared_base_aware"] is True


@pytest.mark.parametrize(
    "refs",
    [
        # Vite base='/console/'
        ("/console/assets/index-bjARkEC7.js", "/console/assets/index.css"),
        # Vite base='./' -- relative refs resolve against the prefix already
        ("assets/index-bjARkEC7.js", "./assets/index.css"),
        # Absolute and scheme-relative URLs go elsewhere entirely; a prefix
        # cannot strand them, so they must not trip the gate.
        ("https://cdn.example/x.js", "//cdn.example/y.css", "/console/app.js"),
    ],
)
def test_a_base_aware_bundle_under_a_prefix_is_ACCEPTED(
    tmp_path: Path, refs: tuple,
) -> None:
    """The arm that proves this is a gate and not a ban.

    Design decision 11 picks the base-aware build as the resolution of the
    /console question. An operator who does exactly that must be permitted --
    on the SAME host as the chat site, which is the whole point of the
    one-hostname topology.
    """
    index = _console_index(tmp_path, *refs)
    decision = _contract(
        _prefixed(), console_build=ConsoleBuild(index_path=index),
    )
    assert decision.multi_user is True
    console = [r for r in decision.routes if r.surface == CONSOLE_SURFACE][0]
    chat_route = [r for r in decision.routes if r.surface == CHAT_SURFACE][0]
    assert console.path_prefix == "/console"
    # Same host, and both routes live -- the sanctioned one-hostname topology.
    assert console.host == chat_route.host == "okg.example"


def test_a_declared_base_aware_bundle_is_accepted_and_says_it_is_unmeasured(
) -> None:
    """The fallback gate, honest about being a promise.

    The front frequently runs where the console's files are not. A
    declaration permits the prefix; the verdict records that no bundle was
    read, so nothing downstream can report it as a measurement.
    """
    decision = _contract(
        _prefixed(), console_build=ConsoleBuild(declared_base_aware=True),
    )
    assert decision.multi_user is True

    verdict = console_base_awareness(
        "/console",
        ConsoleBuild(declared_base_aware=True),
        deployment=DEPLOYMENT,
    )
    assert verdict.permitted is True
    assert verdict.evidence == CONSOLE_EVIDENCE_DECLARED
    assert verdict.evidence != CONSOLE_EVIDENCE_MEASURED


def test_a_base_aware_bundle_is_accepted_without_any_declaration(
    tmp_path: Path,
) -> None:
    """Measurement beats a promise in the OTHER direction too."""
    index = _console_index(tmp_path, "/console/assets/index.js")
    verdict = console_base_awareness(
        "/console",
        ConsoleBuild(declared_base_aware=False, index_path=index),
        deployment=DEPLOYMENT,
    )
    assert verdict.permitted is True
    assert verdict.evidence == CONSOLE_EVIDENCE_MEASURED


def test_an_unreadable_bundle_is_a_check_that_did_not_happen(
    tmp_path: Path,
) -> None:
    """An absent measurement is not a passing one."""
    with pytest.raises(ChatSsoStartupError) as excinfo:
        _contract(
            _prefixed(),
            console_build=ConsoleBuild(index_path=tmp_path / "nope.html"),
        )
    assert excinfo.value.data["reason"] == "console_bundle_unreadable"


def test_a_root_served_console_needs_no_bundle_evidence() -> None:
    """The gate is about prefixes; a console at the root is unaffected."""
    decision = _contract(_parse(_chat_block(_sso_block())))
    assert decision.multi_user is True
    assert all(route.path_prefix == "" for route in decision.routes)


def test_the_prefix_gate_applies_on_a_separate_host_too(tmp_path: Path) -> None:
    """Keyed on the bundle, not on whether the hosts happen to match.

    The old check keyed on `same host + prefix`, which let a broken bundle
    through on its own hostname -- where the assets 404 against that host's
    root just the same.
    """
    with pytest.raises(ChatSsoStartupError) as excinfo:
        _contract(_prefixed(console_host="console.okg.example"))
    assert excinfo.value.data["reason"] == "console_base_awareness_undeclared"

    index = _console_index(tmp_path, "/console/assets/index.js")
    decision = _contract(
        _prefixed(console_host="console.okg.example"),
        console_build=ConsoleBuild(index_path=index),
    )
    assert decision.multi_user is True


def test_the_startup_contract_actually_resolves_the_secret() -> None:
    """The secret check is WIRED, not merely available.

    ``resolve_client_secret`` has its own arms, but a startup contract that
    never called it would leave every one of them green while the site came
    up with an identity provider it cannot authenticate to.
    """
    chat = _parse(_chat_block(_sso_block()))
    with pytest.raises(ChatSsoStartupError) as excinfo:
        require_sso_startup_contract(
            deployment=DEPLOYMENT,
            chat=chat,
            principal_mapping=MAPPING,
            environ={},
        )
    assert excinfo.value.data["reason"] == "unset"
    # And the escape hatch is explicit, so a caller that skips the check has
    # to say so.
    decision = require_sso_startup_contract(
        deployment=DEPLOYMENT,
        chat=chat,
        principal_mapping=MAPPING,
        environ={},
        check_secret=False,
    )
    assert decision.multi_user is True


def test_a_subject_claim_the_front_cannot_supply_refuses() -> None:
    """``sub`` is a claim oauth2-proxy does not put in a header.

    Accepting it would mean the front stamps an email while the bindings
    are written against opaque ids -- every authenticated person refused as
    unidentified, for a reason that reads like a broken graph.
    """
    from okg.substrate.chat.sso import FRONT_SUPPLIED_CLAIMS

    chat = _parse(_chat_block(_sso_block()))
    with pytest.raises(ChatSsoStartupError) as excinfo:
        require_sso_startup_contract(
            deployment=DEPLOYMENT,
            chat=chat,
            principal_mapping=replace(MAPPING, subject_claim="sub"),
            environ={SECRET_ENV: "s3cr3t"},
        )
    assert excinfo.value.data["reason"] == "subject_claim_unsuppliable"
    assert excinfo.value.data["supported"] == sorted(FRONT_SUPPLIED_CLAIMS)

    # The control: a claim the front CAN supply is accepted, and the
    # decision carries it so the front and the bindings read one name.
    decision = require_sso_startup_contract(
        deployment=DEPLOYMENT,
        chat=chat,
        principal_mapping=replace(MAPPING, subject_claim="preferred_username"),
        environ={SECRET_ENV: "s3cr3t"},
    )
    assert decision.subject_claim == "preferred_username"


def test_the_rendered_front_reads_the_declared_claim() -> None:
    """A different declared claim renders a different nginx variable."""
    chat = _parse(_chat_block(_sso_block()))
    decision = require_sso_startup_contract(
        deployment=DEPLOYMENT,
        chat=chat,
        principal_mapping=replace(MAPPING, subject_claim="preferred_username"),
        environ={SECRET_ENV: "s3cr3t"},
    )
    rendered = render_nginx_site(
        decision,
        upstreams=[
            Upstream(surface=CHAT_SURFACE, base_url="http://127.0.0.1:8080"),
            Upstream(surface=CONSOLE_SURFACE, base_url="http://127.0.0.1:5199"),
        ],
    )
    assert f"proxy_set_header {PRINCIPAL_HEADER} $auth_user;" in rendered
    assert f"proxy_set_header {PRINCIPAL_HEADER} $auth_email;" not in rendered


def test_the_rendered_front_clears_the_whole_identity_namespace() -> None:
    rendered = render_nginx_site(
        _decision(),
        upstreams=[
            Upstream(surface=CHAT_SURFACE, base_url="http://127.0.0.1:8080"),
            Upstream(surface=CONSOLE_SURFACE, base_url="http://127.0.0.1:5199"),
        ],
    )
    for name in (
        "X-Auth-Request-Email",
        "X-Auth-Request-User",
        "X-Auth-Request-Groups",
        "X-Forwarded-User",
        "Remote-User",
        PRINCIPAL_HEADER,
    ):
        assert f'proxy_set_header {name} "";' in rendered, name


def test_the_one_hostname_render_is_one_server_block(tmp_path: Path) -> None:
    """Two `server` blocks with one `server_name` are not two sites.

    nginx matches the first block for that host and the second is dead
    config, so the console published under a prefix on the chat host would
    never be reached — a rendered front that looks complete and serves half
    of itself. One block per HOST, with a location per surface, is the only
    shape that works. This became reachable the moment the prefix topology
    stopped being banned; while it was refused, the bug had nowhere to
    appear.
    """
    index = _console_index(tmp_path, "/console/assets/index.js")
    decision = _contract(
        _prefixed(), console_build=ConsoleBuild(index_path=index),
    )
    rendered = render_nginx_site(
        decision,
        upstreams=[
            Upstream(surface=CHAT_SURFACE, base_url="http://127.0.0.1:8080"),
            Upstream(surface=CONSOLE_SURFACE, base_url="http://127.0.0.1:5199"),
        ],
    )
    assert rendered.count("server_name okg.example;") == 1
    assert rendered.count("server {") == 1
    # Both surfaces are served, from that one block, at distinct locations.
    assert "location / {" in rendered
    assert "location /console/ {" in rendered
    assert "proxy_pass http://127.0.0.1:8080;" in rendered
    assert "proxy_pass http://127.0.0.1:5199;" in rendered
    # And one auth_request per surface, not one per block.
    assert rendered.count("auth_request /oauth2/auth;") == 2
    # The separate-hostname topology still renders two blocks.
    two_hosts = render_nginx_site(
        _decision(),
        upstreams=[
            Upstream(surface=CHAT_SURFACE, base_url="http://127.0.0.1:8080"),
            Upstream(surface=CONSOLE_SURFACE, base_url="http://127.0.0.1:5199"),
        ],
    )
    assert two_hosts.count("server {") == 2


def test_the_rendered_front_carries_websocket_upgrades() -> None:
    """Open WebUI's socket.io channel needs them, or the site never updates."""
    rendered = render_nginx_site(
        _decision(),
        upstreams=[
            Upstream(surface=CHAT_SURFACE, base_url="http://127.0.0.1:8080"),
            Upstream(surface=CONSOLE_SURFACE, base_url="http://127.0.0.1:5199"),
        ],
    )
    assert "map $http_upgrade $okg_connection_upgrade {" in rendered
    assert rendered.count("proxy_set_header Upgrade $http_upgrade;") == 2
    assert rendered.count("proxy_http_version 1.1;") == 2


@pytest.mark.parametrize(
    "chat_host, console_host",
    [
        ("chat.a.ac.uk", "console.b.ac.uk"),
        ("chat.foo.co.uk", "console.bar.co.uk"),
        ("chat.x.github.io", "console.y.github.io"),
    ],
)
def test_a_public_suffix_is_not_a_cookie_domain(
    chat_host: str, console_host: str,
) -> None:
    """Two labels is not the same as a shared owner.

    ``ac.uk`` has a dot and is a registry, not a deployment. Browsers reject
    a cookie scoped there outright, so the outcome of accepting it would be
    a rendered front whose login silently never works.
    """
    chat_block = _chat_block(_sso_block())
    chat_block["deep_links"] = {
        "chat_site_base": f"https://{chat_host}",
        "operator_ui_base": f"https://{console_host}",
    }
    decision = require_sso_startup_contract(
        deployment=DEPLOYMENT,
        chat=_parse(chat_block),
        principal_mapping=MAPPING,
        environ={SECRET_ENV: "s3cr3t"},
    )
    with pytest.raises(ChatSsoStartupError) as excinfo:
        render_oauth2_proxy_cfg(decision)
    assert excinfo.value.data["reason"] == "no_common_cookie_domain"


@pytest.mark.parametrize("field", ["client_id", "issuer", "admin_group"])
def test_a_manifest_value_cannot_inject_a_config_directive(field: str) -> None:
    """A rendered config is line-oriented; a newline is a second directive."""
    payload = 'okg-chat"\nupstreams = [ "http://attacker.example/" ]\n#'
    with pytest.raises(ChatSsoError) as excinfo:
        _parse(_chat_block(_sso_block(**{field: payload})))
    assert excinfo.value.data["reason"] == "config_unsafe_scalar"
    assert excinfo.value.data["path"] == f"chat.sso.{field}"


def test_the_front_refuses_a_route_with_no_upstream() -> None:
    """A route with no upstream answers 502 to an authenticated person."""
    decision = _decision()
    with pytest.raises(ChatSsoStartupError) as excinfo:
        SsoFrontProxy(
            deployment=DEPLOYMENT,
            sso=decision.sso,
            routes=decision.routes,
            upstreams=[
                Upstream(surface=CHAT_SURFACE, base_url="http://127.0.0.1:8080"),
            ],
            idp=object(),
        )
    assert excinfo.value.data["reason"] == "upstream_absent"
    assert excinfo.value.data["missing"] == [CONSOLE_SURFACE]


def test_route_matching_prefers_the_longest_prefix() -> None:
    """The prefix arms are exercised, even though the topology is refused.

    ``surface_routes_from_deep_links`` refuses a console prefix on a shared
    host, so nothing in the harness produces one. That makes ``_route_for``
    and the prefix strip dead to every other test -- which is exactly how a
    route-shadowing bug would survive.
    """
    from okg.substrate.chat.sso import SurfaceRoute

    decision = _decision()
    proxy = SsoFrontProxy(
        deployment=DEPLOYMENT,
        sso=decision.sso,
        routes=[
            SurfaceRoute(surface=CHAT_SURFACE, host="okg.example"),
            SurfaceRoute(
                surface=CONSOLE_SURFACE, host="okg.example", path_prefix="/console",
            ),
        ],
        upstreams=[
            Upstream(surface=CHAT_SURFACE, base_url="http://127.0.0.1:8080"),
            Upstream(surface=CONSOLE_SURFACE, base_url="http://127.0.0.1:5199"),
        ],
        idp=object(),
    )
    assert proxy._route_for("okg.example", "/console/x").surface == CONSOLE_SURFACE
    assert proxy._route_for("okg.example", "/console").surface == CONSOLE_SURFACE
    # Not shadowed by the chat route at "/", and not over-matched either.
    assert proxy._route_for("okg.example", "/consoles").surface == CHAT_SURFACE
    assert proxy._route_for("okg.example", "/").surface == CHAT_SURFACE
    assert proxy._route_for("other.example", "/") is None


def test_missing_surface_bases_refuse() -> None:
    chat_block = _chat_block(_sso_block())
    chat_block.pop("deep_links")
    chat = _parse(chat_block)
    with pytest.raises(ChatSsoStartupError) as excinfo:
        require_sso_startup_contract(
            deployment=DEPLOYMENT,
            chat=chat,
            principal_mapping=MAPPING,
            environ={SECRET_ENV: "s3cr3t"},
        )
    assert excinfo.value.data["reason"] == "surface_base_absent"


# ==========================================================================
# The decision function, independent of transport
# ==========================================================================


def _sso_value():
    block = _parse(_chat_block(_sso_block()))
    assert block is not None and block.sso is not None
    return block.sso


@pytest.mark.parametrize("surface", [CHAT_SURFACE, CONSOLE_SURFACE])
def test_no_session_redirects_on_either_surface(surface: str) -> None:
    decision = authorize_surface(
        surface=surface,
        sso=_sso_value(),
        session=None,
        sign_in_location="/oauth2/start",
    )
    assert decision.outcome == REDIRECT
    assert decision.status == 302
    assert decision.upstream_headers == {}


@pytest.mark.parametrize("surface", [CHAT_SURFACE, CONSOLE_SURFACE])
def test_membership_absence_denies_on_either_surface(surface: str) -> None:
    """Membership absence is a denial, not an absence of restrictions."""
    store = SessionStore()
    session = store.create(principal=OUTSIDER_PRINCIPAL, groups=())
    decision = authorize_surface(
        surface=surface,
        sso=_sso_value(),
        session=session,
        sign_in_location="/oauth2/start",
    )
    assert decision.outcome == DENY
    assert decision.status == 403
    assert decision.reason_code == GROUP_MEMBERSHIP_ABSENT
    assert decision.upstream_headers == {}


def test_expired_session_redirects_rather_than_serving() -> None:
    store = SessionStore(ttl_seconds=1)
    session = store.create(principal=ADMIN_PRINCIPAL, groups=(ADMIN_GROUP,), now=0.0)
    decision = authorize_surface(
        surface=CHAT_SURFACE,
        sso=_sso_value(),
        session=session,
        sign_in_location="/oauth2/start",
        now=100.0,
    )
    assert decision.outcome == REDIRECT
    assert decision.reason_code == "chat.sso.session_expired"


def test_member_is_served_with_the_principal_stamped() -> None:
    store = SessionStore()
    session = store.create(principal=USER_PRINCIPAL, groups=(USER_GROUP,))
    decision = authorize_surface(
        surface=CHAT_SURFACE,
        sso=_sso_value(),
        session=session,
        sign_in_location="/oauth2/start",
    )
    assert decision.outcome == SERVE
    assert decision.reason_code == AUTHORIZED
    assert decision.upstream_headers == {PRINCIPAL_HEADER: USER_PRINCIPAL}


def test_inbound_principal_header_is_stripped_case_insensitively() -> None:
    cleaned = sanitize_inbound_headers(
        {
            "x-okg-principal": OUTSIDER_PRINCIPAL,
            "X-OKG-PRINCIPAL": OUTSIDER_PRINCIPAL,
            "Accept": "text/html",
        },
        _sso_value(),
    )
    assert cleaned == {"Accept": "text/html"}


def test_the_whole_identity_header_namespace_is_stripped() -> None:
    """Not just the declared header.

    ``set_xauthrequest`` makes oauth2-proxy emit the ``X-Auth-Request-*``
    family, and Open WebUI's own trusted-header auth is conventionally
    pointed at ``X-Auth-Request-Email``. A front that stamped a correct
    principal for the graph while forwarding the client's copy of THAT
    header would let the caller pick its chat identity.
    """
    cleaned = sanitize_inbound_headers(
        {
            "X-OKG-Principal": OUTSIDER_PRINCIPAL,
            "X-Auth-Request-Email": OUTSIDER_PRINCIPAL,
            "X-Auth-Request-User": OUTSIDER_PRINCIPAL,
            "X-Auth-Request-Groups": ADMIN_GROUP,
            "x-forwarded-user": OUTSIDER_PRINCIPAL,
            "Remote-User": OUTSIDER_PRINCIPAL,
            "Authorization": "Bearer stolen",
            "X-OKG-Department": "finance",
            "Accept": "text/html",
            "User-Agent": "curl/8",
        },
        _sso_value(),
        extra_claim_headers=("X-OKG-Department",),
    )
    assert cleaned == {"Accept": "text/html", "User-Agent": "curl/8"}
    assert OUTSIDER_PRINCIPAL not in json.dumps(cleaned)


def test_the_session_cookie_is_hardened() -> None:
    """HttpOnly and SameSite always; Secure exactly when the site is https."""
    from okg.substrate.chat.sso import _cookie

    over_https = _cookie(SESSION_COOKIE_NAME, "abc", secure=True)
    assert "HttpOnly" in over_https
    assert "SameSite=Lax" in over_https
    assert "Secure" in over_https
    # Not unconditional: a `Secure` cookie is never sent over http, so a
    # loopback bring-up would log a person in and then never see them again.
    over_http = _cookie(SESSION_COOKIE_NAME, "abc", secure=False)
    assert "Secure" not in over_http
    assert "HttpOnly" in over_http


# ==========================================================================
# Rendered production front
# ==========================================================================


def test_rendered_oauth2_proxy_shares_one_cookie_across_both_hosts() -> None:
    decision = _decision()
    rendered = render_oauth2_proxy_cfg(decision)
    assert 'cookie_domain = "okg.example"' in rendered
    assert f'cookie_name = "{SESSION_COOKIE_NAME}"' in rendered
    assert 'oidc_groups_claim = "groups"' in rendered
    # A login that BEGAN on the console must come back to the console. The
    # single registered redirect URI is on the chat host, so the return hop
    # is cross-host and oauth2-proxy drops it unless the domain is
    # whitelisted -- silently landing the person on the chat site.
    assert 'whitelist_domains = [ ".okg.example" ]' in rendered
    assert "/oauth2/callback" in rendered
    assert ADMIN_GROUP in rendered and USER_GROUP in rendered
    # The secret is named, never carried.
    assert SECRET_ENV in rendered
    assert "s3cr3t" not in rendered
    assert "client_secret =" not in rendered


def test_rendered_nginx_fronts_both_surfaces_through_one_proxy() -> None:
    decision = _decision()
    rendered = render_nginx_site(
        decision,
        upstreams=[
            Upstream(surface=CHAT_SURFACE, base_url="http://127.0.0.1:8080"),
            Upstream(surface=CONSOLE_SURFACE, base_url="http://127.0.0.1:5199"),
        ],
    )
    assert rendered.count("auth_request /oauth2/auth;") == 2
    assert rendered.count("proxy_pass http://127.0.0.1:4180;") == 4
    assert "server_name chat.okg.example;" in rendered
    assert "server_name console.okg.example;" in rendered
    # The client's copy is cleared before the front's value is set, in that
    # order: the upstream refuses a repeated principal header as ambiguous.
    cleared = rendered.index(f'proxy_set_header {PRINCIPAL_HEADER} "";')
    stamped = rendered.index(f"proxy_set_header {PRINCIPAL_HEADER} $auth_email;")
    assert cleared < stamped


def test_rendered_nginx_distinguishes_unauthenticated_from_unauthorized() -> None:
    """403 is a denial, not a reason to log in again.

    Mapping ``error_page 403`` to the sign-in flow -- which is what the
    docs-site template does with 401 alone, and what a copy of it would do
    to both -- sends a person ``allowed_groups`` refused back through a
    login they already completed. They would loop, and never be told they
    are in neither group.
    """
    rendered = render_nginx_site(
        _decision(),
        upstreams=[
            Upstream(surface=CHAT_SURFACE, base_url="http://127.0.0.1:8080"),
            Upstream(surface=CONSOLE_SURFACE, base_url="http://127.0.0.1:5199"),
        ],
    )
    assert "error_page 401 403 = @oauth2_sign_in;" not in rendered
    assert rendered.count("error_page 401 = @oauth2_sign_in;") == 2
    assert rendered.count("error_page 403 = @sso_group_denied_") == 2
    # The typed reason is the SAME constant the reference front and the
    # group projection use, so all three surfaces say one thing.
    assert rendered.count(GROUP_MEMBERSHIP_ABSENT) >= 2


def test_rendered_nginx_refuses_an_authenticated_request_that_named_nobody(
) -> None:
    """An empty principal is a denial, never a forwarded anonymous request.

    Without this, an ``auth_request`` that succeeded but set no email leaves
    ``$auth_email`` empty, nginx skips the header, and the upstream receives
    a request with no principal at all -- which mcp-serve reports as a
    missing header and an operator reads as a client fault.
    """
    rendered = render_nginx_site(
        _decision(),
        upstreams=[
            Upstream(surface=CHAT_SURFACE, base_url="http://127.0.0.1:8080"),
            Upstream(surface=CONSOLE_SURFACE, base_url="http://127.0.0.1:5199"),
        ],
    )
    assert rendered.count('if ($auth_email = "") {') == 2
    guard = rendered.index('if ($auth_email = "") {')
    stamp = rendered.index(f"proxy_set_header {PRINCIPAL_HEADER} $auth_email;")
    assert guard < stamp


def test_surfaces_with_no_common_domain_refuse_one_cookie() -> None:
    chat_block = _chat_block(_sso_block())
    chat_block["deep_links"] = {
        "chat_site_base": "https://chat.example.edu",
        "operator_ui_base": "https://console.other.test",
    }
    chat = _parse(chat_block)
    decision = require_sso_startup_contract(
        deployment=DEPLOYMENT,
        chat=chat,
        principal_mapping=MAPPING,
        environ={SECRET_ENV: "s3cr3t"},
    )
    with pytest.raises(ChatSsoStartupError) as excinfo:
        render_oauth2_proxy_cfg(decision)
    assert excinfo.value.data["reason"] == "no_common_cookie_domain"


# ==========================================================================
# Proxy-level integration harness: real sockets, real redirects
# ==========================================================================


class _QuietHandler(WSGIRequestHandler):
    def log_message(self, *args: Any) -> None:  # noqa: D102
        return


class _Served:
    """One WSGI app on a background thread, at an OS-assigned port."""

    def __init__(self, app: Any) -> None:
        self._server = make_server(
            "127.0.0.1", 0, app, handler_class=_QuietHandler,
        )
        self.port = self._server.server_address[1]
        self._thread = threading.Thread(
            target=self._server.serve_forever, daemon=True,
        )
        self._thread.start()

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)


def _b64(payload: dict) -> str:
    raw = json.dumps(payload).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


class StubIdp:
    """A minimal OIDC provider: discovery, authorize, token.

    Not a mock of the client under test -- a server the client actually
    talks to over a socket. It also COUNTS authorize hits, which is how the
    "one login, two surfaces" arm proves the second surface did not quietly
    log in again.

    Its ``sub`` is deliberately DIFFERENT from its ``email``. When the two
    were the same string, no arm could tell which claim the front read, and
    changing the claim selection to a constant left the suite green. A fake
    whose values collide is a fake that hides the thing under test.
    """

    def __init__(self) -> None:
        self.directory: dict[str, tuple[str, tuple[str, ...]]] = {}
        self.authorize_hits = 0
        self.token_hits = 0
        self.signed_in: str | None = None
        self._codes: dict[str, str] = {}
        self._served = _Served(self)
        self.base = f"http://127.0.0.1:{self._served.port}"

    @property
    def discovery_endpoint(self) -> str:
        return f"{self.base}/.well-known/openid-configuration"

    def close(self) -> None:
        self._served.close()

    def __call__(self, environ, start_response):  # noqa: ANN001, ANN201
        path = environ.get("PATH_INFO", "")
        params = urllib.parse.parse_qs(environ.get("QUERY_STRING", ""))
        if path == "/.well-known/openid-configuration":
            body = json.dumps({
                "issuer": self.base,
                "authorization_endpoint": f"{self.base}/authorize",
                "token_endpoint": f"{self.base}/token",
            }).encode("utf-8")
            start_response("200 OK", [("Content-Type", "application/json")])
            return [body]
        if path == "/authorize":
            self.authorize_hits += 1
            if self.signed_in is None:
                start_response("401 Unauthorized", [])
                return [b"no signed-in user at the identity provider"]
            code = f"code-{self.authorize_hits}"
            self._codes[code] = self.signed_in
            location = params["redirect_uri"][0] + "?" + urllib.parse.urlencode({
                "code": code, "state": params["state"][0],
            })
            start_response("302 Found", [("Location", location)])
            return [b""]
        if path == "/token":
            self.token_hits += 1
            length = int(environ.get("CONTENT_LENGTH") or 0)
            form = urllib.parse.parse_qs(
                environ["wsgi.input"].read(length).decode("utf-8")
            )
            principal = self._codes.pop(form["code"][0])
            email, groups = self.directory[principal]
            id_token = ".".join([
                _b64({"alg": "none"}),
                _b64({
                    # A DIFFERENT namespace from the email, so an arm that
                    # reads the wrong claim is visible rather than silently
                    # equivalent.
                    "sub": f"opaque-id-for-{email}",
                    "email": email,
                    "groups": list(groups),
                    "iss": self.base,
                    "aud": "okg-chat",
                    "exp": time.time() + 3600,
                }),
                "signature",
            ])
            body = json.dumps({
                "access_token": "opaque", "id_token": id_token,
            }).encode("utf-8")
            start_response("200 OK", [("Content-Type", "application/json")])
            return [body]
        start_response("404 Not Found", [])
        return [b""]


def _echo_app(surface: str):
    """An upstream that reports exactly which headers reached it.

    Keys are lower-cased, because HTTP field names are case-insensitive and
    every hop in this chain (urllib, wsgiref) re-cases them. Asserting on a
    particular casing would be asserting on plumbing, not on the contract.
    """

    def app(environ, start_response):  # noqa: ANN001, ANN202
        headers = {
            key[5:].replace("_", "-").lower(): value
            for key, value in environ.items()
            if key.startswith("HTTP_")
        }
        body = json.dumps({
            "surface": surface,
            "path": environ.get("PATH_INFO"),
            "headers": headers,
        }).encode("utf-8")
        start_response("200 OK", [("Content-Type", "application/json")])
        return [body]

    return app


class Browser:
    """A cookie jar and an HTTP client. Redirects are followed EXPLICITLY.

    Following them by hand is the point: each hop is asserted, so "the
    unauthenticated request was redirected" is a recorded 302 to a named
    location rather than an outcome inferred from a final 200.
    """

    def __init__(self, proxy_port: int) -> None:
        self.proxy_port = proxy_port
        self.cookies: dict[str, str] = {}
        self.transcript: list[tuple[str, str, int, str | None]] = []

    def get(self, url: str, *, extra_headers: dict | None = None):
        parts = urllib.parse.urlsplit(url)
        # Every host in this harness resolves to loopback; the Host header
        # is what selects the surface, exactly as it would behind a real
        # front.
        port = parts.port or self.proxy_port
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
        headers = {"Host": parts.netloc}
        if self.cookies:
            headers["Cookie"] = "; ".join(
                f"{key}={value}" for key, value in self.cookies.items()
            )
        headers.update(extra_headers or {})
        target = parts.path or "/"
        if parts.query:
            target += f"?{parts.query}"
        connection.request("GET", target, headers=headers)
        response = connection.getresponse()
        body = response.read()
        for key, value in response.getheaders():
            if key.lower() != "set-cookie":
                continue
            name, _, rest = value.partition("=")
            cookie_value = rest.split(";")[0]
            if "Max-Age=0" in value or not cookie_value:
                self.cookies.pop(name, None)
            else:
                self.cookies[name] = cookie_value
        location = response.getheader("Location")
        self.transcript.append((url, target, response.status, location))
        connection.close()
        return response.status, dict(response.getheaders()), body

    def follow(self, url: str, *, limit: int = 6, **kwargs: Any):
        status, headers, body = self.get(url, **kwargs)
        hops = 0
        while status in {301, 302, 303, 307, 308} and hops < limit:
            location = headers["Location"]
            url = urllib.parse.urljoin(url, location)
            status, headers, body = self.get(url)
            hops += 1
        return status, headers, body


@pytest.fixture()
def front() -> Iterator[dict[str, Any]]:
    """Two upstreams, one identity provider, one front. All on sockets."""
    idp = StubIdp()
    idp.directory = {
        ADMIN_PRINCIPAL: (ADMIN_PRINCIPAL, (ADMIN_GROUP,)),
        USER_PRINCIPAL: (USER_PRINCIPAL, (USER_GROUP,)),
        OUTSIDER_PRINCIPAL: (OUTSIDER_PRINCIPAL, ()),
    }
    chat_upstream = _Served(_echo_app(CHAT_SURFACE))
    console_upstream = _Served(_echo_app(CONSOLE_SURFACE))

    # The manifest's issuer matches the provider's, as a real deployment's
    # would. (Leaving it as the placeholder made every login fail on the
    # token's issuer check -- which is the check doing its job.)
    chat_block = _chat_block(_sso_block(
        discovery_endpoint=idp.discovery_endpoint, issuer=idp.base,
    ))
    parsed = _parse(chat_block)
    assert parsed is not None and parsed.sso is not None
    sso = parsed.sso

    proxy_holder: dict[str, Any] = {}
    proxy_app = _LateBoundApp(proxy_holder)
    served_proxy = _Served(proxy_app)
    port = served_proxy.port

    routes = surface_routes_from_deep_links(
        type("DeepLinks", (), {
            "chat_site_base": f"http://chat.okg.test:{port}",
            "operator_ui_base": f"http://console.okg.test:{port}",
        })(),
        deployment=DEPLOYMENT,
    )
    proxy = SsoFrontProxy(
        deployment=DEPLOYMENT,
        sso=sso,
        routes=routes,
        upstreams=[
            Upstream(
                surface=CHAT_SURFACE,
                base_url=f"http://127.0.0.1:{chat_upstream.port}",
            ),
            Upstream(
                surface=CONSOLE_SURFACE,
                base_url=f"http://127.0.0.1:{console_upstream.port}",
            ),
        ],
        idp=OidcDiscoveryClient(
            sso, subject_claim="email", environ={SECRET_ENV: "s3cr3t"},
        ),
        store=SessionStore(),
        extra_claim_headers=("X-OKG-Department",),
    )
    proxy_holder["app"] = proxy
    try:
        yield {
            "proxy": proxy,
            "idp": idp,
            "port": port,
            "chat_url": f"http://chat.okg.test:{port}/",
            "console_url": f"http://console.okg.test:{port}/",
        }
    finally:
        served_proxy.close()
        chat_upstream.close()
        console_upstream.close()
        idp.close()


class _LateBoundApp:
    """Lets the served socket exist before the proxy that needs its port."""

    def __init__(self, holder: dict[str, Any]) -> None:
        self._holder = holder

    def __call__(self, environ, start_response):  # noqa: ANN001, ANN201
        return self._holder["app"](environ, start_response)


def _sign_in(front: dict, principal: str) -> Browser:
    front["idp"].signed_in = principal
    browser = Browser(front["port"])
    status, _, _ = browser.follow(front["chat_url"])
    assert status in {200, 403}, status
    return browser


def test_unauthenticated_request_to_either_surface_is_redirected(front) -> None:
    """Never served. The 302 and its target are both asserted."""
    browser = Browser(front["port"])
    for url in (front["chat_url"], front["console_url"]):
        status, headers, body = browser.get(url)
        assert status == 302, url
        assert headers["X-OKG-SSO-Reason"] == "chat.sso.unauthenticated"
        assert headers["Location"].startswith(PROXY_SIGN_IN_PATH)
        assert body == b""
    # Nothing reached an upstream: no decision was a serve.
    assert all(item.outcome == REDIRECT for item in front["proxy"].decisions)


def test_one_login_serves_both_surfaces(front) -> None:
    """The requirement, end to end.

    The second surface is fetched with the SAME cookie jar and the identity
    provider's authorize counter is asserted unchanged, so "one login"
    is measured rather than assumed from two 200s.
    """
    browser = _sign_in(front, ADMIN_PRINCIPAL)
    after_first_login = front["idp"].authorize_hits
    assert after_first_login == 1
    assert SESSION_COOKIE_NAME in browser.cookies

    status, headers, body = browser.get(front["chat_url"])
    assert status == 200
    payload = json.loads(body)
    assert payload["surface"] == CHAT_SURFACE
    assert payload["headers"][PRINCIPAL_HEADER.lower()] == ADMIN_PRINCIPAL

    status, headers, body = browser.get(front["console_url"])
    assert status == 200
    payload = json.loads(body)
    assert payload["surface"] == CONSOLE_SURFACE
    assert payload["headers"][PRINCIPAL_HEADER.lower()] == ADMIN_PRINCIPAL

    # No second login happened.
    assert front["idp"].authorize_hits == after_first_login
    assert front["idp"].token_hits == 1


def test_logout_invalidates_both_surfaces(front) -> None:
    """Sign-out ends the SESSION, not just the browser's copy of the cookie.

    The distinction is the whole test and it was measured: an implementation
    that only sends ``Max-Age=0`` passes every assertion that goes through
    the same browser, because that browser has already forgotten the cookie.
    The session id is therefore captured BEFORE sign-out and replayed after
    it, which is what an attacker holding a copied cookie would do.
    """
    browser = _sign_in(front, USER_PRINCIPAL)
    session_id = browser.cookies[SESSION_COOKIE_NAME]
    assert browser.get(front["chat_url"])[0] == 200
    assert browser.get(front["console_url"])[0] == 200
    assert len(front["proxy"].store) == 1

    status, headers, _ = browser.get(
        f"http://chat.okg.test:{front['port']}{PROXY_SIGN_OUT_PATH}"
    )
    assert status == 302
    assert SESSION_COOKIE_NAME not in browser.cookies

    for url in (front["chat_url"], front["console_url"]):
        status, headers, _ = browser.get(url)
        assert status == 302, url
        assert headers["X-OKG-SSO-Reason"] == "chat.sso.unauthenticated"

    # The session is gone from the STORE. Replaying the exact cookie the
    # server issued is refused on both surfaces, and the store is empty.
    assert len(front["proxy"].store) == 0
    for url in (front["chat_url"], front["console_url"]):
        replay = Browser(front["port"])
        replay.cookies[SESSION_COOKIE_NAME] = session_id
        status, headers, _ = replay.get(url)
        assert status == 302, url
        assert headers["X-OKG-SSO-Reason"] == "chat.sso.unauthenticated"


def test_non_member_is_denied_on_both_surfaces_after_a_real_login(front) -> None:
    """Authentication succeeded; authorization did not. Typed, on both."""
    browser = _sign_in(front, OUTSIDER_PRINCIPAL)
    for url in (front["chat_url"], front["console_url"]):
        status, headers, body = browser.get(url)
        assert status == 403, url
        assert headers["X-OKG-SSO-Reason"] == GROUP_MEMBERSHIP_ABSENT
        assert b"neither" in body


def test_a_client_supplied_principal_header_never_reaches_an_upstream(
    front,
) -> None:
    """The front is the only thing that may name the principal.

    Asserted on the header set the front CONSTRUCTED, not only on what the
    upstream received. That distinction is load-bearing and was measured:
    with the strip removed, the upstream still sees the right value, because
    ``urllib`` folds ``X-Okg-Principal`` and ``X-OKG-Principal`` into one
    field before the request leaves the process and the front's value is
    added last. The upstream assertion alone is therefore vacuous. Counting
    the constructed keys is not: two spellings of the principal header means
    the client's copy survived, which upstream is refused as
    ``header_ambiguous`` on any proxy that does not fold them.
    """
    browser = _sign_in(front, USER_PRINCIPAL)
    before = len(front["proxy"].forwarded_headers)
    status, _, body = browser.get(
        front["chat_url"],
        extra_headers={PRINCIPAL_HEADER: ADMIN_PRINCIPAL},
    )
    assert status == 200

    constructed = front["proxy"].forwarded_headers[before]
    principal_keys = [
        key for key in constructed
        if key.lower() == PRINCIPAL_HEADER.lower()
    ]
    assert principal_keys == [PRINCIPAL_HEADER], constructed
    assert constructed[PRINCIPAL_HEADER] == USER_PRINCIPAL
    assert ADMIN_PRINCIPAL not in json.dumps(constructed)

    payload = json.loads(body)
    assert payload["headers"][PRINCIPAL_HEADER.lower()] == USER_PRINCIPAL


def test_an_unsolicited_callback_mints_no_session(front) -> None:
    browser = Browser(front["port"])
    status, headers, _ = browser.get(
        f"http://chat.okg.test:{front['port']}{PROXY_CALLBACK_PATH}"
        "?code=forged&state=forged"
    )
    assert status == 400
    assert headers["X-OKG-SSO-Reason"] == "chat.sso.callback_unsolicited"
    assert SESSION_COOKIE_NAME not in browser.cookies


def test_a_login_another_browser_started_cannot_be_completed(front) -> None:
    """``state`` is a lookup key, not a bearer token for somebody's login.

    Without the browser binding, anyone who observes a live ``state`` (a
    referrer header, a shared screen, a proxy log) can present it with a
    code and be handed the session cookie -- login CSRF, and the victim
    ends up signed in as whoever completed it.
    """
    front["idp"].signed_in = ADMIN_PRINCIPAL
    starter = Browser(front["port"])
    status, headers, _ = starter.get(
        f"http://chat.okg.test:{front['port']}{PROXY_SIGN_IN_PATH}?rd="
        + urllib.parse.quote(front["chat_url"])
    )
    assert status == 302
    state = urllib.parse.parse_qs(
        urllib.parse.urlsplit(headers["Location"]).query
    )["state"][0]
    # The starter received the binding cookie; a different browser did not.
    assert "okg_sso_login" in starter.cookies

    thief = Browser(front["port"])
    status, headers, _ = thief.get(
        f"http://chat.okg.test:{front['port']}/authorize-bounce"
    )
    # Complete the IdP hop out-of-band and present the code without the
    # binding cookie.
    _, idp_headers, _ = thief.get(
        f"{front['idp'].base}/authorize?"
        + urllib.parse.urlencode({
            "redirect_uri": f"http://chat.okg.test:{front['port']}"
            f"{PROXY_CALLBACK_PATH}",
            "state": state,
        })
    )
    status, headers, _ = thief.get(idp_headers["Location"])
    assert status == 400
    assert headers["X-OKG-SSO-Reason"] == "chat.sso.callback_unsolicited"
    assert SESSION_COOKIE_NAME not in thief.cookies


@pytest.mark.parametrize("endpoint", [PROXY_SIGN_IN_PATH, PROXY_SIGN_OUT_PATH])
def test_an_off_site_return_target_is_refused(front, endpoint: str) -> None:
    """``rd`` is caller-supplied, so it is an open redirect until checked.

    Refused rather than rewritten to "/": a silent rewrite would swallow a
    link somebody deliberately crafted, and the sign-in case lands the
    person there at the exact moment they are most likely to trust it.
    """
    browser = Browser(front["port"])
    status, headers, _ = browser.get(
        f"http://chat.okg.test:{front['port']}{endpoint}?rd="
        + urllib.parse.quote("https://evil.example/phish")
    )
    assert status == 400
    assert headers["X-OKG-SSO-Reason"] == "chat.sso.return_to_off_site"
    assert "evil.example" not in headers.get("Location", "")


@pytest.mark.parametrize(
    "target",
    ["/graph", "http://console.okg.test:{port}/", "http://chat.okg.test:{port}/x"],
)
def test_an_on_site_return_target_is_kept(front, target: str) -> None:
    """The control: a target the deployment publishes is not refused."""
    browser = Browser(front["port"])
    status, _, _ = browser.get(
        f"http://chat.okg.test:{front['port']}{PROXY_SIGN_OUT_PATH}?rd="
        + urllib.parse.quote(target.format(port=front["port"]))
    )
    assert status == 302


def test_a_relative_path_segment_is_refused(front) -> None:
    """The prefix strip is string arithmetic; `..` must not reach an upstream."""
    browser = _sign_in(front, USER_PRINCIPAL)
    status, headers, _ = browser.get(
        f"http://chat.okg.test:{front['port']}/a/../../secret"
    )
    assert status == 400
    assert headers["X-OKG-SSO-Reason"] == "chat.sso.path_not_absolute"


def test_the_upstream_keeps_its_own_cookies(front) -> None:
    """Open WebUI holds a session of its own; the front must not eat it.

    Only the front's two cookies are withheld -- they are its business and
    an upstream has no use for them.
    """
    browser = _sign_in(front, USER_PRINCIPAL)
    browser.cookies["token"] = "open-webui-session"
    before = len(front["proxy"].forwarded_headers)
    status, _, body = browser.get(front["chat_url"])
    assert status == 200
    forwarded = front["proxy"].forwarded_headers[before]
    assert "token=open-webui-session" in forwarded["Cookie"]
    assert SESSION_COOKIE_NAME not in forwarded["Cookie"]
    assert "token=open-webui-session" in json.loads(body)["headers"]["cookie"]


def test_the_principal_comes_from_the_declared_claim_not_a_present_one(
    front,
) -> None:
    """The front reads ``email`` because the deployment declared it.

    The stub provider sends BOTH ``sub`` and ``email``, with different
    values, so this arm distinguishes them. It was not distinguishable while
    the fake used one string for both -- and the selection was
    presence-sniffed, which is how a provider that stopped releasing
    ``email`` would have silently switched every principal to an opaque id
    that no subject binding names.
    """
    browser = _sign_in(front, ADMIN_PRINCIPAL)
    before = len(front["proxy"].forwarded_headers)
    browser.get(front["chat_url"])
    forwarded = front["proxy"].forwarded_headers[before]
    assert forwarded[PRINCIPAL_HEADER] == ADMIN_PRINCIPAL
    assert "opaque-id-for-" not in forwarded[PRINCIPAL_HEADER]


def test_the_declared_claim_is_read_even_when_another_is_present() -> None:
    """Presence must not choose the identifier namespace.

    The arm that actually distinguishes a DECLARED claim from a guessed one
    needs a token where the two answers differ: a provider that releases
    ``preferred_username`` and ``sub`` but no ``email``. Presence-sniffing
    ("email if present else sub") lands on ``sub`` here -- an opaque id no
    subject binding names -- while every person still authenticates, so the
    graph refuses them all for a reason that reads like a broken graph.
    """
    from okg.substrate.chat.sso import identity_from_claims

    sso = _sso_value()
    claims = {
        "sub": "opaque-42",
        "preferred_username": "ada",
        "groups": [ADMIN_GROUP],
    }
    identity = identity_from_claims(
        claims, sso, subject_claim="preferred_username",
    )
    assert identity.principal == "ada"
    assert identity.groups == (ADMIN_GROUP,)

    # And a token that does not carry the declared claim is REFUSED, never
    # served from a different one.
    with pytest.raises(ChatSsoStartupError) as excinfo:
        identity_from_claims({"sub": "opaque-42"}, sso, subject_claim="email")
    assert excinfo.value.data["reason"] == "no_principal_claim"
    assert excinfo.value.data["subject_claim"] == "email"
    assert "opaque-42" not in excinfo.value.message


def test_a_token_addressed_to_another_client_is_refused() -> None:
    """TLS says WHERE the token came from, not WHO it is for."""
    block = _parse(_chat_block(_sso_block()))
    assert block is not None and block.sso is not None

    class _Canned:
        def __init__(self, metadata: dict, token_payload: dict) -> None:
            self.metadata = metadata
            self.token_payload = token_payload
            self.calls = 0

        def open(self, request, timeout=None):  # noqa: ANN001, D102
            import io
            from contextlib import closing

            self.calls += 1
            payload = self.metadata if self.calls == 1 else self.token_payload
            return closing(io.BytesIO(json.dumps(payload).encode()))

    id_token = ".".join([
        base64.urlsafe_b64encode(b'{"alg":"none"}').decode().rstrip("="),
        base64.urlsafe_b64encode(
            json.dumps({
                "email": ADMIN_PRINCIPAL,
                "aud": "some-other-client",
                "exp": time.time() + 60,
            }).encode()
        ).decode().rstrip("="),
        "sig",
    ])
    opener = _Canned(
        {
            "authorization_endpoint": "https://idp.example.edu/authorize",
            "token_endpoint": "https://idp.example.edu/token",
        },
        {"id_token": id_token},
    )
    client = OidcDiscoveryClient(
        block.sso,
        subject_claim="email",
        environ={SECRET_ENV: "s3cr3t"},
        opener=opener,
    )
    with pytest.raises(ChatSsoStartupError) as excinfo:
        client.exchange(code="c", redirect_uri="https://chat.okg.example/cb")
    assert excinfo.value.data["reason"] == "token_audience_mismatch"


@pytest.mark.parametrize(
    "document, reason",
    [
        ({"issuer": "x"}, "discovery_incomplete"),
        (
            {
                "authorization_endpoint": "https://idp.example.edu/authorize",
                "token_endpoint": "http://idp.example.edu/token",
            },
            "discovery_endpoint_insecure",
        ),
    ],
)
def test_a_discovery_document_that_cannot_be_trusted_refuses(
    document: dict, reason: str,
) -> None:
    """The document names where the client secret gets sent.

    The id token's signature is not verified, and the stated basis for that
    is the ID Token Validation section of OIDC Core: the token comes back
    over TLS from the token endpoint. A cleartext token endpoint removes the basis, so it is
    refused rather than used with the justification quietly no longer
    holding.
    """
    block = _parse(_chat_block(_sso_block()))
    assert block is not None and block.sso is not None

    class _Canned:
        def __init__(self, payload: dict) -> None:
            self.payload = payload

        def open(self, request, timeout=None):  # noqa: ANN001, D102
            import io
            from contextlib import closing

            return closing(io.BytesIO(json.dumps(self.payload).encode()))

    client = OidcDiscoveryClient(
        block.sso,
        subject_claim="email",
        environ={SECRET_ENV: "s3cr3t"},
        opener=_Canned(document),
    )
    with pytest.raises(ChatSsoStartupError) as excinfo:
        client.exchange(code="c", redirect_uri="https://chat.okg.example/cb")
    assert excinfo.value.data["reason"] == reason


def test_the_front_refuses_to_run_over_a_single_operator_declaration() -> None:
    """The front cannot be pointed at a deployment that declared no SSO."""
    chat = _parse(_chat_block({"mode": SSO_MODE_SINGLE_OPERATOR}))
    assert chat is not None and chat.sso is not None
    with pytest.raises(ChatSsoStartupError) as excinfo:
        SsoFrontProxy(
            deployment=DEPLOYMENT,
            sso=chat.sso,
            routes=(),
            upstreams=(),
            idp=object(),
        )
    assert excinfo.value.data["reason"] == "front_without_multi_user"
