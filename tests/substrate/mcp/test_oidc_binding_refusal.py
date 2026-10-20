"""OIDC mode refuses a user whose subject binding is disabled or expired.

PACT nomos-binding-commands (issue #1180, "disabled or revoked users under
OIDC mode"). The token in every case is VALID: signed by the issuer's key,
right issuer and audience, inside its validity window. What changes is only
the binding an operator manages with ``okg nomos binding``. The request gate
(``_resolve_principal_to_subject``, the one both the tool and resource gates
call) must accept the user while the binding is in effect and refuse them --
with an audit row naming the provider-qualified principal -- once it is
disabled or its ``effective_until`` has passed.

The server never asks the identity provider whether a token was revoked, so
these tests do not cover a user disabled only at the provider; that user keeps
access until the token expires, a documented limit.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import time
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, Iterator

import jwt
import psycopg
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from psycopg.rows import dict_row

import okg.substrate.mcp.server as server_mod
import okg.substrate.nomos.identity as identity_mod
from okg.substrate.cli import main as cli
from okg.substrate.errors import SubstrateError
from okg.substrate.mcp.oidc_auth import (
    OIDC_VERIFIER_ID,
    ROLES_CLAIM_KEY,
    OidcAuthConfig,
    OidcTokenVerifier,
)
from okg.substrate.nomos import (
    IdentityProvider,
    upsert_identity_provider,
)

pytestmark = pytest.mark.postgres

ISSUER = "https://auth.example.org/realms/team"
AUDIENCE = "okg-chat"
PROVIDER_ID = "team-sso"
DEPLOYMENT = "oidc-binding-refusal"
USER = "alice@example.org"
NOMOS_SUBJECT = "subject:team:alice"
BINDING_ID = f"binding:{PROVIDER_ID}:external_subject:{USER}"
EXTERNAL_PRINCIPAL = f"external:{PROVIDER_ID}:{USER}"

CONFIG = OidcAuthConfig(
    provider_id=PROVIDER_ID,
    issuer=ISSUER,
    audience=AUDIENCE,
    subject_claim="email",
    roles_claim=f"resource_access.{AUDIENCE}.roles",
    principal_kind="human_operator",
)

_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)


class _FakeJwkClient:
    def get_signing_key_from_jwt(self, token: str) -> Any:
        return SimpleNamespace(key=_KEY.public_key())


def _verified_token() -> Any:
    now = int(time.time())
    token = jwt.encode(
        {
            "iss": ISSUER,
            "sub": "f:0001:alice",
            "aud": [AUDIENCE],
            "azp": AUDIENCE,
            "iat": now,
            "exp": now + 600,
            "auth_time": now - 30,
            "email": USER,
            "resource_access": {AUDIENCE: {"roles": ["analyst"]}},
        },
        _KEY,
        algorithm="RS256",
        headers={"kid": "test"},
    )
    verifier = OidcTokenVerifier(
        CONFIG, algorithms=("RS256",), jwk_client=_FakeJwkClient(),
    )
    access = asyncio.run(verifier.verify_token(token))
    assert access is not None, "fixture token must verify"
    return access


@contextlib.contextmanager
def _in_request(access_token: Any) -> Iterator[None]:
    """Install one authenticated HTTP request as the in-flight MCP request."""
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
        "user": AuthenticatedUser(access_token),
    }
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


def _register(dsn: str) -> None:
    provider = IdentityProvider(
        provider_id=PROVIDER_ID,
        provider_type="oidc",
        issuer=ISSUER,
        audiences=(AUDIENCE,),
        subject_claim="email",
        role_claims=(ROLES_CLAIM_KEY,),
        metadata={
            "trusted_verifiers": [OIDC_VERIFIER_ID],
            "max_verifier_age_seconds": 3600,
        },
    )
    with psycopg.connect(dsn) as conn:
        upsert_identity_provider(conn, deployment_name=DEPLOYMENT, provider=provider)
        conn.commit()


def _binding(dsn: str, capsys: Any, *extra: str) -> dict[str, Any]:
    rc = cli.main([
        "nomos", "binding", *extra,
        "--dsn", dsn, "--deployment", DEPLOYMENT, "--json",
    ])
    payload = json.loads(capsys.readouterr().out)
    assert rc == 0, payload
    return payload


def _add_binding(dsn: str, capsys: Any, *extra: str) -> dict[str, Any]:
    return _binding(
        dsn, capsys, "add",
        "--provider-id", PROVIDER_ID,
        "--claim-value", USER,
        "--subject", NOMOS_SUBJECT,
        *extra,
    )


def _gate(dsn: str, access: Any) -> Any:
    with psycopg.connect(dsn) as conn, _in_request(access):
        return server_mod._resolve_principal_to_subject(
            conn,
            deployment_name=DEPLOYMENT,
            principal_mapping=CONFIG,
            operator_name="query",
            surface="mcp.tool.call",
            resource_kind="mcp_tool",
            target_kind="mcp_tool_call",
        )


def _gate_decisions(dsn: str) -> list[dict[str, Any]]:
    with psycopg.connect(dsn) as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT d.decision_id, d.subject, d.effect, d.reason_code,
                   d.enforcement_point, d.target, c.external_subject,
                   c.verification_status
              FROM okg.nomos_decisions d
              LEFT JOIN okg.nomos_external_claims c
                ON c.deployment_name = d.deployment_name
               AND c.nomos_decision_id = d.decision_id
             WHERE d.deployment_name = %s
               AND d.enforcement_point = 'mcp.principal_gate'
             ORDER BY d.decision_row_id
            """,
            (DEPLOYMENT,),
        )
        return list(cur.fetchall())


def _assert_refused(dsn: str, access: Any) -> dict[str, Any]:
    before = len(_gate_decisions(dsn))
    with pytest.raises(SubstrateError) as excinfo:
        _gate(dsn, access)
    data = excinfo.value.data
    assert data["channel"] == "no_subject_binding"
    assert data["identity_reason_code"] == "nomos.identity.no_subject_binding"
    assert "okg nomos binding add" in (excinfo.value.fix_hint or "")
    rows = _gate_decisions(dsn)
    refusals = [
        row for row in rows
        if row["reason_code"] == "nomos.identity.no_subject_binding"
    ]
    assert refusals, rows
    refusal = refusals[-1]
    assert refusal["effect"] == "deny"
    assert refusal["subject"] == EXTERNAL_PRINCIPAL
    assert refusal["target"]["kind"] == "mcp_tool_call"
    # The claim row the refusal links to is the VALID OIDC claim: the token
    # was fine, the binding was not.
    assert refusal["external_subject"] == USER
    assert refusal["verification_status"] == "valid"
    # Decision ids are content-derived; the same refusal twice is one row.
    assert len(rows) >= before
    return refusal


def test_active_binding_accepts_then_disable_refuses(
    clean_runtime: str, capsys: Any,
) -> None:
    _register(clean_runtime)
    access = _verified_token()

    # No binding yet: the valid token names nobody the deployment knows.
    _assert_refused(clean_runtime, access)

    added = _add_binding(clean_runtime, capsys)
    assert added["binding"]["binding_id"] == BINDING_ID
    assert added["binding"]["in_effect"] is True

    # The gate admits a request whose principal resolves to a subject; what
    # that subject may then do is the permission grants' job, downstream.
    accepted = _gate(clean_runtime, access)
    assert accepted.resolved_subject == NOMOS_SUBJECT
    assert accepted.binding_ids == (BINDING_ID,)

    disabled = _binding(
        clean_runtime, capsys, "disable",
        "--binding-id", BINDING_ID, "--reason", "left the team",
    )
    assert disabled["changed"] is True
    assert disabled["binding"]["status"] == "disabled"

    refusal = _assert_refused(clean_runtime, access)
    assert refusal["subject"] == EXTERNAL_PRINCIPAL

    # The row stays, as history. Neither a plain re-add nor --replace can
    # undo the disable, and the user stays refused after both.
    listed = _binding(clean_runtime, capsys, "list")
    assert [row["status"] for row in listed["bindings"]] == ["disabled"]
    for extra, code in (
        ((), "nomos.binding.exists"),
        (("--replace",), "nomos.binding.disabled"),
    ):
        rc = cli.main([
            "nomos", "binding", "add", "--provider-id", PROVIDER_ID,
            "--claim-value", USER, "--subject", NOMOS_SUBJECT, *extra,
            "--dsn", clean_runtime, "--deployment", DEPLOYMENT, "--json",
        ])
        refused = json.loads(capsys.readouterr().out)
        assert rc == 1, refused
        assert refused["error_code"] == code
        _assert_refused(clean_runtime, access)

    # Access comes back only through a new binding id, on the record.
    readded = _add_binding(clean_runtime, capsys, "--binding-id", "binding:alice:2")
    assert readded["action"] == "added"
    assert _gate(clean_runtime, access).binding_ids == ("binding:alice:2",)


def test_binding_refuses_once_effective_until_passes(
    clean_runtime: str, capsys: Any, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _register(clean_runtime)
    access = _verified_token()
    until = datetime.now(tz=UTC) + timedelta(seconds=90)
    _add_binding(clean_runtime, capsys, "--effective-until", until.isoformat())

    assert _gate(clean_runtime, access).resolved_subject == NOMOS_SUBJECT

    # Move the evaluator's clock past effective_until but still inside the
    # token's lifetime (exp = now + 600) and the provider's verifier-age
    # bound, so the ONLY thing that changed is the binding's window.
    later = (until + timedelta(seconds=30)).isoformat()
    monkeypatch.setattr(identity_mod, "_utc_now_iso", lambda: later)

    refusal = _assert_refused(clean_runtime, access)
    assert refusal["subject"] == EXTERNAL_PRINCIPAL


def test_already_expired_binding_never_admits(
    clean_runtime: str, capsys: Any,
) -> None:
    _register(clean_runtime)
    access = _verified_token()
    past = (datetime.now(tz=UTC) - timedelta(minutes=5)).isoformat()
    added = _add_binding(clean_runtime, capsys, "--effective-until", past)
    assert added["binding"]["in_effect"] is False

    _assert_refused(clean_runtime, access)


def test_future_effective_from_is_refused_until_it_starts(
    clean_runtime: str, capsys: Any,
) -> None:
    _register(clean_runtime)
    access = _verified_token()
    start = (datetime.now(tz=UTC) + timedelta(hours=1)).isoformat()
    added = _add_binding(clean_runtime, capsys, "--effective-from", start)
    assert added["binding"]["status"] == "active"
    assert added["binding"]["in_effect"] is False

    refusal = _assert_refused(clean_runtime, access)
    assert refusal["subject"] == EXTERNAL_PRINCIPAL


def test_disable_warns_that_a_role_binding_still_admits_the_user(
    clean_runtime: str, capsys: Any,
) -> None:
    # Finding 3(a): the token carries role `analyst`. Disabling the user's
    # own binding stops that row matching, but the role binding still admits
    # them -- the command must say so rather than imply access is cut.
    _register(clean_runtime)
    access = _verified_token()
    _add_binding(clean_runtime, capsys)
    role = _binding(
        clean_runtime, capsys, "add", "--provider-id", PROVIDER_ID,
        "--claim-path", "roles", "--claim-value", "analyst",
        "--subject", "subject:team:analyst",
    )
    role_id = role["binding"]["binding_id"]
    # Both bindings have priority 0 and different subjects, so before the
    # disable the gate refuses as ambiguous.
    before = _gate_refusal_reason(clean_runtime, access)
    assert before == "nomos.identity.ambiguous_subject_binding"

    disabled = _binding(
        clean_runtime, capsys, "disable", "--binding-id", BINDING_ID,
        "--reason", "left the team",
    )
    assert disabled["changed"] is True
    assert [row["binding_id"] for row in disabled["still_matching"]] == [role_id]
    [still] = disabled["still_matching"]
    assert still["match"] == "if_token_carries"
    assert still["subject"] == "subject:team:analyst"
    assert disabled["warning"].startswith("WARNING: ")
    assert role_id in disabled["warning"]
    assert "may still admit the same user" in disabled["warning"]

    # And the warning is true: the user is still admitted, via the role.
    accepted = _gate(clean_runtime, access)
    assert accepted.resolved_subject == "subject:team:analyst"
    assert accepted.binding_ids == (role_id,)


def test_disable_cuts_access_when_a_second_binding_names_the_same_user(
    clean_runtime: str, capsys: Any,
) -> None:
    # Finding 3: two bindings on the same user. Disabling the one the gate
    # used left the other admitting them. The disable must cut access.
    _register(clean_runtime)
    access = _verified_token()
    _add_binding(
        clean_runtime, capsys, "--binding-id", "binding:alice:a",
        "--priority", "5",
    )
    _add_binding(clean_runtime, capsys, "--binding-id", "binding:alice:b")
    assert _gate(clean_runtime, access).binding_ids == ("binding:alice:a",)

    disabled = _binding(
        clean_runtime, capsys, "disable", "--binding-id", "binding:alice:a",
        "--reason", "left the team",
    )
    assert disabled["also_disabled"] == ["binding:alice:b"]
    assert disabled["still_matching"] == []

    refusal = _assert_refused(clean_runtime, access)
    assert refusal["subject"] == EXTERNAL_PRINCIPAL


def test_disabling_one_of_two_ambiguous_bindings_does_not_admit(
    clean_runtime: str, capsys: Any,
) -> None:
    # Finding 3: two equal-priority bindings on the same user map to
    # different subjects, so the gate refuses as ambiguous. Disabling one of
    # them alone would END that refusal and admit the user under the other.
    _register(clean_runtime)
    access = _verified_token()
    _add_binding(clean_runtime, capsys, "--binding-id", "binding:alice:a")
    _binding(
        clean_runtime, capsys, "add", "--provider-id", PROVIDER_ID,
        "--claim-value", USER, "--subject", "subject:team:other",
        "--binding-id", "binding:alice:b",
    )
    assert (
        _gate_refusal_reason(clean_runtime, access)
        == "nomos.identity.ambiguous_subject_binding"
    )

    disabled = _binding(
        clean_runtime, capsys, "disable", "--binding-id", "binding:alice:a",
    )
    assert disabled["also_disabled"] == ["binding:alice:b"]
    assert disabled["still_matching"] == []
    assert disabled["warning"] is None

    _assert_refused(clean_runtime, access)


def test_disable_with_nothing_else_matching_has_no_warning(
    clean_runtime: str, capsys: Any,
) -> None:
    _register(clean_runtime)
    _add_binding(clean_runtime, capsys)
    # A different user's binding cannot admit alice, so it is not listed.
    _binding(
        clean_runtime, capsys, "add", "--provider-id", PROVIDER_ID,
        "--claim-value", "bob@example.org", "--subject", "subject:team:bob",
    )
    disabled = _binding(
        clean_runtime, capsys, "disable", "--binding-id", BINDING_ID,
    )
    assert disabled["still_matching"] == []
    assert disabled["warning"] is None


def test_real_request_gate_refuses_the_request_after_a_disable(
    clean_runtime: str, capsys: Any,
) -> None:
    # Finding 6: go through the wrapper the MCP server composes in front of
    # every tool (`_with_multi_user_principal_gate`) with a pool built by the
    # server's own `_build_pool`. One pooled connection serves both requests,
    # so a stale per-connection read would admit the second one.
    _register(clean_runtime)
    access = _verified_token()
    _add_binding(clean_runtime, capsys)
    pool = server_mod._build_pool(
        conninfo=clean_runtime,
        role="okg_test",
        min_size=1,
        max_size=1,
        open=True,
        name="binding-refusal-gate",
    )
    ran: list[str] = []

    def _identity_tracer(tool_name: str) -> Any:
        return lambda fn: fn

    gate = server_mod._with_multi_user_principal_gate(
        _identity_tracer,
        pool=pool,
        deployment_name=DEPLOYMENT,
        principal_mapping=CONFIG,
    )

    @gate("query")
    def _tool(request: str) -> str:
        ran.append(request)
        return "served"

    try:
        with _in_request(access):
            assert _tool("first") == "served"
        _binding(
            clean_runtime, capsys, "disable", "--binding-id", BINDING_ID,
            "--reason", "left the team",
        )
        with _in_request(access), pytest.raises(SubstrateError) as excinfo:
            _tool("second")
    finally:
        pool.close()
    assert ran == ["first"]
    assert excinfo.value.data["channel"] == "no_subject_binding"
    refusals = [
        row for row in _gate_decisions(clean_runtime)
        if row["reason_code"] == "nomos.identity.no_subject_binding"
    ]
    assert refusals and refusals[-1]["subject"] == EXTERNAL_PRINCIPAL


def _gate_refusal_reason(dsn: str, access: Any) -> str:
    with pytest.raises(SubstrateError) as excinfo:
        _gate(dsn, access)
    return str(excinfo.value.data["identity_reason_code"])
