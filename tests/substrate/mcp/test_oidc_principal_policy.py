"""OIDC principal policy for the public cms-kb server (PACT mcp-oidc-principal-policy, #1180).

Decisions under test:

- 1a: a signed-in user gets in only through a binding an operator wrote (for
  cms-kb, a ROLE binding on a mapped CERN role). A valid token that matches
  none is refused ``nomos_principal_unidentified`` and audited; nothing ever
  creates a per-user row on the user's behalf.
- 3b: a per-user BLOCK (``okg nomos binding block``) outranks every role
  binding whatever its priority, with its own refusal and audit row.
- 4: ``mcp-serve --oidc-auth`` refuses to start under ``audit`` enforcement
  unless it binds loopback only.
- 6: a provider registered with ``provider add --for-oidc-auth`` requires no
  MFA or fresh authentication.
- Review of #2894: caller-supplied identity claims are refused on a
  single-user server unless the operator opted in; caller-planted ``_nomos*``
  authority keys are dropped; ``propose submit`` crosses the same Nomos
  boundary as ``record``; OIDC refusal rows name the OIDC channel.
"""
from __future__ import annotations

import asyncio
import contextlib
import functools
import json
import time
import types
from types import SimpleNamespace
from typing import Any, Iterator

import pytest

from okg.substrate.errors import SubstrateError

ISSUER = "https://auth.example.org/realms/cern"
AUDIENCE = "cms-kb"
PROVIDER_ID = "cern-sso"
DEPLOYMENT = "oidc-principal-policy"
USER = "alice"
READER_ROLE = "cms-kb-reader"
READER_SUBJECT = "subject:mcp_client"  # what the runbook binds CERN roles to
OIDC_BLOCK = {
    "provider_id": PROVIDER_ID,
    "issuer": ISSUER,
    "audience": AUDIENCE,
    "subject_claim": "cern_upn",
    "roles_claim": "cern_roles",
    "principal_kind": "human_operator",
}


def _config() -> Any:
    from okg.substrate.mcp.oidc_auth import parse_oidc_auth

    return parse_oidc_auth(OIDC_BLOCK)


# --- evaluation (no database) -----------------------------------------------


def _claim(**claims: Any) -> Any:
    from okg.substrate.nomos import IdentityProvider, normalize_identity_claim

    provider = IdentityProvider(
        provider_id=PROVIDER_ID,
        provider_type="oidc",
        issuer=ISSUER,
        audiences=(AUDIENCE,),
        subject_claim="cern_upn",
    )
    return normalize_identity_claim(
        {
            "provider_id": PROVIDER_ID,
            "verification_status": "valid",
            "issuer": ISSUER,
            "audiences": [AUDIENCE],
            "claims": {"cern_upn": USER, **claims},
        },
        [provider],
        now="2026-10-01T12:00:00Z",
    )


def _binding(binding_id: str, claim_path: str, claim_value: str, subject: str,
             priority: int = 0) -> Any:
    from okg.substrate.nomos import SubjectBinding

    return SubjectBinding(
        binding_id=binding_id,
        provider_id=PROVIDER_ID,
        issuer=ISSUER,
        claim_path=claim_path,
        claim_value=claim_value,
        subject=subject,
        scope=f"deployment:{DEPLOYMENT}",
        priority=priority,
    )


def _evaluate(bindings: list[Any], **claims: Any) -> Any:
    from okg.substrate.nomos import evaluate_identity_claim

    return evaluate_identity_claim(
        None,
        _claim(**claims),
        bindings=bindings,
        permission_grants=[],
        action="mcp.tool.call",
        resource={"deployment": DEPLOYMENT},
        scope=f"deployment:{DEPLOYMENT}",
        now="2026-10-01T12:00:00Z",
    )


def test_block_outranks_a_higher_priority_role_binding() -> None:
    from okg.substrate.nomos.identity import BLOCKED_REASON_CODE, BLOCKED_SUBJECT

    role = _binding("binding:role", "roles", READER_ROLE, READER_SUBJECT, priority=1000)
    assert _evaluate([role], roles=[READER_ROLE]).resolved_subject == READER_SUBJECT

    block = _binding(f"binding:{PROVIDER_ID}:block:{USER}", "external_subject", USER,
                     BLOCKED_SUBJECT, priority=0)
    result = _evaluate([role, block], roles=[READER_ROLE])
    assert result.resolved_subject is None
    assert result.decision.effect == "deny"
    assert result.decision.reason_code == BLOCKED_REASON_CODE
    assert result.binding_ids == (block.binding_id,)

    # A block on another user changes nothing for this one.
    other = _binding("binding:block:bob", "external_subject", "bob", BLOCKED_SUBJECT)
    assert _evaluate([role, other], roles=[READER_ROLE]).resolved_subject == READER_SUBJECT


def test_blocked_refusal_is_a_recorded_principal_refusal() -> None:
    from okg.substrate.mcp.principal_mapping import (
        BLOCKED_PRINCIPAL_REASON,
        blocked_principal_error,
        refusal_reason_code,
    )

    error = blocked_principal_error(data={"tool": "query"})
    assert error.code == "nomos_principal_blocked"
    assert refusal_reason_code(error) == BLOCKED_PRINCIPAL_REASON
    assert "okg nomos binding unblock" in (error.fix_hint or "")


def test_identity_channel_names_the_oidc_mode() -> None:
    import okg.substrate.mcp.server as server_mod

    assert server_mod._identity_channel_for(_config()) == "oidc_bearer_token"
    assert server_mod._identity_channel_for(None) == "declared_principal_header"


def test_caller_planted_nomos_keys_are_dropped() -> None:
    import okg.substrate.mcp.server as server_mod

    kept = server_mod._strip_server_owned_authority({
        "_nomos_decision": {"effect": "allow"},
        "_nomos_identity": {"resolved_subject": "subject:root"},
        "_nomosanything": 1,
        "classification": "internal",
    })
    assert kept == {"classification": "internal"}


# --- OIDC start refusal under audit enforcement (no database) --------------


class _Stop(Exception):
    pass


@pytest.mark.parametrize(
    ("mode", "host", "refused"),
    [
        ("audit", "0.0.0.0", True),
        ("audit", "okg.example.org", True),
        ("audit", "127.0.0.1", False),
        ("off", "0.0.0.0", True),
        ("off", "127.0.0.1", False),
        ("enforce", "0.0.0.0", False),
    ],
)
def test_oidc_refuses_audit_enforcement_off_loopback(
    monkeypatch: pytest.MonkeyPatch, mode: str, host: str, refused: bool,
) -> None:
    import okg.substrate.mcp.oidc_auth as oidc_mod
    import okg.substrate.mcp.server as server_mod
    import okg.substrate.nomos.runtime_enforcement as enforcement_mod

    monkeypatch.setattr(server_mod, "resolve_oidc_auth", lambda name: _config())
    monkeypatch.setattr(server_mod, "_require_chat_login_agreement", lambda *a: None)
    monkeypatch.setattr(
        enforcement_mod, "deployment_nomos_enforcement_mode", lambda name: mode,
    )

    def _stop(config: Any) -> Any:
        raise _Stop()

    monkeypatch.setattr(oidc_mod, "build_oidc_token_verifier", _stop)
    if refused:
        with pytest.raises(SubstrateError) as excinfo:
            server_mod.run_server(
                transport="streamable-http", host=host, deployment_name=DEPLOYMENT,
                oidc_auth=True,
            )
        assert excinfo.value.code == "mcp_oidc_auth_audit_exposed"
        assert excinfo.value.data["host"] == host
        assert excinfo.value.data["runtime_enforcement"] == mode
        assert "Users no binding admits are still refused" in str(excinfo.value)
    else:
        with pytest.raises(_Stop):
            server_mod.run_server(
                transport="streamable-http", host=host, deployment_name=DEPLOYMENT,
                oidc_auth=True,
            )


# --- the request gate against PostgreSQL -------------------------------------


@functools.lru_cache(maxsize=1)
def _key() -> Any:
    from cryptography.hazmat.primitives.asymmetric import rsa

    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _verified_token(roles: list[str], *, user: str = USER) -> Any:
    import jwt

    from okg.substrate.mcp.oidc_auth import OidcTokenVerifier

    key = _key()

    class _FakeJwkClient:
        def get_signing_key_from_jwt(self, token: str) -> Any:
            return SimpleNamespace(key=key.public_key())

    now = int(time.time())
    token = jwt.encode(
        {
            "iss": ISSUER,
            "sub": f"f:0001:{user}",
            "aud": [AUDIENCE],
            "azp": AUDIENCE,
            "iat": now,
            "exp": now + 600,
            "cern_upn": user,
            "cern_roles": roles,
        },
        key,
        algorithm="RS256",
        headers={"kid": "test"},
    )
    verifier = OidcTokenVerifier(
        _config(), algorithms=("RS256",), jwk_client=_FakeJwkClient(),
    )
    access = asyncio.run(verifier.verify_token(token))
    assert access is not None, "fixture token must verify"
    return access


@contextlib.contextmanager
def _in_request(access_token: Any) -> Iterator[None]:
    from mcp.server.auth.middleware.bearer_auth import AuthenticatedUser
    from mcp.server.lowlevel.server import request_ctx
    from mcp.shared.context import RequestContext
    from starlette.requests import Request

    scope: dict[str, Any] = {
        "type": "http", "method": "POST", "path": "/mcp", "headers": [],
        "query_string": b"", "user": AuthenticatedUser(access_token),
    }
    token = request_ctx.set(
        RequestContext(
            request_id=1, meta=None, session=None, lifespan_context=None,
            request=Request(scope),
        )
    )
    try:
        yield
    finally:
        request_ctx.reset(token)


def _cli(capsys: Any, *argv: str, ok: bool = True) -> dict[str, Any]:
    from okg.substrate.cli import main as cli

    rc = cli.main(["nomos", *argv, "--json"])
    payload = json.loads(capsys.readouterr().out)
    assert (rc == 0) is ok, payload
    return payload


def _db(dsn: str) -> list[str]:
    return ["--dsn", dsn, "--deployment", DEPLOYMENT]


def _setup_cms_kb(dsn: str, capsys: Any, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """The documented CERN recipe: provider from the OIDC block, one role binding."""
    import okg.substrate.deployments as deployments

    manifest = types.SimpleNamespace(nomos={"oidc_auth": dict(OIDC_BLOCK)})
    monkeypatch.setattr(deployments, "resolve_deployment", lambda name: manifest)
    provider = _cli(capsys, "provider", "add", *_db(dsn), "--for-oidc-auth")
    _cli(
        capsys, "binding", "add", *_db(dsn), "--provider-id", PROVIDER_ID,
        "--claim-path", "roles", "--claim-value", READER_ROLE,
        "--subject", READER_SUBJECT,
    )
    return provider


def _gate(dsn: str, access: Any) -> Any:
    import psycopg

    import okg.substrate.mcp.server as server_mod

    with psycopg.connect(dsn) as conn, _in_request(access):
        return server_mod._resolve_principal_to_subject(
            conn,
            deployment_name=DEPLOYMENT,
            principal_mapping=_config(),
            operator_name="query",
            surface="mcp.tool.call",
            resource_kind="mcp_tool",
            target_kind="mcp_tool_call",
        )


def _refuse_and_audit(dsn: str, access: Any) -> tuple[SubstrateError, dict[str, Any]]:
    """Refuse through the gate and write the refusal row, as the tool gate does."""
    import psycopg
    from psycopg.rows import dict_row

    import okg.substrate.mcp.server as server_mod

    with pytest.raises(SubstrateError) as excinfo:
        _gate(dsn, access)
    with psycopg.connect(dsn) as conn:
        server_mod._persist_principal_refusal(
            conn, error=excinfo.value, deployment_name=DEPLOYMENT,
            surface="mcp.tool.call", operator_name="query",
        )
    with psycopg.connect(dsn) as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT reason_code, subject, effect, context FROM okg.nomos_decisions "
            "WHERE deployment_name = %s AND enforcement_point = 'mcp.principal_mapping' "
            "ORDER BY decision_row_id DESC LIMIT 1",
            (DEPLOYMENT,),
        )
        row = cur.fetchone()
    assert row is not None
    return excinfo.value, dict(row)


def _binding_count(dsn: str) -> int:
    import psycopg

    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.nomos_subject_bindings WHERE deployment_name = %s",
            (DEPLOYMENT,),
        )
        return int(cur.fetchone()[0])


@pytest.mark.postgres
def test_only_a_mapped_role_admits_and_no_user_rows_are_created(
    clean_runtime: str, capsys: Any, monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _setup_cms_kb(clean_runtime, capsys, monkeypatch)
    # Decision 6: the cms-kb provider demands no MFA and no fresh login, so a
    # token with no acr/amr and no auth_time is admitted.
    assert provider["provider"]["mfa_required"] is False
    assert provider["provider"]["required_assurance"] == []
    assert provider["provider"]["freshness_seconds"] is None
    assert provider["provider"]["subject_claim"] == "cern_upn"
    bindings_before = _binding_count(clean_runtime)

    admitted = _gate(clean_runtime, _verified_token([READER_ROLE]))
    assert admitted.resolved_subject == READER_SUBJECT

    error, row = _refuse_and_audit(clean_runtime, _verified_token(["cms-other"]))
    assert error.code == "nomos_principal_unidentified"
    assert error.data["channel"] == "no_subject_binding"
    assert row["reason_code"] == "nomos.identity.unidentified_principal"
    assert row["effect"] == "deny"
    # The row names the channel the identity actually came through.
    assert row["context"]["identity_channel"] == "oidc_bearer_token"
    # Admitting one user and refusing another wrote no binding rows.
    assert _binding_count(clean_runtime) == bindings_before


@pytest.mark.postgres
def test_block_refuses_despite_role_binding_and_unblock_restores(
    clean_runtime: str, capsys: Any, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _setup_cms_kb(clean_runtime, capsys, monkeypatch)
    access = _verified_token([READER_ROLE])
    assert _gate(clean_runtime, access).resolved_subject == READER_SUBJECT

    blocked = _cli(
        capsys, "binding", "block", *_db(clean_runtime),
        "--provider-id", PROVIDER_ID, "--user", USER, "--reason", "abuse report",
    )
    block_id = f"binding:{PROVIDER_ID}:block:{USER}"
    assert blocked["changed"] is True
    assert blocked["binding"]["binding_id"] == block_id
    assert blocked["binding"]["subject"] == "nomos:blocked"

    error, row = _refuse_and_audit(clean_runtime, access)
    assert error.code == "nomos_principal_blocked"
    assert error.data["binding_ids"] == [block_id]
    assert row["reason_code"] == "nomos.identity.principal_blocked"
    assert row["context"]["refusal_kind"] == "blocked_principal"
    assert row["context"]["identity_channel"] == "oidc_bearer_token"
    # Another user with the same role is unaffected.
    other = _gate(clean_runtime, _verified_token([READER_ROLE], user="bob"))
    assert other.resolved_subject == READER_SUBJECT

    # A block is lifted only by unblock: binding disable refuses it, and an
    # admit binding cannot be added to the reserved subject.
    refused = _cli(
        capsys, "binding", "disable", *_db(clean_runtime), "--binding-id", block_id,
        ok=False,
    )
    assert refused["error_code"] == "nomos.binding.block"
    refused = _cli(
        capsys, "binding", "add", *_db(clean_runtime), "--provider-id", PROVIDER_ID,
        "--claim-value", "carol", "--subject", "nomos:blocked", ok=False,
    )
    assert refused["error_code"] == "nomos.binding.block"

    # Disabling the user's own admit binding does not lift the block with it.
    _cli(
        capsys, "binding", "add", *_db(clean_runtime), "--provider-id", PROVIDER_ID,
        "--claim-value", USER, "--subject", READER_SUBJECT,
    )
    disabled = _cli(
        capsys, "binding", "disable", *_db(clean_runtime),
        "--binding-id", f"binding:{PROVIDER_ID}:external_subject:{USER}",
    )
    assert disabled["also_disabled"] == []
    assert _refuse_and_audit(clean_runtime, access)[0].code == "nomos_principal_blocked"

    again = _cli(
        capsys, "binding", "block", *_db(clean_runtime),
        "--provider-id", PROVIDER_ID, "--user", USER,
    )
    assert again["changed"] is False

    lifted = _cli(
        capsys, "binding", "unblock", *_db(clean_runtime),
        "--provider-id", PROVIDER_ID, "--user", USER, "--reason", "resolved",
    )
    assert lifted["changed"] is True
    assert lifted["binding"]["status"] == "disabled"
    assert _gate(clean_runtime, access).resolved_subject == READER_SUBJECT
    events = [event["action"] for event in lifted["binding"]["metadata"]["events"]]
    assert events == ["blocked", "unblocked"]

    # Re-blocking re-applies the same row.
    reblocked = _cli(
        capsys, "binding", "block", *_db(clean_runtime),
        "--provider-id", PROVIDER_ID, "--user", USER,
    )
    assert reblocked["changed"] is True
    assert reblocked["binding"]["status"] == "active"
    assert _refuse_and_audit(clean_runtime, access)[0].code == "nomos_principal_blocked"

    missing = _cli(
        capsys, "binding", "unblock", *_db(clean_runtime),
        "--provider-id", PROVIDER_ID, "--user", "nobody", ok=False,
    )
    assert missing["error_code"] == "nomos.binding.block"


# --- single-user write path ---------------------------------------------------


def _close(mcp: Any) -> None:
    mcp._okg_pool.close()
    if mcp._okg_write_pool:
        mcp._okg_write_pool.close()


_SELF_ASSERTED_CLAIM = {
    "provider_id": PROVIDER_ID,
    "issuer": ISSUER,
    "verification_status": "valid",
    "claims": {"cern_upn": "admin"},
}


@pytest.mark.postgres
def test_single_user_server_refuses_a_caller_identity_claim(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.mcp.server import build_server

    monkeypatch.delenv("OKG_MCP_TRUST_CALLER_IDENTITY_CLAIMS", raising=False)
    mcp = build_server(dsn=clean_runtime, deployment_name=DEPLOYMENT, allow_writes=True)
    try:
        refused = mcp._tool_manager._tools["record"].fn(
            kind="observation",
            payload={"summary": "written as someone else"},
            authority={"nomos_identity_claim": _SELF_ASSERTED_CLAIM},
        )
        with pytest.raises(SubstrateError) as excinfo:
            mcp._tool_manager._tools["query"].fn(
                sql="SELECT 1",
                authority={"nomos_identity_claim": _SELF_ASSERTED_CLAIM},
            )
    finally:
        _close(mcp)
    assert refused["status"] == "refused"
    assert refused["error"]["authority"]["evaluated"] is False
    assert refused["error"]["authority"]["nomos"]["code"] == "nomos_identity_claim_untrusted"
    assert excinfo.value.code == "nomos_identity_claim_untrusted"


@pytest.mark.postgres
def test_record_never_echoes_caller_planted_nomos_keys(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import okg.substrate.runtime.admission_workflows as admission_workflows
    from okg.substrate.mcp.server import build_server

    # The admission workflow is process-global: once any earlier test in the
    # same worker registered it, `record` runs it in-process, bound to the
    # deployment that test registered (in CI, `default`, which has no
    # manifest here). Install this test's own workflow so the result never
    # depends on test order, and so the test can see what would be STORED.
    stored: list[dict[str, Any]] = []

    def fake_submit_workflow(
        payload: dict, producer_id: str, producer_kind: str = "agent",
        intent: str = "review_proposal", idempotency_key: str | None = None,
        authority: dict | None = None, check_summary: dict | None = None,
    ) -> dict:
        stored.append({"producer_id": producer_id, "authority": dict(authority or {})})
        return {"deployment": DEPLOYMENT, "status": "pending", "admission_id": "fake"}

    monkeypatch.setitem(
        admission_workflows._DECORATED,
        admission_workflows.ADMISSION_SUBMIT_WORKFLOW_NAME,
        fake_submit_workflow,
    )
    planted = {"effect": "allow", "reason_code": "planted-by-caller"}
    mcp = build_server(dsn=clean_runtime, deployment_name=DEPLOYMENT, allow_writes=True)
    try:
        result = mcp._tool_manager._tools["record"].fn(
            kind="observation",
            payload={"summary": "planted decision"},
            authority={"_nomos_decision": planted, "_nomos_identity": {"x": 1}},
        )
    finally:
        _close(mcp)
    assert len(stored) == 1, result
    assert not [key for key in stored[0]["authority"] if key.startswith("_nomos")]
    assert "planted-by-caller" not in json.dumps(result, default=str)


@pytest.mark.postgres
def test_propose_submit_crosses_the_nomos_boundary(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.mcp.server import build_server

    # Nomos enforced, and no MCP subject configured: record refuses before
    # any write; propose submit must refuse the same way.
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")
    monkeypatch.setenv("OKG_MCP_ADMISSION_TIMEOUT_SECONDS", "0.5")
    mcp = build_server(dsn=clean_runtime, deployment_name=DEPLOYMENT, allow_writes=True)
    try:
        recorded = mcp._tool_manager._tools["record"].fn(
            kind="observation", payload={"summary": "x"},
        )
        proposed = mcp._tool_manager._tools["propose"].fn(
            action="submit",
            payload={"nodes": [], "edges": []},
            authority={"producer_id": "agent:someone-else"},
        )
    finally:
        _close(mcp)
    assert recorded["status"] == "refused"
    assert proposed["status"] == "refused", proposed
    # The same boundary refuses both, with the same kind of evidence.
    assert proposed["error"]["code"] == recorded["error"]["code"]
    assert proposed["error"]["code"] != "admission_runtime_unavailable"
    assert set(proposed["error"]["authority"]) == set(recorded["error"]["authority"])
    decision = proposed["error"]["authority"].get("nomos_decision")
    if decision is not None:
        assert decision["effect"] != "allow"


# --- review round 2 (#2909): reserved block ids, $OKG_DSN, exact-match warning.

_NOWHERE = "postgresql://nobody@127.0.0.1:1/none"


def test_binding_add_cannot_take_a_block_id(capsys: Any) -> None:
    payload = _cli(
        capsys, "binding", "add", "--dsn", _NOWHERE, "--deployment", DEPLOYMENT,
        "--provider-id", PROVIDER_ID, "--claim-value", USER,
        "--subject", READER_SUBJECT, "--binding-id", f"binding:{PROVIDER_ID}:block:{USER}",
        ok=False,
    )
    assert payload["error_code"] == "nomos.binding.block"
    assert "database error" not in payload["error"]


def test_binding_verbs_fall_back_to_okg_dsn(
    capsys: Any, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("OKG_DSN", raising=False)
    for verb in (["list"], ["unblock", "--provider-id", PROVIDER_ID, "--user", USER]):
        payload = _cli(capsys, "binding", *verb, "--deployment", DEPLOYMENT, ok=False)
        assert "pass --dsn or set $OKG_DSN" in payload["error"]
    monkeypatch.setenv("OKG_DSN", _NOWHERE)
    payload = _cli(capsys, "binding", "list", "--deployment", DEPLOYMENT, ok=False)
    assert payload["error_code"] == "nomos.binding.database_error"


@pytest.mark.postgres
def test_block_warns_when_the_exact_subject_value_was_never_seen(
    clean_runtime: str, capsys: Any, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _setup_cms_kb(clean_runtime, capsys, monkeypatch)
    access = _verified_token([READER_ROLE])
    # Alice signs in once, so the deployment has seen her exact value.
    assert _gate(clean_runtime, access).resolved_subject == READER_SUBJECT

    from okg.substrate.cli import main as cli

    rc = cli.main([
        "nomos", "binding", "block", *_db(clean_runtime), "--provider-id",
        PROVIDER_ID, "--user", "Alice", "--json",
    ])
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert rc == 0, payload
    assert "exactly" in payload["match_rule"]
    assert payload["subject_value_check"]["exact_match_seen"] is False
    assert payload["subject_value_check"]["case_variants_seen"] == [USER]
    assert payload["warning"].startswith("WARNING: ")
    assert "may match nobody" in captured.err
    # The warning is true: the mis-cased block does not stop alice.
    assert _gate(clean_runtime, access).resolved_subject == READER_SUBJECT

    exact = _cli(
        capsys, "binding", "block", *_db(clean_runtime),
        "--provider-id", PROVIDER_ID, "--user", USER,
    )
    assert exact["subject_value_check"]["exact_match_seen"] is True
    assert exact["warning"] is None
    assert _refuse_and_audit(clean_runtime, access)[0].code == "nomos_principal_blocked"


# --- final review (#2909): block ids with ':' in the provider id, and the
# "ever seen" check can never undo a block.


@pytest.mark.parametrize("verb", ["add", "disable"])
@pytest.mark.parametrize(
    "binding_id",
    [f"binding:{PROVIDER_ID}:block:{USER}", "binding:okta:prod:block:alice"],
)
def test_add_and_disable_refuse_any_block_id(
    capsys: Any, verb: str, binding_id: str,
) -> None:
    argv = ["--binding-id", binding_id]
    if verb == "add":
        argv += ["--provider-id", "okta:prod", "--claim-value", "alice",
                 "--subject", READER_SUBJECT]
    payload = _cli(
        capsys, "binding", verb, "--dsn", _NOWHERE, "--deployment", DEPLOYMENT,
        *argv, ok=False,
    )
    assert payload["error_code"] == "nomos.binding.block"
    assert "database error" not in payload["error"]


@pytest.mark.postgres
def test_a_failing_seen_check_leaves_the_block_committed(
    clean_runtime: str, capsys: Any, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import psycopg

    import okg.substrate.nomos.identity_store as store

    _setup_cms_kb(clean_runtime, capsys, monkeypatch)
    access = _verified_token([READER_ROLE])

    def cancelled(*args: Any, **kwargs: Any) -> Any:
        raise psycopg.errors.QueryCanceled("canceling statement due to statement timeout")

    monkeypatch.setattr(store, "subject_value_evidence", cancelled)
    from okg.substrate.cli import main as cli

    rc = cli.main([
        "nomos", "binding", "block", *_db(clean_runtime), "--provider-id",
        PROVIDER_ID, "--user", USER, "--json",
    ])
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert rc == 0, payload
    assert payload["changed"] is True
    assert "QueryCanceled" in payload["subject_value_check"]["error"]
    assert "the block is saved" in payload["warning"]
    assert "the block is saved" in captured.err
    # Committed: a new connection sees it, and the gate refuses alice.
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT status FROM okg.nomos_subject_bindings "
            "WHERE deployment_name = %s AND binding_id = %s",
            (DEPLOYMENT, f"binding:{PROVIDER_ID}:block:{USER}"),
        )
        assert cur.fetchone()[0] == "active"
    assert _refuse_and_audit(clean_runtime, access)[0].code == "nomos_principal_blocked"


@pytest.mark.postgres
def test_the_seen_check_runs_read_only_after_the_block(
    clean_runtime: str, capsys: Any, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The real check runs in its own read-only transaction."""
    import psycopg

    import okg.substrate.nomos.identity_store as store

    _setup_cms_kb(clean_runtime, capsys, monkeypatch)
    real = store.subject_value_evidence
    seen: dict[str, Any] = {}

    def spy(conn: Any, **kwargs: Any) -> Any:
        # The block is already committed when the check starts.
        with psycopg.connect(clean_runtime) as other, other.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.nomos_subject_bindings "
                "WHERE deployment_name = %s AND subject = 'nomos:blocked'",
                (DEPLOYMENT,),
            )
            seen["blocks_visible"] = cur.fetchone()[0]
        result = real(conn, **kwargs)
        with conn.cursor() as cur:
            cur.execute("SHOW transaction_read_only")
            seen["read_only"] = cur.fetchone()[0]
        return result

    monkeypatch.setattr(store, "subject_value_evidence", spy)
    payload = _cli(
        capsys, "binding", "block", *_db(clean_runtime),
        "--provider-id", PROVIDER_ID, "--user", USER,
    )
    assert payload["changed"] is True
    assert seen == {"blocks_visible": 1, "read_only": "on"}
