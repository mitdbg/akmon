"""OIDC mode on a real ``okg mcp-serve --oidc-auth`` process (#1180, PR2).

PACT ``mcp-oidc-real-process-proof``. The hermetic suite
(``test_oidc_auth.py``) proves the verifier with fake key clients in one
process. This module proves the same mode end to end, with nothing faked on the
server side:

* A real HTTPS OpenID Connect issuer runs on loopback in this process
  (``_oidc_test_issuer.py``): discovery document, JWKS, RS256 signing keys it
  can rotate while running.
* ``okg mcp-serve --oidc-auth`` runs as a SUBPROCESS, from the command line an
  operator would type. It discovers the issuer and downloads its keys over
  TLS. It trusts the issuer only through ordinary operator settings --
  ``SSL_CERT_FILE``/``REQUESTS_CA_BUNDLE`` naming the test CA and
  ``OKG_EGRESS_ALLOW_HOSTS`` admitting the loopback host -- and there is no
  test hook in production code.
* The client is the plain MCP Python SDK with an ``Authorization: Bearer``
  header (or raw ``httpx`` where the point is the bytes before any session).
  No okg client code is used.

The deployment is the committed ``mcp-two-principal-fixture`` with its header
``principal_mapping`` replaced by a ``nomos.oidc_auth`` block naming the test
issuer (written to a temporary ``OKG_DEPLOYMENTS_DIR``, because the issuer's
port is only known at run time). Users reach Nomos subjects through ROLE
bindings on the token's ``realm_access.roles`` claim: alice's role reaches the
subject that may read both sources, bob's reaches the one denied the private
source, so their answers must differ.

**What this does not prove.** The key-set refresh a token with an unknown key
id triggers is capped at one per 60 seconds and the interval has no setting,
so a key published inside that window is proven REFUSED here and its later
acceptance is not exercised (it would need a 60-second sleep per run). The
server connects as the test superuser through ``OKG_MCP_RO_DSN``, as the header
proof does, so this is not evidence about the least-privilege ``okg_mcp`` role.
Blocking an individual user is not covered here.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import subprocess
import sys
from datetime import timedelta
from pathlib import Path
from typing import Any, Iterator

import psycopg
import pytest
import yaml
from psycopg.rows import dict_row

pytest.importorskip("mcp")
pytest.importorskip("httpx")
pytest.importorskip("jwt")

import httpx  # noqa: E402

from okg.substrate.mcp.oidc_auth import (  # noqa: E402
    OIDC_TOKEN_VERIFICATION_METHOD,
    OIDC_VERIFIER_ID,
    ROLES_CLAIM_KEY,
)
from okg.substrate.mcp.principal_mapping import UNIDENTIFIED_SUBJECT  # noqa: E402
from okg.substrate.nomos import (  # noqa: E402
    IdentityProvider,
    SubjectBinding,
    upsert_identity_provider,
    upsert_subject_binding,
)
from okg.substrate.nomos.identity_store import disable_subject_binding  # noqa: E402
from tests.substrate.mcp._oidc_test_issuer import (  # noqa: E402
    TestIssuer,
    person_claims,
    write_tls_material,
)
from tests.substrate.mcp._result_payload import (  # noqa: E402
    json_client_sessions,
    payload as _result_payload,
)
from tests.substrate.mcp._two_principal_fixture import (  # noqa: E402
    DEPLOYMENT,
    PRIVATE_NODE,
    PUBLIC_NODE,
    SCOPE,
    SUBJECT_A,
    SUBJECT_B,
)
from tests.substrate.mcp.test_multi_user_http_e2e import (  # noqa: E402
    _FILTER_ARGUMENTS,
    _REPO_ROOT,
    _decision_watermark,
    _decisions_since,
    _free_port,
    _node_ids,
    _reason_codes,
    _refusal,
    _request_subjects,
    _stop_server,
    _wait_until_listening,
    two_principal_dsn as two_principal_dsn,
)

#: `postgres` only, like the header proof: this module owns its database (via
#: the imported fixture) and its server process, so it shares no state.
pytestmark = pytest.mark.postgres


@pytest.fixture(autouse=True)
def _json_result_format(monkeypatch: pytest.MonkeyPatch) -> None:
    """These tests assert on the dictionary, so every client call asks for
    the JSON result format (issue #3059: the default is one text block)."""
    json_client_sessions(monkeypatch)

PROVIDER_ID = "okg-test-oidc"
AUDIENCE = "okg-chat"
ROLES_PATH = "realm_access.roles"
PRINCIPAL_KIND = "human_operator"

ALICE = "alice@example.edu"
BOB = "bob@example.edu"
CAROL = "carol@example.edu"  # bound, then her binding is disabled
DAVE = "dave@example.edu"  # a valid token nobody bound

ROLE_A = "okg-research"
ROLE_B = "okg-guest"
ROLE_CAROL = "okg-contractor"
ROLE_UNBOUND = "okg-visitor"

CAROL_BINDING = "binding:oidc:contractor"

_UNIDENTIFIED_CODE = "nomos_principal_unidentified"
_PROCESS_WIDE_SUBJECT = "subject:process-wide-operator"
_ACCEPT = "application/json, text/event-stream"
_TOOLS_LIST = {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}


# ---------------------------------------------------------------------------
# The issuer, the identity rows, the deployment directory, the server
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def issuer(tmp_path_factory: pytest.TempPathFactory) -> Iterator[TestIssuer]:
    tls_dir = tmp_path_factory.mktemp("oidc-tls")
    _ca, cert, key = write_tls_material(tls_dir)
    running = TestIssuer(cert, key)
    running.add_key("key-1")
    running.start()
    running.ca_pem = _ca
    try:
        yield running
    finally:
        running.stop()


def _oidc_provider(issuer_url: str) -> IdentityProvider:
    """The provider the ``nomos.oidc_auth`` block names.

    ``trusted_verifiers`` makes Nomos CHECK the stamp the server writes;
    ``role_claims`` must carry ``roles`` for role bindings to see anything.
    """
    return IdentityProvider(
        provider_id=PROVIDER_ID,
        provider_type="oidc",
        issuer=issuer_url,
        audiences=(AUDIENCE,),
        subject_claim="email",
        group_claims=(),
        role_claims=(ROLES_CLAIM_KEY,),
        entitlement_claims=(),
        assurance_claim=None,
        freshness_seconds=3600,
        metadata={
            "trusted_verifiers": [OIDC_VERIFIER_ID],
            "max_verifier_age_seconds": 300,
        },
    )


@pytest.fixture(scope="module")
def oidc_dsn(two_principal_dsn: str, issuer: TestIssuer) -> str:
    """The two-principal database plus the OIDC provider and ROLE bindings.

    The permission grants and policy bundle come from the header fixture and
    are keyed on SUBJECTS, so they apply unchanged; only the path from a
    person to a subject is new.
    """
    provider = _oidc_provider(issuer.issuer)
    with psycopg.connect(two_principal_dsn) as conn:
        upsert_identity_provider(conn, deployment_name=DEPLOYMENT, provider=provider)
        for binding_id, role, subject in (
            ("binding:oidc:research", ROLE_A, SUBJECT_A),
            ("binding:oidc:guest", ROLE_B, SUBJECT_B),
            (CAROL_BINDING, ROLE_CAROL, SUBJECT_B),
        ):
            upsert_subject_binding(
                conn,
                deployment_name=DEPLOYMENT,
                binding=SubjectBinding(
                    binding_id=binding_id,
                    provider_id=PROVIDER_ID,
                    issuer=issuer.issuer,
                    claim_path=ROLES_CLAIM_KEY,
                    claim_value=role,
                    subject=subject,
                    scope=SCOPE,
                    priority=10,
                ),
            )
        conn.commit()
    return two_principal_dsn


def _write_deployment(root: Path, issuer_url: str, resource_url: str) -> None:
    """Copy the committed fixture and swap its header mapping for oidc_auth."""
    source = _REPO_ROOT / "deployments" / DEPLOYMENT
    target = root / DEPLOYMENT
    if target.exists():
        shutil.rmtree(target)
    shutil.copytree(source, target)
    manifest_path = target / "deployment.yaml"
    manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    nomos = manifest["nomos"]
    nomos.pop("principal_mapping")
    nomos["oidc_auth"] = {
        "provider_id": PROVIDER_ID,
        "issuer": issuer_url,
        "audience": AUDIENCE,
        "subject_claim": "email",
        "roles_claim": ROLES_PATH,
        "principal_kind": PRINCIPAL_KIND,
        "resource_url": resource_url,
    }
    manifest_path.write_text(yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8")


def _server_env(dsn: str, deployments_root: Path, ca_pem: Path) -> dict[str, str]:
    env = dict(os.environ)
    env["OKG_DEPLOYMENTS_DIR"] = str(deployments_root)
    # Ordinary operator trust settings for a private issuer -- no test hook.
    env["SSL_CERT_FILE"] = str(ca_pem)
    env["REQUESTS_CA_BUNDLE"] = str(ca_pem)
    env["OKG_EGRESS_ALLOW_HOSTS"] = "127.0.0.1"
    env["OKG_MCP_RO_DSN"] = dsn
    env["OKG_MCP_TRACE_ENABLED"] = "0"
    env.pop("OKG_MCP_AUTH_TOKEN", None)
    # The manifest must be what turns enforcement on.
    env.pop("OKG_NOMOS_RUNTIME_ENFORCEMENT", None)
    # Set on purpose: a request falling back to it would show in the audit.
    env["OKG_NOMOS_SUBJECT"] = _PROCESS_WIDE_SUBJECT
    return env


class _Server:
    def __init__(self, url: str, log_path: Path, port: int) -> None:
        self.url = url
        self.log_path = log_path
        self.port = port
        self.resource_url = url


@pytest.fixture(scope="module")
def oidc_server(
    oidc_dsn: str, issuer: TestIssuer, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[_Server]:
    """One running ``okg mcp-serve --oidc-auth``."""
    work = tmp_path_factory.mktemp("mcp-oidc")
    deployments_root = work / "deployments"
    deployments_root.mkdir()
    last_log: Path | None = None
    for attempt in range(2):
        # A bind-and-release port can be taken before the server binds it;
        # retry once on a fresh port rather than blame the code under test.
        port = _free_port()
        url = f"http://127.0.0.1:{port}/mcp"
        _write_deployment(deployments_root, issuer.issuer, url)
        log_path = work / f"server-{attempt}.log"
        last_log = log_path
        argv = [
            sys.executable,
            "-m",
            "okg.substrate.cli.main",
            "mcp-serve",
            "--deployment",
            DEPLOYMENT,
            "--dsn",
            oidc_dsn,
            "--transport",
            "streamable-http",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--oidc-auth",
        ]
        with log_path.open("wb") as handle:
            proc = subprocess.Popen(  # noqa: S603
                argv,
                cwd=str(_REPO_ROOT),
                env=_server_env(oidc_dsn, deployments_root, issuer.ca_pem),
                stdout=handle,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                start_new_session=True,
            )
        try:
            if _wait_until_listening(proc, port, log_path):
                yield _Server(url, log_path, port)
                return
        finally:
            _stop_server(proc)
    raise AssertionError(
        "okg mcp-serve --oidc-auth exited before listening; last server log:\n"
        + (last_log.read_text(encoding="utf-8", errors="replace")[-4000:] if last_log else "")
    )


# ---------------------------------------------------------------------------
# Tokens and calls
# ---------------------------------------------------------------------------


def _person(issuer: TestIssuer, email: str, roles: list[str], **overrides: Any) -> str:
    return issuer.mint(
        person_claims(issuer.issuer, AUDIENCE, email=email, roles=roles, **overrides),
        kid="key-1",
    )


def _bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


async def _call(url: str, token: str, arguments: dict[str, Any] | None = None) -> Any:
    from mcp import ClientSession
    from mcp.client.streamable_http import streamablehttp_client

    async with streamablehttp_client(url, headers=_bearer(token)) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            return await session.call_tool("filter", dict(arguments or _FILTER_ARGUMENTS))


def _filter(url: str, token: str) -> dict[str, Any]:
    return _result_payload(asyncio.run(_call(url, token)))


def _raw_post(url: str, headers: dict[str, str]) -> httpx.Response:
    return httpx.post(
        url,
        json=_TOOLS_LIST,
        headers={"Accept": _ACCEPT, "Content-Type": "application/json", **headers},
        timeout=30.0,
    )


# ==========================================================================
# 1. Two users, one server, different answers
# ==========================================================================


def test_two_oidc_users_get_different_rows_from_one_server(
    oidc_server: _Server, oidc_dsn: str, issuer: TestIssuer, record_property
) -> None:
    watermark = _decision_watermark(oidc_dsn)

    alice = _filter(oidc_server.url, _person(issuer, ALICE, [ROLE_A]))
    bob = _filter(oidc_server.url, _person(issuer, BOB, [ROLE_B]))

    # The fixture must differentiate, or equal answers would prove nothing.
    assert _node_ids(alice) == {PUBLIC_NODE, PRIVATE_NODE}
    assert _node_ids(bob) == {PUBLIC_NODE}
    assert bob.get("status") != "refused", bob

    rows = _decisions_since(oidc_dsn, watermark)
    assert _request_subjects(rows) == {SUBJECT_A, SUBJECT_B}
    assert _PROCESS_WIDE_SUBJECT not in {str(row["subject"]) for row in rows}
    identity_rows = [row for row in rows if row["context"].get("identity_verified_by")]
    assert identity_rows, "no decision row recorded the verified identity"
    by_subject: dict[str, set[str]] = {}
    for row in identity_rows:
        context = row["context"]
        # The server's own stamp, not anything the caller said.
        assert context["identity_verified_by"] == OIDC_VERIFIER_ID
        assert context["identity_verification_method"] == OIDC_TOKEN_VERIFICATION_METHOD
        assert context["identity_verification_status"] == "valid"
        assert context["identity_provider_id"] == PROVIDER_ID
        assert context["identity_issuer"] == issuer.issuer
        by_subject.setdefault(str(row["subject"]), set()).add(
            str(context["identity_external_subject"])
        )
    assert by_subject == {SUBJECT_A: {ALICE}, SUBJECT_B: {BOB}}

    with psycopg.connect(oidc_dsn) as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT external_subject, verification_status,
                   evidence ->> 'verified_by' AS verified_by
              FROM okg.nomos_external_claims
             WHERE deployment_name = %s AND provider_id = %s
               AND external_subject = ANY(%s)
            """,
            (DEPLOYMENT, PROVIDER_ID, [ALICE, BOB]),
        )
        claims = [dict(row) for row in cur.fetchall()]
    assert {row["external_subject"] for row in claims} == {ALICE, BOB}
    assert {row["verification_status"] for row in claims} == {"valid"}
    assert {row["verified_by"] for row in claims} == {OIDC_VERIFIER_ID}
    record_property(
        "oidc_differential",
        json.dumps(
            {
                "alice": sorted(_node_ids(alice)),
                "bob": sorted(_node_ids(bob)),
                "subjects": sorted(by_subject),
                "verified_by": OIDC_VERIFIER_ID,
            },
            sort_keys=True,
        ),
    )


# ==========================================================================
# 2. Tokens the verifier must refuse, all with the same 401
# ==========================================================================


def test_every_bad_token_gets_the_same_401_and_reaches_no_tool(
    oidc_server: _Server, oidc_dsn: str, issuer: TestIssuer, record_property
) -> None:
    """Missing, malformed, expired, forged, wrong issuer, wrong audience.

    Raw requests: the point is the response BEFORE any session exists. A 401
    that differed between arms would tell a caller which check they failed.
    """
    stranger_key = issuer.add_key("key-unpublished", publish=False)
    now_claims = dict(email=ALICE, roles=[ROLE_A])
    arms: dict[str, dict[str, str]] = {
        "missing": {},
        "malformed": _bearer("not.a.jwt"),
        "not-bearer": {"Authorization": "Basic YWxpY2U6cGFzc3dvcmQ="},
        # Past the 60-second clock-skew allowance.
        "expired": _bearer(
            _person(issuer, ALICE, [ROLE_A], lifetime_seconds=60, issued_offset_seconds=-600)
        ),
        "wrong-issuer": _bearer(
            issuer.mint(
                person_claims("https://127.0.0.1:1/realms/other", AUDIENCE, **now_claims),
                kid="key-1",
            )
        ),
        "wrong-audience": _bearer(
            issuer.mint(
                person_claims(issuer.issuer, "some-other-client", **now_claims),
                kid="key-1",
            )
        ),
        # Signed by a key the issuer never published, under a KNOWN key id:
        # the signature check, not the key lookup, must refuse it.
        "forged-signature": _bearer(
            issuer.mint(
                person_claims(issuer.issuer, AUDIENCE, **now_claims),
                kid="key-1",
                key=stranger_key,
            )
        ),
    }
    watermark = _decision_watermark(oidc_dsn)
    responses = {label: _raw_post(oidc_server.url, headers) for label, headers in arms.items()}

    assert {label: r.status_code for label, r in responses.items()} == {
        label: 401 for label in arms
    }
    reference = responses["missing"]
    for label, response in responses.items():
        assert response.content == reference.content, label
        assert response.headers.get("www-authenticate") == reference.headers.get(
            "www-authenticate"
        ), label
        for token in issuer.minted:
            assert token not in response.text, label
        assert PRIVATE_NODE not in response.text
    # Refused below the tool surface: no principal gate ran, so no decision.
    assert _decisions_since(oidc_dsn, watermark) == []
    record_property(
        "oidc_bad_token_arms",
        json.dumps({label: r.status_code for label, r in responses.items()}, sort_keys=True),
    )


# ==========================================================================
# 3. Valid tokens that must still reach nothing
# ==========================================================================


def test_an_application_token_lists_tools_but_calls_nothing(
    oidc_server: _Server, oidc_dsn: str, issuer: TestIssuer
) -> None:
    """A service account's token authenticates the caller, never a person.

    Two shapes: a Keycloak service-account token that ALSO carries an email
    (so it would satisfy ``subject_claim``), and an RFC 9068
    client-credentials token whose subject is its client.
    """
    keycloak_service = issuer.mint(
        person_claims(
            issuer.issuer,
            AUDIENCE,
            email=ALICE,
            roles=[ROLE_A],
            sub="service-account-okg-chat",
            clientId=AUDIENCE,
            clientHost="10.0.0.9",
        ),
        kid="key-1",
    )
    client_credentials = issuer.mint(
        {
            key: value
            for key, value in person_claims(
                issuer.issuer, AUDIENCE, email="x@example.edu", roles=[], sub=AUDIENCE
            ).items()
            if key not in {"email", "realm_access", "auth_time"}
        },
        kid="key-1",
    )

    async def list_then_call(token: str) -> tuple[list[str], Any]:
        from mcp import ClientSession
        from mcp.client.streamable_http import streamablehttp_client

        async with streamablehttp_client(oidc_server.url, headers=_bearer(token)) as (
            read,
            write,
            _,
        ):
            async with ClientSession(read, write) as session:
                await session.initialize()
                tools = [tool.name for tool in (await session.list_tools()).tools]
                return tools, await session.call_tool("filter", dict(_FILTER_ARGUMENTS))

    watermark = _decision_watermark(oidc_dsn)
    for label, token in (
        ("keycloak-service-account", keycloak_service),
        ("client-credentials", client_credentials),
    ):
        arm_watermark = _decision_watermark(oidc_dsn)
        tools, result = asyncio.run(list_then_call(token))
        assert "filter" in tools, label
        error = _refusal(_result_payload(result))
        assert error["code"] == _UNIDENTIFIED_CODE, label
        assert error["data"]["channel"] == "token_names_no_person", label
        subjects = {
            str(row["subject"]) for row in _decisions_since(oidc_dsn, arm_watermark)
        }
        assert not subjects & {SUBJECT_A, SUBJECT_B, _PROCESS_WIDE_SUBJECT}, label
    # Refusal rows are content-addressed: the second, identical refusal adds
    # no row (persist_principal_refusal_decision), so assert row CONTENT.
    refusals = [
        row
        for row in _decisions_since(oidc_dsn, watermark)
        if row["subject"] == UNIDENTIFIED_SUBJECT
    ]
    assert refusals, "the application-token refusal was not audited"
    assert {row["context"].get("refusal_channel") for row in refusals} == {
        "token_names_no_person"
    }


def test_a_valid_token_for_an_unbound_user_is_unidentified(
    oidc_server: _Server, oidc_dsn: str, issuer: TestIssuer
) -> None:
    watermark = _decision_watermark(oidc_dsn)

    error = _refusal(_filter(oidc_server.url, _person(issuer, DAVE, [ROLE_UNBOUND])))

    assert error["code"] == _UNIDENTIFIED_CODE
    assert error["data"]["channel"] == "no_subject_binding"
    rows = _decisions_since(oidc_dsn, watermark)
    subjects = {str(row["subject"]) for row in rows}
    # The refusal is recorded under the sentinel subject, and the identity
    # evaluation names the external person -- never another user's subject.
    assert UNIDENTIFIED_SUBJECT in subjects
    assert f"external:{PROVIDER_ID}:{DAVE}" in subjects
    assert "nomos.identity.no_subject_binding" in _reason_codes(rows)
    assert not subjects & {SUBJECT_A, SUBJECT_B, _PROCESS_WIDE_SUBJECT}


def test_disabling_a_binding_refuses_the_next_request(
    oidc_server: _Server, oidc_dsn: str, issuer: TestIssuer
) -> None:
    """The running server re-reads bindings: no restart, no cached subject."""
    token = _person(issuer, CAROL, [ROLE_CAROL])
    before = _filter(oidc_server.url, token)
    assert _node_ids(before) == {PUBLIC_NODE}

    with psycopg.connect(oidc_dsn) as conn:
        disabled = disable_subject_binding(
            conn,
            deployment_name=DEPLOYMENT,
            binding_id=CAROL_BINDING,
            actor="test:operator",
            reason="PACT mcp-oidc-real-process-proof disabled-binding arm",
        )
        conn.commit()
    assert disabled is not None and disabled["status"] == "disabled"

    watermark = _decision_watermark(oidc_dsn)
    error = _refusal(_filter(oidc_server.url, token))
    assert error["code"] == _UNIDENTIFIED_CODE
    assert error["data"]["channel"] == "no_subject_binding"
    rows = _decisions_since(oidc_dsn, watermark)
    assert f"external:{PROVIDER_ID}:{CAROL}" in {str(row["subject"]) for row in rows}
    assert SUBJECT_B not in _request_subjects(rows)


# ==========================================================================
# 4. Concurrent sessions and session ownership
# ==========================================================================


def test_concurrent_oidc_sessions_do_not_mix_principals(
    oidc_server: _Server, issuer: TestIssuer
) -> None:
    from mcp import ClientSession
    from mcp.client.streamable_http import streamablehttp_client

    alice_token = _person(issuer, ALICE, [ROLE_A])
    bob_token = _person(issuer, BOB, [ROLE_B])

    async def scenario() -> list[int]:
        url = oidc_server.url
        timeout = timedelta(seconds=30)
        async with streamablehttp_client(url, headers=_bearer(alice_token)) as (ra, wa, id_a):
            async with ClientSession(ra, wa, read_timeout_seconds=timeout) as a:
                await a.initialize()
                async with streamablehttp_client(url, headers=_bearer(bob_token)) as (
                    rb,
                    wb,
                    id_b,
                ):
                    async with ClientSession(rb, wb, read_timeout_seconds=timeout) as b:
                        await b.initialize()
                        assert id_a() and id_b() and id_a() != id_b()
                        for _ in range(4):
                            result_a, result_b = await asyncio.gather(
                                a.call_tool("filter", dict(_FILTER_ARGUMENTS)),
                                b.call_tool("filter", dict(_FILTER_ARGUMENTS)),
                            )
                            assert _node_ids(_result_payload(result_a)) == {
                                PUBLIC_NODE,
                                PRIVATE_NODE,
                            }
                            assert _node_ids(_result_payload(result_b)) == {PUBLIC_NODE}
                        # Bob's valid token on Alice's session is not Alice.
                        statuses = []
                        async with httpx.AsyncClient(timeout=15) as client:
                            headers = {
                                **_bearer(bob_token),
                                "Mcp-Session-Id": id_a(),
                                "Mcp-Protocol-Version": "2025-11-25",
                                "Accept": _ACCEPT,
                            }
                            response = await client.post(
                                url,
                                headers=headers,
                                json={
                                    "jsonrpc": "2.0",
                                    "id": 99,
                                    "method": "tools/call",
                                    "params": {
                                        "name": "filter",
                                        "arguments": _FILTER_ARGUMENTS,
                                    },
                                },
                            )
                            statuses.append(response.status_code)
                            assert PRIVATE_NODE not in response.text
                            response = await client.request("DELETE", url, headers=headers)
                            statuses.append(response.status_code)
                        # Alice's session survived the attempt.
                        again = await a.call_tool("filter", dict(_FILTER_ARGUMENTS))
                        assert _node_ids(_result_payload(again)) == {
                            PUBLIC_NODE,
                            PRIVATE_NODE,
                        }
                        return statuses

    assert asyncio.run(scenario()) == [404, 404]


# ==========================================================================
# 5. Discovery metadata and key rotation
# ==========================================================================


def test_protected_resource_metadata_names_the_issuer(
    oidc_server: _Server, issuer: TestIssuer
) -> None:
    """An MCP client can find where to log in from the server alone."""
    base = f"http://127.0.0.1:{oidc_server.port}"
    response = httpx.get(f"{base}/.well-known/oauth-protected-resource/mcp", timeout=30.0)
    assert response.status_code == 200, response.text
    body = response.json()
    assert str(body["resource"]).rstrip("/") == oidc_server.resource_url
    assert [str(url).rstrip("/") for url in body["authorization_servers"]] == [
        issuer.issuer
    ]
    challenge = _raw_post(oidc_server.url, {}).headers.get("www-authenticate", "")
    assert "resource_metadata=" in challenge
    assert "/.well-known/oauth-protected-resource/mcp" in challenge


def test_key_rotation_refetches_once_and_caps_unknown_key_ids(
    oidc_server: _Server, issuer: TestIssuer, record_property
) -> None:
    """The ONLY test in this module that sends unknown key ids.

    The server may refetch the key set for an unknown key id at most once per
    60 seconds across all requests. So: a key the issuer rotated in is accepted
    on the first miss (one download); inside the interval, neither a key id
    nobody published nor a second freshly rotated key causes another download,
    and both are refused. The old key keeps working throughout.
    """
    def claims() -> dict[str, Any]:
        return person_claims(issuer.issuer, AUDIENCE, email=ALICE, roles=[ROLE_A])

    # Warm the server's key cache with the original key first, so the count
    # below measures only the rotation -- whether or not an earlier test ran
    # or the 300-second cache lifetime has passed.
    warm = _filter(oidc_server.url, issuer.mint(claims(), kid="key-1"))
    assert _node_ids(warm) == {PUBLIC_NODE, PRIVATE_NODE}

    issuer.add_key("key-2")
    fetches_before = issuer.jwks_fetches
    rotated = _filter(oidc_server.url, issuer.mint(claims(), kid="key-2"))
    assert _node_ids(rotated) == {PUBLIC_NODE, PRIVATE_NODE}
    assert issuer.jwks_fetches == fetches_before + 1

    unknown = issuer.mint(claims(), kid="key-nobody-published", key=issuer.add_key(
        "key-nobody-published", publish=False
    ))
    assert _raw_post(oidc_server.url, _bearer(unknown)).status_code == 401

    issuer.add_key("key-3")
    too_soon = issuer.mint(claims(), kid="key-3")
    assert _raw_post(oidc_server.url, _bearer(too_soon)).status_code == 401
    assert issuer.jwks_fetches == fetches_before + 1, "an unknown key id forced a download"

    still = _filter(oidc_server.url, issuer.mint(claims(), kid="key-1"))
    assert _node_ids(still) == {PUBLIC_NODE, PRIVATE_NODE}
    record_property(
        "oidc_key_rotation",
        json.dumps(
            {
                "rotated_key_accepted": True,
                "jwks_downloads_for_three_unknown_kids": issuer.jwks_fetches
                - fetches_before,
                "unpublished_kid_status": 401,
                "second_rotation_inside_interval_status": 401,
            },
            sort_keys=True,
        ),
    )


# ==========================================================================
# 6. Last: nothing the server logged carries a token
# ==========================================================================


def test_the_server_log_carries_no_token_material(
    oidc_server: _Server, issuer: TestIssuer, record_property
) -> None:
    """Every token minted in this module, and each one's signature, is absent.

    Runs last so the log covers every arm above. The trust exception the
    operator granted IS in the log (a WARNING), which is how an operator sees
    that a private issuer was admitted.
    """
    # One more authenticated call so the log is never empty of requests.
    _filter(oidc_server.url, _person(issuer, BOB, [ROLE_B]))
    text = oidc_server.log_path.read_text(encoding="utf-8", errors="replace")
    assert issuer.minted, "no tokens were minted"
    leaked = [
        index
        for index, token in enumerate(issuer.minted)
        if token in text or token.rsplit(".", 1)[-1] in text
    ]
    assert leaked == [], f"{len(leaked)} token(s) appear in the server log"
    assert "egress allowlist exception" in text
    record_property(
        "oidc_log_scan",
        json.dumps(
            {"tokens_scanned": len(issuer.minted), "log_bytes": len(text), "leaked": 0},
            sort_keys=True,
        ),
    )
