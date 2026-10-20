"""The documented read-only OIDC recipe against the REAL rollout baseline.

``test_oidc_principal_policy.py`` proves a mapped CERN role RESOLVES to a
Nomos subject. Resolving is not enough: after a permission grant matches,
``evaluate_identity_claim`` hands the subject to the active policy bundle,
and the rollout baseline (``okg deployment nomos-rollout-audit
--apply-baseline``) allows ``mcp.tool.call`` and ``graph.read.mcp`` only for
``subject:mcp_client`` (``deployment_baseline_policy_graph`` in
``nomos/store.py``). kb76 showed it live: the recipe as first written bound
CERN roles to ``subject:cms-kb:reader``, and under ``enforce`` every MCP tool
call from a signed-in user was refused ``nomos_identity_denied ...
nomos.default_deny``.

The deployment's ``nomos`` block and the ``provider add`` / ``binding add``
/ ``grant add`` arguments are the runbook's "CERN recipe for a read-only
server", held as literals in ``_oidc_reader_recipe.py``. This file
runs in a PostgreSQL lane whose scope closure excludes the docs tree, so it
does not read the runbook; the database-free
``test_oidc_reader_recipe_docs.py`` asserts the runbook still shows exactly
those literals, so a recipe that stops working fails one of the two. Around
it:

* a real deployment directory with ``runtime_enforcement: enforce`` and a
  deployment posture, found through ``OKG_DEPLOYMENTS_DIR``;
* the real rollout baseline, applied by the function the
  ``--apply-baseline`` CLI flag calls, and one published generation;
* ``okg nomos provider add --for-oidc-auth``, ``binding add`` and ``grant
  add`` through the CLI;
* the tool-call gate ``mcp-serve`` installs on every request
  (``_okg_dispatch_principal_gate``), fed a verified OIDC token, plus the
  ``graph.read.mcp`` boundary decision the read tools ask for.
"""
from __future__ import annotations

import asyncio
import contextlib
import functools
import json
import shutil
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Iterator

import pytest
import yaml

from okg.substrate.errors import SubstrateError
from tests.substrate.mcp._oidc_reader_recipe import recipe as _documented_recipe

pytest.importorskip("mcp")
pytest.importorskip("jwt")

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE = REPO_ROOT / "deployments" / "mcp-two-principal-fixture"

DEPLOYMENT = "oidc-reader-baseline"
SCOPE = f"deployment:{DEPLOYMENT}"
USER = "alice"
OTHER_USER = "bob"
UNMAPPED_ROLE = "cms-other"
SOURCE_ID = "cms-kb-docs"
PUBLIC_SOURCE_POLICY = {
    "sensitivity": "public",
    "data_classification": "public",
    "credential_ref_policy": "none",
    "live_call_allowed": False,
    "exportability": "public",
    "retention": "source_defined",
    "provenance": "local_filesystem",
    "privacy_obligations": ["audit"],
}


# --- the recipe, as the runbook shows it ---------------------------------------


@functools.lru_cache(maxsize=1)
def _recipe() -> dict[str, Any]:
    """The runbook's nomos block and its provider/binding/grant arguments."""
    return _documented_recipe()


def _oidc_block() -> dict[str, Any]:
    return dict(_recipe()["nomos"]["oidc_auth"])


def _config() -> Any:
    from okg.substrate.mcp.oidc_auth import parse_oidc_auth

    return parse_oidc_auth(_oidc_block())


# --- deployment, baseline, publish -------------------------------------------


def _write_deployment(root: Path) -> Path:
    """A cms-kb-shaped deployment carrying the runbook's nomos block."""
    target = root / DEPLOYMENT
    shutil.copytree(FIXTURE, target)
    manifest_path = target / "deployment.yaml"
    manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    manifest["name"] = DEPLOYMENT
    nomos = manifest["nomos"]
    nomos.pop("principal_mapping", None)
    recipe_nomos = _recipe()["nomos"]
    nomos["runtime_enforcement"] = recipe_nomos["runtime_enforcement"]
    nomos["oidc_auth"] = dict(recipe_nomos["oidc_auth"])
    manifest_path.write_text(yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8")
    (target / "source_registry.yaml").write_text(
        yaml.safe_dump({
            "sources": {
                SOURCE_ID: {
                    "ownership_id": SOURCE_ID,
                    "source_class": "reference_catalog",
                    "record_identity_kind": "domain_key",
                    "source_revision_kind": "content_hash",
                    "deletion_semantics": "missing_from_completed_scope",
                    "publication_mode": "published_generation",
                    "source_policy": dict(PUBLIC_SOURCE_POLICY),
                },
            },
        }, sort_keys=False),
        encoding="utf-8",
    )
    return manifest_path


def _apply_rollout_baseline(dsn: str, manifest_path: Path) -> dict[str, Any]:
    """What ``okg deployment nomos-rollout-audit --apply-baseline`` runs."""
    from okg.substrate.cli.deployment import _apply_nomos_rollout_baseline

    return _apply_nomos_rollout_baseline(
        deployment=DEPLOYMENT,
        dsn=dsn,
        inventory={"entries": [{
            "name": DEPLOYMENT, "kind": "deployment", "path": str(manifest_path),
        }]},
    )


def _publish_empty_generation(dsn: str) -> str:
    """mcp-serve needs one published generation to open a read session."""
    import psycopg

    from okg.substrate.publisher import Publisher
    from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer
    from tests.substrate._db_helpers import seed_catalog_version

    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog_version(conn)
    result = Publisher(
        dsn, DEPLOYMENT,
        catalog_identity=bind_test_catalog_writer(dsn, DEPLOYMENT),
    ).cycle()
    assert result["status"] == "published", result
    return str(result["generation_id"])


def _cli(capsys: Any, *argv: str, ok: bool = True) -> dict[str, Any]:
    from okg.substrate.cli import main as cli

    rc = cli.main(["nomos", *argv, "--json"])
    payload = json.loads(capsys.readouterr().out)
    assert (rc == 0) is ok, payload
    return payload


def _db(dsn: str) -> list[str]:
    return ["--dsn", dsn, "--deployment", DEPLOYMENT]


def _setup(
    dsn: str, tmp_path: Path, capsys: Any, monkeypatch: pytest.MonkeyPatch,
    *, subject: str | None = None,
) -> None:
    """Deployment, real baseline, publish, then the runbook's three commands.

    ``subject`` replaces the runbook's subject in the binding and the grant;
    None runs the runbook's commands unchanged.
    """
    deployments_root = tmp_path / "deployments"
    deployments_root.mkdir()
    manifest_path = _write_deployment(deployments_root)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(deployments_root))
    for name in ("OKG_NOMOS_RUNTIME_ENFORCEMENT", "OKG_NOMOS_SUBJECT", "OKG_MCP_NOMOS_SUBJECT"):
        monkeypatch.delenv(name, raising=False)

    from okg.substrate.nomos.runtime_enforcement import (
        deployment_nomos_enforcement_mode,
    )

    assert deployment_nomos_enforcement_mode(DEPLOYMENT) == "enforce"
    applied = _apply_rollout_baseline(dsn, manifest_path)
    assert applied.get("ok") is True, applied
    _publish_empty_generation(dsn)

    recipe = _recipe()
    binding, grant = list(recipe["binding"]), list(recipe["grant"])
    if subject is not None:
        binding[binding.index("--subject") + 1] = subject
        grant[grant.index("--subject") + 1] = subject
    _cli(capsys, "provider", "add", *_db(dsn), *recipe["provider"])
    _cli(capsys, "binding", "add", *_db(dsn), *binding)
    _cli(capsys, "grant", "add", *_db(dsn), *grant)


# --- a verified token inside a request ---------------------------------------


@functools.lru_cache(maxsize=1)
def _key() -> Any:
    from cryptography.hazmat.primitives.asymmetric import rsa

    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _verified_token(
    roles: list[str], *, user: str = USER, with_subject: bool = True,
) -> Any:
    import jwt

    from okg.substrate.mcp.oidc_auth import OidcTokenVerifier

    config = _config()
    key = _key()

    class _FakeJwkClient:
        def get_signing_key_from_jwt(self, token: str) -> Any:
            return SimpleNamespace(key=key.public_key())

    now = int(time.time())
    claims: dict[str, Any] = {
        "iss": config.issuer, "sub": f"f:0001:{user}", "aud": [config.audience],
        "azp": config.audience, "iat": now, "exp": now + 600,
    }
    if with_subject:
        claims[config.subject_claim] = user
    claims[config.roles_claim] = roles
    token = jwt.encode(claims, key, algorithm="RS256", headers={"kid": "test"})
    verifier = OidcTokenVerifier(
        config, algorithms=("RS256",), jwk_client=_FakeJwkClient(),
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


@contextlib.contextmanager
def _oidc_server(dsn: str) -> Iterator[Any]:
    """The multi-user server ``mcp-serve --oidc-auth`` builds, minus HTTP."""
    from okg.substrate.mcp.server import build_server

    mcp = build_server(
        dsn=dsn,
        deployment_name=DEPLOYMENT,
        multi_user_principals=True,
        principal_mapping=_config(),
    )
    try:
        yield mcp
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


def _graph_read_decision(dsn: str, access: Any) -> Any:
    """The ``graph.read.mcp`` boundary decision, as the read tools ask it."""
    import psycopg

    import okg.substrate.mcp.server as server_mod
    from okg.substrate.nomos import evaluate_identity_claim_from_postgres

    with psycopg.connect(dsn) as conn, _in_request(access):
        claim = server_mod._request_identity_claim(_config())
        result = evaluate_identity_claim_from_postgres(
            conn,
            deployment_name=DEPLOYMENT,
            bundle=None,
            claim=claim,
            action="graph.read.mcp",
            resource={
                "deployment": DEPLOYMENT,
                "scope": SCOPE,
                "operator_name": "search",
                "resource_kind": "mcp_graph_read_boundary",
            },
            scope=SCOPE,
            enforcement_point="mcp.identity_boundary",
        )
        conn.commit()
        return result


def _tool_call_gate(mcp: Any, access: Any, tool: str = "search") -> SubstrateError | None:
    """Run the per-request tool gate; return its refusal, or None if admitted."""
    with _in_request(access):
        try:
            mcp._okg_dispatch_principal_gate(tool)
        except SubstrateError as exc:
            return exc
    return None


def _describe(error: SubstrateError | None) -> str:
    if error is None:
        return "admitted"
    return f"{error.code}: {error} data={getattr(error, 'data', None)}"


def _recorded_sign_ins(dsn: str) -> list[tuple[str, list[str]]]:
    import psycopg

    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT external_subject, roles FROM okg.v_nomos_external_claims "
            "WHERE deployment_name = %s ORDER BY observed_at, claim_row_id",
            (DEPLOYMENT,),
        )
        return [(row[0], list(row[1])) for row in cur.fetchall()]


# --- tests ---------------------------------------------------------------------


@pytest.mark.postgres
@pytest.mark.usefixtures("okg_mcp_can_log_in")
def test_documented_cms_kb_recipe_admits_a_mapped_role_under_rollout_baseline(
    clean_runtime: str, tmp_path: Path, capsys: Any, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The runbook recipe, unchanged: a mapped role is served; others are not.

    Fails on 66f2a47c97, whose recipe binds to ``subject:cms-kb:reader``:
    ``nomos_identity_denied`` / ``nomos.default_deny`` on ``mcp.tool.call``.
    """
    _setup(clean_runtime, tmp_path, capsys, monkeypatch)
    recipe = _recipe()

    reader = _verified_token([recipe["role"]])
    with _oidc_server(clean_runtime) as mcp:
        refusal = _tool_call_gate(mcp, reader)
        unmapped = _tool_call_gate(mcp, _verified_token([UNMAPPED_ROLE], user=OTHER_USER))
        nameless = _tool_call_gate(
            mcp, _verified_token([recipe["role"]], with_subject=False),
        )
    read = _graph_read_decision(clean_runtime, reader)

    # A role no binding maps is refused before any policy is consulted.
    assert unmapped is not None and unmapped.code == "nomos_principal_unidentified", (
        _describe(unmapped)
    )
    # So is a token without the recipe's subject claim, whatever its roles.
    assert nameless is not None and nameless.code == "nomos_principal_unidentified", (
        _describe(nameless)
    )
    assert nameless.data["channel"] == "token_names_no_person"
    assert read.resolved_subject == recipe["subject"]
    assert read.permission_grant_ids == (recipe["grant_id"],)
    # Resolving and matching a grant is not enough: the ACTIVE baseline must
    # allow the subject too.
    assert refusal is None, f"mcp.tool.call refused: {_describe(refusal)}"
    assert read.decision.effect == "allow", (
        f"graph.read.mcp: {read.decision.effect} {read.decision.reason_code}"
    )
    # Every sign-in is recorded with the user's subject-claim value: this is
    # where an operator finds the exact value `binding block --user` needs.
    sign_ins = _recorded_sign_ins(clean_runtime)
    assert (USER, [recipe["role"]]) in sign_ins
    assert (OTHER_USER, [UNMAPPED_ROLE]) in sign_ins


@pytest.mark.postgres
@pytest.mark.usefixtures("okg_mcp_can_log_in")
def test_documented_recipe_block_refuses_one_user_and_spares_the_rest(
    clean_runtime: str, tmp_path: Path, capsys: Any, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every CERN reader shares one subject, so a per-user block is the brake."""
    _setup(clean_runtime, tmp_path, capsys, monkeypatch)
    recipe = _recipe()
    provider_id = _oidc_block()["provider_id"]

    _cli(
        capsys, "binding", "block", *_db(clean_runtime),
        "--provider-id", provider_id, "--user", USER, "--reason", "abuse report",
    )
    with _oidc_server(clean_runtime) as mcp:
        blocked = _tool_call_gate(mcp, _verified_token([recipe["role"]]))
        other = _tool_call_gate(mcp, _verified_token([recipe["role"]], user=OTHER_USER))

    assert blocked is not None and blocked.code == "nomos_principal_blocked", (
        _describe(blocked)
    )
    assert other is None, f"an unblocked reader was refused: {_describe(other)}"


@pytest.mark.postgres
@pytest.mark.usefixtures("okg_mcp_can_log_in")
def test_a_grant_alone_does_not_admit_a_subject_the_baseline_does_not_allow(
    clean_runtime: str, tmp_path: Path, capsys: Any, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The recipe as first written (``subject:cms-kb:reader``) stays refused.

    Pins the reason the runbook binds CERN roles to ``subject:mcp_client``:
    the grant matches, the rollout baseline names no rule for the subject, and
    enforcement refuses with ``nomos.default_deny``.
    """
    subject = "subject:cms-kb:reader"
    _setup(clean_runtime, tmp_path, capsys, monkeypatch, subject=subject)

    reader = _verified_token([_recipe()["role"]])
    with _oidc_server(clean_runtime) as mcp:
        refusal = _tool_call_gate(mcp, reader)

    assert refusal is not None and refusal.code == "nomos_identity_denied", (
        _describe(refusal)
    )
    assert refusal.data["reason_code"] == "nomos.default_deny"
    assert refusal.data["resolved_subject"] == subject
    assert refusal.data["permission_grant_ids"] == [_recipe()["grant_id"]]
