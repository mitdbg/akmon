"""Issue #2891: an MCP client cannot choose a lower Nomos classification.

The MCP record and graph-read boundaries evaluate the server-side class of the
surface (its Nomos surface mapping). A client's ``authority.classification``
may raise that class but never lower it; an unknown or incomparable value is
refused before any decision is rendered.

The same issue covers the other policy inputs a client could send in
``authority``: a policy bundle file path is refused and never loaded, and a
scope or resource fact that differs from the server's is refused.

Two tiers: hermetic tests of the ranking rule, and ``postgres`` tests that
drive the real MCP tools against an active policy and read the decision rows.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import psycopg
import pytest

pytest.importorskip("mcp")

from okg.substrate.errors import SubstrateError
from okg.substrate.mcp.authority_overrides import client_policy_overrides
from okg.substrate.nomos import (
    PermissionGrant,
    activate_policy_bundle,
    compile_policy_graph,
    upsert_permission_grant,
)
from okg.substrate.nomos.classification_order import (
    CLASSIFICATION_RANK,
    DeclaredClassificationInvalid,
    effective_classification,
)
from okg.substrate.nomos.vocabulary import DATA_CLASSIFICATIONS
from tests.substrate.mcp.test_nomos_runtime_boundary import (  # noqa: E402
    _publish_fixture_graph,
)
from tests.substrate.mcp.test_nomos_source_lineage_mcp import (  # noqa: E402
    _configure_mit_identity_grants,
    _mit_touchstone_claim,
)

# One module, one lane: the hermetic ranking rows need no database, but they
# ride with the postgres rows so a single blocking step owns the file.
pytestmark = pytest.mark.postgres


# ---------------------------------------------------------------------------
# Hermetic: the ranking rule
# ---------------------------------------------------------------------------


def test_every_known_classification_is_ranked() -> None:
    assert set(CLASSIFICATION_RANK) == set(DATA_CLASSIFICATIONS)


@pytest.mark.parametrize("declared", [None, ""])
def test_no_declaration_keeps_the_server_class(declared: object) -> None:
    assert (
        effective_classification(
            server_classification="restricted", declared=declared, surface="s"
        )
        == "restricted"
    )


def test_a_lower_declaration_cannot_lower_the_server_class() -> None:
    assert (
        effective_classification(
            server_classification="restricted", declared="internal", surface="s"
        )
        == "restricted"
    )
    assert (
        effective_classification(
            server_classification="internal", declared="public", surface="s"
        )
        == "internal"
    )


def test_a_higher_declaration_raises_the_class() -> None:
    assert (
        effective_classification(
            server_classification="internal", declared="restricted", surface="s"
        )
        == "restricted"
    )
    assert (
        effective_classification(
            server_classification="internal", declared="private", surface="s"
        )
        == "private"
    )


@pytest.mark.parametrize("declared", ["top-secret", "INTERNAL", 3, ["internal"]])
def test_an_unknown_declaration_is_refused(declared: object) -> None:
    with pytest.raises(DeclaredClassificationInvalid) as caught:
        effective_classification(
            server_classification="internal", declared=declared, surface="s"
        )
    assert caught.value.code == "nomos_declared_classification_invalid"
    assert caught.value.data["declared_classification"] == declared


def test_an_incomparable_declaration_is_refused() -> None:
    with pytest.raises(DeclaredClassificationInvalid):
        effective_classification(
            server_classification="private", declared="synthetic_phi", surface="s"
        )


def test_an_unknown_server_class_is_refused() -> None:
    with pytest.raises(DeclaredClassificationInvalid):
        effective_classification(
            server_classification="bogus", declared=None, surface="s"
        )


# ---------------------------------------------------------------------------
# Postgres: mcp.record under the runtime policy (no identity claim)
# ---------------------------------------------------------------------------


def _activate(dsn: str, rules: list[dict]) -> None:
    bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:declared-classification",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": rules,
    })
    # The first activation has no active bundle to authorize it, so it runs
    # with enforcement off; the fixtures turn enforcement on for the call.
    saved = os.environ.pop("OKG_NOMOS_RUNTIME_ENFORCEMENT", None)
    try:
        with psycopg.connect(dsn) as conn:
            activate_policy_bundle(
                conn,
                deployment_name="default",
                bundle=bundle,
                scope="deployment:default",
                activated_by="test:operator",
            )
            conn.commit()
    finally:
        if saved is not None:
            os.environ["OKG_NOMOS_RUNTIME_ENFORCEMENT"] = saved


def _rule(
    rule_id: str,
    action: str,
    subject: str,
    *,
    effect: str = "allow",
    match: dict | None = None,
    priority: int = 100,
) -> dict:
    return {
        "rule_id": rule_id,
        "effect": effect,
        "actions": [action],
        "subjects": [subject],
        "match": match or {},
        "priority": priority,
        "reason_code": f"nomos.test.{rule_id}",
    }


def _decisions(dsn: str, action: str) -> list[tuple[str, str]]:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT effect, classification
              FROM okg.nomos_decisions
             WHERE deployment_name = 'default' AND action = %s
             ORDER BY created_at, decision_row_id
            """,
            (action,),
        )
        return [(str(e), str(c)) for e, c in cur.fetchall()]


def _record(dsn: str, authority: dict) -> dict:
    from okg.substrate.mcp.server import build_server

    mcp = build_server(dsn=dsn, deployment_name="default", allow_writes=True)
    try:
        return mcp._tool_manager._tools["record"].fn(
            kind="observation",
            payload={"summary": "classification boundary"},
            authority=authority,
        )
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


_RUNTIME_SUBJECT = "subject:mcp-client"


@pytest.fixture
def runtime_record(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    okg_mcp_can_log_in: str,
) -> str:
    # The MCP tools these tests call open the read pool as okg_mcp, which a
    # fresh migration leaves NOLOGIN.
    del okg_mcp_can_log_in
    _publish_fixture_graph(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", _RUNTIME_SUBJECT)
    return clean_runtime


def test_record_claiming_public_is_evaluated_as_internal(
    runtime_record: str,
) -> None:
    """A policy that admits only public writes refuses an internal write that
    claims public. Before the fix the claim was trusted and the write was
    authorized."""
    _activate(
        runtime_record,
        [
            _rule(
                "allow_public_record",
                "mcp.record",
                _RUNTIME_SUBJECT,
                match={"classification": "public"},
            )
        ],
    )

    out = _record(runtime_record, {"classification": "public"})

    assert out["status"] == "refused"
    decision = out["error"]["authority"]["nomos_decision"]
    assert decision["effect"] == "deny"
    assert decision["classification"] == "internal"
    assert _decisions(runtime_record, "mcp.record") == [("deny", "internal")]


def test_record_may_raise_its_classification(runtime_record: str) -> None:
    """Declaring restricted on internal data is evaluated as restricted, so a
    policy that denies restricted refuses it."""
    _activate(
        runtime_record,
        [
            _rule(
                "allow_internal_record",
                "mcp.record",
                _RUNTIME_SUBJECT,
                match={"classification": "internal"},
            ),
            _rule(
                "deny_restricted_record",
                "mcp.record",
                _RUNTIME_SUBJECT,
                effect="deny",
                match={"classification": "restricted"},
                priority=200,
            ),
        ],
    )

    out = _record(runtime_record, {"classification": "restricted"})

    assert out["status"] == "refused"
    decision = out["error"]["authority"]["nomos_decision"]
    assert decision["effect"] == "deny"
    assert decision["classification"] == "restricted"


def test_record_with_an_unknown_classification_is_refused_unevaluated(
    runtime_record: str,
) -> None:
    _activate(
        runtime_record,
        [_rule("allow_any_record", "mcp.record", _RUNTIME_SUBJECT)],
    )

    out = _record(runtime_record, {"classification": "top-secret"})

    assert out["status"] == "refused"
    error = out["error"]
    assert error["code"] == "authority_evaluation_failed"
    evidence = error["authority"]
    assert evidence["evaluated"] is False
    assert evidence["nomos"]["code"] == "nomos_declared_classification_invalid"
    assert _decisions(runtime_record, "mcp.record") == []


# ---------------------------------------------------------------------------
# Postgres: identity-claim paths (record and graph read)
# ---------------------------------------------------------------------------

_CLAIM_SUBJECT = "subject:mit:scientist"


def _grant(dsn: str, grant_id: str, action: str) -> None:
    with psycopg.connect(dsn) as conn:
        upsert_permission_grant(
            conn,
            deployment_name="default",
            grant=PermissionGrant(
                grant_id=grant_id,
                subject=_CLAIM_SUBJECT,
                actions=(action,),
                scope="deployment:default",
                classifications=("*",),
                priority=20,
            ),
        )
        conn.commit()


@pytest.fixture
def claim_runtime(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    okg_mcp_can_log_in: str,
) -> str:
    # As for runtime_record: the MCP read pool logs in as okg_mcp.
    del okg_mcp_can_log_in
    _publish_fixture_graph(clean_runtime)
    _configure_mit_identity_grants(clean_runtime, source_ids=["fixture"])
    _grant(clean_runtime, "grant:mit:sql-admin", "graph.read.sql_admin")
    _grant(clean_runtime, "grant:mit:record", "mcp.record")
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:env-bypass")
    return clean_runtime


def _deny_restricted_policy(dsn: str, action: str) -> None:
    """Allow the tool call and ``action`` for internal data; deny restricted."""
    _activate(
        dsn,
        [
            _rule("allow_tool_call", "mcp.tool.call", _CLAIM_SUBJECT),
            _rule(
                "allow_internal",
                action,
                _CLAIM_SUBJECT,
                match={"classification": "internal"},
            ),
            _rule(
                "deny_restricted",
                action,
                _CLAIM_SUBJECT,
                effect="deny",
                match={"classification": "restricted"},
                priority=200,
            ),
        ],
    )


def _query(dsn: str, authority: dict) -> dict:
    from okg.substrate.mcp.server import build_server

    mcp = build_server(dsn=dsn, deployment_name="default")
    try:
        return mcp._tool_manager._tools["query"].fn(
            sql="SELECT node_id FROM okg.v_nodes LIMIT 1",
            authority=authority,
        )
    finally:
        mcp._okg_pool.close()


def _inspect(dsn: str, authority: dict) -> dict:
    from okg.substrate.mcp.server import build_server

    mcp = build_server(dsn=dsn, deployment_name="default")
    try:
        return mcp._tool_manager._tools["inspect"].fn(
            target={"kind": "graph"},
            authority=authority,
        )
    finally:
        mcp._okg_pool.close()


def test_sql_read_claiming_internal_is_evaluated_as_restricted(
    claim_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The `query` surface is restricted server-side. With an identity claim
    it used to be evaluated at the client's classification, so a client
    claiming internal passed a policy that denies restricted.

    Under access rules the server withholds and refuses `query` (issue
    #3666). This proves run_sql's own run-time check below that, which still
    guards run_sql's other callers, so the server is told to serve it."""
    monkeypatch.setattr(
        "okg.substrate.mcp.effective_surface.query_withheld_by_access_rules",
        lambda _name: None,
    )
    _deny_restricted_policy(claim_runtime, "graph.read.sql_admin")

    with pytest.raises(SubstrateError) as caught:
        _query(
            claim_runtime,
            {
                "nomos_identity_claim": _mit_touchstone_claim(),
                "classification": "internal",
            },
        )

    assert caught.value.code == "nomos_identity_denied"
    assert caught.value.data["surface"] == "graph.read.sql_admin"
    assert _decisions(claim_runtime, "graph.read.sql_admin") == [
        ("deny", "restricted")
    ]


def test_graph_read_claim_may_raise_its_classification(
    claim_runtime: str,
) -> None:
    _deny_restricted_policy(claim_runtime, "graph.read.mcp")
    authority = {"nomos_identity_claim": _mit_touchstone_claim()}

    # Undeclared: the internal boundary read is allowed.
    _inspect(claim_runtime, authority)
    with pytest.raises(SubstrateError) as caught:
        _inspect(claim_runtime, {**authority, "classification": "restricted"})

    assert caught.value.code == "nomos_identity_denied"
    assert caught.value.data["surface"] == "graph.read.mcp"
    boundary = [
        row
        for row in _decisions(claim_runtime, "graph.read.mcp")
        if row[1] == "restricted"
    ]
    assert boundary == [("deny", "restricted")]


def test_graph_read_claim_with_an_unknown_classification_is_refused(
    claim_runtime: str,
) -> None:
    _deny_restricted_policy(claim_runtime, "graph.read.mcp")

    with pytest.raises(DeclaredClassificationInvalid):
        _inspect(
            claim_runtime,
            {
                "nomos_identity_claim": _mit_touchstone_claim(),
                "classification": "top-secret",
            },
        )

    # Refused before evaluation: not even the tool-call decision is written.
    assert _decisions(claim_runtime, "mcp.tool.call") == []
    assert _decisions(claim_runtime, "graph.read.mcp") == []


def test_record_claim_claiming_public_is_evaluated_as_internal(
    claim_runtime: str,
) -> None:
    _activate(
        claim_runtime,
        [
            _rule(
                "allow_public_record",
                "mcp.record",
                _CLAIM_SUBJECT,
                match={"classification": "public"},
            )
        ],
    )

    out = _record(
        claim_runtime,
        {
            "nomos_identity_claim": _mit_touchstone_claim(),
            "classification": "public",
        },
    )

    assert out["status"] == "refused"
    decision = out["error"]["authority"]["nomos_decision"]
    assert decision["effect"] == "deny"
    assert decision["classification"] == "internal"


def test_record_claim_with_an_unknown_classification_is_refused(
    claim_runtime: str,
) -> None:
    out = _record(
        claim_runtime,
        {
            "nomos_identity_claim": _mit_touchstone_claim(),
            "classification": "top-secret",
        },
    )

    assert out["status"] == "refused"
    evidence = out["error"]["authority"]
    assert evidence["evaluated"] is False
    assert evidence["nomos"]["code"] == "nomos_declared_classification_invalid"
    assert _decisions(claim_runtime, "mcp.record") == []


# ---------------------------------------------------------------------------
# The client cannot choose the policy bundle, scope, or resource facts
# ---------------------------------------------------------------------------

_SERVER_SCOPE = "deployment:default"
_SERVER_RESOURCE = {
    "deployment": "default",
    "scope": _SERVER_SCOPE,
    "surface": "mcp",
    "tool": "record",
    "record_kind": "observation",
}


def _overrides(authority: dict) -> list[str]:
    return client_policy_overrides(
        authority, scope=_SERVER_SCOPE, resource=_SERVER_RESOURCE
    )


def test_an_honest_authority_names_no_override() -> None:
    assert _overrides({}) == []
    assert _overrides({"nomos_identity_claim": {"x": 1}}) == []
    assert _overrides({"nomos_bundle_path": None, "nomos_scope": None}) == []


@pytest.mark.parametrize("key", ["nomos_bundle_path", "policy_bundle_path"])
@pytest.mark.parametrize("value", ["/tmp/permissive.json", "", "   "])
def test_any_bundle_path_is_refused(key: str, value: str) -> None:
    assert _overrides({key: value}) == [key]


def test_scope_equal_to_the_server_is_allowed_and_a_different_one_refused() -> None:
    assert _overrides({"nomos_scope": _SERVER_SCOPE}) == []
    assert _overrides({"nomos_scope": "deployment:other"}) == ["nomos_scope"]


def test_resource_facts_must_equal_the_server_facts() -> None:
    assert _overrides({"nomos_resource": {"tool": "record"}}) == []
    assert _overrides({"nomos_resource": {"tool": "search"}}) == [
        "nomos_resource.tool"
    ]
    assert _overrides({"nomos_resource": {"source_id": "fixture"}}) == [
        "nomos_resource.source_id"
    ]
    assert _overrides({"nomos_resource": "not-a-mapping"}) == ["nomos_resource"]


def _permissive_bundle_file(tmp_path: Path) -> Path:
    bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:client-permissive",
        "policy_version": "v1",
        "default_effect": "allow",
        "rules": [
            {
                "rule_id": "allow_everything",
                "effect": "allow",
                "actions": ["*"],
                "subjects": ["*"],
                "priority": 1000,
                "reason_code": "nomos.test.client_permissive",
            }
        ],
    })
    path = tmp_path / "permissive-bundle.json"
    path.write_text(json.dumps(bundle.to_dict()), encoding="utf-8")
    return path


@pytest.fixture
def load_bundle_calls(monkeypatch: pytest.MonkeyPatch) -> list[object]:
    """Record every Nomos bundle-file load the server makes."""
    import okg.substrate.nomos as nomos_module
    import okg.substrate.nomos.core as nomos_core

    calls: list[object] = []
    original = nomos_core.load_bundle

    def _spy(path):  # type: ignore[no-untyped-def]
        calls.append(path)
        return original(path)

    monkeypatch.setattr(nomos_module, "load_bundle", _spy)
    monkeypatch.setattr(nomos_core, "load_bundle", _spy)
    return calls


def _deny_all_records(dsn: str) -> None:
    # Active policy admits the tool call only; no rule allows mcp.record.
    _activate(dsn, [_rule("allow_tool_call", "mcp.tool.call", _CLAIM_SUBJECT)])


def _assert_override_refused(out: dict, dsn: str, fields: list[str]) -> None:
    assert out["status"] == "refused"
    error = out["error"]
    assert error["code"] == "authority_evaluation_failed"
    evidence = error["authority"]
    assert evidence["evaluated"] is False
    assert evidence["nomos"]["code"] == "nomos_client_policy_override_refused"
    assert evidence["nomos"]["data"]["fields"] == fields
    assert _decisions(dsn, "mcp.record") == []


def test_record_claim_bundle_path_is_refused_and_never_loaded(
    claim_runtime: str,
    tmp_path: Path,
    load_bundle_calls: list[object],
) -> None:
    """Before the fix the server loaded the client's bundle file, and a
    default-allow bundle authorized a write the active policy denies."""
    _deny_all_records(claim_runtime)
    permissive = _permissive_bundle_file(tmp_path)

    out = _record(
        claim_runtime,
        {
            "nomos_identity_claim": _mit_touchstone_claim(),
            "nomos_bundle_path": str(permissive),
        },
    )

    assert load_bundle_calls == []
    _assert_override_refused(out, claim_runtime, ["nomos_bundle_path"])


def test_runtime_record_bundle_path_is_refused_not_ignored(
    runtime_record: str,
    tmp_path: Path,
    load_bundle_calls: list[object],
) -> None:
    _activate(
        runtime_record,
        [_rule("allow_any_record", "mcp.record", _RUNTIME_SUBJECT)],
    )

    out = _record(
        runtime_record,
        {"policy_bundle_path": str(_permissive_bundle_file(tmp_path))},
    )

    assert load_bundle_calls == []
    _assert_override_refused(out, runtime_record, ["policy_bundle_path"])


def test_record_claim_with_a_different_scope_is_refused(
    claim_runtime: str,
) -> None:
    _deny_all_records(claim_runtime)

    out = _record(
        claim_runtime,
        {
            "nomos_identity_claim": _mit_touchstone_claim(),
            "nomos_scope": "deployment:other",
        },
    )

    _assert_override_refused(out, claim_runtime, ["nomos_scope"])


def test_record_claim_with_a_client_resource_fact_is_refused(
    claim_runtime: str,
) -> None:
    _deny_all_records(claim_runtime)

    out = _record(
        claim_runtime,
        {
            "nomos_identity_claim": _mit_touchstone_claim(),
            "nomos_resource": {"source_id": "fixture"},
        },
    )

    _assert_override_refused(out, claim_runtime, ["nomos_resource.source_id"])


def _allow_internal_records(dsn: str) -> None:
    _activate(
        dsn,
        [
            _rule("allow_tool_call", "mcp.tool.call", _CLAIM_SUBJECT),
            _rule(
                "allow_internal_record",
                "mcp.record",
                _CLAIM_SUBJECT,
                match={"classification": "internal"},
            ),
        ],
    )


def test_honest_record_claim_is_evaluated_unchanged(
    claim_runtime: str,
    load_bundle_calls: list[object],
) -> None:
    _allow_internal_records(claim_runtime)

    out = _record(claim_runtime, {"nomos_identity_claim": _mit_touchstone_claim()})

    assert out.get("error", {}).get("authority") is None
    assert load_bundle_calls == []
    assert _decisions(claim_runtime, "mcp.record") == [("allow", "internal")]


def test_record_claim_repeating_the_server_scope_and_facts_is_allowed(
    claim_runtime: str,
) -> None:
    _allow_internal_records(claim_runtime)

    out = _record(
        claim_runtime,
        {
            "nomos_identity_claim": _mit_touchstone_claim(),
            "nomos_scope": _SERVER_SCOPE,
            "nomos_resource": {"tool": "record", "surface": "mcp"},
        },
    )

    assert out.get("error", {}).get("authority") is None
    assert _decisions(claim_runtime, "mcp.record") == [("allow", "internal")]


@pytest.fixture(autouse=True)
def _trust_caller_identity_claims(monkeypatch: pytest.MonkeyPatch) -> None:
    """These rows evaluate caller-supplied identity claims on a single-user
    server, which PACT mcp-oidc-principal-policy makes an explicit launch-time
    opt-in; without it the claim is refused (nomos_identity_claim_untrusted)."""
    monkeypatch.setenv("OKG_MCP_TRUST_CALLER_IDENTITY_CLAIMS", "1")
