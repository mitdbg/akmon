from __future__ import annotations

import json
import os
from pathlib import Path
from types import SimpleNamespace

import psycopg
import pytest

pytest.importorskip("mcp")

from okg.deployment.testing import mock_dbos_context
from okg.substrate.errors import SubstrateError
from okg.substrate.exchange.harness import database_doctor, describe_generation
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.nomos import (
    activate_policy_bundle,
    compile_policy_graph,
    seed_deployment_baseline_nomos_state,
)
from okg.substrate.publish_proof import assert_publish_proof, dlq_count
from okg.substrate.publisher import Publisher
from tests.substrate._db_helpers import seed_catalog_version
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402
from tests.substrate.nomos.test_nomos_fine_grained_access_control import (  # noqa: E402
    _publish_fine_grained_graph,
)


pytestmark = pytest.mark.postgres


def _serve_query_under_rules(monkeypatch: pytest.MonkeyPatch) -> None:
    """Have the next build serve `query` although access rules are on.

    Under audit or enforce the server withholds `query` at build and refuses
    it on every call (issue #3666). The tests that call this prove run_sql's
    own run-time check below that, which still guards run_sql's other
    callers.
    """
    monkeypatch.setattr(
        "okg.substrate.mcp.effective_surface.query_withheld_by_access_rules",
        lambda _name: None,
    )


def test_mcp_graph_read_nomos_denial_blocks_read_and_persists_decisions(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _publish_fixture_graph(clean_runtime)
    _activate_mcp_policy(
        clean_runtime,
        rules=[
            _allow_rule("allow_tool_call", "mcp.tool.call"),
        ],
    )
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mcp-client")

    from okg.substrate.mcp.server import build_server

    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        tool = mcp._tool_manager._tools["inspect"]
        with pytest.raises(SubstrateError, match="Nomos denied graph.read.mcp"):
            tool.fn(target={"kind": "graph"})
    finally:
        mcp._okg_pool.close()

    assert _decision_rows(clean_runtime) == [
        ("policy.activate", "allow", "policy_activation"),
        ("mcp.tool.call", "allow", "mcp_tool_call"),
        ("graph.read.mcp", "deny", "mcp_graph_read"),
    ]


def test_mcp_query_uses_sql_admin_boundary(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _publish_fixture_graph(clean_runtime)
    _activate_mcp_policy(
        clean_runtime,
        rules=[
            _allow_rule("allow_tool_call", "mcp.tool.call"),
        ],
    )
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mcp-client")
    _serve_query_under_rules(monkeypatch)

    from okg.substrate.mcp.server import build_server

    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        tool = mcp._tool_manager._tools["query"]
        with pytest.raises(
            SubstrateError,
            match="Nomos denied graph.read.sql_admin",
        ):
            tool.fn(sql="SELECT * FROM okg.v_nodes LIMIT 1")
    finally:
        mcp._okg_pool.close()

    assert _decision_rows(clean_runtime) == [
        ("policy.activate", "allow", "policy_activation"),
        ("mcp.tool.call", "allow", "mcp_tool_call"),
        ("graph.read.sql_admin", "deny", "mcp_graph_read"),
    ]


def test_mcp_graph_read_nomos_allow_persists_before_read(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _publish_fixture_graph(clean_runtime)
    _activate_mcp_policy(
        clean_runtime,
        rules=[
            _allow_rule("allow_tool_call", "mcp.tool.call"),
            _allow_rule(
                "allow_mcp_read_boundary",
                "graph.read.mcp",
                match={"resource_kind": "mcp_graph_read_boundary"},
            ),
            _allow_rule(
                "allow_mcp_read_source",
                "graph.read.mcp",
                match={"source_id": "fixture"},
            ),
        ],
    )
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mcp-client")

    from okg.substrate.mcp.server import build_server

    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        tool = mcp._tool_manager._tools["inspect"]
        out = tool.fn(target={"kind": "graph"})
    finally:
        mcp._okg_pool.close()

    assert out["node_count"] == 1
    assert _decision_rows(clean_runtime) == [
        ("policy.activate", "allow", "policy_activation"),
        ("mcp.tool.call", "allow", "mcp_tool_call"),
        ("graph.read.mcp", "allow", "mcp_graph_read"),
        ("graph.read.mcp", "allow", "mcp_node_source_read"),
        ("graph.read.mcp", "deny", "mcp_node_graph_object_read"),
        ("graph.read.mcp", "allow", "mcp_node_source_record_read"),
    ]


def test_mcp_record_nomos_denial_blocks_before_payload_lowering(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _publish_fixture_graph(clean_runtime)
    _activate_mcp_policy(
        clean_runtime,
        rules=[
            {
                "rule_id": "allow_other_subject_record",
                "effect": "allow",
                "actions": ["mcp.record"],
                "subjects": ["subject:someone-else"],
                "priority": 1,
                "reason_code": "nomos.test.unmatched_record",
            }
        ],
    )
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mcp-client")

    from okg.substrate.mcp.server import build_server

    mcp = build_server(
        dsn=clean_runtime,
        deployment_name="default",
        allow_writes=True,
    )
    try:
        tool = mcp._tool_manager._tools["record"]
        out = tool.fn(
            kind="observation",
            payload={"summary": "should not lower"},
            authority={"subject": "subject:mcp-client"},
        )
    finally:
        mcp._okg_pool.close()

    assert out["status"] == "refused"
    authority = out["error"]["authority"]
    assert authority["authorized"] is False
    assert authority["nomos_decision"]["effect"] == "deny"
    assert _decision_rows(clean_runtime) == [
        ("policy.activate", "allow", "policy_activation"),
        ("mcp.record", "deny", "mcp_action"),
    ]


def test_mcp_record_ignores_caller_supplied_subject(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _publish_fixture_graph(clean_runtime)
    _activate_mcp_policy(
        clean_runtime,
        rules=[
            {
                "rule_id": "allow_declared_writer_record",
                "effect": "allow",
                "actions": ["mcp.record"],
                "subjects": ["subject:mcp_client"],
                "match": {
                    "deployment": "default",
                    "scope": "deployment:default",
                },
                "priority": 100,
                "reason_code": "nomos.test.declared_writer",
            }
        ],
    )
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:undeclared-server")

    from okg.substrate.mcp.server import build_server

    mcp = build_server(
        dsn=clean_runtime,
        deployment_name="default",
        allow_writes=True,
    )
    # The server's own subject stays subject:undeclared-server for the call
    # (record resolves it per call, the way reads do -- #2157); only the
    # caller's authority dict names subject:mcp_client, and it must not count.
    try:
        out = mcp._tool_manager._tools["record"].fn(
            kind="observation",
            payload={"summary": "Caller identity must not authorize."},
            authority={
                "actor": "subject:mcp_client",
                "subject": "subject:mcp_client",
                "producer_id": "subject:mcp_client",
            },
        )
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()

    assert out["status"] == "refused"
    decision = out["error"]["authority"]["nomos_decision"]
    assert decision["effect"] == "deny"
    assert decision["subject"] == "subject:undeclared-server"
    assert decision["resource"]["scope"] == "deployment:default"


def test_mcp_record_baseline_reaches_review_and_publishes_after_acceptance(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.runtime import admission_workflows

    _seed_agent_memory_catalog(clean_runtime, tmp_path)
    initial_publish = _publish_agent_memory_fixture_graph(clean_runtime)
    deployment = "default"
    scope = "deployment:default"
    manifest = {
        "name": deployment,
        "nomos": {
            "runtime_enforcement": "enforce",
            "deployment_posture": {
                "deployment_class": "dogfood_workspace",
                "read_principals": ["mcp_client", "okg_service"],
                "write_principals": ["mcp_client", "okg_service"],
                "mcp_write_default": "review",
                "export_policy": "no_export",
                "secret_handling": "secret_ref_required",
                "live_call_default": "deny",
            },
        },
    }
    with psycopg.connect(clean_runtime) as conn:
        seeded = seed_deployment_baseline_nomos_state(
            conn,
            deployment_name=deployment,
            manifest=manifest,
        )
        conn.commit()
    assert seeded["ok"] is True

    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mcp_client")
    with mock_dbos_context():
        monkeypatch.setattr(
            admission_workflows,
            "_resolve_dsn",
            lambda _deployment: clean_runtime,
        )
        admission_workflows.register_admission_workflows(deployment=deployment)

        from okg.substrate.mcp.server import build_server

        mcp = build_server(
            dsn=clean_runtime,
            deployment_name=deployment,
            allow_writes=True,
        )
        tools = mcp._tool_manager._tools
        try:
            submitted = tools["record"].fn(
                kind="observation",
                payload={"summary": "The deployment baseline admits review."},
                authority={
                    "actor": "subject:forged-actor",
                    "subject": "subject:forged-subject",
                    "producer_id": "subject:forged-producer",
                },
            )
            assert submitted["status"] == "pending"
            decision = submitted["nomos_decision"]
            assert decision["effect"] == "allow"
            assert decision["subject"] == "subject:mcp_client"
            assert decision["resource"]["scope"] == scope
            assert decision["obligations"] == [{"type": "audit"}]

            with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT producer_id, authority
                      FROM okg.admission_envelopes
                     WHERE admission_id = %s
                    """,
                    (submitted["admission_id"],),
                )
                producer_id, admitted_authority = cur.fetchone()
            assert producer_id == "subject:mcp_client"
            assert admitted_authority["actor"] == "subject:mcp_client"
            assert admitted_authority["subject"] == "subject:mcp_client"
            assert admitted_authority["producer_id"] == "subject:mcp_client"

            # Reviewer/admin authorization is a distinct deployment policy
            # boundary from issue #770's Record admission rule. Exercise the
            # launch-consent reviewer path here, then restore enforcement for
            # the baseline-authorized generation publish below.
            monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "0")
            accepted = tools["propose"].fn(
                action="decide",
                payload={
                    "admission_id": submitted["admission_id"],
                    "decision": "accept",
                    "reason": "issue-770 publish proof",
                },
                authority={"actor": "reviewer", "allow_write": True},
            )
            assert accepted["status"] == "accepted", accepted
            assert accepted["emitted"]["node_facts_inserted"] == 1
            monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
        finally:
            mcp._okg_pool.close()
            if mcp._okg_write_pool:
                mcp._okg_write_pool.close()

    published = Publisher(clean_runtime, deployment, catalog_identity=bind_test_catalog_writer(clean_runtime, deployment)).cycle()
    proof = assert_publish_proof(
        published,
        dsn=clean_runtime,
        require_row_delta=False,
    )
    assert proof["ok"] is True, proof.get("reason")
    before = describe_generation(clean_runtime, initial_publish["generation_id"])
    after = describe_generation(clean_runtime, published["generation_id"])
    assert before["status"] == "published"
    assert after["status"] == "published"
    assert after["nodes_live"] - before["nodes_live"] == 1

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        initial_graph = store.checkout_generation(initial_publish["generation_id"])
        graph = store.checkout_generation(published["generation_id"])
        initial_record_count = int(
            initial_graph.query(
                "SELECT count(*) AS n FROM okg.graph_nodes WHERE node_id = :node_id",
                {"node_id": submitted["record_id"]},
            )[0]["n"]
        )
        published_record_count = int(
            graph.query(
                "SELECT count(*) AS n FROM okg.graph_nodes WHERE node_id = :node_id",
                {"node_id": submitted["record_id"]},
            )[0]["n"]
        )
        visible = int(
            graph.query(
                "SELECT count(*) AS n FROM okg.graph_nodes "
                "WHERE subtype = 'agent_observation'"
            )[0]["n"]
        )
    assert initial_record_count == 0
    assert published_record_count == 1
    assert visible == 2

    dead_letter, queue_depth = dlq_count(clean_runtime)
    doctor = database_doctor(clean_runtime)
    assert dead_letter == 0
    assert all(value == 0 for value in queue_depth.values())
    assert doctor["ok"] is True

    report_path = os.getenv("OKG_ISSUE_770_GRAPH_REPORT")
    if report_path:
        artifact = Path(report_path)
        artifact.parent.mkdir(parents=True, exist_ok=True)
        artifact.write_text(
            json.dumps(
                {
                    "evidence": [
                        {
                            "kind": "published_generation",
                            "status": "pass",
                            "refs": [
                                "req.record.baseline-grant",
                                "task.record.baseline-grant",
                                "test.record.enforce-publish",
                                "graph.record.enforce-publish",
                            ],
                            "summary": (
                                f"generation {published['generation_id']} published; "
                                "agent_observation nodes_live +1; DLQ 0; doctor clean"
                            ),
                            "command": (
                                "pytest tests/substrate/mcp/"
                                "test_nomos_runtime_boundary.py::"
                                "test_mcp_record_baseline_reaches_review_and_"
                                "publishes_after_acceptance"
                            ),
                            "generation_ref": published["generation_id"],
                            "data": {
                                "schema": (
                                    "okg.pact.graph_report."
                                    "mcp_record_baseline.v1"
                                ),
                                "status": after["status"],
                                "nodes_live_before": before["nodes_live"],
                                "nodes_live_after": after["nodes_live"],
                                "nodes_live_delta": 1,
                                "record_id": submitted["record_id"],
                                "record_rows_initial_generation": (
                                    initial_record_count
                                ),
                                "record_rows_published_generation": (
                                    published_record_count
                                ),
                                "edges_live_before": before["edges_live"],
                                "edges_live_after": after["edges_live"],
                                "queue_depth": queue_depth,
                                "dlq_count": dead_letter,
                                "doctor_ok": doctor["ok"],
                            },
                        }
                    ]
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )


# ==========================================================================
# Issue #2157: ONE subject resolution for the read and record boundaries
# ==========================================================================


_SUBJECT_ENV = ("OKG_NOMOS_SUBJECT", "OKG_MCP_NOMOS_SUBJECT", "OKG_PRINCIPAL")


def _baseline_manifest(*, write_principals: list[str]) -> dict:
    return {
        "name": "default",
        "nomos": {
            "runtime_enforcement": "enforce",
            "deployment_posture": {
                "deployment_class": "dogfood_workspace",
                "read_principals": ["mcp_client", "okg_service"],
                "write_principals": write_principals,
                "mcp_write_default": "review",
                "export_policy": "no_export",
                "secret_handling": "secret_ref_required",
                "live_call_default": "deny",
            },
        },
    }


def _call(mcp, name: str, arguments: dict) -> dict:
    """Drive a tool the way a client does: through FastMCP.call_tool."""
    import anyio

    from okg.substrate.mcp.result_text import result_format_scope

    async def drive():
        # The assertions read the dictionary, so the call asks for the JSON
        # result format (issue #3059: the default is one text block).
        with result_format_scope("json"):
            return await mcp.call_tool(name, arguments)

    result = anyio.run(drive)
    structured = getattr(result, "structuredContent", None)
    if structured:
        return structured
    return json.loads(result.content[0].text)


def _admission_count(dsn: str) -> int:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM okg.admission_envelopes")
        return int(cur.fetchone()[0])


def _decision_subjects(dsn: str) -> list[tuple[str, str, str]]:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT action, effect, subject
              FROM okg.nomos_decisions
             WHERE deployment_name = 'default'
               AND action IN ('mcp.tool.call', 'mcp.record')
             ORDER BY created_at, decision_row_id
            """
        )
        return [(str(a), str(e), str(s)) for a, e, s in cur.fetchall()]


def test_record_presents_the_same_subject_as_reads_under_the_baseline(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With no subject env set, read AND record present subject:mcp_client.

    The record boundary used to resolve its subject on its own and fall
    through to the OS login, so a deployment that granted mcp_client the
    write was still denied on record (#2157). Driven through call_tool.
    """
    from okg.substrate.runtime import admission_workflows

    _seed_agent_memory_catalog(clean_runtime, tmp_path)
    _publish_agent_memory_fixture_graph(clean_runtime)
    with psycopg.connect(clean_runtime) as conn:
        seeded = seed_deployment_baseline_nomos_state(
            conn,
            deployment_name="default",
            manifest=_baseline_manifest(
                write_principals=["mcp_client", "okg_service"],
            ),
        )
        conn.commit()
    assert seeded["ok"] is True

    for name in _SUBJECT_ENV:
        monkeypatch.delenv(name, raising=False)
    # A login name that no policy grants: the pre-fix record path used it.
    monkeypatch.setenv("USER", "os-login-2157")
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")

    from okg.substrate.mcp.server import build_server

    with mock_dbos_context():
        monkeypatch.setattr(
            admission_workflows, "_resolve_dsn", lambda _d: clean_runtime,
        )
        admission_workflows.register_admission_workflows(deployment="default")
        mcp = build_server(
            dsn=clean_runtime, deployment_name="default", allow_writes=True,
        )
        try:
            read = _call(mcp, "inspect", {"target": {"kind": "graph"}})
            submitted = _call(
                mcp,
                "record",
                {
                    "kind": "observation",
                    "payload": {"summary": "Record under the read subject."},
                },
            )
        finally:
            mcp._okg_pool.close()
            if mcp._okg_write_pool:
                mcp._okg_write_pool.close()

    assert "error" not in read, read
    assert submitted["status"] == "pending", submitted
    assert submitted["nomos_decision"]["subject"] == "subject:mcp_client"
    subjects = _decision_subjects(clean_runtime)
    assert ("mcp.tool.call", "allow", "subject:mcp_client") in subjects
    assert ("mcp.record", "allow", "subject:mcp_client") in subjects
    assert all(s != "subject:os-login-2157" for _, _, s in subjects)


def test_record_with_an_unresolved_subject_is_refused_with_zero_writes(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No explicit subject, no baseline default, no login: refuse, named.

    Before the fix ``getpass.getuser()`` raised on the missing login name
    while ``build_server`` resolved the record subject, so the server did
    not start at all. Now it starts and refuses each call by name. The
    policy allows the old chain's literal kinds (``subject:mcp_record`` /
    ``subject:mcp_client``) so that a regression inventing either one
    would be ALLOWED here and fail this test: an identity nobody declared is
    not a policy question, and the call is refused before any decision or
    admission row.
    """
    import getpass

    _publish_fixture_graph(clean_runtime)
    _activate_mcp_policy(
        clean_runtime,
        rules=[
            {
                "rule_id": "allow_literal_kind_subjects",
                "effect": "allow",
                "actions": ["mcp.record", "mcp.tool.call"],
                "subjects": ["subject:mcp_record", "subject:mcp_client"],
                "priority": 100,
                "reason_code": "nomos.test.literal_kind_subjects",
            }
        ],
    )
    for name in (*_SUBJECT_ENV, "USER", "LOGNAME", "LNAME", "USERNAME"):
        monkeypatch.delenv(name, raising=False)

    def _no_login() -> str:
        raise OSError("no login name")

    monkeypatch.setattr(getpass, "getuser", _no_login)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    admissions_before = _admission_count(clean_runtime)

    from okg.substrate.mcp.server import build_server

    mcp = build_server(
        dsn=clean_runtime, deployment_name="default", allow_writes=True,
    )
    try:
        out = _call(
            mcp,
            "record",
            {"kind": "observation", "payload": {"summary": "nobody wrote this"}},
        )
        read = _call(mcp, "inspect", {"target": {"kind": "graph"}})
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()

    assert out["status"] == "refused", out
    assert out["error"]["code"] == "mcp_principal_unresolved", out
    assert read["error"]["code"] == "mcp_principal_unresolved", read
    assert _decision_subjects(clean_runtime) == []
    assert _admission_count(clean_runtime) == admissions_before


# ==========================================================================
# Issue #2343: EVERY MCP path presents the one subject reads and record use
# ==========================================================================


def _serve_source_registry(
    monkeypatch: pytest.MonkeyPatch, root: Path, *, sources: tuple[str, ...],
) -> None:
    """Point the source tools at a registry declaring ``sources``.

    Only the registry lookup is redirected: the deployment manifest stays
    unresolved, as in the other tests here, so enforcement comes from
    ``OKG_NOMOS_RUNTIME_ENFORCEMENT`` and the policy from the database.
    """
    root.mkdir(parents=True)
    (root / "source_registry.yaml").write_text(
        json.dumps({
            "sources": {
                name: {"ownership_id": name, "enabled_by_default": True}
                for name in sources
            }
        }),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "okg.substrate.mcp.source_admin._deployment_dir",
        lambda deployment, **_kw: root,
    )


def _decision_subjects_by_target(dsn: str) -> list[tuple[str, str, str, str]]:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT action, effect, target->>'kind', subject
              FROM okg.nomos_decisions
             WHERE deployment_name = 'default'
               AND action <> 'policy.activate'
               AND action NOT LIKE 'config.%'
             ORDER BY created_at, decision_row_id
            """
        )
        return [(str(a), str(e), str(k), str(s)) for a, e, k, s in cur.fetchall()]


def _seed_default_baseline(dsn: str, *, sources: tuple[str, ...]) -> None:
    with psycopg.connect(dsn) as conn:
        seeded = seed_deployment_baseline_nomos_state(
            conn,
            deployment_name="default",
            manifest=_baseline_manifest(
                write_principals=["mcp_client", "okg_service"],
            ),
            source_names=sources,
        )
        conn.commit()
    assert seeded["ok"] is True


def _close(mcp) -> None:
    mcp._okg_pool.close()
    if mcp._okg_write_pool:
        mcp._okg_write_pool.close()


def test_source_tools_present_the_same_subject_as_reads_under_the_baseline(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """source_status, list_sources and coverage present subject:mcp_client.

    They used ``nomos_subject_from_env``, which skips the baseline default:
    on a baseline deployment with no subject env they presented the login
    name, so the baseline hid every source and the source_admin decision
    rows were written under the login name (#2343). Driven through
    call_tool, with a login name no policy grants.
    """
    _publish_fixture_graph(clean_runtime)
    _seed_default_baseline(clean_runtime, sources=("fixture",))
    _serve_source_registry(monkeypatch, tmp_path / "default", sources=("fixture",))
    for name in _SUBJECT_ENV:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("USER", "os-login-2343")
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")

    from okg.substrate.mcp.server import build_server

    # The source tools are registered only on a writable server.
    mcp = build_server(
        dsn=clean_runtime, deployment_name="default", allow_writes=True,
    )
    try:
        status = _call(mcp, "source_status", {"short_name": "fixture"})
        listed = _call(mcp, "list_sources", {})
        coverage = _call(
            mcp, "inspect", {"target": {"kind": "coverage"}},
        )
    finally:
        _close(mcp)

    assert status.get("status") != "not_authorized", status
    assert [row["source_id"] for row in listed["sources"]] == ["fixture"], listed
    assert "error" not in coverage, coverage
    source_admin = [
        row for row in _decision_subjects_by_target(clean_runtime)
        if row[2] == "source_admin"
    ]
    assert source_admin, _decision_subjects_by_target(clean_runtime)
    assert {subject for *_, subject in source_admin} == {"subject:mcp_client"}
    assert all(
        subject != "subject:os-login-2343"
        for *_, subject in _decision_subjects_by_target(clean_runtime)
    )


def test_run_sql_audit_bypass_presents_the_same_subject_as_reads(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The run_sql audit-bypass decision carries the read subject.

    It used ``nomos_subject_from_env(default_kind="cli_search")`` and so
    wrote the graph.read.sql_admin decision under the login name while the
    same call's read boundary presented subject:mcp_client (#2343).
    """
    _publish_fixture_graph(clean_runtime)
    _seed_default_baseline(clean_runtime, sources=("fixture",))
    for name in _SUBJECT_ENV:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("USER", "os-login-2343")
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "audit")
    _serve_query_under_rules(monkeypatch)

    from okg.substrate.mcp.server import build_server

    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        out = _call(
            mcp, "query", {"sql": "SELECT node_id FROM okg.v_nodes LIMIT 1"},
        )
    finally:
        _close(mcp)

    assert "error" not in out, out
    rows = _decision_subjects_by_target(clean_runtime)
    bypass = [row for row in rows if row[2] == "run_sql_graph_backed"]
    assert bypass, rows
    assert {subject for *_, subject in bypass} == {"subject:mcp_client"}
    assert all(subject != "subject:os-login-2343" for *_, subject in rows)


def test_every_mcp_path_refuses_an_unresolved_subject_with_zero_rows(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No subject env, no baseline default, no login: every path refuses.

    The source tools used to present ``subject:mcp_client`` (the literal
    kind at the end of ``nomos_subject_from_env``) and run_sql
    ``subject:cli_search``; the policy here ALLOWS both, so a regression that
    invents either is allowed and fails this test. Each path is refused by
    name before any decision row.
    """
    import getpass

    from okg.substrate.mcp import source_lifecycle

    _publish_fixture_graph(clean_runtime)
    _activate_mcp_policy(
        clean_runtime,
        rules=[
            {
                "rule_id": "allow_literal_kind_subjects",
                "effect": "allow",
                "actions": ["mcp.tool.call", "graph.read.mcp",
                            "graph.read.sql_admin"],
                "subjects": ["subject:mcp_client", "subject:cli_search"],
                "priority": 100,
                "reason_code": "nomos.test.literal_kind_subjects",
            }
        ],
    )
    _serve_source_registry(monkeypatch, tmp_path / "default", sources=("fixture",))
    for name in (*_SUBJECT_ENV, "USER", "LOGNAME", "LNAME", "USERNAME"):
        monkeypatch.delenv(name, raising=False)

    def _no_login() -> str:
        raise OSError("no login name")

    monkeypatch.setattr(getpass, "getuser", _no_login)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    _serve_query_under_rules(monkeypatch)

    from okg.substrate.mcp.server import build_server

    mcp = build_server(
        dsn=clean_runtime, deployment_name="default", allow_writes=True,
    )
    try:
        with pytest.raises(Exception) as status_exc:
            _call(mcp, "source_status", {"short_name": "fixture"})
        with pytest.raises(Exception) as listed_exc:
            _call(mcp, "list_sources", {})
        coverage = _call(mcp, "inspect", {"target": {"kind": "coverage"}})
        query = _call(mcp, "query", {"sql": "SELECT 1"})
    finally:
        _close(mcp)

    # The source tools have no error envelope: FastMCP raises a ToolError
    # carrying the message, chained from the named SubstrateError.
    for exc in (status_exc.value, listed_exc.value):
        cause = exc.__cause__
        assert isinstance(cause, SubstrateError), exc
        assert cause.code == "mcp_principal_unresolved", exc
        assert cause.data["surface"] == "mcp.source_admin", exc
        assert "Nomos subject did not resolve" in str(exc), exc
    assert coverage["error"]["code"] == "mcp_principal_unresolved", coverage
    assert query["error"]["code"] == "mcp_principal_unresolved", query
    assert _decision_subjects_by_target(clean_runtime) == []

    # The source-visibility gate itself, reached without the read boundary
    # (the coverage manifest imports it), refuses by name too.
    monkeypatch.setattr(
        "okg.substrate.nomos.baseline_default_mcp_subject",
        lambda conn, *, deployment_name, scope: None,
    )
    with psycopg.connect(clean_runtime) as conn:
        with pytest.raises(SubstrateError) as gate_exc:
            source_lifecycle._source_visible_for_subject(
                conn, "default", "fixture",
            )
    assert gate_exc.value.code == "mcp_principal_unresolved"
    assert gate_exc.value.data["surface"] == "mcp.source_admin"
    assert _decision_subjects_by_target(clean_runtime) == []


def test_record_policy_load_error_refuses_by_name_with_zero_writes(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unreadable active policy is an authority failure, not a new subject.

    ``baseline_default_mcp_subject`` used to catch every exception and return
    None, so a policy-load error silently switched record's subject from
    subject:mcp_client to the login name (#2343). Now record refuses with
    ``authority_evaluation_failed`` and writes no decision and no admission;
    the read boundary refuses with the same code.
    """
    from okg.substrate.nomos import read_grants
    from okg.substrate.runtime import admission_workflows

    _seed_agent_memory_catalog(clean_runtime, tmp_path)
    _publish_agent_memory_fixture_graph(clean_runtime)
    _seed_default_baseline(clean_runtime, sources=())
    for name in _SUBJECT_ENV:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("USER", "os-login-2343")
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")

    def _unreadable(conn, *, deployment_name, scope):
        raise RuntimeError("active policy bundle unreadable")

    # Only the subject lookup fails; the evaluator's own load still works,
    # so a regression that falls through to the login name reaches it and
    # writes a decision row this test forbids.
    monkeypatch.setattr(read_grants, "load_active_policy_bundle", _unreadable)
    admissions_before = _admission_count(clean_runtime)
    rows_before = _decision_subjects_by_target(clean_runtime)

    from okg.substrate.mcp.server import build_server

    with mock_dbos_context():
        monkeypatch.setattr(
            admission_workflows, "_resolve_dsn", lambda _d: clean_runtime,
        )
        admission_workflows.register_admission_workflows(deployment="default")
        mcp = build_server(
            dsn=clean_runtime, deployment_name="default", allow_writes=True,
        )
        try:
            submitted = _call(
                mcp,
                "record",
                {
                    "kind": "observation",
                    "payload": {"summary": "written under an unread policy"},
                },
            )
            read = _call(mcp, "inspect", {"target": {"kind": "graph"}})
        finally:
            _close(mcp)

    assert submitted["status"] == "refused", submitted
    assert submitted["error"]["code"] == "authority_evaluation_failed", submitted
    assert read["error"]["code"] == "authority_evaluation_failed", read
    assert _decision_subjects_by_target(clean_runtime) == rows_before
    assert _admission_count(clean_runtime) == admissions_before


def test_multi_user_coverage_gate_presents_the_request_principal(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bob's coverage manifest is gated as Bob, not as the server.

    The source-visibility gate never read the session's subject, so in
    multi-user mode it presented the server's own subject: with the server
    running as alice, Bob's inspect(coverage) listed google_drive (which Bob
    is denied) and both source_admin decision rows were written as alice
    (#2343 review). Driven through FastMCP.call_tool with Bob's header.
    """
    from tests.substrate.mcp.test_principal_mapping import (
        PRINCIPAL_A,
        PRINCIPAL_B,
        PRINCIPAL_HEADER,
        SUBJECT_A,
        SUBJECT_B,
        _activate_two_principal_policy,
        _configure_header_identity,
        _multi_user_server,
        _publish_two_source_graph,
        request_headers,
    )

    _publish_two_source_graph(clean_runtime)
    _configure_header_identity(
        clean_runtime,
        principals={PRINCIPAL_A: SUBJECT_A, PRINCIPAL_B: SUBJECT_B},
    )
    _activate_two_principal_policy(clean_runtime)
    _serve_source_registry(
        monkeypatch, tmp_path / "default", sources=("public_docs", "google_drive"),
    )
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    # The server's own subject is alice, who MAY see google_drive: a gate
    # that presents the server instead of the caller shows it to Bob.
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", SUBJECT_A)

    with _multi_user_server(clean_runtime) as mcp:
        with request_headers({PRINCIPAL_HEADER: PRINCIPAL_B}):
            coverage = _call(mcp, "inspect", {"target": {"kind": "coverage"}})

    assert "error" not in coverage, coverage
    assert "google_drive" not in json.dumps(coverage, default=str), coverage
    assert "public_docs" in json.dumps(coverage, default=str), coverage
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT subject, effect, target->>'source_id'
              FROM okg.nomos_decisions
             WHERE deployment_name = 'default'
               AND target->>'kind' = 'source_admin'
             ORDER BY decision_row_id
            """
        )
        gate_rows = [(str(s), str(e), str(src)) for s, e, src in cur.fetchall()]
    assert sorted(gate_rows) == [
        (SUBJECT_B, "allow", "public_docs"),
        (SUBJECT_B, "deny", "google_drive"),
    ], gate_rows


_LEAKY_DB_ERROR = (
    'connection to server at "10.1.2.3", port 5432 failed: FATAL: password '
    'authentication failed for user "okg_app"'
)


def test_policy_load_failure_is_a_bounded_named_refusal_on_every_path(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A database failure in the subject lookup is authority_unavailable.

    The source tools had no mapping for it: source_status and list_sources
    surfaced the raw driver error, database host and user included, with no
    code (#2343 review). Every path now refuses with authority_unavailable,
    and no client-visible text carries the host or the user.
    """
    from okg.substrate.nomos import read_grants

    _publish_fixture_graph(clean_runtime)
    _seed_default_baseline(clean_runtime, sources=("fixture",))
    _serve_source_registry(monkeypatch, tmp_path / "default", sources=("fixture",))
    for name in _SUBJECT_ENV:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("USER", "os-login-2343")
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    _serve_query_under_rules(monkeypatch)
    rows_before = _decision_subjects_by_target(clean_runtime)

    from okg.substrate.mcp.server import build_server

    mcp = build_server(
        dsn=clean_runtime, deployment_name="default", allow_writes=True,
    )

    def _unreachable(conn, *, deployment_name, scope):
        raise psycopg.OperationalError(_LEAKY_DB_ERROR)

    monkeypatch.setattr(read_grants, "load_active_policy_bundle", _unreachable)
    try:
        with pytest.raises(Exception) as status_exc:
            _call(mcp, "source_status", {"short_name": "fixture"})
        with pytest.raises(Exception) as listed_exc:
            _call(mcp, "list_sources", {})
        coverage = _call(mcp, "inspect", {"target": {"kind": "coverage"}})
        query = _call(mcp, "query", {"sql": "SELECT 1"})
        recorded = _call(
            mcp,
            "record",
            {"kind": "observation", "payload": {"summary": "unreadable policy"}},
        )
    finally:
        _close(mcp)

    for exc in (status_exc.value, listed_exc.value):
        cause = exc.__cause__
        assert isinstance(cause, SubstrateError), exc
        assert cause.code == "authority_unavailable", exc
        assert cause.data["surface"] == "mcp.source_admin", exc
    assert coverage["error"]["code"] == "authority_unavailable", coverage
    assert query["error"]["code"] == "authority_unavailable", query
    assert recorded["status"] == "refused", recorded
    assert recorded["error"]["code"] == "authority_unavailable", recorded
    client_text = " ".join(
        [str(status_exc.value), str(listed_exc.value)]
        + [json.dumps(out, default=str) for out in (coverage, query, recorded)]
    )
    assert "10.1.2.3" not in client_text
    assert "okg_app" not in client_text
    assert _decision_subjects_by_target(clean_runtime) == rows_before


def _publish_fixture_graph(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog_version(conn)
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    node_id="node:nomos-mcp",
                    subtype="page",
                    attrs={"text": "nomos mcp read boundary"},
                    source_record_id={"id": "node:nomos-mcp"},
                    source_revision={"v": 1},
                )
            ],
            source="fixture",
        )
    result = Publisher(dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()
    assert result["status"] == "published"


def _seed_agent_memory_catalog(dsn: str, tmp_path: Path) -> int:
    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog

    deployment = tmp_path / "record-agent-memory-deployment"
    deployment.mkdir()
    (deployment / "deployment.yaml").write_text(
        json.dumps(
            {
                "name": "record-agent-memory-deployment",
                "description": "MCP record baseline fixture",
                "modules": ["agent_memory"],
            }
        ),
        encoding="utf-8",
    )
    (deployment / "schemas").mkdir()
    composed = compose_catalog(deployment)
    with psycopg.connect(dsn) as conn:
        return int(apply_composed_catalog(conn, composed))


def _publish_agent_memory_fixture_graph(dsn: str) -> dict:
    with psycopg.connect(dsn, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(
                    node_id="observation:nomos-mcp-baseline",
                    subtype="agent_observation",
                    attrs={
                        "summary": "Initial observation",
                        "text": "Initial observation",
                    },
                    source_record_id={"id": "observation:nomos-mcp-baseline"},
                    source_revision={"v": 1},
                )
            ],
            source="fixture",
        )
    result = Publisher(dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()
    assert result["status"] == "published"
    return result


def _activate_mcp_policy(dsn: str, *, rules: list[dict]) -> None:
    bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:mcp-runtime-boundary",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": rules,
    })
    with psycopg.connect(dsn) as conn:
        activate_policy_bundle(
            conn,
            deployment_name="default",
            bundle=bundle,
            scope="deployment:default",
            activated_by="test:operator",
        )
        conn.commit()


def _allow_rule(
    rule_id: str,
    action: str,
    *,
    match: dict | None = None,
) -> dict:
    return {
        "rule_id": rule_id,
        "effect": "allow",
        "actions": [action],
        "subjects": ["subject:mcp-client"],
        "match": match or {},
        "priority": 100,
        "reason_code": f"nomos.test.{rule_id}",
    }


def _decision_rows(dsn: str) -> list[tuple[str, str, str]]:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT action, effect, target->>'kind'
              FROM okg.nomos_decisions
             WHERE deployment_name = 'default'
             ORDER BY created_at, decision_row_id
            """
        )
        return [(str(a), str(e), str(k)) for a, e, k in cur.fetchall()]


# --- issue #2279: `okg search` reports the Nomos decision for THIS call ------
#
# The CLI search boundary (graph.read.cli_search) under enforce, audit and off.
# These cells live beside the MCP read-boundary tests above because they pin
# the same thing for the CLI read surface: what the boundary decided, and that
# a refusal withholds every row.
#
# Before the fix, audit mode reported the policy's recorded ``deny`` as
# ``nomos_effect`` while the call went ahead and returned rows, and did the
# same on a graph with nothing to match, so the field read as a refusal that
# never happened. The matrix below pins every cell: enforce/audit/off x a
# granted/ungranted subject x a matching/non-matching query x a populated or
# empty graph. A refused call must return no rows at all.


def _publish_empty_graph(dsn: str) -> str:
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog_version(conn)
    result = Publisher(
        dsn, "default",
        catalog_identity=bind_test_catalog_writer(dsn, "default"),
    ).cycle()
    assert result["status"] == "published", result
    return str(result["generation_id"])


def _run_cli_search(
    dsn: str,
    generation_id: str,
    query: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> tuple[int, dict, str]:
    from okg.substrate.cli import _read_side
    from okg.substrate.cli.dsn_authority import ReadDsnResolution

    monkeypatch.setattr(
        _read_side,
        "resolve_read_dsn",
        lambda _args, *, surface: ReadDsnResolution(
            dsn=dsn,
            deployment="default",
            authority={"mode": "test", "surface": surface},
            target=None,
        ),
    )
    exit_code = _read_side.cmd_search(
        SimpleNamespace(
            deployment="default",
            dsn=None,
            generation_id=generation_id,
            query=query,
            method="lexical",
            subtype=None,
            attr_filter=None,
            select=None,
            limit=10,
            offset=0,
        ),
        as_json=True,
    )
    captured = capsys.readouterr()
    stream = captured.out if "{" in captured.out else captured.err
    payload, _end = json.JSONDecoder().raw_decode(stream[stream.index("{"):])
    return exit_code, payload, captured.out + captured.err


def _activate_search_policy_2279(dsn: str) -> None:
    """Allow every read except ``okg search`` by ``subject:stranger``.

    The search boundary is decided per call, so one subject is granted it for
    any query and one is refused it for any query.
    """
    bundle = compile_policy_graph(
        {
            "policy_bundle_id": "nomos:test:search-effect-2279",
            "policy_version": "v1",
            "default_effect": "allow",
            "rules": [
                {
                    "rule_id": "deny_cli_search_to_stranger",
                    "effect": "deny",
                    "actions": ["graph.read.cli_search"],
                    "subjects": ["subject:stranger"],
                    "match": {"resource_kind": "cli_search_boundary"},
                    "priority": 300,
                },
            ],
        }
    )
    with psycopg.connect(dsn) as conn:
        activate_policy_bundle(
            conn,
            deployment_name="default",
            bundle=bundle,
            scope="deployment:default",
            activated_by="test:operator",
        )
        conn.commit()


# (mode, subject) -> expected envelope, or None for a refusal.
_EXPECTED_2279 = {
    ("enforce", "subject:benchmark"): ("allow", "allow"),
    ("enforce", "subject:stranger"): None,
    ("audit", "subject:benchmark"): ("allow", "allow"),
    ("audit", "subject:stranger"): ("audit", "deny"),
    ("off", "subject:benchmark"): (None, None),
    ("off", "subject:stranger"): (None, None),
}


def _published_graph_2279(dsn: str, graph: str) -> str:
    """Reset the runtime, publish one graph shape, activate the policy.

    Runs once per graph shape (class-scoped fixtures below), not once per
    cell: every cell only reads, and each sets its own mode and subject
    through function-scoped ``monkeypatch``, so no cell's state reaches the
    next. The persisted Nomos decision rows are content-addressed and
    insert-once, so a repeated decision changes nothing another cell reads.
    """
    from okg.substrate.db.pool import close_all
    from tests.substrate.conftest import reset_okg_test_runtime

    reset_okg_test_runtime(dsn)
    generation_id = (
        _publish_fine_grained_graph(dsn)
        if graph == "populated"
        else _publish_empty_graph(dsn)
    )
    _activate_search_policy_2279(dsn)
    close_all()
    return generation_id


@pytest.fixture(scope="class")
def populated_graph_2279(okg_migrated: str):
    yield okg_migrated, _published_graph_2279(okg_migrated, "populated")
    from okg.substrate.db.pool import close_all

    close_all()


@pytest.fixture(scope="class")
def empty_graph_2279(okg_migrated: str):
    yield okg_migrated, _published_graph_2279(okg_migrated, "empty")
    from okg.substrate.db.pool import close_all

    close_all()


def _assert_search_cell_2279(
    dsn: str,
    generation_id: str,
    *,
    graph: str,
    mode: str,
    subject: str,
    query: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", mode)
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", subject)

    exit_code, payload, output = _run_cli_search(
        dsn, generation_id, query, monkeypatch, capsys,
    )

    expected = _EXPECTED_2279[(mode, subject)]
    if expected is None:
        # A refused call is an error envelope with no rows in it -- the
        # denial is not a label on a result, it withholds the result.
        assert exit_code != 0
        assert payload["error"]["code"] == "nomos_runtime_denied"
        assert payload["error"]["data"]["final_effect"] == "deny"
        assert "results" not in payload
        assert "node:" not in output
        return

    call_effect, policy_effect = expected
    assert exit_code == 0, output
    assert payload["nomos_enforcement_mode"] == mode
    assert payload["nomos_effect"] == call_effect
    assert payload["nomos_policy_effect"] == policy_effect
    # A call that went ahead is never labelled a refusal.
    assert payload["nomos_effect"] != "deny"
    if mode == "off":
        assert payload["nomos_decision_id"] is None
    else:
        assert payload["nomos_decision_id"].startswith("nomos:decision:")
    rows = payload["results"]
    if graph == "populated" and query == "ticket":
        assert rows
    else:
        assert rows == []


_CELLS_2279 = pytest.mark.parametrize(
    ("mode", "subject", "query"),
    [
        (mode, subject, query)
        for mode in ("enforce", "audit", "off")
        for subject in ("subject:benchmark", "subject:stranger")
        for query in ("ticket", "zzqxnomatch")
    ],
)


class TestSearchNomosEffectPopulatedGraph2279:
    @_CELLS_2279
    def test_cli_search_nomos_effect_is_the_decision_for_this_call(
        self, mode, subject, query, populated_graph_2279, monkeypatch, capsys,
    ) -> None:
        dsn, generation_id = populated_graph_2279
        _assert_search_cell_2279(
            dsn, generation_id, graph="populated", mode=mode,
            subject=subject, query=query,
            monkeypatch=monkeypatch, capsys=capsys,
        )


class TestSearchNomosEffectEmptyGraph2279:
    @_CELLS_2279
    def test_cli_search_nomos_effect_is_the_decision_for_this_call(
        self, mode, subject, query, empty_graph_2279, monkeypatch, capsys,
    ) -> None:
        dsn, generation_id = empty_graph_2279
        _assert_search_cell_2279(
            dsn, generation_id, graph="empty", mode=mode,
            subject=subject, query=query,
            monkeypatch=monkeypatch, capsys=capsys,
        )
