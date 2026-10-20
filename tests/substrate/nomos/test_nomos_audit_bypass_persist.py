"""Audit-mode log-and-allow persists the strict deny to the decision sink.

req.gov.audit-real: in audit (log-and-allow) mode a boundary the active policy
DENIES still ALLOWS the caller (so a non-blocking audit read proceeds), but the
strict deny decision MUST land in ``okg.nomos_decisions`` — otherwise the
"audit" trail is a no-op and the bypass goes unrecorded.

This is the sink-level proof behind the run_sql audit-bypass recorder
(``okg.substrate.mcp.run_sql._record_run_sql_audit_bypass``), which now calls
``evaluate_runtime_boundary_with_active_policy(..., enforcement_mode="audit",
require_active=True)`` without swallowing persist errors. Mocking the recorder
proves nothing; this exercises the real Postgres-backed boundary.
"""
from __future__ import annotations

import psycopg
import pytest

from okg.substrate.nomos import (
    NomosRuntimeRequest,
    activate_policy_bundle,
    compile_policy_graph,
    evaluate_runtime_boundary_with_active_policy,
)


pytestmark = pytest.mark.postgres


def _deny_sql_admin_policy():
    # default_effect deny + a rule that never matches the audited subject, so the
    # graph.read.sql_admin boundary falls through to a strict deny.
    return compile_policy_graph({
        "policy_bundle_id": "nomos:test:audit-bypass-deny",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_nobody",
                "effect": "allow",
                "actions": ["graph.read.sql_admin"],
                "subjects": ["subject:nobody"],
                "priority": 1,
                "reason_code": "nomos.test.never_matches",
            },
        ],
    })


def test_audit_mode_denied_boundary_allows_caller_but_persists_deny(
    clean_runtime: str,
) -> None:
    deployment = "nomos-audit-bypass"
    scope = f"deployment:{deployment}"
    bundle = _deny_sql_admin_policy()

    with psycopg.connect(clean_runtime) as conn:
        activate_policy_bundle(
            conn,
            deployment_name=deployment,
            bundle=bundle,
            scope=scope,
            activated_by="test:operator",
        )
        outcome = evaluate_runtime_boundary_with_active_policy(
            conn,
            deployment_name=deployment,
            scope=scope,
            request=NomosRuntimeRequest(
                surface="graph.read.sql_admin",
                subject="subject:auditor",
                resource={
                    "resource_kind": "mcp_graph_read_boundary",
                    "use": "run_sql.graph_backed_audit_bypass",
                },
            ),
            target={"kind": "run_sql_graph_backed"},
            performed_obligations=("audit",),
            require_active=True,
            enforcement_mode="audit",
        )
        conn.commit()

    # (a) audit log-and-allow: the caller is NOT blocked.
    assert outcome is not None
    assert outcome.allowed is True
    # ...but the observed strict effect under the boundary is a deny.
    assert outcome.decision.effect == "deny"

    # (b) the strict deny landed in okg.nomos_decisions for this boundary.
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT effect, action
              FROM okg.nomos_decisions
             WHERE deployment_name = %s
               AND target->>'kind' = 'run_sql_graph_backed'
            """,
            (deployment,),
        )
        rows = cur.fetchall()

    assert rows, "audit-mode bypass recorded no decision in okg.nomos_decisions"
    assert all(effect == "deny" for effect, _ in rows)
    assert all(action == "graph.read.sql_admin" for _, action in rows)


def test_audit_mode_persists_deny_even_with_no_active_bundle(
    clean_runtime: str,
) -> None:
    # require_active=True forces a fail-closed deny to persist even when NO
    # policy bundle is active. With require_active=False the boundary would
    # early-return None and write nothing, so the bypass would go unrecorded.
    deployment = "nomos-audit-bypass-noactive"
    scope = f"deployment:{deployment}"

    with psycopg.connect(clean_runtime) as conn:
        outcome = evaluate_runtime_boundary_with_active_policy(
            conn,
            deployment_name=deployment,
            scope=scope,
            request=NomosRuntimeRequest(
                surface="graph.read.sql_admin",
                subject="subject:auditor",
                resource={
                    "resource_kind": "mcp_graph_read_boundary",
                    "use": "run_sql.graph_backed_audit_bypass",
                },
            ),
            target={"kind": "run_sql_graph_backed"},
            performed_obligations=("audit",),
            require_active=True,
            enforcement_mode="audit",
        )
        conn.commit()

    assert outcome is not None
    assert outcome.allowed is True
    assert outcome.decision.effect == "deny"

    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT effect, reason_code
              FROM okg.nomos_decisions
             WHERE deployment_name = %s
               AND target->>'kind' = 'run_sql_graph_backed'
            """,
            (deployment,),
        )
        rows = cur.fetchall()

    assert rows, "no-active-bundle audit bypass recorded no decision"
    assert all(effect == "deny" for effect, _ in rows)
    assert any(
        reason_code == "nomos.default_deny.no_active_bundle"
        for _, reason_code in rows
    )
