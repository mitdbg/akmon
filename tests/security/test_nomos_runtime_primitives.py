from __future__ import annotations

import pytest

pytest.importorskip("mcp")

from okg.substrate.mcp.server import _sanitize_caller_admission_mode
from okg.substrate.mcp.source_lifecycle import _literal_secret_param_keys
from okg.substrate.mcp.sql_safety import RunSqlNonSelect, validate_run_sql
from okg.substrate.nomos import (
    NomosPrimitiveCheck,
    NomosRuntimeRequest,
    compile_policy_graph,
    evaluate_runtime_boundary,
)


def test_nomos_composes_with_mcp_write_authority_sanitizer() -> None:
    sanitized = _sanitize_caller_admission_mode({
        "admission_mode": "fast_track",
        "producer_policy": {"admission_mode": "fast_track"},
        "target_branch_name": "experiment:attacker",
    })

    assert sanitized["admission_mode"] == "review"
    assert sanitized["producer_policy"]["admission_mode"] == "review"
    assert "target_branch_name" not in sanitized

    outcome = evaluate_runtime_boundary(
        _allow_mcp_record_policy(),
        NomosRuntimeRequest(
            surface="mcp.record",
            subject="subject:operator",
            resource={"tool": "record", "deployment": "okg-workspace"},
            context={"authority": sanitized},
        ),
        primitive_checks=(
            NomosPrimitiveCheck(name="mcp.write_authority", allowed=True),
        ),
    )

    assert outcome.allowed is True
    assert outcome.decision.effect == "allow"


def test_existing_primitive_denial_overrides_nomos_allow() -> None:
    outcome = evaluate_runtime_boundary(
        _allow_mcp_record_policy(),
        NomosRuntimeRequest(
            surface="mcp.record",
            subject="subject:operator",
            resource={"tool": "record", "deployment": "okg-workspace"},
        ),
        primitive_checks=(
            NomosPrimitiveCheck(
                name="operator_route_authorization",
                allowed=False,
                reason_code="nomos.runtime.operator_route_denied",
            ),
        ),
    )

    assert outcome.allowed is False
    assert outcome.decision.effect == "deny"
    assert outcome.decision.reason_code == "nomos.runtime.operator_route_denied"


def test_sql_safety_guard_remains_authoritative_before_nomos_allow() -> None:
    with pytest.raises(RunSqlNonSelect):
        validate_run_sql("DROP TABLE okg.graph_nodes")

    outcome = evaluate_runtime_boundary(
        _allow_graph_sql_policy(),
        NomosRuntimeRequest(
            surface="graph.read.sql_admin",
            subject="subject:operator",
            resource={"sql": "DROP TABLE okg.graph_nodes"},
        ),
        primitive_checks=(
            NomosPrimitiveCheck(
                name="run_sql.validate_run_sql",
                allowed=False,
                reason_code="nomos.runtime.sql_safety_denied",
            ),
        ),
    )

    assert outcome.allowed is False
    assert outcome.decision.reason_code == "nomos.runtime.sql_safety_denied"


def test_secret_ref_gate_remains_authoritative_before_nomos_allow() -> None:
    literal_keys = _literal_secret_param_keys({
        "api_key": "sk-literal",
        "notes_root": "/tmp/notes",
    })

    outcome = evaluate_runtime_boundary(
        _allow_secret_use_policy(),
        NomosRuntimeRequest(
            surface="secret.use",
            subject="subject:operator",
            resource={"api_key": "sk-literal"},
        ),
        primitive_checks=(
            NomosPrimitiveCheck(
                name="source_lifecycle.secret_ref_required",
                allowed=not literal_keys,
                reason_code="nomos.runtime.secret_ref_required",
                metadata={"literal_secret_keys": literal_keys},
            ),
        ),
    )

    assert literal_keys == ["api_key"]
    assert outcome.allowed is False
    assert outcome.decision.effect == "deny"
    assert outcome.decision.reason_code == "nomos.runtime.secret_ref_required"
    assert "sk-literal" not in str(outcome.trace)


def _allow_mcp_record_policy():
    return compile_policy_graph({
        "policy_bundle_id": "nomos:test:security-mcp",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_record",
                "effect": "allow",
                "actions": ["mcp.record"],
                "subjects": ["subject:operator"],
                "reason_code": "nomos.test.mcp_record_allowed",
            },
        ],
    })


def _allow_graph_sql_policy():
    return compile_policy_graph({
        "policy_bundle_id": "nomos:test:security-sql",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_sql_admin",
                "effect": "allow",
                "actions": ["graph.read.sql_admin"],
                "subjects": ["subject:operator"],
                "reason_code": "nomos.test.sql_admin_allowed",
            },
        ],
    })


def _allow_secret_use_policy():
    return compile_policy_graph({
        "policy_bundle_id": "nomos:test:security-secret",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_secret_use",
                "effect": "allow",
                "actions": ["secret.use"],
                "subjects": ["subject:operator"],
                "reason_code": "nomos.test.secret_use_allowed",
            },
        ],
    })
