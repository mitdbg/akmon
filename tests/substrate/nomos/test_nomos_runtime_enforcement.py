from __future__ import annotations

import json

import pytest

from okg.substrate.nomos import (
    REQUIRED_NOMOS_RUNTIME_BOUNDARIES,
    NomosPrimitiveCheck,
    NomosRuntimeRequest,
    compile_policy_graph,
    manifest_nomos_audit_readiness_exception,
    manifest_nomos_blocks_actions,
    deployment_requires_nomos_runtime,
    evaluate_runtime_boundary,
    manifest_nomos_enforcement_mode_source,
    nomos_runtime_boundary_inventory,
    nomos_runtime_production_bridge_inventory,
    runtime_surface_mapping_for,
    sanitize_trace_value,
)


def _allow_all_policy():
    return compile_policy_graph({
        "policy_bundle_id": "nomos:test:runtime-allow-all",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_operator_runtime",
                "effect": "allow",
                "actions": ["*"],
                "subjects": ["subject:operator"],
                "priority": 100,
                "reason_code": "nomos.test.runtime_allowed",
            },
        ],
    })


def test_runtime_boundary_inventory_covers_architecture_contract() -> None:
    expected = {
        "graph.read.public_demo",
        "graph.read.local_operator",
        "graph.read.mcp",
        "graph.read.cli_search",
        "graph.read.sql_admin",
        "graph.read.export_like",
        "graph.export",
        "mcp.tool.call",
        "mcp.record",
        "source.ingest",
        "source.emit",
        "source.live_call",
        "secret.use",
        "generation.publish",
        "policy.activate",
        "config.activate",
        "declassify",
        "redact",
        "quarantine.release",
        "admin.override",
    }
    inventory = nomos_runtime_boundary_inventory()

    assert set(REQUIRED_NOMOS_RUNTIME_BOUNDARIES) == expected
    assert {row["surface"] for row in inventory} == expected
    for row in inventory:
        mapping = runtime_surface_mapping_for(row["surface"])
        assert row == {
            "surface": row["surface"],
            "action": mapping.action,
            "resource_kind": mapping.resource_kind,
            "principal_kind": mapping.principal_kind,
            "classification": mapping.classification,
        }


def test_production_bridge_inventory_accounts_for_every_boundary() -> None:
    inventory = nomos_runtime_production_bridge_inventory()

    assert {row["surface"] for row in inventory} == set(
        REQUIRED_NOMOS_RUNTIME_BOUNDARIES,
    )
    for row in inventory:
        assert row["status"] in {
            "wired",
            "wired_obligation_effect",
            "unsupported_no_core_surface",
            "unsupported_no_central_hook",
        }
        assert row["bridge"]
        assert row["verification"]
        if row["status"].startswith("unsupported"):
            assert row["verification"] == "checked_inventory"
        else:
            assert row["verification"].startswith("tests/")


def test_deployment_runtime_enforcement_requires_explicit_rollout_flag(
    tmp_path,
    monkeypatch,
) -> None:
    root = tmp_path / "deployments"
    deployment = root / "demo"
    deployment.mkdir(parents=True)
    (deployment / "deployment.yaml").write_text(
        """
name: demo
schema_dir: schemas
source_registry: source_registry.yaml
nomos:
  deployment_posture:
    deployment_class: dogfood_workspace
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))

    assert deployment_requires_nomos_runtime("demo") is False

    (deployment / "deployment.yaml").write_text(
        """
name: demo
schema_dir: schemas
source_registry: source_registry.yaml
nomos:
  runtime_enforcement: enforce
  deployment_posture:
    deployment_class: dogfood_workspace
""",
        encoding="utf-8",
    )
    assert deployment_requires_nomos_runtime("demo") is True

    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "0")
    assert deployment_requires_nomos_runtime("demo") is False
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    assert deployment_requires_nomos_runtime("demo") is True


def test_protected_repository_deployments_enable_runtime_enforcement(
    monkeypatch,
) -> None:
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)

    assert deployment_requires_nomos_runtime("okg-workspace") is True
    assert deployment_requires_nomos_runtime("okg-ci") is True


def test_every_required_boundary_records_a_nomos_decision() -> None:
    bundle = _allow_all_policy()

    for surface in REQUIRED_NOMOS_RUNTIME_BOUNDARIES:
        outcome = evaluate_runtime_boundary(
            bundle,
            NomosRuntimeRequest(
                surface=surface,
                subject="subject:operator",
                resource={"deployment": "okg-workspace"},
            ),
            active_policy_fingerprint=bundle.policy_fingerprint,
        )

        mapping = runtime_surface_mapping_for(surface)
        assert outcome.allowed is True
        assert outcome.decision.decision_id.startswith("nomos:decision:")
        assert outcome.decision.policy_fingerprint == bundle.policy_fingerprint
        assert outcome.decision.action == mapping.action
        assert outcome.decision.enforcement_point == mapping.surface
        assert outcome.trace["decision_id"] == outcome.decision.decision_id


@pytest.mark.parametrize(
    ("bundle", "runtime_request", "kwargs", "reason_code"),
    [
        (
            None,
            NomosRuntimeRequest(
                surface="graph.read.mcp",
                subject="subject:operator",
                resource={"deployment": "okg-workspace"},
            ),
            {},
            "nomos.default_deny.no_active_bundle",
        ),
        (
            _allow_all_policy(),
            NomosRuntimeRequest(
                surface="graph.read.mcp",
                subject="",
                resource={"deployment": "okg-workspace"},
            ),
            {},
            "nomos.runtime.missing_identity",
        ),
        (
            _allow_all_policy(),
            NomosRuntimeRequest(
                surface="graph.read.mcp",
                subject="subject:operator",
                resource={"deployment": "okg-workspace"},
            ),
            {"active_state_fresh": False},
            "nomos.runtime.stale_active_state",
        ),
        (
            _allow_all_policy(),
            NomosRuntimeRequest(
                surface="graph.write.unregistered",
                subject="subject:operator",
                resource={"deployment": "okg-workspace"},
            ),
            {},
            "nomos.runtime.unknown_boundary",
        ),
        (
            _allow_all_policy(),
            NomosRuntimeRequest(
                surface="graph.read.mcp",
                subject="subject:operator",
                resource={"deployment": "okg-workspace"},
                context=[],  # type: ignore[arg-type]
            ),
            {},
            "nomos.runtime.malformed_request",
        ),
    ],
)
def test_runtime_boundary_fail_closed_cases(
    bundle,
    runtime_request: NomosRuntimeRequest,
    kwargs: dict,
    reason_code: str,
) -> None:
    outcome = evaluate_runtime_boundary(bundle, runtime_request, **kwargs)

    assert outcome.allowed is False
    assert outcome.decision.effect == "deny"
    assert outcome.decision.reason_code == reason_code
    assert outcome.trace["allowed"] is False


def test_primitive_denial_blocks_bypass_even_when_policy_allows() -> None:
    bundle = _allow_all_policy()

    outcome = evaluate_runtime_boundary(
        bundle,
        NomosRuntimeRequest(
            surface="mcp.record",
            subject="subject:operator",
            resource={"tool": "record", "deployment": "okg-workspace"},
        ),
        primitive_checks=(
            NomosPrimitiveCheck(
                name="mcp.write_authority",
                allowed=False,
                reason_code="nomos.runtime.mcp_write_refused",
                metadata={"admission_mode": "fast_track"},
            ),
        ),
    )

    assert outcome.allowed is False
    assert outcome.decision.effect == "deny"
    assert outcome.decision.reason_code == "nomos.runtime.mcp_write_refused"
    assert outcome.trace["primitive_checks"][0]["name"] == "mcp.write_authority"


def test_runtime_obligations_change_effect_and_payload() -> None:
    bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:runtime-obligations",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "redact_export_like_read",
                "effect": "redact",
                "actions": ["graph.read.export_like"],
                "subjects": ["subject:operator"],
                "priority": 500,
                "reason_code": "nomos.test.redact_export_like",
                "obligations": [{"type": "redaction"}, {"type": "audit"}],
            },
            {
                "rule_id": "declassify_after_review",
                "effect": "declassify",
                "actions": ["declassify"],
                "subjects": ["subject:operator"],
                "priority": 400,
                "reason_code": "nomos.test.declassify",
                "obligations": [{"type": "declassification"}, {"type": "audit"}],
            },
            {
                "rule_id": "quarantine_ingest",
                "effect": "quarantine",
                "actions": ["source.ingest"],
                "subjects": ["subject:operator"],
                "priority": 300,
                "reason_code": "nomos.test.quarantine",
                "obligations": [
                    {"type": "quarantine"},
                    {"type": "source_retention"},
                ],
            },
            {
                "rule_id": "review_admin",
                "effect": "review",
                "actions": ["admin.override"],
                "subjects": ["subject:operator"],
                "priority": 200,
                "reason_code": "nomos.test.review",
                "obligations": [
                    {"type": "review"},
                    {"type": "break_glass_review"},
                ],
            },
            {
                "rule_id": "no_export_restricted",
                "effect": "allow",
                "actions": ["graph.export"],
                "subjects": ["subject:operator"],
                "priority": 100,
                "reason_code": "nomos.test.no_export",
                "obligations": [{"type": "no_export"}, {"type": "audit"}],
            },
        ],
    })

    redacted = evaluate_runtime_boundary(
        bundle,
        NomosRuntimeRequest(
            surface="graph.read.export_like",
            subject="subject:operator",
            resource={"deployment": "okg-workspace"},
        ),
        performed_obligations=(),
        payload={"title": "visible", "text": "restricted body"},
    )
    declassified = evaluate_runtime_boundary(
        bundle,
        NomosRuntimeRequest(
            surface="declassify",
            subject="subject:operator",
            resource={"deployment": "okg-workspace"},
        ),
        performed_obligations=("declassification",),
    )
    quarantined = evaluate_runtime_boundary(
        bundle,
        NomosRuntimeRequest(
            surface="source.ingest",
            subject="subject:operator",
            resource={"deployment": "okg-workspace"},
        ),
        performed_obligations=("quarantine", "source_retention"),
    )
    reviewed = evaluate_runtime_boundary(
        bundle,
        NomosRuntimeRequest(
            surface="admin.override",
            subject="subject:operator",
            resource={"deployment": "okg-workspace"},
        ),
        performed_obligations=("review", "break_glass_review"),
    )
    no_export = evaluate_runtime_boundary(
        bundle,
        NomosRuntimeRequest(
            surface="graph.export",
            subject="subject:operator",
            resource={"deployment": "okg-workspace"},
        ),
        performed_obligations=("no_export",),
    )

    assert redacted.allowed is True
    assert redacted.final_effect == "redact"
    assert redacted.payload == {"title": "visible", "nomos_redacted": True}
    assert redacted.enforced_obligations == ("audit", "redaction")
    assert declassified.allowed is True
    assert declassified.final_effect == "declassify"
    assert quarantined.allowed is False
    assert quarantined.final_effect == "quarantine"
    assert reviewed.allowed is False
    assert reviewed.final_effect == "review"
    assert no_export.allowed is False
    assert no_export.final_effect == "deny"
    assert no_export.decision.effect == "deny"
    assert no_export.decision.reason_code == "nomos.runtime.obligation_blocked"
    assert "no_export" in no_export.enforced_obligations


def test_missing_obligation_attestation_fails_closed() -> None:
    bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:runtime-missing-obligation-proof",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "declassify_after_review",
                "effect": "declassify",
                "actions": ["declassify"],
                "subjects": ["subject:operator"],
                "reason_code": "nomos.test.declassify",
                "obligations": [{"type": "declassification"}],
            },
            {
                "rule_id": "allow_secret_ref_use",
                "effect": "allow",
                "actions": ["secret.use"],
                "subjects": ["subject:operator"],
                "reason_code": "nomos.test.secret_use",
                "obligations": [{"type": "secret_ref_required"}],
            },
        ],
    })

    declassify = evaluate_runtime_boundary(
        bundle,
        NomosRuntimeRequest(
            surface="declassify",
            subject="subject:operator",
            resource={"deployment": "okg-workspace"},
        ),
    )
    secret_use = evaluate_runtime_boundary(
        bundle,
        NomosRuntimeRequest(
            surface="secret.use",
            subject="subject:operator",
            resource={"deployment": "okg-workspace"},
        ),
    )

    assert declassify.allowed is False
    assert declassify.final_effect == "deny"
    assert declassify.decision.effect == "deny"
    assert declassify.decision.reason_code == "nomos.runtime.obligation_missing"
    assert declassify.missing_obligations == ("declassification",)
    assert secret_use.allowed is False
    assert secret_use.decision.effect == "deny"
    assert secret_use.decision.reason_code == "nomos.runtime.obligation_missing"
    assert secret_use.missing_obligations == ("secret_ref_required",)


def test_runtime_obligation_mismatch_fails_closed() -> None:
    bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:runtime-obligation-mismatch",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "redact_without_obligation",
                "effect": "redact",
                "actions": ["graph.export"],
                "subjects": ["subject:operator"],
                "reason_code": "nomos.test.unsafe_redact",
            },
        ],
    })

    outcome = evaluate_runtime_boundary(
        bundle,
        NomosRuntimeRequest(
            surface="graph.export",
            subject="subject:operator",
            resource={"deployment": "okg-workspace"},
        ),
        performed_obligations=("redaction",),
    )

    assert outcome.allowed is False
    assert outcome.decision.effect == "deny"
    assert outcome.decision.reason_code == "nomos.runtime.obligation_mismatch"


def test_unknown_policy_obligation_fails_closed() -> None:
    bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:runtime-unknown-obligation",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_export_with_misspelled_no_export",
                "effect": "allow",
                "actions": ["graph.export"],
                "subjects": ["subject:operator"],
                "reason_code": "nomos.test.unsafe_unknown_obligation",
                "obligations": [{"type": "no_exprot"}],
            },
        ],
    })

    outcome = evaluate_runtime_boundary(
        bundle,
        NomosRuntimeRequest(
            surface="graph.export",
            subject="subject:operator",
            resource={"deployment": "okg-workspace"},
        ),
        performed_obligations=("no_export",),
    )

    assert outcome.allowed is False
    assert outcome.decision.effect == "deny"
    assert outcome.decision.reason_code == "nomos.runtime.obligation_mismatch"


def test_non_json_runtime_context_fails_closed_without_crashing() -> None:
    outcome = evaluate_runtime_boundary(
        _allow_all_policy(),
        NomosRuntimeRequest(
            surface="mcp.tool.call",
            subject="subject:operator",
            resource={"deployment": "okg-workspace", "blob": b"not-json"},
        ),
    )

    serialized = json.dumps(outcome.trace, sort_keys=True)
    assert outcome.allowed is False
    assert outcome.decision.effect == "deny"
    assert outcome.decision.reason_code == "nomos.runtime.malformed_request"
    assert "not-json" not in serialized
    assert "binary" in serialized


def test_non_finite_float_runtime_context_fails_closed() -> None:
    outcome = evaluate_runtime_boundary(
        _allow_all_policy(),
        NomosRuntimeRequest(
            surface="mcp.tool.call",
            subject="subject:operator",
            resource={"deployment": "okg-workspace", "score": float("nan")},
        ),
    )

    serialized = json.dumps(outcome.trace, sort_keys=True)
    assert outcome.allowed is False
    assert outcome.decision.effect == "deny"
    assert outcome.decision.reason_code == "nomos.runtime.malformed_request"
    assert "NaN" not in serialized


def test_runtime_trace_redacts_sensitive_payload_classes() -> None:
    bundle = _allow_all_policy()
    outcome = evaluate_runtime_boundary(
        bundle,
        NomosRuntimeRequest(
            surface="secret.use",
            subject="subject:operator",
            resource={
                "deployment": "okg-workspace",
                "secret_ref": "secret://okg/raw-token",
                "api_key": "sk-live-raw-secret",
                "path": "/Users/example/private/source.txt",
                "patient_note": "patient Alice 1970-01-01 SSN 123-45-6789",
                "params": {"token": "raw-token-value"},
            },
            context={
                "headers": {"Authorization": "Bearer raw-bearer-token"},
                "content": "private local file contents",
            },
        ),
    )

    serialized = json.dumps(outcome.trace, sort_keys=True)
    for forbidden in [
        "secret://okg/raw-token",
        "sk-live-raw-secret",
        "/Users/example/private/source.txt",
        "123-45-6789",
        "raw-token-value",
        "raw-bearer-token",
        "private local file contents",
    ]:
        assert forbidden not in serialized
    for required in [
        "decision_id",
        "policy_bundle_id",
        "policy_version",
        "policy_fingerprint",
        "input_hash",
        "subject",
        "action",
        "resource",
        "classification",
        "context",
        "effect",
        "reason_code",
        "matched_rule_ids",
        "obligations",
        "enforcement_point",
    ]:
        assert required in outcome.trace


# ---------------------------------------------------------------------------
# req.gov.audit-real — tri-state mode helper + audit log-and-allow chokepoint.
# ---------------------------------------------------------------------------


def _deny_all_policy():
    # default_effect deny + a rule that never matches the test subject, so every
    # boundary falls through to the default deny.
    return compile_policy_graph({
        "policy_bundle_id": "nomos:test:runtime-deny-all",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "never_matches",
                "effect": "allow",
                "actions": ["graph.read.public_demo"],
                "subjects": ["subject:nobody"],
                "priority": 1,
                "reason_code": "nomos.test.never_matches",
            },
        ],
    })


def _write_demo_manifest(tmp_path, *, nomos_block: str) -> None:
    deployment = tmp_path / "deployments" / "demo"
    deployment.mkdir(parents=True, exist_ok=True)
    (deployment / "deployment.yaml").write_text(
        "name: demo\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        f"{nomos_block}",
        encoding="utf-8",
    )


def test_deployment_nomos_enforcement_mode_is_tristate(tmp_path, monkeypatch):
    from okg.substrate.nomos import deployment_nomos_enforcement_mode

    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"))

    _write_demo_manifest(tmp_path, nomos_block="nomos:\n  runtime_enforcement: off\n")
    assert deployment_nomos_enforcement_mode("demo") == "off"
    assert deployment_requires_nomos_runtime("demo") is False

    _write_demo_manifest(tmp_path, nomos_block="nomos:\n  runtime_enforcement: audit\n")
    assert deployment_nomos_enforcement_mode("demo") == "audit"
    # audit EVALUATES the boundary (not a skip): the runtime gate is True.
    assert deployment_requires_nomos_runtime("demo") is True

    _write_demo_manifest(
        tmp_path,
        nomos_block="nomos:\n  runtime_enforcement: enforce\n",
    )
    assert deployment_nomos_enforcement_mode("demo") == "enforce"
    assert deployment_requires_nomos_runtime("demo") is True

    # Missing/unresolved declarations are off (not governance-on).
    _write_demo_manifest(tmp_path, nomos_block="")
    assert deployment_nomos_enforcement_mode("demo") == "off"


def test_manifest_nomos_blocks_actions_only_for_enforce_mode(monkeypatch):
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)

    assert manifest_nomos_blocks_actions({
        "nomos": {"runtime_enforcement": "enforce"},
    }) is True
    assert manifest_nomos_blocks_actions({
        "nomos": {"runtime_enforcement": "audit"},
    }) is False
    assert manifest_nomos_blocks_actions({
        "nomos": {"runtime_enforcement": "off"},
    }) is False


def test_manifest_audit_readiness_exception_is_explicit(monkeypatch):
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    manifest = {
        "nomos": {
            "runtime_enforcement": "audit",
            "rollout": {
                "audit_readiness_exception": {
                    "allowed": True,
                    "readiness_profiles": ["ci"],
                    "reason": "temporary rollout",
                    "owner": {"name": "security"},
                    "expires_at": "2999-01-01T00:00:00Z",
                },
            },
        },
    }

    exception = manifest_nomos_audit_readiness_exception(
        manifest,
        readiness_profile="ci",
    )

    assert exception == {
        "allowed": True,
        "readiness_profile": "ci",
        "readiness_profiles": ["ci"],
        "reason": "temporary rollout",
        "owner": {"name": "security"},
        "expires_at": "2999-01-01T00:00:00Z",
    }
    assert manifest_nomos_audit_readiness_exception(
        manifest,
        readiness_profile="dogfood",
    ) is None
    assert manifest_nomos_audit_readiness_exception(
        {
            "nomos": {
                "runtime_enforcement": "audit",
                "rollout": {
                    "audit_readiness_exception": {
                        "allowed": True,
                        "readiness_profiles": "ci",
                        "reason": "temporary rollout",
                        "owner": "security",
                        "expires_at": "2999-01-01T00:00:00Z",
                    },
                },
            },
        },
        readiness_profile="ci",
    ) is None
    for invalid_exception in (
        {
            "allowed": True,
            "readiness_profiles": ["ci"],
            "owner": "security",
            "expires_at": "2999-01-01T00:00:00Z",
        },
        {
            "allowed": True,
            "readiness_profiles": ["ci"],
            "reason": "temporary rollout",
            "expires_at": "2999-01-01T00:00:00Z",
        },
        {
            "allowed": True,
            "readiness_profiles": ["ci"],
            "reason": "temporary rollout",
            "owner": "security",
            "expires_at": "2000-01-01T00:00:00Z",
        },
    ):
        assert manifest_nomos_audit_readiness_exception(
            {
                "nomos": {
                    "runtime_enforcement": "audit",
                    "rollout": {
                        "audit_readiness_exception": invalid_exception,
                    },
                },
            },
            readiness_profile="ci",
        ) is None
    assert manifest_nomos_audit_readiness_exception(
        {
            "nomos": {
                "runtime_enforcement": "enforce",
                "rollout": {
                    "audit_readiness_exception": {
                        "allowed": True,
                        "readiness_profiles": ["ci"],
                        "reason": "temporary rollout",
                        "owner": "security",
                        "expires_at": "2999-01-01T00:00:00Z",
                    },
                },
            },
        },
        readiness_profile="ci",
    ) is None


def test_enforcement_mode_env_override_is_single_source(tmp_path, monkeypatch):
    from okg.substrate.nomos import (
        deployment_nomos_enforcement_mode,
        manifest_nomos_enforcement_mode,
    )

    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"))
    _write_demo_manifest(
        tmp_path,
        nomos_block="nomos:\n  runtime_enforcement: enforce\n",
    )
    manifest = {"nomos": {"runtime_enforcement": "enforce"}}

    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    assert deployment_nomos_enforcement_mode("demo") == "off"
    assert manifest_nomos_enforcement_mode(manifest) == "off"
    assert manifest_nomos_enforcement_mode_source(manifest) == "environment"
    assert deployment_requires_nomos_runtime("demo") is False

    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "audit")
    assert deployment_nomos_enforcement_mode("demo") == "audit"
    assert manifest_nomos_enforcement_mode(manifest) == "audit"
    assert manifest_nomos_enforcement_mode_source(manifest) == "environment"

    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")
    assert deployment_nomos_enforcement_mode("demo") == "enforce"
    assert manifest_nomos_enforcement_mode(manifest) == "enforce"
    assert manifest_nomos_enforcement_mode_source(manifest) == "environment"


def test_duplicate_manifest_gates_collapse_to_single_source(monkeypatch):
    # The two drifted gate copies now delegate to the single tri-state source,
    # so they honor the env override exactly like the deployment-name gate.
    from okg.substrate.cli.deployment import (
        _manifest_requires_nomos_runtime as cli_gate,
    )
    from okg.substrate.deployment_upgrade import (
        _manifest_requires_nomos_runtime as upgrade_gate,
    )

    audit_manifest = {"nomos": {"runtime_enforcement": "audit"}}
    off_manifest = {"nomos": {"runtime_enforcement": "off"}}

    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    # audit now requires the runtime boundary (True), not byte-identical to off.
    assert cli_gate(audit_manifest) is True
    assert upgrade_gate(audit_manifest) is True
    assert cli_gate(off_manifest) is False
    assert upgrade_gate(off_manifest) is False

    # env override wins for BOTH copies (the drift bug is fixed).
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    assert cli_gate(audit_manifest) is False
    assert upgrade_gate(audit_manifest) is False
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")
    assert cli_gate(off_manifest) is True
    assert upgrade_gate(off_manifest) is True


@pytest.mark.parametrize(
    ("surface", "performed", "payload"),
    [
        ("graph.read.mcp", ("audit",), None),
        ("source.ingest", ("audit",), None),
        ("admin.override", ("audit",), None),
    ],
)
def test_enforce_mode_is_byte_identical_to_prior_behavior(
    surface, performed, payload,
):
    # An enforce-mode evaluation must be IDENTICAL to the default (prior)
    # behavior on deny/review/allow inputs: explicit enforce == default.
    bundle = _deny_all_policy()
    request = NomosRuntimeRequest(
        surface=surface,
        subject="subject:operator",
        resource={"deployment": "okg-workspace"},
    )
    default = evaluate_runtime_boundary(
        bundle, request, performed_obligations=performed, payload=payload,
    )
    enforce = evaluate_runtime_boundary(
        bundle,
        request,
        performed_obligations=performed,
        payload=payload,
        enforcement_mode="enforce",
    )
    assert default.allowed == enforce.allowed
    assert default.final_effect == enforce.final_effect
    assert default.decision.effect == enforce.decision.effect
    assert default.decision.reason_code == enforce.decision.reason_code
    assert default.decision.decision_id == enforce.decision.decision_id
    assert default.trace == enforce.trace
    # Deny-all policy denies the boundary; enforce blocks it.
    assert enforce.allowed is False
    assert enforce.decision.effect == "deny"


def test_allow_input_is_identical_in_enforce_and_audit():
    # An allowed boundary is unaffected by enforcement_mode: audit only changes
    # NON-allow outcomes.
    bundle = _allow_all_policy()
    request = NomosRuntimeRequest(
        surface="graph.read.mcp",
        subject="subject:operator",
        resource={"deployment": "okg-workspace"},
    )
    enforce = evaluate_runtime_boundary(
        bundle, request, active_policy_fingerprint=bundle.policy_fingerprint,
        enforcement_mode="enforce",
    )
    audit = evaluate_runtime_boundary(
        bundle, request, active_policy_fingerprint=bundle.policy_fingerprint,
        enforcement_mode="audit",
    )
    assert enforce.allowed is True
    assert audit.allowed is True
    assert "audit_observed" not in audit.trace
    assert enforce.decision.effect == audit.decision.effect == "allow"


def test_audit_mode_records_but_allows_a_denied_boundary():
    # The core req.gov.audit-real contract: audit performs and records the real
    # deny decision (the audit trail) but the EFFECT reads as allowed so the
    # caller does not block (log-and-allow).
    bundle = _deny_all_policy()
    request = NomosRuntimeRequest(
        surface="graph.read.mcp",
        subject="subject:operator",
        resource={"deployment": "okg-workspace"},
    )
    strict = evaluate_runtime_boundary(
        bundle, request, enforcement_mode="enforce",
    )
    audit = evaluate_runtime_boundary(
        bundle, request, enforcement_mode="audit",
    )

    # Strict (enforce) blocks.
    assert strict.allowed is False
    assert strict.decision.effect == "deny"

    # Audit ALLOWS the effect...
    assert audit.allowed is True
    assert audit.trace["allowed"] is True
    # ...but RECORDS the observed strict deny (not a skip).
    assert audit.decision.effect == "deny"
    assert audit.final_effect == strict.final_effect
    observed = audit.trace["audit_observed"]
    assert observed["enforcement_mode"] == "audit"
    assert observed["strict_allowed"] is False
    assert observed["decision_effect"] == "deny"
    assert observed["decision_id"] == strict.decision.decision_id


def test_audit_mode_records_review_and_quarantine_but_allows():
    bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:runtime-review-quarantine",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "review_override",
                "effect": "review",
                "actions": ["admin.override"],
                "subjects": ["subject:operator"],
                "priority": 200,
                "reason_code": "nomos.test.review",
                "obligations": [{"type": "review"}, {"type": "break_glass_review"}],
            },
        ],
    })
    request = NomosRuntimeRequest(
        surface="admin.override",
        subject="subject:operator",
        resource={"deployment": "okg-workspace"},
    )
    strict = evaluate_runtime_boundary(
        bundle, request,
        performed_obligations=("review", "break_glass_review"),
        enforcement_mode="enforce",
    )
    audit = evaluate_runtime_boundary(
        bundle, request,
        performed_obligations=("review", "break_glass_review"),
        enforcement_mode="audit",
    )
    assert strict.allowed is False
    assert strict.final_effect == "review"
    assert audit.allowed is True
    assert audit.decision.effect == "review"
    assert audit.trace["audit_observed"]["strict_final_effect"] == "review"


@pytest.mark.parametrize(
    ("policy", "mode", "allowed", "call_effect", "final_effect"),
    [
        ("allow", "enforce", True, "allow", "allow"),
        ("allow", "audit", True, "allow", "allow"),
        ("deny", "enforce", False, "deny", "deny"),
        ("deny", "audit", True, "audit", "deny"),
    ],
)
def test_call_effect_names_what_happened_to_the_call(
    policy, mode, allowed, call_effect, final_effect,
):
    # Issue #2279: `final_effect` keeps the policy's verdict, so under audit it
    # stays `deny` while the call goes ahead. `call_effect` is what a read
    # envelope reports: `audit` for a call audit mode let through, never `deny`
    # for a call that was allowed.
    bundle = _allow_all_policy() if policy == "allow" else _deny_all_policy()
    outcome = evaluate_runtime_boundary(
        bundle,
        NomosRuntimeRequest(
            surface="graph.read.cli_search",
            subject="subject:operator",
            resource={"deployment": "okg-workspace"},
        ),
        active_policy_fingerprint=bundle.policy_fingerprint,
        performed_obligations=("audit",),
        enforcement_mode=mode,
    )
    assert outcome.allowed is allowed
    assert outcome.final_effect == final_effect
    assert outcome.call_effect == call_effect
    assert outcome.audit_log_and_allow is (call_effect == "audit")


def test_call_effect_names_audit_for_review_let_through():
    bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:runtime-review-2279",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "review_override",
                "effect": "review",
                "actions": ["admin.override"],
                "subjects": ["subject:operator"],
                "priority": 200,
                "reason_code": "nomos.test.review",
                "obligations": [{"type": "review"}, {"type": "break_glass_review"}],
            },
        ],
    })
    request = NomosRuntimeRequest(
        surface="admin.override",
        subject="subject:operator",
        resource={"deployment": "okg-workspace"},
    )
    strict = evaluate_runtime_boundary(
        bundle, request,
        performed_obligations=("review", "break_glass_review"),
        enforcement_mode="enforce",
    )
    audit = evaluate_runtime_boundary(
        bundle, request,
        performed_obligations=("review", "break_glass_review"),
        enforcement_mode="audit",
    )
    assert strict.call_effect == "review"
    assert audit.final_effect == "review"
    assert audit.call_effect == "audit"


# One row per prefix, branch and class member the trace sanitizer uses to
# recognise a credential by value alone (#2780): the authorization-scheme
# prefixes, the token prefixes, and each keyword and separator of its
# `name: value` pattern. The key `note` names nothing secret, so only the
# value can trigger redaction. Each token is assembled at runtime so the
# source holds no credential-shaped literal.
_TRACE_TOKEN_SHAPES = [
    ("bearer-scheme", "Bearer " + "fake0sentinel0notakey"),
    ("basic-scheme", "Basic " + "ZmFrZTpzZW50aW5lbA"),
    ("openai-style", "sk" + "-" + "fake0sentinel0notakey"),
    ("github-classic", "gh" + "p_" + "FAKE0sentinel0NOTAKEY"),
    ("github-fine-grained", "github" + "_pat_" + "FAKE0sentinel0NOTAKEY"),
    ("slack-bot", "xo" + "xb-" + "FAKE0sentinel0NOTAKEY"),
    ("aws-access-key", "AK" + "IA" + "FAKE0SENTINEL0123"),
    ("token-colon", "token" + ": fake-value"),
    ("secret-equals", "secret" + "=fake-value"),
    ("password-equals", "password" + "=fake-value"),
    ("api-underscore-key", "api_key" + "=fake-value"),
    ("api-hyphen-key", "api-key" + "=fake-value"),
]


@pytest.mark.parametrize(
    "value",
    [value for _, value in _TRACE_TOKEN_SHAPES],
    ids=[name for name, _ in _TRACE_TOKEN_SHAPES],
)
def test_trace_sanitizer_redacts_each_token_prefix(value: str) -> None:
    safe = sanitize_trace_value({"note": value})

    # Checked first: when the value is NOT redacted, `safe["note"]` is the
    # plain string and indexing it would fail with a less readable TypeError.
    assert value not in json.dumps(safe, sort_keys=True)
    assert safe["note"]["redacted"] == "secret"
    assert sanitize_trace_value({"note": "plain words"}) == {"note": "plain words"}
