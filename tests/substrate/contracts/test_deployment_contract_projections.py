from __future__ import annotations

import builtins
from datetime import datetime, timezone
import json
from pathlib import Path

import jsonschema
import pytest
import yaml

from okg.substrate.contracts.deployment_product import (
    AuthoredDeploymentDefinition,
    DistributionRelease,
    LockDiagnostic,
    PlanAction,
    ProductBundle,
    make_action_plan,
    resolve_product_lock,
    sha256_digest,
    validate_selection,
)
from okg.substrate.contracts.deployment_product import projections
from okg.substrate.contracts.deployment_product.projections import (
    AGENT_SCHEMA_MODELS,
    PRODUCT_SCHEMA_MODELS,
    AgentAction,
    AgentDiagnostic,
    AgentOperationEnvelope,
    AgentQuestionEnvelope,
    AgentStatusEnvelope,
    ProjectionDriftError,
    assert_projection_conformance,
    project_agent_diagnostic,
    project_agent_plan,
    project_setup,
    projection_registry,
    render_human_plan,
    render_human_questions,
    render_human_status,
    schema_catalog,
    setup_questions,
)


FIXTURES = Path(__file__).parents[2] / "fixtures" / "deployment_products"
CONFORMANCE = FIXTURES / "projections" / "conformance.json"
PRODUCT = FIXTURES / "lock_determinism" / "product.yaml"


def _plan():
    raw = yaml.safe_load(PRODUCT.read_text())
    release = DistributionRelease.model_validate(raw["distribution_release"])
    bundles = tuple(
        ProductBundle.model_validate(item) for item in raw["product_bundles"]
    )
    definition = AuthoredDeploymentDefinition.model_validate(raw["definition"])
    lock = resolve_product_lock(validate_selection(definition, release, bundles))
    artifact = b"projection reference artifact"
    plan = make_action_plan(
        lock,
        expires_at=datetime(2030, 1, 1, tzinfo=timezone.utc),
        artifact_digests={"cache/projection-reference": sha256_digest(artifact)},
        actions=(
            PlanAction(
                action_id="apply-product",
                kind="apply-sealed-release",
                arguments={"artifact": "cache/projection-reference"},
            ),
        ),
    )
    return plan


def test_json_schemas_and_registry_are_generated_from_the_canonical_contract_models() -> (
    None
):
    schemas = schema_catalog()
    assert set(PRODUCT_SCHEMA_MODELS) == {
        "distribution_release",
        "product_bundle",
        "authored_deployment_definition",
        "resolved_lock",
        "sealed_release",
        "applied_instance",
    }
    assert set(schemas) == set(PRODUCT_SCHEMA_MODELS) | set(AGENT_SCHEMA_MODELS)
    for schema in schemas.values():
        jsonschema.Draft202012Validator.check_schema(schema)
        assert schema.get("additionalProperties") is False

    registry = projection_registry()
    assert registry["schema_version"] == "okg.deployment_product.projection_registry/v1"
    assert {item["name"] for item in registry["canonical_objects"]} == set(
        PRODUCT_SCHEMA_MODELS
    )
    assert {item["name"] for item in registry["agent_envelopes"]} == set(
        AGENT_SCHEMA_MODELS
    )
    assert registry["wave2_composition"]["bundle_order_is_precedence"] is False
    assert registry["wave2_composition"]["registered_asset_kinds"] == []


def test_human_and_agent_projections_share_questions_plans_status_and_diagnostics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def refuse_prompt(*_args, **_kwargs):
        raise AssertionError("agent mode attempted an interactive prompt")

    monkeypatch.setattr(builtins, "input", refuse_prompt)
    agent_questions = project_setup(
        operation_id="operation-1",
        deployment_id="acme-prod",
        agent_mode=True,
    )
    assert isinstance(agent_questions, AgentQuestionEnvelope)
    assert agent_questions.mutation_classification == "read_only"
    assert {item.question_id for item in agent_questions.questions} == {
        f"definition.{name}"
        for name in (
            "definition_id",
            "distribution_release_ref",
            "bundles",
            "instance_values",
            "overrides",
            "local_assets",
            "secret_references",
            "connection_references",
            "provider_connections",
            "source_bindings",
            "tool_bindings",
        )
    }
    questions_text = render_human_questions(setup_questions())
    assert "Why:" in questions_text
    assert "Stores: secret_reference" in questions_text
    assert "literal secret" not in questions_text.lower()

    plan = project_agent_plan(
        _plan(),
        operation_id="operation-1",
        deployment_id="acme-prod",
    )
    plan_text = render_human_plan(plan)
    assert plan.plan_digest in plan_text
    assert "Approval: required" in plan_text
    assert plan.actions[0].preconditions == (
        "exact-plan-digest",
        "unexpired-plan",
        "verified-artifacts",
    )

    diagnostic = project_agent_diagnostic(
        LockDiagnostic(
            code="artifact_digest_mismatch",
            path="cache/projection-reference",
            expected="sha256:" + "1" * 64,
            observed="sha256:" + "2" * 64,
            owner="distribution",
            fix_hint="restore the artifact and plan again",
        )
    )
    status = AgentStatusEnvelope(
        operation_id="operation-1",
        deployment_id="acme-prod",
        state="blocked",
        phase="pre_apply",
        progress_percent=0,
        summary="Artifact verification refused before mutation.",
        diagnostics=(diagnostic,),
        next_actions=(
            AgentAction(
                action_id="replan",
                kind="plan",
                arguments={},
                preconditions=("artifact-restored",),
            ),
        ),
        mutation_classification="read_only",
        safe_to_retry=True,
    )
    status_text = render_human_status(status)
    assert diagnostic.code in status_text
    assert diagnostic.fix_hint in status_text

    question_schema = schema_catalog()["agent_questions"]
    jsonschema.validate(
        agent_questions.model_dump(mode="json", by_alias=True),
        question_schema,
    )


def test_mutating_agent_operations_require_exact_plan_digest_and_idempotency() -> None:
    action = AgentAction(
        action_id="apply-product",
        kind="apply",
        arguments={"deployment_id": "acme-prod"},
        preconditions=("approved-plan",),
    )
    with pytest.raises(ValueError, match="exact plan digest"):
        AgentOperationEnvelope(
            operation_id="operation-1",
            deployment_id="acme-prod",
            action=action,
            mutation_classification="mutating",
            safe_to_retry=True,
            idempotency_key="idempotency-1",
        )
    with pytest.raises(ValueError, match="idempotency key"):
        AgentOperationEnvelope(
            operation_id="operation-1",
            deployment_id="acme-prod",
            action=action,
            mutation_classification="mutating",
            safe_to_retry=True,
            plan_digest="sha256:" + "1" * 64,
        )
    operation = AgentOperationEnvelope(
        operation_id="operation-1",
        deployment_id="acme-prod",
        action=action,
        mutation_classification="mutating",
        safe_to_retry=True,
        plan_digest="sha256:" + "1" * 64,
        idempotency_key="idempotency-1",
    )
    jsonschema.validate(
        operation.model_dump(mode="json", by_alias=True),
        schema_catalog()["agent_operation"],
    )


def test_agent_diagnostics_centrally_redact_unstructured_text_and_non_digest_values() -> (
    None
):
    supplied_secret = "SECRET-VALUE"
    direct = AgentDiagnostic(
        code="unregistered_probe",
        path="definition.value",
        owner="resolver",
        message=supplied_secret,
        fix_hint=supplied_secret,
        expected=supplied_secret,
        observed=supplied_secret,
    )
    projected = project_agent_diagnostic(
        LockDiagnostic(
            code="unregistered_probe",
            path="definition.value",
            expected=supplied_secret,
            observed=supplied_secret,
            owner="resolver",
            fix_hint=supplied_secret,
        ),
        message=supplied_secret,
    )
    for diagnostic in (direct, projected):
        encoded = json.dumps(diagnostic.model_dump(mode="json"), sort_keys=True)
        assert supplied_secret not in encoded
        assert diagnostic.expected == "<redacted>"
        assert diagnostic.observed == "<redacted>"
        assert diagnostic.code == "unregistered_diagnostic"
        assert (
            diagnostic.message
            == "deployment validation reported an unregistered diagnostic"
        )

    identity_probe = AgentDiagnostic(
        code=supplied_secret,
        path=supplied_secret,
        owner=supplied_secret,
        phase=supplied_secret,
        message=supplied_secret,
        fix_hint=supplied_secret,
    )
    encoded = json.dumps(identity_probe.model_dump(mode="json"), sort_keys=True)
    assert supplied_secret not in encoded
    assert identity_probe.path.startswith("sha256:")
    assert identity_probe.owner == "external"
    assert identity_probe.phase == "validation"

    with pytest.raises(ValueError):
        AgentAction(
            action_id="unsafe-action",
            kind="apply",
            arguments={"payload": supplied_secret},
        )
    with pytest.raises(ValueError, match="secret-material vocabulary"):
        AgentAction(
            action_id=supplied_secret,
            kind="apply",
            arguments={},
        )
    with pytest.raises(ValueError, match="secret-material vocabulary"):
        AgentAction(
            action_id="unsafe-action",
            kind="apply",
            arguments={"artifact": supplied_secret},
        )
    with pytest.raises(ValueError, match="secret-material vocabulary"):
        AgentOperationEnvelope(
            operation_id=supplied_secret,
            deployment_id="acme-prod",
            action=AgentAction(
                action_id="inspect-product",
                kind="inspect",
                arguments={},
            ),
            mutation_classification="read_only",
            safe_to_retry=True,
        )

    plan = project_agent_plan(
        _plan(),
        operation_id="operation-1",
        deployment_id="acme-prod",
    )
    sanitized = plan.model_copy(update={"summary": supplied_secret})
    assert supplied_secret not in json.dumps(sanitized.model_dump(mode="json"))


def test_projection_fixture_refuses_schema_registry_and_renderer_drift(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    assert_projection_conformance(CONFORMANCE)

    corrupt = json.loads(CONFORMANCE.read_text())
    corrupt["registry"] = "sha256:" + "0" * 64
    corrupt_path = tmp_path / "corrupt-conformance.json"
    corrupt_path.write_text(json.dumps(corrupt))
    with pytest.raises(ProjectionDriftError):
        assert_projection_conformance(corrupt_path)

    with monkeypatch.context() as patch:
        original_catalog = projections.schema_catalog

        def drifted_catalog():
            catalog = original_catalog()
            catalog["resolved_lock"] = {
                **catalog["resolved_lock"],
                "description": "unreviewed schema drift",
            }
            return catalog

        patch.setattr(projections, "schema_catalog", drifted_catalog)
        with pytest.raises(ProjectionDriftError):
            assert_projection_conformance(CONFORMANCE)

    with monkeypatch.context() as patch:
        original_renderer = projections.render_human_questions
        patch.setattr(
            projections,
            "render_human_questions",
            lambda questions: (
                original_renderer(questions) + "\nunreviewed renderer drift"
            ),
        )
        with pytest.raises(ProjectionDriftError):
            assert_projection_conformance(CONFORMANCE)
