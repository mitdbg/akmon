from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest
import yaml

from okg.substrate.contracts.deployment_product import (
    AuthoredDeploymentDefinition,
    DistributionRelease,
    GovernedToolBindingDefinition,
    ProviderConnectionDefinition,
    authored_definition_digest,
    connection_capability_digest,
    connection_contract_digest,
    resolve_product_lock,
    validate_selection,
)
from okg.substrate.external_actions.action_contracts import (
    ActionContractError,
    digest_value,
    resolve_action_contracts,
)


FIXTURE = Path(__file__).parents[2] / "fixtures" / "deployment_products" / "shared_connection" / "product.yaml"


def _authority():
    raw = yaml.safe_load(FIXTURE.read_text(encoding="utf-8"))
    connection = ProviderConnectionDefinition.model_validate(raw["connection"])
    tool = GovernedToolBindingDefinition.model_validate(raw["tool_binding"])
    definition = AuthoredDeploymentDefinition.model_validate(
        {
            "definition_id": "shared-connection-product",
            "distribution_release": {"distribution_id": "fixture", "version": "1"},
            "provider_connections": [connection.model_dump(mode="python")],
            "tool_bindings": [tool.model_dump(mode="python")],
        }
    )
    release = DistributionRelease(
        distribution_id="fixture",
        version="1",
        origin="embedded",
        release_digest="sha256:" + "a" * 64,
    )
    lock = resolve_product_lock(validate_selection(definition, release, ()))
    return definition, lock, connection, tool


def _ref(value) -> str:
    return f"{value.owner}:{value.kind}:{value.ref}"


def _reference_projection(value) -> dict:
    return {
        "owner": value.owner,
        "id": value.ref,
        "kind": value.kind,
        "version": value.version,
        "digest": value.digest,
    }


def _registry() -> tuple[dict, object, object]:
    definition, lock, connection, tool = _authority()
    schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "additionalProperties": False,
        "required": ["query"],
        "properties": {"query": {"type": "string", "maxLength": 80}},
    }
    executor = {
        "journal_protocol": "okg-external-action-worker-journal/v1",
        "provider_replay_mode": "lookup_or_idempotent_retry",
        "worker_protocol": "okg-external-action-worker/v1",
    }
    renderer = {
        "version": 1,
        "method": "POST",
        "operation": "metadata.search",
        "target": "target_ref",
        "body": "canonical_payload_bytes",
    }
    executor_digest = digest_value("okg-external-action-executor-contract-v1", executor)
    renderer_digest = digest_value("okg-external-action-payload-renderer-v1", renderer)
    descriptor = {
        "operation_id": "metadata.search",
        "effect_kind": "write",
        "effect_phase": "execute",
        "required_capabilities": ["provider.metadata.read"],
        "request_media_type": "application/json",
        "executor_contract_digest": executor_digest,
        "payload_renderer_digest": renderer_digest,
        "provider_replay_mode": "lookup_or_idempotent_retry",
    }
    binding_projection = {
        "definition_digest": authored_definition_digest(definition),
        "binding_id": tool.binding_id,
        "connection_ref": _reference_projection(tool.connection_ref),
        "enabled": tool.enabled,
        "config": dict(tool.config),
        "readiness_policy": tool.readiness_policy,
        "refresh_identity": tool.refresh_identity,
        "required_capabilities": list(tool.required_capabilities),
        "binding_kind": tool.binding_kind,
        "tool_ref": _reference_projection(tool.tool_ref),
    }
    action = {
        "action_type": "fixture.metadata.search/v1",
        "action_schema": schema,
        "action_schema_digest": digest_value("okg-external-action-schema-v1", schema),
        "executor_contract": executor,
        "executor_contract_digest": executor_digest,
        "payload_renderer": renderer,
        "payload_renderer_digest": renderer_digest,
        "governed_tool_binding_ref": _ref(tool.tool_ref),
        "governed_tool_binding_digest": digest_value(
            "okg-external-action-governed-tool-binding-v1", binding_projection
        ),
        "provider_connection_ref": _ref(connection.reference),
        "provider_connection_contract_digest": connection_contract_digest(connection),
        "provider_capability_digest": connection_capability_digest(connection),
        "provider_operation_descriptor": descriptor,
        "provider_operation_id": "metadata.search",
        "provider_operation_descriptor_digest": digest_value(
            "okg-external-action-provider-operation-v1", descriptor
        ),
        "provider_replay_mode": "lookup_or_idempotent_retry",
        "target_constraint": {
            "kind": "ascii_prefix_decimal_v1",
            "prefix": "item-",
            "decimal_digits_minimum": 1,
            "decimal_digits_maximum": 20,
            "leading_zeroes": "allowed",
            "full_match": True,
            "normalization": "none",
            "input_encoding": "UTF-8",
            "ascii_only": True,
        },
        "minimum_risk_tier": "moderate",
        "review_and_execution_limits": {
            "maximum_execution_window_seconds": 600,
            "maximum_review_lifetime_seconds": 3600,
            "reporting_attention_seconds": 900,
            "reconcile_not_before_seconds": 3600,
        },
        "diff_field_codes": ["query"],
        "allowed_summary_codes": ["metadata_search"],
        "transition_reason_codes": ["approved_by_operator"],
        "outcome_reason_rules": {
            "provider_completed": {
                "allowed_outcomes": ["succeeded"],
                "provider_receipt": "required",
            }
        },
    }
    return {"schema": "okg.external_action_contracts/v1", "actions": [action]}, definition, lock


def test_registry_resolves_against_definition_lock_and_validates_payload_schema() -> None:
    registry, definition, lock = _registry()
    resolved = resolve_action_contracts(
        registry, authored_definition=definition, resolved_lock=lock
    )

    contract = resolved["fixture.metadata.search/v1"]
    contract.validate_payload({"query": "safe"})
    with pytest.raises(ActionContractError, match="payload does not match"):
        contract.validate_payload({"wrong": True})


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("action_schema", "properties", "query", "maxLength"), 81),
        (("executor_contract", "worker_protocol"), "foreign-worker/v1"),
        (("payload_renderer", "method"), "DELETE"),
        (("provider_operation_descriptor", "effect_kind"), "observe"),
        (("provider_connection_contract_digest",), "sha256:" + "1" * 64),
        (("governed_tool_binding_digest",), "sha256:" + "2" * 64),
    ],
)
def test_semantic_or_digest_drift_refuses_the_whole_registry(path: tuple[str, ...], value) -> None:
    registry, definition, lock = _registry()
    mutated = deepcopy(registry)
    target = mutated["actions"][0]
    for part in path[:-1]:
        target = target[part]
    target[path[-1]] = value

    with pytest.raises(ActionContractError):
        resolve_action_contracts(mutated, authored_definition=definition, resolved_lock=lock)


def test_disabled_binding_and_definition_lock_drift_fail_closed() -> None:
    registry, definition, lock = _registry()
    disabled = definition.model_copy(
        update={
            "tool_bindings": (
                definition.tool_bindings[0].model_copy(update={"enabled": False}),
            )
        }
    )
    with pytest.raises(ActionContractError):
        resolve_action_contracts(registry, authored_definition=disabled, resolved_lock=lock)

    changed = definition.model_copy(update={"instance_values": {"drift": True}})
    with pytest.raises(ActionContractError, match="digest"):
        resolve_action_contracts(registry, authored_definition=changed, resolved_lock=lock)


def test_resolver_never_imports_or_calls_a_provider_execution_surface(monkeypatch) -> None:
    registry, definition, lock = _registry()

    def forbidden(*args, **kwargs):
        raise AssertionError("provider execution is outside OKG")

    monkeypatch.setattr(
        "okg.substrate.contracts.deployment_product.execute_connection_operation",
        forbidden,
    )
    assert resolve_action_contracts(
        registry, authored_definition=definition, resolved_lock=lock
    )
