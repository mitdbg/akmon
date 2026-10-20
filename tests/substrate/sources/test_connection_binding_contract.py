"""Contract tests for one reusable provider connection and its bindings.

The public connection surface under test is intentionally explicit.  The
expected imports are ``ProviderConnectionDefinition``,
``SourceBindingDefinition``, ``GovernedToolBindingDefinition``,
``LockedConnectionContract``, ``ConnectionOperation``,
``ConnectionContractError``, ``connection_contract_digest``,
``connection_capability_digest``, ``issue_connection_readiness_receipt``, and
``execute_connection_operation``.  These are the Wave 3 model/read-side
contracts; this test must not reach through a private provider implementation.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import ValidationError

from okg.substrate.contracts.deployment_product import (
    AuthoredDeploymentDefinition,
    ConnectionContractError,
    ConnectionOperation,
    DistributionRelease,
    GovernedToolBindingDefinition,
    LockedConnectionContract,
    ProviderConnectionDefinition,
    SourceBindingDefinition,
    canonical_lock_bytes,
    connection_capability_digest,
    connection_contract_digest,
    execute_connection_operation,
    issue_connection_readiness_receipt,
    make_action_plan,
    resolve_product_lock,
    validate_selection,
    verify_plan_before_apply,
    verify_connection_readiness_receipt,
)


FIXTURE = Path(__file__).parents[2] / "fixtures" / "deployment_products" / "shared_connection" / "product.yaml"
_NOW = datetime(2026, 8, 18, 12, 0, tzinfo=timezone.utc)


class _ProviderEffects:
    """Provider and local mutation counters used by the read-only contract."""

    def __init__(self) -> None:
        self.provider_observe = 0
        self.provider_write = 0
        self.registry = 0
        self.graph = 0
        self.generation = 0
        self.lock = 0


class _InstrumentedProvider:
    def __init__(self, effects: _ProviderEffects) -> None:
        self.effects = effects

    def observe(self, operation: ConnectionOperation, binding: Any) -> dict[str, str]:
        self.effects.provider_observe += 1
        return {"operation": str(operation), "binding": binding.binding_id}

    def write(self, operation: ConnectionOperation, binding: Any) -> None:
        self.effects.provider_write += 1
        raise AssertionError("provider write should have been refused before provider execution")


def _raw() -> dict[str, Any]:
    return yaml.safe_load(FIXTURE.read_text())


def _models() -> tuple[ProviderConnectionDefinition, tuple[SourceBindingDefinition, ...], GovernedToolBindingDefinition]:
    raw = _raw()
    connection = ProviderConnectionDefinition.model_validate(raw["connection"])
    sources = tuple(SourceBindingDefinition.model_validate(item) for item in raw["source_bindings"])
    tool = GovernedToolBindingDefinition.model_validate(raw["tool_binding"])
    return connection, sources, tool


def _definition(
    connection: ProviderConnectionDefinition,
    sources: tuple[SourceBindingDefinition, ...],
    tool: GovernedToolBindingDefinition,
) -> AuthoredDeploymentDefinition:
    """Build the aggregate admission object without any runtime material."""

    return AuthoredDeploymentDefinition.model_validate(
        {
            "definition_id": "shared-connection-product",
            "distribution_release": {"distribution_id": "fixture", "version": "1"},
            "provider_connections": [connection.model_dump(mode="python")],
            "source_bindings": [item.model_dump(mode="python") for item in sources],
            "tool_bindings": [tool.model_dump(mode="python")],
        }
    )


def _locked_contract(connection: ProviderConnectionDefinition) -> LockedConnectionContract:
    return LockedConnectionContract(
        connection_ref=connection.reference,
        contract_digest=connection_contract_digest(connection),
        capability_digest=connection_capability_digest(connection),
    )


def _resolved_lock(connection: ProviderConnectionDefinition) -> Any:
    """Resolve the real product lock, including its connection contract."""

    _, sources, tool = _models()
    definition = _definition(connection, sources, tool)
    release = DistributionRelease(
        distribution_id="fixture",
        version="1",
        origin="embedded",
        release_digest="sha256:" + "a" * 64,
    )
    selection = validate_selection(definition, release, ())
    return resolve_product_lock(selection)


def _action_plan(connection: ProviderConnectionDefinition) -> Any:
    return make_action_plan(
        _resolved_lock(connection),
        expires_at=_NOW + timedelta(minutes=5),
        artifact_digests={},
    )


def _execute(
    connection: ProviderConnectionDefinition,
    binding: SourceBindingDefinition | GovernedToolBindingDefinition,
    operation: ConnectionOperation,
    effects: _ProviderEffects,
) -> Any:
    provider = _InstrumentedProvider(effects)

    def provider_call() -> Any:
        if operation.effect == "write":
            return provider.write(operation, binding)
        return provider.observe(operation, binding)

    return execute_connection_operation(
        connection=connection,
        binding=binding,
        operation=operation,
        provider_call=provider_call,
    )


def _operation(phase: str, *, effect: str = "observe") -> ConnectionOperation:
    return ConnectionOperation(
        operation_id=f"fixture-{phase}",
        phase=phase,
        effect=effect,
    )


def _readiness_receipt(
    connection: ProviderConnectionDefinition,
    *,
    issued_at: datetime,
):
    binding = _models()[1][0]
    return issue_connection_readiness_receipt(
        connection,
        binding,
        _operation("preflight"),
        lambda: True,
        issued_at=issued_at,
        expires_at=issued_at + timedelta(minutes=5),
    )


def test_fixture_has_one_typed_connection_two_sources_and_one_governed_tool() -> None:
    connection, sources, tool = _models()
    definition = _definition(connection, sources, tool)

    assert len(definition.provider_connections) == 1
    assert len(definition.source_bindings) == 2
    assert len(definition.tool_bindings) == 1
    assert connection.reference.kind == "provider-connection"
    assert {item.connection_ref for item in (*sources, tool)} == {connection.reference}
    assert {item.binding_id for item in (*sources, tool)} == {
        "source-mail",
        "source-calendar",
        "search-tool",
    }
    assert {item.source_ref.kind for item in sources} == {"source"}
    assert tool.tool_ref.kind == "tool"


def test_bindings_have_independent_disable_config_and_refresh_identity() -> None:
    connection, sources, tool = _models()
    disabled = sources[0].model_copy(
        update={
            "enabled": False,
            "config": {"collection": "mail", "mailbox": "archive"},
            "refresh_identity": "source-mail-refresh-2",
        }
    )
    definition = _definition(connection, (disabled, sources[1]), tool)

    assert definition.provider_connections[0].reference == connection.reference
    assert definition.source_bindings[0].enabled is False
    assert definition.source_bindings[0].config != sources[0].config
    assert definition.source_bindings[0].refresh_identity != sources[0].refresh_identity
    assert definition.source_bindings[1] == sources[1]
    assert definition.tool_bindings[0] == tool
    assert len({item.refresh_identity for item in (*definition.source_bindings, *definition.tool_bindings)}) == 3

    effects = _ProviderEffects()
    with pytest.raises(ConnectionContractError):
        _execute(connection, disabled, _operation("metadata-plan"), effects)
    _execute(connection, sources[1], _operation("metadata-plan"), effects)
    assert effects.provider_observe == 1
    assert effects.provider_write == 0


def test_same_locator_material_rotation_keeps_identity_and_lock_bytes_but_renews_receipt() -> None:
    connection, _, _ = _models()
    contract = _locked_contract(connection)
    lock = _resolved_lock(connection)
    lock_bytes = canonical_lock_bytes(lock)
    locator = connection.secret_reference.ref
    secret_material_v1 = b"fixture-secret-v1"
    secret_material_v2 = b"fixture-secret-v2"
    assert locator == "fixture-provider-token"
    assert secret_material_v1 != secret_material_v2

    receipt_v1 = _readiness_receipt(connection, issued_at=_NOW)
    receipt_v2 = _readiness_receipt(
        connection,
        issued_at=_NOW + timedelta(seconds=1),
    )

    assert connection.secret_reference == connection.model_copy().secret_reference
    assert receipt_v1.connection_ref == receipt_v2.connection_ref == connection.reference
    assert receipt_v1.contract_digest == receipt_v2.contract_digest == contract.contract_digest
    assert receipt_v1.capability_digest == receipt_v2.capability_digest == contract.capability_digest
    assert receipt_v1.status == receipt_v2.status == "ready"
    assert receipt_v1.receipt_id != receipt_v2.receipt_id
    assert receipt_v1.issued_at != receipt_v2.issued_at
    assert receipt_v1.expires_at > receipt_v1.issued_at
    assert receipt_v2.expires_at > receipt_v2.issued_at
    assert connection.secret_reference.ref == locator
    rotated_lock = _resolved_lock(connection)
    assert rotated_lock.product_identity == lock.product_identity
    assert canonical_lock_bytes(rotated_lock) == lock_bytes
    plan = _action_plan(connection)
    definition = _definition(connection, *_models()[1:])
    verification = verify_plan_before_apply(
        plan,
        definition=definition,
        artifacts={},
        now=_NOW,
        expected_plan_digest=plan.plan_digest,
    )
    assert verification.ok
    assert verification.mutation_allowed
    for forbidden in (b"fixture-secret-v1", b"fixture-secret-v2", b"secret_version", b"readiness"):
        assert forbidden not in lock_bytes
    assert receipt_v1.model_dump(mode="json") != receipt_v2.model_dump(mode="json")


@pytest.mark.parametrize(
    "field, value",
    [
        ("endpoint", "https://other-provider.example.test/api"),
        ("auth_strategy", "mtls"),
        ("tls", {"mode": "pinned", "server_name": "other-provider.example.test"}),
        ("proxy", {"mode": "https", "endpoint": "https://proxy.example.test"}),
        ("capabilities", ("provider.metadata.read", "provider.source.read", "provider.admin.write")),
    ],
)
def test_non_secret_contract_drift_changes_digest_and_refuses_old_readiness_plan(
    field: str,
    value: Any,
) -> None:
    connection, _, _ = _models()
    contract = _locked_contract(connection)
    drifted = connection.model_copy(update={field: value})
    assert connection_contract_digest(drifted) != contract.contract_digest or connection_capability_digest(
        drifted
    ) != contract.capability_digest

    receipt = _readiness_receipt(connection, issued_at=_NOW)
    with pytest.raises(ConnectionContractError):
        verify_connection_readiness_receipt(drifted, receipt, now=_NOW)
    plan = _action_plan(connection)
    drifted_definition = _definition(drifted, *_models()[1:])
    verification = verify_plan_before_apply(
        plan,
        definition=drifted_definition,
        artifacts={},
        now=_NOW,
        expected_plan_digest=plan.plan_digest,
    )
    assert not verification.ok
    assert not verification.mutation_allowed
    assert "definition_digest_mismatch" in {item.code for item in verification.diagnostics}


def test_secret_locator_drift_is_not_material_rotation_and_refuses_old_plan() -> None:
    connection, _, _ = _models()
    contract = _locked_contract(connection)
    drifted = connection.model_copy(
        update={
            "secret_reference": connection.secret_reference.model_copy(
                update={"ref": "fixture-another-provider-token"}
            )
        }
    )
    assert connection_contract_digest(drifted) != contract.contract_digest
    receipt = _readiness_receipt(connection, issued_at=_NOW)
    with pytest.raises(ConnectionContractError):
        verify_connection_readiness_receipt(drifted, receipt, now=_NOW)
    plan = _action_plan(connection)
    drifted_definition = _definition(drifted, *_models()[1:])
    verification = verify_plan_before_apply(
        plan,
        definition=drifted_definition,
        artifacts={},
        now=_NOW,
        expected_plan_digest=plan.plan_digest,
    )
    assert not verification.ok
    assert not verification.mutation_allowed
    assert "definition_digest_mismatch" in {item.code for item in verification.diagnostics}


def test_metadata_discovery_setup_preflight_and_health_are_observational() -> None:
    connection, sources, _ = _models()
    effects = _ProviderEffects()
    for phase in ("metadata-plan", "discovery", "setup-proposal", "preflight", "health"):
        _execute(connection, sources[0], _operation(phase), effects)

    assert effects.provider_observe == 5
    assert effects.provider_write == 0
    assert effects.registry == 0
    assert effects.graph == 0
    assert effects.generation == 0
    assert effects.lock == 0


def test_write_attempt_has_stable_refusal_and_no_mutation_counters() -> None:
    connection, sources, _ = _models()
    effects = _ProviderEffects()
    errors: list[str] = []
    for _ in range(2):
        with pytest.raises(ConnectionContractError) as caught:
            _execute(connection, sources[0], _operation("health", effect="write"), effects)
        errors.append(str(caught.value))

    assert errors[0] == errors[1]
    # The diagnostic code, rather than prose, is the stable refusal contract.
    assert caught.value.code == "provider_write_effect_refused"
    assert effects.provider_write == 0
    assert effects.registry == 0
    assert effects.graph == 0
    assert effects.generation == 0
    assert effects.lock == 0


def test_readiness_receipt_requires_a_governed_observational_provider_check() -> None:
    connection, sources, _ = _models()
    calls = 0

    def provider_check() -> bool:
        nonlocal calls
        calls += 1
        return True

    with pytest.raises(ConnectionContractError) as caught:
        issue_connection_readiness_receipt(
            connection,
            sources[0],
            _operation("health", effect="write"),
            provider_check,
            issued_at=_NOW,
            expires_at=_NOW + timedelta(minutes=5),
        )
    assert caught.value.code == "connection_readiness_operation_invalid"
    assert calls == 0


    not_ready = issue_connection_readiness_receipt(
        connection,
        sources[0],
        _operation("health"),
        lambda: False,
        issued_at=_NOW,
        expires_at=_NOW + timedelta(minutes=5),
    )
    assert not_ready.status == "not-ready"
    with pytest.raises(ConnectionContractError) as caught:
        verify_connection_readiness_receipt(connection, not_ready, now=_NOW)
    assert caught.value.code == "connection_not_ready"

    apply_calls = 0

    def apply_call() -> str:
        nonlocal apply_calls
        apply_calls += 1
        return "applied"

    with pytest.raises(ConnectionContractError) as caught:
        execute_connection_operation(
            connection,
            sources[0],
            _operation("apply", effect="write"),
            apply_call,
        )
    assert caught.value.code == "connection_readiness_required"
    assert apply_calls == 0

    ready = _readiness_receipt(connection, issued_at=_NOW)
    assert execute_connection_operation(
        connection,
        sources[0],
        _operation("apply", effect="write"),
        apply_call,
        readiness_receipt=ready,
        now=_NOW,
    ) == "applied"
    assert apply_calls == 1


def test_future_issued_readiness_receipt_is_refused() -> None:
    connection, _, _ = _models()
    receipt = _readiness_receipt(connection, issued_at=_NOW + timedelta(minutes=1))

    with pytest.raises(ConnectionContractError) as caught:
        verify_connection_readiness_receipt(connection, receipt, now=_NOW)
    assert caught.value.code == "connection_receipt_from_future"


def test_undeclared_capability_refuses_before_provider_or_local_mutation() -> None:
    connection, sources, _ = _models()
    undeclared = sources[0].model_copy(
        update={"required_capabilities": ("provider.admin.write",)}
    )
    effects = _ProviderEffects()
    with pytest.raises(ConnectionContractError) as caught:
        _execute(connection, undeclared, _operation("metadata-plan"), effects)

    assert caught.value.code == "connection_capability_undeclared"
    assert effects.provider_observe == 0
    assert effects.provider_write == 0
    assert effects.registry == 0
    assert effects.graph == 0
    assert effects.generation == 0
    assert effects.lock == 0


def test_duplicate_binding_ids_are_rejected_before_aggregate_admission() -> None:
    connection, sources, tool = _models()
    duplicate = sources[1].model_copy(update={"binding_id": sources[0].binding_id})
    with pytest.raises((ValidationError, ValueError)):
        _definition(connection, (sources[0], duplicate), tool)

    duplicate_refresh = sources[1].model_copy(
        update={"refresh_identity": sources[0].refresh_identity}
    )
    with pytest.raises((ValidationError, ValueError)):
        _definition(connection, (sources[0], duplicate_refresh), tool)


def test_missing_or_wrong_kind_connection_refs_are_rejected() -> None:
    connection, sources, tool = _models()
    missing = sources[0].model_dump(mode="python")
    missing.pop("connection_ref")
    with pytest.raises((ValidationError, ValueError)):
        SourceBindingDefinition.model_validate(missing)

    wrong_kind = tool.model_dump(mode="python")
    wrong_kind["connection_ref"] = {"owner": "fixture-team", "id": "not-a-connection", "kind": "tool"}
    with pytest.raises((ValidationError, ValueError)):
        GovernedToolBindingDefinition.model_validate(wrong_kind)

    wrong_source = sources[0].model_dump(mode="python")
    wrong_source["source_ref"] = {"owner": "fixture-team", "id": "mail", "kind": "tool"}
    with pytest.raises((ValidationError, ValueError)):
        SourceBindingDefinition.model_validate(wrong_source)

    # Keep the valid aggregate exercised in this negative test as well: one bad
    # reference must not be silently dropped or replaced with a default.
    assert _definition(connection, sources, tool).source_bindings[0].connection_ref == connection.reference


@pytest.mark.parametrize("smuggled", [{"secret_value": "literal"}, {"secret_version": "v2"}])
def test_literal_secret_or_secret_version_smuggling_is_rejected(smuggled: dict[str, str]) -> None:
    raw = _raw()
    raw["connection"]["secret_reference"].update(smuggled)
    with pytest.raises((ValidationError, ValueError)):
        ProviderConnectionDefinition.model_validate(raw["connection"])
