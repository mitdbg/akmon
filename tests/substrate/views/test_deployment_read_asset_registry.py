from __future__ import annotations

import importlib
import sys
from types import SimpleNamespace

import pytest

from okg.substrate.contracts.deployment_product import (
    TypedAsset,
    TypedAssetReference,
    TypedAssetResolutionError,
    build_read_asset,
    resolve_read_assets,
)
from okg.substrate.views import registry as registry_module
from okg.substrate.publisher.native_view_stage import NativeViewStage
from okg.substrate.views import ViewMaterializer
from okg.substrate.views.base import (
    READ_ASSET_FACTORY_AUTHORITY_ATTRIBUTE,
    ResolvedReadAssetFactoryAuthority,
)
from okg.substrate.views.registry import (
    ViewRegistryEntry,
    adapt_views_registry_entries,
    materializers_from_registry_entries,
    parse_views_registry,
)


READ_KINDS = (
    "view",
    "read-model",
    "lens",
    "dashboard",
    "interface",
    "prompt",
    "skill",
    "playbook",
    "tool",
    "resolver",
    "search",
    "chat-policy",
    "vocabulary",
)


def _contract(
    kind: str,
    *,
    asset_id: str | None = None,
    result_schema: str | None = None,
    output_identity: str | None = None,
    database_object: str | None = None,
    references=(),
):
    stable_id = asset_id or kind
    config_by_kind = {
        "view": {"view_id": stable_id},
        "read-model": {"source_relation": f"demo.{stable_id}"},
        "lens": {"lens_id": stable_id},
        "dashboard": {"dashboard_id": stable_id},
        "interface": {"interface_id": stable_id},
        "prompt": {"template": "Summarize {{input}}"},
        "skill": {"instructions": "Use selected read assets."},
        "playbook": {"steps": ["inspect", "read"]},
        "tool": {"capability_ref": f"capability:{stable_id}"},
        "resolver": {"target_kind": "read-model"},
        "search": {"index_ref": f"index:{stable_id}"},
        "chat-policy": {"policy_ref": f"policy:{stable_id}"},
        "vocabulary": {"terms": [stable_id]},
    }
    return build_read_asset(
        owner="demo",
        asset_id=stable_id,
        kind=kind,
        config_schema=f"okg.deployment_product.asset.{kind}.config/v1",
        result_schema=result_schema or f"okg.deployment_product.asset.{kind}.result/v1",
        capability_schema=f"okg.deployment_product.asset.{kind}.capability/v1",
        output_identity=output_identity or f"demo-output:{stable_id}",
        policy_requirements={
            "audiences": ["demo-reader"],
            "tenants": ["demo-tenant"],
            "visible_fields": ["content"],
            "decision_ids": [f"nomos:demo:{stable_id}"],
        },
        lineage_sources=[f"fixture:{stable_id}"],
        database_object=database_object,
        refresh_policy={
            "mode": "generation" if kind in {"view", "read-model"} else "on-demand"
        },
        config=config_by_kind[kind],
        references=references,
        provenance={"source": "definition", "fixture": "wave4"},
    )


def _codes(exc: TypedAssetResolutionError) -> set[str]:
    return {item.code for item in exc.diagnostics}


def test_all_read_and_agent_kinds_use_canonical_resolution_and_lock_adapter() -> None:
    declarations = [_contract(kind) for kind in READ_KINDS]
    resolution = resolve_read_assets(declarations)

    assert {item.asset.kind for item in resolution.contracts} == set(READ_KINDS)
    assert len({item.qualified_id for item in resolution.contracts}) == len(READ_KINDS)
    assert all(item.config_schema and item.result_schema for item in resolution.contracts)
    assert all(item.refresh_policy.mode in {"generation", "on-demand"} for item in resolution.contracts)
    assert resolution.canonical.assets == resolution.assets
    assert resolution.composition_digest.startswith("sha256:")
    lock_paths = {item.path for item in resolution.lock_provenance()}
    assert {f"assets.demo:{kind}" for kind in READ_KINDS} <= lock_paths


def test_typed_reference_is_resolved_by_wave1_registry() -> None:
    vocabulary = _contract("vocabulary")
    prompt = _contract(
        "prompt",
        references=(TypedAssetReference(
            owner="demo",
            asset_id="vocabulary",
            kind="vocabulary",
            version="v1",
            digest=vocabulary.asset.digest,
        ),),
    )

    resolution = resolve_read_assets([prompt, vocabulary], roots=["demo:prompt"])

    assert [item.qualified_id for item in resolution.canonical.ordered_assets] == [
        "demo:vocabulary",
        "demo:prompt",
    ]


def test_unknown_reference_refuses_before_mutation_observer() -> None:
    called = False
    prompt = _contract(
        "prompt",
        references=(TypedAssetReference(
            owner="demo",
            asset_id="missing-vocabulary",
            kind="vocabulary",
        ),),
    )

    def observe(_resolution) -> None:
        nonlocal called
        called = True

    with pytest.raises(TypedAssetResolutionError) as caught:
        resolve_read_assets([prompt], mutation_observer=observe)

    assert "missing_reference" in _codes(caught.value)
    assert called is False


def test_selected_supporting_read_asset_must_have_a_valid_read_contract() -> None:
    legacy = TypedAsset(
        owner="demo",
        asset_id="legacy",
        kind="view",
        plane="views-read-models",
        version="v1",
        digest="sha256:" + "8" * 64,
        schema="okg.deployment_product.asset.view/v1",
        capabilities=("declarative", "read"),
        merge_identity="legacy",
        merge_rule="refuse",
        provenance={"source": "distribution"},
    )
    prompt = _contract(
        "interface",
        references=(TypedAssetReference(
            owner="demo",
            asset_id="legacy",
            kind="view",
            digest=legacy.digest,
        ),),
    )

    with pytest.raises(TypedAssetResolutionError) as caught:
        resolve_read_assets(
            [prompt],
            supporting_assets=[legacy],
            roots=[prompt.qualified_id],
        )

    assert "read_contract_missing" in _codes(caught.value)


def test_per_kind_contracts_reject_unsafe_config_refresh_reference_and_compatibility() -> None:
    with pytest.raises(ValueError, match="forbidden"):
        build_read_asset(
            owner="demo",
            asset_id="unsafe",
            kind="view",
            config_schema="okg.deployment_product.asset.view.config/v1",
            result_schema="okg.deployment_product.asset.view.result/v1",
            capability_schema="okg.deployment_product.asset.view.capability/v1",
            output_identity="unsafe-output",
            policy_requirements={
                "audiences": ["demo-reader"], "tenants": ["demo-tenant"],
                "visible_fields": ["content"], "decision_ids": ["nomos:demo"],
            },
            lineage_sources=["fixture:unsafe"],
            refresh_policy={"mode": "on-demand"},
            config={"view_id": "unsafe", "dsn": "postgres://user:pass@host/db"},
        )
    with pytest.raises(ValueError, match="does not support"):
        build_read_asset(
            owner="demo",
            asset_id="scheduled-tool",
            kind="tool",
            config_schema="okg.deployment_product.asset.tool.config/v1",
            result_schema="okg.deployment_product.asset.tool.result/v1",
            capability_schema="okg.deployment_product.asset.tool.capability/v1",
            output_identity="scheduled-tool-output",
            policy_requirements={
                "audiences": ["demo-reader"], "tenants": ["demo-tenant"],
                "visible_fields": ["content"], "decision_ids": ["nomos:demo"],
            },
            lineage_sources=["fixture:scheduled-tool"],
            refresh_policy={"mode": "generation"},
            config={"capability_ref": "capability:scheduled-tool"},
        )
    view = _contract("view")
    with pytest.raises(ValueError, match="illegal reference"):
        _contract(
            "vocabulary",
            references=(TypedAssetReference(
                owner="demo",
                asset_id="view",
                kind="view",
                digest=view.asset.digest,
            ),),
        )
    with pytest.raises(ValueError, match="unregistered fields"):
        build_read_asset(
            owner="demo",
            asset_id="compat",
            kind="view",
            config_schema="okg.deployment_product.asset.view.config/v1",
            result_schema="okg.deployment_product.asset.view.result/v1",
            capability_schema="okg.deployment_product.asset.view.capability/v1",
            output_identity="compat-output",
            policy_requirements={
                "audiences": ["demo-reader"], "tenants": ["demo-tenant"],
                "visible_fields": ["content"], "decision_ids": ["nomos:demo"],
            },
            lineage_sources=["fixture:compat"],
            refresh_policy={"mode": "on-demand"},
            config={"view_id": "compat"},
            compatibility={"fallback": "ambient"},
        )
    with pytest.raises(ValueError, match="registered prompt schema"):
        build_read_asset(
            owner="demo",
            asset_id="invented-schema",
            kind="prompt",
            config_schema="demo.prompt.config/v1",
            result_schema="okg.deployment_product.asset.prompt.result/v1",
            capability_schema="okg.deployment_product.asset.prompt.capability/v1",
            output_identity="invented-schema-output",
            policy_requirements={
                "audiences": ["demo-reader"], "tenants": ["demo-tenant"],
                "visible_fields": ["content"], "decision_ids": ["nomos:demo"],
            },
            lineage_sources=["fixture:invented-schema"],
            refresh_policy={"mode": "on-demand"},
            config={"template": "Summarize {{input}}"},
        )
    with pytest.raises(ValueError, match="prompt.template must be a non-empty string"):
        build_read_asset(
            owner="demo",
            asset_id="wrong-config-type",
            kind="prompt",
            config_schema="okg.deployment_product.asset.prompt.config/v1",
            result_schema="okg.deployment_product.asset.prompt.result/v1",
            capability_schema="okg.deployment_product.asset.prompt.capability/v1",
            output_identity="wrong-config-type-output",
            policy_requirements={
                "audiences": ["demo-reader"], "tenants": ["demo-tenant"],
                "visible_fields": ["content"], "decision_ids": ["nomos:demo"],
            },
            lineage_sources=["fixture:wrong-config-type"],
            refresh_policy={"mode": "on-demand"},
            config={"template": 123},
        )


@pytest.mark.parametrize("collision", ["database", "output", "identity"])
def test_cross_kind_collisions_refuse_before_mutation(collision: str) -> None:
    called = False
    first_kwargs = {}
    second_kwargs = {}
    if collision == "database":
        first_kwargs["database_object"] = "product_read.shared"
        second_kwargs["database_object"] = "product_read.shared"
    elif collision == "output":
        first_kwargs.update(
            output_identity="shared-output",
            result_schema="okg.deployment_product.asset.view.result/v1",
        )
        second_kwargs.update(
            output_identity="shared-output",
            result_schema="okg.deployment_product.asset.read-model.result/v1",
        )
    else:
        first_kwargs["asset_id"] = "shared-id"
        second_kwargs["asset_id"] = "shared-id"
    first = _contract("view", **first_kwargs)
    second = _contract("read-model", **second_kwargs)

    def observe(_resolution) -> None:
        nonlocal called
        called = True

    with pytest.raises(TypedAssetResolutionError) as caught:
        resolve_read_assets(
            [first, second],
            mutation_observer=observe,
        )

    assert _codes(caught.value) & {
        "database_object_collision",
        "output_contract_collision",
        "digest_collision",
    }
    assert called is False


def test_runtime_materialization_receipt_cannot_be_selected_or_locked() -> None:
    receipt = TypedAsset(
        owner="demo",
        asset_id="view-receipt-g42",
        kind="materialization-receipt",
        plane="views-read-models",
        version="v1",
        digest="sha256:" + "1" * 64,
        schema="demo.runtime.receipt/v1",
        merge_identity="view-receipt-g42",
        merge_rule="refuse",
        provenance={"source": "local"},
    )

    with pytest.raises(TypedAssetResolutionError) as caught:
        resolve_read_assets([receipt])

    assert _codes(caught.value) == {"runtime_materialization_not_authorable"}


def test_native_executor_refuses_unresolved_or_mismatched_canonical_identity() -> None:
    contract = _contract("view")
    completely_unresolved = ViewMaterializer(
        view_def_factory=lambda: None,
        build_corpus=lambda *_args, **_kwargs: {},
    )
    with pytest.raises(ValueError, match="governed read-asset resolution token"):
        NativeViewStage([completely_unresolved], deployment_name="demo")

    unresolved = ViewMaterializer(
        view_def_factory=lambda: None,
        build_corpus=lambda *_args, **_kwargs: {},
        asset_id=contract.qualified_id,
        asset_digest=contract.asset.digest,
    )
    with pytest.raises(ValueError, match="resolution token"):
        NativeViewStage([unresolved], deployment_name="demo")

    resolution = resolve_read_assets([contract])
    selected = ViewMaterializer(
        view_def_factory=lambda: None,
        build_corpus=lambda *_args, **_kwargs: {},
        asset_id=contract.qualified_id,
        asset_digest=contract.asset.digest,
        asset_execution_token=resolution.execution_token(contract.qualified_id),
    )
    assert NativeViewStage([selected], deployment_name="demo").materializers == [selected]

    wrong_view_factory = type("WrongView", (), {"view_id": "other-view"})
    mismatched_factory = ViewMaterializer(
        view_def_factory=wrong_view_factory,
        build_corpus=lambda *_args, **_kwargs: {},
        name="view",
        asset_id=contract.qualified_id,
        asset_digest=contract.asset.digest,
        asset_execution_token=resolution.execution_token(contract.qualified_id),
    )
    stage = NativeViewStage([mismatched_factory], deployment_name="demo")
    with pytest.raises(ValueError, match="factory/materializer identity"):
        stage.latest_completed_at(object())


def _mark_factory(factory, *, resolution, asset_id: str, marker_type=None):
    marker = (
        ResolvedReadAssetFactoryAuthority(
            resolution=resolution,
            asset_id=asset_id,
        )
        if marker_type is None
        else marker_type(resolution=resolution, asset_id=asset_id)
    )
    setattr(factory, READ_ASSET_FACTORY_AUTHORITY_ATTRIBUTE, marker)
    return factory


def test_materializer_adopts_only_exact_typed_factory_resolution() -> None:
    contract = _contract("view", asset_id="factory-governed")
    resolution = resolve_read_assets([contract], roots=[contract.qualified_id])

    def governed_factory():
        return None

    _mark_factory(
        governed_factory,
        resolution=resolution,
        asset_id=contract.qualified_id,
    )
    materializer = ViewMaterializer(
        view_def_factory=governed_factory,
        build_corpus=lambda *_args, **_kwargs: {},
    )

    assert materializer.asset_id == contract.qualified_id
    assert materializer.asset_digest == contract.asset.digest
    assert materializer.asset_execution_token == resolution.execution_token(
        contract.qualified_id
    )

    from tests.helpers.governed_view_materializer import (
        ViewMaterializer as TestViewMaterializer,
    )

    helper_materializer = TestViewMaterializer(
        view_def_factory=governed_factory,
        build_corpus=lambda *_args, **_kwargs: {},
    )
    assert helper_materializer.asset_id == materializer.asset_id
    assert helper_materializer.asset_digest == materializer.asset_digest
    assert helper_materializer.asset_execution_token == materializer.asset_execution_token


def test_materializer_refuses_invalid_factory_resolution_authority() -> None:
    contract = _contract("view", asset_id="factory-refusal")
    resolution = resolve_read_assets([contract], roots=[contract.qualified_id])
    token = resolution.execution_token(contract.qualified_id)

    def duck_factory():
        return None

    _mark_factory(
        duck_factory,
        resolution=resolution,
        asset_id=contract.qualified_id,
        marker_type=SimpleNamespace,
    )
    with pytest.raises(ValueError, match="exact typed marker"):
        ViewMaterializer(
            view_def_factory=duck_factory,
            build_corpus=lambda *_args, **_kwargs: {},
        )

    def untyped_resolution_factory():
        return None

    _mark_factory(
        untyped_resolution_factory,
        resolution=object(),
        asset_id=contract.qualified_id,
    )
    with pytest.raises(ValueError, match="typed read-asset resolution"):
        ViewMaterializer(
            view_def_factory=untyped_resolution_factory,
            build_corpus=lambda *_args, **_kwargs: {},
        )

    def unselected_factory():
        return None

    _mark_factory(
        unselected_factory,
        resolution=resolution,
        asset_id="demo:not-selected",
    )
    with pytest.raises(ValueError, match="unselected read asset"):
        ViewMaterializer(
            view_def_factory=unselected_factory,
            build_corpus=lambda *_args, **_kwargs: {},
        )

    def partial_factory():
        return None

    _mark_factory(
        partial_factory,
        resolution=resolution,
        asset_id=contract.qualified_id,
    )
    with pytest.raises(ValueError, match="all derived or all explicit"):
        ViewMaterializer(
            view_def_factory=partial_factory,
            build_corpus=lambda *_args, **_kwargs: {},
            asset_id=contract.qualified_id,
        )

    def mismatched_factory():
        return None

    _mark_factory(
        mismatched_factory,
        resolution=resolution,
        asset_id=contract.qualified_id,
    )
    with pytest.raises(ValueError, match="does not match its factory resolution"):
        ViewMaterializer(
            view_def_factory=mismatched_factory,
            build_corpus=lambda *_args, **_kwargs: {},
            asset_id=contract.qualified_id,
            asset_digest="sha256:" + "0" * 64,
            asset_execution_token=token,
        )


def test_legacy_registry_adapts_without_import_or_registration_side_effects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entry = parse_views_registry({
        "deployment": "demo",
        "views": [{
            "view_def_id": "demo.health",
            "view_def_factory": "views.health:factory",
            "build_corpus": "views.health:build_corpus",
        }],
    })[0]
    before_path = tuple(sys.path)
    imports: list[str] = []
    registrations: list[str] = []
    monkeypatch.setattr(
        importlib,
        "import_module",
        lambda name: imports.append(name),
    )
    monkeypatch.setattr(
        registry_module,
        "_materializer_from_attrs",
        lambda *_args, **_kwargs: registrations.append("materializer"),
    )

    resolution = adapt_views_registry_entries([entry])

    contract = resolution.contracts[0]
    assert contract.asset.qualified_id == "demo:demo.health"
    assert contract.asset.provenance["source"] == "compatibility"
    assert contract.asset.provenance["adapter"] == "views_registry.yaml/v1"
    assert contract.asset.digest.startswith("sha256:")
    assert imports == []
    assert registrations == []
    assert tuple(sys.path) == before_path


def test_unsupported_legacy_class_path_refuses_before_import_or_syspath(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entry = ViewRegistryEntry(
        view_def_id="demo.unsafe",
        view_name="Unsafe",
        deployment="demo",
        view_def_factory="views.health:Factory.build",
        build_corpus="views.health:build_corpus",
    )
    side_effects: list[str] = []
    monkeypatch.setattr(
        registry_module,
        "_ensure_deployment_on_syspath",
        lambda _root: side_effects.append("syspath"),
    )
    monkeypatch.setattr(
        registry_module,
        "_materializer_from_attrs",
        lambda *_args, **_kwargs: side_effects.append("materializer"),
    )

    with pytest.raises(TypedAssetResolutionError) as caught:
        materializers_from_registry_entries(
            [entry],
            deployment_name="demo",
            deployment_root=tmp_path,
        )

    assert _codes(caught.value) == {"legacy_view_registry_migration_required"}
    assert side_effects == []
