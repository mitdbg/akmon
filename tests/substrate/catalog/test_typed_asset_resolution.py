"""Table-driven proof for the declarative deployment asset graph."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from okg.substrate.contracts.deployment_product import (
    AuthoredDeploymentDefinition,
    AssetKindSpec,
    DistributionRelease,
    ProductBundle,
    CORE_ASSET_KINDS,
    OverrideSlot,
    TypedAsset,
    TypedAssetRegistry,
    TypedAssetResolutionError,
    VALIDATION_PLANES,
    canonical_lock_bytes,
    resolve_product_lock_with_typed_assets,
    validate_selection,
)
from okg.substrate.contracts.deployment_product.typed_assets import AssetReference as GraphAssetReference


FIXTURES = Path(__file__).parents[2] / "fixtures" / "deployment_products" / "typed_asset_graph"


def _D(n: int) -> str:
    return "sha256:" + str(n) * 64


def _fixture() -> tuple[list[AssetKindSpec], list[TypedAsset]]:
    raw = yaml.safe_load((FIXTURES / "registry.yaml").read_text())
    kinds = [AssetKindSpec.from_value(item) for item in raw["kinds"]]
    assets = [TypedAsset.from_value(item) for item in raw["assets"]]
    return kinds, assets


def _collision_matrix() -> tuple[dict[str, object], ...]:
    raw = yaml.safe_load((FIXTURES / "collision_matrix.yaml").read_text())
    return tuple(raw["cases"])


def test_mixed_six_plane_graph_is_order_independent_and_topological() -> None:
    kinds, assets = _fixture()
    first = TypedAssetRegistry(kinds)
    second = TypedAssetRegistry(kinds)
    first.register_many(assets)
    second.register_many(reversed(assets))

    resolution_a = first.resolve()
    resolution_b = second.resolve()

    positions = {asset.qualified_id: index for index, asset in enumerate(resolution_a.ordered_assets)}
    assert positions["ontology:core-schema"] < positions["source:git-source"] < positions["enrich:mentions"] < positions["operator:dashboard"]
    assert positions["operator:dashboard"] < positions["api:read"] < positions["deployment:safe-defaults"]
    assert {asset.plane for asset in resolution_a.effective_assets} == {
        "semantic-catalog",
        "ingestion-projection",
        "derivation-publication",
        "views-read-models",
        "agent-ui",
        "operations-governance",
    }
    assert resolution_a.composition_digest == resolution_b.composition_digest
    assert resolution_a.to_dict() == resolution_b.to_dict()
    assert all(item.owner == item.reference.owner for item in resolution_a.provenance)
    assert all(
        item.value_digest == item.reference.digest
        for item in resolution_a.provenance
        if item.path.startswith("assets.")
    )


def test_lock_bridge_preserves_declared_origin_decisions_and_is_secret_free() -> None:
    product_root = Path(__file__).parents[2] / "fixtures" / "deployment_products" / "embedded_and_external"
    raw_product = yaml.safe_load((product_root / "embedded.yaml").read_text())
    release = DistributionRelease.model_validate(raw_product["distribution_release"])
    bundles = tuple(ProductBundle.model_validate(item) for item in raw_product["product_bundles"])
    definition = AuthoredDeploymentDefinition.model_validate(raw_product["definition"])
    selection = validate_selection(definition, release, bundles)
    kinds, assets = _fixture()

    def resolve_in_order(values):
        registry = TypedAssetRegistry(kinds)
        registry.register_many(values)
        resolution = registry.resolve(overrides=({"slot_id": "refresh_seconds", "value": 30, "reason": "operator tuning"},))
        return resolution, resolve_product_lock_with_typed_assets(selection, resolution)

    resolution_a, lock_a = resolve_in_order(assets)
    resolution_b, lock_b = resolve_in_order(reversed(assets))
    bytes_a = canonical_lock_bytes(lock_a)
    bytes_b = canonical_lock_bytes(lock_b)
    assert bytes_a == bytes_b
    assert b"supplied-secret" not in bytes_a
    by_path = {item.path: item for item in lock_a.provenance}
    assert by_path["assets.ontology:core-schema"].source == "distribution"
    assert by_path["assets.source:git-source"].source == "bundle"
    assert by_path["assets.api:read"].source == "definition"
    merge = by_path["merges.schema.core-schema"]
    assert merge.reference.kind == "merge-decision"
    assert merge.reference.version == "accepted"
    assert by_path["merges.schema.core-schema.refs.ontology:core-schema"].source == "distribution"
    override = by_path["overrides.operator:refresh_seconds"]
    assert by_path["overrides.operator:refresh_seconds.previous"].owner == "operator"
    assert by_path["overrides.operator:refresh_seconds.previous"].value_digest
    assert by_path["overrides.operator:refresh_seconds.reason"].owner == "instance"
    assert override.source == "override"
    override_paths = {item.path for item in lock_a.override_provenance}
    assert override_paths
    assert all(path.startswith("overrides.") for path in override_paths)
    assert not override_paths.intersection({"assets.ontology:core-schema", "merges.schema.core-schema"})
    provenance_paths = [item.path for item in lock_a.provenance]
    assert len(provenance_paths) == len(set(provenance_paths))
    assert override_paths <= set(provenance_paths)
    assert resolution_a.composition_digest == resolution_b.composition_digest

    state_registry = TypedAssetRegistry(kinds)
    state_registry.register_many(
        (
            TypedAsset(
                owner="owner-a",
                id="disabled",
                kind="schema",
                plane="semantic-catalog",
                version="v1",
                digest=_D(7),
                schema="okg.asset.schema/v1",
                merge_identity="disabled",
                merge_rule="refuse",
                state="disabled",
            ),
            TypedAsset(
                owner="owner-a",
                id="removed",
                kind="schema",
                plane="semantic-catalog",
                version="v1",
                digest=_D(8),
                schema="okg.asset.schema/v1",
                merge_identity="removed",
                merge_rule="refuse",
                state="tombstone",
            ),
        )
    )
    state_lock = resolve_product_lock_with_typed_assets(selection, state_registry.resolve())
    state_by_path = {item.path: item for item in state_lock.provenance}
    for state in ("disabled", "tombstone"):
        state_record = state_by_path[f"assets.{state}.owner-a:{'disabled' if state == 'disabled' else 'removed'}"]
        assert state_record.reference.kind == "asset-state"
        assert state_record.reference.version == state
        assert canonical_lock_bytes(state_lock)


def test_same_id_same_digest_deduplicates() -> None:
    kinds, _ = _fixture()
    registry = TypedAssetRegistry(kinds)
    asset = TypedAsset(owner="test", id="one", kind="schema", plane="semantic-catalog", version="v1", digest=_D(1), schema="okg.asset.schema/v1", merge_identity="one", merge_rule="refuse")
    assert registry.register(asset) is asset
    assert registry.register(TypedAsset(owner="test", id="one", kind="schema", plane="semantic-catalog", version="v1", digest=_D(1), schema="okg.asset.schema/v1", merge_identity="one", merge_rule="refuse")) is asset
    assert len(registry.assets) == 1


@pytest.mark.parametrize("reverse", (False, True))
def test_same_id_same_digest_with_different_declarations_refuses_order_independently(reverse: bool) -> None:
    kinds, _ = _fixture()
    declarations = (
        TypedAsset(
            owner="collision",
            id="same",
            kind="schema",
            plane="semantic-catalog",
            version="v1",
            digest=_D(4),
            schema="okg.asset.schema/v1",
            merge_identity="same",
            merge_rule="refuse",
            provenance={"source": "local"},
        ),
        TypedAsset(
            owner="collision",
            id="same",
            kind="schema",
            plane="semantic-catalog",
            version="v1",
            digest=_D(4),
            schema="okg.asset.schema/v1",
            merge_identity="same",
            merge_rule="refuse",
            provenance={"source": "bundle"},
        ),
    )
    registry = TypedAssetRegistry(kinds)
    ordered = declarations[::-1] if reverse else declarations
    registry.register(ordered[0])
    before = registry.to_dict()
    with pytest.raises(TypedAssetResolutionError) as exc_info:
        registry.register(ordered[1])
    assert exc_info.value.code == "duplicate_declaration_mismatch"
    assert [item.to_dict() for item in exc_info.value.diagnostics] == [
        {
            "code": "duplicate_declaration_mismatch",
            "chain": [],
            "path": "",
            "asset_id": "collision:same",
            "owner": "collision",
            "expected": "canonical declaration equality",
            "observed": "canonical declaration differs",
            "message": "asset 'collision:same' repeats its digest with a different declaration",
            "fix_hint": "deduplicate the complete declaration or change the qualified ID",
            "phase": "pre_mutation",
        }
    ]
    assert registry.to_dict() == before


@pytest.mark.parametrize("case", _collision_matrix(), ids=lambda item: str(item["id"]))
def test_refusal_matrix_has_stable_pre_mutation_diagnostics(case: dict[str, object]) -> None:
    case_id = str(case["id"])
    expected = str(case["expected_code"])
    kinds, assets = _fixture()
    registry = TypedAssetRegistry(kinds)
    registry.register_many(assets)
    if case_id == "missing":
        registry.register(TypedAsset(owner="test", id="missing-root", kind="source", plane="ingestion-projection", version="v1", digest=_D(7), schema="okg.asset.source/v1", merge_identity="missing-root", merge_rule="refuse", references=({"owner": "test", "id": "missing-child", "kind": "schema"},)))
        roots = ("test:missing-root",)
        overrides = ()
    elif case_id == "cycle":
        registry.register_many(
            (
                TypedAsset(owner="cycle", id="a", kind="schema", plane="semantic-catalog", version="v1", digest=_D(7), schema="okg.asset.schema/v1", merge_identity="a", merge_rule="refuse", references=({"owner": "cycle", "id": "b", "kind": "schema"},)),
                TypedAsset(owner="cycle", id="b", kind="schema", plane="semantic-catalog", version="v1", digest=_D(8), schema="okg.asset.schema/v1", merge_identity="b", merge_rule="refuse", references=({"owner": "cycle", "id": "a", "kind": "schema"},)),
            )
        )
        roots = ("cycle:a",)
        overrides = ()
    elif case_id == "duplicate_digest":
        first = TypedAsset(owner="collision", id="duplicate", kind="schema", plane="semantic-catalog", version="v1", digest=_D(1), schema="okg.asset.schema/v1", merge_identity="duplicate", merge_rule="refuse")
        registry.register(first)
        before_assets = dict(registry.assets)
        with pytest.raises(TypedAssetResolutionError) as exc_info:
            registry.register(TypedAsset(owner="collision", id="duplicate", kind="schema", plane="semantic-catalog", version="v1", digest=_D(2), schema="okg.asset.schema/v1", merge_identity="duplicate", merge_rule="refuse"))
        diagnostic = next(item for item in exc_info.value.diagnostics if item.code == expected)
        assert list(diagnostic.chain) == case["expected_chain"]
        assert diagnostic.fix_hint == case["expected_hint"]
        assert dict(registry.assets) == before_assets
        return
    elif case_id == "unsupported_version":
        roots = ({"owner": "ontology", "id": "core-schema", "kind": "schema", "version": "v9"},)
        overrides = ()
    elif case_id == "unsupported_capability":
        roots = ({"owner": "ontology", "id": "core-schema", "kind": "schema", "required_capabilities": ["execute"]},)
        overrides = ()
    else:
        roots = ("operator:dashboard",)
        overrides = ({"slot_id": "refresh_seconds", "value": "fast"},) if case_id == "override_type" else ({"path": "/assets/operator:dashboard", "value": 15},)
    mutations: list[str] = []
    with pytest.raises(TypedAssetResolutionError) as exc_info:
        registry.resolve(roots, overrides=overrides, mutation=lambda result: mutations.append(result.composition_digest))
    diagnostic = next(item for item in exc_info.value.diagnostics if item.code == expected)
    assert list(diagnostic.chain) == case["expected_chain"]
    assert diagnostic.fix_hint == case["expected_hint"]
    assert all(item.to_dict()["phase"] == "pre_mutation" for item in exc_info.value.diagnostics)
    assert mutations == []
    assert diagnostic.chain == tuple(case["expected_chain"])


@pytest.mark.parametrize(
    ("override", "expected_code", "expected_hint"),
    (
        (
            {"slot_id": "refresh_seconds", "id": "other", "value": 15},
            "override_alias_disagreement",
            "provide one slot_id/id value or make both aliases equal",
        ),
        (
            {"slot_id": "refresh_seconds", "value": 15, "unexpected": True},
            "override_unknown_field",
            "remove unknown fields and use the typed override vocabulary",
        ),
    ),
    ids=("alias-disagreement", "unknown-field"),
)
def test_mapping_override_vocabulary_refuses_before_mutation_and_preserves_registry(
    override: dict[str, object], expected_code: str, expected_hint: str
) -> None:
    kinds, assets = _fixture()
    registry = TypedAssetRegistry(kinds)
    registry.register_many(assets)
    before = registry.to_dict()
    mutations: list[str] = []
    with pytest.raises(TypedAssetResolutionError) as exc_info:
        registry.resolve(
            ("operator:dashboard",),
            overrides=(override,),
            mutation=lambda result: mutations.append(result.composition_digest),
        )
    diagnostic = next(item for item in exc_info.value.diagnostics if item.code == expected_code)
    assert diagnostic.fix_hint == expected_hint
    assert diagnostic.to_dict()["phase"] == "pre_mutation"
    assert mutations == []
    assert registry.to_dict() == before


def test_mapping_override_requires_value_before_mutation() -> None:
    kinds, assets = _fixture()
    registry = TypedAssetRegistry(kinds)
    registry.register_many(assets)
    before = registry.to_dict()
    mutations: list[str] = []
    with pytest.raises(TypedAssetResolutionError) as exc_info:
        registry.resolve(
            ("operator:dashboard",),
            overrides=({"slot_id": "refresh_seconds"},),
            mutation=lambda result: mutations.append(result.composition_digest),
        )
    diagnostic = next(item for item in exc_info.value.diagnostics if item.code == "override_value_missing")
    assert diagnostic.fix_hint == "provide the value field; use null explicitly for a json slot"
    assert diagnostic.to_dict()["phase"] == "pre_mutation"
    assert mutations == []
    assert registry.to_dict() == before


def test_mapping_override_accepts_explicit_null_for_json_slot() -> None:
    kinds, _ = _fixture()
    registry = TypedAssetRegistry(kinds)
    asset = TypedAsset(
        owner="nullable",
        id="config",
        kind="policy",
        plane="operations-governance",
        version="v1",
        digest=_D(7),
        schema="okg.asset.policy/v1",
        merge_identity="config",
        merge_rule="refuse",
        override_slots=({"slot_id": "json_value", "value_type": "json"},),
    )
    registry.register(asset)
    mutations: list[str] = []
    resolution = registry.resolve(
        ("nullable:config",),
        overrides=({"slot_id": "json_value", "value": None},),
        mutation=lambda result: mutations.append(result.composition_digest),
    )
    assert len(resolution.overrides) == 1
    assert resolution.overrides[0].new_value_digest.startswith("sha256:")
    assert mutations == [resolution.composition_digest]


@pytest.mark.parametrize(
    "overrides",
    (
        (
            {"slot_id": "refresh_seconds", "value": 30},
            {"slot_id": "refresh_seconds", "value": 30},
        ),
        (
            {"slot_id": "refresh_seconds", "value": 30},
            {"id": "refresh_seconds", "value": 30},
        ),
    ),
    ids=("identical-values", "alias-forms"),
)
def test_duplicate_typed_overrides_refuse_order_independently(
    overrides: tuple[dict[str, object], dict[str, object]],
) -> None:
    diagnostics_by_order: list[dict[str, object]] = []
    for ordered_overrides in (overrides, tuple(reversed(overrides))):
        kinds, assets = _fixture()
        registry = TypedAssetRegistry(kinds)
        registry.register_many(assets)
        before = registry.to_dict()
        mutations: list[str] = []
        with pytest.raises(TypedAssetResolutionError) as exc_info:
            registry.resolve(
                ("operator:dashboard",),
                overrides=ordered_overrides,
                mutation=lambda result: mutations.append(result.composition_digest),
            )
        diagnostic = next(item for item in exc_info.value.diagnostics if item.code == "duplicate_override_slot")
        assert diagnostic.chain == ("operator:refresh_seconds",)
        assert diagnostic.fix_hint == "provide one override per owner-qualified slot"
        assert diagnostic.to_dict()["phase"] == "pre_mutation"
        diagnostics_by_order.append(diagnostic.to_dict())
        assert mutations == []
        assert registry.to_dict() == before
    assert diagnostics_by_order[0] == diagnostics_by_order[1]

    # Alias normalization also keeps the successful single contribution's
    # value digest and resulting composition digest deterministic.
    single_digests: list[str] = []
    for override in (overrides[0], overrides[1]):
        kinds, assets = _fixture()
        registry = TypedAssetRegistry(kinds)
        registry.register_many(assets)
        single_digests.append(
            registry.resolve(("operator:dashboard",), overrides=(override,)).composition_digest
        )
    assert single_digests[0] == single_digests[1]


@pytest.mark.parametrize(
    "overrides",
    (
        (
            {"slot_id": "refresh_seconds", "value": "wrong"},
            {"slot_id": "refresh_seconds", "value": 30},
        ),
        (
            {"slot_id": "refresh_seconds", "owner": "not-instance", "value": 30},
            {"slot_id": "refresh_seconds", "value": 30},
        ),
    ),
    ids=("wrong-value", "invalid-owner"),
)
def test_duplicate_mixed_invalid_and_valid_overrides_have_one_order_independent_refusal(
    overrides: tuple[dict[str, object], dict[str, object]],
) -> None:
    diagnostics_by_order: list[dict[str, object]] = []
    for ordered_overrides in (overrides, tuple(reversed(overrides))):
        kinds, assets = _fixture()
        registry = TypedAssetRegistry(kinds)
        registry.register_many(assets)
        before = registry.to_dict()
        mutations: list[str] = []
        with pytest.raises(TypedAssetResolutionError) as exc_info:
            registry.resolve(
                ("operator:dashboard",),
                overrides=ordered_overrides,
                mutation=lambda result: mutations.append(result.composition_digest),
            )
        assert [item.code for item in exc_info.value.diagnostics] == ["duplicate_override_slot"]
        diagnostic = exc_info.value.diagnostics[0]
        assert diagnostic.chain == ("operator:refresh_seconds",)
        assert diagnostic.fix_hint == "provide one override per owner-qualified slot"
        diagnostics_by_order.append(diagnostic.to_dict())
        assert mutations == []
        assert registry.to_dict() == before
    assert diagnostics_by_order[0] == diagnostics_by_order[1]


def test_explicit_disable_and_tombstone_are_resolved_without_implicit_precedence() -> None:
    kinds, _ = _fixture()
    assets = (
        TypedAsset(owner="owner-a", id="disabled", kind="schema", plane="semantic-catalog", version="v1", digest=_D(1), schema="okg.asset.schema/v1", merge_identity="disabled", merge_rule="refuse", enabled=False),
        TypedAsset(owner="owner-a", id="removed", kind="schema", plane="semantic-catalog", version="v1", digest=_D(2), schema="okg.asset.schema/v1", merge_identity="removed", merge_rule="refuse", tombstone=True),
    )
    first = TypedAssetRegistry(kinds)
    second = TypedAssetRegistry(kinds)
    first.register_many(assets)
    second.register_many(reversed(assets))
    resolution = first.resolve()
    reversed_resolution = second.resolve()
    assert resolution.effective_assets == ()
    assert [item.qualified_id for item in resolution.disabled_assets] == ["owner-a:disabled"]
    assert [item.qualified_id for item in resolution.tombstones] == ["owner-a:removed"]
    assert resolution.composition_digest == reversed_resolution.composition_digest
    assert resolution.to_dict() == reversed_resolution.to_dict()


def test_duplicate_id_with_different_digest_refuses_before_mutation() -> None:
    kinds, _ = _fixture()
    registry = TypedAssetRegistry(kinds)
    registry.register(TypedAsset(owner="same", id="id", kind="schema", plane="semantic-catalog", version="v1", digest=_D(1), schema="okg.asset.schema/v1", merge_identity="id", merge_rule="refuse"))
    with pytest.raises(TypedAssetResolutionError) as exc_info:
        registry.register(TypedAsset(owner="same", id="id", kind="schema", plane="semantic-catalog", version="v1", digest=_D(2), schema="okg.asset.schema/v1", merge_identity="id", merge_rule="refuse"))
    assert {item.code for item in exc_info.value.diagnostics} == {"digest_collision", "duplicate_id"}


def test_batch_registration_collision_is_atomic_and_refusal_callback_stays_untouched() -> None:
    kinds, _ = _fixture()
    registry = TypedAssetRegistry(kinds)
    first = TypedAsset(owner="batch", id="same", kind="schema", plane="semantic-catalog", version="v1", digest=_D(1), schema="okg.asset.schema/v1", merge_identity="same", merge_rule="refuse")
    conflicting = TypedAsset(owner="batch", id="same", kind="schema", plane="semantic-catalog", version="v1", digest=_D(2), schema="okg.asset.schema/v1", merge_identity="same", merge_rule="refuse")
    registry.register(first)
    before = registry.to_dict()
    with pytest.raises(TypedAssetResolutionError):
        registry.register_many((
            TypedAsset(owner="batch", id="new", kind="schema", plane="semantic-catalog", version="v1", digest=_D(3), schema="okg.asset.schema/v1", merge_identity="new", merge_rule="refuse"),
            conflicting,
        ))
    assert registry.to_dict() == before

    unresolved = TypedAsset(
        owner="batch",
        id="root",
        kind="source",
        plane="ingestion-projection",
        version="v1",
        digest=_D(4),
        schema="okg.asset.source/v1",
        merge_identity="root",
        merge_rule="refuse",
        references=({"owner": "batch", "id": "missing", "kind": "schema"},),
    )
    registry.register(unresolved)
    mutations: list[str] = []
    with pytest.raises(TypedAssetResolutionError):
        registry.resolve(("batch:root",), mutation=lambda result: mutations.append(result.composition_digest))
    assert mutations == []


@pytest.mark.parametrize("kind", tuple(TypedAssetRegistry().kinds), ids=str)
def test_hard_digest_collision_is_checked_for_each_registered_kind(kind: str) -> None:
    registry = TypedAssetRegistry()
    spec = registry.kinds[kind]
    first = TypedAsset(owner="collision", id=f"{kind}-asset", kind=kind, plane=spec.plane, version="v1", digest=_D(1), schema=spec.schema, merge_identity=f"{kind}-asset", merge_rule="refuse")
    second = TypedAsset(owner="collision", id=f"{kind}-asset", kind=kind, plane=spec.plane, version="v1", digest=_D(2), schema=spec.schema, merge_identity=f"{kind}-asset", merge_rule="refuse")
    registry.register(first)
    with pytest.raises(TypedAssetResolutionError) as exc_info:
        registry.register(second)
    assert exc_info.value.code == "digest_collision"


@pytest.mark.parametrize(
    ("loader", "payload"),
    (
        (TypedAsset.from_value, {"owner": "a", "id": "x", "kind": "schema", "plane": "semantic-catalog", "version": "v1", "digest": _D(1), "unknown": True}),
        (AssetKindSpec.from_value, {"kind": "schema", "schema": "okg.schema/v1", "plane": "semantic-catalog", "merge_identity": "qualified_id", "unknown": True}),
        (GraphAssetReference.from_value, {"owner": "a", "id": "x", "kind": "schema", "unknown": True}),
        (OverrideSlot.from_value, {"slot_id": "x", "value_type": "integer", "owner": "a", "unknown": True}),
    ),
)
def test_declarative_loaders_fail_closed_on_unknown_fields(loader, payload) -> None:
    kwargs = {"owner": "a"} if loader.__qualname__.startswith("OverrideSlot.") else {}
    with pytest.raises(ValueError, match="unknown fields"):
        loader(payload, **kwargs)


@pytest.mark.parametrize(
    ("missing", "message"),
    (
        ("schema", "asset.schema is required"),
        ("merge_identity", "asset.merge_identity is required"),
        ("merge_rule", "asset.merge_rule or asset.failure_rule is required"),
    ),
)
def test_asset_contributions_require_explicit_merge_and_schema_metadata(missing: str, message: str) -> None:
    declaration = {
        "owner": "metadata",
        "id": "asset",
        "kind": "schema",
        "plane": "semantic-catalog",
        "version": "v1",
        "digest": _D(6),
        "schema": "okg.asset.schema/v1",
        "merge_identity": "qualified_id",
        "merge_rule": "refuse",
    }
    declaration.pop(missing)
    with pytest.raises(ValueError, match=message):
        TypedAsset(**declaration)


def test_default_kind_metadata_covers_planes_and_never_grants_executable_capabilities() -> None:
    registry = TypedAssetRegistry()
    assert set(VALIDATION_PLANES) == {
        spec.plane for spec in registry.kinds.values()
    }
    assert set(CORE_ASSET_KINDS) == set(registry.kinds)
    for spec in registry.kinds.values():
        assert spec.config_schema and spec.result_schema
        assert spec.merge_identity and spec.failure_rule == "refuse"
        assert spec.reference_kinds and spec.reference_cardinality
        assert "execute" not in spec.capabilities
        assert "network" not in spec.capabilities


def test_restricted_kind_requires_declared_reference_kind_and_cardinality_policy() -> None:
    kinds, _ = _fixture()
    registry = TypedAssetRegistry(kinds)
    restricted = TypedAsset(
        owner="test",
        id="source",
        kind="source",
        plane="ingestion-projection",
        version="v1",
        digest=_D(9),
        schema="okg.asset.source/v1",
        merge_identity="source",
        merge_rule="refuse",
        references=({"owner": "test", "id": "unknown", "kind": "interface"},),
    )
    with pytest.raises(TypedAssetResolutionError) as exc_info:
        registry.register(restricted)
    assert exc_info.value.code == "reference_kind_undeclared"
    kind_diagnostic = next(
        item for item in exc_info.value.diagnostics if item.code == "reference_kind_undeclared"
    )
    assert kind_diagnostic.fix_hint == "declare the referenced kind in the registered kind policy"

    missing_cardinality_policy = AssetKindSpec(
        kind="restricted",
        schema="okg.asset.restricted/v1",
        plane="operations-governance",
        supported_versions=("v1",),
        reference_kinds={"schema": ("schema",)},
        default_merge_rule="refuse",
        merge_identity="restricted",
        failure_rule="refuse",
    )
    registry.register_kind(missing_cardinality_policy)
    cardinality_asset = TypedAsset(
        owner="test",
        id="restricted",
        kind="restricted",
        plane="operations-governance",
        version="v1",
        digest=_D(3),
        schema="okg.asset.restricted/v1",
        merge_identity="restricted",
        merge_rule="refuse",
        references=({"owner": "test", "id": "schema", "kind": "schema"},),
    )
    with pytest.raises(TypedAssetResolutionError) as cardinality_exc:
        registry.register(cardinality_asset)
    assert cardinality_exc.value.code == "reference_cardinality_undeclared"
