"""Deterministic, secret-free Wave 2 composition inspection coverage."""

from __future__ import annotations

import hashlib
from copy import deepcopy
from dataclasses import replace
from pathlib import Path

import pytest
import yaml

from okg.substrate.catalog.composition_inspection import (
    CompositionInspectionError,
    composition_inspection_digest,
    inspect_composition,
    inspect_refusal,
)
from okg.substrate.catalog.deployment_product_composition import (
    SemanticCompositionError,
    SemanticSubtype,
    compose_semantic_catalog,
)
from okg.substrate.contracts.deployment_product import (
    TypedAsset,
    TypedAssetRegistry,
    TypedAssetResolutionError,
)


FIXTURES = Path(__file__).parents[2] / "fixtures" / "deployment_products" / "composition_diagnostics"
SEMANTIC_FIXTURES = Path(__file__).parents[2] / "fixtures" / "deployment_products" / "semantic_catalog"


def _digest(label: str) -> str:
    return "sha256:" + hashlib.sha256(label.encode()).hexdigest()


def _resolution() -> tuple[object, list[TypedAsset]]:
    raw = yaml.safe_load((SEMANTIC_FIXTURES / "compatible.yaml").read_text())
    assets = [TypedAsset.from_value(item) for item in raw["assets"]]
    registry = TypedAssetRegistry()
    registry.register_many(assets)
    return registry.resolve(), assets


def _fixture_asset(raw: dict[str, object]) -> TypedAsset:
    return TypedAsset.from_value(raw)


def _mixed_success_resolution(raw: dict[str, object] | None = None) -> tuple[object, object, dict[str, object]]:
    raw = raw or yaml.safe_load((FIXTURES / "success.yaml").read_text())
    assets = [_fixture_asset(item) for item in raw["assets"]]
    registry = TypedAssetRegistry()
    registry.register_many(assets)
    resolution = registry.resolve(
        overrides=[
            {
                "slot_id": "diagnostics:token",
                "owner": "instance",
                "value": raw["secrets"]["instance"],
                "reason": "credential rotation",
            }
        ]
    )
    plan = compose_semantic_catalog(resolution, deployment_name=raw["deployment"])
    return resolution, plan, raw


def _asset(
    owner: str,
    asset_id: str,
    kind: str,
    *,
    compatibility: dict[str, object] | None = None,
    references: tuple[dict[str, object], ...] = (),
    version: str = "v1",
    digest: str | None = None,
    capabilities: tuple[str, ...] = ("declarative", "read"),
    merge_identity: str | None = None,
    merge_rule: str = "refuse",
    state: str = "enabled",
    override_slots: tuple[dict[str, object], ...] = (),
) -> TypedAsset:
    plane = (
        "semantic-catalog"
        if kind in {
            "ontology", "schema", "bridge", "narrowing", "alias",
            "identifier-pattern", "concept-seed", "identity-policy", "catalog-migration",
        }
        else "ingestion-projection"
    )
    return TypedAsset(
        owner=owner,
        id=asset_id,
        kind=kind,
        plane=plane,
        version=version,
        digest=digest or _digest(f"{owner}:{asset_id}:{kind}:{version}"),
        schema=f"okg.deployment_product.asset.{kind}/v1",
        merge_identity=merge_identity or f"{owner}-{asset_id}",
        merge_rule=merge_rule,
        capabilities=capabilities,
        compatibility=compatibility or {},
        references=references,
        state=state,
        override_slots=override_slots,
    )


def _semantic_asset(owner: str, asset_id: str, kind: str, semantic: dict[str, object]) -> TypedAsset:
    return _asset(
        owner,
        asset_id,
        kind,
        compatibility={"semantic": semantic},
        digest=_digest(f"semantic:{owner}:{asset_id}:{kind}:{semantic}"),
    )


def _typed_error(case_id: str, *, reverse: bool = False) -> TypedAssetResolutionError:
    if case_id == "missing-reference":
        root = _asset(
            "owner", "root", "source",
            references=(
                {"owner": "owner", "id": "missing", "kind": "source", "version": "v1"},
            ),
        )
        registry = TypedAssetRegistry()
        registry.register(root)
        with pytest.raises(TypedAssetResolutionError) as exc:
            registry.resolve()
        return exc.value
    if case_id == "dependency-cycle":
        first = _asset("owner", "a", "source", references=({"owner": "owner", "id": "b", "kind": "source", "version": "v1"},))
        second = _asset("owner", "b", "source", references=({"owner": "owner", "id": "a", "kind": "source", "version": "v1"},))
        registry = TypedAssetRegistry()
        registry.register_many([second, first] if reverse else [first, second])
        with pytest.raises(TypedAssetResolutionError) as exc:
            registry.resolve()
        return exc.value
    if case_id in {"same-id-digest-declaration-collision", "different-digest-collision"}:
        digest = _digest("same") if case_id.startswith("same") else _digest("first")
        first = _asset("owner", "shared", "source", digest=digest, compatibility={"value": "first"})
        second = _asset(
            "owner", "shared", "source",
            digest=digest if case_id.startswith("same") else _digest("second"),
            compatibility={"value": "second"},
        )
        registry = TypedAssetRegistry()
        registry.register(first)
        with pytest.raises(TypedAssetResolutionError) as exc:
            registry.register(second)
        return exc.value
    if case_id in {"unsupported-version", "unsupported-capability"}:
        required: dict[str, object] = {
            "owner": "owner", "id": "child", "kind": "source",
        }
        if case_id == "unsupported-version":
            required["version"] = "v2"
        else:
            required["required_capabilities"] = ["execute"]
        root = _asset("owner", "root", "source", references=(required,))
        child = _asset("owner", "child", "source", capabilities=("declarative", "read"))
        registry = TypedAssetRegistry()
        registry.register_many([root, child])
        with pytest.raises(TypedAssetResolutionError) as exc:
            registry.resolve()
        return exc.value
    slot = {"slot_id": "refresh", "value_type": "integer", "default": 60}
    asset = _asset("owner", "source", "source", override_slots=(slot,))
    registry = TypedAssetRegistry()
    registry.register(asset)
    overrides: dict[str, object]
    if case_id == "typed-override-wrong-value":
        overrides = {"slot_id": "refresh", "value": "not-an-integer"}
    elif case_id == "typed-override-raw-pointer":
        overrides = {"slot_id": "refresh", "path": "/credentials/token", "value": 60}
    elif case_id == "typed-override-unknown-field":
        overrides = {"slot_id": "refresh", "value": 60, "unknown": True}
    else:
        raise AssertionError(case_id)
    with pytest.raises(TypedAssetResolutionError) as exc:
        registry.resolve(overrides=[overrides])
    return exc.value


def _semantic_error(case_id: str, *, reverse: bool = False) -> SemanticCompositionError:
    def resolve(assets: list[TypedAsset]) -> SemanticCompositionError:
        registry = TypedAssetRegistry()
        registry.register_many(list(reversed(assets)) if reverse else assets)
        with pytest.raises(SemanticCompositionError) as exc:
            compose_semantic_catalog(registry.resolve(), deployment_name="diagnostics-product")
        return exc.value

    if case_id == "duplicate-semantic-ownership":
        return resolve([
            _semantic_asset("owner-a", "module", "ontology", {"module_id": "shared:module", "subtypes": {"Paper": {}}}),
            _semantic_asset("owner-b", "module", "ontology", {"module_id": "shared:module", "subtypes": {"Work": {}}}),
        ])
    if case_id == "unknown-narrowing-endpoint":
        return resolve([
            _semantic_asset("owner-a", "module", "ontology", {"module_id": "owner-a:module", "subtypes": {"Paper": {}}}),
            _semantic_asset("owner-b", "module", "ontology", {"module_id": "owner-b:module", "subtypes": {"Work": {}}}),
            _semantic_asset("owner-a", "bridge", "bridge", {"endpoint_modules": ["owner-a:module", "owner-b:module"], "narrowings": [{"parent": "Paper", "edge": "references", "child": "Ghost"}]}),
        ])
    if case_id == "bridge-endpoint-mismatch":
        return resolve([
            _semantic_asset("owner-a", "module", "ontology", {"module_id": "owner-a:module", "subtypes": {"Paper": {}}}),
            _semantic_asset("owner-b", "module", "ontology", {"module_id": "owner-b:module", "subtypes": {"Work": {}}}),
            _semantic_asset("owner-c", "module", "ontology", {"module_id": "owner-c:module", "subtypes": {"Claim": {}}}),
            _semantic_asset("owner-a", "bridge", "bridge", {"endpoint_modules": ["owner-a:module", "owner-b:module"], "narrowings": [{"parent": "Paper", "edge": "references", "child": "Claim"}]}),
        ])
    if case_id == "ambiguous-bridge-endpoint":
        return resolve([
            _semantic_asset("owner-a", "module", "ontology", {"module_id": "owner-a:module", "subtypes": {"Paper": {}}}),
            _semantic_asset("owner-a", "bridge", "bridge", {"endpoint_modules": ["owner-a:module"], "narrowings": []}),
        ])
    if case_id == "incompatible-identity":
        return resolve([
            _semantic_asset("owner-a", "module", "ontology", {"module_id": "owner-a:module", "subtypes": {"Paper": {"attributes": ["doi", "title"], "identity": {"fields": ["doi"], "cardinality": "one"}}}}),
            _semantic_asset("owner-a", "identity", "identity-policy", {"policies": [{"id": "identity-paper", "subtype": "Paper", "fields": ["title"], "cardinality": "one"}]}),
        ])
    if case_id == "incompatible-cardinality":
        return resolve([
            _semantic_asset("owner-a", "module", "ontology", {"module_id": "owner-a:module", "subtypes": {"Paper": {}}}),
            _semantic_asset("owner-b", "module", "ontology", {"module_id": "owner-b:module", "subtypes": {"Work": {}}}),
            _semantic_asset("owner-a", "bridge", "bridge", {"endpoint_modules": ["owner-a:module", "owner-b:module"], "narrowings": [{"parent": "Paper", "edge": "references", "child": "Work", "cardinality": "one"}]}),
        ])
    if case_id == "incompatible-evolution":
        return resolve([
            _semantic_asset("owner-a", "module", "ontology", {"module_id": "owner-a:module", "subtypes": {"Paper": {}}}),
            _semantic_asset("owner-a", "migration", "catalog-migration", {"migration": {"id": "paper-v2", "evolution": "breaking", "old_identity": "doi", "new_identity": "title"}}),
        ])
    if case_id == "alias-collision":
        return resolve([
            _semantic_asset("owner-a", "module", "ontology", {"module_id": "owner-a:module", "subtypes": {"Paper": {}, "Work": {}}}),
            _semantic_asset("owner-a", "alias-a", "alias", {"aliases": [{"id": "alias-a1", "alias": "shared", "canonical": "Paper", "scope": "global"}]}),
            _semantic_asset("owner-b", "alias-b", "alias", {"aliases": [{"id": "alias-b1", "alias": "shared", "canonical": "Work", "scope": "global"}]}),
        ])
    if case_id == "concept-collision":
        return resolve([
            _semantic_asset("owner-a", "seed-a", "concept-seed", {"seeds": [{"id": "c1", "name": "Paper", "description": "first", "node_id": "concept:paper"}]}),
            _semantic_asset("owner-b", "seed-b", "concept-seed", {"seeds": [{"id": "c2", "name": "Paper", "description": "second", "node_id": "concept:paper"}]}),
        ])
    if case_id == "inheritance-cycle":
        return resolve([
            _semantic_asset("owner-a", "module", "ontology", {"module_id": "owner-a:module", "subtypes": {"Paper": {"parent": "Work"}, "Work": {"parent": "Paper"}}}),
        ])
    if case_id == "malformed-semantic-payload":
        return resolve([
            _semantic_asset("owner-a", "malformed", "ontology", {"module_id": "owner-a:malformed", "subtypes": [], "typo": True}),
        ])
    if case_id == "malformed-semantic-contribution":
        return resolve([
            _semantic_asset("owner-a", "contribution", "alias", {"aliases": [{"id": "bad"}]}),
        ])
    raise AssertionError(case_id)


def test_success_inspection_enumerates_typed_semantic_and_lock_provenance() -> None:
    resolution, plan, fixture = _mixed_success_resolution()
    report = inspect_composition(
        resolution,
        plan,
        deployment=fixture["deployment"],
        catalog_version_id=fixture["catalog_version_id"],
        generation_id=fixture["generation_id"],
    )
    payload = report.to_dict()

    assert payload["deployment"] == fixture["deployment"]
    assert payload["catalog_identity"]["typed_composition_digest"] == resolution.composition_digest
    assert payload["catalog_identity"]["semantic_composition_digest"] == plan.composition_digest
    assert payload["catalog_identity"]["catalog_version_id"] == str(fixture["catalog_version_id"])
    assert payload["catalog_identity"]["generation_id"] == fixture["generation_id"]
    assert {item["definition_kind"] for item in payload["semantic_definitions"]} == set(fixture["semantic_definition_kinds"])
    expected_definitions = {
        (item["kind"], item["id"]): item
        for item in fixture["expected_semantic_definitions"]
    }
    actual_definitions = {
        (item["definition_kind"], item["definition_id"]): item
        for item in payload["semantic_definitions"]
    }
    assert set(actual_definitions) == set(expected_definitions)
    source_assets = {
        f"{item['owner']}:{item['id']}": item
        for item in fixture["assets"]
    }
    for key, expected in expected_definitions.items():
        actual = actual_definitions[key]
        assert actual["owner"] == expected["owner"]
        assert actual["asset_id"] == expected["asset_id"]
        assert actual["source_asset"] == expected["source_asset"]
        source = source_assets[expected["source_asset"]]
        assert actual["asset_digest"] == source["digest"]
        assert actual["merge_identity"] == source["merge_identity"]
        assert actual["merge_rule"] == source["merge_rule"]
        assert actual["failure_rule"] == source.get("failure_rule", source["merge_rule"])
        assert actual["references"] == []
        assert actual["reference_chains"] == []
    assert {item["state"] for item in payload["typed_assets"]} == {"enabled", "disabled", "tombstone"}
    assert {item["kind"] for item in payload["typed_assets"]} >= {
        "ontology", "bridge", "alias", "identifier-pattern", "concept-seed",
        "identity-policy", "catalog-migration", "source",
    }
    assert payload["lock_paths"]
    assert payload["provenance"]
    assert payload["merge_decisions"]
    assert all("merge_identity" in item and "decision" in item for item in payload["merge_decisions"])
    assert all(item["asset_digest"].startswith("sha256:") for item in payload["semantic_definitions"] if item["asset_digest"])
    assert {item["state"] for item in payload["typed_assets"] if item["state"] != "enabled"} == {"disabled", "tombstone"}
    assert len(payload["typed_assets"]) == len(fixture["assets"])
    assert any(item["path"].startswith("assets.disabled.") for item in payload["lock_paths"])
    assert any(item["path"].startswith("assets.tombstone.") for item in payload["lock_paths"])
    assert any(item["path"].startswith("overrides.") for item in payload["lock_paths"])
    assert any(".previous" in item["path"] for item in payload["lock_paths"])
    assert any(".reason" in item["path"] for item in payload["lock_paths"])
    assert any(item["path"].startswith("merges.") for item in payload["lock_paths"])
    assert any(".refs." in item["path"] for item in payload["lock_paths"])
    override = payload["overrides"][0]
    assert override["old_owner"] == "diagnostics"
    assert override["new_owner"] == "instance"
    assert override["old_value_digest"] and override["new_value_digest"]
    assert override["reason_digest"] and override["references"]
    serialized = report.canonical_bytes().decode()
    for secret in fixture["secrets"].values():
        assert secret not in serialized
    assert composition_inspection_digest(report) == report.composition_digest


def test_reversed_asset_registration_and_rows_have_identical_canonical_report() -> None:
    first_resolution, assets = _resolution()
    first_plan = compose_semantic_catalog(first_resolution, deployment_name="diagnostics-product")
    first = inspect_composition(first_resolution, first_plan)

    registry = TypedAssetRegistry()
    registry.register_many(reversed(assets))
    second_resolution = registry.resolve()
    second_plan = compose_semantic_catalog(second_resolution, deployment_name="diagnostics-product")
    second = inspect_composition(second_resolution, second_plan)

    assert first.canonical_bytes() == second.canonical_bytes()
    assert first.composition_digest == second.composition_digest


def test_inspection_does_not_copy_compatibility_or_override_values() -> None:
    resolution, assets = _resolution()
    secret_asset = TypedAsset(
        owner="secret-owner",
        id="secret-source",
        kind="source",
        plane="ingestion-projection",
        version="v1",
        digest="sha256:" + "9" * 64,
        schema="okg.deployment_product.asset.source/v1",
        merge_identity="secret-source",
        merge_rule="refuse",
        compatibility={"credential": "TOP-SECRET-CREDENTIAL"},
        override_slots=(
            {
                "slot_id": "token",
                "value_type": "string",
                "default": "TOP-SECRET-OVERRIDE",
            },
        ),
    )
    registry = TypedAssetRegistry()
    registry.register_many([*assets, secret_asset])
    resolution = registry.resolve(
        overrides=[
            {
                "slot_id": "secret-owner:token",
                "owner": "instance",
                "value": "TOP-SECRET-INSTANCE",
                "reason": "credential rotation",
            }
        ]
    )
    report = inspect_composition(resolution)
    serialized = report.canonical_bytes().decode()
    assert "TOP-SECRET-CREDENTIAL" not in serialized
    assert "TOP-SECRET-OVERRIDE" not in serialized
    assert "TOP-SECRET-INSTANCE" not in serialized
    assert "compatibility_digest" in serialized
    assert "default_digest" in serialized
    assert report.overrides[0]["old_value_digest"]
    assert report.overrides[0]["new_value_digest"]
    assert report.overrides[0]["reason_digest"]


def test_full_row_description_and_annotation_changes_digest_without_raw_leak() -> None:
    baseline_resolution, baseline_plan, fixture = _mixed_success_resolution()
    baseline = inspect_composition(baseline_resolution, baseline_plan)
    changed = deepcopy(fixture)
    for asset in changed["assets"]:
        if asset["id"] == "concepts":
            asset["compatibility"]["semantic"]["seeds"][0]["description"] = "TOP-SECRET-DESCRIPTION"
        if asset["id"] == "ontology":
            asset["compatibility"]["semantic"]["subtypes"]["Paper"]["annotations"]["searchable_text_fields"] = [
                "TOP-SECRET-ANNOTATION"
            ]
    changed_resolution, changed_plan, _ = _mixed_success_resolution(changed)
    changed_report = inspect_composition(changed_resolution, changed_plan)
    baseline_rows = {
        (item["definition_kind"], item["definition_id"]): item
        for item in baseline.semantic_definitions
    }
    changed_rows = {
        (item["definition_kind"], item["definition_id"]): item
        for item in changed_report.semantic_definitions
    }
    assert baseline_rows[("concept_seed", "paper")]["definition_digest"] != changed_rows[("concept_seed", "paper")]["definition_digest"]
    assert baseline_rows[("annotation", "paper.searchable_text_fields")]["definition_digest"] != changed_rows[("annotation", "paper.searchable_text_fields")]["definition_digest"]
    serialized = changed_report.canonical_bytes().decode()
    assert "TOP-SECRET-DESCRIPTION" not in serialized
    assert "TOP-SECRET-ANNOTATION" not in serialized


def test_refusal_envelope_preserves_fixture_codes_paths_and_fix_actions() -> None:
    fixture = yaml.safe_load((FIXTURES / "cases.yaml").read_text())
    for case in fixture["refusals"]:
        if case["stage"] == "typed_asset_resolution":
            error = _typed_error(case["id"])
        else:
            error = _semantic_error(case["id"])
        refusal = inspect_refusal(error)
        item = refusal.to_dict()["diagnostics"][0]
        assert refusal.to_dict()["phase"] == "pre_mutation"
        assert refusal.to_dict()["mutation_state"] == "pre_mutation"
        assert item["stage"] == case["stage"]
        assert item["code"] == case["code"]
        assert item["chain"] == case["chain"]
        assert item["owner"] == case["owner"]
        assert item["asset_id"] == case["asset_id"]
        assert item["fix_action"] == case["fix_action"]
        assert item["source"]["phase"] == "pre_mutation"
        assert all(diagnostic["chain"] == case["chain"] for diagnostic in refusal.to_dict()["diagnostics"])
        repeated = inspect_refusal(
            _typed_error(case["id"], reverse=True)
            if case["stage"] == "typed_asset_resolution"
            else _semantic_error(case["id"], reverse=True)
        )
        assert refusal.canonical_bytes() == repeated.canonical_bytes()
        assert refusal.refusal_digest == repeated.refusal_digest


def test_refusal_digest_and_order_are_stable_and_semantic_errors_have_no_invented_chain() -> None:
    first = inspect_refusal(_typed_error("dependency-cycle"))
    second = inspect_refusal(_typed_error("dependency-cycle", reverse=True))
    assert first.canonical_bytes() == second.canonical_bytes()
    assert first.refusal_digest == second.refusal_digest

    semantic = inspect_refusal(_semantic_error("alias-collision"))
    assert semantic.to_dict()["diagnostics"][0]["chain"] == []
    assert semantic.to_dict()["diagnostics"][0]["path"] is None


def test_inspection_refuses_catalog_resolution_source_drift() -> None:
    resolution, _ = _resolution()
    plan = compose_semantic_catalog(resolution, deployment_name="diagnostics-product")
    drifted = replace(
        plan,
        subtypes=plan.subtypes + (
            SemanticSubtype(
                name="ghost",
                owner="missing-owner",
                asset_id="missing-asset",
            ),
        ),
    )
    with pytest.raises(CompositionInspectionError, match="source asset is missing"):
        inspect_composition(resolution, drifted)


def test_inspection_refuses_existing_id_subtype_tamper_with_stale_digest() -> None:
    resolution, plan, _ = _mixed_success_resolution()
    tampered = replace(
        plan,
        subtypes=(replace(plan.subtypes[0], parent="paper"), *plan.subtypes[1:]),
    )
    with pytest.raises(CompositionInspectionError, match="payload"):
        inspect_composition(resolution, tampered)


def test_inspection_refuses_existing_id_alias_tamper_with_stale_digest() -> None:
    resolution, plan, _ = _mixed_success_resolution()
    aliases = ({**dict(plan.aliases[0]), "canonical": "artifact"}, *plan.aliases[1:])
    tampered = replace(plan, aliases=aliases)
    with pytest.raises(CompositionInspectionError, match="payload"):
        inspect_composition(resolution, tampered)


def test_inspection_refuses_existing_id_narrowing_tamper_with_stale_digest() -> None:
    resolution, plan, _ = _mixed_success_resolution()
    tampered = replace(
        plan,
        narrowings=(replace(plan.narrowings[0], edge_type="renamed"), *plan.narrowings[1:]),
    )
    with pytest.raises(CompositionInspectionError, match="payload"):
        inspect_composition(resolution, tampered)


def test_inspection_refuses_same_id_content_drift_against_catalog_plan() -> None:
    _, plan, fixture = _mixed_success_resolution()
    changed = deepcopy(fixture)
    for asset in changed["assets"]:
        if asset["id"] == "concepts":
            asset["compatibility"]["semantic"]["seeds"][0]["description"] = "changed content"
    changed_resolution, _, _ = _mixed_success_resolution(changed)
    with pytest.raises(CompositionInspectionError, match="semantic catalog"):
        inspect_composition(changed_resolution, plan)


def test_inspection_refuses_same_semantic_ids_with_different_source_digest() -> None:
    _, plan, fixture = _mixed_success_resolution()
    changed = deepcopy(fixture)
    for asset in changed["assets"]:
        if asset["id"] == "concepts":
            asset["digest"] = "sha256:" + "e" * 64
    changed_resolution, _, _ = _mixed_success_resolution(changed)
    with pytest.raises(CompositionInspectionError, match="semantic catalog"):
        inspect_composition(changed_resolution, plan)


def test_inspection_refuses_semantic_asset_id_set_drift() -> None:
    _, plan, fixture = _mixed_success_resolution()
    changed = deepcopy(fixture)
    changed["assets"].append(
        {
            "owner": "diagnostics",
            "id": "extra-alias",
            "kind": "alias",
            "plane": "semantic-catalog",
            "version": "v1",
            "digest": "sha256:" + "f" * 64,
            "schema": "okg.deployment_product.asset.alias/v1",
            "merge_identity": "diagnostics-extra-alias",
            "merge_rule": "refuse",
            "provenance": {"source": "definition"},
            "compatibility": {
                "semantic": {
                    "aliases": [
                        {"id": "extra", "alias": "extra-paper", "canonical": "Paper", "scope": "global"}
                    ]
                }
            },
        }
    )
    changed_resolution, _, _ = _mixed_success_resolution(changed)
    with pytest.raises(CompositionInspectionError, match="source asset ID set"):
        inspect_composition(changed_resolution, plan)
