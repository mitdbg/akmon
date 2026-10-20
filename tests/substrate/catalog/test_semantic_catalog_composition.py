"""Wave 2 semantic catalog composition and real publish proof."""

from __future__ import annotations

from pathlib import Path
import json

import psycopg
import pytest
import yaml

from okg.substrate.catalog.deployment_product_composition import (
    EXECUTABLE_ASSET_KINDS,
    NONSEMANTIC_DISPOSITION,
    SEMANTIC_ASSET_KINDS,
    SemanticCompositionError,
    apply_semantic_catalog,
    compose_semantic_catalog,
)
from okg.substrate.contracts.deployment_product import AssetKindSpec, CORE_ASSET_KINDS, TypedAsset, TypedAssetRegistry
from okg.substrate.publisher.publisher import Publisher
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


FIXTURES = Path(__file__).parents[2] / "fixtures" / "deployment_products" / "semantic_catalog"


def _asset(raw: dict[str, object]) -> TypedAsset:
    return TypedAsset.from_value(raw)


def _resolution(assets: list[TypedAsset]):
    registry = TypedAssetRegistry()
    registry.register_many(assets)
    return registry.resolve()


def _compatible_assets() -> list[TypedAsset]:
    raw = yaml.safe_load((FIXTURES / "compatible.yaml").read_text())
    return [_asset(item) for item in raw["assets"]]


def _digest(n: int) -> str:
    return "sha256:" + format(n % 16, "x") * 64


def _semantic_asset(
    owner: str,
    asset_id: str,
    kind: str,
    semantic: dict[str, object],
    digest_number: int,
) -> TypedAsset:
    return TypedAsset(
        owner=owner,
        id=asset_id,
        kind=kind,
        plane="semantic-catalog",
        version="v1",
        digest=_digest(digest_number),
        schema=f"okg.deployment_product.asset.{kind}/v1",
        merge_identity=f"{owner}-{asset_id}",
        merge_rule="refuse",
        compatibility={"semantic": semantic},
    )


def _state(conn: psycopg.Connection) -> dict[str, object]:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM okg.catalog_versions")
        versions = int(cur.fetchone()[0])
        cur.execute("SELECT count(*) FROM okg.subtypes")
        subtypes = int(cur.fetchone()[0])
        cur.execute("SELECT count(*) FROM okg.narrowings")
        narrowings = int(cur.fetchone()[0])
        cur.execute("SELECT count(*) FROM okg.graph_nodes")
        graph_nodes = int(cur.fetchone()[0])
        cur.execute(
            "SELECT generation_id, status FROM okg.graph_generations "
            "ORDER BY generation_id DESC LIMIT 1"
        )
        latest = cur.fetchone()
    return {
        "catalog_versions": versions,
        "subtypes": subtypes,
        "narrowings": narrowings,
        "graph_nodes": graph_nodes,
        "latest_generation": tuple(latest) if latest else None,
    }


def test_compatible_modules_bridge_and_executable_assets_are_separated() -> None:
    plan = compose_semantic_catalog(
        _resolution(_compatible_assets()), deployment_name="semantic-product"
    )

    assert {item.name for item in plan.subtypes} == {"artifact", "paper", "work"}
    assert [item.triple for item in plan.narrowings] == [("paper", "references", "work")]
    assert plan.executable_asset_ids == (
        "operator:paper-dashboard",
        "scholarship:runtime-source",
    )
    assert plan.nonsemantic_asset_ids == (
        "operator:paper-dashboard",
        "operator:publish-policy",
        "scholarship:runtime-source",
    )
    assert plan.composed_catalog.subtype_origin["paper"] == "scholarship:papers"
    assert plan.composed_catalog.subtype_metadata["paper"]["attribute_definitions"]["doi"]["required"] is True
    assert plan.composed_catalog.aliases[0]["canonical"] == "paper"
    # Composition remains a declarative plan; no catalog writer is reached.
    assert plan.composition_digest.startswith("sha256:")


@pytest.mark.postgres
def test_compatible_modules_apply_and_publish_real_generation(
    clean_runtime: str,
    record_property,
) -> None:
    plan = compose_semantic_catalog(
        _resolution(_compatible_assets()), deployment_name="semantic-product"
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        before = _state(conn)
        cv = apply_semantic_catalog(conn, plan)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.subtypes WHERE catalog_version_id = %s",
                (cv,),
            )
            subtype_delta = int(cur.fetchone()[0])
            cur.execute(
                "SELECT count(*) FROM okg.narrowings WHERE catalog_version_id = %s",
                (cv,),
            )
            narrowing_delta = int(cur.fetchone()[0])
        applied = _state(conn)

    assert subtype_delta == 3
    assert narrowing_delta == 1
    # Applying the composed catalog adds exactly one catalog version, carrying
    # the rows counted above.
    assert applied["catalog_versions"] == before["catalog_versions"] + 1
    assert applied["subtypes"] == before["subtypes"] + subtype_delta
    assert applied["narrowings"] == before["narrowings"] + narrowing_delta

    # Binding an explicit test catalog writer CLONES a catalog template, so it
    # writes catalog rows of its own. Do that before the publish window opens,
    # or those rows are charged to the publish (issue #1934).
    catalog_identity = bind_test_catalog_writer(clean_runtime, "semantic-product")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        before_publish = _state(conn)

    published = Publisher(
        clean_runtime,
        "semantic-product",
        enrichers=[],
        catalog_identity=catalog_identity,
    ).cycle()
    assert published["status"] == "published", published
    generation = str(published["generation_id"])
    record_property("published_generation", generation)
    record_property("published_status", published["status"])
    record_property("subtype_row_delta", subtype_delta)
    record_property("narrowing_row_delta", narrowing_delta)
    assert generation

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        after = _state(conn)
    # Publishing a generation writes no catalog rows at all.
    assert after["catalog_versions"] == before_publish["catalog_versions"]
    assert after["subtypes"] == before_publish["subtypes"]
    assert after["narrowings"] == before_publish["narrowings"]
    assert not any(
        asset_id.endswith((":runtime-source", ":paper-dashboard"))
        for asset_id in plan.semantic_asset_ids
    )


def _conflict_assets(case_id: str) -> list[TypedAsset]:
    if case_id == "duplicate-subtype-ownership":
        return [
            _semantic_asset("owner-a", "module", "ontology", {"module_id": "owner-a:module", "subtypes": {"Paper": {}}}, 11),
            _semantic_asset("owner-b", "module", "ontology", {"module_id": "owner-b:module", "subtypes": {"Paper": {}}}, 12),
        ]
    if case_id == "unknown-narrowing-endpoint":
        return [
            _semantic_asset("owner-a", "module", "ontology", {"module_id": "owner-a:module", "subtypes": {"Paper": {}}}, 21),
            _semantic_asset("owner-b", "module", "ontology", {"module_id": "owner-b:module", "subtypes": {"Work": {}}}, 23),
            _semantic_asset("owner-a", "bridge", "bridge", {"endpoint_modules": ["owner-a:module", "owner-b:module"], "narrowings": [{"parent": "Paper", "edge": "references", "child": "Ghost"}]}, 22),
        ]
    if case_id == "incompatible-identity-evolution":
        return [
            _semantic_asset("owner-a", "module", "ontology", {"module_id": "owner-a:module", "subtypes": {"Paper": {"attributes": ["doi", "title"], "identity": {"fields": ["doi"], "cardinality": "one"}}}}, 31),
            _semantic_asset("owner-a", "identity", "identity-policy", {"policies": [{"id": "paper-identity-v2", "subtype": "Paper", "fields": ["title"], "cardinality": "one"}]}, 32),
        ]
    if case_id == "ambiguous-bridge-endpoint":
        return [
            _semantic_asset("owner-a", "module", "ontology", {"module_id": "owner-a:module", "subtypes": {"Paper": {}}}, 41),
            _semantic_asset("owner-a", "bridge", "bridge", {"endpoint_modules": ["owner-a:module"], "narrowings": []}, 42),
        ]
    if case_id == "breaking-catalog-migration":
        return [
            _semantic_asset("owner-a", "module", "ontology", {"module_id": "owner-a:module", "subtypes": {"Paper": {}}}, 51),
            _semantic_asset("owner-a", "migration", "catalog-migration", {"migration": {"id": "paper-v2", "evolution": "breaking", "old_identity": "doi", "new_identity": "title"}}, 52),
        ]
    if case_id == "bridge-endpoint-mismatch":
        return [
            _semantic_asset("owner-a", "module", "ontology", {"module_id": "owner-a:module", "subtypes": {"Paper": {}}}, 53),
            _semantic_asset("owner-b", "module", "ontology", {"module_id": "owner-b:module", "subtypes": {"Work": {}}}, 54),
            _semantic_asset("owner-c", "module", "ontology", {"module_id": "owner-c:module", "subtypes": {"Claim": {}}}, 55),
            _semantic_asset("owner-a", "bridge", "bridge", {"endpoint_modules": ["owner-a:module", "owner-b:module"], "narrowings": [{"parent": "Paper", "edge": "references", "child": "Claim"}]}, 56),
        ]
    if case_id == "unsupported-narrowing-cardinality":
        return [
            _semantic_asset("owner-a", "module", "ontology", {"module_id": "owner-a:module", "subtypes": {"Paper": {}}}, 57),
            _semantic_asset("owner-b", "module", "ontology", {"module_id": "owner-b:module", "subtypes": {"Work": {}}}, 58),
            _semantic_asset("owner-a", "bridge", "bridge", {"endpoint_modules": ["owner-a:module", "owner-b:module"], "narrowings": [{"parent": "Paper", "edge": "references", "child": "Work", "cardinality": "one"}]}, 59),
        ]
    if case_id == "inheritance-cycle":
        return [
            _semantic_asset("owner-a", "module", "ontology", {"module_id": "owner-a:module", "subtypes": {"Paper": {"parent": "Work", "attributes": ["doi"]}, "Work": {"parent": "Paper", "attributes": ["title"]}}}, 63),
        ]
    if case_id == "duplicate-concept-node-id":
        return [
            _semantic_asset("owner-a", "seed-a", "concept-seed", {"seeds": [{"id": "c1", "name": "Paper", "description": "first", "node_id": "concept:paper"}]}, 64),
            _semantic_asset("owner-b", "seed-b", "concept-seed", {"seeds": [{"id": "c2", "name": "Paper", "description": "second", "node_id": "concept:paper"}]}, 65),
        ]
    if case_id == "conflicting-alias-key":
        return [
            _semantic_asset("owner-a", "module", "ontology", {"module_id": "owner-a:module", "subtypes": {"Paper": {}, "Work": {}}}, 66),
            _semantic_asset("owner-a", "alias-a", "alias", {"aliases": [{"id": "alias-a1", "alias": "shared", "canonical": "Paper", "scope": "global"}]}, 67),
            _semantic_asset("owner-b", "alias-b", "alias", {"aliases": [{"id": "alias-b1", "alias": "shared", "canonical": "Work", "scope": "global"}]}, 68),
        ]
    raise AssertionError(case_id)


@pytest.mark.postgres
def test_conflict_matrix_refuses_before_catalog_or_generation_mutation(
    clean_runtime: str,
) -> None:
    matrix = yaml.safe_load((FIXTURES / "conflict_matrix.yaml").read_text())["cases"]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        for case in matrix:
            before = _state(conn)
            with pytest.raises(SemanticCompositionError) as exc:
                compose_semantic_catalog(
                    _resolution(_conflict_assets(case["id"])),
                    deployment_name="semantic-product",
                )
            with pytest.raises(SemanticCompositionError) as repeated:
                compose_semantic_catalog(
                    _resolution(_conflict_assets(case["id"])),
                    deployment_name="semantic-product",
                )
            assert exc.value.diagnostics[0].code == case["code"]
            serialized = [item.to_dict() for item in exc.value.diagnostics]
            assert all(
                item["owner"] and item["asset_id"] and item["fix_hint"]
                and item["phase"] == "pre_mutation"
                for item in serialized
            )
            assert json.dumps(serialized, sort_keys=True) == json.dumps(
                [item.to_dict() for item in repeated.value.diagnostics],
                sort_keys=True,
            )
            assert _state(conn) == before


def test_selection_does_not_implicitly_select_executable_kinds() -> None:
    semantic_only = [item for item in _compatible_assets() if item.kind == "ontology"]
    plan = compose_semantic_catalog(_resolution(semantic_only), deployment_name="semantic-product")
    assert plan.executable_asset_ids == ()
    assert all(item not in {"source", "enricher", "view", "dashboard", "prompt", "tool"} for item in plan.catalog.modules)


def test_every_registered_kind_has_an_explicit_wave3_disposition() -> None:
    registry = TypedAssetRegistry()
    assets: list[TypedAsset] = []
    for index, kind in enumerate(sorted(CORE_ASSET_KINDS), start=80):
        spec = registry.kinds[kind]
        if kind == "ontology":
            semantic = {"module_id": "all:ontology", "subtypes": {"Paper": {"attributes": ["id"]}}}
        elif kind == "schema":
            semantic = {"module_id": "all:schema", "subtypes": {"Work": {"attributes": ["id"]}}}
        elif kind in {"bridge", "narrowing"}:
            semantic = {
                "endpoint_modules": ["all:ontology", "all:schema"],
                "narrowings": [{"id": f"{kind}-edge", "parent": "Paper", "edge": "references", "child": "Work", "cardinality": "many"}],
            }
        elif kind == "alias":
            semantic = {"aliases": [{"id": "paper-alias", "alias": "paper-key", "canonical": "Paper", "scope": "global"}]}
        elif kind == "identifier-pattern":
            semantic = {"patterns": [{"id": "paper-id", "id_type": "paper", "regex": "^paper:"}]}
        elif kind == "concept-seed":
            semantic = {"seeds": [{"id": "paper-concept", "name": "Paper", "description": "A paper.", "node_id": "concept:paper"}]}
        elif kind == "identity-policy":
            semantic = {"policies": [{"id": "paper-identity", "subtype": "Paper", "fields": ["id"], "cardinality": "one"}]}
        elif kind == "catalog-migration":
            semantic = {"migration": {"id": "paper-migration", "old_identity": "id", "new_identity": "id", "evolution": "compatible"}}
        else:
            semantic = None
        assets.append(
            TypedAsset(
                owner="all",
                id=kind,
                kind=kind,
                plane=spec.plane,
                version="v1",
                digest=_digest(index % 16),
                schema=spec.schema,
                merge_identity=f"all-{kind}",
                merge_rule="refuse",
                compatibility={"semantic": semantic} if semantic is not None else {"declarative": True},
            )
        )
    plan = compose_semantic_catalog(_resolution(assets), deployment_name="all-kinds")
    semantic_ids = {item.split(":", 1)[1] for item in plan.semantic_asset_ids}
    assert semantic_ids == SEMANTIC_ASSET_KINDS
    nonsemantic_kinds = {item["kind"] for item in plan.nonsemantic_dispositions}
    assert nonsemantic_kinds == set(CORE_ASSET_KINDS) - SEMANTIC_ASSET_KINDS
    assert all(item["disposition"] == NONSEMANTIC_DISPOSITION[item["kind"]] for item in plan.nonsemantic_dispositions)
    assert {item.split(":", 1)[1] for item in plan.executable_asset_ids} == {
        kind for kind in CORE_ASSET_KINDS if kind in EXECUTABLE_ASSET_KINDS
    }
    assert not set(plan.catalog.modules) & nonsemantic_kinds


def test_custom_registered_nonsemantic_kind_refuses_without_disposition() -> None:
    registry = TypedAssetRegistry()
    registry.register_kind(
        AssetKindSpec(
            kind="custom-runtime",
            schema="okg.custom-runtime/v1",
            plane="operations-governance",
            merge_identity="qualified_id",
            default_merge_rule="refuse",
            failure_rule="refuse",
        )
    )
    registry.register(
        TypedAsset(
            owner="custom",
            id="runtime",
            kind="custom-runtime",
            plane="operations-governance",
            version="v1",
            digest=_digest(15),
            schema="okg.custom-runtime/v1",
            merge_identity="custom-runtime",
            merge_rule="refuse",
            compatibility={"declarative": True},
        )
    )
    with pytest.raises(SemanticCompositionError) as exc:
        compose_semantic_catalog(registry.resolve(), deployment_name="custom")
    assert exc.value.diagnostics[0].code == "nonsemantic_disposition_missing"
    assert exc.value.diagnostics[0].owner == "custom"
    assert "NONSEMANTIC_DISPOSITION" in exc.value.diagnostics[0].fix_hint


@pytest.mark.parametrize(
    ("old_identity", "new_identity", "expected_code"),
    [
        (123, "title", "semantic_required_field_invalid"),
        ([], "title", "semantic_required_field_invalid"),
        ("", "title", "semantic_required_field_missing"),
        ("doi", [], "semantic_required_field_invalid"),
    ],
)
def test_catalog_migration_identity_details_are_explicit_strings(
    old_identity: object,
    new_identity: object,
    expected_code: str,
) -> None:
    asset = _semantic_asset(
        "migration-owner",
        "migration",
        "catalog-migration",
        {
            "migration": {
                "id": "migration-v2",
                "evolution": "compatible",
                "old_identity": old_identity,
                "new_identity": new_identity,
            }
        },
        14,
    )
    with pytest.raises(SemanticCompositionError) as exc:
        compose_semantic_catalog(_resolution([asset]), deployment_name="migration")
    diagnostic = exc.value.diagnostics[0]
    assert diagnostic.code == expected_code
    assert diagnostic.owner == "migration-owner"
    assert diagnostic.asset_id == "migration"
    assert diagnostic.fix_hint


def test_reverse_asset_and_semantic_row_order_is_canonical() -> None:
    assets = _compatible_assets()
    raw = yaml.safe_load((FIXTURES / "compatible.yaml").read_text())
    reversed_assets = []
    for item in reversed(raw["assets"]):
        cloned = dict(item)
        compatibility = dict(cloned.get("compatibility", {}))
        semantic = dict(compatibility.get("semantic", {}))
        subtypes = semantic.get("subtypes")
        if isinstance(subtypes, dict):
            semantic["subtypes"] = dict(reversed(list(subtypes.items())))
        compatibility["semantic"] = semantic
        cloned["compatibility"] = compatibility
        reversed_assets.append(_asset(cloned))
    first = compose_semantic_catalog(_resolution(assets), deployment_name="semantic-product")
    second = compose_semantic_catalog(_resolution(reversed_assets), deployment_name="semantic-product")
    assert first.composition_digest == second.composition_digest
    assert first.to_dict() == second.to_dict()


def test_semantic_payload_and_rows_fail_closed_with_stable_diagnostics() -> None:
    missing_payload = TypedAsset(
        owner="owner-a",
        id="missing",
        kind="ontology",
        plane="semantic-catalog",
        version="v1",
        digest=_digest(61),
        schema="okg.deployment_product.asset.ontology/v1",
        merge_identity="missing",
        merge_rule="refuse",
        compatibility={"contract": "v1"},
    )
    with pytest.raises(SemanticCompositionError) as missing:
        compose_semantic_catalog(_resolution([missing_payload]))
    assert missing.value.diagnostics[0].to_dict() == {
        "code": "semantic_payload_missing",
        "owner": "owner-a",
        "asset_id": "missing",
        "qualified_id": "owner-a:missing",
        "message": "semantic asset 'owner-a:missing' must declare compatibility.semantic as a mapping",
        "fix_hint": "add an explicit compatibility.semantic mapping; do not rely on generic compatibility fields",
        "expected": None,
        "observed": None,
        "phase": "pre_mutation",
    }
    unknown_field = _semantic_asset(
        "owner-a", "unknown", "ontology", {"module_id": "owner-a:unknown", "subtypes": [], "typo": True}, 62
    )
    with pytest.raises(SemanticCompositionError) as unknown:
        compose_semantic_catalog(_resolution([unknown_field]))
    assert unknown.value.diagnostics[0].code == "semantic_payload_unknown_field"
    assert unknown.value.diagnostics[0].owner == "owner-a"
    assert unknown.value.diagnostics[0].fix_hint


@pytest.mark.postgres
def test_malformed_concept_seed_refuses_before_catalog_generation_or_graph_mutation(
    clean_runtime: str,
) -> None:
    assets = [
        _semantic_asset(
            "owner-a",
            "module",
            "ontology",
            {"module_id": "owner-a:module", "subtypes": {"Paper": {}}},
            71,
        ),
        _semantic_asset(
            "owner-a",
            "seed",
            "concept-seed",
            {"seeds": [{"id": "paper-seed", "name": "Paper", "description": "missing node identity"}]},
            72,
        ),
    ]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        before = _state(conn)
        with pytest.raises(SemanticCompositionError) as exc:
            compose_semantic_catalog(_resolution(assets), deployment_name="semantic-product")
        assert exc.value.diagnostics[0].code == "semantic_required_field_missing"
        assert exc.value.diagnostics[0].owner == "owner-a"
        assert "node_id" in exc.value.diagnostics[0].message
        assert _state(conn) == before
