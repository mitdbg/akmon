"""Deterministic, secret-free inspection of deployment-product composition.

This module is deliberately read-only.  It adapts the Wave 2 typed graph and
semantic catalog objects into one stable JSON-shaped report used by operator
diagnostics and PACT evidence.  It does not introduce a lock or catalog
authority and never serializes declarative values which may contain secrets.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import hashlib
import json
from typing import Any

from okg.substrate.contracts.deployment_product.typed_assets import (
    AssetDiagnostic,
    AssetProvenance,
    AssetReference,
    AssetResolution,
    TypedAsset,
    TypedAssetResolutionError,
)

from .deployment_product_composition import (
    SemanticCatalog,
    SemanticCompositionError,
    SemanticDiagnostic,
    compose_semantic_catalog,
)


INSPECTION_SCHEMA = "okg.deployment_product.composition_inspection/v1"
REFUSAL_SCHEMA = "okg.deployment_product.composition_refusal/v1"


class CompositionInspectionError(ValueError):
    """The supplied semantic catalog is not coherent with its resolution."""


def _jsonable(value: object) -> object:
    """Convert immutable/mapping-proxy values into canonical JSON values."""

    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_jsonable(item) for item in value]
    if isinstance(value, set):
        return sorted((_jsonable(item) for item in value), key=repr)
    return value


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        _jsonable(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _digest(value: object) -> str:
    return "sha256:" + hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _reference(value: AssetReference | Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(value, AssetReference):
        return {
            "owner": value.owner,
            "asset_id": value.asset_id,
            "kind": value.kind,
            "version": value.version,
            "digest": value.digest,
            "cardinality": value.cardinality,
            "required_capabilities": list(value.required_capabilities),
        }
    raw = dict(value)
    return {
        "owner": raw.get("owner"),
        "asset_id": raw.get("asset_id", raw.get("id")),
        "kind": raw.get("kind"),
        "version": raw.get("version"),
        "digest": raw.get("digest"),
        "cardinality": raw.get("cardinality"),
        "required_capabilities": sorted(raw.get("required_capabilities", ())),
    }


def _reference_chain(asset: TypedAsset) -> list[dict[str, Any]]:
    return [_reference(item) for item in asset.references]


def _reference_paths(asset: TypedAsset, assets: Mapping[str, TypedAsset]) -> list[list[str]]:
    """Expand the resolved DAG into stable owner-qualified reference paths."""

    paths: list[list[str]] = []

    def visit(current: TypedAsset, path: list[str]) -> None:
        for reference in current.references:
            next_path = [*path, reference.qualified_id]
            paths.append(next_path)
            target = assets.get(reference.qualified_id)
            if target is not None and reference.qualified_id not in path:
                visit(target, next_path)

    visit(asset, [asset.qualified_id])
    return sorted(paths, key=tuple)


def _asset_record(asset: TypedAsset, assets: Mapping[str, TypedAsset]) -> dict[str, Any]:
    """Return closed metadata only; compatibility values are never copied."""

    slots: list[dict[str, Any]] = []
    for slot in sorted(asset.override_slots, key=lambda item: item.qualified_id):
        slots.append(
            {
                "qualified_id": slot.qualified_id,
                "owner": slot.owner,
                "slot_id": slot.slot_id,
                "value_type": slot.value_type,
                "schema_digest": _digest(slot.schema),
                "has_default": slot.has_default,
                "default_digest": _digest(slot.default) if slot.has_default else None,
            }
        )
    return {
        "qualified_id": asset.qualified_id,
        "owner": asset.owner,
        "asset_id": asset.asset_id,
        "kind": asset.kind,
        "plane": asset.plane,
        "schema": asset.schema,
        "version": asset.version,
        "content_digest": asset.digest,
        "references": _reference_chain(asset),
        "reference_chains": _reference_paths(asset, assets),
        "capabilities": list(asset.capabilities),
        "compatibility": {
            "keys": sorted(str(key) for key in asset.compatibility),
            "digest": _digest(asset.compatibility),
        },
        "compatibility_digest": _digest(asset.compatibility),
        "merge_identity": asset.merge_identity,
        "merge_rule": asset.merge_rule,
        "failure_rule": asset.failure_rule,
        "override_slots": slots,
        "state": asset.state,
        "declared_provenance_origin": asset.provenance.get("source", "local"),
    }


def _provenance_record(item: AssetProvenance) -> dict[str, Any]:
    """Keep provenance history while digesting free-form reason text."""

    return {
        "path": item.path,
        "owner": item.owner,
        "source": item.source,
        "reference": _reference(item.reference),
        "value_digest": item.value_digest,
        "old_owner": item.old_owner,
        "new_owner": item.new_owner,
        "old_value_digest": item.old_value_digest,
        "new_value_digest": item.new_value_digest,
        "reason_digest": _digest(item.reason) if item.reason is not None else None,
        "references": [_reference(ref) for ref in item.references],
        "merge_decision": item.merge_decision,
    }


def _override_record(item: object) -> dict[str, Any]:
    raw = item.to_dict()  # OverrideDecision is intentionally adapted, not copied.
    return {
        "slot_id": raw.get("slot_id"),
        "old_owner": raw.get("old_owner"),
        "new_owner": raw.get("new_owner"),
        "old_value_digest": raw.get("old_value_digest"),
        "new_value_digest": raw.get("new_value_digest"),
        "reason_digest": _digest(raw["reason"]) if raw.get("reason") is not None else None,
        "references": sorted(str(value) for value in raw.get("references", ())),
    }


_SAFE_SEMANTIC_KEYS: dict[str, frozenset[str]] = {
    "alias": frozenset({"id", "alias", "canonical", "scope", "compatibility_version"}),
    "identifier_pattern": frozenset({"id", "id_type", "regex", "validator_kind", "compatibility_version"}),
    "concept_seed": frozenset({"id", "name", "node_id", "compatibility_version"}),
    "identity_policy": frozenset({"id", "semantic_id", "subtype", "subtype_name", "target", "fields", "cardinality", "evolution", "compatibility_version"}),
    "catalog_migration": frozenset({"id", "semantic_id", "migration_id", "old_identity", "new_identity", "evolution", "compatibility_version"}),
}


def _safe_definition(kind: str, row: Mapping[str, Any]) -> dict[str, Any]:
    allowed = _SAFE_SEMANTIC_KEYS.get(kind, frozenset())
    return {key: _jsonable(row[key]) for key in sorted(allowed & set(row))}


def _semantic_asset_lookup(resolution: AssetResolution) -> dict[str, TypedAsset]:
    return {asset.qualified_id: asset for asset in resolution.effective_assets}


def _source_fields(
    asset: TypedAsset,
    assets: Mapping[str, TypedAsset],
) -> dict[str, Any]:
    """Attach source-asset merge/provenance context to every semantic row."""

    return {
        "source_asset": asset.qualified_id,
        "asset_digest": asset.digest,
        "references": _reference_chain(asset),
        "reference_chains": _reference_paths(asset, assets),
        "merge_identity": asset.merge_identity,
        "merge_rule": asset.merge_rule,
        "failure_rule": asset.failure_rule,
        "declared_provenance_origin": asset.provenance.get("source", "local"),
    }


def _source_record(
    record: dict[str, Any],
    asset: TypedAsset,
    assets: Mapping[str, TypedAsset],
) -> dict[str, Any]:
    """Merge source context into a semantic definition record."""

    return {**record, **_source_fields(asset, assets)}


def _require_source(asset: TypedAsset | None, context: str) -> TypedAsset:
    if asset is None:
        raise CompositionInspectionError(
            f"semantic inspection source asset is missing for {context}"
        )
    return asset


def _require_source_kind(
    asset: TypedAsset | None,
    expected: set[str] | frozenset[str],
    context: str,
) -> TypedAsset:
    source = _require_source(asset, context)
    if source.kind not in expected:
        raise CompositionInspectionError(
            f"semantic inspection source asset {source.qualified_id!r} has kind "
            f"{source.kind!r}; expected {sorted(expected)} for {context}"
        )
    return source


def _semantic_definition(
    *,
    kind: str,
    row: Mapping[str, Any],
    owner: str,
    asset_id: str,
    asset: TypedAsset,
    assets: Mapping[str, TypedAsset],
) -> dict[str, Any]:
    safe = _safe_definition(kind, row)
    return _source_record({
        "definition_kind": kind,
        "definition_id": str(row.get("id", row.get("semantic_id", row.get("name", "")))),
        "owner": owner,
        "asset_id": asset_id,
        "asset_qualified_id": asset.qualified_id,
        "definition_digest": _digest(row),
        "metadata": safe,
    }, asset, assets)


def _semantic_records(catalog: SemanticCatalog, resolution: AssetResolution) -> list[dict[str, Any]]:
    lookup = _semantic_asset_lookup(resolution)
    records: list[dict[str, Any]] = []

    # Module rows are recovered from the closed semantic payload only to expose
    # ownership.  The catalog remains the authoritative apply representation.
    module_assets: dict[str, TypedAsset] = {}
    for asset in resolution.effective_assets:
        if asset.kind not in {"ontology", "schema"}:
            continue
        semantic = asset.compatibility.get("semantic")
        if isinstance(semantic, Mapping):
            module = semantic.get("module_id", semantic.get("module", semantic.get("semantic_module_id")))
            if isinstance(module, str) and module:
                module_assets[module] = asset
    for module in sorted(catalog.catalog.modules):
        asset = _require_source_kind(
            module_assets.get(module), {"ontology", "schema"}, f"module {module!r}"
        )
        records.append(_source_record(
            {
                "definition_kind": "module",
                "definition_id": module,
                "owner": asset.owner,
                "asset_id": asset.asset_id,
                "asset_qualified_id": asset.qualified_id,
                "definition_digest": _digest({"module": module}),
                "metadata": {"module_id": module},
            },
            asset,
            lookup,
        ))

    for subtype in catalog.subtypes:
        source = lookup.get(f"{subtype.owner}:{subtype.asset_id}")
        source = _require_source_kind(
            source, {"ontology", "schema"}, f"subtype {subtype.name!r}"
        )
        definitions = subtype.metadata.get("attribute_definitions", {})
        attr_digests = {
            str(name): _digest(value)
            for name, value in sorted(definitions.items())
        } if isinstance(definitions, Mapping) else {}
        safe_identity = {
            key: _jsonable(subtype.identity[key])
            for key in ("fields", "cardinality", "evolution")
            if key in subtype.identity
        }
        safe = {
            "name": subtype.name,
            "attributes": list(subtype.attributes),
            "attribute_definition_digests": attr_digests,
            "parent": subtype.parent,
            "identity": safe_identity,
            "identity_digest": _digest(subtype.identity),
            "semantic_module": subtype.metadata.get("semantic_module"),
            "annotation_keys": sorted(subtype.metadata.get("annotations", {}))
            if isinstance(subtype.metadata.get("annotations"), Mapping) else [],
            "annotation_digests": {
                str(key): _digest(value)
                for key, value in sorted(subtype.metadata.get("annotations", {}).items())
            } if isinstance(subtype.metadata.get("annotations"), Mapping) else {},
            "archetype_digest": _digest(subtype.metadata["archetype"])
            if "archetype" in subtype.metadata else None,
            "metadata_keys": sorted(subtype.metadata.get("metadata", {}))
            if isinstance(subtype.metadata.get("metadata"), Mapping) else [],
            "metadata_digest": _digest(subtype.metadata.get("metadata"))
            if "metadata" in subtype.metadata else None,
        }
        full_subtype = {
            "name": subtype.name,
            "attributes": _jsonable(definitions),
            "parent": subtype.parent,
            "identity": _jsonable(subtype.identity),
            "metadata": _jsonable(subtype.metadata),
        }
        records.append(_source_record({
            "definition_kind": "subtype",
            "definition_id": subtype.name,
            "owner": subtype.owner,
            "asset_id": subtype.asset_id,
            "asset_qualified_id": source.qualified_id,
            "definition_digest": _digest(full_subtype),
            "metadata": safe,
        }, source, lookup))

        # Keep attribute declarations and inheritance edges as independent
        # inspection records.  The subtype record retains the compact summary
        # above, while these rows make ownership and definition digests
        # addressable without exposing raw attribute values.
        for name in sorted(attr_digests):
            attribute_safe = {
                "subtype": subtype.name,
                "attribute": name,
                "attribute_definition_digest": attr_digests[name],
            }
            records.append(_source_record({
                "definition_kind": "attribute",
                "definition_id": f"{subtype.name}.{name}",
                "owner": subtype.owner,
                "asset_id": subtype.asset_id,
                "asset_qualified_id": source.qualified_id,
                "definition_digest": _digest({
                    "subtype": subtype.name,
                    "attribute": name,
                    "definition": _jsonable(definitions[name]),
                }),
                "metadata": attribute_safe,
            }, source, lookup))

        annotations = subtype.metadata.get("annotations")
        if isinstance(annotations, Mapping):
            for key, value in sorted(annotations.items(), key=lambda item: str(item[0])):
                annotation_safe = {
                    "subtype": subtype.name,
                    "annotation": str(key),
                    "value_digest": _digest(value),
                }
                records.append(_source_record({
                    "definition_kind": "annotation",
                    "definition_id": f"{subtype.name}.{key}",
                    "owner": subtype.owner,
                    "asset_id": subtype.asset_id,
                    "asset_qualified_id": source.qualified_id,
                    "definition_digest": _digest({"subtype": subtype.name, "annotation": key, "value": _jsonable(value)}),
                    "metadata": annotation_safe,
                }, source, lookup))
        if "archetype" in subtype.metadata:
            archetype_safe = {
                "subtype": subtype.name,
                "value_digest": _digest(subtype.metadata["archetype"]),
            }
            records.append(_source_record({
                "definition_kind": "archetype",
                "definition_id": f"{subtype.name}.archetype",
                "owner": subtype.owner,
                "asset_id": subtype.asset_id,
                "asset_qualified_id": source.qualified_id,
                "definition_digest": _digest({"subtype": subtype.name, "archetype": _jsonable(subtype.metadata["archetype"])}),
                "metadata": archetype_safe,
            }, source, lookup))

    subtype_sources = {
        subtype.name: lookup.get(f"{subtype.owner}:{subtype.asset_id}")
        for subtype in catalog.subtypes
    }
    subtype_by_name = {subtype.name: subtype for subtype in catalog.subtypes}
    for subtype in catalog.subtypes:
        if not subtype.parent:
            continue
        source = subtype_sources.get(subtype.name)
        parent = subtype_by_name.get(subtype.parent)
        parent_source = subtype_sources.get(subtype.parent)
        source = _require_source_kind(
            source, {"ontology", "schema"}, f"inheritance child {subtype.name!r}"
        )
        if parent is None:
            raise CompositionInspectionError(
                f"semantic inspection parent subtype is missing for {subtype.name!r}->{subtype.parent!r}"
            )
        parent_source = _require_source_kind(
            parent_source,
            {"ontology", "schema"},
            f"inheritance parent {subtype.parent!r}",
        )
        inheritance_safe = {
            "child": subtype.name,
            "parent": subtype.parent,
            "parent_owner": parent.owner if parent else None,
            "parent_asset_id": parent.asset_id if parent else None,
        }
        records.append(_source_record({
            "definition_kind": "inheritance",
            "definition_id": f"{subtype.name}->{subtype.parent}",
            "owner": subtype.owner,
            "asset_id": subtype.asset_id,
            "asset_qualified_id": source.qualified_id,
            "parent_asset_qualified_id": parent_source.qualified_id,
            "parent_asset_digest": parent_source.digest,
            "definition_digest": _digest(inheritance_safe),
            "metadata": inheritance_safe,
        }, source, lookup))

    for narrowing in catalog.narrowings:
        source = lookup.get(f"{narrowing.owner}:{narrowing.asset_id}")
        source = _require_source_kind(
            source, {"bridge", "narrowing"}, f"narrowing {narrowing.triple!r}"
        )
        safe = {
            "parent": narrowing.parent_subtype,
            "edge": narrowing.edge_type,
            "child": narrowing.child_subtype,
            "cardinality": narrowing.cardinality,
            "endpoints": list(narrowing.endpoints),
        }
        records.append(_source_record({
            "definition_kind": "narrowing",
            "definition_id": ":".join(narrowing.triple),
            "owner": narrowing.owner,
            "asset_id": narrowing.asset_id,
            "asset_qualified_id": source.qualified_id,
            "definition_digest": _digest(safe),
            "metadata": safe,
        }, source, lookup))

    for kind, rows in (
        ("alias", catalog.aliases),
        ("identifier_pattern", catalog.identifier_patterns),
        ("concept_seed", catalog.concept_seeds),
        ("identity_policy", catalog.identity_policies),
        ("catalog_migration", catalog.migrations),
    ):
        for row in rows:
            raw = dict(row)
            owner = str(raw.get("owner", ""))
            asset_id = str(raw.get("asset_id", ""))
            source = lookup.get(f"{owner}:{asset_id}")
            expected_kind = {
                "alias": {"alias"},
                "identifier_pattern": {"identifier-pattern"},
                "concept_seed": {"concept-seed"},
                "identity_policy": {"identity-policy"},
                "catalog_migration": {"catalog-migration"},
            }[kind]
            source = _require_source_kind(
                source,
                expected_kind,
                f"{kind} {raw.get('id', raw.get('semantic_id', ''))!r}",
            )
            records.append(
                _semantic_definition(
                    kind=kind,
                    row=raw,
                    owner=owner,
                    asset_id=asset_id,
                    asset=source,
                    assets=lookup,
                )
            )
    return sorted(records, key=lambda item: (item["definition_kind"], item["definition_id"], item.get("owner") or ""))


def _validate_catalog_binding(
    resolution: AssetResolution,
    catalog: SemanticCatalog,
) -> None:
    """Refuse a plan that was composed from different semantic content."""

    expected = compose_semantic_catalog(
        resolution,
        deployment_name=catalog.deployment_name,
    )
    if tuple(expected.semantic_asset_ids) != tuple(catalog.semantic_asset_ids):
        raise CompositionInspectionError(
            "semantic catalog source asset ID set does not match the supplied "
            f"resolution: expected {list(expected.semantic_asset_ids)!r}, "
            f"observed {list(catalog.semantic_asset_ids)!r}"
        )
    if expected.composition_digest != catalog.composition_digest:
        raise CompositionInspectionError(
            "semantic catalog composition digest does not match the supplied "
            "resolution content: expected "
            f"{expected.composition_digest}, observed {catalog.composition_digest}"
        )
    if _canonical_bytes(expected.to_dict()) != _canonical_bytes(catalog.to_dict()):
        raise CompositionInspectionError(
            "semantic catalog payload does not match the supplied resolution "
            "or contains stale/tampered composition rows"
        )


@dataclass(frozen=True, slots=True)
class CompositionInspection:
    """Canonical read-only composition report."""

    deployment: str
    typed_assets: tuple[Mapping[str, Any], ...]
    semantic_definitions: tuple[Mapping[str, Any], ...]
    provenance: tuple[Mapping[str, Any], ...]
    merge_decisions: tuple[Mapping[str, Any], ...]
    overrides: tuple[Mapping[str, Any], ...]
    lock_paths: tuple[Mapping[str, Any], ...]
    catalog_identity: Mapping[str, Any]
    schema: str = INSPECTION_SCHEMA

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "deployment": self.deployment,
            "typed_assets": [_jsonable(item) for item in self.typed_assets],
            "semantic_definitions": [_jsonable(item) for item in self.semantic_definitions],
            "provenance": [_jsonable(item) for item in self.provenance],
            "merge_decisions": [_jsonable(item) for item in self.merge_decisions],
            "overrides": [_jsonable(item) for item in self.overrides],
            "lock_paths": [_jsonable(item) for item in self.lock_paths],
            "catalog_identity": _jsonable(self.catalog_identity),
        }

    def canonical_bytes(self) -> bytes:
        return _canonical_bytes(self.to_dict())

    @property
    def composition_digest(self) -> str:
        return _digest(self.to_dict())


@dataclass(frozen=True, slots=True)
class CompositionRefusal:
    """Stable normalization of typed and semantic pre-mutation refusals."""

    stage: str
    diagnostics: tuple[Mapping[str, Any], ...]
    schema: str = REFUSAL_SCHEMA
    mutation_state: str = "pre_mutation"

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "status": "refused",
            "stage": self.stage,
            "phase": "pre_mutation",
            "mutation_state": self.mutation_state,
            "diagnostics": [_jsonable(item) for item in self.diagnostics],
        }

    def canonical_bytes(self) -> bytes:
        return _canonical_bytes(self.to_dict())

    @property
    def refusal_digest(self) -> str:
        return _digest(self.to_dict())


def _typed_diagnostic(item: AssetDiagnostic) -> dict[str, Any]:
    source = item.to_dict()
    return {
        "stage": "typed_asset_resolution",
        "phase": "pre_mutation",
        "code": item.code,
        "owner": item.owner,
        "asset_id": item.asset_id,
        "chain": list(item.chain),
        "path": item.path or None,
        "expected": item.expected,
        "observed": item.observed,
        "message": item.message,
        "fix_action": item.fix_hint,
        "source": source,
    }


def _semantic_diagnostic(item: SemanticDiagnostic) -> dict[str, Any]:
    source = item.to_dict()
    return {
        "stage": "semantic_catalog_composition",
        "phase": "pre_mutation",
        "code": item.code,
        "owner": item.owner,
        "asset_id": item.asset_id,
        "chain": [],
        "path": None,
        "expected": item.expected,
        "observed": item.observed,
        "message": item.message,
        "fix_action": item.fix_hint,
        "source": source,
    }


def inspect_refusal(error: Exception) -> CompositionRefusal:
    """Normalize either Wave 2 refusal class without inventing chains."""

    if isinstance(error, TypedAssetResolutionError):
        diagnostics = tuple(_typed_diagnostic(item) for item in error.diagnostics)
        return CompositionRefusal("typed_asset_resolution", diagnostics)
    if isinstance(error, SemanticCompositionError):
        diagnostics = tuple(_semantic_diagnostic(item) for item in error.diagnostics)
        return CompositionRefusal("semantic_catalog_composition", diagnostics)
    raise TypeError("error must be TypedAssetResolutionError or SemanticCompositionError")


def inspect_composition(
    resolution: AssetResolution,
    catalog: SemanticCatalog | None = None,
    *,
    deployment: str | None = None,
    catalog_version_id: str | int | None = None,
    generation_id: str | None = None,
) -> CompositionInspection:
    """Build a stable secret-free inspection report over resolved objects."""

    name = deployment or (catalog.deployment_name if catalog else "deployment")
    # Derive records first so source/coherence errors retain their precise
    # missing/kind diagnostics before catalog-binding comparisons classify
    # cross-resolution drift or stale row tampering.
    semantic = _semantic_records(catalog, resolution) if catalog else []
    if catalog is not None:
        _validate_catalog_binding(resolution, catalog)
    identity = {
        "deployment": name,
        "semantic_composition_digest": catalog.composition_digest if catalog else None,
        "typed_composition_digest": resolution.composition_digest,
        "catalog_version_id": str(catalog_version_id) if catalog_version_id is not None else None,
        "generation_id": generation_id,
    }
    all_assets = {
        asset.qualified_id: asset
        for asset in (*resolution.effective_assets, *resolution.disabled_assets, *resolution.tombstones)
    }
    typed = tuple(
        _asset_record(asset, all_assets)
        for asset in sorted(all_assets.values(), key=lambda item: item.qualified_id)
    )
    provenance = tuple(_provenance_record(item) for item in sorted(resolution.provenance, key=lambda item: item.path))
    overrides = tuple(_override_record(item) for item in sorted(resolution.overrides, key=lambda item: item.slot.qualified_id))
    lock_paths = tuple(
        {
            "path": item.path,
            "owner": item.owner,
            "source": item.source,
            "reference": _reference(item.reference),
            "value_digest": item.value_digest,
        }
        for item in sorted(resolution.lock_provenance(), key=lambda item: item.path)
    )
    return CompositionInspection(
        deployment=name,
        typed_assets=typed,
        semantic_definitions=tuple(semantic),
        provenance=provenance,
        merge_decisions=tuple(
            sorted(
                ({str(key): _jsonable(value) for key, value in dict(item).items()} for item in resolution.merge_decisions),
                key=lambda item: (str(item.get("kind", "")), str(item.get("merge_identity", ""))),
            )
        ),
        overrides=overrides,
        lock_paths=lock_paths,
        catalog_identity=identity,
    )


def composition_inspection_bytes(value: CompositionInspection | Mapping[str, Any]) -> bytes:
    """Return canonical bytes for an inspection or equivalent mapping."""

    return value.canonical_bytes() if isinstance(value, CompositionInspection) else _canonical_bytes(value)


def composition_inspection_digest(value: CompositionInspection | Mapping[str, Any]) -> str:
    return "sha256:" + hashlib.sha256(composition_inspection_bytes(value)).hexdigest()


__all__ = [
    "CompositionInspection",
    "CompositionInspectionError",
    "CompositionRefusal",
    "INSPECTION_SCHEMA",
    "REFUSAL_SCHEMA",
    "composition_inspection_bytes",
    "composition_inspection_digest",
    "inspect_composition",
    "inspect_refusal",
]
