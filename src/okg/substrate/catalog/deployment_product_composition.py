"""Pure semantic-catalog composition for resolved deployment products.

The typed-asset resolver is deliberately agnostic about the *meaning* of a
semantic asset.  This module is the narrow adapter between that resolver and
the existing catalog lifecycle.  It reads only the declarative ``semantic``
mapping in a selected asset's compatibility metadata and produces the
existing :class:`~okg.substrate.catalog.modules_compose.ComposedCatalog`.

Composition never imports an asset, discovers files, or touches a database.
Call :func:`apply_semantic_catalog` only after composition succeeds; the
function delegates to the established transactional catalog apply path.

The supported declarative shape is intentionally small and JSON-compatible::

    compatibility:
      semantic:
        module_id: library:papers
        subtypes:
          Paper:
            attributes: [title]
            identity: {fields: [doi], cardinality: one}
            parent: Artifact
        narrowings:
          - {parent: Paper, edge: cites, child: Paper,
             cardinality: many}

``subtypes`` may also be a list of strings.  Bridges use ``module_id`` and
``endpoint_modules`` (or ``source_module``/``target_module``) and carry the
same narrowing shape.  Aliases, identifier patterns, concept seeds, identity
policies, and catalog migrations are independent lists in the semantic
mapping.  The parser accepts their already-canonical row dictionaries so the
legacy ``ComposedCatalog`` apply path remains the sole writer.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
import hashlib
import json
import re
from typing import Any

from okg.substrate.catalog.modules_compose import ComposedCatalog, NarrowingRow
from okg.substrate.contracts.deployment_product.typed_assets import (
    AssetResolution,
    CORE_ASSET_KINDS,
    TypedAsset,
)


SEMANTIC_ASSET_KINDS = frozenset(
    {
        "ontology",
        "schema",
        "bridge",
        "narrowing",
        "alias",
        "identifier-pattern",
        "concept-seed",
        "identity-policy",
        "catalog-migration",
    }
)

EXECUTABLE_ASSET_KINDS = frozenset(
    {
        "connector",
        "connection-type",
        "source-template",
        "source-binding",
        "extraction-pipeline",
        "link-policy",
        "source",
        "enricher",
        "view",
        "read-model",
        "lens",
        "dashboard",
        "prompt",
        "tool",
        "resolver",
        "search",
        "service",
        "consistency-gate",
        "derived-producer",
        "skill",
    }
)

# Every nonsemantic kind has a deliberate handoff disposition.  These are
# intentionally inert in this stage: composition records the selected asset
# for Wave 3/4 authorities but never installs or executes it.  Keeping the
# complete map next to the closed kind vocabulary prevents a new kind from
# silently falling through an implicit branch.
NONSEMANTIC_DISPOSITION: dict[str, str] = {
    "connector": "operational-handoff",
    "connection-type": "operational-handoff",
    "source-template": "operational-handoff",
    "source-binding": "operational-handoff",
    "extraction-pipeline": "executable-handoff",
    "link-policy": "declarative-handoff",
    "source": "executable-handoff",
    "embedding": "operational-handoff",
    "refresher": "operational-handoff",
    "enricher": "executable-handoff",
    "invariant": "declarative-handoff",
    "consistency-gate": "executable-handoff",
    "derived-producer": "executable-handoff",
    "view": "executable-handoff",
    "read-model": "executable-handoff",
    "lens": "executable-handoff",
    "dashboard": "executable-handoff",
    "interface": "declarative-handoff",
    "prompt": "declarative-handoff",
    "skill": "executable-handoff",
    "playbook": "declarative-handoff",
    "tool": "executable-handoff",
    "resolver": "executable-handoff",
    "search": "executable-handoff",
    "chat-policy": "declarative-handoff",
    "vocabulary": "declarative-handoff",
    "policy": "declarative-handoff",
    "nomos": "declarative-handoff",
    "readiness-policy": "declarative-handoff",
    "doctor-policy": "declarative-handoff",
    "service": "operational-handoff",
    "storage-requirement": "declarative-handoff",
    "release-policy": "declarative-handoff",
    "conformance-policy": "declarative-handoff",
}

_KNOWN_NONSEMANTIC_KINDS = frozenset(NONSEMANTIC_DISPOSITION)
if _KNOWN_NONSEMANTIC_KINDS != frozenset(CORE_ASSET_KINDS) - SEMANTIC_ASSET_KINDS:
    raise RuntimeError("semantic composition disposition map is incomplete")

_PAYLOAD_KEYS: dict[str, frozenset[str]] = {
    "ontology": frozenset({
        "module_id", "module", "semantic_module_id", "subtypes", "semantic_subtypes",
    }),
    "schema": frozenset({
        "module_id", "module", "semantic_module_id", "subtypes", "semantic_subtypes",
    }),
    "bridge": frozenset({
        "endpoint_modules", "endpoints", "modules", "source_module",
        "from_module", "left_module", "target_module", "to_module", "right_module",
        "narrowings", "edges", "parent", "parent_subtype", "source_subtype",
        "src_subtype", "child", "child_subtype", "target_subtype", "dst_subtype",
        "edge", "edge_type", "edge_archetype", "cardinality", "target_cardinality",
    }),
    "narrowing": frozenset({
        "endpoint_modules", "endpoints", "modules", "source_module",
        "from_module", "left_module", "target_module", "to_module", "right_module",
        "narrowings", "edges", "parent", "parent_subtype", "source_subtype",
        "src_subtype", "child", "child_subtype", "target_subtype", "dst_subtype",
        "edge", "edge_type", "edge_archetype", "cardinality", "target_cardinality",
    }),
    "alias": frozenset({"aliases", "alias", "canonical", "scope", "id", "compatibility_version"}),
    "identifier-pattern": frozenset({
        "patterns", "identifier_patterns", "id", "id_type", "regex", "validator_kind",
        "description", "compatibility_version",
    }),
    "concept-seed": frozenset({
        "seeds", "concept_seeds", "id", "name", "description", "node_id",
        "compatibility_version",
    }),
    "identity-policy": frozenset({
        "policies", "identity_policies", "id", "subtype", "subtype_name", "target",
        "fields", "cardinality", "evolution", "compatibility_version",
    }),
    "catalog-migration": frozenset({
        "migrations", "migration", "id", "migration_id", "old_identity", "new_identity",
        "evolution", "compatibility_version", "description",
    }),
}
_CONCEPT_NODE_RE = re.compile(r"^concept:[a-z0-9]+(?:_[a-z0-9]+)*$")


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _digest(value: object) -> str:
    return "sha256:" + hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _snake(value: object) -> str:
    text = str(value or "").strip()
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", text)
    return re.sub(r"[^A-Za-z0-9]+", "_", text).strip("_").lower()


def _mapping(value: object, *, field_name: str = "mapping", allow_none: bool = False) -> dict[str, Any]:
    if value is None and allow_none:
        return {}
    if not isinstance(value, Mapping):
        raise TypeError(f"{field_name} must be a mapping")
    return dict(value)


def _rows(
    value: object,
    *,
    field_name: str = "rows",
    single_row_keys: frozenset[str] = frozenset(),
) -> list[dict[str, Any]]:
    """Normalize a declarative map/list to deterministic row mappings."""

    if value is None:
        return []
    if isinstance(value, Mapping):
        if single_row_keys & set(value):
            return [dict(value)]
        rows: list[dict[str, Any]] = []
        for key, raw in sorted(value.items(), key=lambda item: str(item[0])):
            if not isinstance(raw, Mapping):
                raise TypeError(f"{field_name}[{key!s}] must be a mapping")
            rows.append({"id": str(key), **dict(raw)})
        return rows
    if isinstance(value, (str, bytes)) or not isinstance(value, Iterable):
        raise TypeError(f"{field_name} must be a list or mapping")
    result: list[dict[str, Any]] = []
    for raw in value:
        if isinstance(raw, str):
            raise TypeError(f"{field_name} entries must be mappings")
        elif isinstance(raw, Mapping):
            result.append(dict(raw))
        else:
            raise TypeError(f"{field_name} entries must be mappings")
    return result


def _semantic_payload(asset: TypedAsset) -> dict[str, Any]:
    raw = _mapping(asset.compatibility, field_name=f"{asset.qualified_id}.compatibility")
    semantic = raw.get("semantic")
    if not isinstance(semantic, Mapping):
        raise SemanticCompositionError((SemanticDiagnostic(
            code="semantic_payload_missing",
            owner=asset.owner,
            asset_id=asset.asset_id,
            message=f"semantic asset {asset.qualified_id!r} must declare compatibility.semantic as a mapping",
            fix_hint="add an explicit compatibility.semantic mapping; do not rely on generic compatibility fields",
        ),))
    return dict(semantic)


def _validated_payload(asset: TypedAsset) -> dict[str, Any]:
    """Load one closed semantic payload and turn shape errors into diagnostics."""

    try:
        payload = _semantic_payload(asset)
    except TypeError as exc:
        raise SemanticCompositionError((SemanticDiagnostic(
            code="semantic_payload_invalid",
            owner=asset.owner,
            asset_id=asset.asset_id,
            message=f"semantic asset {asset.qualified_id!r} has invalid payload: {exc}",
            fix_hint="use a mapping under compatibility.semantic and keep its fields in the closed kind vocabulary",
        ),)) from exc
    allowed = _PAYLOAD_KEYS[asset.kind]
    unknown = sorted(set(payload) - allowed)
    if unknown:
        raise SemanticCompositionError((SemanticDiagnostic(
            code="semantic_payload_unknown_field",
            owner=asset.owner,
            asset_id=asset.asset_id,
            message=f"semantic asset {asset.qualified_id!r} declares unknown field(s): {', '.join(unknown)}",
            fix_hint=f"remove unknown fields; allowed fields are {', '.join(sorted(allowed))}",
            expected=", ".join(sorted(allowed)),
            observed=", ".join(unknown),
        ),))
    return payload


@dataclass(frozen=True, slots=True)
class SemanticDiagnostic:
    """Stable, owner-qualified pre-mutation composition refusal."""

    code: str
    owner: str
    asset_id: str
    message: str
    fix_hint: str
    expected: str | None = None
    observed: str | None = None

    @property
    def qualified_id(self) -> str:
        return f"{self.owner}:{self.asset_id}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "owner": self.owner,
            "asset_id": self.asset_id,
            "qualified_id": self.qualified_id,
            "message": self.message,
            "fix_hint": self.fix_hint,
            "expected": self.expected,
            "observed": self.observed,
            "phase": "pre_mutation",
        }


class SemanticCompositionError(ValueError):
    """One or more semantic declarations cannot form a catalog."""

    def __init__(self, diagnostics: Sequence[SemanticDiagnostic]) -> None:
        ordered = tuple(
            sorted(
                diagnostics,
                key=lambda item: (item.code, item.owner, item.asset_id, item.message),
            )
        )
        if not ordered:
            raise ValueError("semantic composition errors need diagnostics")
        self.diagnostics = ordered
        self.code = ordered[0].code
        super().__init__("; ".join(item.message for item in ordered))


@dataclass(frozen=True, slots=True)
class SemanticSubtype:
    name: str
    owner: str
    asset_id: str
    attributes: tuple[str, ...] = ()
    parent: str | None = None
    identity: Mapping[str, Any] = field(default_factory=dict)
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @property
    def qualified_owner(self) -> str:
        return f"{self.owner}:{self.asset_id}"


@dataclass(frozen=True, slots=True)
class SemanticNarrowing:
    parent_subtype: str
    edge_type: str
    child_subtype: str
    owner: str
    asset_id: str
    cardinality: str | None = None
    endpoints: tuple[str, ...] = ()

    @property
    def triple(self) -> tuple[str, str, str]:
        return (self.parent_subtype, self.edge_type, self.child_subtype)


@dataclass(frozen=True, slots=True)
class SemanticCatalog:
    """Validated semantic composition and its legacy apply representation."""

    deployment_name: str
    catalog: ComposedCatalog
    subtypes: tuple[SemanticSubtype, ...]
    narrowings: tuple[SemanticNarrowing, ...]
    aliases: tuple[Mapping[str, Any], ...] = ()
    identifier_patterns: tuple[Mapping[str, Any], ...] = ()
    concept_seeds: tuple[Mapping[str, Any], ...] = ()
    identity_policies: tuple[Mapping[str, Any], ...] = ()
    migrations: tuple[Mapping[str, Any], ...] = ()
    semantic_asset_ids: tuple[str, ...] = ()
    executable_asset_ids: tuple[str, ...] = ()
    nonsemantic_asset_ids: tuple[str, ...] = ()
    nonsemantic_dispositions: tuple[Mapping[str, str], ...] = ()
    composition_digest: str = ""

    @property
    def composed_catalog(self) -> ComposedCatalog:
        """Compatibility alias for callers that name the apply payload."""

        return self.catalog

    def to_dict(self) -> dict[str, Any]:
        return {
            "deployment_name": self.deployment_name,
            "semantic_assets": list(self.semantic_asset_ids),
            "executable_assets_ignored": list(self.executable_asset_ids),
            "nonsemantic_assets_ignored": list(self.nonsemantic_asset_ids),
            "nonsemantic_dispositions": [dict(item) for item in self.nonsemantic_dispositions],
            "subtypes": [
                {
                    "name": item.name,
                    "owner": item.owner,
                    "asset_id": item.asset_id,
                    "attributes": list(item.attributes),
                    "parent": item.parent,
                    "identity": dict(item.identity),
                    "metadata": dict(item.metadata),
                }
                for item in self.subtypes
            ],
            "narrowings": [
                {
                    "parent": item.parent_subtype,
                    "edge": item.edge_type,
                    "child": item.child_subtype,
                    "owner": item.owner,
                    "asset_id": item.asset_id,
                    "cardinality": item.cardinality,
                    "endpoints": list(item.endpoints),
                }
                for item in self.narrowings
            ],
            "aliases": [dict(item) for item in self.aliases],
            "identifier_patterns": [dict(item) for item in self.identifier_patterns],
            "concept_seeds": [dict(item) for item in self.concept_seeds],
            "identity_policies": [dict(item) for item in self.identity_policies],
            "migrations": [dict(item) for item in self.migrations],
            "composition_digest": self.composition_digest,
        }


def _diag(
    diagnostics: list[SemanticDiagnostic],
    asset: TypedAsset,
    code: str,
    message: str,
    fix_hint: str,
    *,
    expected: str | None = None,
    observed: str | None = None,
) -> None:
    diagnostics.append(
        SemanticDiagnostic(
            code=code,
            owner=asset.owner,
            asset_id=asset.asset_id,
            message=message,
            fix_hint=fix_hint,
            expected=expected,
            observed=observed,
        )
    )


def _subtype_rows(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    value = _row_value(payload, "subtypes", "semantic_subtypes")
    return _rows(value, field_name="subtypes")


def _narrowing_rows(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    value = _row_value(payload, "narrowings", "edges")
    rows = _rows(value, field_name="narrowings")
    # A narrowing asset/bridge may describe exactly one row at top-level.
    if not rows and any(key in payload for key in ("parent", "parent_subtype", "source_subtype")):
        row_keys = {
            "id", "semantic_id", "parent", "parent_subtype", "source_subtype", "src_subtype",
            "child", "child_subtype", "target_subtype", "dst_subtype", "edge", "edge_type",
            "edge_archetype", "cardinality", "target_cardinality", "compatibility_version",
        }
        rows = [{key: payload[key] for key in row_keys if key in payload}]
    return rows


def _row_value(row: Mapping[str, Any], *names: str) -> Any:
    values = [(name, row[name]) for name in names if name in row]
    if values and any(value != values[0][1] for _, value in values[1:]):
        raise ValueError(
            f"aliases disagree for {', '.join(name for name, _ in values)}"
        )
    if values:
        return values[0][1]
    return None


def _contribution_rows(
    asset: TypedAsset,
    payload: Mapping[str, Any],
    *,
    container_keys: tuple[str, ...],
    direct_keys: frozenset[str],
    allowed_row_keys: frozenset[str],
    required: tuple[str, ...],
    diagnostics: list[SemanticDiagnostic],
) -> list[dict[str, Any]]:
    """Normalize and validate one independent semantic contribution family."""

    try:
        raw = _row_value(payload, *container_keys)
    except ValueError as exc:
        _diag(
            diagnostics,
            asset,
            "semantic_container_alias_disagreement",
            f"{asset.qualified_id!r} contribution containers disagree: {exc}",
            "make authored container aliases identical or keep only one container key",
        )
        return []
    if raw is None and direct_keys & set(payload):
        raw = payload
    try:
        rows = _rows(
            raw,
            field_name=f"{asset.qualified_id}.{container_keys[0]}",
            single_row_keys=direct_keys,
        )
    except (TypeError, ValueError) as exc:
        _diag(
            diagnostics,
            asset,
            "semantic_contribution_invalid",
            f"{asset.qualified_id!r} has an invalid {container_keys[0]} declaration: {exc}",
            "use a list of row mappings or one complete row mapping",
        )
        return []
    normalized: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        unknown = sorted(set(row) - allowed_row_keys)
        if unknown:
            _diag(
                diagnostics,
                asset,
                "semantic_contribution_unknown_field",
                f"{asset.qualified_id!r} {container_keys[0]} row {index} has unknown field(s): {', '.join(unknown)}",
                f"remove unknown fields; allowed fields are {', '.join(sorted(allowed_row_keys))}",
                expected=", ".join(sorted(allowed_row_keys)),
                observed=", ".join(unknown),
            )
            continue
        try:
            stable_id = _row_value(row, "id", "semantic_id", "migration_id")
            missing = [name for name in required if _row_value(row, name) in (None, "")]
        except ValueError as exc:
            _diag(diagnostics, asset, "semantic_contribution_alias_disagreement", f"{asset.qualified_id!r} {container_keys[0]} aliases disagree: {exc}", "make all authored aliases identical and publish the correction")
            continue
        if not isinstance(stable_id, str) or not stable_id.strip():
            _diag(
                diagnostics,
                asset,
                "semantic_stable_id_missing",
                f"{asset.qualified_id!r} {container_keys[0]} row {index} has no stable ID",
                "add an explicitly authored id/semantic_id to every contribution row",
            )
            continue
        if missing:
            _diag(
                diagnostics,
                asset,
                "semantic_required_field_missing",
                f"{asset.qualified_id!r} {container_keys[0]} row {stable_id!r} is missing: {', '.join(missing)}",
                "provide all fields required by the existing catalog apply lifecycle",
                expected=", ".join(required),
                observed=", ".join(sorted(set(row))),
            )
            continue
        compatibility_version = row.get("compatibility_version", asset.version)
        if not isinstance(compatibility_version, str) or not compatibility_version.strip():
            _diag(
                diagnostics,
                asset,
                "semantic_compatibility_version_missing",
                f"{asset.qualified_id!r} contribution {stable_id!r} lacks a compatibility version",
                "declare compatibility_version or use the typed asset version",
            )
            continue
        normalized.append({
            **dict(row),
            "id": str(stable_id),
            "semantic_id": str(stable_id),
            "compatibility_version": compatibility_version,
            "compatibility_version_source": (
                "row.compatibility_version"
                if "compatibility_version" in row
                else "asset.version"
            ),
        })
    return normalized


def _module_id(asset: TypedAsset, payload: Mapping[str, Any]) -> str:
    module = _row_value(payload, "module_id", "module", "semantic_module_id")
    if not isinstance(module, str) or not module.strip() or ":" not in module:
        raise ValueError("module_id must be one explicit owner-qualified module ID")
    return module.strip()


def _endpoint_modules(payload: Mapping[str, Any]) -> tuple[str, ...]:
    raw = _row_value(payload, "endpoint_modules", "endpoints", "modules")
    if isinstance(raw, Mapping):
        values = list(raw.values())
    elif isinstance(raw, (str, bytes)) or raw is None:
        values = [raw] if raw else []
    elif isinstance(raw, Iterable):
        values = list(raw)
    else:
        values = []
    values.extend(
        value
        for value in (
            _row_value(payload, "source_module", "from_module", "left_module"),
            _row_value(payload, "target_module", "to_module", "right_module"),
        )
        if value
    )
    if any(not isinstance(value, str) or not value.strip() for value in values):
        raise TypeError("endpoint module identities must be non-empty strings")
    return tuple(sorted({value.strip() for value in values}))


def _identity_for_subtype(payload: Mapping[str, Any], row: Mapping[str, Any]) -> dict[str, Any]:
    value = _row_value(row, "identity", "identity_policy")
    if value is None:
        value = _row_value(payload, "identity", "identity_policy")
    return _mapping(value, field_name="identity", allow_none=True)


def compose_semantic_catalog(
    resolution: AssetResolution,
    *,
    deployment_name: str = "deployment",
) -> SemanticCatalog:
    """Validate semantic assets and return a DB-free catalog plan.

    ``resolution`` is expected to be the already-validated typed graph.  The
    function deliberately ignores executable-plane assets, including when
    they happen to be present in the same effective graph; selecting a schema
    therefore cannot install a source, enricher, view, dashboard, prompt, or
    tool as a side effect.
    """

    semantic_assets = tuple(
        sorted(
            (asset for asset in resolution.effective_assets if asset.kind in SEMANTIC_ASSET_KINDS),
            key=lambda item: item.qualified_id,
        )
    )
    nonsemantic_assets = tuple(
        sorted(
            (asset for asset in resolution.effective_assets if asset.kind not in SEMANTIC_ASSET_KINDS),
            key=lambda item: item.qualified_id,
        )
    )
    unknown_dispositions = [
        asset for asset in nonsemantic_assets
        if asset.kind not in NONSEMANTIC_DISPOSITION
    ]
    if unknown_dispositions:
        raise SemanticCompositionError(tuple(
            SemanticDiagnostic(
                code="nonsemantic_disposition_missing",
                owner=asset.owner,
                asset_id=asset.asset_id,
                message=f"registered nonsemantic kind {asset.kind!r} has no explicit Wave 3 disposition",
                fix_hint="add the kind to the closed NONSEMANTIC_DISPOSITION map before selecting it",
                expected="closed nonsemantic disposition",
                observed=asset.kind,
            )
            for asset in sorted(unknown_dispositions, key=lambda item: item.qualified_id)
        ))
    executable_assets = tuple(
        asset for asset in nonsemantic_assets if asset.kind in EXECUTABLE_ASSET_KINDS
    )
    nonsemantic_dispositions = tuple(
        {
            "asset_id": asset.qualified_id,
            "kind": asset.kind,
            "disposition": NONSEMANTIC_DISPOSITION[asset.kind],
        }
        for asset in nonsemantic_assets
    )
    diagnostics: list[SemanticDiagnostic] = []
    modules: dict[str, TypedAsset] = {}
    subtypes: list[SemanticSubtype] = []
    subtype_by_name: dict[str, SemanticSubtype] = {}
    subtype_asset_by_name: dict[str, TypedAsset] = {}
    subtype_module_by_name: dict[str, str] = {}
    narrowings: list[SemanticNarrowing] = []
    aliases: list[Mapping[str, Any]] = []
    identifier_patterns: list[Mapping[str, Any]] = []
    concept_seeds: list[Mapping[str, Any]] = []
    identity_policies: list[Mapping[str, Any]] = []
    migrations: list[Mapping[str, Any]] = []

    # First establish module identities.  Endpoint resolution below can then
    # distinguish a missing endpoint from an ambiguous shorthand.
    for asset in semantic_assets:
        payload = _validated_payload(asset)
        if asset.kind in {"ontology", "schema"}:
            try:
                module = _module_id(asset, payload)
            except (TypeError, ValueError) as exc:
                _diag(diagnostics, asset, "module_identity_invalid", f"{asset.qualified_id!r} has invalid module identity: {exc}", "declare exactly one matching owner-qualified module_id/module/semantic_module_id")
                continue
            prior = modules.get(module)
            if prior is not None and prior.qualified_id != asset.qualified_id:
                _diag(
                    diagnostics,
                    asset,
                    "duplicate_semantic_ownership",
                    f"semantic module {module!r} is owned by both {prior.qualified_id!r} and {asset.qualified_id!r}",
                    "choose one owner-qualified module or publish an explicit catalog migration",
                    expected=prior.qualified_id,
                    observed=asset.qualified_id,
                )
            else:
                modules[module] = asset

    # Parse module-owned subtypes and their declared meaning.
    for asset in semantic_assets:
        if asset.kind not in {"ontology", "schema"}:
            continue
        payload = _validated_payload(asset)
        try:
            module = _module_id(asset, payload)
        except (TypeError, ValueError) as exc:
            _diag(diagnostics, asset, "module_identity_invalid", f"{asset.qualified_id!r} has invalid module identity: {exc}", "declare exactly one matching owner-qualified module_id/module/semantic_module_id")
            continue
        try:
            subtype_rows = _subtype_rows(payload)
        except (TypeError, ValueError) as exc:
            _diag(
                diagnostics,
                asset,
                "semantic_subtypes_invalid",
                f"{asset.qualified_id!r} declares invalid subtypes: {exc}",
                "declare subtypes as a mapping of names to mappings or a list of mappings",
            )
            continue
        for row in subtype_rows:
            unknown_row_fields = sorted(set(row) - {
                "id", "name", "subtype", "subtype_name", "parent", "is_a", "parent_subtype",
                "identity", "identity_policy", "attributes", "archetype", "annotations", "metadata",
            })
            if unknown_row_fields:
                _diag(diagnostics, asset, "semantic_subtype_unknown_field", f"{asset.qualified_id!r} subtype row has unknown field(s): {', '.join(unknown_row_fields)}", "remove unknown subtype fields or add them to the closed semantic contract", expected="closed subtype field vocabulary", observed=", ".join(unknown_row_fields))
                continue
            try:
                raw_name = _row_value(row, "name", "id", "subtype", "subtype_name")
            except ValueError as exc:
                _diag(diagnostics, asset, "semantic_subtype_alias_disagreement", f"{asset.qualified_id!r} subtype aliases disagree: {exc}", "make all authored subtype aliases identical")
                continue
            name = _snake(raw_name)
            if not name:
                _diag(
                    diagnostics,
                    asset,
                    "invalid_semantic_subtype",
                    f"{asset.qualified_id!r} declares a subtype without a stable name",
                    "give every subtype a stable owner-qualified name",
                )
                continue
            prior = subtype_by_name.get(name)
            if prior is not None:
                _diag(
                    diagnostics,
                    asset,
                    "duplicate_semantic_ownership",
                    f"subtype {name!r} is already owned by {prior.qualified_owner!r}; {asset.qualified_id!r} cannot redefine it",
                    "rename the subtype or publish an explicit ownership migration",
                    expected=prior.qualified_owner,
                    observed=asset.qualified_id,
                )
                continue
            detail = _mapping(row)
            try:
                identity = _identity_for_subtype(payload, detail)
            except (TypeError, ValueError) as exc:
                _diag(diagnostics, asset, "semantic_identity_invalid", f"{asset.qualified_id!r} subtype {name!r} has invalid identity metadata: {exc}", "declare identity as a mapping with non-empty fields and supported cardinality")
                continue
            if identity:
                identity_fields = identity.get("fields")
                identity_cardinality = identity.get("cardinality")
                if (
                    "cardinality" not in identity
                    or
                    not isinstance(identity_fields, (list, tuple))
                    or not identity_fields
                    or any(not isinstance(item, str) or not item.strip() for item in identity_fields)
                    or identity_cardinality not in {"one", "optional", "many"}
                ):
                    _diag(diagnostics, asset, "semantic_identity_invalid", f"{asset.qualified_id!r} subtype {name!r} has invalid identity fields/cardinality", "declare a non-empty string fields list and one, optional, or many cardinality")
                    continue
            try:
                parent_raw = _row_value(detail, "parent", "is_a", "parent_subtype")
                attrs = _row_value(detail, "attributes", "attribute_names")
            except ValueError as exc:
                _diag(diagnostics, asset, "semantic_subtype_alias_disagreement", f"{asset.qualified_id!r} subtype {name!r} aliases disagree: {exc}", "make all authored subtype aliases identical")
                continue
            parent = _snake(parent_raw) if parent_raw else None
            if isinstance(attrs, Mapping):
                attribute_definitions: dict[str, dict[str, Any]] = {}
                for key, value in attrs.items():
                    if not isinstance(key, str) or not key.strip():
                        _diag(diagnostics, asset, "semantic_attribute_invalid", f"{asset.qualified_id!r} subtype {name!r} has an empty/non-string attribute name", "use non-empty string attribute names")
                        continue
                    if not isinstance(value, Mapping):
                        _diag(diagnostics, asset, "semantic_attribute_invalid", f"{asset.qualified_id!r} attribute {key!r} definition must be a mapping", "declare each attribute definition as a mapping")
                        continue
                    attribute_definitions[key.strip()] = dict(value)
                if len(attribute_definitions) != len(attrs):
                    continue
                attribute_names = tuple(sorted(attribute_definitions))
            elif isinstance(attrs, Iterable) and not isinstance(attrs, (str, bytes)):
                attr_values = list(attrs)
                if any(not isinstance(item, str) or not item.strip() for item in attr_values):
                    _diag(diagnostics, asset, "semantic_attribute_invalid", f"{asset.qualified_id!r} subtype {name!r} has non-string attribute names", "declare attributes as a list of non-empty names")
                    continue
                attribute_names = tuple(sorted(str(item) for item in attr_values))
                attribute_definitions = {item: {} for item in attribute_names}
            else:
                if attrs is not None:
                    _diag(diagnostics, asset, "semantic_attribute_invalid", f"{asset.qualified_id!r} subtype {name!r} attributes must be a mapping or list", "declare attributes as a mapping or list of names")
                    continue
                attribute_names = ()
                attribute_definitions = {}
            if identity:
                undeclared_identity = sorted(set(identity.get("fields", ())) - set(attribute_names))
                if undeclared_identity:
                    _diag(diagnostics, asset, "identity_attribute_unknown", f"{asset.qualified_id!r} inline identity for subtype {name!r} names undeclared attribute(s): {', '.join(undeclared_identity)}", "declare the identity attributes in the subtype or publish a migration that adds them", expected=", ".join(attribute_names), observed=", ".join(undeclared_identity))
                    continue
            metadata = {
                key: value
                for key, value in detail.items()
                if key not in {"id", "name", "subtype", "subtype_name", "parent", "is_a", "parent_subtype", "identity", "identity_policy", "attributes"}
            }
            metadata.update(
                {
                    "semantic_owner": asset.owner,
                    "semantic_asset": asset.asset_id,
                    "semantic_module": module,
                    "attributes": list(attribute_names),
                    "attribute_definitions": attribute_definitions,
                }
            )
            item = SemanticSubtype(
                name=name,
                owner=asset.owner,
                asset_id=asset.asset_id,
                attributes=attribute_names,
                parent=parent,
                identity=identity,
                metadata=metadata,
            )
            subtype_by_name[name] = item
            subtype_asset_by_name[name] = asset
            subtype_module_by_name[name] = module
            subtypes.append(item)

    for item in subtypes:
        if item.parent and item.parent not in subtype_by_name:
            owner_asset = subtype_asset_by_name[item.name]
            _diag(
                diagnostics,
                owner_asset,
                "unknown_inheritance_endpoint",
                f"subtype {item.name!r} inherits from unknown subtype {item.parent!r}",
                "declare the parent subtype in a selected ontology module or publish an ownership migration",
                expected="declared semantic subtype ID",
                observed=item.parent,
            )
    for start in sorted(subtype_by_name):
        path: list[str] = []
        node: str | None = start
        while node is not None and node in subtype_by_name:
            if node in path:
                cycle = path[path.index(node):] + [node]
                owner_asset = subtype_asset_by_name[node]
                _diag(
                    diagnostics,
                    owner_asset,
                    "inheritance_cycle",
                    f"semantic subtype inheritance cycle: {' -> '.join(cycle)}",
                    "remove one parent/is_a declaration or publish an explicit migration that breaks the cycle",
                    expected="acyclic inheritance graph",
                    observed=" -> ".join(cycle),
                )
                break
            path.append(node)
            node = subtype_by_name[node].parent

    # Identity-policy assets are independent stable contributions.  Applying
    # one that changes an in-module policy is a conflict, not a silent merge.
    identity_by_subtype: dict[str, tuple[dict[str, Any], TypedAsset]] = {}
    for subtype in subtypes:
        if subtype.identity:
            identity_by_subtype[subtype.name] = (
                dict(subtype.identity),
                subtype_asset_by_name[subtype.name],
            )
    for asset in semantic_assets:
        if asset.kind != "identity-policy":
            continue
        payload = _validated_payload(asset)
        rows = _contribution_rows(
            asset,
            payload,
            container_keys=("policies", "identity_policies"),
            direct_keys=frozenset({"id", "subtype", "subtype_name", "target"}),
            allowed_row_keys=frozenset({
                "id", "semantic_id", "subtype", "subtype_name", "target", "fields",
                "cardinality", "evolution", "compatibility_version",
            }),
            required=("fields", "cardinality"),
            diagnostics=diagnostics,
        )
        for row in rows:
            try:
                subtype_name = _snake(_row_value(row, "subtype", "subtype_name", "target"))
            except ValueError as exc:
                _diag(diagnostics, asset, "semantic_identity_alias_disagreement", f"{asset.qualified_id!r} identity aliases disagree: {exc}", "make subtype aliases identical and name the migration correction")
                continue
            if not subtype_name:
                _diag(diagnostics, asset, "invalid_identity_policy", "identity policy lacks a subtype target", "declare the target subtype and its identity fields")
                continue
            policy = dict(row)
            fields = policy.get("fields")
            if (
                not isinstance(fields, (list, tuple))
                or not fields
                or any(not isinstance(item, str) or not item.strip() for item in fields)
            ):
                _diag(diagnostics, asset, "semantic_required_field_invalid", f"identity policy {asset.qualified_id!r} requires a non-empty string fields list", "declare identity fields as a non-empty list of attribute names")
                continue
            policy_cardinality = policy.get("cardinality", "one")
            if policy_cardinality not in {"one", "optional", "many"}:
                _diag(diagnostics, asset, "incompatible_cardinality", f"identity policy {asset.qualified_id!r} declares unsupported cardinality {policy_cardinality!r}", "use one, optional, or many for identity policy cardinality")
                continue
            if subtype_name not in subtype_by_name:
                _diag(
                    diagnostics,
                    asset,
                    "unknown_identity_endpoint",
                    f"identity policy {asset.qualified_id!r} targets unknown subtype {subtype_name!r}",
                    "declare the target subtype before adding an identity policy",
                    expected="declared semantic subtype ID",
                    observed=subtype_name,
                )
                continue
            prior = identity_by_subtype.get(subtype_name)
            comparable_policy = {
                key: value
                for key, value in policy.items()
                if key not in {"id", "semantic_id", "subtype", "subtype_name", "target", "owner", "asset_id", "compatibility_version", "compatibility_version_source"}
            }
            if prior is not None and dict(prior[0]) != comparable_policy:
                _diag(
                    diagnostics,
                    asset,
                    "incompatible_identity",
                    f"identity policy for subtype {subtype_name!r} changes the owner-qualified identity contract",
                    "publish a catalog migration that names the old and new identity fields",
                    expected=json.dumps(prior[0], sort_keys=True),
                    observed=json.dumps(comparable_policy, sort_keys=True),
                )
            identity_by_subtype[subtype_name] = (comparable_policy, asset)
            identity_policies.append({**policy, "subtype": subtype_name, "owner": asset.owner, "asset_id": asset.asset_id})

    # Parse bridges and narrowing assets.  Bridge endpoint modules are always
    # explicit; shorthand endpoints that match multiple modules are refused.
    for asset in semantic_assets:
        payload = _validated_payload(asset)
        if asset.kind == "bridge":
            try:
                endpoints = _endpoint_modules(payload)
            except (TypeError, ValueError) as exc:
                _diag(diagnostics, asset, "ambiguous_bridge_endpoint", f"bridge {asset.qualified_id!r} has invalid endpoint modules: {exc}", "declare exactly two owner-qualified endpoint module IDs")
                continue
            if len(endpoints) != 2 or any(":" not in endpoint or endpoint not in modules for endpoint in endpoints):
                _diag(
                    diagnostics,
                    asset,
                    "ambiguous_bridge_endpoint",
                    f"bridge {asset.qualified_id!r} must name exactly two compatible endpoint modules; observed {list(endpoints)!r}",
                    "declare source_module and target_module using exact owner-qualified module IDs",
                    expected="two owner-qualified endpoint modules",
                    observed=", ".join(endpoints),
                )
                continue
        if asset.kind not in {"bridge", "narrowing"}:
            continue
        try:
            endpoints = _endpoint_modules(payload)
        except (TypeError, ValueError) as exc:
            _diag(diagnostics, asset, "ambiguous_bridge_endpoint", f"narrowing {asset.qualified_id!r} has invalid endpoint modules: {exc}", "declare exactly two owner-qualified endpoint module IDs")
            continue
        if len(endpoints) != 2 or any(":" not in endpoint or endpoint not in modules for endpoint in endpoints):
            _diag(
                diagnostics,
                asset,
                "ambiguous_bridge_endpoint",
                f"{asset.qualified_id!r} must name exactly two declared owner-qualified endpoint modules; observed {list(endpoints)!r}",
                "declare source_module and target_module using exact owner-qualified module IDs",
                expected="two declared owner-qualified endpoint modules",
                observed=", ".join(endpoints),
            )
            continue
        endpoint_set = set(endpoints)
        try:
            narrowing_rows = _narrowing_rows(payload)
        except (TypeError, ValueError) as exc:
            _diag(
                diagnostics,
                asset,
                "semantic_narrowings_invalid",
                f"{asset.qualified_id!r} declares invalid narrowings: {exc}",
                "declare narrowings as a list of mappings or one complete mapping",
            )
            continue
        for row in narrowing_rows:
            unknown_row_fields = sorted(set(row) - {
                "id", "semantic_id", "parent", "parent_subtype", "source_subtype", "src_subtype",
                "child", "child_subtype", "target_subtype", "dst_subtype", "edge", "edge_type",
                "edge_archetype", "cardinality", "target_cardinality", "compatibility_version",
            })
            if unknown_row_fields:
                _diag(diagnostics, asset, "semantic_narrowing_unknown_field", f"{asset.qualified_id!r} narrowing row has unknown field(s): {', '.join(unknown_row_fields)}", "remove unknown narrowing fields or add them to the closed semantic contract", expected="closed narrowing field vocabulary", observed=", ".join(unknown_row_fields))
                continue
            try:
                parent = _snake(_row_value(row, "parent", "parent_subtype", "source_subtype", "src_subtype"))
                child = _snake(_row_value(row, "child", "child_subtype", "target_subtype", "dst_subtype"))
                edge = _snake(_row_value(row, "edge", "edge_type", "edge_archetype"))
            except ValueError as exc:
                _diag(diagnostics, asset, "semantic_narrowing_alias_disagreement", f"{asset.qualified_id!r} narrowing aliases disagree: {exc}", "make parent/child/edge aliases identical and publish the correction")
                continue
            if not parent or not child or not edge:
                _diag(diagnostics, asset, "invalid_narrowing", f"{asset.qualified_id!r} declares an incomplete narrowing", "declare parent, edge, and child endpoint IDs")
                continue
            unknown = [name for name in (parent, child) if name not in subtype_by_name]
            if unknown:
                _diag(
                    diagnostics,
                    asset,
                    "unknown_narrowing_endpoint",
                    f"narrowing {asset.qualified_id!r} refers to unknown subtype endpoint(s): {', '.join(unknown)}",
                    "declare both endpoint subtypes or correct the bridge/module reference before migration",
                    expected="declared semantic subtype IDs",
                    observed=", ".join(unknown),
                )
                continue
            endpoint_mismatches = sorted({
                subtype_module_by_name[parent], subtype_module_by_name[child]
            } - endpoint_set)
            if endpoint_mismatches:
                _diag(
                    diagnostics,
                    asset,
                    "bridge_endpoint_mismatch",
                    f"narrowing {asset.qualified_id!r} endpoints belong to module(s) {', '.join(endpoint_mismatches)} outside its declared bridge endpoints",
                    "change the narrowing endpoints or declare the exact owning modules in endpoint_modules",
                    expected=", ".join(endpoints),
                    observed=", ".join(endpoint_mismatches),
                )
                continue
            try:
                cardinality = _row_value(row, "cardinality", "target_cardinality")
            except ValueError as exc:
                _diag(diagnostics, asset, "semantic_narrowing_alias_disagreement", f"{asset.qualified_id!r} narrowing cardinality aliases disagree: {exc}", "make cardinality aliases identical and publish the correction")
                continue
            cardinality_text = str(cardinality).strip().lower() if cardinality is not None else None
            if cardinality_text not in {None, "many", "at_most_one_target"}:
                _diag(diagnostics, asset, "incompatible_cardinality", f"narrowing {asset.qualified_id!r} declares cardinality {cardinality!r}, which the existing catalog cannot faithfully enforce", "use many (unbounded) or at_most_one_target, or publish a migration that adds an enforceable rule")
                continue
            narrowing = SemanticNarrowing(parent, edge, child, asset.owner, asset.asset_id, cardinality_text, endpoints)
            prior = next((item for item in narrowings if item.triple == narrowing.triple), None)
            if prior is not None and prior.cardinality != narrowing.cardinality:
                _diag(diagnostics, asset, "incompatible_cardinality", f"narrowing {narrowing.triple!r} has conflicting cardinality declarations", "publish one cardinality rule or create an explicit catalog migration")
                continue
            if prior is None:
                narrowings.append(narrowing)

    for asset in semantic_assets:
        payload = _validated_payload(asset)
        if asset.kind == "alias":
            alias_rows = _contribution_rows(
                    asset,
                    payload,
                    container_keys=("aliases",),
                    direct_keys=frozenset({"id", "alias", "canonical", "scope"}),
                    allowed_row_keys=frozenset({"id", "semantic_id", "alias", "canonical", "scope", "compatibility_version"}),
                    required=("alias", "canonical"),
                    diagnostics=diagnostics,
            )
            for row in alias_rows:
                if not all(isinstance(row.get(key), str) and row[key].strip() for key in ("alias", "canonical")):
                    _diag(diagnostics, asset, "semantic_required_field_invalid", f"{asset.qualified_id!r} alias rows require non-empty string alias and canonical values", "provide string alias and canonical values")
                    continue
                aliases.append({**row, "owner": asset.owner, "asset_id": asset.asset_id})
        elif asset.kind == "identifier-pattern":
            pattern_rows = _contribution_rows(
                    asset,
                    payload,
                    container_keys=("patterns", "identifier_patterns"),
                    direct_keys=frozenset({"id", "id_type", "regex"}),
                    allowed_row_keys=frozenset({
                        "id", "semantic_id", "id_type", "regex", "validator_kind",
                        "description", "compatibility_version",
                    }),
                    required=("id_type", "regex"),
                    diagnostics=diagnostics,
            )
            for row in pattern_rows:
                if not all(isinstance(row.get(key), str) and row[key].strip() for key in ("id_type", "regex")):
                    _diag(diagnostics, asset, "semantic_required_field_invalid", f"{asset.qualified_id!r} identifier patterns require non-empty string id_type and regex values", "provide string id_type and regex values")
                    continue
                identifier_patterns.append({**row, "owner": asset.owner, "asset_id": asset.asset_id})
        elif asset.kind == "concept-seed":
            seed_rows = _contribution_rows(
                    asset,
                    payload,
                    container_keys=("seeds", "concept_seeds"),
                    direct_keys=frozenset({"id", "name", "node_id"}),
                    allowed_row_keys=frozenset({
                        "id", "semantic_id", "name", "description", "node_id",
                        "compatibility_version",
                    }),
                    required=("name", "description", "node_id"),
                    diagnostics=diagnostics,
            )
            for row in seed_rows:
                if not all(isinstance(row.get(key), str) and row[key].strip() for key in ("name", "description", "node_id")):
                    _diag(diagnostics, asset, "semantic_required_field_invalid", f"{asset.qualified_id!r} concept seeds require non-empty string name, description, and node_id values", "provide string name, description, and node_id values")
                    continue
                expected_node_id = "concept:" + re.sub(r"[^a-z0-9]+", "_", row["name"].lower()).strip("_")
                if not _CONCEPT_NODE_RE.fullmatch(row["node_id"]) or row["node_id"] != expected_node_id:
                    _diag(diagnostics, asset, "concept_node_id_invalid", f"{asset.qualified_id!r} concept seed {row.get('semantic_id')!r} has non-canonical node_id {row['node_id']!r}", "use concept:<lowercase_slug> matching the canonical concept name", expected=expected_node_id, observed=row["node_id"])
                    continue
                concept_seeds.append({**row, "owner": asset.owner, "asset_id": asset.asset_id})
        elif asset.kind == "catalog-migration":
            for migration_row in _contribution_rows(
                asset,
                payload,
                container_keys=("migrations", "migration"),
                direct_keys=frozenset({"id", "migration_id", "evolution"}),
                allowed_row_keys=frozenset({
                    "id", "semantic_id", "migration_id", "old_identity", "new_identity",
                    "evolution", "compatibility_version", "description",
                }),
                required=("evolution", "old_identity", "new_identity"),
                diagnostics=diagnostics,
            ):
                migration = {**migration_row, "owner": asset.owner, "asset_id": asset.asset_id}
                if not isinstance(migration.get("evolution"), str) or not migration["evolution"].strip():
                    _diag(diagnostics, asset, "semantic_required_field_invalid", f"catalog migration {asset.qualified_id!r} requires a non-empty evolution value", "declare an additive, compatible evolution or an explicitly reviewed breaking migration")
                    continue
                if not all(
                    isinstance(migration.get(field), str)
                    and migration[field].strip()
                    for field in ("old_identity", "new_identity")
                ):
                    _diag(
                        diagnostics,
                        asset,
                        "semantic_required_field_invalid",
                        f"catalog migration {asset.qualified_id!r} requires non-empty string old_identity and new_identity values",
                        "declare explicit string old_identity and new_identity details for the migration",
                        expected="non-empty string old_identity/new_identity",
                        observed=f"{migration.get('old_identity')!r}/{migration.get('new_identity')!r}",
                    )
                    continue
                evolution = migration["evolution"].lower()
                if evolution in {"breaking", "incompatible", "unsupported"}:
                    _diag(diagnostics, asset, "incompatible_evolution", f"catalog migration {asset.qualified_id!r} declares incompatible evolution {evolution!r}", "provide an additive migration with explicit old and new identities, or stage a reviewed breaking migration")
                migrations.append(migration)

    narrowings.sort(key=lambda item: (item.triple, item.owner, item.asset_id, item.cardinality or ""))
    aliases.sort(key=lambda item: (str(item.get("semantic_id", "")), str(item.get("alias", "")), str(item.get("canonical", "")), str(item.get("scope", ""))))
    identifier_patterns.sort(key=lambda item: (str(item.get("semantic_id", "")), str(item.get("id_type", "")), str(item.get("regex", ""))))
    concept_seeds.sort(key=lambda item: (str(item.get("semantic_id", "")), str(item.get("node_id", ""))))
    identity_policies.sort(key=lambda item: (str(item.get("semantic_id", "")), str(item.get("subtype", ""))))
    migrations.sort(key=lambda item: (str(item.get("semantic_id", "")), str(item.get("id", ""))))

    assets_by_qualified = {asset.qualified_id: asset for asset in semantic_assets}
    for alias in aliases:
        canonical = _snake(alias.get("canonical"))
        if canonical not in subtype_by_name:
            source = assets_by_qualified.get(f"{alias.get('owner')}:{alias.get('asset_id')}")
            if source is not None:
                _diag(
                    diagnostics,
                    source,
                    "alias_canonical_unknown",
                    f"alias contribution {alias.get('semantic_id')!r} targets unknown subtype canonical {alias.get('canonical')!r}",
                    "change canonical to a selected subtype ID or publish the subtype in the required ontology module",
                    expected="selected semantic subtype ID",
                    observed=str(alias.get("canonical")),
                )
    for policy in identity_policies:
        subtype_name = _snake(policy.get("subtype"))
        subtype = subtype_by_name.get(subtype_name)
        if subtype is None:
            continue
        declared = set(subtype.attributes)
        unknown_fields = sorted(set(policy.get("fields", ())) - declared)
        if unknown_fields:
            source = assets_by_qualified.get(f"{policy.get('owner')}:{policy.get('asset_id')}")
            if source is not None:
                _diag(
                    diagnostics,
                    source,
                    "identity_attribute_unknown",
                    f"identity policy {policy.get('semantic_id')!r} names undeclared attribute(s) for subtype {subtype_name!r}: {', '.join(unknown_fields)}",
                    "declare the identity attributes in the owning ontology or publish a migration that adds them",
                    expected=", ".join(sorted(declared)),
                    observed=", ".join(unknown_fields),
                )

    alias_keys: dict[str, tuple[str, Any, str]] = {}
    for alias in aliases:
        key = str(alias.get("alias"))
        value = (str(alias.get("canonical")), alias.get("scope"), str(alias.get("semantic_id")))
        prior = alias_keys.get(key)
        if prior is not None and prior[:2] != value[:2]:
            source = assets_by_qualified.get(f"{alias.get('owner')}:{alias.get('asset_id')}")
            if source is not None:
                _diag(diagnostics, source, "alias_key_collision", f"alias key {key!r} has conflicting canonical/scope declarations ({prior[0]!r}, {prior[1]!r}) vs ({value[0]!r}, {value[1]!r})", "choose one canonical/scope for the alias or publish an explicit migration", expected=f"{prior[0]} / {prior[1]}", observed=f"{value[0]} / {value[1]}")
        else:
            alias_keys[key] = value
    concept_nodes: dict[str, str] = {}
    for seed in concept_seeds:
        key = str(seed.get("node_id"))
        source_id = f"{seed.get('owner')}:{seed.get('asset_id')}"
        prior = concept_nodes.get(key)
        if prior is not None:
            source = assets_by_qualified.get(source_id)
            if source is not None:
                _diag(diagnostics, source, "concept_node_id_collision", f"concept node_id {key!r} is declared by both {prior!r} and {source_id!r}", "assign one canonical concept node ID and publish a migration for renames", expected=prior, observed=source_id)
        else:
            concept_nodes[key] = source_id

    for family, rows in (
        ("alias", aliases),
        ("identifier-pattern", identifier_patterns),
        ("concept-seed", concept_seeds),
        ("identity-policy", identity_policies),
        ("catalog-migration", migrations),
    ):
        seen_ids: dict[str, str] = {}
        for row in rows:
            stable_id = str(row.get("semantic_id") or row.get("id") or "")
            owner = str(row.get("owner") or "")
            asset_id = str(row.get("asset_id") or "")
            current = f"{owner}:{asset_id}"
            prior = seen_ids.get(stable_id)
            if prior is not None:
                source = assets_by_qualified.get(current)
                if source is not None:
                    _diag(
                        diagnostics,
                        source,
                        "semantic_duplicate_contribution_id",
                        f"{family} contribution ID {stable_id!r} is declared by both {prior!r} and {current!r}",
                        "give every semantic contribution a unique stable ID or publish an explicit migration",
                        expected=prior,
                        observed=current,
                    )
            else:
                seen_ids[stable_id] = current

    if diagnostics:
        raise SemanticCompositionError(diagnostics)

    composed = ComposedCatalog(deployment_name=deployment_name, modules=sorted(modules))
    composed.subtypes = {item.name for item in subtypes}
    composed.subtype_origin = {item.name: item.qualified_owner for item in subtypes}
    composed.subtype_parent = {item.name: item.parent for item in subtypes if item.parent}
    composed.subtype_metadata = {item.name: dict(item.metadata) for item in subtypes}
    for policy in identity_policies:
        subtype_name = str(policy.get("subtype") or "")
        if subtype_name in composed.subtype_metadata:
            composed.subtype_metadata[subtype_name]["identity_policy"] = dict(policy)
    composed.narrowings = sorted([
        NarrowingRow(
            item.parent_subtype,
            item.edge_type,
            item.child_subtype,
            f"{item.owner}:{item.asset_id}",
            item.asset_id,
            item.cardinality if item.cardinality == "at_most_one_target" else None,
        )
        for item in narrowings
    ], key=lambda item: (item.parent_subtype, item.edge_archetype, item.child_subtype, item.origin))
    composed.aliases = sorted((dict(item) for item in aliases), key=lambda item: (str(item.get("semantic_id", "")), str(item.get("alias", ""))))
    composed.identifier_patterns = sorted((dict(item) for item in identifier_patterns), key=lambda item: (str(item.get("semantic_id", "")), str(item.get("id_type", "")), str(item.get("regex", ""))))
    composed.concept_seeds = sorted((dict(item) for item in concept_seeds), key=lambda item: (str(item.get("semantic_id", "")), str(item.get("node_id", ""))))
    payload = {
        "deployment_name": deployment_name,
        "assets": [asset.to_dict() for asset in semantic_assets],
        "subtypes": sorted(item.name for item in subtypes),
        "subtype_definitions": [
            {
                "name": item.name,
                "owner": item.owner,
                "asset_id": item.asset_id,
                "attributes": list(item.attributes),
                "identity": dict(item.identity),
                "parent": item.parent,
                "metadata": dict(item.metadata),
            }
            for item in sorted(subtypes, key=lambda item: item.name)
        ],
        "narrowings": [
            {
                "triple": item.triple,
                "owner": item.owner,
                "asset_id": item.asset_id,
                "cardinality": item.cardinality,
                "endpoints": item.endpoints,
            }
            for item in narrowings
        ],
        "aliases": [dict(item) for item in aliases],
        "patterns": [dict(item) for item in identifier_patterns],
        "seeds": [dict(item) for item in concept_seeds],
        "identities": [dict(item) for item in identity_policies],
        "migrations": [dict(item) for item in migrations],
    }
    return SemanticCatalog(
        deployment_name=deployment_name,
        catalog=composed,
        subtypes=tuple(sorted(subtypes, key=lambda item: item.name)),
        narrowings=tuple(sorted(narrowings, key=lambda item: item.triple)),
        aliases=tuple(aliases),
        identifier_patterns=tuple(identifier_patterns),
        concept_seeds=tuple(concept_seeds),
        identity_policies=tuple(identity_policies),
        migrations=tuple(migrations),
        semantic_asset_ids=tuple(asset.qualified_id for asset in semantic_assets),
        executable_asset_ids=tuple(asset.qualified_id for asset in executable_assets),
        nonsemantic_asset_ids=tuple(asset.qualified_id for asset in nonsemantic_assets),
        nonsemantic_dispositions=nonsemantic_dispositions,
        composition_digest=_digest(payload),
    )


def apply_semantic_catalog(
    conn: Any,
    semantic_catalog: SemanticCatalog,
    *,
    identity: Any | None = None,
    apply_report: dict[str, Any] | None = None,
) -> int:
    """Apply a validated plan through the canonical catalog transaction."""

    if not isinstance(semantic_catalog, SemanticCatalog):
        raise TypeError("apply_semantic_catalog expects a SemanticCatalog plan")
    from okg.substrate.catalog.modules_apply import apply_composed_catalog

    return apply_composed_catalog(
        conn,
        semantic_catalog.catalog,
        identity=identity,
        apply_report=apply_report,
    )


__all__ = [
    "EXECUTABLE_ASSET_KINDS",
    "NONSEMANTIC_DISPOSITION",
    "SEMANTIC_ASSET_KINDS",
    "SemanticCatalog",
    "SemanticCompositionError",
    "SemanticDiagnostic",
    "SemanticNarrowing",
    "SemanticSubtype",
    "apply_semantic_catalog",
    "compose_semantic_catalog",
]
