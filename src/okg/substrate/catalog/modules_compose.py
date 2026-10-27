"""Compose a catalog from per-deployment modules + bridges.

Pure-Python composer used by `okg catalog load`. Reads
`<deployment_dir>/deployment.yaml`'s `modules:` block; walks each
module's substrate-library schema at
`okg.substrate.library.ontologies/<module>/schema.yaml`; walks
`<deployment_dir>/schemas/bridges/*.yaml` (if present); and
returns a `ComposedCatalog` carrying:

  - the union of subtype names declared by all loaded schemas
  - the union of narrowing triples (parent, edge, child) drawn
    from each schema's `EdgeNarrowings` class
  - the per-module declared origin of each subtype + narrowing
    (so the apply path can audit which module / bridge a row
    came from)

Validates BEFORE returning:
  - every module in `modules:` has a substrate-library schema
    (raises `CatalogModuleMissing` otherwise; nothing applied).
  - every narrowing's src_subtypes / dst_subtypes resolve to a
    subtype declared in one of the loaded schemas (raises
    `BridgeSubtypeUnknown` otherwise).

Apply: a separate function (`apply_to_db`) does the DDL when the
caller has a Postgres connection. The compose path is
DB-agnostic and unit-testable.
"""

from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

import yaml
from okg.substrate import fastyaml

from okg.substrate.errors import (
    BridgeSubtypeUnknown,
    CatalogModuleMissing,
    NarrowingBridgeDirectorySymlink,
    NarrowingSchemaMisplaced,
)
from okg.substrate.catalog.module_providers import (
    ModuleProvider,
    ModuleProviderError,
    ResolvedModule,
    builtin_module_providers,
    normalize_providers,
    refuse_cross_owner_subtype_collisions,
    resolve_modules,
)
from okg.substrate.nomos.pii_classification import resolve_pii_class
from okg.substrate.nomos.read_masking import (
    GOVERNANCE_KEYS,
    LEGACY_REDACTION_KEYS,
    PSEUDONYMIZE_CLASSES,
    READ_DROP_CLASSES,
    REVEAL_CLASSES,
    legacy_attr_classifier,
    subtype_attr_classifier,
)
from okg.substrate.nomos.vocabulary import PII_CLASSES
from okg.substrate.transcript_trust import (
    UNTRUSTED_CONTENT_ORIGIN_ATTR,
    UNTRUSTED_CONTENT_TOOL_NAMES_ATTR,
)

#: Merge dispositions a LinkML attribute may declare via
#: ``annotations.merge_semantics``. Kept as a literal here rather than imported
#: from ``okg.substrate.entity_merge`` so composing a catalog does not drag in
#: the remediation module; the two are kept in step by
#: tests/substrate/catalog/test_merge_semantics_annotation.py.
MERGE_SEMANTICS = frozenset({"union", "keep_survivor", "keep_retiree"})

#: Value types a LinkML attribute may declare via ``annotations.value_type``
#: when it also declares ``annotations.filterable: 'true'``
#: (PACT schema-attr-filter-index, database-schema.md §2.12). Each name is a
#: key type a declared expression index can be built for, so this set and
#: ``okg.substrate.declared_attr_indexes.VALUE_TYPES`` are the same
#: vocabulary; they are kept in step by
#: tests/substrate/db/test_declared_attr_index_admission.py. Declaring an attribute
#: filterable is an ontology annotation compiled into catalog DATA
#: (okg.subtypes.metadata) -- it executes NO DDL, which is the
#: schema-as-data contract the heterogeneous attrs payload rests on.
ATTR_VALUE_TYPES = frozenset({"num", "text", "ts", "bool"})

if TYPE_CHECKING:
    from okg.substrate.deployments import DeploymentManifest

__all__ = [
    "ATTR_VALUE_TYPES",
    "CATALOG_FIELD_ANNOTATIONS",
    "CatalogFieldDeclarationInvalid",
    "ENTITY_MERGE_EQUIVALENCE_SCHEMA_VERSION",
    "ENTITY_MERGE_RESERVED_AUDIT_ATTRS",
    "MERGE_SEMANTICS",
    "ComposedCatalog",
    "DeploymentSubtypeOverlayRefused",
    "NarrowingRow",
    "OVERLAY_EXTENDS_ANNOTATION",
    "PassageRollupDeclarationInvalid",
    "compose_catalog",
    "declared_module_providers",
    "merge_declared_providers",
    "providers_for_declared_deployment",
    "load_modules_root",
]


class PassageRollupDeclarationInvalid(CatalogModuleMissing):
    """A subtype's passage-rollup annotation is malformed."""

    code = "passage_rollup_declaration_invalid"


class CatalogFieldDeclarationInvalid(CatalogModuleMissing):
    """A subtype's title, path or date annotation names no usable attribute.

    `data.reason` says which rule refused it:

    * ``not_a_name`` -- the annotation's value is not a non-empty string;
    * ``unknown_attribute`` -- the value names an attribute the subtype does
      not declare, on its own class or on any `is_a` ancestor.
    """

    code = "catalog_field_declaration_invalid"


#: Class annotations that name the attribute holding a record's handle
#: fields (PACT `read-model-s1-catalog-fields`, design `read-model` D11), and
#: the `okg.subtypes.metadata` key each compiles to. The two date annotations
#: predate the title and path ones (`auto-promote-semantic-timestamps`); all
#: fields are read the same way and checked the same way. Domain dates
#: describe read-side meaning; they do not stamp created/modified times.
CATALOG_FIELD_ANNOTATIONS: dict[str, str] = {
    "okg_title_from": "title_from",
    "okg_path_from": "path_from",
    "okg_created_at_from": "created_at_from",
    "okg_last_modified_at_from": "last_modified_at_from",
    "okg_published_at_from": "published_at_from",
    "okg_resolved_at_from": "resolved_at_from",
    "okg_effective_at_from": "effective_at_from",
}


# Default location for substrate-library ontologies. Tests pass
# their own path; production code uses the package-relative
# default.
def load_modules_root() -> Path:
    """Resolve `src/okg/substrate/library/ontologies/`."""
    return (
        Path(__file__).resolve().parents[1]
        / "library" / "ontologies"
    )


# ---------- result types ----------


# fix-projection-alias-identity #710 (ruling 3): the closed vocabulary of
# narrowing cardinality annotations. `None` (absent) keeps today's semantics;
# `at_most_one_target` means at most one live edge per (src, edge_type) where
# src's subtype is in the ancestor closure of the annotated parent_subtype.
# An unknown value fails compose (umbrella point (g): no silent no-op keys).
NARROWING_CARDINALITY_VALUES = frozenset({"at_most_one_target"})

ENTITY_MERGE_EQUIVALENCE_SCHEMA_VERSION = (
    "okg.entity_merge.edge_equivalence.v1"
)
ENTITY_MERGE_CONFIDENCE_RULES = frozenset({"survivor_gte"})
_ENTITY_MERGE_EQUIVALENCE_KEYS = frozenset({
    "schema_version",
    "survivor",
    "retiree",
    "audit_attrs",
    "confidence_rule",
})
_ENTITY_MERGE_PARTY_KEYS = frozenset({"producer", "provenance"})
ENTITY_MERGE_RESERVED_AUDIT_ATTRS = frozenset({
    *GOVERNANCE_KEYS,
    UNTRUSTED_CONTENT_ORIGIN_ATTR,
    UNTRUSTED_CONTENT_TOOL_NAMES_ATTR,
})


def _entity_merge_equivalence_policy(
    raw: Any,
    *,
    attr_name: str,
    origin: str,
) -> dict[str, Any] | None:
    """Validate and canonicalize one oriented edge-assertion policy.

    The declaration belongs to one narrowing triple.  Nothing is inferred:
    unknown keys, blank values, wildcards-by-convention, and unsafe attempts to
    project lineage out of comparison all fail catalog composition.
    """
    if raw is None:
        return None
    where = f"EdgeNarrowings attribute {attr_name!r} in {origin!r}"
    if not isinstance(raw, dict):
        raise CatalogModuleMissing(
            f"{where} declares non-mapping entity_merge_equivalence"
        )
    unknown = sorted(set(raw) - _ENTITY_MERGE_EQUIVALENCE_KEYS)
    missing = sorted(_ENTITY_MERGE_EQUIVALENCE_KEYS - set(raw))
    if unknown or missing:
        raise CatalogModuleMissing(
            f"{where} declares invalid entity_merge_equivalence keys; "
            f"unknown={unknown}, missing={missing}"
        )
    version = str(raw.get("schema_version") or "").strip()
    if version != ENTITY_MERGE_EQUIVALENCE_SCHEMA_VERSION:
        raise CatalogModuleMissing(
            f"{where} declares unknown entity_merge_equivalence schema_version "
            f"{version!r}; expected {ENTITY_MERGE_EQUIVALENCE_SCHEMA_VERSION!r}"
        )

    parties: dict[str, dict[str, str]] = {}
    for role in ("survivor", "retiree"):
        party = raw.get(role)
        if not isinstance(party, dict) or set(party) != _ENTITY_MERGE_PARTY_KEYS:
            raise CatalogModuleMissing(
                f"{where} entity_merge_equivalence.{role} must contain exactly "
                "producer and provenance"
            )
        producer_raw = party.get("producer")
        provenance_raw = party.get("provenance")
        if (
            not isinstance(producer_raw, str)
            or not producer_raw.strip()
            or not isinstance(provenance_raw, str)
            or not provenance_raw.strip()
        ):
            raise CatalogModuleMissing(
                f"{where} entity_merge_equivalence.{role} producer and "
                "provenance must be non-empty strings"
            )
        producer = producer_raw.strip()
        provenance = provenance_raw.strip()
        parties[role] = {
            "producer": producer,
            "provenance": provenance,
        }
    if parties["survivor"] == parties["retiree"]:
        raise CatalogModuleMissing(
            f"{where} entity_merge_equivalence must name distinct oriented parties"
        )

    audit_raw = raw.get("audit_attrs")
    if not isinstance(audit_raw, list):
        raise CatalogModuleMissing(
            f"{where} entity_merge_equivalence.audit_attrs must be a list"
        )
    if any(not isinstance(item, str) for item in audit_raw):
        raise CatalogModuleMissing(
            f"{where} entity_merge_equivalence.audit_attrs must contain only "
            "strings"
        )
    audit_attrs = [item.strip() for item in audit_raw]
    if (
        not audit_attrs
        or any(not item for item in audit_attrs)
        or len(set(audit_attrs)) != len(audit_attrs)
    ):
        raise CatalogModuleMissing(
            f"{where} entity_merge_equivalence.audit_attrs must contain unique, "
            "non-empty attribute names"
        )
    reserved = sorted(set(audit_attrs) & ENTITY_MERGE_RESERVED_AUDIT_ATTRS)
    if reserved:
        raise CatalogModuleMissing(
            f"{where} entity_merge_equivalence.audit_attrs cannot project out "
            f"reserved lineage attributes: {reserved}"
        )

    confidence_rule = str(raw.get("confidence_rule") or "").strip()
    if confidence_rule not in ENTITY_MERGE_CONFIDENCE_RULES:
        raise CatalogModuleMissing(
            f"{where} declares unknown entity_merge_equivalence confidence_rule "
            f"{confidence_rule!r}; allowed: "
            f"{sorted(ENTITY_MERGE_CONFIDENCE_RULES)}"
        )
    return {
        "schema_version": version,
        "survivor": parties["survivor"],
        "retiree": parties["retiree"],
        "audit_attrs": sorted(audit_attrs),
        "confidence_rule": confidence_rule,
    }


@dataclass(frozen=True)
class NarrowingRow:
    """One narrowing triple, with provenance (which schema it
    came from + the narrowing-attribute name)."""

    parent_subtype: str
    edge_archetype: str
    child_subtype: str
    origin: str          # module name OR "bridge:<filename>"
    narrowing_name: str  # the EdgeNarrowings attribute name
    # #710: optional cardinality annotation from the EdgeNarrowings attribute
    # (`cardinality: at_most_one_target`). None = unannotated (today's
    # semantics). Validated against NARROWING_CARDINALITY_VALUES at compose.
    cardinality: Optional[str] = None
    # Optional, deployment-owned assertion equivalence rule for entity merge.
    # Canonicalized during compose and stamped into okg.narrowings.attr_rules.
    entity_merge_equivalence: dict[str, Any] | None = None


@dataclass
class ComposedCatalog:
    """Result of composing a deployment's catalog from modules +
    bridges.

    `subtypes` is the union; the per-origin keys are kept for
    audit (so the apply path can attribute each subtype back to
    its module / deployment / bridge).
    """

    deployment_name: str
    modules: list[str] = field(default_factory=list)
    subtypes: set[str] = field(default_factory=set)
    subtype_origin: dict[str, str] = field(default_factory=dict)
    # Per `add-subtype-refines-to-catalog`: each refined subtype's
    # single parent (snake-cased), read from LinkML `is_a:` on
    # the class definition. Root subtypes (no `is_a`) are absent
    # from this dict; the apply pass treats absence as NULL.
    subtype_parent: dict[str, str] = field(default_factory=dict)
    # Per-subtype metadata harvested from the LinkML schema's
    # `annotations:` block (per `add-text-attr-declaration`,
    # 2026-04-25). Currently carries
    # `searchable_text_fields: list[str]` populated from the
    # `okg_searchable_text_fields` annotation; the projector
    # auto-stamps `attrs.text` from the listed fields at
    # project_batch time.
    subtype_metadata: dict[str, dict[str, Any]] = field(default_factory=dict)
    # The attribute names each subtype's own class declares (the keys of its
    # LinkML `attributes:` block, unioned over every file that declares the
    # subtype). Compose-time only: `_validate_catalog_field_annotations` reads
    # it, with `subtype_parent`, to refuse a title, path or date annotation
    # naming an attribute the subtype does not have. Not applied to the DB.
    subtype_attributes: dict[str, set[str]] = field(default_factory=dict)
    # The attributes each subtype's own class marks LinkML `required: true`.
    # Compose-time only, like `subtype_attributes`: issue #3666 refuses a
    # linker route `on_miss` whose endpoint cannot fill them.
    subtype_required_attributes: dict[str, set[str]] = field(default_factory=dict)
    # Issue #2176: what a deployment schema file added to a subtype some
    # other file already declared. `subtype_origin` keeps naming the module
    # that owns the subtype; this records, per metadata key, the deployment
    # file that contributed to it and which entries it added, so an audit
    # does not attribute a deployment's masking or search fields to the
    # library. Shape: {subtype: {key: [{"origin", "schema_file", "extends",
    # "entries"}, ...]}}, one row per deployment file in compose order, so a
    # second file never erases the first's record; `entries` is None when the
    # file supplied the whole key. A row for a file that added the first
    # `pii_classes` also carries `library_defaults`: the default-dropped
    # attributes compose kept as `free_text` so reads still drop them. Those
    # entries are the library's read default, not the file's contribution.
    subtype_overlays: dict[str, dict[str, list[dict[str, Any]]]] = field(
        default_factory=dict,
    )
    narrowings: list[NarrowingRow] = field(default_factory=list)
    aliases: list[dict[str, str]] = field(default_factory=list)
    identifier_patterns: list[dict[str, Any]] = field(default_factory=list)
    # Concept seed entries loaded from
    # `<deployment>/concepts/seed.yaml` (per
    # `add-substrate-semantic-concept-linking`). Each entry
    # is a dict with `name`, `description`, and a
    # deterministic `node_id` (`'concept:<slug>'`).
    concept_seeds: list[dict[str, Any]] = field(default_factory=list)
    # InvariantSpec dataclasses loaded from
    # `<deployment>/invariants/*.yaml` (optional).
    invariants: list = field(default_factory=list)
    bridge_paths: list[Path] = field(default_factory=list)
    # Which provider supplied each `modules:` entry (#1179 slice 4).
    # Maps the entry as written to `owner`, `provider_kind`, and, for a
    # package-backed provider, the `package_release_digest` it was built
    # from and the `object_digests` behind its files. Slice 6 seals the
    # non-built-in entries into the release record.
    module_origins: dict[str, dict[str, Any]] = field(default_factory=dict)
    # Per-archetype relation-kind classification
    # (add-hierarchical-graph-navigation §1). Built from the
    # default mapping plus any per-schema `okg_relation_kind`
    # overrides observed during extraction.
    archetype_kind_overrides: dict[str, str] = field(default_factory=dict)

    def narrowing_triples(self) -> set[tuple[str, str, str]]:
        """Convenience: just the (parent, edge, child) triples."""
        return {
            (r.parent_subtype, r.edge_archetype, r.child_subtype)
            for r in self.narrowings
        }

    def narrowing_attr_rules(self) -> dict[tuple[str, str, str], dict[str, Any]]:
        """Per-triple `attr_rules` JSONB, carrying the #710 cardinality
        annotation. Cardinality is declared at the (parent_subtype, edge_type)
        level, so two triples sharing that pair MUST agree; a conflict fails
        attr-rule materialization loudly (umbrella point (g)). Triples with no
        annotation map to an empty dict (today's semantics).
        """
        by_pair: dict[tuple[str, str], Optional[str]] = {}
        for r in self.narrowings:
            pair = (r.parent_subtype, r.edge_archetype)
            if pair in by_pair and by_pair[pair] != r.cardinality:
                raise CatalogModuleMissing(
                    f"conflicting cardinality annotations for narrowing "
                    f"({r.parent_subtype}, {r.edge_archetype}): "
                    f"{by_pair[pair]!r} vs {r.cardinality!r}",
                    fix_hint=(
                        "declare one cardinality per (parent_subtype, "
                        "edge_type) across all EdgeNarrowings attributes"
                    ),
                )
            by_pair[pair] = r.cardinality
        self._validate_entity_merge_equivalence_conflicts()

        out: dict[tuple[str, str, str], dict[str, Any]] = {}
        for r in self.narrowings:
            triple = (r.parent_subtype, r.edge_archetype, r.child_subtype)
            rules: dict[str, Any] = {}
            if r.cardinality is not None:
                rules["cardinality"] = r.cardinality
            if r.entity_merge_equivalence is not None:
                rules["entity_merge_equivalence"] = r.entity_merge_equivalence
            out[triple] = rules
        return out

    def _validate_entity_merge_equivalence_conflicts(self) -> None:
        """Reject duplicate triples with different merge-equivalence policy.

        This validation belongs to composition because entity-merge policy is
        a closed deployment contract. Cardinality conflict validation remains
        in ``narrowing_attr_rules()`` for compatibility with its established
        projection boundary.
        """
        policies_by_triple: dict[
            tuple[str, str, str], dict[str, Any] | None
        ] = {}
        for r in self.narrowings:
            triple = (r.parent_subtype, r.edge_archetype, r.child_subtype)
            if (
                triple in policies_by_triple
                and policies_by_triple[triple] != r.entity_merge_equivalence
            ):
                raise CatalogModuleMissing(
                    "conflicting entity_merge_equivalence declarations for "
                    f"narrowing {triple}: {policies_by_triple[triple]!r} vs "
                    f"{r.entity_merge_equivalence!r}"
                )
            policies_by_triple[triple] = r.entity_merge_equivalence

    def relation_kind_for_archetype(self, archetype: str) -> str:
        """Return `contains` or `references` for an archetype.

        Reads `archetype_kind_overrides` first (deployment-supplied),
        then falls back to the default mapping in `relation_kind.py`.
        """
        from okg.substrate.catalog.relation_kind import classify_archetype
        return classify_archetype(archetype, self.archetype_kind_overrides)


# ---------- compose ----------


def declared_module_providers(
    deployment: Path | str | "DeploymentManifest",
) -> dict[str, Path]:
    """Owner -> directory that a deployment itself declares.

    Takes the same two input shapes as :func:`compose_catalog`. A path reads
    the raw ``deployment.yaml`` and hands it to the manifest loader's own
    validator rather than trusting the value; a resolved manifest already
    carries the validated mapping.
    """
    if isinstance(deployment, (str, Path)):
        deployment_root = Path(deployment)
        manifest_path = deployment_root / "deployment.yaml"
        if not manifest_path.is_file():
            raise FileNotFoundError(
                f"deployment.yaml not found: {manifest_path}"
            )
        with manifest_path.open() as handle:
            raw_manifest = fastyaml.safe_load(handle) or {}
        from okg.substrate.deployments import resolve_declared_module_providers

        return resolve_declared_module_providers(
            raw_manifest, deployment_root, manifest_path=manifest_path,
        )
    return dict(getattr(deployment, "module_providers", None) or {})


def merge_declared_providers(
    base: Mapping[str, ModuleProvider],
    declared: Mapping[str, Path],
) -> dict[str, ModuleProvider]:
    """Add the declared owners to `base`; never replace one already there.

    A declared owner that collided would silently shadow modules the caller
    pinned -- the built-in library for a plain composition, the library of the
    revision being sealed or activated for a release path. The manifest loader
    already refuses the built-in owner `okg`; this is the backstop for any
    other overlap.
    """
    if not declared:
        return dict(base)
    merged = dict(base)
    for owner, root in sorted(declared.items()):
        if owner in merged:
            raise ModuleProviderError(
                f"deployment declares module provider {owner!r}, which is"
                + " already supplied",
                data={"owner": owner, "root": str(root)},
                fix_hint=(
                    "rename the declared owner so it does not shadow a "
                    + "provider the composing command already named"
                ),
            )
        merged[owner] = ModuleProvider(owner=owner, root=root, kind="package")
    return merged


def providers_for_declared_deployment(
    deployment: Path | str | "DeploymentManifest",
    *,
    providers: Optional[Mapping[str, ModuleProvider]] = None,
    modules_root: Optional[Path] = None,
) -> dict[str, ModuleProvider]:
    """The provider mapping a release path must compose a deployment with.

    Sealing, activation and upgrade each name their own base -- the ontology
    library of the revision they are moving to -- and so bypassed the merge
    inside :func:`compose_catalog`, which only fires when the caller named no
    providers at all. A deployment that declared owners was then refused
    `module_provider_unresolved` by exactly those three commands (issue
    #1933). This resolves the base and adds the declaration to it.

    A deployment with no declaration gets the caller's own mapping unchanged.
    """
    return merge_declared_providers(
        normalize_providers(providers, modules_root),
        declared_module_providers(deployment),
    )


def compose_catalog(
    deployment_dir: Path | str | DeploymentManifest,
    *,
    modules_root: Optional[Path] = None,
    providers: Optional[Mapping[str, ModuleProvider]] = None,
    cross_links_root: Optional[Path] = None,
) -> ComposedCatalog:
    """Compose the catalog for a deployment path or resolved manifest.

    A path input preserves the legacy ``deployment.yaml`` loader. A resolved
    ``DeploymentManifest`` uses its already-composed modules and paths, which
    lets strict environment definitions share this catalog implementation
    without materializing a synthetic legacy manifest.

    `providers` maps owner to the `ModuleProvider` supplying that
    owner's ontology modules; a bare `modules:` entry resolves only
    against the built-in `okg` provider and an `owner:name` entry
    resolves by exact owner match, with no search order. `modules_root`
    is the one-provider shim for the built-in library (tests pass a tmp
    path).

    `cross_links_root` is retained as a parameter for the test
    suite that injects an empty hermetic root. Per
    `add-module-manifests-and-composition` the legacy
    `library/cross_links/` directory was deleted; cross-link
    rules now live inside their owning module's `module.yaml`
    (via `provides.intra_cross_link_rules` and
    `extends.<target>.cross_link_rules`) and the composition
    resolver loads them. Passing a tmp path here suppresses
    manifest-driven loading too, so hermetic compose tests that
    skip the manifest layer continue to work.
    """
    resolved_providers = normalize_providers(providers, modules_root)
    # Owners the deployment itself declares (`module_providers:`). Collected
    # below from whichever input shape this call got, then merged into the
    # built-ins. Only when the caller named no providers of its own: an
    # explicit mapping is the caller's whole answer, as it always was.
    declared_providers: dict[str, Path] = {}

    if isinstance(deployment_dir, (str, Path)):
        deployment_root = Path(deployment_dir)
        manifest_path = deployment_root / "deployment.yaml"
        if not manifest_path.is_file():
            raise FileNotFoundError(
                f"deployment.yaml not found: {manifest_path}"
            )
        with manifest_path.open() as f:
            raw_manifest = fastyaml.safe_load(f) or {}
        deployment_name = str(
            raw_manifest.get("name") or deployment_root.name
        )
        modules_list: list[str] = list(raw_manifest.get("modules") or [])
        schema_dir_name = raw_manifest.get("schema_dir") or "schemas"
        deployment_schema_dir = deployment_root / schema_dir_name
        aliases_path = deployment_root / "aliases.json"
        # This branch reads raw YAML rather than going through
        # `DeploymentManifest`, so it validates the key with the manifest
        # loader's own function instead of trusting the raw value.
        from okg.substrate.deployments import resolve_declared_module_providers

        declared_providers = resolve_declared_module_providers(
            raw_manifest, deployment_root, manifest_path=manifest_path,
        )
    else:
        manifest = deployment_dir
        deployment_root = Path(manifest.deployment_dir)
        deployment_name = str(manifest.name)
        modules_list = list(manifest.modules)
        deployment_schema_dir = Path(manifest.schema_dir)
        aliases_path = (
            Path(manifest.aliases)
            if manifest.aliases is not None
            else deployment_root / "aliases.json"
        )
        declared_providers = dict(getattr(manifest, "module_providers", None) or {})

    if declared_providers and providers is None and modules_root is None:
        # Add, never replace. A caller that named its own providers gets
        # exactly those here, as it always has; a release path that wants the
        # declaration merged into its own base asks for it explicitly through
        # `providers_for_declared_deployment`.
        resolved_providers = merge_declared_providers(
            builtin_module_providers(), declared_providers,
        )

    composed = ComposedCatalog(
        deployment_name=deployment_name,
        modules=list(modules_list),
    )

    # Bind every declared module to the provider that owns it before any
    # file is read. An unknown owner or a misspelled `owner:name`
    # refuses here.
    resolved_modules: dict[str, ResolvedModule] = resolve_modules(
        modules_list, resolved_providers,
    )
    for ref, resolved_module in resolved_modules.items():
        origin: dict[str, Any] = {
            "owner": resolved_module.owner,
            "provider_kind": resolved_module.provider_kind,
        }
        if resolved_module.release_digest is not None:
            origin["package_release_digest"] = resolved_module.release_digest
        if resolved_module.object_digests:
            origin["object_digests"] = list(resolved_module.object_digests)
        composed.module_origins[ref] = origin

    # Pre-flight: every declared module has a schema from its provider.
    # Refuse to do any further work otherwise — Phase 6.2.
    for mod_name in modules_list:
        schema_path = resolved_modules[mod_name].module_dir / "schema.yaml"
        if not schema_path.is_file():
            raise CatalogModuleMissing(
                f"module {mod_name!r} has no schema at "
                f"{schema_path}",
                data={
                    "module_name": mod_name,
                    "expected_path": str(schema_path),
                },
                fix_hint=(
                    f"create {schema_path} or remove "
                    f"{mod_name!r} from the deployment's "
                    f"modules: list"
                ),
            )

    # A deployment narrowing is active only as a direct ``*.yaml`` child of
    # the resolved schema directory's ``bridges`` folder. Audit the rest of the
    # tree before extraction so a parseable, explicitly marked declaration can
    # never look configured while being omitted from the composed catalog.
    _validate_deployment_narrowing_placement(deployment_schema_dir)

    # Pass 1: extract subtypes from every module schema. Subtypes
    # must be present before bridges resolve narrowings against
    # them (Phase 6.2 ordering: all subtypes before any narrowing
    # validation).
    owners_by_subtype: dict[str, dict[str, list[str]]] = {}
    for mod_name in modules_list:
        resolved_module = resolved_modules[mod_name]
        schema_path = resolved_module.module_dir / "schema.yaml"
        for subtype in _extract_subtypes(schema_path, mod_name, composed):
            owners_by_subtype.setdefault(subtype, {}).setdefault(
                resolved_module.owner, [],
            ).append(mod_name)

    # One subtype, one owner. Refused before the composed catalog is
    # returned, so nothing downstream has been mutated. Two modules of
    # the SAME owner declaring one subtype keeps first-origin-wins:
    # thirteen subtype names are cross-declared among the built-in
    # `okg` modules and five of six shipped profiles load such a pair.
    refuse_cross_owner_subtype_collisions(owners_by_subtype)

    # Also pull subtypes from the deployment's own schemas/ dir
    # (deployment-private subtypes, e.g. example/Book + Author).
    #
    # Issue #2176: each file is extracted on its own and then laid over the
    # module-composed catalog by `_overlay_deployment_subtypes`, never written
    # straight into it. A direct write replaced any metadata key the file
    # declared, so one PII-annotated attribute on a library subtype dropped the
    # masking of every other attribute the library had classified.
    if deployment_schema_dir.is_dir():
        library_modules_by_subtype = {
            subtype: sorted({
                module for modules in by_owner.values() for module in modules
            })
            for subtype, by_owner in owners_by_subtype.items()
        }
        for schema_path in sorted(deployment_schema_dir.glob("*.yaml")):
            _overlay_deployment_subtypes(
                schema_path,
                f"deployment:{deployment_name}",
                composed,
                library_modules_by_subtype=library_modules_by_subtype,
            )

    # Pass 2: extract narrowings from every module schema.
    for mod_name in modules_list:
        schema_path = resolved_modules[mod_name].module_dir / "schema.yaml"
        _extract_narrowings(schema_path, mod_name, composed)

    # Pass 3: extract narrowings from deployment bridges. Bridges
    # are loaded last so they can reference subtypes from any
    # module. Each bridge file's narrowings carry origin
    # `"bridge:<filename>"` for audit.
    bridges_dir = deployment_schema_dir / "bridges"
    if bridges_dir.is_dir():
        for bridge_path in sorted(bridges_dir.glob("*.yaml")):
            composed.bridge_paths.append(bridge_path)
            _extract_narrowings(
                bridge_path,
                f"bridge:{bridge_path.name}",
                composed,
            )

    # Pass 3.5: cross-link rules sourced from module manifests
    # (per `add-module-manifests-and-composition`, replacing the
    # deleted `library/cross_links/` directory). The resolver
    # walks each loaded module's `module.yaml` and its `extends:`
    # blocks, collects rule files, dedupes, and hands them to
    # `_inject_cross_link_narrowings_from_resolver` which fires
    # the same triple-emission logic the legacy loop ran.
    #
    # The `cross_links_root` argument is honored as an escape
    # hatch for hermetic tests that explicitly want NO cross-link
    # rules loaded (e.g. tmp_path / "cross_links"). When supplied
    # and empty, no resolver pass runs.
    if cross_links_root is not None and not any(
        cross_links_root.iterdir()
    ):
        pass  # hermetic test mode: skip manifest-driven loading
    else:
        _inject_cross_link_narrowings_from_resolver(
            composed, providers=resolved_providers,
        )

    # Validate: every narrowing references known subtypes.
    _validate_narrowing_subtypes(composed)

    # A declared rollup is usable only when the deployment composes the exact
    # parent/edge/child narrowing it names.
    _validate_passage_rollups(composed)

    # A title, path or date annotation must name an attribute the subtype
    # has (design `read-model` D11), so a typo fails the catalog rather than
    # reading as "no title" on every record.
    _validate_catalog_field_annotations(composed)

    # Identifier fields are checked on the composed metadata, so a field a
    # deployment file adds to a library type is held to the same rule.
    _validate_identifier_declarations(composed)

    # Issue #3666: a linker route's `on_miss` writes endpoint rows of a
    # catalog subtype holding only a key (and a scope); refuse one whose
    # subtype is missing or requires attributes the endpoint cannot fill.
    _validate_link_endpoint_routes(composed, deployment_root)

    # Issue #3666: every live type, checked against its subtype's composed
    # relations and against the other live types (prefixes, far-end types).
    _validate_live_types(composed)

    # Pass 4: deployment aliases.json (optional).
    if aliases_path.is_file():
        try:
            data = json.loads(aliases_path.read_text())
        except json.JSONDecodeError as e:
            raise ValueError(
                f"aliases.json at {aliases_path} is not valid JSON: {e}"
            ) from e
        if isinstance(data, dict):
            for alias, target in data.items():
                if isinstance(target, dict):
                    canonical = target.get("canonical")
                    if not canonical:
                        raise ValueError(
                            f"aliases.json entry {alias!r} is missing "
                            "`canonical`"
                        )
                    entry = {
                        "alias": str(alias),
                        "canonical": str(canonical),
                    }
                    if target.get("scope") is not None:
                        entry["scope"] = str(target["scope"])
                    composed.aliases.append(entry)
                else:
                    composed.aliases.append({
                        "alias": str(alias),
                        "canonical": str(target),
                    })
        elif isinstance(data, list):
            for entry in data:
                if isinstance(entry, dict):
                    out = dict(entry)
                    if "canonical" not in out and "target" in out:
                        out["canonical"] = out.pop("target")
                    composed.aliases.append(out)

    # Pass 5: deployment identifier_patterns.yaml (optional).
    ip_path = deployment_root / "identifier_patterns.yaml"
    if ip_path.is_file():
        from okg.substrate.catalog.identifier_patterns import (
            parse_yaml as _parse_identifier_patterns,
        )
        for pattern in _parse_identifier_patterns(ip_path):
            composed.identifier_patterns.append({
                "id_type": pattern.id_type,
                "regex": pattern.regex,
                "validator_kind": pattern.validator_kind,
                "description": pattern.description,
            })

    # Pass 5.5: deployment concepts/seed.yaml (optional,
    # per `add-substrate-semantic-concept-linking`). Each
    # entry has `name` and `description`; the loader
    # validates and produces a deterministic `node_id`
    # of the form `'concept:<slug>'`. The catalog-apply
    # step upserts one Concept `nodes_live` row per entry;
    # the DBOS `okg_embed_batch` workflow populates each
    # Concept's `embedding` from the canonical node text.
    from okg.substrate.catalog.concept_seed import (
        has_seed as _has_concept_seed,
        parse_seed as _parse_concept_seed,
    )
    if _has_concept_seed(deployment_root):
        composed.concept_seeds.extend(
            _parse_concept_seed(deployment_root),
        )

    # Pass 6: deployment invariants (optional): the root
    # `invariants.yaml` a profile ships plus `invariants/*.yaml`,
    # discovered by the same helper the wiring resolver uses so
    # the publisher evaluates exactly the files the operator sees.
    # Each file holds a top-level `invariants:` list; identical
    # cross-file duplicates collapse, conflicting ones refuse. The
    # catalog-apply step installs the collected specs at the new
    # catalog_version_id.
    from okg.substrate.catalog.invariants import (
        load_deployment_invariants as _load_deployment_inv,
    )
    composed.invariants.extend(_load_deployment_inv(deployment_root))

    # Pass 7: cycle detection on `contains`-kind narrowings
    # (add-hierarchical-graph-navigation §2). A real containment
    # relation is a DAG; cycles in the contains-edge graph would
    # make hierarchy drilling non-terminating.
    _check_no_contains_cycles(composed)

    # Entity-merge equivalence is a closed deployment contract, so conflicting
    # duplicate declarations fail before composition returns. Keep cardinality
    # conflict validation at narrowing_attr_rules(), its established projection
    # boundary; broad materialization here would change that existing contract.
    composed._validate_entity_merge_equivalence_conflicts()

    return composed


def _validate_live_types(composed: ComposedCatalog) -> None:
    """Check every composed live type whole (issue #3666).

    `_extract_subtypes` checks a live type against its own file's relations.
    A deployment file may add the annotation to a subtype whose relations
    another file declares, so the relation check is repeated here over the
    composed metadata, together with the checks that need every live type
    at once: one prefix per type, far-end types that are live types, and a
    `system` some declared-only type declares. Binding and stored-node checks
    need the deployment and the database; they run at server start.
    """
    if not any(
        isinstance(meta, Mapping) and meta.get("live_type") is not None
        for meta in composed.subtype_metadata.values()
    ):
        return
    from okg.substrate.external_backed.live_types import LiveTypeRegistry

    LiveTypeRegistry.from_catalog(composed.subtype_metadata)


def _check_no_contains_cycles(composed: ComposedCatalog) -> None:
    """Refuse to publish a catalog whose `contains`-kind narrowings
    form a cycle. Uses Tarjan's strongly-connected components on
    the (parent_subtype → child_subtype) digraph restricted to
    contains-kind edges."""
    # Build the contains digraph (subtype-level, not instance-level).
    contains_edges: list[tuple[str, str]] = []
    for row in composed.narrowings:
        if composed.relation_kind_for_archetype(row.edge_archetype) == "contains":
            contains_edges.append((row.parent_subtype, row.child_subtype))

    if not contains_edges:
        return

    # Tarjan's SCC.
    nodes = {n for pair in contains_edges for n in pair}
    adj: dict[str, list[str]] = {n: [] for n in nodes}
    for src, dst in contains_edges:
        adj[src].append(dst)

    index_counter = [0]
    stack: list[str] = []
    on_stack: set[str] = set()
    indices: dict[str, int] = {}
    lowlinks: dict[str, int] = {}
    sccs: list[list[str]] = []

    def strongconnect(v: str) -> None:
        indices[v] = index_counter[0]
        lowlinks[v] = index_counter[0]
        index_counter[0] += 1
        stack.append(v)
        on_stack.add(v)
        for w in adj[v]:
            if w not in indices:
                strongconnect(w)
                lowlinks[v] = min(lowlinks[v], lowlinks[w])
            elif w in on_stack:
                lowlinks[v] = min(lowlinks[v], indices[w])
        if lowlinks[v] == indices[v]:
            scc = []
            while True:
                w = stack.pop()
                on_stack.discard(w)
                scc.append(w)
                if w == v:
                    break
            if len(scc) > 1:
                sccs.append(scc)

    for n in nodes:
        if n not in indices:
            strongconnect(n)

    if sccs:
        cycle = sorted(sccs[0])
        raise ValueError(
            "containment cycle detected in `contains`-kind narrowings — "
            "hierarchy drilling requires a DAG. "
            f"Cycle members: {cycle}. "
            "Either tag one of the offending archetypes as "
            "`okg_relation_kind: references` to break the cycle, or "
            "remove the cyclic narrowing from the schema."
        )


# ---------- extraction helpers ----------


def _pascal_to_snake(name: str) -> str:
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def _split_subtypes_field(value: Any) -> list[str]:
    """LinkML annotation values are strings; bridges allow
    comma-separated lists ('AgentSession,SubAgentSession') so
    one narrowing-attribute can fan out to multiple triples."""
    if value is None:
        return []
    s = str(value).strip()
    if not s:
        return []
    return [tok.strip() for tok in s.split(",") if tok.strip()]


def _validate_identifier_fields(
    subtype: str, identifier_fields: list[str], text_fields: list[str],
) -> None:
    """Refuse an `okg_identifier_fields` entry that search could never read.

    `text` is the stamped rollup (and `okg_producer_text` its reserved copy),
    not a source field; an `okg_*` name is the platform's own attribute. A
    field that is not also in `okg_searchable_text_fields` is not part of the
    stored text, which the exact-id read searches, so declaring it would be a
    silent no-op."""
    from okg.substrate.searchable_text import PRODUCER_TEXT_ATTR

    reserved = [
        field for field in identifier_fields
        if field == "text" or field == PRODUCER_TEXT_ATTR or field.startswith("okg_")
    ]
    if reserved:
        raise ValueError(
            f"{subtype}: okg_identifier_fields {reserved} cannot name `text` "
            "or an `okg_*` attribute; declare the attribute that HOLDS the "
            "identifier."
        )
    orphans = [field for field in identifier_fields if field not in text_fields]
    if orphans:
        raise ValueError(
            f"{subtype}: okg_identifier_fields {orphans} not also declared in "
            "okg_searchable_text_fields. Search finds a record by its stored "
            "text; an identifier that is not part of the text is never read."
        )


def _validate_identifier_declarations(composed: ComposedCatalog) -> None:
    """Check every composed subtype's `identifier_fields` once all modules and
    deployment files are laid over, so a field a deployment file adds to a
    library type is held to the rule of `_validate_identifier_fields`."""
    for subtype in sorted(composed.subtype_metadata):
        metadata = composed.subtype_metadata[subtype]
        fields = metadata.get("identifier_fields")
        if fields:
            _validate_identifier_fields(
                subtype,
                [str(field) for field in fields],
                [str(field) for field in metadata.get("searchable_text_fields") or []],
            )


def _parse_filterable_annotation(
    declared: Any,
    *,
    schema_path: Any,
    cls_name: str,
    attr_name: str,
) -> bool:
    """Read ``annotations.filterable`` as a boolean, refusing anything else.

    LinkML annotation values arrive as YAML scalars, so both a real bool and
    the quoted string form (``filterable: 'true'``) are accepted. Anything
    else -- ``"yes"``, ``1``, a typo -- raises instead of reading as False:
    an attribute the author believes is indexed but which the composer
    silently drops would leave the operator on the scan path with no signal,
    which is the silent-degradation failure this PACT exists to remove.
    """
    if declared is None:
        return False
    if isinstance(declared, bool):
        return declared
    if isinstance(declared, str) and declared.strip().lower() in ("true", "false"):
        return declared.strip().lower() == "true"
    raise ValueError(
        f"{schema_path}: class {cls_name} attribute {attr_name} declares "
        f"filterable {declared!r}; expected true or false"
    )


def _parse_evidence_annotations(
    annotations: Any,
    *,
    schema_path: Path,
    cls_name: str,
) -> tuple[str | None, dict[str, dict[str, str]]]:
    """Compile deployment-authored evidence labels without a fixed vocabulary."""
    if not isinstance(annotations, dict):
        return None, {}

    def token(value: Any, label: str) -> str:
        if not isinstance(value, str) or not re.fullmatch(
            r"[a-z][a-z0-9_]{0,63}", value.strip().lower()
        ):
            raise ValueError(
                f"{schema_path}: class {cls_name} {label} must be a "
                "lower-snake-case token"
            )
        return value.strip().lower()

    direct: str | None = None
    if annotations.get("okg_evidence_kind") is not None:
        direct = token(
            annotations["okg_evidence_kind"],
            "okg_evidence_kind",
        )
    raw_mapping = annotations.get("okg_evidence_kind_by_attr")
    if raw_mapping is None:
        return direct, {}
    if not isinstance(raw_mapping, dict):
        raise ValueError(
            f"{schema_path}: class {cls_name} "
            "okg_evidence_kind_by_attr must be a mapping"
        )
    mapping: dict[str, dict[str, str]] = {}
    for attr_name, values in raw_mapping.items():
        if not isinstance(attr_name, str) or not attr_name.strip():
            raise ValueError(
                f"{schema_path}: class {cls_name} evidence mapping "
                "attribute names must be non-empty strings"
            )
        if not isinstance(values, dict):
            raise ValueError(
                f"{schema_path}: class {cls_name} evidence mapping for "
                f"{attr_name!r} must be a mapping"
            )
        mapping[attr_name.strip()] = {
            str(raw_value): token(
                mapped_value,
                f"okg_evidence_kind_by_attr[{attr_name!r}]",
            )
            for raw_value, mapped_value in values.items()
        }
    return direct, mapping


def _parse_passage_rollup_annotation(
    raw: Any,
    *,
    schema_path: Any,
    cls_name: str,
) -> dict[str, str] | None:
    """Read a parent's explicitly declared passage relation."""
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise PassageRollupDeclarationInvalid(
            f"{schema_path}: class {cls_name} okg_passage_rollup must be a "
            "mapping with `edge_type` and `child_subtype`",
            data={"schema_path": str(schema_path), "class_name": cls_name},
            fix_hint="declare exactly edge_type and child_subtype as non-empty strings",
        )
    unknown = sorted(set(raw) - {"edge_type", "child_subtype"})
    if unknown:
        raise PassageRollupDeclarationInvalid(
            f"{schema_path}: class {cls_name} okg_passage_rollup has unknown "
            f"key(s) {unknown}; accepted keys are "
            "['child_subtype', 'edge_type']",
            data={
                "schema_path": str(schema_path),
                "class_name": cls_name,
                "unknown_keys": unknown,
            },
            fix_hint="remove unknown keys and declare exactly edge_type and child_subtype",
        )
    edge_type = raw.get("edge_type")
    child_subtype = raw.get("child_subtype")
    for label, value in (
        ("edge_type", edge_type),
        ("child_subtype", child_subtype),
    ):
        if not isinstance(value, str) or not value.strip():
            raise PassageRollupDeclarationInvalid(
                f"{schema_path}: class {cls_name} okg_passage_rollup "
                f"requires a non-empty {label}",
                data={
                    "schema_path": str(schema_path),
                    "class_name": cls_name,
                    "field": label,
                },
                fix_hint=f"set okg_passage_rollup.{label} to a non-empty string",
            )
    return {
        "edge_type": _pascal_to_snake(str(edge_type).strip()),
        "child_subtype": _pascal_to_snake(str(child_subtype).strip()),
    }


def _extract_subtypes(
    schema_path: Path, origin: str, composed: ComposedCatalog,
) -> set[str]:
    """Walk schema's `classes:` for archetype-annotated subtypes.

    Skips `EdgeNarrowings` (it's a narrowing carrier, not a node
    subtype). Skips classes without `annotations.archetype` — by
    convention, only annotated classes are real subtypes.

    Returns the subtype names THIS file declares, which the caller
    needs to see duplicates: `composed.subtypes` is a set and
    `subtype_origin` uses `setdefault`, so a second declaration leaves
    no trace in the composed catalog.
    """
    declared_subtypes: set[str] = set()
    try:
        with schema_path.open() as f:
            schema = fastyaml.safe_load(f) or {}
    except (OSError, yaml.YAMLError):
        return declared_subtypes
    classes = schema.get("classes") or {}
    for cls_name, cls_def in classes.items():
        if cls_name == "EdgeNarrowings":
            continue
        if not isinstance(cls_def, dict):
            continue
        annotations = cls_def.get("annotations") or {}
        if "archetype" not in annotations:
            continue
        subtype = _pascal_to_snake(cls_name)
        declared_subtypes.add(subtype)
        composed.subtypes.add(subtype)
        composed.subtype_origin.setdefault(subtype, origin)
        # read-masking-from-catalog-pii-class: compile LinkML's per-attribute
        # privacy annotations into the subtype metadata consumed by Nomos reads.
        # Reject misspellings here rather than silently weakening masking at run
        # time. An empty declaration is not written by the composer; a deployment
        # may still explicitly store an empty map to opt into fail-safe masking.
        pii_classes: dict[str, str] = {}
        # entity-merge-provenance-vs-identity: compile each attribute's declared
        # merge disposition alongside the privacy one. `okg remediate
        # entity-merge` reads it from okg.subtypes.metadata to decide what to do
        # with an attribute that differs between the two nodes — most usefully,
        # to union a list of citations instead of keeping only the survivor's.
        # Undeclared stays undeclared: the merge then refuses rather than
        # guessing, so this is opt-in per attribute.
        merge_semantics: dict[str, str] = {}
        # schema-attr-filter-index: compile each attribute's declared filter
        # disposition. `filterable: 'true'` + `value_type: num|text|ts|bool`
        # give the attribute a declared expression index on the node graph
        # table (okg.substrate.declared_attr_indexes), which attribute filters
        # narrow through. This is DATA -- a new key
        # inside okg.subtypes.metadata under a new catalog version -- so
        # declaring an attribute filterable runs ZERO DDL and the "add a
        # subtype by editing YAML" heterogeneity contract is untouched
        # (tests/substrate/db/test_declared_attr_index_admission.py::
        # test_declaring_filterable_executes_zero_ddl). The reconciler builds
        # the index later, outside the declaration.
        # Undeclared attributes keep the unindexed scan path.
        filterable_attrs: dict[str, str] = {}
        attributes = cls_def.get("attributes") or {}
        declared_attrs = composed.subtype_attributes.setdefault(subtype, set())
        if isinstance(attributes, dict):
            declared_attrs.update(str(name) for name in attributes)
            composed.subtype_required_attributes.setdefault(subtype, set()).update(
                str(name) for name, attr_def in attributes.items()
                if isinstance(attr_def, dict) and attr_def.get("required") is True
            )
            for attr_name, attr_def in attributes.items():
                if not isinstance(attr_def, dict):
                    continue
                attr_annotations = attr_def.get("annotations") or {}
                if not isinstance(attr_annotations, dict):
                    continue
                if "pii_class" in attr_annotations:
                    declared = attr_annotations["pii_class"]
                    if (
                        not isinstance(declared, str)
                        or declared.strip() not in PII_CLASSES
                    ):
                        raise ValueError(
                            f"{schema_path}: class {cls_name} attribute "
                            f"{attr_name} declares invalid pii_class "
                            f"{declared!r}"
                        )
                    pii_classes[str(attr_name)] = declared.strip()
                if "merge_semantics" in attr_annotations:
                    # Reject a misspelling here rather than let it read as "no
                    # declaration" at merge time, which would silently take the
                    # survivor's value and drop the other node's entries.
                    declared_merge = attr_annotations["merge_semantics"]
                    if (
                        not isinstance(declared_merge, str)
                        or declared_merge.strip() not in MERGE_SEMANTICS
                    ):
                        raise ValueError(
                            f"{schema_path}: class {cls_name} attribute "
                            f"{attr_name} declares invalid merge_semantics "
                            f"{declared_merge!r}; expected one of "
                            f"{sorted(MERGE_SEMANTICS)}"
                        )
                    merge_semantics[str(attr_name)] = declared_merge.strip()
                declared_filterable = _parse_filterable_annotation(
                    attr_annotations.get("filterable"),
                    schema_path=schema_path,
                    cls_name=cls_name,
                    attr_name=str(attr_name),
                )
                declared_value_type = attr_annotations.get("value_type")
                if declared_filterable:
                    # A filterable attribute with no value_type has no
                    # index key type. Refusing here is the difference between
                    # a loud authoring error and an attribute the catalog
                    # advertises as indexed while the reconciler skips it.
                    if (
                        not isinstance(declared_value_type, str)
                        or declared_value_type.strip() not in ATTR_VALUE_TYPES
                    ):
                        raise ValueError(
                            f"{schema_path}: class {cls_name} attribute "
                            f"{attr_name} declares filterable but its "
                            f"value_type {declared_value_type!r} is not one of "
                            f"{sorted(ATTR_VALUE_TYPES)}"
                        )
                    filterable_attrs[str(attr_name)] = declared_value_type.strip()
                elif declared_value_type is not None:
                    # value_type alone indexes nothing. Mirrors the
                    # searchable_text_path_fields orphan check: a declaration
                    # that reads as "this attribute is typed and indexed" but
                    # admits no row is a silent no-op, so reject it.
                    raise ValueError(
                        f"{schema_path}: class {cls_name} attribute "
                        f"{attr_name} declares value_type "
                        f"{declared_value_type!r} without filterable: 'true'. "
                        "value_type only types the declared index of a "
                        "FILTERABLE attribute; on its own it indexes "
                        "nothing."
                    )
        if filterable_attrs:
            composed.subtype_metadata.setdefault(subtype, {})[
                "filterable_attrs"
            ] = filterable_attrs
        if pii_classes:
            composed.subtype_metadata.setdefault(subtype, {})[
                "pii_classes"
            ] = pii_classes
        if merge_semantics:
            composed.subtype_metadata.setdefault(subtype, {})[
                "merge_semantics"
            ] = merge_semantics
        # `add-subtype-refines-to-catalog`: LinkML's `is_a:` field
        # declares this subtype's single parent. Cross-module
        # references resolve through the shared flat namespace
        # (all subtypes live in one snake-case keyspace). If the
        # named parent isn't in the loaded catalog, the DB-level
        # FK on `okg.subtypes.parent_subtype` will fail loudly at
        # apply time.
        is_a = cls_def.get("is_a")
        if isinstance(is_a, str) and is_a.strip():
            parent_snake = _pascal_to_snake(is_a.strip())
            # Self-reference is rejected by the FK + by the
            # cycle-detection pre-pass in modules_apply.
            composed.subtype_parent[subtype] = parent_snake
        # `add-substrate-pre-join-mcp-framework` S-A7-R8: capture
        # each subtype's archetype so the `expand_dst_by_archetype`
        # narrowing affordance can resolve archetype names to the
        # subtype set at narrowing-extraction time.
        archetype_value = annotations.get("archetype")
        if isinstance(archetype_value, str) and archetype_value.strip():
            composed.subtype_metadata.setdefault(subtype, {})[
                "archetype"
            ] = archetype_value.strip()
        evidence_kind, evidence_kind_by_attr = _parse_evidence_annotations(
            annotations,
            schema_path=schema_path,
            cls_name=cls_name,
        )
        if evidence_kind:
            composed.subtype_metadata.setdefault(subtype, {})[
                "evidence_kind"
            ] = evidence_kind
        if evidence_kind_by_attr:
            composed.subtype_metadata.setdefault(subtype, {})[
                "evidence_kind_by_attr"
            ] = evidence_kind_by_attr
        # `live-relations-on-expand` (issue #2013): compile the subtype's
        # declared live relations here, at composition, so a malformed
        # declaration fails the catalog rather than the first agent call.
        # The PUBLISHED form is stored, which is what `inspect` advertises
        # and what the dispatcher's argument model is built from -- one
        # object, so the two cannot drift.
        from okg.substrate.external_backed.live_relations import (
            parse_live_relations,
        )

        live_relations = parse_live_relations(
            annotations, subtype=subtype, where=f" (schema: {schema_path})",
        )
        if live_relations:
            composed.subtype_metadata.setdefault(subtype, {})[
                "live_relations"
            ] = {
                name: relation.to_metadata()
                for name, relation in sorted(live_relations.items())
            }
        # Issue #3666 (PACT live-graph-rucio-sites): a live type, declared
        # once per type next to the relations it names. Checked here against
        # THIS file's relations when the file declares any; a file that adds
        # only the annotation to a subtype whose relations another file
        # declares gets a shape check here, and the whole composed catalog
        # is checked by `_validate_live_types` once every file is laid over.
        from okg.substrate.external_backed.live_types import (
            ANNOTATION_KEY as _LIVE_TYPE_ANNOTATION,
            METADATA_KEY as _LIVE_TYPE_METADATA,
            parse_live_type,
        )

        if annotations.get(_LIVE_TYPE_ANNOTATION) is not None:
            live_type = parse_live_type(
                subtype,
                annotations[_LIVE_TYPE_ANNOTATION],
                live_relations or None,
                where=f" (schema: {schema_path})",
            )
            composed.subtype_metadata.setdefault(subtype, {})[
                _LIVE_TYPE_METADATA
            ] = live_type.to_metadata()
        # `add-text-attr-declaration` (2026-04-25): capture the
        # comma-separated `okg_searchable_text_fields` annotation
        # so the projector can auto-stamp `attrs.text` from
        # those fields. Late-binding subtypes can declare this
        # without forcing every adapter to hand-stamp text.
        text_fields = _split_subtypes_field(
            annotations.get("okg_searchable_text_fields"),
        )
        # Issue #2991: the stamper keeps a producer's own `text` body under
        # a reserved attribute and appends it after the declared parts.
        # Both stampers skip that key as a part, so declaring it would be
        # a silently inert declaration; refuse it like `text` as a path
        # field below.
        from okg.substrate.searchable_text import PRODUCER_TEXT_ATTR

        if PRODUCER_TEXT_ATTR in text_fields:
            raise ValueError(
                f"{subtype}: `{PRODUCER_TEXT_ATTR}` cannot be an "
                "okg_searchable_text_field. It is the stamper's reserved "
                "copy of a producer's `text` body, already searchable as "
                "the last part of `attrs.text`. Declare `text` instead."
            )
        if text_fields:
            composed.subtype_metadata.setdefault(subtype, {})[
                "searchable_text_fields"
            ] = text_fields
        # `index-document-source-file-search` (issue #900): capture the
        # comma-separated `okg_searchable_text_path_fields` annotation.
        # Values of those fields are structured identifiers (paths), which
        # the BM25 index's `english` config lexes as a SINGLE token; the
        # stamper additionally contributes a space-split form so an
        # interior word matches. See okg.substrate.searchable_text.
        text_path_fields = _split_subtypes_field(
            annotations.get("okg_searchable_text_path_fields"),
        )
        if text_path_fields:
            # The path list only ADDS the component-word form; the raw
            # value still comes from `okg_searchable_text_fields`. A path
            # field that is not also a plain searchable field would
            # therefore stamp nothing at all (the stamper returns early on
            # an empty plain declaration) — a silent no-op with a catalog
            # that claims the field is indexed. Fail loudly instead.
            orphans = [
                field for field in text_path_fields
                if field not in text_fields
            ]
            if orphans:
                raise ValueError(
                    f"{subtype}: okg_searchable_text_path_fields "
                    f"{orphans} not also declared in "
                    "okg_searchable_text_fields. The path list only adds "
                    "the component-word form of a field that is already "
                    "searchable; on its own it stamps nothing."
                )
            # `text` IS accepted by the check above (ontologies declare it
            # as a fallback searchable field), but both stampers skip it in
            # the path loop — `attrs.text` is the rollup's destination, and
            # splitting it into itself is not idempotent. Declaring it as a
            # path field is therefore a no-op the catalog would silently
            # present as indexed.
            if "text" in text_path_fields:
                raise ValueError(
                    f"{subtype}: `text` cannot be an "
                    "okg_searchable_text_path_field. `attrs.text` is the "
                    "rollup this stamping produces, not a source field; "
                    "naming it here stamps nothing. Declare the field that "
                    "HOLDS the path instead."
                )
            composed.subtype_metadata.setdefault(subtype, {})[
                "searchable_text_path_fields"
            ] = text_path_fields
        passage_rollup = _parse_passage_rollup_annotation(
            annotations.get("okg_passage_rollup"),
            schema_path=schema_path,
            cls_name=cls_name,
        )
        if passage_rollup is not None:
            composed.subtype_metadata.setdefault(subtype, {})[
                "passage_rollup"
            ] = passage_rollup
        alias_fields = _split_subtypes_field(
            annotations.get("okg_alias_fields"),
        )
        if alias_fields:
            composed.subtype_metadata.setdefault(subtype, {})[
                "alias_fields"
            ] = alias_fields
        # PACT search-exact-id-variants-3959: `okg_identifier_fields` names the
        # attributes that hold a record's identifier (one value, a
        # comma-joined string, or a JSON list of strings). Search lists a
        # record whose declared value equals an id-shaped query first, and
        # one whose value differs only in separators or case after it. The
        # candidate read goes through `attrs.text`, so a declared field must
        # also be a searchable text field: otherwise nothing is ever found.
        identifier_fields = _split_subtypes_field(
            annotations.get("okg_identifier_fields"),
        )
        if identifier_fields:
            composed.subtype_metadata.setdefault(subtype, {})[
                "identifier_fields"
            ] = identifier_fields
        # `add-delta-proportional-substrate-work` (2026-05-12):
        # capture the subtype's declared volatile fields — attrs
        # keys that are observation-time stamps (not content). The
        # ingest runner's `_dedupe_against_persisted` strips these
        # before hashing so a re-emission with only-volatile-field
        # changes (e.g. doc_corpus stamping a new `parsed_at` on
        # every cycle) is recognized as content-identical and
        # dropped. Mirrors the `okg_alias_fields` shape.
        volatile_fields = _split_subtypes_field(
            annotations.get("okg_volatile_fields"),
        )
        if volatile_fields:
            composed.subtype_metadata.setdefault(subtype, {})[
                "volatile_fields"
            ] = volatile_fields
        # Per `auto-promote-semantic-timestamps` (2026-04-26): the
        # subtype's declared semantic timestamp source fields, which
        # `okg._stamp_semantic_timestamps` copies into `attrs.created_at` /
        # `attrs.last_modified_at`. Per `read-model-s1-catalog-fields`
        # (design `read-model` D11), the same convention names the
        # attribute holding the record's title and its path, for the
        # one-line handle a read prints. A value that is not a non-empty
        # string is refused here (it used to be ignored, which made a typo
        # read as "not declared"); `_validate_catalog_field_annotations`
        # refuses one naming an attribute the subtype does not have.
        for annotation, metadata_key in CATALOG_FIELD_ANNOTATIONS.items():
            if annotation not in annotations:
                continue
            declared_field = annotations[annotation]
            if not isinstance(declared_field, str) or not declared_field.strip():
                raise CatalogFieldDeclarationInvalid(
                    f"{schema_path}: class {cls_name} {annotation} must name "
                    f"one attribute; got {declared_field!r}",
                    data={
                        "reason": "not_a_name",
                        "schema_path": str(schema_path),
                        "class_name": cls_name,
                        "subtype": subtype,
                        "annotation": annotation,
                        "declared": declared_field,
                    },
                    fix_hint=(
                        f"set {annotation} to the name of one of the class's "
                        "attributes, or remove it"
                    ),
                )
            composed.subtype_metadata.setdefault(subtype, {})[
                metadata_key
            ] = declared_field.strip()
    return declared_subtypes


#: Class annotation a deployment schema file puts on a subtype it re-declares
#: to say "I add to the library's subtype", naming the library module that
#: declares it. Without it a deployment may still ADD a metadata key the
#: library left unset (except `pii_classes`), or restate one identically, but
#: any other redeclaration is refused (issue #2176). Any `pii_classes`
#: contribution to a library subtype needs the marker, because adding the key
#: where the library left it unset changes how every attribute is masked.
OVERLAY_EXTENDS_ANNOTATION = "okg_extends"


def _read_mask_rank(pii_classes: Any, attr: str) -> int:
    """How strongly a read withholds `attr`, under the read path's own rule.

    Uses the classifier the read filter builds for a node row
    (`okg.substrate.nomos.read_filter._filter_rows_evaluate` ->
    `read_masking.subtype_attr_classifier`, then `mask_attrs` with
    `READ_DROP_CLASSES`):

    * no `pii_classes` (None): the legacy classifier -- the
      `LEGACY_REDACTION_KEYS` (`body`, `content`, `text`, `raw`, ...) are
      `free_text` and dropped; every other attribute is `non_pii` and revealed;
    * `pii_classes` present: the declared class, and an unlisted attribute
      resolves to `sensitive_attribute` and is masked.

    0 = revealed (`REVEAL_CLASSES`), 1 = pseudonymized when a vault key is
    available (`PSEUDONYMIZE_CLASSES`), 2 = masked (the key stays on the row
    with a mask token, so its name is visible), 3 = dropped (`READ_DROP_CLASSES`:
    `mask_attrs` removes the key, so neither name nor value is read).
    """

    pii_class = _read_pii_class(pii_classes, attr)
    if pii_class in REVEAL_CLASSES:
        return 0
    if pii_class in PSEUDONYMIZE_CLASSES:
        return 1
    if pii_class in READ_DROP_CLASSES:
        return 3
    return 2

#: Metadata keys that merge entry by entry under the marker. The deployment may
#: add entries; an entry it restates must equal the library's.
_OVERLAY_MAP_KEYS = frozenset({
    "pii_classes",
    "merge_semantics",
    "filterable_attrs",
    "evidence_kind_by_attr",
    "live_relations",
})
#: Metadata keys that union under the marker, library entries first.
_OVERLAY_LIST_KEYS = frozenset({
    "searchable_text_fields",
    "searchable_text_path_fields",
    "alias_fields",
    "identifier_fields",
    "volatile_fields",
})
# Every other key (`archetype`, `evidence_kind`, `passage_rollup`,
# `created_at_from`, `last_modified_at_from`, `title_from`, `path_from`,
# `live_type`, and any key added later) is
# atomic: a differing value is refused even under the marker.
#
# `live_type` (issue #3666) is atomic ON PURPOSE. A deployment file may add a
# live type where no earlier file set one, or restate it identically; it may
# never merge into or replace one. A merged declaration would be a type no
# single file states, and its relation and prefix checks would run against
# something no operator wrote. The composed result is checked whole by
# `_validate_live_types`.


class DeploymentSubtypeOverlayRefused(CatalogModuleMissing):
    """A deployment schema file would change a subtype another file declared.

    Raised by compose before the catalog is returned, so nothing is applied.
    `data.reason` says which rule refused it:

    * ``redeclared_without_extends`` -- the file sets a metadata key the
      library already set, to a different value, and does not mark the class
      with ``okg_extends: <module>``.
    * ``masking_without_extends`` -- the file contributes ``pii_classes`` (an
      attribute ``pii_class``) to a subtype a library module declares, without
      the marker, even where the library left ``pii_classes`` unset.
    * ``masking_loosened`` -- with or without the marker, the merged
      ``pii_classes`` would make an attribute less masked on read than it was
      before the file (for example ``content: non_pii`` on a subtype whose
      reads drop ``content`` by default).
    * ``entry_conflict`` -- under the marker, the file gives an entry of a
      mergeable key (for example one attribute's ``pii_class``) a value that
      differs from the one already composed.

    ``data.conflict_source`` names who set the value the file conflicts with:
    the library module, or the earlier deployment file
    (``data.conflict_schema_file``) when a deployment file set it.
    * ``value_conflict`` -- the file gives a single-valued key (``archetype``,
      ``is_a``, ``passage_rollup``, ...) a different value.
    * ``extends_target_invalid`` -- the marker names a module that does not
      declare this subtype in this deployment, or sits on a class with no
      ``archetype`` (which is not read as a subtype at all).
    """

    code = "deployment_subtype_overlay_refused"


def _overlay_refusal(
    *,
    reason: str,
    subtype: str,
    key: str,
    schema_path: Path,
    library_origin: str,
    library_modules: list[str],
    detail: str,
    fix_hint: str,
    extra: dict[str, Any] | None = None,
    conflict_source: str | None = None,
    conflict_schema_file: str | None = None,
) -> DeploymentSubtypeOverlayRefused:
    source = conflict_source or library_origin
    data: dict[str, Any] = {
        "reason": reason,
        "subtype": subtype,
        "key": key,
        "schema_file": str(schema_path),
        "library_origin": library_origin,
        "library_modules": list(library_modules),
        "conflict_source": source,
        "conflict_schema_file": conflict_schema_file,
    }
    if extra:
        data.update(extra)
    where = (
        f"earlier deployment file {conflict_schema_file} ({source})"
        if conflict_schema_file is not None
        else source
    )
    return DeploymentSubtypeOverlayRefused(
        f"{schema_path}: subtype {subtype!r} key {key!r} (declared by "
        f"{library_origin}; conflicting value from {where}): {detail}",
        data=data,
        fix_hint=fix_hint,
    )


def _overlay_extends_markers(schema_path: Path) -> dict[str, Any]:
    """Return each re-declarable class's raw `okg_extends` value, by subtype."""

    try:
        with schema_path.open() as f:
            schema = fastyaml.safe_load(f) or {}
    except (OSError, yaml.YAMLError):
        return {}
    markers: dict[str, Any] = {}
    classes = schema.get("classes") or {}
    if not isinstance(classes, dict):
        return markers
    for cls_name, cls_def in classes.items():
        if not isinstance(cls_def, dict):
            continue
        annotations = cls_def.get("annotations") or {}
        if not isinstance(annotations, dict):
            continue
        if OVERLAY_EXTENDS_ANNOTATION in annotations:
            markers[_pascal_to_snake(str(cls_name))] = annotations[
                OVERLAY_EXTENDS_ANNOTATION
            ]
    return markers


def _overlay_deployment_subtypes(
    schema_path: Path,
    origin: str,
    composed: ComposedCatalog,
    *,
    library_modules_by_subtype: Mapping[str, list[str]],
) -> None:
    """Lay one deployment schema file over the composed catalog (#2176).

    A subtype the catalog does not hold yet is copied in whole, as before. For
    a subtype some earlier file declared, each metadata key is resolved on its
    own:

    * a key the earlier declaration left unset is added, except that any
      `pii_classes` contribution to a subtype a library module declares needs
      the marker below, even where the library left `pii_classes` unset;
    * a key restated with an identical value changes nothing;
    * any other redeclaration is refused unless the class carries
      ``okg_extends: <module>`` naming a module that declares the subtype;
      under it, map keys merge entry by entry, list keys union with the
      earlier entries first, and a differing entry or single value is still
      refused.

    Masking is never loosened, with or without the marker: the merged
    `pii_classes` is compared with the one before this file under the read
    path's own rule (`_read_mask_rank`), including the default-dropped
    attributes of a subtype with no `pii_classes` and the masking of every
    attribute a present `pii_classes` does not list. An attribute that would
    read less masked is refused; a dropped attribute shown as a mask token
    counts as less masked, because its name becomes visible. A deployment may
    add masking (a new entry at least as strict as the attribute's current
    read treatment), never remove it. When the file adds the first
    `pii_classes`, every default-dropped attribute it does not list is
    composed as `free_text` so reads still drop it (`_default_dropped_classes`).

    Every key or entry the file contributes is appended to
    `composed.subtype_overlays`, so `subtype_origin` can keep naming the owner
    without hiding the deployment's part, and a later file never erases an
    earlier file's record.
    """

    overlay = ComposedCatalog(deployment_name=composed.deployment_name)
    declared = _extract_subtypes(schema_path, origin, overlay)
    markers = _overlay_extends_markers(schema_path)
    schema_file = schema_path.name
    # A marked class that is not read as a subtype (no `archetype`) would be
    # skipped whole by `_extract_subtypes`, so the overlay its author meant to
    # apply -- masking included -- would silently never happen.
    for subtype in sorted(set(markers) - declared):
        raise _overlay_refusal(
            reason="extends_target_invalid",
            subtype=subtype,
            key=OVERLAY_EXTENDS_ANNOTATION,
            schema_path=schema_path,
            library_origin=composed.subtype_origin.get(subtype, "no loaded module"),
            library_modules=list(library_modules_by_subtype.get(subtype, [])),
            detail=(
                f"the class carries {OVERLAY_EXTENDS_ANNOTATION} but no "
                "`archetype` annotation, so it is not read as a subtype and "
                "nothing in it would be applied"
            ),
            fix_hint=(
                "restate the library's `archetype` on the class; a class is "
                "read as a subtype only when it declares one"
            ),
            extra={"extends": markers[subtype]},
        )
    for subtype in sorted(declared):
        overlay_meta = overlay.subtype_metadata.get(subtype, {})
        overlay_parent = overlay.subtype_parent.get(subtype)
        library_modules = list(library_modules_by_subtype.get(subtype, []))
        marker = markers.get(subtype)
        if subtype not in composed.subtypes:
            if marker is not None:
                raise _overlay_refusal(
                    reason="extends_target_invalid",
                    subtype=subtype,
                    key=OVERLAY_EXTENDS_ANNOTATION,
                    schema_path=schema_path,
                    library_origin="no loaded module",
                    library_modules=[],
                    detail=(
                        f"{OVERLAY_EXTENDS_ANNOTATION}: {marker!r} marks a "
                        "class no loaded module declares, so there is nothing "
                        "to extend"
                    ),
                    fix_hint=(
                        f"remove {OVERLAY_EXTENDS_ANNOTATION} from this "
                        "deployment-private class, or add the module that "
                        "declares it to the deployment's modules: list"
                    ),
                    extra={"extends": marker},
                )
            composed.subtypes.add(subtype)
            composed.subtype_origin.setdefault(subtype, origin)
            if overlay_meta:
                composed.subtype_metadata[subtype] = dict(overlay_meta)
            if overlay_parent is not None:
                composed.subtype_parent[subtype] = overlay_parent
            continue

        library_origin = composed.subtype_origin.get(subtype, "an earlier file")
        extends: str | None = None
        if marker is not None:
            if not isinstance(marker, str) or marker.strip() not in library_modules:
                raise _overlay_refusal(
                    reason="extends_target_invalid",
                    subtype=subtype,
                    key=OVERLAY_EXTENDS_ANNOTATION,
                    schema_path=schema_path,
                    library_origin=library_origin,
                    library_modules=library_modules,
                    detail=(
                        f"{OVERLAY_EXTENDS_ANNOTATION}: {marker!r} does not "
                        "name a loaded module that declares this subtype "
                        f"(declared by: {library_modules or [library_origin]})"
                    ),
                    fix_hint=(
                        f"set {OVERLAY_EXTENDS_ANNOTATION} to one of "
                        f"{library_modules}" if library_modules else
                        f"remove {OVERLAY_EXTENDS_ANNOTATION}: this subtype "
                        "comes from another deployment file, not a module"
                    ),
                    extra={"extends": marker},
                )
            extends = marker.strip()

        base_meta = dict(composed.subtype_metadata.get(subtype, {}))
        earlier_rows = composed.subtype_overlays.get(subtype, {})
        contributions: dict[str, dict[str, Any]] = {}

        def _earlier_file(
            key: str, entry: str | None = None,
        ) -> dict[str, Any] | None:
            """The latest earlier deployment file that set this key or entry.

            None means the value came from the library module (or from the
            file that first declared a deployment-private subtype).
            """
            for row in reversed(earlier_rows.get(key, [])):
                entries = row.get("entries")
                if entries is None or (entry is not None and entry in entries):
                    return row
            return None

        def _refuse(
            reason: str, key: str, detail: str, fix_hint: str,
            extra: dict[str, Any] | None = None,
            source_row: dict[str, Any] | None = None,
        ) -> DeploymentSubtypeOverlayRefused:
            return _overlay_refusal(
                reason=reason,
                subtype=subtype,
                key=key,
                schema_path=schema_path,
                library_origin=library_origin,
                library_modules=library_modules,
                detail=detail,
                fix_hint=fix_hint,
                extra=extra,
                conflict_source=(
                    source_row["origin"] if source_row is not None else None
                ),
                conflict_schema_file=(
                    source_row["schema_file"] if source_row is not None else None
                ),
            )

        def _fix_where(source_row: dict[str, Any] | None, what: str) -> str:
            if source_row is not None:
                return (
                    f"or change {what} in the earlier deployment file "
                    f"{source_row['schema_file']}, which set it; two deployment "
                    "files must agree"
                )
            return f"or change {what} in the library module {library_origin}"

        base_parent = composed.subtype_parent.get(subtype)
        if overlay_parent is not None and overlay_parent != base_parent:
            if base_parent is not None:
                source_row = _earlier_file("is_a")
                raise _refuse(
                    "value_conflict", "is_a",
                    f"the file sets is_a {overlay_parent!r} but the "
                    f"declaration it overlays has {base_parent!r}",
                    "drop is_a from the deployment file, "
                    + _fix_where(source_row, "is_a")
                    + "; a subtype has one parent",
                    source_row=source_row,
                )
            composed.subtype_parent[subtype] = overlay_parent
            contributions["is_a"] = {"entries": None}

        marker_hint = (
            f"`{OVERLAY_EXTENDS_ANNOTATION}: "
            f"{library_modules[0] if library_modules else '<module>'}`"
        )
        for key in sorted(overlay_meta):
            value = overlay_meta[key]
            present = key in base_meta
            current = base_meta.get(key)
            if present and value == current:
                continue
            if key == "pii_classes" and library_modules and extends is None:
                # Adding `pii_classes` where the library left it unset is not
                # a harmless new key: it swaps the read classifier from the
                # default list to the declared map for every attribute.
                source_row = _earlier_file(key) if present else None
                raise _refuse(
                    "masking_without_extends", key,
                    "the file sets pii_class on a subtype a library module "
                    "declares, without the marker; "
                    + (
                        "it would replace the library's whole pii_classes "
                        "map" if present else
                        "the library leaves pii_classes unset, so the file "
                        "would change how every attribute of the subtype is "
                        "masked on read"
                    )
                    + f" (library: {current!r}; file: {value!r})",
                    f"add {marker_hint} to the class's annotations to add "
                    "masking to the library's subtype, or drop the pii_class "
                    "annotations from the deployment file",
                    extra={"library_value": current, "file_value": value},
                    source_row=source_row,
                )
            new_value: Any
            entries: list[str] | None
            library_defaults: list[str] = []
            if not present and key == "pii_classes" and isinstance(value, dict):
                # The first `pii_classes` swaps the read classifier from the
                # default list to this map, where an unlisted attribute is
                # masked (name visible) instead of dropped. Keep the default
                # drop for every default-dropped attribute the file does not
                # classify itself, recorded as the library default.
                defaults = _default_dropped_classes(value)
                new_value = {**defaults, **value}
                library_defaults = sorted(defaults)
                entries = [str(entry) for entry in value] if defaults else None
            elif not present:
                new_value = value
                entries = None
            elif extends is None:
                source_row = _earlier_file(key)
                raise _refuse(
                    "redeclared_without_extends", key,
                    "the file redeclares a key the library already set, and "
                    "a redeclaration would replace the library's whole value "
                    f"(library: {current!r}; file: {value!r})",
                    f"add {marker_hint} to the class's annotations to add to "
                    f"the library's value, or drop {key!r} from the "
                    "deployment file"
                    + (
                        f" (the current value comes from the earlier "
                        f"deployment file {source_row['schema_file']})"
                        if source_row is not None else ""
                    ),
                    extra={"library_value": current, "file_value": value},
                    source_row=source_row,
                )
            elif (
                key in _OVERLAY_MAP_KEYS
                and isinstance(value, dict)
                and isinstance(current, dict)
            ):
                entries = []
                merged = dict(current)
                for entry, entry_value in value.items():
                    if entry in current:
                        if current[entry] != entry_value:
                            source_row = _earlier_file(key, str(entry))
                            raise _refuse(
                                "entry_conflict", key,
                                f"entry {entry!r} is {current[entry]!r} "
                                "already and "
                                f"{entry_value!r} in the file; a "
                                "deployment may add entries, not change them",
                                f"drop {entry!r} from the file's {key}, "
                                + _fix_where(source_row, repr(entry)),
                                extra={
                                    "entry": entry,
                                    "library_value": current[entry],
                                    "file_value": entry_value,
                                },
                                source_row=source_row,
                            )
                        continue
                    merged[entry] = entry_value
                    entries.append(str(entry))
                new_value = merged
            elif (
                key in _OVERLAY_LIST_KEYS
                and isinstance(value, list)
                and isinstance(current, list)
            ):
                added_items = [item for item in value if item not in current]
                new_value = list(current) + added_items
                entries = [str(item) for item in added_items]
            else:
                source_row = _earlier_file(key)
                raise _refuse(
                    "value_conflict", key,
                    f"the composed value is {current!r} and the file has "
                    f"{value!r}; a single-valued key cannot be merged",
                    f"drop {key!r} from the deployment file, "
                    + _fix_where(source_row, repr(key)),
                    extra={"library_value": current, "file_value": value},
                    source_row=source_row,
                )
            if key == "pii_classes":
                loosened = _loosened_read_masking(
                    current if present else None, new_value,
                )
                if loosened:
                    attr, before_class, after_class = loosened[0]
                    source_row = (
                        _earlier_file(key, attr) if present else None
                    )
                    raise _refuse(
                        "masking_loosened", key,
                        f"attribute {attr!r} reads as {before_class} before "
                        f"this file and {after_class} after it; a deployment "
                        "may add masking, never remove it",
                        f"drop the pii_class on {attr!r} from the deployment "
                        f"file, or declare one at least as strict as "
                        f"{before_class}"
                        + (
                            f" (set by the earlier deployment file "
                            f"{source_row['schema_file']})"
                            if source_row is not None else
                            f" (the read treatment of {library_origin}'s "
                            "declaration)"
                        ),
                        extra={
                            "entry": attr,
                            "library_value": before_class,
                            "file_value": after_class,
                            "loosened": [row[0] for row in loosened],
                        },
                        source_row=source_row,
                    )
            base_meta[key] = new_value
            if entries is None or entries or library_defaults:
                contributions[key] = {"entries": entries}
                if library_defaults:
                    contributions[key]["library_defaults"] = library_defaults

        if base_meta != composed.subtype_metadata.get(subtype, {}):
            composed.subtype_metadata[subtype] = base_meta
        if contributions:
            recorded = composed.subtype_overlays.setdefault(subtype, {})
            for key, contribution in contributions.items():
                row: dict[str, Any] = {
                    "origin": origin,
                    "schema_file": schema_file,
                    "extends": extends,
                    "entries": contribution["entries"],
                }
                if "library_defaults" in contribution:
                    row["library_defaults"] = contribution["library_defaults"]
                recorded.setdefault(key, []).append(row)

    # The file's attributes join the subtype's, so a title, path or date
    # annotation may name an attribute this file adds or one the library
    # declared (checked once every file is laid over).
    for subtype in sorted(declared):
        composed.subtype_attributes.setdefault(subtype, set()).update(
            overlay.subtype_attributes.get(subtype, set()),
        )
        composed.subtype_required_attributes.setdefault(subtype, set()).update(
            overlay.subtype_required_attributes.get(subtype, set()),
        )


def _read_pii_class(pii_classes: Any, attr: str) -> str:
    """The class a read resolves for `attr`; see `_read_mask_rank`."""

    declared = pii_classes if isinstance(pii_classes, Mapping) else None
    return resolve_pii_class(subtype_attr_classifier(declared)(attr))


def _default_dropped_classes(pii_classes: Mapping[str, Any]) -> dict[str, str]:
    """Default-dropped attributes a first `pii_classes` map leaves unlisted.

    With no map, the read path's `legacy_attr_classifier` classes each
    `LEGACY_REDACTION_KEYS` attribute `free_text`, which `mask_attrs` drops
    (`READ_DROP_CLASSES`). Once a map exists, an unlisted attribute resolves to
    `sensitive_attribute` and is kept as a mask token instead. Returns the
    legacy class for each such attribute the map does not list, so composing
    it keeps the drop.
    """

    return {
        attr: pii_class
        for attr in sorted(LEGACY_REDACTION_KEYS)
        if attr not in pii_classes
        and (pii_class := legacy_attr_classifier(attr)) in READ_DROP_CLASSES
    }


def _loosened_read_masking(
    before: Any, after: Any,
) -> list[tuple[str, str, str]]:
    """Attributes a read would withhold less under `after` than `before`.

    Returns `(attr, class_before, class_after)` rows, sorted by attribute. The
    attributes checked are every attribute either map lists plus the legacy
    default-dropped keys; any other attribute is revealed (no map) or masked
    (a map that does not list it) before, and masked after, since `after` is
    always a map here, so it can only get stricter.
    """

    attrs = set(LEGACY_REDACTION_KEYS)
    for pii_classes in (before, after):
        if isinstance(pii_classes, Mapping):
            attrs.update(str(attr) for attr in pii_classes)
    return [
        (attr, _read_pii_class(before, attr), _read_pii_class(after, attr))
        for attr in sorted(attrs)
        if _read_mask_rank(after, attr) < _read_mask_rank(before, attr)
    ]


def _edge_narrowings_declaration(schema: Any) -> dict[str, Any] | None:
    """Return an explicitly marked ``EdgeNarrowings`` class, if present."""

    if not isinstance(schema, dict):
        return None
    classes = schema.get("classes") or {}
    if not isinstance(classes, dict):
        return None
    declaration = classes.get("EdgeNarrowings")
    if not isinstance(declaration, dict):
        return None
    annotations = declaration.get("annotations") or {}
    if not isinstance(annotations, dict):
        return None
    if str(annotations.get("okg_edge_narrowings", "")).lower() != "true":
        return None
    return declaration


def _deployment_schema_yaml_paths(schema_dir: Path) -> list[Path]:
    """Return lexical YAML paths without following directory symlinks."""

    paths: list[Path] = []
    for current_root, directory_names, file_names in os.walk(
        schema_dir,
        followlinks=False,
    ):
        current = Path(current_root)
        directory_names[:] = sorted(
            name
            for name in directory_names
            if not (current / name).is_symlink()
        )
        for file_name in file_names:
            candidate = current / file_name
            if candidate.suffix.lower() in {".yaml", ".yml"}:
                paths.append(candidate)
    return sorted(
        paths,
        key=lambda path: path.relative_to(schema_dir).as_posix(),
    )


def _validate_deployment_narrowing_placement(schema_dir: Path) -> None:
    """Refuse explicitly marked deployment narrowings outside bridges/*.yaml."""

    bridges_dir = schema_dir / "bridges"
    if bridges_dir.is_symlink():
        raise NarrowingBridgeDirectorySymlink(
            f"canonical deployment bridge directory {bridges_dir} is a symlink",
            data={
                "bridges_path": str(bridges_dir),
                "schema_dir": str(schema_dir),
            },
            fix_hint=(
                f"replace the {bridges_dir} symlink with a real directory "
                "and place canonical bridge .yaml files directly inside it"
            ),
        )
    if not schema_dir.is_dir():
        return
    for schema_path in _deployment_schema_yaml_paths(schema_dir):
        if schema_path.parent == bridges_dir and schema_path.suffix == ".yaml":
            continue
        try:
            with schema_path.open() as schema_file:
                schema = fastyaml.safe_load(schema_file) or {}
        except (OSError, yaml.YAMLError):
            # Parsing/read errors retain the existing schema-loader behavior;
            # this audit owns only parseable, explicitly marked declarations.
            continue
        if _edge_narrowings_declaration(schema) is None:
            continue
        expected_path = bridges_dir / f"{schema_path.stem}.yaml"
        raise NarrowingSchemaMisplaced(
            f"narrowing schema {schema_path} is outside the canonical "
            f"deployment bridge location {bridges_dir}/*.yaml",
            data={
                "schema_path": str(schema_path),
                "expected_path": str(expected_path),
                "schema_dir": str(schema_dir),
            },
            fix_hint=(
                f"move {schema_path} to {expected_path} as a direct bridge "
                f"file; if {expected_path} already exists, choose a unique "
                ".yaml filename instead of overwriting it"
            ),
        )


def _extract_narrowings(
    schema_path: Path, origin: str, composed: ComposedCatalog,
) -> None:
    """Walk schema's `EdgeNarrowings` class; for each attribute,
    expand src_subtypes × dst_subtypes into NarrowingRow rows."""
    try:
        with schema_path.open() as f:
            schema = fastyaml.safe_load(f) or {}
    except (OSError, yaml.YAMLError):
        return
    en = _edge_narrowings_declaration(schema)
    if en is None:
        return
    annotations = en.get("annotations") or {}

    # Per-schema relation-kind override block (optional). Populates
    # composed.archetype_kind_overrides so the deployment author can
    # mark unusual archetypes as `contains` for hierarchy walking,
    # or override default-mapped archetypes back to `references`.
    rk_block = annotations.get("okg_relation_kind")
    if isinstance(rk_block, dict):
        from okg.substrate.catalog.relation_kind import merge_overrides
        composed.archetype_kind_overrides = merge_overrides(
            composed.archetype_kind_overrides,
            {str(k): str(v).lower() for k, v in rk_block.items()},
        )

    attrs = en.get("attributes") or {}
    for attr_name, attr_def in attrs.items():
        if not isinstance(attr_def, dict):
            continue
        a = attr_def.get("annotations") or {}
        edge_archetype = a.get("edge_archetype")
        if edge_archetype is None:
            continue
        # #710 (ruling 3): optional cardinality annotation, validated against a
        # closed vocabulary. An unknown value fails compose loudly (never a
        # silent no-op key) so a typo can't disable enforcement.
        cardinality_raw = a.get("cardinality")
        cardinality: Optional[str] = None
        if cardinality_raw is not None:
            cardinality = str(cardinality_raw).strip().lower()
            if cardinality not in NARROWING_CARDINALITY_VALUES:
                raise CatalogModuleMissing(
                    f"EdgeNarrowings attribute {attr_name!r} in {origin!r} "
                    f"declares unknown cardinality {cardinality_raw!r}; "
                    f"allowed: {sorted(NARROWING_CARDINALITY_VALUES)}",
                    fix_hint=(
                        "use `cardinality: at_most_one_target` or remove the "
                        "annotation"
                    ),
                )
        entity_merge_equivalence = _entity_merge_equivalence_policy(
            a.get("entity_merge_equivalence"),
            attr_name=str(attr_name),
            origin=origin,
        )
        srcs = _split_subtypes_field(a.get("src_subtypes"))
        dsts = _split_subtypes_field(a.get("dst_subtypes"))
        # `add-substrate-pre-join-mcp-framework` S-A7-R8: an
        # `expand_dst_by_archetype: <comma-joined archetype names>`
        # annotation expands at extract-time to every loaded subtype
        # whose `archetype` matches. Lets a schema author declare one
        # logical narrowing (e.g. `Critique critiques <artifact|claim>`)
        # without enumerating every target subtype.
        archetype_expand = _split_subtypes_field(
            a.get("expand_dst_by_archetype"),
        )
        if archetype_expand:
            wanted = {arch.strip().lower() for arch in archetype_expand}
            for subtype, metadata in composed.subtype_metadata.items():
                if metadata.get("archetype", "").lower() in wanted:
                    if subtype not in dsts:
                        dsts.append(subtype)
        if not srcs or not dsts:
            continue
        if str(a.get("optional_when_subtypes_missing", "")).lower() == "true":
            endpoint_subtypes = {
                _pascal_to_snake(value)
                for value in [*srcs, *dsts]
            }
            if not endpoint_subtypes.issubset(composed.subtypes):
                continue
        for s in srcs:
            for d in dsts:
                # Subtypes resolved from `expand_dst_by_archetype` are
                # already in snake_case (composed.subtype_metadata keys);
                # explicit `dst_subtypes` entries are PascalCase per the
                # YAML convention. _pascal_to_snake is idempotent for
                # snake_case inputs so applying it uniformly is safe.
                composed.narrowings.append(NarrowingRow(
                    parent_subtype=_pascal_to_snake(s),
                    edge_archetype=str(edge_archetype),
                    child_subtype=_pascal_to_snake(d),
                    origin=origin,
                    narrowing_name=str(attr_name),
                    cardinality=cardinality,
                    entity_merge_equivalence=entity_merge_equivalence,
                ))


def _inject_cross_link_narrowings_from_resolver(
    composed: ComposedCatalog,
    *,
    providers: Mapping[str, ModuleProvider],
) -> None:
    """Run the composition resolver against the deployment's
    loaded modules and emit narrowings from every fired rule.

    Replaces the legacy `_inject_cross_link_narrowings` (which
    walked `library/cross_links/*.yaml` directly) per
    `add-module-manifests-and-composition`. The legacy
    silent-skip-when-subtypes-absent behavior is preserved here:
    composition-time validation only fires for rules referenced
    by an `extends:` block whose target IS loaded; rules whose
    referenced subtypes aren't in the live `composed.subtypes`
    set are skipped at emit time (the next loop). This matches
    the legacy convention that a deployment loading only `forge`
    + `git_graph` doesn't trigger `forge_to_openspec` narrowings
    if `openspec` is absent — even though both modules' rules
    might still be declared by the manifest layer.

    For `dst.via: alias_table` rules, the candidate dst subtypes
    are every subtype carrying `alias_fields` in
    `composed.subtype_metadata`; the linker resolves at runtime,
    so narrowings widen here to cover the universe of possible
    target subtypes.
    """
    from okg.substrate.catalog.composition import (
        ResolverError,
        resolve,
    )

    # The resolver's hard-validation pass requires every rule's
    # subtypes to be provided by some loaded module. The compose
    # path historically tolerates rules whose subtypes aren't
    # *live* — e.g. an `extends:` block whose target IS loaded
    # but whose rule references a subtype provided by a third
    # module that ISN'T loaded. We sidestep the hard validation
    # by passing the union of provided subtypes as
    # `available_subtypes`, which the resolver checks against.
    # The narrowings loop below still applies the
    # `composed.subtypes`-only filter for emit — matching legacy
    # behavior.
    try:
        resolved = resolve(
            composed.modules,
            providers=providers,
        )
    except ResolverError:
        # Re-raise — manifest-declared rules that reference
        # unknown subtypes are operator bugs and should fail
        # loudly. The legacy path was permissive (a typo in
        # a cross-link YAML went unnoticed); manifest-driven
        # composition tightens this.
        raise

    alias_target_subtypes = sorted(
        s for s, m in composed.subtype_metadata.items()
        if "alias_fields" in m and s in composed.subtypes
    )

    for rr in resolved.cross_link_rules:
        rule = rr.rule
        srcs = tuple(rule.src.subtypes)
        if not all(s in composed.subtypes for s in srcs):
            continue
        via_dst = (
            rule.src.via_edge.dst_subtype
            if rule.src.via_edge is not None
            else None
        )
        if via_dst is not None and via_dst not in composed.subtypes:
            continue
        if rule.dst.via == "alias_table":
            dsts = tuple(alias_target_subtypes)
        else:
            dsts = tuple(rule.dst.subtypes)
            if not all(d in composed.subtypes for d in dsts):
                continue
        if not srcs or not dsts:
            continue
        for s in srcs:
            for d in dsts:
                composed.narrowings.append(NarrowingRow(
                    parent_subtype=s,
                    edge_archetype=rule.edge,
                    child_subtype=d,
                    origin=f"cross_link:{rule.id}",
                    narrowing_name=rule.id,
                ))
                if rule.bidirectional:
                    composed.narrowings.append(NarrowingRow(
                        parent_subtype=d,
                        edge_archetype=rule.edge,
                        child_subtype=s,
                        origin=f"cross_link:{rule.id}:reverse",
                        narrowing_name=f"{rule.id}_reverse",
                    ))


def _validate_narrowing_subtypes(composed: ComposedCatalog) -> None:
    """Every narrowing's parent + child must be in `composed.subtypes`."""
    for row in composed.narrowings:
        for kind, subtype in (
            ("parent", row.parent_subtype),
            ("child", row.child_subtype),
        ):
            if subtype in composed.subtypes:
                continue
            raise BridgeSubtypeUnknown(
                f"{row.origin}: narrowing {row.narrowing_name!r} "
                f"references unknown {kind} subtype "
                f"{subtype!r}",
                data={
                    "subtype_name": subtype,
                    "bridge_path": row.origin,
                    "narrowing_name": row.narrowing_name,
                    "edge_archetype": row.edge_archetype,
                    "kind": kind,
                },
                fix_hint=(
                    f"declare {subtype!r} in one of the loaded "
                    f"modules' schema.yaml or in the deployment's "
                    f"schemas/ dir, or remove the narrowing"
                ),
            )


def _validate_passage_rollups(composed: ComposedCatalog) -> None:
    """Require each rollup annotation to name a composed narrowing."""
    triples = composed.narrowing_triples()
    for parent, metadata in sorted(composed.subtype_metadata.items()):
        declared = metadata.get("passage_rollup")
        if not declared:
            continue
        child = declared["child_subtype"]
        edge = declared["edge_type"]
        if child not in composed.subtypes:
            raise CatalogModuleMissing(
                f"subtype {parent!r} declares okg_passage_rollup child "
                f"{child!r}, which is not in deployment "
                f"{composed.deployment_name!r}",
                data={"parent_subtype": parent, "declared": declared},
                fix_hint=(
                    "name a child subtype this deployment declares, or "
                    "remove the okg_passage_rollup annotation"
                ),
            )
        if (parent, edge, child) in triples:
            continue
        candidates = sorted(
            f"{p} {e} {c}" for p, e, c in triples if p == parent
        )
        raise CatalogModuleMissing(
            f"subtype {parent!r} declares okg_passage_rollup "
            f"{edge!r} -> {child!r}, but deployment "
            f"{composed.deployment_name!r} composes no such narrowing. "
            f"Narrowings from {parent!r}: {candidates or 'none'}.",
            data={
                "parent_subtype": parent,
                "declared": declared,
                "composed_from_parent": candidates,
            },
            fix_hint=(
                "declare the narrowing in an EdgeNarrowings block, or point "
                "okg_passage_rollup at one that is composed"
            ),
        )


def _subtype_attribute_names(composed: ComposedCatalog, subtype: str) -> set[str]:
    """The attributes `subtype` declares itself or inherits through `is_a`."""
    names: set[str] = set()
    seen: set[str] = set()
    current: Optional[str] = subtype
    while current is not None and current not in seen:
        seen.add(current)
        names.update(composed.subtype_attributes.get(current, set()))
        current = composed.subtype_parent.get(current)
    return names


def _subtype_required_attribute_names(
    composed: ComposedCatalog, subtype: str,
) -> set[str]:
    """The attributes `subtype` or an `is_a` ancestor marks required."""
    names: set[str] = set()
    seen: set[str] = set()
    current: Optional[str] = subtype
    while current is not None and current not in seen:
        seen.add(current)
        names.update(composed.subtype_required_attributes.get(current, set()))
        current = composed.subtype_parent.get(current)
    return names


def _validate_link_endpoint_routes(
    composed: ComposedCatalog, deployment_root: Path,
) -> None:
    """Check the deployment's `on_miss` routes against this catalog."""
    from okg.substrate.catalog.linker_config import (
        check_link_endpoint_subtypes,
        link_endpoint_routes,
    )

    routes = link_endpoint_routes(deployment_root / "linker_config.yaml")
    if not routes:
        return
    check_link_endpoint_subtypes(
        routes,
        subtypes=set(composed.subtypes),
        required_attributes=lambda subtype: _subtype_required_attribute_names(
            composed, subtype,
        ),
    )


def _validate_catalog_field_annotations(composed: ComposedCatalog) -> None:
    """Refuse a title, path or date annotation naming an unknown attribute.

    Checked after every module and deployment file is composed, so a field
    inherited from an `is_a` parent in another module, or added by a
    deployment file, counts. The refusal names the subtype, the annotation,
    the attribute, and the attributes the subtype does have.
    """
    annotation_by_key = {
        key: annotation for annotation, key in CATALOG_FIELD_ANNOTATIONS.items()
    }
    for subtype in sorted(composed.subtype_metadata):
        metadata = composed.subtype_metadata[subtype]
        for key, annotation in annotation_by_key.items():
            declared = metadata.get(key)
            if declared is None:
                continue
            known = _subtype_attribute_names(composed, subtype)
            if declared in known:
                continue
            raise CatalogFieldDeclarationInvalid(
                f"subtype {subtype!r} declares {annotation}: {declared!r}, "
                f"but {subtype!r} has no attribute {declared!r} (its "
                f"attributes, inherited ones included: "
                f"{sorted(known) or 'none'})",
                data={
                    "reason": "unknown_attribute",
                    "subtype": subtype,
                    "subtype_origin": composed.subtype_origin.get(subtype),
                    "annotation": annotation,
                    "declared": declared,
                    "attributes": sorted(known),
                },
                fix_hint=(
                    f"name one of {subtype!r}'s attributes in {annotation}, "
                    "declare the attribute on the class, or remove the "
                    "annotation"
                ),
            )
