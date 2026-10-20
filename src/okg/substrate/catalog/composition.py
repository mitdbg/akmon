"""Composition resolver — sole source of cross-link rules.

Given a deployment's `modules:` list, walks each module's
`module.yaml`, collects intra-module cross-link rule files from
`provides.intra_cross_link_rules`, applies pairwise `extends:`
blocks (firing only when the target module is also loaded), and
returns a `ResolvedCatalog` carrying the union of rules.

Replaces the legacy `library/cross_links/` directory which was
deleted in `add-module-manifests-and-composition`. Output is
consumed by
`modules_compose._inject_cross_link_narrowings_from_resolver`
(which formerly walked the deleted directory directly) so the
downstream shapes (`NarrowingRow`,
`_validate_narrowing_subtypes`, etc.) do not change.

The resolver emits a `ResolverDiagnostics` record exposing which
`extends:` blocks fired, which were silently skipped (target
module not loaded), which subtypes failed to resolve, and how
many duplicate narrowing rows were collapsed. `okg doctor`
surfaces this for operators after a catalog compile.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from okg.substrate.catalog.module_manifest import (
    ModuleManifest,
    discover_manifests,
    load_manifest,
)
from okg.substrate.catalog.module_providers import (
    ModuleProvider,
    normalize_providers,
    resolve_modules,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.library.linkers.rules import (
    CrossLinkRule,
    load_rules_file,
)


__all__ = [
    "ResolvedCatalog",
    "ResolvedRule",
    "ResolverDiagnostics",
    "ResolverError",
    "resolve",
]


class ResolverError(SubstrateError):
    """Raised when a cross-link rule references a subtype that no
    loaded module provides.

    Distinct from `ManifestError` (manifest-syntax problems) and
    `BridgeSubtypeUnknown` (bridge-narrowing problems): a
    `ResolverError` is specifically a composition failure where a
    rule's `src.subtype` / `dst.subtype` can't be found in any
    loaded module's schema. Carries `rule_file`, `rule_id`, and
    `unresolved_subtype` in `data` so the CLI can surface a
    structured envelope.
    """
    code = "composition_resolver_error"


@dataclass(frozen=True)
class ResolvedRule:
    """A cross-link rule plus where it came from.

    `source_module` is the manifest that owns the rule file (the
    module whose `provides.intra_cross_link_rules` or
    `extends.<target>.cross_link_rules` referenced this file).
    `target_module` is None for intra-module rules and the named
    target for `extends`-block rules. `rule_file` is the absolute
    path on disk.
    """
    rule: CrossLinkRule
    source_module: str
    target_module: Optional[str]
    rule_file: Path


@dataclass
class ResolverDiagnostics:
    """Structured trace of what the resolver did.

    Operators see this via `okg doctor` after a catalog compile.
    `fired_extends` lists every `(source, target)` pair whose
    `extends:` block contributed rules. `skipped_extends` lists
    pairs whose target wasn't in `loaded_modules` (silent skip
    by design — see proposal Out-of-Scope). `unresolved_subtypes`
    is the soft form of ResolverError used for diagnostic-only
    walks; the hard resolver raises ResolverError on the first
    miss. `duplicate_narrowings_collapsed` counts triples that
    appeared in multiple modules and were collapsed to one row
    on emit.
    """
    fired_extends: list[tuple[str, str]] = field(default_factory=list)
    skipped_extends: list[dict[str, str]] = field(default_factory=list)
    unresolved_subtypes: list[dict[str, str]] = field(default_factory=list)
    duplicate_narrowings_collapsed: int = 0
    rule_files_loaded: list[str] = field(default_factory=list)


@dataclass
class ResolvedCatalog:
    """Result of resolving a `loaded_modules` list against module
    manifests.

    `manifests` is a name → ModuleManifest mapping for every
    loaded module (used by callers that want to walk
    `provides.sources` / `provides.enrichers` etc.).
    `cross_link_rules` is the deduplicated, fully-resolved list
    of `(rule, source_module, target_module, rule_file)` entries.
    `subtypes_by_module` is name → set of snake_case subtypes the
    module provides (used by callers wanting per-module
    attribution); the union across all modules is the live
    subtype set.
    """
    manifests: dict[str, ModuleManifest]
    cross_link_rules: list[ResolvedRule]
    subtypes_by_module: dict[str, set[str]]
    diagnostics: ResolverDiagnostics

    @property
    def loaded_modules(self) -> list[str]:
        return list(self.manifests.keys())

    @property
    def rules(self) -> list[CrossLinkRule]:
        """Convenience: just the CrossLinkRule objects, in order."""
        return [rr.rule for rr in self.cross_link_rules]


# ---------- public API ----------


def resolve(
    loaded_modules: list[str],
    *,
    modules_root: Optional[Path] = None,
    providers: Optional[Mapping[str, ModuleProvider]] = None,
    available_subtypes: Optional[set[str]] = None,
) -> ResolvedCatalog:
    """Resolve a deployment's `modules:` list into a ResolvedCatalog.

    `loaded_modules` is the deployment's declared module list, in
    order. Each entry is either a bare built-in module name (owner
    `okg`) or an owner-qualified `owner:name`; `providers` maps owner to
    the `ModuleProvider` that supplies that owner's modules, and
    resolution is exact match with no search order. `modules_root` is
    the one-provider shim for the built-in library (tests pass a tmp
    path). `available_subtypes`
    is the set of all subtypes the catalog has access to —
    typically the union of subtypes extracted from each module's
    `schema.yaml` plus any deployment-private subtypes. When
    supplied, rules whose `src` or `dst` subtype is not in this
    set raise ResolverError; when None, the resolver uses each
    manifest's own provided-subtype list and the same check still
    fires, but cross-module subtypes are unresolved.

    Order of operations:

      1. Load each named module's `module.yaml`. Missing manifest
         → ManifestError.
      2. Walk each manifest's `provides.intra_cross_link_rules`,
         load the YAML rule files, attribute to the source module.
      3. Walk each manifest's `extends:` blocks; for each block
         whose target name is in `loaded_modules`, load its
         `cross_link_rules` and attribute to (source, target).
         For blocks whose target is absent, record a
         `skipped_extends` diagnostic and continue (silent skip).
      4. Validate: every rule's src + dst subtypes must resolve
         against `available_subtypes` (when supplied) or against
         the union of subtypes provided by loaded modules. On
         miss, raise ResolverError; the diagnostic record carries
         the first failure detail.
      5. Deduplicate: identical (src_subtype, edge, dst_subtype,
         matcher) triples across different modules collapse to
         a single row. The first occurrence wins for attribution.
    """
    resolved_modules = resolve_modules(
        loaded_modules,
        normalize_providers(providers, modules_root),
    )

    # Step 1: load manifests for every named module.
    #
    # A module without a `module.yaml` is treated as a minimal
    # one (no `provides.intra_cross_link_rules`, no `extends:`).
    # This keeps legacy hermetic test fixtures working — a test
    # that synthesizes a fake `<root>/git_graph/schema.yaml`
    # without a manifest still composes cleanly. Production
    # in-tree modules are expected to ship a manifest; the
    # absence is silent rather than an error so a deployment
    # composing a third-party module dir doesn't have to ship
    # one before integrating.
    manifests: dict[str, ModuleManifest] = {}
    for name in loaded_modules:
        manifest_path = resolved_modules[name].module_dir / "module.yaml"
        if not manifest_path.is_file():
            manifests[name] = ModuleManifest(
                name=name,
                version="0.0.0",
                path=manifest_path,
            )
            continue
        manifests[name] = load_manifest(manifest_path)

    diagnostics = ResolverDiagnostics()
    subtypes_by_module: dict[str, set[str]] = {
        name: _extract_module_subtypes(manifest)
        for name, manifest in manifests.items()
    }

    # Step 2 + 3: collect rules from `provides.intra_cross_link_rules`
    # and from each fired `extends:` block.
    all_rules: list[ResolvedRule] = []
    for name, manifest in manifests.items():
        for rel_path in manifest.provides.intra_cross_link_rules:
            rule_file = (manifest.module_dir / rel_path).resolve()
            rules = _load_rule_file(
                rule_file, source=name, target=None,
                diagnostics=diagnostics,
            )
            all_rules.extend(rules)

    for source_name, manifest in manifests.items():
        for target_name, block in manifest.extends.items():
            if target_name not in manifests:
                diagnostics.skipped_extends.append({
                    "source": source_name,
                    "target": target_name,
                    "reason": "target module not loaded",
                })
                continue
            diagnostics.fired_extends.append(
                (source_name, target_name),
            )
            for rel_path in block.cross_link_rules:
                rule_file = (
                    manifest.module_dir / rel_path
                ).resolve()
                rules = _load_rule_file(
                    rule_file,
                    source=source_name,
                    target=target_name,
                    diagnostics=diagnostics,
                )
                all_rules.extend(rules)

    # Step 4: subtype-resolution validation.
    if available_subtypes is None:
        available_subtypes = set()
        for subs in subtypes_by_module.values():
            available_subtypes.update(subs)

    for rr in all_rules:
        _validate_rule_subtypes(
            rr,
            available_subtypes=available_subtypes,
            diagnostics=diagnostics,
        )

    # Step 5: dedupe identical rule references.
    #
    # Per design.md "Deduplicate narrowings — different modules
    # may declare the same triple": the dedup target is the case
    # where two different modules independently reference the
    # same rule file (e.g. both module A's `extends.B` and module
    # B's `provides.intra_cross_link_rules` listing the same
    # rules YAML by happenstance). We do NOT dedup multiple
    # rules with overlapping triples — those are distinct
    # operator-authored facts (e.g.
    # `citation_cites_paper_by_paper_id` and
    # `citation_cites_paper` both producing (citation, cites,
    # paper) are two intentionally separate rules with different
    # matcher fields). Keeping both preserves the legacy
    # `_inject_cross_link_narrowings` row count exactly; the
    # downstream `apply_composed_catalog` dedupes triples for
    # the DB write, so the catalog-level effect is identical.
    deduped: list[ResolvedRule] = []
    seen: set[tuple] = set()
    for rr in all_rules:
        key = (rr.source_module, rr.target_module, rr.rule.id)
        if key in seen:
            diagnostics.duplicate_narrowings_collapsed += 1
            continue
        seen.add(key)
        deduped.append(rr)

    return ResolvedCatalog(
        manifests=manifests,
        cross_link_rules=deduped,
        subtypes_by_module=subtypes_by_module,
        diagnostics=diagnostics,
    )


# ---------- internals ----------


def _load_rule_file(
    rule_file: Path,
    *,
    source: str,
    target: Optional[str],
    diagnostics: ResolverDiagnostics,
) -> list[ResolvedRule]:
    """Load a single rule YAML file, attribute to (source, target).

    Missing rule files raise ResolverError loudly — a manifest
    that names a rule file path is expected to have that file on
    disk; silently dropping it would mask migration bugs.
    """
    if not rule_file.is_file():
        raise ResolverError(
            f"module {source!r}: cross-link rule file not found: "
            f"{rule_file}",
            data={
                "rule_file": str(rule_file),
                "source_module": source,
                "target_module": target or "",
            },
            fix_hint=(
                "create the rule file at the path declared in the "
                "manifest, or remove the reference from the manifest"
            ),
        )
    try:
        rules = load_rules_file(rule_file)
    except Exception as e:
        raise ResolverError(
            f"module {source!r}: rule file {rule_file}: {e}",
            data={
                "rule_file": str(rule_file),
                "source_module": source,
            },
            fix_hint="fix the YAML rule file syntax",
        ) from e
    diagnostics.rule_files_loaded.append(str(rule_file))
    return [
        ResolvedRule(
            rule=r,
            source_module=source,
            target_module=target,
            rule_file=rule_file,
        )
        for r in rules
    ]


def _validate_rule_subtypes(
    rr: ResolvedRule,
    *,
    available_subtypes: set[str],
    diagnostics: ResolverDiagnostics,
) -> None:
    """Every rule's src + dst subtypes (except alias_table) must
    resolve against `available_subtypes`.

    Silent skip per the legacy convention happens at narrowing
    *emit* time (see `modules_compose._inject_cross_link_narrowings`).
    The resolver validates *manifest-declared* references — a
    rule that names a subtype no loaded module provides is a
    composition bug, not a legitimate cross-module skip.
    """
    rule = rr.rule
    for subtype in rule.src.subtypes:
        if subtype not in available_subtypes:
            diagnostics.unresolved_subtypes.append({
                "rule_file": str(rr.rule_file),
                "rule_id": rule.id,
                "subtype": subtype,
                "endpoint": "src",
            })
            raise ResolverError(
                f"rule {rule.id!r} in {rr.rule_file}: src subtype "
                f"{subtype!r} not provided by any loaded module",
                data={
                    "rule_file": str(rr.rule_file),
                    "rule_id": rule.id,
                    "unresolved_subtype": subtype,
                    "endpoint": "src",
                    "source_module": rr.source_module,
                    "target_module": rr.target_module or "",
                },
                fix_hint=(
                    "either add the providing module to the "
                    "deployment's modules: list, or remove the "
                    "rule from the manifest"
                ),
            )
    if rr.rule.src.via_edge is not None:
        via_dst = rr.rule.src.via_edge.dst_subtype
        if via_dst not in available_subtypes:
            diagnostics.unresolved_subtypes.append({
                "rule_file": str(rr.rule_file),
                "rule_id": rule.id,
                "subtype": via_dst,
                "endpoint": "src.via_edge.dst_subtype",
            })
            raise ResolverError(
                f"rule {rule.id!r} in {rr.rule_file}: "
                f"src.via_edge.dst_subtype {via_dst!r} not "
                f"provided by any loaded module",
                data={
                    "rule_file": str(rr.rule_file),
                    "rule_id": rule.id,
                    "unresolved_subtype": via_dst,
                    "endpoint": "src.via_edge.dst_subtype",
                    "source_module": rr.source_module,
                    "target_module": rr.target_module or "",
                },
            )
    if rule.dst.via == "alias_table":
        return  # dst is the union of alias-bearing subtypes; runtime resolves
    for subtype in rule.dst.subtypes:
        if subtype not in available_subtypes:
            diagnostics.unresolved_subtypes.append({
                "rule_file": str(rr.rule_file),
                "rule_id": rule.id,
                "subtype": subtype,
                "endpoint": "dst",
            })
            raise ResolverError(
                f"rule {rule.id!r} in {rr.rule_file}: dst subtype "
                f"{subtype!r} not provided by any loaded module",
                data={
                    "rule_file": str(rr.rule_file),
                    "rule_id": rule.id,
                    "unresolved_subtype": subtype,
                    "endpoint": "dst",
                    "source_module": rr.source_module,
                    "target_module": rr.target_module or "",
                },
            )


def _extract_module_subtypes(
    manifest: ModuleManifest,
) -> set[str]:
    """Read the module's `provides.subtypes_file` (default
    `schema.yaml`) and return the set of subtype names it
    declares (in snake_case, matching `modules_compose`).

    Tolerant of missing schema files — returns an empty set rather
    than raising so legacy hermetic fixtures and scaffolded modules
    still compose. Malformed YAML fails loudly here; otherwise the
    resolver reports a misleading unresolved-subtype error later.
    """
    import yaml as _yaml

    from okg.substrate import fastyaml
    from okg.substrate.catalog.modules_compose import _pascal_to_snake

    schema_path = manifest.module_dir / manifest.provides.subtypes_file
    if not schema_path.is_file():
        return set()
    try:
        with schema_path.open() as f:
            data = fastyaml.safe_load(f) or {}
    except OSError:
        return set()
    except _yaml.YAMLError as e:
        raise ResolverError(
            f"module {manifest.name!r}: schema file {schema_path}: "
            f"YAML parse error: {e}",
            data={
                "module_name": manifest.name,
                "schema_path": str(schema_path),
            },
            fix_hint="fix the YAML syntax in the module schema",
        ) from e
    classes = data.get("classes") or {}
    out: set[str] = set()
    for cls_name, cls_def in classes.items():
        if cls_name == "EdgeNarrowings":
            continue
        if not isinstance(cls_def, dict):
            continue
        annotations = cls_def.get("annotations") or {}
        if "archetype" not in annotations:
            continue
        out.add(_pascal_to_snake(cls_name))
    return out


# ---------- discovery helper (resolver-facing) ----------


def discover_available_modules(modules_root: Path) -> dict[str, ModuleManifest]:
    """Walk `modules_root/*/module.yaml` and return name → loaded
    manifest. Useful for `okg doctor` and the `okg init` profile
    flow (sister change `add-okg-init-and-module-scaffold-cli`).
    """
    name_to_path = discover_manifests(modules_root)
    return {
        name: load_manifest(path) for name, path in name_to_path.items()
    }
