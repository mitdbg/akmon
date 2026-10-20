"""Descriptor-only deployment wiring resolver.

This module reads deployment-owned descriptors and returns an inspectable plan.
It deliberately does not instantiate deployment classes, import deployment
workflow modules, or open database connections. Import validation is limited to
module-spec discovery so callers can surface likely typos before runtime without
executing custom code.
"""
from __future__ import annotations

import importlib
import importlib.machinery
import sys
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from yaml.nodes import MappingNode

from okg.substrate import fastyaml
from okg.substrate.catalog.invariants import (
    discover_invariant_files,
    sql_contains_writes,
)
from okg.substrate.catalog.module_providers import (
    ModuleProvider,
    ModuleProviderError,
    normalize_providers,
    providers_from_modules_root,
    resolve_modules,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.producer_authority import (
    check_admission_policy_block_consistency,
)
from okg.substrate.runtime.config import RuntimeConfig
from okg.substrate.sources.registry import load_source_registry
from okg.substrate.views.registry import parse_views_registry


__all__ = [
    "DeploymentWiringPlan",
    "WiringComponent",
    "WiringDiagnostic",
    "WiringOrigin",
    "resolve_deployment_wiring_plan",
]


_SQL_GATE_ALLOWED_KEYS = {
    "name",
    "type",
    "check_id",
    "predicate_family",
    "confidence_type",
    "candidate_sql",
    "coverage_sql",
    "evaluator_ref",
    "budget_ms",
    "timeout_ms",
    "relation_inputs",
    "candidate_rule_hash",
    "evaluator_hash",
    "compute_hash",
    "output_schema_hash",
    "renderer_hash",
    "dirty_rule_hash",
    "coverage_contract",
    "severity_policy",
    "backpressure",
    "description",
}
_SQL_GATE_CONFIDENCE_TYPES = {
    "deterministic_boolean",
    "calibrated_probability",
    "thresholded_score",
    "ordinal_label",
}


@dataclass(frozen=True)
class WiringOrigin:
    """Where a resolved wiring component came from."""

    kind: str
    path: str | None = None
    pointer: str | None = None
    custom_code: bool = False
    note: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "path": self.path,
            "pointer": self.pointer,
            "custom_code": self.custom_code,
            "note": self.note,
        }


@dataclass(frozen=True)
class WiringComponent:
    """One descriptor in the effective deployment plan."""

    kind: str
    component_id: str
    origin: WiringOrigin
    attrs: Mapping[str, Any] = field(default_factory=dict)
    import_ref: str | None = None

    @property
    def custom_code(self) -> bool:
        return self.origin.custom_code

    def as_dict(self) -> dict[str, Any]:
        payload = {
            "kind": self.kind,
            "component_id": self.component_id,
            "origin": self.origin.as_dict(),
            "attrs": _jsonable(self.attrs),
            "custom_code": self.custom_code,
        }
        if self.import_ref:
            payload["import_ref"] = self.import_ref
        return payload


@dataclass(frozen=True)
class WiringDiagnostic:
    """Structured problem found while resolving descriptor wiring."""

    code: str
    severity: str
    stage: str
    path: str | None = None
    pointer: str | None = None
    component_id: str | None = None
    observed: Any = None
    expected: Any = None
    next_action: str | None = None
    message: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "severity": self.severity,
            "stage": self.stage,
            "path": self.path,
            "pointer": self.pointer,
            "component_id": self.component_id,
            "observed": _jsonable(self.observed),
            "expected": _jsonable(self.expected),
            "next_action": self.next_action,
            "message": self.message,
        }


@dataclass(frozen=True)
class DeploymentWiringPlan:
    """Descriptor-only effective wiring plan for a deployment."""

    deployment_name: str
    deployment_dir: Path
    sources: tuple[WiringComponent, ...] = ()
    source_dependencies: tuple[WiringComponent, ...] = ()
    modules: tuple[WiringComponent, ...] = ()
    views: tuple[WiringComponent, ...] = ()
    runtime: tuple[WiringComponent, ...] = ()
    enrichers: tuple[WiringComponent, ...] = ()
    checkpoint_attestors: tuple[WiringComponent, ...] = ()
    jobs: tuple[WiringComponent, ...] = ()
    gates: tuple[WiringComponent, ...] = ()
    operator_surfaces: tuple[WiringComponent, ...] = ()
    diagnostics: tuple[WiringDiagnostic, ...] = ()
    active: bool = True

    @property
    def ok(self) -> bool:
        return not any(d.severity == "error" for d in self.diagnostics)

    def diagnostics_by_code(self, code: str) -> tuple[WiringDiagnostic, ...]:
        return tuple(d for d in self.diagnostics if d.code == code)

    def as_dict(self) -> dict[str, Any]:
        return {
            "deployment_name": self.deployment_name,
            "deployment_dir": str(self.deployment_dir),
            "ok": self.ok,
            "active": self.active,
            "executable": self.active and self.ok,
            "sources": [c.as_dict() for c in self.sources],
            "source_dependencies": [
                c.as_dict() for c in self.source_dependencies
            ],
            "modules": [c.as_dict() for c in self.modules],
            "views": [c.as_dict() for c in self.views],
            "runtime": [c.as_dict() for c in self.runtime],
            "enrichers": [c.as_dict() for c in self.enrichers],
            "checkpoint_attestors": [
                c.as_dict() for c in self.checkpoint_attestors
            ],
            "jobs": [c.as_dict() for c in self.jobs],
            "gates": [c.as_dict() for c in self.gates],
            "operator_surfaces": [
                c.as_dict() for c in self.operator_surfaces
            ],
            "diagnostics": [d.as_dict() for d in self.diagnostics],
        }


@dataclass(frozen=True)
class _ImportRef:
    ref: str
    module: str
    attr: str | None = None


@dataclass
class _ResolverState:
    deployment_dir: Path
    deployment_name: str
    check_imports: bool
    modules_root: Path
    # Owner-keyed module providers (#1179). `modules_root` remains the
    # built-ins-only default and is wrapped as the single `okg` provider,
    # so a caller that passes neither sees exactly today's resolution.
    providers: Mapping[str, ModuleProvider] = field(default_factory=dict)
    diagnostics: list[WiringDiagnostic] = field(default_factory=list)
    sources: list[WiringComponent] = field(default_factory=list)
    source_dependencies: list[WiringComponent] = field(default_factory=list)
    modules: list[WiringComponent] = field(default_factory=list)
    views: list[WiringComponent] = field(default_factory=list)
    runtime: list[WiringComponent] = field(default_factory=list)
    enrichers: list[WiringComponent] = field(default_factory=list)
    checkpoint_attestors: list[WiringComponent] = field(default_factory=list)
    jobs: list[WiringComponent] = field(default_factory=list)
    gates: list[WiringComponent] = field(default_factory=list)
    operator_surfaces: list[WiringComponent] = field(default_factory=list)

    def diag(
        self,
        *,
        code: str,
        stage: str,
        severity: str = "error",
        path: Path | str | None = None,
        pointer: str | None = None,
        component_id: str | None = None,
        observed: Any = None,
        expected: Any = None,
        next_action: str | None = None,
        message: str | None = None,
    ) -> None:
        self.diagnostics.append(WiringDiagnostic(
            code=code,
            severity=severity,
            stage=stage,
            path=str(path) if path is not None else None,
            pointer=pointer,
            component_id=component_id,
            observed=observed,
            expected=expected,
            next_action=next_action,
            message=message,
        ))


def resolve_deployment_wiring_plan(
    deployment_dir: Path | str,
    *,
    modules_root: Path | str | None = None,
    providers: Mapping[str, ModuleProvider] | None = None,
    check_imports: bool = True,
) -> DeploymentWiringPlan:
    """Resolve deployment wiring from descriptors only.

    Parameters
    ----------
    deployment_dir:
        Directory containing ``deployment.yaml``.
    modules_root:
        Optional substrate library ontology root used for tests. Defaults to
        ``src/okg/substrate/library/ontologies``.
    providers:
        Optional owner-keyed
        :class:`~okg.substrate.catalog.module_providers.ModuleProvider`
        mapping, normally built from a deployment's install lock. Omitted,
        ``modules_root`` is wrapped as the single ``okg`` provider, which is
        what every caller had before packaged modules existed. Passing both
        refuses.
    check_imports:
        When true, validate Python references with importlib module specs only.
        The resolver never imports or instantiates referenced classes.
    """

    dep_dir = Path(deployment_dir).resolve()
    manifest_path = dep_dir / "deployment.yaml"
    manifest, manifest_load_diags = _load_yaml_mapping(manifest_path)
    deployment_name = str(manifest.get("name") or dep_dir.name)
    if providers is not None and modules_root is not None:
        raise ModuleProviderError(
            "pass either providers= or modules_root=, not both",
            data={"owners": sorted(providers), "modules_root": str(modules_root)},
            fix_hint="drop modules_root= and put the path in a ModuleProvider",
        )
    root = Path(modules_root).resolve() if modules_root else _default_modules_root()
    resolved_providers = (
        normalize_providers(providers)
        if providers is not None
        else providers_from_modules_root(root)
    )
    state = _ResolverState(
        deployment_dir=dep_dir,
        deployment_name=deployment_name,
        check_imports=check_imports,
        modules_root=root,
        providers=resolved_providers,
        diagnostics=list(manifest_load_diags),
    )

    _resolve_manifest_level(state, manifest, manifest_path)
    _resolve_source_registry(state, manifest, manifest_path)
    _resolve_modules(state, manifest, manifest_path)
    _resolve_views(state)
    _resolve_runtime(state, manifest, manifest_path)
    _resolve_enrichers(state, manifest, manifest_path)
    _resolve_checkpoint_attestors(state, manifest_path)
    _resolve_jobs(state, manifest, manifest_path)
    _resolve_gates(state, manifest, manifest_path)
    _resolve_operator_surfaces(state, manifest, manifest_path)

    from okg.substrate.deployments.admission import archived_deployment

    return DeploymentWiringPlan(
        active=archived_deployment(dep_dir) is None,
        deployment_name=deployment_name,
        deployment_dir=dep_dir,
        sources=tuple(state.sources),
        source_dependencies=tuple(state.source_dependencies),
        modules=tuple(state.modules),
        views=tuple(state.views),
        runtime=tuple(state.runtime),
        enrichers=tuple(state.enrichers),
        checkpoint_attestors=tuple(state.checkpoint_attestors),
        jobs=tuple(state.jobs),
        gates=tuple(state.gates),
        operator_surfaces=tuple(state.operator_surfaces),
        diagnostics=tuple(state.diagnostics),
    )


def _resolve_manifest_level(
    state: _ResolverState,
    manifest: Mapping[str, Any],
    manifest_path: Path,
) -> None:
    if not manifest:
        state.diag(
            code="deployment_manifest_missing",
            stage="deployment",
            path=manifest_path,
            expected="deployment.yaml mapping",
            next_action="create deployment.yaml before resolving wiring",
        )
        return
    if not isinstance(manifest, Mapping):
        state.diag(
            code="deployment_manifest_invalid",
            stage="deployment",
            path=manifest_path,
            observed=type(manifest).__name__,
            expected="mapping",
            next_action="make deployment.yaml a YAML mapping",
        )


def _resolve_source_registry(
    state: _ResolverState,
    manifest: Mapping[str, Any],
    manifest_path: Path,
) -> None:
    registry_path = _manifest_relative_path(
        state.deployment_dir,
        manifest.get("source_registry") or "source_registry.yaml",
    )
    duplicates = _duplicate_keys_under(registry_path, "sources")
    for duplicate in duplicates:
        state.diag(
            code="duplicate_source_id",
            stage="sources",
            path=registry_path,
            pointer=f"/sources/{duplicate['key']}",
            component_id=str(duplicate["key"]),
            observed={
                "source_id": duplicate["key"],
                "line": duplicate["line"],
            },
            expected="unique source ids",
            next_action="rename or remove one duplicate source entry",
        )

    registry, diags = _load_yaml_mapping(
        registry_path,
        missing_code="source_registry_missing",
        invalid_code="source_registry_invalid",
        stage="sources",
    )
    state.diagnostics.extend(diags)
    raw_sources = registry.get("sources") if isinstance(registry, Mapping) else None
    if raw_sources is None:
        raw_sources = {}
    if not isinstance(raw_sources, Mapping):
        state.diag(
            code="source_registry_sources_invalid",
            stage="sources",
            path=registry_path,
            pointer="/sources",
            observed=type(raw_sources).__name__,
            expected="mapping of source id to source descriptor",
            next_action="make source_registry.yaml sources a mapping",
        )
        return
    try:
        load_source_registry(registry_path)
    except Exception as exc:  # noqa: BLE001 - normalized into diagnostics.
        state.diag(
            code="source_registry_loader_invalid",
            stage="sources",
            path=registry_path,
            observed=str(exc),
            expected="source registry accepted by runtime loader",
            next_action="fix source_registry.yaml loader validation errors",
        )

    ownership_to_sources: dict[str, list[str]] = {}
    source_order: list[str] = []
    for raw_source_id, raw_body in raw_sources.items():
        source_id = str(raw_source_id)
        source_order.append(source_id)
        if not isinstance(raw_body, Mapping):
            state.diag(
                code="source_descriptor_invalid",
                stage="sources",
                path=registry_path,
                pointer=f"/sources/{source_id}",
                component_id=source_id,
                observed=type(raw_body).__name__,
                expected="mapping",
                next_action="make the source entry a YAML mapping",
            )
            continue
        ownership_id = raw_body.get("ownership_id")
        if ownership_id:
            ownership_to_sources.setdefault(str(ownership_id), []).append(
                source_id,
            )

        module = raw_body.get("module")
        class_name = raw_body.get("class")
        import_ref = None
        custom_code = False
        if isinstance(module, str) and isinstance(class_name, str):
            ref = _ImportRef(
                ref=f"{module}:{class_name}",
                module=module,
                attr=class_name,
            )
            import_ref = ref.ref
            custom_code = _is_custom_module(ref.module)
            _check_import_ref(
                state,
                ref,
                stage="sources",
                path=registry_path,
                pointer=f"/sources/{source_id}",
                component_id=source_id,
            )
        elif module or class_name:
            state.diag(
                code="malformed_import_ref",
                stage="sources",
                path=registry_path,
                pointer=f"/sources/{source_id}",
                component_id=source_id,
                observed={"module": module, "class": class_name},
                expected="both module and class strings",
                next_action="declare both module and class for source code",
            )

        state.sources.append(WiringComponent(
            kind="source",
            component_id=source_id,
            origin=WiringOrigin(
                "source_registry",
                path=str(registry_path),
                pointer=f"/sources/{source_id}",
                custom_code=custom_code,
            ),
            import_ref=import_ref,
            attrs={
                "ownership_id": ownership_id,
                "source_class": raw_body.get("source_class"),
                "module": module,
                "class": class_name,
                "legacy": "source_class" not in raw_body,
                "registry_only": raw_body.get("source_class") == "registry_only",
                "required_for_baseline": raw_body.get(
                    "required_for_baseline",
                    False,
                ),
                "enabled_by_default": raw_body.get("enabled_by_default"),
                "has_sync": isinstance(raw_body.get("sync"), Mapping),
                "has_admission_policy": isinstance(
                    raw_body.get("admission_policy"),
                    Mapping,
                ),
            },
        ))

        admission_policy = raw_body.get("admission_policy")
        if isinstance(admission_policy, Mapping):
            for problem in check_admission_policy_block_consistency(
                admission_policy,
                source_name=source_id,
            ):
                state.diag(
                    code="source_admission_policy_block_drift",
                    stage="sources",
                    path=registry_path,
                    pointer=f"/sources/{source_id}/admission_policy",
                    component_id=source_id,
                    observed=problem,
                    expected=(
                        "output_signature, policy_clauses, and "
                        "output_scope_summary declare the same "
                        "subtype/triple/clause_id set"
                    ),
                    next_action=(
                        "reconcile the admission_policy blocks so the "
                        "signature, clauses, and scope summary agree"
                    ),
                )

    for ownership_id, source_ids in sorted(ownership_to_sources.items()):
        if len(source_ids) <= 1:
            continue
        state.diag(
            code="duplicate_source_ownership_id",
            stage="sources",
            path=registry_path,
            component_id=ownership_id,
            observed={"ownership_id": ownership_id, "source_ids": source_ids},
            expected="unique ownership_id values",
            next_action="give each source a distinct ownership_id",
        )

    ownership_to_source = {
        ownership_id: ids[0]
        for ownership_id, ids in ownership_to_sources.items()
        if len(ids) == 1
    }
    known_refs = set(source_order) | set(ownership_to_source)
    deps: dict[str, set[str]] = {source_id: set() for source_id in source_order}
    for raw_source_id, raw_body in raw_sources.items():
        source_id = str(raw_source_id)
        if not isinstance(raw_body, Mapping):
            continue
        pred_raw = raw_body.get("predecessor_source")
        params = raw_body.get("params")
        if pred_raw is None and isinstance(params, Mapping):
            pred_raw = params.get("predecessor_source")
        for pred in _as_string_list(pred_raw):
            resolved = None
            if pred in raw_sources:
                resolved = pred
            elif pred in ownership_to_source:
                resolved = ownership_to_source[pred]
            elif pred in ownership_to_sources:
                state.diag(
                    code="ambiguous_predecessor_source",
                    stage="source_dependencies",
                    path=registry_path,
                    pointer=f"/sources/{source_id}/predecessor_source",
                    component_id=source_id,
                    observed=pred,
                    expected="a source id or unique ownership_id",
                    next_action=(
                        "disambiguate the predecessor_source or fix "
                        "duplicate ownership ids"
                    ),
                )
            else:
                state.diag(
                    code="unknown_predecessor_source",
                    stage="source_dependencies",
                    path=registry_path,
                    pointer=f"/sources/{source_id}/predecessor_source",
                    component_id=source_id,
                    observed=pred,
                    expected=sorted(known_refs),
                    next_action=(
                        "point predecessor_source at a source id or "
                        "ownership_id declared in source_registry.yaml"
                    ),
                )
            if resolved is not None:
                deps.setdefault(source_id, set()).add(resolved)
            state.source_dependencies.append(WiringComponent(
                kind="source_dependency",
                component_id=f"{source_id}->{pred}",
                origin=WiringOrigin(
                    "source_registry",
                    path=str(registry_path),
                    pointer=f"/sources/{source_id}/predecessor_source",
                ),
                attrs={
                    "source_id": source_id,
                    "predecessor_source": pred,
                    "resolved_source_id": resolved,
                },
            ))

        sync = raw_body.get("sync")
        if isinstance(sync, Mapping):
            _resolve_source_sync_jobs(
                state,
                source_id=source_id,
                sync=sync,
                registry_path=registry_path,
            )

    cycle = _find_cycle(deps)
    if cycle:
        state.diag(
            code="source_dependency_cycle",
            stage="source_dependencies",
            path=registry_path,
            observed=cycle,
            expected="acyclic predecessor_source graph",
            next_action="remove or invert one predecessor_source edge",
            message="cycle in source predecessor declarations",
        )


def _resolve_source_sync_jobs(
    state: _ResolverState,
    *,
    source_id: str,
    sync: Mapping[str, Any],
    registry_path: Path,
) -> None:
    poll = sync.get("poll")
    triggers = _as_string_list(sync.get("triggers"))
    if isinstance(poll, Mapping) or "poll" in triggers:
        attrs = dict(poll) if isinstance(poll, Mapping) else {}
        attrs["source_id"] = source_id
        state.jobs.append(WiringComponent(
            kind="source_poll_job",
            component_id=f"source_poll:{source_id}",
            origin=WiringOrigin(
                "source_registry",
                path=str(registry_path),
                pointer=f"/sources/{source_id}/sync/poll",
            ),
            attrs=attrs,
        ))


def _resolve_modules(
    state: _ResolverState,
    manifest: Mapping[str, Any],
    manifest_path: Path,
) -> None:
    raw_modules = manifest.get("modules") or []
    if not isinstance(raw_modules, Sequence) or isinstance(raw_modules, str):
        state.diag(
            code="modules_invalid",
            stage="modules",
            path=manifest_path,
            pointer="/modules",
            observed=type(raw_modules).__name__,
            expected="list of module names",
            next_action="make deployment.yaml modules a list",
        )
        return

    seen: set[str] = set()
    for index, item in enumerate(raw_modules):
        if not isinstance(item, str) or not item.strip():
            state.diag(
                code="module_ref_invalid",
                stage="modules",
                path=manifest_path,
                pointer=f"/modules/{index}",
                observed=item,
                expected="non-empty module name",
                next_action="replace the module entry with a module name",
            )
            continue
        module_name = item.strip()
        if module_name in seen:
            state.diag(
                code="duplicate_module_id",
                stage="modules",
                path=manifest_path,
                pointer=f"/modules/{index}",
                component_id=module_name,
                observed=module_name,
                expected="unique module names",
                next_action="remove the duplicate module entry",
            )
        seen.add(module_name)
        try:
            module_dir = resolve_modules(
                [module_name], state.providers,
            )[module_name].module_dir
        except SubstrateError as exc:
            state.diag(
                code=exc.code,
                stage="modules",
                path=manifest_path,
                pointer=f"/modules/{index}",
                component_id=module_name,
                observed=module_name,
                expected="a module its provider supplies",
                next_action="install the owner's distribution, or fix the name",
                message=str(exc),
            )
            continue
        manifest_file = module_dir / "module.yaml"
        attrs: dict[str, Any] = {"name": module_name}
        origin_note = None
        if manifest_file.is_file():
            module_doc, diags = _load_yaml_mapping(
                manifest_file,
                missing_code="module_manifest_missing",
                invalid_code="module_manifest_invalid",
                stage="modules",
            )
            state.diagnostics.extend(diags)
            attrs.update(_module_attrs(module_doc))
            _check_module_manifest_imports(
                state,
                module_doc,
                module_name=module_name,
                manifest_file=manifest_file,
            )
            _add_module_manifest_components(
                state,
                module_doc,
                module_name=module_name,
                manifest_file=manifest_file,
            )
            origin_note = "module.yaml resolved"
        else:
            state.diag(
                code="module_manifest_missing",
                stage="modules",
                path=manifest_file,
                component_id=module_name,
                expected=str(manifest_file),
                next_action=(
                    "create the module manifest or remove the module "
                    "from deployment.yaml"
                ),
            )
            origin_note = "module.yaml missing"
        state.modules.append(WiringComponent(
            kind="module",
            component_id=module_name,
            origin=WiringOrigin(
                "deployment_manifest",
                path=str(manifest_path),
                pointer=f"/modules/{index}",
                note=origin_note,
            ),
            attrs=attrs,
        ))


def _module_attrs(module_doc: Mapping[str, Any]) -> dict[str, Any]:
    provides = module_doc.get("provides")
    extends = module_doc.get("extends")
    if not isinstance(provides, Mapping):
        provides = {}
    if not isinstance(extends, Mapping):
        extends = {}
    return {
        "manifest_name": module_doc.get("name"),
        "version": module_doc.get("version"),
        "provides": {
            "subtypes_file": provides.get("subtypes_file", "schema.yaml"),
            "edge_types": list(_as_string_list(provides.get("edge_types"))),
            "source_count": len(_as_list(provides.get("sources"))),
            "enricher_count": len(_as_list(provides.get("enrichers"))),
            "invariants_preset": provides.get("invariants_preset"),
            "dashboards": provides.get("dashboards"),
            "intra_cross_link_rules": list(
                _as_string_list(provides.get("intra_cross_link_rules")),
            ),
        },
        "extends": sorted(str(key) for key in extends),
    }


def _check_module_manifest_imports(
    state: _ResolverState,
    module_doc: Mapping[str, Any],
    *,
    module_name: str,
    manifest_file: Path,
) -> None:
    provides = module_doc.get("provides")
    if not isinstance(provides, Mapping):
        return
    for index, item in enumerate(_as_list(provides.get("sources"))):
        if not isinstance(item, Mapping):
            continue
        class_path = item.get("class")
        ref = _dotted_import_ref(class_path)
        if ref is not None:
            _check_import_ref(
                state,
                ref,
                stage="modules",
                path=manifest_file,
                pointer=f"/provides/sources/{index}/class",
                component_id=module_name,
            )
    for index, item in enumerate(_as_list(provides.get("enrichers"))):
        if not isinstance(item, Mapping):
            continue
        class_path = item.get("class")
        ref = _dotted_import_ref(class_path)
        if ref is not None:
            _check_import_ref(
                state,
                ref,
                stage="modules",
                path=manifest_file,
                pointer=f"/provides/enrichers/{index}/class",
                component_id=module_name,
            )


def _add_module_manifest_components(
    state: _ResolverState,
    module_doc: Mapping[str, Any],
    *,
    module_name: str,
    manifest_file: Path,
) -> None:
    provides = module_doc.get("provides")
    if not isinstance(provides, Mapping):
        return
    for index, item in enumerate(_as_list(provides.get("sources"))):
        if not isinstance(item, Mapping):
            continue
        class_path = item.get("class")
        ref = _dotted_import_ref(class_path)
        custom = _is_custom_module(ref.module) if ref is not None else False
        state.sources.append(WiringComponent(
            kind="module_source",
            component_id=f"{module_name}.source.{index}",
            origin=WiringOrigin(
                "module_manifest",
                path=str(manifest_file),
                pointer=f"/provides/sources/{index}",
                custom_code=custom,
            ),
            attrs={
                "module": module_name,
                "class": class_path,
                "profile": item.get("profile"),
            },
            import_ref=ref.ref if ref is not None else None,
        ))
    for index, item in enumerate(_as_list(provides.get("enrichers"))):
        if not isinstance(item, Mapping):
            continue
        class_path = item.get("class")
        ref = _dotted_import_ref(class_path)
        custom = _is_custom_module(ref.module) if ref is not None else False
        state.enrichers.append(WiringComponent(
            kind="module_enricher",
            component_id=f"{module_name}.enricher.{index}",
            origin=WiringOrigin(
                "module_manifest",
                path=str(manifest_file),
                pointer=f"/provides/enrichers/{index}",
                custom_code=custom,
            ),
            attrs={
                "module": module_name,
                "class": class_path,
                "profile": item.get("profile"),
            },
            import_ref=ref.ref if ref is not None else None,
        ))


def _resolve_views(state: _ResolverState) -> None:
    registry_path = state.deployment_dir / "views_registry.yaml"
    if not registry_path.is_file():
        return
    registry, diags = _load_yaml_mapping(
        registry_path,
        missing_code="views_registry_missing",
        invalid_code="views_registry_invalid",
        stage="views",
    )
    state.diagnostics.extend(diags)
    if not isinstance(registry, Mapping):
        return
    try:
        parse_views_registry(dict(registry), source_path=registry_path)
    except Exception as exc:  # noqa: BLE001 - normalized into diagnostics.
        state.diag(
            code="views_registry_loader_invalid",
            stage="views",
            path=registry_path,
            observed=str(exc),
            expected="views registry accepted by runtime loader",
            next_action="fix views_registry.yaml loader validation errors",
        )
    if registry.get("enabled") is False:
        state.views.append(WiringComponent(
            kind="views_registry",
            component_id="views.disabled",
            origin=WiringOrigin(
                "views_registry",
                path=str(registry_path),
                pointer="/enabled",
            ),
            attrs={"enabled": False},
        ))
        return
    raw_views = registry.get("views") or []
    if not isinstance(raw_views, Sequence) or isinstance(raw_views, str):
        state.diag(
            code="views_invalid",
            stage="views",
            path=registry_path,
            pointer="/views",
            observed=type(raw_views).__name__,
            expected="list of view descriptors",
            next_action="make views_registry.yaml views a list",
        )
        return
    seen: set[str] = set()
    for index, raw in enumerate(raw_views):
        if not isinstance(raw, Mapping):
            state.diag(
                code="view_descriptor_invalid",
                stage="views",
                path=registry_path,
                pointer=f"/views/{index}",
                observed=type(raw).__name__,
                expected="mapping",
                next_action="make the view entry a mapping",
            )
            continue
        view_id = str(raw.get("view_def_id") or f"<missing:{index}>")
        if not raw.get("view_def_id"):
            state.diag(
                code="view_missing_id",
                stage="views",
                path=registry_path,
                pointer=f"/views/{index}/view_def_id",
                component_id=view_id,
                expected="view_def_id",
                next_action="add view_def_id",
            )
        if view_id in seen:
            state.diag(
                code="duplicate_view_id",
                stage="views",
                path=registry_path,
                pointer=f"/views/{index}/view_def_id",
                component_id=view_id,
                observed=view_id,
                expected="unique view_def_id values",
                next_action="rename or remove the duplicate view",
            )
        seen.add(view_id)
        import_refs: list[str] = []
        custom = False
        for field_name in (
            "view_def_factory",
            "build_corpus",
            "anchor_resolver",
        ):
            value = raw.get(field_name)
            if not value:
                if field_name != "anchor_resolver":
                    state.diag(
                        code="view_missing_import_ref",
                        stage="views",
                        path=registry_path,
                        pointer=f"/views/{index}/{field_name}",
                        component_id=view_id,
                        expected=f"{field_name}: module:attribute",
                        next_action=f"add {field_name}",
                    )
                continue
            ref = _colon_import_ref(value)
            if ref is None:
                state.diag(
                    code="malformed_import_ref",
                    stage="views",
                    path=registry_path,
                    pointer=f"/views/{index}/{field_name}",
                    component_id=view_id,
                    observed=value,
                    expected="module:attribute",
                    next_action="fix the import reference shape",
                )
                continue
            import_refs.append(ref.ref)
            custom = custom or _is_custom_module(ref.module)
            _check_import_ref(
                state,
                ref,
                stage="views",
                path=registry_path,
                pointer=f"/views/{index}/{field_name}",
                component_id=view_id,
            )
        state.views.append(WiringComponent(
            kind="view",
            component_id=view_id,
            origin=WiringOrigin(
                "views_registry",
                path=str(registry_path),
                pointer=f"/views/{index}",
                custom_code=custom,
            ),
            attrs={
                "view_name": raw.get("view_name") or view_id,
                "schedule": raw.get("schedule") or "",
                "llm_client": raw.get("llm_client") or "stub",
                "import_refs": import_refs,
                "reads_subtypes": list(
                    _as_string_list(raw.get("reads_subtypes")),
                ),
                "reads_edge_types": list(
                    _as_string_list(raw.get("reads_edge_types")),
                ),
            },
        ))


def _resolve_runtime(
    state: _ResolverState,
    manifest: Mapping[str, Any],
    manifest_path: Path,
) -> None:
    runtime_raw = manifest.get("runtime")
    try:
        runtime_cfg = RuntimeConfig.from_manifest_block(
            runtime_raw if isinstance(runtime_raw, dict) else None,
        )
        if runtime_raw is not None and not isinstance(runtime_raw, dict):
            raise ValueError(
                f"runtime: block must be a mapping, got "
                f"{type(runtime_raw).__name__}"
            )
    except ValueError as exc:
        state.diag(
            code="runtime_config_invalid",
            stage="runtime",
            path=manifest_path,
            pointer="/runtime",
            observed=str(exc),
            expected="valid runtime manifest block",
            next_action="fix deployment.yaml runtime configuration",
        )
        runtime_cfg = RuntimeConfig()
    state.runtime.append(WiringComponent(
        kind="runtime_config",
        component_id="runtime",
        origin=WiringOrigin(
            "deployment_manifest"
            if runtime_raw is not None else "runtime_default",
            path=str(manifest_path) if runtime_raw is not None else None,
            pointer="/runtime" if runtime_raw is not None else None,
        ),
        attrs=runtime_cfg.as_dict(),
    ))

    for index, module_path in enumerate(runtime_cfg.workflows):
        ref = _module_import_ref(module_path)
        custom = _is_custom_module(ref.module)
        _check_import_ref(
            state,
            ref,
            stage="runtime",
            path=manifest_path,
            pointer=f"/runtime/workflows/{index}",
            component_id=module_path,
        )
        state.runtime.append(WiringComponent(
            kind="runtime_workflow",
            component_id=module_path,
            origin=WiringOrigin(
                "deployment_manifest",
                path=str(manifest_path),
                pointer=f"/runtime/workflows/{index}",
                custom_code=custom,
            ),
            attrs={"module": module_path},
            import_ref=module_path,
        ))

    source_sync = runtime_cfg.as_dict().get("source_sync") or {}
    if source_sync:
        state.runtime.append(WiringComponent(
            kind="runtime_source_sync",
            component_id="runtime.source_sync",
            origin=WiringOrigin(
                "deployment_manifest"
                if isinstance(runtime_raw, dict)
                and "source_sync" in runtime_raw else "runtime_default",
                path=str(manifest_path) if isinstance(runtime_raw, dict) else None,
                pointer="/runtime/source_sync"
                if isinstance(runtime_raw, dict)
                and "source_sync" in runtime_raw else None,
            ),
            attrs=source_sync,
        ))


def _resolve_enrichers(
    state: _ResolverState,
    manifest: Mapping[str, Any],
    manifest_path: Path,
) -> None:
    raw_enrichers = manifest.get("enrichers") or []
    if not isinstance(raw_enrichers, Sequence) or isinstance(
        raw_enrichers,
        str,
    ):
        state.diag(
            code="enrichers_invalid",
            stage="enrichers",
            path=manifest_path,
            pointer="/enrichers",
            observed=type(raw_enrichers).__name__,
            expected="list of enricher descriptors",
            next_action="make deployment.yaml enrichers a list",
        )
        return
    for index, raw in enumerate(raw_enrichers):
        if not isinstance(raw, Mapping):
            state.diag(
                code="enricher_descriptor_invalid",
                stage="enrichers",
                path=manifest_path,
                pointer=f"/enrichers/{index}",
                observed=type(raw).__name__,
                expected="mapping with class",
                next_action="make the enricher entry a mapping",
            )
            continue
        class_path = raw.get("class")
        ref = _dotted_import_ref(class_path)
        import_ref = None
        custom = False
        component_id = str(class_path or f"<missing:{index}>")
        if ref is None:
            state.diag(
                code="malformed_import_ref",
                stage="enrichers",
                path=manifest_path,
                pointer=f"/enrichers/{index}/class",
                component_id=component_id,
                observed=class_path,
                expected="pkg.module.Class",
                next_action="declare an enricher class path",
            )
        else:
            import_ref = ref.ref
            custom = _is_custom_module(ref.module)
            _check_import_ref(
                state,
                ref,
                stage="enrichers",
                path=manifest_path,
                pointer=f"/enrichers/{index}/class",
                component_id=component_id,
            )
        state.enrichers.append(WiringComponent(
            kind="enricher",
            component_id=component_id,
            origin=WiringOrigin(
                "deployment_manifest",
                path=str(manifest_path),
                pointer=f"/enrichers/{index}",
                custom_code=custom,
            ),
            attrs={
                "class": class_path,
                "config_keys": sorted(
                    str(k) for k in (raw.get("config") or {})
                ) if isinstance(raw.get("config") or {}, Mapping) else [],
                "has_authority": isinstance(raw.get("authority"), Mapping),
            },
            import_ref=import_ref,
        ))


    extractor = manifest.get("extractor")
    if isinstance(extractor, Mapping):
        class_path = extractor.get("class")
        ref = _dotted_import_ref(class_path)
        if ref is not None:
            _check_import_ref(
                state,
                ref,
                stage="enrichers",
                path=manifest_path,
                pointer="/extractor/class",
                component_id="extractor",
            )
            state.enrichers.append(WiringComponent(
                kind="projection_extractor",
                component_id="extractor",
                origin=WiringOrigin(
                    "deployment_manifest",
                    path=str(manifest_path),
                    pointer="/extractor",
                    custom_code=_is_custom_module(ref.module),
                ),
                attrs={
                    "class": class_path,
                    "source_subtypes": list(
                        _as_string_list(extractor.get("source_subtypes")),
                    ),
                    "has_authority": isinstance(
                        extractor.get("authority"),
                        Mapping,
                    ),
                },
                import_ref=ref.ref,
            ))
        else:
            state.diag(
                code="malformed_import_ref",
                stage="enrichers",
                path=manifest_path,
                pointer="/extractor/class",
                component_id="extractor",
                observed=class_path,
                expected="pkg.module.Class",
                next_action="declare an extractor class path",
            )

    alias = manifest.get("alias_resolver")
    if isinstance(alias, Mapping):
        backends = alias.get("backends") or []
        for index, backend in enumerate(_as_list(backends)):
            if not isinstance(backend, Mapping):
                continue
            class_path = backend.get("class")
            ref = _dotted_import_ref(class_path)
            if ref is None:
                state.diag(
                    code="malformed_import_ref",
                    stage="enrichers",
                    path=manifest_path,
                    pointer=f"/alias_resolver/backends/{index}/class",
                    component_id=f"alias_resolver.backend.{index}",
                    observed=class_path,
                    expected="pkg.module.Class",
                    next_action="declare an alias resolver backend class path",
                )
                continue
            _check_import_ref(
                state,
                ref,
                stage="enrichers",
                path=manifest_path,
                pointer=f"/alias_resolver/backends/{index}/class",
                component_id=f"alias_resolver.backend.{index}",
            )
            state.enrichers.append(WiringComponent(
                kind="alias_backend",
                component_id=f"alias_resolver.backend.{index}",
                origin=WiringOrigin(
                    "deployment_manifest",
                    path=str(manifest_path),
                    pointer=f"/alias_resolver/backends/{index}",
                    custom_code=_is_custom_module(ref.module),
                ),
                attrs={
                    "class": class_path,
                    "config_keys": sorted(
                        str(k) for k in (backend.get("config") or {})
                    ) if isinstance(backend.get("config") or {}, Mapping)
                    else [],
                },
                import_ref=ref.ref,
            ))


def _resolve_checkpoint_attestors(
    state: _ResolverState,
    manifest_path: Path,
) -> None:
    from okg.substrate.publisher.checkpoint_attestations import (
        CheckpointAttestationConfigurationError,
        inspect_checkpoint_attestor_declarations,
    )

    try:
        declarations = inspect_checkpoint_attestor_declarations(
            state.deployment_dir,
            deployment_name=state.deployment_name,
        )
    except CheckpointAttestationConfigurationError as exc:
        state.diag(
            code="checkpoint_attestors_invalid",
            stage="checkpoint_attestors",
            path=manifest_path,
            pointer="/checkpoint_attestors",
            observed=str(exc),
            expected="strict ordered checkpoint attestor declarations",
            next_action="fix deployment.yaml checkpoint_attestors",
        )
        return

    for index, declaration in enumerate(declarations):
        module_name, separator, class_name = declaration.class_ref.partition(":")
        if not separator:
            module_name, _separator, class_name = declaration.class_ref.rpartition(".")
        ref = _ImportRef(
            ref=f"{module_name}:{class_name}",
            module=module_name,
            attr=class_name,
        )
        _check_import_ref(
            state,
            ref,
            stage="checkpoint_attestors",
            path=manifest_path,
            pointer=f"/checkpoint_attestors/{index}/class",
            component_id=declaration.manifest_id,
        )
        state.checkpoint_attestors.append(WiringComponent(
            kind="checkpoint_attestor",
            component_id=declaration.manifest_id,
            origin=WiringOrigin(
                "deployment_manifest",
                path=str(manifest_path),
                pointer=f"/checkpoint_attestors/{index}",
                custom_code=_is_custom_module(module_name),
            ),
            attrs={
                "class": declaration.class_ref,
                "config_keys": sorted(str(key) for key in declaration.config),
            },
            import_ref=ref.ref,
        ))


def _resolve_jobs(
    state: _ResolverState,
    manifest: Mapping[str, Any],
    manifest_path: Path,
) -> None:
    runtime = manifest.get("runtime") if isinstance(manifest, Mapping) else {}
    schedules = runtime.get("schedules") if isinstance(runtime, Mapping) else {}
    if isinstance(schedules, Mapping):
        for schedule_id, cron in sorted(schedules.items()):
            state.jobs.append(WiringComponent(
                kind="runtime_schedule",
                component_id=str(schedule_id),
                origin=WiringOrigin(
                    "deployment_manifest",
                    path=str(manifest_path),
                    pointer=f"/runtime/schedules/{schedule_id}",
                ),
                attrs={"cron": cron},
            ))
    elif schedules is not None:
        state.diag(
            code="runtime_schedules_invalid",
            stage="jobs",
            path=manifest_path,
            pointer="/runtime/schedules",
            observed=type(schedules).__name__,
            expected="mapping of schedule id to cron expression",
            next_action="make runtime.schedules a mapping",
        )

    jobs_raw = manifest.get("jobs") or []
    if isinstance(jobs_raw, Mapping):
        iterable = [
            (str(key), value, f"/jobs/{key}")
            for key, value in jobs_raw.items()
        ]
    elif isinstance(jobs_raw, Sequence) and not isinstance(jobs_raw, str):
        iterable = [
            (
                str(item.get("job_name") or item.get("name") or i)
                if isinstance(item, Mapping) else str(i),
                item,
                f"/jobs/{i}",
            )
            for i, item in enumerate(jobs_raw)
        ]
    else:
        state.diag(
            code="jobs_invalid",
            stage="jobs",
            path=manifest_path,
            pointer="/jobs",
            observed=type(jobs_raw).__name__,
            expected="list or mapping of job descriptors",
            next_action="make deployment.yaml jobs a list or mapping",
        )
        iterable = []
    for job_id, raw, pointer in iterable:
        if not isinstance(raw, Mapping):
            state.diag(
                code="job_descriptor_invalid",
                stage="jobs",
                path=manifest_path,
                pointer=pointer,
                component_id=job_id,
                observed=type(raw).__name__,
                expected="mapping",
                next_action="make the job descriptor a mapping",
            )
            continue
        custom = False
        import_refs: list[str] = []
        for field_name in (
            "workflow",
            "handler",
            "input_frontier_builder",
        ):
            value = raw.get(field_name)
            if not isinstance(value, str) or not value:
                continue
            ref = _flex_import_ref(value)
            if ref is None:
                state.diag(
                    code="malformed_import_ref",
                    stage="jobs",
                    path=manifest_path,
                    pointer=f"{pointer}/{field_name}",
                    component_id=job_id,
                    observed=value,
                    expected="module path, module:attribute, or pkg.mod.attr",
                    next_action="fix the job import reference",
                )
                continue
            custom = custom or _is_custom_module(ref.module)
            import_refs.append(ref.ref)
            _check_import_ref(
                state,
                ref,
                stage="jobs",
                path=manifest_path,
                pointer=f"{pointer}/{field_name}",
                component_id=job_id,
            )
        for field_name in ("provenance_hooks", "admission_hooks"):
            for index, value in enumerate(_as_string_list(raw.get(field_name))):
                ref = _flex_import_ref(value)
                if ref is None:
                    continue
                custom = custom or _is_custom_module(ref.module)
                import_refs.append(ref.ref)
                _check_import_ref(
                    state,
                    ref,
                    stage="jobs",
                    path=manifest_path,
                    pointer=f"{pointer}/{field_name}/{index}",
                    component_id=job_id,
                )
        state.jobs.append(WiringComponent(
            kind="declared_job",
            component_id=job_id,
            origin=WiringOrigin(
                "deployment_manifest",
                path=str(manifest_path),
                pointer=pointer,
                custom_code=custom,
            ),
            attrs={
                "version": raw.get("version"),
                "component": raw.get("component"),
                "owner": raw.get("owner"),
                "trigger_kinds": list(_as_string_list(raw.get("trigger_kinds"))),
                "import_refs": import_refs,
                "registration": "declared_not_registered",
                "runtime_effect": "visibility_only",
            },
        ))


def _resolve_gates(
    state: _ResolverState,
    manifest: Mapping[str, Any],
    manifest_path: Path,
) -> None:
    consistency = manifest.get("consistency")
    dangling_policy = "block"
    if isinstance(consistency, Mapping):
        candidate = consistency.get("dangling_edges")
        if candidate in ("block", "prune"):
            dangling_policy = str(candidate)
        elif candidate is not None:
            state.diag(
                code="dangling_policy_invalid",
                stage="gates",
                path=manifest_path,
                pointer="/consistency/dangling_edges",
                observed=candidate,
                expected=["block", "prune"],
                next_action="use block or prune for consistency.dangling_edges",
            )
    state.gates.append(WiringComponent(
        kind="gate",
        component_id="transform_consistency",
        origin=WiringOrigin(
            "substrate_default",
            note="Publisher always enables TransformConsistencyGate",
        ),
        attrs={"dangling_policy": dangling_policy},
    ))

    _resolve_sql_gate_block(
        state,
        manifest,
        manifest_path,
        block_name="consistency_gates",
        component_kind="consistency_gate",
    )
    _resolve_sql_gate_block(
        state,
        manifest,
        manifest_path,
        block_name="post_publish_consistency_checks",
        component_kind="post_publish_consistency_check",
    )
    _resolve_invariant_files(state)


def _resolve_sql_gate_block(
    state: _ResolverState,
    manifest: Mapping[str, Any],
    manifest_path: Path,
    *,
    block_name: str,
    component_kind: str,
) -> None:
    block = manifest.get(block_name) or []
    if not isinstance(block, Sequence) or isinstance(block, str):
        state.diag(
            code="gate_block_invalid",
            stage="gates",
            path=manifest_path,
            pointer=f"/{block_name}",
            observed=type(block).__name__,
            expected="list of gate descriptors",
            next_action=f"make {block_name} a list",
        )
        return
    seen: set[str] = set()
    for index, raw in enumerate(block):
        pointer = f"/{block_name}/{index}"
        if not isinstance(raw, Mapping):
            state.diag(
                code="gate_descriptor_invalid",
                stage="gates",
                path=manifest_path,
                pointer=pointer,
                observed=type(raw).__name__,
                expected="mapping",
                next_action="make the gate entry a mapping",
            )
            continue
        gate_id = str(raw.get("name") or raw.get("check_id") or index)
        if gate_id in seen:
            state.diag(
                code="duplicate_gate_id",
                stage="gates",
                path=manifest_path,
                pointer=pointer,
                component_id=gate_id,
                observed=gate_id,
                expected="unique gate ids",
                next_action="rename or remove one gate",
            )
        seen.add(gate_id)
        _validate_sql_gate_descriptor(
            state,
            raw,
            manifest_path=manifest_path,
            pointer=pointer,
            gate_id=gate_id,
        )
        evaluator = raw.get("evaluator_ref")
        if isinstance(evaluator, str) and evaluator:
            ref = _flex_import_ref(evaluator)
            if ref is not None:
                _check_import_ref(
                    state,
                    ref,
                    stage="gates",
                    path=manifest_path,
                    pointer=f"{pointer}/evaluator_ref",
                    component_id=gate_id,
                )
        state.gates.append(WiringComponent(
            kind=component_kind,
            component_id=gate_id,
            origin=WiringOrigin(
                "deployment_manifest",
                path=str(manifest_path),
                pointer=pointer,
            ),
            attrs={
                "block_name": block_name,
                "check_id": raw.get("check_id"),
                "type": raw.get("type", "deterministic_sql"),
                "confidence_type": raw.get("confidence_type"),
                "blocks_publish": block_name == "consistency_gates",
            },
        ))


def _validate_sql_gate_descriptor(
    state: _ResolverState,
    raw: Mapping[str, Any],
    *,
    manifest_path: Path,
    pointer: str,
    gate_id: str,
) -> None:
    extras = sorted(set(raw) - _SQL_GATE_ALLOWED_KEYS)
    if extras:
        state.diag(
            code="gate_unknown_keys",
            stage="gates",
            path=manifest_path,
            pointer=pointer,
            component_id=gate_id,
            observed=extras,
            expected=sorted(_SQL_GATE_ALLOWED_KEYS),
            next_action="remove unknown gate descriptor keys",
        )
    gate_type = raw.get("type", "deterministic_sql")
    if gate_type not in {"deterministic_sql", "sql"}:
        state.diag(
            code="gate_type_invalid",
            stage="gates",
            path=manifest_path,
            pointer=f"{pointer}/type",
            component_id=gate_id,
            observed=gate_type,
            expected=["deterministic_sql", "sql"],
            next_action="use type: deterministic_sql",
        )
    for required in (
        "name",
        "check_id",
        "predicate_family",
        "confidence_type",
        "candidate_sql",
        "coverage_sql",
    ):
        value = raw.get(required)
        if not isinstance(value, str) or not value.strip():
            state.diag(
                code="gate_missing_required_field",
                stage="gates",
                path=manifest_path,
                pointer=f"{pointer}/{required}",
                component_id=gate_id,
                observed=type(value).__name__,
                expected=f"non-empty string {required}",
                next_action=f"add {required} to the gate descriptor",
            )
    confidence_type = raw.get("confidence_type")
    if (
        isinstance(confidence_type, str)
        and confidence_type
        and confidence_type not in _SQL_GATE_CONFIDENCE_TYPES
    ):
        state.diag(
            code="gate_confidence_type_invalid",
            stage="gates",
            path=manifest_path,
            pointer=f"{pointer}/confidence_type",
            component_id=gate_id,
            observed=confidence_type,
            expected=sorted(_SQL_GATE_CONFIDENCE_TYPES),
            next_action="use a supported confidence_type",
        )
    for field_name in ("candidate_sql", "coverage_sql"):
        sql = raw.get(field_name)
        if not isinstance(sql, str):
            continue
        hit = sql_contains_writes(sql)
        if hit is not None:
            state.diag(
                code="gate_sql_not_read_only",
                stage="gates",
                path=manifest_path,
                pointer=f"{pointer}/{field_name}",
                component_id=gate_id,
                observed=hit,
                expected="read-only SELECT SQL",
                next_action="remove write-shaped SQL from the gate",
            )
    relation_inputs = raw.get("relation_inputs")
    if relation_inputs is not None and not isinstance(relation_inputs, Mapping):
        state.diag(
            code="gate_relation_inputs_invalid",
            stage="gates",
            path=manifest_path,
            pointer=f"{pointer}/relation_inputs",
            component_id=gate_id,
            observed=type(relation_inputs).__name__,
            expected="mapping",
            next_action="make relation_inputs a mapping",
        )
    for field_name in ("timeout_ms", "budget_ms"):
        value = raw.get(field_name)
        if value is None:
            continue
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            state.diag(
                code="gate_positive_int_invalid",
                stage="gates",
                path=manifest_path,
                pointer=f"{pointer}/{field_name}",
                component_id=gate_id,
                observed=value,
                expected="positive integer",
                next_action=f"set {field_name} to a positive integer",
            )


def _resolve_invariant_files(state: _ResolverState) -> None:
    candidates = discover_invariant_files(state.deployment_dir)
    seen: set[str] = set()
    for path in candidates:
        doc, diags = _load_yaml_mapping(
            path,
            missing_code="invariants_missing",
            invalid_code="invariants_invalid",
            stage="gates",
        )
        state.diagnostics.extend(diags)
        if not isinstance(doc, Mapping):
            continue
        items = doc.get("invariants") or []
        if not isinstance(items, Sequence) or isinstance(items, str):
            state.diag(
                code="invariants_invalid",
                stage="gates",
                path=path,
                pointer="/invariants",
                observed=type(items).__name__,
                expected="list of invariant descriptors",
                next_action="make invariants a list",
            )
            continue
        for index, raw in enumerate(items):
            if not isinstance(raw, Mapping):
                continue
            inv_id = str(raw.get("name") or f"{path.name}:{index}")
            if inv_id in seen:
                state.diag(
                    code="duplicate_gate_id",
                    stage="gates",
                    path=path,
                    pointer=f"/invariants/{index}/name",
                    component_id=inv_id,
                    observed=inv_id,
                    expected="unique invariant names",
                    next_action="rename one invariant",
                )
            seen.add(inv_id)
            state.gates.append(WiringComponent(
                kind="invariant_gate",
                component_id=inv_id,
                origin=WiringOrigin(
                    "invariants",
                    path=str(path),
                    pointer=f"/invariants/{index}",
                ),
                attrs={
                    "kind": raw.get("kind", "sql"),
                    "severity": raw.get("severity"),
                    "scope": raw.get("scope"),
                },
            ))


def _resolve_operator_surfaces(
    state: _ResolverState,
    manifest: Mapping[str, Any],
    manifest_path: Path,
) -> None:
    surface_fields = (
        "mcp_instructions",
        "skills_dir",
        "mcp_trace_path",
        "audit_questions",
        "tool_description_overrides",
        "skill_triggers",
        "mcp_pre_join_tools",
        "viz_path",
    )
    for field_name in surface_fields:
        value = manifest.get(field_name)
        if not value:
            continue
        state.operator_surfaces.append(WiringComponent(
            kind="operator_surface",
            component_id=field_name,
            origin=WiringOrigin(
                "deployment_manifest",
                path=str(manifest_path),
                pointer=f"/{field_name}",
            ),
            attrs={"path": str(_manifest_relative_path(state.deployment_dir, value))},
        ))

    surface = manifest.get("agent_surface")
    surface_instructions = (
        surface.get("instructions") if isinstance(surface, Mapping) else None
    )
    if surface_instructions:
        # The block's replacement for `mcp_instructions` (issue #2067).
        state.operator_surfaces.append(WiringComponent(
            kind="operator_surface",
            component_id="agent_surface.instructions",
            origin=WiringOrigin(
                "deployment_manifest",
                path=str(manifest_path),
                pointer="/agent_surface/instructions",
            ),
            attrs={
                "path": str(_manifest_relative_path(
                    state.deployment_dir, surface_instructions,
                )),
            },
        ))

    if isinstance(manifest.get("viz"), Mapping):
        state.operator_surfaces.append(WiringComponent(
            kind="operator_surface",
            component_id="viz",
            origin=WiringOrigin(
                "deployment_manifest",
                path=str(manifest_path),
                pointer="/viz",
            ),
            attrs=dict(manifest["viz"]),
        ))
    if manifest.get("agent_tools") is not None:
        state.operator_surfaces.append(WiringComponent(
            kind="operator_surface",
            component_id="agent_tools",
            origin=WiringOrigin(
                "deployment_manifest",
                path=str(manifest_path),
                pointer="/agent_tools",
            ),
            attrs={"declared": True},
        ))
    if isinstance(manifest.get("resolvers"), Mapping):
        for name, value in sorted(manifest["resolvers"].items()):
            custom = False
            if isinstance(value, str):
                ref = _flex_import_ref(value)
                if ref is not None:
                    custom = _is_custom_module(ref.module)
                    _check_import_ref(
                        state,
                        ref,
                        stage="operator_surfaces",
                        path=manifest_path,
                        pointer=f"/resolvers/{name}",
                        component_id=str(name),
                    )
            state.operator_surfaces.append(WiringComponent(
                kind="operator_surface",
                component_id=f"resolver:{name}",
                origin=WiringOrigin(
                    "deployment_manifest",
                    path=str(manifest_path),
                    pointer=f"/resolvers/{name}",
                    custom_code=custom,
                ),
                attrs={"resolver": value},
            ))

    doctor_checks = manifest.get("doctor_checks") or []
    if isinstance(doctor_checks, Sequence) and not isinstance(
        doctor_checks,
        str,
    ):
        for index, raw in enumerate(doctor_checks):
            if not isinstance(raw, Mapping):
                continue
            name = str(raw.get("name") or index)
            handler = raw.get("handler")
            custom = False
            if isinstance(handler, str) and handler:
                ref = _colon_import_ref(handler)
                if ref is None:
                    state.diag(
                        code="malformed_import_ref",
                        stage="operator_surfaces",
                        path=manifest_path,
                        pointer=f"/doctor_checks/{index}/handler",
                        component_id=name,
                        observed=handler,
                        expected="module:attribute",
                        next_action="fix the doctor check handler reference",
                    )
                else:
                    custom = _is_custom_module(ref.module)
                    _check_import_ref(
                        state,
                        ref,
                        stage="operator_surfaces",
                        path=manifest_path,
                        pointer=f"/doctor_checks/{index}/handler",
                        component_id=name,
                    )
            state.operator_surfaces.append(WiringComponent(
                kind="operator_surface",
                component_id=f"doctor:{name}",
                origin=WiringOrigin(
                    "deployment_manifest",
                    path=str(manifest_path),
                    pointer=f"/doctor_checks/{index}",
                    custom_code=custom,
                ),
                attrs={
                    "handler": handler,
                    "help": raw.get("help"),
                },
                import_ref=handler if isinstance(handler, str) else None,
            ))


def _check_import_ref(
    state: _ResolverState,
    ref: _ImportRef,
    *,
    stage: str,
    path: Path,
    pointer: str,
    component_id: str,
) -> None:
    if not state.check_imports:
        return
    if _module_spec_available(ref.module, state.deployment_dir):
        return
    state.diag(
        code="unresolved_import_ref",
        stage=stage,
        path=path,
        pointer=pointer,
        component_id=component_id,
        observed=ref.ref,
        expected="importable module discovered via importlib spec",
        next_action=(
            "fix the module path or add the deployment/package directory "
            "to the runtime import path"
        ),
        message=(
            "module was not found by descriptor-only importlib spec lookup; "
            "attribute existence is not checked because that would import code"
        ),
    )


def _module_spec_available(module_name: str, deployment_dir: Path) -> bool:
    """Return whether a module appears importable without importing it."""

    if not module_name:
        return False
    importlib.invalidate_caches()
    search_paths = _search_paths(deployment_dir)
    rel = Path(*module_name.split("."))
    for base in search_paths:
        if (base / f"{rel}.py").is_file():
            return True
        if (base / rel / "__init__.py").is_file():
            return True
    # For a DOTTED name the explicit on-disk checks above already walk the
    # full package path (parents included), so a real module would have been
    # found. `PathFinder.find_spec(name, path=...)` matches only the TAIL
    # component against the supplied dirs and does NOT validate parents, so a
    # bare `enrichers/` package leaked onto sys.path by a sibling test would
    # falsely satisfy `missing.enrichers`. Only fall back to PathFinder for
    # top-level names, where tail == full name and the lookup is sound.
    if "." in module_name:
        return False
    try:
        spec = importlib.machinery.PathFinder.find_spec(
            module_name,
            [str(p) for p in search_paths],
        )
    except (AttributeError, ImportError, KeyError, OSError, ValueError):
        return False
    return spec is not None


def _search_paths(deployment_dir: Path) -> list[Path]:
    out: list[Path] = [deployment_dir]
    parent = deployment_dir.parent.resolve()
    if parent not in out:
        out.append(parent)
    for raw in sys.path:
        if not raw:
            raw = "."
        try:
            path = Path(raw).resolve()
        except OSError:
            continue
        if path not in out:
            out.append(path)
    return out


def _is_custom_module(module_name: str | None) -> bool:
    if not module_name:
        return False
    return not module_name.startswith("okg.")


def _module_import_ref(value: Any) -> _ImportRef:
    return _ImportRef(ref=str(value), module=str(value), attr=None)


def _colon_import_ref(value: Any) -> _ImportRef | None:
    if not isinstance(value, str) or ":" not in value:
        return None
    module, _, attr = value.partition(":")
    module = module.strip()
    attr = attr.strip()
    if not module or not attr:
        return None
    return _ImportRef(ref=value, module=module, attr=attr)


def _dotted_import_ref(value: Any) -> _ImportRef | None:
    if not isinstance(value, str) or "." not in value:
        return None
    module, _, attr = value.rpartition(".")
    if not module or not attr:
        return None
    return _ImportRef(ref=value, module=module, attr=attr)


def _flex_import_ref(value: Any) -> _ImportRef | None:
    if not isinstance(value, str) or not value.strip():
        return None
    if ":" in value:
        return _colon_import_ref(value)
    if "." in value:
        # Treat runtime-style workflow module paths as module-only when the
        # whole path is discoverable. Otherwise fall back to dotted attr form.
        module_ref = _module_import_ref(value.strip())
        return module_ref
    return _module_import_ref(value.strip())


def _find_cycle(deps: Mapping[str, set[str]]) -> list[str] | None:
    visiting: set[str] = set()
    visited: set[str] = set()
    stack: list[str] = []

    def visit(node: str) -> list[str] | None:
        if node in visited:
            return None
        if node in visiting:
            try:
                idx = stack.index(node)
            except ValueError:
                idx = 0
            return [*stack[idx:], node]
        visiting.add(node)
        stack.append(node)
        for child in sorted(deps.get(node, ())):
            found = visit(child)
            if found:
                return found
        stack.pop()
        visiting.remove(node)
        visited.add(node)
        return None

    for node in sorted(deps):
        found = visit(node)
        if found:
            return found
    return None


def _load_yaml_mapping(
    path: Path,
    *,
    missing_code: str = "yaml_missing",
    invalid_code: str = "yaml_invalid",
    stage: str = "deployment",
) -> tuple[dict[str, Any], list[WiringDiagnostic]]:
    diagnostics: list[WiringDiagnostic] = []
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        diagnostics.append(WiringDiagnostic(
            code=missing_code,
            severity="error",
            stage=stage,
            path=str(path),
            expected="existing YAML file",
            next_action="create the file or update the descriptor path",
        ))
        return {}, diagnostics
    try:
        data = fastyaml.safe_load(text) or {}
    except yaml.YAMLError as exc:
        diagnostics.append(WiringDiagnostic(
            code=invalid_code,
            severity="error",
            stage=stage,
            path=str(path),
            observed=str(exc),
            expected="valid YAML",
            next_action="fix the YAML syntax",
        ))
        return {}, diagnostics
    if not isinstance(data, Mapping):
        diagnostics.append(WiringDiagnostic(
            code=invalid_code,
            severity="error",
            stage=stage,
            path=str(path),
            observed=type(data).__name__,
            expected="YAML mapping",
            next_action="make the YAML top level a mapping",
        ))
        return {}, diagnostics
    return dict(data), diagnostics


def _duplicate_keys_under(path: Path, top_key: str) -> list[dict[str, Any]]:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return []
    try:
        root = yaml.compose(text)
    except yaml.YAMLError:
        return []
    if not isinstance(root, MappingNode):
        return []
    target: MappingNode | None = None
    for key_node, value_node in root.value:
        if getattr(key_node, "value", None) == top_key and isinstance(
            value_node,
            MappingNode,
        ):
            target = value_node
            break
    if target is None:
        return []
    seen: dict[str, int] = {}
    duplicates: list[dict[str, Any]] = []
    for key_node, _value_node in target.value:
        key = str(getattr(key_node, "value", ""))
        line = int(getattr(key_node.start_mark, "line", 0)) + 1
        if key in seen:
            duplicates.append({"key": key, "line": line})
        else:
            seen[key] = line
    return duplicates


def _manifest_relative_path(deployment_dir: Path, value: Any) -> Path:
    if not isinstance(value, str) or not value:
        return deployment_dir / str(value)
    expanded = _expand_path_placeholders(deployment_dir, value)
    path = Path(expanded)
    return path if path.is_absolute() else deployment_dir / path


def _expand_path_placeholders(deployment_dir: Path, value: str) -> str:
    if deployment_dir.parent.name == "deployments":
        repo_root = deployment_dir.parent.parent.resolve()
    else:
        repo_root = deployment_dir.parent.resolve()
    return (
        value.replace("${manifest_dir}", str(deployment_dir))
        .replace("${deployment_dir}", str(deployment_dir))
        .replace("${source_registry_dir}", str(deployment_dir))
        .replace("${repo_root}", str(repo_root))
    )


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return list(value)
    if isinstance(value, tuple):
        return list(value)
    return [value]


def _as_string_list(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    if isinstance(value, Iterable) and not isinstance(value, Mapping):
        return tuple(str(item) for item in value if isinstance(item, str))
    return ()


def _default_modules_root() -> Path:
    return Path(__file__).resolve().parent / "library" / "ontologies"


def _jsonable(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, tuple | list):
        return [_jsonable(v) for v in value]
    if isinstance(value, set):
        return sorted(_jsonable(v) for v in value)
    return value
