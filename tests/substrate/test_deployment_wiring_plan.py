from __future__ import annotations

import importlib
from pathlib import Path

import yaml

from okg.substrate.deployment_wiring import (
    DeploymentWiringPlan,
    resolve_deployment_wiring_plan,
)


def _write_yaml(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _write_deployment(
    dep: Path,
    *,
    extra: dict | None = None,
) -> None:
    dep.mkdir(parents=True, exist_ok=True)
    (dep / "schemas").mkdir(exist_ok=True)
    manifest = {
        "name": "demo",
        "schema_dir": "schemas",
        "source_registry": "source_registry.yaml",
    }
    if extra:
        manifest.update(extra)
    _write_yaml(dep / "deployment.yaml", manifest)


def _write_module_manifest(modules_root: Path, name: str) -> None:
    _write_yaml(
        modules_root / name / "module.yaml",
        {
            "name": name,
            "version": "1.0.0",
            "provides": {
                "subtypes_file": "schema.yaml",
                "sources": [
                    {
                        "class": (
                            "okg.substrate.library.sources.git_graph_files."
                            "GitFilesSource"
                        ),
                        "profile": "discovery_crawl",
                    }
                ],
                "enrichers": [
                    {
                        "class": (
                            "okg.substrate.library.linkers.declarative."
                            "DeclarativeLinker"
                        )
                    }
                ],
            },
        },
    )


def _components_by_id(plan: DeploymentWiringPlan, section: str) -> dict:
    return {
        component.component_id: component
        for component in getattr(plan, section)
    }


def test_resolves_descriptor_plan_with_origins_and_custom_code(
    tmp_path: Path,
) -> None:
    dep = tmp_path / "deployments" / "demo"
    modules_root = tmp_path / "modules"
    _write_module_manifest(modules_root, "core")
    _write_deployment(
        dep,
        extra={
            "modules": ["core"],
            "runtime": {
                "enabled": True,
                "workflows": ["workflows.boot"],
                "schedules": {"demo_publish": "*/5 * * * *"},
            },
            "enrichers": [
                {
                    "class": (
                        "okg.substrate.library.linkers.declarative."
                        "DeclarativeLinker"
                    ),
                    "config": {"name": "declarative"},
                },
                {"class": "enrichers.custom.CustomEnricher"},
            ],
            "extractor": {
                "class": "okg.substrate.projection.extractor.RegexEntityExtractor",
                "source_subtypes": ["document_chunk"],
            },
            "alias_resolver": {
                "backends": [
                    {
                        "class": "okg.substrate.alias.backends.SqlCascadeBackend",
                        "config": {"exact_only": True},
                    }
                ]
            },
            "jobs": {
                "declared-demo": {
                    "workflow": "workflows.boot",
                    "trigger_kinds": ["manual"],
                    "owner": "demo",
                }
            },
            "consistency": {"dangling_edges": "prune"},
            "doctor_checks": [
                {
                    "name": "demo",
                    "handler": "doctor_checks:doctor_demo",
                    "help": "demo check",
                }
            ],
            "viz": {"port": 19000},
            "agent_tools": {"enabled": True},
        },
    )
    _write_yaml(
        dep / "source_registry.yaml",
        {
            "sources": {
                "git-files": {
                    "module": "okg.substrate.library.sources.git_graph_files",
                    "class": "GitFilesSource",
                    "ownership_id": "demo.git-files",
                    "source_class": "discovery_crawl",
                    "record_identity_kind": "scoped_locator",
                    "record_identity_fields": ["repo", "path"],
                    "source_revision_kind": "content_hash",
                    "deletion_semantics": "missing_from_completed_scope",
                    "publication_mode": "published_generation",
                },
                "custom": {
                    "module": "custom_sources",
                    "class": "CustomSource",
                    "ownership_id": "demo.custom",
                    "predecessor_source": "demo.git-files",
                    "source_class": "discovery_crawl",
                    "record_identity_kind": "scoped_locator",
                    "record_identity_fields": ["repo", "path"],
                    "source_revision_kind": "content_hash",
                    "deletion_semantics": "missing_from_completed_scope",
                    "publication_mode": "published_generation",
                    "sync": {
                        "triggers": ["poll"],
                        "poll": {"interval_seconds": 60},
                    },
                },
            }
        },
    )
    _write_yaml(
        dep / "views_registry.yaml",
        {
            "deployment": "demo",
            "views": [
                {
                    "view_def_id": "demo.view",
                    "view_def_factory": "views.demo_view:view_def",
                    "build_corpus": "views.demo_view:build_corpus",
                    "anchor_resolver": "views.demo_view:anchor",
                    "schedule": "",
                    "llm_client": "stub",
                }
            ],
        },
    )
    _write_yaml(
        dep / "invariants" / "core.yaml",
        {
            "invariants": [
                {
                    "name": "demo_invariant",
                    "kind": "sql",
                    "severity": "error",
                    "scope": "cross",
                }
            ]
        },
    )

    (dep / "custom_sources.py").write_text(
        "raise RuntimeError('source module was imported')\n",
        encoding="utf-8",
    )
    (dep / "doctor_checks.py").write_text(
        "raise RuntimeError('doctor module was imported')\n",
        encoding="utf-8",
    )
    (dep / "views").mkdir()
    (dep / "views" / "demo_view.py").write_text(
        "raise RuntimeError('view module was imported')\n",
        encoding="utf-8",
    )
    (dep / "workflows").mkdir()
    (dep / "workflows" / "boot.py").write_text(
        "raise RuntimeError('workflow module was imported')\n",
        encoding="utf-8",
    )
    (dep / "enrichers").mkdir()
    (dep / "enrichers" / "custom.py").write_text(
        "raise RuntimeError('enricher module was imported')\n",
        encoding="utf-8",
    )

    plan = resolve_deployment_wiring_plan(
        dep,
        modules_root=modules_root,
    )

    assert plan.ok, [d.as_dict() for d in plan.diagnostics]
    sources = _components_by_id(plan, "sources")
    assert sources["git-files"].origin.kind == "source_registry"
    assert sources["git-files"].custom_code is False
    assert sources["custom"].custom_code is True
    assert sources["core.source.0"].kind == "module_source"
    assert sources["core.source.0"].origin.kind == "module_manifest"

    deps = _components_by_id(plan, "source_dependencies")
    assert deps["custom->demo.git-files"].attrs["resolved_source_id"] == "git-files"
    assert _components_by_id(plan, "modules")["core"].attrs["version"] == "1.0.0"
    assert _components_by_id(plan, "enrichers")[
        "core.enricher.0"
    ].kind == "module_enricher"
    assert _components_by_id(plan, "views")["demo.view"].custom_code is True
    assert _components_by_id(plan, "runtime")["workflows.boot"].custom_code is True
    assert _components_by_id(plan, "jobs")["demo_publish"].kind == "runtime_schedule"
    declared_job = _components_by_id(plan, "jobs")["declared-demo"]
    assert declared_job.kind == "declared_job"
    assert declared_job.attrs["registration"] == "declared_not_registered"
    assert declared_job.attrs["runtime_effect"] == "visibility_only"
    assert _components_by_id(plan, "jobs")[
        "source_poll:custom"
    ].attrs["interval_seconds"] == 60
    assert _components_by_id(plan, "gates")[
        "transform_consistency"
    ].attrs["dangling_policy"] == "prune"
    assert _components_by_id(plan, "gates")["demo_invariant"].origin.kind == "invariants"
    assert _components_by_id(plan, "operator_surfaces")["doctor:demo"].custom_code
    assert _components_by_id(plan, "operator_surfaces")["viz"].attrs["port"] == 19000


def test_reports_unknown_duplicates_and_dependency_cycles(
    tmp_path: Path,
) -> None:
    dep = tmp_path / "deployments" / "demo"
    modules_root = tmp_path / "modules"
    _write_deployment(dep)
    (dep / "source_registry.yaml").write_text(
        """
sources:
  dup:
    ownership_id: demo.first
    source_class: discovery_crawl
  dup:
    ownership_id: demo.second
    source_class: discovery_crawl
  a:
    ownership_id: demo.shared
    predecessor_source: missing.source
    source_class: discovery_crawl
  b:
    ownership_id: demo.shared
    source_class: discovery_crawl
  x:
    ownership_id: demo.x
    predecessor_source: demo.y
    source_class: discovery_crawl
  y:
    ownership_id: demo.y
    predecessor_source: demo.x
    source_class: discovery_crawl
""",
        encoding="utf-8",
    )

    plan = resolve_deployment_wiring_plan(dep, modules_root=modules_root)
    codes = {diag.code for diag in plan.diagnostics}

    assert "duplicate_source_id" in codes
    assert "duplicate_source_ownership_id" in codes
    assert "unknown_predecessor_source" in codes
    assert "source_dependency_cycle" in codes
    cycle = plan.diagnostics_by_code("source_dependency_cycle")[0]
    assert set(cycle.observed) >= {"x", "y"}


def test_reports_runtime_loader_parity_diagnostics(tmp_path: Path) -> None:
    dep = tmp_path / "deployments" / "demo"
    modules_root = tmp_path / "modules"
    _write_deployment(
        dep,
        extra={
            "consistency_gates": [
                {
                    "name": "bad_gate",
                    "check_id": "bad.gate",
                    "predicate_family": "demo",
                    "confidence_type": "not_supported",
                    "candidate_sql": "DELETE FROM graph_nodes",
                    "coverage_sql": "SELECT 1",
                    "unexpected": True,
                    "timeout_ms": 0,
                }
            ]
        },
    )
    _write_yaml(
        dep / "source_registry.yaml",
        {
            "sources": {
                "bad": {
                    "source_class": "not_a_profile",
                    "required_for_baseline": "yes",
                }
            }
        },
    )
    _write_yaml(
        dep / "views_registry.yaml",
        {
            "enabled": "yes",
            "views": [
                {
                    "view_def_id": "bad",
                    "view_def_factory": "views.bad:factory",
                    "build_corpus": "views.bad:build",
                    "reads_subtypes": "document",
                }
            ],
        },
    )

    plan = resolve_deployment_wiring_plan(dep, modules_root=modules_root)
    codes = {diag.code for diag in plan.diagnostics}

    assert plan.ok is False
    assert "source_registry_loader_invalid" in codes
    assert "views_registry_loader_invalid" in codes
    assert "gate_unknown_keys" in codes
    assert "gate_confidence_type_invalid" in codes
    assert "gate_sql_not_read_only" in codes
    assert "gate_positive_int_invalid" in codes


def test_reports_unresolved_import_refs_without_importing_modules(
    tmp_path: Path,
    monkeypatch,
) -> None:
    dep = tmp_path / "deployments" / "demo"
    modules_root = tmp_path / "modules"
    _write_deployment(
        dep,
        extra={
            "runtime": {"enabled": True, "workflows": ["missing.workflow"]},
            "enrichers": [{"class": "missing.enrichers.Custom"}],
            "doctor_checks": [
                {"name": "bad", "handler": "missing_doctor:check"}
            ],
            "jobs": [
                {
                    "job_name": "bad-job",
                    "workflow": "missing.jobs.workflow",
                    "trigger_kinds": ["manual"],
                }
            ],
        },
    )
    _write_yaml(
        dep / "source_registry.yaml",
        {
            "sources": {
                "bad": {
                    "module": "missing_sources",
                    "class": "BadSource",
                    "ownership_id": "demo.bad",
                    "source_class": "discovery_crawl",
                }
            }
        },
    )
    _write_yaml(
        dep / "views_registry.yaml",
        {
            "deployment": "demo",
            "views": [
                {
                    "view_def_id": "bad.view",
                    "view_def_factory": "missing_views:view_def",
                    "build_corpus": "missing_views:build_corpus",
                }
            ],
        },
    )

    def fail_import_module(*_args, **_kwargs):
        raise AssertionError("resolver must not import modules")

    monkeypatch.setattr(importlib, "import_module", fail_import_module)

    plan = resolve_deployment_wiring_plan(dep, modules_root=modules_root)

    unresolved = plan.diagnostics_by_code("unresolved_import_ref")
    assert len(unresolved) >= 6
    assert {
        diag.stage for diag in unresolved
    } >= {"sources", "views", "runtime", "enrichers", "jobs"}


def test_plan_as_dict_is_json_ready(tmp_path: Path) -> None:
    dep = tmp_path / "deployments" / "demo"
    modules_root = tmp_path / "modules"
    _write_deployment(dep, extra={"modules": []})
    _write_yaml(dep / "source_registry.yaml", {"sources": {}})

    payload = resolve_deployment_wiring_plan(
        dep,
        modules_root=modules_root,
    ).as_dict()

    assert payload["deployment_name"] == "demo"
    assert payload["ok"] is True
    assert payload["gates"][0]["component_id"] == "transform_consistency"


def test_reports_malformed_extractor_and_alias_import_refs(
    tmp_path: Path,
) -> None:
    dep = tmp_path / "deployments" / "demo"
    modules_root = tmp_path / "modules"
    _write_deployment(
        dep,
        extra={
            "extractor": {"class": "NotDotted"},
            "alias_resolver": {"backends": [{"class": "also_bad"}]},
        },
    )
    _write_yaml(dep / "source_registry.yaml", {"sources": {}})

    plan = resolve_deployment_wiring_plan(dep, modules_root=modules_root)
    malformed = plan.diagnostics_by_code("malformed_import_ref")

    assert {
        diag.pointer for diag in malformed
    } >= {"/extractor/class", "/alias_resolver/backends/0/class"}


def test_import_lookup_includes_deployment_parent_for_packages(
    tmp_path: Path,
) -> None:
    deployments_root = tmp_path / "deployments"
    dep = deployments_root / "demo"
    modules_root = tmp_path / "modules"
    _write_deployment(
        dep,
        extra={"runtime": {"enabled": True, "workflows": ["shared_pkg.worker"]}},
    )
    _write_yaml(dep / "source_registry.yaml", {"sources": {}})
    package = deployments_root / "shared_pkg"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "worker.py").write_text(
        "raise RuntimeError('resolver must not import package workers')\n",
        encoding="utf-8",
    )

    plan = resolve_deployment_wiring_plan(dep, modules_root=modules_root)

    assert plan.ok, [diag.as_dict() for diag in plan.diagnostics]
    assert _components_by_id(plan, "runtime")["shared_pkg.worker"].custom_code


# ---------- #1179 slice 6: owner-keyed providers beside modules_root ----------


def test_wiring_resolves_a_packaged_module_through_a_provider_mapping(
    tmp_path: Path,
) -> None:
    """`providers=` reaches the same module manifest `modules_root=` does."""
    import pytest

    from okg.substrate.catalog.module_providers import (
        BUILTIN_OWNER,
        ModuleProvider,
        ModuleProviderError,
    )

    dep = tmp_path / "deployments" / "demo"
    modules_root = tmp_path / "ontologies"
    _write_module_manifest(modules_root, "core")
    _write_deployment(dep, extra={"modules": ["core"]})
    _write_yaml(dep / "source_registry.yaml", {"sources": {}})

    by_root = resolve_deployment_wiring_plan(dep, modules_root=modules_root)
    by_provider = resolve_deployment_wiring_plan(
        dep,
        providers={
            BUILTIN_OWNER: ModuleProvider(BUILTIN_OWNER, modules_root),
        },
    )

    assert [item.as_dict() for item in by_provider.modules] == [
        item.as_dict() for item in by_root.modules
    ]
    assert {diag.code for diag in by_provider.diagnostics} == {
        diag.code for diag in by_root.diagnostics
    }

    # A module whose owner supplies nothing is a diagnostic, not a crash and
    # not a silent fall back to the built-in root.
    _write_deployment(dep, extra={"modules": ["archi:lab_units"]})
    unresolved = resolve_deployment_wiring_plan(
        dep,
        providers={
            BUILTIN_OWNER: ModuleProvider(BUILTIN_OWNER, modules_root),
        },
    )
    assert "module_provider_unresolved" in {
        diag.code for diag in unresolved.diagnostics
    }

    with pytest.raises(ModuleProviderError):
        resolve_deployment_wiring_plan(
            dep,
            modules_root=modules_root,
            providers={
                BUILTIN_OWNER: ModuleProvider(BUILTIN_OWNER, modules_root),
            },
        )
