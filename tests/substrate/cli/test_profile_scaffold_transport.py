"""Every shipped profile scaffolds a transport-durable deployment.

PACT ``fix-init-scaffold-provability`` / ``req.init.transportable-scaffold``
(issue #636 residual): the scaffold must write at least one file under
``schemas/`` and list it in ``files_written``, because git does not track
empty directories — a committed-then-cloned scaffold with a bare
``schemas/`` resurrects the original ``DeploymentConfigError``
"points at a missing path" failure. The transport simulation below copies
FILES ONLY (exactly what git/tar transport preserves) and requires the
manifest to resolve from the copy.
"""
from __future__ import annotations

import importlib
import inspect
import shutil
import subprocess
from collections.abc import Mapping
from pathlib import Path
from types import SimpleNamespace

import pytest

from okg.substrate import fastyaml
from okg.substrate.catalog.modules_compose import compose_catalog
from okg.substrate.deployment_bootstrap.profile_init import (
    build_plan,
    discover_profile,
    scaffold_deployment,
)
from okg.substrate.deployment_lint import lint_deployment
from okg.substrate.deployment_profile_explain import BUILTIN_V1_PROFILE_NAMES
from okg.substrate.deployment_wiring import resolve_deployment_wiring_plan
from okg.substrate.deployments import DeploymentManifest
from okg.substrate.ingest.adapter_factory import source_adapter_init_params
from okg.substrate.sources.registry import load_source_registry

PROFILES_ROOT = (
    Path(__file__).resolve().parents[3]
    / "src" / "okg" / "substrate" / "library" / "profiles"
)

SHIPPED_PROFILES = sorted(
    p.name
    for p in PROFILES_ROOT.iterdir()
    if p.is_dir() and (p / "profile.yaml").is_file() and p.name != "_test"
)


def _sample_git_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "sample-repo"
    (repo / "pkg").mkdir(parents=True)
    (repo / "pkg" / "mod.py").write_text("def f():\n    return 1\n")
    (repo / "README.md").write_text("# sample\n")
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    subprocess.run(
        ["git", "-C", str(repo), "add", "-A"], check=True,
    )
    subprocess.run(
        [
            "git", "-C", str(repo),
            "-c", "user.email=t@example.com", "-c", "user.name=t",
            "commit", "-qm", "init",
        ],
        check=True,
    )
    return repo


def _synthesize_answers(
    profile,
    repo: Path,
    *,
    deployment_name: str,
) -> dict[str, object]:
    answers: dict[str, object] = {}
    for q in profile.init_questions:
        qid = q.id.lower()
        if qid == "deployment_name":
            answers[q.id] = deployment_name
        elif "dsn" in qid or "postgres" in qid:
            answers[q.id] = (
                "postgresql://postgres:okg@127.0.0.1:5433/"
                f"okg_{deployment_name}"
            )
        elif q.default is not None:
            answers[q.id] = q.default
        elif q.type == "path":
            answers[q.id] = str(repo)
        elif q.type == "path_list":
            answers[q.id] = [str(repo)]
        elif q.type == "orcid":
            answers[q.id] = "0000-0002-1825-0097"
        elif q.type == "integer":
            answers[q.id] = 18771
        elif q.type == "slug":
            answers[q.id] = "sample_fixture"
        else:
            answers[q.id] = "sample-value"
    return answers


def _transport_files_only(src: Path, dst: Path) -> None:
    """Copy only FILES — empty directories vanish, like git clone / tar."""
    for path in src.rglob("*"):
        if path.is_file():
            target = dst / path.relative_to(src)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target)


def _unresolved_placeholder_paths(
    value: object,
    *,
    path: str,
) -> list[str]:
    if isinstance(value, str):
        return [path] if "${" in value else []
    if isinstance(value, Mapping):
        return [
            unresolved
            for key, child in value.items()
            for unresolved in _unresolved_placeholder_paths(
                child,
                path=f"{path}.{key}",
            )
        ]
    if isinstance(value, list):
        return [
            unresolved
            for index, child in enumerate(value)
            for unresolved in _unresolved_placeholder_paths(
                child,
                path=f"{path}[{index}]",
            )
        ]
    return []


def _source_constructor_errors(
    source_registry: Mapping[str, object],
    *,
    deployment: str = "fixture",
) -> list[str]:
    """Bind every runnable adapter through the substrate own injection.

    The check must not re-list which constructor parameters the substrate
    owns. A shipped default may not author a reserved parameter -- strict
    admission refuses it (issue #2126) -- so the parameter reaches the
    adapter only because `source_adapter_init_params` injects it. A hand
    written copy of that list here silently lost `deployment` and turned a
    correct registry into a smoke failure.
    """
    errors: list[str] = []
    for source_id, entry in source_registry.items():
        if entry.is_registry_only:
            continue
        raw = entry.raw
        try:
            module = importlib.import_module(str(raw["module"]))
            source_class = getattr(module, str(raw["class"]))
            params = source_adapter_init_params(
                entry,
                dsn="postgresql://fixture",
                deployment=deployment,
                graph_branch="main",
                adapter_class=source_class,
            )
            inspect.signature(source_class).bind(**params)
        except (AttributeError, ImportError, TypeError, ValueError) as exc:
            errors.append(
                f"{source_id}: {raw.get('module')}:{raw.get('class')}: {exc}",
            )
    return errors


def test_profile_smoke_placeholder_check_rejects_nested_value() -> None:
    assert _unresolved_placeholder_paths(
        {"sources": {"demo": {"params": {"root": "${UNSET}"}}}},
        path="source_registry",
    ) == ["source_registry.sources.demo.params.root"]


def test_profile_smoke_constructor_check_rejects_bad_params() -> None:
    source_registry = {
        "openalex": SimpleNamespace(
            is_registry_only=False,
            ownership_id="fixture.openalex",
            raw={
                "ownership_id": "fixture.openalex",
                "module": "okg.substrate.library.sources.scholarly.openalex",
                "class": "OpenAlexSource",
                "params": {"cache_only": True},
            },
        ),
        "doc-corpus": SimpleNamespace(
            is_registry_only=False,
            ownership_id="fixture.doc-corpus",
            raw={
                "ownership_id": "fixture.doc-corpus",
                "module": "okg.substrate.library.sources.doc_corpus",
                "class": "DocCorpusSource",
                "params": {"repo": "fixture"},
            },
        ),
    }

    errors = _source_constructor_errors(source_registry)

    assert len(errors) == 2
    assert any("cache_only" in error for error in errors)
    assert any("repo_root" in error for error in errors)


def test_profile_smoke_constructor_check_injects_substrate_owned_params(
) -> None:
    """A reserved parameter the registry may not author still binds.

    `deployment` is substrate owned, so strict admission refuses a registry
    that authors it and every shipped default leaves it out. The smoke check
    has to supply it the way ingest does; when it re-listed the injected
    names by hand it did not, and a correct registry looked broken.
    """
    source_registry = {
        "profile_measurements": SimpleNamespace(
            is_registry_only=False,
            ownership_id="fixture.profile_measurements",
            raw={
                "ownership_id": "fixture.profile_measurements",
                "module": "okg.substrate.library.sources.profile_measurements",
                "class": "ProfileMeasurementSource",
                "params": {"repo": "fixture", "repo_root": "/tmp/fixture"},
            },
        ),
    }

    errors = _source_constructor_errors(source_registry, deployment="fixture")

    assert errors == []


@pytest.mark.parametrize("profile_name", SHIPPED_PROFILES)
def test_profile_scaffold_survives_file_only_transport(
    profile_name: str, tmp_path: Path,
) -> None:
    repo = _sample_git_repo(tmp_path)
    profile = discover_profile(profile_name)
    deployment_name = f"matrix_{profile_name.replace('-', '_')}"
    answers = _synthesize_answers(
        profile,
        repo,
        deployment_name=deployment_name,
    )
    plan = build_plan(
        profile=profile,
        deployment_name=deployment_name,
        deployments_root=tmp_path / "deployments",
        raw_answers=answers,
    )
    result = scaffold_deployment(plan)

    schemas = result.deployment_dir / "schemas"
    schema_files = sorted(p for p in schemas.rglob("*") if p.is_file())
    assert schema_files, (
        f"{profile_name}: scaffold left schemas/ empty — the deployment "
        "cannot survive git/tar transport (issue #636 residual)"
    )
    written = {p.resolve() for p in result.files_written}
    assert any(p.resolve() in written for p in schema_files), (
        f"{profile_name}: no schemas/ file is listed in files_written"
    )

    # The scaffold itself resolves.
    DeploymentManifest.from_yaml(result.deployment_dir / "deployment.yaml")

    # And so does a file-only transported copy (git drops empty dirs).
    transported = tmp_path / "transported" / profile_name
    _transport_files_only(result.deployment_dir, transported)
    manifest = DeploymentManifest.from_yaml(transported / "deployment.yaml")
    assert manifest.schema_dir == transported / "schemas"


@pytest.mark.parametrize("profile_name", BUILTIN_V1_PROFILE_NAMES)
def test_builtin_profile_smoke_matrix_scaffolds_lints_and_plans(
    profile_name: str,
    tmp_path: Path,
) -> None:
    assert tuple(SHIPPED_PROFILES) == tuple(sorted(BUILTIN_V1_PROFILE_NAMES)), (
        "the shipped reusable profiles and canonical registry diverged"
    )

    repo = _sample_git_repo(tmp_path)
    profile = discover_profile(profile_name)
    deployment_name = f"smoke_{profile_name.replace('-', '_')}"
    plan = build_plan(
        profile=profile,
        deployment_name=deployment_name,
        deployments_root=tmp_path / "deployments",
        raw_answers=_synthesize_answers(
            profile,
            repo,
            deployment_name=deployment_name,
        ),
    )
    result = scaffold_deployment(plan)

    manifest_path = result.deployment_dir / "deployment.yaml"
    registry_path = result.deployment_dir / "source_registry.yaml"
    manifest = DeploymentManifest.from_yaml(manifest_path)
    composed = compose_catalog(result.deployment_dir)
    lint_report = lint_deployment(result.deployment_dir, env={})
    wiring_plan = resolve_deployment_wiring_plan(
        result.deployment_dir,
        check_imports=True,
    )
    manifest_config = fastyaml.safe_load(
        manifest_path.read_text(encoding="utf-8"),
    )
    manifest_config.pop("profile", None)
    source_registry = load_source_registry(registry_path)
    # The registry is checked as the loader expands it. A source default may
    # keep a run-time path anchor such as `${deployment_dir}` in the file,
    # because the installer cannot know where the deployment will sit; the
    # loader fills it in, so only a placeholder left after loading is an error.
    expanded_registry = {
        source_id: entry.raw for source_id, entry in source_registry.items()
    }
    unresolved_placeholders = [
        *_unresolved_placeholder_paths(manifest_config, path="deployment"),
        *_unresolved_placeholder_paths(
            expanded_registry, path="source_registry.sources",
        ),
    ]

    assert manifest.name == plan.deployment_name, profile_name
    assert composed.subtypes, profile_name
    assert not unresolved_placeholders, {
        "profile": profile_name,
        "files": unresolved_placeholders,
    }
    assert lint_report["ok"], {
        "profile": profile_name,
        "blockers": [
            finding
            for finding in lint_report["findings"]
            if finding["severity"] == "blocker"
        ],
    }
    assert wiring_plan.ok, {
        "profile": profile_name,
        "diagnostics": [
            diagnostic.as_dict()
            for diagnostic in wiring_plan.diagnostics
            if diagnostic.severity == "error"
        ],
    }
    constructor_errors = _source_constructor_errors(
        source_registry,
        deployment=deployment_name,
    )
    assert not constructor_errors, {
        "profile": profile_name,
        "sources": constructor_errors,
    }


def test_workspace_graph_base_schema_composes(tmp_path: Path) -> None:
    """The generated base schema is loadable by catalog composition."""
    repo = _sample_git_repo(tmp_path)
    profile = discover_profile("workspace-graph")
    deployment_name = "matrix_compose"
    plan = build_plan(
        profile=profile,
        deployment_name=deployment_name,
        deployments_root=tmp_path / "deployments",
        raw_answers=_synthesize_answers(
            profile,
            repo,
            deployment_name=deployment_name,
        ),
    )
    result = scaffold_deployment(plan)
    composed = compose_catalog(result.deployment_dir)
    assert "source_file" in composed.subtypes
