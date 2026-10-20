"""Sealing, activation and upgrade honour a deployment's declared owners.

`module_providers:` (issue #1921, PR #1930) lets a deployment say which
owners its own configuration supplies. `compose_catalog` merged those owners
into the built-ins only when the caller named neither `providers=` nor
`modules_root=`. The three release paths always name one -- the library of
the revision being sealed, activated or upgraded to -- so a deployment
carrying the key was refused `module_provider_unresolved` by exactly the
commands that move it between releases (issue #1933).

Each test below drives one of those three paths against a deployment that
declares an owner. A deployment without the declaration is covered too: it
must compose exactly what it composed before.
"""

from __future__ import annotations

import os
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from okg.substrate.catalog.module_providers import ModuleProviderError
from okg.substrate.deployments.environment import load_environment
from okg.substrate.release_activation import activate_environment_release
from okg.substrate.sealed_release import prepare_environment_release

from tests.substrate.test_deployment_upgrade import (
    UpgradeOptions,
    _PLANNER_MODULE_RELPATH,
    _vendored_layout,
)
from tests.substrate.test_deployment_upgrade import _git as _upgrade_git
from tests.substrate.test_sealed_releases import (
    _commit,
    _environment_values,
    _fake_sync,
    _fixture_repo,
)

_OWNER = "neutral-lab"
_MODULE = "records"
_MODULE_ID = f"{_OWNER}:{_MODULE}"

_MODULE_SCHEMA = (
    "id: https://example.invalid/neutral-lab/records\n"
    "name: records\n"
    "classes:\n"
    "  LabRecord:\n"
    "    annotations:\n"
    "      archetype: artifact\n"
)


# ---------------------------------------------------------------- sealing


def _declare_in_definition(repo: Path, relative: str) -> None:
    """Add `module_providers:` and the module it names to the definition."""
    definition = repo / "ops/definitions/shared-definition/definition.yaml"
    raw = yaml.safe_load(definition.read_text())
    raw["modules"] = [*raw["modules"], _MODULE_ID]
    raw["module_providers"] = {_OWNER: relative}
    definition.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    module_dir = definition.parent / relative / _MODULE
    module_dir.mkdir(parents=True, exist_ok=True)
    (module_dir / "schema.yaml").write_text(_MODULE_SCHEMA, encoding="utf-8")


def test_sealing_resolves_a_declared_owner(tmp_path: Path) -> None:
    """`okg environment prepare-release` composes the declared module.

    Before the fix this refused `module_provider_unresolved`: sealing passes
    the revision's own ontology library as `modules_root=`, which the merge
    treated as the caller's whole answer.
    """
    repo, _initial = _fixture_repo(tmp_path)
    _declare_in_definition(repo, f"modules/{_OWNER}")
    commit = _commit(repo, "declare a package owner")
    values = _environment_values(tmp_path / "state")

    environment = load_environment(
        repo / "ops/environments/fixture-production/environment.yaml",
        environ=values,
    )
    release = prepare_environment_release(
        environment=environment,
        source_repo=repo,
        revision=commit,
        dependency_sync=_fake_sync("runtime", []),
        environ=values,
    )

    provenance = {
        entry["module_id"]: entry for entry in release.inputs.module_provenance
    }
    assert _MODULE_ID in provenance
    assert provenance[_MODULE_ID]["owner"] == _OWNER
    assert provenance[_MODULE_ID]["provider_kind"] == "package"
    # The declaration carries no per-module digest by design; the deployment
    # tree the release seals is what binds the bytes.
    assert provenance[_MODULE_ID].get("package_release_digest") is None


def test_sealing_a_deployment_without_the_key_is_unchanged(tmp_path: Path) -> None:
    """No declaration, no new owners and no module provenance."""
    repo, commit = _fixture_repo(tmp_path)
    values = _environment_values(tmp_path / "state")
    environment = load_environment(
        repo / "ops/environments/fixture-production/environment.yaml",
        environ=values,
    )
    release = prepare_environment_release(
        environment=environment,
        source_repo=repo,
        revision=commit,
        dependency_sync=_fake_sync("runtime", []),
        environ=values,
    )
    assert tuple(release.inputs.module_provenance) == ()


# ------------------------------------------------------------- activation


def test_activation_resolves_a_declared_owner(tmp_path: Path) -> None:
    """Activation re-composes the same catalog the seal recorded.

    Activation re-resolves providers and refuses when the result differs from
    what the release sealed. Resolving the declaration on only one of the two
    sides would make this refuse instead of activating.
    """
    repo, _initial = _fixture_repo(tmp_path)
    _declare_in_definition(repo, f"modules/{_OWNER}")
    commit = _commit(repo, "declare a package owner")
    values = _environment_values(tmp_path / "state")

    environment = load_environment(
        repo / "ops/environments/fixture-production/environment.yaml",
        environ=values,
    )
    release = prepare_environment_release(
        environment=environment,
        source_repo=repo,
        revision=commit,
        dependency_sync=_fake_sync("runtime", []),
        environ=values,
    )
    result = activate_environment_release(
        environment=environment,
        release=release,
        source_repo=repo,
        activated_by="test",
        environ=values,
    )
    assert result.artifact_id == release.artifact_id
    assert result.changed is True
    # Activation re-resolved the declared owner and matched what the seal
    # recorded; a one-sided resolution would have refused before this point.
    assert [entry["owner"] for entry in release.inputs.module_provenance] == [_OWNER]


# ---------------------------------------------------------------- upgrade


def _upgrade_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    declare: bool,
) -> tuple[SimpleNamespace, SimpleNamespace]:
    """A vendored deployment repo, optionally declaring a package owner."""
    from okg.substrate import deployment_upgrade as du

    outer, okg_repo, deployment_dir = _vendored_layout(tmp_path)
    if declare:
        manifest_path = deployment_dir / "deployment.yaml"
        raw = yaml.safe_load(manifest_path.read_text())
        raw["modules"] = [*raw["modules"], _MODULE_ID]
        raw["module_providers"] = {_OWNER: f"modules/{_OWNER}"}
        manifest_path.write_text(yaml.safe_dump(raw, sort_keys=False))
        module_dir = deployment_dir / "modules" / _OWNER / _MODULE
        module_dir.mkdir(parents=True)
        (module_dir / "schema.yaml").write_text(_MODULE_SCHEMA)
        _upgrade_git(outer, "add", ".")
        _upgrade_git(outer, "commit", "-m", "declare a package owner")

    monkeypatch.setattr(du, "__file__", str(okg_repo / _PLANNER_MODULE_RELPATH))
    manifest = SimpleNamespace(
        deployment_dir=deployment_dir,
        release={"channel": "dev"},
    )
    ctx = SimpleNamespace(
        options=UpgradeOptions(deployment="dep"),
        env={"OKG_IVM_INTERVAL_VIEW": "1"},
        dsn="postgres://user@host/db",
        data={},
    )
    monkeypatch.setattr(du, "_resolve_upgrade_deployment", lambda _ctx: manifest)
    return ctx, manifest


def _target_inventory(ctx: SimpleNamespace, manifest: SimpleNamespace) -> dict:
    from okg.substrate import deployment_upgrade as du

    authority = du._git_target_revision(ctx, manifest)
    return du._target_catalog_and_nomos_inventory(
        ctx,
        repo_root=authority.code_repo_root,
        revision=authority.code_revision,
        deployment_repo_root=authority.deployment_repo_root,
        deployment_revision=authority.deployment_revision,
        deployment_relative=authority.deployment_relative,
    )


def test_upgrade_target_planning_resolves_a_declared_owner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`okg deployment up` plans against the declared owner's modules too."""
    ctx, manifest = _upgrade_authority(tmp_path, monkeypatch, declare=True)
    target = _target_inventory(ctx, manifest)
    provenance = {entry["module_id"]: entry for entry in target["module_provenance"]}
    # Built-in modules carry no provenance row; the declared one does.
    assert list(provenance) == [_MODULE_ID]
    assert provenance[_MODULE_ID]["owner"] == _OWNER
    assert provenance[_MODULE_ID]["provider_kind"] == "package"
    assert target["catalog_fingerprint"].startswith("sha256:")


def test_upgrade_target_planning_without_the_key_is_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No declaration, the built-in library is still the only provider."""
    ctx, manifest = _upgrade_authority(tmp_path, monkeypatch, declare=False)
    target = _target_inventory(ctx, manifest)
    assert target["raw_manifest"]["modules"] == ["document_starter"]
    assert target["module_provenance"] == []
    assert target["catalog_fingerprint"].startswith("sha256:")


# ----------------------------------------------------- collisions refuse


def test_a_declared_owner_may_not_shadow_the_release_library(
    tmp_path: Path,
) -> None:
    """The declaration adds owners; it never replaces one the caller named.

    The manifest loader already refuses the built-in owner `okg`. This is the
    backstop for any other overlap between a declaration and the provider
    mapping a release path supplies.
    """
    from okg.substrate.catalog.module_providers import ModuleProvider
    from okg.substrate.catalog.modules_compose import (
        providers_for_declared_deployment,
    )

    deployment = tmp_path / "dep"
    (deployment / "modules" / _OWNER / _MODULE).mkdir(parents=True)
    (deployment / "modules" / _OWNER / _MODULE / "schema.yaml").write_text(
        _MODULE_SCHEMA
    )
    (deployment / "schemas").mkdir()
    (deployment / "source_registry.yaml").write_text("sources: {}\n")
    (deployment / "deployment.yaml").write_text(
        yaml.safe_dump(
            {
                "name": "dep",
                "schema_dir": "schemas",
                "source_registry": "source_registry.yaml",
                "modules": [_MODULE_ID],
                "module_providers": {_OWNER: f"modules/{_OWNER}"},
            },
            sort_keys=False,
        )
    )
    colliding = {
        _OWNER: ModuleProvider(
            owner=_OWNER, root=tmp_path / "elsewhere", kind="package",
        ),
    }
    with pytest.raises(ModuleProviderError) as caught:
        providers_for_declared_deployment(deployment, providers=colliding)
    assert _OWNER in str(caught.value)
    assert caught.value.data["owner"] == _OWNER


def test_the_helper_is_a_no_op_without_a_declaration(tmp_path: Path) -> None:
    """A deployment with no key gets byte-for-byte the caller's own mapping."""
    from okg.substrate.catalog.module_providers import providers_from_modules_root
    from okg.substrate.catalog.modules_compose import (
        providers_for_declared_deployment,
    )

    deployment = tmp_path / "plain"
    (deployment / "schemas").mkdir(parents=True)
    (deployment / "source_registry.yaml").write_text("sources: {}\n")
    (deployment / "deployment.yaml").write_text(
        yaml.safe_dump(
            {
                "name": "plain",
                "schema_dir": "schemas",
                "source_registry": "source_registry.yaml",
                "modules": [],
            },
            sort_keys=False,
        )
    )
    root = tmp_path / "library"
    root.mkdir()
    base = providers_from_modules_root(root)
    assert providers_for_declared_deployment(deployment, modules_root=root) == base


# ------------------------------------- what the seal examines, and binds


def test_sealing_refuses_a_malformed_provenance_row(tmp_path: Path) -> None:
    """The seal examines the provenance it is handed, rather than trusting it.

    `module_provenance` is not a digest string, so the release-input
    validation cannot check it the way it checks every other field. Skipping
    it outright let a resolver seal a row naming a module and nothing else;
    the release then failed at activation, far from the cause.
    """
    from okg.substrate.errors import SealedReleaseInvalid
    from okg.substrate.sealed_release import (
        prepare_sealed_release,
        release_inputs_from_tree,
    )
    from tests.substrate.test_sealed_releases import _sha

    repo, commit = _fixture_repo(tmp_path)

    def malformed_resolver(candidate: Path, revision: str):
        inputs = release_inputs_from_tree(
            candidate,
            code_revision=revision,
            catalog_fingerprint=_sha("catalog"),
            definition_digest=_sha("definition"),
            skills_path=candidate / "skills",
        )
        return replace(inputs, module_provenance=({"module_id": "lab:records"},))

    with pytest.raises(SealedReleaseInvalid) as caught:
        prepare_sealed_release(
            source_repo=repo,
            revision=commit,
            artifact_store=tmp_path / "store",
            input_resolver=malformed_resolver,
            dependency_sync=_fake_sync("runtime", []),
            environ=dict(os.environ),
        )
    assert "module_provenance" in str(caught.value)


def test_declared_module_bytes_move_the_catalog_fingerprint(tmp_path: Path) -> None:
    """What binds a declared module's bytes is not its provenance row.

    The row carries the owner and the provider kind and nothing else -- the
    declaration has no per-module digest to record. Editing a declared
    module's schema leaves the row byte-identical, so the binding has to come
    from the catalog fingerprint and the sealed deployment tree instead.
    """
    repo, _initial = _fixture_repo(tmp_path)
    _declare_in_definition(repo, f"modules/{_OWNER}")
    first_commit = _commit(repo, "declare a package owner")
    values = _environment_values(tmp_path / "state")
    descriptor = repo / "ops/environments/fixture-production/environment.yaml"

    first = prepare_environment_release(
        environment=load_environment(descriptor, environ=values),
        source_repo=repo,
        revision=first_commit,
        dependency_sync=_fake_sync("runtime", []),
        environ=values,
    )

    module_schema = (
        repo / "ops/definitions/shared-definition/modules" / _OWNER / _MODULE
        / "schema.yaml"
    )
    module_schema.write_text(
        _MODULE_SCHEMA.replace("LabRecord", "LabRecordV2"), encoding="utf-8",
    )
    second_commit = _commit(repo, "edit the declared module")
    second = prepare_environment_release(
        environment=load_environment(descriptor, environ=values),
        source_repo=repo,
        revision=second_commit,
        dependency_sync=_fake_sync("runtime", []),
        environ=values,
    )

    # The provenance row does not move: it has nothing in it that could.
    assert first.inputs.module_provenance == second.inputs.module_provenance
    # These do, and they are what an activation mismatch is actually caught by.
    assert first.inputs.catalog_fingerprint != second.inputs.catalog_fingerprint
    assert first.inputs.deployment_tree_sha != second.inputs.deployment_tree_sha


def test_upgrade_accepts_the_explicit_built_in_owner_spelling(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`okg:name` resolves from the library, as it does in composition.

    The candidate-path validator treated every entry as a bare directory
    name, so the owner-qualified spelling of a built-in module -- which
    `compose_catalog` has always accepted -- refused one step earlier.
    """
    ctx, manifest = _upgrade_authority(tmp_path, monkeypatch, declare=False)
    manifest_path = manifest.deployment_dir / "deployment.yaml"
    raw = yaml.safe_load(manifest_path.read_text())
    raw["modules"] = ["okg:document_starter"]
    manifest_path.write_text(yaml.safe_dump(raw, sort_keys=False))
    _upgrade_git(manifest.deployment_dir.parents[1], "add", ".")
    _upgrade_git(manifest.deployment_dir.parents[1], "commit", "-m", "qualify")

    target = _target_inventory(ctx, manifest)
    assert target["raw_manifest"]["modules"] == ["okg:document_starter"]
    # A built-in owner records no provenance row, however it is spelled.
    assert target["module_provenance"] == []
