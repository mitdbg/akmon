"""A packaged install composes the ontology modules its package supplies.

Before `module_providers`, a package could declare an ontology module, have
`okg deployment install-plan` resolve and compose it, and then watch the
install refuse at catalog load with `module_provider_unresolved` -- because
`package_module_providers` was reached only during plan composition, in
memory, and provision resolved built-in owners only (issue #1921).

The deployment now declares the owners its configuration supplies, so every
`compose_catalog` on that deployment -- provision, publish, doctor, upgrade --
resolves the same modules.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml

from okg.substrate.catalog.modules_compose import compose_catalog
from okg.substrate.catalog.module_providers import ModuleProviderError
from okg.substrate.deployments import (
    DeploymentConfigError,
    resolve_declared_module_providers,
)
from okg.substrate.distributions.install_execution import _check_module_provider_drift
from okg.substrate.errors import SubstrateError

_OWNER = "neutral-lab"
_MODULE = "records"
_MODULE_ID = f"{_OWNER}:{_MODULE}"

_SCHEMA_YAML = b"""classes:
  ExampleCollection:
    annotations:
      archetype: entity
  ExampleRecord:
    annotations:
      archetype: artifact
"""


def _deployment(
    root: Path,
    *,
    modules: list[str],
    declare: dict[str, str] | None = None,
    with_module_files: bool = True,
) -> Path:
    """One deployment tree, optionally carrying a packaged module."""
    root.mkdir(parents=True, exist_ok=True)
    (root / "schemas").mkdir(exist_ok=True)
    (root / "source_registry.yaml").write_text("sources: {}\n")
    if with_module_files:
        module_dir = root / "modules" / _OWNER / _MODULE
        module_dir.mkdir(parents=True, exist_ok=True)
        (module_dir / "schema.yaml").write_bytes(_SCHEMA_YAML)
    body: dict = {
        "name": "sample",
        "schema_dir": "schemas",
        "source_registry": "source_registry.yaml",
        "modules": modules,
    }
    if declare is not None:
        body["module_providers"] = declare
    (root / "deployment.yaml").write_text(yaml.safe_dump(body, sort_keys=False))
    return root


# ---------- (a) the regression ----------


def test_a_declared_owner_supplies_its_modules_to_composition(tmp_path: Path) -> None:
    """The #1921 regression: this composition is what the install performs.

    Without the declaration the same tree refuses, which is exactly what a
    packaged install did on every consumer-owned module.
    """
    root = _deployment(
        tmp_path / "declared",
        modules=[_MODULE_ID],
        declare={_OWNER: f"modules/{_OWNER}"},
    )
    composed = compose_catalog(root)
    assert composed.modules == [_MODULE_ID]
    assert {"example_collection", "example_record"} <= composed.subtypes
    assert composed.subtype_origin["example_record"] == _MODULE_ID
    assert composed.module_origins[_MODULE_ID]["owner"] == _OWNER
    assert composed.module_origins[_MODULE_ID]["provider_kind"] == "package"


def test_the_same_tree_without_the_declaration_still_refuses(tmp_path: Path) -> None:
    """Pins the pre-fix behaviour: bytes on disk are not an owner."""
    root = _deployment(tmp_path / "undeclared", modules=[_MODULE_ID])
    with pytest.raises(ModuleProviderError) as caught:
        compose_catalog(root)
    assert caught.value.code == "module_provider_unresolved"


# ---------- (b) built-ins are added to, never replaced ----------


def test_built_in_modules_resolve_beside_a_declared_owner(tmp_path: Path) -> None:
    """A declared owner must not cost the deployment its built-in library."""
    root = _deployment(
        tmp_path / "mixed",
        modules=["bookkeeping", _MODULE_ID],
        declare={_OWNER: f"modules/{_OWNER}"},
    )
    composed = compose_catalog(root)
    assert composed.module_origins["bookkeeping"]["provider_kind"] == "builtin"
    assert composed.module_origins[_MODULE_ID]["provider_kind"] == "package"


def test_a_built_in_only_deployment_is_unchanged(tmp_path: Path) -> None:
    """No declaration, no new behaviour."""
    root = _deployment(
        tmp_path / "builtin", modules=["bookkeeping"], with_module_files=False,
    )
    composed = compose_catalog(root)
    assert composed.modules == ["bookkeeping"]
    assert composed.module_origins["bookkeeping"]["provider_kind"] == "builtin"


def test_an_explicit_provider_mapping_still_wins(tmp_path: Path) -> None:
    """A caller that names its own providers gets exactly those, as before."""
    root = _deployment(
        tmp_path / "explicit",
        modules=["bookkeeping"],
        declare={_OWNER: f"modules/{_OWNER}"},
    )
    from okg.substrate.catalog.module_providers import builtin_module_providers

    composed = compose_catalog(root, providers=dict(builtin_module_providers()))
    assert composed.module_origins["bookkeeping"]["provider_kind"] == "builtin"


# ---------- (c) the declaration refuses what it cannot contain ----------


@pytest.mark.parametrize(
    "declaration, because",
    [
        ({"okg": "modules/x"}, "the built-in owner would be shadowed"),
        ({_OWNER: "/etc"}, "absolute paths leave the deployment"),
        ({_OWNER: "../outside"}, "a parent escape leaves the deployment"),
        ({_OWNER: "modules/missing"}, "a declared directory must exist"),
        ({"a:b": "modules/x"}, "an owner is one path segment"),
        ({"..": "modules/x"}, "an owner is not a traversal"),
        ({}, "an empty declaration declares nothing"),
        ({_OWNER: "modules/x", _OWNER.upper(): "modules/x"}, "owners that fold together collide"),
    ],
)
def test_the_declaration_refuses_what_it_cannot_contain(
    tmp_path: Path, declaration: dict, because: str,
) -> None:
    root = tmp_path / "refused"
    root.mkdir()
    (root / "modules" / _OWNER).mkdir(parents=True)
    (root / "modules" / "x").mkdir(parents=True, exist_ok=True)
    with pytest.raises(DeploymentConfigError):
        resolve_declared_module_providers(
            {"module_providers": declaration},
            root,
            manifest_path=root / "deployment.yaml",
        )


def test_a_symlinked_component_is_refused(tmp_path: Path) -> None:
    """Containment follows links, so a link inside the tree is refused first."""
    root = tmp_path / "linked"
    (root / "modules").mkdir(parents=True)
    outside = tmp_path / "elsewhere"
    outside.mkdir()
    os.symlink(outside, root / "modules" / "link")
    with pytest.raises(DeploymentConfigError) as caught:
        resolve_declared_module_providers(
            {"module_providers": {_OWNER: "modules/link"}},
            root,
            manifest_path=root / "deployment.yaml",
        )
    assert "symlink" in str(caught.value)


def test_an_absent_declaration_is_not_an_error(tmp_path: Path) -> None:
    root = tmp_path / "absent"
    root.mkdir()
    assert resolve_declared_module_providers(
        {}, root, manifest_path=root / "deployment.yaml",
    ) == {}


# ---------- (d) extraction reads only what the plan named ----------


def _package(tmp_path: Path) -> tuple[Path, object]:
    """Build and verify a minimal package-v2 archive declaring one module."""
    import hashlib

    from okg.distributions import (
        AssetLocator, CompatibilityMetadata, DistributionObject,
        DistributionPackageManifest, DistributionPackageProvenance,
        DistributionPackageSource, DistributionRelease, ModuleDeclaration,
        OwnerQualifiedReference, PackagedAsset, TypedAsset,
        build_distribution_package, canonical_bundle_digest,
        canonical_release_digest, verify_distribution_package,
    )
    from okg.substrate.distributions import package as package_impl

    def digest(body: bytes) -> str:
        return "sha256:" + hashlib.sha256(body).hexdigest()

    authoring = tmp_path / "authoring"
    authoring.mkdir(parents=True)
    output = tmp_path / "packages"
    output.mkdir()

    schema_file = authoring / "schema.yaml"
    schema_file.write_bytes(_SCHEMA_YAML)
    locator = AssetLocator(object_digest=digest(_SCHEMA_YAML))
    source_path, object_body, media = schema_file, _SCHEMA_YAML, "application/octet-stream"

    asset = TypedAsset(
        owner=_OWNER, asset_id="records-schema", kind="ontology_schema",
        plane="semantic-catalog", version="v1", digest=digest(_SCHEMA_YAML),
        schema="okg.deployment_product.typed_asset/v1", merge_identity="records-schema",
        merge_rule="refuse",
        compatibility={"contract_versions": ["v1"], "capabilities": ["public"]},
        provenance={"source": "package", "repository_url": "https://example.invalid/pkg",
                    "revision": "0" * 40, "artifact_ref": f"{_OWNER}:records-schema"},
    )
    reference = OwnerQualifiedReference(
        owner=asset.owner, id=asset.asset_id, version=asset.version, digest=asset.digest,
    )
    compatibility = CompatibilityMetadata(
        contract_versions=("v1",), capabilities=("public",),
    )
    provisional_bundle = package_impl.ProductBundle(
        bundle_id="records", version="v1", distribution_id=_OWNER,
        distribution_version="1.0.0", bundle_digest="sha256:" + "0" * 64,
        assets=(reference,), compatibility=compatibility,
    )
    bundle = provisional_bundle.model_copy(
        update={"bundle_digest": canonical_bundle_digest(provisional_bundle)},
    )
    release = DistributionRelease(
        distribution_id=_OWNER, version="1.0.0", origin="external",
        release_digest="sha256:" + "0" * 64, assets=(reference,),
        bundle_ids=("records",), compatibility=compatibility,
    )
    provisional = DistributionPackageManifest(
        schema_version=package_impl.SCHEMA_V2,
        distribution_release=release, bundles=(bundle,),
        assets=(PackagedAsset(asset=asset, locator=locator,
                              uncompressed_size=len(_SCHEMA_YAML)),),
        objects=(DistributionObject(digest=digest(object_body),
                                    size=len(object_body), media_type=media),),
        provenance=DistributionPackageProvenance(
            repository_url="https://example.invalid/pkg", revision="0" * 40,
            builder="module-provider-tests",
        ),
        okg_compatibility=package_impl.OkgCompatibility(
            min_version="0.1.0", max_version_exclusive="9.0.0",
        ),
        module_declarations=(
            ModuleDeclaration(module_id=_MODULE_ID, asset_refs=(reference,)),
        ),
    )
    manifest = provisional.model_copy(update={
        "distribution_release": release.model_copy(
            update={"release_digest": canonical_release_digest(provisional)}),
    })
    archive = build_distribution_package(
        manifest,
        {digest(object_body): DistributionPackageSource(root=authoring, path=source_path)},
        output,
    )
    return archive, verify_distribution_package(archive)


def test_extraction_writes_the_planned_module_as_configuration_bytes(
    tmp_path: Path,
) -> None:
    from okg.substrate.distributions.install_execution import _packaged_module_files

    _, package = _package(tmp_path)
    try:
        files = _packaged_module_files(package, [_MODULE_ID])
    finally:
        package.close_runtime_authority()
    assert files == {f"modules/{_OWNER}/{_MODULE}/schema.yaml": _SCHEMA_YAML}


def test_extraction_writes_nothing_when_the_plan_names_no_package_module(
    tmp_path: Path,
) -> None:
    """A built-in-only install declares nothing and behaves as before."""
    from okg.substrate.distributions.install_execution import _packaged_module_files

    _, package = _package(tmp_path)
    try:
        assert _packaged_module_files(package, ["bookkeeping"]) == {}
        assert _packaged_module_files(package, []) == {}
    finally:
        package.close_runtime_authority()


def test_extraction_refuses_an_owner_the_plan_did_not_name(tmp_path: Path) -> None:
    from okg.substrate.distributions.install_execution import _packaged_module_files

    _, package = _package(tmp_path)
    try:
        with pytest.raises(SubstrateError) as caught:
            _packaged_module_files(package, ["other-owner:records"])
    finally:
        package.close_runtime_authority()
    assert caught.value.data["reason"] == "packaged_module_owner_unplanned"


def test_extraction_refuses_an_unlisted_module_of_a_planned_owner(
    tmp_path: Path,
) -> None:
    """The owner is planned; this module of theirs is not, and says so."""
    from okg.substrate.distributions.install_execution import _packaged_module_files

    _, package = _package(tmp_path)
    try:
        with pytest.raises(SubstrateError) as caught:
            _packaged_module_files(package, [f"{_OWNER}:something_else"])
    finally:
        package.close_runtime_authority()
    assert caught.value.data["reason"] == "packaged_module_unplanned"


def test_extraction_refuses_a_module_stored_inside_another_object() -> None:
    """The plan records the enclosing object's digest, so nothing could match.

    Shaped stub rather than a real archive: the refusal reads only the
    manifest's locators and declarations, and fires before a byte is read, so
    building a carrier wheel would test the packager rather than this rule.
    """
    from types import SimpleNamespace

    from okg.substrate.distributions.install_execution import _packaged_module_files

    reference = SimpleNamespace(qualified_id=f"{_OWNER}:records-schema")
    package = SimpleNamespace(manifest=SimpleNamespace(
        assets=[SimpleNamespace(
            asset=SimpleNamespace(qualified_id=f"{_OWNER}:records-schema"),
            locator=SimpleNamespace(member_path="schema.yaml"),
        )],
        module_declarations=(SimpleNamespace(
            module_id=_MODULE_ID, asset_refs=(reference,),
        ),),
    ))
    with pytest.raises(SubstrateError) as caught:
        _packaged_module_files(package, [_MODULE_ID])
    assert caught.value.data["reason"] == "packaged_module_member_asset_unsupported"


# ---------- (e) drift against a plan the planner actually produced ----------


def _planned(tmp_path: Path) -> tuple[dict, dict[str, bytes]]:
    """A real plan from `resolve_install_plan`, and configuration matching it."""
    from okg.substrate.distributions.install_execution import _packaged_module_files
    from okg.substrate.distributions.install_plan import resolve_install_plan

    archive, package = _package(tmp_path)
    try:
        files = _packaged_module_files(package, [_MODULE_ID])
    finally:
        package.close_runtime_authority()
    plan, _lock = resolve_install_plan(
        str(archive), "records", {"name": "sample", "modules": [_MODULE_ID]},
    )
    body = {
        "name": "sample",
        "schema_dir": "schemas",
        "source_registry": "source_registry.yaml",
        "modules": [_MODULE_ID],
        "module_providers": {_OWNER: f"modules/{_OWNER}"},
    }
    configuration = dict(files)
    configuration["deployment.yaml"] = yaml.safe_dump(body, sort_keys=False).encode()
    configuration["source_registry.yaml"] = b"sources: {}\n"
    return plan, configuration


def test_drift_accepts_the_configuration_the_planner_composed(tmp_path: Path) -> None:
    plan, files = _planned(tmp_path)
    _check_module_provider_drift(files, plan)


def test_drift_accepts_deployment_owned_schemas_the_plan_never_saw(
    tmp_path: Path,
) -> None:
    """The installed tree carries schemas/; the planner's scratch did not."""
    plan, files = _planned(tmp_path)
    files["schemas/sample_base.yaml"] = (
        b"classes:\n  SampleLocal:\n    annotations:\n      archetype: entity\n"
    )
    _check_module_provider_drift(files, plan)


def test_drift_ignores_how_a_built_in_module_is_spelled(tmp_path: Path) -> None:
    """The manifest spells it bare, the plan spells it `okg:<name>`."""
    plan, files = _planned(tmp_path)
    body = yaml.safe_load(files["deployment.yaml"])
    body["modules"] = [_MODULE_ID, "bookkeeping"]
    files["deployment.yaml"] = yaml.safe_dump(body, sort_keys=False).encode()
    plan["catalog"]["module_refs"] = [_MODULE_ID, "okg:bookkeeping"]
    plan["catalog"]["subtype_origin"] = dict(plan["catalog"]["subtype_origin"])
    from okg.substrate.catalog.modules_compose import compose_catalog
    import tempfile
    with tempfile.TemporaryDirectory() as scratch:
        base = Path(scratch) / "configuration"
        base.mkdir()
        for name, data in files.items():
            dest = base / name
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(data)
        composed = compose_catalog(base)
    plan["catalog"]["subtype_origin"] = {
        subtype: origin for subtype, origin in composed.subtype_origin.items()
        if not str(origin).startswith("deployment:")
    }
    _check_module_provider_drift(files, plan)


@pytest.mark.parametrize("mutate, because", [
    (lambda p, f: p["catalog"].__setitem__("module_refs", []),
     "the plan recorded no modules"),
    (lambda p, f: p["catalog"]["subtype_origin"].__setitem__("example_record", "other:mod"),
     "the plan recorded a different subtype owner"),
    (lambda p, f: p["modules"][0].__setitem__("provider_kind", "builtin"),
     "the plan recorded a different provider kind"),
    (lambda p, f: p["modules"][0].__setitem__("object_digests", ["sha256:" + "0" * 64]),
     "the plan recorded different module bytes"),
    (lambda p, f: p.__setitem__("catalog", None), "the plan carries no catalog"),
    (lambda p, f: p.__setitem__("modules", None), "the plan carries no module list"),
    (lambda p, f: p["catalog"].__delitem__("subtype_origin"),
     "the plan carries no subtype origins"),
    (lambda p, f: p["modules"].__setitem__(0, {"provider_kind": "package"}),
     "a module row names no module"),
    (lambda p, f: p["modules"][0].__setitem__("object_digests", []),
     "a packaged module row lists no digests"),
])
def test_drift_refuses_what_the_planner_did_not_record(
    tmp_path: Path, mutate, because: str,
) -> None:
    plan, files = _planned(tmp_path)
    mutate(plan, files)
    with pytest.raises(SubstrateError) as caught:
        _check_module_provider_drift(files, plan)
    assert caught.value.data["reason"] == "packaged_module_provider_drift"


def test_drift_is_not_consulted_without_a_declaration(tmp_path: Path) -> None:
    """A built-in-only configuration has no declared owner to check."""
    files = {
        "deployment.yaml": yaml.safe_dump(
            {"name": "sample", "schema_dir": "schemas",
             "source_registry": "source_registry.yaml", "modules": ["bookkeeping"]},
            sort_keys=False,
        ).encode(),
        "source_registry.yaml": b"sources: {}\n",
    }
    _check_module_provider_drift(files, {})
