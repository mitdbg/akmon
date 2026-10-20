"""Ontology modules served out of a verified distribution package (#1179 s4).

Every test builds a real package-v2 archive in a temporary directory,
verifies it, and composes a catalog from it. No database, no network, no
import of packaged code.

**Fixture ownership.** `_verified_package` here is a deliberately minimal
package: one neutral ontology module, one object, two bundles sharing it.
It is a strict subset of the shared distribution fixture #1185 proposes and
may be absorbed by it; it builds none of #1185's four additional items.
"""
from __future__ import annotations

import hashlib
import json
import sys
import unicodedata
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from pydantic import ValidationError

from okg.substrate.catalog.module_providers import (
    BUILTIN_OWNER,
    CrossOwnerSubtypeCollision,
    ModuleIdentityNotNormalized,
    ModulePathEscapesProviderRoot,
    ModuleProvider,
    PackagedModuleUndeclared,
    builtin_module_providers,
    parse_module_ref,
    resolve_modules,
)
from okg.substrate.catalog.modules_compose import compose_catalog
from okg.substrate.catalog.package_module_provider import (
    PackagedModuleOwnerCollision,
    PackagedModuleUnavailable,
    package_module_providers,
)
from okg.substrate.contracts.deployment_product import (
    CompatibilityMetadata,
    DistributionRelease,
    OwnerQualifiedReference,
)
from okg.substrate.contracts.deployment_product.typed_assets import TypedAsset
from okg.substrate.distributions import (
    AssetLocator,
    DistributionObject,
    DistributionPackageManifest,
    DistributionPackageProvenance,
    DistributionPackageSource,
    ModuleDeclaration,
    OkgCompatibility,
    PackagedAsset,
    VerifiedDistributionPackage,
    build_distribution_package,
    canonical_release_digest,
    verify_distribution_package,
)
from okg.substrate.distributions import package as package_impl

from .test_module_providers import (
    GOLDEN_PATH,
    PROFILES_DIR,
    _catalog_fingerprint,
    _deployment,
    _empty_cross_links,
    _profile_modules,
    _write_module,
)


_ZERO_DIGEST = "sha256:" + "0" * 64
_ASSET_SCHEMA = "okg.deployment_product.typed_asset/v1"
_REVISION = "0" * 40
_OWNER = "archi-test"
_MODULE = "lab_units"
_MODULE_ID = f"{_OWNER}:{_MODULE}"
_SCHEMA_YAML = b"""classes:
  LabUnit:
    annotations:
      archetype: entity
  LabRun:
    annotations:
      archetype: event
"""


def _digest(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _asset(
    asset_id: str,
    payload: bytes,
    *,
    kind: str,
    plane: str = "semantic-catalog",
) -> TypedAsset:
    return TypedAsset(
        owner=_OWNER,
        asset_id=asset_id,
        kind=kind,
        plane=plane,
        version="v1",
        digest=_digest(payload),
        schema=_ASSET_SCHEMA,
        merge_identity=asset_id,
        merge_rule="refuse",
        compatibility={"contract_versions": ["v1"], "capabilities": ["public"]},
        provenance={
            "source": "package",
            "repository_url": "https://github.com/example/lab-units",
            "revision": _REVISION,
            "artifact_ref": f"{_OWNER}:{asset_id}",
        },
    )


def _ref(asset: TypedAsset) -> OwnerQualifiedReference:
    return OwnerQualifiedReference(
        owner=asset.owner,
        id=asset.asset_id,
        kind=asset.kind,
        version=asset.version,
        digest=asset.digest,
    )


def _bundle(bundle_id: str, refs: tuple[OwnerQualifiedReference, ...]) -> Any:
    provisional = package_impl.ProductBundle(
        bundle_id=bundle_id,
        version="v1",
        distribution_id=_OWNER,
        distribution_version="1.0.0",
        bundle_digest=_ZERO_DIGEST,
        assets=refs,
        compatibility=CompatibilityMetadata(
            contract_versions=("v1",), capabilities=("public",)
        ),
    )
    return provisional.model_copy(
        update={"bundle_digest": package_impl.canonical_bundle_digest(provisional)}
    )


def _verified_package(
    tmp_path: Path,
    *,
    schema_payload: bytes = _SCHEMA_YAML,
    declarations: tuple[ModuleDeclaration, ...] | None = None,
    extra_kind: str | None = None,
    connector_payload: bytes | None = None,
) -> VerifiedDistributionPackage:
    """Build, then verify, a minimal package-v2 archive.

    One ontology-schema object, two bundles sharing its reference, and one
    module declaration naming it. `extra_kind` packages the same bytes a
    second time under a different asset kind, for the "declared asset is
    not a module file" arm.
    """
    root = tmp_path / "authoring"
    root.mkdir(parents=True)
    output = tmp_path / "packages"
    output.mkdir()
    schema_file = root / "schema.yaml"
    schema_file.write_bytes(schema_payload)

    schema_asset = _asset("lab-units-schema", schema_payload, kind="ontology_schema")
    assets = [schema_asset]
    if extra_kind is not None:
        assets.append(_asset("lab-units-notes", schema_payload, kind=extra_kind))
    payload_by_asset = {item.asset_id: schema_payload for item in assets}
    sources = {_digest(schema_payload): DistributionPackageSource(
        root=root, path=schema_file
    )}
    objects = [
        DistributionObject(
            digest=_digest(schema_payload),
            size=len(schema_payload),
            media_type="application/octet-stream",
        ),
    ]
    if connector_payload is not None:
        connector_file = root / "connector.json"
        connector_file.write_bytes(connector_payload)
        connector_asset = _asset(
            "lab-units-connector",
            connector_payload,
            kind="connector",
            plane="ingestion-projection",
        )
        assets.append(connector_asset)
        payload_by_asset[connector_asset.asset_id] = connector_payload
        sources[_digest(connector_payload)] = DistributionPackageSource(
            root=root, path=connector_file
        )
        objects.append(
            DistributionObject(
                digest=_digest(connector_payload),
                size=len(connector_payload),
                media_type="application/octet-stream",
            )
        )
    refs = tuple(_ref(item) for item in assets)
    packaged = tuple(
        PackagedAsset(
            asset=item,
            locator=AssetLocator(
                object_digest=_digest(payload_by_asset[item.asset_id])
            ),
            uncompressed_size=len(payload_by_asset[item.asset_id]),
        )
        for item in assets
    )
    bundles = (_bundle("lab-alpha", refs), _bundle("lab-beta", refs))
    if declarations is None:
        declarations = (
            ModuleDeclaration(module_id=_MODULE_ID, asset_refs=(refs[0],)),
        )
    release_provisional = DistributionRelease(
        distribution_id=_OWNER,
        version="1.0.0",
        origin="external",
        release_digest=_ZERO_DIGEST,
        assets=refs,
        bundle_ids=tuple(item.bundle_id for item in bundles),
        compatibility=CompatibilityMetadata(
            contract_versions=("v1",), capabilities=("public",)
        ),
    )
    provisional = DistributionPackageManifest(
        schema_version=package_impl.SCHEMA_V2,
        distribution_release=release_provisional,
        bundles=bundles,
        assets=packaged,
        objects=tuple(
            sorted(objects, key=lambda item: item.digest)
        ),
        provenance=DistributionPackageProvenance(
            repository_url="https://github.com/example/lab-units",
            revision=_REVISION,
            builder="okg-hermetic-test",
        ),
        okg_compatibility=OkgCompatibility(
            min_version="0.1.0", max_version_exclusive="9.0.0"
        ),
        module_declarations=declarations,
    )
    manifest = cast(
        DistributionPackageManifest,
        provisional.model_copy(
            update={
                "distribution_release": release_provisional.model_copy(
                    update={"release_digest": canonical_release_digest(provisional)}
                )
            }
        ),
    )
    archive = build_distribution_package(manifest, sources, output)
    return verify_distribution_package(archive)


def _providers(tmp_path: Path, package: VerifiedDistributionPackage) -> dict[str, Any]:
    return package_module_providers(package, tmp_path / "stage")


class _UnvalidatedPackage:
    """A package whose manifest never passed the v2 model validators.

    Package v2 already refuses a non-NFC module id and a declaration naming
    an absent asset, so those two arms are unreachable through a real
    archive. The provider must not depend on that: it re-checks both, and
    this stub is how those re-checks are exercised.
    """

    def __init__(self, declarations: tuple[ModuleDeclaration, ...]) -> None:
        self.manifest = SimpleNamespace(
            module_declarations=declarations,
            assets=(),
            distribution_release=SimpleNamespace(release_digest=_ZERO_DIGEST),
        )

    def read_asset(self, asset_ref: str) -> bytes:  # pragma: no cover - never reached
        raise AssertionError("the provider must refuse before reading bytes")


class _LooseDeclaration:
    """A declaration carrying an id the v2 model would have refused."""

    def __init__(self, module_id: str) -> None:
        self.module_id = module_id
        self.asset_refs: tuple[OwnerQualifiedReference, ...] = ()

    @property
    def owner(self) -> str:
        return self.module_id.split(":", 1)[0]

    @property
    def name(self) -> str:
        return self.module_id.split(":", 1)[1]


# ---------- (a) resolution through the verified store, by declared id ----------


def test_declared_module_composes_as_a_peer_of_the_builtins(tmp_path: Path) -> None:
    package = _verified_package(tmp_path)
    providers = {
        BUILTIN_OWNER: ModuleProvider(BUILTIN_OWNER, tmp_path / "builtin"),
        **_providers(tmp_path, package),
    }
    _write_module(tmp_path / "builtin", "reasoning", {"Hypothesis": "entity"})
    composed = compose_catalog(
        _deployment(tmp_path, ["reasoning", _MODULE_ID]),
        providers=providers,
        cross_links_root=_empty_cross_links(tmp_path),
    )
    assert composed.subtypes == {"hypothesis", "lab_unit", "lab_run"}
    assert composed.subtype_origin["lab_unit"] == _MODULE_ID
    assert providers[_OWNER].kind == "package"


def test_provider_serves_only_declared_ids_and_imports_nothing(
    tmp_path: Path,
) -> None:
    """The staging root holds the declared module and nothing else."""
    package = _verified_package(tmp_path)
    providers = _providers(tmp_path, package)
    root = providers[_OWNER].root
    assert sorted(path.name for path in root.iterdir()) == [_MODULE]
    assert sorted(p.name for p in (root / _MODULE).iterdir()) == ["schema.yaml"]
    assert (root / _MODULE / "schema.yaml").read_bytes() == _SCHEMA_YAML
    assert providers[_OWNER].declared_modules == frozenset({_MODULE})
    assert not [name for name in sys.modules if name.startswith(_OWNER)]


# ---------- (b) resolve-and-contain ----------


def test_symlinked_module_directory_refuses_on_every_provider_kind(
    tmp_path: Path,
) -> None:
    """The #1737 review found symlinked module dirs were followed."""
    outside = tmp_path / "outside"
    _write_module(outside, "physics", {"Detector": "entity"})
    for kind in ("builtin", "package"):
        root = tmp_path / f"root-{kind}"
        root.mkdir()
        (root / "physics").symlink_to(outside / "physics", target_is_directory=True)
        provider = ModuleProvider(_OWNER, root, kind=kind)
        with pytest.raises(ModulePathEscapesProviderRoot) as excinfo:
            provider.module_dir("physics")
        assert excinfo.value.code == "module_path_escapes_provider_root"
        assert excinfo.value.data["module_name"] == "physics"


def test_dot_dot_and_absolute_module_references_refuse(tmp_path: Path) -> None:
    provider = ModuleProvider(_OWNER, tmp_path / "root")
    for name in ("..", "../escape", "/etc"):
        with pytest.raises(ModulePathEscapesProviderRoot):
            provider.module_dir(name)


def test_package_archive_cannot_carry_a_symlinked_module(tmp_path: Path) -> None:
    """A packaged module's bytes are written by the provider, never linked."""
    package = _verified_package(tmp_path)
    root = _providers(tmp_path, package)[_OWNER].root
    schema = root / _MODULE / "schema.yaml"
    assert not schema.is_symlink() and schema.is_file()
    assert not (root / _MODULE).is_symlink()


# ---------- (c) Unicode NFC ----------


def test_nfc_owner_parses_and_its_nfd_spelling_refuses() -> None:
    nfc = unicodedata.normalize("NFC", "café:units")
    assert parse_module_ref(nfc) == ("café", "units")
    nfd = unicodedata.normalize("NFD", nfc)
    assert nfd != nfc
    with pytest.raises(ModuleIdentityNotNormalized) as excinfo:
        parse_module_ref(nfd)
    assert excinfo.value.code == "module_identity_not_nfc"
    assert excinfo.value.data["nfc_form"] == nfc


def test_package_v2_already_refuses_a_non_nfc_module_id(tmp_path: Path) -> None:
    """A non-NFC id is refused when the manifest is sealed, not later.

    `ModuleDeclaration` itself accepts the decomposed spelling; the refusal
    fires in the canonical-JSON pass that seals the release digest. Slice 4
    keeps the same stance one layer up, on the composition side.
    """
    with pytest.raises((ValidationError, ValueError)) as excinfo:
        _verified_package(
            tmp_path,
            declarations=(
                ModuleDeclaration(
                    module_id=unicodedata.normalize("NFD", "café:units"),
                ),
            ),
        )
    assert "not NFC" in str(excinfo.value)


def test_provider_re_refuses_a_non_nfc_declaration(tmp_path: Path) -> None:
    """Refused where it is declared, not folded away when compared."""
    package = _UnvalidatedPackage(
        cast(
            "tuple[ModuleDeclaration, ...]",
            (_LooseDeclaration(unicodedata.normalize("NFD", "café:units")),),
        )
    )
    with pytest.raises(ModuleIdentityNotNormalized) as excinfo:
        package_module_providers(package, tmp_path / "stage")
    assert excinfo.value.code == "module_identity_not_nfc"


# ---------- (d) declared and undeclared ----------


def test_undeclared_module_refuses_and_names_the_id(tmp_path: Path) -> None:
    package = _verified_package(tmp_path)
    providers = _providers(tmp_path, package)
    with pytest.raises(PackagedModuleUndeclared) as excinfo:
        resolve_modules([f"{_OWNER}:not_declared"], providers)
    assert excinfo.value.code == "package_module_undeclared"
    assert excinfo.value.data["module_id"] == f"{_OWNER}:not_declared"
    assert excinfo.value.data["declared_modules"] == [_MODULE]


def _absent_ref() -> OwnerQualifiedReference:
    return OwnerQualifiedReference(
        owner=_OWNER, id="ghost", kind="ontology_schema",
        version="v1", digest=_digest(b"ghost"),
    )


def test_package_v2_already_refuses_a_declaration_naming_an_absent_asset(
    tmp_path: Path,
) -> None:
    """The manifest model binds every declared reference to a packaged asset."""
    with pytest.raises(ValidationError) as excinfo:
        _verified_package(
            tmp_path,
            declarations=(
                ModuleDeclaration(module_id=_MODULE_ID, asset_refs=(_absent_ref(),)),
            ),
        )
    assert "does not resolve" in str(excinfo.value)


def test_provider_re_refuses_a_declaration_naming_an_absent_asset(
    tmp_path: Path,
) -> None:
    declaration = _LooseDeclaration(_MODULE_ID)
    declaration.asset_refs = (_absent_ref(),)
    package = _UnvalidatedPackage(
        cast("tuple[ModuleDeclaration, ...]", (declaration,))
    )
    with pytest.raises(PackagedModuleUnavailable) as excinfo:
        package_module_providers(package, tmp_path / "stage")
    assert excinfo.value.code == "package_module_unavailable"
    assert excinfo.value.data["module_id"] == _MODULE_ID
    assert excinfo.value.data["asset_ref"] == f"{_OWNER}:ghost"


def test_duplicate_module_declarations_refuse_at_manifest_sealing(
    tmp_path: Path,
) -> None:
    """Two declarations of one id never reach the provider."""
    declaration = ModuleDeclaration(module_id=_MODULE_ID)
    with pytest.raises((ValidationError, ValueError)) as excinfo:
        _verified_package(tmp_path, declarations=(declaration, declaration))
    assert "unique by module id" in str(excinfo.value)


def test_two_declared_assets_supplying_one_module_file_refuse(
    tmp_path: Path,
) -> None:
    """Neither payload silently wins the module's schema.yaml."""
    package = _verified_package(tmp_path, extra_kind="ontology_schema")
    refs = tuple(package.manifest.distribution_release.asset_refs)
    other = _verified_package(
        tmp_path / "second",
        extra_kind="ontology_schema",
        declarations=(
            ModuleDeclaration(module_id=_MODULE_ID, asset_refs=refs),
        ),
    )
    with pytest.raises(PackagedModuleUnavailable) as excinfo:
        _providers(tmp_path / "second", other)
    assert excinfo.value.code == "package_module_unavailable"
    assert "schema.yaml" in excinfo.value.data["reason"]


def test_declaration_supplying_no_schema_refuses(tmp_path: Path) -> None:
    package = _verified_package(tmp_path, extra_kind="playbook")
    notes = next(
        ref
        for ref in package.manifest.distribution_release.asset_refs
        if ref.qualified_id == f"{_OWNER}:lab-units-notes"
    )
    other = _verified_package(
        tmp_path / "second",
        extra_kind="playbook",
        declarations=(
            ModuleDeclaration(module_id=_MODULE_ID, asset_refs=(notes,)),
        ),
    )
    with pytest.raises(PackagedModuleUnavailable) as excinfo:
        _providers(tmp_path / "second", other)
    assert excinfo.value.code == "package_module_unavailable"
    assert excinfo.value.data["module_id"] == _MODULE_ID


# ---------- (e) composition records the supplying provider ----------


def test_composition_records_owner_kind_and_release_digest(tmp_path: Path) -> None:
    package = _verified_package(tmp_path)
    _write_module(tmp_path / "builtin", "reasoning", {"Hypothesis": "entity"})
    composed = compose_catalog(
        _deployment(tmp_path, ["reasoning", _MODULE_ID]),
        providers={
            BUILTIN_OWNER: ModuleProvider(BUILTIN_OWNER, tmp_path / "builtin"),
            **_providers(tmp_path, package),
        },
        cross_links_root=_empty_cross_links(tmp_path),
    )
    assert composed.module_origins["reasoning"] == {
        "owner": BUILTIN_OWNER,
        "provider_kind": "builtin",
    }
    # `object_digests` was added by slice 6, which seals the origin into the
    # release record; the other three keys are what slice 4 recorded.
    assert composed.module_origins[_MODULE_ID] == {
        "owner": _OWNER,
        "provider_kind": "package",
        "package_release_digest": (
            package.manifest.distribution_release.release_digest
        ),
        "object_digests": [_digest(_SCHEMA_YAML)],
    }


# ---------- (f) the golden, and a cross-owner collision ----------


def test_packaged_module_composes_while_shipped_profiles_stay_identical(
    tmp_path: Path,
) -> None:
    """The packaged subtypes appear; the six shipped profiles do not move."""
    package = _verified_package(tmp_path)
    composed = compose_catalog(
        _deployment(tmp_path, [_MODULE_ID]),
        providers={
            BUILTIN_OWNER: ModuleProvider(BUILTIN_OWNER, tmp_path / "builtin"),
            **_providers(tmp_path, package),
        },
        cross_links_root=_empty_cross_links(tmp_path),
    )
    assert {"lab_unit", "lab_run"} <= composed.subtypes
    assert composed.module_origins[_MODULE_ID]["provider_kind"] == "package"

    golden = json.loads(GOLDEN_PATH.read_text())
    profiles = sorted(
        path.name
        for path in PROFILES_DIR.iterdir()
        if (path / "modules.yaml").is_file()
    )
    assert profiles == sorted(golden)
    observed = {
        name: _catalog_fingerprint(_profile_modules(name), tmp_path / "golden" / name)
        for name in profiles
    }
    assert observed == golden


def test_packaged_module_colliding_with_a_builtin_subtype_refuses(
    tmp_path: Path,
) -> None:
    package = _verified_package(
        tmp_path,
        schema_payload=b"classes:\n  Table:\n    annotations:\n      archetype: entity\n",
    )
    with pytest.raises(CrossOwnerSubtypeCollision) as excinfo:
        compose_catalog(
            _deployment(tmp_path, ["dataset", _MODULE_ID]),
            providers={
                **builtin_module_providers(),
                **_providers(tmp_path, package),
            },
            cross_links_root=_empty_cross_links(tmp_path),
        )
    error = excinfo.value
    assert error.code == "duplicate_semantic_ownership"
    assert error.data["phase"] == "pre_mutation"
    assert error.data["subtype"] == "table"
    assert error.data["owners"] == [_OWNER, BUILTIN_OWNER]



# ---------- (e) staging-root hygiene, carried over from slice 4 ----------


class _RelabelledPackage:
    """A real verified package re-declared under caller-chosen module ids.

    The bytes, assets and reader are the package's own, so a declaration
    resolves and stages for real; only the ids differ. Package v2 refuses
    both shapes below at parse time, so this is how the provider's own
    re-checks are reached.
    """

    def __init__(
        self,
        package: VerifiedDistributionPackage,
        module_ids: tuple[str, ...],
    ) -> None:
        refs = package.manifest.module_declarations[0].asset_refs
        self._package = package
        self.manifest = SimpleNamespace(
            module_declarations=tuple(
                _LooseDeclarationWithRefs(module_id, refs)
                for module_id in module_ids
            ),
            assets=package.manifest.assets,
            distribution_release=package.manifest.distribution_release,
        )

    def read_asset(self, asset_ref: str) -> bytes:
        return self._package.read_asset(asset_ref)


class _LooseDeclarationWithRefs(_LooseDeclaration):
    """A declaration the v2 model would refuse, but that resolves to bytes."""

    def __init__(
        self,
        module_id: str,
        asset_refs: tuple[OwnerQualifiedReference, ...],
    ) -> None:
        super().__init__(module_id)
        self.asset_refs = asset_refs


def test_the_owner_segment_is_contained_before_the_staging_mkdir(
    tmp_path: Path,
) -> None:
    """A traversing owner refuses instead of creating a directory outside.

    Unreachable through a validated manifest, which is exactly why the
    re-check exists: the staging root can be operator-named, and every
    other segment this file joins is already checked the same way.
    """
    package = _UnvalidatedPackage((_LooseDeclaration("../escaped:lab_units"),))
    with pytest.raises(ModulePathEscapesProviderRoot) as excinfo:
        package_module_providers(cast(Any, package), tmp_path / "stage" / "deep")
    assert excinfo.value.data["owner"] == "../escaped"
    assert not (tmp_path / "escaped").exists()


def test_a_repeated_module_id_refuses_instead_of_overwriting(
    tmp_path: Path,
) -> None:
    """Two declarations must never share one staging directory.

    Package v2 refuses a duplicate module id, so this drives the provider's
    own exclusivity re-check rather than relying on the model for it.
    """
    package = _RelabelledPackage(
        _verified_package(tmp_path), (_MODULE_ID, _MODULE_ID),
    )
    with pytest.raises(PackagedModuleOwnerCollision) as excinfo:
        package_module_providers(cast(Any, package), tmp_path / "stage")
    error = excinfo.value
    assert error.code == "package_module_owner_collision"
    assert error.data["segment"] == "module"
    assert error.data["module_id"] == _MODULE_ID


def test_case_folded_owners_refuse_rather_than_merging(tmp_path: Path) -> None:
    """Two owners differing only by case refuse on every filesystem.

    The manifest keeps them as two distinct sealed identities, so folding
    them into one staged tree would make the staging root non-injective
    with the declarations that produced it. The refusal is decided from the
    declarations, not from what this filesystem happens to do, so a package
    cannot stage cleanly on a case-sensitive machine and collide on the
    operator's laptop.
    """
    package = _RelabelledPackage(
        _verified_package(tmp_path),
        (f"{_OWNER}:first", f"{_OWNER.upper()}:second"),
    )
    with pytest.raises(PackagedModuleOwnerCollision) as excinfo:
        package_module_providers(cast(Any, package), tmp_path / "stage")
    assert excinfo.value.data["segment"] == "owner"
    assert {excinfo.value.data["first"], excinfo.value.data["second"]} == {
        _OWNER,
        _OWNER.upper(),
    }


def test_every_staging_directory_is_private_including_intermediates(
    tmp_path: Path,
) -> None:
    """0o700 on the leaf is no protection if an intermediate is 0o755."""
    package = _verified_package(tmp_path)
    staging = tmp_path / "stage" / "nested" / "modules"
    package_module_providers(package, staging)
    for path in (staging, staging.parent, staging.parent.parent):
        assert path.stat().st_mode & 0o777 == 0o700, path
