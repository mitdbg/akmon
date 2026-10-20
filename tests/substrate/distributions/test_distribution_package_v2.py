"""Package schema v2: the OKG compatibility window and module declarations.

Every input is built in a temporary directory.  No test here imports a
packaged module or needs a database, network, catalog, or lifecycle runtime.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pytest
from pydantic import ValidationError

from okg.substrate.contracts.deployment_product import (
    AuthoredDeploymentDefinition,
    CompatibilityMetadata,
    DeploymentLockError,
    DistributionRelease,
    OwnerQualifiedReference,
    resolve_product_lock,
    validate_selection,
)
from okg.substrate.distributions import (
    DistributionPackageError,
    DistributionPackageManifest,
    ModuleDeclaration,
    OkgCompatibility,
    canonical_release_digest,
    check_okg_compatibility,
    installed_okg_version,
    verify_distribution_package,
)
from okg.substrate.distributions import package as package_impl

from .test_distribution_package import Fixture, _ZERO_DIGEST, _build, make_fixture


# Recorded from the unmodified package-v1 builder at origin/dev 1475c87d5d,
# before package v2 existed.  A v1 manifest must keep every one of these
# bytes: adding v2 fields to the model must not move a single v1 digest.
_V1_GOLDEN_RELEASE_DIGEST = (
    "sha256:f5119760da91cd37a8a9295f021ac3a295dbcb1c8daf9649503334b5ec9a1596"
)
_V1_GOLDEN_TRANSPORT_NAME = (
    "98bba265d6b2413c04b15931cf43ba141cc6a8d728b8281ac738b7f11112fdb1.okgdist.tar"
)
_V1_GOLDEN_RECEIPT_DIGEST = (
    "sha256:b44cbff3a14a7abf1652f8694410e716ed3611d823bade7670a22f3c477a36a2"
)
_V1_GOLDEN_VERIFICATION_PROFILE_DIGEST = (
    "sha256:a7e3420e425a25871d0cf3a50c5579cfebde6ec5feeb0770a63002f9cd8f2431"
)
_V2_VERIFICATION_PROFILE_DIGEST = (
    "sha256:abb770389d6a3c3ace2753b098c7b0c6df966cfb9054d2299c11b02e6ff47bdd"
)
_V1_GOLDEN_BUNDLE_DIGESTS = {
    "cern-team": (
        "sha256:246f371dd05e1aad56de373a1e60f73694c2ed4703452621fad918f171ba6c57"
    ),
    "fixture-bundle": (
        "sha256:4f5a8af09ab21039448bd8e2c770563744c808d2b984a0035dbe9fb19a04330c"
    ),
}

_WINDOW = OkgCompatibility(min_version="0.1.0", max_version_exclusive="2.0.0")


def _module_declarations(fixture: Fixture) -> tuple[ModuleDeclaration, ...]:
    schema_ref = next(
        ref
        for ref in fixture.manifest.distribution_release.asset_refs
        if ref.qualified_id == "archi:operations-schema"
    )
    return (ModuleDeclaration(module_id="archi:physics", asset_refs=(schema_ref,)),)


def _promote_to_v2(
    fixture: Fixture,
    *,
    okg_compatibility: OkgCompatibility | None = _WINDOW,
    module_declarations: tuple[ModuleDeclaration, ...] | None = None,
) -> Fixture:
    """Rebuild the fixture manifest as package v2 and re-seal its digest."""

    declarations = (
        _module_declarations(fixture)
        if module_declarations is None
        else module_declarations
    )
    manifest = fixture.manifest
    provisional = cast(
        DistributionPackageManifest,
        manifest.model_copy(
            update={
                "schema_version": package_impl.SCHEMA_V2,
                "okg_compatibility": okg_compatibility,
                "module_declarations": declarations,
                "distribution_release": manifest.distribution_release.model_copy(
                    update={"release_digest": _ZERO_DIGEST}
                ),
            }
        ),
    )
    release = provisional.distribution_release.model_copy(
        update={"release_digest": canonical_release_digest(provisional)}
    )
    sealed = cast(
        DistributionPackageManifest,
        provisional.model_copy(update={"distribution_release": release}),
    )
    return Fixture(
        fixture.root,
        fixture.output,
        fixture.wheel,
        fixture.tree,
        fixture.wheel_bytes,
        fixture.assets,
        sealed,
        fixture.sources,
    )


def _manifest_payload(fixture: Fixture, **overrides: Any) -> dict[str, Any]:
    payload = fixture.manifest.to_mapping()
    payload.update(overrides)
    return payload


# --- (c) a v1 package verifies exactly as it did before v2 existed ---------


def test_package_v1_digests_are_byte_identical_after_v2(tmp_path: Path) -> None:
    fixture = make_fixture(tmp_path)
    manifest = fixture.manifest
    assert manifest.schema_version == package_impl.SCHEMA
    assert manifest.is_v2 is False
    assert manifest.okg_compatibility is None
    assert manifest.module_declarations == ()
    assert "okg_compatibility" not in manifest.to_mapping()
    assert "module_declarations" not in manifest.to_mapping()
    assert manifest.distribution_release.release_digest == _V1_GOLDEN_RELEASE_DIGEST
    assert {
        bundle.bundle_id: bundle.bundle_digest for bundle in manifest.bundles
    } == _V1_GOLDEN_BUNDLE_DIGESTS

    package_path = _build(fixture)
    assert package_path.name == _V1_GOLDEN_TRANSPORT_NAME
    checked = verify_distribution_package(package_path)
    assert checked.manifest.schema_version == package_impl.SCHEMA
    assert checked.read_asset("archi:operations-schema") == b"schema: operations\n"
    with checked.extension_runtime_authority() as authority:
        assert authority.receipt.digest == _V1_GOLDEN_RECEIPT_DIGEST
        assert (
            authority.receipt.preimage()["distribution_package_schema"]
            == package_impl.SCHEMA
        )
        assert (
            authority.receipt.verification_profile_digest
            == _V1_GOLDEN_VERIFICATION_PROFILE_DIGEST
        )


def test_package_v1_refuses_the_v2_only_fields(tmp_path: Path) -> None:
    fixture = make_fixture(tmp_path)
    with pytest.raises(ValidationError, match="require package schema v2"):
        DistributionPackageManifest.model_validate(
            _manifest_payload(fixture, okg_compatibility=_WINDOW.model_dump())
        )
    with pytest.raises(ValidationError, match="require package schema v2"):
        DistributionPackageManifest.model_validate(
            _manifest_payload(
                fixture,
                module_declarations=[
                    item.model_dump() for item in _module_declarations(fixture)
                ],
            )
        )


# --- (a) the OKG compatibility window -------------------------------------


def test_package_v2_round_trips_the_window_and_declarations(tmp_path: Path) -> None:
    fixture = _promote_to_v2(make_fixture(tmp_path))
    checked = verify_distribution_package(_build(fixture))
    manifest = checked.manifest
    assert manifest.schema_version == package_impl.SCHEMA_V2
    assert manifest.is_v2 is True
    assert manifest.okg_compatibility == _WINDOW
    assert [item.module_id for item in manifest.module_declarations] == [
        "archi:physics"
    ]
    declaration = manifest.module_declarations[0]
    assert (declaration.owner, declaration.name) == ("archi", "physics")
    mapping = manifest.to_mapping()
    assert mapping["okg_compatibility"] == {
        "min_version": "0.1.0",
        "max_version_exclusive": "2.0.0",
    }
    assert mapping["module_declarations"][0]["module_id"] == "archi:physics"
    # A v2 package is self-describing: its receipt and verification profile
    # name the v2 schema rather than the module-level v1 constant.
    with checked.extension_runtime_authority() as authority:
        assert (
            authority.receipt.preimage()["distribution_package_schema"]
            == package_impl.SCHEMA_V2
        )
        assert authority.receipt.digest != _V1_GOLDEN_RECEIPT_DIGEST
        # The profile mapping is constant apart from package_schema, so this
        # digest is exactly the assertion that the profile names v2.
        assert (
            authority.receipt.verification_profile_digest
            == _V2_VERIFICATION_PROFILE_DIGEST
        )


@pytest.mark.parametrize(
    ("okg_version", "supported"),
    [
        ("0.1.0", True),
        ("0.1.0.dev7", True),
        ("0.1.0rc2", True),
        ("0.1.0a1", True),
        ("0.1.0.post1", True),
        ("0.1.0+local.build", True),
        ("1.9.9", True),
        ("0.0.9", False),
        ("0.0.9.post4", False),
        ("2.0.0", False),
        ("2.0.0.dev1", False),
        ("2.0.0rc1", False),
    ],
)
def test_okg_compatibility_compares_the_base_release_only(
    okg_version: str, supported: bool
) -> None:
    assert _WINDOW.supports(okg_version) is supported


def test_okg_compatibility_window_is_min_inclusive_and_max_exclusive() -> None:
    assert _WINDOW.supports("0.1.0") is True
    assert _WINDOW.supports("2.0.0") is False
    assert _WINDOW.supports("1.999.999") is True


@pytest.mark.parametrize(
    "bound",
    [
        "0.1.0rc1",
        "0.1.0.dev1",
        "0.1.0.post1",
        "0.1.0+local",
        "1!0.1.0",
        "",
        "one.two",
        # These parse, but not to themselves.  A bound is compared as a literal
        # string in the manifest and as a Version at check time, so a spelling
        # that normalizes away would give one window two readings.
        "01.0.0",
        "v1.0.0",
        "0.1.0-1",
    ],
)
def test_okg_compatibility_refuses_a_non_release_bound(bound: str) -> None:
    with pytest.raises(ValidationError):
        OkgCompatibility(min_version=bound, max_version_exclusive="9.0.0")
    with pytest.raises(ValidationError):
        OkgCompatibility(min_version="0.1.0", max_version_exclusive=bound)


@pytest.mark.parametrize(
    ("minimum", "maximum"), [("2.0.0", "1.0.0"), ("1.0.0", "1.0.0")]
)
def test_okg_compatibility_refuses_an_empty_or_inverted_window(
    minimum: str, maximum: str
) -> None:
    with pytest.raises(ValidationError, match="must precede"):
        OkgCompatibility(min_version=minimum, max_version_exclusive=maximum)


def test_check_okg_compatibility_refuses_outside_the_window(tmp_path: Path) -> None:
    fixture = _promote_to_v2(make_fixture(tmp_path))
    manifest = verify_distribution_package(_build(fixture)).manifest
    assert check_okg_compatibility(manifest, okg_version="0.4.0") is None
    assert check_okg_compatibility(manifest, okg_version="0.1.0rc1") is None
    for outside in ("0.0.1", "2.0.0", "2.1.0.dev3"):
        with pytest.raises(DistributionPackageError) as excinfo:
            check_okg_compatibility(manifest, okg_version=outside)
        assert excinfo.value.code == "okg_compatibility_unsupported"
        assert excinfo.value.logical_path == "okg_compatibility"
        assert "0.1.0" in str(excinfo.value) and "2.0.0" in str(excinfo.value)
    with pytest.raises(DistributionPackageError) as bad:
        check_okg_compatibility(manifest, okg_version="not-a-version")
    assert bad.value.code == "okg_version_unknown"


def test_check_okg_compatibility_passes_an_undeclared_window(tmp_path: Path) -> None:
    v1 = make_fixture(tmp_path / "one")
    assert check_okg_compatibility(v1.manifest, okg_version="99.0.0") is None
    v2 = _promote_to_v2(make_fixture(tmp_path / "two"), okg_compatibility=None)
    assert v2.manifest.okg_compatibility is None
    assert check_okg_compatibility(v2.manifest, okg_version="99.0.0") is None


def test_installed_okg_version_is_the_default_subject(tmp_path: Path) -> None:
    running = installed_okg_version()
    fixture = _promote_to_v2(
        make_fixture(tmp_path),
        okg_compatibility=OkgCompatibility(
            min_version="99.0.0", max_version_exclusive="99.1.0"
        ),
    )
    with pytest.raises(DistributionPackageError) as excinfo:
        check_okg_compatibility(fixture.manifest)
    assert excinfo.value.code == "okg_compatibility_unsupported"
    assert running in str(excinfo.value)


# --- (b) module declarations use the strict owner-qualified spelling -------


@pytest.mark.parametrize(
    "module_id",
    [
        "archi",
        "archi:physics:extra",
        "archi:",
        ":physics",
        "archi: physics",
        "archi physics",
        "archi:.",
        "archi:..",
        "archi:.hidden",
        ".:physics",
        "..:physics",
        "archi:physics/child",
        "archi:physics\\child",
        "archi;other:physics",
        "archi,other:physics",
        "",
    ],
)
def test_module_declaration_refuses_a_loose_spelling(module_id: str) -> None:
    with pytest.raises(ValidationError):
        ModuleDeclaration(module_id=module_id)


def test_module_declaration_accepts_the_strict_owner_qualified_form() -> None:
    declaration = ModuleDeclaration(module_id="archi:physics-core")
    assert declaration.module_id == "archi:physics-core"
    assert declaration.owner == "archi"
    assert declaration.name == "physics-core"
    assert declaration.asset_refs == ()


def test_module_declarations_must_be_unique_and_resolve_to_packaged_assets(
    tmp_path: Path,
) -> None:
    fixture = make_fixture(tmp_path)
    declared = _module_declarations(fixture)
    duplicate = declared + (
        ModuleDeclaration(module_id="archi:physics", asset_refs=()),
    )
    with pytest.raises(ValidationError, match="unique by module id"):
        DistributionPackageManifest.model_validate(
            _manifest_payload(
                fixture,
                schema_version=package_impl.SCHEMA_V2,
                module_declarations=[item.model_dump() for item in duplicate],
            )
        )
    unknown = (
        ModuleDeclaration(
            module_id="archi:physics",
            asset_refs=(
                OwnerQualifiedReference(
                    owner="archi", id="absent-schema", digest=_ZERO_DIGEST
                ),
            ),
        ),
    )
    with pytest.raises(ValidationError, match="does not resolve"):
        DistributionPackageManifest.model_validate(
            _manifest_payload(
                fixture,
                schema_version=package_impl.SCHEMA_V2,
                module_declarations=[item.model_dump() for item in unknown],
            )
        )


def test_module_declaration_asset_refs_must_be_unique(tmp_path: Path) -> None:
    fixture = make_fixture(tmp_path)
    ref = fixture.manifest.distribution_release.asset_refs[0]
    with pytest.raises(ValidationError, match="asset references must be unique"):
        ModuleDeclaration(module_id="archi:physics", asset_refs=(ref, ref))


# --- (d) contract_versions keeps its landed membership semantics -----------


def test_contract_versions_stays_a_membership_test_and_is_not_a_range(
    tmp_path: Path,
) -> None:
    fixture = _promote_to_v2(make_fixture(tmp_path))
    manifest = fixture.manifest
    definition = AuthoredDeploymentDefinition(
        definition_id="package-v2-fixture",
        distribution_release_ref=manifest.distribution_release.reference,
        bundles=tuple(bundle.reference for bundle in manifest.bundles),
    )
    selection = validate_selection(
        definition, manifest.distribution_release, manifest.bundles
    )
    lock = resolve_product_lock(
        selection, package_identities=manifest.package_identities
    )
    assert lock is not None

    ranged = cast(
        DistributionRelease,
        manifest.distribution_release.model_copy(
            update={
                "compatibility": CompatibilityMetadata(
                    contract_versions=(">=0.9,<2.0",), capabilities=("public",)
                )
            }
        ),
    )
    ranged_selection = validate_selection(definition, ranged, manifest.bundles)
    with pytest.raises(DeploymentLockError) as excinfo:
        resolve_product_lock(
            ranged_selection, package_identities=manifest.package_identities
        )
    assert any(
        item.code == "compatibility_contract_unsupported"
        for item in excinfo.value.diagnostics
    )
    # The OKG window lives on its own field and never reaches that test.
    assert manifest.okg_compatibility is not None
