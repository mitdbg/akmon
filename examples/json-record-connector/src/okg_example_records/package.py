"""Build the distribution package that carries this connector.

An installed connector publishes through OKG's packaged-install path, and
that path reads its inputs from a verified distribution package rather than
from a checkout. This module assembles one, using only the public
`okg.distributions` producer:

    python -m okg_example_records.package --output DIR

The package carries this wheel as its payload object, declares the bundle
files the installer renders a deployment from, and declares the connector
registration and ontology schema as typed assets. Nothing here is imported
at install time: the installer reads these assets as bytes.
"""

import argparse
import hashlib
import json
import tempfile
import zipfile
from importlib.resources import files
from pathlib import Path

from okg.distributions import (
    AssetLocator,
    CompatibilityMetadata,
    DistributionObject,
    DistributionPackageManifest,
    DistributionPackageProvenance,
    DistributionPackageSource,
    DistributionRelease,
    ModuleDeclaration,
    OkgCompatibility,
    OwnerQualifiedReference,
    PackagedAsset,
    ProductBundle,
    TypedAsset,
    build_distribution_package,
    canonical_bundle_digest,
    canonical_release_digest,
)

from .reader import REGISTRATION

# This example's own identity. It is deliberately neutral: a distribution
# owner is the author's namespace, never a name borrowed from OKG or from
# another consumer.
OWNER = "example-records"
BUNDLE = "example-records"
VERSION = "1.0.0"
MODULE = "records"
MODULE_ID = OWNER + ":" + MODULE
REPOSITORY = "https://example.invalid/okg-example-records"
SCHEMA_VERSION = "okg.deployment_product.distribution_package/v2"
ZERO_DIGEST = "sha256:" + "0" * 64
# Asset provenance must name a 40-hex source revision. This example is built
# from an installed wheel, not a checkout, so it has none to declare and says
# so with the neutral zero revision rather than inventing a commit id.
NO_REVISION = "0" * 40
# The OKG release window this package installs against. Kept wide because the
# example depends on the packaged-install contract, not on a point release.
OKG_MIN_VERSION = "0.1.0"
OKG_MAX_VERSION_EXCLUSIVE = "9.0.0"

# The bundle tree inside the wheel. `render_verified_profile` refuses any
# profile dependency that is not a selected asset, so every one of these is
# declared below.
BUNDLE_ROOT = "okg_example_records/bundles/" + BUNDLE

# Asset kinds are the installer's vocabulary, not free text. `ontology_schema`
# is the kind a packaged module's `schema.yaml` must carry; the bundle's own
# YAML is configuration the profile renderer reads.
PLANE_BY_KIND = {
    "connector": "ingestion-projection",
    "ontology_schema": "semantic-catalog",
    "source-template": "ingestion-projection",
    "invariant": "derivation-publication",
}


def digest(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _typed_asset(asset_id: str, payload: bytes, *, kind: str) -> TypedAsset:
    return TypedAsset(
        owner=OWNER,
        asset_id=asset_id,
        kind=kind,
        plane=PLANE_BY_KIND[kind],
        version="v1",
        digest=digest(payload),
        schema="okg.deployment_product.typed_asset/v1",
        merge_identity=asset_id,
        merge_rule="refuse",
        compatibility={"contract_versions": ["v1"], "capabilities": ["public"]},
        provenance={
            "source": "package",
            "repository_url": REPOSITORY,
            "revision": NO_REVISION,
            "artifact_ref": OWNER + ":" + asset_id,
        },
    )


def _reference(asset: TypedAsset) -> OwnerQualifiedReference:
    return OwnerQualifiedReference(
        owner=asset.owner, id=asset.asset_id, version=asset.version,
        digest=asset.digest,
    )


def _bundle_member_id(member_path: str) -> str:
    """A stable asset id for one bundle file, derived from its own path."""
    relative = member_path[len(BUNDLE_ROOT) + 1:]
    return "bundle-" + relative.replace("/", "-").replace(".", "-")


def _module_schema_bytes() -> bytes:
    """One `schema.yaml` for the packaged module.

    A deployment-native schema tree keeps edge narrowings under `bridges/`,
    but a packaged ontology module carries exactly one schema file, so the
    two are merged here rather than duplicated on disk.
    """
    import yaml

    root = files("okg_example_records").joinpath("deployment/schemas")
    schema = yaml.safe_load(root.joinpath("records.yaml").read_text(encoding="utf-8"))
    bridge = yaml.safe_load(
        root.joinpath("bridges/records_edges.yaml").read_text(encoding="utf-8"),
    )
    schema["classes"].update(bridge["classes"])
    return yaml.safe_dump(schema, sort_keys=False).encode("utf-8")


def build(wheel: Path, output: Path) -> Path:
    """Assemble the package around an already-built consumer wheel."""
    wheel_digest = digest(wheel.read_bytes())
    assets: list[PackagedAsset] = []
    objects = [
        DistributionObject(
            digest=wheel_digest,
            size=wheel.stat().st_size,
            media_type="application/zip",
        ),
    ]
    sources = {wheel_digest: DistributionPackageSource(root=wheel.parent, path=wheel)}

    # 1. The bundle files, addressed as members of the wheel object. The
    #    installer renders deployment.yaml and source_registry.yaml from
    #    exactly these bytes.
    with zipfile.ZipFile(wheel) as archive:
        members = [
            info for info in archive.infolist()
            if info.filename.startswith(BUNDLE_ROOT + "/")
        ]
        if not members:
            raise SystemExit(
                "the wheel carries no " + BUNDLE_ROOT
                + "/ tree; check that pyproject.toml ships the bundle"
                + " as package data"
            )
        for info in sorted(members, key=lambda item: item.filename):
            payload = archive.read(info.filename)
            kind = (
                "invariant" if info.filename.endswith("/invariants.yaml")
                else "source-template"
            )
            asset = _typed_asset(
                _bundle_member_id(info.filename), payload, kind=kind,
            )
            assets.append(PackagedAsset(
                asset=asset,
                locator=AssetLocator(
                    object_digest=wheel_digest,
                    member_path=info.filename,
                    member_crc32=f"{info.CRC:08x}",
                    member_compressed_size=info.compress_size,
                    compression_method=(
                        "deflated" if info.compress_type else "stored"
                    ),
                ),
                uncompressed_size=len(payload),
            ))

    # 2. The connector registration, as the installer reads it: the same
    #    inert payload `setup.py` writes, never an import of this package.
    # Resolved, because the package reader walks each path component and
    # refuses a symlinked anchor -- and the platform temp root is itself a
    # symlink on macOS.
    stage = Path(tempfile.mkdtemp(prefix="okg-example-records-package-")).resolve()
    registration_bytes = (
        json.dumps(REGISTRATION.as_asset_payload(), indent=2) + "\n"
    ).encode("utf-8")
    schema_bytes = _module_schema_bytes()
    for asset_id, payload, kind in (
        ("connector", registration_bytes, "connector"),
        ("records-schema", schema_bytes, "ontology_schema"),
    ):
        path = stage / asset_id
        path.write_bytes(payload)
        asset = _typed_asset(asset_id, payload, kind=kind)
        assets.append(PackagedAsset(
            asset=asset,
            locator=AssetLocator(object_digest=digest(payload)),
            uncompressed_size=len(payload),
        ))
        objects.append(DistributionObject(
            digest=digest(payload),
            size=len(payload),
            media_type="application/octet-stream",
        ))
        sources[digest(payload)] = DistributionPackageSource(
            root=stage, path=path,
        )

    references = tuple(_reference(item.asset) for item in assets)
    compatibility = CompatibilityMetadata(
        contract_versions=("v1",), capabilities=("public",),
    )
    provisional_bundle = ProductBundle(
        bundle_id=BUNDLE,
        version="v1",
        distribution_id=OWNER,
        distribution_version=VERSION,
        bundle_digest=ZERO_DIGEST,
        assets=references,
        compatibility=compatibility,
    )
    bundle = provisional_bundle.model_copy(
        update={"bundle_digest": canonical_bundle_digest(provisional_bundle)},
    )
    release = DistributionRelease(
        distribution_id=OWNER,
        version=VERSION,
        origin="external",
        release_digest=ZERO_DIGEST,
        assets=references,
        bundle_ids=(BUNDLE,),
        compatibility=compatibility,
    )
    schema_reference = next(
        _reference(item.asset) for item in assets
        if item.asset.asset_id == "records-schema"
    )
    provisional = DistributionPackageManifest(
        schema_version=SCHEMA_VERSION,
        distribution_release=release,
        bundles=(bundle,),
        assets=tuple(assets),
        objects=tuple(objects),
        provenance=DistributionPackageProvenance(
            repository_url=REPOSITORY,
            revision=NO_REVISION,
            builder="okg-example-records",
        ),
        # Two different windows, easily confused. `okg_compatibility` is the
        # OKG *release* this package will install against (`okg.__version__`).
        # The connector's `sdk_range` is the *SDK* window
        # (`okg.deployment.__version__`, 1.0.0) and is checked separately when
        # the connector asset is resolved. Declaring the SDK window here
        # refuses every real OKG build.
        okg_compatibility=OkgCompatibility(
            min_version=OKG_MIN_VERSION,
            max_version_exclusive=OKG_MAX_VERSION_EXCLUSIVE,
        ),
        module_declarations=(
            ModuleDeclaration(module_id=MODULE_ID, asset_refs=(schema_reference,)),
        ),
    )
    manifest = provisional.model_copy(update={
        "distribution_release": release.model_copy(
            update={"release_digest": canonical_release_digest(provisional)},
        ),
    })
    return build_distribution_package(manifest, sources, output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheel", type=Path, required=True,
                        help="this package's built wheel")
    parser.add_argument("--output", type=Path, required=True,
                        help="existing directory to write the package into")
    args = parser.parse_args()
    print(build(args.wheel.resolve(), args.output.resolve()))


if __name__ == "__main__":
    main()
