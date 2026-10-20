"""Hermetic conformance tests for the v1 immutable distribution package.

These tests deliberately create all inputs in temporary directories.  The
fixture resembles the Archi v3 wheel/bundle split, but the package API is
exercised with arbitrary distribution names as well.  No test imports a
packaged module or needs a database, network, catalog, or lifecycle runtime.
"""

from __future__ import annotations

import ast
import base64
import builtins
from dataclasses import dataclass
import hashlib
import inspect
import io
import json
from pathlib import Path
import re
import stat
import struct
import tarfile
from typing import Any, cast
import zipfile

import pytest

from okg.substrate.contracts.deployment_product import (
    AuthoredDeploymentDefinition,
    CompatibilityMetadata,
    DistributionRelease,
    OwnerQualifiedReference,
    ProductBundle,
    TypedArtifactIdentity,
    TypedAsset,
    canonical_lock_bytes,
    resolve_product_lock,
    validate_selection,
)
from okg.substrate.distributions import (
    AssetLocator,
    DistributionObject,
    DistributionPackageError,
    DistributionPackageManifest,
    DistributionPackageProvenance,
    DistributionPackageSource,
    PackagedAsset,
    PublicExtensionMetadata,
    UnresolvedPlaceholder,
    VerifiedDistributionPackage,
    build_distribution_package,
    canonical_bundle_digest,
    canonical_release_digest,
    verify_distribution_package,
)
from okg.substrate.distributions import package as package_impl


_ARCHI_REVISION = "728739e64763aa897b2f264259a3d9a6c46e9bd8"
_ZERO_DIGEST = "sha256:" + "0" * 64
_ASSET_SCHEMA = "okg.deployment_product.typed_asset/v1"
_RESOURCE_LIMITS = {
    "manifest_bytes": 4 * 1024 * 1024,
    "archive_bytes": 512 * 1024 * 1024,
    "archive_members": 4097,
    "objects": 4096,
    "object_bytes_each": 64 * 1024 * 1024,
    "object_bytes_total": 512 * 1024 * 1024,
    "assets": 8192,
    "selected_asset_bytes_each": 32 * 1024 * 1024,
    "selected_asset_bytes_total": 512 * 1024 * 1024,
    "bundles": 256,
    "package_identities": 512,
    "public_extensions": 1024,
    "placeholders": 4096,
    "wheel_entries": 8192,
    "wheel_central_directory_bytes": 8 * 1024 * 1024,
    "wheel_member_expanded_bytes": 32 * 1024 * 1024,
    "wheel_expanded_bytes_total": 512 * 1024 * 1024,
    "wheel_selected_expansion_ratio": 100,
    "wheel_expansion_additive_bytes_each": 64 * 1024,
    "wheel_expansion_additive_bytes_total": 1024 * 1024,
    "source_symlink_hops": 16,
    "stream_chunk_bytes": 1024 * 1024,
}


def _digest(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def _error_code(exc: BaseException) -> str:
    """Read the stable code without coupling tests to error formatting."""

    code = getattr(exc, "code", None)
    if code:
        return str(code)
    if getattr(exc, "args", None) and isinstance(exc.args[0], dict):
        return str(exc.args[0].get("code", ""))
    return ""


def _expect_refusal(call: Any, *codes: str) -> DistributionPackageError:
    with pytest.raises(DistributionPackageError) as caught:
        call()
    assert _error_code(caught.value) in codes, str(caught.value)
    return caught.value


def _safe_reference(
    owner: str, asset_id: str, *, kind: str, digest: str
) -> OwnerQualifiedReference:
    return OwnerQualifiedReference(
        owner=owner, id=asset_id, kind=kind, version="v1", digest=digest
    )


def _asset(
    owner: str,
    asset_id: str,
    payload: bytes,
    *,
    kind: str = "schema",
    source: str = "package",
) -> TypedAsset:
    return TypedAsset(
        owner=owner,
        asset_id=asset_id,
        kind=kind,
        plane="semantic-catalog",
        version="v1",
        digest=_digest(payload),
        schema=_ASSET_SCHEMA,
        merge_identity=asset_id,
        merge_rule="refuse",
        compatibility={"contract_versions": ["v1"], "capabilities": ["public"]},
        provenance={
            "source": source,
            "repository_url": "https://github.com/example/distribution",
            "revision": _ARCHI_REVISION,
            "artifact_ref": f"{owner}:{asset_id}",
        },
    )


def _asset_ref(asset: TypedAsset) -> OwnerQualifiedReference:
    return OwnerQualifiedReference(
        owner=asset.owner,
        id=asset.asset_id,
        kind=asset.kind,
        version=asset.version,
        digest=asset.digest,
    )


def _artifact(owner: str, ref: str, kind: str, payload: bytes) -> TypedArtifactIdentity:
    return TypedArtifactIdentity(
        owner=owner, id=ref, kind=kind, version="v1", digest=_digest(payload)
    )


def _wheel_bytes(
    *,
    extra_member: str | None = None,
    extra_members: int = 0,
    extra_member_bytes: int = 0,
    duplicate_alias: bool = False,
    invalid_member: str | None = None,
    record_mutation: bool = False,
) -> bytes:
    """Create a small, RECORD-complete wheel with Archi-shaped members."""

    members: dict[str, bytes] = {
        "archi/__init__.py": b"# inert fixture\n",
        "archi/schemas/operations-schema.yaml": b"schema: operations\n",
        "archi/schemas/operations-bridge.yaml": (
            b"bridge:\n  - operation: endpoint-complete\n  - unsupported: manual-prune\n"
        ),
        "archi/sources/monit.py": b"def inert():\n    return 1\n",
        "archi_fixture-3.0.0a1.dist-info/WHEEL": (
            b"Wheel-Version: 1.0\nGenerator: hermetic-test\nRoot-Is-Purelib: true\nTag: py3-none-any\n"
        ),
        "archi_fixture-3.0.0a1.dist-info/METADATA": b"Metadata-Version: 2.1\nName: archi-fixture\nVersion: 3.0.0a1\n",
    }
    if extra_member:
        members[extra_member] = (
            (b"x" * extra_member_bytes)
            if extra_member_bytes
            else b"extra fixture bytes\n"
        )
    for index in range(extra_members):
        members[f"archi/extra/{index:05d}.txt"] = b"x"
    rows: list[tuple[str, str, int]] = []
    for name, payload in members.items():
        digest = (
            base64.urlsafe_b64encode(hashlib.sha256(payload).digest())
            .rstrip(b"=")
            .decode()
        )
        rows.append((name, f"sha256={digest}", len(payload)))
    record = "\n".join(f"{name},{digest},{size}" for name, digest, size in rows)
    record_name = "archi_fixture-3.0.0a1.dist-info/RECORD"
    if record_mutation:
        record = record.replace("sha256=", "sha256=AAAA", 1)
    members[record_name] = (record + f"\n{record_name},,\n").encode()
    if duplicate_alias:
        members["archi/./schemas/operations-schema.yaml"] = members[
            "archi/schemas/operations-schema.yaml"
        ]
    result = io.BytesIO()
    with zipfile.ZipFile(
        result, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9
    ) as wheel:
        for name, payload in members.items():
            info = zipfile.ZipInfo(name)
            info.create_system = 3
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = (0o100644 & 0xFFFF) << 16
            if (
                invalid_member == "directory"
                and name == "archi/schemas/operations-bridge.yaml"
            ):
                info.filename = "archi/schemas/invalid/"
                info.external_attr = (stat.S_IFDIR | 0o755) << 16
            elif (
                invalid_member == "symlink"
                and name == "archi/schemas/operations-bridge.yaml"
            ):
                info.external_attr = (stat.S_IFLNK | 0o777) << 16
                payload = b"../../outside"
            elif (
                invalid_member == "encrypted"
                and name == "archi/schemas/operations-bridge.yaml"
            ):
                info.flag_bits |= 0x1
            wheel.writestr(info, payload)
    payload = bytearray(result.getvalue())
    if invalid_member == "encrypted":
        target = b"archi/schemas/operations-bridge.yaml"
        for signature, flag_offset, name_offset, fixed_size in (
            (b"PK\x03\x04", 6, 26, 30),
            (b"PK\x01\x02", 8, 28, 46),
        ):
            cursor = 0
            while True:
                start = payload.find(signature, cursor)
                if start < 0:
                    break
                name_length = struct.unpack_from("<H", payload, start + name_offset)[0]
                extra_offset = start + fixed_size
                name = bytes(payload[extra_offset : extra_offset + name_length])
                if name == target:
                    flags = struct.unpack_from("<H", payload, start + flag_offset)[0]
                    struct.pack_into("<H", payload, start + flag_offset, flags | 0x1)
                cursor = start + 4
    elif invalid_member in {"local-crc", "unsupported-compression"}:
        target = b"archi/schemas/operations-schema.yaml"
        for signature, method_offset, crc_offset, name_offset, fixed_size in (
            (b"PK\x03\x04", 8, 14, 26, 30),
            (b"PK\x01\x02", 10, 16, 28, 46),
        ):
            cursor = 0
            while True:
                start = payload.find(signature, cursor)
                if start < 0:
                    break
                name_length = struct.unpack_from("<H", payload, start + name_offset)[0]
                name_start = start + fixed_size
                name = bytes(payload[name_start : name_start + name_length])
                if name == target:
                    if invalid_member == "local-crc" and signature == b"PK\x03\x04":
                        crc = struct.unpack_from("<I", payload, start + crc_offset)[0]
                        struct.pack_into("<I", payload, start + crc_offset, crc ^ 1)
                    elif invalid_member == "unsupported-compression":
                        struct.pack_into("<H", payload, start + method_offset, 99)
                cursor = start + 4
    elif invalid_member == "corrupt-deflate":
        target = b"archi/schemas/operations-schema.yaml"
        cursor = 0
        while True:
            start = payload.find(b"PK\x03\x04", cursor)
            assert start >= 0
            name_length, extra_length = struct.unpack_from("<2H", payload, start + 26)
            name_start = start + 30
            name = bytes(payload[name_start : name_start + name_length])
            if name == target:
                data_start = name_start + name_length + extra_length
                payload[data_start] ^= 0xFF
                break
            cursor = start + 4
    elif invalid_member == "trailing-deflate":
        target = record_name.encode()
        cursor = 0
        while True:
            local = payload.find(b"PK\x03\x04", cursor)
            assert local >= 0
            name_length, extra_length = struct.unpack_from("<2H", payload, local + 26)
            name_start = local + 30
            name = bytes(payload[name_start : name_start + name_length])
            if name == target:
                compressed_size = struct.unpack_from("<I", payload, local + 18)[0]
                struct.pack_into("<I", payload, local + 18, compressed_size + 1)
                insert_at = name_start + name_length + extra_length + compressed_size
                payload[insert_at:insert_at] = b"\0"
                break
            cursor = local + 4
        cursor = insert_at + 1
        while True:
            central = payload.find(b"PK\x01\x02", cursor)
            assert central >= 0
            name_length = struct.unpack_from("<H", payload, central + 28)[0]
            name = bytes(payload[central + 46 : central + 46 + name_length])
            if name == target:
                central_compressed = struct.unpack_from("<I", payload, central + 20)[0]
                struct.pack_into("<I", payload, central + 20, central_compressed + 1)
                break
            cursor = central + 4
        eocd = payload.rfind(b"PK\x05\x06")
        assert eocd >= 0
        central_offset = struct.unpack_from("<I", payload, eocd + 16)[0]
        struct.pack_into("<I", payload, eocd + 16, central_offset + 1)
    return bytes(payload)


def _write_wheel(path: Path, **kwargs: Any) -> bytes:
    payload = _wheel_bytes(**kwargs)
    path.write_bytes(payload)
    return payload


def _source(root: Path, path: Path) -> DistributionPackageSource:
    return DistributionPackageSource(root=root, path=path)


def _new_bundle(
    bundle_id: str,
    distribution: str,
    version: str,
    refs: tuple[OwnerQualifiedReference, ...],
) -> ProductBundle:
    provisional = ProductBundle(
        bundle_id=bundle_id,
        version="v1",
        distribution_id=distribution,
        distribution_version=version,
        bundle_digest=_ZERO_DIGEST,
        assets=refs,
        compatibility=CompatibilityMetadata(
            contract_versions=("v1",), capabilities=("public",)
        ),
    )
    return cast(
        ProductBundle,
        provisional.model_copy(
            update={"bundle_digest": canonical_bundle_digest(provisional)}
        ),
    )


@dataclass
class Fixture:
    root: Path
    output: Path
    wheel: Path
    tree: Path
    wheel_bytes: bytes
    assets: tuple[TypedAsset, ...]
    manifest: DistributionPackageManifest
    sources: dict[str, DistributionPackageSource]


def make_fixture(
    tmp_path: Path,
    *,
    distribution: str = "archi",
    bundle_name: str = "cern-team",
    wheel_kwargs: dict[str, Any] | None = None,
) -> Fixture:
    tmp_path.mkdir(parents=True, exist_ok=True)
    root = tmp_path / "authoring-checkout"
    root.mkdir()
    output = tmp_path / "packages"
    output.mkdir()
    wheel = root / "python" / "dist" / "archi_fixture-3.0.0a1-py3-none-any.whl"
    wheel.parent.mkdir(parents=True)
    wheel_bytes = _write_wheel(wheel, **(wheel_kwargs or {}))
    tree = root / "bundles" / bundle_name
    playbooks = tree / "playbooks"
    playbooks.mkdir(parents=True)
    (tree / "profile.yaml").write_text(
        f"name: {bundle_name}\nversion: v1\nrefs:\n  - {distribution}:operations-schema\n",
        encoding="utf-8",
    )
    (tree / "README.md").write_text("hermetic bundle\n", encoding="utf-8")
    target = root / "skills" / "playbook.md"
    target.parent.mkdir(parents=True)
    target.write_text("public playbook bytes\n", encoding="utf-8")
    (playbooks / "operations.md").symlink_to(Path("../../../skills/playbook.md"))
    selected_bridge = tree / "selected-operations-bridge.yaml"
    selected_bridge.write_bytes(b"bridge:\n  - operation: endpoint-complete\n")

    wheel_asset = _asset(distribution, "wheel", wheel_bytes, kind="package")
    schema_payload = b"schema: operations\n"
    schema_asset = _asset(distribution, "operations-schema", schema_payload)
    bridge_payload = selected_bridge.read_bytes()
    bridge_asset = _asset(
        distribution, "operations-bridge", bridge_payload, kind="bridge"
    )
    profile_payload = (tree / "profile.yaml").read_bytes()
    profile_asset = _asset(bundle_name, "profile", profile_payload, kind="bundle")
    playbook_payload = target.read_bytes()
    playbook_asset = _asset(
        bundle_name, "operations-playbook", playbook_payload, kind="playbook"
    )
    assets = (wheel_asset, schema_asset, bridge_asset, profile_asset, playbook_asset)
    refs = tuple(_asset_ref(item) for item in assets)
    bundle = _new_bundle(bundle_name, distribution, "3.0.0a1", refs)
    fixture_bundle = _new_bundle(
        "fixture-bundle",
        distribution,
        "3.0.0a1",
        refs,
    )
    release_provisional = DistributionRelease(
        distribution_id=distribution,
        version="3.0.0a1",
        origin="external",
        release_digest=_ZERO_DIGEST,
        assets=refs,
        bundle_ids=(bundle.bundle_id, fixture_bundle.bundle_id),
        compatibility=CompatibilityMetadata(
            contract_versions=("v1",), capabilities=("public",)
        ),
    )
    objects = tuple(
        DistributionObject(
            digest=_digest(payload),
            size=len(payload),
            media_type="application/octet-stream",
        )
        for payload in (wheel_bytes, bridge_payload, profile_payload, playbook_payload)
    )
    with zipfile.ZipFile(io.BytesIO(wheel_bytes)) as wheel_archive:
        schema_info = wheel_archive.getinfo("archi/schemas/operations-schema.yaml")
        schema_locator = AssetLocator(
            object_digest=_digest(wheel_bytes),
            member_path=schema_info.filename,
            member_crc32=f"{schema_info.CRC:08x}",
            member_compressed_size=schema_info.compress_size,
            compression_method="deflated",
        )
    packaged_assets = (
        PackagedAsset(
            asset=wheel_asset,
            locator=AssetLocator(object_digest=_digest(wheel_bytes)),
            uncompressed_size=len(wheel_bytes),
        ),
        PackagedAsset(
            asset=schema_asset,
            locator=schema_locator,
            uncompressed_size=len(schema_payload),
        ),
        PackagedAsset(
            asset=bridge_asset,
            locator=AssetLocator(object_digest=_digest(bridge_payload)),
            uncompressed_size=len(bridge_payload),
        ),
        PackagedAsset(
            asset=profile_asset,
            locator=AssetLocator(object_digest=_digest(profile_payload)),
            uncompressed_size=len(profile_payload),
        ),
        PackagedAsset(
            asset=playbook_asset,
            locator=AssetLocator(object_digest=_digest(playbook_payload)),
            uncompressed_size=len(playbook_payload),
        ),
    )
    identities = (_artifact(distribution, "wheel", "wheel", wheel_bytes),)
    provenance = DistributionPackageProvenance(
        repository_url="https://github.com/archi-physics/archi",
        revision=_ARCHI_REVISION,
        builder="okg-hermetic-test",
    )
    manifest_provisional = DistributionPackageManifest(
        distribution_release=release_provisional,
        bundles=(bundle, fixture_bundle),
        assets=packaged_assets,
        objects=objects,
        package_identities=identities,
        public_extensions=(),
        provenance=provenance,
    )
    release = cast(
        DistributionRelease,
        release_provisional.model_copy(
            update={"release_digest": canonical_release_digest(manifest_provisional)}
        ),
    )
    manifest = cast(
        DistributionPackageManifest,
        manifest_provisional.model_copy(update={"distribution_release": release}),
    )
    sources = {
        _digest(wheel_bytes): _source(root, wheel),
        _digest(bridge_payload): _source(root, selected_bridge),
        _digest(profile_payload): _source(root, tree / "profile.yaml"),
        _digest(playbook_payload): _source(root, playbooks / "operations.md"),
    }
    return Fixture(root, output, wheel, tree, wheel_bytes, assets, manifest, sources)


def _build(fixture: Fixture) -> Path:
    return build_distribution_package(fixture.manifest, fixture.sources, fixture.output)


def _nested_keys(value: object) -> set[str]:
    if isinstance(value, dict):
        return set(value).union(*(_nested_keys(item) for item in value.values()), set())
    if isinstance(value, (list, tuple)):
        return set().union(*(_nested_keys(item) for item in value), set())
    return set()


def _with_assets(fixture: Fixture, assets: tuple[PackagedAsset, ...]) -> Fixture:
    """Recompute the descriptor identities after a deliberate asset mutation."""

    refs = tuple(_asset_ref(item.asset) for item in assets)
    bundles: list[ProductBundle] = []
    for existing in fixture.manifest.bundles:
        bundle_base = cast(
            ProductBundle,
            existing.model_copy(
                update={"asset_refs": refs, "bundle_digest": _ZERO_DIGEST}
            ),
        )
        bundles.append(
            cast(
                ProductBundle,
                bundle_base.model_copy(
                    update={"bundle_digest": canonical_bundle_digest(bundle_base)}
                ),
            )
        )
    release_base = cast(
        DistributionRelease,
        fixture.manifest.distribution_release.model_copy(
            update={
                "asset_refs": refs,
                "bundle_ids": tuple(bundle.bundle_id for bundle in bundles),
                "release_digest": _ZERO_DIGEST,
            }
        ),
    )
    manifest_base = cast(
        DistributionPackageManifest,
        fixture.manifest.model_copy(
            update={
                "distribution_release": release_base,
                "bundles": tuple(bundles),
                "assets": assets,
            }
        ),
    )
    release = cast(
        DistributionRelease,
        release_base.model_copy(
            update={"release_digest": canonical_release_digest(manifest_base)}
        ),
    )
    manifest = cast(
        DistributionPackageManifest,
        manifest_base.model_copy(update={"distribution_release": release}),
    )
    return Fixture(
        fixture.root,
        fixture.output,
        fixture.wheel,
        fixture.tree,
        fixture.wheel_bytes,
        fixture.assets,
        manifest,
        fixture.sources,
    )


def _with_unchecked_asset(fixture: Fixture, index: int, asset: TypedAsset) -> Fixture:
    """Create a malformed serialized-shape mutant without validator short-circuiting."""

    original = fixture.manifest.assets[index]
    packaged = PackagedAsset.model_construct(
        asset=asset,
        locator=original.locator,
        uncompressed_size=original.uncompressed_size,
    )
    assets = list(fixture.manifest.assets)
    assets[index] = packaged
    fields = {
        name: getattr(fixture.manifest, name)
        for name in type(fixture.manifest).model_fields
    }
    fields["assets"] = tuple(assets)
    manifest = DistributionPackageManifest.model_construct(**fields)
    return Fixture(
        fixture.root,
        fixture.output,
        fixture.wheel,
        fixture.tree,
        fixture.wheel_bytes,
        fixture.assets,
        manifest,
        fixture.sources,
    )


def _archive_members(path: Path) -> dict[str, bytes]:
    with tarfile.open(path, "r:") as archive:
        result: dict[str, bytes] = {}
        for member in archive.getmembers():
            if not member.isfile():
                continue
            extracted = archive.extractfile(member)
            assert extracted is not None
            result[member.name] = extracted.read()
        return result


def _rewrite_tar(path: Path, destination: Path, mutate: Any) -> None:
    members = _archive_members(path)
    members = mutate(members)
    with tarfile.open(destination, "w", format=tarfile.USTAR_FORMAT) as archive:
        for name in sorted(members):
            info = tarfile.TarInfo(name)
            info.size = len(members[name])
            info.mode = 0o444
            info.uid = info.gid = info.mtime = 0
            archive.addfile(info, io.BytesIO(members[name]))


def _tar_from_members(destination: Path, members: dict[str, bytes]) -> Path:
    with tarfile.open(destination, "w", format=tarfile.USTAR_FORMAT) as archive:
        for name in sorted(members):
            info = tarfile.TarInfo(name)
            info.size = len(members[name])
            info.mode = 0o444
            info.uid = info.gid = info.mtime = 0
            archive.addfile(info, io.BytesIO(members[name]))
    return destination


def _bind_transport_payload(directory: Path, payload: bytes) -> Path:
    path = directory / f"{hashlib.sha256(payload).hexdigest()}.okgdist.tar"
    path.write_bytes(payload)
    return path


def _mutate_ustar_header(payload: bytes, *, offset: int, value: bytes) -> bytes:
    mutant = bytearray(payload)
    mutant[offset : offset + len(value)] = value
    mutant[148:156] = b"        "
    mutant[148:156] = f"{sum(mutant[:512]):06o}\0 ".encode("ascii")
    return bytes(mutant)


@pytest.fixture
def fixture(tmp_path: Path) -> Fixture:
    return make_fixture(tmp_path)


def test_manifest_roundtrip_and_named_public_api(fixture: Fixture) -> None:
    package_path = _build(fixture)
    checked = verify_distribution_package(package_path)
    assert isinstance(checked, VerifiedDistributionPackage)
    assert checked.manifest.distribution_release.distribution_id == "archi"
    assert checked.read_asset("archi:operations-schema") == b"schema: operations\n"
    assert package_path.parent == fixture.output
    assert package_path.name.endswith(".okgdist.tar")


def test_extension_runtime_authority_is_path_free_and_digest_bound(
    fixture: Fixture,
) -> None:
    checked = verify_distribution_package(_build(fixture))

    with checked.extension_runtime_authority() as authority:
        preimage = authority.receipt.preimage()
        record = authority.to_record()
        assert preimage == {
            "schema_version": "okg.extension_runtime.package_verification_receipt/v1",
            "distribution_package_schema": package_impl.SCHEMA,
            "manifest": authority.receipt.manifest_raw_blob,
            "transport_archive": authority.receipt.transport_archive_raw_blob,
            "verified_object_index_digest": {
                "digest_domain": "canonical-json/v1",
                "schema_version": "okg.extension_runtime.verified_object_index/v1",
                "digest": authority.receipt.verified_object_index_digest,
            },
            "verified_asset_index_digest": {
                "digest_domain": "canonical-json/v1",
                "schema_version": "okg.extension_runtime.verified_asset_index/v1",
                "digest": authority.receipt.verified_asset_index_digest,
            },
            "package_identity_index_digest": {
                "digest_domain": "canonical-json/v1",
                "schema_version": "okg.extension_runtime.package_identity_index/v1",
                "digest": authority.receipt.package_identity_index_digest,
            },
            "public_extension_index_digest": {
                "digest_domain": "canonical-json/v1",
                "schema_version": "okg.extension_runtime.public_extension_index/v1",
                "digest": authority.receipt.public_extension_index_digest,
            },
            "verification_profile_digest": {
                "digest_domain": "canonical-json/v1",
                "schema_version": "okg.distribution_package.verification_profile/v1",
                "digest": authority.receipt.verification_profile_digest,
            },
        }
        assert authority.receipt.digest == package_impl._canonical_digest(preimage)
        assert record["package_verification_receipt_digest"] == {
            "digest_domain": "canonical-json/v1",
            "schema_version": "okg.extension_runtime.package_verification_receipt/v1",
            "digest": authority.receipt.digest,
        }
        assert not _nested_keys(record).intersection(
            {
                "archive_path",
                "device",
                "fd",
                "inode",
                "offset",
                "path",
                "runtime_locator",
            }
        )
        assert authority.read_verified_object(_digest(fixture.wheel_bytes)) == (
            fixture.wheel_bytes
        )
        with pytest.raises(AttributeError):
            authority.receipt = authority.receipt  # type: ignore[misc]


def test_extension_runtime_authority_never_reopens_the_package_path(
    fixture: Fixture,
) -> None:
    package_path = _build(fixture)
    checked = verify_distribution_package(package_path)
    authority = checked.extension_runtime_authority()
    object.__setattr__(checked, "path", fixture.root / "not-the-package-path")
    assert authority.read_verified_object(_digest(fixture.wheel_bytes)) == (
        fixture.wheel_bytes
    )

    moved = package_path.with_name("verified-package-moved-aside.okgdist.tar")
    package_path.rename(moved)
    package_path.write_bytes(b"unverified replacement")
    with pytest.raises(DistributionPackageError) as exc_info:
        authority.read_verified_object(_digest(fixture.wheel_bytes))
    assert _error_code(exc_info.value) == "extension_package_object_mismatch"
    authority.close()
    authority.close()
    with pytest.raises(DistributionPackageError) as exc_info:
        authority.read_verified_object(_digest(fixture.wheel_bytes))
    assert _error_code(exc_info.value) == "extension_package_object_mismatch"

    checked.close_runtime_authority()
    checked.close_runtime_authority()
    with pytest.raises(DistributionPackageError) as exc_info:
        checked.extension_runtime_authority()
    assert _error_code(exc_info.value) == "extension_package_object_mismatch"


def test_release_and_transport_digest_preimages_are_reproducible(
    fixture: Fixture,
) -> None:
    first = _build(fixture)
    second_output = fixture.root.parent / "packages-two"
    second_output.mkdir()
    second = build_distribution_package(
        fixture.manifest, fixture.sources, second_output
    )
    assert first.read_bytes() == second.read_bytes()
    assert first.name == second.name
    assert (
        first.name.removesuffix(".okgdist.tar")
        == hashlib.sha256(first.read_bytes()).hexdigest()
    )


def test_bundle_digest_binds_descriptor(fixture: Fixture) -> None:
    bundle = fixture.manifest.bundles[0]
    changed = cast(
        ProductBundle, bundle.model_copy(update={"bundle_id": "different-bundle"})
    )
    assert canonical_bundle_digest(changed) != canonical_bundle_digest(bundle)
    assert bundle.bundle_digest == canonical_bundle_digest(bundle)


def test_shared_objects_are_deduplicated(fixture: Fixture) -> None:
    package = verify_distribution_package(_build(fixture))
    members = _archive_members(package.path)
    object_names = sorted(
        name for name in members if name.startswith("objects/sha256/")
    )
    assert len(package.manifest.bundles) == 2
    assert (
        package.manifest.bundles[0].asset_refs == package.manifest.bundles[1].asset_refs
    )
    assert len(object_names) == len({members[name] for name in object_names})
    assert len(object_names) == len(package.manifest.objects)
    assert len(object_names) < len(fixture.manifest.assets)


def test_wheel_member_locator_verifies_exact_bytes(fixture: Fixture) -> None:
    wheel_bytes = fixture.wheel_bytes
    digest = _digest(wheel_bytes)
    with zipfile.ZipFile(io.BytesIO(wheel_bytes)) as wheel:
        info = wheel.getinfo("archi/schemas/operations-schema.yaml")
        locator = AssetLocator(
            object_digest=digest,
            member_path=info.filename,
            member_crc32=f"{info.CRC:08x}",
            member_compressed_size=info.compress_size,
            compression_method="deflated",
        )
    packaged = cast(
        PackagedAsset,
        fixture.manifest.assets[1].model_copy(update={"locator": locator}),
    )
    manifest = cast(
        DistributionPackageManifest,
        fixture.manifest.model_copy(
            update={
                "assets": (
                    fixture.manifest.assets[0],
                    packaged,
                    *fixture.manifest.assets[2:],
                )
            }
        ),
    )
    local_fixture = Fixture(
        fixture.root,
        fixture.output,
        fixture.wheel,
        fixture.tree,
        fixture.wheel_bytes,
        fixture.assets,
        manifest,
        {**fixture.sources, _digest(wheel_bytes): _source(fixture.root, fixture.wheel)},
    )
    package = verify_distribution_package(_build(local_fixture))
    assert package.read_asset(fixture.assets[1].qualified_id) == b"schema: operations\n"
    assert (
        package.read_asset("archi:operations-bridge")
        == b"bridge:\n  - operation: endpoint-complete\n"
    )
    with zipfile.ZipFile(io.BytesIO(fixture.wheel_bytes)) as wheel:
        assert b"unsupported: manual-prune" in wheel.read(
            "archi/schemas/operations-bridge.yaml"
        )
    assert all(
        item.locator.member_path != "archi/schemas/operations-bridge.yaml"
        for item in package.manifest.assets
    )


def test_contained_symlinks_materialize_as_regular_files(fixture: Fixture) -> None:
    package = verify_distribution_package(_build(fixture))
    destination = fixture.root.parent / "materialized"
    package.materialize(destination)
    files = list(destination.rglob("*"))
    assert files
    assert all(not path.is_symlink() for path in files)
    playbook = next(
        item
        for item in package.manifest.assets
        if item.asset.qualified_id == "cern-team:operations-playbook"
    )
    materialized = (
        destination / "objects" / "sha256" / playbook.locator.object_digest[7:]
    )
    assert materialized.is_file()
    assert materialized.read_bytes() == b"public playbook bytes\n"


def test_unsafe_paths_links_and_special_files_refuse_before_destination(
    fixture: Fixture,
) -> None:
    package = _build(fixture)
    destination = fixture.root.parent / "unsafe.tar"
    _rewrite_tar(package, destination, lambda members: {**members, "../escape": b"no"})
    rebound = _bind_transport_payload(fixture.root.parent, destination.read_bytes())
    _expect_refusal(
        lambda: verify_distribution_package(rebound),
        "archive_noncanonical",
    )
    for typeflag in (b"1", b"2", b"3", b"4", b"6"):
        link_mutant = _bind_transport_payload(
            fixture.root.parent,
            _mutate_ustar_header(package.read_bytes(), offset=156, value=typeflag),
        )
        _expect_refusal(
            lambda link_mutant=link_mutant: verify_distribution_package(link_mutant),
            "archive_noncanonical",
        )
    materialized = fixture.root.parent / "must-not-exist"
    assert not materialized.exists()


def test_manifest_metadata_allowlist_refuses_nested_runtime_values(
    fixture: Fixture,
) -> None:
    unsafe_asset = TypedAsset.from_value(
        {**fixture.assets[0].to_dict(), "provenance": {"nested": {"password": "x"}}}
    )
    mutant = _with_unchecked_asset(fixture, 0, unsafe_asset)
    _expect_refusal(lambda: _build(mutant), "metadata_forbidden")


def test_builder_accepts_non_archi_distribution(tmp_path: Path) -> None:
    fixture = make_fixture(
        tmp_path, distribution="research-suite", bundle_name="research-bundle"
    )
    package = verify_distribution_package(_build(fixture))
    assert package.manifest.distribution_release.distribution_id == "research-suite"


def test_checkout_removal_and_relocation_preserve_lock_bytes(tmp_path: Path) -> None:
    """A moved package resolves to the same lock after its checkout is gone."""
    fixture = make_fixture(
        tmp_path, distribution="research-suite", bundle_name="research-bundle"
    )
    definition = AuthoredDeploymentDefinition(
        definition_id="relocation-fixture",
        distribution_release_ref=fixture.manifest.distribution_release.reference,
        bundles=tuple(bundle.reference for bundle in fixture.manifest.bundles),
    )
    first = _build(fixture)
    selection_a = validate_selection(
        definition,
        fixture.manifest.distribution_release,
        fixture.manifest.bundles,
    )
    lock_a = canonical_lock_bytes(
        resolve_product_lock(
            selection_a,
            package_identities=fixture.manifest.package_identities,
        )
    )
    relocated = tmp_path / "relocated"
    relocated.mkdir()
    second = relocated / first.name
    second.write_bytes(first.read_bytes())
    fixture.root.rename(tmp_path / "authoring-checkout-removed")
    assert not fixture.root.exists()
    checked = verify_distribution_package(second)
    assert {bundle.bundle_id for bundle in checked.manifest.bundles} == {
        "research-bundle",
        "fixture-bundle",
    }
    assert (
        checked.manifest.bundles[0].asset_refs == checked.manifest.bundles[1].asset_refs
    )
    selection_b = validate_selection(
        definition,
        checked.manifest.distribution_release,
        checked.manifest.bundles,
    )
    lock_b = canonical_lock_bytes(
        resolve_product_lock(
            selection_b,
            package_identities=checked.manifest.package_identities,
        )
    )
    assert lock_a == lock_b
    assert str(tmp_path).encode() not in lock_a
    assert b"password" not in lock_a and b"secret" not in lock_a
    assert first.read_bytes() == second.read_bytes()


_CONSUMER_TOKEN = re.compile(
    r"(?<![a-z0-9])(archi|a2rchi|cern[-_]?team|enterpriserag)(?![a-z0-9])"
)


def _consumer_token(value: str) -> bool:
    """Match whole words, including CamelCase parts such as ``adaptArchiSource``."""
    return bool(_CONSUMER_TOKEN.search(re.sub(r"(?<=[a-z])(?=[A-Z])", "_", value).lower()))


def _runtime_tokens(path: Path):
    """Yield identifiers and non-docstring string and bytes values.

    Comments are not code. Docstrings may describe history.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    docstrings = {
        id(node.body[0].value)
        for node in ast.walk(tree)
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
        and node.body
        and isinstance(node.body[0], ast.Expr)
        and isinstance(node.body[0].value, ast.Constant)
        and isinstance(node.body[0].value.value, str)
    }
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, (str, bytes)):
            if id(node) not in docstrings:
                value = node.value
                yield node.lineno, value if isinstance(value, str) else value.decode("latin-1")
        elif isinstance(node, ast.Name):
            yield node.lineno, node.id
        elif isinstance(node, ast.Attribute):
            yield node.lineno, node.attr
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            yield node.lineno, node.name
        elif isinstance(node, ast.arg):
            yield node.lineno, node.arg
        elif isinstance(node, ast.keyword) and node.arg:
            yield node.lineno, node.arg
        elif isinstance(node, ast.ImportFrom) and node.module:
            yield node.lineno, node.module
        elif isinstance(node, ast.alias):
            yield getattr(node, "lineno", 0), node.name


def test_install_runtime_has_no_consumer_identity() -> None:
    """Installation, adapter construction and chat never branch on a named consumer.

    A consumer's profile, pins, translation, adapters and protocol helpers live
    in its own package. Moving such a name into another of these framework
    modules reds here.
    """
    src = Path(__file__).parents[3] / "src" / "okg"
    paths = [
        *sorted((src / "substrate" / "distributions").rglob("*.py")),
        *sorted((src / "distributions").rglob("*.py")),
        *sorted((src / "substrate" / "chat").rglob("*.py")),
        src / "substrate" / "ingest" / "adapter_factory.py",
        src / "substrate" / "ingest" / "source_construction.py",
        src / "substrate" / "ingest" / "service.py",
        src / "substrate" / "cli" / "main.py",
    ]
    assert len(paths) > 20 and all(path.is_file() for path in paths)
    found = sorted({
        f"{path.relative_to(src)}:{line}: {value!r}"
        for path in paths
        for line, value in _runtime_tokens(path)
        if _consumer_token(value)
    })
    assert found == []


def test_consumer_identity_scan_sees_strings_and_identifiers(tmp_path: Path) -> None:
    probe = tmp_path / "probe.py"
    probe.write_text(
        '"""Mentions Archi in a docstring, which is allowed."""\n'
        "ARCHI_WHEEL = 'sha256:x'\n"
        "def run(profile):\n"
        "    return profile == 'cern-team'\n"
        "archive = 'hierarchical architecture'\n"
        "class ArchiTranslation:\n"
        "    pass\n"
        "def build(archi_snapshot=None, *, cernteam_pin=None):\n"
        "    return call(adaptArchiSource=1, payload=b'archi')\n",
        encoding="utf-8",
    )
    hits = sorted(
        (line, value)
        for line, value in _runtime_tokens(probe)
        if _consumer_token(value)
    )
    assert hits == [
        (2, "ARCHI_WHEEL"),
        (4, "cern-team"),
        (6, "ArchiTranslation"),
        (8, "archi_snapshot"),
        (8, "cernteam_pin"),
        (9, "adaptArchiSource"),
        (9, "archi"),
    ]


def test_package_reader_never_imports_or_executes_payload(
    fixture: Fixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    package = verify_distribution_package(_build(fixture))
    called: list[str] = []
    real_import = builtins.__import__
    monkeypatch.setattr(
        builtins,
        "__import__",
        lambda *args, **kwargs: (
            called.append(str(args[0])) or real_import(*args, **kwargs)
        ),
    )
    assert package.read_asset("archi:operations-schema") == b"schema: operations\n"
    assert not any(name.startswith("archi") for name in called)


def test_package_suite_has_no_network_database_apply_or_sealed_release_dependency() -> (
    None
):
    production = Path(package_impl.__file__).read_text(encoding="utf-8")
    source = (
        production
        + "\n"
        + Path(__file__).read_text(encoding="utf-8")
        + "\n"
        + Path(__file__)
        .with_name("test_distribution_package_v2.py")
        .read_text(encoding="utf-8")
    )
    tree = ast.parse(source)
    forbidden = {
        "socket",
        "requests",
        "psycopg",
        "sealed_release",
        "apply_and_operate",
        "subprocess",
    }
    imports = {
        alias.name.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for alias in node.names
    }
    assert not forbidden & imports


def test_resource_limits_refuse_manifest_archive_objects_and_assets(
    fixture: Fixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cases = {
        "manifest_bytes": 1,
        "objects": len(fixture.manifest.objects) - 1,
        "object_bytes_each": max(item.size for item in fixture.manifest.objects) - 1,
        "object_bytes_total": sum(item.size for item in fixture.manifest.objects) - 1,
        "assets": len(fixture.manifest.assets) - 1,
        "selected_asset_bytes_each": max(
            item.uncompressed_size for item in fixture.manifest.assets
        )
        - 1,
        "selected_asset_bytes_total": sum(
            item.uncompressed_size for item in fixture.manifest.assets
        )
        - 1,
    }
    for limit, maximum in cases.items():
        with monkeypatch.context() as patch:
            patch.setitem(package_impl.LIMITS, limit, maximum)
            error = _expect_refusal(lambda: _build(fixture), "resource_limit_exceeded")
            assert error.payload == {
                "code": "resource_limit_exceeded",
                "logical_path": error.logical_path,
                "limit": limit,
                "observed": error.observed,
                "maximum": maximum,
            }
            assert error.observed is not None and error.observed > maximum
    package_path = _build(fixture)
    with monkeypatch.context() as patch:
        patch.setitem(
            package_impl.LIMITS, "archive_bytes", package_path.stat().st_size - 1
        )
        error = _expect_refusal(
            lambda: verify_distribution_package(package_path), "resource_limit_exceeded"
        )
        assert error.limit == "archive_bytes"
        assert error.maximum == package_path.stat().st_size - 1


def test_zip_member_limits_refuse_count_size_and_ratio(fixture: Fixture) -> None:
    def read_mutant(mutant: Fixture) -> None:
        package = verify_distribution_package(_build(mutant))
        package.read_asset("archi:operations-schema")

    over_count = make_fixture(
        fixture.root.parent / "over-count",
        wheel_kwargs={"extra_members": _RESOURCE_LIMITS["wheel_entries"] + 1},
    )
    _expect_refusal(
        lambda: read_mutant(over_count),
        "zip_resource_limit",
        "resource_limit_exceeded",
        "zip_integrity_failed",
    )
    over_size = make_fixture(
        fixture.root.parent / "over-size",
        wheel_kwargs={
            "extra_member": "archi/oversize.txt",
            "extra_member_bytes": 33 * 1024 * 1024,
        },
    )
    _expect_refusal(
        lambda: read_mutant(over_size),
        "zip_resource_limit",
        "resource_limit_exceeded",
        "zip_integrity_failed",
    )
    over_ratio = make_fixture(
        fixture.root.parent / "over-ratio",
        wheel_kwargs={
            "extra_member": "archi/ratio.txt",
            "extra_member_bytes": 1 * 1024 * 1024,
        },
    )
    _expect_refusal(
        lambda: read_mutant(over_ratio),
        "zip_resource_limit",
        "resource_limit_exceeded",
        "zip_integrity_failed",
    )


def test_zip_member_trust_checks_refuse_duplicate_alias_directory_symlink_and_encryption(
    fixture: Fixture,
) -> None:
    def read_mutant(mutant: Fixture) -> None:
        package = verify_distribution_package(_build(mutant))
        package.read_asset("archi:operations-schema")

    duplicate = make_fixture(
        fixture.root.parent / "duplicate", wheel_kwargs={"duplicate_alias": True}
    )
    _expect_refusal(
        lambda: read_mutant(duplicate),
        "zip_member_unsafe",
        "zip_integrity_failed",
        "zip_resource_limit",
    )
    for kind in ("directory", "symlink", "encrypted"):
        mutant = make_fixture(
            fixture.root.parent / kind, wheel_kwargs={"invalid_member": kind}
        )
        _expect_refusal(
            lambda mutant=mutant: read_mutant(mutant),
            "zip_member_unsafe",
            "zip_integrity_failed",
        )


def test_zip_member_integrity_refuses_local_central_crc_size_record_and_digest_mismatch(
    fixture: Fixture,
) -> None:
    wheel = fixture.wheel_bytes
    with zipfile.ZipFile(io.BytesIO(wheel)) as archive:
        info = archive.getinfo("archi/schemas/operations-schema.yaml")
        mutations = (
            {"member_crc32": f"{info.CRC ^ 1:08x}"},
            {"member_compressed_size": info.compress_size + 1},
            {"compression_method": "stored"},
        )
    for mutation in mutations:
        locator = fixture.manifest.assets[1].locator.model_copy(update=mutation)
        mutant = _with_assets(
            fixture,
            (
                fixture.manifest.assets[0],
                cast(
                    PackagedAsset,
                    fixture.manifest.assets[1].model_copy(update={"locator": locator}),
                ),
                *fixture.manifest.assets[2:],
            ),
        )
        _expect_refusal(
            lambda mutant=mutant: verify_distribution_package(
                _build(mutant)
            ).read_asset("archi:operations-schema"),
            "zip_integrity_failed",
            "digest_mismatch",
            "zip_member_unsafe",
        )
    bad_record = make_fixture(
        fixture.root.parent / "bad-record", wheel_kwargs={"record_mutation": True}
    )
    _expect_refusal(
        lambda: verify_distribution_package(_build(bad_record)).read_asset(
            "archi:operations-schema"
        ),
        "zip_integrity_failed",
    )
    for kind, expected_code in (
        ("local-crc", "zip_integrity_failed"),
        ("unsupported-compression", "zip_member_unsafe"),
        ("corrupt-deflate", "zip_integrity_failed"),
        ("trailing-deflate", "zip_integrity_failed"),
    ):
        mutant = make_fixture(
            fixture.root.parent / kind,
            wheel_kwargs={"invalid_member": kind},
        )
        _expect_refusal(
            lambda mutant=mutant: verify_distribution_package(
                _build(mutant)
            ).read_asset("archi:operations-schema"),
            expected_code,
        )
    bad_digest_asset = fixture.assets[1]
    bad_digest = TypedAsset.from_value(
        {**bad_digest_asset.to_dict(), "digest": _digest(b"not the selected bytes")}
    )
    bad_digest_manifest = _with_assets(
        fixture,
        (
            fixture.manifest.assets[0],
            cast(
                PackagedAsset,
                fixture.manifest.assets[1].model_copy(update={"asset": bad_digest}),
            ),
            *fixture.manifest.assets[2:],
        ),
    )
    _expect_refusal(
        lambda: verify_distribution_package(_build(bad_digest_manifest)).read_asset(
            "archi:operations-schema"
        ),
        "digest_mismatch",
        "zip_integrity_failed",
    )


def test_archive_parser_refuses_noncanonical_ustar_and_trailing_bytes(
    fixture: Fixture,
) -> None:
    package = _build(fixture)
    trailing = _bind_transport_payload(
        fixture.root.parent, package.read_bytes() + b"trailing"
    )
    _expect_refusal(
        lambda: verify_distribution_package(trailing), "archive_noncanonical"
    )
    noncanonical_mode = _bind_transport_payload(
        fixture.root.parent,
        _mutate_ustar_header(package.read_bytes(), offset=100, value=b"0000644\0"),
    )
    _expect_refusal(
        lambda: verify_distribution_package(noncanonical_mode), "archive_noncanonical"
    )


def test_source_symlink_hop_limit_cycle_and_escape_refuse(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.write_text("outside", encoding="utf-8")
    escape = root / "escape"
    escape.symlink_to(outside)
    with pytest.raises((ValueError, DistributionPackageError)):
        DistributionPackageSource(root=root, path=escape)
    cycle_a = root / "a"
    cycle_b = root / "b"
    cycle_a.symlink_to(cycle_b)
    cycle_b.symlink_to(cycle_a)
    with pytest.raises((ValueError, DistributionPackageError)):
        DistributionPackageSource(root=root, path=cycle_a)


def test_source_open_refuses_capture_boundary_replacement(
    fixture: Fixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = fixture.sources[fixture.manifest.assets[3].locator.object_digest]
    original = source.path.read_bytes()
    replaced = False

    def replace_at_capture(path: str) -> None:
        nonlocal replaced
        if replaced or path != source.path.as_posix():
            return
        replaced = True
        source.path.rename(source.path.with_suffix(".captured"))
        source.path.write_bytes(original)

    monkeypatch.setattr(package_impl, "_capture_boundary_hook", replace_at_capture)
    _expect_refusal(lambda: _build(fixture), "source_race_detected")
    assert replaced

    staging_fixture = make_fixture(fixture.root.parent / "staging-race")
    staging_replaced = False

    def replace_staging(_final_name: str) -> None:
        nonlocal staging_replaced
        staged = next(staging_fixture.output.glob(".okgdist-*.tmp"))
        payload = staged.read_bytes()
        staged.rename(staged.with_suffix(".captured"))
        staged.write_bytes(payload)
        staging_replaced = True

    monkeypatch.setattr(package_impl, "_capture_boundary_hook", None)
    monkeypatch.setattr(package_impl, "_publish_boundary_hook", replace_staging)
    _expect_refusal(lambda: _build(staging_fixture), "path_identity_changed")
    assert staging_replaced
    assert not list(staging_fixture.output.glob("*.okgdist.tar"))


def test_package_reader_pins_archive_descriptor_under_replacement(
    fixture: Fixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    package = _build(fixture)
    original = package.read_bytes()
    replaced = False

    def replace_after_open(path: str) -> None:
        nonlocal replaced
        if replaced or path != package.as_posix():
            return
        replaced = True
        package.rename(package.with_suffix(".captured"))
        package.write_bytes(original)

    monkeypatch.setattr(package_impl, "_archive_boundary_hook", replace_after_open)
    _expect_refusal(
        lambda: verify_distribution_package(package), "path_identity_changed"
    )
    assert replaced


def test_materialize_destination_conflict_race_has_no_partial_output(
    fixture: Fixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checked = verify_distribution_package(_build(fixture))
    destination = fixture.root.parent / "destination"
    raced = False

    def create_at_publish(name: str) -> None:
        nonlocal raced
        if raced or name != destination.name:
            return
        raced = True
        destination.mkdir()

    monkeypatch.setattr(package_impl, "_publish_boundary_hook", create_at_publish)
    _expect_refusal(lambda: checked.materialize(destination), "destination_conflict")
    assert raced
    assert not (destination / "manifest.json").exists()


def test_materialize_nested_component_symlink_refuses(fixture: Fixture) -> None:
    checked = verify_distribution_package(_build(fixture))
    parent = fixture.root.parent / "materialize-parent"
    parent.mkdir()
    target = fixture.root.parent / "redirected"
    target.mkdir()
    (parent / "nested").symlink_to(target, target_is_directory=True)
    _expect_refusal(
        lambda: checked.materialize(parent / "nested" / "deeper" / "destination"),
        "path_identity_changed",
    )
    assert not (target / "manifest.json").exists()


def test_streaming_reads_use_bounded_chunks_and_read_asset_refuses_preallocation(
    fixture: Fixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requested: list[int] = []
    real_read = package_impl.os.read

    def bounded_read(fd: int, count: int) -> bytes:
        requested.append(count)
        return real_read(fd, count)

    monkeypatch.setattr(package_impl.os, "read", bounded_read)
    package = verify_distribution_package(_build(fixture))
    assert package.read_asset("archi:operations-schema") == b"schema: operations\n"
    destination = fixture.root.parent / "bounded-materialization"
    package.materialize(destination)
    assert requested and max(requested) <= _RESOURCE_LIMITS["stream_chunk_bytes"]
    with monkeypatch.context() as patch:
        patch.setitem(
            package_impl.LIMITS,
            "selected_asset_bytes_each",
            fixture.manifest.assets[1].uncompressed_size - 1,
        )
        error = _expect_refusal(
            lambda: package.read_asset("archi:operations-schema"),
            "resource_limit_exceeded",
        )
        assert error.limit == "selected_asset_bytes_each"


def test_build_output_directory_and_owner_qualified_reference_grammar(
    fixture: Fixture,
) -> None:
    package = _build(fixture)
    assert package.parent == fixture.output
    checked = verify_distribution_package(package)
    assert checked.read_asset("archi:operations-schema") == b"schema: operations\n"
    for ref in (
        "archi:operations-schema:extra",
        "archi/operations-schema",
        "archi operations",
    ):
        _expect_refusal(
            lambda ref=ref: checked.read_asset(ref),
            "locator_invalid",
            "manifest_invalid",
        )


def test_packaged_typed_asset_metadata_is_closed_and_structural(
    fixture: Fixture,
) -> None:
    asset = fixture.assets[0]
    assert asset.compatibility == {
        "contract_versions": ("v1",),
        "capabilities": ("public",),
    } or asset.compatibility == {
        "contract_versions": ["v1"],
        "capabilities": ["public"],
    }
    mutations = (
        {"compatibility": {"unexpected": {"innocent": "value"}}},
        {
            "provenance": {
                "source": 3,
                "repository_url": "https://example.com",
                "revision": _ARCHI_REVISION,
                "artifact_ref": "archi:wheel",
            }
        },
        {
            "provenance": {
                "source": "package",
                "repository_url": "https://example.com",
                "revision": _ARCHI_REVISION,
                "artifact_ref": "archi:wheel",
                "extra": "public",
            }
        },
        {
            "override_slots": [
                {
                    "slot_id": "runtime",
                    "value_type": "string",
                    "schema": {"type": "string"},
                }
            ]
        },
    )
    for mutation in mutations:
        unsafe = TypedAsset.from_value({**asset.to_dict(), **mutation})
        mutant = _with_unchecked_asset(fixture, 0, unsafe)
        _expect_refusal(lambda mutant=mutant: _build(mutant), "metadata_forbidden")

    extension = PublicExtensionMetadata(
        subject=_asset_ref(asset),
        package_ref=fixture.manifest.package_identities[0],
        api_version="v1",
        entry_point_group="okg-extensions",
        entry_point_name="public-reader",
        placeholders=(
            UnresolvedPlaceholder(kind="secret", owner="archi", ref="token"),
            UnresolvedPlaceholder(kind="endpoint", owner="archi", ref="api"),
        ),
    )
    assert [item.kind for item in extension.placeholders] == ["endpoint", "secret"]
    fixture.manifest.model_copy(update={"public_extensions": (extension,)})

    unresolved_package = extension.model_copy(
        update={"package_ref": _artifact("archi", "other-wheel", "wheel", b"other")}
    )
    with pytest.raises(ValueError, match="package_ref does not resolve"):
        fixture.manifest.model_copy(update={"public_extensions": (unresolved_package,)})

    unresolved_subject = extension.model_copy(
        update={
            "subject": _safe_reference(
                "archi",
                "missing-asset",
                kind="schema",
                digest=_digest(b"missing"),
            )
        }
    )
    with pytest.raises(ValueError, match="subject does not resolve"):
        fixture.manifest.model_copy(update={"public_extensions": (unresolved_subject,)})


def _release_producer_inputs(root: Path) -> dict[str, Any]:
    """Build the minimal pinned inputs one release entry needs.

    The framework wheel only has to carry the launcher module the producer
    copies out of it; nothing here installs or executes anything.
    """
    framework = root / "framework.whl"
    with zipfile.ZipFile(framework, "w") as wheel:
        wheel.writestr(
            "okg/substrate/distributions/install_launcher.py", "raise SystemExit(0)"
        )
    consumer = root / "consumer.whl"
    consumer.write_bytes(b"consumer")
    installer = root / "pip.whl"
    installer.write_bytes(b"trusted-pip")
    wheelhouse = root / "wheelhouse"
    (wheelhouse / "wheels").mkdir(parents=True)
    dependency = wheelhouse / "wheels" / "dep.whl"
    dependency.write_bytes(b"dependency")
    requirements = wheelhouse / "runtime-wheels.txt"
    requirements.write_text("dep==1 \\\n    --hash=sha256:test\n")

    def pin(path: Path) -> str:
        return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()

    manifest = wheelhouse / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "requirements_digest": pin(requirements),
                "entries": [
                    {"wheel": dependency.name, "wheel_digest": pin(dependency)}
                ],
            }
        )
    )
    return {
        "framework": framework,
        "framework_digest": pin(framework),
        "consumer": consumer,
        "consumer_digest": pin(consumer),
        "consumer_package": "okg_example_records",
        "wheelhouse": wheelhouse,
        "wheelhouse_digest": pin(manifest),
        "installer": installer,
        "installer_digest": pin(installer),
    }


def test_public_namespace_exports_the_release_producer() -> None:
    """Issue #1953: assembling a release entry must not need ``okg.substrate``.

    The public name is an alias of the substrate function on purpose, the same
    way ``okg.deployment`` aliases the absorbed SDK names: a wrapper would let
    the two signatures drift apart.
    """
    # One import style per module in this file: CodeQL's py/import-and-import-from
    # flags mixing ``import okg.distributions`` with ``from okg.distributions``.
    from okg.distributions import __all__ as public_names, build_release_entry
    from okg.substrate.distributions.install_bundle import build_bundle

    assert "build_release_entry" in public_names
    assert build_release_entry is build_bundle
    parameters = inspect.signature(build_release_entry).parameters
    assert set(parameters) == {
        "framework",
        "framework_digest",
        "consumer",
        "consumer_digest",
        "consumer_package",
        "wheelhouse",
        "wheelhouse_digest",
        "installer",
        "installer_digest",
        "output",
    }
    assert all(
        parameter.kind is inspect.Parameter.KEYWORD_ONLY
        for parameter in parameters.values()
    )


def test_public_release_producer_matches_the_private_one(tmp_path: Path) -> None:
    """The same inputs produce the same release bytes through either name."""
    from okg.distributions import build_release_entry
    from okg.substrate.distributions.install_bundle import build_bundle

    root = tmp_path.resolve()
    inputs = _release_producer_inputs(root)

    public_entry = build_release_entry(output=root / "public-release", **inputs)
    private_entry = build_bundle(output=root / "private-release", **inputs)

    assert public_entry.name == "install.py"
    assert public_entry.read_bytes() == private_entry.read_bytes()
    public_release = json.loads((public_entry.parent / "release.json").read_text())
    private_release = json.loads((private_entry.parent / "release.json").read_text())
    assert public_release == private_release
    public_lock = public_entry.parent / "dependencies" / "runtime-wheels.txt"
    private_lock = private_entry.parent / "dependencies" / "runtime-wheels.txt"
    assert hashlib.sha256(public_lock.read_bytes()).hexdigest() == hashlib.sha256(
        private_lock.read_bytes()
    ).hexdigest()
    assert public_release["pins"]["dependency-digest"] == (
        "sha256:" + hashlib.sha256(public_lock.read_bytes()).hexdigest()
    )


def test_public_release_producer_refuses_an_existing_output(tmp_path: Path) -> None:
    """A release directory is published, never adopted."""
    from okg.distributions import DistributionPackageError, build_release_entry

    root = tmp_path.resolve()
    inputs = _release_producer_inputs(root)
    occupied = root / "release"
    occupied.mkdir()
    with pytest.raises(DistributionPackageError) as caught:
        build_release_entry(output=occupied, **inputs)
    assert caught.value.code == "release_output_exists"
    assert list(occupied.iterdir()) == []


def test_public_release_producer_refuses_a_digest_that_does_not_match(
    tmp_path: Path,
) -> None:
    """Every artifact is checked against its pin as it is copied."""
    from okg.distributions import DistributionPackageError, build_release_entry

    root = tmp_path.resolve()
    inputs = _release_producer_inputs(root)
    inputs["consumer_digest"] = _ZERO_DIGEST
    output = root / "release"
    with pytest.raises(DistributionPackageError) as caught:
        build_release_entry(output=output, **inputs)
    assert caught.value.code == "release_artifact_digest_mismatch"
    assert caught.value.logical_path == "consumer"
    assert not output.exists()


def test_public_release_producer_refuses_the_framework_package_name(
    tmp_path: Path,
) -> None:
    """The consumer package must be a distinct importable name."""
    from okg.distributions import DistributionPackageError, build_release_entry

    root = tmp_path.resolve()
    inputs = _release_producer_inputs(root)
    inputs["consumer_package"] = "okg"
    with pytest.raises(DistributionPackageError) as caught:
        build_release_entry(output=root / "release", **inputs)
    assert caught.value.code == "release_consumer_package_invalid"


def test_public_release_producer_refuses_a_malformed_wheelhouse(
    tmp_path: Path,
) -> None:
    """A manifest that is not a list of named wheels refuses with its code."""
    from okg.distributions import DistributionPackageError, build_release_entry

    root = tmp_path.resolve()
    inputs = _release_producer_inputs(root)
    manifest = inputs["wheelhouse"] / "manifest.json"
    manifest.write_text(json.dumps({"requirements_digest": _ZERO_DIGEST}))
    inputs["wheelhouse_digest"] = (
        "sha256:" + hashlib.sha256(manifest.read_bytes()).hexdigest()
    )
    with pytest.raises(DistributionPackageError) as caught:
        build_release_entry(output=root / "release", **inputs)
    assert caught.value.code == "release_wheelhouse_manifest_malformed"
