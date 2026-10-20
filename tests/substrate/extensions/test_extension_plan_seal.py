"""Focused conformance for import-free planning and descriptor-anchored sealing.

The fixture wheels are assembled as inert bytes in a temporary directory.  No
fixture package is put on ``sys.path`` or imported by this test process.
"""

from __future__ import annotations

import base64
import csv
import ctypes
import errno
from dataclasses import dataclass, replace
import hashlib
import io
import json
import os
import select
from pathlib import Path
import signal
import stat
import sys
import time
import threading
import sysconfig
from types import MappingProxyType, SimpleNamespace
from typing import Any, Callable
import zipfile

import pytest

from okg.substrate import extension_package_v2 as plan_impl
from okg.substrate import extension_runtime_seal as seal_impl
from okg.substrate import _sealed_release_runtime as release_runtime
from okg.substrate import sealed_release as sealed_release_impl
from okg.substrate.extension_package_v2 import (
    CanonicalRecordRef,
    PackageObjectSelection,
    PackagePlanSelections,
    RawBlobRef,
    RuntimeToolchainIdentity,
    VerifiedPackageAuthority,
    canonical_json_bytes,
    canonical_record_digest,
    plan_extension_package_v2,
    raw_blob_digest,
    verify_package_plan_v2,
)
from okg.substrate.extension_runtime_seal import (
    ExtensionRuntimeSealError,
    ToolchainComponent,
    VerifiedRuntimeToolchain,
    bootstrap_verifier_program_bytes,
    builder_program_bytes,
    seal_extension_runtime,
    select_runtime_toolchain,
    verify_runtime_toolchain,
    verify_sealed_runtime,
)
from okg.substrate._sealed_release_integrity import (
    executable_tree_digest,
    seal_permissions,
)
from okg.substrate.sealed_release import SealedRelease, SealedReleaseInputs
from tests.substrate.test_sealed_release_runtime import _producer_proof


_FIXTURE_ROOT = (
    Path(__file__).parents[2]
    / "fixtures"
    / "deployment_products"
    / "installed_extension_runtime"
    / "package_objects"
)
_ZERO_DIGEST = "sha256:" + "0" * 64
_PLATFORM = {
    "os": "fixture-os",
    "architecture": "fixture-arch",
    "libc": "fixture-libc",
    "kernel_contract": "fixture-kernel-v1",
}


def _wheel_bytes(name: str, version: str, members: dict[str, bytes]) -> bytes:
    """Build deterministic, fully RECORD-covered wheel bytes."""

    normalized = name.replace("-", "_")
    dist_info = f"{normalized}-{version}.dist-info"
    values = dict(members)
    values[f"{dist_info}/METADATA"] = (
        f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n"
    ).encode()
    values[f"{dist_info}/WHEEL"] = (
        b"Wheel-Version: 1.0\nGenerator: okg-neutral-fixture\n"
        b"Root-Is-Purelib: true\nTag: py3-none-any\n"
    )
    record_name = f"{dist_info}/RECORD"
    rows: list[list[str]] = []
    for member_name, payload in sorted(values.items()):
        encoded = base64.urlsafe_b64encode(hashlib.sha256(payload).digest()).rstrip(
            b"="
        )
        rows.append([member_name, "sha256=" + encoded.decode(), str(len(payload))])
    rows.append([record_name, "", ""])
    record = io.StringIO(newline="")
    csv.writer(record, lineterminator="\n").writerows(rows)
    values[record_name] = record.getvalue().encode()

    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_STORED) as archive:
        for member_name, payload in sorted(values.items()):
            info = zipfile.ZipInfo(member_name, date_time=(1980, 1, 1, 0, 0, 0))
            info.create_system = 3
            info.external_attr = (stat.S_IFREG | 0o444) << 16
            archive.writestr(info, payload)
    return output.getvalue()


def _asset(asset_ref: str, raw_blob: str) -> SimpleNamespace:
    return SimpleNamespace(
        asset=SimpleNamespace(qualified_id=asset_ref),
        locator=SimpleNamespace(object_digest=raw_blob),
    )


@dataclass(slots=True)
class _RetainedAuthority:
    identity: VerifiedPackageAuthority
    objects: dict[str, bytes]
    closed: bool = False

    def to_record(self) -> dict[str, object]:
        record = self.identity.to_record()
        record.pop("schema_version")
        return record

    def read_verified_object(self, raw_blob: str) -> bytes:
        if self.closed:
            raise ValueError("closed")
        return self.objects[raw_blob]

    def close(self) -> None:
        self.closed = True


@dataclass(slots=True)
class _VerifiedPackageFixture:
    manifest: object
    objects: dict[str, bytes]
    identity: VerifiedPackageAuthority

    def extension_runtime_authority(self) -> _RetainedAuthority:
        return _RetainedAuthority(self.identity, self.objects)


@dataclass(slots=True)
class _PlanFixture:
    package: _VerifiedPackageFixture
    selections: PackagePlanSelections
    toolchain: VerifiedRuntimeToolchain
    sentinel: Path

    def close(self) -> None:
        self.toolchain.close()


def _verified_authority(objects: dict[str, bytes]) -> VerifiedPackageAuthority:
    def canonical(schema: str, value: object) -> CanonicalRecordRef:
        return CanonicalRecordRef(
            schema,
            canonical_record_digest({"schema_version": schema, "value": value}),
        )

    manifest = RawBlobRef(raw_blob_digest(b"neutral manifest"), 16)
    archive = RawBlobRef(raw_blob_digest(b"neutral archive"), 15)
    object_index = canonical(
        "okg.extension_runtime.verified_object_index/v1",
        sorted(objects),
    )
    asset_index = canonical(
        "okg.extension_runtime.verified_asset_index/v1", sorted(objects)
    )
    package_index = canonical(
        "okg.extension_runtime.package_identity_index/v1", "neutral"
    )
    extension_index = canonical(
        "okg.extension_runtime.public_extension_index/v1", "neutral"
    )
    verification_profile = canonical(
        "okg.distribution_package.verification_profile/v1", "strict"
    )
    receipt_preimage = {
        "schema_version": "okg.extension_runtime.package_verification_receipt/v1",
        "distribution_package_schema": (
            "okg.deployment_product.distribution_package/v1"
        ),
        "manifest": manifest.to_record(),
        "transport_archive": archive.to_record(),
        "verified_object_index_digest": object_index.to_record(),
        "verified_asset_index_digest": asset_index.to_record(),
        "package_identity_index_digest": package_index.to_record(),
        "public_extension_index_digest": extension_index.to_record(),
        "verification_profile_digest": verification_profile.to_record(),
    }
    receipt = CanonicalRecordRef(
        "okg.extension_runtime.package_verification_receipt/v1",
        canonical_record_digest(receipt_preimage),
    )
    return VerifiedPackageAuthority(
        "okg.deployment_product.distribution_package/v1",
        manifest,
        archive,
        object_index,
        asset_index,
        package_index,
        extension_index,
        verification_profile,
        receipt,
    )


def _runtime_layout_record(
    *,
    identity_parts: dict[str, Any],
    components: dict[str, bytes],
    members: dict[str, ToolchainComponent],
    stdlib_digest: CanonicalRecordRef,
) -> dict[str, object]:
    sdk = seal_impl._wheel_install(components["public_sdk"], None)
    child = seal_impl._wheel_install(components["child_host"], None)
    return {
        "schema_version": seal_impl.RUNTIME_LAYOUT_SCHEMA,
        "python_abi": identity_parts["python_abi"],
        "platform_identity": dict(identity_parts["platform_identity"]),
        "interpreter": members["interpreter"].runtime_record(),
        "prefix_configuration": {
            "profile": "standalone-base-prefix/v1",
            "pyvenv_config": None,
        },
        "distribution_prefix_tree_digest": identity_parts[
            "distribution_prefix_tree_digest"
        ].to_record(),
        "prefix_copy_proof_digest": identity_parts[
            "prefix_copy_proof_digest"
        ].to_record(),
        "stdlib_roots": [],
        "site_packages_root": "lib/python3.12/site-packages",
        "native_library_roots": [],
        "builtin_modules": [],
        "frozen_modules": [],
        "native_shared_libraries": [],
        "stdlib_manifest_digest": stdlib_digest.to_record(),
        "public_sdk": {
            "wheel_raw_blob": identity_parts["public_sdk_raw_blob"].to_record(),
            "installed_tree_manifest_digest": seal_impl._runtime_distribution_tree(
                sdk
            ).to_record(),
        },
        "child_host": {
            "wheel_raw_blob": identity_parts["child_host_raw_blob"].to_record(),
            "installed_tree_manifest_digest": seal_impl._runtime_distribution_tree(
                child
            ).to_record(),
        },
        "bootstrap_verifier": members["bootstrap_verifier"].runtime_record(),
        "builder": members["builder"].runtime_record(),
        "isolated_flags": ["-I", "-s", "-E"],
        "bytecode_policy": "absent",
    }


def _toolchain(tmp_path: Path) -> VerifiedRuntimeToolchain:
    sdk_wheel = _wheel_bytes(
        "okg-extension-sdk",
        "1.0.0",
        {"okg_extension_sdk/__init__.py": b"class Request: pass\n"},
    )
    child_wheel = _wheel_bytes(
        "okg-extension-child",
        "1.0.0",
        {"okg_extension_child/__init__.py": b"def launch(): return None\n"},
    )
    components = {
        "interpreter": b"fixture interpreter bytes",
        "public_sdk": sdk_wheel,
        "child_host": child_wheel,
        "bootstrap_verifier": bootstrap_verifier_program_bytes(),
        "builder": builder_program_bytes(),
        "broker": b"fixture broker bytes",
        "containment_launcher": b"fixture launcher bytes",
    }
    runtime_names = {
        "interpreter": ("bin/python3.12", True),
        "public_sdk": ("objects/public-sdk.whl", False),
        "child_host": ("objects/child-host.whl", False),
        "bootstrap_verifier": (
            "lib/python3.12/site-packages/okg_extension_bootstrap.py",
            False,
        ),
        "builder": (
            "lib/python3.12/site-packages/okg_extension_builder.py",
            False,
        ),
        "broker": ("libexec/okg-extension-broker", True),
        "containment_launcher": ("libexec/okg-extension-launcher", True),
    }
    root = tmp_path / "sealed-release"
    root.mkdir()
    manifest_bytes = canonical_json_bytes({"fixture": "toolchain"})
    (root / "manifest.json").write_bytes(manifest_bytes)
    members: dict[str, ToolchainComponent] = {}
    for name, payload in components.items():
        release_name = f"components/{name}"
        destination = root / release_name
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(payload)
        runtime_name, executable = runtime_names[name]
        members[name] = ToolchainComponent(
            release_name,
            runtime_name,
            RawBlobRef(raw_blob_digest(payload), len(payload)),
            executable,
        )
    stdlib_record = {
        "schema_version": seal_impl.STDLIB_SCHEMA,
        "entries": [],
        "builtin_modules": [],
        "frozen_modules": [],
        "native_shared_libraries": [],
    }
    stdlib_digest = CanonicalRecordRef(
        seal_impl.STDLIB_SCHEMA, canonical_record_digest(stdlib_record)
    )
    identity_parts: dict[str, Any] = {
        "platform_identity": _PLATFORM,
        "python_abi": "cp312",
        "public_sdk_raw_blob": members["public_sdk"].raw_blob,
        "child_host_raw_blob": members["child_host"].raw_blob,
        "distribution_prefix_tree_digest": CanonicalRecordRef(
            "okg.sealed_release.runtime_distribution_prefix_tree/v1",
            "sha256:" + "d" * 64,
        ),
        "prefix_copy_proof_digest": CanonicalRecordRef(
            "okg.sealed_release.runtime_prefix_copy_proof/v1",
            "sha256:" + "e" * 64,
        ),
    }
    layout = _runtime_layout_record(
        identity_parts=identity_parts,
        components=components,
        members=members,
        stdlib_digest=stdlib_digest,
    )
    identity = RuntimeToolchainIdentity(
        {
            "schema_version": "okg.sealed_release/v1",
            "artifact_id": "sha256:" + "1" * 64,
        },
        CanonicalRecordRef("okg.sealed_release.tree/v1", "sha256:" + "2" * 64),
        CanonicalRecordRef(
            "okg.sealed_release.runtime_authority/v2", "sha256:" + "c" * 64
        ),
        _PLATFORM,
        "cp312",
        members["interpreter"].raw_blob,
        identity_parts["distribution_prefix_tree_digest"],
        identity_parts["prefix_copy_proof_digest"],
        stdlib_digest,
        CanonicalRecordRef(
            seal_impl.RUNTIME_LAYOUT_SCHEMA, canonical_record_digest(layout)
        ),
        "okg-extension-sdk-v1",
        members["public_sdk"].raw_blob,
        members["child_host"].raw_blob,
        members["bootstrap_verifier"].raw_blob,
        members["builder"].raw_blob,
        members["broker"].raw_blob,
        members["containment_launcher"].raw_blob,
        CanonicalRecordRef(
            "okg.extension_runtime.containment_profile/v1", "sha256:" + "3" * 64
        ),
    )
    frozen_components = MappingProxyType(components)
    frozen_members = MappingProxyType(members)
    manifest_blob = RawBlobRef(raw_blob_digest(manifest_bytes), len(manifest_bytes))
    root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        root_stat = os.fstat(root_fd)
        root_identity = (
            root_stat.st_dev,
            root_stat.st_ino,
            root_stat.st_mode,
            root_stat.st_mtime_ns,
            root_stat.st_ctime_ns,
        )
        release_member_identities = MappingProxyType({
            "manifest.json": seal_impl._stable_file_token(os.stat(root / "manifest.json")),
            **{
                member.release_relative_name: seal_impl._stable_file_token(
                    os.stat(root / member.release_relative_name)
                )
                for member in members.values()
            },
        })
    except BaseException:
        os.close(root_fd)
        raise
    # The constructor owns root_fd from this point, including partial-object
    # cleanup. Do not close it again around constructor failure or success.
    return VerifiedRuntimeToolchain(
        identity,
        frozen_components,
        frozen_members,
        (),
        "manifest.json",
        manifest_blob,
        (),
        (),
        (),
        root_fd,
        root_identity,
        release_member_identities,
    )


@pytest.mark.parametrize("failure_site", ["root-stat", "member-stat"])
def test_toolchain_fixture_closes_root_when_metadata_fails(
    tmp_path: Path, monkeypatch, failure_site: str
) -> None:
    class FixtureFailure(BaseException):
        pass

    injected = FixtureFailure(failure_site)
    root = tmp_path / "sealed-release"
    opened: list[int] = []
    real_open, real_fstat, real_stat, real_close = os.open, os.fstat, os.stat, os.close

    def tracked_open(path, flags, *args, **kwargs):
        fd = real_open(path, flags, *args, **kwargs)
        if path == root:
            opened.append(fd)
        return fd

    def failing_fstat(fd):
        if failure_site == "root-stat" and opened and fd == opened[0]:
            raise injected
        return real_fstat(fd)

    def failing_stat(path, *args, **kwargs):
        if failure_site == "member-stat" and opened and path == root / "components/builder":
            raise injected
        return real_stat(path, *args, **kwargs)

    monkeypatch.setattr(os, "open", tracked_open)
    monkeypatch.setattr(os, "fstat", failing_fstat)
    monkeypatch.setattr(os, "stat", failing_stat)
    try:
        with pytest.raises(FixtureFailure) as refused:
            _toolchain(tmp_path)
        assert refused.value is injected
        assert len(opened) == 1
        with pytest.raises(OSError) as closed:
            real_fstat(opened[0])
        assert closed.value.errno == errno.EBADF
    finally:
        # Retire only our own leaked descriptor if a regression breaks cleanup.
        for fd in opened:
            try:
                real_fstat(fd)
            except OSError:
                continue
            real_close(fd)


def test_toolchain_fixture_transfers_root_ownership_once(tmp_path: Path, monkeypatch) -> None:
    toolchain = _toolchain(tmp_path)
    root_fd = toolchain._release_root_fd
    real_close = os.close
    closed: list[int] = []

    def tracked_close(fd):
        if fd == root_fd:
            closed.append(fd)
        return real_close(fd)

    monkeypatch.setattr(os, "close", tracked_close)
    try:
        assert verify_runtime_toolchain(toolchain) is toolchain
        os.fstat(root_fd)
        assert closed == []
        toolchain.close()
        assert closed == [root_fd]
        with pytest.raises(OSError) as retired:
            os.fstat(root_fd)
        assert retired.value.errno == errno.EBADF
        toolchain.close()
        assert closed == [root_fd]
    finally:
        toolchain.close()


def _descriptor_exec_supported() -> bool:
    libc = ctypes.CDLL(None)
    return hasattr(libc, "fexecve") or hasattr(libc, "execveat")


def _stdlib_zip(stdlib_root: Path) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for source in sorted(stdlib_root.rglob("*.py")):
            relative = source.relative_to(stdlib_root)
            if "site-packages" in relative.parts or "__pycache__" in relative.parts:
                continue
            info = zipfile.ZipInfo(relative.as_posix(), date_time=(1980, 1, 1, 0, 0, 0))
            info.create_system = 3
            info.external_attr = (stat.S_IFREG | 0o444) << 16
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, source.read_bytes())
    return output.getvalue()


def _sealed_runtime_release(
    tmp_path: Path,
    *,
    distribution: release_runtime.VerifiedRuntimeDistribution,
    python_description: dict[str, object],
    manifest_mutator: Callable[[dict[str, object], Path], None] | None = None,
) -> tuple[SealedRelease, str, dict[str, str]]:
    """Build a v2 release from one already verified complete-prefix input."""

    description = dict(python_description)
    major_value = description.get("major")
    minor_value = description.get("minor")
    assert isinstance(major_value, int) and not isinstance(major_value, bool)
    assert isinstance(minor_value, int) and not isinstance(minor_value, bool)
    major = major_value
    minor = minor_value
    python_abi = str(description["python_abi"])
    sdk_wheel = _wheel_bytes(
        "okg-extension-sdk",
        "1.0.0",
        {"okg_extension_sdk/__init__.py": b"class Request: pass\n"},
    )
    child_wheel = _wheel_bytes(
        "okg-extension-child",
        "1.0.0",
        {"okg_extension_child/__init__.py": b"def launch(): return None\n"},
    )
    draft = tmp_path.resolve() / "release-draft"
    repo = draft / "repo"
    venv = draft / "venv"
    (repo / "toolchain" / "components").mkdir(parents=True)
    distribution.copy_prefix_to(venv)
    interpreter = venv / "bin" / "python3.12"
    assert interpreter.is_file()
    interpreter.chmod(0o755)
    verifier_runtime_name = (
        f"lib/python{major}.{minor}/site-packages/"
        f"{release_runtime.RUNTIME_RELOCATION_VERIFIER_MODULE}.py"
    )
    verifier = venv / verifier_runtime_name
    verifier.parent.mkdir(parents=True, exist_ok=True)
    verifier.write_bytes(release_runtime.RUNTIME_RELOCATION_VERIFIER_SOURCE)
    runtime_root_fd = os.open(venv, os.O_RDONLY | os.O_DIRECTORY)
    repo_root_fd = os.open(repo, os.O_RDONLY | os.O_DIRECTORY)
    try:
        construction = release_runtime.write_runtime_construction_receipt(
            repo_root_fd=repo_root_fd,
            runtime_root_fd=runtime_root_fd,
            python_request=b"3.12",
            interpreter_relative_name="bin/python3.12",
            python_description=description,
            distribution=distribution,
            dependency_wheelhouse_manifest_digest="sha256:" + "d" * 64,
        )
    finally:
        os.close(runtime_root_fd)
        os.close(repo_root_fd)
    components: dict[str, bytes] = {
        "interpreter": interpreter.read_bytes(),
        "public_sdk": sdk_wheel,
        "child_host": child_wheel,
        "bootstrap_verifier": bootstrap_verifier_program_bytes(),
        "builder": builder_program_bytes(),
        "broker": b"fixture broker bytes",
        "containment_launcher": b"fixture launcher bytes",
    }
    site = f"lib/python{major}.{minor}/site-packages"
    runtime_names = {
        "interpreter": ("bin/python3.12", True),
        "public_sdk": ("objects/public-sdk.whl", False),
        "child_host": ("objects/child-host.whl", False),
        "bootstrap_verifier": (
            f"{site}/okg_extension_bootstrap.py",
            False,
        ),
        "builder": (f"{site}/okg_extension_builder.py", False),
        "broker": ("libexec/okg-extension-broker", True),
        "containment_launcher": ("libexec/okg-extension-launcher", True),
    }
    members: dict[str, ToolchainComponent] = {}
    for name, payload in components.items():
        release_name = (
            "venv/bin/python3.12"
            if name == "interpreter"
            else f"repo/toolchain/components/{name}"
        )
        target = draft / release_name
        if not target.exists():
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(payload)
        runtime_name, executable_flag = runtime_names[name]
        members[name] = ToolchainComponent(
            release_name,
            runtime_name,
            RawBlobRef(raw_blob_digest(payload), len(payload)),
            executable_flag,
        )

    stdlib_payloads: list[tuple[seal_impl.ToolchainMember, bytes]] = []
    native_names: list[str] = []
    for candidate in sorted(venv.rglob("*")):
        if (
            not candidate.is_file()
            or candidate.is_symlink()
            or candidate == interpreter
        ):
            continue
        runtime_name = candidate.relative_to(venv).as_posix()
        if "__pycache__" in runtime_name.split("/") or runtime_name.endswith(
            (".pyc", ".pyo")
        ):
            # Bytecode is regenerable and never sealed content.
            continue
        payload = candidate.read_bytes()
        release_name = f"venv/{runtime_name}"
        stdlib_payloads.append(
            (
                seal_impl.ToolchainMember(
                    release_name,
                    runtime_name,
                    RawBlobRef(raw_blob_digest(payload), len(payload)),
                    # A real prefix carries executable scripts (bin/2to3-3.12).
                    bool(candidate.stat().st_mode & 0o111),
                ),
                payload,
            )
        )
        if runtime_name.endswith((".so", ".dylib", ".dll")):
            native_names.append(runtime_name)
    stdlib_payloads.sort(key=lambda value: value[0].relative_name.encode("utf-8"))
    builtin_values = description.get("builtin_modules")
    frozen_values = description.get("frozen_modules")
    assert isinstance(builtin_values, list) and all(
        isinstance(name, str) for name in builtin_values
    )
    assert isinstance(frozen_values, list) and all(
        isinstance(name, str) for name in frozen_values
    )
    builtin = tuple(sorted(name for name in builtin_values if isinstance(name, str)))
    frozen = tuple(sorted(name for name in frozen_values if isinstance(name, str)))
    stdlib_record = {
        "schema_version": seal_impl.STDLIB_SCHEMA,
        "entries": [member.to_record() for member, _ in stdlib_payloads],
        "builtin_modules": list(builtin),
        "frozen_modules": list(frozen),
        "native_shared_libraries": sorted(native_names),
    }
    stdlib_digest = CanonicalRecordRef(
        seal_impl.STDLIB_SCHEMA, canonical_record_digest(stdlib_record)
    )
    platform_identity = release_runtime._platform_identity(description)
    containment = CanonicalRecordRef(
        "okg.extension_runtime.containment_profile/v1", "sha256:" + "6" * 64
    )
    receipt = construction["receipt"]
    assert isinstance(receipt, dict)
    provisional_identity = RuntimeToolchainIdentity(
        {
            "schema_version": "okg.sealed_release/v1",
            "artifact_id": _ZERO_DIGEST,
        },
        CanonicalRecordRef("okg.sealed_release.tree/v1", _ZERO_DIGEST),
        CanonicalRecordRef("okg.sealed_release.runtime_authority/v2", _ZERO_DIGEST),
        platform_identity,
        python_abi,
        members["interpreter"].raw_blob,
        CanonicalRecordRef(
            "okg.sealed_release.runtime_distribution_prefix_tree/v1",
            str(receipt["distribution_prefix_tree_digest"]),
        ),
        CanonicalRecordRef(
            "okg.sealed_release.runtime_prefix_copy_proof/v1",
            str(receipt["prefix_copy_proof_digest"]),
        ),
        stdlib_digest,
        CanonicalRecordRef(seal_impl.RUNTIME_LAYOUT_SCHEMA, _ZERO_DIGEST),
        "okg-extension-sdk-v1",
        members["public_sdk"].raw_blob,
        members["child_host"].raw_blob,
        members["bootstrap_verifier"].raw_blob,
        members["builder"].raw_blob,
        members["broker"].raw_blob,
        members["containment_launcher"].raw_blob,
        containment,
    )
    layout = seal_impl._runtime_layout_record(
        provisional_identity,
        components,
        members,
        stdlib_payloads,
        builtin,
        frozen,
        sorted(native_names),
    )
    manifest = {
        "schema_version": seal_impl.TOOLCHAIN_SCHEMA,
        "platform_identity": platform_identity,
        "python_abi": python_abi,
        "public_sdk_abi": "okg-extension-sdk-v1",
        "containment_profile_manifest_digest": containment.to_record(),
        "runtime_layout_digest": CanonicalRecordRef(
            seal_impl.RUNTIME_LAYOUT_SCHEMA, canonical_record_digest(layout)
        ).to_record(),
        "components": {
            name: {
                "release_relative_name": member.release_relative_name,
                "runtime_relative_name": member.runtime_relative_name,
                "raw_blob": member.raw_blob.to_record(),
                "executable": member.executable,
            }
            for name, member in members.items()
        },
        "stdlib": {
            "entries": [
                {
                    "release_relative_name": member.release_relative_name,
                    "relative_name": member.relative_name,
                    "raw_blob": member.raw_blob.to_record(),
                    "executable": member.executable,
                }
                for member, _ in stdlib_payloads
            ],
            "builtin_modules": list(builtin),
            "frozen_modules": list(frozen),
            "native_shared_libraries": sorted(native_names),
        },
    }
    if manifest_mutator is not None:
        manifest_mutator(manifest, draft)
    manifest_name = "repo/toolchain/manifest.json"
    (draft / manifest_name).write_bytes(canonical_json_bytes(manifest))
    seal_permissions(repo)
    seal_permissions(venv)
    tree = executable_tree_digest(draft)
    inputs = SealedReleaseInputs(
        code_revision="7" * 40,
        dependency_lock_digest="sha256:" + "8" * 64,
        schema_checksum="sha256:" + "9" * 64,
        catalog_fingerprint="sha256:" + "a" * 64,
        definition_digest="sha256:" + "b" * 64,
        skill_bundle_digest=None,
    )
    artifact_id = sealed_release_impl._release_artifact_id(inputs, tree)
    prepared_at = "2026-08-22T00:00:00+00:00"
    preparing_release = SealedRelease(artifact_id, draft, inputs, tree, prepared_at)
    sealed_release_impl._write_release_metadata(preparing_release)
    final_root = draft.with_name(artifact_id.removeprefix("sha256:"))
    draft.rename(final_root)
    seal_permissions(final_root)
    release = SealedRelease(artifact_id, final_root, inputs, tree, prepared_at)
    return release, manifest_name, platform_identity


def _plan_fixture(tmp_path: Path) -> _PlanFixture:
    tmp_path.mkdir(parents=True, exist_ok=True)
    sentinel = tmp_path / "import-sentinel"
    member_values = json.loads(
        (_FIXTURE_ROOT / "extension-wheel-members.json").read_text()
    )
    entry_source = str(member_values["neutral_extension/entry.py"])
    member_values["neutral_extension/entry.py"] = entry_source.replace(
        "os.environ.get('NEUTRAL_IMPORT_SENTINEL')", repr(str(sentinel))
    )
    extension_wheel = _wheel_bytes(
        "neutral-extension",
        "1.0.0",
        {name: str(value).encode() for name, value in member_values.items()},
    )
    dependency_values = json.loads(
        (_FIXTURE_ROOT / "dependency-wheel-members.json").read_text()
    )
    dependency_wheel = _wheel_bytes(
        "neutral-dependency",
        "1.0.0",
        {name: str(value).encode() for name, value in dependency_values.items()},
    )
    descriptor = json.loads((_FIXTURE_ROOT / "descriptor.template.json").read_text())
    descriptor["package_ref"]["raw_blob"] = RawBlobRef(
        raw_blob_digest(extension_wheel), len(extension_wheel)
    ).to_record()
    descriptor["entries"][0]["subject"]["typed_asset_digest"] = "sha256:" + "4" * 64
    descriptor["entries"][0]["output_policy"]["sensitive_value_policy_digest"] = (
        "sha256:" + "5" * 64
    )
    descriptor_bytes = canonical_json_bytes(descriptor)
    dependency_lock = json.loads(
        (_FIXTURE_ROOT / "dependency-lock.template.json").read_text()
    )
    dependency_lock["dependencies"][0]["raw_blob"] = RawBlobRef(
        raw_blob_digest(dependency_wheel), len(dependency_wheel)
    ).to_record()
    lock_bytes = canonical_json_bytes(dependency_lock)
    sbom = canonical_json_bytes(json.loads((_FIXTURE_ROOT / "sbom.json").read_text()))
    provenance = canonical_json_bytes(
        json.loads((_FIXTURE_ROOT / "provenance.json").read_text())
    )
    objects = {
        raw_blob_digest(value): value
        for value in (
            descriptor_bytes,
            extension_wheel,
            lock_bytes,
            dependency_wheel,
            sbom,
            provenance,
        )
    }
    refs = {
        "descriptor": ("neutral-fixture:descriptor", descriptor_bytes),
        "extension-wheel": (
            "neutral-fixture:neutral-extension-wheel",
            extension_wheel,
        ),
        "dependency-lock": ("neutral-fixture:dependency-lock", lock_bytes),
        "dependency-wheel": (
            "neutral-fixture:neutral-dependency-wheel",
            dependency_wheel,
        ),
        "sbom": ("neutral-fixture:sbom", sbom),
        "provenance": ("neutral-fixture:provenance", provenance),
    }
    assets = [
        _asset(asset_ref, raw_blob_digest(payload))
        for asset_ref, payload in refs.values()
    ]
    public_extension = {
        "subject": {
            "owner": "neutral-fixture",
            "ref": "documents",
            "kind": "source",
            "version": "1",
            "digest": "sha256:" + "4" * 64,
        },
        "package_ref": {
            "owner": "neutral-fixture",
            "ref": "neutral-extension",
            "kind": "wheel",
            "version": "1.0.0",
            "digest": raw_blob_digest(extension_wheel),
        },
        "api_version": "v2",
        "entry_point_group": "okg.extensions",
        "entry_point_name": "neutral-connector",
        "capabilities": ["filesystem.read", "graph.output.propose"],
        "placeholders": [],
    }
    package = _VerifiedPackageFixture(
        SimpleNamespace(assets=assets, public_extensions=[public_extension]),
        objects,
        _verified_authority(objects),
    )

    def selection(role: str) -> PackageObjectSelection:
        asset_ref, payload = refs[role]
        return PackageObjectSelection(asset_ref, raw_blob_digest(payload), role)

    selections = PackagePlanSelections(
        selection("descriptor"),
        selection("extension-wheel"),
        selection("dependency-lock"),
        (selection("dependency-wheel"),),
        selection("sbom"),
        selection("provenance"),
    )
    return _PlanFixture(package, selections, _toolchain(tmp_path), sentinel)


def _plan(value: _PlanFixture):
    return plan_extension_package_v2(
        value.package,
        value.selections,
        value.toolchain,
        extension_id="neutral.connector",
        platform_tags=("py3-none-any",),
    )


def _fixture_python_description() -> dict[str, object]:
    return {
        "major": 3,
        "minor": 12,
        "python_abi": "cp312",
        "builtin_modules": ["_abc"],
        "frozen_modules": ["_frozen_importlib"],
        "os": "linux",
        "architecture": "x86_64",
        "libc": "glibc",
    }


def _accept_fixture_descriptor_proof(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        sealed_release_impl,
        "_descriptor_startup_proof",
        lambda *_args, **_kwargs: {
            "schema_version": (
                "okg.sealed_release.runtime_descriptor_startup_proof/v1"
            ),
            "status": "descriptor-exec-verified",
        },
    )
    monkeypatch.setattr(
        sealed_release_impl,
        "_runtime_tk_native_members",
        lambda *_args, **_kwargs: (
            "lib/libtcl9.0.so",
            "lib/libtk9.0.so",
            "lib/python3.12/lib-dynload/_tkinter.cpython-312.so",
        ),
    )


def test_v2_toolchain_identity_consumes_exact_public_prefix_copy_proof(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _document, distribution = _producer_proof(tmp_path / "producer-proof")
    _accept_fixture_descriptor_proof(monkeypatch)
    release, manifest_name, platform_identity = _sealed_runtime_release(
        tmp_path / "release",
        distribution=distribution,
        python_description=_fixture_python_description(),
    )
    with release.extension_runtime_authority() as authority:
        receipt = authority.receipt
        original_artifact_id = authority.sealed_release_artifact_id
        object.__setattr__(
            authority,
            "sealed_release_artifact_id",
            "sha256:" + "0" * 64,
        )
        with pytest.raises(ExtensionRuntimeSealError) as changed_provenance:
            select_runtime_toolchain(
                authority,
                manifest_relative_path=manifest_name,
                platform_identity=platform_identity,
            )
        assert changed_provenance.value.code == "extension_runtime_toolchain_mismatch"
        object.__setattr__(
            authority,
            "sealed_release_artifact_id",
            original_artifact_id,
        )
        toolchain = select_runtime_toolchain(
            authority,
            manifest_relative_path=manifest_name,
            platform_identity=platform_identity,
        )
    try:
        record = toolchain.identity.to_record()

        assert record["schema_version"] == "okg.extension_runtime.toolchain/v2"
        authority_ref = record["release_runtime_authority_receipt_digest"]
        layout_ref = record["runtime_layout_digest"]
        interpreter_receipt = receipt["interpreter"]
        assert isinstance(authority_ref, dict)
        assert isinstance(layout_ref, dict)
        assert isinstance(interpreter_receipt, dict)
        assert authority_ref["schema_version"] == (
            "okg.sealed_release.runtime_authority/v2"
        )
        assert layout_ref["schema_version"] == "okg.extension_runtime.layout/v2"
        assert record["distribution_prefix_tree_digest"] == (
            toolchain.identity.distribution_prefix_tree_digest.to_record()
        )
        assert record["prefix_copy_proof_digest"] == (
            toolchain.identity.prefix_copy_proof_digest.to_record()
        )
        assert (
            toolchain.identity.distribution_prefix_tree_digest.digest
            == receipt["distribution_prefix_tree_digest"]
        )
        assert (
            toolchain.identity.prefix_copy_proof_digest.digest
            == receipt["prefix_copy_proof_digest"]
        )
        assert toolchain.identity.platform_identity == receipt["platform_identity"]
        assert toolchain.identity.python_abi == receipt["python_abi"]
        assert (
            toolchain.identity.interpreter_raw_blob.to_record()
            == interpreter_receipt["raw_blob"]
        )
        assert (
            toolchain.component_members["interpreter"].release_relative_name
            == "venv/bin/python3.12"
        )
        assert (
            toolchain.component_members["interpreter"].runtime_relative_name
            == "bin/python3.12"
        )
        assert "pyvenv_config_raw_blob" not in record
        assert RuntimeToolchainIdentity.from_value(record).to_record() == record
        with pytest.raises(TypeError):
            toolchain.identity.platform_identity["os"] = "changed"  # type: ignore[index]
        with pytest.raises(TypeError):
            toolchain.identity.sealed_release_artifact_id["artifact_id"] = (  # type: ignore[index]
                "sha256:" + "0" * 64
            )
    finally:
        toolchain.close()


@pytest.mark.parametrize(
    ("arm", "expected_code"),
    [
        ("platform", "extension_platform_mismatch"),
        ("python_abi", "extension_python_abi_mismatch"),
        ("interpreter", "extension_runtime_toolchain_mismatch"),
        ("mode", "extension_runtime_toolchain_mismatch"),
        ("stdlib_mode", "extension_runtime_toolchain_mismatch"),
        ("stdlib_type", "extension_runtime_toolchain_mismatch"),
        ("prefix_membership", "extension_runtime_toolchain_mismatch"),
    ],
)
def test_v2_toolchain_selection_refuses_authority_platform_interpreter_or_prefix_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    arm: str,
    expected_code: str,
) -> None:
    _document, distribution = _producer_proof(tmp_path / "producer-proof")
    _accept_fixture_descriptor_proof(monkeypatch)

    def mutate(manifest: dict[str, object], draft: Path) -> None:
        if arm == "platform":
            platform = manifest["platform_identity"]
            assert isinstance(platform, dict)
            platform["architecture"] = "changed-architecture"
            return
        if arm == "python_abi":
            manifest["python_abi"] = "cp313"
            return
        if arm == "interpreter":
            components = manifest["components"]
            assert isinstance(components, dict)
            interpreter = components["interpreter"]
            assert isinstance(interpreter, dict)
            source = draft / str(interpreter["release_relative_name"])
            alias_name = "repo/toolchain/components/interpreter-alias"
            alias = draft / alias_name
            alias.parent.mkdir(parents=True, exist_ok=True)
            alias.write_bytes(source.read_bytes())
            alias.chmod(0o755)
            interpreter["release_relative_name"] = alias_name
            return
        if arm == "mode":
            return
        stdlib = manifest["stdlib"]
        assert isinstance(stdlib, dict)
        entries = stdlib["entries"]
        assert isinstance(entries, list) and entries
        first = entries[0]
        assert isinstance(first, dict)
        if arm == "stdlib_mode":
            first["executable"] = not bool(first["executable"])
            return
        if arm == "stdlib_type":
            first["executable"] = "false"
            return
        source = draft / str(first["release_relative_name"])
        alias_name = "repo/toolchain/components/stdlib-alias"
        alias = draft / alias_name
        alias.parent.mkdir(parents=True, exist_ok=True)
        alias.write_bytes(source.read_bytes())
        first["release_relative_name"] = alias_name

    release, manifest_name, platform_identity = _sealed_runtime_release(
        tmp_path / "release",
        distribution=distribution,
        python_description=_fixture_python_description(),
        manifest_mutator=mutate,
    )
    with release.extension_runtime_authority() as authority:
        if arm == "mode":
            os.chmod(release.artifact_root / "venv" / "bin" / "python3.12", 0o755)
        with pytest.raises(ExtensionRuntimeSealError) as refusal:
            select_runtime_toolchain(
                authority,
                manifest_relative_path=manifest_name,
                platform_identity=platform_identity,
            )

    assert refusal.value.code == expected_code


@pytest.mark.parametrize(
    "arm",
    ["v1", "pyvenv", "missing-copy", "swapped-copy", "changed-copy"],
)
def test_v2_toolchain_identity_refuses_v1_pyvenv_or_changed_copy_proof(
    tmp_path: Path, arm: str
) -> None:
    toolchain = _toolchain(tmp_path)
    try:
        record = json.loads(json.dumps(toolchain.identity.to_record()))
    finally:
        toolchain.close()

    if arm == "v1":
        record["schema_version"] = "okg.extension_runtime.toolchain/v1"
    elif arm == "pyvenv":
        record["pyvenv_config_raw_blob"] = RawBlobRef(
            "sha256:" + "a" * 64, 1
        ).to_record()
    elif arm == "missing-copy":
        record.pop("prefix_copy_proof_digest")
    elif arm == "swapped-copy":
        record["prefix_copy_proof_digest"]["schema_version"] = (
            "okg.sealed_release.runtime_distribution_prefix_tree/v1"
        )
    else:
        record["prefix_copy_proof_digest"]["digest"] = "sha256:" + "f" * 64

    with pytest.raises(plan_impl.ExtensionPackageV2Error) as refusal:
        RuntimeToolchainIdentity.from_value(record)

    assert refusal.value.code == "extension_runtime_toolchain_mismatch"


def test_planning_is_import_free_and_reproducible(tmp_path: Path) -> None:
    fixture = _plan_fixture(tmp_path)
    try:
        first = _plan(fixture)
        second = _plan(fixture)
        assert not fixture.sentinel.exists()
        assert first.to_record() == second.to_record()
        assert first.package_plan_digest == plan_impl.package_plan_digest(first)
        assert first.runtime_toolchain.runtime_layout_digest == (
            fixture.toolchain.runtime_layout_digest
        )
        assert verify_package_plan_v2(first, package=fixture.package) is first
        assert verify_runtime_toolchain(fixture.toolchain) is fixture.toolchain
    finally:
        fixture.close()


def test_planning_refuses_changed_objects_and_stale_receipts(tmp_path: Path) -> None:
    fixture = _plan_fixture(tmp_path)
    try:
        plan = _plan(fixture)
        for selection in (
            fixture.selections.descriptor,
            fixture.selections.dependency_lock,
        ):
            selected_digest = selection.raw_blob_digest
            fixture.package.objects[selected_digest] += b" "
            with pytest.raises(plan_impl.ExtensionPackageV2Error) as changed:
                _plan(fixture)
            assert changed.value.code == "extension_package_object_mismatch"
            fixture.package.objects[selected_digest] = fixture.package.objects[
                selected_digest
            ][:-1]

        stale = replace(
            plan,
            package_plan_digest=CanonicalRecordRef(
                plan.package_plan_digest.schema_version, _ZERO_DIGEST
            ),
        )
        with pytest.raises(plan_impl.ExtensionPackageV2Error) as stale_error:
            verify_package_plan_v2(stale)
        assert stale_error.value.code == "extension_package_object_mismatch"
    finally:
        fixture.close()


@pytest.mark.skipif(
    not _descriptor_exec_supported(),
    reason="focused host lacks fexecve/execveat descriptor execution",
)
def test_isolated_tool_holds_interpreter_open_until_identity_recheck(
    tmp_path: Path, monkeypatch
) -> None:
    # This low-level handoff fixture is laid out like a standalone base prefix:
    # a private copy of the host interpreter plus links to the installed
    # standard library. It is not an admitted runtime. The child names
    # descriptor 6 as argv[0], so CPython resolves this prefix from the copied
    # executable; no pyvenv.cfg or ._pth startup configuration is involved.
    host_interpreter = Path(sys._base_executable).resolve()
    host_payload = host_interpreter.read_bytes()
    host_before = host_interpreter.stat()
    root = tmp_path / "runtime"
    member = f"bin/python{sys.version_info.major}.{sys.version_info.minor}"
    interpreter = root / member
    interpreter.parent.mkdir(parents=True)
    interpreter.write_bytes(host_payload)
    stdlib = Path(sysconfig.get_path("stdlib"))
    fixture_stdlib = root / "lib" / f"python{sys.version_info.major}.{sys.version_info.minor}"
    fixture_stdlib.mkdir(parents=True)
    for source in stdlib.iterdir():
        if source.name in {"site-packages", "dist-packages", "__pycache__"}:
            continue
        if source.name.split(".", 1)[0] in {"sitecustomize", "usercustomize"}:
            continue
        (fixture_stdlib / source.name).symlink_to(source, target_is_directory=source.is_dir())
    copied = interpreter.lstat()
    assert stat.S_ISREG(copied.st_mode)
    assert copied.st_nlink == 1
    assert (copied.st_dev, copied.st_ino) != (host_before.st_dev, host_before.st_ino)
    interpreter_payload = interpreter.read_bytes()
    interpreter.chmod(0o700)
    assert stat.S_IMODE(interpreter.stat().st_mode) == 0o700
    root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    interpreter_fd = os.open(interpreter, os.O_RDONLY)
    child_pids: list[int] = []
    real_fork = seal_impl.os.fork

    def recorded_fork() -> int:
        pid = real_fork()
        if pid > 0:
            child_pids.append(pid)
        return pid

    monkeypatch.setattr(seal_impl.os, "fork", recorded_fork)
    real_exec_fd = seal_impl._exec_fd
    exec_receipts = tmp_path / "exec-receipts"

    def checked_exec_fd(executable_fd, argv, environ) -> None:
        # Retain coverage of the production argv, flags and environment.
        assert executable_fd == 6
        assert len(argv) == 7
        assert argv[:6] == ["/proc/self/fd/6", "-I", "-s", "-E", "-B", "-c"]
        assert environ == {
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PYTHONDONTWRITEBYTECODE": "1",
        }
        receipt_fd = os.open(
            exec_receipts, os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_CLOEXEC, 0o600
        )
        try:
            os.write(receipt_fd, f"{os.getpid()}\n".encode("ascii"))
        finally:
            os.close(receipt_fd)
        real_exec_fd(executable_fd, argv, environ)

    monkeypatch.setattr(seal_impl, "_exec_fd", checked_exec_fd)
    request = {
        "schema_version": "okg.extension_runtime.bootstrap_request/v1",
        "interpreter_member": member,
    }
    try:
        result = seal_impl._run_isolated_module_with_interpreter_fd(
            root_fd,
            interpreter_fd,
            "okg_extension_bootstrap",
            request=request,
            program=bootstrap_verifier_program_bytes(),
            interpreter_expected=interpreter_payload,
        )
        assert result["inherited_fds"] == [3, 4, 5, 6, 8]
        for flag in (
            "executed_member_matches",
            "isolated",
            "ignore_environment",
            "no_user_site",
            "dont_write_bytecode",
            "cwd_empty",
        ):
            assert result[flag] is True, flag
        assert len(child_pids) == 1
        assert exec_receipts.read_text().splitlines() == list(map(str, child_pids))
        with pytest.raises(ChildProcessError):
            os.waitpid(child_pids[0], os.WNOHANG)

        original_verify = seal_impl._verify_open_interpreter_payload
        verification_count = 0

        def tamper_during_child(fd: int, expected: bytes) -> None:
            nonlocal verification_count
            verification_count += 1
            if verification_count == 2:
                os.fchmod(fd, 0o600)
            original_verify(fd, expected)

        monkeypatch.setattr(
            seal_impl,
            "_verify_open_interpreter_payload",
            tamper_during_child,
        )
        with pytest.raises(ExtensionRuntimeSealError) as refusal:
            seal_impl._run_isolated_module_with_interpreter_fd(
                root_fd,
                interpreter_fd,
                "okg_extension_bootstrap",
                request=request,
                program=bootstrap_verifier_program_bytes(),
                interpreter_expected=interpreter_payload,
            )
        assert refusal.value.code == "extension_runtime_toolchain_mismatch"
        assert verification_count == 2
        assert len(child_pids) == 2
        assert exec_receipts.read_text().splitlines() == list(map(str, child_pids))
        with pytest.raises(ChildProcessError):
            os.waitpid(child_pids[1], os.WNOHANG)
    finally:
        os.fchmod(interpreter_fd, 0o700)
        os.close(interpreter_fd)
        os.close(root_fd)
        host_after = host_interpreter.stat()
        assert host_interpreter.read_bytes() == host_payload
        for field in ("st_dev", "st_ino", "st_mode", "st_nlink", "st_mtime_ns", "st_ctime_ns"):
            assert getattr(host_after, field) == getattr(host_before, field), field


def test_isolated_tool_reaps_child_when_parent_pipe_write_fails(
    tmp_path: Path, monkeypatch
) -> None:
    root = tmp_path / "runtime"
    interpreter = root / "bin" / "python3.12"
    interpreter.parent.mkdir(parents=True)
    interpreter_payload = Path(sys.executable).read_bytes()
    interpreter.write_bytes(interpreter_payload)
    interpreter.chmod(0o700)
    root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    interpreter_fd = os.open(interpreter, os.O_RDONLY)
    parent_pid = os.getpid()
    child_pids: list[int] = []
    real_fork = os.fork
    real_write = os.write

    def recorded_fork() -> int:
        pid = real_fork()
        if pid > 0:
            child_pids.append(pid)
        return pid

    def broken_parent_write(fd: int, payload: bytes | memoryview) -> int:
        if os.getpid() == parent_pid:
            raise OSError(errno.EIO, "forced parent request failure")
        return real_write(fd, payload)

    def blocked_child_exec(_fd, _argv, _environ) -> None:
        os.read(3, 65536)
        os._exit(0)

    monkeypatch.setattr(seal_impl.os, "fork", recorded_fork)
    monkeypatch.setattr(seal_impl.os, "write", broken_parent_write)
    monkeypatch.setattr(seal_impl, "_exec_fd", blocked_child_exec)
    try:
        with pytest.raises(OSError, match="forced parent request failure"):
            seal_impl._run_isolated_module_with_interpreter_fd(
                root_fd,
                interpreter_fd,
                "okg_extension_bootstrap",
                request={
                    "schema_version": "okg.extension_runtime.bootstrap_request/v1",
                    "interpreter_member": "bin/python3.12",
                },
                program=bootstrap_verifier_program_bytes(),
                interpreter_expected=interpreter_payload,
            )
        assert len(child_pids) == 1
        with pytest.raises(ChildProcessError):
            os.waitpid(child_pids[0], os.WNOHANG)
    finally:
        os.close(interpreter_fd)
        os.close(root_fd)


def _run_bootstrap_with_child(tmp_path: Path, monkeypatch, child) -> None:
    root = tmp_path / "runtime"
    interpreter = root / "bin" / "python3.12"
    interpreter.parent.mkdir(parents=True)
    interpreter_payload = b"fixture interpreter bytes"
    interpreter.write_bytes(interpreter_payload)
    interpreter.chmod(0o700)
    root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    interpreter_fd = os.open(interpreter, os.O_RDONLY)
    monkeypatch.setattr(seal_impl, "_exec_fd", child)
    try:
        seal_impl._run_isolated_module_with_interpreter_fd(
            root_fd,
            interpreter_fd,
            "okg_extension_bootstrap",
            request={
                "schema_version": "okg.extension_runtime.bootstrap_request/v1",
                "interpreter_member": "bin/python3.12",
            },
            program=bootstrap_verifier_program_bytes(),
            interpreter_expected=interpreter_payload,
        )
    finally:
        os.close(interpreter_fd)
        os.close(root_fd)


def test_isolated_tool_refusal_reports_exit_status_and_stderr_tail(
    tmp_path: Path, monkeypatch
) -> None:
    # Issue #2352: the refusal used to say only "refused or exited".
    def failing_startup(_fd, _argv, _environ) -> None:
        os.write(
            2,
            b"Could not find platform dependent libraries <exec_prefix>\n"
            b"Fatal Python error: \x1b[31mfailed\x1b[0m\n",
        )
        os._exit(3)

    with pytest.raises(ExtensionRuntimeSealError) as refusal:
        _run_bootstrap_with_child(tmp_path, monkeypatch, failing_startup)
    assert refusal.value.code == "extension_builder_input_mismatch"
    message = str(refusal.value)
    assert message.startswith("isolated tool refused or exited: exit status 3; ")
    assert (
        "stderr tail: Could not find platform dependent libraries <exec_prefix>"
        "|Fatal Python error: ?[31mfailed?[0m"
    ) in message
    # The stable machine-readable payload stays path-free.
    assert refusal.value.payload == {
        "code": "extension_builder_input_mismatch",
        "path": "$",
    }


def test_isolated_tool_refusal_names_signal_and_missing_stderr(
    tmp_path: Path, monkeypatch
) -> None:
    def killed_child(_fd, _argv, _environ) -> None:
        os.kill(os.getpid(), signal.SIGKILL)

    with pytest.raises(ExtensionRuntimeSealError) as refusal:
        _run_bootstrap_with_child(tmp_path, monkeypatch, killed_child)
    assert str(refusal.value) == (
        "isolated tool refused or exited: killed by signal 9 (SIGKILL); no stderr"
    )


def test_isolated_tool_stderr_is_drained_and_bounded(
    tmp_path: Path, monkeypatch
) -> None:
    # More than a pipe buffer: an undrained pipe would block the child until
    # the timeout instead of letting it exit.
    def noisy_child(_fd, _argv, _environ) -> None:
        payload = b"x" * (256 * 1024) + b"END-OF-STDERR\n"
        view = memoryview(payload)
        while view:
            view = view[os.write(2, view) :]
        os._exit(1)

    with pytest.raises(ExtensionRuntimeSealError) as refusal:
        _run_bootstrap_with_child(tmp_path, monkeypatch, noisy_child)
    message = str(refusal.value)
    assert "exit status 1; stderr tail: ...x" in message
    assert message.endswith("END-OF-STDERR")
    tail = message.split("stderr tail: ", 1)[1]
    assert len(tail) <= seal_impl.MAX_CHILD_STDERR_TAIL_BYTES + len("...")


def test_isolated_tool_child_exit_before_acknowledgement_is_a_refusal(
    tmp_path: Path, monkeypatch
) -> None:
    # A child that dies after closing its result pipe must not surface as a
    # BrokenPipeError from the acknowledgement write (seen on macOS, #2352).
    parent_pid = os.getpid()
    real_waitpid = seal_impl.os.waitpid
    real_write = seal_impl.os.write
    acknowledged: list[bool] = []

    def late_waitpid(pid: int, options: int):
        if options == os.WNOHANG and not acknowledged:
            return (0, 0)
        return real_waitpid(pid, options)

    def recorded_write(fd: int, payload) -> int:
        if os.getpid() == parent_pid and bytes(payload) == b"1":
            acknowledged.append(True)
        return real_write(fd, payload)

    def dying_child(_fd, _argv, _environ) -> None:
        os.write(2, b"startup failed\n")
        os._exit(4)

    monkeypatch.setattr(seal_impl.os, "waitpid", late_waitpid)
    monkeypatch.setattr(seal_impl.os, "write", recorded_write)
    with pytest.raises(ExtensionRuntimeSealError) as refusal:
        _run_bootstrap_with_child(tmp_path, monkeypatch, dying_child)
    assert acknowledged == [True]
    assert str(refusal.value) == (
        "isolated tool refused or exited: exit status 4; stderr tail: startup failed"
    )


def test_isolated_tool_child_exit_before_request_is_a_refusal(
    tmp_path: Path, monkeypatch
) -> None:
    # Review of #2855: a child that exits before the parent sends the request
    # made the request write raise a raw BrokenPipeError and lost the stderr.
    parent_pid = os.getpid()
    child_pids: list[int] = []
    pipes: list[tuple[int, int]] = []
    real_fork = seal_impl.os.fork
    real_pipe = seal_impl.os.pipe
    real_write = seal_impl.os.write

    def recorded_fork() -> int:
        pid = real_fork()
        if pid > 0:
            child_pids.append(pid)
        return pid

    def recorded_pipe() -> tuple[int, int]:
        pair = real_pipe()
        pipes.append(pair)
        return pair

    def write_after_child_closed_request(fd: int, payload) -> int:
        if os.getpid() == parent_pid and bytes(payload)[:1] == b"{":
            # The child closes its request end before writing stderr, so
            # once stderr is readable this write meets EPIPE (no reaping).
            stderr_read = pipes[3][0]
            select.select([stderr_read], [], [], 10.0)
        return real_write(fd, payload)

    def early_exit(_fd, _argv, _environ) -> None:
        os.close(3)
        os.write(2, b"interpreter missing\n")
        os._exit(5)

    monkeypatch.setattr(seal_impl.os, "fork", recorded_fork)
    monkeypatch.setattr(seal_impl.os, "pipe", recorded_pipe)
    monkeypatch.setattr(seal_impl.os, "write", write_after_child_closed_request)
    with pytest.raises(ExtensionRuntimeSealError) as refusal:
        _run_bootstrap_with_child(tmp_path, monkeypatch, early_exit)
    assert refusal.value.code == "extension_builder_input_mismatch"
    assert str(refusal.value) == (
        "isolated tool closed its request before reading it: exit status 5; "
        "stderr tail: interpreter missing"
    )
    with pytest.raises(ChildProcessError):
        os.waitpid(child_pids[0], os.WNOHANG)


def test_stderr_tail_marks_truncation_only_when_bytes_were_dropped() -> None:
    limit = seal_impl.MAX_CHILD_STDERR_TAIL_BYTES
    exact = seal_impl._StderrTail()
    exact.add(b"a" * limit)
    assert exact.describe(None) == "exit status unknown; stderr tail: " + "a" * limit
    over = seal_impl._StderrTail()
    over.add(b"b" + b"a" * limit)
    assert over.describe(None) == (
        "exit status unknown; stderr tail: ..." + "a" * limit
    )


def test_stderr_drain_stops_at_its_deadline() -> None:
    # A writer that never stops must not hold the parent in the drain loop.
    read_fd, write_fd = os.pipe()
    os.set_blocking(read_fd, False)
    stop = threading.Event()

    def flood() -> None:
        while not stop.is_set():
            try:
                os.write(write_fd, b"x" * 4096)
            except OSError:
                return

    writer = threading.Thread(target=flood, daemon=True)
    writer.start()
    tail = seal_impl._StderrTail()
    try:
        started = time.monotonic()
        assert seal_impl._drain_stderr_tail(
            read_fd, tail, deadline=started + 0.2
        ) is True
        assert time.monotonic() - started < 5.0
        assert len(tail.data) <= seal_impl.MAX_CHILD_STDERR_TAIL_BYTES
    finally:
        stop.set()
        os.close(read_fd)
        writer.join(timeout=5)
        os.close(write_fd)


def test_describe_child_exit_without_status() -> None:
    assert seal_impl._describe_child_exit(None, b"  \n") == (
        "exit status unknown; no stderr"
    )


STANDALONE_RUNTIME_PREFIX_ENV = "OKG_TEST_STANDALONE_RUNTIME_PREFIX"


def _standalone_runtime_prefix() -> Path | None:
    value = os.environ.get(STANDALONE_RUNTIME_PREFIX_ENV)
    return Path(value) if value else None


# Issue #2352: seal against a REAL python-build-standalone runtime. Point
# OKG_TEST_STANDALONE_RUNTIME_PREFIX at an extracted install_only prefix of the
# enrolled release (tag 20260814, CPython 3.12.14) for this host's platform.
# The producer proof over it is signed with fixture keys, so this proves the
# seal's launch and build path, not a producer's reproducible build.
@pytest.mark.skipif(
    not _descriptor_exec_supported(),
    reason="host lacks fexecve/execveat descriptor execution (Linux only)",
)
@pytest.mark.skipif(
    _standalone_runtime_prefix() is None,
    reason=(
        "no standalone runtime: set OKG_TEST_STANDALONE_RUNTIME_PREFIX to an "
        "extracted python-build-standalone 20260814 CPython 3.12.14 "
        "install_only prefix for this platform"
    ),
)
def test_sealing_executes_selected_builder_and_seals_exact_tree(
    tmp_path: Path,
) -> None:
    prefix_source = _standalone_runtime_prefix()
    assert prefix_source is not None
    assert (prefix_source / "bin" / "python3.12").is_file(), prefix_source
    profile = release_runtime._host_platform_profile()
    _, distribution = _producer_proof(
        tmp_path / "producer-proof",
        prefix_source=prefix_source,
        platform_key=(
            profile["os"],
            profile["architecture"],
            profile["libc"],
            profile["python_abi"],
        ),
    )
    inspection_prefix = tmp_path / "verified-producer-runtime"
    distribution.copy_prefix_to(inspection_prefix)
    interpreter = inspection_prefix / "bin" / "python3.12"
    description = release_runtime._describe_interpreter(
        interpreter,
        cwd=tmp_path,
        environ={
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PYTHONDONTWRITEBYTECODE": "1",
        },
    )
    release, manifest_name, platform_identity = _sealed_runtime_release(
        tmp_path / "runtime-release",
        distribution=distribution,
        python_description=dict(description),
    )
    with release.extension_runtime_authority() as authority:
        authority_receipt = authority.receipt
        toolchain = select_runtime_toolchain(
            authority,
            manifest_relative_path=manifest_name,
            platform_identity=platform_identity,
        )
    assert authority_receipt["schema_version"] == (
        "okg.sealed_release.runtime_authority/v2"
    )
    authority_interpreter = authority_receipt["interpreter"]
    assert isinstance(authority_interpreter, dict)
    assert authority_interpreter["relative_name"] == "bin/python3.12"
    assert authority_receipt["prefix_configuration"] == {
        "profile": "standalone-base-prefix/v1",
        "pyvenv_config": None,
    }
    assert "pyvenv_config" not in toolchain.component_members
    writable_member = toolchain.component_members["builder"].runtime_relative_name
    fixture = _plan_fixture(tmp_path / "package-plan")
    fixture.toolchain.close()
    fixture.toolchain = toolchain
    handle = None
    try:
        plan = _plan(fixture)
        handle = seal_extension_runtime(
            plan,
            fixture.package,
            toolchain,
            destination=(tmp_path.resolve() / "sealed-runtime"),
            environ={},
        )
        assert verify_sealed_runtime(handle) is handle
        assert not fixture.sentinel.exists()
        assert handle.receipt.builder_execution_receipt_digest.digest != _ZERO_DIGEST
        assert handle.receipt.installed_tree
        assert {value.name for value in handle.receipt.installed_distributions} == {
            "neutral-dependency",
            "neutral-extension",
            "okg-extension-child",
            "okg-extension-sdk",
        }

        original_receipt = handle.receipt
        changed_receipt = replace(
            original_receipt,
            sealed_runtime_digest=CanonicalRecordRef(
                original_receipt.sealed_runtime_digest.schema_version, _ZERO_DIGEST
            ),
        )
        with pytest.raises(AttributeError):
            handle.receipt = changed_receipt  # type: ignore[misc]
        handle._receipt = changed_receipt
        with pytest.raises(ExtensionRuntimeSealError) as stale:
            verify_sealed_runtime(handle)
        assert stale.value.code == "extension_sealed_runtime_mismatch"
        handle._receipt = original_receipt

        os.chmod(handle.runtime_locator / writable_member, 0o644)
        with pytest.raises(ExtensionRuntimeSealError) as writable:
            verify_sealed_runtime(handle)
        assert writable.value.code == "extension_runtime_writable"
    finally:
        if handle is not None:
            handle.close()
        fixture.close()



def test_selected_toolchain_refuses_same_bytes_new_inode(tmp_path: Path) -> None:
    toolchain = _toolchain(tmp_path)
    try:
        selected = toolchain.component_members["builder"].release_relative_name
        target = tmp_path / "sealed-release" / selected
        replacement = target.with_name(target.name + ".replacement")
        replacement.write_bytes(target.read_bytes())
        os.replace(replacement, target)
        with pytest.raises(ExtensionRuntimeSealError) as refusal:
            verify_runtime_toolchain(toolchain)
        assert refusal.value.code == "extension_runtime_toolchain_mismatch"
    finally:
        toolchain.close()


def test_toolchain_selection_refuses_bare_sealed_release(tmp_path: Path) -> None:
    release = SealedRelease(
        "sha256:" + "1" * 64,
        tmp_path,
        SealedReleaseInputs(
            "7" * 40,
            "sha256:" + "8" * 64,
            "sha256:" + "9" * 64,
            "sha256:" + "a" * 64,
            "sha256:" + "b" * 64,
            None,
        ),
        "sha256:" + "2" * 64,
        "2026-08-22T00:00:00+00:00",
    )

    with pytest.raises(ExtensionRuntimeSealError) as refusal:
        select_runtime_toolchain(
            release,  # type: ignore[arg-type]
            manifest_relative_path="repo/toolchain/manifest.json",
            platform_identity=_PLATFORM,
        )

    assert refusal.value.code == "extension_runtime_toolchain_mismatch"


@pytest.mark.parametrize(
    "arm",
    ["public_sdk", "python_abi", "interpreter", "platform", "builder"],
)
def test_planning_refuses_changed_runtime_identity(tmp_path: Path, arm: str) -> None:
    fixture = _plan_fixture(tmp_path)
    original = fixture.toolchain.identity
    try:
        if arm == "python_abi":
            changed = replace(original, python_abi="cp313")
        elif arm == "platform":
            changed = replace(
                original,
                platform_identity={**original.platform_identity, "os": "changed-os"},
            )
        else:
            field_name = {
                "public_sdk": "public_sdk_raw_blob",
                "interpreter": "interpreter_raw_blob",
                "builder": "builder_raw_blob",
            }[arm]
            previous = getattr(original, field_name)
            changed = replace(
                original,
                **{field_name: RawBlobRef(_ZERO_DIGEST, previous.size)},
            )
        object.__setattr__(fixture.toolchain, "identity", changed)
        with pytest.raises(ExtensionRuntimeSealError) as refusal:
            _plan(fixture)
        assert refusal.value.code in {
            "extension_builder_identity_mismatch",
            "extension_runtime_toolchain_mismatch",
        }
    finally:
        object.__setattr__(fixture.toolchain, "identity", original)
        fixture.close()


def test_builder_is_executable_identity_not_declarative_metadata() -> None:
    source = builder_program_bytes()
    compile(source, "<verified-extension-builder>", "exec")
    assert source.startswith(b"import ")
    assert b"staging_root_fd_slot" in source
    assert b"read_fd_slot" in source
    assert b"pip" not in source.lower()
    assert raw_blob_digest(source) != raw_blob_digest(
        (_FIXTURE_ROOT / "builder-program.json").read_bytes()
    )


@pytest.mark.parametrize("relative_name", ["a//b", "a/", "./a", "a/../b"])
def test_runtime_relative_aliases_refuse(relative_name: str) -> None:
    with pytest.raises(ExtensionRuntimeSealError) as refusal:
        seal_impl._safe_relative_name(relative_name, "fixture-relative-name")
    assert refusal.value.code == "extension_runtime_toolchain_mismatch"


def test_conflicting_selection_raw_identities_refuse() -> None:
    with pytest.raises(plan_impl.ExtensionPackageV2Error) as refusal:
        PackageObjectSelection.from_value(
            {
                "asset_ref": "neutral-fixture:wheel",
                "raw_blob_digest": "sha256:" + "c" * 64,
                "raw_blob": RawBlobRef(
                    "sha256:" + "d" * 64,
                    1,
                ).to_record(),
                "role": "extension-wheel",
            },
            role="extension-wheel",
        )
    assert refusal.value.code == "extension_package_object_mismatch"

    default_role = PackageObjectSelection.from_value(
        {
            "asset_ref": "neutral-fixture:descriptor",
            "raw_blob_digest": "sha256:" + "e" * 64,
        }
    )
    assert default_role.role == "object"


@pytest.mark.parametrize(
    ("member_name", "payload"),
    [
        ("editable.pth", b"/authoring-checkout\n"),
        ("editable-1.0.0.dist-info/direct_url.json", b'{"editable":true}'),
    ],
)
def test_editable_and_active_wheel_members_refuse(
    member_name: str, payload: bytes
) -> None:
    wheel = _wheel_bytes("editable", "1.0.0", {member_name: payload})
    with pytest.raises(ExtensionRuntimeSealError) as refusal:
        seal_impl._wheel_install(wheel, None)
    assert refusal.value.code == "extension_editable_install_refused"


def test_retained_root_reads_survive_path_replacement_and_refuse_symlinks(
    tmp_path: Path,
) -> None:
    tmp_path = tmp_path.resolve()
    root = tmp_path / "authority-root"
    root.mkdir()
    root_fd = seal_impl._open_absolute_directory(
        root, code="extension_runtime_unsealed", label="fixture-root"
    )
    try:
        seal_impl._write_file_at(
            root_fd, "nested/value.txt", b"original", executable=False
        )
        moved = tmp_path / "moved-authority-root"
        root.rename(moved)
        root.mkdir()
        (root / "nested").symlink_to(moved / "nested")
        assert (
            seal_impl._read_regular_at(
                root_fd,
                "nested/value.txt",
                code="extension_installed_tree_mismatch",
            )
            == b"original"
        )
        replacement_fd = seal_impl._open_absolute_directory(
            root, code="extension_runtime_unsealed", label="replacement-root"
        )
        try:
            with pytest.raises(ExtensionRuntimeSealError) as refusal:
                seal_impl._read_regular_at(
                    replacement_fd,
                    "nested/value.txt",
                    code="extension_installed_tree_mismatch",
                )
            assert refusal.value.code == "extension_runtime_symlink_refused"
            assert str(tmp_path) not in str(refusal.value.payload)
        finally:
            os.close(replacement_fd)
    finally:
        os.close(root_fd)


@pytest.mark.parametrize(
    "key",
    [
        "PIP_INDEX_URL",
        "PYTHONPATH",
        "PYTHONUSERBASE",
        "UV_CACHE_DIR",
        "VIRTUAL_ENV",
    ],
)
def test_ambient_package_and_interpreter_authority_refuses(key: str) -> None:
    with pytest.raises(ExtensionRuntimeSealError) as refusal:
        seal_impl._assert_empty_authority_environment({key: "forbidden"})
    assert refusal.value.code == "extension_ambient_package_source_refused"
