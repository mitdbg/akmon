"""Verified, descriptor-anchored sealing for installed public extensions.

This module owns only plan/seal.  It selects exact toolchain bytes from an
already verified sealed release, materializes one isolated Python layout, runs
the selected builder in that copied interpreter, independently verifies the
builder's output, and retains the sealed runtime directory descriptor.  It
does not launch extension code or implement broker, SDK-proxy, lifecycle, or
publication behavior.
"""

from __future__ import annotations

import base64
import binascii
import csv
import ctypes
from dataclasses import dataclass, field, replace
import errno
import fcntl
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import resource
import select
import signal
import stat
import tempfile
import threading
import time
from types import MappingProxyType
from collections.abc import Mapping, Sequence
from typing import Protocol
import unicodedata
import zipfile

from okg.substrate.errors import SealedReleaseInvalid
from okg.substrate.extension_package_v2 import (
    CanonicalRecordRef,
    PackagePlanV2,
    RawBlobRef,
    RuntimeDistributionProjection,
    RuntimePackageProjections,
    RuntimeToolchainIdentity,
    WheelProjection,
    canonical_json_bytes,
    canonical_record_digest,
    raw_blob_digest,
    verify_package_plan_v2,
)
from okg.substrate.sealed_release import VerifiedRelocatableRuntimeAuthority


TOOLCHAIN_SCHEMA = "okg.extension_runtime.toolchain/v2"
STDLIB_SCHEMA = "okg.extension_runtime.stdlib_manifest/v1"
RUNTIME_LAYOUT_SCHEMA = "okg.extension_runtime.layout/v2"
RUNTIME_DISTRIBUTION_TREE_SCHEMA = "okg.extension_runtime.runtime_distribution_tree/v1"
BUILDER_SCHEMA = "okg.extension_runtime.builder/v1"
BUILDER_INPUTS_SCHEMA = "okg.extension_runtime.builder_inputs/v1"
BUILDER_REQUEST_SCHEMA = "okg.extension_runtime.builder_request/v1"
BUILDER_RESULT_SCHEMA = "okg.extension_runtime.builder_result/v1"
BUILDER_EXECUTION_SCHEMA = "okg.extension_runtime.builder_execution_receipt/v1"
BOOTSTRAP_PROOF_SCHEMA = "okg.extension_runtime.bootstrap_proof/v1"
BOOTSTRAP_REQUEST_SCHEMA = "okg.extension_runtime.bootstrap_request/v1"
INSTALLED_TREE_SCHEMA = "okg.extension_runtime.installed_tree/v1"
WHEEL_RECORD_SCHEMA = "okg.extension_runtime.wheel_record/v1"
SEALED_RUNTIME_SCHEMA = "okg.extension_runtime.sealed/v1"
SEAL_RECEIPT_SCHEMA = "okg.extension_runtime.seal_receipt/v1"

INSTALLER_ALGORITHM_VERSION = "wheel-record-unpack/v3"
WHEEL_INSTALL_SCHEME = "abi-site-packages/v1"
TREE_NORMALIZATION_PROFILE = "nfc-posix-0444-0555-dirfd/v1"
MAX_WHEEL_BYTES = 128 * 1024 * 1024
MAX_WHEEL_MEMBERS = 10_000
MAX_WHEEL_EXPANDED_BYTES = 512 * 1024 * 1024
MAX_CHILD_RESULT_BYTES = 8 * 1024 * 1024
CHILD_TIMEOUT_SECONDS = 30.0
MAX_CHILD_STDERR_TAIL_BYTES = 2048
# The isolated child executes descriptor 6; this name resolves to that file.
CHILD_ARGV0 = "/proc/self/fd/6"

_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_PYTHON_ABI_RE = re.compile(r"^cp(?P<major>[0-9])(?P<minor>[0-9]{1,2})$")
_COMPONENT_NAMES = (
    "interpreter",
    "public_sdk",
    "child_host",
    "bootstrap_verifier",
    "builder",
    "broker",
    "containment_launcher",
)
_AMBIENT_PACKAGE_KEYS = frozenset(
    {
        "PIP_CONFIG_FILE",
        "PIP_EXTRA_INDEX_URL",
        "PIP_FIND_LINKS",
        "PIP_INDEX_URL",
        "PIP_NO_INDEX",
        "PIP_REQUIRE_VIRTUALENV",
        "PIP_TRUSTED_HOST",
        "PYTHONHOME",
        "PYTHONPATH",
        "PYTHONUSERBASE",
        "UV_CACHE_DIR",
        "UV_EXTRA_INDEX_URL",
        "UV_INDEX",
        "UV_INDEX_URL",
        "VIRTUAL_ENV",
    }
)


class ExtensionRuntimeSealError(ValueError):
    """Stable, path-redacted refusal from plan/seal."""

    def __init__(
        self, code: str, message: str | None = None, *, path: str = "$"
    ) -> None:
        if (
            not isinstance(code, str)
            or not code
            or any(char.isspace() for char in code)
        ):
            raise ValueError("error code must be a stable token")
        self.code = code
        self.path = path
        super().__init__(message or code)

    @property
    def payload(self) -> dict[str, str]:
        return {"code": self.code, "path": self.path}


def _refuse(code: str, message: str, *, path: str = "$") -> ExtensionRuntimeSealError:
    return ExtensionRuntimeSealError(code, message, path=path)


def _mapping(value: object, path: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise _refuse(
            "extension_runtime_toolchain_mismatch",
            "expected a closed object",
            path=path,
        )
    return value


def _exact_keys(value: Mapping[str, object], keys: set[str], path: str) -> None:
    if set(value) != keys:
        raise _refuse(
            "extension_runtime_toolchain_mismatch",
            "object fields do not match the schema",
            path=path,
        )


def _strict_json(payload: bytes, path: str) -> Mapping[str, object]:
    try:

        def pairs(values: list[tuple[str, object]]) -> dict[str, object]:
            result: dict[str, object] = {}
            for key, value in values:
                if key in result:
                    raise ValueError("duplicate key")
                result[key] = value
            return result

        value = json.loads(
            payload.decode("utf-8", errors="strict"),
            object_pairs_hook=pairs,
            parse_constant=lambda _value: (_ for _ in ()).throw(
                ValueError("non-finite number")
            ),
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise _refuse(
            "extension_runtime_toolchain_mismatch",
            "record is not strict UTF-8 JSON",
            path=path,
        ) from exc
    result = _mapping(value, path)
    if payload != canonical_json_bytes(result):
        raise _refuse(
            "extension_runtime_toolchain_mismatch",
            "record is not canonical JSON",
            path=path,
        )
    return result


def _safe_relative_name(value: object, path: str) -> str:
    if not isinstance(value, str) or not value or "\\" in value or "\x00" in value:
        raise _refuse(
            "extension_runtime_toolchain_mismatch",
            "relative name is invalid",
            path=path,
        )
    if unicodedata.normalize("NFC", value) != value:
        raise _refuse(
            "extension_runtime_toolchain_mismatch",
            "relative name is not NFC",
            path=path,
        )
    components = value.split("/")
    candidate = PurePosixPath(value)
    if (
        candidate.is_absolute()
        or any(part in {"", ".", ".."} for part in components)
        or candidate.as_posix() != value
    ):
        raise _refuse(
            "extension_runtime_toolchain_mismatch",
            "relative name escapes its authority root",
            path=path,
        )
    return candidate.as_posix()


def _raw_ref(value: object, path: str) -> RawBlobRef:
    try:
        return RawBlobRef.from_value(value, path=path)
    except Exception as exc:
        raise _refuse(
            "extension_runtime_toolchain_mismatch",
            "raw blob identity is invalid",
            path=path,
        ) from exc


def _canonical_ref(value: object, schema: str, path: str) -> CanonicalRecordRef:
    raw = _mapping(value, path)
    _exact_keys(raw, {"digest_domain", "schema_version", "digest"}, path)
    if (
        raw.get("digest_domain") != "canonical-json/v1"
        or raw.get("schema_version") != schema
    ):
        raise _refuse(
            "extension_runtime_toolchain_mismatch",
            "canonical digest domain is invalid",
            path=path,
        )
    digest = raw.get("digest")
    if not isinstance(digest, str) or not _DIGEST_RE.fullmatch(digest):
        raise _refuse(
            "extension_runtime_toolchain_mismatch",
            "canonical digest is invalid",
            path=path,
        )
    return CanonicalRecordRef(schema, digest)


def _authority_receipt_ref(
    receipt: Mapping[str, object], name: str, schema: str
) -> CanonicalRecordRef:
    value = receipt.get(name)
    if not isinstance(value, str) or not _DIGEST_RE.fullmatch(value):
        raise _refuse(
            "extension_runtime_toolchain_mismatch",
            "runtime authority canonical reference is missing or invalid",
            path=f"runtime_authority.receipt.{name}",
        )
    return CanonicalRecordRef(schema, value)


def _directory_flags() -> int:
    return os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)


def _regular_read_flags() -> int:
    return os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)


def _stable_file_token(
    value: os.stat_result,
) -> tuple[int, int, int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
        value.st_nlink,
    )


def _open_absolute_directory(path: Path, *, code: str, label: str) -> int:
    if not path.is_absolute() or any(part == ".." for part in path.parts):
        raise _refuse(code, "authority root is not absolute and normalized", path=label)
    current = os.open(path.anchor, _directory_flags())
    try:
        for component in path.relative_to(path.anchor).parts:
            next_fd = os.open(component, _directory_flags(), dir_fd=current)
            os.close(current)
            current = next_fd
        return current
    except OSError as exc:
        os.close(current)
        refusal = (
            "extension_runtime_symlink_refused"
            if exc.errno in {errno.ELOOP, errno.ENOTDIR}
            else code
        )
        raise _refuse(
            refusal,
            "authority root could not be opened without following links",
            path=label,
        ) from exc


def _open_parent_at(root_fd: int, relative_name: str, *, code: str) -> tuple[int, str]:
    safe = _safe_relative_name(relative_name, relative_name)
    parts = PurePosixPath(safe).parts
    root_device = os.fstat(root_fd).st_dev
    current = os.dup(root_fd)
    try:
        for component in parts[:-1]:
            next_fd = os.open(component, _directory_flags(), dir_fd=current)
            if os.fstat(next_fd).st_dev != root_device:
                os.close(next_fd)
                os.close(current)
                raise _refuse(
                    code, "authority traversal crossed a mount boundary", path=safe
                )
            os.close(current)
            current = next_fd
        return current, parts[-1]
    except OSError as exc:
        os.close(current)
        refusal = (
            "extension_runtime_symlink_refused"
            if exc.errno in {errno.ELOOP, errno.ENOTDIR}
            else code
        )
        raise _refuse(
            refusal,
            "authority member parent changed",
            path=safe,
        ) from exc


def _read_regular_identity_at(
    root_fd: int, relative_name: str, *, code: str
) -> tuple[bytes, tuple[int, int, int, int, int, int, int]]:
    parent_fd, leaf = _open_parent_at(root_fd, relative_name, code=code)
    try:
        member_fd = os.open(leaf, _regular_read_flags(), dir_fd=parent_fd)
    except OSError as exc:
        os.close(parent_fd)
        refusal = (
            "extension_runtime_symlink_refused" if exc.errno == errno.ELOOP else code
        )
        raise _refuse(
            refusal, "authority member could not be opened", path=relative_name
        ) from exc
    try:
        before = os.fstat(member_fd)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_dev != os.fstat(root_fd).st_dev
        ):
            raise _refuse(
                code,
                "authority member is not a single regular file",
                path=relative_name,
            )
        chunks: list[bytes] = []
        while True:
            chunk = os.read(member_fd, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        after = os.fstat(member_fd)
        if _stable_file_token(before) != _stable_file_token(after):
            raise _refuse(
                code, "authority member changed while read", path=relative_name
            )
        payload = b"".join(chunks)
        if len(payload) != before.st_size:
            raise _refuse(code, "authority member size changed", path=relative_name)
        return payload, _stable_file_token(after)
    finally:
        os.close(member_fd)
        os.close(parent_fd)


def _read_regular_at(root_fd: int, relative_name: str, *, code: str) -> bytes:
    return _read_regular_identity_at(root_fd, relative_name, code=code)[0]


def _mkdirs_at(root_fd: int, relative_directory: str) -> None:
    safe = _safe_relative_name(relative_directory, relative_directory)
    root_device = os.fstat(root_fd).st_dev
    current = os.dup(root_fd)
    try:
        for component in PurePosixPath(safe).parts:
            try:
                os.mkdir(component, 0o700, dir_fd=current)
            except FileExistsError:
                # The no-follow open and same-device check below validate it.
                pass
            next_fd = os.open(component, _directory_flags(), dir_fd=current)
            if os.fstat(next_fd).st_dev != root_device:
                os.close(next_fd)
                raise _refuse(
                    "extension_runtime_unsealed",
                    "runtime directory crossed a mount boundary",
                    path=safe,
                )
            os.close(current)
            current = next_fd
    except OSError as exc:
        refusal = (
            "extension_runtime_symlink_refused"
            if exc.errno in {errno.ELOOP, errno.ENOTDIR}
            else "extension_runtime_unsealed"
        )
        raise _refuse(
            refusal, "runtime directory could not be created", path=safe
        ) from exc
    finally:
        os.close(current)


def _write_file_at(
    root_fd: int, relative_name: str, payload: bytes, *, executable: bool
) -> None:
    safe = _safe_relative_name(relative_name, relative_name)
    parent = PurePosixPath(safe).parent.as_posix()
    if parent != ".":
        _mkdirs_at(root_fd, parent)
    parent_fd, leaf = _open_parent_at(root_fd, safe, code="extension_runtime_unsealed")
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | os.O_CLOEXEC
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        fd = os.open(leaf, flags, 0o700 if executable else 0o600, dir_fd=parent_fd)
        try:
            view = memoryview(payload)
            while view:
                written = os.write(fd, view)
                if written <= 0:
                    raise OSError("short write")
                view = view[written:]
            os.fsync(fd)
            observed = os.fstat(fd)
            if (
                not stat.S_ISREG(observed.st_mode)
                or observed.st_nlink != 1
                or observed.st_dev != os.fstat(root_fd).st_dev
            ):
                raise _refuse(
                    "extension_runtime_unsealed",
                    "runtime member is not a single regular file",
                    path=safe,
                )
        finally:
            os.close(fd)
    except FileExistsError as exc:
        raise _refuse(
            "extension_installed_name_collision",
            "runtime member already exists",
            path=safe,
        ) from exc
    except OSError as exc:
        refusal = (
            "extension_runtime_symlink_refused"
            if exc.errno == errno.ELOOP
            else "extension_runtime_unsealed"
        )
        raise _refuse(
            refusal, "runtime member could not be written", path=safe
        ) from exc
    finally:
        os.close(parent_fd)


def _open_regular_fd_at(root_fd: int, relative_name: str, *, code: str) -> int:
    parent_fd, leaf = _open_parent_at(root_fd, relative_name, code=code)
    try:
        fd = os.open(leaf, _regular_read_flags(), dir_fd=parent_fd)
        observed = os.fstat(fd)
        if (
            not stat.S_ISREG(observed.st_mode)
            or observed.st_nlink != 1
            or observed.st_dev != os.fstat(root_fd).st_dev
        ):
            os.close(fd)
            raise _refuse(
                code, "runtime executable is not a regular file", path=relative_name
            )
        return fd
    finally:
        os.close(parent_fd)


@dataclass(frozen=True, slots=True)
class ToolchainComponent:
    release_relative_name: str
    runtime_relative_name: str
    raw_blob: RawBlobRef
    executable: bool

    def runtime_record(self) -> dict[str, object]:
        return {
            "relative_name": self.runtime_relative_name,
            "mode": 0o555 if self.executable else 0o444,
            "raw_blob": self.raw_blob.to_record(),
        }


@dataclass(frozen=True, slots=True)
class ToolchainMember:
    release_relative_name: str
    relative_name: str
    raw_blob: RawBlobRef
    executable: bool = False

    def to_record(self) -> dict[str, object]:
        return {
            "relative_name": self.relative_name,
            "mode": 0o555 if self.executable else 0o444,
            "raw_blob": self.raw_blob.to_record(),
        }


@dataclass(frozen=True, slots=True)
class InstalledTreeEntry:
    relative_name: str
    kind: str
    mode: int
    size: int
    raw_blob: RawBlobRef | None
    owner_distribution: str
    wheel_record_entry_digest: CanonicalRecordRef | None = None

    def to_record(self) -> dict[str, object]:
        return {
            "relative_name": self.relative_name,
            "kind": self.kind,
            "mode": self.mode,
            "size": self.size,
            "raw_blob": None if self.raw_blob is None else self.raw_blob.to_record(),
            "owner_distribution": self.owner_distribution,
            "wheel_record_entry_digest": (
                None
                if self.wheel_record_entry_digest is None
                else self.wheel_record_entry_digest.to_record()
            ),
        }


@dataclass(frozen=True, slots=True)
class InstalledDistribution:
    name: str
    version: str
    wheel_raw_blob: RawBlobRef
    wheel_record_manifest_digest: CanonicalRecordRef
    import_roots: tuple[str, ...]

    def to_record(self) -> dict[str, object]:
        return {
            "name": self.name,
            "version": self.version,
            "wheel_raw_blob": self.wheel_raw_blob.to_record(),
            "wheel_record_manifest_digest": self.wheel_record_manifest_digest.to_record(),
            "import_roots": list(self.import_roots),
        }


@dataclass(frozen=True, slots=True)
class _WheelInstall:
    distribution: InstalledDistribution
    files: tuple[tuple[str, bytes, CanonicalRecordRef], ...]


def _archive_read(archive: zipfile.ZipFile, name: str) -> bytes:
    try:
        return archive.read(name)
    except (zipfile.BadZipFile, KeyError, OSError, RuntimeError) as exc:
        raise _refuse(
            "extension_dependency_closure_invalid",
            "wheel member cannot be read",
            path=name,
        ) from exc


def _wheel_member_name(name: str) -> str:
    source = _safe_relative_name(name, "wheel.member")
    parts = PurePosixPath(source).parts
    for index, part in enumerate(parts):
        if part.endswith(".data"):
            if index + 2 >= len(parts) or parts[index + 1] not in {
                "purelib",
                "platlib",
            }:
                raise _refuse(
                    "extension_editable_install_refused",
                    "wheel data scheme is not admitted",
                    path=source,
                )
            source = PurePosixPath(*parts[index + 2 :]).as_posix()
            break
    lowered = source.casefold()
    if (
        lowered.endswith((".pth", ".egg-link", ".pyc"))
        or "__pycache__" in PurePosixPath(source).parts
        or PurePosixPath(source).name.casefold()
        in {"sitecustomize.py", "usercustomize.py"}
        or "direct_url.json" in lowered
    ):
        raise _refuse(
            "extension_editable_install_refused",
            "wheel contains executable or editable install metadata",
            path=source,
        )
    return source


def _wheel_install(payload: bytes, expected: WheelProjection | None) -> _WheelInstall:
    if len(payload) > MAX_WHEEL_BYTES:
        raise _refuse(
            "extension_dependency_closure_invalid", "wheel exceeds the byte limit"
        )
    wheel_blob = RawBlobRef(raw_blob_digest(payload), len(payload))
    if expected is not None and wheel_blob != expected.selection.raw_blob_ref:
        raise _refuse(
            "extension_package_object_mismatch",
            "wheel bytes changed",
            path=expected.selection.asset_ref,
        )
    try:
        archive = zipfile.ZipFile(io.BytesIO(payload))
    except (zipfile.BadZipFile, OSError) as exc:
        raise _refuse(
            "extension_dependency_closure_invalid", "wheel is not a valid ZIP"
        ) from exc
    with archive:
        infos = archive.infolist()
        if (
            len(infos) > MAX_WHEEL_MEMBERS
            or sum(item.file_size for item in infos) > MAX_WHEEL_EXPANDED_BYTES
        ):
            raise _refuse(
                "extension_dependency_closure_invalid", "wheel expansion limit exceeded"
            )
        names = [item.filename for item in infos if not item.is_dir()]
        if len(names) != len(set(names)):
            raise _refuse(
                "extension_installed_name_collision", "wheel member names collide"
            )
        for info in infos:
            mode = info.external_attr >> 16
            if info.is_dir():
                continue
            _safe_relative_name(info.filename, "wheel.member")
            if stat.S_ISLNK(mode) or stat.S_IFMT(mode) not in {0, stat.S_IFREG}:
                raise _refuse(
                    "extension_runtime_symlink_refused",
                    "wheel contains a non-regular member",
                    path=info.filename,
                )
            if info.flag_bits & 0x1 or info.compress_type not in {
                zipfile.ZIP_STORED,
                zipfile.ZIP_DEFLATED,
            }:
                raise _refuse(
                    "extension_dependency_closure_invalid",
                    "wheel member encoding is not admitted",
                    path=info.filename,
                )
        metadata_names = [
            name for name in names if name.endswith(".dist-info/METADATA")
        ]
        record_names = [name for name in names if name.endswith(".dist-info/RECORD")]
        if len(metadata_names) != 1 or len(record_names) != 1:
            raise _refuse(
                "extension_dependency_closure_invalid",
                "wheel metadata or RECORD is not unique",
            )
        try:
            metadata = _archive_read(archive, metadata_names[0]).decode(
                "utf-8", errors="strict"
            )
        except UnicodeDecodeError as exc:
            raise _refuse(
                "extension_dependency_closure_invalid", "wheel METADATA is not UTF-8"
            ) from exc
        headers: dict[str, str] = {}
        for line in metadata.splitlines():
            if ":" in line:
                key, value = line.split(":", 1)
                headers.setdefault(key.casefold(), value.strip())
        name = headers.get("name")
        version = headers.get("version")
        if not name or not version:
            raise _refuse(
                "extension_dependency_closure_invalid", "wheel Name/Version is missing"
            )
        normalized_name = re.sub(r"[-_.]+", "-", name).casefold()
        record_name = record_names[0]
        try:
            rows = list(
                csv.reader(
                    io.StringIO(
                        _archive_read(archive, record_name).decode(
                            "utf-8", errors="strict"
                        )
                    ),
                    strict=True,
                )
            )
        except (UnicodeDecodeError, csv.Error) as exc:
            raise _refuse(
                "extension_dependency_closure_invalid", "wheel RECORD is invalid"
            ) from exc
        records: dict[str, RawBlobRef | None] = {}
        for row in rows:
            if len(row) != 3:
                raise _refuse(
                    "extension_dependency_closure_invalid",
                    "wheel RECORD row is invalid",
                )
            member_name, digest_field, size_field = row
            if member_name in records:
                raise _refuse(
                    "extension_dependency_closure_invalid",
                    "wheel RECORD is duplicated",
                    path=member_name,
                )
            if member_name == record_name:
                if digest_field or size_field:
                    raise _refuse(
                        "extension_dependency_closure_invalid",
                        "wheel RECORD must leave itself unhashed",
                    )
                records[member_name] = None
                continue
            if member_name not in names or not digest_field.startswith("sha256="):
                raise _refuse(
                    "extension_dependency_closure_invalid",
                    "wheel RECORD names an invalid member",
                    path=member_name,
                )
            member = _archive_read(archive, member_name)
            try:
                decoded = base64.urlsafe_b64decode(
                    digest_field[7:] + "=" * (-len(digest_field[7:]) % 4)
                )
            except (ValueError, binascii.Error) as exc:
                raise _refuse(
                    "extension_dependency_closure_invalid",
                    "wheel RECORD digest is invalid",
                    path=member_name,
                ) from exc
            blob = RawBlobRef(raw_blob_digest(member), len(member))
            if decoded != bytes.fromhex(blob.digest[7:]) or size_field != str(
                blob.size
            ):
                raise _refuse(
                    "extension_dependency_closure_invalid",
                    "wheel RECORD bytes changed",
                    path=member_name,
                )
            records[member_name] = blob
        if set(records) != set(names):
            raise _refuse(
                "extension_dependency_closure_invalid",
                "wheel RECORD does not cover the archive",
            )

        def record_raw_blob(member_name: str) -> dict[str, object] | None:
            record = records[member_name]
            return None if record is None else record.to_record()

        if expected is not None:
            expected_records = {
                item.path: item.raw_blob for item in expected.record_entries
            }
            if (
                expected_records != records
                or expected.normalized_name != normalized_name
                or expected.version != version
            ):
                raise _refuse(
                    "extension_installed_tree_mismatch",
                    "wheel projection changed after planning",
                )
            record_manifest = expected.wheel_record_manifest_digest
            roots = expected.import_roots
        else:
            record_value = {
                "schema_version": WHEEL_RECORD_SCHEMA,
                "wheel_raw_blob": wheel_blob.to_record(),
                "entries": [
                    {
                        "path": item,
                        "raw_blob": record_raw_blob(item),
                    }
                    for item in sorted(records)
                ],
            }
            record_manifest = CanonicalRecordRef(
                WHEEL_RECORD_SCHEMA, canonical_record_digest(record_value)
            )
            roots = tuple(
                sorted(
                    {
                        PurePosixPath(item).parts[0].removesuffix(".py")
                        for item in names
                        if ".dist-info/" not in item and ".data/" not in item
                    }
                )
            )
        files: list[tuple[str, bytes, CanonicalRecordRef]] = []
        for member_name in sorted(names):
            installed_name = _wheel_member_name(member_name)
            member = _archive_read(archive, member_name)
            entry_record = {
                "schema_version": "okg.extension_runtime.wheel_record_entry/v1",
                "wheel_record_manifest_digest": record_manifest.to_record(),
                "path": member_name,
                "raw_blob": record_raw_blob(member_name),
            }
            files.append(
                (
                    installed_name,
                    member,
                    CanonicalRecordRef(
                        "okg.extension_runtime.wheel_record_entry/v1",
                        canonical_record_digest(entry_record),
                    ),
                )
            )
        return _WheelInstall(
            InstalledDistribution(
                normalized_name, version, wheel_blob, record_manifest, tuple(roots)
            ),
            tuple(files),
        )


_BUILDER_SOURCE = r"""import base64,csv,hashlib,io,json,os,posixpath,stat,unicodedata,zipfile
REQ=3; RESULT=5
def canon(v): return json.dumps(v,ensure_ascii=False,sort_keys=True,separators=(",",":"),allow_nan=False).encode()
def digest(v): return "sha256:"+hashlib.sha256(v).hexdigest()
def readfd(fd):
 out=[]
 while True:
  b=os.read(fd,1048576)
  if not b: break
  out.append(b)
 return b"".join(out)
def safe(v):
 if not isinstance(v,str) or not v or "\\" in v or "\x00" in v or v.startswith("/") or unicodedata.normalize("NFC",v)!=v: raise ValueError()
 p=v.split("/")
 if any(x in ("",".","..") for x in p) or posixpath.normpath(v)!=v: raise ValueError()
 return p
def mkdirs(root,parts):
 dev=os.fstat(root).st_dev; cur=os.dup(root)
 try:
  for part in parts:
   try: os.mkdir(part,0o700,dir_fd=cur)
   except FileExistsError: pass
   nxt=os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_CLOEXEC|getattr(os,"O_NOFOLLOW",0),dir_fd=cur)
   if os.fstat(nxt).st_dev!=dev: os.close(nxt); raise ValueError()
   os.close(cur); cur=nxt
 finally: os.close(cur)
def write(root,name,data):
 parts=safe(name); mkdirs(root,parts[:-1]); dev=os.fstat(root).st_dev; cur=os.dup(root)
 try:
  for part in parts[:-1]:
   nxt=os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_CLOEXEC|getattr(os,"O_NOFOLLOW",0),dir_fd=cur)
   if os.fstat(nxt).st_dev!=dev: os.close(nxt); raise ValueError()
   os.close(cur); cur=nxt
  fd=os.open(parts[-1],os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_CLOEXEC|getattr(os,"O_NOFOLLOW",0),0o600,dir_fd=cur)
  try:
   if os.fstat(fd).st_dev!=dev: raise ValueError()
   view=memoryview(data)
   while view:
    n=os.write(fd,view)
    if n<=0: raise ValueError()
    view=view[n:]
   os.fsync(fd)
  finally: os.close(fd)
 finally: os.close(cur)
def install(wheel,site,root):
 raw=readfd(wheel["read_fd_slot"]); expected=wheel["raw_blob"]
 if len(raw)!=expected["size"] or digest(raw)!=expected["digest"]: raise ValueError()
 z=zipfile.ZipFile(io.BytesIO(raw)); infos=z.infolist(); names=[i.filename for i in infos if not i.is_dir()]
 if len(names)!=len(set(names)): raise ValueError()
 for info in infos:
  if info.is_dir(): continue
  mode=info.external_attr>>16
  if stat.S_ISLNK(mode) or stat.S_IFMT(mode) not in (0,stat.S_IFREG) or info.flag_bits&1 or info.compress_type not in (zipfile.ZIP_STORED,zipfile.ZIP_DEFLATED): raise ValueError()
 recs=[n for n in names if n.endswith(".dist-info/RECORD")]; metas=[n for n in names if n.endswith(".dist-info/METADATA")]
 if len(recs)!=1 or len(metas)!=1: raise ValueError()
 rows=list(csv.reader(io.StringIO(z.read(recs[0]).decode("utf-8")),strict=True)); covered=set(); installed=[]
 for name,h,s in rows:
  safe(name)
  if name in covered: raise ValueError()
  covered.add(name); data=z.read(name)
  if name==recs[0]:
   if h or s: raise ValueError()
  else:
   if not h.startswith("sha256=") or int(s)!=len(data): raise ValueError()
   got=base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
   if got!=h[7:]: raise ValueError()
  parts=name.split("/")
  for i,p in enumerate(parts):
   if p.endswith(".data"):
    if i+2>=len(parts) or parts[i+1] not in ("purelib","platlib"): raise ValueError()
    name="/".join(parts[i+2:]); break
  low=name.casefold()
  if low.endswith((".pth",".egg-link",".pyc")) or "__pycache__" in name.split("/") or posixpath.basename(low) in ("sitecustomize.py","usercustomize.py") or "direct_url.json" in low: raise ValueError()
  target=site+"/"+name; write(root,target,data); installed.append(target)
 if covered!=set(names): raise ValueError()
 meta=z.read(metas[0]).decode("utf-8"); headers={}
 for line in meta.splitlines():
  if ":" in line:
   k,v=line.split(":",1); headers.setdefault(k.casefold(),v.strip())
 return {"name":headers["name"].replace("_","-").replace(".","-").casefold(),"version":headers["version"],"wheel_raw_blob":expected,"members":sorted(installed)}
try:
 raw=readfd(REQ); request=json.loads(raw.decode("utf-8"))
 if canon(request)!=raw or request.get("schema_version")!="okg.extension_runtime.builder_request/v1": raise ValueError()
 if set(request)!={"schema_version","builder_identity_digest","builder_inputs_digest","runtime_layout_digest","package_plan_digest","wheels","staging_root_fd_slot","site_packages_relative_name","source_date_epoch","locale","bytecode_policy"}: raise ValueError()
 if request["staging_root_fd_slot"]!=4 or request["source_date_epoch"]!=0 or request["locale"]!="C.UTF-8" or request["bytecode_policy"]!="absent": raise ValueError()
 if request["wheels"]!=sorted(request["wheels"],key=lambda value:(value["object_role"],value["raw_blob"]["digest"])): raise ValueError()
 for wheel in request["wheels"]:
  if set(wheel)!={"object_role","raw_blob","wheel_record_manifest_digest","read_fd_slot"}: raise ValueError()
 expected_fds=sorted([3,4,5,6,8]+[value["read_fd_slot"] for value in request["wheels"]]); inherited=[]
 for fd in range(64):
  try: os.fstat(fd); inherited.append(fd)
  except OSError: pass
 if inherited!=expected_fds: raise ValueError()
 distributions=[install(w,request["site_packages_relative_name"],request["staging_root_fd_slot"]) for w in request["wheels"]]
 result={"schema_version":"okg.extension_runtime.builder_result/v1","status":"installed","installed_distributions":sorted(distributions,key=lambda x:x["name"])}
 os.write(RESULT,canon(result))
except BaseException:
 os.write(RESULT,canon({"schema_version":"okg.extension_runtime.builder_result/v1","status":"refused","code":"extension_builder_input_mismatch"})); raise SystemExit(1)
""".encode("utf-8")

_BOOTSTRAP_SOURCE = r"""import _imp,json,os,stat,sys
def canon(v): return json.dumps(v,ensure_ascii=False,sort_keys=True,separators=(",",":"),allow_nan=False).encode()
def readfd(fd):
 out=[]
 while True:
  value=os.read(fd,1048576)
  if not value: break
  out.append(value)
 return b"".join(out)
raw=readfd(3); request=json.loads(raw.decode("utf-8"))
if canon(request)!=raw or request.get("schema_version")!="okg.extension_runtime.bootstrap_request/v1": raise SystemExit(1)
def member_stat(root,name):
 parts=name.split("/"); cur=os.dup(root)
 try:
  for part in parts[:-1]:
   nxt=os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_CLOEXEC|getattr(os,"O_NOFOLLOW",0),dir_fd=cur); os.close(cur); cur=nxt
  fd=os.open(parts[-1],os.O_RDONLY|os.O_CLOEXEC|getattr(os,"O_NOFOLLOW",0),dir_fd=cur)
  try: return os.fstat(fd)
  finally: os.close(fd)
 finally: os.close(cur)
executed=os.fstat(6); selected=member_stat(4,request["interpreter_member"])
executed_member_matches=stat.S_ISREG(executed.st_mode) and stat.S_ISREG(selected.st_mode) and (executed.st_dev,executed.st_ino)==(selected.st_dev,selected.st_ino)
prefix=os.path.realpath(sys.prefix); entries=[]; outside=False
for value in sys.path:
 if not value: outside=True; continue
 real=os.path.realpath(value)
 try: rel=os.path.relpath(real,prefix)
 except ValueError: outside=True; continue
 if rel==".." or rel.startswith(".."+os.sep): outside=True
 else: entries.append(rel.replace(os.sep,"/"))
def inside(value):
 try: rel=os.path.relpath(value,prefix)
 except ValueError: return False
 return rel!=".." and not rel.startswith(".."+os.sep)
origins=[]
for module in tuple(sys.modules.values()):
 value=getattr(module,"__file__",None)
 if value: origins.append(os.path.realpath(value))
fds=[]
for fd in range(64):
 try: os.fstat(fd); fds.append(fd)
 except OSError: pass
proof={"schema_version":"okg.extension_runtime.bootstrap_proof/v1","isolated":bool(sys.flags.isolated),"ignore_environment":bool(sys.flags.ignore_environment),"no_user_site":bool(sys.flags.no_user_site),"dont_write_bytecode":bool(sys.dont_write_bytecode),"outside_prefix":outside,"prefix_entries":sorted(set(entries)),"python_abi":"cp%d%d"%(sys.version_info.major,sys.version_info.minor),"builtin_modules":sorted(sys.builtin_module_names),"frozen_modules":sorted(name for name in sys.stdlib_module_names if _imp.is_frozen(name)),"executed_member_matches":executed_member_matches,"executable_inside_prefix":inside(os.path.realpath(sys.executable)),"base_prefix_inside_prefix":inside(os.path.realpath(sys.base_prefix)),"module_origins_inside_prefix":all(inside(value) for value in origins),"cwd_empty":not os.listdir("."),"inherited_fds":fds}
os.write(5,canon(proof))
""".encode("utf-8")


def builder_program_bytes() -> bytes:
    """Return the exact executable trusted-builder module bytes."""

    return _BUILDER_SOURCE


def bootstrap_verifier_program_bytes() -> bytes:
    return _BOOTSTRAP_SOURCE


def _verify_builder_program(payload: bytes) -> None:
    if payload != _BUILDER_SOURCE:
        raise _refuse(
            "extension_builder_identity_mismatch",
            "selected builder bytes differ from the admitted executable builder",
            path="builder_raw_blob",
        )


def _verify_bootstrap_program(payload: bytes) -> None:
    if payload != _BOOTSTRAP_SOURCE:
        raise _refuse(
            "extension_runtime_toolchain_mismatch",
            "selected bootstrap verifier bytes differ",
            path="bootstrap_verifier_raw_blob",
        )


def _site_packages_relative(python_abi: str) -> str:
    match = _PYTHON_ABI_RE.fullmatch(python_abi)
    if match is None:
        raise _refuse(
            "extension_python_abi_mismatch",
            "python ABI does not determine one site-packages layout",
            path="python_abi",
        )
    return f"lib/python{match.group('major')}.{match.group('minor')}/site-packages"


def _runtime_distribution_tree(install: _WheelInstall) -> CanonicalRecordRef:
    value = {
        "schema_version": RUNTIME_DISTRIBUTION_TREE_SCHEMA,
        "distribution": install.distribution.to_record(),
        "members": [
            {
                "relative_name": name,
                "mode": 0o444,
                "size": len(payload),
                "raw_blob": RawBlobRef(
                    raw_blob_digest(payload), len(payload)
                ).to_record(),
            }
            for name, payload, _record in install.files
        ],
    }
    return CanonicalRecordRef(
        RUNTIME_DISTRIBUTION_TREE_SCHEMA, canonical_record_digest(value)
    )


def _runtime_layout_record(
    identity: RuntimeToolchainIdentity,
    components: Mapping[str, bytes],
    component_members: Mapping[str, ToolchainComponent],
    stdlib_members: Sequence[tuple[ToolchainMember, bytes]],
    builtin_modules: Sequence[str],
    frozen_modules: Sequence[str],
    native_shared_libraries: Sequence[str],
) -> dict[str, object]:
    sdk_install = _wheel_install(components["public_sdk"], None)
    child_install = _wheel_install(components["child_host"], None)
    stdlib_roots = sorted(
        {PurePosixPath(member.relative_name).parts[0] for member, _ in stdlib_members}
    )
    native_roots = sorted(
        {PurePosixPath(value).parent.as_posix() for value in native_shared_libraries}
    )
    return {
        "schema_version": RUNTIME_LAYOUT_SCHEMA,
        "python_abi": identity.python_abi,
        "platform_identity": dict(identity.platform_identity),
        "interpreter": component_members["interpreter"].runtime_record(),
        "prefix_configuration": {
            "profile": "standalone-base-prefix/v1",
            "pyvenv_config": None,
        },
        "distribution_prefix_tree_digest": (
            identity.distribution_prefix_tree_digest.to_record()
        ),
        "prefix_copy_proof_digest": identity.prefix_copy_proof_digest.to_record(),
        "stdlib_roots": stdlib_roots,
        "site_packages_root": _site_packages_relative(identity.python_abi),
        "native_library_roots": native_roots,
        "builtin_modules": list(builtin_modules),
        "frozen_modules": list(frozen_modules),
        "native_shared_libraries": list(native_shared_libraries),
        "stdlib_manifest_digest": identity.stdlib_manifest_digest.to_record(),
        "public_sdk": {
            "wheel_raw_blob": identity.public_sdk_raw_blob.to_record(),
            "installed_tree_manifest_digest": _runtime_distribution_tree(
                sdk_install
            ).to_record(),
        },
        "child_host": {
            "wheel_raw_blob": identity.child_host_raw_blob.to_record(),
            "installed_tree_manifest_digest": _runtime_distribution_tree(
                child_install
            ).to_record(),
        },
        "bootstrap_verifier": component_members["bootstrap_verifier"].runtime_record(),
        "builder": component_members["builder"].runtime_record(),
        "isolated_flags": ["-I", "-s", "-E"],
        "bytecode_policy": "absent",
    }


@dataclass(frozen=True, slots=True)
class VerifiedRuntimeToolchain:
    """Retained-root runtime layout selected from a verified sealed release."""

    identity: RuntimeToolchainIdentity
    components: Mapping[str, bytes]
    component_members: Mapping[str, ToolchainComponent]
    stdlib_members: tuple[tuple[ToolchainMember, bytes], ...]
    manifest_relative_name: str
    manifest_raw_blob: RawBlobRef
    builtin_modules: tuple[str, ...]
    frozen_modules: tuple[str, ...]
    native_shared_libraries: tuple[str, ...]
    _release_root_fd: int = field(repr=False)
    _release_root_identity: tuple[int, int, int, int, int] = field(repr=False)
    _release_member_identities: Mapping[
        str, tuple[int, int, int, int, int, int, int]
    ] = field(repr=False)
    _issued_identity_bytes: bytes = field(init=False, repr=False)
    _closed: bool = field(default=False, init=False, repr=False)
    _lock: threading.RLock = field(
        default_factory=threading.RLock, init=False, repr=False
    )

    def __post_init__(self) -> None:
        object.__setattr__(self, "components", MappingProxyType(dict(self.components)))
        object.__setattr__(
            self,
            "component_members",
            MappingProxyType(dict(self.component_members)),
        )
        object.__setattr__(
            self,
            "_release_member_identities",
            MappingProxyType(dict(self._release_member_identities)),
        )
        if set(self.components) != set(_COMPONENT_NAMES) or set(
            self.component_members
        ) != set(_COMPONENT_NAMES):
            raise _refuse(
                "extension_runtime_toolchain_mismatch",
                "toolchain component set is not closed",
            )
        refs = {
            "interpreter": self.identity.interpreter_raw_blob,
            "public_sdk": self.identity.public_sdk_raw_blob,
            "child_host": self.identity.child_host_raw_blob,
            "bootstrap_verifier": self.identity.bootstrap_verifier_raw_blob,
            "builder": self.identity.builder_raw_blob,
            "broker": self.identity.broker_raw_blob,
            "containment_launcher": self.identity.containment_launcher_raw_blob,
        }
        for name, expected in refs.items():
            payload = self.components[name]
            if RawBlobRef(raw_blob_digest(payload), len(payload)) != expected:
                raise _refuse(
                    "extension_runtime_toolchain_mismatch",
                    "toolchain component bytes changed",
                    path=name,
                )
        _verify_builder_program(self.components["builder"])
        _verify_bootstrap_program(self.components["bootstrap_verifier"])
        if (
            self.component_members["interpreter"].runtime_relative_name
            != "bin/python3.12"
        ):
            raise _refuse(
                "extension_runtime_toolchain_mismatch",
                "interpreter runtime name is not fixed",
                path="components.interpreter.runtime_relative_name",
            )
        site = _site_packages_relative(self.identity.python_abi)
        expected_modules = {
            "builder": f"{site}/okg_extension_builder.py",
            "bootstrap_verifier": f"{site}/okg_extension_bootstrap.py",
        }
        for name, expected in expected_modules.items():
            if self.component_members[name].runtime_relative_name != expected:
                raise _refuse(
                    "extension_runtime_toolchain_mismatch",
                    "trusted module runtime name is not fixed",
                    path=f"components.{name}.runtime_relative_name",
                )
        if self.runtime_layout_digest != self.identity.runtime_layout_digest:
            raise _refuse(
                "extension_runtime_toolchain_mismatch",
                "runtime layout digest differs from the selected manifest",
                path="runtime_layout_digest",
            )
        object.__setattr__(
            self,
            "_issued_identity_bytes",
            canonical_json_bytes(self.identity.to_record()),
        )

    @property
    def runtime_packages(self) -> RuntimePackageProjections:
        sdk = _wheel_install(self.components["public_sdk"], None).distribution
        child = _wheel_install(self.components["child_host"], None).distribution

        def project(value: InstalledDistribution) -> RuntimeDistributionProjection:
            return RuntimeDistributionProjection(
                value.name,
                value.version,
                value.wheel_raw_blob,
                value.wheel_record_manifest_digest,
                value.import_roots,
            )

        return RuntimePackageProjections(project(sdk), project(child))

    def runtime_layout_record(self) -> dict[str, object]:
        return _runtime_layout_record(
            self.identity,
            self.components,
            self.component_members,
            self.stdlib_members,
            self.builtin_modules,
            self.frozen_modules,
            self.native_shared_libraries,
        )

    @property
    def runtime_layout_digest(self) -> CanonicalRecordRef:
        return CanonicalRecordRef(
            RUNTIME_LAYOUT_SCHEMA, canonical_record_digest(self.runtime_layout_record())
        )

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            fd = self._release_root_fd
            object.__setattr__(self, "_release_root_fd", -1)
            object.__setattr__(self, "_closed", True)
            try:
                os.close(fd)
            except OSError:
                # Ownership is already retired; retrying could close a reused fd.
                pass

    def __del__(self) -> None:  # pragma: no cover
        self.close()


def _sorted_string_set(value: object, path: str) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise _refuse(
            "extension_runtime_toolchain_mismatch",
            "manifest set must be an array",
            path=path,
        )
    if any(not isinstance(item, str) for item in value):
        raise _refuse(
            "extension_runtime_toolchain_mismatch",
            "manifest set entries must be strings",
            path=path,
        )
    items = tuple(value)
    if any(
        not item or "\x00" in item or unicodedata.normalize("NFC", item) != item
        for item in items
    ):
        raise _refuse(
            "extension_runtime_toolchain_mismatch",
            "manifest set entry is invalid",
            path=path,
        )
    if items != tuple(sorted(set(items))):
        raise _refuse(
            "extension_runtime_toolchain_mismatch",
            "manifest set must be sorted and unique",
            path=path,
        )
    return items


def _retain_verified_runtime_authority(
    authority: VerifiedRelocatableRuntimeAuthority,
) -> tuple[int, tuple[int, int, int, int, int]]:
    if not isinstance(authority, VerifiedRelocatableRuntimeAuthority):
        raise _refuse(
            "extension_runtime_toolchain_mismatch",
            "runtime toolchain requires a relocatable-runtime authority",
        )
    root_fd: int | None = None
    try:
        root_fd = authority._duplicate_root_fd()
        root_stat = os.fstat(root_fd)
        root_identity = (
            root_stat.st_dev,
            root_stat.st_ino,
            root_stat.st_mode,
            root_stat.st_mtime_ns,
            root_stat.st_ctime_ns,
        )
        final_stat = os.fstat(root_fd)
        final_identity = (
            final_stat.st_dev,
            final_stat.st_ino,
            final_stat.st_mode,
            final_stat.st_mtime_ns,
            final_stat.st_ctime_ns,
        )
        if final_identity != root_identity:
            raise _refuse(
                "extension_runtime_toolchain_mismatch",
                "sealed release root changed while verified",
            )
        return root_fd, root_identity
    except Exception as exc:
        if root_fd is not None:
            os.close(root_fd)
        if isinstance(exc, ExtensionRuntimeSealError):
            raise
        raise _refuse(
            "extension_runtime_toolchain_mismatch",
            "release runtime authority could not be retained",
        ) from exc


def select_runtime_toolchain(
    authority: VerifiedRelocatableRuntimeAuthority,
    *,
    manifest_relative_path: str,
    platform_identity: Mapping[str, str],
) -> VerifiedRuntimeToolchain:
    """Select and retain one exact runtime layout from a verified release."""

    root_fd, root_identity = _retain_verified_runtime_authority(authority)
    try:
        manifest_name = _safe_relative_name(
            manifest_relative_path, "manifest_relative_path"
        )
        release_member_identities: dict[
            str, tuple[int, int, int, int, int, int, int]
        ] = {}
        manifest_bytes, manifest_identity = _read_regular_identity_at(
            root_fd, manifest_name, code="extension_runtime_toolchain_mismatch"
        )
        manifest_authority = authority.member(manifest_name)
        if manifest_authority.raw_blob_digest != raw_blob_digest(
            manifest_bytes
        ) or manifest_authority.size != len(manifest_bytes):
            raise _refuse(
                "extension_runtime_toolchain_mismatch",
                "toolchain manifest is outside the release authority",
            )
        release_member_identities[manifest_name] = manifest_identity
        raw = _strict_json(manifest_bytes, "toolchain_manifest")
        _exact_keys(
            raw,
            {
                "schema_version",
                "platform_identity",
                "python_abi",
                "public_sdk_abi",
                "containment_profile_manifest_digest",
                "runtime_layout_digest",
                "components",
                "stdlib",
            },
            "toolchain_manifest",
        )
        if raw.get("schema_version") != TOOLCHAIN_SCHEMA:
            raise _refuse(
                "extension_runtime_toolchain_mismatch",
                "toolchain schema is unsupported",
            )
        authority_receipt = _mapping(authority.receipt, "runtime_authority.receipt")
        if authority_receipt.get("schema_version") != (
            "okg.sealed_release.runtime_authority/v2"
        ):
            raise _refuse(
                "extension_runtime_toolchain_mismatch",
                "runtime authority receipt schema is unsupported",
                path="runtime_authority.receipt.schema_version",
            )
        authority_receipt_digest = raw_blob_digest(
            canonical_json_bytes(authority_receipt)
        )
        sealed_release_receipt = _mapping(
            authority_receipt.get("sealed_release"),
            "runtime_authority.receipt.sealed_release",
        )
        _exact_keys(
            sealed_release_receipt,
            {
                "schema_version",
                "artifact_id",
                "input_id",
                "executable_tree_digest",
            },
            "runtime_authority.receipt.sealed_release",
        )
        receipt_artifact_id = sealed_release_receipt.get("artifact_id")
        receipt_tree_digest = sealed_release_receipt.get("executable_tree_digest")
        if (
            sealed_release_receipt.get("schema_version") != "okg.sealed_release/v1"
            or not isinstance(receipt_artifact_id, str)
            or _DIGEST_RE.fullmatch(receipt_artifact_id) is None
            or not isinstance(receipt_tree_digest, str)
            or _DIGEST_RE.fullmatch(receipt_tree_digest) is None
            or authority.receipt_digest != authority_receipt_digest
            or authority.sealed_release_artifact_id != receipt_artifact_id
            or authority.sealed_release_tree_digest != receipt_tree_digest
        ):
            raise _refuse(
                "extension_runtime_toolchain_mismatch",
                "runtime authority receipt identity differs from its retained provenance",
                path="runtime_authority.receipt.sealed_release",
            )
        supplied_authority = CanonicalRecordRef(
            "okg.sealed_release.runtime_authority/v2",
            authority_receipt_digest,
        )
        distribution_prefix_tree_digest = _authority_receipt_ref(
            authority_receipt,
            "distribution_prefix_tree_digest",
            "okg.sealed_release.runtime_distribution_prefix_tree/v1",
        )
        prefix_copy_proof_digest = _authority_receipt_ref(
            authority_receipt,
            "prefix_copy_proof_digest",
            "okg.sealed_release.runtime_prefix_copy_proof/v1",
        )
        authority_platform_raw = _mapping(
            authority_receipt.get("platform_identity"),
            "runtime_authority.receipt.platform_identity",
        )
        platform_keys = {"os", "architecture", "libc", "kernel_contract"}
        _exact_keys(
            authority_platform_raw,
            platform_keys,
            "runtime_authority.receipt.platform_identity",
        )
        authority_platform: dict[str, str] = {}
        for key, value in authority_platform_raw.items():
            if not isinstance(value, str) or not value:
                raise _refuse(
                    "extension_platform_mismatch",
                    "runtime authority platform identity is invalid",
                    path=f"runtime_authority.receipt.platform_identity.{key}",
                )
            authority_platform[key] = value
        selected_platform: dict[str, str] = {}
        selected_platform_raw = _mapping(raw["platform_identity"], "platform_identity")
        _exact_keys(selected_platform_raw, platform_keys, "platform_identity")
        for key, value in selected_platform_raw.items():
            if not isinstance(value, str) or not value:
                raise _refuse(
                    "extension_platform_mismatch",
                    "toolchain platform identity is invalid",
                    path=f"platform_identity.{key}",
                )
            selected_platform[key] = value
        if not (
            selected_platform
            == dict(platform_identity)
            == dict(authority.platform_identity)
            == authority_platform
        ):
            raise _refuse(
                "extension_platform_mismatch", "toolchain platform identity differs"
            )
        selected_python_abi = raw.get("python_abi")
        authority_python_abi = authority_receipt.get("python_abi")
        if (
            not isinstance(selected_python_abi, str)
            or _PYTHON_ABI_RE.fullmatch(selected_python_abi) is None
            or selected_python_abi != authority.python_abi
            or selected_python_abi != authority_python_abi
        ):
            raise _refuse(
                "extension_python_abi_mismatch",
                "toolchain Python ABI differs from the runtime authority",
                path="python_abi",
            )
        receipt_interpreter = _mapping(
            authority_receipt.get("interpreter"),
            "runtime_authority.receipt.interpreter",
        )
        _exact_keys(
            receipt_interpreter,
            {"relative_name", "kind", "mode", "size", "raw_blob"},
            "runtime_authority.receipt.interpreter",
        )
        receipt_interpreter_name = _safe_relative_name(
            receipt_interpreter.get("relative_name"),
            "runtime_authority.receipt.interpreter.relative_name",
        )
        receipt_interpreter_raw_blob = _raw_ref(
            receipt_interpreter.get("raw_blob"),
            "runtime_authority.receipt.interpreter.raw_blob",
        )
        receipt_interpreter_mode = receipt_interpreter.get("mode")
        if (
            receipt_interpreter_name != "bin/python3.12"
            or receipt_interpreter.get("kind") != "file"
            or receipt_interpreter_mode != 0o555
            or receipt_interpreter.get("size") != receipt_interpreter_raw_blob.size
        ):
            raise _refuse(
                "extension_runtime_toolchain_mismatch",
                "runtime authority interpreter identity is invalid",
                path="runtime_authority.receipt.interpreter",
            )
        components_raw = _mapping(raw["components"], "components")
        _exact_keys(components_raw, set(_COMPONENT_NAMES), "components")
        components: dict[str, bytes] = {}
        component_members: dict[str, ToolchainComponent] = {}
        for name in _COMPONENT_NAMES:
            item = _mapping(components_raw[name], f"components.{name}")
            _exact_keys(
                item,
                {
                    "release_relative_name",
                    "runtime_relative_name",
                    "raw_blob",
                    "executable",
                },
                f"components.{name}",
            )
            release_name = _safe_relative_name(
                item["release_relative_name"],
                f"components.{name}.release_relative_name",
            )
            runtime_name = _safe_relative_name(
                item["runtime_relative_name"],
                f"components.{name}.runtime_relative_name",
            )
            expected = _raw_ref(item["raw_blob"], f"components.{name}.raw_blob")
            if not isinstance(item["executable"], bool):
                raise _refuse(
                    "extension_runtime_toolchain_mismatch",
                    "component executable flag is invalid",
                    path=f"components.{name}.executable",
                )
            if name == "interpreter" and (
                release_name != f"venv/{receipt_interpreter_name}"
                or runtime_name != receipt_interpreter_name
                or item["executable"] is not True
                or expected != receipt_interpreter_raw_blob
            ):
                raise _refuse(
                    "extension_runtime_toolchain_mismatch",
                    "selected interpreter differs from the runtime authority",
                    path="components.interpreter",
                )
            payload, member_identity = _read_regular_identity_at(
                root_fd, release_name, code="extension_runtime_toolchain_mismatch"
            )
            authority_member = authority.member(release_name)
            if RawBlobRef(raw_blob_digest(payload), len(payload)) != expected:
                raise _refuse(
                    "extension_runtime_toolchain_mismatch",
                    "component bytes changed",
                    path=f"components.{name}",
                )
            if (
                authority_member.raw_blob_digest != expected.digest
                or authority_member.size != expected.size
                or (
                    name == "interpreter"
                    and (
                        authority_member.mode != receipt_interpreter_mode
                        or stat.S_IMODE(member_identity[2]) != receipt_interpreter_mode
                        or stat.S_IMODE(member_identity[2]) != 0o555
                    )
                )
            ):
                raise _refuse(
                    "extension_runtime_toolchain_mismatch",
                    "component is outside the release runtime authority",
                    path=f"components.{name}",
                )
            components[name] = payload
            if release_name in release_member_identities:
                raise _refuse(
                    "extension_runtime_toolchain_mismatch",
                    "toolchain release members are not unique",
                    path=f"components.{name}.release_relative_name",
                )
            release_member_identities[release_name] = member_identity
            component_members[name] = ToolchainComponent(
                release_name, runtime_name, expected, bool(item["executable"])
            )
        stdlib = _mapping(raw["stdlib"], "stdlib")
        _exact_keys(
            stdlib,
            {"entries", "builtin_modules", "frozen_modules", "native_shared_libraries"},
            "stdlib",
        )
        entry_values = stdlib["entries"]
        if isinstance(entry_values, (str, bytes)) or not isinstance(
            entry_values, Sequence
        ):
            raise _refuse(
                "extension_runtime_toolchain_mismatch",
                "stdlib entries must be an array",
            )
        stdlib_members: list[tuple[ToolchainMember, bytes]] = []
        for index, value in enumerate(entry_values):
            item = _mapping(value, f"stdlib.entries[{index}]")
            _exact_keys(
                item,
                {"release_relative_name", "relative_name", "raw_blob", "executable"},
                f"stdlib.entries[{index}]",
            )
            release_name = _safe_relative_name(
                item["release_relative_name"],
                f"stdlib.entries[{index}].release_relative_name",
            )
            runtime_name = _safe_relative_name(
                item["relative_name"], f"stdlib.entries[{index}].relative_name"
            )
            expected = _raw_ref(item["raw_blob"], f"stdlib.entries[{index}].raw_blob")
            executable = item["executable"]
            if not isinstance(executable, bool):
                raise _refuse(
                    "extension_runtime_toolchain_mismatch",
                    "stdlib executable flag is invalid",
                    path=f"stdlib.entries[{index}].executable",
                )
            if release_name != f"venv/{runtime_name}":
                raise _refuse(
                    "extension_runtime_toolchain_mismatch",
                    "stdlib member is outside the verified runtime prefix",
                    path=f"stdlib.entries[{index}].release_relative_name",
                )
            payload, member_identity = _read_regular_identity_at(
                root_fd, release_name, code="extension_runtime_toolchain_mismatch"
            )
            authority_member = authority.member(release_name)
            if RawBlobRef(raw_blob_digest(payload), len(payload)) != expected:
                raise _refuse(
                    "extension_runtime_toolchain_mismatch",
                    "stdlib member bytes changed",
                    path=runtime_name,
                )
            if (
                authority_member.raw_blob_digest != expected.digest
                or authority_member.size != expected.size
                or authority_member.mode != (0o555 if executable else 0o444)
                or stat.S_IMODE(member_identity[2]) != authority_member.mode
            ):
                raise _refuse(
                    "extension_runtime_toolchain_mismatch",
                    "stdlib member is outside the release runtime authority",
                    path=runtime_name,
                )
            if release_name in release_member_identities:
                raise _refuse(
                    "extension_runtime_toolchain_mismatch",
                    "toolchain release members are not unique",
                    path=f"stdlib.entries[{index}].release_relative_name",
                )
            release_member_identities[release_name] = member_identity
            stdlib_members.append(
                (
                    ToolchainMember(
                        release_name,
                        runtime_name,
                        expected,
                        executable,
                    ),
                    payload,
                )
            )
        stdlib_members.sort(key=lambda value: value[0].relative_name.encode("utf-8"))
        builtin = _sorted_string_set(
            stdlib["builtin_modules"], "stdlib.builtin_modules"
        )
        frozen = _sorted_string_set(stdlib["frozen_modules"], "stdlib.frozen_modules")
        native = _sorted_string_set(
            stdlib["native_shared_libraries"], "stdlib.native_shared_libraries"
        )
        stdlib_record = {
            "schema_version": STDLIB_SCHEMA,
            "entries": [member.to_record() for member, _payload in stdlib_members],
            "builtin_modules": list(builtin),
            "frozen_modules": list(frozen),
            "native_shared_libraries": list(native),
        }
        stdlib_digest = CanonicalRecordRef(
            STDLIB_SCHEMA, canonical_record_digest(stdlib_record)
        )
        supplied_layout = _canonical_ref(
            raw["runtime_layout_digest"], RUNTIME_LAYOUT_SCHEMA, "runtime_layout_digest"
        )
        refs = {name: component_members[name].raw_blob for name in _COMPONENT_NAMES}
        identity = RuntimeToolchainIdentity(
            {
                "schema_version": "okg.sealed_release/v1",
                "artifact_id": receipt_artifact_id,
            },
            CanonicalRecordRef("okg.sealed_release.tree/v1", receipt_tree_digest),
            supplied_authority,
            selected_platform,
            selected_python_abi,
            refs["interpreter"],
            distribution_prefix_tree_digest,
            prefix_copy_proof_digest,
            stdlib_digest,
            supplied_layout,
            str(raw["public_sdk_abi"]),
            refs["public_sdk"],
            refs["child_host"],
            refs["bootstrap_verifier"],
            refs["builder"],
            refs["broker"],
            refs["containment_launcher"],
            _canonical_ref(
                raw["containment_profile_manifest_digest"],
                "okg.extension_runtime.containment_profile/v1",
                "containment_profile_manifest_digest",
            ),
        )
        selected_toolchain = VerifiedRuntimeToolchain(
            identity,
            MappingProxyType(components),
            MappingProxyType(component_members),
            tuple(stdlib_members),
            manifest_name,
            RawBlobRef(raw_blob_digest(manifest_bytes), len(manifest_bytes)),
            builtin,
            frozen,
            native,
            root_fd,
            root_identity,
            MappingProxyType(release_member_identities),
        )
        observed_root = os.fstat(root_fd)
        if (
            observed_root.st_dev,
            observed_root.st_ino,
            observed_root.st_mode,
            observed_root.st_mtime_ns,
            observed_root.st_ctime_ns,
        ) != root_identity:
            raise _refuse(
                "extension_runtime_toolchain_mismatch",
                "sealed release root changed during toolchain selection",
            )
        return selected_toolchain
    except SealedReleaseInvalid as exc:
        os.close(root_fd)
        raise _refuse(
            "extension_runtime_toolchain_mismatch",
            "release runtime authority member refused",
        ) from exc
    except BaseException:
        os.close(root_fd)
        raise


def verify_runtime_toolchain(
    toolchain: VerifiedRuntimeToolchain,
) -> VerifiedRuntimeToolchain:
    """Revalidate every selected byte through the retained release descriptor."""

    with toolchain._lock:
        if toolchain._closed or toolchain._release_root_fd < 0:
            raise _refuse(
                "extension_runtime_toolchain_mismatch", "toolchain authority is closed"
            )
        if (
            canonical_json_bytes(toolchain.identity.to_record())
            != toolchain._issued_identity_bytes
        ):
            raise _refuse(
                "extension_runtime_toolchain_mismatch",
                "runtime toolchain identity differs from its issued receipt",
            )
        observed = os.fstat(toolchain._release_root_fd)
        identity = (
            observed.st_dev,
            observed.st_ino,
            observed.st_mode,
            observed.st_mtime_ns,
            observed.st_ctime_ns,
        )
        if identity != toolchain._release_root_identity:
            raise _refuse(
                "extension_runtime_toolchain_mismatch", "retained release root changed"
            )
        expected_component_refs = {
            "interpreter": toolchain.identity.interpreter_raw_blob,
            "public_sdk": toolchain.identity.public_sdk_raw_blob,
            "child_host": toolchain.identity.child_host_raw_blob,
            "bootstrap_verifier": toolchain.identity.bootstrap_verifier_raw_blob,
            "builder": toolchain.identity.builder_raw_blob,
            "broker": toolchain.identity.broker_raw_blob,
            "containment_launcher": toolchain.identity.containment_launcher_raw_blob,
        }
        for name, expected_ref in expected_component_refs.items():
            payload = toolchain.components[name]
            if RawBlobRef(raw_blob_digest(payload), len(payload)) != expected_ref:
                raise _refuse(
                    "extension_runtime_toolchain_mismatch",
                    "runtime toolchain component identity changed",
                    path=name,
                )
        manifest, manifest_identity = _read_regular_identity_at(
            toolchain._release_root_fd,
            toolchain.manifest_relative_name,
            code="extension_runtime_toolchain_mismatch",
        )
        if (
            RawBlobRef(raw_blob_digest(manifest), len(manifest))
            != toolchain.manifest_raw_blob
            or manifest_identity
            != toolchain._release_member_identities[toolchain.manifest_relative_name]
        ):
            raise _refuse(
                "extension_runtime_toolchain_mismatch", "toolchain manifest changed"
            )
        for name, member in toolchain.component_members.items():
            payload, member_identity = _read_regular_identity_at(
                toolchain._release_root_fd,
                member.release_relative_name,
                code="extension_runtime_toolchain_mismatch",
            )
            if (
                payload != toolchain.components[name]
                or member_identity
                != toolchain._release_member_identities[member.release_relative_name]
            ):
                raise _refuse(
                    "extension_runtime_toolchain_mismatch",
                    "toolchain component changed",
                    path=name,
                )
        for member, expected in toolchain.stdlib_members:
            payload, member_identity = _read_regular_identity_at(
                toolchain._release_root_fd,
                member.release_relative_name,
                code="extension_runtime_toolchain_mismatch",
            )
            if (
                payload != expected
                or member_identity
                != toolchain._release_member_identities[member.release_relative_name]
            ):
                raise _refuse(
                    "extension_runtime_toolchain_mismatch",
                    "stdlib member changed",
                    path=member.relative_name,
                )
        if toolchain.runtime_layout_digest != toolchain.identity.runtime_layout_digest:
            raise _refuse(
                "extension_runtime_toolchain_mismatch",
                "runtime layout identity changed",
            )
        return toolchain


class _VerifiedPackage(Protocol):
    manifest: object

    def extension_runtime_authority(self) -> object:
        raise NotImplementedError


def _package_object(package: _VerifiedPackage, wheel: WheelProjection) -> bytes:
    factory = getattr(package, "extension_runtime_authority", None)
    if not callable(factory):
        raise _refuse(
            "extension_offline_object_missing",
            "package runtime authority is unavailable",
        )
    try:
        authority = factory()
        reader = getattr(authority, "read_verified_object", None)
        close = getattr(authority, "close", None)
        if not callable(reader) or not callable(close):
            raise TypeError("invalid package runtime authority")
        try:
            payload = reader(wheel.selection.raw_blob_ref.digest)
        finally:
            close()
    except Exception as exc:
        raise _refuse(
            "extension_offline_object_missing",
            "planned wheel object is unavailable",
            path=wheel.selection.asset_ref,
        ) from exc
    if (
        not isinstance(payload, bytes)
        or RawBlobRef(raw_blob_digest(payload), len(payload))
        != wheel.selection.raw_blob_ref
    ):
        raise _refuse(
            "extension_package_object_mismatch",
            "planned wheel object changed",
            path=wheel.selection.asset_ref,
        )
    return payload


def _assert_empty_authority_environment(environ: Mapping[str, str] | None) -> None:
    if environ is None:
        return
    bad = sorted(key for key in environ if key in _AMBIENT_PACKAGE_KEYS)
    if bad:
        raise _refuse(
            "extension_ambient_package_source_refused",
            "ambient package or interpreter authority is not admitted",
            path=bad[0],
        )


def _create_runtime_root(
    destination: Path,
) -> tuple[int, tuple[int, int, int, int, int]]:
    if not destination.is_absolute() or destination.name in {"", ".", ".."}:
        raise _refuse(
            "extension_runtime_unsealed",
            "runtime destination must be one absolute new leaf",
        )
    parent_fd = _open_absolute_directory(
        destination.parent, code="extension_runtime_unsealed", label="runtime_parent"
    )
    try:
        os.mkdir(destination.name, 0o700, dir_fd=parent_fd)
        root_fd = os.open(destination.name, _directory_flags(), dir_fd=parent_fd)
    except FileExistsError as exc:
        raise _refuse(
            "extension_runtime_unsealed", "runtime destination already exists"
        ) from exc
    except OSError as exc:
        refusal = (
            "extension_runtime_symlink_refused"
            if exc.errno in {errno.ELOOP, errno.ENOTDIR}
            else "extension_runtime_unsealed"
        )
        raise _refuse(refusal, "runtime destination could not be created") from exc
    finally:
        os.close(parent_fd)
    observed = os.fstat(root_fd)
    return root_fd, (
        observed.st_dev,
        observed.st_ino,
        observed.st_mode,
        observed.st_mtime_ns,
        observed.st_ctime_ns,
    )


@dataclass(slots=True)
class _ReadOnlyWheelHandle:
    fd: int

    def close(self) -> None:
        if self.fd < 0:
            return
        fd = self.fd
        self.fd = -1
        os.close(fd)


def _temporary_wheel_fd(payload: bytes) -> tuple[_ReadOnlyWheelHandle, int]:
    writer = tempfile.NamedTemporaryFile(mode="w+b", delete=False)
    name = writer.name
    read_fd = -1
    try:
        writer.write(payload)
        writer.flush()
        os.fsync(writer.fileno())
        os.fchmod(writer.fileno(), 0o400)
        before = os.fstat(writer.fileno())
        read_fd = os.open(
            name,
            os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
        )
        after = os.fstat(read_fd)
        if (
            _stable_file_token(before) != _stable_file_token(after)
            or (fcntl.fcntl(read_fd, fcntl.F_GETFL) & os.O_ACCMODE) != os.O_RDONLY
        ):
            raise _refuse(
                "extension_package_object_mismatch",
                "temporary wheel descriptor is not exact and read-only",
            )
        os.unlink(name)
        writer.close()
        return _ReadOnlyWheelHandle(read_fd), read_fd
    except BaseException:
        if read_fd >= 0:
            os.close(read_fd)
        writer.close()
        try:
            os.unlink(name)
        except OSError:
            # Cleanup failure must not replace the original construction failure.
            pass
        raise


def _high_dup(fd: int) -> int:
    return int(fcntl.fcntl(fd, fcntl.F_DUPFD_CLOEXEC, 64))


def _exec_fd(
    executable_fd: int, argv: Sequence[str], environ: Mapping[str, str]
) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    encoded_argv = [item.encode("utf-8") for item in argv]
    encoded_env = [
        f"{key}={value}".encode("utf-8") for key, value in sorted(environ.items())
    ]
    argv_array = (ctypes.c_char_p * (len(encoded_argv) + 1))(*encoded_argv, None)
    env_array = (ctypes.c_char_p * (len(encoded_env) + 1))(*encoded_env, None)
    fexecve = getattr(libc, "fexecve", None)
    if fexecve is not None:
        fexecve(executable_fd, argv_array, env_array)
    execveat = getattr(libc, "execveat", None)
    if execveat is not None:
        execveat(executable_fd, ctypes.c_char_p(b""), argv_array, env_array, 0x1000)
    error = ctypes.get_errno()
    if fexecve is None and execveat is None:
        reason = b"descriptor exec unavailable: no fexecve or execveat in libc\n"
    else:
        name = errno.errorcode.get(error, "unknown").encode("ascii")
        reason = b"descriptor exec failed: errno %d (%s)\n" % (error, name)
    try:
        os.write(2, reason)
    except OSError:
        # The diagnostic channel is best effort; the exit status still reports.
        pass
    os._exit(126 if error else 127)


class _StderrTail:
    """The last MAX_CHILD_STDERR_TAIL_BYTES the child wrote to stderr."""

    __slots__ = ("data", "truncated")

    def __init__(self) -> None:
        self.data = bytearray()
        self.truncated = False

    def add(self, chunk: bytes) -> None:
        self.data.extend(chunk)
        excess = len(self.data) - MAX_CHILD_STDERR_TAIL_BYTES
        if excess > 0:
            del self.data[:excess]
            self.truncated = True

    def describe(self, status: int | None) -> str:
        return _describe_child_exit(status, bytes(self.data), truncated=self.truncated)


def _drain_stderr_tail(fd: int, tail: _StderrTail, *, deadline: float) -> bool:
    """Read what the child wrote to its error pipe, keeping only a bounded tail.

    Returns False at end of file, True while the pipe may still produce bytes.
    Stops at ``deadline`` so a child that never stops writing cannot hold the
    parent.
    """

    while time.monotonic() < deadline:
        try:
            chunk = os.read(fd, 65536)
        except BlockingIOError:
            return True
        if not chunk:
            return False
        tail.add(chunk)
    return True


def _describe_child_exit(
    status: int | None, stderr_tail: bytes, *, truncated: bool = False
) -> str:
    """Name how the isolated child ended plus the printable tail of its stderr."""

    if status is None:
        outcome = "exit status unknown"
    elif os.WIFEXITED(status):
        outcome = f"exit status {os.WEXITSTATUS(status)}"
    elif os.WIFSIGNALED(status):
        number = os.WTERMSIG(status)
        try:
            name = signal.Signals(number).name
        except ValueError:
            name = "unknown"
        outcome = f"killed by signal {number} ({name})"
    else:
        outcome = f"wait status {status}"
    text = stderr_tail.decode("utf-8", errors="replace")
    printable = "".join(
        char if char.isprintable() else ("|" if char == "\n" else "?")
        for char in text.strip()
    )
    if not printable:
        return f"{outcome}; no stderr"
    if truncated:
        printable = "..." + printable
    return f"{outcome}; stderr tail: {printable}"


def _run_isolated_module(
    root_fd: int,
    interpreter_relative_name: str,
    module: str,
    *,
    request: Mapping[str, object] | None = None,
    wheel_fds: Sequence[int] = (),
    program: bytes | None = None,
) -> Mapping[str, object]:
    interpreter_fd = _open_regular_fd_at(
        root_fd, interpreter_relative_name, code="extension_runtime_toolchain_mismatch"
    )
    try:
        return _run_isolated_module_with_interpreter_fd(
            root_fd,
            interpreter_fd,
            module,
            request=request,
            wheel_fds=wheel_fds,
            program=program,
        )
    finally:
        os.close(interpreter_fd)


def _run_isolated_module_with_interpreter_fd(
    root_fd: int,
    interpreter_fd: int,
    module: str,
    *,
    request: Mapping[str, object] | None = None,
    wheel_fds: Sequence[int] = (),
    program: bytes | None = None,
    interpreter_expected: bytes | None = None,
) -> Mapping[str, object]:
    expected_program = {
        "okg_extension_bootstrap": _BOOTSTRAP_SOURCE,
        "okg_extension_builder": _BUILDER_SOURCE,
    }.get(module)
    if expected_program is None or (
        program is not None and program != expected_program
    ):
        raise _refuse(
            "extension_builder_identity_mismatch",
            "isolated tool program differs from its admitted bytes",
            path=module,
        )
    program = expected_program
    if interpreter_expected is None:
        interpreter_expected = _read_open_file_payload(interpreter_fd)
    _verify_open_interpreter_payload(interpreter_fd, interpreter_expected)
    source = program.decode("utf-8")
    # Descriptor 2 carries interpreter startup errors to the parent. The
    # wrapper retires it before the admitted program runs, so the program
    # still observes exactly the descriptors the bootstrap proof names.
    wrapper = (
        "import os,sys\n"
        "sys.stderr=sys.__stderr__=None\n"
        "os.close(2)\n"
        f"exec(compile({source!r},{module!r},'exec'))\n"
        "os.close(5)\n"
        "os.read(8,1)\n"
    )
    request_read, request_write = os.pipe()
    result_read, result_write = os.pipe()
    acknowledgement_read, acknowledgement_write = os.pipe()
    stderr_read, stderr_write = os.pipe()
    stderr_tail = _StderrTail()
    high_sources: list[int] = []
    pid = -1
    reaped = False
    with tempfile.TemporaryDirectory(prefix="okg-extension-empty-cwd-") as cwd_name:
        cwd_fd = _open_absolute_directory(
            Path(cwd_name).resolve(),
            code="extension_builder_input_mismatch",
            label="isolated_cwd",
        )
        try:
            for fd in wheel_fds:
                if (fcntl.fcntl(fd, fcntl.F_GETFL) & os.O_ACCMODE) != os.O_RDONLY:
                    raise _refuse(
                        "extension_builder_input_mismatch",
                        "builder wheel descriptor is not read-only",
                    )
            root_source = _high_dup(root_fd)
            interpreter_source = _high_dup(interpreter_fd)
            request_source = _high_dup(request_read)
            result_source = _high_dup(result_write)
            cwd_source = _high_dup(cwd_fd)
            acknowledgement_source = _high_dup(acknowledgement_read)
            stderr_source = _high_dup(stderr_write)
            high_sources.extend(
                [
                    root_source,
                    interpreter_source,
                    request_source,
                    result_source,
                    cwd_source,
                    acknowledgement_source,
                    stderr_source,
                ]
            )
            wheel_sources = [_high_dup(fd) for fd in wheel_fds]
            high_sources.extend(wheel_sources)
            pid = os.fork()
            if pid == 0:  # pragma: no cover - observed through parent receipt
                try:
                    targets = {
                        stderr_source: 2,
                        request_source: 3,
                        root_source: 4,
                        result_source: 5,
                        interpreter_source: 6,
                        cwd_source: 7,
                        acknowledgement_source: 8,
                    }
                    targets.update(
                        {
                            source: 10 + index
                            for index, source in enumerate(wheel_sources)
                        }
                    )
                    for source, target in targets.items():
                        if source != target:
                            os.dup2(source, target, inheritable=True)
                    keep = sorted(
                        {
                            2,
                            3,
                            4,
                            5,
                            6,
                            7,
                            8,
                            *(10 + index for index in range(len(wheel_sources))),
                        }
                    )
                    maximum = resource.getrlimit(resource.RLIMIT_NOFILE)[0]
                    if maximum == resource.RLIM_INFINITY:
                        maximum = 65536
                    cursor = 0
                    for kept in keep:
                        if cursor < kept:
                            os.closerange(cursor, kept)
                        cursor = kept + 1
                    os.closerange(cursor, int(maximum))
                    os.fchdir(7)
                    os.close(7)
                    # argv[0] names the executed descriptor. CPython locates
                    # its prefix from argv[0]; a relative name in the empty
                    # cwd sends it to the build-time prefix instead (#2352).
                    _exec_fd(
                        6,
                        [CHILD_ARGV0, "-I", "-s", "-E", "-B", "-c", wrapper],
                        {
                            "LANG": "C.UTF-8",
                            "LC_ALL": "C.UTF-8",
                            "PYTHONDONTWRITEBYTECODE": "1",
                        },
                    )
                except Exception as exc:
                    # After fork, the child exits; the parent validates its status
                    # and reports this best-effort line from descriptor 2.
                    try:
                        os.write(
                            2,
                            f"isolated launch failed: {type(exc).__name__}\n".encode(
                                "ascii", "replace"
                            ),
                        )
                    except OSError:
                        pass
                    os._exit(127)
            for source in high_sources:
                os.close(source)
            high_sources.clear()
            os.close(request_read)
            request_read = -1
            os.close(result_write)
            result_write = -1
            os.close(acknowledgement_read)
            acknowledgement_read = -1
            os.close(stderr_write)
            stderr_write = -1
            os.set_blocking(stderr_read, False)
            payload = b"" if request is None else canonical_json_bytes(request)
            view = memoryview(payload)
            request_refused = False
            while view:
                try:
                    written = os.write(request_write, view)
                except BrokenPipeError:
                    # The child closed its request pipe (usually by exiting);
                    # the reap below collects the status the refusal reports.
                    request_refused = True
                    break
                if written <= 0:
                    raise OSError("short request write")
                view = view[written:]
            os.close(request_write)
            request_write = -1
            os.set_blocking(result_read, False)
            deadline = time.monotonic() + CHILD_TIMEOUT_SECONDS
            chunks: list[bytes] = []
            status: int | None = None
            result_eof = False
            while not result_eof:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    os.kill(pid, signal.SIGKILL)
                    os.waitpid(pid, 0)
                    reaped = True
                    raise _refuse(
                        "extension_builder_input_mismatch", "isolated tool timed out"
                    )
                watched = [result_read]
                if stderr_read >= 0:
                    watched.append(stderr_read)
                ready, _, _ = select.select(watched, [], [], min(remaining, 0.1))
                if stderr_read in ready and not _drain_stderr_tail(
                    stderr_read, stderr_tail, deadline=deadline
                ):
                    os.close(stderr_read)
                    stderr_read = -1
                readable = result_read in ready
                if readable:
                    chunk = os.read(result_read, 65536)
                    if chunk:
                        chunks.append(chunk)
                        if sum(len(item) for item in chunks) > MAX_CHILD_RESULT_BYTES:
                            os.kill(pid, signal.SIGKILL)
                            os.waitpid(pid, 0)
                            reaped = True
                            raise _refuse(
                                "extension_builder_input_mismatch",
                                "isolated tool result exceeded its limit",
                            )
                    else:
                        result_eof = True
                waited, raw_status = os.waitpid(pid, os.WNOHANG)
                if waited == pid:
                    status = raw_status
                    reaped = True
                    if not readable:
                        try:
                            while True:
                                chunk = os.read(result_read, 65536)
                                if not chunk:
                                    break
                                chunks.append(chunk)
                        except BlockingIOError:
                            # The child has exited, so no later bytes can arrive.
                            pass
                        result_eof = True
            if status is None:
                try:
                    _verify_open_interpreter_payload(
                        interpreter_fd, interpreter_expected
                    )
                except BaseException:
                    os.kill(pid, signal.SIGKILL)
                    os.waitpid(pid, 0)
                    reaped = True
                    raise
                try:
                    os.write(acknowledgement_write, b"1")
                except BrokenPipeError:
                    # The child exited after closing its result pipe; the reap
                    # below collects the status that the refusal reports.
                    pass
                os.close(acknowledgement_write)
                acknowledgement_write = -1
                while True:
                    waited, raw_status = os.waitpid(pid, os.WNOHANG)
                    if waited == pid:
                        status = raw_status
                        reaped = True
                        break
                    if time.monotonic() >= deadline:
                        os.kill(pid, signal.SIGKILL)
                        os.waitpid(pid, 0)
                        reaped = True
                        raise _refuse(
                            "extension_builder_input_mismatch",
                            "isolated tool did not exit after verification",
                        )
                    time.sleep(0.01)
            result_payload = b"".join(chunks)
            if (
                request_refused
                or status is None
                or not os.WIFEXITED(status)
                or os.WEXITSTATUS(status) != 0
            ):
                if stderr_read >= 0:
                    # The child is reaped, so the pipe holds all it will get;
                    # still bound the read in case a descendant keeps it open.
                    _drain_stderr_tail(
                        stderr_read, stderr_tail, deadline=time.monotonic() + 1.0
                    )
                prefix = (
                    "isolated tool closed its request before reading it: "
                    if request_refused
                    else "isolated tool refused or exited: "
                )
                raise _refuse(
                    "extension_builder_input_mismatch",
                    prefix + stderr_tail.describe(status),
                )
            return _strict_json(result_payload, "isolated_tool_result")
        finally:
            if pid > 0 and not reaped:
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                try:
                    os.waitpid(pid, 0)
                except ChildProcessError:
                    pass
            os.close(cwd_fd)
            for fd in high_sources:
                try:
                    os.close(fd)
                except OSError:
                    # These aliases are retired; retry could close a reused fd.
                    pass
            for fd in (
                request_read,
                request_write,
                result_read,
                result_write,
                acknowledgement_read,
                acknowledgement_write,
                stderr_read,
                stderr_write,
            ):
                if fd >= 0:
                    try:
                        os.close(fd)
                    except OSError:
                        # A normal path may already have retired this pipe end.
                        pass


def _bootstrap_runtime(
    root_fd: int,
    interpreter_fd: int,
    toolchain: VerifiedRuntimeToolchain,
) -> CanonicalRecordRef:
    request = {
        "schema_version": BOOTSTRAP_REQUEST_SCHEMA,
        "runtime_layout_digest": toolchain.runtime_layout_digest.to_record(),
        "python_abi": toolchain.identity.python_abi,
        "builtin_modules": list(toolchain.builtin_modules),
        "frozen_modules": list(toolchain.frozen_modules),
        "native_shared_libraries": list(toolchain.native_shared_libraries),
        "interpreter_member": toolchain.component_members[
            "interpreter"
        ].runtime_relative_name,
    }
    result = _run_isolated_module_with_interpreter_fd(
        root_fd,
        interpreter_fd,
        "okg_extension_bootstrap",
        request=request,
        program=toolchain.components["bootstrap_verifier"],
        interpreter_expected=toolchain.components["interpreter"],
    )
    _exact_keys(
        result,
        {
            "schema_version",
            "isolated",
            "ignore_environment",
            "no_user_site",
            "dont_write_bytecode",
            "outside_prefix",
            "prefix_entries",
            "python_abi",
            "builtin_modules",
            "frozen_modules",
            "executed_member_matches",
            "executable_inside_prefix",
            "base_prefix_inside_prefix",
            "module_origins_inside_prefix",
            "cwd_empty",
            "inherited_fds",
        },
        "bootstrap_proof",
    )
    if (
        result.get("schema_version") != BOOTSTRAP_PROOF_SCHEMA
        or not result.get("isolated")
        or not result.get("ignore_environment")
        or not result.get("no_user_site")
        or not result.get("dont_write_bytecode")
        or result.get("outside_prefix")
        or result.get("python_abi") != toolchain.identity.python_abi
        or result.get("builtin_modules") != list(toolchain.builtin_modules)
        or result.get("frozen_modules") != list(toolchain.frozen_modules)
        or not result.get("executed_member_matches")
        or not result.get("executable_inside_prefix")
        or not result.get("base_prefix_inside_prefix")
        or not result.get("module_origins_inside_prefix")
        or not result.get("cwd_empty")
        or result.get("inherited_fds") != [3, 4, 5, 6, 8]
    ):
        raise _refuse(
            "extension_runtime_toolchain_mismatch",
            "copied interpreter startup proof admitted an ambient root",
        )
    entries = result.get("prefix_entries")
    site = _site_packages_relative(toolchain.identity.python_abi)
    if not isinstance(entries, list) or site not in entries:
        raise _refuse(
            "extension_runtime_toolchain_mismatch",
            "copied interpreter does not select the verified site-packages root",
        )
    return CanonicalRecordRef(BOOTSTRAP_PROOF_SCHEMA, canonical_record_digest(result))


def _all_expected_files(
    toolchain: VerifiedRuntimeToolchain,
    installs: Sequence[_WheelInstall],
) -> dict[str, tuple[bytes, str, CanonicalRecordRef | None, bool]]:
    files: dict[str, tuple[bytes, str, CanonicalRecordRef | None, bool]] = {}

    def add(
        name: str,
        payload: bytes,
        owner: str,
        record: CanonicalRecordRef | None,
        executable: bool,
    ) -> None:
        safe = _safe_relative_name(name, "installed_tree")
        folded = safe.casefold()
        if safe in files or any(value.casefold() == folded for value in files):
            raise _refuse(
                "extension_installed_name_collision",
                "installed members collide",
                path=safe,
            )
        files[safe] = (payload, owner, record, executable)

    for name in (
        "interpreter",
        "bootstrap_verifier",
        "builder",
        "broker",
        "containment_launcher",
    ):
        member = toolchain.component_members[name]
        add(
            member.runtime_relative_name,
            toolchain.components[name],
            "runtime-toolchain",
            None,
            member.executable,
        )
    for member, payload in toolchain.stdlib_members:
        add(member.relative_name, payload, "python-stdlib", None, member.executable)
    site = _site_packages_relative(toolchain.identity.python_abi)
    for install in installs:
        for name, payload, record in install.files:
            add(
                f"{site}/{name}",
                payload,
                install.distribution.name,
                record,
                False,
            )
    return files


def _materialize_toolchain(root_fd: int, toolchain: VerifiedRuntimeToolchain) -> None:
    for name in (
        "interpreter",
        "bootstrap_verifier",
        "builder",
        "broker",
        "containment_launcher",
    ):
        member = toolchain.component_members[name]
        _write_file_at(
            root_fd,
            member.runtime_relative_name,
            toolchain.components[name],
            executable=member.executable,
        )
    for member, payload in toolchain.stdlib_members:
        _write_file_at(
            root_fd, member.relative_name, payload, executable=member.executable
        )


def _open_verified_materialized_interpreter(
    root_fd: int,
    toolchain: VerifiedRuntimeToolchain,
) -> int:
    member = toolchain.component_members["interpreter"]
    expected = toolchain.components["interpreter"]
    interpreter_fd = _open_regular_fd_at(
        root_fd,
        member.runtime_relative_name,
        code="extension_runtime_toolchain_mismatch",
    )
    try:
        before = os.fstat(interpreter_fd)
        chunks: list[bytes] = []
        while True:
            chunk = os.read(interpreter_fd, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        after = os.fstat(interpreter_fd)
        payload = b"".join(chunks)
        if (
            _stable_file_token(before) != _stable_file_token(after)
            or payload != expected
            or RawBlobRef(raw_blob_digest(payload), len(payload))
            != toolchain.identity.interpreter_raw_blob
            or stat.S_IMODE(after.st_mode) != 0o700
        ):
            raise _refuse(
                "extension_runtime_toolchain_mismatch",
                "materialized interpreter differs from the selected release member",
                path=member.runtime_relative_name,
            )
        os.lseek(interpreter_fd, 0, os.SEEK_SET)
        return interpreter_fd
    except BaseException:
        os.close(interpreter_fd)
        raise


def _read_open_file_payload(member_fd: int) -> bytes:
    before = os.fstat(member_fd)
    chunks: list[bytes] = []
    offset = 0
    while True:
        chunk = os.pread(member_fd, 1024 * 1024, offset)
        if not chunk:
            break
        chunks.append(chunk)
        offset += len(chunk)
    after = os.fstat(member_fd)
    if _stable_file_token(before) != _stable_file_token(after):
        raise _refuse(
            "extension_runtime_toolchain_mismatch",
            "runtime executable changed while verified",
        )
    return b"".join(chunks)


def _verify_open_interpreter_payload(interpreter_fd: int, expected: bytes) -> None:
    observed = os.fstat(interpreter_fd)
    payload = _read_open_file_payload(interpreter_fd)
    if (
        not stat.S_ISREG(observed.st_mode)
        or observed.st_nlink != 1
        or stat.S_IMODE(observed.st_mode) != 0o700
        or payload != expected
    ):
        raise _refuse(
            "extension_runtime_toolchain_mismatch",
            "executed interpreter differs from the admitted bytes",
        )


def _tree_entries(
    root_fd: int,
    expected_files: Mapping[str, tuple[bytes, str, CanonicalRecordRef | None, bool]],
    *,
    sealed: bool,
) -> tuple[InstalledTreeEntry, ...]:
    expected_directories: set[str] = set()
    for name in expected_files:
        parent = PurePosixPath(name).parent
        while parent.as_posix() != ".":
            expected_directories.add(parent.as_posix())
            parent = parent.parent
    expected_names = set(expected_files) | expected_directories
    observed_names: set[str] = set()
    entries: list[InstalledTreeEntry] = []
    root_stat = os.fstat(root_fd)
    root_device = root_stat.st_dev

    def walk(directory_fd: int, prefix: str) -> None:
        before_directory = os.fstat(directory_fd)
        names = os.listdir(directory_fd)
        if len(names) != len(set(names)) or len(
            {name.casefold() for name in names}
        ) != len(names):
            raise _refuse(
                "extension_installed_name_collision", "runtime directory names collide"
            )
        for name in sorted(names, key=lambda value: value.encode("utf-8")):
            if name in {"", ".", ".."} or "/" in name or "\\" in name or "\x00" in name:
                raise _refuse(
                    "extension_installed_tree_mismatch",
                    "runtime member name is invalid",
                )
            relative = f"{prefix}/{name}" if prefix else name
            try:
                child_fd = os.open(
                    name,
                    os.O_RDONLY
                    | os.O_CLOEXEC
                    | getattr(os, "O_NOFOLLOW", 0)
                    | getattr(os, "O_NONBLOCK", 0),
                    dir_fd=directory_fd,
                )
            except OSError as exc:
                refusal = (
                    "extension_runtime_symlink_refused"
                    if exc.errno in {errno.ELOOP, errno.ENOTDIR}
                    else "extension_installed_tree_mismatch"
                )
                raise _refuse(
                    refusal, "runtime member could not be opened", path=relative
                ) from exc
            info = os.fstat(child_fd)
            if info.st_dev != root_device:
                os.close(child_fd)
                raise _refuse(
                    "extension_runtime_unsealed",
                    "runtime tree crosses a mount boundary",
                    path=relative,
                )
            if stat.S_ISLNK(info.st_mode):
                os.close(child_fd)
                raise _refuse(
                    "extension_runtime_symlink_refused",
                    "runtime contains a symlink",
                    path=relative,
                )
            if relative not in expected_names:
                os.close(child_fd)
                raise _refuse(
                    "extension_installed_tree_mismatch",
                    "runtime has an unexpected member",
                    path=relative,
                )
            observed_names.add(relative)
            if stat.S_ISDIR(info.st_mode):
                try:
                    if sealed and stat.S_IMODE(os.fstat(child_fd).st_mode) != 0o555:
                        raise _refuse(
                            "extension_runtime_writable",
                            "runtime directory is writable",
                            path=relative,
                        )
                    entries.append(
                        InstalledTreeEntry(
                            relative, "directory", 0o555, 0, None, "runtime"
                        )
                    )
                    walk(child_fd, relative)
                finally:
                    os.close(child_fd)
                continue
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                os.close(child_fd)
                raise _refuse(
                    "extension_installed_tree_mismatch",
                    "runtime member is not a single regular file",
                    path=relative,
                )
            if relative not in expected_files:
                os.close(child_fd)
                raise _refuse(
                    "extension_installed_tree_mismatch",
                    "runtime member kind changed",
                    path=relative,
                )
            expected_payload, owner, record, executable = expected_files[relative]
            try:
                chunks: list[bytes] = []
                while True:
                    chunk = os.read(child_fd, 1024 * 1024)
                    if not chunk:
                        break
                    chunks.append(chunk)
                after = os.fstat(child_fd)
            finally:
                os.close(child_fd)
            if _stable_file_token(info) != _stable_file_token(after):
                raise _refuse(
                    "extension_installed_tree_mismatch",
                    "runtime member changed while read",
                    path=relative,
                )
            payload = b"".join(chunks)
            if payload != expected_payload:
                raise _refuse(
                    "extension_installed_tree_mismatch",
                    "runtime member bytes changed",
                    path=relative,
                )
            expected_mode = 0o555 if executable else 0o444
            if sealed and stat.S_IMODE(info.st_mode) != expected_mode:
                code = (
                    "extension_runtime_writable"
                    if stat.S_IMODE(info.st_mode) & 0o222
                    else "extension_installed_tree_mismatch"
                )
                raise _refuse(code, "runtime member mode changed", path=relative)
            entries.append(
                InstalledTreeEntry(
                    relative,
                    "executable" if executable else "regular",
                    expected_mode,
                    len(payload),
                    RawBlobRef(raw_blob_digest(payload), len(payload)),
                    owner,
                    record,
                )
            )

        if _stable_file_token(before_directory) != _stable_file_token(
            os.fstat(directory_fd)
        ):
            raise _refuse(
                "extension_installed_tree_mismatch",
                "runtime directory changed while enumerated",
                path=prefix or ".",
            )

    walk(root_fd, "")
    if _stable_file_token(root_stat) != _stable_file_token(os.fstat(root_fd)):
        raise _refuse(
            "extension_installed_tree_mismatch",
            "runtime root changed while enumerated",
        )
    if observed_names != expected_names:
        raise _refuse("extension_installed_tree_mismatch", "runtime member is missing")
    return tuple(sorted(entries, key=lambda value: value.relative_name.encode("utf-8")))


def _seal_modes(
    root_fd: int,
    expected_files: Mapping[str, tuple[bytes, str, CanonicalRecordRef | None, bool]],
) -> None:
    for name, (_payload, _owner, _record, executable) in expected_files.items():
        fd = _open_regular_fd_at(root_fd, name, code="extension_runtime_unsealed")
        try:
            os.fchmod(fd, 0o555 if executable else 0o444)
            os.fsync(fd)
        finally:
            os.close(fd)
    directories: set[str] = set()
    for name in expected_files:
        parent = PurePosixPath(name).parent
        while parent.as_posix() != ".":
            directories.add(parent.as_posix())
            parent = parent.parent
    for name in sorted(directories, key=lambda value: -value.count("/")):
        parent_fd, leaf = _open_parent_at(
            root_fd, name, code="extension_runtime_unsealed"
        )
        try:
            fd = os.open(leaf, _directory_flags(), dir_fd=parent_fd)
            try:
                os.fchmod(fd, 0o555)
                os.fsync(fd)
            finally:
                os.close(fd)
        finally:
            os.close(parent_fd)
    os.fchmod(root_fd, 0o555)
    os.fsync(root_fd)


def _entry_digest(entries: Sequence[InstalledTreeEntry]) -> CanonicalRecordRef:
    return CanonicalRecordRef(
        INSTALLED_TREE_SCHEMA,
        canonical_record_digest(
            {
                "schema_version": INSTALLED_TREE_SCHEMA,
                "entries": [entry.to_record() for entry in entries],
            }
        ),
    )


@dataclass(frozen=True, slots=True)
class SealedRuntimeReceiptV1:
    package_plan_digest: CanonicalRecordRef
    builder_identity_digest: CanonicalRecordRef
    builder_inputs_digest: CanonicalRecordRef
    builder_execution_receipt_digest: CanonicalRecordRef
    runtime_toolchain_manifest_digest: CanonicalRecordRef
    runtime_layout_digest: CanonicalRecordRef
    bootstrap_proof_digest: CanonicalRecordRef
    interpreter_raw_blob: RawBlobRef
    python_abi: str
    platform_identity: Mapping[str, str]
    public_sdk_raw_blob: RawBlobRef
    child_host_raw_blob: RawBlobRef
    installed_tree_digest: CanonicalRecordRef
    installed_tree: tuple[InstalledTreeEntry, ...]
    installed_distributions: tuple[InstalledDistribution, ...]
    stdlib_manifest_digest: CanonicalRecordRef
    import_policy_digest: CanonicalRecordRef
    containment_profile_manifest_digest: CanonicalRecordRef
    sealed_runtime_digest: CanonicalRecordRef

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "platform_identity",
            MappingProxyType(dict(self.platform_identity)),
        )

    def to_record_without_digest(self) -> dict[str, object]:
        return {
            "schema_version": SEALED_RUNTIME_SCHEMA,
            "package_plan_digest": self.package_plan_digest.to_record(),
            "builder_identity_digest": self.builder_identity_digest.to_record(),
            "builder_inputs_digest": self.builder_inputs_digest.to_record(),
            "builder_execution_receipt_digest": self.builder_execution_receipt_digest.to_record(),
            "runtime_toolchain_manifest_digest": self.runtime_toolchain_manifest_digest.to_record(),
            "runtime_layout_digest": self.runtime_layout_digest.to_record(),
            "bootstrap_proof_digest": self.bootstrap_proof_digest.to_record(),
            "interpreter_raw_blob": self.interpreter_raw_blob.to_record(),
            "python_abi": self.python_abi,
            "platform_identity": dict(self.platform_identity),
            "public_sdk_raw_blob": self.public_sdk_raw_blob.to_record(),
            "child_host_raw_blob": self.child_host_raw_blob.to_record(),
            "installed_tree_digest": self.installed_tree_digest.to_record(),
            "installed_distributions": [
                value.to_record() for value in self.installed_distributions
            ],
            "stdlib_manifest_digest": self.stdlib_manifest_digest.to_record(),
            "import_policy_digest": self.import_policy_digest.to_record(),
            "containment_profile_manifest_digest": self.containment_profile_manifest_digest.to_record(),
        }

    def to_record(self) -> dict[str, object]:
        value = self.to_record_without_digest()
        value["schema_version"] = SEAL_RECEIPT_SCHEMA
        value["sealed_runtime_digest"] = self.sealed_runtime_digest.to_record()
        value["installed_tree"] = [entry.to_record() for entry in self.installed_tree]
        return value


@dataclass(slots=True)
class SealedRuntimeHandle:
    """Host-only locator with a retained root descriptor and path-free receipt."""

    _receipt: SealedRuntimeReceiptV1 = field(repr=False)
    runtime_locator: Path
    _root_fd: int = field(repr=False)
    _root_identity: tuple[int, int, int, int, int] = field(repr=False)
    _expected_files: Mapping[
        str, tuple[bytes, str, CanonicalRecordRef | None, bool]
    ] = field(repr=False)
    _issued_receipt_bytes: bytes = field(init=False, repr=False)
    _closed: bool = field(default=False, init=False, repr=False)
    _lock: threading.RLock = field(
        default_factory=threading.RLock, init=False, repr=False
    )

    def __post_init__(self) -> None:
        self._issued_receipt_bytes = canonical_json_bytes(self._receipt.to_record())

    @property
    def receipt(self) -> SealedRuntimeReceiptV1:
        return self._receipt

    def identity_record(self) -> dict[str, object]:
        return self.receipt.to_record()

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            fd = self._root_fd
            self._root_fd = -1
            self._closed = True
            try:
                os.close(fd)
            except OSError:
                # Ownership is already retired; retrying could close a reused fd.
                pass

    def __del__(self) -> None:  # pragma: no cover
        self.close()


def seal_extension_runtime(
    plan: PackagePlanV2,
    package: _VerifiedPackage,
    toolchain: VerifiedRuntimeToolchain,
    *,
    destination: str | Path,
    environ: Mapping[str, str] | None = None,
) -> SealedRuntimeHandle:
    """Execute the exact builder and independently seal its installed tree."""

    _assert_empty_authority_environment(environ)
    verify_package_plan_v2(plan, package=package)
    toolchain = verify_runtime_toolchain(toolchain)
    if (
        plan.runtime_toolchain != toolchain.identity
        or plan.runtime_toolchain.runtime_layout_digest
        != toolchain.runtime_layout_digest
    ):
        raise _refuse(
            "extension_runtime_toolchain_mismatch", "plan and runtime layout differ"
        )
    extension_payload = _package_object(package, plan.extension_wheel)
    dependency_payloads = [
        _package_object(package, wheel) for wheel in plan.dependency_wheels
    ]
    wheel_material = [
        ("extension", extension_payload, plan.extension_wheel),
        *(
            ("dependency", payload, wheel)
            for payload, wheel in zip(
                dependency_payloads, plan.dependency_wheels, strict=True
            )
        ),
        ("public_sdk", toolchain.components["public_sdk"], None),
        ("child_host", toolchain.components["child_host"], None),
    ]
    installs = tuple(
        _wheel_install(payload, projection)
        for _role, payload, projection in wheel_material
    )
    names = [value.distribution.name for value in installs]
    if len(names) != len(set(names)):
        raise _refuse(
            "extension_installed_name_collision", "installed distribution names collide"
        )
    roots: dict[str, str] = {}
    for install in installs:
        for root in install.distribution.import_roots:
            previous = roots.setdefault(root, install.distribution.name)
            if previous != install.distribution.name:
                raise _refuse(
                    "extension_installed_name_collision",
                    "installed import roots collide",
                    path=root,
                )
    expected_files = _all_expected_files(toolchain, installs)
    root_fd, _root_identity = _create_runtime_root(Path(destination))
    wheel_handles: list[object] = []
    interpreter_fd = -1
    try:
        _materialize_toolchain(root_fd, toolchain)
        interpreter_fd = _open_verified_materialized_interpreter(root_fd, toolchain)
        bootstrap_proof = _bootstrap_runtime(root_fd, interpreter_fd, toolchain)
        wheel_fds: list[int] = []
        request_wheels: list[dict[str, object]] = []
        for index, ((role, payload, projection), install) in enumerate(
            zip(wheel_material, installs, strict=True)
        ):
            handle, fd = _temporary_wheel_fd(payload)
            wheel_handles.append(handle)
            wheel_fds.append(fd)
            request_wheels.append(
                {
                    "object_role": role,
                    "raw_blob": install.distribution.wheel_raw_blob.to_record(),
                    "wheel_record_manifest_digest": install.distribution.wheel_record_manifest_digest.to_record(),
                    "read_fd_slot": 10 + index,
                }
            )
        request_wheels.sort(
            key=lambda value: (
                str(value["object_role"]),
                str(_mapping(value["raw_blob"], "builder_request.raw_blob")["digest"]),
            )
        )
        builder_identity = CanonicalRecordRef(
            BUILDER_SCHEMA,
            canonical_record_digest(
                {
                    "schema_version": BUILDER_SCHEMA,
                    "builder_raw_blob": toolchain.identity.builder_raw_blob.to_record(),
                    "installer_algorithm_version": INSTALLER_ALGORITHM_VERSION,
                    "wheel_install_scheme": WHEEL_INSTALL_SCHEME,
                    "tree_normalization_profile": TREE_NORMALIZATION_PROFILE,
                }
            ),
        )
        builder_inputs = CanonicalRecordRef(
            BUILDER_INPUTS_SCHEMA,
            canonical_record_digest(
                {
                    "schema_version": BUILDER_INPUTS_SCHEMA,
                    "package_plan_digest": plan.package_plan_digest.to_record(),
                    "runtime_toolchain_manifest_digest": toolchain.identity.runtime_toolchain_manifest_digest.to_record(),
                    "wheel_raw_blobs": sorted(
                        [
                            install.distribution.wheel_raw_blob.to_record()
                            for install in installs
                        ],
                        key=lambda value: str(value["digest"]),
                    ),
                    "target_platform_identity": dict(
                        toolchain.identity.platform_identity
                    ),
                    "target_python_abi": toolchain.identity.python_abi,
                    "source_date_epoch": 0,
                    "locale": "C.UTF-8",
                    "bytecode_policy": "absent",
                }
            ),
        )
        request = {
            "schema_version": BUILDER_REQUEST_SCHEMA,
            "builder_identity_digest": builder_identity.to_record(),
            "builder_inputs_digest": builder_inputs.to_record(),
            "runtime_layout_digest": toolchain.runtime_layout_digest.to_record(),
            "package_plan_digest": plan.package_plan_digest.to_record(),
            "wheels": request_wheels,
            "staging_root_fd_slot": 4,
            "site_packages_relative_name": _site_packages_relative(
                toolchain.identity.python_abi
            ),
            "source_date_epoch": 0,
            "locale": "C.UTF-8",
            "bytecode_policy": "absent",
        }
        builder_result = _run_isolated_module_with_interpreter_fd(
            root_fd,
            interpreter_fd,
            "okg_extension_builder",
            request=request,
            wheel_fds=wheel_fds,
            program=toolchain.components["builder"],
            interpreter_expected=toolchain.components["interpreter"],
        )
        if (
            builder_result.get("schema_version") != BUILDER_RESULT_SCHEMA
            or builder_result.get("status") != "installed"
        ):
            raise _refuse(
                "extension_builder_input_mismatch", "trusted builder did not install"
            )
        expected_result = [
            {
                "name": install.distribution.name,
                "version": install.distribution.version,
                "wheel_raw_blob": install.distribution.wheel_raw_blob.to_record(),
                "members": sorted(
                    f"{_site_packages_relative(toolchain.identity.python_abi)}/{name}"
                    for name, _payload, _record in install.files
                ),
            }
            for install in installs
        ]
        expected_result.sort(key=lambda value: str(value["name"]))
        if builder_result.get("installed_distributions") != expected_result:
            raise _refuse(
                "extension_builder_input_mismatch",
                "trusted builder result differs from independently verified wheels",
            )
        entries = _tree_entries(root_fd, expected_files, sealed=False)
        tree_digest = _entry_digest(entries)
        distributions = tuple(
            sorted(
                (value.distribution for value in installs), key=lambda value: value.name
            )
        )
        builder_execution = CanonicalRecordRef(
            BUILDER_EXECUTION_SCHEMA,
            canonical_record_digest(
                {
                    "schema_version": BUILDER_EXECUTION_SCHEMA,
                    "builder_identity_digest": builder_identity.to_record(),
                    "builder_inputs_digest": builder_inputs.to_record(),
                    "runtime_layout_digest": toolchain.runtime_layout_digest.to_record(),
                    "package_plan_digest": plan.package_plan_digest.to_record(),
                    "status": "installed",
                    "installed_tree_digest": tree_digest.to_record(),
                    "installed_distributions": [
                        value.to_record() for value in distributions
                    ],
                }
            ),
        )
        _seal_modes(root_fd, expected_files)
        entries = _tree_entries(root_fd, expected_files, sealed=True)
        if _entry_digest(entries) != tree_digest:
            raise _refuse(
                "extension_installed_tree_mismatch", "sealed tree identity changed"
            )
        provisional = SealedRuntimeReceiptV1(
            plan.package_plan_digest,
            builder_identity,
            builder_inputs,
            builder_execution,
            toolchain.identity.runtime_toolchain_manifest_digest,
            toolchain.runtime_layout_digest,
            bootstrap_proof,
            toolchain.identity.interpreter_raw_blob,
            toolchain.identity.python_abi,
            dict(toolchain.identity.platform_identity),
            toolchain.identity.public_sdk_raw_blob,
            toolchain.identity.child_host_raw_blob,
            tree_digest,
            entries,
            distributions,
            toolchain.identity.stdlib_manifest_digest,
            plan.import_policy_digest,
            plan.containment_profile_manifest_digest,
            CanonicalRecordRef(SEALED_RUNTIME_SCHEMA, "sha256:" + "0" * 64),
        )
        receipt = replace(
            provisional,
            sealed_runtime_digest=CanonicalRecordRef(
                SEALED_RUNTIME_SCHEMA,
                canonical_record_digest(provisional.to_record_without_digest()),
            ),
        )
        sealed_root = os.fstat(root_fd)
        sealed_root_identity = (
            sealed_root.st_dev,
            sealed_root.st_ino,
            sealed_root.st_mode,
            sealed_root.st_mtime_ns,
            sealed_root.st_ctime_ns,
        )
        handle = SealedRuntimeHandle(
            receipt,
            Path(destination),
            root_fd,
            sealed_root_identity,
            MappingProxyType(dict(expected_files)),
        )
        root_fd = -1
        return verify_sealed_runtime(handle)
    finally:
        for handle in wheel_handles:
            try:
                handle.close()  # type: ignore[attr-defined]
            except Exception:
                # Cleanup must not replace the sealing result or primary failure.
                pass
        if root_fd >= 0:
            os.close(root_fd)
        if interpreter_fd >= 0:
            os.close(interpreter_fd)


def verify_sealed_runtime(handle: SealedRuntimeHandle) -> SealedRuntimeHandle:
    """Revalidate the receipt and tree through the retained root descriptor."""

    receipt = handle.receipt
    if canonical_json_bytes(receipt.to_record()) != handle._issued_receipt_bytes:
        raise _refuse(
            "extension_sealed_runtime_mismatch",
            "sealed runtime receipt differs from the issued receipt",
        )
    if receipt.sealed_runtime_digest != CanonicalRecordRef(
        SEALED_RUNTIME_SCHEMA,
        canonical_record_digest(receipt.to_record_without_digest()),
    ):
        raise _refuse(
            "extension_sealed_runtime_mismatch", "sealed runtime receipt is stale"
        )
    if receipt.installed_tree_digest != _entry_digest(receipt.installed_tree):
        raise _refuse(
            "extension_installed_tree_mismatch", "installed tree receipt is stale"
        )
    with handle._lock:
        if handle._closed or handle._root_fd < 0:
            raise _refuse(
                "extension_runtime_unsealed", "sealed runtime authority is closed"
            )
        root_stat = os.fstat(handle._root_fd)
        root_identity = (
            root_stat.st_dev,
            root_stat.st_ino,
            root_stat.st_mode,
            root_stat.st_mtime_ns,
            root_stat.st_ctime_ns,
        )
        if (
            root_identity != handle._root_identity
            or stat.S_IMODE(root_stat.st_mode) != 0o555
        ):
            raise _refuse("extension_runtime_unsealed", "retained runtime root changed")
        observed = _tree_entries(handle._root_fd, handle._expected_files, sealed=True)
        if (
            observed != receipt.installed_tree
            or _entry_digest(observed) != receipt.installed_tree_digest
        ):
            raise _refuse(
                "extension_installed_tree_mismatch", "retained runtime tree changed"
            )
    return handle


__all__ = [
    "ExtensionRuntimeSealError",
    "InstalledDistribution",
    "InstalledTreeEntry",
    "SealedRuntimeHandle",
    "SealedRuntimeReceiptV1",
    "ToolchainComponent",
    "ToolchainMember",
    "VerifiedRuntimeToolchain",
    "bootstrap_verifier_program_bytes",
    "builder_program_bytes",
    "seal_extension_runtime",
    "select_runtime_toolchain",
    "verify_runtime_toolchain",
    "verify_sealed_runtime",
]
