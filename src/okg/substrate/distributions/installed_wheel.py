"""Read-only comparison of one installed distribution with separately pinned bytes.

This internal primitive establishes file integrity relative to a caller's release
pin, not publisher authenticity, imports, dependencies or publication authority.
It never imports inspected code. Shared namespaces, .data relocation, executable
caches and editable installs are unsupported; use a fresh --no-compile install.
Member paths are limited to 64 components and 4096 UTF-8 bytes.
Sibling distributions and generated console launchers are outside its scope.
Files can change after the comparison; the result is not an execution capability.
"""

from __future__ import annotations

import base64
import csv
import hashlib
import io
import json
import os
import re
import stat
import tempfile
from dataclasses import dataclass
from email.parser import BytesParser
from pathlib import Path

from .package import (
    LIMITS,
    DistributionPackageError,
    _file_identity,
    _open_anchor,
    _read_zip_member,
    _verified_wheel_inventory,
)


@dataclass(frozen=True)
class WheelPayloadFile:
    path: str
    digest: str
    size: int


@dataclass(frozen=True)
class VerifiedWheelPayload:
    wheel_digest: str
    distribution: str
    version: str
    files: tuple[WheelPayloadFile, ...]
    inventory_digest: str
    excluded_installer_metadata: tuple[str, ...]
    profile: str = "okg.installed-wheel-files/v1"


def _refuse(code: str, path: str, reason: str) -> None:
    raise DistributionPackageError(code, path, reason)


def _normal_name(value: str) -> str:
    return re.sub(r"[-_.]+", "-", value).lower()


def _directory_names(fd: int, *, limit: int) -> tuple[str, ...]:
    names: list[str] = []
    with os.scandir(fd) as entries:
        for entry in entries:
            if len(names) >= limit:
                _refuse(
                    "installed_wheel_limit", "directory", "too many installed entries"
                )
            names.append(entry.name)
    return tuple(names)


def _read_file(parent_fd: int, name: str, *, limit: int) -> bytes:
    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent_fd)
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode):
            _refuse("installed_wheel_path_unsafe", name, "expected regular file")
        if before.st_size > limit:
            _refuse("installed_wheel_limit", name, "file exceeds supported size")
        data = bytearray()
        while chunk := os.read(fd, min(1024 * 1024, limit + 1 - len(data))):
            data.extend(chunk)
            if len(data) > limit:
                _refuse("installed_wheel_limit", name, "file grew beyond size limit")
        if _file_identity(os.fstat(fd)) != _file_identity(before) or _file_identity(
            os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        ) != _file_identity(before):
            _refuse("installed_wheel_changed", name, "file changed during comparison")
        return bytes(data)
    finally:
        os.close(fd)


def _metadata(value: bytes, path: str):
    message = BytesParser().parsebytes(value)
    if message.defects:
        _refuse("installed_wheel_layout_unsupported", path, "malformed metadata")
    return message


def _installer_metadata(path: str, data: bytes) -> None:
    name = path.rsplit("/", 1)[-1]
    try:
        if name == "RECORD":
            rows = list(csv.reader(io.StringIO(data.decode("utf-8")), strict=True))
            if not rows or any(len(row) != 3 for row in rows):
                raise ValueError("record")
        elif name == "REQUESTED":
            if data:
                raise ValueError("requested")
        elif name == "INSTALLER":
            if not re.fullmatch(rb"[A-Za-z0-9_.-]+\n?", data):
                raise ValueError("installer")
        elif name == "direct_url.json":
            value = json.loads(data)
            if not isinstance(value, dict) or not isinstance(value.get("url"), str):
                raise ValueError("direct URL")
            # Directory origins are not wheel installations, even if the
            # editable flag is false. This metadata never supplies the pin.
            if "dir_info" in value or "vcs_info" in value:
                _refuse(
                    "installed_wheel_editable_unsupported", path, "non-wheel origin"
                )
            if not isinstance(value.get("archive_info"), dict):
                raise ValueError("archive origin")
    except (ValueError, UnicodeError, csv.Error) as exc:
        if isinstance(exc, DistributionPackageError):
            raise
        _refuse("installed_wheel_metadata_invalid", path, "invalid installer metadata")


def verify_installed_wheel_payload(
    wheel_path: Path,
    *,
    expected_wheel_digest: str,
    installed_root: Path,
) -> VerifiedWheelPayload:
    """Compare direct-layout files; expected digest must be independently supplied.

    Both paths must have symlink-free ancestors. Installed RECORD and narrowly
    named installer metadata are bounded/shape-checked but excluded from byte
    equivalence. No path from RECORD or direct_url is followed. The caller must
    separately establish actual module origins before using this in a runtime.
    """
    if not isinstance(expected_wheel_digest, str) or not re.fullmatch(
        r"sha256:[0-9a-f]{64}",
        expected_wheel_digest,
    ):
        _refuse(
            "installed_wheel_pin_invalid",
            "expected_wheel_digest",
            "SHA256 pin required",
        )
    if any(".." in Path(path).parts for path in (wheel_path, installed_root)):
        _refuse(
            "installed_wheel_path_unsafe",
            "filesystem",
            "parent traversal is unsupported",
        )
    wheel_path = Path(os.path.abspath(wheel_path))
    installed_root = Path(os.path.abspath(installed_root))
    try:
        parent = _open_anchor(wheel_path.parent, directory=True)
        try:
            wheel_bytes = _read_file(
                parent, wheel_path.name, limit=LIMITS["object_bytes_each"]
            )
        finally:
            os.close(parent)
        if "sha256:" + hashlib.sha256(wheel_bytes).hexdigest() != expected_wheel_digest:
            _refuse("installed_wheel_pin_mismatch", "wheel", "archive differs from pin")
        # All archive reads use the same bounded snapshot that matched the pin.
        with tempfile.SpooledTemporaryFile(max_size=8 * 1024 * 1024) as snapshot:
            snapshot.write(wheel_bytes)
            del wheel_bytes
            zf, names, observations = _verified_wheel_inventory(
                snapshot,
                snapshot.tell(),
                "wheel",
            )
            try:
                return _compare(
                    zf, names, observations, expected_wheel_digest, installed_root
                )
            finally:
                zf.close()
    except OSError as exc:
        raise DistributionPackageError(
            "installed_wheel_path_unsafe",
            "filesystem",
            "unavailable or unsafe input path",
        ) from exc


def _compare(zf, names, observations, pin: str, root: Path) -> VerifiedWheelPayload:
    paths = tuple(sorted(names))
    if any(len(path.split("/")) > 64 or len(path.encode()) > 4096 for path in paths):
        _refuse(
            "installed_wheel_layout_unsupported",
            "wheel",
            "path depth or length exceeds profile",
        )
    components = {
        "/".join(path.split("/")[:i])
        for path in paths
        for i in range(1, len(path.split("/")) + 1)
    }
    if len({path.casefold() for path in components}) != len(components):
        _refuse("installed_wheel_layout_unsupported", "wheel", "case-colliding members")
    metadata_path = next(path for path in paths if path.endswith(".dist-info/METADATA"))
    dist_info = metadata_path.split("/")[0]
    if any(part.endswith(".data") for path in paths for part in path.split("/")):
        _refuse(
            "installed_wheel_layout_unsupported", "wheel", "relocation or long path"
        )

    def member(path):
        return _read_zip_member(
            zf, names[path], path, limit=LIMITS["wheel_member_expanded_bytes"]
        )

    wheel_meta_path = dist_info + "/WHEEL"
    if wheel_meta_path not in names or dist_info + "/RECORD" not in names:
        _refuse(
            "installed_wheel_layout_unsupported",
            "wheel",
            "incoherent metadata directory",
        )
    metadata = _metadata(member(metadata_path), metadata_path)
    wheel_meta = _metadata(member(wheel_meta_path), wheel_meta_path)
    distribution, version = metadata.get("Name", ""), metadata.get("Version", "")
    if (
        len(metadata.get_all("Name", [])) != 1
        or len(metadata.get_all("Version", [])) != 1
        or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", distribution)
        or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.+!-]*", version)
        or dist_info != distribution.replace("-", "_") + "-" + version + ".dist-info"
        or wheel_meta.get_all("Root-Is-Purelib") != ["true"]
        or wheel_meta.get_all("Wheel-Version") != ["1.0"]
    ):
        _refuse(
            "installed_wheel_layout_unsupported",
            "wheel",
            "unsupported wheel identity/profile",
        )
    roots = {path.split("/")[0] for path in paths}
    packages = roots - {dist_info}
    if not packages or any(
        not name.isidentifier() or name + "/__init__.py" not in names
        for name in packages
    ):
        _refuse(
            "installed_wheel_layout_unsupported",
            "wheel",
            "non-package or shared namespace root",
        )
    cached = sorted(
        path for path in paths if path.endswith((".pyc", ".pyo", ".pth"))
    )
    if cached:
        _refuse(
            "installed_wheel_layout_unsupported",
            cached[0],
            "executable cache or path hook in wheel payload: "
            + cached[0]
            + (f" (and {len(cached) - 1} more)" if len(cached) > 1 else ""),
        )
    excluded = {
        dist_info + "/" + name
        for name in ("RECORD", "INSTALLER", "REQUESTED", "direct_url.json")
    }
    if set(names) & (excluded - {dist_info + "/RECORD"}):
        _refuse(
            "installed_wheel_layout_unsupported",
            "wheel",
            "installer metadata embedded in wheel",
        )
    expected = {
        path: WheelPayloadFile(
            path, "sha256:" + base64.urlsafe_b64decode(digest + "=").hex(), size
        )
        for path, (_, digest, size) in observations.items()
        if path not in excluded
    }
    seen: set[str] = set()
    ignored: set[str] = set()
    entries = 0
    total = 0

    def walk(fd: int, prefix: str) -> None:
        nonlocal entries, total
        before = _file_identity(os.fstat(fd))
        children = _directory_names(fd, limit=LIMITS["wheel_entries"] * 2 - entries)
        for name in children:
            entries += 1
            if entries > LIMITS["wheel_entries"] * 2:
                _refuse("installed_wheel_limit", prefix, "too many installed entries")
            path = prefix + "/" + name
            entry = os.stat(name, dir_fd=fd, follow_symlinks=False)
            if stat.S_ISDIR(entry.st_mode):
                if not any(item.startswith(path + "/") for item in expected):
                    _refuse("installed_wheel_extra", path, "unexpected directory")
                child = os.open(
                    name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd
                )
                try:
                    walk(child, path)
                    if _file_identity(os.fstat(child)) != _file_identity(
                        os.stat(name, dir_fd=fd, follow_symlinks=False)
                    ):
                        _refuse("installed_wheel_changed", path, "directory replaced")
                finally:
                    os.close(child)
            else:
                limit = expected[path].size if path in expected else 2 * 1024 * 1024
                if path not in expected and path not in excluded:
                    _refuse("installed_wheel_extra", path, "unexpected installed file")
                data = _read_file(fd, name, limit=limit)
                total += len(data)
                if total > LIMITS["wheel_expanded_bytes_total"]:
                    _refuse(
                        "installed_wheel_limit", prefix, "installed payload too large"
                    )
                if path in excluded:
                    _installer_metadata(path, data)
                    ignored.add(path)
                else:
                    if (
                        len(data) != expected[path].size
                        or "sha256:" + hashlib.sha256(data).hexdigest()
                        != expected[path].digest
                    ):
                        _refuse(
                            "installed_wheel_mismatch", path, "installed bytes differ"
                        )
                    seen.add(path)
        if before != _file_identity(os.fstat(fd)) or set(children) != set(
            _directory_names(fd, limit=len(children))
        ):
            _refuse(
                "installed_wheel_changed", prefix, "directory changed during comparison"
            )

    root_fd = _open_anchor(root, directory=True)
    try:
        root_identity = _file_identity(os.fstat(root_fd))
        children = _directory_names(root_fd, limit=LIMITS["wheel_entries"])
        for name in children:
            if name in roots:
                continue
            if any(
                name.casefold() == owned.casefold()
                or name.casefold().startswith(owned.casefold() + ".")
                for owned in packages
            ):
                _refuse("installed_wheel_extra", name, "conflicting package root")
            if name.endswith(".dist-info") and _normal_name(
                name[:-10].rsplit("-", 1)[0]
            ) == _normal_name(distribution):
                _refuse(
                    "installed_wheel_extra", name, "duplicate distribution metadata"
                )
        for name in sorted(roots):
            fd = os.open(
                name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=root_fd
            )
            try:
                walk(fd, name)
                if _file_identity(os.fstat(fd)) != _file_identity(
                    os.stat(name, dir_fd=root_fd, follow_symlinks=False)
                ):
                    _refuse("installed_wheel_changed", name, "package root replaced")
            finally:
                os.close(fd)
        reopened = _open_anchor(root, directory=True)
        try:
            if root_identity != _file_identity(os.fstat(reopened)) or set(
                children
            ) != set(_directory_names(root_fd, limit=len(children))):
                _refuse(
                    "installed_wheel_changed",
                    "installed_root",
                    "root changed during comparison",
                )
        finally:
            os.close(reopened)
    finally:
        os.close(root_fd)
    if seen != set(expected) or dist_info + "/RECORD" not in ignored:
        _refuse("installed_wheel_missing", "installed_root", "missing installed files")
    files = tuple(expected[path] for path in sorted(expected))
    inventory = json.dumps(
        [(item.path, item.digest, item.size) for item in files], separators=(",", ":")
    )
    return VerifiedWheelPayload(
        pin,
        distribution,
        version,
        files,
        "sha256:" + hashlib.sha256(inventory.encode()).hexdigest(),
        tuple(sorted(ignored)),
    )
