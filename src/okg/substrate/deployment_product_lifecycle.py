"""Fail-closed product request, admission, and journal-snapshot authority.

This bounded lifecycle slice verifies immutable package bytes, admits one exact
first-install request, and projects an admission-only journal snapshot. It owns no
production apply/resume controller, transition receipt verifier, runtime effect, or
completed lifecycle outcome.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import secrets
import stat
from collections.abc import Callable, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import MappingProxyType
from typing import Any, Final, Iterator

from okg.substrate.contracts.deployment_product.lock import (
    API_VERSION,
    PLAN_SCHEMA,
    ApplyAuthorization,
    DeploymentLockError,
    DeploymentProductPlan,
    PlanAction,
    canonical_json_bytes,
    canonical_lock_bytes,
    canonicalize_lock,
    require_plan_before_apply,
    sha256_digest,
)
from okg.substrate.contracts.deployment_product.models import (
    AuthoredDeploymentDefinition,
    ResolvedLock,
    SealedReleaseReference,
)
from okg.substrate.contracts.deployment_product.projections import (
    AgentDiagnostic,
    AgentStatusEnvelope,
)
from okg.substrate.distributions import (
    DistributionPackageError,
    VerifiedDistributionPackage,
    verify_distribution_package,
)
from okg.substrate.errors import DeploymentEnvironmentInvalid


PRODUCT_APPLY_REQUEST_SCHEMA: Final = "okg.deployment_product.apply_request/v1"
PRODUCT_OPERATION_STATUS_SCHEMA: Final = "okg.deployment_product.operation_status/v1"
PACKAGE_SELECTION_SCHEMA: Final = "okg.deployment_product.package_selection/v1"
PLAN_ADMISSION_SCHEMA: Final = "okg.deployment_product.plan_admission/v1"
APPROVAL_SCHEMA_V3: Final = "okg.deployment_promotion_approval/v3"
PROMOTION_STATE_SCHEMA: Final = "okg.deployment_promotion/v1"

_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_ID_RE = re.compile(r"^[^\s/\\]+$")
_PROMOTION_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_LOGICAL_PATH_RE = re.compile(r"^[^\x00\\]+$")
_PACKAGE_ASSET_REF_RE = re.compile(r"^[^\s:;,/\\]+:[^\s:;,/\\]+$")
_MAX_JSON_BYTES: Final = 16 * 1024 * 1024
_MAX_CONTAINED_ARTIFACT_BYTES: Final = 512 * 1024 * 1024


class ProductLifecycleError(DeploymentEnvironmentInvalid, ValueError):
    """A stable fail-closed product request or package refusal."""


def _refuse(message: str, **data: Any) -> ProductLifecycleError:
    return ProductLifecycleError(
        message,
        data=data,
        fix_hint="recreate the exact product request and authorities",
    )


def _utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise _refuse("timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)


def _canonical_json(value: Any) -> bytes:
    def normalize(item: Any) -> Any:
        if isinstance(item, datetime):
            return _utc(item).isoformat().replace("+00:00", "Z")
        if isinstance(item, Path):
            return item.as_posix()
        if isinstance(item, Mapping):
            return {str(key): normalize(child) for key, child in item.items()}
        if isinstance(item, (tuple, list)):
            return [normalize(child) for child in item]
        if isinstance(item, set | frozenset):
            return [normalize(child) for child in sorted(item, key=repr)]
        return item

    try:
        return json.dumps(
            normalize(value),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError) as exc:
        raise _refuse("value is not canonical JSON") from exc


def _freeze_json_authority(value: Any) -> Any:
    """Recursively freeze one already-validated JSON authority."""

    if isinstance(value, Mapping):
        return MappingProxyType(
            {key: _freeze_json_authority(item) for key, item in value.items()}
        )
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return tuple(_freeze_json_authority(item) for item in value)
    return value


def _copy_json_authority(value: Any) -> Any:
    """Return a recursively detached JSON-compatible copy."""

    if isinstance(value, Mapping):
        return {key: _copy_json_authority(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return [_copy_json_authority(item) for item in value]
    return value


def _digest(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_canonical_json(value)).hexdigest()


def _require_digest(value: Any, field: str) -> str:
    if not isinstance(value, str) or not _DIGEST_RE.fullmatch(value):
        raise _refuse(
            f"{field} must be sha256:<64 lowercase hexadecimal characters>",
            field=field,
        )
    return value


def _require_identifier(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value or not _ID_RE.fullmatch(value):
        raise _refuse(f"{field} must be a non-empty identifier", field=field)
    return value


def _require_promotion_id(value: Any) -> str:
    if not isinstance(value, str) or not _PROMOTION_ID_RE.fullmatch(value):
        raise _refuse("idempotency_key is not a valid promotion identifier")
    return value


def _require_relative_path(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise _refuse(f"{field} must be a relative POSIX path", field=field)
    if not _LOGICAL_PATH_RE.fullmatch(value) or value.startswith("/"):
        raise _refuse(f"{field} must be a relative POSIX path", field=field)
    if re.match(r"^[A-Za-z]:", value):
        raise _refuse(f"{field} must not be a drive path", field=field)
    if any(part in {"", ".", ".."} for part in value.split("/")):
        raise _refuse(f"{field} contains an unsafe path component", field=field)
    return value


def _require_package_asset_ref(value: Any, field: str = "asset_ref") -> str:
    if not isinstance(value, str) or not _PACKAGE_ASSET_REF_RE.fullmatch(value):
        raise _refuse(f"{field} must have the exact owner:id form", field=field)
    return value


def _locator(value: Any, field: str) -> str:
    if isinstance(value, Path):
        value = value.as_posix()
    if not isinstance(value, str) or not value or "\x00" in value:
        raise _refuse(f"{field} must be a non-empty locator", field=field)
    return value


def _strict_fields(raw: Mapping[str, Any], expected: set[str], *, scope: str) -> None:
    observed = set(raw)
    missing = sorted(expected - observed)
    unknown = sorted(observed - expected)
    if missing or unknown:
        raise _refuse(
            f"{scope} fields changed",
            missing_fields=missing,
            unknown_fields=unknown,
        )


def _closed_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _refuse("JSON contains a duplicate object key", key=key)
        result[key] = value
    return result


def _read_open_json_mapping(
    descriptor: int,
    *,
    current_stat: Callable[[], os.stat_result],
    scope: str,
    display_path: str,
) -> dict[str, Any]:
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
            raise _refuse(f"{scope} is not a regular file", path=display_path)
        if before.st_size > _MAX_JSON_BYTES:
            raise _refuse(
                f"{scope} exceeds the bounded JSON size",
                path=display_path,
                observed=before.st_size,
                maximum=_MAX_JSON_BYTES,
            )
        try:
            fcntl.flock(descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except OSError as exc:
            raise _refuse(f"{scope} is being modified", path=display_path) from exc
        chunks: list[bytes] = []
        observed_bytes = 0
        while chunk := os.read(descriptor, 1024 * 1024):
            observed_bytes += len(chunk)
            if observed_bytes > _MAX_JSON_BYTES:
                raise _refuse(
                    f"{scope} exceeds the bounded JSON size",
                    path=display_path,
                    observed=observed_bytes,
                    maximum=_MAX_JSON_BYTES,
                )
            chunks.append(chunk)
        after = os.fstat(descriptor)
        current = current_stat()
        final = os.fstat(descriptor)
        if (
            (before.st_dev, before.st_ino) != (current.st_dev, current.st_ino)
            or before.st_size != after.st_size
            or before.st_mtime_ns != after.st_mtime_ns
            or before.st_ctime_ns != after.st_ctime_ns
            or before.st_mode != after.st_mode
            or before.st_nlink != after.st_nlink
            or (
                after.st_dev,
                after.st_ino,
                after.st_size,
                after.st_mtime_ns,
                after.st_ctime_ns,
                after.st_mode,
                after.st_nlink,
            )
            != (
                current.st_dev,
                current.st_ino,
                current.st_size,
                current.st_mtime_ns,
                current.st_ctime_ns,
                current.st_mode,
                current.st_nlink,
            )
            or (
                after.st_dev,
                after.st_ino,
                after.st_size,
                after.st_mtime_ns,
                after.st_ctime_ns,
                after.st_mode,
                after.st_nlink,
            )
            != (
                final.st_dev,
                final.st_ino,
                final.st_size,
                final.st_mtime_ns,
                final.st_ctime_ns,
                final.st_mode,
                final.st_nlink,
            )
        ):
            raise _refuse(f"{scope} changed while being read", path=display_path)
        raw = json.loads(
            b"".join(chunks).decode("utf-8"),
            object_pairs_hook=_closed_json_object,
        )
    except ProductLifecycleError:
        raise
    except (OSError, UnicodeError, ValueError) as exc:
        raise _refuse(f"{scope} is unreadable JSON", path=display_path) from exc
    if not isinstance(raw, dict):
        raise _refuse(f"{scope} must be a JSON object", path=display_path)
    return dict(raw)


def _read_json_mapping(path: str | Path, *, scope: str) -> dict[str, Any]:
    target = Path(path)
    descriptor: int | None = None
    try:
        descriptor = os.open(
            target,
            os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK,
        )
        return _read_open_json_mapping(
            descriptor,
            current_stat=target.lstat,
            scope=scope,
            display_path=str(target),
        )
    except ProductLifecycleError:
        raise
    except OSError as exc:
        raise _refuse(f"{scope} is unavailable", path=str(target)) from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _read_json_mapping_at(
    directory_fd: int,
    name: str,
    *,
    scope: str,
    display_path: str,
) -> dict[str, Any]:
    descriptor: int | None = None
    try:
        descriptor = os.open(
            name,
            os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK,
            dir_fd=directory_fd,
        )
        return _read_open_json_mapping(
            descriptor,
            current_stat=lambda: os.stat(
                name,
                dir_fd=directory_fd,
                follow_symlinks=False,
            ),
            scope=scope,
            display_path=display_path,
        )
    except ProductLifecycleError:
        raise
    except OSError as exc:
        raise _refuse(f"{scope} is unavailable", path=display_path) from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _remove_owned_publication(
    directory_fd: int,
    name: str,
    identity: tuple[int, int],
    *,
    scope: str,
    display_path: str,
) -> None:
    """Remove only this writer's inode through the pinned parent descriptor."""

    try:
        observed = os.stat(
            name,
            dir_fd=directory_fd,
            follow_symlinks=False,
        )
        if (observed.st_dev, observed.st_ino) != identity:
            raise _refuse(
                f"new {scope} identity changed during confirmation rollback",
                path=display_path,
            )
        os.unlink(name, dir_fd=directory_fd)
        os.fsync(directory_fd)
    except ProductLifecycleError:
        raise
    except OSError as exc:
        raise _refuse(
            f"could not remove unconfirmed {scope}",
            path=display_path,
        ) from exc


def _ancestor_protects_operator_child(
    ancestor: os.stat_result,
    child: os.stat_result,
) -> bool:
    if not ancestor.st_mode & 0o022:
        return True
    return bool(
        ancestor.st_mode & stat.S_ISVTX
        and ancestor.st_uid in {0, os.geteuid()}
        and child.st_uid == os.geteuid()
    )


def _write_json_mapping(
    directory_fd: int,
    name: str,
    payload: Mapping[str, Any],
    *,
    scope: str,
    display_path: str,
) -> tuple[bool, tuple[int, int]]:
    """Publish without replacement through one pinned parent descriptor."""

    encoded = _canonical_json(dict(payload)) + b"\n"
    descriptor: int | None = None
    temporary_name = ""
    for _ in range(128):
        candidate = f".{name}.{secrets.token_hex(16)}.tmp"
        try:
            descriptor = os.open(
                candidate,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
                0o600,
                dir_fd=directory_fd,
            )
        except FileExistsError:
            continue
        except OSError as exc:
            raise _refuse(
                f"could not stage {scope}",
                path=display_path,
            ) from exc
        temporary_name = candidate
        break
    if descriptor is None:
        raise _refuse(f"could not allocate private staging for {scope}")

    linked_identity: tuple[int, int] | None = None
    temporary_exists = True

    def cleanup_staging(refusal: ProductLifecycleError | None = None) -> None:
        nonlocal temporary_exists
        if not temporary_exists:
            return
        try:
            os.unlink(temporary_name, dir_fd=directory_fd)
            temporary_exists = False
        except OSError as exc:
            cleanup = _refuse(
                f"could not remove staged {scope}",
                path=display_path,
            )
            if refusal is None:
                raise cleanup from exc
            refusal.data["staging_cleanup_error"] = cleanup.to_json()["error"]

    def cleanup_publication(refusal: ProductLifecycleError) -> None:
        if linked_identity is None:
            return
        try:
            _remove_owned_publication(
                directory_fd,
                name,
                linked_identity,
                scope=scope,
                display_path=display_path,
            )
        except ProductLifecycleError as cleanup_exc:
            refusal.data["publication_cleanup_error"] = cleanup_exc.to_json()["error"]

    try:
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = None
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
            staged = os.fstat(stream.fileno())
        staged_identity = (staged.st_dev, staged.st_ino)
        try:
            os.link(
                temporary_name,
                name,
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
                follow_symlinks=False,
            )
        except FileExistsError:
            if _read_json_mapping_at(
                directory_fd,
                name,
                scope=scope,
                display_path=display_path,
            ) != dict(payload):
                raise _refuse(
                    f"refusing to replace a different {scope}",
                    path=display_path,
                )
            existing = os.stat(
                name,
                dir_fd=directory_fd,
                follow_symlinks=False,
            )
            cleanup_staging()
            return False, (existing.st_dev, existing.st_ino)
        linked_identity = staged_identity
        published = os.stat(
            name,
            dir_fd=directory_fd,
            follow_symlinks=False,
        )
        if (
            (published.st_dev, published.st_ino) != staged_identity
            or published.st_size != staged.st_size
            or published.st_mode != staged.st_mode
            or published.st_nlink != staged.st_nlink + 1
        ):
            raise _refuse(
                f"new {scope} identity changed during atomic publication",
                path=display_path,
            )
        cleanup_staging()
        final = os.stat(
            name,
            dir_fd=directory_fd,
            follow_symlinks=False,
        )
        if (
            (final.st_dev, final.st_ino) != staged_identity
            or final.st_size != staged.st_size
            or final.st_mode != staged.st_mode
            or final.st_nlink != staged.st_nlink
        ):
            raise _refuse(
                f"new {scope} identity changed while finalizing publication",
                path=display_path,
            )
        os.fsync(directory_fd)
        return True, staged_identity
    except ProductLifecycleError as refusal:
        cleanup_publication(refusal)
        cleanup_staging(refusal)
        raise
    except OSError as exc:
        refusal = _refuse(f"could not save {scope}", path=display_path)
        cleanup_publication(refusal)
        cleanup_staging(refusal)
        raise refusal from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


@contextmanager
def _request_publication_lock(path: str | Path) -> Iterator[tuple[int, str]]:
    """Pin trusted ancestry and serialize writers in the private parent."""

    target = Path(path)
    absolute_parent = target.parent.absolute()
    directory_fd: int | None = None
    lock_descriptor: int | None = None
    current_path = Path("/")
    try:
        directory_fd = os.open(
            "/",
            os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY | os.O_NOFOLLOW,
        )
        parent_stat = os.fstat(directory_fd)
        for part in absolute_parent.parts[1:]:
            child_fd = os.open(
                part,
                os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY | os.O_NOFOLLOW,
                dir_fd=directory_fd,
            )
            try:
                child_stat = os.fstat(child_fd)
            except OSError:
                os.close(child_fd)
                raise
            if not _ancestor_protects_operator_child(parent_stat, child_stat):
                os.close(child_fd)
                os.close(directory_fd)
                directory_fd = None
                raise _refuse(
                    "product request ancestry permits replacement by another principal",
                    path=str(current_path),
                )
            os.close(directory_fd)
            directory_fd = child_fd
            parent_stat = child_stat
            current_path /= part
    except OSError as exc:
        if directory_fd is not None:
            os.close(directory_fd)
            directory_fd = None
        raise _refuse(
            "product request directory ancestry is unavailable or contains a symlink",
            path=str(absolute_parent),
        ) from exc
    if (
        not stat.S_ISDIR(parent_stat.st_mode)
        or parent_stat.st_uid != os.geteuid()
        or parent_stat.st_mode & 0o022
    ):
        os.close(directory_fd)
        directory_fd = None
        raise _refuse(
            "product request directory must be operator-owned and non-writable by other principals",
            path=str(absolute_parent),
        )
    lock_name = f".{target.name}.lock"
    try:
        lock_descriptor = os.open(
            lock_name,
            os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK,
            0o600,
            dir_fd=directory_fd,
        )
        lock_stat = os.fstat(lock_descriptor)
        if (
            not stat.S_ISREG(lock_stat.st_mode)
            or lock_stat.st_nlink != 1
            or lock_stat.st_uid != os.geteuid()
        ):
            raise _refuse(
                "product request publication lock is not a private regular file",
                path=str(absolute_parent / lock_name),
            )
        os.fchmod(lock_descriptor, 0o600)
        fcntl.flock(lock_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except ProductLifecycleError:
        if lock_descriptor is not None:
            os.close(lock_descriptor)
            lock_descriptor = None
        if directory_fd is not None:
            os.close(directory_fd)
            directory_fd = None
        raise
    except OSError as exc:
        if lock_descriptor is not None:
            os.close(lock_descriptor)
            lock_descriptor = None
        if directory_fd is not None:
            os.close(directory_fd)
            directory_fd = None
        raise _refuse(
            "product request publication lock is unavailable",
            path=str(absolute_parent / lock_name),
        ) from exc
    try:
        yield directory_fd, target.name
    finally:
        try:
            if lock_descriptor is not None:
                os.close(lock_descriptor)
        finally:
            if directory_fd is not None:
                os.close(directory_fd)


def _release_mapping(value: Mapping[str, Any]) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise _refuse("distribution_release must be a mapping")
    _strict_fields(
        value,
        {"owner", "distribution_id", "version", "release_digest"},
        scope="distribution_release",
    )
    return {
        "owner": _require_identifier(value["owner"], "distribution_release.owner"),
        "distribution_id": _require_identifier(
            value["distribution_id"], "distribution_release.distribution_id"
        ),
        "version": _require_identifier(
            value["version"], "distribution_release.version"
        ),
        "release_digest": _require_digest(
            value["release_digest"], "distribution_release.release_digest"
        ),
    }


def _bundle_mapping(value: Mapping[str, Any]) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise _refuse("bundle must be a mapping")
    _strict_fields(
        value,
        {
            "owner",
            "bundle_id",
            "version",
            "distribution_id",
            "distribution_version",
            "bundle_digest",
        },
        scope="bundle",
    )
    return {
        "owner": _require_identifier(value["owner"], "bundle.owner"),
        "bundle_id": _require_identifier(value["bundle_id"], "bundle.bundle_id"),
        "version": _require_identifier(value["version"], "bundle.version"),
        "distribution_id": _require_identifier(
            value["distribution_id"], "bundle.distribution_id"
        ),
        "distribution_version": _require_identifier(
            value["distribution_version"], "bundle.distribution_version"
        ),
        "bundle_digest": _require_digest(
            value["bundle_digest"], "bundle.bundle_digest"
        ),
    }


def _package_mapping(value: Mapping[str, Any]) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise _refuse("package identity must be a mapping")
    _strict_fields(
        value,
        {"owner", "id", "kind", "version", "digest"},
        scope="package identity",
    )
    return {
        "owner": _require_identifier(value["owner"], "package.owner"),
        "id": _require_identifier(value["id"], "package.id"),
        "kind": _require_identifier(value["kind"], "package.kind"),
        "version": _require_identifier(value["version"], "package.version"),
        "digest": _require_digest(value["digest"], "package.digest"),
    }


def _asset_mapping(value: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise _refuse("package asset must be a mapping")
    expected = {
        "owner",
        "id",
        "kind",
        "version",
        "asset_digest",
        "object_digest",
        "member_path",
        "member_crc32",
        "member_compressed_size",
        "compression_method",
        "uncompressed_size",
    }
    _strict_fields(value, expected, scope="package asset")
    for key in ("member_path", "member_crc32", "compression_method"):
        if value[key] is not None and not isinstance(value[key], str):
            raise _refuse(f"package asset {key} has the wrong type")
    member_path = value["member_path"]
    member_fields = (
        value["member_crc32"],
        value["member_compressed_size"],
        value["compression_method"],
    )
    if member_path is not None:
        _require_relative_path(member_path, "package asset.member_path")
        if any(item is None for item in member_fields):
            raise _refuse("member asset locators require complete metadata")
    elif any(item is not None for item in member_fields):
        raise _refuse("whole-object asset locators cannot carry member metadata")
    compressed_size = value["member_compressed_size"]
    if not isinstance(compressed_size, (int, type(None))) or isinstance(
        compressed_size, bool
    ):
        raise _refuse("package asset.member_compressed_size has the wrong type")
    uncompressed_size = value["uncompressed_size"]
    if (
        not isinstance(uncompressed_size, int)
        or isinstance(uncompressed_size, bool)
        or uncompressed_size < 0
    ):
        raise _refuse("package asset.uncompressed_size must be non-negative")
    return {
        "owner": _require_identifier(value["owner"], "asset.owner"),
        "id": _require_identifier(value["id"], "asset.id"),
        "kind": _require_identifier(value["kind"], "asset.kind"),
        "version": _require_identifier(value["version"], "asset.version"),
        "asset_digest": _require_digest(value["asset_digest"], "asset.asset_digest"),
        "object_digest": _require_digest(value["object_digest"], "asset.object_digest"),
        "member_path": member_path,
        "member_crc32": value["member_crc32"],
        "member_compressed_size": compressed_size,
        "compression_method": value["compression_method"],
        "uncompressed_size": uncompressed_size,
    }


def _object_mapping(value: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise _refuse("package object must be a mapping")
    _strict_fields(value, {"digest", "size", "media_type"}, scope="package object")
    size = value["size"]
    if not isinstance(size, int) or isinstance(size, bool) or size < 0:
        raise _refuse("package object.size must be non-negative")
    media_type = value["media_type"]
    if (
        not isinstance(media_type, str)
        or not media_type
        or any(char.isspace() for char in media_type)
    ):
        raise _refuse("package object.media_type is invalid")
    return {
        "digest": _require_digest(value["digest"], "object.digest"),
        "size": size,
        "media_type": media_type,
    }


def _package_key(item: Mapping[str, Any]) -> tuple[str, ...]:
    return tuple(str(item[key]) for key in ("owner", "id", "kind", "version", "digest"))


def _asset_key(item: Mapping[str, Any]) -> tuple[str, ...]:
    return tuple(
        str(item[key] or "")
        for key in (
            "owner",
            "id",
            "kind",
            "version",
            "asset_digest",
            "object_digest",
            "member_path",
        )
    )


def _object_key(item: Mapping[str, Any]) -> tuple[str, ...]:
    return (str(item["digest"]), str(item["size"]), str(item["media_type"]))


@dataclass(frozen=True, slots=True)
class PackageSelection:
    """Canonical closed identity set copied from one verified package."""

    transport_digest: str
    distribution_release: Mapping[str, str]
    bundle: Mapping[str, str]
    package_identities: tuple[Mapping[str, str], ...]
    assets: tuple[Mapping[str, Any], ...]
    objects: tuple[Mapping[str, Any], ...]

    def __post_init__(self) -> None:
        _require_digest(self.transport_digest, "package_selection.transport_digest")
        object.__setattr__(
            self,
            "distribution_release",
            MappingProxyType(_release_mapping(self.distribution_release)),
        )
        object.__setattr__(
            self, "bundle", MappingProxyType(_bundle_mapping(self.bundle))
        )
        packages = tuple(
            MappingProxyType(_package_mapping(item)) for item in self.package_identities
        )
        assets = tuple(MappingProxyType(_asset_mapping(item)) for item in self.assets)
        objects = tuple(
            MappingProxyType(_object_mapping(item)) for item in self.objects
        )
        if len({tuple(item.items()) for item in packages}) != len(packages):
            raise _refuse("package identities must be unique")
        asset_keys = [
            (item["owner"], item["id"], item["kind"], item["version"])
            for item in assets
        ]
        if len(set(asset_keys)) != len(asset_keys):
            raise _refuse("package assets must be unique")
        object_keys = [
            (item["digest"], item["size"], item["media_type"]) for item in objects
        ]
        if len(set(object_keys)) != len(object_keys):
            raise _refuse("package objects must be unique")
        object_digests = {item["digest"] for item in objects}
        if any(item["object_digest"] not in object_digests for item in assets):
            raise _refuse("package asset locator references an unknown object")
        object.__setattr__(
            self,
            "package_identities",
            tuple(sorted(packages, key=_package_key)),
        )
        object.__setattr__(self, "assets", tuple(sorted(assets, key=_asset_key)))
        object.__setattr__(self, "objects", tuple(sorted(objects, key=_object_key)))

    def to_mapping(self) -> dict[str, Any]:
        return {
            "schema": PACKAGE_SELECTION_SCHEMA,
            "transport_digest": self.transport_digest,
            "distribution_release": dict(self.distribution_release),
            "bundle": dict(self.bundle),
            "package_identities": [dict(item) for item in self.package_identities],
            "assets": [dict(item) for item in self.assets],
            "objects": [dict(item) for item in self.objects],
        }

    @property
    def digest(self) -> str:
        return package_selection_digest(self)


def package_selection_from_mapping(raw: Mapping[str, Any]) -> PackageSelection:
    if not isinstance(raw, Mapping):
        raise _refuse("package selection must be a mapping")
    _strict_fields(
        raw,
        {
            "schema",
            "transport_digest",
            "distribution_release",
            "bundle",
            "package_identities",
            "assets",
            "objects",
        },
        scope="package selection",
    )
    if raw["schema"] != PACKAGE_SELECTION_SCHEMA:
        raise _refuse(
            "package selection schema is unsupported",
            observed=raw.get("schema"),
        )
    for field in ("package_identities", "assets", "objects"):
        if not isinstance(raw[field], Sequence) or isinstance(raw[field], (str, bytes)):
            raise _refuse(f"package selection.{field} must be a sequence")
    return PackageSelection(
        transport_digest=raw["transport_digest"],
        distribution_release=raw["distribution_release"],
        bundle=raw["bundle"],
        package_identities=tuple(raw["package_identities"]),
        assets=tuple(raw["assets"]),
        objects=tuple(raw["objects"]),
    )


def package_selection_digest(selection: PackageSelection | Mapping[str, Any]) -> str:
    canonical = (
        selection
        if isinstance(selection, PackageSelection)
        else package_selection_from_mapping(selection)
    )
    return _digest(canonical.to_mapping())


@dataclass(frozen=True, slots=True)
class PackageArtifactBinding:
    """One selected package asset mapped to one plan artifact."""

    logical_id: str
    asset_ref: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "logical_id",
            _require_relative_path(self.logical_id, "package binding.logical_id"),
        )
        object.__setattr__(
            self,
            "asset_ref",
            _require_package_asset_ref(self.asset_ref, "package binding.asset_ref"),
        )

    def to_mapping(self) -> dict[str, str]:
        return {"logical_id": self.logical_id, "asset_ref": self.asset_ref}


def package_artifact_bindings_from_value(
    raw: Sequence[PackageArtifactBinding | Mapping[str, Any]],
) -> tuple[PackageArtifactBinding, ...]:
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        raise _refuse("package_artifact_bindings must be a sequence")
    bindings: list[PackageArtifactBinding] = []
    for item in raw:
        if isinstance(item, PackageArtifactBinding):
            binding = item
        else:
            if not isinstance(item, Mapping):
                raise _refuse("package artifact binding must be a mapping")
            _strict_fields(
                item,
                {"logical_id", "asset_ref"},
                scope="package artifact binding",
            )
            binding = PackageArtifactBinding(
                logical_id=item["logical_id"],
                asset_ref=item["asset_ref"],
            )
        bindings.append(binding)
    if not bindings:
        raise _refuse("package_artifact_bindings must be non-empty")
    logical_ids = [item.logical_id for item in bindings]
    asset_refs = [item.asset_ref for item in bindings]
    if len(logical_ids) != len(set(logical_ids)):
        raise _refuse("package binding logical IDs must be unique")
    if len(asset_refs) != len(set(asset_refs)):
        raise _refuse("package binding asset references must be unique")
    return tuple(sorted(bindings, key=lambda item: (item.logical_id, item.asset_ref)))


def require_package_artifact_bindings_match_selection(
    raw: Sequence[PackageArtifactBinding | Mapping[str, Any]],
    selection: PackageSelection,
) -> tuple[PackageArtifactBinding, ...]:
    bindings = package_artifact_bindings_from_value(raw)
    expected = {f"{item['owner']}:{item['id']}" for item in selection.assets}
    observed = {item.asset_ref for item in bindings}
    if observed != expected:
        raise _refuse(
            "package artifact bindings differ from selected assets",
            missing_asset_refs=sorted(expected - observed),
            unexpected_asset_refs=sorted(observed - expected),
        )
    return bindings


def _transport_digest_from_path(path: Path) -> str:
    if not re.fullmatch(r"[0-9a-f]{64}\.okgdist\.tar", path.name):
        raise _refuse(
            "distribution package filename is not transport-bound",
            path=str(path),
        )
    return "sha256:" + path.name[:64]


def package_selection_from_verified(
    package: VerifiedDistributionPackage,
    *,
    bundle_id: str | None = None,
) -> PackageSelection:
    if not isinstance(package, VerifiedDistributionPackage):
        raise _refuse("package selection requires a verified distribution package")
    manifest = package.manifest
    bundles = tuple(manifest.bundles)
    if bundle_id is not None:
        bundles = tuple(item for item in bundles if item.bundle_id == bundle_id)
    if len(bundles) != 1:
        raise _refuse(
            "package selection must identify exactly one bundle",
            bundle_id=bundle_id,
        )
    bundle = bundles[0]
    release = manifest.distribution_release
    asset_by_ref = {item.asset.qualified_id: item for item in manifest.assets}
    selected_refs = {
        item.qualified_id for item in (*release.asset_refs, *bundle.asset_refs)
    }
    missing_refs = sorted(selected_refs - set(asset_by_ref))
    if missing_refs:
        raise _refuse(
            "selected package assets do not resolve",
            missing_asset_refs=missing_refs,
        )
    selected_assets = tuple(asset_by_ref[key] for key in sorted(selected_refs))
    selected_object_digests = {item.locator.object_digest for item in selected_assets}
    object_by_digest = {item.digest: item for item in manifest.objects}
    if selected_object_digests - set(object_by_digest):
        raise _refuse("selected package assets reference missing objects")
    return PackageSelection(
        transport_digest=_transport_digest_from_path(Path(package.path)),
        distribution_release={
            "owner": release.owner,
            "distribution_id": release.distribution_id,
            "version": release.version,
            "release_digest": release.release_digest,
        },
        bundle={
            "owner": bundle.owner,
            "bundle_id": bundle.bundle_id,
            "version": bundle.version,
            "distribution_id": bundle.distribution_id,
            "distribution_version": bundle.distribution_version,
            "bundle_digest": bundle.bundle_digest,
        },
        package_identities=tuple(
            {
                "owner": item.owner,
                "id": item.ref,
                "kind": item.kind,
                "version": item.version,
                "digest": item.digest,
            }
            for item in manifest.package_identities
        ),
        assets=tuple(
            {
                "owner": item.asset.owner,
                "id": item.asset.id,
                "kind": item.asset.kind,
                "version": item.asset.version,
                "asset_digest": item.asset.digest,
                "object_digest": item.locator.object_digest,
                "member_path": item.locator.member_path,
                "member_crc32": item.locator.member_crc32,
                "member_compressed_size": item.locator.member_compressed_size,
                "compression_method": item.locator.compression_method,
                "uncompressed_size": item.uncompressed_size,
            }
            for item in selected_assets
        ),
        objects=tuple(
            {
                "digest": item.digest,
                "size": item.size,
                "media_type": item.media_type,
            }
            for digest, item in sorted(object_by_digest.items())
            if digest in selected_object_digests
        ),
    )


def _package_reader_refusal(exc: DistributionPackageError) -> ProductLifecycleError:
    data: dict[str, Any] = {
        "package_error_code": exc.code,
        "logical_path": exc.logical_path,
    }
    for field in ("limit", "observed", "maximum"):
        value = getattr(exc, field, None)
        if value is not None:
            data[field] = value
    return _refuse("distribution package verification failed", **data)


def _open_verified_package_selection(
    path: str | Path,
    expected: PackageSelection | Mapping[str, Any],
) -> tuple[VerifiedDistributionPackage, PackageSelection]:
    expected_selection = (
        expected
        if isinstance(expected, PackageSelection)
        else package_selection_from_mapping(expected)
    )
    verified: VerifiedDistributionPackage | None = None
    try:
        verified = verify_distribution_package(Path(path))
        observed = package_selection_from_verified(
            verified,
            bundle_id=expected_selection.bundle["bundle_id"],
        )
    except DistributionPackageError as exc:
        if verified is not None:
            verified.close_runtime_authority()
        raise _package_reader_refusal(exc) from exc
    except Exception as exc:
        if verified is not None:
            verified.close_runtime_authority()
        if isinstance(exc, (ProductLifecycleError, DeploymentEnvironmentInvalid)):
            raise
        raise _refuse(
            "distribution package verification failed",
            path=str(path),
        ) from exc
    if observed.to_mapping() != expected_selection.to_mapping():
        verified.close_runtime_authority()
        raise _refuse(
            "distribution package identity changed",
            expected_digest=expected_selection.digest,
            observed_digest=observed.digest,
        )
    return verified, observed


def verify_package_selection(
    path: str | Path,
    expected: PackageSelection | Mapping[str, Any],
) -> PackageSelection:
    verified, observed = _open_verified_package_selection(path, expected)
    verified.close_runtime_authority()
    return observed


@dataclass(frozen=True, slots=True)
class ProductApplyRequest:
    """Immutable request whose identity excludes locator paths."""

    staging_environment_id: str
    target_environment_id: str
    idempotency_key: str
    authored_definition_path: str | Path
    product_plan_path: str | Path
    expected_product_plan_digest: str
    distribution_package_path: str | Path
    package_selection: PackageSelection | Mapping[str, Any]
    package_selection_digest: str
    package_artifact_bindings: Sequence[PackageArtifactBinding | Mapping[str, Any]]
    artifact_root: str | Path
    sealed_release: SealedReleaseReference | Mapping[str, Any]
    promotion_approval_path: str | Path
    promotion_approval_digest: str
    requested_transition: str
    logical_artifact_digests: tuple[tuple[str, str], ...] | Mapping[str, str]

    def __post_init__(self) -> None:
        _require_identifier(self.staging_environment_id, "staging_environment_id")
        _require_identifier(self.target_environment_id, "target_environment_id")
        _require_promotion_id(self.idempotency_key)
        for field in (
            "authored_definition_path",
            "product_plan_path",
            "distribution_package_path",
            "artifact_root",
            "promotion_approval_path",
        ):
            _locator(getattr(self, field), field)
        _require_digest(
            self.expected_product_plan_digest,
            "expected_product_plan_digest",
        )
        _require_digest(self.package_selection_digest, "package_selection_digest")
        _require_digest(self.promotion_approval_digest, "promotion_approval_digest")
        if self.requested_transition not in {"first_install", "upgrade"}:
            raise _refuse("requested_transition must be first_install or upgrade")
        selection = (
            self.package_selection
            if isinstance(self.package_selection, PackageSelection)
            else package_selection_from_mapping(self.package_selection)
        )
        if selection.digest != self.package_selection_digest:
            raise _refuse("package selection digest does not match its closed value")
        object.__setattr__(self, "package_selection", selection)
        bindings = require_package_artifact_bindings_match_selection(
            self.package_artifact_bindings,
            selection,
        )
        object.__setattr__(self, "package_artifact_bindings", bindings)
        try:
            sealed = (
                self.sealed_release
                if isinstance(self.sealed_release, SealedReleaseReference)
                else SealedReleaseReference.model_validate(self.sealed_release)
            )
        except (TypeError, ValueError) as exc:
            raise _refuse("sealed release reference is invalid") from exc
        if (
            sealed.input_id is None
            or sealed.definition_digest is None
            or sealed.artifact_root is None
        ):
            raise _refuse("sealed release reference is incomplete")
        object.__setattr__(self, "sealed_release", sealed)
        raw_artifacts = (
            tuple(self.logical_artifact_digests.items())
            if isinstance(self.logical_artifact_digests, Mapping)
            else tuple(self.logical_artifact_digests)
        )
        seen: set[str] = set()
        artifacts: list[tuple[str, str]] = []
        for logical_id, digest in raw_artifacts:
            logical = _require_relative_path(
                logical_id,
                "logical_artifact_digests.logical_id",
            )
            if logical in seen:
                raise _refuse("logical artifact IDs must be unique", logical_id=logical)
            seen.add(logical)
            artifacts.append(
                (logical, _require_digest(digest, f"logical_artifacts.{logical}"))
            )
        binding_ids = {item.logical_id for item in bindings}
        artifact_ids = {item[0] for item in artifacts}
        if not binding_ids <= artifact_ids:
            raise _refuse(
                "package bindings reference unpinned logical artifacts",
                missing_logical_ids=sorted(binding_ids - artifact_ids),
            )
        object.__setattr__(self, "logical_artifact_digests", tuple(sorted(artifacts)))

    @property
    def promotion_id(self) -> str:
        return self.idempotency_key

    def request_preimage(self) -> dict[str, Any]:
        sealed = self.sealed_release
        assert isinstance(sealed, SealedReleaseReference)
        return {
            "schema": PRODUCT_APPLY_REQUEST_SCHEMA,
            "staging_environment_id": self.staging_environment_id,
            "target_environment_id": self.target_environment_id,
            "idempotency_key": self.idempotency_key,
            "expected_product_plan_digest": self.expected_product_plan_digest,
            "logical_artifact_digests": [
                {"logical_id": logical_id, "digest": digest}
                for logical_id, digest in self.logical_artifact_digests
            ],
            "package_selection_digest": self.package_selection_digest,
            "package_artifact_bindings": [
                item.to_mapping() for item in self.package_artifact_bindings
            ],
            "sealed_release_identity": {
                "schema": sealed.schema_version,
                "authority": sealed.authority,
                "artifact_id": sealed.artifact_id,
                "input_id": sealed.input_id,
                "definition_digest": sealed.definition_digest,
            },
            "requested_transition": self.requested_transition,
        }

    @property
    def request_digest(self) -> str:
        return _digest(self.request_preimage())

    @property
    def digest(self) -> str:
        return self.request_digest

    def to_mapping(self) -> dict[str, Any]:
        sealed = self.sealed_release
        selection = self.package_selection
        assert isinstance(sealed, SealedReleaseReference)
        assert isinstance(selection, PackageSelection)
        return {
            "schema": PRODUCT_APPLY_REQUEST_SCHEMA,
            "staging_environment_id": self.staging_environment_id,
            "target_environment_id": self.target_environment_id,
            "idempotency_key": self.idempotency_key,
            "authored_definition_path": _locator(
                self.authored_definition_path,
                "authored_definition_path",
            ),
            "product_plan_path": _locator(self.product_plan_path, "product_plan_path"),
            "expected_product_plan_digest": self.expected_product_plan_digest,
            "distribution_package_path": _locator(
                self.distribution_package_path,
                "distribution_package_path",
            ),
            "package_selection": selection.to_mapping(),
            "package_selection_digest": self.package_selection_digest,
            "package_artifact_bindings": [
                item.to_mapping() for item in self.package_artifact_bindings
            ],
            "artifact_root": _locator(self.artifact_root, "artifact_root"),
            "logical_artifact_digests": [
                {"logical_id": logical_id, "digest": digest}
                for logical_id, digest in self.logical_artifact_digests
            ],
            "sealed_release": sealed.model_dump(mode="json", by_alias=True),
            "promotion_approval_path": _locator(
                self.promotion_approval_path,
                "promotion_approval_path",
            ),
            "promotion_approval_digest": self.promotion_approval_digest,
            "requested_transition": self.requested_transition,
        }

    to_dict = to_mapping
    payload = to_mapping

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> ProductApplyRequest:
        if not isinstance(raw, Mapping):
            raise _refuse("product apply request must be a mapping")
        expected = {
            "schema",
            "staging_environment_id",
            "target_environment_id",
            "idempotency_key",
            "authored_definition_path",
            "product_plan_path",
            "expected_product_plan_digest",
            "distribution_package_path",
            "package_selection",
            "package_selection_digest",
            "package_artifact_bindings",
            "artifact_root",
            "logical_artifact_digests",
            "sealed_release",
            "promotion_approval_path",
            "promotion_approval_digest",
            "requested_transition",
        }
        _strict_fields(raw, expected, scope="product apply request")
        if raw["schema"] != PRODUCT_APPLY_REQUEST_SCHEMA:
            raise _refuse(
                "product apply request schema is unsupported",
                observed=raw.get("schema"),
            )
        entries = raw["logical_artifact_digests"]
        if not isinstance(entries, Sequence) or isinstance(entries, (str, bytes)):
            raise _refuse("logical_artifact_digests must be a sequence")
        logical: list[tuple[str, str]] = []
        for entry in entries:
            if not isinstance(entry, Mapping):
                raise _refuse("logical artifact digest entry must be a mapping")
            _strict_fields(
                entry,
                {"logical_id", "digest"},
                scope="logical artifact digest",
            )
            logical.append((entry["logical_id"], entry["digest"]))
        try:
            return cls(
                staging_environment_id=raw["staging_environment_id"],
                target_environment_id=raw["target_environment_id"],
                idempotency_key=raw["idempotency_key"],
                authored_definition_path=raw["authored_definition_path"],
                product_plan_path=raw["product_plan_path"],
                expected_product_plan_digest=raw["expected_product_plan_digest"],
                distribution_package_path=raw["distribution_package_path"],
                package_selection=raw["package_selection"],
                package_selection_digest=raw["package_selection_digest"],
                package_artifact_bindings=raw["package_artifact_bindings"],
                artifact_root=raw["artifact_root"],
                sealed_release=raw["sealed_release"],
                promotion_approval_path=raw["promotion_approval_path"],
                promotion_approval_digest=raw["promotion_approval_digest"],
                requested_transition=raw["requested_transition"],
                logical_artifact_digests=tuple(logical),
            )
        except (TypeError, ValueError) as exc:
            if isinstance(exc, ProductLifecycleError):
                raise
            raise _refuse("product apply request is invalid") from exc

    from_dict = from_mapping


def canonical_request_preimage(request: ProductApplyRequest) -> dict[str, Any]:
    if not isinstance(request, ProductApplyRequest):
        raise _refuse("request preimage requires ProductApplyRequest")
    return request.request_preimage()


def request_digest(request: ProductApplyRequest) -> str:
    return _digest(canonical_request_preimage(request))


_PLAN_TUPLE_KEYS = frozenset(
    {
        "bundle_refs",
        "asset_refs",
        "package_refs",
        "environment_refs",
        "connection_contracts",
        "compatibility_decisions",
        "override_provenance",
        "provenance",
        "bundles",
        "assets",
        "objects",
        "package_identities",
        "actions",
        "references",
        "capabilities",
        "diagnostics",
    }
)


def _normalise_plan_sequences(value: Any, *, key: str | None = None) -> Any:
    if isinstance(value, Mapping):
        return {
            name: _normalise_plan_sequences(child, key=str(name))
            for name, child in value.items()
        }
    if isinstance(value, list):
        normalised = [_normalise_plan_sequences(child) for child in value]
        return tuple(normalised) if key in _PLAN_TUPLE_KEYS else normalised
    return value


def _parse_timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise _refuse(f"{field} is required", field=field)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise _refuse(f"{field} is invalid", field=field) from exc
    return _utc(parsed)


def _plan_from_mapping(raw: Mapping[str, Any]) -> DeploymentProductPlan:
    _strict_fields(
        raw,
        {
            "schema_version",
            "api_version",
            "lock",
            "lock_digest",
            "definition_digest",
            "artifact_digests",
            "expires_at",
            "actions",
            "plan_digest",
        },
        scope="deployment product plan",
    )
    if raw["schema_version"] != PLAN_SCHEMA or raw["api_version"] != API_VERSION:
        raise _refuse("deployment product plan schema or API version is unsupported")
    try:
        artifact_raw = raw["artifact_digests"]
        if not isinstance(artifact_raw, Sequence) or isinstance(
            artifact_raw, (str, bytes)
        ):
            raise _refuse("plan artifact_digests must be a sequence")
        artifacts: list[tuple[str, str]] = []
        for item in artifact_raw:
            if not isinstance(item, Mapping):
                raise _refuse("plan artifact digest must be a mapping")
            _strict_fields(
                item,
                {"logical_id", "digest"},
                scope="plan artifact digest",
            )
            artifacts.append(
                (
                    _require_relative_path(item["logical_id"], "plan logical ID"),
                    _require_digest(item["digest"], "plan artifact digest"),
                )
            )
        if len({item[0] for item in artifacts}) != len(artifacts):
            raise _refuse("plan logical artifact IDs must be unique")
        actions_raw = raw["actions"]
        if not isinstance(actions_raw, Sequence) or isinstance(
            actions_raw, (str, bytes)
        ):
            raise _refuse("plan actions must be a sequence")
        actions: list[PlanAction] = []
        for item in actions_raw:
            if not isinstance(item, Mapping):
                raise _refuse("plan action must be a mapping")
            _strict_fields(
                item,
                {"action_id", "kind", "arguments"},
                scope="plan action",
            )
            actions.append(
                PlanAction(
                    action_id=item["action_id"],
                    kind=item["kind"],
                    arguments=item["arguments"],
                )
            )
        lock = canonicalize_lock(
            ResolvedLock.model_validate(_normalise_plan_sequences(raw["lock"]))
        )
        lock_digest = _require_digest(raw["lock_digest"], "plan.lock_digest")
        if sha256_digest(canonical_lock_bytes(lock)) != lock_digest:
            raise _refuse("deployment product lock digest does not match its payload")
        plan = DeploymentProductPlan(
            lock=lock,
            lock_digest=lock_digest,
            definition_digest=_require_digest(
                raw["definition_digest"],
                "plan.definition_digest",
            ),
            artifact_digests=tuple(sorted(artifacts)),
            expires_at=_parse_timestamp(raw["expires_at"], "plan.expires_at"),
            actions=tuple(
                sorted(actions, key=lambda item: (item.action_id, item.kind))
            ),
            plan_digest=_require_digest(raw["plan_digest"], "plan.plan_digest"),
            schema_version=raw["schema_version"],
            api_version=raw["api_version"],
        )
    except (TypeError, ValueError, KeyError) as exc:
        if isinstance(exc, ProductLifecycleError):
            raise
        raise _refuse("deployment product plan is invalid") from exc
    observed = sha256_digest(
        canonical_json_bytes(plan.payload(include_plan_digest=False))
    )
    if observed != plan.plan_digest:
        raise _refuse(
            "deployment product plan digest does not match its payload",
            expected=plan.plan_digest,
            observed=observed,
        )
    return plan


def load_product_plan(path: str | Path) -> DeploymentProductPlan:
    return _plan_from_mapping(_read_json_mapping(path, scope="deployment product plan"))


def save_product_plan(path: str | Path, plan: DeploymentProductPlan) -> None:
    if not isinstance(plan, DeploymentProductPlan):
        raise _refuse("save requires DeploymentProductPlan")
    canonical = _plan_from_mapping(plan.payload())
    with _request_publication_lock(path) as (directory_fd, name):
        _write_json_mapping(
            directory_fd,
            name,
            canonical.payload(),
            scope="deployment product plan",
            display_path=str(path),
        )


class ContainedArtifactReader:
    """Read pinned artifacts through descriptor-relative no-symlink traversal."""

    def __init__(self, root: str | Path):
        path = Path(root)
        try:
            root_stat = path.lstat()
            if stat.S_ISLNK(root_stat.st_mode) or not stat.S_ISDIR(root_stat.st_mode):
                raise _refuse(
                    "artifact root must be a real directory",
                    path=str(path),
                )
            self.root = path.resolve(strict=True)
            resolved_stat = self.root.stat()
        except ProductLifecycleError:
            raise
        except OSError as exc:
            raise _refuse("artifact root is unavailable", path=str(path)) from exc
        self._root_identity = (resolved_stat.st_dev, resolved_stat.st_ino)

    def read_bytes(self, logical_id: str) -> bytes:
        return self.read_exact_bytes(logical_id)

    def read_exact_bytes(
        self,
        logical_id: str,
        *,
        expected_size: int | None = None,
    ) -> bytes:
        logical = _require_relative_path(logical_id, "logical artifact ID")
        if expected_size is not None and (
            not isinstance(expected_size, int)
            or isinstance(expected_size, bool)
            or expected_size < 0
        ):
            raise _refuse("expected artifact size must be non-negative")
        maximum_size = (
            expected_size
            if expected_size is not None
            else _MAX_CONTAINED_ARTIFACT_BYTES
        )
        directory_fd: int | None = None
        file_fd: int | None = None
        try:
            directory_fd = os.open(
                self.root,
                os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY | os.O_NOFOLLOW,
            )
            root_stat = os.fstat(directory_fd)
            if (root_stat.st_dev, root_stat.st_ino) != self._root_identity:
                raise OSError("artifact root identity changed")
            parts = logical.split("/")
            for part in parts[:-1]:
                child_fd = os.open(
                    part,
                    os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY | os.O_NOFOLLOW,
                    dir_fd=directory_fd,
                )
                os.close(directory_fd)
                directory_fd = child_fd
            file_fd = os.open(
                parts[-1],
                os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK,
                dir_fd=directory_fd,
            )
            before = os.fstat(file_fd)
            if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
                raise OSError("artifact is not a regular file")
            if expected_size is not None and before.st_size != expected_size:
                raise _refuse(
                    "contained artifact size differs from the selected package",
                    logical_id=logical,
                    expected=expected_size,
                    observed=before.st_size,
                )
            if before.st_size > maximum_size:
                raise _refuse(
                    "contained artifact exceeds the read bound",
                    logical_id=logical,
                    observed=before.st_size,
                    maximum=maximum_size,
                )
            chunks: list[bytes] = []
            observed_size = 0
            while chunk := os.read(file_fd, 1024 * 1024):
                observed_size += len(chunk)
                if observed_size > maximum_size:
                    raise _refuse(
                        "contained artifact grew while being read",
                        logical_id=logical,
                        maximum=maximum_size,
                        observed=observed_size,
                    )
                chunks.append(chunk)
            after = os.fstat(file_fd)
            if (
                before.st_dev,
                before.st_ino,
                before.st_size,
                before.st_mtime_ns,
                before.st_ctime_ns,
                before.st_mode,
                before.st_nlink,
            ) != (
                after.st_dev,
                after.st_ino,
                after.st_size,
                after.st_mtime_ns,
                after.st_ctime_ns,
                after.st_mode,
                after.st_nlink,
            ) or (expected_size is not None and observed_size != expected_size):
                raise _refuse(
                    "contained artifact changed while being read",
                    logical_id=logical,
                )
            return b"".join(chunks)
        except ProductLifecycleError:
            raise
        except OSError as exc:
            raise _refuse(
                "artifact locator escapes, changed, or could not be read",
                logical_id=logical,
            ) from exc
        finally:
            if file_fd is not None:
                os.close(file_fd)
            if directory_fd is not None:
                os.close(directory_fd)


def require_package_selection_matches_lock(
    selection: PackageSelection | Mapping[str, Any],
    lock: ResolvedLock,
) -> None:
    """Require the verified package identities to equal the resolved lock."""

    canonical = (
        selection
        if isinstance(selection, PackageSelection)
        else package_selection_from_mapping(selection)
    )
    if not isinstance(lock, ResolvedLock):
        raise _refuse("package selection requires a resolved lock")
    release = lock.distribution_release_ref
    observed_release = dict(canonical.distribution_release)
    expected_release = {
        "owner": release.owner,
        "distribution_id": release.distribution_id,
        "version": release.version,
        "release_digest": release.digest,
    }
    selected_bundle = canonical.bundle
    expected_bundles = {
        (item.owner, item.bundle_id, item.version, item.digest)
        for item in lock.bundle_refs
    }
    observed_bundle = (
        selected_bundle["owner"],
        selected_bundle["bundle_id"],
        selected_bundle["version"],
        selected_bundle["bundle_digest"],
    )
    expected_packages = {
        (item.owner, item.ref, item.kind, item.version, item.digest)
        for item in lock.package_identities
    }
    observed_packages = {
        (item["owner"], item["id"], item["kind"], item["version"], item["digest"])
        for item in canonical.package_identities
    }
    expected_assets = {
        (item.owner, item.ref, item.kind, item.version, item.digest)
        for item in lock.asset_refs
    }
    observed_assets = {
        (
            item["owner"],
            item["id"],
            item["kind"],
            item["version"],
            item["asset_digest"],
        )
        for item in canonical.assets
    }
    mismatches: dict[str, Any] = {}
    if observed_release != expected_release:
        mismatches["distribution_release"] = {
            "expected": expected_release,
            "observed": observed_release,
        }
    if (
        len(lock.bundle_refs) != 1
        or selected_bundle["distribution_id"] != release.distribution_id
        or selected_bundle["distribution_version"] != release.version
        or expected_bundles != {observed_bundle}
    ):
        mismatches["bundle"] = {
            "expected_exactly": sorted(expected_bundles),
            "observed": observed_bundle,
        }
    if observed_packages != expected_packages:
        mismatches["package_identities"] = {
            "expected": sorted(expected_packages),
            "observed": sorted(observed_packages),
        }
    if observed_assets != expected_assets:
        mismatches["assets"] = {
            "expected": sorted(expected_assets),
            "observed": sorted(observed_assets),
        }
    if mismatches:
        raise _refuse(
            "verified package selection differs from the resolved lock",
            mismatches=mismatches,
        )


def verify_product_request_package_input(
    request: ProductApplyRequest,
    *,
    plan: DeploymentProductPlan | None = None,
) -> PackageSelection:
    """Prove package, lock, plan, partition, and contained bytes before effects."""

    if not isinstance(request, ProductApplyRequest):
        raise _refuse("package input verification requires ProductApplyRequest")
    if plan is None:
        current_plan = load_product_plan(request.product_plan_path)
    elif isinstance(plan, DeploymentProductPlan):
        current_plan = _plan_from_mapping(plan.payload())
    else:
        raise _refuse("package input verification requires DeploymentProductPlan")
    if current_plan.plan_digest != request.expected_product_plan_digest:
        raise _refuse("product plan digest does not match request")
    if tuple(current_plan.artifact_digests) != tuple(request.logical_artifact_digests):
        raise _refuse(
            "logical artifact digests do not match the product plan",
            expected=list(current_plan.artifact_digests),
            observed=list(request.logical_artifact_digests),
        )
    require_package_selection_matches_lock(request.package_selection, current_plan.lock)
    verified, observed = _open_verified_package_selection(
        request.distribution_package_path,
        request.package_selection,
    )
    try:
        artifact_digests = dict(request.logical_artifact_digests)
        package_ids = {item.logical_id for item in request.package_artifact_bindings}
        local_digests = dict(current_plan.lock.local_asset_digests)
        local_ids = set(local_digests)
        if package_ids & local_ids:
            raise _refuse(
                "package and local artifact partitions overlap",
                logical_ids=sorted(package_ids & local_ids),
            )
        expected_ids = package_ids | local_ids
        if set(artifact_digests) != expected_ids:
            raise _refuse(
                "plan artifacts are not a complete package/local partition",
                missing_logical_ids=sorted(expected_ids - set(artifact_digests)),
                unclassified_logical_ids=sorted(set(artifact_digests) - expected_ids),
            )
        for logical_id, expected_digest in local_digests.items():
            if artifact_digests.get(logical_id) != expected_digest:
                raise _refuse(
                    "local artifact digest differs from the resolved lock",
                    logical_id=logical_id,
                    expected=expected_digest,
                    observed=artifact_digests.get(logical_id),
                )
        contained = ContainedArtifactReader(request.artifact_root)
        selected_assets = {
            f"{item['owner']}:{item['id']}": item
            for item in request.package_selection.assets
        }
        for binding in request.package_artifact_bindings:
            expected_digest = artifact_digests[binding.logical_id]
            selected_asset = selected_assets[binding.asset_ref]
            expected_size = selected_asset["uncompressed_size"]
            try:
                package_bytes = verified.read_asset(binding.asset_ref)
            except DistributionPackageError as exc:
                raise _package_reader_refusal(exc) from exc
            package_digest = sha256_digest(package_bytes)
            if len(package_bytes) != expected_size:
                raise _refuse(
                    "package asset size differs from the selected inventory",
                    asset_ref=binding.asset_ref,
                    expected=expected_size,
                    observed=len(package_bytes),
                )
            if package_digest != expected_digest:
                raise _refuse(
                    "package asset bytes differ from the product plan",
                    asset_ref=binding.asset_ref,
                    logical_id=binding.logical_id,
                    expected=expected_digest,
                    observed=package_digest,
                )
            contained_digest = sha256_digest(
                contained.read_exact_bytes(
                    binding.logical_id,
                    expected_size=expected_size,
                )
            )
            if contained_digest != expected_digest:
                raise _refuse(
                    "contained artifact bytes differ from the verified package",
                    asset_ref=binding.asset_ref,
                    logical_id=binding.logical_id,
                    expected=expected_digest,
                    observed=contained_digest,
                )
        return observed
    finally:
        verified.close_runtime_authority()


def load_product_apply_request(path: str | Path) -> ProductApplyRequest:
    return ProductApplyRequest.from_mapping(
        _read_json_mapping(path, scope="product apply request")
    )


def save_product_apply_request(path: str | Path, request: ProductApplyRequest) -> None:
    if not isinstance(request, ProductApplyRequest):
        raise _refuse("save requires ProductApplyRequest")
    with _request_publication_lock(path) as (directory_fd, name):
        verify_product_request_package_input(request)
        payload = request.to_mapping()
        created, identity = _write_json_mapping(
            directory_fd,
            name,
            payload,
            scope="product apply request",
            display_path=str(path),
        )
        try:
            verify_product_request_package_input(request)
            if (
                _read_json_mapping_at(
                    directory_fd,
                    name,
                    scope="product apply request",
                    display_path=str(path),
                )
                != payload
            ):
                raise _refuse(
                    "published product apply request changed during confirmation",
                    path=str(path),
                )
        except Exception as caught:
            exc = (
                caught
                if isinstance(caught, ProductLifecycleError)
                else _refuse(
                    "product apply request confirmation failed",
                    path=str(path),
                    error_type=type(caught).__name__,
                )
            )
            if created:
                try:
                    _remove_owned_publication(
                        directory_fd,
                        name,
                        identity,
                        scope="product apply request",
                        display_path=str(path),
                    )
                except ProductLifecycleError as cleanup_exc:
                    exc.data["publication_cleanup_error"] = cleanup_exc.to_json()[
                        "error"
                    ]
            if exc is caught:
                raise
            raise exc from caught


def load_authored_definition(path: str | Path) -> AuthoredDeploymentDefinition:
    """Strictly reopen one authored product definition for admission."""

    raw = _read_json_mapping(path, scope="authored deployment definition")
    try:
        return AuthoredDeploymentDefinition.model_validate(
            _normalise_plan_sequences(raw)
        )
    except (TypeError, ValueError) as exc:
        raise _refuse("authored deployment definition is invalid") from exc


def canonical_approval_preimage(approval: Any) -> dict[str, Any]:
    raw = (
        dict(approval)
        if isinstance(approval, Mapping)
        else dict(approval.to_dict())
        if hasattr(approval, "to_dict")
        else None
    )
    if raw is None or raw.get("schema") != APPROVAL_SCHEMA_V3:
        raise _refuse("product admission requires promotion approval v3")
    return raw


def approval_digest(approval: Any) -> str:
    return _digest(canonical_approval_preimage(approval))


def load_product_promotion_approval(path: str | Path) -> Any:
    """Descriptor-stably load the strict v3 authority."""

    from okg.substrate.deployment_promotion import (
        product_promotion_approval_from_mapping,
    )

    return product_promotion_approval_from_mapping(
        _read_json_mapping(path, scope="product promotion approval")
    )


def save_product_promotion_approval(path: str | Path, approval: Any) -> None:
    """Publish one v3 approval without replacement through the private writer."""

    from okg.substrate.deployment_promotion import (
        ProductPromotionApprovalV3,
        product_promotion_approval_from_mapping,
    )

    if not isinstance(approval, ProductPromotionApprovalV3):
        raise _refuse("save requires ProductPromotionApprovalV3")
    validated = product_promotion_approval_from_mapping(
        canonical_approval_preimage(approval)
    )
    payload = validated.to_dict()
    with _request_publication_lock(path) as (directory_fd, name):
        _write_json_mapping(
            directory_fd,
            name,
            payload,
            scope="product promotion approval",
            display_path=str(path),
        )


def canonical_admission_preimage(
    *, request_digest: str, approval_digest: str
) -> dict[str, str]:
    return {
        "schema": PLAN_ADMISSION_SCHEMA,
        "request_digest": _require_digest(request_digest, "request_digest"),
        "approval_digest": _require_digest(approval_digest, "approval_digest"),
    }


def admission_digest(*, request_digest: str, approval_digest: str) -> str:
    return _digest(
        canonical_admission_preimage(
            request_digest=request_digest,
            approval_digest=approval_digest,
        )
    )


def _require_product_action(
    plan: DeploymentProductPlan,
    request: ProductApplyRequest,
) -> PlanAction:
    if len(plan.actions) != 1:
        raise _refuse("product plan must contain exactly one action")
    action = plan.actions[0]
    if action.kind != "apply_deployment_instance":
        raise _refuse("product plan action kind is unsupported", kind=action.kind)
    required = {
        "deployment_id",
        "definition_id",
        "lock_digest",
        "sealed_release_digest",
    }
    if set(action.arguments) != required:
        raise _refuse(
            "product plan action arguments are not the closed apply vocabulary"
        )
    if action.arguments["deployment_id"] != request.target_environment_id:
        raise _refuse("product plan action targets a different environment")
    if action.arguments["definition_id"] != plan.lock.definition_id:
        raise _refuse("product plan action definition differs from the lock")
    if action.arguments["lock_digest"] != plan.lock_digest:
        raise _refuse("product plan action lock digest does not match plan")
    if (
        action.arguments["sealed_release_digest"]
        != request.sealed_release.artifact_id
    ):
        raise _refuse("product plan sealed release does not match request")
    return action


def build_product_admission(
    request: ProductApplyRequest,
    *,
    approval: Any,
    plan: DeploymentProductPlan,
) -> dict[str, Any]:
    """Build the closed identity accepted by the v3 journal boundary."""

    if not isinstance(request, ProductApplyRequest):
        raise _refuse("product admission requires ProductApplyRequest")
    approval_digest_value = approval_digest(approval)
    if approval_digest_value != request.promotion_approval_digest:
        raise _refuse("promotion approval digest does not match request")
    if not isinstance(plan, DeploymentProductPlan):
        raise _refuse("product admission requires DeploymentProductPlan")
    require_package_selection_matches_lock(request.package_selection, plan.lock)
    _require_product_action(plan, request)
    if plan.plan_digest != request.expected_product_plan_digest:
        raise _refuse("product plan digest does not match request")
    sealed = request.sealed_release
    checks = {
        "promotion_id": (request.idempotency_key, getattr(approval, "promotion_id", None)),
        "staging_environment_id": (
            request.staging_environment_id,
            getattr(approval, "staging_environment_id", None),
        ),
        "target_environment_id": (
            request.target_environment_id,
            getattr(approval, "production_environment_id", None),
        ),
        "sealed_artifact_digest": (
            sealed.artifact_id,
            getattr(approval, "artifact_id", None),
        ),
        "sealed_input_digest": (
            sealed.input_id,
            getattr(approval, "artifact_input_id", None),
        ),
        "sealed_definition_digest": (
            sealed.definition_digest,
            getattr(approval, "definition_digest", None),
        ),
        "transition_kind": (
            request.requested_transition,
            getattr(approval, "transition_kind", None),
        ),
        "request_digest": (
            request.request_digest,
            getattr(approval, "request_digest", None),
        ),
        "product_plan_digest": (
            plan.plan_digest,
            getattr(approval, "product_plan_digest", None),
        ),
        "lock_digest": (plan.lock_digest, getattr(approval, "lock_digest", None)),
        "composition_digest": (
            plan.lock.composition_digest,
            getattr(approval, "composition_digest", None),
        ),
    }
    mismatches = {
        field: {"expected": expected, "observed": observed}
        for field, (expected, observed) in checks.items()
        if expected != observed
    }
    if mismatches:
        raise _refuse("promotion approval differs from the product request", mismatches=mismatches)
    previous = getattr(approval, "expected_previous_artifact_id", None)
    if request.requested_transition == "first_install" and previous is not None:
        raise _refuse("first-install approval cannot name a previous artifact")
    if request.requested_transition == "upgrade" and previous is None:
        raise _refuse("upgrade approval requires an expected previous artifact")
    target_definition_digest = getattr(
        approval, "production_definition_digest", None
    )
    _require_digest(target_definition_digest, "production_definition_digest")
    selection = request.package_selection
    assert isinstance(selection, PackageSelection)
    body: dict[str, Any] = {
        "schema": PLAN_ADMISSION_SCHEMA,
        "request_digest": request.request_digest,
        "approval_digest": approval_digest_value,
        "admission_digest": admission_digest(
            request_digest=request.request_digest,
            approval_digest=approval_digest_value,
        ),
        "product_plan_digest": plan.plan_digest,
        "lock_digest": plan.lock_digest,
        "composition_digest": plan.lock.composition_digest,
        "definition_id": plan.lock.definition_id,
        "definition_digest": target_definition_digest,
        "sealed_artifact_digest": sealed.artifact_id,
        "target_environment_id": request.target_environment_id,
        "transition_kind": request.requested_transition,
        "package_transport_digest": selection.transport_digest,
        "package_selection_digest": selection.digest,
        "package_selection": selection.to_mapping(),
        "package_artifact_bindings": [
            item.to_mapping() for item in request.package_artifact_bindings
        ],
    }
    return body


def admit_product_apply_request(
    request: ProductApplyRequest,
    *,
    now: datetime,
) -> dict[str, Any]:
    """Reopen and authorize every request authority before journal admission."""

    if not isinstance(request, ProductApplyRequest):
        raise _refuse("product admission requires ProductApplyRequest")
    current = _utc(now)
    definition = load_authored_definition(request.authored_definition_path)
    plan = load_product_plan(request.product_plan_path)
    verify_product_request_package_input(request, plan=plan)
    try:
        authorization = require_plan_before_apply(
            plan,
            definition=definition,
            artifacts=ContainedArtifactReader(request.artifact_root),
            now=current,
            expected_plan_digest=request.expected_product_plan_digest,
        )
    except DeploymentLockError as exc:
        raise _refuse(
            "deployment product plan refused before journal admission",
            diagnostics=[item.to_dict() for item in exc.diagnostics],
        ) from exc
    if not isinstance(authorization, ApplyAuthorization):
        raise _refuse("product plan authorization returned the wrong authority")
    from okg.substrate.deployment_promotion import ProductPromotionApprovalV3

    approval = load_product_promotion_approval(request.promotion_approval_path)
    if not isinstance(approval, ProductPromotionApprovalV3):
        raise _refuse("product admission requires promotion approval v3")
    if approval_digest(approval) != request.promotion_approval_digest:
        raise _refuse("promotion approval digest changed")
    return build_product_admission(request, approval=approval, plan=plan)


@dataclass(frozen=True, slots=True)
class ProductOperationStatus:
    """Truthful read-only snapshot before controller/receipt integration exists."""

    operation_id: str
    target_environment_id: str
    transition_kind: str
    digests: Mapping[str, str]
    package: Mapping[str, Any]
    journal: Mapping[str, Any]
    status: AgentStatusEnvelope | Mapping[str, Any]
    receipts: Mapping[str, Any]
    authority_digest: str

    def __post_init__(self) -> None:
        _require_promotion_id(self.operation_id)
        _require_identifier(self.target_environment_id, "target_environment_id")
        if self.transition_kind not in {"first_install", "upgrade"}:
            raise _refuse("operation status transition kind is unsupported")
        expected_digests = {
            "request",
            "admission",
            "package_transport",
            "package_selection",
            "product_plan",
            "lock",
            "composition",
            "definition",
            "promotion_approval",
            "sealed_artifact",
            "sealed_artifact_input",
            "staging_descriptor",
            "production_descriptor",
            "supervision_evidence",
            "production_service_plan",
            "production_resources",
        }
        _strict_fields(self.digests, expected_digests, scope="operation status digests")
        for field, value in self.digests.items():
            _require_digest(value, f"operation_status.digests.{field}")
        _strict_fields(
            self.package,
            {"selection", "selection_digest", "artifact_bindings"},
            scope="operation status package",
        )
        selection = package_selection_from_mapping(self.package["selection"])
        bindings = require_package_artifact_bindings_match_selection(
            self.package["artifact_bindings"], selection
        )
        if (
            self.package["selection_digest"] != selection.digest
            or self.digests["package_selection"] != selection.digest
            or self.digests["package_transport"] != selection.transport_digest
            or self.package["artifact_bindings"]
            != [item.to_mapping() for item in bindings]
        ):
            raise _refuse("operation status package authority changed")
        _strict_fields(
            self.journal,
            {
                "mode",
                "status",
                "controller_status",
                "mutation_boundary",
                "writers_fenced",
                "release_observation",
                "updated_at",
            },
            scope="operation status journal",
        )
        if (
            self.journal["mode"]
            != "okg.deployment_product.admission_only/v1"
            or self.journal["controller_status"] != "unavailable"
            or not isinstance(self.journal["writers_fenced"], bool)
            or self.journal["release_observation"]
            != {
                "transition_kind": "first_install",
                "active_artifact_id": None,
                "previous_artifact_id": None,
                "active_absent": True,
                "previous_absent": True,
            }
        ):
            raise _refuse("operation status journal authority changed")
        _parse_timestamp(self.journal["updated_at"], "operation_status.updated_at")
        _strict_fields(
            self.receipts,
            {"verification", "verified"},
            scope="operation status receipt boundary",
        )
        if self.receipts != {"verification": "deferred", "verified": []}:
            raise _refuse("operation status cannot claim unverified receipts")
        envelope = (
            self.status
            if isinstance(self.status, AgentStatusEnvelope)
            else AgentStatusEnvelope.model_validate(self.status)
        )
        if (
            envelope.operation_id != self.operation_id
            or envelope.deployment_id != self.target_environment_id
            or envelope.mutation_classification != "read_only"
            or envelope.state == "succeeded"
            or envelope.next_actions
        ):
            raise _refuse("operation status exceeds the bounded read-only authority")
        if self.journal["status"] == "completed" and envelope.state != "blocked":
            raise _refuse("unverified completed journal must remain blocked")
        object.__setattr__(self, "digests", _freeze_json_authority(self.digests))
        object.__setattr__(
            self,
            "package",
            _freeze_json_authority(
                {
                    "selection": selection.to_mapping(),
                    "selection_digest": selection.digest,
                    "artifact_bindings": [item.to_mapping() for item in bindings],
                }
            ),
        )
        object.__setattr__(
            self,
            "journal",
            _freeze_json_authority(self.journal),
        )
        object.__setattr__(
            self,
            "receipts",
            _freeze_json_authority({"verification": "deferred", "verified": []}),
        )
        object.__setattr__(self, "status", envelope)
        body = self.to_mapping(include_authority=False)
        if self.authority_digest != _digest(body):
            raise _refuse("operation status authority digest changed")

    def to_mapping(self, *, include_authority: bool = True) -> dict[str, Any]:
        status = self.status
        assert isinstance(status, AgentStatusEnvelope)
        body: dict[str, Any] = {
            "schema": PRODUCT_OPERATION_STATUS_SCHEMA,
            "operation_id": self.operation_id,
            "target_environment_id": self.target_environment_id,
            "idempotency_key": self.operation_id,
            "transition_kind": self.transition_kind,
            "digests": _copy_json_authority(self.digests),
            "package": _copy_json_authority(self.package),
            "journal": _copy_json_authority(self.journal),
            "status": status.model_dump(mode="json", by_alias=True),
            "receipts": _copy_json_authority(self.receipts),
        }
        if include_authority:
            body["authority_digest"] = self.authority_digest
        return body

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> ProductOperationStatus:
        _strict_fields(
            raw,
            {
                "schema",
                "operation_id",
                "target_environment_id",
                "idempotency_key",
                "transition_kind",
                "digests",
                "package",
                "journal",
                "status",
                "receipts",
                "authority_digest",
            },
            scope="product operation status",
        )
        if raw.get("schema") != PRODUCT_OPERATION_STATUS_SCHEMA:
            raise _refuse("product operation status has the wrong schema")
        if raw.get("operation_id") != raw.get("idempotency_key"):
            raise _refuse("operation and idempotency identities differ")
        return cls(
            operation_id=raw["operation_id"],
            target_environment_id=raw["target_environment_id"],
            transition_kind=raw["transition_kind"],
            digests=raw["digests"],
            package=raw["package"],
            journal=raw["journal"],
            status=raw["status"],
            receipts=raw["receipts"],
            authority_digest=raw["authority_digest"],
        )


def _require_product_journal_identity(state: Mapping[str, Any]) -> PackageSelection:
    expected_fields = {
        "schema",
        "product_lifecycle_mode",
        "controller_status",
        "promotion_id",
        "environment_id",
        "artifact_id",
        "artifact_input_id",
        "staging_descriptor_digest",
        "production_descriptor_digest",
        "supervision_evidence_digest",
        "production_service_plan_digest",
        "production_resources_digest",
        "request_digest",
        "admission_digest",
        "approval_digest",
        "product_plan_digest",
        "lock_digest",
        "composition_digest",
        "definition_id",
        "definition_digest",
        "sealed_artifact_digest",
        "target_environment_id",
        "transition_kind",
        "package_transport_digest",
        "package_selection_digest",
        "package_selection",
        "package_artifact_bindings",
        "product_admission",
        "release_observation",
        "status",
        "completed_transitions",
        "transition_results",
        "operation_intents",
        "mutation_boundary",
        "writers_fenced",
        "database_changed",
        "updated_at",
    }
    _strict_fields(state, expected_fields, scope="admission-only product journal")
    if (
        state["schema"] != PROMOTION_STATE_SCHEMA
        or state["product_lifecycle_mode"]
        != "okg.deployment_product.admission_only/v1"
        or state["controller_status"] != "unavailable"
    ):
        raise _refuse("product promotion journal has the wrong schema")
    for field in (
        "artifact_id",
        "artifact_input_id",
        "staging_descriptor_digest",
        "production_descriptor_digest",
        "supervision_evidence_digest",
        "production_service_plan_digest",
        "production_resources_digest",
        "request_digest",
        "admission_digest",
        "approval_digest",
        "product_plan_digest",
        "lock_digest",
        "composition_digest",
        "definition_digest",
        "sealed_artifact_digest",
        "package_transport_digest",
        "package_selection_digest",
    ):
        _require_digest(state[field], f"journal.{field}")
    _require_promotion_id(state["promotion_id"])
    if state["environment_id"] != state["target_environment_id"]:
        raise _refuse("journal environment and product target differ")
    if state["artifact_id"] != state["sealed_artifact_digest"]:
        raise _refuse("journal sealed artifact identities differ")
    if state["transition_kind"] != "first_install" or state[
        "release_observation"
    ] != {
        "transition_kind": "first_install",
        "active_artifact_id": None,
        "previous_artifact_id": None,
        "active_absent": True,
        "previous_absent": True,
    }:
        raise _refuse("admission-only journal is not a proven first install")
    _parse_timestamp(state["updated_at"], "journal.updated_at")
    admission = state["product_admission"]
    if not isinstance(admission, Mapping):
        raise _refuse("product promotion journal has no admission authority")
    admission_fields = {
        "schema",
        "request_digest",
        "approval_digest",
        "admission_digest",
        "product_plan_digest",
        "lock_digest",
        "composition_digest",
        "definition_id",
        "definition_digest",
        "sealed_artifact_digest",
        "target_environment_id",
        "transition_kind",
        "package_transport_digest",
        "package_selection_digest",
        "package_selection",
        "package_artifact_bindings",
    }
    _strict_fields(admission, admission_fields, scope="journal product admission")
    if admission["schema"] != PLAN_ADMISSION_SCHEMA:
        raise _refuse("product promotion journal has the wrong admission schema")
    for field in admission_fields - {
        "schema",
        "package_selection",
        "package_artifact_bindings",
    }:
        if state.get(field) != admission.get(field):
            raise _refuse("journal product admission identity changed", field=field)
    if state["admission_digest"] != admission_digest(
        request_digest=state["request_digest"],
        approval_digest=state["approval_digest"],
    ):
        raise _refuse("journal admission digest does not match its preimage")
    selection = package_selection_from_mapping(state["package_selection"])
    state_bindings = require_package_artifact_bindings_match_selection(
        state["package_artifact_bindings"], selection
    )
    admission_bindings = require_package_artifact_bindings_match_selection(
        admission["package_artifact_bindings"], selection
    )
    canonical_bindings = [item.to_mapping() for item in state_bindings]
    if (
        selection.digest != state["package_selection_digest"]
        or selection.transport_digest != state["package_transport_digest"]
        or selection.to_mapping() != admission["package_selection"]
        or state_bindings != admission_bindings
        or state["package_artifact_bindings"] != canonical_bindings
        or admission["package_artifact_bindings"] != canonical_bindings
    ):
        raise _refuse("journal package selection authority changed")
    completed = state["completed_transitions"]
    if not isinstance(completed, Sequence) or isinstance(completed, (str, bytes)):
        raise _refuse("promotion completed transitions are malformed")
    if not isinstance(state["transition_results"], Mapping) or not isinstance(
        state["operation_intents"], Mapping
    ):
        raise _refuse("promotion transition authority is malformed")
    if (
        state["status"] != "product_admitted"
        or completed
        or state["transition_results"]
        or state["operation_intents"]
        or state["mutation_boundary"] != "none"
        or state["writers_fenced"] is not False
        or state["database_changed"] is not False
    ):
        raise _refuse("admission-only product journal cannot contain effect claims")
    return selection


def _bounded_status_envelope(state: Mapping[str, Any]) -> AgentStatusEnvelope:
    journal_status = state["status"]
    if journal_status == "product_admitted":
        envelope_state = "planned"
        phase = "pre_apply"
        progress = 0
        code = "production_controller_missing"
    elif journal_status == "completed":
        envelope_state = "blocked"
        phase = "status"
        progress = 99
        code = "receipt_verifier_missing"
    elif journal_status in {
        "mutating",
        "aborting",
        "rollback_required",
        "recovery_blocked",
    }:
        envelope_state = "blocked"
        phase = "apply"
        progress = 0
        code = "production_controller_missing"
    elif journal_status in {"aborted", "rolled_back", "failed"}:
        envelope_state = "failed"
        phase = "status"
        progress = 0
        code = "unregistered_diagnostic"
    else:
        raise _refuse("product journal status is unsupported", status=journal_status)
    diagnostic = AgentDiagnostic(
        code=code,
        severity="error",
        path=f"promotion_journal.{journal_status}",
        owner="system",
        message="",
        fix_hint="",
        observed=_digest(
            {
                "status": journal_status,
                "mutation_boundary": state["mutation_boundary"],
                "writers_fenced": state["writers_fenced"],
            }
        ),
        phase=phase,
    )
    return AgentStatusEnvelope(
        operation_id=state["promotion_id"],
        deployment_id=state["target_environment_id"],
        state=envelope_state,
        phase=phase,
        progress_percent=progress,
        summary="",
        diagnostics=(diagnostic,),
        next_actions=(),
        mutation_classification="read_only",
        safe_to_retry=False,
    )


def project_product_operation_status(
    state: Mapping[str, Any],
) -> ProductOperationStatus:
    selection = _require_product_journal_identity(state)
    bindings = require_package_artifact_bindings_match_selection(
        state["package_artifact_bindings"], selection
    )
    body: dict[str, Any] = {
        "schema": PRODUCT_OPERATION_STATUS_SCHEMA,
        "operation_id": state["promotion_id"],
        "target_environment_id": state["target_environment_id"],
        "idempotency_key": state["promotion_id"],
        "transition_kind": state["transition_kind"],
        "digests": {
            "request": state["request_digest"],
            "admission": state["admission_digest"],
            "package_transport": state["package_transport_digest"],
            "package_selection": state["package_selection_digest"],
            "product_plan": state["product_plan_digest"],
            "lock": state["lock_digest"],
            "composition": state["composition_digest"],
            "definition": state["definition_digest"],
            "promotion_approval": state["approval_digest"],
            "sealed_artifact": state["sealed_artifact_digest"],
            "sealed_artifact_input": state["artifact_input_id"],
            "staging_descriptor": state["staging_descriptor_digest"],
            "production_descriptor": state["production_descriptor_digest"],
            "supervision_evidence": state["supervision_evidence_digest"],
            "production_service_plan": state[
                "production_service_plan_digest"
            ],
            "production_resources": state["production_resources_digest"],
        },
        "package": {
            "selection": selection.to_mapping(),
            "selection_digest": selection.digest,
            "artifact_bindings": [item.to_mapping() for item in bindings],
        },
        "journal": {
            "mode": state["product_lifecycle_mode"],
            "status": state["status"],
            "controller_status": state["controller_status"],
            "mutation_boundary": state["mutation_boundary"],
            "writers_fenced": state["writers_fenced"],
            "release_observation": dict(state["release_observation"]),
            "updated_at": state["updated_at"],
        },
        "status": _bounded_status_envelope(state).model_dump(
            mode="json", by_alias=True
        ),
        "receipts": {"verification": "deferred", "verified": []},
    }
    body["authority_digest"] = _digest(body)
    return ProductOperationStatus.from_mapping(body)


def read_product_apply_status(
    *,
    production: Any,
    promotion_id: str,
    state_path: Path | None = None,
) -> dict[str, Any]:
    from okg.substrate.deployment_promotion import read_deployment_promotion

    state = read_deployment_promotion(
        production=production,
        promotion_id=promotion_id,
        state_path=state_path,
    )
    return project_product_operation_status(state).to_mapping()


def render_product_operation_status(
    operation_status: ProductOperationStatus | Mapping[str, Any],
) -> str:
    projected = (
        operation_status
        if isinstance(operation_status, ProductOperationStatus)
        else ProductOperationStatus.from_mapping(operation_status)
    )
    status = projected.status
    assert isinstance(status, AgentStatusEnvelope)
    lines = [
        f"operation: {projected.operation_id}",
        f"deployment: {projected.target_environment_id}",
        f"state: {status.state}",
        f"phase: {status.phase}",
        f"safe_to_retry: {str(status.safe_to_retry).lower()}",
        f"transition: {projected.transition_kind}",
        f"product_plan: {projected.digests['product_plan']}",
        f"promotion_approval: {projected.digests['promotion_approval']}",
        f"package_selection: {projected.digests['package_selection']}",
        "receipt_verification: deferred",
        f"authority: {projected.authority_digest}",
    ]
    for diagnostic in status.diagnostics:
        lines.append(f"blocker: {diagnostic.message} [{diagnostic.observed}]")
    return "\n".join(lines) + "\n"
