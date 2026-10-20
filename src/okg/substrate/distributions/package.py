"""The v1 immutable distribution package format.

The implementation intentionally uses the standard library only.  The
archive is a small, canonical subset of POSIX USTAR and is parsed directly so
the verifier never delegates authority to a permissive archive extractor.
ZIP wheels are similarly inspected at both the central and local-header
boundaries before their bytes are exposed.
"""

from __future__ import annotations

import base64
import csv
from dataclasses import dataclass, field
import ctypes
import errno
import hashlib
import importlib.metadata
import io
import json
import os
from pathlib import Path
import posixpath
import re
import secrets
import stat
import struct
import tempfile
import threading
import unicodedata
import zlib
from collections.abc import Callable, Mapping, Sequence
from types import MappingProxyType
from typing import Any, BinaryIO, Literal, cast
from urllib.parse import urlsplit
import zipfile

from packaging.version import InvalidVersion, Version
from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from okg.substrate.contracts.deployment_product.models import (
    DistributionRelease,
    OwnerQualifiedReference,
    ProductBundle,
    TypedArtifactIdentity,
)
from okg.substrate.contracts.deployment_product.typed_assets import TypedAsset


SCHEMA = "okg.deployment_product.distribution_package/v1"
SCHEMA_V2 = "okg.deployment_product.distribution_package/v2"
_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_ID_RE = re.compile(r"^[^\s:;,/\\]+$")
_OWNER_ID_RE = re.compile(r"^[^\s:;,/\\]+:[^\s:;,/\\]+$")
_HEX_RE = re.compile(r"^[0-9a-f]+$")
_REVISION_RE = re.compile(r"^[0-9a-f]{40}$")
_CHUNK = 1024 * 1024
_FileIdentity = tuple[int, int, int, int, int, int]

LIMITS: Mapping[str, int] = {
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
    "module_declarations": 1024,
    "source_symlink_hops": 16,
    "stream_chunk_bytes": _CHUNK,
}

_PUBLIC_COMPAT_KEYS = frozenset({"contract_versions", "capabilities"})
_PUBLIC_PROVENANCE_KEYS = frozenset(
    {"source", "repository_url", "revision", "artifact_ref"}
)
_PUBLIC_SOURCES = frozenset({"distribution", "bundle", "package"})


class DistributionPackageError(ValueError):
    """Stable refusal from the distribution package boundary."""

    def __init__(
        self,
        code: str,
        logical_path: str = "",
        message: str | None = None,
        *,
        limit: str | None = None,
        observed: int | None = None,
        maximum: int | None = None,
    ) -> None:
        self.code = code
        self.logical_path = logical_path
        self.limit = limit
        self.observed = observed
        self.maximum = maximum
        text = message or code
        super().__init__(text)

    @property
    def payload(self) -> dict[str, Any]:
        result: dict[str, Any] = {"code": self.code, "logical_path": self.logical_path}
        if self.limit is not None:
            result.update(
                limit=self.limit, observed=self.observed, maximum=self.maximum
            )
        return result


def _refuse(
    code: str, path: str = "", message: str | None = None
) -> DistributionPackageError:
    return DistributionPackageError(code, path, message)


def _limit(
    name: str, observed: int, *, path: str = "", code: str = "resource_limit_exceeded"
) -> DistributionPackageError:
    return DistributionPackageError(
        code,
        path,
        f"{name} exceeded ({observed} > {LIMITS[name]})",
        limit=name,
        observed=observed,
        maximum=LIMITS[name],
    )


def _require_digest(value: object, field: str) -> str:
    if not isinstance(value, str) or not _DIGEST_RE.fullmatch(value):
        raise ValueError(
            f"{field} must be sha256:<64 lowercase hexadecimal characters>"
        )
    return value


def _require_identifier(value: object, field: str) -> str:
    if not isinstance(value, str) or not value or not _ID_RE.fullmatch(value):
        raise ValueError(f"{field} must be a non-empty identifier")
    return value


def _require_owner_id(value: object, field: str = "owner:id") -> str:
    if not isinstance(value, str) or not _OWNER_ID_RE.fullmatch(value):
        raise ValueError(f"{field} must have the form owner:id")
    owner, ident = value.split(":", 1)
    _require_identifier(owner, f"{field}.owner")
    _require_identifier(ident, f"{field}.id")
    return value


def _require_module_id(value: object, field: str = "module_id") -> str:
    """Return one owner-qualified module id in the strict package spelling.

    ``_OWNER_ID_RE`` already refuses whitespace and the ``: ; , / \\``
    separators, so exactly one colon survives.  A module id also names a
    directory once a provider materializes it, so a segment that is ``.``,
    ``..``, or otherwise dot-leading is refused here rather than left to a
    later path join.
    """

    module_id = _require_owner_id(value, field)
    for segment in module_id.split(":"):
        if segment.startswith("."):
            raise ValueError(
                f"{field} must not contain a relative or dot-leading segment"
            )
    return module_id


def _require_release_bound(value: object, field: str) -> str:
    """Return one plain PEP 440 release version usable as a range bound.

    A bound carrying an epoch, pre-release, post-release, dev, or local
    segment is refused: the running OKG build is compared by its base release
    only (see :class:`OkgCompatibility`), so such a bound could never change a
    verdict and would read as a promise the comparison does not keep.
    """

    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be a PEP 440 release version")
    try:
        parsed = Version(value)
    except InvalidVersion as exc:
        raise ValueError(f"{field} must be a PEP 440 release version") from exc
    if parsed.epoch or parsed.pre or parsed.post or parsed.dev or parsed.local:
        raise ValueError(
            f"{field} must be a plain release version with no epoch, "
            "pre-release, post-release, dev, or local segment"
        )
    if str(parsed) != value:
        raise ValueError(f"{field} must use the normalized PEP 440 spelling")
    return value


def _positive_int(value: object, field: str, *, maximum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    if maximum is not None and value > maximum:
        raise ValueError(f"{field} exceeds {maximum}")
    return value


def _nfc(value: object, *, path: str = "$") -> None:
    if isinstance(value, str):
        if unicodedata.normalize("NFC", value) != value:
            raise ValueError(f"string at {path} is not NFC")
        return
    if isinstance(value, Mapping):
        for key, child in value.items():
            if not isinstance(key, str):
                raise ValueError(f"mapping key at {path} is not a string")
            _nfc(key, path=f"{path}.{key}")
            _nfc(child, path=f"{path}.{key}")
    elif isinstance(value, (tuple, list)):
        for index, child in enumerate(value):
            _nfc(child, path=f"{path}[{index}]")


def _canonical_json(value: object) -> bytes:
    _nfc(value)
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError) as exc:
        raise _refuse(
            "manifest_invalid", message="value is not canonical JSON"
        ) from exc


def _canonical_digest(value: object) -> str:
    return "sha256:" + hashlib.sha256(_canonical_json(value)).hexdigest()


def _to_json(value: object) -> object:
    if isinstance(value, BaseModel):
        return _to_json(
            value.model_dump(mode="python", by_alias=True, exclude_none=False)
        )
    if isinstance(value, TypedAsset):
        return _to_json(value.to_dict())
    if isinstance(value, Mapping):
        return {str(key): _to_json(child) for key, child in value.items()}
    if isinstance(value, (tuple, list)):
        return [_to_json(child) for child in value]
    if isinstance(value, Path):
        return value.as_posix()
    return value


def _safe_member_path(value: object, *, field: str = "member_path") -> str:
    if not isinstance(value, str) or not value or "\x00" in value or "\\" in value:
        raise ValueError(f"{field} must be a safe relative POSIX path")
    if unicodedata.normalize("NFC", value) != value:
        raise ValueError(f"{field} must already be Unicode NFC")
    if value.startswith("/") or re.match(r"^[A-Za-z]:", value):
        raise ValueError(f"{field} must be relative")
    lowered = value.lower()
    if "%2f" in lowered or "%5c" in lowered:
        raise ValueError(f"{field} must not contain encoded separators")
    parts = value.split("/")
    if any(not part or part in {".", ".."} for part in parts):
        raise ValueError(f"{field} contains an unsafe path component")
    if posixpath.normpath(value) != value:
        raise ValueError(f"{field} is not normalized")
    return value


def _typed_asset_mapping(asset: TypedAsset) -> dict[str, object]:
    compatibility = dict(asset.compatibility)
    provenance = dict(asset.provenance)
    if asset.override_slots:
        raise DistributionPackageError(
            "metadata_forbidden",
            asset.qualified_id,
            "override slots are not package-v1 metadata",
        )
    if (
        set(compatibility) - _PUBLIC_COMPAT_KEYS
        or set(provenance) - _PUBLIC_PROVENANCE_KEYS
    ):
        raise DistributionPackageError(
            "metadata_forbidden",
            asset.qualified_id,
            "typed asset metadata is outside the package-v1 closure",
        )
    if compatibility:
        for key in compatibility:
            values = compatibility[key]
            if not isinstance(values, (tuple, list)) or any(
                not isinstance(item, str) or not _ID_RE.fullmatch(item)
                for item in values
            ):
                raise DistributionPackageError(
                    "metadata_forbidden",
                    asset.qualified_id,
                    "compatibility values must be identifier sequences",
                )
    if provenance:
        source = provenance.get("source")
        if source not in _PUBLIC_SOURCES:
            raise DistributionPackageError(
                "metadata_forbidden",
                asset.qualified_id,
                "invalid public provenance source",
            )
        required = {"source", "repository_url", "revision", "artifact_ref"}
        if set(provenance) != required:
            raise DistributionPackageError(
                "metadata_forbidden",
                asset.qualified_id,
                "provenance must use the closed package-v1 shape",
            )
        _public_url(provenance["repository_url"], "provenance.repository_url")
        if not isinstance(provenance["revision"], str) or not _REVISION_RE.fullmatch(
            provenance["revision"]
        ):
            raise DistributionPackageError(
                "metadata_forbidden",
                asset.qualified_id,
                "provenance revision must be a 40-hex commit",
            )
        try:
            _require_owner_id(provenance["artifact_ref"], "provenance.artifact_ref")
        except ValueError as exc:
            raise DistributionPackageError(
                "metadata_forbidden", asset.qualified_id, str(exc)
            ) from exc
    result = _to_json(asset)
    if not isinstance(result, dict):  # pragma: no cover - TypedAsset is mapping-shaped
        raise AssertionError("TypedAsset did not serialize to a mapping")
    return result


def _public_url(value: object, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or unicodedata.normalize("NFC", value) != value
    ):
        raise DistributionPackageError(
            "metadata_forbidden", field, "public URL is invalid"
        )
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise DistributionPackageError(
            "metadata_forbidden", field, "public URL must be HTTPS without userinfo"
        )
    if parsed.query or parsed.fragment:
        raise DistributionPackageError(
            "metadata_forbidden", field, "public URL must not contain query or fragment"
        )
    return value


class _StrictModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, validate_assignment=True
    )

    @model_validator(mode="before")
    @classmethod
    def _normalize_sequences(cls, value: object) -> object:
        if not isinstance(value, Mapping):
            return value
        result = dict(value)
        for key in (
            "bundles",
            "assets",
            "objects",
            "package_identities",
            "package_refs",
            "public_extensions",
            "capabilities",
            "placeholders",
            "module_declarations",
            "asset_refs",
        ):
            if isinstance(result.get(key), list):
                result[key] = tuple(result[key])
        return result

    def model_dump(
        self, *, mode: Literal["json", "python"] | str = "python", **kwargs: Any
    ) -> dict[str, Any]:
        # ``TypedAsset`` is an intentionally inert dataclass, not a pydantic
        # model.  Normalize it explicitly so callers can persist a manifest
        # through the normal pydantic API without invoking a serializer that
        # might import or execute package payloads.
        value = super().model_dump(mode="python", **kwargs)
        normalized = _to_json(value)
        assert isinstance(normalized, dict)
        return normalized

    def model_dump_json(
        self,
        *,
        by_alias: bool | None = None,
        exclude_none: bool = False,
        sort_keys: bool = False,
        **_: Any,
    ) -> str:
        return json.dumps(
            self.model_dump(by_alias=by_alias, exclude_none=exclude_none),
            ensure_ascii=False,
            sort_keys=sort_keys,
            separators=(",", ":") if sort_keys else None,
        )

    def model_copy(
        self, *, update: Mapping[str, object] | None = None, deep: bool = False
    ) -> "_StrictModel":
        """Copy through validation; pydantic's default copy trusts updates."""

        payload = self.model_dump(mode="python", by_alias=True, exclude_none=False)
        if update:
            payload.update(dict(update))
        return type(self).model_validate(payload)


class DistributionObject(_StrictModel):
    digest: str
    size: int
    media_type: str

    _digest = field_validator("digest")(_require_digest)

    @field_validator("size")
    @classmethod
    def _size(cls, value: int) -> int:
        return _positive_int(value, "object.size", maximum=LIMITS["object_bytes_each"])

    @field_validator("media_type")
    @classmethod
    def _media_type(cls, value: str) -> str:
        if not value or any(char.isspace() for char in value):
            raise ValueError("object.media_type must be non-empty")
        return value


class AssetLocator(_StrictModel):
    object_digest: str
    member_path: str | None = None
    member_crc32: str | None = None
    member_compressed_size: int | None = None
    compression_method: Literal["stored", "deflated"] | None = None

    _digest = field_validator("object_digest")(_require_digest)

    @field_validator("member_path")
    @classmethod
    def _path(cls, value: str | None) -> str | None:
        return None if value is None else _safe_member_path(value)

    @field_validator("member_crc32")
    @classmethod
    def _crc(cls, value: str | None) -> str | None:
        if value is not None and (len(value) != 8 or not _HEX_RE.fullmatch(value)):
            raise ValueError(
                "member_crc32 must be eight lowercase hexadecimal characters"
            )
        return value

    @field_validator("member_compressed_size")
    @classmethod
    def _compressed(cls, value: int | None) -> int | None:
        return None if value is None else _positive_int(value, "member_compressed_size")

    @model_validator(mode="after")
    def _member_shape(self) -> "AssetLocator":
        values = (
            self.member_path,
            self.member_crc32,
            self.member_compressed_size,
            self.compression_method,
        )
        if self.member_path is None and any(value is not None for value in values[1:]):
            raise ValueError("whole-object locators cannot carry ZIP member metadata")
        if self.member_path is not None and any(value is None for value in values[1:]):
            raise ValueError("member locators require CRC, compressed size, and method")
        return self


class UnresolvedPlaceholder(_StrictModel):
    kind: Literal["secret", "endpoint"]
    owner: str
    ref: str

    _owner = field_validator("owner")(_require_identifier)
    _ref = field_validator("ref")(_require_identifier)


class PublicExtensionMetadata(_StrictModel):
    subject: OwnerQualifiedReference
    package_ref: TypedArtifactIdentity
    api_version: str
    entry_point_group: str
    entry_point_name: str
    capabilities: tuple[str, ...] = ()
    placeholders: tuple[UnresolvedPlaceholder, ...] = ()

    @field_validator("api_version", "entry_point_group", "entry_point_name")
    @classmethod
    def _public_identifier(cls, value: str) -> str:
        return _require_identifier(value, "public extension identifier")

    @field_validator("capabilities")
    @classmethod
    def _capabilities(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(set(value)):
            raise ValueError("extension capabilities must be unique")
        return tuple(
            sorted(_require_identifier(item, "extension capability") for item in value)
        )

    @field_validator("placeholders")
    @classmethod
    def _placeholders(
        cls, value: tuple[UnresolvedPlaceholder, ...]
    ) -> tuple[UnresolvedPlaceholder, ...]:
        keys = [(item.kind, item.owner, item.ref) for item in value]
        if len(keys) != len(set(keys)):
            raise ValueError("extension placeholders must be unique")
        return tuple(sorted(value, key=lambda item: (item.kind, item.owner, item.ref)))


class DistributionPackageProvenance(_StrictModel):
    repository_url: str
    revision: str
    builder: str

    @field_validator("repository_url")
    @classmethod
    def _url(cls, value: str) -> str:
        return _public_url(value, "provenance.repository_url")

    @field_validator("revision")
    @classmethod
    def _revision(cls, value: str) -> str:
        if not _REVISION_RE.fullmatch(value):
            raise ValueError(
                "provenance.revision must be 40 lowercase hexadecimal characters"
            )
        return value

    @field_validator("builder")
    @classmethod
    def _builder(cls, value: str) -> str:
        return _require_identifier(value, "provenance.builder")


class PackagedAsset(_StrictModel):
    asset: Any
    locator: AssetLocator
    uncompressed_size: int

    @field_validator("asset", mode="before")
    @classmethod
    def _asset(cls, value: object) -> TypedAsset:
        try:
            asset = (
                value if isinstance(value, TypedAsset) else TypedAsset.from_value(value)
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("asset must be a valid TypedAsset") from exc
        _typed_asset_mapping(asset)
        return asset

    @field_validator("uncompressed_size")
    @classmethod
    def _size(cls, value: int) -> int:
        return _positive_int(
            value,
            "asset.uncompressed_size",
            maximum=LIMITS["selected_asset_bytes_each"],
        )

    @model_validator(mode="after")
    def _asset_identity(self) -> "PackagedAsset":
        if self.locator.member_path is None:
            # Object size is checked against this field when the manifest is
            # assembled, because the referenced object may be declared later.
            return self
        if self.uncompressed_size > LIMITS["wheel_member_expanded_bytes"]:
            raise ValueError("selected wheel member exceeds package-v1 limit")
        return self


class OkgCompatibility(_StrictModel):
    """The OKG release window one package-v2 distribution declares.

    ``min_version`` is inclusive, ``max_version_exclusive`` is exclusive, and
    both are plain PEP 440 release versions (no epoch, pre-release,
    post-release, dev, or local segment).

    **A running OKG build is compared by its base release only.**
    ``0.1.0.dev7``, ``0.1.0rc2``, ``0.1.0.post1``, and ``0.1.0+local`` all
    compare as ``0.1.0``.  A pre-release sorts *below* its own release under
    PEP 440, so comparing it directly would refuse ``0.1.0rc2`` against
    ``min_version: "0.1.0"`` -- refusing the very build that carries the
    feature.  Comparing the base release keeps a development build and its
    release on the same side of every bound, in both directions: a
    ``2.0.0.dev1`` build is refused by ``max_version_exclusive: "2.0.0"``
    exactly as ``2.0.0`` is.
    """

    min_version: str
    max_version_exclusive: str

    @field_validator("min_version")
    @classmethod
    def _min(cls, value: str) -> str:
        return _require_release_bound(value, "okg_compatibility.min_version")

    @field_validator("max_version_exclusive")
    @classmethod
    def _max(cls, value: str) -> str:
        return _require_release_bound(value, "okg_compatibility.max_version_exclusive")

    @model_validator(mode="after")
    def _ordered(self) -> "OkgCompatibility":
        if Version(self.min_version) >= Version(self.max_version_exclusive):
            raise ValueError(
                "okg_compatibility.min_version must precede max_version_exclusive"
            )
        return self

    def supports(self, okg_version: str) -> bool:
        """Return whether ``okg_version``'s base release falls in the window."""

        try:
            observed = Version(Version(okg_version).base_version)
        except InvalidVersion as exc:
            raise ValueError("okg_version must be a PEP 440 version") from exc
        return (
            Version(self.min_version) <= observed < Version(self.max_version_exclusive)
        )


class ModuleDeclaration(_StrictModel):
    """One owner-qualified ontology module a package-v2 distribution supplies.

    ``module_id`` is the single spelling of the module's identity, in the
    strict ``owner:name`` form ``_OWNER_ID_RE`` fixes.  The owner is a derived
    property rather than a second field, so one module can never carry two
    disagreeing identities.
    """

    module_id: str
    asset_refs: tuple[OwnerQualifiedReference, ...] = ()

    @field_validator("module_id")
    @classmethod
    def _identity(cls, value: str) -> str:
        return _require_module_id(value, "module_declaration.module_id")

    @field_validator("asset_refs")
    @classmethod
    def _refs(
        cls, value: tuple[OwnerQualifiedReference, ...]
    ) -> tuple[OwnerQualifiedReference, ...]:
        keys = [item.qualified_id for item in value]
        if len(keys) != len(set(keys)):
            raise ValueError("module declaration asset references must be unique")
        return tuple(sorted(value, key=lambda item: item.qualified_id))

    @property
    def owner(self) -> str:
        return self.module_id.split(":", 1)[0]

    @property
    def name(self) -> str:
        return self.module_id.split(":", 1)[1]


class DistributionPackageManifest(_StrictModel):
    schema_version: Literal[
        "okg.deployment_product.distribution_package/v1",
        "okg.deployment_product.distribution_package/v2",
    ] = SCHEMA
    distribution_release: DistributionRelease
    bundles: tuple[ProductBundle, ...] = ()
    assets: tuple[PackagedAsset, ...] = ()
    objects: tuple[DistributionObject, ...] = ()
    package_identities: tuple[TypedArtifactIdentity, ...] = ()
    public_extensions: tuple[PublicExtensionMetadata, ...] = ()
    provenance: DistributionPackageProvenance
    # Package v2 only.  A v1 manifest that carries either field is refused,
    # and neither field is emitted into a v1 canonical mapping, so every v1
    # release, bundle, and transport digest is byte-identical to package v1.
    okg_compatibility: OkgCompatibility | None = None
    module_declarations: tuple[ModuleDeclaration, ...] = ()

    @model_validator(mode="before")
    @classmethod
    def _normalize_sequences(cls, value: object) -> object:
        if not isinstance(value, Mapping):
            return value
        result = dict(value)
        for key in (
            "bundles",
            "assets",
            "objects",
            "package_identities",
            "package_refs",
            "public_extensions",
            "module_declarations",
        ):
            if isinstance(result.get(key), list):
                result[key] = tuple(result[key])
        return result

    @property
    def is_v2(self) -> bool:
        return self.schema_version == SCHEMA_V2

    @model_validator(mode="after")
    def _references(self) -> "DistributionPackageManifest":
        if len(self.bundles) > LIMITS["bundles"]:
            raise ValueError("too many bundles")
        if len(self.assets) > LIMITS["assets"]:
            raise ValueError("too many assets")
        if len(self.objects) > LIMITS["objects"]:
            raise ValueError("too many objects")
        if len(self.package_identities) > LIMITS["package_identities"]:
            raise ValueError("too many package identities")
        if len(self.public_extensions) > LIMITS["public_extensions"]:
            raise ValueError("too many public extensions")
        if (
            len(self.assets)
            and sum(item.uncompressed_size for item in self.assets)
            > LIMITS["selected_asset_bytes_total"]
        ):
            raise ValueError("selected assets exceed package-v1 aggregate limit")
        object_by_digest = {item.digest: item for item in self.objects}
        if len(object_by_digest) != len(self.objects):
            raise ValueError("objects must be unique by digest")
        asset_by_id: dict[str, PackagedAsset] = {}
        for item in self.assets:
            if item.asset.qualified_id in asset_by_id:
                raise ValueError("assets must be unique by owner-qualified id")
            asset_by_id[item.asset.qualified_id] = item
            obj = object_by_digest.get(item.locator.object_digest)
            if obj is None:
                raise ValueError("asset locator references an unknown object")
            if item.locator.member_path is None and obj.size != item.uncompressed_size:
                raise ValueError("whole-object asset size must equal object size")

        def check_refs(refs: Sequence[OwnerQualifiedReference], owner: str) -> None:
            seen: set[str] = set()
            for ref in refs:
                key = ref.qualified_id
                if key in seen:
                    raise ValueError(f"{owner} asset references must be unique")
                seen.add(key)
                selected = asset_by_id.get(key)
                if selected is None or selected.asset.digest != ref.digest:
                    raise ValueError(
                        f"{owner} asset reference does not resolve to the identical TypedAsset digest"
                    )

        check_refs(self.distribution_release.asset_refs, "release")
        for bundle in self.bundles:
            check_refs(bundle.asset_refs, f"bundle:{bundle.bundle_id}")
        release_ids = set(self.distribution_release.bundle_ids)
        bundle_ids = {item.bundle_id for item in self.bundles}
        if len(bundle_ids) != len(self.bundles) or release_ids != bundle_ids:
            raise ValueError("release.bundle_ids must exactly match manifest bundles")
        for bundle in self.bundles:
            if (
                bundle.distribution_id != self.distribution_release.distribution_id
                or bundle.distribution_version != self.distribution_release.version
            ):
                raise ValueError("bundle distribution identity differs from release")
        package_keys = [
            (item.owner, item.ref, item.kind, item.version, item.digest)
            for item in self.package_identities
        ]
        if len(package_keys) != len(set(package_keys)):
            raise ValueError("package identities must be unique")
        extension_keys = [
            (
                item.subject.owner,
                item.subject.ref,
                item.api_version,
                item.entry_point_group,
                item.entry_point_name,
            )
            for item in self.public_extensions
        ]
        if len(extension_keys) != len(set(extension_keys)):
            raise ValueError("public extensions must be unique")
        package_key_set = set(package_keys)
        for extension in self.public_extensions:
            package_ref = extension.package_ref
            if (
                package_ref.owner,
                package_ref.ref,
                package_ref.kind,
                package_ref.version,
                package_ref.digest,
            ) not in package_key_set:
                raise ValueError(
                    "public extension package_ref does not resolve to an identical package identity"
                )
            selected = asset_by_id.get(extension.subject.qualified_id)
            subject = extension.subject
            if selected is None or (
                subject.kind,
                subject.version,
                subject.digest,
            ) != (
                selected.asset.kind,
                selected.asset.version,
                selected.asset.digest,
            ):
                raise ValueError(
                    "public extension subject does not resolve to an identical packaged asset"
                )
        placeholder_keys = [
            (placeholder.kind, placeholder.owner, placeholder.ref)
            for item in self.public_extensions
            for placeholder in item.placeholders
        ]
        if len(placeholder_keys) > LIMITS["placeholders"]:
            raise ValueError("too many unresolved placeholders")
        if len(placeholder_keys) != len(set(placeholder_keys)):
            raise ValueError("unresolved placeholders must be unique")
        if not self.is_v2 and (
            self.okg_compatibility is not None or self.module_declarations
        ):
            raise ValueError(
                "okg_compatibility and module_declarations require package schema v2"
            )
        if len(self.module_declarations) > LIMITS["module_declarations"]:
            raise ValueError("too many module declarations")
        module_ids = [item.module_id for item in self.module_declarations]
        if len(module_ids) != len(set(module_ids)):
            raise ValueError("module declarations must be unique by module id")
        for declaration in self.module_declarations:
            check_refs(declaration.asset_refs, f"module:{declaration.module_id}")
        return self

    def to_mapping(self, *, omit_release_digest: bool = False) -> dict[str, object]:
        release = _to_json(self.distribution_release)
        if omit_release_digest:
            assert isinstance(release, dict)
            release.pop("release_digest", None)
        assets = []
        for item in self.assets:
            asset = _typed_asset_mapping(item.asset)
            assets.append(
                {
                    "asset": asset,
                    "locator": _to_json(item.locator),
                    "uncompressed_size": item.uncompressed_size,
                }
            )
        mapping: dict[str, object] = {
            "schema_version": self.schema_version,
            "distribution_release": release,
            "bundles": [
                _to_json(item)
                for item in sorted(
                    self.bundles,
                    key=lambda item: (item.owner, item.bundle_id, item.version),
                )
            ],
            "assets": sorted(
                assets,
                key=lambda item: (
                    item["asset"]["owner"],
                    item["asset"]["id"],
                    item["asset"]["kind"],
                    item["asset"]["version"],
                ),
            ),
            "objects": [
                _to_json(item)
                for item in sorted(self.objects, key=lambda item: item.digest)
            ],
            "package_identities": [
                _to_json(item)
                for item in sorted(
                    self.package_identities,
                    key=lambda item: (item.owner, item.ref, item.kind, item.version),
                )
            ],
            "public_extensions": [
                _to_json(item)
                for item in sorted(
                    self.public_extensions,
                    key=lambda item: (
                        item.subject.owner,
                        item.subject.ref,
                        item.api_version,
                        item.entry_point_group,
                        item.entry_point_name,
                    ),
                )
            ],
            "provenance": _to_json(self.provenance),
        }
        if not self.is_v2:
            # Package v1 keeps exactly the v1 key set, so its canonical bytes
            # -- and therefore its release, bundle, and transport digests --
            # are unchanged by the v2 fields existing on the model.
            return mapping
        mapping["okg_compatibility"] = (
            None if self.okg_compatibility is None else _to_json(self.okg_compatibility)
        )
        mapping["module_declarations"] = [
            _to_json(item)
            for item in sorted(
                self.module_declarations, key=lambda item: item.module_id
            )
        ]
        return mapping


@dataclass(frozen=True, slots=True)
class DistributionPackageSource:
    """Builder-only source path rooted at an absolute directory."""

    root: Path
    path: Path

    def __post_init__(self) -> None:
        root = Path(self.root)
        path = Path(self.path)
        if not root.is_absolute() or not path.is_absolute():
            raise ValueError("distribution sources require absolute root and path")
        if root.is_symlink() or not root.is_dir():
            raise ValueError("distribution source root must be a real directory")
        try:
            path.relative_to(root)
        except ValueError as exc:
            raise ValueError(
                "distribution source path must be contained by root"
            ) from exc
        # Validate link containment at descriptor construction as well as at
        # capture time.  The later anchored walk remains authoritative for
        # races; this early check gives callers an immediate refusal for an
        # obviously escaping, dangling, or cyclic source descriptor.
        root_fd = _open_anchor(root, directory=True)
        try:
            relative = list(path.relative_to(root).parts)
            fd, _ = _resolve_contained(root_fd, relative)
            os.close(fd)
        except (OSError, DistributionPackageError) as exc:
            raise ValueError(f"unsafe distribution source: {path}") from exc
        finally:
            os.close(root_fd)
        object.__setattr__(self, "root", root)
        object.__setattr__(self, "path", path)


@dataclass(frozen=True, slots=True)
class _ArchiveObject:
    offset: int
    size: int


_RUNTIME_RECEIPT_SCHEMA = "okg.extension_runtime.package_verification_receipt/v1"
_RUNTIME_OBJECT_INDEX_SCHEMA = "okg.extension_runtime.verified_object_index/v1"
_RUNTIME_ASSET_INDEX_SCHEMA = "okg.extension_runtime.verified_asset_index/v1"
_RUNTIME_PACKAGE_INDEX_SCHEMA = "okg.extension_runtime.package_identity_index/v1"
_RUNTIME_EXTENSION_INDEX_SCHEMA = "okg.extension_runtime.public_extension_index/v1"
_RUNTIME_VERIFICATION_PROFILE_SCHEMA = (
    "okg.distribution_package.verification_profile/v1"
)


def _runtime_raw_ref(digest: str, size: int) -> dict[str, object]:
    return {
        "digest_domain": "raw-bytes/v1",
        "digest": _require_digest(digest, "raw_blob.digest"),
        "size": _positive_int(size, "raw_blob.size"),
    }


def _runtime_canonical_ref(schema: str, digest: str) -> dict[str, str]:
    return {
        "digest_domain": "canonical-json/v1",
        "schema_version": schema,
        "digest": _require_digest(digest, "canonical_record.digest"),
    }


@dataclass(frozen=True, slots=True)
class PackageVerificationReceiptV1:
    """Path-free proof projected from one successful package verification."""

    manifest_raw_blob: Mapping[str, object]
    transport_archive_raw_blob: Mapping[str, object]
    verified_object_index_digest: str
    verified_asset_index_digest: str
    package_identity_index_digest: str
    public_extension_index_digest: str
    verification_profile_digest: str
    # The schema of the manifest this receipt was projected from.  It defaults
    # to package v1 so every v1 receipt preimage -- and digest -- is unchanged.
    package_schema: str = SCHEMA

    def preimage(self) -> dict[str, object]:
        return {
            "schema_version": _RUNTIME_RECEIPT_SCHEMA,
            "distribution_package_schema": self.package_schema,
            "manifest": dict(self.manifest_raw_blob),
            "transport_archive": dict(self.transport_archive_raw_blob),
            "verified_object_index_digest": _runtime_canonical_ref(
                _RUNTIME_OBJECT_INDEX_SCHEMA, self.verified_object_index_digest
            ),
            "verified_asset_index_digest": _runtime_canonical_ref(
                _RUNTIME_ASSET_INDEX_SCHEMA, self.verified_asset_index_digest
            ),
            "package_identity_index_digest": _runtime_canonical_ref(
                _RUNTIME_PACKAGE_INDEX_SCHEMA, self.package_identity_index_digest
            ),
            "public_extension_index_digest": _runtime_canonical_ref(
                _RUNTIME_EXTENSION_INDEX_SCHEMA, self.public_extension_index_digest
            ),
            "verification_profile_digest": _runtime_canonical_ref(
                _RUNTIME_VERIFICATION_PROFILE_SCHEMA,
                self.verification_profile_digest,
            ),
        }

    @property
    def digest(self) -> str:
        return _canonical_digest(self.preimage())

    def to_record(self) -> dict[str, object]:
        """Return the closed public projection consumed by the v2 planner."""

        value = self.preimage()
        value.pop("schema_version")
        value["package_verification_receipt_digest"] = _runtime_canonical_ref(
            _RUNTIME_RECEIPT_SCHEMA, self.digest
        )
        return value


@dataclass(frozen=True, slots=True)
class ExtensionRuntimePackageAuthority:
    """Closeable retained-FD reader for exact verified package objects.

    Descriptor numbers, archive offsets, and the package pathname are private
    transport state.  The only public byte selector is an already verified raw
    object digest.
    """

    receipt: PackageVerificationReceiptV1
    _objects: Mapping[str, _ArchiveObject] = field(repr=False)
    _fd: int = field(repr=False)
    _identity: _FileIdentity = field(repr=False)
    _size: int = field(repr=False)
    _closed: bool = field(default=False, init=False, repr=False)
    _lock: threading.RLock = field(
        default_factory=threading.RLock, init=False, repr=False
    )

    def to_record(self) -> dict[str, object]:
        return self.receipt.to_record()

    def _stable_stat(self) -> os.stat_result:
        if self._closed or self._fd < 0:
            raise DistributionPackageError(
                "extension_package_object_mismatch",
                "package_verification_receipt",
                "runtime package authority is closed",
            )
        try:
            observed = os.fstat(self._fd)
        except OSError as exc:
            raise DistributionPackageError(
                "extension_package_object_mismatch",
                "package_verification_receipt",
                "runtime package authority is unavailable",
            ) from exc
        if (
            _file_identity(observed) != self._identity
            or observed.st_size != self._size
            or observed.st_nlink != 1
            or not stat.S_ISREG(observed.st_mode)
        ):
            raise DistributionPackageError(
                "extension_package_object_mismatch",
                "package_verification_receipt",
                "retained package object identity changed",
            )
        return observed

    def read_verified_object(self, raw_blob: str) -> bytes:
        """Read one complete indexed object through the retained descriptor."""

        try:
            digest = _require_digest(raw_blob, "raw_blob")
        except ValueError as exc:
            raise DistributionPackageError(
                "extension_package_object_mismatch",
                "raw_blob",
                "verified object digest is invalid",
            ) from exc
        selected = self._objects.get(digest)
        if selected is None:
            raise DistributionPackageError(
                "extension_package_object_mismatch",
                "raw_blob",
                "verified object digest is not indexed",
            )
        with self._lock:
            self._stable_stat()
            chunks: list[bytes] = []
            remaining = selected.size
            offset = selected.offset
            while remaining:
                try:
                    chunk = os.pread(self._fd, min(_CHUNK, remaining), offset)
                except OSError as exc:
                    raise DistributionPackageError(
                        "extension_package_object_mismatch",
                        "raw_blob",
                        "verified object could not be read",
                    ) from exc
                if not chunk:
                    raise DistributionPackageError(
                        "extension_package_object_mismatch",
                        "raw_blob",
                        "verified object was truncated",
                    )
                chunks.append(chunk)
                offset += len(chunk)
                remaining -= len(chunk)
            self._stable_stat()
            payload = b"".join(chunks)
            if len(payload) != selected.size or _digest_bytes(payload) != digest:
                raise DistributionPackageError(
                    "extension_package_object_mismatch",
                    "raw_blob",
                    "verified object bytes changed",
                )
            return payload

    @property
    def closed(self) -> bool:
        with self._lock:
            return self._closed

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            fd = self._fd
            object.__setattr__(self, "_fd", -1)
            object.__setattr__(self, "_closed", True)
            try:
                os.close(fd)
            except OSError:
                # Ownership is already retired; retrying could close a reused fd.
                pass

    def __enter__(self) -> "ExtensionRuntimePackageAuthority":
        with self._lock:
            self._stable_stat()
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def __del__(self) -> None:  # pragma: no cover - deterministic close is preferred
        self.close()


def _runtime_receipt_material(
    manifest: DistributionPackageManifest,
    manifest_bytes: bytes,
    transport_digest: str,
    transport_size: int,
) -> PackageVerificationReceiptV1:
    object_records = tuple(
        MappingProxyType(
            {
                "raw_blob": item.digest,
                "size": item.size,
                "media_type": item.media_type,
                "package_member_name": f"objects/sha256/{item.digest[7:]}",
            }
        )
        for item in sorted(manifest.objects, key=lambda value: value.digest)
    )
    object_index = {
        "schema_version": _RUNTIME_OBJECT_INDEX_SCHEMA,
        "objects": [dict(item) for item in object_records],
    }

    asset_records: list[Mapping[str, object]] = []
    for item in sorted(manifest.assets, key=lambda value: value.asset.qualified_id):
        locator = item.locator
        asset_records.append(
            MappingProxyType(
                {
                    "subject": {
                        "owner": item.asset.owner,
                        "ref": item.asset.asset_id,
                        "kind": item.asset.kind,
                        "version": item.asset.version,
                        "typed_asset_digest": item.asset.digest,
                    },
                    "locator": {
                        "object_raw_blob": locator.object_digest,
                        "member_name": locator.member_path,
                        "member_crc32": locator.member_crc32,
                        "member_compressed_size": locator.member_compressed_size,
                        "compression_method": locator.compression_method,
                    },
                    "uncompressed_size": item.uncompressed_size,
                }
            )
        )
    verified_assets = tuple(asset_records)
    asset_index = {
        "schema_version": _RUNTIME_ASSET_INDEX_SCHEMA,
        "assets": [dict(item) for item in verified_assets],
    }

    package_records = tuple(
        MappingProxyType(
            {
                "owner": item.owner,
                "ref": item.ref,
                "kind": item.kind,
                "version": item.version,
                "raw_blob": item.digest,
            }
        )
        for item in sorted(
            manifest.package_identities,
            key=lambda value: (
                value.owner,
                value.ref,
                value.kind,
                value.version,
                value.digest,
            ),
        )
    )
    package_index = {
        "schema_version": _RUNTIME_PACKAGE_INDEX_SCHEMA,
        "packages": [dict(item) for item in package_records],
    }

    extension_records: list[Mapping[str, object]] = []
    for item in manifest.public_extensions:
        extension_records.append(
            MappingProxyType(
                {
                    "subject": {
                        "owner": item.subject.owner,
                        "ref": item.subject.ref,
                        "kind": item.subject.kind,
                        "version": item.subject.version,
                        "typed_asset_digest": item.subject.digest,
                    },
                    "package_ref": {
                        "owner": item.package_ref.owner,
                        "ref": item.package_ref.ref,
                        "kind": item.package_ref.kind,
                        "version": item.package_ref.version,
                        "raw_blob": item.package_ref.digest,
                    },
                    "api_version": item.api_version,
                    "entry_point_group": item.entry_point_group,
                    "entry_point_name": item.entry_point_name,
                    "capabilities": list(item.capabilities),
                    "placeholders": [
                        {
                            "kind": value.kind,
                            "owner": value.owner,
                            "ref": value.ref,
                        }
                        for value in item.placeholders
                    ],
                }
            )
        )
    public_extensions = tuple(
        sorted(
            extension_records,
            key=lambda value: (
                str(value["api_version"]),
                str(value["entry_point_group"]),
                str(value["entry_point_name"]),
            ),
        )
    )
    extension_index = {
        "schema_version": _RUNTIME_EXTENSION_INDEX_SCHEMA,
        "records": [dict(item) for item in public_extensions],
    }
    verification_profile = {
        "schema_version": _RUNTIME_VERIFICATION_PROFILE_SCHEMA,
        "package_schema": manifest.schema_version,
        "object_digest_algorithm": "sha256",
        "archive_profile": "canonical-posix-ustar/v1",
        "wheel_profile": "okg-distribution-wheel-validation/v1",
        "manifest_profile": "strict-canonical-json/v1",
    }
    receipt = PackageVerificationReceiptV1(
        MappingProxyType(
            _runtime_raw_ref(_digest_bytes(manifest_bytes), len(manifest_bytes))
        ),
        MappingProxyType(_runtime_raw_ref(transport_digest, transport_size)),
        _canonical_digest(object_index),
        _canonical_digest(asset_index),
        _canonical_digest(package_index),
        _canonical_digest(extension_index),
        _canonical_digest(verification_profile),
        manifest.schema_version,
    )
    return receipt


@dataclass(frozen=True, slots=True)
class VerifiedDistributionPackage:
    path: Path
    manifest: DistributionPackageManifest
    _objects: Mapping[str, _ArchiveObject] = field(repr=False, compare=False)
    _runtime_receipt: PackageVerificationReceiptV1 = field(repr=False, compare=False)
    _runtime_fd: int = field(repr=False, compare=False)
    _runtime_identity: _FileIdentity = field(repr=False, compare=False)
    _runtime_size: int = field(repr=False, compare=False)
    _runtime_closed: bool = field(default=False, repr=False, compare=False)
    _runtime_lock: threading.RLock = field(
        default_factory=threading.RLock, repr=False, compare=False
    )

    def verification_receipt(self) -> PackageVerificationReceiptV1:
        """Return the inert, path-free receipt for the retained package bytes.

        This is the package/conformance handoff.  It proves the verified raw
        archive, manifest, and canonical package indexes without exposing the
        retained descriptor or an object reader.  A caller that has explicitly
        retired the retained package authority must verify the archive again.
        """

        with self._runtime_lock:
            if self._runtime_closed or self._runtime_fd < 0:
                raise DistributionPackageError(
                    "package_verification_receipt_unavailable",
                    "package_verification_receipt",
                    "verified package authority is closed",
                )
            try:
                current = os.fstat(self._runtime_fd)
            except OSError as exc:
                raise DistributionPackageError(
                    "package_verification_receipt_unavailable",
                    "package_verification_receipt",
                    "verified package authority is unavailable",
                ) from exc
            if (
                _file_identity(current) != self._runtime_identity
                or current.st_size != self._runtime_size
                or current.st_nlink != 1
                or not stat.S_ISREG(current.st_mode)
            ):
                raise DistributionPackageError(
                    "package_verification_receipt_unavailable",
                    "package_verification_receipt",
                    "retained package identity changed",
                )
            return self._runtime_receipt

    def extension_runtime_authority(self) -> ExtensionRuntimePackageAuthority:
        """Return a fresh retained, path-free reader for verified objects."""

        with self._runtime_lock:
            if self._runtime_closed or self._runtime_fd < 0:
                raise DistributionPackageError(
                    "extension_package_object_mismatch",
                    "package_verification_receipt",
                    "runtime package authority is closed",
                )
            try:
                current = os.fstat(self._runtime_fd)
                if (
                    _file_identity(current) != self._runtime_identity
                    or current.st_size != self._runtime_size
                    or current.st_nlink != 1
                    or not stat.S_ISREG(current.st_mode)
                ):
                    raise OSError("retained archive identity changed")
                duplicate = os.dup(self._runtime_fd)
                os.set_inheritable(duplicate, False)
            except OSError as exc:
                raise DistributionPackageError(
                    "extension_package_object_mismatch",
                    "package_verification_receipt",
                    "runtime package authority is unavailable",
                ) from exc
            return ExtensionRuntimePackageAuthority(
                self._runtime_receipt,
                self._objects,
                duplicate,
                self._runtime_identity,
                self._runtime_size,
            )

    def close_runtime_authority(self) -> None:
        """Release the package-owned retained descriptor; idempotent."""

        with self._runtime_lock:
            if self._runtime_closed:
                return
            fd = self._runtime_fd
            object.__setattr__(self, "_runtime_fd", -1)
            object.__setattr__(self, "_runtime_closed", True)
            try:
                os.close(fd)
            except OSError:
                # Ownership is already retired; retrying could close a reused fd.
                pass

    def __del__(self) -> None:  # pragma: no cover - deterministic close is preferred
        self.close_runtime_authority()

    def read_asset(self, asset_ref: str) -> bytes:
        """Return one selected payload verified from the same retained descriptor."""

        try:
            _require_owner_id(asset_ref, "asset_ref")
        except ValueError as exc:
            raise DistributionPackageError(
                "locator_invalid", asset_ref, "asset_ref must have the form owner:id"
            ) from exc
        path = Path(os.path.abspath(self.path))
        try:
            opened = _open_archive(path)
        except OSError as exc:
            raise DistributionPackageError(
                "path_identity_changed",
                path.as_posix(),
                "archive disappeared or changed before read",
            ) from exc
        with opened as (fd, identity, size):
            verified = _verify_open_archive(fd, identity, size, path)
            selected = next(
                (
                    item
                    for item in verified.manifest.assets
                    if item.asset.qualified_id == asset_ref
                ),
                None,
            )
            if selected is None:
                raise DistributionPackageError(
                    "locator_invalid",
                    asset_ref,
                    "unknown asset reference",
                )
            payload = _read_selected_asset(
                fd,
                verified._objects,
                verified.manifest,
                selected,
            )
            _revalidate_archive(fd, identity, size, path)
            return payload

    def materialize(self, destination: Path) -> Path:
        """Atomically materialize verified objects into a new directory."""

        destination = Path(destination)
        if destination.exists() or destination.is_symlink():
            raise DistributionPackageError(
                "destination_conflict",
                destination.as_posix(),
                "destination already exists",
            )
        _materialize_publish(self, destination)
        return destination


def canonical_bundle_digest(bundle: ProductBundle) -> str:
    """Digest a bundle descriptor without its self-referential digest."""

    if not isinstance(bundle, ProductBundle):
        raise TypeError("bundle must be ProductBundle")
    value = _to_json(bundle)
    assert isinstance(value, dict)
    value.pop("bundle_digest", None)
    return _canonical_digest(value)


def canonical_release_digest(manifest: DistributionPackageManifest) -> str:
    """Digest the canonical manifest with only release_digest omitted."""

    if not isinstance(manifest, DistributionPackageManifest):
        raise TypeError("manifest must be DistributionPackageManifest")
    return _canonical_digest(manifest.to_mapping(omit_release_digest=True))


def _validate_manifest(manifest: DistributionPackageManifest) -> None:
    for name, observed in (
        ("bundles", len(manifest.bundles)),
        ("assets", len(manifest.assets)),
        ("objects", len(manifest.objects)),
        ("package_identities", len(manifest.package_identities)),
        ("public_extensions", len(manifest.public_extensions)),
        ("module_declarations", len(manifest.module_declarations)),
        (
            "placeholders",
            sum(
                len(extension.placeholders) for extension in manifest.public_extensions
            ),
        ),
    ):
        if observed > LIMITS[name]:
            raise _limit(name, observed)
    for obj in manifest.objects:
        if obj.size > LIMITS["object_bytes_each"]:
            raise _limit("object_bytes_each", obj.size, path=obj.digest)
    for item in manifest.assets:
        if item.uncompressed_size > LIMITS["selected_asset_bytes_each"]:
            raise _limit(
                "selected_asset_bytes_each",
                item.uncompressed_size,
                path=item.asset.qualified_id,
            )
    selected_total = sum(item.uncompressed_size for item in manifest.assets)
    if selected_total > LIMITS["selected_asset_bytes_total"]:
        raise _limit("selected_asset_bytes_total", selected_total)
    if (
        canonical_release_digest(manifest)
        != manifest.distribution_release.release_digest
    ):
        raise DistributionPackageError(
            "digest_mismatch",
            "distribution_release.release_digest",
            "release digest mismatch",
        )
    for bundle in manifest.bundles:
        if canonical_bundle_digest(bundle) != bundle.bundle_digest:
            raise DistributionPackageError(
                "digest_mismatch",
                f"bundle:{bundle.owner}:{bundle.bundle_id}",
                "bundle digest mismatch",
            )
    object_total = sum(item.size for item in manifest.objects)
    if object_total > LIMITS["object_bytes_total"]:
        raise _limit("object_bytes_total", object_total)


def installed_okg_version() -> str:
    """Return the running OKG distribution version, or refuse."""

    try:
        return importlib.metadata.version("okg")
    except importlib.metadata.PackageNotFoundError as exc:
        raise DistributionPackageError(
            "okg_version_unknown",
            "okg_compatibility",
            "the running OKG version could not be determined",
        ) from exc


def check_okg_compatibility(
    manifest: DistributionPackageManifest, *, okg_version: str | None = None
) -> None:
    """Refuse a package whose declared OKG window excludes this build.

    A manifest that declares no window (every package v1, and any package v2
    that omits the field) is compatible by declaration and passes.  The
    running build is compared by its base release only; see
    :class:`OkgCompatibility`.
    """

    if not isinstance(manifest, DistributionPackageManifest):
        raise TypeError("manifest must be DistributionPackageManifest")
    declared = manifest.okg_compatibility
    if declared is None:
        return
    observed = okg_version if okg_version is not None else installed_okg_version()
    try:
        supported = declared.supports(observed)
    except ValueError as exc:
        raise DistributionPackageError(
            "okg_version_unknown", "okg_compatibility", str(exc)
        ) from exc
    if not supported:
        raise DistributionPackageError(
            "okg_compatibility_unsupported",
            "okg_compatibility",
            f"OKG {observed} is outside the declared window "
            f"[{declared.min_version}, {declared.max_version_exclusive})",
        )


def build_distribution_package(
    manifest: DistributionPackageManifest,
    sources: Mapping[str, DistributionPackageSource],
    output_directory: Path,
) -> Path:
    """Build one deterministic archive in an existing output directory."""

    if not isinstance(manifest, DistributionPackageManifest):
        raise TypeError("manifest must be DistributionPackageManifest")
    output_directory = Path(output_directory)
    if (
        not output_directory.is_absolute()
        or output_directory.is_symlink()
        or not output_directory.is_dir()
    ):
        raise DistributionPackageError(
            "source_path_unsafe",
            output_directory.as_posix(),
            "output_directory must be an existing real directory",
        )
    _validate_manifest(manifest)
    if not isinstance(sources, Mapping):
        raise TypeError("sources must be a mapping keyed by object digest")
    source_by_object: dict[str, DistributionPackageSource] = {}
    object_sizes: dict[str, int] = {}
    for obj in manifest.objects:
        object_digest = obj.digest
        direct = sources.get(object_digest)
        if isinstance(direct, DistributionPackageSource):
            source_by_object[object_digest] = direct
            object_sizes[object_digest] = obj.size
    for obj in manifest.objects:
        source = source_by_object.get(obj.digest)
        if source is None:
            raise DistributionPackageError(
                "locator_invalid", obj.digest, "no source supplied for object"
            )
    manifest_bytes = _canonical_json(manifest.to_mapping())
    if len(manifest_bytes) > LIMITS["manifest_bytes"]:
        raise _limit("manifest_bytes", len(manifest_bytes), path="manifest.json")
    return _write_archive(
        output_directory,
        manifest_bytes,
        source_by_object,
        object_sizes,
    )


def verify_distribution_package(path: Path) -> VerifiedDistributionPackage:
    """Verify a canonical archive before returning its inert descriptor."""

    path = Path(os.path.abspath(path))
    try:
        opened = _open_archive(path)
    except FileNotFoundError as exc:
        raise DistributionPackageError(
            "path_identity_changed",
            path.as_posix(),
            "archive disappeared before capture",
        ) from exc
    except OSError as exc:
        raise DistributionPackageError(
            "path_identity_changed", path.as_posix(), str(exc)
        ) from exc
    with opened as (fd, identity, size):
        return _verify_open_archive(fd, identity, size, path)


def _verify_open_archive(
    fd: int,
    identity: _FileIdentity,
    size: int,
    path: Path,
) -> VerifiedDistributionPackage:
    """Verify and index one already-open archive without a pathname reopen."""

    if size > LIMITS["archive_bytes"]:
        raise _limit("archive_bytes", size, path=path.as_posix())
    transport = _hash_fd(fd, size, path.as_posix())
    expected = f"{transport[7:]}.okgdist.tar"
    if path.name != expected:
        raise DistributionPackageError(
            "transport_digest_mismatch",
            path.as_posix(),
            "archive filename does not bind transport digest",
        )
    if _archive_boundary_hook is not None:
        _archive_boundary_hook(path.as_posix())
    _revalidate_archive(fd, identity, size, path)
    manifest_bytes, objects = _scan_archive(fd, size, path.as_posix())
    manifest = _parse_manifest(manifest_bytes)
    _validate_manifest(manifest)
    declared = {obj.digest: obj for obj in manifest.objects}
    if set(objects) != set(declared):
        raise DistributionPackageError(
            "manifest_invalid",
            path.as_posix(),
            "archive and manifest object sets differ",
        )
    actual_total = sum(item.size for item in objects.values())
    if actual_total > LIMITS["object_bytes_total"]:
        raise _limit("object_bytes_total", actual_total, path=path.as_posix())
    for digest, obj in declared.items():
        actual = objects[digest]
        if actual.size != obj.size:
            raise DistributionPackageError(
                "digest_mismatch",
                digest,
                "manifest object has the wrong size",
            )
        observed = _hash_archive_range(fd, actual.offset, actual.size, path.as_posix())
        if observed != digest:
            raise DistributionPackageError(
                "digest_mismatch",
                f"objects/sha256/{digest[7:]}",
                "object digest mismatch",
            )
    for asset in manifest.assets:
        _read_selected_asset(fd, objects, manifest, asset, expose=False)
    _revalidate_archive(fd, identity, size, path)
    runtime_receipt = _runtime_receipt_material(
        manifest, manifest_bytes, transport, size
    )
    try:
        retained_fd = os.dup(fd)
        os.set_inheritable(retained_fd, False)
    except OSError as exc:
        raise DistributionPackageError(
            "extension_package_object_mismatch",
            "package_verification_receipt",
            "could not retain verified package authority",
        ) from exc
    return VerifiedDistributionPackage(
        path,
        manifest,
        MappingProxyType(dict(objects)),
        runtime_receipt,
        retained_fd,
        identity,
        size,
    )


# Hooks are intentionally private and no-op.  Race-focused tests can replace
# them to pause at a capture/read or publication boundary without adding a
# runtime callback or an import/execute surface to the package format.
_capture_boundary_hook: Callable[[str], None] | None = None
_archive_boundary_hook: Callable[[str], None] | None = None
_publish_boundary_hook: Callable[[str], None] | None = None


def _digest_bytes(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def _read_chunks(
    fd: int, count: int, *, path: str, maximum: int | None = None
) -> bytes:
    out = bytearray()
    remaining = count
    while remaining:
        take = min(_CHUNK, remaining)
        chunk = os.read(fd, take)
        if not chunk:
            raise DistributionPackageError(
                "archive_noncanonical", path, "truncated payload"
            )
        out.extend(chunk)
        remaining -= len(chunk)
        if maximum is not None and len(out) > maximum:
            raise _limit("selected_asset_bytes_each", len(out), path=path)
    return bytes(out)


def _hash_fd(fd: int, size: int, path: str) -> str:
    os.lseek(fd, 0, os.SEEK_SET)
    digest = hashlib.sha256()
    remaining = size
    while remaining:
        chunk = os.read(fd, min(_CHUNK, remaining))
        if not chunk:
            raise DistributionPackageError(
                "archive_noncanonical", path, "archive changed while hashing"
            )
        digest.update(chunk)
        remaining -= len(chunk)
    return "sha256:" + digest.hexdigest()


def _hash_archive_range(fd: int, offset: int, size: int, path: str) -> str:
    os.lseek(fd, offset, os.SEEK_SET)
    digest = hashlib.sha256()
    remaining = size
    while remaining:
        chunk = os.read(fd, min(_CHUNK, remaining))
        if not chunk:
            raise DistributionPackageError(
                "archive_noncanonical", path, "truncated object"
            )
        digest.update(chunk)
        remaining -= len(chunk)
    return "sha256:" + digest.hexdigest()


def _open_anchor(path: Path, *, directory: bool) -> int:
    if not path.is_absolute():
        raise OSError("path must be absolute")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    if directory:
        flags |= getattr(os, "O_DIRECTORY", 0)
    fd = os.open(path.anchor, flags)
    try:
        for part in path.relative_to(path.anchor).parts:
            next_fd = os.open(
                part,
                flags | (getattr(os, "O_DIRECTORY", 0) if directory else 0),
                dir_fd=fd,
            )
            os.close(fd)
            fd = next_fd
        return fd
    except BaseException:
        os.close(fd)
        raise


@dataclass
class _OpenArchive:
    fd: int
    identity: _FileIdentity
    size: int
    parent_fd: int

    def __enter__(self) -> tuple[int, _FileIdentity, int]:
        return self.fd, self.identity, self.size

    def __exit__(self, *_: object) -> None:
        os.close(self.fd)
        os.close(self.parent_fd)


def _open_archive(path: Path) -> _OpenArchive:
    path = Path(os.path.abspath(path))
    parent_fd = _open_anchor(path.parent, directory=True)
    try:
        fd = os.open(
            path.name,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=parent_fd,
        )
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode) or st.st_nlink != 1:
            raise DistributionPackageError(
                "source_path_unsafe",
                path.as_posix(),
                "archive is not a regular single-link file",
            )
        return _OpenArchive(fd, _file_identity(st), st.st_size, parent_fd)
    except BaseException:
        os.close(parent_fd)
        raise


def _file_identity(st: os.stat_result) -> _FileIdentity:
    return (
        st.st_dev,
        st.st_ino,
        st.st_mode,
        st.st_size,
        st.st_mtime_ns,
        st.st_ctime_ns,
    )


def _file_core_identity(st: os.stat_result) -> tuple[int, int, int, int]:
    return (st.st_dev, st.st_ino, st.st_mode, st.st_size)


def _directory_identity(st: os.stat_result) -> tuple[int, int, int]:
    return (st.st_dev, st.st_ino, st.st_mode)


def _revalidate_directory(fd: int, path: Path, identity: tuple[int, int, int]) -> None:
    """Prove both the retained directory and its visible pathname are stable."""

    current_fd = os.fstat(fd)
    try:
        current_path = os.lstat(path)
    except OSError as exc:
        raise DistributionPackageError(
            "path_identity_changed", path.as_posix(), "directory pathname changed"
        ) from exc
    if (
        _directory_identity(current_fd) != identity
        or _directory_identity(current_path) != identity
        or not stat.S_ISDIR(current_path.st_mode)
    ):
        raise DistributionPackageError(
            "path_identity_changed", path.as_posix(), "directory identity changed"
        )


def _revalidate_archive(
    fd: int, identity: _FileIdentity, size: int, path: Path
) -> None:
    st = os.fstat(fd)
    try:
        visible = os.lstat(path)
    except OSError as exc:
        raise DistributionPackageError(
            "path_identity_changed",
            path.as_posix(),
            "archive pathname changed",
        ) from exc
    if (
        _file_identity(st) != identity
        or st.st_size != size
        or _file_identity(visible) != identity
        or not stat.S_ISREG(visible.st_mode)
    ):
        raise DistributionPackageError(
            "path_identity_changed", path.as_posix(), "archive identity changed"
        )


def _stream_source(
    source: DistributionPackageSource,
    *,
    expected_size: int,
    expected_digest: str,
    write: Callable[[bytes], None],
) -> None:
    """Stream one anchored source while proving its visible name stayed pinned."""

    root_fd = _open_anchor(source.root, directory=True)
    try:
        try:
            components = list(source.path.relative_to(source.root).parts)
        except ValueError as exc:
            raise DistributionPackageError(
                "source_path_unsafe",
                source.path.as_posix(),
                "source is outside root",
            ) from exc
        try:
            fd, chain = _resolve_contained(root_fd, components)
        except FileNotFoundError as exc:
            raise DistributionPackageError(
                "path_identity_changed",
                source.path.as_posix(),
                "source disappeared before capture",
            ) from exc
        except OSError as exc:
            raise DistributionPackageError(
                "path_identity_changed",
                source.path.as_posix(),
                str(exc),
            ) from exc
        try:
            initial = os.fstat(fd)
            if not stat.S_ISREG(initial.st_mode) or initial.st_nlink != 1:
                raise DistributionPackageError(
                    "source_path_unsafe",
                    source.path.as_posix(),
                    "source is not a regular single-link file",
                )
            identity = _file_identity(initial)
            if initial.st_size > LIMITS["object_bytes_each"]:
                raise _limit(
                    "object_bytes_each",
                    initial.st_size,
                    path=source.path.as_posix(),
                )
            if initial.st_size != expected_size:
                raise DistributionPackageError(
                    "digest_mismatch",
                    expected_digest,
                    "source size does not match object descriptor",
                )
            if _capture_boundary_hook is not None:
                _capture_boundary_hook(source.path.as_posix())
            observed = 0
            digest = hashlib.sha256()
            while True:
                chunk = os.read(fd, _CHUNK)
                if not chunk:
                    break
                observed += len(chunk)
                if observed > LIMITS["object_bytes_each"]:
                    raise _limit(
                        "object_bytes_each",
                        observed,
                        path=source.path.as_posix(),
                    )
                digest.update(chunk)
                write(chunk)
            after = os.fstat(fd)
            visible_identity, visible_chain = _visible_source_state(root_fd, components)
            if (
                _file_identity(after) != identity
                or visible_identity != identity
                or chain != visible_chain
            ):
                raise DistributionPackageError(
                    "source_race_detected",
                    source.path.as_posix(),
                    "source path or bytes changed while being captured",
                )
            if (
                observed != expected_size
                or f"sha256:{digest.hexdigest()}" != expected_digest
            ):
                raise DistributionPackageError(
                    "digest_mismatch",
                    expected_digest,
                    "source bytes do not match object descriptor",
                )
        finally:
            os.close(fd)
    finally:
        os.close(root_fd)


def _resolve_contained(
    root_fd: int, components: list[str]
) -> tuple[int, tuple[tuple[str, str], ...]]:
    hops = 0
    chain: list[tuple[str, str]] = []
    while True:
        current = os.dup(root_fd)
        try:
            for index, component in enumerate(components):
                if (
                    not component
                    or component in {".", ".."}
                    or "/" in component
                    or "\\" in component
                    or "\x00" in component
                ):
                    raise DistributionPackageError(
                        "source_path_unsafe",
                        "/".join(components),
                        "unsafe source component",
                    )
                try:
                    next_fd = os.open(
                        component,
                        os.O_RDONLY
                        | getattr(os, "O_CLOEXEC", 0)
                        | getattr(os, "O_NOFOLLOW", 0),
                        dir_fd=current,
                    )
                except OSError as exc:
                    if exc.errno != errno.ELOOP:
                        raise
                    target = os.readlink(component, dir_fd=current)
                    if target.startswith("/") or not target:
                        raise DistributionPackageError(
                            "source_path_unsafe",
                            component,
                            "absolute or empty source link",
                        )
                    hops += 1
                    if hops > LIMITS["source_symlink_hops"]:
                        raise _limit("source_symlink_hops", hops, path=component)
                    chain.append((component, target))
                    prefix = list(components[:index])
                    for part in target.split("/"):
                        if part in {"", "."}:
                            continue
                        if part == "..":
                            if not prefix:
                                raise DistributionPackageError(
                                    "source_path_unsafe",
                                    component,
                                    "source link escapes root",
                                )
                            prefix.pop()
                        else:
                            prefix.append(part)
                    replacement = prefix + components[index + 1 :]
                    components = replacement
                    break
                if index == len(components) - 1:
                    return next_fd, tuple(chain)
                st = os.fstat(next_fd)
                if not stat.S_ISDIR(st.st_mode):
                    os.close(next_fd)
                    raise DistributionPackageError(
                        "source_path_unsafe",
                        component,
                        "source parent is not a directory",
                    )
                os.close(current)
                current = next_fd
            else:
                raise DistributionPackageError(
                    "source_path_unsafe", "/".join(components), "source path is empty"
                )
        finally:
            os.close(current)


def _visible_source_state(
    root_fd: int,
    components: list[str],
) -> tuple[_FileIdentity, tuple[tuple[str, str], ...]]:
    """Re-resolve the visible source name from the retained root descriptor."""

    fd, chain = _resolve_contained(root_fd, list(components))
    try:
        return _file_identity(os.fstat(fd)), chain
    finally:
        os.close(fd)


def _ustar_header(name: str, size: int) -> bytes:
    encoded = name.encode("ascii")
    if len(encoded) > 100:
        raise DistributionPackageError(
            "archive_noncanonical", name, "archive member name is too long"
        )
    header = bytearray(512)
    header[0 : len(encoded)] = encoded
    header[100:108] = b"0000444\0"
    header[108:116] = b"0000000\0"
    header[116:124] = b"0000000\0"
    header[124:136] = f"{size:011o}\0".encode("ascii")
    header[136:148] = b"00000000000\0"
    header[148:156] = b"        "
    header[156] = ord("0")
    header[257:263] = b"ustar\0"
    header[263:265] = b"00"
    checksum = sum(header)
    header[148:156] = f"{checksum:06o}\0 ".encode("ascii")
    return bytes(header)


def _write_archive(
    directory: Path,
    manifest: bytes,
    objects: Mapping[str, DistributionPackageSource],
    object_sizes: Mapping[str, int],
) -> Path:
    try:
        directory_fd = _open_anchor(directory, directory=True)
    except OSError as exc:
        raise DistributionPackageError(
            "path_identity_changed",
            directory.as_posix(),
            "output directory is not a stable real directory",
        ) from exc
    directory_identity = _directory_identity(os.fstat(directory_fd))
    temporary = f".okgdist-{secrets.token_hex(12)}.tmp"
    try:
        fd = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
            0o600,
            dir_fd=directory_fd,
        )
        digest = hashlib.sha256()
        archive_bytes = 0
        try:

            def write(value: bytes) -> None:
                nonlocal archive_bytes
                observed = archive_bytes + len(value)
                if observed > LIMITS["archive_bytes"]:
                    raise _limit("archive_bytes", observed, path=directory.as_posix())
                digest.update(value)
                offset = 0
                while offset < len(value):
                    chunk = value[offset : offset + _CHUNK]
                    written = os.write(fd, chunk)
                    if written <= 0:
                        raise OSError("short archive write")
                    offset += written
                archive_bytes = observed

            def member(name: str, payload: bytes) -> None:
                write(_ustar_header(name, len(payload)))
                for start in range(0, len(payload), _CHUNK):
                    write(payload[start : start + _CHUNK])
                padding = (-len(payload)) % 512
                if padding:
                    write(bytes(padding))

            member("manifest.json", manifest)
            for object_digest in sorted(objects):
                source = objects[object_digest]
                object_size = object_sizes[object_digest]
                write(_ustar_header(f"objects/sha256/{object_digest[7:]}", object_size))
                try:
                    _stream_source(
                        source,
                        expected_size=object_size,
                        expected_digest=object_digest,
                        write=write,
                    )
                except DistributionPackageError:
                    raise
                except (OSError, ValueError) as exc:
                    raise DistributionPackageError(
                        "source_path_unsafe",
                        source.path.as_posix(),
                        str(exc),
                    ) from exc
                padding = (-object_size) % 512
                if padding:
                    write(bytes(padding))
            write(bytes(1024))
            os.fsync(fd)
            temporary_identity = _file_identity(os.fstat(fd))
            temporary_core = _file_core_identity(os.fstat(fd))
        finally:
            os.close(fd)
        transport = "sha256:" + digest.hexdigest()
        final_name = f"{transport[7:]}.okgdist.tar"
        if _publish_boundary_hook is not None:
            _publish_boundary_hook(final_name)
        _revalidate_directory(directory_fd, directory, directory_identity)
        try:
            visible_temporary = os.stat(
                temporary,
                dir_fd=directory_fd,
                follow_symlinks=False,
            )
        except OSError as exc:
            raise DistributionPackageError(
                "path_identity_changed",
                temporary,
                "archive staging path changed before publication",
            ) from exc
        if _file_identity(visible_temporary) != temporary_identity or not stat.S_ISREG(
            visible_temporary.st_mode
        ):
            raise DistributionPackageError(
                "path_identity_changed",
                temporary,
                "archive staging identity changed before publication",
            )
        try:
            os.link(
                temporary, final_name, src_dir_fd=directory_fd, dst_dir_fd=directory_fd
            )
        except FileExistsError as exc:
            # A deterministic rebuild is idempotent.  A same-name archive
            # with different bytes remains a destination conflict; never
            # replace the existing inode.
            try:
                existing_fd = os.open(
                    final_name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory_fd
                )
                try:
                    existing_stat = os.fstat(existing_fd)
                    existing_identity = _file_identity(existing_stat)
                    if (
                        not stat.S_ISREG(existing_stat.st_mode)
                        or existing_stat.st_nlink != 1
                    ):
                        raise DistributionPackageError(
                            "destination_conflict",
                            final_name,
                            "existing archive is not a regular single-link file",
                        )
                    existing_digest = _hash_fd(
                        existing_fd, existing_stat.st_size, final_name
                    )
                    visible_existing = os.stat(
                        final_name,
                        dir_fd=directory_fd,
                        follow_symlinks=False,
                    )
                    if (
                        _file_identity(os.fstat(existing_fd)) != existing_identity
                        or _file_identity(visible_existing) != existing_identity
                    ):
                        raise DistributionPackageError(
                            "path_identity_changed",
                            final_name,
                            "existing archive changed during idempotence check",
                        )
                finally:
                    os.close(existing_fd)
                if existing_digest == transport:
                    os.unlink(temporary, dir_fd=directory_fd)
                    os.fsync(directory_fd)
                    _revalidate_directory(directory_fd, directory, directory_identity)
                    return directory / final_name
            except (OSError, DistributionPackageError):
                # A failed identity/digest probe cannot prove idempotence, so
                # preserve the destination-conflict refusal below.
                pass
            raise DistributionPackageError(
                "destination_conflict", final_name, "archive destination already exists"
            ) from exc
        visible_final = os.stat(final_name, dir_fd=directory_fd, follow_symlinks=False)
        if _file_core_identity(visible_final) != temporary_core or not stat.S_ISREG(
            visible_final.st_mode
        ):
            raise DistributionPackageError(
                "path_identity_changed",
                final_name,
                "published archive identity differs from staged bytes",
            )
        os.unlink(temporary, dir_fd=directory_fd)
        os.fsync(directory_fd)
        _revalidate_directory(directory_fd, directory, directory_identity)
        return directory / final_name
    except BaseException:
        try:
            os.unlink(temporary, dir_fd=directory_fd)
        except FileNotFoundError:
            # The staging name may already have been published or removed.
            pass
        raise
    finally:
        os.close(directory_fd)


def _read_exact(fd: int, count: int, path: str) -> bytes:
    chunks: list[bytes] = []
    remaining = count
    while remaining:
        chunk = os.read(fd, min(_CHUNK, remaining))
        if not chunk:
            raise DistributionPackageError(
                "archive_noncanonical", path, "truncated archive"
            )
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _parse_octal(field: bytes, path: str) -> int:
    if not field or any(byte not in b" 01234567\0" for byte in field):
        raise DistributionPackageError(
            "archive_noncanonical", path, "non-canonical numeric field"
        )
    stripped = field.rstrip(b" \0")
    if not stripped:
        return 0
    try:
        return int(stripped, 8)
    except ValueError as exc:
        raise DistributionPackageError(
            "archive_noncanonical", path, "invalid octal field"
        ) from exc


def _scan_archive(
    fd: int, size: int, path: str
) -> tuple[bytes, dict[str, _ArchiveObject]]:
    os.lseek(fd, 0, os.SEEK_SET)
    manifest: bytes | None = None
    objects: dict[str, _ArchiveObject] = {}
    previous = ""
    members = 0
    offset = 0
    while True:
        header = _read_exact(fd, 512, path)
        offset += 512
        if header == bytes(512):
            if _read_exact(fd, 512, path) != bytes(512):
                raise DistributionPackageError(
                    "archive_noncanonical",
                    path,
                    "archive terminator is not two zero blocks",
                )
            offset += 512
            if os.read(fd, 1):
                raise DistributionPackageError(
                    "archive_noncanonical",
                    path,
                    "trailing bytes after USTAR terminator",
                )
            break
        if (
            header[257:263] != b"ustar\0"
            or header[263:265] != b"00"
            or header[156:157] != b"0"
        ):
            raise DistributionPackageError(
                "archive_noncanonical", path, "archive header is not canonical USTAR"
            )
        if any(header[500:512]):
            raise DistributionPackageError(
                "archive_noncanonical", path, "unsupported trailing USTAR header bytes"
            )
        name_raw = header[:100].split(b"\0", 1)[0]
        try:
            name = name_raw.decode("ascii")
        except UnicodeDecodeError as exc:
            raise DistributionPackageError(
                "archive_noncanonical", path, "member name is not ASCII"
            ) from exc
        if not name or name <= previous or len(name.encode("ascii")) > 100:
            raise DistributionPackageError(
                "archive_noncanonical", name or path, "members are not strictly sorted"
            )
        previous = name
        if header[:100] != name_raw + bytes(100 - len(name_raw)):
            raise DistributionPackageError(
                "archive_noncanonical",
                name,
                "member name field is not canonically NUL padded",
            )
        if name != "manifest.json" and not re.fullmatch(
            r"objects/sha256/[0-9a-f]{64}", name
        ):
            raise DistributionPackageError(
                "archive_noncanonical", name, "unexpected archive member"
            )
        expected_checksum = _parse_octal(header[148:156], name)
        if header[148:156] != f"{expected_checksum:06o}\0 ".encode("ascii"):
            raise DistributionPackageError(
                "archive_noncanonical", name, "non-canonical header checksum field"
            )
        check_header = bytearray(header)
        check_header[148:156] = b"        "
        if sum(check_header) != expected_checksum:
            raise DistributionPackageError(
                "archive_noncanonical", name, "header checksum mismatch"
            )
        if (
            header[100:108] != b"0000444\0"
            or header[108:116] != b"0000000\0"
            or header[116:124] != b"0000000\0"
            or header[136:148] != b"00000000000\0"
        ):
            raise DistributionPackageError(
                "archive_noncanonical", name, "noncanonical USTAR metadata"
            )
        if any(any(header[start:end]) for start, end in ((157, 257), (265, 500))):
            raise DistributionPackageError(
                "archive_noncanonical",
                name,
                "links, users, or extensions are forbidden",
            )
        payload_size = _parse_octal(header[124:136], name)
        if header[124:136] != f"{payload_size:011o}\0".encode("ascii"):
            raise DistributionPackageError(
                "archive_noncanonical",
                name,
                "member size field is not canonical octal",
            )
        if name == "manifest.json":
            if manifest is not None or payload_size > LIMITS["manifest_bytes"]:
                raise (
                    _limit("manifest_bytes", payload_size, path=name)
                    if payload_size > LIMITS["manifest_bytes"]
                    else _refuse("archive_noncanonical", name, "duplicate manifest")
                )
            manifest = _read_exact(fd, payload_size, name)
        else:
            if payload_size > LIMITS["object_bytes_each"]:
                raise _limit("object_bytes_each", payload_size, path=name)
            digest = "sha256:" + name.rsplit("/", 1)[1]
            if digest in objects:
                raise _refuse("archive_noncanonical", name, "duplicate object")
            objects[digest] = _ArchiveObject(offset, payload_size)
            _discard(fd, payload_size, name)
        offset += payload_size
        padding = (-payload_size) % 512
        if padding:
            if _read_exact(fd, padding, name) != bytes(padding):
                raise DistributionPackageError(
                    "archive_noncanonical",
                    name,
                    "member padding is not zero filled",
                )
            offset += padding
        members += 1
        if members > LIMITS["archive_members"]:
            raise _limit("archive_members", members, path=path)
    if manifest is None:
        raise _refuse("archive_noncanonical", path, "manifest.json is missing")
    if offset != size:
        raise DistributionPackageError(
            "archive_noncanonical",
            path,
            "archive size does not match canonical terminator",
        )
    if len(objects) > LIMITS["objects"]:
        raise _limit("objects", len(objects), path=path)
    return manifest, objects


def _discard(fd: int, count: int, path: str) -> None:
    remaining = count
    while remaining:
        chunk = os.read(fd, min(_CHUNK, remaining))
        if not chunk:
            raise DistributionPackageError(
                "archive_noncanonical", path, "truncated member"
            )
        remaining -= len(chunk)


def _parse_manifest(payload: bytes) -> DistributionPackageManifest:
    if len(payload) > LIMITS["manifest_bytes"]:
        raise _limit("manifest_bytes", len(payload), path="manifest.json")
    try:
        value = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
        _nfc(value)
        if _canonical_json(value) != payload:
            raise ValueError("manifest bytes are not canonical JSON")
        _prevalidate_manifest_resources(value)
        return DistributionPackageManifest.model_validate(value)
    except DistributionPackageError:
        raise
    except (UnicodeDecodeError, ValueError, TypeError, json.JSONDecodeError) as exc:
        raise DistributionPackageError(
            "manifest_invalid", "manifest.json", str(exc)
        ) from exc


def _prevalidate_manifest_resources(value: object) -> None:
    """Emit stable resource codes before model parsing can collapse them."""

    if not isinstance(value, Mapping):
        return
    sequence_limits = {
        "bundles": "bundles",
        "assets": "assets",
        "objects": "objects",
        "package_identities": "package_identities",
        "public_extensions": "public_extensions",
        "module_declarations": "module_declarations",
    }
    for field_name, limit in sequence_limits.items():
        items = value.get(field_name)
        if isinstance(items, list) and len(items) > LIMITS[limit]:
            raise _limit(limit, len(items), path=f"manifest.json:{field_name}")
    objects = value.get("objects")
    if isinstance(objects, list):
        total = 0
        for item in objects:
            if not isinstance(item, Mapping):
                continue
            size = item.get("size")
            if isinstance(size, int) and not isinstance(size, bool) and size >= 0:
                if size > LIMITS["object_bytes_each"]:
                    raise _limit(
                        "object_bytes_each",
                        size,
                        path=str(item.get("digest", "manifest.json:objects")),
                    )
                total += size
        if total > LIMITS["object_bytes_total"]:
            raise _limit("object_bytes_total", total, path="manifest.json:objects")
    assets = value.get("assets")
    if isinstance(assets, list):
        selected_total = 0
        for item in assets:
            if not isinstance(item, Mapping):
                continue
            size = item.get("uncompressed_size")
            if isinstance(size, int) and not isinstance(size, bool) and size >= 0:
                logical = item.get("asset")
                logical_path = "manifest.json:assets"
                if isinstance(logical, Mapping):
                    owner = logical.get("owner")
                    ident = logical.get("id")
                    if isinstance(owner, str) and isinstance(ident, str):
                        logical_path = f"{owner}:{ident}"
                if size > LIMITS["selected_asset_bytes_each"]:
                    raise _limit(
                        "selected_asset_bytes_each",
                        size,
                        path=logical_path,
                    )
                selected_total += size
        if selected_total > LIMITS["selected_asset_bytes_total"]:
            raise _limit(
                "selected_asset_bytes_total",
                selected_total,
                path="manifest.json:assets",
            )


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_constant(value: str) -> object:
    raise ValueError(f"non-finite JSON number: {value}")


def _read_archive_bytes(
    fd: int, obj: _ArchiveObject, path: str, *, maximum: int
) -> bytes:
    if obj.size > maximum:
        raise _limit("selected_asset_bytes_each", obj.size, path=path)
    os.lseek(fd, obj.offset, os.SEEK_SET)
    return _read_exact(fd, obj.size, path)


def _read_selected_asset(
    fd: int,
    objects: Mapping[str, _ArchiveObject],
    manifest: DistributionPackageManifest,
    item: PackagedAsset,
    *,
    expose: bool = True,
) -> bytes:
    object_entry = objects.get(item.locator.object_digest)
    if object_entry is None:
        raise DistributionPackageError(
            "locator_invalid", item.asset.qualified_id, "object is absent"
        )
    if item.locator.member_path is None:
        payload = _read_archive_bytes(
            fd,
            object_entry,
            item.asset.qualified_id,
            maximum=LIMITS["selected_asset_bytes_each"],
        )
        if (
            len(payload) != item.uncompressed_size
            or _digest_bytes(payload) != item.asset.digest
        ):
            raise DistributionPackageError(
                "digest_mismatch",
                item.asset.qualified_id,
                "selected object does not match asset",
            )
        return payload if expose else b""
    with tempfile.SpooledTemporaryFile(max_size=8 * 1024 * 1024, mode="w+b") as wheel:
        os.lseek(fd, object_entry.offset, os.SEEK_SET)
        remaining = object_entry.size
        while remaining:
            chunk = os.read(fd, min(_CHUNK, remaining))
            if not chunk:
                raise DistributionPackageError(
                    "archive_noncanonical",
                    item.asset.qualified_id,
                    "truncated wheel object",
                )
            wheel.write(chunk)
            remaining -= len(chunk)
        wheel.flush()
        wheel.seek(0)
        return _verify_wheel_member(
            cast(BinaryIO, wheel),
            object_entry.size,
            item,
            expose=expose,
        )


def _normalized_zip_name(name: str) -> str:
    return _safe_member_path(name, field="zip member name")


def _validate_zip_container(payload: BinaryIO, size: int, logical_path: str) -> None:
    """Refuse multi-disk, ZIP64, commented, or structurally ambiguous wheels."""

    if size < 22:
        raise DistributionPackageError(
            "zip_integrity_failed",
            logical_path,
            "wheel is shorter than the end-of-central-directory record",
        )
    payload.seek(size - 22)
    tail = payload.read(22)
    if len(tail) != 22 or tail[:4] != b"PK\x05\x06":
        raise DistributionPackageError(
            "zip_member_unsafe",
            logical_path,
            "wheel must end with one un-commented standard ZIP directory",
        )
    (
        _signature,
        disk_number,
        central_disk,
        disk_entries,
        total_entries,
        central_size,
        central_offset,
        comment_length,
    ) = struct.unpack("<4s4H2IH", tail)
    if (
        disk_number != 0
        or central_disk != 0
        or disk_entries != total_entries
        or comment_length != 0
    ):
        raise DistributionPackageError(
            "zip_member_unsafe",
            logical_path,
            "multi-disk and commented wheels are unsupported",
        )
    if (
        total_entries == 0xFFFF
        or central_size == 0xFFFFFFFF
        or central_offset == 0xFFFFFFFF
        or b"PK\x06\x06" in tail
        or b"PK\x06\x07" in tail
    ):
        raise DistributionPackageError(
            "zip_member_unsafe",
            logical_path,
            "ZIP64 wheels are unsupported by package v1",
        )
    if total_entries > LIMITS["wheel_entries"]:
        raise _limit(
            "wheel_entries",
            total_entries,
            path=logical_path,
            code="zip_resource_limit",
        )
    if central_size > LIMITS["wheel_central_directory_bytes"]:
        raise _limit(
            "wheel_central_directory_bytes",
            central_size,
            path=logical_path,
            code="zip_resource_limit",
        )
    if central_offset + central_size != size - 22:
        raise DistributionPackageError(
            "zip_integrity_failed",
            logical_path,
            "central-directory bounds do not match the wheel bytes",
        )


def _verified_wheel_inventory(
    payload: BinaryIO, payload_size: int, logical_path: str,
) -> tuple[zipfile.ZipFile, dict[str, zipfile.ZipInfo], dict[str, tuple[int, str, int]]]:
    """Validate all members; caller closes the returned ZIP reader."""
    if payload_size > LIMITS["object_bytes_each"]:
        raise _limit("object_bytes_each", payload_size, path=logical_path)
    _validate_zip_container(payload, payload_size, logical_path)
    payload.seek(0)
    try:
        zf = zipfile.ZipFile(payload, "r")
    except (OSError, ValueError, zipfile.BadZipFile) as exc:
        raise DistributionPackageError(
            "zip_integrity_failed", logical_path, str(exc)
        ) from exc
    try:
        if zf.comment or len(zf.infolist()) > LIMITS["wheel_entries"]:
            raise _limit("wheel_entries", len(zf.infolist()), code="zip_resource_limit")
        infos: list[zipfile.ZipInfo] = []
        names: dict[str, zipfile.ZipInfo] = {}
        permission_only_members: list[str] = []
        central_bytes = 0
        compressed_total = 0
        for info in zf.infolist():
            try:
                normalized = _normalized_zip_name(info.filename)
            except ValueError as exc:
                raise DistributionPackageError(
                    "zip_member_unsafe", info.filename, str(exc)
                ) from exc
            if normalized in names or normalized != info.filename:
                raise DistributionPackageError(
                    "zip_member_unsafe",
                    normalized,
                    "duplicate or non-normalized ZIP name",
                )
            names[normalized] = info
            infos.append(info)
            compressed_total += info.compress_size
            central_bytes += (
                46
                + len(info.filename.encode("utf-8"))
                + len(info.extra)
                + len(info.comment)
            )
            if central_bytes > LIMITS["wheel_central_directory_bytes"]:
                raise _limit(
                    "wheel_central_directory_bytes",
                    central_bytes,
                    code="zip_resource_limit",
                )
            if (
                info.is_dir()
                or info.flag_bits & 0x1
                or info.flag_bits & 0x8
                or info.flag_bits & ~0x800
            ):
                raise DistributionPackageError(
                    "zip_member_unsafe",
                    normalized,
                    "directory, encrypted, descriptor, or unsupported ZIP flags",
                )
            if info.compress_type not in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED):
                raise DistributionPackageError(
                    "zip_member_unsafe", normalized, "unsupported compression"
                )
            if info.extra:
                raise DistributionPackageError(
                    "zip_member_unsafe", normalized, "unsupported ZIP metadata"
                )
            mode = (info.external_attr >> 16) & 0xFFFF
            # Hatchling emits permission-only modes for generated wheel metadata.
            # This exception never authorizes package code/assets or other Unix types.
            parts = normalized.split("/")
            generated_metadata = (
                info.create_system == 3 and mode == 0o644
                and (info.external_attr & 0xFFFF) == 0
                and len(parts) == 2 and parts[0].endswith(".dist-info")
                and parts[1] in {"METADATA", "WHEEL", "entry_points.txt", "RECORD"}
            )
            if generated_metadata:
                permission_only_members.append(normalized)
            if not stat.S_ISREG(mode) and not generated_metadata:
                raise DistributionPackageError(
                    "zip_member_unsafe",
                    normalized,
                    "missing regular-file metadata, symlink, or special ZIP member",
                )
            if info.file_size > LIMITS["wheel_member_expanded_bytes"]:
                raise _limit(
                    "wheel_member_expanded_bytes",
                    info.file_size,
                    path=normalized,
                    code="zip_resource_limit",
                )
            bound = min(
                LIMITS["wheel_member_expanded_bytes"],
                info.compress_size * LIMITS["wheel_selected_expansion_ratio"]
                + LIMITS["wheel_expansion_additive_bytes_each"],
            )
            if info.file_size > bound or (info.compress_size == 0 and info.file_size):
                raise _limit(
                    "wheel_member_expanded_bytes",
                    info.file_size,
                    path=normalized,
                    code="zip_resource_limit",
                )
            data_offset = _check_local_header(payload, payload_size, info, normalized)
            _verify_zip_member_envelope(
                payload,
                payload_size,
                info,
                normalized,
                data_offset,
            )
        required = [name for name in names if name.endswith(".dist-info/WHEEL")]
        metadata = [name for name in names if name.endswith(".dist-info/METADATA")]
        records = [name for name in names if name.endswith(".dist-info/RECORD")]
        if len(required) != 1 or len(metadata) != 1 or len(records) != 1:
            raise DistributionPackageError(
                "zip_integrity_failed",
                logical_path,
                "wheel metadata members are incomplete",
            )
        metadata_root = required[0].split("/")[0]
        if (any(name.rsplit("/", 1)[0] != metadata_root for name in [*required, *metadata, *records])
                or any(name.rsplit("/", 1)[0] != metadata_root for name in permission_only_members)):
            raise DistributionPackageError("zip_member_unsafe", logical_path, "generated metadata has a different dist-info identity")
        expanded_total = 0
        observations: dict[str, tuple[int, str, int]] = {}
        record_rows = _read_zip_member(
            zf,
            names[records[0]],
            records[0],
            limit=LIMITS["wheel_member_expanded_bytes"],
        )
        try:
            rows = list(
                csv.reader(
                    io.StringIO(record_rows.decode("utf-8", errors="strict")),
                    strict=True,
                )
            )
        except (UnicodeDecodeError, csv.Error) as exc:
            raise DistributionPackageError(
                "zip_integrity_failed", records[0], "malformed RECORD"
            ) from exc
        if len(rows) != len(infos):
            raise DistributionPackageError(
                "zip_integrity_failed",
                records[0],
                "RECORD does not list every ZIP entry",
            )
        seen_record: set[str] = set()
        for row in rows:
            if len(row) != 3:
                raise DistributionPackageError(
                    "zip_integrity_failed", records[0], "malformed RECORD row"
                )
            try:
                row_name = _normalized_zip_name(row[0])
            except ValueError as exc:
                raise DistributionPackageError(
                    "zip_integrity_failed", records[0], "unsafe RECORD path"
                ) from exc
            if row_name in seen_record or row_name not in names:
                raise DistributionPackageError(
                    "zip_integrity_failed",
                    records[0],
                    "RECORD has duplicate or unknown member",
                )
            seen_record.add(row_name)
            info = names[row_name]
            value = _read_zip_member(
                zf, info, row_name, limit=LIMITS["wheel_member_expanded_bytes"]
            )
            expanded_total += len(value)
            if expanded_total > LIMITS["wheel_expanded_bytes_total"]:
                raise _limit(
                    "wheel_expanded_bytes_total",
                    expanded_total,
                    path=row_name,
                    code="zip_resource_limit",
                )
            aggregate_bound = min(
                LIMITS["wheel_expanded_bytes_total"],
                compressed_total * LIMITS["wheel_selected_expansion_ratio"]
                + LIMITS["wheel_expansion_additive_bytes_total"],
            )
            if expanded_total > aggregate_bound:
                raise _limit(
                    "wheel_expanded_bytes_total",
                    expanded_total,
                    path=row_name,
                    code="zip_resource_limit",
                )
            crc = __import__("zlib").crc32(value) & 0xFFFFFFFF
            digest = (
                base64.urlsafe_b64encode(hashlib.sha256(value).digest())
                .rstrip(b"=")
                .decode("ascii")
            )
            observations[row_name] = (crc, digest, len(value))
            if row_name != records[0] and (
                not row[1].startswith("sha256=")
                or row[1][7:] != digest
                or row[2] != str(len(value))
            ):
                raise DistributionPackageError(
                    "zip_integrity_failed", row_name, "RECORD hash or size mismatch"
                )
            if row_name == records[0] and (row[1] or row[2]):
                raise DistributionPackageError(
                    "zip_integrity_failed",
                    row_name,
                    "RECORD self-row must omit hash and size",
                )
        if seen_record != set(names):
            raise DistributionPackageError(
                "zip_integrity_failed", records[0], "RECORD member coverage mismatch"
            )
        return zf, names, observations
    except BaseException:
        zf.close()
        raise


def _verify_wheel_member(
    payload: BinaryIO,
    payload_size: int,
    item: PackagedAsset,
    *,
    expose: bool,
) -> bytes:
    if payload_size > LIMITS["object_bytes_each"]:
        raise _limit("object_bytes_each", payload_size, path=item.locator.object_digest)
    zf, names, observations = _verified_wheel_inventory(
        payload, payload_size, item.locator.member_path or "",
    )
    try:
        selected_name = item.locator.member_path
        assert selected_name is not None
        info = names.get(selected_name)
        if info is None:
            raise DistributionPackageError(
                "locator_invalid", selected_name, "selected wheel member is absent"
            )
        crc, _, size = observations[selected_name]
        expected_method = {
            "stored": zipfile.ZIP_STORED,
            "deflated": zipfile.ZIP_DEFLATED,
        }[item.locator.compression_method or "stored"]
        if (
            info.CRC != crc
            or info.compress_size != item.locator.member_compressed_size
            or info.CRC != int(item.locator.member_crc32 or "0", 16)
            or info.compress_type != expected_method
            or size != item.uncompressed_size
        ):
            raise DistributionPackageError(
                "zip_integrity_failed",
                selected_name,
                "selected member metadata mismatch",
            )
        selected = _read_zip_member(
            zf, info, selected_name, limit=item.uncompressed_size
        )
        if _digest_bytes(selected) != item.asset.digest:
            raise DistributionPackageError(
                "digest_mismatch", selected_name, "selected member digest mismatch"
            )
        return selected if expose else b""
    finally:
        zf.close()


def _check_local_header(
    payload: BinaryIO,
    payload_size: int,
    info: zipfile.ZipInfo,
    name: str,
) -> int:
    start = info.header_offset
    if start < 0 or start + 30 > payload_size:
        raise DistributionPackageError(
            "zip_integrity_failed", name, "local ZIP header is missing"
        )
    payload.seek(start)
    header = payload.read(30)
    if len(header) != 30 or header[:4] != b"PK\x03\x04":
        raise DistributionPackageError(
            "zip_integrity_failed", name, "local ZIP header is missing"
        )
    fields = struct.unpack("<4s5H3I2H", header)
    (
        _,
        _,
        flags,
        method,
        _mod_time,
        _mod_date,
        crc,
        compressed,
        expanded,
        name_len,
        extra_len,
    ) = fields
    if start + 30 + name_len + extra_len + compressed > payload_size:
        raise DistributionPackageError(
            "zip_integrity_failed",
            name,
            "local ZIP member exceeds the wheel bounds",
        )
    raw_name = payload.read(name_len)
    raw_extra = payload.read(extra_len)
    encoding = "utf-8" if flags & 0x800 else "cp437"
    try:
        decoded_name = raw_name.decode(encoding, errors="strict")
    except UnicodeDecodeError as exc:
        raise DistributionPackageError(
            "zip_integrity_failed", name, "local ZIP name is not decodable"
        ) from exc
    if decoded_name != name or raw_extra:
        raise DistributionPackageError(
            "zip_integrity_failed", name, "local ZIP name or metadata mismatch"
        )
    if (
        flags != info.flag_bits
        or method != info.compress_type
        or crc != info.CRC
        or compressed != info.compress_size
        or expanded != info.file_size
    ):
        raise DistributionPackageError(
            "zip_integrity_failed", name, "local and central ZIP headers disagree"
        )
    return start + 30 + name_len + extra_len


def _verify_zip_member_envelope(
    payload: BinaryIO,
    payload_size: int,
    info: zipfile.ZipInfo,
    name: str,
    data_offset: int,
) -> None:
    """Prove one member consumes exactly its declared compressed byte range."""

    if data_offset + info.compress_size > payload_size:
        raise DistributionPackageError(
            "zip_integrity_failed", name, "compressed member exceeds wheel bounds"
        )
    if info.compress_type == zipfile.ZIP_STORED:
        if info.compress_size != info.file_size:
            raise DistributionPackageError(
                "zip_integrity_failed",
                name,
                "stored member compressed and expanded sizes differ",
            )
        return
    if info.compress_type != zipfile.ZIP_DEFLATED:
        raise DistributionPackageError(
            "zip_member_unsafe", name, "unsupported compression"
        )
    payload.seek(data_offset)
    decompressor = zlib.decompressobj(-zlib.MAX_WBITS)
    remaining = info.compress_size
    observed = 0
    while remaining:
        compressed = payload.read(min(_CHUNK, remaining))
        if not compressed:
            raise DistributionPackageError(
                "zip_integrity_failed", name, "truncated compressed member"
            )
        remaining -= len(compressed)
        pending = compressed
        while pending:
            allowance = min(_CHUNK, info.file_size - observed + 1)
            if allowance <= 0:
                raise DistributionPackageError(
                    "zip_integrity_failed",
                    name,
                    "deflate output exceeds the declared member size",
                )
            try:
                expanded = decompressor.decompress(pending, allowance)
            except zlib.error as exc:
                raise DistributionPackageError(
                    "zip_integrity_failed", name, "malformed deflate stream"
                ) from exc
            observed += len(expanded)
            if observed > info.file_size:
                raise DistributionPackageError(
                    "zip_integrity_failed",
                    name,
                    "deflate output exceeds the declared member size",
                )
            if decompressor.unused_data:
                raise DistributionPackageError(
                    "zip_integrity_failed",
                    name,
                    "trailing compressed bytes follow the deflate end marker",
                )
            next_pending = decompressor.unconsumed_tail
            if decompressor.eof and (next_pending or remaining):
                raise DistributionPackageError(
                    "zip_integrity_failed",
                    name,
                    "deflate stream ends before the declared compressed range",
                )
            if next_pending == pending and not expanded:
                raise DistributionPackageError(
                    "zip_integrity_failed", name, "deflate decoder made no progress"
                )
            pending = next_pending
    if not decompressor.eof or observed != info.file_size:
        raise DistributionPackageError(
            "zip_integrity_failed",
            name,
            "deflate stream has no exact end-of-member",
        )


def _read_zip_member(
    zf: zipfile.ZipFile, info: zipfile.ZipInfo, name: str, *, limit: int
) -> bytes:
    if info.file_size > limit:
        raise _limit(
            "wheel_member_expanded_bytes",
            info.file_size,
            path=name,
            code="zip_resource_limit",
        )
    output = bytearray()
    try:
        with zf.open(info, "r") as stream:
            while True:
                chunk = stream.read(_CHUNK)
                if not chunk:
                    break
                if len(output) + len(chunk) > limit:
                    raise _limit(
                        "wheel_member_expanded_bytes",
                        len(output) + len(chunk),
                        path=name,
                        code="zip_resource_limit",
                    )
                output.extend(chunk)
    except DistributionPackageError:
        raise
    except (
        OSError,
        RuntimeError,
        ValueError,
        zlib.error,
        zipfile.BadZipFile,
        zipfile.LargeZipFile,
    ) as exc:
        raise DistributionPackageError("zip_integrity_failed", name, str(exc)) from exc
    if len(output) != info.file_size:
        raise DistributionPackageError(
            "zip_integrity_failed", name, "ZIP member size mismatch"
        )
    return bytes(output)


def _materialize_publish(
    package: VerifiedDistributionPackage, destination: Path
) -> None:
    destination = Path(os.path.abspath(destination))
    archive_path = Path(os.path.abspath(package.path))
    try:
        parent_fd = _open_anchor(destination.parent, directory=True)
    except OSError as exc:
        raise DistributionPackageError(
            "path_identity_changed",
            destination.parent.as_posix(),
            "materialization parent is not a stable real directory",
        ) from exc
    parent_identity = _directory_identity(os.fstat(parent_fd))
    temporary = f".{destination.name}.okgdist-{secrets.token_hex(10)}.tmp"
    temp_fd: int | None = None
    objects_fd: int | None = None
    digest_fd: int | None = None
    published = False
    completed = False
    object_names: list[str] = []
    try:
        os.mkdir(temporary, 0o700, dir_fd=parent_fd)
        directory_flags = (
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        temp_fd = os.open(temporary, directory_flags, dir_fd=parent_fd)
        os.mkdir("objects", 0o700, dir_fd=temp_fd)
        objects_fd = os.open("objects", directory_flags, dir_fd=temp_fd)
        os.mkdir("sha256", 0o700, dir_fd=objects_fd)
        digest_fd = os.open("sha256", directory_flags, dir_fd=objects_fd)
        try:
            opened = _open_archive(archive_path)
        except OSError as exc:
            raise DistributionPackageError(
                "path_identity_changed",
                archive_path.as_posix(),
                "archive disappeared before materialization",
            ) from exc
        with opened as (fd, identity, size):
            verified = _verify_open_archive(fd, identity, size, archive_path)
            manifest_bytes = _canonical_json(verified.manifest.to_mapping())
            _write_materialized_file_at(temp_fd, "manifest.json", manifest_bytes)
            for object_digest, archive_object in sorted(verified._objects.items()):
                name = object_digest[7:]
                object_names.append(name)
                _copy_archive_object_at(
                    fd,
                    archive_object,
                    digest_fd,
                    name,
                    object_digest,
                )
            _revalidate_archive(fd, identity, size, archive_path)
        for directory_fd in (digest_fd, objects_fd, temp_fd):
            os.fchmod(directory_fd, 0o555)
            os.fsync(directory_fd)
        staging_identity = _directory_identity(os.fstat(temp_fd))
        if _publish_boundary_hook is not None:
            _publish_boundary_hook(destination.name)
        _revalidate_directory(parent_fd, destination.parent, parent_identity)
        try:
            visible_staging = os.stat(
                temporary,
                dir_fd=parent_fd,
                follow_symlinks=False,
            )
        except OSError as exc:
            raise DistributionPackageError(
                "path_identity_changed",
                destination.as_posix(),
                "materialization staging path changed before publication",
            ) from exc
        if (
            _directory_identity(os.fstat(temp_fd)) != staging_identity
            or _directory_identity(visible_staging) != staging_identity
            or not stat.S_ISDIR(visible_staging.st_mode)
        ):
            raise DistributionPackageError(
                "path_identity_changed",
                destination.as_posix(),
                "materialization staging identity changed before publication",
            )
        _rename_noreplace(
            parent_fd, temporary, parent_fd, destination.name, destination
        )
        published = True
        visible_destination = os.stat(
            destination.name,
            dir_fd=parent_fd,
            follow_symlinks=False,
        )
        if _directory_identity(visible_destination) != staging_identity:
            raise DistributionPackageError(
                "path_identity_changed",
                destination.as_posix(),
                "published materialization identity differs from staging",
            )
        os.fsync(parent_fd)
        _revalidate_directory(parent_fd, destination.parent, parent_identity)
        completed = True
    finally:
        if not completed and temp_fd is not None:
            _remove_private_tree_at(
                parent_fd,
                destination.name if published else temporary,
                temp_fd,
                objects_fd,
                digest_fd,
                object_names,
            )
        if digest_fd is not None:
            os.close(digest_fd)
        if objects_fd is not None:
            os.close(objects_fd)
        if temp_fd is not None:
            os.close(temp_fd)
        os.close(parent_fd)


def _rename_noreplace(
    old_parent_fd: int,
    old_name: str,
    new_parent_fd: int,
    new_name: str,
    logical_destination: Path,
) -> None:
    """Publish a private directory without ever replacing an existing name."""

    # macOS (the supported local operator host) exposes renameatx_np with an
    # exclusive flag.  Linux CI exposes renameat2; use the native primitive
    # rather than a check-then-rename sequence that can overwrite a race.
    if sys_platform := getattr(os, "uname", lambda: None)():
        system = sys_platform.sysname
    else:  # pragma: no cover - defensive for unusual Python ports
        system = ""
    libc = ctypes.CDLL(None, use_errno=True)
    if system == "Darwin" and hasattr(libc, "renameatx_np"):
        function = libc.renameatx_np
        function.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        function.restype = ctypes.c_int
        result = function(
            old_parent_fd,
            old_name.encode(),
            new_parent_fd,
            new_name.encode(),
            0x00000004,
        )
        if result == 0:
            return
        error = ctypes.get_errno()
        if error == errno.EEXIST:
            raise DistributionPackageError(
                "destination_conflict",
                logical_destination.as_posix(),
                "destination appeared during publication",
            )
        raise DistributionPackageError(
            "path_identity_changed", logical_destination.as_posix(), os.strerror(error)
        )
    if system == "Linux" and hasattr(libc, "syscall"):
        machine = os.uname().machine
        syscall_number = 276 if machine in {"aarch64", "arm64"} else 316
        result = libc.syscall(
            syscall_number,
            ctypes.c_int(old_parent_fd),
            ctypes.c_char_p(old_name.encode()),
            ctypes.c_int(new_parent_fd),
            ctypes.c_char_p(new_name.encode()),
            ctypes.c_uint(1),
        )
        if result == 0:
            return
        error = ctypes.get_errno()
        if error == errno.EEXIST:
            raise DistributionPackageError(
                "destination_conflict",
                logical_destination.as_posix(),
                "destination appeared during publication",
            )
        raise DistributionPackageError(
            "path_identity_changed", logical_destination.as_posix(), os.strerror(error)
        )
    raise DistributionPackageError(
        "destination_conflict",
        logical_destination.as_posix(),
        "platform has no atomic no-replace directory publish",
    )


def _open_materialized_file(directory_fd: int, name: str) -> int:
    return os.open(
        name,
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0),
        0o400,
        dir_fd=directory_fd,
    )


def _write_materialized_file_at(
    directory_fd: int,
    name: str,
    payload: bytes,
) -> None:
    fd = _open_materialized_file(directory_fd, name)
    try:
        for start in range(0, len(payload), _CHUNK):
            view = memoryview(payload)[start : start + _CHUNK]
            while view:
                written = os.write(fd, view)
                if written <= 0:
                    raise OSError("short materialization write")
                view = view[written:]
        os.fsync(fd)
        os.fchmod(fd, 0o444)
    finally:
        os.close(fd)


def _copy_archive_object_at(
    archive_fd: int,
    obj: _ArchiveObject,
    destination_fd: int,
    name: str,
    logical_path: str,
) -> None:
    if obj.size > LIMITS["object_bytes_each"]:
        raise _limit("object_bytes_each", obj.size, path=logical_path)
    output_fd = _open_materialized_file(destination_fd, name)
    try:
        os.lseek(archive_fd, obj.offset, os.SEEK_SET)
        remaining = obj.size
        while remaining:
            chunk = os.read(archive_fd, min(_CHUNK, remaining))
            if not chunk:
                raise DistributionPackageError(
                    "archive_noncanonical",
                    logical_path,
                    "truncated object during materialization",
                )
            view = memoryview(chunk)
            while view:
                written = os.write(output_fd, view)
                if written <= 0:
                    raise OSError("short materialization write")
                view = view[written:]
            remaining -= len(chunk)
        os.fsync(output_fd)
        os.fchmod(output_fd, 0o444)
    finally:
        os.close(output_fd)


def _remove_private_tree_at(
    parent_fd: int,
    container_name: str,
    temp_fd: int,
    objects_fd: int | None,
    digest_fd: int | None,
    object_names: Sequence[str],
) -> None:
    """Best-effort cleanup using only retained descriptors and known names."""

    for descriptor in (digest_fd, objects_fd, temp_fd):
        if descriptor is not None:
            try:
                os.fchmod(descriptor, 0o700)
            except OSError:
                # Cleanup is best-effort; retained descriptors are still
                # closed by the caller even when permissions cannot change.
                pass
    if digest_fd is not None:
        for name in object_names:
            try:
                os.unlink(name, dir_fd=digest_fd)
            except OSError:
                # A missing or externally altered private entry must not mask
                # the original materialization failure.
                pass
    try:
        os.unlink("manifest.json", dir_fd=temp_fd)
    except OSError:
        # Best-effort cleanup must preserve the original refusal.
        pass
    if objects_fd is not None:
        try:
            os.rmdir("sha256", dir_fd=objects_fd)
        except OSError:
            # Best-effort cleanup must preserve the original refusal.
            pass
    try:
        os.rmdir("objects", dir_fd=temp_fd)
    except OSError:
        # Best-effort cleanup must preserve the original refusal.
        pass
    try:
        os.rmdir(container_name, dir_fd=parent_fd)
    except OSError:
        # Best-effort cleanup must preserve the original refusal.
        pass


__all__ = [
    "AssetLocator",
    "DistributionObject",
    "DistributionPackageError",
    "DistributionPackageManifest",
    "DistributionPackageProvenance",
    "DistributionPackageSource",
    "ExtensionRuntimePackageAuthority",
    "PackageVerificationReceiptV1",
    "PackagedAsset",
    "PublicExtensionMetadata",
    "UnresolvedPlaceholder",
    "VerifiedDistributionPackage",
    "build_distribution_package",
    "canonical_bundle_digest",
    "canonical_release_digest",
    "verify_distribution_package",
]
