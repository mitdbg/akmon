"""Import-free planning inputs for the installed public-extension runtime.

This module is deliberately the *plan* side of the v2 boundary.  It consumes a
verified distribution package through its path-free retained object authority,
parses inert descriptor/wheel/lock metadata, and emits a
deterministic, digest-bound :class:`PackagePlanV2`.  It does not import a
package, discover a path, install a wheel, select a host interpreter, or
launch an extension.

The module has no dependency on the v1 broker.  In particular, a wheel is
treated as bytes and a module:function entry is treated as descriptor data;
neither is resolved by Python import machinery.
"""

from __future__ import annotations

import base64
import binascii
import csv
from dataclasses import dataclass, fields, is_dataclass
import hashlib
import io
import json
import posixpath
import re
from collections.abc import Mapping, Sequence
from typing import Protocol, Self, cast
import unicodedata
import zipfile
from contextlib import closing
from types import MappingProxyType


EXTENSION_PACKAGE_V2_SCHEMA = "okg.extension_package/v2"
DEPENDENCY_LOCK_V1_SCHEMA = "okg.extension_runtime.dependency_lock/v1"
PACKAGE_PLAN_V1_SCHEMA = "okg.extension_runtime.package_plan/v1"
TOOLCHAIN_V2_SCHEMA = "okg.extension_runtime.toolchain/v2"
IMPORT_POLICY_V1_SCHEMA = "okg.extension_runtime.import_policy/v1"
WHEEL_RECORD_V1_SCHEMA = "okg.extension_runtime.wheel_record/v1"
RAW_DIGEST_DOMAIN = "raw-bytes/v1"
CANONICAL_DIGEST_DOMAIN = "canonical-json/v1"

_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_REF_RE = re.compile(r"^[^\s:;,/\\]+:[^\s:;,/\\]+$")
_ID_RE = re.compile(r"^[^\s:;,/\\]+$")
_PY_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_WHEEL_TAG_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
_BASE64_HASH_RE = re.compile(r"^[A-Za-z0-9_-]+$")


class ExtensionPackageV2Error(ValueError):
    """Stable, redacted refusal from the v2 package-plan boundary."""

    def __init__(
        self, code: str, message: str | None = None, *, path: str = "$"
    ) -> None:
        if not isinstance(code, str) or not _ID_RE.fullmatch(code):
            raise ValueError("error code must be a stable identifier")
        if not isinstance(path, str) or not path or "\x00" in path:
            raise ValueError("error path must be a non-empty safe string")
        self.code = code
        self.path = path
        super().__init__(message or code)

    @property
    def payload(self) -> dict[str, str]:
        return {"code": self.code, "path": self.path}


def _refuse(
    code: str, message: str | None = None, *, path: str = "$"
) -> ExtensionPackageV2Error:
    return ExtensionPackageV2Error(code, message, path=path)


def _require_digest(value: object, field_name: str = "digest") -> str:
    if not isinstance(value, str) or not _DIGEST_RE.fullmatch(value):
        raise _refuse(
            "extension_digest_domain_mismatch",
            f"{field_name} is not a raw sha256 digest",
            path=field_name,
        )
    return value


def _require_ref(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not _REF_RE.fullmatch(value):
        raise _refuse(
            "extension_package_object_mismatch",
            f"{field_name} must be an owner-qualified asset reference",
            path=field_name,
        )
    return value


def _require_id(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value or not _ID_RE.fullmatch(value):
        raise _refuse(
            "extension_descriptor_invalid",
            f"{field_name} must be an identifier",
            path=field_name,
        )
    return value


def _require_schema(value: object, field_name: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or any(char.isspace() for char in value)
        or "\x00" in value
    ):
        raise _refuse(
            "extension_descriptor_invalid",
            f"{field_name} must be a schema token",
            path=field_name,
        )
    return value


def _require_nonnegative_int(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise _refuse(
            "extension_descriptor_invalid",
            f"{field_name} must be a non-negative integer",
            path=field_name,
        )
    return value


def raw_blob_digest(value: bytes | bytearray | memoryview) -> str:
    """Return the raw-byte digest; text is intentionally not accepted."""

    if not isinstance(value, (bytes, bytearray, memoryview)):
        raise TypeError("raw_blob_digest requires bytes")
    return "sha256:" + hashlib.sha256(bytes(value)).hexdigest()


def _nfc(value: object, path: str = "$") -> None:
    if isinstance(value, str):
        if "\x00" in value or unicodedata.normalize("NFC", value) != value:
            raise _refuse(
                "extension_digest_domain_mismatch",
                "canonical strings must be NFC and NUL-free",
                path=path,
            )
        return
    if isinstance(value, Mapping):
        for key, child in value.items():
            if not isinstance(key, str):
                raise _refuse(
                    "extension_digest_domain_mismatch",
                    "canonical object keys must be strings",
                    path=path,
                )
            _nfc(key, f"{path}.{key}")
            _nfc(child, f"{path}.{key}")
    elif isinstance(value, (tuple, list)):
        for index, child in enumerate(value):
            _nfc(child, f"{path}[{index}]")


def _json_value(value: object, path: str = "$") -> object:
    if is_dataclass(value):
        to_record = getattr(value, "to_record", None)
        if callable(to_record):
            return _json_value(to_record(), path)
        return _json_value(
            {item.name: getattr(value, item.name) for item in fields(value)}, path
        )
    if isinstance(value, Mapping):
        result: dict[str, object] = {}
        for key, child in value.items():
            if not isinstance(key, str):
                raise _refuse(
                    "extension_digest_domain_mismatch",
                    "canonical object keys must be strings",
                    path=path,
                )
            if key in result:
                raise _refuse(
                    "extension_digest_domain_mismatch",
                    "duplicate canonical object key",
                    path=f"{path}.{key}",
                )
            result[key] = _json_value(child, f"{path}.{key}")
        return result
    if isinstance(value, (tuple, list)):
        return [
            _json_value(item, f"{path}[{index}]") for index, item in enumerate(value)
        ]
    if isinstance(value, (str, int, bool)) or value is None:
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        return value
    # Floating point authority is intentionally not part of this contract.
    raise _refuse(
        "extension_digest_domain_mismatch",
        "value is not canonical JSON authority",
        path=path,
    )


def canonical_json_bytes(value: Mapping[str, object] | object) -> bytes:
    """Encode the v2 canonical JSON domain (sorted, compact, NFC, UTF-8)."""

    normalized = _json_value(value)
    _nfc(normalized)
    try:
        return json.dumps(
            normalized,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError) as exc:
        raise _refuse(
            "extension_digest_domain_mismatch", "value cannot be canonically encoded"
        ) from exc


def canonical_record_digest(value: Mapping[str, object] | object) -> str:
    """Digest one structured record in the canonical-json domain."""

    return "sha256:" + hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _exact_keys(value: Mapping[str, object], allowed: set[str], path: str) -> None:
    unknown = set(value) - allowed
    if unknown or len(set(value)) != len(value):
        names = ", ".join(sorted(str(item) for item in unknown)) or "duplicate key"
        raise _refuse(
            "extension_descriptor_unknown_field",
            f"unknown or duplicate fields: {names}",
            path=path,
        )


def _mapping(value: object, path: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise _refuse("extension_descriptor_invalid", "expected an object", path=path)
    if any(not isinstance(key, str) for key in value):
        raise _refuse(
            "extension_descriptor_invalid", "object keys must be strings", path=path
        )
    return value


def _read_json(raw: bytes, path: str) -> Mapping[str, object]:
    try:
        text = raw.decode("utf-8", errors="strict")

        def pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
            result: dict[str, object] = {}
            for key, value in pairs:
                if key in result:
                    raise _refuse(
                        "extension_descriptor_unknown_field",
                        "duplicate JSON key",
                        path=path,
                    )
                result[key] = value
            return result

        value = json.loads(
            text,
            object_pairs_hook=pairs,
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError("non-finite")),
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        if isinstance(exc, ExtensionPackageV2Error):
            raise
        raise _refuse(
            "extension_descriptor_invalid", "JSON object is invalid", path=path
        ) from exc
    result = _mapping(value, path)
    if raw != canonical_json_bytes(result):
        raise _refuse(
            "extension_digest_domain_mismatch",
            "structured object bytes are not canonical JSON",
            path=path,
        )
    return result


@dataclass(frozen=True, slots=True)
class RawBlobRef:
    """A digest-domain-explicit reference to exact raw bytes."""

    digest: str
    size: int

    def __post_init__(self) -> None:
        _require_digest(self.digest, "raw_blob.digest")
        _require_nonnegative_int(self.size, "raw_blob.size")

    def to_record(self) -> dict[str, object]:
        return {
            "digest_domain": RAW_DIGEST_DOMAIN,
            "digest": self.digest,
            "size": self.size,
        }

    @classmethod
    def from_value(cls, value: object, *, path: str = "raw_blob") -> Self:
        if isinstance(value, cls):
            return value
        if isinstance(value, str):
            raise _refuse(
                "extension_digest_domain_mismatch",
                "raw blob authority must use an explicit domain wrapper",
                path=path,
            )
        raw = _mapping(value, path)
        _exact_keys(raw, {"digest_domain", "digest", "size"}, path)
        if raw.get("digest_domain") != RAW_DIGEST_DOMAIN:
            raise _refuse(
                "extension_digest_domain_mismatch",
                "raw blob has the wrong digest domain",
                path=path,
            )
        return cls(
            _require_digest(raw.get("digest"), f"{path}.digest"),
            _require_nonnegative_int(raw.get("size"), f"{path}.size"),
        )


@dataclass(frozen=True, slots=True)
class CanonicalRecordRef:
    """A digest-domain-explicit reference to one structured canonical record."""

    schema_version: str
    digest: str

    def __post_init__(self) -> None:
        _require_schema(self.schema_version, "canonical_record.schema_version")
        _require_digest(self.digest, "canonical_record.digest")

    def to_record(self) -> dict[str, str]:
        return {
            "digest_domain": CANONICAL_DIGEST_DOMAIN,
            "schema_version": self.schema_version,
            "digest": self.digest,
        }


# The longer name is useful at call sites that speak in terms of a digest
# rather than a reference.  It remains one type, so no second digest domain is
# accidentally introduced.
CanonicalRecordDigest = CanonicalRecordRef


def _canonical_ref(value: object, schema: str, path: str) -> CanonicalRecordRef:
    if isinstance(value, CanonicalRecordRef):
        if value.schema_version != schema:
            raise _refuse(
                "extension_digest_domain_mismatch",
                "canonical schema mismatch",
                path=path,
            )
        return value
    if isinstance(value, str):
        raise _refuse(
            "extension_digest_domain_mismatch",
            "canonical authority must use an explicit domain wrapper",
            path=path,
        )
    raw = _mapping(value, path)
    _exact_keys(raw, {"digest_domain", "schema_version", "digest"}, path)
    if (
        raw.get("digest_domain") != CANONICAL_DIGEST_DOMAIN
        or raw.get("schema_version") != schema
    ):
        raise _refuse(
            "extension_digest_domain_mismatch",
            "canonical record domain/schema mismatch",
            path=path,
        )
    return CanonicalRecordRef(
        schema, _require_digest(raw.get("digest"), f"{path}.digest")
    )


@dataclass(frozen=True, slots=True)
class PackageObjectSelection:
    """An explicit verified-package asset selection.

    ``asset_ref`` is the only locator accepted by this module.  A host path,
    archive path, ``DistributionObject`` digest, or inferred package name is
    never substituted for it.  ``parsed_record`` and ``import_roots`` are
    metadata projections populated by the planner for wheel selections.
    """

    asset_ref: str
    raw_blob_digest: str
    role: str = "object"
    size: int | None = None
    parsed_record: tuple[Mapping[str, object], ...] = ()
    import_roots: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _require_ref(self.asset_ref, "selection.asset_ref")
        _require_digest(self.raw_blob_digest, "selection.raw_blob_digest")
        _require_id(self.role, "selection.role")
        if self.size is not None:
            _require_nonnegative_int(self.size, "selection.size")
        records: list[Mapping[str, object]] = []
        for index, item in enumerate(self.parsed_record):
            records.append(_mapping(item, f"selection.parsed_record[{index}]"))
        root_values = tuple(
            _require_id(item, "selection.import_roots") for item in self.import_roots
        )
        if root_values != tuple(sorted(set(root_values))):
            raise _refuse(
                "extension_package_object_mismatch",
                "selection import roots must be unique and sorted",
                path="selection.import_roots",
            )
        object.__setattr__(self, "parsed_record", tuple(records))
        object.__setattr__(self, "import_roots", root_values)

    @property
    def raw_blob_ref(self) -> RawBlobRef:
        if self.size is None:
            raise _refuse(
                "extension_package_object_mismatch",
                "selection size is not known",
                path="selection.size",
            )
        return RawBlobRef(self.raw_blob_digest, self.size)

    def with_verified_size(self, size: int) -> "PackageObjectSelection":
        if self.size is not None and self.size != size:
            raise _refuse(
                "extension_package_object_mismatch",
                "selected raw size changed",
                path=self.asset_ref,
            )
        return PackageObjectSelection(
            self.asset_ref,
            self.raw_blob_digest,
            self.role,
            size,
            self.parsed_record,
            self.import_roots,
        )

    def to_record(self) -> dict[str, object]:
        result: dict[str, object] = {
            "asset_ref": self.asset_ref,
            "raw_blob": self.raw_blob_ref.to_record()
            if self.size is not None
            else {"digest_domain": RAW_DIGEST_DOMAIN, "digest": self.raw_blob_digest},
            "role": self.role,
        }
        if self.parsed_record:
            result["parsed_record"] = list(self.parsed_record)
        if self.import_roots:
            result["import_roots"] = list(self.import_roots)
        return result

    @classmethod
    def from_value(
        cls, value: object, *, role: str = "object"
    ) -> "PackageObjectSelection":
        return _selection_from_value(cls, value, role=role)


@dataclass(frozen=True, slots=True)
class WheelRecordEntry:
    path: str
    raw_blob: RawBlobRef | None

    def to_record(self) -> dict[str, object]:
        return {
            "path": self.path,
            "raw_blob": None if self.raw_blob is None else self.raw_blob.to_record(),
        }


@dataclass(frozen=True, slots=True)
class WheelProjection:
    """Metadata-only wheel projection used by plan and seal."""

    selection: PackageObjectSelection
    normalized_name: str
    version: str
    wheel_tags: tuple[str, ...]
    record_entries: tuple[WheelRecordEntry, ...]
    wheel_record_manifest_digest: CanonicalRecordRef
    import_roots: tuple[str, ...]

    @property
    def asset_ref(self) -> str:
        return self.selection.asset_ref

    @property
    def raw_blob_digest(self) -> str:
        return self.selection.raw_blob_digest

    @property
    def parsed_record(self) -> tuple[WheelRecordEntry, ...]:
        return self.record_entries

    def to_record(self) -> dict[str, object]:
        return {
            "asset_ref": self.selection.asset_ref,
            "normalized_name": self.normalized_name,
            "version": self.version,
            "wheel_raw_blob": self.selection.raw_blob_ref.to_record(),
            "wheel_tags": list(self.wheel_tags),
            "wheel_record_manifest_digest": self.wheel_record_manifest_digest.to_record(),
            "record": [entry.to_record() for entry in self.record_entries],
            "import_roots": list(self.import_roots),
        }


@dataclass(frozen=True, slots=True)
class ExtensionEntryV2:
    extension_id: str
    kind: str
    module: str
    function: str
    public_selector: Mapping[str, object]
    subject: Mapping[str, object]
    declared_capabilities: tuple[str, ...]
    requirements: Mapping[str, object]
    provider_effects: Mapping[str, object]
    output_policy: Mapping[str, object]
    descriptor_entry_digest: CanonicalRecordRef
    extension_selection_digest: CanonicalRecordRef

    def to_record_without_digest(self) -> dict[str, object]:
        return {
            "extension_id": self.extension_id,
            "kind": self.kind,
            "module": self.module,
            "function": self.function,
            "public_selector": dict(self.public_selector),
            "subject": dict(self.subject),
            "declared_capabilities": list(self.declared_capabilities),
            "requirements": dict(self.requirements),
            "provider_effects": dict(self.provider_effects),
            "output_policy": dict(self.output_policy),
            "descriptor_entry_digest": self.descriptor_entry_digest.to_record(),
            "extension_selection_digest": self.extension_selection_digest.to_record(),
        }

    @staticmethod
    def _subrecord_digest(
        schema: str, value: Mapping[str, object]
    ) -> CanonicalRecordRef:
        return CanonicalRecordRef(
            schema,
            canonical_record_digest({"schema_version": schema, "value": dict(value)}),
        )

    @property
    def requirements_digest(self) -> CanonicalRecordRef:
        return self._subrecord_digest(
            "okg.extension_runtime.requirements/v1", self.requirements
        )

    @property
    def provider_effects_digest(self) -> CanonicalRecordRef:
        return self._subrecord_digest(
            "okg.extension_runtime.provider_effects/v1", self.provider_effects
        )

    @property
    def output_policy_digest(self) -> CanonicalRecordRef:
        return self._subrecord_digest(
            "okg.extension_runtime.output_policy/v1", self.output_policy
        )

    @property
    def entry_metadata_digest(self) -> CanonicalRecordRef:
        record = {
            "schema_version": "okg.extension_runtime.entry/v1",
            "extension_id": self.extension_id,
            "kind": self.kind,
            "module": self.module,
            "function": self.function,
            "declared_capabilities": list(self.declared_capabilities),
            "requirements_digest": self.requirements_digest.to_record(),
            "provider_effects_digest": self.provider_effects_digest.to_record(),
            "output_policy_digest": self.output_policy_digest.to_record(),
            "descriptor_entry_digest": self.descriptor_entry_digest.to_record(),
            "extension_selection_digest": self.extension_selection_digest.to_record(),
        }
        return CanonicalRecordRef(
            "okg.extension_runtime.entry/v1",
            canonical_record_digest(record),
        )

    def to_record(self) -> dict[str, object]:
        result = self.to_record_without_digest()
        result["requirements_digest"] = self.requirements_digest.to_record()
        result["provider_effects_digest"] = self.provider_effects_digest.to_record()
        result["output_policy_digest"] = self.output_policy_digest.to_record()
        result["entry_metadata_digest"] = self.entry_metadata_digest.to_record()
        return result


@dataclass(frozen=True, slots=True)
class ExtensionPackageDescriptorV2:
    package_ref: Mapping[str, object]
    entries: tuple[ExtensionEntryV2, ...]
    raw_blob: RawBlobRef
    raw_record: Mapping[str, object]

    def to_record(self) -> dict[str, object]:
        return {
            "schema_version": EXTENSION_PACKAGE_V2_SCHEMA,
            "package_ref": dict(self.package_ref),
            "entries": list(cast(Sequence[object], self.raw_record["entries"])),
            "descriptor_raw_blob": self.raw_blob.to_record(),
        }


@dataclass(frozen=True, slots=True)
class RuntimeToolchainIdentity:
    """The public, path-free identity selected from an active sealed release."""

    sealed_release_artifact_id: Mapping[str, object]
    sealed_release_tree_digest: CanonicalRecordRef
    release_runtime_authority_receipt_digest: CanonicalRecordRef
    platform_identity: Mapping[str, str]
    python_abi: str
    interpreter_raw_blob: RawBlobRef
    distribution_prefix_tree_digest: CanonicalRecordRef
    prefix_copy_proof_digest: CanonicalRecordRef
    stdlib_manifest_digest: CanonicalRecordRef
    runtime_layout_digest: CanonicalRecordRef
    public_sdk_abi: str
    public_sdk_raw_blob: RawBlobRef
    child_host_raw_blob: RawBlobRef
    bootstrap_verifier_raw_blob: RawBlobRef
    builder_raw_blob: RawBlobRef
    broker_raw_blob: RawBlobRef
    containment_launcher_raw_blob: RawBlobRef
    containment_profile_manifest_digest: CanonicalRecordRef

    @classmethod
    def from_value(cls, value: object) -> "RuntimeToolchainIdentity":
        """Parse one closed toolchain manifest without discovering host state."""

        parsed = _identity_mapping(value, "toolchain")
        if not isinstance(parsed, cls):  # pragma: no cover - defensive typing guard
            raise _refuse(
                "extension_runtime_toolchain_mismatch",
                "toolchain identity is invalid",
                path="toolchain",
            )
        return parsed

    def __post_init__(self) -> None:
        artifact = _mapping(
            self.sealed_release_artifact_id, "toolchain.sealed_release_artifact_id"
        )
        _exact_keys(
            artifact,
            {"schema_version", "artifact_id"},
            "toolchain.sealed_release_artifact_id",
        )
        if artifact.get("schema_version") != "okg.sealed_release/v1":
            raise _refuse(
                "extension_runtime_toolchain_mismatch",
                "sealed release artifact schema is unsupported",
                path="toolchain.sealed_release_artifact_id.schema_version",
            )
        _require_digest(
            artifact.get("artifact_id"),
            "toolchain.sealed_release_artifact_id.artifact_id",
        )
        required_platform = {"os", "architecture", "libc", "kernel_contract"}
        if set(self.platform_identity) != required_platform:
            raise _refuse(
                "extension_runtime_toolchain_mismatch",
                "platform identity is not closed",
                path="toolchain.platform_identity",
            )
        for key, value in self.platform_identity.items():
            _require_id(value, f"toolchain.platform_identity.{key}")
        _require_id(self.python_abi, "toolchain.python_abi")
        _require_id(self.public_sdk_abi, "toolchain.public_sdk_abi")
        for name in (
            "interpreter_raw_blob",
            "public_sdk_raw_blob",
            "child_host_raw_blob",
            "bootstrap_verifier_raw_blob",
            "builder_raw_blob",
            "broker_raw_blob",
            "containment_launcher_raw_blob",
        ):
            if not isinstance(getattr(self, name), RawBlobRef):
                raise _refuse(
                    "extension_runtime_toolchain_mismatch",
                    "toolchain raw identity is not typed",
                    path=f"toolchain.{name}",
                )
        canonical_schemas = {
            "sealed_release_tree_digest": "okg.sealed_release.tree/v1",
            "release_runtime_authority_receipt_digest": "okg.sealed_release.runtime_authority/v2",
            "distribution_prefix_tree_digest": "okg.sealed_release.runtime_distribution_prefix_tree/v1",
            "prefix_copy_proof_digest": "okg.sealed_release.runtime_prefix_copy_proof/v1",
            "stdlib_manifest_digest": "okg.extension_runtime.stdlib_manifest/v1",
            "runtime_layout_digest": "okg.extension_runtime.layout/v2",
            "containment_profile_manifest_digest": "okg.extension_runtime.containment_profile/v1",
        }
        for name, schema in canonical_schemas.items():
            value = getattr(self, name)
            if (
                not isinstance(value, CanonicalRecordRef)
                or value.schema_version != schema
            ):
                raise _refuse(
                    "extension_runtime_toolchain_mismatch",
                    "toolchain canonical identity is not typed",
                    path=f"toolchain.{name}",
                )
        object.__setattr__(
            self,
            "sealed_release_artifact_id",
            MappingProxyType(dict(artifact)),
        )
        object.__setattr__(
            self,
            "platform_identity",
            MappingProxyType(dict(self.platform_identity)),
        )

    @property
    def runtime_toolchain_manifest_digest(self) -> CanonicalRecordRef:
        return CanonicalRecordRef(
            TOOLCHAIN_V2_SCHEMA,
            canonical_record_digest(self.to_record_without_digest()),
        )

    def to_record_without_digest(self) -> dict[str, object]:
        return {
            "schema_version": TOOLCHAIN_V2_SCHEMA,
            "sealed_release_artifact_id": dict(self.sealed_release_artifact_id),
            "sealed_release_tree_digest": self.sealed_release_tree_digest.to_record(),
            "release_runtime_authority_receipt_digest": self.release_runtime_authority_receipt_digest.to_record(),
            "platform_identity": dict(self.platform_identity),
            "python_abi": self.python_abi,
            "interpreter_raw_blob": self.interpreter_raw_blob.to_record(),
            "distribution_prefix_tree_digest": self.distribution_prefix_tree_digest.to_record(),
            "prefix_copy_proof_digest": self.prefix_copy_proof_digest.to_record(),
            "stdlib_manifest_digest": self.stdlib_manifest_digest.to_record(),
            "runtime_layout_digest": self.runtime_layout_digest.to_record(),
            "public_sdk_abi": self.public_sdk_abi,
            "public_sdk_raw_blob": self.public_sdk_raw_blob.to_record(),
            "child_host_raw_blob": self.child_host_raw_blob.to_record(),
            "bootstrap_verifier_raw_blob": self.bootstrap_verifier_raw_blob.to_record(),
            "builder_raw_blob": self.builder_raw_blob.to_record(),
            "broker_raw_blob": self.broker_raw_blob.to_record(),
            "containment_launcher_raw_blob": self.containment_launcher_raw_blob.to_record(),
            "containment_profile_manifest_digest": self.containment_profile_manifest_digest.to_record(),
        }

    def to_record(self) -> dict[str, object]:
        result = self.to_record_without_digest()
        result["runtime_toolchain_manifest_digest"] = (
            self.runtime_toolchain_manifest_digest.to_record()
        )
        return result


@dataclass(frozen=True, slots=True)
class VerifiedPackageAuthority:
    """Path-free receipt identity from a verified distribution package."""

    distribution_package_schema: str
    manifest_raw_blob: RawBlobRef
    transport_archive_raw_blob: RawBlobRef
    verified_object_index_digest: CanonicalRecordRef
    verified_asset_index_digest: CanonicalRecordRef
    package_identity_index_digest: CanonicalRecordRef
    public_extension_index_digest: CanonicalRecordRef
    verification_profile_digest: CanonicalRecordRef
    package_verification_receipt_digest: CanonicalRecordRef

    def __post_init__(self) -> None:
        _require_schema(
            self.distribution_package_schema,
            "verified_package.distribution_package_schema",
        )
        expected_schemas = {
            "verified_object_index_digest": "okg.extension_runtime.verified_object_index/v1",
            "verified_asset_index_digest": "okg.extension_runtime.verified_asset_index/v1",
            "package_identity_index_digest": "okg.extension_runtime.package_identity_index/v1",
            "public_extension_index_digest": "okg.extension_runtime.public_extension_index/v1",
            "verification_profile_digest": "okg.distribution_package.verification_profile/v1",
            "package_verification_receipt_digest": "okg.extension_runtime.package_verification_receipt/v1",
        }
        for name, schema in expected_schemas.items():
            value = getattr(self, name)
            if (
                not isinstance(value, CanonicalRecordRef)
                or value.schema_version != schema
            ):
                raise _refuse(
                    "extension_package_object_mismatch",
                    "verified package receipt field has the wrong domain",
                    path=f"verified_package.{name}",
                )
        if self.package_verification_receipt_digest.digest != canonical_record_digest(
            self.receipt_preimage()
        ):
            raise _refuse(
                "extension_package_object_mismatch",
                "verified package receipt digest is stale",
                path="verified_package.package_verification_receipt_digest",
            )

    def receipt_preimage(self) -> dict[str, object]:
        return {
            "schema_version": "okg.extension_runtime.package_verification_receipt/v1",
            "distribution_package_schema": self.distribution_package_schema,
            "manifest": self.manifest_raw_blob.to_record(),
            "transport_archive": self.transport_archive_raw_blob.to_record(),
            "verified_object_index_digest": self.verified_object_index_digest.to_record(),
            "verified_asset_index_digest": self.verified_asset_index_digest.to_record(),
            "package_identity_index_digest": self.package_identity_index_digest.to_record(),
            "public_extension_index_digest": self.public_extension_index_digest.to_record(),
            "verification_profile_digest": self.verification_profile_digest.to_record(),
        }

    @property
    def verified_distribution_authority_digest(self) -> CanonicalRecordRef:
        return CanonicalRecordRef(
            "okg.extension_runtime.verified_distribution_authority/v1",
            canonical_record_digest(self.to_record()),
        )

    def to_record(self) -> dict[str, object]:
        return {
            "schema_version": "okg.extension_runtime.verified_distribution_authority/v1",
            "distribution_package_schema": self.distribution_package_schema,
            "manifest": self.manifest_raw_blob.to_record(),
            "transport_archive": self.transport_archive_raw_blob.to_record(),
            "verified_object_index_digest": self.verified_object_index_digest.to_record(),
            "verified_asset_index_digest": self.verified_asset_index_digest.to_record(),
            "package_identity_index_digest": self.package_identity_index_digest.to_record(),
            "public_extension_index_digest": self.public_extension_index_digest.to_record(),
            "verification_profile_digest": self.verification_profile_digest.to_record(),
            "package_verification_receipt_digest": self.package_verification_receipt_digest.to_record(),
        }


@dataclass(frozen=True, slots=True)
class RuntimeDistributionProjection:
    """Metadata-only projection of one trusted toolchain wheel."""

    normalized_name: str
    version: str
    wheel_raw_blob: RawBlobRef
    wheel_record_manifest_digest: CanonicalRecordRef
    import_roots: tuple[str, ...]

    def __post_init__(self) -> None:
        if (
            _normalize_distribution(
                self.normalized_name, "runtime_distribution.normalized_name"
            )
            != self.normalized_name
        ):
            raise _refuse(
                "extension_runtime_toolchain_mismatch",
                "runtime distribution name is not normalized",
            )
        _require_id(self.version, "runtime_distribution.version")
        if not isinstance(self.wheel_raw_blob, RawBlobRef):
            raise _refuse(
                "extension_runtime_toolchain_mismatch",
                "runtime distribution raw blob is not typed",
            )
        if (
            not isinstance(self.wheel_record_manifest_digest, CanonicalRecordRef)
            or self.wheel_record_manifest_digest.schema_version
            != WHEEL_RECORD_V1_SCHEMA
        ):
            raise _refuse(
                "extension_runtime_toolchain_mismatch",
                "runtime distribution RECORD identity is not typed",
            )
        roots = tuple(
            _require_id(root, "runtime_distribution.import_roots")
            for root in self.import_roots
        )
        if roots != tuple(sorted(set(roots))):
            raise _refuse(
                "extension_runtime_toolchain_mismatch",
                "runtime import roots must be unique and sorted",
            )
        object.__setattr__(self, "import_roots", roots)

    def to_record(self) -> dict[str, object]:
        return {
            "normalized_name": self.normalized_name,
            "version": self.version,
            "wheel_raw_blob": self.wheel_raw_blob.to_record(),
            "wheel_record_manifest_digest": self.wheel_record_manifest_digest.to_record(),
            "import_roots": list(self.import_roots),
        }

    @classmethod
    def from_value(cls, value: object, *, path: str) -> "RuntimeDistributionProjection":
        if isinstance(value, cls):
            return value
        raw = _mapping(value, path)
        _exact_keys(
            raw,
            {
                "normalized_name",
                "version",
                "wheel_raw_blob",
                "wheel_record_manifest_digest",
                "import_roots",
            },
            path,
        )
        roots = raw["import_roots"]
        if isinstance(roots, (str, bytes)) or not isinstance(roots, Sequence):
            raise _refuse(
                "extension_runtime_toolchain_mismatch",
                "runtime import roots must be an array",
                path=f"{path}.import_roots",
            )
        return cls(
            _normalize_distribution(raw["normalized_name"], f"{path}.normalized_name"),
            _require_id(raw["version"], f"{path}.version"),
            RawBlobRef.from_value(raw["wheel_raw_blob"], path=f"{path}.wheel_raw_blob"),
            _canonical_ref(
                raw["wheel_record_manifest_digest"],
                WHEEL_RECORD_V1_SCHEMA,
                f"{path}.wheel_record_manifest_digest",
            ),
            tuple(_require_id(root, f"{path}.import_roots") for root in roots),
        )


@dataclass(frozen=True, slots=True)
class RuntimePackageProjections:
    public_sdk: RuntimeDistributionProjection
    child_host: RuntimeDistributionProjection

    def to_record(self) -> dict[str, object]:
        return {
            "public_sdk": self.public_sdk.to_record(),
            "child_host": self.child_host.to_record(),
        }

    @classmethod
    def from_value(cls, value: object) -> "RuntimePackageProjections":
        if isinstance(value, cls):
            return value
        raw = _mapping(value, "runtime_packages")
        _exact_keys(raw, {"public_sdk", "child_host"}, "runtime_packages")
        return cls(
            RuntimeDistributionProjection.from_value(
                raw["public_sdk"], path="runtime_packages.public_sdk"
            ),
            RuntimeDistributionProjection.from_value(
                raw["child_host"], path="runtime_packages.child_host"
            ),
        )


@dataclass(frozen=True, slots=True)
class PackagePlanV2:
    descriptor: ExtensionPackageDescriptorV2
    descriptor_selection: PackageObjectSelection
    selected_entry: ExtensionEntryV2
    distribution_extension_record_digest: CanonicalRecordRef
    verified_distribution_input_digest: CanonicalRecordRef
    extension_wheel: WheelProjection
    dependency_lock: PackageObjectSelection
    dependency_wheels: tuple[WheelProjection, ...]
    sbom: PackageObjectSelection
    provenance: PackageObjectSelection
    verified_package_authority: VerifiedPackageAuthority
    runtime_packages: RuntimePackageProjections
    runtime_toolchain: RuntimeToolchainIdentity
    import_policy_digest: CanonicalRecordRef
    containment_profile_manifest_digest: CanonicalRecordRef
    python_abi: str
    platform_tags: tuple[str, ...]
    public_sdk_abi: str
    package_plan_digest: CanonicalRecordRef

    @property
    def runtime_toolchain_manifest_digest(self) -> CanonicalRecordRef:
        return self.runtime_toolchain.runtime_toolchain_manifest_digest

    @property
    def extension_wheel_asset_ref(self) -> str:
        return self.extension_wheel.asset_ref

    @property
    def extension_wheel_raw_blob_digest(self) -> str:
        return self.extension_wheel.raw_blob_digest

    @property
    def dependency_wheel_asset_refs(self) -> tuple[str, ...]:
        return tuple(wheel.asset_ref for wheel in self.dependency_wheels)

    @property
    def dependency_wheel_raw_blob_digests(self) -> tuple[str, ...]:
        return tuple(wheel.raw_blob_digest for wheel in self.dependency_wheels)

    def to_record_without_digest(self) -> dict[str, object]:
        return {
            "schema_version": PACKAGE_PLAN_V1_SCHEMA,
            "descriptor_schema": EXTENSION_PACKAGE_V2_SCHEMA,
            "verified_distribution_input_digest": self.verified_distribution_input_digest.to_record(),
            "descriptor_object": self.descriptor_selection.to_record(),
            "descriptor": self.descriptor.to_record(),
            "selected_entry": self.selected_entry.to_record(),
            "distribution_extension_record_digest": self.distribution_extension_record_digest.to_record(),
            "extension_distribution": self.extension_wheel.to_record(),
            "dependency_lock": self.dependency_lock.to_record(),
            "dependencies": [wheel.to_record() for wheel in self.dependency_wheels],
            "sbom": self.sbom.to_record(),
            "provenance": self.provenance.to_record(),
            "verified_package_authority": self.verified_package_authority.to_record(),
            "runtime_packages": self.runtime_packages.to_record(),
            "python_abi": self.python_abi,
            "platform_tags": list(self.platform_tags),
            "public_sdk_abi": self.public_sdk_abi,
            "public_sdk_raw_blob": self.runtime_toolchain.public_sdk_raw_blob.to_record(),
            "child_host_raw_blob": self.runtime_toolchain.child_host_raw_blob.to_record(),
            "runtime_toolchain_manifest_digest": self.runtime_toolchain.runtime_toolchain_manifest_digest.to_record(),
            "runtime_layout_digest": self.runtime_toolchain.runtime_layout_digest.to_record(),
            "runtime_toolchain": self.runtime_toolchain.to_record(),
            "import_policy_digest": self.import_policy_digest.to_record(),
            "containment_profile_manifest_digest": self.containment_profile_manifest_digest.to_record(),
        }

    def to_record(self) -> dict[str, object]:
        result = self.to_record_without_digest()
        result["package_plan_digest"] = self.package_plan_digest.to_record()
        return result

    @classmethod
    def from_verified_package(
        cls,
        package: "_VerifiedPackage",
        selections: PackagePlanSelections
        | Mapping[str, object]
        | Sequence[PackageObjectSelection | Mapping[str, object]],
        runtime_toolchain: object,
        *,
        extension_id: str,
        platform_tags: Sequence[str] = (),
    ) -> "PackagePlanV2":
        return plan_extension_package_v2(
            package,
            selections,
            runtime_toolchain,
            extension_id=extension_id,
            platform_tags=platform_tags,
        )


@dataclass(frozen=True, slots=True)
class PackagePlanSelections:
    descriptor: PackageObjectSelection
    extension_wheel: PackageObjectSelection
    dependency_lock: PackageObjectSelection
    dependency_wheels: tuple[PackageObjectSelection, ...] = ()
    sbom: PackageObjectSelection | None = None
    provenance: PackageObjectSelection | None = None

    def __post_init__(self) -> None:
        expected = {
            "descriptor": "descriptor",
            "extension_wheel": "extension-wheel",
            "dependency_lock": "dependency-lock",
        }
        for field_name, role in expected.items():
            selection = getattr(self, field_name)
            if selection.role not in {role, "object"}:
                raise _refuse(
                    "extension_package_object_mismatch",
                    "selection role does not match field",
                    path=field_name,
                )
        for selection in self.dependency_wheels:
            if selection.role not in {"dependency-wheel", "object"}:
                raise _refuse(
                    "extension_package_object_mismatch",
                    "dependency wheel selection role is invalid",
                    path="dependency_wheels",
                )

    @classmethod
    def from_value(cls, value: object) -> "PackagePlanSelections":
        if isinstance(value, cls):
            return value
        if isinstance(value, Mapping):
            allowed = {
                "descriptor",
                "extension_wheel",
                "dependency_lock",
                "dependency_wheels",
                "sbom",
                "provenance",
            }
            _exact_keys(value, allowed, "selections")

            def one(name: str, required: bool = True) -> PackageObjectSelection | None:
                raw = value.get(name)
                if raw is None and not required:
                    return None
                if isinstance(raw, PackageObjectSelection):
                    return raw
                return PackageObjectSelection.from_value(
                    raw, role=name.replace("_", "-")
                )

            wheels_raw = value.get("dependency_wheels", ())
            if isinstance(wheels_raw, (str, bytes)) or not isinstance(
                wheels_raw, Sequence
            ):
                raise _refuse(
                    "extension_package_object_mismatch",
                    "dependency_wheels must be a sequence",
                    path="selections.dependency_wheels",
                )
            wheels = tuple(
                item
                if isinstance(item, PackageObjectSelection)
                else PackageObjectSelection.from_value(item, role="dependency-wheel")
                for item in wheels_raw
            )
            return cls(
                cast(PackageObjectSelection, one("descriptor")),
                cast(PackageObjectSelection, one("extension_wheel")),
                cast(PackageObjectSelection, one("dependency_lock")),
                wheels,
                one("sbom", False),
                one("provenance", False),
            )
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
            grouped: dict[str, list[PackageObjectSelection]] = {}
            for index, item in enumerate(value):
                selection = (
                    item
                    if isinstance(item, PackageObjectSelection)
                    else PackageObjectSelection.from_value(item)
                )
                grouped.setdefault(selection.role, []).append(selection)

            def take(
                role: str, *, optional: bool = False
            ) -> PackageObjectSelection | None:
                values = (
                    grouped.get(role) or grouped.get("object")
                    if role == "descriptor"
                    else grouped.get(role)
                )
                if not values:
                    if optional:
                        return None
                    raise _refuse(
                        "extension_package_object_mismatch",
                        f"missing explicit {role} asset selection",
                        path=f"selections.{role}",
                    )
                if len(values) != 1:
                    raise _refuse(
                        "extension_package_object_mismatch",
                        f"multiple {role} asset selections",
                        path=f"selections.{role}",
                    )
                return values[0]

            deps = tuple(grouped.get("dependency-wheel", ()))
            return cls(
                cast(PackageObjectSelection, take("descriptor")),
                cast(PackageObjectSelection, take("extension-wheel")),
                cast(PackageObjectSelection, take("dependency-lock")),
                deps,
                take("sbom", optional=True),
                take("provenance", optional=True),
            )
        raise _refuse(
            "extension_package_object_mismatch",
            "explicit package selections are required",
            path="selections",
        )


def _selection_from_value(
    cls: type[PackageObjectSelection], value: object, *, role: str
) -> PackageObjectSelection:
    if isinstance(value, PackageObjectSelection):
        return value
    raw = _mapping(value, f"selection.{role}")
    allowed = {
        "asset_ref",
        "raw_blob_digest",
        "raw_blob",
        "role",
        "size",
        "parsed_record",
        "import_roots",
    }
    _exact_keys(raw, allowed, f"selection.{role}")
    explicit_ref: RawBlobRef | None = None
    if "raw_blob" in raw:
        explicit_ref = RawBlobRef.from_value(
            raw["raw_blob"], path=f"selection.{role}.raw_blob"
        )
    if explicit_ref is not None and "raw_blob_digest" in raw:
        legacy = _require_digest(
            raw["raw_blob_digest"], f"selection.{role}.raw_blob_digest"
        )
        if explicit_ref.digest != legacy:
            raise _refuse(
                "extension_package_object_mismatch",
                "selection supplies conflicting raw object identities",
                path=f"selection.{role}",
            )
    digest_value = raw.get("raw_blob_digest", raw.get("raw_blob"))
    ref = explicit_ref
    digest = (
        ref.digest
        if ref is not None
        else _require_digest(digest_value, f"selection.{role}.raw_blob_digest")
    )
    size_value = raw.get("size", ref.size if ref is not None else None)
    parsed = raw.get("parsed_record", ())
    if (
        isinstance(parsed, Mapping)
        or isinstance(parsed, (str, bytes))
        or not isinstance(parsed, Sequence)
    ):
        raise _refuse(
            "extension_package_object_mismatch",
            "parsed_record must be a sequence",
            path=f"selection.{role}.parsed_record",
        )
    roots = raw.get("import_roots", ())
    if isinstance(roots, (str, bytes)) or not isinstance(roots, Sequence):
        raise _refuse(
            "extension_package_object_mismatch",
            "import_roots must be a sequence",
            path=f"selection.{role}.import_roots",
        )
    return PackageObjectSelection(
        _require_ref(raw.get("asset_ref"), f"selection.{role}.asset_ref"),
        digest,
        str(raw.get("role", role)),
        None
        if size_value is None
        else _require_nonnegative_int(size_value, f"selection.{role}.size"),
        tuple(
            _mapping(item, f"selection.{role}.parsed_record[{index}]")
            for index, item in enumerate(parsed)
        ),
        tuple(_require_id(item, f"selection.{role}.import_roots") for item in roots),
    )


class _VerifiedPackage(Protocol):
    manifest: object

    def extension_runtime_authority(self) -> object:
        raise NotImplementedError


class _Closeable(Protocol):
    def close(self) -> object: ...


def _attr(value: object, name: str, default: object = None) -> object:
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


def _model_record(value: object) -> Mapping[str, object]:
    if isinstance(value, Mapping):
        return value
    to_record = getattr(value, "to_record", None)
    if callable(to_record):
        return _mapping(to_record(), "model")
    dump = getattr(value, "model_dump", None)
    if callable(dump):
        result = dump(mode="python", by_alias=True, exclude_none=False)
        return _mapping(result, "model")
    if hasattr(value, "__dict__"):
        return _mapping(vars(value), "model")
    raise _refuse(
        "extension_package_object_mismatch",
        "public metadata object is not serializable",
    )


def _manifest_assets(package: _VerifiedPackage) -> Mapping[str, object]:
    manifest = getattr(package, "manifest", None)
    assets = _attr(manifest, "assets")
    if (
        assets is None
        or isinstance(assets, (str, bytes))
        or not isinstance(assets, Sequence)
    ):
        raise _refuse(
            "extension_package_object_mismatch",
            "verified package does not expose public asset metadata",
            path="manifest.assets",
        )
    result: dict[str, object] = {}
    for item in assets:
        asset = _attr(item, "asset")
        ref = _attr(asset, "qualified_id")
        locator = _attr(item, "locator")
        digest = _attr(locator, "object_digest")
        if not isinstance(ref, str) or not isinstance(digest, str):
            raise _refuse(
                "extension_package_object_mismatch",
                "asset metadata is incomplete",
                path="manifest.assets",
            )
        if ref in result:
            raise _refuse(
                "extension_package_object_mismatch",
                "duplicate asset reference",
                path=ref,
            )
        result[ref] = item
    return result


def _read_selected(
    runtime_authority: object,
    selection: PackageObjectSelection,
    assets: Mapping[str, object],
) -> tuple[PackageObjectSelection, bytes]:
    item = assets.get(selection.asset_ref)
    if item is None:
        raise _refuse(
            "extension_package_object_mismatch",
            "selected asset is not in the verified package",
            path=selection.asset_ref,
        )
    locator = _attr(item, "locator")
    declared = _require_digest(
        _attr(locator, "object_digest"), f"{selection.asset_ref}.raw_blob"
    )
    if declared != selection.raw_blob_digest:
        raise _refuse(
            "extension_package_object_mismatch",
            "selected raw digest disagrees with verified asset",
            path=selection.asset_ref,
        )
    try:
        reader = getattr(runtime_authority, "read_verified_object", None)
        if not callable(reader):
            raise TypeError("runtime authority has no indexed object reader")
        payload = reader(declared)
    except (
        Exception
    ) as exc:  # boundary normalizes package-reader errors without leaking paths
        raise _refuse(
            "extension_package_object_mismatch",
            "verified asset could not be read",
            path=selection.asset_ref,
        ) from exc
    if not isinstance(payload, bytes) or raw_blob_digest(payload) != declared:
        raise _refuse(
            "extension_package_object_mismatch",
            "verified asset bytes changed",
            path=selection.asset_ref,
        )
    return selection.with_verified_size(len(payload)), payload


def _safe_member(name: str, path: str) -> str:
    if (
        not name
        or "\x00" in name
        or "\\" in name
        or name.startswith("/")
        or re.match(r"^[A-Za-z]:", name)
    ):
        raise _refuse(
            "extension_dependency_closure_invalid",
            "wheel member path is unsafe",
            path=path,
        )
    parts = name.split("/")
    if (
        any(not part or part in {".", ".."} for part in parts)
        or posixpath.normpath(name) != name
    ):
        raise _refuse(
            "extension_dependency_closure_invalid",
            "wheel member path is not normalized",
            path=path,
        )
    return name


def _header_values(payload: bytes, path: str) -> dict[str, list[str]]:
    try:
        text = payload.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise _refuse(
            "extension_dependency_closure_invalid",
            "wheel metadata is not UTF-8",
            path=path,
        ) from exc
    result: dict[str, list[str]] = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        if ":" not in line:
            raise _refuse(
                "extension_dependency_closure_invalid",
                "wheel metadata header is malformed",
                path=path,
            )
        key, value = line.split(":", 1)
        key, value = key.strip(), value.strip()
        if not key or "\x00" in key or "\x00" in value:
            raise _refuse(
                "extension_dependency_closure_invalid",
                "wheel metadata header is malformed",
                path=path,
            )
        result.setdefault(key.lower(), []).append(value)
    return result


def _normalize_distribution(value: object, path: str) -> str:
    name = _require_id(value, path)
    normalized = re.sub(r"[-_.]+", "-", name).lower()
    if not normalized or not _ID_RE.fullmatch(normalized):
        raise _refuse(
            "extension_dependency_closure_invalid",
            "distribution name is invalid",
            path=path,
        )
    return normalized


def _wheel_projection(
    selection: PackageObjectSelection, payload: bytes
) -> WheelProjection:
    if not zipfile.is_zipfile(io.BytesIO(payload)):
        raise _refuse(
            "extension_dependency_closure_invalid",
            "selected extension object is not a wheel",
            path=selection.asset_ref,
        )
    try:
        archive = zipfile.ZipFile(io.BytesIO(payload), "r")
    except (OSError, zipfile.BadZipFile) as exc:
        raise _refuse(
            "extension_dependency_closure_invalid",
            "wheel archive is invalid",
            path=selection.asset_ref,
        ) from exc
    with archive:
        names: list[str] = []
        infos: dict[str, zipfile.ZipInfo] = {}
        for info in archive.infolist():
            name = _safe_member(info.filename, f"{selection.asset_ref}.wheel")
            if name in infos:
                raise _refuse(
                    "extension_dependency_closure_invalid",
                    "wheel contains duplicate member paths",
                    path=name,
                )
            mode = (info.external_attr >> 16) & 0o170000
            if info.is_dir() or mode not in {0, 0o100000}:
                raise _refuse(
                    "extension_dependency_closure_invalid",
                    "wheel contains non-regular member",
                    path=name,
                )
            infos[name] = info
            names.append(name)
        dist_info = sorted(name for name in names if name.endswith(".dist-info/WHEEL"))
        metadata_info = sorted(
            name for name in names if name.endswith(".dist-info/METADATA")
        )
        record_info = sorted(
            name for name in names if name.endswith(".dist-info/RECORD")
        )
        if len(dist_info) != 1 or len(metadata_info) != 1 or len(record_info) != 1:
            raise _refuse(
                "extension_dependency_closure_invalid",
                "wheel must contain exactly one WHEEL, METADATA, and RECORD",
                path=selection.asset_ref,
            )
        wheel_headers = _header_values(archive.read(dist_info[0]), dist_info[0])
        metadata_headers = _header_values(
            archive.read(metadata_info[0]), metadata_info[0]
        )
        names_value = metadata_headers.get("name")
        versions = metadata_headers.get("version")
        if (
            not names_value
            or len(names_value) != 1
            or not versions
            or len(versions) != 1
        ):
            raise _refuse(
                "extension_dependency_closure_invalid",
                "wheel metadata lacks unique Name/Version",
                path=metadata_info[0],
            )
        normalized_name = _normalize_distribution(
            names_value[0], f"{metadata_info[0]}.Name"
        )
        version = _require_id(versions[0], f"{metadata_info[0]}.Version")
        tags = tuple(sorted(set(wheel_headers.get("tag", ()))))
        if not tags or any(not _WHEEL_TAG_RE.fullmatch(tag) for tag in tags):
            raise _refuse(
                "extension_dependency_closure_invalid",
                "wheel has no valid Tag metadata",
                path=dist_info[0],
            )
        try:
            record_text = archive.read(record_info[0]).decode("utf-8", errors="strict")
        except UnicodeDecodeError as exc:
            raise _refuse(
                "extension_dependency_closure_invalid",
                "wheel RECORD is not UTF-8",
                path=record_info[0],
            ) from exc
        entries: list[WheelRecordEntry] = []
        seen: set[str] = set()
        for row_index, row in enumerate(
            csv.reader(io.StringIO(record_text), strict=True)
        ):
            if len(row) != 3:
                raise _refuse(
                    "extension_dependency_closure_invalid",
                    "wheel RECORD row must have three fields",
                    path=f"{record_info[0]}[{row_index}]",
                )
            name, hash_value, size_value = row
            _safe_member(name, f"{record_info[0]}[{row_index}].path")
            if name in seen:
                raise _refuse(
                    "extension_dependency_closure_invalid",
                    "wheel RECORD has duplicate member",
                    path=name,
                )
            seen.add(name)
            if name != record_info[0] and name not in infos:
                raise _refuse(
                    "extension_dependency_closure_invalid",
                    "wheel RECORD names missing member",
                    path=name,
                )
            actual = archive.read(name) if name in infos else b""
            if name == record_info[0]:
                if hash_value or size_value:
                    raise _refuse(
                        "extension_dependency_closure_invalid",
                        "wheel RECORD must leave itself unhashed",
                        path=name,
                    )
                entries.append(WheelRecordEntry(name, None))
                continue
            if not hash_value.startswith("sha256=") or not _BASE64_HASH_RE.fullmatch(
                hash_value[7:]
            ):
                raise _refuse(
                    "extension_dependency_closure_invalid",
                    "wheel RECORD must use sha256 hashes",
                    path=name,
                )
            try:
                expected = base64.urlsafe_b64decode(
                    hash_value[7:] + "=" * (-len(hash_value[7:]) % 4)
                )
                expected_digest = (
                    "sha256:" + expected.hex() if len(expected) == 32 else ""
                )
            except (ValueError, binascii.Error) as exc:  # type: ignore[name-defined]
                raise _refuse(
                    "extension_dependency_closure_invalid",
                    "wheel RECORD hash is malformed",
                    path=name,
                ) from exc
            if expected_digest != raw_blob_digest(actual) or size_value != str(
                len(actual)
            ):
                raise _refuse(
                    "extension_dependency_closure_invalid",
                    "wheel RECORD does not match member bytes",
                    path=name,
                )
            entries.append(
                WheelRecordEntry(name, RawBlobRef(raw_blob_digest(actual), len(actual)))
            )
        if set(infos) != seen:
            raise _refuse(
                "extension_dependency_closure_invalid",
                "wheel RECORD does not cover every member",
                path=record_info[0],
            )
        entries = sorted(entries, key=lambda entry: entry.path)
        record_record = {
            "schema_version": WHEEL_RECORD_V1_SCHEMA,
            "wheel_raw_blob": selection.raw_blob_ref.to_record(),
            "entries": [entry.to_record() for entry in entries],
        }
        roots: set[str] = set()
        for name in names:
            if ".dist-info/" in name or ".data/" in name:
                continue
            root = name.split("/", 1)[0].removesuffix(".py")
            if _PY_IDENT_RE.fullmatch(root):
                roots.add(root)
        return WheelProjection(
            selection=selection,
            normalized_name=normalized_name,
            version=version,
            wheel_tags=tags,
            record_entries=tuple(entries),
            wheel_record_manifest_digest=CanonicalRecordRef(
                WHEEL_RECORD_V1_SCHEMA, canonical_record_digest(record_record)
            ),
            import_roots=tuple(sorted(roots)),
        )


def _descriptor_entry(
    raw: Mapping[str, object], index: int
) -> tuple[dict[str, object], ExtensionEntryV2]:
    path = f"descriptor.entries[{index}]"
    _exact_keys(
        raw,
        {
            "extension_id",
            "kind",
            "module",
            "function",
            "public_selector",
            "subject",
            "declared_capabilities",
            "requirements",
            "provider_effects",
            "output_policy",
        },
        path,
    )
    extension_id = _require_id(raw.get("extension_id"), f"{path}.extension_id")
    kind = _require_id(raw.get("kind"), f"{path}.kind")
    module = _require_id(raw.get("module"), f"{path}.module")
    if any(not _PY_IDENT_RE.fullmatch(part) for part in module.split(".")):
        raise _refuse(
            "extension_entry_metadata_mismatch",
            "module is not an importable identifier",
            path=f"{path}.module",
        )
    function = _require_id(raw.get("function"), f"{path}.function")
    if not _PY_IDENT_RE.fullmatch(function):
        raise _refuse(
            "extension_entry_metadata_mismatch",
            "function is not an identifier",
            path=f"{path}.function",
        )
    selector = dict(_mapping(raw.get("public_selector"), f"{path}.public_selector"))
    _exact_keys(
        selector,
        {"api_version", "entry_point_group", "entry_point_name"},
        f"{path}.public_selector",
    )
    for key in selector:
        _require_id(selector[key], f"{path}.public_selector.{key}")
    subject = dict(_mapping(raw.get("subject"), f"{path}.subject"))
    _exact_keys(
        subject,
        {"owner", "ref", "kind", "version", "typed_asset_digest"},
        f"{path}.subject",
    )
    for key in ("owner", "ref", "kind"):
        _require_id(subject[key], f"{path}.subject.{key}")
    if subject["version"] is not None:
        _require_id(subject["version"], f"{path}.subject.version")
    _require_digest(subject["typed_asset_digest"], f"{path}.subject.typed_asset_digest")
    capabilities_raw = raw.get("declared_capabilities")
    if isinstance(capabilities_raw, (str, bytes)) or not isinstance(
        capabilities_raw, Sequence
    ):
        raise _refuse(
            "extension_descriptor_invalid",
            "declared_capabilities must be a sequence",
            path=f"{path}.declared_capabilities",
        )
    capabilities = tuple(
        _require_id(item, f"{path}.declared_capabilities") for item in capabilities_raw
    )
    if capabilities != tuple(sorted(set(capabilities))):
        raise _refuse(
            "extension_descriptor_invalid",
            "declared_capabilities must be unique and sorted",
            path=f"{path}.declared_capabilities",
        )
    requirements = dict(_mapping(raw.get("requirements"), f"{path}.requirements"))
    provider_effects = dict(
        _mapping(raw.get("provider_effects"), f"{path}.provider_effects")
    )
    output_policy = dict(_mapping(raw.get("output_policy"), f"{path}.output_policy"))
    entry_record = {
        "schema_version": "okg.extension_runtime.descriptor_entry/v1",
        "extension_id": extension_id,
        "kind": kind,
        "module": module,
        "function": function,
        "public_selector": selector,
        "subject": subject,
        "declared_capabilities": list(capabilities),
        "requirements": requirements,
        "provider_effects": provider_effects,
        "output_policy": output_policy,
    }
    return entry_record, ExtensionEntryV2(
        extension_id,
        kind,
        module,
        function,
        selector,
        subject,
        capabilities,
        requirements,
        provider_effects,
        output_policy,
        CanonicalRecordRef(
            "okg.extension_runtime.descriptor_entry/v1",
            canonical_record_digest(entry_record),
        ),
        CanonicalRecordRef(
            "okg.extension_runtime.extension_selection/v1", "sha256:" + "0" * 64
        ),
    )


def _parse_descriptor(
    selection: PackageObjectSelection, payload: bytes
) -> ExtensionPackageDescriptorV2:
    raw = _read_json(payload, selection.asset_ref)
    _exact_keys(raw, {"schema_version", "package_ref", "entries"}, "descriptor")
    if raw.get("schema_version") != EXTENSION_PACKAGE_V2_SCHEMA:
        raise _refuse(
            "extension_contract_version_unsupported",
            "descriptor schema is not okg.extension_package/v2",
            path="descriptor.schema_version",
        )
    package_ref = dict(_mapping(raw.get("package_ref"), "descriptor.package_ref"))
    _exact_keys(
        package_ref,
        {"owner", "ref", "kind", "version", "raw_blob"},
        "descriptor.package_ref",
    )
    for key in ("owner", "ref", "kind", "version"):
        _require_id(package_ref[key], f"descriptor.package_ref.{key}")
    if package_ref["kind"] != "wheel":
        raise _refuse(
            "extension_package_object_mismatch",
            "descriptor package_ref.kind must be wheel",
            path="descriptor.package_ref.kind",
        )
    package_ref["raw_blob"] = RawBlobRef.from_value(
        package_ref["raw_blob"], path="descriptor.package_ref.raw_blob"
    ).to_record()
    entries_raw = raw.get("entries")
    if (
        isinstance(entries_raw, (str, bytes))
        or not isinstance(entries_raw, Sequence)
        or not entries_raw
    ):
        raise _refuse(
            "extension_entry_metadata_mismatch",
            "descriptor entries must be a non-empty array",
            path="descriptor.entries",
        )
    entries: list[ExtensionEntryV2] = []
    records: list[dict[str, object]] = []
    seen_ids: set[str] = set()
    selectors: set[tuple[str, str, str]] = set()
    for index, item in enumerate(entries_raw):
        record, entry = _descriptor_entry(
            _mapping(item, f"descriptor.entries[{index}]"), index
        )
        if entry.extension_id in seen_ids:
            raise _refuse(
                "extension_entry_metadata_mismatch",
                "descriptor extension IDs must be unique",
                path=f"descriptor.entries[{index}]",
            )
        selector_key = (
            str(entry.public_selector["api_version"]),
            str(entry.public_selector["entry_point_group"]),
            str(entry.public_selector["entry_point_name"]),
        )
        if selector_key in selectors:
            raise _refuse(
                "extension_entry_metadata_mismatch",
                "descriptor public selectors must be unique",
                path=f"descriptor.entries[{index}]",
            )
        seen_ids.add(entry.extension_id)
        selectors.add(selector_key)
        entries.append(entry)
        records.append(record)
    if tuple(item.extension_id for item in entries) != tuple(
        sorted(item.extension_id for item in entries)
    ):
        raise _refuse(
            "extension_descriptor_invalid",
            "descriptor entries must be sorted by extension_id",
            path="descriptor.entries",
        )
    return ExtensionPackageDescriptorV2(
        package_ref,
        tuple(entries),
        RawBlobRef(raw_blob_digest(payload), len(payload)),
        raw,
    )


def _public_extension_record(
    package: _VerifiedPackage, entry: ExtensionEntryV2
) -> Mapping[str, object]:
    manifest = getattr(package, "manifest", None)
    records = _attr(manifest, "public_extensions")
    if records is not None:
        if isinstance(records, (str, bytes)) or not isinstance(records, Sequence):
            raise _refuse(
                "extension_entry_metadata_mismatch",
                "verified public extension metadata is invalid",
                path="manifest.public_extensions",
            )
        candidates = tuple(records)
        if not candidates:
            raise _refuse(
                "extension_entry_metadata_mismatch",
                "verified package has no public extension metadata",
                path="manifest.public_extensions",
            )
        selected = None
        for item in candidates:
            if (
                _attr(item, "api_version") == entry.public_selector["api_version"]
                and _attr(item, "entry_point_group")
                == entry.public_selector["entry_point_group"]
                and _attr(item, "entry_point_name")
                == entry.public_selector["entry_point_name"]
            ):
                if selected is not None:
                    raise _refuse(
                        "extension_entry_metadata_mismatch",
                        "public selector resolves to multiple metadata records",
                        path="manifest.public_extensions",
                    )
                selected = item
        if selected is None:
            raise _refuse(
                "extension_entry_metadata_mismatch",
                "public selector does not resolve to metadata",
                path="manifest.public_extensions",
            )
    else:
        raise _refuse(
            "extension_entry_metadata_mismatch",
            "public extension metadata is required",
            path="manifest.public_extensions",
        )
    record = _model_record(selected)
    allowed = {
        "subject",
        "package_ref",
        "api_version",
        "entry_point_group",
        "entry_point_name",
        "capabilities",
        "placeholders",
    }
    _exact_keys(record, allowed, "public_extension")
    return record


def _verify_selector(
    entry: ExtensionEntryV2,
    metadata: Mapping[str, object],
    descriptor: ExtensionPackageDescriptorV2,
) -> None:
    subject = _model_record(metadata.get("subject"))
    package_ref = _model_record(metadata.get("package_ref"))
    if tuple(
        metadata.get(key)
        for key in ("api_version", "entry_point_group", "entry_point_name")
    ) != tuple(
        entry.public_selector[key]
        for key in ("api_version", "entry_point_group", "entry_point_name")
    ):
        raise _refuse(
            "extension_entry_metadata_mismatch",
            "descriptor selector differs from verified metadata",
            path="public_extension",
        )
    capability_values = metadata.get("capabilities", ())
    if isinstance(capability_values, (str, bytes)) or not isinstance(
        capability_values, Sequence
    ):
        raise _refuse(
            "extension_entry_metadata_mismatch",
            "verified extension capabilities are invalid",
            path="public_extension.capabilities",
        )
    capabilities = tuple(sorted(str(item) for item in capability_values))
    if capabilities != entry.declared_capabilities:
        raise _refuse(
            "extension_entry_metadata_mismatch",
            "descriptor capabilities differ from verified metadata",
            path="public_extension.capabilities",
        )
    expected_subject = {
        "owner": subject.get("owner"),
        "ref": subject.get("ref", subject.get("id")),
        "kind": subject.get("kind"),
        "version": subject.get("version"),
        "typed_asset_digest": subject.get("digest"),
    }
    if expected_subject != dict(entry.subject):
        raise _refuse(
            "extension_entry_metadata_mismatch",
            "descriptor subject differs from verified metadata",
            path="public_extension.subject",
        )
    expected_package = {
        "owner": package_ref.get("owner"),
        "ref": package_ref.get("ref", package_ref.get("id")),
        "kind": package_ref.get("kind"),
        "version": package_ref.get("version"),
        "raw_blob": RawBlobRef(
            _require_digest(
                package_ref.get("digest"), "public_extension.package_ref.digest"
            ),
            0,
        ).to_record(),
    }
    descriptor_package = dict(descriptor.package_ref)
    descriptor_raw = RawBlobRef.from_value(
        descriptor_package["raw_blob"], path="descriptor.package_ref.raw_blob"
    )
    expected_package["raw_blob"] = {
        "digest_domain": RAW_DIGEST_DOMAIN,
        "digest": descriptor_raw.digest,
        "size": descriptor_raw.size,
    }
    if any(
        expected_package[key] != descriptor_package[key]
        for key in ("owner", "ref", "kind", "version")
    ) or descriptor_raw.digest != _require_digest(
        package_ref.get("digest"), "public_extension.package_ref.digest"
    ):
        raise _refuse(
            "extension_entry_metadata_mismatch",
            "descriptor package reference differs from verified metadata",
            path="public_extension.package_ref",
        )


def _distribution_extension_record(
    entry: ExtensionEntryV2,
    descriptor: ExtensionPackageDescriptorV2,
    metadata: Mapping[str, object],
) -> Mapping[str, object]:
    return {
        "schema_version": "okg.extension_runtime.distribution_extension_record/v1",
        "public_selector": dict(entry.public_selector),
        "subject": dict(entry.subject),
        "package_ref": dict(descriptor.package_ref),
        "capabilities": list(entry.declared_capabilities),
        "placeholders": metadata.get("placeholders"),
    }


def _dependency_lock(
    selection: PackageObjectSelection, payload: bytes
) -> tuple[Mapping[str, object], ...]:
    raw = _read_json(payload, selection.asset_ref)
    _exact_keys(raw, {"schema_version", "dependencies"}, "dependency_lock")
    if raw.get("schema_version") != DEPENDENCY_LOCK_V1_SCHEMA:
        raise _refuse(
            "extension_dependency_closure_invalid",
            "dependency lock schema is unsupported",
            path="dependency_lock.schema_version",
        )
    values = raw.get("dependencies")
    if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
        raise _refuse(
            "extension_dependency_closure_invalid",
            "dependency lock dependencies must be an array",
            path="dependency_lock.dependencies",
        )
    entries: list[Mapping[str, object]] = []
    for index, item in enumerate(values):
        path = f"dependency_lock.dependencies[{index}]"
        mapping = dict(_mapping(item, path))
        _exact_keys(
            mapping,
            {"normalized_name", "version", "asset_ref", "raw_blob", "wheel_tags"},
            path,
        )
        normalized = _normalize_distribution(
            mapping["normalized_name"], f"{path}.normalized_name"
        )
        if normalized != mapping["normalized_name"]:
            raise _refuse(
                "extension_dependency_closure_invalid",
                "dependency names must already be normalized",
                path=f"{path}.normalized_name",
            )
        mapping["version"] = _require_id(mapping["version"], f"{path}.version")
        mapping["asset_ref"] = _require_ref(mapping["asset_ref"], f"{path}.asset_ref")
        mapping["raw_blob"] = RawBlobRef.from_value(
            mapping["raw_blob"], path=f"{path}.raw_blob"
        ).to_record()
        tags = mapping["wheel_tags"]
        if isinstance(tags, (str, bytes)) or not isinstance(tags, Sequence) or not tags:
            raise _refuse(
                "extension_dependency_closure_invalid",
                "dependency wheel_tags must be non-empty",
                path=f"{path}.wheel_tags",
            )
        tag_values = tuple(_require_id(tag, f"{path}.wheel_tags") for tag in tags)
        if tag_values != tuple(sorted(set(tag_values))):
            raise _refuse(
                "extension_dependency_closure_invalid",
                "dependency wheel_tags must be unique and sorted",
                path=f"{path}.wheel_tags",
            )
        mapping["wheel_tags"] = list(tag_values)
        entries.append(mapping)
    keys = [(item["normalized_name"], item["version"]) for item in entries]
    if len(keys) != len(set(keys)) or keys != sorted(keys):
        raise _refuse(
            "extension_dependency_closure_invalid",
            "dependency lock must be unique and sorted",
            path="dependency_lock.dependencies",
        )
    return tuple(entries)


def _identity_mapping(value: object, path: str) -> RuntimeToolchainIdentity:
    if isinstance(value, RuntimeToolchainIdentity):
        return value
    raw = dict(_mapping(value, path))
    supplied_schema = raw.pop("schema_version", None)
    if supplied_schema != TOOLCHAIN_V2_SCHEMA:
        raise _refuse(
            "extension_runtime_toolchain_mismatch",
            "runtime toolchain schema is unsupported",
            path=f"{path}.schema_version",
        )
    supplied_manifest_digest = raw.pop("runtime_toolchain_manifest_digest", None)
    required = {
        "sealed_release_artifact_id",
        "sealed_release_tree_digest",
        "release_runtime_authority_receipt_digest",
        "platform_identity",
        "python_abi",
        "interpreter_raw_blob",
        "distribution_prefix_tree_digest",
        "prefix_copy_proof_digest",
        "stdlib_manifest_digest",
        "runtime_layout_digest",
        "public_sdk_abi",
        "public_sdk_raw_blob",
        "child_host_raw_blob",
        "bootstrap_verifier_raw_blob",
        "builder_raw_blob",
        "broker_raw_blob",
        "containment_launcher_raw_blob",
        "containment_profile_manifest_digest",
    }
    if set(raw) != required:
        raise _refuse(
            "extension_runtime_toolchain_mismatch",
            "runtime toolchain fields are not exact",
            path=path,
        )

    def raw_ref(name: str) -> RawBlobRef:
        try:
            return RawBlobRef.from_value(raw[name], path=f"{path}.{name}")
        except ExtensionPackageV2Error as exc:
            raise _refuse(
                "extension_runtime_toolchain_mismatch",
                "runtime toolchain raw reference is invalid",
                path=f"{path}.{name}",
            ) from exc

    def canonical_ref(name: str, schema: str) -> CanonicalRecordRef:
        try:
            return _canonical_ref(raw[name], schema, f"{path}.{name}")
        except ExtensionPackageV2Error as exc:
            raise _refuse(
                "extension_runtime_toolchain_mismatch",
                "runtime toolchain canonical reference is invalid",
                path=f"{path}.{name}",
            ) from exc

    platform_value = _mapping(raw["platform_identity"], f"{path}.platform_identity")
    if any(not isinstance(item, str) for item in platform_value.values()):
        raise _refuse(
            "extension_runtime_toolchain_mismatch",
            "runtime toolchain platform identity is invalid",
            path=f"{path}.platform_identity",
        )
    platform_identity = {key: cast(str, item) for key, item in platform_value.items()}
    identity = RuntimeToolchainIdentity(
        _mapping(
            raw["sealed_release_artifact_id"], f"{path}.sealed_release_artifact_id"
        ),
        canonical_ref(
            "sealed_release_tree_digest",
            "okg.sealed_release.tree/v1",
        ),
        canonical_ref(
            "release_runtime_authority_receipt_digest",
            "okg.sealed_release.runtime_authority/v2",
        ),
        platform_identity,
        _require_id(raw["python_abi"], f"{path}.python_abi"),
        raw_ref("interpreter_raw_blob"),
        canonical_ref(
            "distribution_prefix_tree_digest",
            "okg.sealed_release.runtime_distribution_prefix_tree/v1",
        ),
        canonical_ref(
            "prefix_copy_proof_digest",
            "okg.sealed_release.runtime_prefix_copy_proof/v1",
        ),
        canonical_ref(
            "stdlib_manifest_digest",
            "okg.extension_runtime.stdlib_manifest/v1",
        ),
        canonical_ref(
            "runtime_layout_digest",
            "okg.extension_runtime.layout/v2",
        ),
        _require_id(raw["public_sdk_abi"], f"{path}.public_sdk_abi"),
        raw_ref("public_sdk_raw_blob"),
        raw_ref("child_host_raw_blob"),
        raw_ref("bootstrap_verifier_raw_blob"),
        raw_ref("builder_raw_blob"),
        raw_ref("broker_raw_blob"),
        raw_ref("containment_launcher_raw_blob"),
        canonical_ref(
            "containment_profile_manifest_digest",
            "okg.extension_runtime.containment_profile/v1",
        ),
    )
    if supplied_manifest_digest is not None:
        expected = identity.runtime_toolchain_manifest_digest.digest
        try:
            supplied = _canonical_ref(
                supplied_manifest_digest,
                TOOLCHAIN_V2_SCHEMA,
                f"{path}.runtime_toolchain_manifest_digest",
            )
        except ExtensionPackageV2Error as exc:
            raise _refuse(
                "extension_runtime_toolchain_mismatch",
                "runtime toolchain manifest reference is invalid",
                path=f"{path}.runtime_toolchain_manifest_digest",
            ) from exc
        if supplied.digest != expected:
            raise _refuse(
                "extension_runtime_toolchain_mismatch",
                "runtime toolchain manifest digest is stale",
                path=f"{path}.runtime_toolchain_manifest_digest",
            )
    return identity


def _authority_mapping(value: object, path: str) -> VerifiedPackageAuthority:
    if isinstance(value, VerifiedPackageAuthority):
        return value
    raw = _model_record(value)
    _exact_keys(
        raw,
        {
            "distribution_package_schema",
            "manifest",
            "transport_archive",
            "verified_object_index_digest",
            "verified_asset_index_digest",
            "package_identity_index_digest",
            "public_extension_index_digest",
            "verification_profile_digest",
            "package_verification_receipt_digest",
        },
        path,
    )
    return VerifiedPackageAuthority(
        _require_schema(
            raw["distribution_package_schema"], f"{path}.distribution_package_schema"
        ),
        RawBlobRef.from_value(raw["manifest"], path=f"{path}.manifest"),
        RawBlobRef.from_value(
            raw["transport_archive"], path=f"{path}.transport_archive"
        ),
        _canonical_ref(
            raw["verified_object_index_digest"],
            "okg.extension_runtime.verified_object_index/v1",
            f"{path}.verified_object_index_digest",
        ),
        _canonical_ref(
            raw["verified_asset_index_digest"],
            "okg.extension_runtime.verified_asset_index/v1",
            f"{path}.verified_asset_index_digest",
        ),
        _canonical_ref(
            raw["package_identity_index_digest"],
            "okg.extension_runtime.package_identity_index/v1",
            f"{path}.package_identity_index_digest",
        ),
        _canonical_ref(
            raw["public_extension_index_digest"],
            "okg.extension_runtime.public_extension_index/v1",
            f"{path}.public_extension_index_digest",
        ),
        _canonical_ref(
            raw["verification_profile_digest"],
            "okg.distribution_package.verification_profile/v1",
            f"{path}.verification_profile_digest",
        ),
        _canonical_ref(
            raw["package_verification_receipt_digest"],
            "okg.extension_runtime.package_verification_receipt/v1",
            f"{path}.package_verification_receipt_digest",
        ),
    )


@dataclass(frozen=True, slots=True)
class _VerifiedPlanInputs:
    authority: VerifiedPackageAuthority
    descriptor_selection: PackageObjectSelection
    descriptor: ExtensionPackageDescriptorV2
    entry: ExtensionEntryV2
    extension_selection: PackageObjectSelection
    extension_wheel: WheelProjection
    lock_selection: PackageObjectSelection
    dependency_wheels: tuple[WheelProjection, ...]
    dependency_selections: tuple[PackageObjectSelection, ...]
    sbom: PackageObjectSelection
    provenance: PackageObjectSelection
    public_metadata: Mapping[str, object]


def _verified_plan_inputs(
    package: _VerifiedPackage,
    selected: PackagePlanSelections,
    *,
    extension_id: str,
) -> _VerifiedPlanInputs:
    package_authority = getattr(package, "extension_runtime_authority", None)
    if not callable(package_authority):
        raise _refuse(
            "extension_package_object_mismatch",
            "verified package reader does not expose runtime authority",
            path="package.extension_runtime_authority",
        )
    try:
        retained = package_authority()
    except Exception as exc:
        raise _refuse(
            "extension_package_object_mismatch",
            "verified package authority receipt is unavailable",
            path="package.extension_runtime_authority",
        ) from exc
    close = getattr(retained, "close", None)
    if not callable(close):
        raise _refuse(
            "extension_package_object_mismatch",
            "verified package authority is not closeable",
            path="package.extension_runtime_authority",
        )
    with closing(cast(_Closeable, retained)) as runtime_authority:
        authority = _authority_mapping(
            runtime_authority, "package.extension_runtime_authority"
        )
        assets = _manifest_assets(package)
        descriptor_selection, descriptor_bytes = _read_selected(
            runtime_authority, selected.descriptor, assets
        )
        descriptor = _parse_descriptor(descriptor_selection, descriptor_bytes)
        selected_extension_id = _require_id(extension_id, "extension_id")
        candidates = tuple(
            item
            for item in descriptor.entries
            if item.extension_id == selected_extension_id
        )
        if len(candidates) != 1:
            raise _refuse(
                "extension_entry_metadata_mismatch",
                "extension_id does not select exactly one descriptor entry",
                path="extension_id",
            )
        entry = candidates[0]
        extension_selection, extension_bytes = _read_selected(
            runtime_authority, selected.extension_wheel, assets
        )
        extension_wheel = _wheel_projection(extension_selection, extension_bytes)
        descriptor_raw = RawBlobRef.from_value(
            descriptor.package_ref["raw_blob"],
            path="descriptor.package_ref.raw_blob",
        )
        if descriptor_raw != extension_wheel.selection.raw_blob_ref:
            raise _refuse(
                "extension_package_object_mismatch",
                "descriptor package raw ref does not match selected wheel",
                path="descriptor.package_ref.raw_blob",
            )
        metadata = _public_extension_record(package, entry)
        _verify_selector(entry, metadata, descriptor)
        lock_selection, lock_bytes = _read_selected(
            runtime_authority, selected.dependency_lock, assets
        )
        lock_values = _dependency_lock(lock_selection, lock_bytes)
        if len(lock_values) != len(selected.dependency_wheels):
            raise _refuse(
                "extension_dependency_closure_invalid",
                "dependency lock and explicit wheel selections differ",
                path="dependency_lock.dependencies",
            )
        dependency_wheels: list[WheelProjection] = []
        dependency_selections: list[PackageObjectSelection] = []
        for index, (locked, selection) in enumerate(
            zip(lock_values, selected.dependency_wheels, strict=True)
        ):
            locked_blob = RawBlobRef.from_value(
                locked["raw_blob"],
                path=f"dependency_lock.dependencies[{index}].raw_blob",
            )
            if (
                locked["asset_ref"] != selection.asset_ref
                or locked_blob.digest != selection.raw_blob_digest
                or (selection.size is not None and locked_blob.size != selection.size)
            ):
                raise _refuse(
                    "extension_dependency_closure_invalid",
                    "dependency selection differs from lock",
                    path=f"dependency_wheels[{index}]",
                )
            dep_selection, dep_bytes = _read_selected(
                runtime_authority, selection, assets
            )
            if locked_blob != dep_selection.raw_blob_ref:
                raise _refuse(
                    "extension_dependency_closure_invalid",
                    "dependency lock raw size differs from verified bytes",
                    path=f"dependency_wheels[{index}]",
                )
            wheel = _wheel_projection(dep_selection, dep_bytes)
            locked_tags = locked.get("wheel_tags")
            if isinstance(locked_tags, (str, bytes)) or not isinstance(
                locked_tags, Sequence
            ):
                raise _refuse(
                    "extension_dependency_closure_invalid",
                    "dependency lock wheel tags are invalid",
                    path=f"dependency_wheels[{index}]",
                )
            if (
                wheel.normalized_name != locked["normalized_name"]
                or wheel.version != locked["version"]
                or tuple(wheel.wheel_tags) != tuple(locked_tags)
            ):
                raise _refuse(
                    "extension_dependency_closure_invalid",
                    "dependency wheel metadata differs from lock",
                    path=f"dependency_wheels[{index}]",
                )
            dependency_selections.append(dep_selection)
            dependency_wheels.append(wheel)
        if selected.sbom is None or selected.provenance is None:
            raise _refuse(
                "extension_package_object_mismatch",
                "SBOM and provenance selections are required",
                path="selections",
            )
        sbom, _ = _read_selected(runtime_authority, selected.sbom, assets)
        provenance, _ = _read_selected(runtime_authority, selected.provenance, assets)
        return _VerifiedPlanInputs(
            authority,
            descriptor_selection,
            descriptor,
            entry,
            extension_selection,
            extension_wheel,
            lock_selection,
            tuple(dependency_wheels),
            tuple(dependency_selections),
            sbom,
            provenance,
            metadata,
        )


def plan_extension_package_v2(
    package: _VerifiedPackage,
    selections: PackagePlanSelections
    | Mapping[str, object]
    | Sequence[PackageObjectSelection | Mapping[str, object]],
    runtime_toolchain: object,
    *,
    extension_id: str,
    platform_tags: Sequence[str] = (),
) -> PackagePlanV2:
    """Plan a v2 package from explicit verified asset selections.

    The sole byte operation is ``read_verified_object(raw_blob)`` on a fresh
    retained package authority. Selection refs must be present in the public
    verified manifest and resolve to the same indexed raw object digest.
    """

    selected = (
        selections
        if isinstance(selections, PackagePlanSelections)
        else PackagePlanSelections.from_value(selections)
    )
    # Local import keeps the data types acyclic while requiring the selector's
    # live sealed-release proof instead of accepting caller-authored identity.
    from okg.substrate.extension_runtime_seal import (
        VerifiedRuntimeToolchain,
        verify_runtime_toolchain,
    )

    if not isinstance(runtime_toolchain, VerifiedRuntimeToolchain):
        raise _refuse(
            "extension_runtime_toolchain_mismatch",
            "planner requires a verified sealed-release toolchain handle",
            path="runtime_toolchain",
        )
    verified_toolchain = verify_runtime_toolchain(runtime_toolchain)
    toolchain = verified_toolchain.identity
    inputs = _verified_plan_inputs(package, selected, extension_id=extension_id)
    authority = inputs.authority
    descriptor_selection = inputs.descriptor_selection
    descriptor = inputs.descriptor
    entry = inputs.entry
    extension_selection = inputs.extension_selection
    extension_wheel = inputs.extension_wheel
    lock_selection = inputs.lock_selection
    dependency_wheels = list(inputs.dependency_wheels)
    verified_dependency_selections = list(inputs.dependency_selections)
    sbom = inputs.sbom
    provenance = inputs.provenance
    metadata = inputs.public_metadata
    runtime_package_set = verified_toolchain.runtime_packages
    if runtime_package_set.public_sdk.wheel_raw_blob != toolchain.public_sdk_raw_blob:
        raise _refuse(
            "extension_sdk_version_mismatch",
            "public SDK projection differs from the toolchain",
        )
    if runtime_package_set.child_host.wheel_raw_blob != toolchain.child_host_raw_blob:
        raise _refuse(
            "extension_runtime_toolchain_mismatch",
            "child-host projection differs from the toolchain",
        )
    extension_record = _distribution_extension_record(entry, descriptor, metadata)
    extension_record_digest = CanonicalRecordRef(
        "okg.extension_runtime.distribution_extension_record/v1",
        canonical_record_digest(extension_record),
    )
    extension_selection_record = {
        "schema_version": "okg.extension_runtime.extension_selection/v1",
        "descriptor_raw_blob": descriptor.raw_blob.to_record(),
        "descriptor_entry_digest": entry.descriptor_entry_digest.to_record(),
        "verified_distribution_extension_record_digest": extension_record_digest.to_record(),
        "public_selector": dict(entry.public_selector),
        "subject": dict(entry.subject),
        "package_ref": dict(descriptor.package_ref),
        "capabilities": list(entry.declared_capabilities),
        "placeholders": metadata.get("placeholders"),
    }
    extension_selection_digest = CanonicalRecordRef(
        "okg.extension_runtime.extension_selection/v1",
        canonical_record_digest(extension_selection_record),
    )
    entry = ExtensionEntryV2(
        entry.extension_id,
        entry.kind,
        entry.module,
        entry.function,
        entry.public_selector,
        entry.subject,
        entry.declared_capabilities,
        entry.requirements,
        entry.provider_effects,
        entry.output_policy,
        entry.descriptor_entry_digest,
        extension_selection_digest,
    )
    selected_object_records = sorted(
        (
            selection.to_record()
            for selection in (
                descriptor_selection,
                extension_selection,
                lock_selection,
                *verified_dependency_selections,
                sbom,
                provenance,
            )
        ),
        key=lambda item: str(item["asset_ref"]).encode("utf-8"),
    )
    verified_input = CanonicalRecordRef(
        "okg.extension_runtime.verified_distribution_input/v1",
        canonical_record_digest(
            {
                "schema_version": "okg.extension_runtime.verified_distribution_input/v1",
                "distribution_package_schema": authority.distribution_package_schema,
                "manifest_raw_blob": authority.manifest_raw_blob.to_record(),
                "transport_archive_raw_blob": authority.transport_archive_raw_blob.to_record(),
                "selected_objects": selected_object_records,
                "package_verification_receipt_digest": authority.package_verification_receipt_digest.to_record(),
                "distribution_extension_record_digest": extension_record_digest.to_record(),
                "extension_selection_digest": extension_selection_digest.to_record(),
            }
        ),
    )
    tags = tuple(_require_id(tag, "platform_tags") for tag in platform_tags)
    if tags != tuple(sorted(set(tags))):
        raise _refuse(
            "extension_platform_mismatch",
            "platform tags must be unique and sorted",
            path="platform_tags",
        )
    if not tags:
        raise _refuse(
            "extension_platform_mismatch",
            "explicit compatible platform tags are required",
            path="platform_tags",
        )
    for wheel in (extension_wheel, *dependency_wheels):
        if not set(wheel.wheel_tags).intersection(tags):
            raise _refuse(
                "extension_platform_mismatch",
                "wheel has no admitted target tag",
                path=wheel.selection.asset_ref,
            )
    import_policy_record = {
        "schema_version": IMPORT_POLICY_V1_SCHEMA,
        "python_abi": toolchain.python_abi,
        "stdlib_manifest_digest": toolchain.stdlib_manifest_digest.to_record(),
        "runtime_layout_digest": toolchain.runtime_layout_digest.to_record(),
        "selected_extension_distribution": extension_wheel.to_record(),
        "public_sdk_distribution": runtime_package_set.public_sdk.to_record(),
        "child_host_distribution": runtime_package_set.child_host.to_record(),
        "locked_dependency_distributions": [
            wheel.to_record() for wheel in dependency_wheels
        ],
        "allowed_import_roots_by_distribution": [
            {
                "asset_ref": extension_wheel.selection.asset_ref,
                "roots": list(extension_wheel.import_roots),
            }
        ]
        + [
            {"asset_ref": wheel.selection.asset_ref, "roots": list(wheel.import_roots)}
            for wheel in dependency_wheels
        ],
        "denied_import_prefixes": ["okg", "okg.substrate", "okg.deployment"],
    }
    import_policy = CanonicalRecordRef(
        IMPORT_POLICY_V1_SCHEMA, canonical_record_digest(import_policy_record)
    )
    containment = toolchain.containment_profile_manifest_digest
    provisional = PackagePlanV2(
        descriptor,
        descriptor_selection,
        entry,
        extension_record_digest,
        verified_input,
        extension_wheel,
        lock_selection,
        tuple(dependency_wheels),
        sbom,
        provenance,
        authority,
        runtime_package_set,
        toolchain,
        import_policy,
        containment,
        toolchain.python_abi,
        tags,
        toolchain.public_sdk_abi,
        CanonicalRecordRef(PACKAGE_PLAN_V1_SCHEMA, "sha256:" + "0" * 64),
    )
    return PackagePlanV2(
        descriptor,
        descriptor_selection,
        entry,
        extension_record_digest,
        verified_input,
        extension_wheel,
        lock_selection,
        tuple(dependency_wheels),
        sbom,
        provenance,
        authority,
        runtime_package_set,
        toolchain,
        import_policy,
        containment,
        toolchain.python_abi,
        tags,
        toolchain.public_sdk_abi,
        CanonicalRecordRef(
            PACKAGE_PLAN_V1_SCHEMA,
            canonical_record_digest(provisional.to_record_without_digest()),
        ),
    )


plan_package_v2 = plan_extension_package_v2


def package_plan_digest(plan: PackagePlanV2) -> CanonicalRecordRef:
    """Recompute the typed package-plan digest from its complete authority record."""

    if not isinstance(plan, PackagePlanV2):
        raise TypeError("package_plan_digest requires PackagePlanV2")
    return CanonicalRecordRef(
        PACKAGE_PLAN_V1_SCHEMA, canonical_record_digest(plan.to_record_without_digest())
    )


def verify_package_plan_v2(
    plan: PackagePlanV2,
    *,
    package: _VerifiedPackage | None = None,
) -> PackagePlanV2:
    """Fail closed on a stale plan and optionally re-open every selected object."""

    if package_plan_digest(plan) != plan.package_plan_digest:
        raise _refuse(
            "extension_package_object_mismatch",
            "package plan digest is stale",
            path="package_plan_digest",
        )
    if package is not None:
        package_authority = getattr(package, "extension_runtime_authority", None)
        if not callable(package_authority):
            raise _refuse(
                "extension_package_object_mismatch",
                "verified package reader lost runtime authority",
                path="package.extension_runtime_authority",
            )
        try:
            retained = package_authority()
        except Exception as exc:
            raise _refuse(
                "extension_package_object_mismatch",
                "verified package authority receipt is unavailable",
                path="package.extension_runtime_authority",
            ) from exc
        close = getattr(retained, "close", None)
        if not callable(close):
            raise _refuse(
                "extension_package_object_mismatch",
                "verified package authority is not closeable",
                path="package.extension_runtime_authority",
            )
        with closing(cast(_Closeable, retained)) as runtime_authority:
            observed_authority = _authority_mapping(
                runtime_authority, "package.extension_runtime_authority"
            )
            if observed_authority != plan.verified_package_authority:
                raise _refuse(
                    "extension_package_object_mismatch",
                    "verified package authority changed after planning",
                    path="package.extension_runtime_authority",
                )
            assets = _manifest_assets(package)
            selected = (
                plan.descriptor_selection,
                plan.extension_wheel.selection,
                plan.dependency_lock,
                *(wheel.selection for wheel in plan.dependency_wheels),
                plan.sbom,
                plan.provenance,
            )
            for selection in selected:
                _read_selected(runtime_authority, selection, assets)
        metadata = _public_extension_record(package, plan.selected_entry)
        _verify_selector(plan.selected_entry, metadata, plan.descriptor)
        observed_extension_record = CanonicalRecordRef(
            "okg.extension_runtime.distribution_extension_record/v1",
            canonical_record_digest(
                _distribution_extension_record(
                    plan.selected_entry, plan.descriptor, metadata
                )
            ),
        )
        if observed_extension_record != plan.distribution_extension_record_digest:
            raise _refuse(
                "extension_entry_metadata_mismatch",
                "verified public-extension metadata changed after planning",
                path="manifest.public_extensions",
            )
    return plan


__all__ = [
    "CANONICAL_DIGEST_DOMAIN",
    "CanonicalRecordDigest",
    "CanonicalRecordRef",
    "DEPENDENCY_LOCK_V1_SCHEMA",
    "EXTENSION_PACKAGE_V2_SCHEMA",
    "ExtensionEntryV2",
    "ExtensionPackageDescriptorV2",
    "ExtensionPackageV2Error",
    "PackageObjectSelection",
    "PackagePlanSelections",
    "PackagePlanV2",
    "RawBlobRef",
    "RAW_DIGEST_DOMAIN",
    "RuntimeToolchainIdentity",
    "RuntimeDistributionProjection",
    "RuntimePackageProjections",
    "VerifiedPackageAuthority",
    "WheelProjection",
    "WheelRecordEntry",
    "canonical_json_bytes",
    "canonical_record_digest",
    "plan_extension_package_v2",
    "plan_package_v2",
    "package_plan_digest",
    "raw_blob_digest",
    "verify_package_plan_v2",
]
