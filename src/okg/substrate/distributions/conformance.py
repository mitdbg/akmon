"""Bind verified distribution package selections to versioned conformance requests.

The package verifier and the external-distribution conformance protocol remain
separate authorities.  This module supplies their inert handoff: a closed,
path-free record derived from already verified package bytes, plus an equality
binding to the existing request schema.  It never reads an authored definition,
imports a packaged object, or performs deployment lifecycle work.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any, ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from okg.substrate.contracts.deployment_product.conformance import (
    CONFORMANCE_PROTOCOL,
    CONFORMANCE_PROTOCOL_V2,
    ConformanceArtifactIdentityV2,
    ConformanceSubjectV2,
    load_conformance_request,
    ConformanceArtifactIdentity,
    ConformanceDefinitionReference,
    ConformanceSelection,
    ConformanceSubject,
    ConformanceSubjectKind,
    ExternalDistributionConformanceRequest,
)
from okg.substrate.contracts.deployment_product.lock import (
    canonical_json_bytes,
    sha256_digest,
)
from okg.substrate.contracts.deployment_product.models import (
    DistributionRelease,
    OwnerQualifiedReference,
    ProductBundle,
)
from okg.substrate.contracts.deployment_product.typed_assets import TYPED_ASSET_SCHEMA

from .package import (
    SCHEMA as DISTRIBUTION_PACKAGE_SCHEMA,
    SCHEMA_V2 as DISTRIBUTION_PACKAGE_SCHEMA_V2,
    DistributionPackageError,
    VerifiedDistributionPackage,
)


DISTRIBUTION_CONFORMANCE_AUTHORITY_SCHEMA = (
    "okg.deployment_product.distribution_conformance_authority/v1"
)
DISTRIBUTION_CONFORMANCE_AUTHORITY_SCHEMA_V2 = (
    "okg.deployment_product.distribution_conformance_authority/v2"
)
RAW_BYTES_DIGEST_DOMAIN = "raw-bytes/v1"
CANONICAL_JSON_DIGEST_DOMAIN = "canonical-json/v1"

_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_REVISION_RE = re.compile(r"^[0-9a-f]{40}$")
_CRC32_RE = re.compile(r"^[0-9a-f]{8}$")


class DistributionConformanceAuthorityError(ValueError):
    """Stable refusal from the verified package-to-conformance adapter."""

    def __init__(
        self,
        code: str,
        field: str,
        *,
        expected: str | None = None,
        observed: str | None = None,
    ) -> None:
        self.code = code
        self.field = field
        self.expected = expected
        self.observed = observed
        super().__init__(f"{code}: {field}")

    @property
    def payload(self) -> dict[str, str]:
        result = {"code": self.code, "field": self.field}
        if self.expected is not None:
            result["expected"] = self.expected
        if self.observed is not None:
            result["observed"] = self.observed
        return result


class _AuthorityModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        populate_by_name=True,
        strict=True,
        validate_assignment=True,
    )

    @model_validator(mode="before")
    @classmethod
    def _normalise_sequences(cls, value: object) -> object:
        if not isinstance(value, Mapping):
            return value
        result = dict(value)
        for key in ("bundles", "assets"):
            if isinstance(result.get(key), list):
                result[key] = tuple(result[key])
        return result


class DistributionConformanceRawBlob(_AuthorityModel):
    """Identity of exact raw bytes; never a filesystem locator."""

    digest_domain: Literal["raw-bytes/v1"]
    digest: str
    size: int = Field(ge=0)

    @field_validator("digest")
    @classmethod
    def _digest(cls, value: str) -> str:
        if not _DIGEST_RE.fullmatch(value):
            raise ValueError(
                "digest must be sha256:<64 lowercase hexadecimal characters>"
            )
        return value


class DistributionConformanceArtifact(_AuthorityModel):
    """Raw package archive identity plus its #1178 subject projection."""

    IDENTITY_MODEL: ClassVar[type[ConformanceArtifactIdentity]] = (
        ConformanceArtifactIdentity
    )

    digest_domain: Literal["raw-bytes/v1"]
    package_schema: Literal["okg.deployment_product.distribution_package/v1"]
    media_type: Literal["application/octet-stream"]
    digest: str
    size: int = Field(ge=0)

    @field_validator("digest")
    @classmethod
    def _digest(cls, value: str) -> str:
        if not _DIGEST_RE.fullmatch(value):
            raise ValueError(
                "digest must be sha256:<64 lowercase hexadecimal characters>"
            )
        return value

    def conformance_identity(self) -> ConformanceArtifactIdentity:
        """Project the raw archive into the unchanged #1178 subject shape."""

        return self.IDENTITY_MODEL(
            package_schema=self.package_schema,
            media_type=self.media_type,
            digest=self.digest,
        )


class DistributionConformanceArtifactV2(DistributionConformanceArtifact):
    IDENTITY_MODEL: ClassVar[type[ConformanceArtifactIdentity]] = (
        ConformanceArtifactIdentityV2
    )

    package_schema: Literal[
        "okg.deployment_product.distribution_package/v1",
        "okg.deployment_product.distribution_package/v2",
    ]


class DistributionConformanceCanonicalRecord(_AuthorityModel):
    """Identity of a versioned canonical-JSON record."""

    digest_domain: Literal["canonical-json/v1"]
    schema_version: str
    digest: str

    @field_validator("schema_version")
    @classmethod
    def _schema_version(cls, value: str) -> str:
        if not value or any(char.isspace() for char in value) or "/v" not in value:
            raise ValueError("schema_version must be a bounded versioned token")
        return value

    @field_validator("digest")
    @classmethod
    def _digest(cls, value: str) -> str:
        if not _DIGEST_RE.fullmatch(value):
            raise ValueError(
                "digest must be sha256:<64 lowercase hexadecimal characters>"
            )
        return value


class DistributionConformanceMember(_AuthorityModel):
    """Logical member identity inside a verified package object."""

    member_name: str
    member_crc32: str
    member_compressed_size: int = Field(ge=0)
    compression_method: Literal["stored", "deflated"]

    @field_validator("member_name")
    @classmethod
    def _member_name(cls, value: str) -> str:
        if (
            not value
            or value.startswith("/")
            or "\\" in value
            or any(part in {"", ".", ".."} for part in value.split("/"))
        ):
            raise ValueError(
                "member_name must be a normalized relative archive identity"
            )
        return value

    @field_validator("member_crc32")
    @classmethod
    def _member_crc32(cls, value: str) -> str:
        if not _CRC32_RE.fullmatch(value):
            raise ValueError(
                "member_crc32 must be eight lowercase hexadecimal characters"
            )
        return value


class DistributionConformanceSelectedAsset(_AuthorityModel):
    """One selected typed asset and its verified object/member identity."""

    owner: str
    asset_id: str = Field(alias="id")
    kind: str
    plane: str | None
    version: str
    asset_schema: str
    typed_asset_digest: str
    declaration: DistributionConformanceCanonicalRecord
    raw_object: DistributionConformanceRawBlob
    member: DistributionConformanceMember | None = None
    uncompressed_size: int = Field(ge=0)

    @field_validator("owner", "asset_id", "kind", "version", "asset_schema")
    @classmethod
    def _token(cls, value: str) -> str:
        if not value or any(char.isspace() for char in value):
            raise ValueError("asset identity fields must be non-empty tokens")
        return value

    @field_validator("plane")
    @classmethod
    def _optional_plane(cls, value: str | None) -> str | None:
        if value is not None and (not value or any(char.isspace() for char in value)):
            raise ValueError("asset identity fields must be non-empty tokens")
        return value

    @field_validator("typed_asset_digest")
    @classmethod
    def _typed_asset_digest(cls, value: str) -> str:
        if not _DIGEST_RE.fullmatch(value):
            raise ValueError("typed_asset_digest must be sha256:<64 lowercase hex>")
        return value

    @property
    def qualified_id(self) -> str:
        return f"{self.owner}:{self.asset_id}"

    @property
    def reference(self) -> OwnerQualifiedReference:
        return OwnerQualifiedReference(
            owner=self.owner,
            id=self.asset_id,
            kind=self.kind,
            version=self.version,
            digest=self.typed_asset_digest,
        )


def _authority_payload(
    authority: "DistributionConformanceAuthority",
    *,
    include_authority_digest: bool,
) -> dict[str, Any]:
    payload = authority.model_dump(mode="json", by_alias=True)
    if not include_authority_digest:
        payload.pop("authority_digest", None)
    payload["bundles"].sort(
        key=lambda item: (item["owner"], item["bundle_id"], item["version"])
    )
    payload["assets"].sort(
        key=lambda item: (
            item["owner"],
            item["id"],
            item["kind"],
            item["version"],
        )
    )
    return payload


def _authority_digest_from_payload(payload: Mapping[str, Any]) -> str:
    value = dict(payload)
    value.pop("authority_digest", None)
    value["bundles"] = sorted(
        value["bundles"],
        key=lambda item: (item["owner"], item["bundle_id"], item["version"]),
    )
    value["assets"] = sorted(
        value["assets"],
        key=lambda item: (
            item["owner"],
            item["id"],
            item["kind"],
            item["version"],
        ),
    )
    return sha256_digest(canonical_json_bytes(value))


class DistributionConformanceAuthority(_AuthorityModel):
    """Closed v1 authority projected from one verified package selection."""

    SCHEMA: ClassVar[str] = DISTRIBUTION_CONFORMANCE_AUTHORITY_SCHEMA
    PROTOCOL: ClassVar[str] = CONFORMANCE_PROTOCOL
    SUBJECT_MODEL: ClassVar[type[ConformanceSubject]] = ConformanceSubject

    schema_version: Literal[
        "okg.deployment_product.distribution_conformance_authority/v1"
    ]
    conformance_protocol: Literal["okg.external_distribution_conformance/v1"]
    authority_digest: str
    artifact: DistributionConformanceArtifact
    manifest: DistributionConformanceRawBlob
    package_verification_receipt: DistributionConformanceCanonicalRecord
    source_revision: str
    distribution_release: DistributionRelease
    bundles: tuple[ProductBundle, ...] = Field(min_length=1)
    assets: tuple[DistributionConformanceSelectedAsset, ...] = Field(min_length=1)

    @field_validator("authority_digest")
    @classmethod
    def _authority_digest(cls, value: str) -> str:
        if not _DIGEST_RE.fullmatch(value):
            raise ValueError("authority_digest must be sha256:<64 lowercase hex>")
        return value

    @field_validator("source_revision")
    @classmethod
    def _source_revision(cls, value: str) -> str:
        if not _REVISION_RE.fullmatch(value):
            raise ValueError("source_revision must be a full lowercase Git revision")
        return value

    @model_validator(mode="after")
    def _closed_authority(self) -> "DistributionConformanceAuthority":
        if self.distribution_release.origin != "external":
            raise ValueError(
                "distribution conformance authority requires external origin"
            )
        bundle_order = tuple(
            sorted(
                self.bundles,
                key=lambda item: (item.owner, item.bundle_id, item.version),
            )
        )
        if self.bundles != bundle_order:
            raise ValueError("bundles must use canonical owner/id/version order")
        asset_order = tuple(
            sorted(
                self.assets,
                key=lambda item: (
                    item.owner,
                    item.asset_id,
                    item.kind,
                    item.version,
                ),
            )
        )
        if self.assets != asset_order:
            raise ValueError("assets must use canonical owner/id/kind/version order")
        keys = tuple(item.qualified_id for item in self.assets)
        if len(keys) != len(set(keys)):
            raise ValueError("selected assets must be unique by owner-qualified id")
        expected_refs: dict[str, OwnerQualifiedReference] = {}
        for item in (
            *self.distribution_release.asset_refs,
            *(ref for bundle in self.bundles for ref in bundle.asset_refs),
        ):
            existing = expected_refs.get(item.qualified_id)
            if existing is not None and existing != item:
                raise ValueError(
                    "release and bundle asset references must agree exactly by "
                    "owner-qualified id"
                )
            expected_refs[item.qualified_id] = item
        observed_refs = {item.qualified_id: item.reference for item in self.assets}
        if expected_refs != observed_refs:
            raise ValueError(
                "selected asset records must exactly match release/bundle closure"
            )
        expected_digest = _authority_digest_from_payload(
            self.model_dump(mode="json", by_alias=True)
        )
        if self.authority_digest != expected_digest:
            raise ValueError(
                "authority_digest does not match canonical authority bytes"
            )
        return self

    def subject(self, subject_kind: ConformanceSubjectKind) -> ConformanceSubject:
        """Derive the existing conformance subject from verified package identity."""

        release = self.distribution_release
        return self.SUBJECT_MODEL(
            subject_kind=subject_kind,
            distribution_id=release.distribution_id,
            distribution_version=release.version,
            release_digest=release.release_digest,
            artifact=self.artifact.conformance_identity(),
            source_revision=self.source_revision,
        )

    def selection(
        self, definition: ConformanceDefinitionReference
    ) -> ConformanceSelection:
        """Combine package authority with one opaque instance-owned definition."""

        checked = ConformanceDefinitionReference.model_validate(
            definition.model_dump(mode="json")
        )
        return ConformanceSelection(
            distribution_release=self.distribution_release,
            bundles=self.bundles,
            definition=checked,
        )


class DistributionConformanceAuthorityV2(DistributionConformanceAuthority):
    """Opt-in v2 identity with the unchanged package binding validators."""

    SCHEMA: ClassVar[str] = DISTRIBUTION_CONFORMANCE_AUTHORITY_SCHEMA_V2
    PROTOCOL: ClassVar[str] = CONFORMANCE_PROTOCOL_V2
    SUBJECT_MODEL: ClassVar[type[ConformanceSubject]] = ConformanceSubjectV2

    schema_version: Literal[DISTRIBUTION_CONFORMANCE_AUTHORITY_SCHEMA_V2]
    conformance_protocol: Literal[CONFORMANCE_PROTOCOL_V2]
    artifact: DistributionConformanceArtifactV2


_AUTHORITY_MODELS = {
    "v1": DistributionConformanceAuthority,
    "v2": DistributionConformanceAuthorityV2,
}


def _authority_model(protocol_version: str) -> type[DistributionConformanceAuthority]:
    try:
        return _AUTHORITY_MODELS[protocol_version]
    except (KeyError, TypeError) as exc:
        raise DistributionConformanceAuthorityError(
            "distribution_conformance_protocol_unsupported",
            "protocol_version",
            expected="v1-or-v2",
            observed="unsupported-version",
        ) from exc


def _checked_authority(
    authority: DistributionConformanceAuthority,
) -> DistributionConformanceAuthority:
    payload = authority.model_dump(mode="json", by_alias=True)
    for model in _AUTHORITY_MODELS.values():
        if payload.get("schema_version") == model.SCHEMA:
            return model.model_validate(payload)
    raise DistributionConformanceAuthorityError(
        "distribution_conformance_protocol_unsupported",
        "schema_version",
        observed="unsupported-schema",
    )


def canonical_distribution_conformance_authority_bytes(
    authority: DistributionConformanceAuthority,
) -> bytes:
    """Canonical bytes including the record's verified self digest."""

    checked = _checked_authority(authority)
    return canonical_json_bytes(
        _authority_payload(checked, include_authority_digest=True)
    )


def distribution_conformance_authority_digest(
    authority: DistributionConformanceAuthority,
) -> str:
    """Compute the root digest with ``authority_digest`` omitted."""

    checked = _checked_authority(authority)
    return sha256_digest(
        canonical_json_bytes(
            _authority_payload(checked, include_authority_digest=False)
        )
    )


def distribution_conformance_authority_schema(
    *,
    protocol_version: str = "v1",
) -> dict[str, Any]:
    """Return the closed Draft 2020-12 JSON Schema for the authority record."""

    model = _authority_model(protocol_version)
    schema = model.model_json_schema(ref_template="#/$defs/{model}")
    schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    schema["$id"] = model.SCHEMA
    return schema


def _selection_refusal(
    field: str,
    *,
    expected: str | None = None,
    observed: str | None = None,
) -> DistributionConformanceAuthorityError:
    return DistributionConformanceAuthorityError(
        "distribution_conformance_bundle_selection_invalid",
        field,
        expected=expected,
        observed=observed,
    )


def _selected_bundles(
    available: Sequence[ProductBundle], bundle_ids: Sequence[str]
) -> tuple[ProductBundle, ...]:
    if isinstance(bundle_ids, (str, bytes)) or not bundle_ids:
        raise _selection_refusal("bundle_ids", expected="non-empty bundle ID sequence")
    requested = tuple(bundle_ids)
    if any(not isinstance(item, str) or not item for item in requested):
        raise _selection_refusal("bundle_ids", expected="non-empty string IDs")
    if len(requested) != len(set(requested)):
        raise _selection_refusal("bundle_ids", expected="unique bundle IDs")

    by_id = {bundle.bundle_id: bundle for bundle in available}
    missing = sorted(set(requested) - set(by_id))
    if missing:
        raise _selection_refusal(
            "bundle_ids",
            expected="declared bundle IDs",
            observed=",".join(missing),
        )
    selected = {bundle_id: by_id[bundle_id] for bundle_id in requested}
    declared = set(by_id)
    for bundle in selected.values():
        requirements = set(bundle.requires_bundle_ids)
        conflicts = set(bundle.conflicts_with_bundle_ids)
        undeclared = sorted((requirements | conflicts) - declared)
        if undeclared:
            raise _selection_refusal(
                f"bundles.{bundle.bundle_id}.references",
                expected="declared bundle IDs",
                observed=",".join(undeclared),
            )
        if bundle.bundle_id in requirements:
            raise _selection_refusal(
                f"bundles.{bundle.bundle_id}.requires_bundle_ids",
                observed=bundle.bundle_id,
            )
        if bundle.bundle_id in conflicts:
            raise _selection_refusal(
                f"bundles.{bundle.bundle_id}.conflicts_with_bundle_ids",
                observed=bundle.bundle_id,
            )
        unselected_requirements = sorted(requirements - set(selected))
        if unselected_requirements:
            raise _selection_refusal(
                f"bundles.{bundle.bundle_id}.requires_bundle_ids",
                expected="selected dependency closure",
                observed=",".join(unselected_requirements),
            )
        selected_conflicts = sorted(conflicts & set(selected))
        if selected_conflicts:
            raise _selection_refusal(
                f"bundles.{bundle.bundle_id}.conflicts_with_bundle_ids",
                expected="no selected conflict",
                observed=",".join(selected_conflicts),
            )
    return tuple(
        sorted(
            selected.values(),
            key=lambda item: (item.owner, item.bundle_id, item.version),
        )
    )


def _selected_asset_references(
    release: DistributionRelease,
    bundles: Sequence[ProductBundle],
) -> dict[str, OwnerQualifiedReference]:
    references: dict[str, OwnerQualifiedReference] = {}
    for reference in (
        *release.asset_refs,
        *(item for bundle in bundles for item in bundle.asset_refs),
    ):
        existing = references.get(reference.qualified_id)
        if existing is not None and existing != reference:
            raise DistributionConformanceAuthorityError(
                "distribution_conformance_asset_closure_invalid",
                f"assets.{reference.qualified_id}",
                expected=_reference_identity(existing),
                observed=_reference_identity(reference),
            )
        references[reference.qualified_id] = reference
    return references


def _reference_identity(reference: OwnerQualifiedReference) -> str:
    return ":".join(
        (
            reference.owner,
            reference.ref,
            reference.kind or "",
            reference.version or "",
            reference.digest or "",
        )
    )


def derive_distribution_conformance_authority(
    package: VerifiedDistributionPackage,
    *,
    bundle_ids: Sequence[str],
    protocol_version: str = "v1",
) -> DistributionConformanceAuthority:
    """Project one verified package selection into closed conformance authority."""

    model = _authority_model(protocol_version)
    if not isinstance(package, VerifiedDistributionPackage):
        raise TypeError("package must be a VerifiedDistributionPackage")
    try:
        receipt = package.verification_receipt()
    except DistributionPackageError as exc:
        raise DistributionConformanceAuthorityError(
            "distribution_conformance_package_unavailable",
            "package_verification_receipt",
            observed=exc.code,
        ) from exc

    manifest = package.manifest
    supported_schemas = (
        (DISTRIBUTION_PACKAGE_SCHEMA,)
        if protocol_version == "v1"
        else (
            DISTRIBUTION_PACKAGE_SCHEMA,
            DISTRIBUTION_PACKAGE_SCHEMA_V2,
        )
    )
    if manifest.schema_version not in supported_schemas:
        raise DistributionConformanceAuthorityError(
            "distribution_conformance_package_schema_unsupported",
            "artifact.package_schema",
            expected="-or-".join(supported_schemas),
            observed=manifest.schema_version,
        )
    if receipt.package_schema != manifest.schema_version:
        raise DistributionConformanceAuthorityError(
            "distribution_conformance_package_schema_mismatch",
            "package_verification_receipt.package_schema",
            expected=manifest.schema_version,
            observed=receipt.package_schema,
        )
    bundles = _selected_bundles(manifest.bundles, bundle_ids)
    refs = _selected_asset_references(manifest.distribution_release, bundles)
    packaged_by_id = {item.asset.qualified_id: item for item in manifest.assets}
    object_by_digest = {item.digest: item for item in manifest.objects}
    assets: list[dict[str, Any]] = []
    for qualified_id, reference in sorted(refs.items()):
        packaged = packaged_by_id.get(qualified_id)
        packaged_reference = None
        if packaged is not None:
            packaged_reference = OwnerQualifiedReference(
                owner=packaged.asset.owner,
                id=packaged.asset.asset_id,
                kind=packaged.asset.kind,
                version=packaged.asset.version,
                digest=packaged.asset.digest,
            )
        if packaged_reference != reference:
            raise DistributionConformanceAuthorityError(
                "distribution_conformance_asset_closure_invalid",
                f"assets.{qualified_id}",
                expected=_reference_identity(reference),
                observed=(
                    None
                    if packaged_reference is None
                    else _reference_identity(packaged_reference)
                ),
            )
        assert packaged is not None
        obj = object_by_digest[packaged.locator.object_digest]
        member = None
        if packaged.locator.member_path is not None:
            member = {
                "member_name": packaged.locator.member_path,
                "member_crc32": packaged.locator.member_crc32,
                "member_compressed_size": packaged.locator.member_compressed_size,
                "compression_method": packaged.locator.compression_method,
            }
        asset = packaged.asset
        assets.append(
            {
                "owner": asset.owner,
                "id": asset.asset_id,
                "kind": asset.kind,
                "plane": asset.plane,
                "version": asset.version,
                "asset_schema": asset.schema,
                "typed_asset_digest": asset.digest,
                "declaration": {
                    "digest_domain": CANONICAL_JSON_DIGEST_DOMAIN,
                    "schema_version": TYPED_ASSET_SCHEMA,
                    "digest": sha256_digest(canonical_json_bytes(asset.to_dict())),
                },
                "raw_object": {
                    "digest_domain": RAW_BYTES_DIGEST_DOMAIN,
                    "digest": obj.digest,
                    "size": obj.size,
                },
                "member": member,
                "uncompressed_size": packaged.uncompressed_size,
            }
        )

    transport = dict(receipt.transport_archive_raw_blob)
    raw_manifest = dict(receipt.manifest_raw_blob)
    receipt_schema = str(receipt.preimage()["schema_version"])
    payload: dict[str, Any] = {
        "schema_version": model.SCHEMA,
        "conformance_protocol": model.PROTOCOL,
        "artifact": {
            "digest_domain": RAW_BYTES_DIGEST_DOMAIN,
            "package_schema": manifest.schema_version,
            "media_type": "application/octet-stream",
            "digest": transport["digest"],
            "size": transport["size"],
        },
        "manifest": {
            "digest_domain": RAW_BYTES_DIGEST_DOMAIN,
            "digest": raw_manifest["digest"],
            "size": raw_manifest["size"],
        },
        "package_verification_receipt": {
            "digest_domain": CANONICAL_JSON_DIGEST_DOMAIN,
            "schema_version": receipt_schema,
            "digest": receipt.digest,
        },
        "source_revision": manifest.provenance.revision,
        "distribution_release": manifest.distribution_release.model_dump(
            mode="json", by_alias=True
        ),
        "bundles": [
            bundle.model_dump(mode="json", by_alias=True) for bundle in bundles
        ],
        "assets": assets,
    }
    payload["authority_digest"] = _authority_digest_from_payload(payload)
    return model.model_validate(payload)


def _request_refusal(
    code: str,
    field: str,
    *,
    expected: str | None = None,
    observed: str | None = None,
) -> DistributionConformanceAuthorityError:
    return DistributionConformanceAuthorityError(
        code, field, expected=expected, observed=observed
    )


def bind_distribution_conformance_request(
    authority: DistributionConformanceAuthority,
    request: ExternalDistributionConformanceRequest,
    *,
    definition: ConformanceDefinitionReference,
) -> ExternalDistributionConformanceRequest:
    """Require a request to equal verified package fields plus one local definition."""

    checked_authority = _checked_authority(authority)
    checked_request = load_conformance_request(
        request.model_dump(mode="json", by_alias=True)
    )
    if (
        checked_authority.conformance_protocol
        != checked_request.compatibility.harness_protocol
    ):
        raise _request_refusal(
            "distribution_conformance_request_protocol_mismatch",
            "compatibility.harness_protocol",
            expected=checked_authority.conformance_protocol,
            observed=checked_request.compatibility.harness_protocol,
        )
    checked_definition = ConformanceDefinitionReference.model_validate(
        definition.model_dump(mode="json")
    )
    expected_subject = checked_authority.subject(checked_request.subject.subject_kind)
    subject_pairs = (
        (
            "subject.distribution_id",
            expected_subject.distribution_id,
            checked_request.subject.distribution_id,
        ),
        (
            "subject.distribution_version",
            expected_subject.distribution_version,
            checked_request.subject.distribution_version,
        ),
        (
            "subject.release_digest",
            expected_subject.release_digest,
            checked_request.subject.release_digest,
        ),
        (
            "subject.source_revision",
            expected_subject.source_revision,
            checked_request.subject.source_revision,
        ),
        (
            "subject.artifact.package_schema",
            expected_subject.artifact.package_schema,
            checked_request.subject.artifact.package_schema,
        ),
        (
            "subject.artifact.media_type",
            expected_subject.artifact.media_type,
            checked_request.subject.artifact.media_type,
        ),
        (
            "subject.artifact.digest",
            expected_subject.artifact.digest,
            checked_request.subject.artifact.digest,
        ),
    )
    for field, expected, observed in subject_pairs:
        if expected != observed:
            raise _request_refusal(
                "distribution_conformance_request_subject_mismatch",
                field,
                expected=str(expected),
                observed=str(observed),
            )

    selected_release = checked_request.selection.distribution_release
    expected_release = checked_authority.distribution_release
    if selected_release.asset_refs != expected_release.asset_refs:
        raise _request_refusal(
            "distribution_conformance_request_asset_mismatch",
            "selection.distribution_release.assets",
        )
    if selected_release != expected_release:
        raise _request_refusal(
            "distribution_conformance_request_release_mismatch",
            "selection.distribution_release",
            expected=expected_release.release_digest,
            observed=selected_release.release_digest,
        )

    expected_bundles = {
        (item.bundle_id, item.version): item for item in checked_authority.bundles
    }
    observed_bundles = {
        (item.bundle_id, item.version): item
        for item in checked_request.selection.bundles
    }
    if set(expected_bundles) != set(observed_bundles):
        raise _request_refusal(
            "distribution_conformance_request_bundle_mismatch",
            "selection.bundles",
            expected=",".join(f"{key[0]}@{key[1]}" for key in sorted(expected_bundles)),
            observed=",".join(f"{key[0]}@{key[1]}" for key in sorted(observed_bundles)),
        )
    for key, expected_bundle in expected_bundles.items():
        observed_bundle = observed_bundles[key]
        if observed_bundle.asset_refs != expected_bundle.asset_refs:
            raise _request_refusal(
                "distribution_conformance_request_asset_mismatch",
                f"selection.bundles.{key[0]}.assets",
            )
        if observed_bundle != expected_bundle:
            raise _request_refusal(
                "distribution_conformance_request_bundle_mismatch",
                f"selection.bundles.{key[0]}",
                expected=expected_bundle.bundle_digest,
                observed=observed_bundle.bundle_digest,
            )

    observed_definition = checked_request.selection.definition
    if observed_definition.definition_id != checked_definition.definition_id:
        raise _request_refusal(
            "distribution_conformance_request_definition_mismatch",
            "selection.definition.definition_id",
            expected=checked_definition.definition_id,
            observed=observed_definition.definition_id,
        )
    if observed_definition.definition_digest != checked_definition.definition_digest:
        raise _request_refusal(
            "distribution_conformance_request_definition_mismatch",
            "selection.definition.definition_digest",
            expected=checked_definition.definition_digest,
            observed=observed_definition.definition_digest,
        )
    return checked_request


__all__ = [
    "DISTRIBUTION_CONFORMANCE_AUTHORITY_SCHEMA_V2",
    "DistributionConformanceArtifactV2",
    "DistributionConformanceAuthorityV2",
    "CANONICAL_JSON_DIGEST_DOMAIN",
    "DISTRIBUTION_CONFORMANCE_AUTHORITY_SCHEMA",
    "RAW_BYTES_DIGEST_DOMAIN",
    "DistributionConformanceAuthority",
    "DistributionConformanceAuthorityError",
    "DistributionConformanceArtifact",
    "DistributionConformanceCanonicalRecord",
    "DistributionConformanceMember",
    "DistributionConformanceRawBlob",
    "DistributionConformanceSelectedAsset",
    "bind_distribution_conformance_request",
    "canonical_distribution_conformance_authority_bytes",
    "derive_distribution_conformance_authority",
    "distribution_conformance_authority_digest",
    "distribution_conformance_authority_schema",
]
