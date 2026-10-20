"""Versioned, transport-neutral external distribution conformance contract.

This module is deliberately inert.  It validates and canonicalizes request and
receipt documents and publishes their JSON Schemas.  It does not install an
artifact, run deployment lifecycle phases, access a database, or emit a blocked
runtime result.  Those executable behaviors remain dependency-gated by the
deployment-product program.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Annotated, Any, ClassVar, Literal, TypeAlias, cast

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    ValidationError,
    field_validator,
    model_validator,
)

from .lock import canonical_json_bytes, sha256_digest
from .models import API_VERSION, DistributionRelease, ProductBundle


CONFORMANCE_PROTOCOL_VERSION = "v1"
CONFORMANCE_PROTOCOL = "okg.external_distribution_conformance/v1"
CONFORMANCE_DISTRIBUTION_PACKAGE_SCHEMA = (
    "okg.deployment_product.distribution_package/v1"
)
CONFORMANCE_REQUEST_SCHEMA = "okg.external_distribution_conformance.request/v1"
CONFORMANCE_RECEIPT_SCHEMA = "okg.external_distribution_conformance.receipt/v1"
CONFORMANCE_SCHEMA_CATALOG = "okg.external_distribution_conformance.schema_catalog/v1"
CONFORMANCE_VALIDATION_RESULT_SCHEMA = (
    "okg.external_distribution_conformance.validation_result/v1"
)
CONFORMANCE_PROTOCOL_V2 = "okg.external_distribution_conformance/v2"
CONFORMANCE_DISTRIBUTION_PACKAGE_SCHEMA_V2 = (
    "okg.deployment_product.distribution_package/v2"
)
CONFORMANCE_REQUEST_SCHEMA_V2 = "okg.external_distribution_conformance.request/v2"
CONFORMANCE_RECEIPT_SCHEMA_V2 = "okg.external_distribution_conformance.receipt/v2"
CONFORMANCE_SCHEMA_CATALOG_V2 = (
    "okg.external_distribution_conformance.schema_catalog/v2"
)
CONFORMANCE_VALIDATION_RESULT_SCHEMA_V2 = (
    "okg.external_distribution_conformance.validation_result/v2"
)
# Unambiguous public names beside the older projection-level
# ``CONFORMANCE_SCHEMA`` export.
EXTERNAL_CONFORMANCE_REQUEST_SCHEMA = CONFORMANCE_REQUEST_SCHEMA
EXTERNAL_CONFORMANCE_RECEIPT_SCHEMA = CONFORMANCE_RECEIPT_SCHEMA

ConformanceArm: TypeAlias = Literal[
    "contract",
    "artifact_materialization",
    "first_publish",
    "readback_health",
    "idempotent_reapply",
    "update_rollback_recovery",
    "legacy_authority",
]
ConformanceClaimProfile: TypeAlias = Literal[
    "contract",
    "ci_fixture",
    "external_release_distribution_lifecycle",
]
ConformanceSubjectKind: TypeAlias = Literal["hermetic_fixture", "external_release"]
ConformanceStatus: TypeAlias = Literal["pass", "fail", "blocked"]
ConformanceSignalKind: TypeAlias = Literal[
    "source_count",
    "node_count",
    "edge_count",
    "subtype_count",
    "view",
    "invariant",
    "interface",
    "readiness",
]
ConformanceDiagnosticCode: TypeAlias = Literal[
    "conformance_admission_digest_mismatch",
    "conformance_artifact_digest_mismatch",
    "conformance_bundle_digest_mismatch",
    "conformance_claim_scope_invalid",
    "conformance_dependency_wave5_lifecycle_unavailable",
    "conformance_dependency_wave6_package_unavailable",
    "conformance_dependency_wave7_migration_unavailable",
    "conformance_declared_optional_credential_unavailable",
    "conformance_definition_digest_mismatch",
    "conformance_distribution_declared_asset_missing",
    "conformance_external_acknowledgement_pending",
    "conformance_release_digest_mismatch",
    "conformance_request_digest_mismatch",
    "conformance_runtime_environment_unavailable",
    "conformance_schema_unsupported",
    "conformance_secret_reference_invalid",
    "conformance_signal_evidence_mismatch",
]

CONFORMANCE_ARM_ORDER: tuple[str, ...] = (
    "contract",
    "artifact_materialization",
    "first_publish",
    "readback_health",
    "idempotent_reapply",
    "update_rollback_recovery",
    "legacy_authority",
)
CONFORMANCE_MANDATORY_ARMS: Mapping[str, tuple[str, ...]] = {
    "contract": ("contract",),
    "ci_fixture": (
        "contract",
        "artifact_materialization",
        "first_publish",
        "readback_health",
        "idempotent_reapply",
        "update_rollback_recovery",
    ),
    "external_release_distribution_lifecycle": CONFORMANCE_ARM_ORDER,
}
CONFORMANCE_PROFILE_SUBJECTS: Mapping[str, frozenset[str]] = {
    "contract": frozenset({"hermetic_fixture", "external_release"}),
    "ci_fixture": frozenset({"hermetic_fixture"}),
    "external_release_distribution_lifecycle": frozenset({"external_release"}),
}
CONFORMANCE_PASS_CLASSIFICATIONS: Mapping[str, tuple[str, str]] = {
    "contract": ("none", "not_applicable"),
    "artifact_materialization": ("mutating", "safe"),
    "first_publish": ("mutating", "safe"),
    "readback_health": ("read_only", "safe"),
    "idempotent_reapply": ("none", "safe"),
    "update_rollback_recovery": ("mutating", "safe"),
    "legacy_authority": ("read_only", "safe"),
}

CONFORMANCE_DIAGNOSTIC_CODES = frozenset(
    {
        "conformance_admission_digest_mismatch",
        "conformance_artifact_digest_mismatch",
        "conformance_bundle_digest_mismatch",
        "conformance_claim_scope_invalid",
        "conformance_dependency_wave5_lifecycle_unavailable",
        "conformance_dependency_wave6_package_unavailable",
        "conformance_dependency_wave7_migration_unavailable",
        "conformance_declared_optional_credential_unavailable",
        "conformance_definition_digest_mismatch",
        "conformance_distribution_declared_asset_missing",
        "conformance_external_acknowledgement_pending",
        "conformance_release_digest_mismatch",
        "conformance_request_digest_mismatch",
        "conformance_runtime_environment_unavailable",
        "conformance_schema_unsupported",
        "conformance_secret_reference_invalid",
        "conformance_signal_evidence_mismatch",
    }
)
CONFORMANCE_BLOCKED_DIAGNOSTIC_CODES = frozenset(
    {
        "conformance_dependency_wave5_lifecycle_unavailable",
        "conformance_dependency_wave6_package_unavailable",
        "conformance_dependency_wave7_migration_unavailable",
        "conformance_declared_optional_credential_unavailable",
        "conformance_external_acknowledgement_pending",
        "conformance_runtime_environment_unavailable",
    }
)

_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_IDENTIFIER_RE = re.compile(r"^[a-z0-9][a-z0-9_.:-]{0,127}$")
_VERSION_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+-]{0,127}$")
_MEDIA_TYPE_RE = re.compile(r"^application/[a-z0-9][a-z0-9.+-]{0,126}$")
_GIT_REVISION_RE = re.compile(r"^[0-9a-f]{40}$")
_INTERFACE_REVISION_RE = re.compile(r"^[0-9a-f]{8,40}$")
_CONSUMER_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_.:/-]{0,159}$")
_ISSUE_COMMENT_RE = re.compile(
    r"^https://github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/issues/"
    r"[1-9][0-9]*#issuecomment-[1-9][0-9]*$"
)
_SECRET_REF_RE = re.compile(
    r"^secretref:[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)
_SAFE_DIAGNOSTIC_VALUE_RE = re.compile(r"^[a-z0-9_.:@+-]{1,160}$")
_MAX_INTEROPERABLE_INTEGER = 2**53 - 1
_UNSAFE_DIAGNOSTIC_TOKENS = (
    "credential",
    "dsn",
    "password",
    "private_key",
    "secret",
    "token",
)
IdentifierValue: TypeAlias = Annotated[
    str, StringConstraints(pattern=_IDENTIFIER_RE.pattern)
]
VersionValue: TypeAlias = Annotated[str, StringConstraints(pattern=_VERSION_RE.pattern)]
DigestValue: TypeAlias = Annotated[str, StringConstraints(pattern=_DIGEST_RE.pattern)]
MediaTypeValue: TypeAlias = Annotated[
    str, StringConstraints(pattern=_MEDIA_TYPE_RE.pattern)
]
GitRevisionValue: TypeAlias = Annotated[
    str, StringConstraints(pattern=_GIT_REVISION_RE.pattern)
]
InterfaceRevisionValue: TypeAlias = Annotated[
    str, StringConstraints(pattern=_INTERFACE_REVISION_RE.pattern)
]
ConsumerIdValue: TypeAlias = Annotated[
    str, StringConstraints(pattern=_CONSUMER_ID_RE.pattern)
]
IssueCommentValue: TypeAlias = Annotated[
    str, StringConstraints(pattern=_ISSUE_COMMENT_RE.pattern)
]
OpaqueSecretToken: TypeAlias = Annotated[
    str, StringConstraints(pattern=_SECRET_REF_RE.pattern)
]
SafeDiagnosticValue: TypeAlias = Annotated[
    str, StringConstraints(pattern=_SAFE_DIAGNOSTIC_VALUE_RE.pattern)
]
_SEQUENCE_FIELDS = frozenset(
    {
        "arms",
        "bundle_identities",
        "bundles",
        "completed_arms",
        "diagnostic_codes",
        "diagnostics",
        "inputs",
        "optional_credential_bindings",
        "readback",
        "requested_arms",
        "secret_references",
        "sealed_releases",
        "signals",
        "source_deltas",
    }
)


def _identifier(value: str) -> str:
    if not isinstance(value, str) or not _IDENTIFIER_RE.fullmatch(value):
        raise ValueError(
            "identifier must use lowercase letters, digits, '.', '_', ':', or '-'"
        )
    return value


def _optional_identifier(value: str | None) -> str | None:
    return None if value is None else _identifier(value)


def _version(value: str) -> str:
    if not isinstance(value, str) or not _VERSION_RE.fullmatch(value):
        raise ValueError("version must be a bounded non-path token")
    return value


def _digest(value: str | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not _DIGEST_RE.fullmatch(value):
        raise ValueError("digest must be sha256:<64 lowercase hexadecimal characters>")
    return value


def _normalise_sequences(value: object) -> object:
    if isinstance(value, Mapping):
        return {
            key: (
                tuple(_normalise_sequences(item) for item in child)
                if str(key) in _SEQUENCE_FIELDS and isinstance(child, list)
                else _normalise_sequences(child)
            )
            for key, child in value.items()
        }
    if isinstance(value, tuple):
        return tuple(_normalise_sequences(item) for item in value)
    return value


def _unique(values: Sequence[str], *, field: str) -> tuple[str, ...]:
    result = tuple(values)
    if len(result) != len(set(result)):
        raise ValueError(f"{field} must not contain duplicates")
    return result


def _ordered_arms(values: Sequence[str]) -> tuple[str, ...]:
    unique = _unique(values, field="arms")
    order = {arm: index for index, arm in enumerate(CONFORMANCE_ARM_ORDER)}
    return tuple(sorted(unique, key=order.__getitem__))


def _mandatory_arms(profile: str) -> tuple[str, ...]:
    try:
        return CONFORMANCE_MANDATORY_ARMS[profile]
    except KeyError as exc:  # Defensive for callers passing untyped data.
        raise ValueError("claim profile is unsupported") from exc


def _validate_claim_request(
    *,
    profile: str,
    subject_kind: str,
    requested_arms: Sequence[str],
) -> None:
    if subject_kind not in CONFORMANCE_PROFILE_SUBJECTS.get(profile, frozenset()):
        raise ValueError("claim profile is not valid for the subject kind")
    missing = set(_mandatory_arms(profile)) - set(requested_arms)
    if missing:
        raise ValueError(
            f"claim profile is missing mandatory arms: {sorted(missing)!r}"
        )


class ConformanceModel(BaseModel):
    """Strict immutable base for the transport protocol."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        populate_by_name=True,
        strict=True,
        validate_assignment=True,
    )

    @model_validator(mode="before")
    @classmethod
    def _normalise_json_sequences(cls, value: object) -> object:
        return _normalise_sequences(value)


class ConformanceArtifactIdentity(ConformanceModel):
    package_schema: Literal["okg.deployment_product.distribution_package/v1"]
    media_type: Literal["application/octet-stream"]
    digest: DigestValue

    _artifact_digest = field_validator("digest")(_digest)


class ConformanceSubject(ConformanceModel):
    subject_kind: ConformanceSubjectKind
    distribution_id: IdentifierValue
    distribution_version: VersionValue
    release_digest: DigestValue
    artifact: ConformanceArtifactIdentity
    source_revision: GitRevisionValue | None = None

    _distribution_id = field_validator("distribution_id")(_identifier)
    _distribution_version = field_validator("distribution_version")(_version)
    _release_digest = field_validator("release_digest")(_digest)

    @field_validator("source_revision")
    @classmethod
    def _source_revision(cls, value: str | None) -> str | None:
        if value is not None and not _GIT_REVISION_RE.fullmatch(value):
            raise ValueError("source_revision must be a full lowercase Git revision")
        return value


class ConformanceDefinitionReference(ConformanceModel):
    definition_id: IdentifierValue
    definition_digest: DigestValue

    _definition_id = field_validator("definition_id")(_identifier)
    _definition_digest = field_validator("definition_digest")(_digest)


class ConformanceSelection(ConformanceModel):
    distribution_release: DistributionRelease
    bundles: tuple[ProductBundle, ...] = Field(min_length=1)
    definition: ConformanceDefinitionReference

    @model_validator(mode="after")
    def _selection_consistent(self) -> "ConformanceSelection":
        bundle_keys = [(bundle.bundle_id, bundle.version) for bundle in self.bundles]
        if len(bundle_keys) != len(set(bundle_keys)):
            raise ValueError("selected bundles must be unique by id and version")
        declared = set(self.distribution_release.bundle_ids)
        for bundle in self.bundles:
            if bundle.distribution_id != self.distribution_release.distribution_id:
                raise ValueError(
                    "selected bundle distribution_id must match the release"
                )
            if bundle.distribution_version != self.distribution_release.version:
                raise ValueError(
                    "selected bundle distribution_version must match the release"
                )
            if bundle.bundle_id not in declared:
                raise ValueError("selected bundle must be declared by the release")
        return self


class ConformanceCase(ConformanceModel):
    case_id: IdentifierValue
    claim_profile: ConformanceClaimProfile
    requested_arms: tuple[ConformanceArm, ...]

    _case_id = field_validator("case_id")(_identifier)

    @field_validator("requested_arms")
    @classmethod
    def _requested_arms(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        return _ordered_arms(values)


class ConformanceExpectedSignal(ConformanceModel):
    signal_id: IdentifierValue
    kind: ConformanceSignalKind
    target: IdentifierValue
    minimum_count: int | None = Field(
        default=None,
        ge=1,
        le=_MAX_INTEROPERABLE_INTEGER,
    )
    expected_digest: DigestValue | None = None

    _signal_id = field_validator("signal_id")(_identifier)
    _target = field_validator("target")(_identifier)
    _expected_digest = field_validator("expected_digest")(_digest)

    @model_validator(mode="after")
    def _signal_shape(self) -> "ConformanceExpectedSignal":
        counted = self.kind in {
            "source_count",
            "node_count",
            "edge_count",
            "subtype_count",
        }
        if counted and self.minimum_count is None:
            raise ValueError("count expectations require minimum_count")
        if not counted and self.minimum_count is not None:
            raise ValueError("non-count expectations must not set minimum_count")
        return self


class ConformanceExpectations(ConformanceModel):
    signals: tuple[ConformanceExpectedSignal, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _unique_signals(self) -> "ConformanceExpectations":
        _unique(tuple(signal.signal_id for signal in self.signals), field="signal ids")
        return self


class ConformanceInputReference(ConformanceModel):
    input_id: IdentifierValue
    kind: Literal["fixture", "snapshot", "connection"]
    digest: DigestValue
    media_type: MediaTypeValue

    _input_id = field_validator("input_id")(_identifier)
    _input_digest = field_validator("digest")(_digest)

    @field_validator("media_type")
    @classmethod
    def _input_media_type(cls, value: str) -> str:
        if not _MEDIA_TYPE_RE.fullmatch(value):
            raise ValueError("input media_type must be a bounded application/* value")
        return value


class OpaqueSecretReference(ConformanceModel):
    reference: OpaqueSecretToken

    @field_validator("reference")
    @classmethod
    def _opaque_reference(cls, value: str) -> str:
        if not _SECRET_REF_RE.fullmatch(value):
            raise ValueError(
                "secret reference must be an opaque canonical secretref:<uuid>"
            )
        return value


class ConformanceOptionalCredentialBinding(ConformanceModel):
    """One deferred source/tool credential declared by immutable product input."""

    binding_id: IdentifierValue
    binding_kind: Literal["source", "tool"]
    declaration_ref: IdentifierValue
    declaration_digest: DigestValue
    provider_connection_ref: IdentifierValue
    provider_connection_digest: DigestValue
    credential_reference: OpaqueSecretToken
    readiness_policy: Literal["deferred"]
    credential_mode: Literal["noninteractive"]

    _identifiers = field_validator(
        "binding_id",
        "declaration_ref",
        "provider_connection_ref",
    )(_identifier)
    _digests = field_validator(
        "declaration_digest",
        "provider_connection_digest",
    )(_digest)

    @field_validator("credential_reference")
    @classmethod
    def _credential_reference(cls, value: str) -> str:
        if not _SECRET_REF_RE.fullmatch(value):
            raise ValueError(
                "optional credential reference must be an opaque canonical "
                "secretref:<uuid>"
            )
        return value


class ConformanceSealedReleaseInput(ConformanceModel):
    """Consumer-supplied immutable input for the recovery arm; never a path."""

    role: Literal["baseline", "candidate"]
    schema_version: Literal["okg.sealed_release/v1"]
    artifact_id: DigestValue
    input_id: DigestValue
    definition_digest: DigestValue
    distribution_release_digest: DigestValue

    _digests = field_validator(
        "artifact_id",
        "input_id",
        "definition_digest",
        "distribution_release_digest",
    )(_digest)


class ConformanceLegacyInputInventory(ConformanceModel):
    """Complete consumer declaration consumed later by the Wave 7 harness arm."""

    inventory_id: IdentifierValue
    inventory_digest: DigestValue
    media_type: Literal["application/json"]
    entry_count: int = Field(ge=0, le=_MAX_INTEROPERABLE_INTEGER)
    empty: bool
    complete: Literal[True]

    _inventory_id = field_validator("inventory_id")(_identifier)
    _inventory_digest = field_validator("inventory_digest")(_digest)

    @model_validator(mode="after")
    def _empty_matches_count(self) -> "ConformanceLegacyInputInventory":
        if self.empty != (self.entry_count == 0):
            raise ValueError("legacy inventory empty must match entry_count == 0")
        return self


class ConformanceInputs(ConformanceModel):
    inputs: tuple[ConformanceInputReference, ...] = ()
    # These are the references the operator actually supplied for this request.
    secret_references: tuple[OpaqueSecretReference, ...] = ()
    optional_credential_bindings: tuple[
        ConformanceOptionalCredentialBinding, ...
    ] = ()
    sealed_releases: tuple[ConformanceSealedReleaseInput, ...] = ()
    legacy_input_inventory: ConformanceLegacyInputInventory | None = None

    @model_validator(mode="after")
    def _unique_inputs(self) -> "ConformanceInputs":
        _unique(tuple(item.input_id for item in self.inputs), field="input ids")
        _unique(
            tuple(item.reference for item in self.secret_references),
            field="secret references",
        )
        _unique(
            tuple(item.binding_id for item in self.optional_credential_bindings),
            field="optional credential binding ids",
        )
        _unique(
            tuple(
                item.credential_reference
                for item in self.optional_credential_bindings
            ),
            field="optional credential references",
        )
        supplied = {item.reference for item in self.secret_references}
        unavailable = {
            item.credential_reference for item in self.optional_credential_bindings
        }
        if supplied & unavailable:
            raise ValueError(
                "optional credential bindings must name references the operator "
                "did not supply"
            )
        release_roles = tuple(item.role for item in self.sealed_releases)
        if len(release_roles) != len(set(release_roles)):
            raise ValueError("sealed release roles must be unique")
        return self


class ConformanceAdmission(ConformanceModel):
    extension_boundary_digest: DigestValue
    package_admission_digest: DigestValue
    capability_digest: DigestValue
    resource_policy_digest: DigestValue

    _digests = field_validator(
        "extension_boundary_digest",
        "package_admission_digest",
        "capability_digest",
        "resource_policy_digest",
    )(_digest)


class ConformanceCompatibility(ConformanceModel):
    product_contract_api: Literal[API_VERSION]
    harness_protocol: Literal[CONFORMANCE_PROTOCOL]


class ExternalDistributionConformanceRequest(ConformanceModel):
    SCHEMA: ClassVar[str] = CONFORMANCE_REQUEST_SCHEMA
    PROTOCOL_VERSION: ClassVar[str] = "v1"

    schema_version: Literal[CONFORMANCE_REQUEST_SCHEMA]
    request_id: IdentifierValue
    subject: ConformanceSubject
    selection: ConformanceSelection
    case: ConformanceCase
    expectations: ConformanceExpectations
    inputs: ConformanceInputs
    admission: ConformanceAdmission
    compatibility: ConformanceCompatibility

    _request_id = field_validator("request_id")(_identifier)

    @model_validator(mode="after")
    def _request_consistent(self) -> "ExternalDistributionConformanceRequest":
        release = self.selection.distribution_release
        if release.origin != "external":
            raise ValueError("conformance distribution_release origin must be external")
        if self.subject.distribution_id != release.distribution_id:
            raise ValueError("subject distribution_id must match the selected release")
        if self.subject.distribution_version != release.version:
            raise ValueError(
                "subject distribution_version must match the selected release"
            )
        if self.subject.release_digest != release.release_digest:
            raise ValueError("subject release_digest must match the selected release")
        _validate_claim_request(
            profile=self.case.claim_profile,
            subject_kind=self.subject.subject_kind,
            requested_arms=self.case.requested_arms,
        )
        selected_assets = {}
        for asset in (
            *release.asset_refs,
            *(
                asset
                for bundle in self.selection.bundles
                for asset in bundle.asset_refs
            ),
        ):
            previous = selected_assets.setdefault(asset.qualified_id, asset)
            if previous != asset:
                raise ValueError(
                    "selected asset declarations must agree by owner-qualified id"
                )
        declaration_kinds = {
            "source": {"source", "source-binding", "source-definition"},
            "tool": {"tool", "tool-binding", "tool-definition"},
        }
        if self.PROTOCOL_VERSION == "v2":
            # Protocol v2 also accepts `source-template`: the in-repo package
            # builders (the example connector and the daily-update fixture)
            # give every bundle file, source-defaults included, that kind, and
            # the packaged installer binds a profile credential to the asset
            # that carries its source-defaults file. v1 is unchanged.
            declaration_kinds["source"] = {
                *declaration_kinds["source"], "source-template",
            }
        for binding in self.inputs.optional_credential_bindings:
            declaration = selected_assets.get(binding.declaration_ref)
            if (
                declaration is None
                or declaration.digest != binding.declaration_digest
                or declaration.kind not in declaration_kinds[binding.binding_kind]
            ):
                raise ValueError(
                    "optional credential bindings must refer to a digest-bound "
                    "typed source/tool declaration selected from the manifest"
                )
            if binding.provider_connection_ref is None:
                # Protocol v2 only: a package need not carry a provider
                # connection. One that is named is still checked below.
                continue
            provider_connection = selected_assets.get(
                binding.provider_connection_ref
            )
            if (
                provider_connection is None
                or provider_connection.kind != "provider-connection"
                or provider_connection.digest
                != binding.provider_connection_digest
            ):
                raise ValueError(
                    "optional credential bindings must refer to a digest-bound "
                    "provider connection selected from the manifest"
                )
        recovery_requested = (
            "update_rollback_recovery" in self.case.requested_arms
        )
        releases_by_role = {item.role: item for item in self.inputs.sealed_releases}
        if recovery_requested and set(releases_by_role) != {"baseline", "candidate"}:
            raise ValueError(
                "update_rollback_recovery requires baseline and candidate sealed releases"
            )
        if not recovery_requested and releases_by_role:
            raise ValueError(
                "sealed release inputs are valid only for update_rollback_recovery"
            )
        if recovery_requested:
            baseline = releases_by_role["baseline"]
            candidate = releases_by_role["candidate"]
            if baseline.artifact_id == candidate.artifact_id:
                raise ValueError("recovery baseline and candidate must be distinct")
            if candidate.distribution_release_digest != self.subject.release_digest:
                raise ValueError(
                    "candidate sealed release must bind the conformance subject release"
                )
            if candidate.definition_digest != self.selection.definition.definition_digest:
                raise ValueError(
                    "candidate sealed release must bind the selected definition"
                )
        legacy_requested = "legacy_authority" in self.case.requested_arms
        if legacy_requested and self.inputs.legacy_input_inventory is None:
            raise ValueError(
                "legacy_authority requires a complete declared legacy-input inventory"
            )
        if not legacy_requested and self.inputs.legacy_input_inventory is not None:
            raise ValueError(
                "legacy inventory is valid only for the legacy_authority arm"
            )
        return self


class ConformanceHarnessIdentity(ConformanceModel):
    okg_revision: GitRevisionValue
    package_version: VersionValue
    harness_protocol: Literal[CONFORMANCE_PROTOCOL]
    product_contract_api: Literal[API_VERSION]

    @field_validator("okg_revision")
    @classmethod
    def _okg_revision(cls, value: str) -> str:
        if not _GIT_REVISION_RE.fullmatch(value):
            raise ValueError("okg_revision must be a full lowercase Git revision")
        return value

    _package_version = field_validator("package_version")(_version)


class ConformanceObservedBundleIdentity(ConformanceModel):
    bundle_id: IdentifierValue
    version: VersionValue
    digest: DigestValue

    _bundle_id = field_validator("bundle_id")(_identifier)
    _bundle_version = field_validator("version")(_version)
    _bundle_digest = field_validator("digest")(_digest)


class ConformanceAuthority(ConformanceModel):
    observed_artifact_digest: DigestValue | None = None
    release_manifest_digest: DigestValue | None = None
    release_digest: DigestValue | None = None
    bundle_identities: tuple[ConformanceObservedBundleIdentity, ...] = ()
    extension_boundary_digest: DigestValue
    package_admission_digest: DigestValue
    capability_digest: DigestValue
    resource_policy_digest: DigestValue
    definition_digest: DigestValue | None = None
    lock_digest: DigestValue | None = None
    composition_digest: DigestValue | None = None
    plan_digest: DigestValue | None = None
    sealed_release_digest: DigestValue | None = None
    applied_instance_id: IdentifierValue | None = None

    _digests = field_validator(
        "observed_artifact_digest",
        "release_manifest_digest",
        "release_digest",
        "extension_boundary_digest",
        "package_admission_digest",
        "capability_digest",
        "resource_policy_digest",
        "definition_digest",
        "lock_digest",
        "composition_digest",
        "plan_digest",
        "sealed_release_digest",
    )(_digest)
    _applied_instance_id = field_validator("applied_instance_id")(_optional_identifier)

    @model_validator(mode="after")
    def _unique_bundles(self) -> "ConformanceAuthority":
        keys = [(item.bundle_id, item.version) for item in self.bundle_identities]
        if len(keys) != len(set(keys)):
            raise ValueError("observed bundle identities must be unique")
        return self


class ConformanceOptionalCredentialUnavailableEvidence(ConformanceModel):
    """Proof shape that excludes every non-credential defect named by protocol v1."""

    binding_id: IdentifierValue
    declaration_ref: IdentifierValue
    declaration_digest: DigestValue
    provider_connection_ref: IdentifierValue
    provider_connection_digest: DigestValue
    credential_reference: OpaqueSecretToken
    operator_reference_supplied: Literal[False]
    missing_tooling: Literal[False]
    missing_packaged_asset: Literal[False]
    missing_dependency: Literal[False]
    permission_denied: Literal[False]
    required_cache_unproduced: Literal[False]
    reference_invalid: Literal[False]
    authentication_failed: Literal[False]
    totp_required: Literal[False]

    _identifiers = field_validator(
        "binding_id",
        "declaration_ref",
        "provider_connection_ref",
    )(_identifier)
    _digests = field_validator(
        "declaration_digest",
        "provider_connection_digest",
    )(_digest)

    @field_validator("credential_reference")
    @classmethod
    def _credential_reference(cls, value: str) -> str:
        if not _SECRET_REF_RE.fullmatch(value):
            raise ValueError(
                "optional credential evidence must bind an opaque secretref:<uuid>"
            )
        return value


class ConformanceDeclaredAssetMissingEvidence(ConformanceModel):
    asset_ref: IdentifierValue
    asset_digest: DigestValue
    declared_in_manifest: Literal[True]
    observed_in_artifact: Literal[False]

    _asset_ref = field_validator("asset_ref")(_identifier)
    _asset_digest = field_validator("asset_digest")(_digest)


class ConformanceDiagnostic(ConformanceModel):
    code: ConformanceDiagnosticCode
    arm: ConformanceArm
    phase: Literal[
        "validation",
        "materialization",
        "apply",
        "readback",
        "reapply",
        "recovery",
        "migration",
    ]
    reference: IdentifierValue
    expected: SafeDiagnosticValue | None = None
    observed: SafeDiagnosticValue | None = None
    owner: Literal["consumer", "distribution", "harness", "operator", "system"]
    retry_classification: Literal["never", "safe", "unsafe", "not_applicable"]
    safe_next_action: Literal[
        "obtain_external_acknowledgement",
        "provide_optional_credential",
        "provide_runtime_environment",
        "publish_wave5_lifecycle_output",
        "publish_wave6_package_output",
        "publish_wave7_migration_output",
        "replace_artifact",
        "revise_request",
    ]
    optional_credential: ConformanceOptionalCredentialUnavailableEvidence | None = None
    declared_asset: ConformanceDeclaredAssetMissingEvidence | None = None

    @field_validator("code")
    @classmethod
    def _code(cls, value: str) -> str:
        if value not in CONFORMANCE_DIAGNOSTIC_CODES:
            raise ValueError("diagnostic code is not registered by protocol v1")
        return value

    _reference = field_validator("reference")(_identifier)

    @field_validator("expected", "observed")
    @classmethod
    def _safe_value(cls, value: str | None) -> str | None:
        if value is None:
            return None
        lowered = value.lower()
        if not _SAFE_DIAGNOSTIC_VALUE_RE.fullmatch(value) or any(
            token in lowered for token in _UNSAFE_DIAGNOSTIC_TOKENS
        ):
            raise ValueError("diagnostic values must be bounded non-secret tokens")
        return value

    @model_validator(mode="after")
    def _code_profile(self) -> "ConformanceDiagnostic":
        optional_code = "conformance_declared_optional_credential_unavailable"
        missing_asset_code = "conformance_distribution_declared_asset_missing"
        if self.code == optional_code:
            expected = (
                self.arm == "first_publish"
                and self.phase == "apply"
                and self.owner == "operator"
                and self.retry_classification == "safe"
                and self.safe_next_action == "provide_optional_credential"
                and self.optional_credential is not None
                and self.declared_asset is None
                and self.reference == self.optional_credential.binding_id
            )
            if not expected:
                raise ValueError(
                    "optional credential diagnostic must use its closed operator profile"
                )
        elif self.optional_credential is not None:
            raise ValueError(
                "optional credential evidence is valid only for its diagnostic code"
            )
        if self.code == missing_asset_code:
            expected = (
                self.arm == "artifact_materialization"
                and self.phase == "materialization"
                and self.owner == "distribution"
                and self.retry_classification == "never"
                and self.safe_next_action == "replace_artifact"
                and self.declared_asset is not None
                and self.optional_credential is None
                and self.reference == self.declared_asset.asset_ref
            )
            if not expected:
                raise ValueError(
                    "missing declared asset diagnostic must use its closed distribution profile"
                )
        elif self.declared_asset is not None:
            raise ValueError(
                "declared asset evidence is valid only for its diagnostic code"
            )
        return self


class ConformanceArmResult(ConformanceModel):
    arm: ConformanceArm
    status: ConformanceStatus
    mutation: Literal["none", "read_only", "mutating"]
    retry_classification: Literal["never", "safe", "unsafe", "not_applicable"]
    diagnostic_codes: tuple[ConformanceDiagnosticCode, ...] = ()

    @field_validator("diagnostic_codes")
    @classmethod
    def _diagnostic_codes(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        unique = _unique(values, field="arm diagnostic codes")
        unknown = set(unique) - CONFORMANCE_DIAGNOSTIC_CODES
        if unknown:
            raise ValueError(
                f"arm contains unregistered diagnostic codes: {sorted(unknown)!r}"
            )
        return tuple(sorted(unique))

    @model_validator(mode="after")
    def _status_is_consistent(self) -> "ConformanceArmResult":
        optional_code = "conformance_declared_optional_credential_unavailable"
        missing_asset_code = "conformance_distribution_declared_asset_missing"
        if self.status == "blocked" and self.mutation != "none":
            raise ValueError("blocked arms must record mutation='none'")
        if self.status == "blocked" and self.retry_classification != "safe":
            raise ValueError("blocked dependency arms must be safe to retry")
        if self.status == "blocked" and not self.diagnostic_codes:
            raise ValueError("blocked arms must name at least one stable diagnostic")
        if self.status == "blocked" and not set(self.diagnostic_codes).issubset(
            CONFORMANCE_BLOCKED_DIAGNOSTIC_CODES
        ):
            raise ValueError("blocked arms may use only dependency diagnostics")
        if self.status == "fail" and not self.diagnostic_codes:
            raise ValueError("failed arms must name at least one stable diagnostic")
        if self.status == "pass" and self.diagnostic_codes:
            raise ValueError("passing arms must not name diagnostics")
        if self.status == "pass":
            expected_mutation, expected_retry = CONFORMANCE_PASS_CLASSIFICATIONS[
                self.arm
            ]
            if self.mutation != expected_mutation:
                raise ValueError(
                    f"passing {self.arm} must record mutation={expected_mutation!r}"
                )
            if self.retry_classification != expected_retry:
                raise ValueError(
                    f"passing {self.arm} must record "
                    f"retry_classification={expected_retry!r}"
                )
        if optional_code in self.diagnostic_codes and self.status != "blocked":
            raise ValueError(
                "optional credential diagnostic is valid only for a blocked arm"
            )
        if missing_asset_code in self.diagnostic_codes and (
            self.status != "fail"
            or self.mutation != "none"
            or self.retry_classification != "never"
        ):
            raise ValueError(
                "missing declared asset must fail without mutation or same-artifact retry"
            )
        return self


def aggregate_conformance_verdict(
    arm_results: Sequence[ConformanceArmResult],
) -> ConformanceStatus:
    """Derive the closed v1 verdict with ``fail > blocked > pass`` precedence."""

    if not arm_results:
        raise ValueError("at least one conformance arm result is required")
    arm_ids = tuple(item.arm for item in arm_results)
    if len(arm_ids) != len(set(arm_ids)):
        raise ValueError("conformance arm results must be unique")
    if any(item.status == "fail" for item in arm_results):
        return "fail"
    if any(item.status == "blocked" for item in arm_results):
        return "blocked"
    return "pass"


class ConformanceSourceDelta(ConformanceModel):
    source_id: IdentifierValue
    delta: int = Field(
        ge=-_MAX_INTEROPERABLE_INTEGER,
        le=_MAX_INTEROPERABLE_INTEGER,
    )

    _source_id = field_validator("source_id")(_identifier)


class ConformancePublishEvidence(ConformanceModel):
    generation_id: IdentifierValue
    generation_status: Literal["published"]
    definition_digest: DigestValue
    lock_digest: DigestValue
    composition_digest: DigestValue
    plan_digest: DigestValue
    sealed_release_digest: DigestValue
    applied_instance_id: IdentifierValue
    nodes_live_delta: int = Field(ge=1, le=_MAX_INTEROPERABLE_INTEGER)
    edges_live_delta: int = Field(ge=1, le=_MAX_INTEROPERABLE_INTEGER)
    source_deltas: tuple[ConformanceSourceDelta, ...]

    _generation_id = field_validator("generation_id")(_identifier)
    _digests = field_validator(
        "definition_digest",
        "lock_digest",
        "composition_digest",
        "plan_digest",
        "sealed_release_digest",
    )(_digest)
    _applied_instance_id = field_validator("applied_instance_id")(_identifier)


class ConformanceSignalEvidence(ConformanceModel):
    signal_id: IdentifierValue
    kind: ConformanceSignalKind
    target: IdentifierValue
    observed_count: int | None = Field(
        default=None,
        ge=0,
        le=_MAX_INTEROPERABLE_INTEGER,
    )
    result_digest: DigestValue | None = None
    positive: bool

    _signal_id = field_validator("signal_id")(_identifier)
    _target = field_validator("target")(_identifier)
    _result_digest = field_validator("result_digest")(_digest)

    @model_validator(mode="after")
    def _signal_shape(self) -> "ConformanceSignalEvidence":
        counted = self.kind in {
            "source_count",
            "node_count",
            "edge_count",
            "subtype_count",
        }
        if counted and self.observed_count is None:
            raise ValueError("count signal evidence requires observed_count")
        if not counted and self.result_digest is None:
            raise ValueError("non-count signal evidence requires result_digest")
        return self


class ConformanceReadbackEvidence(ConformanceModel):
    check_id: IdentifierValue
    kind: Literal["search", "trace", "status", "interface", "view", "invariant"]
    generation_id: IdentifierValue
    result_digest: DigestValue
    positive: bool

    _check_id = field_validator("check_id")(_identifier)
    _generation_id = field_validator("generation_id")(_identifier)
    _result_digest = field_validator("result_digest")(_digest)


class ConformanceReadinessEvidence(ConformanceModel):
    ready: bool
    doctor: Literal["clean", "failed", "unavailable"]
    queue_depth: int = Field(ge=0, le=_MAX_INTEROPERABLE_INTEGER)
    dlq_depth: int = Field(ge=0, le=_MAX_INTEROPERABLE_INTEGER)
    mcp_graph_read: bool


class ConformanceMaterializationEvidence(ConformanceModel):
    materialization_digest: DigestValue
    checkout_independent: Literal[True]
    selected_assets_verified: Literal[True]

    _materialization_digest = field_validator("materialization_digest")(_digest)


class ConformanceReapplyEvidence(ConformanceModel):
    generation_id_before: IdentifierValue
    generation_id_after: IdentifierValue
    lock_digest: DigestValue
    plan_digest: DigestValue
    nodes_live_delta: Literal[0]
    edges_live_delta: Literal[0]
    generations_created_delta: Literal[0]
    active_generation_pointer_changes: Literal[0]
    source_deltas: tuple[ConformanceSourceDelta, ...]

    _generation_ids = field_validator(
        "generation_id_before",
        "generation_id_after",
    )(_identifier)
    _digests = field_validator("lock_digest", "plan_digest")(_digest)

    @model_validator(mode="after")
    def _zero_mutation(self) -> "ConformanceReapplyEvidence":
        if self.generation_id_before != self.generation_id_after:
            raise ValueError("idempotent reapply must retain the active generation")
        if any(item.delta != 0 for item in self.source_deltas):
            raise ValueError("idempotent reapply source deltas must all be zero")
        return self


class ConformanceRecoveryEvidence(ConformanceModel):
    last_known_good_generation_id: IdentifierValue
    restored_generation_id: IdentifierValue
    interruption_recovered: Literal[True]
    rollback_verified: Literal[True]
    digest_fences_verified: Literal[True]
    lock_digest: DigestValue
    plan_digest: DigestValue
    baseline_sealed_release_artifact_id: DigestValue
    candidate_sealed_release_artifact_id: DigestValue
    restored_sealed_release_artifact_id: DigestValue
    result_digest: DigestValue

    _generation_ids = field_validator(
        "last_known_good_generation_id",
        "restored_generation_id",
    )(_identifier)
    _digests = field_validator(
        "lock_digest",
        "plan_digest",
        "baseline_sealed_release_artifact_id",
        "candidate_sealed_release_artifact_id",
        "restored_sealed_release_artifact_id",
        "result_digest",
    )(_digest)

    @model_validator(mode="after")
    def _last_known_good_restored(self) -> "ConformanceRecoveryEvidence":
        if self.restored_generation_id != self.last_known_good_generation_id:
            raise ValueError("recovery must restore the last-known-good generation")
        if (
            self.restored_sealed_release_artifact_id
            != self.baseline_sealed_release_artifact_id
        ):
            raise ValueError("recovery must restore the baseline sealed release")
        if (
            self.candidate_sealed_release_artifact_id
            == self.baseline_sealed_release_artifact_id
        ):
            raise ValueError("recovery candidate must differ from the baseline")
        return self


class ConformanceLegacyAuthorityEvidence(ConformanceModel):
    audit_digest: DigestValue
    declared_inventory_digest: DigestValue
    declared_inventory_entry_count: int = Field(
        ge=0,
        le=_MAX_INTEROPERABLE_INTEGER,
    )
    declared_inputs: int = Field(ge=0, le=_MAX_INTEROPERABLE_INTEGER)
    migrated_inputs: int = Field(ge=0, le=_MAX_INTEROPERABLE_INTEGER)
    refused_inputs: int = Field(ge=0, le=_MAX_INTEROPERABLE_INTEGER)
    unresolved_inputs: Literal[0]
    fallback_authority_absent: Literal[True]

    _digests = field_validator(
        "audit_digest",
        "declared_inventory_digest",
    )(_digest)

    @model_validator(mode="after")
    def _all_declared_inputs_accounted_for(
        self,
    ) -> "ConformanceLegacyAuthorityEvidence":
        if self.declared_inputs != self.migrated_inputs + self.refused_inputs:
            raise ValueError("legacy audit must account for every declared input")
        if self.declared_inputs != self.declared_inventory_entry_count:
            raise ValueError("legacy audit must bind the declared inventory count")
        return self


class ConformanceEvidence(ConformanceModel):
    publish: ConformancePublishEvidence | None = None
    signals: tuple[ConformanceSignalEvidence, ...] = ()
    readback: tuple[ConformanceReadbackEvidence, ...] = ()
    readiness: ConformanceReadinessEvidence | None = None
    materialization: ConformanceMaterializationEvidence | None = None
    reapply: ConformanceReapplyEvidence | None = None
    recovery: ConformanceRecoveryEvidence | None = None
    legacy_authority: ConformanceLegacyAuthorityEvidence | None = None

    @model_validator(mode="after")
    def _generation_bound(self) -> "ConformanceEvidence":
        if self.readback and self.publish is None:
            raise ValueError("readback evidence must be generation-bound")
        if self.publish is not None and any(
            item.generation_id != self.publish.generation_id for item in self.readback
        ):
            raise ValueError("readback evidence generation_id must match the receipt")
        _unique(
            tuple(item.signal_id for item in self.signals), field="signal evidence ids"
        )
        return self


class ConformanceClaim(ConformanceModel):
    profile: ConformanceClaimProfile
    subject_kind: ConformanceSubjectKind
    requested_arms: tuple[ConformanceArm, ...]
    completed_arms: tuple[ConformanceArm, ...]
    satisfied: bool
    scope: Literal["contract", "ci_fixture", "distribution_lifecycle"]
    external_evidence_origin: "ConformanceExternalEvidenceOrigin | None" = None

    @field_validator("requested_arms", "completed_arms")
    @classmethod
    def _arms(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        return _ordered_arms(values)

    @model_validator(mode="after")
    def _profile_consistent(self) -> "ConformanceClaim":
        _validate_claim_request(
            profile=self.profile,
            subject_kind=self.subject_kind,
            requested_arms=self.requested_arms,
        )
        expected_scope = {
            "contract": "contract",
            "ci_fixture": "ci_fixture",
            "external_release_distribution_lifecycle": "distribution_lifecycle",
        }[self.profile]
        if self.scope != expected_scope:
            raise ValueError("claim scope must match its closed profile")
        if not set(self.completed_arms).issubset(self.requested_arms):
            raise ValueError("completed arms must be a subset of requested arms")
        if self.profile == "external_release_distribution_lifecycle":
            if self.external_evidence_origin is None:
                raise ValueError(
                    "external release claims must name their evidence origin"
                )
            if self.satisfied and (
                self.external_evidence_origin.acknowledgement_status != "current"
            ):
                raise ValueError(
                    "a satisfied external release claim requires current acknowledgement"
                )
        elif self.external_evidence_origin is not None:
            raise ValueError(
                "only external release claims may name external evidence origin"
            )
        return self


class ConformanceExternalEvidenceOrigin(ConformanceModel):
    consumer_id: ConsumerIdValue
    review_reference: IssueCommentValue
    acknowledged_interface_revision: InterfaceRevisionValue
    acknowledged_schema_digest: DigestValue
    source_revision: GitRevisionValue
    acknowledgement_status: Literal["historical", "current"]

    @field_validator("consumer_id")
    @classmethod
    def _consumer_id(cls, value: str) -> str:
        if not _CONSUMER_ID_RE.fullmatch(value):
            raise ValueError("consumer_id must be a stable repository/product id")
        return value

    @field_validator("review_reference")
    @classmethod
    def _review_reference(cls, value: str) -> str:
        if not _ISSUE_COMMENT_RE.fullmatch(value):
            raise ValueError("review_reference must be an exact GitHub issue comment")
        return value

    @field_validator("acknowledged_interface_revision")
    @classmethod
    def _interface_revision(cls, value: str) -> str:
        if not _INTERFACE_REVISION_RE.fullmatch(value):
            raise ValueError("interface revision must be 8 to 40 lowercase hex")
        return value

    _schema_digest = field_validator("acknowledged_schema_digest")(_digest)

    @field_validator("source_revision")
    @classmethod
    def _source_revision(cls, value: str) -> str:
        if not _GIT_REVISION_RE.fullmatch(value):
            raise ValueError("source_revision must be a full lowercase Git revision")
        return value


class ExternalDistributionConformanceReceipt(ConformanceModel):
    SCHEMA: ClassVar[str] = CONFORMANCE_RECEIPT_SCHEMA
    PROTOCOL_VERSION: ClassVar[str] = "v1"

    schema_version: Literal[CONFORMANCE_RECEIPT_SCHEMA]
    receipt_digest: DigestValue
    request_digest: DigestValue
    verdict: ConformanceStatus
    harness: ConformanceHarnessIdentity
    subject: ConformanceSubject
    authority: ConformanceAuthority
    arms: tuple[ConformanceArmResult, ...]
    claim: ConformanceClaim
    evidence: ConformanceEvidence
    diagnostics: tuple[ConformanceDiagnostic, ...]

    _digests = field_validator("receipt_digest", "request_digest")(_digest)

    @model_validator(mode="after")
    def _receipt_consistent(self) -> "ExternalDistributionConformanceReceipt":
        arm_ids = tuple(item.arm for item in self.arms)
        if len(arm_ids) != len(set(arm_ids)):
            raise ValueError("receipt must contain exactly one result per arm")
        if set(arm_ids) != set(self.claim.requested_arms):
            raise ValueError("receipt arms must equal the claim requested arms")
        completed = _ordered_arms(
            tuple(item.arm for item in self.arms if item.status != "blocked")
        )
        if completed != self.claim.completed_arms:
            raise ValueError("claim completed arms must equal non-blocked receipt arms")
        expected_verdict = aggregate_conformance_verdict(self.arms)
        if self.verdict != expected_verdict:
            raise ValueError("verdict must aggregate fail over blocked over pass")
        expected_satisfied = self.verdict == "pass" and set(
            _mandatory_arms(self.claim.profile)
        ).issubset(arm_ids)
        if self.claim.satisfied is not expected_satisfied:
            raise ValueError(
                "claim satisfaction must derive from verdict and mandatory arms"
            )
        if self.subject.subject_kind != self.claim.subject_kind:
            raise ValueError("receipt subject kind must match the claim subject kind")
        passed = {item.arm for item in self.arms if item.status == "pass"}
        executable_passed = passed - {"contract"}
        if executable_passed:
            if (
                self.authority.observed_artifact_digest is None
                or self.authority.release_manifest_digest is None
                or self.authority.release_digest is None
                or not self.authority.bundle_identities
            ):
                raise ValueError(
                    "passing executable arms require observed artifact, release, "
                    "and bundle identities"
                )
        if (
            "artifact_materialization" in passed
            and self.evidence.materialization is None
        ):
            raise ValueError(
                "passing artifact_materialization requires typed materialization proof"
            )
        if "first_publish" in passed:
            publish = self.evidence.publish
            if publish is None:
                raise ValueError(
                    "passing first_publish requires typed published-generation proof"
                )
            authority_bindings = (
                (self.authority.definition_digest, publish.definition_digest),
                (self.authority.lock_digest, publish.lock_digest),
                (self.authority.composition_digest, publish.composition_digest),
                (self.authority.plan_digest, publish.plan_digest),
                (self.authority.sealed_release_digest, publish.sealed_release_digest),
                (self.authority.applied_instance_id, publish.applied_instance_id),
            )
            if any(
                authority is None or authority != proof
                for authority, proof in authority_bindings
            ):
                raise ValueError(
                    "published proof must bind the receipt's definition, lock, "
                    "composition, plan, sealed-release, and applied-instance authority"
                )
        if "readback_health" in passed:
            readiness = self.evidence.readiness
            if (
                self.evidence.publish is None
                or not self.evidence.readback
                or not all(item.positive for item in self.evidence.readback)
                or readiness is None
                or not readiness.ready
                or readiness.doctor != "clean"
                or not readiness.mcp_graph_read
            ):
                raise ValueError(
                    "passing readback_health requires positive generation-bound "
                    "readback and clean readiness proof"
                )
        if "idempotent_reapply" in passed and self.evidence.reapply is None:
            raise ValueError(
                "passing idempotent_reapply requires typed zero-mutation proof"
            )
        if "idempotent_reapply" in passed:
            reapply = self.evidence.reapply
            assert reapply is not None
            if (
                reapply.lock_digest != self.authority.lock_digest
                or reapply.plan_digest != self.authority.plan_digest
            ):
                raise ValueError(
                    "idempotent reapply proof must bind the exact lock and plan"
                )
        if "update_rollback_recovery" in passed and self.evidence.recovery is None:
            raise ValueError(
                "passing update_rollback_recovery requires typed recovery proof"
            )
        if "update_rollback_recovery" in passed:
            recovery = self.evidence.recovery
            assert recovery is not None
            if (
                recovery.lock_digest != self.authority.lock_digest
                or recovery.plan_digest != self.authority.plan_digest
                or recovery.candidate_sealed_release_artifact_id
                != self.authority.sealed_release_digest
            ):
                raise ValueError(
                    "recovery proof must bind the exact lock, plan, and candidate "
                    "sealed release"
                )
        if "legacy_authority" in passed and self.evidence.legacy_authority is None:
            raise ValueError(
                "passing legacy_authority requires typed migration-audit proof"
            )
        if passed & {"first_publish", "readback_health"} and not self.evidence.signals:
            raise ValueError(
                "passing publish or readback arms require signal-keyed proof"
            )
        registered = {(item.arm, item.code) for item in self.diagnostics}
        referenced = {
            (arm.arm, code) for arm in self.arms for code in arm.diagnostic_codes
        }
        if referenced != registered:
            raise ValueError(
                "arm diagnostic codes and same-arm diagnostics must match exactly"
            )
        return self


# V2 changes explicit wire identity and makes the provider connection of a
# deferred credential optional (issue 1794, operator decision 2026-09-23: a
# package need not carry a provider-connection asset). Inherited validators
# remain the single authority for credentials, admission, legacy inventory and
# execution evidence; a provider connection that IS named is still validated.
def _provider_connection_pair(value: Any) -> Any:
    if (value.provider_connection_ref is None) != (
        value.provider_connection_digest is None
    ):
        raise ValueError(
            "provider_connection_ref and provider_connection_digest must be "
            "given together or both omitted"
        )
    return value


class ConformanceOptionalCredentialBindingV2(ConformanceOptionalCredentialBinding):
    provider_connection_ref: IdentifierValue | None = None
    provider_connection_digest: DigestValue | None = None

    _identifiers = field_validator("binding_id", "declaration_ref")(_identifier)
    _provider_connection_ref = field_validator("provider_connection_ref")(
        _optional_identifier
    )
    _provider_connection_both = model_validator(mode="after")(
        _provider_connection_pair
    )


class ConformanceInputsV2(ConformanceInputs):
    optional_credential_bindings: tuple[
        ConformanceOptionalCredentialBindingV2, ...
    ] = ()


class ConformanceOptionalCredentialUnavailableEvidenceV2(
    ConformanceOptionalCredentialUnavailableEvidence
):
    provider_connection_ref: IdentifierValue | None = None
    provider_connection_digest: DigestValue | None = None

    _identifiers = field_validator("binding_id", "declaration_ref")(_identifier)
    _provider_connection_ref = field_validator("provider_connection_ref")(
        _optional_identifier
    )
    _provider_connection_both = model_validator(mode="after")(
        _provider_connection_pair
    )


class ConformanceDiagnosticV2(ConformanceDiagnostic):
    optional_credential: ConformanceOptionalCredentialUnavailableEvidenceV2 | None = None


def _widen_to_v2(value: Any, base: type[BaseModel], widened: type[BaseModel]) -> Any:
    """Accept a v1-shaped part where v2 widened its type.

    Every v1 value of these parts is a valid v2 value, so a caller that built
    the part with the shared model is re-read through the v2 model rather
    than refused for its Python class.
    """
    if isinstance(value, base) and not isinstance(value, widened):
        return value.model_dump(mode="python", by_alias=True)
    return value


class ConformanceArtifactIdentityV2(ConformanceArtifactIdentity):
    package_schema: Literal[
        "okg.deployment_product.distribution_package/v1",
        "okg.deployment_product.distribution_package/v2",
    ]


class ConformanceSubjectV2(ConformanceSubject):
    artifact: ConformanceArtifactIdentityV2


class ConformanceCompatibilityV2(ConformanceCompatibility):
    harness_protocol: Literal[CONFORMANCE_PROTOCOL_V2]


class ConformanceHarnessIdentityV2(ConformanceHarnessIdentity):
    harness_protocol: Literal[CONFORMANCE_PROTOCOL_V2]


class ExternalDistributionConformanceRequestV2(ExternalDistributionConformanceRequest):
    SCHEMA: ClassVar[str] = CONFORMANCE_REQUEST_SCHEMA_V2
    PROTOCOL_VERSION: ClassVar[str] = "v2"

    schema_version: Literal[CONFORMANCE_REQUEST_SCHEMA_V2]
    subject: ConformanceSubjectV2
    inputs: ConformanceInputsV2
    compatibility: ConformanceCompatibilityV2

    @field_validator("inputs", mode="before")
    @classmethod
    def _v2_inputs(cls, value: Any) -> Any:
        return _widen_to_v2(value, ConformanceInputs, ConformanceInputsV2)


class ExternalDistributionConformanceReceiptV2(ExternalDistributionConformanceReceipt):
    SCHEMA: ClassVar[str] = CONFORMANCE_RECEIPT_SCHEMA_V2
    PROTOCOL_VERSION: ClassVar[str] = "v2"

    schema_version: Literal[CONFORMANCE_RECEIPT_SCHEMA_V2]
    subject: ConformanceSubjectV2
    harness: ConformanceHarnessIdentityV2
    diagnostics: tuple[ConformanceDiagnosticV2, ...]

    @field_validator("diagnostics", mode="before")
    @classmethod
    def _v2_diagnostics(cls, value: Any) -> Any:
        if isinstance(value, (tuple, list)):
            return tuple(
                _widen_to_v2(item, ConformanceDiagnostic, ConformanceDiagnosticV2)
                for item in value
            )
        return value


@dataclass(frozen=True, slots=True)
class _ConformanceProtocol:
    protocol: str
    catalog_schema: str
    request: type[ExternalDistributionConformanceRequest]
    receipt: type[ExternalDistributionConformanceReceipt]


_PROTOCOLS = {
    "v1": _ConformanceProtocol(
        CONFORMANCE_PROTOCOL,
        CONFORMANCE_SCHEMA_CATALOG,
        ExternalDistributionConformanceRequest,
        ExternalDistributionConformanceReceipt,
    ),
    "v2": _ConformanceProtocol(
        CONFORMANCE_PROTOCOL_V2,
        CONFORMANCE_SCHEMA_CATALOG_V2,
        ExternalDistributionConformanceRequestV2,
        ExternalDistributionConformanceReceiptV2,
    ),
}


def _conformance_protocol(protocol_version: str) -> _ConformanceProtocol:
    try:
        return _PROTOCOLS[protocol_version]
    except (KeyError, TypeError) as exc:
        raise ConformanceContractError(
            "conformance_schema_unsupported",
            "protocol_version",
            expected="v1-or-v2",
            observed="unsupported-version",
        ) from exc


@dataclass(frozen=True, slots=True)
class ValidatedConformanceExchange:
    request: ExternalDistributionConformanceRequest
    receipt: ExternalDistributionConformanceReceipt


class ConformanceContractError(ValueError):
    """Stable, redacted validation failure at the public wire boundary."""

    def __init__(
        self,
        code: str,
        path: str,
        *,
        expected: str | None = None,
        observed: str | None = None,
    ) -> None:
        super().__init__(code)
        self.code = code
        self.path = path
        self.expected = expected
        self.observed = observed

    def to_dict(self) -> dict[str, str | None]:
        return {
            "code": self.code,
            "path": self.path,
            "expected": self.expected,
            "observed": self.observed,
        }


def _sort_product_assets(payload: dict[str, Any]) -> None:
    assets = payload.get("assets")
    if isinstance(assets, list):
        assets.sort(
            key=lambda item: (
                item.get("owner", ""),
                item.get("id", item.get("ref", "")),
                item.get("kind", ""),
                item.get("version") or "",
                item.get("digest") or "",
            )
        )


def _sort_product_compatibility(payload: dict[str, Any]) -> None:
    compatibility = payload.get("compatibility")
    if not isinstance(compatibility, dict):
        return
    for field in ("capabilities", "contract_versions"):
        values = compatibility.get(field)
        if isinstance(values, list):
            values.sort()


def _canonical_request_payload(
    request: ExternalDistributionConformanceRequest,
) -> dict[str, Any]:
    payload = request.model_dump(mode="json", by_alias=True)
    release = payload["selection"]["distribution_release"]
    release["bundle_ids"] = sorted(release.get("bundle_ids", []))
    _sort_product_assets(release)
    _sort_product_compatibility(release)
    bundles = payload["selection"]["bundles"]
    for bundle in bundles:
        bundle["requires_bundle_ids"] = sorted(bundle.get("requires_bundle_ids", []))
        bundle["conflicts_with_bundle_ids"] = sorted(
            bundle.get("conflicts_with_bundle_ids", [])
        )
        _sort_product_assets(bundle)
        _sort_product_compatibility(bundle)
    bundles.sort(
        key=lambda item: (item["bundle_id"], item["version"], item["bundle_digest"])
    )
    payload["case"]["requested_arms"] = list(
        _ordered_arms(payload["case"]["requested_arms"])
    )
    payload["expectations"]["signals"].sort(key=lambda item: item["signal_id"])
    payload["inputs"]["inputs"].sort(key=lambda item: item["input_id"])
    payload["inputs"]["secret_references"].sort(key=lambda item: item["reference"])
    payload["inputs"]["optional_credential_bindings"].sort(
        key=lambda item: item["binding_id"]
    )
    payload["inputs"]["sealed_releases"].sort(key=lambda item: item["role"])
    return payload


def canonical_conformance_request_bytes(
    request: ExternalDistributionConformanceRequest,
) -> bytes:
    return canonical_json_bytes(_canonical_request_payload(request))


def conformance_request_digest(
    request: ExternalDistributionConformanceRequest,
) -> str:
    return sha256_digest(canonical_conformance_request_bytes(request))


def _canonical_receipt_payload(
    receipt: ExternalDistributionConformanceReceipt,
    *,
    include_receipt_digest: bool,
) -> dict[str, Any]:
    payload = receipt.model_dump(mode="json", by_alias=True)
    if not include_receipt_digest:
        payload.pop("receipt_digest", None)
    order = {arm: index for index, arm in enumerate(CONFORMANCE_ARM_ORDER)}
    payload["arms"].sort(key=lambda item: order[item["arm"]])
    payload["claim"]["requested_arms"] = list(
        _ordered_arms(payload["claim"]["requested_arms"])
    )
    payload["claim"]["completed_arms"] = list(
        _ordered_arms(payload["claim"]["completed_arms"])
    )
    payload["authority"]["bundle_identities"].sort(
        key=lambda item: (item["bundle_id"], item["version"], item["digest"])
    )
    payload["diagnostics"].sort(
        key=lambda item: (item["code"], item["arm"], item["reference"])
    )
    payload["evidence"]["signals"].sort(key=lambda item: item["signal_id"])
    payload["evidence"]["readback"].sort(key=lambda item: item["check_id"])
    publish = payload["evidence"].get("publish")
    if isinstance(publish, dict):
        publish["source_deltas"].sort(key=lambda item: item["source_id"])
    reapply = payload["evidence"].get("reapply")
    if isinstance(reapply, dict):
        reapply["source_deltas"].sort(key=lambda item: item["source_id"])
    return payload


def canonical_conformance_receipt_bytes(
    receipt: ExternalDistributionConformanceReceipt,
) -> bytes:
    return canonical_json_bytes(
        _canonical_receipt_payload(receipt, include_receipt_digest=True)
    )


def conformance_receipt_digest(
    receipt: ExternalDistributionConformanceReceipt,
) -> str:
    return sha256_digest(
        canonical_json_bytes(
            _canonical_receipt_payload(receipt, include_receipt_digest=False)
        )
    )


def _load_json_document(payload: bytes | str, *, kind: str) -> Mapping[str, Any]:
    def _object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ConformanceContractError(
                    f"conformance_{kind}_duplicate_key",
                    "$",
                    observed="duplicate-key",
                )
            result[key] = value
        return result

    try:
        value = json.loads(payload, object_pairs_hook=_object)
    except ConformanceContractError:
        raise
    except (json.JSONDecodeError, UnicodeDecodeError, TypeError) as exc:
        raise ConformanceContractError(
            f"conformance_{kind}_invalid_json",
            "$",
            observed=type(exc).__name__,
        ) from exc
    if not isinstance(value, Mapping):
        raise ConformanceContractError(
            f"conformance_{kind}_invalid",
            "$",
            expected="object",
            observed=type(value).__name__,
        )
    return value


def _conformance_document_model(
    payload: bytes | str | Mapping[str, Any],
    *,
    kind: Literal["request", "receipt"],
) -> tuple[
    Mapping[str, Any],
    type[ExternalDistributionConformanceRequest]
    | type[ExternalDistributionConformanceReceipt],
]:
    if kind not in ("request", "receipt"):
        raise ConformanceContractError(
            "conformance_schema_unsupported",
            "kind",
            expected="request-or-receipt",
            observed="unsupported-kind",
        )
    raw = (
        _load_json_document(payload, kind=kind)
        if isinstance(payload, (bytes, str))
        else payload
    )
    model = next(
        (
            getattr(item, kind)
            for item in _PROTOCOLS.values()
            if getattr(item, kind).SCHEMA == raw.get("schema_version")
        ),
        None,
    )
    if model is None:
        raise ConformanceContractError(
            f"conformance_{kind}_schema_unsupported",
            "schema_version",
            expected=getattr(_PROTOCOLS["v1"], kind).SCHEMA,
            observed="unsupported-schema",
        )
    return raw, model


def conformance_document_protocol_version(
    payload: bytes | str | Mapping[str, Any],
    *,
    kind: Literal["request", "receipt"] = "request",
) -> str:
    """Select the protocol by an explicit supported wire tag, without validating its body."""

    _, model = _conformance_document_model(payload, kind=kind)
    return model.PROTOCOL_VERSION


def _load_conformance_document(
    payload: bytes | str | Mapping[str, Any],
    *,
    kind: Literal["request", "receipt"],
) -> ExternalDistributionConformanceRequest | ExternalDistributionConformanceReceipt:
    raw, model = _conformance_document_model(payload, kind=kind)
    try:
        return model.model_validate(raw)
    except ValidationError as exc:
        path = ".".join(str(item) for item in exc.errors()[0].get("loc", ())) or "$"
        raise ConformanceContractError(
            f"conformance_{kind}_invalid",
            path,
            observed=exc.errors()[0].get("type", "validation-error"),
        ) from exc


def load_conformance_request(
    payload: bytes | str | Mapping[str, Any],
) -> ExternalDistributionConformanceRequest:
    return cast(
        ExternalDistributionConformanceRequest,
        _load_conformance_document(payload, kind="request"),
    )


def load_conformance_receipt(
    payload: bytes | str | Mapping[str, Any],
) -> ExternalDistributionConformanceReceipt:
    return cast(
        ExternalDistributionConformanceReceipt,
        _load_conformance_document(payload, kind="receipt"),
    )


def validate_conformance_exchange(
    request: ExternalDistributionConformanceRequest,
    receipt: ExternalDistributionConformanceReceipt,
    *,
    trusted_external_review: ConformanceExternalEvidenceOrigin | None = None,
) -> ValidatedConformanceExchange:
    # Pydantic model_copy/model_construct bypass validation. This public boundary
    # accepts typed inputs, but grants them no authority beyond their wire data.
    request = load_conformance_request(request.model_dump(mode="json", by_alias=True))
    receipt = load_conformance_receipt(receipt.model_dump(mode="json", by_alias=True))
    if request.PROTOCOL_VERSION != receipt.PROTOCOL_VERSION:
        raise ConformanceContractError(
            "conformance_schema_unsupported",
            "receipt.schema_version",
            expected=_conformance_protocol(request.PROTOCOL_VERSION).receipt.SCHEMA,
            observed=receipt.schema_version,
        )
    expected_request_digest = conformance_request_digest(request)
    if receipt.request_digest != expected_request_digest:
        raise ConformanceContractError(
            "conformance_request_digest_mismatch",
            "request_digest",
            expected=expected_request_digest,
            observed=receipt.request_digest,
        )
    expected_receipt_digest = conformance_receipt_digest(receipt)
    if receipt.receipt_digest != expected_receipt_digest:
        raise ConformanceContractError(
            "conformance_receipt_digest_mismatch",
            "receipt_digest",
            expected=expected_receipt_digest,
            observed=receipt.receipt_digest,
        )
    if receipt.subject != request.subject:
        raise ConformanceContractError(
            "conformance_claim_scope_invalid",
            "subject",
            expected="request-subject",
            observed="receipt-subject-mismatch",
        )
    if receipt.claim.profile != request.case.claim_profile:
        raise ConformanceContractError(
            "conformance_claim_scope_invalid",
            "claim.profile",
            expected=request.case.claim_profile,
            observed=receipt.claim.profile,
        )
    if set(receipt.claim.requested_arms) != set(request.case.requested_arms):
        raise ConformanceContractError(
            "conformance_claim_scope_invalid",
            "claim.requested_arms",
            expected="request-arms",
            observed="receipt-arms-mismatch",
        )
    admission_pairs = {
        "extension_boundary_digest": (
            request.admission.extension_boundary_digest,
            receipt.authority.extension_boundary_digest,
        ),
        "package_admission_digest": (
            request.admission.package_admission_digest,
            receipt.authority.package_admission_digest,
        ),
        "capability_digest": (
            request.admission.capability_digest,
            receipt.authority.capability_digest,
        ),
        "resource_policy_digest": (
            request.admission.resource_policy_digest,
            receipt.authority.resource_policy_digest,
        ),
    }
    for field, (expected, observed) in admission_pairs.items():
        if expected != observed:
            raise ConformanceContractError(
                "conformance_admission_digest_mismatch",
                f"authority.{field}",
                expected=expected,
                observed=observed,
            )

    optional_bindings = {
        item.binding_id: item for item in request.inputs.optional_credential_bindings
    }
    supplied_secret_references = {
        item.reference for item in request.inputs.secret_references
    }
    declared_assets = {
        item.qualified_id: item.digest
        for item in (
            *request.selection.distribution_release.asset_refs,
            *(asset for bundle in request.selection.bundles for asset in bundle.asset_refs),
        )
        if item.digest is not None
    }
    for diagnostic in receipt.diagnostics:
        if diagnostic.code == "conformance_declared_optional_credential_unavailable":
            context = diagnostic.optional_credential
            assert context is not None
            binding = optional_bindings.get(context.binding_id)
            if binding is None or (
                context.declaration_ref != binding.declaration_ref
                or context.declaration_digest != binding.declaration_digest
                or context.provider_connection_ref
                != binding.provider_connection_ref
                or context.provider_connection_digest
                != binding.provider_connection_digest
                or context.credential_reference != binding.credential_reference
                or context.credential_reference in supplied_secret_references
            ):
                raise ConformanceContractError(
                    "conformance_claim_scope_invalid",
                    f"diagnostics.{diagnostic.reference}.optional_credential",
                    expected="eligible-unsupplied-deferred-credential-binding",
                    observed="credential-binding-mismatch",
                )
        if diagnostic.code == "conformance_distribution_declared_asset_missing":
            context = diagnostic.declared_asset
            assert context is not None
            if declared_assets.get(context.asset_ref) != context.asset_digest:
                raise ConformanceContractError(
                    "conformance_claim_scope_invalid",
                    f"diagnostics.{diagnostic.reference}.declared_asset",
                    expected="digest-bound-declared-distribution-asset",
                    observed="undeclared-or-digest-mismatched-asset",
                )

    origin = receipt.claim.external_evidence_origin
    if origin is not None:
        if origin.acknowledgement_status == "current":
            if trusted_external_review is None:
                raise ConformanceContractError(
                    "conformance_external_acknowledgement_pending",
                    "claim.external_evidence_origin",
                    expected="trusted-external-review-authority",
                    observed="self-asserted-current-acknowledgement",
                )
            if origin != trusted_external_review:
                raise ConformanceContractError(
                    "conformance_claim_scope_invalid",
                    "claim.external_evidence_origin",
                    expected="trusted-external-review-authority",
                    observed="review-authority-mismatch",
                )
        if origin.source_revision != request.subject.source_revision:
            raise ConformanceContractError(
                "conformance_claim_scope_invalid",
                "claim.external_evidence_origin.source_revision",
                expected="request-source-revision",
                observed="consumer-source-revision-mismatch",
            )
        if (
            origin.acknowledgement_status == "current"
            and origin.acknowledged_schema_digest
            != conformance_schema_catalog(protocol_version=request.PROTOCOL_VERSION)[
                "schema_digest"
            ]
        ):
            raise ConformanceContractError(
                "conformance_claim_scope_invalid",
                "claim.external_evidence_origin.acknowledged_schema_digest",
                expected="current-schema-catalog-digest",
                observed="stale-schema-acknowledgement",
            )

    passed_arms = {result.arm for result in receipt.arms if result.status == "pass"}
    executable_passed = bool(passed_arms - {"contract"})
    if executable_passed:
        authority = receipt.authority
        if (
            "first_publish" in passed_arms
            and authority.definition_digest
            != request.selection.definition.definition_digest
        ):
            raise ConformanceContractError(
                "conformance_definition_digest_mismatch",
                "authority.definition_digest",
                expected=request.selection.definition.definition_digest,
                observed=authority.definition_digest,
            )
        if authority.observed_artifact_digest != request.subject.artifact.digest:
            raise ConformanceContractError(
                "conformance_artifact_digest_mismatch",
                "authority.observed_artifact_digest",
                expected=request.subject.artifact.digest,
                observed=authority.observed_artifact_digest,
            )
        if (
            authority.release_manifest_digest != request.subject.release_digest
            or authority.release_digest != request.subject.release_digest
        ):
            raise ConformanceContractError(
                "conformance_release_digest_mismatch",
                "authority.release_digest",
                expected=request.subject.release_digest,
                observed=authority.release_digest,
            )
        expected_bundles = {
            (bundle.bundle_id, bundle.version, bundle.bundle_digest)
            for bundle in request.selection.bundles
        }
        observed_bundles = {
            (bundle.bundle_id, bundle.version, bundle.digest)
            for bundle in authority.bundle_identities
        }
        if expected_bundles != observed_bundles:
            raise ConformanceContractError(
                "conformance_bundle_digest_mismatch",
                "authority.bundle_identities",
                expected="selected-bundles",
                observed="observed-bundles-mismatch",
            )
    if passed_arms & {"first_publish", "readback_health"}:
        expected_signals = {
            item.signal_id: item for item in request.expectations.signals
        }
        observed_signals = {item.signal_id: item for item in receipt.evidence.signals}
        if set(expected_signals) != set(observed_signals):
            raise ConformanceContractError(
                "conformance_signal_evidence_mismatch",
                "evidence.signals",
                expected="requested-signals",
                observed="signal-coverage-mismatch",
            )
        for signal_id, expected_signal in expected_signals.items():
            observed_signal = observed_signals[signal_id]
            matches = (
                observed_signal.kind == expected_signal.kind
                and observed_signal.target == expected_signal.target
                and observed_signal.positive
            )
            if expected_signal.minimum_count is not None:
                matches = matches and (
                    observed_signal.observed_count is not None
                    and observed_signal.observed_count >= expected_signal.minimum_count
                )
            if expected_signal.expected_digest is not None:
                matches = (
                    matches
                    and observed_signal.result_digest == expected_signal.expected_digest
                )
            if not matches:
                raise ConformanceContractError(
                    "conformance_signal_evidence_mismatch",
                    f"evidence.signals.{signal_id}",
                    expected="requested-signal",
                    observed="signal-proof-mismatch",
                )
        if "readback_health" in passed_arms:
            readback_by_id = {item.check_id: item for item in receipt.evidence.readback}
            required_readback_ids = {
                item.signal_id
                for item in request.expectations.signals
                if item.kind in {"view", "invariant", "interface", "readiness"}
            }
            if not required_readback_ids.issubset(readback_by_id):
                raise ConformanceContractError(
                    "conformance_signal_evidence_mismatch",
                    "evidence.readback",
                    expected="requested-read-signals",
                    observed="readback-coverage-mismatch",
                )
            if any(
                readback_by_id[signal_id].result_digest
                != observed_signals[signal_id].result_digest
                for signal_id in required_readback_ids
            ):
                raise ConformanceContractError(
                    "conformance_signal_evidence_mismatch",
                    "evidence.readback",
                    expected="signal-bound-readback",
                    observed="readback-digest-mismatch",
                )
    if "update_rollback_recovery" in passed_arms:
        releases = {item.role: item for item in request.inputs.sealed_releases}
        recovery = receipt.evidence.recovery
        assert recovery is not None
        if (
            recovery.baseline_sealed_release_artifact_id
            != releases["baseline"].artifact_id
            or recovery.candidate_sealed_release_artifact_id
            != releases["candidate"].artifact_id
        ):
            raise ConformanceContractError(
                "conformance_claim_scope_invalid",
                "evidence.recovery",
                expected="request-sealed-release-pair",
                observed="sealed-release-pair-mismatch",
            )
    if "legacy_authority" in passed_arms:
        inventory = request.inputs.legacy_input_inventory
        evidence = receipt.evidence.legacy_authority
        assert inventory is not None and evidence is not None
        if (
            evidence.declared_inventory_digest != inventory.inventory_digest
            or evidence.declared_inventory_entry_count != inventory.entry_count
        ):
            raise ConformanceContractError(
                "conformance_claim_scope_invalid",
                "evidence.legacy_authority",
                expected="request-declared-legacy-inventory",
                observed="legacy-inventory-mismatch",
            )
    return ValidatedConformanceExchange(request=request, receipt=receipt)


AcknowledgementReissueReason: TypeAlias = Literal[
    "acknowledgement_not_current",
    "consumer_source_revision_changed",
    "schema_catalog_changed",
]


def acknowledgement_reissue_reasons(
    trusted_review: ConformanceExternalEvidenceOrigin,
    request: ExternalDistributionConformanceRequest,
) -> tuple[AcknowledgementReissueReason, ...]:
    """Name why a trusted acknowledgement cannot be reused for ``request``.

    Two reasons are exactly the bindings :func:`validate_conformance_exchange`
    checks on a current acknowledgement: the acknowledged consumer source
    revision must equal the request subject's, and the acknowledged schema
    digest must equal the current catalog digest of the request's protocol.
    The third, a historical record, is refused by :class:`ConformanceClaim`
    (a satisfied external claim needs a current acknowledgement), not by the
    exchange validator.  An empty tuple means the record is reusable as it
    stands; it does not authenticate the reviewer and it supplies no
    execution, credential or publication proof.
    """

    request = load_conformance_request(request.model_dump(mode="json", by_alias=True))
    try:
        trusted_review = ConformanceExternalEvidenceOrigin.model_validate(
            trusted_review.model_dump(mode="json")
        )
    except ValidationError:
        raise ConformanceContractError(
            "conformance_claim_scope_invalid",
            "trusted_external_review",
            expected="validated-trusted-review",
            observed="review-authority-invalid",
        ) from None
    reasons: list[AcknowledgementReissueReason] = []
    if trusted_review.acknowledgement_status != "current":
        reasons.append("acknowledgement_not_current")
    if trusted_review.source_revision != request.subject.source_revision:
        reasons.append("consumer_source_revision_changed")
    catalog_digest = conformance_schema_catalog(
        protocol_version=request.PROTOCOL_VERSION
    )["schema_digest"]
    if trusted_review.acknowledged_schema_digest != catalog_digest:
        reasons.append("schema_catalog_changed")
    return tuple(reasons)


def conformance_schema_catalog(*, protocol_version: str = "v1") -> dict[str, Any]:
    protocol = _conformance_protocol(protocol_version)
    schemas = {
        "request": protocol.request.model_json_schema(
            by_alias=True,
            mode="validation",
        ),
        "receipt": protocol.receipt.model_json_schema(
            by_alias=True,
            mode="validation",
        ),
    }
    schemas["request"]["$id"] = protocol.request.SCHEMA
    schemas["receipt"]["$id"] = protocol.receipt.SCHEMA
    schemas["request"]["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    schemas["receipt"]["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    schemas["request"]["x-okg-semantic-validation"] = {
        "required": True,
        "authority": protocol.protocol,
        "rules": [
            "subject-release-selection-binding",
            (
                "distribution-package-v1-transport-binding"
                if protocol_version == "v1"
                else "distribution-package-v1-v2-transport-binding"
            ),
            "closed-profile-mandatory-arms",
            "canonical-set-ordering",
            "deferred-credential-binding-and-operator-absence",
            "baseline-candidate-sealed-release-binding",
            "declared-legacy-inventory-binding",
        ],
    }
    schemas["receipt"]["x-okg-semantic-validation"] = {
        "required": True,
        "authority": protocol.protocol,
        "rules": [
            "request-and-receipt-digest-binding",
            "exact-requested-arm-coverage",
            "fail-over-blocked-over-pass",
            "arm-specific-mutation-retry-and-proof",
            (
                "distribution-package-v1-transport-binding"
                if protocol_version == "v1"
                else "distribution-package-v1-v2-transport-binding"
            ),
            "observed-artifact-release-bundle-binding",
            "definition-lock-plan-proof-binding",
            "generation-bound-readback",
            "requested-signal-proof-coverage",
            "code-profiled-optional-credential-diagnostic",
            "distribution-declared-asset-fails-not-blocks",
            "trusted-external-review-authority-binding",
            "consumer-acknowledgement-revision-and-schema-binding",
        ],
    }
    digest = sha256_digest(canonical_json_bytes(schemas))
    return {
        "schema_version": protocol.catalog_schema,
        "protocol": protocol.protocol,
        "schema_digest": digest,
        "schemas": schemas,
    }


def conformance_schema(
    kind: Literal["request", "receipt"],
    *,
    protocol_version: str = "v1",
) -> dict[str, Any]:
    protocol = _conformance_protocol(protocol_version)
    catalog = conformance_schema_catalog(protocol_version=protocol_version)
    try:
        schema = catalog["schemas"][kind]
    except KeyError as exc:
        raise ConformanceContractError(
            "conformance_schema_unsupported",
            "kind",
            expected="request-or-receipt",
            observed="unsupported-kind",
        ) from exc
    return {
        "schema_version": schema["$id"],
        "catalog_schema_version": protocol.catalog_schema,
        "protocol": protocol.protocol,
        "kind": kind,
        "schema_digest": catalog["schema_digest"],
        "schema": schema,
    }


__all__ = [
    "AcknowledgementReissueReason",
    "acknowledgement_reissue_reasons",
    "conformance_document_protocol_version",
    "CONFORMANCE_PROTOCOL_V2",
    "CONFORMANCE_DISTRIBUTION_PACKAGE_SCHEMA_V2",
    "CONFORMANCE_REQUEST_SCHEMA_V2",
    "CONFORMANCE_RECEIPT_SCHEMA_V2",
    "CONFORMANCE_SCHEMA_CATALOG_V2",
    "CONFORMANCE_VALIDATION_RESULT_SCHEMA_V2",
    "ConformanceArtifactIdentityV2",
    "ConformanceSubjectV2",
    "ConformanceCompatibilityV2",
    "ConformanceHarnessIdentityV2",
    "ExternalDistributionConformanceRequestV2",
    "ExternalDistributionConformanceReceiptV2",
    "CONFORMANCE_ARM_ORDER",
    "CONFORMANCE_BLOCKED_DIAGNOSTIC_CODES",
    "CONFORMANCE_DIAGNOSTIC_CODES",
    "CONFORMANCE_MANDATORY_ARMS",
    "CONFORMANCE_PASS_CLASSIFICATIONS",
    "CONFORMANCE_PROFILE_SUBJECTS",
    "CONFORMANCE_PROTOCOL",
    "CONFORMANCE_PROTOCOL_VERSION",
    "CONFORMANCE_DISTRIBUTION_PACKAGE_SCHEMA",
    "CONFORMANCE_RECEIPT_SCHEMA",
    "CONFORMANCE_REQUEST_SCHEMA",
    "CONFORMANCE_SCHEMA_CATALOG",
    "CONFORMANCE_VALIDATION_RESULT_SCHEMA",
    "EXTERNAL_CONFORMANCE_RECEIPT_SCHEMA",
    "EXTERNAL_CONFORMANCE_REQUEST_SCHEMA",
    "ConformanceAdmission",
    "ConformanceArm",
    "ConformanceArmResult",
    "ConformanceArtifactIdentity",
    "ConformanceAuthority",
    "ConformanceCase",
    "ConformanceClaim",
    "ConformanceClaimProfile",
    "ConformanceCompatibility",
    "ConformanceContractError",
    "ConformanceDeclaredAssetMissingEvidence",
    "ConformanceDefinitionReference",
    "ConformanceDiagnostic",
    "ConformanceDiagnosticCode",
    "ConformanceEvidence",
    "ConformanceExternalEvidenceOrigin",
    "ConformanceExpectedSignal",
    "ConformanceExpectations",
    "ConformanceHarnessIdentity",
    "ConformanceInputReference",
    "ConformanceInputs",
    "ConformanceLegacyInputInventory",
    "ConformanceLegacyAuthorityEvidence",
    "ConformanceMaterializationEvidence",
    "ConformanceObservedBundleIdentity",
    "ConformanceOptionalCredentialBinding",
    "ConformanceOptionalCredentialBindingV2",
    "ConformanceInputsV2",
    "ConformanceDiagnosticV2",
    "ConformanceOptionalCredentialUnavailableEvidenceV2",
    "ConformanceOptionalCredentialUnavailableEvidence",
    "ConformancePublishEvidence",
    "ConformanceRecoveryEvidence",
    "ConformanceReadbackEvidence",
    "ConformanceReadinessEvidence",
    "ConformanceReapplyEvidence",
    "ConformanceSelection",
    "ConformanceSealedReleaseInput",
    "ConformanceSignalEvidence",
    "ConformanceSignalKind",
    "ConformanceSourceDelta",
    "ConformanceSubject",
    "ConformanceSubjectKind",
    "ConformanceStatus",
    "ExternalDistributionConformanceReceipt",
    "ExternalDistributionConformanceRequest",
    "OpaqueSecretReference",
    "ValidatedConformanceExchange",
    "aggregate_conformance_verdict",
    "canonical_conformance_receipt_bytes",
    "canonical_conformance_request_bytes",
    "conformance_receipt_digest",
    "conformance_request_digest",
    "conformance_schema",
    "conformance_schema_catalog",
    "load_conformance_receipt",
    "load_conformance_request",
    "validate_conformance_exchange",
]
