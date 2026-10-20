"""Generated human and agent projections of the deployment-product contract.

The objects in this module are renderers and machine envelopes over the
canonical models. They do not resolve products, prompt, or mutate runtime
state. Exact conformance fingerprints make schema and renderer changes an
intentional compatibility decision.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    field_validator,
    model_validator,
)

from .lock import (
    DeploymentProductPlan,
    LockDiagnostic,
    canonical_json_bytes,
    sha256_digest,
    validate_action_arguments,
)
from .models import (
    API_VERSION,
    CONTRACT_VERSION,
    AppliedInstance,
    AuthoredDeploymentDefinition,
    DistributionRelease,
    ProductBundle,
    ProductModel,
    ResolvedLock,
    SealedReleaseReference,
)


QUESTION_SCHEMA = "okg.deployment_product.agent_questions/v1"
PLAN_ENVELOPE_SCHEMA = "okg.deployment_product.agent_plan/v1"
OPERATION_SCHEMA = "okg.deployment_product.agent_operation/v1"
STATUS_SCHEMA = "okg.deployment_product.agent_status/v1"
READ_STATUS_SCHEMA_V1 = "okg.deployment_product.read_status/v1"
READ_STATUS_SCHEMA = "okg.deployment_product.read_status/v2"
DIAGNOSTIC_SCHEMA = "okg.deployment_product.diagnostic/v1"
PROJECTION_REGISTRY_SCHEMA = "okg.deployment_product.projection_registry/v1"
CONFORMANCE_SCHEMA = "okg.deployment_product.projection_conformance/v1"

_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_DIAGNOSTIC_TEXT: Mapping[str, tuple[str, str]] = {
    "artifact_digest_mismatch": (
        "cached artifact digest changed",
        "restore the artifact and plan again",
    ),
    "artifact_invalid": (
        "artifact cache returned invalid content",
        "repair the artifact reader and plan again",
    ),
    "artifact_missing": (
        "a pinned artifact is missing",
        "restore the artifact and plan again",
    ),
    "compatibility_contract_unsupported": (
        "a selected product artifact does not support this contract",
        "select a compatible artifact release",
    ),
    "composition_digest_mismatch": (
        "the resolved lock composition digest changed",
        "resolve the authored definition again",
    ),
    "definition_digest_mismatch": (
        "the authored deployment definition changed",
        "create and approve a new plan",
    ),
    "lock_digest_mismatch": (
        "the resolved lock digest changed",
        "resolve the deployment lock again",
    ),
    "plan_api_unsupported": (
        "the plan uses an unsupported API version",
        "create a plan with the supported deployment-product API",
    ),
    "plan_digest_mismatch": (
        "the exact approved plan changed",
        "recreate and re-approve the exact plan",
    ),
    "plan_expired": (
        "the approved plan expired",
        "create a fresh plan",
    ),
    "plan_schema_unsupported": (
        "the plan uses an unsupported schema version",
        "create a plan with the supported deployment-product schema",
    ),
    "production_controller_missing": (
        "the admitted product operation has no production apply controller",
        "install the product apply/resume controller before attempting mutation",
    ),
    "receipt_verifier_missing": (
        "the completed journal has no paired deployment receipt verification",
        "verify the journal and live release before accepting lifecycle success",
    ),
    "unregistered_diagnostic": (
        "deployment validation reported an unregistered diagnostic",
        "register the diagnostic code before exposing it to an agent",
    ),
}
_SAFE_DIAGNOSTIC_OWNERS = frozenset(
    {"distribution", "external", "instance", "operator", "resolver", "system"}
)
_SAFE_DIAGNOSTIC_PHASES = frozenset(
    {"apply", "planning", "pre_apply", "resolution", "status", "validation"}
)
_SAFE_PRECONDITIONS = frozenset(
    {
        "approved-plan",
        "artifact-restored",
        "exact-plan-digest",
        "unexpired-plan",
        "verified-artifacts",
    }
)


def _public_identifier(value: str, *, field: str) -> str:
    if not isinstance(value, str) or not value or value.strip() != value:
        raise ValueError(f"{field} must be a stable non-empty identifier")
    normalized = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    if any(
        token in normalized.split("-")
        for token in ("credential", "password", "private", "secret", "token")
    ):
        raise ValueError(f"{field} cannot contain secret-material vocabulary")
    if any(character in value for character in ("\x00", "\n", "\r")):
        raise ValueError(f"{field} contains an unsupported character")
    return value


class AgentDiagnostic(ProductModel):
    schema_version: Literal[DIAGNOSTIC_SCHEMA] = DIAGNOSTIC_SCHEMA
    code: str
    severity: Literal["info", "warning", "error"] = "error"
    path: str
    owner: str
    message: str
    fix_hint: str
    expected: str | None = None
    observed: str | None = None
    phase: str = "pre_apply"

    @model_validator(mode="before")
    @classmethod
    def _central_redaction(cls, value: object) -> object:
        if not isinstance(value, Mapping):
            return value
        data = dict(value)
        raw_code = str(data.get("code", "unregistered_diagnostic"))
        code = raw_code if raw_code in _DIAGNOSTIC_TEXT else "unregistered_diagnostic"
        message, fix_hint = _DIAGNOSTIC_TEXT.get(
            code,
            (
                "deployment validation reported a structured diagnostic",
                "inspect the diagnostic code and retry after correction",
            ),
        )
        data["code"] = code
        raw_path = str(data.get("path", "unknown"))
        data["path"] = (
            raw_path
            if _DIGEST_RE.fullmatch(raw_path)
            else sha256_digest(raw_path.encode("utf-8"))
        )
        owner = str(data.get("owner", "external"))
        data["owner"] = owner if owner in _SAFE_DIAGNOSTIC_OWNERS else "external"
        phase = str(data.get("phase", "validation"))
        data["phase"] = phase if phase in _SAFE_DIAGNOSTIC_PHASES else "validation"
        data["message"] = message
        data["fix_hint"] = fix_hint
        for field in ("expected", "observed"):
            candidate = data.get(field)
            if candidate is not None and (
                not isinstance(candidate, str) or not _DIGEST_RE.fullmatch(candidate)
            ):
                data[field] = "<redacted>"
        return data


class AgentQuestion(ProductModel):
    question_id: str
    prompt: str
    why: str
    answer_schema: Mapping[str, JsonValue]
    choices: tuple[JsonValue, ...] = ()
    safe_default: JsonValue = None
    safe_default_reason: str
    sensitive: bool = False
    storage_class: Literal[
        "definition_value",
        "product_reference",
        "secret_reference",
    ]
    affected_assets: tuple[str, ...] = ()
    requires_human: bool = False


class AgentActionArguments(BaseModel):
    """Closed reference/digest-only argument object for agent actions."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    artifact: str | None = None
    artifact_id: str | None = None
    bundle_id: str | None = None
    connection_ref: str | None = None
    definition_id: str | None = None
    deployment_id: str | None = None
    environment_id: str | None = None
    lock_digest: str | None = None
    operation_id: str | None = None
    package_id: str | None = None
    plan_digest: str | None = None
    sealed_release_digest: str | None = None
    source_ref: str | None = None
    target: str | None = None

    @model_validator(mode="after")
    def _closed_values(self) -> "AgentActionArguments":
        validate_action_arguments(self.model_dump(exclude_none=True))
        return self


class AgentAction(ProductModel):
    action_id: str
    kind: str
    arguments: AgentActionArguments = Field(default_factory=AgentActionArguments)
    preconditions: tuple[str, ...] = ()
    display_command: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _drop_display_prose(cls, value: object) -> object:
        if isinstance(value, Mapping):
            data = dict(value)
            data["display_command"] = None
            return data
        return value

    @field_validator("action_id", "kind")
    @classmethod
    def _identifiers(cls, value: str, info) -> str:
        return _public_identifier(value, field=info.field_name)

    @field_validator("preconditions")
    @classmethod
    def _closed_preconditions(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        unknown = set(value) - _SAFE_PRECONDITIONS
        if unknown:
            raise ValueError(f"unregistered action preconditions: {sorted(unknown)!r}")
        return tuple(sorted(set(value)))


class AgentQuestionEnvelope(ProductModel):
    schema_version: Literal[QUESTION_SCHEMA] = QUESTION_SCHEMA
    operation_id: str
    deployment_id: str
    state: Literal["questions_required"] = "questions_required"
    questions: tuple[AgentQuestion, ...]
    diagnostics: tuple[AgentDiagnostic, ...] = ()
    mutation_classification: Literal["read_only"] = "read_only"
    safe_to_retry: Literal[True] = True

    @field_validator("operation_id", "deployment_id")
    @classmethod
    def _identifiers(cls, value: str, info) -> str:
        return _public_identifier(value, field=info.field_name)


class AgentPlanEnvelope(ProductModel):
    schema_version: Literal[PLAN_ENVELOPE_SCHEMA] = PLAN_ENVELOPE_SCHEMA
    operation_id: str
    deployment_id: str
    state: Literal["plan_ready"] = "plan_ready"
    plan_digest: str
    lock_digest: str
    composition_digest: str
    expires_at: datetime
    summary: str
    actions: tuple[AgentAction, ...] = ()
    approval_required: bool = True
    mutation_classification: Literal["planned_mutation"] = "planned_mutation"
    safe_to_retry: Literal[True] = True

    @field_validator("operation_id", "deployment_id")
    @classmethod
    def _identifiers(cls, value: str, info) -> str:
        return _public_identifier(value, field=info.field_name)

    @model_validator(mode="before")
    @classmethod
    def _derived_summary(cls, value: object) -> object:
        if isinstance(value, Mapping):
            data = dict(value)
            actions = data.get("actions", ())
            action_count = len(actions) if isinstance(actions, (list, tuple)) else 0
            data["summary"] = f"{action_count} planned deployment action(s)"
            return data
        return value

    @field_validator("plan_digest", "lock_digest", "composition_digest")
    @classmethod
    def _digest(cls, value: str) -> str:
        if not _DIGEST_RE.fullmatch(value):
            raise ValueError("plan and lock identities must be sha256 digests")
        return value

    @field_validator("expires_at")
    @classmethod
    def _aware_expiry(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("plan expiry must be timezone-aware")
        return value.astimezone(timezone.utc)


class AgentOperationEnvelope(ProductModel):
    schema_version: Literal[OPERATION_SCHEMA] = OPERATION_SCHEMA
    operation_id: str
    deployment_id: str
    state: Literal["requested", "accepted", "refused"] = "requested"
    action: AgentAction
    plan_digest: str | None = None
    idempotency_key: str | None = None
    mutation_classification: Literal["read_only", "mutating"]
    safe_to_retry: bool
    diagnostics: tuple[AgentDiagnostic, ...] = ()

    @field_validator("operation_id", "deployment_id", "idempotency_key")
    @classmethod
    def _identifiers(cls, value: str | None, info) -> str | None:
        if value is None:
            return None
        return _public_identifier(value, field=info.field_name)

    @field_validator("plan_digest")
    @classmethod
    def _optional_plan_digest(cls, value: str | None) -> str | None:
        if value is not None and not _DIGEST_RE.fullmatch(value):
            raise ValueError("plan identity must be a sha256 digest")
        return value

    @model_validator(mode="after")
    def _mutations_are_fenced(self) -> "AgentOperationEnvelope":
        if self.mutation_classification == "mutating":
            if self.plan_digest is None or not _DIGEST_RE.fullmatch(self.plan_digest):
                raise ValueError("mutating operations require the exact plan digest")
            if self.idempotency_key is None or not self.idempotency_key.strip():
                raise ValueError("mutating operations require an idempotency key")
        return self


class AgentStatusEnvelope(ProductModel):
    schema_version: Literal[STATUS_SCHEMA] = STATUS_SCHEMA
    operation_id: str
    deployment_id: str
    state: Literal[
        "planned",
        "running",
        "blocked",
        "succeeded",
        "failed",
    ]
    phase: str
    progress_percent: int = Field(ge=0, le=100)
    summary: str
    diagnostics: tuple[AgentDiagnostic, ...] = ()
    next_actions: tuple[AgentAction, ...] = ()
    mutation_classification: Literal["read_only", "mutating"]
    safe_to_retry: bool

    @field_validator("operation_id", "deployment_id")
    @classmethod
    def _identifiers(cls, value: str, info) -> str:
        return _public_identifier(value, field=info.field_name)

    @model_validator(mode="before")
    @classmethod
    def _derived_status_text(cls, value: object) -> object:
        if isinstance(value, Mapping):
            data = dict(value)
            state = str(data.get("state", "blocked"))
            phase = str(data.get("phase", "status"))
            data["phase"] = phase if phase in _SAFE_DIAGNOSTIC_PHASES else "status"
            data["summary"] = f"deployment operation is {state}"
            return data
        return value


class ReadProjectionModel(BaseModel):
    """Strict read-side model over existing runtime and product authorities."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


def _read_reference(value: str, *, field: str) -> str:
    value = _public_identifier(value, field=field)
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:@#-]*", value):
        raise ValueError(f"{field} must be a closed stable reference, not a path or URI")
    if "://" in value:
        raise ValueError(f"{field} cannot contain URI authority or credential material")
    return value


class ReadBlocker(ReadProjectionModel):
    code: str
    phase: str
    retryable: bool

    @field_validator("code", "phase")
    @classmethod
    def _identifiers(cls, value: str, info) -> str:
        return _public_identifier(value, field=info.field_name)


class ReadEvidenceReference(ReadProjectionModel):
    evidence_kind: Literal["graph", "runtime", "materialization"]
    reference: str
    status: Literal["current", "stale", "missing", "unavailable", "published"]
    generation_id: str | None = None

    @field_validator("reference", "generation_id")
    @classmethod
    def _references(cls, value: str | None, info) -> str | None:
        if value is None:
            return None
        return _read_reference(value, field=info.field_name)


class ReadMaterializationStatus(ReadProjectionModel):
    asset_id: str
    asset_digest: str
    generation_id: str
    status: Literal["planned", "published", "blocked", "missing"]
    lineage_reference: str

    @field_validator("asset_id", "generation_id", "lineage_reference")
    @classmethod
    def _identifiers(cls, value: str, info) -> str:
        return _read_reference(value, field=info.field_name)

    @field_validator("asset_digest")
    @classmethod
    def _digest(cls, value: str) -> str:
        if not _DIGEST_RE.fullmatch(value):
            raise ValueError("materialization asset identity must be a sha256 digest")
        return value


class ReadProvenanceReference(ReadProjectionModel):
    owner: str
    asset_id: str
    asset_digest: str
    source_reference: str

    @field_validator("owner", "asset_id", "source_reference")
    @classmethod
    def _identifiers(cls, value: str, info) -> str:
        return _read_reference(value, field=info.field_name)

    @field_validator("asset_digest")
    @classmethod
    def _digest(cls, value: str) -> str:
        if not _DIGEST_RE.fullmatch(value):
            raise ValueError("provenance asset identity must be a sha256 digest")
        return value


class ReadPolicySummary(ReadProjectionModel):
    audience_ids: tuple[str, ...] = ()
    tenant_ids: tuple[str, ...] = ()
    visible_fields: tuple[str, ...] = ()
    masked_fields: tuple[str, ...] = ()
    decision_ids: tuple[str, ...] = ()

    @model_validator(mode="after")
    def _closed_sets(self) -> "ReadPolicySummary":
        for field in (
            "audience_ids",
            "tenant_ids",
            "visible_fields",
            "masked_fields",
            "decision_ids",
        ):
            values = getattr(self, field)
            if tuple(sorted(set(values))) != values:
                raise ValueError(f"{field} must be sorted and unique")
            for value in values:
                _public_identifier(value, field=field)
        if set(self.visible_fields) & set(self.masked_fields):
            raise ValueError("visible and masked fields must be disjoint")
        return self


class ReadExtensionReference(ReadProjectionModel):
    extension_ref: str
    kind: Literal["tool", "resolver"]
    capabilities: tuple[str, ...]
    requirements_digest: str
    package_digest: str
    execution_boundary: Literal["brokered-out-of-process"] = "brokered-out-of-process"
    metadata_only: Literal[True] = True

    @field_validator("extension_ref")
    @classmethod
    def _extension_ref(cls, value: str) -> str:
        return _read_reference(value, field="extension_ref")

    @field_validator("capabilities")
    @classmethod
    def _capabilities(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if tuple(sorted(set(values))) != values:
            raise ValueError("extension capabilities must be sorted and unique")
        for value in values:
            _public_identifier(value, field="capability")
        return values

    @field_validator("requirements_digest", "package_digest")
    @classmethod
    def _digests(cls, value: str) -> str:
        if not _DIGEST_RE.fullmatch(value):
            raise ValueError("extension metadata identities must be sha256 digests")
        return value


class DeploymentReadStatusV1(ReadProjectionModel):
    """Immediately prior read-status schema retained for additive migration."""

    schema_version: Literal[READ_STATUS_SCHEMA_V1] = READ_STATUS_SCHEMA_V1
    contract_version: Literal[CONTRACT_VERSION] = CONTRACT_VERSION
    api_version: Literal[API_VERSION] = API_VERSION
    operation_id: str
    deployment_id: str
    plan_digest: str
    state: Literal["planned", "running", "blocked", "succeeded", "failed"]
    phase: str
    progress_percent: int = Field(ge=0, le=100)
    blockers: tuple[ReadBlocker, ...] = ()
    next_actions: tuple[AgentAction, ...] = ()
    mutation_classification: Literal["read_only"] = "read_only"
    safe_to_retry: bool

    @field_validator("operation_id", "deployment_id", "phase")
    @classmethod
    def _identifiers(cls, value: str, info) -> str:
        return _public_identifier(value, field=info.field_name)

    @field_validator("plan_digest")
    @classmethod
    def _plan_digest(cls, value: str) -> str:
        if not _DIGEST_RE.fullmatch(value):
            raise ValueError("read status requires the exact lifecycle plan digest")
        return value


class DeploymentReadStatus(DeploymentReadStatusV1):
    """Current shared human/agent readback of an existing lifecycle journal."""

    schema_version: Literal[READ_STATUS_SCHEMA] = READ_STATUS_SCHEMA
    evidence: tuple[ReadEvidenceReference, ...] = ()
    materializations: tuple[ReadMaterializationStatus, ...] = ()
    provenance: tuple[ReadProvenanceReference, ...] = ()
    policy: ReadPolicySummary = Field(default_factory=ReadPolicySummary)
    extensions: tuple[ReadExtensionReference, ...] = ()


class ReadbackProjectionAuthority(ReadProjectionModel):
    """Digest-bound plan/receipt inputs accepted by the readback projector."""

    operation_id: str
    deployment_id: str
    plan_digest: str
    evidence: tuple[ReadEvidenceReference, ...] = ()
    materializations: tuple[ReadMaterializationStatus, ...] = ()
    provenance: tuple[ReadProvenanceReference, ...] = ()
    policy: ReadPolicySummary = Field(default_factory=ReadPolicySummary)
    extensions: tuple[ReadExtensionReference, ...] = ()
    authority_digest: str

    @field_validator("operation_id", "deployment_id")
    @classmethod
    def _authority_identifiers(cls, value: str, info) -> str:
        return _public_identifier(value, field=info.field_name)

    @field_validator("plan_digest")
    @classmethod
    def _authority_plan_digest(cls, value: str) -> str:
        if not _DIGEST_RE.fullmatch(value):
            raise ValueError("readback authority requires the exact lifecycle plan digest")
        return value

    @model_validator(mode="after")
    def _digest_bound(self) -> "ReadbackProjectionAuthority":
        payload = self.model_dump(exclude={"authority_digest"}, mode="json")
        expected = sha256_digest(canonical_json_bytes(payload))
        if self.authority_digest != expected:
            raise ValueError("readback projection authority digest mismatch")
        return self


def build_readback_projection_authority(
    *,
    operation_id: str,
    deployment_id: str,
    plan_digest: str,
    evidence: Sequence[ReadEvidenceReference] = (),
    materializations: Sequence[ReadMaterializationStatus] = (),
    provenance: Sequence[ReadProvenanceReference] = (),
    policy: ReadPolicySummary | None = None,
    extensions: Sequence[ReadExtensionReference] = (),
) -> ReadbackProjectionAuthority:
    payload = {
        "operation_id": operation_id,
        "deployment_id": deployment_id,
        "plan_digest": plan_digest,
        "evidence": tuple(evidence),
        "materializations": tuple(materializations),
        "provenance": tuple(provenance),
        "policy": policy or ReadPolicySummary(),
        "extensions": tuple(extensions),
    }
    serializable = {
        "operation_id": operation_id,
        "deployment_id": deployment_id,
        "plan_digest": plan_digest,
        "evidence": [item.model_dump(mode="json") for item in evidence],
        "materializations": [item.model_dump(mode="json") for item in materializations],
        "provenance": [item.model_dump(mode="json") for item in provenance],
        "policy": (policy or ReadPolicySummary()).model_dump(mode="json"),
        "extensions": [item.model_dump(mode="json") for item in extensions],
    }
    return ReadbackProjectionAuthority(
        **payload,
        authority_digest=sha256_digest(canonical_json_bytes(serializable)),
    )


def load_readback_projection_authority(path: Path) -> ReadbackProjectionAuthority:
    """Load a digest-bound plan/materialization sidecar, never runtime state."""

    return ReadbackProjectionAuthority.model_validate_json(
        path.read_text(encoding="utf-8")
    )


def upgrade_deployment_read_status(
    prior: DeploymentReadStatusV1,
) -> DeploymentReadStatus:
    """Perform the sole supported v1 -> v2 additive compatibility migration."""

    return DeploymentReadStatus(
        **prior.model_dump(exclude={"schema_version"}),
        evidence=(),
        materializations=(),
        provenance=(),
        policy=ReadPolicySummary(),
        extensions=(),
    )


def public_extension_references(plan: Any) -> tuple[ReadExtensionReference, ...]:
    """Project Wave 3 package metadata without exposing or importing targets."""

    from okg.substrate.extension_broker import (
        ExtensionPackagePlan,
        verify_extension_package_plan,
    )

    if not isinstance(plan, ExtensionPackagePlan):
        raise TypeError("extension readback requires a canonical ExtensionPackagePlan")
    plan = verify_extension_package_plan(plan)
    references = []
    for entry in plan.entries:
        if entry.kind not in {"tool", "resolver"}:
            continue
        references.append(
            ReadExtensionReference(
                extension_ref=(
                    f"{plan.package_name}@{plan.package_version}#{entry.extension_id}"
                ),
                kind=entry.kind,
                capabilities=tuple(sorted(entry.capabilities)),
                requirements_digest=entry.requirements_digest,
                package_digest=plan.package_digest,
            )
        )
    return tuple(sorted(references, key=lambda item: item.extension_ref))


def project_deployment_read_status(
    journal: Mapping[str, Any],
    *,
    authority: ReadbackProjectionAuthority | None = None,
) -> DeploymentReadStatus:
    """Read the existing lifecycle state; never create an operation identity."""

    operation_id = journal.get("operation_id")
    deployment_id = journal.get("deployment")
    admission = journal.get("lifecycle_plan_admission")
    plan_digest = admission.get("plan_digest") if isinstance(admission, Mapping) else None
    if not isinstance(operation_id, str):
        raise ValueError("lifecycle journal is missing its existing operation_id")
    if not isinstance(deployment_id, str):
        raise ValueError("lifecycle journal is missing deployment")
    if not isinstance(plan_digest, str) or not _DIGEST_RE.fullmatch(plan_digest):
        raise ValueError("lifecycle journal is missing its exact admitted plan digest")
    if authority is not None and (
        authority.operation_id != operation_id
        or authority.deployment_id != deployment_id
        or authority.plan_digest != plan_digest
    ):
        raise ValueError(
            "readback projection authority does not match the lifecycle operation, deployment, and plan digest"
        )

    raw_state = journal.get("status")
    failed_phase = journal.get("failed_phase")
    phase_attempts = journal.get("phase_attempts")
    uncertain_effect = False
    if isinstance(phase_attempts, Mapping):
        uncertain_effect = any(
            isinstance(attempt, Mapping)
            and attempt.get("effect_may_have_happened") is True
            and attempt.get("status") not in {"succeeded", "reconciled"}
            for attempt in phase_attempts.values()
        )
    if uncertain_effect or raw_state in {"blocked", "uncertain", "partial", "superseded"}:
        state = "blocked"
    elif raw_state == "failed" or failed_phase:
        state = "failed"
    elif journal.get("completed") is True:
        state = "succeeded"
    elif journal.get("active_phase"):
        state = "running"
    elif raw_state in {None, "planned", "requested"}:
        state = "planned"
    else:
        state = "blocked"
    phase = str(
        journal.get("active_phase")
        or journal.get("failed_phase")
        or journal.get("next_phase")
        or "status"
    )
    phase_statuses = journal.get("phase_statuses")
    if isinstance(phase_statuses, Mapping) and phase_statuses:
        finished = sum(
            value in {"completed", "succeeded", "skipped"}
            for value in phase_statuses.values()
        )
        progress = round(100 * finished / len(phase_statuses))
    else:
        progress = 100 if state == "succeeded" else 0
    blockers = (
        (ReadBlocker(
            code=(
                "operation-uncertain"
                if uncertain_effect or raw_state == "uncertain"
                else "operation-failed" if state == "failed" else "operation-blocked"
            ),
            phase=phase,
            retryable=False,
        ),)
        if state in {"blocked", "failed"}
        else ()
    )
    action_kind = (
        "poll-operation-status"
        if state == "running"
        else "inspect-operation-recovery"
        if state in {"blocked", "failed"}
        else "read-operation-status"
    )
    next_actions = (
        AgentAction(
            action_id=action_kind,
            kind=action_kind,
            arguments={
                "operation_id": operation_id,
                "deployment_id": deployment_id,
                "plan_digest": plan_digest,
            },
        ),
    )
    supplied_evidence = authority.evidence if authority is not None else ()
    evidence_by_kind = {item.evidence_kind: item for item in supplied_evidence}
    evidence_by_kind["runtime"] = ReadEvidenceReference(
        evidence_kind="runtime",
        reference=operation_id,
        status="current",
    )
    evidence_by_kind.setdefault(
        "graph",
        ReadEvidenceReference(
            evidence_kind="graph",
            reference="graph-authority",
            status="missing",
        ),
    )
    evidence_by_kind.setdefault(
        "materialization",
        ReadEvidenceReference(
            evidence_kind="materialization",
            reference="materialization-authority",
            status="missing",
        ),
    )
    return DeploymentReadStatus(
        operation_id=operation_id,
        deployment_id=deployment_id,
        plan_digest=plan_digest,
        state=state,
        phase=phase,
        progress_percent=progress,
        blockers=blockers,
        next_actions=next_actions,
        safe_to_retry=state in {"planned", "succeeded"},
        evidence=tuple(evidence_by_kind[key] for key in sorted(evidence_by_kind)),
        materializations=authority.materializations if authority else (),
        provenance=authority.provenance if authority else (),
        policy=authority.policy if authority else ReadPolicySummary(),
        extensions=authority.extensions if authority else (),
    )


def load_deployment_read_status(
    state_file: Path,
    *,
    authority: ReadbackProjectionAuthority | None = None,
) -> DeploymentReadStatus:
    """Read, but never write, the existing deployment lifecycle state file."""

    journal = json.loads(state_file.read_text(encoding="utf-8"))
    if not isinstance(journal, Mapping):
        raise ValueError("lifecycle journal must contain a JSON object")
    return project_deployment_read_status(journal, authority=authority)


def render_human_read_status(status: DeploymentReadStatus) -> str:
    """Render the exact machine model while retaining advanced identities."""

    lines = [
        f"Deployment {status.deployment_id}: {status.state}",
        f"Operation: {status.operation_id}",
        f"Plan digest: {status.plan_digest}",
        f"Phase: {status.phase} ({status.progress_percent}%)",
    ]
    lines.extend(
        f"- evidence {item.evidence_kind}: {item.reference} [{item.status}]"
        for item in status.evidence
    )
    lines.extend(
        f"- materialization {item.asset_id}: generation={item.generation_id} status={item.status} lineage={item.lineage_reference}"
        for item in status.materializations
    )
    lines.extend(
        f"- provenance {item.owner}: {item.asset_id} {item.asset_digest}"
        for item in status.provenance
    )
    lines.append(
        "Policy: audiences="
        + ",".join(status.policy.audience_ids)
        + "; tenants="
        + ",".join(status.policy.tenant_ids)
        + "; decisions="
        + ",".join(status.policy.decision_ids)
    )
    lines.append(
        "Fields: visible="
        + ",".join(status.policy.visible_fields)
        + "; masked="
        + ",".join(status.policy.masked_fields)
    )
    lines.extend(
        f"- blocker {item.code}: phase={item.phase} retryable={str(item.retryable).lower()}"
        for item in status.blockers
    )
    lines.extend(
        f"- next action {item.kind}: {item.action_id}"
        for item in status.next_actions
    )
    lines.extend(
        f"- extension {item.kind}: {item.extension_ref} capabilities={','.join(item.capabilities)} [{item.execution_boundary}]"
        for item in status.extensions
    )
    lines.append(f"Safe to retry: {str(status.safe_to_retry).lower()}")
    return "\n".join(lines)


PRODUCT_SCHEMA_MODELS: Mapping[str, type[ProductModel]] = {
    "applied_instance": AppliedInstance,
    "authored_deployment_definition": AuthoredDeploymentDefinition,
    "distribution_release": DistributionRelease,
    "product_bundle": ProductBundle,
    "resolved_lock": ResolvedLock,
    "sealed_release": SealedReleaseReference,
}

AGENT_SCHEMA_MODELS: Mapping[str, type[ProductModel]] = {
    "agent_diagnostic": AgentDiagnostic,
    "agent_operation": AgentOperationEnvelope,
    "agent_plan": AgentPlanEnvelope,
    "agent_questions": AgentQuestionEnvelope,
    "agent_status": AgentStatusEnvelope,
    "deployment_read_status": DeploymentReadStatus,
}

_QUESTION_METADATA: Mapping[str, Mapping[str, Any]] = {
    "definition_id": {
        "prompt": "What stable name should identify this deployment definition?",
        "why": "Plans, locks, and applied instances use this identity.",
        "storage_class": "definition_value",
        "safe_default_reason": "A deployment identity must be chosen explicitly.",
        "requires_human": True,
    },
    "distribution_release_ref": {
        "prompt": "Which immutable distribution release should this deployment use?",
        "why": "The release bounds the available product bundles and assets.",
        "storage_class": "product_reference",
        "safe_default_reason": "No release is inferred from checkout location or profile.",
        "requires_human": True,
    },
    "bundles": {
        "prompt": "Which compatible product bundles should be selected?",
        "why": "Bundles select tested sets; list order never grants precedence.",
        "storage_class": "product_reference",
        "safe_default_reason": "An empty selection adds no optional bundle.",
    },
    "instance_values": {
        "prompt": "Which non-secret instance values should be supplied?",
        "why": "Instance-owned values affect the authored definition and plan.",
        "storage_class": "definition_value",
        "safe_default_reason": "An empty map preserves distribution defaults.",
    },
    "overrides": {
        "prompt": "Which declared typed override slots should change?",
        "why": "Only declared slots may replace a distribution or bundle value.",
        "storage_class": "definition_value",
        "safe_default_reason": "No overrides preserves selected product ownership.",
    },
    "local_assets": {
        "prompt": "Which content-digested instance-local assets should be included?",
        "why": "Local assets are explicit and make their provenance inspectable.",
        "storage_class": "product_reference",
        "safe_default_reason": "No local assets keeps the product fully packaged.",
    },
    "secret_references": {
        "prompt": "Which secret references are required at apply time?",
        "why": "Only locators are authored; secret material and versions stay outside the product.",
        "storage_class": "secret_reference",
        "safe_default_reason": "No reference is added unless the deployment needs it.",
    },
    "connection_references": {
        "prompt": "Which provider connection references should be selected?",
        "why": "Connections are referenced independently from source bindings and secrets.",
        "storage_class": "product_reference",
        "safe_default_reason": "No connection is inferred from local host state.",
    },
    "provider_connections": {
        "prompt": "Which reusable provider connections should this deployment define?",
        "why": "A connection identifies provider access independently from each source or tool binding.",
        "storage_class": "definition_value",
        "safe_default_reason": "No provider connection is created unless it is explicitly authored.",
    },
    "source_bindings": {
        "prompt": "Which sources should bind to the authored provider connections?",
        "why": "Each source binding selects one reusable connection and its source-specific scope.",
        "storage_class": "definition_value",
        "safe_default_reason": "No source binding is inferred from connection metadata.",
    },
    "tool_bindings": {
        "prompt": "Which governed tools should bind to the authored provider connections?",
        "why": "Tool bindings declare provider effects and approval policy separately from source access.",
        "storage_class": "definition_value",
        "safe_default_reason": "No governed tool is enabled unless it is explicitly authored.",
    },
}


class ProjectionDriftError(ValueError):
    def __init__(self, expected: str, observed: str) -> None:
        super().__init__(
            "deployment-product projection fixture drifted; intentionally refresh "
            "the conformance fixture after compatibility review"
        )
        self.expected = expected
        self.observed = observed


def schema_catalog() -> dict[str, dict[str, Any]]:
    """Generate JSON Schemas directly from canonical product and envelope models."""

    models = {**PRODUCT_SCHEMA_MODELS, **AGENT_SCHEMA_MODELS}
    return {
        name: model.model_json_schema(by_alias=True, mode="validation")
        for name, model in sorted(models.items())
    }


def setup_questions() -> tuple[AgentQuestion, ...]:
    """Generate setup questions from authored-definition schema properties."""

    schema = AuthoredDeploymentDefinition.model_json_schema(
        by_alias=True,
        mode="validation",
    )
    properties = schema["properties"]
    projected_fields = tuple(
        name
        for name in properties
        if name not in {"contract_version", "api_version", "schema_version"}
    )
    if set(projected_fields) != set(_QUESTION_METADATA):
        missing = sorted(set(projected_fields) - set(_QUESTION_METADATA))
        stale = sorted(set(_QUESTION_METADATA) - set(projected_fields))
        raise ProjectionDriftError(
            expected=f"question metadata for {sorted(projected_fields)!r}",
            observed=f"missing={missing!r} stale={stale!r}",
        )
    questions: list[AgentQuestion] = []
    for field_name in projected_fields:
        field_schema = properties[field_name]
        metadata = _QUESTION_METADATA[field_name]
        choices = tuple(field_schema.get("enum", ()))
        questions.append(
            AgentQuestion(
                question_id=f"definition.{field_name}",
                prompt=metadata["prompt"],
                why=metadata["why"],
                answer_schema=field_schema,
                choices=choices,
                safe_default=field_schema.get("default"),
                safe_default_reason=metadata["safe_default_reason"],
                sensitive=False,
                storage_class=metadata["storage_class"],
                affected_assets=(field_name,),
                requires_human=metadata.get("requires_human", False),
            )
        )
    return tuple(questions)


def project_agent_questions(
    *,
    operation_id: str,
    deployment_id: str,
    diagnostics: Sequence[AgentDiagnostic] = (),
) -> AgentQuestionEnvelope:
    """Return machine questions without consulting stdin or a prompt callback."""

    return AgentQuestionEnvelope(
        operation_id=operation_id,
        deployment_id=deployment_id,
        questions=setup_questions(),
        diagnostics=tuple(diagnostics),
    )


def project_setup(
    *,
    operation_id: str,
    deployment_id: str,
    agent_mode: bool,
) -> AgentQuestionEnvelope | str:
    """Project setup without owning an interactive input loop.

    Agent mode always returns the typed envelope. Human mode returns rendered
    questions for a caller-owned UI. Neither path consults stdin.
    """

    questions = project_agent_questions(
        operation_id=operation_id,
        deployment_id=deployment_id,
    )
    if agent_mode:
        return questions
    return render_human_questions(questions.questions)


def project_agent_plan(
    plan: DeploymentProductPlan,
    *,
    operation_id: str,
    deployment_id: str,
) -> AgentPlanEnvelope:
    actions = tuple(
        AgentAction(
            action_id=action.action_id,
            kind=action.kind,
            arguments=dict(action.arguments),
            preconditions=("exact-plan-digest", "unexpired-plan", "verified-artifacts"),
        )
        for action in plan.actions
    )
    return AgentPlanEnvelope(
        operation_id=operation_id,
        deployment_id=deployment_id,
        plan_digest=plan.plan_digest,
        lock_digest=plan.lock_digest,
        composition_digest=plan.lock.composition_digest,
        expires_at=plan.expires_at,
        summary=f"{len(actions)} planned deployment action(s)",
        actions=actions,
    )


def project_agent_diagnostic(
    diagnostic: LockDiagnostic,
    *,
    message: str | None = None,
) -> AgentDiagnostic:
    return AgentDiagnostic(
        code=diagnostic.code,
        path=diagnostic.path,
        owner=diagnostic.owner,
        # ``message`` is retained as an API-compatible input but centralized
        # model redaction deliberately ignores all caller-supplied prose.
        message=message or diagnostic.code.replace("_", " "),
        fix_hint=diagnostic.fix_hint,
        expected=diagnostic.expected,
        observed=diagnostic.observed,
        phase=diagnostic.phase,
    )


def render_human_questions(questions: Sequence[AgentQuestion]) -> str:
    lines = ["Deployment setup questions"]
    for index, question in enumerate(questions, start=1):
        default = json.dumps(
            question.safe_default, sort_keys=True, separators=(",", ":")
        )
        lines.extend(
            (
                f"{index}. {question.prompt}",
                f"   Why: {question.why}",
                f"   Stores: {question.storage_class}; safe default: {default} ({question.safe_default_reason})",
                f"   Contract field: {question.question_id}",
            )
        )
    return "\n".join(lines)


def render_human_plan(plan: AgentPlanEnvelope) -> str:
    expiry = plan.expires_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    lines = [
        f"Deployment plan for {plan.deployment_id}",
        f"Summary: {plan.summary}",
        f"Expires: {expiry}",
        f"Approval: {'required' if plan.approval_required else 'not required'}",
        f"Plan digest: {plan.plan_digest}",
    ]
    lines.extend(f"- {action.kind}: {action.action_id}" for action in plan.actions)
    return "\n".join(lines)


def render_human_status(status: AgentStatusEnvelope) -> str:
    lines = [
        f"Deployment {status.deployment_id}: {status.state}",
        f"Phase: {status.phase} ({status.progress_percent}%)",
        f"Summary: {status.summary}",
    ]
    lines.extend(
        f"- {diagnostic.code}: {diagnostic.message}; next: {diagnostic.fix_hint}"
        for diagnostic in status.diagnostics
    )
    lines.extend(
        f"- next action {action.kind}: {action.action_id}"
        for action in status.next_actions
    )
    return "\n".join(lines)


def projection_registry() -> dict[str, Any]:
    """Registry metadata consumed by generated surfaces and Wave 2."""

    return {
        "schema_version": PROJECTION_REGISTRY_SCHEMA,
        "contract_version": CONTRACT_VERSION,
        "api_version": API_VERSION,
        "canonical_objects": [
            {
                "name": name,
                "python_type": f"{model.__module__}.{model.__name__}",
                "schema_version": model.model_fields["schema_version"].default,
            }
            for name, model in sorted(PRODUCT_SCHEMA_MODELS.items())
        ],
        "agent_envelopes": [
            {
                "name": name,
                "python_type": f"{model.__module__}.{model.__name__}",
                "schema_version": model.model_fields["schema_version"].default,
            }
            for name, model in sorted(AGENT_SCHEMA_MODELS.items())
        ],
        "renderers": [
            "render_human_plan",
            "render_human_questions",
            "render_human_read_status",
            "render_human_status",
        ],
        "read_status_compatibility": {
            "current": READ_STATUS_SCHEMA,
            "immediately_prior": READ_STATUS_SCHEMA_V1,
            "rule": "v1 upgrades add empty evidence, materialization, provenance, policy, and extension fields",
        },
        "wave2_composition": {
            "asset_identity_fields": ["owner", "id", "kind", "version", "digest"],
            "bundle_order_is_precedence": False,
            "collision_policy": "same identity and digest deduplicates; different digest refuses",
            "override_policy": "declared typed slots only",
            "registered_asset_kinds": [],
            "resolver_extension_point": "registered_asset_kinds",
        },
    }


def _reference_plan() -> AgentPlanEnvelope:
    digest = "sha256:" + "1" * 64
    return AgentPlanEnvelope(
        operation_id="operation-reference",
        deployment_id="deployment-reference",
        plan_digest=digest,
        lock_digest="sha256:" + "2" * 64,
        composition_digest="sha256:" + "3" * 64,
        expires_at=datetime(2030, 1, 1, tzinfo=timezone.utc),
        summary="1 planned deployment action(s)",
        actions=(
            AgentAction(
                action_id="apply-reference",
                kind="apply-sealed-release",
                arguments={"artifact": "sealed/reference"},
                preconditions=("exact-plan-digest",),
            ),
        ),
    )


def _reference_status() -> AgentStatusEnvelope:
    return AgentStatusEnvelope(
        operation_id="operation-reference",
        deployment_id="deployment-reference",
        state="blocked",
        phase="pre_apply",
        progress_percent=0,
        summary="Artifact verification refused before mutation.",
        diagnostics=(
            AgentDiagnostic(
                code="artifact_digest_mismatch",
                path="cache/reference",
                owner="distribution",
                message="cached artifact digest changed",
                fix_hint="replace the artifact and plan again",
                expected="sha256:" + "4" * 64,
                observed="sha256:" + "5" * 64,
            ),
        ),
        next_actions=(
            AgentAction(
                action_id="plan-again",
                kind="replan",
                arguments={},
                preconditions=("artifact-restored",),
            ),
        ),
        mutation_classification="read_only",
        safe_to_retry=True,
    )


def _reference_read_status() -> DeploymentReadStatus:
    digest = "sha256:" + "6" * 64
    authority = build_readback_projection_authority(
        operation_id="operation-reference",
        deployment_id="deployment-reference",
        plan_digest=digest,
        evidence=(
            ReadEvidenceReference(
                evidence_kind="graph",
                reference="generation-reference",
                status="stale",
                generation_id="generation-reference",
            ),
        ),
        provenance=(
            ReadProvenanceReference(
                owner="distribution",
                asset_id="view-reference",
                asset_digest="sha256:" + "7" * 64,
                source_reference="bundle-reference",
            ),
        ),
        policy=ReadPolicySummary(
            audience_ids=("operator",),
            tenant_ids=("tenant-reference",),
            visible_fields=("display-name",),
            masked_fields=("contact-email",),
            decision_ids=("decision-reference",),
        ),
    )
    return project_deployment_read_status(
        {
            "operation_id": "operation-reference",
            "deployment": "deployment-reference",
            "lifecycle_plan_admission": {"plan_digest": digest},
            "active_phase": None,
            "next_phase": "readiness",
            "completed": False,
            "phase_statuses": {"preflight": "completed", "readiness": "not_started"},
        },
        authority=authority,
    )


def build_projection_conformance() -> dict[str, Any]:
    schemas = schema_catalog()
    questions = setup_questions()
    plan = _reference_plan()
    status = _reference_status()
    read_status = _reference_read_status()
    return {
        "schema_version": CONFORMANCE_SCHEMA,
        "contract_version": CONTRACT_VERSION,
        "api_version": API_VERSION,
        "schemas": {
            name: sha256_digest(canonical_json_bytes(schema))
            for name, schema in sorted(schemas.items())
        },
        "registry": sha256_digest(canonical_json_bytes(projection_registry())),
        "renderers": {
            "human_questions": sha256_digest(
                render_human_questions(questions).encode("utf-8")
            ),
            "human_plan": sha256_digest(render_human_plan(plan).encode("utf-8")),
            "human_status": sha256_digest(render_human_status(status).encode("utf-8")),
            "human_read_status": sha256_digest(
                render_human_read_status(read_status).encode("utf-8")
            ),
        },
        "agent_examples": {
            "questions": sha256_digest(
                canonical_json_bytes(
                    project_agent_questions(
                        operation_id="operation-reference",
                        deployment_id="deployment-reference",
                    ).model_dump(mode="json", by_alias=True)
                )
            ),
            "plan": sha256_digest(
                canonical_json_bytes(plan.model_dump(mode="json", by_alias=True))
            ),
            "status": sha256_digest(
                canonical_json_bytes(status.model_dump(mode="json", by_alias=True))
            ),
            "read_status_v1": sha256_digest(
                canonical_json_bytes(
                    DeploymentReadStatusV1(
                        **read_status.model_dump(
                            exclude={
                                "schema_version",
                                "evidence",
                                "materializations",
                                "provenance",
                                "policy",
                                "extensions",
                            }
                        )
                    ).model_dump(mode="json")
                )
            ),
            "read_status_v2": sha256_digest(
                canonical_json_bytes(read_status.model_dump(mode="json"))
            ),
        },
    }


def assert_projection_conformance(path: Path) -> None:
    expected = json.loads(path.read_text(encoding="utf-8"))
    observed = build_projection_conformance()
    expected_bytes = canonical_json_bytes(expected)
    observed_bytes = canonical_json_bytes(observed)
    if expected_bytes != observed_bytes:
        raise ProjectionDriftError(
            expected=sha256_digest(expected_bytes),
            observed=sha256_digest(observed_bytes),
        )


__all__ = [
    "AGENT_SCHEMA_MODELS",
    "CONFORMANCE_SCHEMA",
    "DIAGNOSTIC_SCHEMA",
    "OPERATION_SCHEMA",
    "PLAN_ENVELOPE_SCHEMA",
    "PRODUCT_SCHEMA_MODELS",
    "PROJECTION_REGISTRY_SCHEMA",
    "QUESTION_SCHEMA",
    "READ_STATUS_SCHEMA",
    "READ_STATUS_SCHEMA_V1",
    "STATUS_SCHEMA",
    "AgentAction",
    "AgentActionArguments",
    "AgentDiagnostic",
    "AgentOperationEnvelope",
    "AgentPlanEnvelope",
    "AgentQuestion",
    "AgentQuestionEnvelope",
    "AgentStatusEnvelope",
    "DeploymentReadStatus",
    "DeploymentReadStatusV1",
    "ReadBlocker",
    "ReadEvidenceReference",
    "ReadExtensionReference",
    "ReadMaterializationStatus",
    "ReadPolicySummary",
    "ReadProvenanceReference",
    "ReadbackProjectionAuthority",
    "ProjectionDriftError",
    "assert_projection_conformance",
    "build_projection_conformance",
    "build_readback_projection_authority",
    "project_agent_diagnostic",
    "project_agent_plan",
    "project_agent_questions",
    "project_deployment_read_status",
    "project_setup",
    "public_extension_references",
    "projection_registry",
    "render_human_plan",
    "render_human_questions",
    "render_human_read_status",
    "render_human_status",
    "schema_catalog",
    "setup_questions",
    "load_deployment_read_status",
    "load_readback_projection_authority",
    "upgrade_deployment_read_status",
]
