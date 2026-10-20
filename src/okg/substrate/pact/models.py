"""PACT v0 typed object model."""

from __future__ import annotations

import re
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictInt, field_validator

from okg.substrate.pact.home_repository import home_repository


ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")


class PactModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
        validate_assignment=True,
    )


def _validate_id(value: str) -> str:
    if not ID_RE.match(value):
        raise ValueError(
            "id must start with an alphanumeric character and contain only "
            "letters, numbers, '_', '-', '.', ':'"
        )
    return value


class EvidencePolicy(PactModel):
    required: list[str] = Field(default_factory=list)
    # `required_any` is satisfied when AT LEAST ONE of its kinds has evidence
    # (an OR, vs. `required`'s AND). Used by the UI-change workflow to require
    # ">=1 rendered-in-browser layer" (ui_interaction OR ui_visual) without
    # forcing a specific one. See `ui-change-on-pact`.
    required_any: list[str] = Field(default_factory=list)


class Change(PactModel):
    id: str
    title: str
    status: Literal["proposal", "investigation", "implementation", "verification"] = "proposal"
    rationale: str | None = None
    root_cause: str | None = None
    labels: list[str] = Field(default_factory=list)
    # PACT v4 lifecycle tier. Labels remain a compatibility projection for
    # older manifests, while new scaffolds persist the tier structurally.
    tier: StrictInt | None = None
    method_pack: str | None = None
    method_metadata: dict[str, Any] = Field(default_factory=dict)
    affected_areas: list[str] = Field(default_factory=list)
    affected_code: list[str] = Field(default_factory=list)
    migration_notes: str | None = None

    @field_validator("id")
    @classmethod
    def _id(cls, value: str) -> str:
        return _validate_id(value)

    @field_validator("labels")
    @classmethod
    def _labels(cls, value: list[str]) -> list[str]:
        return [_validate_id(label) for label in value]

    @field_validator("method_pack")
    @classmethod
    def _method_pack(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value:
            raise ValueError("method_pack must not be empty")
        return _validate_id(value)

    @field_validator("tier")
    @classmethod
    def _tier(cls, value: int | None) -> int | None:
        if value is not None and value not in {0, 1, 2, 3}:
            raise ValueError("tier must be one of 0, 1, 2, or 3")
        return value


class Scenario(PactModel):
    id: str
    title: str
    given: list[str] = Field(default_factory=list)
    when: list[str] = Field(default_factory=list)
    then: list[str] = Field(default_factory=list)
    and_: list[str] = Field(default_factory=list, alias="and")

    @field_validator("id")
    @classmethod
    def _id(cls, value: str) -> str:
        return _validate_id(value)


class AcceptanceArm(PactModel):
    """One named thing a criterion has to show.

    Per `pact-acceptance-prose-is-enforceable`
    req.pact-acceptance-prose-is-enforceable.arms-bind-structurally: a
    criterion's `description` is prose no gate can read, so a criterion that
    demands three things could be closed by one kind-matched row. An arm is
    the STRUCTURAL declaration of one of those things, carrying its own proof
    tokens. Validation refuses a multi-arm criterion whose arms are not
    separably bound — see `_acceptance_arm_collisions` in validation.py.

    Declaring `arms` is optional: when a criterion declares none, every token
    in its own `evidence` list is treated as an arm, which is the shape the
    corpus already uses (a distinct evidence kind, or an explicit `ev.*` id,
    per thing to show).
    """

    id: str
    description: str | None = None
    evidence: list[str] = Field(default_factory=list)

    @field_validator("id")
    @classmethod
    def _id(cls, value: str) -> str:
        return _validate_id(value)


class AcceptanceCriterion(PactModel):
    id: str
    description: str
    evidence: list[str] = Field(default_factory=list)
    # Explicit arm declarations. When non-empty they SUPERSEDE `evidence`
    # (validation refuses a criterion that declares both, rather than
    # silently picking one), and the criterion's proof token set is the
    # union of the arms' tokens.
    arms: list[AcceptanceArm] = Field(default_factory=list)

    @field_validator("id")
    @classmethod
    def _id(cls, value: str) -> str:
        return _validate_id(value)


class Requirement(PactModel):
    id: str
    title: str | None = None
    statement: str
    severity: Literal["must", "should", "may"] = "must"
    evidence_policy: EvidencePolicy = Field(default_factory=EvidencePolicy)
    scenarios: list[Scenario] = Field(default_factory=list)
    acceptance: list[AcceptanceCriterion] = Field(default_factory=list)

    @field_validator("id")
    @classmethod
    def _id(cls, value: str) -> str:
        return _validate_id(value)


class Task(PactModel):
    id: str
    title: str
    status: Literal["todo", "in_progress", "blocked", "done"] = "todo"
    owner: str | None = None
    labels: list[str] = Field(default_factory=list)
    method_pack: str | None = None
    method_metadata: dict[str, Any] = Field(default_factory=dict)
    verification: str
    requirements: list[str] = Field(default_factory=list)
    evidence: list[str] = Field(default_factory=list)
    environment: list["EnvironmentCheck"] = Field(default_factory=list)
    blocked_reason: str | None = None
    completion_satisfied: bool = False
    coordination_gates: list[str] = Field(default_factory=list)
    coordination_generation_id: int | None = None

    @field_validator("id")
    @classmethod
    def _id(cls, value: str) -> str:
        return _validate_id(value)

    @field_validator("labels")
    @classmethod
    def _labels(cls, value: list[str]) -> list[str]:
        return [_validate_id(label) for label in value]

    @field_validator("method_pack")
    @classmethod
    def _method_pack(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value:
            raise ValueError("method_pack must not be empty")
        return _validate_id(value)


class EnvironmentCheck(PactModel):
    id: str
    kind: Literal["env_var"] = "env_var"
    name: str | None = None
    env: str | None = None
    description: str | None = None
    required: bool = True

    @field_validator("id")
    @classmethod
    def _id(cls, value: str) -> str:
        return _validate_id(value)


class TaskDependency(PactModel):
    from_task: str = Field(alias="from")
    to_task: str = Field(alias="to")
    kind: Literal["blocks", "requires", "enables", "supersedes", "conflicts_with"] = "blocks"
    rationale: str | None = None
    waiver: str | None = None


class TestPlanEntry(PactModel):
    id: str
    requirement: str
    test_id: str
    command: str | None = None
    fixture: str | None = None
    evidence: list[str] = Field(default_factory=lambda: ["pytest"])

    @field_validator("id")
    @classmethod
    def _id(cls, value: str) -> str:
        return _validate_id(value)


class FormalBinding(PactModel):
    id: str
    requirement: str
    property_id: str
    module: str
    config: str | None = None
    checker_command: str | None = None
    refinement_tests: list[str] = Field(default_factory=list)

    @field_validator("id")
    @classmethod
    def _id(cls, value: str) -> str:
        return _validate_id(value)


class Waiver(PactModel):
    id: str
    scope: str
    reason: str
    requires_owner_approval: bool = True
    owner: str | None = None
    expires: str | None = None
    accepted_risk: str | None = None

    @field_validator("id")
    @classmethod
    def _id(cls, value: str) -> str:
        return _validate_id(value)


class PactHierarchy(PactModel):
    role: Literal["standalone", "program", "child"] = "standalone"
    parent: str | None = None
    children: list[str] = Field(default_factory=list)
    integration_acceptance: list[str] = Field(default_factory=list)


class Evidence(PactModel):
    id: str
    kind: str
    # `partial` is the honest value for a run that executed some of what the
    # task asked and could not execute the rest — the pytest arm ran, the
    # live arm did not. Per `pact-acceptance-prose-is-enforceable`
    # req.pact-acceptance-prose-is-enforceable.partial-is-recordable: before
    # it existed the only values were `pass` (a lie) or `fail`/`missing`
    # (which read as "nothing ran"), so fourteen rows in the corpus carried
    # `pass` with a summary that disclaimed itself. `partial` is deliberately
    # NOT in validation.PASSING_EVIDENCE: it proves nothing, while staying
    # visible in `okg pact evidence status` as work that ran.
    status: Literal[
        "planned",
        "pass",
        "partial",
        "fail",
        "missing",
        "waived",
    ] = "planned"
    refs: list[str] = Field(default_factory=list)
    artifact: str | None = None
    command: str | None = None
    summary: str | None = None
    generation_id: int | None = None
    data: dict[str, Any] = Field(default_factory=dict)

    @field_validator("id")
    @classmethod
    def _id(cls, value: str) -> str:
        return _validate_id(value)


class PactEdit(PactModel):
    id: str
    at: str
    actor: str
    command: str
    rationale: str | None = None
    affected_ids: list[str] = Field(default_factory=list)
    before_digest: str
    after_digest: str
    validation: Literal["accepted", "rejected"] = "accepted"

    @field_validator("id")
    @classmethod
    def _id(cls, value: str) -> str:
        return _validate_id(value)


class PactCodeTuple(PactModel):
    """One exact Git tree entry covered by a code-proof receipt."""

    path: str
    mode: str
    object_id: str


class PactCodeProofReceipt(PactModel):
    """Tracked receipt for a code-bearing head proved before PACT closure."""

    # Pydantic's BaseModel still exposes a deprecated ``schema`` method. Use
    # an internal name while preserving the contract's exact serialized key.
    schema_: Literal["okg.pact_code_proof_receipt/v1"] = Field(alias="schema")
    # The repository this package is built from, read from package metadata
    # (``[project.urls] repository``); see ``home_repository``.
    repository: str
    base_sha: str
    base_tree: str
    proven_code_sha: str
    proven_code_tree: str
    pact_intent_digest: str
    closure_allowlist: list[str] = Field(default_factory=list)
    code_tuples: list[PactCodeTuple] = Field(default_factory=list)
    code_digest: str
    receipt_digest: str

    @field_validator("repository")
    @classmethod
    def _repository(cls, value: str) -> str:
        expected = home_repository()
        if value != expected:
            raise ValueError(f"code proof repository must be {expected}")
        return value


class PactProofReceipts(PactModel):
    code: list[PactCodeProofReceipt] = Field(default_factory=list)


class PactManifest(PactModel):
    pact_version: int = 0
    target_pact_version: int | None = None
    change: Change
    hierarchy: PactHierarchy | None = None
    requirements: list[Requirement] = Field(default_factory=list)
    tasks: list[Task] = Field(default_factory=list)
    dependencies: list[TaskDependency] = Field(default_factory=list)
    test_plan: list[TestPlanEntry] = Field(default_factory=list)
    formal: list[FormalBinding] = Field(default_factory=list)
    waivers: list[Waiver] = Field(default_factory=list)
    evidence: list[Evidence] = Field(default_factory=list)
    proof_receipts: PactProofReceipts = Field(default_factory=PactProofReceipts)
    edits: list[PactEdit] = Field(default_factory=list)
    prose: dict[str, str] = Field(default_factory=dict)

    @field_validator("pact_version")
    @classmethod
    def _pact_version(cls, value: int) -> int:
        if value not in {0, 1, 2, 3, 4}:
            raise ValueError("unsupported PACT artifact format version")
        return value

    @field_validator("target_pact_version")
    @classmethod
    def _target_pact_version(cls, value: int | None) -> int | None:
        if value is not None and value < 0:
            raise ValueError("target_pact_version must be non-negative")
        return value


def dump_manifest(manifest: PactManifest) -> dict:
    data = manifest.model_dump(
        mode="json",
        by_alias=True,
        exclude_none=True,
    )
    change = data.get("change")
    if isinstance(change, dict):
        if not change.get("labels"):
            change.pop("labels", None)
        if not change.get("method_metadata"):
            change.pop("method_metadata", None)
    for task in data.get("tasks", []):
        if isinstance(task, dict):
            task.pop("completion_satisfied", None)
            if not task.get("labels"):
                task.pop("labels", None)
            if not task.get("method_metadata"):
                task.pop("method_metadata", None)
            if not task.get("coordination_gates"):
                task.pop("coordination_gates", None)
    # Per `simplify-pact-surface` task 4: suppress fields that carry only
    # their default value, so authoring a slim PACT does not require
    # carrying default-valued boilerplate. `severity: must` is the field
    # 95%+ of usage; agents can still write it explicitly when they want
    # `should` / `may` and it round-trips.
    for req in data.get("requirements", []):
        if isinstance(req, dict):
            if req.get("severity") == "must":
                req.pop("severity", None)
            # Suppress the empty `required_any` so non-UI requirements don't
            # carry the default-empty field.
            policy = req.get("evidence_policy")
            if isinstance(policy, dict) and not policy.get("required_any"):
                policy.pop("required_any", None)
            # Same for the empty `arms`: explicit arm declarations are the
            # exception (a criterion whose tokens already name its arms needs
            # none), so an untouched manifest must not grow `arms: []` on
            # every criterion the first time it round-trips.
            for acceptance in req.get("acceptance", []):
                if isinstance(acceptance, dict) and not acceptance.get("arms"):
                    acceptance.pop("arms", None)
    # Per `polish-pact-agent-ux` req.polish.doc-holes: v4 manifests carry no
    # edits ledger and no prose block, yet every corpus file lugged the empty
    # keys as dead weight. Suppress them when empty; nonempty values still
    # round-trip (and a nonempty edits/prose in a v4 file still fails
    # validation exactly as before).
    if not data.get("edits"):
        data.pop("edits", None)
    proof_receipts = data.get("proof_receipts")
    if isinstance(proof_receipts, dict) and not proof_receipts.get("code"):
        data.pop("proof_receipts", None)
    if not data.get("prose"):
        data.pop("prose", None)
    return data
