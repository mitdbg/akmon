"""The v5 manifest schema. Unknown keys are refused.

Eight top-level fields. ``requirements`` and ``tasks`` are reviewed intent and
sit inside the intent digest; ``evidence`` and ``decisions`` are records about
that intent and sit outside it, so attaching proof or an approval never
invalidates the approval it records.
"""

from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

PACT_VERSION = 5

EVIDENCE_KINDS = (
    "executed_test",
    "formal_check",
    "published_generation",
    "design_review",
    "manual",
)

_ID_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,127}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA_RE = re.compile(r"^[0-9a-f]{7,40}$")


class Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class Requirement(Strict):
    id: str
    statement: str = Field(min_length=1)

    @field_validator("id")
    @classmethod
    def _id(cls, value: str) -> str:
        if not _ID_RE.match(value):
            raise ValueError(f"requirement id {value!r} is not a valid id")
        return value


class Task(Strict):
    id: str
    requirement: str
    verification: str = Field(min_length=1)

    @field_validator("id")
    @classmethod
    def _id(cls, value: str) -> str:
        if not _ID_RE.match(value):
            raise ValueError(f"task id {value!r} is not a valid id")
        return value


class Evidence(Strict):
    """One receipt. ``artifact`` + ``sha256`` pin a file; ``head`` pins the
    commit the proof ran on. A row with no ``head`` is unbound: it is kept
    (migrated history) but never satisfies a requirement."""

    kind: Literal[EVIDENCE_KINDS]  # type: ignore[valid-type]
    requirement: str
    status: Literal["pass", "fail"] = "pass"
    command: str | None = None
    artifact: str | None = None
    sha256: str | None = None
    head: str | None = None
    at: str | None = None
    note: str | None = None

    @field_validator("sha256")
    @classmethod
    def _sha(cls, value: str | None) -> str | None:
        if value is not None and not _SHA256_RE.match(value):
            raise ValueError("sha256 must be 64 lowercase hex characters")
        return value

    @field_validator("head")
    @classmethod
    def _head(cls, value: str | None) -> str | None:
        if value is not None and not _GIT_SHA_RE.match(value):
            raise ValueError("head must be a git object id")
        return value


class Decision(Strict):
    """An approval bound to an intent digest. ``reviewer`` is the GitHub
    login (or principal) that wrote the review; ``url`` is where."""

    digest: str
    reviewer: str
    url: str | None = None
    at: str | None = None
    note: str | None = None

    @field_validator("digest")
    @classmethod
    def _digest(cls, value: str) -> str:
        if not value.startswith("sha256:") or not _SHA256_RE.match(value[7:]):
            raise ValueError("digest must be sha256:<64 hex>")
        return value


class Manifest(Strict):
    pact_version: Literal[5] = PACT_VERSION
    id: str
    title: str = Field(min_length=1)
    tier: int = Field(ge=0, le=3)
    why: str = Field(min_length=1)
    affected_code: list[str] = Field(default_factory=list)
    requirements: list[Requirement] = Field(min_length=1)
    tasks: list[Task] = Field(min_length=1)
    evidence: list[Evidence] = Field(default_factory=list)
    decisions: list[Decision] = Field(default_factory=list)

    @field_validator("id")
    @classmethod
    def _id(cls, value: str) -> str:
        if not _ID_RE.match(value):
            raise ValueError(f"change id {value!r} is not a valid id")
        return value

    def structural_errors(self) -> list[str]:
        """Cross-field rules pydantic cannot express."""
        errors: list[str] = []
        req_ids = [r.id for r in self.requirements]
        if len(set(req_ids)) != len(req_ids):
            errors.append("duplicate_requirement_id")
        task_ids = [t.id for t in self.tasks]
        if len(set(task_ids)) != len(task_ids):
            errors.append("duplicate_task_id")
        known = set(req_ids)
        for task in self.tasks:
            if task.requirement not in known:
                errors.append(f"task_requirement_unknown:{task.id}")
        for index, row in enumerate(self.evidence):
            if row.requirement not in known:
                errors.append(f"evidence_requirement_unknown:{index}")
            if (row.artifact is None) != (row.sha256 is None):
                errors.append(f"evidence_artifact_without_digest:{index}")
        covered = {t.requirement for t in self.tasks}
        for req in req_ids:
            if req not in covered:
                errors.append(f"requirement_without_task:{req}")
        return errors
