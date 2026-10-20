"""Agent-facing record payload templates and lowering.

The record operator accepts semantic agent payloads and lowers them into
the existing review-mode admission shape. It deliberately does not write
facts directly; callers still go through admission and publish.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

# The prefix registry (RecordPrefix model + builtin set + merge/resolve) lives
# in a shared module so the `agent_memory` source resolves prefixes through the
# same catalog-backed, deployment-extensible table (per substrate-operability).
# Aliased to the private names this module has always used internally.
from okg.substrate.library.agent_memory_prefixes import (  # noqa: F401
    RecordPrefix,
    normalize_prefixes as _normalize_prefixes,
    prefix_for_id as _prefix_for_id,
    subtype_for_id as _node_subtype_for_id,
)


__all__ = [
    "RecordPayloadError",
    "RecordPrefix",
    "RECORD_KINDS",
    "TARGET_AGNOSTIC_REFERENCE_FIELDS",
    "exempt_reference_fields",
    "record_route_evaluation_counts",
    "unroutable_record_reference_fields",
    "lower_record_payload",
    "record_compatibility_payload",
    "record_prefixes_payload",
    "record_templates_payload",
]


RECORD_KINDS = (
    "artifact",
    "decision",
    "evidence",
    "experiment_run",
    "finding",
    "hypothesis",
    "observation",
    "project",
    "study",
    "domain_entity",
    "verification",
)


# RecordPrefix + BUILTIN_RECORD_PREFIXES now live in
# okg.substrate.library.agent_memory_prefixes (imported above) so the
# agent_memory source shares the same catalog-backed, deployment-extensible
# registry. Per substrate-operability.


@dataclass(frozen=True)
class RecordTemplate:
    kind: str
    template_id: str
    target_subtype: str
    id_attr: str
    id_prefix: str | None
    text_attr: str
    required_fields: tuple[str, ...]
    optional_fields: tuple[str, ...]
    reference_fields: tuple[str, ...]
    default_attrs: Mapping[str, Any]
    description: str
    example_payload: Mapping[str, Any]


@dataclass(frozen=True)
class ReferenceFieldSpec:
    field: str
    edge_type: str
    role: str
    allowed_target_subtypes: tuple[str, ...] = ()
    denied_target_subtypes: tuple[str, ...] = ()
    allow_any_catalog_route: bool = False


class RecordPayloadError(ValueError):
    """Structured validation error for agent record payloads."""

    def __init__(
        self,
        *,
        code: str,
        message: str,
        field: str | None = None,
        template: Mapping[str, Any] | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.field = field
        self.template = dict(template or {})
        self.details = dict(details or {})

    def to_error(self) -> dict[str, Any]:
        error = {
            "code": self.code,
            "message": self.message,
        }
        if self.field is not None:
            error["field"] = self.field
        if self.template:
            error["template"] = self.template
        if self.details:
            error.update(self.details)
        return error


_COMMON_OPTIONAL_FIELDS = (
    "id",
    "node_id",
    "status",
    "confidence_kind",
    "scope",
    "source_artifact",
    "source_hash",
    "occurred_at",
    "observed_at",
    "actor_ref",
    "session_ref",
    "study_ref",
    "project_ref",
    "artifact_refs",
    "code_refs",
    "domain_refs",
    "reference_refs",
)


# Actor subtypes `actor_ref` may target. `operator` was listed here and is
# declared by NO library module, so it could only ever resolve to
# `target_subtype_missing` for a caller who followed the advertised target list.
# Dropped rather than invented; test_actor_target_subtypes_are_declared keeps
# every member of this tuple backed by a real subtype.
_ACTOR_TARGET_SUBTYPES = (
    "agent",
    "git_author",
    "person",
)
_SESSION_TARGET_SUBTYPES = (
    "agent_session",
    "sub_agent_session",
)
_CODE_TARGET_SUBTYPES = (
    "code_import",
    "code_reference",
    "code_symbol",
    "config_key",
    "dependency_package",
    "git_commit",
    "interface_endpoint",
    "package_manifest",
    "source_file",
)
_ARTIFACT_TARGET_SUBTYPES = (
    "artifact",
    "benchmark_measurement",
    "coverage_report",
    "data_asset",
    "document",
    "document_chunk",
    "profile_hotspot",
    "profile_query",
    "profile_run",
    "profile_stage",
    "sbom_artifact",
    "test_report",
)


_COMMON_REFERENCE_FIELD_SPECS: tuple[ReferenceFieldSpec, ...] = (
    ReferenceFieldSpec(
        "actor_ref", "references", "actor",
        allowed_target_subtypes=_ACTOR_TARGET_SUBTYPES,
    ),
    ReferenceFieldSpec(
        "session_ref", "references", "session",
        allowed_target_subtypes=_SESSION_TARGET_SUBTYPES,
    ),
    ReferenceFieldSpec(
        "study_ref", "references", "study",
        allowed_target_subtypes=("study",),
    ),
    ReferenceFieldSpec(
        "project_ref", "references", "project",
        allowed_target_subtypes=("project",),
    ),
    ReferenceFieldSpec(
        "artifact_refs", "references", "artifact",
        allowed_target_subtypes=_ARTIFACT_TARGET_SUBTYPES,
    ),
    ReferenceFieldSpec(
        "code_refs", "references", "code",
        allowed_target_subtypes=_CODE_TARGET_SUBTYPES,
    ),
    ReferenceFieldSpec(
        "domain_refs", "references", "domain",
        allowed_target_subtypes=("domain_entity",),
        denied_target_subtypes=(
            *_ACTOR_TARGET_SUBTYPES,
            # `operator` is not an actor_ref target because no library module
            # declares it, but deployment-owned prefixes must not make it a
            # generic domain target as a side effect of that removal.
            "operator",
            *_SESSION_TARGET_SUBTYPES,
            *_CODE_TARGET_SUBTYPES,
            *_ARTIFACT_TARGET_SUBTYPES,
            "project",
            "study",
        ),
        allow_any_catalog_route=True,
    ),
    ReferenceFieldSpec(
        "reference_refs", "references", "reference",
        allow_any_catalog_route=True,
    ),
)


# Reference fields that name no target subtypes of their own. Their reach IS
# whatever `references` routes the active catalog happens to declare from the
# record's source subtype, so they promise nothing specific and a deployment
# extends them by declaring its own narrowings.
#
# This distinction is what makes "does every advertised field have a route?"
# answerable. Requiring >=1 target from a target-agnostic field yields a pass
# that carries no information: after #947 declared `study references project`,
# `study.reference_refs` read as "closed" while still reaching only what
# `project_ref` and `domain_refs` already reached.
TARGET_AGNOSTIC_REFERENCE_FIELDS = frozenset({"reference_refs"})


def exempt_reference_fields(advertised_fields: Iterable[str]) -> frozenset[str]:
    """Target-agnostic fields to skip when checking one template for routes.

    The exemption is CONDITIONAL. It applies only while the template also
    advertises at least one target-naming field. A template whose ONLY reference
    field is target-agnostic cannot link to anything at all when that field
    reaches zero targets, and zero is not an uninformative number — it is
    exactly #947's defect. `domain_entity` is that template today.

    Shared deliberately: the CI parity check and the per-deployment lint must
    agree on which fields are promises, or an operator-facing finding will
    disagree with the build gate about whether a template is broken.
    """
    advertised = set(advertised_fields)
    if advertised - TARGET_AGNOSTIC_REFERENCE_FIELDS:
        return TARGET_AGNOSTIC_REFERENCE_FIELDS
    return frozenset()


_REFERENCE_FIELD_NAMES = frozenset({
    spec.field for spec in _COMMON_REFERENCE_FIELD_SPECS
}) | {
    "subject_ref",
    "subject_id",
    "evidence_refs",
    "supports",
    "contradicting_refs",
    "contradicts",
    "supersedes_refs",
    "target_refs",
    "verifies",
    "produced_refs",
    "produces",
}

_TEMPLATES: dict[str, RecordTemplate] = {
    "observation": RecordTemplate(
        kind="observation",
        template_id="builtin.agent_memory.observation.v1",
        target_subtype="agent_observation",
        id_attr="observation_id",
        id_prefix="observation:",
        text_attr="summary",
        required_fields=("summary",),
        optional_fields=(
            *_COMMON_OPTIONAL_FIELDS,
            "observation_kind",
        ),
        reference_fields=(
            "actor_ref",
            "session_ref",
            "study_ref",
            "project_ref",
            "artifact_refs",
            "code_refs",
            "domain_refs",
            "reference_refs",
        ),
        default_attrs={
            "status": "candidate",
            "observation_kind": "study_observation",
        },
        description="A structured observation from an agent or study run.",
        example_payload={
            "summary": "Closure agrees within 8 percent after correction v3.",
            "confidence_kind": "deterministic",
            "study_ref": "study:suep-background-closure",
            "artifact_refs": ["artifact:closure-v3/report.json"],
        },
    ),
    "project": RecordTemplate(
        kind="project",
        template_id="builtin.bookkeeping.project.v1",
        target_subtype="project",
        id_attr="project_id",
        id_prefix="proj:",
        text_attr="title",
        required_fields=("project_id", "title"),
        optional_fields=(
            "id",
            "node_id",
            "slug",
            "purpose",
            "status",
            "source_artifact",
            "source_hash",
            "occurred_at",
            "observed_at",
        ),
        reference_fields=(),
        default_attrs={
            "status": "active",
        },
        description="A bookkeeping Project context node explicitly created through a record template.",
        example_payload={
            "project_id": "cooper",
            "title": "Cooper analysis workspace",
            "purpose": "Coordinate SUEP analysis memory and decisions.",
        },
    ),
    "study": RecordTemplate(
        kind="study",
        template_id="builtin.agent_memory.study.v1",
        target_subtype="study",
        id_attr="study_id",
        id_prefix="study:",
        text_attr="title",
        required_fields=("study_id", "title"),
        optional_fields=(
            "id",
            "node_id",
            "description",
            "status",
            "scope",
            "source_artifact",
            "source_hash",
            "occurred_at",
            "observed_at",
            "project_ref",
            "domain_refs",
            "reference_refs",
        ),
        reference_fields=(
            "project_ref",
            "domain_refs",
            "reference_refs",
        ),
        default_attrs={
            "status": "active",
        },
        description="A study context node explicitly created through a record template.",
        example_payload={
            "study_id": "suep-background-closure",
            "title": "SUEP background closure study",
        },
    ),
    "domain_entity": RecordTemplate(
        kind="domain_entity",
        template_id="builtin.agent_memory.domain_entity.v1",
        target_subtype="domain_entity",
        id_attr="domain_entity_id",
        id_prefix=None,
        text_attr="label",
        required_fields=("domain_entity_id", "domain_kind", "label"),
        optional_fields=(
            "id",
            "node_id",
            "title",
            "status",
            "scope",
            "source_artifact",
            "source_hash",
            "occurred_at",
            "observed_at",
            "reference_refs",
        ),
        reference_fields=("reference_refs",),
        default_attrs={
            "status": "active",
        },
        description="A generic domain context node explicitly created through a record template.",
        example_payload={
            "domain_entity_id": "SR",
            "domain_kind": "region",
            "label": "Signal region",
        },
    ),
    "evidence": RecordTemplate(
        kind="evidence",
        template_id="builtin.agent_memory.evidence.v1",
        target_subtype="agent_observation",
        id_attr="observation_id",
        id_prefix="observation:",
        text_attr="summary",
        required_fields=("summary",),
        optional_fields=(
            *_COMMON_OPTIONAL_FIELDS,
            "observation_kind",
            "target_refs",
        ),
        reference_fields=(
            "actor_ref",
            "session_ref",
            "study_ref",
            "project_ref",
            "artifact_refs",
            "code_refs",
            "domain_refs",
            "reference_refs",
            "target_refs",
        ),
        default_attrs={
            "status": "supported",
            "observation_kind": "evidence",
        },
        description="Evidence that can support or contradict claims.",
        example_payload={
            "summary": "The post-correction table was generated from run 42.",
            "confidence_kind": "deterministic",
            "artifact_refs": ["artifact:closure-v3/table.json"],
        },
    ),
    "hypothesis": RecordTemplate(
        kind="hypothesis",
        template_id="builtin.agent_memory.hypothesis.v1",
        target_subtype="agent_claim",
        id_attr="claim_id",
        id_prefix="claim:",
        text_attr="statement",
        required_fields=("statement",),
        optional_fields=(
            *_COMMON_OPTIONAL_FIELDS,
            "claim_kind",
            "subject_id",
            "subject_ref",
            "evidence_refs",
            "supports",
            "contradicting_refs",
            "contradicts",
            "supersedes_refs",
        ),
        reference_fields=(
            "actor_ref",
            "session_ref",
            "study_ref",
            "project_ref",
            "artifact_refs",
            "code_refs",
            "domain_refs",
            "reference_refs",
            "subject_ref",
            "subject_id",
            "evidence_refs",
            "supports",
            "contradicting_refs",
            "contradicts",
            "supersedes_refs",
        ),
        default_attrs={
            "status": "candidate",
            "claim_kind": "hypothesis",
        },
        description="A candidate claim that must not become supported without evidence.",
        example_payload={
            "statement": "CRWJ closure is stable after correction v3.",
            "status": "candidate",
            "study_ref": "study:suep-background-closure",
            "evidence_refs": ["observation:closure-v3:summary"],
        },
    ),
    "finding": RecordTemplate(
        kind="finding",
        template_id="builtin.agent_memory.finding.v1",
        target_subtype="agent_claim",
        id_attr="claim_id",
        id_prefix="claim:",
        text_attr="statement",
        required_fields=("statement",),
        optional_fields=(
            *_COMMON_OPTIONAL_FIELDS,
            "claim_kind",
            "subject_id",
            "subject_ref",
            "evidence_refs",
            "supports",
            "contradicting_refs",
            "contradicts",
            "supersedes_refs",
        ),
        reference_fields=(
            "actor_ref",
            "session_ref",
            "study_ref",
            "project_ref",
            "artifact_refs",
            "code_refs",
            "domain_refs",
            "reference_refs",
            "subject_ref",
            "subject_id",
            "evidence_refs",
            "supports",
            "contradicting_refs",
            "contradicts",
            "supersedes_refs",
        ),
        default_attrs={
            "status": "candidate",
            "claim_kind": "finding",
        },
        description="A candidate finding; deployments may override with a domain subtype.",
        example_payload={
            "statement": "The correction improves closure in CRWJ.",
            "confidence_kind": "model_inferred",
            "study_ref": "study:suep-background-closure",
        },
    ),
    "verification": RecordTemplate(
        kind="verification",
        template_id="builtin.agent_memory.verification.v1",
        target_subtype="verification_result",
        id_attr="verification_id",
        id_prefix="verification:",
        text_attr="summary",
        required_fields=("summary", "verdict"),
        optional_fields=(
            *_COMMON_OPTIONAL_FIELDS,
            "target_refs",
            "verifies",
        ),
        reference_fields=(
            "actor_ref",
            "session_ref",
            "study_ref",
            "project_ref",
            "artifact_refs",
            "code_refs",
            "domain_refs",
            "reference_refs",
            "target_refs",
            "verifies",
        ),
        default_attrs={
            "status": "candidate",
        },
        description="A verification result tied to one or more target records.",
        example_payload={
            "summary": "The deterministic rerun matched the stored table.",
            "verdict": "verified",
            "target_refs": ["claim:closure-v3:stable"],
        },
    ),
    "decision": RecordTemplate(
        kind="decision",
        template_id="builtin.agent_memory.decision.v1",
        target_subtype="agent_decision",
        id_attr="decision_id",
        id_prefix="decision:",
        text_attr="summary",
        required_fields=("summary",),
        optional_fields=(
            *_COMMON_OPTIONAL_FIELDS,
            "decision_kind",
            "rationale",
        ),
        reference_fields=(
            "actor_ref",
            "session_ref",
            "study_ref",
            "project_ref",
            "artifact_refs",
            "code_refs",
            "domain_refs",
            "reference_refs",
        ),
        default_attrs={
            "status": "accepted",
            "decision_kind": "operator_decision",
        },
        description="A decision made by an agent, reviewer, or workflow.",
        example_payload={
            "summary": "Use correction v3 for the next closure table.",
            "rationale": "It is the first deterministic run with stable inputs.",
        },
    ),
    "experiment_run": RecordTemplate(
        kind="experiment_run",
        template_id="builtin.agent_memory.experiment_run.v1",
        target_subtype="agent_experiment",
        id_attr="experiment_id",
        id_prefix="experiment:",
        text_attr="summary",
        required_fields=("summary",),
        optional_fields=(
            *_COMMON_OPTIONAL_FIELDS,
            "experiment_kind",
            "produced_refs",
            "produces",
        ),
        reference_fields=(
            "actor_ref",
            "session_ref",
            "study_ref",
            "project_ref",
            "artifact_refs",
            "code_refs",
            "domain_refs",
            "reference_refs",
            "produced_refs",
            "produces",
        ),
        default_attrs={
            "status": "candidate",
            "experiment_kind": "study_run",
        },
        description="An experiment or analysis run that produced outputs.",
        example_payload={
            "summary": "Ran closure workflow v3 on the CRWJ inputs.",
            "produced_refs": ["artifact:closure-v3/report.json"],
        },
    ),
    "artifact": RecordTemplate(
        kind="artifact",
        template_id="builtin.agent_memory.artifact.v1",
        target_subtype="agent_observation",
        id_attr="observation_id",
        id_prefix="observation:",
        text_attr="summary",
        required_fields=("summary",),
        optional_fields=(
            *_COMMON_OPTIONAL_FIELDS,
            "observation_kind",
        ),
        reference_fields=(
            "actor_ref",
            "session_ref",
            "study_ref",
            "project_ref",
            "artifact_refs",
            "code_refs",
            "domain_refs",
            "reference_refs",
        ),
        default_attrs={
            "status": "candidate",
            "observation_kind": "artifact_record",
        },
        description="A record that describes an artifact without fabricating an artifact node.",
        example_payload={
            "summary": "Closure plot was produced for correction v3.",
            "artifact_refs": ["plot:closure-v3/crwj.png"],
        },
    ),
}


def evaluate_record_reference_routes(
    *,
    prefixes: Iterable[RecordPrefix | Mapping[str, Any]] | None = None,
    active_subtypes: Iterable[str] | None = None,
    narrowing_triples: Iterable[tuple[str, str, str]] | None = None,
) -> tuple[dict[tuple[str, str], dict[str, Any]], dict[str, int]]:
    """Return unroutable fields and coverage counts from one compatibility pass.

    Deployment lint needs both outputs to distinguish a clean evaluation from
    a catalog where no record template was available. Computing them through
    separate public helpers used to run ``_record_compatibility`` twice for
    every lint invocation. Keep the evaluation atomic so both outputs describe
    the exact same normalized prefixes and catalog snapshot.
    """
    compatibility = _record_compatibility(
        _normalize_prefixes(prefixes),
        active_subtypes=active_subtypes,
        narrowing_triples=narrowing_triples,
    )
    templates = compatibility["templates"]
    out: dict[tuple[str, str], dict[str, Any]] = {}
    available_templates = 0
    checked_fields = 0
    for kind, info in templates.items():
        if not info.get("available", True):
            continue
        available_templates += 1
        field_rows = info.get("fields") or []
        exempt = exempt_reference_fields(row["field"] for row in field_rows)
        checked_fields += sum(
            1 for row in field_rows if row["field"] not in exempt
        )
        for row in field_rows:
            if row["field"] in exempt:
                continue
            if row.get("allowed_target_subtypes"):
                continue
            out[(kind, row["field"])] = {
                "edge_type": row.get("edge_type"),
                "role": row.get("role"),
                "reasons": sorted({
                    str(entry.get("reason"))
                    for entry in row.get("inactive_targets") or []
                    if entry.get("reason")
                }),
            }
    return out, {
        "total_templates": len(templates),
        "available_templates": available_templates,
        "checked_fields": checked_fields,
    }


def unroutable_record_reference_fields(
    *,
    prefixes: Iterable[RecordPrefix | Mapping[str, Any]] | None = None,
    active_subtypes: Iterable[str] | None = None,
    narrowing_triples: Iterable[tuple[str, str, str]] | None = None,
) -> dict[tuple[str, str], dict[str, Any]]:
    """Advertised reference fields that CANNOT emit an edge under this catalog.

    Keyed `(record_kind, field)`, valued with the field's edge type, role, and
    the reasons its candidate targets were rejected. A non-empty result means a
    record template promises a link the catalog cannot deliver: `record()` will
    create the primary node, drop the edge, and return a warning — the #947
    defect shape.

    Callers supply the catalog to evaluate. Pass a deployment's composed
    `subtypes` / `narrowing_triples()` for the per-deployment answer, or the
    all-modules composition for the build-time guarantee. Target-agnostic fields
    are skipped per `exempt_reference_fields`, whose exemption is conditional.

    The CI parity check reads this projection while deployment lint reads the
    combined ``evaluate_record_reference_routes`` result, so both surfaces share
    the same evaluator and cannot drift on what counts as unroutable.
    """
    unroutable, _counts = evaluate_record_reference_routes(
        prefixes=prefixes,
        active_subtypes=active_subtypes,
        narrowing_triples=narrowing_triples,
    )
    return unroutable


def record_route_evaluation_counts(
    *,
    prefixes: Iterable[RecordPrefix | Mapping[str, Any]] | None = None,
    active_subtypes: Iterable[str] | None = None,
    narrowing_triples: Iterable[tuple[str, str, str]] | None = None,
) -> dict[str, int]:
    """How much surface `unroutable_record_reference_fields` actually examined.

    A caller cannot tell "no unroutable fields" from "nothing was evaluated" from
    the result alone: a catalog with none of the record subtypes makes every
    template unavailable, so the evaluator skips all of them and returns an empty
    dict — indistinguishable from a healthy deployment. Any consumer that reports
    a verdict MUST check these counts, or it reads absence as agreement.
    """
    _unroutable, counts = evaluate_record_reference_routes(
        prefixes=prefixes,
        active_subtypes=active_subtypes,
        narrowing_triples=narrowing_triples,
    )
    return counts


def record_templates_payload(
    kind: str | None = None,
    prefixes: Iterable[RecordPrefix | Mapping[str, Any]] | None = None,
    active_subtypes: Iterable[str] | None = None,
    narrowing_triples: Iterable[tuple[str, str, str]] | None = None,
) -> dict[str, Any]:
    """Return agent-readable record templates."""
    active_prefixes = _normalize_prefixes(prefixes)
    compatibility = _record_compatibility(
        active_prefixes,
        active_subtypes=active_subtypes,
        narrowing_triples=narrowing_triples,
    )
    if kind:
        normalized = _normalize_kind(kind)
        templates = [
            _template_payload(
                _TEMPLATES[normalized],
                compatibility=compatibility,
            )
        ]
    else:
        templates = [
            _template_payload(
                _TEMPLATES[name],
                compatibility=compatibility,
            )
            for name in RECORD_KINDS
        ]
    return {
        "kind": "record_templates",
        "templates": templates,
        "count": len(templates),
        "record_kinds": list(RECORD_KINDS),
        "reference_prefixes": record_prefixes_payload(
            active_prefixes,
            active_subtypes=active_subtypes,
            narrowing_triples=narrowing_triples,
        )["prefixes"],
    }


def record_prefixes_payload(
    prefixes: Iterable[RecordPrefix | Mapping[str, Any]] | None = None,
    active_subtypes: Iterable[str] | None = None,
    narrowing_triples: Iterable[tuple[str, str, str]] | None = None,
) -> dict[str, Any]:
    """Return the active record reference prefix metadata.

    This is the built-in registry surface. Deployment-owned extensions can
    be layered in by passing additional entries to the lowerer/server path.
    """
    active = _normalize_prefixes(prefixes)
    compatibility = _record_compatibility(
        active,
        active_subtypes=active_subtypes,
        narrowing_triples=narrowing_triples,
    )
    return {
        "kind": "record_prefixes",
        "prefixes": [
            _prefix_payload(entry, compatibility=compatibility)
            for entry in active
        ],
        "count": len(active),
    }


def record_compatibility_payload(
    prefixes: Iterable[RecordPrefix | Mapping[str, Any]] | None = None,
    active_subtypes: Iterable[str] | None = None,
    narrowing_triples: Iterable[tuple[str, str, str]] | None = None,
) -> dict[str, Any]:
    """Return compact record template/prefix compatibility metadata."""
    active = _normalize_prefixes(prefixes)
    compatibility = _record_compatibility(
        active,
        active_subtypes=active_subtypes,
        narrowing_triples=narrowing_triples,
    )
    return {
        "kind": "record_compatibility",
        "catalog_constrained": compatibility["catalog_constrained"],
        "prefixes": [
            _prefix_payload(entry, compatibility=compatibility)
            for entry in active
        ],
        "templates": [
            _template_payload(_TEMPLATES[name], compatibility=compatibility)
            for name in RECORD_KINDS
        ],
    }


def lower_record_payload(
    *,
    kind: str,
    payload: Mapping[str, Any],
    authority: Mapping[str, Any] | None = None,
    idempotency_key: str | None = None,
    prefixes: Iterable[RecordPrefix | Mapping[str, Any]] | None = None,
    active_subtypes: Iterable[str] | None = None,
    narrowing_triples: Iterable[tuple[str, str, str]] | None = None,
) -> dict[str, Any]:
    """Lower a semantic record payload to review admission nodes/edges."""
    normalized_kind = _normalize_kind(kind)
    if not isinstance(payload, Mapping):
        raise RecordPayloadError(
            code="invalid_record_payload",
            message="record payload must be an object",
            template=_template_payload(_TEMPLATES[normalized_kind]),
        )
    item = dict(payload)
    template = _TEMPLATES[normalized_kind]
    active_prefixes = _normalize_prefixes(prefixes)
    compatibility = _record_compatibility(
        active_prefixes,
        active_subtypes=active_subtypes,
        narrowing_triples=narrowing_triples,
    )
    _validate_required_fields(item, template)

    payload_sha = _json_sha256({
        "kind": normalized_kind,
        "payload": item,
    })
    record_id = _record_id(
        normalized_kind, item, payload_sha, template, active_prefixes,
    )
    observed_at = str(
        item.get("observed_at")
        or item.get("occurred_at")
        or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    )
    warnings = _validation_warnings(normalized_kind, item, active_prefixes)
    edges, edge_warnings, reference_diagnostics = _record_edges(
        record_id,
        template.target_subtype,
        normalized_kind,
        item,
        payload_sha=payload_sha,
        prefixes=active_prefixes,
        compatibility=compatibility,
    )
    if edge_warnings:
        warnings = sorted(set(warnings).union(edge_warnings))
    if (
        normalized_kind in {"hypothesis", "finding"}
        and str(item.get("status") or "").lower() == "supported"
        and not any(edge.get("edge_type") == "supported_by" for edge in edges)
    ):
        warnings = sorted(
            set(warnings).union({"supported_claim_without_known_evidence"})
        )
    attrs = _record_attrs(
        item,
        template=template,
        record_id=record_id,
        kind=normalized_kind,
        payload_sha=payload_sha,
        observed_at=observed_at,
        authority=authority or {},
        warnings=warnings,
    )
    nodes = [{
        "node_id": record_id,
        "subtype": template.target_subtype,
        "attrs": attrs,
        "source_record_id": {
            "operator": "record",
            "record_kind": normalized_kind,
            "record_id": record_id,
        },
        "source_revision": {
            "operator": "record",
            "record_kind": normalized_kind,
            "payload_sha256": payload_sha,
        },
    }]
    admission_payload = {
        "intent": f"record_{normalized_kind}",
        "record_kind": normalized_kind,
        "record_id": record_id,
        "template_id": template.template_id,
        "payload_sha256": payload_sha,
        "nodes": nodes,
    }
    if edges:
        admission_payload["edges"] = edges
    check_summary = {
        "record": {
            "kind": normalized_kind,
            "record_id": record_id,
            "template_id": template.template_id,
            "payload_sha256": payload_sha,
            "node_ids": [node["node_id"] for node in nodes],
            "edge_count": len(edges),
            "emitted_edge_count": len(edges),
            "suppressed_edge_count": len(reference_diagnostics),
            "warnings": warnings,
            "reference_diagnostics": reference_diagnostics,
            "reference_prefix_count": len(active_prefixes),
        }
    }
    return {
        "record_id": record_id,
        "record_kind": normalized_kind,
        "template": _template_payload(template, compatibility=compatibility),
        "payload_sha256": payload_sha,
        "idempotency_key": idempotency_key or f"record:{record_id}:{payload_sha}",
        "admission_payload": admission_payload,
        "check_summary": check_summary,
        "warnings": warnings,
        "node_ids": [node["node_id"] for node in nodes],
        "edge_count": len(edges),
        "emitted_edge_count": len(edges),
        "suppressed_edge_count": len(reference_diagnostics),
        "reference_diagnostics": reference_diagnostics,
    }


def _template_payload(
    template: RecordTemplate,
    *,
    compatibility: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "kind": template.kind,
        "template_id": template.template_id,
        "target_subtype": template.target_subtype,
        "id_prefix": template.id_prefix,
        "required_fields": list(template.required_fields),
        "optional_fields": list(template.optional_fields),
        "reference_fields": list(template.reference_fields),
        "default_attrs": dict(template.default_attrs),
        "primary_node_creation": {
            "target_subtype": template.target_subtype,
            "identity_fields": [template.id_attr],
            "creation_surface": "record_template",
        },
        "description": template.description,
        "example_payload": dict(template.example_payload),
    }
    if compatibility is not None:
        template_info = (
            compatibility.get("templates", {}).get(template.kind, {})
            if isinstance(compatibility.get("templates"), Mapping)
            else {}
        )
        payload["available"] = bool(template_info.get("available", True))
        if template_info.get("unavailable_reason"):
            payload["unavailable_reason"] = template_info["unavailable_reason"]
        payload["reference_field_compatibility"] = list(
            template_info.get("fields") or []
        )
    return payload


def _prefix_payload(
    entry: RecordPrefix,
    *,
    compatibility: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    payload = entry.to_payload()
    if compatibility is None:
        return payload
    prefix_info = {}
    prefixes = compatibility.get("prefixes")
    if isinstance(prefixes, Mapping):
        prefix_info = dict(prefixes.get(entry.prefix) or {})
    payload.update({
        "active": bool(prefix_info.get("active", True)),
        "inactive_reason": prefix_info.get("inactive_reason"),
        "allowed_fields": list(prefix_info.get("allowed_fields") or []),
    })
    return payload


def _record_compatibility(
    prefixes: tuple[RecordPrefix, ...],
    *,
    active_subtypes: Iterable[str] | None,
    narrowing_triples: Iterable[tuple[str, str, str]] | None,
) -> dict[str, Any]:
    active_subtype_set = (
        {str(value) for value in active_subtypes}
        if active_subtypes is not None
        else None
    )
    narrowing_set = (
        {
            (str(src), str(edge), str(dst))
            for src, edge, dst in narrowing_triples
        }
        if narrowing_triples is not None
        else None
    )
    catalog_constrained = (
        active_subtype_set is not None or narrowing_set is not None
    )
    prefix_info: dict[str, dict[str, Any]] = {}
    for prefix in prefixes:
        active = True
        inactive_reason = None
        if (
            active_subtype_set is not None
            and prefix.target_subtype not in active_subtype_set
        ):
            active = False
            inactive_reason = "target_subtype_missing"
        prefix_info[prefix.prefix] = {
            "active": active,
            "inactive_reason": inactive_reason,
            "target_subtype": prefix.target_subtype,
            "allowed_fields": set(),
        }

    template_info: dict[str, dict[str, Any]] = {}
    for kind in RECORD_KINDS:
        template = _TEMPLATES[kind]
        target_missing = (
            active_subtype_set is not None
            and template.target_subtype not in active_subtype_set
        )
        fields: list[dict[str, Any]] = []
        for spec in _field_specs_for_template(kind, template):
            allowed_targets: set[str] = set()
            inactive_targets: list[dict[str, Any]] = []
            for prefix in prefixes:
                status = prefix_info[prefix.prefix]
                if (
                    spec.field == "domain_refs"
                    and prefix.owner == "okg"
                    and prefix.target_subtype != "domain_entity"
                ):
                    allowed, reason = False, "field_target_not_allowed"
                else:
                    allowed, reason = _route_allowed(
                        src_subtype=template.target_subtype,
                        edge_type=spec.edge_type,
                        dst_subtype=prefix.target_subtype,
                        spec=spec,
                        active_subtypes=active_subtype_set,
                        narrowing_triples=narrowing_set,
                    )
                if allowed:
                    allowed_targets.add(prefix.target_subtype)
                    status["allowed_fields"].add(spec.field)
                elif reason and reason != "field_target_not_allowed":
                    inactive_targets.append({
                        "target_subtype": prefix.target_subtype,
                        "prefix": prefix.prefix,
                        "reason": reason,
                    })
            fields.append({
                "field": spec.field,
                "edge_type": spec.edge_type,
                "role": spec.role,
                "allowed_target_subtypes": sorted(allowed_targets),
                "inactive_targets": inactive_targets[:20],
                "available": bool(allowed_targets) or not catalog_constrained,
            })
        template_info[kind] = {
            "available": not target_missing,
            "unavailable_reason": (
                "target_subtype_missing" if target_missing else None
            ),
            "fields": fields,
        }

    for status in prefix_info.values():
        allowed_fields = sorted(status["allowed_fields"])
        status["allowed_fields"] = allowed_fields
        if status["active"] and catalog_constrained and not allowed_fields:
            status["active"] = False
            status["inactive_reason"] = "no_active_record_route"

    return {
        "catalog_constrained": catalog_constrained,
        "active_subtypes": active_subtype_set,
        "narrowing_triples": narrowing_set,
        "prefixes": prefix_info,
        "templates": template_info,
    }


def _field_specs_for_template(
    kind: str,
    template: RecordTemplate,
) -> tuple[ReferenceFieldSpec, ...]:
    advertised = set(template.reference_fields)
    specs: list[ReferenceFieldSpec] = [
        spec for spec in _COMMON_REFERENCE_FIELD_SPECS
        if spec.field in advertised
    ]
    if "subject_ref" in advertised:
        specs.append(ReferenceFieldSpec(
            "subject_ref", "references", "subject",
            allow_any_catalog_route=True,
        ))
    if "subject_id" in advertised:
        specs.append(ReferenceFieldSpec(
            "subject_id", "references", "subject",
            allow_any_catalog_route=True,
        ))
    if kind in {"hypothesis", "finding"}:
        for field in ("evidence_refs", "supports"):
            if field in advertised:
                specs.append(ReferenceFieldSpec(
                    field, "supported_by", "supporting_evidence",
                    allow_any_catalog_route=True,
                ))
        for field in ("contradicting_refs", "contradicts"):
            if field in advertised:
                specs.append(ReferenceFieldSpec(
                    field, "contradicted_by", "contradicting_evidence",
                    allow_any_catalog_route=True,
                ))
        if "supersedes_refs" in advertised:
            specs.append(ReferenceFieldSpec(
                "supersedes_refs", "supersedes",
                "superseded_record",
                allow_any_catalog_route=True,
            ))
    elif kind == "verification":
        for field in ("target_refs", "verifies"):
            if field in advertised:
                specs.append(ReferenceFieldSpec(
                    field, "verifies", "verification_target",
                    allow_any_catalog_route=True,
                ))
    elif kind == "experiment_run":
        for field in ("produced_refs", "produces"):
            if field in advertised:
                specs.append(ReferenceFieldSpec(
                    field, "produces", "produced",
                    allow_any_catalog_route=True,
                ))
    elif kind == "evidence":
        if "target_refs" in advertised:
            specs.append(ReferenceFieldSpec(
                "target_refs", "references", "evidence_target",
                allow_any_catalog_route=True,
            ))
    return tuple(specs)


def _route_allowed(
    *,
    src_subtype: str,
    edge_type: str,
    dst_subtype: str,
    spec: ReferenceFieldSpec,
    active_subtypes: set[str] | None,
    narrowing_triples: set[tuple[str, str, str]] | None,
) -> tuple[bool, str | None]:
    if active_subtypes is not None and dst_subtype not in active_subtypes:
        return False, "target_subtype_missing"
    if (
        spec.denied_target_subtypes
        and dst_subtype in spec.denied_target_subtypes
    ):
        return False, "field_target_not_allowed"
    if (
        spec.allowed_target_subtypes
        and dst_subtype not in spec.allowed_target_subtypes
    ):
        if not spec.allow_any_catalog_route:
            return False, "field_target_not_allowed"
        if narrowing_triples is None:
            return True, None
    if (
        narrowing_triples is not None
        and (src_subtype, edge_type, dst_subtype) not in narrowing_triples
    ):
        return False, "missing_narrowing"
    return True, None


def _normalize_kind(kind: str) -> str:
    normalized = str(kind or "").strip().lower().replace("-", "_")
    if normalized not in _TEMPLATES:
        raise RecordPayloadError(
            code="unsupported_record_kind",
            message=f"unsupported record kind {kind!r}",
            details={
                "record_kinds": list(RECORD_KINDS),
            },
        )
    return normalized


def _validate_required_fields(
    item: Mapping[str, Any],
    template: RecordTemplate,
) -> None:
    for field in template.required_fields:
        value = item.get(field)
        if not isinstance(value, str) or not value.strip():
            raise RecordPayloadError(
                code="invalid_record_payload",
                message=f"record payload missing required field {field!r}",
                field=field,
                template=_template_payload(template),
            )


def _record_attrs(
    item: Mapping[str, Any],
    *,
    template: RecordTemplate,
    record_id: str,
    kind: str,
    payload_sha: str,
    observed_at: str,
    authority: Mapping[str, Any],
    warnings: list[str],
) -> dict[str, Any]:
    status = item.get("status") or template.default_attrs.get("status")
    if (
        kind in {"hypothesis", "finding"}
        and str(status).lower() == "supported"
        and "supported_claim_without_known_evidence" in warnings
    ):
        status = "candidate"
    attrs: dict[str, Any] = {
        template.id_attr: record_id,
        "record_kind": kind,
        "status": status,
        "confidence_kind": item.get("confidence_kind") or "model_inferred",
        "observed_at": observed_at,
        "extraction_method": item.get("extraction_method") or "record_operator",
        "scope": (
            item.get("scope")
            or item.get("study_ref")
            or item.get("project_ref")
            or authority.get("producer_id")
            or authority.get("actor")
            or "record"
        ),
        "source_artifact": (
            item.get("source_artifact")
            or _first_string(item.get("artifact_refs"))
            or f"record:{record_id}"
        ),
        "source_hash": item.get("source_hash") or payload_sha,
    }
    if item.get("session_ref") or item.get("session_id"):
        attrs["session_id"] = item.get("session_ref") or item.get("session_id")
    if template.target_subtype == "agent_claim":
        statement = _required_text(item, "statement")
        attrs.update({
            "statement": statement,
            "claim_kind": item.get("claim_kind")
            or template.default_attrs.get("claim_kind"),
            "subject_id": _claim_subject_id(item),
            "text": statement[:1000],
        })
    elif template.target_subtype == "verification_result":
        summary = _required_text(item, "summary")
        attrs.update({
            "summary": summary,
            "verdict": _required_text(item, "verdict"),
            "text": summary[:1000],
        })
    elif template.target_subtype == "agent_decision":
        summary = _required_text(item, "summary")
        attrs.update({
            "summary": summary,
            "decision_kind": item.get("decision_kind")
            or template.default_attrs.get("decision_kind"),
            "text": summary[:1000],
        })
        if item.get("rationale"):
            attrs["rationale"] = item.get("rationale")
    elif template.target_subtype == "agent_experiment":
        summary = _required_text(item, "summary")
        attrs.update({
            "summary": summary,
            "experiment_kind": item.get("experiment_kind")
            or template.default_attrs.get("experiment_kind"),
            "text": summary[:1000],
        })
    elif template.target_subtype == "project":
        title = _required_text(item, "title")
        slug = _project_slug(item, record_id)
        attrs.update({
            "project_id": record_id,
            "slug": slug,
            "title": title,
            "status": status or "active",
            "created_at": observed_at,
            "last_modified_at": observed_at,
            "text": title[:1000],
        })
        if item.get("purpose"):
            attrs["purpose"] = item.get("purpose")
    elif template.target_subtype == "study":
        title = _required_text(item, "title")
        attrs.update({
            "study_id": _identity_value(record_id, template.id_prefix),
            "title": title,
            "text": title[:1000],
        })
        if item.get("description"):
            attrs["description"] = item.get("description")
    elif template.target_subtype == "domain_entity":
        label = _required_text(item, "label")
        domain_kind = _required_text(item, "domain_kind")
        attrs.update({
            "domain_entity_id": _identity_value(
                record_id, f"{domain_kind}:",
            ),
            "domain_kind": domain_kind,
            "label": label,
            "title": item.get("title") or label,
            "text": label[:1000],
        })
    else:
        summary = _required_text(item, "summary")
        attrs.update({
            "summary": summary,
            "observation_kind": item.get("observation_kind")
            or template.default_attrs.get("observation_kind"),
            "text": summary[:1000],
        })
    if warnings:
        attrs["validation_warnings"] = warnings
    return _clean(attrs)


def _record_edges(
    record_id: str,
    record_subtype: str,
    kind: str,
    item: Mapping[str, Any],
    *,
    payload_sha: str,
    prefixes: tuple[RecordPrefix, ...],
    compatibility: Mapping[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], list[str], list[dict[str, Any]]]:
    edges: list[dict[str, Any]] = []
    warnings: list[str] = []
    diagnostics: list[dict[str, Any]] = []
    compatibility = compatibility or {}
    prefix_status = (
        compatibility.get("prefixes", {})
        if isinstance(compatibility.get("prefixes"), Mapping)
        else {}
    )
    active_subtypes = compatibility.get("active_subtypes")
    if active_subtypes is not None:
        active_subtypes = {str(value) for value in active_subtypes}
    narrowing_triples = compatibility.get("narrowing_triples")
    if narrowing_triples is not None:
        narrowing_triples = {
            (str(src), str(edge), str(dst))
            for src, edge, dst in narrowing_triples
        }

    def suppress(
        *,
        field: str,
        node_id: str,
        edge_type: str | None,
        role: str | None,
        prefix: RecordPrefix | None,
        reason: str,
        src_subtype: str = record_subtype,
    ) -> None:
        diagnostics.append(_clean({
            "code": "unsupported_record_reference",
            "reason": reason,
            "field": field,
            "node_id": node_id,
            "edge_type": edge_type,
            "role": role,
            "src_subtype": src_subtype,
            "dst_subtype": prefix.target_subtype if prefix else None,
            "prefix": prefix.prefix if prefix else None,
        }))
        if reason == "unrecognized_node_id":
            warnings.append(f"unrecognized_node_id:{node_id}")
        elif reason == "field_not_advertised":
            warnings.append(f"unsupported_record_field:{field}")
        else:
            warnings.append(f"unsupported_record_reference:{field}:{node_id}")

    def add(spec: ReferenceFieldSpec, dst: str) -> None:
        prefix = _prefix_for_id(dst, prefixes)
        if prefix is None:
            suppress(
                field=spec.field,
                node_id=dst,
                edge_type=spec.edge_type,
                role=spec.role,
                prefix=None,
                reason="unrecognized_node_id",
            )
            return
        status = (
            prefix_status.get(prefix.prefix, {})
            if isinstance(prefix_status, Mapping)
            else {}
        )
        inactive_reason = str(status.get("inactive_reason") or "")
        if (
            status
            and not bool(status.get("active", True))
            and inactive_reason != "no_active_record_route"
        ):
            suppress(
                field=spec.field,
                node_id=dst,
                edge_type=spec.edge_type,
                role=spec.role,
                prefix=prefix,
                reason=inactive_reason or "inactive_prefix",
            )
            return
        if (
            spec.field == "domain_refs"
            and prefix.owner == "okg"
            and prefix.target_subtype != "domain_entity"
        ):
            suppress(
                field=spec.field,
                node_id=dst,
                edge_type=spec.edge_type,
                role=spec.role,
                prefix=prefix,
                reason="field_target_not_allowed",
            )
            return
        allowed, reason = _route_allowed(
            src_subtype=record_subtype,
            edge_type=spec.edge_type,
            dst_subtype=prefix.target_subtype,
            spec=spec,
            active_subtypes=active_subtypes,
            narrowing_triples=narrowing_triples,
        )
        if not allowed:
            suppress(
                field=spec.field,
                node_id=dst,
                edge_type=spec.edge_type,
                role=spec.role,
                prefix=prefix,
                reason=reason or "unsupported_route",
            )
            return
        edges.append({
            "src": record_id,
            "dst": dst,
            "edge_type": spec.edge_type,
            "src_subtype": record_subtype,
            "dst_subtype": prefix.target_subtype,
            "attrs": _clean({
                "role": spec.role,
                "source": "record_operator",
                "record_kind": kind,
                "target_subtype": prefix.target_subtype,
                "field": spec.field,
            }),
            "provenance": "explicit",
            "confidence": 1.0,
            "source_record_id": {
                "operator": "record",
                "record_id": record_id,
                "target_id": dst,
                "edge": spec.edge_type,
                "role": spec.role,
                "field": spec.field,
            },
            "source_revision": {
                "operator": "record",
                "record_kind": kind,
                "payload_sha256": payload_sha,
            },
        })

    template = _TEMPLATES[kind]
    advertised_fields = set(template.reference_fields)
    spec_by_field = {
        spec.field: spec for spec in _field_specs_for_template(kind, template)
    }
    for field in sorted(_REFERENCE_FIELD_NAMES):
        values = _string_values(item.get(field))
        if not values:
            continue
        spec = spec_by_field.get(field)
        if spec is None or field not in advertised_fields:
            for dst in values:
                suppress(
                    field=field,
                    node_id=dst,
                    edge_type=None,
                    role=None,
                    prefix=_prefix_for_id(dst, prefixes),
                    reason="field_not_advertised",
                )
            continue
        seen: set[str] = set()
        for dst in values:
            if dst in seen:
                continue
            seen.add(dst)
            add(spec, dst)
    return edges, sorted(set(warnings)), diagnostics


def _record_id(
    kind: str,
    item: Mapping[str, Any],
    payload_sha: str,
    template: RecordTemplate,
    active_prefixes: tuple[RecordPrefix, ...] = (),
) -> str:
    explicit_node_id = item.get("node_id") or item.get("id")
    if isinstance(explicit_node_id, str) and explicit_node_id.strip():
        candidate = explicit_node_id.strip()
        # Only honor a caller-supplied node_id when its prefix is one the record
        # tool may MINT under (creation_surface == "record_template"). A prefix
        # that is merely *present* in the active set is not enough: the curated /
        # ingest-owned builtins (doc:, commit:, file:, issue:, pr:, chunk:,
        # mention:, session:, code-graph symbols, ...) are active prefixes too but
        # carry creation_surface="forbidden", so honoring on presence alone let an
        # agent write to / collide with curated node ids. Enforce the surface and
        # fail closed when no namespace is configured (harden-mcp-write-boundaries;
        # adversarial-review remediation).
        entry = _prefix_for_id(candidate, active_prefixes) if active_prefixes else None
        if entry is not None and entry.creation_surface == "record_template":
            return candidate
        # else: forbidden / foreign / unconfigured prefix -> server-mint below.
    explicit = item.get(template.id_attr)
    if isinstance(explicit, str) and explicit.strip():
        identity = explicit.strip()
        if kind == "project":
            if identity.startswith(("proj:", "project:")):
                return identity
            return f"proj:{identity}"
        if kind == "domain_entity":
            # A domain id may use an arbitrary domain namespace (region:, method:,
            # a novel one) supplied via an explicit prefix OR via domain_kind —
            # BUT it must not mint in a curated / ingest-owned namespace (doc:,
            # commit:, file:, issue:, pr:, chunk:, mention:, session:, ...). Both
            # `domain_entity_id="doc:x"` and `domain_kind="doc"` were honored
            # verbatim, bypassing the node_id creation_surface gate above; reject
            # a forbidden-builtin prefix and server-mint instead (harden-mcp-write-
            # boundaries; adversarial-review remediation).
            domain_kind = str(item.get("domain_kind") or "").strip()
            if ":" in identity:
                candidate = identity
            elif domain_kind:
                candidate = f"{domain_kind}:{identity}"
            else:
                candidate = identity
            entry = (
                _prefix_for_id(candidate, active_prefixes)
                if active_prefixes else None
            )
            if entry is None or entry.creation_surface != "forbidden":
                return candidate
            # forbidden-namespace injection -> server-mint here (do NOT fall
            # through to the generic `return identity` below, which would honor
            # the curated id verbatim).
            return f"domain:record:{payload_sha[:20]}"
        if template.id_prefix and not identity.startswith(template.id_prefix):
            return f"{template.id_prefix}{identity}"
        return identity
    prefix_by_kind = {
        "artifact": "observation",
        "decision": "decision",
        "domain_entity": "domain",
        "evidence": "observation",
        "experiment_run": "experiment",
        "finding": "claim",
        "hypothesis": "claim",
        "observation": "observation",
        "project": "proj",
        "study": "study",
        "verification": "verification",
    }
    return f"{prefix_by_kind[kind]}:record:{payload_sha[:20]}"


def _validation_warnings(
    kind: str,
    item: Mapping[str, Any],
    prefixes: tuple[RecordPrefix, ...],
) -> list[str]:
    warnings: list[str] = []
    advertised_fields = set(_TEMPLATES[kind].reference_fields)
    if (
        kind in {"hypothesis", "finding"}
        and str(item.get("status") or "").lower() == "supported"
        and not _known_node_ids(item.get("evidence_refs"), prefixes)
        and not _known_node_ids(item.get("supports"), prefixes)
    ):
        warnings.append("supported_claim_without_known_evidence")
    for field in _REFERENCE_FIELD_NAMES:
        if field not in advertised_fields:
            continue
        for node_id in _string_values(item.get(field)):
            if node_id and _node_subtype_for_id(node_id, prefixes) is None:
                warnings.append(f"unrecognized_node_id:{node_id}")
    return sorted(set(warnings))


def _claim_subject_id(item: Mapping[str, Any]) -> str:
    subject = item.get("subject_id") or item.get("subject_ref")
    if isinstance(subject, str) and subject.strip():
        return subject.strip()
    for field in ("study_ref", "project_ref", "domain_refs", "artifact_refs"):
        value = _first_string(item.get(field))
        if value:
            return value
    return "record:unspecified_subject"


def _required_text(item: Mapping[str, Any], field: str) -> str:
    value = item.get(field)
    if isinstance(value, str) and value.strip():
        return value.strip()
    raise RecordPayloadError(
        code="invalid_record_payload",
        message=f"record payload missing required field {field!r}",
        field=field,
    )


def _json_sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8"),
    ).hexdigest()


def _clean(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        str(key): val
        for key, val in value.items()
        if val is not None and val != ""
    }


# _node_subtype_for_id / _prefix_for_id / _normalize_prefixes now live in
# okg.substrate.library.agent_memory_prefixes (imported + aliased at the top of
# this module). Per substrate-operability.


def _string_values(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [str(v) for v in value if isinstance(v, str)]
    return []


def _known_node_ids(
    value: Any,
    prefixes: tuple[RecordPrefix, ...],
) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for node_id in _string_values(value):
        if node_id in seen:
            continue
        if _node_subtype_for_id(node_id, prefixes) is None:
            continue
        seen.add(node_id)
        out.append(node_id)
    return out


def _first_string(value: Any) -> str | None:
    values = _string_values(value)
    return values[0] if values else None


def _identity_value(node_id: str, prefix: str | None) -> str:
    if prefix and node_id.startswith(prefix):
        return node_id[len(prefix):]
    return node_id


def _project_slug(item: Mapping[str, Any], record_id: str) -> str:
    explicit = item.get("slug")
    if isinstance(explicit, str) and explicit.strip():
        return explicit.strip()
    for prefix in ("proj:", "project:"):
        if record_id.startswith(prefix):
            return record_id[len(prefix):]
    return record_id
