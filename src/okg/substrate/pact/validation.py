"""PACT v0 validation and derived alignment."""

from __future__ import annotations

import ast
import hashlib
import os
import re
import shlex
import tomllib
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

import yaml

from okg.substrate.pact import envelope
from okg.substrate.pact.evaluation_context import PactEvaluationContext
from okg.substrate.pact.evidence_kinds import (
    DEFAULT_REGISTRY,
    EvidenceKindRegistry,
    load_registry,
)
from okg.substrate.pact.models import PactManifest
from okg.substrate.pact.store import intent_digest


PASSING_EVIDENCE = {"pass", "waived"}
# `partial` records a run that executed some of what a task asked and could
# not execute the rest. It is deliberately ABSENT from PASSING_EVIDENCE:
# per `pact-acceptance-prose-is-enforceable`
# req.pact-acceptance-prose-is-enforceable.partial-is-recordable a partial row
# proves nothing, while staying visible in evidence status as work that ran.
# The constant exists so the surfaces that must distinguish it from `missing`
# name it once instead of hard-coding the string.
PARTIAL_EVIDENCE = "partial"
# `evidence_policy.required` answers ONE question: "what proof must EXIST"
# (completeness). It was also being read as the answer to a second, different
# question — "what recorded outcome may REFUTE" (soundness) — and it is the
# wrong index for that one. Requirement satisfaction quantified over the
# policy's kinds, so a `fail` row of any OTHER kind was never a candidate and
# its status was never read: a requirement could not fail on a measurement its
# policy had no slot for, however plainly its own statement said that
# measurement blocked it (`pact-evidence-policy-blindness`
# req.pact-evidence-policy-blindness.blocking-measurement-blocks). These are
# the statuses that refute a requirement whatever kind carries them — the
# non-passing statuses that record an OUTCOME.
#
# `planned` is deliberately absent, and its absence is not unknown->success.
# It records an INTENTION, not a measurement, so there is nothing in it to
# weigh, and the completeness quantifier above is untouched: a requirement
# still cannot close without passing rows of every kind its policy names, and
# a `planned` row never becomes the thing that closes it either. Blocking on
# it would refuse a requirement for having DECLARED A PLAN — 10 of the 17
# exposed pairs in the corpus sweep are future-tense design_review scaffolds —
# and would put standing pressure on authors to DELETE scaffold rows in order
# to close a gate, which is the same falsification this rule exists to stop.
REFUTING_EVIDENCE = {"fail", PARTIAL_EVIDENCE, "missing"}
# The bare token `_missing_requirement_evidence` emits for a refuting row, and
# the prefix `repair.parse_proof_slot` matches to keep it OUT of the attach
# commands: no `evidence attach` clears a refutation. Only fixing what the row
# measured, or correcting the row's status, does. A requirement-scoped waiver
# clears the REQUIREMENT alignment below (:2771-2776) and so removes the gap
# from `pact view`, but not from `gate task-done`: the task rollup calls this
# function directly at :2819-2827 and never consults `_waiver_for`. That
# asymmetry predates this change — it applies to every kind of gap, not only
# refutations — and is recorded rather than widened here.
REFUTATION_TOKEN = "refuted_by"
# Waiver proof tokens do not resolve to evidence rows at all (they resolve to
# `manifest.waivers`), so no evidence row can ever close a waiver arm
# alongside another arm. The sentinel keeps the arm-separability domain
# arithmetic total instead of special-casing waivers at every comparison.
_WAIVER_ARM_DOMAIN = frozenset({"__waiver__"})
# Derived from the canonical evidence-kind registry (single source per
# `polish-pact-agent-ux` req.polish.evidence-kinds).
TEST_KINDS = DEFAULT_REGISTRY.expand("executed_test")
FORMAL_KINDS = DEFAULT_REGISTRY.expand("formal_check")
CURRENT_ARTIFACT_FORMAT_VERSION = 4
SUPPORTED_TARGET_LANGUAGE_VERSIONS = {0, 1, 2, 3, 4}
ACCEPTANCE_PROOF_TOKENS = {
    "planned_test",
    "executed_test",
    "pytest",
    "junit",
    "test",
    "manual",
    "formal",
    "formal_check",
    "graph_report",
    "published_generation",
    "waiver",
}
LOOP_RECORD_REQUIRED_FIELDS = {
    "observation",
    "failure_mode",
    "friction_ledger",
    "ranked_improvement",
    "decision",
    "verification",
    "next_observation",
}
FRICTION_LEDGER_REQUIRED_FIELDS = {
    "observation",
    "failure_mode",
    "severity",
    "frequency",
    "candidate_improvement",
    "verification",
}
LifecycleGate = Literal["proposal", "implementation", "verification"]

GUIDANCE_REVIEW_TRIGGER_AREAS = {
    "agent-coordination",
    "agent-instructions",
    "agent-skills",
    "agent-workflow",
    "cli",
    "deployment",
    "deployment-runtime",
    "deployment-upgrade",
    "operator-cli",
    "operator-guidance",
    "pact",
    "pact-gates",
    "pact-workflow",
    "prompt-surface",
    "prompts",
    "skills",
}
GUIDANCE_SURFACE_AREAS = {
    "agent-guidance",
    "agent-instructions",
    "agent-skills",
    "documentation",
    "docs",
    "operator-guidance",
    "prompt-surface",
    "prompts",
}
GUIDANCE_REVIEW_TRIGGER_PATH_PREFIXES = (
    ".agents/",
    ".claude/",
    "deployments/okg-workspace/coordination/",
    "src/okg/deployment/",
    "src/okg/substrate/cli/",
    "src/okg/substrate/coordination/",
    "src/okg/substrate/deployment",
    "src/okg/substrate/deployment_",
    "src/okg/substrate/pact/",
    "src/okg/substrate/runtime/",
)
GUIDANCE_SURFACE_PATH_PREFIXES = (
    ".agents/",
    ".claude/",
    ".codex/skills/",
    "docs/",
    "pact/AGENTS.md",
    "prompts/",
    "src/okg/substrate/pact/skills/",
)
GUIDANCE_REVIEW_EVIDENCE_TOKENS = {
    "agent-guidance",
    "docs",
    "documentation",
    "guidance",
    "guidance-review",
    "operator-guidance",
}
GUIDANCE_REVIEW_WAIVER_TOKENS = {
    "docs-review",
    "documentation-review",
    "guidance",
    "guidance-review",
}


class _Unset:
    """Sentinel: this check names no offending value at all."""

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return "<unset>"


_UNSET = _Unset()


@dataclass
class ValidationIssue:
    code: str
    message: str
    path: str
    severity: str = "error"
    # The value the check REFUSED, carried from the raise site — never parsed
    # back out of `message` (pact-self-repairing-refusals req.refusal.complete).
    # A repair descriptor needs the offending value to be mechanically
    # applicable; recovering it from prose is how the vocabulary rots.
    # `_UNSET` distinguishes "this check does not name a value" from "the
    # offending value IS None/empty", which is itself the defect for the
    # subject-deletion codes (an empty policy, an unlinked task).
    offending_value: Any = _UNSET

    @property
    def names_offending_value(self) -> bool:
        return self.offending_value is not _UNSET

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "severity": self.severity,
            "code": self.code,
            "path": self.path,
            "message": self.message,
        }
        if self.names_offending_value:
            payload["offending_value"] = self.offending_value
        return payload


@dataclass
class ValidationReport:
    ok: bool
    issues: list[ValidationIssue] = field(default_factory=list)
    alignment: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return envelope.component(
            ok=self.ok,
            errors=[i.to_dict() for i in self.issues if i.severity == "error"],
            warnings=[i.to_dict() for i in self.issues if i.severity == "warning"],
            alignment=self.alignment,
        )


def _kind_registry(
    repo_root: Path | None,
    *,
    context: PactEvaluationContext | None,
) -> EvidenceKindRegistry:
    if context is not None and context.evidence_registry is not None:
        return context.evidence_registry
    if context is not None and context.files is not None:
        return EvidenceKindRegistry(
            canonical=DEFAULT_REGISTRY.canonical,
            aliases=dict(DEFAULT_REGISTRY.aliases),
            config_errors=(
                "captured file authority did not include a parsed "
                "pact/project.yaml evidence-kind registry",
            ),
        )
    return load_registry(repo_root)


def validate_manifest(
    manifest: PactManifest,
    *,
    completion_gate: bool = False,
    lifecycle_gate: LifecycleGate | None = None,
    repo_root: Path | str | None = None,
    context: PactEvaluationContext | None = None,
) -> ValidationReport:
    issues: list[ValidationIssue] = []
    repo_root_path = Path(repo_root) if repo_root is not None else None
    kind_registry = _kind_registry(repo_root_path, context=context)
    # A malformed registry block must not silently fall back to defaults.
    for config_error in kind_registry.config_errors:
        issues.append(
            ValidationIssue(
                "invalid_evidence_kind_registry",
                config_error,
                "pact/project.yaml:evidence_kinds",
                severity="warning",
            )
        )
    req_ids = {r.id for r in manifest.requirements}
    acceptance_ids = {a.id for r in manifest.requirements for a in r.acceptance}
    # PRESENCE, not quantification (pact-vacuous-gate-close req.vacuous.presence):
    # the per-requirement policy loop below is vacuously satisfied when there are
    # no requirements at all, so a manifest that declares work (tasks) with
    # nothing to prove reads as clean. A program umbrella that delegates every
    # task to its children declares no tasks and is exempt: its contract is
    # `hierarchy.integration_acceptance`, carried by the children's manifests.
    issues.extend(_no_requirements_at_all_issues(manifest))
    task_ids = {t.id for t in manifest.tasks}
    test_ids = {t.id for t in manifest.test_plan}
    waiver_ids = {w.id for w in manifest.waivers}
    formal_ids = {f.id for f in manifest.formal}
    evidence_by_id = {e.id: e for e in manifest.evidence}
    target_language_version = _target_language_version(manifest)

    # Receipt self-hashes and canonical ordering are file-local invariants,
    # so every gate can reject tamper without consulting Git or a network.
    # Commit/tree ancestry is checked separately at the exact-head boundary.
    from okg.substrate.pact.proof_identity import static_code_proof_errors

    for receipt_error in static_code_proof_errors(manifest):
        issues.append(
            ValidationIssue(
                receipt_error.code,
                receipt_error.message,
                str(receipt_error.data.get("path") or "proof_receipts.code"),
                offending_value=receipt_error.data or None,
            )
        )

    tier_labels: list[int] = []
    invalid_tier_labels: list[str] = []
    for label in manifest.change.labels:
        if not label.startswith("tier-"):
            continue
        suffix = label.removeprefix("tier-")
        if not suffix.isdigit():
            invalid_tier_labels.append(label)
            continue
        tier = int(suffix)
        if tier not in {0, 1, 2, 3} or suffix != str(tier):
            invalid_tier_labels.append(label)
            continue
        tier_labels.append(tier)
    if invalid_tier_labels:
        issues.append(ValidationIssue(
            "invalid_lifecycle_tier_label",
            "tier labels must use the canonical tier-0 through tier-3 shape",
            "change.labels",
            offending_value=invalid_tier_labels,
        ))
    if len(set(tier_labels)) > 1 or (
        manifest.change.tier is not None
        and tier_labels
        and any(tier != manifest.change.tier for tier in tier_labels)
    ):
        issues.append(ValidationIssue(
            "lifecycle_tier_conflict",
            "change.tier and tier-N labels must declare one lifecycle tier",
            "change.tier",
            offending_value={
                "tier": manifest.change.tier,
                "label_tiers": tier_labels,
            },
        ))
    if (
        target_language_version >= 4
        and manifest.change.tier is None
        and not tier_labels
    ):
        legacy_allowed = False
        if repo_root_path is not None:
            from okg.substrate.pact.project_settings import (
                legacy_missing_tier_allowed,
            )

            legacy_allowed = legacy_missing_tier_allowed(
                repo_root_path,
                manifest.change.id,
                context=context,
            )
        if not legacy_allowed:
            issues.append(ValidationIssue(
                "missing_lifecycle_tier",
                "v4 PACT must declare change.tier (or use the repo legacy cohort)",
                "change.tier",
                offending_value=None,
            ))

    _check_unique(req_ids, len(manifest.requirements), "requirements", issues)
    _check_unique(task_ids, len(manifest.tasks), "tasks", issues)
    _check_unique(
        acceptance_ids,
        sum(len(r.acceptance) for r in manifest.requirements),
        "acceptance",
        issues,
    )

    for idx, req in enumerate(manifest.requirements):
        # Per `simplify-pact-surface` task 4: scenarios + acceptance are
        # advisory (most PACTs encode the contract in the requirement's
        # `statement` field; the BDD-style scaffolding restates it).
        # Per `polish-pact-first-run-dx` req.pactdx.warning-contradiction
        # (issue #643): a requirement whose evidence_policy declares
        # executable evidence IS the documented lean shape (pact/AGENTS.md
        # says to skip `acceptance.scenarios` boilerplate) — warning on it
        # scolds authors for following the instructions. The warnings
        # remain only for requirements with NO evidence policy, where
        # scenarios/acceptance are the sole completeness signal.
        declares_evidence = bool(
            req.evidence_policy.required or req.evidence_policy.required_any
        )
        if not req.scenarios and not declares_evidence:
            issues.append(
                ValidationIssue(
                    "missing_scenario",
                    f"requirement {req.id} has no scenarios",
                    f"requirements[{idx}].scenarios",
                    severity="warning",
                    offending_value=list(req.scenarios),
                )
            )
        if not req.acceptance and not declares_evidence:
            issues.append(
                ValidationIssue(
                    "missing_acceptance",
                    f"requirement {req.id} has no acceptance criteria",
                    f"requirements[{idx}].acceptance",
                    severity="warning",
                    offending_value=[],
                )
            )
        for acc_idx, acceptance in enumerate(req.acceptance):
            # Arms carry proof tokens too, so a criterion that declares its
            # proofs per-arm is linked even with an empty flat `evidence`.
            if not acceptance_proof_tokens(acceptance):
                issues.append(
                    ValidationIssue(
                        "missing_acceptance_evidence",
                        f"acceptance criterion {acceptance.id} has no proof link",
                        f"requirements[{idx}].acceptance[{acc_idx}].evidence",
                        offending_value=list(acceptance.evidence),
                    )
                )
        if not req.evidence_policy.required and not req.evidence_policy.required_any:
            issues.append(
                ValidationIssue(
                    "missing_evidence_policy",
                    f"requirement {req.id} has no required evidence policy",
                    f"requirements[{idx}].evidence_policy.required",
                    offending_value=list(req.evidence_policy.required),
                )
            )
        issues.extend(_policy_kind_issues(idx, req, kind_registry))
        # Per `right-size-pact-authoring` req.rightsize.tiers: the fast-shape
        # scaffold mints TODO placeholders; they must stay visible until
        # replaced with real content, never quietly gate to done.
        if "TODO" in req.statement:
            issues.append(
                ValidationIssue(
                    "placeholder_content",
                    f"requirement {req.id} statement still carries a TODO placeholder",
                    f"requirements[{idx}].statement",
                    severity="warning",
                )
            )
        issues.extend(_default_requirement_boilerplate_issues(idx, req))

    for idx, task in enumerate(manifest.tasks):
        issues.extend(_task_requirements_link_issues(idx, task))
        from okg.substrate.pact.published_generation_authority import (
            parse_published_generation_authority,
        )

        _authority, authority_issues = parse_published_generation_authority(
            task,
            task_path=f"tasks[{idx}]",
            target_pact_version=target_language_version,
        )
        issues.extend(
            ValidationIssue(
                issue.code,
                issue.message,
                issue.path,
                offending_value=issue.offending_value,
            )
            for issue in authority_issues
        )
        for req_id in task.requirements:
            if req_id not in req_ids:
                issues.append(
                    ValidationIssue(
                        "unknown_requirement_ref",
                        f"task {task.id} references unknown requirement {req_id}",
                        f"tasks[{idx}].requirements",
                        offending_value=req_id,
                    )
                )
        if not task.verification.strip():
            issues.append(
                ValidationIssue(
                    "missing_task_verification",
                    f"task {task.id} has no verification command",
                    f"tasks[{idx}].verification",
                    offending_value=task.verification,
                )
            )

    graph: dict[str, list[str]] = {task_id: [] for task_id in task_ids}
    for idx, dep in enumerate(manifest.dependencies):
        if dep.from_task not in task_ids:
            issues.append(
                ValidationIssue(
                    "unknown_dependency_from",
                    f"dependency references unknown from task {dep.from_task}",
                    f"dependencies[{idx}].from",
                )
            )
        if dep.to_task not in task_ids:
            issues.append(
                ValidationIssue(
                    "unknown_dependency_to",
                    f"dependency references unknown to task {dep.to_task}",
                    f"dependencies[{idx}].to",
                )
            )
        if dep.waiver and dep.waiver not in waiver_ids:
            issues.append(
                ValidationIssue(
                    "unknown_dependency_waiver",
                    f"dependency waiver {dep.waiver} does not exist",
                    f"dependencies[{idx}].waiver",
                )
            )
        if dep.kind in {"blocks", "requires"} and not dep.waiver:
            graph.setdefault(dep.from_task, []).append(dep.to_task)
    issues.extend(_cycle_issues(graph))

    for idx, test in enumerate(manifest.test_plan):
        if test.requirement not in req_ids and test.requirement not in acceptance_ids:
            issues.append(
                ValidationIssue(
                    "stale_test_reference",
                    f"test {test.id} references unknown requirement or criterion {test.requirement}",
                    f"test_plan[{idx}].requirement",
                    offending_value=test.requirement,
                )
            )
        if "TODO" in str(test.test_id):
            issues.append(
                ValidationIssue(
                    "placeholder_content",
                    f"test {test.id} still points at the TODO placeholder test id",
                    f"test_plan[{idx}].test_id",
                    severity="warning",
                )
            )

    for idx, formal in enumerate(manifest.formal):
        if formal.requirement not in req_ids:
            issues.append(
                ValidationIssue(
                    "unknown_formal_requirement",
                    f"formal binding {formal.id} references unknown requirement {formal.requirement}",
                    f"formal[{idx}].requirement",
                )
            )
        if not formal.refinement_tests:
            issues.append(
                ValidationIssue(
                    "missing_formal_refinement",
                    f"formal binding {formal.id} has no refinement tests",
                    f"formal[{idx}].refinement_tests",
                )
            )
        if target_language_version >= 3:
            if not formal.config:
                issues.append(
                    ValidationIssue(
                        "missing_formal_config",
                        f"formal binding {formal.id} has no checker config",
                        f"formal[{idx}].config",
                    )
                )
            if not formal.checker_command:
                issues.append(
                    ValidationIssue(
                        "missing_formal_checker_command",
                        f"formal binding {formal.id} has no checker command",
                        f"formal[{idx}].checker_command",
                    )
                )
        if repo_root_path is not None:
            issues.extend(
                _formal_artifact_issues(
                    idx,
                    formal,
                    repo_root_path,
                    context=context,
                )
            )

    # KNOWN GAP (measured, deliberate): `manifest.change.id` is a valid ref, so
    # `refs: [<change-id>]` passes the ref check while resolving to no
    # requirement, acceptance criterion or task — the vacuity `refless_evidence`
    # closes, one indirection later. 13 rows across 8 active manifests rely on
    # it, and 10 of those are written by okg itself (`evidence approve` emits
    # `intent_review` with refs=[change id]; the loop recorder does the same),
    # so refusing it here would make a verb emit rows its own validator
    # rejects — the round-trip req.vacuous.presence forbids. Follow-on PACT:
    # give change-scoped rows a real subject, then require every ref to name a
    # requirement/acceptance/task/test/formal id.
    known_refs = evidence_ref_vocabulary(manifest)
    for idx, evidence in enumerate(manifest.evidence):
        issues.extend(_refless_evidence_issues(idx, evidence))
        # A ref that names only the CHANGE resolves to no requirement,
        # acceptance criterion or task, so it satisfies no proof slot — the
        # same vacuity `refless_evidence` catches, one step less obvious.
        # NOT an error today: `evidence approve` and the loop recorder both
        # WRITE change-scoped rows (13 of them in the active corpus), so
        # refusing here would make verbs emit rows their own validator
        # rejects. Follow-on: give those rows a real subject, then promote.
        for ref in evidence.refs:
            if ref not in known_refs:
                issues.append(
                    ValidationIssue(
                        "unknown_evidence_ref",
                        f"evidence {evidence.id} references unknown id {ref}",
                        f"evidence[{idx}].refs",
                        offending_value=ref,
                    )
                )
        issues.extend(_evidence_kind_issues(idx, evidence, kind_registry))
        if evidence.kind == "loop_record":
            issues.extend(_loop_record_issues(idx, evidence))
        # A row explicitly downgraded with a recorded supersession reason is
        # history, not rot: it no longer claims fresh proof, and the binding's
        # current proof is carried by its valid sibling row. Rows that are
        # stale while still claiming fresh: true keep erroring loudly.
        evidence_data = evidence.data or {}
        explicitly_superseded = evidence_data.get("fresh") is False and bool(
            evidence_data.get("superseded")
        )
        if (
            target_language_version >= 3
            and repo_root_path is not None
            and evidence.kind in FORMAL_KINDS
            and evidence.status in PASSING_EVIDENCE
            and not explicitly_superseded
        ):
            binding = _formal_evidence_binding(manifest, evidence)
            if binding is not None:
                reasons = _formal_binding_evidence_stale_reasons(
                    manifest,
                    binding,
                    evidence,
                    repo_root_path=repo_root_path,
                    context=context,
                )
                if reasons:
                    issues.append(
                        ValidationIssue(
                            "stale_formal_evidence",
                            (
                                f"formal evidence {evidence.id} is stale for "
                                f"{binding.id}: {', '.join(reasons)}"
                            ),
                            f"evidence[{idx}]",
                        )
                    )

    issues.extend(
        _acceptance_proof_issues(
            manifest=manifest,
            test_ids=test_ids,
            formal_ids=formal_ids,
            waiver_ids=waiver_ids,
            evidence_ids=set(evidence_by_id),
            kind_registry=kind_registry,
        )
    )
    issues.extend(_hierarchy_issues(manifest, task_ids))
    issues.extend(
        _integration_acceptance_issues(
            manifest,
            repo_root_path=repo_root_path,
            context=context,
        )
    )
    issues.extend(
        acceptance_arm_binding_issues(
            manifest,
            repo_root_path=repo_root_path,
            context=context,
        )
    )
    issues.extend(
        _slow_pytest_command_issues(
            manifest,
            repo_root_path=repo_root_path,
        )
    )
    issues.extend(_fast_path_policy_issues(manifest))

    # PACT v4 (standardize-pact-review-interface design.md §6): the edits
    # audit trail is a ledger event stream, not file content. v4 manifests
    # must not carry it; migrate-version --target 4 strips it.
    if target_language_version >= 4:
        if manifest.edits:
            issues.append(
                ValidationIssue(
                    "v4_edits_in_file",
                    "v4 manifests carry no edits ledger (events live in the "
                    "decision ledger); run okg pact migrate-version "
                    f"{manifest.change.id} --target 4",
                    "edits",
                )
            )
        if manifest.prose:
            issues.append(
                ValidationIssue(
                    "v4_prose_in_file",
                    "v4 manifests carry no prose block",
                    "prose",
                )
            )

    for idx, waiver in enumerate(manifest.waivers):
        if not waiver.requires_owner_approval:
            issues.append(
                ValidationIssue(
                    "invalid_waiver_owner_approval",
                    f"waiver {waiver.id} must require owner approval in v0",
                    f"waivers[{idx}].requires_owner_approval",
                )
            )

    alignment = derive_alignment(
        manifest,
        repo_root=repo_root_path,
        context=context,
    )
    for idx, task in enumerate(manifest.tasks):
        state = alignment["tasks"].get(task.id, {})
        if task.completion_satisfied and state.get("status") == "blocked":
            issues.append(
                ValidationIssue(
                    "blocked_task_completion_satisfied",
                    f"task {task.id} is blocked but marked completion-satisfied",
                    f"tasks.{task.id}.completion_satisfied",
                )
            )
        if task.status == "done" and state.get("missing"):
            issues.append(
                ValidationIssue(
                    "task_done_missing_evidence",
                    f"task {task.id} is done but missing required evidence",
                    f"tasks.{task.id}",
                )
            )
        else:
            issues.extend(_task_done_without_obligation_issues(manifest, task))
        if _target_language_version(manifest) >= 1:
            issues.extend(
                _task_evidence_ref_issues(
                    task_index=idx,
                    task=task,
                    manifest=manifest,
                    evidence_by_id=evidence_by_id,
                    repo_root_path=repo_root_path,
                    context=context,
                )
            )
    gate = lifecycle_gate or ("verification" if completion_gate else None)
    if gate:
        issues.extend(
            _lifecycle_gate_issues(
                manifest,
                alignment,
                gate,
                repo_root_path=repo_root_path,
                context=context,
            )
        )

    return ValidationReport(
        ok=not any(i.severity == "error" for i in issues),
        issues=issues,
        alignment=alignment,
    )


def _task_requirements_link_issues(index: int, task) -> list[ValidationIssue]:
    """The subject-deletion class (pact-vacuous-gate-close req.vacuous.presence).

    Every downstream completion check quantifies over ``task.requirements``, so
    an empty list deletes the subject and the task closes against nothing.
    Refused here, at the link, rather than left for the gate to discover it has
    nothing to check.
    """
    if task.requirements:
        return []
    return [
        ValidationIssue(
            "task_requirements_link",
            (
                f"task {task.id} links to no requirement, so closing it would "
                "check nothing; link the requirement(s) this task proves"
            ),
            f"tasks[{index}].requirements",
            offending_value=list(task.requirements),
        )
    ]


def _no_requirements_at_all_issues(manifest: PactManifest) -> list[ValidationIssue]:
    """PRESENCE, not quantification (pact-vacuous-gate-close req.vacuous.presence).

    The per-requirement policy loop is vacuously satisfied when there are no
    requirements at all, so a manifest that declares work (tasks) with nothing
    to prove reads as clean. A program umbrella that delegates every task to its
    children declares no tasks and is exempt: its contract is
    ``hierarchy.integration_acceptance``, carried by the children's manifests.
    """
    if not manifest.tasks or manifest.requirements:
        return []
    return [
        ValidationIssue(
            "missing_evidence_policy",
            (
                f"change {manifest.change.id} declares "
                f"{len(manifest.tasks)} task(s) but no requirements, so no "
                "evidence policy exists for any of them to satisfy"
            ),
            "requirements",
            offending_value=[],
        )
    ]


def evidence_ref_vocabulary(manifest: PactManifest) -> set[str]:
    """Every id an ``evidence.refs`` entry may legally name.

    THE definition, exported: `unknown_evidence_ref` is raised against this
    set, so a repair that offers a narrower vocabulary would send an author to
    change a ref the validator already accepts. The refusal's legal set and the
    check's legal set are now the same expression, not two lists that agree
    today (pact-self-repairing-refusals req.refusal.complete).
    """
    return (
        {manifest.change.id}
        | {r.id for r in manifest.requirements}
        | {a.id for r in manifest.requirements for a in r.acceptance}
        | {t.id for t in manifest.tasks}
        | {f.id for f in manifest.formal}
        | {t.id for t in manifest.test_plan}
    )


def _refless_evidence_issues(index: int, evidence) -> list[ValidationIssue]:
    """Subject deletion in an evidence row: it points at nothing.

    The ref loop that validates each ref is vacuously satisfied by an empty
    ``refs``, and a row that points at nothing proves nothing.
    """
    if evidence.refs:
        return []
    return [
        ValidationIssue(
            "unknown_evidence_ref",
            (
                f"evidence {evidence.id} references nothing (empty refs), "
                "so it can satisfy no requirement, acceptance criterion, "
                "or task"
            ),
            f"evidence[{index}].refs",
            offending_value=list(evidence.refs),
        )
    ]


def _task_done_without_obligation_issues(
    manifest: PactManifest,
    task,
) -> list[ValidationIssue]:
    """The compound subject-deletion case: done, and owing nothing.

    The exact mechanism of the 35-task incident. The alignment-driven arm reads
    ``state["missing"]``, which is derived from the task's linked requirements,
    so deleting the links empties it and a task that proved nothing passes. This
    arm needs no alignment — it asks whether an obligation EXISTS — which is
    what lets the corpus-hygiene lane evaluate it over the whole tree.
    """
    if task.status != "done" or task_proof_obligation(manifest, task):
        return []
    return [
        ValidationIssue(
            "task_done_missing_evidence",
            (
                f"task {task.id} is done but resolves to no proof "
                "obligation at all (no linked requirement carries an "
                "evidence policy), so nothing was checked"
            ),
            # The LINKS are the cause, so the path names them — and it keeps this
            # arm distinguishable from the alignment one, which reports a genuine
            # unmet obligation.
            f"tasks.{task.id}.requirements",
            offending_value=list(task.requirements),
        )
    ]


def _policy_kind_issues(
    index: int,
    req,
    kind_registry: EvidenceKindRegistry,
) -> list[ValidationIssue]:
    """A policy slot naming a kind outside the registry can never be filled.

    ERROR, not warning (pact-vacuous-gate-close): no evidence row resolves to
    an unregistered kind, so the requirement is unsatisfiable and ``ok: true``
    would be a lie.
    """
    issues: list[ValidationIssue] = []
    for token in (*req.evidence_policy.required, *req.evidence_policy.required_any):
        if token and not kind_registry.is_known(token):
            issues.append(
                ValidationIssue(
                    "unknown_evidence_kind",
                    (
                        f"requirement {req.id} evidence_policy names kind "
                        f"{token!r} outside the canonical registry; no evidence "
                        "row can ever resolve to it (register or alias the kind "
                        "in pact/project.yaml evidence_kinds)"
                    ),
                    f"requirements[{index}].evidence_policy",
                    offending_value=token,
                )
            )
    return issues


def _evidence_kind_issues(
    index: int,
    evidence,
    kind_registry: EvidenceKindRegistry,
) -> list[ValidationIssue]:
    """Registry drift: rogue kinds forked the corpus into 23 vocabularies.

    ERROR, not warning (pact-vacuous-gate-close): a kind outside the registry
    resolves to no policy slot, so the row is unusable as proof no matter what
    it claims — grandfathering it as a warning under ``ok: true`` was the
    steady state this change ends.
    """
    if not evidence.kind or kind_registry.is_known(evidence.kind):
        return []
    return [
        ValidationIssue(
            "unknown_evidence_kind",
            (
                f"evidence {evidence.id} has kind {evidence.kind!r} outside the "
                "canonical registry, so it resolves to no evidence policy slot "
                "(register or alias the kind in pact/project.yaml evidence_kinds)"
            ),
            f"evidence[{index}].kind",
            offending_value=evidence.kind,
        )
    ]


def structural_issues(
    manifest: PactManifest,
    *,
    repo_root: Path | str | None = None,
    context: PactEvaluationContext | None = None,
) -> list[ValidationIssue]:
    """The checks that need no alignment, no filesystem, and no ledger.

    The corpus-hygiene lane (`okg.substrate.pact.corpus_hygiene`) runs over
    every manifest in the tree, where `derive_alignment` costs two YAML dumps
    per manifest for the intent digest alone. It calls THESE FUNCTIONS — the
    same ones `validate_manifest` calls — rather than reimplementing the
    predicates, so the lane and the gate cannot drift apart.

    ALL FIVE arms `pact-vacuous-gate-close` owns are evaluated here, not just
    the two the lane BLOCKS on: a corpus-wide claim about the vacuity classes
    has to be measured over the corpus, and a class the lane never evaluates
    can only be reported as absent because nobody looked.
    """
    kind_registry = _kind_registry(
        Path(repo_root) if repo_root is not None else None,
        context=context,
    )
    issues: list[ValidationIssue] = []
    issues.extend(_no_requirements_at_all_issues(manifest))
    for idx, req in enumerate(manifest.requirements):
        issues.extend(_policy_kind_issues(idx, req, kind_registry))
    for idx, task in enumerate(manifest.tasks):
        issues.extend(_task_requirements_link_issues(idx, task))
        issues.extend(_task_done_without_obligation_issues(manifest, task))
    for idx, evidence in enumerate(manifest.evidence):
        issues.extend(_evidence_kind_issues(idx, evidence, kind_registry))
        issues.extend(_refless_evidence_issues(idx, evidence))
    return issues


def pact_hygiene_issues(
    manifest: PactManifest,
    *,
    raw_document: dict[str, Any] | None = None,
) -> list[ValidationIssue]:
    """Reject PACT-status/evidence rot the review queue cannot rank around.

    task.audit-2026-07.enforce-pact-hygiene (#699). Three banned patterns.

    v1 ships all three at ``severity="warning"`` (visible, non-blocking): the
    existing tree still carries ~97 done-but-proposal PACTs (Rule 1) and ~71
    genuinely-unevidenced done tasks (Rule 2). A ``severity="error"`` gate would
    red-wall ``okg pact gate post-edit`` on every one of those the moment it is
    edited -- including the operator-gated sweep that must clean them. They flip
    to ``severity="error"`` in a follow-up once a tree-sweep clears the backlog
    (see pact/changes/remediate-audit-2026-07/reports/pact-hygiene-sweep.md).

    1. ``change_status_stale_all_tasks_done`` -- every task is ``status: done``
       but ``change.status`` is still ``proposal``. Status no longer
       discriminates: the review queue ranks finished work as not-started.
    2. ``task_done_empty_evidence`` -- a ``status: done`` task with no passing
       evidence anywhere. A task is evidenced when its own ``task.evidence`` list
       is non-empty OR a passing change-level evidence record refs it -- the same
       predicate the completion gate uses (``_task_valid_evidence_refs`` +
       ``PASSING_EVIDENCE``). The canonical ``okg pact evidence attach`` verb
       writes change-level records (``manifest.evidence[].refs=[target]``) and
       never touches ``task.evidence``, so checking only ``task.evidence`` would
       false-flag ~86% of correctly-evidenced done tasks.
    3. ``task_missing_status`` -- a task omits the schema-required ``status``
       field. The parser silently backfills ``todo``, so this rule reads
       ``raw_document`` (the authored YAML) rather than the parsed manifest;
       absent the raw document it is dormant (the gate always threads it).
    """
    issues: list[ValidationIssue] = []

    # Rule 1: all-tasks-done-but-status-proposal.
    if (
        manifest.tasks
        and manifest.change.status == "proposal"
        and all(task.status == "done" for task in manifest.tasks)
    ):
        issues.append(
            ValidationIssue(
                "change_status_stale_all_tasks_done",
                (
                    f"change {manifest.change.id} has every task done but "
                    "change.status is still 'proposal'; advance it "
                    "(investigation/implementation/verification) or archive so the "
                    "review queue stops ranking finished work as not-started"
                ),
                "change.status",
                severity="warning",
            )
        )

    # Rule 2: done task with no passing evidence anywhere. A task is evidenced
    # when its own evidence list is non-empty OR a passing change-level evidence
    # record refs it in scope -- the predicate the completion gate uses
    # (_has_evidence / _task_valid_evidence_refs). Checking only task.evidence
    # false-flags every task evidenced via `okg pact evidence attach`, which
    # writes change-level records (refs=[target]), never task.evidence.
    for idx, task in enumerate(manifest.tasks):
        if task.status != "done":
            continue
        valid_refs = _task_valid_evidence_refs(manifest, task)
        evidenced = bool(task.evidence) or any(
            evidence.status in PASSING_EVIDENCE
            and bool(set(evidence.refs or ()) & valid_refs)
            for evidence in manifest.evidence
        )
        if not evidenced:
            issues.append(
                ValidationIssue(
                    "task_done_empty_evidence",
                    (
                        f"task {task.id} is done but no passing evidence refs it "
                        "(neither task.evidence nor a change-level evidence record); "
                        "attach the proving evidence or revert the status"
                    ),
                    f"tasks[{idx}].evidence",
                    severity="warning",
                )
            )

    # Rule 3: task missing the schema-required status field. Read the raw
    # document because model_validate defaults an absent status to 'todo'.
    if isinstance(raw_document, dict):
        raw_tasks = raw_document.get("tasks")
        if isinstance(raw_tasks, list):
            for idx, raw_task in enumerate(raw_tasks):
                if not isinstance(raw_task, dict):
                    continue
                if "status" not in raw_task:
                    task_id = raw_task.get("id") or f"tasks[{idx}]"
                    issues.append(
                        ValidationIssue(
                            "task_missing_status",
                            (
                                f"task {task_id} omits the required 'status' field; "
                                "the parser would silently default it to 'todo'. "
                                "Declare status explicitly"
                            ),
                            f"tasks[{idx}].status",
                            severity="warning",
                        )
                    )
    return issues


def _formal_artifact_issues(
    index: int,
    formal: Any,
    repo_root: Path,
    *,
    context: PactEvaluationContext | None = None,
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    issues.extend(
        _formal_file_issue(
            repo_root,
            formal.module,
            code="missing_formal_module",
            message=f"formal binding {formal.id} references missing module {formal.module}",
            path=f"formal[{index}].module",
            context=context,
        )
    )
    if formal.config:
        issues.extend(
            _formal_file_issue(
                repo_root,
                formal.config,
                code="missing_formal_config_file",
                message=(
                    f"formal binding {formal.id} references missing checker config "
                    f"{formal.config}"
                ),
                path=f"formal[{index}].config",
                context=context,
            )
        )
    if formal.checker_command:
        for script_ref in _checker_script_refs(formal.checker_command):
            issues.extend(
                _formal_file_issue(
                    repo_root,
                    script_ref,
                    code="missing_formal_checker_script",
                    message=(
                        f"formal binding {formal.id} references missing checker script "
                        f"{script_ref}"
                    ),
                    path=f"formal[{index}].checker_command",
                    context=context,
                )
            )
    for ref_index, refinement_ref in enumerate(formal.refinement_tests):
        issues.extend(
            _formal_refinement_issues(
                repo_root,
                refinement_ref,
                binding_id=formal.id,
                path=f"formal[{index}].refinement_tests[{ref_index}]",
                context=context,
            )
        )
    return issues


def _formal_file_issue(
    repo_root: Path,
    ref: str,
    *,
    code: str,
    message: str,
    path: str,
    context: PactEvaluationContext | None = None,
) -> list[ValidationIssue]:
    resolved = _resolve_repo_file(
        repo_root,
        ref,
        path,
        context=context,
    )
    if isinstance(resolved, ValidationIssue):
        return [resolved]
    lookup = context.file_lookup(repo_root, resolved) if context is not None else None
    if (lookup is not None and lookup.captured and not lookup.exists) or (
        (lookup is None or not lookup.captured) and not resolved.is_file()
    ):
        return [ValidationIssue(code, message, path)]
    return []


def _resolve_repo_file(
    repo_root: Path,
    ref: str,
    path: str,
    *,
    context: PactEvaluationContext | None = None,
) -> Path | ValidationIssue:
    candidate = Path(ref)
    if candidate.is_absolute():
        return ValidationIssue(
            "invalid_formal_artifact_path",
            f"formal artifact path must be repo-relative: {ref}",
            path,
        )
    if context is not None:
        root = context.resolve_repo_path(repo_root, ".")
        resolved = context.resolve_repo_path(repo_root, candidate)
    else:
        root = repo_root.resolve()
        resolved = (root / candidate).resolve()
    if resolved != root and root not in resolved.parents:
        return ValidationIssue(
            "invalid_formal_artifact_path",
            f"formal artifact path escapes the repo root: {ref}",
            path,
        )
    return resolved


def _checker_script_refs(command: str) -> list[str]:
    try:
        tokens = shlex.split(command)
    except ValueError:
        return []
    refs: list[str] = []
    for token in tokens:
        value = token
        if token.startswith("--") and "=" in token:
            value = token.split("=", 1)[1]
        elif token.startswith("-"):
            continue
        if value.endswith(".py") and ("/" in value or value.startswith(".")):
            refs.append(value)
    return refs


def _path_digest(
    path: Path,
    *,
    repo_root: Path | None = None,
    context: PactEvaluationContext | None = None,
) -> str | None:
    lookup = (
        context.file_lookup(repo_root or path.parent, path)
        if context is not None
        else None
    )
    if lookup is not None and lookup.captured:
        payload = lookup.content
    elif path.is_file():
        payload = path.read_bytes()
    else:
        return None
    if payload is None:
        return None
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _formal_refinement_issues(
    repo_root: Path,
    refinement_ref: str,
    *,
    binding_id: str,
    path: str,
    context: PactEvaluationContext | None = None,
) -> list[ValidationIssue]:
    file_ref, selectors = _split_pytest_ref(refinement_ref)
    if not file_ref:
        return [
            ValidationIssue(
                "invalid_formal_refinement",
                f"formal binding {binding_id} refinement test is not a pytest ref: "
                f"{refinement_ref}",
                path,
            )
        ]
    resolved = _resolve_repo_file(
        repo_root,
        file_ref,
        path,
        context=context,
    )
    if isinstance(resolved, ValidationIssue):
        return [resolved]
    lookup = context.file_lookup(repo_root, resolved) if context is not None else None
    exists = (
        lookup.exists if lookup is not None and lookup.captured else resolved.is_file()
    )
    if not exists:
        return [
            ValidationIssue(
                "missing_formal_refinement_file",
                f"formal binding {binding_id} references missing refinement test file "
                f"{file_ref}",
                path,
            )
        ]
    if selectors and not _pytest_selector_exists(
        resolved,
        selectors,
        repo_root=repo_root,
        context=context,
    ):
        return [
            ValidationIssue(
                "missing_formal_refinement_target",
                f"formal binding {binding_id} references missing refinement test target "
                f"{refinement_ref}",
                path,
            )
        ]
    return []


def _split_pytest_ref(ref: str) -> tuple[str | None, list[str]]:
    parts = ref.split("::")
    file_ref = parts[0].strip()
    if not file_ref.endswith(".py"):
        return None, []
    selectors = [
        _clean_pytest_selector(part)
        for part in parts[1:]
        if _clean_pytest_selector(part)
    ]
    return file_ref, selectors


def _clean_pytest_selector(selector: str) -> str:
    return selector.strip().split("[", 1)[0].split("(", 1)[0]


def _pytest_selector_exists(
    path: Path,
    selectors: list[str],
    *,
    repo_root: Path | None = None,
    context: PactEvaluationContext | None = None,
) -> bool:
    try:
        lookup = (
            context.file_lookup(repo_root or path.parent, path)
            if context is not None
            else None
        )
        if lookup is not None and lookup.captured:
            if lookup.content is None:
                return False
            text = lookup.content.decode("utf-8")
        else:
            text = path.read_text(encoding="utf-8")
        module = ast.parse(text)
    except (SyntaxError, UnicodeDecodeError):
        return False
    body: list[ast.stmt] = list(module.body)
    for index, selector in enumerate(selectors):
        node = next(
            (
                item
                for item in body
                if isinstance(
                    item, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
                )
                and item.name == selector
            ),
            None,
        )
        if node is None:
            return False
        if index == len(selectors) - 1:
            return True
        if not isinstance(node, ast.ClassDef):
            return False
        body = list(node.body)
    return bool(selectors)


def derive_alignment(
    manifest: PactManifest,
    *,
    repo_root: Path | str | None = None,
    context: PactEvaluationContext | None = None,
) -> dict[str, Any]:
    repo_root_path = Path(repo_root) if repo_root is not None else None
    alignment = _derive_alignment_base(
        manifest,
        repo_root_path=repo_root_path,
        context=context,
    )
    alignment["acceptance"] = _acceptance_alignment(
        manifest,
        repo_root_path=repo_root_path,
        context=context,
    )
    alignment["lifecycle"] = lifecycle_status(
        manifest,
        alignment,
        repo_root=repo_root_path,
        context=context,
    )
    return alignment


def _loop_record_issues(index: int, evidence) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    data = evidence.data or {}
    for field_name in sorted(LOOP_RECORD_REQUIRED_FIELDS):
        if not data.get(field_name):
            issues.append(
                ValidationIssue(
                    "incomplete_loop_record",
                    f"loop_record {evidence.id} is missing {field_name}",
                    f"evidence[{index}].data.{field_name}",
                )
            )
    ledger = data.get("friction_ledger")
    if not isinstance(ledger, list) or not ledger:
        return issues
    for entry_index, entry in enumerate(ledger):
        if not isinstance(entry, dict):
            issues.append(
                ValidationIssue(
                    "incomplete_loop_record",
                    f"loop_record {evidence.id} friction ledger entry is not a mapping",
                    f"evidence[{index}].data.friction_ledger[{entry_index}]",
                )
            )
            continue
        for field_name in sorted(FRICTION_LEDGER_REQUIRED_FIELDS):
            if not entry.get(field_name):
                issues.append(
                    ValidationIssue(
                        "incomplete_loop_record",
                        f"loop_record {evidence.id} friction ledger entry is missing {field_name}",
                        f"evidence[{index}].data.friction_ledger[{entry_index}].{field_name}",
                    )
                )
    return issues


def _acceptance_proof_issues(
    *,
    manifest: PactManifest,
    test_ids: set[str],
    formal_ids: set[str],
    waiver_ids: set[str],
    evidence_ids: set[str],
    kind_registry=DEFAULT_REGISTRY,
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    for req_index, req in enumerate(manifest.requirements):
        for acc_index, acceptance in enumerate(req.acceptance):
            for proof in acceptance_proof_tokens(acceptance):
                # Any registry-known kind (canonical or alias) is a valid
                # proof token; ACCEPTANCE_PROOF_TOKENS keeps the historical
                # extras that never became evidence kinds.
                if proof in ACCEPTANCE_PROOF_TOKENS or kind_registry.is_known(proof):
                    continue
                if proof in test_ids | formal_ids | waiver_ids | evidence_ids:
                    continue
                issues.append(
                    ValidationIssue(
                        "unknown_acceptance_evidence",
                        f"acceptance criterion {acceptance.id} references unknown proof {proof}",
                        f"requirements[{req_index}].acceptance[{acc_index}].evidence",
                        offending_value=proof,
                    )
                )
    return issues


def _hierarchy_issues(
    manifest: PactManifest,
    task_ids: set[str],
) -> list[ValidationIssue]:
    del task_ids
    hierarchy = manifest.hierarchy
    if hierarchy is None:
        return []
    issues: list[ValidationIssue] = []
    if hierarchy.parent == manifest.change.id:
        issues.append(
            ValidationIssue(
                "invalid_hierarchy_self_parent",
                "PACT hierarchy parent cannot be the same change",
                "hierarchy.parent",
            )
        )
    if manifest.change.id in hierarchy.children:
        issues.append(
            ValidationIssue(
                "invalid_hierarchy_self_child",
                "PACT hierarchy children cannot include the same change",
                "hierarchy.children",
            )
        )
    if hierarchy.role == "program" and not hierarchy.children:
        issues.append(
            ValidationIssue(
                "program_hierarchy_missing_children",
                "program PACT hierarchy should name child changes",
                "hierarchy.children",
                severity="warning",
            )
        )
    if hierarchy.role == "child" and not hierarchy.parent:
        issues.append(
            ValidationIssue(
                "child_hierarchy_missing_parent",
                "child PACT hierarchy should name its parent change",
                "hierarchy.parent",
                severity="warning",
            )
        )
    return issues


_REQUIREMENT_TOKEN_RE = re.compile(r"\breq\.[A-Za-z0-9][A-Za-z0-9_.:-]*")
_PYTEST_TARGET_RE = re.compile(
    r"(?P<path>(?:\.?/?)(?:tests|test)/[A-Za-z0-9_./-]+\.py)"
    r"(?:::(?P<node>[A-Za-z0-9_\[\]-]+(?:::[A-Za-z0-9_\[\]-]+)*))?"
)


def _clause_requirement_tokens(clause: str) -> list[str]:
    """Requirement ids named by one integration-acceptance clause.

    Structural: it extracts `req.*` ids, it does not read the sentence they
    sit in. Trailing sentence punctuation is stripped because a clause that
    ends "... satisfies req.foo.bar." names `req.foo.bar`, not `req.foo.bar.`.
    """
    return [
        token.rstrip(".,;:") for token in _REQUIREMENT_TOKEN_RE.findall(str(clause))
    ]


def _command_runs_slow(command: str) -> bool:
    """Whether a pytest command explicitly counteracts the repo default."""
    try:
        tokens = shlex.split(command)
    except ValueError:
        return False

    def excludes_slow(expression: str) -> bool:
        normalized = re.sub(r"[()]", " ", expression.lower())
        if not re.search(r"\bnot\s+slow\b", normalized):
            return False
        positive = re.sub(r"\bnot\s+slow\b", "", normalized)
        return not bool(re.search(r"\bslow\b", positive))

    marker_expression: str | None = None
    index = 0
    while index < len(tokens):
        token = tokens[index]
        override: str | None = None
        if token in {"-o", "--override-ini"} and index + 1 < len(tokens):
            override = tokens[index + 1]
            index += 1
        elif token.startswith("--override-ini="):
            override = token.partition("=")[2]
        elif token in {"-m", "--markexpr"} and index + 1 < len(tokens):
            marker_expression = tokens[index + 1]
            index += 1
        elif token.startswith(("-m=", "--markexpr=")):
            marker_expression = token.partition("=")[2]
        if override is not None and override.startswith("addopts="):
            replacement = override.partition("=")[2]
            try:
                replacement_tokens = shlex.split(replacement)
            except ValueError:
                return False
            replacement_marker: str | None = None
            for replacement_index, replacement_token in enumerate(replacement_tokens):
                if replacement_token in {"-m", "--markexpr"}:
                    if replacement_index + 1 < len(replacement_tokens):
                        replacement_marker = replacement_tokens[replacement_index + 1]
                elif replacement_token.startswith(("-m=", "--markexpr=")):
                    replacement_marker = replacement_token.partition("=")[2]
            return replacement_marker is None or not excludes_slow(replacement_marker)
        index += 1
    # Any explicit marker expression replaces (rather than combines with) the
    # configured `-m "not slow"`. It is safe only when it does not continue to
    # exclude every slow-marked test. Parentheses around `slow` are normalized
    # so `-m "not (slow)"` cannot evade the check.
    return marker_expression is not None and not excludes_slow(marker_expression)


def _repo_default_excludes_slow(repo_root_path: Path) -> bool:
    """Read the repository's pytest policy instead of assuming one globally."""
    try:
        raw = tomllib.loads(
            (repo_root_path / "pyproject.toml").read_text(encoding="utf-8")
        )
    except (OSError, tomllib.TOMLDecodeError):
        return False
    addopts = (
        ((raw.get("tool") or {}).get("pytest") or {}).get("ini_options") or {}
    ).get("addopts", "")
    try:
        tokens = (
            [str(value) for value in addopts]
            if isinstance(addopts, list)
            else shlex.split(str(addopts))
        )
    except ValueError:
        return False
    for index, token in enumerate(tokens):
        if token == "-m" and index + 1 < len(tokens):
            return bool(re.search(r"\bnot\s+slow\b", tokens[index + 1]))
        if token.startswith("-m="):
            return bool(re.search(r"\bnot\s+slow\b", token.partition("=")[2]))
    return False


_PYTEST_INVOCATION_RE = re.compile(
    r"\b(?:uv\s+run(?:\s+--extra\s+\S+)*\s+)?"
    r"(?:python\s+-m\s+)?pytest\b"
)


@lru_cache(maxsize=1024)
def _slow_nodes(path: str, mtime_ns: int) -> tuple[bool, frozenset[str]]:
    """Return (module-wide slow, directly slow test names)."""
    del mtime_ns
    source = Path(path).read_text(encoding="utf-8")
    module_wide = False
    names: set[str] = set()
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return module_wide, frozenset()
    for statement in tree.body:
        if not isinstance(statement, (ast.Assign, ast.AnnAssign)):
            continue
        targets = (
            statement.targets
            if isinstance(statement, ast.Assign)
            else [statement.target]
        )
        if not any(
            isinstance(target, ast.Name) and target.id == "pytestmark"
            for target in targets
        ):
            continue
        if "pytest.mark.slow" in ast.unparse(statement.value):
            module_wide = True
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue
        for decorator in node.decorator_list:
            if "pytest.mark.slow" in ast.unparse(decorator):
                names.add(node.name)
    return module_wide, frozenset(names)


def _slow_pytest_command_issues(
    manifest: PactManifest,
    *,
    repo_root_path: Path | None,
) -> list[ValidationIssue]:
    """Refuse canonical commands that silently deselect their named proof."""
    if repo_root_path is None or not _repo_default_excludes_slow(repo_root_path):
        return []
    fields = [
        (f"tasks[{index}].verification", task.verification)
        for index, task in enumerate(manifest.tasks)
    ] + [
        (f"test_plan[{index}].command", test.command or "")
        for index, test in enumerate(manifest.test_plan)
    ]
    issues: list[ValidationIssue] = []
    for field_path, command in fields:
        hazardous = _hazardous_slow_pytest_targets(command, repo_root_path)
        if hazardous:
            issues.append(
                ValidationIssue(
                    "pytest_command_deselects_named_slow_test",
                    (
                        f"{field_path} cites slow-marked pytest targets "
                        f"{sorted(set(hazardous))}, but project addopts excludes "
                        'slow tests. Add `-m "slow or not slow"` (or explicitly '
                        "clear addopts) so a green command cannot prove a test it "
                        "deselected"
                    ),
                    field_path,
                )
            )
    return issues


def _hazardous_slow_pytest_targets(command: str, repo_root_path: Path) -> list[str]:
    """Slow targets a command can silently deselect, per pytest invocation."""
    hazardous: list[str] = []
    invocations = list(_PYTEST_INVOCATION_RE.finditer(command))
    separators = list(re.finditer(r"(?:\r?\n|&&|\|\||;)", command))
    for invocation in invocations:
        start = max(
            (
                separator.end()
                for separator in separators
                if separator.end() <= invocation.start()
            ),
            default=0,
        )
        end = min(
            (
                separator.start()
                for separator in separators
                if separator.start() >= invocation.end()
            ),
            default=len(command),
        )
        segment = command[start:end].strip()
        try:
            leading = shlex.split(command[start : invocation.start()])
        except ValueError:
            continue
        leading = [
            token for token in leading if "=" not in token or token.startswith("-")
        ]
        if leading and leading[0].strip("`") not in {"env", "uv", "python", "python3"}:
            # `echo pytest ...`, quoted examples, and prose such as
            # `Run pytest ...` are not executable pytest invocations. A real
            # shell segment starts with pytest itself or a supported wrapper.
            continue
        executable = segment
        # Marker options begin after the pytest executable. In
        # `python -m pytest`, the interpreter's `-m pytest` is not a pytest
        # marker expression and must not be mistaken for an override.
        arguments = command[invocation.end() : end]
        if _command_runs_slow(arguments):
            continue
        for match in _PYTEST_TARGET_RE.finditer(executable):
            relative = match.group("path").lstrip("./")
            test_path = repo_root_path / relative
            try:
                module_wide, names = _slow_nodes(
                    str(test_path), test_path.stat().st_mtime_ns
                )
            except OSError:
                continue
            node_names = {
                selector.split("[")[0]
                for selector in (match.group("node") or "").split("::")
                if selector
            }
            if module_wide or bool(node_names & names) or (not node_names and names):
                hazardous.append(match.group(0))
    return hazardous


@lru_cache(maxsize=512)
def _cached_manifest_requirement_ids(
    path: str,
    mtime_ns: int,
) -> frozenset[str] | None:
    del mtime_ns  # cache key only: an edit to the child manifest invalidates
    try:
        raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    except (OSError, UnicodeError, TypeError, ValueError, yaml.YAMLError):
        # None, not an empty set: an unreadable child means "unresolved", and
        # reporting it as "this child declares no requirements" would turn a
        # read failure into a false unknown-requirement refusal.
        return None
    if not isinstance(raw, dict):
        return None
    return _requirement_ids_from_raw(raw)


def _requirement_ids_from_raw(raw: Any) -> frozenset[str]:
    if not isinstance(raw, dict):
        return frozenset()
    return frozenset(
        str(row["id"])
        for row in (raw.get("requirements") or [])
        if isinstance(row, dict) and row.get("id")
    )


def _child_requirement_ids(
    child_id: str,
    *,
    repo_root_path: Path | None,
    context: PactEvaluationContext | None,
) -> frozenset[str] | None:
    """Requirement ids declared by a child PACT, or None when unresolvable.

    None is NOT "no requirements": it means this process could not read the
    child manifest, and the caller must say so out loud rather than treat an
    unreadable child as agreement that the clause names nothing real.
    """
    relative_paths = (
        f"pact/changes/{child_id}/pact.yaml",
        f"pact/archive/{child_id}/pact.yaml",
    )
    if context is not None:
        root = repo_root_path or Path(".")
        for relative in relative_paths:
            lookup = context.manifest_file_lookup(root, relative)
            if not lookup.captured:
                return None
            if lookup.content is not None:
                try:
                    raw = yaml.safe_load(lookup.content)
                except (UnicodeError, TypeError, ValueError, yaml.YAMLError):
                    return None
                if not isinstance(raw, dict):
                    return None
                return _requirement_ids_from_raw(raw)
        # Captured authority says every candidate path is absent.
        return None
    if repo_root_path is None:
        return None
    for relative in relative_paths:
        path = repo_root_path / relative
        try:
            mtime_ns = path.stat().st_mtime_ns
        except OSError:
            continue
        return _cached_manifest_requirement_ids(str(path), mtime_ns)
    return None


def _child_manifest_raw(
    child_id: str,
    *,
    repo_root_path: Path | None,
    context: PactEvaluationContext | None,
) -> dict[str, Any] | None:
    """Read one child document without falling through a captured authority.

    A captured child that is malformed is unresolved, not an empty contract and
    not permission to reopen the live checkout.  The native hierarchical
    validator uses the raw mapping so it can check the metadata contract before
    Pydantic's ordinary manifest validation has erased unknown fields.
    """
    relative_paths = (
        f"pact/changes/{child_id}/pact.yaml",
        f"pact/archive/{child_id}/pact.yaml",
    )
    if context is not None:
        root = repo_root_path or Path(".")
        for relative in relative_paths:
            lookup = context.manifest_file_lookup(root, relative)
            if not lookup.captured:
                return None
            if lookup.content is None:
                continue
            try:
                raw = yaml.safe_load(lookup.content)
            except (UnicodeError, TypeError, ValueError, yaml.YAMLError):
                return None
            return raw if isinstance(raw, dict) else None
        return None
    if repo_root_path is None:
        return None
    for relative in relative_paths:
        path = repo_root_path / relative
        if not path.is_file():
            continue
        try:
            raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, TypeError, ValueError, yaml.YAMLError):
            return None
        return raw if isinstance(raw, dict) else None
    return None


_OUTPUT_ID_RE = re.compile(r"^output\.[A-Za-z0-9][A-Za-z0-9_.:-]*$")


def _hierarchical_program_contract_issues(
    manifest: PactManifest,
    *,
    repo_root_path: Path | None,
    context: PactEvaluationContext | None,
    severity: str,
) -> list[ValidationIssue]:
    """Validate the generic contract between a program and its child PACTs.

    The ordinary integration-acceptance rule checks that clause tokens resolve
    to requirements.  A hierarchical program also needs a machine-readable
    ownership and handoff contract, otherwise a clause can name a child while
    the child silently changes its outcome slice or its producer/consumer
    boundary.  This validator is deliberately driven only by the declared
    metadata shape; it contains no deployment-specific IDs.
    """
    hierarchy = manifest.hierarchy
    if hierarchy is None or hierarchy.role != "program":
        return []
    metadata = manifest.change.method_metadata
    declares_program_contract = (
        "program_outcomes" in metadata or "program_dependencies" in metadata
    )
    if not declares_program_contract:
        # A program cannot disable an adopted child contract by deleting both
        # parent inventories. Programs that predate this optional contract keep
        # their legacy validation behavior; once any declared child carries a
        # parent_contract, the umbrella must carry the matching inventories.
        for child_id in hierarchy.children:
            raw = _child_manifest_raw(
                child_id,
                repo_root_path=repo_root_path,
                context=context,
            )
            change = raw.get("change") if isinstance(raw, dict) else None
            child_metadata = (
                change.get("method_metadata")
                if isinstance(change, dict)
                else None
            )
            if (
                isinstance(child_metadata, dict)
                and "parent_contract" in child_metadata
            ):
                declares_program_contract = True
                break
    if not declares_program_contract:
        return []

    issues: list[ValidationIssue] = []

    def add(code: str, message: str, path: str) -> None:
        issues.append(ValidationIssue(code, message, path, severity=severity))

    if metadata.get("pact_shape") != "hierarchical_umbrella":
        add(
            "integration_acceptance_program_pact_shape_invalid",
            "program contract metadata must declare pact_shape hierarchical_umbrella",
            "change.method_metadata.pact_shape",
        )
    if manifest.change.tier != 3:
        add(
            "integration_acceptance_program_tier_invalid",
            "program contract must declare lifecycle tier 3",
            "change.tier",
        )

    outcomes_value = metadata.get("program_outcomes")
    program_outcome_ids: set[str] = set()
    if not isinstance(outcomes_value, list) or not outcomes_value:
        add(
            "integration_acceptance_program_outcomes_invalid",
            "program_outcomes must be a non-empty list of unique mappings with id, title, and statement",
            "change.method_metadata.program_outcomes",
        )
    else:
        for index, outcome in enumerate(outcomes_value):
            path = f"change.method_metadata.program_outcomes[{index}]"
            if not isinstance(outcome, dict):
                add(
                    "integration_acceptance_program_outcome_malformed",
                    "program outcome must be a mapping with a non-empty id, title, and statement",
                    path,
                )
                continue
            outcome_id = outcome.get("id")
            if (
                not isinstance(outcome_id, str)
                or not outcome_id
                or not isinstance(outcome.get("title"), str)
                or not outcome.get("title")
                or not isinstance(outcome.get("statement"), str)
                or not outcome.get("statement")
            ):
                add(
                    "integration_acceptance_program_outcome_malformed",
                    "program outcome must have non-empty string id, title, and statement",
                    path,
                )
                continue
            if outcome_id in program_outcome_ids:
                add(
                    "integration_acceptance_program_outcome_duplicate",
                    f"program outcome {outcome_id} is declared more than once",
                    f"{path}.id",
                )
            program_outcome_ids.add(outcome_id)

    child_ids = list(hierarchy.children)
    child_id_set = set(child_ids)
    outputs_by_child: dict[str, set[str]] = {}
    output_owners: dict[str, list[str]] = {}
    consumes_by_child: dict[str, list[tuple[str, str, int]]] = {}
    handoffs_by_child: dict[str, list[tuple[str, str, int]]] = {}
    outcome_owners: dict[str, list[str]] = {}

    def parse_output_ids(child_id: str, contract: dict[str, Any]) -> set[str]:
        field_path = (
            f"pact/changes/{child_id}/pact.yaml:"
            "change.method_metadata.parent_contract.produces"
        )
        produces = contract.get("produces")
        if not isinstance(produces, list) or not produces:
            add(
                "integration_acceptance_produced_outputs_invalid",
                "parent_contract.produces must be a non-empty list of mappings with unique output.* ids",
                field_path,
            )
            return set()
        output_ids: set[str] = set()
        for index, item in enumerate(produces):
            path = f"{field_path}[{index}]"
            if not isinstance(item, dict):
                add(
                    "integration_acceptance_produced_output_malformed",
                    "produced output must be a mapping with an output.* id and non-empty description",
                    path,
                )
                continue
            output_id = item.get("id")
            if (
                not isinstance(output_id, str)
                or not _OUTPUT_ID_RE.fullmatch(output_id)
                or not isinstance(item.get("description"), str)
                or not item.get("description")
            ):
                add(
                    "integration_acceptance_produced_output_malformed",
                    "produced output needs a stable output.* id and non-empty description",
                    path,
                )
                continue
            if output_id in output_ids:
                add(
                    "integration_acceptance_produced_output_duplicate",
                    f"child {child_id} declares produced output {output_id} more than once",
                    f"{path}.id",
                )
            output_ids.add(output_id)
            output_owners.setdefault(output_id, []).append(child_id)
        return output_ids

    def parse_edges(
        child_id: str,
        contract: dict[str, Any],
        field: str,
        peer_key: str,
    ) -> list[tuple[str, str, int]]:
        field_path = (
            f"pact/changes/{child_id}/pact.yaml:"
            f"change.method_metadata.parent_contract.{field}"
        )
        value = contract.get(field)
        if not isinstance(value, list):
            add(
                "integration_acceptance_parent_contract_edge_malformed",
                f"parent_contract.{field} must be a list of mappings",
                field_path,
            )
            return []
        edges: list[tuple[str, str, int]] = []
        seen: set[tuple[str, str]] = set()
        for index, item in enumerate(value):
            path = f"{field_path}[{index}]"
            if not isinstance(item, dict):
                add(
                    "integration_acceptance_parent_contract_edge_malformed",
                    f"{field}[{index}] must be a mapping with {peer_key} and output",
                    path,
                )
                continue
            peer = item.get(peer_key)
            output_id = item.get("output")
            if (
                not isinstance(peer, str)
                or peer not in child_id_set
                or peer == child_id
                or not isinstance(output_id, str)
                or not _OUTPUT_ID_RE.fullmatch(output_id)
            ):
                add(
                    "integration_acceptance_parent_contract_edge_malformed",
                    f"{field}[{index}] must name another declared child and a stable output.* id",
                    path,
                )
                continue
            edge = (peer, output_id)
            if edge in seen:
                add(
                    "integration_acceptance_parent_contract_edge_duplicate",
                    f"{field}[{index}] duplicates edge {peer} / {output_id}",
                    path,
                )
            seen.add(edge)
            edges.append((peer, output_id, index))
        return edges

    for child_id in child_ids:
        raw = _child_manifest_raw(
            child_id,
            repo_root_path=repo_root_path,
            context=context,
        )
        if raw is None:
            # The ordinary child reader emits the unresolved-child refusal. Do
            # not manufacture a second metadata error from an unreadable file.
            continue
        child_path = f"pact/changes/{child_id}/pact.yaml"
        change = raw.get("change")
        if not isinstance(change, dict):
            add(
                "integration_acceptance_child_contract_invalid",
                f"declared child {child_id} has no mapping-valued change block",
                f"{child_path}:change",
            )
            continue
        child_metadata = change.get("method_metadata")
        if not isinstance(child_metadata, dict):
            add(
                "integration_acceptance_child_contract_invalid",
                f"declared child {child_id} has no mapping-valued change.method_metadata",
                f"{child_path}:change.method_metadata",
            )
            continue
        if child_metadata.get("pact_shape") != "hierarchical_child":
            add(
                "integration_acceptance_child_pact_shape_invalid",
                f"declared child {child_id} must declare pact_shape hierarchical_child",
                f"{child_path}:change.method_metadata.pact_shape",
            )
        if change.get("tier") != 2:
            add(
                "integration_acceptance_child_tier_invalid",
                f"declared child {child_id} must declare lifecycle tier 2",
                f"{child_path}:change.tier",
            )
        contract = child_metadata.get("parent_contract")
        if not isinstance(contract, dict):
            add(
                "integration_acceptance_child_contract_invalid",
                f"declared child {child_id} must declare a mapping-valued parent_contract",
                f"{child_path}:change.method_metadata.parent_contract",
            )
            continue
        if contract.get("program_workstream") != child_id:
            add(
                "integration_acceptance_program_workstream_mismatch",
                f"parent_contract.program_workstream must equal declared child {child_id}",
                f"{child_path}:change.method_metadata.parent_contract.program_workstream",
            )

        slice_value = contract.get("program_outcomes")
        if not isinstance(slice_value, list) or not slice_value:
            add(
                "integration_acceptance_outcome_slice_missing",
                f"child {child_id} must declare a non-empty program_outcomes slice",
                f"{child_path}:change.method_metadata.parent_contract.program_outcomes",
            )
        else:
            seen_slice: set[str] = set()
            for index, outcome_id in enumerate(slice_value):
                path = (
                    f"{child_path}:change.method_metadata.parent_contract."
                    f"program_outcomes[{index}]"
                )
                if not isinstance(outcome_id, str) or not outcome_id:
                    add(
                        "integration_acceptance_outcome_slice_malformed",
                        f"child {child_id} outcome slice entries must be non-empty strings",
                        path,
                    )
                    continue
                if outcome_id not in program_outcome_ids:
                    add(
                        "integration_acceptance_outcome_slice_outside_program",
                        f"child {child_id} names outcome {outcome_id} outside program_outcomes",
                        path,
                    )
                    continue
                if outcome_id in seen_slice:
                    add(
                        "integration_acceptance_outcome_slice_duplicate",
                        f"child {child_id} repeats outcome {outcome_id} in its slice",
                        path,
                    )
                seen_slice.add(outcome_id)
                outcome_owners.setdefault(outcome_id, []).append(child_id)
        if contract.get("integration_owner") is not True:
            add(
                "integration_acceptance_integration_owner_required",
                f"child {child_id} parent_contract.integration_owner must be true",
                f"{child_path}:change.method_metadata.parent_contract.integration_owner",
            )

        outputs_by_child[child_id] = parse_output_ids(child_id, contract)
        consumes_by_child[child_id] = parse_edges(
            child_id,
            contract,
            "consumes",
            "child",
        )
        handoffs_by_child[child_id] = parse_edges(
            child_id,
            contract,
            "handoffs",
            "to",
        )

    for output_id, owners in output_owners.items():
        if len(owners) > 1:
            add(
                "integration_acceptance_produced_output_duplicate",
                f"produced output {output_id} is owned by multiple children {sorted(set(owners))}",
                "change.method_metadata.program_dependencies",
            )

    for outcome_id in sorted(program_outcome_ids):
        owners = outcome_owners.get(outcome_id, [])
        if len(owners) != 1:
            add(
                "integration_acceptance_outcome_ownership_invalid",
                f"program outcome {outcome_id} must be owned by exactly one child; got {owners}",
                "change.method_metadata.program_outcomes",
            )

    for consumer, consumes in consumes_by_child.items():
        for producer, output_id, index in consumes:
            if output_id not in outputs_by_child.get(producer, set()):
                add(
                    "integration_acceptance_output_reference_unknown",
                    f"child {consumer} consumes undeclared output {output_id} from {producer}",
                    f"pact/changes/{consumer}/pact.yaml:change.method_metadata.parent_contract.consumes[{index}]",
                )
    for producer, handoffs in handoffs_by_child.items():
        for consumer, output_id, index in handoffs:
            if output_id not in outputs_by_child.get(producer, set()):
                add(
                    "integration_acceptance_output_reference_unknown",
                    f"child {producer} hands off undeclared output {output_id}",
                    f"pact/changes/{producer}/pact.yaml:change.method_metadata.parent_contract.handoffs[{index}]",
                )

    handoff_edges = {
        (producer, consumer, output_id)
        for producer, handoffs in handoffs_by_child.items()
        for consumer, output_id, _index in handoffs
    }
    consume_edges = {
        (producer, consumer, output_id)
        for consumer, consumes in consumes_by_child.items()
        for producer, output_id, _index in consumes
    }
    for producer, consumer, output_id in sorted(handoff_edges - consume_edges):
        add(
            "integration_acceptance_handoff_consume_asymmetry",
            f"producer {producer} hands off {output_id} to {consumer}, but the consumer does not consume that exact output",
            f"pact/changes/{producer}/pact.yaml:change.method_metadata.parent_contract.handoffs",
        )
    for producer, consumer, output_id in sorted(consume_edges - handoff_edges):
        add(
            "integration_acceptance_handoff_consume_asymmetry",
            f"consumer {consumer} consumes {output_id} from {producer}, but the producer does not hand off that exact output",
            f"pact/changes/{consumer}/pact.yaml:change.method_metadata.parent_contract.consumes",
        )
    actual_edges = {
        (producer, consumer)
        for producer, consumer, _output_id in handoff_edges & consume_edges
    }

    dependencies_value = metadata.get("program_dependencies")
    declared_edges: set[tuple[str, str]] = set()
    declared_edge_indices: dict[tuple[str, str], int] = {}
    if not isinstance(dependencies_value, list):
        add(
            "integration_acceptance_program_dependencies_invalid",
            "program_dependencies must be a list of from/to/kind mappings",
            "change.method_metadata.program_dependencies",
        )
    else:
        for index, dependency in enumerate(dependencies_value):
            path = f"change.method_metadata.program_dependencies[{index}]"
            if not isinstance(dependency, dict):
                add(
                    "integration_acceptance_program_dependency_malformed",
                    "program dependency must be a mapping with declared child from/to and kind blocks",
                    path,
                )
                continue
            source = dependency.get("from")
            target = dependency.get("to")
            if (
                not isinstance(source, str)
                or source not in child_id_set
                or not isinstance(target, str)
                or target not in child_id_set
                or source == target
                or dependency.get("kind") != "blocks"
            ):
                add(
                    "integration_acceptance_program_dependency_malformed",
                    "program dependency must use distinct declared child IDs and kind blocks",
                    path,
                )
                continue
            edge = (source, target)
            if edge in declared_edges:
                add(
                    "integration_acceptance_program_dependency_duplicate",
                    f"program dependency {source} -> {target} is declared more than once",
                    path,
                )
            declared_edges.add(edge)
            declared_edge_indices.setdefault(edge, index)

    missing_edges = sorted(actual_edges - declared_edges)
    extra_edges = sorted(declared_edges - actual_edges)
    if missing_edges or extra_edges:
        add(
            "integration_acceptance_program_dependency_mismatch",
            f"program_dependencies must exactly equal paired child handoffs/consumes; missing={missing_edges}, extra={extra_edges}",
            "change.method_metadata.program_dependencies",
        )
        for source, target in extra_edges:
            index = declared_edge_indices[(source, target)]
            add(
                "integration_acceptance_program_dependency_extra",
                f"program dependency {source} -> {target} has no paired child handoff/consume edge",
                f"change.method_metadata.program_dependencies[{index}]",
            )
    return issues


def _integration_acceptance_issues(
    manifest: PactManifest,
    *,
    repo_root_path: Path | None = None,
    context: PactEvaluationContext | None = None,
    severity: str = "warning",
) -> list[ValidationIssue]:
    """Make `hierarchy.integration_acceptance` checkable.

    Per `pact-acceptance-prose-is-enforceable`
    req.pact-acceptance-prose-is-enforceable.integration-acceptance-validated-or-deleted,
    the CHECKABLE route was taken over deletion (see this change's
    reports/integration-acceptance-route.md for why). The field was declared
    at models.py, echoed into the view payload at cli.py, and listed in the
    schema, with no validation rule anywhere: a program-level invariant such
    as "no silent fallback anywhere in the program" was prose no gate could
    fail.

    A clause must now name an owning `req.*` that exists in this manifest or
    in one of its declared children. Three refusals, none of which reads the
    clause's prose:

    * `integration_acceptance_clause_unowned` — the clause names no `req.*`.
    * `integration_acceptance_clause_unknown_requirement` — it names ids and
      none of them exists, so presence is not mistaken for existence.
    * `integration_acceptance_owner_unresolved` — it names a non-local id and
      the child manifest that would own it could not be read. Reported rather
      than passed: an unreadable child is not agreement.
    * `integration_acceptance_ambiguous_owner` — one clause spans more than one
      child/program owner, so no single PACT is accountable for closing it.
    * `integration_acceptance_child_contract_invalid` — a declared child does
      not identify itself as that child under this program.
    * `integration_acceptance_child_unverified` — a declared child has not
      passed its complete verification validation. Requirement evidence alone
      cannot hide structural errors, incomplete child tasks, or a missing
      implementation decision.

    `severity="error"` is applied by the verification gate, and only for
    programs, matching the requirement's "blocking at the verification bar
    for programs".
    """
    hierarchy = manifest.hierarchy
    if hierarchy is None:
        return []
    if severity == "error" and hierarchy.role != "program":
        return []
    issues: list[ValidationIssue] = _hierarchical_program_contract_issues(
        manifest,
        repo_root_path=repo_root_path,
        context=context,
        severity=severity,
    )
    if not hierarchy.integration_acceptance:
        return issues
    local_alignment = _derive_alignment_base(
        manifest,
        repo_root_path=repo_root_path,
        context=context,
    )["requirements"]
    owned = set(req.id for req in manifest.requirements)
    requirement_owners: dict[str, set[str]] = {
        req.id: {manifest.change.id} for req in manifest.requirements
    }
    owner_status = {
        req_id: state.get("satisfaction_status")
        for req_id, state in local_alignment.items()
    }
    unresolved_children: list[str] = []
    unverified_children: list[str] = []
    invalid_children: list[tuple[str, str]] = []
    for child_id in hierarchy.children:
        child_ids = _child_requirement_ids(
            child_id,
            repo_root_path=repo_root_path,
            context=context,
        )
        if child_ids is None:
            unresolved_children.append(child_id)
            continue
        owned |= child_ids
        for req_id in child_ids:
            requirement_owners.setdefault(req_id, set()).add(child_id)
        child_rollup = _child_requirement_rollup(
            child_id,
            repo_root_path=repo_root_path,
            context=context,
            verify_child=severity == "error",
        )
        if child_rollup is None:
            unresolved_children.append(child_id)
        else:
            child, child_status, child_verified = child_rollup
            owner_status.update(child_status)
            if child.change.id != child_id:
                invalid_children.append(
                    (child_id, f"change.id is {child.change.id!r}")
                )
            child_hierarchy = child.hierarchy
            if (
                child_hierarchy is None
                or child_hierarchy.role != "child"
                or child_hierarchy.parent != manifest.change.id
            ):
                invalid_children.append(
                    (
                        child_id,
                        "hierarchy must declare role child and parent "
                        f"{manifest.change.id}",
                    )
                )
            if severity == "error" and not child_verified:
                unverified_children.append(child_id)
    for child_id, reason in invalid_children:
        issues.append(
            ValidationIssue(
                "integration_acceptance_child_contract_invalid",
                f"declared child PACT {child_id} is invalid: {reason}",
                f"hierarchy.children[{hierarchy.children.index(child_id)}]",
                severity=severity,
            )
        )
    for child_id in unverified_children:
        issues.append(
            ValidationIssue(
                "integration_acceptance_child_unverified",
                (
                    f"child PACT {child_id} has not passed its verification "
                    "lifecycle gate; program completion requires both child "
                    "requirement proof and completed child tasks"
                ),
                f"hierarchy.children[{hierarchy.children.index(child_id)}]",
                severity=severity,
            )
        )
    for index, clause in enumerate(hierarchy.integration_acceptance):
        path = f"hierarchy.integration_acceptance[{index}]"
        tokens = _clause_requirement_tokens(clause)
        excerpt = " ".join(str(clause).split())[:80]
        if not tokens:
            issues.append(
                ValidationIssue(
                    "integration_acceptance_clause_unowned",
                    (
                        f"integration_acceptance clause {excerpt!r} names no owning "
                        "req.* id, so no gate can fail it. Name the requirement in "
                        "this program or in one of its children that carries the "
                        "invariant, or move the clause into a requirement"
                    ),
                    path,
                    severity=severity,
                )
            )
            continue
        clause_owners = {
            owner
            for token in set(tokens)
            for owner in requirement_owners.get(token, set())
        }
        if len(clause_owners) > 1:
            issues.append(
                ValidationIssue(
                    "integration_acceptance_ambiguous_owner",
                    (
                        f"integration_acceptance clause {excerpt!r} spans owners "
                        f"{sorted(clause_owners)}; one child or the program must "
                        "own each integration obligation"
                    ),
                    path,
                    severity=severity,
                )
            )
        unknown = sorted(set(tokens) - owned)
        if unknown and unresolved_children:
            issues.append(
                ValidationIssue(
                    "integration_acceptance_owner_unresolved",
                    (
                        f"integration_acceptance clause {excerpt!r} has possible "
                        f"owners {unknown} that are not declared locally, while "
                        f"child manifests {sorted(unresolved_children)} could not be "
                        "read to resolve them; every named owner must resolve"
                    ),
                    path,
                    severity=severity,
                )
            )
        elif unknown:
            issues.append(
                ValidationIssue(
                    "integration_acceptance_clause_unknown_requirement",
                    (
                        f"integration_acceptance clause {excerpt!r} names unknown "
                        f"requirements {unknown}; every named owner must resolve, "
                        "not merely one of them"
                    ),
                    path,
                    severity=severity,
                )
            )
        if severity == "error":
            unproven = sorted(
                token
                for token in set(tokens) & owned
                if owner_status.get(token) not in {"satisfied", "waived"}
            )
            if unproven:
                issues.append(
                    ValidationIssue(
                        "integration_acceptance_owner_unproven",
                        (
                            f"integration_acceptance clause {excerpt!r} is owned "
                            f"by unproven requirements {unproven}; program completion "
                            "requires every named owner to be satisfied"
                        ),
                        path,
                        severity=severity,
                    )
                )
    return issues


def _child_requirement_rollup(
    child_id: str,
    *,
    repo_root_path: Path | None,
    context: PactEvaluationContext | None,
    verify_child: bool,
) -> tuple[PactManifest, dict[str, str], bool] | None:
    """Read one child's status, verifying its lifecycle only at a blocking gate."""
    raw = _child_manifest_raw(
        child_id,
        repo_root_path=repo_root_path,
        context=context,
    )
    if raw is None:
        return None
    try:
        child = PactManifest.model_validate(raw)
    except Exception:  # malformed child is unresolved, never agreement
        return None
    alignment = _derive_alignment_base(
        child,
        repo_root_path=repo_root_path,
        context=context,
    )
    requirement_status = {
        req_id: str(state.get("satisfaction_status"))
        for req_id, state in alignment["requirements"].items()
    }
    child_verified = not verify_child or validate_manifest(
        child,
        repo_root=repo_root_path,
        context=context,
        lifecycle_gate="verification",
    ).ok
    return child, requirement_status, bool(child_verified)


def _fast_path_policy_issues(manifest: PactManifest) -> list[ValidationIssue]:
    if _target_language_version(manifest) < 3:
        return []
    shape_path = "prose.pact_shape"
    pact_shape = manifest.prose.get("pact_shape")
    if manifest.pact_version >= 4:
        shape_path = "change.method_metadata.pact_shape"
        pact_shape = manifest.change.method_metadata.get("pact_shape")
    if pact_shape != "fast_path":
        return []
    reasons: list[str] = []
    if len(manifest.requirements) > 1:
        reasons.append("multiple requirements")
    if len(manifest.tasks) > 1:
        reasons.append("multiple tasks")
    if manifest.hierarchy and (
        manifest.hierarchy.parent or manifest.hierarchy.children
    ):
        reasons.append("hierarchy")
    if manifest.formal:
        reasons.append("formal bindings")
    required_evidence = {
        kind for req in manifest.requirements for kind in req.evidence_policy.required
    }
    if required_evidence & {"graph_report", "published_generation"}:
        reasons.append("graph evidence")
    risky_areas = {
        "formal-verification",
        "qa-gates",
        "security",
        "architecture",
        "okg-workspace",
    }
    if set(manifest.change.affected_areas) & risky_areas:
        reasons.append("high-risk affected area")
    if len(manifest.change.affected_code) > 3:
        reasons.append("broad affected code")
    if not reasons:
        return []
    return [
        ValidationIssue(
            "fast_path_graduation_recommended",
            (
                "fast-path PACT should graduate to full shape: "
                + ", ".join(sorted(set(reasons)))
                + ". Graduate "
                + manifest.change.id
                + " by editing pact.yaml directly (expand requirements / tasks / test_plan)"
            ),
            shape_path,
            severity="warning",
        )
    ]


def _guidance_review_issues(manifest: PactManifest) -> list[ValidationIssue]:
    if not _touches_guidance_review_trigger(manifest):
        return []
    if (
        _declares_guidance_surface_update(manifest)
        or _has_guidance_review_evidence(manifest)
        or _has_guidance_review_waiver(manifest)
    ):
        return []
    touched = (
        manifest.change.affected_code
        or manifest.change.affected_areas
        or ["operator-facing surface"]
    )
    return [
        ValidationIssue(
            "verification_gate_missing_guidance_review",
            (
                "operator-facing changes must declare a docs/agent-guidance "
                "update, passing guidance-review evidence, or an owner-approved "
                f"guidance-review waiver; touched {', '.join(touched[:3])}"
            ),
            "change.affected_code",
        )
    ]


def _touches_guidance_review_trigger(manifest: PactManifest) -> bool:
    areas = {_normalise_token(area) for area in manifest.change.affected_areas}
    if areas & GUIDANCE_REVIEW_TRIGGER_AREAS:
        return True
    return any(
        _is_guidance_review_trigger_path(path) for path in manifest.change.affected_code
    )


def _declares_guidance_surface_update(manifest: PactManifest) -> bool:
    areas = {_normalise_token(area) for area in manifest.change.affected_areas}
    if areas & GUIDANCE_SURFACE_AREAS:
        return True
    return any(
        _is_guidance_surface_path(path) for path in manifest.change.affected_code
    )


def _has_guidance_review_evidence(manifest: PactManifest) -> bool:
    for evidence in manifest.evidence:
        if evidence.status not in PASSING_EVIDENCE:
            continue
        haystack = _normalise_free_text(
            " ".join(
                [
                    evidence.id,
                    evidence.kind,
                    evidence.artifact or "",
                    evidence.command or "",
                    evidence.summary or "",
                    " ".join(evidence.refs),
                ]
            )
        )
        if any(token in haystack for token in GUIDANCE_REVIEW_EVIDENCE_TOKENS):
            return True
    return False


def _has_guidance_review_waiver(manifest: PactManifest) -> bool:
    for waiver in manifest.waivers:
        if not waiver.owner:
            continue
        haystack = _normalise_free_text(f"{waiver.id} {waiver.scope}")
        if any(token in haystack for token in GUIDANCE_REVIEW_WAIVER_TOKENS):
            return True
    return False


def _is_guidance_review_trigger_path(value: str) -> bool:
    path = _normalise_path(value)
    if _is_agents_file(path):
        return True
    if (
        "/prompts/" in path
        or path.endswith("/prompts.py")
        or path.endswith("prompts.py")
    ):
        return True
    return any(
        _path_matches_prefix(path, prefix)
        for prefix in GUIDANCE_REVIEW_TRIGGER_PATH_PREFIXES
    )


def _is_guidance_surface_path(value: str) -> bool:
    path = _normalise_path(value)
    if _is_agents_file(path):
        return True
    if (
        "/prompts/" in path
        or path.endswith("/prompts.py")
        or path.endswith("prompts.py")
    ):
        return True
    return any(
        _path_matches_prefix(path, prefix) for prefix in GUIDANCE_SURFACE_PATH_PREFIXES
    )


def _is_agents_file(path: str) -> bool:
    return path == "AGENTS.md" or path.endswith("/AGENTS.md")


def _path_matches_prefix(path: str, prefix: str) -> bool:
    prefix = _normalise_path(prefix)
    if prefix.endswith("/"):
        return path == prefix[:-1] or path.startswith(prefix)
    return path == prefix or path.startswith(prefix + "/")


def _normalise_path(value: str) -> str:
    path = value.replace("\\", "/").strip()
    while path.startswith("./"):
        path = path[2:]
    return path


def _normalise_token(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def _normalise_free_text(value: str) -> str:
    return _normalise_token(value)


def _default_requirement_boilerplate_issues(
    req_index: int, req
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    default_scenario_id = f"scn.{_slugify(req.id)}.default"
    if any(
        scenario.id == default_scenario_id
        and scenario.when == ["okg pact validate runs"]
        and scenario.then == [f"requirement {req.id} is checked"]
        for scenario in req.scenarios
    ):
        issues.append(
            ValidationIssue(
                "default_requirement_boilerplate",
                (
                    f"requirement {req.id} still has the generated default scenario; "
                    "replace it with concrete behavior before review"
                ),
                f"requirements[{req_index}].scenarios",
                severity="warning",
            )
        )
    default_acceptance_id = f"acc.{_slugify(req.id)}"
    if any(
        acceptance.id == default_acceptance_id
        and acceptance.description == req.statement
        for acceptance in req.acceptance
    ):
        issues.append(
            ValidationIssue(
                "default_requirement_boilerplate",
                (
                    f"requirement {req.id} still has the generated default acceptance; "
                    "replace it with measurable acceptance criteria before review"
                ),
                f"requirements[{req_index}].acceptance",
                severity="warning",
            )
        )
    return issues


def _derive_alignment_base(
    manifest: PactManifest,
    *,
    repo_root_path: Path | None = None,
    context: PactEvaluationContext | None = None,
) -> dict[str, Any]:
    req_alignment: dict[str, Any] = {}
    task_alignment: dict[str, Any] = {}
    test_alignment: dict[str, Any] = {}
    req_by_id = {r.id: r for r in manifest.requirements}
    task_by_id = {t.id: t for t in manifest.tasks}
    acceptance_ids = {a.id for req in manifest.requirements for a in req.acceptance}

    for req in manifest.requirements:
        missing_traceability = _missing_requirement_traceability(manifest, req.id)
        missing = _missing_requirement_evidence(
            manifest,
            req.id,
            repo_root_path=repo_root_path,
            context=context,
        )
        waiver = _waiver_for(manifest, req.id)
        traceability_status = (
            "traceable" if not missing_traceability else "missing_traceability"
        )
        satisfaction_status = "satisfied" if not missing else "unsatisfied"
        if (missing or missing_traceability) and waiver:
            status = "waived"
            traceability_status = "waived"
            satisfaction_status = "waived"
            missing = []
            missing_traceability = []
        elif _target_language_version(manifest) >= 1:
            status = satisfaction_status
        else:
            status = "complete" if not missing else "missing_evidence"
        req_alignment[req.id] = {
            "status": status,
            "missing": missing,
            "traceability_status": traceability_status,
            "traceability_missing": missing_traceability,
            "satisfaction_status": satisfaction_status,
            "satisfaction_missing": missing,
            **({"waiver": waiver} if waiver else {}),
        }

    for task in manifest.tasks:
        dependency_blockers = [
            dep.from_task
            for dep in manifest.dependencies
            if dep.to_task == task.id
            and dep.kind in {"blocks", "requires"}
            and dep.from_task in task_by_id
            and task_by_id[dep.from_task].status != "done"
            and not dep.waiver
        ]
        environment_blockers = _environment_blockers(
            task,
            context=context,
        )
        blockers = [
            *dependency_blockers,
            *(item["id"] for item in environment_blockers),
        ]
        evidence_issues = _task_evidence_alignment(
            manifest,
            task,
            repo_root_path=repo_root_path,
            context=context,
        )
        missing: list[str] = []
        traceability_missing: list[str] = []
        for req_id in task.requirements:
            if req_id in req_by_id:
                missing.extend(
                    f"{req_id}:{kind}"
                    for kind in _missing_requirement_evidence(
                        manifest,
                        req_id,
                        repo_root_path=repo_root_path,
                        context=context,
                    )
                )
                traceability_missing.extend(
                    f"{req_id}:{kind}"
                    for kind in _missing_requirement_traceability(manifest, req_id)
                )
        missing.extend(evidence_issues)
        if blockers:
            status = "blocked"
        elif missing and task.status == "done":
            status = "missing_evidence"
        elif task.status == "done":
            status = "complete"
        else:
            status = task.status
        task_alignment[task.id] = {
            "status": status,
            "blockers": blockers,
            "dependency_blockers": dependency_blockers,
            "environment_blockers": environment_blockers,
            "evidence_issues": evidence_issues,
            "missing": sorted(set(missing)),
            "traceability_status": (
                "traceable" if not traceability_missing else "missing_traceability"
            ),
            "traceability_missing": sorted(set(traceability_missing)),
            "satisfaction_status": "satisfied"
            if not missing and task.status == "done"
            else "unsatisfied",
            "satisfaction_missing": sorted(set(missing)),
        }

    for test in manifest.test_plan:
        test_alignment[test.id] = {
            "status": (
                "stale"
                if test.requirement not in req_by_id
                and test.requirement not in acceptance_ids
                else "planned"
            ),
            "requirement": test.requirement,
            "test_id": test.test_id,
            "traceability_status": (
                "stale"
                if test.requirement not in req_by_id
                and test.requirement not in acceptance_ids
                else "traceable"
            ),
            "satisfaction_status": _test_satisfaction_status(
                manifest,
                test,
                repo_root_path=repo_root_path,
                context=context,
            ),
        }

    return {
        "versions": version_info(manifest),
        "requirements": req_alignment,
        "tasks": task_alignment,
        "test_plan": test_alignment,
    }


def version_info(manifest: PactManifest) -> dict[str, Any]:
    target = _target_language_version(manifest)
    diagnostics: list[dict[str, str]] = []
    read_mode = "native" if manifest.pact_version == target else "compatibility"
    if manifest.pact_version < target:
        diagnostics.append(
            {
                "code": "compatibility_mode",
                "message": (
                    f"artifact format v{manifest.pact_version} targets "
                    f"PACT language v{target}"
                ),
            }
        )
    if target not in SUPPORTED_TARGET_LANGUAGE_VERSIONS:
        diagnostics.append(
            {
                "code": "unsupported_target_language",
                "message": f"PACT language v{target} is not supported by this reader",
            }
        )
    if manifest.pact_version > CURRENT_ARTIFACT_FORMAT_VERSION:
        diagnostics.append(
            {
                "code": "unsupported_artifact_format",
                "message": (
                    f"artifact format v{manifest.pact_version} is newer than "
                    f"reader support v{CURRENT_ARTIFACT_FORMAT_VERSION}"
                ),
            }
        )
    write_artifact_version = (
        target
        if target <= CURRENT_ARTIFACT_FORMAT_VERSION
        else CURRENT_ARTIFACT_FORMAT_VERSION
    )
    return {
        "artifact_format_version": manifest.pact_version,
        "target_language_version": target,
        "compatibility_mode": manifest.pact_version != target,
        "read_mode": read_mode,
        "write_artifact_version": write_artifact_version,
        "supported_artifact_format_versions": list(
            range(CURRENT_ARTIFACT_FORMAT_VERSION + 1)
        ),
        "supported_target_language_versions": sorted(
            SUPPORTED_TARGET_LANGUAGE_VERSIONS
        ),
        "migration": {
            "can_write_native": target <= CURRENT_ARTIFACT_FORMAT_VERSION,
            "recommended_command": (
                f"okg pact migrate-version {manifest.change.id} "
                f"--target {target} --artifact-format {write_artifact_version}"
            ),
        },
        "migration_diagnostics": diagnostics,
    }


def lifecycle_status(
    manifest: PactManifest,
    alignment: dict[str, Any] | None = None,
    *,
    repo_root: Path | str | None = None,
    context: PactEvaluationContext | None = None,
) -> dict[str, Any]:
    repo_root_path = Path(repo_root) if repo_root is not None else None
    alignment = alignment or _derive_alignment_base(
        manifest,
        repo_root_path=repo_root_path,
        context=context,
    )
    return {
        "proposal": _gate_ok(
            manifest,
            alignment,
            "proposal",
            repo_root_path=repo_root_path,
            context=context,
        ),
        "implementation": _gate_ok(
            manifest,
            alignment,
            "implementation",
            repo_root_path=repo_root_path,
            context=context,
        ),
        "verification": _gate_ok(
            manifest,
            alignment,
            "verification",
            repo_root_path=repo_root_path,
            context=context,
        ),
    }


def _missing_requirement_evidence(
    manifest: PactManifest,
    requirement_id: str,
    *,
    repo_root_path: Path | None = None,
    context: PactEvaluationContext | None = None,
) -> list[str]:
    req = next(r for r in manifest.requirements if r.id == requirement_id)
    registry = _kind_registry(repo_root_path, context=context)
    missing: list[str] = []
    for kind in req.evidence_policy.required:
        if kind == "planned_test":
            if not any(t.requirement == requirement_id for t in manifest.test_plan):
                missing.append(kind)
            elif _target_language_version(manifest) >= 1 and not _has_evidence(
                manifest,
                requirement_id,
                registry.expand("executed_test"),
                repo_root_path=repo_root_path,
                context=context,
                registry=registry,
            ):
                missing.append("executed_test")
        elif registry.canonicalize(kind) == "executed_test":
            if not _has_evidence(
                manifest,
                requirement_id,
                registry.expand(kind),
                repo_root_path=repo_root_path,
                context=context,
                registry=registry,
            ):
                missing.append(kind)
        elif registry.canonicalize(kind) == "formal_check":
            formal_bindings = [
                f for f in manifest.formal if f.requirement == requirement_id
            ]
            if not formal_bindings or not any(
                _formal_binding_evidence_ok(
                    manifest,
                    binding,
                    evidence,
                    repo_root_path=repo_root_path,
                    context=context,
                )
                for binding in formal_bindings
                for evidence in manifest.evidence
            ):
                missing.append(kind)
        else:
            # Alias-aware: a requirement demanding `graph` is satisfied by
            # `graph_report` evidence of the same run, and vice versa.
            if not _has_evidence(
                manifest,
                requirement_id,
                registry.expand(kind),
                repo_root_path=repo_root_path,
                context=context,
                registry=registry,
            ):
                missing.append(kind)
    # `required_any`: satisfied when >=1 of the listed kinds has evidence (OR).
    any_kinds = req.evidence_policy.required_any
    if any_kinds and not any(
        _has_evidence(
            manifest,
            requirement_id,
            registry.expand(kind),
            repo_root_path=repo_root_path,
            context=context,
            registry=registry,
        )
        for kind in any_kinds
    ):
        missing.append("any:" + "|".join(any_kinds))
    if _target_language_version(manifest) >= 3:
        acceptance = _acceptance_alignment(
            manifest,
            repo_root_path=repo_root_path,
            context=context,
            registry=registry,
        )
        for criterion in req.acceptance:
            state = acceptance.get(criterion.id, {})
            # A tokenless criterion is refused by
            # `missing_acceptance_evidence`; it declares no proof slot that
            # could truthfully appear here as owed. Keep that structural
            # refusal separate from missing evidence, matching the alignment
            # surface's `declares_no_proof` distinction.
            if (
                acceptance_proof_tokens(criterion)
                and state.get("satisfaction_status") == "unsatisfied"
            ):
                missing.append(f"acceptance:{criterion.id}")
    # Everything above answers "is the DECLARED proof present". This answers
    # the separate question the policy is the wrong index for: "does a
    # recorded outcome REFUTE this requirement". Both feed one `missing`
    # list on purpose — the gate and `pact view` read the same quantity
    # (operations.py:3166 and cli.py:1632-1638), so fixing one surface and
    # not the other would leave `gaps: 0` on a PACT the gate refuses.
    missing.extend(_refuting_requirement_evidence(manifest, requirement_id))
    return missing


def _refuting_requirement_evidence(
    manifest: PactManifest,
    requirement_id: str,
) -> list[str]:
    """The rows attached to `requirement_id` whose recorded outcome refutes it.

    Deliberately NOT routed through `_has_evidence`. That predicate filters on
    `evidence.kind in kinds` (:4373-4384), which is the correct filter for
    "is the declared proof present" and is the exact filter that made a
    recorded failure of an undeclared kind unreadable. This pass applies NO
    policy filter at all — the same rule `_task_evidence_alignment` (:4232,
    :4249-4250) has always applied to task-attached rows, now reaching the
    change-level rows that are referenced by requirement instead, which is
    where the entire exposed corpus population lives.

    The ref set is the one `_has_evidence` builds (:4360-4372): the
    requirement's own id, its acceptance criteria, and its formal bindings.
    Formal bindings are included unconditionally here rather than only for
    formal kinds — a failing formal row bound to this requirement refutes it
    whatever the policy asks for.

    Returns BARE tokens, matching this module's convention: the requirement
    alignment reports them as-is and the task rollup prefixes `{req_id}:`
    (:2819-2827).
    """
    req = next((r for r in manifest.requirements if r.id == requirement_id), None)
    if req is None:
        return []
    refs = {requirement_id}
    refs.update(criterion.id for criterion in req.acceptance)
    refs.update(
        binding.id
        for binding in manifest.formal
        if binding.requirement == requirement_id
    )
    refuting: list[str] = []
    for evidence in manifest.evidence:
        if evidence.status not in REFUTING_EVIDENCE:
            continue
        if not (set(evidence.refs) & refs):
            continue
        # The row's own spelling is reported, not the canonical form: an
        # operator reading the refusal has to find this row in the file.
        refuting.append(
            f"{REFUTATION_TOKEN}:{evidence.id}:{evidence.kind}:{evidence.status}"
        )
    return refuting


def _missing_requirement_traceability(
    manifest: PactManifest,
    requirement_id: str,
) -> list[str]:
    has_test = any(test.requirement == requirement_id for test in manifest.test_plan)
    has_acceptance_test = any(
        test.requirement == acceptance.id
        for req in manifest.requirements
        if req.id == requirement_id
        for acceptance in req.acceptance
        for test in manifest.test_plan
    )
    has_formal = any(
        binding.requirement == requirement_id for binding in manifest.formal
    )
    if has_test or has_acceptance_test or has_formal:
        return []
    return ["planned_test"]


def _test_satisfaction_status(
    manifest: PactManifest,
    test,
    *,
    repo_root_path: Path | None = None,
    context: PactEvaluationContext | None = None,
) -> str:
    req_ids = {req.id for req in manifest.requirements}
    acceptance_to_req = {
        acceptance.id: req.id
        for req in manifest.requirements
        for acceptance in req.acceptance
    }
    requirement = test.requirement
    if requirement not in req_ids and requirement not in acceptance_to_req:
        return "stale"
    req_id = acceptance_to_req.get(requirement, requirement)
    registry = _kind_registry(repo_root_path, context=context)
    evidence_kinds = _test_plan_evidence_kinds(test, registry=registry)
    refs = {test.id, test.test_id, requirement, req_id}
    return (
        "satisfied"
        if any(
            _evidence_ok_for_alignment(
                manifest,
                evidence,
                repo_root_path=repo_root_path,
                context=context,
                registry=registry,
            )
            and evidence.kind in evidence_kinds
            and bool(set(evidence.refs) & refs)
            for evidence in manifest.evidence
        )
        else "unsatisfied"
    )


def _test_plan_evidence_kinds(
    test,
    *,
    registry: EvidenceKindRegistry = DEFAULT_REGISTRY,
) -> set[str]:
    kinds = {kind for kind in (test.evidence or []) if kind != "planned_test"}
    if not kinds or any(
        registry.canonicalize(kind) == "executed_test" for kind in kinds
    ):
        kinds |= registry.expand("executed_test")
    return kinds


def acceptance_arms(criterion) -> list[tuple[str, list[str]]]:
    """The criterion's declared arms, as (arm id, its proof tokens).

    Explicit ``arms:`` when the author declared them. Otherwise every token in
    ``criterion.evidence`` is its own arm — which is not an inference about
    prose but the corpus's existing convention: a criterion that names two
    things to show lists two proof tokens (the pattern that correctly keeps
    ``req.chat-review-queue-pipe.one-inbox`` unsatisfied while its ``ui_visual``
    arm has no evidence).
    """
    if criterion.arms:
        return [(arm.id, list(arm.evidence)) for arm in criterion.arms]
    return [(proof, [proof]) for proof in criterion.evidence]


def acceptance_proof_tokens(criterion) -> list[str]:
    """Every proof token a criterion declares, arms included, in order."""
    if not criterion.arms:
        return list(criterion.evidence)
    tokens: list[str] = []
    for arm in criterion.arms:
        tokens.extend(token for token in arm.evidence if token not in tokens)
    return tokens


def _arm_proof_domain(
    manifest: PactManifest,
    proof: str,
    registry: EvidenceKindRegistry,
) -> tuple[str | None, frozenset[str]]:
    """``(exclusive_key, kind_domain)`` for one declared proof token.

    ``exclusive_key`` is non-None when the token names ONE specific row (an
    explicit ``ev.*`` id) or one specific formal binding, whose matching rows
    must carry that binding id in ``refs``. Two tokens with different
    exclusive keys can never be closed by the same row, whatever their kinds.

    ``kind_domain`` is the set of evidence kinds a row could carry and still
    match the token, mirroring the branches of
    ``_matching_acceptance_evidence_ids`` one-for-one.

    Everything here is read from DECLARATIONS: the token's form, the
    registry's alias expansion, and the declared ``kind`` of a referenced row.
    No description, summary, or status is consulted — a checker that guessed
    at prose would replace one unfalsifiable gate with another.
    """
    canonical = registry.canonicalize(proof)
    if canonical == "executed_test" or proof == "planned_test":
        return None, frozenset(registry.expand("executed_test"))
    if canonical == "formal_check":
        return None, frozenset(registry.expand("formal_check"))
    if canonical in {"graph_report", "published_generation"}:
        # The matcher folds these two tokens into ONE expanded set, so a
        # criterion naming both is closable by a single row: the pair is
        # deliberately reported as colliding rather than papered over here.
        return None, frozenset(
            registry.expand("graph_report") | registry.expand("published_generation")
        )
    if proof == "waiver":
        return None, _WAIVER_ARM_DOMAIN
    test = next((entry for entry in manifest.test_plan if entry.id == proof), None)
    if test is not None:
        # A test-plan token's refs include the acceptance id, so any row
        # ref'ing the criterion matches it: two test tokens of the same kind
        # are NOT separable, which is exactly the collapse this rule refuses.
        return None, frozenset(_test_plan_evidence_kinds(test, registry=registry))
    binding = next((row for row in manifest.formal if row.id == proof), None)
    if binding is not None:
        return f"formal:{binding.id}", frozenset(registry.expand("formal_check"))
    evidence = next((row for row in manifest.evidence if row.id == proof), None)
    if evidence is not None:
        return f"evidence:{evidence.id}", frozenset({evidence.kind})
    return None, frozenset(registry.expand(proof))


def _acceptance_criterion_declares_no_proof(criterion) -> bool:
    """A criterion naming nothing that could ever show it.

    Neither ``evidence`` nor ``arms``, so ``acceptance_proof_tokens`` is empty,
    so the per-token loop in ``_acceptance_alignment`` finds nothing MISSING
    and reports the criterion satisfied on ZERO rows. That is the same
    absence-as-agreement failure this change exists to refuse, one notch worse
    than the one-row collapse: the criterion is not over-closed by a row, it is
    closed by no row at all. Reported, and it holds the criterion open.
    """
    return not acceptance_proof_tokens(criterion)


def _acceptance_arms_without_proof(criterion) -> list[str]:
    """Declared arms carrying no proof token at all.

    An arm with no token matches nothing, so it contributes no verdict and
    would leave the criterion satisfiable by its siblings alone — absence
    read as agreement. Reported, and it holds the criterion open.
    """
    if not criterion.arms:
        return []
    return [arm.id for arm in criterion.arms if not arm.evidence]


def _acceptance_arm_collisions(
    manifest: PactManifest,
    criterion,
    *,
    registry: EvidenceKindRegistry,
) -> list[dict[str, Any]]:
    """Pairs of declared arms that one evidence row could close on its own.

    A criterion with fewer than two arms has nothing to collapse and is never
    reported here: one arm proved by one row stays satisfied, and ZERO arms is
    a different refusal (``_acceptance_criterion_declares_no_proof``).

    The exemption is deliberate and is the rule's declared limit. A criterion
    whose description demands three things while declaring ONE proof token is
    invisible to this function, because telling that criterion apart from an
    honest one-obligation criterion requires reading the description, and a
    checker that guessed at prose would replace one unfalsifiable gate with
    another. What holds a single-token criterion honest is the OTHER
    mechanism: its task's `verification` enumerates the test ids, and the
    --collect-only / zero-skips check fails if a named id does not exist.
    """
    arms = acceptance_arms(criterion)
    if len(arms) < 2:
        return []
    domains: list[tuple[str, list[tuple[str | None, frozenset[str]]]]] = []
    for arm_id, tokens in arms:
        domains.append(
            (arm_id, [_arm_proof_domain(manifest, token, registry) for token in tokens])
        )

    def compatible(
        left: tuple[str | None, frozenset[str]],
        right: tuple[str | None, frozenset[str]],
    ) -> bool:
        """True when two tokens structurally require different witnesses."""
        left_key, left_kinds = left
        right_key, right_kinds = right
        if left_key is not None and right_key is not None and left_key != right_key:
            return True
        return not bool(left_kinds & right_kinds)

    def has_distinct_assignment(
        candidates: list[list[tuple[str | None, frozenset[str]]]],
    ) -> bool:
        """Choose one non-interchangeable witness token for every arm.

        This is a small backtracking matching problem (acceptance criteria have
        only a handful of arms). It matters for conjunctions: an arm declaring
        both pytest and a live transcript can bind to the transcript while a
        sibling binds to pytest. Unioning their kinds would falsely reject that
        valid declaration. Pairwise checks alone are also insufficient for
        three arms competing for only two distinct witness domains.
        """
        ordered = sorted(candidates, key=len)

        def assign(index: int, chosen: list[tuple[str | None, frozenset[str]]]) -> bool:
            if index == len(ordered):
                return True
            return any(
                all(compatible(candidate, prior) for prior in chosen)
                and assign(index + 1, [*chosen, candidate])
                for candidate in ordered[index]
            )

        return bool(ordered) and all(ordered) and assign(0, [])

    if has_distinct_assignment([candidates for _, candidates in domains]):
        return []

    collisions: list[dict[str, Any]] = []
    for index, (left_id, left_candidates) in enumerate(domains):
        for right_id, right_candidates in domains[index + 1 :]:
            if any(
                compatible(left, right)
                for left in left_candidates
                for right in right_candidates
            ):
                continue
            shared = set().union(
                *(
                    left[1] & right[1]
                    for left in left_candidates
                    for right in right_candidates
                )
            )
            collisions.append(
                {
                    "arms": [left_id, right_id],
                    "shared_kinds": sorted(shared),
                }
            )
    if not collisions:
        collisions.append(
            {
                "arms": [arm_id for arm_id, _ in domains],
                "shared_kinds": sorted(
                    set().union(
                        *(
                            kind_domain
                            for _, candidates in domains
                            for _, kind_domain in candidates
                        )
                    )
                ),
            }
        )
    return collisions


def acceptance_arm_binding_issues(
    manifest: PactManifest,
    *,
    repo_root_path: Path | None = None,
    context: PactEvaluationContext | None = None,
    severity: str = "warning",
) -> list[ValidationIssue]:
    """The named acceptance-binding refusals.

    Per `pact-acceptance-prose-is-enforceable`
    req.pact-acceptance-prose-is-enforceable.arms-bind-structurally, keyed on
    what the criterion DECLARES and never on its description:

    * ``acceptance_criterion_without_proof`` — no proof token at all, so it
      would report satisfied on zero rows;
    * ``acceptance_arms_and_evidence_both_declared`` — two proof sets, so the
      reader cannot tell which one binds;
    * ``acceptance_arm_without_proof`` — a declared arm nothing can match;
    * ``acceptance_arms_not_separably_bound`` — two arms one row could close,
      so the second arm is never shown.

    A criterion that names several arms must bind each one to a proof the
    other arms cannot supply — a distinct evidence kind per arm, or an
    explicit ``ev.*`` id per arm. Both mechanisms already worked; what was
    missing was the refusal when neither is used.

    The issue is raised at ``severity="warning"`` from general validation, so
    every surface REPORTS it, and re-raised as an error by
    ``_lifecycle_gate_issues`` at the verification gate, so it BLOCKS proof
    completion rather than sitting there advisory.
    """
    registry = _kind_registry(repo_root_path, context=context)
    issues: list[ValidationIssue] = []
    for req_index, req in enumerate(manifest.requirements):
        for acc_index, criterion in enumerate(req.acceptance):
            if _acceptance_criterion_declares_no_proof(criterion):
                issues.append(
                    ValidationIssue(
                        "acceptance_criterion_without_proof",
                        (
                            f"acceptance criterion {criterion.id} declares no proof "
                            "token at all, so nothing can ever show it and it "
                            "reports satisfied on zero rows. Give it an evidence "
                            "kind, an explicit ev.* id, or `arms`"
                        ),
                        f"requirements[{req_index}].acceptance[{acc_index}].evidence",
                        severity=severity,
                    )
                )
            if criterion.arms and criterion.evidence:
                issues.append(
                    ValidationIssue(
                        "acceptance_arms_and_evidence_both_declared",
                        (
                            f"acceptance criterion {criterion.id} declares both "
                            "`arms` and a flat `evidence` list; arms supersede the "
                            "flat list, so keep one. Move every flat token into the "
                            "arm it proves"
                        ),
                        f"requirements[{req_index}].acceptance[{acc_index}].arms",
                        severity=severity,
                    )
                )
            for arm_id in _acceptance_arms_without_proof(criterion):
                issues.append(
                    ValidationIssue(
                        "acceptance_arm_without_proof",
                        (
                            f"acceptance criterion {criterion.id} declares arm "
                            f"{arm_id!r} with no proof token, so nothing can ever "
                            "show it. Give the arm an evidence kind or an "
                            "explicit ev.* id, or drop the arm"
                        ),
                        f"requirements[{req_index}].acceptance[{acc_index}].arms",
                        severity=severity,
                    )
                )
            for collision in _acceptance_arm_collisions(
                manifest,
                criterion,
                registry=registry,
            ):
                arm_names = repr(collision["arms"])
                issues.append(
                    ValidationIssue(
                        "acceptance_arms_not_separably_bound",
                        (
                            f"acceptance criterion {criterion.id} declares arms "
                            f"{arm_names} that cannot be assigned distinct "
                            "witnesses from evidence kinds "
                            f"{collision['shared_kinds']}, so ONE row closes both "
                            "and the second arm is never shown. Bind each arm to a "
                            "distinct evidence kind, or to an explicit ev.* id"
                        ),
                        f"requirements[{req_index}].acceptance[{acc_index}].evidence",
                        severity=severity,
                    )
                )
    return issues


def _acceptance_alignment(
    manifest: PactManifest,
    *,
    repo_root_path: Path | None = None,
    context: PactEvaluationContext | None = None,
    registry: EvidenceKindRegistry | None = None,
) -> dict[str, Any]:
    alignment: dict[str, Any] = {}
    registry = registry or _kind_registry(repo_root_path, context=context)
    tests_by_requirement: dict[str, list[Any]] = {}
    for test in manifest.test_plan:
        tests_by_requirement.setdefault(test.requirement, []).append(test)
    for req in manifest.requirements:
        for criterion in req.acceptance:
            proof_rows = []
            missing = []
            # An unbound multi-arm criterion is unsatisfied WHATEVER evidence
            # exists: the arms cannot be told apart, so no set of rows shows
            # each of them. Computed before the per-token matching so the
            # verdict does not depend on which rows happen to be attached.
            arm_collisions = _acceptance_arm_collisions(
                manifest,
                criterion,
                registry=registry,
            )
            arm_unproved = _acceptance_arms_without_proof(criterion)
            # Zero proof tokens leaves the loop below with nothing to iterate,
            # so `missing` stays empty and the criterion would read satisfied
            # on no rows at all. Held open explicitly.
            declares_no_proof = _acceptance_criterion_declares_no_proof(criterion)
            for proof in acceptance_proof_tokens(criterion):
                evidence_ids = _matching_acceptance_evidence_ids(
                    manifest,
                    req.id,
                    criterion.id,
                    proof,
                    repo_root_path=repo_root_path,
                    context=context,
                    registry=registry,
                )
                status = "satisfied" if evidence_ids else "unsatisfied"
                proof_rows.append(
                    {
                        "proof": proof,
                        "status": status,
                        "evidence": evidence_ids,
                    }
                )
                if status == "unsatisfied":
                    missing.append(proof)
            satisfied = (
                not missing
                and not arm_collisions
                and not arm_unproved
                and not declares_no_proof
            )
            alignment[criterion.id] = {
                "requirement": req.id,
                "status": "satisfied" if satisfied else "unsatisfied",
                "satisfaction_status": ("satisfied" if satisfied else "unsatisfied"),
                "satisfaction_missing": missing,
                # Reported separately from `satisfaction_missing`, which lists
                # proof tokens with no matching row. An arm collision is a
                # different failure: the rows may all be there, but two arms
                # are indistinguishable, so attaching more evidence cannot fix
                # it — the DECLARATION has to change.
                "arm_collisions": arm_collisions,
                "arm_unproved": arm_unproved,
                # Reported separately again: this criterion has no missing
                # token because it declared none, which a reader of
                # `satisfaction_missing` alone would misread as "nothing owed".
                "declares_no_proof": declares_no_proof,
                "proofs": proof_rows,
            }
    return alignment


def _matching_acceptance_evidence_ids(
    manifest: PactManifest,
    requirement_id: str,
    acceptance_id: str,
    proof: str,
    *,
    repo_root_path: Path | None = None,
    context: PactEvaluationContext | None = None,
    registry: EvidenceKindRegistry | None = None,
) -> list[str]:
    test_by_id = {test.id: test for test in manifest.test_plan}
    formal_by_id = {binding.id: binding for binding in manifest.formal}
    evidence_by_id = {evidence.id: evidence for evidence in manifest.evidence}
    registry = registry or _kind_registry(repo_root_path, context=context)
    refs = {requirement_id, acceptance_id}
    refs.update(
        test.id
        for test in manifest.test_plan
        if test.requirement in {requirement_id, acceptance_id}
    )
    refs.update(
        test.test_id
        for test in manifest.test_plan
        if test.requirement in {requirement_id, acceptance_id}
    )
    if registry.canonicalize(proof) == "executed_test" or proof == "planned_test":
        kinds = registry.expand("executed_test")
    elif registry.canonicalize(proof) == "formal_check":
        kinds = registry.expand("formal_check")
        refs.update(
            binding.id
            for binding in manifest.formal
            if binding.requirement == requirement_id
        )
    elif registry.canonicalize(proof) in {"graph_report", "published_generation"}:
        kinds = registry.expand("graph_report") | registry.expand(
            "published_generation"
        )
    elif proof == "waiver":
        return [
            waiver.id
            for waiver in manifest.waivers
            if waiver.scope in {requirement_id, acceptance_id} and waiver.owner
        ]
    elif proof in test_by_id:
        test = test_by_id[proof]
        kinds = _test_plan_evidence_kinds(test, registry=registry)
        refs = {test.id, test.test_id, acceptance_id}
    elif proof in formal_by_id:
        binding = formal_by_id[proof]
        return [
            evidence.id
            for evidence in manifest.evidence
            if _formal_binding_evidence_ok(
                manifest,
                binding,
                evidence,
                repo_root_path=repo_root_path,
                context=context,
            )
        ]
    elif proof in evidence_by_id:
        evidence = evidence_by_id[proof]
        return (
            [proof]
            if _evidence_ok_for_alignment(
                manifest,
                evidence,
                repo_root_path=repo_root_path,
                context=context,
                registry=registry,
            )
            else []
        )
    else:
        # Alias-aware, matching the policy branch: an acceptance proof
        # token `review` matches `design_review` evidence.
        kinds = registry.expand(proof)
    return [
        evidence.id
        for evidence in manifest.evidence
        if _evidence_ok_for_alignment(
            manifest,
            evidence,
            repo_root_path=repo_root_path,
            context=context,
            registry=registry,
        )
        and evidence.kind in kinds
        and bool(set(evidence.refs) & refs)
    ]


def _formal_binding_evidence_ok(
    manifest: PactManifest,
    binding,
    evidence,
    *,
    repo_root_path: Path | None = None,
    context: PactEvaluationContext | None = None,
) -> bool:
    return not _formal_binding_evidence_stale_reasons(
        manifest,
        binding,
        evidence,
        repo_root_path=repo_root_path,
        context=context,
    )


def formal_bindings_without_fresh_evidence(
    manifest: PactManifest,
    *,
    repo_root_path: Path,
    ignore_reasons: frozenset[str] = frozenset(),
    context: PactEvaluationContext | None = None,
) -> list[dict]:
    """Formal bindings whose every fresh-claiming evidence row is stale
    against the current repo bytes.

    Gate semantics: one currently-valid row per binding suffices; historical
    superseded rows are fine. A binding with NO fresh-claiming rows is not
    reported (it never claimed proof). Used by the corpus freshness sweep
    (tests/pact/test_formal_evidence_freshness_sweep.py) and the repair
    script (scripts/repair_formal_evidence.py).

    ``ignore_reasons`` drops named stale reasons before deciding: the sweep
    passes {"artifact_digest"} because checker artifacts are machine-local
    run outputs (typically under .scratch/, absent on a fresh checkout),
    while module/config/checker-script digests are repo-tracked and are the
    durable rot signal."""
    stale: list[dict] = []
    for binding in manifest.formal:
        rows = [
            evidence
            for evidence in manifest.evidence
            if evidence.kind in {"formal", "formal_check"}
            and binding.id in evidence.refs
            and (evidence.data or {}).get("fresh") is True
        ]
        if not rows:
            continue
        reasons_by_row = {
            evidence.id: [
                reason
                for reason in _formal_binding_evidence_stale_reasons(
                    manifest,
                    binding,
                    evidence,
                    repo_root_path=repo_root_path,
                    context=context,
                )
                if reason not in ignore_reasons
            ]
            for evidence in rows
        }
        if all(reasons_by_row.values()):
            newest_id = sorted(reasons_by_row)[-1]
            stale.append(
                {
                    "binding_id": binding.id,
                    "property_id": binding.property_id,
                    "checker_command": binding.checker_command,
                    "fresh_claiming_rows": len(rows),
                    "newest_row": newest_id,
                    "newest_reasons": sorted(set(reasons_by_row[newest_id])),
                }
            )
    return stale


def _formal_binding_evidence_stale_reasons(
    manifest: PactManifest,
    binding,
    evidence,
    *,
    repo_root_path: Path | None = None,
    context: PactEvaluationContext | None = None,
) -> list[str]:
    reasons: list[str] = []
    if evidence.status not in PASSING_EVIDENCE:
        reasons.append("status")
    if evidence.kind not in FORMAL_KINDS:
        reasons.append("kind")
    if binding.id not in evidence.refs:
        reasons.append("binding_ref")
    if _target_language_version(manifest) < 3:
        return reasons
    data = evidence.data or {}
    refinement_tests = set(data.get("refinement_tests") or [])
    if data.get("property_id") != binding.property_id:
        reasons.append("property_id")
    if data.get("checker_command") != binding.checker_command:
        reasons.append("checker_command")
    if data.get("fresh") is not True:
        reasons.append("fresh")
    if not set(binding.refinement_tests).issubset(refinement_tests):
        reasons.append("refinement_tests")
    _append_digest_reason(
        reasons,
        "module_digest",
        expected=(
            _path_digest(
                repo_root_path / binding.module,
                repo_root=repo_root_path,
                context=context,
            )
            if repo_root_path is not None
            else None
        ),
        stored=data.get("module_digest"),
        require_stored=True,
        compare=repo_root_path is not None,
    )
    if binding.config:
        _append_digest_reason(
            reasons,
            "config_digest",
            expected=(
                _path_digest(
                    repo_root_path / binding.config,
                    repo_root=repo_root_path,
                    context=context,
                )
                if repo_root_path is not None
                else None
            ),
            stored=data.get("config_digest"),
            require_stored=True,
            compare=repo_root_path is not None,
        )
    _append_digest_reason(
        reasons,
        "artifact_digest",
        expected=(
            _path_digest(
                _formal_evidence_artifact_path(
                    repo_root_path,
                    evidence,
                    context=context,
                ),
                repo_root=repo_root_path,
                context=context,
            )
            if repo_root_path is not None
            else None
        ),
        stored=data.get("artifact_digest"),
        require_stored=True,
        compare=repo_root_path is not None,
    )
    reasons.extend(
        _checker_script_stale_reasons(
            binding,
            data,
            repo_root_path=repo_root_path,
            context=context,
        )
    )
    if (
        context is not None
        and context.ignore_machine_local_formal_artifact_digest
        and _formal_evidence_artifact_is_machine_local(evidence)
    ):
        reasons = [reason for reason in reasons if reason != "artifact_digest"]
    return reasons


def _formal_evidence_artifact_is_machine_local(evidence) -> bool:
    """Return whether the checker output uses the untracked scratch boundary."""

    ref = (evidence.data or {}).get("artifact_path") or evidence.artifact
    if not ref:
        return False
    return ".scratch" in Path(str(ref).replace("\\", "/")).parts


def _append_digest_reason(
    reasons: list[str],
    label: str,
    *,
    expected: str | None,
    stored: Any,
    require_stored: bool,
    compare: bool,
) -> None:
    if require_stored and not stored:
        reasons.append(label)
        return
    if compare and (expected is None or stored != expected):
        reasons.append(label)


def _checker_script_stale_reasons(
    binding,
    data: dict[str, Any],
    *,
    repo_root_path: Path | None = None,
    context: PactEvaluationContext | None = None,
) -> list[str]:
    refs = _checker_script_refs(binding.checker_command or "")
    if not refs:
        return []
    stored = data.get("checker_script_digests") or {}
    if not isinstance(stored, dict):
        return ["checker_script_digests"]
    reasons: list[str] = []
    for ref in refs:
        stored_digest = stored.get(ref)
        if not stored_digest:
            reasons.append(f"checker_script_digest:{ref}")
            continue
        if repo_root_path is None:
            continue
        resolved = _resolve_repo_file(
            repo_root_path,
            ref,
            "checker_command",
            context=context,
        )
        digest = (
            None
            if isinstance(resolved, ValidationIssue)
            else _path_digest(
                resolved,
                repo_root=repo_root_path,
                context=context,
            )
        )
        if digest is None or stored_digest != digest:
            reasons.append(f"checker_script_digest:{ref}")
    return reasons


def _formal_evidence_artifact_path(
    repo_root_path: Path,
    evidence,
    *,
    context: PactEvaluationContext | None = None,
) -> Path:
    ref = (evidence.data or {}).get("artifact_path") or evidence.artifact
    if not ref:
        return repo_root_path / "__missing_formal_artifact__"
    path = Path(str(ref)) if context is not None else Path(str(ref)).expanduser()
    if context is not None:
        return context.resolve_repo_path(repo_root_path, path)
    if path.is_absolute():
        return path.resolve()
    return (repo_root_path / path).resolve()


def _formal_evidence_binding(manifest: PactManifest, evidence):
    formal_by_id = {binding.id: binding for binding in manifest.formal}
    for ref in evidence.refs:
        binding = formal_by_id.get(ref)
        if binding is not None:
            return binding
    return None


def _evidence_ok_for_alignment(
    manifest: PactManifest,
    evidence,
    *,
    repo_root_path: Path | None = None,
    context: PactEvaluationContext | None = None,
    registry: EvidenceKindRegistry | None = None,
) -> bool:
    registry = registry or _kind_registry(repo_root_path, context=context)
    canonical_kind = registry.canonicalize(evidence.kind)
    if canonical_kind == "formal_check":
        binding = _formal_evidence_binding(manifest, evidence)
        if binding is None:
            return False
        return _formal_binding_evidence_ok(
            manifest,
            binding,
            evidence,
            repo_root_path=repo_root_path,
            context=context,
        )
    if canonical_kind == "executed_test":
        return (
            evidence.status in PASSING_EVIDENCE
            and not _executed_test_evidence_reasons(manifest, evidence)
        )
    return evidence.status in PASSING_EVIDENCE


def _pytest_identity(value: str) -> tuple[str, str] | None:
    if "::" not in value:
        return None
    path, *selectors = value.split("::")
    path = path.replace("/", ".")
    if path.endswith(".py"):
        path = path[:-3]
    normalized_selectors = [selector.split("[")[0] for selector in selectors]
    if len(normalized_selectors) > 1:
        path = ".".join([path, *normalized_selectors[:-1]])
    return path, normalized_selectors[-1]


def _pytest_identity_matches(planned: str, observed: str) -> bool:
    want = _pytest_identity(planned)
    got = _pytest_identity(observed)
    if want is None or got is None:
        return False
    return want[1] == got[1] and (got[0] == want[0] or got[0].startswith(f"{want[0]}."))


def _executed_test_evidence_reasons(manifest: PactManifest, evidence) -> list[str]:
    """Why a passing test row does not attest its concrete named promises.

    This is evaluated on every read and gate, not only at ingest, so evidence
    recorded before the ingest check is not grandfathered. The canonical row
    remains unchanged and visible; it simply no longer counts as proof until a
    fresh JUnit report shows every named test passed.
    """
    direct_refs = set(evidence.refs)
    acceptance_owner = {
        criterion.id: requirement.id
        for requirement in manifest.requirements
        for criterion in requirement.acceptance
    }
    task_requirements = {
        requirement
        for task in manifest.tasks
        if task.id in direct_refs
        for requirement in task.requirements
    }
    direct_requirement_refs = (
        {req.id for req in manifest.requirements} & direct_refs
    ) | {acceptance_owner[ref] for ref in direct_refs if ref in acceptance_owner}
    # Ingested requirement rollups also carry their owning task ids for
    # traceability. Those generated task refs must not broaden one requirement
    # row into a claim over every sibling requirement. A row whose ONLY scope
    # is a task, however, claims that whole task and must attest its plans.
    requirement_refs = direct_requirement_refs or task_requirements
    scoped_subjects = requirement_refs | {
        criterion.id
        for requirement in manifest.requirements
        if requirement.id in requirement_refs
        for criterion in requirement.acceptance
    }
    candidates = [
        entry
        for entry in manifest.test_plan
        if ".py::" in entry.test_id
        and (
            entry.requirement in scoped_subjects
            or entry.id in direct_refs
            or entry.test_id in direct_refs
        )
    ]
    if not candidates:
        return []
    data = evidence.data or {}
    junit_ids = [str(value) for value in data.get("junit_test_ids") or []]
    legacy_junit_id = data.get("junit_test_id")
    if legacy_junit_id:
        junit_ids.append(str(legacy_junit_id))
    skipped = [str(value) for value in data.get("skipped_test_ids") or []]
    failed = [str(value) for value in data.get("failed_test_ids") or []]
    reasons: list[str] = []
    for entry in candidates:
        if any(_pytest_identity_matches(entry.test_id, value) for value in failed):
            reasons.append(f"failed:{entry.id}")
        elif any(_pytest_identity_matches(entry.test_id, value) for value in skipped):
            reasons.append(f"skipped:{entry.id}")
        elif not any(
            _pytest_identity_matches(entry.test_id, value) for value in junit_ids
        ):
            reasons.append(f"unattested:{entry.id}")
    return reasons


def _lifecycle_gate_issues(
    manifest: PactManifest,
    alignment: dict[str, Any],
    gate: LifecycleGate,
    *,
    repo_root_path: Path | None = None,
    context: PactEvaluationContext | None = None,
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    if gate == "proposal":
        for req_id, state in alignment["requirements"].items():
            if state["traceability_status"] not in {"traceable", "waived"}:
                issues.append(
                    ValidationIssue(
                        "proposal_gate_untraceable",
                        f"requirement {req_id} is {state['traceability_status']}",
                        f"requirements.{req_id}",
                    )
                )
        return issues

    if gate == "implementation":
        issues.extend(
            _lifecycle_gate_issues(
                manifest,
                alignment,
                "proposal",
                repo_root_path=repo_root_path,
                context=context,
            )
        )
        if not _has_fresh_intent_review(manifest):
            issues.append(
                ValidationIssue(
                    "implementation_gate_missing_intent_review",
                    "implementation requires fresh digest-bound intent_review evidence",
                    "evidence.intent_review",
                )
            )
        return issues

    if _target_language_version(manifest) >= 3 and not _has_lifecycle_bypass(
        manifest,
        "lifecycle.implementation",
    ):
        implementation_issues = _lifecycle_gate_issues(
            manifest,
            alignment,
            "implementation",
            repo_root_path=repo_root_path,
            context=context,
        )
        if implementation_issues:
            issues.append(
                ValidationIssue(
                    "verification_gate_missing_implementation_gate",
                    (
                        "verification requires implementation gate approval before "
                        "proof completion unless a lifecycle bypass waiver is owner-approved"
                    ),
                    "change.status",
                )
            )
    issues.extend(_guidance_review_issues(manifest))
    # The arm-binding and integration-acceptance refusals are warnings in
    # general validation (so an in-flight PACT can still be edited and
    # gated) and ERRORS here. Per
    # req.pact-acceptance-prose-is-enforceable.arms-bind-structurally and
    # req.pact-acceptance-prose-is-enforceable.integration-acceptance-validated-or-deleted:
    # an advisory refusal at the proof bar is the same absence-as-agreement
    # failure one level up.
    issues.extend(
        acceptance_arm_binding_issues(
            manifest,
            repo_root_path=repo_root_path,
            context=context,
            severity="error",
        )
    )
    issues.extend(
        _integration_acceptance_issues(
            manifest,
            repo_root_path=repo_root_path,
            context=context,
            severity="error",
        )
    )
    for req_id, state in alignment["requirements"].items():
        if state["satisfaction_status"] not in {"satisfied", "waived"}:
            issues.append(
                ValidationIssue(
                    "completion_gate_unsatisfied",
                    f"requirement {req_id} is {state['satisfaction_status']}",
                    f"requirements.{req_id}",
                )
            )
    for binding in manifest.formal:
        if not any(
            _formal_binding_evidence_ok(
                manifest,
                binding,
                ev,
                repo_root_path=repo_root_path,
                context=context,
            )
            for ev in manifest.evidence
        ):
            issues.append(
                ValidationIssue(
                    "verification_gate_missing_formal_evidence",
                    f"formal binding {binding.id} has no passing checker evidence",
                    f"formal.{binding.id}",
                )
            )
    for task_id, state in alignment["tasks"].items():
        if state.get("status") != "complete":
            issues.append(
                ValidationIssue(
                    "verification_gate_incomplete_task",
                    f"task {task_id} is {state.get('status')}",
                    f"tasks.{task_id}",
                )
            )
    return issues


def _gate_ok(
    manifest: PactManifest,
    alignment: dict[str, Any],
    gate: LifecycleGate,
    *,
    repo_root_path: Path | None = None,
    context: PactEvaluationContext | None = None,
) -> bool:
    return not _lifecycle_gate_issues(
        manifest,
        alignment,
        gate,
        repo_root_path=repo_root_path,
        context=context,
    )


def _slugify(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "-", value.strip().lower()).strip("-") or "item"


def _has_fresh_intent_review(manifest: PactManifest) -> bool:
    candidates = [
        evidence
        for evidence in manifest.evidence
        if evidence.kind == "intent_review"
        and evidence.status in PASSING_EVIDENCE
        and evidence.data.get("decision") in {"approved", "approve"}
    ]
    if not candidates:
        return False
    expected = intent_digest(manifest)
    return any(
        evidence.data.get("reviewed_digest") == expected
        for evidence in candidates
    )


def _task_evidence_ref_issues(
    *,
    task_index: int,
    task,
    manifest: PactManifest,
    evidence_by_id: dict[str, Any],
    repo_root_path: Path | None = None,
    context: PactEvaluationContext | None = None,
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    valid_refs = _task_valid_evidence_refs(manifest, task)
    for evidence_id in task.evidence:
        evidence = evidence_by_id.get(evidence_id)
        if evidence is None:
            issues.append(
                ValidationIssue(
                    "unresolved_task_evidence",
                    f"task {task.id} references unknown evidence {evidence_id}",
                    f"tasks[{task_index}].evidence",
                )
            )
            continue
        if evidence.status not in PASSING_EVIDENCE:
            issues.append(
                ValidationIssue(
                    "task_evidence_not_passing",
                    f"task {task.id} references {evidence_id} with status {evidence.status}",
                    f"tasks[{task_index}].evidence",
                )
            )
        if evidence.status in PASSING_EVIDENCE and not _evidence_ok_for_alignment(
            manifest,
            evidence,
            repo_root_path=repo_root_path,
            context=context,
        ):
            if evidence.kind in FORMAL_KINDS:
                code = "task_evidence_stale_formal"
                message = (
                    f"task {task.id} references stale formal evidence {evidence_id}"
                )
            else:
                code = "task_evidence_invalid_proof"
                message = (
                    f"task {task.id} references evidence {evidence_id} "
                    "that is not valid proof"
                )
            issues.append(
                ValidationIssue(
                    code,
                    message,
                    f"tasks[{task_index}].evidence",
                )
            )
        if not set(evidence.refs) & valid_refs:
            issues.append(
                ValidationIssue(
                    "task_evidence_scope_mismatch",
                    f"task {task.id} references {evidence_id} outside its scope",
                    f"tasks[{task_index}].evidence",
                )
            )
    return issues


def _task_evidence_alignment(
    manifest: PactManifest,
    task,
    *,
    repo_root_path: Path | None = None,
    context: PactEvaluationContext | None = None,
) -> list[str]:
    if _target_language_version(manifest) < 1:
        return []
    evidence_by_id = {evidence.id: evidence for evidence in manifest.evidence}
    valid_refs = _task_valid_evidence_refs(manifest, task)
    missing: list[str] = []
    for evidence_id in task.evidence:
        evidence = evidence_by_id.get(evidence_id)
        if evidence is None:
            missing.append(f"unresolved_evidence:{evidence_id}")
            continue
        if evidence.status not in PASSING_EVIDENCE:
            missing.append(f"failing_evidence:{evidence_id}:{evidence.status}")
        if evidence.status in PASSING_EVIDENCE and not _evidence_ok_for_alignment(
            manifest,
            evidence,
            repo_root_path=repo_root_path,
            context=context,
        ):
            prefix = (
                "stale_formal_evidence"
                if evidence.kind in FORMAL_KINDS
                else "invalid_proof"
            )
            missing.append(f"{prefix}:{evidence_id}")
        if not set(evidence.refs) & valid_refs:
            missing.append(f"scope_mismatch:{evidence_id}")
    return missing


def task_proof_obligation(manifest: PactManifest, task) -> list[str]:
    """The `(requirement, evidence kind)` pairs closing this task would check.

    The completion checks all quantify over this set, so an EMPTY result is the
    vacuity: `gate task-done` would report `missing_proof: []` and pass while
    having checked nothing (pact-vacuous-gate-close req.vacuous.presence). It
    is derived from the DECLARED policy, not from what is currently missing —
    "there is a proof obligation" and "the obligation is met" are different
    questions and only the first one can be vacuous.
    """
    req_by_id = {req.id: req for req in manifest.requirements}
    obligations: list[str] = []
    for req_id in task.requirements:
        req = req_by_id.get(req_id)
        if req is None:
            continue
        for kind in (
            *req.evidence_policy.required,
            *req.evidence_policy.required_any,
        ):
            if kind:
                obligations.append(f"{req_id}:{kind}")
    return obligations


def _task_valid_evidence_refs(manifest: PactManifest, task) -> set[str]:
    refs = {task.id}
    req_ids = set(task.requirements)
    refs |= req_ids
    for req in manifest.requirements:
        if req.id in req_ids:
            refs.update(acceptance.id for acceptance in req.acceptance)
    refs.update(test.id for test in manifest.test_plan if test.requirement in refs)
    refs.update(
        binding.id for binding in manifest.formal if binding.requirement in req_ids
    )
    return refs


def _environment_blockers(
    task,
    *,
    context: PactEvaluationContext | None = None,
) -> list[dict[str, str]]:
    if task.status == "done":
        return []
    blockers: list[dict[str, str]] = []
    for check in task.environment:
        if not check.required:
            continue
        if check.kind == "env_var":
            name = check.env or check.name or check.id
            captured_truth = (
                context.env_is_truthy(name) if context is not None else None
            )
            truthy = (
                bool(os.environ.get(name)) if captured_truth is None else captured_truth
            )
            if not truthy:
                blockers.append(
                    {
                        "id": check.id,
                        "kind": check.kind,
                        "name": name,
                        "message": f"environment variable {name} is not set",
                    }
                )
    return blockers


def _target_language_version(manifest: PactManifest) -> int:
    if manifest.target_pact_version is not None:
        return manifest.target_pact_version
    raw = manifest.prose.get("target_pact_version")
    if raw is None:
        return manifest.pact_version
    try:
        return int(raw)
    except ValueError:
        return manifest.pact_version


def _has_evidence(
    manifest: PactManifest,
    requirement_id: str,
    kinds: set[str],
    *,
    repo_root_path: Path | None = None,
    context: PactEvaluationContext | None = None,
    registry: EvidenceKindRegistry | None = None,
) -> bool:
    registry = registry or _kind_registry(repo_root_path, context=context)
    acceptance_ids = {
        a.id
        for req in manifest.requirements
        if req.id == requirement_id
        for a in req.acceptance
    }
    refs = {requirement_id} | acceptance_ids
    if kinds & FORMAL_KINDS:
        refs.update(
            binding.id
            for binding in manifest.formal
            if binding.requirement == requirement_id
        )
    return any(
        _evidence_ok_for_alignment(
            manifest,
            evidence,
            repo_root_path=repo_root_path,
            context=context,
            registry=registry,
        )
        and evidence.kind in kinds
        and bool(set(evidence.refs) & refs)
        for evidence in manifest.evidence
    )


def _waiver_for(manifest: PactManifest, scope: str) -> str | None:
    for waiver in manifest.waivers:
        if waiver.scope == scope and waiver.owner:
            return waiver.id
    return None


def _has_lifecycle_bypass(manifest: PactManifest, scope: str) -> bool:
    accepted_scopes = {scope, "lifecycle.bypass", "lifecycle.verification"}
    return any(
        waiver.scope in accepted_scopes and bool(waiver.owner)
        for waiver in manifest.waivers
    )


def _check_unique(
    values: set[str],
    expected_len: int,
    label: str,
    issues: list[ValidationIssue],
) -> None:
    if len(values) != expected_len:
        issues.append(
            ValidationIssue(
                "duplicate_id",
                f"{label} contain duplicate ids",
                label,
            )
        )


def _cycle_issues(graph: dict[str, list[str]]) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    visiting: set[str] = set()
    visited: set[str] = set()

    def dfs(node: str, path: list[str]) -> None:
        if node in visiting:
            cycle = path[path.index(node) :] if node in path else path
            issues.append(
                ValidationIssue(
                    "dependency_cycle",
                    "unwaived task dependency cycle: " + " -> ".join(cycle + [node]),
                    "dependencies",
                )
            )
            return
        if node in visited:
            return
        visiting.add(node)
        for child in graph.get(node, []):
            dfs(child, path + [child])
        visiting.remove(node)
        visited.add(node)

    for node in graph:
        dfs(node, [node])
    return issues
