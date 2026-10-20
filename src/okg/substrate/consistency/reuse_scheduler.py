"""The resolution-aware reuse scheduler — worklist to a fixed point (spec §9.6).

PACT ``consistency-resolution-aware-reuse``, task.reuse.scheduler
(req.reuse.protocol + req.reuse.single-mechanism). Normative semantics in
docs/develop/architecture/consistency.md §9.6 (update protocol + fallback ladder).

What this module is
-------------------
The update protocol. Given a worklist of *reached* steps inside the resolved
state's dependency cone, it processes them to a **fixed point**: each reached
step either RUNS (its observed outputs extend the worklist with dependents) or
SKIPS (behind a certificate — the only skip mechanism, §9.5). Commit of the
maintenance state happens only at that fixed point (:func:`commit_gate`).

The decision ladder for one reached step (§9.6, exactly as ratified):

1. a prior certificate that :func:`validate_certificate` reports **fully valid**
   → SKIP, cause ``certificate_valid``. The certificate is re-recorded for THIS
   generation with refreshed dependency facts (a valid prior stayed valid, so
   the refreshed facts equal the recorded ones — the row is byte-stable bar its
   generation label);
2. ELSE, if the step's current dependency facts are **byte-identical to the
   prior generation's** (``prior_facts``, the manifest-hash / carry-forward
   condition — all ten classes match) → write a DEGENERATE certificate (the
   trivial unchanged-inputs comparison) and SKIP, cause
   ``certificate_degenerate_unchanged``. This is the single re-expression of
   today's manifest-hash carry-forward: it no longer bypasses the certificate
   mechanism (§9.5). Its invalidation is identical to any certificate's —
   mutate any class and the step is reached again;
3. ELSE compile the step's :class:`PossibleEffects` from its declared envelope;
   :class:`RunAlways` or a :func:`skip_is_justifiable` refusal → RUN, cause
   ``run_always`` / ``effects_overlap``. A RUN's observed dependents extend the
   worklist;
4. ELSE (justified-and-bounded) → write a NEW certificate (comparison: the
   incumbent selection outcome+basis, the effects digest, and the competitor
   bound from the disjointness judgment) and SKIP, cause ``skip_justified``.

The three SKIP flavors (:data:`SKIP_CAUSES`) are distinct members of the cause
taxonomy so accounting can tell a revalidated certificate, a degenerate
carry-forward, and a freshly justified changed-inputs skip apart.

The fallback ladder is closed and loud (§9.6):

* a reached step with **no declared envelope** is an *unknown dependency* → the
  tentative pass is DISCARDED and the complete path executes (cause
  ``unknown_dependency``);
* a step whose producer carries a well-typed **out-of-envelope** output (an
  ``okg.envelope_violations`` poisoning row for this generation) → DISCARD and
  complete execution (cause ``envelope_violation``);
* an **operator failure** (a step that raises when run) → ABORT the update
  leaving prior state intact (cause ``operator_failure``).

Every outcome carries EXACTLY one cause from :data:`FALLBACK_CAUSES`; an outcome
without a cause is structurally impossible (the ``StepOutcome`` constructor
refuses). No path substitutes a default value for a failed computation and no
branch is silent.

Scope (v1, the CLAIM-PLANE CONE)
--------------------------------
The v1 scheduler governs the steps whose outputs feed the resolved state — the
claim-producing steps, the claim derivations, and repair selection. Artifacts
OUTSIDE that cone are enumerated in :data:`OUTSIDE_CONE` with a one-line reason
each and are NOT scheduled by v1 (the single-mechanism requirement demands the
enumeration; the ``consistency-single-engine`` child audits it).

Durability
----------
This module is deterministic from its inputs: the same worklist seed, envelopes,
prior certificates, changed terms, and (deterministic) step runner yield the same
:class:`ReusePassResult` — byte-equal :meth:`ReusePassResult.digest`. That is
what lets the pass ride inside the publisher DBOS workflow as a resumable step
(:mod:`okg.substrate.runtime.reuse_pass_workflow`): a step-boundary re-execution
after a crash reaches the same decisions, so the commit is transactional with the
generation checkpoint — never a half-commit. Recording a skip goes through
:meth:`ReuseCertificateStore.record_skip` on the caller's connection, so the
certificate rows commit atomically with the maintenance state they justify (§9.6).
"""
from __future__ import annotations

import hashlib
import json
import logging
from collections import deque
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Callable, Mapping, Sequence

from okg.substrate.consistency.claim_model.precedence import SIGNAL_SOURCE_AUTHORITY
from okg.substrate.consistency.possible_effects import (
    REFUSE_BUCKET_OVERLAP,
    REFUSE_UNSTATED_CHANGE,
    UNSTATED_CHANGE,
    ChangeStatement,
    PossibleEffects,
    RunAlways,
    compile_possible_effects,
    skip_is_justifiable,
)
from okg.substrate.consistency.reuse_certificates import (
    COMPARISON_KIND_DEGENERATE,
    COMPARISON_KIND_RESOLUTION,
    DEPENDENCY_CLASSES,
    RESOLUTION_V1_FACT_COVERAGE,
    CertificateDependencies,
    ReuseCertificate,
    ReuseCertificateStore,
    dependency_classes_differ,
    validate_certificate,
)
from okg.substrate.errors import SubstrateError

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# The outside-cone enumeration (single-mechanism requirement).                #
# --------------------------------------------------------------------------- #
# Artifact families the v1 scheduler does NOT govern, each with a one-line
# reason. The reuse decision path (certificates) covers ONLY the resolved
# state's dependency cone; these live outside it and keep their existing
# scheduling. Enumerated here (not implied) so consistency-single-engine's audit
# can assert the boundary. The scheduler REFUSES to schedule a step whose family
# is declared outside the cone would be a category error — instead a reached step
# with no envelope is treated as an unknown dependency and discards loudly.
OUTSIDE_CONE: dict[str, str] = {
    "sources": (
        "raw source ingest emits facts UPSTREAM of the claim plane; it feeds the "
        "cone's inputs but is not part of the resolved state"
    ),
    "generic_enrichers": (
        "non-claim enrichers (embeddings, alias links, rollups' inputs) write "
        "graph artifacts that do not feed effective-merge / resolution / selection"
    ),
    "native_views": (
        "materialized read-side views are projections OF the resolved state, not "
        "producers of it; they refresh on their own coalescing policy"
    ),
    "rollups": (
        "ontology rollups aggregate the published graph downstream of resolution; "
        "they consume the cone's output, they do not decide it"
    ),
    "embeddings": (
        "embedding population is background read-side enrichment, never a term in "
        "the resolved claim state"
    ),
}


# --------------------------------------------------------------------------- #
# The cause vocabulary (declared; the accounting child formalizes it).        #
# --------------------------------------------------------------------------- #
CAUSE_CERTIFICATE_VALID = "certificate_valid"     # SKIP: valid prior certificate
CAUSE_CERTIFICATE_DEGENERATE_UNCHANGED = (         # SKIP: unchanged-inputs carry-forward
    "certificate_degenerate_unchanged"
)
CAUSE_SKIP_JUSTIFIED = "skip_justified"           # SKIP: fresh justified (disjoint) certificate
CAUSE_SKIP_DOMINANCE = "skip_dominance"           # SKIP: bounded-dominance certificate
CAUSE_EFFECTS_OVERLAP = "effects_overlap"         # RUN: possible-effects overlap
CAUSE_RUN_ALWAYS = "run_always"                   # RUN: unbounded envelope
CAUSE_UNSTATED_CHANGE = "unstated_change"         # RUN: no change statement made
CAUSE_RUNG_DISABLED = "rung_disabled"             # RUN: an ABLATED rung would have skipped
CAUSE_UNKNOWN_DEPENDENCY = "unknown_dependency"   # DISCARD: no declared envelope
CAUSE_ENVELOPE_VIOLATION = "envelope_violation"   # DISCARD: poisoning violation
CAUSE_OPERATOR_FAILURE = "operator_failure"       # ABORT: a step raised

# The four SKIP flavors — each a distinct member so accounting can tell them
# apart: a revalidated prior certificate, the degenerate unchanged-inputs
# carry-forward, a freshly justified changed-inputs skip on a DISJOINT reach, and
# a bounded-DOMINANCE skip (the changed input overlaps the incumbent bucket but
# every permitted output provably loses to the decisive incumbent) — §9.5 / §9.6.
SKIP_CAUSES: frozenset[str] = frozenset({
    CAUSE_CERTIFICATE_VALID,
    CAUSE_CERTIFICATE_DEGENERATE_UNCHANGED,
    CAUSE_SKIP_JUSTIFIED,
    CAUSE_SKIP_DOMINANCE,
})

# The closed cause taxonomy. Every StepOutcome carries exactly one member; an
# outcome without a cause is refused. consistency-maintenance-accounting
# formalizes the ledger over this vocabulary.
FALLBACK_CAUSES: frozenset[str] = frozenset({
    CAUSE_CERTIFICATE_VALID,
    CAUSE_CERTIFICATE_DEGENERATE_UNCHANGED,
    CAUSE_SKIP_JUSTIFIED,
    CAUSE_SKIP_DOMINANCE,
    CAUSE_EFFECTS_OVERLAP,
    CAUSE_RUN_ALWAYS,
    CAUSE_UNSTATED_CHANGE,
    CAUSE_RUNG_DISABLED,
    CAUSE_UNKNOWN_DEPENDENCY,
    CAUSE_ENVELOPE_VIOLATION,
    CAUSE_OPERATOR_FAILURE,
})

# Outcome actions.
ACTION_SKIP = "skip"
ACTION_RUN = "run"

# Terminal pass states.
TERMINAL_FIXED_POINT = "fixed_point"   # worklist drained; commit may proceed
TERMINAL_DISCARD = "discard"           # tentative state dropped; complete path runs
TERMINAL_ABORT = "abort"               # update aborted; prior state intact

# The causes that terminate the pass into the complete path (discard) versus the
# abort path — used by the integration to pick the loud fallback.
DISCARD_CAUSES: frozenset[str] = frozenset({
    CAUSE_UNKNOWN_DEPENDENCY, CAUSE_ENVELOPE_VIOLATION,
})
ABORT_CAUSES: frozenset[str] = frozenset({CAUSE_OPERATOR_FAILURE})


# --------------------------------------------------------------------------- #
# The reuse LADDER as named, independently-disablable rungs (E3 ablations).     #
# --------------------------------------------------------------------------- #
#: The unchanged-inputs certificate — the degenerate "nothing changed"
#: carry-forward rung (the ``a2`` block of :meth:`ReusePass.step`).
RUNG_CERTIFICATE_UNCHANGED = "certificate_unchanged"
#: The effect-bound rung — :func:`skip_is_justifiable` over the compiled possible
#: effects (the ``c`` block; a proven-DISJOINT reach licenses the skip).
RUNG_EFFECT_BOUND = "effect_bound"
#: The bounded-dominance rung — :meth:`ReusePass._try_dominance` (the ``b2``
#: block; the CENTREPIECE changed-inputs skip).
RUNG_BOUNDED_DOMINANCE = "bounded_dominance"

#: The three shipped rungs, in ladder order. Revalidating a still-VALID prior
#: certificate (:data:`CAUSE_CERTIFICATE_VALID`) is deliberately NOT a rung: it
#: re-uses a certificate an EARLIER pass's rung already earned, so ablating it
#: would remove the durability of the ladder rather than one of its mechanisms.
REUSE_RUNGS: tuple[str, ...] = (
    RUNG_CERTIFICATE_UNCHANGED, RUNG_EFFECT_BOUND, RUNG_BOUNDED_DOMINANCE,
)

#: The cause each rung stamps on the skip it approves — the ONLY observable
#: signature of a rung having fired. An ablation is asserted on the recorded
#: cause tally through this map, never on the configuration flag: a switch that
#: is set but does not change behaviour would pass a flag assertion and fail
#: this one.
RUNG_SKIP_CAUSE: Mapping[str, str] = MappingProxyType({
    RUNG_CERTIFICATE_UNCHANGED: CAUSE_CERTIFICATE_DEGENERATE_UNCHANGED,
    RUNG_EFFECT_BOUND: CAUSE_SKIP_JUSTIFIED,
    RUNG_BOUNDED_DOMINANCE: CAUSE_SKIP_DOMINANCE,
})


@dataclass(frozen=True)
class ReuseRungConfig:
    """WHICH reuse rungs this pass may use — a recorded part of the run config.

    The E3 mechanism ablation (PACT ``concord-e3-mechanism-ablations``) reruns one
    workload with ONE rung disabled at a time to attribute the saving per rung.
    That switch is deliberately a first-class CONSTRUCTOR input carried into
    :class:`ReusePassResult` and its canonical dict / digest — NOT an environment
    variable read at a call site. A reader of a recorded ablation therefore sees
    which rungs were active in the record itself, without re-deriving it from the
    code that produced the run, and two passes over identical inputs under
    DIFFERENT rung configurations cannot collide on a digest.

    ``active`` is validated against :data:`REUSE_RUNGS` on construction: an
    unknown rung name RAISES rather than being ignored, so a typo'd ablation
    cannot silently run the complete ladder and report itself as an arm."""

    active: frozenset[str] = frozenset(REUSE_RUNGS)

    def __post_init__(self) -> None:
        unknown = sorted(set(self.active) - set(REUSE_RUNGS))
        if unknown:
            raise SubstrateError(
                "ReuseRungConfig names an unknown reuse rung; an unrecognized rung "
                "is refused rather than ignored (a silently-dropped name would run "
                "the complete ladder and report itself as an ablation arm)",
                data={"unknown": unknown, "known": list(REUSE_RUNGS)},
            )
        object.__setattr__(self, "active", frozenset(self.active))

    # -- constructors ------------------------------------------------------- #
    @classmethod
    def all_enabled(cls) -> "ReuseRungConfig":
        """Every shipped rung active — the production ladder (the default)."""
        return cls(active=frozenset(REUSE_RUNGS))

    @classmethod
    def none_enabled(cls) -> "ReuseRungConfig":
        """No rung active — the degenerate arm that bounds the total saving from
        above. The pass still terminates and still returns the complete-processing
        answer; it just proves nothing skippable."""
        return cls(active=frozenset())

    @classmethod
    def without(cls, *rungs: str) -> "ReuseRungConfig":
        """The complete ladder MINUS ``rungs`` — one ablation arm."""
        unknown = sorted(set(rungs) - set(REUSE_RUNGS))
        if unknown:
            raise SubstrateError(
                "ReuseRungConfig.without names an unknown reuse rung",
                data={"unknown": unknown, "known": list(REUSE_RUNGS)},
            )
        return cls(active=frozenset(REUSE_RUNGS) - set(rungs))

    # -- observation -------------------------------------------------------- #
    def enabled(self, rung: str) -> bool:
        if rung not in REUSE_RUNGS:
            raise SubstrateError(
                "ReuseRungConfig.enabled asked about an unknown reuse rung",
                data={"rung": rung, "known": list(REUSE_RUNGS)},
            )
        return rung in self.active

    @property
    def disabled(self) -> frozenset[str]:
        return frozenset(REUSE_RUNGS) - self.active

    @property
    def is_complete_ladder(self) -> bool:
        return self.active == frozenset(REUSE_RUNGS)

    @property
    def arm_label(self) -> str:
        """A stable, human-readable arm name for a recorded ablation table."""
        if self.is_complete_ladder:
            return "all_rungs"
        if not self.active:
            return "no_rungs"
        return "minus_" + "+".join(sorted(self.disabled))

    def suppressed_skip_causes(self) -> frozenset[str]:
        """The skip causes that MUST NOT appear in this pass's cause tally."""
        return frozenset(RUNG_SKIP_CAUSE[r] for r in self.disabled)

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "arm": self.arm_label,
            "rungs_active": sorted(self.active),
            "rungs_disabled": sorted(self.disabled),
            "suppressed_skip_causes": sorted(self.suppressed_skip_causes()),
        }


def _canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


class CommitBeforeFixedPoint(SubstrateError):
    """The commit gate refused: the worklist has not reached a fixed point.

    Raised by :func:`commit_gate` when an integration tries to proceed to the
    generation checkpoint while reached steps remain unprocessed (§9.6 — commit
    happens only at the fixed point). A discarded or aborted pass is a DIFFERENT
    terminal state handled by the fallback ladder, not by the commit gate."""


# --------------------------------------------------------------------------- #
# The reached step.                                                           #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ReuseStep:
    """One step in the cone: its identity, producer, and declared envelope.

    ``envelope`` is the producer's declared :class:`WriteEnvelope`; ``None`` means
    the step is reached with NO declared write surface — an unknown dependency
    that discards the pass into complete execution (§9.6). ``producer_id`` keys
    the envelope-violation check (a poisoning row is per producer); it defaults to
    ``step_id``. ``claim_predicates`` are the predicates the step writes claims
    under (a declaration the compiler consumes, never inferred from output)."""

    step_id: str
    envelope: Any  # WriteEnvelope | None
    producer_id: str = ""
    claim_predicates: tuple[str, ...] = ()

    def resolved_producer_id(self) -> str:
        return self.producer_id or self.step_id


# --------------------------------------------------------------------------- #
# One decided outcome.                                                         #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class StepOutcome:
    """The decided outcome for one reached step: action + a single cause.

    ``action`` is ``skip`` or ``run``; ``cause`` is exactly one member of
    :data:`FALLBACK_CAUSES` (the constructor refuses anything else — an outcome
    without a declared cause is structurally impossible). ``certificate_digest``
    is present for a SKIP (a skip is always behind a certificate) and ``None`` for
    a RUN. ``detail`` records subordinate facts (the invalidated dependency
    classes, the overlapping buckets, the discovered dependents) for the report;
    it never carries the cause."""

    step_id: str
    action: str
    cause: str
    certificate_digest: str | None = None
    detail: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.action not in (ACTION_SKIP, ACTION_RUN):
            raise SubstrateError(
                f"StepOutcome.action must be {ACTION_SKIP!r} or {ACTION_RUN!r}",
                data={"got": self.action},
            )
        if self.cause not in FALLBACK_CAUSES:
            raise SubstrateError(
                "StepOutcome.cause must be a declared FALLBACK_CAUSES member — an "
                "outcome without a cause is structurally impossible (spec §9.6)",
                data={"got": self.cause, "allowed": sorted(FALLBACK_CAUSES)},
            )
        if self.action == ACTION_SKIP and not self.certificate_digest:
            raise SubstrateError(
                "a SKIP outcome must carry a certificate digest — the certificate "
                "is the only skip mechanism on the cone (spec §9.5)",
                data={"step_id": self.step_id, "cause": self.cause},
            )
        if self.action == ACTION_RUN and self.certificate_digest is not None:
            raise SubstrateError(
                "a RUN outcome carries no certificate (the step recomputes)",
                data={"step_id": self.step_id},
            )

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "step_id": self.step_id,
            "action": self.action,
            "cause": self.cause,
            "certificate_digest": self.certificate_digest,
            "detail": dict(self.detail),
        }


# --------------------------------------------------------------------------- #
# The pass result.                                                            #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ReusePassResult:
    """The decided outcomes of one reuse pass + its terminal state (§9.6).

    ``terminal`` is one of :data:`TERMINAL_FIXED_POINT` (the worklist drained and
    the commit may proceed), :data:`TERMINAL_DISCARD` (an unknown dependency or an
    envelope violation dropped the tentative pass into the complete path), or
    :data:`TERMINAL_ABORT` (an operator failure aborted the update, prior state
    intact). ``fallback_cause`` is the cause that terminated a discard/abort (a
    member of :data:`FALLBACK_CAUSES`), ``None`` at a clean fixed point.

    ``rungs`` records WHICH reuse rungs the pass was allowed to use (E3 mechanism
    ablation). It is part of the record, not a side channel: a reader of a stored
    result can tell an ablation arm from the production ladder without inspecting
    the code that produced it.

    :meth:`digest` is byte-stable over the deployment, generation, terminal state,
    fallback cause, the active rung set, and the (step-sorted) outcomes — so two
    runs from the same inputs share a digest (the durability property the DBOS
    resume rests on) while two runs under DIFFERENT rung configurations cannot."""

    deployment: str
    generation_id: str
    terminal: str
    outcomes: tuple[StepOutcome, ...]
    fallback_cause: str | None = None
    failed_step: str | None = None
    pending: tuple[str, ...] = ()
    detail: Mapping[str, Any] = field(default_factory=dict)
    rungs: ReuseRungConfig = field(default_factory=ReuseRungConfig.all_enabled)

    @property
    def reached_fixed_point(self) -> bool:
        return self.terminal == TERMINAL_FIXED_POINT

    @property
    def discarded(self) -> bool:
        return self.terminal == TERMINAL_DISCARD

    @property
    def aborted(self) -> bool:
        return self.terminal == TERMINAL_ABORT

    @property
    def skips(self) -> tuple[StepOutcome, ...]:
        return tuple(o for o in self.outcomes if o.action == ACTION_SKIP)

    @property
    def runs(self) -> tuple[StepOutcome, ...]:
        return tuple(o for o in self.outcomes if o.action == ACTION_RUN)

    def cause_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for outcome in self.outcomes:
            counts[outcome.cause] = counts.get(outcome.cause, 0) + 1
        if self.fallback_cause is not None:
            counts[self.fallback_cause] = counts.get(self.fallback_cause, 0) + 1
        return counts

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "deployment": self.deployment,
            "generation_id": self.generation_id,
            "terminal": self.terminal,
            "fallback_cause": self.fallback_cause,
            "failed_step": self.failed_step,
            "pending": sorted(self.pending),
            "rungs": self.rungs.to_canonical_dict(),
            "outcomes": [
                o.to_canonical_dict()
                for o in sorted(self.outcomes, key=lambda o: o.step_id)
            ],
        }

    def rung_ablation_violations(self) -> dict[str, int]:
        """Skips that carry a DISABLED rung's cause — must always be empty.

        The acceptance an ablation stands on (req.e3.rungs-independently-disablable)
        read straight off the recorded tally: with a rung disabled, NO skip in the
        run may carry that rung's cause. A non-empty result means the switch was
        set and the behaviour did not change."""
        suppressed = self.rungs.suppressed_skip_causes()
        counts = self.cause_counts()
        return {c: counts[c] for c in sorted(suppressed) if counts.get(c)}

    def canonical_json(self) -> str:
        return _canonical_json(self.to_canonical_dict())

    def digest(self) -> str:
        return hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------- #
# Comparison + certificate construction.                                      #
# --------------------------------------------------------------------------- #
def build_skip_comparison(
    *,
    incumbent_outcome: str,
    incumbent_basis: str,
    effects: PossibleEffects,
    competitor_bound: Mapping[str, Any],
) -> dict[str, Any]:
    """The §9.5 comparison object for a freshly justified (changed-inputs) skip.

    Carries the incumbent decision's recorded outcome + basis (from the recorded
    selection), the possible-effects summary digest, and the competitor-bound
    statement the disjointness judgment established. ``tie_break`` is ``None`` —
    v1 disjointness decides without a score tie-break; the key is present so the
    certificate's required-key contract is met without a defaulted value. The
    ``kind`` is stamped ``resolution_dominance`` so every comparison declares its
    justification shape explicitly."""
    return {
        "kind": COMPARISON_KIND_RESOLUTION,
        "incumbent_outcome": incumbent_outcome,
        "incumbent_basis": incumbent_basis,
        "effects_summary_digest": effects.digest(),
        "competitor_bound": dict(competitor_bound),
        "tie_break": None,
        # Four-fact contract (§9.5, adjudication v2 F3): which premise
        # argues each harmlessness fact. Shared with the validator.
        "fact_coverage": dict(RESOLUTION_V1_FACT_COVERAGE),
    }


def build_degenerate_comparison(
    *,
    incumbent_outcome: str,
    incumbent_basis: str,
) -> dict[str, Any]:
    """The §9.5 comparison object for a DEGENERATE (unchanged-inputs) skip.

    The justification is trivial — every recorded dependency class is
    byte-identical to the prior generation's, so no permitted output can differ
    because no input differs. The comparison declares the
    ``degenerate_unchanged_inputs`` kind and the incumbent (carried-forward)
    outcome + basis, and states the unchanged-inputs justification enumerating
    the dependency classes that matched. It carries NO generation-varying value,
    so a step's degenerate certificate is byte-stable across generations (the
    prior generation id is recorded on the step OUTCOME's detail for the audit
    trail, not folded into the content-addressed certificate). This is a distinct
    schema, validated as loudly as the resolution one — never a looser shape."""
    return {
        "kind": COMPARISON_KIND_DEGENERATE,
        "incumbent_outcome": incumbent_outcome,
        "incumbent_basis": incumbent_basis,
        "unchanged_inputs": {
            "statement": (
                "all recorded dependency classes are byte-identical to the prior "
                "generation's; no permitted output can differ because no input "
                "differs (§9.5, degenerate certificate)"
            ),
            "dependency_classes_matched": list(DEPENDENCY_CLASSES),
        },
    }


# --------------------------------------------------------------------------- #
# The bounded-dominance rung — the CENTREPIECE changed-inputs skip (§9.5/§9.7). #
# --------------------------------------------------------------------------- #
# possible_effects.skip_is_justifiable answers ONLY set-disjointness ("could this
# step touch anything the change touched"); it explicitly excludes scoring bounds.
# The bounded-dominance rung lives HERE, in the scheduler / certificate layer that
# possible_effects.py names as its owner: it rescues a pure BUCKET-OVERLAP refusal
# when the step's envelope provably constrains EVERY permitted output to a source
# class whose authority signal loses to the decisive incumbent — the
# incumbent-worst vs competitor-best comparison the resolution_dominance
# certificate was shaped for (reuse_certificates §9.5 part (a)).
#
# It is CONSERVATIVE-OR-REFUSE. Dominance is claimable ONLY when, for EVERY bucket
# the change overlaps, the recorded incumbent won DECISIVELY by declared source
# authority AND the step's declared competitor bound proves no permitted output
# can outrank it — no higher-precedence signal escape, and a strictly lower
# authority ceiling. Anything the analysis cannot prove refuses and the step runs
# (running is never wrong). Typed reasons, never prose the scheduler parses.
DOMINANCE_APPROVED = "dominance"
REFUSE_DOMINANCE_BASIS = "dominance_incumbent_basis_not_source_authority"
REFUSE_DOMINANCE_UNBOUNDED = "dominance_competitor_authority_unbounded"
REFUSE_DOMINANCE_AUTHORITY = "dominance_competitor_authority_not_below_incumbent"
REFUSE_DOMINANCE_HIGHER_SIGNAL = "dominance_competitor_can_win_higher_signal"


@dataclass(frozen=True)
class IncumbentDecision:
    """The recorded decisive incumbent for one bucket the change overlaps.

    ``basis`` is the recorded deciding signal (the ≻ signal class the projection
    persisted — dominance requires it to be
    :data:`~okg.substrate.consistency.claim_model.precedence.SIGNAL_SOURCE_AUTHORITY`,
    ``"declared_source_authority"``); ``authority`` is the winning claim's declared
    source-authority rank (higher wins). ``outcome`` is the recorded selection
    outcome (``"selected"``) recorded onto the certificate comparison."""

    bucket: tuple[str, str]   # (predicate, normalized_key)
    outcome: str
    basis: str
    authority: int

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "bucket": list(self.bucket),
            "outcome": self.outcome,
            "basis": self.basis,
            "authority": self.authority,
        }


@dataclass(frozen=True)
class CompetitorBound:
    """The bound EVERY permitted output of a step carries, from its declared
    envelope (§9.3) — the competitor-best side of the dominance comparison.

    ``source_classes`` is the closed source-class vocabulary the envelope
    constrains outputs to; ``authority_max`` is the highest declared source
    authority any permitted output could carry, or ``None`` when the envelope does
    NOT provably bound it (unbounded authority refuses dominance — a competitor
    that could carry any authority might outrank the incumbent). The three
    ``can_win_*`` / ``precedent_preferred`` flags record whether a permitted output
    could win a signal ABOVE declared source authority in the ≻ order
    (operator precedent, explicit supersession, claim-local effective date); any
    True refuses (a higher-precedence escape defeats the authority argument).
    Derived from declarations ONLY — never from observed output."""

    source_classes: tuple[str, ...]
    authority_max: int | None
    can_win_supersession: bool = False
    can_win_effective_date: bool = False
    precedent_preferred: bool = False

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "source_classes": sorted(self.source_classes),
            "authority_max": self.authority_max,
            "can_win_supersession": self.can_win_supersession,
            "can_win_effective_date": self.can_win_effective_date,
            "precedent_preferred": self.precedent_preferred,
        }


@dataclass(frozen=True)
class DominanceInput:
    """Per-step inputs for the bounded-dominance rung.

    ``incumbents`` maps EVERY bucket the step could overlap to its decisive
    incumbent decision; ``competitor_bound`` is the bound the step's envelope
    declares. The rung approves a skip ONLY when every overlapped bucket has an
    incumbent that :func:`dominance_is_justifiable` approves against the bound."""

    incumbents: Mapping[tuple[str, str], IncumbentDecision]
    competitor_bound: CompetitorBound


@dataclass(frozen=True)
class DominanceJudgment:
    """The typed verdict of :func:`dominance_is_justifiable`.

    ``approved`` with reason :data:`DOMINANCE_APPROVED`, or refused with one of the
    ``REFUSE_DOMINANCE_*`` tokens. ``detail`` carries the incumbent-worst vs
    competitor-best numbers for the recorded comparison."""

    approved: bool
    reason: str
    detail: Mapping[str, Any] = field(default_factory=dict)

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "approved": self.approved,
            "reason": self.reason,
            "detail": dict(self.detail),
        }


def dominance_is_justifiable(
    incumbent: IncumbentDecision, competitor_bound: CompetitorBound
) -> DominanceJudgment:
    """Approve a bounded-dominance skip for ONE bucket, conservative-or-refuse.

    Dominance holds iff, for the overlapped bucket, the recorded incumbent won
    DECISIVELY by declared source authority AND the step's competitor bound proves
    every permitted output loses to it:

    * the incumbent basis is ``declared_source_authority``
      (:data:`SIGNAL_SOURCE_AUTHORITY`) — the ONLY decisive signal this v1 rung
      reasons about; any other basis refuses;
    * the competitor authority is provably BOUNDED (a finite envelope vocabulary);
      an unbounded ceiling refuses (it might reach the incumbent);
    * that ceiling is STRICTLY below the incumbent's winning authority — a tie
      cannot be settled by authority, so ``>=`` refuses;
    * no permitted output can win a signal ABOVE source authority in ≻ (operator
      precedent / explicit supersession / claim-local effective date) — any such
      escape refuses.

    Refusal is always sound (running the step is never wrong). Pure."""
    if incumbent.basis != SIGNAL_SOURCE_AUTHORITY:
        return DominanceJudgment(
            approved=False, reason=REFUSE_DOMINANCE_BASIS,
            detail={"incumbent_basis": incumbent.basis,
                    "required_basis": SIGNAL_SOURCE_AUTHORITY},
        )
    if competitor_bound.authority_max is None:
        return DominanceJudgment(
            approved=False, reason=REFUSE_DOMINANCE_UNBOUNDED,
            detail={"reason": "envelope does not bound the output source authority"},
        )
    if (
        competitor_bound.can_win_supersession
        or competitor_bound.can_win_effective_date
        or competitor_bound.precedent_preferred
    ):
        return DominanceJudgment(
            approved=False, reason=REFUSE_DOMINANCE_HIGHER_SIGNAL,
            detail={
                "can_win_supersession": competitor_bound.can_win_supersession,
                "can_win_effective_date": competitor_bound.can_win_effective_date,
                "precedent_preferred": competitor_bound.precedent_preferred,
            },
        )
    if competitor_bound.authority_max >= incumbent.authority:
        return DominanceJudgment(
            approved=False, reason=REFUSE_DOMINANCE_AUTHORITY,
            detail={"competitor_authority_max": competitor_bound.authority_max,
                    "incumbent_authority": incumbent.authority},
        )
    return DominanceJudgment(
        approved=True, reason=DOMINANCE_APPROVED,
        detail={
            "bucket": list(incumbent.bucket),
            "incumbent_worst": {"basis": incumbent.basis,
                                "authority": incumbent.authority},
            "competitor_best": {"source_classes": sorted(competitor_bound.source_classes),
                                "authority": competitor_bound.authority_max},
        },
    )


def derive_competitor_bound(
    envelope: Any, *, precedent_preferred: bool = False
) -> CompetitorBound | None:
    """Derive the competitor-best bound from a producer's declared envelope (§9.3).

    The bound COMES FROM THE ENVELOPE, never observed output: the closed
    ``source_class`` value vocabulary is the competitor source class; the closed
    ``source_authority`` value vocabulary's MAX is the authority ceiling. Returns
    ``None`` — no provable bound, so dominance must refuse — when the envelope is
    unbounded, declares no ``source_authority`` vocabulary, or declares a
    non-integer authority value. A declared ``is_superseded`` / ``effective_date``
    vocabulary means a permitted output could win a higher signal (the flags are
    set); ``precedent_preferred`` is supplied by the caller from the deployment's
    precedents (no precedent favors the crawl class in v1). Pure."""
    if getattr(envelope, "unbounded", ()):  # an unbounded dimension bounds nothing
        return None
    vocab = dict(getattr(envelope, "value_vocabularies", {}) or {})
    auth_vocab = vocab.get("source_authority")
    if not auth_vocab:
        return None
    try:
        authority_max = max(int(v) for v in auth_vocab)
    except (TypeError, ValueError):
        return None
    return CompetitorBound(
        source_classes=tuple(vocab.get("source_class", ())),
        authority_max=authority_max,
        can_win_supersession="is_superseded" in vocab,
        can_win_effective_date="effective_date" in vocab,
        precedent_preferred=precedent_preferred,
    )


def build_dominance_comparison(
    *,
    incumbent: IncumbentDecision,
    effects: PossibleEffects,
    competitor_bound: CompetitorBound,
    per_bucket: Sequence[tuple[tuple[str, str], IncumbentDecision, DominanceJudgment]],
) -> dict[str, Any]:
    """The §9.5 comparison object for a bounded-DOMINANCE skip (kind
    ``resolution_dominance``).

    Records the incumbent decision's WORST outcome (its decisive
    ``declared_source_authority`` basis + winning authority) against the
    competitors' BEST under every permitted output (the envelope's competitor
    bound), per overlapped bucket. Same required-key set as any resolution
    comparison — the ``competitor_bound`` key carries the richer bounded-dominance
    statement the store was shaped for (reuse_certificates §9.5). ``tie_break`` is
    ``None``: authority strictly decides, no score tie."""
    return {
        "kind": COMPARISON_KIND_RESOLUTION,
        "incumbent_outcome": incumbent.outcome,
        "incumbent_basis": incumbent.basis,
        "effects_summary_digest": effects.digest(),
        "competitor_bound": {
            "kind": "bounded_dominance",
            "statement": (
                "every permitted output of the step is bounded to a source class "
                "whose declared source authority provably loses to the decisive "
                "incumbent; no permitted output can outrank the incumbent decision "
                "(§9.5 resolution_dominance, §9.7 resolved-state equality) — "
                "incumbent-worst vs competitor-best, per overlapped bucket"
            ),
            "competitor_best": competitor_bound.to_canonical_dict(),
            "per_bucket": [
                {
                    "bucket": list(bucket),
                    "incumbent_worst": inc.to_canonical_dict(),
                    "judgment": dj.to_canonical_dict(),
                }
                for bucket, inc, dj in per_bucket
            ],
        },
        "tie_break": None,
        # Four-fact contract (§9.5, adjudication v2 F3): which premise
        # argues each harmlessness fact. Shared with the validator.
        "fact_coverage": dict(RESOLUTION_V1_FACT_COVERAGE),
    }


# --------------------------------------------------------------------------- #
# The pass — a small, deterministic worklist state machine.                   #
# --------------------------------------------------------------------------- #
class ReusePass:
    """A reuse pass over the reached-step worklist (§9.6). Drive it one
    :meth:`step` at a time (the commit gate is observable between steps) or run
    :meth:`run_to_fixed_point`.

    The state is deterministic from the constructor inputs; :meth:`result`
    renders the byte-stable :class:`ReusePassResult`. A skip is recorded through
    :meth:`ReuseCertificateStore.record_skip` on ``conn`` the moment it is
    decided (inside the caller's transaction) — so a discard/abort that the caller
    rolls back leaves no certificate rows, and a fixed-point commit lands the
    certificates atomically with the checkpoint."""

    def __init__(
        self,
        conn: Any,
        *,
        deployment: str,
        generation_id: str,
        worklist_seed: Sequence[str],
        steps: Mapping[str, ReuseStep],
        checks: Sequence[Any] = (),
        prior_certificates: Mapping[str, ReuseCertificate] | None = None,
        changed_terms: Mapping[str, ChangeStatement] | None = None,
        current_facts: Mapping[str, CertificateDependencies] | None = None,
        prior_facts: Mapping[str, CertificateDependencies] | None = None,
        prior_generation_id: str | None = None,
        incumbent_selection: Mapping[str, Mapping[str, Any]] | None = None,
        dominance_inputs: Mapping[str, DominanceInput] | None = None,
        step_runner: Callable[[str], Sequence[str]] | None = None,
        cert_store: ReuseCertificateStore | None = None,
        poisoned_producers: frozenset[str] = frozenset(),
        rungs: ReuseRungConfig | None = None,
    ) -> None:
        if not deployment:
            raise SubstrateError("ReusePass requires a deployment label")
        if not generation_id:
            raise SubstrateError("ReusePass requires a generation_id")
        self._conn = conn
        self.deployment = deployment
        self.generation_id = str(generation_id)
        self._steps = dict(steps)
        self._checks = tuple(checks)
        self._prior = dict(prior_certificates or {})
        # A step with no entry here has an UNSTATED change, never an empty one:
        # the omission is fail-closed at the justified-skip rung (see _decide).
        self._changed: dict[str, ChangeStatement] = dict(changed_terms or {})
        self._facts = dict(current_facts or {})
        self._prior_facts = dict(prior_facts or {})
        self._prior_generation_id = (
            str(prior_generation_id) if prior_generation_id is not None else None
        )
        self._incumbent = dict(incumbent_selection or {})
        self._dominance = dict(dominance_inputs or {})
        self._runner = step_runner or (lambda _step: ())
        self._store = cert_store or ReuseCertificateStore(deployment=deployment)
        self._poisoned = frozenset(poisoned_producers)
        # WHICH rungs this pass may use. Defaults to the complete production
        # ladder; an E3 ablation arm passes a reduced set. Recorded on the result.
        self.rungs = rungs if rungs is not None else ReuseRungConfig.all_enabled()

        self._pending: deque[str] = deque(dict.fromkeys(str(s) for s in worklist_seed))
        self._processed: set[str] = set()
        self._outcomes: list[StepOutcome] = []
        self.terminal: str | None = None
        self.fallback_cause: str | None = None
        self.failed_step: str | None = None
        self._detail: dict[str, Any] = {}

    # -- observable state --------------------------------------------------- #
    @property
    def pending(self) -> tuple[str, ...]:
        return tuple(self._pending)

    @property
    def outcomes(self) -> tuple[StepOutcome, ...]:
        return tuple(self._outcomes)

    @property
    def at_fixed_point(self) -> bool:
        """True once the worklist has drained with no discard/abort — the ONLY
        state in which the maintenance state may commit (§9.6)."""
        return self.terminal == TERMINAL_FIXED_POINT

    @property
    def is_terminal(self) -> bool:
        return self.terminal is not None

    # -- the loud terminal transitions -------------------------------------- #
    def _discard(self, cause: str, step_id: str, detail: Mapping[str, Any]) -> None:
        self.terminal = TERMINAL_DISCARD
        self.fallback_cause = cause
        self.failed_step = step_id
        self._detail = dict(detail)
        logger.warning(
            "reuse pass DISCARD into complete execution: deployment=%s gen=%s "
            "step=%s cause=%s %s",
            self.deployment, self.generation_id, step_id, cause, dict(detail),
        )
        self._pending.clear()

    def _abort(self, step_id: str, detail: Mapping[str, Any]) -> None:
        self.terminal = TERMINAL_ABORT
        self.fallback_cause = CAUSE_OPERATOR_FAILURE
        self.failed_step = step_id
        self._detail = dict(detail)
        logger.error(
            "reuse pass ABORT (operator failure), prior state intact: "
            "deployment=%s gen=%s step=%s %s",
            self.deployment, self.generation_id, step_id, dict(detail),
        )
        self._pending.clear()

    # -- record a skip through the ONLY skip mechanism ---------------------- #
    def _record_skip(
        self,
        step_id: str,
        comparison: Mapping[str, Any],
        dependencies: CertificateDependencies,
        cause: str,
        detail: Mapping[str, Any],
    ) -> StepOutcome:
        # Fail-closed: a DISABLED rung can never reach the one skip mechanism. The
        # gates below already return before here, so this raises only if a future
        # edit adds a skip path that forgets its rung gate — the ablation stays
        # honest structurally rather than by the discipline of each call site.
        if cause in self.rungs.suppressed_skip_causes():
            raise SubstrateError(
                "a disabled reuse rung tried to record a skip; the rung ablation "
                "must remove the mechanism, not merely label it (spec §9.5, PACT "
                "concord-e3-mechanism-ablations req.e3.rungs-independently-disablable)",
                data={"step_id": step_id, "cause": cause,
                      "rungs_active": sorted(self.rungs.active),
                      "rungs_disabled": sorted(self.rungs.disabled)},
            )
        certificate = ReuseCertificate(
            deployment=self.deployment,
            generation_id=self.generation_id,
            step_id=step_id,
            comparison=comparison,
            dependencies=dependencies,
        )
        digest = self._store.record_skip(self._conn, certificate)
        outcome = StepOutcome(
            step_id=step_id, action=ACTION_SKIP, cause=cause,
            certificate_digest=digest, detail=detail,
        )
        self._outcomes.append(outcome)
        return outcome

    # -- process exactly one reached step ----------------------------------- #
    def step(self) -> StepOutcome | None:
        """Process one reached step. Returns its :class:`StepOutcome`, or ``None``
        when the worklist is empty (the pass has reached a fixed point) or the
        pass is already terminal. A discard/abort is recorded on the pass state
        (``terminal`` / ``fallback_cause``) rather than as a step outcome."""
        if self.is_terminal:
            return None
        if not self._pending:
            self.terminal = TERMINAL_FIXED_POINT
            return None

        step_id = self._pending.popleft()
        if step_id in self._processed:
            return self.step()
        self._processed.add(step_id)

        reached = self._steps.get(step_id)
        # (fallback) unknown dependency — a reached step with no declared envelope.
        if reached is None or reached.envelope is None:
            self._discard(
                CAUSE_UNKNOWN_DEPENDENCY, step_id,
                {"reason": "reached step has no declared write envelope"},
            )
            return None

        # (fallback) envelope violation — a poisoning out-of-envelope output.
        producer = reached.resolved_producer_id()
        if producer in self._poisoned:
            self._discard(
                CAUSE_ENVELOPE_VIOLATION, step_id,
                {"producer_id": producer,
                 "reason": "producer has an envelope_violations poisoning row"},
            )
            return None

        facts = self._facts.get(step_id)
        if facts is None:
            # No dependency-fact snapshot for a step we must certify is itself an
            # unknown dependency: we cannot record what a skip would consume.
            self._discard(
                CAUSE_UNKNOWN_DEPENDENCY, step_id,
                {"reason": "no current dependency-fact snapshot for the step"},
            )
            return None

        # (a) a fully valid prior certificate -> SKIP, re-recorded for this gen.
        prior = self._prior.get(step_id)
        if prior is not None:
            validity = validate_certificate(facts, prior)
            if validity.valid:
                outcome = self._record_skip(
                    step_id, prior.comparison, facts,
                    CAUSE_CERTIFICATE_VALID,
                    {"prior_generation_id": prior.generation_id},
                )
                return outcome
            invalidated = sorted(validity.invalidated_classes)
        else:
            invalidated = []

        # (a2) DEGENERATE unchanged-inputs carry-forward -> SKIP behind a
        #      degenerate certificate. Reached only when no valid prior
        #      certificate applied. If the step's current dependency facts are
        #      byte-identical to the prior generation's (the manifest-hash /
        #      carry-forward condition — all ten classes match), the comparison
        #      that justifies reuse is trivial. This is the re-expression of
        #      today's manifest-hash carry-forward: it no longer bypasses the
        #      certificate mechanism but records one through the SAME record_skip,
        #      with the SAME 10-class dependency snapshot and the SAME invalidation
        #      (mutate any class -> the prior facts no longer match, and the
        #      degenerate certificate this pass writes is itself invalid next
        #      pass, so the step is reached again). Envelope boundedness is
        #      irrelevant here: the justification is "nothing changed", not "the
        #      change is provably inconsequential", so §9.4's unbounded->run rule
        #      (which bounds the effect of a CHANGE) does not apply.
        #      ABLATION (E3): with RUNG_CERTIFICATE_UNCHANGED disabled the rung is
        #      not consulted at all — the step falls through to the effect-bound
        #      rung below exactly as if this mechanism had never been built. The
        #      remaining rungs stay REACHABLE (this is a removal, not a collapse).
        prior_facts = self._prior_facts.get(step_id)
        if (
            self.rungs.enabled(RUNG_CERTIFICATE_UNCHANGED)
            and prior_facts is not None
            and not dependency_classes_differ(prior_facts, facts)
        ):
            incumbent = self._incumbent.get(step_id, {})
            comparison = build_degenerate_comparison(
                incumbent_outcome=str(incumbent.get("outcome", "carry_forward")),
                incumbent_basis=str(incumbent.get("basis", "unchanged_inputs")),
            )
            degenerate_detail: dict[str, Any] = {
                "unchanged_inputs": True,
                "prior_generation_id": self._prior_generation_id,
                "dependency_classes_matched": list(DEPENDENCY_CLASSES),
            }
            if invalidated:
                degenerate_detail["prior_certificate_invalidated"] = invalidated
            return self._record_skip(
                step_id, comparison, facts,
                CAUSE_CERTIFICATE_DEGENERATE_UNCHANGED, degenerate_detail,
            )

        # (b) compile effects; RunAlways or a skip refusal -> RUN.
        effects = compile_possible_effects(
            reached.envelope,
            checks=self._checks,
            claim_predicates=reached.claim_predicates,
            producer_id=producer,
        )
        # A step the caller said nothing about gets UNSTATED_CHANGE, not an empty
        # statement: the justified-skip rung then REFUSES. Certificates stay the
        # only skip mechanism — an omission can no longer license one.
        judgment = skip_is_justifiable(
            effects,
            changed_state_terms=self._changed.get(step_id, UNSTATED_CHANGE),
        )
        if not judgment.approved:
            # (b2) bounded-DOMINANCE rung — the CENTREPIECE changed-inputs skip.
            #      A pure bucket-overlap refusal may still be skippable when the
            #      step's envelope provably constrains every permitted output to a
            #      source class that loses to the decisive incumbent (§9.5
            #      resolution_dominance). Conservative-or-refuse: anything the
            #      analysis cannot prove falls through to RUN.
            dominated = self._try_dominance(
                step_id, effects, judgment, facts, invalidated
            )
            if dominated is not None:
                return dominated
            return self._run(step_id, effects, judgment, invalidated)

        # (c) justified-and-bounded -> a NEW certificate + SKIP.
        #     ABLATION (E3): with RUNG_EFFECT_BOUND disabled the approval is not
        #     honoured and the step RUNS. Its cause is CAUSE_RUNG_DISABLED, not
        #     effects_overlap: no term overlapped (the reach was proven disjoint),
        #     and labelling this run an overlap would report a refusal the engine
        #     never made. The cause therefore also COUNTS the skips this rung would
        #     have earned — the per-rung attribution read straight off the tally.
        if not self.rungs.enabled(RUNG_EFFECT_BOUND):
            return self._run(
                step_id, effects, judgment, invalidated,
                rung_disabled=RUNG_EFFECT_BOUND,
            )
        assert isinstance(effects, PossibleEffects)  # RunAlways never approves
        incumbent = self._incumbent.get(step_id, {})
        comparison = build_skip_comparison(
            incumbent_outcome=str(incumbent.get("outcome", "unknown")),
            incumbent_basis=str(incumbent.get("basis", "unknown")),
            effects=effects,
            competitor_bound={
                "statement": (
                    "no changed state term overlaps the step's possible-effects "
                    "reach (v1 set-disjointness over check ids and claim buckets)"
                ),
                "judgment": judgment.to_canonical_dict(),
            },
        )
        detail: dict[str, Any] = {"effects_digest": effects.digest()}
        if invalidated:
            detail["prior_certificate_invalidated"] = invalidated
        return self._record_skip(
            step_id, comparison, facts, CAUSE_SKIP_JUSTIFIED, detail,
        )

    def _try_dominance(
        self,
        step_id: str,
        effects: PossibleEffects | RunAlways,
        judgment: Any,
        facts: CertificateDependencies,
        invalidated: Sequence[str],
    ) -> StepOutcome | None:
        """Bounded-dominance rung: rescue a pure bucket-overlap refusal (§9.5).

        Returns a SKIP outcome behind a ``resolution_dominance`` certificate iff
        the refusal was purely a BUCKET overlap AND EVERY overlapped bucket has a
        recorded incumbent that :func:`dominance_is_justifiable` approves against
        the step's declared competitor bound. Returns ``None`` (fall through to
        RUN) on anything the analysis cannot prove — a check overlap, a missing
        dominance input, a bucket with no incumbent, or any per-bucket refusal.
        Conservative-or-refuse; running the step is never wrong.

        ABLATION (E3): with :data:`RUNG_BOUNDED_DOMINANCE` disabled this rung is
        not consulted at all — it proves nothing and does no analysis, so the
        caller falls through to RUN with the truthful ``effects_overlap`` cause
        the effect-bound rung already established."""
        if not self.rungs.enabled(RUNG_BOUNDED_DOMINANCE):
            return None
        # Only a PURE bucket-overlap refusal is a dominance candidate: a graph /
        # unclassified check overlap is a coarse term the authority argument can
        # say nothing about (RunAlways never reaches here — it is not `approved`
        # False with a bucket-overlap reason).
        if judgment.reason != REFUSE_BUCKET_OVERLAP:
            return None
        dominance = self._dominance.get(step_id)
        if dominance is None:
            return None
        if not isinstance(effects, PossibleEffects):  # pragma: no cover - defensive
            return None
        per_bucket: list[
            tuple[tuple[str, str], IncumbentDecision, DominanceJudgment]
        ] = []
        for bucket in judgment.overlapping_buckets:
            incumbent = dominance.incumbents.get(bucket)
            if incumbent is None:
                # an overlapped bucket the caller did not vouch for -> refuse.
                return None
            dj = dominance_is_justifiable(incumbent, dominance.competitor_bound)
            if not dj.approved:
                return None
            per_bucket.append((bucket, incumbent, dj))
        if not per_bucket:  # pragma: no cover - a bucket-overlap refusal names >=1
            return None
        # Every overlapped bucket is dominance-justified -> SKIP behind a
        # resolution_dominance certificate recording incumbent-worst vs
        # competitor-best.
        incumbent0 = per_bucket[0][1]
        comparison = build_dominance_comparison(
            incumbent=incumbent0,
            effects=effects,
            competitor_bound=dominance.competitor_bound,
            per_bucket=per_bucket,
        )
        detail: dict[str, Any] = {
            "dominance": True,
            "dominated_buckets": [list(b) for b, _i, _j in per_bucket],
            "competitor_authority_max": dominance.competitor_bound.authority_max,
            "effects_digest": effects.digest(),
        }
        if invalidated:
            detail["prior_certificate_invalidated"] = list(invalidated)
        return self._record_skip(
            step_id, comparison, facts, CAUSE_SKIP_DOMINANCE, detail,
        )

    def _run(
        self,
        step_id: str,
        effects: PossibleEffects | RunAlways,
        judgment: Any,
        invalidated: Sequence[str],
        *,
        rung_disabled: str | None = None,
    ) -> StepOutcome | None:
        """RUN the step: execute it (may raise -> operator failure -> ABORT) and
        extend the worklist with its observed dependents (§9.6).

        ``rung_disabled`` names the ABLATED rung that would otherwise have approved
        a skip here (E3). It replaces the cause with :data:`CAUSE_RUNG_DISABLED` so
        the run is never mislabelled as a refusal the analysis did not make."""
        try:
            dependents = tuple(dict.fromkeys(str(d) for d in self._runner(step_id)))
        except Exception as exc:  # noqa: BLE001 - a raising step is an operator failure
            self._abort(
                step_id,
                {"error": f"{type(exc).__name__}: {exc}",
                 "reason": "step runner raised while executing the step"},
            )
            return None

        # The cause names WHY the step runs. An unstated change is NOT an
        # effects overlap (no term overlapped — none was stated), so it gets its
        # own member: the accounting ledger must be able to see a run caused by a
        # missing statement rather than mislabel it as a proven overlap.
        if rung_disabled is not None:
            cause = CAUSE_RUNG_DISABLED
        elif isinstance(effects, RunAlways):
            cause = CAUSE_RUN_ALWAYS
        elif judgment.reason == REFUSE_UNSTATED_CHANGE:
            cause = CAUSE_UNSTATED_CHANGE
        else:
            cause = CAUSE_EFFECTS_OVERLAP
        detail: dict[str, Any] = {
            "dependents": list(dependents),
            "refusal_reason": judgment.reason,
        }
        if rung_disabled is not None:
            detail["rung_disabled"] = rung_disabled
            detail["would_have_skipped_with_cause"] = RUNG_SKIP_CAUSE[rung_disabled]
        if isinstance(effects, RunAlways):
            detail["unbounded_dimensions"] = list(effects.unbounded_dimensions)
        else:
            if judgment.overlapping_checks:
                detail["overlapping_checks"] = list(judgment.overlapping_checks)
            if judgment.overlapping_buckets:
                detail["overlapping_buckets"] = [
                    list(b) for b in judgment.overlapping_buckets
                ]
        if invalidated:
            detail["prior_certificate_invalidated"] = list(invalidated)

        # Observed dependents extend the worklist (a step is reached once).
        for dependent in dependents:
            if dependent not in self._processed:
                self._pending.append(dependent)

        outcome = StepOutcome(
            step_id=step_id, action=ACTION_RUN, cause=cause, detail=detail,
        )
        self._outcomes.append(outcome)
        return outcome

    # -- drive to a fixed point --------------------------------------------- #
    def run_to_fixed_point(self) -> "ReusePass":
        while not self.is_terminal:
            self.step()
        return self

    def result(self) -> ReusePassResult:
        terminal = self.terminal
        if terminal is None:
            # A pass observed mid-flight before any terminal transition: report the
            # non-fixed-point state honestly (the commit gate refuses it).
            terminal = TERMINAL_FIXED_POINT if not self._pending else "in_progress"
        return ReusePassResult(
            deployment=self.deployment,
            generation_id=self.generation_id,
            terminal=terminal,
            outcomes=tuple(self._outcomes),
            fallback_cause=self.fallback_cause,
            failed_step=self.failed_step,
            pending=tuple(self._pending),
            detail=dict(self._detail),
            rungs=self.rungs,
        )


# --------------------------------------------------------------------------- #
# The commit gate + the top-level entry point.                                #
# --------------------------------------------------------------------------- #
def commit_gate(pass_or_result: "ReusePass | ReusePassResult") -> None:
    """Refuse to proceed to the checkpoint unless the pass is at a fixed point.

    §9.6: commit of the maintenance state happens ONLY at the worklist's fixed
    point. A pass with reached-but-unprocessed steps raises
    :class:`CommitBeforeFixedPoint`. A discarded / aborted pass is a different
    terminal state — the fallback ladder handles it, so the commit gate refuses
    it too (there is no fixed-point commit for a discarded or aborted pass)."""
    if isinstance(pass_or_result, ReusePass):
        at_fixed_point = pass_or_result.at_fixed_point
        pending = pass_or_result.pending
        terminal = pass_or_result.terminal
    elif isinstance(pass_or_result, ReusePassResult):
        at_fixed_point = pass_or_result.reached_fixed_point
        pending = pass_or_result.pending
        terminal = pass_or_result.terminal
    else:  # pragma: no cover - defensive
        raise SubstrateError(
            "commit_gate requires a ReusePass or ReusePassResult",
            data={"got_type": type(pass_or_result).__name__},
        )
    if at_fixed_point:
        return
    raise CommitBeforeFixedPoint(
        "reuse pass has not reached a fixed point; the maintenance state cannot "
        "commit while reached steps remain or the pass discarded/aborted (spec "
        "§9.6)",
        data={"terminal": terminal, "pending": sorted(pending)},
    )


def run_reuse_pass(
    conn: Any,
    *,
    deployment: str,
    generation_id: str,
    worklist_seed: Sequence[str],
    steps: Mapping[str, ReuseStep],
    checks: Sequence[Any] = (),
    prior_certificates: Mapping[str, ReuseCertificate] | None = None,
    changed_terms: Mapping[str, ChangeStatement] | None = None,
    current_facts: Mapping[str, CertificateDependencies] | None = None,
    prior_facts: Mapping[str, CertificateDependencies] | None = None,
    prior_generation_id: str | None = None,
    incumbent_selection: Mapping[str, Mapping[str, Any]] | None = None,
    dominance_inputs: Mapping[str, DominanceInput] | None = None,
    step_runner: Callable[[str], Sequence[str]] | None = None,
    cert_store: ReuseCertificateStore | None = None,
    poisoned_producers: frozenset[str] = frozenset(),
    rungs: ReuseRungConfig | None = None,
) -> ReusePassResult:
    """Run the reuse pass to a fixed point and return its result (§9.6).

    Processes ``worklist_seed`` (reached steps) to a fixed point through the
    decision ladder, recording each skip behind a certificate via
    :meth:`ReuseCertificateStore.record_skip` on ``conn`` (inside the caller's
    transaction). ``prior_facts`` is the prior generation's per-step dependency
    snapshot: a step whose current facts are byte-identical to it (and that has
    no valid prior certificate) skips behind a DEGENERATE certificate — the
    unchanged-inputs carry-forward re-expressed as the single skip mechanism
    (§9.5). ``changed_terms`` is per-step and OPTIONAL, but omission is not
    permission: a step with no entry is treated as :data:`UNSTATED_CHANGE` and the
    justified-skip rung refuses it (cause ``unstated_change``). A caller whose
    cycle genuinely changed nothing says so with
    :meth:`ChangedStateTerms.nothing_changed`. An unknown dependency or an
    envelope violation DISCARDS the pass
    (terminal ``discard``, its cause recorded) so the caller runs complete
    execution; an operator failure ABORTS (terminal ``abort``) so the caller
    leaves prior state intact. Deterministic from its inputs — the same inputs
    yield a byte-equal :class:`ReusePassResult` (the DBOS-resume property)."""
    reuse_pass = ReusePass(
        conn,
        deployment=deployment,
        generation_id=generation_id,
        worklist_seed=worklist_seed,
        steps=steps,
        checks=checks,
        prior_certificates=prior_certificates,
        changed_terms=changed_terms,
        current_facts=current_facts,
        prior_facts=prior_facts,
        prior_generation_id=prior_generation_id,
        incumbent_selection=incumbent_selection,
        dominance_inputs=dominance_inputs,
        step_runner=step_runner,
        cert_store=cert_store,
        poisoned_producers=poisoned_producers,
        rungs=rungs,
    )
    reuse_pass.run_to_fixed_point()
    return reuse_pass.result()


__all__ = [
    "OUTSIDE_CONE",
    "FALLBACK_CAUSES",
    "SKIP_CAUSES",
    "DISCARD_CAUSES",
    "ABORT_CAUSES",
    "CAUSE_CERTIFICATE_VALID",
    "CAUSE_CERTIFICATE_DEGENERATE_UNCHANGED",
    "CAUSE_SKIP_JUSTIFIED",
    "CAUSE_SKIP_DOMINANCE",
    "CAUSE_EFFECTS_OVERLAP",
    "CAUSE_RUN_ALWAYS",
    "CAUSE_UNSTATED_CHANGE",
    "CAUSE_RUNG_DISABLED",
    "CAUSE_UNKNOWN_DEPENDENCY",
    "CAUSE_ENVELOPE_VIOLATION",
    "CAUSE_OPERATOR_FAILURE",
    "RUNG_CERTIFICATE_UNCHANGED",
    "RUNG_EFFECT_BOUND",
    "RUNG_BOUNDED_DOMINANCE",
    "REUSE_RUNGS",
    "RUNG_SKIP_CAUSE",
    "ReuseRungConfig",
    "ACTION_SKIP",
    "ACTION_RUN",
    "TERMINAL_FIXED_POINT",
    "TERMINAL_DISCARD",
    "TERMINAL_ABORT",
    "DOMINANCE_APPROVED",
    "REFUSE_DOMINANCE_BASIS",
    "REFUSE_DOMINANCE_UNBOUNDED",
    "REFUSE_DOMINANCE_AUTHORITY",
    "REFUSE_DOMINANCE_HIGHER_SIGNAL",
    "CommitBeforeFixedPoint",
    "ReuseStep",
    "StepOutcome",
    "ReusePass",
    "ReusePassResult",
    "IncumbentDecision",
    "CompetitorBound",
    "DominanceInput",
    "DominanceJudgment",
    "dominance_is_justifiable",
    "derive_competitor_bound",
    "build_dominance_comparison",
    "build_skip_comparison",
    "build_degenerate_comparison",
    "commit_gate",
    "run_reuse_pass",
]
