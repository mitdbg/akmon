"""E3 — which reuse mechanism actually earns the saving.

PACT ``concord-e3-mechanism-ablations``, task.e3.ablation-harness
(req.e3.per-rung-attribution). The consistency paper's E3 asks which of the three
shipped reuse mechanisms pays, and prescribes the method exactly: rerun the cost
workload with ONE mechanism disabled at a time and report actions reached,
actions reused, analysis work, evidence lifetime and invalidations.

What this module is
-------------------
The arm runner and the report. It builds ONE seeded workload, runs it once per
arm through the live three-mode harness
(:func:`okg.substrate.qa.equivalence_series.run_reuse_governed_series`), and
reports per-rung attribution BY DIFFERENCE against the all-rungs-on arm on that
same seed. It adds no analysis machinery: the rung switch is
:class:`okg.substrate.consistency.reuse_scheduler.ReuseRungConfig` and the
quantities are read off the reuse pass' recorded cause tally and the charged-work
ledger, both of which already exist.

The four ways an ablation looks successful and proves nothing, and what refuses
each here:

* **A switch that is set but changes nothing.** :meth:`ArmMeasurement.check`
  asserts on the RECORDED CAUSE TALLY — with a rung disabled, no skip may carry
  that rung's cause — never on the flag.
* **Collapsing the ladder instead of removing one rung.**
  :meth:`AblationReport.reachability_violations` requires every rung the arm still
  has to approve at least one skip on a seed where it did so under the complete
  ladder.
* **An ablation whose corpus was regenerated.** Every arm on a seed is built from
  the SAME :class:`AblationWorkload` — one corpus, one seed, one specification,
  one price table, one update order. The single permitted difference is recorded
  explicitly (:attr:`ArmMeasurement.recorded_differences`): whether the
  dominance-skippable claims were withheld from production, which is the
  mechanism under test and not a property of the corpus.
* **A per-rung number inside the noise.** Deltas are reported as a spread across
  seeds (:class:`RungAttribution`), never as a single draw, and a rung whose
  interval spans zero is reported INSIDE THE NOISE rather than as an effect.

Two outcomes are explicitly allowed and are NOT harness bugs. A rung that
contributes nothing measurable on every workload tried is reported AS THE
FINDING (:attr:`RungAttribution.verdict` ``did_not_pay``). A rung that turns out
to be load-bearing for CORRECTNESS rather than cost — one whose ablation changes
the ANSWER — surfaces loudly through :attr:`AblationReport.answer_violations`
rather than being smoothed into the cost table.

MARGINAL IS NOT GROSS, and conflating them is how this table gets misread. A
one-at-a-time delta measures what a rung contributes THAT NO OTHER RUNG WOULD
HAVE CONTRIBUTED. A rung can fire repeatedly under the complete ladder and still
show a delta of exactly zero, because disabling it lets a rung that is still on
re-approve the very same skips. That is SUBSTITUTION, and reading it as "the
workload never exercises this rung" is false. Three recorded figures keep the
two apart: :attr:`RungAttribution.gross_firings` (the rung's firing count under
the complete ladder), :meth:`AblationReport.attribution_residuals` (where
Sum(one-at-a-time marginals) falls short of the measured total saving by exactly
the substituted work), and :meth:`AblationReport.substitution_findings`. Such a
rung's verdict is :data:`VERDICT_REDUNDANT`, never ``did_not_pay``.

Out of scope, stated so the ablation does not quietly grow: the paper lists a
fourth ablation, **objective bounds**, which is the scalar cost objective
deferred by adjudication row 9 (2026-07-13). It does not exist and this module
does not build it. It is reported as NOT APPLICABLE with that citation
(:data:`OBJECTIVE_BOUNDS_ROW`) — never as a zero, never silently dropped.
"""
from __future__ import annotations

import json
import statistics
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable, Mapping, Sequence

from okg.substrate.consistency.equivalence_differ import CertifiedDifferences
from okg.substrate.consistency.possible_effects import ChangedStateTerms
from okg.substrate.consistency.reuse_certificates import (
    CertificateDependencies,
    ReuseCertificateStore,
    validate_certificate,
)
from okg.substrate.consistency.reuse_scheduler import (
    ACTION_SKIP,
    CAUSE_SKIP_DOMINANCE,
    REUSE_RUNGS,
    RUNG_BOUNDED_DOMINANCE,
    RUNG_CERTIFICATE_UNCHANGED,
    RUNG_EFFECT_BOUND,
    RUNG_SKIP_CAUSE,
    ReuseRungConfig,
    ReuseStep,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.producer_authority import normalize_write_envelope
from okg.substrate.qa.equivalence_series import (
    ReuseConeResult,
    ReuseGovernedUpdate,
    run_reuse_governed_series,
)
from okg.substrate.runtime.reuse_pass_workflow import (
    CLAIM_PRODUCER_STEP,
    build_dominance_skip_cone,
    run_reuse_pass_in_cycle,
)
from okg.substrate.telemetry.work_ledger import (
    CATEGORY_CERTIFICATE_VALIDATION,
    CATEGORY_EFFECT_ANALYSIS,
    MODE_DIFFERENTIAL,
    UnitPricing,
    load_work_ledger,
    render_savings_report,
)

__all__ = [
    "ABLATION_PRICE_TABLE",
    "REPORT_CAVEATS",
    "OBJECTIVE_BOUNDS_ROW",
    "ABLATION_ARMS",
    "ATTRIBUTION_METRICS",
    "RESIDUAL_METRICS",
    "METRIC_COST_DIRECTION",
    "BASELINE_ARM",
    "NO_RUNGS_ARM",
    "DECISION_FIELDS",
    "ROSTER_FIELDS",
    "VERDICT_PAID",
    "VERDICT_COST_MORE",
    "VERDICT_MOVED_NO_COST_DIRECTION",
    "VERDICT_DID_NOT_PAY",
    "VERDICT_REDUNDANT",
    "VERDICT_INSIDE_NOISE",
    "attribute",
    "AblationWorkload",
    "ArmMeasurement",
    "RungAttribution",
    "AblationReport",
    "build_workload",
    "build_updates",
    "run_arm",
    "run_ablation",
]

DEPLOYMENT = "consistency-fixture"
PREDICATE = "preferred_target"
CONSTRAINT = "unique_preferred"
SCOPE = "HCAL/Run3"
SOURCE_AUTHORITY_BASIS = "declared_source_authority"
INCUMBENT_AUTHORITY = 20

#: The ONE price table every arm shares (req.e3.per-rung-attribution: an arm that
#: differs from its baseline in the price table is not an ablation). Declared
#: here rather than left unset so the dollar column is a measured figure and not
#: a loud null; ``model_calls`` is never unit-priced — it carries measured
#: dollars joined from ``okg.llm_calls`` (a measured $0.00 on this fixture, which
#: has no model calls).
ABLATION_PRICE_TABLE: Mapping[str, float] = {
    "effect_analysis": 0.02,
    "certificate_validation": 0.01,
    "deterministic_execution": 0.001,
    "propagation": 0.005,
    "repair_solving": 0.05,
}

#: What the recorded numbers do and do NOT support. Carried in the report so a
#: reader cannot take a column further than the measurement earns — the same
#: discipline that made this benchmark retract an earlier effect.
REPORT_CAVEATS: Mapping[str, str] = MappingProxyType({
    "cone_steps_have_no_executor": (
        "The reuse pass runs as POST-PUBLISH maintenance over a declared cone, "
        "and the cone's step runner is a no-op. RUNNING a cone step therefore "
        "costs no deterministic_execution. Consequence: for the "
        "certificate_unchanged and effect_bound rungs — whose skips avoid cone "
        "steps only — 'actions_reused' is the meaningful column, while "
        "charged_units / cost_usd move mostly with the cost of WRITING "
        "certificates and can come out NEGATIVE (ablating the rung looks "
        "cheaper). That negative is an artefact of the no-op runner, not a "
        "finding that the rung costs more than it saves."
    ),
    "dominance_cost_is_real": (
        "The bounded-dominance rung is the exception: its skip withholds a claim "
        "from PRODUCTION, so ablating it changes what the differential run "
        "actually extracts and materializes. Its charged_units / cost_usd deltas "
        "are therefore measured work, not an accounting artefact."
    ),
    "model_calls_are_a_measured_zero": (
        "This fixture makes no LLM calls, so model_calls is a MEASURED $0.00 "
        "(never a gap). The dollar column is entirely unit-priced deterministic "
        "work under the declared price table, and is comparable ACROSS ARMS "
        "rather than to any absolute production spend."
    ),
    "a_zero_marginal_may_be_redundancy_not_absence": (
        "MARGINAL is not GROSS. A one-at-a-time delta measures what a rung "
        "contributes THAT NO OTHER RUNG WOULD HAVE CONTRIBUTED. A rung can "
        "therefore fire many times under the complete ladder and still show a "
        "delta of exactly zero, because when it is disabled another rung "
        "re-approves the very same skips — SUBSTITUTION, not absence. Read a "
        "zero marginal against two other recorded figures before concluding a "
        "rung is idle: the rung's GROSS firing count in the baseline arm's "
        "cause_tally, and 'attribution_residuals', where Sum(one-at-a-time "
        "marginals) falls short of the measured total saving (all_rungs vs "
        "no_rungs) by exactly the substituted work. Such a rung is reported "
        "'redundant_on_this_workload', never 'did_not_pay'."
    ),
})

#: The paper's FOURTH E3 ablation, reported as not-applicable with its citation.
#: It is a scalar cost objective that does not exist in the tree; reporting it as
#: a zero would claim a measured no-effect for a mechanism never built.
OBJECTIVE_BOUNDS_ROW: Mapping[str, Any] = {
    "ablation": "objective_bounds",
    "status": "not_applicable",
    "measured": False,
    "reason": (
        "the scalar cost objective this ablation would disable does not exist in "
        "the tree: it was DEFERRED by adjudication row 9 (2026-07-13). Reported "
        "not-applicable rather than as a zero — a zero would claim a measured "
        "no-effect for a mechanism that was never built."
    ),
    "citation": "adjudication row 9 (2026-07-13), scalar cost objective deferred",
}

#: The arms: the complete ladder, one single-rung ablation per shipped rung, and
#: the degenerate all-off arm that bounds the total saving from above.
ABLATION_ARMS: tuple[ReuseRungConfig, ...] = (
    ReuseRungConfig.all_enabled(),
    ReuseRungConfig.without(RUNG_CERTIFICATE_UNCHANGED),
    ReuseRungConfig.without(RUNG_EFFECT_BOUND),
    ReuseRungConfig.without(RUNG_BOUNDED_DOMINANCE),
    ReuseRungConfig.none_enabled(),
)

BASELINE_ARM = ReuseRungConfig.all_enabled().arm_label
#: The degenerate all-off arm — the MEASURED total saving is the difference
#: between it and the baseline, and is what the one-at-a-time marginals are
#: reconciled against.
NO_RUNGS_ARM = ReuseRungConfig.none_enabled().arm_label


# --------------------------------------------------------------------------- #
# The seeded workload — ONE corpus per seed, shared by every arm.              #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class AblationWorkload:
    """The corpus + specification one seed pins, shared by every arm on it.

    A seed varies the SIZE of each rung's opportunity (how many buckets a
    dominance skip covers, how many cone steps the unchanged-inputs rung carries,
    how many steps the effect-bound rung certifies) so a per-rung delta has a
    spread rather than being a single draw. It never varies between the arms on
    one seed: an ablation whose corpus was regenerated is not an ablation."""

    seed: int
    dominance_buckets: int
    unchanged_cone_steps: int
    effect_bound_steps: int
    filler_claims: int
    #: Whether the unchanged-inputs cone DECLARES ``nothing_changed()`` per step.
    #: True mirrors the production call site
    #: (``reuse_pass_workflow._run_fixture_cone_pass``, which states it
    #: explicitly); False models a caller that says nothing, which the scheduler
    #: reads as UNSTATED (never as an empty, permissive statement). The two give
    #: opposite answers for what the unchanged-inputs rung is worth, so it is a
    #: recorded part of the specification rather than an accident of the fixture.
    declare_nothing_changed: bool = True

    @property
    def update_order(self) -> tuple[str, ...]:
        return (
            "fresh-conflict",
            "incremental-higher-authority",
            "unchanged-policy-reaffirm",
            "disjoint-bucket-change",
            "genuine-changed-inputs-skip",
        )

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "seed": self.seed,
            "dominance_buckets": self.dominance_buckets,
            "unchanged_cone_steps": self.unchanged_cone_steps,
            "effect_bound_steps": self.effect_bound_steps,
            "filler_claims": self.filler_claims,
            "declare_nothing_changed": self.declare_nothing_changed,
            "update_order": list(self.update_order),
            "deployment": DEPLOYMENT,
            "price_table": dict(ABLATION_PRICE_TABLE),
        }

    def digest_source(self) -> str:
        return json.dumps(self.to_canonical_dict(), sort_keys=True)


def build_workload(
    seed: int, *, declare_nothing_changed: bool = True,
) -> AblationWorkload:
    """The deterministic seed -> workload map. Same seed, same corpus, always."""
    if seed < 0:
        raise SubstrateError(
            "reuse ablation workload seed must be non-negative",
            data={"seed": seed},
        )
    return AblationWorkload(
        seed=seed,
        dominance_buckets=1 + (seed % 3),
        unchanged_cone_steps=3 + (seed % 3),
        effect_bound_steps=1 + (seed % 2),
        filler_claims=seed % 3,
        declare_nothing_changed=declare_nothing_changed,
    )


def _bucket_key(index: int) -> str:
    return f"pref|k{index}"


def _claim(
    node_id: str, value: str, *, key: str, source: str, source_class: str,
    **signals: Any,
) -> NodeFact:
    attrs = {
        "predicate": PREDICATE, "normalized_key": key, "value": value,
        "scope": SCOPE, "scope_state": "known",
        "claim_provenance": "derived", "source_id": source,
        "source_class": source_class,
    }
    attrs.update({k: str(v) for k, v in signals.items()})
    return NodeFact(node_id, "consistency_claim", attrs, {"id": node_id}, {"v": 1})


def _policy(node_id: str, *, rev: int = 1, **extra: Any) -> NodeFact:
    attrs = {"enabled": "true", "policy_id": CONSTRAINT,
             "policy_kind": "functional", "predicate": PREDICATE}
    attrs.update({k: str(v) for k, v in extra.items()})
    return NodeFact(node_id, "predicate_policy", attrs, {"id": node_id}, {"v": rev})


def _cone_facts(step_id: str, tag: str) -> CertificateDependencies:
    """A deterministic ten-class dependency snapshot for a cone step."""
    return CertificateDependencies(
        input_digests={step_id: f"{DEPLOYMENT}:{step_id}:{tag}"},
        operator_versions={step_id: "v1"},
        model_prompt_keys={},
        envelope_fingerprint=f"{DEPLOYMENT}:{step_id}:envelope",
        constraint_set={},
        generator_versions={},
        policy_fingerprint=f"{DEPLOYMENT}:policy",
        precedence_fingerprint=f"{DEPLOYMENT}:precedence",
        competitor_bounds={},
        catalog_version_id=f"{DEPLOYMENT}:catalog",
    )


# --------------------------------------------------------------------------- #
# The three cones — one per rung, so every rung fires under the complete arm.  #
# --------------------------------------------------------------------------- #
def _unchanged_cone(
    workload: AblationWorkload, rungs: ReuseRungConfig,
) -> Callable[[str, str, str], ReuseConeResult]:
    """The UNCHANGED-INPUTS rung's cone: every step's dependency facts are
    byte-identical to the prior generation's, so under the complete ladder each
    skips behind a degenerate certificate.

    Whether the cone DECLARES ``nothing_changed()`` decides what this rung is
    worth, so it is a recorded part of the workload specification. Declared (the
    production shape) an empty change set is trivially disjoint from any reach,
    so ablating this rung migrates its skips to the effect-bound rung and costs
    nothing. Omitted, the scheduler reads UNSTATED and refuses, so ablating this
    rung turns every one of its skips into a run."""
    step_ids = tuple(
        f"unchanged_step_{i}" for i in range(workload.unchanged_cone_steps)
    )
    envelope = normalize_write_envelope(
        {"nodes": [{"subtype": "consistency_claim",
                    "clause_id": f"{DEPLOYMENT}:unchanged"}]},
        source_name="unchanged",
    )
    steps = {
        s: ReuseStep(step_id=s, envelope=envelope, producer_id=s) for s in step_ids
    }
    facts = {s: _cone_facts(s, "steady") for s in step_ids}
    incumbent = {
        s: {"outcome": "carry_forward", "basis": "unchanged_inputs"} for s in step_ids
    }
    changed = (
        {s: ChangedStateTerms.nothing_changed() for s in step_ids}
        if workload.declare_nothing_changed else None
    )

    def _run(production_dsn: str, deployment: str, generation: str) -> ReuseConeResult:
        import psycopg

        conn = psycopg.connect(production_dsn)
        try:
            summary = run_reuse_pass_in_cycle(
                conn, deployment=deployment, generation_id=generation,
                worklist_seed=step_ids, steps=steps, current_facts=facts,
                prior_facts=facts, prior_generation_id="prior",
                changed_terms=changed,
                incumbent_selection=incumbent, rungs=rungs,
                cert_store=ReuseCertificateStore(deployment=deployment),
                checkpoint_writer=lambda _c: generation,
            )
        finally:
            conn.close()
        return ReuseConeResult(reuse_pass=summary, certified_differences=None)

    return _run


def _effect_bound_cone(
    workload: AblationWorkload, rungs: ReuseRungConfig,
) -> Callable[[str, str, str], ReuseConeResult]:
    """The EFFECT-BOUND rung's cone: each step's envelope confines its possible
    effects to a bucket the update did NOT change, so set-disjointness approves a
    fresh justified certificate. Nothing is withheld from production — an
    effect-bound skip asserts the step could not have touched the changed state,
    so not running it is invisible in the answer."""
    # EVERY bucket the series touches, not just the first. The change statement
    # must not be understated: a seed with several dominance buckets changes all
    # of them on the corpus updates, and the scheduler treats what the caller
    # says as the whole truth. (The disjointness conclusion is the same either
    # way here — the steps' reach is a bucket no update touches — but a statement
    # that happens to be harmless is still a statement that is wrong.)
    changed_buckets = frozenset(
        (PREDICATE, _bucket_key(i + 1)) for i in range(workload.dominance_buckets)
    )
    step_ids = tuple(f"effect_step_{i}" for i in range(workload.effect_bound_steps))
    steps: dict[str, ReuseStep] = {}
    for i, step_id in enumerate(step_ids):
        envelope = normalize_write_envelope(
            {"nodes": [{"subtype": "consistency_claim",
                        "clause_id": f"{DEPLOYMENT}:{step_id}"}],
             "claim_keys": [_bucket_key(900 + i)]},   # a bucket the change misses
            source_name=step_id,
        )
        steps[step_id] = ReuseStep(
            step_id=step_id, envelope=envelope, producer_id=step_id,
            claim_predicates=(PREDICATE,),
        )
    facts = {s: _cone_facts(s, "disjoint") for s in step_ids}
    changed = {s: ChangedStateTerms(buckets=changed_buckets) for s in step_ids}
    incumbent = {
        s: {"outcome": "selected", "basis": SOURCE_AUTHORITY_BASIS} for s in step_ids
    }

    def _run(production_dsn: str, deployment: str, generation: str) -> ReuseConeResult:
        import psycopg

        conn = psycopg.connect(production_dsn)
        try:
            summary = run_reuse_pass_in_cycle(
                conn, deployment=deployment, generation_id=generation,
                worklist_seed=step_ids, steps=steps, current_facts=facts,
                changed_terms=changed, incumbent_selection=incumbent, rungs=rungs,
                cert_store=ReuseCertificateStore(deployment=deployment),
                checkpoint_writer=lambda _c: generation,
            )
        finally:
            conn.close()
        return ReuseConeResult(reuse_pass=summary, certified_differences=None)

    return _run


def _dominance_cone(
    workload: AblationWorkload, rungs: ReuseRungConfig,
) -> Callable[[str, str, str], ReuseConeResult]:
    """The BOUNDED-DOMINANCE rung's cone (the centrepiece changed-inputs skip):
    each producer step's input changed on a bucket the decisive incumbent already
    wins, and its envelope ceils every permitted output below that incumbent's
    declared source authority.

    With the rung active the producer steps are skipped and their claims are
    withheld from production (§9.7 licensed divergence). With it ablated the steps
    RUN, the claims are extracted, and the differential run's raw extraction
    matches the reference exactly — the same ANSWER by a different amount of
    work, which is precisely what E3 measures."""
    cones = [
        build_dominance_skip_cone(
            DEPLOYMENT, predicate=PREDICATE, normalized_key=_bucket_key(i + 1),
            incumbent_authority=INCUMBENT_AUTHORITY,
            incumbent_basis=SOURCE_AUTHORITY_BASIS,
            competitor_source_classes=["crawl"], competitor_authority_max=0,
            producer_step=f"{CLAIM_PRODUCER_STEP}_{i + 1}",
        )
        for i in range(workload.dominance_buckets)
    ]
    steps: dict[str, Any] = {}
    facts: dict[str, Any] = {}
    changed: dict[str, Any] = {}
    incumbent: dict[str, Any] = {}
    dominance: dict[str, Any] = {}
    seed_ids: list[str] = []
    covered: set[str] = set()
    for cone in cones:
        steps.update(cone.steps)
        facts.update(cone.current_facts)
        changed.update(cone.changed_terms)
        incumbent.update(cone.incumbent_selection)
        dominance.update(cone.dominance_inputs)
        seed_ids.extend(cone.worklist_seed)
        covered.add(cone.covered_bucket_row_key)
    expect_skip = rungs.enabled(RUNG_BOUNDED_DOMINANCE)

    def _run(production_dsn: str, deployment: str, generation: str) -> ReuseConeResult:
        import psycopg

        conn = psycopg.connect(production_dsn)
        try:
            summary = run_reuse_pass_in_cycle(
                conn, deployment=deployment, generation_id=generation,
                worklist_seed=tuple(seed_ids), steps=steps, current_facts=facts,
                changed_terms=changed, incumbent_selection=incumbent,
                dominance_inputs=dominance, rungs=rungs,
                cert_store=ReuseCertificateStore(deployment=deployment),
                checkpoint_writer=lambda _c: generation,
            )
        finally:
            conn.close()
        result = summary["result"]
        skips = [o for o in result.skips if o.cause == CAUSE_SKIP_DOMINANCE]
        # The one recorded difference between arms (whether the losers were
        # withheld from production) is DECLARED from the rung config; this ties
        # that declaration to the observed tally, so a switch that did not change
        # behaviour cannot pass as a correctly-withheld arm.
        if expect_skip and not skips:
            raise SubstrateError(
                "the dominance arm withheld the losers from production but the "
                "pass approved no bounded-dominance skip — the corpus and the "
                "mechanism disagree, so the arm would measure a corpus change "
                "rather than an ablation",
                data={"generation": generation,
                      "outcomes": [(o.step_id, o.action, o.cause)
                                   for o in result.outcomes]},
            )
        if not expect_skip and skips:
            raise SubstrateError(
                "the bounded-dominance rung is DISABLED yet the pass approved a "
                "dominance skip — the switch changed no behaviour",
                data={"generation": generation,
                      "skips": [o.step_id for o in skips]},
            )
        certified = (
            CertifiedDifferences(
                covered_buckets=frozenset(covered),
                certificate_digests=tuple(o.certificate_digest for o in skips),
            )
            if skips else None
        )
        return ReuseConeResult(
            reuse_pass=summary, certified_differences=certified,
            skipped_producer_step=seed_ids[0] if skips else None,
        )

    return _run


def build_updates(
    workload: AblationWorkload, rungs: ReuseRungConfig,
) -> list[ReuseGovernedUpdate]:
    """The five-update series for one arm on one seed.

    Every arm gets the SAME corpus, specification and update order. The single
    permitted difference is where the dominance-skippable crawl claims go: they
    are WITHHELD from production when the bounded-dominance rung is active (the
    scheduler skipped their producer, so the differential run never extracts
    them) and applied normally when it is not. That is the mechanism under test,
    not a change of corpus — and both routings fold the same claims into the
    cumulative net the complete reference runs over, so the reference is
    identical in every arm."""
    n_dom = workload.dominance_buckets
    withhold = rungs.enabled(RUNG_BOUNDED_DOMINANCE)

    # -- the shared corpus ---------------------------------------------------
    # update 0: a fresh conflict on EVERY dominance bucket (registry beats wiki),
    # plus filler.
    #
    # Every dominance bucket gets the SAME three-claim structure, and that is
    # load-bearing rather than tidiness. A bounded-dominance skip is harmless
    # because the bucket already has a DECIDED incumbent the withheld claim
    # provably loses to. Give a bucket a single uncontested claim instead and the
    # bucket has no resolution at all until the ablated arm extracts the loser and
    # creates one — so the arms differ in whether a resolution row EXISTS, which
    # is a genuine answer difference and not a §9.7-licensed roster difference.
    # (An earlier revision of this workload seeded only bucket 1 with a conflict;
    # the report's answer / verdict / roster checks all fired on seeds 1 and 2.)
    fresh: list[NodeFact] = [_policy("pol:fixture")]
    for i in range(n_dom):
        fresh.append(
            _claim(f"claim:e{i + 1}", f"vE{i + 1}", key=_bucket_key(i + 1),
                   source=f"registry:rec:e{i + 1}", source_class="registry",
                   source_authority=10)
        )
        fresh.append(
            _claim(f"claim:w{i + 1}", f"vW{i + 1}", key=_bucket_key(i + 1),
                   source=f"page:w{i + 1}", source_class="wiki",
                   source_authority=1)
        )
    for i in range(workload.filler_claims):
        fresh.append(
            _claim(f"claim:fill{i}", f"vF{i}", key=_bucket_key(500 + i),
                   source=f"registry:fill:{i}", source_class="registry",
                   source_authority=5)
        )

    # update 1: the decisive incumbent for every dominance bucket.
    incumbents = [
        _claim(f"claim:g{i + 1}", f"vG{i + 1}", key=_bucket_key(i + 1),
               source=f"registry:rec:{i + 2}", source_class="registry",
               source_authority=INCUMBENT_AUTHORITY)
        for i in range(n_dom)
    ]

    # update 4: the crawl batch. The losers (one per dominance bucket) are the
    # claims the dominance rung withholds; the carrier delta is a crawl claim on
    # a FRESH bucket that has no incumbent to dominate, so it is always extracted.
    losers = [
        _claim(f"claim:cr{i + 1}", f"vCR{i + 1}", key=_bucket_key(i + 1),
               source=f"crawl:page:{i + 1}", source_class="crawl",
               source_authority=0)
        for i in range(n_dom)
    ]
    carrier = _claim("claim:crfresh", "vCRfresh", key=_bucket_key(800),
                     source="crawl:page:fresh", source_class="crawl",
                     source_authority=0)

    # EVERY update runs a cone. Not cosmetic: an update with no reuse pass
    # records effect_analysis / certificate_validation as a GAP (measured=FALSE),
    # which makes the charged dollars a loud null for the whole arm — the cost
    # column E3 needs would be unpriced rather than measured.
    return [
        ReuseGovernedUpdate(
            label="fresh-conflict", production_facts=fresh,
            run_cone=_effect_bound_cone(workload, rungs),
        ),
        ReuseGovernedUpdate(
            label="incremental-higher-authority", production_facts=incumbents,
            run_cone=_effect_bound_cone(workload, rungs),
        ),
        ReuseGovernedUpdate(
            label="unchanged-policy-reaffirm",
            production_facts=[_policy("pol:fixture", rev=2,
                                      review_note="reaffirmed")],
            run_cone=_unchanged_cone(workload, rungs),
        ),
        ReuseGovernedUpdate(
            label="disjoint-bucket-change",
            production_facts=[
                _claim("claim:e1", "vE1b", key=_bucket_key(1),
                       source="registry:rec:e1", source_class="registry",
                       source_authority=10),
            ],
            run_cone=_effect_bound_cone(workload, rungs),
        ),
        ReuseGovernedUpdate(
            label="genuine-changed-inputs-skip",
            # withheld when the rung is active; extracted when it is ablated.
            production_facts=[carrier] if withhold else [carrier, *losers],
            snapshot_extra_facts=losers if withhold else (),
            run_cone=_dominance_cone(workload, rungs),
        ),
    ]


# --------------------------------------------------------------------------- #
# One arm's measurement.                                                       #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ArmMeasurement:
    """The E3 quantities for ONE arm on ONE seed, read off the record."""

    seed: int
    arm: str
    rungs: ReuseRungConfig
    workload: AblationWorkload
    # -- the quantities E3 names --------------------------------------------
    actions_reached: int          # reuse-pass step outcomes decided
    actions_reused: int           # of those, skips (work not done)
    analysis_work_units: int      # differential effect_analysis charged units
    certificate_work_units: int   # differential certificate_validation units
    evidence_lifetime_generations: float   # mean generations a certificate stays valid
    invalidations: int            # certificates that ceased to validate in-series
    # -- the ledger's own figures -------------------------------------------
    charged_units: int
    cost_usd: float
    cost_priced_complete: bool
    savings_vs_reference_units: int | None
    savings_vs_reference_cost: float | None
    # -- the record the acceptances are asserted on --------------------------
    cause_tally: Mapping[str, int]
    verdicts: Mapping[str, int]
    answer_digest: str
    loser_roster: Mapping[str, Sequence[str]]
    withheld_claim_ids: tuple[str, ...]
    effective_claim_count: int
    recorded_differences: Mapping[str, Any]
    pass_digests: tuple[str, ...]
    #: The generations this arm actually PUBLISHED, in update order. The series
    #: raises unless each reached ``published``, so their presence is the
    #: published-generation evidence the requirement asks for.
    production_generations: tuple[str, ...] = ()
    #: The Chronos checkpoint ids for the same generations.
    production_chronos_generations: tuple[str, ...] = ()
    #: The final update's resolved families, kept for cross-arm inspection.
    #: Diagnostic only — never part of :meth:`to_canonical_dict`.
    snapshots: Mapping[str, Mapping[str, Mapping[str, Any]]] = field(
        default_factory=dict, repr=False, compare=False,
    )

    def rung_violations(self) -> dict[str, int]:
        """Skips carrying a DISABLED rung's cause. Must be empty — the acceptance
        asserted on the tally, never on the flag."""
        return {
            cause: self.cause_tally[cause]
            for cause in sorted(self.rungs.suppressed_skip_causes())
            if self.cause_tally.get(cause)
        }

    def rungs_that_fired(self) -> frozenset[str]:
        return frozenset(
            rung for rung in REUSE_RUNGS
            if self.cause_tally.get(RUNG_SKIP_CAUSE[rung], 0) > 0
        )

    def check(self) -> None:
        """Refuse an arm that does not actually ablate what it claims to."""
        violations = self.rung_violations()
        if violations:
            raise SubstrateError(
                "ablation arm recorded skips carrying a DISABLED rung's cause: "
                "the switch was set and the behaviour did not change",
                data={"seed": self.seed, "arm": self.arm,
                      "violations": violations,
                      "rungs_disabled": sorted(self.rungs.disabled)},
            )

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "seed": self.seed,
            "arm": self.arm,
            "rungs": self.rungs.to_canonical_dict(),
            # recorded per ARM, not once per seed: the acceptance is that every
            # arm shares one corpus / seed / specification / price table / update
            # order with its baseline, and a reader must be able to CHECK that
            # rather than take it on trust.
            "workload": self.workload.to_canonical_dict(),
            "actions_reached": self.actions_reached,
            "actions_reused": self.actions_reused,
            "analysis_work_units": self.analysis_work_units,
            "certificate_work_units": self.certificate_work_units,
            "evidence_lifetime_generations": self.evidence_lifetime_generations,
            "invalidations": self.invalidations,
            "charged_units": self.charged_units,
            "cost_usd": self.cost_usd,
            "cost_priced_complete": self.cost_priced_complete,
            "savings_vs_reference_units": self.savings_vs_reference_units,
            "savings_vs_reference_cost": self.savings_vs_reference_cost,
            "cause_tally": dict(self.cause_tally),
            "verdicts": dict(self.verdicts),
            "answer_digest": self.answer_digest,
            "decision_fields": list(DECISION_FIELDS),
            "loser_roster": {k: list(v) for k, v in self.loser_roster.items()},
            "withheld_claim_ids": list(self.withheld_claim_ids),
            "effective_claim_count": self.effective_claim_count,
            "rungs_that_fired": sorted(self.rungs_that_fired()),
            "rung_violations": self.rung_violations(),
            "recorded_differences": dict(self.recorded_differences),
            "pass_digests": list(self.pass_digests),
            "production_generations": list(self.production_generations),
            "production_chronos_generations": list(
                self.production_chronos_generations
            ),
        }


#: The fields that constitute the ANSWER: which claim WON each bucket, and under
#: what outcome. Every other recorded field of a resolution / selection row is
#: either derived from them or is a ROSTER of the losers the run happened to have
#: extracted — and §9.7 licenses exactly that roster to differ when a
#: bounded-dominance skip means the differential run never extracted a loser the
#: complete reference contains. The roster is NOT silently excluded: it is
#: measured separately (:func:`_loser_roster`) and the difference is required to
#: be exactly the claims the arm withheld (:meth:`AblationReport.roster_anomalies`).
DECISION_FIELDS: tuple[str, ...] = ("outcome", "winner_claim_id")

#: The roster fields — the losers a run extracted. Licensed to differ by §9.7.
ROSTER_FIELDS: tuple[str, ...] = (
    "ranked_claim_ids", "superseded_claim_ids", "distrusted_claim_ids",
)


def _decision_payload(result: Any) -> list[dict[str, Any]]:
    return [
        {
            "update": o.update_index,
            "resolution_outcomes": {
                k: {f: v.get(f) for f in DECISION_FIELDS}
                for k, v in sorted(o.differential_snapshot.resolution_outcomes.items())
            },
            "selections": {
                k: {f: v.get(f) for f in DECISION_FIELDS}
                for k, v in sorted(o.differential_snapshot.selections.items())
            },
        }
        for o in result.outcomes
    ]


def _answer_digest(result: Any) -> str:
    """The RESOLVED DECISION across the series — the 'answer' an ablation must not
    change: which claim won each bucket, under what outcome, on every update.

    Deliberately NOT the raw effective set and NOT the loser roster. §9.7
    distinguishes them, and a bounded-dominance skip's entire point is that the
    differential run never extracts a claim the reference contains while the
    decision is identical. Folding either in would report the licensed divergence
    as an answer change and bury the question E3 is actually asking."""
    import hashlib

    blob = json.dumps(
        _decision_payload(result), sort_keys=True, separators=(",", ":"), default=str,
    )
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _loser_roster(result: Any) -> dict[str, list[str]]:
    """Per bucket, the claim ids the run extracted as losers — the §9.7-licensed
    material. Measured so a cross-arm difference is CHECKED against the claims the
    arm withheld, never quietly dropped from the comparison."""
    roster: dict[str, set[str]] = {}
    last = result.outcomes[-1]
    for family in (last.differential_snapshot.resolution_outcomes,
                   last.differential_snapshot.selections):
        for bucket, row in family.items():
            acc = roster.setdefault(bucket, set())
            for field_name in ROSTER_FIELDS:
                acc.update(str(c) for c in (row.get(field_name) or ()))
    return {k: sorted(v) for k, v in sorted(roster.items())}


def _evidence_lifetime(
    production_dsn: str, generations: Sequence[str],
) -> tuple[float, int]:
    """(mean certificate lifetime in generations, invalidation count).

    A certificate's LIFETIME is the number of series generations, from the one
    that recorded it onward, in which it still validates against the dependency
    facts it pinned. An INVALIDATION is a certificate that stops validating
    within the series. Both are read off the recorded certificate rows — nothing
    is re-derived and nothing is simulated."""
    import psycopg

    store = ReuseCertificateStore(deployment=DEPLOYMENT)
    per_generation: dict[str, list[Any]] = {}
    with psycopg.connect(production_dsn, autocommit=True) as conn:
        for gen in generations:
            per_generation[gen] = list(store.for_generation(conn, gen))

    # the latest recorded dependency facts per step, in generation order.
    latest_facts: dict[str, CertificateDependencies] = {}
    facts_at: dict[str, dict[str, CertificateDependencies]] = {}
    for gen in generations:
        for cert in per_generation[gen]:
            latest_facts[cert.step_id] = cert.dependencies
        facts_at[gen] = dict(latest_facts)

    lifetimes: list[int] = []
    invalidations = 0
    for index, gen in enumerate(generations):
        for cert in per_generation[gen]:
            lifetime = 1
            invalidated = False
            for later in generations[index + 1:]:
                facts = facts_at[later].get(cert.step_id)
                if facts is None:
                    break
                if validate_certificate(facts, cert).valid:
                    lifetime += 1
                else:
                    invalidated = True
                    break
            lifetimes.append(lifetime)
            invalidations += int(invalidated)
    mean = round(statistics.fmean(lifetimes), 4) if lifetimes else 0.0
    return mean, invalidations


def run_arm(
    *,
    seed: int,
    rungs: ReuseRungConfig,
    deployment_dir: Path | str,
    production_dsn: str,
    scratch_dsn_factory: Callable[[], str],
    declare_nothing_changed: bool = True,
) -> ArmMeasurement:
    """Run ONE arm of the ablation on ONE seed and measure it."""
    import psycopg

    workload = build_workload(
        seed, declare_nothing_changed=declare_nothing_changed
    )
    updates = build_updates(workload, rungs)
    result = run_reuse_governed_series(
        updates,
        deployment_dir=deployment_dir,
        production_dsn=production_dsn,
        scratch_dsn_factory=scratch_dsn_factory,
        persist=True,
        pricing=UnitPricing.from_config(ABLATION_PRICE_TABLE),
    )

    generations = [o.production_generation for o in result.outcomes]
    tally: dict[str, int] = {}
    reached = 0
    reused = 0
    digests: list[str] = []
    for outcome in result.outcomes:
        if outcome.reuse_pass is None:
            continue
        pass_result = outcome.reuse_pass["result"]
        digests.append(pass_result.digest())
        for step_outcome in pass_result.outcomes:
            reached += 1
            reused += int(step_outcome.action == ACTION_SKIP)
            tally[step_outcome.cause] = tally.get(step_outcome.cause, 0) + 1

    with psycopg.connect(production_dsn, autocommit=True) as conn:
        ledger = load_work_ledger(conn, DEPLOYMENT)
        report = render_savings_report(conn, DEPLOYMENT)

    def _units(category: str) -> int:
        return sum(
            row.work_units or 0 for row in ledger
            if row.mode == MODE_DIFFERENTIAL and row.category == category
            and row.measured
        )

    lifetime, invalidations = _evidence_lifetime(production_dsn, generations)
    last = result.outcomes[-1]

    return ArmMeasurement(
        seed=seed,
        arm=rungs.arm_label,
        rungs=rungs,
        workload=workload,
        actions_reached=reached,
        actions_reused=reused,
        analysis_work_units=_units(CATEGORY_EFFECT_ANALYSIS),
        certificate_work_units=_units(CATEGORY_CERTIFICATE_VALIDATION),
        evidence_lifetime_generations=lifetime,
        invalidations=invalidations,
        charged_units=report.production_total_units,
        cost_usd=report.production_cost_usd,
        cost_priced_complete=report.production_cost_priced_complete,
        savings_vs_reference_units=report.savings_vs_reference_units,
        savings_vs_reference_cost=report.savings_vs_reference_cost,
        cause_tally=tally,
        verdicts=result.verdict_distribution(),
        answer_digest=_answer_digest(result),
        loser_roster=_loser_roster(result),
        # the claims this arm's mechanism withheld from production — the ONLY
        # claims by which its loser roster may differ from the baseline's.
        withheld_claim_ids=tuple(
            sorted(
                str(f.node_id)
                for u in updates for f in u.snapshot_extra_facts
            )
        ),
        effective_claim_count=len(last.differential_snapshot.effective_claims),
        recorded_differences={
            "dominance_losers_withheld_from_production": rungs.enabled(
                RUNG_BOUNDED_DOMINANCE
            ),
            "note": (
                "the ONLY difference from the baseline arm's run: whether the "
                "dominance-skippable claims were withheld from production. That "
                "is the mechanism under test. Corpus, seed, specification, price "
                "table and update order are identical, and the cumulative net "
                "the complete reference runs over contains the same claims in "
                "every arm."
            ),
        },
        pass_digests=tuple(digests),
        production_generations=tuple(generations),
        production_chronos_generations=tuple(
            o.production_chronos_generation for o in result.outcomes
        ),
        snapshots={
            "resolution_outcomes": {
                k: dict(v) for k, v in last.differential_snapshot
                .resolution_outcomes.items()
            },
            "selections": {
                k: dict(v) for k, v in last.differential_snapshot.selections.items()
            },
        },
    )


# --------------------------------------------------------------------------- #
# Per-rung attribution, BY DIFFERENCE, with a spread.                          #
# --------------------------------------------------------------------------- #
#: A stable nonzero effect in the direction that means the rung REDUCED cost or
#: EARNED reuse under :data:`METRIC_COST_DIRECTION`.
VERDICT_PAID = "paid"
#: A stable nonzero effect in the OPPOSITE direction: ablating the rung made this
#: metric cheaper. Never labelled ``paid`` — see the sign discussion below.
VERDICT_COST_MORE = "cost_more"
#: A stable nonzero effect on a metric that has NO declared cost polarity
#: (evidence lifetime, invalidations). The rung moved it; whether that is a
#: saving is not a question this metric can answer, so no direction is claimed.
VERDICT_MOVED_NO_COST_DIRECTION = "moved_no_cost_direction"
#: Every delta is zero AND the rung never fired under the complete ladder: the
#: workload never presented the condition, so nothing was measured about it.
VERDICT_DID_NOT_PAY = "did_not_pay"
#: Every delta is zero YET the rung FIRED under the complete ladder: every skip
#: it earned was also earnable by another rung, so its MARGINAL contribution is
#: zero while its GROSS contribution is its firing count. Redundancy, not
#: absence — see the ``a_zero_marginal_may_be_redundancy_not_absence`` caveat.
VERDICT_REDUNDANT = "redundant_on_this_workload"
VERDICT_INSIDE_NOISE = "inside_the_noise"

#: Which SIGN of a delta (``ablated_arm - baseline_arm``) means the rung PAID.
#:
#: ``verdict`` used to be direction-blind: any stable nonzero delta read
#: ``paid``, so ``effect_bound / cost_usd = -0.040`` and
#: ``bounded_dominance / cost_usd = +0.102`` carried the SAME label while meaning
#: opposite things, and anyone tabulating ``verdict == "paid"`` drew the wrong
#: conclusion. The polarity differs per metric and cannot be inferred from the
#: sign alone: ablating a rung LOSES reuses (a negative ``actions_reused`` delta
#: means the rung earned them) but ADDS charged work (a positive ``charged_units``
#: delta means the rung saved it). Metrics mapped to ``0`` have no cost polarity
#: at all and are never labelled paid or cost_more.
METRIC_COST_DIRECTION: Mapping[str, int] = MappingProxyType({
    "actions_reused": -1,
    "charged_units": +1,
    "cost_usd": +1,
    "analysis_work_units": +1,
    "evidence_lifetime_generations": 0,
    "invalidations": 0,
})

DIRECTION_NONE = "none"
DIRECTION_ABLATION_INCREASES = "ablation_increases"
DIRECTION_ABLATION_DECREASES = "ablation_decreases"
DIRECTION_MIXED = "mixed"


def _direction(deltas: Sequence[float]) -> str:
    """The raw sign of the deltas, recorded ALONGSIDE the verdict so a reader
    never has to infer direction from a label."""
    if not deltas or all(d == 0 for d in deltas):
        return DIRECTION_NONE
    if all(d >= 0 for d in deltas):
        return DIRECTION_ABLATION_INCREASES
    if all(d <= 0 for d in deltas):
        return DIRECTION_ABLATION_DECREASES
    return DIRECTION_MIXED


@dataclass(frozen=True)
class RungAttribution:
    """What ONE rung is worth, as a spread of per-seed deltas.

    Every delta is ``ablated_arm - baseline_arm`` on the SAME seed, so it is a
    measured difference and not a separately-generated run that happens to be
    cheaper. A rung whose delta interval SPANS ZERO is reported inside the noise;
    a rung whose every delta is zero is reported as not having paid. Neither is a
    harness defect — 'this mechanism did not pay' is a result E3 must be able to
    return."""

    rung: str
    metric: str
    per_seed_deltas: Mapping[int, float]
    verdict: str
    #: How many skips this rung approved under the COMPLETE ladder on each seed —
    #: its GROSS contribution, read off the baseline arm's cause tally. Recorded
    #: beside the marginal because the two answer different questions and a
    #: reader who sees only the marginal will misread a substituted rung as idle.
    gross_firings: Mapping[int, int] = field(default_factory=dict)

    @property
    def deltas(self) -> list[float]:
        return [self.per_seed_deltas[s] for s in sorted(self.per_seed_deltas)]

    @property
    def mean(self) -> float:
        return round(statistics.fmean(self.deltas), 4) if self.deltas else 0.0

    @property
    def spread(self) -> tuple[float, float]:
        return (min(self.deltas), max(self.deltas)) if self.deltas else (0.0, 0.0)

    def to_canonical_dict(self) -> dict[str, Any]:
        lo, hi = self.spread
        return {
            "rung": self.rung,
            "metric": self.metric,
            "per_seed_deltas": {str(k): v for k, v in sorted(
                self.per_seed_deltas.items()
            )},
            "mean_delta": self.mean,
            "spread_min": lo,
            "spread_max": hi,
            "seeds": len(self.per_seed_deltas),
            "verdict": self.verdict,
            # the raw sign, recorded so the verdict never has to be trusted to
            # carry direction, and the polarity that turned one into the other.
            "direction": _direction(self.deltas),
            "metric_cost_direction": METRIC_COST_DIRECTION.get(self.metric, 0),
            "gross_firings": {
                str(k): v for k, v in sorted(self.gross_firings.items())
            },
            "gross_firings_total": sum(self.gross_firings.values()),
        }


def _classify(
    deltas: Sequence[float], metric: str, *, substituted: bool = False,
) -> str:
    """The verdict for one rung on one metric.

    Two things the direction-blind predecessor got wrong, both of which made the
    label mean the opposite of the number on some rows:

    * A nonzero delta was ``paid`` regardless of SIGN, so a rung whose ablation
      made the metric CHEAPER carried the same label as one whose ablation made
      it dearer. Direction now comes from :data:`METRIC_COST_DIRECTION`, and a
      metric with no declared polarity is never labelled paid or cost_more.
    * An all-zero delta was ``did_not_pay`` regardless of whether the rung had
      FIRED. A rung that fires and whose every skip is re-earned by another rung
      is reported :data:`VERDICT_REDUNDANT`, which is a statement about this
      workload rather than about the mechanism.

    ``substituted`` is that second distinction, decided ONCE per rung from the
    skip counts (see :meth:`AblationReport.substitution_findings`) rather than
    per metric: a rung whose skips another rung covers reports every one of its
    zeros as redundancy, while a rung that genuinely earns skips still reports
    ``did_not_pay`` on a metric its work does not move.
    """
    if not deltas:
        return VERDICT_INSIDE_NOISE
    if all(d == 0 for d in deltas):
        # MARGINAL zero. Whether that is absence or redundancy is decided by
        # what the run RECORDED, never assumed.
        return VERDICT_REDUNDANT if substituted else VERDICT_DID_NOT_PAY
    lo, hi = min(deltas), max(deltas)
    if lo <= 0 <= hi:
        # the sign is not stable across seeds — exactly the shape of the effect
        # this benchmark already retracted once. Never published as an effect.
        return VERDICT_INSIDE_NOISE
    polarity = METRIC_COST_DIRECTION.get(metric, 0)
    if polarity == 0:
        return VERDICT_MOVED_NO_COST_DIRECTION
    saved = (hi < 0) if polarity < 0 else (lo > 0)
    return VERDICT_PAID if saved else VERDICT_COST_MORE


ATTRIBUTION_METRICS: tuple[str, ...] = (
    "actions_reused",
    "charged_units",
    "cost_usd",
    "analysis_work_units",
    "evidence_lifetime_generations",
    "invalidations",
)

#: The EXTENSIVE metrics — counts and dollars, which sum, so
#: Sum(marginals) vs the measured total is a meaningful reconciliation.
#: ``evidence_lifetime_generations`` is deliberately absent: it is a MEAN over
#: certificates, and differences of means do not add.
RESIDUAL_METRICS: tuple[str, ...] = (
    "actions_reused",
    "charged_units",
    "cost_usd",
    "analysis_work_units",
    "invalidations",
)


@dataclass(frozen=True)
class AblationReport:
    """The whole E3 table: every arm on every seed, plus per-rung attribution."""

    measurements: tuple[ArmMeasurement, ...]
    attributions: tuple[RungAttribution, ...] = ()
    objective_bounds: Mapping[str, Any] = field(
        default_factory=lambda: dict(OBJECTIVE_BOUNDS_ROW)
    )

    @property
    def seeds(self) -> tuple[int, ...]:
        return tuple(sorted({m.seed for m in self.measurements}))

    def by_arm(self, arm: str) -> list[ArmMeasurement]:
        return [m for m in self.measurements if m.arm == arm]

    def baseline_for(self, seed: int) -> ArmMeasurement:
        for m in self.measurements:
            if m.seed == seed and m.arm == BASELINE_ARM:
                return m
        raise SubstrateError(
            "the ablation has no all-rungs baseline arm for the seed; a per-rung "
            "delta must be measured BY DIFFERENCE against the same seed's "
            "baseline, never against another run",
            data={"seed": seed},
        )

    # -- the loud checks the acceptances rest on ----------------------------- #
    def switch_violations(self) -> list[dict[str, Any]]:
        """Arms whose disabled rung still stamped a skip (the switch did nothing)."""
        return [
            {"seed": m.seed, "arm": m.arm, "violations": m.rung_violations()}
            for m in self.measurements if m.rung_violations()
        ]

    def reachability_violations(self) -> list[dict[str, Any]]:
        """Arms where ablating one rung silently killed ANOTHER rung — a collapse
        of the ladder rather than the removal of one mechanism."""
        out: list[dict[str, Any]] = []
        for m in self.measurements:
            if m.rungs.is_complete_ladder or not m.rungs.active:
                continue
            baseline_fired = self.baseline_for(m.seed).rungs_that_fired()
            expected = baseline_fired & m.rungs.active
            missing = sorted(expected - m.rungs_that_fired())
            if missing:
                out.append({"seed": m.seed, "arm": m.arm,
                            "rungs_that_stopped_firing": missing})
        return out

    def answer_violations(self) -> list[dict[str, Any]]:
        """Arms whose ANSWER differs from the complete-ladder arm's on the same
        seed. Must be empty: a rung that is load-bearing for CORRECTNESS rather
        than for cost is a significant finding and surfaces HERE, loudly."""
        out: list[dict[str, Any]] = []
        for seed in self.seeds:
            baseline = self.baseline_for(seed)
            for m in self.measurements:
                if m.seed != seed or m.arm == BASELINE_ARM:
                    continue
                if m.answer_digest != baseline.answer_digest:
                    out.append({
                        "seed": seed, "arm": m.arm,
                        "baseline_answer_digest": baseline.answer_digest,
                        "arm_answer_digest": m.answer_digest,
                    })
        return out

    def roster_anomalies(self) -> list[dict[str, Any]]:
        """Loser-roster differences that are NOT explained by what an arm withheld.

        The answer digest deliberately excludes the roster (§9.7 licenses a
        dominance skip to leave a loser unextracted). This makes that exclusion
        ACCOUNTABLE rather than silent: an arm's roster may differ from the
        baseline's by exactly the claims one of the two withheld, and by nothing
        else. Anything else is an unexplained divergence and must surface."""
        out: list[dict[str, Any]] = []
        for seed in self.seeds:
            baseline = self.baseline_for(seed)
            for m in self.measurements:
                if m.seed != seed or m.arm == BASELINE_ARM:
                    continue
                licensed = set(baseline.withheld_claim_ids) | set(m.withheld_claim_ids)
                for bucket in sorted(set(baseline.loser_roster) | set(m.loser_roster)):
                    delta = set(baseline.loser_roster.get(bucket, ())) ^ set(
                        m.loser_roster.get(bucket, ())
                    )
                    unexplained = sorted(delta - licensed)
                    if unexplained:
                        out.append({
                            "seed": seed, "arm": m.arm, "bucket": bucket,
                            "unexplained_claims": unexplained,
                        })
        return out

    # -- non-additivity, made visible rather than left inferable -------------- #
    def _arms_for(self, seed: int) -> dict[str, ArmMeasurement]:
        return {m.arm: m for m in self.measurements if m.seed == seed}

    def attribution_residuals(self) -> list[dict[str, Any]]:
        """Sum(one-at-a-time marginals) versus the MEASURED total saving.

        The all-off arm already bounds the total saving; this differences the two
        so the shortfall is a recorded figure rather than something a reader must
        derive. A nonzero residual means the marginals are NOT additive: work
        that some rung did under the complete ladder is attributed to NO rung by
        the one-at-a-time table, because a disabled rung's skips were re-earned
        by a rung that was still on.

        Restricted to the EXTENSIVE metrics — counts and dollars, which sum.
        ``evidence_lifetime_generations`` is a MEAN over certificates, so
        differences of it do not add up and a 'residual' would be meaningless.
        """
        out: list[dict[str, Any]] = []
        for seed in self.seeds:
            arms = self._arms_for(seed)
            baseline = arms.get(BASELINE_ARM)
            all_off = arms.get(NO_RUNGS_ARM)
            singles = {
                rung: arms.get(ReuseRungConfig.without(rung).arm_label)
                for rung in REUSE_RUNGS
            }
            if baseline is None or all_off is None or any(
                m is None for m in singles.values()
            ):
                continue
            for metric in RESIDUAL_METRICS:
                base_value = float(getattr(baseline, metric))
                marginals = {
                    rung: round(float(getattr(m, metric)) - base_value, 6)
                    for rung, m in singles.items() if m is not None
                }
                total = round(float(getattr(all_off, metric)) - base_value, 6)
                summed = round(sum(marginals.values()), 6)
                residual = round(total - summed, 6)
                out.append({
                    "seed": seed,
                    "metric": metric,
                    "per_rung_marginals": {r: marginals[r] for r in REUSE_RUNGS},
                    "sum_of_marginals": summed,
                    "measured_total": total,
                    "residual": residual,
                    "residual_share_of_total": (
                        round(abs(residual) / abs(total), 4) if total else None
                    ),
                    "additive": residual == 0,
                })
        return out

    def seeds_without_residual(self) -> list[dict[str, Any]]:
        """Seeds whose residual could not be computed, and WHY.

        Recorded rather than left as a silent absence: a reader who finds no
        residual row for a seed must be able to tell a missing arm from an
        additive one."""
        out: list[dict[str, Any]] = []
        for seed in self.seeds:
            arms = self._arms_for(seed)
            needed = {BASELINE_ARM, NO_RUNGS_ARM} | {
                ReuseRungConfig.without(rung).arm_label for rung in REUSE_RUNGS
            }
            missing = sorted(needed - set(arms))
            if missing:
                out.append({"seed": seed, "arms_not_run": missing,
                            "reason": "a residual needs the baseline, every "
                                      "single-rung arm and the all-off arm"})
        return out

    def substitution_findings(self) -> list[dict[str, Any]]:
        """Rungs that FIRED under the complete ladder yet whose one-at-a-time
        marginal on ``actions_reused`` is zero — the skips were re-earned by
        another rung, so the marginal is zero and the gross is not.

        This is the finding a direction-blind, gross-blind attribution table
        reports as 'the workload never exercises this rung', which is false: it
        exercises it, and another rung covers it."""
        out: list[dict[str, Any]] = []
        for seed in self.seeds:
            arms = self._arms_for(seed)
            baseline = arms.get(BASELINE_ARM)
            if baseline is None:
                continue
            for rung in REUSE_RUNGS:
                ablated = arms.get(ReuseRungConfig.without(rung).arm_label)
                if ablated is None:
                    continue
                gross = int(baseline.cause_tally.get(RUNG_SKIP_CAUSE[rung], 0))
                marginal = ablated.actions_reused - baseline.actions_reused
                if gross <= 0 or marginal != 0:
                    continue
                absorbed = {
                    cause: ablated.cause_tally.get(cause, 0)
                    - baseline.cause_tally.get(cause, 0)
                    for cause in sorted(
                        set(baseline.cause_tally) | set(ablated.cause_tally)
                    )
                }
                out.append({
                    "seed": seed,
                    "rung": rung,
                    "gross_skips_under_complete_ladder": gross,
                    "marginal_actions_reused": marginal,
                    "absorbed_by_causes": {
                        c: d for c, d in absorbed.items() if d > 0
                    },
                    "reading": (
                        "every skip this rung earned was ALSO earnable by "
                        "another rung on this workload, so its MARGINAL "
                        "contribution is zero while its GROSS contribution is "
                        f"{gross} skips. A zero marginal here is REDUNDANCY "
                        "between rungs, not a rung the workload never reached."
                    ),
                })
        return out

    def verdict_violations(self) -> list[dict[str, Any]]:
        """Arms whose differential run did not equal the complete reference."""
        return [
            {"seed": m.seed, "arm": m.arm, "verdicts": dict(m.verdicts)}
            for m in self.measurements
            if m.verdicts.get("mismatch", 0) or m.verdicts.get("excluded", 0)
        ]

    def to_canonical_dict(self) -> dict[str, Any]:
        return {
            "pact": "concord-e3-mechanism-ablations",
            "requirement": "req.e3.per-rung-attribution",
            "deployment": DEPLOYMENT,
            "seeds": list(self.seeds),
            "arms": [c.arm_label for c in ABLATION_ARMS],
            "price_table": dict(ABLATION_PRICE_TABLE),
            "decision_fields": list(DECISION_FIELDS),
            "roster_fields_licensed_to_differ": list(ROSTER_FIELDS),
            "caveats": dict(REPORT_CAVEATS),
            "workloads": [
                m.workload.to_canonical_dict()
                for m in self.measurements if m.arm == BASELINE_ARM
            ],
            "measurements": [m.to_canonical_dict() for m in self.measurements],
            "attributions": [a.to_canonical_dict() for a in self.attributions],
            "metric_cost_direction": dict(METRIC_COST_DIRECTION),
            # Sum(one-at-a-time marginals) vs the measured total, so a reader of
            # the JSON alone sees that the marginals do not add up rather than
            # having to derive it from the arm rows.
            "attribution_residuals": self.attribution_residuals(),
            "seeds_without_residual": self.seeds_without_residual(),
            "substitution_findings": self.substitution_findings(),
            "objective_bounds": dict(self.objective_bounds),
            "checks": {
                "switch_violations": self.switch_violations(),
                "reachability_violations": self.reachability_violations(),
                "answer_violations": self.answer_violations(),
                "roster_anomalies": self.roster_anomalies(),
                "verdict_violations": self.verdict_violations(),
            },
        }

    def render(self) -> str:
        lines: list[str] = []
        lines.append(f"E3 mechanism ablation — {DEPLOYMENT} — seeds {list(self.seeds)}")
        lines.append("")
        header = (
            f"{'seed':>4} {'arm':<28} {'reach':>6} {'reused':>7} {'analysis':>9} "
            f"{'units':>7} {'cost_usd':>9} {'life':>6} {'inval':>6} {'answer':>10}"
        )
        lines.append(header)
        lines.append("-" * len(header))
        for m in sorted(self.measurements, key=lambda m: (m.seed, m.arm)):
            lines.append(
                f"{m.seed:>4} {m.arm:<28} {m.actions_reached:>6} "
                f"{m.actions_reused:>7} {m.analysis_work_units:>9} "
                f"{m.charged_units:>7} {m.cost_usd:>9.2f} "
                f"{m.evidence_lifetime_generations:>6.2f} {m.invalidations:>6} "
                f"{m.answer_digest[:10]:>10}"
            )
        lines.append("")
        lines.append("PER-RUNG ATTRIBUTION (delta vs the all-rungs arm on the SAME seed)")
        lines.append("")
        head2 = (
            f"{'rung':<24} {'metric':<30} {'mean':>10} {'min':>10} {'max':>10} "
            f"{'gross':>6}  verdict"
        )
        lines.append(head2)
        lines.append("-" * len(head2))
        for a in self.attributions:
            lo, hi = a.spread
            lines.append(
                f"{a.rung:<24} {a.metric:<30} {a.mean:>10.3f} {lo:>10.3f} "
                f"{hi:>10.3f} {sum(a.gross_firings.values()):>6}  {a.verdict}"
            )
        lines.append(
            f"{OBJECTIVE_BOUNDS_ROW['ablation']:<24} "
            f"{'(all)':<30} {'n/a':>10} {'n/a':>10} {'n/a':>10} {'n/a':>6}  "
            f"{OBJECTIVE_BOUNDS_ROW['status']} — {OBJECTIVE_BOUNDS_ROW['citation']}"
        )

        residuals = self.attribution_residuals()
        if residuals:
            lines.append("")
            lines.append(
                "ADDITIVITY — Sum(one-at-a-time marginals) vs the MEASURED total "
                "(all_rungs vs no_rungs)"
            )
            lines.append("")
            head3 = (
                f"{'seed':>4} {'metric':<24} {'sum_marginals':>14} "
                f"{'measured_total':>15} {'residual':>10} {'share':>7}"
            )
            lines.append(head3)
            lines.append("-" * len(head3))
            for row in residuals:
                share = row["residual_share_of_total"]
                lines.append(
                    f"{row['seed']:>4} {row['metric']:<24} "
                    f"{row['sum_of_marginals']:>14.3f} "
                    f"{row['measured_total']:>15.3f} {row['residual']:>10.3f} "
                    f"{('n/a' if share is None else f'{share:.1%}'):>7}"
                )
        for row in self.seeds_without_residual():
            lines.append(f"  seed {row['seed']}: residual NOT MEASURED — "
                         f"arms not run: {row['arms_not_run']}")
        findings = self.substitution_findings()
        if findings:
            lines.append("")
            lines.append("SUBSTITUTION — rungs that FIRED yet have a zero marginal")
            for row in findings:
                lines.append(
                    f"  seed {row['seed']} {row['rung']}: gross="
                    f"{row['gross_skips_under_complete_ladder']} skips, marginal=0, "
                    f"absorbed by {row['absorbed_by_causes']}"
                )
        return "\n".join(lines)

    def write(self, path: Path | str) -> Path:
        out = Path(path)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(
            json.dumps(self.to_canonical_dict(), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return out


def attribute(measurements: Sequence[ArmMeasurement]) -> tuple[RungAttribution, ...]:
    """Per-rung deltas against the same seed's all-rungs arm, as a spread."""
    report = AblationReport(measurements=tuple(measurements))
    out: list[RungAttribution] = []
    substituted_rungs = {f["rung"] for f in report.substitution_findings()}
    for rung in REUSE_RUNGS:
        arm_label = ReuseRungConfig.without(rung).arm_label
        cause = RUNG_SKIP_CAUSE[rung]
        # decided once per rung from the recorded SKIP counts, so a zero on any
        # metric of a fully-covered rung reads as redundancy rather than absence.
        substituted = rung in substituted_rungs
        for metric in ATTRIBUTION_METRICS:
            deltas: dict[int, float] = {}
            gross: dict[int, int] = {}
            for m in measurements:
                if m.arm != arm_label:
                    continue
                baseline = report.baseline_for(m.seed)
                deltas[m.seed] = round(
                    float(getattr(m, metric)) - float(getattr(baseline, metric)), 6
                )
                # the rung's GROSS work under the complete ladder — what a zero
                # marginal must be read against before calling the rung idle.
                gross[m.seed] = int(baseline.cause_tally.get(cause, 0))
            if deltas:
                out.append(RungAttribution(
                    rung=rung, metric=metric, per_seed_deltas=deltas,
                    verdict=_classify(
                        list(deltas.values()), metric, substituted=substituted,
                    ),
                    gross_firings=gross,
                ))
    return tuple(out)


def run_ablation(
    *,
    seeds: Sequence[int],
    deployment_dir: Path | str,
    production_dsn_factory: Callable[[], str],
    scratch_dsn_factory: Callable[[], str],
    arms: Sequence[ReuseRungConfig] = ABLATION_ARMS,
    declare_nothing_changed: bool = True,
) -> AblationReport:
    """Run every arm on every seed and build the report.

    ``production_dsn_factory`` yields a FRESH production database per arm. That is
    not incidental: an arm must not inherit certificates a disabled rung wrote in
    an earlier arm, or the ablation would carry the removed mechanism's saving
    forward under the ``certificate_valid`` cause."""
    measurements: list[ArmMeasurement] = []
    for seed in seeds:
        for rungs in arms:
            measurement = run_arm(
                seed=seed, rungs=rungs, deployment_dir=deployment_dir,
                production_dsn=production_dsn_factory(),
                scratch_dsn_factory=scratch_dsn_factory,
                declare_nothing_changed=declare_nothing_changed,
            )
            measurement.check()
            measurements.append(measurement)
    return AblationReport(
        measurements=tuple(measurements),
        attributions=attribute(measurements),
    )
