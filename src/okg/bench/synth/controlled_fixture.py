"""The paper's "Controlled fixture" row, filled from ONE replayed history.

PACT ``concord-bench-e2e``, ``req.e2e.capstone`` — the completion gate of the
Concord program. The four children that landed before it each produce one part
of the row and none of them produces the row: ``synth-update-histories`` seals
the history and the per-step gold, ``concord-complete-binding`` maps the corpus
onto the scheduler's vocabulary, ``concord-exact-parity`` replays that history
through three execution modes and grades per-update parity, and
``synth-cost-weighted-gold`` prices counts into dollars. This module is the
composition, and it invents no measurement of its own: every cell is READ from
run output (a pinned generation's funnel, a mode's work ledger, a per-update
parity verdict) or from the sealed gold the run was graded against. Nothing is
read from configuration, and nothing is estimated.

Two disciplines make the row auditable rather than merely printed.

TRACEABILITY. Every cell is a :func:`cell` — a value plus the provenance that
produced it: which pinned generation it was read at, which artifact it came out
of, and which field of that artifact. A reader checks any number by opening the
named artifact at the named pin; re-running the history is never required to
audit a cell.

REFUSAL OVER BLANKS. :func:`assert_row_invariants` runs at BUILD time and fails
loudly: the parity verdicts must cover every update of the history, all three
execution modes must be present in the ledger and in the cost block, each mode's
priced action classes must reconstruct its total, qualification must be stated,
and the detection denominators must be non-zero. A missing input is a raised
:class:`ControlledFixtureError`, never a blank cell that reads as a zero.

DEFERRED CELLS. Two cells cannot be produced honestly here, and both are marked
:data:`DEFERRED` with the gating surface named — never filled with a proxy
number, and never rendered as ``0``:

* remaining error cost AT DECLARED BUDGETS, and
* disclosure misses,

both gated on ``concord-disclosure-budget`` / ``task.budget.gate``, whose status
is ``blocked`` because the approximate (budget-respecting) scheduler does not
exist. The UNBUDGETED remaining error cost IS produced, from the same funnel
every count cell reads, and is a distinct cell: a run's standing error cost at
its own operating point is a measurement, while its error cost under a declared
budget is a capability the engine does not yet have.

THE ZERO THAT IS EVIDENCE. The exact-mode row reports zero unsound reuses over
the history. A zero is otherwise indistinguishable from an untested mechanism,
so the safety mutations ride WITH it, in the same cell: each mutation moves one
input the scheduler is entitled to trust, and each is recorded with what FIRED
(a certificate invalidated and the step ran, or the pass refused to commit). The
zero is evidence because those witnesses show the mechanism can be made to
report otherwise.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from okg.bench.synth.config import ACTION_CLASSES
from okg.bench.synth.cost import (
    action_counts_from_step_ledger,
    qualification,
    remaining_error_cost,
    sealed_cost_model,
    spent_action_cost,
)
from okg.bench.synth.history_replay import materialize_base, materialize_step
from okg.bench.synth.parity import (
    MODE_COMPLETE,
    MODE_DEPENDENCY,
    MODE_DIFFERENTIAL,
    MODES,
    MUTATION_CORRUPT_CERTIFICATE,
    MUTATION_DEPENDENCY_FACT,
    MUTATION_ENVELOPE_VIOLATION,
    MUTATION_PRIOR_RESULT_DELETED,
    MUTATION_VACUOUS_CHANGE,
    MUTATIONS,
    VERDICT_CERTIFIED,
    VERDICT_EQUAL,
    ModeRun,
    ModeState,
    Mutation,
    ParityHistory,
    ParityReplayError,
    UpdateVerdict,
    grade_update,
    replay_mode,
    run_update,
)
from okg.bench.synth.resolution import corruption_bucket_map, funnel
from okg.bench.synth.runner import (
    SMOKE_DEPLOYMENT,
    RunnerError,
    synth_survival_equivalent,
)
from okg.substrate.runtime.reuse_pass_workflow import (
    CLAIM_EFFECTIVE_STEP,
    CLAIM_SOURCE_STEP,
)

CONTROLLED_FIXTURE_SCHEMA = "okg.synth.controlled-fixture-row.v1"
#: The row's artifact name. Named here rather than spelled out at each call site
#: so the durable record set (:mod:`okg.bench.synth.durable`) and the writers
#: cannot drift apart and leave the row out of a stored bundle.
CONTROLLED_FIXTURE_FILENAME = "controlled-fixture-row.json"

#: The marker a deferred cell carries in place of a value. A STRING on purpose:
#: a reader (and ``json.load``) can never mistake it for a measured zero, and an
#: arithmetic consumer that tries to sum it raises rather than silently counting
#: it as nothing.
DEFERRED = "DEFERRED"

#: The gated child that would un-defer the budget and disclosure cells.
BUDGET_GATE = "concord-disclosure-budget / task.budget.gate (status: blocked)"

#: What a $0.00 action-cost total MEANS on a corpus that declares no prices.
#: Carried inside the cell, so the zero can never be read as a measured saving.
UNPRICED_NOTE = (
    "this corpus seals NO action prices (every class defaults to 0.0), so every "
    "mode's dollar total is 0.00 BY CONSTRUCTION and the dollar figures compare "
    "nothing. The three-mode comparison this row states is the ACTION COUNTS in "
    "the adjacent action_counts cell, which are the work ledger in the units it "
    "charges. A corpus that declares cost.action_prices flips this cell to a "
    "dollar comparison with no other change"
)

#: The finding channels that carry a CONFLICT signal. The same filter the
#: scorecard uses for its false-positive accounting (``runner`` §"clean-run FPs
#: use the SAME channel filter"), so precision here and detection there are the
#: one predicate.
CONFLICT_CHANNELS = frozenset({"definite", "possible"})

#: The exact execution mode: scheduler-governed differential.
MODE_EXACT = MODE_DIFFERENTIAL

#: The four classes every flagged bucket falls into, exactly one each.
#:
#: ``TRUE_POSITIVE``  the bucket carries a corruption the history declares LIVE
#:                    at this step. The only class that is a detection.
#: ``EXTRACTOR_FP``   the bucket holds a claim the sealed corpus does not carry
#:                    (invented, mis-keyed, misread). The engine was handed a
#:                    contradiction that the documents do not contain, so the
#:                    flag is an extraction defect and is not charged to it.
#: ``ROSTER_ARTIFACT``the SEALED per-step gold declares this bucket CONFLICTED,
#:                    but the live CORRUPTION roster does not enumerate it. The
#:                    engine agreed with gold; the roster is simply a narrower
#:                    object than the conflict set (see :func:`bucket_conflict_causes`).
#: ``ENGINE_FP``      none of the above: real claims, no sealed conflict, flagged
#:                    anyway. The only class that debits the engine.
CLASS_TRUE_POSITIVE = "true_positive"
CLASS_EXTRACTOR_FP = "extractor_false_positive"
CLASS_ROSTER_ARTIFACT = "roster_artifact"
CLASS_ENGINE_FP = "engine_false_positive"
BUCKET_CLASSES = (
    CLASS_TRUE_POSITIVE,
    CLASS_EXTRACTOR_FP,
    CLASS_ROSTER_ARTIFACT,
    CLASS_ENGINE_FP,
)

#: History delta kinds that PLANT a contradicting claim into a bucket. A
#: ``dominated_claim`` update restates the bucket's existing value and is
#: deliberately absent: it plants no disagreement, so it can never excuse a flag.
CONFLICTING_DELTA_KINDS = frozenset({"fresh_conflict", "bucket_increment"})


class ControlledFixtureError(RunnerError):
    """A cell could not be produced from run output, so the row refuses to build."""


# --------------------------------------------------------------------------- #
# Cells                                                                        #
# --------------------------------------------------------------------------- #
def cell(value: Any, *, source: str, **provenance: Any) -> dict[str, Any]:
    """One traceable cell: a value plus where it was READ from.

    ``source`` names the artifact (a mode's work ledger, a pinned generation's
    funnel, the parity verdict list); the keyword provenance names the pin and
    the field inside it. A cell with no source is refused — an untraceable
    number is exactly what this row exists to rule out."""
    if not source:
        raise ControlledFixtureError("a cell must name the artifact it was read from")
    if value is None:
        raise ControlledFixtureError(
            f"cell from {source!r} carries no value; a cell that cannot be "
            "produced must be DEFERRED with its gating surface named, never left "
            "empty (see deferred_cell)"
        )
    return {"value": value, "provenance": {"source": source, **provenance}}


def deferred_cell(*, gating_surface: str, reason: str) -> dict[str, Any]:
    """A cell that CANNOT be produced honestly, marked so it cannot read as zero."""
    if not gating_surface or not reason:
        raise ControlledFixtureError(
            "a DEFERRED cell must name both its gating surface and the reason; an "
            "unexplained deferral is indistinguishable from an omission"
        )
    return {
        "value": DEFERRED,
        "deferred": True,
        "gating_surface": gating_surface,
        "reason": reason,
    }


def is_deferred(entry: Mapping[str, Any]) -> bool:
    return bool(entry.get("deferred")) or entry.get("value") == DEFERRED


def _rate(numerator: int, denominator: int) -> float | None:
    return None if not denominator else numerator / denominator


# --------------------------------------------------------------------------- #
# Reading ONE execution mode                                                   #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ModeReading:
    """Everything the row needs from ONE execution mode, all read from its run.

    Built by :func:`read_mode` from a :class:`~okg.bench.synth.parity.ModeRun`
    alone: the work ledger the mode charged, and the pinned read each update
    already performed (``UpdateExecution.detection`` / ``.resolutions``). No
    second database read, so the row and the replay can never disagree about
    what the run produced."""

    mode: str
    updates: int
    final_pin: str
    ledger_totals: Mapping[str, Any]
    funnel: Mapping[str, Any]
    precision: Mapping[str, Any]
    attribution: Mapping[str, Any]
    action_counts: Mapping[str, int]
    check_pins: tuple[str, ...]
    live_corruptions: tuple[str, ...]


def live_manifest(
    manifest: Mapping[str, Any], live: Sequence[str]
) -> dict[str, Any]:
    """``manifest`` with the gold corruption roster narrowed to the LIVE set.

    A history plants and unplants errors (``plant_error`` / ``unplant_error``),
    so the corruption set at a pin is a property of the STEP, not of the sealed
    corpus. Grading the final pin against the full roster would debit the engine
    for corruptions the corpus no longer carries. This is the same narrowing
    ``history_replay`` applies per step, restated here over the same field so
    the two readings cannot drift."""
    live_ids = set(live)
    roster = [
        site
        for site in manifest["gold"]["corruptions"]
        if site["corruption_id"] in live_ids
    ]
    missing = live_ids - {site["corruption_id"] for site in roster}
    if missing:
        raise ControlledFixtureError(
            f"the history declares corruption(s) {sorted(missing)} live at this "
            f"pin, but the sealed manifest carries no such gold site; the funnel "
            f"would grade a corruption it cannot see"
        )
    return {**manifest, "gold": {**manifest["gold"], "corruptions": roster}}


def detection_precision(
    *, findings: Sequence[Any], gold_buckets: frozenset[str],
    surviving_gold_buckets: frozenset[str],
) -> dict[str, Any]:
    """Detection PRECISION at one pin, in the funnel's two conditionings.

    The funnel answers recall (of the corruptions planted, how many were
    noticed); precision is the other half and needs the flagged set, which is a
    property of the engine's FINDING rows rather than of gold. A bucket the
    engine flagged on a conflict channel is a TRUE positive iff it carries a live
    gold corruption.

    The two conditionings differ in what counts as a true positive, exactly as
    they do for recall:

    ``end_to_end``    every live gold bucket counts. This is precision against
                      what was PLANTED.
    ``conditional``   only gold buckets whose corruption SURVIVED extraction
                      count. A flag on a bucket whose conflict extraction
                      destroyed is not evidence of detection under the
                      conditional reading, so it is charged as a false positive
                      — the strictly harsher of the two, and the one that cannot
                      be flattered by extraction loss.

    The denominator is the flagged set in both, so the two rates are comparable
    and their difference is exactly the flags on non-surviving sites."""
    flagged = frozenset(
        str(f.bucket_key) for f in findings if f.channel in CONFLICT_CHANNELS
    )
    e2e_tp = flagged & gold_buckets
    cond_tp = flagged & surviving_gold_buckets
    return {
        "flagged_buckets": len(flagged),
        "end_to_end": {
            "denominator": len(flagged),
            "true_positive": len(e2e_tp),
            "false_positive": len(flagged - gold_buckets),
            "rate": _rate(len(e2e_tp), len(flagged)),
        },
        "conditional": {
            "denominator": len(flagged),
            "true_positive": len(cond_tp),
            "false_positive": len(flagged - surviving_gold_buckets),
            "rate": _rate(len(cond_tp), len(flagged)),
        },
        "false_positive_buckets": sorted(flagged - gold_buckets),
    }


# --------------------------------------------------------------------------- #
# Attributing every flagged bucket                                             #
# --------------------------------------------------------------------------- #
def sealed_surface_keys(
    manifest: Mapping[str, Any], history: Any, index: int
) -> dict[tuple[str, str, str], frozenset[str]]:
    """``(doc, predicate, entity)`` -> the survival keys the SEALED corpus carries.

    The ground truth an extracted claim is checked against, computed at STEP
    ``index`` rather than over the frozen corpus. Two sources, both sealed and
    both gold-side:

    * the manifest's trace, one row per base rendering, valued at the site's
      ``post_text`` when that corruption is LIVE at this step and at its
      ``pre_text`` when an update unplanted it; and
    * the documents the history's own updates added, valued at the delta's
      sealed surface.

    :func:`~okg.bench.synth.runner._attribute_unexpected_findings` answers the
    same question for the FROZEN-corpus lane, where neither source moves. Reusing
    it over a history would call every update-document claim invented (no trace
    row exists for a document the base corpus never had) and every unplanted
    site's restored claim misread, charging the extractor for both — which is
    exactly the misattribution this function exists to avoid.
    """
    from okg.bench.synth.history import _base_claim_rows
    from okg.bench.synth.runner import _survival_key

    live = set(history.steps[index].live_corruptions)
    live_by_rendering = {
        (site["fact_id"], site["doc"]): site
        for site in manifest["gold"]["corruptions"]
        if site["corruption_id"] in live
    }
    out: dict[tuple[str, str, str], set[str]] = {}
    for row in _base_claim_rows(manifest):
        site = live_by_rendering.get((row["fact_id"], row["doc"]))
        surface = site["post_text"] if site is not None else row["clean_surface"]
        predicate, entity = row["bucket"].split(":", 1)
        out.setdefault((row["doc"], predicate, entity), set()).add(
            _survival_key(surface)
        )
    for step in history.steps[: index + 1]:
        delta = step.delta
        doc = str(dict(delta.detail).get("doc") or "")
        value = dict(delta.detail).get("value")
        if not delta.bucket or value is None or not doc:
            continue
        predicate, entity = delta.bucket.split(":", 1)
        out.setdefault((doc, predicate, entity), set()).add(_survival_key(value))
    return {key: frozenset(values) for key, values in out.items()}


def bucket_conflict_causes(history: Any, index: int) -> dict[str, list[dict[str, Any]]]:
    """bucket -> the SEALED history deltas that put a contradiction in it.

    Two shapes, and only two, both read off the sealed step list:

    ``history_update_conflict``  an update added a document whose claim
                                 DISAGREES with the bucket's standing claims
                                 (``fresh_conflict`` / ``bucket_increment``).
                                 The history's arm buckets are chosen disjoint
                                 from every corruption bucket, so a conflict
                                 planted this way can never appear in the gold
                                 corruption roster.
    ``unplanted_corruption``     a corruption was planted in this bucket and a
                                 later update unplanted it, so a finding here is
                                 the residue of a real prior conflict.

    A ``dominated_claim`` update restates the standing value and is not a cause;
    if it were, an update that plants no disagreement could excuse a flag.
    """
    causes: dict[str, list[dict[str, Any]]] = {}
    planted: dict[str, tuple[int, str]] = {}
    for step in history.steps[: index + 1]:
        delta = step.delta
        if delta.kind in CONFLICTING_DELTA_KINDS and delta.bucket:
            causes.setdefault(delta.bucket, []).append({
                "cause": "history_update_conflict",
                "delta": delta.kind,
                "update": step.index,
                "doc": str(dict(delta.detail).get("doc") or ""),
                "value": str(dict(delta.detail).get("value") or ""),
            })
        elif delta.kind == "plant_error" and delta.corruption_id:
            planted[delta.corruption_id] = (step.index, delta.bucket)
        elif delta.kind == "unplant_error" and delta.corruption_id:
            plant = planted.get(delta.corruption_id)
            if plant is not None and plant[1]:
                causes.setdefault(plant[1], []).append({
                    "cause": "unplanted_corruption",
                    "delta": delta.kind,
                    "corruption_id": delta.corruption_id,
                    "planted_at": plant[0],
                    "update": step.index,
                })
    return causes


def classify_flagged_buckets(
    *,
    findings: Sequence[Any],
    claims: Mapping[str, str],
    gold_buckets: frozenset[str],
    surviving_gold_buckets: frozenset[str],
    gold_conflicted: frozenset[str],
    sealed_keys: Mapping[tuple[str, str, str], frozenset[str]],
    causes: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, Any]:
    """Attribute EVERY flagged bucket, then report precision with and without
    the roster artifacts.

    The raw figure grades the flagged set against the LIVE CORRUPTION ROSTER: a
    bucket is a true positive iff it carries a corruption the history declares
    live. That is the right denominator for "of the errors we planted, how many
    did it find" and the wrong one for "when it flags, is it right", because the
    roster is not the conflict set. A history plants contradictions two other
    ways — an update that files a disagreeing claim, and a corruption a later
    update unplanted — and neither is in the roster.

    So each non-roster flag is attributed before the number is quoted, in a
    fixed order, and the order is stated because a bucket can satisfy more than
    one predicate (every predicate is recorded per bucket, so an overlap is
    visible rather than hidden by the order):

    1. EXTRACTOR — the bucket holds a claim the sealed corpus does not carry.
       First because a flag over invented claims says nothing about the engine
       either way, whatever gold says about the bucket.
    2. ROSTER — the SEALED per-step gold declares the bucket conflicted AND a
       sealed delta explains why. Both are required: gold alone would let an
       unexplained agreement stand as an excuse, and a delta alone would let a
       bucket whose conflict gold does not recognize pass as an artifact.
    3. ENGINE — everything else. Real claims, no sealed conflict, flagged anyway.

    ``engine`` is the corrected reading: roster artifacts and extractor defects
    leave BOTH the numerator and the denominator, so what remains is the flags
    the engine is answerable for. ``end_to_end`` / ``conditional`` keep the raw
    figure unchanged beside it — neither hides the other.
    """
    from okg.bench.synth.runner import _survival_key

    by_bucket: dict[str, dict[str, Any]] = {}
    for finding in findings:
        if finding.channel not in CONFLICT_CHANNELS:
            continue
        entry = by_bucket.setdefault(
            str(finding.bucket_key), {"channels": set(), "claim_ids": set()}
        )
        entry["channels"].add(str(finding.channel))
        entry["claim_ids"].update(str(cid) for cid in finding.claim_ids)

    table: list[dict[str, Any]] = []
    by_class: dict[str, list[str]] = {name: [] for name in BUCKET_CLASSES}
    for bucket in sorted(by_bucket):
        entry = by_bucket[bucket]
        members = {
            cid: claims.get(cid) for cid in sorted(entry["claim_ids"])
        }
        unsealed: list[dict[str, Any]] = []
        for claim_id, value in sorted(members.items()):
            parts = claim_id.split(":")
            if len(parts) != 5 or parts[0] != "claim" or value is None:
                unsealed.append({"claim_id": claim_id, "value": value,
                                 "why": "unparseable claim id or no value at the pin"})
                continue
            _c, doc, predicate, entity, _occ = parts
            sealed = sealed_keys.get((doc, predicate, entity), frozenset())
            if _survival_key(value) not in sealed:
                unsealed.append({
                    "claim_id": claim_id, "value": value,
                    "why": "no sealed surface in this document carries this value",
                    "sealed_surfaces": sorted(sealed),
                })
        bucket_causes = [dict(c) for c in causes.get(bucket, ())]
        conflicted = bucket in gold_conflicted
        if bucket in gold_buckets:
            classification = CLASS_TRUE_POSITIVE
            evidence = "carries a corruption the history declares LIVE at this step"
        elif unsealed:
            classification = CLASS_EXTRACTOR_FP
            evidence = (
                f"{len(unsealed)} claim(s) carry a value no sealed surface of "
                "their document holds"
            )
        elif conflicted and bucket_causes:
            classification = CLASS_ROSTER_ARTIFACT
            evidence = (
                "sealed per-step gold declares this bucket CONFLICTED "
                f"({sorted({str(c['cause']) for c in bucket_causes})}); the live "
                "corruption roster does not enumerate it"
            )
        elif conflicted:
            raise ControlledFixtureError(
                f"bucket {bucket!r} is declared CONFLICTED by the sealed gold at "
                "this step, but no sealed delta plants a contradiction in it; the "
                "row refuses to file an unexplained flag under any class"
            )
        else:
            classification = CLASS_ENGINE_FP
            evidence = (
                "every claim matches a sealed surface and the sealed gold does "
                "NOT declare this bucket conflicted"
            )
        by_class[classification].append(bucket)
        table.append({
            "bucket": bucket,
            "classification": classification,
            "channels": sorted(entry["channels"]),
            "claims": members,
            "in_live_gold_roster": bucket in gold_buckets,
            "gold_declares_conflicted": conflicted,
            "sealed_causes": bucket_causes,
            "unsealed_claims": unsealed,
            "evidence": evidence,
        })

    flagged = frozenset(by_bucket)
    excluded = frozenset(
        by_class[CLASS_ROSTER_ARTIFACT] + by_class[CLASS_EXTRACTOR_FP]
    )
    answerable = flagged - excluded
    engine_e2e = answerable & gold_buckets
    engine_cond = answerable & surviving_gold_buckets
    return {
        "table": table,
        "by_class": {name: sorted(by_class[name]) for name in BUCKET_CLASSES},
        "excluded_from_engine_precision": sorted(excluded),
        "engine": {
            "end_to_end": {
                "denominator": len(answerable),
                "true_positive": len(engine_e2e),
                "false_positive": len(by_class[CLASS_ENGINE_FP]),
                "rate": _rate(len(engine_e2e), len(answerable)),
            },
            "conditional": {
                "denominator": len(answerable),
                "true_positive": len(engine_cond),
                "false_positive": len(answerable - surviving_gold_buckets),
                "rate": _rate(len(engine_cond), len(answerable)),
            },
        },
        "gold_conflict_agreement": {
            "flagged_buckets": len(flagged),
            "gold_conflicted_buckets": len(gold_conflicted),
            "agreed": len(flagged & gold_conflicted),
            "flagged_not_conflicted": sorted(flagged - gold_conflicted),
            "conflicted_not_flagged": sorted(gold_conflicted - flagged),
        },
    }


def read_mode(
    *,
    run: ModeRun,
    manifest: Mapping[str, Any],
    parity: ParityHistory,
    delivering_docs: Mapping[int, frozenset[str]],
    equivalent: Any,
) -> ModeReading:
    """Reduce ONE mode's replay to the cells the row needs — from run output only.

    The funnel and precision are read at the mode's FINAL pin, against the
    corruption roster the history declares live at the final step. The action
    counts are reduced from the work ledger with the SAME bridge the scorecard
    uses (:func:`~okg.bench.synth.cost.action_counts_from_step_ledger`), so the
    three modes and the frozen-corpus scorecard bill from one table:

    ``parse``             one per document step that RAN (a step that runs reads
                          its document; a step that skips reads nothing).
    ``extract_document``  one per RAN document step that DELIVERED claims. Equal
                          to ``parse`` in a lossless lane, strictly fewer when a
                          document contributes nothing.
    ``check``             the candidate buckets the checker evaluated, summed
                          over the updates whose effective-merge step RAN. An
                          update that skipped the merge evaluated no candidate,
                          and an ABSORBED update evaluated none at all.
    ``publish``           one per generation the mode actually published.
    ``correction``        operator corrections applied. This history plants none
                          (the corrections lane is a separate arm), so it is a
                          measured zero rather than an omission."""
    if not run.executions:
        raise ControlledFixtureError(
            f"mode {run.mode!r} replayed ZERO updates; every cell derived from it "
            "would be vacuously true"
        )
    last = run.executions[-1]
    if last.detection is None:
        raise ControlledFixtureError(
            f"mode {run.mode!r} carries no pinned read at its final generation "
            f"{last.generation_id!r}; the funnel cannot be scored from run output"
        )
    live = tuple(parity.gold_step(last.index).live_corruptions)
    narrowed = live_manifest(manifest, live)
    if not narrowed["gold"]["corruptions"]:
        raise ControlledFixtureError(
            f"no corruption is live at the final step of the history, so every "
            f"detection cell for mode {run.mode!r} would be vacuous"
        )
    stages = funnel(
        manifest=narrowed,
        extracted_claims=last.detection.claims,
        resolutions=tuple(last.resolutions),
        findings=last.detection.findings,
        equivalent=equivalent,
    )
    buckets = corruption_bucket_map(narrowed)
    non_surviving = set(stages["per_class_survival"]["non_surviving_sites"])
    gold_buckets = frozenset(buckets.values())
    surviving_gold_buckets = frozenset(
        bucket for cid, bucket in buckets.items() if cid not in non_surviving
    )
    precision = detection_precision(
        findings=last.detection.findings,
        gold_buckets=gold_buckets,
        surviving_gold_buckets=surviving_gold_buckets,
    )
    # Every flagged bucket outside the live roster is attributed before the
    # precision figure is quotable. The inputs are all SEALED gold at this step —
    # the per-step expected state (which lists conflicted buckets and only those),
    # the per-step corpus surfaces, and the delta list — so the classification
    # cannot be moved by anything the engine did.
    attribution = classify_flagged_buckets(
        findings=last.detection.findings,
        claims=last.detection.claims,
        gold_buckets=gold_buckets,
        surviving_gold_buckets=surviving_gold_buckets,
        gold_conflicted=frozenset(parity.gold_step(last.index).expected_state),
        sealed_keys=sealed_surface_keys(manifest, parity.history, last.index),
        causes=bucket_conflict_causes(parity.history, last.index),
    )

    parsed = 0
    delivered = 0
    checks = 0
    check_pins: list[str] = []
    for execution in run.executions:
        ran_docs = [
            step[len(CLAIM_SOURCE_STEP) + 1:]
            for step in execution.run_steps
            if step.startswith(f"{CLAIM_SOURCE_STEP}:")
        ]
        parsed += len(ran_docs)
        with_claims = delivering_docs.get(execution.index)
        if with_claims is None:
            raise ControlledFixtureError(
                f"update {execution.index}: no recorded per-document claim "
                "partition, so extract_document cannot be distinguished from "
                "parse; refusing to bill the two as equal by assumption"
            )
        delivered += sum(1 for doc in ran_docs if doc in with_claims)
        if CLAIM_EFFECTIVE_STEP in execution.run_steps:
            if execution.detection is None:
                raise ControlledFixtureError(
                    f"update {execution.index} [{run.mode}]: the effective-merge "
                    "step ran but the update carries no pinned read, so the "
                    "checker's work surface cannot be counted"
                )
            checks += len(execution.detection.candidates)
            check_pins.append(execution.generation_id)

    counts = action_counts_from_step_ledger(
        documents_parsed=parsed,
        source_steps_run=delivered,
        checks_evaluated=checks,
        publishes=sum(1 for e in run.executions if e.published),
        corrections=0,
    )
    return ModeReading(
        mode=run.mode,
        updates=len(run.executions),
        final_pin=last.generation_id,
        ledger_totals=run.totals(),
        funnel=stages,
        precision=precision,
        attribution=attribution,
        action_counts=counts,
        check_pins=tuple(check_pins),
        live_corruptions=live,
    )


def delivering_documents(parity: ParityHistory) -> dict[int, frozenset[str]]:
    """update index -> the documents whose RECORDED fact set carries claims.

    Read from the recorded fact sets the history sealed, which is what every
    executor consumed, so ``extract_document`` is billed against the same bytes
    the run published rather than against a re-parse."""
    from okg.bench.synth.governed import partition_recorded_facts

    out: dict[int, frozenset[str]] = {}
    for update in parity.updates:
        facts = update.facts()
        docs = sorted({
            str(
                dict(getattr(fact, "source_record_id", {}) or {}).get("path")
                or dict(getattr(fact, "attrs", {}) or {}).get("source_id")
                or ""
            )
            for fact in facts
            if getattr(fact, "subtype", "") == "consistency_claim"
        })
        by_doc, _other = partition_recorded_facts(
            facts, [d for d in docs if d]
        )
        out[update.index] = frozenset(
            doc for doc, slice_ in by_doc.items() if slice_
        )
    return out


# --------------------------------------------------------------------------- #
# The safety witnesses: why the zero is evidence                               #
# --------------------------------------------------------------------------- #
#: Which update each mutation is applied at. Chosen for what the mutation needs
#: to be non-vacuous, not for speed: (a)(b)(d) need a prior certificate in the
#: store, so they run at an update whose predecessors published; (c) needs an
#: update where the producer really assembles, so a missing prior slice is
#: actually reached; (e) is a FIRST publish, where an empty change statement is
#: unsound by construction.
SAFETY_UPDATES: Mapping[str, int] = {
    MUTATION_DEPENDENCY_FACT: 2,
    MUTATION_ENVELOPE_VIOLATION: 2,
    MUTATION_PRIOR_RESULT_DELETED: 4,
    MUTATION_CORRUPT_CERTIFICATE: 2,
    MUTATION_VACUOUS_CHANGE: 0,
}


def _build_to(
    *, dsn, deployment_dir, corpus_dir: Path, parity: ParityHistory, policies,
    checks, upto: int, deployment: str, reset,
) -> ModeState:
    """Replay updates ``0..upto-1`` in exact mode and hand back the live state."""
    reset()
    materialize_base(parity.history, corpus_dir)
    state = ModeState()
    for update in parity.updates[:upto]:
        materialize_step(parity.history, update.index, corpus_dir)
        run_update(
            dsn=dsn, deployment_dir=Path(deployment_dir), corpus_dir=corpus_dir,
            mode=MODE_EXACT, update=update,
            gold_step=parity.gold_step(update.index), policies=policies,
            checks=checks, state=state, deployment=deployment,
        )
    return state


def _run_one(
    *, dsn, deployment_dir, corpus_dir: Path, parity: ParityHistory, policies,
    checks, index: int, state: ModeState, deployment: str,
    mode: str = MODE_EXACT, mutation=None,
):
    materialize_step(parity.history, index, corpus_dir)
    return run_update(
        dsn=dsn, deployment_dir=Path(deployment_dir), corpus_dir=corpus_dir,
        mode=mode, update=parity.updates[index],
        gold_step=parity.gold_step(index), policies=policies, checks=checks,
        state=state, deployment=deployment, mutation=mutation,
    )


def run_safety_witnesses(
    *,
    dsn: str,
    deployment_dir: Path,
    corpus_root: Path,
    parity: ParityHistory,
    policies: Sequence[str],
    checks: Sequence[Any],
    reference: ModeRun,
    reset,
    deployment: str,
) -> list[dict[str, Any]]:
    """Run the five safety mutations against THIS history and record what fired.

    One broken assumption at a time, each on a fresh runtime (``reset``) so the
    mutated run is not stacked on another mutation's leavings. Every witness is
    checked for NON-VACUITY here: a mutation that changed nothing observable is a
    raised :class:`ControlledFixtureError`, because a decorative witness would
    make the row's zero unsound-reuses cell mean nothing.

    Each witness records the mutation, the update it was applied at, what FIRED
    (in one line a reader can check against the named artifact), and — where the
    mutation forced a fallback — that parity with the complete reference was
    re-established at that update. Safety costs work, never correctness."""
    from okg.substrate.errors import SubstrateError

    common = dict(
        dsn=dsn, deployment_dir=deployment_dir, parity=parity, policies=policies,
        checks=checks, deployment=deployment,
    )
    witnesses: list[dict[str, Any]] = []

    def _restored(execution) -> bool:
        verdict = grade_update(
            gold_step=parity.gold_step(execution.index),
            reference=reference.executions[execution.index],
            differential=execution,
        )
        return (
            verdict.verdict in (VERDICT_EQUAL, VERDICT_CERTIFIED)
            and not verdict.gold_problems
        )

    # (a) a dependency-fact class moves UNDER a valid certificate.
    index = SAFETY_UPDATES[MUTATION_DEPENDENCY_FACT]
    corpus = Path(corpus_root) / "safety-a"
    state = _build_to(corpus_dir=corpus, upto=index, reset=reset, **common)
    mutation = Mutation(kind=MUTATION_DEPENDENCY_FACT)
    mutated = _run_one(
        corpus_dir=corpus, index=index, state=state, mutation=mutation, **common
    )
    if not mutated.ledger.certificates_invalidated or not mutated.ledger.steps_run:
        raise ControlledFixtureError(
            f"the dependency-fact mutation cost nothing: invalidated="
            f"{mutated.ledger.certificates_invalidated} run={mutated.ledger.steps_run}. "
            "A witness that does not fire cannot support the zero"
        )
    witnesses.append({
        "mutation": MUTATION_DEPENDENCY_FACT,
        "update": index,
        "fired": (
            f"certificate for {mutation.target_step!r} invalidated on "
            f"{list(mutated.ledger.invalidated_classes)}; "
            f"{mutated.ledger.steps_run} step(s) RAN"
        ),
        "detail": dict(mutation.detail),
        "steps_run": mutated.ledger.steps_run,
        "certificates_invalidated": mutated.ledger.certificates_invalidated,
        "parity_restored": _restored(mutated),
    })

    # (b) a step's declared effects envelope violated by its actual output.
    index = SAFETY_UPDATES[MUTATION_ENVELOPE_VIOLATION]
    corpus = Path(corpus_root) / "safety-b"
    state = _build_to(corpus_dir=corpus, upto=index, reset=reset, **common)
    mutation = Mutation(kind=MUTATION_ENVELOPE_VIOLATION)
    message = _expect_refusal(
        lambda: _run_one(
            corpus_dir=corpus, index=index, state=state, mutation=mutation,
            **common
        ),
        (ParityReplayError,),
        label=MUTATION_ENVELOPE_VIOLATION,
        expect=("envelope_violation", "discard"),
    )
    fallback = _run_one(
        corpus_dir=corpus, index=index, state=state, mode=MODE_COMPLETE, **common
    )
    witnesses.append({
        "mutation": MUTATION_ENVELOPE_VIOLATION,
        "update": index,
        "fired": (
            f"producer {mutation.detail.get('poisoned_producer')!r} violated its "
            "envelope; the pass DISCARDED into complete execution rather than "
            "approving a skip"
        ),
        "detail": dict(mutation.detail),
        "refusal": message[:400],
        "fallback_steps_skipped": fallback.ledger.steps_skipped,
        "parity_restored": _restored(fallback),
    })

    # (c) a recorded prior RESULT deleted.
    index = SAFETY_UPDATES[MUTATION_PRIOR_RESULT_DELETED]
    corpus = Path(corpus_root) / "safety-c"
    state = _build_to(corpus_dir=corpus, upto=index, reset=reset, **common)
    mutation = Mutation(kind=MUTATION_PRIOR_RESULT_DELETED)
    message = _expect_refusal(
        lambda: _run_one(
            corpus_dir=corpus, index=index, state=state, mutation=mutation,
            **common
        ),
        (ParityReplayError,),
        label=MUTATION_PRIOR_RESULT_DELETED,
        expect=("Reuse is impossible",),
    )
    if mutation.detail.get("had_prior_result") is not True:
        raise ControlledFixtureError(
            "the prior-result mutation deleted a result that did not exist, so "
            "the refusal proves nothing"
        )
    fallback = _run_one(
        corpus_dir=corpus, index=index, state=state, mode=MODE_COMPLETE, **common
    )
    witnesses.append({
        "mutation": MUTATION_PRIOR_RESULT_DELETED,
        "update": index,
        "fired": (
            f"the stored prior result for {mutation.detail.get('deleted_doc')!r} "
            "was gone; the executor REFUSED to publish a state that quietly drops "
            "the step's output"
        ),
        "detail": dict(mutation.detail),
        "refusal": message[:400],
        "fallback_steps_skipped": fallback.ledger.steps_skipped,
        "parity_restored": _restored(fallback),
    })

    # (d) a corrupted certificate row.
    index = SAFETY_UPDATES[MUTATION_CORRUPT_CERTIFICATE]
    corpus = Path(corpus_root) / "safety-d"
    state = _build_to(corpus_dir=corpus, upto=index, reset=reset, **common)
    mutation = Mutation(kind=MUTATION_CORRUPT_CERTIFICATE)
    message = _expect_refusal(
        lambda: _run_one(
            corpus_dir=corpus, index=index, state=state, mutation=mutation,
            **common
        ),
        (SubstrateError,),
        label=MUTATION_CORRUPT_CERTIFICATE,
        expect=("digest",),
    )
    fallback = _run_one(
        corpus_dir=corpus, index=index, state=state, mode=MODE_COMPLETE, **common
    )
    witnesses.append({
        "mutation": MUTATION_CORRUPT_CERTIFICATE,
        "update": index,
        "fired": (
            f"the stored certificate for {mutation.detail.get('corrupted_step')!r} "
            "was tampered with after write; the store recomputed its digest on "
            "read and REFUSED the row"
        ),
        "detail": dict(mutation.detail),
        "refusal": message[:400],
        "fallback_steps_skipped": fallback.ledger.steps_skipped,
        "parity_restored": _restored(fallback),
    })

    # (e) a vacuous change statement, on a FIRST publish.
    index = SAFETY_UPDATES[MUTATION_VACUOUS_CHANGE]
    corpus = Path(corpus_root) / "safety-e"
    reset()
    materialize_base(parity.history, corpus)
    message = _expect_refusal(
        lambda: _run_one(
            corpus_dir=corpus, index=index, state=ModeState(),
            mutation=Mutation(kind=MUTATION_VACUOUS_CHANGE), **common
        ),
        (ParityReplayError,),
        label=MUTATION_VACUOUS_CHANGE,
        expect=("absorbed",),
    )
    witnesses.append({
        "mutation": MUTATION_VACUOUS_CHANGE,
        "update": index,
        "fired": (
            "an EMPTY change statement made the ladder approve a skip for every "
            "reached step on a FIRST publish; the executor refused to absorb an "
            "update with no prior generation to carry forward"
        ),
        "detail": {"stated_buckets": 0},
        "refusal": message[:400],
        "parity_restored": True,
    })
    if len(witnesses) != len(MUTATIONS):
        raise ControlledFixtureError(
            f"{len(witnesses)} safety witness(es) for {len(MUTATIONS)} declared "
            "mutation(s); the row would cite a trial that did not run"
        )
    return witnesses


def _expect_refusal(thunk, exceptions, *, label: str, expect: Sequence[str]) -> str:
    """Run ``thunk``, require it to REFUSE, and require the refusal to say why.

    A mutation that is quietly tolerated is the failure this trial exists to
    detect, so "no exception" is an error here, not a pass. The ``expect``
    fragments keep the witness attached to the mechanism that fired rather than
    to any incidental failure."""
    try:
        thunk()
    except exceptions as exc:
        message = str(exc)
        missing = [token for token in expect if token not in message]
        if missing:
            raise ControlledFixtureError(
                f"the {label} mutation was refused, but the refusal does not "
                f"mention {missing}: {message[:400]}"
            ) from exc
        return message
    raise ControlledFixtureError(
        f"the {label} mutation was TOLERATED — no refusal was raised. The row's "
        "zero unsound-reuse cell cannot stand on a witness that never fires"
    )


# --------------------------------------------------------------------------- #
# The row                                                                      #
# --------------------------------------------------------------------------- #
def build_controlled_fixture_row(
    *,
    manifest: Mapping[str, Any],
    parity: ParityHistory,
    readings: Mapping[str, ModeReading],
    verdicts: Sequence[UpdateVerdict],
    safety_witnesses: Sequence[Mapping[str, Any]],
    direct_report: Mapping[str, Any],
) -> dict[str, Any]:
    """Compose the paper's controlled-fixture row and CHECK it before returning.

    Every argument is run output or sealed gold. The row is built, then
    :func:`assert_row_invariants` is run over it — so a caller can never hold a
    row that failed its own contract."""
    missing = set(MODES) - set(readings)
    if missing:
        raise ControlledFixtureError(
            f"the row needs all three execution modes and is missing "
            f"{sorted(missing)}; a cost comparison over a subset of the modes is "
            "not the comparison the row states"
        )
    exact = readings[MODE_EXACT]
    prices = sealed_cost_model(manifest)["action_prices"]
    # Is this corpus PRICED at all? ``ActionPrices`` defaults every class to 0.0,
    # so a corpus that declares no ``cost.action_prices`` block bills every mode
    # $0.00 — a faithful reading of its sealed economics, and a comparison that
    # discriminates nothing. The row records which case it is IN THE CELL rather
    # than printing three identical zeros that read like a measured tie: under an
    # unpriced corpus the three-mode comparison is carried by the ACTION COUNTS,
    # which are a real ledger measurement in the units the ledger charges.
    priced = any(float(value) > 0.0 for value in prices.values())

    detection: dict[str, Any] = {}
    restoration: dict[str, Any] = {}
    cost_block: dict[str, Any] = {}
    for mode in MODES:
        reading = readings[mode]
        recall = reading.funnel["detection_recall"]
        pin = reading.final_pin
        detection[mode] = {
            "recall_conditional": cell(
                {
                    "detected": recall["conditional"]["detected"],
                    "denominator": recall["conditional"]["denominator"],
                    "rate": _rate(
                        recall["conditional"]["detected"],
                        recall["conditional"]["denominator"],
                    ),
                },
                source=f"funnel at the {mode} mode's final published generation",
                pin=pin,
                field="detection_recall.conditional",
            ),
            "recall_end_to_end": cell(
                {
                    "detected": recall["end_to_end"]["detected"],
                    "denominator": recall["end_to_end"]["denominator"],
                    "rate": _rate(
                        recall["end_to_end"]["detected"],
                        recall["end_to_end"]["denominator"],
                    ),
                },
                source=f"funnel at the {mode} mode's final published generation",
                pin=pin,
                field="detection_recall.end_to_end",
            ),
            "precision_conditional": cell(
                reading.precision["conditional"],
                source=(
                    f"finding rows on channels {sorted(CONFLICT_CHANNELS)} at the "
                    f"{mode} mode's final published generation, joined to the live "
                    "gold bucket roster"
                ),
                pin=pin,
                field="findings x corruption_bucket_map (surviving sites)",
            ),
            "precision_end_to_end": cell(
                reading.precision["end_to_end"],
                source=(
                    f"finding rows on channels {sorted(CONFLICT_CHANNELS)} at the "
                    f"{mode} mode's final published generation, joined to the live "
                    "gold bucket roster"
                ),
                pin=pin,
                field="findings x corruption_bucket_map (all live sites)",
            ),
            "precision_engine": cell(
                reading.attribution["engine"],
                source=(
                    "the same flagged set as precision_end_to_end, with the "
                    "roster artifacts and extractor defects removed from BOTH "
                    "the numerator and the denominator"
                ),
                pin=pin,
                field="precision_attribution.engine",
                excluded_buckets=reading.attribution[
                    "excluded_from_engine_precision"
                ],
                raw_figure=reading.precision["end_to_end"],
            ),
            "precision_attribution": cell(
                {
                    "table": reading.attribution["table"],
                    "by_class": reading.attribution["by_class"],
                    "gold_conflict_agreement": reading.attribution[
                        "gold_conflict_agreement"
                    ],
                },
                source=(
                    f"every flagged bucket at the {mode} mode's final published "
                    "generation, classified against the SEALED per-step gold "
                    "(expected_state, corpus surfaces, delta list)"
                ),
                pin=pin,
                field="classify_flagged_buckets",
                classes=list(BUCKET_CLASSES),
            ),
            "live_corruptions": cell(
                len(reading.live_corruptions),
                source="the sealed history's final step",
                field="HistoryStep.live_corruptions",
            ),
        }
        states = reading.funnel["states"]
        restoration[mode] = {
            "repaired": cell(
                states["repaired"],
                source=f"funnel at the {mode} mode's final published generation",
                pin=reading.final_pin,
                field="states.repaired",
            ),
            "mis_repaired": cell(
                states["mis_repaired"],
                source=f"funnel at the {mode} mode's final published generation",
                pin=reading.final_pin,
                field="states.mis_repaired",
            ),
            "states": cell(
                dict(states),
                source=f"funnel at the {mode} mode's final published generation",
                pin=reading.final_pin,
                field="states (exhaustive over the live corruption roster)",
            ),
        }
        spent = spent_action_cost(prices, reading.action_counts)
        totals = reading.ledger_totals
        cost_block[mode] = {
            "spent_action_cost": cell(
                spent,
                source=f"the {mode} mode's work ledger, priced through the "
                       "manifest's sealed action_prices",
                field="LedgerRow.run_steps / .published x cost_model.action_prices",
                ledger_rows=reading.updates,
                priced_corpus=priced,
                price_table=dict(prices),
                note=None if priced else UNPRICED_NOTE,
            ),
            "steps_run": cell(
                totals["steps_run"],
                source=f"the {mode} mode's work ledger",
                field="sum(LedgerRow.steps_run)",
            ),
            "steps_reached": cell(
                totals["steps_reached"],
                source=f"the {mode} mode's work ledger",
                field="sum(LedgerRow.steps_reached)",
            ),
            "steps_skipped": cell(
                totals["steps_skipped"],
                source=f"the {mode} mode's work ledger",
                field="sum(LedgerRow.steps_skipped)",
            ),
            "skips_by_cause": cell(
                dict(totals["skips_by_cause"]),
                source=f"the {mode} mode's work ledger",
                field="LedgerRow.skips_by_cause",
            ),
            "publishes": cell(
                totals["publishes"],
                source=f"the {mode} mode's work ledger",
                field="count(LedgerRow.published)",
            ),
            "action_counts": cell(
                dict(reading.action_counts),
                source=f"the {mode} mode's work ledger, reduced to priced classes",
                field="action_counts_from_step_ledger",
            ),
        }

    # ---- parity + the zero that is evidence -------------------------------- #
    tally: dict[str, int] = {}
    for verdict in verdicts:
        tally[verdict.verdict] = tally.get(verdict.verdict, 0) + 1
    unsound = [
        v.update
        for v in verdicts
        if v.verdict not in (VERDICT_EQUAL, VERDICT_CERTIFIED)
        or v.gold_problems
        or not v.same_facts
    ]
    remaining = remaining_error_cost(manifest_for_cost := live_manifest(
        manifest, exact.live_corruptions
    ), exact.funnel)
    qual = qualification(manifest_for_cost, remaining)

    row = {
        "schema": CONTROLLED_FIXTURE_SCHEMA,
        "row": "Controlled fixture",
        "history": {
            "updates": cell(
                len(parity.updates),
                source="the sealed history",
                field="History.steps",
            ),
            "history_digest": cell(
                parity.history.history_digest,
                source="the sealed history", field="History.history_digest",
            ),
            "manifest_digest": cell(
                manifest["manifest_digest"],
                source="the sealed manifest", field="manifest_digest",
            ),
            "config_digest": cell(
                manifest["config_digest"],
                source="the sealed manifest", field="config_digest",
            ),
            "direct_path_seal": cell(
                {
                    "updates": direct_report["updates"],
                    "expected_diff_problems": len(
                        direct_report["expected_diff_problems"]
                    ),
                    "skippable_steps": list(direct_report["skippable_steps"]),
                },
                source="the direct-path replay that recorded the fact sets",
                field="history_replay report",
            ),
        },
        "detection": detection,
        "restoration": restoration,
        "parity": {
            "verdicts": cell(
                dict(sorted(tally.items())),
                source="per-update parity verdicts, exact mode vs the complete "
                       "reference",
                field="UpdateVerdict.verdict",
                updates=len(verdicts),
            ),
            "silent_divergences": cell(
                len(unsound),
                source="per-update parity verdicts",
                field="verdicts that are neither EQUAL nor "
                      "EQUAL-with-certified-differences, or that disagree with "
                      "gold, or that consumed different bytes",
                offending_updates=unsound,
            ),
            "certified_updates": cell(
                [v.update for v in verdicts if v.verdict == VERDICT_CERTIFIED],
                source="per-update parity verdicts",
                field="UpdateVerdict.verdict == EQUAL-with-certified-differences",
            ),
            "qualified": cell(
                qual["qualified"],
                source="the exact mode's funnel at its final published generation",
                pin=exact.final_pin,
                field="cost.qualification over remaining_error_cost",
                reason=qual["reason"],
            ),
            "qualification": qual,
        },
        "safety": {
            "unsound_reuses": cell(
                len(unsound),
                source="the exact mode's per-update parity verdicts over the "
                       "whole history",
                field="a reuse is UNSOUND iff the state it produced differs from "
                      "the complete reference's outside a certificate licence, or "
                      "disagrees with gold",
                updates_graded=len(verdicts),
            ),
            "why_the_zero_is_evidence": cell(
                [dict(w) for w in safety_witnesses],
                source="the safety mutations, each run against this same sealed "
                       "history",
                field="one broken assumption at a time; each witness records what "
                      "FIRED",
                mutations=len(safety_witnesses),
            ),
        },
        "action_cost": cost_block,
        "error_cost": {
            "remaining_unbudgeted": cell(
                remaining,
                source="the exact mode's funnel at its final published generation, "
                       "priced through the manifest's sealed error costs",
                pin=exact.final_pin,
                field="cost.remaining_error_cost (scale 1.0, no budget)",
            ),
            "remaining_at_declared_budgets": deferred_cell(
                gating_surface=BUDGET_GATE,
                reason=(
                    "a remaining-error-cost figure AT A DECLARED BUDGET requires a "
                    "scheduler that can defer work to respect a budget. That "
                    "scheduler does not exist: the exact mode either proves a skip "
                    "sound or runs the step, and has no budget input at all. The "
                    "unbudgeted figure in remaining_unbudgeted is a measurement at "
                    "the run's own operating point and is NOT a proxy for this cell"
                ),
            ),
            "disclosure_misses": deferred_cell(
                gating_surface=BUDGET_GATE,
                reason=(
                    "disclosure grading needs budgeted runs that DEFER work and "
                    "disclose what they deferred. With no approximate scheduler "
                    "there is nothing to disclose and nothing to grade, so any "
                    "number here — including zero — would be a claim about an "
                    "unexercised surface"
                ),
            ),
        },
    }
    assert_row_invariants(row)
    return row


def assert_row_invariants(row: Mapping[str, Any]) -> None:
    """FAIL-CLOSED over the finished row. A missing input is a refusal.

    Each check names what a reader would otherwise be misled about: the parity
    verdicts cover the whole history and none of them is silent; the zero
    unsound-reuse cell carries a witness; all three modes appear in the
    detection, restoration and cost blocks; each mode's priced classes
    reconstruct its total and cover every action class; a corpus that prices
    nothing SAYS so rather than showing a bare zero, and carries the action
    counts that then hold the comparison; a corpus that DOES price its actions
    bills every mode something and no two modes the same, so the dollar column
    can never read as a measured tie; qualification is STATED; every
    detection denominator is non-zero; and every deferred cell names its gating
    surface and carries the marker rather than a number."""
    if row.get("schema") != CONTROLLED_FIXTURE_SCHEMA:
        raise ControlledFixtureError(f"not a controlled-fixture row: {row.get('schema')!r}")

    updates = row["history"]["updates"]["value"]
    graded = row["parity"]["verdicts"]["provenance"]["updates"]
    if graded != updates:
        raise ControlledFixtureError(
            f"the parity verdicts cover {graded} update(s) but the history has "
            f"{updates}; an ungraded update is a silent divergence the row would "
            "not report"
        )
    tally = row["parity"]["verdicts"]["value"]
    unknown = set(tally) - {VERDICT_EQUAL, VERDICT_CERTIFIED}
    if unknown:
        raise ControlledFixtureError(
            f"parity verdicts include {sorted(unknown)}; the row states that every "
            "update is EQUAL or EQUAL-with-certified-differences"
        )
    if sum(tally.values()) != updates:
        raise ControlledFixtureError(
            f"the verdict tally sums to {sum(tally.values())}, not the history's "
            f"{updates} update(s)"
        )
    if row["parity"]["silent_divergences"]["value"] != 0:
        raise ControlledFixtureError(
            "the row reports a silent divergence at update(s) "
            f"{row['parity']['silent_divergences']['provenance']['offending_updates']}"
        )
    if row["safety"]["unsound_reuses"]["value"] != 0:
        raise ControlledFixtureError("the exact mode reports a non-zero unsound reuse count")
    if not row["safety"]["why_the_zero_is_evidence"]["value"]:
        raise ControlledFixtureError(
            "the unsound-reuse zero carries no safety witness; an untested zero "
            "and a proven zero are indistinguishable, so the row refuses"
        )

    for block in ("detection", "restoration", "action_cost"):
        missing = set(MODES) - set(row[block])
        if missing:
            raise ControlledFixtureError(
                f"the {block} block is missing mode(s) {sorted(missing)}"
            )

    for mode in MODES:
        spent = row["action_cost"][mode]["spent_action_cost"]["value"]
        classes = set(spent["by_class"])
        if classes != set(ACTION_CLASSES):
            raise ControlledFixtureError(
                f"mode {mode}: priced action classes {sorted(classes)} != "
                f"{sorted(ACTION_CLASSES)}; an unpriced class costs nothing"
            )
        recomputed = math.fsum(c["cost"] for c in spent["by_class"].values())
        if not math.isclose(recomputed, spent["total"], rel_tol=1e-9, abs_tol=1e-9):
            raise ControlledFixtureError(
                f"mode {mode}: per-class costs sum to {recomputed}, total says "
                f"{spent['total']}"
            )
        provenance = row["action_cost"][mode]["spent_action_cost"]["provenance"]
        if not provenance.get("priced_corpus"):
            if not provenance.get("note"):
                raise ControlledFixtureError(
                    f"mode {mode}: the corpus prices no action, so the dollar total "
                    "is 0.00 by construction — the cell must SAY so or the zero "
                    "reads as a measured saving"
                )
            if spent["total"]:
                raise ControlledFixtureError(
                    f"mode {mode}: the corpus is marked unpriced but the total is "
                    f"{spent['total']}"
                )
        elif not spent["total"]:
            raise ControlledFixtureError(
                f"mode {mode}: the corpus SEALS action prices "
                f"{dict(provenance.get('price_table') or {})} but the mode's dollar "
                "total is 0.00; a priced corpus that bills nothing means the mode "
                "performed no priced action, and the cost cell would read as a "
                "measured saving that no work backs"
            )
        if not row["action_cost"][mode]["action_counts"]["value"]:
            raise ControlledFixtureError(
                f"mode {mode}: no action counts, so nothing carries the three-mode "
                "comparison when the corpus is unpriced"
            )
        # Precision is only quotable once every flagged bucket is attributed: a
        # raw figure standing alone cannot be read as a statement about the
        # engine, because the roster it grades against is not the conflict set.
        attribution = row["detection"][mode].get("precision_attribution")
        if attribution is not None:
            table = attribution["value"]["table"]
            flagged = row["detection"][mode]["precision_end_to_end"]["value"][
                "denominator"
            ]
            if len(table) != flagged:
                raise ControlledFixtureError(
                    f"mode {mode}: {len(table)} bucket(s) attributed against "
                    f"{flagged} flagged; an unattributed flag would leave the "
                    "precision figure unexplained"
                )
            for entry in table:
                if entry["classification"] not in BUCKET_CLASSES:
                    raise ControlledFixtureError(
                        f"mode {mode}: bucket {entry['bucket']!r} carries class "
                        f"{entry['classification']!r}, which is not one of "
                        f"{list(BUCKET_CLASSES)}"
                    )
                if not entry["evidence"]:
                    raise ControlledFixtureError(
                        f"mode {mode}: bucket {entry['bucket']!r} is classified "
                        "with no evidence; an unevidenced class is an assertion"
                    )
            if "precision_engine" not in row["detection"][mode]:
                raise ControlledFixtureError(
                    f"mode {mode}: the row attributes the flagged buckets but "
                    "does not report the corrected engine precision beside the "
                    "raw figure"
                )
        for facet in ("recall_conditional", "recall_end_to_end"):
            if not row["detection"][mode][facet]["value"]["denominator"]:
                raise ControlledFixtureError(
                    f"mode {mode}: {facet} has a ZERO denominator, so its rate is "
                    "vacuous; a row cell must not be readable as a measurement it "
                    "is not"
                )

    # A PRICED corpus must produce three DISTINCT dollar totals. The three modes
    # do measurably different amounts of work — that is the row's whole claim —
    # so once the work is priced, two modes billing the same amount is either a
    # ledger that stopped discriminating or a price table that prices only
    # classes the modes share. Either way the cost cells would read as a measured
    # tie between execution modes, which is exactly the misreading the unpriced
    # branch above exists to prevent, arriving by the other door.
    if all(
        row["action_cost"][mode]["spent_action_cost"]["provenance"].get("priced_corpus")
        for mode in MODES
    ):
        totals = {
            mode: row["action_cost"][mode]["spent_action_cost"]["value"]["total"]
            for mode in MODES
        }
        ties = [
            (a, b)
            for i, a in enumerate(MODES)
            for b in MODES[i + 1:]
            if math.isclose(totals[a], totals[b], rel_tol=1e-9, abs_tol=1e-9)
        ]
        if ties:
            raise ControlledFixtureError(
                f"the corpus is PRICED but modes {ties} bill the same dollar total "
                f"({totals}); identical totals across execution modes read as a "
                "measured tie, and the row states that the modes do different "
                "amounts of work"
            )

    if "qualified" not in row["parity"]:
        raise ControlledFixtureError("the row does not state qualification")

    for name, entry in row["error_cost"].items():
        if not is_deferred(entry):
            continue
        if entry["value"] != DEFERRED or not entry.get("gating_surface"):
            raise ControlledFixtureError(
                f"deferred cell {name!r} does not carry the {DEFERRED} marker and "
                "its gating surface; it could be read as a measured value"
            )


def run_controlled_fixture(
    *,
    dsn: str,
    deployment_dir: Path,
    corpus_root: Path,
    parity: ParityHistory,
    manifest: Mapping[str, Any],
    policies: Sequence[str],
    checks: Sequence[Any],
    direct_report: Mapping[str, Any],
    reset,
    deployment: str = SMOKE_DEPLOYMENT,
    equivalent=synth_survival_equivalent,
) -> tuple[dict[str, Any], dict[str, ModeRun], list[UpdateVerdict]]:
    """THE capstone driver: one sealed history, three execution modes, one row.

    Replays the history in complete, dependency-only and exact (scheduler-governed
    differential) execution against ONE database — with ``reset`` returning the
    runtime to an empty graph between them, so the comparison is of execution
    modes and not of starting states — grades every update, runs the safety
    mutations, and composes the row.

    ``reset`` is injected rather than imported so this module stays free of the
    test harness; it must leave the runtime in the state a first publish expects
    (the ontology catalog surviving, the certificates gone).

    Returns ``(row, runs, verdicts)`` so a caller can assert over the underlying
    run output as well as over the row."""
    runs: dict[str, ModeRun] = {}
    for mode in (MODE_COMPLETE, MODE_DEPENDENCY, MODE_EXACT):
        reset()
        runs[mode] = replay_mode(
            dsn=dsn, deployment_dir=Path(deployment_dir),
            corpus_dir=Path(corpus_root) / f"corpus-{mode}", parity=parity,
            mode=mode, policies=policies, checks=checks, deployment=deployment,
        )
    verdicts = [
        grade_update(
            gold_step=parity.gold_step(index),
            reference=runs[MODE_COMPLETE].executions[index],
            differential=runs[MODE_EXACT].executions[index],
        )
        for index in range(len(parity.updates))
    ]
    delivering = delivering_documents(parity)
    readings = {
        mode: read_mode(
            run=run, manifest=manifest, parity=parity,
            delivering_docs=delivering, equivalent=equivalent,
        )
        for mode, run in runs.items()
    }
    # The safety trial RESETS the runtime, so it runs after every mode has been
    # read: the readings above are plain data and survive it.
    witnesses = run_safety_witnesses(
        dsn=dsn, deployment_dir=Path(deployment_dir), corpus_root=Path(corpus_root),
        parity=parity, policies=policies, checks=checks,
        reference=runs[MODE_COMPLETE], reset=reset, deployment=deployment,
    )
    row = build_controlled_fixture_row(
        manifest=manifest, parity=parity, readings=readings, verdicts=verdicts,
        safety_witnesses=witnesses, direct_report=direct_report,
    )
    return row, runs, verdicts


def write_controlled_fixture(row: Mapping[str, Any], out_path: str | Path) -> Path:
    """Write the row artifact, sorted-key + deterministic (the spread precedent)."""
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(row, sort_keys=True, indent=1) + "\n", encoding="utf-8"
    )
    return out_path


# --------------------------------------------------------------------------- #
# The rendered row                                                             #
# --------------------------------------------------------------------------- #
def _pct(value: Any) -> str:
    if value is None:
        return "  n/a"
    return f"{100.0 * float(value):5.1f}%"


def _money(value: float) -> str:
    return f"${value:,.2f}"


def render_controlled_fixture(row: Mapping[str, Any]) -> str:
    """The row as a human-readable table, in the ``render_spread`` house style.

    Pure over the artifact — no timestamp, no live read — so the same row
    renders the same text, and the rendered table and the JSON can be diffed
    against each other."""
    hist = row["history"]
    lines = [
        f"Controlled fixture  ({hist['updates']['value']} updates, history "
        f"{hist['history_digest']['value'][:16]}, manifest "
        f"{hist['manifest_digest']['value'][:16]})",
        "  every cell below is READ from run output; see .provenance in the JSON",
        "",
        "  DETECTION (at each mode's final published generation)",
        f"    {'mode':<16} {'recall cond':>12} {'recall e2e':>12} "
        f"{'prec cond':>12} {'prec e2e':>12}",
    ]
    for mode in MODES:
        det = row["detection"][mode]
        lines.append(
            f"    {mode:<16} "
            f"{_pct(det['recall_conditional']['value']['rate']):>12} "
            f"{_pct(det['recall_end_to_end']['value']['rate']):>12} "
            f"{_pct(det['precision_conditional']['value']['rate']):>12} "
            f"{_pct(det['precision_end_to_end']['value']['rate']):>12}"
        )
        rc = det["recall_conditional"]["value"]
        re2 = det["recall_end_to_end"]["value"]
        pe2 = det["precision_end_to_end"]["value"]
        lines.append(
            f"    {'':<16}   detected {rc['detected']}/{rc['denominator']} "
            f"(conditional), {re2['detected']}/{re2['denominator']} (end-to-end); "
            f"flagged {pe2['denominator']} bucket(s), "
            f"{pe2['false_positive']} false positive(s)"
        )
        engine = det.get("precision_engine")
        if engine is not None:
            eng = engine["value"]["end_to_end"]
            lines.append(
                f"    {'':<16}   ENGINE precision {_pct(eng['rate']).strip()} "
                f"({eng['true_positive']}/{eng['denominator']}, "
                f"{eng['false_positive']} engine FP) vs RAW "
                f"{_pct(pe2['rate']).strip()} ({pe2['true_positive']}/"
                f"{pe2['denominator']}) — neither replaces the other"
            )

    lines += ["", "  FLAGGED-BUCKET ATTRIBUTION (exact mode, every flagged bucket)"]
    attribution = row["detection"][MODE_EXACT].get("precision_attribution")
    if attribution is None:
        lines.append("    not attributed")
    else:
        for entry in attribution["value"]["table"]:
            lines.append(f"    {entry['classification']:<26} {entry['bucket']}")
            lines.append(
                f"      channels {','.join(entry['channels'])}; "
                f"{len(entry['claims'])} claim(s): "
                + "; ".join(
                    f"{cid.split(':')[1]}={value}"
                    for cid, value in sorted(entry["claims"].items())
                )
            )
            lines.append(f"      {entry['evidence']}")
        agreement = attribution["value"]["gold_conflict_agreement"]
        lines.append(
            f"    flagged {agreement['flagged_buckets']} bucket(s); the sealed "
            f"per-step gold declares {agreement['gold_conflicted_buckets']} "
            f"conflicted; {agreement['agreed']} in common"
        )

    lines += ["", "  GOLD-CORRECTION RESTORATION"]
    for mode in MODES:
        rest = row["restoration"][mode]
        lines.append(
            f"    {mode:<16} repaired {rest['repaired']['value']:>4}   "
            f"mis_repaired {rest['mis_repaired']['value']:>4}"
        )

    parity = row["parity"]
    lines += [
        "",
        "  EXACT-MODE PARITY + QUALIFICATION",
        f"    verdicts        {parity['verdicts']['value']} over "
        f"{parity['verdicts']['provenance']['updates']} update(s)",
        f"    certified at    {parity['certified_updates']['value']}",
        f"    silent diverge  {parity['silent_divergences']['value']}",
        f"    qualified       {parity['qualified']['value']} — "
        f"{parity['qualified']['provenance']['reason']}",
        f"    unsound reuses  {row['safety']['unsound_reuses']['value']}  "
        f"(evidence, not absence: "
        f"{row['safety']['why_the_zero_is_evidence']['provenance']['mutations']} "
        f"safety mutation(s) below)",
    ]
    for witness in row["safety"]["why_the_zero_is_evidence"]["value"]:
        lines.append(
            f"      {witness['mutation']:<28} update {witness['update']:<3} "
            f"{witness['fired']}"
        )

    priced = row["action_cost"][MODE_COMPLETE]["spent_action_cost"]["provenance"].get(
        "priced_corpus"
    )
    lines += [
        "",
        "  ACTION COST (work ledger x sealed action prices)",
        f"    {'mode':<16} {'steps run':>10} {'skipped':>9} {'publishes':>10} "
        f"{'extract':>9} {'check':>9} {'spent':>12}",
    ]
    for mode in MODES:
        block = row["action_cost"][mode]
        spent = block["spent_action_cost"]["value"]
        counts = block["action_counts"]["value"]
        lines.append(
            f"    {mode:<16} {block['steps_run']['value']:>10} "
            f"{block['steps_skipped']['value']:>9} "
            f"{block['publishes']['value']:>10} "
            f"{counts['extract_document']:>9} {counts['check']:>9} "
            f"{_money(spent['total']):>12}"
        )
    if priced:
        baseline = row["action_cost"][MODE_COMPLETE][
            "spent_action_cost"]["value"]["total"]
        for mode in (MODE_DEPENDENCY, MODE_EXACT):
            total = row["action_cost"][mode]["spent_action_cost"]["value"]["total"]
            if baseline:
                lines.append(
                    f"    {mode} spends {100.0 * total / baseline:.1f}% of complete "
                    f"execution ({_money(baseline - total)} saved)"
                )
    else:
        lines.append(
            "    UNPRICED CORPUS: the $0.00 column is not a measured tie — this "
            "corpus seals no"
        )
        lines.append(
            "    action prices, so the comparison above is the ACTION COUNTS."
        )
        base_counts = row["action_cost"][MODE_COMPLETE]["action_counts"]["value"]
        for mode in (MODE_DEPENDENCY, MODE_EXACT):
            counts = row["action_cost"][mode]["action_counts"]["value"]
            billed = sum(counts.values())
            base = sum(base_counts.values()) or 1
            lines.append(
                f"    {mode} bills {billed} priced action(s), "
                f"{100.0 * billed / base:.1f}% of complete execution's {base}"
            )
    for name, count in sorted(
        row["action_cost"][MODE_EXACT]["skips_by_cause"]["value"].items()
    ):
        lines.append(f"      exact-mode skip cause: {name:<44} {count:>5}")

    err = row["error_cost"]
    remaining = err["remaining_unbudgeted"]["value"]
    lines += [
        "",
        "  ERROR COST",
        f"    remaining (unbudgeted)  {_money(remaining['total'])} over "
        f"{remaining['corruptions']} unresolved corruption(s); "
        f"mandatory unresolved {remaining['mandatory_unresolved']}",
    ]
    for stage, cellv in remaining["by_stage"].items():
        lines.append(
            f"      {stage:<12} {_money(cellv['cost']):>12}  "
            f"{cellv['corruptions']} corruption(s)"
        )
    for name in ("remaining_at_declared_budgets", "disclosure_misses"):
        entry = err[name]
        lines.append(
            f"    {name:<24} {DEFERRED} — gated on {entry['gating_surface']}"
        )
    return "\n".join(lines)


__all__ = [
    "BUCKET_CLASSES",
    "BUDGET_GATE",
    "CLASS_ENGINE_FP",
    "CLASS_EXTRACTOR_FP",
    "CLASS_ROSTER_ARTIFACT",
    "CLASS_TRUE_POSITIVE",
    "CONFLICT_CHANNELS",
    "CONFLICTING_DELTA_KINDS",
    "CONTROLLED_FIXTURE_FILENAME",
    "CONTROLLED_FIXTURE_SCHEMA",
    "DEFERRED",
    "ControlledFixtureError",
    "MODE_EXACT",
    "ModeReading",
    "assert_row_invariants",
    "bucket_conflict_causes",
    "build_controlled_fixture_row",
    "cell",
    "classify_flagged_buckets",
    "deferred_cell",
    "delivering_documents",
    "detection_precision",
    "is_deferred",
    "live_manifest",
    "read_mode",
    "render_controlled_fixture",
    "run_controlled_fixture",
    "run_safety_witnesses",
    "sealed_surface_keys",
    "write_controlled_fixture",
]
