"""Cost-weighted scoring: remaining error cost, spent action cost, qualification.

The benchmark grades a run in COUNTS — how many corruptions were detected, how
many repaired. The paper's objective is MONEY: each violation carries a dollar
error cost, an INFINITE cost marks the violation MANDATORY, each action carries a
dollar price, and a result QUALIFIES only when no mandatory violation is left
unresolved. This module is the counts-to-dollars layer.

Three products, all pure over the SEALED gold plus the funnel the engine
produced — no live read, no model call, so a re-score under different weights is
a function call over recorded artifacts:

* :func:`remaining_error_cost` — the loss-weighted sum over corruptions that are
  still unresolved, SPLIT BY FUNNEL STAGE. A corruption lost at extraction costs
  the extraction stage; one the engine never noticed costs detection; one it
  noticed and mis-settled costs repair. The stage sums are checked against the
  total FAIL-CLOSED, in the same spirit as the funnel's own exhaustiveness guard.
* :func:`spent_action_cost` — the price-weighted ledger of the actions the run
  actually executed, per action class.
* :func:`qualification` — the paper's qualification predicate, reported as a LOUD
  top-level field: a result leaving any mandatory violation unresolved is
  ``qualified=false``, never a footnote a reader can miss.

RESOLVED means the violation no longer holds. Two terminal states qualify: the
engine settled the bucket on the clean value (``repaired``), and an operator
retracted or overrode the claim (``removed_from_scope``). The second is not a
loophole — the operator PAID for it, at the ``correction`` action price, so the
cost simply moves from the error side of the ledger to the action side. Every
other terminal state leaves the violation standing: ``mis_repaired`` in
particular is worse than silence (the engine made the wrong value canonical) and
is charged in full to the repair stage.
"""

from __future__ import annotations

import math
from typing import Any, Mapping, Sequence

from okg.bench.synth.config import (
    ACTION_CHECK,
    ACTION_CLASSES,
    ACTION_CORRECTION,
    ACTION_EXTRACT_DOCUMENT,
    ACTION_PARSE,
    ACTION_PUBLISH,
    MANDATORY_COST,
)
from okg.bench.synth.resolution import (
    FUNNEL_STATES,
    STATE_COUNTERPART_LOST,
    STATE_DETECTED_ONLY,
    STATE_LAUNDERED_AT_EXTRACTION,
    STATE_MIS_REPAIRED,
    STATE_MISSED_AT_EXTRACTION,
    STATE_NOT_DETECTED,
    STATE_REMOVED_FROM_SCOPE,
    STATE_REPAIRED,
)

COST_SCHEMA = "okg.synth.cost.v1"

# Floating-point associativity allowance for the stage-sum identity ONLY. The
# per-stage sums and the total are computed by separate fsum passes over the same
# dollar amounts, so they can differ in the last ulp without any corruption having
# gone missing. The TEETH of the fail-closed check are the INTEGER corruption and
# mandatory counts, which are compared exactly; this tolerance never hides a
# missing corruption, because a missing corruption also moves an integer.
_SUM_TOL = 1e-9


class CostError(ValueError):
    """A fail-closed cost failure: an unpriced state, or stage sums that do not
    reconstruct the total."""


# -- funnel stage attribution ----------------------------------------------

STAGE_EXTRACTION = "extraction"
STAGE_DETECTION = "detection"
STAGE_REPAIR = "repair"
FUNNEL_STAGE_ORDER = (STAGE_EXTRACTION, STAGE_DETECTION, STAGE_REPAIR)

# A violation no longer holds in these two terminal states, so it carries no
# remaining error cost (see the module docstring).
RESOLVED_STATES = frozenset({STATE_REPAIRED, STATE_REMOVED_FROM_SCOPE})

# Every UNRESOLVED terminal state, charged to the stage that failed it.
# counterpart_lost is an EXTRACTION charge even though the corrupted value itself
# survived: what extraction dropped was the material the conflict needed, so the
# engine never had the question. (The funnel keeps counterpart_lost out of its
# own _EXTRACTION_LOSS_STATES because that set answers a different question —
# did the INJECTION survive — and conflating the two would move injection
# survival. The two readings are deliberately distinct.)
STATE_STAGE: Mapping[str, str] = {
    STATE_MISSED_AT_EXTRACTION: STAGE_EXTRACTION,
    STATE_LAUNDERED_AT_EXTRACTION: STAGE_EXTRACTION,
    STATE_COUNTERPART_LOST: STAGE_EXTRACTION,
    STATE_NOT_DETECTED: STAGE_DETECTION,
    STATE_DETECTED_ONLY: STAGE_REPAIR,
    STATE_MIS_REPAIRED: STAGE_REPAIR,
}

# TOTALITY, checked at import: every funnel state is either resolved or priced to
# a stage. A new terminal state added to resolution.FUNNEL_STATES without a
# decision here is an ImportError at load, never a corruption that silently
# vanishes from the cost ledger.
_uncovered = set(FUNNEL_STATES) - set(STATE_STAGE) - RESOLVED_STATES
if _uncovered:  # pragma: no cover - guarded by construction
    raise CostError(
        f"funnel state(s) {sorted(_uncovered)} are neither RESOLVED_STATES nor "
        f"mapped to a funnel stage in STATE_STAGE; every terminal state must be "
        f"one or the other or its remaining error cost would go uncharged"
    )
_unknown = (set(STATE_STAGE) | RESOLVED_STATES) - set(FUNNEL_STATES)
if _unknown:  # pragma: no cover - guarded by construction
    raise CostError(
        f"STATE_STAGE / RESOLVED_STATES name state(s) {sorted(_unknown)} that "
        f"resolution.FUNNEL_STATES does not define"
    )


# -- the sealed tables ------------------------------------------------------


def sealed_cost_model(manifest: Mapping[str, Any]) -> dict[str, Any]:
    """The cost tables SEALED into the manifest, fail-closed on absence.

    Read from the manifest rather than from a live ``SynthConfig`` on purpose: a
    re-score hands its own config object around, and reading the economics from
    there would let a re-score silently re-price a recorded run. The sealed
    tables are covered by ``manifest_digest``.
    """
    model = manifest.get("cost_model")
    if not isinstance(model, Mapping):
        raise CostError(
            "manifest carries no sealed cost_model: this artifact predates "
            "synth-cost-weighted-gold, and scoring it would have to invent the "
            "error costs and action prices. Re-seal the corpus."
        )
    return dict(model)


def error_cost_by_corruption(
    manifest: Mapping[str, Any], *, scale: float = 1.0
) -> dict[str, float | None]:
    """corruption_id -> its dollar error cost, or ``None`` when INFINITE.

    Deduped by corruption_id: a majority-visibility corruption emits one gold
    SITE ROW per corrupted rendering, and the cost is a property of the
    CORRUPTION, not of how many renderings carried it.

    ``scale`` multiplies the FINITE costs — the sensitivity protocol's 0.1x /
    10x re-price. An infinite cost is invariant under scaling (0.1 * inf = inf),
    which is why qualification is invariant across the sensitivity points.
    """
    if scale < 0.0:
        raise CostError(f"error-cost scale {scale} is negative")
    costs: dict[str, float | None] = {}
    for site in manifest["gold"]["corruptions"]:
        cid = site["corruption_id"]
        if cid in costs:
            continue
        declared = site.get("error_cost")
        if declared is None and not site.get("mandatory", False):
            raise CostError(
                f"gold corruption {cid!r} declares no error_cost: this artifact "
                "predates synth-cost-weighted-gold. Re-seal the corpus."
            )
        if site.get("mandatory", False) or declared == MANDATORY_COST:
            costs[cid] = None
        else:
            costs[cid] = float(declared) * float(scale)
    return costs


# -- remaining error cost ---------------------------------------------------


def _empty_stage_cell() -> dict[str, Any]:
    return {"cost": 0.0, "corruptions": 0, "mandatory_unresolved": 0}


def remaining_error_cost(
    manifest: Mapping[str, Any],
    stages: Mapping[str, Any],
    *,
    scale: float = 1.0,
) -> dict[str, Any]:
    """The loss-weighted sum over UNRESOLVED corruptions, split by funnel stage.

    ``stages`` is :func:`okg.bench.synth.resolution.funnel`'s output — the one
    terminal state per corruption. The finite dollars and the MANDATORY count are
    reported separately: an unresolved mandatory violation's cost is infinite, and
    a JSON number cannot say so, so ``total`` is the FINITE part and
    ``mandatory_unresolved`` / ``infinite`` carry the rest. A reader who sums only
    ``total`` on an unqualified result is warned by ``infinite`` being true and by
    the top-level ``qualified`` field.
    """
    costs = error_cost_by_corruption(manifest, scale=scale)
    per_corruption = stages["per_corruption"]

    by_stage: dict[str, dict[str, Any]] = {
        stage: _empty_stage_cell() for stage in FUNNEL_STAGE_ORDER
    }
    stage_dollars: dict[str, list[float]] = {s: [] for s in FUNNEL_STAGE_ORDER}
    all_dollars: list[float] = []
    unresolved = 0
    mandatory_unresolved = 0
    unresolved_mandatory_ids: list[str] = []

    for cid in sorted(per_corruption):
        state = per_corruption[cid]
        if state in RESOLVED_STATES:
            continue
        stage = STATE_STAGE.get(state)
        if stage is None:
            raise CostError(
                f"corruption {cid!r} terminated in state {state!r}, which is "
                f"neither resolved nor charged to a funnel stage; remaining error "
                f"cost would silently omit it"
            )
        if cid not in costs:
            raise CostError(
                f"corruption {cid!r} has a funnel state but no sealed error cost"
            )
        unresolved += 1
        cell = by_stage[stage]
        cell["corruptions"] += 1
        dollars = costs[cid]
        if dollars is None:
            mandatory_unresolved += 1
            unresolved_mandatory_ids.append(cid)
            cell["mandatory_unresolved"] += 1
            continue
        stage_dollars[stage].append(dollars)
        all_dollars.append(dollars)

    for stage in FUNNEL_STAGE_ORDER:
        by_stage[stage]["cost"] = math.fsum(stage_dollars[stage])
    total = math.fsum(all_dollars)

    block = {
        "scale": float(scale),
        "total": total,
        "corruptions": unresolved,
        "mandatory_unresolved": mandatory_unresolved,
        "infinite": mandatory_unresolved > 0,
        "unresolved_mandatory_corruptions": sorted(unresolved_mandatory_ids),
        "by_stage": by_stage,
    }
    assert_stage_sums(block)
    return block


def assert_stage_sums(block: Mapping[str, Any]) -> None:
    """FAIL-CLOSED: the per-stage cells must reconstruct the total, exactly.

    Three identities, all required. The two INTEGER ones (unresolved corruption
    count, mandatory count) are compared exactly and are the real teeth: a
    corruption that fell out of a stage moves an integer whatever its price. The
    dollar identity carries a last-ulp tolerance because the per-stage sums and
    the total are separate fsum passes (see :data:`_SUM_TOL`).

    Exposed as a standalone assertion, not only as an internal step, so a witness
    can drive it against a deliberately holed block and see it FIRE.
    """
    by_stage = block["by_stage"]
    missing = set(FUNNEL_STAGE_ORDER) - set(by_stage)
    extra = set(by_stage) - set(FUNNEL_STAGE_ORDER)
    if missing or extra:
        raise CostError(
            f"remaining error cost stage set is wrong: missing {sorted(missing)}, "
            f"unexpected {sorted(extra)}; the stages must be exactly "
            f"{list(FUNNEL_STAGE_ORDER)}"
        )
    stage_corruptions = sum(by_stage[s]["corruptions"] for s in FUNNEL_STAGE_ORDER)
    if stage_corruptions != block["corruptions"]:
        raise CostError(
            f"remaining error cost is not total by stage: the stages account for "
            f"{stage_corruptions} unresolved corruption(s) but the run has "
            f"{block['corruptions']}. Per-stage counts must partition the "
            f"unresolved set exactly (by_stage={dict(by_stage)})"
        )
    stage_mandatory = sum(
        by_stage[s]["mandatory_unresolved"] for s in FUNNEL_STAGE_ORDER
    )
    if stage_mandatory != block["mandatory_unresolved"]:
        raise CostError(
            f"MANDATORY unresolved violations are not total by stage: the stages "
            f"account for {stage_mandatory} but the run has "
            f"{block['mandatory_unresolved']} (by_stage={dict(by_stage)})"
        )
    stage_cost = math.fsum(by_stage[s]["cost"] for s in FUNNEL_STAGE_ORDER)
    if not math.isclose(stage_cost, block["total"], rel_tol=_SUM_TOL, abs_tol=_SUM_TOL):
        raise CostError(
            f"remaining error cost stage sums do not reconstruct the total: "
            f"stages sum to {stage_cost}, total is {block['total']} "
            f"(by_stage={dict(by_stage)})"
        )


# -- spent action cost ------------------------------------------------------


def spent_action_cost(
    prices: Mapping[str, float], counts: Mapping[str, int]
) -> dict[str, Any]:
    """The price-weighted ledger of the actions a run EXECUTED.

    ``counts`` maps an action class to how many of it the run performed;
    ``prices`` is the sealed per-action price table. Both must cover exactly
    :data:`okg.bench.synth.config.ACTION_CLASSES` — an action class the caller
    forgot to count would silently cost nothing, so it is rejected.

    The shape is deliberately generic over ``counts``: the same function prices
    any step ledger whose work has been reduced to per-class counts, which is how
    an incremental-execution ledger (``parity.LedgerRow``) becomes dollars.
    """
    expected = set(ACTION_CLASSES)
    if set(prices) != expected:
        raise CostError(
            f"action price table covers {sorted(prices)}, expected exactly "
            f"{sorted(expected)}"
        )
    if set(counts) != expected:
        raise CostError(
            f"action counts cover {sorted(counts)}, expected exactly "
            f"{sorted(expected)}"
        )
    by_class: dict[str, dict[str, Any]] = {}
    dollars: list[float] = []
    for name in sorted(expected):
        count = int(counts[name])
        if count < 0:
            raise CostError(f"action count {name!r}={count} is negative")
        unit = float(prices[name])
        cost = unit * count
        by_class[name] = {"count": count, "unit_price": unit, "cost": cost}
        dollars.append(cost)
    total = math.fsum(dollars)
    check = math.fsum(cell["cost"] for cell in by_class.values())
    if not math.isclose(check, total, rel_tol=_SUM_TOL, abs_tol=_SUM_TOL):
        raise CostError(  # pragma: no cover - guarded by construction
            f"spent action cost per-class sums ({check}) do not reconstruct the "
            f"total ({total})"
        )
    return {"total": total, "by_class": by_class}


def action_counts_from_step_ledger(
    *,
    documents_parsed: int,
    source_steps_run: int,
    checks_evaluated: int,
    publishes: int,
    corrections: int = 0,
) -> dict[str, int]:
    """Reduce a STEP ledger to the priced action classes.

    The bridge from an incremental-execution ledger (``parity.LedgerRow`` plus the
    execution's ``run_steps``) to dollars, kept here — free of any parity import —
    so the umbrella's controlled-fixture row prices a work ledger with the SAME
    table the scorecard uses, rather than a second private one.

    The mapping is the obvious one and is the whole point of the composition: a
    per-document source step that RAN is an ``extract_document`` action, a bucket a
    check evaluated is a ``check`` action, a generation that published is a
    ``publish`` action. A mode that skips source steps therefore bills strictly
    less, which is exactly the saving an incremental mechanism claims.
    """
    counts = {
        ACTION_PARSE: documents_parsed,
        ACTION_EXTRACT_DOCUMENT: source_steps_run,
        ACTION_CHECK: checks_evaluated,
        ACTION_CORRECTION: corrections,
        ACTION_PUBLISH: publishes,
    }
    for name, value in counts.items():
        if int(value) < 0:
            raise CostError(f"step-ledger action count {name!r}={value} is negative")
    return {name: int(value) for name, value in counts.items()}


# -- qualification ----------------------------------------------------------


def qualification(
    manifest: Mapping[str, Any], remaining: Mapping[str, Any]
) -> dict[str, Any]:
    """The paper's QUALIFICATION predicate over a scored run.

    A result qualifies iff NO mandatory violation is left unresolved. The block
    names the unresolved ones so the verdict is auditable rather than asserted,
    and reports how many mandatory violations were planted at all — a corpus that
    planted none qualifies trivially, and saying so keeps a trivially-qualified
    run from reading like a strong one.
    """
    planted = {
        site["corruption_id"]
        for site in manifest["gold"]["corruptions"]
        if site.get("mandatory", False)
    }
    unresolved = list(remaining["unresolved_mandatory_corruptions"])
    qualified = not unresolved
    if qualified:
        reason = (
            f"no mandatory violation is unresolved ({len(planted)} planted)"
            if planted
            else "the corpus plants no mandatory violation"
        )
    else:
        reason = (
            f"{len(unresolved)} of {len(planted)} mandatory violation(s) left "
            f"UNRESOLVED: {', '.join(unresolved)}"
        )
    return {
        "qualified": qualified,
        "mandatory_planted": len(planted),
        "mandatory_unresolved": len(unresolved),
        "unresolved_mandatory_corruptions": sorted(unresolved),
        "reason": reason,
    }


# -- the operator rendering -------------------------------------------------


def render_cost_blocks(core: Mapping[str, Any]) -> list[str]:
    """The scorecard's cost lines, qualification FIRST and unmissable."""
    qual = core["qualification"]
    remaining = core["remaining_error_cost"]
    spent = core["spent_action_cost"]
    verdict = "QUALIFIED" if qual["qualified"] else "*** NOT QUALIFIED ***"
    lines = [
        f"  {verdict}  ({qual['reason']})",
        f"  remaining error cost  ${remaining['total']:.2f}"
        + ("  + INFINITE (mandatory unresolved)" if remaining["infinite"] else "")
        + f"  over {remaining['corruptions']} unresolved corruption(s)"
        + (f"  [scale {remaining['scale']}x]" if remaining["scale"] != 1.0 else ""),
    ]
    for stage in FUNNEL_STAGE_ORDER:
        cell = remaining["by_stage"][stage]
        lines.append(
            f"    {stage:12s} ${cell['cost']:.2f}"
            f"  ({cell['corruptions']} corruption(s)"
            + (
                f", {cell['mandatory_unresolved']} MANDATORY"
                if cell["mandatory_unresolved"]
                else ""
            )
            + ")"
        )
    lines.append(f"  spent action cost     ${spent['total']:.2f}")
    for name in sorted(spent["by_class"]):
        cell = spent["by_class"][name]
        lines.append(
            f"    {name:18s} {cell['count']:>5d} x ${cell['unit_price']:.4f}"
            f" = ${cell['cost']:.2f}"
        )
    return lines


def render_sensitivity(report: Mapping[str, Any]) -> str:
    """The three-price-point sensitivity table, for an operator."""
    lines = [
        f"synth cost sensitivity  (config {report['config_digest'][:16]})",
        f"  qualification: {'QUALIFIED' if report['qualified'] else 'NOT QUALIFIED'}"
        f"  (invariant across price points: scaling a finite cost never creates "
        f"or removes a mandatory violation)",
        f"  {'scale':>8s} {'remaining $':>14s} {'extraction $':>14s} "
        f"{'detection $':>14s} {'repair $':>14s} {'spent $':>12s}",
    ]
    for point in report["points"]:
        remaining = point["remaining_error_cost"]
        stages = remaining["by_stage"]
        lines.append(
            f"  {point['scale']:>7g}x {remaining['total']:>14.2f} "
            f"{stages[STAGE_EXTRACTION]['cost']:>14.2f} "
            f"{stages[STAGE_DETECTION]['cost']:>14.2f} "
            f"{stages[STAGE_REPAIR]['cost']:>14.2f} "
            f"{point['spent_action_cost']['total']:>12.2f}"
        )
    return "\n".join(lines) + "\n"


def assert_sensitivity_invariants(points: Sequence[Mapping[str, Any]]) -> None:
    """FAIL-CLOSED invariants over a sensitivity sweep.

    Two of them. (1) Qualification is INVARIANT: multiplying finite costs cannot
    create or destroy an infinite one, so a sweep that reports different verdicts
    at different price points has re-scored something other than the same run.
    (2) The finite remaining cost scales LINEARLY with the scale factor, which is
    what makes the sweep a re-PRICE rather than a re-run: any divergence means a
    price point saw a different funnel.
    """
    if not points:
        raise CostError("a sensitivity sweep needs at least one price point")
    verdicts = {bool(p["qualified"]) for p in points}
    if len(verdicts) != 1:
        raise CostError(
            f"qualification is not invariant across the sensitivity price points "
            f"{[p['scale'] for p in points]}: verdicts {sorted(verdicts)}. "
            "Scaling FINITE error costs cannot create or remove a MANDATORY "
            "violation, so the points did not re-score one run."
        )
    base = next((p for p in points if p["scale"] == 1.0), None)
    if base is None:
        return
    for point in points:
        expected = base["remaining_error_cost"]["total"] * point["scale"]
        got = point["remaining_error_cost"]["total"]
        if not math.isclose(got, expected, rel_tol=1e-9, abs_tol=1e-9):
            raise CostError(
                f"the {point['scale']}x price point reports remaining error cost "
                f"{got}, not {expected} = {point['scale']} x the 1x total. A "
                "re-price must scale linearly; a divergence means the point "
                "re-scored a different funnel."
            )
