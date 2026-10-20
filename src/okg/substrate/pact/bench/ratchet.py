"""High-water ratchet against the committed baseline.

The baseline is NOT a floor to sit on: it is updated only by the commit that
IMPROVES a metric, and any regression against it fails the lane. Directions
are declared per metric here so "lower is better" numbers (round-trip calls,
waivers, over-budget verbs) cannot be gamed by inverting them.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

HIGHER_IS_BETTER = (
    "metrics.envelope_honesty.honest_fraction",
    "metrics.envelope_honesty.immutable_honest_fraction",
    "metrics.envelope_honesty.derived_honest_fraction",
    "metrics.gate_catch_rate.catch_rate",
    "metrics.gate_catch_rate.catch_rate_subject_present",
    "metrics.gate_catch_rate.catch_rate_subject_absent",
    "metrics.gate_specificity.acceptance_rate",
    # The three populations, ratcheted INDEPENDENTLY. A single blended rate
    # would let a gain in one population pay for a regression in another,
    # which is exactly the "no population absorbs another's shortfall"
    # failure the split exists to prevent.
    "metrics.refusal_self_sufficiency.command_repairable_rate",
    "metrics.refusal_self_sufficiency.author_action_rate",
    "metrics.refusal_self_sufficiency.operator_action_rate",
    # ...and the POPULATION SIZE, so a rate cannot be raised by emitting fewer
    # refusals. Every one of these rates is a fraction whose denominator this
    # repo controls: stop refusing somewhere and all three go up for free.
    # Ratcheting the refusal count upward makes that trade visible — a change
    # that genuinely removes a refusal has to say so by moving the baseline.
    "metrics.refusal_self_sufficiency.refusals",
    "metrics.round_trips_to_green.episodes_green",
    # Instrument coverage, ratcheted UPWARD: an episode may not be quietly
    # removed from the scored population by declaring a harness gap.
    "metrics.round_trips_to_green.episodes_scored",
    "metrics.manifest_quality.mean_score",
)

LOWER_IS_BETTER = (
    "metrics.refusal_self_sufficiency.unclassified",
    # Refusals that did not repair, in absolute terms. The counterpart to the
    # ratcheted population size: rates alone move when the denominator moves,
    # this one does not.
    "metrics.refusal_self_sufficiency.unrepaired",
    # A blind spot in the instrument. If the CLI starts recommending a verb
    # the harness has not wired, this rises and reds the lane — which is the
    # signal to wire the verb, not to rescore the refusal as a PACT defect.
    "metrics.refusal_self_sufficiency.harness_gap",
    "metrics.round_trips_to_green.total_scored_calls",
    "metrics.round_trips_to_green.max_scored_calls",
    "metrics.envelope_payload_size.over_budget_verbs",
)

# Reported, not ratcheted. The corpus waiver count is live repo state, not a
# harness measurement: it moves with every change anyone authors, so ratcheting
# it here would red this lane for unrelated work. `within_cap` keeps the
# design's cap of 10 visible in metrics.json; enforcing it belongs to the child
# that owns the corpus sweep.
#
# The hygiene numbers are listed for a second reason: `compare` reds on a
# REPORTED_ONLY key that goes MISSING, so deleting the capped waiver count — or
# the count of violations those waivers suppress — from `metrics.json` breaks
# the ratchet instead of quietly removing the cap's subject. A cap nobody
# reports is a cap nobody holds.
REPORTED_ONLY = (
    "metrics.gate_specificity.waiver_sweep.waivers",
    "metrics.gate_specificity.waiver_sweep.hygiene_waivers",
    "metrics.gate_specificity.waiver_sweep.suppressed_violations",
)


def lookup(metrics: dict[str, Any], dotted: str) -> Any:
    node: Any = metrics
    for part in dotted.split("."):
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    return node


def compare(current: dict[str, Any], baseline: dict[str, Any]) -> list[dict[str, Any]]:
    """Return the regressions; empty means the ratchet holds."""
    regressions: list[dict[str, Any]] = []
    for dotted in HIGHER_IS_BETTER:
        now, before = lookup(current, dotted), lookup(baseline, dotted)
        if now is None or before is None:
            regressions.append(
                {"metric": dotted, "direction": "higher", "reason": "missing"}
            )
        elif now < before:
            regressions.append(
                {
                    "metric": dotted,
                    "direction": "higher",
                    "baseline": before,
                    "current": now,
                }
            )
    for dotted in LOWER_IS_BETTER:
        now, before = lookup(current, dotted), lookup(baseline, dotted)
        if now is None or before is None:
            regressions.append(
                {"metric": dotted, "direction": "lower", "reason": "missing"}
            )
        elif now > before:
            regressions.append(
                {
                    "metric": dotted,
                    "direction": "lower",
                    "baseline": before,
                    "current": now,
                }
            )
    for dotted in REPORTED_ONLY:
        if lookup(current, dotted) is None:
            regressions.append(
                {"metric": dotted, "direction": "reported", "reason": "missing"}
            )
    # The six audited lie classes may never be redefined: a baseline row whose
    # ground truth moved is a rewritten measuring stick, not an improvement.
    for row in lookup(baseline, "metrics.envelope_honesty.rows") or []:
        if not row.get("immutable"):
            continue
        match = next(
            (
                candidate
                for candidate in lookup(current, "metrics.envelope_honesty.rows") or []
                if candidate.get("case") == row.get("case")
            ),
            None,
        )
        if match is None:
            regressions.append(
                {
                    "metric": f"immutable_case:{row.get('case')}",
                    "direction": "pinned",
                    "reason": "missing",
                }
            )
        elif match.get("ground_truth") != row.get("ground_truth"):
            regressions.append(
                {
                    "metric": f"immutable_case:{row.get('case')}",
                    "direction": "pinned",
                    "baseline": row.get("ground_truth"),
                    "current": match.get("ground_truth"),
                }
            )
    return regressions


def load_baseline(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))
