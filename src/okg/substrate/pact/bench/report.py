"""Metric 6 — envelope payload size — plus the metrics.json emitter.

``metrics.json`` must be byte-identical across runs: sorted keys, no
timestamps, no machine paths, no durations. Payload sizes are measured on
payloads whose fixture root has already been replaced by a fixed token
(``calls.normalize_payload``), because otherwise the byte count would depend
on where the operating system happened to put the tmp directory.
"""
from __future__ import annotations

import json
from statistics import median_low
from typing import Any

from okg.substrate.pact.bench.calls import Recorder

SCHEMA = "okg.pact.bench.metrics.v1"


def declared_budgets() -> dict[str, int]:
    from okg.substrate.pact import cli as pact_cli

    return {
        "gate post-edit": pact_cli.POST_EDIT_SUMMARY_BUDGET_BYTES,
        "view --format=bootstrap": pact_cli.BOOTSTRAP_SUMMARY_BUDGET_BYTES,
    }


def payload_sizes(recorder: Recorder) -> dict[str, Any]:
    budgets = declared_budgets()
    grouped: dict[str, list[int]] = {}
    for result in recorder.results:
        grouped.setdefault(result.verb, []).append(result.size_bytes)
    verbs: dict[str, Any] = {}
    for verb, sizes in sorted(grouped.items()):
        budget = budgets.get(verb)
        largest = max(sizes)
        verbs[verb] = {
            "count": len(sizes),
            "p50_bytes": median_low(sizes),
            "max_bytes": largest,
            "budget_bytes": budget,
            "over_budget": bool(budget is not None and largest > budget),
        }
    return {
        "verbs": verbs,
        "envelopes": len(recorder.results),
        "max_bytes": max((row["max_bytes"] for row in verbs.values()), default=0),
        "over_budget_verbs": sum(1 for row in verbs.values() if row["over_budget"]),
    }


def emit(metrics: dict[str, Any]) -> str:
    """Serialize deterministically. Sorted keys, no timestamps, trailing NL."""
    return json.dumps(metrics, sort_keys=True, indent=2) + "\n"


def human_table(metrics: dict[str, Any]) -> str:
    families = metrics["metrics"]
    honesty = families["envelope_honesty"]
    catch = families["gate_catch_rate"]
    specificity = families["gate_specificity"]
    refusals = families["refusal_self_sufficiency"]
    trips = families["round_trips_to_green"]
    manifest = families["manifest_quality"]
    size = families["envelope_payload_size"]
    lines = [
        "PACT friction benchmark",
        "-" * 64,
        f"1  envelope honesty          {honesty['honest_fraction']:.2f} "
        f"({honesty['immutable_honest']}/{honesty['immutable_rows']} immutable, "
        f"{honesty['derived_honest']}/{honesty['derived_rows']} derived)",
        f"2  gate catch rate           {catch['catch_rate']:.2f} "
        f"(present {catch['catch_rate_subject_present']:.2f} / "
        f"absent {catch['catch_rate_subject_absent']:.2f})",
        f"2b gate specificity          {specificity['acceptance_rate']:.2f} "
        f"({specificity['accepted']}/{specificity['sample_size']}), "
        # The CAPPED number is the hygiene one — waivers taken instead of
        # sweeping a violation. The corpus total is reported beside it because
        # it is the bigger number and would otherwise read as the capped one.
        f"hygiene waivers {specificity['waiver_sweep'].get('hygiene_waivers')}"
        f"/{specificity['waiver_sweep'].get('cap')} "
        f"(corpus total {specificity['waiver_sweep'].get('waivers')})",
        # THREE rates, printed as three. A blended headline number is exactly
        # what the three-population contract forbids.
        f"3  refusal self-sufficiency  "
        f"command {refusals['command_repairable_rate']:.2f} "
        f"({refusals['populations']['command_repairable']['repaired']}"
        f"/{refusals['command_repairable']}), "
        f"author {refusals['author_action_rate']:.2f} "
        f"({refusals['populations']['author_action']['repaired']}"
        f"/{refusals['author_action']}), "
        f"operator {refusals['operator_action_rate']:.2f} "
        f"({refusals['populations']['operator_action']['repaired']}"
        f"/{refusals['operator_action']})",
        f"4  round-trips-to-green      {trips['total_scored_calls']} scored calls "
        f"({trips['total_calls']} made, "
        f"{trips['total_edits_applied']} edits applied), "
        f"{trips['episodes_green']}/{trips['episodes_total']} green "
        f"(cap {trips['cap']})",
        f"5  manifest quality          {manifest['mean_score']:.2f} mean over "
        f"{manifest['scored']} manifests",
        f"6  envelope payload size     max {size['max_bytes']} B, "
        f"{size['over_budget_verbs']} verbs over budget",
        "-" * 64,
    ]
    lines.append("gate catch rate by (check, family):")
    for row in catch["rows"]:
        lines.append(
            f"  {row['check']:<28} {row['family']:<16} {row['outcome']}"
        )
    lines.append("envelope honesty, immutable rows:")
    for row in honesty["rows"]:
        if not row["immutable"]:
            continue
        lines.append(
            f"  {row['case']:<44} observed={row['observed_verdict']:<9} "
            f"truth={row['ground_truth']}"
        )
    return "\n".join(lines) + "\n"
