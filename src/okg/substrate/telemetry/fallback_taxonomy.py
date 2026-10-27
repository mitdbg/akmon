"""The closed FallbackCause taxonomy over every recorded scheduling reason.

PACT ``consistency-maintenance-accounting``, task.accounting.taxonomy
(req.accounting.fallback-taxonomy). Normative semantics in
docs/develop/architecture/consistency.md §9.9 (charged maintenance work).

What this module is
-------------------
A single **closed** taxonomy (:data:`FALLBACK_TAXONOMY`) into which EVERY
step-level scheduling outcome maps — exactly one cause per outcome. The engine
already grew several disjoint reason vocabularies, each for its own operator's
debugging; this module ABSORBS them without loss rather than inventing a
parallel one. :func:`classify_reason` is the total map: a recorded reason with
no mapping raises loudly (a silent bucket would turn the hardest updates into
missing data, req.accounting.fallback-taxonomy).

The absorbed vocabularies (each mapped below, verbatim, with its source):

* ``FALLBACK_CAUSES`` / ``SKIP_CAUSES`` — the reuse scheduler's step-outcome
  causes (``okg.substrate.consistency.reuse_scheduler``), the only vocabulary
  already enforced closed at runtime (``StepOutcome`` refuses a non-member);
* ``FULL_RECOMPUTE_CAUSES`` — the maintained-derivation full-recompute reasons
  surfaced as ``summary['full_reason']`` / the ``by_full_reason`` histogram
  (``okg.substrate.derived_artifacts.maintained_derivation``);
* the coverage-scope reasons ``scope_plan_reason`` / ``scope_reason`` (the
  ``FrontierPlan.reason`` set in ``okg.substrate.consistency.ir_locality`` plus
  the runtime causes in ``ir_executor``);
* the enricher lineage reasons ``lineage_fallback_reason`` / ``lineage_status``
  / ``change_basis`` (``okg.substrate.enrichers.lineage`` + publisher);
* the dirty-scope ``fallback_reason`` (``derived_artifacts.dirty_rules``) and
  the correction-expiry reasons (``claim_effective_derivation``).

Some scope reasons carry a dynamic ``:``-suffix (``unrecognized_op:<kind>``,
``non_local_join:<...>``, ``unknown_subject_role:<role>``, …); those map on
their stem (the text before the first ``:``). A genuinely unknown reason — one
whose exact value and whose stem are both absent — raises, so a new recorded
reason cannot silently fall through the accounting.

This module holds NO heavy imports (the ledger and the equivalence harness
import it); the source vocabularies are mirrored here as literals with their
file citation, and ``tests/substrate/telemetry/test_work_ledger.py`` imports the
LIVE frozensets and asserts totality against this map — the drift detector.
"""
from __future__ import annotations

from okg.substrate.errors import SubstrateError

# --------------------------------------------------------------------------- #
# The closed taxonomy (the members a savings claim's denominator is drawn from)#
# --------------------------------------------------------------------------- #
# Reuse / carry-forward (work safely avoided, still charged to the update):
CAUSE_CERTIFICATE_BACKED_REUSE = "certificate_backed_reuse"
CAUSE_UNCHANGED_INPUTS_CARRY_FORWARD = "unchanged_inputs_carry_forward"
CAUSE_BOUNDED_INCREMENTAL = "bounded_incremental"
# Rerun / fallback (work the update had to do because reuse was not provable):
CAUSE_UNBOUNDED_ENVELOPE_OR_GENERATION = "unbounded_envelope_or_generation"
CAUSE_INCONCLUSIVE_SCORE_OR_TIEBREAK = "inconclusive_score_or_tiebreak"
CAUSE_GLOBAL_COUPLING_BUDGET = "global_coupling_budget"
CAUSE_MISSING_PROVENANCE_OR_REPLAY = "missing_provenance_or_replay"
CAUSE_CHANGED_PIPELINE_VERSION = "changed_pipeline_version"
# Loud terminal / operational fallbacks:
CAUSE_ENVELOPE_VIOLATION = "envelope_violation"
CAUSE_OPERATOR_FAILURE = "operator_failure"
CAUSE_OPERATIONAL_GATE_FAILURE = "operational_gate_failure"

#: The closed taxonomy. Every recorded reason maps to exactly one member; the
#: per-update distribution is a histogram over this set (surfaced through
#: ``okg metrics --consistency``). Covers the req.accounting.fallback-taxonomy
#: minimum (unbounded envelope/generation, inconclusive score/tie-break,
#: global-coupling budget, missing provenance/replay, changed pipeline version,
#: envelope violation, operator failure, operational gate failure) plus the skip
#: flavors (certificate-backed reuse, unchanged-inputs carry-forward).
FALLBACK_TAXONOMY: frozenset[str] = frozenset({
    CAUSE_CERTIFICATE_BACKED_REUSE,
    CAUSE_UNCHANGED_INPUTS_CARRY_FORWARD,
    CAUSE_BOUNDED_INCREMENTAL,
    CAUSE_UNBOUNDED_ENVELOPE_OR_GENERATION,
    CAUSE_INCONCLUSIVE_SCORE_OR_TIEBREAK,
    CAUSE_GLOBAL_COUPLING_BUDGET,
    CAUSE_MISSING_PROVENANCE_OR_REPLAY,
    CAUSE_CHANGED_PIPELINE_VERSION,
    CAUSE_ENVELOPE_VIOLATION,
    CAUSE_OPERATOR_FAILURE,
    CAUSE_OPERATIONAL_GATE_FAILURE,
})

#: One-line human descriptions (surfaced by the CLI so an operator reads the
#: distribution without the source code).
FALLBACK_CAUSE_DESCRIPTIONS: dict[str, str] = {
    CAUSE_CERTIFICATE_BACKED_REUSE:
        "reuse behind a valid / freshly justified reuse certificate",
    CAUSE_UNCHANGED_INPUTS_CARRY_FORWARD:
        "carry-forward: every dependency byte-identical to the prior generation",
    CAUSE_BOUNDED_INCREMENTAL:
        "work bounded to the changed scope (proven locality / watermark / lineage)",
    CAUSE_UNBOUNDED_ENVELOPE_OR_GENERATION:
        "unbounded envelope or whole-surface read — no locality to prove",
    CAUSE_INCONCLUSIVE_SCORE_OR_TIEBREAK:
        "score / tie-break / scope bound inconclusive — could not prove a skip",
    CAUSE_GLOBAL_COUPLING_BUDGET:
        "bounded-dominance skip (the global-coupling budget interaction, adopted)",
    CAUSE_MISSING_PROVENANCE_OR_REPLAY:
        "missing provenance, dependency declaration, or replay outcome",
    CAUSE_CHANGED_PIPELINE_VERSION:
        "a pinned pipeline / policy / catalog version changed",
    CAUSE_ENVELOPE_VIOLATION:
        "a producer emitted an out-of-envelope output (reuse poisoned)",
    CAUSE_OPERATOR_FAILURE:
        "a step raised while executing — the update aborted",
    CAUSE_OPERATIONAL_GATE_FAILURE:
        "an operational gate forced full work (forced / self-heal / anti-entropy)",
}


# --------------------------------------------------------------------------- #
# The total mapping — every recorded reason, verbatim, with its source.        #
# --------------------------------------------------------------------------- #
# The mapping is TOTAL over the closed vocabularies (the test proves it against
# the live frozensets). It is NOT injective: several vocabularies express the
# same engine condition under different operators' names, and they fold together
# here without loss. Each block cites the source module the literals mirror.
RECORDED_REASON_TO_CAUSE: dict[str, str] = {
    # -- reuse_scheduler.FALLBACK_CAUSES / SKIP_CAUSES (consistency/reuse_scheduler.py:148-185)
    "certificate_valid": CAUSE_CERTIFICATE_BACKED_REUSE,
    "certificate_degenerate_unchanged": CAUSE_UNCHANGED_INPUTS_CARRY_FORWARD,
    "skip_justified": CAUSE_CERTIFICATE_BACKED_REUSE,
    "skip_dominance": CAUSE_GLOBAL_COUPLING_BUDGET,
    "effects_overlap": CAUSE_INCONCLUSIVE_SCORE_OR_TIEBREAK,
    "run_always": CAUSE_UNBOUNDED_ENVELOPE_OR_GENERATION,
    # No change statement was made for the step, so the justified-skip rung had
    # nothing to test disjointness against. That is a MISSING declaration, not an
    # inconclusive comparison — folding it into effects_overlap would report an
    # overlap the engine never proved.
    "unstated_change": CAUSE_MISSING_PROVENANCE_OR_REPLAY,
    # An E3 mechanism-ablation arm ran the step because the rung that would have
    # licensed the skip was switched OFF in the run configuration. That is an
    # operator-set gate forcing full work — NOT an inconclusive comparison: the
    # analysis proved the skip and the configuration declined it, so folding it
    # into effects_overlap would report a refusal the engine never made.
    "rung_disabled": CAUSE_OPERATIONAL_GATE_FAILURE,
    "unknown_dependency": CAUSE_MISSING_PROVENANCE_OR_REPLAY,
    "envelope_violation": CAUSE_ENVELOPE_VIOLATION,
    "operator_failure": CAUSE_OPERATOR_FAILURE,

    # -- maintained_derivation.FULL_RECOMPUTE_CAUSES (derived_artifacts/maintained_derivation.py:118-153)
    #    (the summary['full_reason'] / by_full_reason histogram values)
    "forced": CAUSE_OPERATIONAL_GATE_FAILURE,
    "scope_unavailable": CAUSE_INCONCLUSIVE_SCORE_OR_TIEBREAK,
    "first_publish": CAUSE_MISSING_PROVENANCE_OR_REPLAY,
    "periodic_self_heal": CAUSE_OPERATIONAL_GATE_FAILURE,
    "deletion_shape": CAUSE_INCONCLUSIVE_SCORE_OR_TIEBREAK,
    "policy_change": CAUSE_CHANGED_PIPELINE_VERSION,
    "precedent_scope_unprovable": CAUSE_INCONCLUSIVE_SCORE_OR_TIEBREAK,
    "linked_pair_cross_bucket": CAUSE_INCONCLUSIVE_SCORE_OR_TIEBREAK,
    # The persisted selection rows were written under an older selection schema
    # version, so the differential carry is not licensed — a changed pipeline
    # version, exactly like policy_change.
    "selection_schema_upgrade": CAUSE_CHANGED_PIPELINE_VERSION,
    # The carried-forward rollup rows were maintained by a release that
    # predates a dimension the current release derives (issue #446's
    # console-count source_subtype dimension), so incremental folding is not
    # licensed until one full recount backfills the new shape — a changed
    # pipeline version, exactly like selection_schema_upgrade.
    "dimension_shape_upgrade": CAUSE_CHANGED_PIPELINE_VERSION,
    # The cycle changed a large share of the graph (a bulk load), so the
    # rollup recounted the whole generation instead of resolving every
    # changed id twice (issue 3955). Locality was available; the full read is
    # a configured cost choice, like the periodic self-heal.
    "large_change_set": CAUSE_OPERATIONAL_GATE_FAILURE,

    # -- ir_locality FrontierPlan.reason: scope_plan_reason (consistency/ir_locality.py)
    "claim_state_surface_bucket_locality_pending":  # :60 CLAIM_STATE_FULL_REASON
        CAUSE_INCONCLUSIVE_SCORE_OR_TIEBREAK,
    "search_op_reads_whole_index": CAUSE_UNBOUNDED_ENVELOPE_OR_GENERATION,  # :123
    "rank_over_other_rows": CAUSE_UNBOUNDED_ENVELOPE_OR_GENERATION,        # :129
    "no_from_surface": CAUSE_MISSING_PROVENANCE_OR_REPLAY,                 # :141
    "aggregation_over_other_rows": CAUSE_UNBOUNDED_ENVELOPE_OR_GENERATION,  # :152
    "limit_changes_result_set": CAUSE_UNBOUNDED_ENVELOPE_OR_GENERATION,   # :154
    "multiple_where_bindings": CAUSE_INCONCLUSIVE_SCORE_OR_TIEBREAK,      # :160
    "base_nodes+edges_live_joins": CAUSE_BOUNDED_INCREMENTAL,             # :180 (scoped)
    "single_node_surface_no_joins": CAUSE_BOUNDED_INCREMENTAL,           # :186 (scoped)
    "no_participant_roles": CAUSE_MISSING_PROVENANCE_OR_REPLAY,           # :304
    "ambiguous_subject_role": CAUSE_INCONCLUSIVE_SCORE_OR_TIEBREAK,       # :310
    "subject_not_a_base_relation_column": CAUSE_INCONCLUSIVE_SCORE_OR_TIEBREAK,  # :322

    # -- ir_executor runtime scope_reason (consistency/ir_executor.py)
    "dirty_scope_unavailable": CAUSE_MISSING_PROVENANCE_OR_REPLAY,        # ir_locality:259
    "dirty_edge_scope_unavailable": CAUSE_MISSING_PROVENANCE_OR_REPLAY,   # ir_locality:261
    "previous_checkpoint_unavailable_for_dirty_edges":                   # ir_locality:277
        CAUSE_MISSING_PROVENANCE_OR_REPLAY,
    "explicit_scope_subject_ids": CAUSE_BOUNDED_INCREMENTAL,              # ir_executor:179
    "antientropy_after_error": CAUSE_OPERATIONAL_GATE_FAILURE,            # ir_executor:344
    "antientropy_cadence_sweep": CAUSE_OPERATIONAL_GATE_FAILURE,          # ir_executor:370

    # -- enrichers/lineage.py lineage_fallback_reason + lineage_status
    "missing_dependency_declaration": CAUSE_MISSING_PROVENANCE_OR_REPLAY,  # lineage.py:22
    "available": CAUSE_BOUNDED_INCREMENTAL,                               # lineage.py:20
    "unavailable": CAUSE_MISSING_PROVENANCE_OR_REPLAY,                    # lineage.py:21

    # -- publisher change_basis (enrichers/base.py:131, publisher.py:4279-4287)
    "fact_watermark": CAUSE_BOUNDED_INCREMENTAL,
    "chronos_segment": CAUSE_BOUNDED_INCREMENTAL,
    "lineage_dependency": CAUSE_BOUNDED_INCREMENTAL,
    "targeted_enricher_backfill": CAUSE_BOUNDED_INCREMENTAL,

    # -- derived_artifacts/dirty_rules.py fallback_reason
    "dirty_scope_outside_scope_universe": CAUSE_MISSING_PROVENANCE_OR_REPLAY,  # :169
    "dirty_scope_sql_error": CAUSE_OPERATIONAL_GATE_FAILURE,                  # :173
    "dirty_scope_coverage_incomplete": CAUSE_INCONCLUSIVE_SCORE_OR_TIEBREAK,  # :394

    # -- claim_effective_derivation correction-expiry reasons (:199-200)
    "vanished_target": CAUSE_MISSING_PROVENANCE_OR_REPLAY,
    "target_never_existed": CAUSE_MISSING_PROVENANCE_OR_REPLAY,
}

#: Dynamic reasons carry a ``:``-suffix; they map on the stem before the first
#: ``:``. Sourced from ir_locality.py:138/164/169/313/315.
_DYNAMIC_STEM_TO_CAUSE: dict[str, str] = {
    "unrecognized_op": CAUSE_INCONCLUSIVE_SCORE_OR_TIEBREAK,           # :138
    "base_surface_not_nodes_live": CAUSE_UNBOUNDED_ENVELOPE_OR_GENERATION,  # :164
    "non_local_join": CAUSE_UNBOUNDED_ENVELOPE_OR_GENERATION,          # :169
    "unknown_subject_role": CAUSE_INCONCLUSIVE_SCORE_OR_TIEBREAK,      # :313
    "subject_column_not_projected": CAUSE_INCONCLUSIVE_SCORE_OR_TIEBREAK,  # :315
}


def classify_reason(reason: str) -> str:
    """Map one recorded reason to its closed-taxonomy cause, or RAISE.

    Total over the absorbed vocabularies (:data:`RECORDED_REASON_TO_CAUSE` +
    :data:`_DYNAMIC_STEM_TO_CAUSE`). Exact match first, then the ``:``-stem for a
    dynamic reason. A reason that matches neither raises :class:`SubstrateError`
    — an outcome without a taxonomy cause fails validation loudly
    (req.accounting.fallback-taxonomy), so a newly-introduced recorded reason
    cannot silently escape the accounting."""
    if not isinstance(reason, str) or not reason:
        raise SubstrateError(
            "classify_reason requires a non-empty recorded reason string",
            data={"got": repr(reason)},
        )
    cause = RECORDED_REASON_TO_CAUSE.get(reason)
    if cause is not None:
        return cause
    stem = reason.split(":", 1)[0]
    cause = _DYNAMIC_STEM_TO_CAUSE.get(stem)
    if cause is not None:
        return cause
    raise SubstrateError(
        "unmapped scheduling reason: no closed FallbackCause taxonomy member — "
        "an outcome without a cause is a loud validation failure, never a silent "
        "bucket (spec §9.9, req.accounting.fallback-taxonomy). Add it to "
        "RECORDED_REASON_TO_CAUSE (telemetry/fallback_taxonomy.py) with its "
        "source citation.",
        data={"reason": reason, "stem": stem,
              "taxonomy": sorted(FALLBACK_TAXONOMY)},
    )


def classify_distribution(reason_counts: dict[str, int]) -> dict[str, int]:
    """Fold a ``{recorded_reason: count}`` histogram into a taxonomy histogram.

    Every input reason is classified (an unmapped reason raises through
    :func:`classify_reason`), so a distribution is either fully mapped or fails
    loudly. Members with no observations are omitted (a zero is not fabricated)."""
    out: dict[str, int] = {}
    for reason, count in reason_counts.items():
        cause = classify_reason(reason)
        out[cause] = out.get(cause, 0) + int(count)
    return out


__all__ = [
    "FALLBACK_TAXONOMY",
    "FALLBACK_CAUSE_DESCRIPTIONS",
    "RECORDED_REASON_TO_CAUSE",
    "CAUSE_CERTIFICATE_BACKED_REUSE",
    "CAUSE_UNCHANGED_INPUTS_CARRY_FORWARD",
    "CAUSE_BOUNDED_INCREMENTAL",
    "CAUSE_UNBOUNDED_ENVELOPE_OR_GENERATION",
    "CAUSE_INCONCLUSIVE_SCORE_OR_TIEBREAK",
    "CAUSE_GLOBAL_COUPLING_BUDGET",
    "CAUSE_MISSING_PROVENANCE_OR_REPLAY",
    "CAUSE_CHANGED_PIPELINE_VERSION",
    "CAUSE_ENVELOPE_VIOLATION",
    "CAUSE_OPERATOR_FAILURE",
    "CAUSE_OPERATIONAL_GATE_FAILURE",
    "classify_reason",
    "classify_distribution",
]
