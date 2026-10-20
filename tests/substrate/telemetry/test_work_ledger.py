"""Witnesses for the charged maintenance-work ledger + fallback taxonomy.

PACT ``consistency-maintenance-accounting`` (task.accounting.ledger /
task.accounting.taxonomy / task.accounting.report). The two §9.9-named
obligations land here:
``test_validation_runs_excluded_from_savings`` and
``test_savings_report_renders_from_ledger_rows_only``
(docs/develop/architecture/consistency.md §9.9).

The pure witnesses (taxonomy totality, builder gap behavior, dataclass
invariants) run without a database; the ledger / savings-report witnesses use the
shared ``clean_runtime`` real-Postgres fixture (rows truncated per test).
"""
from __future__ import annotations

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.telemetry.fallback_taxonomy import (
    FALLBACK_TAXONOMY,
    classify_distribution,
    classify_reason,
)
from okg.substrate.telemetry.work_ledger import (
    CATEGORY_CERTIFICATE_VALIDATION,
    CATEGORY_DETERMINISTIC_EXECUTION,
    CATEGORY_EFFECT_ANALYSIS,
    CATEGORY_MODEL_CALLS,
    CATEGORY_PROPAGATION,
    CATEGORY_REPAIR_SOLVING,
    MODE_COMPLETE_REFERENCE,
    MODE_DEPENDENCY_BASELINE,
    MODE_DIFFERENTIAL,
    WORK_CATEGORIES,
    CategoryMeasure,
    ModelSpend,
    UnitPricing,
    WorkLedgerRow,
    build_rows_from_cycle_output,
    load_work_ledger,
    model_call_spend,
    record_work_ledger,
    render_savings_report,
    work_ledger_distribution,
)


# --------------------------------------------------------------------------- #
# A duck-typed reuse-pass result (the builder reads .outcomes / .skips /       #
# .cause_counts()).                                                            #
# --------------------------------------------------------------------------- #
class _FakeReusePass:
    def __init__(self, *, outcomes: int, skips: int, causes: dict[str, int]):
        self.outcomes = [None] * outcomes
        self.skips = [None] * skips
        self._causes = causes

    def cause_counts(self) -> dict[str, int]:
        return dict(self._causes)


# =========================================================================== #
# task.accounting.taxonomy — the closed taxonomy absorbs every vocabulary.     #
# =========================================================================== #
def test_taxonomy_absorbs_reuse_and_full_recompute_vocabularies_without_loss() -> None:
    """Every live FALLBACK_CAUSES / SKIP_CAUSES / FULL_RECOMPUTE_CAUSES member
    classifies to exactly one closed-taxonomy cause (mapping totality)."""
    from okg.substrate.consistency.reuse_scheduler import (
        FALLBACK_CAUSES,
        SKIP_CAUSES,
    )
    from okg.substrate.derived_artifacts.maintained_derivation import (
        FULL_RECOMPUTE_CAUSES,
    )

    assert SKIP_CAUSES <= FALLBACK_CAUSES
    for reason in FALLBACK_CAUSES | FULL_RECOMPUTE_CAUSES:
        cause = classify_reason(reason)
        assert cause in FALLBACK_TAXONOMY, (reason, cause)
    # the skip flavors land on the reuse / carry-forward members, never a fallback.
    from okg.substrate.telemetry.fallback_taxonomy import (
        CAUSE_CERTIFICATE_BACKED_REUSE,
        CAUSE_GLOBAL_COUPLING_BUDGET,
        CAUSE_UNCHANGED_INPUTS_CARRY_FORWARD,
    )
    assert classify_reason("certificate_valid") == CAUSE_CERTIFICATE_BACKED_REUSE
    assert classify_reason("certificate_degenerate_unchanged") == (
        CAUSE_UNCHANGED_INPUTS_CARRY_FORWARD
    )
    assert classify_reason("skip_dominance") == CAUSE_GLOBAL_COUPLING_BUDGET


def test_taxonomy_absorbs_scope_and_lineage_reasons() -> None:
    """scope_plan_reason, scope_reason, and lineage_fallback_reason /
    lineage_status all map into the taxonomy without loss (the verification
    clause names these explicitly)."""
    from okg.substrate.consistency.ir_locality import CLAIM_STATE_FULL_REASON
    from okg.substrate.enrichers.lineage import (
        DIRTY_SCOPE_UNAVAILABLE,
        LINEAGE_STATUS_AVAILABLE,
        LINEAGE_STATUS_UNAVAILABLE,
        MISSING_DEPENDENCY_DECLARATION,
    )

    scope_reasons = [
        CLAIM_STATE_FULL_REASON,
        "search_op_reads_whole_index",
        "rank_over_other_rows",
        "aggregation_over_other_rows",
        "limit_changes_result_set",
        "multiple_where_bindings",
        "no_from_surface",
        "base_nodes+edges_live_joins",
        "single_node_surface_no_joins",
        "no_participant_roles",
        "ambiguous_subject_role",
        "subject_not_a_base_relation_column",
        "explicit_scope_subject_ids",
        "antientropy_after_error",
        "antientropy_cadence_sweep",
        "previous_checkpoint_unavailable_for_dirty_edges",
    ]
    for reason in scope_reasons:
        assert classify_reason(reason) in FALLBACK_TAXONOMY, reason
    for reason in (
        DIRTY_SCOPE_UNAVAILABLE, MISSING_DEPENDENCY_DECLARATION,
        LINEAGE_STATUS_AVAILABLE, LINEAGE_STATUS_UNAVAILABLE,
        "fact_watermark", "chronos_segment", "lineage_dependency",
    ):
        assert classify_reason(reason) in FALLBACK_TAXONOMY, reason


def test_dynamic_scope_reason_stems_classify() -> None:
    """Dynamic ``:``-suffix scope reasons map on their stem."""
    for reason in (
        "unrecognized_op:search",
        "non_local_join:edge_facts",
        "unknown_subject_role:foo",
        "subject_column_not_projected:col_x",
        "base_surface_not_nodes_live:edges_live",
    ):
        assert classify_reason(reason) in FALLBACK_TAXONOMY, reason


def test_unmapped_reason_fails_validation_loudly() -> None:
    """An outcome whose reason is absent from every vocabulary raises — never a
    silent bucket (req.accounting.fallback-taxonomy)."""
    with pytest.raises(SubstrateError, match="unmapped scheduling reason"):
        classify_reason("totally_invented_reason_that_does_not_exist")
    with pytest.raises(SubstrateError):
        classify_reason("")
    # a distribution with one unknown reason fails as a whole (no partial map).
    with pytest.raises(SubstrateError):
        classify_distribution({"certificate_valid": 3, "bogus_reason": 1})


def test_classify_distribution_preserves_total_and_maps_each() -> None:
    """Each recorded reason folds into exactly one taxonomy member; the histogram
    total is preserved."""
    reasons = {"certificate_valid": 2, "skip_justified": 1, "run_always": 1,
               "first_publish": 3, "policy_change": 1}
    dist = classify_distribution(reasons)
    assert set(dist) <= FALLBACK_TAXONOMY
    assert sum(dist.values()) == sum(reasons.values()) == 8
    # certificate_valid + skip_justified both fold onto certificate_backed_reuse.
    from okg.substrate.telemetry.fallback_taxonomy import (
        CAUSE_CERTIFICATE_BACKED_REUSE,
    )
    assert dist[CAUSE_CERTIFICATE_BACKED_REUSE] == 3


# =========================================================================== #
# task.accounting.ledger — builders cover every category; gaps are loud.       #
# =========================================================================== #
def test_cycle_output_builder_covers_every_category() -> None:
    """A differential cycle with a reuse pass + maintained derivations + a model
    join + a repair count measures ALL SIX categories."""
    cycle_output = {
        "maintained_derivations": {
            "claim_effective": {"status": "maintained", "mode": "full_recount",
                                "rows_written": 5, "full_reason": "first_publish"},
            "claim_projection": {"status": "maintained", "mode": "incremental",
                                 "rows_written": 3, "full_reason": None},
        },
    }
    reuse_pass = _FakeReusePass(
        outcomes=3, skips=2, causes={"certificate_valid": 2, "run_always": 1},
    )
    rows = build_rows_from_cycle_output(
        deployment="dep", generation_id=11, cycle_output=cycle_output,
        model_spend=ModelSpend(4, 100, 40, 140, 0.5),
        repair_solved=6, reuse_pass_result=reuse_pass,
    )
    assert {r.category for r in rows} == set(WORK_CATEGORIES)
    assert all(r.mode == MODE_DIFFERENTIAL and not r.validation for r in rows)
    assert all(r.measured for r in rows)  # nothing uninstrumented here
    by_cat = {r.category: r for r in rows}
    assert by_cat[CATEGORY_EFFECT_ANALYSIS].work_units == 3      # outcomes
    assert by_cat[CATEGORY_CERTIFICATE_VALIDATION].work_units == 2  # skips
    assert by_cat[CATEGORY_DETERMINISTIC_EXECUTION].work_units == 8  # 5 + 3
    assert by_cat[CATEGORY_PROPAGATION].work_units == 2          # executed
    assert by_cat[CATEGORY_REPAIR_SOLVING].work_units == 6
    model = by_cat[CATEGORY_MODEL_CALLS]
    assert (model.work_units, model.total_tokens, model.cost_usd) == (4, 140, 0.5)
    # the closed-taxonomy histogram rides on the effect_analysis row.
    assert by_cat[CATEGORY_EFFECT_ANALYSIS].cause_counts
    assert set(by_cat[CATEGORY_EFFECT_ANALYSIS].cause_counts) <= FALLBACK_TAXONOMY


def test_uninstrumented_category_renders_loud_gap() -> None:
    """A differential cycle with no reuse pass, no maintained_derivations, and no
    repair read renders those categories as LOUD GAPS (measured=FALSE, NULL units)
    — never a silent zero. The model join still measures (0 is honest)."""
    rows = build_rows_from_cycle_output(
        deployment="dep", generation_id=1, cycle_output={},
        model_spend=ModelSpend(0, 0, 0, 0, 0.0),
        repair_solved=None, reuse_pass_result=None,
    )
    by_cat = {r.category: r for r in rows}
    gap_categories = {
        CATEGORY_EFFECT_ANALYSIS, CATEGORY_CERTIFICATE_VALIDATION,
        CATEGORY_DETERMINISTIC_EXECUTION, CATEGORY_PROPAGATION,
        CATEGORY_REPAIR_SOLVING,
    }
    for category in gap_categories:
        assert by_cat[category].measured is False
        assert by_cat[category].work_units is None
        assert by_cat[category].detail.get("gap_reason")
    # model_calls ran the join: a measured, honest zero.
    assert by_cat[CATEGORY_MODEL_CALLS].measured is True
    assert by_cat[CATEGORY_MODEL_CALLS].work_units == 0


def test_category_measure_rejects_silent_zero() -> None:
    """A GAP measure must carry NULL units; a measured one must carry a count."""
    with pytest.raises(SubstrateError):
        CategoryMeasure(measured=True, work_units=None)   # measured, no count
    with pytest.raises(SubstrateError):
        CategoryMeasure(measured=False, work_units=0)     # a silent zero
    assert CategoryMeasure.of(0).work_units == 0          # honest zero is fine
    assert CategoryMeasure.gap("x").work_units is None


def test_work_ledger_row_validation_is_structural() -> None:
    """validation must equal (mode is a from-scratch mode); measured must equal
    (work_units is not None). The constructor refuses a malformed row."""
    with pytest.raises(SubstrateError):
        WorkLedgerRow(
            deployment="d", generation_id="1", mode=MODE_COMPLETE_REFERENCE,
            category=CATEGORY_EFFECT_ANALYSIS, validation=False, measured=True,
            work_units=0,
        )
    with pytest.raises(SubstrateError):
        WorkLedgerRow(
            deployment="d", generation_id="1", mode=MODE_DIFFERENTIAL,
            category=CATEGORY_EFFECT_ANALYSIS, validation=False, measured=True,
            work_units=None,
        )


# =========================================================================== #
# task.wave2.dollar — the PRICED dimension (D8 / F1.2).                         #
# =========================================================================== #
def test_unit_pricing_from_config_is_loud_on_bad_declaration() -> None:
    """A deployment-config unit price map is validated LOUDLY: model_calls is not
    unit-priceable (measured dollars), an unknown category and a negative price
    both raise — never a silently dropped price (a dropped price is the fabricated
    default the P0 rule forbids). Empty / None declares NO prices (loud null)."""
    with pytest.raises(SubstrateError, match="unit-priceable"):
        UnitPricing.from_config({CATEGORY_MODEL_CALLS: 0.01})
    with pytest.raises(SubstrateError, match="unit-priceable"):
        UnitPricing.from_config({"totally_made_up_category": 0.01})
    with pytest.raises(SubstrateError, match="non-negative"):
        UnitPricing.from_config({CATEGORY_PROPAGATION: -1.0})
    assert UnitPricing.from_config(None).price_for(CATEGORY_PROPAGATION) is None
    assert UnitPricing.empty().price_for(CATEGORY_EFFECT_ANALYSIS) is None
    priced = UnitPricing.from_config({CATEGORY_DETERMINISTIC_EXECUTION: 0.0})
    # a declared $0.00 IS a price basis (0.0, not None) — distinct from unpriced.
    assert priced.price_for(CATEGORY_DETERMINISTIC_EXECUTION) == 0.0
    assert priced.price_for(CATEGORY_PROPAGATION) is None


def test_pricing_does_not_alter_unit_accounting() -> None:
    """The PRICED dimension is purely additive: pricing sets cost_usd only, NEVER
    work_units. Building the same cycle with and without a unit-price config
    yields byte-identical units in every category (contract: unit numbers must be
    byte-identical to before). model_calls dollars are the same either way (never
    unit-priced); deterministic dollars are NULL without a price, a value with."""
    cycle_output = {
        "maintained_derivations": {
            "claim_effective": {"status": "maintained", "mode": "full_recount",
                                "rows_written": 5, "full_reason": "first_publish"},
            "claim_projection": {"status": "maintained", "mode": "incremental",
                                 "rows_written": 3, "full_reason": None},
        },
    }
    reuse_pass = _FakeReusePass(
        outcomes=3, skips=2, causes={"certificate_valid": 2, "run_always": 1},
    )
    common = dict(
        deployment="dep", generation_id=11, cycle_output=cycle_output,
        model_spend=ModelSpend(4, 100, 40, 140, 0.5),
        repair_solved=6, reuse_pass_result=reuse_pass,
    )
    unpriced = {r.category: r for r in build_rows_from_cycle_output(**common)}
    priced = {r.category: r for r in build_rows_from_cycle_output(
        **common,
        pricing=UnitPricing.from_config({
            CATEGORY_EFFECT_ANALYSIS: 1.5,
            CATEGORY_CERTIFICATE_VALIDATION: 2.0,
            CATEGORY_DETERMINISTIC_EXECUTION: 0.01,
            CATEGORY_PROPAGATION: 0.5,
            CATEGORY_REPAIR_SOLVING: 3.0,
        }),
    )}
    # byte-identical units across every category — pricing is additive.
    assert {c: r.work_units for c, r in unpriced.items()} == {
        c: r.work_units for c, r in priced.items()
    }
    # deterministic dollars: NULL without a price basis, unit*price with one.
    assert unpriced[CATEGORY_EFFECT_ANALYSIS].cost_usd is None
    assert unpriced[CATEGORY_DETERMINISTIC_EXECUTION].cost_usd is None
    assert priced[CATEGORY_EFFECT_ANALYSIS].cost_usd == pytest.approx(3 * 1.5)
    assert priced[CATEGORY_DETERMINISTIC_EXECUTION].cost_usd == pytest.approx(8 * 0.01)
    # model_calls carries MEASURED dollars — identical either way, never priced.
    assert unpriced[CATEGORY_MODEL_CALLS].cost_usd == 0.5
    assert priced[CATEGORY_MODEL_CALLS].cost_usd == 0.5


# =========================================================================== #
# Real-Postgres ledger witnesses.                                             #
# =========================================================================== #
def _seed_llm_call(cur, *, deployment, generation_id, prompt, completion,
                   total, cost, success=True):
    cur.execute(
        """
        INSERT INTO okg.llm_calls
            (deployment_name, caller, model, prompt_tokens, completion_tokens,
             total_tokens, cost_usd, success, generation_id)
        VALUES (%s, 'test:caller', 'test-model', %s, %s, %s, %s, %s, %s)
        """,
        (deployment, prompt, completion, total, cost, success, generation_id),
    )


@pytest.mark.postgres
def test_model_calls_joined_from_llm_calls(clean_runtime: str) -> None:
    """model_call_spend aggregates okg.llm_calls by generation (success only,
    the deployment + generation scoped join the ledger freezes into the row)."""
    import psycopg

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            # okg.llm_calls is a durable cost ledger (NOT in the per-test
            # truncation set), so scope + delete-first keeps this seed idempotent.
            cur.execute(
                "DELETE FROM okg.llm_calls WHERE deployment_name IN ('acc', 'other')"
            )
            _seed_llm_call(cur, deployment="acc", generation_id=42,
                           prompt=100, completion=50, total=150, cost="0.010")
            _seed_llm_call(cur, deployment="acc", generation_id=42,
                           prompt=200, completion=60, total=260, cost="0.020")
            # excluded: a failure, another generation, another deployment.
            _seed_llm_call(cur, deployment="acc", generation_id=42, prompt=9,
                           completion=9, total=9, cost="9.0", success=False)
            _seed_llm_call(cur, deployment="acc", generation_id=99, prompt=1,
                           completion=1, total=1, cost="1.0")
            _seed_llm_call(cur, deployment="other", generation_id=42, prompt=1,
                           completion=1, total=1, cost="1.0")
        spend = model_call_spend(conn, deployment="acc", generation_id=42)

    assert spend.calls == 2
    assert spend.prompt_tokens == 300
    assert spend.completion_tokens == 110
    assert spend.total_tokens == 410
    assert abs(spend.cost_usd - 0.03) < 1e-9


@pytest.mark.postgres
def test_ledger_rows_queryable_per_generation(clean_runtime: str) -> None:
    """A built row set records + loads back per generation (task.accounting.ledger:
    rows queryable per generation)."""
    import psycopg

    cycle_output = {"maintained_derivations": {
        "claim_projection": {"status": "maintained", "mode": "incremental",
                             "rows_written": 4, "full_reason": None},
    }}
    reuse_pass = _FakeReusePass(outcomes=2, skips=1,
                                causes={"certificate_degenerate_unchanged": 2})
    rows = build_rows_from_cycle_output(
        deployment="acc", generation_id=5, cycle_output=cycle_output,
        model_spend=ModelSpend(0, 0, 0, 0, 0.0), repair_solved=2,
        reuse_pass_result=reuse_pass,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        written = record_work_ledger(conn, rows)
        assert written == len(WORK_CATEGORIES)
        one_gen = load_work_ledger(conn, "acc", generation_id=5)
        assert {r.category for r in one_gen} == set(WORK_CATEGORIES)
        assert load_work_ledger(conn, "acc", generation_id=999) == []
        # re-record replaces its own (deployment, generation, mode) rows only.
        assert record_work_ledger(conn, rows) == len(WORK_CATEGORIES)
        assert len(load_work_ledger(conn, "acc")) == len(WORK_CATEGORIES)


def _mode_rows(deployment, generation_id, mode, *, units, cause_counts=None):
    """A full six-category row set with a uniform per-category unit count."""
    rows: list[WorkLedgerRow] = []
    for category in WORK_CATEGORIES:
        if category == CATEGORY_MODEL_CALLS:
            measure = ModelSpend(
                units, units, units, units, float(units) / 100.0
            ).as_measure()
        else:
            measure = CategoryMeasure.of(units)
        rows.append(WorkLedgerRow.build(
            deployment=deployment, generation_id=generation_id, mode=mode,
            category=category, measure=measure,
            cause_counts=(cause_counts if category == CATEGORY_EFFECT_ANALYSIS
                          else None),
        ))
    return rows


@pytest.mark.postgres
def test_validation_runs_excluded_from_savings(clean_runtime: str) -> None:
    """§9.9 witness. A from-scratch validation run's charged work is NEVER counted
    as production work saved: the report's production charged figure sums the
    differential (validation=FALSE) rows only, though all three modes are stored."""
    import psycopg

    deployment = "acc"
    # differential (production) charges 10 units/category; the two from-scratch
    # validation runs charge far more (they run everything). If a validation run
    # leaked into the production figure it would balloon it.
    diff_rows = _mode_rows(deployment, 7, MODE_DIFFERENTIAL, units=10)
    ref_rows = _mode_rows(deployment, 7, MODE_COMPLETE_REFERENCE, units=100)
    base_rows = _mode_rows(deployment, 7, MODE_DEPENDENCY_BASELINE, units=80)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        record_work_ledger(conn, [*diff_rows, *ref_rows, *base_rows])
        report = render_savings_report(conn, deployment)
        rows = load_work_ledger(conn, deployment)

    # production charged work counts validation=FALSE (differential) rows only.
    differential_units = sum(
        int(r.work_units or 0) for r in rows
        if not r.validation and r.measured
    )
    all_units = sum(int(r.work_units or 0) for r in rows if r.measured)
    assert report.production_total_units == differential_units == 60  # 6 x 10
    assert all_units == 60 + 600 + 480  # would be the WRONG figure if leaked
    assert report.production_total_units < all_units

    # the validation runs ARE present (as the counterfactual columns), flagged.
    assert report.charges[MODE_COMPLETE_REFERENCE].validation is True
    assert report.charges[MODE_DEPENDENCY_BASELINE].validation is True
    assert report.charges[MODE_DIFFERENTIAL].validation is False
    # savings = counterfactual − differential, positive here (reuse saved work).
    assert report.savings_vs_reference_units == 600 - 60
    assert report.savings_vs_baseline_units == 480 - 60


@pytest.mark.postgres
def test_savings_report_renders_from_ledger_rows_only(clean_runtime: str) -> None:
    """§9.9 witness. The report renders from ledger rows ALONE — deleting one
    instrumentation row breaks it loudly rather than papering over the gap."""
    import psycopg

    deployment = "acc"
    all_rows = [
        *_mode_rows(deployment, 9, MODE_DIFFERENTIAL, units=10,
                    cause_counts={"certificate_backed_reuse": 3}),
        *_mode_rows(deployment, 9, MODE_COMPLETE_REFERENCE, units=100),
        *_mode_rows(deployment, 9, MODE_DEPENDENCY_BASELINE, units=80),
    ]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        record_work_ledger(conn, all_rows)

        report = render_savings_report(conn, deployment)
        assert set(report.charges) == {
            MODE_DIFFERENTIAL, MODE_COMPLETE_REFERENCE, MODE_DEPENDENCY_BASELINE,
        }
        printed = report.render()
        assert deployment in printed
        assert "certificate_backed_reuse" in printed  # the distribution rendered

        # delete a single instrumentation row -> the report refuses to render.
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM okg.work_ledger "
                "WHERE deployment_name = %s AND generation_id = '9' "
                "  AND mode = %s AND category = %s",
                (deployment, MODE_DIFFERENTIAL, CATEGORY_DETERMINISTIC_EXECUTION),
            )
        with pytest.raises(SubstrateError, match="missing an instrumentation row"):
            render_savings_report(conn, deployment)


@pytest.mark.postgres
def test_metrics_consistency_surfaces_distribution(clean_runtime: str) -> None:
    """The per-update cause distribution + loud gaps are queryable through the
    surface okg metrics --consistency renders (task.accounting.taxonomy)."""
    import psycopg

    deployment = "acc"
    rows = _mode_rows(
        deployment, 3, MODE_DIFFERENTIAL, units=5,
        cause_counts={"certificate_backed_reuse": 2,
                      "unbounded_envelope_or_generation": 1},
    )
    # replace one category with a LOUD GAP so the surface reports it.
    rows = [r for r in rows if r.category != CATEGORY_PROPAGATION]
    rows.append(WorkLedgerRow(
        deployment=deployment, generation_id="3", mode=MODE_DIFFERENTIAL,
        category=CATEGORY_PROPAGATION, validation=False, measured=False,
        work_units=None, detail={"gap_reason": "uninstrumented in this test"},
    ))
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        record_work_ledger(conn, rows)
        dist = work_ledger_distribution(conn, deployment)

    assert dist["aggregate_cause_distribution"] == {
        "certificate_backed_reuse": 2, "unbounded_envelope_or_generation": 1,
    }
    assert len(dist["per_update"]) == 1
    assert dist["per_update"][0]["mode"] == MODE_DIFFERENTIAL
    assert any(g["category"] == CATEGORY_PROPAGATION for g in dist["gaps"])


@pytest.mark.postgres
def test_unpriced_category_renders_loud_null_not_zero(clean_runtime: str) -> None:
    """task.wave2.dollar witness (a). A production category with NO deployment
    price basis renders a LOUD NULL cost (None) in the report — never a silent
    zero — and drives production_cost_priced_complete FALSE. A category priced at
    $0.00 stays a real 0.0, kept distinct from the unpriced null."""
    import psycopg

    deployment = "acc"
    pricing = UnitPricing.from_config({
        CATEGORY_EFFECT_ANALYSIS: 0.0,            # priced at $0.00 (a real basis)
        CATEGORY_DETERMINISTIC_EXECUTION: 0.002,  # priced positive
        # certificate_validation / propagation / repair_solving: NO price basis.
    })
    cycle_output = {"maintained_derivations": {
        "claim_projection": {"status": "maintained", "mode": "incremental",
                             "rows_written": 4, "full_reason": None},
    }}
    reuse_pass = _FakeReusePass(outcomes=3, skips=2,
                                causes={"certificate_valid": 2})
    rows = build_rows_from_cycle_output(
        deployment=deployment, generation_id=5, cycle_output=cycle_output,
        model_spend=ModelSpend(0, 0, 0, 0, 0.0), repair_solved=6,
        reuse_pass_result=reuse_pass, pricing=pricing,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        record_work_ledger(conn, rows)
        report = render_savings_report(conn, deployment)

    charge = report.charges[MODE_DIFFERENTIAL]
    # priced at $0.00 is a real basis — 0.0, NOT None.
    assert charge.per_category_cost[CATEGORY_EFFECT_ANALYSIS] == 0.0
    # priced positive: 4 rows_written * 0.002.
    assert charge.per_category_cost[CATEGORY_DETERMINISTIC_EXECUTION] == pytest.approx(
        0.008
    )
    # model_calls carries MEASURED dollars from the join: an honest, priced zero.
    assert charge.per_category_cost[CATEGORY_MODEL_CALLS] == 0.0
    # NO price basis -> LOUD NULL (None), never a silent zero.
    for unpriced in (CATEGORY_CERTIFICATE_VALIDATION, CATEGORY_PROPAGATION,
                     CATEGORY_REPAIR_SOLVING):
        assert charge.per_category_cost[unpriced] is None
    # the completeness flag is FALSE: not every production category was priced.
    assert charge.cost_priced_complete is False
    assert report.production_cost_priced_complete is False
    # units are whole and unaffected by pricing (the priced dimension is additive).
    assert charge.per_category_units[CATEGORY_DETERMINISTIC_EXECUTION] == 4
    assert charge.total_units == 3 + 2 + 4 + 0 + 1 + 6
    # the rendered report shows the loud-null token, distinct from $0.00.
    printed = report.render()
    assert "unpriced" in printed


@pytest.mark.postgres
def test_measured_model_dollars_flow_into_report(clean_runtime: str) -> None:
    """task.wave2.dollar witness (b). MEASURED model dollars joined from
    okg.llm_calls flow into the model_calls category cost and, when the
    deterministic categories are all priced, into the complete production
    priced-cost figure."""
    import psycopg

    deployment = "acc"
    all_priced = UnitPricing.from_config({
        CATEGORY_EFFECT_ANALYSIS: 0.0,
        CATEGORY_CERTIFICATE_VALIDATION: 0.0,
        CATEGORY_DETERMINISTIC_EXECUTION: 0.0,
        CATEGORY_PROPAGATION: 0.0,
        CATEGORY_REPAIR_SOLVING: 0.0,
    })
    cycle_output = {"maintained_derivations": {
        "claim_projection": {"status": "maintained", "mode": "incremental",
                             "rows_written": 4, "full_reason": None},
    }}
    reuse_pass = _FakeReusePass(outcomes=1, skips=1,
                                causes={"certificate_valid": 1})
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM okg.llm_calls WHERE deployment_name = 'acc'")
            _seed_llm_call(cur, deployment="acc", generation_id=21,
                           prompt=100, completion=50, total=150, cost="0.010")
            _seed_llm_call(cur, deployment="acc", generation_id=21,
                           prompt=200, completion=60, total=260, cost="0.020")
        spend = model_call_spend(conn, deployment="acc", generation_id=21)
        assert abs(spend.cost_usd - 0.03) < 1e-9  # the measured join
        rows = build_rows_from_cycle_output(
            deployment=deployment, generation_id=21, cycle_output=cycle_output,
            model_spend=spend, repair_solved=0,
            reuse_pass_result=reuse_pass, pricing=all_priced,
        )
        record_work_ledger(conn, rows)
        report = render_savings_report(conn, deployment)

    charge = report.charges[MODE_DIFFERENTIAL]
    # the MEASURED model dollars land on the model_calls category cost.
    assert charge.per_category_cost[CATEGORY_MODEL_CALLS] == pytest.approx(0.03)
    assert charge.model_calls == 2 and charge.total_tokens == 410
    # every deterministic category priced ($0.00) -> a complete priced report.
    assert charge.cost_priced_complete is True
    assert report.production_cost_priced_complete is True
    # production priced cost == the model dollars (deterministic all $0.00).
    assert report.production_cost_usd == pytest.approx(0.03)
    # and it renders.
    assert "0.030000" in report.render()
