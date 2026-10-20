"""The per-update charged maintenance-work ledger + the three-mode savings report.

PACT ``consistency-maintenance-accounting``, task.accounting.ledger +
task.accounting.report (req.accounting.per-update-ledger +
req.accounting.savings-report). Normative semantics in
docs/develop/architecture/consistency.md §9.9 (charged maintenance work).

What this module is
-------------------
The cross-cutting per-update roll-up the engine never had: one
:class:`WorkLedgerRow` per ``(deployment, generation, mode, category)`` charging
the six categories a savings claim must pay for — **effect_analysis,
certificate_validation, deterministic_execution, model_calls, propagation,
repair_solving**. The pieces existed and did not compose (a strong per-call LLM
spend ledger, per-stage publish timings, scattered fallback reasons); this
composes them into the denominator an honest "did differential save work after
paying for its own analysis?" needs.

Three honesty rails, each structural (a DB CHECK), never prose:

* **Validation runs are marked and excluded from production savings.** The
  harness's complete-reference and dependency-baseline runs write
  ``validation = TRUE`` rows (``mode <> 'differential'``); the savings report
  sums *production* charged work over ``validation = FALSE`` only, so a
  from-scratch counterfactual can never be miscounted as work saved.
* **A missing instrumentation is a LOUD GAP, never a silent zero.** A category
  the run did not instrument records ``measured = FALSE`` with NULL units — a
  visible gap in the row. A silent zero would let an unaccounted category read
  as "no work", exactly how a net-negative mechanism would hide as a win.
* **The savings report renders from ledger rows ALONE.** Deleting an
  instrumentation row breaks the report loudly (:func:`render_savings_report`
  refuses a ``(generation, mode)`` group missing any category) rather than the
  report papering over the regression.

Measurement model (record what the runs carry; invent no timings)
-----------------------------------------------------------------
Each category is measured in the unit its run output naturally carries:

* **effect_analysis** — steps the scheduler analyzed (the reuse pass' outcome
  count). A validation run does ZERO analysis by construction (``reuse_disabled``),
  a structural zero; a differential cycle with no recorded reuse pass is a GAP.
* **certificate_validation** — certificates the scheduler validated / wrote (the
  reuse pass' skip count). Structural zero on a validation run; GAP with no pass.
* **deterministic_execution** — rows the maintained derivations materialized
  (``Σ rows_written``). The primary non-model savings signal: a full recompute
  writes ``≥`` an incremental cycle. Same source both modes → comparable.
* **model_calls** — calls / prompt+completion+total tokens / cost, joined from
  ``okg.llm_calls`` by generation (the ``llm_call_stats`` idiom,
  :func:`model_call_spend`) and frozen into the row so a dropped scratch DB does
  not take its charged spend with it.
* **propagation** — dependent computations reached (maintained-derivation
  executions this cycle).
* **repair_solving** — repairs freshly solved this generation (the
  ``okg.repair_selections`` rows with ``carried_from_generation IS NULL``,
  :func:`repair_solved_count`); a full recompute solves every bucket, an
  incremental only the dirty ones → comparable.

The closed-taxonomy cause histogram (:mod:`.fallback_taxonomy`) rides on the
``effect_analysis`` row (the category that owns the scheduling decisions),
folding the reuse-pass causes and the derivation ``full_reason`` values into one
per-update distribution surfaced through ``okg metrics --consistency``.
"""
from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import Any

from okg.substrate.errors import SubstrateError
from okg.substrate.telemetry.fallback_taxonomy import classify_distribution

# --------------------------------------------------------------------------- #
# Vocabulary (fixed by the schema CHECK constraints + the ratified adjudication)#
# --------------------------------------------------------------------------- #
WORK_LEDGER_TABLE = "okg.work_ledger"

MODE_DIFFERENTIAL = "differential"
MODE_COMPLETE_REFERENCE = "complete_reference"
MODE_DEPENDENCY_BASELINE = "dependency_baseline"
WORK_MODES: tuple[str, ...] = (
    MODE_DIFFERENTIAL, MODE_COMPLETE_REFERENCE, MODE_DEPENDENCY_BASELINE,
)
_VALIDATION_MODES = frozenset({MODE_COMPLETE_REFERENCE, MODE_DEPENDENCY_BASELINE})

CATEGORY_EFFECT_ANALYSIS = "effect_analysis"
CATEGORY_CERTIFICATE_VALIDATION = "certificate_validation"
CATEGORY_DETERMINISTIC_EXECUTION = "deterministic_execution"
CATEGORY_MODEL_CALLS = "model_calls"
CATEGORY_PROPAGATION = "propagation"
CATEGORY_REPAIR_SOLVING = "repair_solving"
#: The six adjudicated categories, in a fixed order (a ledger for a
#: (deployment, generation, mode) MUST carry exactly this set — the report's
#: loud-break check keys on it).
WORK_CATEGORIES: tuple[str, ...] = (
    CATEGORY_EFFECT_ANALYSIS,
    CATEGORY_CERTIFICATE_VALIDATION,
    CATEGORY_DETERMINISTIC_EXECUTION,
    CATEGORY_MODEL_CALLS,
    CATEGORY_PROPAGATION,
    CATEGORY_REPAIR_SOLVING,
)
_WORK_CATEGORY_SET = frozenset(WORK_CATEGORIES)


def _is_validation_mode(mode: str) -> bool:
    return mode in _VALIDATION_MODES


# --------------------------------------------------------------------------- #
# The PRICED dimension (D8 / F1.2) — a deployment-configured unit price.        #
# --------------------------------------------------------------------------- #
#: ``model_calls`` carries MEASURED dollars joined from ``okg.llm_calls``; it is
#: NEVER unit-priced. The remaining (deterministic) categories carry a
#: deployment-declared USD-per-work-unit price WHERE one is configured — and a
#: LOUD NULL where none is (never a silent zero, never a fabricated default: the
#: P0 rule, spec §9.9).
_PRICEABLE_CATEGORIES = frozenset(WORK_CATEGORIES) - {CATEGORY_MODEL_CALLS}


@dataclass(frozen=True)
class UnitPricing:
    """Deployment-configured USD unit prices for the deterministic categories.

    ``price_for(category)`` returns the declared USD-per-work-unit price, or
    ``None`` when the deployment declared none — the loud-null basis the report
    renders as an *unpriced* category, kept distinct from a category priced at
    ``$0.00``. ``model_calls`` is never priced here: it carries measured dollars
    joined from ``okg.llm_calls``, not a unit price."""

    per_unit_usd: Mapping[str, float] = field(default_factory=dict)

    @classmethod
    def empty(cls) -> "UnitPricing":
        """No declared prices — every deterministic category is unpriced (a loud
        null). The safe default: a missing config never fabricates a price."""
        return cls(per_unit_usd={})

    @classmethod
    def from_config(cls, config: Mapping[str, Any] | None) -> "UnitPricing":
        """Build from a deployment-config ``{category: usd_per_unit}`` mapping.

        This is the plumbing surface for ledger config (e.g. a
        ``work_ledger.unit_prices`` block on the deployment manifest, read the
        same way :func:`equivalence_series._deployment_name` reads
        ``deployment.yaml``). LOUD on a bad declaration — an unknown /
        non-priceable category or a negative price raises rather than being
        silently dropped (a silently-dropped price is exactly the fabricated
        default the P0 rule forbids)."""
        if not config:
            return cls.empty()
        prices: dict[str, float] = {}
        for category, value in config.items():
            if category not in _PRICEABLE_CATEGORIES:
                raise SubstrateError(
                    "work_ledger unit price declared for a category that is not "
                    "unit-priceable (model_calls carries measured dollars; only "
                    "the deterministic categories take a unit price)",
                    data={"category": category,
                          "priceable": sorted(_PRICEABLE_CATEGORIES)},
                )
            try:
                price = float(value)
            except (TypeError, ValueError) as exc:
                raise SubstrateError(
                    "work_ledger unit price must be a number",
                    data={"category": category, "value": value},
                ) from exc
            if price < 0.0:
                raise SubstrateError(
                    "work_ledger unit price must be non-negative",
                    data={"category": category, "value": price},
                )
            prices[category] = price
        return cls(per_unit_usd=dict(prices))

    def price_for(self, category: str) -> float | None:
        """The declared USD-per-work-unit price for ``category``, or ``None`` —
        no price basis, a loud null, never a fabricated default."""
        return self.per_unit_usd.get(category)


# --------------------------------------------------------------------------- #
# Model-call spend join (the llm_call_stats idiom, all-caller, per generation) #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ModelSpend:
    """The model-call spend a generation charged, joined from ``okg.llm_calls``.

    An honest zero (the join ran, found no calls) is distinct from a GAP (the
    join never ran): :func:`model_call_spend` always returns a ModelSpend, so the
    model_calls category is always *measured*.

    ``cost_usd`` is ``None`` -- the ledger's LOUD NULL -- when any call has no
    price (issue #2573 slice 2, closes #1739); ``unpriced_calls`` counts them
    and ``priced_cost_usd`` keeps the priced part. A token total is ``None``
    the same way when any call did not record it."""

    calls: int
    prompt_tokens: int | None
    completion_tokens: int | None
    total_tokens: int | None
    cost_usd: float | None
    unpriced_calls: int = 0
    priced_cost_usd: float = 0.0
    tokens_missing: Mapping[str, int] = field(default_factory=dict)

    def as_measure(self) -> "CategoryMeasure":
        detail: dict[str, Any] = {"source": "okg.llm_calls join (success = TRUE)"}
        if self.unpriced_calls:
            detail["unpriced_calls"] = self.unpriced_calls
            detail["priced_cost_usd"] = self.priced_cost_usd
            detail["cost_unknown_reason"] = (
                f"unpriced_calls:{self.unpriced_calls}_of_{self.calls}"
            )
        missing = {k: v for k, v in self.tokens_missing.items() if v}
        if missing:
            detail["tokens_missing"] = missing
        return CategoryMeasure(
            measured=True,
            work_units=self.calls,
            model_calls=self.calls,
            prompt_tokens=self.prompt_tokens,
            completion_tokens=self.completion_tokens,
            total_tokens=self.total_tokens,
            cost_usd=self.cost_usd,
            detail=detail,
        )


def model_call_spend(
    conn: Any, *, deployment: str, generation_id: int | str
) -> ModelSpend:
    """Aggregate ``okg.llm_calls`` for one generation (the ``llm_call_stats``
    idiom, all-caller).

    Mirrors ``catalog/invariants_llm.py::_load_llm_call_stats`` but without the
    ``caller LIKE 'invariants:%'`` predicate — every successful model call the
    generation charged. Both sides of the join are now the TEXT generation ref
    (D2), so the predicate executes CAST-FREE: ``work_ledger.generation_id`` and
    ``llm_calls.generation_id`` are the same type and the same value. Before
    this, the bigint cast dropped every Chronos checkpoint ref on the floor and
    the join returned an all-zero spend that read as "this generation charged
    nothing". Returns an all-zero ModelSpend when the generation genuinely
    charged nothing (a measured zero, never a gap). Reads through the shared
    ``llm_usage_summary`` so an unpriced call is never summed as $0."""
    from okg.substrate.telemetry.llm_usage import (
        LlmUsageFilters,
        llm_usage_summary,
    )
    from okg.substrate.unknown import Unknown

    with conn.cursor() as cur:
        (usage,) = llm_usage_summary(
            cur,
            filters=LlmUsageFilters(
                deployment=deployment, generation_id=str(generation_id)
            ),
        )

    def known(value: Any) -> Any:
        return None if isinstance(value, Unknown) else value

    return ModelSpend(
        calls=usage.calls,
        prompt_tokens=known(usage.tokens("prompt_tokens")),
        completion_tokens=known(usage.tokens("completion_tokens")),
        total_tokens=known(usage.tokens("total_tokens")),
        cost_usd=known(usage.cost_usd),
        unpriced_calls=usage.unpriced_calls,
        priced_cost_usd=usage.priced_cost_usd,
        tokens_missing=dict(usage.missing_tokens),
    )


def repair_solved_count(
    conn: Any, *, deployment: str, generation_id: int | str
) -> int:
    """Count repairs FRESHLY solved this generation (``okg.repair_selections``).

    ``carried_from_generation IS NULL`` is the freshly-selected set (a full
    recompute solves every bucket; a differential incremental only the dirty
    ones), so the count is a comparable repair-solving measure across modes.
    ``repair_selections.generation_id`` is TEXT (the compat token) — no cast."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)
              FROM okg.repair_selections
             WHERE deployment_name = %s
               AND generation_id = %s
               AND carried_from_generation IS NULL
            """,
            (deployment, str(generation_id)),
        )
        row = cur.fetchone()
    return int(row[0]) if row is not None else 0


# --------------------------------------------------------------------------- #
# One category measure — a value, or an explicit LOUD GAP.                      #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class CategoryMeasure:
    """A charged-work measure for one category, or an explicit LOUD GAP.

    ``measured = True`` carries a ``work_units`` count (``>= 0`` — an honest zero
    is allowed). ``measured = False`` is the loud gap: NULL units, recorded so the
    category is visibly present-but-uninstrumented, never a silent zero. The token
    / cost fields are meaningful only for the ``model_calls`` category."""

    measured: bool
    work_units: int | None = None
    model_calls: int | None = None
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    total_tokens: int | None = None
    cost_usd: float | None = None
    detail: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.measured and self.work_units is None:
            raise SubstrateError(
                "a measured CategoryMeasure must carry a work_units count "
                "(an honest zero is allowed; a gap uses CategoryMeasure.gap)",
            )
        if not self.measured and self.work_units is not None:
            raise SubstrateError(
                "a GAP CategoryMeasure must carry NULL work_units — never a "
                "silent zero (spec §9.9)",
                data={"work_units": self.work_units},
            )

    @classmethod
    def of(cls, work_units: int, **detail: Any) -> "CategoryMeasure":
        """A measured value (honest zero allowed)."""
        return cls(measured=True, work_units=int(work_units), detail=dict(detail))

    @classmethod
    def gap(cls, reason: str) -> "CategoryMeasure":
        """A LOUD GAP: the run did not instrument this category."""
        return cls(measured=False, work_units=None, detail={"gap_reason": reason})

    def with_unit_price(self, unit_price: float | None) -> "CategoryMeasure":
        """Return a copy carrying a deployment-priced dollar cost
        (``unit_price * work_units``) when a price basis is declared.

        Returns SELF unchanged when there is nothing to price: ``unit_price is
        None`` (the deployment declared no price for this category — its
        ``cost_usd`` stays NULL, a LOUD NULL, never a silent zero), a GAP
        (nothing measured to price), or a measure that already carries measured
        dollars (``model_calls`` — its cost is joined from ``okg.llm_calls``,
        not unit-priced). A declared ``$0.00`` price yields ``cost_usd == 0.0``
        — a real price basis, distinct from an unpriced NULL. A model_calls
        measure whose joined cost is unknown (``unpriced_calls`` in its
        detail) also stays NULL: a unit price would replace a measured-but-
        unpriced cost with an estimate that reads as measured."""
        if (
            unit_price is None
            or not self.measured
            or self.work_units is None
            or self.cost_usd is not None
            or self.detail.get("unpriced_calls")
        ):
            return self
        return replace(self, cost_usd=float(unit_price) * float(self.work_units))


# --------------------------------------------------------------------------- #
# One ledger row (mirrors the okg.work_ledger schema + its CHECK invariants).   #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class WorkLedgerRow:
    """One ``okg.work_ledger`` row. The constructor enforces every schema CHECK
    so a malformed row cannot be built (and thus cannot be stored)."""

    deployment: str
    generation_id: str
    mode: str
    category: str
    validation: bool
    measured: bool
    work_units: int | None = None
    model_calls: int | None = None
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    total_tokens: int | None = None
    cost_usd: float | None = None
    cause_counts: dict[str, int] | None = None
    detail: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.mode not in WORK_MODES:
            raise SubstrateError(
                f"work_ledger mode must be one of {list(WORK_MODES)}",
                data={"got": self.mode},
            )
        if self.category not in _WORK_CATEGORY_SET:
            raise SubstrateError(
                f"work_ledger category must be one of {list(WORK_CATEGORIES)}",
                data={"got": self.category},
            )
        if self.validation != _is_validation_mode(self.mode):
            raise SubstrateError(
                "work_ledger validation must be True iff the mode is a "
                "from-scratch validation run (structural, mirrors the DB CHECK)",
                data={"mode": self.mode, "validation": self.validation},
            )
        if self.measured != (self.work_units is not None):
            raise SubstrateError(
                "work_ledger measured must be True iff work_units is not NULL — "
                "a LOUD GAP is measured=FALSE + NULL units, never a silent zero "
                "(spec §9.9)",
                data={"measured": self.measured, "work_units": self.work_units},
            )

    @classmethod
    def build(
        cls,
        *,
        deployment: str,
        generation_id: int | str,
        mode: str,
        category: str,
        measure: CategoryMeasure,
        cause_counts: dict[str, int] | None = None,
    ) -> "WorkLedgerRow":
        """Assemble a row from a :class:`CategoryMeasure` (validation derived from
        the mode)."""
        return cls(
            deployment=deployment,
            generation_id=str(generation_id),
            mode=mode,
            category=category,
            validation=_is_validation_mode(mode),
            measured=measure.measured,
            work_units=measure.work_units,
            model_calls=measure.model_calls,
            prompt_tokens=measure.prompt_tokens,
            completion_tokens=measure.completion_tokens,
            total_tokens=measure.total_tokens,
            cost_usd=measure.cost_usd,
            cause_counts=cause_counts,
            detail=dict(measure.detail),
        )


# --------------------------------------------------------------------------- #
# Derivation-summary work extraction (shared by both builders).                #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class _DerivationWork:
    present: bool
    rows_written: int
    executed: int
    full_reasons: dict[str, int]


def _extract_derivation_work(
    summaries: Iterable[tuple[str | None, str | None, int | None, str | None]],
) -> _DerivationWork:
    """Reduce ``(status, mode, rows_written, full_reason)`` tuples to the shared
    per-cycle work facts: total rows materialized, executed count, and a
    ``{full_reason: count}`` histogram (for the taxonomy)."""
    rows_written = 0
    executed = 0
    full_reasons: dict[str, int] = {}
    seen = False
    for status, _mode, written, full_reason in summaries:
        seen = True
        if written is not None:
            rows_written += int(written)
        if status == "maintained":
            executed += 1
        if full_reason:
            full_reasons[full_reason] = full_reasons.get(full_reason, 0) + 1
    return _DerivationWork(
        present=seen, rows_written=rows_written,
        executed=executed, full_reasons=full_reasons,
    )


def _cause_counts_for(
    *, reuse_pass_result: Any | None, full_reasons: dict[str, int],
) -> dict[str, int] | None:
    """Fold the reuse-pass causes + the derivation ``full_reason`` values into
    one closed-taxonomy histogram (:func:`classify_distribution`).

    Returns ``None`` when there are no scheduling reasons at all (a clean
    incremental cycle with no full recompute and no recorded pass) so an empty
    distribution is not fabricated."""
    reasons: dict[str, int] = dict(full_reasons)
    if reuse_pass_result is not None:
        for cause, count in reuse_pass_result.cause_counts().items():
            reasons[cause] = reasons.get(cause, 0) + int(count)
    if not reasons:
        return None
    return classify_distribution(reasons)


# --------------------------------------------------------------------------- #
# Builder — the DIFFERENTIAL (production) row set (from a publish-cycle output).#
# --------------------------------------------------------------------------- #
def build_rows_from_cycle_output(
    *,
    deployment: str,
    generation_id: int | str,
    cycle_output: Mapping[str, Any],
    model_spend: ModelSpend,
    repair_solved: int | None = None,
    reuse_pass_result: Any | None = None,
    pricing: UnitPricing | None = None,
) -> list[WorkLedgerRow]:
    """Build the six DIFFERENTIAL-mode rows from a publish-cycle output dict.

    The generic publisher path records what its cycle output already carries; the
    fixture / scheduler path additionally passes the reuse pass' ``ReusePassResult``
    (duck-typed: ``.outcomes``, ``.skips``, ``.cause_counts()``) so effect analysis
    and certificate validation are measured rather than a gap.

    * ``effect_analysis`` / ``certificate_validation`` — measured from the reuse
      pass (steps analyzed / skips); a differential cycle with NO recorded pass
      renders each as a LOUD GAP (never a silent zero).
    * ``deterministic_execution`` / ``propagation`` — from ``maintained_derivations``;
      a no-op cycle that carries none renders them as GAPs.
    * ``model_calls`` — the :class:`ModelSpend` join (always measured).
    * ``repair_solving`` — ``repair_solved`` (freshly solved this generation); a
      caller that did not read ``okg.repair_selections`` passes ``None`` → GAP.

    ``pricing`` (default: no declared prices) attaches a deployment-configured
    dollar cost to each deterministic category where a price basis is declared;
    ``model_calls`` keeps its measured dollars. An unpriced category records a
    LOUD NULL cost, never a fabricated default (D8 / F1.2).
    """
    pricing = pricing or UnitPricing.empty()
    md = cycle_output.get("maintained_derivations")
    # Presence (the key exists), not emptiness, decides measured-vs-gap: an empty
    # maintained_derivations dict is an honest measured zero (we looked, the
    # deployment registered none); an ABSENT key (a no-op / reuse cycle that never
    # carried the summary) is a LOUD GAP.
    md_present = isinstance(md, Mapping)
    if md_present:
        work = _extract_derivation_work(
            (
                (s.get("status"), s.get("mode"),
                 s.get("rows_written"), s.get("full_reason"))
                for s in (v or {} for v in md.values())
            )
        )
    else:
        work = _DerivationWork(False, 0, 0, {})

    if reuse_pass_result is not None:
        effect = CategoryMeasure.of(
            len(reuse_pass_result.outcomes), source="reuse_pass.outcomes",
        )
        cert = CategoryMeasure.of(
            len(reuse_pass_result.skips), source="reuse_pass.skips",
        )
    else:
        effect = CategoryMeasure.gap(
            "no reuse pass recorded for this differential cycle "
            "(effect analysis uninstrumented)",
        )
        cert = CategoryMeasure.gap(
            "no reuse pass recorded for this differential cycle "
            "(certificate validation uninstrumented)",
        )

    if md_present:
        det = CategoryMeasure.of(
            work.rows_written, source="Σ maintained_derivations.rows_written",
        )
        prop = CategoryMeasure.of(
            work.executed, source="maintained derivations executed this cycle",
        )
    else:
        det = CategoryMeasure.gap(
            "cycle carried no maintained_derivations (no-op / reuse cycle)",
        )
        prop = CategoryMeasure.gap(
            "cycle carried no maintained_derivations (no-op / reuse cycle)",
        )

    repair = (
        CategoryMeasure.of(int(repair_solved), source="okg.repair_selections")
        if repair_solved is not None
        else CategoryMeasure.gap("repair_selections not read for this cycle")
    )

    cause_counts = _cause_counts_for(
        reuse_pass_result=reuse_pass_result, full_reasons=work.full_reasons,
    )
    return _assemble(
        deployment=deployment, generation_id=generation_id,
        mode=MODE_DIFFERENTIAL,
        measures={
            CATEGORY_EFFECT_ANALYSIS: effect,
            CATEGORY_CERTIFICATE_VALIDATION: cert,
            CATEGORY_DETERMINISTIC_EXECUTION: det,
            CATEGORY_MODEL_CALLS: model_spend.as_measure(),
            CATEGORY_PROPAGATION: prop,
            CATEGORY_REPAIR_SOLVING: repair,
        },
        cause_counts=cause_counts,
        pricing=pricing,
    )


# --------------------------------------------------------------------------- #
# Builder — a VALIDATION run's row set (complete reference / dependency baseline)#
# --------------------------------------------------------------------------- #
def build_rows_from_reference_result(
    *,
    deployment: str,
    anchor_generation_id: int | str,
    result: Any,
    model_spend: ModelSpend,
    repair_solved: int,
    pricing: UnitPricing | None = None,
) -> list[WorkLedgerRow]:
    """Build the six rows for a from-scratch VALIDATION run (its ``result.mode``).

    ``result`` is a ``CompleteReferenceResult`` (duck-typed: ``.mode``,
    ``.reuse_disabled``, ``.derivation_modes`` of ``StepMode``). The row is keyed to
    the update's ``anchor_generation_id`` (the production generation it is the
    counterfactual FOR) so two updates' reference runs never collide; the run's own
    scratch generation is recorded in ``detail``.

    ``effect_analysis`` / ``certificate_validation`` are a STRUCTURAL ZERO (not a
    gap): a from-scratch validation run executes the whole pipeline and runs NO
    reuse scheduler, so it performs no scheduling analysis and validates no
    certificate — a measured, provable zero the savings report charges to the
    differential column, not this one. (The dependency baseline additionally sets
    ``reuse_disabled = True``; the complete reference does not, but neither runs the
    scheduler — the structural zero follows from the mode, not the flag.)

    ``pricing`` prices the deterministic categories identically to the
    differential run (same deployment config across all three modes), so the
    per-category dollar comparison is apples-to-apples; ``model_calls`` keeps its
    measured dollars and any unpriced category records a LOUD NULL cost."""
    pricing = pricing or UnitPricing.empty()
    mode = result.mode
    if mode not in _VALIDATION_MODES:
        raise SubstrateError(
            "build_rows_from_reference_result expects a from-scratch validation "
            f"result.mode in {sorted(_VALIDATION_MODES)}",
            data={"got": mode},
        )
    scratch_gen = getattr(result, "compat_generation_id", None)
    work = _extract_derivation_work(
        (
            (sm.status, sm.mode, sm.rows_written, sm.full_reason)
            for sm in result.derivation_modes.values()
        )
    )
    structural = {
        "from_scratch_validation": True,
        "reuse_disabled": bool(getattr(result, "reuse_disabled", False)),
        "scratch_generation": str(scratch_gen),
    }
    cause_counts = _cause_counts_for(
        reuse_pass_result=None, full_reasons=work.full_reasons,
    )
    return _assemble(
        deployment=deployment, generation_id=anchor_generation_id, mode=mode,
        measures={
            CATEGORY_EFFECT_ANALYSIS: CategoryMeasure(
                measured=True, work_units=0,
                detail={**structural,
                        "structural_zero": "from-scratch validation run — no "
                                           "reuse scheduler, no effect analysis"},
            ),
            CATEGORY_CERTIFICATE_VALIDATION: CategoryMeasure(
                measured=True, work_units=0,
                detail={**structural,
                        "structural_zero": "from-scratch validation run — no "
                                           "reuse scheduler, no certificates"},
            ),
            CATEGORY_DETERMINISTIC_EXECUTION: CategoryMeasure(
                measured=True, work_units=work.rows_written,
                detail={**structural,
                        "source": "Σ derivation_modes.rows_written"},
            ),
            CATEGORY_MODEL_CALLS: model_spend.as_measure(),
            CATEGORY_PROPAGATION: CategoryMeasure(
                measured=True, work_units=work.executed,
                detail={**structural,
                        "source": "derivation_modes executed"},
            ),
            CATEGORY_REPAIR_SOLVING: CategoryMeasure(
                measured=True, work_units=int(repair_solved),
                detail={**structural, "source": "okg.repair_selections"},
            ),
        },
        cause_counts=cause_counts,
        pricing=pricing,
    )


def _assemble(
    *,
    deployment: str,
    generation_id: int | str,
    mode: str,
    measures: Mapping[str, CategoryMeasure],
    cause_counts: dict[str, int] | None,
    pricing: UnitPricing,
) -> list[WorkLedgerRow]:
    """Assemble the six category rows (cause_counts rides on effect_analysis).

    Each measure is priced from ``pricing`` where a price basis is declared;
    ``with_unit_price`` leaves ``model_calls`` (measured dollars) and any
    unpriced / gap category untouched (a loud-null cost, never a fabricated
    default)."""
    missing = _WORK_CATEGORY_SET - set(measures)
    if missing:  # pragma: no cover - builders always supply all six
        raise SubstrateError(
            "a ledger row set must cover every adjudicated category",
            data={"missing": sorted(missing)},
        )
    return [
        WorkLedgerRow.build(
            deployment=deployment, generation_id=generation_id, mode=mode,
            category=category,
            measure=measures[category].with_unit_price(pricing.price_for(category)),
            cause_counts=(
                cause_counts if category == CATEGORY_EFFECT_ANALYSIS else None
            ),
        )
        for category in WORK_CATEGORIES
    ]


# --------------------------------------------------------------------------- #
# Storage + query.                                                             #
# --------------------------------------------------------------------------- #
def record_work_ledger(conn: Any, rows: Sequence[WorkLedgerRow]) -> int:
    """Persist ledger rows into ``okg.work_ledger``.

    Delete-then-insert per ``(deployment_name, generation_id, mode)`` in one
    transaction (the ``store_equivalence_verdicts`` idiom): re-running a
    generation's mode replaces its own rows and nothing else. Returns the count
    written."""
    rows = list(rows)
    if not rows:
        return 0
    groups: dict[tuple[str, str, str], list[WorkLedgerRow]] = {}
    for row in rows:
        groups.setdefault(
            (row.deployment, row.generation_id, row.mode), []
        ).append(row)
    written = 0
    with conn.transaction(), conn.cursor() as cur:
        for (deployment, generation_id, mode), group in groups.items():
            cur.execute(
                f"DELETE FROM {WORK_LEDGER_TABLE} "
                "WHERE deployment_name = %s AND generation_id = %s "
                "AND mode = %s",
                (deployment, generation_id, mode),
            )
            for row in group:
                cur.execute(
                    f"INSERT INTO {WORK_LEDGER_TABLE} "
                    "(deployment_name, generation_id, mode, category, "
                    " validation, "
                    " measured, work_units, model_calls, prompt_tokens, "
                    " completion_tokens, total_tokens, cost_usd, cause_counts, "
                    " detail) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                    " %s::jsonb, %s::jsonb)",
                    (
                        row.deployment, row.generation_id, row.mode,
                        row.category, row.validation, row.measured,
                        row.work_units, row.model_calls, row.prompt_tokens,
                        row.completion_tokens, row.total_tokens, row.cost_usd,
                        json.dumps(row.cause_counts)
                        if row.cause_counts is not None else None,
                        json.dumps(row.detail),
                    ),
                )
                written += 1
    return written


def load_work_ledger(
    conn: Any, deployment: str, *, generation_id: int | str | None = None,
) -> list[WorkLedgerRow]:
    """Load ledger rows for a deployment (optionally one generation), ordered."""
    params: list[Any] = [deployment]
    where = "deployment_name = %s"
    if generation_id is not None:
        where += " AND generation_id = %s"
        params.append(str(generation_id))
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT deployment_name, generation_id, mode, category, validation,
                   measured, work_units, model_calls, prompt_tokens,
                   completion_tokens, total_tokens, cost_usd, cause_counts, detail
              FROM {WORK_LEDGER_TABLE}
             WHERE {where}
             ORDER BY generation_id, mode, category
            """,
            tuple(params),
        )
        rows = cur.fetchall()
    out: list[WorkLedgerRow] = []
    for r in rows:
        out.append(
            WorkLedgerRow(
                deployment=str(r[0]), generation_id=str(r[1]), mode=str(r[2]),
                category=str(r[3]), validation=bool(r[4]), measured=bool(r[5]),
                work_units=int(r[6]) if r[6] is not None else None,
                model_calls=int(r[7]) if r[7] is not None else None,
                prompt_tokens=int(r[8]) if r[8] is not None else None,
                completion_tokens=int(r[9]) if r[9] is not None else None,
                total_tokens=int(r[10]) if r[10] is not None else None,
                cost_usd=float(r[11]) if r[11] is not None else None,
                cause_counts=dict(r[12]) if r[12] is not None else None,
                detail=dict(r[13]) if r[13] is not None else {},
            )
        )
    return out


# --------------------------------------------------------------------------- #
# Per-update cause distribution (for okg metrics --consistency).               #
# --------------------------------------------------------------------------- #
def work_ledger_distribution(
    conn: Any, deployment: str, *, generation_id: int | str | None = None,
) -> dict[str, Any]:
    """The per-update closed-taxonomy cause distribution + a category summary.

    Reads the ``effect_analysis`` rows' ``cause_counts`` (the per-update
    histogram) and a per-(mode, category) measured/gap roll-up. The read surface
    ``okg metrics --consistency`` renders."""
    rows = load_work_ledger(conn, deployment, generation_id=generation_id)
    per_update: list[dict[str, Any]] = []
    aggregate: dict[str, int] = {}
    for row in rows:
        if row.category == CATEGORY_EFFECT_ANALYSIS and row.cause_counts:
            per_update.append({
                "generation_id": row.generation_id,
                "mode": row.mode,
                "validation": row.validation,
                "cause_counts": dict(row.cause_counts),
            })
            for cause, count in row.cause_counts.items():
                aggregate[cause] = aggregate.get(cause, 0) + int(count)
    gaps = [
        {"generation_id": row.generation_id, "mode": row.mode,
         "category": row.category,
         "gap_reason": row.detail.get("gap_reason")}
        for row in rows if not row.measured
    ]
    return {
        "deployment": deployment,
        "generation_id": str(generation_id) if generation_id is not None else None,
        "aggregate_cause_distribution": aggregate,
        "per_update": per_update,
        "gaps": gaps,
        "row_count": len(rows),
    }


# --------------------------------------------------------------------------- #
# The three-mode savings report (renders from ledger rows ALONE).              #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ModeCharge:
    """The charged work one execution mode totalled, per category + overall."""

    mode: str
    validation: bool
    generation_count: int
    per_category_units: dict[str, int | None]  # None == a category with a gap
    per_category_cost: dict[str, float | None]  # None == UNPRICED (a loud null)
    category_gaps: dict[str, int]              # category -> number of gap rows
    total_units: int
    model_calls: int
    total_tokens: int
    cost_usd: float                            # total PRICED dollars (measured + unit-priced)
    cost_priced_complete: bool                 # True iff every category carried a price basis
    cause_distribution: dict[str, int]


@dataclass(frozen=True)
class SavingsReport:
    """The three-mode savings comparison, rendered from ledger rows alone."""

    deployment: str
    generations: tuple[str, ...]
    charges: dict[str, ModeCharge]           # mode -> ModeCharge
    production_total_units: int              # validation=FALSE (differential) only
    production_cost_usd: float                # total PRICED differential dollars
    production_cost_priced_complete: bool     # True iff every production category was priced
    analysis_overhead_units: int             # differential effect+certificate units
    savings_vs_reference_units: int | None
    savings_vs_baseline_units: int | None
    savings_vs_reference_cost: float | None
    savings_vs_baseline_cost: float | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "deployment": self.deployment,
            "generations": list(self.generations),
            "charges": {
                mode: {
                    "mode": c.mode,
                    "validation": c.validation,
                    "generation_count": c.generation_count,
                    "per_category_units": c.per_category_units,
                    "per_category_cost": c.per_category_cost,
                    "category_gaps": c.category_gaps,
                    "total_units": c.total_units,
                    "model_calls": c.model_calls,
                    "total_tokens": c.total_tokens,
                    "cost_usd": c.cost_usd,
                    "cost_priced_complete": c.cost_priced_complete,
                    "cause_distribution": c.cause_distribution,
                }
                for mode, c in self.charges.items()
            },
            "production_total_units": self.production_total_units,
            "production_cost_usd": self.production_cost_usd,
            "production_cost_priced_complete": self.production_cost_priced_complete,
            "analysis_overhead_units": self.analysis_overhead_units,
            "savings_vs_reference_units": self.savings_vs_reference_units,
            "savings_vs_baseline_units": self.savings_vs_baseline_units,
            "savings_vs_reference_cost": self.savings_vs_reference_cost,
            "savings_vs_baseline_cost": self.savings_vs_baseline_cost,
        }

    def render(self) -> str:
        """A compact fixed-width text report for the operator / paper quote."""
        lines: list[str] = []
        lines.append(
            f"Charged maintenance-work savings report — deployment "
            f"{self.deployment!r}"
        )
        lines.append(
            f"  generations: {len(self.generations)}  "
            f"({', '.join(self.generations) if self.generations else 'none'})"
        )
        lines.append("")
        header = (
            f"  {'mode':<22}{'val':>4}{'units':>12}"
            f"{'model_calls':>13}{'tokens':>12}{'cost_usd':>14}"
        )
        lines.append(header)
        lines.append("  " + "-" * (len(header) - 2))
        for mode in WORK_MODES:
            charge = self.charges.get(mode)
            if charge is None:
                lines.append(f"  {mode:<22}{'—':>4}{'(no rows)':>12}")
                continue
            # A partially-priced mode's dollar sum is marked, so the summary
            # cell can never read as a complete $0.00 (the detailed priced
            # block below carries the per-category loud nulls).
            cost_cell = f"{charge.cost_usd:.6f}" + (
                "" if charge.cost_priced_complete else "*"
            )
            lines.append(
                f"  {charge.mode:<22}"
                f"{('Y' if charge.validation else 'N'):>4}"
                f"{charge.total_units:>12,}"
                f"{charge.model_calls:>13,}"
                f"{charge.total_tokens:>12,}"
                f"{cost_cell:>14}"
            )
        lines.append("  " + "-" * (len(header) - 2))
        lines.append(
            "  (* = priced column incomplete: one or more categories in that "
            "mode are unpriced)"
        )
        lines.append("")
        lines.append("  per-category charged units (— = a loud instrumentation gap):")
        cat_header = "    " + f"{'category':<26}" + "".join(
            f"{mode.split('_')[0][:11]:>13}" for mode in WORK_MODES
        )
        lines.append(cat_header)
        for category in WORK_CATEGORIES:
            cells = []
            for mode in WORK_MODES:
                charge = self.charges.get(mode)
                if charge is None:
                    cells.append(f"{'·':>13}")
                    continue
                value = charge.per_category_units.get(category)
                cells.append(f"{('—' if value is None else f'{value:,}'):>13}")
            lines.append("    " + f"{category:<26}" + "".join(cells))
        lines.append("")
        lines.append(
            "  per-category priced dollars (unpriced = no deployment price basis,"
        )
        lines.append(
            "  a loud null distinct from a category priced at $0.00):"
        )
        lines.append(cat_header)
        for category in WORK_CATEGORIES:
            cells = []
            for mode in WORK_MODES:
                charge = self.charges.get(mode)
                if charge is None:
                    cells.append(f"{'·':>13}")
                    continue
                cost = charge.per_category_cost.get(category)
                cells.append(
                    f"{('unpriced' if cost is None else f'${cost:,.4f}'):>13}"
                )
            lines.append("    " + f"{category:<26}" + "".join(cells))
        lines.append("")
        lines.append(
            f"  production (differential, validation=FALSE) charged units: "
            f"{self.production_total_units:,}"
        )
        priced_note = (
            "all production categories priced"
            if self.production_cost_priced_complete
            else "INCOMPLETE — one or more production categories are unpriced "
                 "(loud null)"
        )
        lines.append(
            f"  production priced cost_usd: {self.production_cost_usd:.6f}  "
            f"[{priced_note}]"
        )
        lines.append(
            f"  analysis + certificate overhead charged to differential: "
            f"{self.analysis_overhead_units:,} units"
        )
        if self.savings_vs_reference_units is not None:
            lines.append(
                f"  savings vs complete reference:  "
                f"{self.savings_vs_reference_units:+,} units, "
                f"{(self.savings_vs_reference_cost or 0.0):+.6f} cost_usd "
                f"(reference − differential, after paying the overhead above)"
            )
        if self.savings_vs_baseline_units is not None:
            lines.append(
                f"  savings vs dependency baseline: "
                f"{self.savings_vs_baseline_units:+,} units, "
                f"{(self.savings_vs_baseline_cost or 0.0):+.6f} cost_usd "
                f"(baseline − differential)"
            )
        if self.charges.get(MODE_DIFFERENTIAL):
            dist = self.charges[MODE_DIFFERENTIAL].cause_distribution
            if dist:
                lines.append("")
                lines.append("  differential fallback-cause distribution:")
                for cause, count in sorted(
                    dist.items(), key=lambda kv: (-kv[1], kv[0])
                ):
                    lines.append(f"    {cause:<36}{count:>8,}")
        return "\n".join(lines)


def _mode_charge(mode: str, rows: Sequence[WorkLedgerRow]) -> ModeCharge:
    generations = {row.generation_id for row in rows}
    # Loud break: every (generation, mode) group must carry all six categories.
    by_gen: dict[str, set[str]] = {}
    for row in rows:
        by_gen.setdefault(row.generation_id, set()).add(row.category)
    for generation, categories in by_gen.items():
        missing = _WORK_CATEGORY_SET - categories
        if missing:
            raise SubstrateError(
                "savings report cannot render — a ledger (generation, mode) group "
                "is missing an instrumentation row; the report refuses to paper "
                "over the regression (spec §9.9, req.accounting.savings-report)",
                data={"deployment": rows[0].deployment, "generation": generation,
                      "mode": mode, "missing_categories": sorted(missing)},
            )
    per_category: dict[str, int | None] = {}
    per_category_cost: dict[str, float | None] = {}
    category_gaps: dict[str, int] = {}
    total_units = 0
    model_calls = 0
    total_tokens = 0
    cost_usd = 0.0
    cost_priced_complete = True
    cause_distribution: dict[str, int] = {}
    for category in WORK_CATEGORIES:
        cat_rows = [r for r in rows if r.category == category]
        measured = [r for r in cat_rows if r.measured]
        gaps = [r for r in cat_rows if not r.measured]
        if gaps:
            category_gaps[category] = len(gaps)
        per_category[category] = (
            sum(int(r.work_units or 0) for r in measured) if measured else None
        )
        total_units += sum(int(r.work_units or 0) for r in measured)
        # PRICED dimension: a category is priced only when EVERY one of its rows
        # carries a dollar basis (measured AND cost_usd not NULL). A gap or a
        # measured-but-unpriced row makes it a LOUD NULL (None) — distinct from a
        # category priced at $0.00 (all rows present, cost_usd == 0.0). Never a
        # silent zero, never a fabricated default (D8 / F1.2, spec §9.9).
        if cat_rows and all(
            r.measured and r.cost_usd is not None for r in cat_rows
        ):
            cat_cost = sum(float(r.cost_usd) for r in cat_rows)  # type: ignore[arg-type]
            per_category_cost[category] = cat_cost
            cost_usd += cat_cost
        else:
            per_category_cost[category] = None
            cost_priced_complete = False
        if category == CATEGORY_MODEL_CALLS:
            model_calls += sum(int(r.model_calls or 0) for r in measured)
            total_tokens += sum(int(r.total_tokens or 0) for r in measured)
    for row in rows:
        if row.category == CATEGORY_EFFECT_ANALYSIS and row.cause_counts:
            for cause, count in row.cause_counts.items():
                cause_distribution[cause] = (
                    cause_distribution.get(cause, 0) + int(count)
                )
    return ModeCharge(
        mode=mode,
        validation=_is_validation_mode(mode),
        generation_count=len(generations),
        per_category_units=per_category,
        per_category_cost=per_category_cost,
        category_gaps=category_gaps,
        total_units=total_units,
        model_calls=model_calls,
        total_tokens=total_tokens,
        cost_usd=cost_usd,
        cost_priced_complete=cost_priced_complete,
        cause_distribution=cause_distribution,
    )


def render_savings_report(
    conn: Any,
    deployment: str,
    *,
    generations: Sequence[int | str] | None = None,
) -> SavingsReport:
    """Render the three-mode savings comparison from ``okg.work_ledger`` rows ALONE.

    Compares total charged work across complete reference / dependency baseline /
    differential, as sums and per-category distributions, with analysis and
    certificate costs charged to the differential column and validation runs
    (complete reference / dependency baseline) excluded from the *production*
    charged-work figure. The report renders from ledger rows — no hand-assembled
    numbers — so a regression in instrumentation (a deleted row) breaks it loudly
    (:func:`_mode_charge` refuses a group missing a category) rather than papering
    over it (req.accounting.savings-report)."""
    rows = load_work_ledger(conn, deployment)
    if generations is not None:
        wanted = {str(g) for g in generations}
        rows = [r for r in rows if r.generation_id in wanted]
    if not rows:
        raise SubstrateError(
            "savings report has no ledger rows — nothing to render; run the "
            "harness series first so the report has real input",
            data={"deployment": deployment,
                  "generations": (
                      [str(g) for g in generations]
                      if generations is not None else None
                  )},
        )
    by_mode: dict[str, list[WorkLedgerRow]] = {}
    for row in rows:
        by_mode.setdefault(row.mode, []).append(row)
    charges = {mode: _mode_charge(mode, mode_rows)
               for mode, mode_rows in by_mode.items()}

    differential = charges.get(MODE_DIFFERENTIAL)
    reference = charges.get(MODE_COMPLETE_REFERENCE)
    baseline = charges.get(MODE_DEPENDENCY_BASELINE)

    # Production charged work counts VALIDATION = FALSE (differential) only — a
    # from-scratch counterfactual is never counted as production work saved.
    production_total = differential.total_units if differential else 0
    production_cost = differential.cost_usd if differential else 0.0
    analysis_overhead = 0
    if differential:
        for category in (CATEGORY_EFFECT_ANALYSIS, CATEGORY_CERTIFICATE_VALIDATION):
            value = differential.per_category_units.get(category)
            analysis_overhead += int(value or 0)

    def _units_savings(counterfactual: ModeCharge | None) -> int | None:
        if counterfactual is None or differential is None:
            return None
        return counterfactual.total_units - differential.total_units

    def _cost_savings(counterfactual: ModeCharge | None) -> float | None:
        if counterfactual is None or differential is None:
            return None
        return counterfactual.cost_usd - differential.cost_usd

    return SavingsReport(
        deployment=deployment,
        generations=tuple(sorted({r.generation_id for r in rows})),
        charges=charges,
        production_total_units=production_total,
        production_cost_usd=production_cost,
        analysis_overhead_units=analysis_overhead,
        savings_vs_reference_units=_units_savings(reference),
        savings_vs_baseline_units=_units_savings(baseline),
        savings_vs_reference_cost=_cost_savings(reference),
        savings_vs_baseline_cost=_cost_savings(baseline),
        production_cost_priced_complete=(
            differential.cost_priced_complete if differential else True
        ),
    )


__all__ = [
    "WORK_LEDGER_TABLE",
    "MODE_DIFFERENTIAL",
    "MODE_COMPLETE_REFERENCE",
    "MODE_DEPENDENCY_BASELINE",
    "WORK_MODES",
    "WORK_CATEGORIES",
    "CATEGORY_EFFECT_ANALYSIS",
    "CATEGORY_CERTIFICATE_VALIDATION",
    "CATEGORY_DETERMINISTIC_EXECUTION",
    "CATEGORY_MODEL_CALLS",
    "CATEGORY_PROPAGATION",
    "CATEGORY_REPAIR_SOLVING",
    "UnitPricing",
    "ModelSpend",
    "model_call_spend",
    "repair_solved_count",
    "CategoryMeasure",
    "WorkLedgerRow",
    "build_rows_from_cycle_output",
    "build_rows_from_reference_result",
    "record_work_ledger",
    "load_work_ledger",
    "work_ledger_distribution",
    "ModeCharge",
    "SavingsReport",
    "render_savings_report",
]
