# Althea Formal Models

## System

The Althea optimizer model covers the safety kernel for optimizer rewrite
selection and materialization. The runtime subsystem is the pure optimizer
package under `src/okg/optimizer/`, especially candidate generation, beam search,
materialization plans, and the executable checker that maps runtime results to
formal invariants.

The production failure mode is a rewrite candidate becoming visible without
being admitted, selected, beneficial, conflict-free, or additive over the base
graph facts.

## Design Contract

- Safety property: selected candidates must come from the admitted candidate
  universe, materialized candidates must have been selected, selected candidates
  must be eligible and conflict-free, search cost must remain non-negative, and
  materialization must preserve existing node and edge facts.
- Authority boundary: `src/okg/optimizer/rewrite.py` declares the candidate
  universe, `src/okg/optimizer/search.py` owns selection and additive rewrite
  validation, and `src/okg/optimizer/formal_search.py` owns the executable
  refinement check. PACTs and trace records can record evidence, but they do not
  admit or select optimizer candidates.
- Valid runtime paths: bounded candidate generation, beam-search acceptance,
  suppressed candidates, selected candidate materialization, conflict
  suppression, non-beneficial suppression, and additive overlay writes.
- Out of scope: global optimality, learned model calibration, replay-measured
  value, branch promotion policy, storage-level isolation, and deployment
  publish health remain covered by implementation tests and future design work.
- Preconditions/refinement: runtime tests must build concrete
  `RewriteCandidate`, `OptimizationResult`, and `OptimizerGraph` values that
  exercise the same admission, selection, materialization, conflict, eligibility,
  and additive-overlay boundaries.

## Model Inventory

| Model | Config | Purpose |
| --- | --- | --- |
| `AltheaOptimizerSafety.tla` | `AltheaOptimizerSafety.cfg` | Positive safety model for candidate admission, search selection, materialization, conflict exclusion, non-negative cost, and additive overlay preservation. |
| `AltheaOptimizerSafety.tla` | `AltheaOptimizerMutatesBaseCounterexample.cfg` | Buggy-toggle witness: `BuggyOptimizerMutatesBase = TRUE` lets a materialization rewrite a base-graph node attribute, refuting `BaseNodeAttrsPreserved` by name so the preservation invariants are load-bearing rather than guard-restatements (no transition otherwise touches base attrs). The load-bearing production proof remains the executable checker `src/okg/optimizer/formal_search.py`, which diffs real before/after graph attributes. |
| `AltheaOptimizerSafety.tla` | `AltheaOptimizerMutatesBaseEdgeCounterexample.cfg` | The edge twin: the same toggle rewrites a base-graph edge attribute, refuting `BaseEdgeAttrsPreserved` by name. TLC halts at the first violated invariant, so each base-preservation twin needs its own config to be witnessed in isolation. |

## Transformation Map

| Model | Runtime transformation | Production code | Implementation tests |
| --- | --- | --- | --- |
| `AltheaOptimizerSafety.tla` | Rewrite templates declare a bounded admitted candidate universe before search can select candidates. | `src/okg/optimizer/rewrite.py`, `src/okg/optimizer/tier1.py` | `tests/optimizer/test_formal_search.py` |
| `AltheaOptimizerSafety.tla` | Beam search suppresses conflicts and non-beneficial candidates, applies accepted candidates, and records accepted candidate ids. | `src/okg/optimizer/search.py` | `tests/optimizer/test_formal_search.py` |
| `AltheaOptimizerSafety.tla` | The executable checker maps concrete search results to formal invariant names and rejects bypass, conflict, eligibility, materialization, and base-mutation counterexamples. | `src/okg/optimizer/formal_search.py` | `tests/optimizer/test_formal_search.py` |
| `AltheaOptimizerSafety.tla` | Payload summary rewrites materialize an overlay node and edge instead of mutating the original node. | `src/okg/optimizer/tier1.py` | `tests/optimizer/test_formal_search.py` |

## Invariant Map

| Invariants | What it means | Code/test signal |
| --- | --- | --- |
| `TypeOK` | The model stays inside the finite candidate, graph, phase, and cost domains. | Construction and validation paths in `src/okg/optimizer/rewrite.py` and `tests/optimizer/test_formal_search.py`. |
| `SearchCostNonNegative` | Search results cannot report a negative final cost. | `src/okg/optimizer/search.py` clamps accepted state cost and `tests/optimizer/test_formal_search.py` checks the executable refinement path. |
| `SelectedCandidatesAreAdmitted`, `NoAdmissionBypass` | Search selection and materialization cannot name candidates outside the admitted candidate universe. | `src/okg/optimizer/formal_search.py` rejects selected or materialized ids outside the supplied candidate universe. |
| `SelectedCandidatesAreEligible` | Selected candidates must have a beneficial accepted cost delta, a scorer-admitted IR score when one is present, and stay within any configured quality-risk bound. | Non-beneficial, inadmissible-score, and risk-bound cases in `tests/optimizer/test_formal_search.py`. |
| `SearchConflictExclusion` | A selected set cannot contain two candidates with the same conflict token. | Conflict suppression in `src/okg/optimizer/search.py` and conflict counterexamples in `tests/optimizer/test_formal_search.py`. |
| `MaterializationRequiresSearchSelection` | Materialization cannot expose a candidate unless search selected it first. | `src/okg/optimizer/formal_search.py` rejects materialized ids missing from `OptimizationResult.applied_candidates`. |
| `BaseNodeAttrsPreserved`, `BaseEdgeAttrsPreserved`, `AdditiveOverlayPreservesBase` | Existing node ids, node subtypes, node attrs, edge ids, edge endpoints, edge types, and edge attrs are preserved while overlays may add new nodes or edges. | Additive validation in `src/okg/optimizer/search.py`, checker validation in `src/okg/optimizer/formal_search.py`, and payload-overlay tests in `tests/optimizer/test_formal_search.py`. |
| `AltheaOptimizerSafetyKernel` | The full safety kernel holds as one named property for PACT formal evidence. | `scripts/check_formal_althea_optimizer_safety.py` emits the strict `AltheaOptimizerSafetyKernel` formal-report row. |

## Verification Process

Run the TLC model and refinement tests:

```sh
uv run --extra dev python scripts/check_formal_althea_optimizer_safety.py --out .scratch/formal/althea-optimizer-safety
uv run --extra dev pytest tests/optimizer/test_formal_search.py tests/pact/test_formal_report_producers.py::test_althea_optimizer_safety_report_producer_emits_strict_rows tests/docs/test_formal_readmes.py -q
```

The checker uses the pinned TLC jar at `.tools/tla2tools.jar`,
`$TLA2TOOLS_JAR`, or `tools/tla2tools.jar`. Missing Java or TLC is a hard
failure. Artifacts are written under `.scratch/formal/althea-optimizer-safety`
by default, including `formal-report.yaml`.

## Model Boundary

The model does not prove optimizer optimality, learned cost-model quality,
branch promotion, storage isolation, or deployment publishing health. It also
does not decide whether a rewrite family is a good product design. Those
questions require design review, replay measurements, quality labels, and
deployment evidence. This model only proves that the runtime safety kernel
cannot bypass admission, selection, eligibility, conflict exclusion, or
additive overlay preservation.
