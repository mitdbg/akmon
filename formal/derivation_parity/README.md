# Derivation Parity Formal Model

## System

The derivation parity model covers the maintained-derivation storage boundary
introduced for issue #444: a deterministic linear derivation can keep an
incremental materialization while remaining equivalent to a batch recompute over
the sealed base truth. It focuses on the generic maintained-derivation helper and
the effective-attribute interval-table consumer, where a missed close/open row or
corrupted kept value would otherwise make generation-pinned reads drift from the
source snapshot.

## Design Contract

- Safety property: for deterministic linear maintained derivations, the kept
  incremental interval-fold stays equal to an independent batch recompute over
  the accumulated delta stream after every accepted delta; a missed interval
  close (open-without-close) that makes the fold double-count is detected by the
  same invariant via a reachable transition trace.
- Authority boundary: `src/okg/substrate/derived_artifacts/maintained_derivation.py`
  owns the reusable explicit-interval sync/stab helper, while
  `src/okg/substrate/attributes.py` owns the effective-attribute interval
  consumer that decides when that helper is active for a branch.
- Valid runtime paths: full interval backfill, dirty-target delta sync, unchanged
  target no-op carry-forward, target retraction, branch isolation, generation
  ordinal lookup, and interval stab reads for a pinned generation.
- Out of scope: non-linear aggregations, SQL planner cost, PostgreSQL physical
  storage compression, source extraction correctness, Chronos publication
  ordering, and UI/read-surface formatting remain runtime-test and
  published-generation obligations.
- Preconditions/refinement: the model assumes a non-empty finite item set,
  bounded integer deltas, deterministic application of one delta to one item at
  a time, and a branch generation with an assigned ordinal. Refinement is covered
  by `tests/substrate/test_maintained_derivation_interval_storage.py` and
  `tests/substrate/test_deployment_maintained_derivation_consumers.py`.

## Model Inventory

| Model | Config | Purpose |
| --- | --- | --- |
| `DerivationParity.tla` | `DerivationParity.cfg` | Positive safety model: `base` is a batch recompute over the accumulated delta stream, `kept` is an independent incremental interval-fold (close old contribution, open new), and the two stay equal after every delta. |
| `DerivationParity.tla` | `DerivationParityDrift.cfg` | Trace-based drift witness: `BuggyIncrementalMissesClose = TRUE` opens a new interval without closing the old one, so `kept` double-counts the prior contribution and diverges from the recompute after a second delta. `base` and `kept` start equal, so the counterexample is a reachable sequence of steps, not an initial-state mismatch. |

## Transformation Map

| Model | Runtime transformation | Production code | Implementation tests |
| --- | --- | --- | --- |
| `DerivationParity.tla` | A full generation snapshot is folded into explicit branch-ordinal intervals, and the kept interval rows visible at the generation equal the batch snapshot for that generation. | `src/okg/substrate/derived_artifacts/maintained_derivation.py` | `tests/substrate/test_maintained_derivation_interval_storage.py` |
| `DerivationParity.tla` | Dirty-target delta sync closes intervals for changed or retracted targets, opens replacement intervals for changed targets, and leaves unchanged targets carried by the prior open interval. | `src/okg/substrate/derived_artifacts/maintained_derivation.py`, `src/okg/substrate/attributes.py` | `tests/substrate/test_maintained_derivation_interval_storage.py`, `tests/substrate/test_deployment_maintained_derivation_consumers.py` |
| `DerivationParity.tla` | A missed interval close (open-without-close) is treated as drift: the incremental fold double-counts a prior contribution, and the checker must find a multi-step counterexample for the parity invariant rather than passing vacuously or failing only at the initial state. | `src/okg/substrate/derived_artifacts/maintained_derivation.py` | `tests/pact/test_formal_report_producers.py` |

## Invariant Map

| Invariants | What it means | Code/test signal |
| --- | --- | --- |
| `TypeOK` | The modeled item set is non-empty; the accumulated stream and batch base stay within the tight value range, while the kept fold and its open contribution have headroom so a missed-close divergence stays type-valid (letting TLC report the parity invariant rather than a type error swallowing the counterexample). | Strict report producer coverage in `tests/pact/test_formal_report_producers.py`. |
| `IncrementalEqualsBatchForLinearDerivation` | The maintained incremental interval-fold visible to readers equals the batch recompute over the accumulated base after every accepted delta. | Interval stab equivalence, branch isolation, retraction, and idempotency checks in `tests/substrate/test_maintained_derivation_interval_storage.py`. |
| `BaseIsRecompute` | The batch path (`base`) is genuinely a recompute over the accumulated delta stream, not a mirror of `kept`: it always equals `InitialBase + accumulated`. This pins the independence of the two sequences, so parity is not the pre-strengthening `x = x` tautology. Holds in both configs (the bug corrupts only the incremental fold). | Full-backfill batch recompute in `tests/substrate/test_maintained_derivation_interval_storage.py`. |
| `DeterministicLinearSafety` | Type safety, batch-path independence, and batch-vs-incremental parity hold together for the deterministic linear derivation class. | Consumer and interval-storage refinement coverage in `tests/substrate/test_deployment_maintained_derivation_consumers.py` and `tests/substrate/test_maintained_derivation_interval_storage.py`. |
| `DerivationParityDrift.cfg` expected counterexample | A missed interval close (`BuggyIncrementalMissesClose = TRUE`) makes `kept` double-count a prior contribution, so TLC refutes `IncrementalEqualsBatchForLinearDerivation` via a reachable multi-step trace (base and kept start equal). | `tests/pact/test_formal_report_producers.py::test_derivation_parity_report_producer_emits_strict_rows`. |

## Verification Process

Run:

```sh
uv run --extra dev python scripts/check_formal_derivation_parity.py \
  --out .scratch/formal/derivation-parity --timeout-seconds 120
```

The checker runs `DerivationParity.cfg` as a passing safety proof, then runs
`DerivationParityDrift.cfg` as an expected counterexample. It emits
`.scratch/formal/derivation-parity/formal-report.yaml` with strict
`formal_check` rows for `IncrementalEqualsBatchForLinearDerivation` and
`DerivationParityDriftCounterexample`. Change records should attach that report
with `uv run okg pact evidence ingest <change-id> --formal-report
.scratch/formal/derivation-parity/formal-report.yaml --json` when the parity
proof is part of PACT evidence.

## Model Boundary

The model deliberately abstracts the maintained derivation to bounded integer
items and linear deltas. It does not prove arbitrary aggregate associativity,
planner index selection, physical table size, generation publication, or source
adapter correctness. Runtime proof still needs PostgreSQL-backed interval tests,
consumer tests, and published-generation evidence for the deployment using the
maintained derivation.
