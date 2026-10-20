# Concord design-verification program

Ten modules contain checked **general Lean proofs for explicit candidate
kernels**, plus executable failing variants. Three (added 2026-10-01) model the
algorithms of
[`concord-design.tex`](../../../docs/develop/architecture/concord-design.tex)
directly: `extract` (Algorithm 1), `pairlevel` (Algorithm 2; `pairs` is the earlier whole-bucket variant) and `repair`
(Algorithm 3). The other six model related mechanisms over grouped sums. These are design results, not proofs
of production code or the entire Concord rule language. The umbrella is
`concord-design-guarantees`; no approval or merge is implied.

## Results and limits

| Child | General checked guarantee | Modeled interface | Remaining full-Concord connection |
| --- | --- | --- | --- |
| `pairs` (umbrella) | UPDATE with the affected buckets **computed** from removed/added claims and a request index over arrived answers gives, for every bucket, the same rows and requests as the full pair check (`update_exact`); exact list equality when new claims = old minus removed plus added (`update_exact_lists`). Charged work does not depend on unaffected buckets (`cost_local`). The whole stored state (bucket members, unplaced IDs, outputs, a complete and sound request index) stays exact across any number of runs (`update_step`, `update_runs`), assuming unique claim IDs. Proved counterexamples: ignoring arrived answers, refreshing only a moved claim's new bucket, omitting added claims' buckets, an incomplete index, and an undeclared answer read each give a stale result. | Claims + pair rule declaring the requests it reads + answer snapshot → per-bucket outputs. | Rule changes (full pass by design), multi-stage calls, real index storage cost, the link from EXTRACT claim IDs to unique Pairs IDs. |
| `pairlevel` (umbrella) | Pair-level UPDATE, the design's Algorithm 2 (operator decision 2026-10-01): delete removed claims' pairs, test added claims against their bucket (including claims added in the same batch), re-test pairs whose requests arrived. The whole stored state stays exact across runs (`pair_step_exact`, `pair_runs`) and each bucket's view equals the full check (`pair_view_exact`). Tests ≤ added claims' bucket sizes + arrived index entries; one changed claim costs ≤ n (`pair_cost_bound`, `pair_cost_single`). Hypotheses include `RemovedIds` (removed claims are old claims, given unique IDs) and unique new IDs; counterexamples prove both needed. Proved counterexamples: untested same-batch pairs, kept removed pairs, ignored arrivals. | Claims + pair rule + answers → pair table + request index. | Rule changes, multi-stage calls, index edit cost, the EXTRACT-to-unique-ID link. |
| `extract` (umbrella) | Incremental EXTRACT equals a rebuild from scratch (`extract_exact`); removed/added are exactly the claim-set differences (`delta_exact`); one run maps an exact stored state to the exact new state (`extract_step`). Hypotheses: changed covers edited sources, first answer wins, arrived covers new answers, exact pending index. Proved counterexample: ignoring the pending index leaves a stale Pending record. | Source map + answer snapshot + changed sources + arrived requests + pending index → records, delta. | Producer configuration changes (all sources selected); real Chronos difference cost. |
| `repair` (umbrella) | Minimal sets = safe + each rejected claim has a private edge (`minimal_iff_private_edge`); minimality splits over disjoint components (`component_decomposition`); positive-weight minimum sets are minimal, zero weights are not (`min_weight_is_minimal`, `zero_weight_counterexample`); the enumerator is sound and complete in both modes (`repair_sound`, `repair_complete`, `repair_weighted_*`). (`unfinished_unavailable` holds by definition over a Boolean flag; deriving that flag from the hard rules is design only.) | Conflict edges + component + protected IDs + policy → proposals. | Weighted decomposition across components: finite check only (4 claims). General k-ary constraints. |
| `concord-local-work` | **Weaker than it reads:** the touched set is an input, so locality holds by the definition of the cost (`pairs` derives the set for the pair-rule core). A sparse batch's abstract operation cost depends on touched group cells, tracing/index edits, root extraction, reference deduplication, retired answers and retries. Unrelated graph cells change neither output nor charged cost. | Indexed groups + changed groups + primitive operation streams → sparse writes + cost. | Completeness/cost of real indexes and generated operation streams; wider rule language. |
| `concord-delete-move` | Moving an existing value refreshes its actual old/new groups, preserving count and value sum. Deleting the last value removes the finding. Refresh equals full evaluation after either operation. | Indexed numeric-value bags + move/delete → changed bags + replacement findings. | Stable record identities, relational joins, edge deletion and general rule dependencies. |
| `concord-contained-uncertainty` | Every unmarked expression result agrees under every completion of missing answers. Marks compose through arbitrary addition trees and never spread outside extracted dependencies. | Literal/answer/add expression + answer snapshot → result + mark. | Unknown calls deciding group membership/ranking/branch selection; general rule DAGs. |
| `concord-settled-progress` | Given the hypothesis that the target's queue prefix fits the charged envelope (`prefixCharge`, assumed, not derived from the queue), a target's concrete immutable commit event is executed within the FIFO service bound. Its committed group result is current while that entire dependency group stays unchanged, despite unrelated publications. | Pinned grouped graph + charged FIFO event prefix + later events → committed target result. | General dependencies, AI waiting, actual whole-generation commit scheduling. |
| `concord-retry-checkpoint` | Atomic durable steps append the next unique ordinal/evaluated value. Crash/resume keeps effects/progress/identities; retry cannot duplicate ordinals. Only completion advances the checkpoint, whose value equals the journal's own canonical evaluation (defined by the fixed evaluator, not by Concord's full check). | Fixed identity/evaluator + durable journal + step/crash/resume/finish → journal. | External nontransactional effects, database atomicity, provider faults, dynamic job expansion. |
| `concord-answer-cleanup` | Compute references from certified and unfinished expression roots; deduplicate; retain their answers. Cleanup preserves root evaluation **and uncertainty marks**. Retained entries are at most `(certified roots + unfinished roots) × maximum dependencies per root`. | Root expressions + answer store → retained store. | Completeness of roots for full Concord stages, evolving reference registration, response bytes. |

The proof examples correspond to concepts in the pinned Concord W2 specification
(okg PR 2707, head
`29c26122289b1ee9a49bbb6e85fd08f7ed642332`): grouped sum findings, before/after
replacement, model-answer identity, composed dependencies, pinned runs and live
answer retention. They are **not imports or extensions of its broader relational
semantics**. W2b (okg PR 2764) remains the place
for broad language/network and structural read-bound work. This program provides
candidate small mechanisms to integrate, not a second production engine.

## Reproduce every check

From repository root, with Lean `4.34.1` available:

```sh
python3 formal/concord_bounded_maintenance/program/run_checks.py
```

The runner builds Bounds and ten modules into a fresh temporary directory, sets
`LEAN_PATH` for imports, audits **every theorem** for only `propext`/`Quot.sound`,
rejects placeholders/custom axioms, then runs executable controls. It needs no
TLC, Hypothesis package, database, or provider. The Lean arguments are general;
Python comparisons and state exploration are finite counterexample checks.

## What 'incremental' means here

The graph is already indexed by complete group inputs. A run returns a **sparse
output delta**, rather than copying or scanning all existing findings. In the
abstract unit-cost model, the proved batch bound is:

```text
sum over touched groups (number of their input values + 2)
+ trace operations + index-edit operations + root-extraction operations
+ number of answer references squared + number of answer references
+ retired entries + retries + one commit
```

The squared term explicitly permits simple membership-based deduplication; it
must not be silently described as linear. Root extraction, index maintenance,
retirement and retry streams must enumerate **all** corresponding primitive
operations. Their generation/completeness is an interface obligation here,
not established for full Concord. Group lookup and sparse writes are abstract
indexed primitives; Python overlay depth and real map/tree implementation costs
are not covered by the unit-cost theorem.

`run_groups_cost` connects actual recursive sparse evaluation to its read/write
count; `batch_bound` adds the complete declared primitive streams and dedup budget.
`batch_unrelated_independence` proves no unrelated graph-size term appears when
these streams stay the same. A changed hot group can still require graph-wide
work. This establishes a useful restricted abstract bound, **not the full
Concord algorithm's total complexity**.

The helper `maintained_exact` assumes input equality outside touched groups; it
is not a proof that arbitrary tracing is complete. Move/delete proofs instantiate
completeness directly with their actual old/new group edits.

## Bound and identity discipline

Progress inherits the FIFO burst/work-capacity conditions from the parent
[bounded-maintenance contract](../README.md). Every actual publication and every
induced operation must count. Settlement means the **whole dependency group**
stops changing, not merely a subject's own record. Pinned commit events contain
the evaluation of that group's immutable snapshot. Group reads are charged in
the prefix before its final commit marker. The result-log query demonstrates the
target commit occurred; it is not a whole graph checkpoint storage implementation.

Recovery assumes each effect value and its ordinal/progress are atomically
stored in the durable journal. The initial journal must be valid. The evaluator
is a fixed deterministic function of graph/rule/answer identity and ordinal;
changed provider answers or code identity cannot be substituted mid-run. A
stopped worker performs no step; resuming retains the same target. Provider calls
and arbitrary external side effects do not acquire exactly-once guarantees from
this model.

Cleanup roots include all pinned input expressions even if they produce no
finding, plus active and queued work. Roots must be registered before cleanup.
An unlimited growing graph or pending queue permits unlimited retained answers;
the bound is conditional on root counts and expression sizes, and counts entries,
not bytes. Root evaluation and honest missing-answer status both survive cleanup.

## Failing variants

Executed controls demonstrate these concrete failures:

- EXTRACT that ignores the pending-request index leaves a stale Pending record
  after the answer arrives; keeping a removed source's claims leaves stale claims.
- UPDATE that ignores arrived answers, refreshes only a moved claim's new bucket,
  uses an incomplete request index, or runs a test that reads an unreported
  answer leaves a stale bucket output.
- REPAIR with a zero weight returns a non-minimal rejection set; enumerating one
  component misses choices; treating a Pending pair as no conflict proposes an
  unsafe acceptance (one hand-built case).
- A mutant whole-graph read loop makes charged work grow with unrelated padding.
- Ignoring retry work undercounts no-change runs.
- Refreshing only a moved row's destination leaves a stale source finding.
- Dropping a dependent expression's missing-answer mark presents a wrong result as exact.
- Resetting pinned targets on arriving publications prevents commit.
- Losing durable progress while keeping an effect duplicates its ordinal on retry.
- Changing the answer-snapshot identity mid-run invalidates the canonical effect ledger.
- Cleaning from certified roots alone discards an unfinished input's answer.

The original queue experiment separately exercises overload, omitted/free
publications, zero service and mixed-version results. These checks demonstrate
needed design rules; they are not claims that production bugs were reproduced.

## Review and next connection

Independent read-only review evaluates every child contract and its actual files.
Earlier review found bad same-group/missing-source movement semantics, a progress
lemma that lacked a commit connection, incomplete cleanup roots, and recovery
without an effect ledger. Those defects were corrected before this result.

The next design work is connecting these kernels to W2's full rule language:
complete dependency-index updates, unknown grouping/ranking, general layered
rule networks, and bounded dynamic costs. Those remain open even when every
child's explicitly restricted proof checks pass. Operator approval remains a
separate PACT decision; this work does not record it automatically.
