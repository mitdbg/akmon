# Concord: candidate bounded maintenance contract

**Checked:** a FIFO work queue has bounded backlog and finishes immutable targets
under an explicit work-arrival envelope. **Not yet proved:** Concord's complete
incremental evaluation algorithm satisfies that envelope or has the proposed
end-to-end work bound. No production behavior changes in this experiment.

This complements the planned W2b follow-ups (okg PR 2764),
head inspected `9f637230f3d1ef8224a5b9fc958e722f31342ae0`. It proposes a snapshot
FIFO option, rather than assuming W2b's live-subject replacement scheduler already
has these properties. That design choice remains unapproved.

## System

Concord is designed to maintain consistency findings over published graph versions. This
folder models one proposed piece of that design: a FIFO queue of immutable
maintenance jobs with a charged-work envelope, which bounds how far the last
certified checkpoint can fall behind. The `program/` subfolder holds the Lean
models of the design document's Algorithms 1-3 and six related mechanisms; its
own README lists them. Concord is not implemented in production code yet, so
every result here is a design result.

## Design Contract

- Safety property: under the stated work-arrival envelope, the backlog after
  each service round stays within `B`, every admitted target finishes within
  `B+R` service rounds, and a certified checkpoint equals a full check of its
  pinned inputs (the last is an evaluator obligation, not proved here).
- Authority boundary: no runtime surface owns this decision yet. Today's
  claim conflicts are computed in full by
  `src/okg/substrate/consistency/claim_model/conflicts.py`; a future Concord
  engine would own incremental maintenance and checkpoint commits.
- Valid runtime paths: none in production. The modeled paths are job
  admission, FIFO service and checkpoint commit. The witnesses under
  "Counterexamples and correction" are assumption violations, not valid paths.
- Out of scope: the Concord rule language, SQL evaluation, provider calls and
  retries, index construction, answer retention, and deployment workload
  numbers; see "Model Boundary".
- Preconditions/refinement: the envelope, positive per-job cost, immutable
  targets, and in-order completion are assumptions of the formal model. No
  implementation tests bind them yet, and `formal/gap-inventory.toml` has no
  entry for them because nothing is implemented; the future Concord engine
  must add refinement tests before any production claim.

## Model Inventory

| Artifact | What it models |
| --- | --- |
| `Bounds.lean` | Lean proofs of the burst invariant, FIFO deadline, and version-lag bound (table under "What is checked"). |
| `check_design.py` | Exhaustive reachable queues in four finite profiles, indexed DAG update agreement, and the counterexample witnesses. |
| `program/` | Lean models of Algorithms 1-3 and six related mechanisms; run by `program/run_checks.py`. |
| `lean-toolchain` | Pins Lean `leanprover/lean4:v4.34.1`. |

## Transformation Map

| Model | Runtime transformation | Production code | Implementation tests |
| --- | --- | --- | --- |
| `Bounds.lean`, `check_design.py` | Proposed: admit each publication as an immutable maintenance job, serve jobs in FIFO order, commit a certified checkpoint when a job finishes. Not implemented. | None yet. Nearest existing code: `src/okg/substrate/consistency/claim_model/conflicts.py` (full, non-incremental conflict computation). | None yet. Nearest existing tests: `tests/substrate/test_claim_model_conflicts.py`. |

## Invariant Map

| Invariant | What it means | Code/test signal |
| --- | --- | --- |
| `burst_invariant`, `all_rounds` | Unfinished work plus unused allowance stays within `B` after every round. | Lean proof in `Bounds.lean`; no runtime signal yet. |
| `fifo_deadline` | The work ahead of a FIFO target drains to zero once `steps * capacity` covers it. | Lean proof in `Bounds.lean`; finite check in `check_design.py`. |
| `admitted_job_deadline` | With service capacity at least 1, an admitted target finishes within `B+R` service rounds. | Lean proof in `Bounds.lean`; no runtime signal yet. |
| `version_lag_bound` | The last certified checkpoint is at most `B` publications behind, assuming each publication is queued once with positive cost and completed in order. | Lean proof in `Bounds.lean`; no runtime signal yet. |
| `exact_commit` | A committed checkpoint keeps the full-check equality it was given. | Assumed equality; evaluator not proved here. |

## The proposed promise

1. A **certified checkpoint** identifies one graph version, one rule version, and
   one model-answer snapshot. Its findings equal a full check of those inputs.
2. New graph publications create immutable, finite maintenance jobs in FIFO order.
   Newer jobs never replace an active target. Partial work stays separate from
   the last certified checkpoint.
3. If **all actual publications** satisfy the charged-work envelope below, and
   the worker delivers its guaranteed service, then after each service round
   the last certified checkpoint is at most **B publications behind**.
4. Every admitted target finishes within **B+R service rounds** when capacity is
   positive. The scalar drain theorem additionally proves any bound D satisfying
   `D*C >= target_prefix_work`, hence `ceil((B+R)/C)` rounds. The Python exploration
   checks that sharper bound for its four profiles.

The checkpoint equality is an evaluator/commit **obligation**, not a new proof of
Concord evaluation correctness. A partially updated result can agree with neither
old nor new graph; do not describe it as 'consistent through an older version'.

## Precise units and assumptions

A round is one guaranteed service opportunity, **not a second**. A work unit is
one finite, bounded charged operation. Every generation costs at least one unit,
including a no-change publication, for its completion/commit bookkeeping.

- `Q`: all unfinished work, including active and queued jobs.
- `T`: unused allowance for arriving work.
- `B`: burst allowance; initially `Q+T <= B` (the arbitrary-round theorem uses
  the empty initial queue and `T=B`; the single-step theorem permits a nonempty
  initial queue satisfying the invariant).
- `R`: allowance added each round.
- `C`: work actually served each round, or all work if fewer than C units remain.
- `A`: total newly arriving work, with `A <= min(B,T+R)`.
- Require `C >= R` for the backlog bound and `C >= 1` for completion.

Each round:

```text
available = min(B, T + R)
append every new generation's complete charged work, of total cost A
serve the oldest C work units (or empty the queue)
T' = available - A
Q' = max(0, Q + A - C)
```

Lean proves `Q'+T' <= B`, by induction for arbitrarily many rounds. Before
service the backlog is at most `B+R`; readers observing during a round have that
weaker bound, not the after-round B bound. Every unfinished generation has at
least one remaining unit, so its count cannot exceed remaining work. With an
ordered complete queue, this count equals the number of publications after the completed checkpoint,
using contiguous publication ordinals rather than Chronos IDs or hashes.

**No source backpressure is implemented or proposed here.** The envelope is an
assumption on the actual stream of publications and all their work. Counting only
admitted versions does not bound lag from the latest graph. An oversized job or
an omitted publication violates the assumption; it cannot be hidden by refusing
admission. The old certified checkpoint remains coherent but its lag guarantee
is lost.

All cost must include change discovery, dependency tracing, rule execution,
answer acquisition, answer retention/cleanup, dependent layers, retries, and
commit. Provider failures cannot block the head indefinitely: retries and waits
must have finite charged bounds, or a separately specified failure outcome must
complete that job. This experiment does not prove such bounds, nor treat an
unanswered call as an exact successful finding. If total job cost is not known
in advance, a conservative certified upper bound is required; arbitrary later
cost expansion is outside this model.

Example: `B=20`, `R=5`, `C=5` gives at most 20 pending versions after service and
at most 25 immediately before service. A queued target needs at most five
service rounds (`ceil(25/5)`) even if new publications continue. These are
illustrative design-operation units, not deployment measurements or seconds.
The general division-free deadline theorem also gives a looser 25-round bound.

## What is checked

| Artifact | Checked claim | Limit |
| --- | --- | --- |
| `burst_invariant`, `all_rounds` | Work backlog bound for arbitrary admitted rounds | Actual total workload must satisfy the envelope |
| `fifo_work_recurrence`, `fifo_burst_invariant` | Concrete append/drop FIFO queue obeys that recurrence | Queue items represent finite charged operations |
| `fifo_prefix_step`, `fifo_prefix_length`, `drain_bound`, `fifo_deadline` | Later appends do not displace an old target; its remaining prefix drains at the service rate | Immutable targets, no preemption/reset, guaranteed service |
| `queue_count_le_work`, `version_lag_bound` | Positive residual job costs convert work bound to pending-version bound | Every publication queued exactly once, completed strictly in order |
| `exact_commit` | Equality obligation is preserved in checkpoint representation | Candidate equality is assumed; evaluator is not proved here |
| `positive_cost` | Ledger charges even a no-op job positively | Accounting identity, not a complexity theorem |
| `check_design.py` | Exhaustive reachable queues in four finite profiles; indexed static additive DAG updates agree with full checks | No general Concord language/SQL/provider theorem |

The concrete tagged queue is flat charged units and append/drop; the scalar
prefix proof is connected to it by prefix decomposition. The file does not prove
a full scheduler state machine with tagged commit events, snapshot storage,
answer retrieval, or a Concord evaluator. Those are further design proofs.

## Verification Process

Lean uses only its standard library, pinned here to `leanprover/lean4:v4.34.1`.
The audited displayed lemmas use only `propext` and/or `Quot.sound`; there are no
placeholder proofs or custom axioms. These checks run by hand; they are not
registered in `scripts/run_formal_suite.py` and are not a CI gate. Run:

```sh
lean formal/concord_bounded_maintenance/Bounds.lean
python3 formal/concord_bounded_maintenance/check_design.py
python3 formal/concord_bounded_maintenance/program/run_checks.py
```

## Model Boundary

The model covers the queue and checkpoint arithmetic only. It does not model a
scheduler state machine, snapshot storage, answer retrieval, or a Concord
evaluator, and it does not prove the incremental work bound that the envelope
assumes. The first section below states what remains open; the second shows
why the assumptions matter.

### Incrementality: still a proof target

The useful target is a bound in changed records, visited dependency edges,
affected evaluation inputs, retained/retried answers, and commit operations.
Unrelated graph size should not appear **when indexes support complete local
enumeration**. A single change with graph-wide dependencies legitimately requires
graph-wide work. Pending retries can cost work even with no graph change.

The toy DAG checker supplies prebuilt adjacency indexes. It counts changed-row
processing, visited edges, affected node/incoming-edge evaluation, and commit.
Across all 192 three-node acyclic cases, incremental results equal a full check;
adding 100 unrelated isolated nodes changes no charged run work. Index construction
and maintenance are deliberately outside that toy measurement. The toy also
copies the entire result array and sorts affected subjects; neither operation
is charged in its selected-operation counter. Its actual Python runtime is
therefore not claimed incremental. These operations must be redesigned or
included for an end-to-end Concord theorem. It has no AI answers, rule
changes, deletions of edges, general networks, or retention algorithm.

**Thus the incrementality requirement is not yet fulfilled.** Next: instantiate
the existing W2b per-rule structural bounds, add dependency-index maintenance,
retention, and retries, and prove the complete bound. Only then derive numeric
workload/freshness parameters for Concord. This experiment establishes the queue
consequence of such a bound, not the bound itself.

### Counterexamples and correction

The checker asserts concrete witnesses for overload (two versions/round, service
one), free publications, omitted publications, target reset, zero service, mixed
version results, missed dependencies, and graph-wide fanout. These show why the
corresponding assumptions matter; they are small executions, not exhaustive
counterexample searches over all variants.

W2b `req.concord-w2b.progress` asks for a newest-first starvation counterexample
with **total** arrivals N below capacity B. That assertion is false for a finite
backlog and work-conserving service if all induced work is counted: B-N residual
capacity drains old work. The checker demonstrates that residual-capacity case.
Starvation needs overload, uncounted induced/retry work, wasted capacity, or
replacing/resetting targets. This does not establish newest-first is the right
policy; it corrects that proposed negative control.
