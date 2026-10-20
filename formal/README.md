# Formal Verification Surface

This tree contains bounded TLA+ models and Python executable formal models for
production OKG behavior. Each direct subdirectory covers one assurance area and
must explain the intended design contract, the system under test, the runtime
transformation being modeled, the source files that implement it, the tests that
exercise the same behavior, the invariants or executable properties checked, and
the verification steps.

The models are intentionally small. They prove structural contracts over the
state machines, admission boundaries, graph transformations, and workflow
authority rules that are easiest to get wrong in code review. They do not
replace runtime tests, published-generation checks, queue/DLQ checks, or doctor
output.

## Folder Index

| Folder | Assurance area |
| --- | --- |
| `admission` | Admission envelopes, source-sync fact submission, idempotency, derived fact visibility, and publish eligibility. |
| `agent_operator` | Runtime approval eligibility, source-sync trigger deduplication, due-backstop authority, and started-work cleanup safety. |
| `althea` | Althea optimizer candidate admission, selection, materialization, conflict exclusion, eligibility, and additive overlay preservation. |
| `attributes` | AKMON attribute observation history, effective attrs projection, external live boundaries, and explicit user-graph promotion. |
| `chronos` | Chronos generation publication, session read pins, graph checkout consistency, and operational SQL compatibility pins. |
| `concord_bounded_maintenance` | Proposed Concord design: FIFO maintenance backlog and checkpoint-lag bounds, plus Lean models of the design's EXTRACT, UPDATE and REPAIR algorithms. Design results only; not implemented and not a CI gate. |
| `consistency` | TransformConsistencyGate dirty-scope soundness for incremental publish validation. |
| `console` | Archive-only historical legacy-console lookup, focus, offline replay, conflict routing, and degradation state machines. |
| `dbos_epoch_supersession` | Bounded DBOS application/epoch binding, stale-body and transaction fencing, recovery uniqueness, and static mutation-boundary inventory coverage. |
| `deployment_authoring` | Deployment authoring command safety for scaffolds, profile explanation, wiring plans, static linting, env diagnostics, and readiness explanation. |
| `deployment_backup_policy` | Rollback-archive validity, full-restore verification, and the narrow routine-dogfood receipt-reuse path that may skip a disposable restore drill. |
| `deployment_control_plane_kernel` | Bounded deployment-attempt authority, effect ordering, activation/publication visibility, crash recovery, rollback rotation, and semantic guard-removal witnesses. |
| `deployment_lifecycle` | Phase-local deployment receipts, dependency-aware invalidation, cancellation, compensation, and clean terminal resource ownership. |
| `deployment_restore_point` | Exact target and verified-restore authority, irreversible provider-effect disclosure, and interruption-safe deployment resume. |
| `deployment_promotion` | Serialized staging-to-production promotion, writer fencing, verified backup, mutation boundaries, readiness, rollback, and fail-closed recovery. |
| `derivation_parity` | Maintained-derivation batch-vs-incremental parity, drift detection, and explicit interval storage refinement. |
| `enricher_backfill` | Targeted enricher backfill selection, branch/chunk safety, retryable chunk progress, edge-scope validation, and progress advancement. |
| `consistency/EnricherBacklogDrain` | Successful-watermark backlog drainage across phase-start snapshots and both generation-reuse guards. |
| `entity_resolution` | Pinned duplicate-node merge refusal, private transaction staging, atomic graph/ledger/receipt commit, rollback, and replay. |
| `external_action_review` | External-effect proposal review, stale-decision fencing, separated approval, claim/outcome uniqueness, expiry, replay ownership, and zero provider calls. |
| `fact_batch_recovery` | FactBatch receipt, graph-effect, progress-marker, compensation, terminal-disposition, and publication ordering. |
| `fleet_capacity_reservation` | Bounded shared-domain capacity admission, reservation crash reconciliation, and activation-bound standing-permit conversion and release. |
| `library_config` | Whole-document library configuration admission, frozen source-adapter bindings, and refusal before construction or side effects. |
| `merge_back` | Worktree fork merge-back replay, provenance restamp, quarantine partition atomicity, and blocked-LAND rollback. |
| `mcp` | Bounded MCP path outcomes, hidden-structure non-disclosure, compact subtype-summary pagination, and context-bound continuations. |
| `nomos` | Policy/configuration admission, bundle activation, request decisions, export visibility, and external-claim enforcement. |
| `pact` | PACT mutation safety, graph projection, formal evidence gating, workflow authority, and v4 decision-record merge safety. |
| `poros` | Cross-OKG exchange, verification, receipts, Nomos decisions, admission, quarantine, promotion, and proof views. |
| `runtime` | Digest-bound deployment apply ordering, stale-plan exclusion, source-registration deduplication, exact worker acknowledgment, and fail-closed readiness. |
| `scholarly` | BibTeX cite-key union, entry-type-specific conference lineage, canonical venue projection, alias safety, and replay idempotence. |
| `source_sync` | Source delta contracts, frontiers, readiness convergence, projection deltas, execution lanes, observability, and dogfood cutover rollback. |
| `source_retirement_liveness` | Digest-bound source retirement, crash-safe application, publication fencing, historical retention, and derived-support safety. |
| `transform` | Writer-path narrowing semantics versus TransformConsistencyGate narrowing semantics. |
| `worktree_branches` | Worktree execution capsules, branch-isolated contributions, recoverable publication, deterministic clean-trunk projection, lifecycle fencing, and capability-checked activation. |
The worktree-branch checker is registered at all four gate-facing surfaces:
the folder index above, `formal/worktree_branches/README.md`'s model inventory,
the `CHECKERS` registry in `scripts/run_formal_suite.py`, and the report-schema
inventory in `tests/pact/test_formal_report_producers.py`. Keep all four in sync
when adding another formal model.

Generated TLC state directories such as `states/` and fixture data such as
`fixtures/` are support artifacts. The folder-level README contract applies to
the direct model folders listed above.

## Standard Folder Format

Every direct model folder uses the same sections:

| Section | Purpose |
| --- | --- |
| `System` | Names the runtime subsystem and the production failure mode the model is meant to catch. |
| `Design Contract` | States the intended safety property, authority boundary, valid runtime paths, out-of-scope behavior, and preconditions/refinement checks used to decide model/code mismatches. |
| `Model Inventory` | Lists each `.tla` module, its `.cfg` files, and the purpose of the model. |
| `Transformation Map` | Maps each model to a runtime state transition, the production code that performs it, and implementation tests that exercise it. |
| `Invariant Map` | Explains each invariant or grouped set of invariants in plain language and names the code/test signal that should fail if it breaks. |
| `Verification Process` | Gives the TLC/checker commands and expected artifacts. |
| `Model Boundary` | States what the model deliberately abstracts away and what must remain covered by live tests or publish evidence. |

Checker wrappers under `scripts/check_formal_*.py` run TLC or Python executable
models and produce reports. They are verification tools; the Transformation and
Invariant maps are the link from model claims to implementation behavior.
Expensive TLC wrappers expose execution profiles: `--profile smoke` is the
bounded CI/operator freshness check, while `--profile full` is the
release/PACT evidence path. Use `--list-profiles --json` on a checker to select
the right config, property set, output directory, and timeout budget. The
full profiles also run on a scheduled CI cadence (`run_formal_suite.py --mode
full`, the `formal-full` lane), so the invariant sets that smoke reduces are
proven on a cadence rather than nowhere — with two named exceptions. The
`admission` and `nomos` full profiles have never completed inside their 900s
TLC cap, so the suite reports them as `UNPROVEN`: a loud line, a junit skip
saying the property set was NOT proven, and no lane red. They are not proven
on any cadence today; issue #1537 tracks restoring them. Every passing fixpoint run must also
clear a distinct-states vacuity floor (`min_distinct_states` on the profile,
conservative default otherwise); a constraint-collapsed state space fails
closed with `failure_class: vacuous_state_space`. Witness runs must violate
exactly the invariant they name — witness configs declare one invariant each,
and the suite has no any-witness fallback. Suite runs emit junit
(`--junit-xml`) so the formal verdict reaches the `test-results-gate`
aggregator. Stale evidence is swept corpus-wide by
`test_formal_evidence_freshness_sweep.py` in the development repository (not in this copy) and repaired with
`scripts/repair_formal_evidence.py`.

### Machine resources a run takes

TLC's own defaults take the whole machine: one worker per core, and — with no
JVM max heap set — a fingerprint set sized from a default heap of a quarter of
physical RAM. A suite run is roughly a hundred such processes, so the untuned
defaults make an interactive host crawl for the duration. Both launch surfaces
(`run_tlc_formal_report` and `run_formal_suite.py`'s direct models) therefore
resolve one shared profile from `okg.substrate.formal_tlc`:

| Knob | Default | Effect |
| --- | --- | --- |
| `OKG_TLC_WORKERS` | `auto` when `CI` is set truthy, else a third of the cores | TLC's `-workers`, plus `-XX:ActiveProcessorCount` so the JVM's GC and JIT pools are capped too (`-workers` alone does not bound them). Set `auto` to restore full parallelism locally. Needs JDK 8u191+; an older JVM refuses to start rather than ignoring the flag, so set `auto` there. |
| `OKG_TLC_HEAP` | `4g` locally, **off under `CI`** | `-Xmx`. Measured on the smoke corpus: peak RSS across a full smoke run drops 9.44 GB → 2.26 GB with the same 45/45 pass set. Set `off` locally if a model outgrows it — it fails loudly as heap exhaustion, never as a weakened proof. |

The profile is deliberately **interactive-only**: under `CI` both knobs resolve
to what they were before it existed, so the scheduled `formal-full` lane keeps
its heap headroom. That lane runs profiles far larger than anything measured
here — `check_formal_admission.py`'s full profile floors at 22.3M distinct
states against `formal/nomos/Nomos.cfg`'s 380K — and capping a gating lane
without a measurement behind it is not something this profile does. Set
`OKG_TLC_HEAP` explicitly in CI if you want one.

No knob changes a verdict: capping bounds how much of the machine a run holds
at once, not which states TLC explores.

`formal/design-contract.md` defines the decision contract used when code and a
model disagree. `formal/gap-inventory.toml` records the current classified gaps
from model review so follow-up work can decide whether a fix belongs in code,
the model, boundary text, or evidence mapping.
`formal/boundary-inventory.toml` records runtime/test boundaries, indirect
coverage, archive-only helpers, and audit examples that are now directly
modeled so the coverage map does not imply accidental proof.

## Verification Process

Every model should have:

- a `.tla` module and `.cfg` config under `formal/<area>/`, or an executable
  `.py` model with explicit non-vacuity/state-count floors in its checker;
- a checker wrapper under `scripts/check_formal_*.py` or a Makefile target that
  fails loudly when Java/TLC or executable-model coverage is unavailable;
- a strict `formal-report.yaml` row with `kind: formal_check`, `status: pass`,
  `fresh: true`, local module/config digests, the executed command, and all
  linked implementation tests;
- timeout reports are nonpassing rows with `failure_class: timeout` and
  `proof_status: not_freshly_proven`; invariant violations are
  `failure_class: invariant_violation` and `proof_status: semantic_failure`;
- implementation tests that drive the production implementation through the same
  states or decision surfaces named by the model;
- evidence ingestion through `uv run okg pact evidence ingest <id>
  --formal-report <path> --json` when a change record needs formal proof.

Folder READMEs are guarded by `tests/docs/test_formal_readmes.py` and
`tests/docs/test_formal_design_contract.py`. Those tests require each direct
model folder to declare its design contract, list its modules and configs or
Python executable model files, map every model to production code and
implementation tests, and explain every invariant declared in its TLC configs or
every executable property emitted by the checker.

## Updating The Surface

When adding or changing a model:

1. Update the `.tla`/`.cfg` pair together, or update the executable `.py` model
   with its checker count floors.
2. Update or add the checker wrapper and strict report producer test.
3. Update the folder README's System, Design Contract, Model Inventory,
   Transformation Map, Invariant Map, Verification Process, and Model Boundary
   sections.
4. Bind the property to source code and implementation tests, then ingest fresh
   formal evidence in the relevant change record when needed.
5. Add negative or counterexample coverage when the property could otherwise be
   vacuous.
