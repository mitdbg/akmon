# Console Formal Models

These models are historical reference material for the archived console. The
runtime code and implementation tests they map to were archived under
`archive/ui/legacy-viz-2026-06-17/`, which PACT layout-history-openspec
deleted from the tree: every `archive/` path below resolves only at git
commit `3253beb069ba`. They are not verification gates for the
future single operator app and must not be ingested as active PACT
`formal_check` evidence. Keep them only as design history until a new UI PACT
defines fresh active state-machine properties.

## System

The archived console models cover deployment-agnostic UI state machines whose failures
would mislead operators: vocabulary lookup, modal focus, Escape handling,
offline write replay, conflict routing, and degraded-state banners.

## Design Contract

- Safety property: console chrome must resolve vocabulary deterministically,
  keep modal focus/dismissal coherent, surface offline replay state without
  silently losing queued writes, and prove that online queued writes can make
  replay progress under the model's fairness condition.
- Authority boundary: view-pack vocabulary helpers and shell state machines own
  UI safety; backend graph state is outside this model.
- Valid runtime paths: deployment vocabulary override, missing term error,
  modal push/pop, Escape dismissal, offline queue replay, conflict triage, and
  degraded banner display.
- Out of scope: browser focus APIs, React rendering internals, backend write
  success, and network scheduling remain implementation-test concerns. The
  replay progress property assumes weak fairness for the replay action while
  the console stays online; going offline is allowed and must show degraded
  state.
- Preconditions/refinement: root Console integration gaps are tracked in
  `formal/gap-inventory.toml`; UI integration tests must show the modeled state
  machines are wired into rendered console behavior.

## Model Inventory

| Model | Config | Purpose |
| --- | --- | --- |
| `VocabularyLookup.tla` | `VocabularyLookup.cfg` | Deterministic vocabulary resolution. |
| `VocabularyLookupCounterexample.tla` | `VocabularyLookupCounterexample.cfg` | Expected-failing lookup negative coverage. |
| `ConsoleFocus.tla` | `ConsoleFocus.cfg` | Modal stack and focus safety. |
| `OfflineReplay.tla` | `OfflineReplay.cfg` | Offline replay safety, triage routing, degraded-state visibility, and online replay progress. |
| `OfflineReplayCounterexample.tla` | `OfflineReplayCounterexample.cfg` | Expected-failing replay negative coverage. |

## Transformation Map

| Model | Runtime transformation | Production code | Implementation tests |
| --- | --- | --- | --- |
| `VocabularyLookup.tla` | Profile vocabulary plus deployment overrides becomes a display term or explicit `ERROR`; the active backend vocabulary surface rejects unknown or malformed terms. | `src/okg/substrate/nomos/vocabulary.py`, archived reference `archive/ui/legacy-viz-2026-06-17/src/okg/console/src/view-pack/vocabulary.ts` | `tests/substrate/nomos/test_nomos_deployment_policy_audit.py`, `tests/pact/test_formal_report_producers.py` |
| `VocabularyLookupCounterexample.tla` | The broken lookup prefers profile values over deployment overrides so TLC must find a failure. | `src/okg/substrate/nomos/vocabulary.py`, archived reference `archive/ui/legacy-viz-2026-06-17/src/okg/console/src/view-pack/vocabulary.ts` | `tests/pact/test_formal_report_producers.py` |
| `ConsoleFocus.tla` | Modal stack changes update the topmost modal, focus target, visible dismiss control, and Escape behavior; the active console endpoint keeps composer session/focus authority pinned server-side. | `src/okg/viz/endpoints.py`, `src/okg/substrate/console_views/knowledge.py`, archived reference `archive/ui/legacy-viz-2026-06-17/src/okg/console/src/shell/modalStack.ts` | `test_console_endpoint_session_wiring.py` in the development repository (not in this copy), `tests/pact/test_formal_report_producers.py` |
| `OfflineReplay.tla` | Offline writes are queued, replayed in commit order, routed to conflict triage, shown as degraded when offline or pending, and consumed from the queue head while online under replay fairness; active console readiness views surface degraded substrate state without auto-recovery. | `src/okg/substrate/console_views/health.py`, archived reference `archive/ui/legacy-viz-2026-06-17/src/okg/console/src/shell/offlineReplay.ts` | `test_console_health_composer.py` in the development repository (not in this copy), `tests/pact/test_formal_report_producers.py` |
| `OfflineReplayCounterexample.tla` | The broken replay queue can stay stuck, proving the positive replay model is not vacuous. | archived reference `archive/ui/legacy-viz-2026-06-17/src/okg/console/src/shell/offlineReplay.ts` | `tests/pact/test_formal_report_producers.py` |

## Invariant Map

| Invariants | What it means | Code/test signal |
| --- | --- | --- |
| `TypeOK` | Each model keeps its vocabulary, modal, queue, conflict, and banner state inside the expected shape. | Model-specific setup in `archive/ui/legacy-viz-2026-06-17/src/okg/console/src/view-pack/vocabulary.test.ts`, `archive/ui/legacy-viz-2026-06-17/src/okg/console/src/shell/modalStack.test.ts`, and `archive/ui/legacy-viz-2026-06-17/src/okg/console/src/shell/offlineReplay.test.ts`. |
| `LookupIsDeterministic`, `DeploymentOverridesProfile`, `NoSilentFallbackToSubstrateTerm` | Lookup is repeatable, deployment terms override profile defaults, and missing terms surface `ERROR` instead of leaking substrate words into chrome. | `archive/ui/legacy-viz-2026-06-17/src/okg/console/src/view-pack/vocabulary.ts` and `archive/ui/legacy-viz-2026-06-17/src/okg/console/src/view-pack/vocabulary.test.ts`. |
| `BrokenProfileAlwaysWins` | The negative lookup model must fail when profile values override deployment-specific vocabulary. | Expected counterexample via `scripts/check_formal_console.py --model vocabulary-lookup-counterexample`. |
| `AtMostOneTopmostModal`, `EveryModalHasVisibleDismiss`, `FocusReflectsTopmost`, `EscapeIsEnabledWheneverAModalIsOpen` | The modal stack has one active top modal, a visible dismiss target, correct focus, and Escape enabled whenever dismissal is possible. | `archive/ui/legacy-viz-2026-06-17/src/okg/console/src/shell/modalStack.ts` and `archive/ui/legacy-viz-2026-06-17/src/okg/console/src/shell/modalStack.test.ts`. |
| `WritesAreUniquelyClassified`, `WritesReplayInCommitOrder`, `ConflictsRouteToTriage`, `DegradedBannerReflectsState`, `ConsoleNeverBlocksIndefinitely` | Offline writes are either committed, conflicted, or waiting; replay order is stable; conflicts go to triage; degraded banners match replay state. | `archive/ui/legacy-viz-2026-06-17/src/okg/console/src/shell/offlineReplay.ts` and `archive/ui/legacy-viz-2026-06-17/src/okg/console/src/shell/offlineReplay.test.ts`. |
| `OnlineHeadWriteEventuallyLeavesQueue` | Under `ProgressSpec`, if the console is online and a write is at the queue head, that write eventually leaves the queue unless the console goes offline. | `archive/ui/legacy-viz-2026-06-17/src/okg/console/src/shell/offlineReplay.ts` exposes `replayAccept` and `replayConflict`; `archive/ui/legacy-viz-2026-06-17/src/okg/console/src/shell/offlineReplay.test.ts` checks every reachable online non-empty queue state has replay transitions that consume the head write. |
| `BrokenReplayNeverHappens` | The negative replay model must fail when queued writes never replay. | Expected counterexample via `scripts/check_formal_console.py --model offline-replay-counterexample`. |

## Verification Process

Use the wrapper only as an archived helper. It runs TLC and writes
`archived-formal-report.yaml`; it deliberately removes `formal-report.yaml` so
the output cannot satisfy PACT formal evidence:

```sh
uv run --extra dev python scripts/check_formal_console.py --archived-helper --model vocabulary-lookup
uv run --extra dev python scripts/check_formal_console.py --archived-helper --model console-focus
uv run --extra dev python scripts/check_formal_console.py --archived-helper --model offline-replay
uv run --extra dev python scripts/check_formal_console.py --archived-helper --model vocabulary-lookup-counterexample
uv run --extra dev python scripts/check_formal_console.py --archived-helper --model offline-replay-counterexample
```

Makefile aliases are also available:

```sh
make formal-console
make verify-console
```

Counterexample runs are expected to find the named broken invariant. The wrapper
turns that expected failure into a passing archived-helper row only when TLC
produces the intended counterexample. A run without `--archived-helper` fails
loudly to prevent stale legacy-console proof from entering active PACT evidence.

## Model Boundary

For the archived console, implementation tests drove lookup, modal stack, focus,
offline replay, and triage code through the same cases. These TLA models do not
prove rendering, CSS, animation, browser accessibility trees, or React
implementation details. The future single operator app must define new active
formal or browser contracts instead of reusing these archived-console bindings.
