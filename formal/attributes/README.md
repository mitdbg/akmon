# Attribute State Formal Model

## System

The attribute state model covers the proposed internal AKMON attribute
management boundary. It focuses on observation history for node and edge
attributes, accepted effective projections, rejected/superseded/stale values,
external live hydration, read-surface consistency, audit-retention pruning,
conflict overwrite prevention, and explicit promotion into user-facing graph
topology.

## Design Contract

- Safety property: default AKMON reads expose only accepted, materialized
  effective attrs; rejected, superseded, stale, and live external observations
  cannot become default effective attrs; audit-critical observations cannot be
  pruned; and user-facing promotion requires an explicit promotable target.
- Authority boundary: the future internal attribute-management write/read path
  owns observation state and effective projection, while current read surfaces
  in `src/okg/deployment/_internal/reads.py`, `src/okg/substrate/mcp/search.py`,
  and `src/okg/substrate/graph_store.py` define the raw-attrs compatibility
  boundary.
- Valid runtime paths: raw source attr publication, candidate overlay
  admission, reviewer rejection, overlay supersession, dependency invalidation,
  external live hydration, derived materialization, default effective reads, and
  explicit promotion into user-facing OKG topology.
- Out of scope: value-level merge policy, SQL index shape, retention windows,
  conflict ranking, source-run scheduling, and full graph publish mechanics
  remain covered by implementation tests and published-generation evidence.
- Preconditions/refinement: the model assumes a non-empty node attr target, edge
  attr target, observation set, and value set. Existing refinement checks cover
  flat pinned attrs and external-live read purity; later implementation tasks
  must add storage, projection, provenance, and invalidation tests before
  claiming runtime completion.

## Model Inventory

| Model | Config | Purpose |
| --- | --- | --- |
| `AttributeState.tla` | `AttributeState.cfg` | Attribute observation history, default effective projection, the maintained overlay-range read model (`viewRow`) and its lazy `MaintainView` refresh, read-surface consistency, retention pruning, external live boundary, conflict prevention, and explicit user-graph promotion contract. |
| `AttributeState.tla` | `AttributeStateSmoke.cfg` | CI/operator smoke profile over a reduced observation/read-surface universe for read-surface, retention, and view-maintenance contracts. |
| `AttributeState.tla` | `AttributeViewDriftCounterexample.cfg` | Buggy-toggle witness: `BuggyViewMaintenanceSkipsCopy = TRUE` makes `MaintainView` mark the view fresh without copying `effective`, refuting `ViewMatchesEffectiveWhenFresh` by name via a reachable stale-view trace so the view-drift invariant is load-bearing rather than a conflation of the view with truth. |

## Transformation Map

| Model | Runtime transformation | Production code | Implementation tests |
| --- | --- | --- | --- |
| `AttributeState.tla` | Raw source attrs publish as observations and accepted overlays materialize into a per-generation effective-attrs snapshot; raw values remain source-owned facts under the pinned graph tables. | `src/okg/substrate/attributes.py` | `tests/substrate/test_mcp_tools.py`, `tests/pact/test_formal_report_producers.py` |
| `AttributeState.tla` | Default reads project accepted effective attrs over flat pinned attrs without exposing observation envelopes or provenance unless explicitly requested. | `src/okg/substrate/attributes.py`, `src/okg/substrate/mcp/search.py` | `tests/substrate/test_mcp_tools.py`, `tests/substrate/mcp/test_search_context_budget.py` |
| `AttributeState.tla` | External live hydration attaches an opt-in overlay and does not mutate pinned graph rows or become default effective attrs. | `src/okg/substrate/external_backed/resolver.py` | `tests/substrate/external_backed/test_inspect_hydration.py` |
| `AttributeState.tla` | Inspect/query/filter/map/expand-style reads use the same `apply_effective_attrs_to_rows` projection instead of raw history or live overlays. | `src/okg/substrate/attributes.py`, `src/okg/substrate/mcp/search.py`, `src/okg/substrate/mcp/operators.py` | `tests/substrate/test_mcp_tools.py`, `tests/substrate/mcp/test_operator_surface_integration.py` |
| `AttributeState.tla` | Reads resolve off the maintained overlay validity ranges (`viewRow`), a materialized projection distinct from ground-truth `effective`; the view is refreshed by a separate maintenance step (`MaintainView`) and may lag truth between refreshes, so `ViewMatchesEffectiveWhenFresh` only holds once the view is marked fresh. | `src/okg/substrate/attributes.py` (`okg.attribute_overlay_ranges`, `sync_overlay_ranges_for_generation`) | `tests/substrate/test_mcp_tools.py::test_get_node_returns_at_pin`, `tests/pact/test_formal_report_producers.py` |
| `AttributeState.tla` | Retention pruning of non-materialized external-live candidates is a proposed subsystem with no production counterpart yet; raw, terminal, effective, and promoted observations are intended to remain audit-retained. | `src/okg/substrate/attributes.py` | `tests/pact/test_formal_report_producers.py` |
| `AttributeState.tla` | The smoke profile reruns read-surface and retention contracts over reduced observation/read-surface bounds for CI/operator freshness accounting. | `src/okg/substrate/attributes.py`, `src/okg/substrate/mcp/search.py` | `tests/pact/test_formal_report_producers.py::test_expensive_tlc_smoke_profiles_emit_profile_metadata` |
| `AttributeState.tla` | Overlay acceptance, rejection, supersession, invalidation, and provenance are implemented in the attribute store; explicit promotion into user-facing topology is a proposed subsystem with no production counterpart yet. | `src/okg/substrate/attributes.py`, `src/okg/substrate/mcp/search.py` | `tests/pact/test_formal_report_producers.py` |

## Invariant Map

| Invariants | What it means | Code/test signal |
| --- | --- | --- |
| `TypeOK` | Observation state, effective projections, external live overlays, and promoted observations keep the expected shape. | Strict report producer checks in `tests/pact/test_formal_report_producers.py`. |
| `NodeEdgeSymmetry` | The same status vocabulary and effective projection contract covers node attributes and edge attributes. | Future node/edge symmetry tests must bind to the storage/projection implementation before runtime completion. |
| `EffectiveKnown`, `EffectiveMatchesAttribute`, `EffectiveOnlyAcceptedProjection` | A default effective attr can only point to a known, accepted, published, non-external observation for the same target. | Flat inspect/read tests in `tests/substrate/test_mcp_tools.py` and future effective-read tests. |
| `RejectedNeverEffective`, `SupersededNeverEffective`, `StaleNeverEffective` | Rejected, superseded, and stale observations remain in history but are never served as default effective attrs. | Future history and invalidation tests must bind to the storage/projection implementation before runtime completion. |
| `ExternalLiveMatchesAttribute`, `ExternalLiveNeverEffective` | Hydrated live values stay labeled overlays for the requested attr and never become pinned effective attrs merely by inspection. | `tests/substrate/external_backed/test_inspect_hydration.py`. |
| `RawSourceObservationImmutable` | Raw source observations stay accepted, materialized, non-external, and unsuperseded as source facts even when an overlay becomes effective. | Raw graph attrs are owned by source tables; the effective-attrs projection over them is in `src/okg/substrate/attributes.py` (`apply_effective_attrs_to_rows`); future raw/overlay tests are declared in the PACT. |
| `SupersededKeepsHistory`, `TerminalObservationRetained` | Past rejected, superseded, and stale observations remain retained in the internal history rather than disappearing through JSON overwrite. | Future attribute-history tests must bind to the storage/projection implementation before runtime completion. |
| `AcceptedOverlayIsEffectiveOrSuperseded`, `OneEffectivePerAttribute` | Accepted overlay values are either the current effective value or have an explicit replacement, and each attr has at most one effective observation. | Future effective-read and supersession tests are declared in the PACT. |
| `ReservedEffectiveOnlyRaw`, `ReservedPinnedEffectiveOnlyRaw`, `PinnedSnapshotKnown`, `PinnedSnapshotOnlyMaterializedProjection`, `PinnedSnapshotNeverUsesExternalLive` | Authority-owned attrs such as Nomos source-lineage metadata may be raw source facts, but derived/candidate overlays never become effective or pinned effective values for those keys; pinned snapshots refer only to known materialized projection values and never to external live overlays. | `tests/substrate/mcp/test_nomos_source_lineage_mcp.py::test_effective_attrs_cannot_spoof_source_lineage_for_search` and flat pinned-read coverage in `tests/substrate/test_mcp_tools.py`. |
| `PromotedOnlyAcceptedMaterialized`, `PromotedOnlyPromotable` | Attribute observations become user-facing graph topology only through explicit promotion of accepted materialized values whose target is eligible for promotion. | Promotion into user-facing topology is a proposed/implementation-pending subsystem with no production counterpart yet; future promotion/admission tests must bind to the attribute store in `src/okg/substrate/attributes.py`. |
| `ReadSurfaceCoverage`, `LastReadUsesEffective`, `LastReadOnlyAcceptedProjection`, `LastReadNeverUsesExternalLive` | Every modeled AKMON read surface reads through the maintained view snapshot (which only ever holds materialized projection values copied from `effective`) and cannot accidentally use raw history or external live overlays. | Read-surface tests in `tests/substrate/test_mcp_tools.py` and future operator tests declared in the PACT. |
| `ViewMatchesEffectiveWhenFresh`, `ViewOnlyHoldsMaterialized`, `ViewMatchesAttribute`, `ViewNeverUsesExternalLive`, `AttributeViewMaintenanceSafety` | The maintained overlay-range read model is a variable distinct from ground-truth `effective`: it may lag between refreshes but, once `MaintainView` marks it fresh, it equals `effective`; while lagging it still only holds materialized, correctly-keyed, non-external observations. Load-bearing via `AttributeViewDriftCounterexample.cfg` (a `MaintainView` that skips the copy but claims freshness serves stale reads). | The maintained read model is `okg.attribute_overlay_ranges` (migration 0044), synced at every root-branch publish; pin-read purity in `tests/substrate/test_mcp_tools.py::test_get_node_returns_at_pin`. |
| `RetainedObservationsKnown`, `AuditCriticalRetained`, `UnretainedOnlyNonAudit` | Retention policy may prune non-critical external live candidates, but raw, terminal, effective, and promoted observations remain audit-retained. | Retention/pruning is a proposed/implementation-pending subsystem with no production counterpart yet; future retention/provenance tests must bind to the attribute store in `src/okg/substrate/attributes.py` before runtime completion. |
| `AttributeReadSurfaceSafety` | The query-consistency risk is checked as a standalone formal property. | The checker command emits a strict formal report consumed by PACT evidence. |
| `AttributeRetentionSafety` | The storage-growth/retention risk is checked as a standalone formal property. | The checker command emits a strict formal report consumed by PACT evidence. |
| `AttributePromotionLifecycleSafety` | The graph-promotion lifecycle risk is checked as a standalone formal property. | The checker command emits a strict formal report consumed by PACT evidence. |
| `AttributeConflictSafety` | The silent overwrite/conflict risk is checked as a standalone formal property. | The checker command emits a strict formal report consumed by PACT evidence. |
| `AttributeStateSafety` | The combined attribute-management contract holds across raw facts, overlays, read surfaces, retention, external live values, history, conflicts, and promotion. | The checker command emits a strict formal report consumed by PACT evidence. |

## Execution Profiles

| Profile | Config | Budget | Purpose | Command |
| --- | --- | --- | --- | --- |
| `smoke` | `AttributeStateSmoke.cfg` | 30s | CI/operator freshness check for `AttributeReadSurfaceSafety` and `AttributeRetentionSafety`; not release evidence. | `uv run --extra dev python scripts/check_formal_attribute_state.py --profile smoke --out .scratch/formal/attribute-state-smoke --timeout-seconds 30` |
| `full` | `AttributeState.cfg` | 600s | Full-strength release/PACT evidence for the aggregate and standalone AttributeState properties. | `uv run --extra dev python scripts/check_formal_attribute_state.py --profile full --out .scratch/formal/attribute-state --timeout-seconds 600` |

Use `uv run --extra dev python scripts/check_formal_attribute_state.py
--list-profiles --json` to inspect the profile metadata. A timeout row is
`timeout/not freshly proven`, not a semantic invariant failure.

## Verification Process

Run:

```sh
uv run --extra dev python scripts/check_formal_attribute_state.py \
  --profile full --out .scratch/formal/attribute-state --timeout-seconds 600
```

The checker emits a strict formal report and fails loudly if TLC, Java, model
artifacts, property rows, or expected invariants are missing. Change records
should attach that report with `uv run okg pact evidence ingest <change-id>
--formal-report .scratch/formal/attribute-state/formal-report.yaml --json`.

## Model Boundary

The model deliberately treats attribute targets, values, producers, dependency
fingerprints, reviewers, namespaces, read surfaces, and promotion eligibility as
finite abstract values. It does not prove storage schema shape, SQL performance,
human semantic promotion criteria, provenance payload fields, graph publish
correctness, or retention-window sizing. Those remain runtime obligations for
the later PACT tasks and must be proven with real tests and published-generation
evidence before implementation is considered complete.
