# Actionable MCP Bounds Formal Model

## System

This model covers bounded MCP path expansion and whole-subtype inspection. It
focuses on typed path outcomes, hidden-structure non-disclosure, bounded
diagnostic work, stable subtype-summary pagination, and continuation tokens
that cannot be replayed across authority or snapshot boundaries.

## Design Contract

- Safety property: callers receive a typed outcome for every supported path
  state; visible-equivalent hidden and absent graph structures produce the same
  public envelope; subtype pages reconstruct one stable summary without gaps or
  duplicates; and a continuation is accepted only in its issuing context.
- Authority boundary: the pinned `Session` and sanitized authority context in
  `src/okg/substrate/mcp/tools.py`, `src/okg/substrate/mcp/path_diagnostics.py`,
  `src/okg/substrate/mcp/subtype_summary.py`, and
  `src/okg/substrate/mcp/continuation.py` own read and cursor authority.
- Valid runtime paths: pre-execution path validation, visible exact-empty hop
  diagnostics, visibility-limited no-disclosure results, complete and truncated
  path reads, compact subtype-summary pages, and authenticated continuation
  resume or typed rejection.
- Out of scope: database isolation, catalog publication, Nomos policy-language
  correctness, cryptographic primitive implementation, and selection of the
  session's default generation remain separate runtime contracts.
- Preconditions/refinement: `tests/substrate/mcp/test_expand_path_diagnostics.py`
  and `tests/substrate/mcp/test_inspect_subtype_summary.py` refine the finite
  model against real PostgreSQL and public MCP behavior; the executable checker
  does not replace those tests or published-generation health evidence.

## Model Inventory

| Model | Config | Purpose |
| --- | --- | --- |
| `ActionableBounds.tla` | `ActionableBounds.cfg` | Finite safety model for path outcome equivalence, count disclosure, bounded probes, stable summary pages, and complete cursor-context binding. |
| `check_actionable_bounds.py` | executable-model constants matching `ActionableBounds.cfg` | Offline exhaustive refinement checker and strict formal-report producer used when the Python engine is selected. |

## Transformation Map

| Model | Runtime transformation | Production code | Implementation tests |
| --- | --- | --- | --- |
| `ActionableBounds.tla` | A validated path request becomes one typed invalid, empty, complete, truncated, or visibility-limited public envelope while probing only a bounded frontier. | `src/okg/substrate/mcp/path_diagnostics.py`, `src/okg/substrate/mcp/tools.py` | `tests/substrate/mcp/test_expand_path_diagnostics.py`, `tests/substrate/mcp/test_mcp_actionable_bounds_e2e.py` |
| `ActionableBounds.tla` | A subtype request becomes an ordered compact schema-summary page whose continuation advances through stable sections without repeating or skipping data. | `src/okg/substrate/mcp/subtype_summary.py`, `src/okg/substrate/mcp/response_bounds.py` | `tests/substrate/mcp/test_inspect_subtype_summary.py`, `tests/substrate/mcp/test_response_byte_budget.py` |
| `ActionableBounds.tla` | A continuation is authenticated and compared with deployment, database, branch, generation, catalog, subtype/path, authority, policy, sort, projection, key, and cursor bindings before resume. | `src/okg/substrate/mcp/continuation.py`, `src/okg/substrate/mcp/server.py` | `tests/substrate/mcp/test_inspect_subtype_summary.py`, `tests/substrate/mcp/test_expand_path_diagnostics.py` |
| `check_actionable_bounds.py` | The finite graph-visibility, summary-page, and token-context state spaces are exhaustively checked and emitted as strict formal evidence. | `src/okg/substrate/mcp/path_diagnostics.py`, `src/okg/substrate/mcp/subtype_summary.py`, `src/okg/substrate/mcp/continuation.py` | `tests/substrate/mcp/test_expand_path_diagnostics.py`, `tests/substrate/mcp/test_inspect_subtype_summary.py` |

## Invariant Map

| Invariants | What it means | Code/test signal |
| --- | --- | --- |
| `TypeOK`, `StructurePairVisibilityBound`, `SummarySectionShape` | Model phases, graph visibility pairs, outcomes, count states, summary sections, and token contexts stay inside their declared finite domains. | Shape and malformed-input cases in `tests/substrate/mcp/test_expand_path_diagnostics.py` and `tests/substrate/mcp/test_inspect_subtype_summary.py`. |
| `PublicOutcomeEquivalence`, `NoHiddenStructureLeak` | Hidden and absent structures with the same visible projection cannot produce distinguishable public envelopes or hidden cardinalities. | Nomos hidden-versus-absent cases in `tests/substrate/mcp/test_expand_path_diagnostics.py` and `tests/substrate/nomos/test_nomos_source_lineage_read_filter.py`. |
| `CountStateSafety`, `HiddenHopOutcomeSafety`, `VisibleHopOutcomeSafety` | Visibility-limited counts are withheld, visible empty results report exact zero, and each visible decisive hop receives the correct actionable outcome. | Outcome and count assertions in `tests/substrate/mcp/test_expand_path_diagnostics.py`. |
| `BoundedProbeWork` | Empty-hop diagnosis never requires an unbounded frontier materialization. | Bounded SQL/probe assertions in `tests/substrate/mcp/test_expand_path_diagnostics.py` and `tests/substrate/test_mcp_read_surface_bounds.py`. |
| `SummaryPaginationSafety` | Ordered subtype-summary pages reconstruct every section once, without gaps or duplicates. | Multi-page reconstruction in `tests/substrate/mcp/test_inspect_subtype_summary.py`. |
| `CursorContextBinding`, `CursorContextFieldsBound`, `ContinuationTokenShape` | Tokens are deterministic for one complete context and reject tampering or any changed snapshot, authority, projection, policy, key, or position field. | Tamper and cross-context rejection in `tests/substrate/mcp/test_inspect_subtype_summary.py` and `tests/substrate/mcp/test_expand_path_diagnostics.py`. |
| `ActionableBoundsSafety` | The combined path, disclosure, paging, work-bound, and cursor contracts hold together. | `formal/mcp/check_actionable_bounds.py` plus the two refinement suites above. |

## Verification Process

Run the offline executable model:

```sh
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python \
  formal/mcp/check_actionable_bounds.py \
  --engine python \
  --report .scratch/formal/actionable-bounds/formal-report.yaml
```

The report is a strict `formal_check` row with module/config digests, refinement
tests, distinct-state counts, and `proof_kind: executable_model`. When a valid
pinned `tla2tools.jar` is available, the checker can run TLC; a missing or
broken selected engine is a failure, not a passing fallback. Ingest fresh
evidence with `uv run okg pact evidence ingest <id> --formal-report <path>
--json`.

## Model Boundary

The model abstracts graph rows to visible, hidden, and absent hop classes and
abstracts the MAC to equality with a canonical context-bound token. Real SQL
ordering, response serialization, HMAC verification, generation/catalog pins,
and Nomos enforcement remain covered by the mapped implementation tests and a
published disposable deployment. The model proves bounded public-envelope and
cursor-context safety; it does not prove graph ingestion, publication health,
or default-generation selection.
