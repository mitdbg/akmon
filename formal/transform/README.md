# Transform Formal Models

## System

The transform model covers narrowing semantics shared by the writer path and
the TransformConsistencyGate. It ensures a tuple accepted by one surface is
accepted by the other when both surfaces use the same catalog version. It also
covers inactive catalogs, subtype-refinement closure, and alias-row behavior.

## Design Contract

- Safety property: writer-path narrowing and TransformConsistencyGate narrowing
  accept the same edge triples when they use the same catalog version and
  subtype closure.
- Authority boundary: catalog narrowing data owns allowed triples; writer and
  gate code must consume the same effective catalog semantics.
- Valid runtime paths: inactive catalogs, zero-row active catalogs, subtype
  refinement closure, alias rows, writer checks, and gate checks.
- Out of scope: producer profile authorization, catalog composition, and
  publish-time node-subtype conformance remain separate checks.
- Preconditions/refinement: same-catalog-version agreement is explicit in the
  model as `SameCatalogVersion`. `test_writer_gate_allowed_sets_require_same_catalog_version`
  checks that the mapped runtime comparison uses the writer checker's resolved
  catalog version as the gate catalog version.
- Reflexive-closure completeness: both surfaces admit a declared narrowing triple
  via a descendant-closure arm UNION a raw-rows arm, so the declared
  `(parent, edge, child)` triple is accepted even when `subtype_ancestors` lacks
  the reflexive self-row. The writer (`catalog/narrowings.py`), the DB trigger
  (`db/schema.sql`), and the gate binding (`transform/consistency.py`
  `narrowing_allowed_binding`) all carry the raw-rows arm; the model proves they
  stay equivalent under an incomplete closure (`WriterGateNarrowingIncompleteClosure.cfg`)
  and the witness shows a closure-only gate diverges
  (`WriterGateNarrowingRawRowsCounterexample.cfg`). The
  `transform.catalog-version-skew-unmodeled` gap in `formal/gap-inventory.toml`
  tracks the remaining catalog-version-derivation seam (distinct from closure
  completeness); `test_writer_and_gate_agree_under_incomplete_reflexive_closure`
  is the refinement test for the closure case.

## Model Inventory

| Model | Config | Purpose |
| --- | --- | --- |
| `WriterGateNarrowingEquivalence.tla` | `WriterGateNarrowingEquivalence.cfg` | Writer/gate narrowing acceptance equivalence over a complete reflexive closure with both surfaces' raw-rows arm. |
| `WriterGateNarrowingEquivalence.tla` | `WriterGateNarrowingIncompleteClosure.cfg` | Positive proof the gate fix holds: with the gate's raw-rows arm on, writer and gate stay equivalent even when the reflexive `subtype_ancestors` self-row is missing (`ReflexiveClosureComplete = FALSE`). |
| `WriterGateNarrowingEquivalence.tla` | `WriterGateNarrowingRawRowsCounterexample.cfg` | Buggy-toggle witness: a closure-only gate (`BuggyGateOmitsRawRows = TRUE`) disagrees with the writer under an incomplete closure, refuting `WriterGateNarrowingEquivalence` by name -- the spurious publish-block the raw-rows gate fix closes. |

## Transformation Map

| Model | Runtime transformation | Production code | Implementation tests |
| --- | --- | --- | --- |
| `WriterGateNarrowingEquivalence.tla` | The writer path resolves the latest applied catalog version and expands its narrowing rows through subtype refinement. | `src/okg/substrate/catalog/narrowings.py`, `src/okg/substrate/projection/projector.py`, `src/okg/substrate/producer_authority.py` | `tests/substrate/test_narrowing_equivalence.py`, `tests/substrate/test_derived_producer_authority.py` |
| `WriterGateNarrowingEquivalence.tla` | The gate receives the publish catalog version and binds the same narrowing closure as the TransformConsistencyGate allowed surface. | `src/okg/substrate/consistency/transform_gate.py`, `src/okg/substrate/transform/consistency.py`, `src/okg/substrate/publisher/publisher.py` | `tests/substrate/test_narrowing_equivalence.py`, `tests/substrate/transform/test_chronos_ir_seam.py` |

## Invariant Map

| Invariants | What it means | Code/test signal |
| --- | --- | --- |
| `TypeOK` | Catalog rows, subtype relationships, aliases, and candidate triples keep the expected shape. | Catalog setup in `tests/substrate/test_narrowing_equivalence.py`. |
| `WriterGateNarrowingEquivalence`, `AllTuplesEquivalent`, `WriterGateNarrowingContract` | The writer path and transform gate accept the same triples when `SameCatalogVersion` holds. | Equivalence assertions in `tests/substrate/test_narrowing_equivalence.py`. |
| `CatalogVersionMismatchCanDisagree` | If the writer and gate use different non-empty catalog versions, the model does not claim equivalence. | Mismatch case in `test_writer_gate_allowed_sets_require_same_catalog_version`. |
| `InactiveCatalogPermitsAll` | A catalog with no active narrowing rows does not accidentally block edges. | Inactive-catalog test in `tests/substrate/test_narrowing_equivalence.py`. |
| `AliasRowsDoNotCanonicalizeNarrowing` | Alias metadata must not silently broaden narrowing acceptance. | Alias-row cases in `tests/substrate/test_narrowing_equivalence.py`. |
| `AllowedTriplesUseCatalogSubtypesOnly`, `RepresentativeRefinementCases` | Allowed triples come from catalog subtypes and known subtype-refinement closure. | Catalog and subtype cases in `tests/substrate/test_derived_producer_authority.py` and `tests/substrate/transform/test_chronos_ir_seam.py`. |

## Verification Process

Run:

```sh
uv run --extra dev python scripts/check_formal_writer_gate_narrowing.py \
  --out .scratch/formal/writer-gate-narrowing
```

The checker emits a strict formal report row for
`WriterGateNarrowingEquivalence` and names the DB-backed implementation tests in
`tests/substrate/test_narrowing_equivalence.py`, including the same-catalog
runtime comparison test.

## Model Boundary

Implementation tests must compare the real writer whitelist with the gate
allowed surface for the same catalog version, including inactive catalogs and
subtype-refinement cases. The model does not prove catalog composition or
publish-time node-subtype conformance; those are separate substrate checks.
