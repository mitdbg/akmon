# Scholarly Source Shape Formal Model

## System

`ScholarlySourceShapes.tla` models three BibTeX entries ingested in arbitrary
order or replayed: two DOI-grouped article entries with distinct cite keys and
venue spellings, plus one conference-bearing entry. It tracks processed
records, cite-key sets, canonical venues, and conference lineage.

## Design Contract

- Safety property: Processed entries contribute exactly their cite keys; venue
  nodes use only configured canonical identities; only dated
  conference-bearing entries contribute conference lineage; replay does not
  change the modeled state; and normalized aliases cannot select conflicting
  canonicals.
- Authority boundary:
  `src/okg/substrate/library/sources/scholarly/bib_references.py` owns BibTeX
  entry classification, DOI grouping, cite-key union, alias validation, and
  canonical venue projection, while
  `src/okg/substrate/library/linkers/declarative.py` expands the resulting
  multivalue citation references.
- Valid runtime paths: Any entry may arrive first, every entry may be replayed,
  DOI-grouped articles union both keys, an `inproceedings` entry mints its dated
  conference lineage, and configured full/abbreviated spellings select one
  canonical venue.
- Out of scope: BibTeX parsing fidelity, DOI registry truth, source-record
  provenance storage, database narrowing, fact retraction, linker query
  execution, and generation publication remain concrete-test and live-proof
  obligations.
- Preconditions/refinement: Source-shape and permutation tests in
  `tests/substrate/library/sources/scholarly/test_bib_reference_shapes.py` bind
  the modeled entry, alias, and merge decisions to production behavior;
  `tests/substrate/library/linkers/test_declarative_multivalue.py` checks the
  concrete array-expansion boundary.

## Model Inventory

| Model | Config | Purpose |
| --- | --- | --- |
| `ScholarlySourceShapes.tla` | `ScholarlySourceShapes.cfg` | Positive exhaustive safety model for cite-key union, entry-type conference projection, canonical venues, alias consistency, and replay idempotence. |
| `ScholarlySourceShapes.tla` | `ScholarlySourceShapesAmbiguousAlias.cfg` | Retained negative witness in which one normalized alias selects two canonicals and must violate `AliasConfigurationSafe`. |

## Transformation Map

| Model | Runtime transformation | Production code | Implementation tests |
| --- | --- | --- | --- |
| `ScholarlySourceShapes.tla` | Classify each BibTeX entry, union every DOI-grouped cite key, canonicalize its venue before minting, and add conference lineage only for a dated conference-bearing type. | `src/okg/substrate/library/sources/scholarly/bib_references.py` | `tests/substrate/library/sources/scholarly/test_bib_reference_shapes.py`, `tests/substrate/library/sources/scholarly/test_bib_references.py` |
| `ScholarlySourceShapes.tla` alias witness | Reject two raw spellings whose shared normalized key selects conflicting canonical venue identities. | `src/okg/substrate/library/sources/scholarly/bib_references.py`, `src/okg/substrate/library/linkers/declarative.py` | `tests/substrate/library/sources/scholarly/test_bib_reference_shapes.py`, `tests/substrate/library/linkers/test_declarative_multivalue.py` |

## Invariant Map

| Invariant or property | What it means | Code/test signal |
| --- | --- | --- |
| `TypeOK` | Processed entries, per-paper key sets, canonical venue set, and conference set remain within their explicitly modeled domains. | Strict report and model-contract checks in `tests/pact/test_formal_report_producers.py`. |
| `NoLostOrInventedKeys` | Each paper's key set equals exactly the keys of its processed contributing entries. | DOI merge, replay, and permutation tests in `tests/substrate/library/sources/scholarly/test_bib_reference_shapes.py`. |
| `OnlyDatedConferenceEntriesMintConference` | Conference lineage equals exactly the canonical venues contributed by processed dated conference-bearing entries. | Article/inproceedings/proceedings matrix tests in `tests/substrate/library/sources/scholarly/test_bib_reference_shapes.py`. |
| `OnlyCanonicalVenuesMinted` | Every processed raw venue contributes its configured canonical venue and no raw alias identity. | Canonical venue and observed-alias tests in `tests/substrate/library/sources/scholarly/test_bib_reference_shapes.py`. |
| `AliasConfigurationSafe` | Raw aliases with the same normalized key cannot select different canonical venues. | Ambiguous-alias constructor test in `tests/substrate/library/sources/scholarly/test_bib_reference_shapes.py`; `ScholarlySourceShapesAmbiguousAlias.cfg` must violate this invariant. |
| `ReplayIdempotent` | Reprocessing an already processed entry cannot add a new processed record or cite key. | Replay tests in `tests/substrate/library/sources/scholarly/test_bib_reference_shapes.py`. |
| `ScholarlySourceShapeSafety` | Cite-key, conference, venue, alias, and replay invariants hold together for every reachable state. | Aggregate checker registration in `scripts/run_formal_suite.py` and report validation in `tests/pact/test_formal_report_producers.py`. |

## Verification Process

Run the positive model and retained ambiguous-alias witness with:

```sh
uv run --extra dev python scripts/check_formal_scholarly_source_shapes.py \
  --out .scratch/formal/scholarly-source-shapes
```

Run the same checker through the gating aggregate formal surface with:

```sh
uv run python scripts/qa.py formal
```

The checker writes one fresh passing row for the positive model and one fresh
passing witness row only when TLC violates exactly `AliasConfigurationSafe` in
the ambiguous configuration. Aggregate reachability and strict report shape
are guarded by `tests/docs/test_formal_suite_reachability.py` and
`tests/pact/test_formal_report_producers.py`.

## Model Boundary

The model treats entry classification, DOI grouping, key extraction, year
presence, venue lookup, and alias normalization as fixed functions. It does not
model malformed BibTeX, missing years, all supported entry types, Unicode
normalization mechanics, alias chains/cycles/empty values, legacy scalar
compatibility, exact source provenance, database retraction, cross-link
execution, queue health, or graph publication. Those remain obligations of the
production tests named in the Transformation Map and the isolated published
generation proof.
