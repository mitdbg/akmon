"""task.gtir.coverage-probe -- is the closed operator set enough for the workload?

Lowers a representative program per substrate species into the operators and
checks the falsifiable claim: the read/consistency species express with ZERO
CALL-escape, and mutation species (ingestion/enrichment) stay bounded. Deferred
breadth checks (TPC-H/DS, live trace) are reported, never silently dropped.
"""

from __future__ import annotations

from okg.substrate.transform.coverage import (
    DEFERRED,
    PURE_CORE_SPECIES,
    SPECIES,
    call_escape_rate,
    coverage_report,
)


def test_read_and_consistency_species_have_zero_call_escape():
    # The common-case read/consistency species use NO opaque CALL -- the closed
    # core expresses them directly.
    for name in PURE_CORE_SPECIES:
        prog = SPECIES[name]()
        assert call_escape_rate(prog) == 0.0, name


def test_mutation_species_call_escape_is_bounded():
    # Ingestion/enrichment legitimately use CALL (parser / LLM derivation) but the
    # opaque share stays small (one CALL among several core ops).
    assert 0.0 < call_escape_rate(SPECIES["ingestion"]()) <= 0.34
    assert 0.0 < call_escape_rate(SPECIES["enrichment_compute"]()) <= 0.5


def test_report_covers_all_species_and_logs_deferred():
    report = coverage_report()
    assert set(report["species"]) == set(SPECIES)
    # Every species' histogram is non-empty and over core op kinds.
    for name, info in report["species"].items():
        assert info["histogram"], name
    # No silent caps: deferred breadth checks are explicitly recorded.
    assert report["deferred"] == DEFERRED
    assert len(report["deferred"]) >= 2


def test_no_species_leaks_majority_to_call():
    # Sanity bound on the whole workload: no species is mostly opaque.
    for name in SPECIES:
        assert call_escape_rate(SPECIES[name]()) < 0.5, name
