"""Phase 2 tests for A4 — BibReferencesSource.

Reads a 3-entry sample .bib file and verifies that the source emits
the expected Paper / Person / Venue / ConferenceEdition NodeFacts
plus authored_by / published_in / member_of EdgeFacts, with tier
propagating from BibTeX `annote`.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources.scholarly.bib_references import (
    BibReferencesSource,
)


FIXTURE = Path(__file__).resolve().parent / "fixtures" / "sample.bib"


def _collect(source: BibReferencesSource) -> tuple[dict, list, dict]:
    """Run the source and split facts into (nodes_by_id, edges, stats)."""
    run = source.run("test-run-id", mode="scope_complete")
    nodes: dict[str, object] = {}
    edges: list[object] = []
    for f in run.facts:
        if hasattr(f, "node_id"):
            nodes[f.node_id] = f
        elif hasattr(f, "edge_type"):
            edges.append(f)
    return nodes, edges, source.last_run_stats


def test_bib_emits_paper_person_venue_for_each_entry() -> None:
    src = BibReferencesSource(bib_paths=[str(FIXTURE)])
    nodes, edges, stats = _collect(src)

    # Three papers — keyed by DOI, arxiv_id, and bib cite_key respectively
    assert "paper:doi:10.5555/0001.1986" in nodes  # example storage paper
    assert "paper:arxiv:1706.03762" in nodes  # Attention
    assert "paper:bib:codd1970relational" in nodes  # Codd, no DOI/arxiv

    # Six unique Person nodes (Vale, Quinn, Vaswani, Shazeer, Parmar, Codd)
    persons = [k for k in nodes if k.startswith("person:")]
    assert len(persons) >= 6

    # Three Venue nodes (SIGMOD Record, NeurIPS, Communications of the ACM)
    venues = [k for k in nodes if k.startswith("venue:")]
    assert len(venues) == 3
    assert any("neurips" in v for v in venues)

    # Stats
    assert stats["entries"] == 3
    assert stats["papers"] == 3


def test_bib_tier_from_annote() -> None:
    src = BibReferencesSource(bib_paths=[str(FIXTURE)])
    nodes, _, _ = _collect(src)

    storage = nodes["paper:doi:10.5555/0001.1986"]
    assert storage.attrs["tier"] == 1
    assert storage.attrs["auto_extract"] is True

    codd = nodes["paper:bib:codd1970relational"]
    assert codd.attrs["tier"] == 2
    assert "auto_extract" not in codd.attrs

    # Vaswani has no annote → default tier 1
    attention = nodes["paper:arxiv:1706.03762"]
    assert attention.attrs["tier"] == 1


def test_bib_authored_by_edges() -> None:
    src = BibReferencesSource(bib_paths=[str(FIXTURE)])
    _, edges, _ = _collect(src)

    authored = [e for e in edges if e.edge_type == "authored_by"]
    # Storage paper: 2 authors; Attention: 3; Codd: 1 → 6 authored_by edges
    assert len(authored) == 6


def test_bib_published_in_and_member_of_edges() -> None:
    src = BibReferencesSource(bib_paths=[str(FIXTURE)])
    nodes, edges, _ = _collect(src)

    published = [e for e in edges if e.edge_type == "published_in"]
    member_of = [e for e in edges if e.edge_type == "member_of"]
    # All three entries have a venue + year → 3 published_in
    assert len(published) == 3
    # Only @inproceedings mints conference lineage; journal articles link to Venue.
    assert len(member_of) == 1

    # ConferenceEdition node ids include year
    assert "confed:neurips-2017" in nodes
    assert "confed:sigmod-record-1986" not in nodes
    assert any(
        edge.src == "paper:doi:10.5555/0001.1986"
        and edge.dst == "venue:sigmod-record"
        for edge in published
    )


def test_unscoped_source_record_identities_remain_legacy_compatible() -> None:
    src = BibReferencesSource(bib_paths=[str(FIXTURE)])
    nodes, edges, _ = _collect(src)
    fixture_path = str(FIXTURE)

    venue = nodes["venue:neurips"]
    conference = nodes["conf:neurips"]
    edition = nodes["confed:neurips-2017"]
    member = next(
        edge for edge in edges
        if edge.src == "confed:neurips-2017" and edge.edge_type == "member_of"
    )
    assert venue.source_record_id == {
        "bib_path": fixture_path, "venue": "NeurIPS",
    }
    assert conference.source_record_id == {"venue": "NeurIPS"}
    assert edition.source_record_id == {"venue": "NeurIPS", "year": "2017"}
    assert member.source_record_id == {"venue": "NeurIPS", "year": "2017"}


def test_bib_empty_paths_raises() -> None:
    with pytest.raises(SubstrateError) as exc:
        BibReferencesSource(bib_paths=[])
    assert "bib_paths is required" in str(exc.value)


def test_bib_unresolved_glob_raises(tmp_path: Path) -> None:
    src = BibReferencesSource(bib_paths=[str(tmp_path / "*.bib")])
    with pytest.raises(SubstrateError) as exc:
        list(src.run("test", mode="scope_complete").facts)
    assert "resolved to zero files" in str(exc.value)
