from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources.change_probe import tokens_equal
from okg.substrate.library.sources.scholarly.bib_references import (
    BibReferencesSource,
)


MIXED_BIB = """\
@article{Graham_2021,
  title={Review A}, journal={Annual Review of Nuclear and Particle Science},
  year={2021}, doi={10.1146/annurev-nucl-110320-051823}}
@article{Graham:2021ggy,
  title={Review A}, journal={Ann. Rev. Nucl. Part. Sci.},
  year={2021}, doi={10.1146/annurev-nucl-110320-051823}}
@inproceedings{ConfPaper,
  title={Conference Paper}, booktitle={NeurIPS}, year={2024}}
@proceedings{ProceedingsVolume,
  title={Systems Conference}, year={2025}}
@misc{UnknownJournalShape,
  title={Unknown Shape}, journal={Fallback Journal}, year={2023}}
"""


def _facts(path: Path, *, aliases: dict[str, str] | None = None):
    source = BibReferencesSource(
        bib_paths=[str(path)],
        venue_aliases=aliases,
    )
    facts = list(source.run("shape-test", mode="scope_complete").facts)
    nodes = {fact.node_id: fact for fact in facts if hasattr(fact, "node_id")}
    edges = [fact for fact in facts if hasattr(fact, "edge_type")]
    return source, nodes, edges, facts


def test_entry_type_matrix_aliases_and_doi_key_union(tmp_path: Path) -> None:
    bib = tmp_path / "mixed.bib"
    bib.write_text(MIXED_BIB, encoding="utf-8")
    canonical = "Annual Review of Nuclear and Particle Science"
    _, nodes, edges, facts = _facts(
        bib,
        aliases={"Ann. Rev. Nucl. Part. Sci.": canonical},
    )

    paper_id = "paper:doi:10.1146/annurev-nucl-110320-051823"
    paper_facts = [
        fact for fact in facts
        if getattr(fact, "node_id", None) == paper_id
    ]
    assert len(paper_facts) == 2
    assert all(
        fact.attrs["bib_keys"] == ["Graham:2021ggy", "Graham_2021"]
        for fact in paper_facts
    )
    assert all(
        fact.attrs["bib_key_refs"]
        == ["paper:Graham:2021ggy", "paper:Graham_2021"]
        for fact in paper_facts
    )
    assert all(
        fact.attrs["cite_key_ref"] == "paper:graham-2021ggy"
        for fact in paper_facts
    )
    assert {
        fact.source_record_id["cite_key"] for fact in paper_facts
    } == {"Graham:2021ggy", "Graham_2021"}

    venue_id = "venue:annual-review-of-nuclear-and-particle-science"
    assert nodes[venue_id].attrs["venue_aliases"] == [
        "Ann. Rev. Nucl. Part. Sci.",
        canonical,
    ]
    assert not any(
        node_id.startswith("conf:annual-review")
        or node_id.startswith("confed:annual-review")
        for node_id in nodes
    )
    assert not any("ann-rev-nucl" in node_id for node_id in nodes)
    assert sum(
        edge.src == paper_id
        and edge.dst == venue_id
        and edge.edge_type == "published_in"
        for edge in edges
    ) == 2

    assert "conf:neurips" in nodes
    assert "confed:neurips-2024" in nodes
    assert "venue:systems-conference" in nodes
    assert "conf:systems-conference" in nodes
    assert "confed:systems-conference-2025" in nodes
    assert "venue:fallback-journal" in nodes
    assert "conf:fallback-journal" not in nodes
    assert any(
        edge.src == "paper:bib:unknownjournalshape"
        and edge.dst == "venue:fallback-journal"
        and edge.edge_type == "published_in"
        for edge in edges
    )


def test_live_proof_rejects_blocked_or_unpinned_replay() -> None:
    from scripts.prove_scholarly_source_shapes import _published_generation_id

    with pytest.raises(RuntimeError, match="replay publish blocked"):
        _published_generation_id(
            {"status": "blocked", "generation_id": None},
            stage="replay",
        )

    with pytest.raises(RuntimeError, match="replay publish missing generation_id"):
        _published_generation_id({"status": "published"}, stage="replay")

    assert _published_generation_id(
        {"status": "published", "generation_id": "generation:replay"},
        stage="replay",
    ) == "generation:replay"


def test_merge_is_permutation_and_replay_stable(tmp_path: Path) -> None:
    entries = MIXED_BIB.split("@inproceedings", maxsplit=1)[0]
    forward = tmp_path / "forward.bib"
    reverse = tmp_path / "reverse.bib"
    forward.write_text(entries, encoding="utf-8")
    blocks = [block for block in entries.split("@article") if block.strip()]
    reverse.write_text(
        "".join(f"@article{block}" for block in reversed(blocks)),
        encoding="utf-8",
    )
    aliases = {
        "Ann. Rev. Nucl. Part. Sci.":
            "Annual Review of Nuclear and Particle Science",
    }

    source_a, nodes_a, _, facts_a = _facts(forward, aliases=aliases)
    source_b, nodes_b, _, facts_b = _facts(reverse, aliases=aliases)
    paper_id = "paper:doi:10.1146/annurev-nucl-110320-051823"
    stable_fields = ("bib_keys", "bib_key_refs", "cite_key_ref")
    assert {
        key: nodes_a[paper_id].attrs[key] for key in stable_fields
    } == {
        key: nodes_b[paper_id].attrs[key] for key in stable_fields
    }
    replay = list(source_a.run("shape-test", mode="scope_complete").facts)
    assert [fact.attrs for fact in facts_a if hasattr(fact, "node_id")] == [
        fact.attrs for fact in replay if hasattr(fact, "node_id")
    ]
    assert len(facts_a) == len(replay)
    assert facts_b


@pytest.mark.parametrize(
    "aliases",
    [
        {"Venue": "Canonical A", "venue": "Canonical B"},
        {"A": "B", "B": "C"},
        {"A": "B", "B": "A"},
        {"": "Canonical"},
        {"Alias": ""},
    ],
)
def test_venue_alias_configuration_fails_closed(
    tmp_path: Path, aliases: dict[str, str],
) -> None:
    bib = tmp_path / "one.bib"
    bib.write_text("@article{x,title={X},journal={Venue}}", encoding="utf-8")
    with pytest.raises(SubstrateError):
        BibReferencesSource(bib_paths=[str(bib)], venue_aliases=aliases)


def test_venue_alias_configuration_participates_in_probe(tmp_path: Path) -> None:
    bib = tmp_path / "one.bib"
    bib.write_text("@article{x,title={X},journal={Alias}}", encoding="utf-8")
    first = BibReferencesSource(
        bib_paths=[str(bib)], venue_aliases={"Alias": "Canonical A"}
    ).change_probe.build_token()
    second = BibReferencesSource(
        bib_paths=[str(bib)], venue_aliases={"Alias": "Canonical B"}
    ).change_probe.build_token()
    assert not tokens_equal(first, second)
