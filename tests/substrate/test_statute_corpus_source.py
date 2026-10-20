"""StatuteCorpusSource + legal ontology module.

Synthetic TSV fixtures only — the adapter has NO live network path, so
every test writes a local slice file and asserts on the emitted fact
stream.

Layers:

  * parsing (`okg-bench-domain-adapters`, task.dom.housing) —
    header-driven parsing of the citation/path/state/text/idx schema;
    state_filter; missing/unknown headers raise; zero passages after
    filtering raises; text truncation at 4000 chars sets the flag;
  * hierarchy (`okg-corpus-legal`, req.legal.hierarchy) — the `path`
    trail becomes statute_section nodes keyed by (state, prefix), the
    `state` column becomes jurisdiction nodes, and a row with an empty
    path is counted rather than dropped or fabricated;
  * cross-references (`okg-corpus-legal`, req.legal.crossref) — an
    in-slice citation becomes an edge, an out-of-slice one is counted
    and kept as an attribute with NO stub node and NO dangling edge,
    and a slice large enough to contain references that resolves none
    fails the build.
"""
from __future__ import annotations

import pytest
import yaml

from okg.substrate.catalog.modules_compose import compose_catalog
from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.library.sources.statute_corpus import StatuteCorpusSource

_HEADER = "citation\tpath\tstate\ttext\tidx"

# Every element is ONE tab-separated row; the `+` continues a single row
# across source lines for width and is never a missing comma.
_ROWS = [
    "WI Stat. § 704.17\tTitle 704 > Chapter 17\tWI\t"
    + "A landlord may not terminate...\t11",
    "WI Stat. § 799.40\tTitle 799 > Chapter 40\tWI\t"
    + "An eviction action begins...\t12",
    "CA Civ. Code § 1946\tDivision 3 > Part 4\tCA\t"
    + "A hiring of real property...\t21",
    "CA Civ. Code § 1950.5\tDivision 3 > Part 4 > Deposits\tCA\t"
    + ("security deposit " * 300) + "\t22",
]


def _write_tsv(tmp_path, rows=None, header=_HEADER, name="statutes_slice.tsv"):
    path = tmp_path / name
    lines = [header] + list(_ROWS if rows is None else rows)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _facts(source):
    run = source.run("rid", mode="scope_complete")
    return list(run.facts)


def _nodes(source, subtype="statute_passage"):
    return [
        f for f in _facts(source)
        if isinstance(f, NodeFact) and (subtype is None or f.subtype == subtype)
    ]


def _split(source):
    """Return (nodes-by-subtype, edges) for one run."""
    facts = _facts(source)
    nodes: dict[str, list[NodeFact]] = {}
    edges: list[EdgeFact] = []
    for fact in facts:
        if isinstance(fact, NodeFact):
            nodes.setdefault(fact.subtype, []).append(fact)
        else:
            edges.append(fact)
    return nodes, edges


# ---------- parsing ----------


def test_tsv_parses_to_statute_passage_nodes(tmp_path):
    path = _write_tsv(tmp_path)
    nodes = _nodes(StatuteCorpusSource(statutes_tsv=path))
    assert len(nodes) == 4
    by_idx = {n.attrs["statute_idx"]: n for n in nodes}
    wi = by_idx[11]
    assert wi.node_id == "statute_passage:reglab/housing_qa:11"
    assert wi.attrs["citation"] == "WI Stat. § 704.17"
    assert wi.attrs["state"] == "WI"
    assert wi.attrs["header_path"] == "Title 704 > Chapter 17"
    assert wi.attrs["text"].startswith("A landlord may not")
    assert wi.attrs["truncated"] is False
    assert wi.source_record_id == {"repo": "reglab/housing_qa", "idx": 11}


def test_header_order_is_immaterial(tmp_path):
    path = _write_tsv(
        tmp_path,
        header="idx\tstate\tcitation\ttext\tpath",
        rows=["7\tWI\tWI Stat. § 1\tSome text.\tTitle 1"],
    )
    (node,) = _nodes(StatuteCorpusSource(statutes_tsv=path))
    assert node.attrs["statute_idx"] == 7
    assert node.attrs["citation"] == "WI Stat. § 1"
    assert node.attrs["header_path"] == "Title 1"


def test_long_text_is_truncated_with_flag(tmp_path):
    path = _write_tsv(tmp_path)
    nodes = _nodes(StatuteCorpusSource(statutes_tsv=path))
    long_node = next(n for n in nodes if n.attrs["statute_idx"] == 22)
    assert long_node.attrs["truncated"] is True
    assert len(long_node.attrs["text"]) == 4000


# ---------- state_filter ----------


def test_state_filter_limits_emission(tmp_path):
    path = _write_tsv(tmp_path)
    nodes = _nodes(StatuteCorpusSource(statutes_tsv=path, state_filter="WI"))
    assert sorted(n.attrs["statute_idx"] for n in nodes) == [11, 12]
    assert all(n.attrs["state"] == "WI" for n in nodes)
    # Filter comparison is case-insensitive on the code.
    nodes = _nodes(StatuteCorpusSource(statutes_tsv=path, state_filter="wi"))
    assert len(nodes) == 2


def test_zero_passages_after_filtering_raises(tmp_path):
    path = _write_tsv(tmp_path)
    with pytest.raises(SubstrateError, match="zero statute passages"):
        _nodes(StatuteCorpusSource(statutes_tsv=path, state_filter="WY"))


def test_zero_data_rows_raises(tmp_path):
    path = _write_tsv(tmp_path, rows=[])
    with pytest.raises(SubstrateError, match="zero statute passages"):
        _nodes(StatuteCorpusSource(statutes_tsv=path))


# ---------- malformed input ----------


def test_missing_header_raises(tmp_path):
    path = _write_tsv(
        tmp_path,
        header="citation\tpath\tstate\ttext",  # no idx
        rows=["WI Stat. § 1\tTitle 1\tWI\tSome text."],
    )
    with pytest.raises(SubstrateError, match=r"missing=\['idx'\]"):
        _nodes(StatuteCorpusSource(statutes_tsv=path))


def test_unknown_header_raises(tmp_path):
    path = _write_tsv(
        tmp_path,
        header=_HEADER + "\tbonus",
        rows=[_ROWS[0] + "\textra"],
    )
    with pytest.raises(SubstrateError, match=r"unknown=\['bonus'\]"):
        _nodes(StatuteCorpusSource(statutes_tsv=path))


def test_short_row_raises(tmp_path):
    path = _write_tsv(tmp_path, rows=["WI Stat. § 1\tTitle 1\tWI\tonly four"])
    with pytest.raises(SubstrateError, match="expected 5"):
        _nodes(StatuteCorpusSource(statutes_tsv=path))


def test_non_integer_idx_raises(tmp_path):
    path = _write_tsv(tmp_path, rows=["WI Stat. § 1\tTitle 1\tWI\ttext\tnope"])
    with pytest.raises(SubstrateError, match="non-integer idx"):
        _nodes(StatuteCorpusSource(statutes_tsv=path))


def test_missing_file_and_missing_config_raise(tmp_path):
    with pytest.raises(SubstrateError, match="pass statutes_tsv"):
        StatuteCorpusSource().run("rid")
    with pytest.raises(SubstrateError, match="not found"):
        _nodes(StatuteCorpusSource(statutes_tsv=tmp_path / "nope.tsv"))


# ---------- hierarchy (req.legal.hierarchy) ----------


_HIERARCHY_ROWS = [
    # Two Alaska passages sharing the "Title 34 > Chapter 03" prefix.
    "AK Stat. 34.03.010\tTitle 34 > Chapter 03 > Landlord and Tenant\tAK\t"
    + "A landlord shall keep the premises fit.\t1",
    "AK Stat. 34.03.020\tTitle 34 > Chapter 03 > Rental Agreements\tAK\t"
    + "A rental agreement may not waive.\t2",
    # Wisconsin publishes its OWN "Title 34" — a different body of law.
    "WI Stat. 34.704.010\tTitle 34 > Chapter 704\tWI\t"
    + "A tenancy at will may be terminated.\t3",
    # No released path: a section-orphan, counted rather than fabricated.
    "AK Stat. 09.45.060\t\tAK\tAn action for possession lies.\t4",
]


def test_path_trail_becomes_sections_and_jurisdiction(tmp_path):
    """test.legal.hierarchy — the `path` trail and the `state` column
    become traversable structure, and (state, prefix) keying keeps two
    states' identically-labelled titles apart."""
    path = _write_tsv(tmp_path, rows=_HIERARCHY_ROWS)
    source = StatuteCorpusSource(statutes_tsv=path)
    nodes, edges = _split(source)

    assert len(nodes["statute_passage"]) == 4
    assert len(nodes["statute_section"]) == 6
    assert len(nodes["jurisdiction"]) == 2

    sections = {n.node_id: n for n in nodes["statute_section"]}
    by_trail = {
        (n.attrs["state"], n.attrs["trail"]): n
        for n in nodes["statute_section"]
    }
    assert len(by_trail) == 6

    # (state, prefix) keying: both states publish "Title 34" at depth 0,
    # and they are DIFFERENT nodes.
    ak_root = by_trail[("AK", "Title 34")]
    wi_root = by_trail[("WI", "Title 34")]
    assert ak_root.node_id != wi_root.node_id
    assert ak_root.attrs["label"] == wi_root.attrs["label"] == "Title 34"
    assert ak_root.attrs["depth"] == wi_root.attrs["depth"] == 0

    # Depths are 0-based along the trail.
    deepest = by_trail[
        ("AK", "Title 34 > Chapter 03 > Landlord and Tenant")
    ]
    assert deepest.attrs["depth"] == 2
    assert deepest.attrs["label"] == "Landlord and Tenant"

    part_of = {(e.src, e.dst) for e in edges if e.edge_type == "part_of"}
    in_juris = {
        (e.src, e.dst) for e in edges if e.edge_type == "in_jurisdiction"
    }
    assert len(edges) == 13
    assert len(part_of) == 7        # 3 passage->section + 4 section->section
    assert len(in_juris) == 6       # 4 passages + 2 root sections

    passages = {n.attrs["statute_idx"]: n for n in nodes["statute_passage"]}
    p1 = passages[1].node_id

    # The passage attaches to the DEEPEST prefix of its own trail, not
    # to the title.
    assert (p1, deepest.node_id) in part_of
    assert (p1, ak_root.node_id) not in part_of

    # ... and the parent chain walks back up to the root.
    mid = by_trail[("AK", "Title 34 > Chapter 03")]
    assert (deepest.node_id, mid.node_id) in part_of
    assert (mid.node_id, ak_root.node_id) in part_of
    assert all(
        sections[src].attrs["depth"] > 0
        for src, _ in part_of
        if src in sections
    )

    # Two rows sharing a prefix share the section node.
    sibling = by_trail[("AK", "Title 34 > Chapter 03 > Rental Agreements")]
    assert (sibling.node_id, mid.node_id) in part_of

    # Every passage reaches its jurisdiction; root sections do too.
    ak_juris = next(
        n.node_id for n in nodes["jurisdiction"] if n.attrs["code"] == "AK"
    )
    wi_juris = next(
        n.node_id for n in nodes["jurisdiction"] if n.attrs["code"] == "WI"
    )
    assert all((p.node_id, ak_juris) in in_juris or p.attrs["state"] == "WI"
               for p in nodes["statute_passage"])
    assert (ak_root.node_id, ak_juris) in in_juris
    assert (wi_root.node_id, wi_juris) in in_juris
    assert (mid.node_id, ak_juris) not in in_juris  # roots only

    # The empty-path row: passage + jurisdiction edge, NO section parent,
    # counted and reported.
    orphan = passages[4].node_id
    assert (orphan, ak_juris) in in_juris
    assert not [dst for src, dst in part_of if src == orphan]
    stats = source.last_run_stats
    assert stats["passages_without_path"] == 1
    assert stats["section_orphan_statute_idx"] == [4]
    assert stats["sections_emitted"] == 6
    assert stats["jurisdictions_emitted"] == 2
    assert stats["passage_part_of_edges"] == 3
    assert stats["section_part_of_edges"] == 4
    assert stats["section_in_jurisdiction_edges"] == 2


def test_unparsed_path_separator_raises_rather_than_flattening(tmp_path):
    """A slice whose `path` uses another separator would collapse every
    trail into one flat section — refused, not guessed."""
    rows = [
        "AK Stat. 34.03.010\tTitle 34 / Chapter 03 / Landlord\tAK\tText.\t1",
        "AK Stat. 34.03.020\tTitle 34 / Chapter 03 / Rentals\tAK\tText.\t2",
    ]
    path = _write_tsv(tmp_path, rows=rows)
    with pytest.raises(SubstrateError, match="different separator"):
        _facts(StatuteCorpusSource(statutes_tsv=path))
    # Naming the separator the slice actually uses parses it.
    nodes, _ = _split(
        StatuteCorpusSource(statutes_tsv=path, path_separator="/")
    )
    assert len(nodes["statute_section"]) == 4


# ---------- cross-references (req.legal.crossref) ----------


_CROSSREF_ROWS = [
    "AK Stat. 34.03.010\tTitle 34 > Chapter 03\tAK\t"
    + "A landlord shall comply with sec. 34.03.020, as provided in "
    + "sec. 34.03.010, and with sec. 34.03.999.\t10",
    "AK Stat. 34.03.020\tTitle 34 > Chapter 03\tAK\t"
    + "This section is subject to sec. 34.03.010.\t11",
]


def test_cross_reference_enricher_skips_out_of_slice_targets(tmp_path):
    """test.legal.crossref — in-slice citations become edges; an
    out-of-slice one is counted and kept as an attribute with NO stub
    node and NO dangling edge; a self-citation emits no self-edge; and a
    slice large enough to contain references that resolves NONE fails
    the build."""
    path = _write_tsv(tmp_path, rows=_CROSSREF_ROWS)
    source = StatuteCorpusSource(statutes_tsv=path)
    nodes, edges = _split(source)

    node_ids = {n.node_id for n in nodes["statute_passage"]}
    assert node_ids == {
        "statute_passage:reglab/housing_qa:10",
        "statute_passage:reglab/housing_qa:11",
    }
    citing = "statute_passage:reglab/housing_qa:10"
    cited = "statute_passage:reglab/housing_qa:11"

    xrefs = {
        (e.src, e.dst) for e in edges if e.edge_type == "cross_references"
    }
    assert xrefs == {(citing, cited), (cited, citing)}
    # No self-edge, even though the passage's text names its own section.
    assert not [e for e in edges if e.src == e.dst]

    # The out-of-slice reference produced NO stub node ...
    assert len(nodes["statute_passage"]) == 2
    assert "34.03.999" not in " ".join(node_ids)
    # ... and NO dangling edge: every endpoint is an emitted node.
    all_ids = {n.node_id for group in nodes.values() for n in group}
    assert all(e.src in all_ids and e.dst in all_ids for e in edges)

    # ... it is kept as an attribute on the citing passage, and counted.
    by_id = {n.node_id: n for n in nodes["statute_passage"]}
    assert by_id[citing].attrs["unresolved_references"] == ["sec. 34.03.999"]
    assert by_id[cited].attrs["unresolved_references"] == []

    stats = source.last_run_stats
    assert stats["cross_references_resolved"] == 2
    assert stats["cross_references_unresolved"] == 1
    assert stats["cross_references_self_skipped"] == 1

    # A slice big enough to contain references that resolves ZERO fails:
    # "no matches" must not masquerade as "no cross-references exist".
    big = [
        f"AK Stat. 34.03.{n:03d}\tTitle 34 > Chapter 03\tAK\t"
        f"See sec. 99.99.999 for the definition.\t{100 + n}"
        for n in range(1, 31)
    ]
    big_path = _write_tsv(tmp_path, rows=big, name="big.tsv")
    with pytest.raises(SubstrateError, match="resolved ZERO cross-references"):
        _facts(StatuteCorpusSource(statutes_tsv=big_path))

    # The contract's negative-control build (enricher off) is the only
    # way to get that corpus without the refusal — and it emits no
    # cross_references edge, which is what the contract then fails on.
    _, off_edges = _split(
        StatuteCorpusSource(statutes_tsv=big_path, cross_references=False)
    )
    assert not [e for e in off_edges if e.edge_type == "cross_references"]


# ---------- legal module composition ----------


def test_legal_module_composes(tmp_path):
    """A deployment manifest listing module 'legal' composes a catalog
    admitting the three subtypes the source emits and the five
    narrowings its edges need (same harness as
    test_cti_module_composes)."""
    deployment = tmp_path / "scratch"
    deployment.mkdir()
    (deployment / "deployment.yaml").write_text(
        yaml.safe_dump(
            {"name": "scratch", "schema_dir": "schemas", "modules": ["legal"]}
        )
    )
    (deployment / "schemas").mkdir()

    composed = compose_catalog(deployment)

    for subtype in ("statute_passage", "statute_section", "jurisdiction"):
        assert subtype in composed.subtypes, sorted(composed.subtypes)
        assert composed.subtype_origin[subtype] == "legal"
    assert {
        (r.parent_subtype, r.edge_archetype, r.child_subtype)
        for r in composed.narrowings if r.origin == "legal"
    } == {
        ("statute_passage", "part_of", "statute_section"),
        ("statute_section", "part_of", "statute_section"),
        ("statute_passage", "in_jurisdiction", "jurisdiction"),
        ("statute_section", "in_jurisdiction", "jurisdiction"),
        ("statute_passage", "cross_references", "statute_passage"),
    }
