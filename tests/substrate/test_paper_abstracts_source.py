"""req.dom.scifact + okg-corpus-literature: PaperAbstractsSource.

Synthetic snapshot fixtures only — the adapter has NO live network
path, so every test writes local files and asserts on the emitted
fact stream: paper node shapes, the ~4000-char truncation flag,
malformed-line and zero-paper refusals, and the literature ontology
module composing the subtypes the source emits.

req.lit.sentence adds the answer unit: one `abstract_sentence` node
per element of the RELEASED abstract array, with stable ids and a
`paper -contains-> abstract_sentence` edge each.

req.lit.structure adds the derived concept relation: `concept` nodes
banded by paper document frequency, `abstract_sentence -mentions->
concept`, and `paper -shares_concept-> paper` — computed from corpus
text ALONE, never from claims or gold labels.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from okg.substrate.catalog.modules_compose import compose_catalog
from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.library.sources.paper_abstracts import (
    ABSTRACT_MAX_CHARS,
    MAX_CONCEPTS_PER_SENTENCE,
    PaperAbstractsSource,
)

REPO = "allenai/scifact"


def _paper(doc_id, title="A study of things", sentences=None, **extra) -> dict:
    if sentences is None:
        sentences = ["First sentence.", "Second sentence.", "Third sentence."]
    return {"doc_id": doc_id, "title": title, "abstract": sentences, **extra}


def _write_corpus(path: Path, rows: list[dict]) -> Path:
    path.write_text(
        "\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8"
    )
    return path


def _run_facts(path: Path) -> list[NodeFact | EdgeFact]:
    run = PaperAbstractsSource(repo=REPO, corpus_jsonl=path).run(
        "rid", mode="scope_complete"
    )
    facts = list(run.facts)
    assert run.completed_scope is True
    return facts


def _nodes(facts, subtype: str) -> list[NodeFact]:
    return [
        f for f in facts
        if isinstance(f, NodeFact) and f.subtype == subtype
    ]


def _edges(facts, edge_type: str) -> list[EdgeFact]:
    return [
        f for f in facts
        if isinstance(f, EdgeFact) and f.edge_type == edge_type
    ]


def _run_nodes(path: Path) -> list[NodeFact]:
    """Paper nodes only — the shape the pre-sentence tests assert on."""
    return _nodes(_run_facts(path), "paper")


def test_emits_paper_nodes_with_declared_shape(tmp_path):
    corpus = _write_corpus(
        tmp_path / "corpus.jsonl",
        [
            _paper(4983, structured=False),
            _paper(101, title="Structured one", sentences=["Only sentence."],
                   structured=True),
        ],
    )
    nodes = _run_nodes(corpus)
    assert len(nodes) == 2
    by_doc = {n.attrs["doc_id"]: n for n in nodes}

    first = by_doc["4983"]
    assert first.node_id == f"paper:{REPO}:4983"
    assert first.attrs["title"] == "A study of things"
    assert first.attrs["sentence_count"] == 3
    assert first.attrs["abstract"] == (
        "First sentence. Second sentence. Third sentence."
    )
    assert first.attrs["truncated"] is False
    assert first.attrs["structured"] is False
    assert first.source_record_id == {"repo": REPO, "doc_id": "4983"}
    assert first.source_revision == {"repo": REPO, "corpus": "corpus.jsonl"}

    second = by_doc["101"]
    assert second.attrs["sentence_count"] == 1
    assert second.attrs["structured"] is True


def test_record_set_covers_every_paper(tmp_path):
    """A paper's record holds the paper, its sentences, and the edges
    it originates; concepts are corpus-level and carry their own."""
    corpus = _write_corpus(
        tmp_path / "corpus.jsonl", [_paper(1), _paper(2)]
    )
    run = PaperAbstractsSource(repo=REPO, corpus_jsonl=corpus).run(
        "rid", mode="scope_complete"
    )
    facts = list(run.facts)
    assert run.record_set is not None
    records = run.record_set.records

    paper_keys = [k for k in records if k.startswith("paper:")]
    assert sorted(paper_keys) == [f"paper:{REPO}:1", f"paper:{REPO}:2"]

    emission = records[f"paper:{REPO}:1"]
    assert emission.primary is not None
    assert emission.primary.subtype == "paper"
    assert [d.node_id for d in emission.descendants] == [
        f"sentence:{REPO}:1:{i}" for i in range(3)
    ]
    # Every edge the source emitted lands in some record.
    declared = {
        edge.identity()
        for rec in records.values()
        for edge in rec.edges
    }
    assert declared == {
        (e.src, e.edge_type, e.dst)
        for e in facts if isinstance(e, EdgeFact)
    }
    # Concepts are their own records, not descendants of a paper.
    concept_ids = {n.node_id for n in _nodes(facts, "concept")}
    assert concept_ids <= set(records)
    for concept_id in concept_ids:
        assert records[concept_id].primary is not None
        assert records[concept_id].descendants == []


def test_long_abstract_truncates_with_flag(tmp_path):
    sentences = ["word " * 200] * 10  # joined length far beyond the bound
    corpus = _write_corpus(
        tmp_path / "corpus.jsonl", [_paper(7, sentences=sentences)]
    )
    [node] = _run_nodes(corpus)
    assert node.attrs["truncated"] is True
    assert len(node.attrs["abstract"]) == ABSTRACT_MAX_CHARS
    assert node.attrs["sentence_count"] == 10


def test_malformed_line_raises_with_line_number(tmp_path):
    corpus = tmp_path / "corpus.jsonl"
    corpus.write_text(
        json.dumps(_paper(1)) + "\nnot json at all\n", encoding="utf-8"
    )
    with pytest.raises(SubstrateError, match="line 2"):
        _run_nodes(corpus)

    _write_corpus(
        corpus, [_paper(1), {"doc_id": 2, "title": "no abstract"}]
    )
    with pytest.raises(SubstrateError, match="line 2.*abstract"):
        _run_nodes(corpus)

    _write_corpus(corpus, [_paper(1), {"title": "t", "abstract": []}])
    with pytest.raises(SubstrateError, match="line 2.*doc_id"):
        _run_nodes(corpus)

    _write_corpus(corpus, [_paper(3), _paper(3)])
    with pytest.raises(SubstrateError, match="line 2.*doc_id '3'"):
        _run_nodes(corpus)


def test_zero_papers_raises(tmp_path):
    corpus = tmp_path / "corpus.jsonl"
    corpus.write_text("\n", encoding="utf-8")
    with pytest.raises(SubstrateError, match="zero papers"):
        _run_nodes(corpus)


def test_missing_config_and_missing_file_raise(tmp_path):
    with pytest.raises(SubstrateError, match="pass repo and corpus_jsonl"):
        PaperAbstractsSource(repo=REPO).run("rid")
    with pytest.raises(SubstrateError, match="not found"):
        PaperAbstractsSource(
            repo=REPO, corpus_jsonl=tmp_path / "nope.jsonl"
        ).run("rid")


# ---------- req.lit.sentence: the sentence is a first-class node ----------


def test_sentence_nodes_and_order(tmp_path):
    """test.lit.sentence — one node per RELEASED array element, in
    array order, with stable ids and one containment edge each."""
    released = [
        "Sentence zero states the background.",
        "Sentence one. Which contains a period-looking split point.",
        "Sentence two reports the finding.",
    ]
    corpus = _write_corpus(
        tmp_path / "corpus.jsonl",
        [_paper(4983, sentences=released), _paper(101)],
    )
    facts = _run_facts(corpus)
    sentences = [
        n for n in _nodes(facts, "abstract_sentence")
        if n.attrs["doc_id"] == "4983"
    ]

    assert [n.attrs["sentence_index"] for n in sentences] == [0, 1, 2]
    # Ordering is the released array's — NOT a re-split of the joined
    # blob, which would have produced four sentences here.
    assert [n.attrs["text"] for n in sentences] == released
    assert [n.node_id for n in sentences] == [
        f"sentence:{REPO}:4983:0",
        f"sentence:{REPO}:4983:1",
        f"sentence:{REPO}:4983:2",
    ]
    assert all(n.attrs["truncated"] is False for n in sentences)
    assert sentences[0].source_record_id == {
        "repo": REPO, "doc_id": "4983", "sentence_index": 0,
    }

    contains = [
        e for e in _edges(facts, "contains")
        if e.src == f"paper:{REPO}:4983"
    ]
    assert [e.dst for e in contains] == [n.node_id for n in sentences]
    assert [e.attrs["sentence_index"] for e in contains] == [0, 1, 2]
    assert all(e.provenance == "explicit" for e in contains)

    # The paper still carries its own declared shape.
    [paper] = [
        n for n in _nodes(facts, "paper") if n.attrs["doc_id"] == "4983"
    ]
    assert paper.attrs["sentence_count"] == 3

    # Ids are stable across runs over the same bytes.
    again = _run_facts(corpus)
    assert [
        n.node_id for n in _nodes(again, "abstract_sentence")
    ] == [n.node_id for n in _nodes(facts, "abstract_sentence")]


def test_fourteen_sentence_abstract_emits_fourteen_of_each(tmp_path):
    """The task's stated shape: a 14-sentence abstract yields indices
    0..13 and 14 containment edges."""
    released = [f"Sentence number {i} of the abstract." for i in range(14)]
    corpus = _write_corpus(
        tmp_path / "corpus.jsonl", [_paper(9, sentences=released)]
    )
    facts = _run_facts(corpus)
    sentences = _nodes(facts, "abstract_sentence")
    assert [n.attrs["sentence_index"] for n in sentences] == list(range(14))
    assert len(_edges(facts, "contains")) == 14


def test_empty_abstract_raises(tmp_path):
    """A paper with no sentences is a corpus defect, not a paper with
    an empty attribute — it must fail loudly rather than publish a
    node the gold can address but the graph cannot answer for."""
    corpus = _write_corpus(
        tmp_path / "corpus.jsonl",
        [_paper(1), {"doc_id": 2, "title": "Empty", "abstract": []}],
    )
    with pytest.raises(SubstrateError, match="line 2.*non-empty list"):
        _run_facts(corpus)


# ---------- req.lit.structure: the derived concept relation ----------


def _concept_corpus(tmp_path: Path, *, extra_files: bool = False) -> Path:
    """Three papers sharing `mitochondrial` and `apoptosis`.

    With three papers the band ceiling is max(2, ceil(0.02*3)) = 2, so
    a concept must appear in EXACTLY two papers here: terms in all
    three (`cells`) are corpus-generic, terms in one are unshared, and
    the stopword `these` sits in two but is filtered.
    """
    rows = [
        _paper(
            1,
            title="Paper one",
            sentences=[
                "Mitochondrial cells undergo apoptosis.",
                "Unique zebrafish observation here.",
            ],
        ),
        _paper(
            2,
            title="Paper two",
            sentences=["These mitochondrial cells resist apoptosis."],
        ),
        _paper(
            3,
            title="Paper three",
            sentences=["These unrelated cells proliferate rapidly."],
        ),
    ]
    tmp_path.mkdir(parents=True, exist_ok=True)
    corpus = _write_corpus(tmp_path / "corpus.jsonl", rows)
    if extra_files:
        # A claims/gold file sitting in the same directory. The adapter
        # reads corpus_jsonl and nothing else; if it ever widened its
        # input this fixture would change the answer.
        (tmp_path / "claims_train.jsonl").write_text(
            json.dumps({
                "id": 1,
                "claim": "Mitochondrial cells resist apoptosis.",
                "evidence": {"2": [{"sentences": [0], "label": "SUPPORT"}]},
                "cited_doc_ids": [1, 2],
            }) + "\n",
            encoding="utf-8",
        )
        (tmp_path / "gold.json").write_text(
            json.dumps({"1": ["sentence:allenai/scifact:2:0"]}),
            encoding="utf-8",
        )
    return corpus


def test_concept_enricher_uses_corpus_text_only(tmp_path):
    """test.lit.structure — concepts derive from abstract text alone;
    a claims-like file in the input directory changes nothing."""
    without = _run_facts(_concept_corpus(tmp_path / "a"))
    with_claims = _run_facts(
        _concept_corpus(tmp_path / "b", extra_files=True)
    )

    def _summary(facts):
        return (
            sorted(n.attrs["term"] for n in _nodes(facts, "concept")),
            sorted(
                (e.src, e.dst) for e in _edges(facts, "mentions")
            ),
            sorted(
                (e.src, e.dst, e.attrs["shared_concept_count"])
                for e in _edges(facts, "shares_concept")
            ),
        )

    assert _summary(without) == _summary(with_claims)

    terms = {
        n.attrs["term"]: n.attrs["document_frequency"]
        for n in _nodes(without, "concept")
    }
    # In exactly two papers -> inside the band.
    assert terms == {"apoptosis": 2, "mitochondrial": 2}
    # In all three papers -> above the 2%-of-corpus ceiling.
    assert "cells" not in terms
    # In one paper -> below the floor; links nothing.
    assert "zebrafish" not in terms
    # In two papers, but a stopword -> filtered before counting.
    assert "these" not in terms

    for node in _nodes(without, "concept"):
        assert node.node_id == f"concept:{REPO}:{node.attrs['term']}"

    mentions = _edges(without, "mentions")
    assert all(e.provenance == "derived_deterministic" for e in mentions)
    assert {(e.src, e.dst) for e in mentions} == {
        (f"sentence:{REPO}:1:0", f"concept:{REPO}:apoptosis"),
        (f"sentence:{REPO}:1:0", f"concept:{REPO}:mitochondrial"),
        (f"sentence:{REPO}:2:0", f"concept:{REPO}:apoptosis"),
        (f"sentence:{REPO}:2:0", f"concept:{REPO}:mitochondrial"),
    }


def test_mentions_are_capped_at_the_rarest_concepts(tmp_path):
    """One sentence cannot dominate the edge budget: the cap keeps the
    rarest concepts, ties broken by term string."""
    # 12 terms shared by exactly two papers each -> 12 concepts, all in
    # one sentence of paper 0.
    shared = [f"term{i:02d}aa" for i in range(12)]
    rows = [_paper(0, sentences=[" ".join(shared) + "."])]
    for i, term in enumerate(shared, start=1):
        rows.append(_paper(i, sentences=[f"{term} appears again here."]))
    corpus = _write_corpus(tmp_path / "corpus.jsonl", rows)

    facts = _run_facts(corpus)
    assert len(_nodes(facts, "concept")) == 12
    first_sentence = f"sentence:{REPO}:0:0"
    capped = [e for e in _edges(facts, "mentions") if e.src == first_sentence]
    assert len(capped) == MAX_CONCEPTS_PER_SENTENCE
    # All twelve tie at document_frequency 2, so the tie-break by term
    # string picks the first eight.
    assert [e.dst for e in capped] == [
        f"concept:{REPO}:{term}" for term in sorted(shared)[:8]
    ]


def test_shares_concept_is_deduped_and_pair_ordered(tmp_path):
    """Papers sharing a concept are linked once, in node-id order,
    with the count of concepts they share."""
    rows = [
        _paper(1, sentences=["Mitochondrial apoptosis observed clearly."]),
        _paper(2, sentences=["Mitochondrial apoptosis observed rarely."]),
        _paper(3, sentences=["Unrelated proliferation reported instead."]),
    ]
    corpus = _write_corpus(tmp_path / "corpus.jsonl", rows)
    facts = _run_facts(corpus)

    shares = _edges(facts, "shares_concept")
    pairs = [(e.src, e.dst) for e in shares]
    assert pairs == [(f"paper:{REPO}:1", f"paper:{REPO}:2")]
    assert len(pairs) == len(set(pairs))
    assert all(e.src < e.dst for e in shares)
    # mitochondrial + apoptosis + observed = three shared concepts.
    assert shares[0].attrs["shared_concept_count"] == 3
    assert shares[0].provenance == "derived_deterministic"
    assert shares[0].source_record_id == {"repo": REPO, "doc_id": "1"}


def test_single_paper_corpus_emits_no_derived_edges(tmp_path):
    """No silent invention: one paper shares nothing, so the derived
    relation is empty rather than self-linked."""
    corpus = _write_corpus(tmp_path / "corpus.jsonl", [_paper(1)])
    facts = _run_facts(corpus)
    assert _nodes(facts, "concept") == []
    assert _edges(facts, "mentions") == []
    assert _edges(facts, "shares_concept") == []
    assert len(_edges(facts, "contains")) == 3


# ---------- literature module composition ----------


def test_literature_module_composes(tmp_path):
    """A deployment manifest listing module 'literature' composes a
    catalog admitting the three subtypes and exactly the three
    narrowings the source emits (same harness as
    test_cti_module_composes)."""
    deployment = tmp_path / "scratch"
    deployment.mkdir()
    (deployment / "deployment.yaml").write_text(
        yaml.safe_dump(
            {"name": "scratch", "schema_dir": "schemas",
             "modules": ["literature"]}
        )
    )
    (deployment / "schemas").mkdir()

    composed = compose_catalog(deployment)

    for subtype in ("paper", "abstract_sentence", "concept"):
        assert subtype in composed.subtypes, sorted(composed.subtypes)
        assert composed.subtype_origin[subtype] == "literature"

    triples = {
        (r.parent_subtype, r.edge_archetype, r.child_subtype)
        for r in composed.narrowings if r.origin == "literature"
    }
    assert triples == {
        ("paper", "contains", "abstract_sentence"),
        ("abstract_sentence", "mentions", "concept"),
        ("paper", "shares_concept", "paper"),
    }
