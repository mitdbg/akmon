"""Shared fixture for the passage→document rollup tests.

Mirrors the erag-seg shape that motivates the rollup, at fixture scale:

  * `doc` is a LOCATOR subtype — every searchable field is also a path field,
    so its `attrs.text` is a rendering of its own path and a self-ranked
    search over it is a filename search.
  * `chunk` is a CONTENT subtype carrying the body text.
  * `figure` is a second child reachable by the SAME `contains` edge, which is
    why the relation cannot be inferred: a rollup that guessed `contains`
    without the declared child subtype would fold figures into a document's
    relevance.
  * four narrowings between doc and chunk, exactly as the pinned erag-seg
    catalog declares them.
"""

from __future__ import annotations

from typing import Any

from okg.substrate.library.sources.base import EdgeFact, NodeFact
from tests.pytest.catalog_fixture_builders import SubtypeSpec


#: The word under test. It appears in every chunk BODY and in every doc PATH,
#: so both a self-ranked and a rolled-up search match — which is what makes the
#: comparison between them meaningful rather than a match/no-match artifact.
TERM = "hafnium"

ROLLUP_METADATA = {"edge_type": "contains", "child_subtype": "chunk"}

DOC = SubtypeSpec(
    "doc",
    metadata={
        "searchable_text_fields": ["path"],
        "searchable_text_path_fields": ["path"],
        "passage_rollup": ROLLUP_METADATA,
    },
)
#: Identical to DOC except that it declares NO passage rollup. The P0 negative
#: test rolls up through this one.
UNDECLARED_DOC = SubtypeSpec(
    "undeclared_doc",
    metadata={
        "searchable_text_fields": ["path"],
        "searchable_text_path_fields": ["path"],
    },
)
CHUNK = SubtypeSpec("chunk", metadata={"searchable_text_fields": ["text"]})
FIGURE = SubtypeSpec("figure", metadata={"searchable_text_fields": ["text"]})

SUBTYPES = [DOC, UNDECLARED_DOC, CHUNK, FIGURE]

#: The four candidate relations, as erag-seg declares them, plus the same set
#: for the undeclared parent so the negative test differs ONLY in the
#: declaration and not in the available structure.
NARROWINGS = [
    ("doc", "contains", "chunk"),
    ("doc", "contains", "figure"),
    ("chunk", "member_of", "doc"),
    ("chunk", "references", "doc"),
    ("undeclared_doc", "contains", "chunk"),
]


def _fact(node_id: str, subtype: str, attrs: dict[str, Any]) -> NodeFact:
    return NodeFact(
        node_id=node_id,
        subtype=subtype,
        attrs=attrs,
        source_record_id={"id": node_id},
        source_revision={"v": 1},
    )


def _edge(src: str, dst: str, edge_type: str) -> EdgeFact:
    return EdgeFact(
        src=src,
        dst=dst,
        edge_type=edge_type,
        source_record_id={"id": f"{src}|{edge_type}|{dst}"},
        source_revision={"v": 1},
    )


#: (doc_id, [chunk body repetitions]) — a doc whose single chunk is a strong
#: match, and a doc with three moderate ones, so `max` and `sum_top3` have
#: something to disagree about.
CORPUS: list[tuple[str, list[int]]] = [
    # The sharp document also has one weak passage. Authority tests deny its
    # strongest passage and prove the readable sibling, rather than the hidden
    # score, determines the visible parent's rollup.
    ("doc:sharp", [9, 1]),
    ("doc:broad", [3, 3, 3]),
    ("doc:thin", [1]),
    ("doc:quiet", [1, 1]),
]


def build_facts() -> list[Any]:
    """Nodes + edges for the fixture corpus."""
    facts: list[Any] = []
    for doc_id, weights in CORPUS:
        path = f"corpus/{doc_id.split(':')[1]}-{TERM}-report.txt"
        facts.append(_fact(doc_id, DOC.name, {"path": path}))
        for i, weight in enumerate(weights):
            chunk_id = f"{doc_id}:chunk:{i}"
            body = " ".join([TERM] * weight)
            facts.append(
                _fact(
                    chunk_id,
                    CHUNK.name,
                    {"text": f"The assay records {body} cladding tolerances."},
                )
            )
            facts.append(_edge(doc_id, chunk_id, "contains"))
            facts.append(_edge(chunk_id, doc_id, "member_of"))
    # A figure hanging off `contains` from the sharpest doc, carrying the term
    # emphatically. It must NEVER contribute to a rollup: the declaration names
    # `chunk`, and a rollup that followed the edge type alone would fold this
    # caption into the document's relevance.
    facts.append(
        _fact(
            "fig:loud", FIGURE.name, {"text": " ".join([TERM] * 30) + " figure caption"}
        )
    )
    facts.append(_edge("doc:thin", "fig:loud", "contains"))
    # A doc of the UNDECLARED parent subtype with real chunks underneath, so
    # the P0 negative test refuses a rollup that would otherwise have worked.
    facts.append(
        _fact(
            "undoc:1",
            UNDECLARED_DOC.name,
            {"path": f"corpus/undeclared-{TERM}-report.txt"},
        )
    )
    facts.append(
        _fact(
            "undoc:1:chunk:0",
            CHUNK.name,
            {"text": f"The undeclared parent discusses {TERM} at length."},
        )
    )
    facts.append(_edge("undoc:1", "undoc:1:chunk:0", "contains"))
    return facts


#: Chunks per doc, by the declared relation only (figures excluded).
EXPECTED_CHUNK_COUNTS = {doc_id: len(weights) for doc_id, weights in CORPUS}
