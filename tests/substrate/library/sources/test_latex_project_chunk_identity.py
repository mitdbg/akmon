"""Cross-adapter `document_chunk` identity contract.

`LatexProjectSource` never creates `document_chunk` nodes: it points
`paper_section --contains-->` and `citation --references-->` edges at the
chunks `DocCorpusSource` creates from the same LaTeX bytes. When the two
computed different ids the edges were rejected at admission — the destination
node did not exist, so its subtype could not be resolved and the producer
triple could not match. These tests pin the two adapters to one identity.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate.library.extraction.chunk.latex_structural import (
    LatexStructuralChunker,
)
from okg.substrate.library.extraction.table_grid import table_occurrence_id
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.library.sources.doc_corpus import DocCorpusSource
from okg.substrate.library.sources.scholarly.latex_project import LatexProjectSource
from okg.substrate.library.sources.scholarly.latex_project_manifest import (
    LatexProjectManifestBuilder,
)


_EXTRACTORS = """\
parser:
  - format: latex
    class: okg.substrate.library.extraction.parse.latex.LatexParser
chunker:
  - format: latex
    class: okg.substrate.library.extraction.chunk.latex_structural.LatexStructuralChunker
ner: []
"""

_PAPER_TEX = r"""\documentclass{article}
\addbibresource{refs.bib}
\begin{document}
\section{Evidence}

Prose citing \citep{alpha} before the table.

\begin{table}[t]
\caption{Systems compared against \cite{beta}.}
\begin{tabular}{ll}
System & Result \\
A & 1 \\
\end{tabular}
\end{table}

Prose after the table.
\end{document}
"""

_REFS_BIB = "\n".join(
    f"@article{{{key},title={{{key.title()}}},doi={{10.1/{key}}}}}"
    for key in ("alpha", "beta")
)


def _corpus(tmp_path: Path) -> Path:
    root = tmp_path / "papers"
    (root / "paper").mkdir(parents=True)
    (root / "paper" / "main.tex").write_text(_PAPER_TEX, encoding="utf-8")
    (root / "paper" / "refs.bib").write_text(_REFS_BIB, encoding="utf-8")
    return root


def _doc_corpus_chunks(tmp_path: Path, root: Path) -> list[tuple[str, str]]:
    """Run the adapter that actually creates the nodes; list (chunk_kind, id)."""
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    (extractors / "extractors.yaml").write_text(_EXTRACTORS)
    adapter = DocCorpusSource(
        repo="papers",
        repo_root=root, non_git_walk=True,
        extractors_dir=extractors,
        include_globs=("**/*.tex",),
    )
    facts = list(adapter.run("run-doc-corpus", mode="scope_complete").facts)
    return [
        (fact.attrs["chunk_kind"], fact.node_id)
        for fact in facts
        if isinstance(fact, NodeFact) and fact.subtype == "document_chunk"
    ]


def _latex_project_facts(root: Path) -> tuple[list[NodeFact], list[EdgeFact]]:
    manifest = LatexProjectManifestBuilder(
        source_root=root,
        repository_authority="papers",
        projects=[{"project_dir": "paper", "entrypoints": ["main.tex"]}],
    ).build()
    emitted = list(
        LatexProjectSource(root, manifest).run("run-latex-project", mode="scope_complete").facts
    )
    return (
        [fact for fact in emitted if isinstance(fact, NodeFact)],
        [fact for fact in emitted if isinstance(fact, EdgeFact)],
    )


def test_latex_project_chunk_edges_land_on_doc_corpus_chunk_nodes(
    tmp_path: Path,
) -> None:
    root = _corpus(tmp_path)
    created = _doc_corpus_chunks(tmp_path, root)
    created_ids = {node_id for _, node_id in created}
    nodes, edges = _latex_project_facts(root)

    referenced = {
        edge.dst
        for edge in edges
        if edge.dst.startswith("chunk:")
        and edge.edge_type in {"contains", "references"}
    }
    assert referenced, "latex_project emitted no chunk-bearing edges"
    # Every destination must be a chunk DocCorpusSource actually created.
    assert referenced <= created_ids, {
        "dangling": sorted(referenced - created_ids),
        "created": sorted(created_ids),
    }

    # The table is the case that regressed: occurrence identity, not content
    # hash. Assert it is exercised so this test cannot pass vacuously.
    table_ids = [node_id for kind, node_id in created if kind == "table"]
    assert len(table_ids) == 1
    assert table_ids[0] in referenced
    table_chunk_id = table_ids[0]

    # Citations inside the table point at the table chunk, by its real id.
    table_citation = next(
        node
        for node in nodes
        if node.subtype == "citation" and node.attrs["normalized_key"] == "beta"
    )
    assert table_citation.attrs["canonical_chunk_ref"] == table_chunk_id


def test_table_chunk_edges_use_source_occurrence_identity(tmp_path: Path) -> None:
    root = _corpus(tmp_path)
    _, edges = _latex_project_facts(root)

    table_chunk = next(
        chunk
        for chunk in LatexStructuralChunker().chunk(_PAPER_TEX)
        if chunk.chunk_kind == "table"
    )
    expected = table_occurrence_id(
        "doc:papers:paper/main.tex",
        table_chunk.extras["source_locator"],
    )
    assert expected.startswith("chunk:table:")

    table_edges = [edge for edge in edges if edge.dst == expected]
    assert [edge.edge_type for edge in table_edges] == ["contains", "references"]
    for edge in table_edges:
        assert edge.attrs["document_ref"] == "doc:papers:paper/main.tex"


@pytest.mark.parametrize("chunk_kind", ["paragraph", "table"])
def test_every_emitted_chunk_kind_is_reachable_from_a_section(
    tmp_path: Path,
    chunk_kind: str,
) -> None:
    root = _corpus(tmp_path)
    created = _doc_corpus_chunks(tmp_path, root)
    wanted = [node_id for kind, node_id in created if kind == chunk_kind]
    assert wanted, f"fixture produced no {chunk_kind} chunk"
    _, edges = _latex_project_facts(root)
    section_contains = {
        edge.dst
        for edge in edges
        if edge.edge_type == "contains" and edge.src.startswith("section:")
    }
    assert set(wanted) <= section_contains
