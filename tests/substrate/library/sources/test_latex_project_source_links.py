from __future__ import annotations

from pathlib import Path

from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.library.sources.doc_corpus import DocCorpusSource
from okg.substrate.library.sources.scholarly.latex_project import LatexProjectSource
from okg.substrate.library.sources.scholarly.latex_project_manifest import (
    LatexProjectManifestBuilder,
)


def _write(root: Path, rel: str, content: str | bytes) -> None:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(content, bytes):
        path.write_bytes(content)
    else:
        path.write_text(content, encoding="utf-8")


def _manifest(root: Path):
    return LatexProjectManifestBuilder(
        source_root=root,
        repository_authority="papers",
        projects=[{"project_dir": "paper", "entrypoints": ["main.tex"]}],
    ).build()


def test_project_links_reuse_document_chunk_visual_and_paper_endpoints(
    tmp_path: Path,
) -> None:
    _write(
        tmp_path,
        "paper/main.tex",
        r"""\documentclass{article}
\addbibresource{refs/library.bib}
\begin{document}
\section{Introduction}
Root text \cite{Known}.
\input{parts/body}
\input{parts/body}
\includegraphics{figures/plot.png}
\end{document}
""",
    )
    _write(tmp_path, "paper/parts/body.tex", "Included manuscript text.\n")
    _write(
        tmp_path,
        "paper/refs/library.bib",
        "@article{Known,title={Known Work},doi={10.1000/KNOWN}}\n",
    )
    _write(tmp_path, "paper/figures/plot.png", b"\x89PNG\r\nfixture")
    manifest = _manifest(tmp_path)
    project = manifest.projects[0]

    run = LatexProjectSource(tmp_path, manifest).run("run", mode="scope_complete")
    facts = list(run.facts)
    nodes = [fact for fact in facts if isinstance(fact, NodeFact)]
    edges = [fact for fact in facts if isinstance(fact, EdgeFact)]

    root_doc = "doc:papers:paper/main.tex"
    member_doc = "doc:papers:paper/parts/body.tex"
    figure = next(
        item for item in project.files if item.path == "paper/figures/plot.png"
    )
    visual_id = f"visual:{figure.content_sha256[:16]}"
    assert any(
        edge.src == project.paper_id
        and edge.dst == root_doc
        and edge.edge_type == "derived_from"
        for edge in edges
    )

    member_edge = next(
        edge for edge in edges
        if edge.src == project.paper_id
        and edge.dst == member_doc
        and edge.edge_type == "references"
    )
    assert member_edge.attrs["latex_roles"] == ["input"]
    assert member_edge.attrs["latex_commands"] == ["input"]
    assert len(member_edge.attrs["latex_occurrences"]) == 2
    assert member_edge.attrs["latex_occurrences"] == sorted(
        member_edge.attrs["latex_occurrences"],
        key=lambda item: (
            item["source_path"], item["start"], item["end"],
            item["role"], item["resolved_path"] or "",
        ),
    )
    assert any(
        edge.src == project.paper_id
        and edge.dst == visual_id
        and edge.edge_type == "references"
        for edge in edges
    )

    citation = next(node for node in nodes if node.subtype == "citation")
    section = next(
        node for node in nodes
        if node.subtype == "paper_section" and node.attrs["title"] == "Introduction"
    )
    assert any(
        edge.src == root_doc
        and edge.dst == citation.node_id
        and edge.edge_type == "contains"
        for edge in edges
    )
    assert any(
        edge.src == section.node_id
        and edge.dst == citation.node_id
        and edge.edge_type == "contains"
        for edge in edges
    )
    chunk_edge = next(
        edge for edge in edges
        if edge.src == section.node_id and edge.edge_type == "contains"
        and edge.dst.startswith("chunk:")
    )
    assert any(
        edge.src == citation.node_id
        and edge.dst == chunk_edge.dst
        and edge.edge_type == "references"
        for edge in edges
    )
    assert any(
        edge.src == citation.node_id
        and edge.dst == "paper:doi:10.1000/known"
        and edge.edge_type == "cites"
        for edge in edges
    )

    assert run.record_set is not None
    record = run.record_set.records[project.paper_id]
    assert record.primary is not None and record.primary.node_id == project.paper_id
    assert citation.node_id in record.nodes_by_id()
    assert (project.paper_id, "derived_from", root_doc) in record.edges_by_identity()


def test_project_member_references_are_deterministic(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "paper/main.tex",
        r"\documentclass{article}\begin{document}\input{part}\input{part}\end{document}",
    )
    _write(tmp_path, "paper/part.tex", "Body")
    manifest = _manifest(tmp_path)
    source = LatexProjectSource(tmp_path, manifest)

    first = list(source.run("same", mode="scope_complete").facts)
    second = list(source.run("same", mode="scope_complete").facts)
    assert first == second


def test_selected_visual_is_discovered_when_captioning_is_disabled(
    tmp_path: Path,
) -> None:
    _write(
        tmp_path,
        "paper/main.tex",
        r"\documentclass{article}\includegraphics{figure.pdf}",
    )
    _write(tmp_path, "paper/figure.pdf", b"%PDF-1.4\nfixture\n")
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    _write(
        extractors,
        "extractors.yaml",
        "parser:\n"
        "  - format: latex\n"
        "    class: okg.substrate.library.extraction.parse.latex.LatexParser\n"
        "  - format: pdf\n"
        "    class: okg.substrate.library.extraction.parse.pdf_text.PdfTextParser\n"
        "chunker:\n"
        "  - format: latex\n"
        "    class: okg.substrate.library.extraction.chunk.latex_structural.LatexStructuralChunker\n"
        "  - format: default\n"
        "    class: okg.substrate.library.extraction.chunk.window.WindowChunker\n"
        "    config: {max_chars: 1500, overlap: 200}\n"
        "ner: []\n"
        "captioning:\n"
        "  enabled: false\n",
    )
    manifest = _manifest(tmp_path)
    figure = next(
        item for item in manifest.projects[0].files
        if item.path == "paper/figure.pdf"
    )
    visual_id = f"visual:{figure.content_sha256[:16]}"

    run = DocCorpusSource(
        repo="papers",
        repo_root=tmp_path, non_git_walk=True,
        extractors_dir=extractors,
        project_manifest=manifest,
    ).run("docs", mode="scope_complete")
    facts = list(run.facts)
    nodes = [fact for fact in facts if isinstance(fact, NodeFact)]
    edges = [fact for fact in facts if isinstance(fact, EdgeFact)]
    document_id = "doc:papers:paper/figure.pdf"

    assert any(
        node.node_id == visual_id and node.subtype == "visual_artifact"
        for node in nodes
    )
    assert any(
        edge.src == document_id
        and edge.dst == visual_id
        and edge.edge_type == "contains"
        for edge in edges
    )


def test_tex_support_routing_is_project_opt_in(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "paper/main.tex",
        r"\documentclass{article}\usepackage{local}\addbibresource{refs.bib}",
    )
    _write(tmp_path, "paper/local.sty", "Local support prose.")
    _write(tmp_path, "paper/refs.bib", "@article{A,title={A}}\n")
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    _write(
        extractors,
        "extractors.yaml",
        "parser:\n"
        "  - format: latex\n"
        "    class: okg.substrate.library.extraction.parse.latex.LatexParser\n"
        "  - format: text\n"
        "    class: okg.substrate.library.extraction.parse.text.TextParser\n"
        "chunker:\n"
        "  - format: latex\n"
        "    class: okg.substrate.library.extraction.chunk.latex_structural.LatexStructuralChunker\n"
        "  - format: default\n"
        "    class: okg.substrate.library.extraction.chunk.window.WindowChunker\n"
        "    config: {max_chars: 1500, overlap: 200}\n"
        "ner: []\n",
    )
    file_only = DocCorpusSource(
        repo="papers",
        repo_root=tmp_path, non_git_walk=True,
        extractors_dir=extractors,
        include_globs=("paper/*",),
    )
    file_only_nodes = [
        fact for fact in file_only.run("files", mode="scope_complete").facts
        if isinstance(fact, NodeFact)
    ]
    file_only_docs = {
        node.attrs["path"]: node for node in file_only_nodes
        if node.subtype == "document"
    }
    assert file_only_docs["paper/local.sty"].attrs["format"] == "unknown"
    assert file_only_docs["paper/refs.bib"].attrs["format"] == "unknown"
    assert not any(
        node.subtype == "document_chunk"
        for node in file_only_nodes
        if node.attrs.get("text") in {"Local support prose.", "@article{A,title={A}}"}
    )

    project = DocCorpusSource(
        repo="papers",
        repo_root=tmp_path, non_git_walk=True,
        extractors_dir=extractors,
        project_manifest=_manifest(tmp_path),
    )
    project_nodes = [
        fact for fact in project.run("project", mode="scope_complete").facts
        if isinstance(fact, NodeFact)
    ]
    project_docs = {
        node.attrs["path"]: node for node in project_nodes
        if node.subtype == "document"
    }
    assert project_docs["paper/local.sty"].attrs["format"] == "latex"
    assert project_docs["paper/refs.bib"].attrs["format"] == "text"
