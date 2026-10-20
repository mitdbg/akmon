from __future__ import annotations

from pathlib import Path

from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.library.sources.scholarly.latex_project import (
    LatexProjectSource,
    scan_citations,
)
from okg.substrate.library.sources.scholarly.latex_project_manifest import (
    LatexProjectManifestBuilder,
)


def _write(root: Path, rel: str, content: str) -> None:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _manifest(root: Path, projects: list[dict[str, object]]):
    return LatexProjectManifestBuilder(
        source_root=root,
        repository_authority="papers",
        projects=projects,
    ).build()


def _facts(source: LatexProjectSource) -> tuple[list[NodeFact], list[EdgeFact]]:
    emitted = list(source.run("test", mode="scope_complete").facts)
    return (
        [fact for fact in emitted if isinstance(fact, NodeFact)],
        [fact for fact in emitted if isinstance(fact, EdgeFact)],
    )


def test_explicit_cite_allowlist_accepts_starred_forms_and_two_balanced_options() -> None:
    commands = (
        "cite", "citep", "citet", "citealp", "citealt", "citeauthor",
        "citeyear", "citeyearpar", "parencite", "textcite", "autocite",
        "smartcite", "footcite", "supercite",
    )
    text = " ".join(
        f"\\{command}*[pre[nested]][post]{{{command}}}"
        for command in commands
    ) + r" \citestyle{not-a-cite} \cite[one][two][three]{too-many}"
    occurrences, selections = scan_citations(text)

    assert selections == ()
    assert [item.normalized_key for item in occurrences] == list(commands)
    assert [item.command for item in occurrences] == [
        f"{command}*" for command in commands
    ]


def test_cite_grammar_spans_containment_chunks_and_nocite_metadata(tmp_path: Path) -> None:
    text = r"""\documentclass{article}
\addbibresource{refs.bib}
\newcommand{\hidden}{\cite{macro}}
\begin{document}
\section{Heading \textcite{heading}}
Body \citep[see][p.~2]{ alpha, alpha } and \autocite*{beta}.
% \cite{comment}
\begin{verbatim}\cite{verbatim}\end{verbatim}
\nocite{selected}
\end{document}
"""
    _write(tmp_path, "paper/main.tex", text)
    _write(
        tmp_path,
        "paper/refs.bib",
        "\n".join(
            f"@article{{{key},title={{{key.title()}}},doi={{10.1/{key}}}}}"
            for key in ("heading", "alpha", "beta", "selected")
        ),
    )
    manifest = _manifest(tmp_path, [{"project_dir": "paper", "entrypoints": ["main.tex"]}])
    source = LatexProjectSource(tmp_path, manifest)
    nodes, edges = _facts(source)
    citations = [node for node in nodes if node.subtype == "citation"]

    assert [node.attrs["normalized_key"] for node in citations] == [
        "heading", "alpha", "alpha", "beta",
    ]
    assert len({node.node_id for node in citations}) == 4
    for citation in citations:
        attrs = citation.attrs
        assert text[attrs["key_start"]:attrs["key_end"]] == attrs["raw_key"]
        assert attrs["cited_paper_ref"].startswith("paper:doi:")
        assert attrs["citation_lookup_ref"]["project_id"] == manifest.projects[0].paper_id
        assert sum(
            edge.src == "doc:papers:paper/main.tex"
            and edge.dst == citation.node_id and edge.edge_type == "contains"
            for edge in edges
        ) == 1
        assert sum(
            edge.dst == citation.node_id and edge.edge_type == "contains"
            and edge.src.startswith("section:")
            for edge in edges
        ) == 1
        assert sum(
            edge.src == citation.node_id and edge.edge_type == "cites"
            for edge in edges
        ) == 1

    heading = citations[0]
    assert "canonical_chunk_ref" not in heading.attrs
    assert not any(
        edge.src == heading.node_id and edge.edge_type == "references"
        for edge in edges
    )
    for body in citations[1:]:
        assert body.attrs["canonical_chunk_ref"].startswith("chunk:")
    authored_paper = next(
        node for node in nodes
        if node.subtype == "paper" and node.node_id == manifest.projects[0].paper_id
    )
    assert authored_paper.attrs["bibliography_selection"] == [{
        "source_path": "paper/main.tex", "normalized_key": "selected",
    }]
    assert all(node.attrs["normalized_key"] != "selected" for node in citations)


def test_unresolved_and_ambiguous_citations_remain_without_guessed_edges(
    tmp_path: Path,
) -> None:
    _write(
        tmp_path,
        "paper/main.tex",
        r"\documentclass{article}\addbibresource{a.bib}\addbibresource{b.bib}\begin{document}\section{S}Text \cite{conflict,missing}.\end{document}",
    )
    _write(tmp_path, "paper/a.bib", "@article{conflict,title={A}}")
    _write(tmp_path, "paper/b.bib", "@article{conflict,title={B}}")
    manifest = _manifest(tmp_path, [{"project_dir": "paper", "entrypoints": ["main.tex"]}])
    source = LatexProjectSource(tmp_path, manifest)
    nodes, edges = _facts(source)
    citations = {node.attrs["normalized_key"]: node for node in nodes if node.subtype == "citation"}

    assert citations["conflict"].attrs["resolution_status"] == "ambiguous"
    assert citations["conflict"].attrs["resolution_warning"] == "latex_citation_ambiguous"
    assert citations["missing"].attrs["resolution_status"] == "unresolved"
    assert "cited_paper_ref" not in citations["conflict"].attrs
    assert "cited_paper_ref" not in citations["missing"].attrs
    assert not any(
        edge.edge_type == "cites" and edge.src in {
            citations["conflict"].node_id, citations["missing"].node_id,
        }
        for edge in edges
    )
    assert {warning["code"] for warning in source.last_run_stats["warnings"]} == {
        "latex_citation_ambiguous", "latex_citation_unresolved",
    }


def test_comments_inside_key_lists_are_inert_and_escaped_headings_do_not_own_cites(
    tmp_path: Path,
) -> None:
    _write(
        tmp_path,
        "paper/main.tex",
        "\\documentclass{article}\\addbibresource{refs.bib}"
        "\\begin{document}\\\\section{Escaped}"
        "\\section[Short]{Real}Text \\cite{al% comment\npha,beta}."
        "\\end{document}",
    )
    _write(
        tmp_path,
        "paper/refs.bib",
        "@article{alpha,title={A}}\n@article{beta,title={B}}",
    )
    manifest = _manifest(tmp_path, [{"project_dir": "paper", "entrypoints": ["main.tex"]}])
    nodes, _ = _facts(LatexProjectSource(tmp_path, manifest))
    citations = [node for node in nodes if node.subtype == "citation"]
    sections = [node for node in nodes if node.subtype == "paper_section"]

    assert [node.attrs["normalized_key"] for node in citations] == ["alpha", "beta"]
    assert all(node.attrs["resolution_status"] == "resolved" for node in citations)
    assert [node.attrs["title"] for node in sections] == ["Real"]
    assert {node.attrs["citing_section_ref"] for node in citations} == {
        sections[0].node_id
    }


def test_same_cite_key_in_two_projects_never_cross_links(tmp_path: Path) -> None:
    for project, title in (("one", "First"), ("two", "Second")):
        _write(
            tmp_path,
            f"{project}/main.tex",
            r"\documentclass{article}\bibliography{refs}\begin{document}\section{S}\cite{shared}\end{document}",
        )
        _write(tmp_path, f"{project}/refs.bib", f"@article{{shared,title={{{title}}}}}")
    manifest = _manifest(tmp_path, [
        {"project_dir": "one", "entrypoints": ["main.tex"]},
        {"project_dir": "two", "entrypoints": ["main.tex"]},
    ])
    nodes, edges = _facts(LatexProjectSource(tmp_path, manifest))
    citations = [node for node in nodes if node.subtype == "citation"]

    assert len(citations) == 2
    assert len({node.attrs["cited_paper_ref"] for node in citations}) == 2
    assert all(node.attrs["resolution_status"] == "resolved" for node in citations)
    assert len([edge for edge in edges if edge.edge_type == "cites"]) == 2


def test_included_files_share_one_project_section_topology(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "paper/main.tex",
        r"\documentclass{article}\addbibresource{refs.bib}\begin{document}\section{Intro}\input{body}\section{End}\cite{end}\end{document}",
    )
    _write(
        tmp_path,
        "paper/body.tex",
        r"\subsection{Background}\cite{nested}",
    )
    _write(
        tmp_path,
        "paper/refs.bib",
        "@article{nested,title={Nested}}\n@article{end,title={End}}",
    )
    manifest = _manifest(tmp_path, [{"project_dir": "paper", "entrypoints": ["main.tex"]}])
    nodes, _ = _facts(LatexProjectSource(tmp_path, manifest))
    sections = [node for node in nodes if node.subtype == "paper_section"]
    citations = {
        node.attrs["normalized_key"]: node
        for node in nodes if node.subtype == "citation"
    }
    by_title = {node.attrs["title"]: node for node in sections}

    assert [node.attrs["title"] for node in sections] == [
        "Intro", "Background", "End",
    ]
    assert by_title["Intro"].attrs["path"] == "§1"
    assert by_title["Intro"].attrs["ordinal"] == 0
    assert by_title["Background"].attrs["path"] == "§1.1"
    assert by_title["Background"].attrs["ordinal"] == 0
    assert by_title["Background"].attrs["parent_section_ref"] == by_title["Intro"].node_id
    assert by_title["End"].attrs["path"] == "§2"
    assert by_title["End"].attrs["ordinal"] == 1
    assert citations["nested"].attrs["citing_section_ref"] == by_title["Background"].node_id
    assert citations["end"].attrs["citing_section_ref"] == by_title["End"].node_id


def test_mixed_child_heading_depths_get_unique_sibling_paths(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "paper/main.tex",
        r"\documentclass{article}\begin{document}\section{A}\subsubsection{Deep}\subsection{Shallow}\end{document}",
    )
    manifest = _manifest(tmp_path, [{"project_dir": "paper", "entrypoints": ["main.tex"]}])
    nodes, _ = _facts(LatexProjectSource(tmp_path, manifest))
    sections = {
        node.attrs["title"]: node
        for node in nodes if node.subtype == "paper_section"
    }

    assert sections["Deep"].attrs["path"] == "§1.1"
    assert sections["Deep"].attrs["ordinal"] == 0
    assert sections["Shallow"].attrs["path"] == "§1.2"
    assert sections["Shallow"].attrs["ordinal"] == 1
    assert sections["Deep"].attrs["parent_section_ref"] == sections["A"].node_id
    assert sections["Shallow"].attrs["parent_section_ref"] == sections["A"].node_id


def test_duplicate_key_resources_resolve_only_when_canonical_paper_matches(
    tmp_path: Path,
) -> None:
    _write(
        tmp_path,
        "paper/main.tex",
        r"\documentclass{article}\addbibresource{a.bib}\addbibresource{b.bib}\begin{document}\section{S}\cite{same}\end{document}",
    )
    _write(tmp_path, "paper/a.bib", "@article{same,title={A},doi={doi:10.2/SAME}}")
    _write(tmp_path, "paper/b.bib", "@article{same,title={B},doi={https://doi.org/10.2/same}}")
    manifest = _manifest(tmp_path, [{"project_dir": "paper", "entrypoints": ["main.tex"]}])
    nodes, edges = _facts(LatexProjectSource(tmp_path, manifest))
    citation = next(node for node in nodes if node.subtype == "citation")

    assert citation.attrs["resolution_status"] == "resolved"
    assert citation.attrs["cited_paper_ref"] == "paper:doi:10.2/same"
    assert citation.attrs["citation_lookup_ref"]["bibliography_paths"] == [
        "paper/a.bib", "paper/b.bib",
    ]
    assert len([edge for edge in edges if edge.edge_type == "cites"]) == 1
