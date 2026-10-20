from __future__ import annotations

from pathlib import Path

from okg.substrate.library.sources.base import NodeFact
from okg.substrate.library.sources.scholarly.bib_references import BibReferencesSource
from okg.substrate.library.sources.scholarly.latex_project import LatexProjectSource


PROJECT_CONFIG = {
    "repository_authority": "papers",
    "projects": [{"project_dir": "paper", "entrypoints": ["main.tex"]}],
}


def _write(root: Path, rel: str, content: str) -> None:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_removed_citation_and_bibliography_entry_leave_current_record_sets(
    tmp_path: Path,
) -> None:
    _write(
        tmp_path,
        "paper/main.tex",
        r"\documentclass{article}\addbibresource{refs.bib}\begin{document}Text \cite{A,B}.\end{document}",
    )
    _write(
        tmp_path,
        "paper/refs.bib",
        "@article{A,title={A}}\n@article{B,title={B}}\n",
    )
    project_source = LatexProjectSource(tmp_path, PROJECT_CONFIG)
    bib_source = BibReferencesSource(
        project_manifest=PROJECT_CONFIG,
        source_root=tmp_path,
    )

    old_project = project_source.run("old", mode="reconcile")
    old_bib = bib_source.run("old", mode="reconcile")
    old_citations = {
        fact.node_id for fact in old_project.facts
        if isinstance(fact, NodeFact) and fact.subtype == "citation"
    }
    old_papers = {
        fact.node_id for fact in old_bib.facts
        if isinstance(fact, NodeFact) and fact.subtype == "paper"
    }
    assert len(old_citations) == 2
    assert len(old_papers) == 2

    _write(
        tmp_path,
        "paper/main.tex",
        r"\documentclass{article}\addbibresource{refs.bib}\begin{document}Text \cite{A}.\end{document}",
    )
    _write(tmp_path, "paper/refs.bib", "@article{A,title={A}}\n")
    new_project = project_source.run("new", mode="reconcile")
    new_bib = bib_source.run("new", mode="reconcile")
    assert new_project.record_set is not None
    assert new_bib.record_set is not None
    current_project_nodes = {
        node_id
        for record in new_project.record_set.records.values()
        for node_id in record.nodes_by_id()
    }
    current_bib_nodes = {
        node_id
        for record in new_bib.record_set.records.values()
        for node_id in record.nodes_by_id()
    }
    assert len(old_citations - current_project_nodes) == 1
    assert len(old_papers - current_bib_nodes) == 1


def test_removed_root_produces_empty_authoritative_scope(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "paper/main.tex",
        r"\documentclass{article}\begin{document}Body\end{document}",
    )
    source = LatexProjectSource(tmp_path, PROJECT_CONFIG)
    first = source.run("first", mode="reconcile")
    assert first.record_set is not None and first.record_set.records

    (tmp_path / "paper/main.tex").unlink()
    second = source.run("second", mode="reconcile")
    assert list(second.facts) == []
    assert second.record_set is not None
    assert second.record_set.records == {}
