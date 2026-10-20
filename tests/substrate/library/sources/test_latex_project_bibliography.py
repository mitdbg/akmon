from __future__ import annotations

import hashlib
from pathlib import Path

from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.library.sources.scholarly.bib_references import (
    BibReferencesSource,
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


def _facts(source: BibReferencesSource) -> tuple[list[NodeFact], list[EdgeFact]]:
    emitted = list(source.run("test", mode="scope_complete").facts)
    return (
        [fact for fact in emitted if isinstance(fact, NodeFact)],
        [fact for fact in emitted if isinstance(fact, EdgeFact)],
    )


def test_project_bibliography_uses_nested_resources_and_canonical_identities(
    tmp_path: Path,
) -> None:
    _write(
        tmp_path,
        "paper/main.tex",
        r"\documentclass{article}\addbibresource{refs/library.bib}\begin{document}Body\end{document}",
    )
    _write(
        tmp_path,
        "paper/refs/library.bib",
        r"""
@article{DoiKey, title={DOI}, author={Ada Lovelace}, doi={https://doi.org/10.1000/ABC}, journal={Journal}, year={2024}}
@article{ArxivKey, title={Arxiv}, author={Grace Hopper}, eprint={arXiv:2401.01234v3}}
@book{CaseKey, title={Scoped}, author={Edsger Dijkstra}}
@article{NoYear, title={Journal only}, journal={Timeless Journal}}
""",
    )
    manifest = _manifest(tmp_path, [{"project_dir": "paper", "entrypoints": ["main.tex"]}])
    project_id = manifest.projects[0].paper_id
    source = BibReferencesSource(project_manifest=manifest, source_root=tmp_path)
    nodes, edges = _facts(source)
    by_id = {node.node_id: node for node in nodes}

    assert "paper:doi:10.1000/abc" in by_id
    assert "paper:arxiv:2401.01234" in by_id
    seed = "\0".join((project_id, "paper/refs/library.bib", "CaseKey"))
    scoped_id = f"paper:bibscoped:{hashlib.sha256(seed.encode()).hexdigest()[:32]}"
    assert scoped_id in by_id
    assert by_id[scoped_id].attrs["bib_lookup_ref"] == {
        "project_id": project_id,
        "bib_path": "paper/refs/library.bib",
        "normalized_key": "CaseKey",
    }
    assert "cite_key_ref" not in by_id[scoped_id].attrs
    assert {edge.edge_type for edge in edges} >= {
        "authored_by", "published_in",
    }
    assert not any(edge.edge_type == "member_of" for edge in edges)
    no_year = next(node for node in nodes if node.attrs.get("title") == "Journal only")
    timeless = next(node for node in nodes if node.node_id == "venue:timeless-journal")
    assert any(
        edge.src == no_year.node_id
        and edge.dst == timeless.node_id
        and edge.edge_type == "published_in"
        for edge in edges
    )


def test_same_key_without_global_identity_is_isolated_between_projects(
    tmp_path: Path,
) -> None:
    for project, title in (("one", "First"), ("two", "Second")):
        _write(
            tmp_path,
            f"{project}/main.tex",
            r"\documentclass{article}\bibliography{refs}\begin{document}Body\end{document}",
        )
        _write(tmp_path, f"{project}/refs.bib", f"@article{{Shared,title={{{title}}}}}")
    manifest = _manifest(tmp_path, [
        {"project_dir": "one", "entrypoints": ["main.tex"]},
        {"project_dir": "two", "entrypoints": ["main.tex"]},
    ])
    source = BibReferencesSource(project_manifest=manifest, source_root=tmp_path)
    nodes, _ = _facts(source)
    papers = [node for node in nodes if node.subtype == "paper"]

    assert len(papers) == 2
    assert len({paper.node_id for paper in papers}) == 2
    assert {paper.attrs["title"] for paper in papers} == {"First", "Second"}
    assert all(paper.node_id.startswith("paper:bibscoped:") for paper in papers)


def test_existing_unscoped_bibliography_identity_remains_the_default(tmp_path: Path) -> None:
    bib = tmp_path / "plain.bib"
    bib.write_text("@article{StableKey,title={Stable}}", encoding="utf-8")
    nodes, _ = _facts(BibReferencesSource(bib_paths=[str(bib)]))
    paper = next(node for node in nodes if node.subtype == "paper")
    assert paper.node_id == "paper:bib:stablekey"
    assert paper.attrs["cite_key_ref"] == "paper:stablekey"
