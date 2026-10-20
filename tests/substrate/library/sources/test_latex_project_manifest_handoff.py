from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate.library.sources.doc_corpus import DocCorpusSource
from okg.substrate.library.sources.scholarly.bib_references import BibReferencesSource
from okg.substrate.library.sources.scholarly.latex_project import (
    LatexProjectManifestMismatch,
    LatexProjectSource,
)


PROJECT_CONFIG = {
    "repository_authority": "papers",
    "projects": [{"project_dir": "paper", "entrypoints": ["main.tex"]}],
}


class _FakeCursor:
    def __init__(self, digests: dict[str, str | None]) -> None:
        self.digests = digests
        self.sources: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def cursor(self):
        return self

    def execute(self, _sql: str, params: tuple[list[str]]) -> None:
        self.sources = list(params[0])

    def fetchall(self):
        return [(source, self.digests.get(source)) for source in self.sources]


class _FakePool:
    # `pooled_connection` sizes the general-admission bound from the pool's
    # width, so a stub pool has to declare one.
    max_size = 8
    name = "stub-pool"

    def __init__(self, digests: dict[str, str | None]) -> None:
        self.connection_object = _FakeCursor(digests)

    def connection(self):
        return self.connection_object


def _write_project(root: Path) -> None:
    paper = root / "paper"
    paper.mkdir()
    (paper / "main.tex").write_text(
        r"\documentclass{article}\addbibresource{refs.bib}\begin{document}Text \cite{A}.\end{document}",
        encoding="utf-8",
    )
    (paper / "refs.bib").write_text("@article{A,title={A}}\n", encoding="utf-8")


def test_three_consumers_persist_and_require_the_exact_manifest_digest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path)
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    (extractors / "extractors.yaml").write_text(
        "parser:\n"
        "  - format: latex\n"
        "    class: okg.substrate.library.extraction.parse.latex.LatexParser\n"
        "chunker:\n"
        "  - format: latex\n"
        "    class: okg.substrate.library.extraction.chunk.latex_structural.LatexStructuralChunker\n"
        "ner: []\n",
        encoding="utf-8",
    )
    doc = DocCorpusSource(
        repo="papers",
        repo_root=tmp_path,
        extractors_dir=extractors,
        project_manifest=PROJECT_CONFIG,
    )
    bib = BibReferencesSource(
        source_root=tmp_path,
        project_manifest=PROJECT_CONFIG,
    )
    doc_run = doc.run("doc", mode="scope_complete")
    bib_run = bib.run("bib", mode="scope_complete")
    digest = doc.project_manifest.digest
    assert doc_run.next_cursor["latex_project_manifest_digest"] == digest
    assert bib_run.next_cursor["latex_project_manifest_digest"] == digest

    from okg.substrate.db import pool as pool_module

    monkeypatch.setattr(
        pool_module,
        "get_pool",
        lambda _dsn: _FakePool({"docs": digest, "bib": digest}),
    )
    project = LatexProjectSource(
        tmp_path,
        PROJECT_CONFIG,
        predecessor_sources=("docs", "bib"),
        dsn="postgresql://fixture",
    )
    run = project.run("project", mode="scope_complete")
    assert run.next_cursor["latex_project_manifest_digest"] == digest

    monkeypatch.setattr(
        pool_module,
        "get_pool",
        lambda _dsn: _FakePool({"docs": digest, "bib": "stale"}),
    )
    with pytest.raises(LatexProjectManifestMismatch) as exc:
        project.run("refused", mode="scope_complete")
    assert exc.value.code == "latex_project_manifest_mismatch"
    assert exc.value.data["mismatches"] == {"bib": "stale"}


def test_member_edit_between_predecessors_changes_manifest_and_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path)
    first = BibReferencesSource(
        source_root=tmp_path,
        project_manifest=PROJECT_CONFIG,
    )
    old_digest = first.project_manifest.digest
    (tmp_path / "paper/main.tex").write_text(
        r"\documentclass{article}\addbibresource{refs.bib}\begin{document}Changed \cite{A}.\end{document}",
        encoding="utf-8",
    )
    source = LatexProjectSource(
        tmp_path,
        PROJECT_CONFIG,
        predecessor_sources=("docs", "bib"),
        dsn="postgresql://fixture",
    )
    from okg.substrate.db import pool as pool_module

    monkeypatch.setattr(
        pool_module,
        "get_pool",
        lambda _dsn: _FakePool({"docs": old_digest, "bib": old_digest}),
    )
    with pytest.raises(LatexProjectManifestMismatch):
        source.run("project", mode="scope_complete")


def test_project_mode_probes_refresh_automatic_root_membership(
    tmp_path: Path,
) -> None:
    (tmp_path / "first.tex").write_text(
        r"\documentclass{article}\addbibresource{first.bib}",
        encoding="utf-8",
    )
    (tmp_path / "first.bib").write_text(
        "@article{First,title={First}}\n", encoding="utf-8",
    )
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    (extractors / "extractors.yaml").write_text(
        "parser:\n"
        "  - format: latex\n"
        "    class: okg.substrate.library.extraction.parse.latex.LatexParser\n"
        "chunker:\n"
        "  - format: latex\n"
        "    class: okg.substrate.library.extraction.chunk.latex_structural.LatexStructuralChunker\n"
        "ner: []\n",
        encoding="utf-8",
    )
    automatic = {
        "repository_authority": "papers",
        "projects": [{"project_dir": "."}],
    }
    docs = DocCorpusSource(
        repo="papers",
        repo_root=tmp_path,
        extractors_dir=extractors,
        project_manifest=automatic,
    )
    bibliography = BibReferencesSource(
        source_root=tmp_path,
        project_manifest=automatic,
    )
    first_doc_token = docs.change_probe.build_token()
    first_bib_token = bibliography.change_probe.build_token()
    assert len(docs.project_manifest.projects) == 1
    assert len(bibliography.project_manifest.projects) == 1

    (tmp_path / "second.tex").write_text(
        r"\documentclass{article}\addbibresource{second.bib}",
        encoding="utf-8",
    )
    (tmp_path / "second.bib").write_text(
        "@article{Second,title={Second}}\n", encoding="utf-8",
    )

    assert docs.change_probe.build_token() != first_doc_token
    assert bibliography.change_probe.build_token() != first_bib_token
    assert len(docs.project_manifest.projects) == 2
    assert len(bibliography.project_manifest.projects) == 2

    second_bib_token = bibliography.change_probe.build_token()
    (tmp_path / "first.tex").write_text(
        r"\documentclass{article}\addbibresource{first.bib} Body-only edit.",
        encoding="utf-8",
    )
    assert bibliography.change_probe.build_token() != second_bib_token
