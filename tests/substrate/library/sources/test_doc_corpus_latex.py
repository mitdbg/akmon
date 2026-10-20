from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from okg.substrate.library.extraction.parse.latex import LatexParser
from okg.substrate.library.extraction.registry import load_extractors
from okg.substrate.library.extraction.registry import ExtractorBundle
from okg.substrate.library.sources import doc_corpus as doc_corpus_module
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.library.sources.doc_corpus import (
    DEFAULT_EXTENSION_TO_FORMAT,
    DocCorpusSource,
)
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


def _adapter(tmp_path: Path) -> DocCorpusSource:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    (extractors / "extractors.yaml").write_text(_EXTRACTORS)
    return DocCorpusSource(
        repo="latex-test",
        repo_root=corpus, non_git_walk=True,
        extractors_dir=extractors,
        include_globs=("**/*.tex",),
    )


def test_tex_extension_routes_only_when_deployment_opts_in(tmp_path: Path) -> None:
    assert DEFAULT_EXTENSION_TO_FORMAT[".tex"] == "latex"
    adapter = _adapter(tmp_path)
    bundle = load_extractors(adapter.extractors_dir)
    assert type(bundle.parsers["latex"]).__name__ == "LatexParser"
    assert (
        type(bundle.chunker_for_format("latex")).__name__
        == "LatexStructuralChunker"
    )


def test_corpus_emits_typed_latex_chunks_and_stable_warnings(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path)
    raw = r"""\begin{document}
\section{Evidence}

Prose.

\begin{equation}
unclosed

Trailing prose.
\end{document}
"""
    (adapter.repo_root / "paper.tex").write_text(raw)

    facts = list(adapter.run("run-latex", mode="scope_complete").facts)
    documents = [fact for fact in facts if getattr(fact, "subtype", None) == "document"]
    chunks = [
        fact for fact in facts
        if getattr(fact, "subtype", None) == "document_chunk"
    ]
    assert len(documents) == 1
    warning = f"latex_unbalanced_environment:{raw.index(r'\begin{equation}')}"
    assert documents[0].attrs["parse_warnings"] == [warning]
    assert [(chunk.attrs["chunk_kind"], chunk.attrs["char_end"]) for chunk in chunks] == [
        ("paragraph", raw.index("Prose.") + len("Prose.")),
        ("paragraph", raw.index("Trailing prose.") + len("Trailing prose.")),
    ]


def test_invalid_utf8_fails_closed_with_no_chunks(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path)
    (adapter.repo_root / "invalid.tex").write_bytes(
        b"\\begin{document}\xff\\end{document}"
    )
    facts = list(adapter.run("run-invalid", mode="scope_complete").facts)
    document = next(
        fact for fact in facts if getattr(fact, "subtype", None) == "document"
    )
    assert document.attrs["parse_warnings"] == ["parse_failed:UnicodeDecodeError"]
    assert not any(
        getattr(fact, "subtype", None) == "document_chunk" for fact in facts
    )


def test_storage_unsafe_c0_fails_as_typed_parser_error(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path)
    (adapter.repo_root / "invalid.tex").write_text(
        "\\begin{document}\x00\\end{document}"
    )
    facts = list(adapter.run("run-c0", mode="scope_complete").facts)
    document = next(
        fact for fact in facts if getattr(fact, "subtype", None) == "document"
    )
    assert document.attrs["parse_warnings"] == [
        "parse_failed:LatexControlCharacterError"
    ]
    assert not any(
        getattr(fact, "subtype", None) == "document_chunk" for fact in facts
    )


class _ExplodingChunker:
    prefers_raw_text = True

    def chunk(self, _text: str):
        raise RuntimeError("unexpected chunker failure")


def test_unexpected_chunker_error_aborts_source_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = _adapter(tmp_path)
    (adapter.repo_root / "paper.tex").write_text(
        "\\begin{document}Body.\\end{document}"
    )
    bundle = ExtractorBundle(
        parsers={"latex": LatexParser()},
        chunker=None,
        ner=None,
        chunkers_by_format={"latex": _ExplodingChunker()},
    )
    monkeypatch.setattr(doc_corpus_module, "load_extractors", lambda _path: bundle)
    with pytest.raises(RuntimeError, match="unexpected chunker failure"):
        list(adapter.run("run-exploding", mode="scope_complete").facts)


def test_figure_caption_links_only_resolved_graphics_in_its_environment(
    tmp_path: Path,
) -> None:
    adapter = _adapter(tmp_path)
    paper = adapter.repo_root / "paper"
    paper.mkdir()
    raw = r"""\documentclass{article}
\includegraphics{outside.png}
\begin{document}
\begin{figure}
\includegraphics{inside-a.png}
\includegraphics{missing.png}
\includegraphics{inside-b.png}
\caption{Shared caption.}
\end{figure}
\end{document}
"""
    (paper / "main.tex").write_text(raw)
    (paper / "outside.png").write_bytes(b"outside")
    (paper / "inside-a.png").write_bytes(b"inside-a")
    (paper / "inside-b.png").write_bytes(b"inside-b")
    manifest = LatexProjectManifestBuilder(
        source_root=adapter.repo_root,
        repository_authority="latex-test",
        projects=[{"project_dir": "paper", "entrypoints": ["main.tex"]}],
    ).build()
    source = DocCorpusSource(
        repo="latex-test",
        repo_root=adapter.repo_root, non_git_walk=True,
        extractors_dir=adapter.extractors_dir,
        project_manifest=manifest,
    )

    run = source.run("run-caption-links", mode="scope_complete")
    facts = list(run.facts)
    caption = next(
        fact
        for fact in facts
        if isinstance(fact, NodeFact)
        and fact.subtype == "document_chunk"
        and fact.attrs["chunk_kind"] == "figure_caption"
    )
    assert "_okg_latex_figure_environment_span" not in caption.attrs
    assert "_okg_caption_visual_ids" not in caption.attrs

    expected_visual_ids = {
        f"visual:{item.content_sha256[:16]}"
        for item in manifest.projects[0].files
        if item.path in {"paper/inside-a.png", "paper/inside-b.png"}
    }
    caption_edges = {
        (fact.src, fact.dst)
        for fact in facts
        if isinstance(fact, EdgeFact) and fact.edge_type == "caption_of"
    }
    assert caption_edges == {
        (caption.node_id, visual_id) for visual_id in expected_visual_ids
    }

    assert run.record_set is not None
    record = run.record_set.records["paper/main.tex"]
    footprint_edges = [edge for edge in record.edges if edge.edge_type == "caption_of"]
    assert {(edge.src, edge.dst) for edge in footprint_edges} == caption_edges
    facts_by_endpoint = {
        (fact.src, fact.dst): fact
        for fact in facts
        if isinstance(fact, EdgeFact) and fact.edge_type == "caption_of"
    }
    for edge in footprint_edges:
        fact = facts_by_endpoint[(edge.src, edge.dst)]
        assert fact.edge_type == edge.edge_type
        assert fact.attrs == edge.attrs
        assert fact.source_record_id == edge.source_record_id
        assert fact.source_revision == edge.source_revision
    assert {
        tuple(sorted(edge.source_record_id.items())) for edge in footprint_edges
    } == {
        tuple(
            sorted(
                {
                    "repo": "latex-test",
                    "path": "paper/main.tex",
                    "chunk_id": caption.node_id,
                    "visual_id": visual_id,
                }.items()
            )
        )
        for visual_id in expected_visual_ids
    }


def test_incomplete_image_scope_latches_manifest_refresh_for_full_reconcile(
    tmp_path: Path,
) -> None:
    adapter = _adapter(tmp_path)
    paper = adapter.repo_root / "paper"
    paper.mkdir()
    (paper / "main.tex").write_text(
        "\\begin{figure}\n"
        "\\includegraphics{figure.png}\n"
        "\\caption{Stable caption.}\n"
        "\\end{figure}\n"
    )
    first_image = b"first-figure"
    second_image = b"second-figure"
    (paper / "figure.png").write_bytes(first_image)
    manifest_config = {
        "repository_authority": "latex-test",
        "projects": [{"project_dir": "paper", "entrypoints": ["main.tex"]}],
    }
    source = DocCorpusSource(
        repo="latex-test",
        repo_root=adapter.repo_root, non_git_walk=True,
        extractors_dir=adapter.extractors_dir,
        project_manifest=manifest_config,
    )
    first = source.run("run-first", mode="scope_complete")
    list(first.facts)
    assert first.record_set is not None
    first_cursor = dict(first.next_cursor or {})
    first_cursor["record_set"] = first.record_set.to_cursor()
    first_digest = first_cursor["latex_project_manifest_digest"]

    (paper / "figure.png").write_bytes(second_image)
    incomplete_source = DocCorpusSource(
        repo="latex-test",
        repo_root=adapter.repo_root, non_git_walk=True,
        extractors_dir=adapter.extractors_dir,
        project_manifest=manifest_config,
    )
    incomplete = incomplete_source.run(
        "run-incomplete",
        mode="scope_incomplete",
        cursor=first_cursor,
        sync_scope={"scope_keys": ["paper/figure.png"]},
    )
    list(incomplete.facts)
    assert incomplete.next_cursor["latex_project_manifest_digest"] == first_digest

    pending_cursor = dict(incomplete.next_cursor or {})
    pending_cursor["record_set"] = first.record_set.to_cursor()
    full_source = DocCorpusSource(
        repo="latex-test",
        repo_root=adapter.repo_root, non_git_walk=True,
        extractors_dir=adapter.extractors_dir,
        project_manifest=manifest_config,
    )
    full = full_source.run(
        "run-full",
        mode="scope_complete",
        cursor=pending_cursor,
    )
    full_facts = list(full.facts)
    assert full_source.last_run_stats["scope_status"] == "full"
    assert full_source.last_run_stats["files_changed"] == 2
    new_visual_id = f"visual:{hashlib.sha256(second_image).hexdigest()[:16]}"
    assert any(
        isinstance(fact, EdgeFact)
        and fact.edge_type == "caption_of"
        and fact.dst == new_visual_id
        for fact in full_facts
    )
    assert full.next_cursor["latex_project_manifest_digest"] != first_digest
