"""Tests for standalone image file ingestion via CaptionService.

req.doc-corpus-image-files + req.extractors-yaml-config.

Runs DocCorpusSource with a stub CaptionService and asserts:
- 32×32 PNG → one Document + one DocumentChunk (chunk_source=vision_caption)
- 8×8 PNG (below min_image_px=10000) → Document emitted, zero caption chunks
- captioning.enabled=false → zero vision_caption chunks even with image present
- Captioning failure → no exception raised
"""
from __future__ import annotations

import hashlib
import io
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml
from PIL import Image as PILImage
from pypdf import PdfWriter
from pypdf.generic import DecodedStreamObject, NameObject

from okg.substrate.library.sources import doc_corpus as doc_corpus_module
from okg.substrate.library.sources.doc_corpus import (
    CaptioningConfig,
    DocCorpusSource,
)
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.library.sources.scholarly.latex_project_manifest import (
    LatexProjectManifestBuilder,
)
from okg.substrate.library.captioning.service import CaptionResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_png(w: int, h: int) -> bytes:
    img = PILImage.new("RGB", (w, h), color=(100, 150, 200))
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def _make_pdf(width_points: int = 144, height_points: int = 72) -> bytes:
    """Build a real one-page PDF plot (2×1 inches) without a render double."""
    writer = PdfWriter()
    page = writer.add_blank_page(width=width_points, height=height_points)
    plot = DecodedStreamObject()
    plot.set_data(
        b"0 0 0 RG 1.5 w 18 12 m 18 60 l 132 60 l S "
        b"1 0 0 RG 2 w 18 20 m 48 28 l 82 44 l 128 54 l S"
    )
    page[NameObject("/Contents")] = writer._add_object(plot)
    buf = io.BytesIO()
    writer.write(buf)
    return buf.getvalue()


def _make_zero_page_pdf() -> bytes:
    writer = PdfWriter()
    buf = io.BytesIO()
    writer.write(buf)
    return buf.getvalue()


def _write_pdf_project(
    corpus: Path,
    content: bytes,
    *,
    filename: str = "figure.pdf",
) -> object:
    (corpus / "main.tex").write_text(r"\includegraphics{figure}")
    (corpus / filename).write_bytes(content)
    return LatexProjectManifestBuilder(
        source_root=corpus,
        repository_authority="test-repo",
        projects=[{"project_dir": ".", "entrypoints": ["main.tex"]}],
    ).build()


def _write_extractors_yaml(d: Path, *, enabled: bool = True) -> None:
    cfg = {
        "parser": [
            {"format": "text",
             "class": "okg.substrate.library.extraction.parse.text.TextParser"},
        ],
        "chunker": [
            {"format": "default",
             "class": "okg.substrate.library.extraction.chunk.window.WindowChunker",
             "config": {"max_chars": 1000, "overlap": 100}},
        ],
        "ner": [],
    }
    if enabled:
        cfg["captioning"] = {
            "enabled": True,
            "mode": "coarse",
            "model": "chatgpt/gpt-4o",
            "min_image_px": 10_000,
        }
    else:
        cfg["captioning"] = {"enabled": False}
    (d / "extractors.yaml").write_text(yaml.safe_dump(cfg))


def _run_and_collect(source: DocCorpusSource) -> list[NodeFact]:
    run = source.run("test-run-id", mode="scope_complete", cursor=None)
    return [f for f in run.facts if isinstance(f, NodeFact)]


def _chunks(facts: list[NodeFact]) -> list[NodeFact]:
    return [f for f in facts if f.subtype == "document_chunk"]


def _caption_chunks(facts: list[NodeFact]) -> list[NodeFact]:
    return [
        f for f in _chunks(facts)
        if f.attrs.get("chunk_source") == "vision_caption"
    ]


class _StubCaptionSvc:
    """Minimal stub: always returns one CaptionResult per call."""

    def caption(self, image: object) -> list[CaptionResult]:
        return [CaptionResult(
            text="a stub caption",
            chunk_source="vision_caption",
            caption_model="chatgpt/gpt-4o",
            caption_provider="chatgpt",
        )]


class _PixelInspectingCaptionSvc:
    """Proves the provider receives decoded render pixels, not raw PDF bytes."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def caption(self, image: object) -> list[CaptionResult]:
        assert isinstance(image, PILImage.Image)
        self.calls.append({
            "format": image.format,
            "size": image.size,
            "extrema": image.convert("RGB").getextrema(),
        })
        assert min(channel[0] for channel in self.calls[-1]["extrema"]) < 250
        return [CaptionResult(
            text="Plot with an x-axis from 0 to 10 and a rising distribution.",
            chunk_source="vision_caption",
            caption_model="chatgpt/gpt-4o",
            caption_provider="chatgpt",
        )]


# ---------------------------------------------------------------------------
# Test 1 — 32×32 PNG produces one Document + one caption chunk
# ---------------------------------------------------------------------------


def test_large_image_produces_document_and_chunk(tmp_path: Path) -> None:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    # 105×105 = 11025 px, above the default 10000px threshold
    (corpus / "figure.png").write_bytes(_make_png(105, 105))

    extractors = tmp_path / "extractors"
    extractors.mkdir()
    _write_extractors_yaml(extractors, enabled=True)

    src = DocCorpusSource(
        repo="test-repo",
        repo_root=corpus, non_git_walk=True,
        extractors_dir=extractors,
        caption_service=_StubCaptionSvc(),
    )
    facts = _run_and_collect(src)

    docs = [f for f in facts if f.subtype == "document"]
    assert len(docs) == 1, f"Expected 1 document, got {len(docs)}"

    captions = _caption_chunks(facts)
    assert len(captions) == 1, f"Expected 1 caption chunk, got {len(captions)}"
    assert captions[0].attrs["chunk_source"] == "vision_caption"
    assert captions[0].attrs["text"] == "a stub caption"


# ---------------------------------------------------------------------------
# Test 2 — 8×8 PNG (< 10000px) produces Document but zero caption chunks
# ---------------------------------------------------------------------------


def test_small_image_produces_document_no_chunks(tmp_path: Path) -> None:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "tiny.png").write_bytes(_make_png(8, 8))

    extractors = tmp_path / "extractors"
    extractors.mkdir()
    _write_extractors_yaml(extractors, enabled=True)

    src = DocCorpusSource(
        repo="test-repo",
        repo_root=corpus, non_git_walk=True,
        extractors_dir=extractors,
        caption_service=_StubCaptionSvc(),
    )
    facts = _run_and_collect(src)

    docs = [f for f in facts if f.subtype == "document"]
    assert len(docs) == 1

    captions = _caption_chunks(facts)
    assert len(captions) == 0, f"Tiny image should produce no caption chunks; got {captions}"


# ---------------------------------------------------------------------------
# Test 3 — captioning.enabled=false → zero vision_caption chunks
# ---------------------------------------------------------------------------


def test_captioning_disabled_emits_no_caption_chunks(tmp_path: Path) -> None:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "figure.png").write_bytes(_make_png(200, 200))

    extractors = tmp_path / "extractors"
    extractors.mkdir()
    _write_extractors_yaml(extractors, enabled=False)

    src = DocCorpusSource(
        repo="test-repo",
        repo_root=corpus, non_git_walk=True,
        extractors_dir=extractors,
        caption_service=_StubCaptionSvc(),
    )
    facts = _run_and_collect(src)

    captions = _caption_chunks(facts)
    assert captions == [], (
        f"captioning.enabled=false must emit zero vision_caption chunks; got {captions}"
    )


# ---------------------------------------------------------------------------
# Test 4 — captioning failure is caught, no exception raised
# ---------------------------------------------------------------------------


class _FailingCaptionSvc:
    def caption(self, image: object) -> list[CaptionResult]:
        raise RuntimeError("intentional caption failure")


class _EmptyCaptionSvc:
    def caption(self, image: object) -> list[CaptionResult]:
        return [CaptionResult(
            text="  \t\n",
            chunk_source="vision_caption",
            caption_model="chatgpt/gpt-4o",
            caption_provider="chatgpt",
        )]


def test_captioning_failure_does_not_raise(tmp_path: Path) -> None:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "figure.png").write_bytes(_make_png(200, 200))

    extractors = tmp_path / "extractors"
    extractors.mkdir()
    _write_extractors_yaml(extractors, enabled=True)

    src = DocCorpusSource(
        repo="test-repo",
        repo_root=corpus, non_git_walk=True,
        extractors_dir=extractors,
        caption_service=_FailingCaptionSvc(),
    )
    # Must not raise — failures are logged as warnings
    facts = _run_and_collect(src)
    # Document is still emitted even if captioning failed
    docs = [f for f in facts if f.subtype == "document"]
    assert len(docs) == 1


# ---------------------------------------------------------------------------
# _load_captioning_config — absent block disables; malformed block raises
# (a silent disable would be a P0 default-value-as-fallback per CLAUDE.md)
# ---------------------------------------------------------------------------


def test_config_absent_file_disables(tmp_path: Path) -> None:
    from okg.substrate.library.sources.doc_corpus import _load_captioning_config

    # No extractors.yaml on disk → disabled default, not an error.
    cfg = _load_captioning_config(tmp_path)
    assert cfg.enabled is False


def test_config_absent_block_disables(tmp_path: Path) -> None:
    from okg.substrate.library.sources.doc_corpus import _load_captioning_config

    (tmp_path / "extractors.yaml").write_text(yaml.safe_dump({"ner": []}))
    cfg = _load_captioning_config(tmp_path)
    assert cfg.enabled is False


def test_config_malformed_min_image_px_raises(tmp_path: Path) -> None:
    from okg.substrate.library.sources.doc_corpus import _load_captioning_config

    (tmp_path / "extractors.yaml").write_text(yaml.safe_dump({
        "captioning": {
            "enabled": True, "model": "openai/gpt-4o",
            "min_image_px": "100x100",
        },
    }))
    # Malformed numeric field must surface, not silently disable captioning.
    with pytest.raises(ValueError, match="min_image_px|integer"):
        _load_captioning_config(tmp_path)


def test_config_malformed_prompts_raises(tmp_path: Path) -> None:
    from okg.substrate.library.sources.doc_corpus import _load_captioning_config

    (tmp_path / "extractors.yaml").write_text(yaml.safe_dump({
        "captioning": {
            "enabled": True, "model": "openai/gpt-4o",
            "prompts": ["not", "a", "mapping"],
        },
    }))
    with pytest.raises(ValueError, match="prompts"):
        _load_captioning_config(tmp_path)


# ---------------------------------------------------------------------------
# Standalone PDF figures — real PDFium render, provenance, and closed outcomes
# ---------------------------------------------------------------------------


def test_selected_pdf_is_rendered_and_captioned_from_pixels(tmp_path: Path) -> None:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    pdf = _make_pdf()
    manifest = _write_pdf_project(corpus, pdf)
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    _write_extractors_yaml(extractors, enabled=True)
    caption_svc = _PixelInspectingCaptionSvc()

    facts = _run_and_collect(DocCorpusSource(
        repo="test-repo",
        repo_root=corpus, non_git_walk=True,
        extractors_dir=extractors,
        caption_service=caption_svc,
        project_manifest=manifest,
    ))

    visuals = [fact for fact in facts if fact.subtype == "visual_artifact"]
    assert len(visuals) == 1
    attrs = visuals[0].attrs
    assert attrs["source_kind"] == "standalone_pdf"
    assert attrs["content_sha256"] == hashlib.sha256(pdf).hexdigest()
    assert attrs["byte_count"] == len(pdf)
    assert attrs["mime_type"] == "application/pdf"
    assert attrs["render_sha256"] != attrs["content_sha256"]
    assert attrs["renderer_name"] == "pdfium"
    assert "pypdfium2-" in attrs["renderer_version"]
    assert "pdfium-" in attrs["renderer_version"]
    assert attrs["page_number"] == 1
    assert attrs["render_dpi"] == 100
    assert (attrs["width"], attrs["height"]) == (200, 100)
    assert attrs["caption_status"] == "captioned"
    assert "caption_skip_reason" not in attrs

    captions = _caption_chunks(facts)
    assert len(captions) == 1
    assert "x-axis" in captions[0].attrs["text"]
    assert caption_svc.calls[0]["format"] == "PNG"
    assert caption_svc.calls[0]["size"] == (200, 100)


def test_pdf_detection_uses_magic_not_filename_suffix(tmp_path: Path) -> None:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    pdf = _make_pdf()
    manifest = _write_pdf_project(corpus, pdf, filename="figure.png")
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    _write_extractors_yaml(extractors, enabled=True)

    facts = _run_and_collect(DocCorpusSource(
        repo="test-repo",
        repo_root=corpus, non_git_walk=True,
        extractors_dir=extractors,
        caption_service=_PixelInspectingCaptionSvc(),
        project_manifest=manifest,
    ))

    attrs = next(
        fact.attrs for fact in facts if fact.subtype == "visual_artifact"
    )
    assert attrs["path"] == "figure.png"
    assert attrs["source_kind"] == "standalone_pdf"
    assert attrs["mime_type"] == "application/pdf"
    assert attrs["caption_status"] == "captioned"

    raster_corpus = tmp_path / "raster-corpus"
    raster_corpus.mkdir()
    png = _make_png(200, 200)
    raster_manifest = _write_pdf_project(raster_corpus, png)
    raster_extractors = tmp_path / "raster-extractors"
    raster_extractors.mkdir()
    _write_extractors_yaml(raster_extractors, enabled=True)
    raster_facts = _run_and_collect(DocCorpusSource(
        repo="test-repo",
        repo_root=raster_corpus, non_git_walk=True,
        extractors_dir=raster_extractors,
        caption_service=_StubCaptionSvc(),
        project_manifest=raster_manifest,
    ))
    raster_attrs = next(
        fact.attrs for fact in raster_facts if fact.subtype == "visual_artifact"
    )
    assert raster_attrs["path"] == "figure.pdf"
    assert raster_attrs["source_kind"] == "standalone_image"
    assert raster_attrs["mime_type"] == "image/png"
    assert "renderer_name" not in raster_attrs


@pytest.mark.parametrize(
    ("content", "expected_reason"),
    [
        (b"%PDF-1.7\nnot a valid document", "pdf_render_failed"),
        (_make_zero_page_pdf(), "pdf_render_failed"),
    ],
)
def test_selected_pdf_render_failure_is_named(
    tmp_path: Path,
    content: bytes,
    expected_reason: str,
) -> None:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    manifest = _write_pdf_project(corpus, content)
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    _write_extractors_yaml(extractors, enabled=True)

    facts = _run_and_collect(DocCorpusSource(
        repo="test-repo",
        repo_root=corpus, non_git_walk=True,
        extractors_dir=extractors,
        caption_service=_StubCaptionSvc(),
        project_manifest=manifest,
    ))

    attrs = next(
        fact.attrs for fact in facts if fact.subtype == "visual_artifact"
    )
    assert attrs["caption_status"] == "failed"
    assert attrs["caption_skip_reason"] == expected_reason
    assert _caption_chunks(facts) == []


def test_missing_pdfium_is_named_renderer_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    manifest = _write_pdf_project(corpus, _make_pdf())
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    _write_extractors_yaml(extractors, enabled=True)
    monkeypatch.setitem(sys.modules, "pypdfium2", None)

    facts = _run_and_collect(DocCorpusSource(
        repo="test-repo",
        repo_root=corpus, non_git_walk=True,
        extractors_dir=extractors,
        caption_service=_StubCaptionSvc(),
        project_manifest=manifest,
    ))

    attrs = next(
        fact.attrs for fact in facts if fact.subtype == "visual_artifact"
    )
    assert attrs["renderer_name"] == "pdfium"
    assert attrs["caption_status"] == "failed"
    assert attrs["caption_skip_reason"] == "pdf_renderer_unavailable"


def test_empty_provider_result_is_named(tmp_path: Path) -> None:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "figure.png").write_bytes(_make_png(200, 200))
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    _write_extractors_yaml(extractors, enabled=True)

    facts = _run_and_collect(DocCorpusSource(
        repo="test-repo",
        repo_root=corpus, non_git_walk=True,
        extractors_dir=extractors,
        caption_service=_EmptyCaptionSvc(),
    ))

    attrs = next(
        fact.attrs for fact in facts if fact.subtype == "visual_artifact"
    )
    assert attrs["caption_status"] == "failed"
    assert attrs["caption_skip_reason"] == "empty_result"
    assert _caption_chunks(facts) == []


def test_undecodable_raster_is_named(tmp_path: Path) -> None:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "figure.png").write_bytes(b"not an image")
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    _write_extractors_yaml(extractors, enabled=True)

    facts = _run_and_collect(DocCorpusSource(
        repo="test-repo",
        repo_root=corpus, non_git_walk=True,
        extractors_dir=extractors,
        caption_service=_StubCaptionSvc(),
    ))

    attrs = next(
        fact.attrs for fact in facts if fact.subtype == "visual_artifact"
    )
    assert attrs["caption_status"] == "failed"
    assert attrs["caption_skip_reason"] == "decode_failed"
    assert _caption_chunks(facts) == []


def test_oversize_pdf_uses_source_bytes_and_is_named(tmp_path: Path) -> None:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    pdf = _make_pdf()
    manifest = _write_pdf_project(corpus, pdf)
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    _write_extractors_yaml(extractors, enabled=True)
    caption_svc = _PixelInspectingCaptionSvc()

    facts = _run_and_collect(DocCorpusSource(
        repo="test-repo",
        repo_root=corpus, non_git_walk=True,
        extractors_dir=extractors,
        caption_service=caption_svc,
        project_manifest=manifest,
        max_bytes_per_file=len(pdf) - 1,
    ))

    attrs = next(
        fact.attrs for fact in facts if fact.subtype == "visual_artifact"
    )
    assert attrs["byte_count"] == len(pdf)
    assert attrs["caption_status"] == "skipped"
    assert attrs["caption_skip_reason"] == "source_too_large"
    assert "render_sha256" not in attrs
    assert caption_svc.calls == []


def test_pdf_minimum_pixel_gate_uses_rendered_dimensions(tmp_path: Path) -> None:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    pdf = _make_pdf(width_points=72, height_points=36)
    manifest = _write_pdf_project(corpus, pdf)
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    _write_extractors_yaml(extractors, enabled=True)
    caption_svc = _PixelInspectingCaptionSvc()

    facts = _run_and_collect(DocCorpusSource(
        repo="test-repo",
        repo_root=corpus, non_git_walk=True,
        extractors_dir=extractors,
        caption_service=caption_svc,
        project_manifest=manifest,
    ))

    attrs = next(
        fact.attrs for fact in facts if fact.subtype == "visual_artifact"
    )
    assert attrs["mime_type"] == "application/pdf"
    assert attrs["width"] == 100
    assert attrs["height"] == 50
    assert attrs["caption_status"] == "skipped"
    assert attrs["caption_skip_reason"] == "below_min_pixels"
    assert attrs["render_dpi"] == 100
    assert caption_svc.calls == []
    assert _caption_chunks(facts) == []


def test_cache_outcome_requires_non_whitespace_caption(tmp_path: Path) -> None:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    _write_extractors_yaml(extractors, enabled=True)
    source = DocCorpusSource(
        repo="test-repo",
        repo_root=corpus, non_git_walk=True,
        extractors_dir=extractors,
        caption_service=_FailingCaptionSvc(),
    )
    image = _make_png(200, 200)
    content_sha256 = hashlib.sha256(image).hexdigest()
    visual_id = source._canonicalize(f"visual:{content_sha256[:16]}")
    config = CaptioningConfig(
        enabled=True,
        model="chatgpt/gpt-4o",
        min_image_px=10_000,
    )
    signature = source._compute_vision_signature(
        content_sha256=content_sha256,
        render_sha256=None,
        caption_model=config.model,
        caption_provider="chatgpt",
        prompt=config.prompt_for(source.name),
        captioning_cfg=config,
    )
    emitted: list[object] = []
    chunks: list[tuple[str, str, dict[str, Any]]] = []

    source._emit_caption_chunks(
        image_bytes=image,
        doc_id="doc:test",
        rel_str="figure.png",
        image_label="figure.png",
        revision={"run_id": "test"},
        emit=emitted.append,
        caption_svc=_FailingCaptionSvc(),
        captioning_cfg=config,
        chunks_this_doc=chunks,
        source_kind="standalone_image",
        visual_descendants_this_doc=[],
        visual_edges_this_doc=[],
        caption_cache={
            visual_id: {
                "vision_signature": signature,
                "chunks": [{"text": " \t", "attrs": {}}],
            },
        },
    )

    visual = next(
        fact for fact in emitted
        if isinstance(fact, NodeFact) and fact.subtype == "visual_artifact"
    )
    assert visual.attrs["caption_status"] == "failed"
    assert visual.attrs["caption_skip_reason"] == "empty_result"
    assert chunks == []

    reused_emitted: list[object] = []
    reused_chunks: list[tuple[str, str, dict[str, Any]]] = []
    source._emit_caption_chunks(
        image_bytes=image,
        doc_id="doc:test",
        rel_str="figure.png",
        image_label="figure.png",
        revision={"run_id": "test"},
        emit=reused_emitted.append,
        caption_svc=_FailingCaptionSvc(),
        captioning_cfg=config,
        chunks_this_doc=reused_chunks,
        source_kind="standalone_image",
        visual_descendants_this_doc=[],
        visual_edges_this_doc=[],
        caption_cache={
            visual_id: {
                "vision_signature": signature,
                "chunks": [{"text": "cached caption", "attrs": {}}],
            },
        },
    )
    reused_visual = next(
        fact for fact in reused_emitted
        if isinstance(fact, NodeFact) and fact.subtype == "visual_artifact"
    )
    assert reused_visual.attrs["caption_status"] == "reused"
    assert "caption_skip_reason" not in reused_visual.attrs
    assert reused_chunks[0][1] == "cached caption"


def test_pdf_renderer_config_changes_vision_gate_fingerprint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    _write_extractors_yaml(extractors, enabled=True)
    source = DocCorpusSource(
        repo="test-repo",
        repo_root=corpus, non_git_walk=True,
        extractors_dir=extractors,
    )

    config = CaptioningConfig(
        enabled=True,
        model="chatgpt/gpt-4o",
        pdf_render_dpi=100,
    )
    baseline = source._vision_gate_fingerprint(config)

    monkeypatch.setattr(
        doc_corpus_module,
        "_STANDALONE_PDF_RENDERER",
        "different-renderer",
    )
    assert source._vision_gate_fingerprint(config) != baseline
    monkeypatch.undo()

    monkeypatch.setattr(
        doc_corpus_module,
        "_pdfium_renderer_version",
        lambda: "different-version",
    )
    assert source._vision_gate_fingerprint(config) != baseline
    monkeypatch.undo()

    monkeypatch.setattr(doc_corpus_module, "_STANDALONE_PDF_PAGE_NUMBER", 2)
    assert source._vision_gate_fingerprint(config) != baseline
    monkeypatch.undo()

    assert source._vision_gate_fingerprint(CaptioningConfig(
        enabled=True,
        model="chatgpt/gpt-4o",
        pdf_render_dpi=144,
    )) != baseline


def test_pdf_renderer_change_updates_persisted_fact_revision(
    tmp_path: Path,
) -> None:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    manifest = _write_pdf_project(corpus, _make_pdf())
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    _write_extractors_yaml(extractors, enabled=True)

    first_facts = _run_and_collect(DocCorpusSource(
        repo="test-repo",
        repo_root=corpus, non_git_walk=True,
        extractors_dir=extractors,
        caption_service=_PixelInspectingCaptionSvc(),
        project_manifest=manifest,
    ))
    first_visual = next(
        fact for fact in first_facts if fact.subtype == "visual_artifact"
    )

    config_path = extractors / "extractors.yaml"
    config = yaml.safe_load(config_path.read_text())
    config["captioning"]["pdf_render_dpi"] = 144
    config_path.write_text(yaml.safe_dump(config))
    second_facts = _run_and_collect(DocCorpusSource(
        repo="test-repo",
        repo_root=corpus, non_git_walk=True,
        extractors_dir=extractors,
        caption_service=_PixelInspectingCaptionSvc(),
        project_manifest=manifest,
    ))
    second_visual = next(
        fact for fact in second_facts if fact.subtype == "visual_artifact"
    )

    assert first_visual.source_revision[
        "doc_corpus_emission_schema_version"
    ] == doc_corpus_module.DOC_CORPUS_EMISSION_SCHEMA_VERSION
    assert first_visual.source_revision["vision_config_fingerprint"]
    assert second_visual.source_revision["vision_config_fingerprint"]
    assert first_visual.source_revision != second_visual.source_revision


def test_config_out_of_range_pdf_render_dpi_raises(tmp_path: Path) -> None:
    from okg.substrate.library.sources.doc_corpus import _load_captioning_config

    (tmp_path / "extractors.yaml").write_text(yaml.safe_dump({
        "captioning": {
            "enabled": True,
            "model": "openai/gpt-4o",
            "pdf_render_dpi": 24,
        },
    }))
    with pytest.raises(ValueError, match="pdf_render_dpi.*between 36 and 600"):
        _load_captioning_config(tmp_path)
