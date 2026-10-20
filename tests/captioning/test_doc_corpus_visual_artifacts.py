"""Tests for first-class VisualArtifact discovery in DocCorpusSource.

req.visual-artifacts.ontology + req.visual-artifacts.incremental +
req.visual-artifacts.fail-loud-config (first-class-visual-artifacts, Task 2).

Drives DocCorpusSource with a stub CaptionService and asserts that visual
DISCOVERY (the VisualArtifact node + contains/member_of/derived_from edges) is
first-class and runs independent of the VLM caption call:

- standalone image → VisualArtifact node + edges + caption chunk linked via
  visual_ref and a derived_from edge, carrying a deterministic vision_signature
- undersized image → VisualArtifact still emitted, zero caption chunks
  (discovery runs before the size gate)
- captioning disabled → zero VisualArtifacts and zero caption chunks
- PDF page → pdf_page_render VisualArtifact with render_sha256 + renderer
- captioning failure → VisualArtifact survives, no caption chunk, no raise
- vision_signature is sensitive to the caption model
- missing pymupdf on an enabled PDF path fails loud (no silent skip)
"""
from __future__ import annotations

import hashlib
import io
import json
import sys
from pathlib import Path

import pytest
import yaml
from PIL import Image as PILImage

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources.doc_corpus import DocCorpusSource
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.library.captioning.service import CaptionResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_png(w: int, h: int) -> bytes:
    img = PILImage.new("RGB", (w, h), color=(100, 150, 200))
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def _write_extractors_yaml(
    d: Path, *, enabled: bool = True, pdf: bool = False,
) -> None:
    parser_cls = (
        "okg.substrate.library.extraction.parse.pdf_text.PdfTextParser"
        if pdf
        else "okg.substrate.library.extraction.parse.text.TextParser"
    )
    cfg: dict = {
        "parser": [{"format": "pdf" if pdf else "text", "class": parser_cls}],
        "chunker": [
            {"format": "default",
             "class": "okg.substrate.library.extraction.chunk.window.WindowChunker",
             "config": {"max_chars": 1000, "overlap": 100}},
        ],
        "ner": [],
    }
    caption_cfg: dict = {
        "enabled": True, "mode": "coarse", "model": "chatgpt/gpt-4o",
        "min_image_px": 10_000,
    }
    if pdf:
        # PDF visual discovery runs only via inline figure detection; tests
        # inject a stub detector so no DocLayout-YOLO/torch is required.
        caption_cfg["figure_detection"] = True
    cfg["captioning"] = caption_cfg if enabled else {"enabled": False}
    (d / "extractors.yaml").write_text(yaml.safe_dump(cfg))


class _StubFigureDetector:
    """Stand-in for FigureDetector: returns a fixed box list (no YOLO)."""

    def __init__(self, boxes: list) -> None:
        self._boxes = boxes

    def detect(self, doc: object) -> list:
        return list(self._boxes)


def _make_pdf_with_image(png_bytes: bytes) -> bytes:
    try:
        import fitz
    except ImportError:
        pytest.skip("pymupdf not installed — skipping PDF visual-artifact test")
    doc = fitz.open()
    page = doc.new_page(width=200, height=300)
    page.insert_text((10, 20), "Test document with embedded image")
    page.insert_image(fitz.Rect(10, 40, 120, 150), stream=png_bytes)
    return doc.tobytes()


def _make_pdf_with_vectors() -> bytes:
    """A PDF whose only visual content is drawn vectors (no embedded raster),
    so get_drawings() >= 5 triggers a page render."""
    try:
        import fitz
    except ImportError:
        pytest.skip("pymupdf not installed — skipping PDF vector test")
    doc = fitz.open()
    page = doc.new_page(width=200, height=300)
    for i in range(6):
        page.draw_rect(
            fitz.Rect(10 + i * 8, 10 + i * 8, 70 + i * 8, 70 + i * 8),
            color=(0, 0, 1),
        )
    return doc.tobytes()


def _all_facts(source: DocCorpusSource) -> list:
    run = source.run("test-run-id", mode="scope_complete", cursor=None)
    return list(run.facts)


def _nodes(facts: list, subtype: str) -> list[NodeFact]:
    return [f for f in facts if isinstance(f, NodeFact) and f.subtype == subtype]


def _caption_chunks(facts: list) -> list[NodeFact]:
    return [
        f for f in _nodes(facts, "document_chunk")
        if f.attrs.get("chunk_source") == "vision_caption"
    ]


def _edges(facts: list, edge_type: str) -> list[EdgeFact]:
    return [
        f for f in facts
        if isinstance(f, EdgeFact) and f.edge_type == edge_type
    ]


def _has_edge(facts: list, src: str, dst: str, edge_type: str) -> bool:
    return any(e.src == src and e.dst == dst for e in _edges(facts, edge_type))


class _StubCaptionSvc:
    """Returns one CaptionResult per call; model/provider are configurable
    so vision_signature sensitivity can be asserted."""

    def __init__(self, model: str = "chatgpt/gpt-4o", provider: str = "chatgpt"):
        self._model = model
        self._provider = provider

    def caption(
        self, image: object, context: str = "", **kwargs: object,
    ) -> list[CaptionResult]:
        return [CaptionResult(
            text="a stub caption",
            chunk_source="vision_caption",
            caption_model=self._model,
            caption_provider=self._provider,
        )]


class _FailingCaptionSvc:
    def caption(
        self, image: object, context: str = "", **kwargs: object,
    ) -> list[CaptionResult]:
        raise RuntimeError("boom — VLM exploded")


def _source(
    corpus: Path, extractors: Path, svc: object,
    figure_detector: object = None,
) -> DocCorpusSource:
    # Always inject an in-memory crop store: since the caption-enricher split,
    # figure_detection requires a place to stage crops (or a dsn). Harmless on
    # the standalone-image paths that never touch the figure detector.
    from okg.substrate.library.captioning.crop_store import InMemoryCropStore

    return DocCorpusSource(
        repo="test-repo",
        repo_root=corpus, non_git_walk=True,
        extractors_dir=extractors,
        caption_service=svc,
        figure_detector=figure_detector,
        figure_crop_store=InMemoryCropStore(),
    )


# ---------------------------------------------------------------------------
# Test 1 — standalone image → VisualArtifact node + edges + linked caption
# ---------------------------------------------------------------------------


def test_standalone_image_emits_visual_artifact_and_links(tmp_path: Path) -> None:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    png = _make_png(105, 105)  # 11025 px > min
    (corpus / "figure.png").write_bytes(png)
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    _write_extractors_yaml(extractors, enabled=True)

    facts = _all_facts(_source(corpus, extractors, _StubCaptionSvc()))

    visuals = _nodes(facts, "visual_artifact")
    assert len(visuals) == 1
    va = visuals[0]
    doc_id = _nodes(facts, "document")[0].node_id
    assert va.attrs["source_kind"] == "standalone_image"
    assert va.attrs["content_sha256"] == hashlib.sha256(png).hexdigest()
    assert va.attrs["mime_type"] == "image/png"
    assert va.attrs["width"] == 105 and va.attrs["height"] == 105
    assert va.attrs["byte_count"] == len(png)
    assert va.attrs["parent_document_ref"] == doc_id
    assert va.attrs["caption_status"] == "captioned"
    assert "caption_skip_reason" not in va.attrs

    # contains / member_of edges both present
    assert _has_edge(facts, doc_id, va.node_id, "contains")
    assert _has_edge(facts, va.node_id, doc_id, "member_of")

    # caption chunk links back to the visual (attr + edge)
    caps = _caption_chunks(facts)
    assert len(caps) == 1
    assert caps[0].attrs["visual_ref"] == va.node_id
    assert _has_edge(facts, caps[0].node_id, va.node_id, "derived_from")

    sig = caps[0].attrs["vision_signature"]
    assert isinstance(sig, str) and len(sig) == 64 and int(sig, 16) >= 0


def test_standalone_image_vision_signature_deterministic(tmp_path: Path) -> None:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "figure.png").write_bytes(_make_png(105, 105))
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    _write_extractors_yaml(extractors, enabled=True)

    sig1 = _caption_chunks(
        _all_facts(_source(corpus, extractors, _StubCaptionSvc()))
    )[0].attrs["vision_signature"]
    sig2 = _caption_chunks(
        _all_facts(_source(corpus, extractors, _StubCaptionSvc()))
    )[0].attrs["vision_signature"]
    assert sig1 == sig2


# ---------------------------------------------------------------------------
# Test 2 — discovery runs BEFORE the caption size gate
# ---------------------------------------------------------------------------


def test_undersized_image_still_discovered_no_caption(tmp_path: Path) -> None:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "tiny.png").write_bytes(_make_png(8, 8))  # 64 px < min
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    _write_extractors_yaml(extractors, enabled=True)

    facts = _all_facts(_source(corpus, extractors, _StubCaptionSvc()))

    visuals = _nodes(facts, "visual_artifact")
    assert len(visuals) == 1  # discovered
    assert visuals[0].attrs["caption_status"] == "skipped"
    assert visuals[0].attrs["caption_skip_reason"] == "below_min_pixels"
    assert len(_caption_chunks(facts)) == 0             # but not captioned


# ---------------------------------------------------------------------------
# Test 3 — captioning disabled → no visual pipeline at all
# ---------------------------------------------------------------------------


def test_captioning_disabled_emits_no_visual_artifacts(tmp_path: Path) -> None:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "figure.png").write_bytes(_make_png(200, 200))
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    _write_extractors_yaml(extractors, enabled=False)

    facts = _all_facts(_source(corpus, extractors, _StubCaptionSvc()))

    assert len(_nodes(facts, "visual_artifact")) == 0
    assert len(_caption_chunks(facts)) == 0


# ---------------------------------------------------------------------------
# Test 4a — detected figure → pdf_figure node with bbox/confidence + caption
# ---------------------------------------------------------------------------


def test_pdf_detected_figure_emits_pdf_figure_visual(tmp_path: Path) -> None:
    pdf = _make_pdf_with_image(_make_png(105, 105))
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "doc.pdf").write_bytes(pdf)
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    _write_extractors_yaml(extractors, enabled=True, pdf=True)

    detector = _StubFigureDetector([
        {"page": 1, "bbox": [10.0, 40.0, 120.0, 150.0], "conf": 0.93,
         "kind": "figure", "caption_text": "Figure 1: a blue square"},
    ])
    facts = _all_facts(
        _source(corpus, extractors, _StubCaptionSvc(), detector),
    )

    visuals = _nodes(facts, "visual_artifact")
    assert {v.attrs["source_kind"] for v in visuals} == {"pdf_figure"}
    va = visuals[0]
    assert va.attrs["page_number"] == 1
    assert va.attrs["render_sha256"] != va.attrs["content_sha256"]  # cropped
    assert va.attrs["confidence"] == 0.93
    assert json.loads(va.attrs["bbox"]) == [10.0, 40.0, 120.0, 150.0]

    # Discovery only: the source emits no caption chunk on the figure path.
    # Instead the node carries the enricher's inputs (crop ref + context).
    assert _caption_chunks(facts) == []
    assert va.attrs["crop_blob_ref"] == va.attrs["render_sha256"]
    assert va.attrs["caption_context"] == "Figure 1: a blue square"


# ---------------------------------------------------------------------------
# Test 4b — two figures on one page → two distinct pdf_figure visuals; a table
# box in the same detection is never cropped
# ---------------------------------------------------------------------------


def test_pdf_two_figures_distinct_table_not_cropped(tmp_path: Path) -> None:
    pdf = _make_pdf_with_vectors()
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "doc.pdf").write_bytes(pdf)
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    _write_extractors_yaml(extractors, enabled=True, pdf=True)

    detector = _StubFigureDetector([
        {"page": 1, "bbox": [10.0, 10.0, 90.0, 90.0], "conf": 0.9,
         "kind": "figure"},
        {"page": 1, "bbox": [100.0, 10.0, 180.0, 90.0], "conf": 0.8,
         "kind": "figure"},
        {"page": 1, "bbox": [10.0, 100.0, 180.0, 200.0], "conf": 0.7,
         "kind": "table"},
    ])
    facts = _all_facts(
        _source(corpus, extractors, _StubCaptionSvc(), detector),
    )

    visuals = _nodes(facts, "visual_artifact")
    # Two figures cropped, table ignored.
    assert all(v.attrs["source_kind"] == "pdf_figure" for v in visuals)
    assert len(visuals) == 2
    # Distinct content identity per bbox (no collapse onto one visual).
    assert len({v.node_id for v in visuals}) == 2
    bboxes = {tuple(json.loads(v.attrs["bbox"])) for v in visuals}
    assert bboxes == {
        (10.0, 10.0, 90.0, 90.0), (100.0, 10.0, 180.0, 90.0),
    }
    # Discovery only: no caption chunks from the source; each figure visual
    # carries a crop ref for the enricher.
    assert _caption_chunks(facts) == []
    assert all(v.attrs["crop_blob_ref"] == v.attrs["render_sha256"] for v in visuals)


# ---------------------------------------------------------------------------
# Test 5 — a VLM failure does not erase the discovered visual
# ---------------------------------------------------------------------------


def test_caption_failure_keeps_visual_artifact(tmp_path: Path) -> None:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "figure.png").write_bytes(_make_png(200, 200))
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    _write_extractors_yaml(extractors, enabled=True)

    facts = _all_facts(_source(corpus, extractors, _FailingCaptionSvc()))

    visuals = _nodes(facts, "visual_artifact")
    assert len(visuals) == 1  # survived the failure
    assert visuals[0].attrs["caption_status"] == "failed"
    assert visuals[0].attrs["caption_skip_reason"] == "provider_error"
    assert len(_caption_chunks(facts)) == 0
    assert len(_nodes(facts, "document")) == 1          # document still emitted


# ---------------------------------------------------------------------------
# Test 6 — vision_signature is sensitive to the caption model
# ---------------------------------------------------------------------------


def test_vision_signature_changes_with_model(tmp_path: Path) -> None:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "figure.png").write_bytes(_make_png(105, 105))
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    _write_extractors_yaml(extractors, enabled=True)

    sig_a = _caption_chunks(_all_facts(
        _source(corpus, extractors, _StubCaptionSvc(model="chatgpt/gpt-4o"))
    ))[0].attrs["vision_signature"]
    sig_b = _caption_chunks(_all_facts(
        _source(corpus, extractors, _StubCaptionSvc(model="openrouter/gemma"))
    ))[0].attrs["vision_signature"]
    assert sig_a != sig_b


# ---------------------------------------------------------------------------
# Test 7 — enabled PDF path with pymupdf missing fails loud (no silent skip)
# ---------------------------------------------------------------------------


def test_missing_pymupdf_on_enabled_pdf_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    pdf = _make_pdf_with_image(_make_png(105, 105))  # built while fitz present
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "doc.pdf").write_bytes(pdf)
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    _write_extractors_yaml(extractors, enabled=True, pdf=True)

    # Simulate pymupdf being unavailable at caption time.
    monkeypatch.setitem(sys.modules, "fitz", None)

    with pytest.raises(SubstrateError, match="pymupdf"):
        _all_facts(_source(corpus, extractors, _StubCaptionSvc()))
