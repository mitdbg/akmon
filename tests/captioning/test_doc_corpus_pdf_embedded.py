"""Tests for PDF figure discovery via inline detection (DocCorpusSource).

Since the caption-enricher split (PACT visual-caption-enricher), the source's
PDF figure path is **discovery only**: it runs the (stubbed here) figure
detector, crops each kept figure box, STAGES the crop into the injected crop
store, and emits one ``pdf_figure`` ``visual_artifact`` per box carrying
``bbox`` + ``confidence`` + ``render_sha256`` + ``crop_blob_ref`` +
``caption_context`` — and NO ``vision_caption`` chunk. VLM captioning is the
VisualCaptionEnricher's job (Phase B), tested in
``test_visual_caption_enricher.py``.

Creates a synthetic in-memory PDF with one embedded PNG using pymupdf.

Asserts:
- Discovery emits a pdf_figure visual per kept box with bbox/confidence and the
  crop staged + context persisted; the source makes NO caption call.
- Text parsing still runs — the document node is emitted.
- Tables are never cropped/emitted as visuals.
- Loud-failure contract: detector crash / malformed box / out-of-range box /
  figure_detection-off-on-PDF / missing-dsn all raise, never silently drop.
"""
from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path

import pytest
import yaml
from PIL import Image as PILImage

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources.doc_corpus import DocCorpusSource
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.library.captioning.crop_store import InMemoryCropStore


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_png_bytes(w: int = 105, h: int = 105) -> bytes:
    img = PILImage.new("RGB", (w, h), color=(80, 120, 160))
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def _write_extractors_yaml(d: Path, *, enabled: bool = True) -> None:
    cfg = {
        "parser": [
            {"format": "pdf",
             "class": "okg.substrate.library.extraction.parse.pdf_text.PdfTextParser"},
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
            # Inline figure detection is the only PDF visual-discovery path;
            # tests inject a stub detector so no DocLayout-YOLO/torch is needed.
            "figure_detection": True,
        }
    else:
        cfg["captioning"] = {"enabled": False}
    (d / "extractors.yaml").write_text(yaml.safe_dump(cfg))


class _StubFigureDetector:
    """Stand-in for FigureDetector: returns a fixed box list (no YOLO).

    Default: one figure box on page 1 over the inserted image region, with an
    anchored "Figure 1" caption. `boxes` overrides the list; `fail=True` raises
    from `detect()` to exercise detector-failure handling.
    """

    def __init__(self, boxes: list | None = None, *, fail: bool = False) -> None:
        self._boxes = boxes
        self._fail = fail

    def detect(self, doc: object) -> list:
        if self._fail:
            raise RuntimeError("intentional detector failure")
        if self._boxes is not None:
            return self._boxes
        return [{
            "page": 1, "bbox": [10.0, 40.0, 120.0, 150.0], "conf": 0.95,
            "kind": "figure", "caption_text": "Figure 1: a blue square",
        }]


class _StubCaptionSvc:
    """A caption service that records calls. On the discovery-only figure path
    it must NEVER be called; tests assert `.contexts == []`."""

    def __init__(self) -> None:
        self.contexts: list[str] = []

    def caption(self, image: object, context: str = "", **kwargs: object) -> list:
        self.contexts.append(context)
        return []


def _make_pdf_with_image(png_bytes: bytes) -> bytes:
    """Build a minimal PDF containing one embedded PNG using pymupdf (fitz)."""
    try:
        import fitz
    except ImportError:
        pytest.skip("pymupdf not installed — skipping PDF embedded image test")

    doc = fitz.open()
    page = doc.new_page(width=200, height=300)
    page.insert_text((10, 20), "Test document with embedded image")
    rect = fitz.Rect(10, 40, 120, 150)
    page.insert_image(rect, stream=png_bytes)
    return doc.tobytes()


def _run_and_collect(source: DocCorpusSource) -> list[NodeFact]:
    run = source.run("test-run-id", mode="scope_complete", cursor=None)
    return [f for f in run.facts if isinstance(f, NodeFact)]


def _caption_chunks(facts: list[NodeFact]) -> list[NodeFact]:
    return [
        f for f in facts
        if f.subtype == "document_chunk"
        and f.attrs.get("chunk_source") == "vision_caption"
    ]


def _visuals(facts: list[NodeFact]) -> list[NodeFact]:
    return [f for f in facts if f.subtype == "visual_artifact"]


def _build_src(
    tmp_path: Path,
    *,
    detector: _StubFigureDetector,
    crop_store: InMemoryCropStore | None = None,
    caption_service: _StubCaptionSvc | None = None,
    enabled: bool = True,
) -> DocCorpusSource:
    pdf_bytes = _make_pdf_with_image(_make_png_bytes())
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "doc.pdf").write_bytes(pdf_bytes)
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    _write_extractors_yaml(extractors, enabled=enabled)
    return DocCorpusSource(
        repo="test-repo",
        repo_root=corpus, non_git_walk=True,
        extractors_dir=extractors,
        caption_service=caption_service or _StubCaptionSvc(),
        figure_detector=detector,
        figure_crop_store=crop_store if crop_store is not None else InMemoryCropStore(),
    )


# ---------------------------------------------------------------------------
# Test 1 (task.persist-crop-and-context verification) — discovery-only:
# one pdf_figure visual per box with crop staged + context persisted, NO VLM.
# ---------------------------------------------------------------------------


def test_discovery_only_stages_crop_and_context_no_vlm(tmp_path: Path) -> None:
    try:
        import fitz  # noqa: F401
    except ImportError:
        pytest.skip("pymupdf not installed")

    store = InMemoryCropStore()
    cap_svc = _StubCaptionSvc()
    src = _build_src(
        tmp_path,
        detector=_StubFigureDetector(),
        crop_store=store,
        caption_service=cap_svc,
    )
    facts = _run_and_collect(src)

    # The source emits NO caption chunk on the figure path, and never calls the
    # caption service.
    assert _caption_chunks(facts) == []
    assert cap_svc.contexts == [], "source must not call the VLM on the figure path"

    # Exactly one pdf_figure discovery visual, carrying the enricher's inputs.
    visuals = _visuals(facts)
    assert len(visuals) == 1
    attrs = visuals[0].attrs
    assert attrs["source_kind"] == "pdf_figure"
    assert attrs.get("confidence") == 0.95
    assert json.loads(attrs["bbox"]) == [10.0, 40.0, 120.0, 150.0]
    # Anchored "Figure N" context is persisted verbatim (so the enricher
    # reproduces the vision_signature) and the crop is staged under its
    # render_sha256 key.
    assert attrs["caption_context"] == "Figure 1: a blue square"
    ref = attrs["crop_blob_ref"]
    assert ref == attrs["render_sha256"]
    staged = store.get(ref)
    assert hashlib.sha256(staged).hexdigest() == ref

    # Text parsing still runs — the document node is emitted.
    assert len([f for f in facts if f.subtype == "document"]) == 1


# ---------------------------------------------------------------------------
# Test 2 — captioning.enabled=false → no visuals, no caption chunks.
# ---------------------------------------------------------------------------


def test_pdf_captioning_disabled_no_visuals(tmp_path: Path) -> None:
    try:
        import fitz  # noqa: F401
    except ImportError:
        pytest.skip("pymupdf not installed")

    pdf_bytes = _make_pdf_with_image(_make_png_bytes())
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "doc.pdf").write_bytes(pdf_bytes)
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
    assert _caption_chunks(facts) == []
    assert _visuals(facts) == []


# ---------------------------------------------------------------------------
# Test 3 — discovery emits both the document and the figure visual (text
# parsing coexists with figure discovery). The old "captioning failure does
# not abort text parsing" property moved to the enricher (its incomplete/
# watermark-hold path; see test_visual_caption_enricher.py).
# ---------------------------------------------------------------------------


def test_discovery_emits_document_and_figure_visual(tmp_path: Path) -> None:
    try:
        import fitz  # noqa: F401
    except ImportError:
        pytest.skip("pymupdf not installed")

    src = _build_src(tmp_path, detector=_StubFigureDetector())
    facts = _run_and_collect(src)
    assert len([f for f in facts if f.subtype == "document"]) == 1
    assert len(_visuals(facts)) == 1


def test_max_images_per_doc_does_not_cap_discovery(tmp_path: Path) -> None:
    """acc.cap-off-discovery: max_images_per_doc no longer drops discovery
    nodes — every kept box becomes a visual (the cap moved to the enricher's
    VLM fan-out). The F3 silent-drop is gone."""
    try:
        import fitz  # noqa: F401
    except ImportError:
        pytest.skip("pymupdf not installed")

    pdf_bytes = _make_pdf_with_image(_make_png_bytes())
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "doc.pdf").write_bytes(pdf_bytes)
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    _write_extractors_yaml(extractors, enabled=True)
    # Set the cap BELOW the kept-box count (3 figures, cap 1).
    cfg = yaml.safe_load((extractors / "extractors.yaml").read_text())
    cfg["captioning"]["max_images_per_doc"] = 1
    (extractors / "extractors.yaml").write_text(yaml.safe_dump(cfg))

    boxes = [
        {"page": 1, "bbox": [10.0, 40.0, 60.0, 90.0], "conf": 0.9, "kind": "figure"},
        {"page": 1, "bbox": [70.0, 40.0, 119.0, 90.0], "conf": 0.9, "kind": "figure"},
        {"page": 1, "bbox": [10.0, 100.0, 119.0, 149.0], "conf": 0.9, "kind": "figure"},
    ]
    src = DocCorpusSource(
        repo="test-repo",
        repo_root=corpus, non_git_walk=True,
        extractors_dir=extractors,
        caption_service=_StubCaptionSvc(),
        figure_detector=_StubFigureDetector(boxes),
        figure_crop_store=InMemoryCropStore(),
    )
    facts = _run_and_collect(src)
    # All three kept boxes become discovery visuals despite the cap of 1.
    assert len(_visuals(facts)) == 3


# ---------------------------------------------------------------------------
# Test 4 — figure boxes emit bbox/confidence; tables are never cropped/emitted
# as visuals; the anchored caption becomes the persisted context (req.inline-
# detector + req.caption-anchoring carry through discovery only).
# ---------------------------------------------------------------------------


def test_inline_yolo_regions_emit_bbox_and_no_table_crop(tmp_path: Path) -> None:
    try:
        import fitz  # noqa: F401
    except ImportError:
        pytest.skip("pymupdf not installed")

    boxes = [
        {"page": 1, "bbox": [10.0, 40.0, 120.0, 150.0], "conf": 0.91,
         "kind": "figure", "caption_text": "Figure 1: a blue square"},
        {"page": 1, "bbox": [10.0, 160.0, 120.0, 260.0], "conf": 0.88,
         "kind": "table"},
    ]
    cap_svc = _StubCaptionSvc()
    src = _build_src(
        tmp_path, detector=_StubFigureDetector(boxes), caption_service=cap_svc,
    )
    facts = _run_and_collect(src)

    # No caption chunk (captioning is the enricher's job); no VLM call.
    assert _caption_chunks(facts) == []
    assert cap_svc.contexts == []
    # Exactly one visual (the figure); the table is never cropped/emitted.
    visuals = _visuals(facts)
    assert len(visuals) == 1, f"table must not become a visual; got {len(visuals)}"
    attrs = visuals[0].attrs
    assert attrs.get("confidence") == 0.91
    assert json.loads(attrs["bbox"]) == [10.0, 40.0, 120.0, 150.0]
    assert attrs["caption_context"] == "Figure 1: a blue square"


def test_inline_detection_persists_page_text_context_without_anchor(
    tmp_path: Path,
) -> None:
    """A figure with no anchored caption persists the page text as context."""
    try:
        import fitz  # noqa: F401
    except ImportError:
        pytest.skip("pymupdf not installed")

    boxes = [{"page": 1, "bbox": [10.0, 40.0, 120.0, 150.0], "conf": 0.9,
              "kind": "figure"}]
    src = _build_src(tmp_path, detector=_StubFigureDetector(boxes))
    facts = _run_and_collect(src)
    visuals = _visuals(facts)
    assert len(visuals) == 1
    assert "Test document with embedded image" in visuals[0].attrs["caption_context"]


# ---------------------------------------------------------------------------
# Test 5 — captioning-enabled PDF with figure_detection=false fails loud.
# ---------------------------------------------------------------------------


def test_captioning_pdf_without_figure_detection_raises(tmp_path: Path) -> None:
    try:
        import fitz  # noqa: F401
    except ImportError:
        pytest.skip("pymupdf not installed")

    pdf_bytes = _make_pdf_with_image(_make_png_bytes())
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "doc.pdf").write_bytes(pdf_bytes)
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    _write_extractors_yaml(extractors, enabled=True)
    cfg = yaml.safe_load((extractors / "extractors.yaml").read_text())
    cfg["captioning"]["figure_detection"] = False
    (extractors / "extractors.yaml").write_text(yaml.safe_dump(cfg))

    src = DocCorpusSource(
        repo="test-repo",
        repo_root=corpus, non_git_walk=True,
        extractors_dir=extractors,
        caption_service=_StubCaptionSvc(),
    )
    with pytest.raises(SubstrateError, match="figure_detection is false"):
        _run_and_collect(src)


# ---------------------------------------------------------------------------
# Test 6 (acc.missing-dsn-loud) — figure_detection on but no crop store and no
# dsn fails loud at ingest start (crops cannot be staged), never silently.
# ---------------------------------------------------------------------------


def test_missing_crop_store_and_dsn_fails_loud(tmp_path: Path) -> None:
    try:
        import fitz  # noqa: F401
    except ImportError:
        pytest.skip("pymupdf not installed")

    pdf_bytes = _make_pdf_with_image(_make_png_bytes())
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "doc.pdf").write_bytes(pdf_bytes)
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    _write_extractors_yaml(extractors, enabled=True)
    # Stub detector (so YOLO is not required), but NO crop store and NO dsn.
    src = DocCorpusSource(
        repo="test-repo",
        repo_root=corpus, non_git_walk=True,
        extractors_dir=extractors,
        caption_service=_StubCaptionSvc(),
        figure_detector=_StubFigureDetector(),
    )
    with pytest.raises(SubstrateError, match="figure crops cannot be staged"):
        _run_and_collect(src)


# ---------------------------------------------------------------------------
# Test 7 — a detector crash fails loud (not swallowed into zero figures).
# ---------------------------------------------------------------------------


def test_detector_failure_fails_loud(tmp_path: Path) -> None:
    try:
        import fitz  # noqa: F401
    except ImportError:
        pytest.skip("pymupdf not installed")

    src = _build_src(tmp_path, detector=_StubFigureDetector(fail=True))
    with pytest.raises(SubstrateError, match="inline figure detection failed"):
        _run_and_collect(src)


# ---------------------------------------------------------------------------
# Test 8 — a KEPT box that is malformed or out-of-range fails loud, rather than
# being logged-and-skipped into no graph fact (silent-drop P0).
# ---------------------------------------------------------------------------


def test_malformed_kept_box_fails_loud(tmp_path: Path) -> None:
    try:
        import fitz  # noqa: F401
    except ImportError:
        pytest.skip("pymupdf not installed")

    boxes = [{"page": 1, "bbox": [10.0, 40.0, 120.0, "nope"], "conf": 0.9,
              "kind": "figure"}]
    src = _build_src(tmp_path, detector=_StubFigureDetector(boxes))
    with pytest.raises(SubstrateError, match="malformed box"):
        _run_and_collect(src)


def test_out_of_range_page_box_fails_loud(tmp_path: Path) -> None:
    try:
        import fitz  # noqa: F401
    except ImportError:
        pytest.skip("pymupdf not installed")

    boxes = [{"page": 99, "bbox": [10.0, 40.0, 120.0, 150.0], "conf": 0.9,
              "kind": "figure"}]
    src = _build_src(tmp_path, detector=_StubFigureDetector(boxes))
    with pytest.raises(SubstrateError, match="out-of-range box"):
        _run_and_collect(src)
