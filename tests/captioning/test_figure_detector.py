"""Unit tests for the inline DocLayout-YOLO FigureDetector.

req.inline-detector + req.caption-anchoring
(PACT yolo-figure-detection-at-ingest).

The detector's geometry — the locked filter (conf >= 0.5, area >= 0.5% page,
containment suppression), table retention as inert provenance, and
figure_caption anchoring — is exercised with a FAKE YOLO predictor, so no
DocLayout-YOLO/torch is required. Coordinates are supplied in the detector's
2x render space (RENDER_SCALE) and asserted back in PDF points.
"""
from __future__ import annotations

import pytest

from okg.substrate.library.captioning.figure_detector import (
    RENDER_SCALE,
    FigureDetector,
)


# --- Fake YOLO result objects (mimic doclayout_yolo's predict()[0]) ---

class _FakeBox:
    def __init__(self, cls_id: int, conf: float, xyxy: list[float]) -> None:
        self.cls = cls_id
        self.conf = conf
        self.xyxy = [list(xyxy)]


class _FakeResult:
    # Class ids mirror DocLayout-YOLO's DocStructBench label set (subset).
    names = {0: "figure", 1: "table", 2: "figure_caption", 3: "plain_text"}

    def __init__(self, boxes: list[_FakeBox]) -> None:
        self.boxes = boxes


def _predictor(boxes: list[_FakeBox]):
    def _predict(_img: object) -> _FakeResult:
        return _FakeResult(boxes)

    return _predict


def _one_page_doc(caption_text: str | None = None):
    """Open a single 200x300pt fitz doc, optionally with caption text at y=205."""
    fitz = pytest.importorskip("fitz")
    doc = fitz.open()
    page = doc.new_page(width=200, height=300)
    if caption_text:
        page.insert_text((12, 205), caption_text)
    return doc


def test_filter_suppress_anchor() -> None:
    """One kept figure (nested/low-conf/tiny dropped), one table, caption
    anchored from the overlapping figure_caption box."""
    doc = _one_page_doc("Figure 1: the anchored caption")
    s = RENDER_SCALE
    # Coordinates in 2x render space; page area (2x) = 400*600 = 240000,
    # so the min-area floor (0.5%) is 1200 px^2.
    boxes = [
        _FakeBox(0, 0.90, [10 * s, 40 * s, 120 * s, 150 * s]),   # figure, kept
        _FakeBox(0, 0.85, [30 * s, 60 * s, 100 * s, 130 * s]),   # nested -> drop
        _FakeBox(0, 0.30, [10 * s, 160 * s, 120 * s, 210 * s]),  # low conf -> drop
        _FakeBox(0, 0.95, [10 * s, 220 * s, 25 * s, 235 * s]),   # tiny -> drop
        _FakeBox(1, 0.80, [10 * s, 245 * s, 120 * s, 290 * s]),  # table, kept
        _FakeBox(2, 0.60, [10 * s, 195 * s, 200 * s, 215 * s]),  # caption
        _FakeBox(3, 0.99, [0, 0, 200 * s, 20 * s]),              # plain_text -> ignored
    ]
    detector = FigureDetector(predictor=_predictor(boxes))
    out = detector.detect(doc)

    figures = [b for b in out if b["kind"] == "figure"]
    tables = [b for b in out if b["kind"] == "table"]
    assert len(figures) == 1, f"expected one kept figure, got {figures}"
    assert len(tables) == 1, f"expected one table box, got {tables}"

    fig = figures[0]
    assert fig["page"] == 1
    assert fig["bbox"] == [10.0, 40.0, 120.0, 150.0]  # divided back to PDF pts
    assert fig["conf"] == 0.9
    assert "Figure 1: the anchored caption" in fig["caption_text"]

    assert tables[0]["bbox"] == [10.0, 245.0, 120.0, 290.0]
    assert "caption_text" not in tables[0]  # tables are never anchored


def test_anchored_caption_used_as_context() -> None:
    """A figure_caption box overlapping the figure yields caption_text (the VLM
    context); a caption beyond CAPTION_MAX_GAP does not anchor."""
    s = RENDER_SCALE

    # Near caption (gap 45pt <= 80) -> anchored.
    near = _one_page_doc("Figure 2: nearby caption")
    near_boxes = [
        _FakeBox(0, 0.9, [10 * s, 40 * s, 120 * s, 150 * s]),
        _FakeBox(2, 0.6, [10 * s, 195 * s, 200 * s, 215 * s]),
    ]
    fig = FigureDetector(predictor=_predictor(near_boxes)).detect(near)[0]
    assert fig.get("caption_text") and "Figure 2" in fig["caption_text"]

    # Far caption (gap ~130pt > 80) -> no anchor, so caption_text absent and
    # the source falls back to page text.
    far = _one_page_doc("Figure 3: far caption")
    far_boxes = [
        _FakeBox(0, 0.9, [10 * s, 10 * s, 120 * s, 60 * s]),
        _FakeBox(2, 0.6, [10 * s, 195 * s, 200 * s, 215 * s]),
    ]
    fig_far = FigureDetector(predictor=_predictor(far_boxes)).detect(far)[0]
    assert "caption_text" not in fig_far


def test_detector_is_import_free_until_predict() -> None:
    """Constructing a detector must not import doclayout_yolo (torch)."""
    import sys

    sys.modules.pop("doclayout_yolo", None)
    FigureDetector()  # no predictor, no weights — must not import the model
    assert "doclayout_yolo" not in sys.modules
