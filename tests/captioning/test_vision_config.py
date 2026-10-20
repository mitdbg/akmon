"""Tests for `req.visual-artifacts.fail-loud-config`
(first-class-visual-artifacts).

Named directly by `pact/changes/first-class-visual-artifacts/pact.yaml`'s
`test.visual-artifacts.config` evidence. Covers the fail-loud / config-
robustness contract:

- Malformed `captioning:` config RAISES (not silently disabled).
- Absent file / absent / `false` `captioning:` block -> disabled default,
  no raise.
- Missing renderer dependency (pymupdf/`fitz`) on an ENABLED PDF path
  raises loudly.
- Oversize standalone image (> `max_bytes_per_file`) still emits a
  VisualArtifact but produces zero caption chunks.
- Undecodable image still emits a VisualArtifact WITH `mime_type` present
  (required-attr regression guard).
"""
from __future__ import annotations

import io
import sys
from pathlib import Path

import pytest
import yaml
from PIL import Image as PILImage

from okg.substrate.errors import SubstrateError
from okg.substrate.library.captioning.service import CaptionResult
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.library.sources.doc_corpus import (
    DocCorpusSource,
    _load_captioning_config,
)


# ---------------------------------------------------------------------------
# Helpers (mirrors test_doc_corpus_images.py / test_doc_corpus_visual_artifacts.py)
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
    cfg["captioning"] = (
        {"enabled": True, "mode": "coarse", "model": "chatgpt/gpt-4o",
         "min_image_px": 10_000}
        if enabled
        else {"enabled": False}
    )
    (d / "extractors.yaml").write_text(yaml.safe_dump(cfg))


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


class _StubCaptionSvc:
    def caption(self, image: object) -> list[CaptionResult]:
        return [CaptionResult(
            text="a stub caption",
            chunk_source="vision_caption",
            caption_model="chatgpt/gpt-4o",
            caption_provider="chatgpt",
        )]


def _source(corpus: Path, extractors: Path, svc: object, **kwargs) -> DocCorpusSource:
    return DocCorpusSource(
        repo="test-repo",
        repo_root=corpus, non_git_walk=True,
        extractors_dir=extractors,
        caption_service=svc,
        **kwargs,
    )


# ---------------------------------------------------------------------------
# Malformed config raises (fail-loud, not silent-disable)
# ---------------------------------------------------------------------------


def test_malformed_min_image_px_raises(tmp_path: Path) -> None:
    (tmp_path / "extractors.yaml").write_text(yaml.safe_dump({
        "captioning": {
            "enabled": True, "model": "openai/gpt-4o",
            "min_image_px": "100x100",
        },
    }))
    with pytest.raises(ValueError, match="min_image_px|integer"):
        _load_captioning_config(tmp_path)


def test_malformed_prompts_raises(tmp_path: Path) -> None:
    (tmp_path / "extractors.yaml").write_text(yaml.safe_dump({
        "captioning": {
            "enabled": True, "model": "openai/gpt-4o",
            "prompts": ["not", "a", "mapping"],
        },
    }))
    with pytest.raises(ValueError, match="prompts"):
        _load_captioning_config(tmp_path)


def test_enabled_without_model_raises(tmp_path: Path) -> None:
    # No default model: enabling captioning without naming a model must
    # fail loud, not silently pick one.
    (tmp_path / "extractors.yaml").write_text(yaml.safe_dump({
        "captioning": {"enabled": True, "min_image_px": 10_000},
    }))
    with pytest.raises(ValueError, match="model"):
        _load_captioning_config(tmp_path)


def test_disabled_without_model_is_fine(tmp_path: Path) -> None:
    # A disabled block never needs a model — no raise, disabled default.
    (tmp_path / "extractors.yaml").write_text(yaml.safe_dump({
        "captioning": {"enabled": False},
    }))
    cfg = _load_captioning_config(tmp_path)
    assert cfg.enabled is False
    assert cfg.model == ""


# ---------------------------------------------------------------------------
# Absent file / absent / false block -> disabled default, no raise
# ---------------------------------------------------------------------------


def test_absent_file_disables(tmp_path: Path) -> None:
    cfg = _load_captioning_config(tmp_path)
    assert cfg.enabled is False


def test_absent_block_disables(tmp_path: Path) -> None:
    (tmp_path / "extractors.yaml").write_text(yaml.safe_dump({"ner": []}))
    cfg = _load_captioning_config(tmp_path)
    assert cfg.enabled is False


def test_false_block_disables(tmp_path: Path) -> None:
    (tmp_path / "extractors.yaml").write_text(yaml.safe_dump({
        "captioning": False,
    }))
    cfg = _load_captioning_config(tmp_path)
    assert cfg.enabled is False


# ---------------------------------------------------------------------------
# Missing renderer dependency on an ENABLED PDF path raises loudly
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


# ---------------------------------------------------------------------------
# Oversize standalone image: VisualArtifact still emitted, zero captions
# (Finding 3 — the caption path has no upper size bound of its own; the
# image branch never reaches run()'s normal max_bytes_per_file guard.)
# ---------------------------------------------------------------------------


def test_oversize_image_emits_visual_artifact_no_caption(tmp_path: Path) -> None:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    png = _make_png(200, 200)
    (corpus / "huge.png").write_bytes(png)
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    _write_extractors_yaml(extractors, enabled=True)

    # max_bytes_per_file smaller than the PNG forces the oversize path.
    src = _source(
        corpus, extractors, _StubCaptionSvc(),
        max_bytes_per_file=len(png) - 1,
    )
    facts = _all_facts(src)

    visuals = _nodes(facts, "visual_artifact")
    assert len(visuals) == 1, "oversize image must still be discovered"
    assert visuals[0].attrs["byte_count"] == len(png)

    captions = _caption_chunks(facts)
    assert captions == [], (
        f"oversize image must produce zero caption chunks; got {captions}"
    )


# ---------------------------------------------------------------------------
# Undecodable image: VisualArtifact still emitted WITH mime_type present
# (Finding 1 — mime_type is required: true on VisualArtifact in schema.yaml;
# it must never be omitted, even on PIL decode failure.)
# ---------------------------------------------------------------------------


def test_undecodable_image_emits_visual_artifact_with_mime_type(
    tmp_path: Path,
) -> None:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    # `.png` extension but garbage bytes — PIL.Image.open will fail to decode.
    (corpus / "broken.png").write_bytes(b"not a real png " * 10)
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    _write_extractors_yaml(extractors, enabled=True)

    facts = _all_facts(_source(corpus, extractors, _StubCaptionSvc()))

    visuals = _nodes(facts, "visual_artifact")
    assert len(visuals) == 1, "undecodable image must still be discovered"
    assert "mime_type" in visuals[0].attrs, (
        "mime_type is required: true on VisualArtifact and must always be set"
    )
    assert visuals[0].attrs["mime_type"] == "application/octet-stream"

    # No dimensions available -> caption is skipped, but discovery holds.
    assert _caption_chunks(facts) == []


# ---------------------------------------------------------------------------
# Inline figure detection: opt-in dependency (req.optional-dependency,
# PACT yolo-figure-detection-at-ingest)
# ---------------------------------------------------------------------------


def test_figure_detection_config_parsed(tmp_path: Path) -> None:
    """The captioning block parses figure_detection + weights; default off."""
    (tmp_path / "extractors.yaml").write_text(yaml.safe_dump({
        "captioning": {
            "enabled": True, "model": "openai/gpt-4o",
            "figure_detection": True,
            "figure_detection_weights": "/models/doclayout.pt",
        },
    }))
    cfg = _load_captioning_config(tmp_path)
    assert cfg.figure_detection is True
    assert cfg.figure_detection_weights == "/models/doclayout.pt"

    (tmp_path / "extractors.yaml").write_text(yaml.safe_dump({
        "captioning": {"enabled": True, "model": "openai/gpt-4o"},
    }))
    assert _load_captioning_config(tmp_path).figure_detection is False


def test_inline_detection_requires_extra(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """figure_detection=true with no injected detector fails loud when the
    DocLayout-YOLO extra is absent — no silent fall back to zero figures.

    The extra-absent state is forced via monkeypatch so the loud path is
    exercised whether or not doclayout-yolo happens to be installed."""
    try:
        import fitz  # noqa: F401
    except ImportError:
        pytest.skip("pymupdf not installed")

    import importlib.util

    real_find_spec = importlib.util.find_spec

    def _no_doclayout(name: str, *a: object, **k: object):
        if name == "doclayout_yolo":
            return None
        return real_find_spec(name, *a, **k)

    monkeypatch.setattr(importlib.util, "find_spec", _no_doclayout)

    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "doc.pdf").write_bytes(_make_pdf_with_image(_make_png(105, 105)))
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    _write_extractors_yaml(extractors, enabled=True, pdf=True)
    # This file's _write_extractors_yaml has no figure_detection knob; add it.
    cfg = yaml.safe_load((extractors / "extractors.yaml").read_text())
    cfg["captioning"]["figure_detection"] = True
    (extractors / "extractors.yaml").write_text(yaml.safe_dump(cfg))

    # Detection enabled, NO detector injected → a real FigureDetector must be
    # built → the vision extra is required.
    src = _source(corpus, extractors, _StubCaptionSvc())
    with pytest.raises(SubstrateError, match="doclayout-yolo|vision extra"):
        _all_facts(src)


def test_require_doclayout_yolo_names_the_extra(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The loud guard raises and names the vision extra in its fix hint."""
    import importlib.util

    from okg.substrate.library.captioning.figure_detector import (
        require_doclayout_yolo,
    )

    monkeypatch.setattr(
        importlib.util, "find_spec", lambda name, *a, **k: None,
    )
    with pytest.raises(SubstrateError) as exc:
        require_doclayout_yolo()
    assert "vision" in str(exc.value.fix_hint).lower()


def test_missing_extra_fails_at_ingest_start_without_any_pdf(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """acc.misconfig-loud: the extra guard fires at ingest START, not first-PDF.

    A corpus with NO PDF at all still raises when figure_detection is enabled
    and the extra is absent, proving the check runs at config/startup time and
    is not deferred until a PDF happens to be processed."""
    import importlib.util

    real_find_spec = importlib.util.find_spec

    def _no_doclayout(name: str, *a: object, **k: object):
        if name == "doclayout_yolo":
            return None
        return real_find_spec(name, *a, **k)

    monkeypatch.setattr(importlib.util, "find_spec", _no_doclayout)

    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "notes.txt").write_text("no pdf here, only text")  # NO PDF
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    _write_extractors_yaml(extractors, enabled=True, pdf=False)
    cfg = yaml.safe_load((extractors / "extractors.yaml").read_text())
    cfg["captioning"]["figure_detection"] = True
    (extractors / "extractors.yaml").write_text(yaml.safe_dump(cfg))

    src = _source(corpus, extractors, _StubCaptionSvc())
    with pytest.raises(SubstrateError, match="doclayout-yolo|vision extra"):
        _all_facts(src)


# ---------------------------------------------------------------------------
# Weights identity — pinned default revision + content-addressed local weights
# (req.optional-dependency: model changes must not reuse stale detections).
# ---------------------------------------------------------------------------


def test_default_weights_identity_pins_revision_and_digest() -> None:
    from okg.substrate.library.captioning.figure_detector import (
        _DEFAULT_WEIGHTS_REVISION,
        _DEFAULT_WEIGHTS_SHA256,
        default_weights_identity,
        weights_identity,
    )

    ident = default_weights_identity()
    assert _DEFAULT_WEIGHTS_REVISION in ident
    assert _DEFAULT_WEIGHTS_SHA256 in ident
    # Unset weights resolve to the pinned default identity.
    assert weights_identity(None) == ident


def test_weights_identity_local_file_is_content_addressed(tmp_path: Path) -> None:
    from okg.substrate.library.captioning.figure_detector import weights_identity

    w = tmp_path / "weights.pt"
    w.write_bytes(b"model-A")
    id1 = weights_identity(str(w))
    assert "sha256" in id1
    # Re-reading the same bytes is stable.
    assert weights_identity(str(w)) == id1
    # Replacing the weights in place (same path) perturbs the identity.
    w.write_bytes(b"model-A-v2")
    assert weights_identity(str(w)) != id1


def test_vision_fingerprint_folds_weights_identity(tmp_path: Path) -> None:
    """A weights change (different file, or in-place replacement) changes the
    vision fingerprint, so unchanged-file cursors do not reuse stale
    detections."""
    from okg.substrate.library.sources.doc_corpus import CaptioningConfig

    corpus = tmp_path / "corpus"
    corpus.mkdir()
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    _write_extractors_yaml(extractors, enabled=True, pdf=True)
    src = _source(corpus, extractors, _StubCaptionSvc())

    w1 = tmp_path / "w1.pt"
    w1.write_bytes(b"model-A")
    w2 = tmp_path / "w2.pt"
    w2.write_bytes(b"model-B")

    def _fp(weights: str) -> str:
        return src._vision_gate_fingerprint(CaptioningConfig(
            enabled=True, model="chatgpt/gpt-4o",
            figure_detection=True, figure_detection_weights=weights,
        ))

    fp1, fp2 = _fp(str(w1)), _fp(str(w2))
    assert fp1 != fp2                 # different weights → different fingerprint
    assert _fp(str(w1)) == fp1        # stable for unchanged weights
    w1.write_bytes(b"model-A-v2")     # replace in place
    assert _fp(str(w1)) != fp1
