"""Inline DocLayout-YOLO figure-region detection for DocCorpusSource.

Promotes the figure-detection logic of
`deployments/irpapers-bench/scripts/detect_figures.py` (the most refined and
tested variant) from an offline sidecar into an at-ingest detector, so figure
regions are bounded during projection (Phase A) rather than by an out-of-band
preprocessing step.

`detect()` returns boxes in the SAME schema the precomputed `figures.json`
sidecar used, so the existing crop -> caption -> emit machinery in
`DocCorpusSource` is reused unchanged::

    {"page": int (1-based),
     "bbox": [x0, y0, x1, y1]  # PDF points, top-left origin
     "conf": float,
     "kind": "figure" | "table",
     "caption_text": str}      # present only when a YOLO figure_caption box
                               # anchored to this figure; used as VLM context

Filter (locked, validated at P=R=1.00 on the mmlongbench/irpapers corpora — see
the okg-vision KB notes mmlongbench-yolo-validation-results and
layout-model-figure-detection-eval): conf >= 0.5, area >= 0.5% of the page,
containment suppression keeping the outer box. Caption anchoring uses YOLO's
own ``figure_caption`` class + nearest-neighbour assignment, NOT a caption
regex (the regex is venue-fragile). ``table`` boxes are inert provenance — the
caller never crops them (their text rides the PDF text layer).

DocLayout-YOLO (AGPL-3.0) is an OPTIONAL extra (``.[vision]``). Importing this
module does NOT import it — only constructing a real (non-stubbed)
``FigureDetector`` and running detection loads the model.
"""
from __future__ import annotations

import hashlib
import io
from pathlib import Path
from typing import Any, Callable, Optional

from okg.substrate.errors import SubstrateError

# --- Locked detection constants (lifted verbatim from detect_figures.py) ---
RENDER_SCALE = 2.0          # 2x pixmap; divide coords by this to get PDF points
IMGSZ = 1024                # DocLayout-YOLO inference image size
PREDICT_CONF = 0.2          # low predict floor; the real filter is CONF below
CONF = 0.5                  # keep figures/tables scoring >= this
MIN_AREA_FRAC = 0.005       # >= 0.5% of page area (min_image_px analogue)
CAPTION_CONF = 0.3          # figure_caption anchoring floor; a miss is safe
CAPTION_MAX_GAP = 80.0      # max vertical gap (PDF pts) between figure & caption

# Default weights: the DocStructBench DocLayout-YOLO model on the HF hub. When
# `figure_detection_weights` is unset the detector resolves this via
# huggingface_hub (cached after first download).
#
# The revision is pinned to an immutable commit so an upstream model update at
# the same repo path can never silently change detections under an unchanged
# corpus (a file-identity-only cursor would not invalidate). The sha256 is the
# LFS digest of the pinned file; the download is verified against it, and the
# digest also feeds the vision fingerprint (see `default_weights_identity`) so
# any future re-pin forces a re-walk. Re-pin all three together.
_DEFAULT_WEIGHTS_REPO = "juliozhao/DocLayout-YOLO-DocStructBench"
_DEFAULT_WEIGHTS_FILE = "doclayout_yolo_docstructbench_imgsz1024.pt"
_DEFAULT_WEIGHTS_REVISION = "8c3299a30b8ff29a1503c4431b035b93220f7b11"
_DEFAULT_WEIGHTS_SHA256 = (
    "9a2ee0220fe3d9ad31b47e1d9f1282f46959a54e4618fce9cffcc9715b8286e2"
)


def default_weights_identity() -> str:
    """Stable identity of the pinned default weights (repo@revision + digest).

    Folded into the vision fingerprint so re-pinning the default model defeats
    the file-identity-only short-circuits. No download or import — pure
    constants — so it is safe to call from the fingerprint path.
    """
    return (
        f"hf:{_DEFAULT_WEIGHTS_REPO}@{_DEFAULT_WEIGHTS_REVISION}"
        f"/{_DEFAULT_WEIGHTS_FILE}#sha256:{_DEFAULT_WEIGHTS_SHA256}"
    )


def weights_identity(weights: Optional[str]) -> str:
    """Content-addressed identity of the detector weights, for fingerprinting.

    Unset → the pinned default (`default_weights_identity`). A local path →
    the file's sha256 content digest, so replacing the `.pt` in place (same
    path) perturbs the identity and defeats file-identity-only cursors. If the
    file cannot be read at fingerprint time, falls back to path+size+mtime (a
    weaker but still change-sensitive identity); a truly missing model still
    fails loud later when `FigureDetector.detect` loads it.
    """
    if not weights:
        return default_weights_identity()
    path = Path(weights).expanduser()
    try:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1 << 20), b""):
                digest.update(chunk)
        return f"file:{path}#sha256:{digest.hexdigest()}"
    except OSError:
        try:
            st = path.stat()
            return f"file:{path}#stat:{st.st_size}:{st.st_mtime_ns}"
        except OSError:
            return f"file:{path}#unresolved"


# --- Pure geometry helpers (no YOLO / no torch — unit-testable directly) ---

def _contained(inner: list[float], outer: list[float], tol: float = 2.0) -> bool:
    return (inner[0] >= outer[0] - tol and inner[1] >= outer[1] - tol
            and inner[2] <= outer[2] + tol and inner[3] <= outer[3] + tol)


def _suppress_nested(
    raw: list[tuple[list[float], float, float]],
) -> list[tuple[list[float], float, float]]:
    """Containment suppression: largest first, drop boxes nested in a kept one."""
    keep: list[tuple[list[float], float, float]] = []
    for xyxy, conf, area in sorted(raw, key=lambda x: -x[2]):
        if any(_contained(xyxy, k[0]) for k in keep):
            continue
        keep.append((xyxy, conf, area))
    return keep


def _h_overlap(a: list[float], b: list[float]) -> float:
    return max(0.0, min(a[2], b[2]) - max(a[0], b[0]))


def _anchor_caption(
    fig: list[float], captions: list[dict[str, Any]],
) -> Optional[dict[str, Any]]:
    """Nearest figure_caption box for a KEEP figure box, or None.

    Requires horizontal overlap; picks the smallest vertical gap (caption below
    or above the figure) within CAPTION_MAX_GAP. A caption overlapping the
    figure box counts as gap 0.
    """
    best: Optional[dict[str, Any]] = None
    best_gap = CAPTION_MAX_GAP
    for cap in captions:
        cb = cap["bbox"]
        if _h_overlap(fig, cb) <= 0:
            continue
        if cb[1] >= fig[3]:        # caption below figure
            gap = cb[1] - fig[3]
        elif cb[3] <= fig[1]:      # caption above figure
            gap = fig[1] - cb[3]
        else:                      # vertical overlap
            gap = 0.0
        if gap <= best_gap:
            best, best_gap = cap, gap
    return best


def require_doclayout_yolo() -> None:
    """Loud guard: raise a typed, actionable error if the vision extra is absent.

    Used at config resolution so a deployment that enables inline detection
    without the extra fails loud at startup rather than silently falling back to
    zero figures (a default-value-as-fallback P0 — see CLAUDE.md). Uses
    ``find_spec`` so the check itself does not import torch.
    """
    import importlib.util

    if importlib.util.find_spec("doclayout_yolo") is None:
        raise SubstrateError(
            "captioning.figure_detection is enabled but DocLayout-YOLO "
            "(doclayout-yolo) is not installed, so inline figure detection "
            "cannot run.",
            fix_hint="Install the vision extra (e.g. "
                     "`uv pip install -e '.[vision]'`, which pulls "
                     "doclayout-yolo), or set captioning.figure_detection=false "
                     "for this deployment.",
        )


class FigureDetector:
    """Runs DocLayout-YOLO over a PyMuPDF document, returning figure boxes.

    The model is loaded lazily on first `detect()` — constructing the detector
    is cheap and import-free. For tests, pass `predictor`: a callable taking a
    PIL image and returning a YOLO-result-like object (``.names`` mapping +
    ``.boxes`` with ``.cls`` / ``.conf`` / ``.xyxy``), so the filter,
    suppression, and anchoring logic can be exercised without torch/weights.
    """

    def __init__(
        self,
        weights: Optional[str] = None,
        *,
        predictor: Optional[Callable[[Any], Any]] = None,
    ) -> None:
        self._weights = weights
        self._predictor = predictor
        self._model: Any = None

    def _resolve_weights(self) -> str:
        if self._weights:
            return str(Path(self._weights).expanduser())
        from huggingface_hub import hf_hub_download

        # Pin the immutable commit revision so an upstream update at the same
        # repo path cannot silently change detections, then verify the LFS
        # digest before handing the file to YOLO — a mismatch is a real
        # integrity failure, not a load to shrug off.
        path = hf_hub_download(
            _DEFAULT_WEIGHTS_REPO,
            _DEFAULT_WEIGHTS_FILE,
            revision=_DEFAULT_WEIGHTS_REVISION,
        )
        digest = hashlib.sha256()
        with open(path, "rb") as handle:
            for chunk in iter(lambda: handle.read(1 << 20), b""):
                digest.update(chunk)
        if digest.hexdigest() != _DEFAULT_WEIGHTS_SHA256:
            raise SubstrateError(
                "DocLayout-YOLO default weights failed checksum verification "
                f"({_DEFAULT_WEIGHTS_FILE} at revision "
                f"{_DEFAULT_WEIGHTS_REVISION}): expected sha256 "
                f"{_DEFAULT_WEIGHTS_SHA256}, got {digest.hexdigest()}.",
                fix_hint="Clear the huggingface_hub cache and re-download, or "
                         "re-pin _DEFAULT_WEIGHTS_* in figure_detector.py to a "
                         "revision whose digest you trust.",
            )
        return path

    def _predict(self, img: Any) -> Any:
        if self._predictor is not None:
            return self._predictor(img)
        if self._model is None:
            require_doclayout_yolo()
            from doclayout_yolo import YOLOv10

            self._model = YOLOv10(self._resolve_weights())
        return self._model.predict(
            img, imgsz=IMGSZ, conf=PREDICT_CONF, verbose=False,
        )[0]

    def detect(self, doc: Any) -> list[dict[str, Any]]:
        """Detect figure/table regions across every page of an open document.

        `doc` is an open `fitz.Document`. Returns the box list described in the
        module docstring (bbox in PDF points, 1-based page). Never crops or
        renders anything itself — only the caller does.
        """
        import fitz  # pymupdf — caller (DocCorpusSource) already proved it loads
        from PIL import Image

        boxes_out: list[dict[str, Any]] = []
        for pno in range(len(doc)):
            page = doc[pno]
            pix = page.get_pixmap(
                matrix=fitz.Matrix(RENDER_SCALE, RENDER_SCALE),
            )
            img = Image.open(io.BytesIO(pix.tobytes("png"))).convert("RGB")
            result = self._predict(img)
            page_area = pix.width * pix.height

            raw_fig: list[tuple[list[float], float, float]] = []
            raw_tab: list[tuple[list[float], float, float]] = []
            captions: list[dict[str, Any]] = []
            for b in result.boxes:
                cls = result.names[int(b.cls)]
                conf = float(b.conf)
                xyxy = [float(v) for v in b.xyxy[0]]
                if cls == "figure_caption":
                    if conf >= CAPTION_CONF:
                        captions.append({
                            "bbox": [v / RENDER_SCALE for v in xyxy],
                            "conf": conf,
                        })
                    continue
                if cls not in ("figure", "table"):
                    continue
                area = (xyxy[2] - xyxy[0]) * (xyxy[3] - xyxy[1])
                if conf < CONF or area < MIN_AREA_FRAC * page_area:
                    continue
                (raw_fig if cls == "figure" else raw_tab).append(
                    (xyxy, conf, area),
                )

            for xyxy, conf, _area in _suppress_nested(raw_fig):
                bbox_pts = [round(v / RENDER_SCALE, 2) for v in xyxy]
                entry: dict[str, Any] = {
                    "page": pno + 1, "bbox": bbox_pts,
                    "conf": round(conf, 3), "kind": "figure",
                }
                cap = _anchor_caption(bbox_pts, captions)
                if cap is not None:
                    text = page.get_text(clip=fitz.Rect(cap["bbox"])).strip()
                    if text:
                        entry["caption_text"] = " ".join(text.split())
                boxes_out.append(entry)
            # Inert provenance: YOLO table boxes, same filter, never cropped.
            for xyxy, conf, _area in _suppress_nested(raw_tab):
                boxes_out.append({
                    "page": pno + 1,
                    "bbox": [round(v / RENDER_SCALE, 2) for v in xyxy],
                    "conf": round(conf, 3), "kind": "table",
                })
        return boxes_out
