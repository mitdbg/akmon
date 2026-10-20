"""Image rendering shared by ingest and the read surface.

`doc_corpus` rasterises standalone PDF figures for captioning; the MCP
`inspect(kind="image")` read (issue #3046) rasterises the same kind of
file for an agent to look at. One renderer, so a figure an agent sees is
the figure a caption was made from.

PDFium comes from the optional ``pypdfium2`` package and is imported
lazily; its absence is a named outcome, never an ImportError. Pillow is
a core dependency.
"""
from __future__ import annotations

import hashlib
import io
import logging
from dataclasses import dataclass
from typing import Optional

log = logging.getLogger(__name__)

PDF_MAGIC_SCAN_BYTES: int = 1024


def has_pdf_magic(content: bytes) -> bool:
    """Return whether the payload carries a PDF header near its start.

    PDF readers permit a small leading preamble, so inspect the first KiB for
    the header marker. The filename is deliberately irrelevant: LaTeX projects
    may copy figures through extensionless paths.
    """
    return b"%PDF-" in content[:PDF_MAGIC_SCAN_BYTES]


def pdfium_renderer_version() -> Optional[str]:
    """Return a stable pypdfium2/PDFium version string, or None if unavailable."""
    try:
        import pypdfium2 as pdfium  # type: ignore[import-untyped]
    except ImportError:
        return None
    return f"pypdfium2-{pdfium.PYPDFIUM_INFO};pdfium-{pdfium.PDFIUM_INFO}"


def render_pdf_first_page(
    pdf_bytes: bytes,
    *,
    dpi: int,
    image_label: str,
) -> tuple[Optional[bytes], Optional[str], Optional[str]]:
    """Rasterize page 1 to PNG.

    Returns ``(png_bytes, renderer_version, failure_reason)``. Failure reasons
    are closed, graph-visible outcomes -- ``pdf_renderer_unavailable`` or
    ``pdf_render_failed`` -- and exception text remains diagnostic-only.
    """
    try:
        import pypdfium2 as pdfium  # type: ignore[import-untyped]
    except ImportError:
        log.warning("PDFium is unavailable to render %s", image_label)
        return None, None, "pdf_renderer_unavailable"

    renderer_version = pdfium_renderer_version()
    document = page = bitmap = pil_image = rgb_image = None
    try:
        document = pdfium.PdfDocument(pdf_bytes)
        if len(document) < 1:
            raise ValueError("PDF contains no pages")
        page = document[0]
        bitmap = page.render(scale=dpi / 72.0)
        pil_image = bitmap.to_pil()
        rgb_image = pil_image.convert("RGB")
        output = io.BytesIO()
        rgb_image.save(output, format="PNG")
        return output.getvalue(), renderer_version, None
    except Exception:
        log.warning(
            "PDFium could not render page 1 of %s", image_label, exc_info=True,
        )
        return None, renderer_version, "pdf_render_failed"
    finally:
        for resource in (rgb_image, pil_image, bitmap, page, document):
            if resource is not None:
                try:
                    resource.close()
                except Exception:
                    # Cleanup is best-effort and must not replace the renderer
                    # result (or its named failure) with a close-time error.
                    pass


# ── preparing an image for a reader ────────────────────────────────────

#: What a reader may be sent, by the format the bytes turn out to be.
#: Decided by magic bytes, never by a filename.
_PNG_MAGIC = b"\x89PNG\r\n\x1a\n"
_JPEG_MAGIC = b"\xff\xd8\xff"
_PS_MAGIC = b"%!PS"


def sniff_image_format(content: bytes) -> str:
    """``pdf`` / ``png`` / ``jpeg`` / ``postscript`` / ``svg`` / ``unknown``."""
    if content.startswith(_PNG_MAGIC):
        return "png"
    if content.startswith(_JPEG_MAGIC):
        return "jpeg"
    if content.startswith(_PS_MAGIC):
        return "postscript"  # EPS included; checked before the PDF scan
    if has_pdf_magic(content):
        return "pdf"
    head = content[:512].lstrip().lower()
    if head.startswith(b"<svg") or (head.startswith(b"<?xml") and b"<svg" in content[:2048]):
        return "svg"
    return "unknown"


@dataclass(frozen=True)
class ImageLimits:
    """How big an image a reader is sent."""

    dpi: int = 100
    max_side: int = 1600
    #: A single image above this is re-encoded as JPEG rather than dropped.
    jpeg_above_bytes: int = 750_000


@dataclass(frozen=True)
class PreparedImage:
    data: bytes
    mime_type: str
    width: int
    height: int
    source_format: str

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.data).hexdigest()


class ImageRefused(ValueError):
    """The bytes are not an image this reader can be sent.

    ``reason`` is a closed code, safe to show a caller:
    ``image_format_unsupported``, ``pdf_renderer_unavailable``,
    ``pdf_render_failed``, ``image_decode_failed``.
    """

    def __init__(self, reason: str, detail: str) -> None:
        super().__init__(detail)
        self.reason = reason


def prepare_image(content: bytes, *, limits: ImageLimits, label: str) -> PreparedImage:
    """Turn stored bytes into what a reader is sent: PDF page 1 rendered at
    ``limits.dpi``, PNG/JPEG decoded; either downscaled to ``max_side`` and
    encoded as PNG, or JPEG when the PNG would be large. Raises ImageRefused
    with a closed reason for anything else."""
    from PIL import Image

    fmt = sniff_image_format(content)
    if fmt in ("postscript", "svg", "unknown"):
        raise ImageRefused(
            "image_format_unsupported",
            f"{label}: {fmt} images cannot be rendered by this server"
            + (" (no PostScript/SVG renderer)" if fmt != "unknown" else ""),
        )
    if fmt == "pdf":
        png, _version, failure = render_pdf_first_page(
            content, dpi=limits.dpi, image_label=label,
        )
        if png is None:
            raise ImageRefused(
                failure or "pdf_render_failed",
                f"{label}: PDF page 1 could not be rendered ({failure})",
            )
        content = png
    try:
        image = Image.open(io.BytesIO(content))
        image.load()
    except Exception:
        raise ImageRefused("image_decode_failed", f"{label}: the image could not be decoded") from None
    try:
        image.thumbnail((limits.max_side, limits.max_side))
        if image.mode not in ("RGB", "RGBA", "L"):
            image = image.convert("RGBA" if "A" in image.getbands() else "RGB")
        out = io.BytesIO()
        image.save(out, format="PNG", optimize=True)
        data, mime = out.getvalue(), "image/png"
        if len(data) > limits.jpeg_above_bytes:
            out = io.BytesIO()
            image.convert("RGB").save(out, format="JPEG", quality=85)
            data, mime = out.getvalue(), "image/jpeg"
        width, height = image.size
    finally:
        image.close()
    return PreparedImage(data=data, mime_type=mime, width=width, height=height, source_format=fmt)
