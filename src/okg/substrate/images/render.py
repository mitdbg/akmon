"""Ingest's image step: complete each file image value with a stored PNG (#3046).

The source runner calls this before the fact pipeline (sanitize, canonicalize,
dedupe). For every ``{"okg_image": "file", "path": ...}`` value in a NodeFact's
attrs, it:

1. opens the file under the run's ``image_root`` -- a regular file, reached
   with no symlink on any path component, no larger than ``file_max_bytes``;
2. hashes the bytes and derives the render id from that hash, the render
   settings and the renderer;
3. reuses the ``okg.blobs`` row with that id when one exists (refreshing its
   ``last_used_at`` first, so blob cleanup cannot delete it in between), or
   renders and stores one;
4. returns the value completed with ``png`` or with a named ``error``.

Rendering itself -- everything after the bytes are read -- runs in a bounded
helper process (`okg.substrate.images.render_worker`) with a wall-clock and a
memory limit, so a hostile file cannot stall or exhaust the ingest process.

A failure before the hash (``PRE_HASH_REASONS``) is not stored: there is no
content to key it by. A failure after the hash is stored as a failure row under
the render id, so an unrenderable file is not retried until its bytes, the
settings or the renderer change. The exception is
``image_render_limit_exceeded`` (a timeout, the memory limit, a helper crash or
a PDF page too large to rasterise): it depends on the limits and the machine,
so it is not stored and a later run tries again.

A declared ``image_root`` that is not an existing directory fails the run with
`ImageRootInvalid` on its first file value, rather than completing every value
as missing.

The value carries only the relative path. The absolute root is never written to
a value, a blob row, an error or a log line; logs name the relative path.

Blob writes do not join the fact transaction. They run on their own autocommit
connection: rows are content-addressed and idempotent, so a row written by a
run that later rolls back is an orphan that blob cleanup reclaims after its
grace window, never a wrong answer.
"""
from __future__ import annotations

import dataclasses
import errno
import hashlib
import io
import json
import logging
import math
import os
import stat
import sys
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Optional

from okg.substrate.errors import SubstrateError
from okg.substrate.images.config import ImagesConfig
from okg.substrate.images.render_worker import LIMIT_REASON, RenderWorker
from okg.substrate.images.values import (
    IMAGE_TAG,
    KIND_FILE,
    PNG_MIME_TYPE,
    iter_image_values,
    with_error,
    with_png,
)

log = logging.getLogger(__name__)

#: Bumped when the render output for the same inputs would change in a way the
#: other id fields do not capture (a new encoder setting, a new format path).
RENDER_ID_VERSION = "okg-image-render/1"

#: The renderer named in the render id for PNG and JPEG sources. Deliberately
#: not the Pillow version: a Pillow upgrade would otherwise re-render every
#: stored image although decoding and scaling a raster does not meaningfully
#: change between releases. Formats no renderer handles (PostScript, SVG,
#: unknown) are keyed the same way; supporting one later bumps
#: `RENDER_ID_VERSION`.
PILLOW_RENDERER = "pillow"
#: The renderer named in the render id for a PDF when PDFium is not installed.
#: Installing it changes the id, so those files render on the next run.
PDFIUM_ABSENT = "pdfium:none"

#: Largest decoded raster accepted, in pixels. A PNG or JPEG declaring more is
#: a decompression bomb and is recorded as ``image_decode_failed``; a PDF page
#: that would rasterise larger at the render dpi is rendered at a lower dpi
#: that fits, since the result is scaled down to ``max_side`` anyway.
MAX_DECODE_PIXELS = 50_000_000

_BLOB_KIND = "image"
_READ_CHUNK = 1 << 20
_O_CLOEXEC = getattr(os, "O_CLOEXEC", 0)


# ── metrics ────────────────────────────────────────────────────────────

@dataclass
class ImageRenderMetrics:
    """What one run's image step did.

    ``rendered`` counts render-cache misses (a byte row or a failure row was
    written this run); ``reused`` counts cache hits. ``errors`` counts values
    completed with an error, by reason, whether the error was new or reused.
    ``bytes_stored`` sums the PNG bytes this run newly stored.
    """

    rendered: int = 0
    reused: int = 0
    errors: dict[str, int] = field(default_factory=dict)
    bytes_stored: int = 0

    def any(self) -> bool:
        return bool(self.rendered or self.reused or self.errors)

    def count_error(self, reason: str) -> None:
        self.errors[reason] = self.errors.get(reason, 0) + 1

    def as_dict(self) -> dict[str, Any]:
        return {
            "images_rendered": self.rendered,
            "images_reused": self.reused,
            "images_error": dict(sorted(self.errors.items())),
            "image_bytes_stored": self.bytes_stored,
        }


# ── render id ──────────────────────────────────────────────────────────

def render_id(
    source_sha256: str, *, dpi: int, max_side: int, renderer: str,
) -> str:
    """The sha256 of the canonical JSON of every input to one render."""
    payload = {
        "v": RENDER_ID_VERSION,
        "source_sha256": source_sha256,
        "dpi": dpi,
        "max_side": max_side,
        "renderer": renderer,
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("ascii")).hexdigest()


def renderer_for(source_format: str) -> str:
    """The renderer string a source of this format is keyed by."""
    if source_format == "pdf":
        from okg.substrate.imaging import pdfium_renderer_version

        return pdfium_renderer_version() or PDFIUM_ABSENT
    return PILLOW_RENDERER


# ── opening a file under the root ──────────────────────────────────────

class _Refused(Exception):
    """A pre-hash outcome: ``reason`` is one of `PRE_HASH_REASONS`."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def _os_refusal(exc: OSError) -> _Refused:
    """The per-image outcome of an OSError met while opening under the root.

    ENOENT/ENOTDIR name a file that is not there. Every other error -- ELOOP
    or EMLINK (O_NOFOLLOW meeting a symlink that appeared after the lstat),
    EACCES/EPERM, ENAMETOOLONG, EIO, ENXIO, EOVERFLOW, EINVAL, anything else
    -- refuses this one image. None escapes: one unreadable file must not
    fail the run and lose its other images, and an OSError's text names the
    absolute path, which must not reach a run's error.
    """
    if exc.errno in (errno.ENOENT, errno.ENOTDIR):
        return _Refused("image_file_missing")
    return _Refused("image_path_refused")


#: Longest path component and whole relative path accepted, in UTF-8 bytes
#: (NAME_MAX and PATH_MAX on Linux). Longer ones are refused before any
#: system call, rather than relying on ENAMETOOLONG.
MAX_PATH_COMPONENT_BYTES = 255
MAX_PATH_BYTES = 4096


def _path_length_problem(path: str) -> bool:
    """Whether `path` is too long, or not encodable as strict UTF-8.

    A lone surrogate is refused here: the filesystem encoding's
    ``surrogateescape`` would otherwise turn it into an arbitrary byte and
    open a file whose name is not the path the value names.
    """
    try:
        encoded = path.encode("utf-8")
    except UnicodeError:
        return True
    if len(encoded) > MAX_PATH_BYTES:
        return True
    return any(len(part) > MAX_PATH_COMPONENT_BYTES for part in encoded.split(b"/"))


def read_under_root(root: str, path: str, *, max_bytes: int) -> bytes:
    """Read the regular file `path` names under `root`, or raise `_Refused`.

    `path` has already passed `values.path_problem`: relative POSIX, no
    ``.``/``..`` segment, no empty segment. The walk opens each directory
    relative to the one before it (``dir_fd``) with ``O_NOFOLLOW``, so a
    symlink on any component is refused rather than followed, and a
    component swapped for a symlink between the check and the open is
    refused too (the open fails with ELOOP). The final file is lstat'ed, then
    opened ``O_NOFOLLOW | O_NONBLOCK`` (a FIFO cannot block the run), then
    fstat'ed on the open descriptor: it must be a regular file and the same
    inode the lstat saw. The size comes from that fstat, and at most
    ``max_bytes + 1`` bytes are read, so a file that grows after the check
    is still refused.

    `root` itself may be or sit under a symlink (``/tmp`` on macOS); only the
    components below it are walked. A final file with more than one hard link
    is refused: a hard link inside the root can name a file outside it, and
    the two cannot be told apart.

    Every OSError, ValueError or UnicodeError on the way becomes `_Refused`
    (see `_os_refusal`), so nothing raised here carries the absolute path.
    """
    if _path_length_problem(path):
        raise _Refused("image_path_refused")
    try:
        return _read_under_root(root, path, max_bytes=max_bytes)
    except OSError as exc:
        raise _os_refusal(exc) from None
    except (ValueError, UnicodeError):
        raise _Refused("image_path_refused") from None


def _read_under_root(root: str, path: str, *, max_bytes: int) -> bytes:
    if not (os.open in os.supports_dir_fd and os.stat in os.supports_dir_fd):
        # Every POSIX platform okg runs on has both. Refuse loudly rather than
        # fall back to a walk that could follow a swapped-in symlink.
        raise RuntimeError(
            "image ingest needs os.open/os.stat with dir_fd support on this platform"
        )
    root_real = os.path.realpath(root)
    # Belt and braces beside the walk below: the resolved target must lie
    # inside the resolved root. The walk is what makes this hold at open time.
    target_real = os.path.realpath(os.path.join(root_real, path))
    if os.path.commonpath([root_real, target_real]) != root_real:
        raise _Refused("image_path_refused")

    parts = path.split("/")
    try:
        dir_fd = os.open(root_real, os.O_RDONLY | os.O_DIRECTORY | _O_CLOEXEC)
    except OSError as exc:
        raise _os_refusal(exc) from None
    fd = -1
    try:
        for name in parts[:-1]:
            try:
                st = os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
            except OSError as exc:
                raise _os_refusal(exc) from None
            if stat.S_ISLNK(st.st_mode):
                raise _Refused("image_path_refused")
            if not stat.S_ISDIR(st.st_mode):
                # The path runs through a non-directory: no file is there.
                raise _Refused("image_file_missing")
            try:
                next_fd = os.open(
                    name,
                    os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | _O_CLOEXEC,
                    dir_fd=dir_fd,
                )
            except OSError as exc:
                raise _os_refusal(exc) from None
            os.close(dir_fd)
            dir_fd = next_fd

        name = parts[-1]
        try:
            before = os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
        except OSError as exc:
            raise _os_refusal(exc) from None
        if not stat.S_ISREG(before.st_mode):
            # A symlink, directory, FIFO, socket or device: never opened, so
            # opening a device cannot have side effects.
            raise _Refused("image_path_refused")
        try:
            fd = os.open(
                name,
                os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | _O_CLOEXEC,
                dir_fd=dir_fd,
            )
        except OSError as exc:
            raise _os_refusal(exc) from None
        opened = os.fstat(fd)
        if (
            not stat.S_ISREG(opened.st_mode)
            or (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino)
            # A hard link inside the root can point at a file outside it.
            or opened.st_nlink > 1
        ):
            raise _Refused("image_path_refused")
        if opened.st_size > max_bytes:
            raise _Refused("image_too_large")
        chunks: list[bytes] = []
        remaining = max_bytes + 1
        while remaining > 0:
            chunk = os.read(fd, min(remaining, _READ_CHUNK))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        data = b"".join(chunks)
        if len(data) > max_bytes:
            raise _Refused("image_too_large")
        return data
    finally:
        if fd >= 0:
            os.close(fd)
        os.close(dir_fd)


# ── rendering ──────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Rendered:
    """A render result: PNG bytes with dimensions, or a post-hash reason."""

    png: Optional[bytes] = None
    width: int = 0
    height: int = 0
    error: Optional[str] = None


#: The lowest dpi a PDF page is rendered at. A page too large to fit
#: `MAX_DECODE_PIXELS` even at this dpi is refused before PDFium rasterises it.
MIN_PDF_RENDER_DPI = 1


def _pdf_render_dpi(content: bytes, dpi: int) -> Optional[int]:
    """`dpi`, lowered when page 1 would rasterise past `MAX_DECODE_PIXELS`.

    Returns None when even `MIN_PDF_RENDER_DPI` would pass the cap, or the
    page size is not a finite number: such a page is refused before it is
    rasterised. Only the page size is read here. A failure to read it returns
    `dpi` unchanged and leaves the render itself to name the outcome.
    """
    try:
        import pypdfium2 as pdfium  # type: ignore[import-untyped]
    except ImportError:
        return dpi
    try:
        document = pdfium.PdfDocument(content)
        try:
            if len(document) < 1:
                return dpi
            page = document[0]
            try:
                width_pt, height_pt = page.get_size()
            finally:
                page.close()
        finally:
            document.close()
    except Exception:  # noqa: BLE001 - the render reports pdf_render_failed
        return dpi

    def pixels_at(at_dpi: float) -> float:
        return abs(width_pt * at_dpi / 72.0) * abs(height_pt * at_dpi / 72.0)

    if not math.isfinite(pixels_at(dpi)) or pixels_at(MIN_PDF_RENDER_DPI) > MAX_DECODE_PIXELS:
        return None
    pixels = pixels_at(dpi)
    if pixels <= MAX_DECODE_PIXELS:
        return dpi
    return max(MIN_PDF_RENDER_DPI, int(dpi * math.sqrt(MAX_DECODE_PIXELS / pixels)))


class _MemoryErrorSeen(logging.Handler):
    """Notices a MemoryError that `okg.substrate.imaging` logged and turned
    into an ordinary render failure, so it is reported as a limit instead of
    being cached as ``pdf_render_failed``."""

    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.seen = False

    def emit(self, record: logging.LogRecord) -> None:
        if record.exc_info and _caused_by_memory_error(record.exc_info[1]):
            self.seen = True


def _caused_by_memory_error(exc: Optional[BaseException]) -> bool:
    depth = 0
    while exc is not None and depth < 10:
        if isinstance(exc, MemoryError):
            return True
        exc = exc.__cause__ or exc.__context__
        depth += 1
    return False


_EXIF_ORIENTATION = 0x0112
#: Modes a PNG can hold; anything else is converted before re-encoding.
_PNG_MODES = frozenset({"1", "L", "LA", "I", "P", "RGB", "RGBA"})


def _upright(content: bytes, source_format: str) -> bytes:
    """`content` re-encoded upright when its EXIF orientation says the pixels
    are stored rotated or mirrored; otherwise `content` unchanged.

    The upright pixels are re-encoded as PNG (lossless) and then prepared
    like any other raster, so the output is deterministic.
    """
    from PIL import Image, ImageOps

    with Image.open(io.BytesIO(content), formats=[source_format.upper()]) as image:
        if image.getexif().get(_EXIF_ORIENTATION) in (None, 1):
            return content
        upright = ImageOps.exif_transpose(image)
    try:
        if upright.mode not in _PNG_MODES:
            upright = upright.convert("RGBA" if "A" in upright.getbands() else "RGB")
        out = io.BytesIO()
        upright.save(out, format="PNG", compress_level=1)
        return out.getvalue()
    finally:
        upright.close()


def render_bytes(
    content: bytes, *, source_format: str, dpi: int, max_side: int, label: str,
) -> Rendered:
    """Render `content` in this process: the work the render helper does.

    PDF page 1 renders through PDFium; PNG and JPEG decode through Pillow,
    turned upright by their EXIF orientation. Either is scaled to fit
    `max_side` and encoded as PNG by `okg.substrate.imaging.prepare_image`,
    the read side's own preparation, so a stored image and a fetched one are
    prepared alike. `label` is the relative path, used in log lines only.

    Ingest never calls this directly; `RenderWorker` runs it in a bounded
    helper process. A MemoryError, raised or swallowed into a render
    failure, is reported as ``image_render_limit_exceeded``.
    """
    from PIL import Image

    from okg.substrate.imaging import ImageLimits, ImageRefused, prepare_image

    if source_format in ("png", "jpeg"):
        try:
            with Image.open(io.BytesIO(content), formats=[source_format.upper()]) as probe:
                width, height = probe.size
        except Exception:  # noqa: BLE001 - any decode failure has one name
            return Rendered(error="image_decode_failed")
        if width * height > MAX_DECODE_PIXELS:
            log.warning(
                "image %s declares %dx%d pixels, over the %d-pixel decode cap",
                label, width, height, MAX_DECODE_PIXELS,
            )
            return Rendered(error="image_decode_failed")
        try:
            content = _upright(content, source_format)
        except MemoryError:
            return Rendered(error=LIMIT_REASON)
        except Exception:  # noqa: BLE001 - any decode failure has one name
            return Rendered(error="image_decode_failed")
    elif source_format == "pdf":
        fitted = _pdf_render_dpi(content, dpi)
        if fitted is None:
            log.warning(
                "image %s: PDF page 1 is too large to render under the %d-pixel cap",
                label, MAX_DECODE_PIXELS,
            )
            return Rendered(error=LIMIT_REASON)
        dpi = fitted
    limits = ImageLimits(
        dpi=dpi,
        max_side=max_side,
        # Ingest always stores PNG; the JPEG fallback is a read-side budget.
        jpeg_above_bytes=sys.maxsize,
    )
    watch = _MemoryErrorSeen()
    imaging_log = logging.getLogger("okg.substrate.imaging")
    imaging_log.addHandler(watch)
    try:
        prepared = prepare_image(content, limits=limits, label=label)
    except MemoryError:
        return Rendered(error=LIMIT_REASON)
    except ImageRefused as exc:
        if watch.seen or _caused_by_memory_error(exc):
            return Rendered(error=LIMIT_REASON)
        return Rendered(error=exc.reason)
    finally:
        imaging_log.removeHandler(watch)
    if prepared.mime_type != PNG_MIME_TYPE:
        raise AssertionError(f"image step produced {prepared.mime_type}, not PNG")
    return Rendered(png=prepared.data, width=prepared.width, height=prepared.height)


def render(content: bytes, *, source_format: str, config: ImagesConfig, label: str) -> Rendered:
    """`render_bytes` at `config`'s render settings, in this process."""
    return render_bytes(
        content,
        source_format=source_format,
        dpi=config.render.dpi,
        max_side=config.render.max_side,
        label=label,
    )


# ── completing values ──────────────────────────────────────────────────

class ImageRootInvalid(SubstrateError):
    """The run declares an ``image_root`` that is not an existing directory.

    Raised on the run's first file image value, before that chunk is
    written: otherwise every value would complete as ``image_file_missing``
    and churn a new node version. The message never names the root.
    """

    code = "image_root_invalid"


class ImageCompleter:
    """Completes the file image values of one source run.

    `connect` returns a new autocommit connection; it is called at most once,
    on the first value that needs the blob store, and `close` closes it.
    Renders run in one `RenderWorker` helper, started on the first render and
    closed by `close`; `render_timeout_s` and `render_memory_bytes` default
    to that module's limits.
    """

    def __init__(
        self,
        *,
        image_root: Optional[str],
        config: ImagesConfig,
        connect: Callable[[], Any],
        source_name: str = "",
        render_timeout_s: Optional[float] = None,
        render_memory_bytes: Optional[int] = None,
    ) -> None:
        self.image_root = image_root
        self.config = config
        self.source_name = source_name
        self.metrics = ImageRenderMetrics()
        self._connect = connect
        self._conn: Any = None
        self._root_checked = False
        self.worker = RenderWorker(
            timeout_s=render_timeout_s, memory_bytes=render_memory_bytes,
        )

    def close(self) -> None:
        conn, self._conn = self._conn, None
        try:
            self.worker.close()
        finally:
            if conn is not None:
                conn.close()

    def _db(self) -> Any:
        if self._conn is None or self._conn.closed:
            self._conn = self._connect()
        return self._conn

    def _check_root(self) -> None:
        """Refuse the run when its declared root is not a directory."""
        if self._root_checked or not self.image_root:
            return
        try:
            # Follows a symlink: the root itself may be one (/tmp on macOS).
            is_dir = stat.S_ISDIR(os.stat(self.image_root).st_mode)
        except (OSError, ValueError, UnicodeError):
            is_dir = False
        if not is_dir:
            raise ImageRootInvalid(
                "the run's image_root is not an existing directory; "
                "file image values cannot be resolved",
                data={"reason": "image_root_invalid", "source_name": self.source_name},
            )
        self._root_checked = True

    def complete_facts(self, facts: list) -> list:
        """`facts` with every NodeFact's file image values completed.

        Retractions (``op == 'R'``) and EdgeFacts pass through: a retraction
        writes no value, and edges carry no rendered images. A fact with no
        file value is returned as the same object.
        """
        from okg.substrate.library.sources.base import NodeFact

        targets = [
            isinstance(fact, NodeFact)
            and getattr(fact, "op", "I") != "R"
            and any(
                site.value.get(IMAGE_TAG) == KIND_FILE
                for site in iter_image_values(fact.attrs)
            )
            for fact in facts
        ]
        if any(targets):
            self._check_root()
        out = []
        for fact, target in zip(facts, targets):
            if target:
                fact = dataclasses.replace(fact, attrs=self.complete_attrs(fact.attrs))
            out.append(fact)
        return out

    def complete_attrs(self, attrs: Mapping[str, Any]) -> dict[str, Any]:
        """A copy of `attrs` with each file value completed in place."""
        completed = dict(attrs)
        for site in iter_image_values(attrs):
            if site.value.get(IMAGE_TAG) != KIND_FILE:
                continue  # URL values pass through unchanged
            value = self.complete_value(site.value)
            if site.index is None:
                completed[site.attr] = value
            else:
                items = completed[site.attr]
                rebuilt = list(items)
                rebuilt[site.index] = value
                completed[site.attr] = type(items)(rebuilt) if isinstance(items, tuple) else rebuilt
        return completed

    def complete_value(self, value: Mapping[str, Any]) -> dict[str, Any]:
        """One file value, completed with ``png`` or ``error``."""
        if not self.image_root:
            return self._error(value, "image_root_undeclared")
        self._check_root()
        path = value["path"]
        try:
            content = read_under_root(
                self.image_root, path, max_bytes=self.config.file_max_bytes,
            )
        except _Refused as refused:
            return self._error(value, refused.reason)

        from okg.substrate.db import blob_store
        from okg.substrate.imaging import sniff_image_format

        source_sha256 = hashlib.sha256(content).hexdigest()
        source_format = sniff_image_format(content)
        renderer = renderer_for(source_format)
        settings = {
            "dpi": self.config.render.dpi,
            "max_side": self.config.render.max_side,
            "renderer": renderer,
        }
        blob_id = render_id(source_sha256, **settings)

        conn = self._db()
        # The touch comes before the value is returned, so a cache hit's row
        # has a fresh last_used_at for as long as cleanup's grace window.
        row = blob_store.touch(conn, kind=_BLOB_KIND, id=blob_id)
        if row is not None:
            self.metrics.reused += 1
            return self._from_row(value, row)

        # The source file's hash is not stored: the image view is readable by
        # okg_mcp, and the hash would let a caller confirm a guessed file.
        meta: dict[str, Any] = {"source_format": source_format, **settings}
        if source_format in ("postscript", "svg", "unknown"):
            result = Rendered(error="image_format_unsupported")
        elif renderer == PDFIUM_ABSENT:
            # Decided here, where the id was keyed: the row stored under a
            # "no PDFium" id must say so, whatever the helper could import.
            result = Rendered(error="pdf_renderer_unavailable")
        else:
            outcome = self.worker.render(
                content,
                source_format=source_format,
                dpi=self.config.render.dpi,
                max_side=self.config.render.max_side,
                renderer=renderer,
                label=path,
            )
            result = Rendered(
                png=outcome.png, width=outcome.width, height=outcome.height,
                error=outcome.error,
            )
        if result.error == LIMIT_REASON:
            # Not cached: whether a render fits depends on the limits and the
            # machine, so a later run tries again. No row is written.
            log.warning(
                "source %s: image %s passed a render limit; it is retried next run",
                self.source_name, path,
            )
            return self._error(value, result.error)

        self.metrics.rendered += 1
        if result.error is not None:
            log.warning(
                "source %s: image %s could not be rendered: %s",
                self.source_name, path, result.error,
            )
            row = blob_store.put_error(
                conn, kind=_BLOB_KIND, id=blob_id, error=result.error, meta=meta,
            )
        else:
            assert result.png is not None
            row = blob_store.put(
                conn,
                kind=_BLOB_KIND,
                id=blob_id,
                data=result.png,
                mime_type=PNG_MIME_TYPE,
                meta={**meta, "width": result.width, "height": result.height},
            )
            # put keeps a row another writer stored first; count only the
            # bytes this statement inserted.
            if row.inserted and row.error is None:
                self.metrics.bytes_stored += int(row.size_bytes or 0)
        return self._from_row(value, row)

    def _error(self, value: Mapping[str, Any], reason: str) -> dict[str, Any]:
        self.metrics.count_error(reason)
        return with_error(value, reason)

    def _from_row(self, value: Mapping[str, Any], row: Any) -> dict[str, Any]:
        if row.error is not None:
            return self._error(value, row.error)
        meta = row.meta
        return with_png(
            value,
            id=row.id,
            sha256=row.sha256,
            mime_type=row.mime_type,
            width=meta.get("width"),
            height=meta.get("height"),
            byte_count=row.size_bytes,
        )


def images_config_for_manifest(raw: Any, *, manifest_path: Any = None) -> ImagesConfig:
    """The ingest image settings a deployment manifest declares.

    Parsed through `parse_agent_surface`, the parser deployment load uses, so
    ingest and load agree on the block. A manifest with no ``agent_surface``
    block, or a block without ``images``, gets `ImagesConfig.default()`.
    """
    from okg.substrate.mcp.effective_surface import parse_agent_surface

    surface = parse_agent_surface(raw, manifest_path=manifest_path)
    return surface.images_config if surface is not None else ImagesConfig.default()


def images_config_for_deployment(deployment_dir: Any, raw_manifest: Any) -> ImagesConfig:
    """The deployment's `agent_surface.images` settings for the ingest image step.

    The raw `deployment.yaml` is the manifest, except in a sealed environment,
    where the composed manifest is the authority (as for readiness groups in
    `source_sync_convergence`). Parsed by the same `parse_agent_surface` that
    deployment load runs, so a block load accepted parses the same here.
    """
    from pathlib import Path

    manifest_path = Path(deployment_dir) / "deployment.yaml"
    manifest: Any = raw_manifest
    if os.environ.get("OKG_ENVIRONMENT_DESCRIPTOR"):
        from okg.substrate.deployments import active_effective_manifest_for_path

        composed = active_effective_manifest_for_path(manifest_path)
        if composed is not None:
            manifest = composed
    return images_config_for_manifest(manifest, manifest_path=manifest_path)


__all__ = [
    "ImageCompleter",
    "ImageRenderMetrics",
    "ImageRootInvalid",
    "MAX_DECODE_PIXELS",
    "PDFIUM_ABSENT",
    "PILLOW_RENDERER",
    "RENDER_ID_VERSION",
    "Rendered",
    "images_config_for_deployment",
    "images_config_for_manifest",
    "read_under_root",
    "render",
    "render_bytes",
    "render_id",
    "renderer_for",
]
