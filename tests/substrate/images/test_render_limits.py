"""Render limits and file-opening hardening for ingest images (PACT first-class-images, #3046).

Adversarial-review fixes on the ingest render path:

* rendering runs in a bounded helper process: a nested-XObject PDF bomb is
  stopped by the memory limit and by the wall-clock limit, completes as
  ``image_render_limit_exceeded``, is not cached, and a later run retries it;
  the other images of the run are still stored;
* a PDF page too large to rasterise even at the lowest dpi is refused before
  PDFium renders it;
* an over-long path, a lone-surrogate path, an unmapped OSError and a hard link
  are per-image ``image_path_refused``, never a failed run;
* a declared ``image_root`` that is not a directory fails the run with
  ``image_root_invalid``, naming no absolute path, before anything is written;
* blob ``meta`` holds no source hash; ``image_bytes_stored`` counts only rows
  the run inserted; an EXIF-rotated JPEG is stored upright.
"""
from __future__ import annotations

import errno
import io
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

import psycopg
import pytest
from PIL import Image

from okg.deployment import ImageRef
from okg.substrate.db import blob_store
from okg.substrate.images import render as render_module
from okg.substrate.images import render_worker
from okg.substrate.images.render import (
    ImageRootInvalid,
    MAX_DECODE_PIXELS,
    _Refused,
    read_under_root,
    render_bytes,
)
from okg.substrate.images.render_worker import LIMIT_REASON, RenderWorker
from tests.substrate.images.test_ingest_render import (  # noqa: F401 - fixture
    _blob_rows,
    _graph_nodes,
    _node,
    _png,
    _run,
    runtime,
)

MAX_FILE = 100_000


# ── fixture files ──────────────────────────────────────────────────────

def bomb_pdf(levels: int, fanout: int = 10, *, media_box: str = "0 0 200 200") -> bytes:
    """A small PDF whose page draws a chain of nested form XObjects, each
    drawing the next `fanout` times: `fanout ** (levels - 1)` leaf draws."""
    objs: dict[int, bytes] = {}
    first = 5
    for i in range(levels):
        num = first + i
        if i == levels - 1:
            body = b"0 0 1 1 re f"
            resources = b""
        else:
            body = b" ".join(b"q /X Do Q" for _ in range(fanout))
            resources = b"/Resources << /XObject << /X %d 0 R >> >> " % (num + 1)
        objs[num] = (
            b"<< /Type /XObject /Subtype /Form /BBox [0 0 100 100] " + resources
            + b"/Length %d >>\nstream\n" % len(body) + body + b"\nendstream"
        )
    content = b"q /X Do Q"
    objs[1] = b"<< /Type /Catalog /Pages 2 0 R >>"
    objs[2] = b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>"
    objs[3] = (
        b"<< /Type /Page /Parent 2 0 R /MediaBox [" + media_box.encode() + b"] "
        b"/Contents 4 0 R /Resources << /XObject << /X %d 0 R >> >> >>" % first
    )
    objs[4] = b"<< /Length %d >>\nstream\n" % len(content) + content + b"\nendstream"
    out = bytearray(b"%PDF-1.4\n")
    offsets: dict[int, int] = {}
    for num in sorted(objs):
        offsets[num] = len(out)
        out += b"%d 0 obj\n" % num + objs[num] + b"\nendobj\n"
    xref = len(out)
    out += b"xref\n0 %d\n0000000000 65535 f \n" % (max(objs) + 1)
    for num in range(1, max(objs) + 1):
        out += b"%010d 00000 n \n" % offsets[num]
    out += b"trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n" % (max(objs) + 1, xref)
    return bytes(out)


def _rotated_jpeg(path: Path) -> None:
    """40 x 20 pixels, red over blue, tagged EXIF orientation 6 (rotate 90
    clockwise to view): upright it is 20 x 40, red on the right."""
    image = Image.new("RGB", (40, 20), (255, 0, 0))
    image.paste((0, 0, 255), (0, 10, 40, 20))
    exif = image.getexif()
    exif[0x0112] = 6
    path.parent.mkdir(parents=True, exist_ok=True)
    image.save(path, format="JPEG", quality=95, exif=exif.tobytes())


def _pdfium() -> Any:
    return pytest.importorskip("pypdfium2")


def _small_png_bytes() -> bytes:
    buffer = io.BytesIO()
    Image.new("RGB", (30, 20), (5, 6, 7)).save(buffer, format="PNG")
    return buffer.getvalue()


def _worker_render(worker: RenderWorker, content: bytes, fmt: str) -> render_worker.WorkerResult:
    return worker.render(
        content, source_format=fmt, dpi=100, max_side=1600,
        renderer=render_module.renderer_for(fmt), label="case",
    )


# ── the helper process, without a database ─────────────────────────────

def test_the_helper_matches_the_in_process_render_and_is_reused() -> None:
    content = _small_png_bytes()
    with RenderWorker() as worker:
        first = _worker_render(worker, content, "png")
        second = _worker_render(worker, content, "png")
    local = render_bytes(content, source_format="png", dpi=100, max_side=1600, label="case")
    assert first.error is None and first.png == second.png == local.png
    assert (first.width, first.height) == (30, 20)
    assert worker.starts == 1


def test_the_helper_stops_a_bomb_at_the_wall_clock_limit(caplog: pytest.LogCaptureFixture) -> None:
    _pdfium()
    with RenderWorker(timeout_s=0.5) as worker:
        with caplog.at_level(logging.WARNING, logger=render_worker.__name__):
            result = _worker_render(worker, bomb_pdf(7), "pdf")
        assert result.error == LIMIT_REASON
        assert "limit; the helper was killed" in caplog.text
        # The next job gets a fresh helper and renders normally.
        after = _worker_render(worker, _small_png_bytes(), "png")
    assert after.error is None and worker.starts == 2


def test_a_parent_larger_than_the_limit_does_not_fail_the_helper() -> None:
    # Linux carries ru_maxrss across exec, so a helper forked from a parent
    # already over the limit used to exit before its first job (CI, PR 3157).
    ballast = bytearray(400 * 1024 * 1024)
    for offset in range(0, len(ballast), 4096):
        ballast[offset] = 1
    with RenderWorker(memory_bytes=300 * 1024 * 1024) as worker:
        result = _worker_render(worker, _small_png_bytes(), "png")
    del ballast
    assert result.error is None and worker.starts == 1


def test_the_helper_stops_a_bomb_at_the_memory_limit(caplog: pytest.LogCaptureFixture) -> None:
    _pdfium()
    # Level 6 peaks near 600 MB; the limit is 300 MB and the clock generous,
    # so only the memory limit can stop it.
    with RenderWorker(timeout_s=60, memory_bytes=300 * 1024 * 1024) as worker:
        with caplog.at_level(logging.WARNING, logger=render_worker.__name__):
            result = _worker_render(worker, bomb_pdf(6), "pdf")
    assert result.error == LIMIT_REASON
    if sys.platform == "darwin":
        # macOS enforces no rlimit on memory; the helper's RSS watchdog does.
        assert "the render helper died (memory limit)" in caplog.text
    else:
        # Linux: RLIMIT_DATA fails the allocation inside PDFium (a crash, or a
        # MemoryError reply), or the RSS watchdog fires first.
        assert "limit; the helper was killed" not in caplog.text


def test_an_oversize_pdf_page_is_refused_before_it_is_rasterised(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _pdfium()
    import okg.substrate.imaging as imaging

    def must_not_render(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("PDFium was asked to rasterise an oversize page")

    monkeypatch.setattr(imaging, "render_pdf_first_page", must_not_render)
    page = bomb_pdf(1, media_box="0 0 1000000 1000000")
    # Even at 1 dpi the page is (1e6 / 72) ** 2, about 193 Mpx.
    assert (1_000_000 / 72) ** 2 > MAX_DECODE_PIXELS
    result = render_bytes(page, source_format="pdf", dpi=100, max_side=1600, label="big.pdf")
    assert result.error == LIMIT_REASON and result.png is None


def test_the_helper_environment_carries_no_secrets(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OKG_DSN", "postgres://user:secret@db/okg")
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_secret")
    env = render_worker._child_env()
    assert "OKG_DSN" not in env and "GITHUB_TOKEN" not in env
    assert "secret" not in json.dumps(env)


def test_an_exif_rotated_jpeg_is_rendered_upright(tmp_path: Path) -> None:
    _rotated_jpeg(tmp_path / "rot.jpg")
    content = (tmp_path / "rot.jpg").read_bytes()
    first = render_bytes(content, source_format="jpeg", dpi=100, max_side=1600, label="rot.jpg")
    again = render_bytes(content, source_format="jpeg", dpi=100, max_side=1600, label="rot.jpg")
    assert first.png == again.png  # deterministic
    image = Image.open(io.BytesIO(first.png or b"")).convert("RGB")
    assert image.size == (20, 40)
    red, _, blue = image.getpixel((15, 20))
    assert red > 200 and blue < 60
    red, _, blue = image.getpixel((4, 20))
    assert blue > 200 and red < 60


# ── opening files, without a database ──────────────────────────────────

def test_over_long_and_unencodable_paths_are_refused(tmp_path: Path) -> None:
    for path in ["a" * 300 + ".png", "d/" * 2100 + "a.png", "fig\udcff.png", "\ud800/a.png"]:
        with pytest.raises(_Refused) as refused:
            read_under_root(str(tmp_path), path, max_bytes=MAX_FILE)
        assert refused.value.reason == "image_path_refused", path[:20]


@pytest.mark.parametrize("code", [errno.EIO, errno.ENXIO, errno.EOVERFLOW, errno.EINVAL, errno.ENAMETOOLONG])
def test_an_unmapped_os_error_is_a_refusal_naming_no_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, code: int,
) -> None:
    _png(tmp_path / "a.png", (4, 4), (0, 0, 0))

    def failing_read(fd: int, n: int) -> bytes:
        raise OSError(code, os.strerror(code), str(tmp_path / "a.png"))

    monkeypatch.setattr(render_module.os, "read", failing_read)
    with pytest.raises(_Refused) as refused:
        read_under_root(str(tmp_path), "a.png", max_bytes=MAX_FILE)
    assert refused.value.reason == "image_path_refused"
    assert str(tmp_path) not in str(refused.value)
    assert refused.value.__cause__ is None and refused.value.__suppress_context__


def test_a_hard_link_is_refused(tmp_path: Path) -> None:
    outside = tmp_path / "outside.png"
    _png(outside, (4, 4), (0, 0, 0))
    root = tmp_path / "root"
    root.mkdir()
    os.link(outside, root / "linked.png")
    with pytest.raises(_Refused) as refused:
        read_under_root(str(root), "linked.png", max_bytes=MAX_FILE)
    assert refused.value.reason == "image_path_refused"


# ── through the source runner ──────────────────────────────────────────

@pytest.mark.postgres
def test_a_pdf_bomb_is_a_limit_not_cached_and_retried(
    runtime: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,  # noqa: F811
) -> None:
    _pdfium()
    root = tmp_path / "corpus"
    _png(root / "plot.png", (32, 20), (0, 128, 0))
    (root / "bomb.pdf").write_bytes(bomb_pdf(6))
    facts = [
        _node("n:ok", {"figure": ImageRef.file("plot.png")}),
        _node("n:bomb", {"figure": ImageRef.file("bomb.pdf")}),
    ]
    with monkeypatch.context() as patch:
        patch.setattr(render_worker, "RENDER_MEMORY_BYTES", 300 * 1024 * 1024)
        first = _run(runtime, facts, image_root=str(root))
    nodes = _graph_nodes(runtime)
    assert nodes["n:bomb"]["attrs"]["figure"] == {
        "okg_image": "file", "path": "bomb.pdf", "error": LIMIT_REASON,
    }
    assert "png" in nodes["n:ok"]["attrs"]["figure"]
    rows = _blob_rows(runtime)
    assert len(rows) == 1 and rows[0][5] is None  # the PNG; no failure row
    assert first.image_metrics == {
        "images_rendered": 1,
        "images_reused": 0,
        "images_error": {LIMIT_REASON: 1},
        "image_bytes_stored": rows[0][4],
    }

    # A later run with the default limits retries and renders it.
    second = _run(runtime, facts, image_root=str(root))
    assert second.image_metrics is not None
    assert second.image_metrics["images_rendered"] == 1
    assert second.image_metrics["images_reused"] == 1
    assert second.image_metrics["images_error"] == {}
    assert "png" in _graph_nodes(runtime)["n:bomb"]["attrs"]["figure"]


@pytest.mark.postgres
def test_refused_paths_do_not_fail_the_run(runtime: str, tmp_path: Path) -> None:  # noqa: F811
    root = tmp_path / "corpus"
    _png(root / "plot.png", (32, 20), (0, 128, 0))
    _png(tmp_path / "outside.png", (8, 8), (9, 9, 9))
    os.link(tmp_path / "outside.png", root / "hard.png")
    facts = [
        _node("n:ok", {"figure": ImageRef.file("plot.png")}),
        _node("n:long", {"figure": ImageRef.file("a" * 300 + ".png")}),
        _node("n:hard", {"figure": ImageRef.file("hard.png")}),
    ]
    result = _run(runtime, facts, image_root=str(root))
    nodes = _graph_nodes(runtime)
    assert "png" in nodes["n:ok"]["attrs"]["figure"]
    # A lone-surrogate path is refused by `read_under_root` (tested above),
    # but a value holding one cannot be stored as JSON at all; refusing it at
    # the value check is `values.path_problem`'s job.
    for node_id in ("n:long", "n:hard"):
        assert nodes[node_id]["attrs"]["figure"]["error"] == "image_path_refused", node_id
    assert result.image_metrics is not None
    assert result.image_metrics["images_error"] == {"image_path_refused": 2}
    assert len(_blob_rows(runtime)) == 1  # plot.png only


@pytest.mark.postgres
@pytest.mark.parametrize("kind", ["missing", "file"])
def test_a_root_that_is_not_a_directory_fails_the_run(
    runtime: str, tmp_path: Path, kind: str,  # noqa: F811
) -> None:
    root = tmp_path / "corpus"
    if kind == "file":
        root.write_bytes(b"not a directory")
    facts = [_node("n:a", {"figure": ImageRef.file("plot.png")})]
    with pytest.raises(ImageRootInvalid) as raised:
        _run(runtime, facts, image_root=str(root))
    assert raised.value.code == "image_root_invalid"
    text = str(raised.value) + json.dumps(raised.value.data, default=str)
    for spelling in {str(tmp_path), os.path.realpath(tmp_path)}:
        assert spelling not in text
    assert _graph_nodes(runtime) == {}
    assert _blob_rows(runtime) == []


@pytest.mark.postgres
def test_meta_has_no_source_hash_and_a_rotated_jpeg_is_stored_upright(
    runtime: str, tmp_path: Path,  # noqa: F811
) -> None:
    root = tmp_path / "corpus"
    _rotated_jpeg(root / "rot.jpg")
    (root / "drawing.eps").write_bytes(b"%!PS-Adobe-3.0 EPSF-3.0\nshowpage\n")
    facts = [
        _node("n:rot", {"figure": ImageRef.file("rot.jpg")}),
        _node("n:eps", {"figure": ImageRef.file("drawing.eps")}),
    ]
    _run(runtime, facts, image_root=str(root))
    rows = _blob_rows(runtime)
    assert len(rows) == 2
    for row in rows:
        assert "source_sha256" not in row[6], row[6]
    value = _graph_nodes(runtime)["n:rot"]["attrs"]["figure"]
    assert "source_sha256" not in json.dumps(value)
    assert (value["png"]["width"], value["png"]["height"]) == (20, 40)


@pytest.mark.postgres
def test_bytes_stored_counts_only_rows_this_run_inserted(
    runtime: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,  # noqa: F811
) -> None:
    with psycopg.connect(runtime, autocommit=True) as conn:
        first = blob_store.put(conn, kind="image", id="r" * 64, data=b"abc", mime_type="image/png", meta={})
        again = blob_store.put(conn, kind="image", id="r" * 64, data=b"abc", mime_type="image/png", meta={})
        failed = blob_store.put_error(conn, kind="image", id="e" * 64, error="pdf_render_failed", meta={})
        failed_again = blob_store.put_error(conn, kind="image", id="e" * 64, error="pdf_render_failed", meta={})
    assert (first.inserted, again.inserted) == (True, False)
    assert (failed.inserted, failed_again.inserted) == (True, False)

    # Another writer stored the row between this run's cache miss and its put.
    root = tmp_path / "corpus"
    _png(root / "plot.png", (32, 20), (0, 128, 0))
    facts = [_node("n:a", {"figure": ImageRef.file("plot.png")})]
    _run(runtime, facts, image_root=str(root))
    monkeypatch.setattr(blob_store, "touch", lambda conn, *, kind, id: None)
    raced = _run(runtime, facts, image_root=str(root))
    assert raced.image_metrics is not None
    assert raced.image_metrics["images_rendered"] == 1
    assert raced.image_metrics["image_bytes_stored"] == 0
