"""Ingest renders file image values (PACT first-class-images, #3046).

task.first-class-images.render, plus the runner's share of no-forgery. A real
`SourceRunner` drives a `Connector` through `ConnectorAdapter` against a
disposable Postgres. Each case checks the completed value the graph holds,
the stored ``okg.blobs`` row behind it, and the run's image metrics:

* Pillow-written PNG and JPEG files and a real PDF render to stored PNGs no
  larger than ``max_side``;
* a symlinked file, a symlinked directory component, a missing file, an
  oversize file and an EPS each complete with their named error;
* a second run of unchanged files renders nothing and writes no node
  version; a changed file renders again under a new id;
* PDFium absent gives ``pdf_renderer_unavailable``; installed, the same file
  renders on the next run;
* a connector value carrying ``png`` or ``error``, or a raw ``okg_image``
  mapping with a bad path, refuses the run and writes nothing;
* a run with no ``image_root`` completes every file value with
  ``image_root_undeclared``;
* the absolute root appears in no fact, graph row, blob row or report.

The file-opening rules also have direct, database-free tests at the end.
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import struct
import sys
import zlib
from pathlib import Path
from typing import Any

import psycopg
import pytest
from PIL import Image

import okg.deployment as sdk
from okg.deployment import ImageRef
from okg.substrate.db import blob_store
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.images.config import ImagesConfig
from okg.substrate.images.render import (
    MAX_DECODE_PIXELS,
    _Refused,
    read_under_root,
    render,
    render_id,
)
from okg.substrate.images.values import ImageRefInvalid, ImageValueForged
from okg.substrate.ingest import SourceRunner
from okg.substrate.ingest.runner import image_report_fields
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from tests.pytest.catalog_fixture_builders import seed_catalog

SOURCE = "test.images"
MAX_FILE = 100_000
CONFIG = ImagesConfig.parse({"file_max_bytes": MAX_FILE, "render": {"max_side": 1600}})


# ── fixture files ──────────────────────────────────────────────────────

def _png(path: Path, size: tuple[int, int], color: tuple[int, int, int]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    Image.new("RGB", size, color).save(path, format="PNG")


def _jpeg(path: Path, size: tuple[int, int]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    Image.new("RGB", size, (10, 120, 200)).save(path, format="JPEG", quality=90)


def _pdf(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # A 4 x 3 inch page at 72 dpi; at the default render dpi of 100 it is
    # 400 x 300 pixels.
    Image.new("RGB", (288, 216), (200, 30, 30)).save(path, format="PDF", resolution=72.0)


def _populate(root: Path) -> None:
    _png(root / "figs" / "plot.png", (320, 200), (0, 128, 0))
    _png(root / "figs" / "wide.png", (2400, 600), (90, 90, 90))
    _jpeg(root / "photos" / "shot.jpg", (400, 300))
    _pdf(root / "figs" / "chart.pdf")
    (root / "figs" / "drawing.eps").write_bytes(
        b"%!PS-Adobe-3.0 EPSF-3.0\n%%BoundingBox: 0 0 10 10\nshowpage\n"
    )
    (root / "figs" / "huge.png").write_bytes(
        b"\x89PNG\r\n\x1a\n" + os.urandom(MAX_FILE + 1)
    )
    (root / "figs" / "link.png").symlink_to(root / "figs" / "plot.png")
    (root / "linked").symlink_to(root / "figs", target_is_directory=True)


#: node id -> the path its `figure` attr names, and the outcome expected.
CASES: dict[str, tuple[str, str]] = {
    "n:png": ("figs/plot.png", "png"),
    "n:wide": ("figs/wide.png", "png"),
    "n:jpeg": ("photos/shot.jpg", "png"),
    "n:pdf": ("figs/chart.pdf", "png"),
    "n:eps": ("figs/drawing.eps", "image_format_unsupported"),
    "n:huge": ("figs/huge.png", "image_too_large"),
    "n:missing": ("figs/absent.png", "image_file_missing"),
    "n:symlink-file": ("figs/link.png", "image_path_refused"),
    "n:symlink-dir": ("linked/plot.png", "image_path_refused"),
}
URL = "https://example.org/figures/f1.png"


# ── the connector ──────────────────────────────────────────────────────

class _ImageConnector:
    name = "image-connector"
    profile = "discovery_crawl"

    def __init__(self, facts: list[Any], *, image_root: str | None) -> None:
        self._facts = facts
        self._image_root = image_root

    def run(self, ctx: Any) -> Any:
        del ctx
        return sdk.ConnectorRun(
            facts=list(self._facts),
            completed_scope=True,
            run_mode="scope_complete",
            image_root=self._image_root,
        )


def _node(node_id: str, attrs: dict[str, Any]) -> NodeFact:
    return NodeFact(
        node_id=node_id,
        subtype="concept",
        attrs=attrs,
        source_record_id={"id": node_id},
        source_revision={"v": 1},
    )


def _case_facts() -> list[Any]:
    facts = [_node(node_id, {"figure": ImageRef.file(path)}) for node_id, (path, _) in CASES.items()]
    facts.append(_node("n:list", {
        "panels": [ImageRef.file("figs/plot.png"), ImageRef.url(URL), ImageRef.file("figs/absent.png")],
        "title": "three panels",
    }))
    facts.append(_node("n:url", {"figure": ImageRef.url(URL)}))
    return facts


def _run(dsn: str, facts: list[Any], *, image_root: str | None, config: ImagesConfig = CONFIG):
    adapter = sdk.ConnectorAdapter(_ImageConnector(facts, image_root=image_root))
    return SourceRunner(dsn, source_name=SOURCE, images_config=config).run(
        adapter, mode="scope_complete",
    )


# ── database helpers ───────────────────────────────────────────────────

@pytest.fixture
def runtime(clean_runtime: str) -> str:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["concept"], narrowings=[("concept", "links_to", "concept")])
    return clean_runtime


def _graph_nodes(dsn: str) -> dict[str, dict[str, Any]]:
    graph = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        rows = graph.checkout().query(
            "SELECT node_id, attrs, source_record_id, source_revision, run_id "
            "FROM okg.graph_nodes WHERE source = :source",
            {"source": SOURCE},
        )
    finally:
        graph.close()
    return {row["node_id"]: dict(row) for row in rows}


def _blob_rows(dsn: str) -> list[tuple[Any, ...]]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        return conn.execute(
            "SELECT id, kind, sha256, mime_type, size_bytes, error, meta FROM okg.blobs ORDER BY id"
        ).fetchall()


def _blob(dsn: str, blob_id: str) -> blob_store.BlobRow:
    with psycopg.connect(dsn, autocommit=True) as conn:
        row = blob_store.get(conn, kind="image", id=blob_id)
    assert row is not None, blob_id
    return row


def _assert_stored_png(dsn: str, png: dict[str, Any], *, max_side: int = 1600) -> Image.Image:
    row = _blob(dsn, png["id"])
    assert row.error is None and row.data is not None
    assert hashlib.sha256(row.data).hexdigest() == png["sha256"] == row.sha256
    assert len(row.data) == png["byte_count"] == row.size_bytes
    image = Image.open(io.BytesIO(row.data))
    assert image.format == "PNG"
    assert image.size == (png["width"], png["height"])
    assert (row.meta["width"], row.meta["height"]) == image.size
    assert max(image.size) <= max_side
    return image


# ── the render cases ───────────────────────────────────────────────────

@pytest.mark.postgres
def test_ingest_completes_every_file_value_and_republish_writes_nothing(
    runtime: str, tmp_path: Path,
) -> None:
    root = tmp_path / "corpus"
    _populate(root)
    facts = _case_facts()

    first = _run(runtime, facts, image_root=str(root))

    nodes = _graph_nodes(runtime)
    for node_id, (path, outcome) in CASES.items():
        value = nodes[node_id]["attrs"]["figure"]
        assert value["okg_image"] == "file" and value["path"] == path, node_id
        if outcome == "png":
            assert set(value) == {"okg_image", "path", "png"}, (node_id, value)
        else:
            assert value == {"okg_image": "file", "path": path, "error": outcome}, node_id

    _assert_stored_png(runtime, nodes["n:png"]["attrs"]["figure"]["png"])
    assert _assert_stored_png(runtime, nodes["n:png"]["attrs"]["figure"]["png"]).size == (320, 200)
    # Downscaled to max_side, aspect kept.
    assert _assert_stored_png(runtime, nodes["n:wide"]["attrs"]["figure"]["png"]).size == (1600, 400)
    assert _assert_stored_png(runtime, nodes["n:jpeg"]["attrs"]["figure"]["png"]).size == (400, 300)
    # PDF page 1 at the default 100 dpi: 4 x 3 inches -> 400 x 300.
    pdf = _assert_stored_png(runtime, nodes["n:pdf"]["attrs"]["figure"]["png"])
    assert pdf.size == (400, 300)
    # Pillow stores the page image JPEG-compressed, so the colour is close.
    red, green, blue = pdf.convert("RGB").getpixel((200, 150))
    assert abs(red - 200) <= 6 and green <= 40 and blue <= 40

    # A list attr: each file element completed in place, the URL passed through.
    panels = nodes["n:list"]["attrs"]["panels"]
    assert panels[0] == nodes["n:png"]["attrs"]["figure"] | {"path": "figs/plot.png"}
    assert panels[1] == {"okg_image": "url", "url": URL}
    assert panels[2] == {"okg_image": "file", "path": "figs/absent.png", "error": "image_file_missing"}
    assert nodes["n:list"]["attrs"]["title"] == "three panels"
    assert nodes["n:url"]["attrs"]["figure"] == {"okg_image": "url", "url": URL}

    # The EPS failure is stored under its render id; pre-hash errors are not.
    rows = _blob_rows(runtime)
    errors = sorted(row[5] for row in rows if row[5] is not None)
    assert errors == ["image_format_unsupported"]
    assert len(rows) == 5  # png, wide, jpeg, pdf, eps

    assert first.image_metrics == {
        "images_rendered": 5,
        # The list's plot.png is the same file as n:png: one render, one hit.
        "images_reused": 1,
        "images_error": {
            "image_file_missing": 2,
            "image_format_unsupported": 1,
            "image_path_refused": 2,
            "image_too_large": 1,
        },
        "image_bytes_stored": sum(row[4] for row in rows if row[4] is not None),
    }
    assert first.as_dict()["images"] == first.image_metrics
    assert image_report_fields(first) == {"images": first.image_metrics}

    # The absolute root is nowhere: not in a graph row, a blob row or a report.
    stored = json.dumps([nodes, [list(map(str, row)) for row in rows], first.as_dict()], default=str)
    for spelling in {str(tmp_path), os.path.realpath(tmp_path)}:
        assert spelling not in stored

    # Second run, unchanged files: nothing renders, no node version is written.
    second = _run(runtime, facts, image_root=str(root))
    assert second.nodes_appended == 0, second.as_dict()
    assert second.image_metrics is not None
    assert second.image_metrics["images_rendered"] == 0
    assert second.image_metrics["images_reused"] == 6
    assert second.image_metrics["image_bytes_stored"] == 0
    again = _graph_nodes(runtime)
    assert {k: v["run_id"] for k, v in again.items()} == {k: v["run_id"] for k, v in nodes.items()}
    assert {k: v["attrs"] for k, v in again.items()} == {k: v["attrs"] for k, v in nodes.items()}
    assert len(_blob_rows(runtime)) == 5

    # A changed file renders again under a new id; only its nodes change.
    _png(root / "figs" / "plot.png", (320, 200), (255, 0, 0))
    third = _run(runtime, facts, image_root=str(root))
    assert third.image_metrics is not None
    assert third.image_metrics["images_rendered"] == 1
    assert third.image_metrics["image_bytes_stored"] > 0
    assert third.nodes_appended == 2  # n:png and n:list
    changed = _graph_nodes(runtime)
    old_png = nodes["n:png"]["attrs"]["figure"]["png"]
    new_png = changed["n:png"]["attrs"]["figure"]["png"]
    assert new_png["id"] != old_png["id"]
    assert _assert_stored_png(runtime, new_png).convert("RGB").getpixel((0, 0)) == (255, 0, 0)
    assert changed["n:png"]["run_id"] == third.run_id
    assert changed["n:jpeg"]["run_id"] == nodes["n:jpeg"]["run_id"]


@pytest.mark.postgres
def test_pdfium_absent_is_named_and_a_later_run_with_pdfium_renders(
    runtime: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "corpus"
    _pdf(root / "chart.pdf")
    facts = [_node("n:pdf", {"figure": ImageRef.file("chart.pdf")})]

    with monkeypatch.context() as patch:
        # `import pypdfium2` raises ImportError while the entry is None.
        patch.setitem(sys.modules, "pypdfium2", None)
        absent = _run(runtime, facts, image_root=str(root))
    assert _graph_nodes(runtime)["n:pdf"]["attrs"]["figure"] == {
        "okg_image": "file", "path": "chart.pdf", "error": "pdf_renderer_unavailable",
    }
    assert absent.image_metrics is not None
    assert absent.image_metrics["images_error"] == {"pdf_renderer_unavailable": 1}
    assert [row[5] for row in _blob_rows(runtime)] == ["pdf_renderer_unavailable"]

    present = _run(runtime, facts, image_root=str(root))
    assert present.image_metrics is not None
    assert present.image_metrics["images_rendered"] == 1
    assert present.nodes_appended == 1
    value = _graph_nodes(runtime)["n:pdf"]["attrs"]["figure"]
    assert _assert_stored_png(runtime, value["png"]).size == (400, 300)
    # Both rows stay: the failure is keyed by the absent renderer.
    assert len(_blob_rows(runtime)) == 2


@pytest.mark.postgres
def test_a_run_without_image_root_names_it_and_stores_nothing(
    runtime: str, tmp_path: Path,
) -> None:
    root = tmp_path / "corpus"
    _png(root / "plot.png", (10, 10), (1, 2, 3))
    result = _run(runtime, [_node("n:a", {"figure": ImageRef.file("plot.png")})], image_root=None)
    assert _graph_nodes(runtime)["n:a"]["attrs"]["figure"] == {
        "okg_image": "file", "path": "plot.png", "error": "image_root_undeclared",
    }
    assert result.image_metrics == {
        "images_rendered": 0,
        "images_reused": 0,
        "images_error": {"image_root_undeclared": 1},
        "image_bytes_stored": 0,
    }
    assert _blob_rows(runtime) == []


@pytest.mark.postgres
def test_a_run_without_image_values_reports_no_image_metrics(runtime: str) -> None:
    result = _run(runtime, [_node("n:a", {"text": "plain"})], image_root=None)
    assert result.image_metrics is None
    assert "images" not in result.as_dict()
    assert image_report_fields(result) == {}


# ── refusals: nothing is written ───────────────────────────────────────

def _forged_png() -> dict[str, Any]:
    return {
        "okg_image": "file",
        "path": "figs/plot.png",
        "png": {
            "id": "a" * 64, "sha256": "b" * 64, "mime_type": "image/png",
            "width": 1, "height": 1, "byte_count": 1,
        },
    }


@pytest.mark.postgres
@pytest.mark.parametrize(
    ("attrs", "error", "code"),
    [
        ({"figure": _forged_png()}, ImageValueForged, "image_value_forged"),
        (
            {"figure": {"okg_image": "file", "path": "figs/plot.png", "error": "image_file_missing"}},
            ImageValueForged, "image_value_forged",
        ),
        ({"panels": [ImageRef.url(URL), _forged_png()]}, ImageValueForged, "image_value_forged"),
        ({"figure": {"okg_image": "file", "path": "../outside.png"}}, ImageRefInvalid, "image_ref_invalid"),
        ({"figure": {"okg_image": "file", "path": "/etc/passwd"}}, ImageRefInvalid, "image_ref_invalid"),
        ({"figure": {"okg_image": "url", "url": "http://example.org/a.png"}}, ImageRefInvalid, "image_ref_invalid"),
        ({"figure": {"okg_image": "video", "path": "a.mp4"}}, ImageRefInvalid, "image_ref_invalid"),
    ],
    ids=["png", "error", "png-in-list", "dotdot", "absolute", "http", "unknown-kind"],
)
def test_a_forged_or_invalid_value_refuses_the_run_and_writes_nothing(
    runtime: str, tmp_path: Path, attrs: dict[str, Any], error: type, code: str,
) -> None:
    root = tmp_path / "corpus"
    _populate(root)
    facts = [
        # A good fact first, in the same chunk: it must not land either.
        _node("n:good", {"figure": ImageRef.file("figs/plot.png")}),
        _node("n:bad", attrs),
    ]
    with pytest.raises(error) as raised:
        _run(runtime, facts, image_root=str(root))
    assert raised.value.code == code
    assert raised.value.data["fact_identity"] == "n:bad"
    assert raised.value.data["source_name"] == SOURCE
    assert _graph_nodes(runtime) == {}
    assert _blob_rows(runtime) == []


@pytest.mark.postgres
def test_an_edge_carrying_a_forged_value_refuses_the_run(runtime: str, tmp_path: Path) -> None:
    facts = [
        _node("n:a", {"text": "a"}),
        _node("n:b", {"text": "b"}),
        EdgeFact(
            src="n:a", dst="n:b", edge_type="links_to",
            attrs={"figure": _forged_png()},
            source_record_id={"id": "e:1"}, source_revision={"v": 1},
        ),
    ]
    with pytest.raises(ImageValueForged) as raised:
        _run(runtime, facts, image_root=str(tmp_path))
    assert raised.value.data["fact_kind"] == "edge"
    assert _graph_nodes(runtime) == {}


# ── file opening and rendering, without a database ─────────────────────

def test_read_under_root_refuses_a_symlink_out_of_the_root(tmp_path: Path) -> None:
    outside = tmp_path / "outside.png"
    _png(outside, (4, 4), (0, 0, 0))
    root = tmp_path / "root"
    root.mkdir()
    (root / "escape.png").symlink_to(outside)
    (root / "dir").symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(_Refused) as file_link:
        read_under_root(str(root), "escape.png", max_bytes=MAX_FILE)
    assert file_link.value.reason == "image_path_refused"
    with pytest.raises(_Refused) as dir_link:
        read_under_root(str(root), "dir/outside.png", max_bytes=MAX_FILE)
    assert dir_link.value.reason == "image_path_refused"


def test_read_under_root_refuses_a_fifo_without_blocking(tmp_path: Path) -> None:
    os.mkfifo(tmp_path / "pipe.png")
    with pytest.raises(_Refused) as refused:
        read_under_root(str(tmp_path), "pipe.png", max_bytes=MAX_FILE)
    assert refused.value.reason == "image_path_refused"


def test_read_under_root_names_missing_and_oversize(tmp_path: Path) -> None:
    (tmp_path / "a.bin").write_bytes(b"x" * 11)
    (tmp_path / "file").write_bytes(b"not a directory")
    assert read_under_root(str(tmp_path), "a.bin", max_bytes=11) == b"x" * 11
    for path, max_bytes, reason in [
        ("a.bin", 10, "image_too_large"),
        ("nope/a.bin", 10, "image_file_missing"),
        ("file/a.bin", 10, "image_file_missing"),
        ("b.bin", 10, "image_file_missing"),
    ]:
        with pytest.raises(_Refused) as refused:
            read_under_root(str(tmp_path), path, max_bytes=max_bytes)
        assert refused.value.reason == reason, path
    # A root that is not there names every file missing.
    with pytest.raises(_Refused) as no_root:
        read_under_root(str(tmp_path / "no-root"), "a.bin", max_bytes=10)
    assert no_root.value.reason == "image_file_missing"


def test_read_under_root_accepts_a_root_reached_through_a_symlink(tmp_path: Path) -> None:
    real = tmp_path / "real"
    _png(real / "sub" / "a.png", (4, 4), (0, 0, 0))
    (tmp_path / "alias").symlink_to(real, target_is_directory=True)
    assert read_under_root(str(tmp_path / "alias"), "sub/a.png", max_bytes=MAX_FILE).startswith(b"\x89PNG")


def _png_header_only(width: int, height: int) -> bytes:
    def chunk(kind: bytes, body: bytes) -> bytes:
        return struct.pack(">I", len(body)) + kind + body + struct.pack(">I", zlib.crc32(kind + body))

    ihdr = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
    return b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", ihdr) + chunk(b"IEND", b"")


def test_a_decompression_bomb_is_a_decode_failure() -> None:
    side = 8000
    assert side * side > MAX_DECODE_PIXELS
    result = render(_png_header_only(side, side), source_format="png", config=CONFIG, label="bomb.png")
    assert result.error == "image_decode_failed" and result.png is None


def test_a_corrupt_png_is_a_decode_failure() -> None:
    result = render(b"\x89PNG\r\n\x1a\ngarbage", source_format="png", config=CONFIG, label="bad.png")
    assert result.error == "image_decode_failed"


def test_render_is_deterministic_and_the_id_covers_every_setting() -> None:
    buffer = io.BytesIO()
    Image.new("RGB", (50, 40), (1, 2, 3)).save(buffer, format="PNG")
    content = buffer.getvalue()
    assert render(content, source_format="png", config=CONFIG, label="a").png == render(
        content, source_format="png", config=CONFIG, label="a",
    ).png
    base = render_id("c" * 64, dpi=100, max_side=1600, renderer="pillow")
    assert base == render_id("c" * 64, dpi=100, max_side=1600, renderer="pillow")
    assert len({
        base,
        render_id("d" * 64, dpi=100, max_side=1600, renderer="pillow"),
        render_id("c" * 64, dpi=101, max_side=1600, renderer="pillow"),
        render_id("c" * 64, dpi=100, max_side=1599, renderer="pillow"),
        render_id("c" * 64, dpi=100, max_side=1600, renderer="pdfium:none"),
    }) == 5
