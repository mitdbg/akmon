"""`inspect(target={"kind": "image"})` -- the image a node points at (#3046).

Three layers, each without a database:

* `okg.substrate.imaging` over real Pillow-written files (PDF rendering
  through pypdfium2 where it is installed; its absence is faked);
* `record_image.read_record_image` with the node read stubbed the way
  `test_record_text.py` stubs it, and a fake `ImageSource`;
* the registered call path through `ByteBoundedFastMCP.call_tool`, on the
  surface-probe server, for the published schema, the serving guard and
  the image content blocks.
"""
from __future__ import annotations

import base64
import io
import json
import sys
from types import SimpleNamespace
from typing import Any

import pytest
from PIL import Image

from okg.substrate.errors import NodeNotFound, SubstrateError
from okg.substrate.imaging import (
    ImageLimits,
    ImageRefused,
    prepare_image,
    sniff_image_format,
)
from okg.substrate.mcp import record_image
from okg.substrate.mcp.record_image import (
    ImageBudget,
    ImageRef,
    ImageUnavailable,
    ImageWithheld,
    InvalidImageInspection,
    NoImageForNode,
    read_record_image,
)


def _png(size=(400, 300), colour=(30, 60, 200)) -> bytes:
    out = io.BytesIO()
    Image.new("RGB", size, colour).save(out, "PNG")
    return out.getvalue()


def _noisy_png(size=(900, 900)) -> bytes:
    """Incompressible: large as PNG, so the JPEG fallback has a reason."""
    import os

    out = io.BytesIO()
    Image.frombytes("RGB", size, os.urandom(size[0] * size[1] * 3)).save(out, "PNG")
    return out.getvalue()


def _pdf(size=(400, 300)) -> bytes:
    out = io.BytesIO()
    Image.new("RGB", size, (200, 30, 30)).save(out, "PDF")
    return out.getvalue()


def _decoded(data: bytes) -> Image.Image:
    return Image.open(io.BytesIO(data))


# ── imaging ────────────────────────────────────────────────────────────

@pytest.mark.parametrize("content,expected", [
    (_png(), "png"),
    (b"\xff\xd8\xff\xe0rest", "jpeg"),
    (b"%PDF-1.7\n...", "pdf"),
    (b"%!PS-Adobe-3.0 EPSF-3.0\n", "postscript"),
    (b'<?xml version="1.0"?>\n<svg xmlns="http://www.w3.org/2000/svg"/>', "svg"),
    (b"<svg/>", "svg"),
    (b"not an image", "unknown"),
])
def test_the_format_comes_from_the_bytes_not_a_name(content, expected) -> None:
    assert sniff_image_format(content) == expected


def test_a_png_is_downscaled_to_the_max_side() -> None:
    prepared = prepare_image(_png((3000, 1000)), limits=ImageLimits(max_side=600), label="p")
    assert (prepared.width, prepared.height) == (600, 200)
    assert prepared.mime_type == "image/png"
    assert _decoded(prepared.data).size == (600, 200)


def test_a_large_png_is_sent_as_jpeg_rather_than_dropped() -> None:
    prepared = prepare_image(
        _noisy_png(), limits=ImageLimits(max_side=900, jpeg_above_bytes=100_000), label="p",
    )
    assert prepared.mime_type == "image/jpeg"
    assert _decoded(prepared.data).format == "JPEG"


def test_a_pdf_renders_page_one_at_the_requested_dpi() -> None:
    pytest.importorskip("pypdfium2")
    # Pillow writes a 400 px image as a 400 pt page at its default 72 dpi.
    at72 = prepare_image(_pdf(), limits=ImageLimits(dpi=72), label="p")
    at144 = prepare_image(_pdf(), limits=ImageLimits(dpi=144), label="p")
    assert at72.width == 400 and at144.width == 800
    assert at72.source_format == "pdf" and at72.mime_type == "image/png"


def test_a_pdf_without_pdfium_is_refused_by_name(monkeypatch) -> None:
    monkeypatch.setitem(sys.modules, "pypdfium2", None)
    with pytest.raises(ImageRefused) as excinfo:
        prepare_image(_pdf(), limits=ImageLimits(), label="p")
    assert excinfo.value.reason == "pdf_renderer_unavailable"


@pytest.mark.parametrize("content", [
    b"%!PS-Adobe-3.0 EPSF-3.0\n%%BoundingBox: 0 0 10 10\n",
    b"<svg xmlns='http://www.w3.org/2000/svg'/>",
    b"GIF89a but we never said we serve these",
])
def test_formats_without_a_renderer_are_refused_by_name(content) -> None:
    with pytest.raises(ImageRefused) as excinfo:
        prepare_image(content, limits=ImageLimits(), label="p")
    assert excinfo.value.reason == "image_format_unsupported"


def test_a_corrupt_png_is_a_decode_failure_not_a_crash() -> None:
    with pytest.raises(ImageRefused) as excinfo:
        prepare_image(_png()[:40], limits=ImageLimits(), label="p")
    assert excinfo.value.reason == "image_decode_failed"


# ── the read, hermetic ─────────────────────────────────────────────────

class FakeSource:
    """Images listed on the node as attrs["images"]; bytes from a dict."""

    def __init__(self, store: dict[str, bytes]) -> None:
        self.store = store
        self.reads: list[str] = []

    def refs(self, node, attr=None):
        out = []
        name = attr or "images"
        for entry in (node.get("attrs") or {}).get(name) or []:
            out.append(ImageRef(
                display=entry["name"], locator=entry["name"],
                unavailable=entry.get("unavailable"),
                # stored unless the entry says it is fetched on the call
                rendered_at_ingest=not entry.get("url", False),
            ))
        return out

    def read(self, _conn, ref, *, dpi, max_side, deadline=None):
        self.reads.append(ref.locator)
        if ref.locator not in self.store:
            raise ImageUnavailable("image_missing", ref.locator)
        return self.store[ref.locator]

    def text_attrs(self, node):
        return ("caption", "label")


SESSION = SimpleNamespace(generation_id="g1", deployment_name="d", branch_name="default")


def _node(images, **attrs):
    return {
        "node_id": "fig:1", "subtype": "figure", "source": "papers",
        "attrs": {"images": images, "caption": "The mass peak.", "label": "fig:mass", **attrs},
    }


@pytest.fixture
def stub_read(monkeypatch):
    """The node read, stubbed at the same seams test_record_text uses."""
    state: dict[str, Any] = {"node": None}

    def visible(_conn, _session, _graph, node_id):
        if state["node"] is None or state["node"]["node_id"] != node_id:
            raise NodeNotFound(f"node {node_id!r} is not visible", data={"node_id": node_id})
        return state["node"]

    monkeypatch.setattr(record_image, "assert_session_matches", lambda *_: None)
    monkeypatch.setattr(record_image, "_checkout", lambda *_: None)
    monkeypatch.setattr(record_image, "_visible_start", visible)
    monkeypatch.setattr(
        record_image, "classify_sources",
        lambda _conn, *, deployment_name, sources: {s: "untrusted" for s in sources},
    )
    return state


def _read(stub_read, node, source, **kw):
    stub_read["node"] = node
    return read_record_image(None, SESSION, node["node_id"], source=source, **kw)


def test_panels_are_attached_up_to_the_cap_and_the_rest_offered(stub_read) -> None:
    store = {f"p{i}.png": _png() for i in range(1, 6)}
    node = _node([{"name": f"p{i}.png"} for i in range(1, 6)] + [{"name": "never.png"}])
    read = _read(stub_read, node, FakeSource(store))
    assert len(read.images) == 4
    panels = read.payload["panels"]
    assert [p["status"] for p in panels] == [
        "attached", "attached", "attached", "attached", "not_sent_panel_cap", "image_missing",
    ]
    # offered with the exact call that fetches it; a missing one is not offered
    assert panels[4]["request"] == {"kind": "image", "node_id": "fig:1", "panel": 5}
    assert "request" not in panels[5]
    assert read.payload["truncated"] is True
    assert panels[0]["content_index"] == 1 and panels[0]["mime_type"] == "image/png"


def test_the_payload_carries_metadata_never_image_bytes(stub_read) -> None:
    store = {"a.png": _png((1200, 900))}
    read = _read(stub_read, _node([{"name": "a.png"}]), FakeSource(store))
    encoded = json.dumps(read.payload)
    assert len(encoded) < 4096
    assert base64.b64encode(read.images[0].data)[:64].decode() not in encoded
    assert read.payload["image_bytes"] == len(read.images[0].data)


def test_one_panel_on_request(stub_read) -> None:
    store = {"a.png": _png(), "b.png": _png()}
    read = _read(stub_read, _node([{"name": "a.png"}, {"name": "b.png"}]), FakeSource(store), panel=2)
    assert [p["panel"] for p in read.payload["panels"]] == [2]
    assert len(read.images) == 1 and read.payload["panel_count"] == 2


def test_the_byte_budget_stops_images_but_lists_them(stub_read) -> None:
    one = len(prepare_image(_png(), limits=ImageLimits(), label="x").data)
    store = {"a.png": _png(), "b.png": _png()}
    read = _read(
        stub_read, _node([{"name": "a.png"}, {"name": "b.png"}]), FakeSource(store),
        budget=ImageBudget(max_bytes=one + 10),
    )
    assert [p["status"] for p in read.payload["panels"]] == ["attached", "not_sent_byte_budget"]
    assert read.payload["panels"][1]["request"]["panel"] == 2


def test_a_source_known_gap_is_reported_without_a_read(stub_read) -> None:
    source = FakeSource({})
    read = _read(stub_read, _node([{"name": "gone.pdf", "unavailable": "image_not_committed"}]), source)
    assert read.payload["panels"][0]["status"] == "image_not_committed"
    assert source.reads == [] and read.images == []
    assert "note" in read.payload


def test_the_deployment_render_dpi_and_max_side_reach_the_source(stub_read) -> None:
    """There is no read-time dpi (#3046): a URL image renders at the
    deployment's `render.dpi`, carried by the budget."""
    seen = {}

    class Recording(FakeSource):
        def read(self, conn, ref, *, dpi, max_side, deadline=None):
            seen.update(dpi=dpi, max_side=max_side)
            return super().read(conn, ref, dpi=dpi, max_side=max_side)

    _read(stub_read, _node([{"name": "a.png"}]), Recording({"a.png": _png()}),
          budget=ImageBudget(render_dpi=144, max_side=900))
    assert seen == {"dpi": 144, "max_side": 900}


def test_an_offered_request_repeats_the_attr_it_was_counted_under(stub_read) -> None:
    """panel=N counts within the attr filter; a request without the attr
    would fetch the Nth image of the whole node instead (review finding)."""
    store = {n: _png() for n in ("a1.png", "a2.png", "f1.png", "f2.png")}
    node = _node([], a_first=[{"name": "a1.png"}, {"name": "a2.png"}],
                 figs=[{"name": "f1.png"}, {"name": "f2.png"}])
    read = _read(stub_read, node, FakeSource(store), attr="figs",
                 budget=ImageBudget(max_panels=1))
    offered = read.payload["panels"][1]
    assert offered["image"] == "f2.png"
    assert offered["request"] == {"kind": "image", "node_id": "fig:1", "panel": 2, "attr": "figs"}
    again = _read(stub_read, node, FakeSource(store), **{k: v for k, v in offered["request"].items()
                                                         if k in ("panel", "attr")})
    assert [p["image"] for p in again.payload["panels"]] == ["f2.png"]


def test_url_panels_past_the_cap_are_offered_without_being_read(stub_read) -> None:
    """Reading one is a fetch and a render for an image the call won't send."""
    store = {f"u{i}.png": _png() for i in range(1, 6)}
    source = FakeSource(store)
    node = _node([{"name": f"u{i}.png", "url": True} for i in range(1, 6)])
    read = _read(stub_read, node, source, budget=ImageBudget(max_panels=2))
    assert source.reads == ["u1.png", "u2.png"]
    rows = read.payload["panels"][2:]
    assert {r["status"] for r in rows} == {"not_sent_panel_cap"}
    assert all(r["read"] is False and r["request"]["panel"] == r["panel"] for r in rows)


def test_url_reads_stop_at_the_call_deadline(stub_read, monkeypatch) -> None:
    import time as time_mod

    clock = iter([0.0, 0.0, 50.0, 50.0, 50.0])
    monkeypatch.setattr(time_mod, "monotonic", lambda: next(clock, 50.0))
    source = FakeSource({"u1.png": _png(), "u2.png": _png(), "s.png": _png()})
    node = _node([{"name": "u1.png", "url": True}, {"name": "u2.png", "url": True}, {"name": "s.png"}])
    read = _read(stub_read, node, source)
    statuses = [p["status"] for p in read.payload["panels"]]
    # the deadline stops URL reads only; a stored panel is a row lookup
    assert statuses == ["attached", "not_read_call_deadline", "attached"]
    assert read.payload["panels"][1]["request"]["panel"] == 2 and read.payload["truncated"] is True


def test_a_url_panel_that_waits_past_the_deadline_is_offered_not_failed(stub_read) -> None:
    """A URL read that waited for the shared render helper until the call
    deadline is offered with its `request`, like one the deadline stopped
    before it started (#3046 review)."""
    from okg.substrate.mcp.image_sources import ImageReadDeadline

    seen = []

    class Queued(FakeSource):
        def read(self, conn, ref, *, dpi, max_side, deadline=None):
            seen.append(deadline)
            if ref.locator == "u2.png":
                raise ImageReadDeadline(ref.locator)
            return super().read(conn, ref, dpi=dpi, max_side=max_side)

    node = _node([{"name": "u1.png", "url": True}, {"name": "u2.png", "url": True}])
    read = _read(stub_read, node, Queued({"u1.png": _png(), "u2.png": _png()}))
    rows = read.payload["panels"]
    assert rows[0]["status"] == "attached"
    assert rows[1]["status"] == "not_read_call_deadline" and rows[1]["read"] is False
    assert rows[1]["request"] == {"kind": "image", "node_id": "fig:1", "panel": 2}
    assert read.payload["truncated"] is True
    # every read is told the call's deadline
    assert seen and all(isinstance(d, float) for d in seen)


def test_a_panel_rendered_at_ingest_says_so(stub_read) -> None:
    """A stored PNG was rendered at ingest; only URL panels render at the
    payload's `render_dpi`."""

    class Stored(FakeSource):
        def refs(self, node, attr=None):
            return [
                ImageRef(display=r.display, locator=r.locator, rendered_at_ingest=r.display == "a.png")
                for r in super().refs(node, attr)
            ]

    store = {"a.png": _png(), "b.png": _png()}
    node = _node([{"name": "a.png"}, {"name": "b.png"}])
    read = _read(stub_read, node, Stored(store), budget=ImageBudget(render_dpi=150))
    assert [p.get("rendered_at_ingest") for p in read.payload["panels"]] == [True, None]
    assert read.payload["render_dpi"] == 150
    assert "dpi" not in read.payload and "dpi_not_applied" not in read.payload


def test_a_source_that_hands_back_a_document_is_refused_not_rendered(stub_read, monkeypatch) -> None:
    """The read path never renders: PDFium in the server process is what the
    #3046 review found a 2 KB PDF can take to 5.5 GB."""
    import okg.substrate.imaging as imaging

    monkeypatch.setattr(imaging, "render_pdf_first_page", lambda *a, **k: pytest.fail("rendered in-process"))
    read = _read(stub_read, _node([{"name": "a.pdf"}]), FakeSource({"a.pdf": _pdf()}))
    assert read.payload["panels"][0]["status"] == "image_format_unsupported"
    assert read.images == []


@pytest.mark.parametrize("kw,field", [
    ({"panel": 0}, "panel"), ({"panel": 3}, "panel"), ({"panel": True}, "panel"),
    ({"panel": "1"}, "panel"),
])
def test_bad_arguments_are_refused_by_field(stub_read, kw, field) -> None:
    node = _node([{"name": "a.png"}, {"name": "b.png"}])
    with pytest.raises(InvalidImageInspection) as excinfo:
        _read(stub_read, node, FakeSource({}), **kw)
    assert excinfo.value.code == "invalid_image_inspection"
    assert excinfo.value.data["field"] == field


def test_node_id_is_required(stub_read) -> None:
    with pytest.raises(InvalidImageInspection) as excinfo:
        read_record_image(None, SESSION, "", source=FakeSource({}))
    assert excinfo.value.data["field"] == "node_id"


def test_a_hidden_node_is_not_found_and_nothing_is_read(stub_read) -> None:
    source = FakeSource({"a.png": _png()})
    stub_read["node"] = None
    with pytest.raises(NodeNotFound):
        read_record_image(None, SESSION, "fig:secret", source=source)
    assert source.reads == []


@pytest.mark.parametrize("redaction", [
    {"_okg_redacted": True},
    {"caption": "[redacted]"},
])
def test_a_redacted_node_is_withheld_before_any_read(stub_read, redaction, monkeypatch) -> None:
    from okg.substrate.nomos import read_masking

    if "_okg_redacted" in redaction:
        redaction = {read_masking.REDACTION_MARKER: True}
    else:
        redaction = {"caption": read_masking.MASK_TOKEN}
    source = FakeSource({"a.png": _png()})
    with pytest.raises(ImageWithheld) as excinfo:
        _read(stub_read, _node([{"name": "a.png"}], **redaction), source)
    assert excinfo.value.code == "image_withheld"
    assert source.reads == []


def test_attr_narrows_to_one_attribute(stub_read) -> None:
    node = _node([{"name": "a.png"}], plots=[{"name": "b.png"}, {"name": "c.png"}])
    read = _read(stub_read, node, FakeSource({"b.png": _png(), "c.png": _png()}), attr="plots")
    assert read.payload["attr"] == "plots" and read.payload["panel_count"] == 2
    assert [p["image"] for p in read.payload["panels"]] == ["b.png", "c.png"]


@pytest.mark.parametrize("attr", ["", 7])
def test_a_bad_attr_is_refused_by_field(stub_read, attr) -> None:
    with pytest.raises(InvalidImageInspection) as excinfo:
        _read(stub_read, _node([{"name": "a.png"}]), FakeSource({}), attr=attr)
    assert excinfo.value.data["field"] == "attr"


def test_a_node_without_images_is_refused_by_name(stub_read) -> None:
    with pytest.raises(NoImageForNode) as excinfo:
        _read(stub_read, _node([]), FakeSource({}))
    assert excinfo.value.code == "image_not_available"


def test_an_untrusted_producers_text_is_fenced(stub_read) -> None:
    read = _read(stub_read, _node([{"name": "a.png"}]), FakeSource({"a.png": _png()}))
    assert read.payload["provenance"]["trust"] == "untrusted"
    caption = read.payload["attrs"]["caption"]
    assert caption != "The mass peak." and "The mass peak." in caption
    assert set(read.payload["attrs"]) == {"caption", "label"}  # only what the source names


# ── the registered call path ───────────────────────────────────────────

@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
def probe(monkeypatch):
    monkeypatch.setenv("OKG_MCP_SURFACE_PROBE", "1")
    monkeypatch.setenv("OKG_MCP_TRACE_ENABLED", "0")
    monkeypatch.delenv("OKG_MCP_TOOL_ALLOWLIST", raising=False)
    monkeypatch.delenv("OKG_MCP_ALLOW_WRITES", raising=False)
    from okg.substrate.mcp import server as server_mod
    from okg.substrate.mcp.surface_budget import PROBE_DSN
    from okg.substrate.sources import registry as registry_mod

    monkeypatch.setattr(
        server_mod, "_server_dsn_resolution",
        lambda _dsn, _name: {"dsn": PROBE_DSN, "authority": {"source": "surface_probe_stub"}},
    )
    monkeypatch.setattr(registry_mod, "_REGISTRY_CACHE", {})
    built = []

    def build(**kw):
        from okg.substrate.mcp.server import build_server

        mcp = build_server(dsn=PROBE_DSN, **kw)
        built.append(mcp)
        return mcp

    yield build
    for mcp in built:
        mcp._okg_pool.close()


def _published_kinds(tool) -> set[str]:
    text = json.dumps(tool.inputSchema)
    from okg.substrate.mcp.operator_models import INSPECT_TARGET_KINDS

    return {k for k in INSPECT_TARGET_KINDS if f'"{k}"' in text}


@pytest.mark.anyio
async def test_an_image_source_serves_the_image_kind_unpublished(probe) -> None:
    # The read surface publishes only its shown kinds (#3666, M10): `image`
    # is served where an image source is declared, and accepted there, but
    # not published on either launch.
    without = {t.name: t for t in await probe().list_tools()}["inspect"]
    served = probe(image_source=FakeSource({}))
    with_source = {t.name: t for t in await served.list_tools()}["inspect"]
    assert "image" not in _published_kinds(without)
    assert _published_kinds(with_source) == _published_kinds(without)
    assert "image" in served._okg_inspect_served_kinds


@pytest.mark.anyio
async def test_without_an_image_source_the_kind_is_refused_by_name(probe) -> None:
    result = await probe().call_tool("inspect", {"target": {"kind": "image", "node_id": "x"}})
    assert result.isError is True
    assert [b.type for b in result.content] == ["text"]
    text = result.content[0].text
    assert "agent_surface_inspect_kind_unserved" in text and "image source" in text


@pytest.mark.anyio
async def test_a_read_time_dpi_is_refused_as_unknown(probe) -> None:
    """There is no read-time dpi (#3046, Jason's decision): images render at
    the deployment's `render.dpi`. A caller's `dpi` is refused and named,
    never silently ignored, and nothing is read."""
    source = FakeSource({"a.png": _png()})
    result = await probe(image_source=source).call_tool(
        "inspect", {"target": {"kind": "image", "node_id": "fig:1", "dpi": 150}},
    )
    assert result.isError is True
    assert [b.type for b in result.content] == ["text"]
    text = result.content[0].text
    assert "invalid_operator_arguments" in text and "dpi" in text
    assert source.reads == []


def _swap_inspect(mcp, monkeypatch, fn) -> None:
    monkeypatch.setattr(mcp._tool_manager._tools["inspect"], "fn", fn)


@pytest.mark.anyio
async def test_a_successful_image_call_carries_image_content(probe, monkeypatch) -> None:
    from okg.substrate.mcp.response_bounds import attach_images

    mcp = probe(image_source=FakeSource({}))
    png = _png()
    payload = {"status": "ok", "node_id": "fig:1", "panels": [{"panel": 1, "status": "attached"}]}

    def fake_inspect(*_a, **_k):
        assert attach_images([(png, "image/png")]) is True
        return dict(payload)

    _swap_inspect(mcp, monkeypatch, fake_inspect)
    result = await mcp.call_tool("inspect", {"target": {"kind": "image", "node_id": "fig:1"}})
    assert result.isError is False
    assert [b.type for b in result.content] == ["text", "image"]
    # the payload as the server renders it comes first, then the image
    assert "fig:1" in result.content[0].text and "attached" in result.content[0].text
    assert base64.b64decode(result.content[1].data) == png
    assert result.content[1].mimeType == "image/png"


@pytest.mark.anyio
async def test_images_from_a_call_that_then_fails_are_never_sent(probe, monkeypatch) -> None:
    from okg.substrate.mcp.response_bounds import attach_images

    mcp = probe(image_source=FakeSource({}))

    def failing(*_a, **_k):
        attach_images([(_png(), "image/png")])
        raise SubstrateError("boom", data={})

    _swap_inspect(mcp, monkeypatch, failing)
    result = await mcp.call_tool("inspect", {"target": {"kind": "image", "node_id": "fig:1"}})
    assert result.isError is True
    assert [b.type for b in result.content] == ["text"]

    # ... and they do not leak into the next call's result either
    _swap_inspect(mcp, monkeypatch, lambda *_a, **_k: {"status": "ok", "node_id": "n"})
    after = await mcp.call_tool("inspect", {"target": {"kind": "node", "node_id": "n"}})
    assert [b.type for b in after.content] == ["text"]


@pytest.mark.anyio
async def test_a_refusal_payload_carries_no_images(probe, monkeypatch) -> None:
    from okg.substrate.mcp.response_bounds import attach_images

    mcp = probe(image_source=FakeSource({}))

    def refusing(*_a, **_k):
        attach_images([(_png(), "image/png")])
        return {"status": "refused", "error": {"code": "x", "message": "no"}}

    _swap_inspect(mcp, monkeypatch, refusing)
    result = await mcp.call_tool("inspect", {"target": {"kind": "image", "node_id": "fig:1"}})
    assert not any(b.type == "image" for b in result.content)


@pytest.mark.anyio
async def test_no_other_tool_can_attach_images(probe, monkeypatch) -> None:
    from okg.substrate.mcp.response_bounds import attach_images

    mcp = probe(image_source=FakeSource({}))
    seen = {}

    def fake_filter(*_a, **_k):
        seen["attached"] = attach_images([(_png(), "image/png")])
        return {"items": [], "total_count": 0, "truncated": False, "pinned_generation_id": "g"}

    monkeypatch.setattr(mcp._tool_manager._tools["filter"], "fn", fake_filter)
    result = await mcp.call_tool("filter", {"input": {"kind": "nodes"}, "predicate": {}})
    assert seen["attached"] is False
    assert [b.type for b in result.content] == ["text"]


def test_in_process_calls_have_no_slot() -> None:
    from okg.substrate.mcp.response_bounds import attach_images

    assert attach_images([(b"x", "image/png")]) is False
