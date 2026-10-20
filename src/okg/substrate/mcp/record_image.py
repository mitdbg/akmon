"""`inspect(target={"kind": "image"})`: the image a node points at (#3046).

The read surface otherwise returns text and structured JSON only, so an
agent that reaches the graph through MCP can read a figure's caption and
where its image lives, but never look at it. This read returns the image
itself, as MCP image content, beside a metadata payload.

It is a node read like `inspect(kind="text")` and inherits that read's
guarantees, in the same order (`record_text._visible_start`): the session
pin, accepted attribute overrides, and the Nomos row filter -- a hidden
node and an absent one get the same answer. A node whose attrs were
redacted for this caller is refused before any image bytes are touched,
and the node's own text in the payload is fenced by its producer's trust.

WHERE THE BYTES COME FROM is an `ImageSource`, supplied by the launch from
deployment configuration; with none, the kind is not served at all
(`effective_surface.IMAGE_INSPECT_KINDS`). The source decides which images
a node has and reads their bytes; this module decides everything else:
which panels are sent, how they are rendered, and the per-call budget.
Image bytes never enter the payload -- they leave through
`response_bounds.attach_images`, so the 64 KiB response bound, which
measures the payload, and the call trace, which records it, see metadata
only.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Optional, Protocol, Sequence

import psycopg

from okg.substrate.errors import SubstrateError
from okg.substrate.imaging import (
    ImageLimits,
    ImageRefused,
    PreparedImage,
    prepare_image,
    sniff_image_format,
)
from okg.substrate.mcp.provenance import classify_sources, serialize_node_read_row
from okg.substrate.mcp.record_text import _checkout, _visible_start
from okg.substrate.mcp.session import Session, assert_session_matches
from okg.substrate.nomos.read_masking import MASK_TOKEN, REDACTION_MARKER, ReadRedactedAttrs


# ── the byte source ────────────────────────────────────────────────────

@dataclass(frozen=True)
class ImageRef:
    """One image a node points at.

    `display` is what a caller is shown for it -- a root-relative path or a
    name, never an absolute host path. `locator` is opaque to this module.
    """

    display: str
    locator: Any
    #: Set when the source already knows the image cannot be read (a
    #: reference to a file that was never committed, say). A closed code.
    unavailable: Optional[str] = None
    #: The bytes are a PNG ingest already rendered and stored at the
    #: deployment's `render.dpi`; only URL images are rendered at read time.
    rendered_at_ingest: bool = False


class ImageUnavailable(Exception):
    """The source cannot produce this image's bytes. `reason` is a closed
    code safe to show a caller; the message may carry detail for logs."""

    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(detail or reason)
        self.reason = reason


class ImageReadDeadline(ImageUnavailable):
    """The read waited for a shared resource (the server's one render
    helper) until the call's deadline. The panel was not read, so it is
    offered with its `request` rather than failed."""

    def __init__(self, detail: str = "") -> None:
        super().__init__("not_read_call_deadline", detail)


class ImageSource(Protocol):
    """Where a node's images are and how to read them.

    Implementations must never read a location the graph alone names: the
    graph may say WHICH image, only operator configuration may say WHERE
    images may be read from (see #3046).
    """

    def refs(self, node: Mapping[str, Any], attr: Optional[str] = None) -> Sequence[ImageRef]:
        """The node's images in order, or only `attr`'s; empty if none."""

    def read(
        self, conn: psycopg.Connection, ref: ImageRef, *, dpi: int, max_side: int,
        deadline: Optional[float] = None,
    ) -> bytes:
        """The image as PNG or JPEG bytes, or raise ImageUnavailable. Never a
        document to decode: a source that renders (a URL PDF) renders at the
        deployment's `render.dpi` and `max_side` in a bounded process of its
        own. The caller never chooses `dpi` (#3046). `deadline` is the
        call's `time.monotonic()` deadline: a read that would wait past it
        for a shared resource raises ImageReadDeadline. `conn` is the
        read's own connection, for bytes stored in the graph's database."""

    def text_attrs(self, node: Mapping[str, Any]) -> Sequence[str]:
        """Attrs worth showing beside the image (a caption, a label)."""


# ── the budget ─────────────────────────────────────────────────────────

@dataclass(frozen=True)
class ImageBudget:
    """How much image one call may send. Separate from the response bound:
    that one measures the metadata payload, this one the image bytes."""

    #: The deployment's `agent_surface.images.render.dpi`. There is no
    #: read-time dpi: a caller cannot choose one (#3046).
    render_dpi: int = 100
    max_side: int = 1600
    max_panels: int = 4
    max_bytes: int = 1_500_000
    #: Wall-clock seconds one call may spend reading panels that are not
    #: stored PNGs (URL fetches and renders). Past it, such a panel is
    #: offered with its `request` instead of read.
    read_deadline_s: float = 45.0


# ── refusals ───────────────────────────────────────────────────────────

class InvalidImageInspection(SubstrateError):
    """A malformed `inspect(kind="image")` target. `data.field` names it."""

    code = "invalid_image_inspection"


class NoImageForNode(SubstrateError):
    """The node is visible but points at no image this server can serve."""

    code = "image_not_available"


class ImageWithheld(SubstrateError):
    """The node's attrs are redacted for this caller, so its images are too."""

    code = "image_withheld"


def _bounded_int(value: Any, name: str, lo: int, hi: Optional[int]) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < lo or (hi is not None and value > hi):
        bound = f"between {lo} and {hi}" if hi is not None else f"at least {lo}"
        raise InvalidImageInspection(
            f"inspect image: {name} must be an integer {bound}, got {value!r}",
            data={"field": name, "value": value, "minimum": lo, "maximum": hi},
            fix_hint=f"pass {name} as an integer {bound}, or omit it",
        )
    return value


def _is_redacted(attrs: Any) -> bool:
    """The same test the operator record read applies
    (`console_views.records._row_is_redacted`): a filter-redacted attrs
    mapping, the redaction marker, or any single field masked."""
    if isinstance(attrs, ReadRedactedAttrs):
        return True
    if not isinstance(attrs, Mapping):
        return False
    if attrs.get(REDACTION_MARKER) not in (None, False, "", 0, "false"):
        return True
    return any(isinstance(v, str) and v.strip() == MASK_TOKEN for v in attrs.values())


# ── the read ───────────────────────────────────────────────────────────

@dataclass
class ImageRead:
    payload: dict[str, Any]
    images: list[PreparedImage] = field(default_factory=list)


def read_record_image(
    conn: psycopg.Connection,
    session: Session,
    node_id: str,
    *,
    source: ImageSource,
    panel: Any = None,
    attr: Any = None,
    budget: ImageBudget = ImageBudget(),
) -> ImageRead:
    """The node's images, rendered, within `budget`; and a payload listing
    every panel with what happened to it.

    With `panel` set, only that panel. Otherwise the first `max_panels` that
    render, while the byte budget lasts; every other panel is listed with
    how to ask for it -- but only if asking would work.
    """
    if not node_id:
        raise InvalidImageInspection(
            "inspect image: node_id is required",
            data={"field": "node_id", "value": node_id},
            fix_hint='pass target={"kind":"image","node_id":"<node id>"}',
        )
    if attr is not None and (not isinstance(attr, str) or not attr):
        raise InvalidImageInspection(
            f"inspect image: attr must be a non-empty attribute name, got {attr!r}",
            data={"field": "attr", "value": attr},
            fix_hint="pass the name of the attribute holding the images, or omit it",
        )
    panel = _bounded_int(panel, "panel", 1, None)

    assert_session_matches(conn, session)
    graph = _checkout(conn, session)
    node = _visible_start(conn, session, graph, node_id)
    attrs = node.get("attrs") or {}
    if _is_redacted(attrs):
        raise ImageWithheld(
            f"node {node_id!r} is redacted for this caller, so its images are withheld",
            data={"node_id": node_id, "generation_id": session.generation_id},
            fix_hint="read the node with inspect(kind='node') to see what is visible",
        )
    refs = list(source.refs(node, attr))
    if not refs:
        raise NoImageForNode(
            f"node {node_id!r} ({node.get('subtype')}) points at no image this server serves",
            data={"node_id": node_id, "subtype": node.get("subtype"), "attr": attr},
            fix_hint="inspect a node that carries image values, or check `attr`",
        )
    if panel is not None and panel > len(refs):
        raise InvalidImageInspection(
            f"inspect image: node {node_id!r} has {len(refs)} panel(s); panel={panel}",
            data={"field": "panel", "value": panel, "maximum": len(refs)},
            fix_hint=f"pass panel between 1 and {len(refs)}, or omit it",
        )

    import time

    limits = ImageLimits(
        dpi=budget.render_dpi,
        max_side=budget.max_side,
        jpeg_above_bytes=budget.max_bytes // 2,
    )
    wanted = [panel] if panel is not None else list(range(1, len(refs) + 1))
    images: list[PreparedImage] = []
    rows: dict[int, dict[str, Any]] = {}
    spent = 0
    truncated = False
    deadline = time.monotonic() + budget.read_deadline_s

    def offer(row: dict[str, Any], n: int) -> None:
        # The exact call that fetches this panel: the same attr filter, or
        # panel n would name a different image.
        request: dict[str, Any] = {"kind": "image", "node_id": node_id, "panel": n}
        if attr is not None:
            request["attr"] = attr
        row["request"] = request
    for n in wanted:
        ref = refs[n - 1]
        row: dict[str, Any] = {"panel": n, "image": ref.display}
        rows[n] = row
        if ref.unavailable:
            row["status"] = ref.unavailable
            continue
        # Past the panel cap a panel is offered, not sent. A stored PNG is
        # read first, a cheap row lookup, so its offer is known to work; a
        # URL panel is offered unread, because reading it is a fetch and a
        # render for an image this call will not send.
        over_cap = panel is None and len(images) >= budget.max_panels
        if not ref.rendered_at_ingest:
            if over_cap:
                row["status"] = "not_sent_panel_cap"
                row["read"] = False
                offer(row, n)
                truncated = True
                continue
            if time.monotonic() > deadline:
                row["status"] = "not_read_call_deadline"
                row["read"] = False
                offer(row, n)
                truncated = True
                continue
        try:
            content = source.read(
                conn, ref, dpi=limits.dpi, max_side=limits.max_side, deadline=deadline,
            )
        except ImageReadDeadline:
            # Queued behind another caller's render until this call's
            # deadline: not read, so offered like a panel the deadline
            # stopped before it started.
            row["status"] = "not_read_call_deadline"
            row["read"] = False
            offer(row, n)
            truncated = True
            continue
        except ImageUnavailable as exc:
            row["status"] = exc.reason
            continue
        if over_cap:
            row["status"] = "not_sent_panel_cap"
            offer(row, n)
            truncated = True
            continue
        if sniff_image_format(content) not in ("png", "jpeg"):
            # A source hands back raster bytes only; rendering a document
            # here would put PDFium in the server process (#3046 review).
            row["status"] = "image_format_unsupported"
            continue
        try:
            prepared = prepare_image(content, limits=limits, label=ref.display)
        except ImageRefused as exc:
            row["status"] = exc.reason
            continue
        if images and spent + len(prepared.data) > budget.max_bytes:
            row["status"] = "not_sent_byte_budget"
            offer(row, n)
            truncated = True
            continue
        spent += len(prepared.data)
        images.append(prepared)
        if ref.rendered_at_ingest:
            row["rendered_at_ingest"] = True
        row.update(
            status="attached",
            content_index=len(images),
            mime_type=prepared.mime_type,
            width=prepared.width,
            height=prepared.height,
            byte_count=len(prepared.data),
            sha256=prepared.sha256,
            source_format=prepared.source_format,
        )

    trust = classify_sources(
        conn, deployment_name=session.deployment_name, sources=[node.get("source")],
    ).get(node.get("source"), "untrusted")
    shown = {
        key: attrs[key] for key in source.text_attrs(node)
        if isinstance(attrs, Mapping) and isinstance(attrs.get(key), str)
    }
    described = serialize_node_read_row(
        {"node_id": node_id, "source": node.get("source"), "attrs": shown},
        trust=trust,
    )
    payload: dict[str, Any] = {
        "status": "ok",
        "node_id": node_id,
        "subtype": node.get("subtype"),
        "generation_id": session.generation_id,
        "pinned_generation_id": session.generation_id,
        "attrs": described.get("attrs", {}),
        "provenance": described.get("provenance"),
        "attr": attr,
        "panel_count": len(refs),
        "panels": [rows[n] for n in sorted(rows)],
        "images_attached": len(images),
        "image_bytes": spent,
        # URL panels render at this; stored panels were rendered at ingest.
        "render_dpi": limits.dpi,
        "truncated": truncated,
    }
    if not images:
        payload["note"] = "no panel could be attached; see each panel's status"
    return ImageRead(payload=payload, images=images)
