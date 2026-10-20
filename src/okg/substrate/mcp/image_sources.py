"""The byte source behind `inspect(kind="image")` (#3046).

`record_image` decides which panels a call sends and how; this module
decides which images a node has and produces their PNG bytes. The rule both
sides keep is `console_views/records.py`'s: a read path never opens a file
a stored attribute names. The graph says WHICH image; only the database or
an operator's URL allowlist supplies bytes.

A node's images are its `okg_image` values, parsed by
`okg.substrate.images.values` (the one module that owns their shape) and
ordered by `iter_image_values` -- attribute name, then list position:

* a `file` value ingest rendered (`png`): read from the ``okg.image_blobs``
  view by its render id, and checked against the recorded sha256;
* a `file` value ingest could not render (`error`): reported with its reason;
* a `file` value ingest never saw: ``image_not_ingested``, never opened;
* a `url` value: fetched on the call when `agent_surface.images.url` admits
  it, then rendered in the bounded helper process
  (`okg.substrate.images.render_worker`), never in this one -- a hostile
  PDF from an allowlisted host must cost one bounded job, not the server.

`read` always returns PNG (or JPEG) bytes, never a document to decode.
"""
from __future__ import annotations

import hashlib
import http.client
import ipaddress
import re
import socket
import ssl
import threading
import time
from dataclasses import dataclass, field, replace
from typing import Any, Callable, Mapping, Optional, Sequence
from urllib.parse import urljoin, urlsplit

import psycopg

from okg.substrate.images.config import ImagesConfig, UrlConfig
from okg.substrate.imaging import sniff_image_format
from okg.substrate.mcp.record_image import ImageReadDeadline, ImageRef, ImageUnavailable

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_CLOSED_CODE_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_SERVABLE_FORMATS = frozenset({"pdf", "png", "jpeg"})


# ── fetching public images ─────────────────────────────────────────────

#: Redirect hops followed before a fetch gives up.
MAX_REDIRECTS = 3


#: IPv6 ranges that embed an IPv4 address and can reach it, yet report
#: `is_global`: NAT64 (RFC 6052 / 8215) and the deprecated IPv4-compatible
#: form. `64:ff9b::7f00:1` is 127.0.0.1 through a NAT64 gateway.
_EMBEDDED_V4_NETS = tuple(ipaddress.ip_network(n) for n in ("64:ff9b::/96", "64:ff9b:1::/48", "::/96"))


def _is_public(address: str) -> bool:
    ip = ipaddress.ip_address(address.split("%", 1)[0])
    if isinstance(ip, ipaddress.IPv6Address):
        if ip.ipv4_mapped is not None:
            ip = ip.ipv4_mapped
        elif any(ip in net for net in _EMBEDDED_V4_NETS):
            return False
    return ip.is_global and not ip.is_multicast


def resolve_public(host: str, port: int, *, getaddrinfo: Callable[..., Any] = socket.getaddrinfo) -> str:
    """One address for `host`, provided EVERY address it resolves to is
    public. A name with a private or loopback answer among public ones is
    refused outright rather than raced."""
    try:
        infos = getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except OSError:
        raise ImageUnavailable("image_url_unresolvable", host) from None
    addresses = sorted({info[4][0] for info in infos})
    if not addresses:
        raise ImageUnavailable("image_url_unresolvable", host)
    if not all(_is_public(a) for a in addresses):
        raise ImageUnavailable("image_url_not_public", f"{host} -> {addresses}")
    return addresses[0]


class _PinnedHTTPSConnection(http.client.HTTPSConnection):
    """HTTPS to an address already checked, with the certificate verified
    against the hostname: the name is resolved once, so a second lookup
    cannot hand the socket a different (private) address."""

    def __init__(self, host: str, address: str, port: int, timeout: float) -> None:
        super().__init__(host, port, timeout=timeout, context=ssl.create_default_context())
        self._address = address

    def connect(self) -> None:  # noqa: D401 - http.client hook
        sock = socket.create_connection((self._address, self.port), self.timeout)
        self.sock = self._context.wrap_socket(sock, server_hostname=self.host)


def fetch_image_url(
    url: str,
    policy: UrlConfig,
    *,
    resolve: Callable[[str, int], str] = resolve_public,
    connect: Callable[[str, str, int, float], http.client.HTTPConnection] = _PinnedHTTPSConnection,
) -> bytes:
    """The bytes at `url`, or ImageUnavailable with a closed reason.

    Every hop -- the URL and each redirect -- must be under the allowlist
    and resolve to public addresses only. One deadline covers the whole
    fetch; the body is cut at `max_bytes`; the bytes must be a PDF, PNG or
    JPEG by signature. No cookies, no credentials, no caching.
    """
    deadline = time.monotonic() + policy.timeout_s
    current = url
    for _hop in range(MAX_REDIRECTS + 1):
        if not policy.admits(current):
            raise ImageUnavailable("image_url_not_allowed", current)
        parts = urlsplit(current)
        port = parts.port or 443
        address = resolve(parts.hostname, port)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ImageUnavailable("image_fetch_timeout", current)
        conn = None
        try:
            conn = connect(parts.hostname, address, port, remaining)
            target = (parts.path or "/") + (f"?{parts.query}" if parts.query else "")
            conn.request("GET", target, headers={
                "User-Agent": "okg-inspect-image/1",
                "Accept": "image/png, image/jpeg, application/pdf",
            })
            response = conn.getresponse()
            if response.status in (301, 302, 303, 307, 308):
                location = response.getheader("Location")
                if not location:
                    raise ImageUnavailable("image_fetch_failed", f"{current}: redirect without Location")
                current = urljoin(current, location)
                continue
            if response.status != 200:
                raise ImageUnavailable("image_fetch_failed", f"{current}: HTTP {response.status}")
            declared = response.getheader("Content-Length")
            if declared and declared.isdigit() and int(declared) > policy.max_bytes:
                raise ImageUnavailable("image_too_large", f"{current}: {declared} bytes")
            chunks, size = [], 0
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise ImageUnavailable("image_fetch_timeout", current)
                # Each wait is bounded by what is left of the deadline, and
                # read1 returns what has arrived rather than waiting to fill
                # the buffer: a server trickling bytes cannot outlast it.
                sock = getattr(conn, "sock", None)
                if sock is not None:
                    sock.settimeout(remaining)
                chunk = response.read1(64 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                if size > policy.max_bytes:
                    raise ImageUnavailable("image_too_large", f"{current}: over {policy.max_bytes} bytes")
                chunks.append(chunk)
            data = b"".join(chunks)
        except (OSError, http.client.HTTPException) as exc:
            if isinstance(exc, (socket.timeout, TimeoutError)):
                raise ImageUnavailable("image_fetch_timeout", current) from None
            raise ImageUnavailable("image_fetch_failed", f"{current}: {type(exc).__name__}") from None
        finally:
            if conn is not None:
                conn.close()
        if sniff_image_format(data) not in _SERVABLE_FORMATS:
            raise ImageUnavailable("image_format_unsupported", f"{current}: not a PDF, PNG or JPEG")
        return data
    raise ImageUnavailable("image_fetch_failed", f"{url}: more than {MAX_REDIRECTS} redirects")


# ── the source ─────────────────────────────────────────────────────────

@dataclass
class GraphImageSource:
    """A node's `okg_image` values; PNG bytes from ``okg.image_blobs`` or an
    allowlisted URL rendered in the bounded helper."""

    config: ImagesConfig = field(default_factory=ImagesConfig.default)
    fetch: Callable[[str, UrlConfig], bytes] = field(default=fetch_image_url)
    #: The render helper for URL images; started on the first URL render.
    worker: Any = None
    _lock: Any = field(default_factory=threading.Lock, repr=False)

    def refs(self, node: Mapping[str, Any], attr: Optional[str] = None) -> list[ImageRef]:
        from okg.substrate.images.values import classify, iter_image_values

        refs: list[ImageRef] = []
        for n, site in enumerate(
            (s for s in iter_image_values(node.get("attrs")) if attr is None or s.attr == attr), 1,
        ):
            image = classify(site.value)
            display = image.path or image.url or f"image {n}"
            if image.state == "png":
                refs.append(ImageRef(
                    display=display, locator=("blob", image.png), rendered_at_ingest=True,
                ))
            elif image.state == "error":
                refs.append(ImageRef(display=display, locator=None, unavailable=image.reason))
            elif image.state == "not_ingested":
                # Ingest completes every file value; one that reaches the
                # read path was never ingested, and nothing here opens files.
                refs.append(ImageRef(display=display, locator=None, unavailable="image_not_ingested"))
            elif image.state == "url":
                if self.config.url.admits(image.url):
                    refs.append(ImageRef(display=display, locator=("url", image.url)))
                else:
                    refs.append(ImageRef(display=display, locator=None, unavailable="image_url_not_allowed"))
            else:
                refs.append(ImageRef(display=f"image {n}", locator=None, unavailable="image_ref_malformed"))
        return refs

    def read(
        self, conn: psycopg.Connection, ref: ImageRef, *, dpi: int, max_side: int,
        deadline: Optional[float] = None,
    ) -> bytes:
        kind, value = ref.locator
        if kind == "blob":
            return _read_stored_png(conn, value)
        if kind == "url":
            return self._render_url(value, dpi=dpi, max_side=max_side, deadline=deadline)
        raise ImageUnavailable("image_ref_malformed", str(kind))

    def _render_url(
        self, url: str, *, dpi: int, max_side: int, deadline: Optional[float] = None,
    ) -> bytes:
        from okg.substrate.images.render import renderer_for
        from okg.substrate.images.render_worker import LIMIT_REASON, RenderWorker, RenderWorkerError

        # A URL panel in flight holds one of the server's request workers
        # (issue #3493): its fetch and its render each get only what is left
        # of the call deadline, never their own longer limits.
        policy = self.config.url
        if deadline is not None:
            left = deadline - time.monotonic()
            if left <= 0:
                raise ImageReadDeadline(url)
            policy = replace(policy, timeout_s=min(policy.timeout_s, left))
        try:
            content = self.fetch(url, policy)
        except ImageUnavailable as exc:
            if exc.reason == "image_fetch_timeout" and deadline is not None and time.monotonic() >= deadline:
                raise ImageReadDeadline(url) from None
            raise
        source_format = sniff_image_format(content)
        # One helper per server, one job at a time: a render is bounded in
        # time and memory, so serialising them bounds the server too. A
        # caller queued behind another caller's render waits only until its
        # own call deadline (#3046 read-side review). Reads from different
        # sessions run in concurrent request workers (issue #3493), so this
        # wait is real: two image reads can arrive together, and the second
        # waits here, holding its worker, until the first render ends or its
        # own deadline passes.
        if deadline is None:
            acquired = self._lock.acquire()
        else:
            acquired = self._lock.acquire(timeout=max(0.0, deadline - time.monotonic()))
        if not acquired:
            raise ImageReadDeadline(url)
        try:
            if self.worker is None:
                self.worker = RenderWorker()
            try:
                result = self.worker.render(
                    content,
                    source_format=source_format,
                    dpi=dpi,
                    max_side=max_side,
                    renderer=renderer_for(source_format),
                    label=url,
                    **({} if deadline is None else {"timeout_s": deadline - time.monotonic()}),
                )
            except RenderWorkerError:
                # The helper could not start, or the render raised: this
                # panel failed, not the call.
                raise ImageUnavailable("image_render_failed", url) from None
        finally:
            self._lock.release()
        if result.error == LIMIT_REASON and deadline is not None and time.monotonic() >= deadline:
            # Cut short by the call deadline, not by the helper's own limit.
            raise ImageReadDeadline(url)
        if result.error:
            raise ImageUnavailable(result.error, url)
        if not result.png:
            raise ImageUnavailable("image_render_failed", url)
        return result.png

    def text_attrs(self, node: Mapping[str, Any]) -> Sequence[str]:
        """The node's other short string attrs: what it says about itself
        (a caption, a label), never the image values."""
        attrs = node.get("attrs") or {}
        return sorted(
            k for k, v in attrs.items()
            if isinstance(v, str) and len(v) <= 4000 and not k.startswith("_")
        )


def _read_stored_png(conn: psycopg.Connection, png: Any) -> bytes:
    """The PNG ingest stored under `png.id`, through the security-barrier
    view `okg_mcp` is granted -- never the `okg.blobs` table."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT bytes, sha256, error FROM okg.image_blobs WHERE id = %s",
            (png.id,),
        )
        row = cur.fetchone()
    if row is None:
        raise ImageUnavailable("image_blob_missing", png.id)
    data, sha256, error = row
    if error:
        # Ingest writes a closed reason code here; anything else (another
        # writer of okg.blobs) is never shown to a caller as-is.
        reason = str(error)
        raise ImageUnavailable(
            reason if _CLOSED_CODE_RE.match(reason) else "image_render_failed", png.id,
        )
    if data is None or sha256 != png.sha256 or hashlib.sha256(data).hexdigest() != png.sha256:
        # The value and the row disagree: serve nothing rather than bytes
        # the node did not name.
        raise ImageUnavailable("image_blob_mismatch", png.id)
    return bytes(data)
