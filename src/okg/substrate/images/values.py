"""The image value: its shapes, and every rule about them (#3046).

An image is a first-class okg value. It lives in a node's attrs, either as
the attr value or as an element of a list value, and is a plain JSON mapping
tagged ``okg_image``. There are two kinds:

* ``{"okg_image": "file", "path": <relative POSIX path>}`` names a file under
  the connector run's ``image_root``. Ingest completes it with exactly one of
  ``"png": {id, sha256, mime_type, width, height, byte_count}`` (the image
  ingest rendered and stored) or ``"error": <reason>`` (why it could not).
* ``{"okg_image": "url", "url": <https URL>}`` names a public image. It
  passes through ingest unchanged.

Only ingest adds ``png`` or ``error``. `check_incoming` is the one check every
write path runs on connector- and caller-supplied attrs, so the graph never
names an image row ingest did not write. `classify` is the read side's view
of a stored value; it never raises.

The write paths that call `check_incoming`, besides the source runner. A new
path that writes caller-supplied node or edge attrs, or an attribute
observation, must call it too:

* SDK: `okg.deployment` `emit_node_fact`, `emit_edge_fact`, and
  `update_node_attrs` (on the new attrs only; the current attrs may hold
  values ingest completed).
* Enrichers: `library.enrichers._fact_emitters.emit_node_fact` and
  `emit_edge_fact`.
* Review admission (MCP record and propose, the CLI, Poros):
  `runtime.admission_workflows.check_review_payload_images` at submit and
  in the MCP server's `_submit_admission_payload`, and again per fact in
  `_normalize_review_payloads` at apply.
* Lens promotion: `lenses.promote._materialize_rule` at plan time and
  `apply_promoted_overlay` before any fact is written.
* MCP: `connector_ingest.connector_ingest_batch`, `fan_out._emit_audit_nodes`
  and `review_as._emit_critique`.
* Attribute overlays: `attributes.record_attribute_observation`, on
  ``{attr_key: value}``.

Where a reference may arrive from, and what becomes of it:

* Connector ingest (the source runner) is the only place a ``file``
  reference is rendered. It is also the only place a value gains ``png``
  or ``error``.
* SDK workflow writes (`emit_node_fact`, `emit_edge_fact`,
  `update_node_attrs`) and enricher output may carry a bare ``file`` or
  ``url`` reference. Nothing renders a ``file`` reference written this way:
  it stays bare and `classify` reads it as ``not_ingested``.
* Edge attrs may carry either kind, from any writer. Ingest renders only
  node attrs, so a ``file`` reference on an edge is never rendered and
  reads as ``not_ingested``.
* Agent record writes refuse a ``file`` reference with ``image_ref_invalid``:
  MCP ``record`` and ``propose``, and the CLI admission submit, all through
  `runtime.admission_workflows.check_review_payload_images`, which runs
  `check_record_write`. A ``file`` image names a file under a connector
  run's image root, and an agent write has no such root. ``url``
  references are accepted.
* Poros export reduces every image value to its bare reference
  (`strip_ingest_fields`) before it leaves, so a peer never receives this
  okg's blob ids and the receiver's checks accept the node. The receiver
  applies the payload through review admission, whose apply-time check is
  `check_incoming`, so a received bare ``file`` reference is accepted and
  reads as ``not_ingested``.
* ``okg snapshot load`` (`db.bootstrap`) is operator-trusted: it applies a
  JSON snapshot straight to the graph. It reduces every node and edge
  image value with `strip_ingest_fields` first, because a snapshot from
  another okg names blob rows this database does not hold. It does not
  refuse an invalid reference; `classify` reads one as ``invalid``.
* A runner retraction fact (``op == "R"``) is not checked. It writes no
  new value: a retraction the runner synthesizes carries the stored attrs
  back, and those already passed this check when they were written.

Image values are found only at the two positions above. A mapping tagged
``okg_image`` nested deeper is ordinary data: no reader treats it as an
image, so it cannot name one.

The check reads a mapping's real entries. A ``dict`` subclass or other
mapping whose views disagree (``items()``, iteration, ``keys()``, lookup,
or a ``dict`` subclass's stored entries), where an image value could hide,
is refused with ``image_ref_invalid``; so is a list or tuple subclass whose
iteration hides an element. Writers serialize through these views, so a
value the check did not see could otherwise reach the graph.

This module is the single definition. The source runner, the SDK
(`okg.deployment.ImageRef`), the read side and blob cleanup all import it.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterator, Mapping, Optional
from urllib.parse import urlsplit

from okg.substrate.errors import SubstrateError

#: The key that marks a mapping as an image value.
IMAGE_TAG = "okg_image"

KIND_FILE = "file"
KIND_URL = "url"
IMAGE_KINDS: frozenset[str] = frozenset({KIND_FILE, KIND_URL})

#: Reasons ingest can record before it has hashed the file's bytes.
PRE_HASH_REASONS: frozenset[str] = frozenset({
    "image_root_undeclared",
    "image_path_refused",
    "image_file_missing",
    "image_too_large",
})
#: Reasons ingest can record after it has hashed the file's bytes; these are
#: stored as failure rows under the render id, so they are cached too --
#: except ``image_render_limit_exceeded`` (a render timeout, memory limit,
#: helper crash or oversize PDF page), which is never stored, so a later run
#: retries it.
POST_HASH_REASONS: frozenset[str] = frozenset({
    "image_format_unsupported",
    "pdf_renderer_unavailable",
    "pdf_render_failed",
    "image_decode_failed",
    "image_render_limit_exceeded",
})
IMAGE_ERROR_REASONS: frozenset[str] = PRE_HASH_REASONS | POST_HASH_REASONS

#: The fields of a completed file value's ``png`` mapping, in order.
PNG_FIELDS: tuple[str, ...] = (
    "id", "sha256", "mime_type", "width", "height", "byte_count",
)
PNG_MIME_TYPE = "image/png"

#: Keys a connector may put on each kind. Ingest-only keys are not here.
_INCOMING_KEYS: Mapping[str, frozenset[str]] = {
    KIND_FILE: frozenset({IMAGE_TAG, "path"}),
    KIND_URL: frozenset({IMAGE_TAG, "url"}),
}
#: Keys only the ingest image step may add.
INGEST_ONLY_KEYS: frozenset[str] = frozenset({"png", "error"})

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


# ── errors ─────────────────────────────────────────────────────────────

class ImageValueForged(SubstrateError):
    """A caller-supplied image value carries ``png`` or ``error``.

    Only the ingest image step adds those keys. A value that arrives with
    one would make the graph name an image row ingest never wrote, or hide
    a file behind an invented failure.
    """

    code = "image_value_forged"


class ImageRefInvalid(SubstrateError):
    """A mapping tagged ``okg_image`` is not a valid image reference: an
    unknown kind, a refused path, a refused URL, or an extra key."""

    code = "image_ref_invalid"


# ── path and URL rules ─────────────────────────────────────────────────

def path_problem(path: Any) -> Optional[str]:
    """Why `path` is not an acceptable image path, or None when it is.

    A path is relative POSIX: non-empty segments joined by ``/``, none of
    them ``.`` or ``..``, with no backslash and no NUL. An absolute path is
    refused because the root is the run's, never the value's.
    """
    if not isinstance(path, str):
        return f"path must be a string, got {type(path).__name__}"
    if not path:
        return "path is empty"
    if "\x00" in path:
        return "path contains NUL"
    if "\\" in path:
        return "path contains a backslash; use POSIX '/' separators"
    if path.startswith("/"):
        return "path is absolute; it must be relative to the run's image root"
    for segment in path.split("/"):
        if segment == "":
            return "path has an empty segment"
        if segment == ".":
            return "path has a '.' segment"
        if segment == "..":
            return "path has a '..' segment"
    return None


def url_problem(url: Any) -> Optional[str]:
    """Why `url` is not an acceptable image URL, or None when it is.

    https only, with a host, and no credentials. NUL and whitespace are
    refused so a URL cannot be read two ways by two parsers, and so are
    `.`/`..` path segments (plain or percent-encoded) and backslashes, which
    a server would resolve past an allowlisted prefix.
    """
    if not isinstance(url, str):
        return f"url must be a string, got {type(url).__name__}"
    if not url:
        return "url is empty"
    if any(ch == "\x00" or ch.isspace() for ch in url):
        return "url contains NUL or whitespace"
    try:
        parts = urlsplit(url)
        parts.port  # noqa: B018 - raises ValueError on a malformed port
    except ValueError as exc:
        return f"url does not parse: {exc}"
    if parts.scheme != "https":
        return f"url scheme must be https, got {parts.scheme or 'none'!r}"
    if "@" in parts.netloc:
        return "url carries credentials"
    if not parts.hostname:
        return "url has no host"
    from okg.substrate.images.config import url_path_problem

    path_issue = url_path_problem(parts.path)
    if path_issue is not None:
        return f"url {path_issue}"
    return None


def _require_path(path: Any) -> str:
    problem = path_problem(path)
    if problem is not None:
        raise ImageRefInvalid(
            f"image path {path!r} is refused: {problem}",
            data={"field": "path", "problem": problem},
            fix_hint="name the image by a relative POSIX path under the run's image root",
        )
    return path


def _require_url(url: Any) -> str:
    problem = url_problem(url)
    if problem is not None:
        raise ImageRefInvalid(
            f"image url {url!r} is refused: {problem}",
            data={"field": "url", "problem": problem},
            fix_hint="name the image by an https URL with a host and no credentials",
        )
    return url


# ── builders ───────────────────────────────────────────────────────────

def file_value(path: str) -> dict[str, Any]:
    """A file image value for `path`, relative to the run's image root."""
    return {IMAGE_TAG: KIND_FILE, "path": _require_path(path)}


def url_value(url: str) -> dict[str, Any]:
    """A URL image value for an https `url`."""
    return {IMAGE_TAG: KIND_URL, "url": _require_url(url)}


class ImageRef:
    """Build image values for connector facts: ``okg.deployment.ImageRef``.

    Each builder returns a plain JSON dict, not an object, so the value
    passes the fact pipeline's JSON checks unchanged and is stored as-is::

        NodeFact(..., attrs={"figure": ImageRef.file("figs/f1.pdf")})

    A file path resolves against ``ConnectorRun.image_root``. Ingest then
    renders the file and completes the value; a connector never sets
    ``png`` or ``error`` itself.
    """

    def __init__(self) -> None:
        raise TypeError("ImageRef builds plain dicts; call ImageRef.file() or ImageRef.url()")

    @staticmethod
    def file(path: str) -> dict[str, Any]:
        """A file image value. `path` is relative POSIX under the image root."""
        return file_value(path)

    @staticmethod
    def url(url: str) -> dict[str, Any]:
        """A URL image value. `url` is https with a host and no credentials."""
        return url_value(url)


def _require_file_value(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ImageRefInvalid(
            "only a file image value can be completed by ingest",
            data={"field": IMAGE_TAG},
        )
    value = _plain_mapping(value, where="image value", tagged_only=False)
    if value.get(IMAGE_TAG) != KIND_FILE:
        raise ImageRefInvalid(
            "only a file image value can be completed by ingest",
            data={"field": IMAGE_TAG},
        )
    if INGEST_ONLY_KEYS & value.keys():
        raise ImageValueForged(
            "this file image value is already completed",
            data={"keys": sorted(INGEST_ONLY_KEYS & value.keys())},
        )
    _check_incoming_value(value, attr=None, index=None)
    return value


def with_png(
    value: Mapping[str, Any],
    *,
    id: str,  # noqa: A002 - the field's name in the value
    sha256: str,
    mime_type: str,
    width: int,
    height: int,
    byte_count: int,
) -> dict[str, Any]:
    """`value` completed with the stored PNG. Ingest only."""
    value = _require_file_value(value)
    png = {
        "id": id, "sha256": sha256, "mime_type": mime_type,
        "width": width, "height": height, "byte_count": byte_count,
    }
    problem = _png_problem(png)
    if problem is not None:
        raise ValueError(f"cannot complete image value: {problem}")
    return {IMAGE_TAG: KIND_FILE, "path": value["path"], "png": png}


def with_error(value: Mapping[str, Any], reason: str) -> dict[str, Any]:
    """`value` completed with a named failure reason. Ingest only."""
    value = _require_file_value(value)
    if reason not in IMAGE_ERROR_REASONS:
        raise ValueError(
            f"image error reason {reason!r} is not one of {sorted(IMAGE_ERROR_REASONS)}"
        )
    return {IMAGE_TAG: KIND_FILE, "path": value["path"], "error": reason}


# ── finding values ─────────────────────────────────────────────────────

def is_image_value(value: Any) -> bool:
    """True for any mapping carrying the ``okg_image`` key, valid or not."""
    return isinstance(value, Mapping) and IMAGE_TAG in value


@dataclass(frozen=True)
class ImageValueSite:
    """Where an image value sits: the attr, and the list index when the
    attr value is a list (None when the value is the attr itself)."""

    attr: str
    index: Optional[int]
    value: Mapping[str, Any]


def iter_image_values(attrs: Any) -> Iterator[ImageValueSite]:
    """Every image value in `attrs`, by attr name, then list position.

    A value counts when it is the attr value or an element of a list (or
    tuple, as frozen facts hold lists) attr value. Anything that is not a
    mapping yields nothing.
    """
    if not isinstance(attrs, Mapping):
        return
    for name in sorted(attrs, key=str):
        value = attrs[name]
        if isinstance(value, (list, tuple)):
            for index, item in enumerate(value):
                if is_image_value(item):
                    yield ImageValueSite(attr=str(name), index=index, value=item)
        elif is_image_value(value):
            yield ImageValueSite(attr=str(name), index=None, value=value)


# ── the write-path check ───────────────────────────────────────────────

def _where(attr: Optional[str], index: Optional[int]) -> str:
    if attr is None:
        return "image value"
    return f"attr {attr!r}" + (f"[{index}]" if index is not None else "")


def _plain_key(key: Any) -> Any:
    # A str subclass may override __hash__/__eq__ and so hide from a set or
    # dict lookup; a writer serializes its characters. str.__str__ returns
    # an exact str with those characters.
    if isinstance(key, str) and type(key) is not str:
        return str.__str__(key)
    return key


def _views_disagree(where: str) -> ImageRefInvalid:
    return ImageRefInvalid(
        f"{where} presents different entries than it holds; image values "
        "must be plain JSON mappings and lists",
        data={"field": "keys", "problem": "mapping_views_disagree"},
        fix_hint="pass plain dicts and lists; build image values with ImageRef",
    )


def _plain_mapping(
    value: Mapping[Any, Any], *, where: str, tagged_only: bool,
) -> dict[Any, Any]:
    """`value`'s real entries as a plain dict with exact-str keys.

    A plain ``dict`` with exact-str keys is returned as it is: none of its
    views can differ. Anything else is read through every view a writer or a
    check might use -- ``items()``, iteration, ``keys()``, lookup, and a
    ``dict`` subclass's stored entries -- and refused with
    `ImageRefInvalid` when they disagree, so ``png`` cannot hide from the
    check and still be serialized. With `tagged_only`, a disagreement is
    refused only when some view carries ``okg_image`` (an attr value that is
    no image may be any mapping); otherwise it is always refused.
    """
    if type(value) is dict and all(type(k) is str for k in dict.keys(value)):
        return value
    try:
        presented = list(value.items())
        iterated = list(value)
        listed = list(value.keys())
        looked_up = [(k, value[k]) for k in iterated]
        stored = list(dict.items(value)) if isinstance(value, dict) else presented
    except Exception as exc:  # noqa: BLE001 - any failure is a refusal here
        raise _views_disagree(where) from exc

    def as_dict(items: list[tuple[Any, Any]]) -> Optional[dict[Any, Any]]:
        out = {_plain_key(k): v for k, v in items}
        return out if len(out) == len(items) else None

    real = as_dict(stored)
    views = [as_dict(presented), as_dict(looked_up), as_dict([(k, None) for k in listed])]
    agree = real is not None and all(
        view is not None and view.keys() == real.keys() for view in views
    ) and all(
        view[k] is real[k] or view[k] == real[k]
        for view in views[:2] for k in real
    )
    if agree:
        return real
    if tagged_only:
        carries_tag = any(
            IMAGE_TAG in {_plain_key(k) for k, _ in items}
            for items in (stored, presented, looked_up)
        ) or IMAGE_TAG in {_plain_key(k) for k in listed}
        if not carries_tag:
            return dict(presented)
    raise _views_disagree(where)


def _plain_sequence(value: Any, *, where: str) -> list[Any]:
    """A list or tuple's real elements; refused when a subclass's iteration
    differs from its stored elements and either holds a mapping."""
    iterated = list(value)
    if type(value) in (list, tuple):
        return iterated
    stored = list(
        list.__iter__(value) if isinstance(value, list) else tuple.__iter__(value)
    )
    if len(stored) == len(iterated) and all(
        a is b for a, b in zip(stored, iterated)
    ):
        return stored
    if any(isinstance(item, Mapping) for item in (*stored, *iterated)):
        raise _views_disagree(where)
    return iterated


def _plain_attrs(attrs: Mapping[Any, Any]) -> dict[Any, Any]:
    """`attrs` with every position an image value can sit at read through
    its real entries (see `_plain_mapping`)."""
    top = _plain_mapping(attrs, where="attrs", tagged_only=False)
    out: dict[Any, Any] = {}
    for name, value in top.items():
        where = _where(str(name), None)
        if isinstance(value, (list, tuple)):
            out[name] = [
                _plain_mapping(item, where=_where(str(name), index), tagged_only=True)
                if isinstance(item, Mapping) else item
                for index, item in enumerate(_plain_sequence(value, where=where))
            ]
        elif isinstance(value, Mapping):
            out[name] = _plain_mapping(value, where=where, tagged_only=True)
        else:
            out[name] = value
    return out


def _check_incoming_value(
    value: Mapping[str, Any], *, attr: Optional[str], index: Optional[int],
) -> None:
    where = _where(attr, index)
    site = {"attr": attr, "index": index}
    value = _plain_mapping(value, where=where, tagged_only=False)
    forged = sorted(INGEST_ONLY_KEYS & value.keys())
    if forged:
        raise ImageValueForged(
            f"{where} carries {forged}, which only okg's ingest image step may "
            f"add; a connector emits the reference alone",
            data={**site, "keys": forged},
            fix_hint="emit ImageRef.file(path) or ImageRef.url(url) and let ingest render it",
        )
    kind = value.get(IMAGE_TAG)
    if not isinstance(kind, str) or kind not in IMAGE_KINDS:
        raise ImageRefInvalid(
            f"{where} has image kind {kind!r}; the kinds are {sorted(IMAGE_KINDS)}",
            data={**site, "field": IMAGE_TAG, "kind": repr(kind)},
            fix_hint="build the value with ImageRef.file() or ImageRef.url()",
        )
    extra = sorted(str(k) for k in set(value) - _INCOMING_KEYS[kind])
    if extra:
        raise ImageRefInvalid(
            f"{where} carries unknown key(s) {extra}; a {kind} image value has "
            f"only {sorted(_INCOMING_KEYS[kind])}",
            data={**site, "field": "keys", "unknown_keys": extra},
            fix_hint="build the value with ImageRef.file() or ImageRef.url()",
        )
    field = "path" if kind == KIND_FILE else "url"
    problem = (path_problem if kind == KIND_FILE else url_problem)(value.get(field))
    if problem is not None:
        raise ImageRefInvalid(
            f"{where} is refused: {problem}",
            data={**site, "field": field, "problem": problem},
            fix_hint=(
                "name the image by a relative POSIX path under the run's image root"
                if kind == KIND_FILE
                else "name the image by an https URL with a host and no credentials"
            ),
        )


def check_incoming(attrs: Any) -> None:
    """Refuse caller-supplied attrs whose image values are forged or invalid.

    Every write path that accepts attrs from a connector, an adapter or an
    SDK caller runs this before anything is written: `ImageValueForged`
    when a value carries ``png`` or ``error``, `ImageRefInvalid` for an
    unknown kind, a bad path or URL, or an extra key. The first offending
    value, in `iter_image_values` order, is reported.

    The check reads each mapping's real entries; a mapping or list whose
    views disagree where an image value could hide is refused with
    `ImageRefInvalid` (see the module docstring).
    """
    if not isinstance(attrs, Mapping):
        return
    for site in iter_image_values(_plain_attrs(attrs)):
        _check_incoming_value(site.value, attr=site.attr, index=site.index)


#: Why an agent record write may not carry a file reference.
FILE_REF_FROM_INGEST_ONLY = (
    "file images come only from a connector's ingest run, which renders them "
    "from the run's image root; an agent record write may name an image by "
    "an https URL"
)


def check_record_write(attrs: Any) -> None:
    """`check_incoming`, plus: refuse a ``file`` reference.

    Agent record writes (MCP ``record`` and ``propose``, the CLI admission
    submit) run this instead of `check_incoming`. A ``file`` reference
    names a file under a connector run's image root, and an agent write has
    none, so the value could never be rendered; it is refused with
    `ImageRefInvalid`. A ``url`` reference is accepted.
    """
    if not isinstance(attrs, Mapping):
        return
    for site in iter_image_values(_plain_attrs(attrs)):
        _check_incoming_value(site.value, attr=site.attr, index=site.index)
        if site.value.get(IMAGE_TAG) == KIND_FILE:
            where = _where(site.attr, site.index)
            raise ImageRefInvalid(
                f"{where} is a file image reference; {FILE_REF_FROM_INGEST_ONLY}",
                data={
                    "attr": site.attr,
                    "index": site.index,
                    "field": IMAGE_TAG,
                    "kind": KIND_FILE,
                    "problem": "file_ref_outside_ingest",
                },
                fix_hint="name the image with ImageRef.url(<https URL>)",
            )


# ── reducing a value to its reference ──────────────────────────────────

def to_reference(value: Any) -> Any:
    """An image value without its ingest-only keys (``png``, ``error``).

    What a value means to a different okg: the reference alone, which that
    okg's checks accept and which names none of this okg's blob rows. A
    value that is not an image value is returned unchanged.
    """
    if not is_image_value(value):
        return value
    plain = _plain_mapping(value, where="image value", tagged_only=False)
    if not INGEST_ONLY_KEYS & plain.keys():
        return plain
    return {k: v for k, v in plain.items() if k not in INGEST_ONLY_KEYS}


def strip_ingest_fields(attrs: Any) -> Any:
    """`attrs` with every image value reduced by `to_reference`.

    Only the positions `iter_image_values` reads are touched: an attr value
    and an element of a list attr value. Returns `attrs` itself when no
    value carries ``png`` or ``error``, else a new dict (lists become
    lists). Anything that is not a mapping is returned unchanged.
    """
    if not isinstance(attrs, Mapping):
        return attrs
    plain = _plain_attrs(attrs)
    if not any(INGEST_ONLY_KEYS & site.value.keys() for site in iter_image_values(plain)):
        return attrs
    out: dict[Any, Any] = {}
    for name, value in plain.items():
        if isinstance(value, list):
            out[name] = [to_reference(item) for item in value]
        else:
            out[name] = to_reference(value)
    return out


# ── the read-side view ─────────────────────────────────────────────────

STATE_PNG = "png"
STATE_ERROR = "error"
STATE_NOT_INGESTED = "not_ingested"
STATE_URL = "url"
STATE_INVALID = "invalid"


@dataclass(frozen=True)
class StoredPng:
    """The ``png`` mapping of a completed file value."""

    id: str
    sha256: str
    mime_type: str
    width: int
    height: int
    byte_count: int


@dataclass(frozen=True)
class ClassifiedImage:
    """One stored image value, read.

    ``state`` is one of ``png``, ``error``, ``not_ingested``, ``url`` or
    ``invalid``. ``path`` is set for a file value whose path is valid;
    ``png`` for state ``png``; ``reason`` for state ``error``; ``url`` for
    state ``url``; ``problem`` says why a value is ``invalid``.
    """

    state: str
    path: Optional[str] = None
    url: Optional[str] = None
    png: Optional[StoredPng] = None
    reason: Optional[str] = None
    problem: Optional[str] = None


def _is_count(value: Any, *, minimum: int) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= minimum


def _png_problem(png: Any) -> Optional[str]:
    if not isinstance(png, Mapping):
        return "png is not a mapping"
    try:
        png = _plain_mapping(png, where="png", tagged_only=False)
    except ImageRefInvalid:
        return "png mapping presents different entries than it holds"
    keys = set(png)
    if keys != set(PNG_FIELDS):
        return f"png keys are {sorted(map(str, keys))}, expected {sorted(PNG_FIELDS)}"
    for name in ("id", "sha256"):
        if not isinstance(png[name], str) or not _SHA256_RE.match(png[name]):
            return f"png.{name} is not a lowercase sha256 hex digest"
    if png["mime_type"] != PNG_MIME_TYPE:
        return f"png.mime_type is {png['mime_type']!r}, expected {PNG_MIME_TYPE!r}"
    for name in ("width", "height", "byte_count"):
        if not _is_count(png[name], minimum=1):
            return f"png.{name} is not a positive integer"
    return None


def classify(value: Any) -> ClassifiedImage:
    """Read one stored image value. Never raises: a malformed value is
    state ``invalid`` with the problem named."""
    if isinstance(value, Mapping):
        try:
            value = _plain_mapping(value, where="image value", tagged_only=False)
        except ImageRefInvalid:
            return ClassifiedImage(
                STATE_INVALID,
                problem="mapping presents different entries than it holds",
            )
    if not is_image_value(value):
        return ClassifiedImage(STATE_INVALID, problem="not an okg_image mapping")
    kind = value.get(IMAGE_TAG)
    if kind == KIND_URL:
        extra = set(value) - _INCOMING_KEYS[KIND_URL]
        if extra:
            return ClassifiedImage(STATE_INVALID, problem=f"url value has unknown keys {sorted(map(str, extra))}")
        problem = url_problem(value.get("url"))
        if problem is not None:
            return ClassifiedImage(STATE_INVALID, problem=problem)
        return ClassifiedImage(STATE_URL, url=value["url"])
    if kind != KIND_FILE:
        return ClassifiedImage(STATE_INVALID, problem=f"unknown image kind {kind!r}")
    extra = set(value) - _INCOMING_KEYS[KIND_FILE] - INGEST_ONLY_KEYS
    if extra:
        return ClassifiedImage(STATE_INVALID, problem=f"file value has unknown keys {sorted(map(str, extra))}")
    problem = path_problem(value.get("path"))
    if problem is not None:
        return ClassifiedImage(STATE_INVALID, problem=problem)
    path = value["path"]
    has_png, has_error = "png" in value, "error" in value
    if has_png and has_error:
        return ClassifiedImage(STATE_INVALID, path=path, problem="file value carries both png and error")
    if has_png:
        png = value["png"]
        problem = _png_problem(png)
        if problem is not None:
            return ClassifiedImage(STATE_INVALID, path=path, problem=problem)
        return ClassifiedImage(
            STATE_PNG, path=path,
            png=StoredPng(**{name: png[name] for name in PNG_FIELDS}),
        )
    if has_error:
        reason = value["error"]
        if not isinstance(reason, str) or reason not in IMAGE_ERROR_REASONS:
            return ClassifiedImage(STATE_INVALID, path=path, problem=f"unknown error reason {reason!r}")
        return ClassifiedImage(STATE_ERROR, path=path, reason=reason)
    return ClassifiedImage(STATE_NOT_INGESTED, path=path)


__all__ = [
    "ClassifiedImage",
    "IMAGE_ERROR_REASONS",
    "IMAGE_KINDS",
    "IMAGE_TAG",
    "INGEST_ONLY_KEYS",
    "ImageRef",
    "ImageRefInvalid",
    "ImageValueForged",
    "ImageValueSite",
    "KIND_FILE",
    "KIND_URL",
    "PNG_FIELDS",
    "PNG_MIME_TYPE",
    "POST_HASH_REASONS",
    "PRE_HASH_REASONS",
    "STATE_ERROR",
    "STATE_INVALID",
    "STATE_NOT_INGESTED",
    "STATE_PNG",
    "STATE_URL",
    "StoredPng",
    "FILE_REF_FROM_INGEST_ONLY",
    "check_incoming",
    "check_record_write",
    "classify",
    "file_value",
    "is_image_value",
    "iter_image_values",
    "path_problem",
    "strip_ingest_fields",
    "to_reference",
    "url_problem",
    "url_value",
    "with_error",
    "with_png",
]
