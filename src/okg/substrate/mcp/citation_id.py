"""The record attribute a reader cites, on every read row (PACT read-names-list-3666).

Issue #3666. A deployment may name, in `deployment.yaml`, the attribute that
holds the id a reader cites for a record (`citation_attr`; a benchmark corpus
may name `dsid`, its document id). Agents never saw it in a text read or a
search row and spent a read per record selecting it. When it is named, every
search row, every grep list row and every `inspect` node or text read
carries it as one short field, ``citation_id: {"<attr>": "<value>"}``, which
the text view prints as ``<attr> <value>`` beside the record's id.

The value is read from the attributes the reader may see: a value the read
filter masked, or dropped from a redacted record, is served as the mask
token, never shown and never read as absent. On a record whose producer is
untrusted the value is fenced like any other producer text. A deployment
that names no attribute gets no field, so its rows do not change.

The server sets :data:`CITATION_ATTR` at the start of each read call from its
own deployment manifest; the readers deep in search and inspect take it from
there, as they take the `attrs.<key>` select.
"""
from __future__ import annotations

import contextvars
import functools
from typing import Any, Mapping, Optional

from okg.substrate.nomos.read_masking import MASK_TOKEN, REDACTION_MARKER

#: The row and payload key.
CITATION_KEY = "citation_id"
#: The deployment.yaml key.
DEPLOYMENT_KEY = "citation_attr"

#: The attribute the current read cites, or None (set per call by the server).
CITATION_ATTR: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "okg_citation_attr", default=None,
)


@functools.lru_cache(maxsize=64)
def deployment_citation_attr(deployment_name: str) -> Optional[str]:
    """The attribute `deployment_name` names as `citation_attr`, or None.

    Read once per server process from the manifest the server was built
    with. A manifest that cannot be resolved names none; the session the
    call opens reports why it cannot be resolved."""
    from okg.substrate.deployments import resolve_deployment

    try:
        manifest = resolve_deployment(deployment_name)
    except Exception:  # noqa: BLE001 - the session's own resolution reports it.
        return None
    return getattr(manifest, "citation_attr", None)


def citation_field(attrs: Any, attr: Optional[str] = None) -> Optional[dict[str, str]]:
    """``{attr: value}`` for one record's reader-visible attrs, else None.

    `attr` defaults to the current call's :data:`CITATION_ATTR`. A masked
    value, or a declared key missing from a redacted record, is the mask
    token. A missing, empty or non-text value gives None."""
    name = attr if attr is not None else CITATION_ATTR.get()
    if not name or not isinstance(attrs, Mapping):
        return None
    raw = attrs.get(name)
    if raw is None:
        if attrs.get(REDACTION_MARKER) is True:
            return {name: MASK_TOKEN}
        return None
    if isinstance(raw, bool) or not isinstance(raw, (str, int)):
        return None
    text = str(raw).strip()
    if not text:
        return None
    return {name: text}


def fenced_citation(field: Optional[dict[str, str]], provenance: Any) -> Optional[dict[str, str]]:
    """`field` with its value fenced when the row's trust is not "trusted"
    (a mask token is the server's, never fenced)."""
    if not field:
        return field
    trust = provenance.get("trust") if isinstance(provenance, Mapping) else None
    if trust == "trusted":
        return field
    from okg.substrate.mcp.provenance import _fence_value

    source = provenance.get("source") if isinstance(provenance, Mapping) else None
    return {
        name: value if value == MASK_TOKEN else _fence_value(value, source=source)
        for name, value in field.items()
    }
