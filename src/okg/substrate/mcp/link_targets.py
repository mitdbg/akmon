"""The one count of links whose target is not loaded (issue #3666).

A route with ``on_miss`` keeps a link to a record the graph does not hold:
the edge reaches a "known by key, not loaded" endpoint. A reply that lists a
record's links says how many it cites and how many of those are not loaded,
in one field, ``link_targets``, so a reader (an agent, or a later join step)
never takes a missing record for a missing link:

    {"cited": N, "loaded": N - M, "not_loaded": M,
     "not_loaded_keys": [up to 10 ids], "basis": ...}

``basis`` is ``exact`` when every outgoing link of the record is on the
page, ``page`` when the page is not the whole list (the counts are this
page's), and ``not_counted`` under access rules (audit or enforce). Under
access rules an endpoint is never shown, so its key is withheld:
``not_loaded`` is null and ``not_loaded_keys`` is empty.
"""
from __future__ import annotations

from pathlib import Path
from typing import AbstractSet, Any, Iterable, Mapping, Optional

from okg.substrate.catalog.linker_config import (
    ENDPOINT_RESERVED_ATTRS,
    LOAD_STATE_ATTR,
    NOT_LOADED,
)

__all__ = [
    "endpoint_edge_types",
    "LINK_TARGETS_KEY",
    "LOAD_NOTE_KEY",
    "NOT_LOADED_KEYS_CAP",
    "endpoint_load_note",
    "link_target_counts",
    "link_targets_line",
    "record_link_target_counts",
]

LINK_TARGETS_KEY = "link_targets"
NOT_LOADED_KEYS_CAP = 10

_ROUTE_CACHE: dict[str, tuple[Optional[float], frozenset[str]]] = {}
_DIR_CACHE: dict[str, Optional[Path]] = {}


def endpoint_edge_types(deployment_name: str) -> frozenset[str]:
    """The edge types an `on_miss` route of this deployment produces.

    Read from the deployment's `linker_config.yaml` (cached by the file's
    modification time). Empty when the deployment has no folder, no linker
    config, or no route declaring `on_miss`: then no endpoint is written and
    a reply carries no `link_targets`. An invalid config that declares
    `on_miss` raises, as the publisher would.
    """
    from okg.substrate.catalog.linker_config import link_endpoint_routes
    from okg.substrate.deployments import find_deployment_dir

    name = str(deployment_name)
    if name not in _DIR_CACHE:
        # Resolved once per process: a deployment's folder does not move
        # under a running server, and this runs on every expand.
        _DIR_CACHE[name] = find_deployment_dir(name)
    deployment_dir = _DIR_CACHE[name]
    if deployment_dir is None:
        return frozenset()
    path = Path(deployment_dir) / "linker_config.yaml"
    try:
        mtime: Optional[float] = path.stat().st_mtime
    except FileNotFoundError:
        return frozenset()
    key = str(path)
    cached = _ROUTE_CACHE.get(key)
    if cached is not None and cached[0] == mtime:
        return cached[1]
    edge_types = frozenset(route.edge_type for route in link_endpoint_routes(path))
    _ROUTE_CACHE[key] = (mtime, edge_types)
    return edge_types


def link_target_counts(
    neighbors: Iterable[Mapping[str, Any]],
    *,
    counted: bool,
    complete: bool,
    edge_types: Optional[AbstractSet[str]] = None,
) -> dict[str, Any]:
    """`link_targets` for the outgoing rows of one expand page.

    ``neighbors`` are served rows: ``direction``, ``dst``, ``edge_type`` and
    the far end's ``neighbor`` summary (whose ``load_state`` marks an
    endpoint). ``counted`` is false under access rules; ``complete`` says the
    page holds every outgoing link. ``edge_types`` are the citation link
    types (those an ``on_miss`` route produces): only those links are
    counted, so a commit's files and authors are not "cited" records.
    """
    outgoing = [
        row for row in neighbors
        if str(row.get("direction") or "out") == "out"
        and (edge_types is None or row.get("edge_type") in edge_types)
    ]
    if not counted:
        return {
            "cited": None,
            "loaded": None,
            "not_loaded": None,
            "not_loaded_keys": [],
            "basis": "not_counted",
        }
    not_loaded = sorted({
        str(row.get("dst"))
        for row in outgoing
        if isinstance(row.get("neighbor"), Mapping)
        and row["neighbor"].get(LOAD_STATE_ATTR) == NOT_LOADED
    })
    cited = len({str(row.get("dst")) for row in outgoing})
    return {
        "cited": cited,
        "loaded": cited - len(not_loaded),
        "not_loaded": len(not_loaded),
        "not_loaded_keys": not_loaded[:NOT_LOADED_KEYS_CAP],
        "basis": "exact" if complete else "page",
    }


def record_link_target_counts(
    rows: Iterable[Mapping[str, Any]],
    *,
    titles: Mapping[str, Any],
    counted: bool,
    complete: bool,
    edge_types: Optional[AbstractSet[str]] = None,
) -> dict[str, Any]:
    """`link_targets` for one page of a record-shaped expand.

    A record row is the far record (``node_id``) with the ``link`` that
    reaches it; ``titles`` is ``resolve_neighbor_summaries``' answer for the
    page, whose ``load_state`` marks an endpoint. The rows are recast as the
    link rows `link_target_counts` reads, so both shapes count alike.
    """
    as_links = []
    for row in rows:
        link = row.get("link")
        summary = titles.get(str(row.get("node_id")))
        as_links.append({
            "dst": row.get("node_id"),
            "direction": link.get("direction") if isinstance(link, Mapping) else None,
            "edge_type": link.get("edge_type") if isinstance(link, Mapping) else None,
            "neighbor": summary if isinstance(summary, Mapping) else None,
        })
    return link_target_counts(
        as_links, counted=counted, complete=complete, edge_types=edge_types,
    )


#: The inspect key that says a record is only known by key.
LOAD_NOTE_KEY = "load_note"


def endpoint_load_note(attrs: Mapping[str, Any]) -> str:
    """inspect's sentence for an endpoint: what is known, and that the
    graph holds no record. The scope (a project, say) is the one attribute
    besides the key, the label and the state."""
    from okg.substrate.searchable_text import PRODUCER_TEXT_ATTR

    label = attrs.get("label")
    scope = [
        f"{key} {value}"
        for key, value in sorted(attrs.items())
        if not str(key).startswith("_")
        and key not in ENDPOINT_RESERVED_ATTRS
        # The store's copy of the endpoint's own text, when the subtype
        # declares searchable fields.
        and key != PRODUCER_TEXT_ATTR
        and value != label
        and isinstance(value, (str, int))
    ]
    where = f" ({'; '.join(scope)})" if scope else ""
    return (
        f"known by key, not loaded{where}: a record cites it, and the "
        "graph holds no record for it"
    )


def link_targets_line(value: Any) -> Optional[str]:
    """The reply's text line, or None when nothing needs saying.

    Said only when a cited record is not loaded: "N cited, M not loaded".
    """
    if not isinstance(value, Mapping):
        return None
    missing = value.get("not_loaded")
    if not isinstance(missing, int) or isinstance(missing, bool) or missing < 1:
        return None
    keys = [str(key) for key in value.get("not_loaded_keys") or []]
    scope = " on this page" if value.get("basis") == "page" else ""
    line = (
        f"! links: {value.get('cited')} cited, {missing} not loaded{scope}"
        " (known by key; the graph holds no record)"
    )
    if keys:
        line += ": " + ", ".join(keys)
        if missing > len(keys):
            line += f" (+{missing - len(keys)} more)"
    return line
