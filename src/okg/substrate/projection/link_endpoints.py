"""Plan "known by key, not loaded" link endpoints (issue #3666).

A route with `on_miss` keeps a link whose target record is not in the graph:
the resolver miss returns the edge to the target's real id, and the
publisher writes a node there from the core producer
``LINK_ENDPOINT_SOURCE`` with ``load_state: not_loaded``. This module only
reads the branch the extractor is writing; the publisher writes the facts.

Rules the plan keeps:

* An endpoint is written only at an id with no live node, so it never
  replaces a loaded record. It is a ``substitute`` declaration, so a real
  record arriving later takes the row and the edge, at the same id, stays.
* An endpoint registers no alias. The landing hook wakes retry rows by alias
  value alone (``retry.mark_link_retries_ready_for_aliases``), so an alias
  of its own would wake its retry rows on every publish.
* An endpoint is retracted when a real record owns its id, or when no edge
  from a live record reaches it any more. The publisher runs that sweep on
  every publish of a deployment with an ``on_miss`` route, extractor run or
  not, so a retraction lands in the publish where its cause does.
"""
from __future__ import annotations

from typing import Any, Iterable, Sequence

from okg.substrate.catalog.linker_config import (
    LINK_ENDPOINT_SOURCE,
    LOAD_STATE_ATTR,
    NOT_LOADED,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.projection.router import LinkEndpointRequest

__all__ = [
    "endpoints_to_retract",
    "endpoints_to_write",
    "plan_link_endpoint_facts",
    "unique_endpoint_requests",
]

_CHUNK = 1000


def plan_link_endpoint_facts(
    conn: Any,
    *,
    checkout: Any,
    requests: Sequence[LinkEndpointRequest],
    catalog_version_id: int,
) -> list[LinkEndpointRequest]:
    """The endpoints one extractor run writes.

    Refuses, before anything is written, an endpoint whose subtype the
    catalog the route read does not hold: an endpoint is a node of that
    subtype, and an unknown subtype would only fail later at the gate.
    """
    writes = endpoints_to_write(checkout, requests)
    subtypes = sorted({request.subtype for request in writes})
    if subtypes:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT name FROM okg.subtypes "
                "WHERE catalog_version_id = %s AND name = ANY(%s)",
                (int(catalog_version_id), subtypes),
            )
            known = {str(row[0]) for row in cur.fetchall()}
        unknown = [subtype for subtype in subtypes if subtype not in known]
        if unknown:
            raise SubstrateError(
                "linker_config on_miss names subtype(s) the catalog does not "
                f"hold: {', '.join(unknown)}",
                data={
                    "subtypes": unknown,
                    "catalog_version_id": int(catalog_version_id),
                },
                fix_hint=(
                    "set the route's on_miss.subtype to a catalog subtype, or "
                    "remove on_miss from the route"
                ),
            )
    return writes


def unique_endpoint_requests(
    requests: Iterable[LinkEndpointRequest],
) -> dict[str, LinkEndpointRequest]:
    """One request per endpoint id; the first one routed wins."""
    out: dict[str, LinkEndpointRequest] = {}
    for request in requests:
        out.setdefault(request.node_id, request)
    return out


def endpoints_to_write(
    checkout: Any,
    requests: Iterable[LinkEndpointRequest],
) -> list[LinkEndpointRequest]:
    """The requested endpoints whose id has no live node on the branch."""
    unique = unique_endpoint_requests(requests)
    ids = sorted(unique)
    live: set[str] = set()
    for start in range(0, len(ids), _CHUNK):
        rows = checkout.query(
            "SELECT node_id FROM okg.graph_nodes WHERE node_id = ANY(:ids)",
            {"ids": ids[start:start + _CHUNK]},
        )
        live.update(str(row["node_id"]) for row in rows)
    return [unique[node_id] for node_id in ids if node_id not in live]


def endpoints_to_retract(
    checkout: Any,
    *,
    keep: Sequence[str] = (),
) -> list[tuple[str, str]]:
    """``(node_id, subtype)`` of every endpoint declaration to withdraw.

    Two cases: a real record now owns the id (the declaration is all that is
    left of the endpoint), or no edge from a live record reaches the endpoint
    row (its citations are gone). An edge whose citing record was removed in
    this publish still sits on the branch until the gate prunes it, so the
    citing record must be live, not only the edge. ``keep`` holds ids this
    publish's extractor run cited: their new edges are not applied yet.
    """
    kept = set(keep)
    out: dict[str, str] = {}
    for row in checkout.query(
        """
        SELECT d.node_id, d.subtype
          FROM okg.graph_node_declarers d
          JOIN okg.graph_nodes n ON n.node_id = d.node_id
         WHERE d.source = :src
           AND n.source <> :src
        """,
        {"src": LINK_ENDPOINT_SOURCE},
    ):
        out[str(row["node_id"])] = str(row["subtype"])
    for row in checkout.query(
        """
        SELECT n.node_id, n.subtype
          FROM okg.graph_nodes n
         WHERE n.source = :src
           AND n.attrs->>:state_attr = :not_loaded
           AND NOT EXISTS (
                 SELECT 1
                   FROM okg.graph_edges e
                   JOIN okg.graph_nodes s ON s.node_id = e.src
                  WHERE e.dst = n.node_id
           )
        """,
        {
            "src": LINK_ENDPOINT_SOURCE,
            "state_attr": LOAD_STATE_ATTR,
            "not_loaded": NOT_LOADED,
        },
    ):
        node_id = str(row["node_id"])
        if node_id not in kept:
            out[node_id] = str(row["subtype"])
    return sorted(out.items())
