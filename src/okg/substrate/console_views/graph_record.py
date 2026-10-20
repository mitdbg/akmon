"""The console Graph view's local read: one record's neighbours, grouped.

PACT console-graph-views. ``compose_graph_record`` groups a record's
neighbours by (direction, relationship, neighbour type), lists the first
``GRAPH_PER_GROUP`` neighbours of each group by neighbour id, and names the
producer (the edge's ``source``) of each listed neighbour's first link. No
neighbour is chosen by degree.

Counts are distinct neighbour RECORDS, not links, as on Explore's Related tab.
A group counts each neighbour once. One record linked more than one way (a
document `contains` a chunk and the chunk is `member_of` it) sits in two
groups, so the groups do not add up to the record's neighbours; ``neighbours``
counts them per direction and in all.

Every group is reached. The per-(direction, relationship) link totals come
from the edge index; then each relationship's links, up to ``GRAPH_COUNT_CAP``
of them in (neighbour id, edge id) order, are grouped by neighbour type and
counted in the database. Every type among those links is a group, however
many links another type has. Only a relationship with more links than
``GRAPH_COUNT_CAP`` has uncounted links: its groups' counts are floors, and
the relationship says so with ``types_complete: false``, because a type seen
only past the cap cannot be named.

Policy: the centre record, every relationship and every neighbour pass the same
Nomos row filter the record reads use (``records._visible_record_row`` /
``_visible_nodes`` / ``filter_edge_rows_for_session``). A record the reader may
not see is refused as ``record_not_found``, the same answer as an absent one.
When the deployment evaluates the Nomos runtime, every count is counted over
what the reader may see (``count_basis = "visible_to_reader"``), as
``records._related_tally`` does: a total counted before the access rules would
tell the reader how many neighbours are kept from them. A neighbour counts when
the reader may see it and at least one link to it. Each group is decided up to
``GRAPH_POLICY_SCAN_CAP`` links; past that its count is a floor. A group or a
relationship in which the reader may see nothing is not named. Without the
runtime nothing is hidden and the counts are the published totals.

Under the runtime every link decided, and every neighbour it reaches, goes
through the per-row filter, so each counted row gets its own recorded policy
decision. That filter is the read's main cost (about 1 ms a row), which is why
a group stops at ``GRAPH_POLICY_SCAN_CAP`` links, and the whole read at
``GRAPH_CHECK_BUDGET`` checked links, shared evenly over its groups (each
checked link's neighbour is checked too, so up to twice that many decisions).
A group the budget cuts short is a floor, and so is its relationship and its
side, visible or not. A group the budget never reaches is not named: its side
says only that links went unchecked (``unchecked_links``), because a name read
before the access check could name what the reader may not see. ``capped_by``
says which caps made each side a floor, so every "+" can be explained.
"""
from __future__ import annotations

from typing import Any

from okg.substrate.searchable_text import without_producer_text
from okg.substrate.console_views._chronos import checkout
from okg.substrate.console_views.graph_views import GraphReadRefused
from okg.substrate.console_views.ontology_rollup_console import COUNT_BASIS
from okg.substrate.console_views.records import (
    RELATED_TALLY_CEILING,
    _EDGE_ROW_KEYS,
    _display_label,
    _visible_nodes,
    _visible_record_row,
)
from okg.substrate.mcp.session import Session
from okg.substrate.nomos.read_filter import filter_edge_rows_for_session
from okg.substrate.nomos.runtime_enforcement import deployment_requires_nomos_runtime

#: Neighbours listed per (direction, relationship, neighbour type) group.
GRAPH_PER_GROUP = 25
#: Links counted per (direction, relationship). Past it, the relationship's
#: counts are floors and a type seen only past it is not reached. 14,099 links
#: were grouped and counted in 53 ms on okg-workspace.
GRAPH_COUNT_CAP = 100_000
#: Links decided per (direction, relationship, neighbour type) when the Nomos
#: runtime can hide rows. Each decided link and its neighbour go through the
#: per-row filter (about 1 ms a row), so a group stops where the record page's
#: own visible tally stops (``records.RELATED_TALLY_CEILING``); past it the
#: group's count is a floor.
GRAPH_POLICY_SCAN_CAP = RELATED_TALLY_CEILING
#: Links one record read sends through the access check, over all its groups
#: (the per-row check is the read's cost: about 9 ms a row on okg-workspace at
#: a load average of 15-30, 2026-09-28). The budget is shared evenly across
#: the groups; a group it cuts short is a floor, and a side with a group it
#: never reached says so in ``unchecked_links``, with no name and no count.
GRAPH_CHECK_BUDGET = 400
#: What a record read's counts mean when the reader's policy can hide rows.
VISIBLE_COUNT_BASIS = "visible_to_reader"

_DIRECTIONS = ("in", "out")
# (the record's column, the neighbour's column, extra predicate). A self-loop
# is read once, outward.
_SIDES = {
    "out": ("src", "dst", ""),
    "in": ("dst", "src", " AND src <> dst"),
}

# One record's edges, counted per relationship from the edge index.
# derivation-read-lint: allow PACT console-graph-views one-record edge index count.
_COUNTS_SQL = """
SELECT 'out' AS dir, edge_type, count(*) AS n
  FROM okg.graph_edges WHERE src = :id GROUP BY edge_type
UNION ALL
SELECT 'in' AS dir, edge_type, count(*) AS n
  FROM okg.graph_edges WHERE dst = :id AND src <> dst GROUP BY edge_type
"""

# One relationship in one direction. Its links, up to {count_cap} in neighbour
# order, are grouped by neighbour type; every returned link carries its
# group's link count and distinct-neighbour count, and only the {selected}
# links are returned, with their attributes. The LEFT JOIN keeps a link whose
# neighbour is absent from the version, grouped under a null type.
# derivation-read-lint: allow PACT console-graph-views one-record grouped count over a capped read.
_RELATIONSHIP_SQL = """
WITH e AS MATERIALIZED (
  SELECT edge_id, {far} AS nbr
    FROM okg.graph_edges
   WHERE {near} = :id AND edge_type = :relationship{extra}
   ORDER BY {far}, edge_id
   LIMIT {count_cap}
),
r AS (
  SELECT e.edge_id, e.nbr, n.subtype AS nbr_subtype,
         count(*) OVER (PARTITION BY n.subtype) AS group_links,
         dense_rank() OVER (PARTITION BY n.subtype ORDER BY e.nbr) AS nbr_rank,
         row_number() OVER (PARTITION BY n.subtype ORDER BY e.nbr, e.edge_id) AS rn,
         row_number() OVER (PARTITION BY e.nbr ORDER BY e.edge_id) AS link_rank
    FROM e LEFT JOIN okg.graph_nodes n ON n.node_id = e.nbr
),
g AS (
  SELECT nbr_subtype, max(nbr_rank) AS group_neighbours,
         max(group_links) AS group_links
    FROM r GROUP BY nbr_subtype
)
SELECT s.nbr_subtype, s.nbr, s.rn, g.group_neighbours, g.group_links,
       ed.edge_id, ed.src, ed.dst, ed.edge_type, ed.attrs, ed.source,
       ed.source_record_id, ed.source_revision,
       n.attrs AS nbr_attrs, n.source AS nbr_source,
       n.source_record_id AS nbr_source_record_id,
       n.source_revision AS nbr_source_revision
  FROM r s
  JOIN g ON g.nbr_subtype IS NOT DISTINCT FROM s.nbr_subtype
  JOIN okg.graph_edges ed ON ed.edge_id = s.edge_id
  LEFT JOIN okg.graph_nodes n ON n.node_id = s.nbr
 WHERE {selected}
 ORDER BY s.nbr_subtype, s.rn
"""
# Distinct neighbour records per direction and in all, over the record's first
# {cap} links each way. Two links to one record, in one direction or both
# (a chunk `member_of` its document and the document `contains` it), are one
# neighbour, as on Explore's Related tab.
# derivation-read-lint: allow PACT console-graph-views one-record distinct neighbour count.
_NEIGHBOURS_SQL = """
WITH l AS MATERIALIZED (
  (SELECT 'out' AS dir, dst AS nbr FROM okg.graph_edges WHERE src = :id
    ORDER BY dst LIMIT {cap})
  UNION ALL
  (SELECT 'in' AS dir, src AS nbr FROM okg.graph_edges WHERE dst = :id AND src <> dst
    ORDER BY src LIMIT {cap})
)
SELECT dir, count(DISTINCT nbr) AS n FROM l GROUP BY dir
UNION ALL
SELECT 'all' AS dir, count(DISTINCT nbr) AS n FROM l
"""
# Without a policy: each listed neighbour's first link. Under one: every link
# the group decides.
_LISTED = "s.nbr_rank <= {per_group} AND s.link_rank = 1"
_DECIDED = "s.rn <= {scan_cap}"


def _neighbour_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "node_id": row["nbr"],
        "subtype": row["nbr_subtype"],
        "attrs": without_producer_text(row["nbr_attrs"]),
        "source": row["nbr_source"],
        "source_record_id": row["nbr_source_record_id"],
        "source_revision": row["nbr_source_revision"],
    }


def _visible_rows(
    conn: Any, session: Session, rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    """The rows whose edge AND neighbour the reader may see, and those neighbours."""
    if not rows:
        return [], {}
    visible_edges = {
        str(edge["edge_id"])
        for edge in filter_edge_rows_for_session(
            conn, session, [{key: row[key] for key in _EDGE_ROW_KEYS} for row in rows],
        )
    }
    candidates: dict[str, dict[str, Any]] = {}
    for row in rows:
        if str(row["edge_id"]) in visible_edges and row["nbr_subtype"] is not None:
            candidates.setdefault(str(row["nbr"]), _neighbour_row(row))
    neighbours = {
        str(node["node_id"]): node
        for node in _visible_nodes(conn, session, list(candidates.values()))
    }
    shown = [
        row for row in rows
        if str(row["edge_id"]) in visible_edges and str(row["nbr"]) in neighbours
    ]
    return shown, neighbours


def _group_key(row: dict[str, Any]) -> tuple[str, str, Any]:
    return (str(row["dir"]), str(row["edge_type"]), row["nbr_subtype"])


def _within_budget(
    rows: list[dict[str, Any]], budget: int,
) -> tuple[list[dict[str, Any]], set[tuple[str, str, Any]], dict[str, bool]]:
    """The rows the access check will decide, shared evenly over the groups.

    Returns the kept rows, the groups cut short (a group given no share at all
    among them), and per side whether some group was given no share.
    """
    by_group: dict[tuple[str, str, Any], list[dict[str, Any]]] = {}
    for row in rows:
        by_group.setdefault(_group_key(row), []).append(row)
    share: dict[tuple[str, str, Any], int] = {}
    left = max(0, int(budget))
    order = sorted(by_group, key=lambda k: (len(by_group[k]), k[0], k[1], str(k[2])))
    for i, key in enumerate(order):
        share[key] = min(len(by_group[key]), left // (len(order) - i))
        left -= share[key]
    kept = [row for key in order for row in by_group[key][:share[key]]]
    cut = {key for key in order if share[key] < len(by_group[key])}
    unchecked = {d: any(key[0] == d and not share[key] for key in order) for d in _DIRECTIONS}
    return kept, cut, unchecked


def _neighbour_counts(
    graph: Any,
    node_id: str,
    *,
    totals: dict[tuple[str, str], int],
    counted: list[dict[str, Any]],
    side_floor: dict[str, bool],
    policy_active: bool,
    count_cap: int,
) -> dict[str, dict[str, Any]]:
    """Distinct neighbour records per direction and in all, with floor flags.

    Without a policy they are counted in the database over each direction's
    first ``count_cap`` links. Under one they are the distinct neighbours the
    reader may see among the links decided, a floor wherever a group was.
    """
    if policy_active:
        names: dict[str, set[str]] = {"in": set(), "out": set()}
        for row in counted:
            names[str(row["dir"])].add(str(row["nbr"]))
        floor = {d: side_floor[d] for d in _DIRECTIONS}
        numbers = {d: len(names[d]) for d in _DIRECTIONS}
        numbers["all"] = len(names["in"] | names["out"])
    else:
        numbers = {d: 0 for d in (*_DIRECTIONS, "all")}
        for row in graph.query(_NEIGHBOURS_SQL.format(cap=int(count_cap)), {"id": node_id}):
            numbers[str(row["dir"])] = int(row["n"])
        links = {
            d: sum(n for (direction, _), n in totals.items() if direction == d)
            for d in _DIRECTIONS
        }
        floor = {d: links[d] > count_cap for d in _DIRECTIONS}
    floor["all"] = floor["in"] or floor["out"]
    return {
        d: {"count": numbers[d], "count_is_floor": floor[d]}
        for d in (*_DIRECTIONS, "all")
    }


def _read_relationship(
    graph: Any, node_id: str, direction: str, relationship: str, *, selected: str,
    count_cap: int,
) -> list[dict[str, Any]]:
    near, far, extra = _SIDES[direction]
    rows = graph.query(
        _RELATIONSHIP_SQL.format(
            near=near, far=far, extra=extra, count_cap=int(count_cap), selected=selected,
        ),
        {"id": node_id, "relationship": relationship},
    )
    return [{**row, "dir": direction} for row in rows]


def compose_graph_record(
    conn: Any,
    *,
    session: Session,
    deployment_name: str,
    node_id: str,
    per_group: int = GRAPH_PER_GROUP,
    scan_cap: int = GRAPH_POLICY_SCAN_CAP,
    count_cap: int = GRAPH_COUNT_CAP,
    check_budget: int | None = None,
) -> dict[str, Any]:
    """One record and its neighbours, grouped and bounded.

    ``count_cap`` bounds the links counted per relationship; ``scan_cap``
    bounds the links decided per group when the Nomos runtime can hide rows.
    The response's ``scan_cap`` is the cap this read's floors come from.
    """
    if check_budget is None:
        check_budget = GRAPH_CHECK_BUDGET
    graph = checkout(conn, session)
    centre = _visible_record_row(conn, session, graph, node_id)
    if centre is None:
        raise GraphReadRefused(
            404,
            "record_not_found",
            "This version has no record with that id that you may open.",
            generation_id=session.generation_id,
        )
    policy_active = deployment_requires_nomos_runtime(session.deployment_name)
    totals: dict[tuple[str, str], int] = {
        (str(row["dir"]), str(row["edge_type"])): int(row["n"])
        for row in graph.query(_COUNTS_SQL, {"id": node_id})
    }
    selected = (
        _DECIDED.format(scan_cap=int(scan_cap)) if policy_active
        else _LISTED.format(per_group=int(per_group))
    )
    rows: list[dict[str, Any]] = []
    for direction, relationship in sorted(totals):
        rows.extend(_read_relationship(
            graph, node_id, direction, relationship,
            selected=selected, count_cap=count_cap,
        ))
    # A relationship with more links than the count reads has links nobody
    # counted, and possibly types nobody saw.
    uncounted = {pair for pair, n in totals.items() if n > count_cap}
    # Sides whose count is a floor whatever the reader turns out to see: a
    # group with more links than one group's check, or links the read's
    # check budget never reached. Set from the raw link counts, so a group
    # whose checked links were all hidden still marks its side.
    # The caps behind each relationship's and each side's floor, recorded
    # from every group read, whether or not the reader sees any of it.
    rel_caps: dict[tuple[str, str], set[str]] = {}
    for pair in uncounted:
        rel_caps.setdefault(pair, set()).add("count_cap")
    budget_cut: set[tuple[str, str, Any]] = set()
    unchecked_links = {d: False for d in _DIRECTIONS}
    if policy_active:
        for row in rows:
            if int(row["group_links"]) > scan_cap:
                rel_caps.setdefault((str(row["dir"]), str(row["edge_type"])), set()).add("scan_cap")
        rows, budget_cut, unchecked_links = _within_budget(rows, check_budget)
        for key in budget_cut:
            rel_caps.setdefault(key[:2], set()).add("check_budget")
    side_caps: dict[str, set[str]] = {d: set() for d in _DIRECTIONS}
    for (d, _), caps in rel_caps.items():
        side_caps[d] |= caps
    side_floor = {d: bool(side_caps[d]) for d in _DIRECTIONS}
    if policy_active:
        # Every decided link and its neighbour pass the per-row filter, which
        # records each row's decision; only what the reader may see is counted.
        shown, neighbours = _visible_rows(conn, session, rows)
        counted = shown
    else:
        shown, neighbours = _visible_rows(conn, session, rows)
        # Without a policy nothing is hidden, and a link whose neighbour is
        # absent from this version is still a relationship: listed unlabelled.
        shown_ids = {str(row["edge_id"]) for row in shown}
        shown = counted = [
            row for row in rows
            if str(row["edge_id"]) in shown_ids or row["nbr_subtype"] is None
        ]
    listed_ids = {str(row["edge_id"]) for row in shown}
    groups: dict[tuple[str, str, Any], dict[str, Any]] = {}
    seen: dict[tuple[str, str, Any], set[str]] = {}
    for row in counted:
        key = _group_key(row)
        group = groups.get(key)
        if group is None:
            group = groups[key] = {
                "direction": key[0],
                "relationship": key[1],
                "type": key[2],
                # Without a policy, the group's distinct neighbours as counted
                # in the database; under one, those the reader may see.
                "count": 0 if policy_active else int(row["group_neighbours"]),
                "floor_reason": (
                    "count_cap" if key[:2] in uncounted
                    else "check_budget" if key in budget_cut
                    else "scan_cap" if policy_active and int(row["group_links"]) > scan_cap
                    else None
                ),
                "items": [],
            }
            group["count_is_floor"] = group["floor_reason"] is not None
            seen[key] = set()
        nbr = str(row["nbr"])
        if nbr in seen[key]:
            # A second link to a neighbour already counted.
            continue
        seen[key].add(nbr)
        if policy_active:
            group["count"] += 1
        if str(row["edge_id"]) in listed_ids and len(group["items"]) < per_group:
            neighbour = neighbours.get(nbr)
            group["items"].append({
                "id": nbr,
                "label": (
                    _display_label(nbr, str(key[2]), neighbour.get("attrs"))
                    if neighbour is not None
                    else None
                ),
                # The producer of the neighbour's first (visible) link.
                "producer": row["source"],
            })
    by_relationship: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for group in groups.values():
        by_relationship.setdefault(
            (group["direction"], group["relationship"]), [],
        ).append(group)
    relationships = [
        {
            "direction": direction,
            "relationship": relationship,
            # A neighbour has one type, so the groups' distinct counts add up.
            "count": sum(g["count"] for g in by_relationship[(direction, relationship)]),
            # A floor when any of its groups was capped or cut, including a
            # group the reader sees nothing of.
            "count_is_floor": bool(rel_caps.get((direction, relationship))) or any(
                g["count_is_floor"] for g in by_relationship[(direction, relationship)]
            ),
            "types_complete": (direction, relationship) not in uncounted,
        }
        for direction, relationship in sorted(totals)
        # Under a policy a relationship the reader sees nothing of is not named.
        if (direction, relationship) in by_relationship
    ]
    for r in relationships:
        side_floor[r["direction"]] = side_floor[r["direction"]] or r["count_is_floor"]
    neighbours_count = _neighbour_counts(
        graph, node_id, totals=totals, counted=counted, side_floor=side_floor,
        policy_active=policy_active, count_cap=count_cap,
    )
    return {
        "deployment": deployment_name,
        "generation_id": session.generation_id,
        "record": {
            "id": str(centre["node_id"]),
            "type": str(centre["subtype"]),
            "label": _display_label(
                str(centre["node_id"]), str(centre["subtype"]), centre.get("attrs"),
            ),
            "source": centre.get("source"),
        },
        "per_group": per_group,
        # The cap this read's floors come from.
        "scan_cap": scan_cap if policy_active else count_cap,
        "count_cap": count_cap,
        "count_basis": VISIBLE_COUNT_BASIS if policy_active else COUNT_BASIS,
        # Links per record read that go through the access check, and per
        # side whether some links were never checked (no name, no count).
        "check_budget": check_budget if policy_active else None,
        "unchecked_links": unchecked_links,
        # Which caps made each side a floor: "count_cap", "scan_cap",
        # "check_budget"; empty when the side's counts are exact.
        "capped_by": {d: sorted(side_caps[d]) for d in _DIRECTIONS},
        # A side is capped when some count on it is a floor, including a
        # group whose checked links were all hidden but which has more.
        "capped": dict(side_floor),
        "relationships": relationships,
        # Distinct neighbour records, which the relationship counts are not
        # when one record is linked more than one way.
        "neighbours": neighbours_count,
        "groups": sorted(
            groups.values(),
            key=lambda g: (g["direction"], -g["count"], g["relationship"], str(g["type"])),
        ),
    }


__all__ = [
    "GRAPH_CHECK_BUDGET",
    "GRAPH_COUNT_CAP",
    "GRAPH_PER_GROUP",
    "GRAPH_POLICY_SCAN_CAP",
    "VISIBLE_COUNT_BASIS",
    "compose_graph_record",
]
