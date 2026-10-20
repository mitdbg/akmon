"""Recency annotations and sibling surfacing, loudly absent where the graph
has nothing.

Per PACT `recover-passage-rollup-reads`.

Both affordances here have the same contract and the same failure mode to
avoid. An affordance that returns nothing WHERE STRUCTURE EXISTS is a bug; an
affordance that returns nothing BECAUSE THE STRUCTURE DOES NOT EXIST is a fact
about the graph. They are indistinguishable from an empty result, so neither is
allowed to be reported as an empty result: each says which of the two it is.

**Recency is corpus-relative, never wall-clock.** Measured on the benchmark
corpus this was written against, 42.0 % of `fireflies` records (4,271 of
10,173) are dated AFTER today and the corpus runs to 2029-12-11. A wall-clock
"is this recent?" test would call 42 % of a source implausible, and a
plausibility clamp would silently discard it. So the comparison is against the
corpus's own measured span, the span is reported, and a value outside it is
FLAGGED rather than clamped.

**No authority prior.** An unqualified "formal sources outrank working
documents" prior is FORBIDDEN here until it is measured against frozen gold,
because measured on this corpus it is backwards: a confluence "finalized
playbook" holds superseded numbers while a drive "working draft" holds the gold
ones. Nothing in this module ranks by source formality.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping, Optional, Sequence

import psycopg
from psycopg.rows import dict_row

from okg.substrate.mcp.session import Session
from okg.substrate.nomos.read_filter import (
    filter_edge_rows_for_session,
    filter_node_rows_for_session,
)
from okg.substrate.nomos.runtime_enforcement import (
    deployment_requires_nomos_runtime,
)


__all__ = [
    "POLICY_ENFORCED_NOT_MEASURED",
    "RECENCY_ATTRIBUTE",
    "RECENCY_DERIVATION",
    "recency_annotations",
    "sibling_disclosure",
    "sibling_relations",
]


#: The ONE attribute recency orders on, named rather than implied. The
#: requirement is explicit that an unnamed "recency" is not implementable and
#: that the derivation must be stated: this is the timestamp the SOURCE
#: attributes to the document's authorship, not the substrate's ingest time
#: (`parsed_at`, `observed_at`) and not `updated_at` (a revision stamp, which
#: on a snapshot corpus is the crawl, not the writing).
#:
#: There is deliberately NO fallback attribute. Falling back to `updated_at`
#: when `authored_at` is missing would order two pages by two different
#: quantities and report one word for both.
RECENCY_ATTRIBUTE = "authored_at"
RECENCY_DERIVATION = "source_declared_authorship_timestamp"

#: The span state, and each row's position basis, under Nomos runtime
#: enforcement: the span is not measured at all (issue #2760).
POLICY_ENFORCED_NOT_MEASURED = "policy_enforced_not_measured"

#: Rows the corpus-span measurement may read. The span is a min/max over an
#: unindexed JSONB key, so an unbounded scan would put a full-corpus aggregate
#: on the critical path of every annotated search. When the cap is reached the
#: block says so (`corpus_span_scanned: "sample"`) instead of presenting a
#: partial span as the corpus's.
_CORPUS_SPAN_ROW_CAP = 5000

#: Raw sibling edges a policy-enforced request may inspect while assembling
#: the first bounded readable page. If that prefix cannot prove completeness,
#: the response reports bounded uncertainty instead of a false empty state.
_SIBLING_POLICY_SCAN_CAP = 500

#: Rows counted per declared sibling relation when read policy is off.  The
#: production probe is diagnostic metadata, not permission to aggregate the
#: whole graph.  A saturated count is reported as a bounded lower bound via
#: ``graph_edge_counts_complete``.
_SIBLING_GRAPH_COUNT_CAP = 500


def _attribute_value(row: Mapping[str, Any], key: str) -> Any:
    attrs = row.get("attrs")
    if not isinstance(attrs, dict):
        return None
    value = attrs.get(key)
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    return value


def _corpus_span(
    conn: psycopg.Connection,
    *,
    session: Session,
    segment: str,
    branch_point_value: Any,
    subtypes: Optional[Sequence[str]],
) -> dict[str, Any]:
    """The corpus's own measured min/max of the recency attribute, bounded.

    Returns the span plus how it was measured. Never a wall-clock reference:
    "recent" on this corpus means "late within what this corpus contains".

    **Not measured under Nomos runtime enforcement** (issue #2760, decided by
    the operator). A span over the rows a reader may see has to send the whole
    sample through the per-record read filter: on okg-workspace that was 52 s
    of a 64 s console search. A cheaper span (probing from the ends) discloses
    how many hidden rows lie beyond the reader's endpoints and, with overlays
    and redaction, which values the ordering came from. So under enforcement
    (mode `audit` or `enforce`) the span is reported as
    `policy_enforced_not_measured`: no read-filter call, no endpoints, no row
    count and no sample size, whatever the reader may see.

    With enforcement off the read filter returns every row it is given, writes
    no decision and costs nothing per row, so the whole sample is measured
    exactly as before. The filter is still called there: it refuses a
    multi-user read that carries no subject, and a test may stand in for it.
    """
    if deployment_requires_nomos_runtime(session.deployment_name):
        return {
            "corpus_span_lo": None,
            "corpus_span_hi": None,
            "corpus_span_rows": None,
            "corpus_span_scanned": POLICY_ENFORCED_NOT_MEASURED,
            "corpus_span_ordering": POLICY_ENFORCED_NOT_MEASURED,
            "corpus_span_subtypes": list(subtypes) if subtypes else None,
        }
    params: dict[str, Any] = {
        "bp": branch_point_value,
        "key": RECENCY_ATTRIBUTE,
        "cap": _CORPUS_SPAN_ROW_CAP + 1,
    }
    where = [
        "deleted = FALSE",
        "live_lo <= %(bp)s",
        "%(bp)s < live_hi",
        "attrs ? %(key)s",
    ]
    if subtypes:
        where.append("subtype = ANY(%(subtypes)s)")
        params["subtypes"] = list(subtypes)
    # Only the recency value is read, not the whole attrs document. Each row is
    # one row version, identified by the segment's primary key
    # `(node_id, live_lo)`: the schema does not forbid two live versions of one
    # node at a branch point, and each counts on its own, as before.
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            SELECT node_id, live_lo, subtype,
                   jsonb_build_object(%(key)s::text, attrs -> %(key)s::text) AS attrs
              FROM {segment}
             WHERE {" AND ".join(where)}
             ORDER BY node_id, live_lo
             LIMIT %(cap)s
            """,
            params,
        )
        raw = cur.fetchall()
    readable = filter_node_rows_for_session(conn, session, raw)
    values = [
        str(value)
        for row in readable
        if (value := _attribute_value(row, RECENCY_ATTRIBUTE)) is not None
    ]
    span_lo, span_hi, ordering = _timestamp_span(values)
    return {
        "corpus_span_lo": span_lo,
        "corpus_span_hi": span_hi,
        "corpus_span_rows": len(values),
        "corpus_span_scanned": "sample" if len(raw) > _CORPUS_SPAN_ROW_CAP else "all",
        "corpus_span_ordering": ordering,
        "corpus_span_subtypes": list(subtypes) if subtypes else None,
    }


def recency_annotations(
    conn: psycopg.Connection,
    page: Sequence[Mapping[str, Any]],
    *,
    session: Session,
    segment: str,
    branch_point_value: Any,
    subtypes: Optional[Sequence[str]] = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """`(per_node_annotation, envelope_block)` for one page of rows.

    Annotates ONLY the rows that carry `RECENCY_ATTRIBUTE`; it never derives a
    date from anything else. Where the graph carries nothing, the envelope
    block says which attribute is missing and over how many rows, so an
    un-annotated page is a reported state rather than an absence the caller has
    to infer. When the attribute lands on the corpus, the same code path
    annotates — no change required.

    The corpus span is measured ONLY when at least one page row carries a
    value: with nothing to place, there is nothing to place it against, and
    paying for a corpus aggregate to report "no coverage" would put the cost on
    exactly the deployments that get no benefit.
    """
    carriers = {
        str(row["node_id"]): _attribute_value(row, RECENCY_ATTRIBUTE) for row in page
    }
    carriers = {
        node_id: value for node_id, value in carriers.items() if value is not None
    }
    block: dict[str, Any] = {
        "attribute": RECENCY_ATTRIBUTE,
        "derivation": RECENCY_DERIVATION,
        "basis": "corpus_relative",
        # Stated as a value, not left to be inferred from the absence of a
        # wall-clock field: the requirement forbids a wall-clock comparison and
        # a reader must be able to CHECK that it did not happen.
        "wall_clock_compared": False,
        "authority_prior_applied": False,
        "rows_on_page": len(page),
        "rows_annotated": len(carriers),
    }
    if not carriers:
        block["annotated"] = False
        block["absent_reason"] = (
            f"no row on this page carries `attrs.{RECENCY_ATTRIBUTE}`, so no "
            f"recency ordering is available. This is a property of the graph, "
            f"not of the query: the annotation is computed from that attribute "
            f"alone and is never derived from ingest time, from a date found "
            f"in the body text, or from source formality."
        )
        block["corpus_span_lo"] = None
        block["corpus_span_hi"] = None
        block["corpus_span_rows"] = 0
        block["corpus_span_scanned"] = "none"
        block["corpus_span_ordering"] = "empty"
        return {}, block

    span = _corpus_span(
        conn,
        session=session,
        segment=segment,
        branch_point_value=branch_point_value,
        subtypes=subtypes,
    )
    block.update(span)
    block["annotated"] = True
    lo, hi = span["corpus_span_lo"], span["corpus_span_hi"]

    annotations: dict[str, dict[str, Any]] = {}
    for node_id, value in carriers.items():
        text = str(value)
        # Outside the MEASURED span is reported, never clamped and never
        # dropped: on a corpus 42 % of whose records are future-dated, a
        # plausibility clamp deletes real data, and a sampled span legitimately
        # excludes real values.
        position, position_basis = _span_position(text, lo, hi)
        if span["corpus_span_ordering"] == POLICY_ENFORCED_NOT_MEASURED:
            # No span exists to place the value in; the value itself may parse.
            position_basis = POLICY_ENFORCED_NOT_MEASURED
        outside = bool(
            position is not None and (position < 0.0 or position > 1.0)
        )
        annotations[node_id] = {
            "attribute": RECENCY_ATTRIBUTE,
            "derivation": RECENCY_DERIVATION,
            "value": value,
            "corpus_span_position": position,
            "corpus_span_position_basis": position_basis,
            "outside_corpus_span": outside,
        }
    return annotations, block


def _parse_timestamp(value: Any) -> Optional[datetime]:
    """Parse an ISO-8601 timestamp, or return None. Never guesses a format."""
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _timestamp_span(values: Sequence[str]) -> tuple[Any, Any, str]:
    """Return comparable raw endpoints without lexically ordering instants."""
    if not values:
        return None, None, "empty"
    parsed = [(value, _parse_timestamp(value)) for value in values]
    if any(instant is None for _, instant in parsed):
        return None, None, "unparseable_timestamp"
    typed = [(value, instant) for value, instant in parsed if instant is not None]
    awareness = {instant.utcoffset() is not None for _, instant in typed}
    if len(awareness) != 1:
        return None, None, "mixed_timezone_awareness"
    low = min(typed, key=lambda item: (item[1], item[0]))
    high = max(typed, key=lambda item: (item[1], item[0]))
    return low[0], high[0], "measured_instants"


def _span_position(
    value: str,
    lo: Any,
    hi: Any,
) -> tuple[Optional[float], str]:
    """Where `value` sits between the corpus's own endpoints, in [0, 1].

    NOT clamped: a value outside the measured span yields a position outside
    [0, 1] and `outside_corpus_span` flags it, because the span may be a
    bounded sample and because a corpus legitimately runs past today.

    Returns `(None, reason)` rather than a fabricated number when the values do
    not parse as timestamps or when the span is a single instant — a position
    invented from an unparseable string would be a number the caller could not
    tell apart from a measured one.
    """
    low, high, here = (
        _parse_timestamp(lo),
        _parse_timestamp(hi),
        _parse_timestamp(value),
    )
    if low is None or high is None or here is None:
        return None, "unparseable_timestamp"
    try:
        width = (high - low).total_seconds()
    except TypeError:
        # ISO-8601 permits both offset-naive and offset-aware forms. Mixing
        # them has no defined instant comparison without guessing a timezone,
        # so disclose that state instead of crashing or assuming UTC.
        return None, "mixed_timezone_awareness"
    if width <= 0:
        return None, "degenerate_span"
    try:
        position = (here - low).total_seconds() / width
    except TypeError:
        return None, "mixed_timezone_awareness"
    return position, "measured"


def recency_disclosure(
    conn: psycopg.Connection,
    page: Sequence[Mapping[str, Any]],
    *,
    session: Session,
    segment: str,
    branch_point_value: Any,
    subtypes: Optional[Sequence[str]] = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """Alias of `recency_annotations`, named for the envelope's point of view."""
    return recency_annotations(
        conn,
        page,
        session=session,
        segment=segment,
        branch_point_value=branch_point_value,
        subtypes=subtypes,
    )


def sibling_relations(
    conn: psycopg.Connection,
    *,
    catalog_version_id: int,
    subtype: str,
) -> list[dict[str, str]]:
    """The declared SELF-relations of a subtype, from the pinned catalog.

    A sibling is a peer of the same kind, so a sibling relation is a narrowing
    whose parent and child subtypes are BOTH this subtype — `document
    near_duplicate_of document`, `document supersedes document`, `document
    references document`. This is a structural rule read off the catalog, not a
    list of blessed edge-type names: a name list would do nothing on a graph
    that calls its versioned-pair edge something else, and would claim
    structure on a graph that declares the name for an unrelated pair.

    Containment is excluded by the same rule without a special case: `document
    contains document_chunk` and `document_chunk member_of document` relate two
    DIFFERENT subtypes, so a document's own passages are never reported as its
    siblings.
    """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT edge_type FROM okg.narrowings
             WHERE catalog_version_id = %(cv)s
               AND parent_subtype = %(subtype)s
               AND child_subtype = %(subtype)s
             ORDER BY edge_type
            """,
            {"cv": catalog_version_id, "subtype": subtype},
        )
        return [
            {
                "edge_type": str(row["edge_type"]),
                "parent_subtype": subtype,
                "child_subtype": subtype,
            }
            for row in cur.fetchall()
        ]


def sibling_disclosure(
    conn: psycopg.Connection,
    *,
    session: Session,
    node_id: str,
    subtype: str,
    catalog_version_id: int,
    node_segment: str,
    edge_segment: str,
    branch_point_value: Any,
    limit: int = 25,
) -> dict[str, Any]:
    """A node's sibling neighbours, with the empty cases told apart.

    Three states, never collapsed into one empty list:

      `no_declared_relations`  the pinned catalog declares no self-relation for
                               this subtype — this graph has no such structure,
                               and no ingest or enricher run will produce one
                               until the ontology declares it.
      `declared_but_empty`     relations are declared and the readable graph
                               holds no such edge for this node at the pin.
                               Bounded per-narrowing counts say whether the graph
                               has produced any matching structure elsewhere.
      `authority_bounded_unknown`
                               active read authority returned no sibling and
                               therefore forbids a graph-empty claim.
      `neighbours`             siblings, per relation.
    """
    relations = sibling_relations(
        conn,
        catalog_version_id=catalog_version_id,
        subtype=subtype,
    )
    if not relations:
        return {
            "node_id": node_id,
            "subtype": subtype,
            "state": "no_declared_relations",
            "declared_relations": [],
            "neighbours": [],
            "reason": (
                f"the pinned catalog (version {catalog_version_id}) declares "
                f"no narrowing whose parent and child subtypes are both "
                f"{subtype!r}, so this graph carries no sibling structure for "
                f"it — reference, near-duplicate and versioned-pair edges are "
                f"absent by ONTOLOGY, not by content. Nothing to surface, and "
                f"no query can make one appear."
            ),
        }

    edge_types = [rel["edge_type"] for rel in relations]
    policy_enforced = deployment_requires_nomos_runtime(
        session.deployment_name,
    )
    raw_edge_budget = _SIBLING_POLICY_SCAN_CAP if policy_enforced else limit
    params = {
        "node_id": node_id,
        "edge_types": edge_types,
        "bp": branch_point_value,
        "raw_limit": raw_edge_budget + 1,
    }
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            SELECT e.edge_id, e.src, e.dst, e.edge_type, e.attrs, e.source,
                   e.source_record_id, e.source_revision,
                   CASE WHEN e.src = %(node_id)s THEN e.dst ELSE e.src END
                       AS neighbour_id,
                   CASE WHEN e.src = %(node_id)s THEN 'out' ELSE 'in' END
                       AS direction
              FROM {edge_segment} AS e
              JOIN {node_segment} AS neighbour
                ON neighbour.node_id = CASE
                     WHEN e.src = %(node_id)s THEN e.dst ELSE e.src
                   END
               AND neighbour.subtype = %(subtype)s
               AND neighbour.deleted = FALSE
               AND neighbour.live_lo <= %(bp)s AND %(bp)s < neighbour.live_hi
             WHERE e.deleted = FALSE
               AND e.live_lo <= %(bp)s AND %(bp)s < e.live_hi
               AND e.edge_type = ANY(%(edge_types)s)
               AND (e.src = %(node_id)s OR e.dst = %(node_id)s)
             ORDER BY e.edge_type, neighbour_id
             LIMIT %(raw_limit)s
            """,
            {**params, "subtype": subtype},
        )
        raw_neighbour_edges = cur.fetchall()
        raw_edge_budget_reached = len(raw_neighbour_edges) > raw_edge_budget
        raw_neighbour_edges = raw_neighbour_edges[:raw_edge_budget]
        # Whether the GRAPH holds any such edge at all. Without this a node
        # with no siblings looks identical to a graph where nothing has ever
        # emitted the edge — the first is a fact about the node, the second is
        # a gap in the pipeline, and only the second is a defect.
        graph_counts: Optional[dict[str, int]] = None
        graph_counts_complete: Optional[dict[str, bool]] = None
        if not policy_enforced:
            graph_counts = {}
            graph_counts_complete = {}
            for edge_type in edge_types:
                cur.execute(
                    f"""
                    SELECT e.edge_id
                      FROM {edge_segment} AS e
                      JOIN {node_segment} AS src_node
                        ON src_node.node_id = e.src
                       AND src_node.subtype = %(subtype)s
                       AND src_node.deleted = FALSE
                       AND src_node.live_lo <= %(bp)s
                       AND %(bp)s < src_node.live_hi
                      JOIN {node_segment} AS dst_node
                        ON dst_node.node_id = e.dst
                       AND dst_node.subtype = %(subtype)s
                       AND dst_node.deleted = FALSE
                       AND dst_node.live_lo <= %(bp)s
                       AND %(bp)s < dst_node.live_hi
                     WHERE e.deleted = FALSE
                       AND e.live_lo <= %(bp)s AND %(bp)s < e.live_hi
                       AND e.edge_type = %(edge_type)s
                     ORDER BY e.edge_id
                     LIMIT %(count_limit)s
                    """,
                    {
                        "bp": branch_point_value,
                        "subtype": subtype,
                        "edge_type": edge_type,
                        "count_limit": _SIBLING_GRAPH_COUNT_CAP + 1,
                    },
                )
                counted = cur.fetchall()
                graph_counts[edge_type] = min(
                    len(counted),
                    _SIBLING_GRAPH_COUNT_CAP,
                )
                graph_counts_complete[edge_type] = (
                    len(counted) <= _SIBLING_GRAPH_COUNT_CAP
                )

    readable_edges = filter_edge_rows_for_session(
        conn,
        session,
        raw_neighbour_edges,
    )
    neighbour_ids = sorted({str(row["neighbour_id"]) for row in readable_edges})
    readable_neighbour_ids: set[str] = set()
    if neighbour_ids:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT node_id, subtype, attrs, text_hash, source,
                       source_record_id, source_revision
                  FROM {node_segment}
                 WHERE node_id = ANY(%(node_ids)s)
                   AND subtype = %(subtype)s
                   AND deleted = FALSE
                   AND live_lo <= %(bp)s AND %(bp)s < live_hi
                 ORDER BY node_id
                """,
                {
                    "node_ids": neighbour_ids,
                    "subtype": subtype,
                    "bp": branch_point_value,
                },
            )
            raw_neighbour_nodes = cur.fetchall()
        readable_neighbour_ids = {
            str(row["node_id"])
            for row in filter_node_rows_for_session(
                conn,
                session,
                raw_neighbour_nodes,
            )
        }
    all_neighbours = [
        {
            "edge_type": str(row["edge_type"]),
            "neighbour_id": str(row["neighbour_id"]),
            "direction": str(row["direction"]),
        }
        for row in readable_edges
        if str(row["neighbour_id"]) in readable_neighbour_ids
    ]
    readable_page_saturated = len(all_neighbours) > limit
    selection_complete = (
        not raw_edge_budget_reached or readable_page_saturated
    )
    neighbours = all_neighbours[:limit]
    authority_empty = policy_enforced and not neighbours
    if authority_empty:
        # Reporting exact graph emptiness here would distinguish "no edge"
        # from "edge or node hidden by policy" and turn the affordance into a
        # structure oracle.  No readable row means no authority to make that
        # distinction, even when the bounded raw prefix was exhaustive.
        selection_complete = False
    state = (
        "neighbours"
        if neighbours
        else "authority_bounded_unknown"
        if authority_empty
        else "declared_but_empty"
        if selection_complete
        else "bounded_unknown"
    )

    return {
        "node_id": node_id,
        "subtype": subtype,
        "state": state,
        "declared_relations": relations,
        "neighbours": neighbours,
        "graph_edge_counts": (
            {edge_type: graph_counts.get(edge_type, 0) for edge_type in edge_types}
            if graph_counts is not None
            else None
        ),
        "graph_edge_counts_complete": (
            {
                edge_type: graph_counts_complete.get(edge_type, False)
                for edge_type in edge_types
            }
            if graph_counts_complete is not None
            else None
        ),
        "graph_edge_count_cap": (
            _SIBLING_GRAPH_COUNT_CAP if graph_counts is not None else None
        ),
        "limit": limit,
        "returned_count": len(neighbours),
        "truncated": readable_page_saturated or not selection_complete,
        "selection_complete": selection_complete,
        "reason": (
            None
            if neighbours and selection_complete
            else (
                "active read authority returned no readable sibling; graph "
                "emptiness and hidden structure are intentionally not "
                "distinguished"
            )
            if authority_empty
            else (
                "the bounded policy scan could not prove that every readable "
                "sibling candidate was reached; no empty or complete claim is "
                "made"
            )
            if not selection_complete
            else (
                f"{subtype!r} declares sibling relation(s) "
                f"{edge_types}, and none reaches {node_id!r} at this pin. "
                f"Bounded, same-subtype `graph_edge_counts` and their "
                f"completeness flags separate the two readings when Nomos "
                f"runtime enforcement is off. Under enforcement they are "
                f"omitted rather than leaking hidden graph structure."
            )
        ),
    }
