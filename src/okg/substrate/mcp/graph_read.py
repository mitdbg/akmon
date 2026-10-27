"""The graph read path: effective attributes from Chronos rows plus overlays.

PACT lane-s-storage-redesign-s2, design section 2; since PACT
lane-s-storage-cutover the only read path. A page read (filter, map, lookup,
references) reads the Chronos graph rows visible at the pin's branch point
and merges that generation's overlays (``okg.attribute_effective_attrs``, the
same per-generation rows Nomos reads) at read time. No MCP reader reads the
three materialized copies of the effective attributes.

The page is **ids first** (design 2.3):

* graph ids: visible rows that pass the whole compiled predicate on their
  graph values, narrowed by the caller's candidate set (declared expression
  index, path prefix), and with **no** overlay at the generation on any key
  the predicate reads (``NOT EXISTS``, so the branch streams in id order and
  stops at the page limit). For those targets the effective value of every
  predicate key is the graph value, so this branch is exact (design 2.2,
  rule 1). A predicate with an equality on the whole ``attrs`` object
  admits only targets with no overlay at all (rule 4).
* overlay ids: targets with an overlay on any predicate key (any key for a
  whole-attrs predicate). When the predicate reads exactly one key, the
  top-level conjuncts that read only that key also run on the overlay value
  (rule 3); otherwise no value test (rule 2).
* the two disjoint id lists merge in id order (edges by their numeric id),
  each id fetches its graph row, the overlay is merged (``graph attrs ||
  overlay attrs``, reserved keys excluded) and the caller's exact compiled
  predicate decides the row.

``raw_attrs`` is the graph attrs when the target has an overlay row at the
generation, else NULL: exactly what today's read returns (design 1.3), so the
row dicts handed to the Nomos read filter are the same on both paths.

Every statement built here is sent unprepared by the caller
(``bm25_node_search.execute_with_custom_plan``). Nothing here sets a planner,
parallel or plan-cache option.
"""
from __future__ import annotations

import json
import math
from collections.abc import Callable, Mapping
from typing import Any

from okg.substrate.attributes import (
    ATTRIBUTE_EFFECTIVE_TABLE,
    RESERVED_EFFECTIVE_ATTR_KEYS,
    require_overlay_generation_marker,
)

__all__ = [
    "GRAPH_RELATION",
    "count_relation_sql",
    "graph_relation_sql",
    "id_stream_branches",
    "order_sql",
    "page_relation_sql",
    "planning_predicate",
    "predicate_reads_whole_attrs",
    "scope_relation_sql",
]

#: The alias of the graph-value relation the predicate is compiled over for
#: the graph-id branch.
GRAPH_RELATION = "graph_rows"
#: The alias of the overlay-value relation of the single-key pushdown.
_OVERLAY_VALUE_RELATION = "overlay_value"
#: Default of ``id_stream_sql(graph_predicate=...)``: the whole predicate.
_WHOLE_PREDICATE = object()


def require_graph_pin(conn: Any, session: Any) -> None:
    """Refuse, by name, a pin the graph path cannot answer (design 5.3),
    before its first statement: a generation retired by history compaction
    (``GenerationCompacted``; the session's read lease already refuses one,
    this also covers the lease-less direct callers), then a generation with
    no overlay marker (``overlay_generation_unmaterialized``). Any older pin
    whose Chronos rows and marker exist is served, however old: the graph
    path keeps no retention window of its own.

    The marker refusal is restated for a reader (PACT
    lane-s-storage-cutover): with the graph path the only read path, no
    other path answers such a pin. Since issue #4049 every publish,
    fast-forward, history compaction and rollover root writes a marker,
    so the cause is a generation minted before that fix or before
    migration 0044, or a bare checkpoint outside those operations (a
    bench branch); the next publish on the branch writes its own. The
    overlay ranges are never read here (only the per-generation overlay
    rows and the marker), so a generation whose ranges were not synced is
    read like any other."""
    from okg.substrate.errors import SubstrateError
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.history_read_guard import raise_if_compacted

    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    raise_if_compacted(
        store.context.db, store._generation_checkpoint_id(session.generation_id),
        deployment_name=session.deployment_name,
    )
    try:
        require_overlay_generation_marker(conn, session.generation_id)
    except SubstrateError as exc:
        if getattr(exc, "code", None) != UNMARKED_PIN_CODE:
            raise
        raise unmarked_pin_refusal(exc) from exc


#: The refusal of a pin with no overlay marker.
UNMARKED_PIN_CODE = "overlay_generation_unmaterialized"


def unmarked_pin_refusal(cause: Any) -> Any:
    """``overlay_generation_unmaterialized`` restated: it names the cause
    and the reads that serve, and keeps the marker check's data."""
    from okg.substrate.errors import SubstrateError

    data = dict(getattr(cause, "data", None) or {})
    if data.get("marker_table_present") is False:
        message = (
            "this database has no overlay marker table (migration 0044 is not "
            "applied), so no generation's effective attributes can be read"
        )
        hint = "an operator runs `okg migrate --apply`, which also writes the markers"
        reason = "marker_table_missing"
    else:
        message = (
            "the pinned generation has no overlay marker, so its effective "
            "attributes cannot be read (the graph rows plus overlays are the "
            "only read path): its overlay step never ran. Every publish, "
            "fast-forward, history compaction and rollover now writes one; a "
            "generation minted by an older release, or a bare checkpoint "
            "outside them (a bench branch), has none"
        )
        hint = (
            "pin an earlier generation that has its marker (see `okg "
            "generation list --deployment <name>`), or publish this branch "
            "again: a publish that succeeds writes the marker, and a read "
            "with no generation_id then pins it. That publish can itself be "
            "refused; its refusal names its own fix"
        )
        reason = "overlay_step_not_run"
    err = SubstrateError(message, data={**data, "cause": reason}, fix_hint=hint)
    err.code = UNMARKED_PIN_CODE
    return err


def predicate_attr_keys(value: Any) -> set[str]:
    """The attribute keys a predicate (or a list of ``{"field": ...}``)
    reads: ``attrs.<key>`` fields and the keys of a ``contains`` on
    ``attrs``. Keys holding NUL name no stored attribute and are left out."""
    keys: set[str] = set()
    if isinstance(value, dict):
        field = value.get("field")
        if isinstance(field, str) and field.startswith("attrs.") and field[6:]:
            keys.add(field[6:])
        if value.get("op") == "contains" and value.get("field") == "attrs":
            contained = value.get("value")
            if isinstance(contained, dict):
                keys.update(str(k) for k in contained)
        for child in value.values():
            keys |= predicate_attr_keys(child)
    elif isinstance(value, list):
        for child in value:
            keys |= predicate_attr_keys(child)
    return {k for k in keys if "\x00" not in k}


def planning_predicate(predicate: Any) -> Any:
    """The predicate the graph path plans with: a top-level ``and`` whose
    plain ``and`` clauses (no key but ``op`` and a non-empty ``clauses``
    list) are replaced by their own conjuncts, recursively; anything else
    as it is. A new object when anything was spliced; ``predicate`` itself
    is never changed.

    The candidate planner (``attr_filter_source.pushable_clauses``) and the
    conjunct helpers here (:func:`two_phase_key`, :func:`scope_predicate`,
    :func:`predicate_subtype`, :func:`_single_key_conjuncts`) read
    top-level conjuncts only. The filter tool sends ``and(subtype,
    and(a, b))`` (``server._compose_scope_predicate``), so neither declared
    index narrowed such a page (Krama head10, h_and_2keys: 3.8-4.6 s for 51
    rows walking every visible row). ``and`` is associative, so the rows
    are the same. Only for planning and the graph id stream: the page's
    compiled predicate, its fingerprint and every echo keep the caller's
    predicate."""
    if not (
        isinstance(predicate, dict) and predicate.get("op") == "and"
        and isinstance(predicate.get("clauses"), list)
    ):
        return predicate
    clauses = predicate["clauses"]
    spliced = _spliced_conjuncts(clauses)
    if len(spliced) == len(clauses) and all(a is b for a, b in zip(spliced, clauses)):
        return predicate
    return {**predicate, "clauses": spliced}


def _plain_and(clause: Any) -> bool:
    return (
        isinstance(clause, dict) and set(clause) == {"op", "clauses"}
        and clause["op"] == "and" and isinstance(clause["clauses"], list)
        and bool(clause["clauses"])
    )


def _spliced_conjuncts(clauses: list[Any]) -> list[Any]:
    out: list[Any] = []
    for clause in clauses:
        if _plain_and(clause):
            out.extend(_spliced_conjuncts(clause["clauses"]))
        else:
            out.append(clause)
    return out


def predicate_reads_whole_attrs(predicate: Any) -> bool:
    """True when a leaf compares the whole ``attrs`` object (``eq`` or ``in``
    on the field ``attrs``): such a predicate has no finite key set. An
    ``exists`` on ``attrs`` lowers to TRUE and a ``contains`` on ``attrs`` to
    per-key equalities, so neither counts (operator_sql.effective_attr_lowerer)."""
    if isinstance(predicate, dict):
        if predicate.get("op") in ("eq", "in") and predicate.get("field") == "attrs":
            return True
        return any(predicate_reads_whole_attrs(v) for v in predicate.values())
    if isinstance(predicate, list):
        return any(predicate_reads_whole_attrs(v) for v in predicate)
    return False


#: The scope column as the broad-scope count exposes it: the same text
#: (``x || ''`` is ``x``, NULL stays NULL) as an expression no index of the
#: segment is built on (see :func:`broad_scope`).
_SCOPE_UNINDEXED = {"nodes": "(subtype || ''::text)", "edges": "(edge_type || ''::text)"}

#: The visibility bounds as the broad-scope split aggregate writes them:
#: ``x + 0`` is ``x`` (NULL stays NULL), an expression no index of the
#: segment is built on. Every visibility index of the segment carries
#: ``live_lo, live_hi``, and ``node_source_node_visible`` leads with
#: ``source``, one value at Krama (pg_stats correlation 1.0): the planner
#: costs a walk of it as a sequential read although it visits the heap and
#: the TOAST in node-id order (correlation -0.18). See
#: :func:`split_aggregate_sql`.
_VISIBILITY_UNINDEXED = ("(live_lo + 0)", "(live_hi + 0)")


def graph_relation_sql(
    kind: str, segment: str, *, scope_unindexed: bool = False,
    visibility_unindexed: bool = False,
) -> str:
    """The visible Chronos rows at ``%(graph_bp)s`` with the effective
    relation's column names. Edges also carry ``edge_num``, their bigint id,
    which orders and joins them. ``scope_unindexed`` exposes the subtype
    (edge type) through :data:`_SCOPE_UNINDEXED`: the same values, which no
    index can serve. ``visibility_unindexed`` writes the visibility test
    over :data:`_VISIBILITY_UNINDEXED` (the same truth), which no index can
    serve either. With both, no index can narrow the scope or the
    visibility test; the planner may still walk an index whose predicate is
    only ``deleted = FALSE`` (or serve another attribute condition from a
    declared-attribute index), so the sequential scan is what it picks on
    a broad scope, not the only path it has."""
    lo, hi = _VISIBILITY_UNINDEXED if visibility_unindexed else ("live_lo", "live_hi")
    visible = f"deleted = FALSE AND {lo} <= %(graph_bp)s AND %(graph_bp)s < {hi}"
    if kind == "nodes":
        subtype = (
            f"{_SCOPE_UNINDEXED['nodes']} AS subtype" if scope_unindexed else "subtype"
        )
        return (
            f"(SELECT node_id AS target_id, {subtype}, NULL::text AS src, "
            "NULL::text AS dst, NULL::text AS edge_type, attrs, "
            f"text_hash AS raw_text_hash FROM {segment} WHERE {visible})"
        )
    edge_type = (
        f"{_SCOPE_UNINDEXED['edges']} AS edge_type" if scope_unindexed else "edge_type"
    )
    return (
        "(SELECT edge_id::text AS target_id, edge_id AS edge_num, "
        f"NULL::text AS subtype, src, dst, {edge_type}, attrs, "
        f"NULL::text AS raw_text_hash FROM {segment} WHERE {visible})"
    )


def _single_key_conjuncts(predicate: Any, key: str) -> list[Any]:
    """The top-level conjuncts that read only ``attrs.<key>``: no stable
    column, no other key, no whole-attrs leaf (design 2.2, rule 3)."""
    if not isinstance(predicate, dict):
        return []
    conjuncts = (
        list(predicate.get("clauses") or [])
        if predicate.get("op") == "and" else [predicate]
    )
    wanted = f"attrs.{key}"

    def only_key(node: Any) -> bool:
        if isinstance(node, list):
            return all(only_key(v) for v in node)
        if not isinstance(node, dict):
            return True
        if "field" in node and node.get("field") != wanted:
            return False
        children = [v for k, v in node.items() if k in ("clauses", "clause")]
        return all(only_key(c) for c in children)

    return [c for c in conjuncts if isinstance(c, dict) and "op" in c and only_key(c)]


def id_stream_sql(**stream: Any) -> str:
    """The merged, id-ordered candidate id stream (design 2.3): one SELECT
    yielding ``id`` (and ``num`` for edges), ordered by it. Keyword arguments
    are those of :func:`id_stream_branches`."""
    graph_ids, overlay_ids, order_key = id_stream_branches(**stream)
    if overlay_ids is None:
        return graph_ids
    return f"({graph_ids}) UNION ALL ({overlay_ids}) ORDER BY {order_key}"


def id_stream_branches(
    *,
    kind: str,
    segment: str,
    predicate: Any,
    attr_keys: list[str],
    params: dict[str, Any],
    compile_predicate: Callable[[Any, dict[str, Any], str], Any],
    candidates_sql: str = "",
    candidates_as_array: bool = False,
    member_ids: bool = False,
    members_sql: str = "",
    keyset_param: str | None = None,
    after_param: str | None = None,
    gin_params: tuple[str, str] | None = None,
    graph_predicate: Any = _WHOLE_PREDICATE,
    value_tests: bool = True,
    ordered: bool = True,
    broad_scope: bool = False,
) -> tuple[str, str | None, str]:
    """The two disjoint branches of the candidate id stream (design 2.3):
    ``(graph_ids, overlay_ids, order_key)``, each a SELECT yielding ``id``
    (and ``num`` for edges), ``overlay_ids`` None when no overlay can change
    the predicate. ``ordered=False`` leaves out each branch's ``ORDER BY``
    (the split count of :func:`count_relation_sql` reads them unordered).
    ``broad_scope`` (:func:`broad_scope`) applies to that unordered graph-id
    branch only: its scan reads the subtype (edge type) through an
    expression no index serves, so it is a sequential scan of the segment;
    an ordered branch reads in id order through the scope's index and
    ignores it.

    ``members_sql`` (issue #3666, the page's ``member_sql``) is a one-column
    query of target ids; both branches keep only ids it returns, as the
    materialized page restricts its base rows to that set. ``keyset_param``
    (issue #3666 step 3b, the page's ``after_id``) names a parameter both
    branches restrict to ids after it in the page's default order: a node's
    id as text, an edge's id as a number. Both are restrictions of the set
    itself, so the split count reads them too.

    ``after_param`` names a parameter both branches restrict to ids after
    (keyset continuation, design 2.4). ``gin_params`` names the (key, value)
    parameters of the GIN containment pre-filter, added to the graph-id
    branch only (the GIN form of phase 2, and the split count of the same
    equality, :func:`count_relation_sql`).
    ``graph_predicate`` replaces the predicate the graph-id branch tests
    (the two-phase read passes only the scope conjuncts, so its stream is
    the candidates, not the matches, and the cut bounds the work).
    ``value_tests=False`` (the reader's view decides, Nomos slice 7a) tests
    no attribute value in either branch: a stored or overlay value must not
    decide which rows are scanned, so the stream is every visible row (and
    every target overlaid on a predicate key) and the page's reader-view
    WHERE decides each fetched row, as the materialized path does with its
    candidate sets off.

    ``params`` must already hold ``graph_bp``, ``tk``, ``overlay_gen``;
    ``rr_member_ids`` when ``member_ids``; the candidate set's own
    parameters. The graph-branch predicate is compiled here through
    ``compile_predicate(predicate, params, relation)``; its parameters are
    bound into ``params``.
    """
    edges = kind == "edges"
    graph_rows = graph_relation_sql(
        kind, segment, scope_unindexed=broad_scope and not ordered,
    )
    whole_attrs = predicate_reads_whole_attrs(predicate)
    keyed = bool(attr_keys) or whole_attrs
    params["graph_overlay_keys"] = list(attr_keys)
    params.setdefault("overlay_reserved", sorted(RESERVED_EFFECTIVE_ATTR_KEYS))
    key_test = (
        "o.attr_key <> ALL(%(overlay_reserved)s::text[])"
        if whole_attrs
        else "o.attr_key = ANY(%(graph_overlay_keys)s::text[])"
    )
    overlay_scope = (
        "o.target_kind = %(tk)s AND o.generation_id = %(overlay_gen)s "
        "AND o.namespace = 'default'"
    )

    graph_where = [compile_predicate(
        predicate if graph_predicate is _WHOLE_PREDICATE else graph_predicate,
        params, GRAPH_RELATION,
    )] if value_tests else ["TRUE"]
    if gin_params is not None:
        key_param, value_param = gin_params
        graph_where.append(
            f"{GRAPH_RELATION}.attrs @> jsonb_build_object("
            f"%({key_param})s::text, %({value_param})s::jsonb)"
        )
    # An `IN` candidate set on an ordered node stream is read as a join on
    # the distinct candidate ids sorted by id: the declared expression index
    # yields candidates in value order, and with `IN` the planner probed
    # every candidate (9,086 at Krama 10%), then sorted, before the page's
    # LIMIT could stop it; sorted first, the probes stop at the page.
    # `x IN (S)` is the join with DISTINCT S.
    candidate_join = bool(candidates_sql) and not candidates_as_array \
        and ordered and not edges
    if candidates_sql and not candidate_join:
        graph_where.append(
            f"{GRAPH_RELATION}.target_id = ANY(ARRAY({candidates_sql}))"
            if candidates_as_array
            else f"{GRAPH_RELATION}.target_id IN ({candidates_sql})"
        )
    if member_ids:
        graph_where.append(
            f"{GRAPH_RELATION}.target_id = ANY(%(rr_member_ids)s::text[])"
        )
    if members_sql:
        graph_where.append(f"{GRAPH_RELATION}.target_id IN ({members_sql})")
    if keyset_param is not None:
        graph_where.append(
            f"{GRAPH_RELATION}.edge_num > %({keyset_param})s::bigint" if edges
            else f"{GRAPH_RELATION}.target_id > %({keyset_param})s"
        )
    if after_param is not None:
        graph_where.append(f"{GRAPH_RELATION}.target_id > %({after_param})s")
    overlaid_ids = (
        f"SELECT o.target_id FROM {ATTRIBUTE_EFFECTIVE_TABLE} o "
        f"WHERE {overlay_scope} AND {key_test}"
    )
    # How the graph-id branch leaves out overlaid targets. Ordered (a page):
    # a correlated NOT EXISTS, a primary-key probe per row that keeps the id
    # order. Unordered (the split count): a hashed
    # subplan of the generation's overlay ids, which does not depend on the
    # overlay table's row estimate for a new generation (about one row; with
    # NOT EXISTS the planner picked a nested-loop anti join comparing every
    # graph row with every overlay row: 36M comparisons, 9.9 s at Krama
    # 10%). With a candidate set the scan is fenced (OFFSET 0) and the
    # subplan tests its output: inside a candidate-driven nested loop the
    # planner charges the subplan once per probe and picks a full parallel
    # scan instead (0.79 s against 0.34 s at Krama 10%). `target_id` is NOT
    # NULL on both sides, so NOT IN has no null case.
    anti = (
        "exists" if keyed and ordered
        else "fenced" if keyed and candidates_sql
        else "hashed" if keyed
        else None
    )
    if anti == "exists":
        graph_where.append(
            f"NOT EXISTS (SELECT 1 FROM {ATTRIBUTE_EFFECTIVE_TABLE} o "
            f"WHERE {overlay_scope} AND o.target_id = {GRAPH_RELATION}.target_id "
            f"AND {key_test})"
        )
    elif anti == "hashed":
        graph_where.append(f"{GRAPH_RELATION}.target_id NOT IN ({overlaid_ids})")
    id_columns = (
        f"{GRAPH_RELATION}.target_id AS id, {GRAPH_RELATION}.edge_num AS num"
        if edges else f"{GRAPH_RELATION}.target_id AS id"
    )
    order_key = "num" if edges else "id"
    order_tail = f" ORDER BY {order_key}" if ordered else ""
    graph_from = f"{graph_rows} AS {GRAPH_RELATION}"
    if candidate_join:
        graph_from = (
            "(SELECT DISTINCT candidate_ids.target_id FROM "
            f"({candidates_sql}) AS candidate_ids(target_id) ORDER BY 1) AS candidates "
            f"JOIN {graph_from} ON {GRAPH_RELATION}.target_id = candidates.target_id"
        )
    graph_ids = (
        f"SELECT {id_columns} FROM {graph_from} "
        f"WHERE {' AND '.join(f'({w})' for w in graph_where)}{order_tail}"
    )
    if anti == "fenced":
        graph_ids = (
            f"SELECT graph_scan.* FROM ({graph_ids} OFFSET 0) AS graph_scan "
            f"WHERE graph_scan.id NOT IN ({overlaid_ids})"
        )
    overlay_ids: str | None = None
    if keyed:
        single = len(attr_keys) == 1 and not whole_attrs
        overlay_where = [overlay_scope, key_test]
        lateral = ""
        if single and value_tests:
            conjuncts = _single_key_conjuncts(predicate, attr_keys[0])
            if conjuncts:
                pushed = (
                    conjuncts[0] if len(conjuncts) == 1
                    else {"op": "and", "clauses": conjuncts}
                )
                lateral = (
                    " CROSS JOIN LATERAL (SELECT jsonb_build_object(o.attr_key, "
                    f"o.value) AS attrs) AS {_OVERLAY_VALUE_RELATION}"
                )
                overlay_where.append(
                    compile_predicate(pushed, params, _OVERLAY_VALUE_RELATION)
                )
        if member_ids:
            overlay_where.append("o.target_id = ANY(%(rr_member_ids)s::text[])")
        if members_sql:
            overlay_where.append(f"o.target_id IN ({members_sql})")
        if keyset_param is not None:
            # The cast is guarded: a WHERE clause may be evaluated before the
            # `target_kind` test, and a node overlay's id is not a number.
            overlay_where.append(
                f"(CASE WHEN o.target_id ~ '^-?[0-9]+$' THEN o.target_id::bigint END)"
                f" > %({keyset_param})s::bigint" if edges
                else f"o.target_id > %({keyset_param})s"
            )
        if after_param is not None:
            overlay_where.append(f"o.target_id > %({after_param})s")
        overlay_columns = (
            "o.target_id AS id, o.target_id::bigint AS num" if edges
            else "o.target_id AS id"
        )
        overlay_ids = (
            f"SELECT {'' if single else 'DISTINCT '}{overlay_columns} "
            f"FROM {ATTRIBUTE_EFFECTIVE_TABLE} AS o{lateral} "
            f"WHERE {' AND '.join(f'({w})' for w in overlay_where)}{order_tail}"
        )
    return graph_ids, overlay_ids, order_key


def fetch_relation_sql(ids_from: str, *, kind: str, segment: str, relation: str) -> str:
    """The FROM item ``(...) AS <relation>`` that fetches each id of
    ``ids_from`` (a parenthesized id stream or a CTE name, aliased ``ids``),
    merges its overlays and exposes the effective relation's columns
    (``target_kind``, ``namespace``, ``target_id``, ``subtype``, ``src``,
    ``dst``, ``edge_type``, ``attrs``, ``raw_attrs``, ``raw_text_hash``, and
    for edges ``edge_num``)."""
    edges = kind == "edges"
    graph_rows = graph_relation_sql(kind, segment)
    join = "fetched.edge_num = ids.num" if edges else "fetched.target_id = ids.id"
    edge_num = ", fetched.edge_num" if edges else ""
    kind_literal = "'edge'" if edges else "'node'"
    return (
        f"(SELECT {kind_literal}::text AS target_kind, 'default'::text AS namespace, "
        "fetched.target_id, fetched.subtype, fetched.src, fetched.dst, "
        "fetched.edge_type, "
        "fetched.attrs || COALESCE(ov.attrs, '{}'::jsonb) AS attrs, "
        "CASE WHEN ov.attrs IS NULL THEN NULL ELSE fetched.attrs END AS raw_attrs, "
        f"fetched.raw_text_hash{edge_num} "
        f"FROM {ids_from} AS ids "
        f"JOIN {graph_rows} AS fetched ON {join} "
        "LEFT JOIN LATERAL (SELECT jsonb_object_agg(attr_key, value) AS attrs "
        f"FROM {ATTRIBUTE_EFFECTIVE_TABLE} "
        "WHERE target_kind = %(tk)s AND target_id = fetched.target_id "
        "AND generation_id = %(overlay_gen)s AND namespace = 'default' "
        "AND attr_key <> ALL(%(overlay_reserved)s::text[])) AS ov ON TRUE"
        f") AS {relation}"
    )


def page_relation_sql(*, relation: str, **stream: Any) -> str:
    """The FROM item of a single-statement graph-path page: the id stream of
    :func:`id_stream_sql` (same keyword arguments) fetched and merged by
    :func:`fetch_relation_sql`."""
    ids = id_stream_sql(**stream)
    return fetch_relation_sql(
        f"({ids})", kind=stream["kind"], segment=stream["segment"], relation=relation,
    )


def scope_relation_sql(*, relation: str, **stream: Any) -> str:
    """The FROM item of a read over every row of a set, with no page and no
    order (the stored date forms of a date condition, PACT
    lane-s-storage-cutover): the two id branches of
    :func:`id_stream_branches` (same keyword arguments) read unordered,
    fetched and merged by :func:`fetch_relation_sql`. The caller's WHERE
    over ``relation`` decides each row, as on the page."""
    graph_ids, overlay_ids, _order_key = id_stream_branches(**stream, ordered=False)
    ids = graph_ids if overlay_ids is None else f"({graph_ids}) UNION ALL ({overlay_ids})"
    return fetch_relation_sql(
        f"({ids})", kind=stream["kind"], segment=stream["segment"], relation=relation,
    )


def count_relation_sql(*, relation: str, page_where: str, **stream: Any) -> str:
    """``FROM (...) AS counted``: the rows of the single-statement page
    (:func:`page_relation_sql` with the same stream arguments, filtered by
    ``page_where``), for the bounded count, read as two disjoint branches.

    The graph-id branch is counted as it streams, with no fetch, no overlay
    merge and no recheck: its targets have no overlay on any key the
    predicate reads (on any key for a whole-attrs predicate), so their
    effective value of every such key is their graph value and the graph-id
    branch's own test of the whole predicate is the page's exact predicate
    (design 2.2, rule 1). Only the overlay-id branch is fetched, merged and
    decided by ``page_where``. Neither branch is ordered: a count needs no
    order, and an order makes the planner sort the whole graph-id branch.

    Each branch is a ``MATERIALIZED`` CTE. PostgreSQL plans a CTE for all of
    its rows, so the bounded count's ``LIMIT`` does not turn the scan into a
    one-process fast-start plan sized by a misestimate (an ``or`` of two
    rare values was estimated at 238K rows, so a ``LIMIT 100001`` looked
    like an early stop: 3.65 s single-process against 0.94 s parallel at
    Krama 10%); the CTE is still read lazily, so the ``LIMIT`` still stops
    the scan once the cap is passed.

    Only for a stream whose graph-id branch tests the whole predicate
    (``value_tests`` true, no ``graph_predicate``) and a ``page_where`` that
    is that predicate over ``relation`` (plus the stable kind and namespace
    literals and the stream's own ``member_ids``, ``members_sql`` and
    ``keyset_param`` restrictions); the caller keeps the page's own
    FROM/WHERE under the reader's view.

    ``gin_params`` (the :func:`two_phase_key` equality's key and value)
    adds the GIN containment pre-filter to the graph-id branch, as the
    page's GIN form does. That branch still tests the whole predicate, so
    the pre-filter can only leave out rows, and every row it leaves out
    fails the predicate: the branch holds targets with no overlay on the
    one key, the predicate has the top-level conjunct
    ``COALESCE((attrs -> key) = value::jsonb, FALSE)`` with a non-null
    scalar value, and a graph value equal to a scalar under ``jsonb =`` is
    a scalar of the same type and value, which ``attrs @> {key: value}``
    holds (numbers compare as numbers both ways: 1 and 1.0). Without it the
    count of an equality on an undeclared key walked every visible row of
    the scope (Krama head10, b_undecl_rare: 4.3-4.9 s for 636 rows)."""
    if stream.get("value_tests", True) is False:
        raise ValueError("the split count needs an id stream that tests values")
    if stream.get("graph_predicate", _WHOLE_PREDICATE) is not _WHOLE_PREDICATE:
        raise ValueError("the split count needs the whole predicate on the graph ids")
    if stream.get("after_param") is not None:
        raise ValueError("the split count reads the whole stream")
    graph_ids, overlay_ids, _order_key = id_stream_branches(**stream, ordered=False)
    ctes = [f"counted_graph AS MATERIALIZED (SELECT 1 AS one FROM ({graph_ids}) AS graph_ids)"]
    reads = ["SELECT one FROM counted_graph"]
    if overlay_ids is not None:
        fetched = fetch_relation_sql(
            f"({overlay_ids})", kind=stream["kind"], segment=stream["segment"],
            relation=relation,
        )
        ctes.append(
            f"counted_overlay AS MATERIALIZED (SELECT 1 AS one FROM {fetched} "
            f"WHERE {page_where})"
        )
        reads.append("SELECT one FROM counted_overlay")
    return (
        f"FROM (WITH {', '.join(ctes)} {' UNION ALL '.join(reads)}) AS counted"
    )


def order_sql(relation: str, kind: str) -> str:
    """The default page order: node id (text), or edge id as a number."""
    return f" ORDER BY {relation}.{'edge_num' if kind == 'edges' else 'target_id'} ASC"


# --------------------------------------------------------------------------
# Two-phase read for an undeclared-key equality (design 2.4, gin2 A)
# --------------------------------------------------------------------------

#: Phase 1 cuts the merged id stream at this many ids (parent 1.4.1: the
#: wasted work on a short page grows with it; 2,000 measured best).
TWO_PHASE_CUT = 2000
#: Measured cost rates (parent design 1.4.1, step W, Krama copy): keyset
#: continuation reads about 26 us per candidate id; the GIN form costs about
#: 18 us per matching row. They set the threshold M for each read.
CONTINUATION_US_PER_ID = 26.0
GIN_US_PER_MATCH = 18.0
#: Continuation chunks before the GIN form takes over.
CONTINUATION_CHUNK_CAP = 3


#: The split count of a two-phase-key equality reads the GIN containment
#: (see :func:`gin_count_wanted`) only when phase 1 found at most this share
#: of its cut matching.
GIN_COUNT_MAX_PHASE1_SHARE = 0.01


def gin_count_wanted(report: Mapping[str, Any]) -> bool:
    """Whether the split count after a two-phase page (``report`` is
    :func:`run_two_phase`'s record) narrows its graph-id branch by the GIN
    containment. Only when phase 1 read a whole cut (``phase1_ids == cut``),
    did not fill the page (``m < target``, so ``m`` is every match in the
    cut, not a cap) and found at most ``GIN_COUNT_MAX_PHASE1_SHARE`` of the
    cut matching. Phase 1's cut is then a sample of the scope in id order
    that puts the value at about 1% of the rows or less.

    The containment pays per matching row and its bitmap reads them all
    before the count's cap can stop it; the walk pays per row of the scope
    and stops at the cap. Krama head10: a value on 636 rows counted in
    0.15 s through the containment against 4.3-4.9 s walking every visible
    row (b_undecl_rare); a value on every row of the subtype (b_undecl_all,
    its count stopped at the 100,001-row cap) took 3.35 s through the
    containment against 1.7-1.85 s walking, and its first count was cut by
    the 5 s budget, so the share bounds it. Below 1% of the rows the
    containment reads under about 3,100 matches of the 314,642-row scope.
    No report (no two-phase page), a short stream (the walk is short) or a
    filled phase 1 (the value may be common): no containment."""
    try:
        cut, ids, m, target = (int(report[k]) for k in ("cut", "phase1_ids", "m", "target"))
    except (KeyError, TypeError, ValueError):
        return False
    return ids >= cut > 0 and m < target and m <= GIN_COUNT_MAX_PHASE1_SHARE * cut


def two_phase_key(
    predicate: Any, *, attr_keys: list[str], declared: Mapping[str, str],
) -> tuple[str, Any] | None:
    """``(key, value)`` when the page takes the two-phase read: the predicate
    reads exactly one attribute key, that key is not declared, and a
    top-level conjunct is an equality on it with a non-null scalar (string,
    finite number, boolean). An equality with null never takes it (it also
    matches a missing key, which containment cannot express)."""
    if len(attr_keys) != 1 or predicate_reads_whole_attrs(predicate):
        return None
    key = attr_keys[0]
    if key in declared or not isinstance(predicate, dict):
        return None
    conjuncts = (
        list(predicate.get("clauses") or [])
        if predicate.get("op") == "and" else [predicate]
    )
    for clause in conjuncts:
        if not isinstance(clause, dict) or clause.get("op") != "eq":
            continue
        if clause.get("field") != f"attrs.{key}":
            continue
        value = clause.get("value")
        if isinstance(value, bool) or isinstance(value, int):
            return key, value
        if isinstance(value, float) and math.isfinite(value):
            return key, value
        if isinstance(value, str) and "\x00" not in value:
            return key, value
    return None


def _reads_attrs(node: Any) -> bool:
    if isinstance(node, dict):
        field = node.get("field")
        if isinstance(field, str) and (field == "attrs" or field.startswith("attrs.")):
            return True
        return any(_reads_attrs(v) for v in node.values())
    if isinstance(node, list):
        return any(_reads_attrs(v) for v in node)
    return False


def scope_predicate(predicate: Any) -> Any:
    """The top-level conjuncts that read no attribute (the scope: subtype,
    ids, text hash), or None. The two-phase stream's graph-id branch tests
    only these, so it yields candidates in id order; the exact predicate
    decides each fetched row."""
    if not isinstance(predicate, dict):
        return None
    conjuncts = (
        list(predicate.get("clauses") or [])
        if predicate.get("op") == "and" else [predicate]
    )
    kept = [c for c in conjuncts if not _reads_attrs(c)]
    if not kept:
        return None
    return kept[0] if len(kept) == 1 else {"op": "and", "clauses": kept}


def predicate_subtype(predicate: Any, field: str = "subtype") -> str | None:
    """The subtype (or, with ``field``, the edge type) a top-level conjunct
    pins, if any."""
    if not isinstance(predicate, dict):
        return None
    conjuncts = (
        list(predicate.get("clauses") or [])
        if predicate.get("op") == "and" else [predicate]
    )
    for clause in conjuncts:
        if (
            isinstance(clause, dict) and clause.get("op") == "eq"
            and clause.get("field") == field and isinstance(clause.get("value"), str)
        ):
            return str(clause["value"])
    return None


#: Measured cost rates (Krama 10%, 1.07M overlay rows over 11 generations)
#: that choose between ids first and the merged scope read (see
#: :func:`small_scope`). Merging one visible scope row (its overlay
#: aggregate and the predicate) costs about 11 us: 1,506 ``in_community``
#: edges took 17 ms to count and 12 ms for a 51-row page, against 22 ms and
#: 27 ms ids first. Reading one generation's overlay ids (what ids first
#: does) walks every generation's overlay rows of the target kind, because
#: no index leads with the generation: 70-115 ms, about 0.093 us per row of
#: the whole overlay table.
MERGED_US_PER_SCOPE_ROW = 11.0
OVERLAY_SCAN_US_PER_ROW = 0.093


def small_scope(cur: Any, *, kind: str, segment: str, predicate: Any) -> dict[str, Any] | None:
    """The estimates, when a page should read the merged scope instead of
    ids first, else None.

    Taken when a top-level conjunct pins the subtype (nodes) or edge type
    (edges) and merging every row of that scope is expected to cost no more
    than reading the generation's overlay ids:
    ``MERGED_US_PER_SCOPE_ROW x scope rows <= OVERLAY_SCAN_US_PER_ROW x
    overlay rows``, both row counts from the planner's statistics
    (``pg_class.reltuples`` and the value's ``pg_stats`` frequency). A table
    never analyzed gives no estimate: ids first. Both forms answer the same
    rows; this only picks the cheaper one."""
    column = "subtype" if kind == "nodes" else "edge_type"
    value = predicate_subtype(predicate, column)
    if value is None:
        return None
    scope_rows = _column_row_estimate(cur, segment, value, column=column)
    overlay_rows = _relation_rows(cur, ATTRIBUTE_EFFECTIVE_TABLE)
    if scope_rows is None or overlay_rows is None:
        return None
    if MERGED_US_PER_SCOPE_ROW * scope_rows > OVERLAY_SCAN_US_PER_ROW * overlay_rows:
        return None
    return {"scope_rows": scope_rows, "overlay_rows": overlay_rows}


def broad_scope(cur: Any, *, kind: str, segment: str, predicate: Any) -> dict[str, Any] | None:
    """The estimates, when the split count (and, since the aggregate fix,
    the graph branch of a keyed split aggregate not scoped to a result
    reference's ids, and its each() facts) should scan the
    segment sequentially rather than through the scope's index, else None.

    Taken when a top-level conjunct pins the subtype (nodes) or edge type
    (edges) and the scope's estimated rows are at least the segment's heap
    pages (``pg_class.relpages``): an index scan of the scope then reads
    about every heap page anyway, one at a time and in one process, while a
    sequential scan reads each page once and the planner may split it
    across workers. Measured at Krama (h_or_2keys, 320K of 1.17M nodes):
    the planner chose the one-process index scan on the 1% copy (3.7 s for
    the counted branch) and a parallel sequential scan on the 10% copy
    (1.0-1.8 s) for the same statement, as its statistics moved. Exposing
    the scope column through an expression no index is built on removes
    the index path; the values, and so the rows, are the same. A table
    never analyzed gives no estimate: None."""
    column = "subtype" if kind == "nodes" else "edge_type"
    value = predicate_subtype(predicate, column)
    if value is None:
        return None
    scope_rows = _column_row_estimate(cur, segment, value, column=column)
    cur.execute(
        "SELECT c.relpages FROM pg_class c WHERE c.oid = to_regclass(%s)", (segment,),
    )
    row = cur.fetchone()
    pages = None if row is None else (row["relpages"] if isinstance(row, Mapping) else row[0])
    if scope_rows is None or pages is None or int(pages) <= 0:
        return None
    if scope_rows < int(pages):
        return None
    return {"scope_rows": scope_rows, "segment_pages": int(pages)}


def _relation_rows(cur: Any, relation: str) -> float | None:
    """``pg_class.reltuples`` of ``relation``; None when never analyzed."""
    cur.execute(
        "SELECT c.reltuples FROM pg_class c WHERE c.oid = to_regclass(%s)", (relation,),
    )
    row = cur.fetchone()
    if row is None:
        return None
    value = row["reltuples"] if isinstance(row, Mapping) else row[0]
    if value is None or float(value) < 0:
        return None
    return float(value)


def subtype_row_estimate(cur: Any, segment: str, subtype: str | None) -> float | None:
    """The planner's estimate of the visible rows of ``subtype`` in the node
    table: ``pg_class.reltuples`` times the subtype's most-common-value
    frequency from ``pg_stats``, else ``reltuples`` over the number of
    distinct subtypes. None when the table or its subtype column was never
    analyzed."""
    return _column_row_estimate(cur, segment, subtype, column="subtype")


def _column_row_estimate(
    cur: Any, segment: str, value: str | None, *, column: str,
) -> float | None:
    """:func:`subtype_row_estimate` for the value of any column (``edge_type``
    in the edge table)."""
    cur.execute(
        "SELECT n.nspname, c.relname, c.reltuples FROM pg_class c "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE c.oid = to_regclass(%s)",
        (segment,),
    )
    row = cur.fetchone()
    if row is None:
        return None
    schema, table, reltuples = (row[k] for k in ("nspname", "relname", "reltuples")) \
        if isinstance(row, Mapping) else row
    if reltuples is None or float(reltuples) < 0:
        return None
    total = float(reltuples)
    if value is None:
        return total
    cur.execute(
        "SELECT most_common_vals::text::text[] AS vals, most_common_freqs AS freqs, "
        "n_distinct FROM pg_stats WHERE schemaname = %s AND tablename = %s "
        "AND attname = %s",
        (schema, table, column),
    )
    stats = cur.fetchone()
    if stats is None:
        # reltuples is also set by VACUUM or CREATE INDEX without ANALYZE:
        # with no column statistics there is no estimate of the value.
        return None
    vals, freqs, n_distinct = (
        (stats["vals"], stats["freqs"], stats["n_distinct"])
        if isinstance(stats, Mapping) else stats
    )
    if vals and freqs and value in vals:
        return total * float(freqs[list(vals).index(value)])
    if n_distinct:
        distinct = float(n_distinct) if n_distinct > 0 else -float(n_distinct) * total
        if distinct > 0:
            return total / distinct
    return total


def continuation_threshold(
    *, rows_estimate: float | None, cut: int, target: int,
) -> int:
    """M: the smallest match count m among the first ``cut`` ids at which
    keyset continuation is expected to cost no more than the GIN form.

    Continuation costs about ``CONTINUATION_US_PER_ID x cut x (target - m) /
    m`` (the chunks still needed at density m / cut); the GIN form about
    ``GIN_US_PER_MATCH x (m / cut) x N`` (the matches it must read). With no
    row estimate (never analyzed) M is 1: any match prefers continuation and
    none prefers GIN. Above ``target`` no m qualifies (always GIN)."""
    if rows_estimate is None:
        return 1
    for m in range(1, target + 1):
        continuation = CONTINUATION_US_PER_ID * cut * (target - m) / m
        gin = GIN_US_PER_MATCH * (m / cut) * rows_estimate
        if continuation <= gin:
            return m
    return target + 1


def phase_statement_sql(
    *, stream_sql: str, kind: str, segment: str, relation: str,
    select_list: str, page_where: str,
) -> str:
    """One phase-1 or continuation statement: the id stream cut at
    ``%(tp_cut)s`` ids BEFORE the fetch, the overlay merge and the exact
    predicate; returns the number of ids in the cut stream and its last id
    (``n_ids``, ``last_id``) beside up to ``%(tp_target)s`` matching rows in
    id order (one all-NULL row when none match)."""
    fetch = fetch_relation_sql("cut", kind=kind, segment=segment, relation=relation)
    return (
        "WITH cut AS MATERIALIZED (SELECT id FROM ("
        f"{stream_sql}) AS stream ORDER BY id LIMIT %(tp_cut)s) "
        "SELECT tp.n_ids, tp.last_id, pg.* FROM "
        "(SELECT count(*) AS n_ids, max(id) AS last_id FROM cut) AS tp "
        f"LEFT JOIN LATERAL (SELECT {select_list} FROM {fetch} WHERE {page_where} "
        f"ORDER BY {relation}.target_id LIMIT %(tp_target)s) AS pg ON TRUE"
    )


def gin_statement_sql(
    *, stream_sql: str, kind: str, segment: str, relation: str,
    select_list: str, page_where: str,
) -> str:
    """The GIN form of phase 2: the stream whose graph-id branch carries the
    containment pre-filter, fetched, merged and decided by the exact
    predicate, first ``%(tp_target)s`` rows in id order."""
    fetch = fetch_relation_sql(f"({stream_sql})", kind=kind, segment=segment,
                               relation=relation)
    return (
        f"SELECT {select_list} FROM {fetch} WHERE {page_where} "
        f"ORDER BY {relation}.target_id LIMIT %(tp_target)s"
    )


def run_two_phase(
    *,
    run: Callable[[str, dict[str, Any]], list[dict[str, Any]]],
    phase_sql: Callable[[bool], str],
    gin_sql: Callable[[], str],
    params: dict[str, Any],
    target: int,
    threshold: int,
    cut: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Run the two-phase read (design 2.4). ``run(sql, params)`` executes one
    statement unprepared and returns its rows as dicts. Returns the first
    ``target`` matching records in id order and a record of what ran."""
    record: dict[str, Any] = {
        "cut": cut, "target": target, "threshold": threshold,
        "forms": ["phase1"], "statements": 1,
    }

    def phase(after: Any, need: int) -> tuple[list[dict[str, Any]], int, Any]:
        bound = {**params, "tp_cut": cut, "tp_target": need}
        if after is not None:
            bound["tp_after"] = after
        rows = run(phase_sql(after is not None), bound)
        if not rows:
            raise RuntimeError("a two-phase statement returned no row")
        n_ids, last = int(rows[0]["n_ids"]), rows[0]["last_id"]
        return [r for r in rows if r.get("target_id") is not None], n_ids, last

    matches, n_ids, last = phase(None, target)
    record["m"] = len(matches)
    record["phase1_ids"] = n_ids
    if len(matches) >= target or n_ids < cut:
        return matches[:target], record
    if len(matches) >= threshold:
        record["forms"].append("continuation")
        for _ in range(CONTINUATION_CHUNK_CAP):
            chunk, n_ids, after = phase(last, target - len(matches))
            record["statements"] += 1
            matches.extend(chunk)
            if len(matches) >= target or n_ids < cut:
                return matches[:target], record
            last = after
        record["continuation_capped"] = True
    record["forms"].append("gin")
    record["statements"] += 1
    rows = run(gin_sql(), {**params, "tp_target": target})
    return rows[:target], record


# --------------------------------------------------------------------------
# Row-merged relation (expand) and the split aggregate (design 2.5, 2.6)
# --------------------------------------------------------------------------


def merged_relation_sql(
    *, kind: str, segment: str, relation: str, tk_param: str = "tk",
) -> str:
    """Every visible graph row with its overlays merged, as ``(...) AS
    <relation>`` with the effective relation's columns: for reads already
    bounded by an anchor (expand), where the anchor's own predicate narrows
    the Chronos scan and no ids-first stream is needed."""
    graph_rows = graph_relation_sql(kind, segment)
    edge_num = ", fetched.edge_num" if kind == "edges" else ""
    kind_literal = "'edge'" if kind == "edges" else "'node'"
    return (
        f"(SELECT {kind_literal}::text AS target_kind, 'default'::text AS namespace, "
        "fetched.target_id, fetched.subtype, fetched.src, fetched.dst, "
        "fetched.edge_type, "
        "fetched.attrs || COALESCE(ov.attrs, '{}'::jsonb) AS attrs, "
        "CASE WHEN ov.attrs IS NULL THEN NULL ELSE fetched.attrs END AS raw_attrs, "
        f"fetched.raw_text_hash{edge_num} "
        f"FROM {graph_rows} AS fetched "
        "LEFT JOIN LATERAL (SELECT jsonb_object_agg(attr_key, value) AS attrs "
        f"FROM {ATTRIBUTE_EFFECTIVE_TABLE} "
        f"WHERE target_kind = %({tk_param})s AND target_id = fetched.target_id "
        "AND generation_id = %(overlay_gen)s AND namespace = 'default' "
        "AND attr_key <> ALL(%(overlay_reserved)s::text[])) AS ov ON TRUE"
        f") AS {relation}"
    )


def graph_value_relation_sql(
    *, kind: str, segment: str, relation: str, sequential: bool = False,
) -> str:
    """The visible graph rows as the effective relation's columns with the
    GRAPH attrs (no overlay): the not-overlaid branch of the split aggregate
    and the stable-column count. ``sequential`` exposes the scope column and
    the visibility bounds through expressions no index serves, so the rows
    are read by a sequential scan (:func:`split_aggregate_sql`)."""
    edge_num = ", graph.edge_num" if kind == "edges" else ""
    kind_literal = "'edge'" if kind == "edges" else "'node'"
    rows = graph_relation_sql(
        kind, segment, scope_unindexed=sequential, visibility_unindexed=sequential,
    )
    return (
        f"(SELECT {kind_literal}::text AS target_kind, 'default'::text AS namespace, "
        "graph.target_id, graph.subtype, graph.src, graph.dst, graph.edge_type, "
        f"graph.attrs, NULL::jsonb AS raw_attrs, graph.raw_text_hash{edge_num} "
        f"FROM {rows} AS graph) AS {relation}"
    )


def overlaid_ids_sql(
    *, kind: str, whole_attrs: bool, member_ids: bool, ids_only: bool = False,
) -> str:
    """Targets with an overlay at the generation on any of
    ``%(graph_overlay_keys)s`` (on any key for a whole-attrs predicate): an
    id stream for ``fetch_relation_sql``, or with ``ids_only`` the bare
    ``target_id`` list a ``NOT IN`` reads."""
    key_test = (
        "o.attr_key <> ALL(%(overlay_reserved)s::text[])"
        if whole_attrs
        else "o.attr_key = ANY(%(graph_overlay_keys)s::text[])"
    )
    members = " AND o.target_id = ANY(%(rr_member_ids)s::text[])" if member_ids else ""
    num = ", o.target_id::bigint AS num" if kind == "edges" else ""
    columns = "o.target_id" if ids_only else f"DISTINCT o.target_id AS id{num}"
    return (
        f"SELECT {columns} FROM {ATTRIBUTE_EFFECTIVE_TABLE} o "
        "WHERE o.target_kind = %(tk)s AND o.generation_id = %(overlay_gen)s "
        f"AND o.namespace = 'default' AND {key_test}{members}"
    )


def split_aggregate_sql(
    *,
    kind: str,
    segment: str,
    relation: str,
    by_exprs: list[str],
    where_sql: str,
    keyed: bool,
    whole_attrs: bool,
    member_ids: bool,
    join_sql: str = "",
    sequential: bool = False,
) -> str:
    """``SELECT g0.., count`` for an aggregate on the graph path (design 2.5).

    Without attributes in play (``keyed`` false) one GROUP BY over the graph
    rows: overlays change no stable column. Otherwise the graph-value counts
    over targets NOT overlaid on any key the predicate or the grouping reads
    (``NOT IN``, a hashed subplan, kept on purpose: 0.67 s against 2.5 s for
    ``NOT EXISTS`` in the parent's measurement), plus the merged-value counts
    over the overlaid targets, summed per group. ``by_exprs`` and
    ``where_sql`` are written over ``relation`` (the caller inlines a grouped
    key as a literal so the planner can use its statistics object).
    ``join_sql`` follows the relation in each branch's FROM (the per-element
    lateral join of a count per list element, issue #3666 ``each()``); it
    may read only ``relation``'s columns.

    ``sequential`` (the caller sets it when :func:`broad_scope` holds for
    the predicate) has the keyed graph branch read the segment by a
    sequential scan: that branch reads every attrs blob of its rows, mostly
    stored out of line in TOAST, and an index walk visits heap and TOAST in
    node-id order. Measured at Krama head10 (f_aggregate, 287,861 graph rows
    grouped by ``community_id``): 4.17 s through ``node_source_node_visible``
    against 2.64 s by a sequential scan (EXPLAIN ANALYZE, timing off; lane SG
    AGG-DIAG 2026-10-20). The unkeyed GROUP BY reads no attrs and is left as
    it was."""
    select = ", ".join(f"{e} AS g{i}" for i, e in enumerate(by_exprs))
    groups = ", ".join(str(i + 1) for i in range(len(by_exprs)))
    graph_values = graph_value_relation_sql(kind=kind, segment=segment, relation=relation)
    if not keyed:
        return (
            f"SELECT {select}, count(*) AS n FROM {graph_values}{join_sql} "
            f"WHERE {where_sql} GROUP BY {groups}"
        )
    graph_branch, merged_branch = _split_branches(
        kind=kind, segment=segment, relation=relation, where_sql=where_sql,
        whole_attrs=whole_attrs, member_ids=member_ids, join_sql=join_sql,
        sequential=sequential,
    )
    outer = ", ".join(f"g{i}" for i in range(len(by_exprs)))
    return (
        f"SELECT {outer}, sum(n)::bigint AS n FROM ("
        f"SELECT {select}, count(*) AS n {graph_branch} GROUP BY {groups} "
        "UNION ALL "
        f"SELECT {select}, count(*) AS n {merged_branch} GROUP BY {groups}"
        f") AS split GROUP BY {outer}"
    )


def split_totals_sql(
    *,
    kind: str,
    segment: str,
    relation: str,
    counts: list[str],
    where_sql: str,
    whole_attrs: bool,
    member_ids: bool,
    sequential: bool = False,
) -> str:
    """One row of totals (``count(*)``, ``count(*) FILTER (...)``) on the
    graph path, read as the keyed split aggregate reads its rows: the graph
    values of the targets not overlaid on a key in play, plus the merged
    values of the overlaid ones, each count summed over the two branches.
    ``counts`` are aggregate expressions over ``relation``; issue #3666's
    ``each()`` facts statement is the caller. ``sequential`` as in
    :func:`split_aggregate_sql`."""
    graph_branch, merged_branch = _split_branches(
        kind=kind, segment=segment, relation=relation, where_sql=where_sql,
        whole_attrs=whole_attrs, member_ids=member_ids, sequential=sequential,
    )
    inner = ", ".join(f"{c} AS c{i}" for i, c in enumerate(counts))
    outer = ", ".join(f"COALESCE(sum(c{i}), 0)::bigint" for i in range(len(counts)))
    return (
        f"SELECT {outer} FROM ("
        f"SELECT {inner} {graph_branch} UNION ALL SELECT {inner} {merged_branch}"
        ") AS split"
    )


def _split_branches(
    *,
    kind: str,
    segment: str,
    relation: str,
    where_sql: str,
    whole_attrs: bool,
    member_ids: bool,
    join_sql: str = "",
    sequential: bool = False,
) -> tuple[str, str]:
    """``(FROM ... WHERE ...)`` of the two disjoint branches of a keyed
    split read: graph values of the targets not overlaid on any key in
    ``%(graph_overlay_keys)s``, and the overlaid targets with their
    overlays merged. ``sequential`` reads the graph branch by a sequential scan
    (:func:`graph_value_relation_sql`); the overlay branch fetches its few
    targets by id either way."""
    graph_values = graph_value_relation_sql(
        kind=kind, segment=segment, relation=relation, sequential=sequential,
    )
    overlaid = overlaid_ids_sql(kind=kind, whole_attrs=whole_attrs, member_ids=member_ids)
    overlaid_target_ids = overlaid_ids_sql(
        kind=kind, whole_attrs=whole_attrs, member_ids=member_ids, ids_only=True,
    )
    merged = fetch_relation_sql(f"({overlaid})", kind=kind, segment=segment,
                                relation=relation)
    return (
        f"FROM {graph_values}{join_sql} "
        f"WHERE ({where_sql}) AND {relation}.target_id NOT IN ({overlaid_target_ids})",
        f"FROM {merged}{join_sql} WHERE {where_sql}",
    )


# --------------------------------------------------------------------------
# Search lexical supplement (design 2.7)
# --------------------------------------------------------------------------

#: The internal leaf of the supplement's keyed containment; only
#: :func:`containment_lowerer` lowers it.
_KEY_CONTAINS = "_attrs_key_contains"


def containment_lowerer(attrs_sql: str):
    """A leaf lowerer for :func:`operator_sql.compile_read_predicate` that
    lowers ``{"op": "_attrs_key_contains", "field": "attrs.<k>", "value": v}``
    to ``attrs @> {k: v}`` decided on the one key: the same JSON type and
    ``(attrs -> k) @> v``. The type test keeps a top-level array from
    containing a scalar, which object containment does not allow one level
    down, so the leaf equals object containment key by key. It reads no GIN
    index (design 2.7: a GIN scan's constant row estimate would win for a
    common value)."""
    from okg.substrate.mcp.operator_sql import SqlLeaf, _bind_param

    def lower(node: Mapping[str, Any], params: dict[str, Any], prefix: str):
        field = node.get("field")
        if node.get("op") != _KEY_CONTAINS or not isinstance(field, str):
            return None
        if not field.startswith("attrs.") or not field[6:]:
            return None
        key = _bind_param(params, prefix, field[6:])
        value = f"{_bind_param(params, prefix, json.dumps(node.get('value')))}::jsonb"
        expr = f"({attrs_sql} -> {key})"
        return SqlLeaf(
            f"COALESCE(jsonb_typeof({expr}) = jsonb_typeof({value}) "
            f"AND {expr} @> {value}, FALSE)"
        )

    return lower


def containment_sql(attr_filters: Mapping[str, Any], params: dict[str, Any],
                    attrs_sql: str) -> str:
    """``attrs_sql @> attr_filters`` as per-key leaves through the one
    predicate compiler (#3235), bound into ``params``. Refuses a compile
    that is not exact."""
    from okg.substrate.mcp.operator_sql import compile_read_predicate

    compiled = compile_read_predicate(
        {"op": "and", "clauses": [
            {"op": _KEY_CONTAINS, "field": f"attrs.{key}", "value": value}
            for key, value in attr_filters.items()
        ]},
        leaf_lowerer=containment_lowerer(attrs_sql),
        field_map_identity="search-supplement-graph-v1",
        prefix="sc",
        params=params,
    )
    if compiled.qualification != "exact" or not compiled.truth_exact:
        raise ValueError("the search supplement's containment did not compile exactly")
    return compiled.bind_into(params)


def search_supplement_sql(
    *, segment: str, graph_where: list[str], graph_terms: list[str],
    merged_where: list[str], candidates_sql: str = "",
) -> str:
    """The supplement's statement on the graph path (design 2.7).

    Graph branch: visible rows (alias ``n``) passing ``graph_where`` over
    their graph attrs, with no overlay at the generation on any of
    ``%(graph_overlay_keys)s`` (the filter's keys, ``text``, and ``path``
    when a path exclusion applies), so their graph values of those keys are
    their effective values. Overlay branch: visible rows overlaid on any of
    those keys, merged (alias ``m``: ``n.attrs || overlay``), passing
    ``merged_where``. The two disjoint id lists merge in id order, are cut at
    ``%(limit)s`` and each row returns its merged attrs, as the materialized
    path returns the effective attrs. Needs ``bp``, ``overlay_gen``,
    ``overlay_reserved``, ``graph_overlay_keys``, ``limit``.

    The graph branch tests ``graph_terms`` (the ``ILIKE`` terms over
    ``n.attrs``) only on rows that passed every other clause: an ``OFFSET 0``
    subquery keeps them out of the scan, so the text trigram index never
    drives it. That index reads every row version, and a common term ran the
    recheck on 643,388 of them for 1,699 visible rows (11.1 s at Krama);
    today's read also tests the terms last. ``candidates_sql`` (the declared
    expression-index candidate set of the filter's keys, text target ids)
    narrows the graph branch when given; overlaid targets come from the
    overlay branch, so it never narrows that one. The overlay branch merges
    each row once (``OFFSET 0`` on the lateral), not once per clause."""
    visible = "n.deleted = FALSE AND n.live_lo <= %(bp)s AND %(bp)s < n.live_hi"
    overlay_scope = (
        "o.target_kind = 'node' AND o.generation_id = %(overlay_gen)s "
        "AND o.namespace = 'default'"
    )
    overlaid = (
        f"SELECT o.target_id FROM {ATTRIBUTE_EFFECTIVE_TABLE} o WHERE {overlay_scope} "
        "AND o.attr_key = ANY(%(graph_overlay_keys)s::text[])"
    )
    overlay_agg = (
        f"(SELECT jsonb_object_agg(ov.attr_key, ov.value) FROM {ATTRIBUTE_EFFECTIVE_TABLE} ov "
        "WHERE ov.target_kind = 'node' AND ov.target_id = n.node_id "
        "AND ov.generation_id = %(overlay_gen)s AND ov.namespace = 'default' "
        "AND ov.attr_key <> ALL(%(overlay_reserved)s::text[]))"
    )
    narrow = [f"n.node_id IN ({candidates_sql})"] if candidates_sql else []
    terms = " AND ".join(f"({t})" for t in graph_terms) or "TRUE"
    graph_branch = (
        f"SELECT n.node_id FROM (SELECT n.node_id, n.attrs FROM {segment} n "
        f"WHERE {visible} AND "
        + " AND ".join(f"({w})" for w in [*graph_where, *narrow])
        + f" AND NOT EXISTS ({overlaid} AND o.target_id = n.node_id) OFFSET 0) n "
        f"WHERE {terms} ORDER BY n.node_id LIMIT %(limit)s"
    )
    overlay_branch = (
        f"SELECT n.node_id FROM {segment} n "
        f"CROSS JOIN LATERAL (SELECT n.attrs || COALESCE({overlay_agg}, "
        "'{}'::jsonb) AS attrs OFFSET 0) m "
        f"WHERE {visible} AND n.node_id IN ({overlaid}) AND "
        + " AND ".join(f"({w})" for w in merged_where)
        + " ORDER BY n.node_id LIMIT %(limit)s"
    )
    return (
        "SELECT n.node_id, n.subtype, "
        f"n.attrs || COALESCE({overlay_agg}, '{{}}'::jsonb) AS attrs, "
        "n.text_hash, n.source, n.source_record_id, n.source_revision, "
        "-0.000001::float8 AS score "
        f"FROM (({graph_branch}) UNION ALL ({overlay_branch}) "
        "ORDER BY node_id LIMIT %(limit)s) AS ids "
        f"JOIN {segment} n ON n.node_id = ids.node_id AND {visible} "
        "ORDER BY n.node_id"
    )
