"""`rank_by="records"`: fold a passage into its parent record.

PACT search-result-shape, DESIGN.md D1 and D3 (issues #3069, #3070). A record
and its passages used to be separate ranked rows, so `limit` counted rows and
a page of ten held about seven records. Under `records` each passage folds
into the record it belongs to; the record ranks by its best member (its own
row or a folded passage) and names the winning passage.

Two shapes use this module:

- **Unfiltered** (no `subtype_filter`): the one ranked read today's search
  makes. A passage whose parent cannot take it (not exactly one parent, out
  of scope, failing a selection, unreadable) stays its own row, as today. The
  candidates' relation edges and the parents fetched from outside the ranked
  read are decided once per Nomos decision class (`count`); the rows the page
  returns that the self read did not read (a fetched parent, the winning
  passage's edge) are read row by row (decision 142a extends 5b).
- **Filtered two-read fold** (a `subtype_filter` naming a subtype that
  declares a passage relation, D3-i): the self read plus one capped read of
  the passages whose declared parent has the filtered subtype. A passage that
  does not fold is DROPPED, never shown: a passage row would break the filter
  and could reveal that a hidden or excluded record matches. The fold's
  candidates are decided once per Nomos decision class
  (`visible_ids_by_decision_class`, recorded as `count`); the rows the page
  returns are read row by row (decision 5 = 5b).
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping, Optional, Sequence

import psycopg
from psycopg.rows import dict_row

from okg.substrate.attributes import apply_effective_attrs_to_rows
from okg.substrate.errors import SubstrateError
from okg.substrate.mcp.passage_rollup import (
    PARENT_EDGE_SCAN_CAP,
    declared_parents_reached,
    passage_child_declarations,
)
from okg.substrate.mcp.session import Session
from okg.substrate.nomos.read_filter import (
    filter_edge_rows_for_session,
    filter_node_rows_for_session,
    visible_ids_by_decision_class,
)

__all__ = [
    "CHECK_CLASS",
    "CHECK_ROW",
    "FoldLink",
    "NEXT_OFFSET_REASON_BEYOND_EXACT_ORDER",
    "PHRASE_KEY",
    "PHRASE_SPANS_KEY",
    "RecordsOffsetBeyondExactOrder",
    "SearchFoldReadCheckMismatch",
    "exact_prefix_length",
    "fold_members",
    "member_key",
    "read_returned_rows",
    "resolve_fold_links",
]

#: Per-row read filter (records `read` decisions) or per decision class
#: (records one `count` decision per class).
CHECK_ROW = "row"
CHECK_CLASS = "class"

#: `next_offset_reason` when the next page would start past the exact prefix.
NEXT_OFFSET_REASON_BEYOND_EXACT_ORDER = "beyond_exact_order"

#: Private keys a folded row carries through the pipeline; removed before the
#: page is built.
WINNER_KEY = "_fold_winner"
MATCHING_KEY = "_fold_matching"
EDGE_KEY = "_fold_edge"
#: Whether the text a row is served from holds every quoted span (PACT
#: lexical-quoted-phrases-3064), and which spans it holds (a frozenset of span
#: indexes). Set by the search on the rows it read; named `_fold...` so a
#: fetched parent keeps it through `read_returned_rows`.
PHRASE_KEY = "_fold_phrase"
PHRASE_SPANS_KEY = "_fold_phrase_spans"


class SearchFoldReadCheckMismatch(SubstrateError):
    """A row the fold's class check admitted was hidden or dropped on read.

    The message states what was observed, not why: a policy naming the read
    purpose, or a policy bundle that activated between the class check and the
    row read, can each produce it. The page is never returned without the row,
    and never ranked by a passage the reader cannot read.
    """

    code = "search_fold_read_check_mismatch"


class RecordsOffsetBeyondExactOrder(SubstrateError):
    """An `offset` at or past the exactly ordered records of a filtered fold.

    Refused only when the record count is a lower bound: records past the
    exact prefix have no exact rank, so "the next page" does not exist to be
    returned. `offset=0` is never refused.
    """

    code = "records_offset_beyond_exact_order"


@dataclass(frozen=True)
class FoldLink:
    """A passage's one resolved parent and the edge that reaches it."""

    parent_id: str
    parent_subtype: str
    edge: Mapping[str, Any]


def phrase_rank(row: Mapping[str, Any]) -> int:
    """`0` when the row's served text holds every quoted span, else `1`."""
    return 0 if row.get(PHRASE_KEY) else 1


def member_key(
    row: Mapping[str, Any], *, phrase_first: bool = False,
) -> tuple[int, ...] | tuple[Any, ...]:
    """Today's lexical sort key: exact before expanded, then score, then id.

    With `phrase_first` (a quoted search's unfiltered fold, PACT
    lexical-quoted-phrases-3064) a member whose text holds every quoted
    span leads, so a matching passage wins its record."""
    key: tuple[Any, ...] = (
        int(row.get("expansion_rank") or 0),
        float(row.get("score") or 0.0),
        str(row["node_id"]),
    )
    return (phrase_rank(row), *key) if phrase_first else key


def resolve_fold_links(
    conn: psycopg.Connection,
    session: Session,
    *,
    passages: Sequence[Mapping[str, Any]],
    known_parents: Mapping[str, Mapping[str, Any]],
    node_segment: str,
    edge_segment: str,
    branch_point_value: Any,
    check: str,
    mask_fetched_parents: bool = False,
    edge_scan_cap: int = PARENT_EDGE_SCAN_CAP,
) -> tuple[dict[str, FoldLink], dict[str, dict[str, Any]]]:
    """`({passage_id: FoldLink}, {fetched_parent_id: row})`.

    A passage links only when its readable declared edges reach exactly one
    readable parent (state `resolved`); `multiple_parents`, unlinked and
    bounded-unknown passages get no link. `known_parents` are rows the caller
    already read and holds (they are readable); every other reached parent is
    fetched at the pin and checked here. Fetched parents come back with the
    effective-attrs overlay applied, so the caller can test selections on
    them; under `CHECK_CLASS` they have NOT been read row by row yet.

    `mask_fetched_parents` (review B1): when the caller will test the fetched
    parents' attributes (`attr_filters`, path exclusions), they must be the
    attributes the reader sees. The fetched parents then go through the
    per-row read filter, which masks them (and records `read`, as the self
    read does for its candidates), so a masked attribute never matches and a
    record's presence cannot reveal a hidden value. Without such selections
    nothing is tested on them and the per-class check stands.
    """
    if check not in (CHECK_ROW, CHECK_CLASS):
        raise ValueError(f"check must be {CHECK_ROW!r} or {CHECK_CLASS!r}")
    by_id = {str(row["node_id"]): row for row in passages}
    if not by_id:
        return {}, {}
    declarations = passage_child_declarations(
        conn,
        catalog_version_id=session.catalog_version_id,
        child_subtypes=sorted({str(row.get("subtype") or "") for row in passages}),
    )
    declared_of = {
        pid: declarations.get(str(row.get("subtype") or "")) or []
        for pid, row in by_id.items()
    }
    child_ids = sorted(pid for pid, declared in declared_of.items() if declared)
    if not child_ids:
        return {}, {}
    edge_types = sorted({
        rollup.edge_type for pid in child_ids for rollup in declared_of[pid]
    })
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            SELECT edge_id, src, dst, edge_type, attrs, source,
                   source_record_id, source_revision
              FROM {edge_segment}
             WHERE dst = ANY(%(child_ids)s)
               AND edge_type = ANY(%(edge_types)s)
               AND deleted = FALSE
               AND live_lo <= %(bp)s AND %(bp)s < live_hi
             ORDER BY dst, edge_type, src
             LIMIT %(cap)s
            """,
            {
                "child_ids": child_ids,
                "edge_types": edge_types,
                "bp": branch_point_value,
                "cap": edge_scan_cap + 1,
            },
        )
        raw_edges = cur.fetchall()
    undecided: set[str] = set()
    if len(raw_edges) > edge_scan_cap:
        raw_edges = raw_edges[:edge_scan_cap]
        seen = [str(edge["dst"]) for edge in raw_edges]
        decided = set(seen[:-1]) if seen else set()
        undecided = {pid for pid in child_ids if pid not in decided}
    if check == CHECK_ROW:
        edges = filter_edge_rows_for_session(conn, session, raw_edges)
    else:
        visible = visible_ids_by_decision_class(
            conn, session, raw_edges, row_kind="edge",
        )
        edges = [edge for edge in raw_edges if str(edge["edge_id"]) in visible]

    parent_subtypes = sorted({
        rollup.parent_subtype for pid in child_ids for rollup in declared_of[pid]
    })
    wanted = sorted({str(edge["src"]) for edge in edges} - set(known_parents))
    fetched: dict[str, dict[str, Any]] = {}
    if wanted:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT node_id, subtype, attrs, text_hash, source,
                       source_record_id, source_revision
                  FROM {node_segment}
                 WHERE node_id = ANY(%(ids)s)
                   AND subtype = ANY(%(subtypes)s)
                   AND deleted = FALSE
                   AND live_lo <= %(bp)s AND %(bp)s < live_hi
                 ORDER BY node_id
                """,
                {"ids": wanted, "subtypes": parent_subtypes, "bp": branch_point_value},
            )
            raw_parents = cur.fetchall()
        if check == CHECK_ROW or mask_fetched_parents:
            readable = filter_node_rows_for_session(conn, session, raw_parents)
        else:
            visible = visible_ids_by_decision_class(
                conn, session, raw_parents, row_kind="node",
            )
            readable = [row for row in raw_parents if str(row["node_id"]) in visible]
        readable = apply_effective_attrs_to_rows(
            conn, readable, target_kind="node", target_id_field="node_id",
            generation_id=session.generation_id,
        )
        fetched = {str(row["node_id"]): row for row in readable}

    subtype_of = {pid: str(row.get("subtype")) for pid, row in known_parents.items()}
    subtype_of.update({pid: str(row.get("subtype")) for pid, row in fetched.items()})
    edges_by_child: dict[str, list[Mapping[str, Any]]] = {}
    for edge in edges:
        edges_by_child.setdefault(str(edge["dst"]), []).append(edge)
    links: dict[str, FoldLink] = {}
    for pid in child_ids:
        if pid in undecided:
            continue
        child_edges = edges_by_child.get(pid, [])
        reached = declared_parents_reached(
            ((str(e["src"]), str(e["edge_type"])) for e in child_edges),
            declared=declared_of[pid],
            parent_subtype_by_id=subtype_of,
        )
        if len(reached) != 1:
            continue
        parent_id, rollup = reached[0]
        edge = next(
            e for e in child_edges
            if str(e["src"]) == parent_id and str(e["edge_type"]) == rollup.edge_type
        )
        links[pid] = FoldLink(
            parent_id=parent_id, parent_subtype=rollup.parent_subtype, edge=edge,
        )
    return links, fetched


def fold_members(
    *,
    records: Sequence[dict[str, Any]],
    passages: Sequence[dict[str, Any]],
    links: Mapping[str, FoldLink],
    fetched_parents: Mapping[str, dict[str, Any]],
    parent_admitted: Callable[[Mapping[str, Any]], bool],
    keep_unfolded: bool,
    takes_passages: Callable[[Mapping[str, Any]], bool] = lambda row: True,
    head_id: Optional[str] = None,
    drop_when_parent_rejected: bool = False,
    keep_when_parent_matches: Optional[Callable[[Mapping[str, Any]], bool]] = None,
    phrase_first: bool = False,
) -> list[dict[str, Any]]:
    """Fold passages into their records; return the rows sorted by best member.

    `records` are rows that rank on their own text and may take passages.
    A passage folds into the record its link names when that record is among
    `records`, or is a fetched parent `parent_admitted` accepts (scope and
    selections, D1 rules 3-4). A record's sort key is the best of its own
    row's key and its passages' keys; the winning member is kept under
    `WINNER_KEY` (None when the record's own text won). A passage that does
    not fold stays its own row when `keep_unfolded`, else it is dropped.
    With `drop_when_parent_rejected`, a passage whose fetched parent
    `parent_admitted` refuses is dropped even under `keep_unfolded`: it
    matches through its record (design `read-model` D6), so a record a `where`
    excludes does not come back as its passage. A passage with no readable
    parent still stays its own row. `keep_when_parent_matches` (issue #3466)
    names the refused parents whose passages stay their own rows anyway: a
    parent that passes the `where` but lies outside the search profile (a
    content profile without `document`), so a chunk matched through its
    document's path is not lost with the document.

    `matching_passages` is 0 on a record whose type takes passages
    (`takes_passages`) and null on any other row. `head_id` is the
    alias-promoted row (rule 8): it stays at the head whatever a folded
    passage scores elsewhere.

    `phrase_first` (PACT lexical-quoted-phrases-3064): members and rows lead
    with `phrase_rank`, so a passage holding every quoted span wins its
    record and such records come first. Whatever the flag, a record's
    `PHRASE_KEY` becomes its own ranked text's verdict OR its winner's, and
    `PHRASE_SPANS_KEY` the union of the two; a fetched parent's own text was
    not ranked and counts for nothing.
    """
    key = lambda row: member_key(row, phrase_first=phrase_first)  # noqa: E731
    rows: dict[str, dict[str, Any]] = {}
    for record in records:
        row = dict(record)
        row[WINNER_KEY] = None
        row[MATCHING_KEY] = 0 if takes_passages(record) else None
        row["_fold_own_key"] = key(record)
        rows[str(record["node_id"])] = row
    unfolded: list[dict[str, Any]] = []
    for passage in passages:
        link = links.get(str(passage["node_id"]))
        target: Optional[dict[str, Any]] = None
        if link is not None:
            target = rows.get(link.parent_id)
            if target is None:
                parent = fetched_parents.get(link.parent_id)
                if parent is not None and parent_admitted(parent):
                    target = dict(parent)
                    target[WINNER_KEY] = None
                    target[MATCHING_KEY] = 0
                    target["_fold_own_key"] = None
                    # Its own text was not ranked: it matches through a passage.
                    target.pop(PHRASE_KEY, None)
                    target.pop(PHRASE_SPANS_KEY, None)
                    rows[link.parent_id] = target
                elif (
                    parent is not None and drop_when_parent_rejected
                    and not (
                        keep_when_parent_matches is not None
                        and keep_when_parent_matches(parent)
                    )
                ):
                    continue
        if target is None:
            if keep_unfolded:
                row = dict(passage)
                row[MATCHING_KEY] = None
                unfolded.append(row)
            continue
        target[MATCHING_KEY] = int(target[MATCHING_KEY] or 0) + 1
        best = target[WINNER_KEY]
        if best is None or key(passage) < key(best):
            target[WINNER_KEY] = passage
            target[EDGE_KEY] = link.edge
    out: list[dict[str, Any]] = []
    for row in rows.values():
        own = row.pop("_fold_own_key")
        winner = row[WINNER_KEY]
        if winner is not None and (own is None or key(winner) < own):
            row["score"] = winner.get("score")
            row["expansion_rank"] = int(winner.get("expansion_rank") or 0)
            if PHRASE_KEY in winner or PHRASE_KEY in row:
                row[PHRASE_KEY] = bool(row.get(PHRASE_KEY) or winner.get(PHRASE_KEY))
                row[PHRASE_SPANS_KEY] = frozenset(
                    row.get(PHRASE_SPANS_KEY) or ()
                ) | frozenset(winner.get(PHRASE_SPANS_KEY) or ())
        else:
            row[WINNER_KEY] = None
            row.pop(EDGE_KEY, None)
        out.append(row)
    out.extend(unfolded)
    out.sort(key=key)
    if head_id is not None:
        head = [row for row in out if str(row["node_id"]) == head_id]
        out = head + [row for row in out if str(row["node_id"]) != head_id]
    return out


def declaring_subtypes(conn: psycopg.Connection, catalog_version_id: int) -> set[str]:
    """The record types that declare a passage relation at this catalog version."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT name FROM okg.subtypes WHERE catalog_version_id = %s"
            " AND metadata ? 'passage_rollup'",
            (catalog_version_id,),
        )
        return {str(row[0]) for row in cur.fetchall()}


def self_cutoffs(
    rows: Sequence[Mapping[str, Any]],
    *,
    literal_saturated: bool,
    effective_saturated: bool,
    effective_ids: set[str],
) -> list[float]:
    """The self read's cutoffs, one per saturated read, each from that read's
    OWN visible rows (review): the literal read's rows and the
    effective-attribute read's rows are separate capped reads, so one read's
    rows never bound the other's prefix. Alias-promoted and expansion-only
    rows set no cutoff."""
    exact = [
        row for row in rows
        if not row.get("match_kind") and int(row.get("expansion_rank") or 0) == 0
    ]
    out: list[float] = []
    for saturated, mine in (
        (literal_saturated, [r for r in exact if str(r["node_id"]) not in effective_ids]),
        (effective_saturated, [r for r in exact if str(r["node_id"]) in effective_ids]),
    ):
        cut = cutoff(saturated, (float(r.get("score") or 0.0) for r in mine))
        if cut is not None:
            out.append(cut)
    return out


def cutoff(saturated: bool, visible_scores: Iterable[float]) -> Optional[float]:
    """A read's cutoff: None when it did not saturate (it sets no cutoff); the
    worst (highest) visible score when it did; minus infinity when it
    saturated and none of its rows is visible (nothing is known exact)."""
    if not saturated:
        return None
    scores = [float(score) for score in visible_scores]
    return max(scores) if scores else -math.inf


def exact_prefix_length(
    rows: Sequence[Mapping[str, Any]], cutoffs: Iterable[Optional[float]],
) -> int:
    """Leading rows whose order is exact: strictly better than the stricter
    (lower) cutoff of the saturated reads, exact wording only. With no
    saturated read the whole list is exact."""
    cuts = [cut for cut in cutoffs if cut is not None]
    if not cuts:
        return len(rows)
    stricter = min(cuts)
    count = 0
    for index, row in enumerate(rows):
        if index == count and row.get("match_kind"):
            # An alias-promoted head (rule 8) is placed, not ranked.
            count += 1
            continue
        if int(row.get("expansion_rank") or 0) != 0:
            break
        if not float(row.get("score") or 0.0) < stricter:
            break
        count += 1
    return count


def read_returned_rows(
    conn: psycopg.Connection,
    session: Session,
    *,
    page: Sequence[dict[str, Any]],
    already_read: set[str],
) -> None:
    """Read, row by row, every row a fold returns that the self read did not
    already read: a fetched parent, each winning passage and its relation
    edge. Records `read` decisions and applies redaction; the page's rows are
    updated in place with the read-filtered, overlaid rows. A winning passage
    in `already_read` (the unfiltered fold's passages are ranked candidates)
    was read row by row already and is not read again; its edge is.

    A row the class check admitted and the row read hides fails the call.
    """
    parents = [row for row in page if str(row["node_id"]) not in already_read]
    winners = [
        row[WINNER_KEY] for row in page
        if row.get(WINNER_KEY) is not None
        and str(row[WINNER_KEY]["node_id"]) not in already_read
    ]
    edges = [row[EDGE_KEY] for row in page if row.get(WINNER_KEY) is not None]
    plain = lambda row: {k: v for k, v in row.items() if not k.startswith("_fold")}  # noqa: E731
    read_parents = filter_node_rows_for_session(
        conn, session, [plain(row) for row in parents],
    )
    read_winners = filter_node_rows_for_session(
        conn, session, [plain(row) for row in winners],
    )
    read_edges = filter_edge_rows_for_session(conn, session, list(edges))
    missing = {
        "node": (len(parents) - len(read_parents)) + (len(winners) - len(read_winners)),
        "edge": len(edges) - len(read_edges),
    }
    if any(missing.values()):
        kinds = ", ".join(f"{n} {kind}" for kind, n in missing.items() if n)
        raise SearchFoldReadCheckMismatch(
            f"the per-row read hid {kinds} row(s) that the search's per-class "
            f"check had admitted; the page is not returned without them",
            data={"hidden_node_rows": missing["node"], "hidden_edge_rows": missing["edge"]},
            fix_hint=(
                "retry the search; if it repeats, check the active Nomos policy "
                "bundle for rules that treat counting and reading differently"
            ),
        )
    overlay = lambda rows: apply_effective_attrs_to_rows(  # noqa: E731
        conn, rows, target_kind="node", target_id_field="node_id",
        generation_id=session.generation_id,
    )
    by_parent = {str(row["node_id"]): row for row in overlay(read_parents)}
    by_winner = {str(row["node_id"]): row for row in overlay(read_winners)}
    by_edge = {str(row["edge_id"]): row for row in read_edges}
    for row in page:
        node_id = str(row["node_id"])
        if node_id in by_parent:
            keep = {k: v for k, v in row.items() if k.startswith("_fold") or k in (
                "score", "expansion_rank",
            )}
            row.clear()
            row.update(by_parent[node_id])
            row.update(keep)
        winner = row.get(WINNER_KEY)
        if winner is not None:
            read = by_winner.get(str(winner["node_id"]))
            if read is not None:
                row[WINNER_KEY] = {**read, "score": winner.get("score")}
            row[EDGE_KEY] = by_edge[str(row[EDGE_KEY]["edge_id"])]
