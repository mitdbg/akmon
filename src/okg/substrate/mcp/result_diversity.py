"""Per-parent result diversity: stop one parent from owning a whole page.

Per issue #1444 (part 2). Lexical ranking is BM25 score then `node_id`, with no
bound on how many rows may come from the same parent document. On a corpus of
chunked documents that is a real failure mode rather than a stylistic one: a
document whose text repeats the query terms contributes chunk after chunk, and
because the rank cap (`_LEXICAL_RANK_CAP`) is a fixed 200 rows, those siblings
do not merely crowd the first page — they EVICT other documents from the ranking
entirely.

Four decisions, all made here rather than left implicit:

1. **Off by default.** `max_per_parent=None` is today's behaviour. A caller who
   never asked for diversity must not silently receive a page with rows removed.

2. **Parents come only from the pinned catalog's declared passage relation** —
   the same `okg_passage_rollup` declaration `passage_rollup.py` reads, followed
   in the child→parent direction. Nothing is inferred from `attrs.path`, from
   `member_of`, or from node-id shape. `passage_rollup_declaration` already
   records why: the erag-seg catalog declares four candidate relations between
   `document` and `document_chunk`, so "the edge that relates a passage to its
   parent" is not derivable, and guessing would suppress rows on the strength of
   a guess. An edge names a parent only when the node it reaches is live,
   readable, and of a subtype that declares that edge type for the row's
   subtype (`passage_rollup.declared_parents_reached`, issue #2753): an edge
   from an undeclared subtype groups nothing.

3. **A row with no resolvable parent is never suppressed.** Absence of a
   declared relation is not evidence of a shared parent. This is the property
   that keeps diversity from quietly deleting the answer on a corpus that
   declares no rollup at all.

4. **After ranking, before pagination.** Suppressing after pagination would
   under-fill the page; suppressing before ranking is not possible (the ranking
   is what decides which sibling is best). Ordering is preserved exactly, and
   the surviving rows keep their raw scores untouched — this is a SELECTION
   control, not a reranker.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

import psycopg
from psycopg.rows import dict_row

from okg.substrate.errors import SubstrateError
from okg.substrate.mcp.passage_rollup import (
    declared_parents_reached,
    passage_child_declarations,
)
from okg.substrate.mcp.session import Session
from okg.substrate.nomos.read_filter import (
    filter_edge_rows_for_session,
    filter_node_rows_for_session,
)


__all__ = [
    "DIVERSITY_RELATION_CAP",
    "DiversityNotApplicable",
    "DiversityRelationBudgetExceeded",
    "DiversityOutcome",
    "apply_max_per_parent",
    "assert_diversity_rank_mode",
    "resolve_declared_parents",
]


#: Declared child→parent associations one diversity pass may materialize.
#: Matches `PASSAGE_ROLLUP_RELATION_CAP`: the same relation, the same fan-out
#: risk, and a refusal rather than a truncated parent map — a truncated map
#: would silently stop suppressing siblings while the envelope claimed the cap
#: applied.
DIVERSITY_RELATION_CAP = 8000


class DiversityNotApplicable(SubstrateError):
    """`max_per_parent` was named where it has no defined effect.

    The sharp case is `rank_by='passages'`, which already returns exactly one
    row per parent: accepting `max_per_parent` there would be a guaranteed
    no-op, and a no-op accepted in silence is indistinguishable from a control
    that worked.
    """

    code = "diversity_not_applicable"


class DiversityRelationBudgetExceeded(SubstrateError):
    """The declared parent relation exceeds the bounded association budget."""

    code = "diversity_relation_budget_exceeded"


@dataclass(frozen=True)
class DiversityOutcome:
    """The kept rows plus everything the envelope has to state."""

    rows: list[dict[str, Any]]
    applied: bool
    max_per_parent: int | None
    suppressed_sibling_count: int
    total_count_before_diversity: int
    parents_resolved: int
    rows_without_parent: int


def assert_diversity_rank_mode(
    max_per_parent: int | None, *, rank_by: str, method: str,
) -> None:
    """Refuse `max_per_parent` where it cannot do anything. Pure validation."""
    if max_per_parent is None:
        return
    if method != "lexical":
        raise DiversityNotApplicable(
            f"max_per_parent applies only to method='lexical'; got "
            f"method={method!r}. The parent relation is followed over the "
            f"ranked lexical candidates; no other method produces that set.",
            data={"method": method, "max_per_parent": max_per_parent},
            fix_hint="drop max_per_parent, or use method='lexical'",
        )
    if rank_by != "self":
        raise DiversityNotApplicable(
            f"max_per_parent cannot combine with rank_by={rank_by!r}: that "
            f"mode already rolls every child up into its parent and returns "
            f"ONE row per parent, so a per-parent cap of {max_per_parent} "
            f"could never remove a row. Accepting it would be a silent no-op "
            f"the caller could not tell from a cap that worked.",
            data={"rank_by": rank_by, "max_per_parent": max_per_parent},
            fix_hint=(
                "drop max_per_parent (rank_by='records' and 'passages' "
                "already give one row per parent), or use rank_by='self'"
            ),
        )


def resolve_declared_parents(
    conn: psycopg.Connection,
    session: Session,
    *,
    catalog_version_id: int,
    node_segment: str,
    edge_segment: str,
    branch_point_value: Any,
    rows: Iterable[dict[str, Any]],
) -> dict[str, str]:
    """Map `node_id -> primary parent node_id` for rows that HAVE a declared
    parent relation. Rows absent from the map have no resolvable parent.

    Three bounded queries, all at the pin, whichever the page size:

      1. the pinned catalog's `okg_passage_rollup` declarations whose
         `child_subtype` is one of the candidate subtypes, giving the legal
         `(parent_subtype, edge_type)` pairs (`passage_child_declarations`,
         the lookup parent resolution uses);
      2. the live edges of those types pointing AT the candidates;
      3. the live nodes those edges reach, of a declared parent subtype.

    Edges and parent nodes both pass the Nomos read filter first. An edge
    names a parent only when the reached node's own subtype declares that edge
    type for the candidate's subtype, so a row whose only edge comes from an
    undeclared subtype, or from a parent the caller cannot read, has no parent
    and is never suppressed.

    A candidate with several parents is assigned the lexicographically smallest
    parent node id. That choice is arbitrary in the same way `node_id` is
    arbitrary as the score tie-break, and it is made for the same reason: the
    cap must return the same page twice. It is stated in the docs rather than
    left for a reader to discover from a changing page.
    """
    candidates = {str(row["node_id"]): str(row.get("subtype")) for row in rows}
    if not candidates:
        return {}
    declarations = passage_child_declarations(
        conn,
        catalog_version_id=catalog_version_id,
        child_subtypes=sorted(set(candidates.values())),
    )
    if not declarations:
        return {}

    edge_types = sorted(
        {rollup.edge_type for declared in declarations.values() for rollup in declared}
    )
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            SELECT edge_id, src, dst, edge_type, attrs, source
              FROM {edge_segment}
             WHERE dst = ANY(%(ids)s)
               AND edge_type = ANY(%(edge_types)s)
               AND deleted = FALSE
               AND live_lo <= %(bp)s AND %(bp)s < live_hi
             ORDER BY edge_id
             LIMIT %(cap)s
            """,
            {
                "ids": sorted(candidates),
                "edge_types": edge_types,
                "bp": branch_point_value,
                "cap": DIVERSITY_RELATION_CAP + 1,
            },
        )
        raw_edges = cur.fetchall()
    if len(raw_edges) > DIVERSITY_RELATION_CAP:
        raise DiversityRelationBudgetExceeded(
            f"the declared parent relation returned more than "
            f"{DIVERSITY_RELATION_CAP} associations for this page; refusing "
            f"rather than capping the parent map, which would stop "
            f"suppressing siblings while the envelope reported the cap.",
            data={"edge_types": edge_types,
                  "relation_cap": DIVERSITY_RELATION_CAP},
            fix_hint=(
                "narrow the query, or correct unexpected passage-to-parent "
                "fan-out before retrying"
            ),
        )
    # Policy first, exactly as the rollup does: a hidden edge must not decide
    # which visible row is suppressed.
    readable_edges = filter_edge_rows_for_session(conn, session, raw_edges)

    edges_by_child: dict[str, set[tuple[str, str]]] = {}
    for edge in readable_edges:
        child_id = str(edge["dst"])
        if declarations.get(candidates.get(child_id, "")):
            edges_by_child.setdefault(child_id, set()).add(
                (str(edge["src"]), str(edge["edge_type"]))
            )
    parent_ids = sorted(
        {src for pairs in edges_by_child.values() for src, _ in pairs}
    )
    if not parent_ids:
        return {}
    parent_subtypes = sorted(
        {
            rollup.parent_subtype
            for declared in declarations.values()
            for rollup in declared
        }
    )
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            SELECT node_id, subtype, attrs, source
              FROM {node_segment}
             WHERE node_id = ANY(%(ids)s)
               AND subtype = ANY(%(parent_subtypes)s)
               AND deleted = FALSE
               AND live_lo <= %(bp)s AND %(bp)s < live_hi
            """,
            {
                "ids": parent_ids,
                "parent_subtypes": parent_subtypes,
                "bp": branch_point_value,
            },
        )
        raw_parents = cur.fetchall()
    # A parent the caller cannot read must not decide which visible row is
    # suppressed either.
    parent_subtype_by_id = {
        str(row["node_id"]): str(row["subtype"])
        for row in filter_node_rows_for_session(conn, session, raw_parents)
    }

    resolved: dict[str, str] = {}
    for child_id, pairs in edges_by_child.items():
        reached = declared_parents_reached(
            pairs,
            declared=declarations[candidates[child_id]],
            parent_subtype_by_id=parent_subtype_by_id,
        )
        if reached:
            # `reached` is sorted by parent id: the smallest is the primary.
            resolved[child_id] = reached[0][0]
    return resolved


def apply_max_per_parent(
    conn: psycopg.Connection,
    session: Session,
    *,
    catalog_version_id: int,
    node_segment: str,
    edge_segment: str,
    branch_point_value: Any,
    rows: list[dict[str, Any]],
    max_per_parent: int | None,
) -> DiversityOutcome:
    """Keep the top `max_per_parent` ranked rows per declared parent.

    `rows` must already be in final rank order; this preserves that order
    exactly and never touches a score.
    """
    if max_per_parent is None:
        return DiversityOutcome(
            rows=rows,
            applied=False,
            max_per_parent=None,
            suppressed_sibling_count=0,
            total_count_before_diversity=len(rows),
            parents_resolved=0,
            rows_without_parent=len(rows),
        )
    parent_by_child = resolve_declared_parents(
        conn,
        session,
        catalog_version_id=catalog_version_id,
        node_segment=node_segment,
        edge_segment=edge_segment,
        branch_point_value=branch_point_value,
        rows=rows,
    )
    kept: list[dict[str, Any]] = []
    seen: dict[str, int] = {}
    suppressed = 0
    without_parent = 0
    for row in rows:
        parent = parent_by_child.get(str(row["node_id"]))
        if parent is None:
            without_parent += 1
            kept.append(row)
            continue
        count = seen.get(parent, 0)
        if count >= max_per_parent:
            suppressed += 1
            continue
        seen[parent] = count + 1
        kept.append(row)
    return DiversityOutcome(
        rows=kept,
        applied=True,
        max_per_parent=max_per_parent,
        suppressed_sibling_count=suppressed,
        total_count_before_diversity=len(rows),
        parents_resolved=len(seen),
        rows_without_parent=without_parent,
    )
