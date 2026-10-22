"""The passage→document rollup: rank a parent subtype by its children's scores.

Per PACT `recover-passage-rollup-reads`.

**The defect.** A `document` node carries no body text. Its `attrs.text` is a
rendering of the file path (both of its searchable fields are also declared
path fields, which is exactly what `subtype_text_roles` reports as `locator`),
so `search(method="lexical", subtype_filter="document")` is a FILENAME search.
Measured on the arm-B benchmark run, 20 of 25 questions opened that way; the
gold document for `qst_0118` ranks 49th on its filename and 1st on its body,
and `qst_0358`'s gold is unreachable at document level at any depth an operator
would page to.

**The fix.** Rank the CHUNKS, then attribute each chunk's score to the
document that contains it. Four things about that are decisions, not details,
and all four are resolved here rather than left implicit:

1. **Operator mode.** An explicit `rank_by` argument, defaulting to `"self"`
   (today's behaviour). Overloading `subtype_filter` was rejected: a caller
   asking for `document` rows ranked by their own text would silently get
   something else ranked a different way, and the envelope could not tell the
   two apart.

2. **Declared parent relation** — see `passage_rollup_declaration`. Absent a
   declaration this RAISES. Degrading to self-ranking would hand a caller a
   filename search they believe is a content search, which is the defect
   restated, and a silent fallback is a P0 under this repo's standing rule.

3. **Aggregation** — `PASSAGE_ROLLUP_AGGREGATION`, decided by measurement (see
   its docstring) and reported in every envelope as `rank_by_aggregation`,
   because two deployments ranking documents differently must not both call
   the result "the top documents".

4. **Pagination** — the ranked unit is the CHUNK and the returned unit is the
   DOCUMENT, so the SQL budget is a chunk budget and `total_count` is a
   document count and they are not the same number. See
   `PASSAGE_ROLLUP_CHUNK_CAP` and `assert_rollup_offset_ordered`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import (
    Any, Callable, Hashable, Iterable, Mapping, Optional, Sequence, TypeVar,
)

import psycopg
from psycopg.rows import dict_row

from okg.substrate.attributes import apply_effective_attrs_to_rows
from okg.substrate.bm25_node_search import (
    RANKED_BM25_MATCH,
    execute_with_custom_plan,
)
from okg.substrate.catalog.linker_config import (
    LOAD_STATE_ATTR,
    NOT_LOADED,
    is_link_endpoint_row,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.mcp.call_read_cache import cached_read
from okg.substrate.mcp.session import Session
from okg.substrate.mcp.provenance import _fence_value, classify_sources
from okg.substrate.mcp.readable_text import keep_readable_text
from okg.substrate.nomos.read_filter import (
    filter_edge_rows_for_session,
    filter_node_rows_for_session,
)
from okg.substrate.nomos.read_masking import MASK_TOKEN
from okg.substrate.transcript_trust import (
    attrs_mark_untrusted_transcript_content,
)

T = TypeVar("T")

__all__ = [
    "PARENT_EDGE_SCAN_CAP",
    "PARENT_LABEL_SOURCE",
    "PARENT_RELATION_SOURCE",
    "PARENT_STATE_AMBIGUOUS_DECLARATION",
    "PARENT_STATE_BOUNDED_UNKNOWN",
    "PARENT_STATE_CANDIDATE",
    "PARENT_STATE_MULTIPLE_PARENTS",
    "PARENT_STATE_RESOLVED",
    "PARENT_STATE_UNLINKED",
    "PARENT_STATUS_MAX_CHARS",
    "PARENT_TITLE_ATTRS",
    "PARENT_TITLE_MAX_CHARS",
    "PASSAGE_COUNT_CAP",
    "POSITION_HEADING_MAX_CHARS",
    "POSITION_INDEX_ATTRS",
    "PASSAGE_ROLLUP_AGGREGATION",
    "PASSAGE_ROLLUP_AGGREGATIONS",
    "PASSAGE_ROLLUP_CHUNK_CAP",
    "PASSAGE_ROLLUP_POLICY_SCAN_CAP",
    "PASSAGE_ROLLUP_RELATION_CAP",
    "PassageRollup",
    "PassageRollupChunkRowsUnavailable",
    "PassageRollupOffsetUnordered",
    "PassageRollupPolicyScanExhausted",
    "PassageRollupRelationBudgetExceeded",
    "PassageRollupSubtypeRequired",
    "PassageRollupUndeclared",
    "RANK_BY_PASSAGES",
    "RANK_BY_RECORDS",
    "RANK_BY_SELF",
    "RANK_BY_VALUES",
    "assert_rollup_offset_ordered",
    "declared_parents_reached",
    "passage_child_declarations",
    "passage_rollup_candidates",
    "fence_position_heading",
    "fill_passage_rows",
    "passage_count_disclosure",
    "passage_position",
    "passage_rollup_declaration",
    "ranked_chunk_sql",
    "resolve_edge_segment",
    "resolve_passage_parents",
    "validate_rank_by",
]


#: `rank_by` values. `records` (PACT search-result-shape) is the lexical
#: default: one row per record, a passage folded into its parent. `self` ranks
#: each row by its own text and is the default of every other method, and of a
#: lexical call that passes an explicit `max_per_parent` (it asked for sibling
#: rows). `passages` is the opt-in parent rollup.
RANK_BY_SELF = "self"
RANK_BY_PASSAGES = "passages"
RANK_BY_RECORDS = "records"
RANK_BY_VALUES = (RANK_BY_SELF, RANK_BY_PASSAGES, RANK_BY_RECORDS)

#: Chunk BM25 scores are negative and more negative is better -- the
#: matches-only test `ranked.score < 0`, which both `_search_lexical` and
#: `ranked_chunk_sql` apply in an outer query over the capped ranked page
#: rather than as a predicate of the scan -- so `max` is `min(score)`.
#:
#:   max       parent score = its best chunk's score.
#:   sum_top3  sum of the parent's three best chunk scores.
PASSAGE_ROLLUP_AGGREGATIONS = ("max", "sum_top3")

#: The applied aggregation, DECIDED BY MEASUREMENT, not taste.
#:
#: The deciding experiment is the development set's own first queries, at the
#: arm's own requested limits, replayed at the index level under both rules
#: against the pinned erag-seg generation. The preserved PR #1333 benchmark
#: record is pinned by `tests/bench/test_erag_rollup_aggregation.py`. Measured
#: gold-in-first-page over the 47 gold documents of that set:
#:
#:     document-level (today)   26/47
#:     rollup, max              33/47
#:     rollup, sum_top3         30/47
#:
#: `max` wins the table by three, so the choice is made on evidence rather than
#: on the tie-break. It is ALSO the rule whose failure mode (ignoring breadth)
#: is legible, whereas `sum_top_k`'s length bias is a silent systematic
#: distortion and `k` would be a second unjustified constant — but that argument
#: is now the corroboration, not the decision.
#:
#: Under `max`, 8 questions strictly improve (`qst_0118` gold rank 49 -> 1,
#: `qst_0358` not-in-top-200 -> 3, plus 0198/0280/0341/0348/0354/0428) and 2
#: regress by one gold document each (`qst_0402` 2 -> 1, `qst_0420` 2 -> 1).
#: The regressions are enumerated rather than netted away.
PASSAGE_ROLLUP_AGGREGATION = "max"

#: Chunk rows one rollup may rank. The ranked unit is the chunk, so this is a
#: CHUNK budget and it is NOT `_LEXICAL_RANK_CAP` (a document budget): measured
#: on erag-seg, 2,000 chunks yield 976–1,642 distinct documents, comfortably
#: more than any page, while 200 would cap the document count below a deep
#: page. 2,000 rows off the BM25 index's ordered scan is the same budget
#: `_ATTR_SCOPE_CANDIDATE_CAP` was measured at (~0.5 s).
PASSAGE_ROLLUP_CHUNK_CAP = 2000

#: Raw matches a policy-enforced rollup may inspect while assembling the first
#: readable chunk page. If this prefix does not establish an exact readable
#: ordering, the operation refuses rather than letting hidden rows evict later
#: readable rows or change a public cap flag.
PASSAGE_ROLLUP_POLICY_SCAN_CAP = 8000

#: Declared child-to-parent edge associations one rollup may materialize.
#: A passage relation is not assumed functional by the catalog, so the edge
#: fan-out needs its own bound independent of the chunk budget.
PASSAGE_ROLLUP_RELATION_CAP = 8000


class PassageRollupUndeclared(SubstrateError):
    """`rank_by='passages'` on a subtype with no `okg_passage_rollup`.

    RAISES rather than degrading to self-ranking. On a corpus whose parent
    subtype indexes only locator text, a silent degrade hands back a filename
    search the caller reads as a content search — the exact defect the rollup
    exists to fix, restated as a fallback.
    """

    code = "passage_rollup_undeclared"


class RankByNotApplicable(SubstrateError):
    """A `rank_by` mode the chosen search method cannot run.

    Refused rather than accepted and discarded: a discarded mode would hand
    back a page ranked another way to a caller who believes it ran.
    """

    code = "rank_by_not_applicable"


class PassageRollupSubtypeRequired(SubstrateError):
    """`rank_by='passages'` without naming the parent subtype to roll up.

    The rollup ranks one parent subtype through one declared relation. With no
    subtype named there is no parent to attribute chunk scores to, and picking
    one — say, the only subtype that happens to declare a rollup today — would
    make the operator's meaning depend on the catalog's current contents.
    """

    code = "passage_rollup_subtype_required"


class PassageRollupOffsetUnordered(SubstrateError):
    """An `offset` past the documents the chunk budget produced.

    Refused rather than answered with an empty page. Document ranks beyond the
    chunk budget are not merely missing, they are UNORDERED: a document all of
    whose chunks fell outside the budget has no score at all, so "the next page"
    does not exist to be returned. Same argument `SubtypeOutsideProfile` makes
    for refusing an empty intersection.
    """

    code = "passage_rollup_offset_unordered"


class PassageRollupPolicyScanExhausted(SubstrateError):
    """Policy filtering could not establish an exact bounded chunk prefix."""

    code = "passage_rollup_policy_scan_exhausted"


class PassageRollupRelationBudgetExceeded(SubstrateError):
    """The declared passage relation exceeds the bounded association budget."""

    code = "passage_rollup_relation_budget_exceeded"


@dataclass(frozen=True)
class PassageRollup:
    """One parent subtype's declared passage relation, from the pinned catalog."""

    parent_subtype: str
    edge_type: str
    child_subtype: str


def validate_rank_by(
    rank_by: Any, *, method: str, max_per_parent_argument: bool = False,
) -> str:
    """Validate the `rank_by` argument and resolve its default. Pure.

    Unset, lexical search ranks `records` and every other method `self`. A
    lexical call that passes an explicit `max_per_parent` asked for per-parent
    sibling rows, which only `self` returns, so its default is `self`.

    A rollup or a fold has no defined meaning for a method that does not rank
    text over child rows, so naming one there RAISES instead of being accepted
    and discarded; a discarded mode would hand back a self-ranked page to a
    caller who believes it was rolled up.
    """
    if method == "grep":
        # Grep's results are always records (PACT mcp-grep-3067), ranked by
        # BM25 on the query's literal words (PACT mcp-grep-rank), so
        # `records` is the one legal value.
        if rank_by in (None, RANK_BY_RECORDS):
            return RANK_BY_RECORDS
        raise RankByNotApplicable(
            f"search: method='grep' ranks records only; rank_by must be 'records' "
            f"or unset, got {rank_by!r}",
            data={"method": method, "rank_by": rank_by},
            fix_hint="drop rank_by, or pass rank_by='records'",
        )
    if rank_by is None:
        if method == "lexical" and not max_per_parent_argument:
            return RANK_BY_RECORDS
        return RANK_BY_SELF
    if rank_by not in RANK_BY_VALUES:
        raise ValueError(
            f"search: rank_by must be one of {list(RANK_BY_VALUES)}; got {rank_by!r}"
        )
    if rank_by in (RANK_BY_PASSAGES, RANK_BY_RECORDS) and method != "lexical":
        raise RankByNotApplicable(
            f"search: rank_by={rank_by!r} applies only to method='lexical'; "
            f"got method={method!r}. It folds child rows ranked by BM25 into "
            f"their parent; no other method scores child text.",
            data={"method": method, "rank_by": rank_by},
            fix_hint="drop rank_by, or use method='lexical'",
        )
    return str(rank_by)


def passage_rollup_declaration(
    conn: psycopg.Connection,
    *,
    catalog_version_id: int,
    subtype: str,
) -> PassageRollup:
    """The parent subtype's declared passage relation, or RAISE.

    Read from `okg.subtypes.metadata->'passage_rollup'` at the pinned catalog
    version, which the composer populates from the `okg_passage_rollup`
    annotation and validates against the composed narrowings.

    **Never inferred.** The pinned erag-seg catalog declares FOUR candidate
    relations between `document` and `document_chunk`:

        document        contains      document_chunk
        document        contains      visual_artifact
        document_chunk  member_of     document
        document_chunk  references    document

    so "the edge that relates a passage to its parent" is not uniquely
    derivable. Guessing `contains` would roll figures into a document's
    relevance and guessing `references` would roll citations. The
    `doc_path`/`path` attribute join is likewise REJECTED: it is a string
    coincidence of one corpus adapter, it carries no catalog meaning, and it
    breaks the moment two sources use the same path in different repos.
    """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT name, metadata -> 'passage_rollup' AS rollup
              FROM okg.subtypes
             WHERE catalog_version_id = %(cv)s AND name = %(name)s
            """,
            {"cv": catalog_version_id, "name": subtype},
        )
        row = cur.fetchone()
        cur.execute(
            """
            SELECT name FROM okg.subtypes
             WHERE catalog_version_id = %(cv)s
               AND metadata ? 'passage_rollup'
             ORDER BY name
            """,
            {"cv": catalog_version_id},
        )
        declared_elsewhere = [str(r["name"]) for r in cur.fetchall()]

    rollup = row["rollup"] if row is not None else None
    if (
        not isinstance(rollup, dict)
        or not rollup.get("edge_type")
        or not rollup.get("child_subtype")
    ):
        raise PassageRollupUndeclared(
            f"subtype {subtype!r} declares no `okg_passage_rollup` at catalog "
            f"version {catalog_version_id}, so rank_by='passages' has no "
            f"relation to roll up through. Subtypes that DO declare one: "
            f"{declared_elsewhere or 'none'}. Refusing rather than ranking "
            f"{subtype!r} by its own text: on a subtype whose indexed text is "
            f"a locator, that would return a filename search a caller cannot "
            f"tell apart from a content search.",
            data={
                "subtype": subtype,
                "catalog_version_id": catalog_version_id,
                "subtypes_with_passage_rollup": declared_elsewhere,
                "declared": rollup,
            },
            fix_hint=(
                "declare `okg_passage_rollup: {edge_type, child_subtype}` on "
                f"the {subtype!r} class and recompose the catalog, or drop "
                "rank_by='passages'"
            ),
        )
    return PassageRollup(
        parent_subtype=subtype,
        edge_type=str(rollup["edge_type"]),
        child_subtype=str(rollup["child_subtype"]),
    )


def assert_rollup_offset_ordered(
    *,
    offset: int,
    total_count: int,
    subtype: str,
    chunk_cap_reached: bool,
) -> None:
    """Refuse an offset past the documents the chunk budget actually ordered.

    `offset == total_count` is already past the last ordered document, so it is
    refused too: the caller is asking for rank N+1 of a list that has N ranked
    entries, and beyond the budget there is no rank.
    """
    if offset <= 0 or offset < total_count:
        return
    raise PassageRollupOffsetUnordered(
        f"offset {offset} is past the {total_count} {subtype!r} row(s) the "
        f"{PASSAGE_ROLLUP_CHUNK_CAP}-chunk rollup budget ordered"
        f"{' (the budget was saturated)' if chunk_cap_reached else ''}. "
        f"Documents beyond the budget are not merely missing, they are "
        f"UNORDERED: a document whose chunks all fell outside the budget has "
        f"no score at all, so an empty page here would be indistinguishable "
        f"from 'there is nothing more to rank'.",
        data={
            "offset": offset,
            "total_count": total_count,
            "subtype": subtype,
            "chunk_cap": PASSAGE_ROLLUP_CHUNK_CAP,
            "rollup_chunk_cap_reached": chunk_cap_reached,
        },
        fix_hint=(
            "page within the ordered documents, or narrow the query so the "
            "chunk budget covers the region you are paging into"
        ),
    )


class PassageRollupEdgeSegmentUnavailable(SubstrateError):
    """The chronos edge segment could not be located.

    Raises rather than returning the un-rolled page: without the edge segment
    there is no relation to attribute chunk scores through, and answering with
    the parent subtype's own (locator) ranking is the silent degrade this
    module exists to refuse.
    """

    code = "passage_rollup_edge_segment_unavailable"


def resolve_edge_segment(conn: psycopg.Connection) -> str:
    """The physical chronos table holding live edge rows.

    Read from `_chronos_branch_tables` — the same registry
    `semantic_readiness.resolve_node_segment` uses — rather than from
    `pg_indexes`, because the edge segment carries no BM25 index to key off.
    """
    from okg.substrate.graph_store_internal.common import EDGE_TABLE

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT physical_table
                  FROM _chronos_branch_tables
                 WHERE backend = 'interval' AND table_name = %s
                """,
                (EDGE_TABLE,),
            )
            row = cur.fetchone()
    except psycopg.Error as exc:
        raise PassageRollupEdgeSegmentUnavailable(
            f"the chronos edge segment registry is unreadable: {exc}",
            fix_hint="run `okg doctor` on this deployment",
        ) from exc
    if row is None or not row[0]:
        raise PassageRollupEdgeSegmentUnavailable(
            "the chronos edge segment is not registered, so the passage "
            "relation a rollup ranks through cannot be located",
            fix_hint=(
                "publish a generation on this deployment, then retry; "
                "`okg doctor` reports segment registration"
            ),
        )
    return str(row[0])


#: Chunks whose scores may be summed under `sum_top3`.
_SUM_TOP_K = 3


def ranked_chunk_sql(
    *,
    node_segment: str,
    order_sql: str,
    extra_child_where: tuple[str, ...] = (),
    narrow: bool = False,
    count_inner: bool = False,
) -> str:
    """The rollup's ranked chunk scan: the match test OUTSIDE the scan.

    Built by a named function rather than inline, so the shape can be asserted
    without a database and without a rollup declaration -- okg-workspace
    declares exactly one (`document` -> `document_chunk`) and its pinned
    generation holds zero rows of either subtype, so the composed path cannot
    be exercised there at all.

    THE MATCH TEST IS NOT A WHERE PREDICATE. BM25 `ORDER BY <@> LIMIT` pads a
    short page with 0-score non-matches, so the non-matches have to be dropped
    somewhere; written as `AND {order_sql} < 0` inside the ranked scan it is a
    restriction the planner costs on its own, which is the shape that cost
    `_search_lexical` the BM25 index entirely (issue #1924). Here the scan
    always carries `subtype = %(rollup_child_subtype)s`, so the planner has a
    selective btree either way and the index is not lost -- but the inner
    predicate still makes the planner mis-estimate the page and sort it on
    disk. Measured on okg-workspace 2026-09-22 against the 695,052-record
    `default` branch, over the populated `code_symbol` subtype at the
    2,001-row policy-scan cap, three queries each:

        inner predicate   2.87 / 2.82 / 2.87 s, external merge sort, 9,912 kB
                          spilled to disk, 219,684 shared buffer hits
        outer test        2.62 / 2.63 / 2.70 s, top-N heapsort in 3,883 kB,
                          199,983 shared buffer hits

    The two forms return the SAME rows in the SAME order: scores sort
    ascending and a scored match is negative, so scored matches fill the
    capped page before any 0-score row can enter it. Verified by comparing
    ordered node-id lists for those three queries against the live
    deployment -- all three identical, all three at the saturated cap, which
    is the case where a mis-ordered cap would show.

    A 0-score row is NOT always a pad (issue #3232): pg_textsearch can score a
    real match 0 on a stale term statistic, and `RANKED_BM25_MATCH` keeps it
    by a word test. Such matches sort among the 0-score pads by `node_id`, so
    pads can take LIMIT slots ahead of a later 0-score match, and an outer
    result shorter than its LIMIT does not mean the matches ran out.
    `count_inner=True` therefore also returns `rollup_inner_rows`, the number
    of rows the inner LIMITed scan produced before the match test, on every
    row, plus one row with a null `node_id` when no row matched; the rollup
    treats only an inner read shorter than its LIMIT as exhausted (PACT
    passage-rollup-scan-cost). The match test runs directly over the ordered
    LIMIT read (which keeps the BM25 index's ordered scan), and only its
    result is kept: the MATERIALIZED `scan` CTE holds `node_id`, `score` and
    a `matched` flag on the narrow path, and on the full path the row columns
    with `attrs` nulled for pads, so it stays small enough not to spill.

    `extra_child_where` carries the caller's already-parameterized selection
    predicates. They stay in the INNER scan: they select which chunks may spend
    the budget, which is a different job from dropping BM25's padding.

    `narrow=True` returns only `node_id` and `score`, the columns ranking and
    the edge read need (PACT passage-rollup-scan-cost). The inner scan still
    carries `attrs`, because the outer match test reads its text; the server
    then never ships a chunk's attrs (about 2.3 kB of JSON each on comp-ops)
    to the client. Only a caller whose read filter inspects nothing -- Nomos
    off -- may ask for it; `fill_passage_rows` reads the rest of each
    winning chunk afterwards.
    """
    extra_sql = "".join(
        f"\n               AND {clause}" for clause in extra_child_where
    )
    outer = "ranked.node_id, ranked.score" if narrow else "ranked.*"
    inner = f"""
            SELECT node_id, subtype, attrs, text_hash, source,
                   source_record_id, source_revision,
                   {order_sql} AS score
              FROM {node_segment} n
             WHERE deleted = FALSE
               AND live_lo <= %(bp)s AND %(bp)s < live_hi
               AND attrs ? 'text'
               AND subtype = %(rollup_child_subtype)s{extra_sql}
             ORDER BY {order_sql}, node_id
             LIMIT %(rollup_raw_limit)s"""
    if not count_inner:
        return f"""
        SELECT {outer} FROM ({inner}
        ) AS ranked
         WHERE {RANKED_BM25_MATCH}
    """
    if narrow:
        kept = "ranked.node_id, ranked.score"
        served = "scan.node_id, scan.score"
    else:
        # A pad's attrs are never used; only a match's row reaches a filter.
        kept = (
            "ranked.node_id, ranked.subtype, "
            f"CASE WHEN {RANKED_BM25_MATCH} THEN ranked.attrs END AS attrs, "
            "ranked.text_hash, ranked.source, ranked.source_record_id, "
            "ranked.source_revision, ranked.score"
        )
        served = (
            "scan.node_id, scan.subtype, scan.attrs, scan.text_hash, "
            "scan.source, scan.source_record_id, scan.source_revision, scan.score"
        )
    return f"""
        WITH scan AS MATERIALIZED (
            SELECT {kept}, {RANKED_BM25_MATCH} AS matched
              FROM ({inner}
              ) AS ranked
        ), inner_size AS (
            SELECT count(*) AS rollup_inner_rows FROM scan
        )
        SELECT matched_rows.*, inner_size.rollup_inner_rows
          FROM inner_size
          LEFT JOIN LATERAL (
            SELECT {served} FROM scan WHERE scan.matched
          ) AS matched_rows ON TRUE
         ORDER BY matched_rows.score, matched_rows.node_id
    """



#: Columns of a full chunk or parent row, in the order the full scan returns
#: them; `fill_passage_rows` rebuilds a narrow row in this same order.
_NODE_ROW_COLUMNS = (
    "node_id", "subtype", "attrs", "text_hash", "source",
    "source_record_id", "source_revision",
)


class PassageRollupChunkRowsUnavailable(SubstrateError):
    """A winning chunk the narrow scan ranked is absent from its row read.

    Both reads name the same pinned branch point, so this means the pin is
    not holding still. Raised rather than serving a passage with no text.
    """

    code = "passage_rollup_chunk_rows_unavailable"


_ReadKey = Callable[[str, str, Mapping[str, Any]], Hashable]


def _fetch(
    conn: psycopg.Connection, sql: str, params: Mapping[str, Any], *,
    custom_plan: bool = False,
) -> list[dict[str, Any]]:
    """Run one rollup statement and return its rows (one seam for counting)."""
    with conn.cursor(row_factory=dict_row) as cur:
        if custom_plan:
            execute_with_custom_plan(cur, sql, dict(params))
        else:
            cur.execute(sql, dict(params))
        return cur.fetchall()


def _once(
    read_key: Optional[_ReadKey], kind: str, sql: str, params: Mapping[str, Any],
    read: Callable[[], T],
) -> T:
    """`read()`, at most once per key inside the caller's per-call cache.

    Without a `read_key` every read runs. The key comes from the caller
    (`search._read_key`): the database, the pinned session, the statement,
    its parameters and the completeness mode (PACT passage-rollup-scan-cost,
    read-once). Only stored rows are cached; Nomos and paging rerun.
    """
    if read_key is None:
        return read()
    return cached_read(read_key(kind, sql, params), read)


def passage_rollup_candidates(
    conn: psycopg.Connection,
    *,
    session: Session,
    rollup: PassageRollup,
    node_segment: str,
    edge_segment: str,
    order_sql: str,
    params: dict[str, Any],
    aggregation: str = PASSAGE_ROLLUP_AGGREGATION,
    chunk_cap: int = PASSAGE_ROLLUP_CHUNK_CAP,
    policy_scan_cap: int = PASSAGE_ROLLUP_POLICY_SCAN_CAP,
    relation_cap: int = PASSAGE_ROLLUP_RELATION_CAP,
    extra_child_where: tuple[str, ...] = (),
    complete_read: Optional[Callable[..., tuple[list[dict[str, Any]], Any]]] = None,
    record_read: Optional[Callable[..., None]] = None,
    runtime_required: Optional[bool] = None,
    read_key: Optional[_ReadKey] = None,
    text_redaction: bool = False,
) -> tuple[list[dict[str, Any]], int, bool]:
    """Rank the declared child rows, then attribute their scores to parents.

    Returns `(rows, readable_chunks_scanned, readable_chunk_cap_reached)`. Each row
    has the same
    shape the unscoped lexical candidate query returns — `node_id, subtype,
    attrs, text_hash, source, score` — so the whole downstream pipeline (nomos
    read filter, effective-attrs overlay, `attr_filters`, alias promotion,
    fencing, pagination) is reused unchanged; that is the property that keeps
    this a reviewable branch rather than a second search implementation. Two
    extra keys ride along for the envelope: `rollup_chunk_count` (how many of
    the budgeted chunks this parent contributed) and `rollup_best_chunk_id`,
    plus `rollup_best_chunk`, the winning readable passage row, which the
    caller must remove before the parent rows enter its read pipeline.

    The raw chunk scan is ordered by score, so the budget is spent on the
    best-matching passages in the corpus. Before a passage can contribute its
    score or identity, the existing Nomos filter is applied to the full
    passage row. The declared relation edges are filtered through the same
    session authority before they can attribute a readable passage to a
    parent. This ordering is deliberate: applying policy only to the returned
    parents would let a hidden passage change a visible parent's score, rank,
    count, and evidence.

    **How much is scanned** (PACT passage-rollup-scan-cost). The result is
    the first `chunk_cap` readable chunks in `(score, node_id)` order, a total
    order, so any read that holds `chunk_cap + 1` readable chunks decides it:

    * The scan reads growing windows, `chunk_cap + 1` first and doubling,
      each re-reading from rank 1 with no OFFSET, and stops at the first
      window holding `chunk_cap + 1` readable chunks, at a window whose INNER
      read (before the match test drops 0-score pads, see `ranked_chunk_sql`)
      is shorter than its LIMIT -- the only proof the matches ran out -- or at
      `policy_scan_cap + 1`, where today's refusal applies unchanged. A
      window whose inner read held a pad (fewer matched rows than inner
      rows) jumps straight to the bound: every later row scores 0 or more,
      so a low-match query reads `chunk_cap + 1` then `policy_scan_cap + 1` rows.
    * `runtime_required=False` (Nomos off: the filter hides nothing) usually
      stops at the first window; it grows only while pads fill a window. Its
      rows are narrow (`ranked_chunk_sql(narrow=True)`): the winning chunks'
      rows are read later by `fill_passage_rows`.
    * Audit or enforce, both of which can hide a row, or a caller that does
      not say, read full rows. Each window filters only the rows past the
      previous one, so a chunk's audit decision is normally recorded once.
      If a window's earlier rows differ from the previous window's in
      `(node_id, score)` (the BM25 statistics moved between statements), the
      window is filtered again from rank 1, and those rows' audit decisions
      are then recorded a second time.
    * `complete_read` is given only under `completeness="exact"`; it keeps
      today's single `policy_scan_cap + 1` read, so the exact-read
      disclosures and saturation flags do not change.

    Only the final read is passed to `record_read` (the coverage cap's
    record), so an earlier filled window never sets a saturation flag.

    `readable_chunks_scanned` and the cap flag are intentionally post-policy,
    so the envelope does not disclose how many matching passages were hidden.
    If the bounded raw prefix cannot establish an exact first readable page,
    the operation refuses rather than return policy-distorted ordering.

    `order_sql` and `params` come from the caller so the BM25 index name, the
    query text and the branch point are resolved exactly once, in
    `_search_lexical`. `runtime_required` is the caller's single reading of
    the deployment's Nomos mode for this call; None lets each filter call
    resolve it and keeps the windowed, full-row read.

    `extra_child_where` carries the caller's already-parameterized selection
    predicates (the source-family filter and the profile's path exclusions,
    per `mcp-authority-diversity-budgets`) into the CHUNK scan. They belong on
    the chunks and not on the parents: the chunk is the ranked unit here, so an
    out-of-family chunk left in the scan would spend the chunk budget and could
    attribute its score to a parent the caller excluded.

    `complete_read(rows, where=..., limit=..., params=...) -> (rows, count)`
    reads the ranked chunks again if the BM25 index's result cap may have
    cut them (`search._IndexScanCap`, PACT lexical-index-scan-cap-3162);
    `record_read(rows, limit=..., count=...)` records the final read.
    `text_redaction` is the caller's reading of whether the active Nomos
    policy can redact (`readable_text.redaction_possible`): when true, a
    chunk counts only when the reader reads its text as stored (#3377).
    `read_key(kind, sql, params)` keys the chunk, edge and parent reads in
    the per-call read cache; the cached chunk read carries its count, and
    `record_read` runs on every call, cached or not.
    """
    if aggregation not in PASSAGE_ROLLUP_AGGREGATIONS:
        raise ValueError(
            f"passage rollup aggregation must be one of "
            f"{list(PASSAGE_ROLLUP_AGGREGATIONS)}; got {aggregation!r}"
        )
    if not 0 < chunk_cap <= PASSAGE_ROLLUP_CHUNK_CAP:
        raise ValueError(
            f"passage rollup chunk_cap must be in "
            f"[1, {PASSAGE_ROLLUP_CHUNK_CAP}]; got {chunk_cap}"
        )
    if not chunk_cap <= policy_scan_cap <= PASSAGE_ROLLUP_POLICY_SCAN_CAP:
        raise ValueError(
            "passage rollup policy_scan_cap must be between chunk_cap and "
            f"{PASSAGE_ROLLUP_POLICY_SCAN_CAP}; "
            f"got policy_scan_cap={policy_scan_cap}, chunk_cap={chunk_cap}"
        )
    if not 0 < relation_cap <= PASSAGE_ROLLUP_RELATION_CAP:
        raise ValueError(
            f"passage rollup relation_cap must be in "
            f"[1, {PASSAGE_ROLLUP_RELATION_CAP}]; got {relation_cap}"
        )
    scan_bound = policy_scan_cap + 1
    narrow = runtime_required is False and complete_read is None
    # A narrow scan reads no text; it runs only with Nomos off, where the
    # read filter changes nothing.
    redaction = runtime_required is not False and text_redaction
    ranked_sql = ranked_chunk_sql(
        node_segment=node_segment,
        order_sql=order_sql,
        extra_child_where=extra_child_where,
        narrow=narrow,
        count_inner=True,
    )
    complete_where = [
        "live_lo <= %(bp)s", "%(bp)s < live_hi",
        "subtype = %(rollup_child_subtype)s", *extra_child_where,
    ]

    def window(limit: int) -> tuple[list[dict[str, Any]], int, Any]:
        """`(matched rows, inner rows before the match test, exact count)`."""
        scored = dict(params)
        scored["rollup_child_subtype"] = rollup.child_subtype
        scored["rollup_edge_type"] = rollup.edge_type
        scored["rollup_raw_limit"] = limit

        def read() -> tuple[list[dict[str, Any]], int, Any]:
            fetched = _fetch(conn, ranked_sql, scored, custom_plan=True)
            inner_rows = int(fetched[0]["rollup_inner_rows"]) if fetched else 0
            rows = []
            for row in fetched:
                row.pop("rollup_inner_rows")
                if row["node_id"] is not None:
                    rows.append(row)
            if complete_read is None:
                return rows, inner_rows, None
            rows, count = complete_read(
                rows, where=complete_where, limit=limit, params=scored,
            )
            return rows, inner_rows, count

        return _once(read_key, "rollup_chunks", ranked_sql, scored, read)

    if complete_read is not None:
        limit = scan_bound
    else:
        limit = min(chunk_cap + 1, scan_bound)
    readable_chunks: list[dict[str, Any]] = []
    filtered_ids: list[tuple[str, Any]] = []
    while True:
        raw_chunks, inner_rows, count = window(limit)
        considered = raw_chunks[:policy_scan_cap]
        ids = [(str(row["node_id"]), row["score"]) for row in considered]
        if ids[: len(filtered_ids)] != filtered_ids:
            readable_chunks, filtered_ids = [], []
        fresh = considered[len(filtered_ids):]
        # A chunk counts only when the reader reads its text as stored
        # (#3377): a hit on masked or dropped text is dropped before it can
        # contribute a score, a parent or a chunk count.
        readable_chunks.extend(keep_readable_text(
            fresh,
            filter_node_rows_for_session(
                conn,
                session,
                fresh,
                runtime_required=runtime_required,
            ),
            redaction=redaction,
        ))
        filtered_ids = ids
        final = (
            # Only a short INNER read proves the matches ran out: 0-score
            # pads can make the matched rows short while a later 0-score
            # match still exists (issue #3232).
            inner_rows < limit
            or limit >= scan_bound
            or len(readable_chunks) > chunk_cap
        )
        if final:
            break
        if len(raw_chunks) < inner_rows:
            # A pad entered this window, so every later row scores 0 or
            # more: doubling would read more padded windows. Read the bound
            # next; that read is exactly today's read.
            limit = scan_bound
        else:
            limit = min(limit * 2, scan_bound)
    if record_read is not None:
        record_read(raw_chunks, limit=limit, count=count)

    raw_chunk_budget_reached = len(raw_chunks) > policy_scan_cap
    if (
        raw_chunk_budget_reached
        and len(readable_chunks) <= chunk_cap
    ):
        raise PassageRollupPolicyScanExhausted(
            "passage rollup could not establish an exact readable ordering "
            "within its bounded policy scan; refusing rather than letting "
            "hidden matching passages evict later readable passages or "
            "change public rank/count state",
            data={
                "subtype": rollup.child_subtype,
                "chunk_cap": chunk_cap,
                "policy_scan_cap": policy_scan_cap,
            },
            fix_hint=(
                "narrow the lexical query or reduce policy-hidden matches; "
                "the rollup will not return a policy-distorted partial page"
            ),
        )
    chunk_cap_reached = len(readable_chunks) > chunk_cap
    readable_chunks = readable_chunks[:chunk_cap]
    if not readable_chunks:
        return [], 0, chunk_cap_reached

    readable_by_id = {str(row["node_id"]): row for row in readable_chunks}
    edge_params = {
        "bp": params["bp"],
        "rollup_edge_type": rollup.edge_type,
        "rollup_child_ids": sorted(readable_by_id),
        "rollup_relation_limit": relation_cap + 1,
    }
    edge_sql = f"""
        SELECT edge_id, src, dst, edge_type, attrs, source,
               source_record_id, source_revision
          FROM {edge_segment}
         WHERE dst = ANY(%(rollup_child_ids)s)
           AND edge_type = %(rollup_edge_type)s
           AND deleted = FALSE
           AND live_lo <= %(bp)s AND %(bp)s < live_hi
         ORDER BY edge_id
         LIMIT %(rollup_relation_limit)s
    """
    raw_edges = _once(
        read_key, "rollup_edges", edge_sql, edge_params,
        lambda: _fetch(conn, edge_sql, edge_params),
    )
    if len(raw_edges) > relation_cap:
        raise PassageRollupRelationBudgetExceeded(
            "the declared passage relation exceeds the bounded association "
            "budget; refusing rather than truncate parent attribution",
            data={
                "edge_type": rollup.edge_type,
                "child_subtype": rollup.child_subtype,
                "relation_cap": relation_cap,
            },
            fix_hint=(
                "narrow the query or correct unexpected passage-to-parent "
                "fan-out before retrying"
            ),
        )
    readable_edges = filter_edge_rows_for_session(
        conn,
        session,
        raw_edges,
        runtime_required=runtime_required,
    )

    scores_by_parent: dict[str, list[tuple[Any, str]]] = {}
    for edge in readable_edges:
        child_id = str(edge["dst"])
        child = readable_by_id.get(child_id)
        if child is None:
            continue
        scores_by_parent.setdefault(str(edge["src"]), []).append(
            (child["score"], child_id)
        )
    if not scores_by_parent:
        return [], len(readable_chunks), chunk_cap_reached

    parent_params = {
        "bp": params["bp"],
        "rollup_parent_subtype": rollup.parent_subtype,
        "rollup_parent_ids": sorted(scores_by_parent),
    }
    parent_sql = f"""
        SELECT node_id, subtype, attrs, text_hash, source,
               source_record_id, source_revision
          FROM {node_segment}
         WHERE node_id = ANY(%(rollup_parent_ids)s)
           AND subtype = %(rollup_parent_subtype)s
           AND deleted = FALSE
           AND live_lo <= %(bp)s AND %(bp)s < live_hi
         ORDER BY node_id
    """
    parents = _once(
        read_key, "rollup_parents", parent_sql, parent_params,
        lambda: _fetch(conn, parent_sql, parent_params),
    )

    rows: list[dict[str, Any]] = []
    for parent in parents:
        contributions = sorted(
            scores_by_parent[str(parent["node_id"])],
            key=lambda item: (item[0], item[1]),
        )
        score = contributions[0][0]
        if aggregation == "sum_top3":
            score = sum(
                (item[0] for item in contributions[:_SUM_TOP_K]),
                start=0,
            )
        rows.append(
            {
                **dict(parent),
                "score": score,
                "rollup_chunk_count": len(contributions),
                "rollup_best_chunk_id": contributions[0][1],
                # The winning passage row itself, post-Nomos and with its
                # attrs object untouched (a redacted-attrs type must survive
                # to the effective-attrs pass), so the caller can preview the
                # passage instead of the parent's locator text (issue #2670).
                # A narrow scan's row holds only `node_id` and `score` until
                # `fill_passage_rows` reads the rest for the returned page.
                "rollup_best_chunk": readable_by_id[contributions[0][1]],
            }
        )
    rows.sort(key=lambda row: (float(row["score"]), str(row["node_id"])))
    return rows, len(readable_chunks), chunk_cap_reached


def fill_passage_rows(
    conn: psycopg.Connection,
    chunks: Iterable[dict[str, Any]],
    *,
    node_segment: str,
    child_subtype: str,
    branch_point_value: Any,
    read_key: Optional[_ReadKey] = None,
) -> None:
    """Complete, in place, the narrow winning-chunk rows a page returns.

    A narrow scan (Nomos off) ranked chunks on `node_id` and `score` alone;
    the page needs each returned parent's winning chunk whole, for its
    excerpt, location and producer trust. This reads those rows once and
    rebuilds each dict in the full scan's column order, `score` last, so the
    served page is the page a full scan serves. Rows that already carry
    `attrs` (a full scan, possibly redacted by Nomos) are never re-read.
    """
    pending: dict[str, list[dict[str, Any]]] = {}
    for chunk in chunks:
        if "attrs" not in chunk:
            pending.setdefault(str(chunk["node_id"]), []).append(chunk)
    if not pending:
        return
    fill_params = {
        "bp": branch_point_value,
        "rollup_child_subtype": child_subtype,
        "rollup_chunk_ids": sorted(pending),
    }
    fill_sql = f"""
        SELECT {", ".join(_NODE_ROW_COLUMNS)}
          FROM {node_segment}
         WHERE node_id = ANY(%(rollup_chunk_ids)s)
           AND subtype = %(rollup_child_subtype)s
           AND deleted = FALSE
           AND live_lo <= %(bp)s AND %(bp)s < live_hi
         ORDER BY node_id
    """
    found = {
        str(row["node_id"]): row
        for row in _once(
            read_key, "rollup_chunk_rows", fill_sql, fill_params,
            lambda: _fetch(conn, fill_sql, fill_params),
        )
    }
    missing = sorted(set(pending) - set(found))
    if missing:
        raise PassageRollupChunkRowsUnavailable(
            f"{len(missing)} winning passage row(s) the rollup ranked are not "
            "live at the pinned branch point",
            data={"chunk_ids": missing[:10], "subtype": child_subtype},
            fix_hint="retry the search; run `okg doctor` if it repeats",
        )
    for node_id, targets in pending.items():
        row = found[node_id]
        for chunk in targets:
            score = chunk["score"]
            chunk.clear()
            chunk.update({column: row[column] for column in _NODE_ROW_COLUMNS})
            chunk["score"] = score


# ---------- child -> canonical parent resolution (issue #1444) ----------
#
# The rollup above answers "rank these parents by their children". This answers
# the inverse question a CITING agent actually has: "this passage is my
# evidence; which document do I cite?". A `document_chunk` hit carries
# `canonical_node_id` equal to its own id — an alias self-reference, not a
# parent — so without this the parent identity is simply not on the card, and
# an agent either cites the chunk id or invents a document.
#
# The relation is read from the SAME pinned catalog declaration the rollup
# ranks through, inverted: a subtype is a passage CHILD when some parent
# subtype declares `okg_passage_rollup` naming it. Nothing here infers a parent
# from a path attribute, a title, or an edge-type name, and nothing composes a
# citation the parent does not carry.

#: `parent` block states. A row's `parent` is `null` ONLY when the pinned
#: catalog declares no passage relation for its subtype — a fact about the
#: ontology. Every other outcome is an object naming which outcome it is,
#: because "no parent edge at this pin" and "no such relation exists" send an
#: operator to two different places.
PARENT_STATE_RESOLVED = "resolved"
#: A parent listed inside `multiple_parents`. Distinct from `resolved` on
#: purpose: nothing chose it, so nothing may cite it as the parent.
PARENT_STATE_CANDIDATE = "candidate"
PARENT_STATE_UNLINKED = "declared_but_unlinked"
#: RETIRED (issue #2753): no longer emitted, kept so a client that matches on
#: it still imports. Several declarations naming one child subtype are not an
#: ambiguity: each declares a distinct `(parent_subtype, edge_type)` pair, so
#: the subtype of the node an edge actually reaches picks the declaration.
PARENT_STATE_AMBIGUOUS_DECLARATION = "ambiguous_declaration"
PARENT_STATE_MULTIPLE_PARENTS = "multiple_parents"
PARENT_STATE_BOUNDED_UNKNOWN = "bounded_unknown"

#: Where the relation came from, echoed on every block so a caller never has to
#: assume the read side picked the edge.
PARENT_RELATION_SOURCE = "catalog:okg_passage_rollup"
#: Where a parent's citation label came from. There is exactly one legal answer
#: and it is the producer's own attribute: `source-supplied-search-citations`
#: forbids the read side from composing a citation, and this change does not
#: relax that for parents.
PARENT_LABEL_SOURCE = "producer_attrs:citation_label"

#: Association rows one page's parent resolution may inspect. A page is at most
#: the search limit cap and a passage relation is not declared functional, so
#: the fan-out needs its own bound. Ordered by `dst`, so a saturated scan
#: leaves a PREFIX of children decided exactly and the rest explicitly unknown
#: — never a silent "no parent".
PARENT_EDGE_SCAN_CAP = 4000

# ---------- compact parent summary and passage position (issue #2675) ----------
#
# A resolved parent used to say WHICH record it is (id, subtype, citation) but
# not WHAT it is, and a passage never said where it sits inside that record, so
# an agent spent one more call per hit to find out. These fields are the
# smallest summary that answers both. Every one of them is bounded, because the
# card is paid for on every row of every page (issue #2674).

#: Attribute keys a parent's `title` is read from; the first non-empty,
#: unmasked string wins and `title_attr` names the key that answered, so the
#: precedence is never hidden. `summary` is an issue tracker's one-line title;
#: `key` and `change_id` are record keys. `path` is deliberately absent: a
#: filesystem path is not a title, and the citation already names the record.
PARENT_TITLE_ATTRS: tuple[str, ...] = (
    "title", "summary", "name", "display_name", "label", "key", "change_id",
)
PARENT_TITLE_MAX_CHARS = 120
PARENT_STATUS_MAX_CHARS = 40

#: A passage's own position attributes, in precedence order. Both are the
#: producer's 0-based index of the passage within its record; nothing here
#: derives an index from an id, an offset, or an ordering of siblings.
POSITION_INDEX_ATTRS: tuple[str, ...] = ("chunk_ordinal", "chunk_index")
POSITION_HEADING_MAX_CHARS = 120

#: Per-parent bound on `passage_count`. A parent with more passages than this
#: reports null rather than a number the read did not finish establishing.
PASSAGE_COUNT_CAP = 1000

_PASSAGE_COUNT_WITHHELD_REASON = (
    "Nomos read enforcement is on, so a parent's passage count could include "
    "passages this caller may not read; the count is withheld rather than "
    "leak their number"
)

_PARENT_ABSENT_REASON = (
    "`parent` is null when the pinned catalog declares no `okg_passage_rollup` "
    "naming this row's subtype as a child: the graph carries no passage "
    "relation for it, so there is no parent to cite and no query can produce "
    "one. A declared relation that reaches no parent is reported as an object "
    "with a state instead, so the two are never the same value."
)
_PARENT_UNLINKED_REASON = (
    "the pinned catalog declares this passage relation and no readable parent "
    "edge reaches this row at this generation"
)
_PARENT_BOUNDED_REASON = (
    "the bounded association scan was saturated before this row was decided, "
    "so neither a parent nor its absence is claimed"
)


def passage_child_declarations(
    conn: psycopg.Connection,
    *,
    catalog_version_id: int,
    child_subtypes: Sequence[str],
) -> dict[str, list[PassageRollup]]:
    """Passage declarations naming each of `child_subtypes` as the CHILD.

    Returns `{child_subtype: [PassageRollup, ...]}`, containing only subtypes
    that at least one declaration names. A subtype absent from the mapping has
    no passage relation at this catalog version, which is why its rows carry
    `parent: null` rather than an empty object.

    The list is deliberately not collapsed to one entry: two parent subtypes may
    both declare the same child (okg's `document` and a bundle's
    `documentation_page` both `contains` a `document_chunk`), and choosing
    between them here would make the cited document depend on catalog
    ordering. `declared_parents_reached` chooses per EDGE instead, by the
    subtype of the node that edge reaches. This is the one declaration lookup
    parent resolution and `max_per_parent` diversity share.
    """
    wanted = sorted({str(subtype) for subtype in child_subtypes if subtype})
    if not wanted:
        return {}
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT name,
                   metadata -> 'passage_rollup' ->> 'edge_type' AS edge_type,
                   metadata -> 'passage_rollup' ->> 'child_subtype'
                       AS child_subtype
              FROM okg.subtypes
             WHERE catalog_version_id = %(cv)s
               AND metadata ? 'passage_rollup'
               AND metadata -> 'passage_rollup' ->> 'child_subtype'
                   = ANY(%(children)s)
             ORDER BY name
            """,
            {"cv": catalog_version_id, "children": wanted},
        )
        rows = cur.fetchall()
    declarations: dict[str, list[PassageRollup]] = {}
    for row in rows:
        edge_type, child = row["edge_type"], row["child_subtype"]
        if not edge_type or not child:
            # A half-written annotation is not a relation. The composer
            # validates both fields, so this only fires on a hand-edited
            # catalog; skipping it makes the row read as undeclared, which is
            # the truthful reading of "there is no usable relation here".
            continue
        declarations.setdefault(str(child), []).append(
            PassageRollup(
                parent_subtype=str(row["name"]),
                edge_type=str(edge_type),
                child_subtype=str(child),
            )
        )
    return declarations


def declared_parents_reached(
    edges: Iterable[tuple[str, str]],
    *,
    declared: Sequence[PassageRollup],
    parent_subtype_by_id: Mapping[str, str],
) -> list[tuple[str, PassageRollup]]:
    """`[(parent_id, declaration), ...]` a child's edges actually reach, by id.

    `edges` are one child's readable `(src, edge_type)` pairs at the pin;
    `parent_subtype_by_id` holds only parent nodes that are live and readable
    at the pin. An edge names a parent ONLY when the subtype of the node it
    reaches and its edge type together form a declared
    `(parent_subtype, edge_type)` pair for this child. A subtype declares at
    most one relation, so that pair names exactly one declaration: several
    declarations sharing a child subtype and edge type are never ambiguous.

    An edge from an undeclared subtype, an edge of an undeclared type, and an
    edge to a parent the caller cannot read all name nothing, and nothing is
    inferred from ids, paths, titles or names (fail closed).
    """
    by_pair = {
        (rollup.parent_subtype, rollup.edge_type): rollup for rollup in declared
    }
    reached: dict[str, PassageRollup] = {}
    for src, edge_type in edges:
        subtype = parent_subtype_by_id.get(str(src))
        if subtype is None:
            continue
        rollup = by_pair.get((subtype, str(edge_type)))
        if rollup is not None:
            reached[str(src)] = rollup
    return sorted(reached.items())


def _declaration_list(declared: Sequence[PassageRollup]) -> list[dict[str, str]]:
    return [
        {"parent_subtype": rollup.parent_subtype, "edge_type": rollup.edge_type}
        for rollup in declared
    ]


def _shared_value(values: Iterable[str]) -> Optional[str]:
    """The one value every entry shares, else None (so none is implied)."""
    distinct = set(values)
    return next(iter(distinct)) if len(distinct) == 1 else None


def _unresolved_block(
    declared: Sequence[PassageRollup], *, state: str, reason: str,
) -> dict[str, Any]:
    """A declared relation that named no parent for this row.

    With one declaration the block names it, exactly as before. With several
    the block names no single parent subtype -- the row was looked for under
    every one of them -- and lists them all.
    """
    block: dict[str, Any] = {
        "state": state,
        "node_id": None,
        "subtype": _shared_value(r.parent_subtype for r in declared),
        "edge_type": _shared_value(r.edge_type for r in declared),
        "relation_source": PARENT_RELATION_SOURCE,
        "citation": None,
        "citation_source": None,
        "reason": reason,
    }
    if len(declared) > 1:
        block["declarations"] = _declaration_list(declared)
    return block


def _visible_text(value: Any) -> Optional[str]:
    """A non-empty string the read filter did not mask, else None.

    A Nomos-masked attribute holds the literal `MASK_TOKEN`. It is a policy
    placeholder, not the record's name, so it never becomes a title, a status
    or a heading path.
    """
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    if not stripped or stripped == MASK_TOKEN:
        return None
    return stripped


def _first_line(value: str, *, max_chars: int) -> str:
    """The first line, cut to `max_chars`. Cut BEFORE any fencing."""
    line = value.split("\n", 1)[0].strip()
    return line if len(line) <= max_chars else line[: max_chars - 3] + "..."


def _parent_title(attrs: Any) -> tuple[Optional[str], Optional[str]]:
    """`(title, title_attr)` from a parent's effective, Nomos-projected attrs."""
    if not isinstance(attrs, Mapping):
        return None, None
    for key in PARENT_TITLE_ATTRS:
        text = _visible_text(attrs.get(key))
        if text is not None:
            return _first_line(text, max_chars=PARENT_TITLE_MAX_CHARS), key
    return None, None


def _parent_status(attrs: Any) -> Optional[str]:
    if not isinstance(attrs, Mapping):
        return None
    value = attrs.get("status")
    if isinstance(value, int) and not isinstance(value, bool):
        return _first_line(str(value), max_chars=PARENT_STATUS_MAX_CHARS)
    text = _visible_text(value)
    if text is None:
        return None
    return _first_line(text, max_chars=PARENT_STATUS_MAX_CHARS)


def passage_position(attrs: Any) -> Optional[dict[str, Any]]:
    """The passage's own position within its record, or None.

    Read only from the passage's producer attributes: `index` from the first
    non-negative integer in `POSITION_INDEX_ATTRS` (with `index_attr` naming
    it), and `heading_path` from a string or a list of strings. None when
    neither is present, so a row without a position carries one null rather
    than an empty object. Values are raw; the caller fences them by the ROW's
    trust. `char_offset` is deliberately not reported: it locates bytes, not a
    place a reader can name.
    """
    if not isinstance(attrs, Mapping):
        return None
    position: dict[str, Any] = {}
    for key in POSITION_INDEX_ATTRS:
        value = attrs.get(key)
        if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
            position["index"] = value
            position["index_attr"] = key
            break
    heading = attrs.get("heading_path")
    if isinstance(heading, list):
        heading = " / ".join(
            part for part in (_visible_text(item) for item in heading) if part
        )
    heading_text = _visible_text(heading)
    if heading_text is not None:
        position["heading_path"] = _first_line(
            heading_text, max_chars=POSITION_HEADING_MAX_CHARS,
        )
    return position or None


def fence_position_heading(row: dict[str, Any], *, attrs: Any) -> None:
    """Fence a serialized row's `position.heading_path` by the ROW's trust.

    The heading is producer text like any attr, but the node serializer fences
    only `attrs`, `citation.label` and `excerpt` and knows nothing of
    `position`. Called on a row that already carries its `provenance` block;
    the heading was cut to its bound first, so fencing cannot be truncated away.

    `attrs` is the row's FULL effective attrs, not its projection: the
    serializer reads the untrusted-transcript marker only from projected
    `attrs`, so a card that did not select `attrs` would otherwise report a
    marked row as trusted and leave its heading bare.
    """
    position = row.get("position")
    provenance = row.get("provenance") or {}
    if (
        isinstance(position, dict)
        and "heading_path" in position
        and (
            provenance.get("trust") == "untrusted"
            or attrs_mark_untrusted_transcript_content(attrs)
        )
    ):
        row["position"] = {
            **position,
            "heading_path": _fence_value(
                position["heading_path"], source=provenance.get("source"),
            ),
        }


def _parent_card(
    node: Mapping[str, Any],
    *,
    rollup: PassageRollup,
    citation_from_attrs: Callable[[Any], Optional[dict[str, Any]]],
    state: str = PARENT_STATE_RESOLVED,
    untrusted: bool = False,
    passage_count: Optional[int] = None,
) -> dict[str, Any]:
    attrs = node.get("attrs")
    citation = citation_from_attrs(attrs)
    title, title_attr = _parent_title(attrs)
    status = _parent_status(attrs)
    if untrusted:
        # The PARENT's trust decides, not the child's: a trusted passage can
        # sit inside an untrusted record, and that record's own text must reach
        # the agent fenced exactly as it would on the parent's own row. Values
        # were already cut to their bound, so fencing cannot be truncated away.
        source = node.get("source")
        title = _fence_value(title, source=source)
        status = _fence_value(status, source=source)
        if citation and "label" in citation:
            citation = {
                **citation,
                "label": _fence_value(citation["label"], source=source),
            }
    return {
        "state": state,
        "node_id": str(node["node_id"]),
        "subtype": str(node["subtype"]),
        "edge_type": rollup.edge_type,
        "relation_source": PARENT_RELATION_SOURCE,
        "citation": citation,
        # Null when the parent carries no producer citation. The read side
        # never fills this from title/path/url, so a null here means the
        # SOURCE did not supply one, not that the lookup failed.
        "citation_source": PARENT_LABEL_SOURCE if citation else None,
        "title": title,
        "title_attr": title_attr,
        "status": status,
        "passage_count": passage_count,
    }


#: Most neighbours one `expand` page names (its `limit` is capped lower).
NEIGHBOR_SUMMARY_CAP = 1_000


def resolve_neighbor_summaries(
    conn: psycopg.Connection,
    *,
    session: Session,
    node_ids: Sequence[str],
    runtime_required: bool | None = None,
    count: bool = False,
) -> dict[str, dict[str, Any]]:
    """`{node_id: {subtype, title}}` for the far ends of 1-hop edges.

    Issues #2675 and #3072: an `expand` row named only the neighbour's id, so
    agents did not follow links (0 expand calls in 946 in one benchmark run). The
    summary is read exactly as a passage's parent card is: one batch read at
    the session's pin, the Nomos read filter, effective attrs, the same title
    rule (`_parent_title`), and the neighbour's OWN trust deciding whether the
    title is fenced. A neighbour the caller may not read, or that does not
    exist at the pin, is absent: the two answer alike. `runtime_required`
    is the caller's one reading of the Nomos mode (issue #3393), so the
    titles are filtered under the mode that decided the page's edges.
    `count=True` is for a caller that has already read these very records
    for the page it returns (issue #3666, a 1-hop record row): the filter
    decides and redacts as it does for a read, and records the decisions
    with the counting purpose, so each returned record is logged as read
    once.
    """
    from okg.substrate.bm25_node_search import branch_point
    from okg.substrate.mcp.semantic_readiness import resolve_node_segment

    ids = sorted({str(node_id) for node_id in node_ids})[:NEIGHBOR_SUMMARY_CAP]
    if not ids:
        return {}
    try:
        node_segment = resolve_node_segment(conn)
        pin = branch_point(conn, session)
    except SubstrateError as exc:
        # The resolvers' own errors name semantic or lexical search; this is
        # an expand read, so say so rather than borrow their wording.
        raise SubstrateError(
            "expand could not read its neighbours' titles: the pinned node "
            f"segment is unavailable ({exc.code})",
            data={"cause": exc.code},
            fix_hint="retry; if it persists, the deployment needs okg up or a publish",
        ) from exc
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            SELECT node_id, subtype, attrs, text_hash, source,
                   source_record_id, source_revision
              FROM {node_segment}
             WHERE node_id = ANY(%(ids)s)
               AND deleted = FALSE
               AND live_lo <= %(bp)s AND %(bp)s < live_hi
             ORDER BY node_id
            """,
            {"ids": ids, "bp": pin},
        )
        raw = cur.fetchall()
    readable = filter_node_rows_for_session(
        conn, session, raw, runtime_required=runtime_required, count=count,
    )
    readable = apply_effective_attrs_to_rows(
        conn,
        readable,
        target_kind="node",
        target_id_field="node_id",
        generation_id=session.generation_id,
    )
    untrusted = _untrusted_parent_ids(conn, session=session, parents=readable)
    out: dict[str, dict[str, Any]] = {}
    for row in readable:
        node_id = str(row["node_id"])
        title, _title_attr = _parent_title(row.get("attrs"))
        if title is not None and node_id in untrusted:
            title = _fence_value(title, source=row.get("source"))
        # The row already names the id (`dst` going out, `src` coming in).
        out[node_id] = {"subtype": str(row["subtype"]), "title": title}
        if is_link_endpoint_row(row):
            # Issue #3666: a record known by key that the graph does not hold.
            out[node_id][LOAD_STATE_ATTR] = NOT_LOADED
    return out


def passage_count_disclosure(withheld: bool) -> dict[str, Any]:
    """The one statement of what `passage_count` means for this read.

    On every `parent_relation` envelope, whatever the page holds, so a null
    count is never ambiguous between "over the cap" and "withheld".
    """
    return {
        "cap": PASSAGE_COUNT_CAP,
        "withheld": withheld,
        "reason": _PASSAGE_COUNT_WITHHELD_REASON if withheld else None,
    }


def _empty_parent_envelope(
    *, catalog_version_id: int, edge_scan_cap: int, withheld: bool,
) -> dict[str, Any]:
    return {
        "source": PARENT_RELATION_SOURCE,
        "catalog_version_id": catalog_version_id,
        "child_subtypes": {},
        "rows_on_page": 0,
        "rows_with_declared_relation": 0,
        "rows_resolved": 0,
        "edge_scan_cap": edge_scan_cap,
        "edge_scan_cap_reached": False,
        "absent_reason": _PARENT_ABSENT_REASON,
        "passage_count": passage_count_disclosure(withheld),
    }


def resolve_passage_parents(
    conn: psycopg.Connection,
    *,
    session: Session,
    page: Sequence[Mapping[str, Any]],
    node_segment: str,
    branch_point_value: Any,
    citation_from_attrs: Callable[[Any], Optional[dict[str, Any]]],
    withhold_passage_counts: bool,
    edge_scan_cap: int = PARENT_EDGE_SCAN_CAP,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """`(parent_block_by_node_id, envelope_block)` for one page of result rows.

    `withhold_passage_counts` is the CALLER's statement that read policy is
    enforced for this session (Nomos `enforce`), so no passage count may be
    reported. It is a required argument, not a default, because this module
    keeps enforcement-mode resolution outside it and a forgotten flag must
    fail at the call rather than silently count hidden passages.

    One bounded batch read per page, at the SESSION'S PIN — the same
    `deleted = FALSE AND live_lo <= bp AND bp < live_hi` predicate every other
    pinned read uses — so a concurrent publish cannot change which parent a
    pinned page cites. Edges and parent nodes both pass the Nomos read filter
    before they can name a parent: a parent the caller may not read must not
    become visible through its child's card.

    Rows whose subtype has no declaration are absent from the returned mapping;
    the caller writes `parent: null` for them and the envelope block says once
    what that null means.
    """
    if not page:
        return {}, _empty_parent_envelope(
            catalog_version_id=session.catalog_version_id,
            edge_scan_cap=edge_scan_cap,
            withheld=withhold_passage_counts,
        )

    subtypes_on_page = {str(row.get("subtype") or "") for row in page}
    declarations = passage_child_declarations(
        conn,
        catalog_version_id=session.catalog_version_id,
        child_subtypes=sorted(subtypes_on_page),
    )

    # Per-subtype disclosure, bounded by the subtypes actually on the page.
    child_subtypes: dict[str, Any] = {}
    for subtype in sorted(subtypes_on_page):
        declared = declarations.get(subtype) or []
        if not declared:
            continue
        child_subtypes[subtype] = (
            {
                "parent_subtype": declared[0].parent_subtype,
                "edge_type": declared[0].edge_type,
            }
            if len(declared) == 1
            else {"declarations": _declaration_list(declared)}
        )

    blocks: dict[str, Any] = {}
    # Every declaration naming the row's subtype stays in play. Several are
    # not an ambiguity (issue #2753): each is a distinct
    # `(parent_subtype, edge_type)` pair, and the subtype of the node an edge
    # actually reaches decides which one that edge is an instance of.
    resolvable: dict[str, list[PassageRollup]] = {}
    for row in page:
        node_id, subtype = str(row["node_id"]), str(row.get("subtype") or "")
        declared = declarations.get(subtype) or []
        if declared:
            resolvable[node_id] = declared

    envelope = {
        "source": PARENT_RELATION_SOURCE,
        "catalog_version_id": session.catalog_version_id,
        "child_subtypes": child_subtypes,
        "rows_on_page": len(page),
        "rows_with_declared_relation": len(resolvable),
        "rows_resolved": 0,
        "edge_scan_cap": edge_scan_cap,
        "edge_scan_cap_reached": False,
        "absent_reason": _PARENT_ABSENT_REASON,
        "passage_count": passage_count_disclosure(withhold_passage_counts),
    }
    if not resolvable:
        return blocks, envelope

    child_ids = sorted(resolvable)
    edge_types = sorted(
        {rollup.edge_type for declared in resolvable.values() for rollup in declared}
    )
    # Resolved LAZILY, and only when a page actually holds a declared passage
    # child. An unregistered edge segment must not turn every ordinary lexical
    # search into a refusal; when it is missing, the affected rows say the
    # relation could not be read rather than reporting no parent.
    try:
        edge_segment = resolve_edge_segment(conn)
    except PassageRollupEdgeSegmentUnavailable as exc:
        for child_id, declared in resolvable.items():
            blocks[child_id] = _unresolved_block(
                declared,
                state=PARENT_STATE_BOUNDED_UNKNOWN,
                reason=f"the passage relation is unreadable: {exc}",
            )
        envelope["edge_segment_unavailable"] = True
        return blocks, envelope
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

    cap_reached = len(raw_edges) > edge_scan_cap
    raw_edges = raw_edges[:edge_scan_cap]
    # A saturated scan decided only the children it actually reached, and the
    # LAST one it touched may have had its edges cut in half. Everything else
    # is unknown.
    #
    # The decided set is derived from the `dst` values the scan RETURNED, never
    # by comparing ids against a boundary. `ORDER BY dst` runs under the
    # database's collation while Python compares strings by code point, and the
    # two disagree (`ICU`/`en_US` ignore punctuation that `C` orders); a
    # boundary comparison would then place a child on the wrong side and report
    # `declared_but_unlinked` for a passage the scan never looked at — a
    # fabricated negative, which is exactly the failure this module refuses to
    # produce elsewhere.
    undecided: set[str] = set()
    if cap_reached:
        seen = [str(edge["dst"]) for edge in raw_edges]
        # The final dst may be truncated mid-child, so it is NOT decided.
        decided = set(seen[:-1]) if seen else set()
        undecided = {
            child_id for child_id in child_ids if child_id not in decided
        }
    readable_edges = filter_edge_rows_for_session(conn, session, raw_edges)

    edges_by_child: dict[str, set[tuple[str, str]]] = {}
    for edge in readable_edges:
        child_id = str(edge["dst"])
        if child_id not in resolvable:
            continue
        edges_by_child.setdefault(child_id, set()).add(
            (str(edge["src"]), str(edge["edge_type"]))
        )

    parent_ids = sorted(
        {src for pairs in edges_by_child.values() for src, _ in pairs}
    )
    parent_subtypes = sorted(
        {
            rollup.parent_subtype
            for declared in resolvable.values()
            for rollup in declared
        }
    )
    parents_by_id: dict[str, Mapping[str, Any]] = {}
    if parent_ids:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT node_id, subtype, attrs, text_hash, source,
                       source_record_id, source_revision
                  FROM {node_segment}
                 WHERE node_id = ANY(%(parent_ids)s)
                   AND subtype = ANY(%(parent_subtypes)s)
                   AND deleted = FALSE
                   AND live_lo <= %(bp)s AND %(bp)s < live_hi
                 ORDER BY node_id
                """,
                {
                    "parent_ids": parent_ids,
                    "parent_subtypes": parent_subtypes,
                    "bp": branch_point_value,
                },
            )
            raw_parents = cur.fetchall()
        readable_parents = filter_node_rows_for_session(
            conn, session, raw_parents,
        )
        # The citation is read off EFFECTIVE attrs, so a curated override of a
        # parent's `citation_label` is honoured here exactly as it is on the
        # parent's own search row.
        readable_parents = apply_effective_attrs_to_rows(
            conn,
            readable_parents,
            target_kind="node",
            target_id_field="node_id",
            generation_id=session.generation_id,
        )
        parents_by_id = {str(row["node_id"]): row for row in readable_parents}

    # The declaration each reached parent is an instance of: the reached
    # node's OWN subtype, paired with the edge type, among the declared pairs.
    parent_subtype_by_id = {
        pid: str(row["subtype"]) for pid, row in parents_by_id.items()
    }
    reached_by_child = {
        child_id: declared_parents_reached(
            edges_by_child.get(child_id, ()),
            declared=declared,
            parent_subtype_by_id=parent_subtype_by_id,
        )
        for child_id, declared in resolvable.items()
    }

    untrusted_parents = _untrusted_parent_ids(
        conn, session=session, parents=parents_by_id.values(),
    )
    passage_counts = _passage_counts(
        conn,
        withheld=withhold_passage_counts,
        reached_by_child=reached_by_child,
        node_segment=node_segment,
        edge_segment=edge_segment,
        branch_point_value=branch_point_value,
    )

    def card(pid: str, rollup: PassageRollup, state: str) -> dict[str, Any]:
        return _parent_card(
            parents_by_id[pid],
            rollup=rollup,
            citation_from_attrs=citation_from_attrs,
            state=state,
            untrusted=pid in untrusted_parents,
            passage_count=passage_counts.get(pid),
        )

    resolved_count = 0
    for child_id, declared in resolvable.items():
        reached = reached_by_child[child_id]
        if child_id in undecided:
            # The scan did not finish this child: the one parent it saw may be
            # one of several, so neither a parent nor its absence is claimed.
            blocks[child_id] = _unresolved_block(
                declared,
                state=PARENT_STATE_BOUNDED_UNKNOWN,
                reason=_PARENT_BOUNDED_REASON,
            )
            continue
        if len(reached) == 1:
            pid, rollup = reached[0]
            blocks[child_id] = card(pid, rollup, PARENT_STATE_RESOLVED)
            resolved_count += 1
            continue
        if len(reached) > 1:
            # A passage inside two parents is a real corpus state, not an
            # error, and so is a passage inside parents of two DECLARED
            # subtypes. Naming one of them would put a fabricated citation on
            # the card, so name all of them and resolve none. The outer block
            # names a subtype or edge type only when every candidate shares it.
            blocks[child_id] = {
                "state": PARENT_STATE_MULTIPLE_PARENTS,
                "node_id": None,
                "subtype": _shared_value(r.parent_subtype for _, r in reached),
                "edge_type": _shared_value(r.edge_type for _, r in reached),
                "relation_source": PARENT_RELATION_SOURCE,
                "citation": None,
                "citation_source": None,
                # Each nested entry is a CANDIDATE, never `resolved`. The outer
                # block resolved nothing — that is what `multiple_parents`
                # means — so stamping the nested cards `resolved` would let an
                # agent lift one out and cite it as the chosen parent, which is
                # the fabricated citation this state exists to refuse.
                "parents": [
                    card(pid, rollup, PARENT_STATE_CANDIDATE)
                    for pid, rollup in reached
                ],
            }
            continue
        blocks[child_id] = _unresolved_block(
            declared,
            state=PARENT_STATE_UNLINKED,
            reason=_PARENT_UNLINKED_REASON,
        )

    envelope["rows_resolved"] = resolved_count
    envelope["edge_scan_cap_reached"] = cap_reached
    return blocks, envelope


def _untrusted_parent_ids(
    conn: psycopg.Connection,
    *,
    session: Session,
    parents: Iterable[Mapping[str, Any]],
) -> set[str]:
    """Ids of parents whose OWN effective trust is untrusted.

    The same rule `provenance.serialize_node_read_row` applies to a node's own
    row: the producer's classification, overridden to untrusted when the attrs
    carry the untrusted-transcript marker. One producer-profile read per page.
    """
    rows = list(parents)
    if not rows:
        return set()
    trust_by_source = classify_sources(
        conn,
        deployment_name=session.deployment_name,
        sources=(row.get("source") for row in rows),
    )
    return {
        str(row["node_id"])
        for row in rows
        if trust_by_source.get(row.get("source"), "untrusted") == "untrusted"
        or attrs_mark_untrusted_transcript_content(row.get("attrs"))
    }


def _passage_counts(
    conn: psycopg.Connection,
    *,
    withheld: bool,
    reached_by_child: Mapping[str, Sequence[tuple[str, PassageRollup]]],
    node_segment: str,
    edge_segment: str,
    branch_point_value: Any,
) -> dict[str, Optional[int]]:
    """`{parent_id: count or None}` for one page.

    Counted ONLY where nothing can be hidden (operator decision, issue #2675):
    under Nomos `enforce` a count of the parent's children could include
    passages this caller may not read, and any bounded, filtered count would
    still leak whether hidden ones exist, so every count is withheld and the
    envelope says so once. Otherwise one query per declared relation on the
    page counts each parent's distinct live children of the declared child
    subtype through the declared edge, at the session pin, stopping at
    `PASSAGE_COUNT_CAP + 1`; a parent past the cap reports null. No DISTINCT:
    an edge's identity is its (src, edge_type, dst) triple
    (`graph_store_internal.common.edge_id`), so at one pin each child appears
    once, and the LIMIT can stop the scan early instead of after a full
    de-duplication of every edge the parent has.
    """
    if withheld:
        return {}
    # Group the parents carded on this page by the declaration that reached
    # them. A parent subtype declares one relation, and a parent is reached
    # only through its own subtype's declaration, so each parent is counted
    # once, through the relation its subtype declares.
    by_rollup: dict[PassageRollup, set[str]] = {}
    for reached in reached_by_child.values():
        for pid, rollup in reached:
            by_rollup.setdefault(rollup, set()).add(pid)
    counts: dict[str, Optional[int]] = {}
    for rollup, parent_ids in sorted(
        by_rollup.items(),
        key=lambda item: (item[0].parent_subtype, item[0].edge_type),
    ):
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT p.parent_id,
                       (SELECT count(*)
                          FROM (SELECT 1
                                  FROM {edge_segment} e
                                  JOIN {node_segment} n
                                    ON n.node_id = e.dst
                                   AND n.subtype = %(child)s
                                   AND n.deleted = FALSE
                                   AND n.live_lo <= %(bp)s
                                   AND %(bp)s < n.live_hi
                                 WHERE e.src = p.parent_id
                                   AND e.edge_type = %(edge_type)s
                                   AND e.deleted = FALSE
                                   AND e.live_lo <= %(bp)s
                                   AND %(bp)s < e.live_hi
                                 LIMIT %(cap)s) AS children) AS passages
                  FROM unnest(%(parent_ids)s::text[]) AS p(parent_id)
                """,
                {
                    "child": rollup.child_subtype,
                    "edge_type": rollup.edge_type,
                    "bp": branch_point_value,
                    "cap": PASSAGE_COUNT_CAP + 1,
                    "parent_ids": sorted(parent_ids),
                },
            )
            for row in cur.fetchall():
                value = int(row["passages"])
                counts[str(row["parent_id"])] = (
                    value if value <= PASSAGE_COUNT_CAP else None
                )
    return counts
