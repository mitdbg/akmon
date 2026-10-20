"""Shared pg_textsearch BM25 lexical-node-search primitives (PACT
restore-bm25-lexical-node-search).

The AKMON ``search(method="lexical")`` operator (``mcp.search._search_lexical``)
is the only caller left. The console topbar composer that also ranked through
here (``console_views.search.compose_search``) went with the Graph view in PACT
operator-ui-remove-four-views. This stays the ONE index + resolution path, so a
console search that returns resolves nodes here rather than growing a second,
divergent lexical implementation.

BM25 queries cannot go through the chronos checkout -- its sqlglot rewriter cannot
parse the pg_textsearch ``<@>`` operator -- so callers resolve the physical node
segment + BM25 index from ``pg_indexes`` and query the segment directly. To honor the
pinned generation (the M4.5 pin guarantee) they apply the chronos branch-point
visibility filter ``live_lo <= bp AND bp < live_hi`` (``bp`` = the pin's branch point,
from ``branch_point``), NOT a fixed ``live_hi`` sentinel: the open-interval value
varies per database, so a hardcoded sentinel is only incidentally correct. The
maintained index (``graph_store_internal/schema.py::
_ensure_interval_source_read_indexes``) covers all non-deleted text rows; visibility
is selected at read time, so the index is pin-agnostic.
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping, Optional, Sequence, Union

from psycopg.rows import dict_row

from okg.substrate.errors import SubstrateError


class SearchIndexUnavailable(SubstrateError):
    """The pg_textsearch BM25 node-text index is absent on the live node segment, or
    the pinned generation's branch point cannot be resolved.

    Lexical search fails loud here rather than silently falling back to a full
    sequential scan or a default (P0: no silent degradation). ``okg migrate``
    recreates the index via
    ``graph_store_internal/schema.py::_ensure_interval_source_read_indexes``."""


#: The text search configuration every node-text BM25 index is built with
#: (``graph_store_internal/schema.py``: ``WITH (text_config='english')``).
BM25_TEXT_CONFIG = "english"

#: ``to_tsvector`` refuses a document whose lexemes exceed 1 MB. A document no
#: longer than this cannot reach that, so the match test below never errors.
_MATCH_TEST_MAX_BYTES = 1_000_000


def bm25_match_sql(*, score: str, text: str, query: str = "%(q)s") -> str:
    """The SQL test that keeps a ranked row only when it matches the query.

    ``score`` is the row's ``<@> to_bm25query(...)`` score, ``text`` its
    indexed text, ``query`` the query text. A negative score is a match: the
    score only gains from a query term the document contains. A zero score is
    NOT proof of a non-match (issue #3232). pg_textsearch 1.3.0 adds a term's
    score only when the term's document frequency, read from the index's
    memtable and segments, is above zero, and that read is not consistent with
    the reader's snapshot: on the first read after a publish, while
    autovacuum's ANALYZE runs, it can read 0 for a term the document contains
    and score a real match 0. Filtering ``score < 0`` then dropped that match
    with no notice (9 of 288 first reads in the diagnosis; 0 of 288 after an
    explicit ANALYZE).

    So a row without a negative score is kept when its own words contain a
    query word, tokenized the way the index tokenizes both (``to_tsvector``
    with the index's text configuration). That test reads no index
    statistics. Ranking still uses the score, so such a row sorts after the
    rows that scored. A document over ``_MATCH_TEST_MAX_BYTES`` cannot take
    the word test and is kept only on a negative score, as before.
    """
    cfg = f"'{BM25_TEXT_CONFIG}'::regconfig"
    return (
        f"({score} < 0 OR (octet_length({text}) <= {_MATCH_TEST_MAX_BYTES} "
        f"AND tsvector_to_array(to_tsvector({cfg}, {text})) "
        f"&& tsvector_to_array(to_tsvector({cfg}, {query}::text))))"
    )



#: The match test over a ranked page aliased ``ranked`` that carries
#: ``score`` and ``attrs`` (the lexical and passage-rollup ranked reads).
RANKED_BM25_MATCH = bm25_match_sql(score="ranked.score", text="ranked.attrs->>'text'")

#: The oldest pg_textsearch library measured to score every row the same
#: while the first ANALYZE after a publish moves the index's in-memory rows
#: to disk (issue #3666): 1.5.1. 1.3.0 does not; versions between are
#: unmeasured, so they are treated as 1.3.0.
STEADY_SCORE_LIBRARY = (1, 5, 1)

#: Whether the server's pg_textsearch library is older than
#: ``STEADY_SCORE_LIBRARY``, as one scalar subquery (PostgreSQL evaluates it
#: once per statement). A version it cannot read counts as older.
_OLD_SCORE_LIBRARY_SQL = (
    "(SELECT coalesce((regexp_match(current_setting("
    "'pg_textsearch.library_version', true), '^([0-9]+)[.]([0-9]+)[.]([0-9]+)'))::int[] "
    f"< ARRAY[{', '.join(str(part) for part in STEADY_SCORE_LIBRARY)}], TRUE))"
)


def steady_bm25_score_sql(score: str) -> str:
    """``score``, a per-row ``<@> to_bm25query(...)`` expression, read so a
    passing wrong reading of the index statistics cannot rank the row.

    On pg_textsearch 1.3.0, while the first VACUUM or ANALYZE after a publish
    moves the index's in-memory rows to disk, one evaluation of ``<@>`` now
    and then reads the statistics wrong: 0 for a row that contains the query
    word, or a weaker score (-5.742 for a row the steady index scores -7.087).
    The row then ranks after its equals and can move between pages. The
    evaluation right after it reads them right: on a lab probe, the first of
    two evaluations per row was wrong in 48 of 886 reads and the second in
    none (issue #3666; pact/changes/read-zero-score-rank-3666/evidence/
    zero-score-probe.log).

    So on a library older than ``STEADY_SCORE_LIBRARY`` each row is scored
    twice and keeps the better (lower) score. A real match's score only gains
    from a term the document contains, and every wrong reading seen was
    weaker, never stronger. On 1.5.1 and later the second reading is NULL,
    which ``LEAST`` ignores, so the row is scored once, as before.
    """
    return f"LEAST({score}, CASE WHEN {_OLD_SCORE_LIBRARY_SQL} THEN {score} END)"

def drop_repeated_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    seen: Optional[set[Any]] = None,
    key: Union[str, Sequence[str]] = "node_id",
) -> list[Any]:
    """``rows`` with each stored row's first copy only, in the order given
    (issue #3335).

    One pg_textsearch 1.3.0 index scan can return the same table row more
    than once and leave other matching rows out. The scan scores only up to
    its LIMIT; when a condition above it rejects rows it reruns its scoring
    with a doubled limit and resumes by position. Two triggers make the
    rerun order rows differently, so a row it returned comes back:

    * equal scores held in more than one index segment, on a table no one
      writes to: the two passes break the tie differently. Both copies
      carry the same score. Measured on a copy of the comp-ops graph, 6 of
      16 passage reads at LIMIT 2,001 held up to 9 repeated rows.
    * concurrent writes between the two passes (upstream PR #417): the
      index statistics move, near-equal scores reorder, and the copies can
      carry different scores.

    pg_textsearch 1.3.1 fixes both, and only that upgrade brings back the
    rows the scan leaves out: dropping repeats here cannot.

    A reader of the index passes its rows through here before it counts
    them, charges them against a budget, or pages them, so every count and
    budget is of distinct rows. Each reader gives its rows in rank order
    (the lexical self read by score then node id; grep sorts each window in
    Python), so the copy kept is the one the read ranked first, also when
    the copies' scores differ. Whether the read filled its LIMIT is still judged
    on the rows the read returned: a LIMIT that repeated rows filled may
    hold more matches past it.

    ``key`` names the column, or the columns, that identify one stored row.
    ``node_id`` serves a read that sees one version per node (its WHERE
    selects the pinned versions); a read over every version of a node (grep's
    windows) names the version too, so two versions are never one row.

    ``seen`` carries the identities already taken across several reads of
    one scan: a row whose identity is in it is dropped, and every identity
    kept is added to it.
    """
    names = (key,) if isinstance(key, str) else tuple(key)
    taken = set() if seen is None else seen
    distinct = []
    for row in rows:
        identity = tuple(str(row[name]) for name in names)
        if identity in taken:
            continue
        taken.add(identity)
        distinct.append(row)
    return distinct


def resolve_node_text_bm25_index(conn: Any) -> tuple[str, str]:
    """``(physical_segment, bm25_index_name)`` for the node-text BM25 index.

    Resolved from ``pg_indexes`` so it tracks the current chronos node segment
    (whose physical name changes across compaction). Raises
    ``SearchIndexUnavailable`` when the index is absent."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT quote_ident(schemaname) || '.' || quote_ident(tablename) AS segment, "
            "       indexname AS index_name "
            "FROM pg_indexes "
            "WHERE indexname ~ 'node_text_bm25_visible$' "
            "ORDER BY indexname "
            "LIMIT 1"
        )
        row = cur.fetchone()
    if not row:
        raise SearchIndexUnavailable(
            "node-text BM25 index absent on the live node segment",
            data={"remediation": "okg migrate recreates it"},
        )
    return row["segment"], row["index_name"]


def execute_with_custom_plan(cur: Any, sql: Any, params: Any = None) -> Any:
    """Run one BM25-ordered read so PostgreSQL plans it with ITS query text.

    psycopg 3 server-prepares a statement once it has run ``prepare_threshold``
    (default 5) times on one connection, and PostgreSQL may then switch the
    prepared statement to a generic plan. A generic plan cannot see the query
    text, so the BM25 ``<@> to_bm25query(...)`` ordering loses the index scan
    the text narrows. Measured on a copy of the comp-ops graph, passage scans
    on one owner-login connection: runs 1-10 had a median of about 2.0 s and
    runs 11-16 took 28-68 s on the generic plan; through this helper all 20
    runs took 1.7-2.8 s (pact/changes/mcp-passage-custom-plan/evidence/
    live-summary.json).

    ``prepare=False`` sends the statement unnamed, so every execution is
    planned with its own parameters on ANY connection: the MCP read pool, a
    console or CLI connection that logs in as the deployment owner, or an
    ``OKG_MCP_RO_DSN`` role that lacks the ``okg_mcp`` role's
    ``plan_cache_mode`` setting. It changes this one statement only, never a
    session or role setting, so it is safe on the owner's connection too.
    """
    return cur.execute(sql, params, prepare=False)


def branch_point(conn: Any, session: Any) -> int:
    """The chronos branch point (visibility coordinate) of the session's pinned
    checkpoint. A row is visible at the pin iff ``live_lo <= bp AND bp < live_hi``.

    ``checkpoint.ref`` is the chronos SEGMENT_ID -- a small monotonic allocator value,
    NOT the visibility coordinate. The branch point is that segment's ``branch_point``
    column: a bisection coordinate in a different, large number space (segment ids run
    1, 2, 3, ...; branch points are ~MAX/2 then recursive midpoints). Filtering on the
    segment id directly silently drops every row whose ``live_lo`` exceeds it -- ~40%
    of the live set on a many-generation deployment (an empty/partial result reported
    as success). So resolve the real ``branch_point`` from the chronos segment registry
    (the same lookup ``_IntervalBackend._segment`` does). Using the pin's own branch
    point -- rather than a fixed ``live_hi`` sentinel, whose value also varies per
    database -- honors the pinned generation (the M4.5 pin guarantee) for any
    deployment and any pin. Raises ``SearchIndexUnavailable`` if it cannot be resolved
    (fail loud; never a default)."""
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    checkpoint = store.get_generation(session.generation_id)
    segment_id = getattr(checkpoint, "ref", None) if checkpoint is not None else None
    if segment_id is None:
        raise SearchIndexUnavailable(
            "cannot resolve the pinned checkpoint's segment",
            data={"generation_id": str(session.generation_id)},
        )
    with conn.cursor() as cur:
        cur.execute(
            "SELECT branch_point FROM _chronos_branch_interval_segments "
            "WHERE segment_id = %s",
            (int(segment_id),),
        )
        row = cur.fetchone()
    if not row or row[0] is None:
        raise SearchIndexUnavailable(
            "cannot resolve the branch point of the pinned generation",
            data={
                "generation_id": str(session.generation_id),
                "segment_id": str(segment_id),
            },
        )
    return int(row[0])
