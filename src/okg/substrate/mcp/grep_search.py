"""`search(method="grep")`: exact, case-insensitive text search (#3067).

PACT mcp-grep-3067; design `pact/changes/mcp-substring-search-3067/DESIGN.md`
sections 3 (E1, P1), 4(c), 5 and 11; decisions 167a-170b. PACT mcp-grep-rank
added the BM25 order and the `path:` term; PACT mcp-grep-page builds the page
best-first, the way lexical search does, and moves the count behind one
function (`count_grep_matches`).

How one call runs. Every row bound counts rows of a sequence whose length
does not depend on what the reader may not see or read; which sequence
depends on the regime, decided per call at the policy level
(`_strict_regime`, decision 260a):

- **R1**: Nomos enforcement is `off` or `audit`. The bounds charge stored
  matching rows, admitted or not (under `audit` a refusal is logged and the
  row shown; the rows it still hides -- no source lineage, masked text --
  are neither charged against `PAGE_SCAN_BOUND` nor counted, #3384).
- **R2**: `enforce`. Nothing is decided by how many stored rows match:
  the bounds charge in-scope BM25 rows (what the words match, hidden rows
  included, the exposure lexical search's rank cap has) or every scope row
  walked (how many rows the scoped subtypes and sources hold).

One exception to both (#3384): under `audit` and `enforce`,
`PAGE_SCAN_BOUND` charges only the text units the scan site admits, so a
row the reader may not see or read never decides where a page stops. The
index-row and id-row bounds and the statement timeout still count every
row read: at those scales unreadable matches can still end a page (the
disclosed large-scale gap).

1. The query is parsed (`grep_query`): every term becomes one PostgreSQL
   regular expression matched with `~*` on the node text. Regex terms are
   compiled by PostgreSQL once before any row is read. An unscoped query with
   no indexable positive term is refused.
2. **Page** (`_PageScan`), best first:

   - with literal words (`rank_words`, including an alternation of plain
     literals), from the node-text BM25 index's ordered scan (`ORDER BY <@>
     LIMIT n OFFSET o`), in windows growing from `PAGE_FIRST_WINDOW` rows up
     to `PAGE_INDEX_ROWS`; a window's rows are ordered by (score, node id)
     and only rows scoring strictly better than its last row are taken, so
     a tie at a window's edge waits for the next window. The grep predicate
     (terms, exclusions, path conditions, pushed `attr_filters`) runs on
     each window row. With Nomos off the rows that pass it are charged
     against `PAGE_SCAN_BOUND`; under `audit` and `enforce` only the units
     the scan site admits. R1 may finish from the whole match set when
     it holds at most `PAGE_SMALL_SET` units. When the BM25 matches run out,
     the matches no BM25 word reaches follow in node-id order;
   - in node-id order (no literal word, and that zero-score pass,
     `_scan_id_order`): R1 reads the match set whole when it holds at most
     `PAGE_SMALL_SET` units, else walks the scope; R2 always walks the scope
     in node-id order, at most `PAGE_ID_ROWS_R2` rows, matching or not.

   Rows passing the stored predicate go in batches through the scan site
   (`_Admitter`: `visible_ids_by_decision_class`, recorded `count`; under a
   policy that can redact, `_read_unchanged` and the recheck on read values,
   so a row whose text or deciding attribute the read would change never
   matches), are folded into records (search-result-shape's `record_fold`),
   and the scan stops as soon as `offset + limit + 1` records exist or a
   bound is reached. A bound is reported only when a further row exists
   past it. A record's place is its best text unit's place in the scan, so
   every record already folded keeps its place: the page is the top of all
   visible matches. A page that stopped at a bound gets a
   `continuation_token`: the next call replays the scan in SQL without
   deciding the rows before the point it stopped at, then goes on with
   fresh bounds, leaving out the records already listed.
3. **Count** (`count_grep_matches`): R1 under Nomos `off` counts the stored
   matching units in SQL, up to `COUNT_SCAN_BOUND`, and so does `audit`
   when it can hide nothing in the generation (`_audit_hides_nothing`);
   otherwise `audit` finishes them one by one, up to the same
   `COUNT_SCAN_BOUND` stored units (`COUNT_FINISH_ROWS` with a residual
   `where`); R2 walks `COUNT_SCOPE_ROWS` scope rows and
   counts the admitted matches. Exact below the bound, `lower_bound` past it
   and whenever the active policy can redact. Paging never reads the count.
4. **Serve site** (`_serve_rows`): the page's records and the passages that
   supply their match windows go through `filter_rows_by_read_class`
   (recorded `read`, masked). The pattern is rechecked on the masked,
   effective text, and the match windows are cut from that text. A window's
   line is then turned into a line of its record's text where that is cheap
   (`_record_lines_in_time`, #3666): with access rules off only, a record of
   at most 500 chunks and 1 MiB, at most 10,000 segments and 8 MiB a page,
   under this timeout, never through the prepared-text cache; otherwise
   the window says what its line counts (`line_basis`). So the same page
   can report a record line on one call and a passage line with
   `line_basis` on another when the time limit is hit; each answer says
   which it is, and continuation tokens do not depend on lines.

Every statement runs under `SET LOCAL statement_timeout` of
`STATEMENT_TIMEOUT_MS`. Each read is bounded by rows except two, stopped
only by that timeout: R1's unordered match-set read (it stops at
`PAGE_SMALL_SET + 1` matches, but on a smaller set its cost follows the
trigram candidates) and R1's SQL count (it stops at `COUNT_SCAN_BOUND + 1`
matches, likewise).
AND applies within one text unit (`and_scope: "text_unit"`), never across a
record's passages (decision 170b).
"""
from __future__ import annotations

import contextlib
import contextvars
import hashlib
import json
import logging
import re
import threading
import time
from dataclasses import dataclass, replace
from typing import Any, Callable, Iterator, Mapping, Optional, Sequence

import psycopg
from psycopg import sql as pgsql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from okg.substrate.attributes import apply_effective_attrs_to_rows
from okg.substrate.bm25_node_search import (
    SearchIndexUnavailable,
    branch_point,
    drop_repeated_rows,
    execute_with_custom_plan,
    resolve_node_text_bm25_index,
    steady_bm25_score_sql,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store_internal.schema import (
    GREP_PATH_INDEX_CHARS,
    _is_bounded_path_index,
)
from okg.substrate.mcp.continuation import (
    consume_continuation,
    continuation_binding,
    issue_continuation,
    seal_cursor,
    unseal_cursor,
)
from okg.substrate.mcp import search_where
from okg.substrate.mcp.envelope import validate_select
from okg.substrate.mcp.grep_query import (
    SCOPE_NAMES,
    GrepQuery,
    GrepQueryInvalid,
    assert_indexable_or_scoped,
    own_path_tail_query,
    parse_grep_query,
    spelling_tried,
)
from okg.substrate.mcp.readable_text import read_unchanged, redaction_possible
from okg.substrate.mcp.excerpt_window import UNIT_WHOLE_CHARS, centred_cut
from okg.substrate.mcp.record_text import (
    MATCH_LINE_IN_OWN_TEXT,
    MATCH_LINE_IN_PASSAGE,
    MatchPlace,
    ThreadParts,
    chunk_location,
    is_statement_timeout,
    record_match_lines,
    thread_parts,
)
from okg.substrate.mcp.result_text import HANDLE_KEY, LOCATION_KEY
from okg.substrate.mcp.passage_rollup import (
    passage_child_declarations,
    passage_position,
    resolve_edge_segment,
    resolve_passage_parents,
)
from okg.substrate.mcp.operator_sql import CompiledPredicate
from okg.substrate.mcp.response_bounds import InvalidOperatorArguments
from okg.substrate.mcp.read_scanner import (
    MAX_SCAN_BODY_BYTES,
    EnumerationEvidence,
    FinishedCandidate,
    ScanBatch,
    ScanContext,
    ScanInputInvalid,
    finish_scan_batch,
    make_scan_context,
)
from okg.substrate.mcp.provenance import (
    serialize_node_read_row,
    serialize_node_read_rows,
)
from okg.substrate.mcp.record_fold import (
    CHECK_CLASS,
    MATCHING_KEY,
    SearchFoldReadCheckMismatch,
    declaring_subtypes,
    fold_members,
    resolve_fold_links,
)
from okg.substrate.mcp.search_profiles import ResolvedScope
from okg.substrate.mcp.session import Session
from okg.substrate.nomos.read_filter import (
    filter_rows_by_read_class,
    visible_ids_by_decision_class,
)

__all__ = [
    "COUNT_FINISH_ROWS",
    "COUNT_SCAN_BOUND",
    "GREP_ALWAYS",
    "GREP_DEFAULT",
    "GREP_FIELDS",
    "GrepCount",
    "GrepPathNotApplicable",
    "GrepTimeout",
    "PAGE_ID_ROWS",
    "PAGE_ID_ROWS_R2",
    "PAGE_INDEX_ROWS",
    "PAGE_SCAN_BOUND",
    "STATEMENT_TIMEOUT_MS",
    "STRIP_SPACE_RANGES",
    "bounded_path_prefix_sql",
    "count_grep_matches",
    "PATH_PREFIX_CANDIDATE_BOUND",
    "bounded_path_index_on",
    "path_prefix_candidates_sql",
    "own_path_reads",
    "path_prefix_probe_sql",
    "path_record_subtypes",
    "search_grep",
    "window_text",
]

#: Admitted text units one page may take (rows the scan site refuses are not
#: charged), in scan order. A row count. A 10-record page decided 32-64
#: candidates on every clone query (evidence/page-cost-clone.json); the
#: bound covers the largest page (`limit` 1,000) at about 1 ms per decided
#: row. A scan through many refused rows is bounded by PAGE_INDEX_ROWS (BM25
#: path) or only by the statement timeout (node-id order).
PAGE_SCAN_BOUND = 2_000
#: Global positions of the BM25 index's ordered scan one page may read
#: (every row the words match, in or out of scope; below pg_textsearch's
#: own 100,000-result limit per scan): about
#: 0.04-0.1 ms per row with the grep predicate on the clone. No single word
#: there reaches 25,000 BM25 matches, but a phrase of two common words
#: matches more than 100,000 rows for BM25 (either word), and a phrase with
#: no match walked all 100,000 in about 4 s; 50,000 halves that
#: (evidence/page-cost-clone.json).
PAGE_INDEX_ROWS = 50_000
#: pg_textsearch 1.3.0 through 1.4.x returns at most this many results per
#: index scan (TP_MAX_QUERY_LIMIT) and stops silently past it; every window
#: ends within `PAGE_INDEX_ROWS` global positions, below it. 1.5.0 removed the
#: cut (upstream PR #518), but older libraries still serve databases, and the
#: limit also bounds how far one window may grow, so it stays as okg's own
#: window ceiling on every version.
PG_TEXTSEARCH_SCAN_LIMIT = 100_000
assert PAGE_INDEX_ROWS < PG_TEXTSEARCH_SCAN_LIMIT
#: A match set this small is read whole (one unordered trigram read) and
#: scored on its stored text with the `<@>` expression, about 0.3 ms per row
#: on the clone. It spares a selective query (a phrase whose words are
#: common) a walk through the BM25 index's many partial matches. It is read
#: only after `PAGE_PROBE_AFTER` index rows did not fill the page: for a
#: NEAR query of common words that read alone can take seconds.
PAGE_SMALL_SET = 2_000
#: Rows of the scope one page may walk in node-id order (a query with no
#: literal word, or the matches no BM25 word reaches, when the match set is
#: larger than `PAGE_SMALL_SET`): a row count of the scope, whatever matches.
PAGE_ID_ROWS = 50_000
#: R2: rows of the scope one page walks in node-id order. Every walked row is
#: charged, matched or not, so the bound depends only on how many rows the
#: scope holds; at worst every walked row matches and needs a scan-site
#: decision, about 0.39 ms each on the clone (review of bdf798d229), so
#: 6,000 rows keep that worst case near 2.3 s.
PAGE_ID_ROWS_R2 = 6_000
PAGE_PROBE_AFTER = 3_000
#: Records a continuation token may name as already listed (it carries their
#: ids); past it no token is issued (`continuation_withheld: "listed_cap"`).
CONTINUATION_LISTED_MAX = 200
#: `consume_continuation`'s own limit on a token's length; a longer token is
#: withheld (`continuation_withheld: "token_size"`).
CONTINUATION_TOKEN_MAX = 16_384
#: The first BM25 window; each next window is `PAGE_WINDOW_GROWTH` times larger.
PAGE_FIRST_WINDOW = 1_000
PAGE_WINDOW_GROWTH = 2
#: Candidates decided per batch: `PAGE_BATCH_MULTIPLE` times the records the
#: page still needs, within [PAGE_BATCH_MIN, PAGE_BATCH_MAX].
PAGE_BATCH_MULTIPLE = 2
PAGE_BATCH_MIN = 32
PAGE_BATCH_MAX = 256
#: Under `audit` and `enforce` a decided batch that admits no unit doubles
#: the next one (word windows and node-id walks alike), up to this many rows, so a scan through many unreadable matches
#: decides them in few round trips (#3384). It changes only how rows are
#: grouped for deciding, never which rows the page takes or where it stops.
PAGE_BATCH_DRY_MAX = 4_096
#: R1: stored matching text units the count reads, counted in SQL (decision
#: 170b's cap, as on #3139 and #3146). Past it the count is `lower_bound`
#: with `scan_cap: {"rows": COUNT_SCAN_BOUND}`.
COUNT_SCAN_BOUND = 10_000
#: R2: rows of the scope (matching or not) the count walks in node-id order.
#: The matching rows among them are decided row by row at the scan site and
#: folded (their edges and parents decided too), about 0.8 ms per decided
#: row on the clone: a 10,000-row walk took up to 12 s on common words
#: (evidence/r2-count-profile.json), so 2,000. Lane H's per-class count
#: (`count_visible_by_class`) removes the per-row cost.
COUNT_SCOPE_ROWS = 2_000
#: R1 with a reader residual (a `where` clause SQL cannot decide: attributes,
#: catalog title/path, dates): stored matching text units the count reads
#: whole and finishes one by one (scan site, fold, residual on read values),
#: since a SQL count of stored rows would count rows the residual rejects.
#: The same per-row cost as the R2 count, so the same 2,000. Past it the
#: count is `lower_bound` with `scan_cap: {"rows": COUNT_FINISH_ROWS}`.
COUNT_FINISH_ROWS = 2_000
#: Every grep statement's timeout (decision 170b); the only bound on the
#: node-id-order, match-set and count reads.
STATEMENT_TIMEOUT_MS = 5_000
#: A deadline (``time.monotonic()`` seconds) a caller may set for one grep
#: call (PACT lexical-exact-identifier-hits: lexical search's exact pass runs
#: grep under its own time budget). Each statement then runs under the
#: smaller of `STATEMENT_TIMEOUT_MS` and the time left, and a statement that
#: would start with none left is cancelled before it runs. Unset, every
#: statement gets `STATEMENT_TIMEOUT_MS`.
_DEADLINE: contextvars.ContextVar[Optional[float]] = contextvars.ContextVar(
    "grep_deadline", default=None,
)


@contextlib.contextmanager
def deadline(at: float) -> Iterator[None]:
    """Run the grep calls inside so no statement runs past ``at``
    (``time.monotonic()`` seconds)."""
    token = _DEADLINE.set(at)
    try:
        yield
    finally:
        _DEADLINE.reset(token)


def _statement_timeout_ms() -> int:
    """This statement's timeout; raises `_Cancelled` past the deadline."""
    at = _DEADLINE.get()
    if at is None:
        return STATEMENT_TIMEOUT_MS
    left = int((at - time.monotonic()) * 1000)
    if left <= 0:
        raise _Cancelled()
    return min(STATEMENT_TIMEOUT_MS, left)
#: Match windows per result, and characters of source text per window.
MAX_WINDOWS = 3
WINDOW_CHARS = 160

_COLUMNS = (
    "n.node_id, n.subtype, n.attrs, n.text_hash, n.source, "
    "n.source_record_id, n.source_revision"
)
_TEXT = "(n.attrs->>'text')"


class GrepTimeout(SubstrateError):
    """A grep statement ran past the statement timeout before a page existed."""

    code = "grep_timeout"


class _Cancelled(Exception):
    pass


class _PathLost(Exception):
    """A continuation's replay could not take the read path its earlier
    calls took (a pinned whole-match-set read timed out): the sequence would
    not line up, so the page stops as timed out instead."""


class GrepPathNotApplicable(SubstrateError):
    """`path_prefix` or a `path:` term on a graph where no record carries a
    path: it could only ever match nothing (#3154)."""

    code = "path_prefix_not_applicable"


# ---------- fields ----------

GREP_FIELDS: tuple[str, ...] = (
    "node_id", "canonical_node_id", "subtype", "attrs", "text_hash", "method",
    "match_kind", "excerpt", "provenance", "citation", "evidence_kind",
    "evidence_kind_provenance", "chunk_kind", "source_record_id",
    "source_revision", "parent", "matching_passages", "match_windows",
    "source_family", "position",
    # The record's title, path and date, read from the row's attrs as the
    # reader sees them, as lexical search serves it. Selected only: agents
    # asked grep for it in 41% of grep-using attempts (#3470, `grephandle A`).
    HANDLE_KEY,
    # Where the first own match sits in a chunk row, as lexical serves it
    # (`{chunk, offset, hash}` for inspect(kind="text")). Selected only
    # (#3646).
    LOCATION_KEY,
)
#: Characters of a match's line a grep list row shows (#3659).
LIST_WINDOW_CHARS = 100
#: In-process row key of a list page: whether the record's own text matched.
#: `search_views.grep_list_projection` reads and removes it; no card has it.
OWN_MATCH_KEY = "own_match"
GREP_DEFAULT: tuple[str, ...] = (
    "node_id", "canonical_node_id", "subtype", "method", "match_kind",
    "excerpt", "citation", "evidence_kind", "evidence_kind_provenance",
    "chunk_kind", "source_record_id", "source_revision", "parent",
    "matching_passages", "match_windows", "source_family", "position",
    # One line per match (#3666 R5). Served by default; not a `select` field.
    "match_lines",
)
GREP_ALWAYS: tuple[str, ...] = (
    "node_id", "subtype", "evidence_kind", "evidence_kind_provenance",
    # A grep row's match lines are its passage. A `select` that names any
    # field other than an id or head field keeps them (#3666): under
    # `select=["node_id","attrs"]` they were once dropped, and every row
    # printed its head line with no matched line at all. A `select` of id
    # and head fields only asks for head lines, and `search.search` leaves
    # them out then, on every method alike (`reply_size.select_is_head_only`,
    # PACT read-reply-size-3666).
    "match_lines",
)


# ---------- SQL pieces ----------

def resolve_trgm_segment(conn: psycopg.Connection) -> str:
    """The live node segment carrying the grep trigram index; refuses without it."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT quote_ident(schemaname) || '.' || quote_ident(tablename) AS segment "
            "FROM pg_indexes WHERE indexname ~ 'node_text_trgm_visible$' "
            "ORDER BY indexname LIMIT 1"
        )
        row = cur.fetchone()
    if not row:
        raise SearchIndexUnavailable(
            "node-text trigram index absent on the live node segment",
            data={"index": "node_text_trgm_visible"},
            fix_hint="run `okg migrate` to build the grep trigram index",
        )
    return str(row["segment"])


def assert_path_trgm_index(conn: psycopg.Connection) -> None:
    """Refuse a `path:` term when the path trigram index is absent: without
    it every path term is a sequential scan (PACT mcp-grep-rank)."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM pg_indexes WHERE indexname ~ 'node_path_trgm_visible$' LIMIT 1"
        )
        row = cur.fetchone()
    if not row:
        raise SearchIndexUnavailable(
            "node-path trigram index absent on the live node segment",
            data={"index": "node_path_trgm_visible"},
            fix_hint="run `okg migrate` to build the grep path trigram index",
        )


def _term_sql(parsed: GrepQuery, params: dict[str, Any], *, column: str) -> list[str]:
    """One SQL clause per text term, every one matched with `~*`.

    A NEAR term ANDs each operand's own regex before the NEAR regex (PACT
    grep-near-operands-3251): every NEAR match contains both operands, so
    the result is unchanged, but pg_trgm can narrow `~* a AND ~* b` and
    cannot narrow the two-order alternation. Count, match set, windows,
    walk and recheck all build their predicate here.
    """
    clauses = []
    for index, term in enumerate(parsed.terms):
        key = f"grep_t{index}"
        params[key] = term.pg_regex
        parts = []
        for at, operand in enumerate(term.operands):
            params[f"{key}o{at}"] = operand
            parts.append(f"{column} ~* %({key}o{at})s")
        clause = " AND ".join([*parts, f"{column} ~* %({key})s"])
        clauses.append(f"NOT ({clause})" if term.negated else f"({clause})")
    return clauses


def _path_sql(
    parsed: GrepQuery,
    path_prefix: Optional[str],
    params: dict[str, Any],
    *,
    alias: str,
) -> list[str]:
    """The path conditions on one candidate path (`alias.attrs.path`): the
    prefix and every path term, positive or negated. The bounded expression
    and predicate repeat the prefix index's, so the planner can use it; the
    full value is rechecked beside it."""
    column = f"({alias}.attrs->>'path')"
    clauses = [
        f"{alias}.attrs ? 'path'",
        f"jsonb_typeof({alias}.attrs->'path') = 'string'",
        f"NULLIF({column}, '') IS NOT NULL",
    ]
    if path_prefix:
        clauses.append(bounded_path_prefix_sql(column, path_prefix, params, key="grep_path"))
    for index, term in enumerate(parsed.path_terms):
        key = f"grep_p{index}"
        params[key] = term.pg_regex
        clause = f"{column} ~* %({key})s"
        clauses.append(f"NOT ({clause})" if term.negated else f"({clause})")
    return clauses


def bounded_path_prefix_sql(
    column: str, prefix: str, params: dict[str, Any], *, key: str,
) -> str:
    """`column` (an `attrs->>'path'` expression) starts with `prefix`.

    The bounded half repeats the path-prefix index's expression
    (`node_path_prefix_visible`: `left(attrs->>'path', GREP_PATH_INDEX_CHARS)`,
    text_pattern_ops), so the planner can read it as an index range; it uses
    the first GREP_PATH_INDEX_CHARS characters of a longer prefix and only
    narrows. The full LIKE beside it decides. Binds `key` and `key_b`.
    """
    params[key] = _like_prefix(prefix)
    params[f"{key}_b"] = _like_prefix(prefix[:GREP_PATH_INDEX_CHARS])
    return (
        f"left({column}, {GREP_PATH_INDEX_CHARS}) LIKE %({key}_b)s "
        f"AND {column} LIKE %({key})s"
    )


#: The characters Python's `str.strip()` removes (`str.isspace()`), as
#: inclusive code-point ranges. A catalog `path` reads its stored attribute
#: stripped (`handle_fields`), so a stored path that starts with one of them
#: can read as starting with any prefix. The first two ranges are ASCII and
#: lie below "!" (0x21).
STRIP_SPACE_RANGES: tuple[tuple[int, int], ...] = (
    (0x09, 0x0D), (0x1C, 0x20), (0x85, 0x85), (0xA0, 0xA0), (0x1680, 0x1680),
    (0x2000, 0x200A), (0x2028, 0x2029), (0x202F, 0x202F), (0x205F, 0x205F),
    (0x3000, 0x3000),
)


#: Most visible candidates a lookup's `where` path prefix may narrow to
#: (issue #3194). The candidate set is read whole before the first page row
#: (the index ignores the pin, so old row versions are read and dropped as
#: well); past this many, the plain scan, which stops at the page's LIMIT,
#: is kept. 10,000 ids read through the index and sorted cost milliseconds;
#: the bound is a judgement, not a measurement, until the real-copy timings
#: of a broad and a selective prefix are taken. Read at call time, so a test
#: may lower it.
PATH_PREFIX_CANDIDATE_BOUND = 10_000


def path_prefix_candidates_sql(
    prefix: str,
    params: dict[str, Any],
    *,
    segment: str,
    bp_ref: str,
    key: str,
    union_all: bool = False,
) -> str:
    """Node ids visible at `bp_ref` whose stored `attrs.path`, read stripped
    as a catalog path, may start with `prefix`: a superset, never exact.

    One SELECT per arm, joined by UNION (UNION ALL when `union_all`, for a
    bounded count that must stop early), each a range on the path-prefix
    index (its partial predicate `deleted = FALSE AND attrs ? 'path'` is
    repeated): the bounded prefix of :func:`bounded_path_prefix_sql`, and
    one arm per leading-space range of :data:`STRIP_SPACE_RANGES` (a stored
    path such as "  src/a" reads as "src/a"). text_pattern_ops compares
    bytes, and UTF-8 byte order is code-point order, so a value starting
    with a character in [lo, hi] lies in [chr(lo), chr(hi + 1)). The ASCII
    ranges share one arm: every nonempty value starting below "!" (an empty
    path reads as no path).
    """
    column = "(g.attrs->>'path')"
    bounded = f"left({column}, {GREP_PATH_INDEX_CHARS})"
    base = (
        f"SELECT g.node_id AS target_id FROM {segment} g "
        f"WHERE g.deleted = FALSE AND g.attrs ? 'path' "
        f"AND g.live_lo <= {bp_ref} AND {bp_ref} < g.live_hi AND "
    )
    arms = [base + bounded_path_prefix_sql(column, prefix, params, key=key)]
    params[f"{key}_sp"] = "!"
    arms.append(base + f"{bounded} ~<~ %({key}_sp)s AND {bounded} <> ''")
    for index, (low, high) in enumerate(r for r in STRIP_SPACE_RANGES if r[0] > 0x20):
        params[f"{key}_s{index}_lo"] = chr(low)
        params[f"{key}_s{index}_hi"] = chr(high + 1)
        arms.append(
            base + f"{bounded} ~>=~ %({key}_s{index}_lo)s "
            f"AND {bounded} ~<~ %({key}_s{index}_hi)s"
        )
    joiner = " UNION ALL " if union_all else " UNION "
    return joiner.join(f"({arm})" for arm in arms)


def path_prefix_probe_sql(
    prefix: str,
    params: dict[str, Any],
    *,
    segment: str,
    bp_ref: str,
    key: str,
    bound: int,
) -> str:
    """At most `bound + 1` of the candidate rows of `prefix`, counted: a
    count above `bound` means "too broad to narrow". UNION ALL may count a
    node twice (only one in both the prefix arm and a leading-space arm),
    which can only make the probe say "too broad" sooner."""
    params[f"{key}_limit"] = int(bound) + 1
    candidates = path_prefix_candidates_sql(
        prefix, params, segment=segment, bp_ref=bp_ref, key=key, union_all=True,
    )
    return f"SELECT count(*) AS n FROM ({candidates} LIMIT %({key}_limit)s) probe"


def bounded_path_index_on(conn: psycopg.Connection, physical_table: str) -> bool:
    """Whether the bounded path-prefix index (`node_path_prefix_visible`)
    exists on `public.<physical_table>`, the registered node table."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT indexdef FROM pg_indexes WHERE schemaname = 'public' "
            "AND tablename = %s AND indexname ~ 'node_path_prefix_visible$'",
            (physical_table,),
        )
        rows = cur.fetchall()
    return any(_is_bounded_path_index(row["indexdef"]) for row in rows)


def _path_checks(
    conn: psycopg.Connection,
    parsed: GrepQuery,
    path_prefix: Optional[str],
    paths: Sequence[str],
) -> list[bool]:
    """For each path (a value the reader reads): whether it passes the prefix
    and every path term. Run by PostgreSQL, so the decision uses the same
    LIKE, regex engine and case folding as the match set."""
    if not paths:
        return []
    params: dict[str, Any] = {"paths": list(paths)}
    clauses = ["TRUE"]
    if path_prefix:
        params["grep_path"] = _like_prefix(path_prefix)
        clauses.append("t.p LIKE %(grep_path)s")
    for index, term in enumerate(parsed.path_terms):
        key = f"grep_p{index}"
        params[key] = term.pg_regex
        clause = f"t.p ~* %({key})s"
        clauses.append(f"NOT ({clause})" if term.negated else f"({clause})")
    sql = (
        f"SELECT t.i, ({' AND '.join(clauses)}) AS ok "
        f"FROM unnest(%(paths)s::text[]) WITH ORDINALITY AS t(p, i) ORDER BY t.i"
    )
    try:
        rows = _bounded(conn, sql, params)
    except _Cancelled as exc:
        raise GrepTimeout(
            "checking the record paths ran past the grep statement timeout",
            data={"timeout_ms": STATEMENT_TIMEOUT_MS, "scopes": SCOPE_NAMES},
            fix_hint=f"narrow the call with {SCOPE_NAMES}",
        ) from exc
    return [bool(row["ok"]) for row in rows]


def _any_path(conn: psycopg.Connection, segment: str, branch_point_value: Any) -> bool:
    """Whether a live record has a nonempty string `attrs.path` at the pin.

    Deployment-wide, hidden rows included: one bit saying whether a path
    condition can apply. A timeout refuses rather than masquerade as absence.
    The planner's choice is never used as proof of applicability.
    """
    sql = (
        f"SELECT 1 AS one FROM {segment} n WHERE n.deleted = FALSE AND n.attrs ? 'path' "
        f"AND jsonb_typeof(n.attrs->'path') = 'string' "
        f"AND NULLIF(n.attrs->>'path', '') IS NOT NULL "
        f"AND n.live_lo <= %(bp)s AND %(bp)s < n.live_hi LIMIT 1"
    )
    params = {"bp": branch_point_value}
    try:
        return bool(_bounded_with(conn, sql, params, settings=_INDEX_PLAN))
    except _Cancelled as exc:
        raise GrepTimeout(
            "checking whether paths apply ran past the grep statement timeout",
            data={"timeout_ms": STATEMENT_TIMEOUT_MS, "phase": "path_applicability"},
        ) from exc


#: Seconds an `_any_path` answer is reused, and how many pins are kept.
_ANY_PATH_TTL_S = 600.0
_ANY_PATH_MAX_ENTRIES = 256
_any_path_answers: dict[tuple[Any, ...], tuple[bool, float]] = {}
_any_path_lock = threading.Lock()
#: The cache's clock; a module name so a test can move it alone.
_any_path_clock = time.monotonic


def _any_path_at_pin(
    conn: psycopg.Connection, segment: str, branch_point_value: Any,
    *, generation_id: Any,
) -> bool:
    """`_any_path`, reused for one published generation.

    The check scans for one live record with a path and costs about 1.6 s on
    a comp-ops copy when few records carry one, on every grep call with a
    path scope. The live rows at a published generation's pin do not change:
    a checkpoint is a read-only cut, and `branch_point` refuses a retired
    generation before this runs. The answer is reused for `_ANY_PATH_TTL_S`,
    which bounds only a row edited in place outside the publish path (a
    manual update on a copy, or a database restored under the same name from
    another snapshot). The key holds the generation id as well as the database,
    segment and branch point: a rebuilt or truncated database can reuse a
    segment name and a branch point, never a generation id. A timeout is
    raised, never cached.
    """
    info = conn.info
    key = (info.host, info.port, info.dbname, segment, branch_point_value, generation_id)
    now = _any_path_clock()
    with _any_path_lock:
        hit = _any_path_answers.get(key)
        if hit is not None and now - hit[1] < _ANY_PATH_TTL_S:
            return hit[0]
    answer = _any_path(conn, segment, branch_point_value)
    with _any_path_lock:
        _any_path_answers.pop(key, None)
        if len(_any_path_answers) >= _ANY_PATH_MAX_ENTRIES:
            _any_path_answers.pop(next(iter(_any_path_answers)))
        _any_path_answers[key] = (answer, now)
    return answer


def own_path_reads(
    conn: psycopg.Connection, session: Session, node_ids: Sequence[str],
) -> dict[str, Optional[str]]:
    """`{node_id: attrs.path}` as the session reads it, for the records an
    own-path grep of this session already returned (PACT
    filename-exact-hits); `None` when the path is unknown to the reader.

    It orders and groups rows the reader may see; it never admits one. A
    returned record can be the record of a matching passage, so its own path
    was not what grep decided on. Under a policy that can redact, each
    record is read as the session reads it (`_read_unchanged`, which records
    `read` decisions as grep's scan site does), and a path the read masks
    or drops reads as `None`: the stored value never groups or orders a
    row. Without such a policy no read can change a path, so only the path
    attribute is read, with the effective-attrs overlay, and the records'
    text is never fetched. The path select runs under the grep statement
    timeout (and the exact pass's deadline); a timeout raises `GrepTimeout`.
    The segment and branch-point lookups, the policy lookup, the overlay
    and the read filter are grep's own unbounded helpers."""
    if not node_ids:
        return {}
    segment = resolve_trgm_segment(conn)
    redaction = _redaction_possible(conn, session)
    columns = (
        "n." + ", n.".join(_UNIT_KEYS) if redaction
        else "n.node_id, jsonb_build_object('path', n.attrs->'path') AS attrs"
    )
    sql = (
        f"SELECT {columns} FROM {segment} n WHERE n.node_id = ANY(%(ids)s) "
        f"AND n.deleted = FALSE AND n.live_lo <= %(bp)s AND %(bp)s < n.live_hi"
    )
    try:
        rows = _bounded(conn, sql, {"ids": list(node_ids), "bp": branch_point(conn, session)})
    except _Cancelled as exc:
        raise GrepTimeout(
            "reading the matched records' paths ran past the grep statement timeout",
            data={"timeout_ms": STATEMENT_TIMEOUT_MS, "phase": "own_path_read"},
        ) from exc
    if redaction:
        rows = _read_unchanged(conn, session, [dict(r) for r in rows],
                               decisive=lambda _row: {"path"})
    else:
        rows = apply_effective_attrs_to_rows(
            conn, [dict(r) for r in rows], target_kind="node", target_id_field="node_id",
            generation_id=session.generation_id,
        )
    out: dict[str, Optional[str]] = {str(node_id): None for node_id in node_ids}
    for row in rows:
        path = (row.get("attrs") or {}).get("path")
        out[str(row["node_id"])] = path if isinstance(path, str) else None
    return out


def path_record_subtypes(conn: psycopg.Connection, session: Session) -> set[str]:
    """The record types whose catalog entry makes a path the record's own
    (PACT filename-exact-hits): `path` among its `searchable_text_path_fields`
    (a source file, a document), or a declared passage relation (a record
    with passages, such as a document). A record of one of these types
    represents its path ahead of another type sharing it (a code symbol).
    Runs under the grep statement timeout; a timeout raises `GrepTimeout`."""
    sql = (
        "SELECT name FROM okg.subtypes WHERE catalog_version_id = %(cv)s AND ("
        "metadata ? 'passage_rollup' OR "
        "coalesce(metadata->'searchable_text_path_fields', '[]'::jsonb) ? 'path')"
    )
    try:
        rows = _bounded(conn, sql, {"cv": session.catalog_version_id})
    except _Cancelled as exc:
        raise GrepTimeout(
            "reading the path-owning record types ran past the grep statement timeout",
            data={"timeout_ms": STATEMENT_TIMEOUT_MS, "phase": "path_record_subtypes"},
        ) from exc
    return {str(row["name"]) for row in rows}


def _like_prefix(prefix: str) -> str:
    escaped = prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return escaped + "%"


def _bounded(conn: psycopg.Connection, sql: str, params: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Run one statement under the grep timeout; raise `_Cancelled` on timeout."""
    return _bounded_with(conn, sql, params, settings=())


#: Planner settings for a BM25 window: the index's own ordered scan, never a
#: sort. Every window of a call then takes its order and its scores from the
#: index, so the order cannot change with the plan (a seq scan and sort
#: scores with the `<@>` expression, whose ties and rounding can differ from
#: the index scan's; seen when table statistics changed between two calls).
_INDEX_PLAN = (("enable_seqscan", "off"), ("enable_sort", "off"), ("enable_bitmapscan", "off"))


def _bounded_with(
    conn: psycopg.Connection,
    sql: str,
    params: Mapping[str, Any],
    *,
    settings: Sequence[tuple[str, str]],
    explain: bool = False,
) -> list[dict[str, Any]]:
    """Run one statement under the grep timeout (and `settings`); raise
    `_Cancelled` on timeout. With `explain`, return its plan lines instead.

    Each setting is set with `set_config(..., true)` inside a transaction (a
    savepoint when the caller holds one) and put back afterwards, so it
    never outlives this statement. A cancelled statement rolls the
    (sub)transaction back, which also restores the settings.
    """
    names = ("statement_timeout", *(name for name, _value in settings))
    values = (f"{_statement_timeout_ms()}ms", *(value for _name, value in settings))
    try:
        with conn.transaction():
            with conn.cursor(row_factory=dict_row) as cur:
                previous = []
                for name, value in zip(names, values):
                    cur.execute("SELECT current_setting(%s) AS v", (name,))
                    previous.append(cur.fetchone()["v"])
                    cur.execute("SELECT set_config(%s, %s, true)", (name, value))
                if explain:
                    cur.execute("EXPLAIN " + sql, dict(params), prepare=False)
                    rows = [{"line": str(next(iter(r.values())))} for r in cur.fetchall()]
                else:
                    execute_with_custom_plan(cur, sql, dict(params))
                    rows = cur.fetchall()
                for name, value in zip(names, previous):
                    cur.execute("SELECT set_config(%s, %s, true)", (name, value))
                return rows
    except psycopg.errors.QueryCanceled as exc:
        raise _Cancelled() from exc


def _check_regexes_compile(conn: psycopg.Connection, parsed: GrepQuery, query: str) -> None:
    """PostgreSQL compiles each term once before any row is read; a pattern it
    rejects ("invalid regular expression", "too complex") is refused."""
    for term in parsed.terms:
        try:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute("SELECT '' ~* %s", (term.pg_regex,), prepare=False)
        except psycopg.errors.InvalidRegularExpression as exc:
            raise GrepQueryInvalid(
                f"grep query refused: PostgreSQL rejected {term.text!r}: "
                f"{exc.diag.message_primary or exc}",
                data={"reason": str(exc.diag.message_primary or exc), "query": query},
                fix_hint="simplify the regular expression",
            ) from exc


def _declarations(conn: psycopg.Connection, session: Session) -> list[tuple[str, str, str]]:
    """Every declared passage relation: `(parent, edge_type, child)`."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT name,
                   metadata -> 'passage_rollup' ->> 'edge_type',
                   metadata -> 'passage_rollup' ->> 'child_subtype'
              FROM okg.subtypes
             WHERE catalog_version_id = %s AND metadata ? 'passage_rollup'
             ORDER BY name
            """,
            (session.catalog_version_id,),
        )
        return [(str(a), str(b), str(c)) for a, b, c in cur.fetchall() if b and c]




def _declared_values(declared: Sequence[tuple[str, str, str]], params: dict[str, Any]) -> str:
    """The declared passage relations as a VALUES list `d(parent, edge, child)`."""
    values = []
    for index, (parent, edge, child) in enumerate(declared):
        params[f"dp{index}"], params[f"de{index}"], params[f"dc{index}"] = parent, edge, child
        values.append(f"(%(dp{index})s::text, %(de{index})s::text, %(dc{index})s::text)")
    return f"(VALUES {', '.join(values)}) AS d(parent, edge, child)"


def _lender_branch(
    *,
    segment: str,
    edge_segment: str,
    values_sql: str,
    lender_path: str,
    where: Sequence[str],
    own_path: str,
) -> str:
    """Text units in scope only through a lending record's path.

    One row per unit (`DISTINCT ON`), and never a unit whose own path already
    passes (the other branch holds it), so the two branches together list
    each unit once: the count's match SQL counts rows.
    """
    return (
        f"SELECT DISTINCT ON (n.node_id) {_COLUMNS} FROM {segment} p "
        f"JOIN {edge_segment} e ON e.src = p.node_id AND e.deleted = FALSE "
        f"AND e.live_lo <= %(bp)s AND %(bp)s < e.live_hi "
        f"JOIN {segment} n ON n.node_id = e.dst "
        f"JOIN {values_sql} "
        f"ON d.parent = p.subtype AND d.edge = e.edge_type AND d.child = n.subtype "
        f"WHERE p.deleted = FALSE AND p.live_lo <= %(bp)s AND %(bp)s < p.live_hi "
        f"AND {lender_path} AND {' AND '.join(where)} "
        f"AND NOT coalesce({own_path}, FALSE)"
    )


def _lender_exists(
    *, segment: str, edge_segment: str, values_sql: str, lender_path: str,
) -> str:
    """Whether a declared lending record of the unit `n` passes the path conditions."""
    return (
        f"EXISTS (SELECT 1 FROM {edge_segment} e "
        f"JOIN {segment} p ON p.node_id = e.src "
        f"JOIN {values_sql} "
        f"ON d.parent = p.subtype AND d.edge = e.edge_type AND d.child = n.subtype "
        f"WHERE e.dst = n.node_id AND e.deleted = FALSE "
        f"AND e.live_lo <= %(bp)s AND %(bp)s < e.live_hi "
        f"AND p.deleted = FALSE AND p.live_lo <= %(bp)s AND %(bp)s < p.live_hi "
        f"AND {lender_path})"
    )


def _units_sql(
    *,
    segment: str,
    own_where: Sequence[str],
    path_branch: Optional[str],
    ordered: bool,
    limit_key: Optional[str],
) -> str:
    """The text units matching the whole predicate, each once.

    `ordered`: by node id (the page's id-order scan); unordered for the count.
    """
    own = f"SELECT {_COLUMNS} FROM {segment} n WHERE {' AND '.join(own_where)}"
    if path_branch is not None:
        own = f"({own}) UNION ALL ({path_branch})"
    order = " ORDER BY m.node_id" if ordered else ""
    limit = f" LIMIT %({limit_key})s" if limit_key else ""
    return f"SELECT * FROM ({own}) m{order}{limit}"


#: What identifies one stored row in a BM25 window, which reads every
#: version of a node (issue #3335): the node id and the version's `live_lo`,
#: the primary key of every chronos segment table (`PRIMARY KEY (<logical
#: key>, live_lo)`, chronos_core/branching/_interval_backend.py; the nodes
#: table's logical key is `node_id`).
_WINDOW_ROW = ("node_id", "grep_live_lo")


def _window_sql(
    *,
    segment: str,
    index_name: str,
    rank_where: Sequence[str],
    ok_sql: str,
    index_score: bool,
) -> str:
    """One window of the BM25 index's ordered scan, with the grep predicate.

    The inner read is lexical search's: the bare `ORDER BY <@>` with no
    tie-break, `LIMIT` and `OFFSET`. A second sort key makes PostgreSQL
    compute the score again for every row (about 0.3 ms per row on the
    comp-ops clone), while the index's own scan hands its score over through
    `bm25_get_current_score()` (evidence/page-cost-clone.json). That function
    returns 0 off the index scan, so `index_score` is set only after
    `_index_ordered` saw the plan; otherwise the score is the `<@>`
    expression itself, the same number. Ties are broken by node id in
    Python. The predicate runs above the window, on its rows only: every row
    comes back with its score, and only rows passing it (`grep_ok`) carry
    `attrs`. It decides on stored values; the scan site decides again on the
    values the reader reads.
    """
    literal = index_name.replace("'", "''")
    order = f"(n.attrs->>'text') <@> to_bm25query(%(grep_rank_q)s, '{literal}')"
    # The order and the scores a window is settled on must come from one
    # source. The index scores a row still in its in-memory buffer with the
    # exact text length while the `<@>` expression rounds it, so the two
    # differ by up to about 6% for recently inserted rows (seen on the
    # other BM25-window branch's review). Off the index scan the ORDER BY is
    # the expression plus 0, which no index can serve, so PostgreSQL sorts
    # by the very number the window is settled on.
    score = "bm25_get_current_score()" if index_score else order
    ordering = order if index_score else f"({order}) + 0"
    # The inner read carries only the index's own predicate, so LIMIT and
    # OFFSET count GLOBAL index positions: pg_textsearch returns at most
    # 100,000 results per scan and stops silently past them, and any other
    # condition there would run as a filter after it, so a short window
    # would not mean the matches ran out. The scope (pin, subtype, sources,
    # path exclusions) is checked above, with the predicate.
    index_where = ("n.deleted = FALSE", "n.attrs ? 'text'")
    scope_where = [c for c in rank_where if c not in index_where] or ["TRUE"]
    inner = (
        f"SELECT n.node_id, n.subtype, n.attrs, n.text_hash, n.source, "
        f"n.source_record_id, n.source_revision, n.deleted, n.live_lo, n.live_hi, "
        f"{score} AS grep_score "
        f"FROM {segment} n WHERE {' AND '.join(index_where)} "
        f"ORDER BY {ordering} LIMIT %(grep_window)s OFFSET %(grep_offset)s"
    )
    ok = f"({' AND '.join(scope_where)}) AND ({ok_sql})"
    return (
        f"SELECT n.node_id, n.subtype, n.text_hash, n.source, n.source_record_id, "
        f"n.source_revision, n.live_lo AS grep_live_lo, n.grep_score, "
        f"({' AND '.join(scope_where)}) AS grep_in_scope, "
        f"({ok}) AS grep_ok, CASE WHEN {ok} THEN n.attrs END AS attrs FROM ({inner}) n"
    )


def _walk_sql(*, segment: str, rank_where: Sequence[str], ok_sql: str) -> str:
    """One step of the node-id walk: the next `grep_window` rows of the
    scope after `grep_after`, in node-id order, each with the grep predicate
    (`grep_ok`); only passing rows carry `attrs`. Its cost follows the rows
    walked, never the match set."""
    inner = (
        f"SELECT n.node_id, n.subtype, n.attrs, n.text_hash, n.source, "
        f"n.source_record_id, n.source_revision FROM {segment} n "
        f"WHERE {' AND '.join(rank_where)} AND n.node_id > %(grep_after)s "
        f"ORDER BY n.node_id LIMIT %(grep_window)s"
    )
    return (
        f"SELECT n.node_id, n.subtype, n.text_hash, n.source, n.source_record_id, "
        f"n.source_revision, ({ok_sql}) AS grep_ok, "
        f"CASE WHEN {ok_sql} THEN n.attrs END AS attrs FROM ({inner}) n "
        f"ORDER BY n.node_id"
    )


def _index_ordered(
    conn: psycopg.Connection, sql: str, params: Mapping[str, Any], index_name: str,
) -> bool:
    """Whether PostgreSQL runs `sql` (under `_INDEX_PLAN`) as the BM25 index's
    own ordered scan, with no sort above it, so `bm25_get_current_score()` is
    the row's score."""
    try:
        lines = _bounded_with(conn, sql, params, settings=_INDEX_PLAN, explain=True)
    except _Cancelled:
        return False
    plan = "\n".join(row["line"] for row in lines)
    return (
        f"Index Scan using {index_name}" in plan
        and "Order By:" in plan
        and "Sort" not in plan
    )


# ---------- the count seam ----------

@dataclass(frozen=True)
class GrepCount:
    """What grep reports as its count: `total_count` (matching text units the
    reader may see), its basis, and why it is not exact."""

    total: Optional[int]
    basis: str  # "exact" | "lower_bound" | "timed_out"
    reasons: tuple[str, ...]
    #: The bound that made it a lower bound, with its unit.
    cap: Optional[dict[str, int]] = None
    #: The shared scanner's finished-enumeration proof, when the count
    #: finished every candidate of a terminal enumeration (PACT
    #: shared-read-scanners-3199): None for the SQL count and past a bound.
    proof: Optional[EnumerationEvidence] = None


#: A visible-count basis, as the count function returns it, onto grep's
#: `total_count_basis`: "per_row" means every row was decided one by one and
#: the count is exact below the bound. Lane H's per-class count
#: (`count_visible_by_class`) returns the same three names.
COUNT_BASIS = {"exact": "exact", "per_row": "exact", "lower_bound": "lower_bound"}

#: The shared scanner's domains for grep (read_scanner.EnumerationEvidence):
#: the stored text units passing the SQL prefilter (terms, path conditions,
#: pushed `attr_filters`, the exact part of `where`), and what finishing
#: them leaves: the records (or passages) the reader may see that pass the
#: whole query on read values, the `where` residual included.
CANDIDATE_DOMAIN = "grep_stored_matching_units"
FINISHED_DOMAIN = "grep_visible_matching_records"


def count_grep_matches(
    conn: psycopg.Connection,
    session: Session,
    match_sql: pgsql.Composable,
    params: Mapping[str, Any],
    *,
    scan_bound: int,
    redaction_possible: bool,
    admit: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
    strict: bool = False,
    finish_rows: int = COUNT_FINISH_ROWS,
    walk_sql: Optional[str] = None,
    record_key: Optional[tuple[str, str]] = None,
    new_fold: Optional[Callable[[], "_PageScan"]] = None,
    known_units: Optional[pgsql.Composable] = None,
    finish_sql: Optional[pgsql.Composable] = None,
    context: Optional[ScanContext] = None,
) -> GrepCount:
    """THE count of one grep call (PACT mcp-grep-page): the seam lane H's
    per-class count replaces.

    `match_sql` selects every text unit matching the query, once each, with
    the columns a Nomos decision class needs (node id, subtype, source,
    attrs, ...), `attr_filters` and path conditions included, and no LIMIT;
    `params` are its parameters. Every bound counts rows of a sequence whose
    length never depends on what the reader may not see or read:

    - R1 under Nomos `off` with no reader residual: the stored matching
      units, at most `scan_bound` (`COUNT_SCAN_BOUND`) of them, counted in
      SQL; past it `lower_bound` with `scan_cap: {"rows": ...}`. It is
      exact only because SQL decides the whole query: the call's compiled
      `where`, if any, is `exact`.
    - R1 under `audit`, or with a reader residual (`finish_sql`: a `where`
      clause SQL cannot decide): the stored matching units are read by one
      statement, at most `finish_rows` (or the match set the page already
      read whole, `known_units`), and every one is finished: decided at the
      scan site, folded, and the residual checked on read values. Exact
      only with the shared scanner's finished proof (the read was terminal
      and every candidate finished); past the bound `lower_bound` with
      `scan_cap: {"rows": finish_rows}`. `finish_rows` is
      `COUNT_FINISH_ROWS` with a residual; under `audit` with none it is
      `COUNT_SCAN_BOUND`, the stored-row bound the SQL count had (#3384),
      so audit counts no fewer readable units than before. That bound
      still counts stored rows, hidden ones included (disclosed). Under
      `audit` the SQL count above is kept when audit can hide no row of
      the generation (`_audit_hides_nothing`: no redacting rule, no
      identity claim or access context, no row without source lineage).
    - R2 (`enforce`): the scope is walked in node-id
      order (`walk_sql`), at most `COUNT_SCOPE_ROWS` rows, matching or not,
      and the rows passing the stored predicate are finished the same way.
      Exact only when the walk reached the scope's end (terminal keyset) and
      every candidate finished; past the bound, `lower_bound` with
      `scan_cap: {"scope_rows": ...}`: it depends on how many rows the scope
      holds, never on their content.

    Whenever the active policy can redact, the count is also `lower_bound`
    with reason `redaction` (decision 198a). A read past the statement
    timeout gives `timed_out` with no number.

    Lane H's per-class count replaces the R2 body with one call,
    `count_visible_by_class(conn, session, match_sql, params, row_kind="node",
    scan_bound=...)`, its basis mapped by `COUNT_BASIS`.
    """
    proof: Optional[EnumerationEvidence] = None
    try:
        if strict or finish_sql is not None:
            assert new_fold is not None and context is not None
            if strict:
                assert walk_sql is not None
                candidates, terminal = _walk_candidates(conn, walk_sql, params)
                method, cap = "terminal_keyset", {"scope_rows": COUNT_SCOPE_ROWS}
            else:
                assert finish_sql is not None
                candidates, terminal = _whole_candidates(
                    conn, known_units if known_units is not None else finish_sql, params,
                    bound=finish_rows,
                )
                method, cap = "whole_sql", {"rows": finish_rows}
            total, proof = _finish_count(
                context, candidates, terminal=terminal, method=method, fold=new_fold(),
            )
            basis = "per_row" if proof is not None else "lower_bound"
        else:
            total, basis = _count_stored(
                conn, known_units if known_units is not None else match_sql, params,
                scan_bound=scan_bound,
                record_key=record_key or ("u.node_id", ""),
            )
            cap = {"rows": scan_bound}
    except _Cancelled:
        return GrepCount(None, "timed_out", ("timeout",))
    reasons: list[str] = []
    if COUNT_BASIS[basis] == "lower_bound":
        reasons.append("scan_cap")
    if redaction_possible:
        reasons.append("redaction")
    return GrepCount(
        total, "lower_bound" if reasons else "exact", tuple(reasons),
        cap if "scan_cap" in reasons else None,
        proof,
    )


def _count_stored(
    conn: psycopg.Connection,
    match_sql: pgsql.Composable,
    params: Mapping[str, Any],
    *,
    scan_bound: int,
    record_key: tuple[str, str],
) -> tuple[int, str]:
    """R1: the records (or passages) the page would list, among the first
    `scan_bound` stored matching units, in one statement. `record_key` is
    `(key expression, LATERAL join)`: each unit's record, folding a passage
    into its one declared parent as the page does, NULL for a unit the page
    would drop."""
    key, lateral = record_key
    bounded = pgsql.SQL(
        "SELECT count(DISTINCT k.key) AS records, count(*) AS units FROM ("
        "SELECT " + key + " AS key FROM (SELECT * FROM ({}) grep_count LIMIT {}) u "
        + lateral + ") k"
    ).format(match_sql, pgsql.Literal(scan_bound + 1))
    row = _bounded(conn, bounded.as_string(conn), params)[0]
    if int(row["units"]) > scan_bound:
        return min(int(row["records"]), scan_bound), "lower_bound"
    return int(row["records"]), "exact"


def _page_proof(context: ScanContext, scan: "_PageScan") -> Optional[EnumerationEvidence]:
    """The shared scanner's finished proof for a page that enumerated every
    stored match: `scan.enumeration` names the terminal read (a whole SQL
    statement, or a keyset walk to the scope's end), and every candidate the
    scan took was decided at the scan site; finishing takes those decisions.
    None when no terminal read completed: a bounded, timed-out or complete
    (stopped early) page proves nothing about the matches it did not read."""
    if scan.enumeration is None:
        return None
    ids = list(scan.unit_seq)
    batch = ScanBatch(
        rows=tuple({"node_id": node_id} for node_id in ids),
        count_unit="text_unit", count_set="prefilter_candidates",
        count_basis="exact", total_count=len(ids),
        enumeration=EnumerationEvidence(
            domain=CANDIDATE_DOMAIN, scope_identity=context.scope_identity,
            method=scan.enumeration,
        ),
        terminal=True,
    )

    def finish(candidate: Mapping[str, Any]) -> Optional[FinishedCandidate]:
        got = scan.admitter.decided(str(candidate["node_id"]))
        return FinishedCandidate(rows=({"node_id": got["node_id"]},)) if got is not None else None

    proof = finish_scan_batch(context, batch, finish, finished_domain=FINISHED_DOMAIN).enumeration
    return proof if proof is not None and proof.finished else None


def _whole_candidates(
    conn: psycopg.Connection,
    finish_sql: pgsql.Composable,
    params: Mapping[str, Any],
    *,
    bound: int = COUNT_FINISH_ROWS,
) -> tuple[list[dict[str, Any]], bool]:
    """R1 with a residual or under `audit`: the stored matching units, read
    by one statement limited to `bound + 1` rows (parameter `grep_finish`;
    a known match set ignores it). Terminal when the statement ran to
    completion within `bound` rows."""
    rows = _bounded(
        conn, finish_sql.as_string(conn), {**params, "grep_finish": bound + 1},
    )
    return rows[:bound], len(rows) <= bound


def _walk_candidates(
    conn: psycopg.Connection,
    walk_sql: str,
    params: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], bool]:
    """R2: walk the scope in node-id order, at most `COUNT_SCOPE_ROWS` rows;
    the rows passing the stored predicate are the candidates. Terminal when
    the keyset walk reached the scope's end (a short window, or no row past
    the bound)."""
    p = {**params, "grep_skip": []}
    after = ""
    walked = 0
    window = PAGE_FIRST_WINDOW
    candidates: list[dict[str, Any]] = []
    while True:
        window = min(window, COUNT_SCOPE_ROWS - walked)
        found = _bounded_with(
            conn, walk_sql, {**p, "grep_after": after, "grep_window": window},
            settings=_INDEX_PLAN,
        )
        walked += len(found)
        candidates.extend(r for r in found if r["grep_ok"])
        if len(found) < window:
            return candidates, True
        after = str(found[-1]["node_id"])
        if walked >= COUNT_SCOPE_ROWS:
            return candidates, not _walk_has_more(conn, walk_sql, p, after)
        window *= PAGE_WINDOW_GROWTH


def _finish_count(
    context: ScanContext,
    candidates: Sequence[dict[str, Any]],
    *,
    terminal: bool,
    method: str,
    fold: "_PageScan",
) -> tuple[int, Optional[EnumerationEvidence]]:
    """Finish every candidate through the shared scanner, then fold.

    The scan site decides the candidates in batches (`fold.admitter`, one
    decision per unit, shared with the page); `finish_scan_batch` takes each
    one's decision, and only a terminal enumeration whose every candidate
    finished yields finished proof. Folding (passages into their one
    declared parent, the `where` residual on read values) stays grep's own:
    the scanner leaves grouping to adapters. Returns the folded records and
    the proof (None when the enumeration was bounded)."""
    admitter = fold.admitter
    for start in range(0, len(candidates), COUNT_SCOPE_ROWS):
        admitter.admit(list(candidates[start: start + COUNT_SCOPE_ROWS]))
    # The scanner deep-copies every candidate and finished row; it needs only
    # each unit's identity, so whole rows (attrs.text included) never cross.
    batch = ScanBatch(
        rows=tuple({"node_id": str(row["node_id"])} for row in candidates),
        count_unit="text_unit",
        count_set="prefilter_candidates",
        count_basis="exact" if terminal else "lower_bound",
        total_count=len(candidates),
        work_bounds=() if terminal else ("count_rows",),
        enumeration=EnumerationEvidence(
            domain=CANDIDATE_DOMAIN, scope_identity=context.scope_identity, method=method,
        ) if terminal else None,
        terminal=terminal,
    )

    def finish(candidate: Mapping[str, Any]) -> Optional[FinishedCandidate]:
        got = admitter.decided(str(candidate["node_id"]))
        return FinishedCandidate(rows=({"node_id": got["node_id"]},)) if got is not None else None

    finished = finish_scan_batch(
        context, batch, finish, finished_domain=FINISHED_DOMAIN if terminal else None,
    )
    rows = [admitter.decided(str(row["node_id"])) for row in finished.rows]
    fold.number(rows)
    fold.feed(rows, charge=False)
    proof = finished.enumeration
    return len(fold.folded), proof if proof is not None and proof.finished else None


# ---------- Nomos: scan site and serve site ----------

#: The policy-level redaction test and the read-unchanged check live in
#: `readable_text`, shared with lexical search (#3377).
_redaction_possible = redaction_possible


def _nomos_runtime(session: Session) -> bool:
    """Whether Nomos evaluates reads here (`audit` or `enforce`, not `off`)."""
    from okg.substrate.nomos.runtime_enforcement import deployment_requires_nomos_runtime

    return deployment_requires_nomos_runtime(session.deployment_name)


#: A generation-wide SQL test that a row CERTAINLY carries source lineage as
#: the read filter rebuilds it (`lineage_for_row` -> `normalize_source_lineage`
#: -> `source_ids_from_lineage`). Conservative: a row it does not accept may
#: still have lineage, never the reverse. With no lineage envelope in attrs,
#: the `source` column must hold a letter or digit; with one, it must be an
#: object not marked malformed whose `sources` is an array of objects, each
#: with a `source_id` string holding a letter or digit, at least one of them.
_CERTAIN_LINEAGE = """CASE
  WHEN r.attrs->'nomos_source_lineage' IS NULL
    OR jsonb_typeof(r.attrs->'nomos_source_lineage') = 'null'
  THEN coalesce(r.source ~ '[[:alnum:]]', FALSE)
  WHEN jsonb_typeof(r.attrs->'nomos_source_lineage') = 'object'
  THEN coalesce(r.attrs->'nomos_source_lineage'->>'status', 'ok') <> 'malformed'
    AND jsonb_typeof(r.attrs->'nomos_source_lineage'->'sources') = 'array'
    AND jsonb_array_length(r.attrs->'nomos_source_lineage'->'sources') > 0
    AND NOT EXISTS (
      SELECT 1 FROM jsonb_array_elements(r.attrs->'nomos_source_lineage'->'sources') e
       WHERE NOT (jsonb_typeof(e) = 'object'
                  AND jsonb_typeof(e->'source_id') = 'string'
                  AND (e->>'source_id') ~ '[[:alnum:]]'))
  ELSE FALSE
END"""

#: `_audit_hides_nothing` per published generation and segment pair. A
#: published generation's rows at its branch point do not change, so the
#: scan runs once per generation per process.
_AUDIT_HIDES_NOTHING: dict[tuple[Any, ...], bool] = {}
_AUDIT_HIDES_NOTHING_MAX = 256


def _audit_hides_nothing(
    conn: psycopg.Connection,
    session: Session,
    *,
    redaction: bool,
    segment: str,
    branch_point_value: Any,
) -> bool:
    """Whether Nomos `audit` can hide no row of this generation from this
    reader, so grep's single SQL count of stored rows equals the count of
    admitted ones (#3384 review).

    Audit shows a refused row; it still hides a row whose text or deciding
    values it masks and a row with no source lineage. So this holds only
    when the active policy cannot redact (`redaction` false), the session
    carries no identity claim or access context (whose fine-grained checks
    fail closed), and no live node or edge row of the generation lacks
    lineage. That last test is one EXISTS over the WHOLE generation (every
    node and edge row at the branch point), never over the query's match
    set, so the answer depends only on the policy and on the generation,
    never on which rows hold the term. It is cached per generation, and a
    scan past the statement timeout fails closed (per-row count)."""
    if redaction:
        return False
    if getattr(session, "nomos_identity_claim", None) is not None:
        return False
    if getattr(session, "nomos_access_context", None) is not None:
        return False
    edge_segment = resolve_edge_segment(conn)
    info = conn.info
    key = (info.host, info.port, info.dbname, str(session.generation_id),
           segment, edge_segment, str(branch_point_value))
    # One lookup, not `in` then `[]`: tool calls run in worker threads
    # (#3493), and another call's clear() between the two would raise.
    cached = _AUDIT_HIDES_NOTHING.get(key)
    if cached is not None:
        return cached
    live = "r.deleted = FALSE AND r.live_lo <= %(bp)s AND %(bp)s < r.live_hi"
    sql = (
        f"SELECT (EXISTS (SELECT 1 FROM {segment} r WHERE {live} "
        f"AND NOT ({_CERTAIN_LINEAGE})) OR EXISTS (SELECT 1 FROM {edge_segment} r "
        f"WHERE {live} AND NOT ({_CERTAIN_LINEAGE}))) AS lacking"
    )
    try:
        lacking = bool(_bounded(conn, sql, {"bp": branch_point_value})[0]["lacking"])
    except _Cancelled:
        lacking = True
    if len(_AUDIT_HIDES_NOTHING) >= _AUDIT_HIDES_NOTHING_MAX:
        _AUDIT_HIDES_NOTHING.clear()
    _AUDIT_HIDES_NOTHING[key] = not lacking
    return not lacking


def _strict_regime(session: Session) -> bool:
    """R2 exactly when Nomos enforcement is `enforce` (decision 260a, Jason
    10-02); R1 under `off` and `audit`.

    Under `enforce` the runtime hides or masks rows outside any rule (no
    active bundle, an invalid identity fact, a row without lineage, an
    identity or access-context session, a bundle activated after the
    check), so no bound may depend on how many stored rows match. Under
    `audit` a policy refusal is logged and the row is shown, so the bounds
    walk stored matching rows as with Nomos off. Rows `audit` still hides
    (no source lineage, masked deciding values) are neither charged against
    `PAGE_SCAN_BOUND` nor counted (#3384): the page charges admitted units
    and the count finishes every unit on its read values. The page
    itself still decides every row at the scan site and serves only what the
    read allows."""
    from okg.substrate.nomos.runtime_enforcement import deployment_nomos_enforcement_mode

    return deployment_nomos_enforcement_mode(session.deployment_name) == "enforce"


_read_unchanged = read_unchanged


def _scan_visible(
    conn: psycopg.Connection,
    session: Session,
    rows: list[dict[str, Any]],
    *,
    segment: str,
    branch_point_value: Any,
    parsed: GrepQuery,
    path_prefix: Optional[str],
) -> tuple[set[str], set[str], set[str]]:
    """SCAN SITE: `(visible ids, ids a lending record admits, ids with a
    lending record that carries a path)`.

    Visibility is decided once per Nomos decision class
    (`visible_ids_by_decision_class`, recorded `count`, ids only). A
    lending record counts only when it and its relation edge are visible.
    It admits a row when its path as the session reads it (overlaid; masked
    and unchanged under a redacting policy) passes every path condition: a
    record the reader may not see, or whose path it may not read, cannot
    lend its path (DESIGN.md section 5). The third set names the rows whose
    visible lending record carries a path at all, readable or not, so a
    `-path:` exclusion never treats a masked path as no path.
    """
    visible = visible_ids_by_decision_class(conn, session, rows, row_kind="node")
    lenders = sorted({
        lender for row in rows for lender, _edge in row.get("_lenders", ())
    })
    if not lenders or not (path_prefix or parsed.path_terms):
        return visible, set(), set()
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"SELECT node_id, subtype, attrs, text_hash, source, source_record_id, "
            f"source_revision FROM {segment} WHERE node_id = ANY(%(ids)s) "
            f"AND deleted = FALSE AND live_lo <= %(bp)s AND %(bp)s < live_hi",
            {"ids": lenders, "bp": branch_point_value},
        )
        lender_rows = cur.fetchall()
    shown = visible_ids_by_decision_class(conn, session, lender_rows, row_kind="node")
    lender_rows = [r for r in lender_rows if str(r["node_id"]) in shown]
    overlaid = apply_effective_attrs_to_rows(
        conn, [dict(r) for r in lender_rows], target_kind="node",
        target_id_field="node_id", generation_id=session.generation_id,
    )
    has_path = {
        str(r["node_id"]) for r in overlaid
        if isinstance((r.get("attrs") or {}).get("path"), str)
    }
    if _redaction_possible(conn, session):
        readable = _read_unchanged(
            conn, session, lender_rows, decisive=lambda _row: {"path"},
        )
    else:
        readable = overlaid
    paths = {
        str(r["node_id"]): (r.get("attrs") or {}).get("path") for r in readable
    }
    paths = {k: v for k, v in paths.items() if isinstance(v, str)}
    order = sorted(paths)
    passed = _path_checks(conn, parsed, path_prefix, [paths[k] for k in order])
    lender_ok = {k for k, ok in zip(order, passed) if ok}
    edges = sorted({edge for row in rows for _l, edge in row.get("_lenders", ())})
    edge_ok = visible_ids_by_decision_class(
        conn, session, [{"edge_id": edge} for edge in edges], row_kind="edge",
    )
    admitted = {
        str(row["node_id"]) for row in rows
        if any(
            lender in lender_ok and edge in edge_ok
            for lender, edge in row.get("_lenders", ())
        )
    }
    with_path = {
        str(row["node_id"]) for row in rows
        if any(
            lender in has_path and edge in edge_ok
            for lender, edge in row.get("_lenders", ())
        )
    }
    return visible, admitted, with_path


def _all_lenders(
    conn: psycopg.Connection,
    session: Session,
    rows: list[dict[str, Any]],
    *,
    segment: str,
    branch_point_value: Any,
) -> None:
    """Attach every declared lending record (and its edge) to each row.

    A `-path:` exclusion with no positive path condition reads every path a
    text unit has, so the lenders cannot come from a path-filtered branch.
    """
    declared = _declarations(conn, session)
    children = {child for _p, _e, child in declared}
    ids = sorted({
        str(r["node_id"]) for r in rows if str(r.get("subtype") or "") in children
    })
    if not ids:
        return
    params: dict[str, Any] = {"ids": ids, "bp": branch_point_value}
    values = []
    for index, (parent, edge, child) in enumerate(declared):
        params[f"dp{index}"], params[f"de{index}"], params[f"dc{index}"] = parent, edge, child
        values.append(f"(%(dp{index})s::text, %(de{index})s::text, %(dc{index})s::text)")
    edge_segment = resolve_edge_segment(conn)
    sql = (
        f"SELECT n.node_id::text AS unit_id, p.node_id::text AS lender_id, "
        f"e.edge_id::text AS lender_edge_id "
        f"FROM {edge_segment} e "
        f"JOIN {segment} p ON p.node_id = e.src AND p.deleted = FALSE "
        f"AND p.live_lo <= %(bp)s AND %(bp)s < p.live_hi "
        f"JOIN {segment} n ON n.node_id = e.dst AND n.deleted = FALSE "
        f"AND n.live_lo <= %(bp)s AND %(bp)s < n.live_hi "
        f"JOIN (VALUES {', '.join(values)}) AS d(parent, edge, child) "
        f"ON d.parent = p.subtype AND d.edge = e.edge_type AND d.child = n.subtype "
        f"WHERE e.dst = ANY(%(ids)s) AND e.deleted = FALSE "
        f"AND e.live_lo <= %(bp)s AND %(bp)s < e.live_hi"
    )
    try:
        found = _bounded(conn, sql, params)
    except _Cancelled as exc:
        raise GrepTimeout(
            "reading the records that lend a path ran past the grep statement timeout",
            data={"timeout_ms": STATEMENT_TIMEOUT_MS, "scopes": SCOPE_NAMES},
            fix_hint=f"narrow the call with {SCOPE_NAMES}",
        ) from exc
    by_id = {str(r["node_id"]): r for r in rows}
    for hit in found:
        row = by_id.get(str(hit["unit_id"]))
        if row is not None:
            row["_lenders"].append((str(hit["lender_id"]), str(hit["lender_edge_id"])))


def _serve_rows(
    conn: psycopg.Connection,
    session: Session,
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """SERVE SITE: the page's records and window passages, read and masked.

    `filter_rows_by_read_class` records `read` decisions (per class, per row
    where it must), masks each row with its own subtype classifier and keeps
    the order. A row the scan site admitted and the read hides fails the
    call (`search_fold_read_check_mismatch`), as the lexical fold does.
    """
    plain = [
        {k: v for k, v in row.items() if not k.startswith("_")} for row in rows
    ]
    read = filter_rows_by_read_class(conn, session, plain, row_kind="node")
    if len(read) != len(plain):
        raise SearchFoldReadCheckMismatch(
            f"the per-row read hid {len(plain) - len(read)} node row(s) that the "
            f"grep count had admitted; the page is not returned without them",
            data={"hidden_node_rows": len(plain) - len(read), "hidden_edge_rows": 0},
            fix_hint=(
                "retry the search; if it repeats, check the active Nomos policy "
                "bundle for rules that treat counting and reading differently"
            ),
        )
    return apply_effective_attrs_to_rows(
        conn, read, target_kind="node", target_id_field="node_id",
        generation_id=session.generation_id,
    )


def _recheck(
    conn: psycopg.Connection,
    parsed: GrepQuery,
    texts: list[str],
) -> list[tuple[bool, list[tuple[int, int]]]]:
    """For each text: whether the whole query still matches it, and the first
    match `(start, end)` of each positive term (0-based character offsets).

    Run by PostgreSQL, so the recheck and the windows use the same regex
    engine and case folding as the match set.
    """
    if not texts:
        return []
    params: dict[str, Any] = {"texts": texts}
    # A query of path terms only has no text term: every text unit passes.
    ok = " AND ".join(_term_sql(parsed, params, column="t.txt")) or "TRUE"
    spans = []
    for index, term in enumerate(parsed.terms):
        if term.negated:
            continue
        key = f"grep_t{index}"
        spans.append(
            f"regexp_instr(t.txt, %({key})s, 1, 1, 0, 'i') AS s{index}, "
            f"regexp_instr(t.txt, %({key})s, 1, 1, 1, 'i') AS e{index}"
        )
    sql = (
        f"SELECT {', '.join(['t.i', f'({ok}) AS ok', *spans])} "
        f"FROM unnest(%(texts)s::text[]) WITH ORDINALITY AS t(txt, i) ORDER BY t.i"
    )
    try:
        rows = _bounded(conn, sql, params)
    except _Cancelled as exc:
        raise GrepTimeout(
            "rechecking the served rows ran past the grep statement timeout",
            data={"timeout_ms": STATEMENT_TIMEOUT_MS, "scopes": SCOPE_NAMES},
            fix_hint=f"narrow the call with {SCOPE_NAMES}, or a lower limit",
        ) from exc
    out = []
    for row in rows:
        windows = []
        for index, term in enumerate(parsed.terms):
            if term.negated:
                continue
            start, end = int(row[f"s{index}"] or 0), int(row[f"e{index}"] or 0)
            if start > 0:
                windows.append((start - 1, end - 1))
        out.append((bool(row["ok"]), sorted(set(windows))))
    return out


# ---------- windows ----------

_LINE_BREAKS = "\n\r\x0b\x0c\x1c\x1d\x1e\x85\u2028\u2029"


def _line_start(text: str, at: int) -> int:
    return max((text.rfind(c, 0, at) for c in _LINE_BREAKS), default=-1) + 1


def _line_end(text: str, at: int) -> int:
    ends = [i for i in (text.find(c, at) for c in _LINE_BREAKS) if i >= 0]
    return min(ends) if ends else len(text)


def window_text(text: str, start: int, end: int, *, budget: int = WINDOW_CHARS) -> str:
    """The match's own line(s), cut to `budget` characters around the match."""
    lo, hi = _line_start(text, start), _line_end(text, max(start, end - 1))
    span = end - start
    if span >= budget:
        lo, hi = start, start + budget
    else:
        slack = budget - span
        lo = max(lo, start - slack // 2)
        hi = min(hi, lo + budget)
        lo = max(_line_start(text, start), min(lo, hi - budget))
    prefix = "..." if lo > _line_start(text, start) else ""
    suffix = "..." if hi < _line_end(text, max(start, end - 1)) else ""
    return f"{prefix}{text[lo:hi]}{suffix}"


def _excerpt(text: str, start: int, *, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    lo = max(0, min(start - max_chars // 3, len(text) - (max_chars - 3)))
    prefix = "..." if lo > 0 else ""
    room = max_chars - len(prefix)
    suffix = "..." if lo + room < len(text) else ""
    return f"{prefix}{text[lo: lo + room - len(suffix)]}{suffix}"


def _line_of(text: str, at: int) -> int:
    return text.count("\n", 0, at) + 1


# ---------- match lines (#3666 R5: one line per match, `rg -n` shaped) ----------

#: Match lines a row carries at most, in text order, record text first.
MAX_MATCH_LINES = 10
#: Matches of one term read from one text at most (one statement per page).
MATCH_SURFACES_PER_TEXT = 50
#: Characters of a match line kept around the match: a file's or code's
#: line, and a record line that is not prose (`PROSE_FIELDS`).
MATCH_LINE_CHARS = 200
#: A matched comment or prose paragraph of a record's composed text up to
#: this many characters is its match line whole (#3666, lane RD). The cap a
#: lexical or semantic search row keeps a matched comment or description
#: whole by (`UNIT_WHOLE_CHARS`), so every method shows one comment alike.
#: Measured: cut to `MATCH_LINE_CHARS`, a comment's answer sentence sat just
#: past the cut on a page using 5 KB of its 64 KB budget, and the record
#: was never opened; the page fit (D4) still bounds the whole reply.
PROSE_LINE_WHOLE_CHARS = UNIT_WHOLE_CHARS
#: Characters a longer comment or paragraph keeps, centred on its match and
#: cut at a word boundary (`centred_cut`): about 300 either side, two or
#: three sentences past the match. The line says how many it left out.
PROSE_LINE_CUT_CHARS = 600
#: Field labels of a record's composed text whose value is prose: a match
#: under one keeps its paragraph, as a dated comment keeps the comment.
PROSE_FIELDS = frozenset({
    "description", "summary", "body", "abstract", "message", "message_body",
    "details", "notes", "resolution", "comments", "recent_comments",
})
#: Distinct matched spellings a page reports at most.
MAX_SPELLINGS = 10
#: Characters of one reported spelling at most.
SPELLING_CHARS = 80
#: A dated comment line: `Author YYYY-MM-DDTHH:MM:SS.mmm+zzzz: text`.
_COMMENT_LINE = re.compile(
    r"(?P<author>[^\n:]{1,80}?)\s+(?P<date>\d{4}-\d{2}-\d{2})"
    r"T\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:?\d{2})?:[ \t]*"
)
#: A field label line of a record's composed text (`description: ...`):
#: a lowercase key at the line start, then a colon.
_FIELD_LABEL = re.compile(r"^(?P<label>[a-z][a-z0-9_]{1,39}):(?:[ \t]+|$)", re.MULTILINE)
#: Attributes that make a text a file's or code's: its lines are numbered.
_FILE_ATTRS = ("path", "doc_path")


def _match_surfaces(
    conn: psycopg.Connection, parsed: GrepQuery, texts: Sequence[str],
) -> Optional[list[list[tuple[int, str]]]]:
    """For each text, `(term index, matched text)` of each positive term's
    matches, in text order per term, at most `MATCH_SURFACES_PER_TEXT` per
    term. Matched by PostgreSQL with the call's own regexes and case
    folding, so no second regex engine places a line. None when the
    statement ran past the grep timeout."""
    out: list[list[tuple[int, str]]] = [[] for _ in texts]
    positive = [(i, t) for i, t in enumerate(parsed.terms) if not t.negated]
    if not texts or not positive:
        return out
    params: dict[str, Any] = {"texts": list(texts), "grep_mcap": MATCH_SURFACES_PER_TEXT}
    parts = []
    for index, term in positive:
        key = f"grep_m{index}"
        # One outer group: element 1 is the whole match, whatever groups
        # the term has.
        params[key] = f"({term.pg_regex})"
        parts.append(
            f"SELECT t.i, {index} AS term, m.k, m.v[1] AS s "
            f"FROM unnest(%(texts)s::text[]) WITH ORDINALITY AS t(txt, i) "
            f"CROSS JOIN LATERAL (SELECT r.v, r.k FROM regexp_matches(t.txt, %({key})s, 'gi') "
            f"WITH ORDINALITY AS r(v, k) ORDER BY r.k LIMIT %(grep_mcap)s) m"
        )
    try:
        rows = _bounded(conn, " UNION ALL ".join(parts) + " ORDER BY 1, 2, 3", params)
    except _Cancelled:
        return None
    for row in rows:
        surface = row["s"]
        if surface:
            out[int(row["i"]) - 1].append((int(row["term"]), str(surface)))
    return out


def _match_spans(text: str, surfaces: Sequence[tuple[int, str]]) -> list[tuple[int, int]]:
    """Where each matched text sits in `text`. The matches of one term do
    not overlap and come in text order, so each is found from the end of
    the one before it."""
    spans: set[tuple[int, int]] = set()
    cursor: dict[int, int] = {}
    for term, surface in surfaces:
        at = text.find(surface, cursor.get(term, 0))
        if at < 0:
            continue
        cursor[term] = at + len(surface)
        spans.add((at, at + len(surface)))
    return sorted(spans)


def _next_line_start(text: str, line_end: int) -> int:
    """Where the line after the one ending at `line_end` starts (a CR LF
    pair is one break)."""
    return line_end + (2 if text.startswith("\r\n", line_end) else 1)


def _starts_head(text: str, at: int) -> bool:
    """Whether the line at `at` opens a field (`label:`) or a dated comment."""
    end = _line_end(text, at)
    return bool(_FIELD_LABEL.match(text, at, end) or _COMMENT_LINE.match(text, at, end))


def _prose_unit(
    text: str,
    start: int,
    lo: int,
    labels: Sequence[tuple[int, str]],
    parts: ThreadParts,
) -> Optional[tuple[int, int, int, str, Optional[str], Optional[str]]]:
    """The comment or prose paragraph holding the match at `start` (on the
    line starting at `lo`), as `(key, lo, hi, field, author, date)`, or None
    when the match is in neither. `key` is the unit's start, the same for
    every match in it; `text[lo:hi]` is what the line shows.

    A comment is the dated comment `thread_parts` reads, the unit `inspect`
    and a search passage show: from its text to the next comment or field
    block, its `Author date:` head cut off unless the match is in it. A
    paragraph is the run of non-blank lines under a `PROSE_FIELDS` label,
    from that label's line (its label cut off) or a blank line, to the next
    blank, label or comment line."""
    comment = parts.comment_at(start)
    if comment is not None:
        body = comment.head if start < comment.start else comment.start
        return comment.head, body, comment.end, "comment", comment.author, comment.date[:10]
    above = [label for at, label in labels if at <= lo]
    if not above or above[-1] not in PROSE_FIELDS:
        return None
    first = lo
    while first > 0 and not _starts_head(text, first):
        previous_end = first - 2 if text.startswith("\r\n", first - 2) else first - 1
        previous = _line_start(text, max(0, previous_end))
        if previous == first or not text[previous:_line_end(text, previous)].strip():
            break
        first = previous
    last = _line_end(text, max(start, lo))
    while last < len(text):
        following = _next_line_start(text, last)
        if following >= len(text):
            break
        end = _line_end(text, following)
        if not text[following:end].strip() or _starts_head(text, following):
            break
        last = end
    body = first
    own_label = _FIELD_LABEL.match(text, first, _line_end(text, first))
    if own_label is not None and start >= own_label.end():
        body = own_label.end()
    return first, body, last, above[-1], None, None


def _prose_text(
    text: str, lo: int, hi: int, start: int, end: int, *, chars: Optional[int],
) -> tuple[str, int]:
    """`(shown text, characters left out)` of the comment or paragraph
    `text[lo:hi]`: whole up to `PROSE_LINE_WHOLE_CHARS`, else
    `PROSE_LINE_CUT_CHARS` centred on the match `start:end`, cut at a word
    boundary, with `...` where text was left out. `chars`, the caller's
    `max_chars` when given (D2, the one size control), is both caps."""
    whole_cap = chars if chars is not None else PROSE_LINE_WHOLE_CHARS
    cut_cap = chars if chars is not None else PROSE_LINE_CUT_CHARS
    whole = text[lo:hi].strip()
    if len(whole) <= whole_cap:
        return whole, 0
    cut_start, cut_end = centred_cut(text, (start, end), lo=lo, hi=hi, budget=cut_cap)
    shown = text[cut_start:cut_end].strip()
    prefix = "..." if text[lo:cut_start].strip() else ""
    suffix = "..." if text[cut_end:hi].strip() else ""
    return f"{prefix}{shown}{suffix}", max(0, len(whole) - len(shown))


def _unit_lines(
    text: str,
    spans: Sequence[tuple[int, int]],
    *,
    file_like: bool,
    start_line: Optional[int],
    prose_chars: Optional[int] = None,
) -> list[dict[str, Any]]:
    """One entry per line holding a match (`rg -n` shape), and where it is.

    A file's or code's line (`file_like`), and a record line that is not
    prose, is cut to `MATCH_LINE_CHARS` around its first match. In a
    record's composed text, a match in a dated comment or in a paragraph
    under a `PROSE_FIELDS` label shows that whole comment or paragraph
    (`_prose_unit`, `_prose_text`; #3666 lane RD), once however many matches
    it holds, and carries `chars_left`: the characters of it not shown (0:
    whole). `prose_chars` is the caller's `max_chars` when given.

    `field`: `comment` for a dated comment (`author` and `date` set, the
    `Author date:` head cut off); in a record's composed text the nearest
    `label:` line at or above it (`description`), the label cut off its own
    line; else `text`. `line`: the line number, for a file's or code's text
    only (`file_like`), counted from the unit's `start_line` when it has
    one, else from 1. The text and the author are the source's words: the
    caller fences them for an untrusted source. A label is a lowercase key
    and a date is digits, so neither carries prose."""
    labels = (
        [(m.start(), m.group("label")) for m in _FIELD_LABEL.finditer(text)]
        if not file_like else []
    )
    parts = thread_parts(text) if not file_like else None
    out: list[dict[str, Any]] = []
    seen: set[tuple[str, int]] = set()
    for start, end in spans:
        lo = _line_start(text, start)
        entry: dict[str, Any] = {
            "text": "", "field": "text", "line": None, "author": None, "date": None,
        }
        prose = (
            _prose_unit(text, start, lo, labels, parts) if parts is not None else None
        )
        if prose is not None:
            key, unit_lo, unit_hi, field, author, date = prose
            if ("unit", key) in seen:
                continue
            seen.add(("unit", key))
            entry.update(field=field, author=author, date=date)
            entry["text"], entry["chars_left"] = _prose_text(
                text, unit_lo, unit_hi, start, end, chars=prose_chars,
            )
            out.append(entry)
            continue
        if ("line", lo) in seen:
            continue
        seen.add(("line", lo))
        hi = _line_end(text, max(start, end - 1))
        head = 0
        comment = _COMMENT_LINE.match(text, lo, hi)
        if comment is not None:
            entry["field"] = "comment"
            entry["author"] = comment.group("author").strip()
            entry["date"] = comment.group("date")
            head = comment.end() - lo
        elif labels:
            above = [label for at, label in labels if at <= lo]
            if above:
                entry["field"] = above[-1]
            own_label = _FIELD_LABEL.match(text, lo, hi)
            if own_label is not None:
                head = own_label.end() - lo
        if start < lo + head:
            head = 0  # the match is in the head itself: keep the whole line
        entry["text"] = window_text(
            text[lo + head: hi], start - lo - head, end - lo - head,
            budget=MATCH_LINE_CHARS,
        ).strip()
        if file_like:
            first = start_line if start_line is not None else 1
            entry["line"] = first + text.count("\n", 0, start)
        out.append(entry)
    return out


_LOG = logging.getLogger(__name__)


#: The least statement timeout the record-line reads run under; with less
#: of a caller's deadline left they are skipped.
RECORD_LINES_MIN_TIMEOUT_MS = 500


class StatementTimeoutNotRestored(RuntimeError):
    """A server fault, not a refusal: the session's `statement_timeout`
    could not be put back, so the connection was closed and this call
    cannot go on. Nothing the caller sent is wrong; the call can be made
    again."""


def _record_lines_in_time(
    conn: psycopg.Connection, session: Session, places: Sequence[MatchPlace],
) -> list[Optional[int]]:
    """`record_match_lines` under the grep timeout, never failing the page
    for a line.

    Its statements run under the grep statement timeout, and no record is
    read once one timeout's worth of time has passed since it began. A
    record line is an extra: a statement that times out ends the reads
    there, the lines placed before it are kept, and each window without a
    record line says what its line counts (`line_basis`). Any other failure
    of the reads is a defect: it is logged with its traceback and the page
    is served with no record line for these places.

    The timeout is set on the session and put back afterwards, not inside
    a transaction block: the record read checks a generation out, whose
    read lease commits, which a transaction block forbids. So this runs
    only on an idle connection (inside a caller's transaction no record is
    read and the windows say their basis), and only with at least
    `RECORD_LINES_MIN_TIMEOUT_MS` of the budget left.

    Putting the timeout back is confirmed (`_restore_statement_timeout`).
    If it cannot be, the connection is closed and
    `StatementTimeoutNotRestored` is raised: the one way this step fails a
    call, so that no later request is handed a capped connection."""
    none: list[Optional[int]] = [None] * len(places)
    if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
        return none
    try:
        timeout_ms = _statement_timeout_ms()
    except _Cancelled:
        return none
    if timeout_ms < RECORD_LINES_MIN_TIMEOUT_MS:
        # Too little of a caller's deadline is left to read a record, and
        # a timeout that short could cancel the statement that lifts it.
        return none
    stop_at = time.monotonic() + timeout_ms / 1000
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT current_setting('statement_timeout') AS v")
        previous = cur.fetchone()["v"]
    lines = none
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT set_config('statement_timeout', %s, false)", (f"{timeout_ms}ms",),
            )
        lines = record_match_lines(conn, session, places, stop_at=stop_at)
    except Exception as exc:  # noqa: BLE001 - logged; the page says `line_basis`
        if not is_statement_timeout(exc):
            _LOG.exception("grep: record lines for %d match(es) failed", len(places))
    except BaseException:
        # An interrupt: still lift the limit (or close the connection), and
        # let the interrupt, not the guard's error, go on.
        with contextlib.suppress(Exception):
            _restore_statement_timeout(conn, previous)
        raise
    # Not in a `finally`: an error raised here must not replace one in flight.
    _restore_statement_timeout(conn, previous)
    return lines


def _restore_statement_timeout(conn: psycopg.Connection, previous: str) -> None:
    """Put the session's `statement_timeout` back and confirm it.

    The statement that lifts the limit runs under the limit, so it is tried
    three times and the value read back. With at least
    `RECORD_LINES_MIN_TIMEOUT_MS` to run in, a `SET` cannot plausibly be
    cancelled: failing here is a guard, not an expected outcome. If the
    value still cannot be confirmed, the connection is closed (its pool
    drops a closed connection when it is returned) and
    `StatementTimeoutNotRestored` is raised, so this call ends and no
    request ever runs on a session capped at the grep timeout."""
    for _attempt in range(3):
        try:
            if conn.info.transaction_status == psycopg.pq.TransactionStatus.INERROR:
                conn.rollback()
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    "SELECT set_config('statement_timeout', %s, false) AS v", (previous,),
                )
                cur.execute("SELECT current_setting('statement_timeout') AS v")
                if cur.fetchone()["v"] == previous:
                    return
        except Exception:  # noqa: BLE001 - retried, then the connection is closed
            continue
    _LOG.error(
        "grep: could not restore statement_timeout to %r; closing the connection",
        previous,
    )
    try:
        conn.close()
    except Exception:  # noqa: BLE001 - it is being thrown away either way
        _LOG.exception("grep: closing the connection failed")
    raise StatementTimeoutNotRestored(
        "the database session could not be reset after a search; "
        "the connection was closed, call again"
    )


# ---------- scan site: deciding candidates ----------

_UNIT_KEYS = (
    "node_id", "subtype", "attrs", "text_hash", "source",
    "source_record_id", "source_revision",
)


class _Admitter:
    """SCAN SITE of one call: decides each candidate text unit once.

    A unit is admitted when the reader may see it (`_scan_visible`, recorded
    `count`, per decision class) and the whole query still holds on the
    values the reader reads: the text (rechecked), the path (own or a
    visible lending record's), `excluded_path_globs` and `attr_filters`.
    Under a policy that can redact, a unit whose deciding values the read
    would change (dropped, masked) is never admitted (review B1/B2). The page
    and the count share the decisions, so a unit is decided at most once.
    """

    def __init__(
        self,
        conn: psycopg.Connection,
        session: Session,
        *,
        segment: str,
        branch_point_value: Any,
        parsed: GrepQuery,
        path_prefix: Optional[str],
        path_mode: Optional[str],
        scope: ResolvedScope,
        attr_filters: Mapping[str, Any],
        record_unit: bool,
        redaction: bool,
        own_path_only: bool = False,
    ) -> None:
        self.conn = conn
        self.session = session
        self.segment = segment
        self.bp = branch_point_value
        self.parsed = parsed
        self.path_prefix = path_prefix
        self.path_mode = path_mode
        self.scope = scope
        self.attr_filters = dict(attr_filters)
        self.record_unit = record_unit
        self.redaction = redaction
        #: The own-path pass (PACT filename-exact-hits): no lending record
        #: admits a unit; only its own read path does.
        self.own_path_only = own_path_only
        self._decided: dict[str, Optional[dict[str, Any]]] = {}
        self._child: dict[str, bool] = {}

    def declared_children(self, subtypes: Sequence[str]) -> set[str]:
        """Which of `subtypes` are declared passage children (record results only)."""
        if not self.record_unit:
            return set()
        unknown = sorted({s for s in subtypes if s not in self._child})
        if unknown:
            declared = passage_child_declarations(
                self.conn,
                catalog_version_id=self.session.catalog_version_id,
                child_subtypes=unknown,
            )
            for name in unknown:
                self._child[name] = name in declared
        return {s for s in subtypes if self._child.get(s)}

    def decided(self, node_id: str) -> Optional[dict[str, Any]]:
        """This call's decision on one unit already decided: the unit as the
        reader reads it, or None when refused. An undecided unit raises."""
        if node_id not in self._decided:
            raise KeyError(f"grep scan site has not decided {node_id!r}")
        got = self._decided[node_id]
        return dict(got) if got is not None else None

    def admit(self, rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
        """The admitted units among `rows`, in order, as the reader reads them."""
        fresh: list[Mapping[str, Any]] = []
        queued: set[str] = set()
        for row in rows:
            node_id = str(row["node_id"])
            if node_id not in self._decided and node_id not in queued:
                fresh.append(row)
                queued.add(node_id)
        if fresh:
            self._decide(fresh)
        out = []
        for row in rows:
            got = self._decided.get(str(row["node_id"]))
            if got is not None:
                out.append(dict(got))
        return out

    def _decide(self, rows: Sequence[Mapping[str, Any]]) -> None:
        conn, session, parsed = self.conn, self.session, self.parsed
        path_mode, path_prefix = self.path_mode, self.path_prefix
        units = [
            {**{key: row.get(key) for key in _UNIT_KEYS}, "_lenders": []}
            for row in rows
        ]
        if path_mode and not self.own_path_only:
            # Every declared lending record of each unit; the scan site keeps
            # those the reader may see and whose read path passes.
            _all_lenders(conn, session, units, segment=self.segment, branch_point_value=self.bp)
        visible, lender_admitted, lender_with_path = _scan_visible(
            conn, session, units,
            segment=self.segment, branch_point_value=self.bp,
            parsed=parsed, path_prefix=path_prefix,
        )
        counted = [
            {k: v for k, v in unit.items() if not k.startswith("_")}
            for unit in units if str(unit["node_id"]) in visible
        ]
        children = self.declared_children(
            sorted({str(r.get("subtype") or "") for r in counted})
        )
        attr_filters = self.attr_filters
        scope = self.scope

        def decisive(row: Mapping[str, Any]) -> set[str]:
            """The attributes that decide whether this row is a result."""
            keys = {"text"}
            if path_mode and str(row["node_id"]) not in lender_admitted:
                keys.add("path")
            if scope.excluded_path_globs:
                keys.add("path")
            if attr_filters and str(row.get("subtype") or "") not in children:
                keys.update(str(k) for k in attr_filters)
            return keys

        if self.redaction:
            # Every decision is made on the values the session would read,
            # and a row whose deciding values the read would change is never
            # admitted: otherwise a pattern that matches either the secret or
            # the mask is a yes/no oracle on the secret.
            counted = _read_unchanged(conn, session, counted, decisive=decisive)
        else:
            counted = apply_effective_attrs_to_rows(
                conn, counted, target_kind="node", target_id_field="node_id",
                generation_id=session.generation_id,
            )
        checks = _recheck(
            conn, parsed, [str((r.get("attrs") or {}).get("text") or "") for r in counted],
        )
        counted = [r for r, (ok, _w) in zip(counted, checks) if ok]
        if path_mode:
            # Every path decision is made on the path the reader reads.
            own_paths = {
                str(r["node_id"]): (r.get("attrs") or {}).get("path") for r in counted
            }
            order = [k for k, v in own_paths.items() if isinstance(v, str)]
            own_ok = {
                k for k, ok in zip(order, _path_checks(
                    conn, parsed, path_prefix, [own_paths[k] for k in order],
                )) if ok
            }

            def path_admitted(row: Mapping[str, Any]) -> bool:
                node_id = str(row["node_id"])
                if node_id in lender_admitted or node_id in own_ok:
                    return True
                # `-path:` only: a unit with no path at all, and no visible
                # lending record carrying one, is not excluded.
                return (
                    path_mode == "exclude"
                    and not isinstance(own_paths.get(node_id), str)
                    and node_id not in lender_with_path
                )

            counted = [r for r in counted if path_admitted(r)]
        if scope.excluded_path_globs:
            from okg.substrate.mcp.search_profiles import excluded_by_path_globs

            counted = [
                r for r in counted
                if not excluded_by_path_globs(r.get("attrs"), scope.excluded_path_globs)
            ]
        if attr_filters:
            from okg.substrate.mcp.search import _matches_filter

            counted = [
                r for r in counted
                if str(r.get("subtype") or "") in children
                or _matches_filter(r, {"attr_filters": attr_filters})
            ]
        admitted = {str(r["node_id"]): r for r in counted}
        for unit in units:
            node_id = str(unit["node_id"])
            self._decided[node_id] = admitted.get(node_id)


# ---------- the page ----------

class _PageScan:
    """One page, built best-first from candidates fed in scan order.

    Each candidate gets the next scan position; an admitted unit's sort key
    is its position, so a record (the best of its own text and its folded
    passages, search-result-shape's `fold_members`) sorts where its best
    unit was found. A later candidate can only add a record after every
    record already folded, never move one: once `offset + limit + 1` records
    exist, the page and `next_offset` are final.

    A continuation (`resume_at`, `exclude`) replays the same scan: every row
    the scan reads (each taken BM25 row, in or out of scope, matching or
    not; each walked row) gets a sequence number (`number`), the rows before
    `resume_at` are
    skipped without being charged or decided, and the records `exclude`
    names (the ones earlier pages listed) are left out of the fold. The
    sequence does not depend on window sizes (a window takes its rows in
    (score, node id) order and the next starts right after them), so the
    replay reaches the row the earlier call stopped at, and the bounds count
    only from there.
    """

    def __init__(
        self,
        conn: psycopg.Connection,
        session: Session,
        admitter: _Admitter,
        *,
        offset: int,
        limit: int,
        fold_filtered: bool,
        segment: str,
        branch_point_value: Any,
        mask_fetched_parents: bool,
        parent_admitted: Callable[[Mapping[str, Any]], bool],
        takes: set[str],
        batch_max: int = PAGE_BATCH_MAX,
        resume_at: int = 0,
        exclude: frozenset[str] = frozenset(),
        path: Optional[Mapping[str, Any]] = None,
        row_admitted: Optional[Callable[[Mapping[str, Any]], bool]] = None,
        charge_admitted: bool = False,
        parent_matches_outside_scope: Optional[Callable[[Mapping[str, Any]], bool]] = None,
    ) -> None:
        #: Whether `feed` charges only admitted units against
        #: `PAGE_SCAN_BOUND` (Nomos `audit` and `enforce`, #3384).
        self.charge_admitted = charge_admitted
        #: Consecutive batches that admitted no unit (`PAGE_BATCH_DRY_MAX`).
        self.dry = 0
        #: Rows decided and folded at once: small for a page (it stops as
        #: soon as it is complete), large for the R2 count, which folds every
        #: walked match and pays one fold and one parent read per batch.
        self.batch_max = batch_max
        #: The reader residual of a `where` (lexical's `_row_filter` rule):
        #: checked on every folded row as the reader reads it, and a passage
        #: whose readable parent it rejects is dropped, never listed on its
        #: own (design `read-model` D6). None when the call has no `where`.
        self.row_admitted = row_admitted
        #: Issue #3466: a parent the `where` admits but the profile scopes
        #: out (a content profile without `document`). Its passages stay
        #: their own rows, judged through it, as lexical's records fold does.
        self.parent_matches_outside_scope = parent_matches_outside_scope
        #: How the scan enumerated every stored match, when it did: a whole
        #: SQL read ("whole_sql") or a keyset walk to the scope's end
        #: ("terminal_keyset"); None while no terminal read completed.
        self.enumeration: Optional[str] = None
        self.conn = conn
        self.session = session
        self.admitter = admitter
        self.need = offset + limit + 1
        self.fold_filtered = fold_filtered
        self.segment = segment
        self.bp = branch_point_value
        self.mask_fetched_parents = mask_fetched_parents
        self.parent_admitted = parent_admitted
        self.takes = takes
        self.records: dict[str, dict[str, Any]] = {}
        self.passages: dict[str, dict[str, Any]] = {}
        self.links: dict[str, Any] = {}
        self.fetched: dict[str, dict[str, Any]] = {}
        self.folded: list[dict[str, Any]] = []
        #: Candidate rows decided by the scan site for this page (work).
        self.checked = 0
        #: Rows charged against `PAGE_SCAN_BOUND` (see `feed`).
        self.charged = 0
        #: Scope rows the node-id walk read past a continuation's replay
        #: (what an `id_rows` stop read; Format v10 item G2).
        self.walked = 0
        #: Rows the BM25 windows returned past a continuation's replay (what
        #: an `index_rows` stop read; review P2-2).
        self.index_read = 0
        #: The node ids of the whole stored match set, when one read
        #: returned all of it (R1 only); the count reuses it.
        self.match_set_ids: Optional[list[str]] = None
        #: Whether a further chargeable row exists past the bound.
        self.bound_hit = False
        #: Whether the page took rows from a tie cut by the index-row bound.
        self.tie_cut = False
        self.position = 0
        #: Each candidate's stored text (before overlay or masking).
        self.stored_text: dict[str, Optional[str]] = {}
        #: Why the scan stopped before the page filled: None (every match was
        #: scanned), "candidate_rows", "index_rows" or "timeout".
        self.stop: Optional[str] = None
        self.edge_segment: Optional[str] = None
        #: How the page's rows were scored: "index" (the BM25 index scan's
        #: own score), "expression" (`<@>` per window row, off the index
        #: scan) or "match_set" (the page finished from the whole small
        #: match set, `<@>` per row).
        self.rank_scan = "index"
        #: The next sequence number to give (`number`), and the first row
        #: of the sequence not yet taken (`feed`).
        self.next_seq = 0
        self.seq = 0
        self.resume_at = resume_at
        self.exclude = exclude
        #: Each read-path choice the scan made (`pinned`), carried in a
        #: continuation's cursor so a replay makes the same ones: a whole
        #: match set numbers only its matches, a window or walk every row
        #: read, so a choice a timeout flipped would misplace the resume point.
        self.path: dict[str, Any] = dict(path or {})
        #: Each decided unit's sequence number.
        self.unit_seq: dict[str, int] = {}
        #: The sequence number of the first row of a tie the last BM25
        #: window cut (`tie_cut`); a continuation starts there.
        self.tie_seq: Optional[int] = None
        #: Whether this call's unordered read of the whole match set (no
        #: unit skipped) ran past the statement timeout. The R1 count reads
        #: the same match set further, so it would too: it is not run.
        self.match_set_timed_out = bool((path or {}).get("whole_read_timed_out"))
        #: Whether an `index_rows` stop came from pg_textsearch's per-scan
        #: limit rather than this call's budget: no continuation can read on.
        self.index_limit = False

    @property
    def complete(self) -> bool:
        return len(self.folded) >= self.need

    def pinned(
        self, key: str, read: Callable[[], Any], usable: Callable[[Any], bool],
    ) -> Any:
        """`read()` when usable, else None, as this chain first decided: a
        choice an earlier call made is made again (not read at all when it
        was not taken; `_PathLost` when it was and is not usable now)."""
        want = self.path.get(key)
        if want is False:
            return None
        got = read()
        ok = usable(got)
        if want is True and not ok:
            raise _PathLost(key)
        self.path[key] = ok
        return got if ok else None

    @property
    def skipping(self) -> bool:
        """Whether the scan is still before a continuation's resume point."""
        return self.seq < self.resume_at

    def resume_point(self, listed: int) -> int:
        """Where a continuation of this page starts: the first row of the
        earliest record folded but not among the first `listed`, else the
        row the scan stopped at, and never past a tie the last window cut.
        Every row before it was decided here or by an earlier page, so the
        records it gave are listed (and excluded) or it gave none."""
        point = self.seq
        if self.tie_seq is not None:
            point = min(point, self.tie_seq)
        unlisted = {str(row["node_id"]) for row in self.folded[listed:]}
        for unit, seq in self.unit_seq.items():
            link = self.links.get(unit)
            owner = link.parent_id if link is not None and link.parent_id in unlisted else unit
            if owner in unlisted:
                point = min(point, seq)
        return point

    def number(self, rows: Sequence[dict[str, Any]]) -> None:
        """Give `rows`, every row the scan read next in its order (taken BM25
        rows in or out of scope, matching or not; every walked row), their
        sequence numbers (`_seq`). The sequence is what a continuation's
        resume point counts."""
        for row in rows:
            row["_seq"] = self.next_seq
            self.next_seq += 1

    def feed(self, rows: Sequence[dict[str, Any]], *, charge: bool = True) -> None:
        """Take `rows` (numbered, in scan order) batch by batch, until every
        one was taken, the page is complete, or a row past the bound turns up.

        With `charge` and Nomos `off` (`charge_admitted` false), every row
        taken counts against `PAGE_SCAN_BOUND` whether or not the scan site
        admits it, and the bound is reported hit only when a further row
        exists (the next row fed). A row whose `grep_ok` is false is charged
        but never decided. Under `audit` and `enforce` (`charge_admitted`,
        #3384) each batch is decided first and only the units the scan site
        admits are charged: a row the reader may not see, or whose text it
        may not read as stored, never moves the bound, and the bound is
        reported hit only when a further admitted unit exists; the page
        then stops right before that unit. Rows before a continuation's
        resume point are skipped, neither charged nor decided. Afterwards
        `seq` is the first row not taken: the first row fed but left, else
        the end of the numbered rows."""
        i = 0
        while i < len(rows) and rows[i]["_seq"] < self.resume_at:
            i += 1
        by_admitted = charge and self.charge_admitted
        while i < len(rows) and not self.complete and not self.bound_hit:
            if charge and not by_admitted and self.charged >= PAGE_SCAN_BOUND:
                self.bound_hit = True
                break
            missing = self.need - len(self.folded)
            size = min(max(PAGE_BATCH_MULTIPLE * missing, PAGE_BATCH_MIN), self.batch_max)
            if charge and not by_admitted:
                size = min(size, PAGE_SCAN_BOUND - self.charged)
            if self.charge_admitted and self.dry:
                # Batches that admitted nothing: double, up to the cap. A
                # unit past the bound inside a grown batch leaves the rows
                # after it decided here and again by the continuation.
                size = max(size, min(size << self.dry, PAGE_BATCH_DRY_MAX))
            elif by_admitted:
                # Never past the first unit that could exceed the bound, so
                # no row after the one that reports it is decided.
                size = min(size, PAGE_SCAN_BOUND - self.charged + 1)
            batch = rows[i: i + size]
            if by_admitted:
                admitted = {
                    str(row["node_id"]) for row in self.admitter.admit(
                        [r for r in batch if r.get("grep_ok", True)]
                    )
                }
                for k, row in enumerate(batch):
                    if not row.get("grep_ok", True) or str(row["node_id"]) not in admitted:
                        continue
                    if self.charged >= PAGE_SCAN_BOUND:
                        # A further admitted unit past the bound: stop before it.
                        self.bound_hit = True
                        batch = batch[:k]
                        break
                    self.charged += 1
            elif charge:
                self.charged += len(batch)
            i += len(batch)
            for row in batch:
                if row.get("grep_ok", True):
                    self.unit_seq.setdefault(str(row["node_id"]), row["_seq"])
            decide = [r for r in batch if r.get("grep_ok", True)]
            if decide:
                self._decide(decide)
        self.seq = rows[i]["_seq"] if i < len(rows) else self.next_seq

    def _decide(self, batch: Sequence[dict[str, Any]]) -> None:
        position: dict[str, int] = {}
        for row in batch:
            node_id = str(row["node_id"])
            position[node_id] = self.position
            self.position += 1
            self.stored_text[node_id] = (row.get("attrs") or {}).get("text")
        self.checked += len(batch)
        admitted = self.admitter.admit(batch)
        self.dry = 0 if admitted else min(self.dry + 1, 16)
        for row in admitted:
            row["score"] = float(position[str(row["node_id"])])
            row["expansion_rank"] = 0
        children = self.admitter.declared_children(
            sorted({str(r.get("subtype") or "") for r in admitted})
        )
        fresh_passages = []
        for row in admitted:
            if str(row.get("subtype") or "") in children:
                self.passages[str(row["node_id"])] = row
                fresh_passages.append(row)
            else:
                self.records[str(row["node_id"])] = row
        if fresh_passages:
            if self.edge_segment is None:
                self.edge_segment = resolve_edge_segment(self.conn)
            links, fetched = resolve_fold_links(
                self.conn, self.session,
                passages=fresh_passages,
                known_parents=self.records,
                node_segment=self.segment,
                edge_segment=self.edge_segment,
                branch_point_value=self.bp,
                check=CHECK_CLASS,
                mask_fetched_parents=self.mask_fetched_parents,
                edge_scan_cap=2 * PAGE_SCAN_BOUND,
            )
            self.links.update(links)
            for parent_id, parent in fetched.items():
                self.fetched.setdefault(parent_id, parent)
        self.folded = fold_members(
            records=list(self.records.values()),
            passages=list(self.passages.values()),
            links=self.links,
            fetched_parents=self.fetched,
            parent_admitted=self.parent_admitted,
            keep_unfolded=not self.fold_filtered,
            takes_passages=lambda row: str(row.get("subtype") or "") in self.takes,
            drop_when_parent_rejected=self.row_admitted is not None,
            keep_when_parent_matches=self.parent_matches_outside_scope,
        )
        through_parent = self._through_parent()
        # Search-result-shape rule 4: a passage that does not fold is listed
        # only if it passes the selections itself (attr_filters, scope).
        # Under a `where`, every row also passes its residual on read values,
        # as lexical's record view does (only TRUE admits). A passage kept
        # through a record outside the profile (issue #3466) skips both: it
        # is judged through that record, as a `where` judges a passage (D6).
        self.folded = [
            row for row in self.folded
            if str(row["node_id"]) not in self.exclude
            and (
                str(row["node_id"]) in through_parent
                or (
                    (str(row["node_id"]) not in self.passages or self.parent_admitted(row))
                    and (self.row_admitted is None or self.row_admitted(row))
                )
            )
        ]

    def _through_parent(self) -> set[str]:
        """The passages kept as their own rows through a parent outside the
        profile that passes the `where` (issue #3466)."""
        if self.parent_matches_outside_scope is None:
            return set()
        kept: set[str] = set()
        for node_id in self.passages:
            link = self.links.get(node_id)
            parent = self.fetched.get(link.parent_id) if link is not None else None
            if parent is not None and self.parent_matches_outside_scope(parent):
                kept.add(node_id)
        return kept


def _scan_bm25(
    scan: _PageScan,
    conn: psycopg.Connection,
    *,
    segment: str,
    rank_where: Sequence[str],
    ok_sql: str,
    params: Mapping[str, Any],
    words: Sequence[str],
    probe: Optional[Callable[[], Optional[list[dict[str, Any]]]]],
    strict: bool,
) -> tuple[bool, set[str], Optional[list[dict[str, Any]]]]:
    """Feed the page from the BM25 index's ordered scan, best first.

    Returns `(exhausted, ids, match_set)`: whether every BM25 match in scope
    was scanned, the ids of the BM25 matches that passed the predicate (the
    zero-score pass skips them), and the whole match set when `probe` found
    it small. Once `PAGE_PROBE_AFTER` index rows were read without filling
    the page, `probe` reads the match set once: a selective query (a phrase
    of common words, few matches) then finishes from it rather than walk
    the index's many partial matches. Windows grow from `PAGE_FIRST_WINDOW` rows,
    `PAGE_WINDOW_GROWTH` times each step, until `PAGE_INDEX_ROWS` rows of
    the scan were read. A window takes only its rows that score strictly
    better than its last row and are sorted by (score, node id); the next
    window starts (`OFFSET`) right after them, so rows tied across a
    window's edge are read again together and the order is exact whatever
    order the index returns ties in. Cost of a straddling tie of T rows: the
    next windows restart at the tie's first row (`OFFSET`, never 0) and
    double until the tie fits, so they re-read up to about 2T rows. The last
    window allowed serves its tied rows in node-id order and reports
    `index_rows` (`tie_cut`). A continuation's replay (`scan.skipping`)
    does not count against `PAGE_INDEX_ROWS`: rows read in windows that ended
    before the resume point (`base`) are added to it.

    The index can return one row more than once (issue #3335). A window
    reads every version of a node, so a row is a node id and its `live_lo`
    (`_WINDOW_ROW`, the segment's primary key). As soon as a window is read,
    its repeated rows and the rows an earlier window settled are dropped
    (`fresh`); the matches, the settled and tied rows, their numbers and
    the charge against the candidate bound count fresh rows only. The work
    bound counts rows as returned: `read` against `PAGE_INDEX_ROWS`, and the
    replay's share of it (`base`), which frees a replay window's rows before
    the resume point and the rows it dropped. The progress rule at the end
    of the loop bounds the number of window queries a call runs, replay or
    not. The tests of whether the window
    came back short or reached
    non-matches also look at the rows as returned. The next `OFFSET`
    advances by the distinct rows settled: a repeat took a position the
    next statement need not repeat, so counting it could skip an unread
    row, while counting too few only re-reads settled rows, which are
    dropped again.
    """
    _segment, index_name = resolve_node_text_bm25_index(conn)
    window = PAGE_FIRST_WINDOW
    taken = 0
    read = 0
    #: Rows read that a continuation's replay does not charge.
    base = 0
    probed = False
    index_score: Optional[bool] = None
    passed: set[str] = set()
    #: Row versions settled by this scan's windows so far (issue #3335).
    settled_ids: set[tuple[str, str]] = set()
    while True:
        # `read` counts every row the windows returned, re-reads of a tie and
        # repeats included, so neither can make a call read more than the
        # bound.
        # A continuation's windows start past earlier pages' positions; every
        # window (and the one-row probe after it) still ends below
        # pg_textsearch's per-scan limit, which it would cut silently.
        cap = PG_TEXTSEARCH_SCAN_LIMIT - 1 - taken
        window = min(window, PAGE_INDEX_ROWS + base - read, cap)
        if window <= 0:
            scan.stop = "index_rows"
            scan.index_limit = cap <= 0
            return False, passed, None
        p = {
            **params, "grep_rank_q": " ".join(words),
            "grep_window": window, "grep_offset": taken,
        }
        sql = _window_sql(
            segment=segment, index_name=index_name, rank_where=rank_where,
            ok_sql=ok_sql, index_score=index_score is not False,
        )
        if index_score is None:
            # Decided once per call (and once per continuation chain), so
            # every window scores the same way.
            index_score = scan.path.get("index_score")
            if index_score is None:
                index_score = _index_ordered(conn, sql, p, index_name)
            scan.path["index_score"] = index_score
            if not index_score:
                scan.rank_scan = "expression"
                sql = _window_sql(
                    segment=segment, index_name=index_name, rank_where=rank_where,
                    ok_sql=ok_sql, index_score=False,
                )
        try:
            rows = _bounded_with(conn, sql, p, settings=_INDEX_PLAN if index_score else ())
        except _Cancelled:
            scan.stop = "timeout"
            return False, passed, None
        # Issue #3335: drop the window's repeats and the rows an earlier
        # window settled before anything counts them.
        fresh = drop_repeated_rows(rows, seen=set(settled_ids), key=_WINDOW_ROW)
        read += len(rows)
        matches = [r for r in fresh if float(r["grep_score"] or 0.0) < 0]
        # The index scan returns matches only; a plan off the index returns
        # non-matches (score 0) after them. Either way a short window, or a
        # non-match, means every match in scope is in this window. Both are
        # judged on the rows as returned: repeats fill a window too.
        exhausted = len(rows) < window or any(
            float(r["grep_score"] or 0.0) >= 0 for r in rows
        )
        matches.sort(key=lambda r: (float(r["grep_score"]), str(r["node_id"])))
        freed = base
        if scan.next_seq < scan.resume_at:
            # A continuation's replay: this window's rows up to the resume
            # point do not count against `PAGE_INDEX_ROWS`, nor do the rows
            # it dropped as repeats or settled re-reads (issue #3335), all
            # counted as returned, like `read`. A replay window can thus be
            # free; the progress rule at the end of the loop is what keeps
            # free windows from repeating.
            dropped = len(rows) - len(fresh)
            base += min(len(rows), scan.resume_at - scan.next_seq + dropped)
        #: Rows this pass counted against `PAGE_INDEX_ROWS` (0 when the
        #: replay freed them all).
        charged = len(rows) - (base - freed)
        scan.index_read = read - base
        last_window = read >= PAGE_INDEX_ROWS + base or window >= cap
        scan.index_limit = window >= cap
        if last_window and not exhausted:
            # A bound is reported only when a further row exists past it.
            try:
                further = _bounded_with(
                    conn, sql, {**p, "grep_window": 1, "grep_offset": taken + window},
                    settings=_INDEX_PLAN if index_score else (),
                )
            except _Cancelled:
                further = [{"grep_score": -1.0}]
            exhausted = not any(float(r["grep_score"] or 0.0) < 0 for r in further)
        if exhausted or not matches:
            settled, tail = matches, []
        else:
            edge = float(matches[-1]["grep_score"])
            settled = [r for r in matches if float(r["grep_score"]) < edge]
            tail = matches[len(settled):]
        # `fresh` holds no repeat and no settled row: this only records the
        # rows settled now.
        drop_repeated_rows(settled, seen=settled_ids, key=_WINDOW_ROW)
        taken += len(settled)
        candidates = [r for r in settled if r["grep_ok"]]
        passed.update(str(r["node_id"]) for r in candidates)
        # R1 feeds the rows that pass the stored predicate; R2 every
        # in-scope BM25 row. With Nomos off each fed row is charged; under
        # `audit` and `enforce` only admitted units are (`feed`, #3384).
        scan.number(settled)
        scan.feed([r for r in settled if r["grep_in_scope"]] if strict else candidates)
        if not scan.complete and not scan.bound_hit and last_window and tail:
            # The last window allowed ends inside a run of tied rows (the
            # index rounds lengths, so ties are common). Its tied rows are
            # served in node-id order rather than dropped, and the result
            # says the index-row bound was hit: tied rows past the window
            # may exist.
            scan.tie_cut = True
            scan.tie_seq = scan.next_seq
            scan.number(tail)
            tied = [r for r in tail if r["grep_ok"]]
            passed.update(str(r["node_id"]) for r in tied)
            scan.feed([r for r in tail if r["grep_in_scope"]] if strict else tied)
        if scan.complete:
            return False, passed, None
        if scan.bound_hit:
            scan.stop = "candidate_rows"
            return False, passed, None
        if exhausted:
            return True, passed, None
        if probe is not None and not probed and read >= PAGE_PROBE_AFTER:
            probed = True
            whole = probe()
            if whole is not None:
                return False, passed, whole
        if last_window:
            scan.stop = "index_rows"
            return False, passed, None
        # Progress rule (issue #3335). Every pass numbers a row (`next_seq`
        # grows; a row is numbered once), counts a row against
        # `PAGE_INDEX_ROWS` (which ends the scan once it is spent), runs a
        # window strictly larger than the one before it at the same offset,
        # or ends. Windows are capped below pg_textsearch's per-scan limit,
        # so a call runs at most (rows numbered + rows counted + 1) x (log2
        # of that limit + 1) window queries. A replay pass the replay freed
        # entirely that numbered nothing and cannot grow its window would
        # run the same statement again: it stops on the index-row bound,
        # disclosed like any other stop.
        grown = min(window * PAGE_WINDOW_GROWTH, PAGE_INDEX_ROWS + base - read, cap)
        if not settled and not charged and grown <= window:
            scan.stop = "index_rows"
            return False, passed, None
        window *= PAGE_WINDOW_GROWTH


def _small_match_set(
    conn: psycopg.Connection,
    *,
    sql: str,
    params: Mapping[str, Any],
    words: Sequence[str],
    scan: Optional["_PageScan"] = None,
) -> Optional[list[dict[str, Any]]]:
    """The whole match set, sorted by (score, node id), when it holds at most
    `PAGE_SMALL_SET` units; None when it is larger or its read passes the
    timeout. Each unit is scored on its stored text with the `<@>`
    expression. That is the index's number for rows in its segments (to
    about 2e-6); for rows still in its in-memory buffer the expression can
    score up to about 6% better. When the windows fed rows before this read,
    those keep their places and the rest follow in expression order, so the
    sequence stays fixed across pages but near a window's edge it can differ
    slightly from the index's own order (disclosed). BM25 matches come
    first and units no word reaches (score 0) follow in node-id order, as on
    the window path."""
    try:
        rows = _bounded(conn, sql, {**params, "grep_limit": PAGE_SMALL_SET + 1})
    except _Cancelled:
        if scan is not None:
            scan.match_set_timed_out = True
        return None
    if len(rows) > PAGE_SMALL_SET:
        return None
    if rows:
        _segment, index_name = resolve_node_text_bm25_index(conn)
        literal = index_name.replace("'", "''")
        # Scored twice on an old library, keeping the better (issue #3666).
        text_score = steady_bm25_score_sql(
            f"t.txt <@> to_bm25query(%(grep_rank_q)s, '{literal}')"
        )
        try:
            scored = _bounded(conn, (
                f"SELECT t.i, {text_score} AS score "
                f"FROM unnest(%(texts)s::text[]) WITH ORDINALITY AS t(txt, i) ORDER BY t.i"
            ), {
                "grep_rank_q": " ".join(words),
                "texts": [str((r.get("attrs") or {}).get("text") or "") for r in rows],
            })
        except _Cancelled:
            return None
        for row, score in zip(rows, scored):
            row["grep_score"] = float(score["score"] or 0.0)
        rows.sort(key=lambda r: (r["grep_score"], str(r["node_id"])))
    return rows


def _scan_id_order(
    scan: _PageScan,
    conn: psycopg.Connection,
    *,
    match_sql: str,
    walk_sql: str,
    params: Mapping[str, Any],
    skip: set[str],
    strict: bool,
) -> None:
    """Feed the page in node-id order (a query with no literal word, or the
    matches no BM25 word reaches). `skip` are units an earlier pass decided.

    R1 (`strict` false: Nomos enforcement `off` or `audit`): the match
    set is read once, unordered, up to `PAGE_SMALL_SET + 1` units; when it
    holds no more it is sorted by node id and fed whole, otherwise the scope
    is walked. Every matching row read is charged against `PAGE_SCAN_BOUND`.

    R2 (`enforce`): nothing is decided by the size of
    the stored match set. The scope (rows of the scoped subtypes and
    sources, matching or not) is walked in node-id order, at most
    `PAGE_ID_ROWS_R2` rows, every one charged, so the bound depends only on
    how many rows the
    scope holds, never on their content. Rows passing the stored predicate
    go to the scan site, uncharged: a row the read would change never
    passes there, so the stored predicate never drops a row it would admit.

    The walk reads windows doubling from `PAGE_FIRST_WINDOW` rows through
    the node-id index; it reports `id_rows` only when a further scope row
    exists past the bound."""
    p = {**params, "grep_skip": sorted(skip)}
    bound = PAGE_ID_ROWS_R2 if strict else PAGE_ID_ROWS
    #: Rows walked while a continuation replayed (`scan.skipping`) do not
    #: count against the bound.
    base = 0
    if not strict:
        def read() -> Optional[list[dict[str, Any]]]:
            try:
                return _bounded(conn, match_sql, {**p, "grep_limit": PAGE_SMALL_SET + 1})
            except _Cancelled:
                scan.match_set_timed_out = scan.match_set_timed_out or not skip
                return None

        rows = scan.pinned(
            "id_set", read, lambda got: got is not None and len(got) <= PAGE_SMALL_SET,
        )
        if rows is not None:
            rows.sort(key=lambda r: str(r["node_id"]))
            if not skip:
                scan.match_set_ids = [str(r["node_id"]) for r in rows]
            scan.number(rows)
            scan.feed(rows)
            if scan.bound_hit and not scan.complete:
                scan.stop = "candidate_rows"
            elif not scan.complete:
                # One statement returned every remaining match (the units an
                # earlier pass decided are skipped in it).
                scan.enumeration = "whole_sql"
            return
    after = ""
    walked = 0
    window = PAGE_FIRST_WINDOW
    while True:
        window = min(window, bound + base - walked)
        try:
            found = _bounded_with(
                conn, walk_sql, {**p, "grep_after": after, "grep_window": window},
                settings=_INDEX_PLAN,
            )
        except _Cancelled:
            scan.stop = "timeout"
            return
        walked += len(found)
        before = scan.next_seq
        scan.number(found)
        scan.feed([r for r in found if r["grep_ok"]], charge=not strict)
        if before < scan.resume_at:
            # The walked rows up to the resume point were the replay's.
            base += min(len(found), scan.resume_at - before)
        scan.walked = walked - base
        if scan.complete:
            return
        if scan.bound_hit:
            scan.stop = "candidate_rows"
            return
        if len(found) < window:
            scan.enumeration = "terminal_keyset"
            return
        after = str(found[-1]["node_id"])
        if walked >= bound + base:
            if _walk_has_more(conn, walk_sql, p, after):
                scan.stop = "id_rows"
            else:
                scan.enumeration = "terminal_keyset"
            return
        window *= PAGE_WINDOW_GROWTH


def _walk_has_more(
    conn: psycopg.Connection, walk_sql: str, params: Mapping[str, Any], after: str,
) -> bool:
    """Whether a further scope row exists past `after` (one row read)."""
    try:
        return bool(_bounded_with(
            conn, walk_sql, {**params, "grep_after": after, "grep_window": 1},
            settings=_INDEX_PLAN,
        ))
    except _Cancelled:
        return True


# ---------- the method ----------

def _continuation_binding(
    conn: psycopg.Connection,
    session: Session,
    *,
    query: str,
    filter: Mapping[str, Any],
    path_prefix: Optional[str],
    scope: ResolvedScope,
    strict: bool,
    compiled: CompiledPredicate,
) -> dict[str, Any]:
    """What a grep continuation token is bound to: the read context
    (generation, catalog, authority, policy) and everything that decides the
    scan sequence, the compiled `where` (its fingerprint names the predicate,
    compiler version and field map) included. `limit`, `select` and
    `max_chars` may change between pages. The `where` diagnostics object
    (`where_context`, which the scan fills) is not part of it."""
    projection = hashlib.sha256(json.dumps({
        "query": query,
        "filter": {k: v for k, v in filter.items() if k != "where_context"},
        "where_compiled": compiled.fingerprint,
        "path_prefix": path_prefix,
        "subtypes": scope.subtypes,
        "source_ids": scope.source_ids,
        "excluded_path_globs": scope.excluded_path_globs,
        "regime": "R2" if strict else "R1",
    }, sort_keys=True, default=str).encode("utf-8")).hexdigest()
    return continuation_binding(
        conn, session, kind="grep_page", sort="grep", projection=projection,
    )


#: The stored columns of a grep text unit for the shared compiler's
#: positively invariant fields (`search_where.INVARIANT_FIELDS`).
_WHERE_COLUMNS = {"id": "node_id", "type": "subtype", "source": "source"}


def _compile_grep_where(
    where: Optional[Mapping[str, Any]],
    params: Mapping[str, Any],
    *,
    alias: str = "n",
    prefix: str = "grep_w",
) -> CompiledPredicate:
    """The shared compiler over one alias's invariant columns, its parameter
    names allocated against `params` (merge with `bind_into`)."""
    return search_where.compile_where(
        where, columns={k: f"{alias}.{c}" for k, c in _WHERE_COLUMNS.items()},
        prefix=prefix, params=params,
    )


def _scan_context(
    binding: Mapping[str, Any],
    *,
    query: str,
    filter: Mapping[str, Any],
    path_prefix: Optional[str],
    scope: ResolvedScope,
    compiled: CompiledPredicate,
    result_unit: str,
    own_path_tail: Optional[str] = None,
) -> ScanContext:
    """The shared scanner's context for one grep call: the continuation
    binding (every authority part kept; its projection, the grep hash, moves
    under `grep`), the whole query and scope, and the compiled `where`.
    Grep still issues its own sealed continuation (the cursor carries rows
    the reader may not see), so no scanner token is issued from it and it
    carries no ordering identity; its byte budget is the scanner's state
    bound. A query or scope value with no canonical JSON form is refused by
    name (`_refuse_scan_input`), never a raw encoding error."""
    identity_query, identity_scope = _scan_identity(
        query=query, filter=filter, path_prefix=path_prefix, scope=scope,
        own_path_tail=own_path_tail,
    )
    try:
        return make_scan_context(
            {**binding, "projection": {"grep": binding["projection"]}},
            query=identity_query, scope=identity_scope,
            compiled=compiled, source="node_text", result_unit=result_unit,
            membership_target="grep_matching_" + result_unit + "s",
            max_bytes=MAX_SCAN_BODY_BYTES,
        )
    except ScanInputInvalid as exc:
        raise _scan_input_refusal(exc.data["part"]) from exc


def _scan_identity(
    *,
    query: str,
    filter: Mapping[str, Any],
    path_prefix: Optional[str],
    scope: ResolvedScope,
    own_path_tail: Optional[str] = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """The query and scope the shared scanner fingerprints for one call."""
    identity_query: dict[str, Any] = {"text": query, "path_prefix": path_prefix}
    if own_path_tail is not None:
        # Named only when set, so every other call keeps its fingerprint.
        identity_query["own_path_tail"] = own_path_tail
    return (
        identity_query,
        {
            "filter": {k: v for k, v in filter.items() if k not in ("where", "where_context")},
            "subtypes": scope.subtypes, "source_ids": scope.source_ids,
            "excluded_path_globs": scope.excluded_path_globs,
        },
    )


def _scan_input_refusal(part: str) -> GrepQueryInvalid:
    return GrepQueryInvalid(
        f"grep query refused: the {part} holds a value with no JSON form "
        "(an unpaired surrogate, NaN or Infinity)",
        data={"reason": "invalid_scan_value", "part": part},
        fix_hint="send valid Unicode text and finite numbers",
    )


def _refuse_scan_input(
    *,
    query: str,
    filter: Mapping[str, Any],
    path_prefix: Optional[str],
    scope: ResolvedScope,
    own_path_tail: Optional[str] = None,
) -> None:
    """Refuse, before any database work, a query or scope the shared scanner
    could not fingerprint (`read_scanner.ScanInputInvalid`, reason
    `invalid_scan_value`), with grep's own `grep_query_invalid` code."""
    identity_query, identity_scope = _scan_identity(
        query=query, filter=filter, path_prefix=path_prefix, scope=scope,
        own_path_tail=own_path_tail,
    )
    for part, value in (("query", identity_query), ("scope", identity_scope)):
        try:
            json.dumps(value, sort_keys=True, ensure_ascii=False,
                       allow_nan=False).encode("utf-8")
        except (TypeError, ValueError) as exc:  # UnicodeEncodeError is a ValueError
            raise _scan_input_refusal(part) from exc


def search_grep(
    conn: psycopg.Connection,
    session: Session,
    query: str,
    *,
    filter: dict[str, Any],
    limit: int,
    offset: int,
    select: Optional[list[str]],
    scope: ResolvedScope,
    path_prefix: Optional[str],
    max_chars: int,
    continuation_token: Optional[str] = None,
    own_path_tail: Optional[str] = None,
    list_view: bool = False,
    line_chars: Optional[int] = None,
) -> dict[str, Any]:
    """Exact grep over the node text the reader may read; see the module doc.

    `line_chars` is the `max_chars` the caller passed (D2), None when not
    passed: it sizes each match line that shows a comment or prose
    paragraph (`_unit_lines`). `max_chars` sizes the row `excerpt` as
    before; a file's or code's line keeps `MATCH_LINE_CHARS`.

    `list_view` (in-process only; #3659) prepares the page for
    `search_views.grep_list_projection`: each match window's line is cut to
    `LIST_WINDOW_CHARS` around the match, and each row says whether the
    record's own text matched (`OWN_MATCH_KEY`), which a query with no text
    term (only `path:`) leaves no window to show. Which records are read,
    their order, the counts, bounds and read checks do not change.

    `continuation_token` continues a page that stopped at a bound (or a
    page of a continuation): the scan resumes where it ended, with fresh
    bounds, and leaves out the records already listed.

    `own_path_tail` (in-process only; PACT filename-exact-hits) replaces the
    query's terms with one path condition: the text unit's OWN `attrs.path`
    equals it or ends with `/<it>`, case-insensitively. No lending record's
    path counts, so a passage that only inherits the path is not a
    candidate. Every other part of the read (access rules, profile scope,
    excluded path globs, `where`, `select`, the count) is grep's. Such a
    call issues no continuation token, continuing one is refused, and a
    graph where no record carries a path returns an empty page rather than
    `path_prefix_not_applicable`."""
    from okg.substrate.mcp.search import (
        _citation_from_attrs,
        _family_sql,
        _fence_search_rows,
        _matches_filter,
        _path_exclusion_sql,
        _row_filter,
        _scope_sql,
        _selects_on_attrs,
        _source_sql,
        passage_counts_withheld,
        record_identity,
    )

    from okg.substrate.mcp.search import search_select_elsewhere

    validate_select(select, GREP_FIELDS, tool_name="search(method=grep)",
                    elsewhere=search_select_elsewhere("grep"))
    if own_path_tail is not None:
        if not isinstance(own_path_tail, str) or not own_path_tail:
            raise ValueError("search_grep: own_path_tail must be a non-empty string")
        if continuation_token is not None:
            raise ValueError(
                "search_grep: own_path_tail cannot be combined with continuation_token"
            )
    _refuse_scan_input(
        query=query, filter=filter, path_prefix=path_prefix, scope=scope,
        own_path_tail=own_path_tail,
    )
    parsed = (
        own_path_tail_query(own_path_tail) if own_path_tail is not None
        else parse_grep_query(query)
    )
    # A `where` type naming a passage subtype lists those passages, as
    # `subtype_filter` does (#3666: judged against the record it dropped
    # every passage and answered "0 of 0").
    lifted_filter = search_where.lift_passage_type(
        filter, lambda name: bool(passage_child_declarations(
            conn, catalog_version_id=session.catalog_version_id, child_subtypes=[name],
        )),
    )
    where_lifted_whole = False
    if lifted_filter is not None:
        where_lifted_whole = lifted_filter.get("where") is None
        filter = lifted_filter
    subtype = filter.get("subtype")
    scoped = bool(
        path_prefix
        or subtype
        or filter.get("source") is not None
        or scope.subtypes is not None
        or scope.source_ids is not None
    )
    assert_indexable_or_scoped(parsed, scoped=scoped)
    segment = resolve_trgm_segment(conn)
    if parsed.path_terms:
        assert_path_trgm_index(conn)
    _check_regexes_compile(conn, parsed, query)
    bp = branch_point(conn, session)

    # Which subtypes the text units may have, and how results fold.
    fold_filtered = False
    result_unit = "record"
    unit_subtypes: Optional[list[str]] = None
    if subtype is not None:
        unit_subtypes = [str(subtype)]
        if passage_child_declarations(
            conn, catalog_version_id=session.catalog_version_id, child_subtypes=[subtype],
        ):
            result_unit = "passage"
        else:
            children = sorted({
                child for parent, _edge, child in _declarations(conn, session)
                if parent == subtype
            })
            if children:
                fold_filtered = True
                unit_subtypes += children

    params: dict[str, Any] = {"bp": bp}
    where = [
        "n.deleted = FALSE",
        "n.live_lo <= %(bp)s",
        "%(bp)s < n.live_hi",
        "n.attrs ? 'text'",
    ]
    if unit_subtypes is not None:
        params["grep_subtypes"] = unit_subtypes
        where.append("n.subtype = ANY(%(grep_subtypes)s)")
    for clause in (
        _scope_sql(scope, params, column="n.subtype"),
        _family_sql(scope, params, column="n.source"),
        _source_sql(filter, params, column="n.source"),
    ):
        if clause is not None:
            where.append(clause)
    # Format v10 item G2: the record types a `where` admits (its top-level
    # `type` conjuncts), and the passage subtypes declared under them. A
    # passage folds only into a parent of a declared (parent, child) pair,
    # and under a `where` a passage whose parent the `where` rejects is
    # dropped (D6), so a passage of any other child subtype can never be
    # listed. Both narrow the scope, as `subtype_filter` does, so a
    # selective `where` no longer spends the page's candidate bound (and
    # the node-id walk) on rows of other types. Records only: a passage
    # subtype filter judges the passages themselves.
    residual_where = filter.get("where")
    where_types = (
        search_where.admitted_types(residual_where)
        if residual_where is not None and result_unit == "record" else None
    )
    where_type_children: list[str] = []
    if where_types is not None:
        where_type_children = sorted({
            child for parent, _edge, child in _declarations(conn, session)
            if parent in where_types
        })
        if unit_subtypes is None:
            params["grep_w_scope"] = sorted(set(where_types) | set(where_type_children))
            where.append("n.subtype = ANY(%(grep_w_scope)s)")
    # The scope without the terms: the BM25 index's ordered scan runs in it,
    # and R2 charges its rows. Profile path exclusions are not scope: they
    # read a row's stored path, so they join the predicate.
    rank_where = list(where)
    terms = [
        *_term_sql(parsed, params, column=_TEXT),
        *_path_exclusion_sql(scope, params, column="n.attrs"),
    ]
    attr_filters = filter.get("attr_filters") or {}
    if attr_filters:
        # Pushed into the predicate (never into the scope the bounds walk):
        # exact equality on stored attrs. Safe in both regimes: a row whose
        # filtered attribute the read would mask is refused at the scan
        # site anyway (B2), and the scan site still checks the read values.
        # A declared passage is not filtered itself: its record is.
        params["grep_af"] = Jsonb(dict(attr_filters))
        clause = "n.attrs @> %(grep_af)s::jsonb"
        declared_af = _declarations(conn, session) if result_unit == "record" else []
        if declared_af:
            params["grep_af_children"] = sorted({child for _p, _e, child in declared_af})
            # Format v10 item G2: a declared passage passes when its own
            # stored attrs match (it may stay unfolded and be judged itself)
            # or a declared parent's stored attrs do (it folds into that
            # parent, which is judged); every other passage is either
            # dropped with its rejecting parent or rejected itself. Before,
            # every passage of every parent passed and could spend the
            # page's candidate bound on records the filter excludes.
            parent_af = _lender_exists(
                segment=segment, edge_segment=resolve_edge_segment(conn),
                values_sql=_declared_values(declared_af, params),
                lender_path="p.attrs @> %(grep_af)s::jsonb",
            )
            clause = (
                f"({clause} OR (n.subtype = ANY(%(grep_af_children)s) AND {parent_af}))"
            )
        terms = [*terms, clause]
    # The `where` residual (PACT read-model-s3-where-scope; shared compiler,
    # PACT shared-read-scanners-3199). Its id/type/source part compiles to
    # SQL that admits every row the reader's check accepts (`exact` when the
    # SQL decides the whole predicate, `superset` when a residual remains);
    # it joins the predicate, never the scope the bounds count. A declared
    # passage passes it: its record is judged (D6). Every folded row is
    # checked again on read values (`_PageScan.row_admitted`).
    compiled_where = _compile_grep_where(residual_where, params)
    where_children: list[str] = []
    if where_types is not None:
        # Only passages that can fold into a record of an admitted type.
        where_children = where_type_children
    elif residual_where is not None and result_unit == "record":
        where_children = sorted({child for _p, _e, child in _declarations(conn, session)})
    if compiled_where.sql != "TRUE":
        clause = compiled_where.bind_into(params)
        if where_children:
            params["grep_w_children"] = where_children
            clause = f"(n.subtype = ANY(%(grep_w_children)s) OR {clause})"
        terms = [*terms, clause]
    where.extend(terms)

    # How the path conditions apply (PACT mcp-grep-rank). "include": a
    # prefix or a positive `path:` term; a text unit is a candidate when its
    # own path, or one lending record's path, passes every path condition.
    # "exclude": `-path:` terms only; SQL does not narrow (a unit with no
    # path passes). Either way the scan site decides on read values.
    path_mode: Optional[str] = None
    if path_prefix or any(not t.negated for t in parsed.path_terms):
        path_mode = "include"
    elif parsed.path_terms:
        path_mode = "exclude"
    # The own-path pass is not refused on a graph without paths: its empty
    # page already says no record has the path, and the deployment-wide
    # probe would spend most of the exact pass's budget (PACT
    # filename-exact-hits; 1.6 s of 2 s on the C4 copy). Other path scopes
    # reuse the probe's answer for the generation (`_any_path_at_pin`).
    path_presence = (
        _any_path_at_pin(conn, segment, bp, generation_id=session.generation_id)
        if path_mode == "include" and own_path_tail is None else True
    )
    if path_presence is False:
        raise GrepPathNotApplicable(
            "grep refused: no record in this graph carries attrs.path, so "
            "path_prefix and path: terms can only match nothing",
            data={"path_prefix": path_prefix, "path_terms": [t.text for t in parsed.path_terms]},
            fix_hint=(
                "drop path_prefix and path: terms; scope with subtype_filter, "
                "source_family or filter.source instead"
            ),
        )
    own_where = list(where)
    path_ok: list[str] = []
    branch_parts: Optional[dict[str, str]] = None
    if path_mode == "include":
        own_path = "(" + " AND ".join(_path_sql(parsed, path_prefix, params, alias="n")) + ")"
        own_where.append(own_path)
        path_ok = [own_path]
        # The own-path pass reads each unit's own path only: no lender branch.
        declared = _declarations(conn, session) if own_path_tail is None else []
        if declared:
            values_sql = _declared_values(declared, params)
            edge_segment = resolve_edge_segment(conn)
            lender_path = " AND ".join(_path_sql(parsed, path_prefix, params, alias="p"))
            branch_parts = {
                "segment": segment, "edge_segment": edge_segment,
                "values_sql": values_sql, "lender_path": lender_path,
                "own_path": own_path,
            }
            lender_ok = _lender_exists(
                segment=segment, edge_segment=edge_segment,
                values_sql=values_sql, lender_path=lender_path,
            )
            path_ok = [f"({own_path} OR {lender_ok})"]

    count_where: list[str] = []
    if path_mode == "exclude":
        # The R1 count is SQL alone, so it states the `-path:` rule there:
        # a unit passes when its own path, or a lending record's path,
        # passes every exclusion, or when neither carries a path.
        own_ok = " AND ".join(_path_sql(parsed, None, params, alias="n"))
        declared_x = _declarations(conn, session)
        if declared_x:
            values_x = _declared_values(declared_x, params)
            edge_x = resolve_edge_segment(conn)
            lender_ok_x = _lender_exists(
                segment=segment, edge_segment=edge_x, values_sql=values_x,
                lender_path=" AND ".join(_path_sql(parsed, None, params, alias="p")),
            )
            lender_has_x = _lender_exists(
                segment=segment, edge_segment=edge_x, values_sql=values_x,
                lender_path="p.attrs ? 'path'",
            )
            count_where.append(
                f"(({own_ok}) OR {lender_ok_x} OR "
                f"(NOT (n.attrs ? 'path') AND NOT {lender_has_x}))"
            )
        else:
            count_where.append(f"(({own_ok}) OR NOT (n.attrs ? 'path'))")

    def units_sql(
        *, ordered: bool, limit_key: Optional[str], skip: bool, for_count: bool = False,
    ) -> str:
        extra = list(count_where) if for_count else []
        if skip:
            extra.append("NOT (n.node_id = ANY(%(grep_skip)s))")
        branch = None
        if branch_parts is not None:
            branch = _lender_branch(where=[*where, *extra], **branch_parts)
        return _units_sql(
            segment=segment, own_where=[*own_where, *extra], path_branch=branch,
            ordered=ordered, limit_key=limit_key,
        )

    redaction = _redaction_possible(conn, session)
    #: The regime (see the module doc): R2 only under Nomos `enforce`, R1
    #: under `off` and `audit` (decision 260a).
    strict = _strict_regime(session)
    listed: list[str] = []
    resume_at = 0
    path: dict[str, Any] = {}
    binding: Optional[dict[str, Any]] = None
    if continuation_token is not None:
        if offset:
            raise InvalidOperatorArguments(
                "search(method=grep): offset and continuation_token cannot be "
                "combined; a continuation starts where its page ended"
            )
        binding = _continuation_binding(
            conn, session, query=query, filter=filter, path_prefix=path_prefix,
            scope=scope, strict=strict, compiled=compiled_where,
        )
        cursor = unseal_cursor(
            consume_continuation(continuation_token, expected_binding=binding).get("sealed")
        )
        resume_at = int(cursor["resume"])
        listed = [str(node_id) for node_id in cursor["listed"]]
        path = dict(cursor.get("path") or {})
    admitter = _Admitter(
        conn, session,
        segment=segment, branch_point_value=bp, parsed=parsed,
        path_prefix=path_prefix, path_mode=path_mode, scope=scope,
        attr_filters=filter.get("attr_filters") or {},
        record_unit=result_unit == "record", redaction=redaction,
        own_path_only=own_path_tail is not None,
    )
    if fold_filtered:
        def parent_admitted(parent: Mapping[str, Any]) -> bool:
            return _matches_filter(dict(parent), filter, scope=scope)
    else:
        def parent_admitted(parent: Mapping[str, Any]) -> bool:
            return _matches_filter(dict(parent), {**filter, "subtype": None}, scope=scope)
    row_admitted: Optional[Callable[[Mapping[str, Any]], bool]] = None
    parent_matches_outside_scope: Optional[Callable[[Mapping[str, Any]], bool]] = None
    if residual_where is not None:
        row_filter = _row_filter(filter)

        def row_admitted(row: Mapping[str, Any]) -> bool:
            return _matches_filter(dict(row), row_filter)

        if result_unit == "record" and scope.subtypes is not None:
            # Issue #3466: only the profile's subtype list is relaxed; a
            # parent excluded by path, producer family or the `where` still
            # drops its passages.
            subtypes_relaxed = replace(scope, subtypes=None)
            parent_filter = filter if fold_filtered else {**filter, "subtype": None}

            def parent_matches_outside_scope(parent: Mapping[str, Any]) -> bool:
                return (
                    not parent_admitted(parent)
                    and _matches_filter(dict(parent), parent_filter, scope=subtypes_relaxed)
                )
    scan = _PageScan(
        conn, session, admitter,
        offset=offset, limit=limit, fold_filtered=fold_filtered,
        segment=segment, branch_point_value=bp,
        mask_fetched_parents=_selects_on_attrs(filter, scope),
        parent_admitted=parent_admitted,
        takes=declaring_subtypes(conn, session.catalog_version_id) if result_unit == "record" else set(),
        resume_at=resume_at, exclude=frozenset(listed),
        path=path, row_admitted=row_admitted,
        # Under `audit` and `enforce` only admitted units move the page
        # bound (#3384); Nomos `off` keeps the stored-row charge unchanged.
        charge_admitted=_nomos_runtime(session),
        parent_matches_outside_scope=parent_matches_outside_scope,
    )
    contexts: list[ScanContext] = []

    def scan_context() -> ScanContext:
        """The shared scanner's identity of this call (read once, on use),
        over the same binding a continuation token of this call carries."""
        nonlocal binding
        if not contexts:
            if binding is None:
                binding = _continuation_binding(
                    conn, session, query=query, filter=filter, path_prefix=path_prefix,
                    scope=scope, strict=strict, compiled=compiled_where,
                )
            contexts.append(_scan_context(
                binding, query=query, filter=filter, path_prefix=path_prefix,
                scope=scope, compiled=compiled_where, result_unit=result_unit,
                own_path_tail=own_path_tail,
            ))
        return contexts[0]

    # --- PAGE: best first ---
    id_match_sql = units_sql(ordered=False, limit_key="grep_limit", skip=True)
    id_walk_sql = _walk_sql(
        segment=segment, rank_where=rank_where,
        ok_sql=" AND ".join([*terms, *path_ok, "NOT (n.node_id = ANY(%(grep_skip)s))"]),
    )
    rank_words = parsed.rank_words
    order_basis = "bm25" if rank_words else "node_id"
    # R1 reads a small match set whole before any BM25 window when no term
    # is a NEAR (whose regex makes that unordered read slow on common words;
    # NEAR reads it only after `PAGE_PROBE_AFTER` index rows). A selective
    # query (phrases of common words) then costs one trigram read.
    exhausted = False
    try:
        early = None
        if rank_words and not strict and not any(t.kind == "near" for t in parsed.terms):
            early = scan.pinned("early", lambda: _small_match_set(
                conn, sql=units_sql(ordered=False, limit_key="grep_limit", skip=False),
                params=params, words=rank_words, scan=scan,
            ), lambda got: got is not None)
        if early is not None:
            scan.rank_scan = "match_set"
            scan.match_set_ids = [str(r["node_id"]) for r in early]
            scan.number(early)
            scan.feed(early)
            if scan.bound_hit and not scan.complete:
                scan.stop = "candidate_rows"
            exhausted = scan.stop is None and not scan.complete
            if exhausted:
                # One unordered statement returned the whole match set.
                scan.enumeration = "whole_sql"
        elif rank_words:
            bm25_done, passed, whole = _scan_bm25(
                scan, conn, segment=segment, rank_where=rank_where,
                ok_sql=" AND ".join([*terms, *path_ok]), params=params, words=rank_words,
                probe=None if (strict or not any(t.kind == "near" for t in parsed.terms)) else (
                    lambda: scan.pinned("probe", lambda: _small_match_set(
                        conn, sql=units_sql(ordered=False, limit_key="grep_limit", skip=False),
                        params=params, words=rank_words, scan=scan,
                    ), lambda got: got is not None)
                ),
                strict=strict,
            )
            if whole is not None:
                # Every match is known; the units the windows already fed are
                # its best ones, so the rest follow in the same order.
                scan.rank_scan = "match_set"
                scan.match_set_ids = [str(r["node_id"]) for r in whole]
                rest = [r for r in whole if str(r["node_id"]) not in passed]
                scan.number(rest)
                scan.feed(rest)
                if scan.bound_hit and not scan.complete:
                    scan.stop = "candidate_rows"
                exhausted = scan.stop is None and not scan.complete
                if exhausted:
                    scan.enumeration = "whole_sql"
            elif bm25_done:
                # The matches no BM25 word reaches (score 0), in node-id order.
                _scan_id_order(
                    scan, conn, match_sql=id_match_sql, walk_sql=id_walk_sql,
                    params=params, skip=passed, strict=strict,
                )
                exhausted = scan.stop is None and not scan.complete
        else:
            _scan_id_order(
                scan, conn, match_sql=id_match_sql, walk_sql=id_walk_sql,
                params=params, skip=set(), strict=strict,
            )
            exhausted = scan.stop is None and not scan.complete
    except _PathLost:
        # A continuation could not replay its chain's read path.
        scan.stop = "timeout"
        exhausted = False
    if scan.stop == "timeout" and not scan.folded:
        raise GrepTimeout(
            f"the grep match ran past the {STATEMENT_TIMEOUT_MS} ms statement "
            + ("timeout before this continuation found a record"
               if continuation_token is not None else "timeout before any page existed"),
            data={"timeout_ms": STATEMENT_TIMEOUT_MS, "scopes": SCOPE_NAMES},
            fix_hint=f"narrow the call with {SCOPE_NAMES}, or add a longer literal",
        )

    # --- COUNT: one seam, never read by paging ---
    # The count counts what the page lists: records, a passage folded into
    # its one declared parent (in the filtered subtype when one is named),
    # or passages when the filter names a passage subtype.
    record_key: tuple[str, str] = ("u.node_id", "")
    declared_all = _declarations(conn, session) if result_unit == "record" else []
    if declared_all:
        params["grep_cnt_children"] = sorted({child for _p, _e, child in declared_all})
        values_c = _declared_values(declared_all, params)
        edge_c = resolve_edge_segment(conn)
        admitted_c = ["TRUE"]
        if fold_filtered:
            params["grep_cnt_parent"] = str(subtype)
            admitted_c.append("p.subtype = %(grep_cnt_parent)s")
        for clause in (
            _scope_sql(scope, params, column="p.subtype"),
            _family_sql(scope, params, column="p.source"),
            _source_sql(filter, params, column="p.source"),
        ):
            if clause is not None:
                admitted_c.append(clause)
        admitted_c.extend(_path_exclusion_sql(scope, params, column="p.attrs"))
        if attr_filters:
            admitted_c.append("p.attrs @> %(grep_af)s::jsonb")
        if compiled_where.sql != "TRUE":
            # The SQL count judges the record the passage folds into. This
            # statement is built for every call, but the count runs it only
            # when the compile is `exact`; with a residual it finishes units
            # instead (`finish` below), so a `superset` clause here never
            # decides a count.
            admitted_c.append(
                "coalesce(" + _compile_grep_where(
                    residual_where, params, alias="p", prefix="grep_wp",
                ).bind_into(params) + ", FALSE)"
            )
        lateral = (
            f"LEFT JOIN LATERAL (SELECT count(DISTINCT p.node_id) AS n, min(p.node_id) AS pid, "
            f"bool_and({' AND '.join(admitted_c)}) AS ok FROM {edge_c} e "
            f"JOIN {segment} p ON p.node_id = e.src AND p.deleted = FALSE "
            f"AND p.live_lo <= %(bp)s AND %(bp)s < p.live_hi "
            f"JOIN {values_c} ON d.parent = p.subtype AND d.edge = e.edge_type "
            f"AND d.child = u.subtype "
            f"WHERE e.dst = u.node_id AND e.deleted = FALSE "
            f"AND e.live_lo <= %(bp)s AND %(bp)s < e.live_hi) par ON TRUE"
        )
        # An unfolded passage counts only if it passes the selections itself.
        unit_ok = ["u.attrs @> %(grep_af)s::jsonb"] if attr_filters else []
        if compiled_where.sql != "TRUE":
            unit_ok.append(
                "coalesce(" + _compile_grep_where(
                    residual_where, params, alias="u", prefix="grep_wu",
                ).bind_into(params) + ", FALSE)"
            )
        unfolded = "NULL" if fold_filtered else (
            f"(CASE WHEN {' AND '.join(unit_ok) or 'TRUE'} THEN u.node_id END)"
        )
        # Under a `where`, a passage whose one parent the selections (scope,
        # source, attr_filters, the compiled `where`) reject is dropped, not
        # counted on its own (D6, as the page folds it). Without a `where` it
        # counts on its own when it passes them itself, as before.
        rejected = "NULL" if residual_where is not None else unfolded
        record_key = (
            f"CASE WHEN u.subtype = ANY(%(grep_cnt_children)s) THEN "
            f"CASE WHEN par.n = 1 AND par.ok THEN par.pid "
            f"WHEN par.n = 1 THEN {rejected} ELSE {unfolded} END "
            f"ELSE u.node_id END",
            lateral,
        )

    def new_fold() -> _PageScan:
        return _PageScan(
            conn, session, admitter,
            offset=0, limit=10**9, fold_filtered=fold_filtered,
            segment=segment, branch_point_value=bp,
            mask_fetched_parents=_selects_on_attrs(filter, scope),
            parent_admitted=parent_admitted, takes=scan.takes,
            batch_max=COUNT_SCOPE_ROWS, row_admitted=row_admitted,
            parent_matches_outside_scope=parent_matches_outside_scope,
        )

    if scan.match_set_ids is not None:
        params["grep_known"] = scan.match_set_ids
    #: R1 finishes the count when the compiled `where` leaves a residual;
    #: when passages may be kept through a record outside the profile (issue
    #: #3466): the SQL count judges a parent with the profile's subtypes, so
    #: only the fold counts what the page lists; and under `audit` unless
    #: audit can hide nothing here (#3384): a SQL count of stored rows would
    #: count rows audit still hides (no source lineage, masked text), so a
    #: term only they hold would count where an absent term counts none.
    finish = not strict and (
        compiled_where.qualification != "exact"
        or parent_matches_outside_scope is not None
        or (
            _nomos_runtime(session) and not _audit_hides_nothing(
                conn, session, redaction=redaction, segment=segment,
                branch_point_value=bp,
            )
        )
    )
    if not strict and scan.match_set_timed_out:
        # Lane P (10-02): a regex-only page paid the timeout on its match-set
        # read and again on the count. The R1 count reads that match set
        # further (up to COUNT_SCAN_BOUND + 1 units, the path conditions
        # added), so it is reported timed out without running.
        counted = GrepCount(None, "timed_out", ("timeout",))
    else:
        counted = count_grep_matches(
            conn, session,
            pgsql.SQL(units_sql(ordered=False, limit_key=None, skip=False, for_count=True)),
            params,
            scan_bound=COUNT_SCAN_BOUND, redaction_possible=redaction,
            admit=admitter.admit, strict=strict, walk_sql=id_walk_sql,
            # Under `audit` with no residual the finished count keeps the
            # SQL count's 10,000-row bound (#3384); a residual, or passages
            # kept through a record outside the profile (#3466), keep the
            # finish bound they had.
            finish_rows=(
                COUNT_SCAN_BOUND if (
                    compiled_where.qualification == "exact"
                    and parent_matches_outside_scope is None
                )
                else COUNT_FINISH_ROWS
            ),
            record_key=record_key, new_fold=new_fold,
            # R1: when the page read the whole match set, the count folds those
            # units rather than read the match set again. Finishing decides
            # the path rules on read values itself, so it reads the units the
            # page read, without the SQL count's `-path:` statement.
            known_units=None if strict or scan.match_set_ids is None else pgsql.SQL(
                f"SELECT {_COLUMNS} FROM {segment} n WHERE n.node_id = ANY(%(grep_known)s) "
                f"AND n.deleted = FALSE AND n.live_lo <= %(bp)s AND %(bp)s < n.live_hi"
                + ("" if finish else "".join(f" AND {clause}" for clause in count_where))
            ),
            # A residual SQL cannot decide: finish each unit instead of
            # counting stored rows (a SQL count would count rows it rejects).
            finish_sql=pgsql.SQL(
                units_sql(ordered=False, limit_key="grep_finish", skip=False)
            ) if finish else None,
            context=scan_context() if strict or finish else None,
        )

    folded = scan.folded
    passage_by_id = scan.passages
    members: dict[str, list[str]] = {}
    for pid, link in scan.links.items():
        if pid in passage_by_id:
            members.setdefault(link.parent_id, []).append(pid)
    for pids in members.values():
        pids.sort(key=lambda pid: float(passage_by_id[pid]["score"]))
    # A unit the page lists on its own text: a record, or a passage that
    # stayed unfolded (no single declared parent); both are their own row.
    own_matched = set(scan.records) | {
        str(row["node_id"]) for row in folded if str(row["node_id"]) in passage_by_id
    }
    #: The text each admitted unit was decided on; a served unit whose read
    #: text differs (masked since, or overlaid) supplies no window.
    counted_text = {
        node_id: str((r.get("attrs") or {}).get("text") or "")
        for node_id, r in (*scan.records.items(), *scan.passages.items())
    }

    truncated = len(folded) > offset + limit
    page = folded[offset: offset + limit]

    # --- SERVE SITE ---
    units: list[tuple[int, dict[str, Any], bool]] = []  # (page index, row, is_record)
    serve: list[dict[str, Any]] = []
    for index, row in enumerate(page):
        node_id = str(row["node_id"])
        serve.append(row)
        units.append((index, row, True))
        if row.get(MATCHING_KEY) is not None or node_id in members:
            for pid in members.get(node_id, [])[:MAX_WINDOWS]:
                serve.append(passage_by_id[pid])
                units.append((index, passage_by_id[pid], False))
    served = _serve_rows(conn, session, serve) if serve else []
    texts = [str((r.get("attrs") or {}).get("text") or "") for r in served]
    checks = _recheck(conn, parsed, texts)
    probes = serialize_node_read_rows(
        conn,
        [{"source": r.get("source"), "attrs": r.get("attrs")} for r in served],
        deployment_name=session.deployment_name,
    ) if served else []

    record_rows: dict[int, dict[str, Any]] = {}
    record_trust: dict[int, str] = {}
    windows: dict[int, list[dict[str, Any]]] = {}
    first_own: dict[int, tuple[str, int]] = {}
    alive: dict[int, bool] = {}
    #: Each window whose line is not yet a record line: what its line counts
    #: now (`passage`, `own_text`) and where it matched.
    placed_windows: list[tuple[dict[str, Any], str, MatchPlace]] = []
    # Every match of the units that matched (#3666 R5), read in one
    # statement: a line per match, and the spellings the page matched.
    live_units = [
        at for at, ((_index, _orig, is_record), row, (ok, _spans)) in enumerate(
            zip(units, served, checks)
        )
        if ok
        and (not is_record or str(row["node_id"]) in own_matched)
        and texts[at] == counted_text.get(str(row["node_id"]))
    ]
    live_surfaces = _match_surfaces(conn, parsed, [texts[at] for at in live_units])
    surfaces_by_unit = (
        dict(zip(live_units, live_surfaces)) if live_surfaces is not None else None
    )
    literal_terms = {
        i for i, t in enumerate(parsed.terms) if not t.negated and t.kind in ("word", "phrase")
    }
    spellings: dict[str, None] = {}
    match_lines: dict[int, list[dict[str, Any]]] = {}
    line_keys: dict[int, set[tuple[Any, ...]]] = {}
    record_attrs: dict[int, Mapping[str, Any]] = {}
    for at, ((index, _orig, is_record), row, (ok, spans), probe) in enumerate(
        zip(units, served, checks, probes)
    ):
        trust = probe["provenance"]["trust"]
        node_id = str(row["node_id"])
        text = str((row.get("attrs") or {}).get("text") or "")
        if is_record:
            record_rows[index] = {**row, MATCHING_KEY: page[index].get(MATCHING_KEY)}
            record_trust[index] = trust
            record_attrs[index] = row.get("attrs") or {}
            alive.setdefault(index, False)
            if node_id not in own_matched:
                continue
        if not ok or text != counted_text.get(node_id):
            continue
        alive[index] = True
        untrusted = "untrusted" in (trust, record_trust.get(index, trust))
        if surfaces_by_unit is not None:
            unit_surfaces = surfaces_by_unit.get(at, [])
            unit_spans = _match_spans(text, unit_surfaces)
            for term, surface in unit_surfaces:
                if term in literal_terms and len(spellings) < MAX_SPELLINGS:
                    spellings.setdefault(surface[:SPELLING_CHARS], None)
        else:
            unit_spans = list(spans)
            if len(literal_terms) == len(parsed.positive):
                for start, end in spans:
                    if len(spellings) < MAX_SPELLINGS:
                        spellings.setdefault(text[start:end][:SPELLING_CHARS], None)
        unit_attrs = row.get("attrs") or {}
        start_line = unit_attrs.get("start_line")
        bucket_lines = match_lines.setdefault(index, [])
        seen_keys = line_keys.setdefault(index, set())
        for entry in _unit_lines(
            text, unit_spans,
            file_like=any(
                (attrs or {}).get(key)
                for attrs in (unit_attrs, record_attrs.get(index)) for key in _FILE_ATTRS
            ),
            start_line=start_line if isinstance(start_line, int) and not isinstance(
                start_line, bool) else None,
            prose_chars=line_chars,
        ):
            if len(bucket_lines) >= MAX_MATCH_LINES:
                break
            same = (entry["field"], entry["text"], entry["author"], entry["date"])
            if same in seen_keys:
                continue  # the record's text and its passage hold the same line
            seen_keys.add(same)
            # The line and the author are the source's words: fenced as the
            # windows' excerpts are, for an untrusted source.
            for key in ("text", "author"):
                if entry[key] is not None:
                    entry[key] = serialize_node_read_row(
                        {"source": row.get("source"), "excerpt": entry[key]},
                        trust="untrusted" if untrusted else "trusted",
                    )["excerpt"]
            bucket_lines.append(entry)
        # `spans` are in text order (`_recheck` sorts them), so the windows
        # kept under the cap below are the earliest matches.
        for start, end in spans:
            if is_record and index not in first_own:
                first_own[index] = (text, start)
            bucket = windows.setdefault(index, [])
            if len(bucket) >= MAX_WINDOWS:
                break
            line = (
                window_text(text, start, end, budget=LIST_WINDOW_CHARS) if list_view
                else window_text(text, start, end)
            )
            excerpt = serialize_node_read_row(
                {"source": row.get("source"), "excerpt": line},
                trust="untrusted" if untrusted else "trusted",
            )["excerpt"]
            window: dict[str, Any] = {
                "char_start": start,
                "char_end": end,
                "line": _line_of(text, start),
                "excerpt": excerpt,
            }
            if not is_record:
                # Offsets are relative to the passage (no declared char offset).
                window["passage_id"] = node_id
                placed_windows.append((window, MATCH_LINE_IN_PASSAGE, MatchPlace(
                    str(page[index]["node_id"]), node_id, row.get("subtype"), text, start,
                )))
            elif row.get("subtype") != "document_chunk":
                # The record's own `text` attribute, which `inspect` text
                # does not read. (A `document_chunk` row's own text is what
                # `inspect` text reads for that id, so its line stands.)
                placed_windows.append((window, MATCH_LINE_IN_OWN_TEXT, MatchPlace(
                    node_id, None, None, text, start,
                )))
            bucket.append(window)
    if placed_windows and (list_view or select is None or "match_windows" in select):
        # #3666: a window's `line` is a line of the row's record as `inspect`
        # text numbers it, the number `lines` takes back; a window whose line
        # is not that says what it counts instead (`line_basis`: inside the
        # passage, or in the record's own text). `record_match_lines` says
        # when a record line can be had and bounds the work. An own-text
        # match is tried only where a passage of the same record is on the
        # page, so it never causes a record to be read by itself; elsewhere
        # it says `own_text`.
        with_passage = {
            place.record_id for _window, basis, place in placed_windows
            if basis == MATCH_LINE_IN_PASSAGE and place.chunk_subtype == "document_chunk"
        }
        tried = [
            entry for entry in placed_windows
            if entry[1] == MATCH_LINE_IN_PASSAGE or entry[2].record_id in with_passage
        ]
        record_lines = dict(zip(
            (id(window) for window, _basis, _place in tried),
            _record_lines_in_time(conn, session, [place for _window, _basis, place in tried]),
        ))
        for window, basis, _place in placed_windows:
            record_line = record_lines.get(id(window))
            if record_line is None:
                window["line_basis"] = basis
            else:
                window["line"] = record_line
    dropped = sum(1 for index in record_rows if not alive.get(index))
    page_rows = [record_rows[i] for i in sorted(record_rows) if alive.get(i)]
    kept_index = [i for i in sorted(record_rows) if alive.get(i)]
    parent_rows, parent_block = resolve_passage_parents(
        conn,
        session=session,
        page=page_rows,
        node_segment=segment,
        branch_point_value=bp,
        citation_from_attrs=_citation_from_attrs,
        withhold_passage_counts=passage_counts_withheld(session),
    )
    full_rows = []
    for index, row in zip(kept_index, page_rows):
        node_id = str(row["node_id"])
        own = first_own.get(index)
        full_row = {
            "node_id": row["node_id"],
            "canonical_node_id": row["node_id"],
            "subtype": row["subtype"],
            "attrs": row.get("attrs"),
            "excerpt": _excerpt(own[0], own[1], max_chars=max_chars) if own else None,
            **({"text_hash": row["text_hash"]} if "text_hash" in row else {}),
            "method": "grep",
            "match_kind": "grep",
            "source": row.get("source"),
            "source_record_id": record_identity(row.get("source_record_id")),
            "source_revision": record_identity(row.get("source_revision")),
            "parent": parent_rows.get(node_id),
            "position": passage_position(row.get("attrs")),
            "matching_passages": row.get(MATCHING_KEY),
            "match_windows": windows.get(index, []),
            "match_lines": match_lines.get(index, []),
        }
        citation = _citation_from_attrs(row.get("attrs"))
        if citation:
            full_row["citation"] = citation
        location = chunk_location(row, own[1]) if own else None
        if location is not None:
            full_row[LOCATION_KEY] = location
        if list_view:
            full_row[OWN_MATCH_KEY] = node_id in own_matched
        full_rows.append(full_row)
    results = _fence_search_rows(
        conn, session, full_rows,
        # A list is projected from grep's default row plus the own-match fact.
        select=[*GREP_DEFAULT, OWN_MATCH_KEY] if list_view else select,
        default=GREP_DEFAULT, always=GREP_ALWAYS, scope=scope, max_chars=max_chars,
    )

    stopped = (scan.stop is not None and not scan.complete) or scan.tie_cut
    if scan.tie_cut:
        scan.stop = "index_rows"
    if not stopped:
        page_basis = "top_of_all_visible_matches"
    elif scan.stop == "timeout":
        page_basis = "timed_out"
    else:
        page_basis = "scan_bound_reached"
    bound: dict[str, int] = {"candidate_rows": PAGE_SCAN_BOUND}
    if order_basis == "bm25":
        bound["index_rows"] = PAGE_INDEX_ROWS
    bound["id_rows"] = PAGE_ID_ROWS_R2 if strict else PAGE_ID_ROWS
    total = counted.total
    if total is not None and counted.basis == "lower_bound":
        # A lower bound is never below the records the pages already found.
        total = max(total, len(listed) + len(scan.folded))
    chain = continuation_token is not None
    token: Optional[str] = None
    withheld: Optional[str] = None
    if own_path_tail is None and scan.stop != "timeout" and (
        (chain and (truncated or stopped)) or (not chain and stopped and not truncated)
    ):
        # A continuation: the scan resumes where this page ended, with fresh
        # bounds, leaving out every record listed so far.
        shown = min(len(folded), offset + limit)
        point = scan.resume_point(listed=shown)
        upto = [*listed, *(str(row["node_id"]) for row in folded[:shown])]
        if scan.stop == "index_rows" and scan.index_limit:
            withheld = "index_scan_limit"
        elif point <= resume_at:
            withheld = "no_progress"
        elif len(upto) > CONTINUATION_LISTED_MAX:
            withheld = "listed_cap"
        else:
            if binding is None:
                binding = _continuation_binding(
                    conn, session, query=query, filter=filter, path_prefix=path_prefix,
                    scope=scope, strict=strict, compiled=compiled_where,
                )
            # Sealed: the resume point counts rows the reader may not see
            # (every BM25 row the words match, in or out of scope).
            # A whole read that timed out is not read again on replay, so the
            # chain remembers it and its later pages skip the count too.
            pinned_path = {**scan.path, "whole_read_timed_out": scan.match_set_timed_out}
            # Under `audit` and `enforce` the resume point travels as a
            # fixed-width integer outside the compressed part, so the
            # token's length cannot count the hidden rows before it (#3384);
            # with Nomos off nothing is hidden and the cursor is unchanged.
            if scan.charge_admitted:
                sealed = seal_cursor(
                    {"listed": upto, "path": pinned_path}, fixed={"resume": point},
                )
            else:
                sealed = seal_cursor({"resume": point, "listed": upto, "path": pinned_path})
            token = issue_continuation(binding, cursor={"sealed": sealed})
            if len(token) > CONTINUATION_TOKEN_MAX:
                # Long record ids: consume_continuation would refuse it.
                token, withheld = None, "token_size"
    payload: dict[str, Any] = {
        "query": query,
        "method": "grep",
        "generation_id": session.generation_id,
        "pinned_generation_id": session.generation_id,
        "limit": limit,
        # On a continuation, the records the earlier pages listed.
        "offset": len(listed) if chain else offset,
        "next_offset": offset + limit if truncated and not chain else None,
        "results": results,
        "total_count": total,
        "truncated": truncated,
        "total_count_basis": counted.basis,
        "count_unit": result_unit,
        "result_unit": result_unit,
        "and_scope": "text_unit",
        "grep_terms": parsed.disclosure(),
        "path_prefix": path_prefix,
        "order_basis": order_basis,
        "page_basis": page_basis,
        "page_scan_bound": bound,
        "statement_timeout_ms": STATEMENT_TIMEOUT_MS,
        "parent_relation": parent_block,
    }
    # What this call searched, so no reply claims more (#3666 R5): the text
    # fields, whether the records' passages were in scope (a ticket's
    # comments live in them), the subtypes (None: every subtype with
    # text), each term's spelling rule and the spellings the page matched.
    searched_types = unit_subtypes if unit_subtypes is not None else (
        list(scope.subtypes) if scope.subtypes is not None else None
    )
    passage_types = {child for _p, _e, child in _declarations(conn, session)}
    searched: dict[str, Any] = {
        "fields": [
            *(["text"] if parsed.terms else []),
            *(["path"] if path_prefix or parsed.path_terms else []),
        ],
        "comments": bool(
            passage_types if searched_types is None
            else passage_types.intersection(searched_types)
        ),
        "subtypes": searched_types,
        "spellings_tried": [spelling_tried(term) for term in parsed.positive],
        "spellings_matched": list(spellings),
    }
    if surfaces_by_unit is None and live_units:
        # The every-match read ran past the timeout: each line is a term's
        # first match in its text only.
        searched["lines_basis"] = "first_match_per_term"
    if stopped and scan.stop in ("candidate_rows", "id_rows", "index_rows"):
        # Format v10 item G2: how far the scan read before its bound, so an
        # empty page says "no match in the first N", never "no match"; on a
        # continuation page, "in the next N" (review P2-2).
        searched["bound_read"] = {
            "bound": scan.stop,
            "rows": {
                "candidate_rows": scan.charged,
                "id_rows": scan.walked,
                "index_rows": scan.index_read,
            }[scan.stop],
            **({"continued": True} if continuation_token is not None else {}),
        }
    payload["grep_searched"] = searched
    if stopped:
        if not truncated:
            payload["next_offset_reason"] = page_basis
        if scan.stop != "timeout":
            payload["page_scan_bound_hit"] = scan.stop
    if token is not None:
        payload["continuation_token"] = token
    elif withheld is not None:
        payload["continuation_withheld"] = withheld
    if counted.reasons:
        payload["count_basis_reasons"] = list(counted.reasons)
    if counted.cap is not None:
        payload["scan_cap"] = counted.cap
    if not exhausted:
        # The scan stopped once the page was complete (or at a bound), so a
        # record's `matching_passages` counts the passages scanned so far.
        payload["matching_passages_basis"] = "scanned"
    if order_basis == "bm25":
        payload["rank_words"] = list(rank_words)
        if scan.rank_scan != "index":
            payload["rank_scan"] = scan.rank_scan
        differs = sum(
            1 for node_id, text in counted_text.items()
            if text != str(scan.stored_text.get(node_id) or "")
        )
        if differs:
            # Placed by the stored text's score; matched on the read text.
            payload["rank_read_text_differs"] = differs
    else:
        payload["order_basis_reason"] = "no_literal_words"
    if dropped:
        payload["recheck_dropped"] = dropped
    if residual_where is not None:
        # Whether SQL decided the whole `where` (`exact`) or a residual was
        # checked on read values (`superset`).
        payload["where_qualification"] = compiled_where.qualification
    elif where_lifted_whole:
        # The whole `where` was one passage type, applied as the subtype.
        payload["where_qualification"] = "exact"
    if exhausted and continuation_token is None and not dropped:
        # Retrieval is exhaustive only on the shared scanner's finished
        # proof: a terminal read of every stored match, every candidate
        # decided. A continuation skips rows earlier pages decided, and a
        # serve-site recheck drop leaves the page short, so neither claims it.
        proof = _page_proof(scan_context(), scan)
        if proof is not None:
            payload["retrieval_exhaustive"] = True
            payload["retrieval_basis"] = "grep_" + proof.method
    return payload
