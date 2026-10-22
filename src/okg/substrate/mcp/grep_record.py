r"""`search(method="grep", within="record")`: every term anywhere in one record (#3666).

PACT read-search-caps-3666. Grep's default (`within="unit"`,
`grep_search`) matches all of a query's terms inside one text unit: a
record's own text, or one of its passages (a chunk, a comment). A record
whose words are split across two passages is then never found, whatever
the agent tries. `within="record"` matches each positive term in any text
unit of the same record (its own text and every declared passage), and an
excluded term (`-word`) in none of them. Each term is matched inside one
unit, newline-sensitively, as `filter`'s text condition reads lines
(`text_match.line_term_sql`).

The page is unranked: records in node-id order. Each row says how many
matches the record holds (`match_count`) and shows its matched lines, each
labelled with the unit it is in (`text`, or the passage's type and place).
With `select=["node_id","handle"]` the rows are head lines only: id, type,
title and match count. That is the sweep: every matching record of a type
or source, a page at a time, small.

How one call stays bounded. Every number is stated in the reply when it
stops a page.

1. **Index read**, per positive term the trigram index can serve (an
   `EXPLAIN` names the index), at most `INDEX_TERMS` terms: the records in
   scope holding the term in some unit, read unordered, at most
   `INDEX_UNITS` units. A term whose read returns no more than that is
   complete; the candidates are the records every complete term holds.
   Each term's read stops at `INDEX_TERM_MS`, so a common term whose
   index hits must each be rechecked cannot spend the call's budget
   before a rarer term is tried (on a 542,000-record graph one common
   word's read took 3 s, a rare one's 0.2 s).
2. Without a complete term, the **walk**: the scope's records in node-id
   order, at most `WALK_ROWS` records a call, in windows from
   `WALK_FIRST` doubling to `WALK_MAX`.
3. Candidates are decided in batches of `BATCH` records, in id order, by
   one statement over their units.
4. Steps 1-3 run under `CALL_MS` (`grep_search.deadline`); each statement
   under the smaller of grep's statement timeout and the time left. The
   page's lines are read after, under grep's own timeout. A page that
   reached a bound ends with a continuation token that resumes after the
   last record decided.

Index-read pages are exact: their count is every candidate decided, when
that finished in time (else a lower bound), and they page by offset. Walk
pages count what they decided (a lower bound) and page by token.

Not served (refused by name, `validate_within`): under access rules (Nomos
`audit` or `enforce`; matching reads stored text before the reader's
check, as `text_match` refuses), `path:` terms (use `path_prefix`), a
query with no positive text term, and the arguments only unit grep reads.
"""
from __future__ import annotations

import time
from typing import Any, Callable, Mapping, Optional, Sequence

import psycopg

from okg.substrate.bm25_node_search import branch_point
from okg.substrate.errors import SubstrateError
from okg.substrate.mcp import search_where
from okg.substrate.mcp.continuation import (
    consume_continuation,
    continuation_binding,
    issue_continuation,
)
from okg.substrate.mcp.envelope import validate_select
from okg.substrate.mcp.grep_query import GrepQuery, GrepTerm, parse_grep_query, spelling_tried
from okg.substrate.mcp.grep_search import (
    GREP_ALWAYS,
    GREP_DEFAULT,
    GREP_FIELDS,
    STATEMENT_TIMEOUT_MS,
    _DEADLINE,
    GrepTimeout,
    _bounded,
    _bounded_with,
    _Cancelled,
    _check_regexes_compile,
    _compile_grep_where,
    _declarations,
    _nomos_runtime,
    _INDEX_PLAN,
    deadline,
    resolve_trgm_segment,
)
from okg.substrate.mcp.passage_rollup import resolve_edge_segment
from okg.substrate.mcp.response_bounds import InvalidOperatorArguments
from okg.substrate.mcp.search_profiles import ResolvedScope
from okg.substrate.mcp.session import Session

__all__ = [
    "BATCH",
    "CALL_MS",
    "INDEX_TERMS",
    "INDEX_TERM_MS",
    "INDEX_UNITS",
    "MATCH_COUNT_KEY",
    "WALK_FIRST",
    "WALK_MAX",
    "WALK_ROWS",
    "WITHIN_RECORD",
    "WITHIN_UNIT",
    "search_grep_record",
    "validate_within",
]

WITHIN_UNIT = "unit"
WITHIN_RECORD = "record"
#: The row field: how many matches the record holds over all its units.
MATCH_COUNT_KEY = "match_count"

#: Text units one term's index read may return; a term with more is not a
#: complete candidate set (its records are found by the walk instead).
INDEX_UNITS = 10_000
#: Positive terms tried for the index read, longest literal first.
INDEX_TERMS = 3
#: The most one term's index read may take; a read cut there counts as not
#: complete (`timed_out`) and the next term is tried.
INDEX_TERM_MS = 900
#: Records one statement decides.
BATCH = 500
#: Scope records one call walks, in node-id order, when no term's index
#: read is complete.
WALK_ROWS = 50_000
#: The walk's first window, its growth, and its largest window (records).
WALK_FIRST = 1_000
WALK_GROWTH = 2
WALK_MAX = 8_000
#: The time budget of the call's search for matching records, under grep's
#: 5 s statement timeout; the page's lines are read after it.
CALL_MS = 3_500

_LIVE = "{a}.deleted = FALSE AND {a}.live_lo <= %(bp)s AND %(bp)s < {a}.live_hi"
_TEXT = "({a}.attrs->>'text')"
#: PostgreSQL's embedded option for newline-sensitive matching, as
#: `text_match._LINES`: `^`/`$` at each line, `.` never crosses a line feed.
_LINES = "(?n)"
#: The fields a record row adds to grep's.
RECORD_FIELDS: tuple[str, ...] = (*GREP_FIELDS, MATCH_COUNT_KEY)
_RECORD_DEFAULT: tuple[str, ...] = (*GREP_DEFAULT, MATCH_COUNT_KEY)
_RECORD_ALWAYS: tuple[str, ...] = (*GREP_ALWAYS, MATCH_COUNT_KEY)


def _refusal(message: str, *, fix_hint: str, **data: Any) -> SubstrateError:
    err = SubstrateError(
        f"search: {message}", data={"within": WITHIN_RECORD, **data}, fix_hint=fix_hint,
    )
    err.code = "unsupported_operator_form"
    return err


def validate_within(
    within: Any,
    *,
    method: str,
    query: Any,
    view: Any,
    record_scope: bool,
    rank_by: Any,
    completeness: Any,
    retain: bool,
    max_per_parent: Any,
    offset: int,
    continuation_token: Any,
) -> Optional[str]:
    """The `within` the call is served with, or a refusal before any read.

    None or "unit" is grep's default and every method's only form;
    "record" is served on `method="grep"` with the arguments above unset."""
    if within is None or within == WITHIN_UNIT:
        return None if within is None else WITHIN_UNIT
    if within != WITHIN_RECORD:
        raise InvalidOperatorArguments(
            f"search: within must be 'unit' or 'record'; got {within!r}",
        )
    if method != "grep":
        raise _refusal(
            f"within='record' is a grep form; this call is method={method!r}",
            fix_hint="send method='grep' with within='record', or drop within",
            method=method,
        )
    parsed = parse_grep_query(query or "")
    if parsed.path_terms:
        raise _refusal(
            "within='record' takes no path: term",
            fix_hint="scope by path with path_prefix, or drop within",
            reason="path_term",
        )
    if not parsed.positive:
        raise _refusal(
            "within='record' needs at least one word, phrase or re:/.../ to match",
            fix_hint="add a term the records must hold",
            reason="no_positive_term",
        )
    unread = [
        name for name, given in (
            ("view", view not in (None, "cards")),
            ("scope", record_scope),
            ("rank_by", rank_by is not None),
            ("completeness", completeness is not None),
            ("retain", bool(retain)),
            ("max_per_parent", max_per_parent is not None),
        ) if given
    ]
    if unread:
        raise _refusal(
            f"within='record' does not read {', '.join(unread)}",
            fix_hint=f"drop {', '.join(unread)}, or drop within for grep's unit form",
            arguments=unread,
        )
    if offset and continuation_token is not None:
        raise InvalidOperatorArguments(
            "search(method=grep): offset and continuation_token cannot be "
            "combined; a continuation starts where its page ended"
        )
    return WITHIN_RECORD


# ---------- SQL ----------


def _term_clause(term: GrepTerm, params: dict[str, Any], key: str, *, column: str) -> str:
    """One term's match on one unit's text, newline-sensitive, never negated
    here (the caller negates over the record). A NEAR term ANDs each operand
    first, as grep does, so the index can narrow it."""
    params[key] = _LINES + term.pg_regex
    parts = []
    for at, operand in enumerate(term.operands):
        params[f"{key}o{at}"] = _LINES + operand
        parts.append(f"{column} ~* %({key}o{at})s")
    return "(" + " AND ".join([*parts, f"{column} ~* %({key})s"]) + ")"


class _Scope:
    """The records a call reads: SQL on a record alias, and the declared
    passage relations whose children are its units."""

    def __init__(
        self,
        conn: psycopg.Connection,
        session: Session,
        *,
        filter: Mapping[str, Any],
        scope: ResolvedScope,
        path_prefix: Optional[str],
    ) -> None:
        self.segment = resolve_trgm_segment(conn)
        self.edge_segment = resolve_edge_segment(conn)
        self.declared = _declarations(conn, session)
        children = sorted({child for _p, _e, child in self.declared})
        self.params: dict[str, Any] = {"bp": branch_point(conn, session)}
        subtype = filter.get("subtype")
        if subtype is not None and str(subtype) in children:
            raise _refusal(
                f"within='record' reads passages through their record; "
                f"{subtype!r} is a passage type",
                fix_hint="name the record type the passages belong to, or drop within",
                reason="passage_type",
            )
        residual = filter.get("where")
        types: Optional[set[str]] = None
        for named in (
            [str(subtype)] if subtype is not None else None,
            search_where.admitted_types(residual) if residual is not None else None,
            list(scope.subtypes) if scope.subtypes is not None else None,
        ):
            if named is not None:
                types = set(named) if types is None else types & set(named)
        # A passage type a profile lists is read through its record, never
        # as a record of its own.
        self.types = sorted(types - set(children)) if types is not None else None
        clauses = []
        if self.types is not None:
            self.params["gr_types"] = self.types
            clauses.append("{a}.subtype = ANY(%(gr_types)s)")
        elif children:
            # Every record type: a passage is read through its record.
            self.params["gr_children"] = children
            clauses.append("NOT ({a}.subtype = ANY(%(gr_children)s))")
        if scope.source_ids is not None:
            self.params["gr_sources"] = list(scope.source_ids)
            clauses.append("{a}.source = ANY(%(gr_sources)s)")
        if filter.get("source") is not None:
            self.params["gr_source"] = str(filter["source"])
            clauses.append("{a}.source = %(gr_source)s")
        attr_filters = filter.get("attr_filters") or {}
        if attr_filters:
            from psycopg.types.json import Jsonb

            self.params["gr_af"] = Jsonb(dict(attr_filters))
            clauses.append("{a}.attrs @> %(gr_af)s::jsonb")
        if path_prefix:
            escaped = (
                path_prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            )
            self.params["gr_path"] = escaped + "%"
            clauses.append("({a}.attrs->>'path') LIKE %(gr_path)s")
        self.clauses = clauses
        # The `where`: its id/type/source part in SQL (`exact` or a
        # superset), every record it admits checked again in Python on the
        # values the reader reads (`_admitted`).
        self.where_sql: Optional[str] = None
        if residual is not None:
            compiled = _compile_grep_where(residual, self.params, alias="r", prefix="gr_w")
            if compiled.sql != "TRUE":
                self.where_sql = compiled.bind_into(self.params)
        self.row_check = bool(residual is not None or attr_filters or scope.excluded_path_globs)
        if declared := self.declared:
            values = []
            for index, (parent, edge, child) in enumerate(declared):
                self.params[f"grp{index}"] = parent
                self.params[f"gre{index}"] = edge
                self.params[f"grc{index}"] = child
                values.append(
                    f"(%(grp{index})s::text, %(gre{index})s::text, %(grc{index})s::text)"
                )
            self.values = f"(VALUES {', '.join(values)}) AS d(parent, edge, child)"
        else:
            self.values = ""

    def record_where(self, alias: str) -> list[str]:
        """The scope on a record alias (`where` SQL is on `r` only)."""
        out = [_LIVE.format(a=alias), *(c.format(a=alias) for c in self.clauses)]
        if self.where_sql is not None:
            if alias != "r":
                raise ValueError("_Scope.record_where: the where SQL is bound to alias r")
            out.append(self.where_sql)
        return out

    def units_sql(
        self, *, unit_clause: Optional[Callable[[str], str]], record_ids: bool,
    ) -> str:
        """`(rid, t)`: each text unit of the scoped records, with its record.

        `unit_clause(alias)` is SQL on that alias's text keeping the units
        that hold a term (None: every unit). The record is `r`; its own
        text is the unit `r`, a declared passage the unit `n`. `record_ids`
        restricts to `%(gr_ids)s`, whose scope was applied already."""
        own = [_LIVE.format(a="r"), "r.attrs ? 'text'"]
        child = [_LIVE.format(a="r"), _LIVE.format(a="e"), _LIVE.format(a="n"), "n.attrs ? 'text'"]
        if record_ids:
            own.append("r.node_id = ANY(%(gr_ids)s)")
            child.append("r.node_id = ANY(%(gr_ids)s)")
        else:
            scoped = self.record_where("r")[1:]
            own += scoped
            child += scoped
        if unit_clause is not None:
            own.append(unit_clause(_TEXT.format(a="r")))
            child.append(unit_clause(_TEXT.format(a="n")))
        parts = [
            f"SELECT r.node_id AS rid, {_TEXT.format(a='r')} AS t FROM {self.segment} r "
            f"WHERE {' AND '.join(own)}"
        ]
        if self.values:
            parts.append(
                f"SELECT r.node_id AS rid, {_TEXT.format(a='n')} AS t FROM {self.segment} r "
                f"JOIN {self.edge_segment} e ON e.src = r.node_id "
                f"JOIN {self.segment} n ON n.node_id = e.dst "
                f"JOIN {self.values} ON d.parent = r.subtype AND d.edge = e.edge_type "
                f"AND d.child = n.subtype WHERE {' AND '.join(child)}"
            )
        return " UNION ALL ".join(f"({part})" for part in parts)


# ---------- the method ----------


def _binding(
    conn: psycopg.Connection,
    session: Session,
    *,
    query: str,
    filter: Mapping[str, Any],
    path_prefix: Optional[str],
    scope: ResolvedScope,
) -> dict[str, Any]:
    import hashlib
    import json

    projection = hashlib.sha256(json.dumps({
        "within": WITHIN_RECORD,
        "query": query,
        "filter": {k: v for k, v in filter.items() if k != "where_context"},
        "path_prefix": path_prefix,
        "subtypes": scope.subtypes,
        "source_ids": scope.source_ids,
        "excluded_path_globs": scope.excluded_path_globs,
    }, sort_keys=True, default=str).encode("utf-8")).hexdigest()
    return continuation_binding(
        conn, session, kind="grep_page", sort="grep_record", projection=projection,
    )


def _indexed(conn: psycopg.Connection, sql: str, params: Mapping[str, Any]) -> bool:
    """Whether PostgreSQL reads `sql` through the grep trigram index."""
    try:
        lines = _bounded_with(conn, sql, params, settings=(), explain=True)
    except _Cancelled:
        return False
    return any("node_text_trgm_visible" in row["line"] for row in lines)


def _index_candidates(
    conn: psycopg.Connection,
    sc: _Scope,
    parsed: GrepQuery,
    *,
    after: str,
    tried: list[dict[str, Any]],
) -> Optional[list[str]]:
    """The records (id order, past `after`) every complete term's index read
    holds, or None when no positive term's read was complete."""
    terms = sorted(
        enumerate(parsed.positive),
        key=lambda item: (-item[1].run, item[0]),
    )[:INDEX_TERMS]
    found: Optional[set[str]] = None
    for index, term in terms:
        params = dict(sc.params)
        params["gr_cap"] = INDEX_UNITS + 1
        def clause(column: str, term: GrepTerm = term, key: str = f"gri{index}") -> str:
            return _term_clause(term, params, key, column=column)

        sql = (
            f"SELECT u.rid FROM ({sc.units_sql(unit_clause=clause, record_ids=False)}) u "
            "LIMIT %(gr_cap)s"
        )
        entry: dict[str, Any] = {"term": term.text}
        tried.append(entry)
        if not _indexed(conn, sql, params):
            entry["read"] = "no_index"
            continue
        outer = _DEADLINE.get()
        term_at = time.monotonic() + INDEX_TERM_MS / 1000
        try:
            with deadline(term_at if outer is None else min(outer, term_at)):
                rows = _bounded(conn, sql, params)
        except _Cancelled:
            entry["read"] = "timed_out"
            continue
        if len(rows) > INDEX_UNITS:
            entry["read"] = "over_cap"
            continue
        records = {str(row["rid"]) for row in rows}
        entry["read"] = "complete"
        entry["records"] = len(records)
        found = records if found is None else found & records
    if found is None:
        return None
    return sorted(r for r in found if r > after)


def _walk(
    conn: psycopg.Connection, sc: _Scope, *, after: str, window: int,
) -> list[str]:
    """The next `window` scope records past `after`, in node-id order,
    through the node-id index (grep's walk plan)."""
    params = {**sc.params, "gr_after": after, "gr_window": window}
    sql = (
        f"SELECT r.node_id FROM {sc.segment} r WHERE "
        f"{' AND '.join(sc.record_where('r'))} AND r.node_id > %(gr_after)s "
        "ORDER BY r.node_id LIMIT %(gr_window)s"
    )
    return [str(row["node_id"]) for row in _bounded_with(conn, sql, params, settings=_INDEX_PLAN)]


def _decide(
    conn: psycopg.Connection, sc: _Scope, parsed: GrepQuery, ids: Sequence[str],
) -> set[str]:
    """Which of `ids` hold every positive term in some unit and no excluded
    term in any: one statement over their units."""
    if not ids:
        return set()
    params = {**sc.params, "gr_ids": list(ids)}
    having = []
    for index, term in enumerate(parsed.terms):
        clause = _term_clause(term, params, f"grd{index}", column="u.t")
        having.append(f"{'NOT ' if term.negated else ''}bool_or({clause})")
    sql = (
        f"SELECT u.rid FROM ({sc.units_sql(unit_clause=None, record_ids=True)}) u "
        f"GROUP BY u.rid HAVING {' AND '.join(having)}"
    )
    return {str(row["rid"]) for row in _bounded(conn, sql, params)}


def _record_rows(
    conn: psycopg.Connection, session: Session, sc: _Scope, ids: Sequence[str],
) -> dict[str, dict[str, Any]]:
    """The records' rows, with the attributes the reader reads."""
    from okg.substrate.attributes import apply_effective_attrs_to_rows

    if not ids:
        return {}
    params = {**sc.params, "gr_ids": list(ids)}
    rows = _bounded(conn, (
        f"SELECT r.node_id, r.subtype, r.attrs, r.text_hash, r.source, "
        f"r.source_record_id, r.source_revision FROM {sc.segment} r "
        f"WHERE r.node_id = ANY(%(gr_ids)s) AND {_LIVE.format(a='r')}"
    ), params)
    rows = apply_effective_attrs_to_rows(
        conn, rows, target_kind="node", target_id_field="node_id",
        generation_id=session.generation_id,
    )
    return {str(row["node_id"]): row for row in rows}


def _admitted(
    rows: Mapping[str, dict[str, Any]],
    ids: Sequence[str],
    *,
    filter: Mapping[str, Any],
    scope: ResolvedScope,
) -> list[str]:
    """The ids whose record passes the call's row selections on read values."""
    from okg.substrate.mcp.search import _matches_filter, _row_filter

    row_filter = {**_row_filter(dict(filter)), "subtype": None}
    out = []
    for node_id in ids:
        row = rows.get(node_id)
        if row is None:
            continue
        if _matches_filter(dict(row), row_filter, scope=scope):
            out.append(node_id)
    return out


def _unit_label(entry: Mapping[str, Any]) -> tuple[str, Optional[int]]:
    """Which unit a match line is in, and its place: `text` (the record's
    own text, or a line of the record's text a chunk was placed in; no
    place) or the passage's type and its stated place among the record's
    passages."""
    subtype = entry.get("unit_subtype")
    ordinal = entry.get("unit_ordinal")
    if not entry.get("passage_id"):
        return "text", None
    if entry.get("line_basis") is None and subtype == "document_chunk":
        # Placed as a line of the record's text, which `inspect` text reads.
        return "text", None
    place = int(ordinal) if isinstance(ordinal, int) and not isinstance(ordinal, bool) else None
    return str(subtype or "passage"), place


def search_grep_record(
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
) -> dict[str, Any]:
    """One page of records holding every term somewhere in them; see the
    module doc."""
    from okg.substrate.mcp import text_match as tm
    from okg.substrate.mcp.search import _fence_search_rows, record_identity
    from okg.substrate.mcp.search import search_select_elsewhere

    validate_select(select, RECORD_FIELDS, tool_name="search(method=grep, within=record)",
                    elsewhere=search_select_elsewhere("grep"))
    if _nomos_runtime(session):
        raise _refusal(
            "within='record' is not served under access rules on this deployment",
            fix_hint="drop within: grep's unit form reads text under the access rules",
            reason="access_rules",
        )
    parsed = parse_grep_query(query)
    _check_regexes_compile(conn, parsed, query)
    sc = _Scope(conn, session, filter=filter, scope=scope, path_prefix=path_prefix)
    binding = _binding(
        conn, session, query=query, filter=filter, path_prefix=path_prefix, scope=scope,
    )
    after, listed_before = "", 0
    if continuation_token is not None:
        cursor = consume_continuation(continuation_token, expected_binding=binding)
        after = str(cursor.get("after") or "")
        listed_before = int(cursor.get("listed") or 0)
    started = time.monotonic()
    need = offset + limit + 1
    matched: list[str] = []
    rows_by_id: dict[str, dict[str, Any]] = {}
    decided = 0
    last_decided = after
    stop: Optional[str] = None
    tried: list[dict[str, Any]] = []
    candidates: Optional[list[str]] = None
    complete_count = False
    total_index: Optional[int] = None

    def take(batch: list[str]) -> bool:
        """Decide `batch` (id order); True when the page is full."""
        nonlocal decided, last_decided
        hits = _decide(conn, sc, parsed, batch)
        ordered = [node_id for node_id in batch if node_id in hits]
        if ordered and sc.row_check:
            fetched = _record_rows(conn, session, sc, ordered)
            rows_by_id.update(fetched)
            ordered = _admitted(fetched, ordered, filter=filter, scope=scope)
        kept = set(ordered)
        for node_id in batch:
            if len(matched) >= need:
                return True
            decided += 1
            last_decided = node_id
            if node_id in kept:
                matched.append(node_id)
        return len(matched) >= need

    with deadline(started + CALL_MS / 1000):
        try:
            candidates = _index_candidates(conn, sc, parsed, after=after, tried=tried)
            if candidates is not None:
                at = 0
                while at < len(candidates):
                    if take(candidates[at: at + BATCH]):
                        break
                    at += BATCH
                if len(matched) >= need:
                    # The count: the remaining candidates, in the time left.
                    rest = candidates[candidates.index(last_decided) + 1:] if last_decided in candidates else []
                    extra = 0
                    try:
                        for start in range(0, len(rest), BATCH):
                            part = rest[start: start + BATCH]
                            hits = _decide(conn, sc, parsed, part)
                            ordered = [i for i in part if i in hits]
                            if ordered and sc.row_check:
                                ordered = _admitted(
                                    _record_rows(conn, session, sc, ordered), ordered,
                                    filter=filter, scope=scope,
                                )
                            extra += len(ordered)
                        complete_count = True
                    except _Cancelled:
                        extra = 0
                    total_index = len(matched) + extra if complete_count else None
                else:
                    complete_count = True
                    total_index = len(matched)
            else:
                walked = 0
                window = WALK_FIRST
                cursor_at = after
                while True:
                    window = min(window, WALK_ROWS - walked)
                    ids = _walk(conn, sc, after=cursor_at, window=window)
                    walked += len(ids)
                    full = False
                    for start in range(0, len(ids), BATCH):
                        if take(ids[start: start + BATCH]):
                            full = True
                            break
                    if full:
                        break
                    if len(ids) < window:
                        break  # the scope is walked to its end
                    cursor_at = ids[-1]
                    if walked >= WALK_ROWS:
                        stop = "record_rows"
                        break
                    window = min(window * WALK_GROWTH, WALK_MAX)
        except _Cancelled:
            stop = "call_ms"
    if stop == "call_ms" and not matched and decided == 0:
        raise GrepTimeout(
            f"search: within='record' decided no record within its {CALL_MS:,} ms budget",
            data={"timeout_ms": CALL_MS, "within": WITHIN_RECORD},
            fix_hint=(
                "add a type in where, a source or a path_prefix, or a longer "
                "literal the index can use"
            ),
        )
    mode = "index" if candidates is not None else "walk"
    truncated = len(matched) > offset + limit
    page_ids = matched[offset: offset + limit]

    # The rows: each record's matches, counted over all its units.
    condition = tm.TextCondition(value=query, parsed=parsed)
    lines_timed_out = False
    try:
        found = tm.record_matches(
            conn, session, condition, page_ids, tool="search", with_lines=True,
            with_units=True,
        )
    except GrepTimeout:
        # Said on the page (`match_lines_timed_out`): the records matched,
        # their lines were not read in time.
        found, lines_timed_out = {}, True
    missing = [i for i in page_ids if i not in rows_by_id]
    rows_by_id.update(_record_rows(conn, session, sc, missing))
    full_rows = []
    for node_id in page_ids:
        row = rows_by_id.get(node_id)
        if row is None:
            continue
        result = found.get(node_id)
        lines = []
        for entry in (result.entries if result is not None else []):
            unit, place = _unit_label(entry)
            lines.append({
                "line": entry.get("line"),
                "text": entry.get("line_text", entry.get("text")),
                "field": None, "author": None, "date": None,
                "unit": unit,
                **({"unit_place": place} if place is not None else {}),
            })
        full_rows.append({
            "node_id": row["node_id"],
            "canonical_node_id": row["node_id"],
            "subtype": row["subtype"],
            "attrs": row.get("attrs"),
            "excerpt": None,
            "text_hash": row.get("text_hash"),
            "method": "grep",
            "match_kind": "grep",
            "source": row.get("source"),
            "source_record_id": record_identity(row.get("source_record_id")),
            "source_revision": record_identity(row.get("source_revision")),
            "parent": None,
            "position": None,
            "matching_passages": None,
            "match_windows": [],
            "match_lines": lines,
            MATCH_COUNT_KEY: result.count if result is not None else None,
        })
    results = _fence_search_rows(
        conn, session, full_rows, select=select,
        default=_RECORD_DEFAULT, always=_RECORD_ALWAYS, scope=scope, max_chars=max_chars,
    )

    token: Optional[str] = None
    next_offset: Optional[int] = None
    shown = listed_before + offset + len(page_ids)
    if truncated:
        if mode == "index" and continuation_token is None:
            next_offset = offset + limit
        else:
            token = issue_continuation(
                binding, cursor={"after": page_ids[-1], "listed": shown},
            )
    elif stop is not None:
        token = issue_continuation(binding, cursor={"after": last_decided, "listed": shown})
    if mode == "index" and continuation_token is None:
        total = total_index
        basis = "exact" if complete_count else "lower_bound"
        if total is None:
            total = len(matched)
    else:
        total = len(matched) + listed_before
        basis = "exact" if (stop is None and not truncated) else "lower_bound"
    if stop is None:
        page_basis = "top_of_all_visible_matches"
    else:
        page_basis = "scan_bound_reached"
    passage_types = {child for _p, _e, child in sc.declared}
    payload: dict[str, Any] = {
        "query": query,
        "method": "grep",
        "within": WITHIN_RECORD,
        "generation_id": session.generation_id,
        "pinned_generation_id": session.generation_id,
        "limit": limit,
        "offset": listed_before + offset,
        "next_offset": next_offset,
        "results": results,
        "total_count": total,
        "truncated": truncated,
        "total_count_basis": basis,
        "count_unit": "record",
        "result_unit": "record",
        "and_scope": "record",
        "grep_terms": parsed.disclosure(),
        "path_prefix": path_prefix,
        "order_basis": "node_id",
        "page_basis": page_basis,
        "page_scan_bound": {"record_rows": WALK_ROWS, "call_ms": CALL_MS},
        "statement_timeout_ms": STATEMENT_TIMEOUT_MS,
        "record_scan": {
            "mode": mode,
            "records_decided": decided,
            **({"index_terms": tried} if tried else {}),
        },
        "grep_searched": {
            "fields": ["text"],
            "comments": bool(
                passage_types if sc.types is None
                else {c for p, _e, c in sc.declared if p in sc.types}
            ),
            "subtypes": sc.types,
            "spellings_tried": [spelling_tried(term) for term in parsed.positive],
            "spellings_matched": [],
            **({"bound_read": {
                "bound": stop, "rows": decided,
                **({"continued": True} if continuation_token is not None else {}),
            }} if stop is not None else {}),
        },
    }
    if stop is not None:
        payload["page_scan_bound_hit"] = stop
        if not truncated:
            payload["next_offset_reason"] = page_basis
    if token is not None:
        payload["continuation_token"] = token
    if lines_timed_out:
        payload["match_lines_timed_out"] = True
    return payload
