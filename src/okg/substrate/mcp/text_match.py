r"""Text matching as a condition on a set of records (issue #3666, step M7).

Design: DESIGN.md of lane RB, section 2.2 "Text", 2.3 (the fields a reply
adds) and 3 (filter, aggregate). One clause on the field `text` with the
operator `matches` keeps the records whose text holds the value:

    {"text": {"op": "matches", "value": "re:/CMS[A-Z]+-[0-9]+/"}}
    {"op": "matches", "field": "text", "value": "reaper pods"}

The value is grep's grammar (`grep_query`): words, all of which must occur;
`"a phrase"`; `re:/.../`; `-word` to exclude; case-insensitive. A `path:`
term is refused here: a record's path is the field `attrs.path`.

What `text` reads. A record's text is its own `attrs.text` and the text of
each passage the catalog declares for its type (`passage_rollup`, the units
`search(method="grep")` folds into a record). Unlike grep's text units, the
value is tested on the record's whole text (its texts in reading order,
joined by line feeds): every term and every exclusion holds over the record.
Every pattern is matched newline-sensitively, as grep reads lines: `^` and
`$` match at each line boundary, and `.` and a bracket complement do not
cross a line feed (`\s` still does, so a phrase can run across lines).
A shown match is found inside one text, so a phrase running across two
passages admits the record but has no match to show (the reply counts such
records).

How a call stays exact. The records whose text matches are found first
(`matching_record_ids`), then the rest of the condition runs over exactly
those ids through the operator's own SQL, so the count, the order and the
pages are the operator's. Finding the matching records is bounded by one
number, :data:`SCAN_LIMIT` (the bound `filter` already reads to,
`READ_FILTER_SCAN_CAP`):

* a value with an indexable term (a run of 3 or more literal letters or
  digits, `grep_query.MIN_INDEXABLE_RUN`) is looked up through grep's
  trigram index, restricted by the condition's type when it names one; at
  most `SCAN_LIMIT` matching records are served this way;
* otherwise, and when that lookup finds more, the rest of the condition is
  read first; when it holds at most `SCAN_LIMIT` records, their text is
  checked one by one; when it holds more, the call is refused, naming the
  limit and the narrowing that works. Nothing is ever served from part of
  the set.

Under access rules (Nomos `audit` or `enforce`) the condition is refused
plainly: matching reads stored text before the reader's check, so a match
could reveal text the reader may not read. The grep search method keeps its
own enforcement; this condition does not reuse it.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from re import _constants as _C  # type: ignore[attr-defined]
from re import _parser as _P  # type: ignore[attr-defined]
from typing import Any, Mapping, Optional, Sequence

import psycopg

from okg.substrate.bm25_node_search import branch_point
from okg.substrate.errors import SubstrateError
from okg.substrate.mcp.grep_query import (
    GrepQuery,
    GrepTerm,
    _literal_term,
    _silence_nested_set_warnings,
    GrepQueryInvalid,
    parse_grep_query,
)
from okg.substrate.mcp.grep_search import (
    STATEMENT_TIMEOUT_MS,
    _DEADLINE,
    GrepTimeout,
    _bounded,
    deadline,
    _Cancelled,
    _check_regexes_compile,
    _declarations,
    _line_of,
    _record_lines_in_time,
    resolve_trgm_segment,
    window_text,
)
from okg.substrate.mcp.passage_rollup import resolve_edge_segment
from okg.substrate.mcp.provenance import _fence_value, classify_sources
from okg.substrate.mcp.record_text import MatchPlace
from okg.substrate.mcp.result_text import MATCH_LINE_IN_OWN_TEXT, MATCH_LINE_IN_PASSAGE
from okg.substrate.mcp.session import Session
from okg.substrate.transcript_trust import (
    UNTRUSTED_CONTENT_ORIGIN_ATTR,
    WEB_TOOL_RESULT_ORIGIN,
)

__all__ = [
    "MATCHES_CAP",
    "MATCH_FIELDS",
    "NARROWING_TERMS",
    "NARROWING_TERM_MS",
    "REPEATED_PASSAGE_MIN_CHARS",
    "SCAN_LIMIT",
    "TEXT_FIELD",
    "TextCondition",
    "TextConditionInvalid",
    "TextMatchScanLimit",
    "capture_terms",
    "capturing_groups",
    "count_matches",
    "holds_text_clause",
    "id_scope",
    "matching_record_ids",
    "line_term_sql",
    "narrowing_queries",
    "narrowing_query",
    "required_literal",
    "neighbour_ids",
    "record_matches",
    "refuse_scan_limit",
    "refuse_under_access_rules",
    "split_text_condition",
    "timed_out",
    "type_scope",
]

#: The field a text condition names.
TEXT_FIELD = "text"
#: Its one operator.
MATCHES_OP = "matches"
#: The fields a row selected through a text condition can carry.
MATCH_FIELDS = ("match", "matches", "match_count")
#: Most records whose text one call checks or serves from a text condition.
SCAN_LIMIT = 10_000
#: Most entries of one row's `matches`; `match_count` counts every match.
MATCHES_CAP = 10

_LIVE = "{a}.deleted = FALSE AND {a}.live_lo <= %(bp)s AND %(bp)s < {a}.live_hi"
_TEXT = "(n.attrs->>'text')"
#: Where a refusal says the condition is accepted.
_ACCEPTED_ON = "filter, map, aggregate and expand's far end"
_FORM = '{"text": {"op": "matches", "value": "<words, \\"a phrase\\" or re:/<regex>/>"}}'


class TextMatchScanLimit(SubstrateError):
    """A text condition the server cannot finish within the scan limit."""

    code = "text_match_scan_limit"


class TextConditionInvalid(SubstrateError):
    """A text condition the operator cannot read as written.

    The code is the argument check's own (`response_bounds.
    InvalidOperatorArguments`). It is not a `ValueError`, so an operator
    body's `except ValueError` (which turns a shape it cannot lower into
    `unsupported_operator_form` with `query` as the fallback) never swallows
    the refusal and its fix."""

    code = "invalid_operator_arguments"


@dataclass(frozen=True)
class TextCondition:
    """One `text matches` clause: the value as sent and as parsed."""

    value: str
    parsed: GrepQuery


@dataclass
class RecordMatches:
    """What one record's text holds: every match counted, the first ones kept."""

    count: int = 0
    first: Optional[str] = None
    entries: list[dict[str, Any]] = field(default_factory=list)


# ---------- the condition ----------


def _is_text_clause(node: Any) -> bool:
    return isinstance(node, Mapping) and node.get("field") == TEXT_FIELD


def _conjuncts(node: Any) -> list[Any]:
    """The clauses a condition ANDs, through nested `and`s (and is
    associative, so an `and` inside an `and` adds nothing)."""
    if (
        isinstance(node, Mapping) and node.get("op") == "and"
        and isinstance(node.get("clauses"), list)
    ):
        return [c for clause in node["clauses"] for c in _conjuncts(clause)]
    return [] if node in (None, {}, True) else [node]


def holds_text_clause(node: Any) -> bool:
    """Whether a condition tree holds a clause on `text` or a `matches` test.

    Walks the condition's own structure only (`clause`, `clauses`, and a
    set scope's `filter`), never a compared value."""
    if not isinstance(node, Mapping):
        return False
    if node.get("field") == TEXT_FIELD or node.get("op") == MATCHES_OP:
        return True
    subs: list[Any] = [node.get("clause"), node.get("filter")]
    if isinstance(node.get("clauses"), list):
        subs.extend(node["clauses"])
    return any(holds_text_clause(sub) for sub in subs)


def _refuse(message: str, *, fix_hint: str, tool: str, **data: Any) -> "TextConditionInvalid":
    return TextConditionInvalid(
        f"{tool}: {message}", data={"reason": "text_condition", **data}, fix_hint=fix_hint,
    )


def split_text_condition(
    predicate: Any, *, tool: str,
) -> tuple[Any, Optional[TextCondition]]:
    """`(the condition without its text clause, the text clause)`.

    The text clause may stand alone or directly inside one top-level `and`;
    anywhere else (under `or` or `not`, nested) it is refused, because the
    set operator would have to decide it per record beside other tests.
    One text clause per call; its value is checked against grep's grammar
    here, before any SQL runs. A condition with no text clause comes back
    unchanged with None.
    """
    if not holds_text_clause(predicate):
        return predicate, None
    conjuncts = _conjuncts(predicate)
    clauses = [
        c for c in conjuncts
        if _is_text_clause(c) or (isinstance(c, Mapping) and c.get("op") == MATCHES_OP)
    ]
    rest = [c for c in conjuncts if not any(c is t for t in clauses)]
    if any(holds_text_clause(c) for c in rest):
        raise _refuse(
            "a text condition stands alone or directly inside one top-level "
            "and, not under or, not or a nested group",
            fix_hint=(
                "write the text clause beside the other conditions: "
                '{"op": "and", "clauses": [<text clause>, <other clauses>]}, '
                "or as one more key of the short form"
            ),
            tool=tool,
        )
    if len(clauses) > 1:
        raise _refuse(
            "one call takes one text condition",
            fix_hint=(
                "write every term in one value: its words must all occur in "
                'one text, e.g. "reaper pods" or re:/a|b/ for either'
            ),
            tool=tool,
        )
    clause = clauses[0]
    if clause.get("field") != TEXT_FIELD:
        raise _refuse(
            f"`matches` tests the field `text`, not {clause.get('field')!r}",
            fix_hint=f"write {_FORM}; compare an attribute with eq, prefix or suffix",
            tool=tool,
        )
    if clause.get("op") != MATCHES_OP:
        raise _refuse(
            f"the field `text` takes the operator `matches`, not {clause.get('op')!r}",
            fix_hint=f"write {_FORM}",
            tool=tool,
        )
    value = clause.get("value")
    if not isinstance(value, str) or not value.strip():
        raise _refuse(
            "a text condition needs a non-empty string value",
            fix_hint=f"write {_FORM}",
            tool=tool,
        )
    parsed = parse_grep_query(value)
    if parsed.path_terms:
        raise GrepQueryInvalid(
            "a text condition matches text; a path: term reads the record's path",
            data={"reason": "path_term_in_text_condition", "query": value[:1024]},
            fix_hint=(
                'compare the path itself: {"attrs.path": {"op": "prefix", '
                '"value": "<dir/>"}} or suffix, beside the text clause'
            ),
        )
    if not rest:
        remaining: Any = None
    elif len(rest) == 1:
        remaining = rest[0]
    else:
        remaining = {"op": "and", "clauses": rest}
    return remaining, TextCondition(value=value, parsed=parsed)


def type_scope(predicate: Any) -> Optional[list[str]]:
    """The record types a condition's top level requires, or None.

    Only an `eq` or `in` on `subtype` that the top level ANDs (alone, or in
    `and`s only) narrows: it then holds for every record the condition
    admits, so the types are a safe restriction of the text lookup."""
    found: Optional[set[str]] = None
    for clause in _conjuncts(predicate):
        if not isinstance(clause, Mapping) or clause.get("field") != "subtype":
            continue
        if clause.get("op") == "eq" and isinstance(clause.get("value"), str):
            values = {clause["value"]}
        elif clause.get("op") == "in" and isinstance(clause.get("values"), list) and all(
            isinstance(v, str) for v in clause["values"]
        ):
            values = set(clause["values"])
        else:
            continue
        found = values if found is None else found & values
    return None if found is None else sorted(found)


def id_scope(predicate: Any) -> Optional[list[str]]:
    """The record ids a condition's top level requires, or None.

    An `eq` or `in` on `node_id` that the top level ANDs holds for every
    record the condition admits, so the set the text is checked within is
    at most those ids (PACT read-search-caps-3666: a text condition scoped
    to 2 ids read the whole graph's index matches first and timed out)."""
    found: Optional[set[str]] = None
    for clause in _conjuncts(predicate):
        if not isinstance(clause, Mapping) or clause.get("field") != "node_id":
            continue
        if clause.get("op") == "eq" and isinstance(clause.get("value"), str):
            values = {clause["value"]}
        elif clause.get("op") == "in" and isinstance(clause.get("values"), list) and all(
            isinstance(v, str) for v in clause["values"]
        ):
            values = set(clause["values"])
        else:
            continue
        found = values if found is None else found & values
    return None if found is None else sorted(found)


# ---------- refusals with the number ----------


def refuse_under_access_rules(tool: str) -> SubstrateError:
    """The plain refusal under Nomos `audit` or `enforce`."""
    err = SubstrateError(
        f"{tool}: text matching is not available under access rules on this "
        "deployment",
        data={"reason": "text_condition_under_access_rules", "tool": tool},
        fix_hint=(
            'drop the text clause; search(query=..., method="grep") reads text '
            "under the access rules, one page of records at a time"
        ),
    )
    err.code = "unsupported_operator_form"
    return err


def refuse_scan_limit(
    tool: str,
    *,
    indexable: bool,
    text_over: bool,
    rest_over: bool,
    rest_given: bool,
    set_words: str = "the rest of the condition",
) -> TextMatchScanLimit:
    """The refusal when the matching records cannot be found within the limit.

    It names the limit and each part that passed it: the records the text
    matches, and the set the text would be checked within (`set_words`)."""
    limit = f"{SCAN_LIMIT:,}"
    parts = []
    if not indexable:
        parts.append("the value has no run of 3 letters or digits the index can use")
    if text_over:
        parts.append(f"more than {limit} records hold the value's indexed term")
    if rest_over:
        parts.append(f"{set_words if rest_given else 'the set'} holds more than {limit} records")
    why = ", and ".join(parts)
    narrow = (
        'add a type ({"type": "<subtype>"}) or an attrs.<key> condition that '
        f"leaves at most {limit} records"
    )
    if not indexable:
        narrow += ", or put 3 literal letters or digits in the value"
    return TextMatchScanLimit(
        f"{tool}: {why}; text matching is exact only within {limit} records",
        data={
            "scan_limit": SCAN_LIMIT, "indexable": indexable,
            "text_over_limit": text_over, "rest_over_limit": rest_over,
        },
        fix_hint=narrow,
    )


# ---------- the matching records ----------


def timed_out(tool: str) -> GrepTimeout:
    return GrepTimeout(
        f"{tool}: matching the text ran past the {STATEMENT_TIMEOUT_MS:,} ms "
        "statement timeout",
        data={"timeout_ms": STATEMENT_TIMEOUT_MS},
        fix_hint="narrow the condition with a type or an attrs.<key> condition",
    )


#: A unit's place in its record's reading order: its own text first, then
#: passages by their stated order, then by id (`_ordinal` in Python).
_ORDINAL_SQL = (
    "CASE WHEN jsonb_typeof(n.attrs->'chunk_ordinal') = 'number' "
    "THEN (n.attrs->>'chunk_ordinal')::numeric "
    "WHEN jsonb_typeof(n.attrs->'chunk_index') = 'number' "
    "THEN (n.attrs->>'chunk_index')::numeric "
    "WHEN jsonb_typeof(n.attrs->'char_offset') = 'number' "
    "THEN (n.attrs->>'char_offset')::numeric END"
)


def _units_sql(
    conn: psycopg.Connection,
    session: Session,
    params: dict[str, Any],
    *,
    subtypes: Optional[Sequence[str]],
    record_ids: Optional[Sequence[str]],
    columns: str,
    child_columns: str,
    unit_terms: Optional[str] = None,
    positions: bool = False,
) -> str:
    """The text units of records, each with its record.

    Own units: the record's own `attrs.text`. Passage units: a declared
    passage of the record (`_declarations`). `n` is the unit, `p` the
    record of a passage unit. `unit_terms` (SQL on the unit's text,
    `line_term_sql`) keeps only the units that hold it, so the trigram
    index narrows the read as it does for grep. With `positions`, each
    unit also carries `pos_attrs`: the stored attributes of the passage's
    `member_of` edge to its record (its span in the record's source), NULL
    for an own unit or a passage with no such edge."""
    segment = resolve_trgm_segment(conn)
    params.setdefault("bp", branch_point(conn, session))
    position_join = ""
    if positions:
        columns += ", NULL::jsonb AS pos_attrs"
        child_columns += ", pos.attrs AS pos_attrs"
        position_join = (
            f" LEFT JOIN LATERAL (SELECT pe.attrs FROM {resolve_edge_segment(conn)} pe "
            "WHERE pe.src = n.node_id AND pe.dst = p.node_id "
            f"AND pe.edge_type = 'member_of' AND {_LIVE.format(a='pe')} "
            "ORDER BY pe.edge_id LIMIT 1) pos ON TRUE"
        )
    own_where = [_LIVE.format(a="n"), "n.attrs ? 'text'"]
    if unit_terms:
        own_where.append(unit_terms)
    if subtypes is not None:
        params["tm_types"] = list(subtypes)
        own_where.append("n.subtype = ANY(%(tm_types)s)")
    if record_ids is not None:
        params["tm_ids"] = list(record_ids)
        own_where.append("n.node_id = ANY(%(tm_ids)s)")
    parts = [f"SELECT {columns} FROM {segment} n WHERE {' AND '.join(own_where)}"]
    declared = [
        d for d in _declarations(conn, session)
        if subtypes is None or d[0] in subtypes
    ]
    if declared:
        edge_segment = resolve_edge_segment(conn)
        values = []
        for index, (parent, edge, child) in enumerate(declared):
            params[f"tmp{index}"], params[f"tme{index}"], params[f"tmc{index}"] = (
                parent, edge, child,
            )
            values.append(f"(%(tmp{index})s::text, %(tme{index})s::text, %(tmc{index})s::text)")
        child_where = [
            _LIVE.format(a="n"), "n.attrs ? 'text'",
            _LIVE.format(a="e"), _LIVE.format(a="p"),
        ]
        if unit_terms:
            child_where.append(unit_terms)
        if record_ids is not None:
            child_where.append("e.src = ANY(%(tm_ids)s)")
        parts.append(
            f"SELECT {child_columns} FROM {segment} n "
            f"JOIN {edge_segment} e ON e.dst = n.node_id "
            f"JOIN {segment} p ON p.node_id = e.src "
            f"JOIN (VALUES {', '.join(values)}) AS d(parent, edge, child) "
            "ON d.parent = p.subtype AND d.edge = e.edge_type AND d.child = n.subtype"
            f"{position_join} WHERE {' AND '.join(child_where)}"
        )
    return " UNION ALL ".join(f"({part})" for part in parts)


def _span_sql(key: str) -> str:
    """A unit's stored span bound `key` (`char_offset`, `char_end`) when it
    is a whole number, else NULL."""
    value = f"(u.pos_attrs->>'{key}')::numeric"
    return (
        f"CASE WHEN jsonb_typeof(u.pos_attrs->'{key}') = 'number' "
        f"AND {value} = trunc({value}) AND {value} >= 0 THEN {value} END"
    )


def _with_heads(units: str) -> str:
    """Units (`_units_sql` with `positions`) with `head`: how many leading
    characters of a passage's text repeat the previous passage's tail
    (issue #3802).

    A window chunker repeats the tail of one chunk at the head of the
    next. The repeat counts only when it is proven, by the rule `inspect`
    text trims by (`record_text.trim_overlap`, `record_windows.
    repeated_head`): both passages carry exact stored spans (`char_end -
    char_offset` is the length of the text), the spans increase and
    overlap, and the previous text really ends with the characters the
    overlap names. Anything else is 0, and the text is read whole. A match
    wholly inside the head is the previous passage's match, counted there."""
    exact = (
        f"CASE WHEN {_span_sql('char_end')} - {_span_sql('char_offset')} "
        f"= char_length(u.t) THEN {_span_sql('char_offset')} END"
    )
    # Only the passages with an exact span are paired, in a narrow window of
    # their own; every other unit is read once and joined to no head.
    proven = (
        "p.poff < p.off AND p.off < p.pend AND p.pend - p.off <= char_length(p.t) "
        "AND right(p.pt, (p.pend - p.off)::int) = left(p.t, (p.pend - p.off)::int)"
    )
    # `prev_t` and `prev_shift` (this passage's start less the previous
    # one's) let a head match be checked against the previous passage's own
    # matches (`_taken_back_sql`).
    return (
        f"WITH s AS MATERIALIZED (SELECT u.*, {exact} AS off FROM ({units}) u), "
        "h AS (SELECT DISTINCT ON (p.record_id, p.unit_id) p.record_id, p.unit_id, "
        f"CASE WHEN {proven} THEN (p.pend - p.off)::int ELSE 0 END AS head, "
        f"CASE WHEN {proven} THEN p.pt END AS prev_t, "
        f"CASE WHEN {proven} THEN (p.off - p.poff)::int END AS prev_shift FROM ("
        "SELECT s.record_id, s.unit_id, s.off, s.t, lag(s.off) OVER w AS poff, "
        "lag(s.off + char_length(s.t)) OVER w AS pend, lag(s.t) OVER w AS pt "
        "FROM s WHERE s.off IS NOT NULL "
        # Ties by id in byte order, as Python orders the windows view's.
        'WINDOW w AS (PARTITION BY s.record_id ORDER BY s.off, s.unit_id COLLATE "C")) p '
        f"ORDER BY p.record_id, p.unit_id, CASE WHEN {proven} THEN 0 ELSE 1 END), "
        "rh AS (SELECT h.record_id, max(h.head) AS record_head FROM h GROUP BY h.record_id) "
        "SELECT s.*, coalesce(h.head, 0) AS head, h.prev_t, h.prev_shift, "
        "coalesce(rh.record_head, 0) AS record_head "
        "FROM s LEFT JOIN h ON h.record_id = s.record_id AND h.unit_id = s.unit_id "
        "LEFT JOIN rh ON rh.record_id = s.record_id"
    )


def _units_with_heads(
    conn: psycopg.Connection,
    session: Session,
    params: dict[str, Any],
    *,
    record_ids: Sequence[str],
    columns: str,
    child_columns: str,
) -> str:
    """The records' units with `head` and `record_head` (`_with_heads`).

    When no record holds a passage with a stored `member_of` span (tickets
    whose comments carry no positions, say), no head can be proven: the
    units are read as before, every head 0, and nothing is paired."""
    if not _any_positioned(conn, session, record_ids):
        units = _units_sql(
            conn, session, params, subtypes=None, record_ids=record_ids,
            columns=columns, child_columns=child_columns,
        )
        return (
            "SELECT u.*, 0 AS head, NULL::text AS prev_t, NULL::int AS prev_shift, "
            f"0 AS record_head FROM ({units}) u"
        )
    units = _units_sql(
        conn, session, params, subtypes=None, record_ids=record_ids,
        columns=columns, child_columns=child_columns, positions=True,
    )
    return _with_heads(units)


def _any_positioned(
    conn: psycopg.Connection, session: Session, record_ids: Sequence[str],
) -> bool:
    """Whether a live `member_of` edge into one of the records stores a
    numeric `char_offset` and `char_end`."""
    if not record_ids:
        return False
    params = {
        "bp": branch_point(conn, session), "tm_pos_ids": list(record_ids),
    }
    sql = (
        f"SELECT EXISTS (SELECT 1 FROM {resolve_edge_segment(conn)} pe "
        "WHERE pe.dst = ANY(%(tm_pos_ids)s) AND pe.edge_type = 'member_of' "
        f"AND {_LIVE.format(a='pe')} "
        "AND jsonb_typeof(pe.attrs->'char_offset') = 'number' "
        "AND jsonb_typeof(pe.attrs->'char_end') = 'number') AS found"
    )
    return bool(_bounded(conn, sql, params)[0]["found"])


def _taken_back_sql(key: str, *, group: bool = False) -> str:
    """A query of `(start, txt)`: the non-empty matches of the pattern
    `%(<key>)s` in a unit `u` (1-based start) that its previous passage
    holds too, at the same place in the record and with the same text.

    Only a match inside the repeated head can be one. Matches are found
    left to right and do not overlap, so those are the first; the matches
    of the head alone, plus one, bound how many are tried (one may run on
    past the head, so each is checked on the whole text; an ordinal past
    the last match gives `regexp_instr` 0, which the non-empty test
    drops). The previous passage's matches that can lie in its tail are
    its last ones, as many as that bound. A head match that the previous
    passage does not read the same way (a line-start anchor that is
    mid-line there, a match the previous passage's scan split otherwise)
    is not taken back: it is this passage's own.

    With `group` (a pattern with one capture group, PACT
    read-aggregate-variants-3666), `txt` is the group's text of the match
    taken back; the match is still compared whole."""
    pattern = f"%({key})s"
    end = f"regexp_instr(u.t, {pattern}, 1, g, 1, 'in')"
    start = f"regexp_instr(u.t, {pattern}, 1, g, 0, 'in')"
    shown = (
        f"regexp_substr(u.t, {pattern}, 1, g, 'in', 1) AS gtxt"
        if group else "NULL::text AS gtxt"
    )
    return (
        f"SELECT hm.start, {'hm.gtxt' if group else 'hm.txt'} AS txt FROM (SELECT "
        f"CASE WHEN u.head > 0 THEN regexp_count(left(u.t, u.head), {pattern}, 1, 'in') + 1 "
        "ELSE 0 END AS k, "
        f"CASE WHEN u.head > 0 THEN regexp_count(u.prev_t, {pattern}, 1, 'in') ELSE 0 END AS cp"
        ") b CROSS JOIN LATERAL (SELECT ARRAY("
        f"SELECT regexp_instr(u.prev_t, {pattern}, 1, j, 0, 'in')::text || ':' || "
        f"regexp_substr(u.prev_t, {pattern}, 1, j, 'in') "
        "FROM generate_series(greatest(1, b.cp - b.k + 1), b.cp) AS j) AS tails) tl "
        f"CROSS JOIN LATERAL (SELECT {start} AS start, "
        f"regexp_substr(u.t, {pattern}, 1, g, 'in') AS txt, {shown} "
        f"FROM generate_series(1, b.k) AS g WHERE {end} - 1 <= u.head "
        f"AND {end} > {start}) hm "
        "WHERE (hm.start + u.prev_shift)::text || ':' || hm.txt = ANY(tl.tails)"
    )


#: PostgreSQL's embedded option for newline-sensitive matching: `^` and `$`
#: match at every line boundary and `.` and a bracket complement do not
#: cross a line feed, as grep reads a text line by line. The functions take
#: the same option as the `n` flag.
_LINES = "(?n)"


def line_term_sql(parsed: GrepQuery, params: dict[str, Any], *, column: str) -> list[str]:
    """grep's term SQL (`grep_search._term_sql`), each pattern matched
    newline-sensitively (`_LINES`): one clause per term, an exclusion as
    NOT, a NEAR term with each operand ANDed first so the index narrows it."""
    clauses = []
    for index, term in enumerate(parsed.terms):
        key = f"tml{index}"
        params[key] = _LINES + term.pg_regex
        parts = []
        for at, operand in enumerate(term.operands):
            params[f"{key}o{at}"] = _LINES + operand
            parts.append(f"{column} ~* %({key}o{at})s")
        clause = " AND ".join([*parts, f"{column} ~* %({key})s"])
        clauses.append(f"NOT ({clause})" if term.negated else f"({clause})")
    return clauses


def required_literal(pattern: str) -> str:
    """The longest run of literal letters and digits every match of
    `pattern` contains ("" when it has none).

    The same walk as grep's index rule (`grep_query.literal_run`), keeping
    the run's text: a literal inside a group or a repeat of at least one
    counts, a branch does not (no one run is in every branch). A run of
    letters and digits holds no line feed, so whichever of a record's texts
    holds a match holds the run whole, whatever else the pattern can match
    (`.`, `\\u000a`, a range such as `[\\t-\\r]`)."""
    if any(mark in pattern for mark in ("[:", "[.", "[=")):
        # A POSIX bracket expression (`[[:space:]]`, `[[.x.]]`): Python's
        # parser reads it differently from PostgreSQL's, so a run found here
        # might not be required there. No narrowing; the set path decides.
        return ""
    _silence_nested_set_warnings()

    def walk(items: Any) -> str:
        best = cur = ""
        for op, av in items:
            if op is _C.LITERAL and chr(av).isalnum():
                cur += chr(av)
            else:
                cur = ""
                if op in (_C.MAX_REPEAT, _C.MIN_REPEAT) and av[0] >= 1:
                    inner = walk(av[2])
                    best = inner if len(inner) > len(best) else best
                elif op is _C.SUBPATTERN:
                    inner = walk(av[3])
                    best = inner if len(inner) > len(best) else best
            best = cur if len(cur) > len(best) else best
        return best

    return walk(_P.parse(pattern).data)


def narrowing_query(parsed: GrepQuery) -> Optional[GrepQuery]:
    """One literal every matching record holds inside one of its texts, for
    the trigram index to find candidates with; None when the value has none
    of 3 or more letters or digits.

    The value is tested over a record's texts joined by line feeds, so a
    phrase, a NEAR pair or a regex may be met only across two texts and
    cannot narrow as written. A word of a positive word, phrase or NEAR
    term, and a regex's required literal run (`required_literal`), hold no
    line feed, so whichever text holds the match holds them whole. The
    longest narrows best."""
    choices: list[GrepTerm] = []
    for term in parsed.positive:
        words = (required_literal(term.pg_regex),) if term.kind == "regex" else term.words
        for word in words:
            if not word:
                continue
            literal = _literal_term(word, kind="word", negated=False, query=word)
            if literal.indexable:
                choices.append(literal)
    if not choices:
        return None
    return GrepQuery((max(choices, key=lambda t: t.run),))


#: Literals one index read tries, longest first (PACT read-aggregate-variants-3666).
NARROWING_TERMS = 3
#: The most one literal's index read may take when another literal, or the
#: rest of the condition, can be read instead; the same slice grep's
#: whole-record read gives a term (`grep_record.INDEX_TERM_MS`).
NARROWING_TERM_MS = 900


def narrowing_queries(parsed: GrepQuery) -> list[GrepQuery]:
    """Every distinct literal `narrowing_query` may choose from, longest
    first (the first is its choice), at most `NARROWING_TERMS`."""
    choices: dict[str, GrepTerm] = {}
    for term in parsed.positive:
        words = (required_literal(term.pg_regex),) if term.kind == "regex" else term.words
        for word in words:
            if not word:
                continue
            literal = _literal_term(word, kind="word", negated=False, query=word)
            if literal.indexable:
                choices.setdefault(literal.pg_regex.lower(), literal)
    ordered = sorted(choices.values(), key=lambda t: -t.run)
    return [GrepQuery((t,)) for t in ordered[:NARROWING_TERMS]]


def matching_record_ids(
    conn: psycopg.Connection,
    session: Session,
    condition: TextCondition,
    *,
    tool: str,
    subtypes: Optional[Sequence[str]] = None,
    record_ids: Optional[Sequence[str]] = None,
    rest_given: bool = False,
    whole: bool = False,
) -> list[str]:
    """The records whose text matches, in id order, at most `SCAN_LIMIT + 1`.

    The value is tested on a record's whole text: its own text and its
    passages in reading order, joined by line feeds, so every term and
    every exclusion holds over the record, not over one piece of it.

    The candidates are `record_ids` (the set the rest of the condition
    holds) or, without them, the records one of whose texts holds the
    value's best indexed term, found through the trigram index and
    restricted to `subtypes`. More than `SCAN_LIMIT` candidates are
    returned as they are (longer than `SCAN_LIMIT`): the caller refuses or
    narrows; nothing is tested on part of them.

    The index read (PACT read-aggregate-variants-3666) first tries up to
    `NARROWING_TERMS` literals, longest first, each stopped at
    `NARROWING_TERM_MS`: a literal whose read stops there, or finds more
    than `SCAN_LIMIT` records, gives way to the next, so a common word
    cannot spend the call before a rarer one is read. When none finishes,
    the longest is read once more with the whole statement timeout (as
    before this change), unless `rest_given`: then the timeout is raised
    and the caller reads the rest of the condition instead, coming back
    with `whole` (the longest literal alone, the whole timeout) when that
    rest is over the limit. A value with one literal and no rest is read
    with the whole timeout at once."""
    _check_regexes_compile(conn, condition.parsed, condition.value)
    if record_ids is None:
        narrowings = narrowing_queries(condition.parsed)
        if not narrowings:
            raise ValueError("matching_record_ids: no narrowing term; pass record_ids")
        if whole or (len(narrowings) == 1 and not rest_given):
            plan = [(narrowings[0], False)]
        else:
            plan = [(n, True) for n in narrowings]
            if not rest_given:
                plan.append((narrowings[0], False))
        found: Optional[list[str]] = None
        for narrowing, sliced in plan:
            if not sliced and found is not None:
                # A sliced read already finished, over the limit: the
                # whole read would return the same.
                break
            params: dict[str, Any] = {"tm_cap": SCAN_LIMIT + 1}
            narrow = " AND ".join(line_term_sql(narrowing, params, column=_TEXT))
            units = _units_sql(
                conn, session, params, subtypes=subtypes, record_ids=None,
                columns="n.node_id AS record_id", child_columns="p.node_id AS record_id",
                unit_terms=narrow,
            )
            sql = f"SELECT DISTINCT u.record_id FROM ({units}) u ORDER BY 1 LIMIT %(tm_cap)s"
            try:
                if sliced:
                    outer = _DEADLINE.get()
                    until = time.monotonic() + NARROWING_TERM_MS / 1000
                    with deadline(until if outer is None else min(outer, until)):
                        rows = _bounded(conn, sql, params)
                else:
                    rows = _bounded(conn, sql, params)
            except _Cancelled:
                continue
            read = [str(row["record_id"]) for row in rows]
            if len(read) <= SCAN_LIMIT:
                found = read
                break
            found = found if found is not None else read
            if not sliced:
                break
        if found is None:
            raise timed_out(tool)
        candidates = found
        if len(candidates) > SCAN_LIMIT:
            return candidates
    else:
        candidates = list(record_ids)
    if not candidates:
        return []
    params = {}
    units = _units_sql(
        conn, session, params, subtypes=None, record_ids=candidates,
        columns=f"n.node_id AS record_id, TRUE AS own, {_ORDINAL_SQL} AS ord, "
                f"n.node_id AS unit_id, {_TEXT} AS t",
        child_columns=f"p.node_id AS record_id, FALSE AS own, {_ORDINAL_SQL} AS ord, "
                      f"n.node_id AS unit_id, {_TEXT} AS t",
    )
    terms = " AND ".join(line_term_sql(condition.parsed, params, column="r.t"))
    sql = (
        "SELECT r.record_id FROM (SELECT u.record_id, string_agg(u.t, E'\\n' "
        "ORDER BY u.own DESC, u.ord NULLS LAST, u.unit_id) AS t "
        f"FROM ({units}) u GROUP BY u.record_id) r WHERE {terms} ORDER BY 1"
    )
    try:
        return [str(row["record_id"]) for row in _bounded(conn, sql, params)]
    except _Cancelled as exc:
        raise timed_out(tool) from exc


def neighbour_ids(
    conn: psycopg.Connection,
    session: Session,
    node_id: str,
    *,
    edge_type: Optional[str],
    direction: str,
    subtypes: Optional[Sequence[str]] = None,
    tool: str = "expand",
) -> list[str]:
    """The records at the far end of `node_id`'s links, at most `SCAN_LIMIT + 1`.

    `subtypes` (the types the far-end condition requires, `type_scope`)
    keeps only far records of those types, so a record with many links but
    few of the asked type is checked whole."""
    near, far = ("src", "dst") if direction == "out" else ("dst", "src")
    params: dict[str, Any] = {
        "bp": branch_point(conn, session), "tm_node": node_id, "tm_cap": SCAN_LIMIT + 1,
    }
    where = [_LIVE.format(a="e"), f"e.{near} = %(tm_node)s"]
    joined = ""
    if edge_type is not None:
        params["tm_edge"] = str(edge_type)
        where.append("e.edge_type = %(tm_edge)s")
    if subtypes is not None:
        params["tm_types"] = list(subtypes)
        joined = f" JOIN {resolve_trgm_segment(conn)} n ON n.node_id = e.{far}"
        where += [_LIVE.format(a="n"), "n.subtype = ANY(%(tm_types)s)"]
    sql = (
        f"SELECT DISTINCT e.{far} AS record_id FROM {resolve_edge_segment(conn)} e"
        f"{joined} WHERE {' AND '.join(where)} ORDER BY 1 LIMIT %(tm_cap)s"
    )
    try:
        rows = _bounded(conn, sql, params)
    except _Cancelled as exc:
        raise timed_out(tool) from exc
    return [str(row["record_id"]) for row in rows]


# ---------- what each record's text holds ----------


def whole_match_regex(pattern: str) -> str:
    """`pattern` with every capturing group made non-capturing.

    `regexp_matches(text, p, 'g')` returns a pattern's groups when it has
    any, and the whole match only when it has none. grep's subset refuses
    back-references (`grep_query._walk`), so no group is referred to and the
    rewrite matches exactly what `pattern` matches."""
    return _scan_groups(pattern)[0]


def capturing_groups(pattern: str) -> int:
    """How many capturing groups `pattern` has (`(?:...)` and other `(?`
    forms are not; a `(` escaped or inside brackets is a literal)."""
    return _scan_groups(pattern)[1]


def _scan_groups(pattern: str) -> tuple[str, int]:
    """`pattern` with every capturing group made non-capturing, and how
    many there were."""
    out: list[str] = []
    groups = 0
    i, n = 0, len(pattern)
    in_class = False
    while i < n:
        c = pattern[i]
        if c == "\\" and i + 1 < n:
            out.append(pattern[i: i + 2])
            i += 2
            continue
        if in_class:
            if c == "[" and i + 1 < n and pattern[i + 1] in ":.=":
                # A POSIX element inside the brackets (`[:alpha:]`, `[.x.]`,
                # `[=e=]`) runs to its own closing `:]`, `.]` or `=]`; its
                # `]` does not end the bracket expression.
                close = pattern.find(pattern[i + 1] + "]", i + 2)
                if close >= 0:
                    out.append(pattern[i: close + 2])
                    i = close + 2
                    continue
            if c == "]":
                in_class = False
            out.append(c)
            i += 1
            continue
        if c == "[":
            in_class = True
            out.append(c)
            i += 1
            # A `]` first in a class (after an optional `^`) is a literal.
            if i < n and pattern[i] == "^":
                out.append("^")
                i += 1
            if i < n and pattern[i] == "]":
                out.append("]")
                i += 1
            continue
        if c == "(" and not pattern.startswith("(?", i):
            out.append("(?:")
            groups += 1
            i += 1
            continue
        out.append(c)
        i += 1
    return "".join(out), groups


def _span_columns(parsed: GrepQuery, params: dict[str, Any], *, column: str) -> list[str]:
    """Per positive term: the first `MATCHES_CAP` matches past the unit's
    repeated head as `[start, text]` pairs (`p<i>`; 1-based start, from
    PostgreSQL itself, so an anchored or look-alike match is placed where
    PostgreSQL found it), the number of non-empty matches (`c<i>`), and
    the 1-based starts of those the previous passage holds too (`dp<i>`,
    read from `dd`, `_dropped_columns`)."""
    params["tm_cap"] = MATCHES_CAP
    columns = []
    for index, term in enumerate(parsed.positive):
        key, whole = f"tmr{index}", f"tmw{index}"
        params[key] = term.pg_regex
        params[whole] = whole_match_regex(term.pg_regex)
        columns.append(
            f"ARRAY(SELECT ARRAY[regexp_instr({column}, %({key})s, 1, g, 0, 'in')::text, "
            f"regexp_substr({column}, %({key})s, 1, g, 'in')] FROM generate_series(1, "
            f"LEAST(%(tm_cap)s + cardinality(dd.dp{index}), "
            f"regexp_count({column}, %({key})s, 1, 'in'))) AS g) AS p{index}"
        )
        columns.append(
            f"(SELECT count(*) FROM regexp_matches({column}, %({whole})s, 'gin') AS x "
            f"WHERE x[1] <> '') AS c{index}"
        )
        columns.append(f"dd.dp{index}")
    return columns


def _dropped_columns(parsed: GrepQuery) -> str:
    """`dp<i>` per positive term: the starts of the unit's matches its
    previous passage holds too (`_taken_back_sql`), for `CROSS JOIN LATERAL
    (SELECT ...) dd`."""
    return ", ".join(
        f"ARRAY(SELECT z.start FROM ({_taken_back_sql(f'tmr{index}')}) z) AS dp{index}"
        for index in range(len(parsed.positive))
    ) or "0 AS d_none"


def _marked(alias: str) -> str:
    return (
        f"(({alias}.attrs->>'{UNTRUSTED_CONTENT_ORIGIN_ATTR}') "
        f"IS NOT DISTINCT FROM '{WEB_TOOL_RESULT_ORIGIN}')"
    )


def _ordinal(attrs: Any) -> float:
    if isinstance(attrs, Mapping):
        for key in ("chunk_ordinal", "chunk_index", "char_offset"):
            value = attrs.get(key)
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                return float(value)
    return float("inf")


def _fence(text: str, *, untrusted: bool, source: Optional[str]) -> str:
    return _fence_value(text, source=source) if untrusted else text


def record_matches(
    conn: psycopg.Connection,
    session: Session,
    condition: TextCondition,
    record_ids: Sequence[str],
    *,
    tool: str,
    with_lines: bool,
    with_units: bool = False,
) -> dict[str, RecordMatches]:
    """Per record: every match counted, `match` and the first `MATCHES_CAP`.

    `with_units` (grep's `within="record"`, PACT read-search-caps-3666):
    a passage match also names its passage's type (`unit_subtype`) and its
    stated place among the record's passages (`unit_ordinal`), so a row can
    label each line with the unit it is in.

    Each of the record's texts is read once; PostgreSQL (the engine that
    decided the match) finds each positive term's matches and where each
    starts. A record's matches come in reading order: its own text first,
    then its passages by their stated order (`chunk_ordinal`,
    `chunk_index`, `char_offset`) and id. A match is found inside one text,
    so a phrase that runs from one passage into the next is counted for the
    record but has no match to show. A match's `line` is a line of the
    record's text as `inspect` text numbers it where grep can place it
    (`_record_lines_in_time`, only with `with_lines`); otherwise the entry
    says what its line counts (`line_basis`, and `passage_id` for a
    passage). Text from a source that is not trusted is fenced.

    Where a window chunker repeated the tail of one passage at the head of
    the next (`_with_heads`), a match wholly inside that head is counted
    and listed once, in the passage before (issue #3802)."""
    if not record_ids:
        return {}
    params: dict[str, Any] = {}
    spans_sql = ", ".join(_span_columns(condition.parsed, params, column="u.t"))
    try:
        units = _units_with_heads(
            conn, session, params, record_ids=record_ids,
            columns=(
                "n.node_id AS record_id, n.node_id AS unit_id, TRUE AS own, "
                "n.subtype AS unit_subtype, n.attrs AS unit_attrs, "
                f"{_TEXT} AS t, n.source AS unit_source, n.source AS record_source, "
                f"{_marked('n')} AS marked"
            ),
            child_columns=(
                "p.node_id AS record_id, n.node_id AS unit_id, FALSE AS own, "
                "n.subtype AS unit_subtype, n.attrs AS unit_attrs, "
                f"{_TEXT} AS t, n.source AS unit_source, p.source AS record_source, "
                f"({_marked('n')} OR {_marked('p')}) AS marked"
            ),
        )
        sql = (
            "SELECT u.record_id, u.unit_id, u.own, u.unit_subtype, u.unit_attrs, u.t, "
            f"u.unit_source, u.record_source, u.marked, u.head, {spans_sql} "
            f"FROM ({units}) u "
            f"CROSS JOIN LATERAL (SELECT {_dropped_columns(condition.parsed)}) dd"
        )
        rows = _bounded(conn, sql, params)
    except _Cancelled as exc:
        raise timed_out(tool) from exc
    trust = classify_sources(
        conn, deployment_name=session.deployment_name,
        sources={src for row in rows for src in (row["unit_source"], row["record_source"])},
    )
    rows.sort(key=lambda r: (
        str(r["record_id"]), not r["own"], _ordinal(r["unit_attrs"]), str(r["unit_id"]),
    ))
    out: dict[str, RecordMatches] = {}
    places: list[tuple[dict[str, Any], MatchPlace]] = []
    positive = condition.parsed.positive
    for row in rows:
        record_id = str(row["record_id"])
        result = out.setdefault(record_id, RecordMatches())
        text = str(row["t"] or "")
        untrusted = bool(row["marked"]) or "untrusted" in (
            trust.get(row["unit_source"], "untrusted"),
            trust.get(row["record_source"], "untrusted"),
        )
        source = row["unit_source"] if row["unit_source"] is not None else row["record_source"]
        spans: list[tuple[int, str]] = []
        # Issue #3802: a match inside the head a window chunker repeated
        # that the passage before holds at the same place is its match.
        for index in range(len(positive)):
            repeated = {int(at) for at in row[f"dp{index}"] or []}
            result.count += int(row[f"c{index}"] or 0) - len(repeated)
            for start, matched in row[f"p{index}"] or []:
                if matched and start and int(start) > 0:
                    if int(start) in repeated:
                        continue
                    spans.append((int(start) - 1, str(matched)))
        spans.sort(key=lambda span: span[0])
        for at, matched in spans:
            served = _fence(matched, untrusted=untrusted, source=source)
            if result.first is None:
                result.first = served
            if len(result.entries) >= MATCHES_CAP:
                break
            entry: dict[str, Any] = {"line": _line_of(text, at), "text": served}
            if with_units:
                # The matched line itself, around the match, as grep's
                # windows cut it.
                entry["line_text"] = _fence(
                    window_text(text, at, at + len(matched)), untrusted=untrusted, source=source,
                )
            if not row["own"]:
                entry["passage_id"] = str(row["unit_id"])
                entry["line_basis"] = MATCH_LINE_IN_PASSAGE
                if with_units:
                    entry["unit_subtype"] = row["unit_subtype"]
                    place = _ordinal(row["unit_attrs"])
                    if place != float("inf"):
                        entry["unit_ordinal"] = int(place)
                if with_lines and row["unit_subtype"] == "document_chunk":
                    places.append((entry, MatchPlace(
                        record_id, str(row["unit_id"]), row["unit_subtype"], text, at,
                    )))
            elif row["unit_subtype"] != "document_chunk":
                # The record's own `text` attribute, which `inspect` text does
                # not read (grep says the same of its own-text windows).
                entry["line_basis"] = MATCH_LINE_IN_OWN_TEXT
            result.entries.append(entry)
    if places:
        lines = _record_lines_in_time(conn, session, [place for _entry, place in places])
        for (entry, _place), line in zip(places, lines):
            if line is not None:
                entry["line"] = line
                entry.pop("line_basis", None)
    return out


@dataclass(frozen=True)
class MatchGroup:
    """One distinct matched text over a set: records holding it, and matches."""

    text: str
    records: int
    occurrences: int
    #: The texts as matched that a merged group holds, most occurrences
    #: first, each with its occurrences (`match_keys.variants_text`, fenced
    #: as one text when the group's source is not trusted); empty when
    #: grouped as written.
    variants: str = ""
    #: The key unfenced, for comparing groups; never printed.
    raw: str = ""
    #: Whether `text` is fenced (its source is not trusted).
    fenced: bool = False


#: A declared passage this long or longer whose whole text is inside its
#: record's own text repeats that text: aggregate counts its matches once,
#: in the own text (PACT read-aggregate-variants-3666).
REPEATED_PASSAGE_MIN_CHARS = 200


def capture_terms(parsed: GrepQuery, *, tool: str = "aggregate") -> Optional[tuple[int, ...]]:
    """The positive terms whose capture group is counted, by index; None
    when no regex term has a capture group (every term counts its whole
    matches, as before).

    A regex with one capture group counts the group's text. A regex with
    more is refused: which group is the value cannot be told."""
    counted = []
    for index, term in enumerate(parsed.positive):
        if term.kind != "regex":
            continue
        groups = capturing_groups(term.pg_regex)
        if groups > 1:
            raise _refuse(
                f"the regex {term.text!r} has {groups} capture groups; "
                "one capture group picks the text to count",
                fix_hint=(
                    "keep one (...) around the part to count and write the "
                    "others (?:...), e.g. re:/(?:Status|State): ([A-Za-z ]+)/"
                ),
                tool=tool, reason="several_capture_groups", groups=groups,
            )
        if groups == 1:
            counted.append(index)
    return tuple(counted) if counted else None


def count_matches(
    conn: psycopg.Connection,
    session: Session,
    condition: TextCondition,
    record_ids: Sequence[str],
    *,
    tool: str,
    key: str = "match",
) -> tuple[list[MatchGroup], int, dict[str, Any]]:
    """Every distinct matched text over the records, exactly, largest first,
    how many of the records hold at least one matched text, and facts for
    the reply.

    `records` is how many of the records hold that text (the `count` of an
    aggregate group); `occurrences` how many times it occurs in them. With
    `key` "match" the text is grouped as written (letter case kept), like
    `grep -o | sort | uniq -c`; "norm(match)" and "words(match)" merge the
    variants of one value (`match_keys.key_sql`) and list them per group.
    Each term counts its own matches, so a spot two terms match counts once
    for each; when a regex term has a capture group (`capture_terms`), only
    the capture groups are counted, and the other terms only choose the
    records. A record admitted by a phrase that runs across two of its
    passages holds no matched text. A match inside the head a window
    chunker repeated from the passage before counts once, there (issue
    #3802). A passage of at least `REPEATED_PASSAGE_MIN_CHARS` whose whole
    text is inside its record's own text is not read again (facts
    `repeated_passages`). Text from a source that is not trusted is fenced."""
    from okg.substrate.mcp.match_keys import MATCH_KEY, key_sql, variants_text

    captured = capture_terms(condition.parsed, tool=tool)
    facts: dict[str, Any] = {"capture_group": True} if captured is not None else {}
    if not record_ids:
        return [], 0, facts
    params: dict[str, Any] = {"tm_rep_min": REPEATED_PASSAGE_MIN_CHARS}
    branches = []
    for index, term in enumerate(condition.parsed.positive):
        if captured is not None and index not in captured:
            continue
        group = captured is not None
        key_name, whole = f"tmr{index}", f"tmw{index}"
        params[key_name] = term.pg_regex
        # With one capture group, `regexp_matches` returns the group's text.
        params[whole] = term.pg_regex if group else whole_match_regex(term.pg_regex)
        branches.append(
            f"SELECT x[1] AS txt, 1 AS n FROM regexp_matches(u.t, %({whole})s, 'gin') AS x"
        )
        # Issue #3802: a match inside the head a window chunker repeated,
        # held at the same place by the passage before, is that passage's
        # match: taken back here.
        branches.append(
            f"SELECT z.txt, -1 AS n FROM ({_taken_back_sql(key_name, group=group)}) z"
        )
    grouped = key_sql(key, "m.txt")
    merged = key != MATCH_KEY
    try:
        units = _units_with_heads(
            conn, session, params, record_ids=record_ids,
            columns=(
                f"n.node_id AS record_id, n.node_id AS unit_id, {_TEXT} AS t, "
                f"n.source AS unit_source, n.source AS record_source, "
                f"{_marked('n')} AS marked"
            ),
            child_columns=(
                f"p.node_id AS record_id, n.node_id AS unit_id, {_TEXT} AS t, "
                "n.source AS unit_source, p.source AS record_source, "
                f"({_marked('n')} OR {_marked('p')}) AS marked"
            ),
        )
        # Each match once (`x`). A record with a repeated head (`headed`)
        # holds a text only while a match of it is left after the repeats
        # are taken back (`net`). A repeat is taken back only when the
        # passage before holds the same match, so this is a guard: no text
        # is left counted for a record with none of it. Every other record
        # holds each text a match was found for, in one pass as before.
        # `holding` counts the records holding any text; one statement
        # serves both. `x.txt` is the group key, `x.raw` the text as
        # matched (one and the same for "match"). A passage repeating its
        # record's own text (`rep`) is left out before any match is read.
        variants = (
            ", v AS (SELECT x.txt, x.raw, sum(x.n) AS occ FROM x "
            "GROUP BY x.txt, x.raw HAVING sum(x.n) > 0), "
            "vv AS (SELECT v.txt, array_agg(v.raw ORDER BY v.occ DESC, v.raw) AS raws, "
            "array_agg(v.occ ORDER BY v.occ DESC, v.raw) AS occs FROM v GROUP BY v.txt) "
            if merged else " "
        )
        sql = (
            f"WITH uu AS MATERIALIZED ({units}), "
            "rep AS (SELECT p.record_id, p.unit_id FROM uu p JOIN uu o "
            "ON o.record_id = p.record_id AND o.unit_id = o.record_id "
            "WHERE p.unit_id <> p.record_id AND char_length(p.t) >= %(tm_rep_min)s "
            "AND strpos(o.t, p.t) > 0), "
            "u AS (SELECT uu.* FROM uu WHERE NOT EXISTS (SELECT 1 FROM rep "
            "WHERE rep.record_id = uu.record_id AND rep.unit_id = uu.unit_id)), "
            f"x AS MATERIALIZED (SELECT {grouped} AS txt, m.txt AS raw, m.n, u.record_id, "
            "u.unit_source, u.record_source, u.marked, u.record_head > 0 AS headed "
            f"FROM u CROSS JOIN LATERAL ({' UNION ALL '.join(branches)}) m "
            f"WHERE m.txt <> '' AND {grouped} <> ''), "
            "net AS (SELECT x.txt, x.record_id FROM x WHERE x.headed "
            "GROUP BY x.txt, x.record_id HAVING sum(x.n) > 0), "
            "held AS (SELECT x.txt, x.record_id FROM x WHERE NOT x.headed "
            f"UNION SELECT net.txt, net.record_id FROM net){variants}"
            "SELECT g.txt, r.records, g.occurrences, g.sources, g.marked, "
            f"{'vv.raws, vv.occs' if merged else 'NULL::text[] AS raws, NULL::bigint[] AS occs'}, "
            "(SELECT count(DISTINCT held.record_id) FROM held) AS holding, "
            "(SELECT count(*) FROM rep) AS repeated "
            "FROM (SELECT x.txt, sum(x.n) AS occurrences, "
            "array_agg(DISTINCT x.unit_source) || array_agg(DISTINCT x.record_source) "
            "AS sources, bool_or(x.marked) AS marked FROM x GROUP BY x.txt) g "
            "JOIN (SELECT held.txt, count(*) AS records FROM held GROUP BY held.txt) r "
            "ON r.txt = g.txt"
            + (" LEFT JOIN vv ON vv.txt = g.txt" if merged else "")
        )
        rows = _bounded(conn, sql, params)
        if not rows:
            # No group: the passages left out are still said.
            rows_rep = _bounded(conn, (
                f"WITH uu AS MATERIALIZED ({units}) SELECT count(*) AS repeated "
                "FROM uu p JOIN uu o ON o.record_id = p.record_id "
                "AND o.unit_id = o.record_id WHERE p.unit_id <> p.record_id "
                "AND char_length(p.t) >= %(tm_rep_min)s AND strpos(o.t, p.t) > 0"
            ), params)
            repeated = int(rows_rep[0]["repeated"] or 0) if rows_rep else 0
        else:
            repeated = int(rows[0]["repeated"] or 0)
    except _Cancelled as exc:
        raise timed_out(tool) from exc
    if repeated:
        facts["repeated_passages"] = repeated
    holding = rows[0]["holding"] if rows else 0
    trust = classify_sources(
        conn, deployment_name=session.deployment_name,
        sources={src for row in rows for src in (row["sources"] or [])},
    )
    groups = []
    for row in rows:
        sources = [s for s in (row["sources"] or []) if s is not None]
        untrusted_sources = [s for s in sources if trust.get(s, "untrusted") != "trusted"]
        untrusted = bool(row["marked"]) or bool(untrusted_sources) or len(sources) < len(
            row["sources"] or [],
        )
        source = (untrusted_sources or sources or [None])[0]
        groups.append(MatchGroup(
            text=_fence(str(row["txt"]), untrusted=untrusted, source=source),
            records=int(row["records"]),
            occurrences=int(row["occurrences"]),
            variants=_fence(variants_text([
                (str(raw), int(occ))
                for raw, occ in zip(row["raws"] or [], row["occs"] or [])
            ]), untrusted=untrusted, source=source) if merged else "",
            raw=str(row["txt"]), fenced=untrusted,
        ))
    groups.sort(key=lambda g: (-g.records, -g.occurrences, g.text))
    return groups, int(holding or 0), facts
