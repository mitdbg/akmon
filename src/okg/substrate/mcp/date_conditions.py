"""Date conditions: one comparison rule for every read (issue #3666).

``date.created`` (or ``modified``, ``published``, ``resolved``, ``effective``)
and ``date.attrs.<key>`` are condition fields in ``search(where=...)``,
``filter``, ``map``, ``aggregate`` and on the far end of ``expand``. This
module holds the rule they share and the two halves of its evaluation: the
Python reading of one stored value, and the SQL condition the set operators
compile.

The rule. A date compares as an instant. A bound is a calendar day
(``2025-01-01``) or a timestamp (``2025-01-01T00:00:00Z``). A day means its
first instant, with two exceptions: as the upper bound of ``between`` it
means its last instant, and ``eq`` (or one value of ``in``) with a day means
"on that day". A bound or a stored value with no time zone is read as UTC,
and the reply says so once (``dates_read_as_utc``). A stored value that is
not a date matches nothing; a missing, empty or masked value is no value.

Agreement between SQL and Python. The one date parser is
``handle_fields._parse_date``. SQL reads a stored string through
``okg.attr_ts_v4`` (migration 0037), and only where the string has the
strict form :data:`STRICT_FORM`. For every string of that form the two
agree, value for value:

* the form is a subset of what ``okg.attr_ts_v4`` accepts and of what
  ``_parse_date`` accepts as ISO-8601 (an extended day; optionally ``T`` or
  one space and ``HH:MM[:SS[.f]]``; optionally ``Z``, ``+HH``, ``+HHMM`` or
  ``+HH:MM`` straight after the time);
* both refuse the same strings of that form: only an impossible calendar
  date is left to refuse, because the form itself keeps the hour to 00-23,
  minutes and seconds to 00-59 (PostgreSQL would take 24:00 and a leap
  second, Python 3.12 neither), the offset to 14:59 at most (PostgreSQL
  refuses 16:00 and more, Python takes up to 23:59), the year to 1000-9999
  and the fraction to six digits (past six PostgreSQL rounds and Python
  truncates);
* both read a string with no offset as UTC (``okg.attr_ts_v4`` sets
  ``timezone = 'UTC'``; :func:`instant_of` pins UTC), a day as its first
  instant, and ``-00:00`` as UTC;
* both hold an instant to the microsecond, so "the last instant of a day"
  is 23:59:59.999999 on both sides.

Every other stored value (an RFC 2822 date, a compact or week date, a day
with an offset, an offset after a space, surrounding whitespace, seven or
more fraction digits, a lower-case ``t`` or ``z``, a strict-form string
that is an impossible date, a value that is not a string) is never decided
by SQL. The caller reads the distinct such values (:func:`odd_values_sql`),
this module reads each with ``_parse_date`` and decides it against the
windows in Python (:func:`resolve_leaf`), and the compiled condition matches
those exact strings by equality. So a value Python reads and SQL cannot is
decided by Python, never silently dropped, and a value that is no date is
counted whatever its form. Past :data:`MAX_ODD_DATE_VALUES` distinct such
values the call is refused with the field.

No stored value is ever put in a reply or a refusal by this module: a
stored string is untrusted data, and what is said about values that are
not dates is their count and the field (issue #3666).

Under run-time access rules none of that is read: a leaf marked
``strict_only`` compares strict-form strings alone. Any other stored string
was not compared, so the leaf is unknown on it (SQL NULL; None from
:func:`window_leaf_verdict`): it matches neither the condition nor its
``not``. The reply says so in a fixed note (``date_forms_compared``), so
nothing in a reply depends on a stored value the reader may not see.
"""
from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping, Optional, Sequence

from okg.substrate.mcp.handle_fields import (
    DATE_MEANINGS,
    MASK_TOKEN,
    PRECISION_DATE,
    _parse_date,
)
from okg.substrate.mcp.operator_sql import DATE_WINDOW_OP
from okg.substrate.mcp.run_steps import run_time_left_ms
from okg.substrate.mcp.response_bounds import DateCallOverBudget, InvalidOperatorArguments

__all__ = [
    "DATE_OPS",
    "DateBudget",
    "INDEX_ACCESS_RULES",
    "INDEX_NOT_DECLARED_TS",
    "INDEX_NOT_NARROWABLE",
    "ISO_ONLY_ACCESS_RULES",
    "ISO_ONLY_REASONS",
    "ISO_ONLY_REMEDIES",
    "REMEDY_NAME_A_TYPE",
    "REMEDY_NARROWER_CONDITION",
    "ISO_ONLY_SCOPE_TOO_LARGE",
    "names_a_type",
    "narrowing_predicate",
    "window_keys",
    "DATE_WINDOW_OP",
    "DateConditionReport",
    "DateField",
    "LONG_VALUE",
    "MAX_DATE_TEXT",
    "MAX_ODD_DATE_VALUES",
    "NAIVE_FORM",
    "STRICT_FORM",
    "Window",
    "date_leaf_fields",
    "date_leaves",
    "holds_date_condition",
    "instant_of",
    "leaf_windows",
    "not_declared_refusal",
    "odd_values_sql",
    "parse_date_field",
    "resolve_leaf",
    "rewrite_date_leaves",
    "scoped_subtypes",
    "stored_instant",
    "tally_odd_rows",
    "too_many_odd_values_refusal",
    "window_leaf_matches",
    "window_leaf_verdict",
    "window_leaf_sql",
    "without_date_clauses",
]

UTC = timezone.utc

#: The operators a date field takes.
DATE_OPS = ("eq", "in", "lt", "gt", "between", "exists")

#: ``DATE_WINDOW_OP`` (defined in ``operator_sql``, which imports nothing) is
#: the internal leaf a date condition is rewritten to before the set
#: operators compile it: ``{"op": DATE_WINDOW_OP, "field": "attrs.<key>",
#: "windows": [[low, low_inclusive, high, high_inclusive], ...], "also":
#: [...]}``. The public predicate op set (``operator_models.Predicate``)
#: cannot name it.

#: Most distinct stored values per date field that SQL cannot read and Python
#: decides one by one. Past it the call refuses; it never compares a part.
MAX_ODD_DATE_VALUES = 10_000

#: The stored forms SQL compares (a POSIX regular expression, bound as a
#: parameter). See the module docstring for why each limit is there.
STRICT_FORM = (
    r"^[1-9][0-9]{3}-[0-9]{2}-[0-9]{2}"
    r"([T ]([01][0-9]|2[0-3]):[0-5][0-9](:[0-5][0-9](\.[0-9]{1,6})?)?"
    r"(Z|[+-](0[0-9]|1[0-4])(:?[0-5][0-9])?)?)?$"
)
#: The strict forms with no offset: read as UTC, which the reply notes.
NAIVE_FORM = (
    r"^[1-9][0-9]{3}-[0-9]{2}-[0-9]{2}"
    r"([T ]([01][0-9]|2[0-3]):[0-5][0-9](:[0-5][0-9](\.[0-9]{1,6})?)?)?$"
)

#: Longest stored string read as a date, in characters. Every date form is
#: far shorter; a longer string (a text body named as a date) is not a date
#: on any read, so SQL counts it without fetching it.
MAX_DATE_TEXT = 128
#: What stands for such strings in the stored-value read (never served).
LONG_VALUE = f"(a text longer than {MAX_DATE_TEXT} characters)"

_STRICT_RE = re.compile(STRICT_FORM)
_MEANINGS = ", ".join(f"date.{m}" for m in DATE_MEANINGS)


@dataclass(frozen=True)
class DateField:
    """``date.<meaning>`` or ``date.attrs.<key>``; exactly one is set."""

    text: str
    meaning: Optional[str] = None
    attr: Optional[str] = None


def _range_example(field_name: str) -> str:
    return (
        '{"op":"between","field":"%s","values":["2025-01-01","2025-12-31"]}'
        % field_name
    )


def parse_date_field(
    text: Any, *, operator: str = "filter", named_dates: bool = True,
) -> DateField | None:
    """The date field ``text`` names, None for any other field, or a refusal
    for a ``date.`` name that is neither a known meaning nor an attribute.

    ``named_dates=False`` is an input that declares no named date (an edge
    set): the refusal's hint then names ``date.attrs.<key>`` alone."""
    if not isinstance(text, str) or not text.startswith("date."):
        return None
    rest = text[len("date."):]
    if rest.startswith("attrs."):
        # No stored attribute name holds NUL: refused here, by name.
        if rest[len("attrs."):] and "\x00" not in rest:
            return DateField(text, attr=rest[len("attrs."):])
    elif rest in DATE_MEANINGS:
        return DateField(text, meaning=rest)
    raise InvalidOperatorArguments(
        f"{operator}: {text!r} is not a date field",
        data={"field": text, "reason": "unknown_date_field"},
        fix_hint=(
            f"name one of {_MEANINGS}, or date.attrs.<key> for any other "
            "date attribute"
        ) if named_dates else "name the edge attribute as date.attrs.<key>",
    )


def instant_of(parsed: tuple[datetime, str]) -> tuple[datetime, bool]:
    """``(instant, read_as_utc)`` of a value ``_parse_date`` returned.

    The instant keeps the offset it was written with (two instants compare
    across offsets without leaving the calendar); a value with no offset is
    pinned to UTC and flagged. A day is its first instant.
    """
    when = parsed[0]
    if when.utcoffset() is None:
        return when.replace(tzinfo=UTC), True
    return when, False


def stored_instant(raw: Any) -> Optional[tuple[datetime, bool]]:
    """``(instant, read_as_utc)`` of one stored value, or None when it is no
    value (missing, empty, masked) or not a date."""
    if not isinstance(raw, str) or len(raw) > MAX_DATE_TEXT:
        return None
    text = raw.strip()
    if not text or text == MASK_TOKEN:
        return None
    parsed = _parse_date(text)
    return None if parsed is None else instant_of(parsed)


def _day_end(start: datetime) -> datetime:
    """The last instant of the day ``start`` opens (never past year 9999)."""
    return start.replace(hour=23, minute=59, second=59, microsecond=999999)


@dataclass(frozen=True)
class Window:
    """Instants from ``low`` to ``high``; a missing side is open. A window
    with neither side holds every date (``exists``)."""

    low: Optional[datetime] = None
    low_inclusive: bool = True
    high: Optional[datetime] = None
    high_inclusive: bool = True

    def holds(self, instant: datetime) -> bool:
        if self.low is not None and (
            instant < self.low or (instant == self.low and not self.low_inclusive)
        ):
            return False
        if self.high is not None and (
            instant > self.high or (instant == self.high and not self.high_inclusive)
        ):
            return False
        return True

    def encode(self) -> list[Any]:
        return [_utc_text(self.low), self.low_inclusive,
                _utc_text(self.high), self.high_inclusive]

    @classmethod
    def decode(cls, item: Sequence[Any]) -> "Window":
        low, low_inclusive, high, high_inclusive = item
        return cls(
            None if low is None else datetime.fromisoformat(low), bool(low_inclusive),
            None if high is None else datetime.fromisoformat(high), bool(high_inclusive),
        )


def _utc_text(when: Optional[datetime]) -> Optional[str]:
    return None if when is None else when.astimezone(UTC).isoformat()


def _bound(value: Any, field_name: str, operator: str) -> tuple[datetime, bool, bool]:
    """``(instant, is_day, read_as_utc)`` of one caller bound, or a refusal."""
    parsed = _parse_date(value)
    if parsed is not None:
        instant, assumed = instant_of(parsed)
        try:
            instant.astimezone(UTC)
        except OverflowError:
            parsed = None
    if parsed is None:
        raise _bound_refusal(value, field_name, operator)
    return instant, parsed[1] == PRECISION_DATE, assumed


def _bound_refusal(value: Any, field_name: str, operator: str) -> InvalidOperatorArguments:
    return InvalidOperatorArguments(
        f"{operator}: a bound on {field_name} is a day (2025-01-01) or a "
        f"timestamp (2025-01-01T00:00:00Z); got {value!r}",
        data={"field": field_name, "reason": "date_operand"},
        fix_hint="write the condition as " + _range_example(field_name)
        + " (both days included); a number is not read as a date",
    )


def leaf_windows(
    clause: Mapping[str, Any], *, operator: str = "filter",
) -> tuple[list[Window], bool]:
    """``(windows, a bound was read as UTC)`` for one date clause.

    A record matches when its date is inside any window. Refuses an operator
    a date does not take, a bound that is not a date, and a ``between``
    whose low bound is after its high one, each with the form that works.
    """
    field_name, op = str(clause.get("field")), clause.get("op")
    if op not in DATE_OPS:
        raise InvalidOperatorArguments(
            f"{operator}: a condition on {field_name} takes eq, in, lt, gt, "
            f"between or exists; got {op!r}",
            data={"field": field_name, "op": op, "reason": "date_op"},
            fix_hint="write the condition as " + _range_example(field_name),
        )
    if op == "exists":
        return [Window()], False
    if op in ("in", "between"):
        values = clause.get("values")
        if not isinstance(values, (list, tuple)) or not values or (
            op == "between" and len(values) != 2
        ):
            raise InvalidOperatorArguments(
                f"{operator}: {op} on {field_name} takes values"
                + (" [low, high]" if op == "between" else " (a non-empty list)"),
                data={"field": field_name, "op": op, "reason": "date_values"},
                fix_hint="write the condition as " + (
                    _range_example(field_name) if op == "between" else
                    '{"op":"in","field":"%s","values":["2025-01-01"]}' % field_name
                ),
            )
    else:
        values = [clause.get("value")]
    bounds = [_bound(value, field_name, operator) for value in values]
    assumed = any(b[2] for b in bounds)
    for (instant, is_day, _), value in zip(bounds, values):
        if is_day:
            try:
                _day_end(instant).astimezone(UTC)
            except OverflowError:
                # The last day of the calendar in a zone behind UTC.
                raise _bound_refusal(value, field_name, operator) from None

    def on(bound: tuple[datetime, bool, bool]) -> Window:
        instant, is_day, _ = bound
        return Window(instant, True, _day_end(instant) if is_day else instant, True)

    if op == "eq":
        return [on(bounds[0])], assumed
    if op == "in":
        return [on(bound) for bound in bounds], assumed
    if op == "gt":
        return [Window(low=bounds[0][0], low_inclusive=False)], assumed
    if op == "lt":
        return [Window(high=bounds[0][0], high_inclusive=False)], assumed
    low, high = bounds
    high_instant = _day_end(high[0]) if high[1] else high[0]
    if low[0] > high_instant:
        raise InvalidOperatorArguments(
            f"{operator}: between on {field_name} takes values [low, high], "
            "low first",
            data={"field": field_name, "reason": "date_bounds_reversed"},
            fix_hint="write the condition as " + _range_example(field_name),
        )
    return [Window(low[0], True, high_instant, True)], assumed


def _is_date_leaf(node: Any) -> bool:
    return isinstance(node, Mapping) and isinstance(node.get("field"), str) and (
        node["field"].startswith("date.")
    )


def holds_date_condition(node: Any) -> bool:
    """Whether a predicate tree holds a clause on a ``date.`` field."""
    if _is_date_leaf(node):
        return True
    if isinstance(node, Mapping):
        return holds_date_condition(node.get("clause")) or holds_date_condition(
            node.get("clauses")
        )
    if isinstance(node, (list, tuple)):
        return any(holds_date_condition(child) for child in node)
    return False


def date_leaf_fields(node: Any) -> list[str]:
    """The distinct ``date.`` field names a predicate tree reads, in order."""
    out: list[str] = []

    def walk(item: Any) -> None:
        if _is_date_leaf(item):
            if item["field"] not in out:
                out.append(item["field"])
            return
        if isinstance(item, Mapping):
            walk(item.get("clause"))
            walk(item.get("clauses"))
        elif isinstance(item, (list, tuple)):
            for child in item:
                walk(child)

    walk(node)
    return out


def _conjuncts(predicate: Any) -> list[Any]:
    """The conditions that must all hold, with nested ``and`` flattened: an
    input's own narrowing is composed as ``and(subtype eq ..., <predicate>)``
    and the predicate is often an ``and`` itself."""
    if isinstance(predicate, Mapping) and predicate.get("op") == "and" and isinstance(
        predicate.get("clauses"), (list, tuple)
    ):
        out: list[Any] = []
        for clause in predicate["clauses"]:
            out += _conjuncts(clause)
        return out
    return [] if predicate in (None, True, {}) else [predicate]


def without_date_clauses(predicate: Any) -> Any:
    """``predicate`` without its date conditions, as a superset.

    A top-level conjunct that holds a date condition anywhere is dropped;
    the rest is kept as written. Every record the whole predicate can admit
    passes what is left, so it bounds the records whose stored dates a call
    can compare. None when nothing is left.
    """
    rest = [c for c in _conjuncts(predicate) if not holds_date_condition(c)]
    if not rest:
        return None
    return rest[0] if len(rest) == 1 else {"op": "and", "clauses": rest}


def scoped_subtypes(predicate: Any) -> Optional[set[str]]:
    """The record types a nodes predicate is narrowed to by its top-level
    ``subtype`` ``eq``/``in`` conjuncts, or None when it names none."""
    scope: Optional[set[str]] = None
    for clause in _conjuncts(predicate):
        if not isinstance(clause, Mapping) or clause.get("field") != "subtype":
            continue
        if clause.get("op") == "eq":
            named = {clause.get("value")}
        elif clause.get("op") == "in" and isinstance(clause.get("values"), (list, tuple)):
            named = set(clause["values"])
        else:
            continue
        named = {str(v) for v in named if isinstance(v, str)}
        scope = named if scope is None else scope & named
    return scope


@dataclass
class DateConditionReport:
    """What the date conditions of one call read (served in the reply)."""

    #: Date fields with a bound that had no time zone.
    utc_bounds: list[str] = field(default_factory=list)
    #: Date fields with a stored value in scope that had no time zone.
    utc_stored: list[str] = field(default_factory=list)
    #: Per date field: records in scope whose value is present and not a
    #: date, with one such value.
    unparseable: dict[str, dict[str, Any]] = field(default_factory=dict)
    #: Why only strict ISO-8601 stored values were compared, one of
    #: :data:`ISO_ONLY_REASONS`, or None when every stored form was. Set
    #: from the deployment's mode or from the read running out of its time,
    #: never from a stored value.
    iso_only: Optional[str] = None
    #: With ``scope_too_large``: what the caller can still do, one of
    #: :data:`ISO_ONLY_REMEDIES`. Decided from the call alone.
    remedy: Optional[str] = None

    def note_bound(self, field_name: str) -> None:
        if field_name not in self.utc_bounds:
            self.utc_bounds.append(field_name)

    def note_stored(self, field_name: str) -> None:
        if field_name not in self.utc_stored:
            self.utc_stored.append(field_name)

    def payload(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        if self.utc_bounds or self.utc_stored:
            note: dict[str, Any] = {}
            if self.utc_bounds:
                note["bounds"] = sorted(set(self.utc_bounds))
            if self.utc_stored:
                note["stored"] = sorted(set(self.utc_stored))
            out["dates_read_as_utc"] = note
        if self.unparseable:
            out["date_unparseable"] = dict(sorted(self.unparseable.items()))
        if self.iso_only:
            out["date_forms_compared"] = "iso_8601_only"
            out["date_forms_reason"] = self.iso_only
            if self.remedy is not None:
                out["date_forms_remedy"] = self.remedy
        return out


def odd_values_sql(
    value_sql: str, strict_param: str, naive_param: str, long_param: str,
) -> tuple[str, str]:
    """``(select list, condition)`` reading what SQL does not decide.

    ``value_sql`` is the stored JSON value of the date field for a row (NULL
    where the row has none). SQL decides a value when it is a strict-form
    string that ``okg.attr_ts_v4`` reads. Grouped by the two selected
    columns with a count, the rows are: ``(NULL, true, n)`` for the ``n``
    records whose value SQL decides and that has no offset (read as UTC),
    and ``(text, is_string, n)`` for each distinct value SQL does not
    decide: a string in any other form, a strict-form string that is an
    impossible date (``2025-02-30``), or a value that is not a string. A
    string longer than :data:`MAX_DATE_TEXT` is never fetched: all of them
    are one row, ``(<long_param's value>, false, n)``, counted as not dates.
    """
    text = f"({value_sql} #>> '{{}}')"
    is_string = f"jsonb_typeof({value_sql}) = 'string'"
    long = f"({is_string} AND char_length({text}) > {int(MAX_DATE_TEXT)})"
    decided = (
        f"(jsonb_typeof({value_sql}) = 'string' AND {text} ~ %({strict_param})s "
        f"AND okg.attr_ts_v4({text}) IS NOT NULL)"
    )
    select = (
        f"CASE WHEN {decided} THEN NULL WHEN {long} THEN %({long_param})s "
        f"ELSE {text} END, ({is_string} AND NOT {long})"
    )
    where = (
        f"{value_sql} IS NOT NULL AND jsonb_typeof({value_sql}) <> 'null' "
        f"AND (NOT {decided} OR {text} ~ %({naive_param})s)"
    )
    return select, where


def resolve_leaf(
    windows: Sequence[Window],
    odd_rows: Iterable[Sequence[Any]],
) -> list[str]:
    """The odd stored strings (:func:`odd_values_sql` rows) inside any window,
    each read by the one date parser."""
    matching: list[str] = []
    for row in odd_rows:
        text, is_string = row[0], bool(row[1])
        if text is None or not is_string:
            continue
        read = stored_instant(text)
        if read is not None and any(w.holds(read[0]) for w in windows):
            matching.append(str(text))
    return sorted(set(matching))


def tally_odd_rows(
    field_name: str,
    odd_rows: Iterable[Sequence[Any]],
    report: Optional[DateConditionReport],
) -> bool:
    """Record what the odd rows of one field say in ``report`` (a count,
    never a stored value); return whether any of them is a date Python reads
    and SQL does not."""
    bad_count, odd_date = 0, False
    saw_naive = False
    for row in odd_rows:
        text, is_string, count = row[0], bool(row[1]), int(row[2])
        if text is None:
            saw_naive = True
            continue
        if not is_string:
            bad_count += count
            continue
        stripped = str(text).strip()
        if not stripped or stripped == MASK_TOKEN:
            continue  # no value
        read = stored_instant(text)
        if read is None:
            bad_count += count
            continue
        odd_date = True
        saw_naive = saw_naive or read[1]
    if report is not None:
        if saw_naive:
            report.note_stored(field_name)
        if bad_count:
            report.unparseable[field_name] = {"count": bad_count}
    return odd_date


def too_many_odd_values_refusal(
    field_name: str, *, operator: str, holds_dates: bool = True,
) -> InvalidOperatorArguments:
    """More distinct values than Python decides one by one. ``holds_dates``
    is whether any value read was a date in a form SQL does not read; when
    none was, the field is not a date field and the refusal says that. It
    shows no stored value."""
    data: dict[str, Any] = {
        "field": field_name, "limit": MAX_ODD_DATE_VALUES,
        "reason": "date_values_not_comparable_in_sql" if holds_dates else "not_a_date",
    }
    if not holds_dates:
        return InvalidOperatorArguments(
            f"{operator}: {field_name} holds more than {MAX_ODD_DATE_VALUES} "
            "distinct stored values that are not dates",
            data=data,
            fix_hint=(
                "name a field that holds dates; to test this one as text use "
                "its attribute, attrs.<key>, with eq, in or prefix"
            ),
        )
    return InvalidOperatorArguments(
        f"{operator}: {field_name} holds more than {MAX_ODD_DATE_VALUES} "
        "distinct stored values that are not written as ISO-8601 "
        "YYYY-MM-DD[THH:MM[:SS]][offset]; they cannot all be compared "
        "exactly in one call",
        data=data,
        fix_hint=(
            "narrow the records first (a type, another condition, or a "
            "result reference) so fewer such values are compared; values "
            "written as 2025-01-01 or 2025-01-01T00:00:00Z have no limit"
        ),
    )


def not_declared_refusal(field_name: str, *, operator: str) -> InvalidOperatorArguments:
    return InvalidOperatorArguments(
        f"{operator}: no selected type declares {field_name}",
        data={"field": field_name, "reason": "field_not_declared"},
        fix_hint=(
            "select a type that declares it (inspect a type's dates), or "
            "name the attribute itself as date.attrs.<key>"
        ),
    )


def rewrite_date_leaves(
    predicate: Any,
    *,
    attrs_for: Mapping[str, Mapping[str, list[str]]],
    also_for: Mapping[str, Mapping[int, list[str]]],
    operator: str = "filter",
    strict_only: bool = False,
) -> Any:
    """``predicate`` with every date clause as :data:`DATE_WINDOW_OP` leaves.

    ``attrs_for[field]`` maps each stored attribute the date field reads to
    the record types that read it there (``{"<key>": []}`` for
    ``date.attrs.<key>``: every record). ``also_for[field][n]`` lists the
    odd stored strings that match the ``n``-th clause on that field, in
    tree order (:func:`resolve_leaf`). A named date becomes ``or`` over its
    attributes of ``and(subtype in [...], window on attrs.<key>)``, so every
    reader of ``attrs.<key>`` fields (the SQL lowerer's read guard, the
    index narrowing, the read-row check) sees an ordinary attribute leaf.
    ``strict_only`` marks every leaf as comparing strict-form strings alone
    (run-time access rules; see the module docstring).
    """
    seen: dict[str, int] = {}

    def leaf(node: Mapping[str, Any]) -> Any:
        field_name = str(node["field"])
        index = seen.get(field_name, 0)
        seen[field_name] = index + 1
        windows, _ = leaf_windows(node, operator=operator)
        body = {
            "windows": [w.encode() for w in windows],
            "also": list(also_for.get(field_name, {}).get(index, [])),
        }
        if strict_only:
            body["also"] = []
            body["strict_only"] = True
        parts: list[Any] = []
        for attr, subtypes in sorted(attrs_for[field_name].items()):
            window = {"op": DATE_WINDOW_OP, "field": f"attrs.{attr}", **body}
            if subtypes:
                window = {"op": "and", "clauses": [
                    {"op": "in", "field": "subtype", "values": sorted(subtypes)},
                    window,
                ]}
            parts.append(window)
        if not parts:
            raise not_declared_refusal(field_name, operator=operator)
        return parts[0] if len(parts) == 1 else {"op": "or", "clauses": parts}

    def walk(node: Any) -> Any:
        if _is_date_leaf(node):
            return leaf(node)
        if isinstance(node, Mapping):
            out = dict(node)
            if "clause" in out:
                out["clause"] = walk(out["clause"])
            if isinstance(out.get("clauses"), (list, tuple)):
                out["clauses"] = [walk(child) for child in out["clauses"]]
            return out
        return node

    return walk(predicate)


def date_leaves(predicate: Any) -> list[Mapping[str, Any]]:
    """Every date clause of a predicate tree, in the order
    :func:`rewrite_date_leaves` numbers them."""
    out: list[Mapping[str, Any]] = []

    def walk(node: Any) -> None:
        if _is_date_leaf(node):
            out.append(node)
        elif isinstance(node, Mapping):
            if "clause" in node:
                walk(node["clause"])
            if isinstance(node.get("clauses"), (list, tuple)):
                for child in node["clauses"]:
                    walk(child)

    walk(predicate)
    return out


def window_leaf_verdict(value: Any, leaf: Mapping[str, Any]) -> Optional[bool]:
    """True, False or unknown (None) for one stored value against a
    :data:`DATE_WINDOW_OP` leaf, read by the one date parser.

    No value and a value that is not a date are False. On a ``strict_only``
    leaf a stored string that is not in the strict form was not compared:
    unknown, as its SQL is NULL, so ``not`` does not match it either.
    """
    if leaf.get("strict_only") and isinstance(value, str) and not _STRICT_RE.fullmatch(value):
        return None
    read = stored_instant(value)
    if read is None:
        return False
    return any(Window.decode(item).holds(read[0]) for item in leaf.get("windows") or [])


def window_leaf_matches(value: Any, leaf: Mapping[str, Any]) -> bool:
    """Whether one stored value passes a :data:`DATE_WINDOW_OP` leaf: only a
    True :func:`window_leaf_verdict` does."""
    return window_leaf_verdict(value, leaf) is True


def window_leaf_sql(
    value_sql: str, leaf: Mapping[str, Any], bind: Any,
) -> str:
    """The SQL condition of a :data:`DATE_WINDOW_OP` leaf.

    ``value_sql`` is the stored JSON value; ``bind(value)`` returns a
    placeholder for a bound parameter. A strict-form string compares through
    ``okg.attr_ts_v4``; any other string matches only when it is one of the
    leaf's ``also`` strings, which Python already decided; anything else is
    FALSE. The condition is two-valued, except on a ``strict_only`` leaf,
    where a string in any other form was not compared and is NULL. See the
    module docstring for why SQL and Python agree.
    """
    text = f"({value_sql} #>> '{{}}')"
    # The stored instant is computed once per row (okg.attr_ts_v4 is a
    # plpgsql function): a one-row subquery, fenced with OFFSET 0 so the
    # planner does not copy the call into each comparison.
    stored = "date_value.ts"
    windows = []
    for item in leaf.get("windows") or []:
        low, low_inclusive, high, high_inclusive = item
        sides = [f"{stored} IS NOT NULL"]
        if low is not None:
            sides.append(
                f"{stored} {'>=' if low_inclusive else '>'} {bind(low)}::timestamptz"
            )
        if high is not None:
            sides.append(
                f"{stored} {'<=' if high_inclusive else '<'} {bind(high)}::timestamptz"
            )
        windows.append("(" + " AND ".join(sides) + ")")
    in_window = (
        "(SELECT (" + " OR ".join(windows) + ") "
        f"FROM (SELECT okg.attr_ts_v4({text}) AS ts OFFSET 0) AS date_value)"
        if windows else "FALSE"
    )
    also = [str(v) for v in leaf.get("also") or []]
    if leaf.get("strict_only"):
        other = "CAST(NULL AS boolean)"
    else:
        other = f"{text} = ANY({bind(also)}::text[])" if also else "FALSE"
    return (
        f"CASE WHEN jsonb_typeof({value_sql}) = 'string' THEN "
        f"(CASE WHEN {text} ~ {bind(STRICT_FORM)} "
        f"THEN COALESCE({in_window}, FALSE) ELSE {other} END) "
        "ELSE FALSE END"
    )


#: Why a call compares ISO-8601 stored values alone (``date_forms_reason``
#: beside ``date_forms_compared``). The one place this vocabulary is defined.
ISO_ONLY_ACCESS_RULES = "access_rules"
ISO_ONLY_SCOPE_TOO_LARGE = "scope_too_large"
ISO_ONLY_REASONS = (ISO_ONLY_ACCESS_RULES, ISO_ONLY_SCOPE_TOO_LARGE)
#: What a caller told ``scope_too_large`` can still do (``date_forms_remedy``):
#: name a type when the call names none, else narrow by another condition.
REMEDY_NAME_A_TYPE = "name_a_type"
REMEDY_NARROWER_CONDITION = "add_a_narrower_condition"
ISO_ONLY_REMEDIES = (REMEDY_NAME_A_TYPE, REMEDY_NARROWER_CONDITION)

#: Why no declared timestamp index narrowed a date condition
#: (``data.date_index`` of the over-budget refusal), beside the reasons of
#: ``declared_attr_indexes.READINESS_REASONS`` and ``pinned_generation``.
INDEX_NOT_DECLARED_TS = "not_declared_ts"
INDEX_NOT_NARROWABLE = "not_narrowable"
INDEX_ACCESS_RULES = "access_rules"


def names_a_type(predicate: Any, kind: str) -> bool:
    """Whether a predicate narrows to record types (``subtype``) or, for an
    edge set, link types (``edge_type``) by a condition that must hold."""
    column = "subtype" if kind == "nodes" else "edge_type"
    return any(
        isinstance(clause, Mapping) and clause.get("field") == column
        and clause.get("op") in ("eq", "in")
        for clause in _conjuncts(predicate)
    )


def window_keys(predicate: Any) -> list[str]:
    """The stored attribute keys the :data:`DATE_WINDOW_OP` leaves of a
    rewritten predicate read, in tree order."""
    out: list[str] = []

    def walk(node: Any) -> None:
        if isinstance(node, Mapping):
            if node.get("op") == DATE_WINDOW_OP:
                key = str(node.get("field", ""))[len("attrs."):]
                if key and key not in out:
                    out.append(key)
                return
            walk(node.get("clause"))
            walk(node.get("clauses"))
        elif isinstance(node, (list, tuple)):
            for child in node:
                walk(child)

    walk(predicate)
    return out


def _nudged(text: str, microseconds: int) -> Optional[str]:
    try:
        return (datetime.fromisoformat(text) + timedelta(microseconds=microseconds)).isoformat()
    except (OverflowError, ValueError):
        return None


def index_clause(leaf: Mapping[str, Any]) -> Optional[dict[str, Any]]:
    """A range clause on the leaf's attribute that admits every row the
    leaf can, in the form the candidate planner narrows by, or None.

    Only a leaf with no ``also`` strings qualifies: those are stored values
    ``okg.attr_ts_v4`` does not read, which no timestamp index holds. The
    range covers every window, both ends included (a superset of an
    exclusive end); an open end is moved one microsecond outward, which is
    the resolution both sides compare at.
    """
    windows = leaf.get("windows") or []
    if leaf.get("also") or not windows:
        return None
    lows = [w[0] for w in windows]
    highs = [w[2] for w in windows]
    low = None if any(v is None for v in lows) else min(lows, key=datetime.fromisoformat)
    high = None if any(v is None for v in highs) else max(highs, key=datetime.fromisoformat)
    field_name = leaf["field"]
    if low is not None and high is not None:
        return {"op": "between", "field": field_name, "values": [low, high]}
    if low is not None:
        bound = _nudged(low, -1)
        return None if bound is None else {"op": "gt", "field": field_name, "value": bound}
    if high is not None:
        bound = _nudged(high, 1)
        return None if bound is None else {"op": "lt", "field": field_name, "value": bound}
    return None


def narrowing_predicate(predicate: Any, declared: Optional[Mapping[str, str]]) -> Any:
    """A rewritten predicate as the candidate planner should read it.

    The planner (``attr_filter_source``) narrows a read by the top-level
    ``eq``/``in``/``lt``/``gt``/``between`` clauses on declared attributes,
    through the declared expression indexes. It does not know the date
    leaf. This returns a predicate that admits every row ``predicate``
    admits and that the planner can read: nested ``and`` is flattened, and
    each date leaf that must hold, on an attribute declared ``ts``, with no
    ``also`` strings, becomes the range :func:`index_clause` gives. The
    declared ``ts`` index is on ``okg.attr_ts_v4(attrs ->> key)``, the same
    function the leaf compares through, so the range is what the index
    holds. Every other clause is kept as it is. The exact predicate still
    decides each row; this only chooses the rows read.
    """
    clauses = _conjuncts(predicate)
    if not clauses:
        return predicate
    out: list[Any] = []
    for clause in clauses:
        swapped = None
        if isinstance(clause, Mapping) and clause.get("op") == DATE_WINDOW_OP:
            key = str(clause.get("field", ""))[len("attrs."):]
            if (declared or {}).get(key) == "ts":
                swapped = index_clause(clause)
        out.append(clause if swapped is None else swapped)
    return out[0] if len(out) == 1 else {"op": "and", "clauses": out}


class DateBudget:
    """The one time budget of a call's date work (issue #3666).

    Created when the call starts its date work. Every statement that
    belongs to it runs with ``statement_timeout`` set to the time left, so
    the server cancels it; when the time is spent the call is refused by
    name (:meth:`refusal`). ``statements`` records each statement's label
    and the timeout it ran under; ``date_index`` records, per stored
    attribute of a date condition, why no index narrowed it.
    """

    def __init__(
        self, operator: str, fields: Sequence[str], *, total_ms: int, typed: bool,
        kind: str = "nodes", clock: Any = None, started_at: Optional[float] = None,
    ) -> None:
        self.operator = operator
        self.fields = list(fields)
        self.total_ms = int(total_ms)
        self.typed = bool(typed)
        self.kind = kind
        self._clock = clock or time.monotonic
        # The clock starts when the call does (``started_at``), not when
        # its first date statement is built: opening the session and the
        # checks before it are part of what the caller waits for.
        self._deadline = (
            self._clock() if started_at is None else started_at
        ) + self.total_ms / 1000.0
        self.statements: list[tuple[str, int]] = []
        self.date_index: dict[str, str] = {}
        #: Milliseconds per phase of the call, in the order first seen
        #: (:meth:`mark`). Read by one debug log line per call and by the
        #: refusal; never part of a served reply.
        self.phases: dict[str, int] = {}
        self._started = self._clock() if started_at is None else started_at
        self._marked = self._started
        #: Called once with ``(budget, outcome)`` when the call ends.
        self.report: Any = None
        self._reported = False
        #: How many times the tool body ran again for this call (the
        #: byte-budget wrapper's reruns), all inside this one budget.
        self.reruns = 0
        #: The phase in progress, set by whoever starts a statement or a
        #: loop: when the call ends inside it, its time is that phase's.
        self.doing: Optional[str] = None
        #: True while work the call can do without is running (the read of
        #: stored date forms, a second read with them): running out of the
        #: budget there does not end the call.
        self.soft = False
        #: The clock stands still while work that is not the call's date
        #: work runs (:meth:`pause`), up to an allowance per call.
        self._paused_at: Optional[float] = None
        self._allowance = 0.0
        #: Milliseconds the deadline moved out for such work.
        self.uncharged_ms = 0

    def remaining_ms(self) -> int:
        now = self._clock()
        if self._paused_at is not None:
            now -= min(now - self._paused_at, self._allowance)
        own = int((self._deadline - now) * 1000)
        # Inside a `run` step (#3666) no date statement outlives the run's
        # budget: what is left of it caps this one.
        run_left = run_time_left_ms()
        return own if run_left is None else min(own, run_left)

    def pause(self, allowance_ms: int) -> None:
        """Stop charging the budget: what runs until :meth:`resume` is
        not the call's date work (reading the registry, the index
        readiness and the plan before a page). At most ``allowance_ms`` a
        call is left uncharged; past that the clock runs again."""
        if self._paused_at is None:
            self._paused_at = self._clock()
            self._allowance = max(0, int(allowance_ms) - self.uncharged_ms) / 1000.0

    def resume(self) -> None:
        if self._paused_at is None:
            return
        given = min(self._clock() - self._paused_at, self._allowance)
        self._deadline += given
        self.uncharged_ms += round(given * 1000)
        self._paused_at = None

    def mark(self, phase: str) -> None:
        """The time since the last mark (or the call's start) was ``phase``."""
        now = self._clock()
        self.phases[phase] = self.phases.get(phase, 0) + round((now - self._marked) * 1000)
        self._marked = now

    def elapsed_ms(self) -> int:
        return round((self._clock() - self._started) * 1000)

    def finish(self, outcome: str) -> None:
        """The call ended (``served`` or ``refused``): report it once."""
        if self._reported:
            return
        self._reported = True
        # What ran since the last mark is the phase in progress, when one
        # was named; only the remainder after the last phase is `rest`.
        self.mark(self.doing or "rest")
        if self.report is not None:
            self.report(self, outcome)

    def check(self) -> None:
        """Refuse when the budget is spent. Called between statements and
        inside row loops, so work in Python ends with the budget too."""
        if self.remaining_ms() <= 0:
            raise self.refusal()

    def refusal(self) -> DateCallOverBudget:
        """The call ran out of its budget. Its own error code: the caller's
        arguments are valid. Says what to add; names no retry call, because
        none is known to be served. ``data.phases`` and ``data.elapsed_ms``
        say where the time went. While ``soft`` work is running the
        call has not ended: nothing is reported."""
        if not self.soft:
            self.finish("refused")
        named = ", ".join(self.fields) or "a date"
        column = "subtype" if self.kind == "nodes" else "edge_type"
        if not self.typed:
            fix = (
                f"name a type: add input.{column} (or a condition on {column}) "
                "so only that type's records are read"
            )
        else:
            fix = (
                "add a narrower condition that is not on a date (an attribute "
                "value, a list of ids, or a saved result as the input)"
            )
        if self.date_index:
            fix += "; no date index serves " + ", ".join(
                f"attrs.{key} ({reason})" for key, reason in sorted(self.date_index.items())
            ) + ", so every record in scope is read"
        return DateCallOverBudget(
            f"{self.operator}: the date work on {named} was cut at its "
            f"{self.total_ms} ms budget",
            data={
                "reason": "date_call_over_budget", "fields": list(self.fields),
                "budget_ms": self.total_ms, "typed": self.typed,
                "date_index": dict(sorted(self.date_index.items())),
                "elapsed_ms": self.elapsed_ms(), "phases": dict(self.phases),
                **({"uncharged_ms": self.uncharged_ms} if self.uncharged_ms else {}),
            },
            fix_hint=fix,
        )
