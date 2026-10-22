"""Date-period group keys for ``aggregate`` (issue #3658).

``aggregate(by=["month(date.created)"])`` counts records per calendar period
of a date. A period field is ``<period>(<date field>)``: ``<period>`` is one of
:data:`PERIODS`, and the date field is named as ``search(where=...)`` names
dates (docs/reference/mcp-results.md, "Search predicates and named dates"):
``date.created``, ``date.modified``, ``date.published``, ``date.resolved`` or
``date.effective`` read the attribute each record's type declares for that
meaning; ``date.attrs.<key>`` reads one attribute on every record.

Counting stays exact: SQL counts the selected records per distinct raw date
value (with the other group fields), and :func:`fold_period_groups` folds
those counts into periods, reading each raw value with the one date parser
handles and ``where`` use (``handle_fields._parse_date``). Nothing is
re-implemented in SQL, so a value is a date here exactly when it is a date
to ``search``.

Calendar rule (the one rule of ``date_conditions``, issue #3666): a date is
an instant and is counted in the UTC calendar. A value with no offset is
read as UTC, so it is counted on the day it was written, and the reply says
so (``dates_read_as_utc``). A day in a zone is its first instant. One field
may hold values with and without an offset. The group key is the period's
first day, ``YYYY-MM-DD``; weeks start on Monday (ISO 8601). Records with no
value (missing, JSON null, empty, or a type that declares no such date) form
one ``None`` group. Values that are present but are not dates are not
grouped: they are counted and shown in the reply (``date_unparseable``), and
the call is refused only when no selected value is a date.

A date range in ``input.filter`` on a period's own date field is applied by
the fold (:func:`split_date_filter`), so the reply can say what the range
left out (``date_filter``). It follows the one comparison rule of
``date_conditions`` (issue #3666): the date as an instant, a bound a day or
a timestamp, no time zone read as UTC. Every other date condition is
compiled with the rest of the filter and never reaches this module.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Mapping, Optional, Sequence

from okg.substrate.mcp.aggregate_each import is_each_shaped
from okg.substrate.mcp.date_conditions import (
    MAX_DATE_TEXT,
    Window,
    date_leaves,
    leaf_windows,
    parse_date_field,
    stored_instant,
)
from okg.substrate.mcp.handle_fields import (
    DATE_MEANINGS,
    MASK_TOKEN,
    _parse_date,
)
from okg.substrate.mcp.response_bounds import InvalidOperatorArguments

__all__ = [
    "DATE_MEANINGS",
    "DateRange",
    "split_date_filter",
    "MAX_DISTINCT_DATE_ROWS",
    "PERIODS",
    "PeriodField",
    "PeriodReport",
    "fold_period_groups",
    "parse_period_field",
    "period_not_declared_refusal",
    "too_many_rows_refusal",
]

#: Calendar periods a date can be truncated to, finest first.
PERIODS = ("day", "week", "month", "year")

#: How often the fold asks its caller whether to go on (rows).
DEADLINE_CHECK_EVERY = 1_000

#: Most (distinct raw date, other key) rows one call folds. Every row is
#: counted in SQL; this bounds what is carried to Python. Past it the call
#: refuses and names the narrowing fix, never a partial count.
MAX_DISTINCT_DATE_ROWS = 250_000

_PERIOD_RE = re.compile(r"^(?P<period>[A-Za-z_]+)\((?P<field>[^()]*)\)$")
_MEANINGS = ", ".join(f"date.{m}" for m in DATE_MEANINGS)
_FORM = (
    "<period>(<date>) with <period> one of day, week, month, year and "
    f"<date> one of {_MEANINGS}, or date.attrs.<key>"
)
#: The form over an input that declares no named date (an edge set).
_FORM_NO_NAMED_DATES = (
    "<period>(<date>) with <period> one of day, week, month, year and "
    "<date> date.attrs.<key>"
)


@dataclass(frozen=True)
class PeriodField:
    """One ``<period>(<date field>)`` group field.

    Exactly one of ``meaning`` (``created``...) and ``attr`` is set.
    """

    text: str
    period: str
    date_field: str
    meaning: Optional[str] = None
    attr: Optional[str] = None


def parse_period_field(text: str, *, named_dates: bool = True) -> PeriodField | None:
    """The period field ``text`` names, None for a plain field, or a refusal.

    A plain field (no parentheses) returns None and keeps exact-value
    grouping. Anything shaped like a call that is not a valid period field
    refuses by name, with the accepted form.

    ``named_dates=False`` is an input that declares no named date (an edge
    set): a refusal's example then names ``date.attrs.<key>``, never a
    named date that input would refuse in turn (#3666).
    """
    raw = str(text)
    fallback = "date.created" if named_dates else _ATTR_DATE
    form = _FORM if named_dates else _FORM_NO_NAMED_DATES
    if raw in ("norm(match)", "words(match)"):
        # Matched text with its variants merged (`match_keys`, PACT
        # read-aggregate-variants-3666), never a date period.
        return None
    if is_each_shaped(raw):
        # A count per list element (aggregate_each), never a date period:
        # its own parser reads it, and refuses it with its own form.
        return None
    if raw.startswith("attrs.") or ("(" not in raw and ")" not in raw):
        # An attribute key may hold parentheses (`attrs.Size (MB)`); it is
        # grouped by exact value, as before.
        return None
    match = _PERIOD_RE.match(raw)
    if match is None:
        raise _refuse_form(
            raw, "it is not one call with one argument", example=f"month({fallback})",
            form=form,
        )
    period = match.group("period")
    inner = match.group("field")
    if period not in PERIODS:
        # Strict spelling: `Month(...)` is not accepted for `month(...)`.
        guess = period.lower() if period.lower() in PERIODS else "month"
        named = inner.strip()
        keep = _is_date_field(named) and (named_dates or named.startswith("date.attrs."))
        raise _refuse_form(
            raw, f"{period!r} is not a period",
            example=f"{guess}({named if keep else fallback})", form=form,
        )
    if inner.startswith("date.attrs."):
        attr = inner[len("date.attrs."):]
        if attr:
            return PeriodField(raw, period, inner, attr=attr)
    elif inner.startswith("date.") and inner[len("date."):] in DATE_MEANINGS:
        return PeriodField(raw, period, inner, meaning=inner[len("date."):])
    if inner.startswith("attrs.") and inner[len("attrs."):]:
        example = f"{period}(date.{inner})"
        reason = (
            "an attribute is named date.attrs.<key> here, as in "
            "search(where=...)"
        )
    else:
        example = f"{period}({fallback})"
        reason = f"{inner!r} is not a date field"
    raise _refuse_form(raw, reason, example=example, form=form)


def _is_date_field(inner: str) -> bool:
    if inner.startswith("date.attrs."):
        return bool(inner[len("date.attrs."):])
    return inner.startswith("date.") and inner[len("date."):] in DATE_MEANINGS


def _refuse_form(
    text: str, reason: str, *, example: str = "month(date.created)",
    form: str = _FORM,
) -> InvalidOperatorArguments:
    return InvalidOperatorArguments(
        f"aggregate: by field {text!r} is not a date period: {reason}",
        data={"field": text, "expected": form},
        fix_hint=f"write the field as {form}, e.g. by=[{example!r}]",
    )


def period_not_declared_refusal(field: PeriodField) -> InvalidOperatorArguments:
    """No selected type declares ``field``'s named date."""
    return InvalidOperatorArguments(
        f"aggregate: no selected type declares {field.date_field}",
        data={
            "field": field.text,
            "date_field": field.date_field,
            "reason": "field_not_declared",
        },
        fix_hint=(
            "select a type that declares it (inspect a type's dates), or "
            f"name the attribute itself, e.g. by=['{field.period}"
            "(date.attrs.<key>)']"
        ),
    )


@dataclass(frozen=True)
class DateRange:
    """One date-range clause of ``input.filter`` on a period's date field:
    the window of instants it keeps (``date_conditions.leaf_windows``), the
    clause as the caller wrote it, and whether a bound had no time zone."""

    op: str
    window: Window
    shown: Mapping[str, Any]
    bound_read_as_utc: bool = False

    def holds(self, instant: datetime) -> bool:
        return self.window.holds(instant)

    def disclosure(self) -> dict[str, Any]:
        return dict(self.shown)


#: The range operators the fold applies; any other clause is compiled.
_RANGE_OPS = ("between", "gt", "lt")
#: The date field an input with no named date (an edge set) can use.
_ATTR_DATE = "date.attrs.<key>"

_RANGE_EXAMPLE = (
    '{"op":"between","field":"%s","values":["2025-01-01","2025-12-31"]}'
)


def _is_date_clause(node: Any) -> bool:
    return isinstance(node, Mapping) and str(node.get("field") or "").startswith("date.")


def _date_range(clause: Mapping[str, Any]) -> DateRange:
    op = str(clause.get("op"))
    windows, assumed = leaf_windows(clause, operator="aggregate")
    shown: dict[str, Any] = {"op": op}
    if op == "between":
        shown["values"] = list(clause["values"])
    else:
        shown["value"] = clause.get("value")
    return DateRange(op, windows[0], shown, assumed)


def split_date_filter(
    filter: Any, periods: Mapping[str, PeriodField], *, named_dates: bool = True,
) -> tuple[Any, dict[str, list[DateRange]]]:
    """Take the date ranges on a period's own date out of ``input.filter``.

    ``by=["month(date.created)"]`` with ``filter={"op":"between","field":
    "date.created","values":["2025-01-01","2025-12-31"]}`` is the natural
    call for "per month in 2025". Such a clause (``between``, ``gt`` or
    ``lt`` on a date field a period in ``by`` names, standing alone or
    directly inside one top-level ``and``) is applied by the period fold,
    which then reports what the range left out. Returns ``(the filter
    without those clauses, {period text: ranges})``. Every other date
    condition stays in the filter and is compiled with it (issue #3666): a
    range with no period in ``by``, another operator, a clause under ``or``
    or ``not``. A bound that is not a date refuses with the form that
    works. Pure; never mutates ``filter``.

    ``named_dates=False`` is an input that declares no named date (an edge
    set): a clause on one, anywhere in the filter, is refused with the
    attribute form, which is a call that input accepts (#3666).
    """
    if not named_dates:
        for clause in date_leaves(filter):
            field_name = str(clause.get("field"))
            # A name that is no date field at all is refused as that.
            named = parse_date_field(field_name, operator="aggregate", named_dates=False)
            if named is not None and named.meaning is not None:
                raise InvalidOperatorArguments(
                    f"aggregate: a filter on {field_name} names a date declared "
                    "per record type; edges have none",
                    data={"field": field_name, "reason": "date_filter_named_date"},
                    fix_hint=(
                        "filter the edge attribute, " + _RANGE_EXAMPLE % _ATTR_DATE
                        + f'; to count per period add "year({_ATTR_DATE})" (or '
                        "month, week, day) to by"
                    ),
                )
    by_date_field: dict[str, list[str]] = {}
    for text, period in periods.items():
        by_date_field.setdefault(period.date_field, []).append(text)

    def folded(node: Any) -> bool:
        return (
            _is_date_clause(node)
            and node.get("op") in _RANGE_OPS
            and str(node.get("field")) in by_date_field
        )

    if folded(filter):
        clauses, rest = [filter], []
    elif isinstance(filter, Mapping) and filter.get("op") == "and" and isinstance(
        filter.get("clauses"), (list, tuple)
    ):
        clauses = [c for c in filter["clauses"] if folded(c)]
        rest = [c for c in filter["clauses"] if not folded(c)]
    else:
        return filter, {}
    if not clauses:
        return filter, {}
    ranges: dict[str, list[DateRange]] = {}
    for clause in clauses:
        parsed = _date_range(clause)
        for text in by_date_field[str(clause.get("field"))]:
            ranges.setdefault(text, []).append(parsed)
    remaining: Any = None
    if len(rest) == 1:
        remaining = rest[0]
    elif rest:
        remaining = {"op": "and", "clauses": rest}
    return remaining, ranges


@dataclass
class _FieldTally:
    instants: int = 0
    local: int = 0
    unparseable: int = 0


@dataclass
class PeriodReport:
    """What a period fold read, per period field (served in the reply)."""

    date_fields: dict[str, dict[str, str]] = field(default_factory=dict)
    unparseable: dict[str, dict[str, Any]] = field(default_factory=dict)
    #: Per period field with a date filter: the ranges applied and how many
    #: records they left out (outside the range, or with no date).
    filtered: dict[str, dict[str, Any]] = field(default_factory=dict)
    #: Date fields whose range had a bound, or compared a stored value, with
    #: no time zone: read as UTC. The caller puts them in the reply's
    #: ``dates_read_as_utc`` note beside those of its other date conditions.
    utc_bounds: list[str] = field(default_factory=list)
    utc_stored: list[str] = field(default_factory=list)

    def payload(self) -> dict[str, Any]:
        out: dict[str, Any] = {"date_fields": self.date_fields}
        if self.unparseable:
            out["date_unparseable"] = self.unparseable
        if self.filtered:
            out["date_filter"] = self.filtered
        return out


def _first_day(when: date, period: str) -> date:
    if period == "day":
        return when
    if period == "week":
        return when - timedelta(days=when.weekday())
    if period == "month":
        return when.replace(day=1)
    return when.replace(month=1, day=1)


def _classify(raw: Any, is_string: bool) -> tuple[str, Optional[date]]:
    """``("none" | "bad" | "instant" | "local", calendar day)`` of one value."""
    if raw is None:
        return "none", None
    if not is_string:
        return "bad", None
    if not str(raw).strip() or str(raw).strip() == MASK_TOKEN:
        # Empty, or masked from this reader: no value, as search reads it.
        return "none", None
    parsed = _parse_date(raw) if len(str(raw)) <= MAX_DATE_TEXT else None
    if parsed is None:
        return "bad", None
    when = parsed[0]
    if when.utcoffset() is None:
        # Read as UTC: the day as written is its UTC day.
        return "local", when.date()
    try:
        return "instant", when.astimezone(timezone.utc).date()
    except OverflowError:
        # A sentinel at the edge of the calendar (year 1 or 9999 with an
        # offset) has no UTC day: counted as not a date, never a crash.
        return "bad", None


def fold_period_groups(
    rows: Sequence[Sequence[Any]],
    *,
    by: Sequence[str],
    periods: Mapping[str, PeriodField],
    report: PeriodReport,
    ranges: Optional[Mapping[str, Sequence[DateRange]]] = None,
    check: Optional[Callable[[], None]] = None,
) -> list[tuple[tuple[Any, ...], int]]:
    """Fold SQL rows into period groups, oldest first.

    ``ranges`` (:func:`split_date_filter`) keeps only the records whose date
    is inside every range of its field, compared as an instant (a date with
    no time zone read as UTC); a record with no date is outside any range.
    What a filter left out is counted in ``report.filtered``.

    Each row holds, per ``by`` field in order, the plain group key, or for a
    period field two columns (the raw value as text, and whether the stored
    value is a JSON string), then the row count last. Refuses a field with
    no date at all. A field whose counted values include one with no time
    zone is named in ``report.utc_stored``.
    """
    tallies = {text: _FieldTally() for text in periods}
    filtered: dict[str, dict[str, Any]] = {
        text: {
            "ranges": [r.disclosure() for r in wanted],
            "compared": "instant (a date with no time zone is read as UTC)",
            "excluded_outside": 0, "excluded_no_date": 0,
        }
        for text, wanted in (ranges or {}).items()
    }
    report.filtered.update(filtered)
    for text, wanted in (ranges or {}).items():
        date_field = periods[text].date_field
        if any(r.bound_read_as_utc for r in wanted) and date_field not in report.utc_bounds:
            report.utc_bounds.append(date_field)
    counts: dict[tuple[str, ...], list[Any]] = {}
    for position, row in enumerate(rows):
        if check is not None and position % DEADLINE_CHECK_EVERY == 0:
            # A count under a time budget: this loop reads up to
            # MAX_DISTINCT_DATE_ROWS rows in Python and must end with it.
            check()
        n = int(row[-1])
        key: list[Any] = []
        col = 0
        read: dict[str, tuple[str, Any]] = {}
        days: dict[str, Optional[date]] = {}
        for text in by:
            period = periods.get(text)
            if period is None:
                key.append(row[col])
                col += 1
                continue
            raw, is_string = row[col], bool(row[col + 1])
            col += 2
            kind, day = _classify(raw, is_string)
            read[text] = (kind, raw)
            days[text] = day
            key.append(None if day is None else _first_day(day, period.period).isoformat())
        bad = [text for text, (kind, _) in read.items() if kind == "bad"]
        if bad:
            # Not grouped. Counted under each field whose value is not a
            # date (a record bad in two fields is in both counts), and not
            # read any further: it cannot make another field look mixed.
            for text in bad:
                tally = tallies[text]
                tally.unparseable += n
            continue
        left_out = False
        for text, wanted in (ranges or {}).items():
            # The same reading of the stored value as every other date
            # condition (date_conditions.stored_instant).
            instant = stored_instant(read[text][1]) if days.get(text) is not None else None
            if instant is not None:
                date_field = periods[text].date_field
                if instant[1] and date_field not in report.utc_stored:
                    report.utc_stored.append(date_field)
                if all(r.holds(instant[0]) for r in wanted):
                    continue
            left_out = True
            slot_ = filtered[text]
            slot_["excluded_no_date" if instant is None else "excluded_outside"] += n
        if left_out:
            continue
        for text, (kind, raw) in read.items():
            tally = tallies[text]
            if kind == "instant":
                tally.instants += n
            elif kind == "local":
                tally.local += n
                if periods[text].date_field not in report.utc_stored:
                    report.utc_stored.append(periods[text].date_field)
        # A plain key may be a list or object (a JSON attribute), and True
        # must not merge with 1: groups are told apart by their JSON form.
        identity = tuple(json.dumps(v, sort_keys=True, default=str) for v in key)
        slot = counts.setdefault(identity, [tuple(key), 0])
        slot[1] += n
    for text, tally in tallies.items():
        period = periods[text]
        if tally.unparseable and not (tally.instants or tally.local) and not (
            filtered.get(text, {}).get("excluded_outside")
        ):
            raise _no_dates(period, tally, report.date_fields.get(text) or {})
        if tally.unparseable:
            # The count only: a stored value is untrusted data and is not
            # put in a reply outside a record's own fenced fields (#3666).
            report.unparseable[text] = {"count": tally.unparseable}
    return sorted(
        ((key, count) for key, count in counts.values()),
        key=lambda item: tuple(
            (v is None, "" if v is None else str(v)) for v in item[0]
        ),
    )


def _no_dates(
    field_: PeriodField, tally: _FieldTally, read_from: Mapping[str, str],
) -> InvalidOperatorArguments:
    # The attribute(s) the field read: the caller's own key, or what each
    # selected type declares for the named date.
    attrs = [field_.attr] if field_.attr else sorted(set(read_from.values()))
    exact = " or ".join(f"by=['attrs.{attr}']" for attr in attrs[:3])
    return InvalidOperatorArguments(
        f"aggregate: {field_.date_field} holds no date: {tally.unparseable} "
        "selected record(s) have a value that is not a date",
        data={
            "field": field_.text,
            "date_field": field_.date_field,
            "reason": "not_a_date",
            "unparseable_count": tally.unparseable,
        },
        fix_hint=(
            "group by the exact value instead"
            + (f", {exact}" if exact else "")
            + ", or pick a date field (ISO-8601 or RFC 2822 values, such as "
            "2026-10-09T12:00:00Z)"
        ),
    )


def too_many_rows_refusal(count: int) -> InvalidOperatorArguments:
    return InvalidOperatorArguments(
        "aggregate: the date period grouping would read more than "
        f"{MAX_DISTINCT_DATE_ROWS} distinct stored dates",
        data={"limit": MAX_DISTINCT_DATE_ROWS},
        fix_hint=(
            "narrow the input to fewer records (one type, a filter on "
            "another attribute, or a result reference), or group by fewer "
            "other fields"
        ),
    )
