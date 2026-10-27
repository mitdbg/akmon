"""Computed-column expressions for ``map`` (PACT ``read-map-expressions``).

One small closed language, parsed here by a hand-written recursive-descent
parser and evaluated here by a tree walker. No input text ever reaches
Python evaluation: there is no ``eval``, no ``exec``, no ``compile`` of
Python source, no Python ``ast``, no attribute access on a value (this module
calls no ``getattr``) and no import or I/O driven by an expression. A field
is read from a row only through ``Mapping`` lookups.

This module serves no tool. ``map(compute=...)`` (PACT ``read-map-compute``)
wires it in; it imports nothing from ``server.py``.

THE INTERFACE
-------------
* :func:`compile` parses every column of one ``compute`` mapping, resolves its
  names against the input's columns (:class:`ColumnSpec`), orders the columns
  by dependency and refuses a cycle by name.
* :meth:`Program.operands` lists the input fields the columns read;
  :attr:`Program.bare_names` reports the bare names read as ``attrs.<name>``.
* :meth:`Program.evaluate` computes every column of every row, under the
  time, regex and byte budgets, and gives values, states and counts.
* :func:`to_number` is the one rule for reading a value as a number;
  :func:`store_number` is the one rule for the number a column keeps.

THE NUMBER MODEL
----------------
Integers are exact Python ``int``. Decimals are ``Decimal`` inside one
evaluation only, in a local context (34 significant digits, half to even,
``Emax=37``, traps on). Every intermediate and final number stays below
10^38 in absolute value; every decimal signal becomes a row error, never an
exception. A column keeps a whole number as ``int`` and any other number as a
``float`` holding its value rounded to 15 significant digits, so ``repr`` of
the stored float is that text and :func:`to_number` reads it back exactly. No
``Decimal`` leaves this module.

THE STATES OF ONE VALUE
-----------------------
``ok`` (a value, possibly null); ``not read`` (an input was ``unavailable``,
``denied`` or withheld: the expression is not evaluated, so ``coalesce`` can
never turn a failed call into a default); ``error:<kind>``. Nothing is ever
silently 0.

THE ROW CONTRACT A CALLER MEETS
-------------------------------
* A live typed row (``node_id``, ``subtype``, ``attrs``, ``live.fields``) is
  read as it is: ``attrs.<k>`` and ``date.attrs.<k>`` read ``attrs[k]``, and
  both take the state of ``live.fields[k]``. A flat row is its upstream
  fields by name. A groups handle's row is its printed columns by name.
* ``date.<meaning>`` takes only the meanings in ``DATE_MEANINGS``
  (``created``, ``modified``, ``published``, ``resolved``, ``effective``) and
  reads the row key ``"date.<meaning>"``. No served row carries that key
  today: the caller (PACT ``read-map-compute`` and later) must put each
  meaning's resolved value there, or the operand reads as null.
* An input ``calc.<name>`` column is read from the row key ``"calc.<name>"``.
* ``field_states``, when given, maps an operand name to its state and wins
  over the row's own ``live.fields``.
"""
from __future__ import annotations

import datetime as _dt
import decimal
import difflib
import re as _re
import time
import warnings
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Callable, Mapping, Optional, Sequence, Union

import regex as _regex

from okg.substrate.errors import SubstrateError
from okg.substrate.external_backed.live_dispatch import _backtracks
from okg.substrate.mcp.handle_fields import DATE_MEANINGS
from okg.substrate.mcp.live_calls import FIELD_STATES, RECORD_FIELDS

# -- limits (spec section 5.8) --------------------------------------------------

#: Computed columns one call may add.
MAX_COLUMNS_PER_CALL = 8
#: ``calc.`` columns one handle may carry, over chained maps.
MAX_CALC_COLUMNS = 32
#: Characters in one expression.
MAX_EXPRESSION_CHARS = 400
#: Syntax nodes in one expression. A chain of one precedence level
#: (``a + b - c``, ``a * b``, ``a and b``) is one node holding its operands.
MAX_NODES = 100
#: Nesting depth: each ``( … )`` group, each call's or metric's argument
#: list, each prefix ``-`` and each prefix ``not`` adds one.
MAX_DEPTH = 20
#: Characters in one string literal.
MAX_LITERAL_CHARS = 200
#: Rows one evaluation takes.
MAX_ROWS = 50_000
#: Characters in one computed text value (a row error past it).
MAX_TEXT_CHARS = 4_096
#: Computed bytes per call: text by UTF-8 length, numbers and booleans at
#: :data:`NUMBER_BYTES` each.
MAX_OUTPUT_BYTES = 8 * 1024 * 1024
NUMBER_BYTES = 16
#: Evaluation time per call, checked before every row.
EVAL_SECONDS = 5.0
#: One regex match; past it the row's value is ``error:regex_timeout``.
REGEX_MATCH_SECONDS = 0.020
#: Regex time per call; past it the whole call is refused.
REGEX_CALL_SECONDS = 2.0
#: UTF-8 bytes of a regex input text.
MAX_REGEX_INPUT_BYTES = 64 * 1024
#: Characters in a regex pattern.
MAX_PATTERN_CHARS = 200
#: Largest regex repeat count (``a{1000}``); the regex library expands a
#: count when it compiles, so a larger one costs time and memory at parse.
MAX_REPEAT_COUNT = 1_000
#: Characters of text read as a number.
MAX_NUMBER_TEXT = 64
#: Every number stays below this in absolute value.
NUMBER_LIMIT = 10 ** 38
#: Significant digits a stored non-whole number keeps (DBL_DIG).
STORED_DIGITS = 15
#: The smallest non-zero absolute value a column stores.
SMALLEST_STORED = Decimal("1e-38")
#: ``round(x, digits)`` takes digits in this range.
ROUND_DIGITS = (-10, 10)
#: Name of a computed column, without ``calc.``.
NAME_PATTERN = _re.compile(r"[a-z][a-z0-9_]{0,31}")

#: The row error kinds (spec 5.5).
ROW_ERROR_KINDS = (
    "division_by_zero", "not_a_number", "number_too_large", "number_too_small",
    "bad_argument", "value_too_long", "regex_timeout", "bad_date",
)
#: Field states that mean the input was not read.
NOT_READ_INPUT_STATES = frozenset({"unavailable", "denied", "withheld", "not read"})
#: Field states that mean the value is read from the row.
READ_INPUT_STATES = frozenset(FIELD_STATES) - NOT_READ_INPUT_STATES | {"ok"}
OK = "ok"
NOT_READ = "not read"

#: Units ``to_unit`` knows, in bytes.
UNITS: dict[str, int] = {
    "B": 1,
    **{name: 1000 ** (i + 1) for i, name in enumerate(("KB", "MB", "GB", "TB", "PB", "EB"))},
    **{name: 1024 ** (i + 1) for i, name in enumerate(("KiB", "MiB", "GiB", "TiB", "PiB", "EiB"))},
}

#: Every function, with its argument counts (min, max; None is no max).
FUNCTIONS: dict[str, tuple[int, Optional[int]]] = {
    "abs": (1, 1), "round": (1, 2), "floor": (1, 1), "ceil": (1, 1),
    "min": (2, None), "max": (2, None),
    "coalesce": (2, None), "is_null": (1, 1),
    "if": (3, 3),
    "lower": (1, 1), "upper": (1, 1), "len": (1, 1), "concat": (2, None),
    "substr": (2, 3), "split_part": (3, 3), "starts_with": (2, 2),
    "ends_with": (2, 2), "contains": (2, 2), "regex": (2, 3),
    "number": (1, 1), "string": (1, 1), "to_unit": (3, 3),
    "days_between": (2, 2),
}
#: Group metrics, readable only over a groups handle.
METRICS = ("count", "sum", "min", "max")
KEYWORDS = frozenset({"and", "or", "not", "true", "false", "null", "count"})
COMPARISONS = ("==", "!=", "<", "<=", ">", ">=")

#: The evaluation clock. Tests monkeypatch it.
_clock: Callable[[], float] = time.monotonic


# -- refusals and row errors ----------------------------------------------------


class ExpressionRefused(SubstrateError):
    """A ``compute`` expression or its evaluation refused as a whole, by
    name (``data["reason"]``). Nothing is answered from part of a set."""

    code = "invalid_operator_arguments"


def _refuse(reason: str, message: str, fix_hint: str, **data: Any) -> ExpressionRefused:
    return ExpressionRefused(message, data={"reason": reason, **data}, fix_hint=fix_hint)


class RowError(Exception):
    """One row's value cannot be computed: ``kind`` is one of
    :data:`ROW_ERROR_KINDS`. Raised by :func:`to_number` and
    :func:`store_number` too, so callers map it to a state."""

    def __init__(self, kind: str) -> None:
        super().__init__(kind)
        self.kind = kind


# -- numbers ------------------------------------------------------------------

Number = Union[int, Decimal]


def _context() -> decimal.Context:
    """The local context of one evaluation (spec 5.3). ``Underflow`` is
    trapped too, so a value too small for the context is a row error and
    never a silent zero."""

    return decimal.Context(
        prec=34, rounding=decimal.ROUND_HALF_EVEN, Emax=37,
        traps=[decimal.InvalidOperation, decimal.DivisionByZero,
               decimal.Overflow, decimal.Underflow],
    )


#: The context of rounding and storage: wide enough that no quantize of an
#: in-range value raises.
_WIDE = decimal.Context(
    prec=120, rounding=decimal.ROUND_HALF_UP, Emax=999_999, Emin=-999_999,
    traps=[decimal.InvalidOperation, decimal.DivisionByZero, decimal.Overflow,
           decimal.Underflow, decimal.Subnormal],
)


def _signal_kind(exc: BaseException) -> str:
    """The row error of one trapped decimal signal or arithmetic error.

    The C decimal module raises a trapped condition as its signal class
    (``InvalidOperation``) with the conditions in ``args[0]``
    (``[DivisionImpossible]``), so both the class and the conditions are
    read."""

    conditions: list[Any] = [type(exc)]
    if exc.args and isinstance(exc.args[0], list):
        conditions.extend(exc.args[0])

    def any_of(*kinds: type) -> bool:
        return any(isinstance(c, type) and issubclass(c, kinds) for c in conditions)

    if any_of(decimal.DivisionImpossible, decimal.Overflow, OverflowError):
        return "number_too_large"
    if any_of(decimal.DivisionByZero, ZeroDivisionError):
        return "division_by_zero"
    if any_of(decimal.Subnormal):
        return "number_too_small"
    return "not_a_number"


def _in_range(value: Number) -> Number:
    if abs(value) >= NUMBER_LIMIT:
        raise RowError("number_too_large")
    return value


def _is_number_text(text: str) -> bool:
    """``[+-]? digits [. digits] [(e|E) [+-] digits]``, scanned by hand
    (linear, no regular expression)."""

    i, n = 0, len(text)
    if i < n and text[i] in "+-":
        i += 1
    start = i
    while i < n and text[i].isascii() and text[i].isdigit():
        i += 1
    if i == start:
        return False
    if i < n and text[i] == ".":
        i += 1
        frac = i
        while i < n and text[i].isascii() and text[i].isdigit():
            i += 1
        if i == frac:
            return False
    if i < n and text[i] in "eE":
        i += 1
        if i < n and text[i] in "+-":
            i += 1
        exp = i
        while i < n and text[i].isascii() and text[i].isdigit():
            i += 1
        if i == exp:
            return False
    return i == n


def _whole(value: Decimal) -> Number:
    """A finite decimal as ``int`` when whole, else itself."""

    if value == value.to_integral_value(context=_WIDE):
        return int(value)
    return value


def _from_text(text: str) -> Number:
    stripped = text.strip()
    if len(stripped) > MAX_NUMBER_TEXT or not _is_number_text(stripped):
        raise RowError("not_a_number")
    if "." not in stripped and "e" not in stripped and "E" not in stripped:
        return _in_range(int(stripped))
    try:
        value = _WIDE.create_decimal(stripped)
    except decimal.DecimalException:
        # The exponent is past what any context holds.
        raise RowError(
            "number_too_small" if "e-" in stripped.lower() else "number_too_large"
        ) from None
    if value.adjusted() >= 38:
        raise RowError("number_too_large")
    return _in_range(_whole(value))


def to_number(value: Any) -> Optional[Number]:
    """The one rule for reading a value as a number (spec 5.3).

    ``None`` is null. A ``bool`` is not a number. An ``int`` is itself. A
    finite ``float`` is ``Decimal(repr(x))``, its shortest text, so a stored
    15-digit float reads back exactly. A ``Decimal`` is itself. Numeric text
    (at most 64 characters) is read the same way. A whole value is ``int``.
    ``nan``, ``inf``, other text and other kinds raise
    ``RowError("not_a_number")``; a value past 10^38 raises
    ``RowError("number_too_large")``.
    """

    if value is None:
        return None
    if isinstance(value, bool):
        raise RowError("not_a_number")
    if isinstance(value, int):
        return _in_range(value)
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            raise RowError("not_a_number")
        return _from_text(repr(value))
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise RowError("not_a_number")
        if value.adjusted() >= 38:
            raise RowError("number_too_large")
        return _in_range(_whole(value))
    if isinstance(value, str):
        return _from_text(value)
    raise RowError("not_a_number")


def _fifteen(value: Decimal) -> Decimal:
    """``value`` rounded to 15 significant digits, halves away from zero."""

    quantum = Decimal(1).scaleb(value.adjusted() - (STORED_DIGITS - 1), context=_WIDE)
    return value.quantize(quantum, rounding=decimal.ROUND_HALF_UP, context=_WIDE)


def store_number(value: Number) -> Union[int, float]:
    """The number a column keeps (spec 5.3, "Stored values").

    A whole number is an ``int``. Any other number is a ``float`` holding the
    value rounded to 15 significant digits, halves away from zero; ``repr``
    of it is that text. A non-zero value below 10^-38 raises
    ``RowError("number_too_small")``.
    """

    if isinstance(value, bool) or not isinstance(value, (int, Decimal)):
        raise TypeError(f"store_number takes an int or a Decimal, not {type(value).__name__}")
    if isinstance(value, int):
        return _in_range(value)
    if not value.is_finite():
        raise RowError("not_a_number")
    if value.is_zero():
        return 0
    if abs(value) < SMALLEST_STORED:
        raise RowError("number_too_small")
    if value == value.to_integral_value(context=_WIDE):
        return _in_range(int(value))
    # Rounding to 15 digits can carry a value up to 10^38 exactly
    # (99...9.5 -> 1e38), so the range is checked on the rounded value.
    rounded = _in_range(_fifteen(value))
    if rounded == rounded.to_integral_value(context=_WIDE):
        return int(rounded)
    return float(rounded)


def _number_text(value: Number) -> str:
    """The printed text of a number: ``string(x)``."""

    stored = store_number(value)
    return str(stored) if isinstance(stored, int) else repr(stored)


# -- dates --------------------------------------------------------------------

_DATE_FORM = _re.compile(
    r"[1-9][0-9]{3}-[0-9]{2}-[0-9]{2}"
    r"(?:[T ](?:[01][0-9]|2[0-3]):[0-5][0-9](?::[0-5][0-9](?:\.[0-9]{1,6})?)?"
    r"(?:Z|[+-](?:0[0-9]|1[0-4]):?[0-5][0-9])?)?"
)
#: Longest text read as a date.
MAX_DATE_TEXT = 40


def _to_date(value: Any) -> Optional[_dt.datetime]:
    """A date operand as an aware UTC instant. ``'2026-10-21'`` is UTC
    midnight; a time with no offset is read as UTC. Anything else is
    ``bad_date``."""

    if value is None:
        return None
    if isinstance(value, _dt.datetime):
        return (value if value.tzinfo else value.replace(tzinfo=_dt.timezone.utc)).astimezone(
            _dt.timezone.utc
        )
    if isinstance(value, _dt.date):
        return _dt.datetime(value.year, value.month, value.day, tzinfo=_dt.timezone.utc)
    if not isinstance(value, str) or len(value) > MAX_DATE_TEXT:
        raise RowError("bad_date")
    if _DATE_FORM.fullmatch(value) is None:
        raise RowError("bad_date")
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = _dt.datetime.fromisoformat(text)
    except ValueError:
        raise RowError("bad_date") from None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=_dt.timezone.utc)
    return parsed.astimezone(_dt.timezone.utc)


def _date_text(value: _dt.datetime) -> str:
    text = value.astimezone(_dt.timezone.utc).replace(tzinfo=None).isoformat()
    return text + "Z"


# -- regex (spec 5.6) -----------------------------------------------------------


def _repeat_count_at(pattern: str, i: int) -> tuple[int, int]:
    """``(index past, largest count)`` of a plain repeat count ``{n}``,
    ``{n,}``, ``{,m}`` or ``{n,m}`` starting at ``pattern[i] == "{"``, or
    ``(-1, 0)``."""

    j, n = i + 1, len(pattern)
    left = j
    while j < n and pattern[j].isascii() and pattern[j].isdigit():
        j += 1
    counts = [pattern[left:j]]
    if j < n and pattern[j] == ",":
        j += 1
        right = j
        while j < n and pattern[j].isascii() and pattern[j].isdigit():
            j += 1
        counts.append(pattern[right:j])
    if not any(counts) or j >= n or pattern[j] != "}":
        return -1, 0
    return j + 1, max(int(c) for c in counts if c)


def _dialect_problem(pattern: str) -> Optional[str]:
    """What the standard ``re`` and ``regex`` would read differently, or None.

    * an unescaped ``{`` outside a set that does not start a plain repeat
      count: ``regex`` may read it as a fuzzy match (``{e<=2}``, ``{e}``,
      ``{1<=e<=3}``, ``{2i+2d+1s<=4}``) where ``re`` reads literal text;
    * ``[:``, ``[=`` or ``[.`` inside a set: a POSIX class to ``regex``;
    * ``\\B``: on empty text ``re`` finds no match and ``regex`` finds one.
    """

    i, n = 0, len(pattern)
    in_set = False
    while i < n:
        ch = pattern[i]
        if ch == "\\":
            if not in_set and i + 2 < n and pattern[i + 1] == "N" and pattern[i + 2] == "{":
                end = pattern.find("}", i + 3)
                if end < 0:
                    return "an unterminated \\N{...} escape"
                i = end + 1
                continue
            if not in_set and i + 1 < n and pattern[i + 1] == "B":
                # Found by the fuzz check: on empty text Python 3.12's re finds
                # no \B match and regex (VERSION0) finds one.
                return "\\B, which re and regex match differently on empty text"
            i += 2
            continue
        if in_set:
            if ch == "[" and i + 1 < n and pattern[i + 1] in ":=.":
                return f"the POSIX class form '[{pattern[i + 1]}' inside a set"
            if ch == "]":
                in_set = False
            i += 1
            continue
        if ch == "[":
            in_set = True
            i += 1
            if i < n and pattern[i] == "^":
                i += 1
            if i < n and pattern[i] == "]":
                i += 1
            continue
        if ch == "{":
            end, largest = _repeat_count_at(pattern, i)
            if end < 0:
                return "a '{' that is not a repeat count (fuzzy-match syntax)"
            if largest > MAX_REPEAT_COUNT:
                # The regex library expands a count when it compiles:
                # a{100000000} took 46 s and gigabytes (code review 3b).
                raise _refuse(
                    "regex_too_complex",
                    f"the regex pattern {pattern!r} repeats {largest} times; the limit is {MAX_REPEAT_COUNT}",
                    f"use a repeat count of at most {MAX_REPEAT_COUNT}",
                    limit=MAX_REPEAT_COUNT,
                )
            i = end
            continue
        i += 1
    return None


@dataclass(frozen=True)
class _Pattern:
    text: str
    compiled: Any
    group: Union[int, str]


def _compile_pattern(pattern: str, group: Union[int, str], where: str) -> _Pattern:
    hint = "write a pattern of the standard re dialect, at most 200 characters, with no repeat inside a repeat"
    if len(pattern) > MAX_PATTERN_CHARS:
        raise _refuse(
            "regex_too_long", f"{where}: the regex pattern has {len(pattern)} characters; the limit is {MAX_PATTERN_CHARS}",
            hint, limit=MAX_PATTERN_CHARS,
        )
    problem = _dialect_problem(pattern)
    if problem is not None:
        raise _refuse(
            "regex_dialect", f"{where}: the regex pattern {pattern!r} has {problem}",
            "write \\{ for a literal brace and \\[ for a literal bracket; fuzzy matching and POSIX classes are not offered",
        )
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            standard = _re.compile(pattern)
    except (_re.error, OverflowError, RecursionError, ValueError) as exc:
        raise _refuse(
            "regex_dialect", f"{where}: the regex pattern {pattern!r} is not a standard re pattern ({exc})", hint,
        ) from None
    if _backtracks(pattern):
        raise _refuse(
            "regex_backtracks",
            f"{where}: the regex pattern {pattern!r} has a repeat inside a repeat or a branch inside a repeat",
            "write the pattern without a repeated group that itself repeats or branches",
        )
    try:
        compiled = _regex.compile(pattern, flags=_regex.VERSION0)
    except (_regex.error, OverflowError, RecursionError, ValueError) as exc:
        raise _refuse("regex_dialect", f"{where}: the regex pattern {pattern!r} does not compile ({exc})", hint) from None
    if isinstance(group, int):
        if group < 0 or group > standard.groups:
            raise _refuse(
                "bad_argument", f"{where}: the pattern has {standard.groups} groups; group {group} does not exist",
                "name a group the pattern has, or leave group out for the whole match",
            )
    elif group not in standard.groupindex:
        raise _refuse(
            "bad_argument", f"{where}: the pattern has no group named {group!r}",
            "name a group the pattern has, or leave group out for the whole match",
        )
    return _Pattern(pattern, compiled, group)


# -- the input's columns ------------------------------------------------------


@dataclass(frozen=True)
class ColumnSpec:
    """What the input rows carry.

    ``groups`` is ``None`` for record rows (live rows or flat rows); for a
    groups handle it is the exact printed column names (by fields, metric
    labels such as ``count`` and ``sum(attrs.file_size)``, ``rank``,
    ``occurrences``, ``month(date.created)``). ``calc`` names the ``calc.``
    columns the input already has, without the prefix.
    """

    groups: Optional[tuple[str, ...]] = None
    calc: tuple[str, ...] = ()

    @property
    def is_groups(self) -> bool:
        return self.groups is not None


# -- syntax tree --------------------------------------------------------------


@dataclass(frozen=True)
class _Lit:
    value: Any


@dataclass(frozen=True)
class _Field:
    #: The operand name: ``attrs.<key>``, ``node_id``, ``calc.<name>``,
    #: ``date.<name>``, ``date.attrs.<key>``, or a groups-handle column.
    name: str
    #: ``attrs``, ``record``, ``calc``, ``date``, ``date_attrs`` or ``column``.
    kind: str
    #: The key read: the attrs key, the record column, the calc name, the
    #: date name or the groups column.
    key: str


@dataclass(frozen=True)
class _Neg:
    operand: Any


@dataclass(frozen=True)
class _Not:
    operand: Any


@dataclass(frozen=True)
class _Chain:
    """``first (op operand)*`` of one level: ``+ -`` or ``* / %``."""

    first: Any
    rest: tuple[tuple[str, Any], ...]


@dataclass(frozen=True)
class _Logic:
    op: str  # "and" | "or"
    operands: tuple[Any, ...]


@dataclass(frozen=True)
class _Cmp:
    op: str
    left: Any
    right: Any


@dataclass(frozen=True)
class _Call:
    name: str
    args: tuple[Any, ...]
    pattern: Optional[_Pattern] = None


# -- lexer --------------------------------------------------------------------


@dataclass(frozen=True)
class _Tok:
    kind: str  # num str name field op ( ) , [ ] end
    text: str
    pos: int
    value: Any = None


def _is_word_char(ch: str) -> bool:
    return ch.isascii() and (ch.isalnum() or ch == "_")


class _Syntax(Exception):
    def __init__(self, pos: int, what: str) -> None:
        super().__init__(what)
        self.pos = pos
        self.what = what


def _scan_key(text: str, i: int) -> int:
    """The index past ``[A-Za-z0-9_]+ ( "." [A-Za-z0-9_]+ )*`` at ``i``."""

    n = len(text)
    start = i
    while i < n and _is_word_char(text[i]):
        i += 1
    if i == start:
        raise _Syntax(start, "a key of letters, digits and _ must follow")
    while i < n and text[i] == "." and i + 1 < n and _is_word_char(text[i + 1]):
        i += 1
        while i < n and _is_word_char(text[i]):
            i += 1
    return i


def _lex(text: str, column: str) -> list[_Tok]:
    toks: list[_Tok] = []
    i, n = 0, len(text)
    while i < n:
        ch = text[i]
        if ch in " \t\r\n":
            i += 1
            continue
        if ord(ch) < 0x20 or ord(ch) == 0x7F:
            raise _Syntax(i, f"the control character U+{ord(ch):04X} is not allowed")
        if ch in "'\"":
            quote, j, out = ch, i + 1, []
            while True:
                if j >= n:
                    raise _Syntax(i, "this string is not closed")
                c = text[j]
                if ord(c) < 0x20 or ord(c) == 0x7F:
                    raise _Syntax(j, f"the control character U+{ord(c):04X} is not allowed")
                if c == "\\" and j + 1 < n and text[j + 1] in "'\"\\":
                    out.append(text[j + 1])
                    j += 2
                    continue
                if c == quote:
                    break
                out.append(c)
                j += 1
            value = "".join(out)
            if len(value) > MAX_LITERAL_CHARS:
                raise _refuse(
                    "literal_too_long",
                    f"calc.{column}: a string literal has {len(value)} characters; the limit is {MAX_LITERAL_CHARS}",
                    "shorten the literal", limit=MAX_LITERAL_CHARS, position=i + 1, column=column,
                )
            toks.append(_Tok("str", text[i:j + 1], i, value))
            i = j + 1
            continue
        if ch.isascii() and ch.isdigit():
            j = i
            while j < n and text[j].isascii() and text[j].isdigit():
                j += 1
            if j < n and text[j] == ".":
                j += 1
                if not (j < n and text[j].isascii() and text[j].isdigit()):
                    raise _Syntax(j, "a digit must follow '.' in a number")
                while j < n and text[j].isascii() and text[j].isdigit():
                    j += 1
            if j < n and text[j] in "eE":
                k = j + 1
                if k < n and text[k] in "+-":
                    k += 1
                if k < n and text[k].isascii() and text[k].isdigit():
                    while k < n and text[k].isascii() and text[k].isdigit():
                        k += 1
                    j = k
            if j < n and _is_word_char(text[j]):
                raise _Syntax(j, "a number cannot run into a name")
            literal = text[i:j]
            if len(literal) > MAX_NUMBER_TEXT:
                raise _refuse(
                    "literal_too_long", f"calc.{column}: a number literal has {len(literal)} characters; the limit is {MAX_NUMBER_TEXT}",
                    "shorten the number", limit=MAX_NUMBER_TEXT, position=i + 1, column=column,
                )
            try:
                value = to_number(literal)
            except RowError as exc:
                raise _refuse(
                    exc.kind, f"calc.{column}: the number {literal} is out of range: every number stays below 10^38 and above 10^-999999",
                    "use a smaller number", position=i + 1, column=column,
                ) from None
            toks.append(_Tok("num", literal, i, value))
            i = j
            continue
        if _is_word_char(ch):
            j = i
            while j < n and _is_word_char(text[j]):
                j += 1
            word = text[i:j]
            if j < n and text[j] == "." and word in ("attrs", "calc", "date"):
                if word == "attrs":
                    end = _scan_key(text, j + 1)
                    toks.append(_Tok("field", text[i:end], i, ("attrs", text[j + 1:end])))
                elif word == "calc":
                    end = _scan_key(text, j + 1)
                    toks.append(_Tok("field", text[i:end], i, ("calc", text[j + 1:end])))
                else:
                    rest = j + 1
                    if text.startswith("attrs.", rest):
                        end = _scan_key(text, rest + len("attrs."))
                        toks.append(_Tok("field", text[i:end], i, ("date_attrs", text[rest + 6:end])))
                    else:
                        end = _scan_key(text, rest)
                        toks.append(_Tok("field", text[i:end], i, ("date", text[rest:end])))
                i = end
                continue
            if j < n and text[j] == ".":
                end = _scan_key(text, j + 1) if j + 1 < n and _is_word_char(text[j + 1]) else j + 1
                raise _Syntax(
                    i, f"{text[i:end]!r} is not a field; write attrs.{text[i:end]} or attrs['{text[i:end]}']"
                )
            toks.append(_Tok("name", word, i))
            i = j
            continue
        two = text[i:i + 2]
        if two in ("==", "!=", "<=", ">="):
            toks.append(_Tok("op", two, i))
            i += 2
            continue
        if ch in "=<>+-*/%":
            toks.append(_Tok("op", ch, i))
            i += 1
            continue
        if ch in "(),[]":
            toks.append(_Tok(ch, ch, i))
            i += 1
            continue
        raise _Syntax(i, f"the character {ch!r} is not part of the language")
    toks.append(_Tok("end", "", n))
    return toks


# -- parser -------------------------------------------------------------------


def _did_you_mean(word: str, choices: Sequence[str]) -> str:
    close = difflib.get_close_matches(word, list(choices), n=1, cutoff=0.6)
    return f"; did you mean {close[0]}?" if close else ""


class _Parser:
    def __init__(self, text: str, column: str, spec: ColumnSpec, siblings: frozenset[str]) -> None:
        self.text = text
        self.column = column
        self.spec = spec
        self.siblings = siblings
        self.toks = _lex(text, column)
        self.i = 0
        self.depth = 0
        self.nodes = 0
        self.notes: list[str] = []
        self.bare: dict[str, str] = {}

    # -- plumbing --
    def peek(self) -> _Tok:
        return self.toks[self.i]

    def take(self) -> _Tok:
        tok = self.toks[self.i]
        self.i += 1
        return tok

    def expect(self, kind: str, what: str) -> _Tok:
        tok = self.peek()
        if tok.kind != kind:
            raise _Syntax(tok.pos, f"expected {what}, found {tok.text or 'the end'!r}")
        return self.take()

    def node(self, value: Any) -> Any:
        self.nodes += 1
        if self.nodes > MAX_NODES:
            raise _refuse(
                "too_many_nodes", f"calc.{self.column}: the expression has more than {MAX_NODES} syntax nodes",
                "split it into two computed columns", limit=MAX_NODES, column=self.column,
            )
        return value

    def enter(self, tok: _Tok) -> None:
        self.depth += 1
        if self.depth > MAX_DEPTH:
            raise _refuse(
                "too_deep",
                f"calc.{self.column}: the expression nests deeper than {MAX_DEPTH} at character {tok.pos + 1}",
                "split it into two computed columns", limit=MAX_DEPTH, column=self.column, position=tok.pos + 1,
            )

    def leave(self) -> None:
        self.depth -= 1

    def is_word(self, tok: _Tok, word: str) -> bool:
        return tok.kind == "name" and tok.text.lower() == word

    # -- grammar --
    def parse(self) -> Any:
        tree = self.expr()
        tok = self.peek()
        if tok.kind != "end":
            raise _Syntax(tok.pos, f"unexpected {tok.text!r}")
        return tree

    def expr(self) -> Any:
        return self.logic("or", self.logic_and)

    def logic_and(self) -> Any:
        return self.logic("and", self.logic_not)

    def logic(self, op: str, below: Callable[[], Any]) -> Any:
        first = below()
        operands = [first]
        while self.is_word(self.peek(), op):
            self.take()
            operands.append(below())
        if len(operands) == 1:
            return first
        return self.node(_Logic(op, tuple(operands)))

    def logic_not(self) -> Any:
        nots: list[_Tok] = []
        while self.is_word(self.peek(), "not"):
            tok = self.take()
            self.enter(tok)
            nots.append(tok)
        tree = self.comparison()
        for _ in nots:
            tree = self.node(_Not(tree))
            self.leave()
        return tree

    def comparison(self) -> Any:
        left = self.additive()
        tok = self.peek()
        if tok.kind == "op" and tok.text in COMPARISONS + ("=",):
            self.take()
            op = tok.text
            if op == "=":
                op = "=="
                note = "'=' read as '=='"
                if note not in self.notes:
                    self.notes.append(note)
            right = self.additive()
            after = self.peek()
            if after.kind == "op" and after.text in COMPARISONS + ("=",):
                raise _Syntax(after.pos, "comparisons do not chain; join two comparisons with and")
            return self.node(_Cmp(op, left, right))
        return left

    def arithmetic_operand(self, tree: Any, tok: _Tok) -> Any:
        if isinstance(tree, _Lit) and isinstance(tree.value, bool):
            raise _Syntax(tok.pos, "a boolean is not a number")
        return tree

    def additive(self) -> Any:
        first_tok = self.peek()
        first = self.multiplicative()
        rest: list[tuple[str, Any]] = []
        while self.peek().kind == "op" and self.peek().text in "+-":
            op = self.take().text
            tok = self.peek()
            rest.append((op, self.arithmetic_operand(self.multiplicative(), tok)))
        if not rest:
            return first
        return self.node(_Chain(self.arithmetic_operand(first, first_tok), tuple(rest)))

    def multiplicative(self) -> Any:
        tok = self.peek()
        first = self.unary()
        rest: list[tuple[str, Any]] = []
        while self.peek().kind == "op" and self.peek().text in "*/%":
            op = self.take().text
            nxt = self.peek()
            rest.append((op, self.arithmetic_operand(self.unary(), nxt)))
        if not rest:
            return first
        return self.node(_Chain(self.arithmetic_operand(first, tok), tuple(rest)))

    def unary(self) -> Any:
        signs: list[_Tok] = []
        while self.peek().kind == "op" and self.peek().text == "-":
            tok = self.take()
            self.enter(tok)
            signs.append(tok)
        tok = self.peek()
        tree = self.atom()
        if signs:
            self.arithmetic_operand(tree, tok)
        for _ in signs:
            tree = self.node(_Neg(tree))
            self.leave()
        return tree

    def atom(self) -> Any:
        tok = self.peek()
        if tok.kind == "num" or tok.kind == "str":
            self.take()
            return self.node(_Lit(tok.value))
        if tok.kind == "(":
            self.take()
            self.enter(tok)
            if self.peek().kind == ")":
                raise _Syntax(self.peek().pos, "empty parentheses")
            tree = self.expr()
            self.expect(")", "')'")
            self.leave()
            return tree
        if tok.kind == "field":
            self.take()
            kind, key = tok.value
            return self.node(self.resolve(kind, key, tok))
        if tok.kind == "name":
            return self.name(tok)
        if tok.kind == "end":
            raise _Syntax(tok.pos, "the expression ends too early")
        raise _Syntax(tok.pos, f"unexpected {tok.text!r}")

    def name(self, tok: _Tok) -> Any:
        self.take()
        word = tok.text
        low = word.lower()
        nxt = self.peek()
        if low in ("true", "false"):
            return self.node(_Lit(low == "true"))
        if low == "null":
            return self.node(_Lit(None))
        if word in ("attrs", "col") and nxt.kind == "[":
            self.take()
            key_tok = self.expect("str", "a quoted name")
            self.expect("]", "']'")
            if word == "attrs":
                return self.node(self.resolve("attrs", key_tok.value, tok))
            return self.node(self.group_column(key_tok.value, tok, written=f"col[{key_tok.text}]"))
        if word in ("attrs", "calc", "date", "col"):
            raise _Syntax(tok.pos, f"{word} needs a name: write {word}.<name>" + (
                f" or {word}['<name>']" if word in ("attrs", "col") else ""))
        if low == "count":
            if nxt.kind == "(":
                raise _Syntax(nxt.pos, "count takes no argument; write count")
            return self.node(self.metric("count", None, tok))
        if nxt.kind == "(":
            if low == "sum" or (low in ("min", "max") and self.one_field_argument()):
                return self.metric_call(low, tok)
            if low not in FUNCTIONS:
                raise _refuse(
                    "unknown_function",
                    f"calc.{self.column}: {word!r} is not a function{_did_you_mean(low, list(FUNCTIONS))}",
                    "functions: " + " ".join(FUNCTIONS),
                    column=self.column, position=tok.pos + 1, function=word,
                )
            return self.call(low, tok)
        if low in KEYWORDS:
            raise _Syntax(tok.pos, f"{word!r} is a keyword here; write attrs['{word}'] for a field of that name")
        if low in FUNCTIONS or low in METRICS:
            raise _Syntax(tok.pos, f"{word} is a function; write {word}(...)")
        if word in RECORD_FIELDS:
            return self.node(self.resolve("record", word, tok))
        if word in self.spec.calc or (word != self.column and word in self.siblings):
            # PACT read-map-compute (6.5): a bare name equal to a computed
            # column of the input reads it (the column being defined
            # included, so {"fill": "fill * 100"} scales the input's fill), and
            # one equal to another column of this compute reads that one.
            # A column's own name with no such input column stays an
            # upstream field.
            note = f"bare name {word!r} read as calc.{word}"
            if note not in self.notes:
                self.notes.append(note)
            return self.node(self.resolve("calc", word, tok))
        if word.isascii() and _re.fullmatch(r"[A-Za-z0-9_]+", word):
            name = f"attrs.{word}"
            self.bare[word] = name
            note = f"bare name {word!r} read as {name}"
            if note not in self.notes:
                self.notes.append(note)
            return self.node(self.resolve("attrs", word, tok))
        raise _Syntax(tok.pos, f"unexpected {word!r}")

    def one_field_argument(self) -> bool:
        """Whether ``( field )`` follows: the metric form of min and max."""

        toks = self.toks
        i = self.i
        if toks[i].kind != "(":
            return False
        j = i + 1
        if toks[j].kind == "field":
            return toks[j + 1].kind == ")"
        if toks[j].kind == "name" and toks[j].text in ("attrs", "col") and toks[j + 1].kind == "[":
            return toks[j + 4].kind == ")" if j + 4 < len(toks) else False
        if toks[j].kind == "name" and toks[j + 1].kind == ")":
            word = toks[j].text
            return word.lower() not in KEYWORDS and word.lower() not in FUNCTIONS
        return False

    def metric_call(self, function: str, tok: _Tok) -> Any:
        open_tok = self.take()
        self.enter(open_tok)
        arg_tok = self.peek()
        name = self.metric_field(function)
        if self.peek().kind != ")":
            raise _Syntax(arg_tok.pos, f"{function}(...) is a group metric and takes one field")
        self.expect(")", "')'")
        self.leave()
        return self.node(self.metric(function, name, tok))

    def metric_field(self, function: str) -> str:
        """The field inside ``sum( )``, ``min( )`` or ``max( )``, by the
        name the metric label prints. It is not a column of a groups handle
        itself, so it is not resolved against one."""

        tok = self.peek()
        if tok.kind == "field":
            self.take()
            kind, key = tok.value
            if kind == "calc" and not NAME_PATTERN.fullmatch(key):
                raise _Syntax(tok.pos, f"calc.{key} is not a computed column name")
            if kind == "date":
                self.check_date_name(key, tok)
            return {
                "attrs": f"attrs.{key}", "calc": f"calc.{key}",
                "date": f"date.{key}", "date_attrs": f"date.attrs.{key}",
            }[kind]
        if tok.kind == "name" and tok.text in ("attrs", "col") and self.toks[self.i + 1].kind == "[":
            self.take()
            self.take()
            key = self.expect("str", "a quoted name").value
            self.expect("]", "']'")
            # col['...'] names the field by its exact printed name.
            return f"attrs.{key}" if tok.text == "attrs" else key
        if tok.kind == "name" and tok.text in RECORD_FIELDS:
            self.take()
            return tok.text
        word = tok.text
        if tok.kind == "name" and word.lower() not in KEYWORDS and word.lower() not in FUNCTIONS:
            self.take()
            name = f"attrs.{word}"
            self.bare[word] = name
            note = f"bare name {word!r} read as {name}"
            if note not in self.notes:
                self.notes.append(note)
            return name
        if tok.kind == ")":
            raise _Syntax(tok.pos, f"{function} needs a field")
        raise _Syntax(tok.pos, f"{function}(...) is a group metric and takes one field")

    def check_date_name(self, name: str, tok: _Tok) -> None:
        """``date.<name>`` takes only the named date meanings the read
        surface serves (``created``, ``modified``, ...)."""

        if name not in DATE_MEANINGS:
            meanings = sorted(DATE_MEANINGS)
            raise _refuse(
                "unknown_date_field",
                f"calc.{self.column}: date.{name} is not a date field{_did_you_mean(name, meanings)}",
                "name one of " + ", ".join(f"date.{m}" for m in meanings)
                + f", or date.attrs.{name} for a date attribute",
                column=self.column, position=tok.pos + 1,
            )

    def metric(self, function: str, field_name: Optional[str], tok: _Tok) -> _Field:
        label = "count" if field_name is None else f"{function}({field_name})"
        if not self.spec.is_groups:
            raise _refuse(
                "metric_needs_groups",
                f"calc.{self.column}: {label} is a group metric; aggregate first, or give two or more values",
                "aggregate(..., metrics=[...]) first, then map over its groups handle; "
                "for a field named count write attrs.count",
                column=self.column, position=tok.pos + 1,
            )
        return self.group_column(label, tok, written=label)

    def group_column(self, name: str, tok: _Tok, *, written: str) -> _Field:
        if not self.spec.is_groups:
            raise _refuse(
                "col_needs_groups",
                f"calc.{self.column}: {written} names a column of a groups handle; this input is not one",
                "write attrs.<key> or attrs['<key>'] for a field of a record",
                column=self.column, position=tok.pos + 1,
            )
        columns = self.spec.groups or ()
        if name not in columns:
            raise _refuse(
                "unknown_column",
                f"calc.{self.column}: the groups handle has no column {name!r}{_did_you_mean(name, columns)}",
                "columns: " + ", ".join(columns),
                column=self.column, position=tok.pos + 1,
            )
        return _Field(name, "column", name)

    def resolve(self, kind: str, key: str, tok: _Tok) -> _Field:
        if kind == "calc":
            if not NAME_PATTERN.fullmatch(key):
                raise _Syntax(tok.pos, f"calc.{key} is not a computed column name ([a-z][a-z0-9_]{{0,31}})")
            known = self.siblings | frozenset(self.spec.calc)
            if key not in known:
                raise _refuse(
                    "unknown_column",
                    f"calc.{self.column}: there is no computed column calc.{key}"
                    f"{_did_you_mean(key, sorted(known))}",
                    "name a column of this compute or one the input already has",
                    column=self.column, position=tok.pos + 1,
                )
            return _Field(f"calc.{key}", "calc", key)
        if kind == "date":
            self.check_date_name(key, tok)
        name = {
            "attrs": f"attrs.{key}", "record": key, "date": f"date.{key}", "date_attrs": f"date.attrs.{key}",
        }[kind]
        if self.spec.is_groups:
            return self.group_column(name, tok, written=name)
        return _Field(name, kind, key)

    def call(self, function: str, tok: _Tok) -> Any:
        open_tok = self.take()
        self.enter(open_tok)
        args: list[Any] = []
        if self.peek().kind != ")":
            args.append(self.expr())
            while self.peek().kind == ",":
                self.take()
                args.append(self.expr())
        self.expect(")", "')' or ','")
        self.leave()
        low, high = FUNCTIONS[function]
        if len(args) < low or (high is not None and len(args) > high):
            if function in ("min", "max") and len(args) == 1:
                raise _refuse(
                    "metric_needs_groups",
                    f"calc.{self.column}: {function}(x) with one value is a group metric; "
                    "aggregate first, or give two or more values",
                    f"{function}(a, b) compares values of one row",
                    column=self.column, position=tok.pos + 1,
                )
            want = f"{low}" if low == high else (f"{low} or more" if high is None else f"{low} to {high}")
            raise _refuse(
                "bad_argument_count",
                f"calc.{self.column}: {function} takes {want} arguments, not {len(args)}",
                f"write {function}(" + ", ".join(["…"] * low) + ")",
                column=self.column, position=tok.pos + 1,
            )
        pattern = self.check_literals(function, args, tok)
        return self.node(_Call(function, tuple(args), pattern))

    def literal_int(self, node: Any) -> Optional[int]:
        if isinstance(node, _Neg) and isinstance(node.operand, _Lit):
            inner = node.operand.value
            if isinstance(inner, int) and not isinstance(inner, bool):
                return -inner
            return None
        if isinstance(node, _Lit) and isinstance(node.value, int) and not isinstance(node.value, bool):
            return node.value
        return None

    def refuse_argument(self, function: str, what: str, tok: _Tok) -> ExpressionRefused:
        return _refuse(
            "bad_argument", f"calc.{self.column}: {function}: {what}", "correct the literal argument",
            column=self.column, position=tok.pos + 1,
        )

    def check_literals(self, function: str, args: list[Any], tok: _Tok) -> Optional[_Pattern]:
        literal = [self.literal_int(a) for a in args]
        if function == "substr":
            if literal[1] is not None and literal[1] < 1:
                raise self.refuse_argument(function, "start counts from 1", tok)
            if len(args) == 3 and literal[2] is not None and literal[2] < 0:
                raise self.refuse_argument(function, "length cannot be negative", tok)
        elif function == "split_part":
            if literal[2] == 0:
                raise self.refuse_argument(function, "n counts from 1 (or -1 for the last piece), never 0", tok)
            if isinstance(args[1], _Lit) and args[1].value == "":
                raise self.refuse_argument(function, "the separator cannot be empty", tok)
        elif function == "round" and len(args) == 2:
            if isinstance(args[1], _Lit) and isinstance(args[1].value, Decimal):
                raise self.refuse_argument(function, "digits is a whole number", tok)
            value = literal[1]
            if value is not None and not ROUND_DIGITS[0] <= value <= ROUND_DIGITS[1]:
                raise self.refuse_argument(function, "digits is a whole number from -10 to 10", tok)
        elif function == "to_unit":
            for unit in args[1:]:
                if not (isinstance(unit, _Lit) and isinstance(unit.value, str)):
                    raise self.refuse_argument(function, "units are quoted names such as 'B' and 'PB'", tok)
                if unit.value not in UNITS:
                    same = [u for u in UNITS if u.lower() == unit.value.lower()]
                    hint = f"; did you mean {same[0]}?" if same else _did_you_mean(unit.value, list(UNITS))
                    raise _refuse(
                        "unknown_unit",
                        f"calc.{self.column}: to_unit: {unit.value!r} is not a unit{hint}",
                        "units: " + " ".join(UNITS), column=self.column, position=tok.pos + 1,
                    )
        elif function == "regex":
            pattern = args[1]
            if not (isinstance(pattern, _Lit) and isinstance(pattern.value, str)):
                raise self.refuse_argument(function, "the pattern is a quoted string literal", tok)
            group: Union[int, str] = 0
            if len(args) == 3:
                g = args[2]
                if isinstance(g, _Lit) and isinstance(g.value, str):
                    group = g.value
                elif literal[2] is not None:
                    group = literal[2]
                else:
                    raise self.refuse_argument(function, "the group is a literal number or name", tok)
            return _compile_pattern(pattern.value, group, f"calc.{self.column}")
        return None


def _caret(text: str, pos: int) -> str:
    return f"{text}\n{' ' * pos}^"


def _parse(text: str, column: str, spec: ColumnSpec, siblings: frozenset[str]) -> _Parser:
    if len(text) > MAX_EXPRESSION_CHARS:
        raise _refuse(
            "expression_too_long",
            f"calc.{column}: the expression has {len(text)} characters; the limit is {MAX_EXPRESSION_CHARS}",
            "split it into two computed columns", limit=MAX_EXPRESSION_CHARS, column=column,
        )
    try:
        parser = _Parser(text, column, spec, siblings)
        parser.tree = parser.parse()
    except _Syntax as exc:
        raise _refuse(
            "expression_syntax",
            f"calc.{column}: {exc.what} at character {exc.pos + 1}\n{_caret(text, exc.pos)}",
            "correct the expression at the caret",
            column=column, position=exc.pos + 1, caret=_caret(text, exc.pos),
        ) from None
    return parser


# -- program ------------------------------------------------------------------


@dataclass(frozen=True)
class Column:
    """One computed column."""

    name: str  # without calc.
    source: str
    tree: Any = field(repr=False)
    #: Input operands this column reads directly (sibling calc excluded).
    operands: tuple[str, ...] = ()
    #: Sibling computed columns this column reads.
    depends: tuple[str, ...] = ()

    @property
    def label(self) -> str:
        return f"calc.{self.name}"


@dataclass(frozen=True)
class Budget:
    """The budgets of one evaluation (spec 5.8)."""

    seconds: float = EVAL_SECONDS
    regex_seconds: float = REGEX_CALL_SECONDS
    max_bytes: int = MAX_OUTPUT_BYTES
    max_rows: int = MAX_ROWS


@dataclass
class ColumnResult:
    """Values, states and counts of one computed column, per input row."""

    name: str
    values: list[Any]
    states: list[str]
    counts: dict[str, int]


@dataclass
class Result:
    columns: dict[str, ColumnResult]
    rows: int
    bytes: int
    regex_timeouts: int
    regex_seconds: float
    seconds: float


def _fields_of(tree: Any, out: list[_Field]) -> None:
    if isinstance(tree, _Field):
        out.append(tree)
    elif isinstance(tree, (_Neg, _Not)):
        _fields_of(tree.operand, out)
    elif isinstance(tree, _Chain):
        _fields_of(tree.first, out)
        for _, operand in tree.rest:
            _fields_of(operand, out)
    elif isinstance(tree, _Logic):
        for operand in tree.operands:
            _fields_of(operand, out)
    elif isinstance(tree, _Cmp):
        _fields_of(tree.left, out)
        _fields_of(tree.right, out)
    elif isinstance(tree, _Call):
        for arg in tree.args:
            _fields_of(arg, out)


@dataclass(frozen=True)
class Program:
    """Compiled computed columns, in dependency order."""

    columns: tuple[Column, ...]
    spec: ColumnSpec
    notes: tuple[str, ...] = ()
    #: Bare names read as ``attrs.<name>``: bare name -> operand.
    bare_names: Mapping[str, str] = field(default_factory=dict)

    def operands(self) -> list[str]:
        """The input fields every column reads, in first-use order. Bare
        names are included as their ``attrs.<name>`` operand and reported
        apart in :attr:`bare_names`."""

        seen: dict[str, None] = {}
        for column in self.columns:
            for operand in column.operands:
                seen.setdefault(operand, None)
        return list(seen)

    def evaluate(
        self,
        rows: Sequence[Any],
        field_states: Optional[Sequence[Optional[Mapping[str, str]]]] = None,
        budget: Optional[Budget] = None,
    ) -> Result:
        """Every column of every row (spec 5.5, 5.8).

        ``field_states``, when given, holds one mapping per row from an
        operand name (``attrs.x``, ``calc.y``) to its state; without it a
        live typed row's own ``live.fields`` states are read. A state of
        ``unavailable``, ``denied``, ``withheld`` or ``not read`` makes every
        column that reads the operand ``not read``, unevaluated;
        ``error:<kind>`` (an input ``calc.`` column) makes it that error.
        """

        budget = budget or Budget()
        count = len(rows)
        if count > budget.max_rows:
            raise _refuse(
                "too_many_rows",
                f"map: the input has {count} rows; computed columns take at most {budget.max_rows}",
                "narrow the input with where=..., then map", rows=count, limit=budget.max_rows,
            )
        if field_states is not None and len(field_states) != count:
            raise ValueError("field_states needs one entry per row")
        run = _Run(self, budget)
        results = {
            c.label: ColumnResult(c.label, [None] * count, [OK] * count, {}) for c in self.columns
        }
        for index, row in enumerate(rows):
            run.check_clock(index, count)
            states = field_states[index] if field_states is not None else None
            done: dict[str, tuple[str, Any]] = {}
            for column in self.columns:
                state, value = run.column(column, row, states, done)
                done[column.name] = (state, value)
                result = results[column.label]
                result.values[index] = value
                result.states[index] = state
                result.counts[state] = result.counts.get(state, 0) + 1
                run.charge(value, index)
        return Result(
            results, count, run.bytes, run.regex_timeouts, run.regex_spent, _clock() - run.started,
        )


def compile(compute: Mapping[str, str], columns: Optional[ColumnSpec] = None) -> Program:  # noqa: A001
    """Parse every column of ``compute``, resolve its names against the
    input's ``columns``, order the columns by dependency and refuse a cycle
    by name. Every refusal comes before any row is read."""

    spec = columns or ColumnSpec()
    if not isinstance(compute, Mapping) or not compute:
        raise _refuse("compute_empty", "map: compute names no column", 'write compute={"<name>": "<expression>"}')
    if len(compute) > MAX_COLUMNS_PER_CALL:
        raise _refuse(
            "too_many_columns",
            f"map: compute has {len(compute)} columns; one call adds at most {MAX_COLUMNS_PER_CALL}",
            "split the columns over two map calls", limit=MAX_COLUMNS_PER_CALL, columns=len(compute),
        )
    notes: list[str] = []
    named: dict[str, str] = {}
    for raw, source in compute.items():
        if not isinstance(raw, str):
            raise _refuse("bad_column_name", f"map: the column name {raw!r} is not text", "name columns [a-z][a-z0-9_]*")
        name = raw
        if name.startswith("calc."):
            name = name[len("calc."):]
            notes.append(f"column {raw!r} named calc.{name}")
        if not NAME_PATTERN.fullmatch(name):
            raise _refuse(
                "bad_column_name",
                f"map: {raw!r} is not a column name: a lowercase letter, then up to 31 lowercase letters, digits or _",
                "name columns like fill or total_pb", column=raw,
            )
        if name in named:
            raise _refuse("duplicate_column", f"map: compute names calc.{name} twice", "name each column once", column=name)
        if not isinstance(source, str):
            raise _refuse("bad_expression", f"map: calc.{name} is not a text expression", "write the expression as text", column=name)
        named[name] = source
    total = len(set(spec.calc) | set(named))
    if total > MAX_CALC_COLUMNS:
        raise _refuse(
            "too_many_calc_columns",
            f"map: the handle would carry {total} computed columns; the limit is {MAX_CALC_COLUMNS}",
            "select fewer columns into a new set, then map", limit=MAX_CALC_COLUMNS, columns=total,
        )
    siblings = frozenset(named)
    parsed: dict[str, Column] = {}
    bare: dict[str, str] = {}
    for name, source in named.items():
        parser = _parse(source, name, spec, siblings)
        for note in parser.notes:
            text = f"calc.{name}: {note}"
            if text not in notes:
                notes.append(text)
        bare.update(parser.bare)
        found: list[_Field] = []
        _fields_of(parser.tree, found)
        operands: dict[str, None] = {}
        depends: dict[str, None] = {}
        for f in found:
            if f.kind == "calc" and f.key in siblings and not (
                f.key == name and f.key in spec.calc
            ):
                # A column naming itself reads the input's column of that
                # name (an operand), never its own new value.
                depends.setdefault(f.key, None)
            else:
                operands.setdefault(f.name, None)
        parsed[name] = Column(name, source, parser.tree, tuple(operands), tuple(depends))
    return Program(_order(parsed), spec, tuple(notes), bare)


def _order(columns: dict[str, Column]) -> tuple[Column, ...]:
    """Columns in dependency order, ties in written order; a cycle is
    refused naming it."""

    ordered: list[Column] = []
    placed: set[str] = set()
    pending = list(columns)
    while pending:
        ready = [n for n in pending if all(d in placed for d in columns[n].depends)]
        if not ready:
            cycle = _find_cycle(columns, pending)
            raise _refuse(
                "compute_cycle",
                "map: computed columns read each other in a cycle: " + " -> ".join(f"calc.{n}" for n in cycle),
                "compute one of them from input fields only", columns=cycle,
            )
        for name in ready:
            ordered.append(columns[name])
            placed.add(name)
        pending = [n for n in pending if n not in placed]
    return tuple(ordered)


def _find_cycle(columns: dict[str, Column], pending: list[str]) -> list[str]:
    start = pending[0]
    path = [start]
    seen = {start: 0}
    current = start
    while True:
        nxt = next(d for d in columns[current].depends if d in pending)
        if nxt in seen:
            return path[seen[nxt]:] + [nxt]
        seen[nxt] = len(path)
        path.append(nxt)
        current = nxt


# -- evaluation ---------------------------------------------------------------

_MISSING = object()


def _row_get(container: Any, key: str) -> Any:
    """``container[key]`` when ``container`` is a mapping holding ``key``,
    else None. Only mapping lookups; never an attribute."""

    if isinstance(container, Mapping):
        return container.get(key)
    return None


def _is_typed(row: Any) -> bool:
    return isinstance(row, Mapping) and "node_id" in row and isinstance(row.get("attrs"), Mapping)


def _input_value(value: Any) -> Any:
    """A row value as the evaluator holds it."""

    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, (int, float, Decimal)):
        return to_number(value)
    if isinstance(value, (_dt.datetime, _dt.date)):
        return _to_date(value)
    raise RowError("bad_argument")


def _as_bool(value: Any) -> Optional[bool]:
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, str) and value.lower() in ("true", "false"):
        return value.lower() == "true"
    raise RowError("bad_argument")


def _text_arg(value: Any) -> str:
    if isinstance(value, str):
        return value
    raise RowError("bad_argument")


def _text_result(value: str) -> str:
    if len(value) > MAX_TEXT_CHARS:
        raise RowError("value_too_long")
    return value


def _int_arg(value: Any) -> int:
    if isinstance(value, bool):
        raise RowError("bad_argument")
    number = to_number(value)
    if not isinstance(number, int):
        raise RowError("bad_argument")
    return number


def _number_arg(value: Any) -> Number:
    if isinstance(value, (bool, _dt.datetime)):
        raise RowError("not_a_number")
    number = to_number(value)
    assert number is not None
    return number


def _text_of(value: Any) -> Optional[str]:
    """``string(x)``."""

    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return value
    if isinstance(value, (int, Decimal)):
        return _number_text(value)
    if isinstance(value, _dt.datetime):
        return _date_text(value)
    raise RowError("bad_argument")


def _compare_pair(left: Any, right: Any) -> Optional[tuple[Any, Any]]:
    """Both values in one kind by the part 1 rule, or None when they cannot
    be compared. A date next to text that is no date is ``bad_date``."""

    if isinstance(left, _dt.datetime) or isinstance(right, _dt.datetime):
        return _to_date(left), _to_date(right)
    if isinstance(left, bool) or isinstance(right, bool):
        if isinstance(left, bool) and isinstance(right, bool):
            return left, right
        other = right if isinstance(left, bool) else left
        if isinstance(other, str) and other.lower() in ("true", "false"):
            flag = other.lower() == "true"
            return (left, flag) if isinstance(left, bool) else (flag, right)
        return None
    left_num = isinstance(left, (int, Decimal))
    right_num = isinstance(right, (int, Decimal))
    if left_num and right_num:
        return left, right
    if left_num or right_num:
        other = right if left_num else left
        if not isinstance(other, str):
            return None
        try:
            number = to_number(other)
        except RowError:
            return None
        return (left, number) if left_num else (number, right)
    if isinstance(left, str) and isinstance(right, str):
        return left, right
    return None


def _less(left: Any, right: Any) -> bool:
    return left < right


class _Run:
    """The state of one :meth:`Program.evaluate` call."""

    def __init__(self, program: Program, budget: Budget) -> None:
        self.program = program
        self.budget = budget
        self.ctx = _context()
        self.started = _clock()
        self.bytes = 0
        self.regex_spent = 0.0
        self.regex_timeouts = 0
        # per-row state
        self.row: Any = None
        self.states: Optional[Mapping[str, str]] = None
        self.done: dict[str, tuple[str, Any]] = {}

    # -- budgets --
    def check_clock(self, index: int, total: int) -> None:
        elapsed = _clock() - self.started
        if elapsed > self.budget.seconds:
            raise _refuse(
                "evaluation_budget",
                f"map: computing passed {self.budget.seconds:g} s before row {index + 1} of {total}; "
                "nothing is answered from part of the set",
                "narrow the input with where=..., or compute fewer columns",
                row=index + 1, rows=total, limit_seconds=self.budget.seconds,
            )

    def charge(self, value: Any, index: int) -> None:
        if value is None:
            return
        if isinstance(value, str):
            size = len(value) if value.isascii() else len(value.encode("utf-8"))
        else:
            size = NUMBER_BYTES
        self.bytes += size
        if self.bytes > self.budget.max_bytes:
            raise _refuse(
                "map_output_too_large",
                f"map: computed values pass {self.budget.max_bytes} bytes at row {index + 1}",
                "narrow the input with where=..., or compute shorter text",
                row=index + 1, limit_bytes=self.budget.max_bytes,
            )

    # -- one column of one row --
    def column(
        self, column: Column, row: Any, states: Optional[Mapping[str, str]], done: dict[str, tuple[str, Any]],
    ) -> tuple[str, Any]:
        blocked: Optional[str] = None
        for operand in column.operands:
            state = self.input_state(row, states, operand)
            if state == NOT_READ:
                return NOT_READ, None
            if state.startswith("error:") and blocked is None:
                blocked = state
        for dep in column.depends:
            state = done[dep][0]
            if state == NOT_READ:
                return NOT_READ, None
            if state != OK and blocked is None:
                blocked = state
        if blocked is not None:
            return blocked, None
        self.row = row
        self.states = states
        self.done = done
        try:
            value = self.store(self.eval(column.tree))
        except RowError as exc:
            if exc.kind == "regex_timeout":
                self.regex_timeouts += 1
            return f"error:{exc.kind}", None
        return OK, value

    def input_state(self, row: Any, states: Optional[Mapping[str, str]], operand: str) -> str:
        # attrs.<k> and date.attrs.<k> read the same upstream field k.
        key = None
        if operand.startswith("attrs."):
            key = operand[len("attrs."):]
        elif operand.startswith("date.attrs."):
            key = operand[len("date.attrs."):]
        state = states.get(operand) if states is not None else None
        if state is None and states is not None and key is not None:
            state = states.get(f"attrs.{key}")
        if state is None and key is not None and _is_typed(row):
            live = _row_get(row, "live")
            state = _row_get(_row_get(live, "fields"), key)
        if state is None:
            return OK
        if isinstance(state, str):
            if state in READ_INPUT_STATES:
                return OK if state != "absent" else "absent"
            if state in NOT_READ_INPUT_STATES:
                return NOT_READ
            if state.startswith("error:") and state[6:] in ROW_ERROR_KINDS:
                return state
        raise _refuse(
            "internal_live_state",
            f"map: the row's field state for {operand} is {state!r}, which is not a known state",
            "this is a server fault, not a fault of the call; report it",
            operand=operand,
        )

    def store(self, value: Any) -> Any:
        if value is None or isinstance(value, bool):
            return value
        if isinstance(value, (int, Decimal)):
            return store_number(_in_range(value))
        if isinstance(value, str):
            return _text_result(value)
        if isinstance(value, _dt.datetime):
            return _date_text(value)
        raise RowError("bad_argument")

    # -- reading fields --
    def read(self, f: _Field) -> Any:
        if f.kind == "calc" and f.key in self.done:
            return self.done[f.key][1]
        if self.states is not None and self.states.get(f.name) == "absent":
            return None
        row = self.row
        if f.kind == "column":
            raw = _row_get(row, f.key)
        elif f.kind == "record":
            raw = _row_get(row, f.key)
        elif f.kind in ("attrs", "date_attrs"):
            container = _row_get(row, "attrs") if _is_typed(row) else row
            raw = _row_get(container, f.key)
        else:  # calc read from the input, date.<name>
            raw = _row_get(row, f.name)
        if f.kind in ("date", "date_attrs"):
            return _to_date(_input_value(raw)) if not isinstance(raw, str) else _to_date(raw)
        return _input_value(raw)

    # -- the tree walker --
    def eval(self, node: Any) -> Any:
        if isinstance(node, _Lit):
            return node.value
        if isinstance(node, _Field):
            return self.read(node)
        if isinstance(node, _Chain):
            value = self.eval(node.first)
            for op, operand in node.rest:
                value = self.arith(op, value, self.eval(operand))
            return value
        if isinstance(node, _Neg):
            value = self.eval(node.operand)
            if value is None:
                return None
            return self.guard(lambda: self.negate(_number_arg(value)))
        if isinstance(node, _Cmp):
            return self.compare(node.op, self.eval(node.left), self.eval(node.right))
        if isinstance(node, _Logic):
            return self.logic(node)
        if isinstance(node, _Not):
            value = _as_bool(self.eval(node.operand))
            return None if value is None else not value
        if isinstance(node, _Call):
            return self.call(node)
        raise TypeError(f"unknown node {type(node).__name__}")

    def guard(self, op: Callable[[], Any]) -> Any:
        """One arithmetic operation: every decimal signal and arithmetic
        error becomes a row error, and the result's range is checked."""

        try:
            result = op()
        except (decimal.DecimalException, ArithmeticError) as exc:
            raise RowError(_signal_kind(exc)) from None
        if isinstance(result, (int, Decimal)) and not isinstance(result, bool):
            _in_range(result)
        return result

    def negate(self, value: Number) -> Number:
        return -value if isinstance(value, int) else self.ctx.minus(value)

    def arith(self, op: str, left: Any, right: Any) -> Any:
        if left is None or right is None:
            return None
        if type(left) is int and type(right) is int and op in "+-*":
            # The common case, exact, with the same range check.
            result = left + right if op == "+" else (left - right if op == "-" else left * right)
            if -NUMBER_LIMIT < result < NUMBER_LIMIT:
                return result
            raise RowError("number_too_large")
        a = _number_arg(left)
        b = _number_arg(right)
        if op in "/%" and b == 0:
            raise RowError("division_by_zero")
        ctx = self.ctx

        def run() -> Number:
            if isinstance(a, int) and isinstance(b, int) and op != "/":
                if op == "+":
                    return a + b
                if op == "-":
                    return a - b
                if op == "*":
                    return a * b
                remainder = abs(a) % abs(b)
                return -remainder if a < 0 else remainder
            x, y = Decimal(a), Decimal(b)
            if op == "+":
                return ctx.add(x, y)
            if op == "-":
                return ctx.subtract(x, y)
            if op == "*":
                return ctx.multiply(x, y)
            if op == "/":
                return ctx.divide(x, y)
            return ctx.remainder(x, y)

        return self.guard(run)

    def compare(self, op: str, left: Any, right: Any) -> Optional[bool]:
        if left is None or right is None:
            return None
        pair = _compare_pair(left, right)
        if pair is None:
            # Decision Q5: kinds that cannot be compared (true == 1,
            # attrs.n < 'abc') are a row error, as in min and max, never null.
            raise RowError("bad_argument")
        a, b = pair
        if op == "==":
            return a == b
        if op == "!=":
            return a != b
        if op == "<":
            return a < b
        if op == "<=":
            return a <= b
        if op == ">":
            return a > b
        return a >= b

    def logic(self, node: _Logic) -> Optional[bool]:
        unknown = False
        stop = node.op == "or"
        for operand in node.operands:
            value = _as_bool(self.eval(operand))
            if value is None:
                unknown = True
            elif value is stop:
                return stop
        return None if unknown else (not stop)

    # -- functions --
    def call(self, node: _Call) -> Any:
        name = node.name
        args = node.args
        if name == "if":
            condition = _as_bool(self.eval(args[0]))
            return self.eval(args[1] if condition is True else args[2])
        if name == "coalesce":
            for arg in args:
                value = self.eval(arg)
                if value is not None:
                    return value
            return None
        if name == "is_null":
            return self.eval(args[0]) is None
        values = [self.eval(arg) for arg in args]
        if name == "concat":
            return _text_result("".join(_text_of(v) or "" for v in values))
        if any(v is None for v in values):
            return None
        return _apply(self, name, values, node)


def _apply(run: _Run, name: str, values: list[Any], node: _Call) -> Any:
    """The functions whose null argument gives null."""

    ctx = run.ctx
    if name == "abs":
        x = _number_arg(values[0])
        return run.guard(lambda: abs(x) if isinstance(x, int) else ctx.abs(x))
    if name == "round":
        x = _number_arg(values[0])
        digits = 0
        if len(values) == 2:
            digits = _int_arg(values[1])
            if not ROUND_DIGITS[0] <= digits <= ROUND_DIGITS[1]:
                raise RowError("bad_argument")
        return run.guard(lambda: _round(x, digits))
    if name in ("floor", "ceil"):
        x = _number_arg(values[0])
        if isinstance(x, int):
            return x
        rounding = decimal.ROUND_FLOOR if name == "floor" else decimal.ROUND_CEILING
        return run.guard(lambda: int(x.to_integral_value(rounding=rounding, context=_WIDE)))
    if name in ("min", "max"):
        best = values[0]
        for value in values[1:]:
            pair = _compare_pair(best, value)
            if pair is None:
                raise RowError("bad_argument")
            a, b = pair
            best = (b if _less(b, a) else a) if name == "min" else (b if _less(a, b) else a)
        return best
    if name == "lower":
        return _text_result(_text_arg(values[0]).lower())
    if name == "upper":
        return _text_result(_text_arg(values[0]).upper())
    if name == "len":
        return len(_text_arg(values[0]))
    if name == "substr":
        text = _text_arg(values[0])
        start = _int_arg(values[1])
        if start < 1:
            raise RowError("bad_argument")
        if len(values) == 3:
            length = _int_arg(values[2])
            if length < 0:
                raise RowError("bad_argument")
            return text[start - 1:start - 1 + length]
        return text[start - 1:]
    if name == "split_part":
        text = _text_arg(values[0])
        sep = _text_arg(values[1])
        n = _int_arg(values[2])
        if sep == "" or n == 0:
            raise RowError("bad_argument")
        pieces = text.split(sep)
        if abs(n) > len(pieces):
            return None
        return pieces[n - 1] if n > 0 else pieces[n]
    if name == "starts_with":
        return _text_arg(values[0]).startswith(_text_arg(values[1]))
    if name == "ends_with":
        return _text_arg(values[0]).endswith(_text_arg(values[1]))
    if name == "contains":
        return _text_arg(values[1]) in _text_arg(values[0])
    if name == "regex":
        assert node.pattern is not None
        return _match(run, node.pattern, _text_arg(values[0]))
    if name == "number":
        value = values[0]
        if isinstance(value, (bool, _dt.datetime)):
            raise RowError("not_a_number")
        return to_number(value)
    if name == "string":
        return _text_result(_text_of(values[0]) or "")
    if name == "to_unit":
        x = _number_arg(values[0])
        source, target = UNITS[values[1]], UNITS[values[2]]
        scaled = run.arith("*", x, source)
        return run.arith("/", scaled, target)
    if name == "days_between":
        a, b = _to_date(values[0]), _to_date(values[1])
        assert a is not None and b is not None
        micro = (b - a) // _dt.timedelta(microseconds=1)
        return run.guard(lambda: ctx.divide(Decimal(micro), Decimal(86_400_000_000)))
    raise TypeError(f"unknown function {name}")


def _round(x: Number, digits: int) -> Number:
    """Halves away from zero; ``digits`` may be negative (tens, hundreds).

    A value with no more fractional digits than asked needs no rounding and
    is returned unchanged (R7): ``round(1e37, 10)`` is ``1e37``, never the
    ``InvalidOperation`` a quantize past 34 digits would raise."""

    value = Decimal(x)
    if value.as_tuple().exponent >= -digits:  # type: ignore[operator]
        return _whole(value) if digits <= 0 else x
    quantum = Decimal(1).scaleb(-digits, context=_WIDE)
    rounded = value.quantize(quantum, rounding=decimal.ROUND_HALF_UP, context=_WIDE)
    return _whole(rounded) if digits <= 0 or rounded == rounded.to_integral_value(context=_WIDE) else rounded


def _match(run: _Run, pattern: _Pattern, text: str) -> Optional[str]:
    if len(text) > MAX_REGEX_INPUT_BYTES or (
        len(text) * 4 > MAX_REGEX_INPUT_BYTES and len(text.encode("utf-8")) > MAX_REGEX_INPUT_BYTES
    ):
        raise RowError("value_too_long")
    left = run.budget.regex_seconds - run.regex_spent
    if left <= 0:
        raise _regex_budget(run)
    timeout = min(REGEX_MATCH_SECONDS, left)
    started = _clock()
    try:
        found = pattern.compiled.search(text, timeout=timeout)
    except TimeoutError:
        run.regex_spent += max(_clock() - started, timeout)
        if timeout < REGEX_MATCH_SECONDS or run.regex_spent >= run.budget.regex_seconds:
            raise _regex_budget(run) from None
        raise RowError("regex_timeout") from None
    run.regex_spent += _clock() - started
    if run.regex_spent > run.budget.regex_seconds:
        raise _regex_budget(run)
    if found is None:
        return None
    value = found.group(pattern.group)
    return None if value is None else _text_result(value)


def _regex_budget(run: _Run) -> ExpressionRefused:
    return _refuse(
        "regex_budget",
        f"map: regex matching took past {run.budget.regex_seconds:g} s in this call; "
        "nothing is answered from part of the set",
        "narrow the input with where=..., or use starts_with, ends_with or contains",
        limit_seconds=run.budget.regex_seconds, regex_timeouts=run.regex_timeouts,
    )
