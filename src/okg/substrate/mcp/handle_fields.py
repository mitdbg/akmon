"""A record's title, path and date, read from its catalog declarations.

Per PACT `read-model-s1-catalog-fields` (design `read-model` section 3.6,
decision D11). A result handle reads
`<id> · <title> · <path> · <date> · <type>`; this module decides the three
middle fields for one record from its subtype's `okg.subtypes.metadata` and
its attributes. Search rows carry its values as `handle` (slice 2, PACT
`read-model-s2-handle`, `search._with_handles`), which the text view prints
as the handle line; the `where` predicate (slice 3) is its next reader.

The catalog says which attribute holds each field:

* ``okg_title_from`` -> ``metadata["title_from"]``
* ``okg_path_from`` -> ``metadata["path_from"]``
* ``okg_created_at_from`` -> ``metadata["created_at_from"]`` and
  ``okg_last_modified_at_from`` -> ``metadata["last_modified_at_from"]``,
  the existing semantic-timestamp declarations.

Rules, each reported in the field's ``status`` so a reader can tell "none"
from "hidden" from "unreadable":

* **Title.** The declared attribute's value. When the subtype declares no
  title, today's fixed list `PARENT_TITLE_ATTRS` (the passage rollup's
  parent-title rule, applied by that module's own function) and nothing past
  it; no match means ``None``, "no title". A declared attribute that is
  missing or empty is ``None`` too: the fixed list is NOT consulted, because
  the catalog said where the title lives. The value is its first line, cut to
  `PARENT_TITLE_MAX_CHARS`, as parent titles are today.
* **Path.** The declared attribute's value, stripped, else ``None``. Never
  inferred from `member_path`, `doc_path` or any other attribute the subtype
  did not declare.
* **Date.** The attribute named by ``created_at_from``, or, when only
  ``last_modified_at_from`` is declared, that one. Normalized to an ISO-8601
  date-time string (`datetime.isoformat`); a value with no timezone stays
  without one, never assumed UTC. ``None`` when nothing is declared, the
  value is absent, masked, or cannot be parsed, with ``status`` saying which.

Values are read from whatever attrs the caller passes. A caller serving a
read passes the reader's effective, Nomos-projected attrs. A masked value
arrives as `MASK_TOKEN`; a dropped class such as free text arrives as a
missing key on attrs stamped with `REDACTION_MARKER`. Both are reported as
``masked``, never as a title and never as ``absent``: on a redacted record a
missing declared key reads as ``masked`` even if the record never had it, so
the status does not reveal which. A pseudonymized identifier is what the
reader is meant to see, and is reported as ``declared``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, tzinfo
from email.utils import parsedate_to_datetime
import re
from typing import Any, Mapping, Optional

from okg.substrate.mcp.passage_rollup import (
    PARENT_TITLE_ATTRS,
    PARENT_TITLE_MAX_CHARS,
    _first_line,
    _parent_title,
)
from okg.substrate.nomos.read_masking import MASK_TOKEN, REDACTION_MARKER

__all__ = [
    "FIELD_ABSENT",
    "FIELD_DECLARED",
    "FIELD_FALLBACK_LIST",
    "FIELD_MASKED",
    "FIELD_NOT_DECLARED",
    "FIELD_NOT_TEXT",
    "FIELD_STATUSES",
    "FIELD_UNPARSEABLE",
    "HandleField",
    "HandleFields",
    "NamedDate",
    "PRECISION_DATE",
    "PRECISION_DATETIME",
    "DATE_MEANINGS",
    "PARENT_TITLE_ATTRS",
    "handle_fields",
    "normalize_handle_date",
    "resolve_named_date",
]

# Initial common meanings. Other domain dates remain explicit attrs fields;
# callers must not map them to one of these merely to obtain a date value.
DATE_MEANINGS = {
    "created": "created_at_from",
    "modified": "last_modified_at_from",
    "published": "published_at_from",
    "resolved": "resolved_at_from",
    "effective": "effective_at_from",
}

#: The value came from the attribute the catalog declares.
FIELD_DECLARED = "declared"
#: Title only: the subtype declares no title; the value came from the fixed
#: `PARENT_TITLE_ATTRS` list.
FIELD_FALLBACK_LIST = "fallback_list"
#: The subtype declares no attribute for this field (and, for a title, no
#: fixed-list attribute holds one). `value` is None.
FIELD_NOT_DECLARED = "not_declared"
#: The declared attribute is missing, null, or an empty string.
FIELD_ABSENT = "absent"
#: The read filter hid the declared attribute: it holds the mask token, or
#: it is missing from attrs stamped with `REDACTION_MARKER` (a dropped class).
FIELD_MASKED = "masked"
#: Title or path: the declared attribute holds something other than a string.
FIELD_NOT_TEXT = "not_text"
#: Date only: the declared attribute holds a value that is not a date.
FIELD_UNPARSEABLE = "unparseable"

FIELD_STATUSES = (
    FIELD_DECLARED,
    FIELD_FALLBACK_LIST,
    FIELD_NOT_DECLARED,
    FIELD_ABSENT,
    FIELD_MASKED,
    FIELD_NOT_TEXT,
    FIELD_UNPARSEABLE,
)


@dataclass(frozen=True)
class HandleField:
    """One handle field: its value, the attribute read, and why."""

    value: Optional[str]
    #: The attribute the value came from, or that was declared and yielded
    #: nothing. None only when no attribute applies (`not_declared`).
    attr: Optional[str]
    status: str


@dataclass(frozen=True)
class HandleFields:
    """The title, path and date of one record."""

    title: HandleField
    path: HandleField
    date: HandleField
    #: Meaning of the display date, independent of its value/status. The
    #: existing values() protocol intentionally remains three fields.
    date_meaning: Optional[str] = None

    def values(self) -> dict[str, Optional[str]]:
        """`{"title", "path", "date"}` -> value (None = none to show)."""
        return {
            "title": self.title.value,
            "path": self.path.value,
            "date": self.date.value,
        }


@dataclass(frozen=True)
class NamedDate:
    """A specifically requested date with source, meaning, timezone and precision.

    ``precision`` is ``"date"`` when the source named a calendar day and no
    time (ISO ``2026-10-03``, or an ISO week date with its day such as
    ``2026-W40-6``), ``"datetime"`` when it named a time of day, and None when
    there is no value. A date-precision ``value`` is that day's midnight,
    written so it sorts with datetimes; it is NOT an instant at midnight. It
    carries an offset only when the source gave one with the day (the XML
    Schema date form ``2026-10-03-07:00``: a day in that zone).
    ``timezone`` is ``"aware"`` when the source gave an offset, ``"naive"``
    when it gave none or said the offset is unknown (RFC 3339 ``-00:00`` or
    ``-00``, RFC 2822 ``-0000``), and None when there is no value. Comparison consumers must define or refuse a comparison between a
    date-precision or naive value and an aware instant; they must not treat
    the midnight wall time as UTC.
    """

    value: Optional[str]
    attr: Optional[str]
    status: str
    meaning: str
    timezone: Optional[str]
    precision: Optional[str]


#: An RFC 2822 date as an email ``Date:`` header writes it: optional weekday,
#: day, month name, year, time, then an optional zone and, only after a zone,
#: an optional comment such as ``(PDT)``. A numeric zone may carry a colon
#: (``-07:00``), which real mail archives write although RFC 2822 does not.
#: ASCII only: ``\d`` would also match other scripts' digits.
_EMAIL_DATE = re.compile(
    r"""^(?:(?P<wday>[A-Za-z]{3}),?\s+)?
    (?P<day>\d{1,2})\s+(?P<mon>[A-Za-z]{3})\s+(?P<year>\d{2,4})\s+
    (?P<hh>\d{1,2}):(?P<mm>\d{2})(?::(?P<ss>\d{2}))?
    (?:\s+(?P<zone>[+-]\d{2}:?\d{2}|[A-Za-z]+)(?:\s*\([^()]*\))?)?$""",
    re.VERBOSE | re.ASCII,
)
#: Zone names RFC 2822 defines (section 4.3), plus ``UTC``. Any other name,
#: such as ``CEST``, is refused: its offset is not knowable from the name.
_EMAIL_ZONE_NAMES = frozenset(
    ("UT", "UTC", "GMT", "EST", "EDT", "CST", "CDT", "MST", "MDT", "PST", "PDT"),
)
_WEEKDAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
#: The ISO-8601 forms accepted, ASCII digits only. A calendar date
#: (``2026-10-03`` or ``20261003``) or a week date with its day
#: (``2026-W40-6`` or ``2026W406``), then one of:
#:
#: * nothing (a day, precision date);
#: * a time after exactly ``T`` or one space, then optionally an offset,
#:   which may follow one space (``2026-10-03 12:00:00 +0000``, as Git
#:   ``--date=iso``, Ruby and Go write it). The offset is ``Z``, ``+HH``,
#:   basic ``+HHMM[SS]`` or extended ``+HH:MM[:SS]``; a mix such as
#:   ``+05:3000`` is refused;
#: * only after an EXTENDED day, ``Z`` or ``+HH:MM`` with no space: the
#:   XML Schema date form (``2026-10-03-07:00``), a day in that zone, never a
#:   clock time; its offset may not exceed 14:00. Compact or hour-only forms
#:   (``20261003-1230``, a build stamp; ``2026-10-03-07``) are refused.
#:
#: ``datetime.fromisoformat`` alone takes ANY character as the separator and
#: so reads ``2026-10-03-07:00`` as 07:00; it only ever sees the wall time
#: this pattern has already split off.
_ISO_OFFSET_FORM = (
    r"Z|[+-]\d{2}(?::\d{2}(?::\d{2}(?:[.,]\d+)?)?|\d{2}(?:\d{2}(?:[.,]\d+)?)?)?"
)
_ISO_FORM = re.compile(
    rf"""^(?P<date>\d{{4}}-\d{{2}}-\d{{2}}|\d{{8}}|\d{{4}}-W\d{{2}}-\d|\d{{4}}W\d{{3}})
    (?:[T\x20](?P<time>\d{{2}}(?::?\d{{2}}(?::?\d{{2}}(?:[.,]\d+)?)?)?)
        (?:\x20?(?P<offset>{_ISO_OFFSET_FORM}))?
      |(?P<day_offset>Z|[+-]\d{{2}}:\d{{2}})
    )?$""",
    re.VERBOSE | re.ASCII,
)
_ISO_OFFSET_PARTS = re.compile(
    r"([+-])(\d{2}):?(\d{2})?:?(\d{2})?(?:[.,](\d+))?$", re.ASCII,
)
#: The XML Schema date form allows offsets from -14:00 to +14:00.
_DAY_OFFSET_MAX = timedelta(hours=14)

PRECISION_DATE = "date"
PRECISION_DATETIME = "datetime"


def _declared_attr(metadata: Mapping[str, Any], key: str) -> Optional[str]:
    value = metadata.get(key)
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _missing_status(attrs: Mapping[str, Any]) -> str:
    """`masked` when a redacted read may have dropped the key, else `absent`."""
    if attrs.get(REDACTION_MARKER) is True:
        return FIELD_MASKED
    return FIELD_ABSENT


def _declared_text(
    attrs: Mapping[str, Any], attr: str,
) -> tuple[Optional[str], str]:
    """`(stripped text, status)` for a declared title or path attribute."""
    raw = attrs.get(attr)
    if raw is None:
        return None, _missing_status(attrs)
    if not isinstance(raw, str):
        return None, FIELD_NOT_TEXT
    stripped = raw.strip()
    if not stripped:
        return None, FIELD_ABSENT
    if stripped == MASK_TOKEN:
        return None, FIELD_MASKED
    return stripped, FIELD_DECLARED


def _iso_offset(text: Optional[str]) -> tuple[bool, Optional[tzinfo]]:
    """`(valid, tzinfo)` for an offset `_ISO_FORM` matched; None = none or unknown."""
    if text is None:
        return True, None
    if text == "Z":
        return True, timezone.utc
    parts = _ISO_OFFSET_PARTS.match(text)
    if parts is None:
        return False, None
    sign, hours, minutes, seconds, fraction = parts.groups()
    hours, minutes, seconds = int(hours), int(minutes or 0), int(seconds or 0)
    micros = int((fraction or "0")[:6].ljust(6, "0"))
    if hours > 23 or minutes > 59 or seconds > 59:
        return False, None
    delta = timedelta(hours=hours, minutes=minutes, seconds=seconds, microseconds=micros)
    if sign == "-" and not delta and not int(fraction or "0"):
        # RFC 3339 -00:00 (any length): the local offset is unknown.
        return True, None
    return True, timezone(-delta if sign == "-" else delta)


def _parse_iso(text: str) -> Optional[tuple[datetime, str]]:
    match = _ISO_FORM.match(text)
    if match is None:
        return None
    day_offset = match["day_offset"]
    if day_offset is not None:
        # XML Schema date form: extended day only, offset within 14:00.
        if "-" not in match["date"]:
            return None
        valid, tz = _iso_offset(day_offset)
        if valid and tz is not None and abs(tz.utcoffset(None)) > _DAY_OFFSET_MAX:
            return None
    else:
        valid, tz = _iso_offset(match["offset"])
    if not valid:
        return None
    wall = match["date"] if match["time"] is None else f"{match['date']}T{match['time']}"
    try:
        parsed = datetime.fromisoformat(wall)
    except ValueError:
        return None
    precision = PRECISION_DATE if match["time"] is None else PRECISION_DATETIME
    return parsed.replace(tzinfo=tz), precision


def _rfc2822_year(digits: str) -> int:
    """RFC 2822 section 4.3: 00-49 -> 20xx, 50-99 -> 19xx, three digits + 1900."""
    year = int(digits)
    if len(digits) == 2:
        return year + (2000 if year < 50 else 1900)
    if len(digits) == 3:
        return year + 1900
    return year


def _parse_email(text: str) -> Optional[tuple[datetime, str]]:
    match = _EMAIL_DATE.match(text)
    if match is None:
        return None
    wday = match["wday"]
    # The weekday is redundant with the date. Its name must be a weekday, but
    # a weekday that contradicts the date is ignored: the numeric date wins
    # (8 Gmail dates in a private deployment's archive carry a wrong weekday).
    if wday is not None and wday.lower() not in _WEEKDAYS:
        return None
    zone = match["zone"]
    if zone is not None:
        if zone[0] in "+-":
            digits = zone.replace(":", "")
            if int(digits[1:3]) > 23 or int(digits[3:5]) > 59:
                return None
            # parsedate_to_datetime drops a colon offset to a naive time;
            # hand it the RFC 2822 form so the offset is kept.
            start, end = match.span("zone")
            text = text[:start] + digits + text[end:]
        elif zone.upper() not in _EMAIL_ZONE_NAMES:
            return None
    # The stdlib maps two-digit years with its own pivot (69); RFC 2822 does
    # not. Hand it the four-digit year. The year precedes the zone, so the
    # zone rewrite above did not move it.
    start, end = match.span("year")
    text = text[:start] + str(_rfc2822_year(match["year"])) + text[end:]
    try:
        parsed = parsedate_to_datetime(text)
    except (TypeError, ValueError, IndexError, OverflowError):
        return None
    return parsed, PRECISION_DATETIME


def _parse_date(raw: Any) -> Optional[tuple[datetime, str]]:
    """`(datetime, precision)` for a date string, or None when refused."""
    if not isinstance(raw, str):
        return None
    text = raw.strip()
    if not text:
        return None
    return _parse_iso(text) or _parse_email(text)


def normalize_handle_date(raw: Any) -> Optional[str]:
    """`raw` as an ISO-8601 date-time string, or None when it is not a date.

    Accepts the ISO-8601 forms `_ISO_FORM` lists (a day alone reads as
    midnight with precision date, an extended day followed by `Z` or `+HH:MM`
    within 14:00 is that day in that zone, a trailing `Z` is UTC, a `-00:00`
    or `-00` offset is unknown, one space may precede a time's offset)
    or an RFC 2822 string such as an email `Date:` header (two-digit years
    00-49 are 20xx and 50-99 are 19xx, three-digit years add 1900). Refused:
    numbers and digit strings other than an eight-digit `YYYYMMDD` day (an
    epoch could be seconds or milliseconds and this does not guess), an ISO
    week without a day, a date/time separator other than `T` or one space,
    non-ASCII digits, invalid calendar dates or offset components, an email
    weekday that is not a day name, and an email zone name RFC 2822 does not
    define. An email weekday that contradicts the date is ignored (the
    numeric date wins); an email offset written with a colon is kept. A
    value without a timezone keeps none.
    """
    parsed = _parse_date(raw)
    return None if parsed is None else parsed[0].isoformat()


def _title(metadata: Mapping[str, Any], attrs: Mapping[str, Any]) -> HandleField:
    attr = _declared_attr(metadata, "title_from")
    if attr is None:
        title, title_attr = _parent_title(attrs)
        if title is None:
            return HandleField(None, None, FIELD_NOT_DECLARED)
        return HandleField(title, title_attr, FIELD_FALLBACK_LIST)
    text, status = _declared_text(attrs, attr)
    if text is None:
        return HandleField(None, attr, status)
    return HandleField(
        _first_line(text, max_chars=PARENT_TITLE_MAX_CHARS), attr, status,
    )


def _path(metadata: Mapping[str, Any], attrs: Mapping[str, Any]) -> HandleField:
    attr = _declared_attr(metadata, "path_from")
    if attr is None:
        return HandleField(None, None, FIELD_NOT_DECLARED)
    text, status = _declared_text(attrs, attr)
    return HandleField(text, attr, status)


def resolve_named_date(
    metadata: Optional[Mapping[str, Any]],
    attrs: Optional[Mapping[str, Any]],
    meaning: str,
) -> NamedDate:
    """Read exactly one catalog-declared date; never substitute another.

    Unknown meanings raise ValueError. Wire consumers must translate that
    to a structured refusal listing DATE_MEANINGS, rather than exposing an
    internal error. Predicates/sort/grouping use this resolver, not the
    generic display date. ISO and RFC 2822 values retain timezone offsets;
    ISO date-only values have precision "date"; values with no offset or an
    unknown one (-00:00, -0000) are naive.
    """
    if meaning not in DATE_MEANINGS:
        raise ValueError(f"unknown date meaning {meaning!r}; valid meanings: {', '.join(DATE_MEANINGS)}")
    meta = metadata if isinstance(metadata, Mapping) else {}
    values = attrs if isinstance(attrs, Mapping) else {}
    attr = _declared_attr(meta, DATE_MEANINGS[meaning])
    if attr is None:
        return NamedDate(None, None, FIELD_NOT_DECLARED, meaning, None, None)
    raw = values.get(attr)
    if raw is None:
        return NamedDate(None, attr, _missing_status(values), meaning, None, None)
    if isinstance(raw, str) and not raw.strip():
        return NamedDate(None, attr, FIELD_ABSENT, meaning, None, None)
    if isinstance(raw, str) and raw.strip() == MASK_TOKEN:
        return NamedDate(None, attr, FIELD_MASKED, meaning, None, None)
    parsed = _parse_date(raw)
    if parsed is None:
        return NamedDate(None, attr, FIELD_UNPARSEABLE, meaning, None, None)
    when, precision = parsed
    zone_state = "aware" if when.utcoffset() is not None else "naive"
    return NamedDate(
        when.isoformat(), attr, FIELD_DECLARED, meaning, zone_state, precision,
    )


def handle_fields(
    metadata: Optional[Mapping[str, Any]],
    attrs: Optional[Mapping[str, Any]],
) -> HandleFields:
    """The title, path and date of one record.

    `metadata` is the record's subtype metadata (a row of
    `okg.subtypes.metadata` at the read's catalog version, or
    `ComposedCatalog.subtype_metadata[subtype]`); None or empty means the
    subtype declares nothing. `attrs` is the record's attributes as the
    reader may see them; None reads as no attributes.
    """
    meta: Mapping[str, Any] = metadata if isinstance(metadata, Mapping) else {}
    values: Mapping[str, Any] = attrs if isinstance(attrs, Mapping) else {}
    meaning = (
        "created" if _declared_attr(meta, "created_at_from") is not None
        else "modified" if _declared_attr(meta, "last_modified_at_from") is not None
        else None
    )
    named_date = resolve_named_date(meta, values, meaning) if meaning else None
    return HandleFields(
        title=_title(meta, values),
        path=_path(meta, values),
        date=(HandleField(named_date.value, named_date.attr, named_date.status)
              if named_date else HandleField(None, None, FIELD_NOT_DECLARED)),
        date_meaning=meaning,
    )
