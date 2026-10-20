"""calendar_ics source — reads `.ics` file/URL with strict-allowlist filter.

Per `refine-academic-pi-with-comms-and-calendar` Tier A: emits
`CalendarEvent` + `CalendarFeed` + `Deadline` (events tagged
`okg-deadline:`) from an operator-provided iCalendar feed.

Strict-allowlist filter: only events with `okg:` substring in
DESCRIPTION/SUMMARY OR matching one of `recurring_patterns`
regexes emit nodes. Non-matching events are skipped with zero
metadata retention — the operator's calendar can contain
unrelated events (dentist, family) without leaking into the
graph.

Per `fix-calendar-ics-rrule`: parsing is `icalendar`-backed and
recurring events are expanded into per-occurrence rows via
`dateutil.rrule` within a bounded window (default trailing 12 /
leading 18 months), honoring EXDATE, COUNT/UNTIL, and
timezone/DST transitions. Unparseable components are counted on
the feed node and logged loudly, never skipped silently.
"""
from __future__ import annotations

import hashlib
import logging
import re
import urllib.request
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Optional

from dateutil.relativedelta import relativedelta
from dateutil.rrule import rrulestr
from dateutil.tz import UTC

from okg.substrate.library.sources._url_safety import (
    assert_public_http_url,
    open_validated_url,
)
from okg.substrate.library.sources.base import (
    EdgeFact,
    NodeFact,
    ProgressMarker,
    SourceRun,
)

_log = logging.getLogger(__name__)


_DEFAULT_RECURRING_PATTERNS = [
    r"\blab\s+meeting\b",
    r"\bgroup\s+meeting\b",
    # 1:1 and 1-on-1 variants
    r"\b1[:\-\s]+1\b",
    r"\b1\s+on\s+1\b",
    r"\boffice\s+hours\b",
]

_TITLE_KIND_PATTERNS = [
    (re.compile(r"\blab\s+meeting\b", re.I), "lab_meeting"),
    (re.compile(r"\bgroup\s+meeting\b", re.I), "group_meeting"),
    (re.compile(r"\b1[:\-\s]+1\b|\b1\s+on\s+1\b", re.I), "one_on_one"),
    (re.compile(r"\boffice\s+hours\b", re.I), "office_hours"),
    (re.compile(r"\bcommittee\b", re.I), "committee"),
    (re.compile(r"\bcolloqu", re.I), "colloquium"),
    (re.compile(r"\bseminar\b", re.I), "seminar"),
    (re.compile(r"\bsponsor\b", re.I), "sponsor_call"),
    (re.compile(r"\btravel\b|\btrip\b", re.I), "travel"),
    (re.compile(r"\bconference\b", re.I), "conference_day"),
]


class CalendarIcsSource:
    """Source adapter for `.ics` calendar feeds.

    Params (operator-configurable via source_registry.yaml):

      - calendar_ics_path: path to local `.ics` file
      - calendar_ics_url:  HTTPS URL to `.ics` feed
      - feed_name:         operator-friendly feed name
      - feed_id:           stable identity (defaults to hash)
      - recurring_patterns: list of regex strings; events
        matching any are allowlisted (in addition to `okg:` tag)
      - expand_past_months / expand_future_months: recurrence
        expansion window relative to now (defaults 12 / 18)
      - expand_window_start / expand_window_end: ISO dates that
        pin the window explicitly (tests, replays); both required
        together and they override the month-based window
    """

    name: str = "calendar_ics"
    profile: str = "discovery_crawl"

    def __init__(
        self,
        *,
        calendar_ics_path: Optional[str] = None,
        calendar_ics_url: Optional[str] = None,
        feed_name: str = "default",
        feed_id: Optional[str] = None,
        recurring_patterns: Optional[list[str]] = None,
        expand_past_months: int = 12,
        expand_future_months: int = 18,
        expand_window_start: Optional[str] = None,
        expand_window_end: Optional[str] = None,
        ownership_id: str = "calendar_ics",
        **_ignored: Any,
    ) -> None:
        self.calendar_ics_path = calendar_ics_path
        self.calendar_ics_url = calendar_ics_url
        self.feed_name = feed_name
        self.feed_id = feed_id
        self.recurring_patterns = recurring_patterns
        self.expand_past_months = expand_past_months
        self.expand_future_months = expand_future_months
        self.expand_window_start = expand_window_start
        self.expand_window_end = expand_window_end
        self.ownership_id = ownership_id

    def run(
        self, run_id: str, *, mode: str = "cursor", **_ignored: Any,
    ) -> SourceRun:
        if self.calendar_ics_path is None and self.calendar_ics_url is None:
            return SourceRun(facts=iter([]), completed_scope=True)

        ics_text = self._fetch_ics()
        feed_id = self.feed_id or self._derive_feed_id()
        patterns = [
            re.compile(p, re.I)
            for p in (self.recurring_patterns or _DEFAULT_RECURRING_PATTERNS)
        ]

        return SourceRun(
            facts=self._stream(ics_text, feed_id, patterns),
            completed_scope=True,
        )

    def _fetch_ics(self) -> str:
        if self.calendar_ics_path:
            # Local file: read directly (operator's own file; not a
            # network fetch, so the SSRF guard doesn't apply).
            return Path(self.calendar_ics_path).read_text(encoding="utf-8")
        # Remote feed: SSRF-guard the operator-supplied URL before
        # fetching. Per `validate-source-fetch-urls`.
        assert_public_http_url(self.calendar_ics_url)
        req = urllib.request.Request(
            self.calendar_ics_url,
            headers={"User-Agent": "okg-calendar-ics/1.0"},
        )
        with open_validated_url(req, timeout=30) as resp:
            return resp.read().decode("utf-8", errors="replace")

    def _derive_feed_id(self) -> str:
        ref = self.calendar_ics_url or self.calendar_ics_path or self.feed_name
        h = hashlib.sha256(ref.encode("utf-8")).hexdigest()[:16]
        return f"feed:ics:{h}"

    def _window(self) -> tuple[datetime, datetime]:
        if self.expand_window_start and self.expand_window_end:
            start = datetime.fromisoformat(self.expand_window_start)
            end = datetime.fromisoformat(self.expand_window_end)
        else:
            now = datetime.now(UTC)
            start = now - relativedelta(months=self.expand_past_months)
            end = now + relativedelta(months=self.expand_future_months)
        if start.tzinfo is None:
            start = start.replace(tzinfo=UTC)
        if end.tzinfo is None:
            end = end.replace(tzinfo=UTC)
        return start, end

    def _stream(
        self,
        ics_text: str,
        feed_id: str,
        patterns: list[re.Pattern],
    ) -> Iterable[Any]:
        events, unparseable = _parse_vevents(ics_text)
        if unparseable:
            # Loud by contract: a corrupt component is reported,
            # never silently dropped (silent under-emission is the
            # bug this parser replaced).
            _log.warning(
                "calendar_ics: %d unparseable VEVENT component(s) in "
                "feed %s (%s); valid events still emit",
                unparseable,
                self.feed_name,
                self.calendar_ics_url or self.calendar_ics_path,
            )

        feed_node_id = feed_id
        yield NodeFact(
            node_id=feed_node_id,
            subtype="calendar_feed",
            attrs={
                "feed_id": feed_id,
                "name": self.feed_name,
                "source_kind": (
                    "ics_url" if self.calendar_ics_url else "ics_file"
                ),
                "external_ref": (
                    self.calendar_ics_url or self.calendar_ics_path
                ),
                "unparseable_components": unparseable,
            },
            source_record_id={"feed_id": feed_id},
            source_revision={"content_sha": _sha(ics_text)},
        )
        yield ProgressMarker(record_id=feed_id, fingerprint=_sha(ics_text))

        window_start, window_end = self._window()
        # RECURRENCE-ID overrides displace the matching expanded
        # occurrence of their series and emit as their own row.
        overridden = {
            (ev["uid"], _dt_key(ev["recurrence_id"]))
            for ev in events
            if ev["recurrence_id"] is not None
        }

        for event in events:
            if not _event_allowlisted(event, patterns):
                continue  # PII guard — zero metadata retention
            for occ_start, occ_end, occurrence_of in _expand_event(
                event, window_start, window_end,
            ):
                if (
                    occurrence_of is not None
                    and (event["uid"], _dt_key(occ_start)) in overridden
                ):
                    continue
                yield from self._emit_event_row(
                    event, occ_start, occ_end, occurrence_of, feed_node_id,
                )

    def _emit_event_row(
        self,
        event: dict[str, Any],
        occ_start: datetime | date,
        occ_end: datetime | date | None,
        occurrence_of: str | None,
        feed_node_id: str,
    ) -> Iterable[Any]:
        uid = event["uid"]
        kind = _infer_kind(event["summary"])
        if occurrence_of is not None or event["recurrence_id"] is not None:
            # Recurring occurrence or a RECURRENCE-ID override row:
            # the UID names the series, so the row id needs the
            # occurrence timestamp to be unique within it.
            suffix = _dt_key(
                occ_start
                if event["recurrence_id"] is None
                else event["recurrence_id"]
            )
            event_id = f"event:ics:{uid}:{suffix}" if uid else (
                f"event:ics:{_sha(event['summary'] + suffix)[:16]}"
            )
        else:
            event_id = f"event:ics:{uid}" if uid else (
                f"event:ics:{_sha(event['summary'] + _iso(occ_start))[:16]}"
            )
        revision_sha = _sha(repr((event, _iso(occ_start))))
        yield NodeFact(
            node_id=event_id,
            subtype="calendar_event",
            attrs={
                "event_id": event_id,
                "title": event["summary"],
                "kind": kind,
                "start_dt": _iso(occ_start),
                "end_dt": _iso(occ_end) if occ_end is not None else None,
                "all_day": event["all_day"],
                "location": event["location"],
                "external_ref": uid,
                "feed_ref": feed_node_id,
                "recurrence_rule": event["rrule"] or "",
            },
            source_record_id={"event_id": event_id},
            source_revision={"content_sha": revision_sha},
        )
        yield EdgeFact(
            src=event_id,
            dst=feed_node_id,
            edge_type="at_feed",
            source_record_id={"event_id": event_id},
        )
        if "okg-deadline:" in (event["description"] or "").lower():
            deadline_id = event_id.replace("event:ics:", "deadline:ics:", 1)
            yield NodeFact(
                node_id=deadline_id,
                subtype="deadline",
                attrs={
                    "deadline_id": deadline_id,
                    "title": event["summary"],
                    "kind": "personal",
                    "due": _iso(occ_start),
                    "status": "pending",
                    "subject_ref": event_id,
                },
                source_record_id={"deadline_id": deadline_id},
                source_revision={"content_sha": revision_sha},
            )
        yield ProgressMarker(record_id=event_id, fingerprint=revision_sha)


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _iso(value: datetime | date | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


def _dt_key(value: datetime | date) -> str:
    """Stable comparison/identity key for occurrence datetimes.

    Aware datetimes normalize to UTC so an EXDATE given with a
    TZID matches the occurrence generated from a Z-suffixed
    DTSTART and vice versa; naive datetimes and dates key on
    their literal ISO form.
    """
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            return value.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
        return value.strftime("%Y%m%dT%H%M%S")
    return value.strftime("%Y%m%d")


def _parse_vevents(ics_text: str) -> tuple[list[dict[str, Any]], int]:
    """Parse with `icalendar`; per-block fallback on a corrupt file.

    Returns (events, unparseable_count). Each event dict carries
    decoded fields: uid, summary, description, location, start,
    end, all_day, rrule (text or None), exdates (set of keys),
    recurrence_id (datetime | None).
    """
    from icalendar import Calendar, Event

    components: list[Any] = []
    unparseable = 0
    try:
        cal = Calendar.from_ical(ics_text)
        components = list(cal.walk("VEVENT"))
    except Exception:
        # Whole-file parse failed — recover valid components
        # block-by-block and count the corrupt ones.
        for block in _split_vevent_blocks(ics_text):
            try:
                components.append(Event.from_ical(block))
            except Exception:
                unparseable += 1

    events: list[dict[str, Any]] = []
    for comp in components:
        try:
            events.append(_decode_vevent(comp))
        except Exception:
            unparseable += 1
    return events, unparseable


def _split_vevent_blocks(ics_text: str) -> list[str]:
    blocks: list[str] = []
    cur: list[str] | None = None
    for raw in ics_text.splitlines():
        line = raw.rstrip("\r")
        if line == "BEGIN:VEVENT":
            cur = [line]
        elif line == "END:VEVENT":
            if cur is not None:
                cur.append(line)
                blocks.append("\n".join(cur))
            cur = None
        elif cur is not None:
            cur.append(line)
    return blocks


def _decode_vevent(comp: Any) -> dict[str, Any]:
    dtstart = comp.decoded("DTSTART")
    if not isinstance(dtstart, (datetime, date)):
        # icalendar is lenient and hands back the raw string for a
        # malformed DTSTART; treat that as an unparseable component.
        raise ValueError(f"DTSTART did not decode to a date: {dtstart!r}")
    all_day = isinstance(dtstart, date) and not isinstance(dtstart, datetime)

    end: datetime | date | None = None
    if comp.get("DTEND") is not None:
        end = comp.decoded("DTEND")
    elif comp.get("DURATION") is not None:
        end = dtstart + comp.decoded("DURATION")

    rrule_text: str | None = None
    if comp.get("RRULE") is not None:
        rrule_text = comp["RRULE"].to_ical().decode("utf-8")

    exdates: set[str] = set()
    raw_exdate = comp.get("EXDATE")
    if raw_exdate is not None:
        exdate_props = raw_exdate if isinstance(raw_exdate, list) else [raw_exdate]
        for prop in exdate_props:
            for entry in prop.dts:
                exdates.add(_dt_key(entry.dt))

    recurrence_id: datetime | date | None = None
    if comp.get("RECURRENCE-ID") is not None:
        recurrence_id = comp.decoded("RECURRENCE-ID")

    return {
        "uid": str(comp.get("UID", "")),
        "summary": str(comp.get("SUMMARY", "")),
        "description": str(comp.get("DESCRIPTION", "")),
        "location": str(comp.get("LOCATION", "")),
        "start": dtstart,
        "end": end,
        "all_day": all_day,
        "rrule": rrule_text,
        "exdates": exdates,
        "recurrence_id": recurrence_id,
    }


def _expand_event(
    event: dict[str, Any],
    window_start: datetime,
    window_end: datetime,
) -> Iterable[tuple[datetime | date, datetime | date | None, str | None]]:
    """Yield (start, end, occurrence_of) rows for one event.

    Non-recurring events (and RECURRENCE-ID overrides, which are
    single instances by definition) yield exactly one row,
    unconditionally — matching the pre-RRULE parser, the window
    bounds only recurrence expansion. Recurring events yield one
    row per occurrence within the window; `occurrence_of` carries
    the series UID.
    """
    start = event["start"]
    end = event["end"]
    duration = (
        end - start
        if isinstance(end, (datetime, date)) and type(end) is type(start)
        else None
    )

    if not event["rrule"] or event["recurrence_id"] is not None:
        yield start, end, None
        return

    # dateutil needs datetimes; all-day series expand at naive
    # midnight and map back to dates on emission.
    if isinstance(start, datetime):
        dtstart = start
    else:
        dtstart = datetime(start.year, start.month, start.day)

    ws, we = window_start, window_end
    if dtstart.tzinfo is None:
        ws = ws.astimezone(UTC).replace(tzinfo=None)
        we = we.astimezone(UTC).replace(tzinfo=None)

    rule = rrulestr(event["rrule"], dtstart=dtstart)
    for occ in rule.between(ws, we, inc=True):
        if _dt_key(occ) in event["exdates"]:
            continue
        if event["all_day"]:
            occ_start: datetime | date = occ.date()
            if _dt_key(occ_start) in event["exdates"]:
                continue
            occ_end = occ_start + duration if duration is not None else None
        else:
            occ_start = occ
            occ_end = occ + duration if duration is not None else None
        yield occ_start, occ_end, event["uid"]


def _event_allowlisted(
    event: dict[str, Any],
    patterns: list[re.Pattern],
) -> bool:
    """Strict-allowlist filter: events with `okg:` tag OR
    matching a configured pattern emit. Else skipped.
    """
    text = (event["summary"] or "") + " " + (event["description"] or "")
    if "okg:" in text.lower():
        return True
    for pat in patterns:
        if pat.search(text):
            return True
    return False


def _infer_kind(title: str) -> str:
    for pat, kind in _TITLE_KIND_PATTERNS:
        if pat.search(title):
            return kind
    return "other"
