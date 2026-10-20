"""google_calendar_api source — Google Calendar v3 with feed allowlist.

Per `refine-academic-pi-with-comms-and-calendar` Tier C:
emits CalendarEvent + CalendarFeed for events on operator-
configured calendars (`google_calendar_feeds` allowlist).
Same strict event-filter as calendar_ics.

Per `cover-calendar-sources`: requests `singleEvents=true` with an
explicit time window so recurring events arrive expanded (pre-fix
they came back as unexpanded masters — the same silent
under-emission class fix-calendar-ics-rrule fixed for ICS), walks
nextPageToken pagination, warns loudly when unconfigured, and
infers event kind like the other calendar sources. Full re-list
per run is a recorded design choice: the API forbids combining
syncToken with timeMin windows, so incremental sync is a follow-on
trade, not an omission.
"""
from __future__ import annotations

import hashlib
import logging as _logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Optional
from urllib.parse import quote

from okg.substrate.library.sources.base import (
    EdgeFact,
    NodeFact,
    ProgressMarker,
    SourcePreflightResult,
    SourceRun,
)
from okg.substrate.library.oauth.providers import GOOGLE_CALENDAR
from okg.substrate.library.oauth.refresh import RefreshHandler
from okg.substrate.library.sources.calendar_ics import (
    _DEFAULT_RECURRING_PATTERNS,
    _infer_kind,
)

_log = _logging.getLogger(__name__)

_API = "https://www.googleapis.com/calendar/v3"


class GoogleCalendarApiSource:
    """Google Calendar v3 connector. Feed allowlist required."""

    name: str = "google_calendar_api"
    profile: str = "mutable_api"

    def __init__(
        self,
        *,
        deployment_name: str,
        google_calendar_feeds: Optional[list[str]] = None,
        recurring_patterns: Optional[list[str]] = None,
        window_past_months: int = 12,
        window_future_months: int = 18,
        window_start: Optional[str] = None,
        window_end: Optional[str] = None,
        page_size: int = 250,
        ownership_id: str = "google_calendar_api",
        refresh_handler: Optional[RefreshHandler] = None,
        **_ignored: Any,
    ) -> None:
        self.deployment_name = deployment_name
        self.feeds = google_calendar_feeds or []
        self.patterns = [
            re.compile(p, re.I)
            for p in (recurring_patterns or _DEFAULT_RECURRING_PATTERNS)
        ]
        self.window_past_months = window_past_months
        self.window_future_months = window_future_months
        self.window_start = window_start
        self.window_end = window_end
        self.page_size = page_size
        self.ownership_id = ownership_id
        self._refresh_handler = refresh_handler

    def preflight(self, mode: str = "live") -> SourcePreflightResult:
        """OAuth preflight for `okg doctor --check source-auth`
        (dogfood F-7a)."""
        from okg.substrate.library.oauth.preflight import oauth_preflight
        return oauth_preflight(
            source_name=self.name,
            deployment_name=self.deployment_name,
            provider=GOOGLE_CALENDAR,
        )

    def _window(self) -> tuple[str, str]:
        if self.window_start and self.window_end:
            return self.window_start, self.window_end
        now = datetime.now(timezone.utc)
        start = now - timedelta(days=30 * self.window_past_months)
        end = now + timedelta(days=30 * self.window_future_months)
        fmt = "%Y-%m-%dT%H:%M:%SZ"
        return start.strftime(fmt), end.strftime(fmt)

    def run(
        self, run_id: str, *, mode: str = "cursor", **_ignored: Any,
    ) -> SourceRun:
        if not self.feeds:
            # No allowlist = no ingest, loudly (per the connector
            # contract; pre-fix this was a silent empty run).
            _log.warning(
                "google_calendar_api: no google_calendar_feeds "
                "configured; refusing to ingest (per strict-allowlist "
                "contract). Set `google_calendar_feeds` in "
                "source_registry.yaml."
            )
            return SourceRun(facts=iter([]), completed_scope=True)
        return SourceRun(facts=self._stream(), completed_scope=True)

    def _stream(self) -> Iterable[Any]:
        # OAuth construction failure is unrecoverable — propagate
        # so the ingest fails loud rather than emitting zero facts.
        refresh = self._refresh_handler or RefreshHandler(
            provider=GOOGLE_CALENDAR,
            deployment_name=self.deployment_name,
        )
        window_start, window_end = self._window()

        for cal_id in self.feeds:
            feed_id = f"feed:gcal:{hashlib.sha256(cal_id.encode()).hexdigest()[:12]}"
            yield NodeFact(
                node_id=feed_id,
                subtype="calendar_feed",
                attrs={
                    "feed_id": feed_id,
                    "name": cal_id,
                    "source_kind": "google_api",
                    "external_ref": cal_id,
                },
                source_record_id={"calendar_id": cal_id},
                source_revision={"content_sha": "static"},
            )
            yield ProgressMarker(record_id=feed_id, fingerprint=cal_id)

            # singleEvents=true expands recurring events server-side;
            # the explicit window bounds the expansion (mirrors the
            # other calendar sources' 12/18-month default).
            base_url = (
                f"{_API}/calendars/{quote(cal_id, safe='')}/events"
                f"?singleEvents=true&orderBy=startTime"
                f"&timeMin={window_start}&timeMax={window_end}"
                f"&maxResults={self.page_size}"
            )
            page_token: Optional[str] = None
            while True:
                url = base_url + (
                    f"&pageToken={page_token}" if page_token else ""
                )
                try:
                    payload = refresh.authed_request(url)
                except Exception:  # noqa: BLE001 — one calendar failure shouldn't block the whole sync
                    _log.exception(
                        "google_calendar_api: events fetch failed for %s",
                        cal_id,
                    )
                    break

                for ev in payload.get("items", []) or []:
                    yield from self._emit_event(ev, feed_id)

                page_token = payload.get("nextPageToken")
                if not page_token:
                    break

    def _emit_event(self, ev: dict, feed_id: str) -> Iterable[Any]:
        if not _event_allowlisted(ev, self.patterns):
            return  # PII guard — zero metadata retention
        ev_id_raw = ev.get("id", "")
        event_id = f"event:gcal:{ev_id_raw}"
        start = (ev.get("start") or {}).get(
            "dateTime",
            (ev.get("start") or {}).get("date", ""),
        )
        end = (ev.get("end") or {}).get(
            "dateTime",
            (ev.get("end") or {}).get("date", ""),
        )
        content_hash = hashlib.sha256(str(ev).encode("utf-8")).hexdigest()
        yield NodeFact(
            node_id=event_id,
            subtype="calendar_event",
            attrs={
                "event_id": event_id,
                "title": ev.get("summary", ""),
                "kind": _infer_kind(ev.get("summary", "") or ""),
                "start_dt": start,
                "end_dt": end,
                "location": ev.get("location", ""),
                "external_ref": ev_id_raw,
                "feed_ref": feed_id,
            },
            source_record_id={"event_id": ev_id_raw},
            source_revision={"content_sha": content_hash},
        )
        yield EdgeFact(
            src=event_id, dst=feed_id, edge_type="at_feed",
            source_record_id={"event_id": ev_id_raw},
        )
        yield ProgressMarker(
            record_id=ev_id_raw, fingerprint=content_hash,
        )


def _event_allowlisted(
    ev: dict, patterns: list[re.Pattern],
) -> bool:
    summary = ev.get("summary", "") or ""
    description = ev.get("description", "") or ""
    text = summary + " " + description
    if "okg:" in text.lower():
        return True
    return any(p.search(text) for p in patterns)
