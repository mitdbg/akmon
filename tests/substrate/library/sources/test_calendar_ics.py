"""Smoke tests for the calendar_ics source.

Verifies strict-allowlist filter + CalendarEvent emission shape.
"""
from __future__ import annotations

from pathlib import Path


import pytest

from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.library.sources.calendar_ics import CalendarIcsSource
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


_SAMPLE_ICS = """BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//okg//test//EN
BEGIN:VEVENT
UID:event-001@okg
SUMMARY:Lab meeting weekly
DTSTART:20260520T140000Z
DTEND:20260520T150000Z
DESCRIPTION:okg: lab meeting
LOCATION:Stata 32-G505
END:VEVENT
BEGIN:VEVENT
UID:event-002@okg
SUMMARY:Dentist appointment
DTSTART:20260521T100000Z
DESCRIPTION:Regular checkup
END:VEVENT
BEGIN:VEVENT
UID:event-003@okg
SUMMARY:1:1 with Alice
DTSTART:20260522T140000Z
DESCRIPTION:Weekly catch-up
END:VEVENT
BEGIN:VEVENT
UID:event-004@okg
SUMMARY:Family dinner
DTSTART:20260523T180000Z
DESCRIPTION:Mom's birthday
END:VEVENT
END:VCALENDAR
"""


def test_calendar_ics_allowlist_filter(tmp_path: Path) -> None:
    """3 events should emit: the okg-tagged lab meeting, the 1:1
    (matches default pattern), and NOT the dentist or family
    events.
    """
    ics_path = tmp_path / "sample.ics"
    ics_path.write_text(_SAMPLE_ICS, encoding="utf-8")
    src = CalendarIcsSource(
        calendar_ics_path=str(ics_path),
        feed_name="test",
    )
    run = src.run(run_id="test-run-1")
    nodes = [f for f in run.facts if isinstance(f, NodeFact)]
    calendar_events = [n for n in nodes if n.subtype == "calendar_event"]
    feeds = [n for n in nodes if n.subtype == "calendar_feed"]
    titles = {n.attrs.get("title") for n in calendar_events}
    # 1 feed + 2 allowlisted events expected.
    assert len(feeds) == 1
    assert len(calendar_events) == 2
    assert "Lab meeting weekly" in titles
    assert "1:1 with Alice" in titles
    # Non-allowlisted events MUST NOT emit.
    assert "Dentist appointment" not in titles
    assert "Family dinner" not in titles


def test_calendar_ics_emits_at_feed_edges(tmp_path: Path) -> None:
    ics_path = tmp_path / "sample.ics"
    ics_path.write_text(_SAMPLE_ICS, encoding="utf-8")
    src = CalendarIcsSource(
        calendar_ics_path=str(ics_path),
        feed_name="test",
    )
    run = src.run(run_id="test-run-2")
    facts = list(run.facts)
    edges = [f for f in facts if isinstance(f, EdgeFact)]
    at_feed_edges = [e for e in edges if e.edge_type == "at_feed"]
    # Each allowlisted event emits an at_feed edge.
    assert len(at_feed_edges) == 2


def test_calendar_ics_empty_config_returns_empty_run() -> None:
    """No path / no URL → empty SourceRun, no crash."""
    src = CalendarIcsSource()
    run = src.run(run_id="test-run-3")
    nodes = list(run.facts)
    assert nodes == []


def test_calendar_ics_single_event_id_unchanged(tmp_path: Path) -> None:
    """Back-compat: non-recurring events keep the `event:ics:{uid}`
    identity so existing graph rows don't churn under the new parser."""
    ics_path = tmp_path / "sample.ics"
    ics_path.write_text(_SAMPLE_ICS, encoding="utf-8")
    src = CalendarIcsSource(calendar_ics_path=str(ics_path), feed_name="test")
    run = src.run(run_id="test-run-4")
    nodes = [f for f in run.facts if isinstance(f, NodeFact)]
    ids = {n.node_id for n in nodes if n.subtype == "calendar_event"}
    assert "event:ics:event-001@okg" in ids
    assert "event:ics:event-003@okg" in ids


# --- fix-calendar-ics-rrule: recurrence expansion -------------------

_WINDOW = {
    "expand_window_start": "2026-01-01T00:00:00+00:00",
    "expand_window_end": "2027-06-01T00:00:00+00:00",
}


def _events_from(ics_text: str, tmp_path: Path, **params) -> list[NodeFact]:
    ics_path = tmp_path / "feed.ics"
    ics_path.write_text(ics_text, encoding="utf-8")
    src = CalendarIcsSource(
        calendar_ics_path=str(ics_path), feed_name="test", **{**_WINDOW, **params},
    )
    run = src.run(run_id="test-run-rrule")
    return [
        f for f in run.facts
        if isinstance(f, NodeFact) and f.subtype == "calendar_event"
    ]


_RRULE_EXDATE_ICS = """BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//okg//test//EN
BEGIN:VEVENT
UID:lab-weekly@okg
SUMMARY:Lab meeting
DTSTART:20260506T140000Z
DTEND:20260506T150000Z
RRULE:FREQ=WEEKLY;UNTIL=20260624T140000Z
EXDATE:20260513T140000Z
EXDATE:20260603T140000Z
DESCRIPTION:okg: weekly lab meeting
END:VEVENT
END:VCALENDAR
"""


def test_weekly_rrule_with_exdate_expands_exact_occurrences(
    tmp_path: Path,
) -> None:
    """Weekly Wednesdays 2026-05-06 → 2026-06-24 is 8 occurrences;
    two EXDATEs remove May 13 and Jun 3 → exactly 6 rows, asserted
    start-for-start."""
    events = _events_from(_RRULE_EXDATE_ICS, tmp_path)
    starts = sorted(e.attrs["start_dt"] for e in events)
    assert starts == [
        "2026-05-06T14:00:00+00:00",
        "2026-05-20T14:00:00+00:00",
        "2026-05-27T14:00:00+00:00",
        "2026-06-10T14:00:00+00:00",
        "2026-06-17T14:00:00+00:00",
        "2026-06-24T14:00:00+00:00",
    ]
    # Occurrence rows are unique per occurrence and carry the rule.
    assert len({e.node_id for e in events}) == 6
    assert all(e.attrs["recurrence_rule"].startswith("FREQ=WEEKLY") for e in events)
    # End time follows the series duration (1h).
    assert all(
        e.attrs["end_dt"].endswith("15:00:00+00:00") for e in events
    )


_COUNT_UNTIL_ICS = """BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//okg//test//EN
BEGIN:VEVENT
UID:oh-count@okg
SUMMARY:Office hours
DTSTART:20260505T100000Z
DTEND:20260505T110000Z
RRULE:FREQ=WEEKLY;COUNT=5
DESCRIPTION:okg: office hours
END:VEVENT
BEGIN:VEVENT
UID:grp-until@okg
SUMMARY:Group meeting
DTSTART:20260504T090000Z
DTEND:20260504T100000Z
RRULE:FREQ=WEEKLY;UNTIL=20260525T090000Z
DESCRIPTION:okg: group meeting
END:VEVENT
END:VCALENDAR
"""


def test_count_and_until_terminate_expansion(tmp_path: Path) -> None:
    """COUNT=5 emits exactly 5; UNTIL (inclusive) emits exactly 4 —
    expansion stops at the rule bound, not the window edge."""
    events = _events_from(_COUNT_UNTIL_ICS, tmp_path)
    count_rows = [e for e in events if e.attrs["external_ref"] == "oh-count@okg"]
    until_rows = [e for e in events if e.attrs["external_ref"] == "grp-until@okg"]
    assert len(count_rows) == 5
    assert len(until_rows) == 4
    assert max(e.attrs["start_dt"] for e in until_rows) == (
        "2026-05-25T09:00:00+00:00"
    )


_DST_ICS = """BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//okg//test//EN
BEGIN:VEVENT
UID:dst-weekly@okg
SUMMARY:Lab meeting
DTSTART;TZID=America/New_York:20260301T100000
DTEND;TZID=America/New_York:20260301T110000
RRULE:FREQ=WEEKLY;COUNT=3
DESCRIPTION:okg: spring meetings
END:VEVENT
END:VCALENDAR
"""


def test_dst_crossing_weekly_rule_keeps_local_time(tmp_path: Path) -> None:
    """US DST starts 2026-03-08. The weekly 10:00 America/New_York
    rule keeps 10:00 local across the transition: the UTC offset
    shifts -05:00 → -04:00 instead of the wall time drifting."""
    events = _events_from(_DST_ICS, tmp_path)
    starts = sorted(e.attrs["start_dt"] for e in events)
    assert starts == [
        "2026-03-01T10:00:00-05:00",
        "2026-03-08T10:00:00-04:00",
        "2026-03-15T10:00:00-04:00",
    ]


_CORRUPT_ICS = """BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//okg//test//EN
BEGIN:VEVENT
UID:good-1@okg
SUMMARY:Lab meeting
DTSTART:20260520T140000Z
DESCRIPTION:okg: valid event
END:VEVENT
BEGIN:VEVENT
UID:bad-1@okg
SUMMARY:Office hours
DTSTART:NOT-A-DATE
DESCRIPTION:okg: corrupt event
END:VEVENT
END:VCALENDAR
"""


def test_unparseable_component_counted_and_reported(
    tmp_path: Path, caplog,
) -> None:
    """A corrupt VEVENT among valid ones: valid events still emit,
    the corrupt one increments the feed's reported counter and logs
    a warning — no silent skip."""
    import logging

    ics_path = tmp_path / "corrupt.ics"
    ics_path.write_text(_CORRUPT_ICS, encoding="utf-8")
    src = CalendarIcsSource(
        calendar_ics_path=str(ics_path), feed_name="test", **_WINDOW,
    )
    with caplog.at_level(logging.WARNING):
        run = src.run(run_id="test-run-corrupt")
        nodes = [f for f in run.facts if isinstance(f, NodeFact)]
    events = [n for n in nodes if n.subtype == "calendar_event"]
    feeds = [n for n in nodes if n.subtype == "calendar_feed"]
    assert {e.attrs["external_ref"] for e in events} == {"good-1@okg"}
    assert feeds[0].attrs["unparseable_components"] == 1
    assert any("unparseable" in r.message for r in caplog.records)


_OVERRIDE_ICS = """BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//okg//test//EN
BEGIN:VEVENT
UID:lab-ovr@okg
SUMMARY:Lab meeting
DTSTART:20260506T140000Z
DTEND:20260506T150000Z
RRULE:FREQ=WEEKLY;COUNT=3
DESCRIPTION:okg: weekly
END:VEVENT
BEGIN:VEVENT
UID:lab-ovr@okg
RECURRENCE-ID:20260513T140000Z
SUMMARY:Lab meeting (moved)
DTSTART:20260514T140000Z
DTEND:20260514T150000Z
DESCRIPTION:okg: moved instance
END:VEVENT
END:VCALENDAR
"""


def test_recurrence_id_override_displaces_occurrence(tmp_path: Path) -> None:
    """A RECURRENCE-ID override replaces its expanded occurrence:
    3 rows total (May 6, the moved May 14 override, May 20) — the
    original May 13 slot must not double-emit."""
    events = _events_from(_OVERRIDE_ICS, tmp_path)
    starts = sorted(e.attrs["start_dt"] for e in events)
    assert starts == [
        "2026-05-06T14:00:00+00:00",
        "2026-05-14T14:00:00+00:00",
        "2026-05-20T14:00:00+00:00",
    ]
    moved = [e for e in events if e.attrs["title"] == "Lab meeting (moved)"]
    assert len(moved) == 1


def test_real_export_recurrence_row_delta(tmp_path: Path) -> None:
    """The synthetic real-world-shaped export: 3 recurring series
    (COUNT=40 weekly lab meeting, COUNT=20 weekly office hours,
    COUNT=10 biweekly group meeting) + 2 tagged singles + 3
    non-allowlisted singles. The regex baseline emitted 1 row per
    allowlisted VEVENT = 5; expansion must emit 40+20+10+2 = 72."""
    fixture = (
        Path(__file__).parent / "fixtures" / "ics" / "real_export.ics"
    )
    src = CalendarIcsSource(
        calendar_ics_path=str(fixture), feed_name="real", **_WINDOW,
    )
    run = src.run(run_id="test-run-real")
    events = [
        f for f in run.facts
        if isinstance(f, NodeFact) and f.subtype == "calendar_event"
    ]
    regex_baseline_rows = 5
    assert len(events) == 72
    assert len(events) > regex_baseline_rows
    # The delta is exactly the expanded recurrence mass.
    assert len(events) - regex_baseline_rows == (40 - 1) + (20 - 1) + (10 - 1)
    # Allowlist still holds on the realistic feed.
    titles = {e.attrs["title"] for e in events}
    assert "Personal appointment" not in titles
    assert "Personal travel" not in titles
    assert "Family errand" not in titles


@pytest.mark.postgres
def test_real_export_publish_row_delta(clean_runtime: str) -> None:
    """The graph_report proof: the real-export fixture runs through
    SourceRunner -> Publisher against the test DB; the published
    generation's calendar_event row count equals the expansion math
    (72 = 5 regex-baseline rows + 67 expanded occurrences)."""
    from okg.substrate.ingest import SourceRunner
    from okg.substrate.publisher import Publisher
    from tests.substrate.library.sources.test_ci_source_sync_db import (
        _latest_generation_status,
        _live_node_count,
        _seed_catalog_and_external_nodes,
    )

    fixture = Path(__file__).parent / "fixtures" / "ics" / "real_export.ics"

    def _source() -> CalendarIcsSource:
        return CalendarIcsSource(
            calendar_ics_path=str(fixture), feed_name="real", **_WINDOW,
        )

    _seed_catalog_and_external_nodes(
        clean_runtime, _source().run("preview").facts,
    )
    runner = SourceRunner(clean_runtime, source_name="test.calendar-ics")
    result = runner.run(_source(), mode="scope_complete")
    assert result.completed_scope is True
    publish = Publisher(clean_runtime, "test", catalog_identity=bind_test_catalog_writer(clean_runtime, "test")).cycle()
    assert publish["status"] == "published", publish
    assert _latest_generation_status(clean_runtime) == "published"
    assert _live_node_count(clean_runtime, "calendar_event") == 72
    assert _live_node_count(clean_runtime, "calendar_feed") == 1
    # okg-deadline: tagged single emits its Deadline row too.
    assert _live_node_count(clean_runtime, "deadline") == 1
