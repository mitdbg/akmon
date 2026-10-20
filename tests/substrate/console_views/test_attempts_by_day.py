"""Bucketing and range rules for the console's attempts-per-day read.

No database: the composer's day arithmetic, its range vocabulary and its
refusal to invent a fact are all decidable from a fake history, and a Postgres
test cannot pin a clock across a day boundary the way these do. The route and
the real checkpoint columns are covered by the Postgres suite in
tests/substrate/viz/test_console_home_live_reads.py.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from okg.substrate.console_views.attempts_by_day import (
    MAX_RANGE_DAYS,
    RANGE_DAYS,
    AttemptsRangeInvalid,
    compose_attempts_by_day,
    parse_checkpoint_created_at,
    range_days,
)

NOW = datetime(2026, 9, 21, 13, 30, tzinfo=timezone.utc)
SESSION = SimpleNamespace(branch_name="default", generation_id="gen:1")


class _Generation:
    def __init__(self, created_at, status: str) -> None:
        self.checkpoint_id = f"cp:{created_at}:{status}"
        self.created_at = created_at
        self.metadata = {"status": status}


class _Store:
    """The slice of ChronosGraphStore the composer uses, plus a call count.

    It behaves like the real bounded reader rather than like the unbounded one
    it replaced: it honours the `since` scan bound the same way (a lexical
    compare on the `created_at` column) and projects only the keys it was
    asked for, so a composer that reads an unprojected key sees None here too.
    """

    def __init__(self, generations: list[_Generation]) -> None:
        self.generations = generations
        self.calls = 0
        self.undated_calls = 0

    def recent_generations(self, *, keys=None, since=None, scan_window=None, **_kwargs):
        from okg.substrate.graph_store_internal.generations import (
            GenerationRow,
            RecentGenerations,
            _generation_projection,
        )

        self.calls += 1
        projected = _generation_projection(keys)
        scanned = [
            g for g in self.generations
            if since is None or str(g.created_at) >= since
        ]
        scanned.sort(key=lambda g: str(g.created_at), reverse=True)
        window = 200 if scan_window is None else scan_window
        scanned = scanned[:window]
        rows = [
            GenerationRow(
                g.checkpoint_id, "default", 0, g.created_at,
                {key: (g.metadata or {}).get(key) for key in projected},
            )
            for g in scanned
        ]
        return RecentGenerations(
            rows,
            scanned=len(scanned),
            scan_window=window,
            window_exhausted=len(scanned) >= window,
        )

    def count_undated_checkpoints(self, **_kwargs):
        """Branch-wide, as the real one is: every checkpoint, not a window."""
        import re

        self.undated_calls += 1
        return sum(
            1 for g in self.generations
            if not re.match(r"^\d{4}-\d{2}-\d{2}T", str(g.created_at))
        )


@pytest.fixture
def store(monkeypatch: pytest.MonkeyPatch):
    holder: dict[str, _Store] = {}

    def install(generations: list[_Generation]) -> _Store:
        made = _Store(generations)
        holder["store"] = made
        monkeypatch.setattr(
            "okg.substrate.console_views.attempts_by_day.ChronosGraphStore",
            SimpleNamespace(from_connection=lambda *_a, **_k: made),
        )
        return made

    return install


def _compose(requested_range: str = "7d", *, now: datetime = NOW):
    return compose_attempts_by_day(
        object(),
        deployment_name="demo",
        session=SESSION,
        requested_range=requested_range,
        now=now,
    )


def test_every_offered_range_is_accepted_and_nothing_else_is() -> None:
    assert set(RANGE_DAYS) == {"1d", "7d", "14d", "30d", "90d"}
    for name, days in RANGE_DAYS.items():
        assert range_days(name) == min(days, MAX_RANGE_DAYS)
    with pytest.raises(AttemptsRangeInvalid) as raised:
        range_days("all")
    assert raised.value.requested == "all"
    assert raised.value.accepted == tuple(RANGE_DAYS)


def test_a_range_is_refused_rather_than_served_as_the_default(store) -> None:
    """The silent-default failure this guards: answering 7d for '365d' puts a
    chart on the page that says something other than what was asked."""
    store([])
    with pytest.raises(AttemptsRangeInvalid):
        _compose("365d")


def test_the_span_covers_today_and_the_days_before_it(store) -> None:
    """A span of n days reaches back n WHOLE DAYS, so it holds n + 1 of them.

    The read's own day and the n before it. The extra day is the boundary the
    console's age label uses: a row the page calls "7 d ago" belongs inside a
    7-day span, and a span of seven calendar days ending today stops one day
    short of it.
    """
    store([])
    assert [bucket["day"] for bucket in _compose("1d")["buckets"]] == [
        "2026-09-20", "2026-09-21",
    ]
    week = [bucket["day"] for bucket in _compose("7d")["buckets"]]
    assert week == [
        "2026-09-14", "2026-09-15", "2026-09-16", "2026-09-17",
        "2026-09-18", "2026-09-19", "2026-09-20", "2026-09-21",
    ]
    assert len(_compose("90d")["buckets"]) == MAX_RANGE_DAYS + 1


def test_an_empty_deployment_answers_with_empty_days(store) -> None:
    """An answer, not an error: nothing published yet is a real state."""
    store([])
    payload = _compose("7d")
    assert len(payload["buckets"]) == 8
    assert all(
        bucket["published"] == bucket["blocked"] == bucket["running"] == 0
        for bucket in payload["buckets"]
    )
    assert payload["totals"] == {"published": 0, "blocked": 0, "running": 0}


def test_attempts_are_counted_into_their_utc_day_by_outcome(store) -> None:
    store([
        _Generation("2026-09-21T13:00:00+00:00", "published"),
        _Generation("2026-09-21T01:00:00+00:00", "blocked"),
        _Generation("2026-09-20T23:59:59+00:00", "published"),
        _Generation("2026-09-19T12:00:00+00:00", "running"),
    ])
    payload = _compose("7d")
    by_day = {bucket["day"]: bucket for bucket in payload["buckets"]}
    assert by_day["2026-09-21"] == {
        "day": "2026-09-21", "published": 1, "blocked": 1, "running": 0,
    }
    assert by_day["2026-09-20"]["published"] == 1
    assert by_day["2026-09-19"]["running"] == 1
    assert payload["totals"] == {"published": 2, "blocked": 1, "running": 1}


def test_a_checkpoint_exactly_at_midnight_belongs_to_the_day_it_opens(store) -> None:
    """00:00:00Z is the first instant of its own day, not the last of the one
    before. An off-by-one here moves a publish onto the wrong bar."""
    store([
        _Generation("2026-09-20T00:00:00+00:00", "published"),
        _Generation("2026-09-19T23:59:59.999999+00:00", "published"),
    ])
    by_day = {b["day"]: b for b in _compose("7d")["buckets"]}
    assert by_day["2026-09-20"]["published"] == 1
    assert by_day["2026-09-19"]["published"] == 1


def test_a_stamp_in_another_zone_is_bucketed_by_its_utc_day(store) -> None:
    """20:00 on the 20th in UTC-8 is 04:00 on the 21st in UTC."""
    store([_Generation("2026-09-20T20:00:00-08:00", "published")])
    by_day = {b["day"]: b for b in _compose("7d")["buckets"]}
    assert by_day["2026-09-21"]["published"] == 1
    assert by_day["2026-09-20"]["published"] == 0


def test_a_naive_stamp_is_read_as_utc(store) -> None:
    store([_Generation("2026-09-20T06:00:00", "published")])
    by_day = {b["day"]: b for b in _compose("7d")["buckets"]}
    assert by_day["2026-09-20"]["published"] == 1


def test_attempts_outside_the_span_are_not_counted(store) -> None:
    store([
        _Generation("2026-09-13T12:00:00+00:00", "published"),
        _Generation("2026-09-14T12:00:00+00:00", "published"),
    ])
    payload = _compose("7d")
    assert payload["totals"]["published"] == 1
    assert len(payload["buckets"]) == 8


def test_the_ninety_day_cap_holds_against_a_longer_history(store) -> None:
    store([
        _Generation(
            (NOW - timedelta(days=offset)).isoformat(), "published",
        )
        for offset in range(0, 200)
    ])
    payload = _compose("90d")
    assert len(payload["buckets"]) == MAX_RANGE_DAYS + 1
    assert payload["totals"]["published"] == MAX_RANGE_DAYS + 1


def test_an_unknown_status_is_reported_not_folded_into_an_outcome(store) -> None:
    """Counting an unrecognised status as 'blocked' would invent a failure."""
    store([
        _Generation("2026-09-21T10:00:00+00:00", "superseded"),
        _Generation("2026-09-21T11:00:00+00:00", "published"),
    ])
    payload = _compose("7d")
    assert payload["totals"] == {"published": 1, "blocked": 0, "running": 0}
    assert payload["other_status_count"] == 1


def test_an_unreadable_stamp_is_reported_not_dropped_silently(store) -> None:
    store([
        _Generation("not-a-timestamp", "published"),
        _Generation(None, "published"),
    ])
    payload = _compose("7d")
    assert payload["undated_count"] == 2
    assert payload["totals"]["published"] == 0


def test_the_history_is_read_once_for_the_whole_chart(store) -> None:
    """Bounded by construction: no per-attempt query."""
    made = store([
        _Generation("2026-09-21T10:00:00+00:00", "published") for _ in range(50)
    ])
    _compose("90d")
    assert made.calls == 1


def test_the_payload_carries_no_duration_or_records_added(store) -> None:
    """Cut, not faked: neither is recorded per attempt, so no field exists for
    a view to start drawing a zero from."""
    store([_Generation("2026-09-21T10:00:00+00:00", "published")])
    payload = _compose("7d")
    bucket = payload["buckets"][-1]
    assert set(bucket) == {"day", "published", "blocked", "running"}
    assert "duration_ms" not in payload
    assert "records_added" not in payload


def test_a_store_failure_is_raised_not_answered_with_zeros(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def explode(*_args, **_kwargs):
        raise RuntimeError("chronos unavailable")

    monkeypatch.setattr(
        "okg.substrate.console_views.attempts_by_day.ChronosGraphStore",
        SimpleNamespace(from_connection=explode),
    )
    with pytest.raises(RuntimeError, match="chronos unavailable"):
        _compose("7d")


def test_now_is_reported_so_the_marker_and_the_buckets_agree(store) -> None:
    store([])
    payload = _compose("7d")
    assert payload["now"] == NOW.isoformat()
    assert payload["range"] == "7d"
    assert payload["deployment"] == "demo"


def test_the_shared_parser_returns_aware_utc_for_every_written_form() -> None:
    """The generations panel and the chart read the same column through this
    one function. Two parsers with different rules put one checkpoint on two
    different days in two panels of the same page."""
    naive = parse_checkpoint_created_at("2026-09-20T06:00:00")
    assert naive is not None
    assert naive.tzinfo is not None
    assert naive.utcoffset() == timedelta(0)
    assert naive.isoformat() == "2026-09-20T06:00:00+00:00"

    shifted = parse_checkpoint_created_at("2026-09-20T20:00:00-08:00")
    assert shifted is not None
    assert shifted.utcoffset() == timedelta(0)
    assert shifted.isoformat() == "2026-09-21T04:00:00+00:00"

    already = parse_checkpoint_created_at("2026-09-21T02:48:10.042141+00:00")
    assert already is not None
    assert already.isoformat() == "2026-09-21T02:48:10.042141+00:00"

    assert parse_checkpoint_created_at("not-a-timestamp") is None
    assert parse_checkpoint_created_at(None) is None


def _age_label_days(created_at: str, now: datetime) -> int:
    """What the console prints as the age, in the console's own arithmetic.

    `relativeAge` in `console/model/shared/time.ts` counts whole UTC days
    between the two dates past 48 hours. Reimplemented here in one line so the
    span's boundary is asserted against the LABEL a reader sees, which is the
    thing it must not contradict.
    """
    stamp = parse_checkpoint_created_at(created_at)
    assert stamp is not None
    return (now.astimezone(timezone.utc).date() - stamp.date()).days


@pytest.mark.parametrize("age_days", [14.0, 14.5])
def test_an_attempt_the_page_calls_fourteen_days_old_is_inside_a_14d_span(
    store, age_days: float,
) -> None:
    """The defect this closes, at both ends of the label's own range.

    Live okg-workspace on 2026-09-22 drew "No attempt in this span" on a
    14-day chart with a row three lines below it reading "14 d ago": the
    newest attempt was 14.47 days old, the label floored to 14, and the span
    of fourteen calendar days ending today started the day after it. The span
    and the label now share one boundary, so a row the page calls "n d ago"
    is inside an n-day span for every n up to the span.
    """
    created_at = (NOW - timedelta(days=age_days)).isoformat()
    assert _age_label_days(created_at, NOW) == 14
    store([_Generation(created_at, "published")])
    payload = _compose("14d")
    assert payload["totals"]["published"] == 1, (
        f"an attempt {age_days} days old, which the page labels "
        f"\"{_age_label_days(created_at, NOW)} d ago\", fell outside the "
        f"14-day span: {[b['day'] for b in payload['buckets']][:3]} ..."
    )


def test_an_attempt_the_page_calls_fifteen_days_old_is_outside_a_14d_span(
    store,
) -> None:
    """The other side of the same boundary, so the span is not merely wider.

    A span that swallowed this row would put an attempt the page calls "15 d
    ago" on a chart labelled fourteen days, which is the same contradiction
    read from the other end.
    """
    created_at = (NOW - timedelta(days=15.0)).isoformat()
    assert _age_label_days(created_at, NOW) == 15
    store([_Generation(created_at, "published")])
    payload = _compose("14d")
    assert payload["totals"]["published"] == 0


def test_the_scan_margin_covers_a_stamp_written_in_another_utc_offset(
    store, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`_SCAN_MARGIN_DAYS` is load-bearing, and nothing measured it.

    The scan bound is a LEXICAL compare on the `created_at` TEXT column, so a
    checkpoint written with a non-UTC offset sorts up to fourteen hours away
    from where its UTC instant belongs. Both rows below sit inside the span by
    the UTC instant the composer buckets on, and outside it by their text:

      -12:00  02:00 UTC on the span's oldest day, written in the furthest-west
              offset, so its text reads as 14:00 on the day BEFORE and sorts
              under `span[0]`. This is the row the margin exists for: with the
              margin at zero the scan never reaches it and the chart is short
              by one attempt, with nothing saying so.
      +14:00  11:00 UTC on the read's own day, written in the furthest-east
              offset, so its text reads as 01:00 on the day AFTER. A lower
              bound cannot drop it, and it is asserted here so a future change
              to an upper bound cannot drop it silently either.
    """
    from okg.substrate.console_views import attempts_by_day

    span_oldest = NOW.date() - timedelta(days=7)
    west = datetime(
        span_oldest.year, span_oldest.month, span_oldest.day, 2, 0,
        tzinfo=timezone.utc,
    ).astimezone(timezone(timedelta(hours=-12)))
    east = datetime(
        NOW.year, NOW.month, NOW.day, 11, 0, tzinfo=timezone.utc,
    ).astimezone(timezone(timedelta(hours=14)))

    # Inside the span by the instant, outside it by the text.
    assert west.astimezone(timezone.utc).date() == span_oldest
    assert east.astimezone(timezone.utc).date() == NOW.date()
    assert west.isoformat() < span_oldest.isoformat()
    assert east.isoformat() > NOW.date().isoformat()

    generations = [
        _Generation(west.isoformat(), "published"),
        _Generation(east.isoformat(), "published"),
    ]
    store(generations)
    assert _compose("7d")["totals"]["published"] == 2

    monkeypatch.setattr(attempts_by_day, "_SCAN_MARGIN_DAYS", 0)
    store(generations)
    assert _compose("7d")["totals"]["published"] == 1, (
        "with no scan margin the west-offset checkpoint should fall outside "
        "the lexical scan bound; if it does not, this test no longer measures "
        "what _SCAN_MARGIN_DAYS is for"
    )
