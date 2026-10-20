"""Attempts-per-day composer for the operator console's Home chart.

Home's chart answers one question -- "how often does this deployment publish,
and how often does it fail?" -- over a span the operator picks. The answer is
the same Chronos checkpoint history ``console_views/generations.py`` reads,
bucketed by UTC day and grouped by outcome.

Outcome ONLY. The chart carries no per-day duration and no per-day
records-added figure, because neither is recorded per attempt: a checkpoint
stores ``created_at`` and a ``status``, ``metadata.total_duration_ms`` is
written by some publishes and absent on others, and ``metadata.published_at``
precedes ``created_at`` by the checkpoint write latency rather than by the run
duration. A composer that returned a zero for an unrecorded duration would put
a measured-looking flat line on an operator's chart, so this one does not carry
the field at all.

What this read costs, honestly: the history is listed ONCE and bucketed in one
pass, with no per-attempt query -- but that one read is the whole checkpoint
history of the branch, not the requested range. ``list_generations`` reaches
``chronos_core``'s ``list_checkpoints``, which selects every checkpoint row for
the branch with no LIMIT and no ``created_at`` predicate, sorts them, and
filters the metadata in Python. So a ``1d`` chart on a deployment with years of
checkpoints loads them all, and the cost grows with the history rather than
with the span. Pushing a time predicate down needs a new optional parameter on
``list_checkpoints`` in all three Chronos backends, which is a change to that
package rather than to this composer; it is recorded for its own issue. The
span itself is still capped at 90 days, and nothing here is per-attempt.

An empty deployment is an ANSWER, not an error: the range's days come back
with zero counts. A store failure is not -- it raises, and the route names it.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.mcp.session import Session

#: The spans Home's range control offers, in days. The console sends one of
#: these five strings; anything else is refused by name rather than quietly
#: served as the default, because a chart that silently answered a different
#: question than the one asked is worse than a refusal.
RANGE_DAYS: dict[str, int] = {
    "1d": 1,
    "7d": 7,
    "14d": 14,
    "30d": 30,
    "90d": 90,
}

#: No span longer than this is composed, whatever a caller asks for.
MAX_RANGE_DAYS = 90

#: The three outcomes a bucket counts. A checkpoint whose status is none of
#: these is counted under no outcome and is reported in ``other_status_count``,
#: so an unknown status is visible instead of silently inflating "blocked".
OUTCOMES = ("published", "blocked", "running")


class AttemptsRangeInvalid(ValueError):
    """A range the console did not offer. Carries the accepted values."""

    def __init__(self, requested: str) -> None:
        self.requested = requested
        self.accepted = tuple(RANGE_DAYS)
        super().__init__(
            f"range {requested!r} is not one of {', '.join(self.accepted)}",
        )


def range_days(requested: str) -> int:
    """Days for an offered range, or raise :class:`AttemptsRangeInvalid`."""
    days = RANGE_DAYS.get(requested)
    if days is None:
        raise AttemptsRangeInvalid(requested)
    return min(days, MAX_RANGE_DAYS)


def parse_checkpoint_created_at(value: Any) -> datetime | None:
    """A checkpoint's ``created_at`` as an aware UTC datetime, or None.

    The column is text, so a row written by an older publisher can be naive.
    A naive stamp is READ as UTC (every publisher writes UTC), and a stamp in
    another zone is converted, so a day boundary means one thing everywhere.
    """
    if isinstance(value, datetime):
        stamp = value
    elif isinstance(value, str):
        try:
            stamp = datetime.fromisoformat(value)
        except ValueError:
            return None
    else:
        return None
    if stamp.tzinfo is None:
        return stamp.replace(tzinfo=timezone.utc)
    return stamp.astimezone(timezone.utc)


# How far BEFORE the span's first day the bounded scan starts. The scan bound
# compares the checkpoint's `created_at` text lexically, so a checkpoint
# written with a non-UTC offset can sort up to 14 hours away from where its
# UTC instant belongs; two days of margin covers that with room, and the exact
# per-checkpoint test inside the loop is still what decides a bucket.
_SCAN_MARGIN_DAYS = 2

# The only checkpoint metadata this chart reads. Named here, and asserted
# against this composer's source by a guard test, so the bounded reader
# projects one key rather than every checkpoint's whole document.
ATTEMPTS_BY_DAY_METADATA_KEYS = ("status",)


def _days(now: datetime, days: int) -> list[date]:
    """The UTC days of the span, oldest first, ending on ``now``'s day.

    A span of ``n`` days reaches back ``n`` whole days from the read, so it
    holds ``n + 1`` UTC days: ``now``'s own day and the ``n`` before it.

    THE EXTRA DAY IS THE POINT, not an off-by-one. The console prints an
    attempt's age by flooring it to whole days (`relativeAge` in
    `console/model/shared/time.ts`, which the page pairs with a UTC
    calendar-day count so one arithmetic decides both), and the chart under it
    says whether the span holds any attempt. A span of ``n`` calendar days
    ending today covers the ages 0 through ``n - 1`` only, so an attempt whose
    label reads exactly "n d ago" fell outside it: live okg-workspace on
    2026-09-22 drew "No attempt in this span" over a 14-day chart with a row
    three lines below reading "14 d ago", because its newest attempt is 14.47
    days old and the span started the next day. One boundary now decides both:
    the span covers every attempt whose age label reads "0 d" through "n d",
    and nothing else.

    Every day is present, including the ones with no attempt: a missing bucket
    would let a chart draw a gap that reads as "no data recorded" rather than
    "no attempt was made".
    """
    end = now.astimezone(timezone.utc).date()
    return [end - timedelta(days=offset) for offset in range(days, -1, -1)]


def compose_attempts_by_day(
    conn: Any,
    *,
    deployment_name: str,
    session: Session,
    requested_range: str,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Attempts per UTC day by outcome over the requested span.

    ``now`` is injectable so a test can pin the clock; production passes none
    and the composer reads the wall clock once, reporting it in the payload so
    the chart's "now" marker and its buckets cannot disagree.
    """
    days = range_days(requested_range)
    moment = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    span = _days(moment, days)
    oldest = span[0]

    buckets: dict[date, dict[str, int]] = {
        day: {outcome: 0 for outcome in OUTCOMES} for day in span
    }
    totals = {outcome: 0 for outcome in OUTCOMES}
    other_status_count = 0

    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    # One BOUNDED read of the history, then one pass over it. This used to
    # call `store.list_generations`, which has no LIMIT and no projection and
    # so moved every checkpoint's whole metadata document to count a status
    # per day -- 88 rows and 131 MB, 1.65 s, on okg-workspace (issue #2202).
    #
    # The chart covers a SPAN, not a count, so the scan is narrowed by the
    # `created_at` column rather than by a row count. That column is text and
    # the store already orders it lexically, so the bound carries a two-day
    # margin: a checkpoint written in another UTC offset still lands inside
    # the scan, and the exact in-range test below is the one that decides.
    scan_from = (span[0] - timedelta(days=_SCAN_MARGIN_DAYS)).isoformat()
    recent = store.recent_generations(
        deployment_name=deployment_name,
        branch=session.branch_name,
        status=None,
        keys=ATTEMPTS_BY_DAY_METADATA_KEYS,
        since=scan_from,
    )
    history = recent.rows
    # `undated_count` is a fact about the WHOLE branch, not about the span, so
    # it cannot come from a scan bounded by `created_at`: a checkpoint whose
    # stamp is unreadable may sort anywhere. It is counted separately, without
    # parsing any metadata, and is None when the backend cannot answer -- the
    # page says "not counted" rather than printing a zero nobody measured.
    undated_count = store.count_undated_checkpoints(branch=session.branch_name)
    for generation in history:
        stamp = parse_checkpoint_created_at(generation.created_at)
        if stamp is None:
            # Counted branch-wide above, not here: this loop only sees the
            # scanned window, so tallying here would report a span's worth of
            # unreadable stamps as the branch's.
            continue
        day = stamp.date()
        if day < oldest or day > span[-1]:
            continue
        status = str((generation.metadata or {}).get("status") or "")
        if status not in OUTCOMES:
            other_status_count += 1
            continue
        buckets[day][status] += 1
        totals[status] += 1

    return {
        "deployment": deployment_name,
        "range": requested_range,
        "now": moment.isoformat(),
        "buckets": [
            {"day": day.isoformat(), **buckets[day]} for day in span
        ],
        "totals": dict(totals),
        # Attempts the buckets do NOT account for, named rather than dropped:
        # an attempt whose status is not one of the three outcomes, and one
        # whose created_at could not be read. Both are zero on a healthy
        # deployment; a non-zero value says the chart is not the whole history.
        "other_status_count": other_status_count,
        "undated_count": undated_count,
        # What the bounded scan looked at: the oldest `created_at` it would
        # accept, how many checkpoints it read and whether it stopped at its
        # row cap rather than at the start of the span.
        "scan_from": scan_from,
        "scanned": recent.scanned,
        "window_exhausted": recent.window_exhausted,
    }
