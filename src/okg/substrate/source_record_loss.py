"""Find sources whose published record count collapsed (issue #3783).

Bug #3776 moved almost every file record of two git-files sources to another
source. The graph kept its total, readers lost the records under their real
owner, and ``okg doctor`` read ok before, during and after. This probe
compares each source's record count in the newest published generation with
the largest count it had in the published generations of the last
``WINDOW_DAYS`` days, and names every source that fell to zero or lost more
than a set share.

The counts come from the console count rollup the publisher maintains for
each generation (the ``source`` dimension: nodes grouped by their source).
Each read is one as-of statement over that small table; the probe never scans
the graph. A generation whose rollup was never maintained is named as
unreadable, never read as zero. A database error in any read (a timeout, a
lost connection) propagates, so the caller reports a failed probe.

A drop can be deliberate: a source retired, renamed, or narrowed in scope.
The probe cannot tell that from a loss, so the caller reports a warning, not
an error, and the warning stays until the generations that held the records
leave the window. Two kinds of name are left out: a source whose retirement
on the default branch is newer than its baseline peak, and names starting
with ``_`` (internal producers such as the publisher's derived nodes and
enrichers).
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

#: Published generations newer than this many days before now form the baseline.
WINDOW_DAYS = 7

#: At most this many published generations before the newest one are read.
#: When the window holds more, they are spread evenly across it.
MAX_BASELINE_GENERATIONS = 50

#: A source whose count falls by more than this share of its baseline is
#: reported. A fall to zero is always reported.
DROP_SHARE = 0.5

#: Baselines smaller than this are reported only on a fall to zero.
MIN_BASELINE_FOR_SHARE = 20

#: Statement timeout for each statement of the probe, in milliseconds.
TIMEOUT_MS = 15_000

_BRANCH = "default"


def _published_sql() -> str:
    from okg.substrate.ingest.run_base import EXCLUDE_RUN_BASE_SQL

    return f"""
SELECT checkpoint_id, created, in_window, total
  FROM (
    SELECT checkpoint_id,
           created_at::timestamptz AS created,
           created_at::timestamptz >= now() - make_interval(days => %(days)s)
             AS in_window,
           row_number() OVER (
             ORDER BY created_at::timestamptz DESC, checkpoint_id DESC
           ) AS rn,
           count(*) OVER () AS total
      FROM public._chronos_branch_interval_checkpoints
     WHERE metadata::jsonb->>'deployment_name' = %(deployment)s
       AND COALESCE(metadata::jsonb->>'branch', 'default') = %(branch)s
       AND metadata::jsonb->>'status' = 'published'
       AND {EXCLUDE_RUN_BASE_SQL}
  ) published
 WHERE rn = 1 OR in_window
 ORDER BY rn
"""


def _spread(items: list[Any], limit: int) -> list[Any]:
    """At most ``limit`` items spread evenly over ``items``, keeping the first
    and the last, in their order."""
    if len(items) <= limit:
        return list(items)
    if limit <= 1:
        return list(items[:limit])
    last = len(items) - 1
    picked = sorted({round(i * last / (limit - 1)) for i in range(limit)})
    return [items[i] for i in picked]


_RETIRED_SQL = """
SELECT registry_source_id, ownership_id, COALESCE(published_at, applied_at)
  FROM okg.source_retirement_receipts
 WHERE deployment_name = %(deployment)s
   AND branch_name = %(branch)s
   AND status <> 'planned'
"""


def _is_internal(source: str) -> bool:
    return source.startswith("_")


def _database_error(exc: BaseException) -> BaseException | None:
    """The database error behind a "rollup unavailable" refusal, if any.

    The maintained-derivation read turns every failure of its statement into
    "unavailable", a query timeout included. A rollup that was never
    maintained has no database error behind it; a read that failed does.
    """
    import psycopg

    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        if isinstance(current, psycopg.Error):
            return current
        seen.add(id(current))
        current = current.__cause__ or current.__context__
    return None


def source_record_loss(
    conn: Any,
    deployment: str,
    *,
    window_days: int | None = None,
    max_baseline_generations: int | None = None,
    drop_share: float | None = None,
    min_baseline_for_share: int | None = None,
) -> dict[str, Any]:
    """Compare the newest published generation's per-source counts with the
    largest count each source had in the published generations of the last
    ``window_days`` days (at most ``max_baseline_generations`` of them).

    Returns a report with ``ok`` false and one ``losses`` entry per source
    that fell to zero, or lost more than ``drop_share`` of a baseline of at
    least ``min_baseline_for_share`` records. A check that cannot compare
    (counts disabled, no earlier published generation in the window, no
    maintained counts) returns ``skipped`` with a code saying why. A database
    error propagates: the caller reports it as a failed probe, never as ok.
    """
    from okg.substrate.console_views.console_count_rollup_read import (
        COUNT_KIND_SOURCE,
        ConsoleCountRollupUnavailable,
        console_count_rollup_enabled,
        read_console_count_rollup,
    )
    from okg.substrate.errors import GenerationCompacted

    # Defaults are read at call time, so the module constants decide.
    if window_days is None:
        window_days = WINDOW_DAYS
    if max_baseline_generations is None:
        max_baseline_generations = MAX_BASELINE_GENERATIONS
    if drop_share is None:
        drop_share = DROP_SHARE
    if min_baseline_for_share is None:
        min_baseline_for_share = MIN_BASELINE_FOR_SHARE
    thresholds = {
        "window_days": window_days,
        "max_baseline_generations": max_baseline_generations,
        "drop_share": drop_share,
        "min_baseline_for_share": min_baseline_for_share,
    }
    if not console_count_rollup_enabled():
        return {
            "ok": True,
            "skipped": True,
            "code": "source_record_loss_skipped_counts_disabled",
            "thresholds": thresholds,
        }
    with conn.cursor() as cur:
        cur.execute(
            "SELECT set_config('statement_timeout', %s, false)", (str(TIMEOUT_MS),)
        )
        cur.execute(
            _published_sql(),
            {"deployment": deployment, "branch": _BRANCH, "days": window_days},
        )
        rows = cur.fetchall()
    if not rows:
        return {
            "ok": True,
            "skipped": True,
            "code": "source_record_loss_skipped_no_previous_generation",
            "published_generations": 0,
            "previous_generations_in_window": 0,
            "thresholds": thresholds,
        }
    # The newest published generation is compared whatever its age. The
    # baseline is the earlier ones inside the window; when there are more
    # than the cap, a sample spread evenly over the window (newest and
    # oldest included), so a frequently published graph keeps its whole
    # window and not only its last few hours.
    newest = (str(rows[0][0]), rows[0][1])
    in_window = [(str(row[0]), row[1]) for row in rows[1:]]
    previous = _spread(in_window, max_baseline_generations)
    if not previous:
        return {
            "ok": True,
            "skipped": True,
            "code": "source_record_loss_skipped_no_previous_generation",
            "published_generations": int(rows[0][3]),
            "newest_generation": newest[0],
            "newest_generation_in_window": bool(rows[0][2]),
            "previous_generations_in_window": 0,
            "thresholds": thresholds,
        }
    checkpoints = [newest, *previous]

    def counts_at(checkpoint: str) -> dict[str, int]:
        counts = read_console_count_rollup(
            conn, generation_id=checkpoint, kinds={COUNT_KIND_SOURCE}
        )
        return {
            value: count
            for (_kind, value), count in counts.items()
            if not _is_internal(value)
        }

    def readable(exc: BaseException) -> bool:
        # A compacted generation or a rollup that was never maintained is a
        # gap in history; a database error is a failed read.
        return _database_error(exc) is None

    (current_id, _), previous = checkpoints[0], checkpoints[1:]
    try:
        current = counts_at(current_id)
    except (ConsoleCountRollupUnavailable, GenerationCompacted) as exc:
        if not readable(exc):
            raise
        return {
            "ok": True,
            "skipped": True,
            "code": "source_record_loss_skipped_no_counts",
            "generation": current_id,
            "reason": f"{type(exc).__name__}: {exc}",
            "thresholds": thresholds,
        }
    created = dict(checkpoints)
    baseline: dict[str, tuple[int, str]] = {}
    compared: list[str] = []
    unreadable: list[dict[str, str]] = []
    for checkpoint, _created_at in previous:
        try:
            counts = counts_at(checkpoint)
        except (ConsoleCountRollupUnavailable, GenerationCompacted) as exc:
            if not readable(exc):
                raise
            unreadable.append(
                {"generation": checkpoint, "reason": type(exc).__name__}
            )
            continue
        compared.append(checkpoint)
        for source, count in counts.items():
            if count > baseline.get(source, (-1, ""))[0]:
                baseline[source] = (count, checkpoint)
    if not compared:
        return {
            "ok": True,
            "skipped": True,
            "code": "source_record_loss_skipped_no_counts",
            "generation": current_id,
            "unreadable_generations": unreadable,
            "thresholds": thresholds,
        }

    retired_at: dict[str, datetime | None] = {}
    with conn.cursor() as cur:
        cur.execute(_RETIRED_SQL, {"deployment": deployment, "branch": _BRANCH})
        for registry_id, ownership_id, when in cur.fetchall():
            for name in (registry_id, ownership_id):
                if name:
                    retired_at[str(name)] = when

    losses: list[dict[str, Any]] = []
    retired_drops: list[str] = []
    for source in sorted(baseline):
        before, seen_in = baseline[source]
        now = current.get(source, 0)
        if before <= 0 or now >= before:
            continue
        lost_share = (before - now) / before
        if not (
            now == 0
            or (before >= min_baseline_for_share and lost_share > drop_share)
        ):
            continue
        # A retirement counts only when it came after the records were seen:
        # a source retired, re-added and then emptied again is reported.
        when = retired_at.get(source)
        if when is not None and when > created[seen_in]:
            retired_drops.append(source)
            continue
        losses.append(
            {
                "source": source,
                "records_before": before,
                "records_now": now,
                "lost_share": round(lost_share, 4),
                "baseline_generation": seen_in,
            }
        )
    return {
        "ok": not losses,
        "generation": current_id,
        "previous_generations_in_window": len(in_window),
        "compared_generations": compared,
        "unreadable_generations": unreadable,
        "sources_checked": len(baseline),
        "losses": losses,
        "retired_sources_skipped": retired_drops,
        "thresholds": thresholds,
    }
