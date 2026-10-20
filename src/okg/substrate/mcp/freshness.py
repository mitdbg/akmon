"""Per-source freshness for graph read responses.

worktree-access-tiers req.tiers.freshness-honesty: a pinned-generation
read is a snapshot; without watermark ages an agent mistakes it for the
present (the 2026-06-10 case: agent_session rows confidently answering
yesterday while the sessions source sat 15h stale behind a dead worker).
Every read-shaped MCP operator attaches this summary; staleness is a
first-class warning in the response, not a runbook fact.

Only a source the deployment polls on a schedule can fall behind it (#3666):
a source synced by hand, on a file or ref change, or by webhook has no
interval to miss, so its last-run age says nothing about staleness. Those
sources are counted as `unscheduled`, never named stale. When the registry
cannot be read, every source is judged and the summary says so.
"""

from __future__ import annotations

import datetime
import json
import os
import re
import time
from collections.abc import Collection
from typing import Any

READ_OPERATORS = {
    "query", "search", "inspect", "expand", "filter", "map", "aggregate",
}

_CACHE_TTL_S = 60.0
#: (deployment, database) -> (monotonic time stored, payload). One process
#: can read more than one deployment or database, so one's summary never
#: answers another's (#3666; census finding rf-read-4).
_cache: dict[tuple[str, tuple[Any, ...]], tuple[float, dict[str, Any]]] = {}


def _cache_key(conn: Any, deployment: str) -> tuple[str, tuple[Any, ...]]:
    info = getattr(conn, "info", None)
    database = (
        getattr(info, "host", None),
        getattr(info, "port", None),
        getattr(info, "dbname", None),
    )
    return deployment, database


def polled_source_keys(deployment: str) -> frozenset[str] | None:
    """Every name a poll-scheduled source of ``deployment`` is recorded under.

    A source is polled when its registry entry lists the `poll` trigger with
    a `sync.poll` schedule and is not dormant (`enabled_by_default: false`).
    Its progress rows may carry the registry key, the declared
    `ownership_id`, or `<deployment>.<key>`, so all three are returned.
    ``None`` when the deployment's source registry cannot be read: then no
    source's schedule is known.
    """
    from okg.substrate.mcp.source_admin import _load_registry_yaml

    try:
        _path, raw = _load_registry_yaml(deployment)
    except Exception:  # noqa: BLE001 - reported as schedules unknown.
        return None
    entries = raw.get("sources") if isinstance(raw, dict) else None
    if not isinstance(entries, dict):
        return None
    keys: set[str] = set()
    for source_id, entry in entries.items():
        body = entry if isinstance(entry, dict) else {}
        sync = body.get("sync")
        if body.get("enabled_by_default") is False or not isinstance(sync, dict):
            continue
        triggers = sync.get("triggers") or ()
        if "poll" not in triggers or not isinstance(sync.get("poll"), dict):
            continue
        keys.update({
            str(source_id),
            str(body.get("ownership_id") or source_id),
            f"{deployment}.{source_id}",
        })
    return frozenset(keys)


def unavailable_freshness(
    *, deployment: str, cause: str, omitted_work: str,
) -> dict[str, Any]:
    """Return the stable public shape for an unreadable freshness signal."""
    return {
        "status": "unavailable",
        "cause": cause,
        "omitted_work": [omitted_work],
        "scope": {"deployment": deployment},
        "retryable": True,
        "freshness": "unavailable",
    }


def stale_threshold_hours() -> float:
    try:
        return float(os.environ.get("OKG_FRESHNESS_STALE_HOURS", "6"))
    except ValueError:
        return 6.0


def summarize_watermarks(
    rows: list[tuple[str, str | None]],
    *,
    now: datetime.datetime,
    threshold_h: float,
    polled: Collection[str] | None = None,
) -> dict[str, Any]:
    """Pure summary: (source, completed_at iso) -> freshness payload.

    ``polled`` names the sources on a poll schedule; any other row is
    counted under ``unscheduled`` and never called stale. ``None`` judges
    every row (a caller that knows no registry).
    """
    ages: list[dict[str, Any]] = []
    # Exact ages, for ordering: the rounded ones tie, and sources polled by
    # one run all do.
    exact: dict[int, float] = {}
    unscheduled = 0
    for source, completed_at in rows:
        if polled is not None and source not in polled:
            unscheduled += 1
            continue
        if not completed_at:
            ages.append({"source": source, "age_hours": None})
            continue
        try:
            ts = datetime.datetime.fromisoformat(completed_at)
        except ValueError:
            ages.append({"source": source, "age_hours": None})
            continue
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=datetime.timezone.utc)
        hours = (now - ts).total_seconds() / 3600
        exact[len(ages)] = hours
        ages.append({"source": source, "age_hours": round(hours, 1)})
    stale_at = [
        i for i, a in enumerate(ages)
        if a["age_hours"] is None or a["age_hours"] > threshold_h
    ]
    # Never polled first, then oldest first, then by name: a total order, so
    # the five named below are the same on every server and every call while
    # the sources' state is unchanged (the rows arrive in no fixed order).
    # The session's once-only stale notice is keyed by those names (#3635).
    stale_at.sort(key=lambda i: (
        i in exact, -exact.get(i, 0.0), str(ages[i]["source"]),
    ))
    stale = [ages[i] for i in stale_at]
    payload: dict[str, Any] = {
        "sources_checked": len(ages) + unscheduled,
        "threshold_hours": threshold_h,
        "stale": stale[:5],
        "stale_count": len(stale),
    }
    if unscheduled:
        payload["unscheduled"] = unscheduled
    if stale:
        names = ", ".join(
            f"{a['source']} ({a['age_hours']}h)"
            if a["age_hours"] is not None
            else f"{a['source']} (never)"
            for a in stale[:3]
        )
        payload["warning"] = (
            f"{len(stale)} source(s) exceed the {threshold_h}h freshness "
            f"threshold: {names} - rows from their subtypes reflect the "
            "last sync, not the present"
        )
    return payload


def source_freshness(conn, deployment: str) -> dict[str, Any] | None:
    """Cached per-source summary or a typed unavailable result.

    Cached per deployment and database for `_CACHE_TTL_S`; an unavailable result is
    never cached. When the source registry cannot be read no source's
    schedule is known, so every source is judged, as before #3666, and the
    payload says `schedules: unknown`.
    """
    key = _cache_key(conn, deployment)
    cached = _cache.get(key)
    if cached is not None and time.monotonic() - cached[0] < _CACHE_TTL_S:
        return cached[1]
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT source_name, value FROM okg.source_progress "
                "WHERE deployment = %s AND key = 'sync.last_run'",
                (deployment,),
            )
            raw = cur.fetchall()
    except Exception:  # noqa: BLE001 - freshness must never break a read.
        return unavailable_freshness(
            deployment=deployment,
            cause="source_freshness_query_failed",
            omitted_work="source_watermark_summary",
        )
    rows: list[tuple[str, str | None]] = []
    for source, value in raw:
        completed: str | None = None
        try:
            payload = json.loads(value) if isinstance(value, str) else value
            completed = (payload or {}).get("completed_at") or (
                payload or {}
            ).get("finished_at")
        except (ValueError, TypeError):
            completed = None
        rows.append((source, completed))
    polled = polled_source_keys(deployment) if rows else frozenset()
    summary = summarize_watermarks(
        rows,
        now=datetime.datetime.now(datetime.timezone.utc),
        threshold_h=stale_threshold_hours(),
        polled=polled,
    )
    if polled is None:
        # Never fresh by default: an unreadable registry judges every source,
        # as before #3666, and says the schedules were not known.
        summary["schedules"] = "unknown"
        if "warning" in summary:
            summary["warning"] += " (poll schedules unknown: every source judged)"
    _cache[key] = (time.monotonic(), summary)
    return summary


# --------------------------------------------------------------------------
# Publish-staleness signal (harden-okg-ci-runtime req.self-staleness-signal)
#
# source_freshness above measures last source POLL. The 2026-06-16 okg-ci
# outage showed that is not enough: the publisher was wedged for ~5h while
# polls kept landing every few minutes, so source_freshness read green
# (stale_count=0) the whole time. The signal that catches a publish stall is
# the LAG between the newest source poll and the latest PUBLISHED generation:
# polled-but-unpublished work means reads are serving the past. Comparing the
# two (rather than a raw published-gen age) avoids false-positives on a
# quiet-but-healthy deployment, where the newest poll is no fresher than the
# latest publish so the lag is ~0.
# --------------------------------------------------------------------------


def publish_stale_threshold_seconds() -> float:
    try:
        return float(os.environ.get("OKG_PUBLISH_STALE_SECONDS", "1800"))
    except ValueError:
        return 1800.0


def parse_generation_timestamp(
    generation_id: str | None,
) -> datetime.datetime | None:
    """Extract the publish timestamp embedded in a generation id of the form
    ``okg:<dep>:branch:<branch>:gen:<YYYYMMDDTHHMMSSffffffZ>:<hash>``."""
    if not generation_id or ":gen:" not in generation_id:
        return None
    try:
        stamp = generation_id.split(":gen:", 1)[1].split(":", 1)[0]
        ts = datetime.datetime.strptime(stamp, "%Y%m%dT%H%M%S%fZ")
        return ts.replace(tzinfo=datetime.timezone.utc)
    except (ValueError, IndexError):
        return None


def summarize_publish_lag(
    *,
    latest_publish: datetime.datetime | None,
    newest_poll: datetime.datetime | None,
    now: datetime.datetime,
    threshold_s: float,
) -> dict[str, Any] | None:
    """Pure summary: is publishing lagging behind the freshest source poll?"""
    if latest_publish is None:
        return None
    publish_age_s = round((now - latest_publish).total_seconds(), 1)
    lag_s: float | None = None
    if newest_poll is not None:
        lag_s = round(
            max(0.0, (newest_poll - latest_publish).total_seconds()), 1,
        )
    stale = lag_s is not None and lag_s > threshold_s
    payload: dict[str, Any] = {
        "latest_published_at": latest_publish.isoformat(),
        "publish_age_seconds": publish_age_s,
        "newest_source_poll_at": (
            newest_poll.isoformat() if newest_poll else None
        ),
        "publish_lag_seconds": lag_s,
        "threshold_seconds": threshold_s,
        "stale": stale,
    }
    if stale:
        payload["warning"] = (
            f"publishing is stalled: the newest source poll is {lag_s:.0f}s "
            f"ahead of the latest published generation (> {threshold_s:.0f}s "
            "threshold) - reads serve a generation that predates "
            "polled-but-unpublished work; check the publisher/worker"
        )
    return payload


def publish_freshness(
    conn, deployment: str, pinned_generation_id: str | None,
) -> dict[str, Any]:
    """Publish-staleness signal: latest published generation vs newest source
    poll, or a typed unavailable result when the pin has no usable timestamp."""
    latest_publish = parse_generation_timestamp(pinned_generation_id)
    if latest_publish is None:
        return unavailable_freshness(
            deployment=deployment,
            cause="publish_generation_timestamp_unavailable",
            omitted_work="publish_lag_summary",
        )
    newest_poll: datetime.datetime | None = None
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT source_name, value FROM okg.source_progress "
                "WHERE deployment = %s AND key = 'sync.last_run'",
                (deployment,),
            )
            raw = cur.fetchall()
        for _source, value in raw:
            try:
                pld = json.loads(value) if isinstance(value, str) else value
                completed = (pld or {}).get("completed_at") or (
                    pld or {}
                ).get("finished_at")
            except (ValueError, TypeError):
                completed = None
            if not completed:
                continue
            try:
                ts = datetime.datetime.fromisoformat(completed)
            except ValueError:
                continue
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=datetime.timezone.utc)
            if newest_poll is None or ts > newest_poll:
                newest_poll = ts
    except Exception:  # noqa: BLE001 - freshness must never break a read.
        return unavailable_freshness(
            deployment=deployment,
            cause="publish_freshness_query_failed",
            omitted_work="publish_lag_summary",
        )
    summary = summarize_publish_lag(
        latest_publish=latest_publish,
        newest_poll=newest_poll,
        now=datetime.datetime.now(datetime.timezone.utc),
        threshold_s=publish_stale_threshold_seconds(),
    )
    assert summary is not None
    return summary


_ORDER_BY_RE = re.compile(
    r"order\s+by\s+(?:[a-z0-9_.]+\s*\(\s*)?(?:n|e)?\.?"
    r"(?:attrs\s*->>\s*'(?P<attr>[^']+)'|(?P<col>[a-z0-9_]+))",
    re.IGNORECASE,
)


def null_ordering_warning(
    sql: str, columns: list[str], rows: list[list[Any]],
) -> str | None:
    """Flag a sort key that is NULL across the entire result set.

    The 2026-06-10 case: ORDER BY attrs->>'last_active_at' over rows
    where that attribute never exists - LIMIT then returns an arbitrary
    sample that reads like a ranking. Heuristic: only fires when the
    sort key is also a selected column, so it can be checked honestly.
    """
    if not rows or not columns:
        return None
    match = _ORDER_BY_RE.search(sql or "")
    if not match:
        return None
    key = match.group("attr") or match.group("col")
    if key not in columns:
        return None
    idx = columns.index(key)
    if all(row[idx] is None for row in rows):
        return (
            f"ordering warning: sort key {key!r} is NULL on every returned "
            "row - this ordering is meaningless and LIMIT returns an "
            "arbitrary sample"
        )
    return None
