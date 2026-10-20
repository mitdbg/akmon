"""Per-source sync-state composer for the console source rail (B4b).

`okg.source_progress` is a key/value store — one row per
(deployment, source_name, key), last-value-wins — NOT a time series.
So there is no honest per-poll "timeline" to show; instead we surface
the source's REAL current sync state: the parsed `sync.producer.poll`
summary (enqueue / error counts + last run) and every other progress
key (watermarks, fingerprints) with its value + age. That's the state
an operator actually needs to debug a stalled or erroring source.

Backs the right-rail source panel's detail section. Direct-SQL,
request-time — same trade-off as the sibling composers.
"""

from __future__ import annotations

import json
from typing import Any

from okg.substrate.console_views.today_workspace import _humanize_age
from okg.substrate.mcp.session import Session

# The poll key carries the {enqueue_count, error_count, last_run_at}
# summary; we parse it out rather than show it as a raw blob.
_POLL_KEY = "sync.producer.poll"

_SOURCE_PROGRESS_SQL = """
SELECT key, value, updated_at
FROM okg.source_progress
WHERE deployment = %(d)s AND source_name = %(s)s
ORDER BY key
"""


def _short(value: Any, limit: int = 96) -> str:
    s = " ".join(str(value).split())
    return s if len(s) <= limit else s[: limit - 1] + "…"


_COUNT_KEYS = ("enqueue_count", "error_count")


def _read_poll(value: Any, updated_at: Any) -> tuple[dict[str, Any] | None, str | None]:
    """The poll row as a summary, or None and the reason it could not be read.

    Nothing is substituted for a value the row does not hold. A value that is
    not JSON, not an object, or holds a count that is not a non-negative
    integer is unreadable, and says so; it is never read as zero. A count the
    poll did not record at all is null.
    """
    if isinstance(value, str):
        try:
            payload = json.loads(value)
        except json.JSONDecodeError:
            return None, "poll_value_not_json"
    else:
        payload = value
    if not isinstance(payload, dict):
        return None, "poll_value_not_an_object"
    counts: dict[str, int | None] = {}
    for key in _COUNT_KEYS:
        count = payload.get(key)
        if count is None:
            counts[key] = None
            continue
        # bool is an int subclass; `true` is not a count.
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            return None, "poll_count_not_an_integer"
        counts[key] = count
    return {
        **counts,
        "last_run_at": payload.get("last_run_at"),
        # The age of the progress ROW, which the poll rewrites each time it
        # runs. It is not derived from `last_run_at`, so it is not named for it.
        "recorded_age": _humanize_age(updated_at) if updated_at else None,
    }, None


def compose_source_detail(
    conn: Any,
    *,
    deployment_name: str,
    session: Session,
    source_name: str,
) -> dict[str, Any]:
    """The source's current sync-progress state from source_progress.

    `sync_progress_recorded` says whether `okg.source_progress` holds any row
    under this exact source name. It is not a record count and does not say
    whether the source exists or has published anything: a source can hold
    published records and no progress rows (issue #2118). Published records
    come from the producers read, which reads the maintained count rollup.
    """
    _ = session
    poll: dict[str, Any] | None = None
    poll_unreadable_reason: str | None = None
    progress: list[dict[str, Any]] = []
    row_count = 0

    with conn.cursor() as cur:
        cur.execute(
            _SOURCE_PROGRESS_SQL,
            {"d": deployment_name, "s": source_name},
        )
        for key, value, updated_at in cur.fetchall():
            row_count += 1
            if key == _POLL_KEY:
                poll, poll_unreadable_reason = _read_poll(value, updated_at)
                continue
            progress.append({
                "key": str(key),
                "value": _short(value),
                "age": _humanize_age(updated_at) if updated_at else None,
            })

    return {
        "source_name": source_name,
        "sync_progress_recorded": row_count > 0,
        "poll": poll,
        "poll_unreadable_reason": poll_unreadable_reason,
        "progress": progress,
    }
