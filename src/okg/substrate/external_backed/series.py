"""node_series writer + windowed reader with retention-aware state.

The writer maintains a per-(node_id, metric) earliest-retained watermark
(``okg.node_series_watermark.first_observed_at``) that survives chunk eviction.
The reader uses it to return a series window as one of three states:

  * ``data``     — rows are present in the requested range.
  * ``aged_out`` — the range existed but was evicted by retention (empty, and
                   the range lies at/after first-seen but before the oldest
                   surviving sample, or all samples are gone).
  * ``no_data``  — the range never existed (before first-seen, or the metric
                   was never reported) OR is a genuine gap within the retained
                   window.

Coercing empty-equals-aged_out is forbidden — that would be a default-value
fallback (a P0 here).
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

import psycopg


def _now() -> datetime:
    return datetime.now(timezone.utc)


def record_sample(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    node_id: str,
    metric: str,
    value: float,
    observed_at: Optional[datetime] = None,
    source: Optional[str] = None,
    status: str = "ok",
) -> None:
    """Append a sample and maintain the monotonic-min first_observed_at watermark."""
    observed_at = observed_at or _now()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.node_series "
            "(deployment_name, node_id, metric, observed_at, value, source, status) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (deployment_name, node_id, metric, observed_at, value, source, status),
        )
        cur.execute(
            """
            INSERT INTO okg.node_series_watermark
              (deployment_name, node_id, metric, first_observed_at)
            VALUES (%s,%s,%s,%s)
            ON CONFLICT (deployment_name, node_id, metric) DO UPDATE
              SET first_observed_at = LEAST(
                    okg.node_series_watermark.first_observed_at, EXCLUDED.first_observed_at),
                  updated_at = now()
            """,
            (deployment_name, node_id, metric, observed_at),
        )


def read_series_window(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    node_id: str,
    metric: str,
    start: datetime,
    end: datetime,
) -> dict[str, Any]:
    """Read a series window, returning a retention-aware state (never a default)."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT observed_at, value FROM okg.node_series "
            "WHERE deployment_name=%s AND node_id=%s AND metric=%s "
            "AND observed_at >= %s AND observed_at <= %s "
            "ORDER BY observed_at",
            (deployment_name, node_id, metric, start, end),
        )
        rows = [{"observed_at": r[0].isoformat(), "value": r[1]} for r in cur.fetchall()]
        if rows:
            return {"state": "data", "rows": rows}

        cur.execute(
            "SELECT first_observed_at FROM okg.node_series_watermark "
            "WHERE deployment_name=%s AND node_id=%s AND metric=%s",
            (deployment_name, node_id, metric),
        )
        wm = cur.fetchone()
        first_seen = wm[0] if wm else None

        cur.execute(
            "SELECT min(observed_at) FROM okg.node_series "
            "WHERE deployment_name=%s AND node_id=%s AND metric=%s",
            (deployment_name, node_id, metric),
        )
        current_min = cur.fetchone()[0]

    if first_seen is None:
        return {"state": "no_data", "reason": "metric_never_reported", "rows": []}
    if end < first_seen:
        return {"state": "no_data", "reason": "before_first_sample", "rows": []}
    # Range is at/after first-seen and empty.
    if current_min is None:
        return {"state": "aged_out", "first_observed_at": first_seen.isoformat(), "rows": []}
    if end < current_min:
        return {
            "state": "aged_out",
            "first_observed_at": first_seen.isoformat(),
            "earliest_retained_at": current_min.isoformat(),
            "rows": [],
        }
    return {"state": "no_data", "reason": "gap_within_retained_window", "rows": []}
