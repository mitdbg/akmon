"""Durable per-source record of the last record-set coverage outcome.

A source whose run fails its record-set coverage check leaves no typed fact
behind. The failure lives in the raised message and, unreliably, in the DBOS
workflow error — so nothing downstream can ask the plain question "is this
source currently failing coverage?".

That gap has an operational cost. On okg-workspace (2026-09-04) the file
watcher kept bundling `agent-sessions` — which was failing coverage — into
multi-source workflows. When the run died partway, the incomplete checkpoint
recorded an EXACT repair contract naming every source in the bundle, and
every other queued workflow refused with a contract mismatch until an
operator intervened. One failing source wedged the deployment.

This module records the outcome under the `sync.record_set_coverage` progress
key after every source run — failing and passing alike, so recovery clears
the flag by itself — and reads it back for the watcher's exclusion filter.

The record is a diagnostic, not an authority. Nothing gates admission or
publication on it; it only keeps a source that is currently failing out of
OTHER sources' repair contracts.
"""
from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any


__all__ = [
    "COVERAGE_STATE_KEY",
    "COVERAGE_STATE_SCHEMA",
    "coverage_state_payload",
    "sources_failing_record_set_coverage",
]


log = logging.getLogger(__name__)


COVERAGE_STATE_KEY = "sync.record_set_coverage"
COVERAGE_STATE_SCHEMA = "okg.source_sync.record_set_coverage.v1"


def coverage_state_payload(
    *,
    deployment: str,
    source_name: str,
    result: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Build the coverage-state row for one finished source run.

    Returns ``None`` when the run's outcome says nothing about coverage — a
    result with no status at all. A run that succeeded records
    ``failing: False``, which is how a repaired source clears itself: the
    flag is derived from the newest run, never from an operator remembering
    to reset it.
    """
    from okg.substrate.ingest.runner import RECORD_SET_COVERAGE_MARKER

    status = result.get("status")
    if status is None:
        return None
    error_text = str(result.get("error") or "")
    ok = str(status).upper() == "OK"
    failing = (not ok) and RECORD_SET_COVERAGE_MARKER in error_text
    payload: dict[str, Any] = {
        "schema": COVERAGE_STATE_SCHEMA,
        "deployment": deployment,
        "source_name": source_name,
        "failing": failing,
        "status": str(status),
        "run_id": str(result.get("run_id") or "") or None,
        "observed_at": datetime.now(timezone.utc).isoformat(),
    }
    if failing:
        payload["error"] = error_text
    return payload


def sources_failing_record_set_coverage(
    *,
    dsn: str,
    deployment: str,
) -> set[str]:
    """Sources whose most recent run failed its record-set coverage check.

    Raises on a database error rather than returning an empty set: an empty
    set means "no source is failing", and a caller that cannot tell that
    apart from "the question could not be asked" would silently drop the
    protection. The watcher catches this and says so.

    A row that cannot be parsed is logged and skipped, not counted as
    failing. This record decides only whether a source shares a workflow with
    others; an unreadable row is not evidence that the source is broken.
    """
    from okg.substrate.db.pool import GENERAL, pooled_connection

    failing: set[str] = set()
    with pooled_connection(dsn, purpose=GENERAL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT source_name, value
                  FROM okg.source_progress
                 WHERE deployment = %s
                   AND key = %s
                """,
                (deployment, COVERAGE_STATE_KEY),
            )
            rows = cur.fetchall()
    for source_name, value in rows:
        try:
            payload = json.loads(value) if isinstance(value, str) else value
        except (TypeError, ValueError):
            # An unreadable row is NOT reported as failing: this record only
            # decides whether a source is bundled with others, and a garbled
            # row is not evidence that the source is broken. Logged so it is
            # visible rather than absorbed.
            log.warning(
                "record-set coverage state for source=%s is not JSON; "
                "this source is not treated as failing on the strength of a "
                "row that cannot be read",
                source_name,
            )
            continue
        if isinstance(payload, Mapping) and bool(payload.get("failing")):
            failing.add(str(source_name))
    return failing
