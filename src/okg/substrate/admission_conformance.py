"""Admission-envelope implementation conformance checks."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import psycopg
from psycopg import sql as pg_sql

from okg.substrate.admission_trace import (
    load_admission_traces,
    validate_admission_traces,
)
from okg.substrate.db.management import (
    _column_types,
    _dbos_stale_anchor,
    _stale_cutoff_value,
)


@dataclass(frozen=True)
class AdmissionConformanceFinding:
    name: str
    status: str
    code: str
    message: str
    count: int = 0
    samples: tuple[dict[str, Any], ...] = ()

    @property
    def passed(self) -> bool:
        return self.status == "OK"

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status,
            "code": self.code,
            "message": self.message,
            "count": self.count,
            "samples": list(self.samples),
        }


def audit_admission_conformance(
    conn: psycopg.Connection,
    *,
    deployment_name: str | None = None,
    generation_id: int | None = None,
    stale_after_seconds: int = 3600,
) -> list[AdmissionConformanceFinding]:
    """Run SQL and trace checks that refine the formal admission model.

    Chronos cutover: the three live-row coverage checks
    (`live_nodes_have_published_admission`,
    `live_edges_have_published_admission`,
    `rejected_admissions_not_visible`) were removed — they audited the
    legacy bitemporal projection (`nodes_live` / `edges_live` visibility
    windows joined to fact-id ranges), which Chronos obsoletes. The
    `generation_id` argument is retained for caller compatibility but is
    no longer consulted.
    """
    findings = [
        _check_fast_track_has_check_summary(conn, deployment_name),
        _check_fast_track_has_producer_policy(conn, deployment_name),
        _check_idempotency_unique(conn, deployment_name),
        _check_stale_pending_admissions(
            conn,
            deployment_name,
            stale_after_seconds=stale_after_seconds,
        ),
        _check_stale_pending_workflows(
            conn,
            deployment_name,
            stale_after_seconds=stale_after_seconds,
        ),
        _check_trace_refinement(conn, deployment_name),
    ]
    return findings


def _check_fast_track_has_check_summary(
    conn: psycopg.Connection,
    deployment_name: str | None,
) -> AdmissionConformanceFinding:
    rows = _fetch_dicts(
        conn,
        """
        SELECT admission_id::text, producer_id, status, check_summary
          FROM okg.admission_envelopes
         WHERE (
           %(deployment_name)s::text IS NULL
           OR deployment_name = %(deployment_name)s
         )
           AND admission_mode = 'fast_track'
           AND (
             check_summary = '{}'::jsonb
             OR NOT check_summary ? 'admission_policy'
           )
         ORDER BY created_at, admission_id
         LIMIT 20
        """,
        {"deployment_name": deployment_name},
    )
    return _finding_from_rows(
        name="fast_track_has_check_summary",
        ok_code="admission_fast_track_check_summary_present",
        fail_code="admission_fast_track_missing_check_summary",
        ok_message="fast-track admissions carry deterministic check summaries",
        fail_message="fast-track admissions are missing check summaries",
        rows=rows,
    )


def _check_fast_track_has_producer_policy(
    conn: psycopg.Connection,
    deployment_name: str | None,
) -> AdmissionConformanceFinding:
    rows = _fetch_dicts(
        conn,
        """
        SELECT admission_id::text, producer_id, status, check_summary
          FROM okg.admission_envelopes
         WHERE (
           %(deployment_name)s::text IS NULL
           OR deployment_name = %(deployment_name)s
         )
           AND admission_mode = 'fast_track'
           AND (
             NOT check_summary ? 'producer_policy'
             OR COALESCE(
                  check_summary->'producer_policy'->>'profile_fingerprint',
                  ''
                ) = ''
           )
         ORDER BY created_at, admission_id
         LIMIT 20
        """,
        {"deployment_name": deployment_name},
    )
    return _finding_from_rows(
        name="fast_track_has_producer_policy",
        ok_code="admission_fast_track_producer_policy_present",
        fail_code="admission_fast_track_missing_producer_policy",
        ok_message="fast-track admissions cite producer-profile evidence",
        fail_message="fast-track admissions are missing producer-profile evidence",
        rows=rows,
    )


def _check_idempotency_unique(
    conn: psycopg.Connection,
    deployment_name: str | None,
) -> AdmissionConformanceFinding:
    rows = _fetch_dicts(
        conn,
        """
        SELECT deployment_name, producer_id, idempotency_key,
               count(*)::int AS duplicate_count
          FROM okg.admission_envelopes
         WHERE (
           %(deployment_name)s::text IS NULL
           OR deployment_name = %(deployment_name)s
         )
         GROUP BY deployment_name, producer_id, idempotency_key
        HAVING count(*) > 1
         ORDER BY duplicate_count DESC, deployment_name, producer_id
         LIMIT 20
        """,
        {"deployment_name": deployment_name},
    )
    return _finding_from_rows(
        name="idempotency_unique",
        ok_code="admission_idempotency_unique",
        fail_code="admission_duplicate_idempotency_effect",
        ok_message="idempotency keys have at most one envelope effect",
        fail_message="duplicate idempotency effects were found",
        rows=rows,
    )


def _check_stale_pending_admissions(
    conn: psycopg.Connection,
    deployment_name: str | None,
    *,
    stale_after_seconds: int,
) -> AdmissionConformanceFinding:
    rows = _fetch_dicts(
        conn,
        """
        SELECT admission_id::text, producer_id, admission_mode,
               status, created_at
          FROM okg.admission_envelopes
         WHERE (
           %(deployment_name)s::text IS NULL
           OR deployment_name = %(deployment_name)s
         )
           AND status IN ('pending', 'accepted')
           AND created_at < now() - (%(stale_after_seconds)s || ' seconds')::interval
         ORDER BY created_at, admission_id
         LIMIT 20
        """,
        {
            "deployment_name": deployment_name,
            "stale_after_seconds": stale_after_seconds,
        },
    )
    return _finding_from_rows(
        name="stale_pending_admissions",
        ok_code="admission_no_stale_pending_envelopes",
        fail_code="admission_stale_pending_envelope",
        ok_message="no pending admission envelopes are stale",
        fail_message="pending admission envelopes exceeded the stale threshold",
        rows=rows,
    )


def _conformance_stale_anchor(
    column_types: dict[str, str],
) -> tuple[pg_sql.Composable, str]:
    """Pick the staleness anchor for the *detector*, not for the collector.

    The detector and the DBOS backlog GC caller in
    ``db/management.py`` deliberately want different anchors:

    * ``_dbos_stale_anchor`` (used by ``dbos_backlog_gc``) returns
      ``GREATEST(updated_at, created_at, started_at_epoch_ms)`` on the
      production DBOS shape. Cancelling a workflow is destructive, so the
      GC anchors on the *newest* evidence of life it can find and only
      reaps rows that look abandoned by every column at once. Over-reaping
      a live workflow is the expensive mistake there.
    * This check only *reports*. Its question is "has this workflow's
      status stopped advancing?", and ``updated_at`` is the column DBOS
      advances on each status transition. Folding in ``created_at`` or
      ``started_at_epoch_ms`` with ``GREATEST`` lets a fresh start time
      mask a two-hour-cold ``updated_at``, so a genuinely wedged workflow
      is reported healthy (count 0). Under-reporting a stuck workflow is
      the expensive mistake here.

    So anchor on ``updated_at`` whenever the column exists, and fall back
    to the shared helper's anchor only when it is absent.
    """
    if "updated_at" in column_types:
        return pg_sql.Identifier("updated_at"), column_types["updated_at"]
    stale_expr, _stale_column, stale_type = _dbos_stale_anchor(column_types)
    return stale_expr, stale_type


def _conformance_cutoff_value(stale_after_seconds: int, data_type: str) -> Any:
    """Cutoff bound to the anchor column's type, naive columns included.

    ``_stale_cutoff_value`` returns an aware UTC datetime for every
    non-numeric type. Against ``timestamp without time zone`` PostgreSQL
    coerces the aware value through the session ``TimeZone`` setting, so
    the effective cutoff shifts by the session offset: with
    ``TimeZone=America/New_York`` a stale row that UTC and Asia/Tokyo both
    catch is missed. The column stores UTC wall-clock, so bind a naive UTC
    datetime and let PostgreSQL compare wall-clock to wall-clock.
    """
    cutoff = _stale_cutoff_value(stale_after_seconds, data_type)
    if data_type == "timestamp without time zone" and isinstance(
        cutoff, datetime
    ):
        return cutoff.replace(tzinfo=None)
    return cutoff


def _check_stale_pending_workflows(
    conn: psycopg.Connection,
    deployment_name: str | None,
    *,
    stale_after_seconds: int,
) -> AdmissionConformanceFinding:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('dbos.workflow_status')")
        if cur.fetchone()[0] is None:
            return _ok(
                "stale_pending_workflows",
                "admission_dbos_workflow_status_unavailable",
                "dbos.workflow_status is unavailable; workflow staleness skipped",
            )
        column_types = _column_types(cur, "dbos", "workflow_status")
    if "updated_at" not in column_types and "created_at" not in column_types:
        return _ok(
            "stale_pending_workflows",
            "admission_dbos_workflow_status_no_timestamp",
            "dbos.workflow_status has no updated_at or created_at column; "
            "workflow staleness skipped",
        )
    stale_expr, stale_type = _conformance_stale_anchor(column_types)
    cutoff_value = _conformance_cutoff_value(stale_after_seconds, stale_type)
    queue_name = f"okg-{deployment_name}" if deployment_name else None
    rows = _fetch_dicts(
        conn,
        pg_sql.SQL(
            """
        SELECT workflow_uuid::text AS workflow_id, name, status,
               queue_name, {stale_expr} AS updated_at
          FROM dbos.workflow_status
         WHERE name IN (
           'okg_source_sync',
           'okg_publisher_cycle',
           'okg_admission_submit',
           'okg_admission_decide'
         )
           AND (%(queue_name)s::text IS NULL OR queue_name = %(queue_name)s)
           AND status IN ('PENDING', 'ENQUEUED')
           AND {stale_expr} < %(cutoff)s
         ORDER BY {stale_expr}, workflow_uuid
         LIMIT 20
        """
        ).format(stale_expr=stale_expr),
        {
            "queue_name": queue_name,
            "cutoff": cutoff_value,
        },
    )
    return _finding_from_rows(
        name="stale_pending_workflows",
        ok_code="admission_no_stale_pending_workflows",
        fail_code="admission_stale_pending_workflow",
        ok_message="no pending admission-related workflows are stale",
        fail_message="pending admission-related workflows are stale",
        rows=rows,
    )


def _check_trace_refinement(
    conn: psycopg.Connection,
    deployment_name: str | None,
) -> AdmissionConformanceFinding:
    traces = load_admission_traces(conn, deployment_name=deployment_name)
    result = validate_admission_traces(traces)
    if result.ok:
        return AdmissionConformanceFinding(
            name="trace_refines_formal_model",
            status="OK",
            code="admission_trace_refines_model",
            message="observed admission traces refine the formal model",
            count=len(traces),
            samples=({"actions": list(result.actions)[:20]},),
        )
    return AdmissionConformanceFinding(
        name="trace_refines_formal_model",
        status="FAIL",
        code="admission_trace_unmodeled_transition",
        message="observed admission traces contain unmodeled transitions",
        count=len(result.errors),
        samples=tuple(result.errors[:20]),
    )


def _fetch_dicts(
    conn: psycopg.Connection,
    sql: str | pg_sql.Composable,
    params: dict[str, Any],
) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(sql, params)
        return [_jsonable_row(dict(row)) for row in cur.fetchall()]


def _finding_from_rows(
    *,
    name: str,
    ok_code: str,
    fail_code: str,
    ok_message: str,
    fail_message: str,
    rows: list[dict[str, Any]],
) -> AdmissionConformanceFinding:
    if not rows:
        return _ok(name, ok_code, ok_message)
    return AdmissionConformanceFinding(
        name=name,
        status="FAIL",
        code=fail_code,
        message=fail_message,
        count=len(rows),
        samples=tuple(rows),
    )


def _ok(
    name: str,
    code: str,
    message: str,
) -> AdmissionConformanceFinding:
    return AdmissionConformanceFinding(
        name=name,
        status="OK",
        code=code,
        message=message,
    )


def _jsonable_row(row: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, datetime):
            out[key] = value.isoformat()
        else:
            out[key] = value
    return out
