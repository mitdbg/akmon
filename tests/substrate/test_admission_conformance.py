"""Admission conformance tests."""
from __future__ import annotations

import psycopg
import pytest

from okg.substrate.admission import ensure_envelope
from okg.substrate.admission_conformance import audit_admission_conformance


pytestmark = pytest.mark.postgres


# Chronos cutover: the live-row coverage tests
# (`*_passes_for_published_node`, `*_flags_live_node_without_admission`,
# `*_flags_source_edge_without_admission`,
# `*_flags_derived_edge_without_admission`,
# `*_flags_rejected_visible_fact`) and the doctor test asserting
# `admission_live_node_without_published_admission` were removed: they
# exercised the legacy bitemporal projection (nodes_live / edges_live
# visibility windows joined to admission fact-id ranges), which the
# Chronos cutover obsoletes along with those checks.


def test_admission_conformance_flags_fast_track_without_summary(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.transaction():
            ensure_envelope(
                conn,
                deployment_name="conf_summary",
                producer_id="summary_source",
                producer_kind="source",
                intent="source_sync",
                admission_mode="fast_track",
                status="pending",
                idempotency_key="summary-key",
            )
        findings = audit_admission_conformance(
            conn,
            deployment_name="conf_summary",
        )

    finding = _finding(findings, "fast_track_has_check_summary")
    assert finding.status == "FAIL"
    assert finding.code == "admission_fast_track_missing_check_summary"


def test_admission_conformance_flags_fast_track_without_producer_policy(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.transaction():
            ensure_envelope(
                conn,
                deployment_name="conf_producer_policy",
                producer_id="policy_source",
                producer_kind="source",
                intent="source_sync",
                admission_mode="fast_track",
                status="pending",
                idempotency_key="producer-policy-key",
                check_summary={
                    "admission_policy": {"declared": True},
                },
            )
        findings = audit_admission_conformance(
            conn,
            deployment_name="conf_producer_policy",
        )

    finding = _finding(findings, "fast_track_has_producer_policy")
    assert finding.status == "FAIL"
    assert finding.code == "admission_fast_track_missing_producer_policy"


def test_admission_conformance_flags_stale_pending_envelope(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.transaction():
            envelope = ensure_envelope(
                conn,
                deployment_name="conf_stale",
                producer_id="stale_source",
                producer_kind="source",
                intent="source_sync",
                admission_mode="fast_track",
                status="pending",
                idempotency_key="stale-key",
                check_summary={
                    "admission_policy": {"declared": True},
                },
            )
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE okg.admission_envelopes
                   SET created_at = now() - interval '2 hours'
                 WHERE admission_id = %s::uuid
                """,
                (envelope.admission_id,),
            )
        findings = audit_admission_conformance(
            conn,
            deployment_name="conf_stale",
            stale_after_seconds=1,
        )

    finding = _finding(findings, "stale_pending_admissions")
    assert finding.status == "FAIL"
    assert finding.code == "admission_stale_pending_envelope"


@pytest.mark.parametrize(
    ("timestamp_ddl", "stale_value_sql", "session_timezone"),
    [
        ("timestamptz", "now() - interval '2 hours'", None),
        (
            "bigint",
            "(extract(epoch from now()) * 1000)::bigint - 7200000",
            None,
        ),
        (
            "timestamp",
            "(now() at time zone 'UTC') - interval '2 hours'",
            "America/New_York",
        ),
    ],
    ids=["timestamptz", "bigint", "naive_timestamp_non_utc_session"],
)
def test_admission_conformance_flags_stale_workflow_for_either_column_type(
    clean_runtime: str,
    timestamp_ddl: str,
    stale_value_sql: str,
    session_timezone: str | None,
) -> None:
    """The staleness cutoff must match the dbos.workflow_status column type.

    Upstream DBOS declares ``updated_at`` as epoch milliseconds, but the
    operator-UI audit fixture creates the table with ``timestamptz``. A
    cutoff hard-coded to epoch milliseconds raised
    ``operator does not exist: timestamp with time zone < numeric``
    against the timestamptz shape (issue #1529).

    The third arm covers ``timestamp without time zone``, which stores UTC
    wall-clock with no offset. Binding an aware UTC cutoff there makes
    PostgreSQL coerce it through the session ``TimeZone``, so the cutoff
    shifts by the session offset. It runs under ``America/New_York`` (a
    negative offset), where a two-hour-old row is missed unless the cutoff
    is bound naive.
    """
    deployment = "conf_stale_wf"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        try:
            with conn.cursor() as cur:
                if session_timezone is not None:
                    cur.execute(f"SET TIME ZONE '{session_timezone}'")
                cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
                cur.execute("CREATE SCHEMA dbos")
                cur.execute(
                    f"""
                    CREATE TABLE dbos.workflow_status (
                      workflow_uuid text PRIMARY KEY,
                      queue_name text,
                      status text NOT NULL,
                      name text,
                      created_at {timestamp_ddl},
                      updated_at {timestamp_ddl}
                    )
                    """
                )
                cur.execute(
                    f"""
                    INSERT INTO dbos.workflow_status (
                      workflow_uuid, queue_name, status, name,
                      created_at, updated_at
                    )
                    VALUES (
                      'wf-stale-1', %s, 'PENDING', 'okg_publisher_cycle',
                      {stale_value_sql}, {stale_value_sql}
                    )
                    """,
                    (f"okg-{deployment}",),
                )
            findings = audit_admission_conformance(
                conn,
                deployment_name=deployment,
                stale_after_seconds=60,
            )
        finally:
            with conn.cursor() as cur:
                cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")

    finding = _finding(findings, "stale_pending_workflows")
    assert finding.status == "FAIL"
    assert finding.code == "admission_stale_pending_workflow"
    assert finding.count == 1
    assert finding.samples[0]["workflow_id"] == "wf-stale-1"


def test_admission_conformance_anchors_on_updated_at_not_started_at(
    clean_runtime: str,
) -> None:
    """A cold updated_at is stale even when started_at_epoch_ms is fresh.

    The production DBOS shape also carries ``started_at_epoch_ms``, and
    ``_dbos_stale_anchor`` (which ``dbos_backlog_gc`` uses) folds it into
    ``GREATEST(...)`` so the destructive reaper never cancels a workflow
    that any column says is alive. This detector only reports, so it must
    anchor on ``updated_at`` alone: a workflow whose status stopped
    advancing two hours ago is stuck even if it started recently.
    """
    deployment = "conf_stale_wf_anchor"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
                cur.execute("CREATE SCHEMA dbos")
                cur.execute(
                    """
                    CREATE TABLE dbos.workflow_status (
                      workflow_uuid text PRIMARY KEY,
                      queue_name text,
                      status text NOT NULL,
                      name text,
                      created_at bigint,
                      updated_at bigint,
                      started_at_epoch_ms bigint
                    )
                    """
                )
                cur.execute(
                    """
                    INSERT INTO dbos.workflow_status (
                      workflow_uuid, queue_name, status, name,
                      created_at, updated_at, started_at_epoch_ms
                    )
                    VALUES (
                      'wf-anchor-1', %s, 'PENDING', 'okg_publisher_cycle',
                      (extract(epoch from now()) * 1000)::bigint - 7200000,
                      (extract(epoch from now()) * 1000)::bigint - 7200000,
                      (extract(epoch from now()) * 1000)::bigint
                    )
                    """,
                    (f"okg-{deployment}",),
                )
            findings = audit_admission_conformance(
                conn,
                deployment_name=deployment,
                stale_after_seconds=60,
            )
        finally:
            with conn.cursor() as cur:
                cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")

    finding = _finding(findings, "stale_pending_workflows")
    assert finding.status == "FAIL"
    assert finding.code == "admission_stale_pending_workflow"
    assert finding.count == 1
    assert finding.samples[0]["workflow_id"] == "wf-anchor-1"


def test_admission_conformance_falls_back_to_created_at_anchor(
    clean_runtime: str,
) -> None:
    """Without updated_at, the shared helper's anchor is used instead."""
    deployment = "conf_stale_wf_created"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
                cur.execute("CREATE SCHEMA dbos")
                cur.execute(
                    """
                    CREATE TABLE dbos.workflow_status (
                      workflow_uuid text PRIMARY KEY,
                      queue_name text,
                      status text NOT NULL,
                      name text,
                      created_at timestamptz
                    )
                    """
                )
                cur.execute(
                    """
                    INSERT INTO dbos.workflow_status (
                      workflow_uuid, queue_name, status, name, created_at
                    )
                    VALUES (
                      'wf-created-1', %s, 'ENQUEUED', 'okg_source_sync',
                      now() - interval '2 hours'
                    )
                    """,
                    (f"okg-{deployment}",),
                )
            findings = audit_admission_conformance(
                conn,
                deployment_name=deployment,
                stale_after_seconds=60,
            )
        finally:
            with conn.cursor() as cur:
                cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")

    finding = _finding(findings, "stale_pending_workflows")
    assert finding.status == "FAIL"
    assert finding.count == 1
    assert finding.samples[0]["workflow_id"] == "wf-created-1"


def test_admission_conformance_skips_workflows_without_timestamp_columns(
    clean_runtime: str,
) -> None:
    """A dbos.workflow_status with no timestamp anchor is skipped, not fatal."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
                cur.execute("CREATE SCHEMA dbos")
                cur.execute(
                    """
                    CREATE TABLE dbos.workflow_status (
                      workflow_uuid text PRIMARY KEY,
                      queue_name text,
                      status text NOT NULL,
                      name text
                    )
                    """
                )
            findings = audit_admission_conformance(
                conn,
                deployment_name="conf_stale_wf_no_ts",
            )
        finally:
            with conn.cursor() as cur:
                cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")

    finding = _finding(findings, "stale_pending_workflows")
    assert finding.status == "OK"
    assert finding.code == "admission_dbos_workflow_status_no_timestamp"


def _finding(findings, name: str):
    for finding in findings:
        if finding.name == name:
            return finding
    raise AssertionError(f"missing finding {name}")

