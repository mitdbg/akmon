"""Lightweight tests for the escalation-sweep DBOS workflow + schedule.

Covers exports / identity / verdict shape without spinning a DBOS
singleton. The sweep semantics are tested by
`test_escalation_sweep.py`; the live workflow firing is exercised by
the cutover-time verification step (proposal task 15), not here.
"""
from __future__ import annotations

import psycopg
import pytest

from okg.substrate.runtime.escalation_sweep_workflow import (
    ESCALATION_SWEEP_WORKFLOW_NAME,
)
from okg.substrate.runtime.escalation_sweep_schedule import (
    ESCALATION_SWEEP_DEFAULT_CRON,
    ESCALATION_SWEEP_SCHEDULE_ID,
)
from okg.substrate.runtime.publisher_health import (
    EscalationOpenBacklogVerdict,
    check_escalation_open_backlog,
)


def test_workflow_name_constant_is_stable():
    assert ESCALATION_SWEEP_WORKFLOW_NAME == "okg_escalation_timeout_sweep"


def test_schedule_constants_pinned():
    assert ESCALATION_SWEEP_SCHEDULE_ID == "okg_escalation_timeout_sweep"
    # 5-field cron, every minute (vs the 5-min cadence of lens-putback).
    assert ESCALATION_SWEEP_DEFAULT_CRON == "*/1 * * * *"


def test_open_backlog_verdict_shape():
    """Verdict.to_dict serializes the expected shape (no DB)."""
    v = EscalationOpenBacklogVerdict(
        deployment="test",
        ok=True,
        code="escalation_open_backlog_high",
        message="75 open escalations",
        count=75,
        warn_threshold=50,
    )
    d = v.to_dict()
    assert d == {
        "deployment": "test",
        "ok": True,
        "code": "escalation_open_backlog_high",
        "message": "75 open escalations",
        "count": 75,
        "warn_threshold": 50,
    }


# ---------------------------------------------------------------------------
# Doctor probe on the empty path (DB-backed)
# ---------------------------------------------------------------------------


pytestmark = pytest.mark.postgres


def _ensure_escalation_schema(dsn: str) -> None:
    """Skip if `okg.escalation_live` is missing. The escalation schema
    is applied during runtime bootstrap (`okg runtime bootstrap`), not
    during the `clean_runtime` substrate-migration fixture, so the
    test DB may not have it. Lifecycle-style tests use a per-test
    fixture DB that explicitly creates the table."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('okg.escalation_live')")
            if cur.fetchone()[0] is None:
                pytest.skip(
                    "okg.escalation_live missing in test DSN — bootstrap "
                    "the runtime first or run against a DSN that has it"
                )


def test_open_backlog_zero_when_no_escalations(clean_runtime: str) -> None:
    """An empty deployment reports zero open escalations."""
    _ensure_escalation_schema(clean_runtime)
    verdict = check_escalation_open_backlog(
        deployment="never-existed",
        dsn=clean_runtime,
        warn_threshold=50,
    )
    assert verdict.ok is True
    assert verdict.code == "escalation_open_backlog_healthy"
    assert verdict.count == 0
