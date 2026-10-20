"""Escalation timeout sweep as a DBOS workflow (deepen-dbos-integration Phase 0).

Closes timed-out HITL escalations. `transition_to_timed_out(...)` was
already implemented but never invoked from anywhere — escalations with
past TTLs sat at `status='pending'` forever. This workflow drains them
on a 1-minute cron.

The workflow body captures `now` once via a `@DBOS.step` at start so
DBOS recovery (kill -9 + resume) sees the same timestamp and produces
the same sweep set, not an expanded one drawn from the wall clock
during downtime.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any


__all__ = [
    "ESCALATION_SWEEP_WORKFLOW_NAME",
    "escalation_sweep_workflow",
    "register_escalation_sweep_workflow",
]


log = logging.getLogger(__name__)


ESCALATION_SWEEP_WORKFLOW_NAME = "okg_escalation_timeout_sweep"


_DECORATED: dict[str, Any] = {}


def register_escalation_sweep_workflow(*, deployment: str) -> None:
    """Decorate the escalation timeout-sweep workflow.

    Called from the worker entry point AFTER ``DBOS()`` is constructed
    but BEFORE ``DBOS.launch()``. Clears the cache on entry so a fresh
    worker boot doesn't pick up a stale closure.
    """
    from dbos import DBOS

    _DECORATED.clear()

    @DBOS.step()
    def _capture_now() -> str:
        """Persist a single ``now`` timestamp at workflow start.

        Step-wrapped so DBOS replay sees the same value; otherwise a
        recovered workflow would compute a different sweep set drawn
        from the wall clock during downtime.

        Returns an ISO-8601 string (UTC) because DBOS step outputs
        must be JSON-serializable.
        """
        return datetime.now(timezone.utc).isoformat()

    @DBOS.workflow(
        name=ESCALATION_SWEEP_WORKFLOW_NAME,
        max_recovery_attempts=10,
    )
    def escalation_sweep_workflow() -> dict:
        """Run one sweep of timed-out escalations.

        Returns the sweep result dict. DBOS persists this as the
        workflow's output; operators read it via
        ``okg runtime debug-workflow <id>``.
        """
        now_iso = _capture_now()
        return _run_one_sweep(deployment=deployment, now_iso=now_iso)

    _DECORATED[ESCALATION_SWEEP_WORKFLOW_NAME] = escalation_sweep_workflow
    log.info(
        "registered escalation timeout-sweep workflow: deployment=%s "
        "name=%s", deployment, ESCALATION_SWEEP_WORKFLOW_NAME,
    )


def escalation_sweep_workflow() -> Any:
    """Public accessor for the decorated workflow function."""
    if ESCALATION_SWEEP_WORKFLOW_NAME not in _DECORATED:
        raise RuntimeError(
            "escalation sweep workflow not registered; call "
            "register_escalation_sweep_workflow() before DBOS.launch()"
        )
    return _DECORATED[ESCALATION_SWEEP_WORKFLOW_NAME]


# ---------------------------------------------------------------------------
# Sweep runner
# ---------------------------------------------------------------------------


def _run_one_sweep(*, deployment: str, now_iso: str) -> dict:
    """Resolve manifest + DSN, open a connection, call the sweep."""
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.deployments import (
        require_postgres_dsn, resolve_deployment,
    )
    from okg.substrate.runtime.escalation import (
        sweep_timed_out_escalations,
    )

    manifest = resolve_deployment(deployment)
    dsn = require_postgres_dsn(manifest, deployment=deployment)
    now = datetime.fromisoformat(now_iso)

    # `restore-publisher-perf-and-profiling` §B
    from okg.substrate.runtime.profiling_bridge import (
        workflow_profiling_context,
    )
    with workflow_profiling_context(
        manifest=manifest,
        deployment=deployment,
        target_kind="escalation_sweep",
        command="okg_escalation_timeout_sweep",
    ), db_connect(dsn) as conn:
        result = sweep_timed_out_escalations(
            conn, deployment=deployment, now=now,
        )

    log.info(
        "escalation timeout sweep complete: deployment=%s "
        "considered=%s transitioned=%s skipped=%s",
        deployment, result["considered"], result["transitioned"],
        result["skipped"],
    )
    return result
