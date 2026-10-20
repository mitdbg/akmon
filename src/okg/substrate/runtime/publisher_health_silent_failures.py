"""Silent-failure backstop probes for `okg doctor`.

Extracted from ``runtime/publisher_health.py`` by
``extract-publisher-health-silent-failures`` (2026-05) per the
runtime subagent audit. Queue and manifest probes remain here; the
workflow-outcome health signal is implemented by
``workflow_operational_outcomes`` and consumed directly by ``okg doctor``.

Each probe catches a specific class of bug the substrate runtime
used to hide:

  - :func:`check_internal_queue_unconsumed`: scheduled crons
    piling up on ``_dbos_internal_queue`` because of the
    ``application_version`` mismatch bug (see
    ``reap_stale_internal_enqueued`` in services.py).
  - :func:`check_manifest_dsn_resolvable`: manifest's
    ``postgres.dsn`` still contains an unsubstituted ``${VAR}``
    or the resolved DSN doesn't accept a connection. The
    literal-``${OKG_DSN}`` failure mode observed 2026-05-11.

``publisher_health`` re-exports the queue and manifest verdicts/checks
at module-import time so doctor.py's existing
``from okg.substrate.runtime.publisher_health import ...``
keeps resolving.
"""
from __future__ import annotations

from dataclasses import dataclass

import psycopg
from okg.substrate.db.connection import connect as db_connect


__all__ = [
    "InternalQueueUnconsumedVerdict",
    "ManifestDsnResolvableVerdict",
    "check_internal_queue_unconsumed",
    "check_manifest_dsn_resolvable",
]


@dataclass(frozen=True)
class InternalQueueUnconsumedVerdict:
    """`_dbos_internal_queue` has ENQUEUED rows older than the threshold."""

    deployment: str
    ok: bool
    code: str
    message: str
    stuck_count: int
    threshold_seconds: int
    sample: list[dict]

    def to_dict(self) -> dict:
        return {
            "ok": self.ok,
            "code": self.code,
            "message": self.message,
            "deployment": self.deployment,
            "stuck_count": self.stuck_count,
            "threshold_seconds": self.threshold_seconds,
            "sample": self.sample,
        }


def check_internal_queue_unconsumed(
    *,
    deployment: str,
    dsn: str,
    threshold_seconds: int = 90,
) -> InternalQueueUnconsumedVerdict:
    """Flag scheduled-cron workflows piling up on `_dbos_internal_queue`.

    Background: when a worker boots with a different `application_version`
    than the most-recent row in `dbos.application_versions`, DBOS's
    scheduler tags new enqueues with the stale version and the dispatcher
    rejects them forever. Without the boot-time fix (`bump_live_version_
    to_latest` + `reap_stale_internal_enqueued`), or under other
    pathologies that prevent the internal queue from draining, ENQUEUED
    rows pile up indefinitely.

    The probe queries `dbos.workflow_status` for ENQUEUED rows on the
    internal queue older than ``threshold_seconds``. Non-zero count is
    a P0 finding.
    """
    cutoff_ms_sql = (
        f"(extract(epoch from now()) * 1000 - {threshold_seconds} * 1000)"
    )
    with db_connect(dsn) as conn, \
            conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT count(*)
              FROM dbos.workflow_status
             WHERE queue_name = '_dbos_internal_queue'
               AND status = 'ENQUEUED'
               AND created_at < {cutoff_ms_sql}
            """,
        )
        row = cur.fetchone()
        stuck_count = int(row[0]) if row else 0
        if stuck_count == 0:
            return InternalQueueUnconsumedVerdict(
                deployment=deployment,
                ok=True,
                code="internal_queue_consumed",
                message="no stuck ENQUEUED rows on _dbos_internal_queue",
                stuck_count=0,
                threshold_seconds=threshold_seconds,
                sample=[],
            )
        cur.execute(
            f"""
            SELECT workflow_uuid, name, application_version,
                   extract(epoch from now()) * 1000 - created_at
                       AS age_ms
              FROM dbos.workflow_status
             WHERE queue_name = '_dbos_internal_queue'
               AND status = 'ENQUEUED'
               AND created_at < {cutoff_ms_sql}
             ORDER BY created_at
             LIMIT 5;
            """,
        )
        sample = [
            {
                "workflow_uuid": r[0],
                "name": r[1],
                "application_version": r[2],
                "age_seconds": int((r[3] or 0) / 1000),
            }
            for r in cur.fetchall()
        ]
    return InternalQueueUnconsumedVerdict(
        deployment=deployment,
        ok=False,
        code="internal_queue_unconsumed",
        message=(
            f"{stuck_count} scheduled-cron workflow(s) stuck ENQUEUED "
            f"on `_dbos_internal_queue` for >{threshold_seconds}s. "
            "Likely cause: application_version mismatch (a prior "
            "worker's version is the latest in dbos.application_"
            "versions). The substrate worker's boot-time "
            "`bump_live_version_to_latest` + `reap_stale_internal_"
            "enqueued` helpers should fix this on next restart; if "
            "they don't, dig deeper."
        ),
        stuck_count=stuck_count,
        threshold_seconds=threshold_seconds,
        sample=sample,
    )


@dataclass(frozen=True)
class WorkflowSilentSuccessVerdict:
    """Deprecated compatibility verdict backed by typed outcome rows."""

    deployment: str
    ok: bool
    code: str
    message: str
    count: int
    window_hours: int
    sample: list[dict]

    def to_dict(self) -> dict:
        return {
            "ok": self.ok,
            "code": self.code,
            "message": self.message,
            "deployment": self.deployment,
            "count": self.count,
            "window_hours": self.window_hours,
            "sample": self.sample,
        }


def check_workflow_silent_success(
    *,
    deployment: str,
    dsn: str,
    window_hours: int = 1,
) -> WorkflowSilentSuccessVerdict:
    """Classify recent successful participant outcomes using typed rows.

    This compatibility wrapper delegates to the typed operational-outcome
    reader. It deliberately never decodes or searches DBOS output.
    """
    from okg.substrate.runtime.workflow_operational_outcomes import (
        check_workflow_operational_outcomes,
    )
    typed = check_workflow_operational_outcomes(
        deployment=deployment,
        dsn=dsn,
        lookback_seconds=max(1, int(window_hours * 3600)),
        sample_limit=5,
    )
    count = (
        typed.partial_count
        + typed.retained_count
        + typed.unknown_count
        + typed.global_unknown_count
    )
    code = (
        "workflow_silent_success_clean" if typed.ok
        else "workflow_silent_success"
        if typed.code == "workflow_outcome_degraded"
        else "workflow_silent_success_unknown"
    )
    return WorkflowSilentSuccessVerdict(
        deployment=deployment,
        ok=typed.ok,
        code=code,
        message=typed.message,
        count=count,
        window_hours=window_hours,
        sample=[dict(item) for item in typed.samples],
    )


@dataclass(frozen=True)
class ManifestDsnResolvableVerdict:
    """Manifest's postgres.dsn resolves and accepts a connection."""

    deployment: str
    ok: bool
    code: str
    message: str
    reason: str | None

    def to_dict(self) -> dict:
        return {
            "ok": self.ok,
            "code": self.code,
            "message": self.message,
            "deployment": self.deployment,
            "reason": self.reason,
        }


def check_manifest_dsn_resolvable(
    *,
    deployment: str,
) -> ManifestDsnResolvableVerdict:
    """Confirm the deployment manifest's ``postgres.dsn`` field resolves
    against the current environment AND that the resulting DSN accepts
    a connection.

    Catches the literal-``${OKG_DSN}`` failure mode (observed 2026-05-11)
    where the manifest references an env var that isn't set in the
    operator's shell; without this probe, the symptom was workflows
    returning SUCCESS while every source connection failed inside.

    The probe imports manifest resolution lazily to avoid a circular
    import with `okg.substrate.deployments`.
    """
    from okg.substrate.deployments import (
        DeploymentResolutionError,
        resolve_deployment,
    )
    try:
        manifest = resolve_deployment(deployment)
    except DeploymentResolutionError as err:
        return ManifestDsnResolvableVerdict(
            deployment=deployment,
            ok=False,
            code="manifest_dsn_unresolvable",
            message=(
                f"deployment manifest itself does not resolve: {err}"
            ),
            reason="manifest_unresolvable",
        )
    dsn = manifest.postgres_dsn
    if dsn is None:
        return ManifestDsnResolvableVerdict(
            deployment=deployment,
            ok=False,
            code="manifest_dsn_unresolvable",
            message=(
                "manifest's postgres.dsn is unresolvable (likely an "
                "unsubstituted ${VAR} placeholder — check OKG_DSN and "
                "other env vars referenced in deployment.yaml). "
                "`_resolve_postgres_dsn` refuses to return literal "
                "placeholders since 2026-05-11."
            ),
            reason="unsubstituted_var",
        )
    try:
        with db_connect(dsn, connect_timeout=5) as _conn:
            pass
    except psycopg.OperationalError as err:
        msg_lower = str(err).lower()
        if "timeout" in msg_lower or "timed out" in msg_lower:
            reason = "timeout"
        elif "authentication" in msg_lower or "password" in msg_lower:
            reason = "auth"
        elif "could not connect" in msg_lower or "refused" in msg_lower:
            reason = "connection_refused"
        else:
            reason = "operational_error"
        return ManifestDsnResolvableVerdict(
            deployment=deployment,
            ok=False,
            code="manifest_dsn_unresolvable",
            message=(
                f"manifest DSN resolved but connection failed: "
                f"{type(err).__name__}: {err}"
            ),
            reason=reason,
        )
    return ManifestDsnResolvableVerdict(
        deployment=deployment,
        ok=True,
        code="manifest_dsn_resolvable",
        message="manifest DSN resolves and accepts a connection",
        reason=None,
    )
