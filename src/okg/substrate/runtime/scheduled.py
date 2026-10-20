"""Substrate-owned scheduled DBOS workflows.

Phase 1 §2.4.2 ships exactly one scheduled workflow: ``doctor_workflow``.
It runs ``okg doctor`` on a fixed cadence and writes the verdict as a
DBOS workflow event so operators can query it via
``okg runtime get-event``. This is the proof-of-life signal for the
coexistence prototype (§2.8 dogfood window).

Subsequent phases add SDK-decorated scheduled workflows from deployment
authors. Those live in deployment code (`deployments/<name>/workflows/`)
and register via the SDK's ``@deployment_scheduled`` decorator. This
module is for substrate-internal scheduled work only.
"""
from __future__ import annotations

import json
import logging
import subprocess
from datetime import datetime
from typing import Any

from okg.substrate.unknown import Unknown, read_ok


__all__ = [
    "DOCTOR_SCHEDULE_ID",
    "DOCTOR_DEFAULT_CRON",
    "DOCTOR_SKIPPED_EVENT",
    "apply_deployment_author_schedules",
    "apply_substrate_schedules",
    "register_substrate_schedules",
]


log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Doctor schedule constants
# ---------------------------------------------------------------------------

DOCTOR_SCHEDULE_ID = "okg_substrate_doctor"

# Run every 5 minutes by default. The cadence is overridable in the
# deployment manifest's ``runtime.schedules`` block per Decision 14.
DOCTOR_DEFAULT_CRON = "*/5 * * * *"

#: Event key a skipped tick publishes. Deliberately not `doctor_verdict` —
#: see the skip branch in `doctor_workflow`.
DOCTOR_SKIPPED_EVENT = "doctor_skipped"


# ---------------------------------------------------------------------------
# Doctor workflow
# ---------------------------------------------------------------------------


# Module-level handle so apply_substrate_schedules can find the
# decorated workflow function the registration call constructed.
_DECORATED: dict[str, Any] = {}


def register_substrate_schedules(
    *,
    deployment: str,
    dsn: str,
) -> None:
    """Decorate substrate-owned scheduled workflows with ``@DBOS.workflow``.

    Called from the worker entry point AFTER ``DBOS()`` is constructed
    but BEFORE ``DBOS.launch()`` (the documented init lifecycle:
    construct → decorate → launch). This step only decorates; it does
    not persist a schedule row. The persistence happens in
    :func:`apply_substrate_schedules` which must run after launch
    because ``DBOS.apply_schedules`` requires a live system database.

    Clears the module-level ``_DECORATED`` cache on entry so a second
    worker boot in the same Python process (e.g. tests with the
    ``dbos_test_runtime`` fixture) does not pick up a stale function
    object bound to a destroyed DBOS singleton. Per the Phase 1 §2.10
    review (SIGNIFICANT recommendation D).
    """
    from dbos import DBOS

    _DECORATED.clear()

    def _doctor_workflow_unfenced(
        scheduled_time: datetime | None = None,
        context: Any = None,
    ) -> None:
        """Run ``okg doctor`` + the deployment's registered checks
        and publish the aggregated verdict as a workflow event.

        The DBOS scheduled-workflow contract is that the workflow
        function receives ``(scheduled_time: datetime, context:
        Any)``. Both are discarded here — operators read the verdict
        via the workflow event published below.

        Per ``wire-deployment-doctor-into-substrate-schedule``
        (deepen-dbos-integration Phase 2): the scheduled run also
        invokes deployment-registered checks (e.g.
        ``doctor_okg_workspace``) so the cron-driven doctor sees the
        same drift an operator-typed ``okg doctor --check X`` would.
        """
        verdict = _run_doctor_subprocess(deployment=deployment)
        verdict["deployment"] = deployment
        deployment_checks = _run_deployment_doctor_checks(
            deployment=deployment,
        )
        verdict["deployment_checks"] = deployment_checks
        # Roll up: any registered deployment check that failed or
        # reported no verdict flips the overall verdict to unhealthy,
        # regardless of base-probe health. Base probes already populate
        # verdict["ok"] from the subprocess exit code; this never
        # re-flips it to True.
        checks_failed = _roll_up_deployment_checks(verdict, deployment_checks)

        # `consolidate-deployment-runtime`: overlay the runtime-services
        # health probe. The subprocess-based doctor (above) lives in a
        # fork that does not share the worker's in-process services
        # registry, so its `runtime_services_health` entry says
        # `not_observed`. Re-run the probe HERE — inside the worker
        # process — so the verdict carries real per-thread liveness.
        try:
            from okg.substrate.runtime.publisher_health import (
                check_runtime_services_health,
            )
            rs = check_runtime_services_health(
                deployment=deployment,
            )
            verdict["runtime_services_health"] = rs.to_dict()
            if not rs.ok:
                verdict["ok"] = False
                verdict.setdefault("errors", []).append(
                    {"code": rs.code, "message": rs.message}
                )
        except Exception as exc:  # noqa: BLE001
            log.exception(
                "runtime_services_health probe raised in "
                "doctor_workflow; continuing with verdict.ok unchanged",
            )
            verdict["runtime_services_health"] = {
                "ok": False,
                "code": "runtime_services_health_probe_failed",
                "message": str(exc),
            }
        # Issue #945: carry the worker's boot-time code identity out of
        # the worker process. This runs INSIDE the worker, so it is the
        # only place that can observe which modules this process loaded
        # and when. Read back out-of-process by
        # `read_worker_code_identity_overlay`. A raised probe never
        # changes verdict["ok"]: unknown code identity is reported as
        # unknown, never as agreement.
        try:
            from okg.substrate.runtime.code_identity import (
                worker_code_identity_report,
            )

            verdict["runtime_code_identity"] = worker_code_identity_report()
        except Exception as exc:  # noqa: BLE001
            log.exception(
                "worker_code_identity_report raised in doctor_workflow; "
                "reporting it as unknown",
            )
            verdict["runtime_code_identity"] = {
                "status": "probe_failed",
                "reason": f"{type(exc).__name__}: {exc}",
            }
        DBOS.set_event("doctor_verdict", verdict)
        log.info(
            "doctor_workflow: deployment=%s ok=%s "
            "deployment_checks=%d failed_or_unknown=%d",
            deployment, verdict.get("ok"),
            len(deployment_checks),
            checks_failed,
        )

    @DBOS.workflow(name="doctor_workflow")
    def doctor_workflow(
        scheduled_time: datetime | None = None,
        context: Any = None,
    ) -> None:
        from okg.substrate.runtime.promotion_fence import (
            assert_deployment_admitted,
            promotion_id_from_recovery_context,
        )
        from okg.substrate.runtime.schedule_overlap import (
            OverlapSkipped,
            claim_schedule_run,
        )

        # Normal scheduled runs carry no recovery context and remain blocked by
        # an active fence. The owner-validated one-shot recovery surface adds
        # the explicit context needed to produce a newer doctor witness without
        # restoring recurring schedules.
        promotion_id = promotion_id_from_recovery_context(context)
        # The doctor's own subprocesses and probes run for minutes on a large
        # graph, far past the 5-minute cron period. Stacking a second run on
        # the first buys no fresher verdict and holds another set of
        # connections the publish cycle needs.
        #
        # The claim is entered OUTSIDE the try, so only a refusal to start is
        # reported as a skip. A body that itself raises OverlapSkipped — from
        # some nested schedule — must not be mislabeled as "never started".
        claim = claim_schedule_run(DOCTOR_SCHEDULE_ID)
        try:
            claim.__enter__()
        except OverlapSkipped as skip:
            # Reported, never silent: the event is what an operator reads to
            # tell "the doctor is slow" from "the doctor is not running".
            log.warning("doctor_workflow skipped: %s", skip)
            # A SEPARATE event key, not `doctor_verdict`: the health
            # overlays read the newest doctor_verdict naming this
            # deployment and derive check counts from it, so publishing a
            # skip under that key would hand them an empty verdict and let
            # "the doctor did not run" read as "the doctor found nothing".
            DBOS.set_event(DOCTOR_SKIPPED_EVENT, {
                "deployment": deployment,
                "schedule_id": DOCTOR_SCHEDULE_ID,
                "status": "skipped_previous_run_active",
                "reason": str(skip),
                "skipped_runs": skip.skipped_count,
            })
            return
        try:
            # Check admission, then LET GO of the transaction before running
            # the body (issue #1310). `promotion_enqueue_permit` is a
            # `pg_advisory_xact_lock`, so wrapping the body held an open
            # Postgres transaction for as long as the doctor ran -- and the
            # doctor's own comment above says that is minutes on a large
            # graph. Live signature during a clean `okg provision`: a session
            # `idle in transaction` for 266 seconds on the fence SELECT, in
            # Client/ClientRead, with backends queued behind its advisory
            # lock; on 2026-08-23 the same shape held for ~57 minutes with 41
            # connections blocked. The doctor enqueues nothing -- it runs
            # `okg doctor` in a subprocess and publishes a verdict event -- so
            # admission is a decision to make once at the start, not a lock to
            # hold throughout.
            #
            # Disclosed trade, not a free win. Under the permit,
            # `acquire_promotion_fence` took the same advisory lock and so
            # could not fence the deployment mid-doctor; now it can, and a
            # doctor run still PENDING at the replay boundary makes the
            # cutover refuse loudly (RuntimeCutoverBlocked) instead of
            # serializing. A bounded refusal beats an unbounded jam. Note also
            # that `_run_deployment_doctor_checks` shells out to
            # DEPLOYMENT-AUTHORED checks: that they only read is an obligation
            # on each deployment's own check code, not something the substrate
            # can verify here.
            assert_deployment_admitted(
                deployment=deployment,
                dsn=dsn,
                promotion_id=promotion_id,
            )
            _doctor_workflow_unfenced(scheduled_time, context)
        finally:
            claim.__exit__(None, None, None)

    _DECORATED[DOCTOR_SCHEDULE_ID] = doctor_workflow
    log.info(
        "decorated substrate workflow: %s", DOCTOR_SCHEDULE_ID,
    )


def apply_substrate_schedules(
    *,
    schedule_overrides: dict[str, str],
) -> None:
    """Persist substrate-owned schedule rows via ``DBOS.apply_schedules``.

    Called AFTER ``DBOS.launch()`` because apply_schedules requires the
    system database to be live. Upserts atomically — safe to call on
    every worker boot (any prior schedule with the same name is
    replaced).

    Per Decision 14 the manifest can override the cron via
    ``runtime.schedules.<schedule_id>: <cron>``. The default lives in
    :data:`DOCTOR_DEFAULT_CRON`.
    """
    from dbos import DBOS

    if DOCTOR_SCHEDULE_ID not in _DECORATED:
        raise RuntimeError(
            "apply_substrate_schedules called before "
            "register_substrate_schedules; the worker must call "
            "register before launch and apply after launch."
        )

    cron = schedule_overrides.get(DOCTOR_SCHEDULE_ID, DOCTOR_DEFAULT_CRON)

    DBOS.apply_schedules([
        {
            "schedule_name": DOCTOR_SCHEDULE_ID,
            "workflow_fn": _DECORATED[DOCTOR_SCHEDULE_ID],
            "schedule": cron,
            "context": None,
            "automatic_backfill": False,
            "cron_timezone": None,
            "queue_name": None,
        },
    ])
    log.info(
        "applied substrate schedule: %s cron=%r",
        DOCTOR_SCHEDULE_ID, cron,
    )


def apply_deployment_author_schedules(
    *,
    schedule_overrides: dict[str, str],
) -> list[str]:
    """Persist durable schedule rows for deployment-author scheduled workflows.

    Walks the SDK's deployment-author schedule registry (populated when the
    worker imports the manifest's ``runtime.workflows`` modules during boot)
    and upserts one ``dbos.workflow_schedules`` row per ``@deployment_scheduled``
    workflow via :meth:`DBOS.apply_schedules` — the same durable mechanism
    :func:`apply_substrate_schedules` uses for substrate-owned schedules.

    Called AFTER ``DBOS.launch()`` because ``apply_schedules`` requires a live
    system database. The manifest's ``runtime.schedules.<schedule_id>`` overrides
    the cron per Decision 14. Idempotent (apply_schedules upserts by
    ``schedule_name``), so safe on every boot. Returns the applied schedule_ids;
    ``[]`` when no deployment-author schedules are registered (the common case —
    only fleet-monitoring deployments use ``@deployment_scheduled``).

    Without this walk a ``@deployment_scheduled`` workflow gets no schedule row
    and never runs; the prior implementation armed DBOS's native scheduler, which
    errored every tick instead (okg-ci ``okg_ci_resource_monitor``, 2026-06-16).
    """
    from dbos import DBOS

    from okg.deployment._internal.decorators import iter_deployment_scheduled

    entries = iter_deployment_scheduled()
    if not entries:
        return []

    specs: list[dict[str, Any]] = []
    applied: list[str] = []
    for entry in entries:
        schedule_id = entry["schedule_id"]
        cron = schedule_overrides.get(schedule_id, entry["default_cron"])
        specs.append({
            "schedule_name": schedule_id,
            "workflow_fn": entry["workflow_fn"],
            "schedule": cron,
            "context": None,
            "automatic_backfill": bool(entry.get("automatic_backfill", False)),
            "cron_timezone": None,
            "queue_name": None,
        })
        applied.append(schedule_id)
        log.info(
            "applied deployment-author schedule: %s cron=%r",
            schedule_id, cron,
        )
    DBOS.apply_schedules(specs)
    return applied


def _run_doctor_subprocess(*, deployment: str) -> dict[str, Any]:
    """Run ``okg doctor --deployment X --json`` in a subprocess and
    parse the verdict.

    Subprocess rather than in-process function call because ``okg
    doctor`` performs its own DB connection management and signal
    handling, and we want the scheduled workflow to be insulated from
    any process-state surprises in the doctor's transitive imports.

    The child is THIS okg (issue #2301): :func:`running_okg_argv` names
    the interpreter running the worker with ``-P``, so neither a PATH
    entry nor the worker's working directory can select other code.
    """
    from okg.substrate.cli.child_argv import running_okg_argv

    cmd = [
        *running_okg_argv(),
        "doctor", "--deployment", deployment, "--json",
    ]
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return {
            "ok": False,
            "error": "doctor subprocess timed out",
            "deployment": deployment,
        }

    payload: dict[str, Any] = {
        "deployment": deployment,
        "exit_code": proc.returncode,
        "ok": proc.returncode == 0,
    }
    if proc.stdout:
        try:
            payload["doctor"] = json.loads(proc.stdout)
        except json.JSONDecodeError:
            payload["stdout"] = proc.stdout[:2000]
    if proc.stderr and proc.returncode != 0:
        payload["stderr"] = proc.stderr[:2000]
    return payload


def _run_deployment_doctor_checks(
    *, deployment: str,
) -> dict[str, dict[str, Any]]:
    """Invoke each deployment-registered ``doctor_check`` as its own
    subprocess and return a dict keyed by check name.

    Per ``wire-deployment-doctor-into-substrate-schedule``
    (deepen-dbos-integration Phase 2): the scheduled doctor workflow
    aggregates these into the verdict event so cron-driven doctor
    sees deployment-specific drift, not just substrate base probes.

    Returns ``{}`` when the deployment has no registered checks (fast
    path; no subprocess invocations). Per-check timeout is 60s,
    matching the base doctor subprocess; an overall timeout-or-error
    is recorded inline (the function never raises).
    """
    try:
        from okg.substrate.doctor import (
            DoctorCheckLoadError,
            load_deployment_doctor_checks,
        )
    except Exception as exc:  # noqa: BLE001
        # Defensive: if the doctor module can't be imported (e.g.
        # the test environment), report a single synthetic error
        # rather than crashing the scheduled workflow.
        return {
            "<load-failed>": {
                "ok": False,
                "error": f"failed to import doctor module: {exc}",
            },
        }

    try:
        checks = load_deployment_doctor_checks(deployment)
    except DoctorCheckLoadError as exc:
        return {
            "<load-failed>": {
                "ok": False,
                "error": str(exc),
            },
        }

    if not checks:
        return {}

    results: dict[str, dict[str, Any]] = {}
    for check in checks:
        results[check.name] = _run_one_deployment_doctor_check(
            deployment=deployment, check_name=check.name,
        )
    return results


def _run_one_deployment_doctor_check(
    *, deployment: str, check_name: str,
) -> dict[str, Any]:
    """Run one ``okg doctor --check <name>`` as a subprocess.

    Mirrors :func:`_run_doctor_subprocess`'s shell-out + timeout
    pattern. Returns a payload dict — never raises.
    """
    from okg.substrate.cli.child_argv import running_okg_argv

    cmd = [
        *running_okg_argv(),
        "doctor",
        "--deployment", deployment,
        "--check", check_name,
        "--json",
    ]
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return {
            "ok": False,
            "error": "doctor check subprocess timed out",
            "check_name": check_name,
        }

    payload: dict[str, Any] = {
        "check_name": check_name,
        "exit_code": proc.returncode,
        "ok": proc.returncode == 0,
    }
    if proc.stdout:
        try:
            payload["doctor"] = json.loads(proc.stdout)
        except json.JSONDecodeError:
            payload["stdout"] = proc.stdout[:2000]
    if proc.stderr and proc.returncode != 0:
        payload["stderr"] = proc.stderr[:2000]
    return payload


def _check_payload_ok(payload: dict[str, Any]) -> bool | Unknown:
    """Read the ``ok`` verdict from a deployment-check payload.

    A payload that is not a mapping, has no ``ok``, or has a non-boolean
    ``ok`` is :class:`Unknown` with the reason, never a pass (#2573,
    decision D1). This used to read a missing verdict as ``True`` so the
    base probe's result stood; that hid a check that reported nothing.
    """
    return read_ok(payload, source="deployment_check")


def _roll_up_deployment_checks(
    verdict: dict[str, Any],
    deployment_checks: dict[str, Any],
) -> int:
    """Fold the deployment checks into the scheduled doctor's verdict.

    A failed check sets ``verdict["ok"] = False``. A check with no verdict
    does too, and is named with its reason under
    ``verdict["deployment_checks_unknown"]`` so the alert says what it is.
    Returns the number of failed or unknown checks.
    """
    failed = 0
    unknown: dict[str, str] = {}
    for name, payload in deployment_checks.items():
        check_ok = _check_payload_ok(payload)
        if isinstance(check_ok, Unknown):
            unknown[str(name)] = check_ok.reason
        elif not check_ok:
            failed += 1
    if unknown:
        verdict["deployment_checks_unknown"] = dict(sorted(unknown.items()))
    if failed or unknown:
        verdict["ok"] = False
    return failed + len(unknown)
