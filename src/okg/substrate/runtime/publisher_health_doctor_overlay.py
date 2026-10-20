"""Scheduled-doctor verdict + runtime-services-overlay probes.

Extracted from ``runtime/publisher_health.py`` by
``extract-publisher-health-doctor-overlay`` (2026-05) per the
cleanup audit. Two `okg doctor` probes that read the most-recent
``okg_substrate_doctor`` workflow's ``doctor_verdict`` event:

  - ``check_deployment_doctor_health(deployment, dsn, ...)`` —
    summarizes the per-check pass/fail counts from the verdict's
    ``deployment_checks`` payload. Reports stale when the
    last successful scheduled-doctor fire is older than the
    configured ``threshold_minutes``.

  - ``check_runtime_services_health(deployment, dsn, ...)`` —
    inspects the in-process services registry
    (``services.get_services_state()``) when called from the
    worker process. When called from outside the worker (registry
    empty), overlays the most-recent
    ``runtime_services_health`` payload from a scheduled doctor
    verdict via ``_try_doctor_verdict_overlay`` (see
    ``harden-runtime-supervisor`` §C).

Both probes tolerate DSNs that haven't been runtime-bootstrapped
(test fixtures, fresh DBs) by returning a "never_ran" verdict
rather than raising.

``runtime/publisher_health.py`` re-exports these names at
module-import time so doctor.py + the test suite continue to
import from `publisher_health` without changes.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from okg.substrate.db.connection import connect as db_connect
from okg.substrate.unknown import Unknown, read_ok


__all__ = [
    "DeploymentDoctorHealthVerdict",
    "RuntimeServicesHealthVerdict",
    "check_deployment_doctor_health",
    "check_runtime_services_health",
    "read_worker_code_identity_overlay",
]


#: How many recent successful doctor_workflow rows to walk looking for a
#: verdict that names this deployment.
#:
#: Raised from 10/5 when the doctor schedule learned to SKIP a tick whose
#: predecessor is still running. A skipped tick succeeds quickly and carries
#: no `doctor_verdict` event, so on a deployment where each doctor run
#: outlasts several cron periods the skips crowd the recency window. Too
#: small a window would report a healthy-but-slow doctor as "no verdict".
_VERDICT_CANDIDATE_LIMIT = 30


@dataclass(frozen=True)
class DeploymentDoctorHealthVerdict:
    """Outcome of the deployment-doctor health probe.

    Per ``wire-deployment-doctor-into-substrate-schedule``
    (deepen-dbos-integration Phase 2). Reads the most-recent
    ``okg_substrate_doctor`` workflow's ``doctor_verdict`` event for
    the deployment and summarizes per-check pass/fail counts.
    """

    deployment: str
    ok: bool
    code: str
    message: str
    checks_total: int
    checks_failed: int
    last_run_at: str | None
    threshold_minutes: int
    # Checks whose payload carried no real boolean `ok` (#2573): counted
    # apart from failed ones, and never as passing.
    checks_unknown: int = 0

    def to_dict(self) -> dict:
        return {
            "ok": self.ok,
            "code": self.code,
            "message": self.message,
            "deployment": self.deployment,
            "checks_total": self.checks_total,
            "checks_failed": self.checks_failed,
            "checks_unknown": self.checks_unknown,
            "last_run_at": self.last_run_at,
            "threshold_minutes": self.threshold_minutes,
        }


def check_deployment_doctor_health(
    *,
    deployment: str,
    dsn: str,
    threshold_minutes: int = 15,
) -> DeploymentDoctorHealthVerdict:
    """Check the freshness + pass/fail of deployment-registered
    doctor checks driven by the substrate's scheduled doctor.

    Reads the most-recent ``okg_substrate_doctor`` workflow's
    ``doctor_verdict`` event (stored in ``dbos.workflow_events``).
    The event's ``deployment_checks`` field carries the per-check
    payloads. If no recent successful scheduled fire exists within
    ``threshold_minutes``, the probe reports stale.

    ``ok`` is True when the most-recent verdict's deployment_checks
    are all passing. Stale verdicts also report ok=False.
    """
    # Find the most-recent SUCCESS doctor workflow for this deployment.
    # The DBOS workflow name does not include the deployment, and the
    # workflow_status.inputs column is a pickled payload rather than a
    # searchable text field. Match deployment at the doctor_verdict
    # event level, where the workflow explicitly writes it.
    candidates: list[tuple[str, int]] = []
    with db_connect(dsn) as conn:
        with conn.cursor() as cur:
            # Tolerate DSNs that haven't been runtime-bootstrapped
            # (test fixtures, fresh DBs). The probe just returns
            # "never_ran" — same signal as a real never-ran deployment.
            cur.execute("SELECT to_regclass('dbos.workflow_status')")
            if cur.fetchone()[0] is None:
                return DeploymentDoctorHealthVerdict(
                    deployment=deployment,
                    ok=False,
                    code="deployment_doctor_never_ran",
                    message=(
                        f"dbos schema not present in this DSN; "
                        f"deployment {deployment!r} not runtime-"
                        f"bootstrapped"
                    ),
                    checks_total=0,
                    checks_failed=0,
                    last_run_at=None,
                    threshold_minutes=threshold_minutes,
                )
            cur.execute(
                """
                SELECT workflow_uuid, updated_at
                  FROM dbos.workflow_status
                 WHERE name LIKE %s
                   AND status = 'SUCCESS'
                 ORDER BY updated_at DESC
                 LIMIT %s
                """,
                ("%doctor_workflow%", _VERDICT_CANDIDATE_LIMIT),
            )
            candidates = cur.fetchall()

    if not candidates:
        return DeploymentDoctorHealthVerdict(
            deployment=deployment,
            ok=False,
            code="deployment_doctor_never_ran",
            message=(
                f"no successful okg_substrate_doctor workflow for "
                f"{deployment!r} has run yet"
            ),
            checks_total=0,
            checks_failed=0,
            last_run_at=None,
            threshold_minutes=threshold_minutes,
        )

    # Read verdict events in recency order until one names this
    # deployment. This mirrors the runtime-services overlay path and
    # handles multiple deployments sharing a DB.
    verdict: dict[str, Any] | None = None
    workflow_uuid: str | None = None
    updated_at_ms: int | None = None
    from okg.substrate.runtime.admin import client_get_event
    for candidate_uuid, candidate_updated_at_ms in candidates:
        try:
            candidate_verdict = client_get_event(
                dsn=dsn,
                workflow_id=candidate_uuid,
                key="doctor_verdict",
                timeout_seconds=0.5,
            )
        except Exception:  # noqa: BLE001
            continue
        if not isinstance(candidate_verdict, dict):
            continue
        if candidate_verdict.get("deployment") != deployment:
            continue
        verdict = candidate_verdict
        workflow_uuid = candidate_uuid
        updated_at_ms = candidate_updated_at_ms
        break

    if verdict is None or workflow_uuid is None or updated_at_ms is None:
        return DeploymentDoctorHealthVerdict(
            deployment=deployment,
            ok=False,
            code="deployment_doctor_no_verdict",
            message=(
                f"recent doctor_workflow SUCCESS rows exist, but none of "
                f"the last {_VERDICT_CANDIDATE_LIMIT} has a doctor_verdict "
                f"event for {deployment!r}. A doctor run that outlasts its "
                f"own cron period skips the ticks that arrive meanwhile, and "
                f"a skipped tick succeeds without a verdict — check the "
                f"doctor_skipped events, and widen the cron if the runs are "
                f"overlapping"
            ),
            checks_total=0,
            checks_failed=0,
            last_run_at=None,
            threshold_minutes=threshold_minutes,
        )

    import datetime as _dt
    last_run_at = _dt.datetime.fromtimestamp(
        updated_at_ms / 1000.0, tz=_dt.timezone.utc,
    ).isoformat()
    age_minutes = (
        _dt.datetime.now(_dt.timezone.utc)
        - _dt.datetime.fromtimestamp(
            updated_at_ms / 1000.0, tz=_dt.timezone.utc,
        )
    ).total_seconds() / 60.0

    if age_minutes > threshold_minutes:
        return DeploymentDoctorHealthVerdict(
            deployment=deployment,
            ok=False,
            code="deployment_doctor_stale",
            message=(
                f"last successful okg_substrate_doctor for "
                f"{deployment!r} was {age_minutes:.1f}m ago "
                f"(threshold {threshold_minutes}m)"
            ),
            checks_total=0,
            checks_failed=0,
            last_run_at=last_run_at,
            threshold_minutes=threshold_minutes,
        )

    deployment_checks = verdict.get("deployment_checks") or {}
    if not isinstance(deployment_checks, dict):
        deployment_checks = {}
    checks_total = len(deployment_checks)
    checks_failed = 0
    unknown_names: list[str] = []
    for name, payload in deployment_checks.items():
        check_ok = read_ok(payload, source="deployment_doctor_check")
        if isinstance(check_ok, Unknown):
            unknown_names.append(str(name))
        elif not check_ok:
            checks_failed += 1
    checks_unknown = len(unknown_names)

    # A check that reported no verdict is unknown, not passing (#2573,
    # decision D1): the probe is not ok and says which check it was.
    ok = checks_failed == 0 and checks_unknown == 0
    if checks_failed:
        code = "deployment_doctor_check_failed"
    elif checks_unknown:
        code = "deployment_doctor_check_unknown"
    else:
        code = "deployment_doctor_healthy"
    message = (
        f"{checks_failed} of {checks_total} deployment doctor "
        f"check(s) failed for {deployment!r}"
    )
    if checks_unknown:
        message += (
            f", {checks_unknown} of {checks_total} reported no result "
            f"({', '.join(sorted(unknown_names))})"
        )
    message += f" (last run {age_minutes:.1f}m ago)"
    return DeploymentDoctorHealthVerdict(
        deployment=deployment,
        ok=ok,
        code=code,
        message=message,
        checks_total=checks_total,
        checks_failed=checks_failed,
        last_run_at=last_run_at,
        threshold_minutes=threshold_minutes,
        checks_unknown=checks_unknown,
    )


@dataclass(frozen=True)
class RuntimeServicesHealthVerdict:
    """Outcome of the runtime-services health probe.

    Per ``consolidate-deployment-runtime``: the worker process
    supervises the embedded Watcher + webhook server as daemon
    threads. This probe inspects the in-process services registry
    (``services.get_services_state()``) and reports per-service
    alive/last-error state.

    Per ``harden-runtime-supervisor`` §C: when the calling process
    is NOT the worker (the in-process registry is empty), the probe
    overlays the most-recent ``doctor_verdict`` event's
    ``runtime_services_health`` payload — so an operator running
    ``okg doctor`` from a shell sees the worker's reported state
    rather than the calling process's empty registry. The overlay
    carries ``source: "scheduled_doctor_event"`` plus a
    ``observed_minutes_ago`` staleness stamp.
    """

    deployment: str
    ok: bool
    code: str
    message: str
    watcher: dict[str, Any]
    webhook: dict[str, Any]
    # `harden-runtime-supervisor` §C overlay fields. Present when
    # this verdict was reconstituted from a scheduled doctor event;
    # absent (None) when the verdict came from the live in-process
    # registry or from a fresh `not_observed` fallback.
    source: str = "in_process_registry"
    last_observed_at: str | None = None
    observed_minutes_ago: float | None = None

    def to_dict(self) -> dict:
        out: dict[str, Any] = {
            "ok": self.ok,
            "code": self.code,
            "message": self.message,
            "deployment": self.deployment,
            "watcher": dict(self.watcher),
            "webhook": dict(self.webhook),
            "source": self.source,
        }
        if self.last_observed_at is not None:
            out["last_observed_at"] = self.last_observed_at
        if self.observed_minutes_ago is not None:
            out["observed_minutes_ago"] = self.observed_minutes_ago
        return out


def check_runtime_services_health(
    *,
    deployment: str,
    dsn: str | None = None,
    scheduled_doctor_threshold_minutes: float = 10.0,
) -> RuntimeServicesHealthVerdict:
    """Probe the runtime services registry.

    When the calling process is the worker (in-process registry
    populated), returns the live in-process state with
    ``source="in_process_registry"``.

    When the calling process is NOT the worker (registry empty)
    AND ``dsn`` is provided, attempts to overlay the most-recent
    ``doctor_verdict`` event's ``runtime_services_health`` payload
    (`harden-runtime-supervisor` §C). The overlay carries
    ``source="scheduled_doctor_event"`` plus a
    ``observed_minutes_ago`` staleness stamp. If the overlay is
    older than ``scheduled_doctor_threshold_minutes``, the verdict
    flips to ``ok=False`` with code
    ``runtime_services_overlay_stale``.

    When the registry is empty AND no overlay is available (no
    DSN, no recent doctor verdict, the dbos schema isn't
    bootstrapped, etc.), falls back to ``not_observed`` —
    informational, not a failure.
    """
    from okg.substrate.runtime.services import get_services_state

    state = get_services_state()
    watcher_view = state.watcher.to_dict()
    webhook_view = state.webhook.to_dict()

    # Probe is being run outside the worker (or before the worker
    # started services). The state.deployment field is unset.
    if state.deployment is None:
        if dsn is not None:
            overlay = _try_doctor_verdict_overlay(
                deployment=deployment,
                dsn=dsn,
                threshold_minutes=scheduled_doctor_threshold_minutes,
            )
            if overlay is not None:
                return overlay
        return RuntimeServicesHealthVerdict(
            deployment=deployment,
            ok=True,
            code="runtime_services_not_observed",
            message=(
                "runtime services registry empty in this process AND "
                "no scheduled-doctor verdict available to overlay; "
                "this probe only sees data when run inside the "
                "worker process (e.g. from the scheduled "
                "okg_substrate_doctor workflow)"
            ),
            watcher=watcher_view,
            webhook=webhook_view,
        )

    if state.deployment != deployment:
        return RuntimeServicesHealthVerdict(
            deployment=deployment,
            ok=True,
            code="runtime_services_other_deployment",
            message=(
                f"runtime services registry was last booted for "
                f"deployment {state.deployment!r}, not "
                f"{deployment!r}"
            ),
            watcher=watcher_view,
            webhook=webhook_view,
        )

    failures: list[str] = []
    if state.watcher.enabled and not state.watcher.alive():
        failures.append(
            "watcher: "
            + (state.watcher.last_error or "thread not alive")
        )
    if state.webhook.enabled and not state.webhook.alive():
        failures.append(
            "webhook: "
            + (state.webhook.last_error or "thread not alive")
        )

    if failures:
        return RuntimeServicesHealthVerdict(
            deployment=deployment,
            ok=False,
            code="runtime_services_down",
            message=(
                f"supervised service(s) not alive for {deployment!r}: "
                + "; ".join(failures)
            ),
            watcher=watcher_view,
            webhook=webhook_view,
        )

    enabled_names = [
        kind for kind, enabled in (
            ("watcher", state.watcher.enabled),
            ("webhook", state.webhook.enabled),
        ) if enabled
    ]
    if not enabled_names:
        return RuntimeServicesHealthVerdict(
            deployment=deployment,
            ok=True,
            code="runtime_services_none_enabled",
            message=(
                f"no source-sync services enabled in manifest for "
                f"{deployment!r}"
            ),
            watcher=watcher_view,
            webhook=webhook_view,
        )
    return RuntimeServicesHealthVerdict(
        deployment=deployment,
        ok=True,
        code="runtime_services_healthy",
        message=(
            f"supervised services alive for {deployment!r}: "
            + ", ".join(enabled_names)
        ),
        watcher=watcher_view,
        webhook=webhook_view,
    )


def _latest_doctor_verdict_field(
    *,
    deployment: str,
    dsn: str,
    field: str,
    limit: int = _VERDICT_CANDIDATE_LIMIT,
) -> tuple[dict | None, int | None]:
    """Return the newest ``doctor_verdict``'s ``field`` for a deployment.

    Walks the most-recent successful ``%doctor_workflow%`` rows in
    descending update order and returns the first one whose
    ``doctor_verdict`` event names this deployment AND carries ``field``
    as a mapping, together with that row's ``updated_at`` in epoch
    milliseconds. Returns ``(None, None)`` when nothing usable exists —
    the caller must render that as "not observed", never as health.

    The deployment match happens at the EVENT level rather than in SQL
    because ``dbos.workflow_status.inputs`` is pickled bytes, not text.
    """
    try:
        with db_connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT to_regclass('dbos.workflow_status')"
                )
                if cur.fetchone()[0] is None:
                    return None, None
                cur.execute(
                    """
                    SELECT workflow_uuid, updated_at
                      FROM dbos.workflow_status
                     WHERE name LIKE %s
                       AND status = 'SUCCESS'
                     ORDER BY updated_at DESC
                     LIMIT %s
                    """,
                    ("%doctor_workflow%", limit),
                )
                candidates = cur.fetchall()
    except Exception:  # noqa: BLE001
        return None, None

    if not candidates:
        return None, None

    from okg.substrate.runtime.admin import client_get_event

    for workflow_uuid, updated_at_ms in candidates:
        try:
            verdict = client_get_event(
                dsn=dsn,
                workflow_id=workflow_uuid,
                key="doctor_verdict",
                timeout_seconds=0.5,
            )
        except Exception:  # noqa: BLE001
            continue
        if not isinstance(verdict, dict):
            continue
        if verdict.get("deployment") != deployment:
            continue
        payload = verdict.get(field)
        if not isinstance(payload, dict):
            continue
        return payload, updated_at_ms

    return None, None


def read_worker_code_identity_overlay(
    *,
    deployment: str,
    dsn: str,
) -> dict[str, Any] | None:
    """Read the worker's boot-time code identity from outside the worker.

    Issue #945. The identity can only be observed inside the worker
    process, so it travels on the scheduled doctor's ``doctor_verdict``
    event. Returns the reported payload with the event's freshness
    stamps attached, or ``None`` when no verdict carries one — an
    absent identity is the caller's ``not_observed``, never a match.
    """
    payload, updated_at_ms = _latest_doctor_verdict_field(
        deployment=deployment,
        dsn=dsn,
        field="runtime_code_identity",
    )
    if payload is None or updated_at_ms is None:
        return None
    last_observed_at = datetime.fromtimestamp(
        updated_at_ms / 1000.0, tz=timezone.utc,
    )
    observed_minutes_ago = (
        datetime.now(timezone.utc) - last_observed_at
    ).total_seconds() / 60.0
    return {
        **payload,
        "source": "scheduled_doctor_event",
        "last_observed_at": last_observed_at.isoformat(),
        "observed_minutes_ago": round(observed_minutes_ago, 2),
    }


def _try_doctor_verdict_overlay(
    *,
    deployment: str,
    dsn: str,
    threshold_minutes: float,
) -> RuntimeServicesHealthVerdict | None:
    """Look up the most-recent ``doctor_verdict`` event and rebuild
    a ``RuntimeServicesHealthVerdict`` from its
    ``runtime_services_health`` payload.

    Walks the most-recent successful ``%doctor_workflow%`` rows in
    descending update order and returns the first one whose
    ``doctor_verdict`` event names this deployment. Stops at the
    first match (or after 5 candidates — a sane upper bound to
    handle the multi-deployment-on-one-DB case without paging
    through all of history). Returns None when no usable verdict
    is available.

    The deployment match is done at the EVENT level rather than
    in SQL because the ``inputs`` column of ``dbos.workflow_status``
    is pickled bytes (the cron's ``scheduled_time`` + ``context``),
    not text — a SQL LIKE filter on it never matches. The verdict
    event itself carries a ``deployment`` field we can match on.
    """
    services_payload, chosen_updated_at_ms = _latest_doctor_verdict_field(
        deployment=deployment,
        dsn=dsn,
        field="runtime_services_health",
    )
    if services_payload is None or chosen_updated_at_ms is None:
        return None

    updated_at_ms = chosen_updated_at_ms

    last_observed_at = datetime.fromtimestamp(
        updated_at_ms / 1000.0, tz=timezone.utc,
    )
    observed_minutes_ago = (
        datetime.now(timezone.utc) - last_observed_at
    ).total_seconds() / 60.0

    watcher_view = (
        services_payload.get("watcher")
        if isinstance(services_payload.get("watcher"), dict)
        else {}
    )
    webhook_view = (
        services_payload.get("webhook")
        if isinstance(services_payload.get("webhook"), dict)
        else {}
    )

    if observed_minutes_ago > threshold_minutes:
        return RuntimeServicesHealthVerdict(
            deployment=deployment,
            ok=False,
            code="runtime_services_overlay_stale",
            message=(
                f"most-recent scheduled-doctor verdict for "
                f"{deployment!r} is {observed_minutes_ago:.1f}m old "
                f"(threshold {threshold_minutes:.0f}m); the worker "
                f"may not be running"
            ),
            watcher=watcher_view,
            webhook=webhook_view,
            source="scheduled_doctor_event",
            last_observed_at=last_observed_at.isoformat(),
            observed_minutes_ago=round(observed_minutes_ago, 2),
        )

    services_ok = read_ok(services_payload, source="runtime_services_overlay")
    if isinstance(services_ok, Unknown):
        # A recorded services result with no verdict is unknown, not
        # healthy (#2573, decision D1).
        return RuntimeServicesHealthVerdict(
            deployment=deployment,
            ok=False,
            code="runtime_services_overlay_unknown",
            message=(
                f"the scheduled doctor's runtime-services result for "
                f"{deployment!r} carries no ok verdict "
                f"({services_ok.reason}); worker service health is unknown"
            ),
            watcher=watcher_view,
            webhook=webhook_view,
            source="scheduled_doctor_event",
            last_observed_at=last_observed_at.isoformat(),
            observed_minutes_ago=round(observed_minutes_ago, 2),
        )

    # Fresh overlay: trust the payload's ok / code / message and
    # carry them through, layered with the observability stamps.
    return RuntimeServicesHealthVerdict(
        deployment=deployment,
        ok=services_ok,
        code=str(
            services_payload.get("code", "runtime_services_healthy")
        ),
        message=str(
            services_payload.get(
                "message",
                f"overlay from scheduled doctor verdict "
                f"({observed_minutes_ago:.1f}m ago)",
            ),
        ),
        watcher=watcher_view,
        webhook=webhook_view,
        source="scheduled_doctor_event",
        last_observed_at=last_observed_at.isoformat(),
        observed_minutes_ago=round(observed_minutes_ago, 2),
    )

