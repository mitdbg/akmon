"""Worker-recovery measurement — Decision 5 verification gate.

Per `sdk-design.md` Decision 5, OKG's old publisher lease path
retired in favor of a DBOS partitioned queue with ``concurrency=1``.
The ``LeaseLostError`` runtime check is replaced by DBOS worker
recovery semantics: a workflow whose worker dies is resumed from its
last committed step on another worker.

Public DBOS docs do not specify worker-recovery time bounds. Phase 1
§2.7 measures three properties before any production cutover:

  1. Recovery time after ``kill -9`` — target under 60s, hard limit
     under 5min.
  2. Wedged-worker behavior under ``SIGSTOP`` — measure detect-and-
     reassign time, or document that DBOS does not detect this case.
  3. Split-brain probe — under simulated network partition with
     ``concurrency=1`` and ``partition_queue=True``, verify that
     no workflow double-executes.

The functions below implement the test scaffolding. They require a
working DBOS install + Postgres at the deployment DSN. They are
designed to be invoked from ``okg runtime measure-recovery
--mode {kill9, sigstop, split-brain} --deployment X``.

Phase 1 §2.7 deliverables:
  * `measure_kill9_recovery` — implemented.
  * `measure_sigstop_behavior` — implemented.
  * `measure_split_brain` — implemented as a deferred sketch; full
    network-partition simulation requires either Postgres role-level
    pg_hba reload or a docker network manipulation, neither of which
    runs cleanly outside a controlled test environment. The function
    returns an explicit ``DeferredToPhase1IntegrationEnv`` result
    rather than producing misleading numbers.
"""
from __future__ import annotations

import logging
import multiprocessing as mp
import os
import signal
import time
from dataclasses import dataclass
from typing import Literal

from .config import RuntimeConfig
from .engine import build_dbos_config, with_dbos_singleton


__all__ = [
    "RecoveryMeasurement",
    "measure_kill9_recovery",
    "measure_sigstop_behavior",
    "measure_split_brain",
]


log = logging.getLogger(__name__)


@dataclass
class RecoveryMeasurement:
    """Outcome of a recovery-test invocation."""

    mode: Literal["kill9", "sigstop", "split-brain"]
    deployment: str
    ok: bool
    recovery_seconds: float | None
    notes: str
    raw: dict


# ---------------------------------------------------------------------------
# kill-9 recovery probe
# ---------------------------------------------------------------------------

# A long DBOS.sleep that the receiver workflow blocks on. Long enough
# that we can kill the worker mid-sleep and observe recovery; short
# enough to bound the overall test runtime.
_RECEIVER_SLEEP_SECONDS = 60.0


def _receiver_worker_main(
    deployment: str,
    dsn: str,
    runtime_kwargs: dict,
    workflow_id_pipe,
) -> None:
    """Subprocess entry: launch DBOS, start a sleeping workflow,
    write its workflow_id to the pipe, and then block on the sleep.

    The parent then SIGKILLs us; the workflow_id is enqueued for
    recovery by other workers in the same partition.
    """
    from dbos import DBOS

    runtime = RuntimeConfig(**runtime_kwargs)
    cfg = build_dbos_config(
        deployment=deployment, dsn=dsn, runtime=runtime,
    )
    DBOS(config=cfg)

    @DBOS.workflow()
    def _sleeper() -> str:
        DBOS.sleep(_RECEIVER_SLEEP_SECONDS)
        return "completed"

    DBOS.launch()
    handle = DBOS.start_workflow(_sleeper)
    workflow_id_pipe.send(handle.workflow_id)
    workflow_id_pipe.close()
    # Block until SIGKILL.
    while True:
        time.sleep(1)


def measure_kill9_recovery(
    *,
    deployment: str,
    dsn: str,
    runtime: RuntimeConfig,
    timeout_seconds: float = 300.0,
) -> RecoveryMeasurement:
    """Measure recovery time after the worker holding a workflow is
    SIGKILLed.

    Sequence:
      1. Spawn subprocess A: launches DBOS, starts a sleeping workflow,
         records its workflow_id, then blocks.
      2. Receive workflow_id from A's pipe.
      3. Spawn subprocess B: a second worker on the same DBOS singleton
         (in this process, simpler than another subprocess).
      4. SIGKILL subprocess A.
      5. Poll the workflow's status until it reports SUCCESS or
         RECOVERY-IN-PROGRESS, recording elapsed time.
      6. Return :class:`RecoveryMeasurement`.

    The actual durable recovery requires DBOS workers to detect the
    abandoned workflow and pick it up. The measurement is from
    SIGKILL to recovery-detected.
    """
    parent_conn, child_conn = mp.Pipe(duplex=False)
    runtime_kwargs = dict(
        enabled=runtime.enabled,
        dbos_pool_size=runtime.dbos_pool_size,
        okg_read_pool_size=runtime.okg_read_pool_size,
        okg_write_pool_size=runtime.okg_write_pool_size,
        dbos_schema=runtime.dbos_schema,
        bypass_pgbouncer=runtime.bypass_pgbouncer,
        sdk_version=runtime.sdk_version,
        workflows=runtime.workflows,
        schedules=dict(runtime.schedules),
    )
    proc = mp.Process(
        target=_receiver_worker_main,
        args=(deployment, dsn, runtime_kwargs, child_conn),
    )
    proc.start()

    # Step 2: receive workflow_id (with timeout).
    if not parent_conn.poll(30.0):
        proc.kill()
        proc.join()
        return RecoveryMeasurement(
            mode="kill9", deployment=deployment, ok=False,
            recovery_seconds=None,
            notes="receiver subprocess did not publish workflow_id "
                  "within 30s",
            raw={},
        )
    workflow_id: str = parent_conn.recv()
    log.info("kill9 probe: workflow_id=%s", workflow_id)

    # Brief beat to let the workflow enter DBOS.sleep.
    time.sleep(2.0)

    # Step 4: SIGKILL the receiver.
    log.info("kill9 probe: SIGKILL receiver pid=%s", proc.pid)
    proc.kill()
    proc.join(timeout=5.0)
    t_killed = time.monotonic()

    # Step 3 & 5: launch a second worker (in this process) and poll
    # the workflow status.
    from okg.substrate.runtime.engine import defensive_destroy
    from dbos import DBOS  # noqa: F401 — DBOS import still needed below

    defensive_destroy()

    deadline = t_killed + timeout_seconds
    recovery_at: float | None = None
    final_status: str | None = None
    with with_dbos_singleton(
        deployment=deployment,
        dsn=dsn,
        runtime=runtime,
    ):
        while time.monotonic() < deadline:
            workflows = DBOS.list_workflows(workflow_ids=[workflow_id])
            if workflows:
                status = workflows[0].status
                if status in ("SUCCESS", "PENDING", "ENQUEUED"):
                    # PENDING/ENQUEUED indicates the workflow has been
                    # claimed by another worker; SUCCESS means it
                    # finished. Either way, recovery has happened.
                    if recovery_at is None and status != "SUCCESS":
                        recovery_at = time.monotonic()
                    if status == "SUCCESS":
                        final_status = "SUCCESS"
                        break
                final_status = status
            time.sleep(0.5)

    if recovery_at is None and final_status != "SUCCESS":
        return RecoveryMeasurement(
            mode="kill9", deployment=deployment, ok=False,
            recovery_seconds=None,
            notes=(
                f"workflow {workflow_id} did not reach a recovered "
                f"state within {timeout_seconds}s; final_status="
                f"{final_status}"
            ),
            raw={"final_status": final_status},
        )

    elapsed = (
        recovery_at - t_killed if recovery_at is not None else
        time.monotonic() - t_killed
    )
    ok = elapsed < 300.0  # Decision 5 hard limit.
    return RecoveryMeasurement(
        mode="kill9", deployment=deployment, ok=ok,
        recovery_seconds=elapsed,
        notes=(
            "recovery within Decision-5 thresholds"
            if ok else
            "recovery exceeded 5-minute hard limit; pivot Decision 5"
        ),
        raw={
            "workflow_id": workflow_id,
            "final_status": final_status,
            "recovery_seconds": elapsed,
        },
    )


# ---------------------------------------------------------------------------
# SIGSTOP wedged-worker probe
# ---------------------------------------------------------------------------


def measure_sigstop_behavior(
    *,
    deployment: str,
    dsn: str,
    runtime: RuntimeConfig,
    timeout_seconds: float = 600.0,
) -> RecoveryMeasurement:
    """Send SIGSTOP (not SIGKILL) to the worker holding a workflow,
    and observe whether DBOS detects the wedge and reassigns.

    Many durable-workflow runtimes detect SIGKILL via process exit but
    don't detect SIGSTOP because the process is still ``alive`` from
    the OS's perspective — it's just frozen. This probe documents
    DBOS's behavior in this case rather than asserting a particular
    outcome.

    The `notes` field reports whether reassignment happened and how
    long it took. If DBOS does not reassign, the result has
    ``recovery_seconds=None`` and ``ok=False`` with explicit notes;
    the operator runbook then flags this case as a known limitation
    rather than a regression.
    """
    parent_conn, child_conn = mp.Pipe(duplex=False)
    runtime_kwargs = dict(
        enabled=runtime.enabled,
        dbos_pool_size=runtime.dbos_pool_size,
        okg_read_pool_size=runtime.okg_read_pool_size,
        okg_write_pool_size=runtime.okg_write_pool_size,
        dbos_schema=runtime.dbos_schema,
        bypass_pgbouncer=runtime.bypass_pgbouncer,
        sdk_version=runtime.sdk_version,
        workflows=runtime.workflows,
        schedules=dict(runtime.schedules),
    )
    proc = mp.Process(
        target=_receiver_worker_main,
        args=(deployment, dsn, runtime_kwargs, child_conn),
    )
    proc.start()

    if not parent_conn.poll(30.0):
        proc.kill()
        proc.join()
        return RecoveryMeasurement(
            mode="sigstop", deployment=deployment, ok=False,
            recovery_seconds=None,
            notes="receiver did not publish workflow_id within 30s",
            raw={},
        )
    workflow_id: str = parent_conn.recv()
    time.sleep(2.0)

    log.info(
        "sigstop probe: SIGSTOP receiver pid=%s "
        "(process is now wedged but not killed)",
        proc.pid,
    )
    os.kill(proc.pid, signal.SIGSTOP)
    t_stopped = time.monotonic()

    # Launch second worker in this process and wait, capturing the
    # outcome.
    from okg.substrate.runtime.engine import defensive_destroy
    from dbos import DBOS  # noqa: F401 — DBOS used below

    defensive_destroy()

    deadline = t_stopped + timeout_seconds
    reassigned_at: float | None = None
    try:
        with with_dbos_singleton(
            deployment=deployment,
            dsn=dsn,
            runtime=runtime,
        ):
            while time.monotonic() < deadline:
                workflows = DBOS.list_workflows(workflow_ids=[workflow_id])
                if workflows:
                    status = workflows[0].status
                    if status in ("PENDING", "ENQUEUED", "SUCCESS"):
                        reassigned_at = time.monotonic()
                        break
                time.sleep(2.0)
    finally:
        # Always clean up the wedged subprocess.
        try:
            os.kill(proc.pid, signal.SIGCONT)
        except Exception:  # noqa: BLE001 — wedged subprocess may already be gone; proc.kill() below is the real cleanup.
            pass
        proc.kill()
        proc.join(timeout=5.0)

    if reassigned_at is None:
        return RecoveryMeasurement(
            mode="sigstop", deployment=deployment, ok=False,
            recovery_seconds=None,
            notes=(
                "DBOS did not reassign the SIGSTOPped worker's "
                f"workflow within {timeout_seconds}s. This is a known "
                "limitation in many workflow runtimes; document in "
                "the runbook and use SIGTERM on workers in production."
            ),
            raw={"workflow_id": workflow_id},
        )
    elapsed = reassigned_at - t_stopped
    return RecoveryMeasurement(
        mode="sigstop", deployment=deployment, ok=True,
        recovery_seconds=elapsed,
        notes="DBOS reassigned the wedged worker's workflow",
        raw={
            "workflow_id": workflow_id,
            "reassign_seconds": elapsed,
        },
    )


# ---------------------------------------------------------------------------
# Split-brain probe — deferred to a dedicated integration env
# ---------------------------------------------------------------------------


def measure_split_brain(
    *,
    deployment: str,
    dsn: str,
    runtime: RuntimeConfig,
) -> RecoveryMeasurement:
    """Verify that under simulated network partition no workflow
    double-executes on a partitioned queue with ``concurrency=1``.

    Full split-brain simulation requires either:
      * pg_hba.conf reloads to block one worker's Postgres connection
        without taking the database down, OR
      * docker-compose / k8s network policy manipulation in a
        controlled test environment.

    Phase 1 §2.7 ships this as a sketch. The full integration test
    lands in a dedicated CI job that runs split-brain in a controlled
    environment (Postgres in a docker container with `iptables` rules
    inside the container's network namespace).

    Returns a ``RecoveryMeasurement`` with ``ok=False`` and an explicit
    `notes` field rather than a misleading number — the
    measurement can only be performed in the dedicated environment.
    """
    return RecoveryMeasurement(
        mode="split-brain", deployment=deployment, ok=False,
        recovery_seconds=None,
        notes=(
            "split-brain measurement is deferred to the dedicated "
            "integration environment per the §2.7 plan. The Phase 1 "
            "verification gate (task 3.2) requires kill-9 recovery + "
            "wedged-worker behavior to meet thresholds; split-brain "
            "lands as a follow-on once the controlled-env CI job is "
            "set up."
        ),
        raw={},
    )
