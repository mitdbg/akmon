"""Declared-attribute index maintenance as a DBOS workflow (okg#3112 step 2).

One run of ``okg_declared_attr_index_maintenance`` reconciles the
declared-attribute expression indexes (drops, then builds, design section
4.3). It runs on the deployment's index-maintenance queue
(``okg-<deployment>-index-maintenance``, concurrency 1), never on the
control queue, so a ``CREATE INDEX CONCURRENTLY`` that waits ten minutes for
an older transaction never holds a publish. The five-minute schedule
(:mod:`declared_attr_index_schedule`) and the publisher cycle enqueue it; the
publisher and catalog apply only ask, never wait.

Nothing is built before an operator's first ``okg db attr-index reconcile
--apply`` (the reconciler's opt-in), and ``OKG_DECLARED_ATTR_INDEXES=off``
turns every run into a no-op.
"""
from __future__ import annotations

import logging
from typing import Any

from okg.substrate.errors import SubstrateError


__all__ = [
    "DECLARED_ATTR_INDEX_WORKFLOW_NAME",
    "declared_attr_index_workflow",
    "enqueue_declared_attr_index_maintenance",
    "maintenance_workflow_body",
    "register_declared_attr_index_workflow",
    "run_index_maintenance",
]


log = logging.getLogger(__name__)


DECLARED_ATTR_INDEX_WORKFLOW_NAME = "okg_declared_attr_index_maintenance"

_DECORATED: dict[str, Any] = {}


def register_declared_attr_index_workflow(*, deployment: str) -> None:
    """Decorate the maintenance workflow. Call after ``DBOS()`` and before
    ``DBOS.launch()``; clears the cache so a fresh boot gets a fresh closure."""
    from dbos import DBOS

    _DECORATED.clear()

    @DBOS.workflow(
        name=DECLARED_ATTR_INDEX_WORKFLOW_NAME,
        max_recovery_attempts=3,
    )
    def declared_attr_index_workflow() -> dict:
        """One reconcile run. A recovered run starts over:
        the reconciler reads the catalog and the manifest afresh, marks a
        build its predecessor left ``building`` as failed and drops it."""
        return maintenance_workflow_body(deployment=deployment)

    _DECORATED[DECLARED_ATTR_INDEX_WORKFLOW_NAME] = declared_attr_index_workflow
    log.info(
        "registered declared-attribute index workflow: deployment=%s name=%s",
        deployment, DECLARED_ATTR_INDEX_WORKFLOW_NAME,
    )


def declared_attr_index_workflow() -> Any:
    """Public accessor for the decorated workflow function."""
    if DECLARED_ATTR_INDEX_WORKFLOW_NAME not in _DECORATED:
        raise RuntimeError(
            "declared-attribute index workflow not registered; call "
            "register_declared_attr_index_workflow() before DBOS.launch()"
        )
    return _DECORATED[DECLARED_ATTR_INDEX_WORKFLOW_NAME]


def maintenance_workflow_body(*, deployment: str, dsn: str | None = None) -> dict:
    """The workflow's body: one maintenance run, raising ``SubstrateError``
    (with the run's report) when the reconcile raised, so DBOS records the
    workflow as failed rather than as a success."""
    out = run_index_maintenance(deployment=deployment, dsn=dsn)
    if out.get("status") == "error":
        raise SubstrateError(
            "declared-attribute index maintenance failed",
            data={"deployment": deployment, "report": out},
            fix_hint="okg doctor --check declared-attr-indexes; okg db attr-index reconcile",
        )
    return out


def run_index_maintenance(*, deployment: str, dsn: str | None = None) -> dict:
    """Reconcile. A failure is logged and returned, never swallowed into
    success: ``status`` is ``error`` when the reconcile raised, else ``ok``.
    (PACT lane-s-storage-drop-copies retired the drift check: the
    effective-attribute view it compared the graph with is gone.)"""
    from okg.substrate import declared_attr_indexes as dai

    if dsn is None:
        from okg.substrate.deployments import require_postgres_dsn, resolve_deployment

        manifest = resolve_deployment(deployment)
        dsn = require_postgres_dsn(manifest, deployment=deployment)
    if dai.maintenance_disabled():
        return {"status": "disabled", "reason": f"{dai.MAINTENANCE_ENV} is off"}
    out: dict[str, Any] = {"deployment": deployment}
    try:
        out["reconcile"] = dai.reconcile(
            dsn, deployment=deployment, apply=True, require_enabled=True,
            on_progress=lambda msg: log.info("declared-attribute indexes: %s", msg),
        )
    except Exception as exc:  # noqa: BLE001 -- reported in the workflow output
        log.exception("declared-attribute index reconcile failed: deployment=%s", deployment)
        out["reconcile"] = {"status": "error", "error": f"{type(exc).__name__}: {exc}"}
    reconcile_status = (out.get("reconcile") or {}).get("status")
    out["status"] = "error" if reconcile_status == "error" else "ok"
    log.info(
        "declared-attribute index maintenance: deployment=%s reconcile=%s",
        deployment, reconcile_status,
    )
    return out


def enqueue_declared_attr_index_maintenance(
    *, deployment: str, dsn: str, reason: str
) -> dict:
    """Enqueue one maintenance run on the index-maintenance queue unless one
    is already enqueued or running there. Returns what it did."""
    from okg.substrate.runtime.dbos_enqueue import (
        active_workflow_id_for_name,
        enqueue_deployment_workflow,
    )
    from okg.substrate.runtime.partition_queue import index_maintenance_queue_name

    queue_name = index_maintenance_queue_name(deployment)
    active = active_workflow_id_for_name(
        dsn=dsn,
        queue_name=queue_name,
        workflow_name=DECLARED_ATTR_INDEX_WORKFLOW_NAME,
    )
    if active is not None:
        return {"status": "already_queued", "workflow_id": active, "reason": reason}
    result = enqueue_deployment_workflow(
        deployment=deployment,
        dsn=dsn,
        workflow_name=DECLARED_ATTR_INDEX_WORKFLOW_NAME,
        queue_name=queue_name,
    )
    return {"status": "enqueued", "workflow_id": result.workflow_id, "reason": reason}
