"""Periodic re-evaluation of conditional schedule registration gates.

Per ``harden-runtime-supervisor`` §B. The runtime worker registers
the three conditional schedules (``okg_embed_batch``,
``okg_lens_putback_sweep``, ``okg_escalation_timeout_sweep``) only
when their gate predicate returns True at boot. If the gate flips
from False → True later in the worker's life (operator registers
the first lens, a workflow opens the first escalation, manifest
flips ``embeddings.enabled``), the corresponding schedule was never
registered and the substrate silently stops doing that work.

This workflow re-runs the same gates that ``worker.py`` ran at boot
and registers + applies any schedule whose gate has flipped. It
runs on a cron — default every 30 minutes, manifest-overridable.

Design notes:

  * **Idempotence.** A gate that was True at boot stays True after
    re-evaluation; we don't re-decorate (DBOS rejects duplicate
    workflow names). The check is `<feature>_workflow._DECORATED.get(name)`
    — if the entry already exists, skip; otherwise call the
    feature's ``register_*_workflow`` + ``register_*_schedule`` +
    ``apply_*_schedule``.
  * **No un-registration.** A gate that flipped True → False is
    NOT unregistered. DBOS lacks a clean late-unregister path,
    and the cost of an unused schedule fire is much smaller than
    the cost of missing a real cleanup.
  * **Replay determinism.** ``_capture_now`` is a ``@DBOS.step`` so
    recovery sees the same wall-clock value.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any


__all__ = [
    "GATES_REEVALUATE_WORKFLOW_NAME",
    "register_gates_reevaluate_workflow",
    "gates_reevaluate_workflow",
    "reset_decorated_cache_for_tests",
]


log = logging.getLogger(__name__)


GATES_REEVALUATE_WORKFLOW_NAME = "okg_runtime_gates_reevaluate"


_DECORATED: dict[str, Any] = {}


def reset_decorated_cache_for_tests() -> None:
    """Test-only helper: force a fresh _DECORATED cache."""
    _DECORATED.clear()


def register_gates_reevaluate_workflow(*, deployment: str) -> None:
    """Decorate the gate-re-evaluation workflow.

    Called from the worker entry point AFTER ``DBOS()`` is
    constructed but BEFORE ``DBOS.launch()``. Always registers,
    regardless of feature gates — the re-eval workflow's purpose
    is to *be* the late-registration mechanism.
    """
    from dbos import DBOS

    _DECORATED.clear()

    @DBOS.step()
    def _capture_now() -> str:
        return datetime.now(timezone.utc).isoformat()

    @DBOS.workflow(
        name=GATES_REEVALUATE_WORKFLOW_NAME,
        max_recovery_attempts=10,
    )
    def _gates_reevaluate_workflow() -> dict:
        _ = _capture_now()  # replay determinism
        return _run_reevaluation(deployment=deployment)

    _DECORATED[GATES_REEVALUATE_WORKFLOW_NAME] = (
        _gates_reevaluate_workflow
    )
    log.info(
        "registered gates-reevaluate workflow: deployment=%s name=%s",
        deployment, GATES_REEVALUATE_WORKFLOW_NAME,
    )


def gates_reevaluate_workflow() -> Any:
    if GATES_REEVALUATE_WORKFLOW_NAME not in _DECORATED:
        raise RuntimeError(
            "gates-reevaluate workflow not registered; call "
            "register_gates_reevaluate_workflow() before DBOS.launch()"
        )
    return _DECORATED[GATES_REEVALUATE_WORKFLOW_NAME]


def _run_reevaluation(*, deployment: str) -> dict:
    """Re-run the gates and register schedules that have flipped on.

    Returns a diff dict naming which features were considered, which
    were registered now, and which were skipped (already registered
    at boot or still gated False).
    """
    from okg.substrate.deployments import (
        require_postgres_dsn, resolve_deployment,
    )
    from okg.substrate.runtime.services import (
        should_register_embed,
        should_register_escalation_sweep,
        should_register_lens_putback,
    )

    manifest = resolve_deployment(deployment)
    dsn = require_postgres_dsn(manifest, deployment=deployment)
    schedule_overrides = dict(manifest.runtime.schedules)

    gates = {
        "embed": should_register_embed(manifest),
        "lens_putback": should_register_lens_putback(dsn=dsn),
        "escalation_sweep": should_register_escalation_sweep(dsn=dsn),
    }

    registered: list[str] = []
    skipped_already_registered: list[str] = []
    skipped_gate_false: list[str] = []

    for feature, gate_open in gates.items():
        if not gate_open:
            skipped_gate_false.append(feature)
            continue
        if _is_already_registered(feature):
            skipped_already_registered.append(feature)
            continue
        try:
            _register_feature(
                feature,
                deployment=deployment,
                dsn=dsn,
                schedule_overrides=schedule_overrides,
            )
            registered.append(feature)
            log.info(
                "gates re-evaluation: registering %s "
                "(gate flipped from skip to register)",
                feature,
            )
        except Exception:  # noqa: BLE001
            log.exception(
                "gates re-evaluation: failed to register %s; "
                "leaving unregistered for next tick",
                feature,
            )

    diff = {
        "deployment": deployment,
        "gates": gates,
        "registered_now": registered,
        "skipped_already_registered": skipped_already_registered,
        "skipped_gate_false": skipped_gate_false,
    }
    log.info("gates re-evaluation complete: %s", diff)
    return diff


def _is_already_registered(feature: str) -> bool:
    """True iff the feature's workflow module has its decorated
    handle cached. Reads the same `_DECORATED` dict the feature's
    boot-time `register_*_workflow` populates.
    """
    if feature == "embed":
        from okg.substrate.runtime import embed_workflow as m
        return m.EMBED_WORKFLOW_NAME in m._DECORATED
    if feature == "lens_putback":
        from okg.substrate.runtime import lens_putback_workflow as m
        return m.LENS_PUTBACK_WORKFLOW_NAME in m._DECORATED
    if feature == "escalation_sweep":
        from okg.substrate.runtime import escalation_sweep_workflow as m
        return m.ESCALATION_SWEEP_WORKFLOW_NAME in m._DECORATED
    raise ValueError(f"unknown feature: {feature!r}")


def _register_feature(
    feature: str,
    *,
    deployment: str,
    dsn: str,
    schedule_overrides: dict[str, str],
) -> None:
    """Late-register and apply a feature's workflow + schedule.

    Honors the manifest's ``schedule_overrides`` so an operator's
    cron override applies to the late-registered schedule too.
    """
    if feature == "embed":
        from okg.substrate.deployments import resolve_deployment
        from okg.substrate.embeddings import build_embedding_execution_plan
        from okg.substrate.runtime.embed_workflow import (
            apply_embed_schedule,
            register_embed_schedule,
            register_embed_workflow,
        )
        plan = build_embedding_execution_plan(
            resolve_deployment(deployment).embedding_config
        )
        if not plan.enabled:
            raise RuntimeError(
                "cannot late-register embedding from a disabled plan"
            )
        register_embed_workflow(
            deployment=deployment,
            embedding_plan=plan,
        )
        register_embed_schedule(deployment=deployment, dsn=dsn)
        apply_embed_schedule(schedule_overrides=schedule_overrides)
        return
    if feature == "lens_putback":
        from okg.substrate.runtime.lens_putback_schedule import (
            apply_lens_putback_schedule,
            register_lens_putback_schedule,
        )
        from okg.substrate.runtime.lens_putback_workflow import (
            register_lens_putback_workflow,
        )
        register_lens_putback_workflow(deployment=deployment)
        register_lens_putback_schedule(
            deployment=deployment, dsn=dsn,
        )
        apply_lens_putback_schedule(
            schedule_overrides=schedule_overrides,
        )
        return
    if feature == "escalation_sweep":
        from okg.substrate.runtime.escalation_sweep_schedule import (
            apply_escalation_sweep_schedule,
            register_escalation_sweep_schedule,
        )
        from okg.substrate.runtime.escalation_sweep_workflow import (
            register_escalation_sweep_workflow,
        )
        register_escalation_sweep_workflow(deployment=deployment)
        register_escalation_sweep_schedule(
            deployment=deployment, dsn=dsn,
        )
        apply_escalation_sweep_schedule(
            schedule_overrides=schedule_overrides,
        )
        return
    raise ValueError(f"unknown feature: {feature!r}")
