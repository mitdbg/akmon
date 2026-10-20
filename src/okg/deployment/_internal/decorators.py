"""SDK decorators (Phase 2 §2.2).

The five decorator categories the SDK exposes:

  * :func:`deployment_workflow` — wraps ``@DBOS.workflow()``. Stamps
    SDK + deployment-module version into the workflow's input row so
    a worker resuming an in-flight workflow can refuse to continue
    under a mismatched version.
  * :func:`deployment_step` — wraps ``@DBOS.step``. Default
    ``retries_allowed=False`` per Phase 0 C-correction (retries are
    opt-in, not opt-out).
  * :func:`deployment_transaction` — wraps ``@DBOS.transaction``.
    Substrate-aware mutation helpers (Phase 2 §3) read the active
    DBOS sql_session inside the wrapped function.
  * :func:`deployment_scheduled` — wraps ``@DBOS.scheduled``.
    Manifest's ``runtime.schedules.<schedule_id>`` overrides the
    cron at boot.
  * :func:`on_generation_published`, :func:`on_generation_blocked`,
    :func:`on_escalation_decided` — register the decorated function
    in the substrate's hook registry at module-import time, so the
    publisher / escalation lifecycle enqueues them on the
    deployment's partitioned queue.

The decorators expose a stable user-facing signature even when the
DBOS package is not installed (e.g. when an operator imports the SDK
just to look at decorator help text). The actual DBOS-binding work
happens lazily on first call.
"""
from __future__ import annotations

import functools
import hashlib
import inspect
import logging
import os
from contextlib import AbstractContextManager
from typing import Any, Callable, TypeVar

from .._version import __version__ as SDK_VERSION


__all__ = [
    "deployment_scheduled",
    "deployment_step",
    "deployment_transaction",
    "deployment_workflow",
    "module_source_hash",
    "on_escalation_decided",
    "on_generation_blocked",
    "on_generation_published",
]


log = logging.getLogger(__name__)


F = TypeVar("F", bound=Callable[..., Any])


# ---------------------------------------------------------------------------
# Module-version pinning helpers
# ---------------------------------------------------------------------------


def module_source_hash(func: Callable[..., Any]) -> str:
    """Deterministic hash of the function's defining module source.

    Used by :func:`deployment_workflow` to stamp ``deployment_module_version``
    into the workflow's input row. A worker resuming an in-flight
    workflow recomputes the hash for the loaded module and refuses
    to resume if it differs from the stamped value.

    Robust to:

      * Whitespace-only changes — the hash is over the raw source
        bytes, so even comment-only edits move it. That is the
        intended strictness; cross-version resume is meant to be
        rare and operator-flagged.
      * Compiled .pyc files — uses ``inspect.getsourcefile`` to find
        the .py source on disk.

    Returns
    -------
    str
        First 16 hex chars of SHA-256 over the source bytes, or
        ``"unknown"`` when source cannot be retrieved (e.g. dynamically
        defined functions in tests).
    """
    try:
        path = inspect.getsourcefile(func)
        if path is None or not os.path.isfile(path):
            return "unknown"
        with open(path, "rb") as fh:
            return hashlib.sha256(fh.read()).hexdigest()[:16]
    except (OSError, TypeError):  # pragma: no cover — defensive
        return "unknown"


# ---------------------------------------------------------------------------
# @deployment_workflow
# ---------------------------------------------------------------------------


def deployment_workflow(
    name: str | None = None,
    *,
    max_recovery_attempts: int = 50,
    validate_args: bool = True,
) -> Callable[[F], F]:
    """Decorate a deployment workflow.

    Wraps ``@DBOS.workflow`` and stamps two version values into the
    workflow's input row at start:

      * ``okg_sdk_version`` — :data:`okg.deployment.__version__`.
      * ``deployment_module_version`` — :func:`module_source_hash` of
        the decorated function's defining module.

    On worker resume of an in-flight workflow, the SDK refuses to
    continue if either value differs from the current process's
    installed values. The mismatched workflow is marked
    ``cancelled_due_to_version_mismatch`` (Phase 2 §6 / spec
    requirement "Workflow-version pinning").

    Parameters
    ----------
    name : str | None
        Optional workflow name; defaults to ``func.__qualname__``.
    max_recovery_attempts : int, default 50
        Forwarded to DBOS.
    validate_args : bool, default True
        Reserved for Phase 2 §2.2.1's args-shape validation; currently
        a no-op while args-typing lands in §3.
    """

    def _decorator(func: F) -> F:
        from dbos import DBOS  # lazy import — DBOS may be optional

        workflow_name = name or func.__qualname__
        module_version = module_source_hash(func)

        @functools.wraps(func)
        def _wrapped(*args, **kwargs):
            # Phase 2 §2.2.1 stamping: write version metadata to the
            # workflow's set_event so an operator can inspect it via
            # `okg runtime debug-workflow`. The full
            # cancelled_due_to_version_mismatch logic lands once the
            # SDK's worker entry point reads the stamped values on
            # resume — see Phase 2 §6 spec deltas.
            try:
                DBOS.set_event("okg_sdk_version", SDK_VERSION)
                DBOS.set_event("deployment_module_version", module_version)
            except Exception:  # noqa: BLE001 — set_event may fail
                # outside a workflow context (mock_dbos tests); the
                # main behavior is still safe.
                pass
            return func(*args, **kwargs)

        # Apply the DBOS workflow decorator with our wrapper.
        decorated = DBOS.workflow(
            name=workflow_name,
            max_recovery_attempts=max_recovery_attempts,
        )(_wrapped)

        # Surface metadata so the lint + operator tooling can read it.
        decorated._okg_sdk_version = SDK_VERSION  # type: ignore[attr-defined]
        decorated._okg_module_version = module_version  # type: ignore[attr-defined]
        decorated._okg_workflow_name = workflow_name  # type: ignore[attr-defined]
        return decorated  # type: ignore[return-value]

    return _decorator


# ---------------------------------------------------------------------------
# @deployment_step
# ---------------------------------------------------------------------------


def deployment_step(
    name: str | None = None,
    *,
    retries_allowed: bool = False,
    max_attempts: int = 3,
    interval_seconds: float = 1.0,
    backoff_rate: float = 2.0,
) -> Callable[[F], F]:
    """Decorate a deployment step.

    Wraps ``@DBOS.step``. **Retries are off by default** per Phase 0
    C-correction (the SDK design called for retries-default-on; the
    review flagged that as silently masking author intent). Callers
    opt into retries explicitly with ``retries_allowed=True``.
    """

    def _decorator(func: F) -> F:
        from dbos import DBOS

        return DBOS.step(  # type: ignore[return-value]
            name=name,
            retries_allowed=retries_allowed,
            max_attempts=max_attempts,
            interval_seconds=interval_seconds,
            backoff_rate=backoff_rate,
        )(func)

    return _decorator


# ---------------------------------------------------------------------------
# @deployment_transaction
# ---------------------------------------------------------------------------


def deployment_transaction(
    name: str | None = None,
    *,
    isolation_level: str = "SERIALIZABLE",
) -> Callable[[F], F]:
    """Decorate a deployment transaction.

    Wraps ``@DBOS.transaction`` so the wrapped function runs against
    DBOS's ``sql_session`` (per Decision 9), and substrate-aware
    mutation helpers (Phase 2 §3) read the active session via DBOS's
    contextvar.

    The default ``isolation_level='SERIALIZABLE'`` matches the
    publisher's own pattern — readers see a consistent generation.
    """

    def _decorator(func: F) -> F:
        from dbos import DBOS

        return DBOS.transaction(  # type: ignore[return-value]
            name=name,
            isolation_level=isolation_level,
        )(func)

    return _decorator


# ---------------------------------------------------------------------------
# @deployment_scheduled
# ---------------------------------------------------------------------------


# Process-local registry of deployment-author scheduled workflows. Populated by
# @deployment_scheduled at module-import time (the worker imports the manifest's
# ``runtime.workflows`` modules during boot) and consumed AFTER ``DBOS.launch()``
# by ``okg.substrate.runtime.scheduled.apply_deployment_author_schedules``, which
# persists one durable ``dbos.workflow_schedules`` row per entry — the same model
# the substrate's own schedules use. A worker boots once per process, so a plain
# dict keyed by schedule_id suffices; tests that re-boot in-process call
# ``reset_deployment_scheduled()`` and re-import the workflow module.
_DEPLOYMENT_SCHEDULED: dict[str, dict[str, Any]] = {}
_DEPLOYMENT_SCHEDULED_GUARD: (
    Callable[[str | None], AbstractContextManager[Any]] | None
) = None


def reset_deployment_scheduled() -> None:
    """Clear the deployment-author schedule registry (worker boot / test reset)."""
    global _DEPLOYMENT_SCHEDULED_GUARD
    _DEPLOYMENT_SCHEDULED.clear()
    _DEPLOYMENT_SCHEDULED_GUARD = None


def configure_deployment_scheduled_guard(
    guard: Callable[[str | None], AbstractContextManager[Any]],
) -> None:
    """Bind deployment-author schedule execution to runtime writer authority."""
    global _DEPLOYMENT_SCHEDULED_GUARD
    _DEPLOYMENT_SCHEDULED_GUARD = guard


def iter_deployment_scheduled() -> list[dict[str, Any]]:
    """Return registered author-schedule entries in registration order.

    Each entry is ``{schedule_id, default_cron, automatic_backfill,
    workflow_fn}``. The substrate worker walks these after launch to persist
    their durable schedule rows.
    """
    return list(_DEPLOYMENT_SCHEDULED.values())


def deployment_scheduled(
    schedule_id: str,
    *,
    default_cron: str,
    automatic_backfill: bool = True,
    guard_body: bool = True,
) -> Callable[[F], F]:
    """Decorate a deployment scheduled workflow.

    The decorated function is registered as a named ``@DBOS.workflow`` and
    recorded in the deployment-author schedule registry. The substrate worker
    persists its durable ``dbos.workflow_schedules`` row after ``DBOS.launch()``
    via ``apply_deployment_author_schedules`` — the same durable model the
    substrate's own schedules use — applying the manifest's
    ``runtime.schedules.<schedule_id>`` cron override (Decision 14) over
    ``default_cron``. Code is the source of truth for *which* schedules exist;
    the manifest is the source of truth for *cadence*.

    NOTE: this deliberately registers a plain ``@DBOS.workflow`` and NOT
    ``@DBOS.scheduled``. ``@DBOS.scheduled`` arms DBOS's native in-process
    scheduler, which the substrate suppresses/bypasses in favor of durable
    ``apply_schedules`` rows. A native scheduled author workflow fires by name
    under a path where it is not resolvable, raising
    ``WorkflowFunctionNotFoundError`` every tick while never running (observed
    live on okg-ci 2026-06-16: ``okg_ci_resource_monitor`` errored every 5 min
    and fleet monitoring never ran, with no ``workflow_schedules`` row at all).

    Parameters
    ----------
    schedule_id : str
        Stable identifier; the workflow registers under this name, and both the
        durable schedule row and the manifest cron override key on it.
    default_cron : str
        Cron expression used when the manifest does not override.
    automatic_backfill : bool, default True
        Whether DBOS replays missed ticks when the durable schedule is applied.
    guard_body : bool, default True
        Hold the deployment writer permit around the workflow body. Enqueue-only
        wrappers may disable this when their child enqueue acquires the same
        permit itself, avoiding a nested advisory-lock deadlock.
    """

    def _decorator(func: F) -> F:
        from dbos import DBOS

        @functools.wraps(func)
        def _guarded(*args, **kwargs):
            from okg.substrate.runtime.schedule_overlap import (
                OverlapSkipped,
                claim_schedule_run,
            )

            # A cron says how often a run should START. On a large graph an
            # author schedule can outlast its own period — okg-workspace's
            # resource monitor averaged ~22 minutes on a 5-minute cron — and
            # every extra copy holds database connections the publish cycle
            # needs. Skip the tick instead of stacking it.
            #
            # The claim is entered OUTSIDE the try, so only a refusal to start
            # is reported as a skip. A body that itself raises OverlapSkipped
            # must not be mislabeled as a run that never started.
            claim = claim_schedule_run(schedule_id)
            try:
                claim.__enter__()
            except OverlapSkipped as skip:
                log.warning(
                    "deployment schedule %s skipped: %s", schedule_id, skip,
                )
                return {
                    "status": "skipped_previous_run_active",
                    "schedule_id": schedule_id,
                    "reason": str(skip),
                    "skipped_runs": skip.skipped_count,
                }
            try:
                return _run_body(*args, **kwargs)
            finally:
                claim.__exit__(None, None, None)

        def _run_body(*args, **kwargs):
            guard = _DEPLOYMENT_SCHEDULED_GUARD
            if guard is None or not guard_body:
                return func(*args, **kwargs)
            from okg.substrate.runtime.promotion_fence import (
                promotion_id_from_recovery_context,
            )

            context = kwargs.get("context")
            if context is None and len(args) > 1:
                context = args[1]
            promotion_id = promotion_id_from_recovery_context(context)
            with guard(promotion_id):
                return func(*args, **kwargs)

        decorated = DBOS.workflow(name=schedule_id)(_guarded)

        # Surface metadata for the substrate's apply-schedules walker + the
        # author-schedule registry it consumes after launch.
        decorated._okg_schedule_id = schedule_id  # type: ignore[attr-defined]
        decorated._okg_default_cron = default_cron  # type: ignore[attr-defined]
        decorated._okg_automatic_backfill = automatic_backfill  # type: ignore[attr-defined]
        _DEPLOYMENT_SCHEDULED[schedule_id] = {
            "schedule_id": schedule_id,
            "default_cron": default_cron,
            "automatic_backfill": automatic_backfill,
            "guard_body": guard_body,
            "workflow_fn": decorated,
        }
        return decorated  # type: ignore[return-value]

    return _decorator


# ---------------------------------------------------------------------------
# Hook-registration decorators
# ---------------------------------------------------------------------------


def _make_hook_decorator(event_type: str) -> Callable:
    """Build a hook-registration decorator for one event type.

    The returned decorator registers the function in the substrate's
    hook registry at module-import time, so the publisher /
    escalation lifecycle finds it when an event fires.
    """

    def _outer(
        deployment: str,
        *,
        workflow_name: str | None = None,
        **kwargs: Any,
    ) -> Callable[[F], F]:
        def _decorator(func: F) -> F:
            # Lazy import so the SDK is usable in environments without
            # the substrate runtime layer (e.g. test fixtures).
            from okg.substrate.runtime.hooks import register_hook

            wf_name = workflow_name or func.__qualname__
            register_hook(
                deployment=deployment,
                event_type=event_type,
                workflow_name=wf_name,
            )

            # Auto-apply @deployment_workflow so the hook is durable.
            # Operators / authors can also decorate explicitly with
            # @deployment_workflow first; the inner DBOS.workflow
            # decorator deduplicates by name.
            if not hasattr(func, "_okg_workflow_name"):
                return deployment_workflow(name=wf_name)(func)
            return func

        # The decorator can be called as @on_*(deployment=...) (without
        # explicit kwargs) or with extra hook-type-specific arguments
        # (e.g. subject_subtype on on_escalation_decided). The kwargs
        # passthrough preserves Phase 2.2.5's signature.
        if kwargs:
            log.debug(
                "%s hook kwargs ignored (Phase 2.1 scope): %s",
                event_type, kwargs,
            )
        return _decorator

    return _outer


on_generation_published = _make_hook_decorator("generation_published")
on_generation_blocked = _make_hook_decorator("generation_blocked")
on_escalation_decided = _make_hook_decorator("escalation_decided")
