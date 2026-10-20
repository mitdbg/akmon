"""Hook registry for substrate post-publish observers (Phase 2 §1.3).

Deployment authors register post-publish callbacks via the SDK's
`@on_generation_published` / `@on_generation_blocked` /
`@on_escalation_decided` decorators. Those decorators (Phase 2 §2.2.5)
call :func:`register_hook` at module-import time; the substrate's
:class:`Publisher.cycle()` invokes :func:`enqueue_hooks` after Phase C
commits, which uses :class:`DBOSClient` to start each registered
workflow on the same partitioned queue as the publisher.

## Atomicity contract

Hook failures MUST NOT roll back the publisher's commit. The publisher
treats hooks as post-publish observers, not part of the cycle's
atomicity boundary:

  1. Publisher.cycle() commits ``status='published'`` (or 'blocked').
  2. Publisher.cycle() then calls :func:`enqueue_hooks` *after* the
     commit. If the enqueue itself fails (DB unreachable, queue
     misconfigured), the failure is logged but does not raise — the
     publisher continues normally.
  3. Hook workflows execute durably on the partitioned queue. Their
     failures (MAX_RECOVERY_ATTEMPTS_EXCEEDED, worker crash) surface
     in ``dbos.workflow_status``; an operator triages via
     ``okg runtime debug-workflow``.

## Queue + concurrency

Hooks run on ``okg-<deployment>`` with concurrency=1 (the same
partitioned queue as the publisher itself). This serializes hooks
per-deployment without forcing the publisher to wait on them: the
publisher enqueues and returns immediately; the queue's concurrency
limit means hooks process one-at-a-time after the publisher's own
workflow yields.

## State

The registry is module-level in-process state. Multiple workers in
the same deployment-DB run the registry independently — each imports
the SDK-decorated module, each registers its hooks, each observes
publisher commits. There is no cross-process synchronization needed
because every worker only enqueues hooks it itself registered (after
importing the deployment's workflow module).
"""
from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any


__all__ = [
    "HOOK_EVENT_GENERATION_PUBLISHED",
    "HOOK_EVENT_GENERATION_BLOCKED",
    "HOOK_EVENT_ESCALATION_DECIDED",
    "clear_hooks",
    "enqueue_hooks",
    "iter_hooks",
    "register_hook",
]


log = logging.getLogger(__name__)


# Event-type constants. SDK decorators use these; the publisher uses
# these. Keeping them as strings (not enums) so the registry is
# trivially picklable / inspectable from operator tooling.
HOOK_EVENT_GENERATION_PUBLISHED = "generation_published"
HOOK_EVENT_GENERATION_BLOCKED = "generation_blocked"
HOOK_EVENT_ESCALATION_DECIDED = "escalation_decided"

_VALID_EVENTS = frozenset({
    HOOK_EVENT_GENERATION_PUBLISHED,
    HOOK_EVENT_GENERATION_BLOCKED,
    HOOK_EVENT_ESCALATION_DECIDED,
})


# Registry keyed by (deployment, event_type). Each value is the
# ordered list of workflow names registered for that pair. Order
# matches import order, which matches the order operators see in
# ``okg deployment show``; the spec says concurrency=1 on the
# partition queue serializes execution, so registration order is also
# the on-the-wire execution order.
_HOOKS: dict[tuple[str, str], list[str]] = defaultdict(list)


def register_hook(
    *,
    deployment: str,
    event_type: str,
    workflow_name: str,
) -> None:
    """Register a hook workflow for an event on a deployment.

    Called by SDK decorators at module-import time, before
    ``DBOS.launch()``. Idempotent on duplicate registrations from the
    same source: re-registering the same ``(deployment, event_type,
    workflow_name)`` is a no-op.

    Raises
    ------
    ValueError
        If ``event_type`` is not one of the three valid event types.
    """
    if event_type not in _VALID_EVENTS:
        raise ValueError(
            f"unknown hook event type {event_type!r}; "
            f"expected one of {sorted(_VALID_EVENTS)}"
        )
    key = (deployment, event_type)
    if workflow_name in _HOOKS[key]:
        log.debug(
            "register_hook: duplicate registration (deployment=%s "
            "event=%s workflow=%s) — keeping existing entry",
            deployment, event_type, workflow_name,
        )
        return
    _HOOKS[key].append(workflow_name)
    log.info(
        "registered hook: deployment=%s event=%s workflow=%s "
        "(%d total)",
        deployment, event_type, workflow_name, len(_HOOKS[key]),
    )


def iter_hooks(
    *,
    deployment: str,
    event_type: str,
) -> list[str]:
    """Return the list of registered hook workflow names.

    Order matches registration order. Returns a fresh list — callers
    may mutate it without affecting the registry.
    """
    if event_type not in _VALID_EVENTS:
        raise ValueError(
            f"unknown hook event type {event_type!r}; "
            f"expected one of {sorted(_VALID_EVENTS)}"
        )
    return list(_HOOKS.get((deployment, event_type), []))


def clear_hooks(
    *,
    deployment: str | None = None,
    event_type: str | None = None,
) -> None:
    """Drop registered hooks. Used by tests to isolate worker boots.

    When both ``deployment`` and ``event_type`` are None, clears the
    entire registry. When one is set, narrows the scope.
    """
    if deployment is None and event_type is None:
        _HOOKS.clear()
        return
    keys_to_drop = [
        k for k in _HOOKS
        if (deployment is None or k[0] == deployment)
        and (event_type is None or k[1] == event_type)
    ]
    for k in keys_to_drop:
        _HOOKS.pop(k, None)


def enqueue_hooks(
    *,
    deployment: str,
    event_type: str,
    dsn: str,
    args: tuple[Any, ...] = (),
) -> list[str]:
    """Enqueue every registered hook on the deployment's partition queue.

    Called by :class:`Publisher.cycle` after the post-commit transaction
    is durable, and by the escalation lifecycle after a decision
    transitions an escalation to ``decided``.

    Each hook is enqueued on ``okg-<deployment>`` (concurrency=1) so
    hooks serialize per-deployment but do not block the publisher.

    Returns
    -------
    list[str]
        The workflow_id of each successfully-enqueued hook, in
        registration order. A hook whose enqueue fails is logged
        and skipped — the publisher's commit is durable independent
        of hook success.
    """
    from dbos import DBOSClient
    from okg.substrate.runtime.promotion_fence import promotion_enqueue_permit

    hooks = iter_hooks(deployment=deployment, event_type=event_type)
    if not hooks:
        return []

    client = DBOSClient(
        database_url=dsn,
        system_database_url=dsn,
        application_database_url=dsn,
    )
    queue_name = f"okg-{deployment}"

    enqueued: list[str] = []
    for workflow_name in hooks:
        try:
            with promotion_enqueue_permit(deployment=deployment, dsn=dsn):
                handle = client.enqueue(
                    {
                        "queue_name": queue_name,
                        "workflow_name": workflow_name,
                    },
                    *args,
                )
            enqueued.append(handle.workflow_id)
            log.info(
                "enqueued hook: deployment=%s event=%s workflow=%s "
                "workflow_id=%s",
                deployment, event_type, workflow_name, handle.workflow_id,
            )
        except Exception as e:  # noqa: BLE001 — best-effort enqueue
            # Per the spec: hook failures must not roll back the
            # publisher's commit. Log loudly and continue.
            log.error(
                "hook enqueue failed: deployment=%s event=%s "
                "workflow=%s error=%s",
                deployment, event_type, workflow_name, e,
            )
    return enqueued


def _registry_snapshot() -> dict[tuple[str, str], list[str]]:
    """Operator diagnostic: snapshot the current registry.

    Used by ``okg deployment show`` to surface the hook contract for
    each deployment alongside the manifest's runtime block. Returns a
    fresh copy of the registry.
    """
    return {k: list(v) for k, v in _HOOKS.items()}
