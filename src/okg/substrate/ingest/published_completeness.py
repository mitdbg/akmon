"""Published-but-incomplete generations (issue #3342).

A source-sync run takes a group of sources (the selected one plus its
predecessors and dependents). When one of them fails and a sibling succeeds,
okg used to cut a BLOCKED checkpoint of the whole branch head and demand that
the next run repeat exactly the same sources (#987,
``req.publish.incomplete-blocked``). Under ``okg runtime worker`` every other
source's poll is a different selection, so one persistently failing source
stopped every other source from syncing.

Now the run publishes normally, as long as every failed source's own undo
finished (its writes are back at its run base, its watermark and progress are
restored). The successful sources' writes become the new published
generation, their admissions end ``published``, the failed sources' end
``failed``, and the generation records which sources are not current in its
metadata under :data:`METADATA_KEY`.

What the marker records, and what it does not:

- It records a failure only from a run that publishes, that is, a run in
  which at least one selected source completed. A run whose every selected
  source failed publishes nothing, so a source that fails in runs of its own
  (no predecessor or dependent that completes beside it) never appears here;
  its failed admission envelopes are the record of that.
- It is cumulative: an entry stays until a run completes that source. An
  ingest run also drops entries whose ownership id is no longer in the
  source registry, so a removed or re-owned source does not stay listed.
- A publish that is not an ingest run (``okg run --once``, a staged-work
  redrive, a record write-back) carries the previous marker forward
  unchanged.

Read it with :func:`published_generation_completeness`.

When a failed source's undo did NOT finish, the old fail-closed path still
applies: the run freezes a blocked ``repair_required`` checkpoint, stamped
with :data:`FREEZE_POLICY_KEY` so the upgrade below never retires it (and
while a run-base undo is pending the publisher refuses every publish with
``source_run_undo_unfinished`` anyway).

``repair_required`` checkpoints frozen by the old rule before an upgrade are
retired once, with a recorded reason, by
:func:`retire_pre_publish_incomplete_checkpoints`, which ``okg migrate`` runs
after its schema commit.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping, Sequence
from types import SimpleNamespace
from typing import Any

from okg.substrate.errors import SubstrateError

log = logging.getLogger(__name__)

#: Checkpoint-metadata key of the marker on a generation.
METADATA_KEY = "ingest_completeness"
#: Schema of the marker stored under :data:`METADATA_KEY`.
SCHEMA = "okg.published_ingest_completeness.v1"
#: Schema of one ingest run's own receipt (``report["completeness"]``).
RUN_SCHEMA = "okg.ingest_completeness.v1"
INCOMPLETE_REASON = "ingest_source_incomplete"

#: Key on a failed source result that says whether its writes were undone.
UNDO_STATUS_KEY = "undo_status"
#: The run wrote, failed, and its writes and source state were put back.
UNDO_COMPLETED = "completed"
#: The run failed before it wrote anything, so there was nothing to undo.
UNDO_NOTHING_WRITTEN = "nothing_written"
#: Compensation or state restoration raised: what the run wrote may remain.
UNDO_UNRESOLVED = "unresolved"
_UNDO_FINISHED = frozenset({UNDO_COMPLETED, UNDO_NOTHING_WRITTEN})

#: Stamped on the ``blocked_reason`` of a freeze cut on purpose after #3342
#: (a failed source's undo did not finish). The upgrade retirement leaves
#: such a checkpoint alone; every unstamped freeze predates this code, or
#: continues the repair of one that does.
FREEZE_POLICY_KEY = "freeze_policy"
FREEZE_POLICY_UNDO_UNFINISHED = "post_3342_undo_unfinished"

#: Recorded on every checkpoint the upgrade retires.
UPGRADE_SUPERSEDE_REASON = "upgrade_3342_publish_incomplete"
UPGRADE_SUPERSEDE_ACTOR = "okg migrate"


def ingest_completeness(
    *,
    plan: Any,
    results: list[dict[str, Any]],
    failed_source_details: list[dict[str, Any]],
    dependency_blocked_sources: Mapping[str, list[str]],
) -> dict[str, Any]:
    """Build the stable source-plan completion receipt for one ingest run."""
    failed_names = {str(detail["source"]) for detail in failed_source_details}
    failed = [
        {key: value for key, value in detail.items() if value is not None}
        for name in plan.ordered
        for detail in failed_source_details
        if detail["source"] == name
    ]
    blocked = [
        {
            "source": name,
            "blocked_by": list(dependency_blocked_sources[name]),
        }
        for name in plan.ordered
        if name in dependency_blocked_sources
    ]
    completed = [
        str(result["name"])
        for result in results
        if result.get("status") == "OK" and str(result.get("name")) not in failed_names
    ]
    incomplete = bool(failed or blocked)
    return {
        "schema": RUN_SCHEMA,
        "status": "incomplete" if incomplete else "complete",
        "reason": INCOMPLETE_REASON if incomplete else None,
        "selected_sources": list(plan.ordered),
        "completed_sources": completed,
        "failed_sources": failed,
        "dependency_blocked_sources": blocked,
    }


def undo_status(*, wrote: bool, recovery_blocked: bool) -> str:
    """The :data:`UNDO_STATUS_KEY` value of one failed source result."""
    if recovery_blocked:
        return UNDO_UNRESOLVED
    return UNDO_COMPLETED if wrote else UNDO_NOTHING_WRITTEN


def unfinished_undo_sources(results: Iterable[Mapping[str, Any]]) -> list[str]:
    """Failed sources whose undo is not known to have finished.

    A failed result without :data:`UNDO_STATUS_KEY` counts as unfinished: a
    producer that does not say its writes were undone must not let the run
    publish over them.
    """
    return [
        str(result.get("name"))
        for result in results
        if result.get("status") != "OK"
        and result.get(UNDO_STATUS_KEY) not in _UNDO_FINISHED
    ]


def partial_failure_may_publish(results: Iterable[Mapping[str, Any]]) -> bool:
    """Whether an incomplete run publishes instead of freezing (#3342)."""
    return not unfinished_undo_sources(results)


def freeze_state(
    *,
    incomplete: bool,
    results: Iterable[Mapping[str, Any]],
) -> str:
    """The recovery state of a freeze this run cuts.

    ``repair_required`` (it holds every other selection) only when the run
    is incomplete AND a failed source's undo did not finish. A repair run of
    an earlier freeze whose failures all finished their undo is ``frozen``:
    the same run promotes it to a published, incomplete-marked generation,
    exactly as a successful repair does, so the hold ends (#3342).
    """
    from okg.substrate.ingest.incomplete_recovery import FROZEN, REPAIR_REQUIRED

    if incomplete and not partial_failure_may_publish(results):
        return REPAIR_REQUIRED
    return FROZEN


def stamp_freeze(
    blocker: dict[str, Any] | None,
    results: Iterable[Mapping[str, Any]],
) -> None:
    """Mark a freeze cut because an undo did not finish (in place).

    The stamp sits beside ``recovery`` in the checkpoint's ``blocked_reason``,
    not inside it, so the recovery contract and its readers are unchanged.
    """
    unfinished = unfinished_undo_sources(results)
    if blocker is None or not unfinished:
        return
    blocker[FREEZE_POLICY_KEY] = FREEZE_POLICY_UNDO_UNFINISHED
    blocker["unfinished_undo_sources"] = unfinished


def run_marker(
    completeness: Mapping[str, Any],
    runnable: Mapping[str, Any],
    registry: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """One run's contribution to the generation marker.

    ``runnable`` maps each selected registry name to its source entry (for
    its ``ownership_id``). ``registry`` is the whole current source registry
    (name to entry); when given, merging drops earlier entries for sources
    that are no longer registered.
    """
    ownership_ids = {
        name: getattr(entry, "ownership_id", None)
        for name, entry in runnable.items()
    }

    def owned(name: Any) -> dict[str, Any]:
        return {
            "source": str(name),
            "ownership_id": (
                str(ownership_ids[name]) if ownership_ids.get(name) else None
            ),
        }

    return {
        "schema": RUN_SCHEMA,
        "completed_sources": [
            owned(name) for name in completeness.get("completed_sources") or ()
        ],
        "failed_sources": [
            {
                **owned(item["source"]),
                **{
                    key: item[key]
                    for key in ("stage", "code")
                    if item.get(key) is not None
                },
            }
            for item in completeness.get("failed_sources") or ()
        ],
        "dependency_blocked_sources": [
            {
                **owned(item["source"]),
                "blocked_by": list(item.get("blocked_by") or ()),
            }
            for item in completeness.get("dependency_blocked_sources") or ()
        ],
        **(
            {
                "registered": sorted(
                    _identity({
                        "source": name,
                        "ownership_id": getattr(entry, "ownership_id", None),
                    })
                    for name, entry in registry.items()
                ),
            }
            if registry is not None
            else {}
        ),
    }


def deferred_run_marker(
    receipt: Mapping[str, Any] | None,
    registry: Mapping[str, Any],
) -> dict[str, Any] | None:
    """The run marker of a ``frozen`` checkpoint promoted by a later run (#3375).

    A repair run promotes its own ``frozen`` checkpoint with its
    :func:`run_marker`. When that promotion is refused, the next run promotes
    the checkpoint before its source writes. ``receipt`` is that checkpoint's
    ``blocked_reason``: :func:`incomplete_recovery.blocker_receipt` copied the
    repair run's whole completeness receipt into it, and its recovery
    ``source_contract`` names every selected source with its ownership id. So
    this rebuilds the marker the refused promotion would have published.
    ``registry`` is the current source registry (name to entry).

    A ``blocked_reason`` that is not a run completeness receipt returns None
    (the promotion carries the previous marker forward) and logs a warning.
    """
    if not isinstance(receipt, Mapping) or receipt.get("schema") != RUN_SCHEMA:
        log.warning(
            "frozen checkpoint has no %s receipt; its promotion carries the "
            "previous ingest_completeness marker forward", RUN_SCHEMA,
        )
        return None
    recovery = receipt.get("recovery")
    contract = recovery.get("source_contract") if isinstance(recovery, Mapping) else None
    runnable = {
        str(item["source"]): SimpleNamespace(ownership_id=item.get("ownership_id"))
        for item in contract or ()
        if isinstance(item, Mapping) and item.get("source")
    }
    return run_marker(receipt, runnable, registry)


def _identity(entry: Mapping[str, Any]) -> str:
    owner = entry.get("ownership_id")
    return f"ownership:{owner}" if owner else f"source:{entry.get('source')}"


def _entries(marker: Mapping[str, Any] | None, key: str) -> list[dict[str, Any]]:
    if not isinstance(marker, Mapping):
        return []
    return [
        dict(item) for item in marker.get(key) or ()
        if isinstance(item, Mapping) and item.get("source")
    ]


def _marker(
    failed: Sequence[Mapping[str, Any]],
    blocked: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    incomplete = bool(failed or blocked)
    return {
        "schema": SCHEMA,
        "status": "incomplete" if incomplete else "complete",
        "reason": INCOMPLETE_REASON if incomplete else None,
        "failed_sources": sorted(
            (dict(item) for item in failed), key=lambda item: str(item["source"]),
        ),
        "dependency_blocked_sources": sorted(
            (dict(item) for item in blocked), key=lambda item: str(item["source"]),
        ),
    }


def merge_marker(
    previous: Mapping[str, Any] | None,
    run: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    """The marker a new generation records.

    ``previous`` is the previous published generation's marker (or None).
    ``run`` is :func:`run_marker` of the ingest run publishing now, or None
    for a publish that is not an ingest run: the previous marker is then
    carried forward unchanged. A source the run completed, failed, or held
    back replaces its earlier entry; an earlier entry whose source is not in
    the run's ``registered`` list is dropped; every other one stays.
    """
    if run is None:
        if not isinstance(previous, Mapping) or previous.get("schema") != SCHEMA:
            return None
        return _marker(
            _entries(previous, "failed_sources"),
            _entries(previous, "dependency_blocked_sources"),
        )
    failed = _entries(run, "failed_sources")
    blocked = _entries(run, "dependency_blocked_sources")
    touched = {
        _identity(item)
        for item in [*_entries(run, "completed_sources"), *failed, *blocked]
    }
    registered = run.get("registered")
    registered = set(registered) if isinstance(registered, list) else None

    def carried(key: str) -> list[dict[str, Any]]:
        return [
            item for item in _entries(previous, key)
            if _identity(item) not in touched
            and (registered is None or _identity(item) in registered)
        ]

    carried_failed = carried("failed_sources")
    carried_blocked = carried("dependency_blocked_sources")
    return _marker([*carried_failed, *failed], [*carried_blocked, *blocked])


def _comparable(marker: Mapping[str, Any] | None) -> dict[str, Any]:
    # A generation recorded before #3342 has no marker; it reads the same as
    # a complete one, so the first clean run after an upgrade does not cut a
    # generation only to say "complete".
    if not isinstance(marker, Mapping) or marker.get("schema") != SCHEMA:
        return _marker([], [])
    return _marker(
        _entries(marker, "failed_sources"),
        _entries(marker, "dependency_blocked_sources"),
    )


def marker_changes(
    previous: Mapping[str, Any] | None,
    merged: Mapping[str, Any] | None,
) -> bool:
    """Whether ``merged`` says something ``previous`` does not."""
    return _comparable(previous) != _comparable(merged)


def _latest_published_row(
    conn: Any,
    *,
    deployment: str,
    branch: str,
) -> tuple[str, dict[str, Any]] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT checkpoint_id, metadata::jsonb
              FROM public._chronos_branch_interval_checkpoints
             WHERE metadata::jsonb->>'deployment_name' = %(deployment)s
               AND COALESCE(metadata::jsonb->>'branch', 'default') = %(branch)s
               AND metadata::jsonb->>'status' = 'published'
             ORDER BY created_at DESC, checkpoint_id DESC
             LIMIT 1
            """,
            {"deployment": deployment, "branch": branch},
        )
        row = cur.fetchone()
    if row is None:
        return None
    return str(row[0]), dict(row[1] or {})


def published_generation_completeness(
    conn: Any,
    deployment: str,
    branch: str = "default",
) -> dict[str, Any] | None:
    """Whether the latest published generation is complete, and if not why.

    Returns None when the deployment has no published generation. Otherwise
    a dict with ``generation_id``, ``status``, ``reason``,
    ``failed_sources`` and ``dependency_blocked_sources``. ``status`` is
    ``complete``, ``incomplete``, or ``unrecorded`` for a generation
    published before this marker existed (no ingest run has published
    since). Each source entry carries ``source`` (the registry name) and
    ``ownership_id``; failed entries also carry the failing ``stage`` and
    ``code`` when known, held-back entries carry ``blocked_by`` (registry
    names of the failed predecessors).
    """
    row = _latest_published_row(conn, deployment=deployment, branch=branch)
    if row is None:
        return None
    generation_id, metadata = row
    marker = metadata.get(METADATA_KEY)
    if not isinstance(marker, Mapping) or marker.get("schema") != SCHEMA:
        return {
            "generation_id": generation_id,
            "status": "unrecorded",
            "reason": None,
            "failed_sources": [],
            "dependency_blocked_sources": [],
        }
    current = _comparable(marker)
    return {
        "generation_id": generation_id,
        "status": current["status"],
        "reason": current["reason"],
        "failed_sources": current["failed_sources"],
        "dependency_blocked_sources": current["dependency_blocked_sources"],
    }


def run_changes_published_marker(
    *,
    dsn: str,
    deployment: str,
    run: Mapping[str, Any],
    branch: str = "default",
) -> bool:
    """Whether publishing ``run`` would change the latest published marker.

    An ingest run that would otherwise publish nothing (a clean no-op, or a
    run in which every source failed) still publishes when it does: the
    generation then records that a listed source recovered, or that a new
    one failed. A deployment with no published generation has no marker to
    change.
    """
    from okg.substrate.db.pool import GENERAL, pooled_connection

    with pooled_connection(dsn, purpose=GENERAL) as conn:
        row = _latest_published_row(conn, deployment=deployment, branch=branch)
    if row is None:
        return False
    previous = row[1].get(METADATA_KEY)
    return marker_changes(previous, merge_marker(previous, run))


def _latest_incomplete_freeze(
    conn: Any,
    *,
    deployment: str,
) -> tuple[str, str] | None:
    """The latest checkpoint's id and state when it is a pre-#3342 freeze.

    Only ``repair_required``: a ``frozen`` checkpoint is promoted by the next
    run (with its admission feedback) and starves nothing, and superseding it
    would leave its admissions blocked. A freeze stamped with
    :data:`FREEZE_POLICY_KEY` was cut on purpose by this code and stays.
    """
    from okg.substrate.ingest.incomplete_recovery import (
        BLOCKED_INGEST_REASON,
        REPAIR_REQUIRED,
    )
    from okg.substrate.ingest.run_base import EXCLUDE_RUN_BASE_SQL

    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('public._chronos_branch_interval_checkpoints')")
        if cur.fetchone()[0] is None:
            return None
        cur.execute(
            f"""
            SELECT checkpoint_id, metadata::jsonb
              FROM public._chronos_branch_interval_checkpoints
             WHERE metadata::jsonb->>'deployment_name' = %(deployment)s
               AND COALESCE(metadata::jsonb->>'branch', 'default') = 'default'
               AND {EXCLUDE_RUN_BASE_SQL}
             ORDER BY created_at DESC, checkpoint_id DESC
             LIMIT 1
            """,
            {"deployment": deployment},
        )
        row = cur.fetchone()
    if row is None:
        return None
    metadata = dict(row[1] or {})
    blocked = metadata.get("blocked_reason")
    recovery = blocked.get("recovery") if isinstance(blocked, Mapping) else None
    if (
        metadata.get("status") != "blocked"
        or not isinstance(blocked, Mapping)
        or blocked.get("reason") != BLOCKED_INGEST_REASON
        or blocked.get(FREEZE_POLICY_KEY) is not None
        or not isinstance(recovery, Mapping)
        or recovery.get("state") != REPAIR_REQUIRED
    ):
        return None
    return str(row[0]), str(recovery["state"])


def _deployments_with_incomplete_blocks(conn: Any) -> list[str]:
    """Deployments with any blocked incomplete-ingest checkpoint."""
    from okg.substrate.ingest.incomplete_recovery import BLOCKED_INGEST_REASON

    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('public._chronos_branch_interval_checkpoints')")
        if cur.fetchone()[0] is None:
            return []
        cur.execute(
            """
            SELECT DISTINCT metadata::jsonb->>'deployment_name'
              FROM public._chronos_branch_interval_checkpoints
             WHERE metadata::jsonb->>'status' = 'blocked'
               AND metadata::jsonb #>> '{blocked_reason,reason}' = %(reason)s
               AND metadata::jsonb->>'deployment_name' IS NOT NULL
             ORDER BY 1
            """,
            {"reason": BLOCKED_INGEST_REASON},
        )
        return [str(row[0]) for row in cur.fetchall()]


def retire_pre_publish_incomplete_checkpoints(
    dsn: str,
    deployments: Iterable[str] | None = None,
    *,
    apply: bool = True,
) -> list[dict[str, Any]]:
    """Retire repair contracts frozen by the pre-#3342 rule, once.

    For each deployment whose latest checkpoint is a blocked incomplete
    freeze in ``repair_required`` that this code did not stamp (see
    :func:`_latest_incomplete_freeze`), run the operator supersede
    (:func:`incomplete_recovery.supersede_incomplete_checkpoint`) with actor
    :data:`UPGRADE_SUPERSEDE_ACTOR` and reason
    :data:`UPGRADE_SUPERSEDE_REASON`. The supersede records on the
    checkpoint who did it, when, why, and the state it retired; nothing is
    deleted, the checkpoint stays blocked, and the next run resumes each
    source from its own saved progress.

    ``deployments`` None means every deployment in the database that has a
    blocked incomplete-ingest checkpoint at all. Idempotent: a deployment
    whose latest checkpoint is not such a freeze, or is already superseded,
    is skipped. A checkpoint that moved while it was being retired (a
    concurrent publish) is reported with ``skipped`` and ``applied: false``
    and retried by the next migrate. ``apply=False`` reports what would be
    retired and writes nothing. A checkpoint with no recovery
    authority at all (a receipt older than the repair contract) is not
    touched; its own fix is ``okg run --once --inline --force-enrichers``.
    """
    # The scan uses direct connections, closed on return, so a migrate with
    # nothing to retire opens no pooled connection. A retirement runs the
    # operator supersede, which borrows this process's pool for the DSN;
    # `okg migrate` is a short-lived process, and an in-process caller that
    # drops the database afterwards must close pools first
    # (`db.pool.close_all`), as it must after any other pooled read.
    from okg.substrate.db.connection import connect as db_connect
    from okg.substrate.ingest.incomplete_recovery import (
        supersede_incomplete_checkpoint,
    )

    if deployments is None:
        with db_connect(dsn, autocommit=True) as conn:
            deployments = _deployments_with_incomplete_blocks(conn)
    reports: list[dict[str, Any]] = []
    seen: set[str] = set()
    for deployment in deployments:
        if not deployment or deployment in seen:
            continue
        seen.add(deployment)
        with db_connect(dsn, autocommit=True) as conn:
            freeze = _latest_incomplete_freeze(conn, deployment=deployment)
        if freeze is None:
            continue
        checkpoint_id, state = freeze
        try:
            report = supersede_incomplete_checkpoint(
                dsn=dsn,
                deployment=deployment,
                reason=UPGRADE_SUPERSEDE_REASON,
                actor=UPGRADE_SUPERSEDE_ACTOR,
                checkpoint_id=checkpoint_id,
                apply=apply,
            )
        except SubstrateError as exc:
            reason = exc.data.get("reason")
            if reason in {
                "incomplete_recovery_already_superseded",
                "incomplete_recovery_nothing_to_supersede",
            }:
                continue
            if reason in {
                "incomplete_recovery_supersede_checkpoint_drift",
                "incomplete_recovery_supersede_state_drift",
            }:
                reports.append({
                    "deployment": deployment,
                    "checkpoint_id": checkpoint_id,
                    "retired_state": state,
                    "applied": False,
                    "skipped": reason,
                })
                continue
            raise
        report["retired_state"] = state
        reports.append(report)
        if report.get("applied"):
            log.warning(
                "retired the %s repair contract of blocked checkpoint %s "
                "(deployment %s, reason %s): an incomplete run now publishes "
                "and records its failed sources instead",
                state, checkpoint_id, deployment, UPGRADE_SUPERSEDE_REASON,
            )
    return reports


def retire_contracts_of_removed_sources(
    dsn: str,
    deployment: str | None = None,
    *,
    actor: str | None = None,
    apply: bool = True,
) -> list[dict[str, Any]]:
    """Retire repair contracts whose required sources were removed or renamed.

    With ``deployment``: classify that deployment's `repair_required`
    contract against its source registry and retire it when
    :func:`incomplete_recovery.retire_removed_source_contract` says it is
    safe, as ``actor`` (default ``okg migrate``). Returns the one report, or
    nothing when the deployment holds no such contract.

    Without ``deployment`` no source registry can be loaded, so each
    deployment that holds a `repair_required` contract is reported as a
    candidate naming ``okg migrate --deployment <name> --apply``, and
    nothing is retired.
    """
    from okg.substrate.db.connection import connect as db_connect
    from okg.substrate.ingest import incomplete_recovery as ir

    if deployment:
        report = ir.retire_removed_source_contract_for_deployment(
            dsn=dsn,
            deployment=deployment,
            actor=actor or ir.MIGRATE_ACTOR,
            apply=apply,
        )
        return [report] if report is not None else []
    with db_connect(dsn, autocommit=True) as conn:
        names = _deployments_with_incomplete_blocks(conn)
    candidates = [
        ir.removed_source_contract_candidate(dsn=dsn, deployment=name)
        for name in names
    ]
    return [item for item in candidates if item is not None]


__all__ = [
    "INCOMPLETE_REASON",
    "METADATA_KEY",
    "RUN_SCHEMA",
    "SCHEMA",
    "UNDO_COMPLETED",
    "UNDO_NOTHING_WRITTEN",
    "UNDO_STATUS_KEY",
    "UNDO_UNRESOLVED",
    "FREEZE_POLICY_KEY",
    "FREEZE_POLICY_UNDO_UNFINISHED",
    "UPGRADE_SUPERSEDE_ACTOR",
    "UPGRADE_SUPERSEDE_REASON",
    "deferred_run_marker",
    "ingest_completeness",
    "marker_changes",
    "merge_marker",
    "partial_failure_may_publish",
    "published_generation_completeness",
    "retire_pre_publish_incomplete_checkpoints",
    "retire_contracts_of_removed_sources",
    "freeze_state",
    "stamp_freeze",
    "run_changes_published_marker",
    "run_marker",
    "undo_status",
    "unfinished_undo_sources",
]
