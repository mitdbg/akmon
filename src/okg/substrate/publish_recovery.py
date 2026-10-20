"""Read-only recovery plan for a deployment whose publish was refused (#1791).

A refused publish leaves the source records it imported *staged*: admitted,
never published. The next ``okg ingest`` republishes them before any new
source write (the staged-source redrive), so the supported recovery is to fix
the cause the refusing publish named and run ``okg ingest`` again. ``okg up``
is not that recovery: it upgrades an already-published deployment and refuses
at its ``backup`` phase on authority checks that have nothing to do with the
failed publish. Before this module the ``okg up`` refusal, the provision
summary, and the repository-provenance refusal all pointed elsewhere.

:func:`plan_publish_recovery` observes the deployment and names the one
operation that recovers it. It never writes: the staged-work read runs in a
``REPEATABLE READ, READ ONLY`` transaction, the published counts are the
ones recorded on the published checkpoint (no generation read lease is
taken, so a ``--plan`` run stays mutation-free), and a read failure is
returned as ``ok: false`` with the error named rather than as a verdict.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any
from okg.substrate.support.user_hints import okg_prefix


PUBLISH_RECOVERY_SCHEMA = "okg.publish_recovery_plan.v1"

STATE_HEALTHY = "healthy"
STATE_NEVER_PUBLISHED = "never_published"
STATE_STAGED_WORK_UNPUBLISHED = "staged_work_unpublished"
STATE_CHECKPOINT_BLOCKED = "checkpoint_blocked"
STATE_CHECKPOINT_REPAIR_REQUIRED = "checkpoint_repair_required"
STATE_UNKNOWN = "unknown"

#: Only the default branch carries the canonical publish; the plan says so.
PLAN_BRANCH = "default"

_INCOMPLETE_REASON = "ingest_source_incomplete"
_RECOVERY_SCHEMA = "okg.ingest_incomplete_recovery.v1"

StateReader = Callable[[str, str], Mapping[str, Any]]
HistoryReader = Callable[[str, str], Mapping[str, Any]]
QueueReader = Callable[[str], Mapping[str, Any]]


def plan_publish_recovery(
    *,
    dsn: str,
    deployment: str,
    runtime_enabled: bool | None = None,
    read_state: StateReader | None = None,
    read_history: HistoryReader | None = None,
    read_queues: QueueReader | None = None,
) -> dict[str, Any]:
    """Observe ``deployment`` and name the operation that recovers it.

    ``runtime_enabled`` is the manifest's ``runtime.enabled``; ``None`` means
    the caller could not read it, and the plan reports the execution route as
    unknown rather than guessing. The three readers exist so unit tests can
    drive every state without a database; production callers leave them
    unset.
    """
    read_state = read_state or _read_redrive_state
    read_history = read_history or _read_publish_history
    read_queues = read_queues or _read_queue_health
    try:
        redrive = dict(read_state(dsn, deployment))
        history = dict(read_history(dsn, deployment))
    except Exception as exc:  # noqa: BLE001 - a plan must report, not raise.
        return {
            "schema": PUBLISH_RECOVERY_SCHEMA,
            "ok": False,
            "deployment": deployment,
            "branch": PLAN_BRANCH,
            "state": STATE_UNKNOWN,
            "recovery_command": None,
            "okg_up_applies": False,
            "error": {"type": type(exc).__name__, "message": str(exc)},
            "explanation": (
                "the deployment's publish state could not be read; fix the "
                "database access error before choosing a recovery"
            ),
        }
    try:
        queues = dict(read_queues(dsn))
    except Exception as exc:  # noqa: BLE001 - queue health is reported, not fatal.
        queues = {
            "measured": False,
            "dlq_count": None,
            "active_queue_depth": None,
            "error": {"type": type(exc).__name__, "message": str(exc)},
        }

    plan = _derive(deployment=deployment, redrive=redrive, history=history)
    plan["queues"] = queues
    dlq = queues.get("dlq_count")
    # None means "not measured", never "clean".
    plan["queues_ok"] = None if dlq is None else int(dlq) == 0
    plan["execution"] = _execution(runtime_enabled, deployment)
    return {
        "schema": PUBLISH_RECOVERY_SCHEMA,
        "ok": True,
        "deployment": deployment,
        "branch": PLAN_BRANCH,
        **plan,
    }


def _ingest_command(deployment: str, *, include: list[str] | None = None) -> str:
    selection = f" --include {','.join(include)}" if include else ""
    return f"{okg_prefix()} ingest --deployment {deployment}{selection} --json"


def _provision_command(deployment: str) -> str:
    return f"{okg_prefix()} provision --deployment {deployment} --json"


def _supersede_command(deployment: str, *, apply: bool = True) -> str:
    """The supersede-checkpoint command line, with or without `--apply`.

    `apply=False` gives the dry run, which changes nothing and prints what it
    would retire. Offer that form wherever the command may be pasted as-is:
    the reason below is a placeholder, and an applied supersede cannot be
    undone.
    """
    return (
        f"{okg_prefix()} runtime supersede-checkpoint --deployment "
        f"{deployment} --reason '<what was done about the incomplete sources>' "
        + ("--apply " if apply else "")
        + "--json"
    )


def _describe_command(deployment: str, checkpoint_id: str) -> str:
    return (
        f"{okg_prefix()} generation describe {checkpoint_id} "
        f"--deployment {deployment} --json"
    )


def _execution(runtime_enabled: bool | None, deployment: str) -> dict[str, Any]:
    if runtime_enabled is True:
        return {
            "route": "dbos_enqueue",
            "note": (
                "runtime.enabled is true, so `okg ingest` enqueues the run "
                "and its JSON is an enqueue receipt, not the publish"
            ),
            "follow_up_command": (
                f"{okg_prefix()} sync health --deployment {deployment} --json"
            ),
        }
    if runtime_enabled is False:
        return {
            "route": "inline",
            "note": "runtime.enabled is false; `okg ingest` runs and publishes in-process",
            "follow_up_command": None,
        }
    return {
        "route": "unknown",
        "note": (
            "the manifest's runtime.enabled could not be read; with runtime "
            "enabled the command enqueues instead of publishing inline"
        ),
        "follow_up_command": None,
    }


def _derive(
    *,
    deployment: str,
    redrive: Mapping[str, Any],
    history: Mapping[str, Any],
) -> dict[str, Any]:
    last_published = history.get("last_published")
    last_published = dict(last_published) if isinstance(last_published, Mapping) else None
    staged = bool(redrive.get("staged_work_present"))
    latest_id = redrive.get("latest_checkpoint_id")
    latest_status = redrive.get("latest_checkpoint_status")
    blocked_reason = redrive.get("latest_blocked_reason")
    receipt = redrive.get("latest_blocked_receipt")
    receipt = dict(receipt) if isinstance(receipt, Mapping) else {}

    base: dict[str, Any] = {
        "last_known_good": last_published,
        "staged_source_work": staged,
        "latest_checkpoint": {
            "checkpoint_id": latest_id,
            "status": latest_status,
            "blocked_reason": blocked_reason,
        },
        "diagnose_command": None,
        "escape_hatch_command": None,
        "required_sources": None,
    }

    if latest_status == "blocked" and blocked_reason == _INCOMPLETE_REASON:
        recovery = receipt.get("recovery")
        recovery = dict(recovery) if isinstance(recovery, Mapping) else None
        recovery_state = recovery.get("state") if recovery else None
        if (
            recovery is not None
            and recovery.get("schema") == _RECOVERY_SCHEMA
            and recovery_state == "repair_required"
            and isinstance(recovery.get("source_contract"), list)
        ):
            sources = [
                str(item.get("source"))
                for item in recovery["source_contract"]
                if isinstance(item, Mapping) and item.get("source")
            ]
            return {
                **base,
                "state": STATE_CHECKPOINT_REPAIR_REQUIRED,
                "recovery_command": _ingest_command(deployment, include=sources),
                "required_sources": sources,
                "escape_hatch_command": _supersede_command(deployment),
                "okg_up_applies": False,
                "explanation": (
                    "the latest checkpoint is blocked because a source run did "
                    "not complete; the next run must reproduce the exact source "
                    "selection, run mode and scope the checkpoint recorded, or "
                    "it is refused incomplete_recovery_contract_mismatch. Repair "
                    "the named sources and re-run with that selection; if one of "
                    "them cannot be made to pass, retire the checkpoint's "
                    "recovery authority with the escape hatch and record why"
                ),
            }
        if (
            recovery is not None
            and recovery.get("schema") == _RECOVERY_SCHEMA
            and recovery_state == "frozen"
        ):
            return {
                **base,
                "state": STATE_CHECKPOINT_BLOCKED,
                "recovery_command": _ingest_command(deployment),
                "escape_hatch_command": _supersede_command(deployment),
                "okg_up_applies": False,
                "explanation": (
                    "the latest checkpoint holds repaired source work frozen "
                    "for promotion; `okg ingest` promotes it through the "
                    "publisher's guarded path before any new source write"
                ),
            }
        if recovery_state != "superseded":
            from okg.substrate.ingest.incomplete_recovery import operator_fix_hint

            return {
                **base,
                "state": STATE_CHECKPOINT_BLOCKED,
                "recovery_command": None,
                "legacy_receipt": True,
                "escape_hatch_command": _supersede_command(deployment),
                "okg_up_applies": False,
                "explanation": (
                    "the latest checkpoint is an incomplete-source checkpoint "
                    "whose receipt predates exact recovery authority; a plain "
                    "re-run cannot recover it. " + operator_fix_hint(deployment)
                ),
            }
        # A superseded checkpoint no longer demands a repair; fall through.

    if staged:
        return {
            **base,
            "state": STATE_STAGED_WORK_UNPUBLISHED,
            "recovery_command": _ingest_command(deployment),
            "diagnose_command": _ingest_command(deployment),
            "okg_up_applies": False,
            "explanation": (
                "source records were imported but their publish was refused, "
                "so they wait staged and the last published generation stays "
                "readable. `okg ingest` re-attempts that publish before any "
                "new source write: while the cause persists it stops there "
                "and its error.fix_hint names the cause; once the cause is "
                "fixed it publishes the staged records exactly once, chained "
                "to the previous generation, and a repeat reports "
                "no_source_work"
            ),
        }

    if latest_status == "blocked" and latest_id:
        return {
            **base,
            "state": STATE_CHECKPOINT_BLOCKED,
            "recovery_command": _ingest_command(deployment),
            "diagnose_command": _describe_command(deployment, str(latest_id)),
            "okg_up_applies": False,
            "explanation": (
                "the latest generation was blocked by a publication check; "
                "describe it to read last_blocked_reason, fix that cause, then "
                "re-run `okg ingest`"
            ),
        }

    if last_published is None:
        checkpoint_count = int(history.get("checkpoint_count_any_branch") or 0)
        legacy_published = history.get("legacy_published_generation_id")
        if checkpoint_count == 0 and legacy_published is None:
            return {
                **base,
                "state": STATE_NEVER_PUBLISHED,
                "recovery_command": _provision_command(deployment),
                "okg_up_applies": False,
                "explanation": (
                    "nothing was ever published and nothing is staged; "
                    "`okg provision` runs extensions, migrate, catalog apply, "
                    "the sources and the first publish, and is idempotent"
                ),
            }
        return {
            **base,
            "state": STATE_UNKNOWN,
            "recovery_command": None,
            "okg_up_applies": False,
            "checkpoint_count_any_branch": checkpoint_count,
            "legacy_published_generation_id": legacy_published,
            "explanation": (
                "no published generation exists on the default branch, but the "
                "deployment has checkpoints on another branch or a legacy "
                "generation row; inspect them with `okg generation list` before "
                "choosing a recovery -- re-provisioning would re-run every source"
            ),
        }

    return {
        **base,
        "state": STATE_HEALTHY,
        "recovery_command": None,
        "okg_up_applies": True,
        "explanation": (
            "the latest generation is published and nothing is staged; no "
            "publish recovery is needed"
        ),
    }


def _read_redrive_state(dsn: str, deployment: str) -> Mapping[str, Any]:
    from okg.substrate.db.connection import connect as db_connect
    from okg.substrate.ingest.incomplete_recovery import staged_source_redrive_state

    # The staged-work query is the ingest service's own (one source of truth),
    # but it runs through the connection pool, which waits several seconds
    # before reporting an unreachable server. Probe with a bounded direct
    # connection first so a refusal payload never stalls on a dead DSN.
    with db_connect(dsn, connect_timeout=5):
        pass
    return staged_source_redrive_state(dsn=dsn, deployment=deployment)


def _read_publish_history(dsn: str, deployment: str) -> dict[str, Any]:
    """Recorded facts about the latest published checkpoint; no read lease."""
    from okg.substrate.db.connection import connect as db_connect
    from okg.substrate.ingest.run_base import EXCLUDE_RUN_BASE_SQL

    with db_connect(dsn, connect_timeout=5) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT checkpoint_id, metadata::jsonb
                  FROM public._chronos_branch_interval_checkpoints
                 WHERE metadata::jsonb->>'deployment_name' = %(deployment)s
                   AND COALESCE(metadata::jsonb->>'branch', 'default') = 'default'
                   AND metadata::jsonb->>'status' = 'published'
                 ORDER BY created_at DESC, checkpoint_id DESC
                 LIMIT 1
                """,
                {"deployment": deployment},
            )
            row = cur.fetchone()
            cur.execute(
                f"""
                SELECT count(*)
                  FROM public._chronos_branch_interval_checkpoints
                 WHERE metadata::jsonb->>'deployment_name' = %(deployment)s
                   AND {EXCLUDE_RUN_BASE_SQL}
                """,
                {"deployment": deployment},
            )
            # Counted without run-base checkpoints: a source run's private
            # undo point, never a generation (issue #2993).
            checkpoint_count = int(cur.fetchone()[0])
            cur.execute("SELECT to_regclass('okg.graph_generations')")
            legacy_published = None
            if cur.fetchone()[0] is not None:
                cur.execute(
                    """
                    SELECT max(generation_id)
                      FROM okg.graph_generations
                     WHERE deployment_name = %(deployment)s
                       AND status = 'published'
                    """,
                    {"deployment": deployment},
                )
                legacy_row = cur.fetchone()
                legacy_published = (
                    legacy_row[0] if legacy_row and legacy_row[0] is not None else None
                )
    last_published = None
    if row is not None:
        metadata = dict(row[1] or {})
        last_published = {
            "generation_id": str(row[0]),
            "recorded_node_count": metadata.get("node_count"),
            "recorded_edge_count": metadata.get("edge_count"),
            "published_at": metadata.get("published_at"),
            "predecessor_generation_id": metadata.get("predecessor_generation_id"),
        }
    return {
        "last_published": last_published,
        "checkpoint_count_any_branch": checkpoint_count,
        "legacy_published_generation_id": legacy_published,
    }


def _read_queue_health(dsn: str) -> dict[str, Any]:
    from okg.substrate.db.connection import connect as db_connect
    from okg.substrate.runtime_queue_health import summarize_runtime_queue_health

    # The dead-letter scan walks runtime history; bound it so a refusal
    # payload never waits on it. A cancelled scan is reported as unmeasured.
    with db_connect(
        dsn, connect_timeout=5, options="-c statement_timeout=5000",
    ) as conn:
        with conn.cursor() as cur:
            health = summarize_runtime_queue_health(cur)
    return {
        "measured": health.get("ok") is not None,
        "status": health.get("status"),
        "dlq_count": health.get("dlq_count"),
        "active_queue_depth": health.get("active_queue_depth"),
    }
