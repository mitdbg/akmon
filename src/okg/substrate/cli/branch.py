"""CLI handler for `okg branch`.

Extracted from ``cli/main.py`` by ``extract-cli-branch``
(2026-05). The branch subcommand family lives here: list,
create, show, abandon. The cli.main helpers are imported
lazily via the shared ``_load_main_helpers()`` pattern (per
Wave D-1).

Post-chronos-cutover, branches are **Chronos branches** (carried in the
branch metadata), not rows in the retired relational ``okg.branches``
table: ``list`` / ``show`` read branch metadata, ``create`` opens a new
Chronos branch off a base, ``abandon`` flips its metadata status to
``abandoned``, and ``delete`` removes the Chronos branch row outright.

``abandon`` and ``delete`` are different operations on purpose (#1306).
``abandon`` is a metadata tombstone -- the row survives in
``_chronos_branch_interval_branches``, and the branch's interval space stays
allocated. ``delete`` removes the row, decrements the parent's ``child_count``
and asks the interval backend for a GC pass, which is what actually returns
storage. History compaction ignores abandoned branches but a live child branch
still blocks it, so an orphaned fork needs ``delete`` (or at least ``abandon``)
before its parent's dead history can be reclaimed.
"""
from __future__ import annotations

import argparse
from typing import Any


# Module-level helper bindings, populated by _load_main_helpers().
_emit_error: Any = None
_emit_ok: Any = None
_resolve_dsn: Any = None
SubstrateError: Any = None


def _load_main_helpers() -> None:
    """Lazily import ``cli.main`` helpers + ``SubstrateError``.

    Called from the first line of ``_cmd_branch``. Subsequent
    calls are a no-op.
    """
    global _emit_error, _emit_ok
    global _resolve_dsn, SubstrateError
    from okg.substrate.cli.main import (
        _emit_error as _e,
        _emit_ok as _o,
        _resolve_dsn as _r,
    )
    from okg.substrate.errors import SubstrateError as _se
    _emit_error = _e
    _emit_ok = _o
    _resolve_dsn = _r
    SubstrateError = _se


def _branch_payload(info: Any) -> dict[str, Any]:
    """Shape a Chronos ``BranchInfo`` into the CLI's branch JSON."""
    md = dict(info.metadata or {})
    return {
        "branch_id": info.branch_id,
        "name": md.get("branch_name", info.branch_id),
        "deployment_name": md.get("deployment_name"),
        "role": md.get("role"),
        "status": md.get("status"),
        "base_branch": md.get("base_branch"),
        "base_generation_id": md.get("base_generation_id"),
        "created_at": getattr(info, "created_at", None),
        "closed_at": md.get("closed_at"),
        "metadata": md,
    }


_ROLE_ORDER = {"default": 0, "quarantine": 1, "experiment": 2}


def _active_source_sync_workflow_id_for_rollover(
    *,
    deployment: str,
    dsn: str,
) -> str | None:
    """Fail-closed probe for source work that has priority over maintenance."""
    from okg.substrate.runtime.dbos_enqueue import active_workflow_id_for_name
    from okg.substrate.runtime.partition_queue import source_sync_queue_name
    from okg.substrate.runtime.source_sync_constants import (
        SOURCE_SYNC_WORKFLOW_NAME,
    )
    from okg.substrate.runtime.source_sync_priority import (
        source_sync_admission_intent_workflow_id,
    )

    intent_id = source_sync_admission_intent_workflow_id(
        dsn=dsn,
        deployment=deployment,
    )
    if intent_id is not None:
        return intent_id
    return active_workflow_id_for_name(
        dsn=dsn,
        queue_name=source_sync_queue_name(deployment),
        workflow_name=SOURCE_SYNC_WORKFLOW_NAME,
    )


def _interval_branch_row(conn: Any, branch_id: str) -> dict[str, Any] | None:
    """Read the interval backend's own branch row.

    ``BranchInfo`` carries no parent/child bookkeeping, but a delete has to
    know both: ``delete_branch`` cascades to descendants, so a branch with
    children is refused unless the operator opts in.
    """
    row = conn.execute(
        """
        SELECT parent_branch_id, child_count
          FROM _chronos_branch_interval_branches
         WHERE branch_id = %s
        """,
        (branch_id,),
    ).fetchone()
    if row is None:
        return None
    return {"parent_branch_id": row[0], "child_count": int(row[1])}


def _interval_descendants(conn: Any, branch_id: str) -> list[str]:
    """Branches ``delete_branch(branch_id)`` would cascade into (excluding it)."""
    rows = conn.execute(
        """
        WITH RECURSIVE subtree(branch_id) AS (
          SELECT branch_id
            FROM _chronos_branch_interval_branches
           WHERE branch_id = %s
          UNION ALL
          SELECT child.branch_id
            FROM _chronos_branch_interval_branches AS child
            JOIN subtree ON child.parent_branch_id = subtree.branch_id
           WHERE child.branch_id <> 'main'
        )
        SELECT branch_id FROM subtree ORDER BY branch_id
        """,
        (branch_id,),
    ).fetchall()
    return [str(row[0]) for row in rows if str(row[0]) != branch_id]


def _interval_checkpoint_ids(conn: Any, branch_ids: list[str]) -> list[str]:
    """Checkpoints owned by these branches.

    ``delete_branch`` does not cascade checkpoints, so these survive the delete
    and keep pinning their snapshot segments from GC. The verb reports them
    rather than deleting generations behind the operator's back.
    """
    if not branch_ids:
        return []
    rows = conn.execute(
        """
        SELECT checkpoint_id
          FROM _chronos_branch_interval_checkpoints
         WHERE branch_id = ANY(%s)
         ORDER BY checkpoint_id
        """,
        (branch_ids,),
    ).fetchall()
    return [str(row[0]) for row in rows]


def _cmd_branch(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg branch list|show|create|abandon|delete` — manage Chronos branches.

    `list` returns one entry per branch in the deployment, `show <name>`
    returns one branch's metadata, `create <name> --role` opens a new
    Chronos branch off a base, `abandon <name>` flips its metadata
    status to 'abandoned' (refusing the default branch), and
    `delete <name> --confirm` removes the Chronos branch row.
    """
    _load_main_helpers()
    from datetime import datetime, timezone

    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.graph_store import (
        deployment_interval_continuation_percent,
    )

    subcmd = getattr(args, "subcmd", None)
    if subcmd not in {
        "list",
        "show",
        "create",
        "abandon",
        "delete",
        "compact",
        "rollover",
    }:
        return _emit_error(
            SubstrateError(
                f"okg branch: unknown subcommand {subcmd!r}",
                fix_hint=(
                    "supported subcommands: list, show, create, abandon, "
                    "delete, compact, rollover"
                ),
            ),
            as_json=as_json,
        )

    try:
        dsn = _resolve_dsn(args)
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)

    deployment = getattr(args, "deployment", None)
    if not deployment:
        return _emit_error(
            SubstrateError(
                "okg branch requires --deployment <name>",
                fix_hint=(
                    "pass --deployment so the query is scoped to a "
                    "single deployment's branches"
                ),
            ),
            as_json=as_json,
        )

    if subcmd == "list":
        with db_connect(dsn, autocommit=True) as conn:
            store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
            branches = [
                _branch_payload(info)
                for info in store.context.list_branches()
                if (info.metadata or {}).get("deployment_name") == deployment
            ]
        branches.sort(
            key=lambda b: (_ROLE_ORDER.get(b["role"], 3), b["name"] or "")
        )
        _emit_ok(
            {"deployment_name": deployment, "branches": branches},
            as_json=as_json,
        )
        return 0

    if subcmd == "create":
        name = getattr(args, "name", None)
        role = getattr(args, "role", None)
        if not name:
            return _emit_error(
                SubstrateError(
                    "okg branch create requires a branch name",
                    fix_hint=(
                        "okg branch create <name> --role "
                        "(experiment|quarantine) --deployment <name>"
                    ),
                ),
                as_json=as_json,
            )
        if role not in {"experiment", "quarantine"}:
            return _emit_error(
                SubstrateError(
                    f"okg branch create --role must be 'experiment' or "
                    f"'quarantine'; got {role!r}",
                    fix_hint=(
                        "the 'default' role is materialized "
                        "automatically per deployment; explicit "
                        "branches must be experiment or quarantine"
                    ),
                ),
                as_json=as_json,
            )
        base_branch = getattr(args, "base_branch", None) or "default"
        # ensure_schema=True guarantees the Chronos default branch exists
        # before we open a new branch off it (fresh deployments).
        with db_connect(dsn, autocommit=True) as conn:
            store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
            # Lineage anchor: the base branch's latest published checkpoint
            # id (a string), or None when the base has no published
            # generation yet (fresh deployment).
            base_cp = store.latest_generation(
                deployment_name=deployment,
                branch=base_branch,
                status="published",
            )
            base_generation_id = (
                base_cp.checkpoint_id if base_cp is not None else None
            )
            try:
                store.create_branch(
                    name,
                    from_branch=base_branch,
                    metadata={
                        "kind": "okg_branch",
                        "deployment_name": deployment,
                        "branch_name": name,
                        "role": role,
                        "status": "active",
                        "base_branch": base_branch,
                        "base_generation_id": base_generation_id,
                        "created_by": "okg branch create",
                    },
                )
            except Exception as exc:  # noqa: BLE001 — map chronos error to CLI envelope
                if type(exc).__name__ == "BranchAlreadyExistsError":
                    return _emit_error(
                        SubstrateError(
                            f"branch {name!r} already exists for deployment "
                            f"{deployment!r}",
                            fix_hint=(
                                f"okg branch show {name} --deployment "
                                f"{deployment} to inspect it"
                            ),
                        ),
                        as_json=as_json,
                    )
                raise
            info = store.context.get_branch(name)
        _emit_ok(_branch_payload(info), as_json=as_json)
        return 0

    if subcmd == "compact":
        name = getattr(args, "name", None)
        if not name:
            return _emit_error(
                SubstrateError(
                    "okg branch compact requires a branch name argument",
                    fix_hint=(
                        "okg branch compact <name> --deployment <name>"
                    ),
                ),
                as_json=as_json,
            )
        # Convention point (d): refuse the default branch. Compaction
        # rewrites branch lineage from a chosen generation; on the
        # deployment's primary lineage that is a destructive history
        # rewrite, so it fails closed here (#694).
        if name == "default":
            return _emit_error(
                SubstrateError(
                    "refusing to compact the default branch",
                    fix_hint=(
                        "the default branch is the deployment's primary "
                        "lineage; compaction rewrites branch history. "
                        "Compact an experiment or quarantine branch "
                        "instead."
                    ),
                ),
                as_json=as_json,
            )
        source_generation_id = getattr(args, "generation_id", None)
        # Dry-run is the default: --confirm is required to rebase (#694).
        if not bool(getattr(args, "confirm", False)):
            _emit_ok(
                {
                    "dry_run": True,
                    "applied": False,
                    "deployment_name": deployment,
                    "branch": name,
                    "would_compact_from": (
                        source_generation_id
                        or "latest-published-generation"
                    ),
                    "hint": (
                        "re-run with --confirm to rebase the branch"
                    ),
                },
                as_json=as_json,
            )
            return 0
        with db_connect(dsn, autocommit=True) as conn:
            store = ChronosGraphStore.from_connection(
                conn,
                ensure_schema=False,
                interval_continuation_percent=(
                    deployment_interval_continuation_percent(deployment)
                ),
            )
            if not source_generation_id:
                latest = store.latest_generation(
                    deployment_name=deployment,
                    branch=name,
                    status="published",
                )
                if latest is None:
                    return _emit_error(
                        SubstrateError(
                            f"branch {name!r} has no published generation "
                            f"for deployment {deployment!r}",
                            fix_hint=(
                                "run `okg generation list --deployment "
                                f"{deployment}` or provide --generation-id"
                            ),
                        ),
                        as_json=as_json,
                    )
                source_generation_id = latest.checkpoint_id
            result = store.compact_branch_from_generation(
                source_generation_id,
                deployment_name=deployment,
                branch=name,
                metadata={"compacted_by": "okg branch compact"},
            )
        _emit_ok(result, as_json=as_json)
        return 0

    if subcmd == "rollover":
        name = getattr(args, "name", None)
        if not name:
            return _emit_error(
                SubstrateError(
                    "okg branch rollover requires a branch name argument",
                    fix_hint=(
                        "okg branch rollover <name> --deployment <name>"
                    ),
                ),
                as_json=as_json,
            )
        source_generation_id = getattr(args, "generation_id", None)
        if not bool(getattr(args, "confirm", False)):
            _emit_ok(
                {
                    "dry_run": True,
                    "applied": False,
                    "deployment_name": deployment,
                    "branch": name,
                    "would_rollover_from": (
                        source_generation_id
                        or "latest-published-generation"
                    ),
                    "history_policy": "preserve_all_generation_checkpoints",
                    "hint": (
                        "stop concurrent publishers, then re-run with "
                        "--confirm to replace the working branch head"
                    ),
                },
                as_json=as_json,
            )
            return 0
        from okg.substrate.errors import PublisherLeaseHeld
        from okg.substrate.deployments import resolve_deployment
        from okg.substrate.publisher.factory import build_deployment_publisher

        manifest = resolve_deployment(deployment)
        maintenance_guard = build_deployment_publisher(
            dsn=dsn,
            deployment_name=deployment,
            deployment_dir=manifest.deployment_dir,
            command="okg branch rollover",
            branch_name=name,
            publisher_overrides={
                "source_sync_priority_probe": lambda: (
                    _active_source_sync_workflow_id_for_rollover(
                        deployment=deployment,
                        dsn=dsn,
                    )
                ),
            },
        )
        try:
            with db_connect(dsn) as conn:
                # Maintenance never waits behind live writers. The session-level
                # publisher mutex and the source-sync admission probes make
                # quiescence an enforced precondition for the full replacement.
                with maintenance_guard._publish_lock_on_connection(
                    conn,
                    mode="dbos",
                    wait_seconds=0.0,
                ):
                    store = ChronosGraphStore.from_connection(
                        conn,
                        ensure_schema=False,
                        autocommit=False,
                        interval_continuation_percent=(
                            deployment_interval_continuation_percent(deployment)
                        ),
                    )
                    if not source_generation_id:
                        latest = store.latest_generation(
                            deployment_name=deployment,
                            branch=name,
                            status="published",
                        )
                        if latest is None:
                            return _emit_error(
                                SubstrateError(
                                    f"branch {name!r} has no published generation "
                                    f"for deployment {deployment!r}",
                                    fix_hint=(
                                        "run `okg generation list --deployment "
                                        f"{deployment}` or provide --generation-id"
                                    ),
                                ),
                                as_json=as_json,
                            )
                        source_generation_id = latest.checkpoint_id
                    try:
                        result = store.rollover_branch_from_generation(
                            source_generation_id,
                            deployment_name=deployment,
                            branch=name,
                            metadata={"rolled_over_by": "okg branch rollover"},
                        )
                        conn.commit()
                        store.context.finalize_external_transaction(committed=True)
                    except Exception:
                        conn.rollback()
                        store.context.finalize_external_transaction(committed=False)
                        raise
        except PublisherLeaseHeld as err:
            return _emit_error(err, as_json=as_json)
        _emit_ok(
            {
                **result,
                "dry_run": False,
                "applied": True,
                "history_policy": "preserve_all_generation_checkpoints",
            },
            as_json=as_json,
        )
        return 0

    if subcmd == "abandon":
        name = getattr(args, "name", None)
        if not name:
            return _emit_error(
                SubstrateError(
                    "okg branch abandon requires a branch name",
                    fix_hint=(
                        "okg branch abandon <name> --deployment "
                        "<name>"
                    ),
                ),
                as_json=as_json,
            )
        if name == "default":
            return _emit_error(
                SubstrateError(
                    "refusing to abandon the default branch",
                    fix_hint=(
                        "the default branch is the deployment's "
                        "primary lineage; abandon experiment or "
                        "quarantine branches instead"
                    ),
                ),
                as_json=as_json,
            )
        with db_connect(dsn, autocommit=True) as conn:
            store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
            try:
                info = store.context.get_branch(name)
            except Exception as exc:  # noqa: BLE001 — map chronos error to CLI envelope
                if type(exc).__name__ == "BranchNotFoundError":
                    return _emit_error(
                        SubstrateError(
                            f"branch {name!r} not found for deployment "
                            f"{deployment!r}",
                            fix_hint=(
                                f"okg branch list --deployment {deployment}"
                            ),
                        ),
                        as_json=as_json,
                    )
                raise
            md = dict(info.metadata or {})
            if md.get("status") == "abandoned":
                return _emit_error(
                    SubstrateError(
                        f"branch {name!r} is already abandoned for "
                        f"deployment {deployment!r}",
                        fix_hint=(
                            f"okg branch list --deployment {deployment}"
                        ),
                    ),
                    as_json=as_json,
                )
            md["status"] = "abandoned"
            md["closed_at"] = datetime.now(timezone.utc).isoformat()
            store.context.update_branch_metadata(name, md)
            info = store.context.get_branch(name)
        _emit_ok(_branch_payload(info), as_json=as_json)
        return 0

    if subcmd == "delete":
        name = getattr(args, "name", None)
        if not name:
            return _emit_error(
                SubstrateError(
                    "okg branch delete requires a branch name",
                    fix_hint=(
                        "okg branch delete <name> --deployment <name> "
                        "--confirm"
                    ),
                ),
                as_json=as_json,
            )
        # Fail closed on the two lineages nothing may remove: the Chronos root
        # and the deployment's primary branch. `merge_guard.PROTECTED_BRANCHES`
        # is the same list `merge` refuses to be pushed into without a human
        # decider, so the two verbs cannot disagree about what is protected.
        from okg.substrate.merge_guard import is_protected_target

        if name == "main" or is_protected_target(name):
            return _emit_error(
                SubstrateError(
                    f"refusing to delete the protected branch {name!r}",
                    fix_hint=(
                        "'main' is the Chronos root and 'default' is the "
                        "deployment's active lineage; delete experiment or "
                        "quarantine branches instead"
                    ),
                ),
                as_json=as_json,
            )
        cascade = bool(getattr(args, "cascade", False))
        with db_connect(dsn, autocommit=True) as conn:
            store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
            try:
                info = store.context.get_branch(name)
            except Exception as exc:  # noqa: BLE001 — map chronos error to CLI envelope
                if type(exc).__name__ == "BranchNotFoundError":
                    return _emit_error(
                        SubstrateError(
                            f"branch {name!r} not found for deployment "
                            f"{deployment!r}",
                            fix_hint=(
                                f"okg branch list --deployment {deployment}"
                            ),
                        ),
                        as_json=as_json,
                    )
                raise
            md = dict(info.metadata or {})
            if md.get("role") == "default":
                return _emit_error(
                    SubstrateError(
                        f"refusing to delete branch {name!r}: it is the "
                        f"default-role branch for deployment "
                        f"{md.get('deployment_name')!r}",
                        fix_hint=(
                            "the default-role branch is a deployment's active "
                            "lineage and is never deletable"
                        ),
                    ),
                    as_json=as_json,
                )
            owner = md.get("deployment_name")
            # Cross-deployment guard: one database holds every deployment's
            # branches, so a mistyped --deployment must not delete another
            # deployment's branch.
            if owner is not None and owner != deployment:
                return _emit_error(
                    SubstrateError(
                        f"branch {name!r} belongs to deployment {owner!r}, "
                        f"not {deployment!r}",
                        fix_hint=(
                            f"re-run with --deployment {owner}, or check "
                            f"`okg branch list --deployment {owner}`"
                        ),
                    ),
                    as_json=as_json,
                )
            row = _interval_branch_row(conn, name)
            if row is None:
                return _emit_error(
                    SubstrateError(
                        f"branch {name!r} is not tracked by the interval "
                        f"backend; refusing to delete",
                        fix_hint=(
                            "only interval-backend Chronos branches are "
                            "deletable through this verb"
                        ),
                    ),
                    as_json=as_json,
                )
            descendants = _interval_descendants(conn, name)
            if descendants and not cascade:
                return _emit_error(
                    SubstrateError(
                        f"branch {name!r} has {len(descendants)} child "
                        f"branch(es); refusing to delete",
                        data={"children": descendants},
                        fix_hint=(
                            "delete the children first, or pass --cascade to "
                            "delete the whole subtree in one call"
                        ),
                    ),
                    as_json=as_json,
                )
            doomed = [name, *descendants] if cascade else [name]
            stranded = _interval_checkpoint_ids(conn, doomed)
            payload = {
                "deployment_name": deployment,
                "branch": name,
                "status": md.get("status"),
                "role": md.get("role"),
                "parent_branch_id": row["parent_branch_id"],
                "cascade": cascade,
                "children": descendants,
                "would_delete_branches": doomed,
                # delete_branch does not cascade checkpoints. Say so: these
                # generations survive and keep pinning their snapshot segments,
                # so the storage they hold is NOT reclaimed by this verb.
                "stranded_checkpoints": stranded,
            }
            if not bool(getattr(args, "confirm", False)):
                _emit_ok(
                    {
                        **payload,
                        "dry_run": True,
                        "applied": False,
                        "hint": (
                            "re-run with --confirm to remove the Chronos "
                            "branch row"
                        ),
                    },
                    as_json=as_json,
                )
                return 0
            # Same lock `create_branch` takes: refuse to mutate branch
            # protection state while a destructive compaction apply is in
            # flight. Held only around the delete, and only in this
            # operator-facing verb -- the best-effort cleanup paths in
            # merge_back keep their current lock-free behavior.
            from okg.substrate.history_compaction import (
                _history_protection_lock,
            )

            try:
                with _history_protection_lock(conn):
                    store.delete_branch(name)
            except SubstrateError as err:
                return _emit_error(err, as_json=as_json)
        _emit_ok(
            {**payload, "dry_run": False, "applied": True, "deleted": doomed},
            as_json=as_json,
        )
        return 0

    # subcmd == "show"
    name = getattr(args, "name", None)
    if not name:
        return _emit_error(
            SubstrateError(
                "okg branch show requires a branch name argument",
                fix_hint="okg branch show <name> --deployment <name>",
            ),
            as_json=as_json,
        )
    with db_connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        try:
            info = store.context.get_branch(name)
        except Exception as exc:  # noqa: BLE001 — map chronos error to CLI envelope
            if type(exc).__name__ == "BranchNotFoundError":
                return _emit_error(
                    SubstrateError(
                        f"branch {name!r} not found for deployment "
                        f"{deployment!r}",
                        fix_hint=(
                            f"okg branch list --deployment {deployment} "
                            "to see available branches"
                        ),
                    ),
                    as_json=as_json,
                )
            raise
    _emit_ok(_branch_payload(info), as_json=as_json)
    return 0


# `_cmd_admission`, `_cmd_admission_submit`, `_cmd_admission_decide_promote`, `_cmd_admission_decide`, `_json_arg_or_file` moved to `cli/admission.py` by `extract-cli-admission`.
