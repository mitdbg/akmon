"""Declared postcondition flags for destructive migrations.

A destructive migration's recorded postcondition is the only durable evidence
that it did what it claimed. Issue #1054 is what happens when that evidence can
be wrong: ``0001_retire_job_runtime_ledger`` recorded seven relation-absence
flags as ``true`` while the relations it dropped were re-created by
``schema.sql`` on the next apply, and no surface ever re-checked the claim.

Three properties make that failure impossible here.

1. **Single-sourced vocabulary.** The postcondition SELECT is *generated* from
   :data:`POSTCONDITION_FLAGS`, so a flag can never be emitted without a
   declared way to re-evaluate it. A name-stripping heuristic would resolve
   ``job_runs_absent`` and silently give up on ``marker_present``,
   ``reconciliation_function_absent`` and ``job_runtime_subsystem_absent`` --
   leaving drift empty on a drifted database.
2. **Re-evaluable off-transaction.** Every flag declares a check that runs
   against a live cursor at any later time, so
   :func:`reverify_recorded_postcondition` can ask the database today whether a
   claim recorded in 2026-07 is still true.
3. **Unknown is never agreement.** An unregistered flag is reported
   ``unresolved`` rather than skipped.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import psycopg
from psycopg import sql


# How a recorded flag is re-evaluated. The kind is part of the declaration
# because the flag name does not carry it: ``reconciliation_function_absent``
# is a routine, ``marker_present`` is a row test, and
# ``job_runtime_subsystem_absent`` is a compound predicate over many relations.
KIND_RELATION_ABSENT = "relation_absent"
KIND_RELATION_PRESENT = "relation_present"
KIND_RELATION_NONEMPTY = "relation_nonempty"
KIND_ROUTINE_ABSENT = "routine_absent"
KIND_PREDICATE = "predicate"

# Row counts are reported for drifted absence claims so an operator sees the
# size of the false claim (issue #1054: "recorded absent, holds 1,326 rows").
# The count is capped rather than unbounded: a status surface must not scan an
# arbitrarily large table, and a capped count is reported AS capped.
ROW_COUNT_CAP = 1_000_000


@dataclass(frozen=True)
class PostconditionFlag:
    """One declared postcondition flag and how to re-evaluate it."""

    name: str
    kind: str
    target: str
    description: str


def _flag(name: str, kind: str, target: str, description: str) -> PostconditionFlag:
    return PostconditionFlag(name=name, kind=kind, target=target, description=description)


POSTCONDITION_FLAGS: dict[str, PostconditionFlag] = {
    flag.name: flag
    for flag in (
        _flag(
            "reconciliation_view_absent",
            KIND_RELATION_ABSENT,
            "okg.v_job_runtime_reconciliation",
            "legacy job-runtime reconciliation view is gone",
        ),
        _flag(
            "job_queue_view_absent",
            KIND_RELATION_ABSENT,
            "okg.v_job_queue",
            "legacy job queue view is gone",
        ),
        _flag(
            "reconciliation_function_absent",
            KIND_ROUTINE_ABSENT,
            "okg.job_runtime_reconciliation()",
            "legacy reconciliation routine is gone",
        ),
        _flag(
            "legacy_jobs_absent",
            KIND_RELATION_ABSENT,
            "okg.jobs",
            "pre-ledger jobs table is gone",
        ),
        _flag(
            "publisher_leases_absent",
            KIND_RELATION_ABSENT,
            "okg.publisher_leases",
            "pre-ledger publisher lease table is gone",
        ),
        _flag(
            "legacy_jobs_dedupe_index_absent",
            KIND_RELATION_ABSENT,
            "okg.jobs_live_dedupe_uidx",
            "pre-ledger jobs dedupe index is gone",
        ),
        _flag(
            "workload_node_source_index_absent",
            KIND_RELATION_ABSENT,
            "okg.node_facts_workload_source_fact_idx",
            "workload node-fact index is gone",
        ),
        _flag(
            "workload_edge_source_index_absent",
            KIND_RELATION_ABSENT,
            "okg.edge_facts_workload_source_fact_idx",
            "workload edge-fact index is gone",
        ),
        _flag(
            "workload_source_pattern_index_absent",
            KIND_RELATION_ABSENT,
            "okg.edge_facts_source_pattern_idx",
            "workload source-pattern index is gone",
        ),
        _flag(
            "legacy_retirement_marker_absent",
            KIND_RELATION_ABSENT,
            "okg.legacy_runtime_retirement_marker",
            "pre-ledger retirement marker is gone",
        ),
        _flag(
            "job_definitions_absent",
            KIND_RELATION_ABSENT,
            "okg.job_definitions",
            "ledger job_definitions table is gone",
        ),
        _flag(
            "job_dependencies_absent",
            KIND_RELATION_ABSENT,
            "okg.job_dependencies",
            "ledger job_dependencies table is gone",
        ),
        _flag(
            "job_runs_absent",
            KIND_RELATION_ABSENT,
            "okg.job_runs",
            "ledger job_runs table is gone",
        ),
        _flag(
            "job_chunks_absent",
            KIND_RELATION_ABSENT,
            "okg.job_chunks",
            "ledger job_chunks table is gone",
        ),
        _flag(
            "job_items_absent",
            KIND_RELATION_ABSENT,
            "okg.job_items",
            "ledger job_items table is gone",
        ),
        _flag(
            "job_item_frontiers_absent",
            KIND_RELATION_ABSENT,
            "okg.job_item_frontiers",
            "ledger job_item_frontiers table is gone",
        ),
        _flag(
            "job_external_calls_absent",
            KIND_RELATION_ABSENT,
            "okg.job_external_calls",
            "ledger job_external_calls table is gone",
        ),
        _flag(
            "retirement_marker_absent",
            KIND_RELATION_ABSENT,
            "okg.job_runtime_ledger_retirement_marker",
            "ledger retirement marker table is gone",
        ),
        _flag(
            "lock_inventory_present",
            KIND_RELATION_PRESENT,
            "okg.v_runtime_lock_surface_inventory",
            "canonical lock-surface inventory view is installed",
        ),
        _flag(
            "stale_lock_view_present",
            KIND_RELATION_PRESENT,
            "okg.v_runtime_stale_locks",
            "canonical stale-lock view is installed",
        ),
        _flag(
            "job_chunk_inventory_absent",
            KIND_PREDICATE,
            "job_chunk_lock_surface_absent",
            "no job_chunk_claim row remains in the lock-surface inventory",
        ),
        _flag(
            "legacy_cron_schedules_absent",
            KIND_PREDICATE,
            "job_runtime_cron_schedules_absent",
            "no obsolete pg_cron schedule targets this database",
        ),
        _flag(
            "job_runtime_subsystem_absent",
            KIND_PREDICATE,
            "job_runtime_subsystem_absent",
            "no job-runtime relation, routine, or cron schedule remains",
        ),
        # 0001_retire_job_runtime_ledger is deleted from the tree, but ledger
        # rows it wrote survive on live deployments (issue #1054) and MUST stay
        # re-verifiable -- that row is the whole reason this module exists.
        _flag(
            "marker_present",
            KIND_RELATION_NONEMPTY,
            "okg.job_runtime_ledger_retirement_marker",
            "ledger retirement marker table holds its marker row",
        ),
    )
}


# A later destructive migration can legitimately undo an earlier one's end
# state. `0001_retire_job_runtime_ledger` recorded `marker_present` for a marker
# table that `0002_remove_job_runtime_subsystem` then drops on purpose. That is
# supersession, not drift: once the superseding migration is recorded applied
# with its own true postcondition, IT is the current claim, and re-checking the
# superseded row would leave a deployment permanently red with nothing able to
# clear it. Keyed superseded -> superseding.
SUPERSEDED_BY: dict[str, str] = {
    "0001_retire_job_runtime_ledger": "0002_remove_job_runtime_subsystem",
}


class UnknownPostconditionFlag(KeyError):
    """Raised when a flag is emitted or re-checked without a declaration."""


class PostconditionUnevaluable(RuntimeError):
    """A declared flag could not be evaluated against this database."""


def _split_relation(target: str) -> tuple[str, str]:
    schema_name, _, table_name = target.partition(".")
    if not schema_name or not table_name:
        raise ValueError(f"postcondition target {target!r} is not schema-qualified")
    return schema_name, table_name


def _regclass_is_null(cur: psycopg.Cursor, target: str) -> bool:
    cur.execute("SELECT to_regclass(%s) IS NULL AS absent", (target,))
    return bool(_scalar(cur.fetchone(), "absent"))


def _regprocedure_is_null(cur: psycopg.Cursor, target: str) -> bool:
    cur.execute("SELECT to_regprocedure(%s) IS NULL AS absent", (target,))
    return bool(_scalar(cur.fetchone(), "absent"))


def _scalar(row: Any, key: str) -> Any:
    if row is None:
        # A single-row aggregate query returning no row is a broken read, not a
        # false verdict -- refuse rather than coercing to None/False.
        raise RuntimeError(f"postcondition read for {key!r} returned no row")
    if isinstance(row, Mapping):
        return row[key]
    return row[0]


def _predicate_job_chunk_lock_surface_absent(cur: psycopg.Cursor) -> bool:
    if _regclass_is_null(cur, "okg.v_runtime_lock_surface_inventory"):
        # The inventory view is itself a postcondition flag. When it is missing
        # this predicate cannot be evaluated -- say so instead of returning a
        # verdict off a relation that does not exist.
        raise PostconditionUnevaluable(
            "okg.v_runtime_lock_surface_inventory is absent"
        )
    cur.execute(
        """
        SELECT NOT EXISTS (
          SELECT 1
            FROM okg.v_runtime_lock_surface_inventory
           WHERE surface_id = 'job_chunk_claim'
        ) AS absent
        """
    )
    return bool(_scalar(cur.fetchone(), "absent"))


def _predicate_job_runtime_cron_schedules_absent(cur: psycopg.Cursor) -> bool:
    from okg.substrate.db.management import job_runtime_cron_schedules

    return not job_runtime_cron_schedules(cur)


def _predicate_job_runtime_subsystem_absent(cur: psycopg.Cursor) -> bool:
    from okg.substrate.db.management import (
        JOB_RUNTIME_PROCEDURES,
        JOB_RUNTIME_RELATIONS,
        job_runtime_cron_schedules,
    )

    for relation in JOB_RUNTIME_RELATIONS:
        if not _regclass_is_null(cur, relation):
            return False
    for procedure in JOB_RUNTIME_PROCEDURES:
        if not _regprocedure_is_null(cur, procedure):
            return False
    return not job_runtime_cron_schedules(cur)


_PREDICATES: dict[str, Callable[[psycopg.Cursor], bool]] = {
    "job_chunk_lock_surface_absent": _predicate_job_chunk_lock_surface_absent,
    "job_runtime_cron_schedules_absent": _predicate_job_runtime_cron_schedules_absent,
    "job_runtime_subsystem_absent": _predicate_job_runtime_subsystem_absent,
}


def declared_flag(name: str) -> PostconditionFlag:
    """Return the declaration for ``name`` or refuse."""
    try:
        return POSTCONDITION_FLAGS[name]
    except KeyError:
        raise UnknownPostconditionFlag(
            f"postcondition flag {name!r} has no declared re-evaluation; "
            f"add it to POSTCONDITION_FLAGS"
        ) from None


def evaluate_flag(cur: psycopg.Cursor, name: str) -> bool:
    """Evaluate one declared flag against the live catalog."""
    flag = declared_flag(name)
    if flag.kind == KIND_RELATION_ABSENT:
        return _regclass_is_null(cur, flag.target)
    if flag.kind == KIND_RELATION_PRESENT:
        return not _regclass_is_null(cur, flag.target)
    if flag.kind == KIND_ROUTINE_ABSENT:
        return _regprocedure_is_null(cur, flag.target)
    if flag.kind == KIND_RELATION_NONEMPTY:
        if _regclass_is_null(cur, flag.target):
            return False
        schema_name, table_name = _split_relation(flag.target)
        cur.execute(
            sql.SQL("SELECT EXISTS (SELECT 1 FROM {}) AS present").format(
                sql.Identifier(schema_name, table_name)
            )
        )
        return bool(_scalar(cur.fetchone(), "present"))
    if flag.kind == KIND_PREDICATE:
        return bool(_PREDICATES[flag.target](cur))
    raise UnknownPostconditionFlag(
        f"postcondition flag {name!r} declares unknown kind {flag.kind!r}"
    )


def evaluate_postcondition(
    cur: psycopg.Cursor,
    flag_names: Sequence[str],
) -> dict[str, bool]:
    """Evaluate every named flag, in order, against the live catalog.

    Emission goes through the same registry that re-verification reads, so a
    migration cannot record a flag nobody can later re-check.
    """
    return {name: evaluate_flag(cur, name) for name in flag_names}


def _evaluate_flag_in_savepoint(cur: psycopg.Cursor, name: str) -> bool:
    """Evaluate a flag so a database error cannot poison the transaction.

    Re-verification runs inside a read surface (`migration_status`) that keeps
    using the connection afterwards. A failed statement aborts the whole
    transaction in Postgres, so without a savepoint the `unevaluable` arm below
    is unreachable: the caller dies with InFailedSqlTransaction on its NEXT
    query instead of reporting the flag it could not check.
    """
    cur.execute("SAVEPOINT okg_postcondition_flag")
    try:
        value = evaluate_flag(cur, name)
    except Exception:
        cur.execute("ROLLBACK TO SAVEPOINT okg_postcondition_flag")
        raise
    cur.execute("RELEASE SAVEPOINT okg_postcondition_flag")
    return value


def _relation_row_count(cur: psycopg.Cursor, target: str) -> dict[str, Any]:
    """Bounded row count for a drifted relation, or why there is none."""
    cur.execute(
        "SELECT relkind::text AS relkind FROM pg_class WHERE oid = to_regclass(%s)",
        (target,),
    )
    row = cur.fetchone()
    if row is None:
        return {"row_count": None, "row_count_note": "relation disappeared mid-read"}
    relkind = str(_scalar(row, "relkind"))
    if relkind not in {"r", "p", "m", "v", "f"}:
        # Indexes and sequences have no row count of their own.
        return {"row_count": None, "row_count_note": f"relkind {relkind} has no rows"}
    schema_name, table_name = _split_relation(target)
    cur.execute(
        sql.SQL(
            "SELECT count(*)::bigint AS row_count "
            "FROM (SELECT 1 FROM {} LIMIT %s) AS bounded"
        ).format(sql.Identifier(schema_name, table_name)),
        (ROW_COUNT_CAP + 1,),
    )
    counted = int(_scalar(cur.fetchone(), "row_count"))
    if counted > ROW_COUNT_CAP:
        return {"row_count": ROW_COUNT_CAP, "row_count_capped": True}
    return {"row_count": counted}


def reverify_recorded_postcondition(
    cur: psycopg.Cursor,
    migration_id: str,
    recorded: Any,
) -> list[dict[str, Any]]:
    """Re-check a recorded postcondition against the live database.

    Returns one entry per flag that disagrees with reality or cannot be
    resolved. An empty list means the recorded claim still holds. A recorded
    value that is not a boolean-keyed mapping is itself reported -- a
    postcondition nobody can parse is not a postcondition that passed.
    """
    drift: list[dict[str, Any]] = []
    if isinstance(recorded, str) and recorded in POSTCONDITION_FLAGS:
        # The fresh-install baseline recorded a BARE FLAG NAME as the whole
        # postcondition before this module existed. Deployments provisioned in
        # that window carry the string form; reading it as unparseable would
        # leave them permanently red with no command able to rewrite the row.
        # The string names a declared flag, so it is a claim -- re-check it.
        recorded = {recorded: True}
    if not isinstance(recorded, Mapping):
        return [
            {
                "migration_id": migration_id,
                "flag": None,
                "status": "unparseable",
                "recorded": recorded,
                "detail": "recorded postcondition is not a flag mapping",
            }
        ]
    for name, recorded_value in recorded.items():
        flag_name = str(name)
        try:
            flag = declared_flag(flag_name)
        except UnknownPostconditionFlag as exc:
            drift.append(
                {
                    "migration_id": migration_id,
                    "flag": flag_name,
                    "status": "unresolved",
                    "recorded": recorded_value,
                    "detail": str(exc),
                }
            )
            continue
        try:
            actual = _evaluate_flag_in_savepoint(cur, flag_name)
        except UnknownPostconditionFlag:
            raise
        except Exception as exc:  # noqa: BLE001 - a read surface reports, never dies
            drift.append(
                {
                    "migration_id": migration_id,
                    "flag": flag_name,
                    "status": "unevaluable",
                    "recorded": recorded_value,
                    "target": flag.target,
                    "detail": str(exc),
                }
            )
            continue
        if bool(recorded_value) == actual:
            continue
        entry: dict[str, Any] = {
            "migration_id": migration_id,
            "flag": flag_name,
            "status": "drifted",
            "kind": flag.kind,
            "target": flag.target,
            "recorded": bool(recorded_value),
            "actual": actual,
            "detail": flag.description,
        }
        if flag.kind in {
            KIND_RELATION_ABSENT,
            KIND_RELATION_PRESENT,
            KIND_RELATION_NONEMPTY,
        }:
            cur.execute("SAVEPOINT okg_postcondition_count")
            try:
                if not _regclass_is_null(cur, flag.target):
                    entry.update(_relation_row_count(cur, flag.target))
            except psycopg.Error as exc:
                cur.execute("ROLLBACK TO SAVEPOINT okg_postcondition_count")
                entry["row_count"] = None
                entry["row_count_note"] = f"count unavailable: {exc}"
            else:
                cur.execute("RELEASE SAVEPOINT okg_postcondition_count")
        drift.append(entry)
    return drift
