"""The multi-phase migration driver — checkpointed, resumable data motion.

Per `schema-migration-hardening` / req.multiphase-class: migrations
declared ``class: multi_phase`` in ``migrations/manifest.yaml`` are data
motion too large for one transaction (interval-table compaction, large
identity backfills). They are EXCLUDED from migrate's single-commit
invariant and run here instead:

* **Checkpointed batches.** Each batch executes inside its own
  transaction together with the durable watermark update in
  ``okg.schema_migration_phases`` — the batch's effect and its progress
  record commit atomically, so a crash never records work that did not
  happen nor repeats work that did.
* **Crash-resumable.** A re-run reads the recorded per-phase watermark
  and continues from the checkpoint; completed phases are skipped.
* **Doctor-visible mid-state.** In-progress phase rows surface through
  ``migration_status()`` (``multiphase_in_progress``) and the doctor
  findings (``multiphase_migration_in_progress``) while the run is
  paused or between crash and resume.
* **Distinct ledger completion.** The final ledger row carries
  ``applied_via: multiphase_runner`` and ``migration_class:
  multi_phase`` in ``result_metadata``, distinguishing it from
  single-transaction rows (``applied_via: ordered_runner``).

The driver serializes against migrate() and against other multi-phase
runs on the same per-database advisory lock key migrate uses; a held
lock is a typed :class:`MigrationLockTimeout`, never a queued wedge.

Phase specs are Python (a batch of declarative SQL cannot express
"repeat until the watermark stops moving"): shipped multi-phase
migrations register a :class:`MultiphaseSpec` through
:func:`registered_multiphase_specs`, exactly as destructive migrations
register confirm-gated adapters. The migrate() orchestration loop runs
every pending registered spec after its own transaction commits and then
re-runs the ordered pass for the ordinals deferred behind the barrier.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from okg.substrate.db.migration_class import (
    MIGRATION_CLASS_MULTI_PHASE,
    MULTIPHASE_PROGRESS_TABLE,
    declared_migration_class,
)
from okg.substrate.db.migrate import MIGRATE_ADVISORY_LOCK_NAMESPACE
from okg.substrate.errors import (
    MigrationLockTimeout,
    MultiphaseMigrationInvalid,
)
from okg.substrate.db.connection import connect as db_connect


@dataclass(frozen=True)
class MultiphasePhase:
    """One named, resumable phase of a multi-phase migration.

    ``run_batch(conn, watermark)`` performs ONE bounded batch of work
    inside the caller's open transaction and returns the new watermark,
    or ``None`` when the phase has nothing left to do. It must never
    commit or roll back ``conn`` itself — the driver commits the batch
    and the watermark together. The watermark must be JSON-serializable
    (it is stored durably as JSONB); ``None`` in means "start from the
    beginning".
    """

    name: str
    run_batch: Callable[
        [psycopg.Connection, Any | None], Any | None
    ]


@dataclass(frozen=True)
class MultiphaseSpec:
    """The executable phase plan for one declared multi-phase migration."""

    migration_id: str
    phases: tuple[MultiphasePhase, ...]


# Shipped multi-phase migrations register their spec here, keyed by
# migration_id — the analogue of DESTRUCTIVE_MIGRATION_ADAPTERS. Specs are
# built lazily (the spec modules import MultiphasePhase/MultiphaseSpec from
# this module, so eager registration would be an import cycle). The
# reference migration used by tests lives in the test fixture tree (a
# shipped NNNN file would be auto-enumerated and surfaced as pending on
# every real deployment).
def registered_multiphase_specs() -> dict[str, MultiphaseSpec]:
    """The shipped multi-phase migration specs, keyed by migration_id."""
    from okg.substrate.db import interval_narrow_split_migration

    specs = (interval_narrow_split_migration.build_spec(),)
    return {spec.migration_id: spec for spec in specs}


def _progress_table_exists(conn: psycopg.Connection) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT to_regclass(%s)", (MULTIPHASE_PROGRESS_TABLE,)
        )
        return cur.fetchone()[0] is not None


def multiphase_in_progress_rows(cur: psycopg.Cursor) -> list[dict[str, Any]]:
    """In-progress phase rows, for the status / doctor surfaces.

    Returns ``[]`` when the progress table is absent (a database that
    predates it has, by construction, never run a multi-phase
    migration). Rows are plain dicts regardless of the cursor's row
    factory.
    """
    return _multiphase_phase_rows(cur, state="in_progress")


def multiphase_complete_unrecorded_rows(
    cur: psycopg.Cursor, *, recorded_migration_ids: set[str]
) -> list[dict[str, Any]]:
    """Completed phase rows whose migration has NO ledger row yet.

    The one crash window ``multiphase_in_progress_rows`` cannot label: a
    kill after every phase completed but before ``record_known_migration``
    leaves zero in-progress rows while the migration still reports plain
    "pending" — the database is converged but the status looks
    never-started. Surfacing these rows keeps that state labelled; a
    re-run no-ops each phase and records the ledger row.
    """
    return [
        row
        for row in _multiphase_phase_rows(cur, state="complete")
        if str(row.get("migration_id")) not in recorded_migration_ids
    ]


def _multiphase_phase_rows(
    cur: psycopg.Cursor, *, state: str
) -> list[dict[str, Any]]:
    cur.execute("SELECT to_regclass(%s)", (MULTIPHASE_PROGRESS_TABLE,))
    row = cur.fetchone()
    regclass = row[0] if not isinstance(row, dict) else row.get("to_regclass")
    if regclass is None:
        return []
    cur.execute(
        f"""
        SELECT migration_id, phase, batches_committed,
               watermark, started_at, updated_at
          FROM {MULTIPHASE_PROGRESS_TABLE}
         WHERE state = %s
         ORDER BY migration_id, started_at, phase
        """,
        (state,),
    )
    columns = [
        "migration_id",
        "phase",
        "batches_committed",
        "watermark",
        "started_at",
        "updated_at",
    ]
    out: list[dict[str, Any]] = []
    for fetched in cur.fetchall():
        if isinstance(fetched, dict):
            record = {key: fetched.get(key) for key in columns}
        else:
            record = dict(zip(columns, fetched, strict=True))
        for key in ("started_at", "updated_at"):
            if record.get(key) is not None:
                record[key] = str(record[key])
        out.append(record)
    return out


def _acquire_migration_lock(conn: psycopg.Connection) -> None:
    """Take the per-database migration advisory lock (session scope).

    Session scope, not transaction scope: the driver commits per batch,
    and the lock must span every batch. Non-blocking — a held lock is a
    typed refusal, not a wait."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pg_try_advisory_lock("
            "hashtextextended(%s || current_database(), 0))",
            (MIGRATE_ADVISORY_LOCK_NAMESPACE,),
        )
        acquired = bool(cur.fetchone()[0])
    conn.commit()
    if not acquired:
        raise MigrationLockTimeout(
            "another migrate or multi-phase run holds the per-database "
            "migration lock; refusing to run concurrently",
            data={"lock_namespace": MIGRATE_ADVISORY_LOCK_NAMESPACE},
            fix_hint=(
                "wait for the other migration activity to finish "
                "(pg_locks / pg_stat_activity), then re-run"
            ),
        )


def _read_phase_progress(
    conn: psycopg.Connection, migration_id: str, phase: str
) -> tuple[str, Any, int] | None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT state, watermark, batches_committed
              FROM {MULTIPHASE_PROGRESS_TABLE}
             WHERE migration_id = %s AND phase = %s
            """,
            (migration_id, phase),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return str(row[0]), row[1], int(row[2])


def _upsert_phase_progress(
    conn: psycopg.Connection,
    migration_id: str,
    phase: str,
    *,
    state: str,
    watermark: Any,
    increment_batches: bool,
) -> None:
    """Record phase progress INSIDE the caller's open batch transaction."""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {MULTIPHASE_PROGRESS_TABLE}
              (migration_id, phase, state, watermark, batches_committed)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (migration_id, phase) DO UPDATE SET
              state = EXCLUDED.state,
              watermark = EXCLUDED.watermark,
              batches_committed =
                {MULTIPHASE_PROGRESS_TABLE}.batches_committed
                  + %s,
              updated_at = now()
            """,
            (
                migration_id,
                phase,
                state,
                Jsonb(watermark),
                1 if increment_batches else 0,
                1 if increment_batches else 0,
            ),
        )


def _ledger_row(
    conn: psycopg.Connection, migration_id: str
) -> tuple[str, dict[str, Any]] | None:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('okg.schema_migrations')")
        if cur.fetchone()[0] is None:
            return None
        cur.execute(
            "SELECT status, result_metadata FROM okg.schema_migrations "
            "WHERE migration_id = %s",
            (migration_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    metadata = row[1] if isinstance(row[1], dict) else {}
    return str(row[0]), metadata


class _BatchConnection:
    """Connection proxy handed to ``run_batch``: transaction control is
    the DRIVER'S. A phase that commits would detach its data motion from
    the watermark update (a crash between the two silently repeats or
    loses work); a phase that rolls back would discard the driver's
    bookkeeping. Both are refused loudly instead of corrupting the
    checkpoint contract."""

    def __init__(self, conn: psycopg.Connection, migration_id: str) -> None:
        self._okg_real_conn = conn
        self._okg_migration_id = migration_id

    def _refuse(self, verb: str) -> None:
        raise MultiphaseMigrationInvalid(
            f"phase spec for {self._okg_migration_id!r} called "
            f"conn.{verb}() inside run_batch; transaction control "
            "belongs to the driver (batch effect and watermark commit "
            "together)",
            data={
                "migration_id": self._okg_migration_id,
                "detail": f"run_batch called {verb}()",
            },
            fix_hint="remove the commit/rollback from the phase body",
        )

    def commit(self) -> None:
        self._refuse("commit")

    def rollback(self) -> None:
        self._refuse("rollback")

    def __getattr__(self, name: str) -> Any:
        return getattr(self._okg_real_conn, name)


def _validate_spec(spec: MultiphaseSpec, migration_id: str) -> None:
    if spec.migration_id != migration_id:
        raise MultiphaseMigrationInvalid(
            f"phase spec is for {spec.migration_id!r}, not "
            f"{migration_id!r}",
            data={
                "migration_id": migration_id,
                "detail": f"spec targets {spec.migration_id!r}",
            },
            fix_hint="pass the spec registered for this migration",
        )
    if not spec.phases:
        raise MultiphaseMigrationInvalid(
            f"phase spec for {migration_id!r} declares no phases",
            data={"migration_id": migration_id, "detail": "empty phase list"},
            fix_hint="declare at least one phase",
        )
    names = [phase.name for phase in spec.phases]
    if len(set(names)) != len(names):
        raise MultiphaseMigrationInvalid(
            f"phase spec for {migration_id!r} has duplicate phase names",
            data={"migration_id": migration_id, "detail": f"names {names}"},
            fix_hint="phase names must be unique",
        )


def run_multiphase_migration(
    dsn: str,
    migration: Any,
    spec: MultiphaseSpec,
    *,
    manifest: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Run (or resume) one declared multi-phase migration to completion.

    ``migration`` is the :class:`~okg.substrate.db.management.KnownMigration`
    for the declared file (identity + checksum for the ledger row).
    Idempotent: an already-recorded migration returns
    ``status: already_applied`` without touching data; a partial run
    resumes from the durable per-phase watermark.
    """
    from okg.substrate.db.management import (
        load_migrations_manifest,
        record_known_migration,
    )

    migration_id = str(migration.migration_id)
    _validate_spec(spec, migration_id)
    if manifest is None:
        manifest = load_migrations_manifest()
    entry = manifest.get(migration_id)
    if entry is None:
        raise MultiphaseMigrationInvalid(
            f"{migration_id!r} has no migrations/manifest.yaml entry",
            data={
                "migration_id": migration_id,
                "detail": "no manifest entry",
            },
            fix_hint="add the manifest entry with class: multi_phase",
        )
    declared = declared_migration_class(migration_id, entry)
    if declared != MIGRATION_CLASS_MULTI_PHASE:
        raise MultiphaseMigrationInvalid(
            f"{migration_id!r} is declared class {declared!r}; the "
            "multi-phase driver only runs class multi_phase migrations",
            data={"migration_id": migration_id, "detail": declared},
            fix_hint=(
                "declare class: multi_phase in the manifest entry, or "
                "let the ordered runner apply it as a single transaction"
            ),
        )

    with db_connect(dsn) as conn:
        if not _progress_table_exists(conn):
            raise MultiphaseMigrationInvalid(
                f"{MULTIPHASE_PROGRESS_TABLE} is absent; run `okg "
                "migrate` first so the progress table exists",
                data={
                    "migration_id": migration_id,
                    "detail": f"{MULTIPHASE_PROGRESS_TABLE} missing",
                },
                fix_hint="run `okg migrate` before the multi-phase driver",
            )
        _acquire_migration_lock(conn)
        # Lock held from here to connection close (session scope).
        ledger = _ledger_row(conn, migration_id)
        if ledger is not None:
            status, metadata = ledger
            if metadata.get("applied_via") != "multiphase_runner":
                raise MultiphaseMigrationInvalid(
                    f"{migration_id!r} already has a ledger row that was "
                    "not written by the multi-phase driver "
                    f"(applied_via={metadata.get('applied_via')!r})",
                    data={
                        "migration_id": migration_id,
                        "detail": "foreign ledger row",
                    },
                    fix_hint=(
                        "reconcile the ledger; this migration was "
                        "recorded through a different apply path"
                    ),
                )
            return {
                "status": "already_applied",
                "migration_id": migration_id,
                "ledger_status": status,
                "phases": metadata.get("phases", {}),
            }

        batch_counts: dict[str, int] = {}
        for phase in spec.phases:
            progress = _read_phase_progress(conn, migration_id, phase.name)
            if progress is not None and progress[0] == "complete":
                batch_counts[phase.name] = progress[2]
                continue
            watermark = progress[1] if progress is not None else None
            batch_conn = _BatchConnection(conn, migration_id)
            while True:
                new_watermark = phase.run_batch(batch_conn, watermark)
                if new_watermark is not None and new_watermark == watermark:
                    conn.rollback()
                    raise MultiphaseMigrationInvalid(
                        f"phase {phase.name!r} of {migration_id!r} "
                        "returned an unchanged watermark "
                        f"({watermark!r}); a batch must advance or "
                        "return None, or the run loops forever holding "
                        "the migration lock",
                        data={
                            "migration_id": migration_id,
                            "detail": (
                                f"phase {phase.name!r} watermark stuck "
                                f"at {watermark!r}"
                            ),
                        },
                        fix_hint="fix the phase's watermark arithmetic",
                    )
                if new_watermark is None:
                    _upsert_phase_progress(
                        conn,
                        migration_id,
                        phase.name,
                        state="complete",
                        watermark=watermark,
                        increment_batches=False,
                    )
                    conn.commit()
                    break
                _upsert_phase_progress(
                    conn,
                    migration_id,
                    phase.name,
                    state="in_progress",
                    watermark=new_watermark,
                    increment_batches=True,
                )
                # THE checkpoint: batch effect + watermark commit together.
                conn.commit()
                watermark = new_watermark
            refreshed = _read_phase_progress(conn, migration_id, phase.name)
            batch_counts[phase.name] = refreshed[2] if refreshed else 0

        record_known_migration(
            conn,
            migration,
            result_metadata={
                "applied_via": "multiphase_runner",
                "migration_class": MIGRATION_CLASS_MULTI_PHASE,
                "phases": batch_counts,
            },
        )
        conn.commit()

    return {
        "status": "applied",
        "migration_id": migration_id,
        "phases": batch_counts,
    }
