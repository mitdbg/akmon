"""Database-management inspection helpers for the substrate.

The CLI and doctor both use this module so database health semantics stay
in one place. All inspection functions are read-only unless a caller
explicitly invokes ``drop_database``.
"""
from __future__ import annotations

import hashlib
import importlib.resources
import os
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from okg.substrate.db import dsn as dsn_mod
from okg.substrate.db.migration_class import (
    MIGRATION_CLASS_MULTI_PHASE,
    declared_migration_class,
    is_undeclared_data_migration,
)
from okg.substrate.db.migration_postcondition import (
    POSTCONDITION_FLAGS,
    SUPERSEDED_BY,
    evaluate_postcondition,
    reverify_recorded_postcondition,
)
from okg.substrate.errors import (
    MigrationClassUndeclared,
    MigrationManifestChecksumMismatch,
    MigrationManifestDestructiveDisagreement,
    MigrationManifestEntryMissing,
    MigrationManifestPrerequisiteUnmet,
    RequiresConfirm,
    SubstrateError,
)
from okg.substrate.db.connection import connect as db_connect


SCHEMA_SNAPSHOT_MIGRATION_ID = "0000_schema_snapshot"
REMOVE_JOB_RUNTIME_SUBSYSTEM_MIGRATION_ID = "0002_remove_job_runtime_subsystem"
DEPLOYMENT_PROMOTION_FENCE_MIGRATION_ID = "0003_deployment_promotion_fence"
# Migration 0003 briefly shipped with session-level SET statements before the
# canonical SET LOCAL form was restored. Its durable DDL is identical, and
# okg-workspace recorded that interim checksum. Accept only this audited pair;
# tying the compatibility to the canonical checksum ensures any future edit
# still fails closed.
_ORDERED_MIGRATION_CHECKSUM_COMPATIBILITIES: dict[str, dict[str, Any]] = {
    DEPLOYMENT_PROMOTION_FENCE_MIGRATION_ID: {
        "canonical_checksum": (
            "2189be32b689869225a5b75b23ae8f99cf733b79d5ec250f518edfb3817fcd86"
        ),
        "compatible_recorded_checksums": {
            "62d3e8b167509386144464ddf5270f4b43765211bb2f1bd77346b375e5f823bc"
        },
        "reason": (
            "audited legacy variant differs only by SET versus SET LOCAL "
            "session-timeout scope; durable DDL is identical"
        ),
    },
    "0020_interval_narrow_split": {
        "canonical_checksum": (
            "0fb391996cef32cad9d0c3c589b5841c47cce9ad79d708eabe34ad4ba7aca257"
        ),
        "compatible_recorded_checksums": {
            "a1cfd4ca74a6bb96b5ae4ecf01b200019293aac441925b86f5d6b28ca87f9968"
        },
        "reason": (
            "audited rephasing for phase-interval-narrow-split-migration: "
            "the recorded checksum is the original "
            "single-transaction DO-block variant, whose durable end state "
            "(hash functions, okg.attribute_versions, NOT NULL attrs_hash "
            "columns, composite FKs, wide columns dropped) is identical to "
            "what the multi-phase driver now produces; databases that "
            "applied it atomically are already at that end state"
        ),
    },
    "0004_effective_view_raw_attrs_nullable": {
        "canonical_checksum": (
            "a9dc10e8301304e41f297857d5c5b997b679eb3db0357852b2c0e0c2b0f82af9"
        ),
        "compatible_recorded_checksums": {
            "5f8a20155caccb9ab729f2b47050b51d38b59bce03ec1c0290c4fd0fdc1f450d"
        },
        "reason": (
            "audited edit for schema-interval-narrow-split: the bare ALTER "
            "TABLE IF EXISTS form errored on the post-0020 narrow shape "
            "(raw_attrs column absent); the DO-block variant applies the "
            "identical DROP NOT NULL / DROP DEFAULT only where the wide "
            "column exists, so durable DDL on every already-migrated "
            "deployment is unchanged"
        ),
    },
}
JOB_RUNTIME_TABLES = (
    "okg.jobs",
    "okg.publisher_leases",
    "okg.legacy_runtime_retirement_marker",
    "okg.job_definitions",
    "okg.job_dependencies",
    "okg.job_runs",
    "okg.job_chunks",
    "okg.job_items",
    "okg.job_item_frontiers",
    "okg.job_external_calls",
    "okg.job_runtime_ledger_retirement_marker",
)
JOB_RUNTIME_VIEWS = (
    "okg.v_job_runtime_reconciliation",
    "okg.v_job_queue",
)
JOB_RUNTIME_INDEXES = (
    "okg.jobs_live_dedupe_uidx",
    "okg.node_facts_workload_source_fact_idx",
    "okg.edge_facts_workload_source_fact_idx",
    "okg.edge_facts_source_pattern_idx",
)
JOB_RUNTIME_RELATIONS = (
    *JOB_RUNTIME_TABLES,
    *JOB_RUNTIME_VIEWS,
    *JOB_RUNTIME_INDEXES,
)
JOB_RUNTIME_PROCEDURES = ("okg.job_runtime_reconciliation()",)
JOB_RUNTIME_CRON_JOB_NAMES = ("okg-publish", "okg-lease-sweep")
LEGACY_GRAPH_LIVE_TABLES = ("okg.nodes_live", "okg.edges_live")
LEGACY_GRAPH_RETIREMENT_PRESERVED_RELATIONS = (
    "okg.node_facts",
    "okg.edge_facts",
    "okg.graph_generations",
    "okg.branches",
    "okg.branch_merge_requests",
    "okg.edge_fact_strandings",
    # okg.projectable_node_facts / okg.projectable_edge_facts are absent:
    # `schema-canonical-read-path` deleted them with the live complex they
    # filtered for (design §2.1).
    "okg.v_generations",
    "okg.v_nodes",
    "okg.v_edges",
    "okg.v_branch_layered_nodes",
    "okg.v_branch_layered_edges",
)

@dataclass(frozen=True)
class ManifestDatabase:
    deployment: str
    dsn: str
    database: str
    runtime_mode: str
    manifest_path: str


@dataclass(frozen=True)
class KnownMigration:
    migration_id: str
    description: str
    checksum: str
    destructive: bool
    # Package-relative path (under ``okg.substrate.db``) of the ``.down.sql``
    # inverse for this migration, or ``None`` when no safe inverse exists
    # (e.g. the whole-schema snapshot). Excluded from equality/hash so the
    # known-migration identity stays byte-identical: the byte-identical proof
    # compares only (migration_id, checksum, destructive), and ``to_dict``
    # carries the same three keys plus an explicit ``reversible`` flag.
    down_sql_path: str | None = field(default=None, compare=False)
    # Declared in migrations/manifest.yaml as `reads_safe_behind: true`: code
    # reading a database WITHOUT this migration returns the same answers (it
    # adds a table, a column no existing read selects, or an index). The
    # release verdict may then serve reads behind it instead of refusing
    # (PACT unknown-s7-release-verdict). Excluded from identity like
    # ``down_sql_path``. `reads_safe_behind_refusal` refuses the declaration
    # on SQL that could change an answer.
    reads_safe_behind: bool = field(default=False, compare=False)

    @property
    def reversible(self) -> bool:
        return self.down_sql_path is not None

    def down_sql(self) -> str:
        """Read the recorded inverse SQL, or refuse loudly when absent."""
        if self.down_sql_path is None:
            raise SubstrateError(
                f"migration {self.migration_id!r} has no reversible inverse",
                data={"migration_id": self.migration_id},
                fix_hint=(
                    "this migration cannot be rolled back automatically; "
                    "restore from a backup if you must undo it"
                ),
            )
        return importlib.resources.files("okg.substrate.db").joinpath(
            self.down_sql_path
        ).read_text()

    def to_dict(self) -> dict[str, Any]:
        return {
            "migration_id": self.migration_id,
            "description": self.description,
            "checksum": self.checksum,
            "destructive": self.destructive,
            "reversible": self.reversible,
            "reads_safe_behind": self.reads_safe_behind,
        }


def _migration_checksum_compatibility(
    migration: KnownMigration,
    recorded_checksum: str,
) -> dict[str, Any] | None:
    """Describe an audited legacy checksum match, else return ``None``."""
    compatibility = _ORDERED_MIGRATION_CHECKSUM_COMPATIBILITIES.get(
        migration.migration_id
    )
    if compatibility is None:
        return None
    if migration.checksum != compatibility["canonical_checksum"]:
        return None
    if recorded_checksum not in compatibility["compatible_recorded_checksums"]:
        return None
    return {
        "migration_id": migration.migration_id,
        "recorded_checksum": recorded_checksum,
        "expected_checksum": migration.checksum,
        "reason": compatibility["reason"],
    }


def ensure_schema_migrations(conn: psycopg.Connection) -> None:
    """Ensure the forward-migration ledger exists."""
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS okg")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS okg.schema_migrations (
              migration_id    TEXT PRIMARY KEY,
              checksum        TEXT NOT NULL,
              description     TEXT NOT NULL,
              destructive     BOOLEAN NOT NULL DEFAULT false,
              applied_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
              applied_by      TEXT NOT NULL DEFAULT current_user,
              tool_version    TEXT,
              status          TEXT NOT NULL DEFAULT 'applied'
                CHECK (status IN ('applied', 'failed', 'skipped')),
              result_metadata JSONB NOT NULL DEFAULT '{}'::jsonb
            )
            """
        )


# result_metadata key on the 0000 snapshot row that carries the checksum of
# the POST-TRANSFORM executable schema body (after
# ``schema_without_legacy_live_table_compatibility``). It is the skip key for
# the migrate fast path (make-migrate-idempotent, design ruling 1): the raw
# ``checksum`` column stays the joint schema+views hash that ``migration_status``
# consumes, while this field decides whether ``cur.execute(executable_sql)`` is
# replayed. Absence => replay (fail-closed): a legacy row written before this
# field existed cannot prove the body is current.
EXECUTABLE_SCHEMA_CHECKSUM_KEY = "executable_schema_checksum"


def executable_schema_checksum(executable_sql: str) -> str:
    """Checksum of the post-transform executable schema BODY only.

    This is the closure of "what ``cur.execute`` runs" in ``migrate`` — the
    schema text after ``schema_without_legacy_live_table_compatibility``. It
    deliberately excludes ``views.sql`` (views are always reinstalled, outside
    the body skip) so a views-only edit still skips the schema body.
    """
    return hashlib.sha256(executable_sql.encode("utf-8")).hexdigest()


def applied_executable_schema_checksum(conn: psycopg.Connection) -> str | None:
    """Return the recorded executable-schema checksum, or None when absent.

    None means the fast path MUST replay: either there is no 0000 row yet, or
    it is a legacy row that predates the field. Absence is never a skip.
    """
    with conn.cursor() as cur:
        if not _regclass_exists(cur, "okg.schema_migrations"):
            return None
        cur.execute(
            "SELECT result_metadata FROM okg.schema_migrations "
            "WHERE migration_id = %s",
            (SCHEMA_SNAPSHOT_MIGRATION_ID,),
        )
        row = cur.fetchone()
    if row is None or not isinstance(row[0], dict):
        return None
    value = row[0].get(EXECUTABLE_SCHEMA_CHECKSUM_KEY)
    return str(value) if value else None


def record_schema_snapshot(
    conn: psycopg.Connection,
    *,
    checksum: str,
    statements_executed: int,
    executable_schema_checksum: str | None = None,
) -> None:
    metadata: dict[str, Any] = {"statements_executed": statements_executed}
    if executable_schema_checksum is not None:
        metadata[EXECUTABLE_SCHEMA_CHECKSUM_KEY] = executable_schema_checksum
    record_known_migration(
        conn,
        KnownMigration(
            migration_id=SCHEMA_SNAPSHOT_MIGRATION_ID,
            description="current schema.sql and views.sql snapshot",
            checksum=checksum,
            destructive=False,
        ),
        result_metadata=metadata,
    )


# result_metadata key carrying a destructive migration's postcondition, and the
# key a caller sets when it could NOT evaluate one. Issue #1054: recording an
# unevaluated postcondition as success is what made a destructive migration's
# ledger row worthless, so "not checked" gets its own recorded shape.
POSTCONDITION_KEY = "postcondition"
POSTCONDITION_UNVERIFIED_KEY = "postcondition_unverified_reason"


def _assert_recordable_postcondition(
    migration: KnownMigration,
    result_metadata: dict[str, Any],
) -> None:
    """Refuse to record a destructive migration whose postcondition is not true.

    Five refusals, one per way issue #1054 stayed invisible:

    * **absent** -- ``0001_retire_job_runtime_ledger`` never asserted anything;
      a missing postcondition must not read as a passing one.
    * **not a flag mapping** -- a bare string cannot be re-checked later.
    * **not boolean-valued** -- the string ``"false"`` is truthy in Python and
      would be recorded as a pass.
    * **any false flag** -- the check ran and said no.
    * **undeclared flag** -- a flag with no registered re-check is not evidence.

    Non-destructive migrations are unaffected. A caller that genuinely cannot
    evaluate its postcondition uses :func:`record_unverified_migration`, which
    records ``status = 'failed'``; declaring "unverified" while claiming
    ``applied`` is exactly the "not checked read as passed" shape this refuses.
    """
    if not migration.destructive:
        return
    if POSTCONDITION_UNVERIFIED_KEY in result_metadata:
        raise SubstrateError(
            f"destructive migration {migration.migration_id!r} cannot be recorded "
            f"applied while declaring its postcondition unverified",
            data={
                "migration_id": migration.migration_id,
                "reason": result_metadata[POSTCONDITION_UNVERIFIED_KEY],
            },
            fix_hint=(
                "use record_unverified_migration, which records status='failed'; "
                "an unverified postcondition must never be stored as applied"
            ),
        )
    postcondition = result_metadata.get(POSTCONDITION_KEY)
    if postcondition is None:
        raise SubstrateError(
            f"destructive migration {migration.migration_id!r} recorded no "
            f"postcondition",
            data={"migration_id": migration.migration_id},
            fix_hint=(
                "evaluate the migration's postcondition flags and pass them as "
                "result_metadata['postcondition'], or record it unverified with "
                "an explicit reason"
            ),
        )
    if not isinstance(postcondition, Mapping):
        raise SubstrateError(
            f"destructive migration {migration.migration_id!r} recorded a "
            f"postcondition that is not a flag mapping",
            data={
                "migration_id": migration.migration_id,
                "postcondition": postcondition,
            },
            fix_hint=(
                "record a mapping of declared flag name to boolean so the claim "
                "can be re-verified against the database later"
            ),
        )
    mistyped = sorted(
        str(key)
        for key, value in postcondition.items()
        if not isinstance(value, bool)
    )
    if mistyped:
        raise SubstrateError(
            f"destructive migration {migration.migration_id!r} recorded "
            f"non-boolean postcondition values",
            data={"migration_id": migration.migration_id, "flags": mistyped},
            fix_hint=(
                "record real booleans; a truthy string such as \"false\" would "
                "be stored as a passing flag"
            ),
        )
    failed = sorted(str(key) for key, value in postcondition.items() if not value)
    if failed:
        raise SubstrateError(
            f"destructive migration {migration.migration_id!r} postcondition failed",
            data={
                "migration_id": migration.migration_id,
                "failed_flags": failed,
                "postcondition": dict(postcondition),
            },
            fix_hint=(
                "the migration did not achieve its declared end state; "
                "investigate the named flags instead of recording success"
            ),
        )
    unknown = sorted(
        str(key) for key in postcondition if str(key) not in POSTCONDITION_FLAGS
    )
    if unknown:
        raise SubstrateError(
            f"destructive migration {migration.migration_id!r} recorded "
            f"undeclared postcondition flags",
            data={"migration_id": migration.migration_id, "unknown_flags": unknown},
            fix_hint=(
                "declare each flag in okg.substrate.db.migration_postcondition."
                "POSTCONDITION_FLAGS so migration status can re-check it"
            ),
        )


def record_unverified_migration(
    conn: psycopg.Connection,
    migration: KnownMigration,
    *,
    reason: str,
    result_metadata: dict[str, Any] | None = None,
) -> None:
    """Record a migration whose postcondition could NOT be evaluated.

    Writes ``status = 'failed'`` with the reason. "Could not check" is stored
    as its own outcome so it is never mistaken for "checked and passed".

    This also DOWNGRADES an existing ``applied`` row. Correcting a row that
    claims an end state the database contradicts is the whole subject of issue
    #1054, so it must not be a no-op just because the migration id is already
    recorded.
    """
    if not reason.strip():
        raise SubstrateError(
            "recording a migration unverified requires a reason",
            data={"migration_id": migration.migration_id},
            fix_hint="state what prevented the postcondition from being evaluated",
        )
    metadata = dict(result_metadata or {})
    metadata[POSTCONDITION_UNVERIFIED_KEY] = reason
    record_known_migration(
        conn,
        migration,
        result_metadata=metadata,
        status="failed",
        overwrite_existing=True,
    )


def record_known_migration(
    conn: psycopg.Connection,
    migration: KnownMigration,
    *,
    result_metadata: dict[str, Any] | None = None,
    status: str = "applied",
    overwrite_existing: bool = False,
) -> None:
    metadata = dict(result_metadata or {})
    if status == "applied":
        _assert_recordable_postcondition(migration, metadata)
    ensure_schema_migrations(conn)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT checksum FROM okg.schema_migrations WHERE migration_id = %s",
            (migration.migration_id,),
        )
        existing = cur.fetchone()
        if (
            existing is not None
            and not overwrite_existing
            and migration.migration_id != SCHEMA_SNAPSHOT_MIGRATION_ID
        ):
            recorded_checksum = str(existing[0])
            if (
                recorded_checksum != migration.checksum
                and _migration_checksum_compatibility(
                    migration, recorded_checksum
                ) is None
            ):
                raise SubstrateError(
                    f"migration checksum changed for {migration.migration_id!r}",
                    data={
                        "migration_id": migration.migration_id,
                        "recorded_checksum": recorded_checksum,
                        "current_checksum": migration.checksum,
                    },
                    fix_hint=(
                        "never edit an applied migration; add a new ordered migration"
                    ),
                )
            return
        cur.execute(
            """
            INSERT INTO okg.schema_migrations (
              migration_id, checksum, description, destructive,
              applied_at, applied_by, status, result_metadata
            )
            VALUES (
              %s, %s, %s, %s, now(), current_user, %s, %s
            )
            ON CONFLICT (migration_id) DO UPDATE SET
              -- Keep the RECORDED checksum when a row is being downgraded to
              -- unverified: overwriting it would erase checksum-drift evidence
              -- as a side effect of correcting a postcondition.
              checksum = CASE
                WHEN EXCLUDED.status = 'failed' THEN okg.schema_migrations.checksum
                ELSE EXCLUDED.checksum
              END,
              description = EXCLUDED.description,
              destructive = EXCLUDED.destructive,
              applied_at = EXCLUDED.applied_at,
              applied_by = EXCLUDED.applied_by,
              status = EXCLUDED.status,
              result_metadata = EXCLUDED.result_metadata
            """,
            (
                migration.migration_id,
                migration.checksum,
                migration.description,
                migration.destructive,
                status,
                Jsonb(metadata),
            ),
        )


def schema_snapshot_checksum(schema_sql: str, views_sql: str) -> str:
    return hashlib.sha256(
        (schema_sql + "\n-- views.sql\n" + views_sql).encode("utf-8")
    ).hexdigest()


def job_runtime_cron_schedules(cur: psycopg.Cursor) -> list[str]:
    """Return obsolete pg_cron schedules targeting the current database."""
    cur.execute("SELECT to_regclass('cron.job') AS cron_job")
    row = cur.fetchone()
    cron_job = row.get("cron_job") if isinstance(row, dict) else row[0]
    if cron_job is None:
        return []
    cur.execute(
        """
        SELECT DISTINCT jobname
          FROM cron.job
         WHERE jobname = ANY(%s)
           AND database = current_database()
         ORDER BY jobname
        """,
        (list(JOB_RUNTIME_CRON_JOB_NAMES),),
    )
    return [
        str(row.get("jobname") if isinstance(row, dict) else row[0])
        for row in cur.fetchall()
    ]


def job_runtime_subsystem_present(conn: psycopg.Connection) -> bool:
    """Return whether any obsolete job-runtime object or schedule remains."""
    with conn.cursor() as cur:
        for relation in JOB_RUNTIME_RELATIONS:
            cur.execute("SELECT to_regclass(%s)", (relation,))
            if cur.fetchone()[0] is not None:
                return True
        for procedure in JOB_RUNTIME_PROCEDURES:
            cur.execute("SELECT to_regprocedure(%s)", (procedure,))
            if cur.fetchone()[0] is not None:
                return True
        if job_runtime_cron_schedules(cur):
            return True
    return False


def current_known_migrations(
    *,
    schema_sql: str | None = None,
    views_sql: str | None = None,
) -> list[KnownMigration]:
    if schema_sql is None:
        schema_sql = importlib.resources.files("okg.substrate.db").joinpath(
            "schema.sql"
        ).read_text()
    if views_sql is None:
        views_sql = importlib.resources.files("okg.substrate.db").joinpath(
            "views.sql"
        ).read_text()
    migrations = [
        # The whole-schema snapshot has NO safe inverse — dropping the
        # entire schema is never the right rollback — so ``down_sql_path``
        # is None and a migrate-down request against it refuses loudly.
        KnownMigration(
            migration_id=SCHEMA_SNAPSHOT_MIGRATION_ID,
            description="current schema.sql and views.sql snapshot",
            checksum=schema_snapshot_checksum(schema_sql, views_sql),
            destructive=False,
            down_sql_path=None,
        ),
    ]
    # Ordered convention for FUTURE migrations: each lives as
    # ``migrations/NNNN_<slug>.sql`` paired with ``NNNN_<slug>.down.sql``.
    # They enumerate in NNNN order after the schema snapshot above; their
    # checksum is the sha256 of the forward file, their inverse is the
    # paired ``.down.sql`` (None when the pair is absent). This block adds
    # nothing while no NNNN-prefixed file exists.
    migrations.extend(_enumerate_ordered_migrations())
    return migrations


_ORDERED_MIGRATION_RE = re.compile(r"^(\d{4})_(.+)\.sql$")

# Filename slug prefixes that HISTORICALLY classified a migration as
# destructive. Per `schema-migration-hardening` (audit F4) this heuristic is
# no longer the classification authority — migrations/manifest.yaml is — but
# it stays as a cross-check: the apply-time validator refuses when the two
# disagree, so a mis-edited manifest or a renamed file cannot silently flip a
# destructive migration onto the auto-run path.
_DESTRUCTIVE_SLUG_PREFIXES = ("retire_", "drop_", "remove_")


def load_migrations_manifest() -> dict[str, dict[str, Any]]:
    """Parse ``migrations/manifest.yaml`` into ``{migration_id: entry}``.

    The manifest is the apply-time authority for ordered migrations
    (`schema-migration-hardening` / req.manifest-authority): destructive
    classification, expected file checksum, and prerequisites. Fail-loud:
    a missing or unparseable manifest raises — an unreadable authority must
    never degrade to the filename heuristic.
    """
    from okg.substrate import fastyaml

    text = (
        importlib.resources.files("okg.substrate.db")
        .joinpath("migrations/manifest.yaml")
        .read_text()
    )
    raw = fastyaml.safe_load(text) or {}
    entries: dict[str, dict[str, Any]] = {}
    for entry in raw.get("migrations") or ():
        if not isinstance(entry, dict):
            raise SubstrateError(
                "migrations/manifest.yaml entry is not a mapping",
                data={"entry": str(entry)},
                fix_hint="fix the malformed manifest entry",
            )
        migration_id = str(entry.get("migration_id") or "")
        if not migration_id:
            raise SubstrateError(
                "migrations/manifest.yaml entry missing migration_id",
                data={"entry_keys": sorted(entry.keys())},
                fix_hint="every manifest entry must declare migration_id",
            )
        entries[migration_id] = entry
    return entries


def _filename_heuristic_destructive(migration_id: str) -> bool:
    """The legacy slug-prefix classification, kept only as a cross-check."""
    match = re.match(r"^\d{4}_(.+)$", migration_id)
    slug = match.group(1) if match else migration_id
    return slug.startswith(_DESTRUCTIVE_SLUG_PREFIXES)


# The machine-checkable prerequisite form: ``migration <id> is applied``.
# Every other prerequisite string in the manifest is operator prose (relation
# existence, drained queues, explicit confirmations) that no ledger query can
# evaluate; those stay informational.
_PREREQ_MIGRATION_APPLIED_RE = re.compile(
    r"^migration\s+(\S+)\s+is applied$"
)


def validate_pending_against_manifest(
    pending: list[KnownMigration],
    *,
    manifest: dict[str, dict[str, Any]] | None = None,
    applied_ids: set[str] | None = None,
    forward_sql_loader: Any = None,
) -> None:
    """Refuse, BEFORE any execution, when a pending ordered migration
    violates its migrations/manifest.yaml contract.

    Typed refusals (`schema-migration-hardening` / req.manifest-authority
    + req.multiphase-class):

    * :class:`MigrationManifestEntryMissing` — no manifest entry for the file.
    * :class:`MigrationManifestChecksumMismatch` — file sha256 disagrees with
      the entry's ``checksum_sha256``.
    * :class:`MigrationManifestDestructiveDisagreement` — the entry's
      ``destructive`` flag disagrees with the filename heuristic.
    * :class:`MigrationClassInvalid` — the entry declares an unrecognized
      ``class``, or a destructive multi-phase combination.
    * :class:`MigrationClassUndeclared` — the file is a multi-statement
      data migration (two or more top-level INSERT/UPDATE/DELETE/MERGE
      statements) but the entry does not declare ``class: multi_phase``.
      The single-commit contract covers DDL and bounded seeds only;
      batched data motion must declare itself.
    * :class:`MigrationManifestPrerequisiteUnmet` — a declared
      ``migration <id> is applied`` prerequisite is neither in the ledger
      (``applied_ids``) nor scheduled to run earlier — non-destructively,
      single-transaction — in this same ordered batch. Prose
      prerequisites are not evaluated.

    ``forward_sql_loader(migration_id) -> str`` supplies the forward SQL
    for the data-motion classifier; it defaults to reading the shipped
    ``migrations/NNNN_<slug>.sql`` file. Hermetic tests with synthetic
    pending migrations inject their own loader.
    """
    if manifest is None:
        manifest = load_migrations_manifest()
    if forward_sql_loader is None:
        forward_sql_loader = _ordered_migration_forward_sql
    applied_ids = set(applied_ids or ())
    # Non-destructive, single-transaction pending migrations run in list
    # (NNNN) order, so an earlier batch member satisfies a later member's
    # prerequisite. A destructive pending member does NOT (the runner skips
    # it). A declared multi_phase member satisfies prerequisites of later
    # NON-destructive ordinals only: the runner's ordering barrier defers
    # those behind it and migrate()'s orchestration runs the driver first —
    # but a destructive migration routes through its confirm-gated adapter,
    # a path with no barrier, so it may not lean on a driver run that has
    # not happened yet (phase-interval-narrow-split-migration).
    scheduled_before: set[str] = set()
    scheduled_multiphase: set[str] = set()
    for migration in pending:
        entry = manifest.get(migration.migration_id)
        if entry is None:
            raise MigrationManifestEntryMissing(
                "ordered migration "
                f"{migration.migration_id!r} has no migrations/manifest.yaml "
                "entry; refusing to apply an unlisted migration",
                data={"migration_id": migration.migration_id},
                fix_hint=(
                    "add the manifest entry (checksum_sha256, destructive, "
                    "prerequisites) before shipping the migration"
                ),
            )
        manifest_checksum = str(entry.get("checksum_sha256") or "")
        if manifest_checksum != migration.checksum:
            raise MigrationManifestChecksumMismatch(
                "ordered migration "
                f"{migration.migration_id!r} file checksum does not match its "
                "migrations/manifest.yaml checksum_sha256",
                data={
                    "migration_id": migration.migration_id,
                    "file_checksum": migration.checksum,
                    "manifest_checksum": manifest_checksum,
                },
                fix_hint=(
                    "never edit a shipped migration; add a new ordered "
                    "migration and update the manifest together"
                ),
            )
        heuristic = _filename_heuristic_destructive(migration.migration_id)
        manifest_destructive = bool(entry.get("destructive"))
        if manifest_destructive != heuristic:
            raise MigrationManifestDestructiveDisagreement(
                "ordered migration "
                f"{migration.migration_id!r}: manifest destructive="
                f"{manifest_destructive} disagrees with the filename "
                f"heuristic ({heuristic})",
                data={
                    "migration_id": migration.migration_id,
                    "manifest_destructive": manifest_destructive,
                    "heuristic_destructive": heuristic,
                },
                fix_hint=(
                    "reconcile the manifest entry and the filename; the "
                    "manifest is the classification authority"
                ),
            )
        declared_class = declared_migration_class(
            migration.migration_id, entry
        )
        # Destructive migrations are exempt from the data-motion check:
        # they never run inline (the runner routes them to confirm-gated
        # adapters that own their own transaction shape), and multi_phase
        # + destructive is a refused contradiction — so requiring the
        # declaration here would leave a destructive multi-DML migration
        # with no legal manifest at all.
        if (
            declared_class != MIGRATION_CLASS_MULTI_PHASE
            and not manifest_destructive
        ):
            counts = is_undeclared_data_migration(
                forward_sql_loader(migration.migration_id)
            )
            if counts is not None:
                raise MigrationClassUndeclared(
                    "ordered migration "
                    f"{migration.migration_id!r} contains "
                    f"{counts['data_motion_statements']} top-level "
                    "data-motion statements but does not declare "
                    "`class: multi_phase`; refusing to run batched data "
                    "motion inside migrate's single transaction",
                    data={
                        "migration_id": migration.migration_id,
                        **counts,
                    },
                    fix_hint=(
                        "declare `class: multi_phase` in the manifest "
                        "entry and ship a phase spec, or restructure as "
                        "a single-transaction migration"
                    ),
                )
        for prerequisite in entry.get("prerequisites") or ():
            match = _PREREQ_MIGRATION_APPLIED_RE.match(
                str(prerequisite).strip()
            )
            if match is None:
                continue  # prose prerequisite: informational, not evaluable
            prerequisite_id = match.group(1)
            if (
                prerequisite_id in applied_ids
                or prerequisite_id in scheduled_before
                or (
                    not manifest_destructive
                    and prerequisite_id in scheduled_multiphase
                )
            ):
                continue
            raise MigrationManifestPrerequisiteUnmet(
                "ordered migration "
                f"{migration.migration_id!r} declares prerequisite "
                f"{prerequisite!r}, but {prerequisite_id!r} is neither "
                "applied nor scheduled earlier in this batch",
                data={
                    "migration_id": migration.migration_id,
                    "prerequisite_id": prerequisite_id,
                    "prerequisite": str(prerequisite),
                },
                fix_hint=(
                    f"apply {prerequisite_id} first, or reconcile the "
                    "manifest prerequisites"
                ),
            )
        if not manifest_destructive:
            if declared_class == MIGRATION_CLASS_MULTI_PHASE:
                # Counts as scheduled for later NON-destructive ordinals
                # only: the barrier + orchestration guarantee the driver
                # runs before they execute (see the comment above).
                scheduled_multiphase.add(migration.migration_id)
            else:
                scheduled_before.add(migration.migration_id)


# The statements a `reads_safe_behind` migration may contain: an ALLOW-list.
# Each adds something no existing read selects (a table, an index, a new
# column, a comment, a grant) or only frames the transaction. Anything else
# -- UPDATE, DELETE, INSERT, MERGE, TRUNCATE, DROP, a redefined view or
# function, ALTER COLUMN, a policy, a DO block -- may change what an existing
# read returns, and reads behind it would give old or partial answers with no
# error: the silent stand-in `unknown-as-a-value` removes. A deny-list would
# let the next unlisted form through; this list fails closed.
_READS_SAFE_STATEMENT_RES = (
    re.compile(r"CREATE\s+TABLE\b", re.IGNORECASE),
    re.compile(r"CREATE\s+INDEX\b", re.IGNORECASE),
    re.compile(r"COMMENT\s+ON\b", re.IGNORECASE),
    re.compile(r"GRANT\b", re.IGNORECASE),
    re.compile(r"SET\b", re.IGNORECASE),
    re.compile(r"(BEGIN|COMMIT)\b", re.IGNORECASE),
)
# `ALTER TABLE <name>` followed only by comma-separated `ADD COLUMN` clauses.
# The name may be masked to spaces and dots when it is double-quoted.
_ALTER_TABLE_HEAD_RE = re.compile(
    r"ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:ONLY\s+)?[\w.\s]*?\s(ADD\b.*)\Z",
    re.IGNORECASE | re.DOTALL,
)
_ADD_COLUMN_RE = re.compile(r"ADD\s+COLUMN\b", re.IGNORECASE)


def _split_top_level_commas(text: str) -> list[str]:
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    for ch in text:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(current))
            current = []
            continue
        current.append(ch)
    parts.append("".join(current))
    return parts


def _is_reads_safe_statement(masked: str) -> bool:
    if any(pattern.match(masked) for pattern in _READS_SAFE_STATEMENT_RES):
        return True
    head = _ALTER_TABLE_HEAD_RE.match(masked)
    if head is None:
        return False
    return all(
        _ADD_COLUMN_RE.match(clause.strip())
        for clause in _split_top_level_commas(head.group(1))
    )


def reads_safe_behind_refusal(sql_text: str) -> list[str]:
    """The statements in ``sql_text`` that forbid `reads_safe_behind`.

    Empty means the declaration is allowed. Each statement is read with its
    comments, strings, quoted identifiers and dollar-quoted bodies blanked,
    and must start with an allowed form; a refused statement is reported by
    its first 80 characters with comments and quoted text blanked.
    """
    from okg.substrate.db.migration_class import (
        _mask_quoted_regions,
        top_level_statements,
    )

    refused: list[str] = []
    for statement in top_level_statements(sql_text):
        masked = " ".join(_mask_quoted_regions(statement).split())
        if not masked:
            continue  # comments only
        if not _is_reads_safe_statement(masked):
            refused.append(masked[:80])
    return refused


def _declared_reads_safe_behind(
    migration_id: str, manifest_entry: dict[str, Any] | None, forward_sql: str,
) -> bool:
    """Read and check one manifest entry's `reads_safe_behind` declaration.

    Refuses loudly -- never downgrades to False -- on a non-boolean value or
    on SQL that could change a read's answer, because a declaration the
    release verdict trusts must be one the code has checked.
    """
    if manifest_entry is None or "reads_safe_behind" not in manifest_entry:
        return False
    declared = manifest_entry["reads_safe_behind"]
    if not isinstance(declared, bool):
        raise SubstrateError(
            f"migrations/manifest.yaml entry {migration_id!r} declares "
            f"reads_safe_behind={declared!r}; it must be true or false",
            data={"migration_id": migration_id, "reads_safe_behind": declared},
            fix_hint="write `reads_safe_behind: true` or remove the key",
        )
    if not declared:
        return False
    found = reads_safe_behind_refusal(forward_sql)
    if found:
        raise SubstrateError(
            f"migration {migration_id!r} declares reads_safe_behind, but "
            f"{len(found)} of its statements are not on the reads-safe list "
            "(CREATE TABLE, CREATE INDEX, ALTER TABLE ... ADD COLUMN, COMMENT "
            "ON, GRANT, SET, BEGIN, COMMIT) and may change what an existing "
            f"read returns: {found[0]!r}",
            data={"migration_id": migration_id, "forbidden": found},
            fix_hint=(
                "remove `reads_safe_behind: true` from the manifest entry; "
                "a database behind this migration must refuse, not serve"
            ),
        )
    return True


def _enumerate_ordered_migrations() -> list[KnownMigration]:
    """Enumerate ``migrations/NNNN_<slug>.sql`` forward files in NNNN order.

    The schema snapshot already owns the ``0000`` ordinal as a special
    rolling-checksum entry, so an on-disk ``0000_*.sql`` forward file (if any)
    is skipped here to avoid a duplicate ``0000`` identity. Only ``0001`` and
    higher are seeded as ordered, file-backed migrations.

    Destructive classification comes FROM migrations/manifest.yaml when the
    entry exists (`schema-migration-hardening`); the filename heuristic is
    only the fallback for an unlisted file — which the apply-time validator
    refuses to run anyway — so status surfaces still classify it sanely.
    """
    migrations_dir = importlib.resources.files("okg.substrate.db").joinpath(
        "migrations"
    )
    manifest = load_migrations_manifest()
    discovered: list[tuple[str, str, str]] = []
    for entry in migrations_dir.iterdir():
        name = entry.name
        match = _ORDERED_MIGRATION_RE.match(name)
        if match is None or name.endswith(".down.sql"):
            continue
        ordinal, slug = match.group(1), match.group(2)
        if ordinal == "0000":
            # Reserved for the schema snapshot's special rolling checksum.
            continue
        discovered.append((ordinal, slug, name))
    out: list[KnownMigration] = []
    for ordinal, slug, name in sorted(discovered):
        forward = migrations_dir.joinpath(name).read_text()
        down_name = name[: -len(".sql")] + ".down.sql"
        has_down = migrations_dir.joinpath(down_name).is_file()
        migration_id = f"{ordinal}_{slug}"
        manifest_entry = manifest.get(migration_id)
        if manifest_entry is not None:
            destructive = bool(manifest_entry.get("destructive"))
        else:
            destructive = slug.startswith(_DESTRUCTIVE_SLUG_PREFIXES)
        out.append(
            KnownMigration(
                migration_id=migration_id,
                description=f"ordered migration {ordinal}_{slug}",
                checksum=hashlib.sha256(forward.encode("utf-8")).hexdigest(),
                destructive=destructive,
                down_sql_path=(
                    f"migrations/{down_name}" if has_down else None
                ),
                reads_safe_behind=_declared_reads_safe_behind(
                    migration_id, manifest_entry, forward
                ),
            )
        )
    return out


# Destructive ordered migrations are NEVER auto-run by the generic runner: each
# has a confirm-gated, reviewed adapter that owns its data-loss semantics. The
# runner reports the pending destructive migration with the adapter verb as a
# fix_hint instead of executing it. (make-migrate-idempotent, design ruling 2.)
DESTRUCTIVE_MIGRATION_ADAPTERS: dict[str, str] = {
    REMOVE_JOB_RUNTIME_SUBSYSTEM_MIGRATION_ID: (
        "okg db remove-job-runtime-subsystem"
    ),
}


def _ordered_migration_forward_sql(migration_id: str) -> str:
    """Read the forward SQL for an ordered ``NNNN_<slug>`` migration."""
    return (
        importlib.resources.files("okg.substrate.db")
        .joinpath("migrations")
        .joinpath(f"{migration_id}.sql")
        .read_text()
    )


_ORDERED_MIGRATION_SESSION_SETTINGS = (
    "lock_timeout",
    "statement_timeout",
)


def _execute_ordered_migration_sql(
    conn: psycopg.Connection,
    forward_sql: str,
) -> None:
    """Execute historical SQL without leaking its session settings.

    Ordered migration files are immutable once applied.  Migration 0003
    predates that rule's enforcement and uses session-level ``SET`` statements;
    changing them to ``SET LOCAL`` changed its checksum and correctly wedged
    upgrades on databases that had already recorded the original bytes.  Keep
    the historical file byte-identical and contain those settings at the runner
    boundary instead.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT "
            + ", ".join(
                f"current_setting('{name}')"
                for name in _ORDERED_MIGRATION_SESSION_SETTINGS
            )
        )
        previous_settings = tuple(str(value) for value in cur.fetchone())
        cur.execute(forward_sql)
        for name, value in zip(
            _ORDERED_MIGRATION_SESSION_SETTINGS,
            previous_settings,
            strict=True,
        ):
            cur.execute("SELECT set_config(%s, %s, false)", (name, value))


def run_pending_ordered_migrations(
    conn: psycopg.Connection,
    *,
    schema_sql: str,
    views_sql: str,
) -> dict[str, Any]:
    """Run every pending NON-destructive ordered migration exactly once.

    Generic runner (make-migrate-idempotent, design ruling 2). Pending =
    the known ordered migrations (``0001`` and higher; the ``0000`` snapshot is
    owned by ``record_schema_snapshot``) minus the rows already in the ledger.
    Each pending non-destructive file executes in NNNN order inside the caller's
    open transaction (``conn``) and is recorded via ``record_known_migration``
    in the SAME transaction, so effect and record commit atomically — a second
    run finds nothing pending and executes nothing.

    Refusals, before any execution:
      * an applied migration whose recorded checksum no longer matches the
        shipped file is a typed refusal (never a silent re-execute), except
        for an explicitly audited legacy/canonical checksum pair.
      * a pending migration that violates its migrations/manifest.yaml
        contract — missing entry, checksum mismatch with the manifest, or a
        destructive flag that disagrees with the filename heuristic — is a
        typed refusal (`schema-migration-hardening` /
        req.manifest-authority); the manifest is consulted at apply time.

    Destructive pending migrations are never auto-run: they route through their
    reviewed, confirm-gated adapter and are returned in ``skipped_destructive``
    with the adapter verb as a fix_hint. Destructive classification comes from
    the manifest (via ``current_known_migrations``), not the filename.

    Multi-phase pending migrations (`schema-migration-hardening` /
    req.multiphase-class) are likewise never run inline: the single-commit
    invariant covers ordinary migrations only, so a manifest entry declaring
    ``class: multi_phase`` routes the migration to the checkpointed driver
    (:func:`okg.substrate.db.multiphase.run_multiphase_migration`) and the
    runner returns it in ``pending_multiphase`` instead of executing it
    inside migrate's transaction. Ordinals BEHIND the first pending
    multi-phase migration are deferred (``deferred_post_multiphase``),
    preserving NNNN total order; migrate()'s orchestration loop runs the
    driver and then a second ordered pass that applies them.
    """
    known = current_known_migrations(schema_sql=schema_sql, views_sql=views_sql)
    ordered = [
        migration
        for migration in known
        if migration.migration_id != SCHEMA_SNAPSHOT_MIGRATION_ID
    ]
    manifest = load_migrations_manifest()

    ensure_schema_migrations(conn)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT migration_id, checksum FROM okg.schema_migrations"
        )
        applied = {str(row[0]): str(row[1]) for row in cur.fetchall()}

    # Refuse an edited, already-applied migration BEFORE running anything.
    checksum_compatibilities: list[dict[str, Any]] = []
    for migration in ordered:
        recorded = applied.get(migration.migration_id)
        if recorded is not None and recorded != migration.checksum:
            compatibility = _migration_checksum_compatibility(
                migration, recorded
            )
            if compatibility is None:
                raise SubstrateError(
                    "ordered migration checksum changed for "
                    f"{migration.migration_id!r}",
                    data={
                        "migration_id": migration.migration_id,
                        "recorded_checksum": recorded,
                        "current_checksum": migration.checksum,
                    },
                    fix_hint=(
                        "never edit an applied migration; add a new ordered "
                        "migration file with the next NNNN ordinal"
                    ),
                )
            checksum_compatibilities.append(compatibility)

    # Manifest authority at apply time: every PENDING file must satisfy its
    # migrations/manifest.yaml entry BEFORE anything executes, so a partial
    # apply can never precede the refusal.
    pending = [
        migration
        for migration in ordered
        if migration.migration_id not in applied
    ]
    validate_pending_against_manifest(
        pending, manifest=manifest, applied_ids=set(applied)
    )

    executed: list[str] = []
    skipped_destructive: list[dict[str, Any]] = []
    pending_multiphase: list[dict[str, Any]] = []
    deferred_post_multiphase: list[str] = []
    for migration in ordered:
        if migration.migration_id in applied:
            continue
        if migration.destructive:
            skipped_destructive.append(
                {
                    "migration_id": migration.migration_id,
                    "fix_hint": DESTRUCTIVE_MIGRATION_ADAPTERS.get(
                        migration.migration_id,
                        "run the reviewed adapter for this destructive migration",
                    ),
                }
            )
            continue
        entry = manifest.get(migration.migration_id)
        if (
            entry is not None
            and declared_migration_class(migration.migration_id, entry)
            == MIGRATION_CLASS_MULTI_PHASE
        ):
            # Excluded from the single-commit invariant BY DECLARATION:
            # never executed inside migrate's transaction.
            pending_multiphase.append(
                {
                    "migration_id": migration.migration_id,
                    "fix_hint": (
                        "run the registered phase spec through "
                        "okg.substrate.db.multiphase."
                        "run_multiphase_migration (checkpointed, "
                        "resumable batches)"
                    ),
                }
            )
            continue
        if pending_multiphase:
            # Ordering barrier (phase-interval-narrow-split-migration): an
            # ordinal behind a pending multi-phase migration is DEFERRED,
            # never executed out of NNNN order past unfinished data motion.
            # The migrate() orchestration runs the multi-phase driver and
            # then a second ordered pass, which applies these.
            deferred_post_multiphase.append(migration.migration_id)
            continue
        forward_sql = _ordered_migration_forward_sql(migration.migration_id)
        _execute_ordered_migration_sql(conn, forward_sql)
        record_known_migration(
            conn,
            migration,
            result_metadata={"applied_via": "ordered_runner"},
        )
        executed.append(migration.migration_id)

    return {
        "executed": executed,
        "skipped_destructive": skipped_destructive,
        "pending_multiphase": pending_multiphase,
        "deferred_post_multiphase": deferred_post_multiphase,
        "checksum_compatibilities": checksum_compatibilities,
    }


def migration_status(dsn: str) -> dict[str, Any]:
    from okg.substrate.db.multiphase import (
        multiphase_complete_unrecorded_rows,
        multiphase_in_progress_rows,
    )

    known = current_known_migrations()
    ledger_exists = False
    applied: list[dict[str, Any]] = []
    pending: list[dict[str, Any]] = []
    mismatches: list[dict[str, Any]] = []
    checksum_compatibilities: list[dict[str, Any]] = []
    multiphase_in_progress: list[dict[str, Any]] = []
    postcondition_drift: list[dict[str, Any]] = []

    with db_connect(dsn, connect_timeout=5) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            ledger_exists = _regclass_exists(cur, "okg.schema_migrations")
            if ledger_exists:
                cur.execute(
                    """
                    SELECT migration_id, checksum, description, destructive,
                           applied_at, applied_by, status, result_metadata
                      FROM okg.schema_migrations
                     ORDER BY applied_at, migration_id
                    """
                )
                applied = [_jsonify_row(row) for row in cur.fetchall()]
                postcondition_drift = _postcondition_drift_rows(cur, applied)
            multiphase_in_progress = multiphase_in_progress_rows(cur)
            multiphase_complete_unrecorded = multiphase_complete_unrecorded_rows(
                cur,
                recorded_migration_ids={
                    str(row["migration_id"]) for row in applied
                },
            )

    applied_by_id = {str(row["migration_id"]): row for row in applied}
    for migration in known:
        row = applied_by_id.get(migration.migration_id)
        if row is None:
            pending.append(migration.to_dict())
            continue
        if row.get("checksum") != migration.checksum:
            compatibility = _migration_checksum_compatibility(
                migration, str(row.get("checksum") or "")
            )
            if compatibility is not None:
                checksum_compatibilities.append(compatibility)
            else:
                mismatches.append(
                    {
                        "migration_id": migration.migration_id,
                        "recorded_checksum": row.get("checksum"),
                        "expected_checksum": migration.checksum,
                        "destructive": migration.destructive,
                    }
                )

    destructive_pending = [
        row for row in pending if bool(row.get("destructive"))
    ]
    return {
        "ledger_exists": ledger_exists,
        "applied": applied,
        "pending": pending,
        "checksum_mismatches": mismatches,
        "checksum_compatibilities": checksum_compatibilities,
        "destructive_pending": destructive_pending,
        "unknown_applied": _unknown_applied_rows(applied, known),
        # Mid-state of declared multi-phase migrations: phase rows whose
        # batches are committed but whose completion ledger row does not
        # exist yet (paused, crashed-awaiting-resume, or running).
        "multiphase_in_progress": multiphase_in_progress,
        # The one crash window in-progress rows cannot label: every phase
        # complete, ledger row not yet written. A re-run no-ops the phases
        # and records the row (phase-interval-narrow-split-migration).
        "multiphase_complete_unrecorded": multiphase_complete_unrecorded,
        # Declared multi-phase migrations the ordered runner will NOT
        # run inline and the driver has not completed.
        "multiphase_pending": _multiphase_pending_rows(pending),
        # Recorded destructive postconditions re-checked against the live
        # database (issue #1054). A recorded claim is evidence only while it
        # still holds; drift clears `ok` so a false claim cannot read as green.
        "postcondition_drift": postcondition_drift,
        "ok": (
            ledger_exists
            and not mismatches
            and not _blocking_postcondition_drift(postcondition_drift)
        ),
    }


# Drift statuses that clear `ok` -- fail closed on anything that means the
# ledger is not evidence: a recorded claim the database contradicts
# (`drifted`), a flag with no declared re-check (`unresolved`), a postcondition
# nobody can parse (`unparseable`), one that could not be evaluated
# (`unevaluable`), and a row recorded applied while declaring itself unverified
# (`unverified`) -- that last one is precisely "not checked" stored as
# "checked and passed", which is the defect, not an exemption from it.
#
# `unrecorded` is reported but does NOT clear `ok`. It is the one status that
# carries no claim at all: rows written before a destructive migration was
# required to record a postcondition. Making it blocking would turn every
# existing deployment red for a historical recording gap with no way to clear
# it, which is the failure mode this change exists to avoid. `superseded` is
# likewise non-blocking: a later destructive migration deliberately undid the
# earlier end state and carries the current claim.
BLOCKING_POSTCONDITION_DRIFT_STATUSES = frozenset(
    {"drifted", "unresolved", "unparseable", "unevaluable", "unverified"}
)


def _blocking_postcondition_drift(
    drift: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return [
        entry
        for entry in drift
        if str(entry.get("status") or "") in BLOCKING_POSTCONDITION_DRIFT_STATUSES
    ]


def _postcondition_drift_rows(
    cur: psycopg.Cursor,
    applied: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Re-check every applied destructive migration's recorded postcondition.

    Issue #1054: ``0001_retire_job_runtime_ledger`` recorded seven relation
    absences that were true only inside its own transaction -- the relations
    were re-created by the next schema apply and repopulated. The recorded
    claim was never looked at again, so a green migration status carried a
    false premise for a month. This asks the database instead of the ledger.

    Only rows recorded ``applied`` are re-checked: a row already recorded
    ``failed``/``skipped`` is not claiming anything. A row a later destructive
    migration deliberately superseded is reported as such and not re-derived --
    the superseding migration's own postcondition is the current claim.
    """
    drift: list[dict[str, Any]] = []
    applied_ids = {
        str(row.get("migration_id") or "")
        for row in applied
        if str(row.get("status") or "") == "applied"
    }
    for row in applied:
        if str(row.get("status") or "") != "applied":
            continue
        if not bool(row.get("destructive")):
            # Scope: this contract is about DESTRUCTIVE migrations, whose end
            # state is a set of relations that must be gone. Non-destructive
            # migrations record free-form result metadata under the same key
            # (0003 records shape assertions from its own vocabulary), and
            # re-deriving those through this registry would report `unresolved`
            # on rows that were never making an absence claim -- a permanent
            # red nothing can clear, which is the failure mode this change
            # exists to prevent.
            continue
        migration_id = str(row.get("migration_id") or "")
        superseding = SUPERSEDED_BY.get(migration_id)
        if superseding and superseding in applied_ids:
            drift.append(
                {
                    "migration_id": migration_id,
                    "flag": None,
                    "status": "superseded",
                    "detail": (
                        f"end state deliberately replaced by {superseding}, "
                        f"whose own postcondition is the current claim"
                    ),
                }
            )
            continue
        metadata = row.get("result_metadata")
        if not isinstance(metadata, Mapping):
            # A destructive row whose metadata is not even a mapping cannot be
            # re-checked; skipping it silently is how #1054 survived.
            drift.append(
                {
                    "migration_id": migration_id,
                    "flag": None,
                    "status": "unparseable",
                    "recorded": metadata,
                    "detail": "result_metadata is not a mapping",
                }
            )
            continue
        if metadata.get(POSTCONDITION_UNVERIFIED_KEY):
            drift.append(
                {
                    "migration_id": migration_id,
                    "flag": None,
                    "status": "unverified",
                    "detail": str(metadata[POSTCONDITION_UNVERIFIED_KEY]),
                }
            )
            continue
        if POSTCONDITION_KEY not in metadata:
            # A destructive migration can no longer be recorded without one
            # (see _assert_recordable_postcondition), but rows written before
            # that rule exist on live deployments.
            drift.append(
                {
                    "migration_id": migration_id,
                    "flag": None,
                    "status": "unrecorded",
                    "detail": (
                        "destructive migration recorded applied with no "
                        "postcondition; its end state was never proven"
                    ),
                }
            )
            continue
        drift.extend(
            reverify_recorded_postcondition(
                cur, migration_id, metadata[POSTCONDITION_KEY]
            )
        )
    return drift


def _multiphase_pending_rows(
    pending: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Pending migrations whose manifest entry declares multi_phase.

    The ordered runner never executes these inline, so `okg up` going
    green does NOT mean they ran — until the multi-phase driver records
    the completion ledger row they are surfaced here and by doctor
    (`multiphase_migration_pending`), never silently omitted. This is a
    read surface: the raw `class` value is inspected without validation
    (an invalid class is the APPLY path's typed refusal; a status
    surface must report, not crash)."""
    manifest = load_migrations_manifest()
    out: list[dict[str, Any]] = []
    for row in pending:
        migration_id = str(row.get("migration_id"))
        entry = manifest.get(migration_id)
        if entry is None:
            continue
        if str(entry.get("class") or "") == MIGRATION_CLASS_MULTI_PHASE:
            out.append(
                {
                    "migration_id": migration_id,
                    "fix_hint": (
                        "not run by okg up; run the registered phase "
                        "spec through okg.substrate.db.multiphase."
                        "run_multiphase_migration"
                    ),
                }
            )
    return out


def _unknown_applied_rows(
    applied: list[dict[str, Any]],
    known: list[KnownMigration],
) -> list[dict[str, Any]]:
    """Applied ledger rows that match no known file and no manifest entry.

    Per `schema-migration-hardening` (audit F6): a ledger row whose
    migration file was deleted (the retired ``0001`` is live evidence) was
    previously ignored by every status surface. It is surfaced here —
    ``migration_status``/doctor consumers — instead of silently dropped.
    "Known" is the union of the enumerated file-backed migrations and the
    migrations/manifest.yaml history (which includes legacy non-ordered
    migrations such as ``retire_legacy_runtime``).
    """
    known_ids = {migration.migration_id for migration in known}
    known_ids.update(load_migrations_manifest().keys())
    return [
        row
        for row in applied
        if str(row.get("migration_id")) not in known_ids
    ]


def chronos_native_graph_storage_enabled(conn: psycopg.Connection) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('okg.chronos_native_graph_storage_marker')")
        if cur.fetchone()[0] is None:
            return False
        cur.execute(
            "SELECT EXISTS (SELECT 1 FROM okg.chronos_native_graph_storage_marker)"
        )
        return bool(cur.fetchone()[0])


# The Python schema/view rewriters that used to live here are GONE
# (`schema-canonical-read-path`).
#
# `schema_without_legacy_graph_storage`,
# `schema_without_legacy_live_table_compatibility`,
# `_CHRONOS_GRAPH_READ_VIEW_SQL` and `views_without_legacy_graph_storage`
# rewrote schema.sql and views.sql on the way to the database: the canonical
# files described the pre-Chronos live-table architecture, and these functions
# spliced the real Chronos-native one over it at migrate time. Anyone reading
# the canonical SQL therefore read a read path that served zero rows on every
# supported deployment.
#
# schema.sql and views.sql now contain the definitions that get installed, so
# there is nothing left to rewrite. `migrate()` executes both files as written.


# --------------------------------------------------------------------------- #
# Honest reads for Chronos-registered maintained-derivation kept tables (#706) #
# --------------------------------------------------------------------------- #
#
# okg.require_graph_checkout (above) guards ONLY okg.graph_nodes /
# okg.graph_edges. The maintained-derivation kept tables
# (okg.claim_effective, okg.claim_projection, and any future
# GenericChronosKeptTable) are registered on the SAME Chronos branch-table
# path but had no guard, no honest-read view, and no doctor check: their base
# rows are schema templates that are never materialized in place (the rows
# live in Chronos interval storage), so a bare `SELECT ... FROM
# okg.claim_effective` returned a confident 0 while a checkout served 81,165
# rows (#706, the wrong-lens trap).
#
# The fix is registry-shaped, not a third hardcoded case:
#   * okg.require_chronos_checkout(kept_table) raises on any
#     empty-but-Chronos-registered kept table, deriving the guarded set from
#     public._chronos_branch_tables rather than a literal table list;
#   * every GenericChronosKeptTable registration installs an
#     okg.v_current_<table> honest-read view that rides a constant arm so the
#     guard is evaluated even on an empty base scan;
#   * honest_read_reconciliation_report() reconciles each registered kept
#     table's bare base count against a Chronos checkout read for the doctor.

CHRONOS_KEPT_CHECKOUT_GUARD_FN_SQL = """
CREATE OR REPLACE FUNCTION okg.require_chronos_checkout(kept_table text)
RETURNS boolean
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $okg_require_chronos_checkout$
DECLARE
  has_rows boolean;
  chronos_registered boolean;
BEGIN
  -- Registry-driven, not a hardcoded table allow-list: a kept table is
  -- guarded iff it is registered in public._chronos_branch_tables. The
  -- registration check runs FIRST so the SECURITY DEFINER dynamic-SQL probe
  -- below only ever targets a Chronos-registered relation.
  IF to_regclass(kept_table) IS NULL THEN
    RETURN false;
  END IF;
  IF to_regclass('public._chronos_branch_tables') IS NULL THEN
    RETURN false;
  END IF;
  SELECT EXISTS (
    SELECT 1 FROM public._chronos_branch_tables t
     WHERE t.table_name = kept_table
  ) INTO chronos_registered;
  IF NOT chronos_registered THEN
    -- Plain storage: an empty bare read is honest (empty table = empty result).
    RETURN false;
  END IF;
  -- String concatenation (not the format() percent-placeholder helper) is
  -- deliberate: this DDL is executed both through psycopg cur.execute
  -- (migration) and the Chronos db adapter, and the latter percent-
  -- interpolates the statement -- a percent-placeholder anywhere in the body,
  -- even in a comment, is mis-read as a bind placeholder. kept_table is
  -- registry-derived (checked registered above), not caller free-text.
  EXECUTE 'SELECT EXISTS (SELECT 1 FROM ' || kept_table || ')'
     INTO has_rows;
  IF has_rows THEN
    -- Someone materialized rows into the base table: a bare read is honest.
    RETURN false;
  END IF;
  RAISE EXCEPTION USING
    MESSAGE = 'okg kept-table read requires a Chronos checkout: '
      || kept_table
      || ' is a Chronos-registered maintained-derivation surface that is '
      || 'never materialized for bare SQL sessions, so this read would '
      || 'report an empty result even when published generations exist',
    HINT = 'Read maintained-derivation kept tables through a Chronos '
           'checkout: ChronosGraphStore.checkout_generation() in Python, or '
           'the MCP operators, which rewrite reads to a pinned checkout. '
           'Check live counts with `okg --json status --deployment <name>`, '
           'not bare psql.',
    ERRCODE = 'object_not_in_prerequisite_state';
END
$okg_require_chronos_checkout$;
REVOKE ALL ON FUNCTION okg.require_chronos_checkout(text) FROM PUBLIC;
DO $okg_require_chronos_checkout_acl$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'okg_mcp') THEN
    GRANT EXECUTE ON FUNCTION okg.require_chronos_checkout(text) TO okg_mcp;
  END IF;
END
$okg_require_chronos_checkout_acl$;
"""


def honest_read_view_name(table: str) -> str:
    """``okg.claim_effective`` -> ``okg.v_current_claim_effective``.

    The honest-read view name is derived from the kept table name so the
    doctor reconciliation can discover the governed set from the Chronos
    registry alone (no separate hand-maintained mapping).
    """
    schema, _, base = table.partition(".")
    if not base:
        schema, base = "okg", schema
    return f"{schema}.v_current_{base}"


def honest_read_view_sql(table: str) -> str:
    """CREATE OR REPLACE VIEW SQL for a kept table's honest-read surface.

    The view UNIONs the base table with a single constant all-NULL row of the
    table's composite type, gated by ``okg.require_chronos_checkout``. The
    constant arm exists so the guard is evaluated even when the base scan
    yields zero rows -- a bare ``WHERE guard()`` on the base table alone would
    never run on an empty (never-materialized) table. When the base has rows,
    or the table is not Chronos-registered, the guard returns false and the
    view is transparent; when the base is empty-but-registered, the guard
    raises instead of serving a deceptive zero.
    """
    view = honest_read_view_name(table)
    return (
        f"CREATE OR REPLACE VIEW {view} AS\n"
        f"SELECT t.* FROM {table} t\n"
        f"UNION ALL\n"
        f"SELECT (NULL::{table}).*\n"
        f"WHERE okg.require_chronos_checkout('{table}');"
    )


def ensure_kept_table_honest_read_view(db: Any, table: str) -> str:
    """Idempotently install the guard function + ``v_current_<table>`` view.

    Called from ``GenericChronosKeptTable.ensure_registered`` so every kept
    table registered on the Chronos branch-table path automatically gains an
    honest-read surface. The expensive DDL runs only when the view is missing;
    the caller's connection is committed so the view survives the transaction.
    """
    view = honest_read_view_name(table)
    # Install the guard function once: it is a single shared object, and a
    # CREATE OR REPLACE FUNCTION racing the OTHER kept table's registration on
    # the same pg_proc tuple raises "tuple concurrently updated".
    fn_row = db.execute(
        "SELECT to_regprocedure('okg.require_chronos_checkout(text)') "
        "IS NOT NULL AS present"
    ).fetchone()
    if not (fn_row and fn_row["present"]):
        db.execute(CHRONOS_KEPT_CHECKOUT_GUARD_FN_SQL)
    # Always refresh the view (self-healing): a drifted or replaced honest-read
    # view is repaired to the refusing definition on the next registration,
    # rather than silently persisting a non-refusing surface.
    db.execute(honest_read_view_sql(table))
    # app_ro default-deny invariant (asserted by
    # tests/sql/roles/{test_app_ro_default_deny.py,app_ro_read_only.sql}): this
    # view MUST be app_ro-readable so MCP run_sql (downgraded to app_ro)
    # reaches the honest RAISE, not permission-denied.
    #
    # REGISTERED, then granted (`schema-grant-coherence`). The grant alone is
    # not enough and never was: views.sql's schema-wide
    # `REVOKE ALL ON ALL TABLES IN SCHEMA okg FROM app_ro` strips it on the next
    # migrate. What used to put it back was the `v\_%` auto-grant loop -- i.e.
    # this view was readable because of what it is NAMED. The registry row is
    # the durable half: `okg.apply_grant_registry()` re-grants it after every
    # REVOKE, and `okg.grant_registry_drift()` reports it if it goes missing.
    # `source` is 'runtime' so views.sql's substrate seed, which deletes and
    # re-inserts only `source = 'substrate'`, never removes it.
    #
    # The direct GRANT stays for the current transaction: the registry table
    # may not exist yet on a database that predates migration 0027, and a
    # freshly-registered kept table has to be readable NOW, not after the next
    # migrate. No '%' anywhere -- the Chronos db adapter percent-interpolates
    # db.execute.
    #
    # The conflict target is the (relation, object_kind, role_name) partial
    # unique index, NOT the entry_ref primary key. entry_ref alone is the wrong
    # arbiter: this runs on the publish path for every kept-table registration,
    # and a row inserted under a different entry_ref for the SAME
    # (relation, kind, role) -- for instance a substrate seed row, or an
    # operator-authored deployment row -- would sail past an entry_ref check
    # and then raise unique_violation on the index, failing the publish. The
    # index predicate `WHERE role_name IS NOT NULL` must be repeated for
    # PostgreSQL to infer a PARTIAL index as the arbiter.
    db.execute(
        "DO $okg_register_vcurrent$ BEGIN "
        "IF to_regclass('okg.grant_registry') IS NOT NULL THEN "
        "INSERT INTO okg.grant_registry "
        "(entry_ref, relation, object_kind, role_name, privileges, "
        " classification, source, note) "
        "VALUES ('grant:app_ro:" + view + "', '" + view + "', 'TABLE', "
        "'app_ro', 'SELECT', 'contract', 'runtime', "
        "'honest-read surface for a Chronos-registered kept table') "
        "ON CONFLICT (relation, object_kind, role_name) "
        "WHERE role_name IS NOT NULL DO NOTHING; "
        "END IF; "
        "IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_ro') THEN "
        "EXECUTE 'GRANT SELECT ON " + view + " TO app_ro'; "
        "END IF; END $okg_register_vcurrent$;"
    )
    db.commit()
    return view


def _registered_kept_tables_with_honest_view(
    cur: psycopg.Cursor,
) -> list[tuple[str, str]]:
    """Governed (base_table, honest_view) pairs, derived from the registry.

    A kept table is governed when it is Chronos-registered AND carries a
    ``v_current_<table>`` honest-read view (the graph tables use their own
    v_nodes/v_edges guard and the rollup tables have no honest view, so both
    are excluded here without a hardcoded skip list).
    """
    cur.execute(
        "SELECT table_name FROM public._chronos_branch_tables ORDER BY table_name"
    )
    pairs: list[tuple[str, str]] = []
    for (table,) in cur.fetchall():
        view = honest_read_view_name(str(table))
        cur.execute("SELECT to_regclass(%s)", (view,))
        if cur.fetchone()[0] is not None:
            pairs.append((str(table), view))
    return pairs


def honest_read_reconciliation_report(
    dsn: str,
    *,
    deployment_name: str | None = None,
) -> dict[str, Any]:
    """Reconcile every governed kept table's bare base count against a
    Chronos checkout read (the #706 derived-vs-base reconciliation).

    For each governed ``(base_table, v_current_view)`` pair the report reads:
      * ``checkout_count`` -- the count a Chronos checkout of the latest
        published generation serves (what consumers actually see);
      * ``base_count`` -- the bare ``SELECT count(*)`` from the base table;
      * whether the honest-read view ``refused`` (raised) or returned a count.

    A finding (``ok: false``) is emitted when the base count DIVERGES from the
    checkout count -- i.e. the base is a vestigial never-materialized lens --
    yet the honest-read view failed to refuse (it returned a confident count
    instead of raising). That is exactly the #706 wrong-lens: a reader hitting
    the un-refusing surface would silently see the wrong number. When the base
    and checkout agree (materialized/consistent) or the view refuses (the
    designed healthy state), the pair reconciles.
    """
    from okg.substrate.graph_store import ChronosGraphStore

    deployment = deployment_name or "default"
    findings: list[dict[str, Any]] = []
    reconciled: list[dict[str, Any]] = []

    with db_connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            governed = _registered_kept_tables_with_honest_view(cur)

    if not governed:
        return {
            "check": "honest_reads",
            "deployment": deployment,
            "ok": True,
            "governed_tables": 0,
            "reconciled": [],
            "findings": [],
        }

    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        generation = store.latest_generation(deployment_name=deployment)
        checkout = (
            store.checkout_generation(generation.checkpoint_id)
            if generation is not None
            else None
        )
        generation_id = (
            str(generation.checkpoint_id) if generation is not None else None
        )
        for table, view in governed:
            checkout_count: int | None = None
            if checkout is not None:
                checkout_count = int(
                    checkout.query(f"SELECT count(*) AS n FROM {table}")[0]["n"]
                )
            base_count, refused, view_count = _probe_honest_surface(
                dsn, table=table, view=view
            )
            entry = {
                "table": table,
                "view": view,
                "generation_id": generation_id,
                "base_count": base_count,
                "checkout_count": checkout_count,
                "view_refused": refused,
                "view_count": view_count,
            }
            reconciled.append(entry)
            if checkout_count is None:
                # No published generation to reconcile against: nothing to
                # compare, and an empty registered base correctly refuses.
                continue
            if base_count == checkout_count:
                continue
            if not refused:
                findings.append({
                    **entry,
                    "reason": "vestigial_base_not_refusing",
                    "detail": (
                        f"{view} returned {view_count} and base {table} "
                        f"returned {base_count}, but a Chronos checkout of "
                        f"generation {generation_id} serves {checkout_count} "
                        "rows -- the honest-read view must refuse a bare read "
                        "of a never-materialized kept table, not serve a "
                        "confident count"
                    ),
                })
    finally:
        store.close()

    return {
        "check": "honest_reads",
        "deployment": deployment,
        "ok": not findings,
        "governed_tables": len(governed),
        "reconciled": reconciled,
        "findings": findings,
    }


def _probe_honest_surface(
    dsn: str,
    *,
    table: str,
    view: str,
) -> tuple[int, bool, int | None]:
    """Return ``(base_count, view_refused, view_count)`` for one kept table.

    The bare base count and the honest-view probe run on their own autocommit
    connection so a view refusal (a raised guard) cannot poison the reconciler's
    other reads.
    """
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {table}")
        base_count = int(cur.fetchone()[0])
        try:
            cur.execute(f"SELECT count(*) FROM {view}")
            return base_count, False, int(cur.fetchone()[0])
        except psycopg.errors.ObjectNotInPrerequisiteState:
            return base_count, True, None


def views_without_mcp_call_log_storage(sql_text: str) -> str:
    """Remove physical MCP call-log DDL for view-only refreshes.

    ``okg migrate --views-only`` refreshes the read-side surface on live
    databases. It must not replay Timescale storage DDL because compressed
    chunks can reject otherwise-idempotent table changes. Keep the dependent
    ``v_mcp_*`` views and grants in place.
    """
    replacement = """
-- MCP call-log storage is installed by full migrate(). View-only refreshes
-- intentionally skip physical Timescale DDL so live compressed chunks do not
-- block read-side view updates.
"""
    return _replace_between(
        sql_text,
        "-- TimescaleDB hypertable (chunk_time_interval = 7 days)",
        "-- `okg.v_mcp_calls` — agent-facing view over the log.",
        replacement,
        keep_end=True,
    )


def legacy_graph_storage_retirement_plan(conn: psycopg.Connection) -> dict[str, Any]:
    """Return the exact default legacy graph-retirement scope.

    This is the dry-run/reporting surface for the default cleanup path. It
    intentionally reports only compatibility live tables as destructive
    targets; fact logs, generation rows, branch metadata, and read/audit
    surfaces are listed separately as preserved relations.
    """
    with conn.cursor() as cur:
        targets: list[dict[str, Any]] = []
        for relation in LEGACY_GRAPH_LIVE_TABLES:
            exists = _regclass_exists(cur, relation)
            targets.append(
                {
                    "relation": relation,
                    "action": "drop_table_restrict",
                    "exists": exists,
                    "row_count": _relation_row_count(cur, relation) if exists else 0,
                }
            )

        preserved = [
            {"relation": relation, "exists": _regclass_exists(cur, relation)}
            for relation in LEGACY_GRAPH_RETIREMENT_PRESERVED_RELATIONS
        ]

    return {
        "mode": "legacy_live_table_retirement",
        "destructive_targets": targets,
        "preserved_relations": preserved,
    }


def _assert_graph_read_path_is_chronos_backed(
    conn: psycopg.Connection,
) -> None:
    """Refuse to retire the legacy tables while a view still reads them.

    This function used to REPOINT the read views itself, by executing a copy
    of the Chronos view definitions that lived in this module. The copy is
    gone (`schema-canonical-read-path`): views.sql is the only place those
    definitions exist, and ``migrate()`` installs it — through the
    frozen-view-shape convergence pass — BEFORE it calls
    :func:`retire_legacy_graph_storage`. So by the time retirement runs on
    the supported path there is nothing left to repoint.

    What remains is the precondition, checked rather than assumed. A caller
    that retires before installing views is refused by name, with the
    command that fixes it, instead of hitting a bare
    ``DependentObjectsStillExist`` from the restrictive DROP below (or, on
    an older code path, silently having its read layer rewritten by a
    definition nobody could find in the SQL files).
    """
    with conn.cursor() as cur:
        missing = [
            relation
            for relation in (
                "okg.graph_nodes",
                "okg.graph_edges",
                "public._chronos_branch_interval_checkpoints",
            )
            if not _regclass_exists(cur, relation)
        ]
        if missing:
            raise SubstrateError(
                "cannot retire legacy graph storage before Chronos graph "
                f"storage exists; missing: {', '.join(missing)}",
                data={"missing_relations": missing},
                fix_hint="run `okg migrate` first; it ensures the Chronos "
                         "graph tables before it retires anything",
            )
        cur.execute(
            """
            SELECT DISTINCT dependent_ns.nspname || '.' || dependent.relname
              FROM pg_depend
              JOIN pg_rewrite ON pg_depend.objid = pg_rewrite.oid
              JOIN pg_class dependent ON pg_rewrite.ev_class = dependent.oid
              JOIN pg_class source ON pg_depend.refobjid = source.oid
              JOIN pg_namespace dependent_ns
                ON dependent.relnamespace = dependent_ns.oid
              JOIN pg_namespace source_ns
                ON source.relnamespace = source_ns.oid
             WHERE source_ns.nspname = 'okg'
               AND source.relname = ANY(%s)
               AND dependent.oid <> source.oid
             ORDER BY 1
            """,
            ([name.split(".", 1)[1] for name in LEGACY_GRAPH_LIVE_TABLES],),
        )
        readers = [str(row[0]) for row in cur.fetchall()]
    if readers:
        raise SubstrateError(
            "cannot retire legacy graph storage while "
            f"{len(readers)} relation(s) still read it: {', '.join(readers)}",
            data={
                "legacy_tables": list(LEGACY_GRAPH_LIVE_TABLES),
                "dependent_relations": readers,
            },
            fix_hint=(
                "install the canonical read path first — `okg migrate` runs "
                "views.sql, which binds okg.v_nodes / okg.v_edges to "
                "okg.graph_nodes / okg.graph_edges — then retire. Retirement "
                "does not rewrite views; views.sql is the only definition of "
                "the read path."
            ),
        )


def retire_legacy_graph_storage(conn: psycopg.Connection) -> None:
    """Drop legacy live-table storage from a Chronos-native deployment.

    The graph source of truth is Chronos interval storage. Catalog,
    admission, source watermark, runtime, and audit tables remain
    relational. This helper intentionally retires only the obsolete
    bitemporal live projection tables; fact logs, generation rows, branch
    metadata, and read views are current audit/read surfaces.
    """
    _assert_graph_read_path_is_chronos_backed(conn)
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS okg")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS okg.chronos_native_graph_storage_marker (
              enabled_at timestamptz NOT NULL DEFAULT now(),
              reason text NOT NULL DEFAULT 'chronos_native_graph_storage'
            )
            """
        )
        cur.execute(
            """
            INSERT INTO okg.chronos_native_graph_storage_marker (enabled_at)
            SELECT now()
            WHERE NOT EXISTS (
              SELECT 1 FROM okg.chronos_native_graph_storage_marker
            )
            """
        )
        drop_targets = sql.SQL(", ").join(
            sql.Identifier(*relation.split(".", 1))
            for relation in LEGACY_GRAPH_LIVE_TABLES
        )
        cur.execute(
            sql.SQL("DROP TABLE IF EXISTS {}").format(drop_targets)
        )


def list_databases(
    *,
    cluster_dsn: str | None = None,
    deployments_root: Path | None = None,
) -> dict[str, Any]:
    manifests = discover_manifest_databases(deployments_root=deployments_root)
    manifest_by_db: dict[str, list[ManifestDatabase]] = {}
    for manifest in manifests:
        manifest_by_db.setdefault(manifest.database, []).append(manifest)

    if cluster_dsn:
        entries = _cluster_inventory(cluster_dsn, manifest_by_db)
    else:
        entries = [
            _inventory_entry_for_manifest_group(database, owners)
            for database, owners in sorted(manifest_by_db.items())
        ]

    return {
        "mode": "cluster" if cluster_dsn else "manifest",
        "database_count": len(entries),
        "databases": entries,
    }


def discover_manifest_databases(
    *, deployments_root: Path | None = None,
) -> list[ManifestDatabase]:
    from okg.substrate.deployments import (
        DeploymentManifest,
        find_deployments_root,
    )

    root = deployments_root or find_deployments_root()
    if root is None:
        return []
    manifests: list[ManifestDatabase] = []
    for manifest_path in sorted(Path(root).glob("*/deployment.yaml")):
        try:
            manifest = DeploymentManifest.from_yaml(manifest_path)
        except Exception:  # noqa: BLE001 — one bad deployment.yaml shouldn't fail the inventory scan; skip and continue.
            continue
        dsn = manifest.postgres_dsn
        if not dsn:
            continue
        manifests.append(
            ManifestDatabase(
                deployment=manifest.name,
                dsn=dsn,
                database=database_name_from_dsn(dsn) or "",
                runtime_mode=(
                    "dbos" if bool(getattr(manifest.runtime, "enabled", False))
                    else "disabled"
                ),
                manifest_path=str(manifest_path),
            )
        )
    return manifests


def inspect_database(
    dsn: str,
    *,
    deployment_name: str | None = None,
    top_limit: int = 20,
) -> dict[str, Any]:
    with db_connect(dsn, connect_timeout=5) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            database = _current_database(cur)
            status: dict[str, Any] = {
                "database": database,
                "deployment": deployment_name,
                "size_bytes": _database_size(cur, database),
                "installed_schemas": _installed_schemas(cur),
                "generation": _generation_summary(cur),
                "source_watermarks": _source_watermark_summary(cur),
                "top_relations": _top_relations(cur, limit=top_limit),
                "relation_families": {},
                "timescale": _timescale_summary(cur),
                "obsolete_job_runtime": _obsolete_job_runtime_summary(cur),
                "dbos_workflows": _dbos_workflow_summary(cur),
                "runtime_queue_health": _runtime_queue_health_summary(cur),
                "execution_authority": "dbos.workflow_status",
                "source_progress_authority": (
                    "okg.source_sync_progress_and_watermarks"
                ),
                "derived_relations": _derived_relation_summary(cur),
                "migration_status": _migration_status_from_cursor(cur),
                "retirement_marker_present": _regclass_exists(
                    cur, "okg.legacy_runtime_retirement_marker"
                ),
                "warnings": [],
            }
            families: dict[str, int] = {}
            for rel in status["top_relations"]:
                families.setdefault(str(rel["family"]), 0)
                families[str(rel["family"])] += int(rel["total_bytes"] or 0)
            status["relation_families"] = [
                {"family": family, "total_bytes": total}
                for family, total in sorted(
                    families.items(), key=lambda item: item[1], reverse=True
                )
            ]
            detected = status["generation"].get("detected_deployments", [])
            non_bootstrap = [d for d in detected if d != "bootstrap"]
            if len(non_bootstrap) > 1:
                status["warnings"].append(
                    {
                        "code": "mixed_deployment_state",
                        "message": "database contains multiple deployment names",
                        "deployments": non_bootstrap,
                    }
                )
            return _jsonify_row(status)


def database_health_findings(status: dict[str, Any]) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    database = str(status.get("database") or "")
    detected = (
        status.get("generation", {}).get("detected_deployments", [])
        if isinstance(status.get("generation"), dict) else []
    )
    non_bootstrap = [d for d in detected if d != "bootstrap"]
    if len(non_bootstrap) > 1:
        findings.append(
            {
                "severity": "warning",
                "database": database,
                "subsystem": "deployment_ownership",
                "code": "mixed_deployment_state",
                "observed": non_bootstrap,
                "expected": "one deployment name per deployment DB",
                "fix_hint": "split deployments into separate Postgres DBs",
            }
        )

    obsolete_runtime = status.get("obsolete_job_runtime") or {}
    if obsolete_runtime.get("cleanup_required"):
        findings.append(
            {
                "severity": "error",
                "database": database,
                "subsystem": "obsolete_job_runtime",
                "code": "obsolete_job_runtime_cleanup_required",
                "observed": obsolete_runtime,
                "expected": "obsolete OKG job-runtime subsystem absent",
                "fix_hint": (
                    "deploy the DBOS-only runtime, drain affected workflows, "
                    "then run `okg db remove-job-runtime-subsystem "
                    "--confirm --apply`"
                ),
            }
        )

    dbos = status.get("dbos_workflows") or {}
    runtime_queue = status.get("runtime_queue_health") or {}
    if runtime_queue.get("present"):
        if runtime_queue.get("error"):
            findings.append(
                {
                    "severity": "warning",
                    "database": database,
                    "subsystem": "runtime_queue",
                    "code": "runtime_queue_health_unreadable",
                    "observed": {
                        "error": runtime_queue.get("error"),
                        "message": runtime_queue.get("message"),
                    },
                    "expected": (
                        "okg.v_runtime_queue_health is readable and returns "
                        "runtime queue classification rows"
                    ),
                    "fix_hint": (
                        "inspect okg.v_runtime_queue_health and DBOS runtime "
                        "schema compatibility"
                    ),
                }
            )
        for row in runtime_queue.get("rows", []):
            queue_status = str(row.get("status") or "")
            if queue_status in {"idle", "running", "waiting"}:
                continue
            findings.append(
                {
                    "severity": "warning",
                    "database": database,
                    "subsystem": "runtime_queue",
                    "code": "runtime_queue_unhealthy",
                    "observed": row,
                    "expected": (
                        "runtime queue health is idle, running, or waiting "
                        "without stale or terminal failure state"
                    ),
                    "fix_hint": (
                        "inspect DBOS worker liveness, queue backlog, and "
                        "terminal workflow history"
                    ),
                }
            )
    if dbos.get("present"):
        for row in dbos.get("by_status", []):
            if row.get("status") not in {"PENDING", "ENQUEUED"}:
                continue
            if int(row.get("workflow_count") or 0) <= 0:
                continue
            observed = dict(row)
            observed["workflow_status_total_bytes"] = dbos.get(
                "workflow_status_total_bytes"
            )
            findings.append(
                {
                    "severity": "warning",
                    "database": database,
                    "subsystem": "dbos",
                    "code": "dbos_workflow_backlog",
                    "observed": observed,
                    "expected": "no stale PENDING or ENQUEUED workflows",
                    "fix_hint": "inspect DBOS worker liveness and queue backlog",
                }
            )

    migration = status.get("migration_status") or {}
    if migration and not migration.get("ledger_exists", False):
        findings.append(
            {
                "severity": "warning",
                "database": database,
                "subsystem": "schema_migrations",
                "code": "schema_migration_ledger_missing",
                "observed": "okg.schema_migrations is absent",
                "expected": "migration ledger exists after okg migrate",
                "fix_hint": "run okg migrate before relying on drift status",
            }
        )
    if migration.get("checksum_mismatches"):
        findings.append(
            {
                "severity": "error",
                "database": database,
                "subsystem": "schema_migrations",
                "code": "schema_migration_checksum_mismatch",
                "observed": migration["checksum_mismatches"],
                "expected": "ledger checksums match current migration files",
                "fix_hint": "inspect migration files and ledger before migrating",
            }
        )
    blocking_drift = _blocking_postcondition_drift(
        migration.get("postcondition_drift") or []
    )
    if blocking_drift:
        # Issue #1054: a destructive migration's recorded postcondition is the
        # only evidence it did what it claimed. Reporting drift without failing
        # anything would leave the false claim exactly as invisible as before,
        # so this is an ERROR finding, not a note.
        findings.append(
            {
                "severity": "error",
                "database": database,
                "subsystem": "schema_migrations",
                "code": "schema_migration_postcondition_drift",
                "observed": blocking_drift,
                "expected": (
                    "each applied destructive migration's recorded "
                    "postcondition still holds against this database"
                ),
                "fix_hint": (
                    "the ledger claims an end state the database contradicts; "
                    "reconcile it (re-run the migration, or record it "
                    "unverified with a reason) before trusting migration status"
                ),
            }
        )
    if migration.get("destructive_pending"):
        findings.append(
            {
                "severity": "info",
                "database": database,
                "subsystem": "schema_migrations",
                "code": "destructive_migration_pending",
                "observed": migration["destructive_pending"],
                "expected": "operator-run destructive migrations are explicit",
                "fix_hint": "run only after documented cutover gates pass",
            }
        )
    if migration.get("multiphase_in_progress"):
        findings.append(
            {
                "severity": "warning",
                "database": database,
                "subsystem": "schema_migrations",
                "code": "multiphase_migration_in_progress",
                "observed": migration["multiphase_in_progress"],
                "expected": (
                    "no multi-phase migration is mid-run; every "
                    "declared multi-phase migration has a completion "
                    "ledger row"
                ),
                "fix_hint": (
                    "resume the run via okg.substrate.db.multiphase."
                    "run_multiphase_migration; it continues from the "
                    "recorded per-phase watermark"
                ),
            }
        )
    if migration.get("multiphase_pending"):
        findings.append(
            {
                "severity": "warning",
                "database": database,
                "subsystem": "schema_migrations",
                "code": "multiphase_migration_pending",
                "observed": migration["multiphase_pending"],
                "expected": (
                    "no declared multi-phase migration is pending; "
                    "`okg up` does NOT run these inline"
                ),
                "fix_hint": (
                    "run the registered phase spec through "
                    "okg.substrate.db.multiphase.run_multiphase_migration"
                ),
            }
        )
    if migration.get("unknown_applied"):
        findings.append(
            {
                "severity": "warning",
                "database": database,
                "subsystem": "schema_migrations",
                "code": "schema_migration_unknown_applied",
                "observed": [
                    str(row.get("migration_id"))
                    for row in migration["unknown_applied"]
                ],
                "expected": (
                    "every applied ledger row matches a known migration "
                    "file or a migrations/manifest.yaml entry"
                ),
                "fix_hint": (
                    "identify the retired/foreign migration rows; document "
                    "them in migrations/manifest.yaml or reconcile the ledger"
                ),
            }
        )

    timescale = status.get("timescale") or {}
    missing_policy = [
        row for row in timescale.get("hypertables", [])
        if str(row.get("hypertable_name")) in {
            "node_facts",
            "edge_facts",
            "mcp_trace_events",
        }
        and int(row.get("policy_count") or 0) == 0
    ]
    if missing_policy:
        findings.append(
            {
                "severity": "warning",
                "database": database,
                "subsystem": "timescale",
                "code": "missing_timescale_policy",
                "observed": missing_policy,
                "expected": "substrate hypertables have retention/compression policies",
                "fix_hint": "rerun migrate and inspect timescaledb_information.jobs",
            }
        )
    return findings


#: The plan-cache mode every graph read login role keeps (PACT
#: lane-s-redesign-s1, lane-s-storage-redesign section 1.7): a generic plan
#: cannot use the partial and per-value indexes the reads depend on.
READ_ROLE_PLAN_CACHE_MODE = "force_custom_plan"
#: Read login roles checked by name; each only when it exists.
#: ``projection_remediator`` is not one: it is created NOLOGIN (migrate.py),
#: and PostgreSQL applies ``ALTER ROLE ... SET`` only at login, never on
#: ``SET ROLE``, so its setting could neither help nor hurt a read.
_NAMED_READ_ROLES = (("okg_mcp", "mcp_read_role"),)
#: The MCP read DSN override; its login role is checked too.
_MCP_RO_DSN_ENV = "OKG_MCP_RO_DSN"
#: Precedence of ``pg_db_role_setting`` rows, most specific first (the order
#: PostgreSQL applies them at login): the role in this database, the role
#: everywhere, this database for every role, then ``ALTER ROLE ALL SET``
#: (every role in every database).
_SETTING_SCOPES = ("role_in_database", "role", "database", "global")
#: ``pg_settings.source`` values under which the doctor session's own value
#: is the server-wide one (built-in default, postgresql.conf, ALTER SYSTEM,
#: the command line). Any other source (``database``, ``user``, ``database
#: user``, ``session``, ``client``) means the doctor's own login or session
#: hides the server value.
_SERVER_SETTING_SOURCES = frozenset({
    "default", "configuration file", "command line", "environment variable",
    "override",
})


def server_setting_value(row: Mapping[str, Any] | None) -> str | None:
    """The server-wide value from the doctor session's ``pg_settings`` row
    (``setting``, ``source``), or None when that session's own role,
    database or session setting hides it."""
    if row is None:
        return None
    if str(row.get("source") or "") not in _SERVER_SETTING_SOURCES:
        return None
    return str(row["setting"])


def read_roles_to_check(env: Mapping[str, str] | None = None) -> list[dict[str, str]]:
    """The read login roles the doctor checks: ``okg_mcp`` and the login role
    of ``OKG_MCP_RO_DSN`` (what libpq would log in as: its ``user``, else
    ``PGUSER``, else the OS user). Each once.

    When that login role cannot be worked out (a malformed DSN, no OS user
    name) the entry carries ``unresolved`` with a reason that names only the
    source, never any DSN text: a DSN can hold a password."""
    import getpass

    from okg.substrate.db import dsn as dsn_mod

    environ = os.environ if env is None else env
    roles = [{"role": role, "source": source} for role, source in _NAMED_READ_ROLES]
    read_dsn = environ.get(_MCP_RO_DSN_ENV)
    if read_dsn:
        parsed = dsn_mod.try_parse(read_dsn)
        if parsed is None:  # reported as a finding, DSN text withheld
            roles.append({
                "role": "",
                "source": _MCP_RO_DSN_ENV,
                "unresolved": f"{_MCP_RO_DSN_ENV} could not be parsed",
            })
            return roles
        user = parsed.params.get("user") or environ.get("PGUSER")
        if not user:
            try:
                user = getpass.getuser()
            except Exception:  # noqa: BLE001 -- reported as a finding
                roles.append({
                    "role": "",
                    "source": _MCP_RO_DSN_ENV,
                    "unresolved": (
                        f"{_MCP_RO_DSN_ENV} names no user, PGUSER is unset and the "
                        + "operating-system user name could not be read"
                    ),
                })
                return roles
        if str(user) not in {item["role"] for item in roles}:
            roles.append({"role": str(user), "source": _MCP_RO_DSN_ENV})
    return roles


def effective_role_setting(
    rows: Sequence[Mapping[str, Any]], name: str, *, server: str | None = None
) -> tuple[str | None, str | None]:
    """``(value, scope)`` of setting ``name`` for a role logging in to this
    database, with PostgreSQL's precedence: ``pg_db_role_setting`` rows (each
    ``{"scope": ..., "setconfig": [...]}``) for the role in this database,
    then the role everywhere, then this database for every role, then every
    role everywhere (``ALTER ROLE ALL SET``, scope ``global``), then the
    server-wide value ``server`` (scope ``server``). ``(None, None)`` when no
    row sets it and the server value is unknown."""
    by_scope: dict[str, str] = {}
    prefix = f"{name.lower()}="
    for row in rows:
        for item in row.get("setconfig") or ():
            text = str(item)
            if text.lower().startswith(prefix):
                by_scope[str(row["scope"])] = text[len(prefix):]
    for scope in _SETTING_SCOPES:
        if scope in by_scope:
            return by_scope[scope], scope
    if server is not None:
        return server, "server"
    return None, None


def plan_cache_role_findings(
    roles: Sequence[Mapping[str, Any]], *, database: str
) -> list[dict[str, Any]]:
    """Findings for the checked read roles. Each role is
    ``{"role", "source", "exists", "owner", "value", "scope"}``."""
    findings: list[dict[str, Any]] = []
    for item in roles:
        role = str(item["role"])
        if item.get("unresolved"):
            findings.append({
                "severity": "error",
                "database": database,
                "subsystem": "plan_cache",
                "code": "read_role_unresolved",
                "role": None,
                "source": item.get("source"),
                "observed": str(item["unresolved"]),
                "expected": f"the login role of {_MCP_RO_DSN_ENV} can be worked out",
                "fix_hint": f"correct {_MCP_RO_DSN_ENV} (a libpq connection string or URI)",
            })
            continue
        if not item.get("exists"):
            if item.get("source") == _MCP_RO_DSN_ENV:
                findings.append({
                    "severity": "error",
                    "database": database,
                    "subsystem": "plan_cache",
                    "code": "read_role_missing",
                    "role": role,
                    "observed": "role does not exist",
                    "expected": f"the login role of {_MCP_RO_DSN_ENV} exists",
                    "fix_hint": f"correct {_MCP_RO_DSN_ENV} or create the role",
                })
            continue
        if item.get("owner"):
            if item.get("source") == _MCP_RO_DSN_ENV:
                # The MCP server would read as the owner (or as the role
                # doctor runs as, which keeps the server default on purpose);
                # mcp-serve warns on that real connection, so doctor does too.
                findings.append({
                    "severity": "warning",
                    "database": database,
                    "subsystem": "plan_cache",
                    "code": "read_role_is_owner",
                    "role": role,
                    "source": item.get("source"),
                    "observed": {
                        "plan_cache_mode": item.get("value"),
                        "scope": item.get("scope"),
                    },
                    "expected": f"{_MCP_RO_DSN_ENV} names a read-only login role, not "
                    + "the database owner or the role doctor runs as",
                    "fix_hint": f"point {_MCP_RO_DSN_ENV} at a read login role with "
                    + f"plan_cache_mode = {READ_ROLE_PLAN_CACHE_MODE}",
                })
            continue
        value = item.get("value")
        if str(value or "").lower() == READ_ROLE_PLAN_CACHE_MODE:
            continue
        quoted = '"' + role.replace('"', '""') + '"'
        if item.get("scope") == "role_in_database":
            fix = (
                f"ALTER ROLE {quoted} IN DATABASE \"{database}\" RESET "
                "plan_cache_mode (an in-database setting overrides the "
                f"role-wide one), and ALTER ROLE {quoted} SET plan_cache_mode = "
                f"{READ_ROLE_PLAN_CACHE_MODE} if that is missing too"
            )
        else:
            fix = f"ALTER ROLE {quoted} SET plan_cache_mode = {READ_ROLE_PLAN_CACHE_MODE}"
        findings.append({
            "severity": "error",
            "database": database,
            "subsystem": "plan_cache",
            "code": "read_role_plan_cache_mode",
            "role": role,
            "source": item.get("source"),
            "observed": {
                "plan_cache_mode": value if value is not None else (
                    "unknown: no role or database setting, and the doctor's own "
                    + "role or session setting hides the server value"
                ),
                "scope": item.get("scope"),
            },
            "expected": f"plan_cache_mode = {READ_ROLE_PLAN_CACHE_MODE} for this read login role",
            "fix_hint": fix,
        })
    return findings


def missing_tool_index_findings(
    dsn: str,
    *,
    deployment_name: str | None = None,
) -> list[dict[str, Any]]:
    """One finding per tool index `okg migrate --apply` would build (#3311).

    The migration ledger does not list these indexes, so a database could
    pass this report while `search(method="grep")` refused every call. A
    missing index that makes a tool refuse is an error; one that only makes
    it scan is a warning. Readiness that cannot be read is an error finding
    of its own, never a clean report.
    """
    from okg.substrate.db.connection import connect as db_connect
    from okg.substrate.db.readiness import (
        drop_invalid_index_hint,
        migrate_command,
        read_database_readiness,
    )

    try:
        with db_connect(dsn, autocommit=True) as conn:
            readiness = read_database_readiness(conn)
    except Exception as exc:  # noqa: BLE001 - reported as a finding, not raised.
        return [{
            "severity": "error",
            "subsystem": "schema",
            "code": "schema_objects_unverifiable",
            "observed": str(exc),
            "expected": "the tool indexes okg migrate builds can be read",
            "fix_hint": "check that the role can read pg_index and "
            "public._chronos_branch_tables",
        }]
    findings: list[dict[str, Any]] = []
    for missing in readiness.missing_schema_objects:
        refused = missing.effect == "refused"
        consequence = (
            f"{missing.required_by} refuses every call"
            if refused
            else f"{missing.required_by} scans instead of using it"
        )
        findings.append({
            "severity": "error" if refused else "warning",
            "subsystem": "schema",
            "code": "tool_index_missing",
            "index": missing.name,
            "state": missing.state,
            "required_by": missing.required_by,
            "observed": f"index {missing.name} is {missing.state}, so {consequence}",
            "expected": "every tool index okg migrate builds is present and valid",
            "fix_hint": (
                f"{drop_invalid_index_hint((missing,))}, then run "
                if missing.state == "invalid" else "run "
            ) + f"`{migrate_command(deployment_name)}`",
        })
    return findings


def read_role_plan_cache_report(
    dsn: str, *, env: Mapping[str, str] | None = None
) -> dict[str, Any]:
    """Every read login role's effective ``plan_cache_mode`` in this database,
    read from ``pg_db_role_setting``, and the findings for it. The database
    owner and the connecting role are never held to the setting (the owner
    and publisher roles keep the server default on purpose); when
    ``OKG_MCP_RO_DSN`` names one of them, that is a warning."""
    checked: list[dict[str, Any]] = []
    with db_connect(dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT current_database() AS database, current_user AS me,
                       pg_get_userbyid(d.datdba) AS owner
                  FROM pg_database d
                 WHERE d.datname = current_database()
                """
            )
            identity = dict(cur.fetchone())
            cur.execute(
                "SELECT setting, source FROM pg_settings WHERE name = 'plan_cache_mode'"
            )
            server_row = cur.fetchone()
            server = server_setting_value(dict(server_row) if server_row else None)
            for item in read_roles_to_check(env):
                if item.get("unresolved"):
                    checked.append({**item, "exists": False, "owner": False,
                                    "value": None, "scope": None})
                    continue
                role = item["role"]
                cur.execute("SELECT oid FROM pg_roles WHERE rolname = %s", (role,))
                found = cur.fetchone()
                row: dict[str, Any] = {
                    **item,
                    "exists": found is not None,
                    "owner": role in {identity["me"], identity["owner"]},
                    "value": None,
                    "scope": None,
                }
                if found is not None:
                    cur.execute(
                        """
                        SELECT CASE
                                 WHEN s.setrole <> 0 AND s.setdatabase <> 0 THEN 'role_in_database'
                                 WHEN s.setrole <> 0 THEN 'role'
                                 WHEN s.setdatabase <> 0 THEN 'database'
                                 ELSE 'global'
                               END AS scope,
                               s.setconfig
                          FROM pg_db_role_setting s
                         WHERE s.setrole IN (%s, 0)
                           AND s.setdatabase IN (
                                 (SELECT oid FROM pg_database WHERE datname = current_database()),
                                 0
                               )
                        """,
                        (found["oid"],),
                    )
                    row["value"], row["scope"] = effective_role_setting(
                        cur.fetchall(), "plan_cache_mode", server=server
                    )
                checked.append(row)
    findings = plan_cache_role_findings(checked, database=str(identity["database"]))
    return {
        "expected": READ_ROLE_PLAN_CACHE_MODE,
        "server_setting": dict(server_row) if server_row else None,
        "roles": [
            {key: row.get(key) for key in (
                "role", "source", "exists", "owner", "value", "scope", "unresolved",
            )}
            for row in checked
        ],
        "findings": findings,
    }


def database_health_report(
    dsn: str,
    *,
    deployment_name: str | None = None,
) -> dict[str, Any]:
    status = inspect_database(dsn, deployment_name=deployment_name)
    findings = database_health_findings(status)
    # The cold-history manifest is optional on fresh/small deployments. Once
    # an online rotation starts, however, doctor must fail closed on stale
    # floors, invalid/missing hot or BRIN indexes, full-index resurrection, or
    # an interrupted lifecycle state.
    from okg.substrate.db.cold_history import (
        cold_history_health_report,
        graph_read_index_findings,
    )

    cold_history = cold_history_health_report(dsn)
    findings.extend(cold_history["findings"])
    # PACT lane-s-redesign-s1: every graph read index spec (the GIN index on
    # node attrs among them) is built and valid, and every read login role
    # keeps custom plans, on every deployment.
    findings.extend(graph_read_index_findings(dsn))
    plan_cache = read_role_plan_cache_report(dsn)
    findings.extend(plan_cache["findings"])
    findings.extend(missing_tool_index_findings(dsn, deployment_name=deployment_name))
    ok = not any(f.get("severity") == "error" for f in findings)
    return {
        "ok": ok,
        "status": "OK" if ok else "ERROR",
        "database": status.get("database"),
        "deployment": deployment_name,
        "cold_history": cold_history,
        "plan_cache": plan_cache,
        "findings": findings,
        "summary": {
            "finding_count": len(findings),
            "error_count": sum(1 for f in findings if f.get("severity") == "error"),
            "warning_count": sum(1 for f in findings if f.get("severity") == "warning"),
        },
    }


# --- disk-usage report (PACT bound-okg-workspace-disk-growth, task 1) ---
#
# Cheap catalog reads only (no full-table scans) so the report is safe to run
# recurringly. pgstattuple_approx is opt-in via deep=True (it scans). Every read
# that must yield a value raises rather than returning a default/zero, and an
# unreadable DSN raises at connect() -- the never-default-zero contract.

DISK_USAGE_SAMPLE_TABLE = "okg.disk_usage_samples"
_DISK_UNUSED_INDEX_WARN_BYTES = 5 * 1024**3
_DISK_DEEP_DEAD_PCT_WARN = 25.0

# okg#1324: index structure that outlived its rows. VACUUM reclaims dead tuples
# but never shrinks a btree, so a table drained of the bulk of its rows keeps
# index pages sized to rows that are long gone. On okg-workspace
# `okg.attribute_effective_view` carried 2,333 MB of indexes over a 16 kB heap
# (16 live rows) after its payload moved to the interval sidecar, and its 974 MB
# primary key had been scanned 24.7M times -- every one of those lookups walking
# a near-empty multi-gigabyte tree. `REINDEX TABLE CONCURRENTLY` returned it to
# 56 kB.
#
# The thresholds are the operator's ad-hoc scan (3x, and indexes over 50 MB),
# which returned exactly that one table. The ratio alone would flag every small
# healthy table -- a three-index 8 kB table is "24x its heap" and costs nothing
# -- so the byte floor is what makes the finding actionable.
#
# The probe TIGHTENS that scan in one place: the operator compared against
# `pg_relation_size` (the main fork only), and this compares against
# `pg_table_size` (main fork + TOAST + FSM/VM), the same measure
# `_disk_top_relations` uses. A one-off scan of a table already known to be
# drained can use either; a standing probe cannot. The attribute tables store
# jsonb payloads, so most of a HEALTHY one's bytes live in TOAST: measured here,
# a 20k-row toasted table has a 1.2 MB main fork against a 208 MB table size.
# Ranking indexes against the main fork alone would flag a fully populated,
# perfectly healthy attribute table -- the very table this probe exists for,
# before its drain. On the 16-row incident state the heap was 16 kB either way,
# so the true positive is unaffected.
_DISK_INDEX_BLOAT_RATIO = 3.0
_DISK_INDEX_BLOAT_MIN_INDEX_BYTES = 50 * 1024**2


def _disk_dead_pct(
    n_live: int, n_dead: int, reltuples: int
) -> tuple[float | None, bool]:
    """Cheap dead-tuple ratio with a stale-counter guard.

    pg_stat_user_tables counters reset on an unclean restart, leaving
    n_live_tup=0 while the catalog still estimates rows present (reltuples>0).
    In that case return (None, True) -- NEVER a misleading 100% -- so the
    operator runs the deep (pgstattuple) mode for ground truth.
    """
    stale = n_live == 0 and reltuples > 0
    total = n_live + n_dead
    if stale or total == 0:
        return None, stale
    return round(100.0 * n_dead / total, 1), False


def _disk_marginal_from_samples(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Marginal bytes-per-generation from the two most-recent size samples.

    ``rows`` is newest-first ``[{generations, total_bytes}, ...]``. With fewer
    than two samples, or no new generations between them, report ``None`` plus a
    reason -- never a zero.
    """
    if len(rows) < 2:
        return {
            "bytes_per_generation": None,
            "reason": "insufficient_history",
            "samples_used": len(rows),
        }
    g_new, g_prev = rows[0]["generations"], rows[1]["generations"]
    if g_new is None or g_prev is None or (g_new - g_prev) <= 0:
        return {
            "bytes_per_generation": None,
            "reason": "no_new_generations",
            "samples_used": 2,
        }
    delta = int(rows[0]["total_bytes"]) - int(rows[1]["total_bytes"])
    return {
        "bytes_per_generation": round(delta / (g_new - g_prev), 1),
        "reason": None,
        "samples_used": 2,
    }


def _disk_top_relations(cur, limit: int) -> list[dict[str, Any]]:
    cur.execute(
        """
        -- table_bytes = pg_table_size (heap + TOAST + FSM/VM), so
        -- table_bytes + index_bytes == total_bytes reconciles. pg_relation_size
        -- (main fork only) would silently drop TOAST, which for the jsonb/blob
        -- tables here is the bulk of the storage. Only concrete relations
        -- ('r','m'): a partitioned parent ('p') has pg_table_size=0 but a
        -- non-zero pg_total_relation_size (the partition tree), so it would NOT
        -- reconcile and would show a misleading 0-byte row -- its real bytes
        -- live in the child partitions, which appear as their own 'r' rows.
        SELECT n.nspname AS schema, c.relname AS relation,
               pg_total_relation_size(c.oid) AS total_bytes,
               pg_size_pretty(pg_total_relation_size(c.oid)) AS total_pretty,
               pg_table_size(c.oid)     AS table_bytes,
               pg_indexes_size(c.oid)   AS index_bytes,
               c.reltuples::bigint      AS est_rows
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind IN ('r', 'm')
        ORDER BY pg_total_relation_size(c.oid) DESC
        LIMIT %s
        """,
        (limit,),
    )
    return [_jsonify_row(row) for row in cur.fetchall()]


def _disk_dead_tuples_cheap(cur, limit: int) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT s.relid::regclass::text AS relation,
               s.n_live_tup AS n_live, s.n_dead_tup AS n_dead,
               c.reltuples::bigint AS reltuples
        FROM pg_stat_user_tables s JOIN pg_class c ON c.oid = s.relid
        ORDER BY pg_total_relation_size(s.relid) DESC
        LIMIT %s
        """,
        (limit,),
    )
    out: list[dict[str, Any]] = []
    for row in cur.fetchall():
        n_live, n_dead = int(row["n_live"]), int(row["n_dead"])
        reltuples = int(row["reltuples"])
        dead_pct, stale = _disk_dead_pct(n_live, n_dead, reltuples)
        out.append({
            "relation": row["relation"], "n_live": n_live, "n_dead": n_dead,
            "reltuples": reltuples, "dead_pct": dead_pct,
            "stats_stale": stale, "source": "pg_stat",
        })
    return out


def _disk_ensure_pgstattuple(cur) -> None:
    """Ensure pgstattuple is available for the deep scan, or raise a clear error.

    pgstattuple is NOT a trusted extension, so CREATE needs a superuser DSN. On
    a least-privilege deployment DSN this fails -- raise an actionable error
    (caught + emitted by the CLI wrappers) rather than a bare privilege
    traceback or, worse, a silent skip.
    """
    cur.execute("SELECT count(*) AS n FROM pg_extension WHERE extname = 'pgstattuple'")
    if int(cur.fetchone()["n"]) > 0:
        return
    try:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pgstattuple")
    except psycopg.Error as exc:
        raise RuntimeError(
            "deep disk report needs the pgstattuple extension; it is not "
            "installed and could not be created (CREATE EXTENSION needs a "
            "superuser DSN). Install it once as a superuser "
            "(`CREATE EXTENSION pgstattuple`) or omit --deep."
        ) from exc


def _disk_dead_tuples_deep(cur, limit: int) -> list[dict[str, Any]]:
    _disk_ensure_pgstattuple(cur)
    # Include the largest relations regardless of schema -- the TimescaleDB chunk
    # heaps under _timescaledb_internal hold real bytes; excluding them would let
    # deep mode report "no bloat" while skipping the bulk of the DB. pg_toast /
    # catalog are still excluded (their bloat is not actionable here).
    cur.execute(
        """
        SELECT c.oid::regclass::text AS relation
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind IN ('r', 'm')
          AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        ORDER BY pg_total_relation_size(c.oid) DESC
        LIMIT %s
        """,
        (limit,),
    )
    rels = [row["relation"] for row in cur.fetchall()]
    out: list[dict[str, Any]] = []
    for rel in rels:
        # Savepoint per relation: pgstattuple_approx can error on some relations
        # (e.g. a compressed chunk). Roll back to the savepoint and record None +
        # a note -- never a fabricated 0 -- so one bad relation can't abort the
        # whole report or invalidate the outer transaction.
        cur.execute("SAVEPOINT disk_pgst")
        try:
            cur.execute(
                "SELECT dead_tuple_percent FROM pgstattuple_approx(%s)", (rel,)
            )
            row = cur.fetchone()
            dead_pct = round(float(row["dead_tuple_percent"]), 1) if row else None
            note = None
            cur.execute("RELEASE SAVEPOINT disk_pgst")
        except psycopg.Error as exc:
            cur.execute("ROLLBACK TO SAVEPOINT disk_pgst")
            cur.execute("RELEASE SAVEPOINT disk_pgst")
            dead_pct, note = None, str(exc).splitlines()[0][:120]
        out.append({
            "relation": rel, "dead_pct": dead_pct, "note": note,
            "source": "pgstattuple_approx",
        })
    return out


def _disk_compression(cur) -> list[dict[str, Any]]:
    cur.execute("SELECT count(*) AS n FROM pg_extension WHERE extname = 'timescaledb'")
    if int(cur.fetchone()["n"]) == 0:
        return []
    cur.execute(
        """
        SELECT h.hypertable_name AS hypertable,
               count(c.*) AS chunks,
               count(c.*) FILTER (WHERE c.is_compressed) AS compressed_chunks
        FROM timescaledb_information.hypertables h
        LEFT JOIN timescaledb_information.chunks c
               ON c.hypertable_name = h.hypertable_name
        GROUP BY h.hypertable_name
        ORDER BY h.hypertable_name
        """
    )
    out: list[dict[str, Any]] = []
    for row in cur.fetchall():
        chunks, compressed = int(row["chunks"]), int(row["compressed_chunks"])
        out.append({
            "hypertable": row["hypertable"], "chunks": chunks,
            "compressed_chunks": compressed,
            "compressed_ratio": None if chunks == 0 else round(compressed / chunks, 3),
        })
    return out


def _disk_index_usage(cur) -> dict[str, Any]:
    cur.execute(
        """
        -- coalesce(idx_scan,0): a NULL scan count (stats reset / never scanned)
        -- is "unused", not "exclude" -- matching the dead-tuple stale guard.
        SELECT coalesce(sum(pg_relation_size(indexrelid)), 0) AS total_index_bytes,
               coalesce(sum(pg_relation_size(indexrelid))
                        FILTER (WHERE coalesce(idx_scan, 0) = 0), 0)
                   AS unused_index_bytes,
               count(*) FILTER (WHERE coalesce(idx_scan, 0) = 0)
                   AS unused_index_count
        FROM pg_stat_user_indexes
        """
    )
    row = cur.fetchone()
    return {
        "total_index_bytes": int(row["total_index_bytes"]),
        "unused_index_bytes": int(row["unused_index_bytes"]),
        "unused_index_count": int(row["unused_index_count"]),
    }


def _quote_qualified(schema: str, relation: str) -> str:
    """Double-quote a schema-qualified relation for an operator-runnable
    command. The probe emits DDL text a HUMAN pastes into psql; an unquoted
    name would be wrong for any mixed-case or reserved identifier."""
    dq = '"'
    return (
        f"{dq}{schema.replace(dq, dq * 2)}{dq}."
        f"{dq}{relation.replace(dq, dq * 2)}{dq}"
    )


def _disk_index_bloat(
    cur,
    *,
    schema: str = "okg",
    ratio: float | None = None,
    min_index_bytes: int | None = None,
) -> list[dict[str, Any]]:
    """Read-only: tables and materialized views in ``schema`` whose total index
    size exceeds both ``ratio`` x their table size and ``min_index_bytes``
    (okg#1324).

    Reports table and index bytes, every index with its own size and recorded
    scan count, and the exact ``REINDEX TABLE CONCURRENTLY`` an operator can
    run. It NEVER reindexes: a rebuild costs real time and I/O on a live
    deployment, so that decision stays with the operator.

    ``relkind IN ('r', 'm')``, matching :func:`_disk_top_relations`: a
    materialized view's indexes bloat the same way after a REFRESH. A
    partitioned parent ('p') is excluded because its own heap and indexes are
    empty by construction and it would flag vacuously.

    Scope is one schema, ``okg`` by default. Relations OUTSIDE it are NOT
    checked, including TimescaleDB chunks (``relkind = 'r'`` but living in
    ``_timescaledb_internal``) and the partitions of any parent that stores
    them elsewhere. Widen ``schema`` deliberately rather than assuming coverage.

    ``idx_scan`` is reported as a FACT with no verdict attached -- pg_stat
    counters reset on restart, so a zero-scan index here is not evidence that
    the index is unused. ``est_rows`` is ``None`` when the relation has never
    been analyzed (PG stores ``reltuples = -1``), never a negative row count.

    ``ratio`` and ``min_index_bytes`` default to the module thresholds, read
    at call time so ``okg doctor --check disk`` and ``okg db
    reclaim-index-space`` (okg#3112) always select by the same numbers.
    """
    if ratio is None:
        ratio = _DISK_INDEX_BLOAT_RATIO
    if min_index_bytes is None:
        min_index_bytes = _DISK_INDEX_BLOAT_MIN_INDEX_BYTES
    cur.execute(
        """
        -- table_bytes = pg_table_size (main fork + TOAST + FSM/VM), NOT
        -- pg_relation_size: the attribute tables keep most of a healthy row's
        -- bytes in TOAST, so ranking indexes against the main fork alone would
        -- flag a fully populated healthy table. pg_indexes_size excludes the
        -- TOAST table's own index (that lives inside pg_table_size), so
        -- table_bytes + index_bytes reconciles to pg_total_relation_size, and
        -- the per-index sizes below sum exactly to index_bytes because they
        -- are read with pg_total_relation_size for the same fork coverage.
        SELECT c.oid AS oid,
               n.nspname AS schema,
               c.relname AS relation,
               pg_table_size(c.oid)     AS table_bytes,
               pg_indexes_size(c.oid)   AS index_bytes,
               c.reltuples::bigint      AS est_rows
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind IN ('r', 'm')
          AND n.nspname = %(schema)s
          AND pg_indexes_size(c.oid) > %(min_index_bytes)s
          AND pg_indexes_size(c.oid) >
              %(ratio)s * pg_table_size(c.oid)::float8
        ORDER BY pg_indexes_size(c.oid) DESC
        """,
        {
            "schema": schema,
            "min_index_bytes": int(min_index_bytes),
            "ratio": float(ratio),
        },
    )
    flagged = cur.fetchall()
    out: list[dict[str, Any]] = []
    for row in flagged:
        cur.execute(
            """
            -- pg_total_relation_size, NOT pg_relation_size: the total above
            -- is pg_indexes_size, which counts every fork of every index,
            -- while pg_relation_size is the main fork alone. They agree only
            -- until autovacuum creates an index free-space map, after which
            -- the per-index list silently stops accounting for the footprint
            -- the finding reports -- an operator reading it sees sizes that do
            -- not add up to the number they were given. Measured on a drained
            -- 200k-row fixture: after a second VACUUM, pg_indexes_size was
            -- 87,498,752 against 87,433,216 of summed main forks, the missing
            -- 65,536 being two FSM forks. pg_total_relation_size matches
            -- fork-for-fork. (okg#1324; the mismatch reddened okg#1629 and
            -- okg#1634.)
            SELECT i.indexrelid::regclass::text AS index_name,
                   pg_total_relation_size(i.indexrelid) AS index_bytes,
                   coalesce(s.idx_scan, 0)              AS idx_scan
            FROM pg_index i
            LEFT JOIN pg_stat_all_indexes s ON s.indexrelid = i.indexrelid
            WHERE i.indrelid = %s
            ORDER BY pg_total_relation_size(i.indexrelid) DESC
            """,
            (row["oid"],),
        )
        indexes = [
            {
                "index": str(idx["index_name"]),
                "index_bytes": int(idx["index_bytes"]),
                "idx_scan": int(idx["idx_scan"]),
            }
            for idx in cur.fetchall()
        ]
        table_bytes = int(row["table_bytes"])
        index_bytes = int(row["index_bytes"])
        schema_name = str(row["schema"])
        relation = str(row["relation"])
        quoted = _quote_qualified(schema_name, relation)
        est_rows = int(row["est_rows"])
        out.append({
            "schema": schema_name,
            "relation": relation,
            "qualified": f"{schema_name}.{relation}",
            "table_bytes": table_bytes,
            "index_bytes": index_bytes,
            # None, not a fabricated ratio, when the table is empty -- the byte
            # floor has already proved the finding actionable.
            "index_to_table_ratio": (
                round(index_bytes / table_bytes, 1) if table_bytes else None
            ),
            # PG stores reltuples = -1 for a never-analyzed relation. Reporting
            # that verbatim reads to an operator as corruption; say "unknown".
            "est_rows": est_rows if est_rows >= 0 else None,
            "indexes": indexes,
            "reindex_command": f"REINDEX TABLE CONCURRENTLY {quoted};",
        })
    return out


def _disk_generation_count(cur) -> int | None:
    for table in ("okg.generation_ordinals", "okg.graph_generations"):
        if _regclass_exists(cur, table):
            return _relation_row_count(cur, table)
    return None


def _disk_ensure_sample_table(cur) -> None:
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {DISK_USAGE_SAMPLE_TABLE} (
            sampled_at  timestamptz NOT NULL DEFAULT now(),
            generations integer,
            total_bytes bigint NOT NULL
        )
        """
    )


_DISK_SAMPLE_RETENTION = 500


def _disk_record_sample(cur, generations: int | None, total_bytes: int) -> None:
    cur.execute(
        f"INSERT INTO {DISK_USAGE_SAMPLE_TABLE} (generations, total_bytes) "
        "VALUES (%s, %s)",
        (generations, total_bytes),
    )
    # Keep this growth-monitor from being its own unbounded writer: retain only
    # the most recent N samples.
    cur.execute(
        f"""
        DELETE FROM {DISK_USAGE_SAMPLE_TABLE}
        WHERE ctid NOT IN (
            SELECT ctid FROM {DISK_USAGE_SAMPLE_TABLE}
            ORDER BY sampled_at DESC LIMIT %s
        )
        """,
        (_DISK_SAMPLE_RETENTION,),
    )


def _disk_recent_samples(cur) -> list[dict[str, Any]]:
    # The marginal is only meaningful across DISTINCT generations: take the
    # latest sample per generation, then the two most recent generations. This
    # is robust to multiple same-generation samples (e.g. two runs racing).
    cur.execute(
        f"""
        SELECT generations, total_bytes FROM (
            SELECT DISTINCT ON (generations) generations, total_bytes
            FROM {DISK_USAGE_SAMPLE_TABLE}
            WHERE generations IS NOT NULL
            ORDER BY generations, sampled_at DESC
        ) s
        ORDER BY generations DESC
        LIMIT 2
        """
    )
    return [dict(row) for row in cur.fetchall()]


def _disk_findings(
    *, index_usage, compression, dead_tuples, generations, deep,
    index_bloat,
) -> list[dict[str, Any]]:
    # index_bloat is REQUIRED, like every other input here. A default would let
    # a future caller drop it and produce a report with zero bloat findings
    # from a database that has them -- absence read as agreement.
    findings: list[dict[str, Any]] = []
    for table in index_bloat:
        findings.append({
            "severity": "warning",
            "code": "index_larger_than_table",
            "relation": table["qualified"],
            "table_bytes": table["table_bytes"],
            "index_bytes": table["index_bytes"],
            "index_to_table_ratio": table["index_to_table_ratio"],
            "est_rows": table["est_rows"],
            "indexes": table["indexes"],
            "reindex_command": table["reindex_command"],
            "fix_hint": (
                "index pages outlived their rows (VACUUM reclaims tuples, not "
                "btree structure); rebuild with "
                f"`{table['reindex_command']}`"
            ),
        })
    if generations is None:
        findings.append({
            "severity": "warning", "code": "generation_table_missing",
            "fix_hint": "run `okg migrate` "
                        "(generation_ordinals/graph_generations absent)",
        })
    if index_usage["unused_index_bytes"] > _DISK_UNUSED_INDEX_WARN_BYTES:
        findings.append({
            "severity": "warning", "code": "reclaimable_unused_indexes",
            "observed_bytes": index_usage["unused_index_bytes"],
            "unused_index_count": index_usage["unused_index_count"],
            "fix_hint": "drop idx_scan=0 indexes "
                        "(PACT bound-okg-workspace-disk-growth task 2)",
        })
    for hypertable in compression:
        if hypertable["chunks"] > 0 and hypertable["compressed_chunks"] == 0:
            findings.append({
                "severity": "warning", "code": "uncompressed_hypertable",
                "hypertable": hypertable["hypertable"], "chunks": hypertable["chunks"],
                "fix_hint": "run/repair the TimescaleDB compression policy (task 3)",
            })
    if deep:
        for relation in dead_tuples:
            pct = relation.get("dead_pct")
            if pct is not None and pct > _DISK_DEEP_DEAD_PCT_WARN:
                findings.append({
                    "severity": "warning", "code": "bloated_relation",
                    "relation": relation["relation"], "dead_pct": pct,
                    "fix_hint": "repack/vacuum-full + tune autovacuum (task 4)",
                })
    return findings


def disk_usage_report(
    dsn: str,
    *,
    deployment_name: str | None = None,
    top_limit: int = 20,
    deep: bool = False,
    record_sample: bool = True,
) -> dict[str, Any]:
    """Disk-usage report for a deployment's database.

    Reports total size, the top relations (total/table/index), dead-tuple %
    (cheap pg_stat by default; pgstattuple_approx when ``deep``), the
    TimescaleDB compressed-chunk ratio, unused-index footprint, okg-schema
    tables whose indexes outgrew their heap (okg#1324), and the marginal
    bytes-per-generation between the two most-recent size samples.

    Findings are advisory (warning/info), so ``ok`` stays True on a successful
    read; an unreadable DSN raises at connect() and is never papered over with a
    zero. Backs ``okg metrics --disk`` and ``okg doctor --check disk`` (PACT
    bound-okg-workspace-disk-growth, task 1).
    """
    with db_connect(dsn, connect_timeout=5) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            database = _current_database(cur)
            total_bytes = _database_size(cur, database)
            generations = _disk_generation_count(cur)
            top_relations = _disk_top_relations(cur, top_limit)
            dead_tuples = (
                _disk_dead_tuples_deep(cur, min(top_limit, 8))
                if deep else _disk_dead_tuples_cheap(cur, top_limit)
            )
            compression = _disk_compression(cur)
            index_usage = _disk_index_usage(cur)
            index_bloat = _disk_index_bloat(cur)

            okg_present = _schema_exists(cur, "okg")
            if record_sample and okg_present:
                _disk_ensure_sample_table(cur)
                _disk_record_sample(cur, generations, total_bytes)
                conn.commit()
            if okg_present and _regclass_exists(cur, DISK_USAGE_SAMPLE_TABLE):
                marginal = _disk_marginal_from_samples(_disk_recent_samples(cur))
            else:
                marginal = {
                    "bytes_per_generation": None,
                    "reason": "okg_schema_absent" if not okg_present else "no_samples",
                    "samples_used": 0,
                }

            cur.execute(
                "SELECT pg_size_pretty(%s::bigint) AS pretty, now()::text AS now",
                (total_bytes,),
            )
            meta = cur.fetchone()

    findings = _disk_findings(
        index_usage=index_usage, compression=compression,
        dead_tuples=dead_tuples, generations=generations, deep=deep,
        index_bloat=index_bloat,
    )
    return {
        "ok": True,
        "status": "OK",
        "check": "disk",
        "database": database,
        "deployment": deployment_name,
        "total_bytes": total_bytes,
        "total_pretty": meta["pretty"],
        "generations": generations,
        "top_relations": top_relations,
        "dead_tuples": dead_tuples,
        "compression": compression,
        "index_usage": index_usage,
        "index_bloat": index_bloat,
        "marginal_growth": marginal,
        "deep": deep,
        "sampled_at": meta["now"],
        "findings": findings,
        "summary": {
            "finding_count": len(findings),
            "warning_count": sum(
                1 for f in findings if f.get("severity") == "warning"
            ),
        },
    }


def _bootstrap_readiness(
    *,
    installed: set[str],
    schema_applied: bool,
    catalog_version: int | None,
    database: str | None = None,
    deployment_name: str | None = None,
) -> dict[str, Any]:
    """Build the bootstrap-readiness report from probed facts (no DB access).

    Split out from :func:`bootstrap_preflight` so the report + finding logic is
    unit-testable without a live database. Per ``canonical-dev-okg``.
    """
    from okg.substrate.db.migrate import REQUIRED_EXTENSIONS

    missing_extensions = [n for n in REQUIRED_EXTENSIONS if n not in installed]
    findings: list[dict[str, Any]] = []
    if missing_extensions:
        findings.append({
            "severity": "error",
            "subsystem": "extensions",
            "code": "missing_required_extensions",
            "observed": missing_extensions,
            "expected": sorted(REQUIRED_EXTENSIONS),
            "fix_hint": "`okg provision --deployment <deployment>` "
                        "installs them, or "
                        "`CREATE EXTENSION IF NOT EXISTS <name>` per extension",
        })
    if not schema_applied:
        findings.append({
            "severity": "error",
            "subsystem": "schema",
            "code": "schema_not_applied",
            "observed": "okg.graph_generations absent",
            "expected": "v1 schema applied",
            "fix_hint": "`okg migrate` (applies the v1 schema idempotently)",
        })
    if catalog_version is None:
        findings.append({
            "severity": "error",
            "subsystem": "catalog",
            "code": "catalog_not_applied",
            "observed": "no okg.catalog_versions row",
            "expected": "a composed catalog applied",
            "fix_hint": "`okg catalog load --deployment <dir> --apply`",
        })

    ok = not findings
    return {
        "ok": ok,
        "status": "OK" if ok else "ERROR",
        "check": "preflight",
        "database": database,
        "deployment": deployment_name,
        "extensions": {
            "present": sorted(installed),
            "missing": missing_extensions,
        },
        "schema": {"applied": schema_applied, "sentinel": "okg.graph_generations"},
        "catalog": {"applied": catalog_version is not None, "version_id": catalog_version},
        "findings": findings,
        "summary": {
            "missing_extension_count": len(missing_extensions),
            "schema_applied": schema_applied,
            "catalog_applied": catalog_version is not None,
            "blocker_count": len(findings),
        },
    }


def bootstrap_preflight(
    dsn: str,
    *,
    deployment_name: str | None = None,
) -> dict[str, Any]:
    """Single bootstrap-readiness report: extensions + schema + catalog.

    Per ``canonical-dev-okg``. Names ALL missing prerequisites in one report so
    an operator standing a deployment up never learns one missing object per
    blocked publish. Probes the database, then defers to
    :func:`_bootstrap_readiness`.
    """
    from okg.substrate.db.migrate import REQUIRED_EXTENSIONS

    with db_connect(dsn, connect_timeout=5) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            database = cur.fetchone()[0]
            cur.execute(
                "SELECT extname FROM pg_extension WHERE extname = ANY(%s)",
                (list(REQUIRED_EXTENSIONS),),
            )
            installed = {row[0] for row in cur.fetchall()}
            schema_applied = _regclass_exists(cur, "okg.graph_generations")
            catalog_version: int | None = None
            if _regclass_exists(cur, "okg.catalog_versions"):
                cur.execute("SELECT max(catalog_version_id) FROM okg.catalog_versions")
                row = cur.fetchone()
                catalog_version = row[0] if row and row[0] is not None else None

    return _bootstrap_readiness(
        installed=installed,
        schema_applied=schema_applied,
        catalog_version=catalog_version,
        database=database,
        deployment_name=deployment_name,
    )


def dbos_backlog_gc(
    dsn: str,
    *,
    stale_after_seconds: int = 86400,
    confirm: bool = False,
    queue_name: str | None = None,
    workflow_name: str | None = None,
    statuses: tuple[str, ...] = ("ENQUEUED",),
    sample_limit: int = 20,
) -> dict[str, Any]:
    """Dry-run or cancel stale non-running DBOS workflow rows.

    This is an operator cleanup path for old DBOS rows whose workers are no
    longer alive. It never deletes rows; confirmed cleanup changes eligible
    ``ENQUEUED`` rows to ``CANCELLED`` so DBOS history remains inspectable.
    A persisted ``PENDING`` row may still have a Python body executing; changing
    that row's status cannot interrupt the thread.  Such rows remain visible in
    dry-run output, but confirmed cleanup refuses them until process
    termination is established by the worker restart path.
    """
    if stale_after_seconds < 600:
        raise RequiresConfirm(
            "DBOS backlog cleanup requires a stale window of at least 600 seconds",
            data={"stale_after_seconds": stale_after_seconds},
            fix_hint="rerun with --stale-after-seconds 600 or larger",
        )
    normalized_statuses = tuple(sorted({status.upper() for status in statuses}))
    if not normalized_statuses:
        raise RequiresConfirm(
            "DBOS backlog cleanup requires at least one status",
            fix_hint="use --status PENDING and/or --status ENQUEUED",
        )
    unsupported = set(normalized_statuses) - {"PENDING", "ENQUEUED"}
    if unsupported:
        raise RequiresConfirm(
            "DBOS backlog cleanup only supports non-terminal queued statuses",
            data={"unsupported_statuses": sorted(unsupported)},
            fix_hint="use --status PENDING and/or --status ENQUEUED",
        )

    with db_connect(dsn, connect_timeout=5) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            if not _schema_exists(cur, "dbos") or not _regclass_exists(
                cur, "dbos.workflow_status"
            ):
                return {
                    "present": False,
                    "dry_run": not confirm,
                    "eligible_count": 0,
                    "cancelled_count": 0,
                    "by_status_queue_name": [],
                    "sample": [],
                }
            column_types = _column_types(cur, "dbos", "workflow_status")
            required = {"workflow_uuid", "status"}
            missing = sorted(required - set(column_types))
            if missing:
                raise RequiresConfirm(
                    "dbos.workflow_status is missing required columns",
                    data={"missing_columns": missing},
                    fix_hint="inspect DBOS schema before cleanup",
                )
            if (
                "updated_at" not in column_types
                and "created_at" not in column_types
            ):
                raise RequiresConfirm(
                    "dbos.workflow_status has no updated_at or created_at column",
                    fix_hint="inspect DBOS schema before cleanup",
                )
            stale_expr, stale_column, stale_type = _dbos_stale_anchor(
                column_types
            )
            cutoff_value = _stale_cutoff_value(
                stale_after_seconds,
                stale_type,
            )
            where_sql, params = _dbos_stale_where(
                stale_expr=stale_expr,
                statuses=normalized_statuses,
                cutoff_value=cutoff_value,
                has_queue_name="queue_name" in column_types,
                queue_name=queue_name,
                has_workflow_name="name" in column_types,
                workflow_name=workflow_name,
                has_priority="priority" in column_types,
            )
            queue_expr = (
                sql.SQL("queue_name")
                if "queue_name" in column_types else sql.SQL("NULL::text")
            )
            name_expr = (
                sql.SQL("name")
                if "name" in column_types else sql.SQL("NULL::text")
            )
            executor_expr = _optional_dbos_column_expr(
                column_types,
                "executor_id",
            )
            deduplication_expr = _optional_dbos_column_expr(
                column_types,
                "deduplication_id",
            )
            app_version_expr = _optional_dbos_column_expr(
                column_types,
                "application_version",
            )
            recovery_attempts_expr = _optional_dbos_column_expr(
                column_types,
                "recovery_attempts",
                null_type="bigint",
            )
            inputs_expr = _optional_dbos_column_expr(
                column_types,
                "inputs",
            )
            serialization_expr = _optional_dbos_column_expr(
                column_types,
                "serialization",
            )
            cur.execute(
                sql.SQL(
                    """
                    SELECT status,
                           {queue_expr} AS queue_name,
                           {name_expr} AS name,
                           count(*)::int AS workflow_count,
                           min({stale_expr}) AS oldest_raw,
                           max({stale_expr}) AS newest_raw
                      FROM dbos.workflow_status
                     WHERE {where_sql}
                     GROUP BY status, queue_name, name
                     ORDER BY status, queue_name, name
                    """
                ).format(
                    queue_expr=queue_expr,
                    name_expr=name_expr,
                    stale_expr=stale_expr,
                    where_sql=where_sql,
                ),
                params,
            )
            groups = [
                _dbos_backlog_group(row)
                for row in cur.fetchall()
            ]
            eligible_count = sum(
                int(row["workflow_count"] or 0) for row in groups
            )
            unconfirmed_pending_count = sum(
                int(row["workflow_count"] or 0)
                for row in groups
                if str(row.get("status") or "").upper() == "PENDING"
            )
            cur.execute(
                sql.SQL(
                    """
                    SELECT workflow_uuid,
                           status,
                           {queue_expr} AS queue_name,
                           {name_expr} AS name,
                           {stale_expr} AS stale_raw,
                           {executor_expr} AS executor_id,
                           {deduplication_expr} AS deduplication_id,
                           {app_version_expr} AS application_version,
                           {recovery_attempts_expr} AS recovery_attempts,
                           {inputs_expr} AS inputs,
                           {serialization_expr} AS serialization
                      FROM dbos.workflow_status
                     WHERE {where_sql}
                     ORDER BY {stale_expr}, workflow_uuid
                     LIMIT %s
                    """
                ).format(
                    queue_expr=queue_expr,
                    name_expr=name_expr,
                    stale_expr=stale_expr,
                    executor_expr=executor_expr,
                    deduplication_expr=deduplication_expr,
                    app_version_expr=app_version_expr,
                    recovery_attempts_expr=recovery_attempts_expr,
                    inputs_expr=inputs_expr,
                    serialization_expr=serialization_expr,
                    where_sql=where_sql,
                ),
                [*params, sample_limit],
            )
            sample = [
                _dbos_backlog_sample(
                    row,
                    stale_column,
                    deployment=_deployment_from_dbos_queue_name(queue_name),
                )
                for row in cur.fetchall()
            ]

            cancelled_count = 0
            if confirm and eligible_count:
                if unconfirmed_pending_count:
                    raise RequiresConfirm(
                        "cannot cancel PENDING DBOS workflows because persisted "
                        "status does not confirm Python execution terminated",
                        data={
                            "unconfirmed_pending_count": (
                                unconfirmed_pending_count
                            ),
                            "execution_termination_confirmed": False,
                            "safe_statuses": ["ENQUEUED"],
                        },
                        fix_hint=(
                            "restart the owning worker to terminate running "
                            "execution, or rerun cleanup with --status ENQUEUED"
                        ),
                    )
                set_clauses = [sql.SQL("status = 'CANCELLED'")]
                update_params: list[Any] = []
                if "updated_at" in column_types:
                    set_clauses.append(sql.SQL("updated_at = %s"))
                    update_params.append(
                        _current_dbos_time_value(column_types["updated_at"])
                    )
                cur.execute(
                    sql.SQL(
                        """
                        UPDATE dbos.workflow_status
                           SET {set_sql}
                         WHERE {where_sql}
                        """
                    ).format(
                        set_sql=sql.SQL(", ").join(set_clauses),
                        where_sql=where_sql,
                    ),
                    [*update_params, *params],
                )
                cancelled_count = int(cur.rowcount or 0)
                conn.commit()

    return {
        "present": True,
        "dry_run": not confirm,
        "stale_after_seconds": stale_after_seconds,
        "statuses": list(normalized_statuses),
        "queue_name": queue_name,
        "workflow_name": workflow_name,
        "stale_column": stale_column,
        "eligible_count": eligible_count,
        "cancelled_count": cancelled_count,
        "unconfirmed_pending_count": unconfirmed_pending_count,
        "execution_termination_confirmed": (
            unconfirmed_pending_count == 0
        ),
        "replacement_safe": unconfirmed_pending_count == 0,
        "by_status_queue_name": groups,
        "sample": sample,
    }


_SOURCE_BLOB_TABLES = ("source_cursor_blobs", "source_record_set_blobs")


def prune_orphaned_source_blobs(
    dsn: str,
    *,
    confirm: bool = False,
    grace_seconds: int = 86400,
) -> dict[str, Any]:
    """Delete source blob rows that no live watermark can reach (operator tool).

    ``okg.source_cursor_blobs`` and ``okg.source_record_set_blobs`` keep one row
    per distinct ``(source, sha256)`` content version a source has ever
    produced. The runner only ever hydrates the sha256 embedded in that source's
    current ``okg.source_watermarks`` rev (``last_applied_rev`` /
    ``cutover_watermark`` / ``last_published_rev``); every superseded version is
    unreachable and accrues forever, which is the source-blob disk-growth leak.

    This deletes those orphans (a plain row ``DELETE`` -- no ``VACUUM FULL``, no
    long lock; freed space is reused by future inserts). A blob is *kept* if its
    sha256 appears anywhere in its own source's watermark; a sha256 is a unique
    64-hex string, so the substring match can only ever over-keep, never wrongly
    delete a referenced blob. A blob newer than ``grace_seconds`` is never
    deleted even when unreferenced.

    CONCURRENCY CAVEAT -- run ``confirm=True`` during an ingest lull (or with the
    worker paused). A source run is optimistic: it advances the watermark to the
    new rev (the pool is autocommit), and only rolls *back* to the prior rev if
    admission is later rejected (``ingest.service._restore_source_state``).
    During that narrow window a just-superseded blob looks orphaned while a
    pending restore still needs it, so a concurrent confirmed prune could delete
    a blob a rejected run is about to require. The always-safe *automatic*
    version -- which pins each source's blobs for the duration of its run so the
    GC can run unattended in the maintenance loop -- is a tracked follow-up
    (PACT bound-okg-workspace-disk-growth, task 6 auto-hook).

    Dry-run unless ``confirm`` is set.
    """
    if grace_seconds < 0:
        raise RequiresConfirm(
            "source blob GC requires a non-negative grace window",
            data={"grace_seconds": grace_seconds},
            fix_hint="pass grace_seconds >= 0",
        )
    tables: list[dict[str, Any]] = []
    with db_connect(dsn, connect_timeout=5) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            # Bound any lock wait so a confirmed prune against a live DB can
            # never hang on a contended row; a timeout just surfaces as an error
            # the operator can re-run.
            cur.execute("SET LOCAL lock_timeout = '5s'")
            if not _schema_exists(cur, "okg") or not _regclass_exists(
                cur, "okg.source_watermarks"
            ):
                # Without the watermark table reachability cannot be proven, so
                # never delete: the watermark is the only referrer.
                return {
                    "present": False,
                    "dry_run": not confirm,
                    "grace_seconds": grace_seconds,
                    "orphaned_count": 0,
                    "deleted_count": 0,
                    "tables": [],
                }
            for table in _SOURCE_BLOB_TABLES:
                if not _regclass_exists(cur, f"okg.{table}"):
                    continue
                tables.append(
                    _prune_one_source_blob_table(
                        cur,
                        table=table,
                        confirm=confirm,
                        grace_seconds=grace_seconds,
                    )
                )
            if confirm:
                conn.commit()
    return {
        "present": True,
        "dry_run": not confirm,
        "grace_seconds": grace_seconds,
        "orphaned_count": sum(int(t["orphaned_count"]) for t in tables),
        "deleted_count": sum(int(t["deleted_count"]) for t in tables),
        "tables": tables,
    }


def _prune_one_source_blob_table(
    cur,
    *,
    table: str,
    confirm: bool,
    grace_seconds: int,
) -> dict[str, Any]:
    relation = sql.Identifier("okg", table)
    # A blob is an orphan when it is older than the grace window AND no rev of
    # its own source's watermark mentions its sha256. ``position`` over the
    # rev text is path-agnostic, so a referenced sha256 can never be missed.
    orphan_where = sql.SQL(
        """
        b.updated_at < now() - make_interval(secs => %s)
        AND NOT EXISTS (
          SELECT 1 FROM okg.source_watermarks sw
          WHERE sw.source = b.source
            AND (position(b.sha256 in sw.last_applied_rev::text) > 0
              OR position(b.sha256 in sw.cutover_watermark::text) > 0
              OR position(b.sha256 in sw.last_published_rev::text) > 0)
        )
        """
    )
    if confirm:
        cur.execute(
            sql.SQL("DELETE FROM {rel} AS b WHERE {where}").format(
                rel=relation, where=orphan_where
            ),
            (grace_seconds,),
        )
        deleted = int(cur.rowcount or 0)
        return {"table": table, "orphaned_count": deleted, "deleted_count": deleted}
    cur.execute(
        sql.SQL(
            "SELECT count(*)::bigint AS n FROM {rel} AS b WHERE {where}"
        ).format(rel=relation, where=orphan_where),
        (grace_seconds,),
    )
    row = cur.fetchone()
    orphaned = int(row["n"] if isinstance(row, dict) else row[0])
    return {"table": table, "orphaned_count": orphaned, "deleted_count": 0}


def _assert_usable_dump_reference(dump_ref: str | None) -> None:
    """Refuse a dump reference that is not a real, non-empty artifact.

    The reference is the recorded way back from a destructive decision. An
    unchecked string is not a way back -- it is the same "recorded claim nobody
    verified" shape as the postcondition in issue #1054. A remote URI cannot be
    read from here, so it is accepted only for a recognized scheme; a local path
    must exist and be non-empty.
    """
    reference = (dump_ref or "").strip()
    if not reference:
        raise SubstrateError(
            "accepting retired job-runtime ledger rows requires a pre-drop dump "
            "reference",
            data={"accept_retired_ledger_rows": True},
            fix_hint=(
                "dump the legacy job_* tables first and pass the dump path or "
                "URI so the destroyed rows are recoverable and the decision is "
                "auditable"
            ),
        )
    scheme = re.match(r"^([A-Za-z][A-Za-z0-9+.-]*)://", reference)
    if scheme:
        if scheme.group(1).lower() not in {"s3", "gs", "https", "file"}:
            raise SubstrateError(
                "the pre-drop dump reference uses an unrecognized URI scheme",
                data={"retired_rows_dump_ref": reference, "scheme": scheme.group(1)},
                fix_hint=(
                    "use a local path, or an s3://, gs://, https://, or file:// "
                    "URI this substrate can name in an audit record"
                ),
            )
        return
    path = Path(reference).expanduser()
    if not path.is_file():
        raise SubstrateError(
            "the pre-drop dump reference does not name a readable file",
            data={"retired_rows_dump_ref": reference},
            fix_hint=(
                "pass the path of the dump you actually took, or a remote URI; "
                "an unverifiable reference is not a way back"
            ),
        )
    if path.stat().st_size == 0:
        raise SubstrateError(
            "the pre-drop dump reference names an empty file",
            data={"retired_rows_dump_ref": str(path)},
            fix_hint="re-run the dump; an empty dump restores nothing",
        )


def _destroyed_job_runtime_row_counts(cur: psycopg.Cursor) -> list[dict[str, Any]]:
    """Per-relation, per-status counts of every row the drop will destroy.

    Not just the ACTIVE rows that triggered the refusal: 0002 drops whole
    tables, so `job_definitions`, `job_dependencies`, `job_item_frontiers` and
    `job_external_calls` -- and every non-active row in the rest -- go too. An
    audit record that lists only the rows that blocked the migration understates
    what the decision cost.
    """
    counts: list[dict[str, Any]] = []
    for relation in JOB_RUNTIME_TABLES:
        if not _regclass_exists(cur, relation):
            continue
        schema_name, table_name = relation.split(".", 1)
        identifier = sql.Identifier(schema_name, table_name)
        cur.execute(
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s AND column_name = 'status')"
            " AS has_status",
            (schema_name, table_name),
        )
        row = cur.fetchone()
        has_status = bool(row["has_status"] if isinstance(row, dict) else row[0])
        if has_status:
            cur.execute(
                sql.SQL(
                    "SELECT status, count(*)::bigint AS row_count FROM {} "
                    "GROUP BY status ORDER BY status"
                ).format(identifier)
            )
            counts.extend(
                {"relation": relation, **_jsonify_row(entry)}
                for entry in cur.fetchall()
            )
            continue
        cur.execute(
            sql.SQL("SELECT count(*)::bigint AS row_count FROM {}").format(identifier)
        )
        counts.append(
            {"relation": relation, "status": None, **_jsonify_row(cur.fetchone())}
        )
    return counts


# Flags 0002 evaluates after its DDL. Declared here and evaluated through the
# postcondition registry so the same vocabulary is used to record the claim and
# to re-check it later (issue #1054) -- a flag cannot be emitted that no one can
# re-verify.
REMOVE_JOB_RUNTIME_SUBSYSTEM_POSTCONDITION_FLAGS = (
    "reconciliation_view_absent",
    "job_queue_view_absent",
    "reconciliation_function_absent",
    "legacy_jobs_absent",
    "publisher_leases_absent",
    "legacy_jobs_dedupe_index_absent",
    "workload_node_source_index_absent",
    "workload_edge_source_index_absent",
    "workload_source_pattern_index_absent",
    "legacy_retirement_marker_absent",
    "job_definitions_absent",
    "job_dependencies_absent",
    "job_runs_absent",
    "job_chunks_absent",
    "job_items_absent",
    "job_item_frontiers_absent",
    "job_external_calls_absent",
    "retirement_marker_absent",
    "lock_inventory_present",
    "stale_lock_view_present",
    "job_chunk_inventory_absent",
    "legacy_cron_schedules_absent",
)


def apply_remove_job_runtime_subsystem(
    dsn: str,
    *,
    confirm: bool = False,
    accept_retired_ledger_rows: bool = False,
    retired_rows_dump_ref: str | None = None,
) -> dict[str, Any]:
    """Drop the obsolete job-runtime subsystem.

    ``accept_retired_ledger_rows`` is the audited escape from the trap in issue
    #1054: the default refusal on ``queued``/``running``/``retryable`` legacy
    rows assumes a worker exists to drain them, and for this subsystem none
    does, so on okg-workspace 1,324 retryable rows made the cleanup permanently
    unsatisfiable. Taking the decision relaxes ONLY that refusal -- the DBOS
    drain check, the advisory lock, the restrictive drops, and every
    postcondition still apply -- and requires ``retired_rows_dump_ref`` naming a
    pre-drop dump, so nothing is destroyed without a recorded way back. What it
    destroyed is written into the migration's ledger metadata.
    """
    if accept_retired_ledger_rows:
        _assert_usable_dump_reference(retired_rows_dump_ref)
    if not confirm:
        raise RequiresConfirm(
            "removing the obsolete OKG job-runtime subsystem requires --confirm",
            fix_hint=(
                "rerun with --confirm --apply only after confirming DBOS "
                "workflow status and source-sync progress are the runtime "
                "authorities"
            ),
        )

    migration = _known_migration(REMOVE_JOB_RUNTIME_SUBSYSTEM_MIGRATION_ID)
    retire_sql = importlib.resources.files("okg.substrate.db").joinpath(
        "migrations/0002_remove_job_runtime_subsystem.sql"
    ).read_text()

    with db_connect(dsn, connect_timeout=5) as conn:
        ensure_schema_migrations(conn)
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("SET LOCAL lock_timeout = '5s'")
            cur.execute("SET LOCAL statement_timeout = '60s'")
            cur.execute(
                "SELECT pg_try_advisory_xact_lock(hashtextextended(%s, 0)) "
                "AS acquired",
                ("okg:remove-job-runtime-subsystem",),
            )
            if not bool(cur.fetchone()["acquired"]):
                raise SubstrateError(
                    "another job-runtime cleanup is already in progress",
                    fix_hint="wait for the other cleanup transaction to finish",
                )

            if not job_runtime_subsystem_present(conn):
                result = evaluate_postcondition(cur, ("job_runtime_subsystem_absent",))
                record_known_migration(
                    conn,
                    migration,
                    result_metadata={
                        "applied_via": "okg db remove-job-runtime-subsystem",
                        "postcondition": result,
                        "already_absent": True,
                    },
                )
                conn.commit()
                return {
                    "applied": False,
                    "already_absent": True,
                    "migration": migration.to_dict(),
                    "preflight": {"required": False},
                    "postcondition": result,
                }

            if not _regclass_exists(cur, "dbos.workflow_status"):
                raise SubstrateError(
                    "job-runtime cleanup cannot verify DBOS workflow state",
                    data={"missing_relation": "dbos.workflow_status"},
                    fix_hint=(
                        "deploy and initialize the DBOS replacement runtime, "
                        "then retry after its workflows are drained"
                    ),
                )
            workflow_columns = _column_types(cur, "dbos", "workflow_status")
            missing_workflow_columns = sorted(
                {"name", "status"} - set(workflow_columns)
            )
            if missing_workflow_columns:
                raise SubstrateError(
                    "job-runtime cleanup cannot verify DBOS workflow state",
                    data={
                        "relation": "dbos.workflow_status",
                        "missing_columns": missing_workflow_columns,
                    },
                    fix_hint=(
                        "repair or migrate the DBOS runtime schema before cleanup"
                    ),
                )
            # Prevent an enqueue/write race between the drain check and the
            # destructive transaction. A stopped and drained worker should make
            # this lock immediate; otherwise lock_timeout fails closed.
            cur.execute("LOCK TABLE dbos.workflow_status IN SHARE MODE")
            cur.execute(
                """
                SELECT name, status, count(*)::int AS workflow_count
                  FROM dbos.workflow_status
                 WHERE status IN ('PENDING', 'ENQUEUED', 'DELAYED')
                   AND name IN (
                     'okg_source_sync',
                     'okg_generic_workload',
                     'okg_publisher_cycle'
                   )
                 GROUP BY name, status
                 ORDER BY name, status
                """
            )
            active_dbos = [_jsonify_row(row) for row in cur.fetchall()]
            if active_dbos:
                raise SubstrateError(
                    "job-runtime cleanup requires DBOS runtime work to be drained",
                    data={"active_dbos_workflows": active_dbos},
                    fix_hint=(
                        "stop old runtime workers, drain source-sync/publisher work, "
                        "deploy the new worker code, then retry"
                    ),
                )

            # Rebind canonical runtime views before restrictive table drops. This
            # removes known legacy dependencies while preserving unrelated views.
            from okg.substrate.db.migrate import (
                _close_function_owner_transfer,
                _executable_views_sql,
                _open_function_owner_transfer,
                _views_sql,
            )

            # views.sql hands the external-action helpers to their owner
            # role, which needs CREATE on okg for the length of this
            # transaction only (issue #2239).
            _open_function_owner_transfer(cur)
            cur.execute(_executable_views_sql(conn, _views_sql()))
            _close_function_owner_transfer(cur)

            existing_tables = [
                relation
                for relation in JOB_RUNTIME_TABLES
                if _regclass_exists(cur, relation)
            ]
            for relation in existing_tables:
                schema_name, table_name = relation.split(".", 1)
                cur.execute(
                    sql.SQL("LOCK TABLE {} IN ACCESS EXCLUSIVE MODE").format(
                        sql.Identifier(schema_name, table_name)
                    )
                )

            active_legacy: list[dict[str, Any]] = []
            for relation, active_statuses in (
                ("okg.job_runs", ["queued", "running", "retryable"]),
                ("okg.job_chunks", ["queued", "running", "retryable"]),
                ("okg.job_items", ["queued", "running", "retryable"]),
                ("okg.jobs", ["queued", "leased"]),
            ):
                if not _regclass_exists(cur, relation):
                    continue
                schema_name, table_name = relation.split(".", 1)
                cur.execute(
                    sql.SQL(
                        "SELECT status, count(*)::int AS row_count FROM {} "
                        "WHERE status = ANY(%s) "
                        "GROUP BY status ORDER BY status"
                    ).format(sql.Identifier(schema_name, table_name)),
                    (active_statuses,),
                )
                active_legacy.extend(
                    {
                        "relation": relation,
                        **_jsonify_row(row),
                    }
                    for row in cur.fetchall()
                )
            if active_legacy and not accept_retired_ledger_rows:
                raise SubstrateError(
                    "job-runtime cleanup refuses active legacy rows",
                    data={"active_legacy_rows": active_legacy},
                    fix_hint=(
                        "drain or explicitly cancel legacy work before destructive "
                        "cleanup. This subsystem has no worker left to drain it, so "
                        "if these rows are retired residue, dump them and rerun with "
                        "accept_retired_ledger_rows plus a dump reference: the "
                        "destroyed per-relation row counts and that reference are "
                        "recorded in the migration ledger"
                    ),
                )

            destroyed_rows: list[dict[str, Any]] = []
            if active_legacy:
                destroyed_rows = _destroyed_job_runtime_row_counts(cur)
            cur.execute(retire_sql)
            # Evaluated through the shared postcondition registry, so every flag
            # recorded here has a declared way to be re-checked against the
            # database later (issue #1054).
            result = evaluate_postcondition(
                cur, REMOVE_JOB_RUNTIME_SUBSYSTEM_POSTCONDITION_FLAGS
            )
            obsolete_cron_schedules = job_runtime_cron_schedules(cur)
            if not all(bool(value) for value in result.values()):
                raise SubstrateError(
                    "job-runtime cleanup postconditions failed",
                    data={
                        "postcondition": result,
                        "obsolete_cron_schedules": obsolete_cron_schedules,
                    },
                    fix_hint="restore from backup and inspect remaining dependencies",
                )
        metadata: dict[str, Any] = {
            "applied_via": "okg db remove-job-runtime-subsystem",
            "postcondition": result,
        }
        if active_legacy:
            # The decision and what it cost, recorded where an auditor looks:
            # the rows that blocked it AND every row the drop actually removed.
            metadata["accepted_retired_ledger_rows"] = active_legacy
            metadata["destroyed_row_counts"] = destroyed_rows
            metadata["retired_rows_dump_ref"] = retired_rows_dump_ref
        record_known_migration(conn, migration, result_metadata=metadata)
        conn.commit()

    return {
        "applied": True,
        "migration": migration.to_dict(),
        "preflight": {
            "dbos_workflow_status_available": True,
            "active_dbos_workflows": active_dbos,
            "active_legacy_rows": active_legacy,
        },
        "accepted_retired_ledger_rows": active_legacy if active_legacy else [],
        "destroyed_row_counts": destroyed_rows,
        "retired_rows_dump_ref": retired_rows_dump_ref if active_legacy else None,
        "postcondition": result,
    }


def migrate_down(
    dsn: str,
    migration_id: str,
    *,
    confirm: bool = False,
) -> dict[str, Any]:
    """Reverse an applied migration by replaying its recorded inverse.

    Replays the migration's ``down_sql`` and removes its
    ``okg.schema_migrations`` ledger row. Refuses **loudly** (raises)
    rather than no-op'ing when:

      * the migration id is unknown,
      * the migration has no ``.down.sql`` inverse (e.g. the whole-schema
        snapshot), or
      * the migration is not currently recorded as applied.

    Destructive inverses (recreating dropped tables) require ``--confirm``.
    """
    try:
        migration = _known_migration(migration_id)
    except KeyError:
        raise SubstrateError(
            f"unknown migration id {migration_id!r}",
            data={
                "migration_id": migration_id,
                "known": [m.migration_id for m in current_known_migrations()],
            },
            fix_hint="pass a migration id from `okg db migrations`",
        ) from None

    if not migration.reversible:
        # Fail loud — never silently succeed on a non-reversible migration.
        raise SubstrateError(
            f"migration {migration_id!r} has no reversible inverse",
            data={"migration_id": migration_id, "reversible": False},
            fix_hint=(
                "this migration cannot be rolled back automatically "
                "(no .down.sql inverse exists); restore from a backup "
                "if you must undo it"
            ),
        )

    if migration.destructive and not confirm:
        raise RequiresConfirm(
            f"reversing {migration_id!r} requires --confirm",
            data={"migration_id": migration_id, "destructive": True},
            fix_hint=(
                "the inverse recreates dropped relations; rerun with "
                "--confirm --apply once you are sure"
            ),
        )

    down_sql = migration.down_sql()

    with db_connect(dsn, connect_timeout=5) as conn:
        ensure_schema_migrations(conn)
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                "SELECT status FROM okg.schema_migrations "
                "WHERE migration_id = %s",
                (migration_id,),
            )
            ledger_row = cur.fetchone()
            if ledger_row is None or ledger_row.get("status") != "applied":
                raise SubstrateError(
                    f"migration {migration_id!r} is not recorded as applied",
                    data={
                        "migration_id": migration_id,
                        "ledger_status": (
                            ledger_row.get("status") if ledger_row else None
                        ),
                    },
                    fix_hint=(
                        "nothing to reverse; inspect `okg db migrations` "
                        "to confirm the ledger state"
                    ),
                )
            cur.execute(down_sql)
            cur.execute(
                "DELETE FROM okg.schema_migrations WHERE migration_id = %s",
                (migration_id,),
            )
        conn.commit()

    return {
        "reversed": True,
        "migration": migration.to_dict(),
        "ledger_row_removed": True,
    }


# Exact name-prefix allowlist for the test-database janitor's apply path
# (bound-host-storage-pressure req.host.workspace-hygiene). Deliberately
# narrow: `okg_test_` requires the underscore (the bare `okg_test` default
# dev DB survives), and live deployment names such as `okg_workspace` match
# nothing here. The heuristic `classification` column is informational only
# and never drives a drop.
TEST_DATABASE_GC_ALLOWLIST_PREFIXES: tuple[str, ...] = (
    "okg_test_",
    "okg_upgrade_verify_",
    "okg_golden",
)

# Positive registration for ephemeral databases (operator hardening,
# 2026-07-17): a name prefix is a CANDIDATE FILTER, never drop authority.
# Every ephemeral-database creator stamps this single-row marker table at
# creation; the janitor drops ONLY databases carrying the marker. A
# prefix-matching database WITHOUT the marker is reported loudly and always
# survives — a production database that merely resembles a test name can
# never be swept.
EPHEMERAL_MARKER_TABLE = "okg_ephemeral_database"


def stamp_ephemeral_database(
    conn: Any, *, registered_by: str, purpose: str
) -> None:
    """Register the CURRENT database as ephemeral (droppable by the janitor).

    Idempotent; call at creation time from every ephemeral-DB creator
    (test fixtures, harness scratch DBs, upgrade verify DBs). The marker
    lives IN the database so registration travels with it and dies with
    it — no external registry to drift.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS public.{EPHEMERAL_MARKER_TABLE} (
                singleton      boolean PRIMARY KEY DEFAULT TRUE
                               CHECK (singleton),
                registered_by  text NOT NULL,
                purpose        text NOT NULL,
                registered_at  timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        cur.execute(
            f"""
            INSERT INTO public.{EPHEMERAL_MARKER_TABLE}
                (registered_by, purpose)
            VALUES (%s, %s)
            ON CONFLICT (singleton) DO NOTHING
            """,
            (registered_by, purpose),
        )


def database_is_registered_ephemeral(
    cluster_dsn: str, database: str
) -> bool | None:
    """Whether ``database`` on the instance carries the ephemeral marker.

    Returns ``True``/``False`` from a real read, or ``None`` when the
    database cannot be checked (connection failure) — the janitor treats
    ``None`` exactly like unregistered: never dropped, loudly reported.
    """
    dsn = dsn_for_database(cluster_dsn, database)
    try:
        with db_connect(dsn, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT to_regclass(%s)",
                    (f"public.{EPHEMERAL_MARKER_TABLE}",),
                )
                if cur.fetchone()[0] is None:
                    return False
                cur.execute(
                    f"SELECT count(*) FROM public.{EPHEMERAL_MARKER_TABLE}"
                )
                return bool(cur.fetchone()[0])
    except psycopg.Error:
        return None


def allowlisted_test_databases(names: Any) -> list[str]:
    """Names matching the exact janitor prefix allowlist, sorted."""
    return sorted(
        name
        for name in names
        if any(
            str(name).startswith(prefix)
            for prefix in TEST_DATABASE_GC_ALLOWLIST_PREFIXES
        )
    )


def gc_sweep(
    *,
    cluster_dsn: str,
    apply: bool = False,
    deployments_root: Path | None = None,
) -> dict[str, Any]:
    """Sweep ephemeral test databases on one explicit instance.

    Dry-run by default: reports every database with its size, the
    allowlist-prefixed candidates (``okg_test_*``, ``okg_upgrade_verify_*``,
    ``okg_golden*``), and the legacy classification-based candidate listing
    (informational only). The prefix is only the CANDIDATE FILTER — drop
    authority is the in-database ephemeral marker
    (:data:`EPHEMERAL_MARKER_TABLE`, stamped by every ephemeral-DB creator
    at creation). With ``apply=True`` it drops ONLY candidates that are
    REGISTERED ephemeral, routing each drop through :func:`drop_database`
    so the protected-name and manifest-ownership refusals still hold per
    database. A prefix-matching database WITHOUT the marker (or that cannot
    be checked) is reported under ``unregistered_prefix_matches`` and always
    survives. Non-allowlisted databases (e.g. ``okg_workspace``) always
    survive; per-database failures are recorded and the sweep continues.
    """
    before_count = _database_count(cluster_dsn)
    inventory = list_databases(
        cluster_dsn=cluster_dsn,
        deployments_root=deployments_root,
    )
    rows = inventory["databases"]
    sizes = {
        str(row.get("database")): int(row.get("size_bytes") or 0)
        for row in rows
    }
    allowlisted = set(allowlisted_test_databases(sizes))
    registered: list[dict[str, Any]] = []
    unregistered: list[dict[str, Any]] = []
    for name in sorted(allowlisted):
        mark = database_is_registered_ephemeral(cluster_dsn, name)
        row = {"database": name, "size_bytes": sizes[name]}
        if mark is True:
            registered.append(row)
        else:
            unregistered.append({
                **row,
                "reason": (
                    "no ephemeral marker" if mark is False
                    else "marker check failed (connection error)"
                ),
            })
    allowlist_candidates = registered
    non_allowlisted_survivors = [
        {"database": name, "size_bytes": sizes[name]}
        for name in sorted(sizes)
        if name not in allowlisted
    ]
    legacy_candidates = [
        row for row in rows
        if row.get("classification") in {"dev", "test", "bench", "scratch"}
        and not row.get("manifest_owners")
    ]
    payload: dict[str, Any] = {
        "dry_run": not apply,
        "applied": False,
        "allowlist_prefixes": list(TEST_DATABASE_GC_ALLOWLIST_PREFIXES),
        "database_count_before": before_count,
        "cluster_size_bytes_before": sum(sizes.values()),
        "allowlist_candidate_count": len(allowlist_candidates),
        "allowlist_candidate_bytes": sum(
            row["size_bytes"] for row in allowlist_candidates
        ),
        "allowlist_candidates": allowlist_candidates,
        "unregistered_prefix_matches": unregistered,
        "non_allowlisted_survivors": non_allowlisted_survivors,
        "candidate_count": len(legacy_candidates),
        "candidates": legacy_candidates,
    }
    if not apply:
        payload["database_count_after"] = _database_count(cluster_dsn)
        payload["hint"] = (
            "re-run with --apply to drop the REGISTERED candidates; "
            "unregistered prefix matches and non-allowlisted databases "
            "are never dropped"
        )
        return payload

    dropped: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    for row in allowlist_candidates:
        name = row["database"]
        try:
            result = drop_database(
                cluster_dsn=cluster_dsn,
                database=name,
                confirm=True,
                apply=True,
                deployments_root=deployments_root,
            )
        except (SubstrateError, psycopg.Error) as exc:
            # One refused/failed drop (active connections, manifest-owned
            # name, ...) must not abort the rest of the sweep.
            failed.append({
                "database": name,
                "size_bytes": row["size_bytes"],
                "error": f"{type(exc).__name__}: "
                         f"{getattr(exc, 'message', None) or exc}",
            })
            continue
        dropped.append({**row, "result": result})

    after_count = _database_count(cluster_dsn)
    after_sizes: dict[str, int] = {}
    with db_connect(cluster_dsn, connect_timeout=5) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT datname, pg_database_size(datname) FROM pg_database "
                "WHERE datistemplate = false ORDER BY datname"
            )
            for datname, size_bytes in cur.fetchall():
                after_sizes[str(datname)] = int(size_bytes or 0)
    payload.update({
        "applied": True,
        "dropped_count": len(dropped),
        "freed_bytes": sum(row["size_bytes"] for row in dropped),
        "dropped": dropped,
        "failed": failed,
        "database_count_after": after_count,
        "cluster_size_bytes_after": sum(after_sizes.values()),
        "databases_after": [
            {"database": name, "size_bytes": size}
            for name, size in sorted(after_sizes.items())
        ],
    })
    return payload


def drop_database(
    *,
    cluster_dsn: str,
    database: str,
    confirm: bool,
    apply: bool = False,
    override_deployment: str | None = None,
    deployments_root: Path | None = None,
) -> dict[str, Any]:
    if database in {"postgres", "okg"}:
        raise RequiresConfirm(
            "refusing to drop protected database name",
            data={"database": database},
            fix_hint=(
                "use a scratch/test/bench database name; do not drop "
                "canonical deployment or maintenance databases through "
                "this helper"
            ),
        )
    manifests = discover_manifest_databases(deployments_root=deployments_root)
    owners = [m.deployment for m in manifests if m.database == database]
    if owners and override_deployment not in owners:
        raise RequiresConfirm(
            "refusing to drop manifest-owned deployment database",
            data={"database": database, "owners": owners},
            fix_hint=(
                "pass --override-deployment <owner> only after backing up "
                "and confirming the deployment is disposable"
            ),
        )
    # Convention point (d): dry-run is the default. --apply is required to
    # execute the irreversible DROP; --confirm alone no longer drops (#694).
    if not apply:
        return {
            "dry_run": True,
            "applied": False,
            "would_drop": database,
            "manifest_owners": owners,
            "hint": "re-run with --confirm --apply to drop the database",
        }
    if not confirm:
        raise RequiresConfirm(
            "dropping a database requires --confirm",
            data={"database": database},
            fix_hint="rerun with --confirm --apply and an explicit --name",
        )
    with db_connect(cluster_dsn, autocommit=True, connect_timeout=5) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("DROP DATABASE {}").format(sql.Identifier(database))
            )
    return {"dropped": database, "applied": True, "manifest_owners": owners}


def database_name_from_dsn(dsn: str) -> str | None:
    # A malformed DSN gives None: callers treat it as "couldn't parse".
    parsed = dsn_mod.try_parse(dsn)
    return parsed.dbname if parsed is not None else None


def dsn_for_database(base_dsn: str, database: str) -> str:
    """``base_dsn`` pointed at ``database``, in the same form as ``base_dsn``.

    Every other parameter (``sslmode``, ``options``, ``application_name``, the
    password) is kept.
    """
    return dsn_mod.parse(base_dsn).with_dbname(database).render()


def _known_migration(migration_id: str) -> KnownMigration:
    for migration in current_known_migrations():
        if migration.migration_id == migration_id:
            return migration
    raise KeyError(migration_id)


def _cluster_inventory(
    cluster_dsn: str,
    manifest_by_db: dict[str, list[ManifestDatabase]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with db_connect(cluster_dsn, connect_timeout=5) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT datname AS database,
                       pg_database_size(datname) AS size_bytes,
                       pg_get_userbyid(datdba) AS owner,
                       datistemplate
                  FROM pg_database
                 WHERE datistemplate = false
                 ORDER BY datname
                """
            )
            dbs = cur.fetchall()
    for row in dbs:
        database = str(row["database"])
        owners = manifest_by_db.get(database, [])
        entry = {
            "database": database,
            "size_bytes": int(row["size_bytes"] or 0),
            "owner": row.get("owner"),
            "classification": _classify_database(database, owners),
            "manifest_owners": [owner.deployment for owner in owners],
            "runtime_modes": sorted({owner.runtime_mode for owner in owners}),
            "detected_deployments": [],
            "latest_published_generation_id": None,
            "warnings": [],
        }
        try:
            detail = _inventory_probe(
                dsn_for_database(cluster_dsn, database),
                manifest_by_db.get(database, []),
            )
            entry.update(detail)
        except Exception as exc:
            entry["warnings"].append(
                {
                    "code": "inventory_probe_failed",
                    "message": f"{type(exc).__name__}: {exc}",
                }
            )
        rows.append(_jsonify_row(entry))
    return rows


def _inventory_entry_for_manifest_group(
    database: str,
    manifests: list[ManifestDatabase],
) -> dict[str, Any]:
    manifest = manifests[0]
    entry = {
        "database": database,
        "classification": "deployment",
        "manifest_owners": [owner.deployment for owner in manifests],
        "runtime_modes": sorted(
            {owner.runtime_mode for owner in manifests}
        ),
        "manifest_paths": [owner.manifest_path for owner in manifests],
        "warnings": [],
    }
    try:
        entry.update(
            _inventory_probe(
                manifest.dsn,
                manifests,
            )
        )
    except Exception as exc:
        entry["warnings"].append(
            {
                "code": "inventory_probe_failed",
                "message": f"{type(exc).__name__}: {exc}",
            }
        )
    return _jsonify_row(entry)


def _inventory_probe(
    dsn: str,
    manifests: list[ManifestDatabase],
) -> dict[str, Any]:
    with db_connect(dsn, connect_timeout=5) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            database = _current_database(cur)
            detected = _detected_deployments(cur)
            non_bootstrap = [d for d in detected if d != "bootstrap"]
            warnings = []
            if len(non_bootstrap) > 1:
                warnings.append(
                    {
                        "code": "mixed_deployment_state",
                        "deployments": non_bootstrap,
                    }
                )
            return {
                "database": database,
                "size_bytes": _database_size(cur, database),
                "owner": _current_owner(cur, database),
                "classification": _classify_database(database, manifests),
                "detected_deployments": detected,
                "latest_published_generation_id": _latest_published_generation(cur),
                "warnings": warnings,
            }


def _migration_status_from_cursor(cur) -> dict[str, Any]:
    from okg.substrate.db.multiphase import multiphase_in_progress_rows

    known = current_known_migrations()
    if not _regclass_exists(cur, "okg.schema_migrations"):
        return {
            "ledger_exists": False,
            "applied": [],
            "pending": [m.to_dict() for m in known],
            "checksum_mismatches": [],
            "checksum_compatibilities": [],
            "destructive_pending": [
                m.to_dict() for m in known if m.destructive
            ],
            "unknown_applied": [],
            "multiphase_in_progress": multiphase_in_progress_rows(cur),
            "multiphase_pending": _multiphase_pending_rows(
                [m.to_dict() for m in known]
            ),
            "postcondition_drift": [],
            "ok": False,
        }
    cur.execute(
        """
        SELECT migration_id, checksum, description, destructive,
               applied_at, applied_by, status, result_metadata
          FROM okg.schema_migrations
         ORDER BY applied_at, migration_id
        """
    )
    applied = [_jsonify_row(row) for row in cur.fetchall()]
    postcondition_drift = _postcondition_drift_rows(cur, applied)
    applied_by_id = {str(row["migration_id"]): row for row in applied}
    pending = []
    mismatches = []
    checksum_compatibilities = []
    for migration in known:
        row = applied_by_id.get(migration.migration_id)
        if row is None:
            pending.append(migration.to_dict())
        elif row.get("checksum") != migration.checksum:
            compatibility = _migration_checksum_compatibility(
                migration, str(row.get("checksum") or "")
            )
            if compatibility is not None:
                checksum_compatibilities.append(compatibility)
            else:
                mismatches.append(
                    {
                        "migration_id": migration.migration_id,
                        "recorded_checksum": row.get("checksum"),
                        "expected_checksum": migration.checksum,
                        "destructive": migration.destructive,
                    }
                )
    return {
        "ledger_exists": True,
        "applied": applied,
        "pending": pending,
        "checksum_mismatches": mismatches,
        "checksum_compatibilities": checksum_compatibilities,
        "destructive_pending": [
            row for row in pending if bool(row.get("destructive"))
        ],
        "unknown_applied": _unknown_applied_rows(applied, known),
        "multiphase_in_progress": multiphase_in_progress_rows(cur),
        "multiphase_pending": _multiphase_pending_rows(pending),
        # Same re-check as `migration_status`. Doctor and `okg up` preflight read
        # THIS function; two status surfaces disagreeing about whether the
        # ledger is trustworthy is the "two disagreeing gates" defect, so both
        # carry the drift and both fail on it.
        "postcondition_drift": postcondition_drift,
        "ok": not mismatches and not _blocking_postcondition_drift(
            postcondition_drift
        ),
    }


def _generation_summary(cur) -> dict[str, Any]:
    if not _regclass_exists(cur, "okg.graph_generations"):
        return {"present": False, "detected_deployments": []}
    cur.execute(
        """
        SELECT deployment_name, count(*)::int AS generation_count
          FROM okg.graph_generations
         GROUP BY deployment_name
         ORDER BY deployment_name
        """
    )
    by_deployment = [_jsonify_row(row) for row in cur.fetchall()]
    cur.execute(
        """
        SELECT status, count(*)::int AS generation_count
          FROM okg.graph_generations
         GROUP BY status
         ORDER BY status
        """
    )
    by_status = [_jsonify_row(row) for row in cur.fetchall()]
    cur.execute(
        """
        SELECT generation_id, deployment_name, status, catalog_version_id,
               published_at, blocked_reason
          FROM okg.graph_generations
         ORDER BY generation_id DESC
         LIMIT 1
        """
    )
    latest = cur.fetchone()
    return {
        "present": True,
        "detected_deployments": [
            row["deployment_name"] for row in by_deployment
        ],
        "by_deployment": by_deployment,
        "by_status": by_status,
        "latest": _jsonify_row(latest) if latest else None,
    }


def _source_watermark_summary(cur) -> dict[str, Any]:
    if not _regclass_exists(cur, "okg.source_watermarks"):
        return {"present": False, "count": 0}
    cur.execute("SELECT count(*)::int AS count FROM okg.source_watermarks")
    return {"present": True, "count": int(cur.fetchone()["count"])}


def _top_relations(cur, *, limit: int) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT n.nspname AS schema_name,
               c.relname AS relation_name,
               c.relkind,
               pg_total_relation_size(c.oid) AS total_bytes,
               pg_relation_size(c.oid) AS heap_bytes,
               pg_indexes_size(c.oid) AS index_bytes
          FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname IN ('okg', 'okg_derived', 'dbos', '_timescaledb_internal')
           AND c.relkind IN ('r', 'm', 'p', 'i')
         ORDER BY pg_total_relation_size(c.oid) DESC
         LIMIT %s
        """,
        (limit,),
    )
    rows = []
    for row in cur.fetchall():
        item = _jsonify_row(row)
        item["relation"] = f"{item['schema_name']}.{item['relation_name']}"
        item["family"] = _relation_family(
            str(item["schema_name"]),
            str(item["relation_name"]),
        )
        rows.append(item)
    return rows


def _timescale_summary(cur) -> dict[str, Any]:
    if not _schema_exists(cur, "timescaledb_information"):
        return {"present": False, "hypertables": []}
    try:
        cur.execute(
            """
            SELECT h.hypertable_schema,
                   h.hypertable_name,
                   h.num_chunks,
                   h.compression_enabled,
                   count(j.*)::int AS policy_count
              FROM timescaledb_information.hypertables h
              LEFT JOIN timescaledb_information.jobs j
                ON j.hypertable_schema = h.hypertable_schema
               AND j.hypertable_name = h.hypertable_name
             WHERE h.hypertable_schema IN ('okg', 'okg_derived')
             GROUP BY h.hypertable_schema, h.hypertable_name,
                      h.num_chunks, h.compression_enabled
             ORDER BY h.hypertable_schema, h.hypertable_name
            """
        )
        return {
            "present": True,
            "hypertables": [_jsonify_row(row) for row in cur.fetchall()],
        }
    except Exception as exc:
        return {
            "present": True,
            "error": f"{type(exc).__name__}: {exc}",
            "hypertables": [],
        }


def _obsolete_job_runtime_summary(cur) -> dict[str, Any]:
    objects_present = [
        relation
        for relation in JOB_RUNTIME_RELATIONS
        if _regclass_exists(cur, relation)
    ]
    objects_present.extend(
        procedure
        for procedure in JOB_RUNTIME_PROCEDURES
        if _regprocedure_exists(cur, procedure)
    )
    cron_schedules_present = job_runtime_cron_schedules(cur)
    return {
        "cleanup_required": bool(objects_present or cron_schedules_present),
        "objects_present": objects_present,
        "cron_schedules_present": cron_schedules_present,
    }


def _dbos_workflow_summary(cur) -> dict[str, Any]:
    if not _schema_exists(cur, "dbos") or not _regclass_exists(
        cur, "dbos.workflow_status"
    ):
        return {"present": False, "by_status": []}
    column_types = _column_types(cur, "dbos", "workflow_status")
    columns = set(column_types)
    total_bytes = _relation_total_bytes(cur, "dbos.workflow_status")
    if "status" not in columns:
        return {
            "present": True,
            "workflow_status_total_bytes": total_bytes,
            "by_status": [],
            "warning": "no status column",
        }
    observed_column = None
    for candidate in ("created_at", "updated_at", "modified_at"):
        if candidate in columns:
            observed_column = candidate
            break
    if observed_column is None:
        cur.execute(
            """
            SELECT status,
                   count(*)::int AS workflow_count,
                   NULL AS oldest_observed_raw
              FROM dbos.workflow_status
             GROUP BY status
             ORDER BY status
            """
        )
    else:
        cur.execute(
            sql.SQL(
                """
                SELECT status,
                       count(*)::int AS workflow_count,
                       min({observed_column}) AS oldest_observed_raw
                  FROM dbos.workflow_status
                 GROUP BY status
                 ORDER BY status
                """
            ).format(observed_column=sql.Identifier(observed_column))
        )

    by_status = []
    for row in cur.fetchall():
        item = dict(row)
        raw_observed = item.pop("oldest_observed_raw", None)
        oldest_observed_at = _coerce_observed_at(raw_observed)
        item["observed_at_column"] = observed_column
        item["oldest_observed_at"] = oldest_observed_at
        item["oldest_observed_age_seconds"] = _age_seconds(oldest_observed_at)
        by_status.append(_jsonify_row(item))
    return {
        "present": True,
        "workflow_status_total_bytes": total_bytes,
        "by_status": by_status,
    }


def _runtime_queue_health_summary(cur) -> dict[str, Any]:
    if not _regclass_exists(cur, "okg.v_runtime_queue_health"):
        return {"present": False, "rows": []}
    try:
        cur.execute(
            """
            SELECT queue_name, status, availability, active_count,
                   pending_count, waiting_count, delayed_count,
                   running_count, failed_count, cancelled_count,
                   dlq_count, terminal_history_count,
                   unknown_active_age_count,
                   oldest_active_age_seconds, newest_active_age_seconds,
                   stale_after_seconds, stale_active_work_present,
                   generated_at, reason
              FROM okg.v_runtime_queue_health
             ORDER BY queue_name NULLS FIRST
            """
        )
        rows = [_jsonify_row(row) for row in cur.fetchall()]
    except Exception as exc:  # noqa: BLE001 - doctor remains readable.
        return {
            "present": True,
            "rows": [],
            "status_counts": {},
            "error": type(exc).__name__,
            "message": str(exc),
        }
    status_counts: dict[str, int] = {}
    for row in rows:
        status = str(row.get("status") or "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1
    return {
        "present": True,
        "rows": rows,
        "status_counts": status_counts,
        "active_count": sum(int(row.get("active_count") or 0) for row in rows),
        "dlq_count": sum(int(row.get("dlq_count") or 0) for row in rows),
    }


def _derived_relation_summary(cur) -> dict[str, Any]:
    if not _regclass_exists(cur, "okg.derived_relation_registry"):
        return {"present": False, "by_status": []}
    cur.execute(
        """
        SELECT status, count(*)::int AS relation_count
          FROM okg.derived_relation_registry
         GROUP BY status
         ORDER BY status
        """
    )
    return {
        "present": True,
        "by_status": [_jsonify_row(row) for row in cur.fetchall()],
    }


def _current_database(cur) -> str:
    cur.execute("SELECT current_database() AS database")
    return str(cur.fetchone()["database"])


def _database_size(cur, database: str) -> int:
    cur.execute("SELECT pg_database_size(%s) AS size_bytes", (database,))
    return int(cur.fetchone()["size_bytes"] or 0)


def _database_count(dsn: str) -> int:
    with db_connect(dsn, connect_timeout=5) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM pg_database")
            return int(cur.fetchone()[0])


def _current_owner(cur, database: str) -> str | None:
    cur.execute(
        """
        SELECT pg_get_userbyid(datdba) AS owner
          FROM pg_database
         WHERE datname = %s
        """,
        (database,),
    )
    row = cur.fetchone()
    return str(row["owner"]) if row else None


def _installed_schemas(cur) -> list[str]:
    cur.execute(
        """
        SELECT schema_name
          FROM information_schema.schemata
         WHERE schema_name IN ('okg', 'okg_derived', 'dbos')
         ORDER BY schema_name
        """
    )
    return [str(row["schema_name"]) for row in cur.fetchall()]


def _detected_deployments(cur) -> list[str]:
    if not _regclass_exists(cur, "okg.graph_generations"):
        return []
    cur.execute(
        """
        SELECT DISTINCT deployment_name
          FROM okg.graph_generations
         WHERE deployment_name IS NOT NULL
         ORDER BY deployment_name
        """
    )
    return [str(row["deployment_name"]) for row in cur.fetchall()]


def _latest_published_generation(cur) -> int | None:
    if not _regclass_exists(cur, "okg.graph_generations"):
        return None
    cur.execute(
        """
        SELECT max(generation_id) AS generation_id
          FROM okg.graph_generations
         WHERE status = 'published'
        """
    )
    value = cur.fetchone()["generation_id"]
    return int(value) if value is not None else None


def _schema_exists(cur, schema_name: str) -> bool:
    cur.execute(
        """
        SELECT EXISTS (
          SELECT 1 FROM information_schema.schemata WHERE schema_name = %s
        ) AS exists
        """,
        (schema_name,),
    )
    row = cur.fetchone()
    return bool(row["exists"] if isinstance(row, dict) else row[0])


def _regclass_exists(cur, relation: str) -> bool:
    cur.execute("SELECT to_regclass(%s) AS relation", (relation,))
    row = cur.fetchone()
    value = row["relation"] if isinstance(row, dict) else row[0]
    return value is not None


def _regprocedure_exists(cur, procedure: str) -> bool:
    cur.execute("SELECT to_regprocedure(%s) AS procedure", (procedure,))
    row = cur.fetchone()
    value = row["procedure"] if isinstance(row, dict) else row[0]
    return value is not None


def _relation_row_count(cur, relation: str) -> int:
    cur.execute(
        sql.SQL("SELECT count(*)::bigint AS row_count FROM {}").format(
            sql.Identifier(*relation.split(".", 1))
        )
    )
    row = cur.fetchone()
    value = row["row_count"] if isinstance(row, dict) else row[0]
    return int(value)


def _column_types(cur, schema: str, table: str) -> dict[str, str]:
    cur.execute(
        """
        SELECT column_name, data_type
          FROM information_schema.columns
         WHERE table_schema = %s
           AND table_name = %s
        """,
        (schema, table),
    )
    return {
        str(row["column_name"] if isinstance(row, dict) else row[0]): str(
            row["data_type"] if isinstance(row, dict) else row[1]
        )
        for row in cur.fetchall()
    }


def _relation_total_bytes(cur, relation: str) -> int | None:
    cur.execute(
        "SELECT pg_total_relation_size(to_regclass(%s)) AS total_bytes",
        (relation,),
    )
    row = cur.fetchone()
    value = row["total_bytes"] if isinstance(row, dict) else row[0]
    return int(value) if value is not None else None


def _dbos_stale_where(
    *,
    stale_expr: sql.Composable,
    statuses: tuple[str, ...],
    cutoff_value: Any,
    has_queue_name: bool,
    queue_name: str | None,
    has_workflow_name: bool,
    workflow_name: str | None,
    has_priority: bool,
) -> tuple[sql.Composed, list[Any]]:
    clauses = [
        sql.SQL("status = ANY(%s)"),
        sql.SQL("{} < %s").format(stale_expr),
    ]
    params: list[Any] = [list(statuses), cutoff_value]
    if queue_name is not None:
        if not has_queue_name:
            raise RequiresConfirm(
                "cannot filter DBOS cleanup by queue_name; column is absent",
                data={"queue_name": queue_name},
                fix_hint="inspect DBOS schema before cleanup",
            )
        clauses.append(sql.SQL("queue_name = %s"))
        params.append(queue_name)
    if workflow_name is not None:
        if not has_workflow_name:
            raise RequiresConfirm(
                "cannot filter DBOS cleanup by workflow name; column is absent",
                data={"workflow_name": workflow_name},
                fix_hint="inspect DBOS schema before cleanup",
            )
        clauses.append(sql.SQL("name = %s"))
        params.append(workflow_name)
    if has_workflow_name and has_priority:
        clauses.append(
            sql.SQL(
                "NOT (status = 'ENQUEUED' "
                "AND name = 'okg_source_sync' "
                "AND COALESCE(priority, 100) < 100)"
            )
        )
    return sql.SQL(" AND ").join(clauses), params


def _dbos_stale_anchor(
    column_types: dict[str, str],
) -> tuple[sql.Composable, str, str]:
    base_column = "updated_at" if "updated_at" in column_types else "created_at"
    base_type = column_types[base_column]
    numeric_types = {"bigint", "integer", "numeric"}
    candidates = [base_column]
    if (
        "created_at" in column_types
        and "created_at" not in candidates
        and column_types["created_at"] == base_type
    ):
        candidates.append("created_at")
    if (
        "started_at_epoch_ms" in column_types
        and base_type in numeric_types
        and column_types["started_at_epoch_ms"] in numeric_types
    ):
        candidates.append("started_at_epoch_ms")
    if len(candidates) == 1:
        return sql.Identifier(candidates[0]), candidates[0], base_type
    return (
        sql.SQL("GREATEST({})").format(
            sql.SQL(", ").join(sql.Identifier(column) for column in candidates)
        ),
        "greatest(" + ",".join(candidates) + ")",
        base_type,
    )


def _stale_cutoff_value(stale_after_seconds: int, data_type: str) -> Any:
    cutoff = datetime.now(timezone.utc).timestamp() - stale_after_seconds
    if data_type in {"bigint", "integer", "numeric"}:
        return int(cutoff * 1000)
    return datetime.fromtimestamp(cutoff, tz=timezone.utc)


def _current_dbos_time_value(data_type: str) -> Any:
    now = datetime.now(timezone.utc)
    if data_type in {"bigint", "integer", "numeric"}:
        return int(now.timestamp() * 1000)
    return now


def _dbos_backlog_group(row: dict[str, Any]) -> dict[str, Any]:
    oldest = _coerce_observed_at(row.get("oldest_raw"))
    newest = _coerce_observed_at(row.get("newest_raw"))
    return _jsonify_row(
        {
            "status": row.get("status"),
            "queue_name": row.get("queue_name"),
            "name": row.get("name"),
            "workflow_count": row.get("workflow_count"),
            "oldest_observed_at": oldest,
            "newest_observed_at": newest,
            "oldest_observed_age_seconds": _age_seconds(oldest),
            "newest_observed_age_seconds": _age_seconds(newest),
        }
    )


def _dbos_backlog_sample(
    row: dict[str, Any],
    stale_column: str,
    *,
    deployment: str | None,
) -> dict[str, Any]:
    observed = _coerce_observed_at(row.get("stale_raw"))
    source_sync = _dbos_source_sync_summary(row, deployment=deployment)
    return _jsonify_row(
        {
            "workflow_uuid": row.get("workflow_uuid"),
            "status": row.get("status"),
            "queue_name": row.get("queue_name"),
            "name": row.get("name"),
            "executor_id": row.get("executor_id"),
            "deduplication_id": row.get("deduplication_id"),
            "application_version": row.get("application_version"),
            "recovery_attempts": row.get("recovery_attempts"),
            "stale_column": stale_column,
            "observed_at": observed,
            "observed_age_seconds": _age_seconds(observed),
            "source_sync": source_sync,
            "recovery_action": "cancel_stale_replaceable_workflow",
        }
    )


def _optional_dbos_column_expr(
    column_types: dict[str, str],
    column: str,
    *,
    null_type: str = "text",
) -> sql.Composable:
    if column in column_types:
        return sql.Identifier(column)
    return sql.SQL("NULL::{}").format(sql.SQL(null_type))


def _dbos_source_sync_summary(
    row: dict[str, Any],
    *,
    deployment: str | None,
) -> dict[str, Any] | None:
    if row.get("name") != "okg_source_sync":
        return None
    payload = _decode_source_sync_dbos_payload(
        deployment=deployment,
        inputs=row.get("inputs"),
        serialization=row.get("serialization"),
    )
    if not payload:
        return {
            "workflow_name": "okg_source_sync",
            "decode_status": "unavailable",
        }
    events = payload.get("events")
    events = events if isinstance(events, list) else []
    scope_keys: list[str] = []
    for event in events:
        if not isinstance(event, dict):
            continue
        scope_key = event.get("scope_key")
        if not scope_key:
            continue
        scope_keys.append(str(scope_key))
        if len(scope_keys) >= 10:
            break
    return {
        "workflow_name": "okg_source_sync",
        "decode_status": "decoded",
        "deployment": payload.get("deployment") or deployment,
        "sources": payload.get("sources") or [],
        "run_mode": payload.get("run_mode"),
        "publish": payload.get("publish"),
        "include_dependents": payload.get("include_dependents"),
        "include_predecessors": payload.get("include_predecessors"),
        "dependent_fanout": payload.get("dependent_fanout"),
        "event_count": len(events),
        "scope_keys_sample": scope_keys,
        "stage_timing": _source_sync_stage_timing_summary(row),
    }


def _deployment_from_dbos_queue_name(queue_name: str | None) -> str | None:
    if not queue_name or not queue_name.startswith("okg-"):
        return None
    deployment = queue_name.removeprefix("okg-")
    source_sync_suffix = "-source-sync"
    if deployment.endswith(source_sync_suffix):
        deployment = deployment[: -len(source_sync_suffix)]
    return deployment or None


def _source_sync_stage_timing_summary(row: dict[str, Any]) -> dict[str, Any]:
    """Describe where source-sync timing is observable for this workflow.

    Queued DBOS rows have not entered source execution yet, so the only live
    timing is queue age. Once the workflow starts, the returned DBOS payload's
    ingest report carries per-source elapsed seconds and failed-stage names.
    """
    status = str(row.get("status") or "").upper()
    pending = status in {"ENQUEUED", "PENDING", "DELAYED"}
    return {
        "queue_wait": {
            "status": "observable",
            "source": "dbos.workflow_status.created_at_or_updated_at",
        },
        "setup": {
            "status": "observable",
            "source": "dbos.operation_outputs.okg_source_sync_setup_done",
        },
        "source_execution": {
            "status": "pending" if pending else "observable",
            "source": "workflow_output.report.results[].elapsed_s",
        },
        "source_stage": {
            "status": "pending" if pending else "observable",
            "source": "workflow_output.report.results[].stage",
        },
        "publish": {
            "status": "pending" if pending else "observable",
            "source": "workflow_output.report.publishes[]",
        },
    }


def _decode_source_sync_dbos_payload(
    *,
    deployment: str | None,
    inputs: Any,
    serialization: Any,
) -> dict[str, Any] | None:
    if inputs is None:
        return None
    try:
        from okg.substrate.source_sync_enqueue import (
            source_sync_payload_from_dbos_inputs,
        )

        payload = source_sync_payload_from_dbos_inputs(
            deployment=deployment or "",
            inputs=str(inputs),
            serialization=str(serialization) if serialization is not None else None,
        )
    except Exception:  # noqa: BLE001 - DBOS input schemas may vary by SDK version.
        return None
    return payload if isinstance(payload, dict) else None


def _coerce_observed_at(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        return _epoch_to_datetime(float(value))
    if isinstance(value, str):
        stripped = value.strip()
        try:
            return _epoch_to_datetime(float(stripped))
        except ValueError:
            pass
        try:
            parsed = datetime.fromisoformat(stripped.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return None


def _epoch_to_datetime(value: float) -> datetime | None:
    # DBOS stores epoch milliseconds in bigint columns; accept seconds too.
    if abs(value) > 10_000_000_000:
        value = value / 1000.0
    try:
        return datetime.fromtimestamp(value, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None


def _age_seconds(dt: datetime | None) -> int | None:
    if dt is None:
        return None
    normalized = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    return max(0, int((datetime.now(timezone.utc) - normalized).total_seconds()))


def _classify_database(
    name: str,
    manifests: list[ManifestDatabase],
) -> str:
    lowered = name.lower()
    if manifests:
        return "deployment"
    if "archive" in lowered or lowered.endswith("_old"):
        return "archive"
    if lowered in {"okg", "postgres"}:
        return "unknown"
    if "bench" in lowered or "perf" in lowered:
        return "bench"
    if "test" in lowered or lowered.startswith("tmp_"):
        return "test"
    if "scratch" in lowered or "scratch" in lowered:
        return "scratch"
    if "dev" in lowered:
        return "dev"
    return "unknown"


def _relation_family(schema_name: str, relation_name: str) -> str:
    if schema_name == "dbos":
        return "dbos_runtime"
    if schema_name == "okg_derived" or relation_name.startswith("derived_"):
        return "derived_artifacts"
    if schema_name == "_timescaledb_internal":
        return "timescale_chunks"
    if relation_name in {"node_facts", "edge_facts"}:
        return "fact_log"
    if relation_name in {"nodes_live", "edges_live"}:
        return "live_graph"
    if relation_name in {"jobs", "publisher_leases", "lease_audit"}:
        return "legacy_runtime"
    if relation_name.startswith("mcp_") or relation_name.startswith("trace_"):
        return "mcp_trace"
    if relation_name in {
        "catalog_versions",
        "subtypes",
        "narrowings",
        "aliases",
        "invariants",
        "identifier_patterns",
    }:
        return "catalog"
    return "other"


def _remove_between(
    text: str,
    start: str,
    end: str,
    *,
    keep_end: bool,
) -> str:
    start_idx = text.find(start)
    if start_idx < 0:
        return text
    end_idx = text.find(end, start_idx)
    if end_idx < 0:
        return text
    replacement = end if keep_end else ""
    return text[:start_idx] + replacement + text[end_idx + len(end):]


def _replace_between(
    text: str,
    start: str,
    end: str,
    replacement: str,
    *,
    keep_end: bool,
) -> str:
    start_idx = text.find(start)
    if start_idx < 0:
        return text
    end_idx = text.find(end, start_idx)
    if end_idx < 0:
        return text
    suffix = end if keep_end else ""
    return text[:start_idx] + replacement + "\n" + suffix + text[end_idx + len(end):]


def _jsonify_row(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, dict):
        return {str(k): _jsonify_row(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_jsonify_row(v) for v in value]
    if isinstance(value, tuple):
        return [_jsonify_row(v) for v in value]
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    return value
