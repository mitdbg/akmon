"""The registered multi-phase spec for ``0020_interval_narrow_split``.

PACT phase-interval-narrow-split-migration (issues #1143/#1114): the
original single-transaction DO block never completed on a real database —
1.35 TB of reads and 583 GB of writes against a 12 GB working set, killed
at 20+ minutes. The measured amplification sources and their fixes:

* Every row rewritten to set ``attrs_hash`` with the dead versions
  unreclaimable inside one transaction → batched keyset backfill committed
  per batch, so autovacuum reclaims between checkpoints. (The one full-row
  rewrite per row is inherent to adding the column and is budgeted, not
  hidden.)
* A ``raw_attrs = attrs`` normalization pass that scanned 4.5M rows and
  matched zero → folded into the backfill expression; no separate pass.
* Per-row ``ON CONFLICT`` probes into a growing random-ordered PK, and FKs
  validated at ``ADD CONSTRAINT`` time → version-store inserts sorted by
  the version-store PK within each batch; FKs added ``NOT VALID``
  (metadata-only) and validated in their own resumable phase.
* One atomic transaction with no abort path → every phase below commits
  bounded batches with a durable watermark (the driver's checkpoint
  contract), so a SIGKILL at any point leaves a labelled, resumable state.

Phase order, per wide table (snapshot view first, then intervals):

1. ``ensure_infra`` — hash functions + ``okg.attribute_versions`` +
   nullable ``attrs_hash`` columns. The CREATE statements are parsed from
   the migration's own SQL file (single source; no duplicate DDL text
   here). When a wide table is present, autovacuum is PAUSED on the three
   involved tables for the migration's duration: the publish-churn tuning
   (scale factor 0.05) otherwise fires a full-heap vacuum every few
   batches — measured 14 autovacuum passes over an 8 GB heap in five
   minutes during rehearsal, which starved the batches behind continuous
   re-reads. The final phase resets the option.
2. ``detach_indexes_<t>`` — drop the table's secondary (non-PK,
   non-constraint) indexes, durably recording each definition and whether
   it references the payload columns in the phase watermark. Every
   backfilled row is a full-row MVCC rewrite; without this, each rewrite
   maintains every secondary index (including the dead jsonb GIN indexes
   this migration exists to retire). Rebuilt after the column drops.
3. ``backfill_<t>`` — keyset batches over the source PK; one materialized
   CTE computes the content hash exactly once per row, inserts the
   version-store rows (PK-sorted, ``ON CONFLICT DO NOTHING``), and sets
   ``attrs_hash`` in the same statement.
4. ``fence_<t>`` — ``ADD CONSTRAINT ... CHECK (attrs_hash IS NOT NULL)
   NOT VALID``: a short ACCESS EXCLUSIVE metadata change that immediately
   rejects NEW null writes (``NOT VALID`` skips only existing rows), so a
   concurrent legacy writer cannot strand stragglers past the sweep.
5. ``sweep_<t>`` — bounded batches for rows written between the backfill
   watermark and the fence (same batch SQL, fresh watermark).
6. ``validate_check_<t>`` — ``VALIDATE CONSTRAINT`` in its own batch
   (SHARE UPDATE EXCLUSIVE only; never holds the ADD's ACCESS EXCLUSIVE
   lock across the scan).
7. ``finalize_<t>`` — metadata-only: ``SET NOT NULL`` (PG uses the
   validated check to skip the scan), DROP the helper check (so an
   upgraded schema converges byte-for-byte with a fresh install), and
   ``ADD`` the composite FK ``NOT VALID``.
8. ``drop_wide_<t>`` — ``DROP COLUMN attrs, raw_attrs`` (metadata-only).
9. ``rebuild_indexes_<t>`` — recreate the recorded secondary indexes,
   one per resumable batch, SKIPPING the payload-referencing ones (the
   dead GIN indexes are retired BY DESIGN — the columns they indexed are
   gone). Skips are recorded in the watermark, never silent.
10. ``validate_fk_<t>`` — ``VALIDATE CONSTRAINT`` for the composite FK.
11. ``analyze`` — reset the autovacuum pause and ANALYZE the three
    tables. (A crash between ensure_infra and here leaves autovacuum
    paused on three tables — a labelled state: the pending migration and
    its in-progress phase rows stay visible in migration_status and
    doctor until a re-run completes, and the re-run always reaches this
    reset.)

Every phase no-ops (returns ``None`` on its first batch) when its work is
already done, so a fresh database — where the schema snapshot creates the
narrow shape directly — pays only catalog probes before the driver records
the ledger row. Every DDL batch runs under its own ``SET LOCAL
lock_timeout`` so an ALTER queued behind a long reader refuses loudly and
resumably instead of wedging the database.
"""

from __future__ import annotations

import importlib.resources
from typing import Any

import psycopg

MIGRATION_ID = "0020_interval_narrow_split"

# Bounded work per checkpoint. 50k rows of ~1.3 KB is ~65 MB of new row
# versions per batch — large enough to amortize per-batch overhead across
# the 6.8M-row corpus (~140 batches), small enough that a kill loses under
# a minute of work and autovacuum can keep pace between commits.
BATCH_ROWS = 50_000

# Lock acquisition budget for the metadata-only DDL batches. The DDL itself
# is instant; the timeout bounds how long we queue behind a long reader
# while our pending ACCESS EXCLUSIVE lock blocks everyone else.
DDL_LOCK_TIMEOUT = "10s"

_VIEW = "okg.attribute_effective_view"
_INTERVALS = "okg.attribute_effective_view_intervals"
_VERSIONS = "okg.attribute_versions"

# (table regclass, bare table name, PK columns, helper check name, FK name)
_TABLES: tuple[dict[str, Any], ...] = (
    {
        "table": _VIEW,
        "name": "attribute_effective_view",
        "pk": ("target_kind", "target_id", "generation_id", "namespace"),
        "check": "attribute_effective_view_attrs_hash_not_null",
        "fk": "attribute_effective_view_version_fkey",
    },
    {
        "table": _INTERVALS,
        "name": "attribute_effective_view_intervals",
        "pk": ("target_kind", "target_id", "namespace", "branch_id", "from_ord"),
        "check": "attribute_effective_view_intervals_attrs_hash_not_null",
        "fk": "attribute_effective_view_intervals_version_fkey",
    },
)


def _migration_sql_text() -> str:
    return (
        importlib.resources.files("okg.substrate.db")
        .joinpath(f"migrations/{MIGRATION_ID}.sql")
        .read_text(encoding="utf-8")
    )


def _table_exists(conn: psycopg.Connection, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (table,))
        return cur.fetchone()[0] is not None


def _column_exists(conn: psycopg.Connection, name: str, column: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM information_schema.columns
             WHERE table_schema = 'okg' AND table_name = %s
               AND column_name = %s
            """,
            (name, column),
        )
        return cur.fetchone() is not None


def _column_not_null(conn: psycopg.Connection, name: str, column: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT is_nullable FROM information_schema.columns
             WHERE table_schema = 'okg' AND table_name = %s
               AND column_name = %s
            """,
            (name, column),
        )
        row = cur.fetchone()
    return row is not None and str(row[0]) == "NO"


def _constraint_state(
    conn: psycopg.Connection, table: str, conname: str
) -> str | None:
    """``'validated'`` / ``'not_valid'`` when the constraint exists, else None."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT convalidated FROM pg_constraint
             WHERE conname = %s AND conrelid = %s::regclass
            """,
            (conname, table),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return "validated" if bool(row[0]) else "not_valid"


def _set_ddl_lock_timeout(conn: psycopg.Connection) -> None:
    # SET LOCAL scopes to the driver's batch transaction; the driver commits
    # (or rolls back) the batch, which clears it.
    with conn.cursor() as cur:
        cur.execute(f"SET LOCAL lock_timeout = '{DDL_LOCK_TIMEOUT}'")


def _any_wide_table(conn: psycopg.Connection) -> bool:
    return any(
        _table_exists(conn, spec["table"])
        and _column_exists(conn, spec["name"], "attrs")
        for spec in _TABLES
    )


def _run_ensure_infra(conn: psycopg.Connection, watermark: Any | None) -> Any | None:
    """Execute the migration file's CREATE statements + add the nullable columns.

    The DDL is parsed from the shipped SQL file (single source): CREATE OR
    REPLACE FUNCTION / CREATE TABLE IF NOT EXISTS are idempotent, so a crash
    after a partial batch re-runs cleanly. On a wide database this also
    pauses autovacuum on the involved tables — the publish-churn tuning
    fires full-heap vacuums every few batches otherwise (see module doc);
    the analyze phase resets it.
    """
    del watermark
    from okg.substrate.db.migration_class import (
        _statement_keyword,
        top_level_statements,
    )

    _set_ddl_lock_timeout(conn)
    with conn.cursor() as cur:
        for statement in top_level_statements(_migration_sql_text()):
            # The file carries SET LOCAL timeout guards plus CREATE
            # statements plus comment-only fragments; execute exactly those
            # (fail loud on anything unexpected so an edited file cannot
            # silently smuggle data motion past the phase plan).
            keyword = _statement_keyword(statement)
            if keyword is None:
                continue  # comment-only tail
            if keyword not in {"CREATE", "SET"}:
                raise RuntimeError(
                    f"{MIGRATION_ID}.sql carries a {keyword} statement; "
                    "the multi-phase spec owns all data motion"
                )
            cur.execute(statement)
    for spec in _TABLES:
        if _table_exists(conn, spec["table"]) and not _column_exists(
            conn, spec["name"], "attrs_hash"
        ):
            with conn.cursor() as cur:
                cur.execute(
                    f"ALTER TABLE {spec['table']} "
                    "ADD COLUMN IF NOT EXISTS attrs_hash bytea"
                )
    if _any_wide_table(conn):
        with conn.cursor() as cur:
            for table in (_VIEW, _INTERVALS, _VERSIONS):
                if _table_exists(conn, table):
                    cur.execute(
                        f"ALTER TABLE {table} SET (autovacuum_enabled = false)"
                    )
    return None


# Secondary (non-PK, non-constraint) indexes on a wide table, with an exact
# catalog answer to "does it reference the payload columns" — indexed
# attnums joined to pg_attribute, plus a word-boundary text match so an
# expression index over the payload cannot slip through as rebuildable.
_SECONDARY_INDEXES_SQL = r"""
SELECT ic.relname,
       pg_get_indexdef(i.indexrelid),
       EXISTS (
         SELECT 1 FROM unnest(i.indkey) AS k(attnum)
           JOIN pg_attribute a
             ON a.attrelid = i.indrelid AND a.attnum = k.attnum
          WHERE a.attname IN ('attrs', 'raw_attrs')
       ) OR pg_get_indexdef(i.indexrelid) ~ '\mattrs\M|\mraw_attrs\M'
  FROM pg_index i
  JOIN pg_class ic ON ic.oid = i.indexrelid
 WHERE i.indrelid = %s::regclass
   AND NOT i.indisprimary
   AND NOT EXISTS (
     SELECT 1 FROM pg_constraint con WHERE con.conindid = i.indexrelid
   )
 ORDER BY ic.relname
"""


def _make_detach_indexes(spec: dict[str, Any]):
    def run_batch(conn: psycopg.Connection, watermark: Any | None) -> Any | None:
        if not _table_exists(conn, spec["table"]) or not _column_exists(
            conn, spec["name"], "attrs"
        ):
            return None  # narrow already: nothing to detach
        with conn.cursor() as cur:
            cur.execute(_SECONDARY_INDEXES_SQL, (spec["table"],))
            current = cur.fetchall()
        recorded = {
            str(entry["name"]): dict(entry) for entry in (watermark or [])
        }
        if not current:
            return None  # already detached (recorded defs stay durable)
        _set_ddl_lock_timeout(conn)
        with conn.cursor() as cur:
            for name, definition, references_payload in current:
                recorded[str(name)] = {
                    "name": str(name),
                    "definition": str(definition),
                    "references_payload": bool(references_payload),
                }
                cur.execute(f"DROP INDEX okg.{name}")
        return sorted(recorded.values(), key=lambda entry: entry["name"])

    return run_batch


def _detached_index_records(
    conn: psycopg.Connection, phase: str
) -> list[dict[str, Any]]:
    from okg.substrate.db.migration_class import MULTIPHASE_PROGRESS_TABLE

    with conn.cursor() as cur:
        cur.execute(
            f"SELECT watermark FROM {MULTIPHASE_PROGRESS_TABLE} "
            "WHERE migration_id = %s AND phase = %s",
            (MIGRATION_ID, phase),
        )
        row = cur.fetchone()
    if row is None or not row[0]:
        return []
    return [dict(entry) for entry in row[0]]


def _make_rebuild_indexes(spec: dict[str, Any], detach_phase: str):
    def run_batch(conn: psycopg.Connection, watermark: Any | None) -> Any | None:
        if not _table_exists(conn, spec["table"]):
            return None
        recorded = _detached_index_records(conn, detach_phase)
        if not recorded:
            return None
        done = {str(name) for name in (watermark or [])}
        for entry in recorded:
            name = str(entry["name"])
            if name in done:
                continue
            if entry.get("references_payload"):
                # The payload columns are gone; these are exactly the dead
                # jsonb GIN indexes this migration retires. Recorded in the
                # watermark as skipped — never a silent omission.
                done.add(name)
                continue
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass(%s)", (f"okg.{name}",))
                if cur.fetchone()[0] is not None:
                    done.add(name)
                    continue
            _set_ddl_lock_timeout(conn)
            with conn.cursor() as cur:
                cur.execute("SET LOCAL maintenance_work_mem = '512MB'")
                cur.execute(str(entry["definition"]))
            done.add(name)
            return sorted(done)  # one index build per committed batch
        return None

    return run_batch


def _backfill_sql(spec: dict[str, Any]) -> str:
    pk_cols = ", ".join(spec["pk"])
    placeholders = ", ".join(["%s"] * len(spec["pk"]))
    join = " AND ".join(f"t.{col} = hashed.{col}" for col in spec["pk"])
    return f"""
        WITH batch AS (
          SELECT {pk_cols}, attrs,
                 CASE WHEN raw_attrs = attrs THEN NULL
                      ELSE raw_attrs END AS norm_raw
            FROM {spec["table"]}
           WHERE ({pk_cols}) > ({placeholders})
             AND attrs_hash IS NULL
           ORDER BY {pk_cols}
           LIMIT %s
        ),
        hashed AS MATERIALIZED (
          SELECT batch.*,
                 okg.attribute_content_hash(batch.attrs, batch.norm_raw) AS h
            FROM batch
        ),
        ins AS (
          INSERT INTO {_VERSIONS}
            (target_kind, target_id, attrs_hash, attrs, raw_attrs)
          SELECT target_kind, target_id, h, attrs, norm_raw
            FROM hashed
           ORDER BY target_kind, target_id, h
          ON CONFLICT (target_kind, target_id, attrs_hash) DO NOTHING
        ),
        upd AS (
          UPDATE {spec["table"]} AS t
             SET attrs_hash = hashed.h
            FROM hashed
           WHERE {join}
        )
        SELECT {pk_cols}
          FROM hashed
         ORDER BY ({pk_cols}) DESC
         LIMIT 1
    """


# Keyset floor for the first batch: row-value comparison needs concrete
# values strictly below every real key. Text PK columns are compared under
# the C-locale-safe empty string; ordinal columns under the BIGINT minimum.
_KEYSET_FLOOR = {"from_ord": -9223372036854775808}


def _initial_watermark(spec: dict[str, Any]) -> list[Any]:
    return [_KEYSET_FLOOR.get(col, "") for col in spec["pk"]]


def _make_backfill(spec: dict[str, Any]):
    sql = _backfill_sql(spec)

    def run_batch(conn: psycopg.Connection, watermark: Any | None) -> Any | None:
        if not _table_exists(conn, spec["table"]) or not _column_exists(
            conn, spec["name"], "attrs"
        ):
            return None  # narrow already (fresh install or completed drop)
        key = list(watermark) if watermark is not None else _initial_watermark(spec)
        with conn.cursor() as cur:
            cur.execute(sql, (*key, BATCH_ROWS))
            # The final SELECT is the batch's max key IN POSTGRES' ordering —
            # the same collation the next batch's keyset comparison uses, so
            # the watermark can never trail a processed-but-unmarked row.
            row = cur.fetchone()
        if row is None:
            return None
        return list(row)

    return run_batch


def _make_fence(spec: dict[str, Any]):
    def run_batch(conn: psycopg.Connection, watermark: Any | None) -> Any | None:
        del watermark
        if not _table_exists(conn, spec["table"]) or not _column_exists(
            conn, spec["name"], "attrs"
        ):
            return None
        if _constraint_state(conn, spec["table"], spec["check"]) is not None:
            return None
        _set_ddl_lock_timeout(conn)
        with conn.cursor() as cur:
            cur.execute(
                f"ALTER TABLE {spec['table']} "
                f"ADD CONSTRAINT {spec['check']} "
                "CHECK (attrs_hash IS NOT NULL) NOT VALID"
            )
        return None

    return run_batch


def _make_validate_check(spec: dict[str, Any]):
    def run_batch(conn: psycopg.Connection, watermark: Any | None) -> Any | None:
        del watermark
        if not _table_exists(conn, spec["table"]):
            return None
        if _constraint_state(conn, spec["table"], spec["check"]) != "not_valid":
            return None
        _set_ddl_lock_timeout(conn)
        with conn.cursor() as cur:
            cur.execute(
                f"ALTER TABLE {spec['table']} "
                f"VALIDATE CONSTRAINT {spec['check']}"
            )
        return None

    return run_batch


def _make_finalize(spec: dict[str, Any]):
    def run_batch(conn: psycopg.Connection, watermark: Any | None) -> Any | None:
        del watermark
        if not _table_exists(conn, spec["table"]):
            return None
        _set_ddl_lock_timeout(conn)
        with conn.cursor() as cur:
            if not _column_not_null(conn, spec["name"], "attrs_hash"):
                # Metadata-only: PG proves NOT NULL from the validated check.
                cur.execute(
                    f"ALTER TABLE {spec['table']} "
                    "ALTER COLUMN attrs_hash SET NOT NULL"
                )
            if _constraint_state(conn, spec["table"], spec["check"]) is not None:
                # Fresh installs never carry the helper check; drop it so an
                # upgraded schema converges with schema.sql exactly.
                cur.execute(
                    f"ALTER TABLE {spec['table']} "
                    f"DROP CONSTRAINT {spec['check']}"
                )
            if _constraint_state(conn, spec["table"], spec["fk"]) is None:
                cur.execute(
                    f"ALTER TABLE {spec['table']} "
                    f"ADD CONSTRAINT {spec['fk']} "
                    "FOREIGN KEY (target_kind, target_id, attrs_hash) "
                    f"REFERENCES {_VERSIONS} "
                    "(target_kind, target_id, attrs_hash) "
                    "ON DELETE RESTRICT NOT VALID"
                )
        return None

    return run_batch


def _make_drop_wide(spec: dict[str, Any]):
    def run_batch(conn: psycopg.Connection, watermark: Any | None) -> Any | None:
        del watermark
        if not _table_exists(conn, spec["table"]) or not _column_exists(
            conn, spec["name"], "attrs"
        ):
            return None
        _set_ddl_lock_timeout(conn)
        with conn.cursor() as cur:
            # Metadata-only (attisdropped); also drops the dead GIN indexes.
            cur.execute(
                f"ALTER TABLE {spec['table']} "
                "DROP COLUMN attrs, DROP COLUMN raw_attrs"
            )
        return None

    return run_batch


def _make_validate_fk(spec: dict[str, Any]):
    def run_batch(conn: psycopg.Connection, watermark: Any | None) -> Any | None:
        del watermark
        if not _table_exists(conn, spec["table"]):
            return None
        if _constraint_state(conn, spec["table"], spec["fk"]) != "not_valid":
            return None
        _set_ddl_lock_timeout(conn)
        with conn.cursor() as cur:
            cur.execute(
                f"ALTER TABLE {spec['table']} "
                f"VALIDATE CONSTRAINT {spec['fk']}"
            )
        return None

    return run_batch


def _run_analyze(conn: psycopg.Connection, watermark: Any | None) -> Any | None:
    del watermark
    _set_ddl_lock_timeout(conn)
    with conn.cursor() as cur:
        for table in (_VIEW, _INTERVALS, _VERSIONS):
            if _table_exists(conn, table):
                # Undo ensure_infra's autovacuum pause (no-op when it never
                # ran — RESET of an unset option changes nothing).
                cur.execute(
                    f"ALTER TABLE {table} RESET (autovacuum_enabled)"
                )
                cur.execute(f"ANALYZE {table}")
    return None


def build_spec():
    """The executable phase plan for 0020 (imported lazily by the driver)."""
    from okg.substrate.db.multiphase import MultiphasePhase, MultiphaseSpec

    phases: list[MultiphasePhase] = [
        MultiphasePhase(name="ensure_infra", run_batch=_run_ensure_infra),
    ]
    for spec in _TABLES:
        name = spec["name"]
        phases.extend(
            [
                MultiphasePhase(
                    name=f"detach_indexes_{name}",
                    run_batch=_make_detach_indexes(spec),
                ),
                MultiphasePhase(
                    name=f"backfill_{name}", run_batch=_make_backfill(spec)
                ),
                MultiphasePhase(name=f"fence_{name}", run_batch=_make_fence(spec)),
                # The sweep is the SAME bounded batch as the backfill with a
                # fresh watermark: it exists for rows a concurrent legacy
                # writer inserted between the backfill watermark passing them
                # and the fence landing.
                MultiphasePhase(
                    name=f"sweep_{name}", run_batch=_make_backfill(spec)
                ),
                MultiphasePhase(
                    name=f"validate_check_{name}",
                    run_batch=_make_validate_check(spec),
                ),
                MultiphasePhase(
                    name=f"finalize_{name}", run_batch=_make_finalize(spec)
                ),
                MultiphasePhase(
                    name=f"drop_wide_{name}", run_batch=_make_drop_wide(spec)
                ),
                MultiphasePhase(
                    name=f"rebuild_indexes_{name}",
                    run_batch=_make_rebuild_indexes(
                        spec, f"detach_indexes_{name}"
                    ),
                ),
            ]
        )
    for spec in _TABLES:
        phases.append(
            MultiphasePhase(
                name=f"validate_fk_{spec['name']}",
                run_batch=_make_validate_fk(spec),
            )
        )
    phases.append(MultiphasePhase(name="analyze", run_batch=_run_analyze))
    return MultiphaseSpec(migration_id=MIGRATION_ID, phases=tuple(phases))
