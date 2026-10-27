"""Protection-unit census for the tiered rollback proof.

Assigns every data-bearing relation in a deployment database to a
protection tier and measures the facts the tiered design needs
(`pact/changes/chronos-tiered-rollback-proof`):

- ``core`` — mutable operational state; protected by copy + test-restore.
  Every relation lands here in this census version; the cheaper tiers
  are *candidacies* settled by measurement, never granted by the census
  alone (fail-closed: the census informs the policy, it does not relax
  it).
- ``sealed_candidate`` — chronos interval history whose *closed* rows
  (``live_hi`` below the default branch's open bound; rows of other
  branches are not told apart and count as closed) could be digest-sealed. The
  census measures row topology only. It never converts a row ratio into
  physical-byte or dump-volume savings.
- ``rebuildable_candidate`` — derived read models with existing rebuild
  machinery whose full input closure is named.

Anything no rule matches is ``core`` with ``unruled=True`` so an
engineer adds an explicit rule; an unruled relation must never reach a
cheaper tier.

The census records the schema fingerprint and the chronos compaction
watermark it was generated against; consumers must treat a census whose
fingerprint or watermark no longer matches the live database as stale.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from psycopg import sql
from psycopg.rows import dict_row

from okg.substrate.db.cold_history import resolve_open_interval_value

_SYSTEM_SCHEMAS = ("pg_catalog", "information_schema", "pg_toast")

_CHRONOS_INTERVAL_COLUMNS = {"live_lo", "live_hi", "writer_segment_id", "deleted"}


def _relation_inventory(cur) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT n.nspname, c.relname, c.relkind,
               pg_total_relation_size(c.oid)
          FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE c.relkind IN ('r', 'm', 'p', 'S')
           AND n.nspname != ALL(%s::text[])
           AND n.nspname NOT LIKE 'pg_temp%%'
           AND n.nspname NOT LIKE 'pg_toast_temp%%'
         ORDER BY n.nspname, c.relname
        """,
        (list(_SYSTEM_SCHEMAS),),
    )
    return [
        {
            "schema": row[0],
            "name": row[1],
            "relkind": row[2],
            "total_bytes": int(row[3]),
        }
        for row in cur.fetchall()
    ]


def _columns(cur, schema: str, name: str) -> set[str]:
    cur.execute(
        """
        SELECT a.attname
          FROM pg_attribute a
         WHERE a.attrelid = format('%%I.%%I', %s::text, %s::text)::regclass
           AND a.attnum > 0 AND NOT a.attisdropped
        """,
        (schema, name),
    )
    return {row[0] for row in cur.fetchall()}


def _schema_fingerprint(cur) -> str:
    """Digest the schema shape that protection rules depend on.

    Relation names alone are insufficient: an in-place ``ALTER TABLE`` can
    invalidate a protection rule without changing the relation inventory or
    migration ledger.  The digest therefore includes columns, defaults,
    partitioning, views, constraints, indexes, triggers, RLS policies,
    types, functions, sequences, and extensions across every non-system
    schema.
    """
    cur.execute(
        """
        SELECT n.nspname, c.relname, c.relkind, c.relpersistence,
               c.relispartition, c.relrowsecurity, c.relforcerowsecurity,
               COALESCE(pg_get_expr(c.relpartbound, c.oid, TRUE), ''),
               COALESCE(pg_get_partkeydef(c.oid), ''),
               COALESCE(c.reloptions::text, ''),
               a.attnum, a.attname,
               format_type(a.atttypid, a.atttypmod), a.attnotnull,
               a.attidentity, a.attgenerated,
               COALESCE(pg_get_expr(d.adbin, d.adrelid), '')
          FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
          LEFT JOIN pg_attribute a
            ON a.attrelid = c.oid
           AND a.attnum > 0
           AND NOT a.attisdropped
          LEFT JOIN pg_attrdef d
            ON d.adrelid = c.oid AND d.adnum = a.attnum
         WHERE n.nspname != ALL(%s::text[])
           AND n.nspname NOT LIKE 'pg_temp%%'
           AND n.nspname NOT LIKE 'pg_toast_temp%%'
         ORDER BY n.nspname, c.relname, a.attnum NULLS FIRST
        """,
        (list(_SYSTEM_SCHEMAS),),
    )
    relations = [list(row) for row in cur.fetchall()]
    cur.execute(
        """
        SELECT n.nspname, c.relname, c.relkind, pg_get_viewdef(c.oid, TRUE)
          FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE c.relkind IN ('v', 'm')
           AND n.nspname != ALL(%s::text[])
           AND n.nspname NOT LIKE 'pg_temp%%'
           AND n.nspname NOT LIKE 'pg_toast_temp%%'
         ORDER BY n.nspname, c.relname
        """,
        (list(_SYSTEM_SCHEMAS),),
    )
    views = [list(row) for row in cur.fetchall()]
    cur.execute(
        """
        SELECT n.nspname, c.relname, con.conname, con.contype,
               pg_get_constraintdef(con.oid, TRUE)
          FROM pg_constraint con
          JOIN pg_class c ON c.oid = con.conrelid
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname != ALL(%s::text[])
           AND n.nspname NOT LIKE 'pg_temp%%'
           AND n.nspname NOT LIKE 'pg_toast_temp%%'
         ORDER BY n.nspname, c.relname, con.conname
        """,
        (list(_SYSTEM_SCHEMAS),),
    )
    constraints = [list(row) for row in cur.fetchall()]
    cur.execute(
        """
        SELECT n.nspname, tab.relname, idx.relname,
               pg_get_indexdef(i.indexrelid)
          FROM pg_index i
          JOIN pg_class tab ON tab.oid = i.indrelid
          JOIN pg_class idx ON idx.oid = i.indexrelid
          JOIN pg_namespace n ON n.oid = tab.relnamespace
         WHERE n.nspname != ALL(%s::text[])
           AND n.nspname NOT LIKE 'pg_temp%%'
           AND n.nspname NOT LIKE 'pg_toast_temp%%'
         ORDER BY n.nspname, tab.relname, idx.relname
        """,
        (list(_SYSTEM_SCHEMAS),),
    )
    indexes = [list(row) for row in cur.fetchall()]
    cur.execute(
        """
        SELECT n.nspname, c.relname, t.tgname,
               pg_get_triggerdef(t.oid, TRUE), t.tgenabled::text
          FROM pg_trigger t
          JOIN pg_class c ON c.oid = t.tgrelid
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE NOT t.tgisinternal
           AND n.nspname != ALL(%s::text[])
           AND n.nspname NOT LIKE 'pg_temp%%'
           AND n.nspname NOT LIKE 'pg_toast_temp%%'
         ORDER BY n.nspname, c.relname, t.tgname
        """,
        (list(_SYSTEM_SCHEMAS),),
    )
    triggers = [list(row) for row in cur.fetchall()]
    cur.execute(
        """
        SELECT n.nspname, c.relname, p.polname, p.polcmd, p.polpermissive,
               pg_get_expr(p.polqual, p.polrelid, TRUE),
               pg_get_expr(p.polwithcheck, p.polrelid, TRUE),
               ARRAY(
                 SELECT r.rolname FROM pg_roles r
                  WHERE r.oid = ANY(p.polroles) ORDER BY r.rolname
               )
          FROM pg_policy p
          JOIN pg_class c ON c.oid = p.polrelid
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname != ALL(%s::text[])
           AND n.nspname NOT LIKE 'pg_temp%%'
           AND n.nspname NOT LIKE 'pg_toast_temp%%'
         ORDER BY n.nspname, c.relname, p.polname
        """,
        (list(_SYSTEM_SCHEMAS),),
    )
    policies = [list(row) for row in cur.fetchall()]
    cur.execute(
        """
        SELECT n.nspname, t.typname, t.typtype, t.typcategory,
               format_type(t.oid, NULL),
               COALESCE(format_type(t.typbasetype, t.typtypmod), ''),
               t.typnotnull, COALESCE(t.typdefault, ''),
               COALESCE((
                 SELECT jsonb_agg(
                   jsonb_build_array(e.enumsortorder, e.enumlabel)
                   ORDER BY e.enumsortorder
                 ) FROM pg_enum e WHERE e.enumtypid = t.oid
               ), '[]'::jsonb)::text
          FROM pg_type t
          JOIN pg_namespace n ON n.oid = t.typnamespace
         WHERE t.typrelid = 0
           AND n.nspname != ALL(%s::text[])
           AND n.nspname NOT LIKE 'pg_temp%%'
           AND n.nspname NOT LIKE 'pg_toast_temp%%'
         ORDER BY n.nspname, t.typname
        """,
        (list(_SYSTEM_SCHEMAS),),
    )
    types = [list(row) for row in cur.fetchall()]
    cur.execute(
        """
        SELECT n.nspname, t.typname, con.conname, con.contype,
               pg_get_constraintdef(con.oid, TRUE)
          FROM pg_constraint con
          JOIN pg_type t ON t.oid = con.contypid
          JOIN pg_namespace n ON n.oid = t.typnamespace
         WHERE n.nspname != ALL(%s::text[])
           AND n.nspname NOT LIKE 'pg_temp%%'
           AND n.nspname NOT LIKE 'pg_toast_temp%%'
         ORDER BY n.nspname, t.typname, con.conname
        """,
        (list(_SYSTEM_SCHEMAS),),
    )
    domain_constraints = [list(row) for row in cur.fetchall()]
    cur.execute(
        """
        SELECT n.nspname, p.proname,
               pg_get_function_identity_arguments(p.oid),
               pg_get_function_result(p.oid), pg_get_functiondef(p.oid),
               p.provolatile, p.proparallel, p.prosecdef, p.proisstrict,
               COALESCE(p.proconfig::text, '')
          FROM pg_proc p
          JOIN pg_namespace n ON n.oid = p.pronamespace
         WHERE p.prokind IN ('f', 'p')
           AND n.nspname != ALL(%s::text[])
           AND n.nspname NOT LIKE 'pg_temp%%'
           AND n.nspname NOT LIKE 'pg_toast_temp%%'
         ORDER BY n.nspname, p.proname,
                  pg_get_function_identity_arguments(p.oid)
        """,
        (list(_SYSTEM_SCHEMAS),),
    )
    functions = [list(row) for row in cur.fetchall()]
    cur.execute(
        """
        SELECT n.nspname, c.relname, format_type(s.seqtypid, NULL),
               s.seqstart, s.seqincrement, s.seqmax, s.seqmin,
               s.seqcache, s.seqcycle
          FROM pg_sequence s
          JOIN pg_class c ON c.oid = s.seqrelid
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname != ALL(%s::text[])
           AND n.nspname NOT LIKE 'pg_temp%%'
           AND n.nspname NOT LIKE 'pg_toast_temp%%'
         ORDER BY n.nspname, c.relname
        """,
        (list(_SYSTEM_SCHEMAS),),
    )
    sequences = [list(row) for row in cur.fetchall()]
    cur.execute(
        "SELECT extname, extversion FROM pg_extension ORDER BY extname"
    )
    extensions = [list(row) for row in cur.fetchall()]
    cur.execute("SELECT to_regclass('okg.schema_migrations') IS NOT NULL")
    migrations: list[list[Any]] = []
    if cur.fetchone()[0]:
        cur.execute(
            "SELECT migration_id, checksum FROM okg.schema_migrations "
            "ORDER BY migration_id"
        )
        migrations = [list(row) for row in cur.fetchall()]
    payload = json.dumps(
        {
            "relations": relations,
            "views": views,
            "constraints": constraints,
            "indexes": indexes,
            "triggers": triggers,
            "policies": policies,
            "types": types,
            "domain_constraints": domain_constraints,
            "functions": functions,
            "sequences": sequences,
            "extensions": extensions,
            "migrations": migrations,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _compaction_watermark(cur) -> dict[str, Any] | None:
    """Chronos segment/checkpoint high-water mark, or None when absent."""
    cur.execute(
        "SELECT to_regclass('_chronos_branch_interval_segments') IS NOT NULL"
    )
    if not cur.fetchone()[0]:
        return None
    cur.execute(
        """
        SELECT count(*), COALESCE(max(segment_id), 0),
               count(*) FILTER (WHERE segment_kind = 'checkpoint')
          FROM _chronos_branch_interval_segments
        """
    )
    total, max_segment, checkpoints = cur.fetchone()
    return {
        "segment_count": int(total),
        "max_segment_id": int(max_segment),
        "checkpoint_count": int(checkpoints),
    }


def _measure_interval_coldness(
    cur, schema: str, name: str, open_interval_value: int
) -> dict[str, Any]:
    cur.execute(
        sql.SQL(
            """
        SELECT count(*),
               count(*) FILTER (
                   WHERE live_hi < %s::numeric
                     AND NOT deleted
               ),
               count(*) FILTER (WHERE deleted)
          FROM {} AS interval_rows
        """
        ).format(sql.Identifier(schema, name)),
        (open_interval_value,),
    )
    total, closed, deleted = cur.fetchone()
    total = int(total)
    return {
        "row_count": total,
        "closed_non_deleted_row_count": int(closed),
        "deleted_row_count": int(deleted),
        "closed_non_deleted_row_fraction": (
            int(closed) / total
        ) if total else 0.0,
        "measurement_basis": "row_count_only_not_byte_share",
    }


def generate_protection_census(conn) -> dict[str, Any]:
    """Generate the census against a live connection (read-only)."""
    entries: list[dict[str, Any]] = []
    observed_at = datetime.now(timezone.utc).isoformat()
    open_interval_value: int | None = None
    with conn.cursor() as cur:
        fingerprint = _schema_fingerprint(cur)
        watermark = _compaction_watermark(cur)
        for relation in _relation_inventory(cur):
            schema, name = relation["schema"], relation["name"]
            entry: dict[str, Any] = {
                **relation,
                "tier": "core",
                "unruled": False,
                # Census v1 grants no cheap tier.  Account every physical
                # relation byte to core; row topology is candidacy evidence,
                # never a proxy for bytes or dump volume.
                "byte_allocation": {
                    "core_bytes": relation["total_bytes"],
                    "sealed_candidate_bytes": 0,
                    "rebuildable_candidate_bytes": 0,
                    "basis": "physical_relation_bytes_fail_closed",
                },
            }
            if schema == "public" and name.startswith("_chronos_b_interval_"):
                columns = _columns(cur, schema, name)
                if _CHRONOS_INTERVAL_COLUMNS <= columns:
                    entry["rule"] = "chronos_interval_history"
                    entry["sealed_candidate"] = True
                    if open_interval_value is None:
                        # The segment tables' open bound is the default
                        # branch's current live_hi, not a fixed constant.
                        with conn.cursor(row_factory=dict_row) as dict_cur:
                            open_interval_value = resolve_open_interval_value(
                                dict_cur
                            )
                    entry["coldness"] = _measure_interval_coldness(
                        cur, schema, name, open_interval_value
                    )
                else:
                    entry["rule"] = "chronos_interval_shape_mismatch"
            elif schema == "public" and name.startswith("_chronos_"):
                entry["rule"] = "chronos_metadata"
            elif schema == "okg":
                entry["rule"] = "okg_operational"
            elif schema == "okg_derived":
                entry["rule"] = "okg_derived"
            elif schema == "dbos":
                entry["rule"] = "dbos_runtime"
            elif schema.startswith("_timescaledb"):
                entry["rule"] = "timescaledb_internal"
            elif schema == "public" and name in (
                "spatial_ref_sys",
            ):
                entry["rule"] = "extension_owned"
            else:
                entry["rule"] = None
                entry["unruled"] = True
            entries.append(entry)
    unruled = [e for e in entries if e["unruled"]]
    total_bytes = sum(e["total_bytes"] for e in entries)
    core_bytes = sum(e["byte_allocation"]["core_bytes"] for e in entries)
    return {
        "schema": "okg.protection_census.v1",
        "observed_at": observed_at,
        "schema_fingerprint": fingerprint,
        "compaction_watermark": watermark,
        "entry_count": len(entries),
        "byte_totals": {
            "total_bytes": total_bytes,
            "core_bytes": core_bytes,
            "sealed_candidate_bytes": 0,
            "rebuildable_candidate_bytes": 0,
            "fully_assigned": total_bytes == core_bytes,
        },
        "unruled_count": len(unruled),
        "unruled": [
            {"schema": e["schema"], "name": e["name"]} for e in unruled
        ],
        "entries": entries,
    }
