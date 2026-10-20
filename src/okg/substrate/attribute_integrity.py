"""Integrity receipts for the interval-backed effective-attribute read model.

The receipt is a *proof cache*, never an authority for graph facts.  A full
repair establishes an anchor after scanning the pinned snapshot and interval
front.  Later publishers advance the two commutative payload accumulators from
the exact changed-target set.  Statement triggers dirty the cache in the same
transaction as every protected-table write, so startup may use the constant-
work path only when the latest receipt is clean and exactly generation-pinned.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any, Literal

import psycopg
from psycopg.rows import dict_row

from okg.substrate.errors import SubstrateError


ATTRIBUTE_INTEGRITY_STATE_TABLE = "okg.attribute_effective_integrity_state"
ATTRIBUTE_INTEGRITY_RECEIPT_TABLE = "okg.attribute_effective_integrity_receipts"
ATTRIBUTE_INTEGRITY_TRANSITION_TABLE = (
    "okg.attribute_effective_integrity_transitions"
)
ATTRIBUTE_EFFECTIVE_VIEW_TABLE = "okg.attribute_effective_view"
ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE = "okg.attribute_effective_view_intervals"
ATTRIBUTE_INTEGRITY_SCHEMA_VERSION = 2

# schema-interval-narrow-split: the jsonb payload columns are replaced by the
# content-addressed ``attrs_hash`` (bytea) reference into okg.attribute_versions.
# Fingerprinting over the hash preserves the snapshot-vs-interval parity
# contract exactly — both sides reference the SAME version store, so equal
# hashes mean equal payloads, and hash honesty itself is proven by the
# recompute-and-compare doctor check (check_attribute_version_hash_honesty).
_PAYLOAD_COLUMNS = (
    "subtype",
    "src",
    "dst",
    "edge_type",
    "overlay_count",
    "raw_text_hash",
    "attrs_hash",
)
_SEEDS = (0, 1)


def _stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _sha256(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_stable_json(value).encode("utf-8")).hexdigest()


def attribute_integrity_receipt_digest(receipt: Mapping[str, Any]) -> str:
    unsigned = dict(receipt)
    unsigned.pop("receipt_digest", None)
    unsigned.pop("created_at", None)
    return _sha256(unsigned)


_RESTRICTED_RUNTIME_ROLES = ("okg_substrate", "app_rw", "app_ro", "okg_mcp")
_FINALIZE_REGPROCEDURE = (
    "okg.finalize_attribute_integrity_receipt("
    "text,text,text,text,text,text,bigint,text,bigint,bigint,bigint,"
    "bigint,integer,text,text,numeric,numeric,numeric,numeric,text,bigint)"
)


def ensure_attribute_integrity_schema(conn: psycopg.Connection) -> None:
    """Install the append-only receipts and statement-level dirtying triggers.

    Installation makes the connecting role the owner of every table and
    SECURITY DEFINER function it creates.  A restricted runtime role owning
    the authority surface defeats the split (owners can re-grant themselves
    and disable triggers), so under those roles this function never runs
    DDL: with the schema fully installed it is a no-op (migrations own
    convergence), and with it missing or incomplete it refuses loudly
    instead of minting a runtime-owned authority surface.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT current_user")
        connecting_role = str(cur.fetchone()[0])
    if connecting_role in _RESTRICTED_RUNTIME_ROLES:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT to_regclass(%s) IS NOT NULL"
                " AND to_regclass(%s) IS NOT NULL"
                " AND to_regclass(%s) IS NOT NULL"
                " AND to_regclass('okg.attribute_effective_view')"
                "     IS NOT NULL"
                " AND to_regclass('okg.attribute_effective_view_intervals')"
                "     IS NOT NULL"
                " AND to_regprocedure(%s) IS NOT NULL"
                " AND to_regprocedure(%s) IS NOT NULL",
                (
                    ATTRIBUTE_INTEGRITY_STATE_TABLE,
                    ATTRIBUTE_INTEGRITY_RECEIPT_TABLE,
                    ATTRIBUTE_INTEGRITY_TRANSITION_TABLE,
                    _FINALIZE_REGPROCEDURE,
                    "okg.mark_attribute_effective_integrity_dirty_rows()",
                ),
            )
            objects_installed = bool(cur.fetchone()[0])
            # The dirtying triggers are as load-bearing as the tables: a
            # dropped or disabled trigger means writes escape invalidation,
            # and a full re-anchor would bake that broken surface into a
            # fresh receipt.  Demand the exact expected set — three scoped
            # row triggers plus the global TRUNCATE trigger on each
            # protected table, every one enabled — plus the receipts
            # append-only trigger, before calling the schema installed.
            triggers_complete = False
            if objects_installed:
                cur.execute(
                    """
                    SELECT (
                      SELECT count(*) FROM pg_trigger t
                       WHERE t.tgrelid IN (
                               'okg.attribute_effective_view'::regclass,
                               'okg.attribute_effective_view_intervals'
                                 ::regclass
                             )
                         AND NOT t.tgisinternal
                         AND t.tgenabled = 'O'
                         AND (
                           t.tgname LIKE 'attribute_integrity_scoped_%%'
                           OR t.tgname
                              LIKE 'attribute_integrity_dirty_truncate_%%'
                         )
                    ) = 8
                    AND EXISTS (
                      SELECT 1 FROM pg_trigger t
                       WHERE t.tgrelid = %s::regclass
                         AND t.tgname =
                             'attribute_integrity_receipts_immutable'
                         AND t.tgenabled = 'O'
                    )
                    """,
                    (ATTRIBUTE_INTEGRITY_RECEIPT_TABLE,),
                )
                triggers_complete = bool(cur.fetchone()[0])
        if objects_installed and triggers_complete:
            return
        raise SubstrateError(
            "attribute integrity schema is missing or incomplete "
            "(tables, functions, or enabled dirtying triggers) and cannot "
            f"be installed by restricted runtime role '{connecting_role}'; "
            "run `okg migrate` as the deployment owner"
        )
    statements = (
        f"""
        CREATE TABLE IF NOT EXISTS {ATTRIBUTE_INTEGRITY_STATE_TABLE} (
          deployment_name TEXT NOT NULL,
          branch_id TEXT NOT NULL,
          namespace TEXT NOT NULL,
          mutation_epoch BIGINT NOT NULL DEFAULT 0,
          clean_mutation_epoch BIGINT,
          dirty BOOLEAN NOT NULL DEFAULT TRUE,
          dirty_reason TEXT NOT NULL DEFAULT 'unanchored',
          latest_receipt_digest TEXT,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          PRIMARY KEY (deployment_name, branch_id, namespace)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {ATTRIBUTE_INTEGRITY_TRANSITION_TABLE} (
          deployment_name TEXT NOT NULL,
          branch_id TEXT NOT NULL,
          namespace TEXT NOT NULL,
          backend_pid INTEGER NOT NULL,
          backend_start TIMESTAMPTZ NOT NULL,
          started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          PRIMARY KEY (deployment_name, branch_id, namespace)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {ATTRIBUTE_INTEGRITY_RECEIPT_TABLE} (
          receipt_digest TEXT PRIMARY KEY,
          receipt_type TEXT NOT NULL CHECK (receipt_type IN ('anchor', 'incremental')),
          deployment_name TEXT NOT NULL,
          branch_id TEXT NOT NULL,
          namespace TEXT NOT NULL,
          generation_id TEXT NOT NULL,
          ordinal BIGINT NOT NULL,
          prior_receipt_digest TEXT,
          checkpoint_node_count BIGINT NOT NULL,
          checkpoint_edge_count BIGINT NOT NULL,
          logical_node_count BIGINT NOT NULL,
          logical_edge_count BIGINT NOT NULL,
          schema_version INTEGER NOT NULL,
          schema_digest TEXT NOT NULL,
          migration_digest TEXT NOT NULL,
          logical_payload_seed0 NUMERIC NOT NULL,
          logical_payload_seed1 NUMERIC NOT NULL,
          interval_payload_seed0 NUMERIC NOT NULL,
          interval_payload_seed1 NUMERIC NOT NULL,
          dirty_scope_digest TEXT NOT NULL,
          mutation_epoch BIGINT NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          FOREIGN KEY (prior_receipt_digest)
            REFERENCES {ATTRIBUTE_INTEGRITY_RECEIPT_TABLE}(receipt_digest)
        )
        """,
        f"""
        CREATE INDEX IF NOT EXISTS attribute_effective_integrity_receipt_scope_idx
          ON {ATTRIBUTE_INTEGRITY_RECEIPT_TABLE}
          (deployment_name, branch_id, namespace, ordinal DESC)
        """,
        """
        CREATE OR REPLACE FUNCTION okg.begin_attribute_integrity_transition(
          p_deployment TEXT, p_branch TEXT, p_namespace TEXT
        ) RETURNS BOOLEAN
        LANGUAGE plpgsql SECURITY DEFINER
        SET search_path = pg_catalog, okg
        AS $function$
        BEGIN
          DELETE FROM okg.attribute_effective_integrity_transitions
           WHERE deployment_name = p_deployment AND branch_id = p_branch
             AND namespace = p_namespace;
          INSERT INTO okg.attribute_effective_integrity_transitions
            (deployment_name, branch_id, namespace, backend_pid, backend_start)
          SELECT p_deployment, p_branch, p_namespace, pg_backend_pid(), backend_start
            FROM pg_stat_activity WHERE pid = pg_backend_pid();
          IF NOT FOUND THEN
            RAISE EXCEPTION 'current backend identity is unavailable';
          END IF;
          RETURN TRUE;
        END
        $function$
        """,
        """
        CREATE OR REPLACE FUNCTION okg.attribute_integrity_transition_matches(
          p_deployment TEXT, p_branch TEXT, p_namespace TEXT
        ) RETURNS BOOLEAN
        LANGUAGE sql STABLE SECURITY DEFINER
        SET search_path = pg_catalog, okg
        AS $function$
          SELECT EXISTS (
            SELECT 1 FROM okg.attribute_effective_integrity_transitions
             WHERE deployment_name = p_deployment AND branch_id = p_branch
               AND namespace = p_namespace
               AND backend_pid = pg_backend_pid()
               AND backend_start = (
                 SELECT backend_start FROM pg_stat_activity
                  WHERE pid = pg_backend_pid()
               )
          )
        $function$
        """,
        """
        CREATE OR REPLACE FUNCTION okg.finish_attribute_integrity_transition(
          p_deployment TEXT, p_branch TEXT, p_namespace TEXT
        ) RETURNS BOOLEAN
        LANGUAGE plpgsql SECURITY DEFINER
        SET search_path = pg_catalog, okg
        AS $function$
        DECLARE affected INTEGER;
        BEGIN
          DELETE FROM okg.attribute_effective_integrity_transitions
           WHERE deployment_name = p_deployment AND branch_id = p_branch
             AND namespace = p_namespace
             AND backend_pid = pg_backend_pid()
             AND backend_start = (
               SELECT backend_start FROM pg_stat_activity
                WHERE pid = pg_backend_pid()
             );
          GET DIAGNOSTICS affected = ROW_COUNT;
          RETURN affected = 1;
        END
        $function$
        """,
        """
        CREATE OR REPLACE FUNCTION okg.abort_attribute_integrity_transition(
          p_deployment TEXT, p_branch TEXT, p_namespace TEXT
        ) RETURNS VOID
        LANGUAGE sql SECURITY DEFINER
        SET search_path = pg_catalog, okg
        AS $function$
          DELETE FROM okg.attribute_effective_integrity_transitions
           WHERE deployment_name = p_deployment AND branch_id = p_branch
             AND namespace = p_namespace
             AND backend_pid = pg_backend_pid()
             AND backend_start = (
               SELECT backend_start FROM pg_stat_activity
                WHERE pid = pg_backend_pid()
             )
        $function$
        """,
        """
        CREATE OR REPLACE FUNCTION okg.register_attribute_integrity_scope(
          p_deployment TEXT, p_branch TEXT, p_namespace TEXT
        ) RETURNS VOID
        LANGUAGE sql SECURITY DEFINER
        SET search_path = pg_catalog, okg
        AS $function$
          INSERT INTO okg.attribute_effective_integrity_state
            (deployment_name, branch_id, namespace)
          VALUES (p_deployment, p_branch, p_namespace)
          ON CONFLICT (deployment_name, branch_id, namespace) DO NOTHING
        $function$
        """,
        """
        CREATE OR REPLACE FUNCTION okg.mark_attribute_integrity_scope_dirty(
          p_deployment TEXT, p_branch TEXT, p_namespace TEXT, p_reason TEXT
        ) RETURNS VOID
        LANGUAGE sql SECURITY DEFINER
        SET search_path = pg_catalog, okg
        AS $function$
          UPDATE okg.attribute_effective_integrity_state
             SET mutation_epoch = mutation_epoch + 1,
                 dirty = TRUE,
                 dirty_reason = p_reason,
                 updated_at = now()
           WHERE deployment_name = p_deployment AND branch_id = p_branch
             AND namespace = p_namespace
        $function$
        """,
        """
        CREATE OR REPLACE FUNCTION okg.finalize_attribute_integrity_receipt(
          p_receipt_digest TEXT,
          p_receipt_type TEXT,
          p_deployment TEXT,
          p_branch TEXT,
          p_namespace TEXT,
          p_generation_id TEXT,
          p_ordinal BIGINT,
          p_prior_receipt_digest TEXT,
          p_checkpoint_node_count BIGINT,
          p_checkpoint_edge_count BIGINT,
          p_logical_node_count BIGINT,
          p_logical_edge_count BIGINT,
          p_schema_version INTEGER,
          p_schema_digest TEXT,
          p_migration_digest TEXT,
          p_logical_payload_seed0 NUMERIC,
          p_logical_payload_seed1 NUMERIC,
          p_interval_payload_seed0 NUMERIC,
          p_interval_payload_seed1 NUMERIC,
          p_dirty_scope_digest TEXT,
          p_mutation_epoch BIGINT
        ) RETURNS BOOLEAN
        LANGUAGE plpgsql SECURITY DEFINER
        SET search_path = pg_catalog, okg
        AS $function$
        BEGIN
          -- The calling backend must own the live transition token for this
          -- scope; finalization consumes it atomically with the receipt
          -- append and the clean-state compare-and-set on the mutation
          -- epoch.  This function is the only clean-marking authority:
          -- direct DML on the state and receipt tables is revoked from the
          -- runtime and application roles.
          IF NOT okg.finish_attribute_integrity_transition(
            p_deployment, p_branch, p_namespace
          ) THEN
            RAISE EXCEPTION
              'attribute integrity transition changed during finalization';
          END IF;
          INSERT INTO okg.attribute_effective_integrity_receipts (
            receipt_digest, receipt_type, deployment_name, branch_id,
            namespace, generation_id, ordinal, prior_receipt_digest,
            checkpoint_node_count, checkpoint_edge_count,
            logical_node_count, logical_edge_count,
            schema_version, schema_digest, migration_digest,
            logical_payload_seed0, logical_payload_seed1,
            interval_payload_seed0, interval_payload_seed1,
            dirty_scope_digest, mutation_epoch
          ) VALUES (
            p_receipt_digest, p_receipt_type, p_deployment, p_branch,
            p_namespace, p_generation_id, p_ordinal, p_prior_receipt_digest,
            p_checkpoint_node_count, p_checkpoint_edge_count,
            p_logical_node_count, p_logical_edge_count,
            p_schema_version, p_schema_digest, p_migration_digest,
            p_logical_payload_seed0, p_logical_payload_seed1,
            p_interval_payload_seed0, p_interval_payload_seed1,
            p_dirty_scope_digest, p_mutation_epoch
          );
          UPDATE okg.attribute_effective_integrity_state
             SET clean_mutation_epoch = p_mutation_epoch,
                 dirty = FALSE,
                 dirty_reason = 'clean',
                 latest_receipt_digest = p_receipt_digest,
                 updated_at = now()
           WHERE deployment_name = p_deployment AND branch_id = p_branch
             AND namespace = p_namespace
             AND mutation_epoch = p_mutation_epoch
             -- Incremental receipts must extend the scope's current chain
             -- head; an anchor (NULL prior) resets the chain.
             AND (p_prior_receipt_digest IS NULL
                  OR latest_receipt_digest
                     IS NOT DISTINCT FROM p_prior_receipt_digest);
          IF NOT FOUND THEN
            -- Distinguish a chain fork (epoch matched, prior did not)
            -- from the benign transition/epoch race for operability.
            IF EXISTS (
              SELECT 1 FROM okg.attribute_effective_integrity_state
               WHERE deployment_name = p_deployment
                 AND branch_id = p_branch
                 AND namespace = p_namespace
                 AND mutation_epoch = p_mutation_epoch
            ) THEN
              RAISE EXCEPTION
                'attribute integrity receipt chain head moved during finalization';
            END IF;
            RAISE EXCEPTION
              'attribute integrity transition changed during finalization';
          END IF;
          RETURN TRUE;
        END
        $function$
        """,
        """
        CREATE OR REPLACE FUNCTION okg.mark_attribute_effective_integrity_dirty()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, okg
        AS $function$
        BEGIN
          -- TRUNCATE has no transition tables, so its invalidation is
          -- necessarily global: every scope's epoch advances and every
          -- other backend's transition token dies.
          UPDATE okg.attribute_effective_integrity_state
             SET mutation_epoch = mutation_epoch + 1,
                 dirty = TRUE,
                 dirty_reason = TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME || ':' || TG_OP,
                 updated_at = now();
          DELETE FROM okg.attribute_effective_integrity_transitions
           WHERE backend_pid <> pg_backend_pid()
              OR backend_start IS DISTINCT FROM (
                SELECT backend_start FROM pg_stat_activity
                 WHERE pid = pg_backend_pid()
              );
          RETURN NULL;
        END
        $function$
        """,
        """
        CREATE OR REPLACE FUNCTION okg.mark_attribute_effective_integrity_dirty_rows()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, okg
        AS $function$
        BEGIN
          -- Scoped invalidation from the statement's transition table
          -- (NEW TABLE for INSERT, OLD TABLE for DELETE; both are named
          -- changed_rows by the trigger). Only the scopes the statement
          -- actually touched lose their clean proof; the interval table
          -- carries the branch, the snapshot table dirties every branch
          -- of the touched namespaces. A zero-row statement dirties
          -- nothing, which is exact.
          IF TG_TABLE_NAME = 'attribute_effective_view_intervals' THEN
            UPDATE okg.attribute_effective_integrity_state s
               SET mutation_epoch = s.mutation_epoch + 1,
                   dirty = TRUE,
                   dirty_reason = TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME
                     || ':' || TG_OP,
                   updated_at = now()
             WHERE EXISTS (
               SELECT 1 FROM changed_rows c
                WHERE c.namespace = s.namespace AND c.branch_id = s.branch_id
             );
            DELETE FROM okg.attribute_effective_integrity_transitions t
             WHERE (t.backend_pid <> pg_backend_pid()
                    OR t.backend_start IS DISTINCT FROM (
                      SELECT backend_start FROM pg_stat_activity
                       WHERE pid = pg_backend_pid()
                    ))
               AND EXISTS (
                 SELECT 1 FROM changed_rows c
                  WHERE c.namespace = t.namespace
                    AND c.branch_id = t.branch_id
               );
          ELSE
            UPDATE okg.attribute_effective_integrity_state s
               SET mutation_epoch = s.mutation_epoch + 1,
                   dirty = TRUE,
                   dirty_reason = TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME
                     || ':' || TG_OP,
                   updated_at = now()
             WHERE EXISTS (
               SELECT 1 FROM changed_rows c WHERE c.namespace = s.namespace
             );
            DELETE FROM okg.attribute_effective_integrity_transitions t
             WHERE (t.backend_pid <> pg_backend_pid()
                    OR t.backend_start IS DISTINCT FROM (
                      SELECT backend_start FROM pg_stat_activity
                       WHERE pid = pg_backend_pid()
                    ))
               AND EXISTS (
                 SELECT 1 FROM changed_rows c WHERE c.namespace = t.namespace
               );
          END IF;
          RETURN NULL;
        END
        $function$
        """,
        """
        CREATE OR REPLACE FUNCTION okg.mark_attribute_effective_integrity_dirty_update()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, okg
        AS $function$
        BEGIN
          -- UPDATE scope is the union of the old and new row coordinates:
          -- a row moved across namespaces or branches dirties both sides.
          IF TG_TABLE_NAME = 'attribute_effective_view_intervals' THEN
            UPDATE okg.attribute_effective_integrity_state s
               SET mutation_epoch = s.mutation_epoch + 1,
                   dirty = TRUE,
                   dirty_reason = TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME
                     || ':' || TG_OP,
                   updated_at = now()
             WHERE EXISTS (
                 SELECT 1 FROM old_rows c
                  WHERE c.namespace = s.namespace
                    AND c.branch_id = s.branch_id
               )
                OR EXISTS (
                 SELECT 1 FROM new_rows c
                  WHERE c.namespace = s.namespace
                    AND c.branch_id = s.branch_id
               );
            DELETE FROM okg.attribute_effective_integrity_transitions t
             WHERE (t.backend_pid <> pg_backend_pid()
                    OR t.backend_start IS DISTINCT FROM (
                      SELECT backend_start FROM pg_stat_activity
                       WHERE pid = pg_backend_pid()
                    ))
               AND (EXISTS (
                     SELECT 1 FROM old_rows c
                      WHERE c.namespace = t.namespace
                        AND c.branch_id = t.branch_id
                   )
                    OR EXISTS (
                     SELECT 1 FROM new_rows c
                      WHERE c.namespace = t.namespace
                        AND c.branch_id = t.branch_id
                   ));
          ELSE
            UPDATE okg.attribute_effective_integrity_state s
               SET mutation_epoch = s.mutation_epoch + 1,
                   dirty = TRUE,
                   dirty_reason = TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME
                     || ':' || TG_OP,
                   updated_at = now()
             WHERE EXISTS (
                 SELECT 1 FROM old_rows c WHERE c.namespace = s.namespace
               )
                OR EXISTS (
                 SELECT 1 FROM new_rows c WHERE c.namespace = s.namespace
               );
            DELETE FROM okg.attribute_effective_integrity_transitions t
             WHERE (t.backend_pid <> pg_backend_pid()
                    OR t.backend_start IS DISTINCT FROM (
                      SELECT backend_start FROM pg_stat_activity
                       WHERE pid = pg_backend_pid()
                    ))
               AND (EXISTS (
                     SELECT 1 FROM old_rows c
                      WHERE c.namespace = t.namespace
                   )
                    OR EXISTS (
                     SELECT 1 FROM new_rows c
                      WHERE c.namespace = t.namespace
                   ));
          END IF;
          RETURN NULL;
        END
        $function$
        """,
        """
        CREATE OR REPLACE FUNCTION okg.reject_attribute_integrity_receipt_mutation()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $function$
        BEGIN
          RAISE EXCEPTION 'attribute integrity receipts are append-only';
        END
        $function$
        """,
        f"""
        DO $triggers$
        DECLARE
          protected_table regclass;
          trigger_name text;
          legacy_name text;
          operation text;
        BEGIN
          FOREACH protected_table IN ARRAY ARRAY[
            'okg.attribute_effective_view'::regclass,
            'okg.attribute_effective_view_intervals'::regclass
          ] LOOP
            FOREACH operation IN ARRAY ARRAY['INSERT', 'UPDATE', 'DELETE'] LOOP
              -- Row operations use transition-table-scoped invalidation;
              -- drop any pre-scoped (global) trigger of the old name.
              -- Put the operation before a compact relation hash.  PostgreSQL
              -- truncates identifiers at 63 bytes; the original descriptive
              -- suffix collided for INSERT/UPDATE on the long intervals name.
              legacy_name := 'attribute_integrity_dirty_' || lower(operation) ||
                '_' || substr(md5(protected_table::text), 1, 10);
              EXECUTE format(
                'DROP TRIGGER IF EXISTS %I ON %s',
                legacy_name, protected_table
              );
              trigger_name := 'attribute_integrity_scoped_' || lower(operation)
                || '_' || substr(md5(protected_table::text), 1, 10);
              IF NOT EXISTS (
                SELECT 1 FROM pg_trigger
                 WHERE tgrelid = protected_table AND tgname = trigger_name
              ) THEN
                IF operation = 'INSERT' THEN
                  EXECUTE format(
                    'CREATE TRIGGER %I AFTER INSERT ON %s '
                    'REFERENCING NEW TABLE AS changed_rows '
                    'FOR EACH STATEMENT EXECUTE FUNCTION '
                    'okg.mark_attribute_effective_integrity_dirty_rows()',
                    trigger_name, protected_table
                  );
                ELSIF operation = 'DELETE' THEN
                  EXECUTE format(
                    'CREATE TRIGGER %I AFTER DELETE ON %s '
                    'REFERENCING OLD TABLE AS changed_rows '
                    'FOR EACH STATEMENT EXECUTE FUNCTION '
                    'okg.mark_attribute_effective_integrity_dirty_rows()',
                    trigger_name, protected_table
                  );
                ELSE
                  EXECUTE format(
                    'CREATE TRIGGER %I AFTER UPDATE ON %s '
                    'REFERENCING OLD TABLE AS old_rows '
                    'NEW TABLE AS new_rows '
                    'FOR EACH STATEMENT EXECUTE FUNCTION '
                    'okg.mark_attribute_effective_integrity_dirty_update()',
                    trigger_name, protected_table
                  );
                END IF;
              END IF;
            END LOOP;
            -- TRUNCATE cannot carry transition tables; it keeps the
            -- global invalidation function.
            trigger_name := 'attribute_integrity_dirty_truncate_' ||
              substr(md5(protected_table::text), 1, 10);
            IF NOT EXISTS (
              SELECT 1 FROM pg_trigger
               WHERE tgrelid = protected_table AND tgname = trigger_name
            ) THEN
              EXECUTE format(
                'CREATE TRIGGER %I AFTER TRUNCATE ON %s FOR EACH STATEMENT '
                'EXECUTE FUNCTION okg.mark_attribute_effective_integrity_dirty()',
                trigger_name, protected_table
              );
            END IF;
          END LOOP;
          IF NOT EXISTS (
            SELECT 1 FROM pg_trigger
             WHERE tgrelid = '{ATTRIBUTE_INTEGRITY_RECEIPT_TABLE}'::regclass
               AND tgname = 'attribute_integrity_receipts_immutable'
          ) THEN
            CREATE TRIGGER attribute_integrity_receipts_immutable
              BEFORE UPDATE OR DELETE OR TRUNCATE
              ON {ATTRIBUTE_INTEGRITY_RECEIPT_TABLE}
              FOR EACH STATEMENT
              EXECUTE FUNCTION okg.reject_attribute_integrity_receipt_mutation();
          END IF;
        END
        $triggers$
        """,
        f"""
        DO $privileges$
        DECLARE role_name text;
        BEGIN
          FOREACH role_name IN ARRAY ARRAY[
            'okg_substrate', 'app_rw', 'app_ro', 'okg_mcp'
          ] LOOP
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
              EXECUTE format(
                'REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON '
                '{ATTRIBUTE_INTEGRITY_STATE_TABLE}, '
                '{ATTRIBUTE_INTEGRITY_RECEIPT_TABLE} FROM %I', role_name
              );
              IF role_name = 'app_ro' THEN
                -- app_ro is intentionally view-only after views.sql
                -- reconciles its allow-list. Keep the runtime self-heal
                -- posture identical so migrate cannot invalidate a healthy
                -- integrity receipt by changing only ACL identity.
                EXECUTE format(
                  'REVOKE SELECT ON {ATTRIBUTE_INTEGRITY_STATE_TABLE}, '
                  '{ATTRIBUTE_INTEGRITY_RECEIPT_TABLE} FROM %I', role_name
                );
              ELSE
                EXECUTE format(
                  'GRANT SELECT ON {ATTRIBUTE_INTEGRITY_STATE_TABLE}, '
                  '{ATTRIBUTE_INTEGRITY_RECEIPT_TABLE} TO %I', role_name
                );
              END IF;
              EXECUTE format(
                'REVOKE ALL ON {ATTRIBUTE_INTEGRITY_TRANSITION_TABLE} FROM %I',
                role_name
              );
            END IF;
          END LOOP;
          REVOKE ALL ON {ATTRIBUTE_INTEGRITY_TRANSITION_TABLE} FROM PUBLIC;
          REVOKE ALL ON FUNCTION
            okg.begin_attribute_integrity_transition(TEXT, TEXT, TEXT),
            okg.attribute_integrity_transition_matches(TEXT, TEXT, TEXT),
            okg.finish_attribute_integrity_transition(TEXT, TEXT, TEXT),
            okg.abort_attribute_integrity_transition(TEXT, TEXT, TEXT),
            okg.register_attribute_integrity_scope(TEXT, TEXT, TEXT),
            okg.mark_attribute_integrity_scope_dirty(TEXT, TEXT, TEXT, TEXT),
            okg.finalize_attribute_integrity_receipt(
              TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, BIGINT, TEXT,
              BIGINT, BIGINT, BIGINT, BIGINT, INTEGER, TEXT, TEXT,
              NUMERIC, NUMERIC, NUMERIC, NUMERIC, TEXT, BIGINT
            )
            FROM PUBLIC;
          -- The publisher runs as okg_substrate; app_rw keeps SELECT but
          -- no transition/finalizer authority.
          FOREACH role_name IN ARRAY ARRAY['app_rw', 'app_ro', 'okg_mcp'] LOOP
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
              EXECUTE format(
                'REVOKE ALL ON FUNCTION '
                'okg.begin_attribute_integrity_transition(TEXT, TEXT, TEXT), '
                'okg.attribute_integrity_transition_matches(TEXT, TEXT, TEXT), '
                'okg.finish_attribute_integrity_transition(TEXT, TEXT, TEXT), '
                'okg.abort_attribute_integrity_transition(TEXT, TEXT, TEXT), '
                'okg.register_attribute_integrity_scope(TEXT, TEXT, TEXT), '
                'okg.mark_attribute_integrity_scope_dirty'
                '(TEXT, TEXT, TEXT, TEXT), '
                'okg.finalize_attribute_integrity_receipt('
                'TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, BIGINT, TEXT, '
                'BIGINT, BIGINT, BIGINT, BIGINT, INTEGER, TEXT, TEXT, '
                'NUMERIC, NUMERIC, NUMERIC, NUMERIC, TEXT, BIGINT'
                ') FROM %I',
                role_name
              );
            END IF;
          END LOOP;
          FOREACH role_name IN ARRAY ARRAY['okg_substrate'] LOOP
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
              EXECUTE format(
                'GRANT EXECUTE ON FUNCTION '
                'okg.begin_attribute_integrity_transition(TEXT, TEXT, TEXT), '
                'okg.attribute_integrity_transition_matches(TEXT, TEXT, TEXT), '
                'okg.finish_attribute_integrity_transition(TEXT, TEXT, TEXT), '
                'okg.abort_attribute_integrity_transition(TEXT, TEXT, TEXT), '
                'okg.register_attribute_integrity_scope(TEXT, TEXT, TEXT), '
                'okg.mark_attribute_integrity_scope_dirty'
                '(TEXT, TEXT, TEXT, TEXT), '
                'okg.finalize_attribute_integrity_receipt('
                'TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, BIGINT, TEXT, '
                'BIGINT, BIGINT, BIGINT, BIGINT, INTEGER, TEXT, TEXT, '
                'NUMERIC, NUMERIC, NUMERIC, NUMERIC, TEXT, BIGINT'
                ') TO %I',
                role_name
              );
            END IF;
          END LOOP;
        END
        $privileges$
        """,
    )
    with conn.cursor() as cur:
        for statement in statements:
            cur.execute(statement)


def register_attribute_integrity_scope(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    branch: str,
    namespace: str,
) -> None:
    """Register before protected writes so their triggers cannot miss the scope."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT to_regclass(%s), to_regclass(%s)",
            (ATTRIBUTE_INTEGRITY_STATE_TABLE, ATTRIBUTE_INTEGRITY_RECEIPT_TABLE),
        )
        installed = all(value is not None for value in cur.fetchone())
    # Migrate/provision installs the complete schema.  This branch is only the
    # old-deployment self-heal; avoiding CREATE OR REPLACE/DO on every publish
    # keeps the steady-state transition a pair of indexed reads.  If a trigger
    # is later dropped, the schema digest mismatch refuses advancement instead
    # of silently repairing away the evidence before validation.
    if not installed:
        ensure_attribute_integrity_schema(conn)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT okg.register_attribute_integrity_scope(%s, %s, %s)",
            (deployment_name, branch, namespace),
        )


def mark_attribute_integrity_dirty(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    branch: str,
    namespace: str,
    reason: str,
) -> None:
    register_attribute_integrity_scope(
        conn,
        deployment_name=deployment_name,
        branch=branch,
        namespace=namespace,
    )
    with conn.cursor() as cur:
        cur.execute(
            "SELECT okg.mark_attribute_integrity_scope_dirty(%s, %s, %s, %s)",
            (deployment_name, branch, namespace, reason),
        )


_IDENTITY_RELATIONS = (
    "attribute_effective_view",
    "attribute_effective_view_intervals",
    "attribute_effective_integrity_state",
    "attribute_effective_integrity_receipts",
    "attribute_effective_integrity_transitions",
)
_IDENTITY_FUNCTIONS = (
    "mark_attribute_effective_integrity_dirty",
    "mark_attribute_effective_integrity_dirty_rows",
    "mark_attribute_effective_integrity_dirty_update",
    "reject_attribute_integrity_receipt_mutation",
    "begin_attribute_integrity_transition",
    "attribute_integrity_transition_matches",
    "finish_attribute_integrity_transition",
    "abort_attribute_integrity_transition",
    "register_attribute_integrity_scope",
    "mark_attribute_integrity_scope_dirty",
    "finalize_attribute_integrity_receipt",
)
_IDENTITY_ROLES = ("okg_substrate", "app_rw", "app_ro", "okg_mcp")
_IDENTITY_TABLE_PRIVILEGES = (
    "SELECT",
    "INSERT",
    "UPDATE",
    "DELETE",
    "TRUNCATE",
)


def _schema_identity(conn: psycopg.Connection) -> tuple[str, str]:
    """Constant-size DDL/migration identity; never reads protected relation rows.

    Every load-bearing part of the integrity machinery is included: the
    protected relations plus the state/receipt/transition tables (columns,
    constraints, indexes, owners, ACLs), the trigger set, the full bodies
    and SECURITY DEFINER posture of the transition/finalizer functions,
    the effective privileges of the runtime and application roles, and the
    complete semantic migration lineage.  Any drift here must force the
    fast path to fall back instead of blessing a receipt minted under a
    different authority surface.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT c.relname, a.attnum, a.attname,
                   format_type(a.atttypid, a.atttypmod), a.attnotnull,
                   COALESCE(pg_get_expr(d.adbin, d.adrelid), '')
              FROM pg_class c
              JOIN pg_namespace n ON n.oid = c.relnamespace
              JOIN pg_attribute a ON a.attrelid = c.oid
              LEFT JOIN pg_attrdef d
                ON d.adrelid = c.oid AND d.adnum = a.attnum
             WHERE n.nspname = 'okg'
               AND c.relname = ANY(%s::text[])
               AND a.attnum > 0 AND NOT a.attisdropped
             ORDER BY c.relname, a.attnum
            """,
            (list(_IDENTITY_RELATIONS),),
        )
        columns = [list(row) for row in cur.fetchall()]
        cur.execute(
            """
            SELECT c.relname, pg_get_userbyid(c.relowner),
                   COALESCE((
                     SELECT jsonb_agg(
                       jsonb_build_array(
                         pg_get_userbyid(acl.grantor),
                         CASE WHEN acl.grantee = 0 THEN 'PUBLIC'
                              ELSE pg_get_userbyid(acl.grantee) END,
                         acl.privilege_type,
                         acl.is_grantable
                       ) ORDER BY pg_get_userbyid(acl.grantor),
                                  CASE WHEN acl.grantee = 0 THEN 'PUBLIC'
                                       ELSE pg_get_userbyid(acl.grantee) END,
                                  acl.privilege_type, acl.is_grantable
                     )::text
                       FROM aclexplode(COALESCE(
                         c.relacl, acldefault('r'::"char", c.relowner)
                       )) AS acl
                   ), '[]')
              FROM pg_class c
              JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = 'okg'
               AND c.relname = ANY(%s::text[])
             ORDER BY c.relname
            """,
            (list(_IDENTITY_RELATIONS),),
        )
        ownership = [list(row) for row in cur.fetchall()]
        cur.execute(
            """
            SELECT c.relname, con.conname, con.contype,
                   pg_get_constraintdef(con.oid, TRUE)
              FROM pg_constraint con
              JOIN pg_class c ON c.oid = con.conrelid
              JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = 'okg'
               AND c.relname = ANY(%s::text[])
             ORDER BY c.relname, con.conname
            """,
            (list(_IDENTITY_RELATIONS),),
        )
        constraints = [list(row) for row in cur.fetchall()]
        cur.execute(
            """
            SELECT tab.relname, idx.relname, pg_get_indexdef(i.indexrelid)
              FROM pg_index i
              JOIN pg_class tab ON tab.oid = i.indrelid
              JOIN pg_class idx ON idx.oid = i.indexrelid
              JOIN pg_namespace n ON n.oid = tab.relnamespace
             WHERE n.nspname = 'okg'
               AND tab.relname = ANY(%s::text[])
             ORDER BY tab.relname, idx.relname
            """,
            (list(_IDENTITY_RELATIONS),),
        )
        indexes = [list(row) for row in cur.fetchall()]
        cur.execute(
            """
            SELECT c.relname, t.tgname, pg_get_triggerdef(t.oid),
                   t.tgenabled::text
              FROM pg_trigger t
              JOIN pg_class c ON c.oid = t.tgrelid
              JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = 'okg'
               AND c.relname = ANY(%s::text[])
               AND NOT t.tgisinternal
             ORDER BY c.relname, t.tgname
            """,
            (list(_IDENTITY_RELATIONS),),
        )
        # tgenabled is separate from pg_get_triggerdef: DISABLE TRIGGER
        # changes only this flag, and a disabled dirtying trigger is a
        # silently forged clean proof.
        triggers = [list(row) for row in cur.fetchall()]
        cur.execute(
            """
            SELECT p.proname, pg_get_function_identity_arguments(p.oid),
                   pg_get_functiondef(p.oid), pg_get_userbyid(p.proowner),
                   p.prosecdef, COALESCE(p.proconfig::text, ''),
                   COALESCE((
                     SELECT jsonb_agg(
                       jsonb_build_array(
                         pg_get_userbyid(acl.grantor),
                         CASE WHEN acl.grantee = 0 THEN 'PUBLIC'
                              ELSE pg_get_userbyid(acl.grantee) END,
                         acl.privilege_type,
                         acl.is_grantable
                       ) ORDER BY pg_get_userbyid(acl.grantor),
                                  CASE WHEN acl.grantee = 0 THEN 'PUBLIC'
                                       ELSE pg_get_userbyid(acl.grantee) END,
                                  acl.privilege_type, acl.is_grantable
                     )::text
                       FROM aclexplode(COALESCE(
                         p.proacl, acldefault('f'::"char", p.proowner)
                       )) AS acl
                   ), '[]')
              FROM pg_proc p
              JOIN pg_namespace n ON n.oid = p.pronamespace
             WHERE n.nspname = 'okg'
               AND p.proname = ANY(%s::text[])
             ORDER BY p.proname, pg_get_function_identity_arguments(p.oid)
            """,
            (list(_IDENTITY_FUNCTIONS),),
        )
        functions = [list(row) for row in cur.fetchall()]
        cur.execute(
            """
            SELECT r.rolname, c.relname, priv.name,
                   has_table_privilege(r.rolname, c.oid, priv.name)
              FROM pg_roles r
             CROSS JOIN unnest(%s::text[]) AS priv(name)
              JOIN pg_class c ON c.relname = ANY(%s::text[])
              JOIN pg_namespace n
                ON n.oid = c.relnamespace AND n.nspname = 'okg'
             WHERE r.rolname = ANY(%s::text[])
             ORDER BY r.rolname, c.relname, priv.name
            """,
            (
                list(_IDENTITY_TABLE_PRIVILEGES),
                list(_IDENTITY_RELATIONS),
                list(_IDENTITY_ROLES),
            ),
        )
        table_privileges = [list(row) for row in cur.fetchall()]
        cur.execute(
            """
            SELECT r.rolname, p.proname,
                   pg_get_function_identity_arguments(p.oid),
                   has_function_privilege(r.rolname, p.oid, 'EXECUTE')
              FROM pg_roles r
              JOIN pg_proc p ON p.proname = ANY(%s::text[])
              JOIN pg_namespace n
                ON n.oid = p.pronamespace AND n.nspname = 'okg'
             WHERE r.rolname = ANY(%s::text[])
             ORDER BY r.rolname, p.proname,
                      pg_get_function_identity_arguments(p.oid)
            """,
            (list(_IDENTITY_FUNCTIONS), list(_IDENTITY_ROLES)),
        )
        function_privileges = [list(row) for row in cur.fetchall()]
        cur.execute("SELECT to_regclass('okg.schema_migrations')")
        if cur.fetchone()[0] is None:
            migrations: list[list[Any]] = []
        else:
            cur.execute(
                """
                SELECT migration_id, checksum, status
                  FROM okg.schema_migrations
                 ORDER BY migration_id
                """
            )
            migrations = [list(row) for row in cur.fetchall()]
    return _sha256(
        {
            "identity_version": 2,
            "columns": columns,
            "ownership": ownership,
            "constraints": constraints,
            "indexes": indexes,
            "triggers": triggers,
            "functions": functions,
            "table_privileges": table_privileges,
            "function_privileges": function_privileges,
        }
    ), _sha256(migrations)


def _payload_expression(alias: str, seed: int) -> str:
    columns = ("target_kind", "target_id", "namespace", *_PAYLOAD_COLUMNS)
    payload = " || E'\\x1f' || ".join(
        f"COALESCE({alias}.{column}::text, E'\\x1e')" for column in columns
    )
    return f"COALESCE(sum(hashtextextended({payload}, {seed})::numeric), 0)"


def _payload_signature(
    conn: psycopg.Connection,
    *,
    source: Literal["snapshot", "interval"],
    generation_id: str,
    namespace: str,
    branch: str,
    ordinal: int,
    target_ids: Mapping[str, set[str]] | None,
) -> dict[str, Any]:
    table = (
        ATTRIBUTE_EFFECTIVE_VIEW_TABLE
        if source == "snapshot"
        else ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE
    )
    alias = "p"
    if source == "snapshot":
        predicate = "p.generation_id = %(generation)s AND p.namespace = %(namespace)s"
        params: dict[str, Any] = {
            "generation": generation_id,
            "namespace": namespace,
        }
    else:
        predicate = (
            "p.branch_id = %(branch)s AND p.namespace = %(namespace)s "
            "AND p.from_ord <= %(ordinal)s AND %(ordinal)s < p.to_ord"
        )
        params = {"branch": branch, "namespace": namespace, "ordinal": ordinal}
    scope = ""
    if target_ids is not None:
        scope = (
            " AND ((p.target_kind = 'node' AND p.target_id = ANY(%(nodes)s::text[]))"
            " OR (p.target_kind = 'edge' AND p.target_id = ANY(%(edges)s::text[])))"
        )
        params.update(
            nodes=sorted(target_ids.get("node", set())),
            edges=sorted(target_ids.get("edge", set())),
        )
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT p.target_kind, count(*)::bigint,
                   {_payload_expression(alias, _SEEDS[0])},
                   {_payload_expression(alias, _SEEDS[1])}
              FROM {table} AS p
             WHERE {predicate}{scope}
             GROUP BY p.target_kind
            """,
            params,
        )
        rows = cur.fetchall()
    result = {
        "counts": {"node": 0, "edge": 0},
        "seed0": 0,
        "seed1": 0,
    }
    for kind, count, seed0, seed1 in rows:
        result["counts"][str(kind)] = int(count)
        result["seed0"] += int(seed0)
        result["seed1"] += int(seed1)
    return result


def _interval_retention_cutoff(
    conn: psycopg.Connection, *, branch: str
) -> int | None:
    """The ordinal below which the retention prune MAY have deleted history.

    Mirrors ``prune_effective_interval_view_closed``'s window: a CLOSED interval
    is prunable iff ``to_ord <= keep_from_ord``, where ``keep_from_ord`` is the
    ``keep_generations``-th highest ordinal on the branch. A branch with fewer
    ordinals than that has no cutoff and is left intact (None).
    """
    from okg.substrate.attributes import EFFECTIVE_VIEW_RETENTION_GENERATIONS

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ordinal FROM (
                SELECT ordinal,
                       row_number() OVER (ORDER BY ordinal DESC) AS rnk
                  FROM okg.generation_ordinals
                 WHERE branch_id = %s
            ) ranked
             WHERE ranked.rnk = %s
            """,
            (branch, int(EFFECTIVE_VIEW_RETENTION_GENERATIONS)),
        )
        row = cur.fetchone()
    return None if row is None else int(row[0])


def _interval_stab_counts(
    conn: psycopg.Connection,
    *,
    branch: str,
    namespace: str,
    ordinal: int,
) -> dict[str, int]:
    """Row counts of the interval STAB at ``ordinal`` (no payload read).

    Used only to prove the old side of a delta is still INTACT: the retention
    prune deletes CLOSED rows outside its window, and pruning the stab the prior
    receipt describes leaves the delta subtracting an empty old side -- a count
    that disagrees with the checkpoint and fails an otherwise sound publish
    (okg#1603).
    """
    counts = {"node": 0, "edge": 0}
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT p.target_kind, count(*)::bigint
              FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} p
             WHERE p.branch_id = %(branch)s AND p.namespace = %(namespace)s
               AND p.target_kind IN ('node', 'edge')
               AND p.from_ord <= %(ordinal)s AND %(ordinal)s < p.to_ord
             GROUP BY p.target_kind
            """,
            {"branch": branch, "namespace": namespace, "ordinal": int(ordinal)},
        )
        for kind, count in cur.fetchall():
            counts[str(kind)] = int(count)
    return counts


def _load_state(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    branch: str,
    namespace: str,
    for_update: bool = False,
) -> dict[str, Any]:
    lock = " FOR UPDATE" if for_update else ""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            SELECT * FROM {ATTRIBUTE_INTEGRITY_STATE_TABLE}
             WHERE deployment_name = %s AND branch_id = %s AND namespace = %s
             {lock}
            """,
            (deployment_name, branch, namespace),
        )
        row = cur.fetchone()
    return dict(row or {})


def _load_receipt(conn: psycopg.Connection, digest: str | None) -> dict[str, Any]:
    if not digest:
        return {}
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"SELECT * FROM {ATTRIBUTE_INTEGRITY_RECEIPT_TABLE} WHERE receipt_digest = %s",
            (digest,),
        )
        row = cur.fetchone()
    return dict(row or {})


def latest_receipt_ordinal(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    branch: str,
    namespace: str = "default",
) -> int | None:
    """Ordinal of the latest attribute-integrity receipt, or None.

    The interval prune protects this ordinal: it is the old side of the next
    incremental delta, and deleting it costs the publish (okg#1603). Returns
    None when there is no receipt (nothing to protect) or when the integrity
    tables are absent (a deployment that never ran the receipt path).

    Namespace-scoped, while the prune is namespace-blind: the publish path uses
    the ``default`` namespace at every receipt call site, so a second namespace
    would need its own protection before its receipts could be trusted.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT to_regclass(%s) IS NOT NULL",
            (ATTRIBUTE_INTEGRITY_STATE_TABLE,),
        )
        row = cur.fetchone()
    if not row or not row[0]:
        return None
    state = _load_state(
        conn,
        deployment_name=deployment_name,
        branch=branch,
        namespace=namespace,
    )
    receipt = _load_receipt(conn, state.get("latest_receipt_digest"))
    if not receipt or receipt.get("ordinal") is None:
        return None
    return int(receipt["ordinal"])


def _receipt_body(row: Mapping[str, Any]) -> dict[str, Any]:
    keys = (
        "receipt_type",
        "deployment_name",
        "branch_id",
        "namespace",
        "generation_id",
        "ordinal",
        "prior_receipt_digest",
        "checkpoint_node_count",
        "checkpoint_edge_count",
        "logical_node_count",
        "logical_edge_count",
        "schema_version",
        "schema_digest",
        "migration_digest",
        "logical_payload_seed0",
        "logical_payload_seed1",
        "interval_payload_seed0",
        "interval_payload_seed1",
        "dirty_scope_digest",
        "mutation_epoch",
    )
    body = {key: row.get(key) for key in keys}
    for key in (
        "ordinal",
        "checkpoint_node_count",
        "checkpoint_edge_count",
        "logical_node_count",
        "logical_edge_count",
        "schema_version",
        "logical_payload_seed0",
        "logical_payload_seed1",
        "interval_payload_seed0",
        "interval_payload_seed1",
        "mutation_epoch",
    ):
        if body[key] is not None:
            body[key] = int(body[key])
    return body


def _receipt_is_well_formed(row: Mapping[str, Any]) -> bool:
    if not row:
        return False
    body = _receipt_body(row)
    return row.get("receipt_digest") == attribute_integrity_receipt_digest(body)


def _store_clean_receipt(
    conn: psycopg.Connection,
    *,
    body: dict[str, Any],
) -> dict[str, Any]:
    """Finalize through the restricted SQL API; never by direct DML.

    The SECURITY DEFINER finalizer is the only clean-marking authority: it
    consumes the caller's backend-bound transition token, appends the
    receipt, and compare-and-sets the state row on the exact mutation
    epoch.  Direct INSERT/UPDATE on the state and receipt tables is revoked
    from the runtime and application roles, so a compromised or buggy
    writer cannot bless its own corruption as clean.
    """
    digest = attribute_integrity_receipt_digest(body)
    row = {**body, "receipt_digest": digest}
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT okg.finalize_attribute_integrity_receipt("
                "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                "%s, %s, %s, %s, %s, %s, %s)",
                (
                    digest,
                    body["receipt_type"],
                    body["deployment_name"],
                    body["branch_id"],
                    body["namespace"],
                    body["generation_id"],
                    body["ordinal"],
                    body["prior_receipt_digest"],
                    body["checkpoint_node_count"],
                    body["checkpoint_edge_count"],
                    body["logical_node_count"],
                    body["logical_edge_count"],
                    body["schema_version"],
                    body["schema_digest"],
                    body["migration_digest"],
                    body["logical_payload_seed0"],
                    body["logical_payload_seed1"],
                    body["interval_payload_seed0"],
                    body["interval_payload_seed1"],
                    body["dirty_scope_digest"],
                    body["mutation_epoch"],
                ),
            )
            finalized = cur.fetchone()
    except psycopg.errors.RaiseException as exc:
        raise SubstrateError(
            getattr(exc.diag, "message_primary", None)
            or "attribute integrity transition changed during finalization"
        ) from exc
    if not finalized or finalized[0] is not True:
        raise SubstrateError(
            "attribute integrity transition changed during finalization"
        )
    return row


def check_attribute_integrity_fast_path(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    branch: str,
    namespace: str,
    generation_id: str,
    ordinal: int,
    checkpoint_counts: Mapping[str, int],
    register_scope: bool = True,
) -> dict[str, Any]:
    """Validate the latest receipt with constant-size catalog/index reads only."""
    if register_scope:
        register_attribute_integrity_scope(
            conn,
            deployment_name=deployment_name,
            branch=branch,
            namespace=namespace,
        )
    state = _load_state(
        conn,
        deployment_name=deployment_name,
        branch=branch,
        namespace=namespace,
    )
    if state.get("dirty") is not False:
        return {"ok": False, "reason": "receipt_dirty"}
    receipt = _load_receipt(conn, state.get("latest_receipt_digest"))
    if not _receipt_is_well_formed(receipt):
        return {"ok": False, "reason": "receipt_missing_or_malformed"}
    schema_digest, migration_digest = _schema_identity(conn)
    expected = {
        "deployment_name": deployment_name,
        "branch_id": branch,
        "namespace": namespace,
        "generation_id": generation_id,
        "ordinal": int(ordinal),
        "checkpoint_node_count": int(checkpoint_counts["node"]),
        "checkpoint_edge_count": int(checkpoint_counts["edge"]),
        "logical_node_count": int(checkpoint_counts["node"]),
        "logical_edge_count": int(checkpoint_counts["edge"]),
        "schema_version": ATTRIBUTE_INTEGRITY_SCHEMA_VERSION,
        "schema_digest": schema_digest,
        "migration_digest": migration_digest,
        "mutation_epoch": int(state["mutation_epoch"]),
    }
    mismatches = {
        key: {"expected": value, "actual": receipt.get(key)}
        for key, value in expected.items()
        if str(receipt.get(key)) != str(value)
    }
    if int(state.get("clean_mutation_epoch") or -1) != int(state["mutation_epoch"]):
        mismatches["clean_mutation_epoch"] = {
            "expected": state["mutation_epoch"],
            "actual": state.get("clean_mutation_epoch"),
        }
    prior_digest = receipt.get("prior_receipt_digest")
    if prior_digest:
        prior = _load_receipt(conn, str(prior_digest))
        if not _receipt_is_well_formed(prior):
            mismatches["prior_receipt"] = {"expected": prior_digest, "actual": None}
        elif int(prior["ordinal"]) != int(receipt["ordinal"]) - 1:
            mismatches["prior_ordinal"] = {
                "expected": int(receipt["ordinal"]) - 1,
                "actual": int(prior["ordinal"]),
            }
        else:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT previous_checkpoint_id FROM okg.generation_ordinals "
                    "WHERE checkpoint_id = %s AND branch_id = %s AND ordinal = %s",
                    (generation_id, branch, ordinal),
                )
                generation_link = cur.fetchone()
            if generation_link is None or str(generation_link[0]) != str(
                prior["generation_id"]
            ):
                mismatches["generation_link"] = {
                    "expected": prior["generation_id"],
                    "actual": generation_link[0] if generation_link else None,
                }
    if mismatches:
        return {"ok": False, "reason": "receipt_mismatch", "mismatches": mismatches}
    return {
        "ok": True,
        "reason": "already_repaired",
        "receipt_digest": receipt["receipt_digest"],
        "generation_id": generation_id,
        "ordinal": ordinal,
        "mutation_epoch": int(state["mutation_epoch"]),
    }


def prepare_attribute_integrity_transition(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    branch: str,
    namespace: str,
) -> dict[str, Any]:
    """Capture a clean pre-write epoch under the publisher lock.

    A dirty state here means a protected write occurred outside the publisher's
    current transition.  The subsequent cycle must not absorb that corruption
    into its own changed scope and relabel the cache clean.
    """
    register_attribute_integrity_scope(
        conn,
        deployment_name=deployment_name,
        branch=branch,
        namespace=namespace,
    )
    with conn.transaction():
        state = _load_state(
            conn,
            deployment_name=deployment_name,
            branch=branch,
            namespace=namespace,
            for_update=True,
        )
        receipt = _load_receipt(conn, state.get("latest_receipt_digest"))
        if state.get("dirty") is not False or not _receipt_is_well_formed(receipt):
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT okg.abort_attribute_integrity_transition(%s, %s, %s)",
                    (deployment_name, branch, namespace),
                )
            return {"eligible": False, "reason": "prior_state_not_clean"}
        epoch = int(state["mutation_epoch"])
        if (
            int(state.get("clean_mutation_epoch") or -1) != epoch
            or int(receipt.get("mutation_epoch", -2)) != epoch
        ):
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT okg.abort_attribute_integrity_transition(%s, %s, %s)",
                    (deployment_name, branch, namespace),
                )
            return {"eligible": False, "reason": "prior_epoch_mismatch"}
        with conn.cursor() as cur:
            cur.execute(
                "SELECT okg.begin_attribute_integrity_transition(%s, %s, %s)",
                (deployment_name, branch, namespace),
            )
            begun = cur.fetchone()
            if not begun or begun[0] is not True:
                return {"eligible": False, "reason": "transition_cas_failed"}
    return {
        "eligible": True,
        "mutation_epoch": epoch,
        "receipt_digest": receipt["receipt_digest"],
        "transition_backend_bound": True,
    }


def clear_attribute_integrity_transition(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    branch: str,
    namespace: str,
    transition_active: bool,
) -> None:
    """Invalidate an unfinished publisher transition without clearing dirty state."""
    with conn.transaction(), conn.cursor() as cur:
        if transition_active:
            cur.execute(
                "SELECT okg.finish_attribute_integrity_transition(%s, %s, %s)",
                (deployment_name, branch, namespace),
            )


def finalize_full_attribute_integrity_anchor(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    branch: str,
    namespace: str,
    generation_id: str,
    ordinal: int,
    checkpoint_counts: Mapping[str, int],
    persist: bool = True,
) -> dict[str, Any]:
    """Validate or write a chain anchor after exact full snapshot/front parity.

    ``persist=False`` is the publisher's pre-checkpoint proof: it performs the
    same full payload and cardinality validation without marking the read model
    clean.  The post-checkpoint ``persist=True`` pass repeats the proof under a
    backend-bound transition and is the only path that stores the anchor.
    """
    state = _load_state(
        conn,
        deployment_name=deployment_name,
        branch=branch,
        namespace=namespace,
        for_update=persist,
    )
    if not state:
        raise SubstrateError("attribute integrity scope is unavailable")
    # A full repair owns the publisher lock and is the recovery authority for
    # any abandoned incremental token: begin replaces whatever token exists
    # with this backend's own, so the finalizer below can demand a live
    # backend-bound transition on the anchor path too. Holding the state row
    # lock before both full signatures prevents a concurrent protected-table
    # writer from committing until this anchor commits; its trigger then
    # dirties the anchor.
    if persist:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT okg.begin_attribute_integrity_transition(%s, %s, %s)",
                (deployment_name, branch, namespace),
            )
            begun = cur.fetchone()
            if not begun or begun[0] is not True:
                raise SubstrateError(
                    "attribute integrity anchor could not bind a transition"
                )
    snapshot = _payload_signature(
        conn,
        source="snapshot",
        generation_id=generation_id,
        namespace=namespace,
        branch=branch,
        ordinal=ordinal,
        target_ids=None,
    )
    interval = _payload_signature(
        conn,
        source="interval",
        generation_id=generation_id,
        namespace=namespace,
        branch=branch,
        ordinal=ordinal,
        target_ids=None,
    )
    expected_counts = {
        "node": int(checkpoint_counts["node"]),
        "edge": int(checkpoint_counts["edge"]),
    }
    if snapshot != interval or snapshot["counts"] != expected_counts:
        raise SubstrateError(
            "attribute integrity anchor refused non-identical full payloads",
            data={
                "checkpoint_counts": expected_counts,
                "snapshot": snapshot,
                "interval": interval,
            },
        )
    schema_digest, migration_digest = _schema_identity(conn)
    body = {
        "receipt_type": "anchor",
        "deployment_name": deployment_name,
        "branch_id": branch,
        "namespace": namespace,
        "generation_id": generation_id,
        "ordinal": int(ordinal),
        "prior_receipt_digest": None,
        "checkpoint_node_count": expected_counts["node"],
        "checkpoint_edge_count": expected_counts["edge"],
        "logical_node_count": expected_counts["node"],
        "logical_edge_count": expected_counts["edge"],
        "schema_version": ATTRIBUTE_INTEGRITY_SCHEMA_VERSION,
        "schema_digest": schema_digest,
        "migration_digest": migration_digest,
        "logical_payload_seed0": snapshot["seed0"],
        "logical_payload_seed1": snapshot["seed1"],
        "interval_payload_seed0": interval["seed0"],
        "interval_payload_seed1": interval["seed1"],
        "dirty_scope_digest": _sha256({"mode": "full", "generation": generation_id}),
        "mutation_epoch": int(state["mutation_epoch"]),
    }
    if not persist:
        return {
            "advanced": True,
            "prepared": True,
            "receipt_type": "anchor",
            "dirty_scope_digest": body["dirty_scope_digest"],
            "mutation_epoch": body["mutation_epoch"],
        }
    return {
        "advanced": True,
        **_store_clean_receipt(conn, body=body),
    }


def finalize_incremental_attribute_integrity_receipt(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    branch: str,
    namespace: str,
    generation_id: str,
    previous_generation_id: str | None,
    ordinal: int,
    checkpoint_counts: Mapping[str, int],
    dirty_node_ids: set[str],
    dirty_edge_ids: set[str],
    expected_prior_mutation_epoch: int | None,
    expected_transition_active: bool,
    protected_from_ord: int | None = None,
    persist: bool = True,
) -> dict[str, Any]:
    """Advance a clean anchor from O(changed) old/new payload signatures.

    ``protected_from_ord`` is the ordinal the caller told the retention prune to
    KEEP (see ``prune_effective_interval_view_closed``). When it covers the prior
    receipt, the old side of the delta is known-intact and no intactness probe
    runs -- that probe is a live-set COUNT, and on a one-generation retention it
    would otherwise run on EVERY incremental publish.
    """
    state = _load_state(
        conn,
        deployment_name=deployment_name,
        branch=branch,
        namespace=namespace,
        for_update=persist,
    )
    if expected_prior_mutation_epoch is None or not expected_transition_active:
        return {"advanced": False, "reason": "prior_state_not_clean"}
    with conn.cursor() as cur:
        cur.execute(
            "SELECT okg.attribute_integrity_transition_matches(%s, %s, %s)",
            (deployment_name, branch, namespace),
        )
        transition_row = cur.fetchone()
    if not transition_row or transition_row[0] is not True:
        return {"advanced": False, "reason": "transition_invalidated"}
    prior = _load_receipt(conn, state.get("latest_receipt_digest"))
    if not _receipt_is_well_formed(prior):
        return {"advanced": False, "reason": "no_clean_prior_receipt"}
    if previous_generation_id is None:
        return {"advanced": False, "reason": "no_previous_generation"}
    # The prior receipt's OWN ordinal is the old side of the delta -- never
    # `ordinal - 1` computed from the incoming ordinal. Before okg#1587 a
    # publish that failed after `assign_generation_ordinal` committed consumed
    # an ordinal, and deployments that ran that code still carry the gap, so
    # the two can disagree (okg#1603); the adjacency guard below is what keeps
    # them equal, so read the position from the receipt that defines it.
    prior_ordinal = int(prior.get("ordinal", -2))
    if (
        str(prior.get("generation_id")) != str(previous_generation_id)
        or prior_ordinal != int(ordinal) - 1
        or int(prior.get("mutation_epoch", -1))
        != int(expected_prior_mutation_epoch)
        or int(state.get("clean_mutation_epoch") or -2)
        != int(expected_prior_mutation_epoch)
    ):
        return {"advanced": False, "reason": "prior_receipt_gap_or_dirty"}
    schema_digest, migration_digest = _schema_identity(conn)
    if (
        int(prior.get("schema_version", -1)) != ATTRIBUTE_INTEGRITY_SCHEMA_VERSION
        or prior.get("schema_digest") != schema_digest
        or prior.get("migration_digest") != migration_digest
    ):
        return {"advanced": False, "reason": "schema_or_migration_changed"}

    with conn.cursor() as cur:
        cur.execute(
            f"SELECT target_kind, target_id FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} "
            "WHERE generation_id = %s AND namespace = %s",
            (generation_id, namespace),
        )
        materialized_ids = {"node": set(), "edge": set()}
        for kind, target_id in cur.fetchall():
            materialized_ids[str(kind)].add(str(target_id))
    scope = {
        "node": set(dirty_node_ids) | materialized_ids["node"],
        "edge": set(dirty_edge_ids) | materialized_ids["edge"],
    }
    # Is the old side of the delta still intact? Skip the question when the
    # publish path protected the prior ordinal from the prune, and otherwise ask
    # only when the retention window COULD have deleted it: on the default
    # window an adjacent prior sits inside the window and this costs nothing.
    protection_covers_prior = (
        protected_from_ord is not None and int(protected_from_ord) <= prior_ordinal
    )
    retention_cutoff = (
        None if protection_covers_prior else _interval_retention_cutoff(
            conn, branch=branch
        )
    )
    if retention_cutoff is not None and prior_ordinal < retention_cutoff:
        prior_counts = {
            kind: int(prior[f"logical_{kind}_count"]) for kind in ("node", "edge")
        }
        stab_counts = _interval_stab_counts(
            conn, branch=branch, namespace=namespace, ordinal=prior_ordinal
        )
        if stab_counts != prior_counts:
            # The stab the prior receipt describes was pruned out of the
            # retention window. Refuse the fast path -- the publisher
            # re-anchors -- rather than subtract a truncated old side and fail
            # the publish on a count that was never wrong (okg#1603).
            return {
                "advanced": False,
                "reason": "prior_interval_history_pruned",
                "prior_ordinal": prior_ordinal,
                "ordinal": int(ordinal),
                "retention_cutoff": retention_cutoff,
                "prior_receipt_counts": prior_counts,
                "interval_stab_counts": stab_counts,
            }
    old_interval = _payload_signature(
        conn,
        source="interval",
        generation_id=str(prior["generation_id"]),
        namespace=namespace,
        branch=branch,
        ordinal=prior_ordinal,
        target_ids=scope,
    )
    new_snapshot = _payload_signature(
        conn,
        source="snapshot",
        generation_id=generation_id,
        namespace=namespace,
        branch=branch,
        ordinal=ordinal,
        target_ids=scope,
    )
    new_interval = _payload_signature(
        conn,
        source="interval",
        generation_id=generation_id,
        namespace=namespace,
        branch=branch,
        ordinal=ordinal,
        target_ids=scope,
    )
    if new_snapshot != new_interval:
        raise SubstrateError(
            "incremental attribute receipt refused snapshot/interval scope mismatch",
            data={"snapshot": new_snapshot, "interval": new_interval},
        )
    counts = {
        kind: int(prior[f"logical_{kind}_count"])
        - int(old_interval["counts"][kind])
        + int(new_snapshot["counts"][kind])
        for kind in ("node", "edge")
    }
    expected_counts = {
        "node": int(checkpoint_counts["node"]),
        "edge": int(checkpoint_counts["edge"]),
    }
    if counts != expected_counts:
        raise SubstrateError(
            "incremental attribute receipt count delta disagrees with checkpoint",
            data={"computed": counts, "checkpoint": expected_counts},
        )
    logical_seed0 = (
        int(prior["logical_payload_seed0"])
        - int(old_interval["seed0"])
        + int(new_snapshot["seed0"])
    )
    logical_seed1 = (
        int(prior["logical_payload_seed1"])
        - int(old_interval["seed1"])
        + int(new_snapshot["seed1"])
    )
    interval_seed0 = (
        int(prior["interval_payload_seed0"])
        - int(old_interval["seed0"])
        + int(new_interval["seed0"])
    )
    interval_seed1 = (
        int(prior["interval_payload_seed1"])
        - int(old_interval["seed1"])
        + int(new_interval["seed1"])
    )
    if (logical_seed0, logical_seed1) != (interval_seed0, interval_seed1):
        raise SubstrateError("incremental attribute accumulators diverged")
    body = {
        "receipt_type": "incremental",
        "deployment_name": deployment_name,
        "branch_id": branch,
        "namespace": namespace,
        "generation_id": generation_id,
        "ordinal": int(ordinal),
        "prior_receipt_digest": prior["receipt_digest"],
        "checkpoint_node_count": expected_counts["node"],
        "checkpoint_edge_count": expected_counts["edge"],
        "logical_node_count": counts["node"],
        "logical_edge_count": counts["edge"],
        "schema_version": ATTRIBUTE_INTEGRITY_SCHEMA_VERSION,
        "schema_digest": schema_digest,
        "migration_digest": migration_digest,
        "logical_payload_seed0": logical_seed0,
        "logical_payload_seed1": logical_seed1,
        "interval_payload_seed0": interval_seed0,
        "interval_payload_seed1": interval_seed1,
        "dirty_scope_digest": _sha256(
            {kind: sorted(scope[kind]) for kind in ("node", "edge")}
        ),
        "mutation_epoch": int(state["mutation_epoch"]),
    }
    if not persist:
        return {
            "advanced": True,
            "prepared": True,
            "prior_receipt_digest": body["prior_receipt_digest"],
            "dirty_scope_digest": body["dirty_scope_digest"],
            "mutation_epoch": body["mutation_epoch"],
        }
    return {
        "advanced": True,
        **_store_clean_receipt(conn, body=body),
    }
