SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- okg#3112 step 4b (PACT lane-h-3112-step4b; step-4 design revision 2,
-- section "4b: deletion plus migration"; decision 304, D4). Drop the
-- declared-attribute filter index okg.attr_index, its coverage markers, its
-- rebuild queue and derive front, and the guarded cast only it used.
--
-- Nothing reads or writes these objects any more: step 4a took attr_index out
-- of every attribute-filter read (a filter narrows through a ready declared
-- expression index, or the exact predicate answers alone), and step 4b removed
-- the publisher's gate, derivation and marker advance, the rebuild verb, the
-- post-cycle drain, the catalog-apply queue, the reclaim probes and the
-- okg_mcp grant-registry rows. The runtime schema ensure no longer creates
-- them, so 0021 and 0036 still create them on a fresh install and this file
-- drops them: fresh and upgraded databases converge on their absence.
--
-- Why this is not flagged `destructive` (decision 304, D4; same argument as
-- 0023-0025): a destructive migration is auto-SKIPPED behind an
-- operator-confirmed adapter, which would leave every upgraded database
-- carrying tables a fresh install lacks and keep the migration-convergence
-- lane red. The rows are derived data: every one was projected from the
-- interval rows in okg.attribute_effective_view_intervals, which stay. The
-- drops are RESTRICT (the default), so a dependency nobody audited blocks the
-- migration instead of being removed by CASCADE. It still frees real space
-- (about 11.7 GB on krama), so the operator approves it knowing that.
--
-- Live order: on step-3 or 4a code run `okg db attr-index reconcile --apply`
-- and check `okg doctor --check declared-attr-indexes` is OK; stop the worker
-- and the MCP servers (a step-3 reader holds locks on okg.attr_index and would
-- hit the 5 s lock timeout); deploy 4b; `okg migrate --apply`; start them.
--
-- Rollback is a forward fix: old code's schema ensure would silently recreate
-- empty tables, and the ledger's record of this migration refuses old code
-- (schema_migration_unknown_applied). Recovery is a new migration plus a full
-- rebuild.

-- 1. A census without a scan. A count(*) over a multi-GB table could exceed
--    the 60 s statement timeout cold, so each table reports the planner's row
--    estimate, its total size on disk and whether it holds any row at all.
DO $attr_index_teardown_census$
DECLARE
  rel text;
  estimate bigint;
  total_bytes bigint;
  has_rows boolean;
BEGIN
  FOREACH rel IN ARRAY ARRAY[
    'okg.attr_index_coverage',
    'okg.attr_index_rebuild_requests',
    'okg.attr_index_derive_front',
    'okg.attr_index'
  ] LOOP
    IF to_regclass(rel) IS NULL THEN
      RAISE WARNING 'okg#3112 step 4b: % is absent; nothing to drop', rel;
      CONTINUE;
    END IF;
    SELECT c.reltuples::bigint, pg_total_relation_size(c.oid)
      INTO estimate, total_bytes
      FROM pg_class c
     WHERE c.oid = to_regclass(rel);
    EXECUTE format('SELECT EXISTS (SELECT 1 FROM %s)', rel) INTO has_rows;
    RAISE WARNING
      'okg#3112 step 4b drops %: reltuples estimate %, total bytes %, holds rows %',
      rel, estimate, total_bytes, has_rows::text;
  END LOOP;
END
$attr_index_teardown_census$;

-- 2. The drops. The three issue-#3029 side tables first, then the index, then
--    the guarded cast only okg.attr_index's derivation called.
DROP TABLE IF EXISTS okg.attr_index_coverage RESTRICT;
DROP TABLE IF EXISTS okg.attr_index_rebuild_requests RESTRICT;
DROP TABLE IF EXISTS okg.attr_index_derive_front RESTRICT;
DROP TABLE IF EXISTS okg.attr_index RESTRICT;
DROP FUNCTION IF EXISTS okg.attr_index_try_ts(text) RESTRICT;
