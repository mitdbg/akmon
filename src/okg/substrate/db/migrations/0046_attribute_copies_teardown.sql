SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- okg#3112 storage redesign S3+S4 (PACT lane-s-storage-drop-copies). Drop the
-- three attribute copies -- okg.attribute_effective_view (the per-generation
-- snapshot), okg.attribute_effective_view_intervals (the interval front) and
-- okg.attribute_versions (their content-addressed payload store) -- and the
-- integrity receipts that guarded them: the three receipt tables of
-- 0013-0015, their functions, and the dirty-marking triggers on the copies.
-- It also drops the storage of two retired checks that read the copies:
-- okg.effective_attribute_recovery_attempts (the recovery journal of
-- 0016-0017) and the declared-index drift record (okg.declared_attr_index_drift_checks
-- and the drift columns of 0037).
--
-- Nothing reads or writes these objects any more. The okg#3112 graph-path
-- change (PR A) moved every reader to the graph rows plus the overlay validity ranges
-- (okg.attribute_overlay_ranges, migration 0044). This change removed the
-- publisher's snapshot materialize, interval sync, reconcile, retention prune
-- and receipts, the pre-lock hash pass, the interval precondition, the
-- recovery classification, the declared-index drift check, the snapshot
-- backfill at every migrate and the repair verbs. The runtime schema ensure no
-- longer creates the tables, so 0013-0015 and 0020 still create some of them
-- on a fresh install and this file drops them: fresh and upgraded databases
-- converge on their absence. okg.attribute_effective_attrs (the per-generation
-- overlay rows Nomos and the graph read path read), the overlay marker and the
-- ranges stay.
--
-- Why this is not flagged `destructive` (decision 304, D4; same argument as
-- 0023-0025 and 0043): a destructive migration is auto-SKIPPED behind an
-- operator-confirmed adapter, which would leave every upgraded database
-- carrying tables a fresh install lacks and keep the migration-convergence
-- lane red. The rows are derived data: every one was computed from the graph
-- rows and okg.attribute_effective_attrs, which stay. The drops are RESTRICT
-- (the default), so a dependency nobody audited blocks the migration instead
-- of being removed by CASCADE. It frees real space (17.9 GB measured on the
-- okg-workspace laptop copy), so the operator approves it knowing that.
--
-- Live order: stop the worker and the MCP servers (an old reader holds locks
-- on these tables and would hit the 5 s lock timeout); deploy this code;
-- `okg migrate --apply`; start them.
--
-- Rollback is a forward fix: the ledger's record of this migration refuses
-- old code (schema_migration_unknown_applied), and old code would also refuse
-- its first publish on the missing interval rows. Recovery is a new migration
-- that recreates the tables plus a full rebuild of the copies.

-- 1. A census without a scan. A count(*) over a multi-GB table could exceed
--    the 60 s statement timeout cold, so each table reports the planner's row
--    estimate, its total size on disk and whether it holds any row at all.
DO $attribute_copies_teardown_census$
DECLARE
  rel text;
  estimate bigint;
  total_bytes bigint;
  has_rows boolean;
BEGIN
  FOREACH rel IN ARRAY ARRAY[
    'okg.attribute_effective_view',
    'okg.attribute_effective_view_intervals',
    'okg.attribute_versions',
    'okg.attribute_effective_integrity_receipts',
    'okg.attribute_effective_integrity_transitions',
    'okg.attribute_effective_integrity_state',
    'okg.effective_attribute_recovery_attempts',
    'okg.declared_attr_index_drift_checks'
  ] LOOP
    IF to_regclass(rel) IS NULL THEN
      RAISE WARNING 'attribute copies teardown: % is absent; nothing to drop', rel;
      CONTINUE;
    END IF;
    SELECT c.reltuples::bigint, pg_total_relation_size(c.oid)
      INTO estimate, total_bytes
      FROM pg_class c
     WHERE c.oid = to_regclass(rel);
    EXECUTE format('SELECT EXISTS (SELECT 1 FROM %s)', rel) INTO has_rows;
    RAISE WARNING
      'attribute copies teardown drops %: reltuples estimate %, total bytes %, holds rows %',
      rel, estimate, total_bytes, has_rows::text;
  END LOOP;
END
$attribute_copies_teardown_census$;

-- 2. The copies, in foreign-key order: the snapshot and the interval front
--    both reference okg.attribute_versions, so they go first. Their
--    dirty-marking triggers go with them.
DROP TABLE IF EXISTS okg.attribute_effective_view RESTRICT;
DROP TABLE IF EXISTS okg.attribute_effective_view_intervals RESTRICT;
DROP TABLE IF EXISTS okg.attribute_versions RESTRICT;

-- 3. The integrity receipts (0013-0015): the tables (the receipts table's
--    immutability trigger goes with it), then their functions, by the exact
--    signatures 0013-0015 and the old runtime ensure created, now that no
--    trigger references them.
DROP TABLE IF EXISTS okg.attribute_effective_integrity_receipts RESTRICT;
DROP TABLE IF EXISTS okg.attribute_effective_integrity_transitions RESTRICT;
DROP TABLE IF EXISTS okg.attribute_effective_integrity_state RESTRICT;

DROP FUNCTION IF EXISTS okg.begin_attribute_integrity_transition(TEXT, TEXT, TEXT) RESTRICT;
DROP FUNCTION IF EXISTS okg.attribute_integrity_transition_matches(TEXT, TEXT, TEXT) RESTRICT;
DROP FUNCTION IF EXISTS okg.finish_attribute_integrity_transition(TEXT, TEXT, TEXT) RESTRICT;
DROP FUNCTION IF EXISTS okg.abort_attribute_integrity_transition(TEXT, TEXT, TEXT) RESTRICT;
DROP FUNCTION IF EXISTS okg.register_attribute_integrity_scope(TEXT, TEXT, TEXT) RESTRICT;
DROP FUNCTION IF EXISTS okg.mark_attribute_integrity_scope_dirty(TEXT, TEXT, TEXT, TEXT) RESTRICT;
DROP FUNCTION IF EXISTS okg.finalize_attribute_integrity_receipt(
  TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, BIGINT, TEXT,
  BIGINT, BIGINT, BIGINT, BIGINT, INTEGER, TEXT, TEXT,
  NUMERIC, NUMERIC, NUMERIC, NUMERIC, TEXT, BIGINT
) RESTRICT;
DROP FUNCTION IF EXISTS okg.mark_attribute_effective_integrity_dirty() RESTRICT;
DROP FUNCTION IF EXISTS okg.mark_attribute_effective_integrity_dirty_rows() RESTRICT;
DROP FUNCTION IF EXISTS okg.mark_attribute_effective_integrity_dirty_update() RESTRICT;
DROP FUNCTION IF EXISTS okg.reject_attribute_integrity_receipt_mutation() RESTRICT;

-- 4. The storage of two retired checks. The corrupt-predecessor recovery
--    journal (0016-0017) bound a recovery of the copies to its predecessor;
--    the declared-index drift check compared each ready expression index
--    with the copies. Nothing writes or reads either any more.
DROP TABLE IF EXISTS okg.effective_attribute_recovery_attempts RESTRICT;
DROP TABLE IF EXISTS okg.declared_attr_index_drift_checks RESTRICT;
ALTER TABLE IF EXISTS okg.declared_attr_indexes
  DROP COLUMN IF EXISTS drift RESTRICT,
  DROP COLUMN IF EXISTS drift_detected_at RESTRICT,
  DROP COLUMN IF EXISTS drift_detail RESTRICT;
ALTER TABLE IF EXISTS okg.declared_attr_index_state
  DROP COLUMN IF EXISTS drift_checked_through_ordinal RESTRICT;
