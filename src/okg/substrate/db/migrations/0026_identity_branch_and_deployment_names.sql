-- schema-identity-convergence / req.branch-naming + req.deployment-scoping
-- (docs/architecture/db-schema-v1-design.md §2.1 branch naming, §4 deployment
-- scoping). PR #1066 shipped these two requirements as a FREEZE GATE only --
-- every divergence registered, none renamed. This migration performs the
-- renames the gate was holding the line for, and converges an upgraded
-- database onto the shape schema.sql now authors directly.
--
-- Every statement is guarded on the OLD column still being present, so a
-- fresh install -- where schema.sql already authored the new name -- runs
-- this migration as a no-op rather than failing on a column that is already
-- gone. RENAME COLUMN is not idempotent on its own.
--
-- SCOPE IS MEASURED, NOT ASSUMED. Two chronos branch columns and ten bare
-- `deployment` columns rename here. Some do NOT, and every exclusion is
-- mechanical rather than editorial -- see the block comments at each site and
-- the rows that stay in src/okg/substrate/db/identity_divergences.yaml, each
-- re-owned to the lane that can actually converge it.
--
-- The governing constraint is the ordered-migration runner: EVERY ordered
-- migration re-executes on a fresh install, in ordinal order, AFTER schema.sql
-- has run (src/okg/substrate/db/migrate.py:1059 then :1092; okg.schema_migrations
-- on a fresh database lists 0002..0023 all `applied`). An applied ordinal can
-- never be edited (management.py:994 refuses a changed checksum), so any column
-- an EARLIER ordinal names must still resolve when that ordinal replays against
-- the NEW shape. Measured against Postgres 17.10 rather than inferred:
--
--   * `LANGUAGE plpgsql` bodies are NOT name-resolved at CREATE time, so an
--     earlier ordinal re-issuing a stale body is harmless -- this migration
--     re-issues the correct body afterwards and is the last writer.
--   * `LANGUAGE sql` bodies ARE name-resolved at CREATE time, so an earlier
--     ordinal naming a renamed column aborts the migrate outright.

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

-- ===================================================================
-- 1. Chronos TEXT branch identifiers -> chronos_branch_id (§2.1)
-- ===================================================================
--
-- okg.branches.branch_id is BIGINT. A TEXT column of the same name in the same
-- schema is the name collision the requirement closes: `branch_id` now means
-- "BIGINT referencing okg.branches" everywhere, and a chronos checkpoint branch
-- is `chronos_branch_id`.
--
-- The general rule, established by measurement: a column can be renamed here
-- only if NO applied ordinal names it in an index column list or in a
-- LANGUAGE sql function body. Both of those are resolved at execution time
-- against the CURRENT shape, and every ordinal replays on a fresh install.
--
-- RENAMED HERE: okg.history_compaction_plans.branch_id. Ordinal 0006 declares
-- this table inside CREATE TABLE IF NOT EXISTS, which is a genuine no-op once
-- the table exists, and no ordinal indexes or selects its branch column.
--
-- NOT RENAMED, each blocked by a DIFFERENT applied ordinal:
--
--   * okg.history_generation_pins.branch_id --
--     0006_chronos_history_compaction:31-32 re-creates
--     history_generation_pins_checkpoint_idx naming `branch_id`. Measured on
--     PG 17.10: CREATE INDEX IF NOT EXISTS resolves its column list BEFORE it
--     checks whether the index name already exists, so an existing index does
--     NOT make the statement a no-op. Renaming the column aborts a fresh
--     install at ordinal 0006.
--   * okg.compacted_generations.branch_id -- ordinals 0009, 0010 and 0011 each
--     re-create okg.compacted_generation_record as LANGUAGE sql with
--     `'branch', branch_id` in the body, and a LANGUAGE sql body IS
--     name-resolved at CREATE time (the plpgsql equivalent is not). Renaming
--     the column aborts a fresh install at ordinal 0009.
--
-- Neither is repairable from here: this migration runs after all of them, and
-- an applied ordinal can never be edited. Both stay registered-divergent,
-- re-owned to the v1.0 re-baseline that retires the ordinal chain -- the same
-- durable-ordinal argument the registry already carries for okg.attr_index
-- under 0021.

DO $rename_chronos_branch_columns$
BEGIN
  IF EXISTS (
    SELECT 1
      FROM pg_attribute
     WHERE attrelid = 'okg.history_compaction_plans'::regclass
       AND attname = 'branch_id'
       AND attnum > 0
       AND NOT attisdropped
  ) THEN
    ALTER TABLE okg.history_compaction_plans
      RENAME COLUMN branch_id TO chronos_branch_id;
  END IF;
END
$rename_chronos_branch_columns$;

-- ===================================================================
-- 2. Bare `deployment` -> `deployment_name` (§4)
-- ===================================================================
--
-- Ten base tables. Two of the audited twelve are deliberately absent, each
-- because another lane owns the relation:
--
--   * okg.verifier_runs -- §2.10 deletes this relation outright (zero
--     consumers) and the dead-weight-retirement lane owns it. Renaming a
--     column on a table that is about to be dropped would only make two lanes
--     conflict over the same relation. That handoff has since LANDED: migration
--     0025 (PR #1132) drops the relation, so its registry row is gone and this
--     migration must never name it.
--   * okg.source_progress -- the ONLY relation in this batch that views.sql
--     reads (okg.v_source_run_diagnostics and okg.v_source_frontier_state,
--     roughly twenty references including two RETURNS TABLE signatures), and
--     views.sql is owned by the concurrent canonical-read-path lane. Renaming
--     it here would mean rewriting ~20 sites in a file another lane is
--     actively editing. Every OTHER relation in this batch is read by zero
--     views, which is why this migration touches views.sql not at all.
--
-- Both keep their registry rows, re-owned to the lane that will converge them.
--
-- RENAME COLUMN carries every dependent index, constraint and default with it,
-- and rewrites dependent VIEW bodies in place, so this is a catalog-only
-- operation: no table is rewritten and no row count can change. A dependent
-- view keeps its OWN output column name, which is why okg.llm_call_stats,
-- okg.v_source_run_diagnostics and okg.v_source_frontier_state still project a
-- column spelled `deployment` afterwards and still need their definitions
-- updated -- see schema.sql for the first and the PR's handoff note for the two
-- that live in views.sql.
--
-- Row preservation was checked by hand rather than asserted here, and this
-- comment says so rather than pointing at a test that does not contain the
-- assertion: the pinned convergence dump was restored, seeded to 25 rows per
-- table in the OLD shape, migrated, then re-read through the NEW column names
-- --- 250 rows, per-table counts equal and per-column content digests
-- identical, zero mismatches. The dump ships these tables EMPTY, so a
-- before/after comparison without that seeding step compares 0 to 0 and reads
-- as a pass while proving nothing.
--
-- What IS asserted in the tree: tests/substrate/db/test_migration_convergence_lane.py
-- proves restored-dump + migrate == fresh install, and
-- tests/substrate/db/test_identity_rename_convergence.py proves the renamed
-- columns exist, the old names are gone, and the indexes survived the rename.

DO $rename_deployment_columns$
DECLARE
  target TEXT;
BEGIN
  FOREACH target IN ARRAY ARRAY[
    'okg.connection_reservations',
    'okg.enricher_progress',
    'okg.enricher_backfill_progress',
    'okg.envelope_violations',
    'okg.equivalence_verdicts',
    'okg.llm_calls',
    'okg.model_outcomes',
    'okg.repair_selections',
    'okg.reuse_certificates',
    'okg.work_ledger'
  ]
  LOOP
    IF to_regclass(target) IS NULL THEN
      CONTINUE;
    END IF;
    IF EXISTS (
      SELECT 1
        FROM pg_attribute
       WHERE attrelid = target::regclass
         AND attname = 'deployment'
         AND attnum > 0
         AND NOT attisdropped
    ) THEN
      EXECUTE format(
        'ALTER TABLE %s RENAME COLUMN deployment TO deployment_name', target
      );
    END IF;
  END LOOP;
END
$rename_deployment_columns$;

-- 2a. Re-create the indexes whose schema.sql statements are now guarded.
--
-- schema.sql's index statements for the renamed columns are wrapped in guards
-- (they run BEFORE this migration and would otherwise abort on the old shape).
-- Those guards delegate recovery to the RENAME above -- but a rename only
-- carries an index that ALREADY EXISTS, and migrate.py:1036 skips the schema
-- body entirely once its checksum is recorded. So a database that reached this
-- point WITHOUT one of these indexes would never get it: the guard declines on
-- the old shape, the rename has nothing to carry, and the body never replays.
-- Before this change every migrate unconditionally self-healed a missing index.
--
-- These statements restore that property. They are no-ops wherever the index
-- already exists, which is every database in the tree today, so this is
-- insurance against losing self-healing rather than a repair of a known gap.

CREATE INDEX IF NOT EXISTS enricher_progress_deployment_last_run_idx
  ON okg.enricher_progress (deployment_name, last_run_at DESC);

CREATE INDEX IF NOT EXISTS enricher_backfill_progress_lookup_idx
  ON okg.enricher_backfill_progress
  (deployment_name, branch_name, enricher_name, status, updated_at DESC);

CREATE INDEX IF NOT EXISTS llm_calls_deployment_caller_ts_idx
  ON okg.llm_calls (deployment_name, caller, ts DESC);

CREATE INDEX IF NOT EXISTS model_outcomes_recorded_at_idx
  ON okg.model_outcomes (deployment_name, recorded_at DESC);

CREATE INDEX IF NOT EXISTS envelope_violations_deployment_generation_idx
  ON okg.envelope_violations (deployment_name, generation_id);

CREATE INDEX IF NOT EXISTS repair_selections_deployment_generation_idx
  ON okg.repair_selections (deployment_name, generation_id);

CREATE INDEX IF NOT EXISTS reuse_certificates_deployment_generation_idx
  ON okg.reuse_certificates (deployment_name, generation_id);

CREATE INDEX IF NOT EXISTS reuse_certificates_deployment_step_idx
  ON okg.reuse_certificates (deployment_name, step_id);

CREATE INDEX IF NOT EXISTS equivalence_verdicts_deployment_generation_idx
  ON okg.equivalence_verdicts (deployment_name, production_generation);

CREATE INDEX IF NOT EXISTS work_ledger_deployment_generation_idx
  ON okg.work_ledger (deployment_name, generation_id);

-- 2b. okg.llm_call_stats -- the one view over a renamed base table.
--
-- RENAME COLUMN rewrites a dependent view's BODY but keeps the view's own
-- output column name, so after the rename above this view would still project
-- a column spelled `deployment` on an upgraded database while a fresh install
-- projects `deployment_name`. That divergence is exactly what the
-- migration-convergence lane refuses, so the view is re-created here rather
-- than left to drift. schema.sql's guarded definition (see the DO block beside
-- okg.llm_calls) declines on the upgrade path precisely so this statement is
-- the one that lands.
--
-- DROP then CREATE, not CREATE OR REPLACE: replacing a view cannot RENAME an
-- output column (`cannot change name of view column`), which is exactly what
-- this needs to do. The drop is paired with the create in this same file, so
-- scripts/check_migration_drop_recreate.py reads it as a replace rather than a
-- permanent no-op. The grant is re-issued below because dropping the view
-- discards it, and schema.sql's GRANT already ran earlier in this transaction.
DROP VIEW IF EXISTS okg.llm_call_stats;

CREATE VIEW okg.llm_call_stats AS
  SELECT deployment_name,
         caller,
         model,
         date_trunc('day', ts) AS day,
         COUNT(*)               AS calls,
         SUM(prompt_tokens)     AS prompt_tokens,
         SUM(completion_tokens) AS completion_tokens,
         SUM(cost_usd)          AS cost_usd
    FROM okg.llm_calls
   WHERE success
   GROUP BY 1, 2, 3, 4;

DO $regrant_llm_call_stats$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_rw') THEN
    GRANT SELECT ON okg.llm_call_stats TO app_rw;
  END IF;
END
$regrant_llm_call_stats$;
