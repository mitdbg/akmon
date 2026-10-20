-- Reference multi-phase migration (TEST-ONLY).
--
-- Lives in the test fixture tree, NOT in
-- src/okg/substrate/db/migrations/: the ordered runner auto-enumerates
-- every shipped NNNN file and would surface this one as pending on
-- every real deployment's `okg up`. A scratch-table backfill has no
-- business appearing in production migration status, so the shipped
-- migrations directory stays clean and tests inject this file through
-- the same seams the runner already exposes (current_known_migrations /
-- load_migrations_manifest / the forward-SQL loader).
--
-- The batched shape below is the DOCUMENTED equivalent of the phase
-- spec in tests/substrate/db/multiphase_reference.py: the executable
-- authority for a multi-phase migration is the registered spec (batches
-- cannot be expressed as replayable flat SQL), and this file is the
-- migration's checksum identity plus the classifier fixture — two
-- top-level data-motion statements, which the manifest check MUST
-- refuse when `class: multi_phase` is not declared.
UPDATE okg_multiphase_scratch
   SET dst = src * 2,
       touch_count = touch_count + 1
 WHERE id > 0 AND id <= 250;

UPDATE okg_multiphase_scratch
   SET dst = src * 2,
       touch_count = touch_count + 1
 WHERE id > 250 AND id <= 500;
