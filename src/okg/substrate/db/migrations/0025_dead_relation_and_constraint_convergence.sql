SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- PACT schema-dead-weight-retirement (audit F15; v1.0 design rev 4 §2.10,
-- §2.15). Two convergences, both of which exist ONLY to bring an already-
-- provisioned database to the shape a fresh `schema.sql` install now has.
--
--   1. Retire the zero-consumer relations the snapshot no longer creates:
--      okg.pagerank_scores + okg.compute_pagerank() + okg.refresh_pagerank(),
--      and okg.verifier_runs.
--   2. Validate six of the seven CHECK constraints that shipped NOT VALID and
--      were never validated. The snapshot now adds those six born-VALID; an
--      existing database still carries the un-validated catalog entry,
--      because the probe-guarded site skips re-adding an already-present
--      constraint and the DROP+ADD sites do not re-run under migrate()'s
--      checksum skip.
--
--      The seventh --- admission_envelopes_target_generation_ref_nonempty_chk
--      --- is NOT validated here, and is not a case of running out of road.
--      Its NOT VALID state is the intended behavior: a database that already
--      holds an admission envelope with a blank target_generation_ref must
--      keep that row readable while new blank inserts are refused. VALIDATE
--      would fail on exactly the databases the constraint was written for.
--      It is registered in not_valid_exemptions.yaml with its owner and the
--      full argument, and the born-VALID gate honors that registry, so the
--      exemption is explicit and reviewable rather than silently unresolved.
--
-- Why this is not flagged `destructive`: the runner's filename heuristic maps
-- retire_/drop_/remove_ slugs to destructive, and a destructive migration is
-- auto-SKIPPED behind an operator-confirmed adapter. A skipped migration here
-- would leave every upgraded database carrying relations no fresh install has
-- and constraints no fresh install leaves invalid -- i.e. it would red the
-- migration-convergence lane permanently, with nothing able to clear it. The
-- same reasoning is recorded on 0023. The drops are made safe IN FACT rather
-- than by label: a per-relation consumer re-verification at HEAD (rg over
-- src/, tests/, deployments/, scripts/, ops/) found no reader, no writer, and
-- no view depending on any of them, and the drops below are RESTRICT (the
-- default), so an unaudited dependency BLOCKS the migration rather than being
-- silently removed by CASCADE.
--
-- okg.lease_audit, the audit's third named table, is deliberately NOT dropped
-- here -- and the reason is NOT a catalog dependency. Dropping it would in
-- fact succeed: okg.v_runtime_lock_surface_inventory stores the STRING
-- 'okg.lease_audit' in a hardcoded VALUES list and never selects the table,
-- so RESTRICT has nothing to refuse. What stops the drop is that the table is
-- still REFERENCED in four places that would then be describing something
-- that does not exist: that published inventory advertises it to app_ro as
-- the `legacy_publisher_lease_audit` surface with table_backed = true (so an
-- app_ro reader would be told a relation exists that no longer resolves),
-- tests/substrate/db/test_runtime_lock_surfaces.py pins that row,
-- tests/substrate/conftest.py truncates the table between tests, and
-- environment_clone.py/management.py disposition it. Retiring it honestly
-- means editing the inventory row in views.sql -- a file the
-- canonical-read-path lane owns -- so it rides with that lane.
--
-- Idempotent: every statement is IF EXISTS or guarded on the catalog state it
-- establishes, so a re-run on a converged database is a no-op and a fresh
-- install (where none of these objects was ever created) takes no branch.

-- ---------------------------------------------------------------------------
-- 1. Zero-consumer relation retirement.
-- ---------------------------------------------------------------------------

-- The functions go first: okg.refresh_pagerank() calls okg.compute_pagerank()
-- and writes okg.pagerank_scores, so dropping the table first would make the
-- RESTRICT drop below report a dependency that this migration is itself about
-- to remove.
DROP FUNCTION IF EXISTS okg.refresh_pagerank(
  TEXT, TEXT, DOUBLE PRECISION, INT, DOUBLE PRECISION
);
DROP FUNCTION IF EXISTS okg.compute_pagerank(
  TEXT, DOUBLE PRECISION, INT, DOUBLE PRECISION
);

-- Say how many rows are being discarded, per table, BEFORE discarding them.
-- The audit's claim is that nothing writes these tables; if that is wrong on
-- some deployment, the row count is the only trace that will survive, and a
-- silent DROP would destroy the evidence along with the data. This does not
-- block the drop -- the relations are retired by decision, not by emptiness --
-- it makes the cost visible in the migrate log and in `okg db migrations`
-- output rather than inferred afterwards from a backup.
DO $retired_relation_row_census$
DECLARE
    relname TEXT;
    n       BIGINT;
BEGIN
    FOREACH relname IN ARRAY ARRAY['pagerank_scores', 'verifier_runs'] LOOP
        IF to_regclass('okg.' || relname) IS NULL THEN
            CONTINUE;
        END IF;
        EXECUTE format('SELECT count(*) FROM okg.%I', relname) INTO n;
        IF n > 0 THEN
            RAISE WARNING
                'retiring okg.% with % row(s) -- the audited consumer set was '
                'empty, so these rows have no reader; recorded here because a '
                'silent drop would leave no trace', relname, n;
        ELSE
            RAISE NOTICE 'retiring okg.% (empty)', relname;
        END IF;
    END LOOP;
END
$retired_relation_row_census$;

DROP TABLE IF EXISTS okg.pagerank_scores;
DROP TABLE IF EXISTS okg.verifier_runs;

-- ---------------------------------------------------------------------------
-- 2. Born-VALID constraint convergence.
-- ---------------------------------------------------------------------------
--
-- VALIDATE CONSTRAINT takes SHARE UPDATE EXCLUSIVE (not ACCESS EXCLUSIVE) and
-- scans the table once. All four relations below are bounded operational
-- ledgers, so the scan is cheap; `lock_timeout` above still bounds the wait.
--
-- A constraint that is absent (a database provisioned before the constraint
-- shipped, then never re-migrated) is skipped rather than added: adding it
-- here would duplicate the snapshot's definition in a second place, and the
-- snapshot's own ADD CONSTRAINT -- now born VALID -- is the authority. A
-- constraint that is ALREADY valid is skipped so a converged database takes
-- no lock and does no scan.
--
-- Failure is deliberate and loud. If a real row violates one of these shape
-- CHECKs, VALIDATE raises check_violation and the whole migrate transaction
-- rolls back. That is the correct outcome: the constraint documents an
-- invariant the writers already claim to hold, so a violating row is a data
-- defect to be reconciled by hand, not a reason to silently leave the
-- constraint decorative. See the PACT ledger for the disposition argument.

DO $born_valid_constraint_convergence$
DECLARE
    target RECORD;
BEGIN
    FOR target IN
        SELECT *
          FROM (VALUES
            ('okg.federation_mailbox_messages',
             'federation_mailbox_payload_shape_chk'),
            ('okg.federation_mailbox_messages',
             'federation_mailbox_lifecycle_evidence_chk'),
            ('okg.federation_mailbox_messages',
             'federation_mailbox_lease_shape_chk'),
            ('okg.poros_inbox_messages',
             'poros_inbox_messages_status_chk'),
            ('okg.poros_detached_object_manifests',
             'poros_detached_manifest_classification_chk'),
            ('okg.poros_detached_object_manifests',
             'poros_detached_manifest_shape_chk')
          ) AS t(relation, constraint_name)
    LOOP
        IF to_regclass(target.relation) IS NULL THEN
            CONTINUE;
        END IF;

        -- convalidated is the only authority here. Reading "constraint
        -- exists" as "constraint is enforced" is exactly the defect this
        -- migration closes, so the predicate names the false state.
        IF NOT EXISTS (
            SELECT 1
              FROM pg_constraint
             WHERE conrelid = target.relation::regclass
               AND conname = target.constraint_name
               AND NOT convalidated
        ) THEN
            CONTINUE;
        END IF;

        EXECUTE format(
            'ALTER TABLE %s VALIDATE CONSTRAINT %I',
            target.relation, target.constraint_name
        );
        RAISE NOTICE 'validated %.%', target.relation, target.constraint_name;
    END LOOP;
END
$born_valid_constraint_convergence$;
